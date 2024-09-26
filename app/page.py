from enum import Enum 
from util import (
    parse_varint,
    parse_sql
)

class PageType(Enum):
    TableLeaf = 13 
    TableInterior = 5
    IndexLeaf = 10
    IndexInterior = 2


class PageHeader:
    def __init__(self, hbytes):
        # hbytes ==8 for leaf, 12 for interior
        self.page_type = int.from_bytes(hbytes[0:1], 'big')
        if self.page_type in (PageType.IndexInterior.value, PageType.TableInterior.value):
            self.is_interior = True
        else: 
            self.is_interior = False
        if self.page_type in (PageType.IndexLeaf.value, PageType.IndexInterior.value):
            self.is_index = True
        else:
            self.is_index = False

        self.cell_content_offset = int.from_bytes(hbytes[5:7], "big")
        self.num_cells = int.from_bytes(hbytes[3:5], "big")


class Page:
    def __init__(self, page_no, pbytes, offset, dtypes, cnames):

        self.page_header = PageHeader(pbytes[offset:offset + 12])
        self.page_no = page_no
        if self.page_header.is_interior:
            self.header_size = 12
            self.right_most_page_no = int.from_bytes(pbytes[offset+8: offset+12])
        else:
            self.header_size = 8
        self.dtypes = dtypes
        self.cnames = cnames
    
        self.cell_ptrs =[] 
        self.cells = []

        offset = offset + self.header_size

        cell_ptr_end = offset + 2 * self.page_header.num_cells
        for i in range(offset, cell_ptr_end, 2):
            self.cell_ptrs.append(
                int.from_bytes(pbytes[i:i+2], "big")
            )
        for cell_pointer in self.cell_ptrs:
            if self.page_header.page_type == PageType.TableLeaf.value:
                cell = TableLeafCell(cell_pointer, pbytes, dtypes, cnames)
            elif self.page_header.page_type == PageType.TableInterior.value:
                cell = TableInteriorCell(cell_pointer, pbytes, dtypes, cnames)
            elif self.page_header.page_type == PageType.IndexInterior.value:
                cell = IndexInteriorCell(cell_pointer, pbytes, dtypes, cnames)
            elif self.page_header.page_type == PageType.IndexLeaf.value:
                cell = IndexLeafCell(cell_pointer, pbytes, dtypes, cnames)
            else:
                print("Not supported")
            
            self.cells.append(cell)
    
    def _bsearch(self, database_file_path, cond_value):
        data = []
        values = [cell.get_index() for cell in self.cells]
        # print("cells", values)
        for i, val in enumerate(values):
            if i-1>=0 and cond_value < values[i-1]:
                break 
            if cond_value <= val:
                data += self.cells[i].get_data(database_file_path, cond_value)
        
        if self.page_header.is_interior and cond_value > values[-1]:
            with open(database_file_path, "rb") as db:
                db.seek((self.right_most_page_no - 1) * 4096)
                self.right_most_page = Page(
                    self.right_most_page_no, db.read(4096), 0, self.dtypes, self.cnames
                )
            data.extend(self.right_most_page._bsearch(database_file_path, cond_value))
        
        return data
    
    def get_data(self, database_file_path, cond_value):

        # print("getting data..", self.page_header.page_type)
        if cond_value:
            return self._bsearch(database_file_path, cond_value)
        data = []
        for cell in self.cells:
            cell_data = cell.get_data(database_file_path, cond_value)
            data += cell_data

        return data

class SchemaPage(Page):
    def __init__(self, pbytes, offset):
        dtypes = ["text", "text", "text", "integer", "text"]
        cnames = ["type", "name", "tbl_name", "rootpage", "sql"]
        
        super().__init__(1, pbytes, offset, dtypes, cnames)

        self.tables = {}
        for cell in self.cells:
            if cell.get_value("type") == "table":
                self.tables.update({
                    cell.get_value("tbl_name"): cell 
                })
            
###################################################################################################


def parse_payload(record, dtypes):
    dvalues = []
    header_offset, num_bytes_header = parse_varint(record)
    value_offset = num_bytes_header
    for dtype in dtypes:
        if dtype == "text":
            ix, dtype_serial_type = parse_varint(record[header_offset:])
            if dtype_serial_type ==0:
                dtype_size = 0
            else: 
                dtype_size = (dtype_serial_type -13)/2
        elif dtype == "integer":
            ix, dtype_serial_type = parse_varint(record[header_offset:])
            dtype_size = dtype_serial_type
    
        start = int(value_offset)
        end = start + int(dtype_size)
        dvalue = record[start:end]
        if dtype == "integer":
            dvalue = int.from_bytes(dvalue, "big")
        else:
            dvalue = dvalue.decode()

        dvalues.append(dvalue)

        # update offsets 
        header_offset+=ix 
        value_offset+=dtype_size

    return dvalues 

class TableLeafCell:
    def __init__(self, cell_pointer, pbytes, dtypes, cnames):
        cell_size_bytes, cell_size = parse_varint(pbytes[cell_pointer: cell_pointer+9])
        
        offset = cell_pointer + cell_size_bytes
        rowid_bytes, self.row_id = parse_varint(pbytes[offset:offset+9])

        offset+= rowid_bytes
        payload = pbytes[offset:offset+cell_size]
        self.payload = payload
        self.payload_offset = offset 
    
        if dtypes:
            self.dvalues = parse_payload(
                self.payload,
                dtypes
            )
        self.cnames = cnames
        self.dtypes = dtypes
        
        if self.cnames[-1] == "sql" and \
            self.get_value("tbl_name") != "sqlite_sequence"\
                and self.get_value("type") == "table":
            self.process_sql(self.get_value("sql"))


    def get_value(self, col):

        for index, name in enumerate(self.cnames):
            if col =="id":
                return self.row_id
            elif col == name:
                value = self.dvalues[index]
                return value
    
    def process_sql(self, sql):
        columns = parse_sql(sql)
        self.tcnames = []
        self.tdtypes = []
        for col in columns:
            if "\"" in col:
                dtype = col.split(" ")[-1]
                cname = col[: len(dtype) -1]
            else:
                dtype = col.split(" ")[1]
                cname = col.split(" ")[0]
            self.tcnames.append(cname)
            self.tdtypes.append(dtype)
    
    def get_data(self, database_file_path, cond_value):
        # print("getting data...", TableLeafCell, self.row_id)
        data_dict = {
            col: self.dvalues[i]
            for i, col in enumerate(self.cnames)
        }
        data_dict.update({"id": self.row_id})
        return [data_dict]
    
    def get_index(self):
        return self.row_id


class TableInteriorCell:
    def __init__(self, cell_pointer, pbytes, dtypes, cnames):
        self.child_page_no = int.from_bytes(pbytes[cell_pointer: cell_pointer+4], "big")
        ix, self.rowid = parse_varint(pbytes[cell_pointer + 4: cell_pointer + 13])
        self.dtypes = dtypes 
        self.cnames = cnames 

    def get_data(self, database_file_path, cond_value):
        # print("getting data...", TableInteriorCell, self.rowid)
        with open(database_file_path, "rb") as f:
            f.seek((self.child_page_no - 1) * 4096)
            self.child_page = Page(self.child_page_no, f.read(4096), 0, self.dtypes, self.cnames)
        
        return self.child_page.get_data(database_file_path, cond_value)
    
    def get_index(self):
        return self.rowid


"""
A varint which is the total number of bytes of key payload, including any overflow
The initial portion of the payload that does not spill to overflow pages.
A 4-byte big-endian integer page number for the first page of the overflow page list - omitted if all payload fits on the b-tree page.
"""
class IndexLeafCell:
    def __init__(self, cell_pointer, pbytes, dtypes, cnames):
        ix, payload_size = parse_varint(pbytes[cell_pointer:])
        if payload_size > 4000:
            print("Big payload. Need to process overflow pages")
        
        # parse_payload(record, dtypes)
        start = cell_pointer + ix 
        end = start + payload_size
        dvalues = parse_payload(
            pbytes[start:end], 
            dtypes
        )
        self.dvalues = dvalues
        self.cnames = cnames 
    

    def get_data(self, database_file_path, cond_value):
        # print("getting data...", IndexLeafCell)
        # print(self.dvalues)
        record = [{
            col: self.dvalues[i]
            for i, col in enumerate(self.cnames)
        }]
        return record 

    def get_index(self):
        return self.dvalues[0]

"""
A 4-byte big-endian page number which is the left child pointer.

A varint which is the total number of bytes of key payload, including any overflow

The initial portion of the payload that does not spill to overflow pages.

A 4-byte big-endian integer page number for the first page of the overflow page list - omitted if all payload fits on the b-tree page.
"""
class IndexInteriorCell:
    def __init__(self, cell_pointer, pbytes, dtypes, cnames):
        self.child_page_no = int.from_bytes(pbytes[cell_pointer:cell_pointer+4], "big")

        ix, payload_size = parse_varint(pbytes[cell_pointer+4: cell_pointer+8])

        if payload_size > 4000:
            print("Big payload. Need to process overflow pages")

        start = cell_pointer + 4 + ix 
        end = start + payload_size
        dvalues = parse_payload(
            pbytes[start:end], 
            dtypes
        )
        self.dvalues = dvalues 
        self.dtypes = dtypes
        self.cnames = cnames 

    def get_data(self, database_file_path, cond_value):
        # print("getting data...", IndexInteriorCell, self.get_index())

        with open(database_file_path, "rb") as f:
            f.seek((self.child_page_no - 1) * 4096)
            self.child_page = Page(self.child_page_no,
                f.read(4096), 0, self.dtypes, self.cnames)
        
        data = []
        if self.dvalues[0] == cond_value:
            data = [{
                col: self.dvalues[i]
                for i, col in enumerate(self.cnames)
            }]
        return data + self.child_page.get_data(database_file_path, cond_value)

    def get_index(self):
        return self.dvalues[0]
      
