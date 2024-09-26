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
    
        self.num_cells = int.from_bytes(hbytes[3:5], "big")



class Page:
    def __init__(self, pbytes, offset, dtypes, cnames):

        self.page_header = PageHeader(pbytes[offset:offset + 12])
        if self.page_header.is_interior:
            self.header_size = 12 
        else:
            self.header_size = 8
        self.dtypes = dtypes
        self.cnames = cnames
    
        self.cell_ptrs =[] 
        self.cells = []
        self.children = []

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
            else:
                print("Not supported")
            
            self.cells.append(cell)
    
    def get_data(self, database_file_path):
        data = []
        for cell in self.cells:
            cell_data = cell.get_data(database_file_path)
            data += cell_data

        return data

class SchemaPage(Page):
    def __init__(self, pbytes, offset):
        dtypes = ["text", "text", "text", "integer", "text"]
        cnames = ["type", "name", "tbl_name", "rootpage", "sql"]
        
        super().__init__(pbytes, offset, dtypes, cnames)

        self.tables = {}
        for cell in self.cells:
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
            dtype_size = (dtype_serial_type -13)/2
        elif dtype == "integer":
            ix, dtype_serial_type = parse_varint(record[header_offset:])
            dtype_size = dtype_serial_type

        start = int(value_offset)
        end = start + int(dtype_size)
        dvalue = record[start:end]
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
        
        if self.cnames[-1] == "sql" and self.get_value("tbl_name") != "sqlite_sequence":
            self.process_sql(self.get_value("sql"))


    def get_value(self, col):

        for index, name in enumerate(self.cnames):
            if col =="id":
                return self.row_id
            elif col == name:
                value = self.dvalues[index]
                if self.dtypes[index] == "integer":
                    value = int.from_bytes(value, "big")
                else: 
                    value = value.decode()
                return value
        
    
    def process_sql(self, sql):
        columns = parse_sql(sql)
        self.tcnames = [col.split(" ")[0] for col in columns]
        self.tdtypes = [col.split(" ")[1] for col in columns]
    
    def get_data(self, database_file_path):
        data_dict = {
            col: self.dvalues[i].decode()
            for i, col in enumerate(self.cnames)
        }
        data_dict.update({"id": self.row_id})
        return [data_dict]

class TableInteriorCell:
    def __init__(self, cell_pointer, pbytes, dtypes, cnames):
        self.child_page_no = int.from_bytes(pbytes[cell_pointer: cell_pointer+4], "big")
        ix, rowid = parse_varint(pbytes[cell_pointer + 4: cell_pointer + 13])
        self.dtypes = dtypes 
        self.cnames = self.cnames 

    def get_data(self, database_file_path):
        with open(database_file_path, "rb") as f:
            f.seek((self.child_page_no - 1) * 4096)
            self.child_page = Page(f.read(4096), 0, self.dtypes, self.cnames)
        
        return self.child_page.get_data(database_file_path)

class IndexLeafCell:
    def __init__(self):
        pass 

class IndexInteriorCell:
    def __init__(self):
        pass 


