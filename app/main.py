import sys
import re
from dataclasses import dataclass

# import sqlparse - available if you need it!


"""
- first 100 bytes contains the database header

"""
    

def parse_sql(sql):
    sql = sql.lower().replace("\n", "").replace("\t", "")
    match = re.match(r"create table [a-z\_]*[\s]*\((.*)\)", sql)
    try:
        cols_string = match.group(1)
    except Exception as e:
        print(sql)
        print(e)
    columns = [col_string.strip() for col_string in cols_string.split(",")]
    return columns


def parse_varint(byte_stream):
    """
    Parses a varint from the given byte stream.
    
    :param byte_stream: A byte-like object to read the varint from.
    :return: The parsed 64-bit two's complement integer.
    """
    result = 0
    shift = 0
    
    for i in range(9):  # Varint can be up to 9 bytes long
        byte = byte_stream[i]
        
        # Add the lower 7 bits of the current byte to the result
        result |= (byte & 0x7F) << shift
        shift += 7
        
        # If high-order bit is clear, we've reached the last byte
        if byte & 0x80 == 0:
            return i+1, result

    return 9, result  # If all 9 bytes are processed, this is the final result

"""
CREATE TABLE sqlite_schema(
  type text,
  name text,
  tbl_name text,
  rootpage integer,
  sql text
);
          ["text", "text", "text", "integer", "text"] 
"""


class Cell:
    def __init__(self, cell_pointer, pbytes, dtypes, cnames):

        cell_size_bytes, cell_size = parse_varint(pbytes[cell_pointer: cell_pointer+9])
        
        offset = cell_pointer + cell_size_bytes
        rowid_bytes, self.row_id = parse_varint(pbytes[offset:offset+9])

        offset+= rowid_bytes
        payload = pbytes[offset:offset+cell_size]
        self.payload = payload
        self.payload_offset = offset 
    
        if dtypes:
            self.parse_schema_record(
                self.payload,
                dtypes
            )
        self.cnames = cnames
        self.dtypes = dtypes
        
        if self.cnames[-1] == "sql" and self.get("tbl_name") != "sqlite_sequence":
            self.process_sql(self.get("sql"))
    
    def parse_schema_record(self, record, dtypes):
        self.dvalues = []
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
            self.dvalues.append(dvalue)

            # update offsets 
            header_offset+=ix 
            value_offset+=dtype_size

    def get(self, col):
        for index, name in enumerate(self.cnames):
            if col == name:
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
        

class PageHeader:
    def __init__(self, hbytes):
        # hbytes ==8 for leaf, 12 for interior
        self.page_type = int.from_bytes(hbytes[0:1], 'big')
        if self.page_type in (2, 5):
            self.is_interior = True
        else: 
            self.is_interior = False
        if self.page_type in (2, 10):
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
    
        self.cell_ptrs =[] 
        self.cells = []

        offset = offset + self.header_size
        cell_ptr_end = offset + 2 * self.page_header.num_cells
        for i in range(offset, cell_ptr_end, 2):
            self.cell_ptrs.append(
                int.from_bytes(pbytes[i:i+2], "big")
            )

        if self.page_header.page_type != 13:
            print("Page Not supported !")
        

        for cell_pointer in self.cell_ptrs:
            cell = Cell(cell_pointer, pbytes, dtypes, cnames)
            self.cells.append(cell)

class SchemaPage(Page):
    def __init__(self, pbytes, offset):
        dtypes = ["text", "text", "text", "integer", "text"]
        cnames = ["type", "name", "tbl_name", "rootpage", "sql"]
        
        super().__init__(pbytes, offset, dtypes, cnames)

        self.tables = {}
        for cell in self.cells:
            self.tables.update({
                cell.get("tbl_name"): cell
            })
        

class DBHeader:
    def __init__(self, hbytes):
        self.page_size = int.from_bytes(hbytes[16:18], 'big')
        self.sheader = PageHeader(hbytes[100:108])

class SQLParser:
    def __init__(self, sql):
        si = sql.lower().find("select")
        fi = sql.lower().find("from")
        wi = sql.lower().find("where")
        if wi ==-1:
            wi = len(sql)

        select_clause = sql[si:fi]
        from_clause = sql[fi:wi]
        where_clause = sql[wi:]

        self.columns = [col.strip() for col in select_clause[7:].split(",")]
        self.table_name = from_clause[5:].strip()

        self.has_where = False
        if len(where_clause)> 6:
            self.has_where = True
            condition = where_clause[6:].strip()
            match = re.search(r"(=|<|>)", condition)
            self.condition = (
                condition[:match.start()].strip(),
                condition[match.start(): match.end()],
                condition[match.end():].strip() 
            )

    def qualify(self, cell):
        if self.has_where:
            value = cell.get(self.condition[0])
            op = self.condition[1]
            op = "==" if op == "=" else op 
            expression = f"\'{value}\'{op}{self.condition[2]}"
            return eval(expression)
        return True

def main(command, database_file_path):
    with open(database_file_path, "rb") as database_file:
        db = DBHeader(database_file.read(108))

    if command == ".dbinfo":
        print(f"database page size: {db.page_size}")
        print(f"number of tables: {db.sheader.num_cells}")
        return 

    with open(database_file_path, "rb") as database_file:
        schema_page = SchemaPage(database_file.read(4096), offset=100)

    if not command.startswith("."):
        sql = SQLParser(command)
        table_name = sql.table_name
        cols = sql.columns
        
    if command == ".tables":
        tables = [tname  
                    for tname in schema_page.tables.keys() 
                    if tname != b'sqlite_sequence']
        print(" ".join(tables))
    
    elif command.startswith("select") and "count" in command:
        with open(database_file_path, "rb") as database_file:
            cell = schema_page.tables[table_name]
            database_file.seek((cell.get("rootpage")-1) * 4096)

            data_page = Page(database_file.read(4096), 0,  cell.tdtypes, cell.tcnames)
            print(data_page.page_header.num_cells)
    elif command.startswith("select"):
        # find rootpage for the table and build data page 
        with open(database_file_path, "rb") as database_file:
            cell = schema_page.tables[table_name]
            database_file.seek((cell.get("rootpage")-1) * 4096)

            data_page = Page(database_file.read(4096), 0, cell.tdtypes, cell.tcnames)

            for cell in data_page.cells:
                if sql.qualify(cell):
                    vals = [cell.get(col) for col in cols]
                    print("|".join(vals))
    else:
        print(f"Invalid command: {command}")

if __name__ == "__main__":
    database_file_path = sys.argv[1]
    command = sys.argv[2].strip()
    main(command, database_file_path)