import sys

from dataclasses import dataclass

# import sqlparse - available if you need it!


"""
- first 100 bytes contains the database header

"""

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

class Cell:
    def __init__(self, cell_pointer, pbytes):

        cell_size_bytes, cell_size = parse_varint(pbytes[cell_pointer: cell_pointer+9])
        
        offset = cell_pointer + cell_size_bytes
        rowid_bytes, self.row_id = parse_varint(pbytes[offset:offset+9])

        offset+= rowid_bytes
        payload = pbytes[offset:offset+cell_size]
        self.payload = payload
        self.payload_offset = offset 
    
"""
CREATE TABLE sqlite_schema(
  type text,
  name text,
  tbl_name text,
  rootpage integer,
  sql text
);
"""
class SchemaCell(Cell):
    def __init__(self, cell_pointer, pbytes):
        super().__init__(cell_pointer, pbytes)

        self.parse_schema_record(self.payload)
    
    def parse_schema_record(self, record):

        offset=0
        ix, num_bytes_header = parse_varint(record)
        offset+=ix
        ix, type_serial_type = parse_varint(record[offset:])

        offset+=ix
        ix, name_serial_type = parse_varint(record[offset:])

        offset+=ix
        ix, tname_serial_type = parse_varint(record[offset:])
        
        offset+=ix
        ix, rootpage_serial_type = parse_varint(record[offset:])
    
        type_size = (type_serial_type -13)/2
        name_size = (name_serial_type -13)/2
        tname_size = (tname_serial_type -13)/2
        

        tname_value_offset = int(num_bytes_header + type_size + name_size)

        self.table_name = record[tname_value_offset: tname_value_offset + int(tname_size)]
        
        rp_offset = tname_value_offset + int(tname_size)
        self.rootpage = int.from_bytes(record[rp_offset: rp_offset + rootpage_serial_type], 'big')

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

class DBHeader:
    def __init__(self, hbytes):
        self.page_size = int.from_bytes(hbytes[16:18], 'big')
        self.sheader = PageHeader(hbytes[100:108])


class Page:
    def __init__(self, pbytes, cell_class, offset=0):
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
            self.cells.append(
                cell_class(cell_pointer, pbytes)
            )


def main(command, database_file_path):
    with open(database_file_path, "rb") as database_file:
        db = DBHeader(database_file.read(108))

    if command == ".dbinfo":
        print(f"database page size: {db.page_size}")
        print(f"number of tables: {db.sheader.num_cells}")
            
    elif command == ".tables":
        with open(database_file_path, "rb") as database_file:

            schema_page = Page(database_file.read(4096), SchemaCell, offset=100)
            tables = [cell.table_name  
                      for cell in schema_page.cells 
                      if cell.table_name != b'sqlite_schema']
            
            print(b" ".join(tables).decode())
    
    elif command.startswith("select"):
        with open(database_file_path, "rb") as database_file:
            schema_page = Page(database_file.read(4096), SchemaCell, offset=100)

            for cell in schema_page.cells:
                if cell.table_name == command.split(" ")[-1].encode():
                    database_file.seek((cell.rootpage-1) * 4096)
                    data_page = Page(database_file.read(4096), Cell)
                    print(data_page.page_header.num_cells)
    else:
        print(f"Invalid command: {command}")

if __name__ == "__main__":
    database_file_path = sys.argv[1]
    command = sys.argv[2].lower().strip()
    main(command, database_file_path)