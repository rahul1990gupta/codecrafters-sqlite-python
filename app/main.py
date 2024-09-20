import sys

from dataclasses import dataclass

# import sqlparse - available if you need it!

database_file_path = sys.argv[1]
command = sys.argv[2]

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


def parse_record(record):
    offset=0
    ix, num_bytes_header = parse_varint(record)
    offset+=ix
    ix, type_serial_type = parse_varint(record[offset:])

    offset+=ix
    ix, name_serial_type = parse_varint(record[offset:])

    offset+=ix
    ix, tname_serial_type = parse_varint(record[offset:])
   
    type_size = (type_serial_type -13)/2
    name_size = (name_serial_type -13)/2
    tname_size = (tname_serial_type -13)/2

    tname_value_offset = int(num_bytes_header + type_size + name_size)

    tname = record[tname_value_offset: tname_value_offset + int(tname_size)]
    return tname


if command == ".dbinfo":
    with open(database_file_path, "rb") as database_file:
        # You can use print statements as follows for debugging, they'll be visible when running tests.

        # Uncomment this to pass the first stage
        database_file.seek(16)  # Skip the first 16 bytes of the header
        page_size = int.from_bytes(database_file.read(2), byteorder="big")
        
        database_file.seek(103) # Skip database header
        number_of_tables = int.from_bytes(database_file.read(2), byteorder="big")

        print(f"database page size: {page_size}")
        print(f"number of tables: {number_of_tables}")
         
elif command == ".tables":
    with open(database_file_path, "rb") as database_file:
        database_file.seek(103) # Skip database header
        ncells = int.from_bytes(database_file.read(2), byteorder="big")


        # read cell pointers
        database_file.seek(100 +8)
        cell_pointers = [
                int.from_bytes(database_file.read(2), "big") 
                for _ in range(ncells)]


        # A varint which is the total number of bytes of payload, including any overflow
        # A varint which is the integer key, a.k.a. "rowid"
        # The initial portion of the payload that does not spill to overflow pages.
        tables = []
        for cell_pointer in cell_pointers:
            database_file.seek(cell_pointer)
            bsize, num_pl = parse_varint(database_file.read(9))
           
            database_file.seek(cell_pointer + bsize)
            bsize2, row_id = parse_varint(database_file.read(9))

            database_file.seek(cell_pointer + bsize + bsize2)
            payload = database_file.read(num_pl)
            tables.append(parse_record(payload))
        
        print(" ".join(map(str, tables)))

else:
    print(f"Invalid command: {command}")
