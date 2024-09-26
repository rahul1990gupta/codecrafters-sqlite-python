from app.util import (
    parse_varint,
    parse_sql
)
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
        