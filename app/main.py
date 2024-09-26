import sys
import re
from page import (
    PageHeader,
    SchemaPage,
    Page
)

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

        self.select_clause = sql[si:fi]
        from_clause = sql[fi:wi]
        where_clause = sql[wi:]

        self.columns = [col.strip() for col in self.select_clause[7:].split(",")]
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

    def qualify(self, record):
        if self.has_where:
            value = record.get(self.condition[0])
            op = self.condition[1]
            op = "==" if op == "=" else op 
            expression = f"\'{value}\'{op}{self.condition[2]}"
            return eval(expression)
        return True
    
    def get_condition_value(self):
        if self.has_where:
            return self.condition[2]

def get_row_ids(schema_page, table_name, database_file_path, cond_value):
    index_rootpage = 0
    for cell in schema_page.cells:
        if cell.get_value("tbl_name") == table_name and cell.get_value("type") == "index":
            index_rootpage = cell.get_value("rootpage")
    
    with open(database_file_path, "rb") as database_file:
        database_file.seek((index_rootpage -1 )* 4096)
        index_rootpage = Page(index_rootpage,
            database_file.read(4096), 0, 
                              ["text", "integer"], 
                              ["country", "rowid"])
        
    index_data = index_rootpage.get_data(database_file_path, cond_value)
    print("cells scanned", len(index_data))
    filtered_data = [
        d for d in index_data if d.get("country") == cond_value
    ]
    return filtered_data

def index_exists(schema_page, table_name):
    for cell in schema_page.cells:
        if cell.get_value("tbl_name") == table_name and cell.get_value("type") == "index":
            return True
    return False    

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
                    if tname != 'sqlite_sequence']
        print(" ".join(tables))
    
    elif command.lower().startswith("select") \
        and "count" in sql.select_clause:
        with open(database_file_path, "rb") as database_file:
            cell = schema_page.tables[table_name]
            page_no = cell.get_value("rootpage")
            database_file.seek((page_no -1) * 4096)

            data_page = Page(page_no, database_file.read(4096), 0,  cell.tdtypes, cell.tcnames)
            print(data_page.page_header.num_cells)
    elif command.lower().startswith("select"):
        # find rootpage for the table and build data page 

        
        # assert row_ids == [121311, 2102438, 5729848, 6634629]
        
        # {6634629, 2102438, 121311}

        data = []
        with open(database_file_path, "rb") as database_file:
            schema_cell = schema_page.tables[table_name]
            page_no = schema_cell.get_value("rootpage")
            database_file.seek((page_no-1) * 4096)

            data_page = Page(page_no, database_file.read(4096), 0, schema_cell.tdtypes, schema_cell.tcnames)

        if index_exists(schema_page, table_name):
            cond_value = sql.get_condition_value().replace("'", "")
            records = get_row_ids(schema_page, table_name, database_file_path, cond_value)

            for record in records:
                print(record, record.get("rowid"))
                data+=data_page.get_data(database_file_path, record.get("rowid"))
            print("data fetched", len(data))
        else:
            data = data_page.get_data(database_file_path, None)

        for record in data:
            if sql.qualify(record):
                vals = [record.get(col) for col in cols]
                print("|".join(map(str, vals)))
    else:
        print(f"Invalid command: {command}")


if __name__ == "__main__":
    database_file_path = sys.argv[1]
    command = sys.argv[2].strip()
    main(command, database_file_path)