import sys
import re
from app.page import (
    PageHeader,
    SchemaPage
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
                    if tname != 'sqlite_sequence']
        print(" ".join(tables))
    
    elif command.lower().startswith("select") and "count" in command:
        with open(database_file_path, "rb") as database_file:
            cell = schema_page.tables[table_name]
            database_file.seek((cell.get("rootpage")-1) * 4096)

            data_page = Page(database_file.read(4096), 0,  cell.tdtypes, cell.tcnames)
            print(data_page.page_header.num_cells)
    elif command.lower().startswith("select"):
        # find rootpage for the table and build data page 
        with open(database_file_path, "rb") as database_file:
            cell = schema_page.tables[table_name]
            database_file.seek((cell.get("rootpage")-1) * 4096)

            data_page = Page(database_file.read(4096), 0, cell.tdtypes, cell.tcnames)

            for cell in data_page.get_cells(database_file_path):
                if sql.qualify(cell):
                    vals = [cell.get(col) for col in cols]
                    print("|".join(map(str, vals)))
    else:
        print(f"Invalid command: {command}")


if __name__ == "__main__":
    database_file_path = sys.argv[1]
    command = sys.argv[2].strip()
    main(command, database_file_path)