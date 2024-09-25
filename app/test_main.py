import unittest 
import sqlparse
from app.main import (
    parse_varint,
    PageHeader,
    DBHeader,
    Page,
    Cell,
    SchemaPage
)
class TestMain(unittest.TestCase):
    def setUp(self):
        with open("sample.db", "rb") as database_file:
            self.first_page = database_file.read(4096)
        
        self.schema_dtypes = ["text", "text", "text", "integer", "text"]
        self.schema_cnames = ["type", "name", "tbl_name", "rootpage", "sql"]

        self.schema_page = SchemaPage(self.first_page, offset=100)

    def test_parse_varint(self):
        self.assertEqual(parse_varint(b'\x00'), (1, 0))
        self.assertEqual(parse_varint(b'\x7f'), (1, 127))
        self.assertEqual(parse_varint(b'\x80\x01'), (2, 128))
        self.assertEqual(parse_varint(b'\xff\x01'), (2, 255))
        self.assertEqual(parse_varint(b'\x80\x80\x01'), (3, 16384))

    def test_page_header(self):
        with open("sample.db", "rb") as database_file:
            database_file.seek(100)
            ph = PageHeader(database_file.read(8))
        # Leaf table B-tree page
        self.assertEqual(ph.page_type, 13)
        self.assertEqual(ph.is_interior, False)
        self.assertEqual(ph.is_index, False)
        self.assertEqual(ph.num_cells, 3)

    def test_dbheader(self):
        with open("sample.db", "rb") as database_file:
            db = DBHeader(database_file.read(108))
        self.assertEqual(db.page_size, 4096)
        self.assertEqual(db.sheader.page_type, 13)
        self.assertEqual(db.sheader.num_cells, 3)        


    def test_cell(self):
        # [3983, 3901, 3779]
        cell = Cell(3983, self.first_page, 
                    self.schema_dtypes, self.schema_cnames)

        self.assertEqual(cell.row_id, 1)
        self.assertEqual(len(cell.payload), 111)
        self.assertEqual(cell.payload_offset, 3985)

        self.assertEqual(cell.get("tbl_name"), 'apples')
        self.assertEqual(cell.get("rootpage"), 2)

        self.assertListEqual(cell.tcnames, ["id", "name", "color"])
        self.assertListEqual(cell.tdtypes, ["integer", "text", "text"]) 


    def test_page(self):
        self.assertEqual(len(self.schema_page.cells), 3)
        self.assertEqual(len(self.schema_page.cell_ptrs), 3)
        self.assertListEqual(
            list(self.schema_page.tables.keys()), 
            ['apples', 'sqlite_sequence', 'oranges']
        )
