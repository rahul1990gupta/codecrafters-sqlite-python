import unittest 
from app.main import (
    parse_varint,
    PageHeader,
    DBHeader,
    Page,
    SchemaCell,
    Cell
)
class TestMain(unittest.TestCase):
    def setUp(self):
        with open("sample.db", "rb") as database_file:
            self.first_page = database_file.read(4096)

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
        cell = Cell(3983, self.first_page)

        self.assertEqual(cell.row_id, 1)
        self.assertEqual(len(cell.payload), 111)
        self.assertEqual(cell.payload_offset, 3985)

                
    def test_schema_cell(self):
        cell = SchemaCell(3983, self.first_page)
        self.assertEqual(cell.table_name, b'apples')
        self.assertEqual(cell.rootpage, 1)
        self.assertEqual(cell.sql, None)

    def test_page(self):
        schema_page = Page(self.first_page, SchemaCell, offset=100)
        self.assertEqual(len(schema_page.cells), 3)
        self.assertEqual(len(schema_page.cell_ptrs), 3)


