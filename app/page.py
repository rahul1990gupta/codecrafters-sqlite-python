from enum import Enum 
from cell import (
    Cell 
)
from util import parse_varint 

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
            if self.page_header.page_type == 13:
                cell = Cell(cell_pointer, pbytes, dtypes, cnames)
                self.cells.append(cell)
            elif self.page_header.page_type == 5:
                child_page = int.from_bytes(pbytes[cell_pointer: cell_pointer+4], "big")
                ix, rowid = parse_varint(pbytes[cell_pointer + 4: cell_pointer + 13])
                
                self.children.append((child_page, rowid))
            else:
                print("Not supported")

    def get_cells(self, database_file_path):
        if self.page_header.page_type == PageType.TableLeaf.value:
            return self.cells
        elif self.page_header.page_type == PageType.TableInterior.value:
            cells = []
            with open(database_file_path, "rb") as f:
                for child_page, _ in self.children:
                    f.seek((child_page-1) * 4096)
                    child_page = Page(f.read(4096), 0, self.dtypes, self.cnames)
                    cells += child_page.get_cells(database_file_path)

            return cells


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
        
