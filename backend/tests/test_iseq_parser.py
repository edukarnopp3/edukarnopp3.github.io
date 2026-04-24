from __future__ import annotations

from datetime import datetime
from pathlib import Path
import sys
import tempfile
import unittest
import zipfile

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.iseq_parser import merge_iseq_xlsx, month_chunks, parse_iseq_xlsx


class IseqParserTests(unittest.TestCase):
    def test_month_chunks_split_calendar_months(self):
        chunks = month_chunks(datetime(2026, 3, 15, 8), datetime(2026, 5, 2, 17))
        self.assertEqual(len(chunks), 3)
        self.assertEqual(chunks[0][0], datetime(2026, 3, 15, 8))
        self.assertEqual(chunks[0][1], datetime(2026, 4, 1))
        self.assertEqual(chunks[-1][1], datetime(2026, 5, 2, 17))

    def test_parse_and_merge_minimal_iseq_exports(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            co2 = tmp_path / "CO2_1C_69_20_C7_31_D8.xlsx"
            nox = tmp_path / "NOx_1C_69_20_C7_31_D8.xlsx"
            write_minimal_xlsx(co2, "CO2", "478")
            write_minimal_xlsx(nox, "NOx", "12,5")

            records = parse_iseq_xlsx(co2)
            self.assertEqual(len(records), 1)
            self.assertEqual(records[0].parameter, "CO2")
            self.assertEqual(records[0].value, 478.0)

            rows = merge_iseq_xlsx([co2, nox])
            self.assertEqual(rows, [{"data_local": "2026-03-01T00:00:13", "CO2": 478.0, "NOx": 12.5}])

    def test_real_downloaded_sample_if_present(self):
        sample = Path(r"C:\Users\eduardo\Downloads\CO2_1C_69_20_C7_31_D8_2026-04-24T20-35.xlsx")
        if not sample.exists():
            self.skipTest("Arquivo exportado do ISEQ não encontrado nesta máquina.")
        records = parse_iseq_xlsx(sample)
        self.assertEqual(len(records), 82675)
        self.assertEqual(records[0].parameter, "CO2")
        self.assertEqual(records[0].data_local, datetime(2026, 3, 1, 0, 0, 13))
        self.assertEqual(records[-1].data_local, datetime(2026, 3, 31, 23, 59, 45))


def write_minimal_xlsx(path: Path, parameter: str, value: str) -> None:
    shared = [
        "Timestamp (UTC)",
        "Timestamp (Local)",
        "Parâmetro solicitado",
        "Sensor no banco",
        "Valor",
        "2026-03-01 03:00:13",
        "01/03/2026, 00:00:13",
        parameter,
        value,
    ]
    rows = [
        [0, 1, 2, 3, 4],
        [5, 6, 7, 7, 8],
    ]
    with zipfile.ZipFile(path, "w") as zf:
        zf.writestr("[Content_Types].xml", CONTENT_TYPES)
        zf.writestr("_rels/.rels", ROOT_RELS)
        zf.writestr("xl/workbook.xml", WORKBOOK)
        zf.writestr("xl/_rels/workbook.xml.rels", WORKBOOK_RELS)
        zf.writestr("xl/sharedStrings.xml", shared_strings_xml(shared))
        zf.writestr("xl/worksheets/sheet1.xml", sheet_xml(rows))


def shared_strings_xml(values: list[str]) -> str:
    items = "".join(f"<si><t>{value}</t></si>" for value in values)
    return f'<?xml version="1.0" encoding="UTF-8"?><sst xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" count="{len(values)}" uniqueCount="{len(values)}">{items}</sst>'


def sheet_xml(rows: list[list[int]]) -> str:
    row_xml = []
    for ridx, row in enumerate(rows, start=1):
        cells = []
        for cidx, shared_idx in enumerate(row):
            col = chr(ord("A") + cidx)
            cells.append(f'<c r="{col}{ridx}" t="s"><v>{shared_idx}</v></c>')
        row_xml.append(f'<row r="{ridx}">{"".join(cells)}</row>')
    return f'<?xml version="1.0" encoding="UTF-8"?><worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"><sheetData>{"".join(row_xml)}</sheetData></worksheet>'


CONTENT_TYPES = """<?xml version="1.0" encoding="UTF-8"?>
<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">
  <Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>
  <Default Extension="xml" ContentType="application/xml"/>
  <Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>
  <Override PartName="/xl/worksheets/sheet1.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>
  <Override PartName="/xl/sharedStrings.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sharedStrings+xml"/>
</Types>"""

ROOT_RELS = """<?xml version="1.0" encoding="UTF-8"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
  <Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/>
</Relationships>"""

WORKBOOK = """<?xml version="1.0" encoding="UTF-8"?>
<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">
  <sheets><sheet name="Dados" sheetId="1" r:id="rId1"/></sheets>
</workbook>"""

WORKBOOK_RELS = """<?xml version="1.0" encoding="UTF-8"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
  <Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet1.xml"/>
</Relationships>"""


if __name__ == "__main__":
    unittest.main()
