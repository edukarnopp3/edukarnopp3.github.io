from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
import posixpath
import re
import unicodedata
import zipfile
import xml.etree.ElementTree as ET


KNOWN_PARAMETERS = ("CO2", "NOx", "VOC", "PM10", "PM2.5", "PM1", "Umid", "Temp", "Pressao")

PARAMETER_ALIASES = {
    "co2": "CO2",
    "nox": "NOx",
    "voc": "VOC",
    "pm10": "PM10",
    "pm25": "PM2.5",
    "pm2_5": "PM2.5",
    "pm2": "PM2.5",
    "pm1": "PM1",
    "umid": "Umid",
    "umidade": "Umid",
    "temp": "Temp",
    "temperatura": "Temp",
    "pressao": "Pressao",
    "press": "Pressao",
}

NS = {
    "main": "http://schemas.openxmlformats.org/spreadsheetml/2006/main",
    "rel": "http://schemas.openxmlformats.org/package/2006/relationships",
    "office_rel": "http://schemas.openxmlformats.org/officeDocument/2006/relationships",
}


@dataclass(frozen=True)
class IseqRecord:
    data_local: datetime
    parameter: str
    value: float


def normalize_text(value: object) -> str:
    text = "" if value is None else str(value)
    text = unicodedata.normalize("NFD", text)
    text = "".join(ch for ch in text if unicodedata.category(ch) != "Mn")
    return re.sub(r"[^a-z0-9]+", "", text.lower())


def normalize_parameter(value: object) -> str | None:
    key = normalize_text(value)
    return PARAMETER_ALIASES.get(key)


def parse_number(value: object) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip()
    if not text:
        return None
    text = text.replace("\u00a0", "").replace(" ", "")
    if "," in text and "." in text:
        text = text.replace(".", "").replace(",", ".")
    else:
        text = text.replace(",", ".")
    try:
        return float(text)
    except ValueError:
        return None


def parse_local_datetime(value: object) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, (int, float)):
        # Excel serial date, using the 1900 date system and preserving Excel's leap-year bug offset.
        return datetime.fromordinal(datetime(1899, 12, 30).toordinal() + int(value)) \
            .replace(hour=0, minute=0, second=0, microsecond=0)

    text = str(value).strip()
    if not text:
        return None
    text = text.replace(",", " ")
    text = re.sub(r"\s+", " ", text)
    formats = (
        "%d/%m/%Y %H:%M:%S",
        "%d/%m/%Y %H:%M",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M",
        "%d/%m/%Y",
        "%Y-%m-%d",
    )
    for fmt in formats:
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            pass
    return None


def month_chunks(start: datetime, end: datetime) -> list[tuple[datetime, datetime]]:
    if end < start:
        raise ValueError("end must be after start")

    chunks: list[tuple[datetime, datetime]] = []
    cursor = start
    while cursor <= end:
        if cursor.month == 12:
            next_month = cursor.replace(year=cursor.year + 1, month=1, day=1, hour=0, minute=0, second=0)
        else:
            next_month = cursor.replace(month=cursor.month + 1, day=1, hour=0, minute=0, second=0)
        chunk_end = min(end, next_month)
        chunks.append((cursor, chunk_end))
        cursor = chunk_end
        if cursor == end:
            break
    return chunks


def parse_iseq_xlsx(path: str | Path) -> list[IseqRecord]:
    rows = read_xlsx_sheet(path, preferred_sheet="Dados")
    if not rows:
        return []

    header = rows[0]
    local_idx = find_header_index(header, ("timestamplocal", "datahora", "datalocal"))
    parameter_idx = find_header_index(header, ("parametrosolicitado", "parametro", "sensorbanco"))
    value_idx = find_header_index(header, ("valor", "value"))

    if local_idx is None or value_idx is None:
        raise ValueError("Planilha ISEQ sem colunas Timestamp (Local) e Valor.")

    records: list[IseqRecord] = []
    for row in rows[1:]:
        local_value = get_cell(row, local_idx)
        value = parse_number(get_cell(row, value_idx))
        data_local = parse_local_datetime(local_value)
        if not data_local or value is None:
            continue

        parameter = None
        if parameter_idx is not None:
            parameter = normalize_parameter(get_cell(row, parameter_idx))
        if not parameter:
            parameter = infer_parameter_from_filename(path)
        if not parameter:
            continue

        records.append(IseqRecord(data_local=data_local, parameter=parameter, value=value))
    return records


def records_to_wide_rows(records: list[IseqRecord]) -> list[dict[str, object]]:
    by_timestamp: dict[datetime, dict[str, object]] = {}
    for record in records:
        row = by_timestamp.setdefault(record.data_local, {"data_local": record.data_local.isoformat(timespec="seconds")})
        row[record.parameter] = record.value
    return [by_timestamp[key] for key in sorted(by_timestamp)]


def merge_iseq_xlsx(paths: list[str | Path]) -> list[dict[str, object]]:
    records: list[IseqRecord] = []
    for path in paths:
        records.extend(parse_iseq_xlsx(path))
    return records_to_wide_rows(records)


def infer_parameter_from_filename(path: str | Path) -> str | None:
    stem = Path(path).stem
    candidate = stem.split("_", 1)[0]
    return normalize_parameter(candidate)


def find_header_index(header: list[object], candidates: tuple[str, ...]) -> int | None:
    normalized = [normalize_text(cell) for cell in header]
    for candidate in candidates:
        if candidate in normalized:
            return normalized.index(candidate)
    for idx, value in enumerate(normalized):
        if any(candidate in value for candidate in candidates):
            return idx
    return None


def get_cell(row: list[object], index: int) -> object | None:
    return row[index] if index < len(row) else None


def read_xlsx_sheet(path: str | Path, preferred_sheet: str | None = None) -> list[list[object]]:
    with zipfile.ZipFile(path) as archive:
        shared_strings = read_shared_strings(archive)
        sheet_path = resolve_sheet_path(archive, preferred_sheet)
        root = ET.fromstring(archive.read(sheet_path))

        rows: list[list[object]] = []
        for row_node in root.findall("main:sheetData/main:row", NS):
            row_values: list[object] = []
            for cell_node in row_node.findall("main:c", NS):
                ref = cell_node.attrib.get("r", "A1")
                col_idx = column_index(ref)
                while len(row_values) < col_idx:
                    row_values.append(None)
                row_values.append(read_cell_value(cell_node, shared_strings))
            rows.append(row_values)
        return rows


def read_shared_strings(archive: zipfile.ZipFile) -> list[str]:
    if "xl/sharedStrings.xml" not in archive.namelist():
        return []
    root = ET.fromstring(archive.read("xl/sharedStrings.xml"))
    values: list[str] = []
    for item in root.findall("main:si", NS):
        values.append("".join(node.text or "" for node in item.findall(".//main:t", NS)))
    return values


def resolve_sheet_path(archive: zipfile.ZipFile, preferred_sheet: str | None) -> str:
    workbook = ET.fromstring(archive.read("xl/workbook.xml"))
    rels = ET.fromstring(archive.read("xl/_rels/workbook.xml.rels"))
    rel_map = {rel.attrib["Id"]: rel.attrib["Target"] for rel in rels}

    first_sheet: str | None = None
    for sheet in workbook.findall("main:sheets/main:sheet", NS):
        relationship_id = sheet.attrib.get(f"{{{NS['office_rel']}}}id")
        target = rel_map.get(relationship_id or "")
        if not target:
            continue
        sheet_path = normalize_workbook_target(target)
        if first_sheet is None:
            first_sheet = sheet_path
        if preferred_sheet and sheet.attrib.get("name") == preferred_sheet:
            return sheet_path

    if first_sheet:
        return first_sheet
    raise ValueError("Nenhuma aba encontrada no arquivo XLSX.")


def normalize_workbook_target(target: str) -> str:
    if target.startswith("/"):
        return target.lstrip("/")
    return posixpath.normpath(posixpath.join("xl", target))


def column_index(cell_ref: str) -> int:
    letters = "".join(ch for ch in cell_ref if ch.isalpha())
    index = 0
    for letter in letters:
        index = index * 26 + ord(letter.upper()) - 64
    return index - 1


def read_cell_value(cell_node: ET.Element, shared_strings: list[str]) -> object | None:
    cell_type = cell_node.attrib.get("t")
    if cell_type == "inlineStr":
        return "".join(node.text or "" for node in cell_node.findall(".//main:t", NS))

    value_node = cell_node.find("main:v", NS)
    if value_node is None:
        return None
    raw_value = value_node.text or ""

    if cell_type == "s":
        return shared_strings[int(raw_value)]
    if cell_type == "b":
        return raw_value == "1"
    return raw_value
