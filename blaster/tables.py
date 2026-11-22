"""Load any tabular file into a list of sheets of string cells.

Supported: .xlsx/.xlsm (openpyxl), .xls (xlrd), .csv/.tsv/.txt (encoding
and delimiter sniffed), .json (list of objects), and http(s) URLs —
Google Sheets links are rewritten to their xlsx export so every tab
comes along.
"""
from __future__ import annotations

import csv
import datetime as _dt
import io
import json
import re
import urllib.request
from dataclasses import dataclass
from pathlib import Path


@dataclass
class Sheet:
    name: str
    rows: list[list[str]]


def _cell_to_str(v: object) -> str:
    if v is None:
        return ""
    if isinstance(v, bool):
        return "TRUE" if v else "FALSE"
    if isinstance(v, int):
        return str(v)
    if isinstance(v, float):
        if v.is_integer():
            return str(int(v))      # phones stored as numbers
        return repr(v)
    if isinstance(v, (_dt.datetime, _dt.date, _dt.time)):
        return v.isoformat()
    return str(v).strip()


def _trim(rows: list[list[str]]) -> list[list[str]]:
    """Drop trailing empty rows/columns. Inner empty rows are KEPT so that
    row numbers in reports match the row numbers the user sees."""
    rows = [[c.strip() for c in r] for r in rows]
    while rows and not any(rows[-1]):
        rows.pop()
    width = 0
    for r in rows:
        for i in range(len(r) - 1, -1, -1):
            if r[i]:
                width = max(width, i + 1)
                break
    return [(r + [""] * width)[:width] for r in rows]


# --------------------------------------------------------------------------- #
# Format readers
# --------------------------------------------------------------------------- #

def _read_xlsx(data: bytes) -> list[Sheet]:
    import openpyxl

    wb = openpyxl.load_workbook(io.BytesIO(data), read_only=True, data_only=True)
    sheets = []
    for ws in wb.worksheets:
        if getattr(ws, "sheet_state", "visible") != "visible":
            continue
        rows = [[_cell_to_str(v) for v in row] for row in ws.iter_rows(values_only=True)]
        sheets.append(Sheet(ws.title, _trim(rows)))
    wb.close()
    return sheets


def _read_xls(data: bytes) -> list[Sheet]:
    import xlrd

    book = xlrd.open_workbook(file_contents=data)
    sheets = []
    for sh in book.sheets():
        rows = []
        for r in range(sh.nrows):
            row = []
            for c in range(sh.ncols):
                cell = sh.cell(r, c)
                if cell.ctype == xlrd.XL_CELL_DATE:
                    try:
                        v = xlrd.xldate.xldate_as_datetime(cell.value, book.datemode)
                    except Exception:  # noqa: BLE001
                        v = cell.value
                else:
                    v = cell.value
                row.append(_cell_to_str(v))
            rows.append(row)
        sheets.append(Sheet(sh.name, _trim(rows)))
    return sheets


def _decode(data: bytes) -> str:
    if data.startswith((b"\xff\xfe", b"\xfe\xff")):
        return data.decode("utf-16")
    for enc in ("utf-8-sig", "cp1251"):
        try:
            return data.decode(enc)
        except UnicodeDecodeError:
            continue
    return data.decode("latin-1")


def _read_csv(data: bytes) -> list[Sheet]:
    text = _decode(data)
    sample = "\n".join(text.splitlines()[:50])
    delim = ","
    try:
        delim = csv.Sniffer().sniff(sample, delimiters=",;\t|").delimiter
    except csv.Error:
        counts = {d: sample.count(d) for d in (",", ";", "\t", "|")}
        delim = max(counts, key=counts.get)
    rows = list(csv.reader(io.StringIO(text), delimiter=delim))
    return [Sheet("csv", _trim(rows))]


def _read_json(data: bytes) -> list[Sheet]:
    obj = json.loads(_decode(data))
    if isinstance(obj, dict):
        # {"contacts": [...]} or {"Sheet1": [...], ...}
        lists = {k: v for k, v in obj.items() if isinstance(v, list)}
        if not lists:
            raise ValueError("JSON: ожидался список объектов")
    else:
        lists = {"json": obj}
    sheets = []
    for name, items in lists.items():
        if items and isinstance(items[0], dict):
            header: list[str] = []
            for it in items:
                for k in it:
                    if k not in header:
                        header.append(k)
            rows = [header] + [[_cell_to_str(it.get(k)) for k in header] for it in items]
        else:
            rows = [[_cell_to_str(v) for v in (it if isinstance(it, list) else [it])]
                    for it in items]
        sheets.append(Sheet(name, _trim(rows)))
    return sheets


def _sniff_format(data: bytes, hint: str) -> str:
    hint = hint.lower()
    if data[:4] == b"PK\x03\x04":
        return "xlsx"
    if data[:8] == b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1":
        return "xls"
    if hint in ("xlsx", "xlsm", "xls", "json"):
        return hint
    head = data[:200].lstrip(b"\xef\xbb\xbf \r\n\t")
    if head[:1] in (b"[", b"{"):
        return "json"
    if head[:15].lower().startswith((b"<!doctype html", b"<html")):
        raise ValueError(
            "Вместо таблицы пришла HTML-страница. Для Google Sheets открой "
            "доступ «Все, у кого есть ссылка» или скачай файл как .xlsx.")
    return "csv"


# --------------------------------------------------------------------------- #
# URLs
# --------------------------------------------------------------------------- #

_GSHEET_RE = re.compile(r"docs\.google\.com/spreadsheets/d/([A-Za-z0-9_-]+)")
_GDRIVE_RE = re.compile(r"drive\.google\.com/(?:file/d/|open\?id=)([A-Za-z0-9_-]+)")


def normalize_url(url: str) -> str:
    m = _GSHEET_RE.search(url)
    if m:
        return f"https://docs.google.com/spreadsheets/d/{m.group(1)}/export?format=xlsx"
    m = _GDRIVE_RE.search(url)
    if m:
        return f"https://drive.google.com/uc?export=download&id={m.group(1)}"
    return url


def _download(url: str, timeout: int = 60) -> tuple[bytes, str]:
    req = urllib.request.Request(normalize_url(url), headers={"User-Agent": "tgblaster/2"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:  # noqa: S310
        data = resp.read()
        ctype = resp.headers.get("Content-Type", "")
    hint = ""
    if "spreadsheetml" in ctype:
        hint = "xlsx"
    elif "ms-excel" in ctype:
        hint = "xls"
    elif "json" in ctype:
        hint = "json"
    return data, hint


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #

def load(source: str | Path) -> list[Sheet]:
    """Load a file path or URL into sheets. Never returns hidden sheets."""
    src = str(source)
    if src.startswith(("http://", "https://")):
        data, hint = _download(src)
    else:
        path = Path(src)
        data = path.read_bytes()
        hint = path.suffix.lstrip(".")
    fmt = _sniff_format(data, hint)
    reader = {"xlsx": _read_xlsx, "xlsm": _read_xlsx, "xls": _read_xls,
              "json": _read_json, "csv": _read_csv}[fmt]
    sheets = [s for s in reader(data) if any(any(r) for r in s.rows)]
    if not sheets:
        raise ValueError("Файл пустой — не нашёл ни одной непустой строки.")
    return sheets
