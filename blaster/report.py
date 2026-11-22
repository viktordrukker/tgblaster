"""CSV output that opens cleanly in Excel."""
from __future__ import annotations

import csv
import io
from pathlib import Path


def _safe_cell(v):
    """Neutralise CSV formula injection when the report is opened in Excel."""
    if isinstance(v, str) and v[:1] in ("=", "-", "@", "\t", "\r"):
        return "'" + v
    return "" if v is None else v


def csv_text(rows: list[dict]) -> str:
    if not rows:
        return ""
    keys: list[str] = []
    for r in rows:
        for k in r:
            if k not in keys:
                keys.append(k)
    buf = io.StringIO(newline="")
    w = csv.DictWriter(buf, fieldnames=keys)
    w.writeheader()
    for r in rows:
        w.writerow({k: _safe_cell(r.get(k)) for k in keys})
    return buf.getvalue()


def write_csv(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8-sig") as f:   # BOM → opens fine in Excel
        f.write(csv_text(rows))
