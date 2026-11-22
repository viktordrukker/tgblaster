"""Read and update a .env file in place, keeping comments and order.

Written in place (not via rename) so it also works when .env is a
single-file bind mount inside Docker.
"""
from __future__ import annotations

import re
from pathlib import Path

_LINE = re.compile(r"^\s*([A-Za-z_][A-Za-z0-9_]*)\s*=(.*)$")


def read(path: str | Path) -> dict[str, str]:
    p = Path(path)
    if not p.is_file():
        return {}
    out = {}
    for line in p.read_text(encoding="utf-8-sig").splitlines():
        m = _LINE.match(line)
        if m:
            out[m.group(1)] = m.group(2).strip().strip("'\"")
    return out


def update(path: str | Path, values: dict) -> None:
    p = Path(path)
    lines = p.read_text(encoding="utf-8-sig").splitlines() if p.is_file() else []
    todo = {k: str(v) for k, v in values.items()}
    for i, line in enumerate(lines):
        m = _LINE.match(line)
        if m and m.group(1) in todo:
            lines[i] = f"{m.group(1)}={todo.pop(m.group(1))}"
    lines += [f"{k}={v}" for k, v in todo.items()]
    with p.open("w", encoding="utf-8", newline="\n") as f:
        f.write("\n".join(lines) + "\n")
