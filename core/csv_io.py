"""CSV import + phone normalization.

Accepts CSVs with *any* reasonable column names. We map them heuristically.
Phones are normalized to E.164 using the `phonenumbers` library with a
configurable default region.
"""
from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Iterable

import pandas as pd
import phonenumbers


NAME_CANDIDATES = ("name", "full_name", "fullname", "имя", "имя_фамилия", "fio", "фио")
PHONE_CANDIDATES = ("phone", "phone_number", "mobile", "tel", "телефон", "номер", "mob")
TG_CANDIDATES = ("telegram", "tg", "tg_username", "username", "nick", "ник", "telegram_nick")


@dataclass
class Contact:
    name: str
    phone: str  # E.164, e.g. +79001234567
    raw_phone: str
    telegram_hint: str | None = None
    extra: dict | None = None

    def extra_json(self) -> str | None:
        return json.dumps(self.extra, ensure_ascii=False) if self.extra else None


def _find_col(df_cols: list[str], candidates: Iterable[str]) -> str | None:
    norm = {c: re.sub(r"[^a-zа-я0-9_]", "", c.strip().lower()) for c in df_cols}
    for col, n in norm.items():
        if n in candidates:
            return col
    # fuzzy: contains any candidate substring
    for col, n in norm.items():
        for cand in candidates:
            if cand in n:
                return col
    return None


def normalize_phone(raw: str, default_region: str = "RU") -> str | None:
    """Return E.164 phone or None if invalid."""
    if raw is None:
        return None
    raw_str = str(raw).strip()
    if not raw_str:
        return None
    # Clean typical artifacts
    cleaned = raw_str.replace(" ", "").replace("-", "").replace("(", "").replace(")", "")
    cleaned = cleaned.replace("\u00a0", "").replace("\u200b", "")
    try:
        parsed = phonenumbers.parse(cleaned, default_region)
    except phonenumbers.NumberParseException:
        return None
    if not phonenumbers.is_valid_number(parsed):
        return None
    return phonenumbers.format_number(parsed, phonenumbers.PhoneNumberFormat.E164)


def parse_csv(source, default_region: str = "RU") -> tuple[list[Contact], list[dict], list[dict]]:
    """Read a CSV (path or file-like or bytes) and return (valid, invalid, duplicates).

    Auto-detects columns heuristically. For an explicit-mapping flow (Google
    Sheets column picker), use `parse_with_mapping` instead.
    """
    df = pd.read_csv(source, dtype=str, keep_default_na=False)
    df.columns = [str(c) for c in df.columns]

    name_col = _find_col(list(df.columns), NAME_CANDIDATES)
    phone_col = _find_col(list(df.columns), PHONE_CANDIDATES)
    tg_col = _find_col(list(df.columns), TG_CANDIDATES)

    if phone_col is None:
        raise ValueError(
            "Не нашёл колонку с телефонами. Назови её phone/tel/телефон/номер."
        )

    return _parse_dataframe(
        df, phone_col=phone_col, name_col=name_col, tg_col=tg_col,
        extra_cols=None, default_region=default_region,
    )


def parse_with_mapping(
    source,
    column_map: dict,
    default_region: str = "RU",
    read_headers_only: bool = False,
) -> tuple[list[Contact], list[dict], list[dict]]:
    """Parse a CSV using an explicit column map (the Google-Sheets flow).

    `column_map` shape:
        {
          "phone":    "<sheet-column>" or None,
          "name":     "<sheet-column>" or None,
          "username": "<sheet-column>" or None,
          "extra":    ["col-a", "col-b"],    # optional — preserved in extra_json
        }

    At least one of `phone` or `username` must be set (otherwise nothing
    is sendable). Unknown column references raise ValueError.
    """
    df = pd.read_csv(source, dtype=str, keep_default_na=False)
    df.columns = [str(c) for c in df.columns]
    all_cols = set(df.columns)

    phone_col = column_map.get("phone") or None
    name_col = column_map.get("name") or None
    username_col = column_map.get("username") or None
    extra_cols = list(column_map.get("extra") or [])

    for role, col in [("phone", phone_col), ("name", name_col),
                      ("username", username_col)]:
        if col and col not in all_cols:
            raise ValueError(
                f"Колонка «{col}» ({role}) не найдена в таблице. "
                f"Доступные: {sorted(all_cols)}"
            )
    unknown_extras = [c for c in extra_cols if c not in all_cols]
    if unknown_extras:
        raise ValueError(f"Extra-колонки не найдены: {unknown_extras}")
    if not phone_col and not username_col:
        raise ValueError(
            "Нужна хотя бы одна из колонок «phone» или «username» — "
            "иначе не по чему слать сообщения."
        )

    return _parse_dataframe(
        df, phone_col=phone_col, name_col=name_col, tg_col=username_col,
        extra_cols=extra_cols, default_region=default_region,
    )


def _parse_dataframe(
    df: "pd.DataFrame",
    phone_col: str | None,
    name_col: str | None,
    tg_col: str | None,
    extra_cols: list[str] | None,
    default_region: str,
) -> tuple[list[Contact], list[dict], list[dict]]:
    """Shared body for parse_csv + parse_with_mapping.

    `extra_cols` is None for parse_csv (preserve every unused column as
    extra) or an explicit list for parse_with_mapping.
    """
    valid: list[Contact] = []
    invalid: list[dict] = []
    duplicates: list[dict] = []
    seen_phones: set[str] = set()

    for _, row in df.iterrows():
        raw_phone = str(row[phone_col]).strip() if phone_col else ""
        name = str(row[name_col]).strip() if name_col else ""
        tg_raw = str(row[tg_col]).strip() if tg_col else ""

        if extra_cols is None:
            consumed = {c for c in (name_col, phone_col, tg_col) if c}
            extra = {
                k: str(v).strip() for k, v in row.items()
                if k not in consumed and str(v).strip()
            }
        else:
            extra = {c: str(row[c]).strip() for c in extra_cols if str(row[c]).strip()}

        e164 = normalize_phone(raw_phone, default_region=default_region) if raw_phone else None
        tg_hint = _normalize_username(tg_raw) if tg_raw else None
        # Google Forms often have one combined column "phone or @username"
        # ("укажи номер если нет ника"). If the phone-column value isn't a
        # real phone AND we don't have a separate username column, try to
        # rescue it as a username. If a distinct username column exists and
        # has its own value, we trust that and never override from phone_col.
        if not tg_hint and raw_phone and (tg_col is None or tg_col == phone_col):
            tg_hint = _normalize_username(raw_phone)

        if e164:
            if e164 in seen_phones:
                duplicates.append({"name": name, "phone": e164, "raw_phone": raw_phone})
                continue
            seen_phones.add(e164)
            valid.append(Contact(
                name=name or "",
                phone=e164,
                raw_phone=raw_phone,
                telegram_hint=tg_hint,
                extra=extra or None,
            ))
            continue

        if tg_hint:
            synth = f"tg:{tg_hint.lower()}"
            if synth in seen_phones:
                duplicates.append({"name": name, "phone": synth, "raw_phone": raw_phone})
                continue
            seen_phones.add(synth)
            valid.append(Contact(
                name=name or "",
                phone=synth,
                raw_phone=raw_phone,
                telegram_hint=tg_hint,
                extra=extra or None,
            ))
            continue

        invalid.append({
            "name": name, "phone": raw_phone,
            "reason": "нет ни валидного номера, ни @username",
        })
    return valid, invalid, duplicates


def read_headers(source) -> list[str]:
    """Cheap: fetch only the header row. Used by the mapping UI."""
    df = pd.read_csv(source, dtype=str, keep_default_na=False, nrows=0)
    return [str(c) for c in df.columns]


def guess_column_map(headers: list[str]) -> dict:
    """Best-effort mapping seed for the UI. Uses the same heuristics as
    `parse_csv` so the default selection matches the auto-detect behavior.
    """
    return {
        "phone": _find_col(headers, PHONE_CANDIDATES),
        "name": _find_col(headers, NAME_CANDIDATES),
        "username": _find_col(headers, TG_CANDIDATES),
        "extra": [],
    }


def contacts_to_db_rows(contacts: list[Contact]) -> list[dict]:
    return [
        {
            "name": c.name,
            "phone": c.phone,
            "raw_phone": c.raw_phone,
            "extra_json": c.extra_json(),
            "tg_username_hint": _normalize_username(c.telegram_hint),
        }
        for c in contacts
    ]


def _normalize_username(raw: str | None) -> str | None:
    """Strip '@', whitespace, wrapping URLs. Returns None for empty/invalid."""
    if not raw:
        return None
    s = str(raw).strip()
    if not s:
        return None
    # Allow pasted links like https://t.me/someuser
    for prefix in ("https://t.me/", "http://t.me/", "t.me/", "tg://resolve?domain="):
        if s.startswith(prefix):
            s = s[len(prefix):]
            break
    s = s.lstrip("@").strip()
    # Basic sanity — TG usernames are 5..32 chars, [A-Za-z0-9_]
    if not re.fullmatch(r"[A-Za-z0-9_]{4,32}", s):
        return None
    return s
