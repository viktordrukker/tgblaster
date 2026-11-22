"""Cell-level normalisation: phones, Telegram usernames, person names.

Everything here is pure (no I/O) so it can be tested exhaustively.
Real-world tables are messy: phones arrive as Excel floats, with or
without country code, several per cell, glued to text; the "Telegram"
column holds @handles, t.me links, phone numbers or "нет". These
helpers turn one cell into clean values or nothing.
"""
from __future__ import annotations

import re
import unicodedata

import phonenumbers

# --------------------------------------------------------------------------- #
# Phones
# --------------------------------------------------------------------------- #

_SPLIT_RE = re.compile(r"[,;/|\n\r]+|\s(?:или|or|yoki|либо)\s", re.IGNORECASE)
_DIGITS_RE = re.compile(r"\d")
_EXCEL_FLOAT_RE = re.compile(r"^\+?\d+\.0+$")
_EXCEL_SCI_RE = re.compile(r"^\d(?:\.\d+)?[eE]\+\d+$")


def _clean_excel_number(s: str) -> str:
    """'998901234567.0' -> '998901234567' (Excel/pandas float artefact)."""
    if _EXCEL_FLOAT_RE.match(s):
        return s.split(".", 1)[0]
    return s


def _candidates(piece: str, region: str) -> list[tuple[str, str | None]]:
    """(text, region) pairs to try with phonenumbers, most likely first."""
    piece = piece.strip()
    digits = re.sub(r"\D", "", piece)
    out: list[tuple[str, str | None]] = []
    if not digits:
        return out
    if piece.lstrip().startswith("+"):
        out.append(("+" + digits, None))
        return out
    if piece.startswith("00") and len(digits) > 10:
        out.append(("+" + digits[2:], None))
    out.append((digits, region))
    if region == "UZ":
        if len(digits) == 9:
            out.append(("+998" + digits, None))
        if len(digits) == 10 and digits.startswith("8"):      # old trunk prefix
            out.append(("+998" + digits[1:], None))
    if len(digits) == 11 and digits.startswith("8"):          # RU/KZ 8-prefix
        out.append(("+7" + digits[1:], None))
    if len(digits) >= 10:
        out.append(("+" + digits, None))
    return out


def _try_parse(text: str, region: str | None, *, strict: bool) -> str | None:
    try:
        num = phonenumbers.parse(text, region)
    except phonenumbers.NumberParseException:
        return None
    ok = phonenumbers.is_valid_number(num) if strict else phonenumbers.is_possible_number(num)
    if not ok:
        return None
    return phonenumbers.format_number(num, phonenumbers.PhoneNumberFormat.E164)


def extract_phones(cell: object, region: str = "UZ") -> list[str]:
    """Return every distinct E.164 phone found in a cell, in order.

    Strict validation (phonenumbers metadata) is tried first for every
    candidate; only when nothing strict is found do we fall back to
    "possible" numbers with an explicit country code, which covers fresh
    number ranges the bundled metadata doesn't know about yet.
    """
    if cell is None:
        return []
    s = unicodedata.normalize("NFKC", str(cell)).replace("​", "").strip()
    if not s or _EXCEL_SCI_RE.match(s):
        # Scientific notation means Excel already destroyed the digits.
        return []
    s = _clean_excel_number(s)
    if len(_DIGITS_RE.findall(s)) < 7:
        return []

    found: list[str] = []

    def add(p: str | None) -> None:
        if p and p not in found:
            found.append(p)

    pieces = [p for p in _SPLIT_RE.split(s) if p and p.strip()]
    for strict in (True, False):
        for piece in pieces:
            for text, reg in _candidates(piece, region):
                if not strict and (reg is not None or not (
                        piece.strip().startswith("+") or text.startswith("+998"))):
                    # Loose mode only for an explicit '+' or a UZ number —
                    # never turn an arbitrary 10-digit ID into "+1…".
                    continue
                p = _try_parse(text, reg, strict=strict)
                if p:
                    add(p)
                    break
        if found:
            return found
        if strict:
            # Free text such as "звоните +998 90 123 45 67 после 18:00"
            for m in phonenumbers.PhoneNumberMatcher(s, region):
                if phonenumbers.is_valid_number(m.number):
                    add(phonenumbers.format_number(
                        m.number, phonenumbers.PhoneNumberFormat.E164))
            if found:
                return found
    return found


def normalize_phone(cell: object, region: str = "UZ") -> str | None:
    phones = extract_phones(cell, region)
    return phones[0] if phones else None


# --------------------------------------------------------------------------- #
# Telegram usernames
# --------------------------------------------------------------------------- #

_USERNAME_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_]{3,31}$")
_LINK_RE = re.compile(
    r"(?:https?://)?(?:www\.)?(?:t(?:elegram)?\.me|telegram\.dog)/(?P<u>[^\s/?#]+)",
    re.IGNORECASE,
)
_RESOLVE_RE = re.compile(r"tg://resolve\?domain=(?P<u>[A-Za-z0-9_]+)", re.IGNORECASE)
_AT_RE = re.compile(r"(?<![\w.])@(?P<u>[A-Za-z][A-Za-z0-9_]{3,31})\b")

# Words people type into a "Telegram" field instead of a handle.
_JUNK = {
    "none", "null", "nill", "nope", "empty", "netu", "yoq", "absent",
    "notelegram", "no_telegram", "notg", "unknown", "test", "telegram",
    "username", "nickname", "phone", "same", "есть", "нету",
}
# t.me paths that are not users.
_RESERVED_PATHS = {"joinchat", "addstickers", "share", "proxy", "socks", "c", "s", "iv"}


def _valid_username(u: str) -> str | None:
    u = u.strip().strip(".,;:!)(").lstrip("@")
    if not _USERNAME_RE.match(u):
        return None
    if u.lower() in _JUNK or u.endswith("_") or "__" in u:
        return None
    if u.lower().endswith("bot"):
        return None  # bots can't be DM'd as people
    return u


def extract_username(cell: object, *, bare_ok: bool = True) -> str | None:
    """Pull a Telegram username out of a cell.

    `bare_ok` allows a naked 'ivan_petrov' (without @) — only sensible
    in a column we already know is a Telegram column.
    """
    if cell is None:
        return None
    s = unicodedata.normalize("NFKC", str(cell)).strip()
    if not s:
        return None
    m = _LINK_RE.search(s)
    if m:
        path = m.group("u")
        if path.startswith("+") or path.lower() in _RESERVED_PATHS:
            return None  # invite link or phone link, not a username
        return _valid_username(path)
    m = _RESOLVE_RE.search(s)
    if m:
        return _valid_username(m.group("u"))
    m = _AT_RE.search(s)
    if m:
        return _valid_username(m.group("u"))
    if bare_ok and " " not in s:
        return _valid_username(s)
    return None


def phone_from_tme_link(cell: object, region: str = "UZ") -> str | None:
    """t.me/+998901234567 is a link to a phone number."""
    if cell is None:
        return None
    m = re.search(r"t\.me/\+(\d{7,15})", str(cell))
    return normalize_phone("+" + m.group(1), region) if m else None


# --------------------------------------------------------------------------- #
# Names
# --------------------------------------------------------------------------- #

_PATRONYMIC_SUFFIXES = (
    "вич", "вна", "чна", "ична", "ugli", "o'g'li", "og'li", "o‘g‘li", "ogli",
    "угли", "уғли", "qizi", "kizi", "кизи", "қизи", "ovich", "evich", "ovna", "evna",
)
_SURNAME_SUFFIXES = (
    "ов", "ова", "ев", "ева", "ёв", "ёва", "ский", "ская", "цкий", "цкая",
    "енко", "ян", "дзе", "швили",
    "ov", "ova", "ev", "eva", "yev", "yeva", "sky", "skiy", "skaya", "enko",
    "yan", "dze",
)
_CLEAN_NAME_RE = re.compile(r"[^\w\s'‘’ʻ`\-.]", re.UNICODE)


def clean_name(raw: object) -> str:
    if raw is None:
        return ""
    s = unicodedata.normalize("NFKC", str(raw))
    s = _CLEAN_NAME_RE.sub(" ", s)
    s = re.sub(r"\s+", " ", s).strip(" .-")
    if any(ch.isdigit() for ch in s):
        return ""
    return s


def _titlecase(token: str) -> str:
    if token.isupper() or token.islower():
        return "-".join(p[:1].upper() + p[1:].lower() for p in token.split("-"))
    return token


def _is_surname(tok: str) -> bool:
    t = tok.lower()
    return len(t) > 3 and t.endswith(_SURNAME_SUFFIXES)


def _is_patronymic(tok: str) -> bool:
    return tok.lower().endswith(_PATRONYMIC_SUFFIXES)


def first_name_from_full(full: str, order: str = "auto") -> str:
    """Pick the given name from a full-name string.

    order: 'auto' | 'surname_first' | 'given_first'.
    """
    tokens = [t for t in clean_name(full).split(" ") if t]
    if not tokens:
        return ""
    if len(tokens) == 1:
        return _titlecase(tokens[0])
    if order == "surname_first":
        return _titlecase(tokens[1])
    if order == "given_first":
        return _titlecase(tokens[0])
    # auto
    if len(tokens) >= 3 and _is_patronymic(tokens[2]):
        return _titlecase(tokens[1])           # Иванов Иван Иванович
    if _is_patronymic(tokens[-1]) and len(tokens) == 2:
        return _titlecase(tokens[0])           # Иван Иванович
    if _is_surname(tokens[0]) and not _is_surname(tokens[1]):
        return _titlecase(tokens[1])           # Karimov Aziz
    return _titlecase(tokens[0])


def is_negative_answer(value: object) -> bool:
    """'нет', 'no', "yo'q", 'отказ' → True. Used for consent columns."""
    if value is None:
        return False
    s = re.sub(r"[^\w']", " ", str(value).lower()).strip()
    if not s:
        return False
    first = s.split()[0]
    return first in {"нет", "no", "not", "yoq", "yo'q", "йўқ", "йук", "отказ",
                     "отказываюсь", "false", "0", "unsubscribe", "stop", "стоп"}


def optout_key(value: str, region: str) -> str | None:
    """Opt-out key for a phone, @username or id:123 — None if not understood."""
    v = value.strip()
    if v.startswith("id:"):
        return v if v[3:].isdigit() else None
    p = normalize_phone(v, region)
    if p:
        return p
    u = extract_username(v, bare_ok=True)
    return f"@{u}" if u else None
