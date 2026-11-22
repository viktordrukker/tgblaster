"""Turn an arbitrary contacts table into clean, deduplicated contacts.

Pipeline:  sheets → pick sheet → find header row → assign column roles
(header keywords + content statistics) → per-row extraction → dedup.

Nothing is guessed silently: `ColumnPlan.describe()` prints every
decision, and each detected role can be overridden from the CLI.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field

from . import normalize as N
from .tables import Sheet

HEADER_SCAN_ROWS = 15
STAT_SAMPLE_ROWS = 400


# --------------------------------------------------------------------------- #
# Header keyword classification
# --------------------------------------------------------------------------- #

def norm_header(h: str) -> str:
    h = (h or "").lower().replace("ё", "е")
    h = re.sub(r"[^\w@.]+", " ", h, flags=re.UNICODE)
    return re.sub(r"\s+", " ", h).strip()


def placeholder_key(h: str) -> str:
    """Header → template key: 'Компания / место работы' → 'компания_место_работы'."""
    return re.sub(r"\W+", "_", (h or "").lower().replace("ё", "е"), flags=re.UNICODE).strip("_")


def _words(h: str) -> list[str]:
    return norm_header(h).replace(".", " ").split()


_USER_PREFIXES = ("telegram", "телеграм", "телега", "telega", "tg", "тг",
                  "username", "юзернейм", "юзер", "ник", "nick", "t.me")
_PHONE_PREFIXES = ("телефон", "тел", "номер", "phone", "mobile", "mob", "tel",
                   "telefon", "raqam", "whatsapp", "cell", "контакт", "contact", "моб")
_CONSENT_KEYS = ("соглас", "consent", "рассылк", "rozilik", "roziman", "подписк",
                 "newsletter", "agree")


def header_is_username(h: str) -> bool:
    n = norm_header(h)
    return "@" in n or any(w.startswith(_USER_PREFIXES) for w in _words(h)) or "t.me" in n


def header_is_phone(h: str) -> bool:
    for w in _words(h):
        if w.startswith(("teleg", "телег")):
            continue
        if w.startswith(_PHONE_PREFIXES):
            return True
    return False


def header_is_consent(h: str) -> bool:
    n = norm_header(h)
    return any(k in n for k in _CONSENT_KEYS)


def header_name_role(h: str) -> tuple[str | None, str]:
    """→ (role, order). role ∈ full | greeting | first | last | None."""
    n = norm_header(h)
    w = set(_words(h))
    if not n:
        return None, "auto"
    compact = re.sub(r"[\W_]", "", n)
    if "фио" in w or "fio" in w or compact.startswith(("фио", "fio")) or "fish" in w:
        return "full", "surname_first"
    if n.startswith(("фамилия имя", "familiya ism", "surname name")):
        return "full", "surname_first"
    if ("имя" in n and "фамил" in n) or ("ism" in w and any(x.startswith("famil") for x in w)) \
            or "full name" in n or "fullname" in w or "как вас зовут" in n or "your name" in n \
            or "ваше имя" in n or "name surname" in n or "first and last" in n:
        return "full", "auto"
    if "обращат" in n or "preferred name" in n or "nickname to call" in n:
        return "greeting", "auto"
    if any(x.startswith(("фамил", "familiya", "surname")) for x in w) or "last name" in n \
            or "lastname" in w:
        return "last", "auto"
    if "first name" in n or "firstname" in w or "given name" in n or "имя" in w or "ism" in w \
            or "ismingiz" in w:
        return "first", "auto"
    if w & {"name", "participant", "участник", "гость"}:
        return "full", "auto"
    return None, "auto"


def _header_keyword_hits(row: list[str]) -> int:
    hits = 0
    for c in row:
        if not c or len(c) > 80:
            continue
        if N.extract_phones(c) or N.extract_username(c, bare_ok=False):
            return -1  # a row with real contact data is not a header
        if header_is_phone(c) or header_is_username(c) or header_name_role(c)[0] \
                or header_is_consent(c) or "email" in c.lower() or "почт" in c.lower():
            hits += 1
    return hits


def detect_header_row(rows: list[list[str]]) -> int | None:
    best, best_i = 0, None
    for i, row in enumerate(rows[:HEADER_SCAN_ROWS]):
        h = _header_keyword_hits(row)
        if h > best:
            best, best_i = h, i
    if best_i is not None:
        return best_i
    if rows and _header_keyword_hits(rows[0]) == 0 and \
            sum(1 for c in rows[0] if c) >= max(1, len(rows[0]) // 2):
        # Generic header like "A | B | C" with no contact data in it.
        return 0
    return None


# --------------------------------------------------------------------------- #
# Column statistics
# --------------------------------------------------------------------------- #

@dataclass
class ColumnStats:
    index: int
    header: str
    filled: int = 0
    phone: int = 0
    user_explicit: int = 0
    user_bare: int = 0
    name_like: int = 0

    def ratio(self, attr: str) -> float:
        return getattr(self, attr) / self.filled if self.filled else 0.0


def _looks_like_name(v: str) -> bool:
    if "@" in v or len(v) > 60:
        return False
    c = N.clean_name(v)
    toks = c.split()
    return bool(c) and 1 <= len(toks) <= 4 and any(ch.isalpha() for ch in c)


def column_stats(rows: list[list[str]], headers: list[str], region: str) -> list[ColumnStats]:
    stats = [ColumnStats(i, h) for i, h in enumerate(headers)]
    for row in rows[:STAT_SAMPLE_ROWS]:
        for st in stats:
            v = row[st.index] if st.index < len(row) else ""
            if not v:
                continue
            st.filled += 1
            if N.extract_phones(v, region) or N.phone_from_tme_link(v, region):
                st.phone += 1
            if N.extract_username(v, bare_ok=False):
                st.user_explicit += 1
            if N.extract_username(v, bare_ok=True):
                st.user_bare += 1
            if _looks_like_name(v):
                st.name_like += 1
    return stats


# --------------------------------------------------------------------------- #
# Column plan
# --------------------------------------------------------------------------- #

@dataclass
class ColumnPlan:
    header_row: int | None
    headers: list[str]
    phone_cols: list[int] = field(default_factory=list)
    username_cols: list[int] = field(default_factory=list)
    full_name_col: int | None = None
    first_name_col: int | None = None
    last_name_col: int | None = None
    greeting_col: int | None = None
    consent_col: int | None = None
    name_order: str = "auto"

    def _h(self, i: int | None) -> str:
        return "—" if i is None else f"«{self.headers[i]}» (#{i + 1})"

    def describe(self) -> str:
        hr = "нет (данные с 1-й строки)" if self.header_row is None else f"строка {self.header_row + 1}"
        lines = [
            f"  заголовок:     {hr}",
            f"  телефон:       {', '.join(self._h(i) for i in self.phone_cols) or '—'}",
            f"  telegram:      {', '.join(self._h(i) for i in self.username_cols) or '—'}",
            f"  ФИО целиком:   {self._h(self.full_name_col)}  (порядок: {self.name_order})",
            f"  имя:           {self._h(self.first_name_col)}",
            f"  фамилия:       {self._h(self.last_name_col)}",
            f"  обращение:     {self._h(self.greeting_col)}",
            f"  согласие:      {self._h(self.consent_col)}",
        ]
        return "\n".join(lines)

    @property
    def role_cols(self) -> set[int]:
        s = set(self.phone_cols) | set(self.username_cols)
        for i in (self.full_name_col, self.first_name_col, self.last_name_col,
                  self.greeting_col):
            if i is not None:
                s.add(i)
        return s


@dataclass
class Overrides:
    """CLI overrides. Column refs are header text or 1-based index."""
    header_row: int | None = None          # 1-based; 0 = no header
    phone: list[str] | None = None
    username: list[str] | None = None
    name: str | None = None
    first_name: str | None = None
    last_name: str | None = None
    consent: str | None = None
    name_order: str | None = None


def _resolve_col(ref: str, headers: list[str]) -> int:
    ref = ref.strip()
    if ref.isdigit() and 1 <= int(ref) <= len(headers):
        return int(ref) - 1
    for i, h in enumerate(headers):
        if h == ref:
            return i
    for i, h in enumerate(headers):
        if norm_header(h) == norm_header(ref):
            return i
    matches = [i for i, h in enumerate(headers) if norm_header(ref) in norm_header(h)]
    if len(matches) == 1:
        return matches[0]
    raise ValueError(f"Колонка «{ref}» не найдена. Есть: {headers}")


def build_plan(sheet: Sheet, region: str, ov: Overrides | None = None) -> ColumnPlan:
    ov = ov or Overrides()
    rows = sheet.rows
    if ov.header_row is not None:
        header_row = None if ov.header_row == 0 else ov.header_row - 1
    else:
        header_row = detect_header_row(rows)
    width = max((len(r) for r in rows), default=0)
    raw_headers = rows[header_row] if header_row is not None else []
    headers: list[str] = []
    for i in range(width):
        h = raw_headers[i].strip() if i < len(raw_headers) else ""
        h = h or f"col{i + 1}"
        if h in headers:
            h = f"{h}_{i + 1}"
        headers.append(h)
    data = rows[header_row + 1:] if header_row is not None else rows
    stats = column_stats(data, headers, region)
    plan = ColumnPlan(header_row=header_row, headers=headers)

    # ---- contact columns -------------------------------------------------
    def phone_score(st: ColumnStats) -> float:
        hdr = header_is_phone(st.header) and not header_is_username(st.header)
        r = st.ratio("phone")
        if hdr and r >= 0.15:
            return 2 + r
        return r if r >= 0.6 else 0

    def user_score(st: ColumnStats) -> float:
        hdr = header_is_username(st.header)
        r_bare, r_exp, r_ph = st.ratio("user_bare"), st.ratio("user_explicit"), st.ratio("phone")
        if hdr and (r_bare + r_ph) >= 0.15:
            return 2 + r_bare
        return r_exp if r_exp >= 0.5 else 0

    if ov.phone:
        plan.phone_cols = [_resolve_col(r, headers) for r in ov.phone]
    else:
        scored = sorted((-round(phone_score(s), 1), s.index) for s in stats)
        plan.phone_cols = [i for sc, i in scored if sc < 0]
    if ov.username:
        plan.username_cols = [_resolve_col(r, headers) for r in ov.username]
    else:
        scored = sorted((-round(user_score(s), 1), s.index) for s in stats)
        plan.username_cols = [i for sc, i in scored if sc < 0]

    # ---- name columns ----------------------------------------------------
    contact = set(plan.phone_cols) | set(plan.username_cols)
    for st in stats:
        if st.index in contact or st.ratio("name_like") < 0.5:
            continue
        role, order = header_name_role(st.header)
        if role == "full" and plan.full_name_col is None:
            plan.full_name_col, plan.name_order = st.index, order
        elif role == "first" and plan.first_name_col is None:
            plan.first_name_col = st.index
        elif role == "last" and plan.last_name_col is None:
            plan.last_name_col = st.index
        elif role == "greeting" and plan.greeting_col is None:
            plan.greeting_col = st.index
    if plan.first_name_col is not None and plan.full_name_col is None and \
            plan.last_name_col is None:
        # A lone "Имя" column very often holds the full name.
        st = stats[plan.first_name_col]
        multi = sum(1 for r in data[:STAT_SAMPLE_ROWS]
                    if plan.first_name_col < len(r) and len(r[plan.first_name_col].split()) >= 2)
        if st.filled and multi / st.filled >= 0.5:
            plan.full_name_col, plan.first_name_col = plan.first_name_col, None
    if all(c is None for c in (plan.full_name_col, plan.first_name_col,
                               plan.last_name_col, plan.greeting_col)):
        cands = [s for s in stats if s.index not in contact
                 and s.ratio("name_like") >= 0.7 and not header_is_consent(s.header)
                 and "mail" not in s.header.lower()]
        if cands:
            best = max(cands, key=lambda s: (s.ratio("name_like"), -s.index))
            plan.full_name_col = best.index

    if ov.name:
        plan.full_name_col = _resolve_col(ov.name, headers)
    if ov.first_name:
        plan.first_name_col = _resolve_col(ov.first_name, headers)
    if ov.last_name:
        plan.last_name_col = _resolve_col(ov.last_name, headers)
    if ov.name_order:
        plan.name_order = ov.name_order

    # ---- consent ---------------------------------------------------------
    if ov.consent:
        plan.consent_col = _resolve_col(ov.consent, headers)
    else:
        for st in stats:
            if header_is_consent(st.header) and st.index not in plan.role_cols:
                plan.consent_col = st.index
                break
    return plan


# --------------------------------------------------------------------------- #
# Row extraction
# --------------------------------------------------------------------------- #

@dataclass
class ParsedContact:
    row_no: int                    # 1-based row number in the sheet
    name: str
    first_name: str
    phones: list[str]
    username: str | None
    extra: dict[str, str]

    @property
    def key(self) -> str:
        return self.phones[0] if self.phones else "@" + (self.username or "").lower()


@dataclass
class ParseResult:
    sheet: str
    plan: ColumnPlan
    contacts: list[ParsedContact]
    rejected: list[dict]
    duplicates: list[dict]
    total_rows: int

    def summary(self) -> str:
        with_phone = sum(1 for c in self.contacts if c.phones)
        only_user = sum(1 for c in self.contacts if not c.phones and c.username)
        with_name = sum(1 for c in self.contacts if c.first_name)
        return (
            f"Лист «{self.sheet}»: строк с данными {self.total_rows}\n"
            f"  ✓ к рассылке:       {len(self.contacts)}  "
            f"(с телефоном {with_phone}, только @username {only_user}, с именем {with_name})\n"
            f"  ↺ дубликаты:        {len(self.duplicates)}\n"
            f"  ✗ отброшено:        {len(self.rejected)}"
        )


def _cell(row: list[str], i: int | None) -> str:
    return row[i].strip() if i is not None and i < len(row) else ""


def parse_rows(sheet: Sheet, plan: ColumnPlan, region: str) -> ParseResult:
    if not plan.phone_cols and not plan.username_cols:
        raise ValueError(
            "Не нашёл ни колонки с телефонами, ни колонки с Telegram.\n"
            "Укажи явно: --phone-col \"Название\" и/или --username-col \"Название\".\n"
            f"Колонки файла: {plan.headers}")
    start = plan.header_row + 1 if plan.header_row is not None else 0
    contacts: list[ParsedContact] = []
    rejected: list[dict] = []
    duplicates: list[dict] = []
    seen_phone: dict[str, int] = {}
    seen_user: dict[str, int] = {}
    total = 0

    for offset, row in enumerate(sheet.rows[start:]):
        row_no = start + offset + 1
        if not any(row):
            continue
        if plan.header_row is not None and [c.strip() for c in row] == \
                [c.strip() for c in sheet.rows[plan.header_row]]:
            continue  # repeated header (concatenated exports)
        total += 1
        raw_preview = " | ".join(c for c in row if c)[:200]

        phones: list[str] = []
        username: str | None = None
        for i in plan.phone_cols:
            v = _cell(row, i)
            for p in N.extract_phones(v, region) + ([N.phone_from_tme_link(v, region)] if "t.me/+" in v else []):
                if p and p not in phones:
                    phones.append(p)
        for i in plan.username_cols:
            v = _cell(row, i)
            u = N.extract_username(v, bare_ok=True)
            if u and not username:
                username = u
            if not u:
                for p in N.extract_phones(v, region) + [N.phone_from_tme_link(v, region)]:
                    if p and p not in phones:
                        phones.append(p)
        if not username:
            # Combined "phone or @username" fields.
            for i in plan.phone_cols:
                u = N.extract_username(_cell(row, i), bare_ok=False)
                if u:
                    username = u
                    break

        full = N.clean_name(_cell(row, plan.full_name_col))
        first = N.clean_name(_cell(row, plan.first_name_col))
        last = N.clean_name(_cell(row, plan.last_name_col))
        greet = N.clean_name(_cell(row, plan.greeting_col))
        name = full or " ".join(x for x in (first, last) if x) or greet
        if greet:
            first_name = N.first_name_from_full(greet, "given_first") if len(greet.split()) > 1 \
                else N._titlecase(greet)
        elif first:
            first_name = N.first_name_from_full(first, "given_first")
        elif full:
            first_name = N.first_name_from_full(full, plan.name_order)
        else:
            first_name = ""

        extra = {}
        for i, h in enumerate(plan.headers):
            if i in plan.role_cols:
                continue
            v = _cell(row, i)
            if v:
                extra[h] = v

        if not phones and not username:
            rejected.append({"row": row_no, "reason": "нет ни телефона, ни @username",
                             "data": raw_preview})
            continue
        if plan.consent_col is not None and N.is_negative_answer(_cell(row, plan.consent_col)):
            rejected.append({"row": row_no,
                             "reason": f"отказ от рассылки («{plan.headers[plan.consent_col]}»)",
                             "data": raw_preview})
            continue

        dup_of = next((seen_phone[p] for p in phones if p in seen_phone), None)
        if dup_of is None and username and username.lower() in seen_user:
            dup_of = seen_user[username.lower()]
        if dup_of is not None:
            duplicates.append({"row": row_no, "duplicate_of_row": dup_of, "data": raw_preview})
            # Fill gaps on the first occurrence (e.g. username only in the 2nd row).
            orig = next(c for c in contacts if c.row_no == dup_of)
            if not orig.username and username:
                orig.username = username
                seen_user[username.lower()] = dup_of
            for p in phones:
                if p not in orig.phones:
                    orig.phones.append(p)
                    seen_phone[p] = dup_of
            if not orig.first_name and first_name:
                orig.first_name, orig.name = first_name, name
            continue

        c = ParsedContact(row_no=row_no, name=name, first_name=first_name,
                          phones=phones, username=username, extra=extra)
        contacts.append(c)
        for p in phones:
            seen_phone[p] = row_no
        if username:
            seen_user[username.lower()] = row_no

    return ParseResult(sheet=sheet.name, plan=plan, contacts=contacts,
                       rejected=rejected, duplicates=duplicates, total_rows=total)


def pick_sheet(sheets: list[Sheet], region: str, name: str | None = None) -> Sheet:
    if name:
        for s in sheets:
            if s.name == name or s.name.lower() == name.lower():
                return s
        if name.isdigit() and 1 <= int(name) <= len(sheets):
            return sheets[int(name) - 1]
        raise ValueError(f"Лист «{name}» не найден. Есть: {[s.name for s in sheets]}")
    if len(sheets) == 1:
        return sheets[0]

    def score(s: Sheet) -> int:
        try:
            r = parse_rows(s, build_plan(s, region), region)
            return len(r.contacts)
        except ValueError:
            return -1
    return max(sheets, key=score)


def parse_table(sheets: list[Sheet], region: str = "UZ", sheet: str | None = None,
                overrides: Overrides | None = None) -> ParseResult:
    sh = pick_sheet(sheets, region, sheet)
    plan = build_plan(sh, region, overrides)
    return parse_rows(sh, plan, region)
