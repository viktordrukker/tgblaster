"""Message templates.

Syntax:
    {first_name}            value or empty string
    {first_name|друзья}     value, or the fallback after '|' when empty
    {{ and }}               literal braces

Keys are matched case-insensitively against the contact's fields and
every column of the source table (header text, normalised via
`importer.placeholder_key`).
"""
from __future__ import annotations

import re
from typing import Mapping

from .importer import placeholder_key

_TOKEN_RE = re.compile(r"\{\{|\}\}|\{([^{}|]+?)(?:\|([^{}]*))?\}")

BUILTIN_KEYS = ("name", "first_name", "phone", "username")

# Telegram limits.
MAX_TEXT = 4096
MAX_CAPTION = 1024


class TemplateError(ValueError):
    pass


def placeholders(template: str) -> list[tuple[str, str | None]]:
    out = []
    for m in _TOKEN_RE.finditer(template):
        if m.group(1) is not None:
            item = (placeholder_key(m.group(1)), m.group(2))
            if item not in out:
                out.append(item)
    return out


def context_for(contact: Mapping) -> dict[str, str]:
    """Build the lookup dict for a contact row (db row or dict)."""
    ctx: dict[str, str] = {}
    extra = contact.get("extra") or {}
    for k, v in extra.items():
        ctx[placeholder_key(k)] = str(v)
    first = contact.get("first_name") or contact.get("tg_first_name") or ""
    ctx.update({
        "name": contact.get("name") or "",
        "first_name": first,
        "phone": contact.get("phone") or "",
        "username": ("@" + contact["username"]) if contact.get("username") else "",
    })
    return ctx


def render(template: str, contact: Mapping) -> str:
    ctx = context_for(contact)

    def sub(m: re.Match) -> str:
        tok = m.group(0)
        if tok == "{{":
            return "{"
        if tok == "}}":
            return "}"
        key = placeholder_key(m.group(1))
        val = ctx.get(key, "").strip()
        if not val and m.group(2) is not None:
            return m.group(2)
        return val

    return _TOKEN_RE.sub(sub, template)


def validate(template: str, available_keys: set[str], *, has_image: bool) -> list[str]:
    """Return a list of human-readable problems ([] == OK).

    Unknown placeholders are errors: we'd rather refuse to start than
    send "{compnay}" to 300 people.
    """
    problems = []
    if not template.strip():
        problems.append("шаблон пустой")
    known = set(BUILTIN_KEYS) | {placeholder_key(k) for k in available_keys}
    for key, _default in placeholders(template):
        if key not in known:
            problems.append(f"неизвестный плейсхолдер {{{key}}} — доступны: {sorted(known)}")
    # Stray single braces would be sent literally — usually a typo.
    stripped = _TOKEN_RE.sub("", template)
    if "{" in stripped or "}" in stripped:
        problems.append("непарная фигурная скобка (для литерала используй {{ или }})")
    limit = MAX_CAPTION if has_image else MAX_TEXT
    # Rough length check on the template itself; exact check happens per
    # contact in preview (rendered text without markdown markers).
    if len(template) > limit + 200:
        problems.append(f"текст длиннее лимита Telegram ({len(template)} > {limit}"
                        f"{' для подписи к картинке' if has_image else ''})")
    return problems
