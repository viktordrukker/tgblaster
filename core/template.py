"""Template rendering with {name}, {phone}, and extras.

Uses str.format_map with a safe dict that returns the literal placeholder
when the key is missing — so typos don't crash the whole campaign.
"""
from __future__ import annotations

import json
import re
from typing import Mapping


_PLACEHOLDER_RE = re.compile(r"\{([a-zA-Z_][a-zA-Z0-9_]*)\}")


class _SafeDict(dict):
    def __missing__(self, key):
        return "{" + key + "}"


def placeholders(template: str) -> list[str]:
    """Return the ordered unique list of placeholders in the template."""
    seen: list[str] = []
    for m in _PLACEHOLDER_RE.finditer(template):
        key = m.group(1)
        if key not in seen:
            seen.append(key)
    return seen


def render(template: str, contact_row: Mapping) -> str:
    """Render the template against a contact row (sqlite Row or dict).

    Known fields: name, phone, tg_username. Extras come from extra_json.
    """
    data = {
        "name": (contact_row["name"] or "").strip() if contact_row.get("name") else "",
        "phone": contact_row.get("phone") or "",
        "tg_username": contact_row.get("tg_username") or "",
    }
    # First name only is useful more often than the full name.
    if data["name"]:
        data["first_name"] = data["name"].split()[0]
    else:
        data["first_name"] = ""

    extras = contact_row.get("extra_json")
    if extras:
        try:
            parsed = json.loads(extras) if isinstance(extras, str) else extras
            if isinstance(parsed, dict):
                for k, v in parsed.items():
                    data.setdefault(k, v)
        except (ValueError, TypeError):
            pass

    return template.format_map(_SafeDict(data))
