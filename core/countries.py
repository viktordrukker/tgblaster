"""Country presets for the region-selector dropdowns.

Grouped by CIS / MENA / APAC / Other so users picking a default region for
phone parsing find what they need without a 250-item ISO list. ISO-3166
alpha-2 codes are what `phonenumbers` expects as the default region.
"""
from __future__ import annotations


# (iso_code, human_label, group). Order here is the display order.
COUNTRIES: list[tuple[str, str, str]] = [
    # CIS / post-Soviet
    ("RU", "🇷🇺 Россия",          "CIS"),
    ("UA", "🇺🇦 Украина",         "CIS"),
    ("BY", "🇧🇾 Беларусь",        "CIS"),
    ("KZ", "🇰🇿 Казахстан",       "CIS"),
    ("UZ", "🇺🇿 Узбекистан",      "CIS"),
    ("KG", "🇰🇬 Кыргызстан",      "CIS"),
    ("TJ", "🇹🇯 Таджикистан",     "CIS"),
    ("TM", "🇹🇲 Туркменистан",    "CIS"),
    ("AM", "🇦🇲 Армения",         "CIS"),
    ("AZ", "🇦🇿 Азербайджан",     "CIS"),
    ("GE", "🇬🇪 Грузия",          "CIS"),
    ("MD", "🇲🇩 Молдова",         "CIS"),

    # MENA
    ("TR", "🇹🇷 Турция",          "MENA"),
    ("AE", "🇦🇪 ОАЭ",             "MENA"),
    ("SA", "🇸🇦 Саудовская Аравия", "MENA"),
    ("QA", "🇶🇦 Катар",           "MENA"),
    ("KW", "🇰🇼 Кувейт",          "MENA"),
    ("BH", "🇧🇭 Бахрейн",         "MENA"),
    ("OM", "🇴🇲 Оман",            "MENA"),
    ("IL", "🇮🇱 Израиль",         "MENA"),
    ("JO", "🇯🇴 Иордания",        "MENA"),
    ("LB", "🇱🇧 Ливан",           "MENA"),
    ("IQ", "🇮🇶 Ирак",            "MENA"),
    ("IR", "🇮🇷 Иран",            "MENA"),
    ("SY", "🇸🇾 Сирия",           "MENA"),
    ("YE", "🇾🇪 Йемен",           "MENA"),
    ("PS", "🇵🇸 Палестина",       "MENA"),
    ("EG", "🇪🇬 Египет",          "MENA"),
    ("MA", "🇲🇦 Марокко",         "MENA"),
    ("DZ", "🇩🇿 Алжир",           "MENA"),
    ("TN", "🇹🇳 Тунис",           "MENA"),
    ("LY", "🇱🇾 Ливия",           "MENA"),

    # APAC
    ("CN", "🇨🇳 Китай",           "APAC"),
    ("HK", "🇭🇰 Гонконг",         "APAC"),
    ("TW", "🇹🇼 Тайвань",         "APAC"),
    ("JP", "🇯🇵 Япония",          "APAC"),
    ("KR", "🇰🇷 Южная Корея",     "APAC"),
    ("MN", "🇲🇳 Монголия",        "APAC"),
    ("IN", "🇮🇳 Индия",           "APAC"),
    ("PK", "🇵🇰 Пакистан",        "APAC"),
    ("BD", "🇧🇩 Бангладеш",       "APAC"),
    ("LK", "🇱🇰 Шри-Ланка",       "APAC"),
    ("NP", "🇳🇵 Непал",           "APAC"),
    ("ID", "🇮🇩 Индонезия",       "APAC"),
    ("PH", "🇵🇭 Филиппины",       "APAC"),
    ("VN", "🇻🇳 Вьетнам",         "APAC"),
    ("TH", "🇹🇭 Таиланд",         "APAC"),
    ("MY", "🇲🇾 Малайзия",        "APAC"),
    ("SG", "🇸🇬 Сингапур",        "APAC"),
    ("MM", "🇲🇲 Мьянма",          "APAC"),
    ("KH", "🇰🇭 Камбоджа",        "APAC"),
    ("LA", "🇱🇦 Лаос",            "APAC"),
    ("AU", "🇦🇺 Австралия",       "APAC"),
    ("NZ", "🇳🇿 Новая Зеландия",  "APAC"),

    # Other commonly-needed
    ("US", "🇺🇸 США",             "Other"),
    ("GB", "🇬🇧 Великобритания",  "Other"),
    ("DE", "🇩🇪 Германия",        "Other"),
    ("FR", "🇫🇷 Франция",         "Other"),
    ("ES", "🇪🇸 Испания",         "Other"),
    ("IT", "🇮🇹 Италия",          "Other"),
    ("PL", "🇵🇱 Польша",          "Other"),
    ("NL", "🇳🇱 Нидерланды",      "Other"),
    ("CA", "🇨🇦 Канада",          "Other"),
]


ISO_CODES: list[str] = [c[0] for c in COUNTRIES]
_LABEL_BY_ISO: dict[str, str] = {c[0]: f"{c[1]}  ·  {c[2]}" for c in COUNTRIES}


def label_for(iso: str) -> str:
    """Human label for a given ISO alpha-2 code (falls back to the code)."""
    return _LABEL_BY_ISO.get(iso, iso)
