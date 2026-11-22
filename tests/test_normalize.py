import pytest

from blaster import normalize as N


@pytest.mark.parametrize("raw,expected", [
    ("+998 90 123 45 67", "+998901234567"),
    ("998901234567", "+998901234567"),
    ("998901234567.0", "+998901234567"),          # Excel float artefact
    ("90 123 45 67", "+998901234567"),             # national 9-digit
    ("(90) 123-45-67", "+998901234567"),
    ("901234567", "+998901234567"),
    ("8 90 123 45 67", "+998901234567"),           # old UZ trunk prefix
    ("+998-93-555-12-34", "+998935551234"),
    ("+7 916 123-45-67", "+79161234567"),
    ("89161234567", "+79161234567"),               # RU 8-prefix
    ("+7 701 123 4567", "+77011234567"),           # KZ
    ("00998901234567", "+998901234567"),
    (" +998 90 123 45 67", "+998901234567"),
])
def test_normalize_phone(raw, expected):
    assert N.normalize_phone(raw, "UZ") == expected


@pytest.mark.parametrize("raw", [
    "", None, "нет", "-", "12345", "9.98901E+11", "@ivan_petrov", "2026-11-20",
    "1234567890",            # an arbitrary 10-digit ID must not become +1…
])
def test_normalize_phone_rejects(raw):
    assert N.normalize_phone(raw, "UZ") is None


def test_multiple_phones_in_one_cell():
    assert N.extract_phones("+998901234567, 93 555 12 34", "UZ") == [
        "+998901234567", "+998935551234"]
    assert N.extract_phones("90 123 45 67 или 93 555 12 34", "UZ") == [
        "+998901234567", "+998935551234"]
    assert N.extract_phones("+998901234567 / +998901234567", "UZ") == ["+998901234567"]


def test_phone_in_free_text():
    assert N.extract_phones("звоните +998 90 123 45 67 после 18:00", "UZ") == ["+998901234567"]


@pytest.mark.parametrize("raw,expected", [
    ("@ivan_petrov", "ivan_petrov"),
    ("ivan_petrov", "ivan_petrov"),
    ("https://t.me/ivan_petrov", "ivan_petrov"),
    ("t.me/ivan_petrov?start=1", "ivan_petrov"),
    ("http://telegram.me/IvanP", "IvanP"),
    ("tg://resolve?domain=ivan_petrov", "ivan_petrov"),
    ("мой ник @ivan_petrov спасибо", "ivan_petrov"),
    ("@ivan_petrov.", "ivan_petrov"),
])
def test_extract_username(raw, expected):
    assert N.extract_username(raw) == expected


@pytest.mark.parametrize("raw", [
    "", None, "нет", "none", "-", "@ab", "https://t.me/+998901234567",
    "https://t.me/joinchat/AAAA", "@some_bot", "+998901234567", "ivan petrov",
    "1ivan", "ivan__petrov", "telegram",
])
def test_extract_username_rejects(raw):
    assert N.extract_username(raw) is None


def test_bare_username_needs_permission():
    assert N.extract_username("ivan_petrov", bare_ok=False) is None
    assert N.extract_username("@ivan_petrov", bare_ok=False) == "ivan_petrov"


def test_phone_from_tme_link():
    assert N.phone_from_tme_link("https://t.me/+998901234567") == "+998901234567"
    assert N.phone_from_tme_link("t.me/ivan") is None


@pytest.mark.parametrize("full,order,expected", [
    ("Иванов Иван Иванович", "auto", "Иван"),
    ("Karimov Aziz Rustamovich", "auto", "Aziz"),
    ("Karimov Aziz", "auto", "Aziz"),
    ("Aziz Karimov", "auto", "Aziz"),
    ("Азиз Каримов", "auto", "Азиз"),
    ("Каримова Дильноза", "auto", "Дильноза"),
    ("Rustamov Jasur Akmal o'g'li", "auto", "Jasur"),
    ("Марина Сергеевна", "auto", "Марина"),
    ("ivan", "auto", "Ivan"),
    ("IVANOV IVAN", "auto", "Ivan"),
    ("Petrov Ivan", "surname_first", "Ivan"),
    ("Ivan Petrov", "given_first", "Ivan"),
    ("Анна-Мария", "auto", "Анна-Мария"),
    ("", "auto", ""),
    ("123", "auto", ""),
])
def test_first_name_from_full(full, order, expected):
    assert N.first_name_from_full(full, order) == expected


@pytest.mark.parametrize("v,neg", [
    ("Нет", True), ("no", True), ("Yo'q", True), ("Отказываюсь", True),
    ("Да", False), ("Yes", False), ("", False), (None, False), ("Да, согласен", False),
])
def test_is_negative_answer(v, neg):
    assert N.is_negative_answer(v) is neg


def test_optout_key_understands_phone_username_and_id():
    from blaster.normalize import optout_key
    assert optout_key("90 123 45 67", "UZ") == "+998901234567"
    assert optout_key("@Some_User", "UZ") == "@Some_User"   # db lowercases on insert
    assert optout_key("id:123", "UZ") == "id:123"
    assert optout_key("id:abc", "UZ") is None
    assert optout_key("???", "UZ") is None
