import json

import openpyxl
import pytest

from blaster import tables
from blaster.importer import Overrides, detect_header_row, header_name_role, parse_table
from blaster.tables import Sheet


def sheet(rows, name="s"):
    return [Sheet(name, tables._trim([[str(c) for c in r] for r in rows]))]


def by_row(result):
    return {c.row_no: c for c in result.contacts}


# --------------------------------------------------------------------------- #
# Layouts seen in the wild
# --------------------------------------------------------------------------- #

def test_google_forms_export():
    rows = [
        ["Отметка времени", "Ваше имя и фамилия", "Номер телефона", "Ваш ник в Telegram",
         "Компания", "Согласие на получение рассылки"],
        ["20.11.2026 10:00:00", "Азиз Каримов", "+998 90 123 45 67", "@aziz_k", "EPAM", "Да"],
        ["20.11.2026 11:00:00", "Иванов Иван Иванович", "93 555 12 34", "", "Uzum", "Да"],
        ["20.11.2026 12:00:00", "Dilnoza", "", "https://t.me/dilnoza_r", "", "Да"],
        ["20.11.2026 13:00:00", "No Consent", "+998 97 111 22 33", "", "", "Нет"],
        ["20.11.2026 14:00:00", "Nobody", "нет", "нет", "", "Да"],
    ]
    r = parse_table(sheet(rows))
    p = r.plan
    assert p.header_row == 0
    assert p.headers[p.phone_cols[0]] == "Номер телефона"
    assert p.headers[p.username_cols[0]] == "Ваш ник в Telegram"
    assert p.headers[p.full_name_col] == "Ваше имя и фамилия"
    assert p.headers[p.consent_col].startswith("Согласие")
    c = by_row(r)
    assert c[2].first_name == "Азиз" and c[2].phones == ["+998901234567"] and c[2].username == "aziz_k"
    assert c[3].first_name == "Иван" and c[3].phones == ["+998935551234"]
    assert c[4].phones == [] and c[4].username == "dilnoza_r"
    assert c[2].extra["Компания"] == "EPAM"
    assert len(r.contacts) == 3
    reasons = sorted(x["reason"] for x in r.rejected)
    assert any("отказ" in x for x in reasons) and any("нет ни" in x for x in reasons)


def test_title_rows_above_header_and_fio():
    rows = [
        ["Agile Uzbekistan Meetup #12 — список участников"],
        [""],
        ["№", "ФИО", "Телефон", "Telegram"],
        ["1", "Rustamov Jasur Akmal o'g'li", "998901112233", "jasur_r"],
        ["2", "Петрова Анна Сергеевна", "+998 (91) 222-33-44", ""],
    ]
    r = parse_table(sheet(rows))
    assert r.plan.header_row == 2
    assert r.plan.name_order == "surname_first"
    c = by_row(r)
    assert c[4].first_name == "Jasur" and c[4].username == "jasur_r"
    assert c[5].first_name == "Анна" and c[5].phones == ["+998912223344"]
    # "№" must never be taken for a phone column
    assert r.plan.headers[r.plan.phone_cols[0]] == "Телефон"


def test_separate_first_last_name_columns():
    rows = [["First name", "Last name", "Phone"],
            ["aziz", "KARIMOV", "+998901234567"]]
    r = parse_table(sheet(rows))
    c = r.contacts[0]
    assert c.first_name == "Aziz" and c.name == "aziz KARIMOV"


def test_lone_name_column_with_full_names_is_treated_as_full():
    rows = [["Имя", "Телефон"],
            ["Каримов Азиз", "+998901234567"],
            ["Иванова Мария", "+998901234568"]]
    r = parse_table(sheet(rows))
    assert r.plan.full_name_col == 0
    assert [c.first_name for c in r.contacts] == ["Азиз", "Мария"]


def test_greeting_column_wins():
    rows = [["ФИО", "Как к вам обращаться?", "Телефон"],
            ["Каримов Азиз Рустамович", "Азиз-ака", "+998901234567"]]
    r = parse_table(sheet(rows))
    assert r.contacts[0].first_name == "Азиз-ака"


def test_no_header_row_detected_by_content():
    rows = [["Азиз Каримов", "+998901234567", "@aziz_k"],
            ["Мария Иванова", "+998901234568", "@maria_i"],
            ["Jasur", "901112233", ""]]
    r = parse_table(sheet(rows))
    assert r.plan.header_row is None
    assert len(r.contacts) == 3
    assert r.plan.phone_cols == [1] and r.plan.username_cols == [2]
    assert r.plan.full_name_col == 0
    assert by_row(r)[1].first_name == "Азиз"


def test_combined_phone_or_username_column():
    rows = [["Имя", "Телефон или ник в телеграм"],
            ["Азиз", "+998901234567"],
            ["Мария", "@maria_i"],
            ["Олег", "oleg_tg"],
            ["Анна", "https://t.me/+998935551234"]]
    r = parse_table(sheet(rows))
    c = by_row(r)
    assert c[2].phones == ["+998901234567"]
    assert c[3].username == "maria_i"
    assert c[4].username == "oleg_tg"
    assert c[5].phones == ["+998935551234"]


def test_phone_in_telegram_column():
    rows = [["Name", "Telegram"],
            ["Aziz", "+998 90 123 45 67"],
            ["Maria", "@maria_i"]]
    r = parse_table(sheet(rows))
    c = by_row(r)
    assert c[2].phones == ["+998901234567"] and c[2].username is None
    assert c[3].username == "maria_i"


def test_two_phone_columns_and_alternates():
    rows = [["Имя", "Телефон", "Доп. телефон"],
            ["Азиз", "+998901234567, +998935551234", "+998971112233"]]
    r = parse_table(sheet(rows))
    assert r.contacts[0].phones == ["+998901234567", "+998935551234", "+998971112233"]


def test_dedup_by_phone_and_username_and_merge():
    rows = [["Имя", "Телефон", "Telegram"],
            ["Азиз", "+998901234567", ""],
            ["Азиз К", "998901234567", "@aziz_k"],      # same phone → dup, fills username
            ["Мария", "", "@Maria_I"],
            ["Мария 2", "", "maria_i"]]                 # same username, other case
    r = parse_table(sheet(rows))
    assert len(r.contacts) == 2
    assert len(r.duplicates) == 2
    assert by_row(r)[2].username == "aziz_k"


def test_repeated_header_rows_are_ignored():
    rows = [["Имя", "Телефон"], ["Азиз", "+998901234567"],
            ["Имя", "Телефон"], ["Мария", "+998901234568"]]
    r = parse_table(sheet(rows))
    assert len(r.contacts) == 2 and not r.rejected


def test_overrides():
    rows = [["a", "b", "c"], ["Азиз", "x", "+998901234567"]]
    r = parse_table(sheet(rows), overrides=Overrides(phone=["3"], name="a"))
    assert r.contacts[0].phones == ["+998901234567"]
    assert r.contacts[0].first_name == "Азиз"
    with pytest.raises(ValueError):
        parse_table(sheet(rows), overrides=Overrides(phone=["нет такой"]))


def test_no_contact_columns_raises_helpful_error():
    rows = [["Имя", "Город"], ["Азиз", "Ташкент"]]
    with pytest.raises(ValueError, match="--phone-col"):
        parse_table(sheet(rows))


def test_picks_sheet_with_contacts():
    s1 = Sheet("Сводка", [["Итого", "15"]])
    s2 = Sheet("Участники", [["Имя", "Телефон"], ["Азиз", "+998901234567"]])
    r = parse_table([s1, s2])
    assert r.sheet == "Участники"
    r = parse_table([s1, s2], sheet="2")
    assert r.sheet == "Участники"


@pytest.mark.parametrize("h,role", [
    ("ФИО", "full"), ("Ф.И.О.", "full"), ("Имя и фамилия", "full"), ("Full name", "full"),
    ("Имя", "first"), ("First name", "first"), ("Ism", "first"),
    ("Фамилия", "last"), ("Familiya", "last"), ("Как к вам обращаться?", "greeting"),
    ("Name", "full"), ("Телефон", None), ("Компания", None),
])
def test_header_name_role(h, role):
    assert header_name_role(h)[0] == role


def test_detect_header_row_skips_data():
    assert detect_header_row([["Азиз", "+998901234567"]]) is None


# --------------------------------------------------------------------------- #
# File formats
# --------------------------------------------------------------------------- #

def test_xlsx_numbers_hidden_sheet(tmp_path):
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "Участники"
    ws.append(["Регистрация на митап"])
    ws.append(["Имя", "Телефон", "Telegram"])
    ws.append(["Азиз", 998901234567, "@aziz_k"])        # phone stored as a number
    ws.append(["Мария", 901234568.0, None])
    hidden = wb.create_sheet("old")
    hidden.append(["Имя", "Телефон"])
    hidden.append(["X", "+998900000000"])
    hidden.sheet_state = "hidden"
    p = tmp_path / "c.xlsx"
    wb.save(p)
    sheets = tables.load(p)
    assert [s.name for s in sheets] == ["Участники"]
    r = parse_table(sheets)
    assert [c.phones for c in r.contacts] == [["+998901234567"], ["+998901234568"]]


@pytest.mark.parametrize("enc,delim", [("utf-8-sig", ","), ("cp1251", ";"), ("utf-16", "\t"),
                                      ("utf-8", "|")])
def test_csv_encodings_and_delimiters(tmp_path, enc, delim):
    text = delim.join(["Имя", "Телефон", "Компания"]) + "\n" + \
        delim.join(["Азиз", "+998 90 123 45 67", "Ромашка, ООО" if delim != "," else "Ромашка"]) + "\n"
    p = tmp_path / "c.csv"
    p.write_bytes(text.encode(enc))
    r = parse_table(tables.load(p))
    assert r.contacts[0].first_name == "Азиз"
    assert r.contacts[0].phones == ["+998901234567"]


def test_json(tmp_path):
    p = tmp_path / "c.json"
    p.write_text(json.dumps([{"name": "Aziz", "phone": "+998901234567"},
                             {"name": "Maria", "telegram": "@maria_i"}]), encoding="utf-8")
    r = parse_table(tables.load(p))
    assert len(r.contacts) == 2


def test_html_instead_of_table(tmp_path):
    p = tmp_path / "x.csv"
    p.write_bytes(b"<!DOCTYPE html><html>login</html>")
    with pytest.raises(ValueError, match="HTML"):
        tables.load(p)


@pytest.mark.parametrize("url,expected", [
    ("https://docs.google.com/spreadsheets/d/AbC-123_x/edit#gid=0",
     "https://docs.google.com/spreadsheets/d/AbC-123_x/export?format=xlsx"),
    ("https://drive.google.com/file/d/XyZ/view?usp=sharing",
     "https://drive.google.com/uc?export=download&id=XyZ"),
    ("https://example.com/a.csv", "https://example.com/a.csv"),
])
def test_normalize_url(url, expected):
    assert tables.normalize_url(url) == expected
