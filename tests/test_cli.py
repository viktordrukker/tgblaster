import openpyxl
import pytest

from blaster import cli
from blaster.db import Database


@pytest.fixture
def home(tmp_path, monkeypatch):
    monkeypatch.setenv("BLASTER_HOME", str(tmp_path))
    monkeypatch.delenv("DB_PATH", raising=False)
    monkeypatch.delenv("DATA_DIR", raising=False)
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.append(["Agile Uzbekistan — регистрация"])
    ws.append(["ФИО", "Телефон", "Telegram", "Компания"])
    ws.append(["Каримов Азиз", 998901234567, "@aziz_k", "EPAM"])
    ws.append(["Иванова Мария Петровна", "93 555 12 34", "", ""])
    ws.append(["Без контактов", "", "", ""])
    wb.save(tmp_path / "c.xlsx")
    (tmp_path / "t.md").write_text("Привет, {first_name|друзья}! **{компания|}**", encoding="utf-8")
    return tmp_path


def test_offline_flow(home, capsys):
    f = str(home / "c.xlsx")
    assert cli.main(["inspect", f]) == 0
    out = capsys.readouterr().out
    assert "к рассылке:       2" in out and "«Телефон»" in out

    assert cli.main(["import", f, "--list", "dec", "--yes"]) == 0
    assert (home / "data" / "reports").exists()
    assert cli.main(["lists"]) == 0
    assert "dec" in capsys.readouterr().out

    db = Database(home / "data" / "blaster.db")
    for r in db.contacts("dec"):
        db.mark_resolved(r["id"], 100 + r["id"], 1, None, None, "t")
    db.close()

    assert cli.main(["campaign", "create", "inv", "--list", "dec",
                     "--template", str(home / "t.md")]) == 0
    capsys.readouterr()
    assert cli.main(["preview", "inv", "-n", "2"]) == 0
    out = capsys.readouterr().out
    assert "Привет, Азиз! **EPAM**" in out and "Привет, Мария!" in out
    assert cli.main(["optout", "+998 93 555 12 34"]) == 0
    assert cli.main(["status", "inv"]) == 0
    assert "осталось 1" in capsys.readouterr().out
    assert cli.main(["report", "inv"]) == 0
    assert (home / "data" / "reports" / "report-inv.csv").exists()


def test_import_needs_yes_without_tty(home, monkeypatch, capsys):
    monkeypatch.setattr("sys.stdin.isatty", lambda: False)
    assert cli.main(["import", str(home / "c.xlsx"), "--list", "x"]) == 1


def test_bad_template_flagged_on_create(home, capsys):
    cli.main(["import", str(home / "c.xlsx"), "--list", "dec", "--yes"])
    (home / "bad.md").write_text("Привет {frist_name}", encoding="utf-8")
    assert cli.main(["campaign", "create", "b", "--list", "dec",
                     "--template", str(home / "bad.md")]) == 1
    assert "frist_name" in capsys.readouterr().out
