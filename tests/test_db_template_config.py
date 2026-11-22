import datetime as dt
import random

import pytest

from blaster.config import Pacing, Settings
from blaster.db import Database, LockHeld
from blaster.template import placeholders, render, validate
from conftest import contact, fill


# ---------------------------------------------------------------- template --

def test_render_fallbacks_and_extras():
    c = {"name": "Азиз Каримов", "first_name": "", "tg_first_name": None,
         "extra": {"Компания / место работы": "EPAM"}}
    t = "Привет, {first_name|друг}! {name}. Работаешь в {Компания / место работы|—}. {{x}}"
    assert render(t, c) == "Привет, друг! Азиз Каримов. Работаешь в EPAM. {x}"


def test_render_tg_first_name_fallback():
    assert render("{first_name|друг}", {"first_name": "", "tg_first_name": "Aziz"}) == "Aziz"


def test_validate():
    assert validate("Привет, {first_name}", set(), has_image=False) == []
    assert validate("{компания}", {"Компания"}, has_image=False) == []
    probs = validate("Привет, {frist_name}", set(), has_image=False)
    assert probs and "frist_name" in probs[0]
    assert validate("a { b", set(), has_image=False)
    assert validate("x" * 1500, set(), has_image=True)
    assert validate("", set(), has_image=False)


def test_placeholders():
    assert placeholders("{a} {b|x} {a}") == [("a", None), ("b", "x")]


# ---------------------------------------------------------------------- db --

def test_reimport_keeps_resolution_and_adds_new(db):
    fill(db, 2)
    ins, upd = db.upsert_contacts("L", [contact(2), contact(3), contact(10)])
    assert (ins, upd) == (1, 2)
    st = db.list_stats("L")
    assert st["resolved"] == 2 and st["pending"] == 1


def test_reimport_with_new_contact_info_retries_not_found(db):
    db.upsert_contacts("L", [contact(2, phones=["+998901111111"])])
    cid = db.contacts("L")[0]["id"]
    db.mark_not_found(cid, "x")
    db.upsert_contacts("L", [contact(2, phones=["+998901111111"], username="aziz_k")])
    assert db.contacts("L")[0]["status"] == "new"


def test_lists_are_isolated(db):
    fill(db, 2, list_name="A")
    fill(db, 1, list_name="B")
    assert db.list_stats("A")["total"] == 2 and db.list_stats("B")["total"] == 1


def test_lock(tmp_path):
    a = Database(tmp_path / "x.db")
    b = Database(tmp_path / "x.db")
    b._owner = "other:1"
    a.acquire_lock()
    with pytest.raises(LockHeld):
        b.acquire_lock()
    a.release_lock()
    b.acquire_lock()
    a.close(), b.close()


def test_stale_lock_is_taken_over(tmp_path):
    a = Database(tmp_path / "x.db")
    a.conn.execute("INSERT INTO run_lock VALUES ('telegram','dead:1',0)")
    a.acquire_lock()
    a.close()


def test_report(db):
    rows = fill(db, 2)
    cid = db.create_campaign("c", "L", "hi", None)
    db.reserve(cid, rows[0])
    db.finish(cid, rows[0]["id"], "sent", "", 77)
    rep = db.report(cid)
    assert [r["send_status"] for r in rep] == ["sent", None]
    assert rep[0]["message_id"] == 77


# ------------------------------------------------------------------ config --

def test_settings_from_env(tmp_path):
    s = Settings.load({"TG_API_ID": "123", "TG_API_HASH": "h", "TG_SESSION_NAME": "meetup",
                       "DAILY_CAP": "50", "SEND_WINDOW": "10:00-20:30",
                       "BLASTER_HOME": str(tmp_path)})
    assert s.api_id == 123 and s.session_path == tmp_path / "sessions" / "meetup.session"
    assert s.pacing.daily_cap == 50 and s.pacing.window_end == dt.time(20, 30)
    assert s.db_path == tmp_path / "data" / "blaster.db"
    assert s.region == "UZ"


def test_bad_pacing():
    with pytest.raises(ValueError):
        Pacing(min_delay=10, max_delay=5)


@pytest.mark.parametrize("hour,expected_zero", [(8, False), (9, True), (20, True), (21, False),
                                                (23, False)])
def test_window(hour, expected_zero):
    from zoneinfo import ZoneInfo
    p = Pacing(rng=random.Random(0))
    now = dt.datetime(2026, 11, 22, hour, 30, tzinfo=ZoneInfo("Asia/Tashkent"))
    w = p.seconds_until_window(now)
    assert (w == 0) is expected_zero
    if hour == 23:
        assert w == int(dt.timedelta(hours=9, minutes=30).total_seconds())


def test_optouts_listed_newest_first(db):
    db.add_optout("+998901234567", "попросил")
    db.add_optout("@User", "")
    rows = db.optouts()
    assert [r["value"] for r in rows] == ["@user", "+998901234567"]
    assert rows[1]["reason"] == "попросил"
