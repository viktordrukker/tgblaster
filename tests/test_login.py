"""Web login runs in steps (phone → code → 2FA password), each on its own
short connection that holds the Telegram run lock."""
import asyncio
from types import SimpleNamespace

import pytest
from telethon import errors

from blaster import telegram as tg
from blaster.config import Settings
from blaster.db import Database, LockHeld


class LoginClient:
    def __init__(self, need_password=False, bad_code=False):
        self.need_password, self.bad_code = need_password, bad_code
        self.calls = []

    async def connect(self):
        self.calls.append("connect")

    def is_connected(self):
        return self.calls[-1:] != ["disconnect"]

    async def disconnect(self):
        self.calls.append("disconnect")

    async def send_code_request(self, phone):
        self.calls.append(("code", phone))
        return SimpleNamespace(phone_code_hash="HASH")

    async def sign_in(self, phone=None, code=None, *, password=None, phone_code_hash=None):
        self.calls.append(("sign_in", phone, code, password, phone_code_hash))
        if code and self.bad_code:
            raise errors.PhoneCodeInvalidError(None)
        if code and self.need_password:
            raise errors.SessionPasswordNeededError(None)
        return SimpleNamespace(id=7, first_name="Виктор", last_name="", username="vd", phone="998901234567")


def settings(tmp_path):
    return Settings.load(env={"BLASTER_HOME": str(tmp_path), "TG_API_ID": "1", "TG_API_HASH": "h"})


def test_code_then_password_flow():
    c = LoginClient(need_password=True)
    h = asyncio.run(tg.send_code(c, "+998 90 123-45-67"))
    assert h == "HASH" and ("code", "+998901234567") in c.calls
    assert asyncio.run(tg.sign_in_code(c, "+998901234567", " 12345 ", h)) is None   # password needed
    me = asyncio.run(tg.sign_in_password(c, "secret"))
    assert me.username == "vd"
    assert c.calls[-1][3] == "secret"


def test_code_without_2fa_returns_user():
    me = asyncio.run(tg.sign_in_code(LoginClient(), "+998901234567", "12345", "HASH"))
    assert me.id == 7


def test_errors_are_explained_in_russian():
    with pytest.raises(errors.PhoneCodeInvalidError) as e:
        asyncio.run(tg.sign_in_code(LoginClient(bad_code=True), "+1", "1", "HASH"))
    assert "Неверный код" in tg.explain(e.value)
    assert "2 мин" in tg.explain(errors.FloodWaitError(None, capture=120))
    assert "1 мин" in tg.explain(errors.FloodWaitError(None, capture=30))     # never "0 мин"


def test_connected_holds_and_releases_lock(tmp_path, monkeypatch):
    s = settings(tmp_path)
    db = Database(s.db_path)
    client = LoginClient()
    monkeypatch.setattr(tg, "make_client", lambda _s: client)

    async def go():
        async with tg.connected(s, db) as c:
            assert c is client
            assert db.conn.execute("SELECT COUNT(*) FROM run_lock").fetchone()[0] == 1
    asyncio.run(go())
    assert client.calls == ["connect", "disconnect"]
    assert db.conn.execute("SELECT COUNT(*) FROM run_lock").fetchone()[0] == 0


def test_connected_refuses_while_a_job_holds_the_lock(tmp_path, monkeypatch):
    s = settings(tmp_path)
    other = Database(s.db_path)
    other._owner = "elsewhere:1"
    other.acquire_lock()
    monkeypatch.setattr(tg, "make_client", lambda _s: LoginClient())

    async def go():
        async with tg.connected(s, Database(s.db_path)):
            pass
    with pytest.raises(LockHeld):
        asyncio.run(go())


def test_account_cache_roundtrip(tmp_path):
    s = settings(tmp_path)
    me = SimpleNamespace(id=7, first_name="Виктор", last_name=None, username="vd", phone="998901")
    tg.save_account(s, me)
    assert tg.load_account(s)["username"] == "vd"
    tg.save_account(s, None)
    assert tg.load_account(s) is None
