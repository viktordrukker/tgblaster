import datetime as dt
import random
from types import SimpleNamespace

import pytest

from blaster.config import Pacing
from blaster.db import Database
from blaster.importer import ParsedContact


def contact(row, name="Азиз Каримов", first="Азиз", phones=None, username=None, extra=None):
    return ParsedContact(row_no=row, name=name, first_name=first,
                         phones=phones if phones is not None else [f"+9989012345{row:02d}"],
                         username=username, extra=extra or {})


@pytest.fixture
def db(tmp_path):
    d = Database(tmp_path / "t.db")
    yield d
    d.close()


def fill(db, n=3, list_name="L", resolve=True, **kw):
    db.upsert_contacts(list_name, [contact(i + 2, **kw) for i in range(n)])
    rows = db.contacts(list_name)
    if resolve:
        for r in rows:
            db.mark_resolved(r["id"], 1000 + r["id"], 555, None, None, "test")
    return db.contacts(list_name)


@pytest.fixture
def pacing():
    return Pacing(min_delay=1, max_delay=1, long_pause_every=100, long_pause_min=5,
                  long_pause_max=5, daily_cap=1000, window_start=dt.time(0, 0),
                  window_end=dt.time(0, 0), rng=random.Random(1))


class FakeClient:
    """Minimal Telethon stand-in. `script[user_id]` is a list of exceptions
    (or None for success) consumed one per send attempt."""

    def __init__(self, script=None, me_id=1):
        self.sent = []
        self.attempts = []
        self.script = script or {}
        self.connected = True
        self.me_id = me_id

    def is_connected(self):
        return self.connected

    async def connect(self):
        self.connected = True

    async def disconnect(self):
        self.connected = False

    async def get_me(self):
        return SimpleNamespace(id=self.me_id, username="me", first_name="Me")

    async def send_message(self, peer, text, **kw):
        uid = getattr(peer, "user_id", peer)
        self.attempts.append(uid)
        beh = self.script.get(uid)
        if beh:
            exc = beh.pop(0)
            if exc is not None:
                raise exc
        self.sent.append((uid, text, kw))
        return SimpleNamespace(id=len(self.sent), photo=("PHOTO" if kw.get("file") else None),
                               document=None)


@pytest.fixture
def fake_client():
    return FakeClient
