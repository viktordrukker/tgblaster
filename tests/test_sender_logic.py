"""Tests for sender.run_campaign — end-to-end with a mocked TelegramClient.

We verify the non-network pieces: state transitions, resume support,
opt-out filtering, error classification, and stop-signal behaviour.
"""
import asyncio
import tempfile
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from core.database import Database
from core.rate_limiter import PacingConfig
from core.sender import run_campaign, StopSignal


class FakeClient:
    """Minimal async-compatible stand-in for a Telethon TelegramClient."""
    def __init__(self, behaviour=None):
        self.behaviour = behaviour or {}      # contact_id -> callable or "ok"
        self.sent = []                         # (user_id, text)
        self.get_me = AsyncMock(return_value=SimpleNamespace(id=999_000))
        self.is_connected = MagicMock(return_value=True)

    async def send_message(self, target, text, **kwargs):
        behaviour = self.behaviour.get(target, "ok")
        if callable(behaviour):
            behaviour()
        self.sent.append((target, text))
        return SimpleNamespace(id=1)

    async def send_file(self, target, file=None, caption=None, **kwargs):
        return await self.send_message(target, caption)


def _seed(db: Database, n: int = 3):
    rows = [
        {"name": f"user{i}", "phone": f"+7900000{str(i).zfill(4)}",
         "raw_phone": "", "extra_json": None}
        for i in range(n)
    ]
    db.upsert_contacts(rows)
    pending = db.pending_resolve()
    for i, row in enumerate(pending):
        db.mark_resolved(row["id"], 1_000_000 + i, f"user{i}", 42)


@pytest.fixture
def tmp_db():
    with tempfile.TemporaryDirectory() as tmp:
        yield Database(Path(tmp) / "t.db")


@pytest.mark.asyncio
async def test_happy_path_all_sent(tmp_db: Database):
    _seed(tmp_db, 3)
    cid = tmp_db.create_campaign("t", "Hi {first_name}!", None, "t.me/g")
    client = FakeClient()
    pacing = PacingConfig(min_delay_sec=0, max_delay_sec=0,
                          long_pause_every=100, long_pause_min_sec=0, long_pause_max_sec=0,
                          daily_cap=999)
    outcome = await run_campaign(client, tmp_db, cid, pacing)
    assert outcome.sent == 3
    assert outcome.errors == 0
    assert len(client.sent) == 3
    # Template was rendered (contains "Hi user0" etc.)
    assert "Hi user0" in client.sent[0][1]


@pytest.mark.asyncio
async def test_resume_skips_already_sent(tmp_db: Database):
    _seed(tmp_db, 3)
    cid = tmp_db.create_campaign("t", "Hi", None, None)
    contacts = tmp_db.resolved_contacts()
    # Pretend we already sent to the first contact in a previous run
    tmp_db.record_send(cid, contacts[0]["id"], "sent")
    client = FakeClient()
    pacing = PacingConfig(min_delay_sec=0, max_delay_sec=0, long_pause_every=100,
                          long_pause_min_sec=0, long_pause_max_sec=0, daily_cap=999)
    outcome = await run_campaign(client, tmp_db, cid, pacing)
    assert outcome.sent == 2      # only the 2 remaining
    assert len(client.sent) == 2


@pytest.mark.asyncio
async def test_opted_out_contacts_skipped(tmp_db: Database):
    _seed(tmp_db, 2)
    cid = tmp_db.create_campaign("t", "Hi", None, None)
    # Opt-out the second contact
    contacts = tmp_db.resolved_contacts()
    tmp_db.add_opt_out(contacts[1]["tg_user_id"], "don't want")
    client = FakeClient()
    pacing = PacingConfig(min_delay_sec=0, max_delay_sec=0, long_pause_every=100,
                          long_pause_min_sec=0, long_pause_max_sec=0, daily_cap=999)
    outcome = await run_campaign(client, tmp_db, cid, pacing)
    assert outcome.sent == 1
    assert len(client.sent) == 1


@pytest.mark.asyncio
async def test_privacy_error_marks_skipped(tmp_db: Database):
    from telethon.errors import UserPrivacyRestrictedError

    _seed(tmp_db, 2)
    cid = tmp_db.create_campaign("t", "Hi", None, None)
    contacts = tmp_db.resolved_contacts()
    first_tg_id = contacts[0]["tg_user_id"]

    def raise_privacy():
        raise UserPrivacyRestrictedError(None)

    client = FakeClient({first_tg_id: raise_privacy})
    pacing = PacingConfig(min_delay_sec=0, max_delay_sec=0, long_pause_every=100,
                          long_pause_min_sec=0, long_pause_max_sec=0, daily_cap=999)
    outcome = await run_campaign(client, tmp_db, cid, pacing)
    assert outcome.sent == 1
    assert outcome.skipped == 1


@pytest.mark.asyncio
async def test_peer_flood_single_peer_is_skipped_not_stopped(tmp_db: Database):
    """PeerFloodError is per-peer (Telegram flags the recipient, not the
    account). One occurrence skips the contact and keeps going; the
    campaign only stops on the account-level threshold."""
    from telethon.errors import PeerFloodError

    _seed(tmp_db, 5)
    cid = tmp_db.create_campaign("t", "Hi", None, None)
    contacts = tmp_db.resolved_contacts()

    def raise_peer_flood():
        raise PeerFloodError(None)

    client = FakeClient({contacts[0]["tg_user_id"]: raise_peer_flood})
    pacing = PacingConfig(min_delay_sec=0, max_delay_sec=0, long_pause_every=100,
                          long_pause_min_sec=0, long_pause_max_sec=0, daily_cap=999)
    outcome = await run_campaign(client, tmp_db, cid, pacing)
    assert outcome.stopped_reason == ""          # kept going
    assert outcome.skipped == 1                  # the flagged peer
    assert outcome.sent == 4                     # the rest went through


@pytest.mark.asyncio
async def test_peer_flood_threshold_stops_campaign(tmp_db: Database):
    """Three distinct PeerFlood events in one run = real account-level
    signal. Stop to protect the account."""
    from telethon.errors import PeerFloodError

    _seed(tmp_db, 5)
    cid = tmp_db.create_campaign("t", "Hi", None, None)
    contacts = tmp_db.resolved_contacts()

    def raise_peer_flood():
        raise PeerFloodError(None)

    client = FakeClient({
        contacts[0]["tg_user_id"]: raise_peer_flood,
        contacts[1]["tg_user_id"]: raise_peer_flood,
        contacts[2]["tg_user_id"]: raise_peer_flood,
    })
    pacing = PacingConfig(min_delay_sec=0, max_delay_sec=0, long_pause_every=100,
                          long_pause_min_sec=0, long_pause_max_sec=0, daily_cap=999)
    outcome = await run_campaign(client, tmp_db, cid, pacing)
    assert outcome.stopped_reason == "peer_flood"
    assert outcome.skipped == 3
    assert outcome.sent == 0


@pytest.mark.asyncio
async def test_stop_signal_respected(tmp_db: Database):
    _seed(tmp_db, 20)
    cid = tmp_db.create_campaign("t", "Hi", None, None)
    client = FakeClient()
    pacing = PacingConfig(min_delay_sec=0, max_delay_sec=0, long_pause_every=100,
                          long_pause_min_sec=0, long_pause_max_sec=0, daily_cap=999)
    stop = StopSignal()

    # Pause after the second send via progress callback. Phase H6 split
    # the stop signal into pause/stop kinds so campaigns can be resumed.
    count = 0
    async def after_second(payload):
        nonlocal count
        if payload.get("status") == "sent":
            count += 1
            if count >= 2:
                stop.set("user_paused")

    outcome = await run_campaign(client, tmp_db, cid, pacing,
                                 on_progress=after_second, stop_signal=stop)
    assert outcome.stopped_reason == "user_paused"
    assert outcome.sent >= 2 and outcome.sent < 20


@pytest.mark.asyncio
async def test_stop_signal_defaults_to_stopped(tmp_db: Database):
    """A bare `stop.set()` without a kind produces 'user_stopped' so the
    worker transitions the campaign to `stopped` (not `paused`)."""
    _seed(tmp_db, 5)
    cid = tmp_db.create_campaign("t", "Hi", None, None)
    client = FakeClient()
    pacing = PacingConfig(min_delay_sec=0, max_delay_sec=0, long_pause_every=100,
                          long_pause_min_sec=0, long_pause_max_sec=0, daily_cap=999)
    stop = StopSignal()
    sent_count = 0
    async def after_first(payload):
        nonlocal sent_count
        if payload.get("status") == "sent":
            sent_count += 1
            if sent_count >= 1:
                stop.set()  # no kind → default stop
    outcome = await run_campaign(client, tmp_db, cid, pacing,
                                 on_progress=after_first, stop_signal=stop)
    assert outcome.stopped_reason == "user_stopped"


@pytest.mark.asyncio
async def test_daily_cap_reached(tmp_db: Database):
    _seed(tmp_db, 5)
    cid = tmp_db.create_campaign("t", "Hi", None, None)
    client = FakeClient()
    pacing = PacingConfig(min_delay_sec=0, max_delay_sec=0, long_pause_every=100,
                          long_pause_min_sec=0, long_pause_max_sec=0, daily_cap=2)
    outcome = await run_campaign(client, tmp_db, cid, pacing)
    assert outcome.stopped_reason == "daily_cap_reached"
    assert outcome.sent == 2


@pytest.mark.asyncio
async def test_dry_run_hits_self_only(tmp_db: Database):
    _seed(tmp_db, 5)
    cid = tmp_db.create_campaign("t", "Hi", None, None)
    client = FakeClient()
    pacing = PacingConfig(min_delay_sec=0, max_delay_sec=0, long_pause_every=100,
                          long_pause_min_sec=0, long_pause_max_sec=0, daily_cap=999)
    outcome = await run_campaign(client, tmp_db, cid, pacing, dry_run_to_self=True)
    assert outcome.sent == 1
    # Recipient should be the fake "me" id, not any contact
    assert client.sent[0][0] == 999_000


@pytest.mark.asyncio
async def test_dry_run_does_not_touch_send_log(tmp_db: Database):
    """Regression: dry-run sent to Saved Messages but used to write a
    send_log row against the first real contact, (a) misleading the UI
    queue ("sent to X") and (b) causing the real campaign run to silently
    skip X via already_sent_ids. Dry-run must leave send_log untouched."""
    _seed(tmp_db, 3)
    cid = tmp_db.create_campaign("t", "Hi", None, None)
    client = FakeClient()
    pacing = PacingConfig(min_delay_sec=0, max_delay_sec=0, long_pause_every=100,
                          long_pause_min_sec=0, long_pause_max_sec=0, daily_cap=999)

    await run_campaign(client, tmp_db, cid, pacing, dry_run_to_self=True)

    # send_log stays empty after a dry-run — no real contact was touched.
    assert tmp_db.already_sent_ids(cid) == set()
    assert tmp_db.send_log_df(cid).empty

    # A real run afterwards must deliver to all 3 contacts.
    real_client = FakeClient()
    outcome = await run_campaign(real_client, tmp_db, cid, pacing)
    assert outcome.sent == 3
    assert len(real_client.sent) == 3


@pytest.mark.asyncio
async def test_reset_contact_send_requeues_contact(tmp_db: Database):
    """reset_contact_send deletes the (campaign, contact) row so the
    contact re-enters the send loop on the next run."""
    _seed(tmp_db, 3)
    cid = tmp_db.create_campaign("t", "Hi", None, None)
    pacing = PacingConfig(min_delay_sec=0, max_delay_sec=0, long_pause_every=100,
                          long_pause_min_sec=0, long_pause_max_sec=0, daily_cap=999)

    first = await run_campaign(FakeClient(), tmp_db, cid, pacing)
    assert first.sent == 3

    contacts = tmp_db.resolved_contacts()
    reset_target = contacts[1]["id"]
    n = tmp_db.reset_contact_send(cid, [reset_target])
    assert n == 1
    assert reset_target not in tmp_db.already_sent_ids(cid)

    # The next run should redeliver only to the reset contact.
    client2 = FakeClient()
    second = await run_campaign(client2, tmp_db, cid, pacing)
    assert second.sent == 1
    assert len(client2.sent) == 1
