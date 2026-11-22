import asyncio
import datetime as dt

import pytest
from telethon import errors

from blaster.sender import Runner
from conftest import FakeClient, fill


def camp(db, template="Привет, {first_name|друзья}!", image=None, list_name="L"):
    return db.create_campaign("c", list_name, template, image)


def runner(client, db, cid, pacing, sleeps=None, **kw):
    async def fake_sleep(s):
        if sleeps is not None:
            sleeps.append(s)
    return Runner(client, db, cid, pacing, out=lambda m: None, sleep_fn=fake_sleep, **kw)


async def test_happy_path_and_personalisation(db, pacing):
    rows = fill(db, 3)
    cid = camp(db)
    client = FakeClient()
    res = await runner(client, db, cid, pacing).run()
    assert res.sent == 3 and res.remaining == 0
    assert [t for _, t, _ in client.sent] == ["Привет, Азиз!"] * 3
    assert db.get_campaign(cid)["status"] == "done"
    # peer carries access_hash → no entity-cache dependency
    assert client.sent[0][0] == rows[0]["tg_user_id"]


async def test_second_run_sends_nothing(db, pacing):
    fill(db, 2)
    cid = camp(db)
    await runner(FakeClient(), db, cid, pacing).run()
    client = FakeClient()
    res = await runner(client, db, cid, pacing).run()
    assert res.sent == 0 and client.sent == []


async def test_crash_between_send_and_confirm_is_not_resent(db, pacing):
    rows = fill(db, 2)
    cid = camp(db)
    db.reserve(cid, rows[0])       # simulates: message left, process died before finish()
    client = FakeClient()
    res = await runner(client, db, cid, pacing).run()
    assert [u for u, *_ in client.sent] == [rows[1]["tg_user_id"]]
    assert db.campaign_stats(cid)["uncertain"] == 1
    # explicit opt-in to resend
    client2 = FakeClient()
    await runner(client2, db, cid, pacing).run(retry_uncertain=True)
    assert [u for u, *_ in client2.sent] == [rows[0]["tg_user_id"]]
    assert res.sent == 1


async def test_flood_wait_short_sleeps_and_retries(db, pacing):
    rows = fill(db, 2)
    cid = camp(db)
    uid = rows[0]["tg_user_id"]
    client = FakeClient({uid: [errors.FloodWaitError(request=None, capture=42), None]})
    sleeps = []
    res = await runner(client, db, cid, pacing, sleeps).run()
    assert res.sent == 2
    assert 47 in sleeps
    assert client.attempts.count(uid) == 2


async def test_flood_wait_long_stops_and_keeps_contact_queued(db, pacing):
    rows = fill(db, 2)
    cid = camp(db)
    uid = rows[0]["tg_user_id"]
    client = FakeClient({uid: [errors.FloodWaitError(request=None, capture=99999)]})
    res = await runner(client, db, cid, pacing).run()
    assert res.sent == 0 and "FloodWait" in res.stopped_reason
    assert res.remaining == 2                      # nothing lost, nothing marked
    assert db.get_campaign(cid)["status"] == "paused"


async def test_peer_flood_stops_after_threshold(db, pacing):
    rows = fill(db, 4)
    cid = camp(db)
    script = {r["tg_user_id"]: [errors.PeerFloodError(request=None)] for r in rows}
    res = await runner(FakeClient(script), db, cid, pacing).run()
    assert "PeerFlood" in res.stopped_reason
    st = db.campaign_stats(cid)
    assert st["error"] == 1           # first one marked, second one released for later
    assert st["remaining"] == 3


@pytest.mark.parametrize("exc", [
    errors.UserPrivacyRestrictedError(request=None),
    errors.UserIsBlockedError(request=None),
    errors.InputUserDeactivatedError(request=None),
])
async def test_skippable_errors(db, pacing, exc):
    rows = fill(db, 2)
    cid = camp(db)
    res = await runner(FakeClient({rows[0]["tg_user_id"]: [exc]}), db, cid, pacing).run()
    assert res.sent == 1 and res.skipped == 1 and res.remaining == 0


async def test_paid_messages_rpc_is_skipped(db, pacing):
    rows = fill(db, 1)
    cid = camp(db)
    exc = errors.RPCError(request=None, message="ALLOW_PAYMENT_REQUIRED_10", code=400)
    res = await runner(FakeClient({rows[0]["tg_user_id"]: [exc]}), db, cid, pacing).run()
    assert res.skipped == 1


async def test_timeout_is_uncertain_not_retried(db, pacing):
    rows = fill(db, 2)
    cid = camp(db)
    client = FakeClient({rows[0]["tg_user_id"]: [asyncio.TimeoutError()]})
    res = await runner(client, db, cid, pacing).run()
    assert res.uncertain == 1 and res.sent == 1
    res2 = await runner(FakeClient(), db, cid, pacing).run()
    assert res2.sent == 0


async def test_fatal_account_error_stops(db, pacing):
    rows = fill(db, 2)
    cid = camp(db)
    client = FakeClient({rows[0]["tg_user_id"]: [errors.AuthKeyUnregisteredError(request=None)]})
    res = await runner(client, db, cid, pacing).run()
    assert "аккаунт" in res.stopped_reason and res.remaining == 2


async def test_consecutive_failures_stop(db, pacing):
    rows = fill(db, 8)
    cid = camp(db)
    script = {r["tg_user_id"]: [errors.RPCError(request=None, message="INTERNAL", code=500)]
              for r in rows}
    res = await runner(FakeClient(script), db, cid, pacing).run()
    assert res.errors == 5 and "подряд" in res.stopped_reason
    res2 = await runner(FakeClient(), db, cid, pacing).run(retry_errors=True)
    assert res2.sent == 8


async def test_limit_per_run(db, pacing):
    fill(db, 5)
    cid = camp(db)
    res = await runner(FakeClient(), db, cid, pacing).run(limit=2)
    assert res.sent == 2 and res.remaining == 3


async def test_daily_cap_rolling_window(db, pacing):
    fill(db, 5)
    cid = camp(db)
    pacing.daily_cap = 3
    res = await runner(FakeClient(), db, cid, pacing).run()
    assert res.sent == 3 and "лимит" in res.stopped_reason
    res = await runner(FakeClient(), db, cid, pacing).run()
    assert res.sent == 0


async def test_daily_cap_until_done_waits(db, pacing):
    fill(db, 3)
    cid = camp(db)
    pacing.daily_cap = 2
    now = [1_000_000.0]
    sleeps = []

    async def fake_sleep(s):
        sleeps.append(s)
        now[0] += s

    import blaster.db as dbmod
    orig = dbmod._now
    dbmod._now = lambda: now[0]
    try:
        r = Runner(FakeClient(), db, cid, pacing, out=lambda m: None,
                   clock=lambda: now[0], sleep_fn=fake_sleep)
        res = await r.run(until_done=True)
    finally:
        dbmod._now = orig
    assert res.sent == 3 and res.remaining == 0
    assert any(s > 80000 for s in sleeps)


async def test_outside_window_stops(db, pacing):
    fill(db, 1)
    cid = camp(db)
    now = dt.datetime.now().astimezone()
    pacing.window_start = (now + dt.timedelta(hours=2)).time().replace(second=0, microsecond=0)
    pacing.window_end = (now + dt.timedelta(hours=3)).time().replace(second=0, microsecond=0)
    pacing.timezone = "UTC"
    if pacing.seconds_until_window() == 0:
        pytest.skip("clock edge")
    res = await runner(FakeClient(), db, cid, pacing).run()
    assert res.sent == 0 and "окна" in res.stopped_reason


async def test_optout_and_self_and_same_tg_user_dedup(db, pacing):
    rows = fill(db, 4)
    # rows[1] and rows[2] are the same Telegram user (two phones)
    db.mark_resolved(rows[2]["id"], rows[1]["tg_user_id"], 1, None, None, "t")
    db.add_optout(rows[3]["phone"])
    cid = camp(db)
    client = FakeClient(me_id=rows[0]["tg_user_id"])          # rows[0] is the sender
    res = await runner(client, db, cid, pacing).run()
    assert [u for u, *_ in client.sent] == [rows[1]["tg_user_id"]]
    assert res.sent == 1


async def test_image_uploaded_once_then_reused(db, pacing, tmp_path):
    img = tmp_path / "b.png"
    img.write_bytes(b"\x89PNG")
    fill(db, 3)
    cid = camp(db, image=str(img))
    client = FakeClient()
    await runner(client, db, cid, pacing).run()
    files = [kw["file"] for *_, kw in client.sent]
    assert files[0] == str(img) and files[1:] == ["PHOTO", "PHOTO"]


async def test_bad_template_refuses_to_start(db, pacing):
    fill(db, 1)
    cid = camp(db, template="Привет, {frist_name}!")
    with pytest.raises(ValueError, match="frist_name"):
        await runner(FakeClient(), db, cid, pacing).run()


async def test_stop_event(db, pacing):
    fill(db, 3)
    cid = camp(db)
    stop = asyncio.Event()
    client = FakeClient()

    async def sleep_and_stop(s):
        stop.set()
    r = Runner(client, db, cid, pacing, stop=stop, out=lambda m: None, sleep_fn=sleep_and_stop)
    res = await r.run()
    assert res.sent == 1 and res.remaining == 2 and "останов" in res.stopped_reason


async def test_long_pause_every_n(db, pacing):
    fill(db, 5)
    cid = camp(db)
    pacing.long_pause_every = 2
    sleeps = []
    await runner(FakeClient(), db, cid, pacing, sleeps).run()
    assert sleeps.count(5) == 2 and sleeps.count(1) == 2
