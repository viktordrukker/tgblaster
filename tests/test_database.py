"""Tests for the SQLite state store."""
import json
import tempfile
from pathlib import Path

import pytest

from core.database import Database


@pytest.fixture
def db():
    with tempfile.TemporaryDirectory() as tmp:
        yield Database(Path(tmp) / "test.db")


def _rows(n=3):
    return [
        {"name": f"user{i}", "phone": f"+7900111{str(i).zfill(4)}",
         "raw_phone": f"8900111{str(i).zfill(4)}",
         "extra_json": json.dumps({"ix": i})}
        for i in range(n)
    ]


class TestSchemaAndUpsert:
    def test_schema_creates_tables(self, db: Database):
        # Simply loading does the work
        assert db.pending_resolve() == []

    def test_upsert_returns_count_of_inserts(self, db: Database):
        assert db.upsert_contacts(_rows(3)) == 3
        # Same phones again → zero
        assert db.upsert_contacts(_rows(3)) == 0

    def test_duplicate_phone_ignored(self, db: Database):
        rows = _rows(1) + [{"name": "dup", "phone": _rows(1)[0]["phone"],
                            "raw_phone": "x", "extra_json": None}]
        assert db.upsert_contacts(rows) == 1


class TestResolveWorkflow:
    def test_pending_then_resolved(self, db: Database):
        db.upsert_contacts(_rows(2))
        pending = db.pending_resolve()
        assert len(pending) == 2
        db.mark_resolved(pending[0]["id"], 111, "user1", 9999)
        db.mark_not_on_tg(pending[1]["id"])
        resolved = db.resolved_contacts()
        assert len(resolved) == 1
        assert resolved[0]["tg_user_id"] == 111

    def test_error_marking(self, db: Database):
        db.upsert_contacts(_rows(1))
        pending = db.pending_resolve()
        db.mark_resolve_error(pending[0]["id"], "rate limited")
        # Error rows are retryable — pending_resolve includes both 'pending'
        # and 'error' so a FloodWait / transient miss can be re-queued.
        retryable = db.pending_resolve()
        assert len(retryable) == 1
        assert retryable[0]["id"] == pending[0]["id"]
        df = db.all_contacts_df()
        assert df.iloc[0]["resolve_error"] == "rate limited"
        assert df.iloc[0]["resolve_status"] == "error"


class TestCampaigns:
    def test_create_and_update(self, db: Database):
        cid = db.create_campaign("test", "hello", None, "t.me/xyz")
        c = db.get_campaign(cid)
        assert c["name"] == "test"
        assert c["status"] == "draft"
        db.update_campaign_status(cid, "running")
        assert db.get_campaign(cid)["status"] == "running"


class TestCampaignCRUD:
    """Phase C1 — edit content fields + guarded delete."""

    def test_update_campaign_allowed_fields(self, db: Database):
        cid = db.create_campaign("old", "old tpl", None, "t.me/old")
        updated = db.update_campaign(cid, name="new", template="new tpl",
                                     group_link="t.me/new")
        assert updated is True
        row = db.get_campaign(cid)
        assert row["name"] == "new"
        assert row["template"] == "new tpl"
        assert row["group_link"] == "t.me/new"
        # Status, created_at, tags are untouched.
        assert row["status"] == "draft"

    def test_update_campaign_unknown_field_rejected(self, db: Database):
        cid = db.create_campaign("x", "t", None, None)
        import pytest
        with pytest.raises(ValueError, match="not editable"):
            db.update_campaign(cid, status="running")
        with pytest.raises(ValueError):
            db.update_campaign(cid, id=99)

    def test_update_campaign_nonexistent_returns_false(self, db: Database):
        # No row with id=9999 → rowcount=0.
        assert db.update_campaign(99_999, name="nope") is False

    def test_update_campaign_empty_fields_is_noop(self, db: Database):
        cid = db.create_campaign("x", "t", None, None)
        assert db.update_campaign(cid) is False

    def test_update_campaign_does_not_touch_status(self, db: Database):
        cid = db.create_campaign("x", "t", None, None)
        db.transition_campaign_state(cid, "running")
        db.update_campaign(cid, template="new")
        assert db.get_campaign(cid)["status"] == "running"

    def test_delete_draft_succeeds(self, db: Database):
        cid = db.create_campaign("x", "t", None, None)
        assert db.delete_campaign(cid) is True
        assert db.get_campaign(cid) is None

    def test_delete_running_refused(self, db: Database):
        cid = db.create_campaign("x", "t", None, None)
        db.transition_campaign_state(cid, "running")
        assert db.delete_campaign(cid) is False
        assert db.get_campaign(cid) is not None

    def test_delete_paused_refused(self, db: Database):
        cid = db.create_campaign("x", "t", None, None)
        db.transition_campaign_state(cid, "running")
        db.transition_campaign_state(cid, "paused")
        assert db.delete_campaign(cid) is False
        assert db.get_campaign(cid) is not None

    def test_delete_stopped_succeeds(self, db: Database):
        cid = db.create_campaign("x", "t", None, None)
        db.transition_campaign_state(cid, "running")
        db.transition_campaign_state(cid, "stopped")
        assert db.delete_campaign(cid) is True

    def test_delete_done_succeeds(self, db: Database):
        cid = db.create_campaign("x", "t", None, None)
        db.transition_campaign_state(cid, "running")
        db.transition_campaign_state(cid, "done")
        assert db.delete_campaign(cid) is True

    def test_delete_wipes_send_log_rows(self, db: Database):
        """send_log rows for the deleted campaign must go away — we own
        the FK and SQLite doesn't cascade unless ON DELETE CASCADE is
        declared (it isn't in our schema)."""
        db.upsert_contacts(_rows(1))
        contact = db.pending_resolve()[0]
        db.mark_resolved(contact["id"], 111, None, None)
        cid = db.create_campaign("x", "t", None, None)
        db.reserve_send(cid, contact["id"], 9999)
        db.confirm_send(cid, contact["id"], "sent", "")
        assert db.campaign_stats(cid)["sent"] == 1
        db.delete_campaign(cid)
        import sqlite3
        with db._conn() as c:
            n = c.execute("SELECT COUNT(*) FROM send_log WHERE campaign_id=?",
                          (cid,)).fetchone()[0]
        assert n == 0

    def test_delete_nonexistent_returns_false(self, db: Database):
        assert db.delete_campaign(99_999) is False


class TestSendLog:
    def test_record_and_stats(self, db: Database):
        db.upsert_contacts(_rows(3))
        contacts = db.pending_resolve()
        for ci in contacts:
            db.mark_resolved(ci["id"], ci["id"] * 10, None, None)
        cid = db.create_campaign("t", "hello", None, None)

        assert db.already_sent_ids(cid) == set()
        db.record_send(cid, contacts[0]["id"], "sent")
        db.record_send(cid, contacts[1]["id"], "error", "boom")
        db.record_send(cid, contacts[2]["id"], "skipped", "privacy")

        stats = db.campaign_stats(cid)
        assert stats["sent"] == 1
        assert stats["errors"] == 1
        assert stats["skipped"] == 1
        assert db.already_sent_ids(cid) == {contacts[0]["id"]}

    def test_sent_today_count(self, db: Database):
        db.upsert_contacts(_rows(1))
        contact = db.pending_resolve()[0]
        db.mark_resolved(contact["id"], 777, None, None)
        cid = db.create_campaign("t", "hello", None, None)
        assert db.sent_today_count(cid) == 0
        db.record_send(cid, contact["id"], "sent")
        assert db.sent_today_count(cid) == 1


class TestReserveConfirmIdempotency:
    """Phase H2 — pre-allocated random_id + reserve-then-confirm must
    survive a crash between send() and confirm() without duplicating
    delivery or losing the random_id."""

    def _seed_resolved(self, db: Database, n: int = 2) -> tuple[list[int], int]:
        db.upsert_contacts(_rows(n))
        contacts = db.pending_resolve()
        for c in contacts:
            db.mark_resolved(c["id"], c["id"] * 10, None, None)
        cid = db.create_campaign("probe-h2", "hi", None, None)
        return [c["id"] for c in contacts], cid

    def test_reserve_then_confirm_happy_path(self, db: Database):
        ids, cid = self._seed_resolved(db, 1)
        rid_in = 0xDEAD_BEEF_CAFE
        rid_out, state = db.reserve_send(cid, ids[0], rid_in)
        assert state == "reserved"
        assert rid_out == rid_in
        # Pending row is NOT in `already_sent_ids` — the sender must be
        # able to re-enter it after a crash and reuse the stored rid so
        # Telegram server-side dedup covers the uncertain delivery.
        # Exclusion from concurrent double-run is the campaign lock's job.
        assert db.already_sent_ids(cid) == set()
        db.confirm_send(cid, ids[0], "sent", "")
        assert db.already_sent_ids(cid) == {ids[0]}
        stats = db.campaign_stats(cid)
        assert stats["sent"] == 1

    def test_resume_reuses_same_random_id_after_crash(self, db: Database):
        """Simulate: worker called reserve(), send_message landed on
        Telegram, then the worker died before confirm(). Next run's
        reserve() must return the ORIGINAL rid so the retried send_message
        gets server-side deduped."""
        ids, cid = self._seed_resolved(db, 1)
        rid_first, state = db.reserve_send(cid, ids[0], 111_111_111)
        assert state == "reserved"
        # Next run tries with a different rid — but reserve keeps the old one.
        rid_second, state2 = db.reserve_send(cid, ids[0], 222_222_222)
        assert state2 == "pending"
        assert rid_second == rid_first == 111_111_111, (
            "resumed reserve must reuse the original random_id for dedup"
        )
        # attempt_count should have ticked up.
        import sqlite3
        with db._conn() as c:
            row = c.execute(
                "SELECT attempt_count FROM send_log WHERE campaign_id=? AND contact_id=?",
                (cid, ids[0]),
            ).fetchone()
        assert row["attempt_count"] == 2

    def test_sent_row_is_never_re_reserved(self, db: Database):
        ids, cid = self._seed_resolved(db, 1)
        rid_out, _ = db.reserve_send(cid, ids[0], 999)
        db.confirm_send(cid, ids[0], "sent", "")
        # A second reserve on the same (campaign, contact) returns 'sent'
        # so the sender knows to skip without double-delivery.
        rid2, state2 = db.reserve_send(cid, ids[0], 12345)
        assert state2 == "sent"
        assert rid2 == rid_out
        stats = db.campaign_stats(cid)
        # Still exactly one sent row — second reserve did not replace anything.
        assert stats["sent"] == 1

    def test_error_row_retry_reuses_stored_rid(self, db: Database):
        """Error rows reuse their stored random_id on retry — the send
        might actually have landed on Telegram despite the error flag,
        and reusing rid lets Telegram's server-side dedup catch that."""
        ids, cid = self._seed_resolved(db, 1)
        db.reserve_send(cid, ids[0], 555)
        db.confirm_send(cid, ids[0], "error", "transient")
        rid_new, state = db.reserve_send(cid, ids[0], 777)
        assert state == "retry"
        assert rid_new == 555, (
            "error-row retry must reuse the original rid, not the new one"
        )


class TestOptOuts:
    def test_add_and_check(self, db: Database):
        assert db.is_opted_out(42) is False
        db.add_opt_out(42, "stop")
        assert db.is_opted_out(42) is True
        # idempotent
        db.add_opt_out(42, "another")
        assert db.is_opted_out(42) is True


class TestReadReceipts:
    """Phase R1 — per-message read-receipt storage + coarse-legacy fallback.

    The sender captures Telegram's message.id at confirm time; a later
    manual read-check fetches each peer's `read_outbox_max_id` and flips
    `send_log.read_at` precisely (message_id ≤ cursor) or — for legacy
    rows missing message_id — coarsely when the cursor is > 0."""

    def _seed_two_sent(self, db: Database, *,
                       with_message_id: bool = True) -> tuple[int, list[int]]:
        """Helper: resolve 2 contacts, send both, optionally store ids."""
        db.upsert_contacts(_rows(2))
        contacts = db.pending_resolve()
        for c in contacts:
            db.mark_resolved(c["id"], c["id"] * 10, None, None)
        cid = db.create_campaign("readprobe", "hi", None, None)
        ids = [c["id"] for c in contacts]
        for i, contact_id in enumerate(ids):
            db.reserve_send(cid, contact_id, 1000 + i)
            if with_message_id:
                db.confirm_send(cid, contact_id, "sent", "",
                                message_id=50 + i * 10)
            else:
                db.confirm_send(cid, contact_id, "sent", "")
        return cid, ids

    def test_confirm_send_stores_message_id(self, db: Database):
        cid, ids = self._seed_two_sent(db, with_message_id=True)
        targets = db.read_check_targets(campaign_ids=[cid])
        # Both contacts should show up as read-check targets (sent + no read_at).
        assert len(targets) == 2
        assert {t["contact_id"] for t in targets} == set(ids)

    def test_confirm_send_omits_message_id_by_default(self, db: Database):
        # Back-compat: old callers that don't pass message_id see NULL.
        cid, ids = self._seed_two_sent(db, with_message_id=False)
        import sqlite3
        with db._conn() as c:
            rows = c.execute(
                "SELECT message_id FROM send_log WHERE campaign_id=?",
                (cid,),
            ).fetchall()
        assert all(r["message_id"] is None for r in rows)

    def test_apply_read_receipts_precise(self, db: Database):
        cid, ids = self._seed_two_sent(db, with_message_id=True)
        # Peer for contact ids[0] has read through message_id=50 → that row
        # is flagged. Peer for ids[1] only up to 40 (below their 60) → NOT flagged.
        updates = [(ids[0], 55), (ids[1], 40)]
        stats = db.apply_read_receipts(
            updates, checked_at_iso="2026-04-19T22:00:00+00:00",
        )
        assert stats["precise"] == 1
        assert stats["coarse"] == 0
        df = db.send_log_df(cid)
        read_by_contact = {int(r["name"].replace("user", "")): r["read_at"]
                           for _, r in df.iterrows()}
        # ids[0] is user0 (first seed row), ids[1] is user1.
        assert read_by_contact[0] == "2026-04-19T22:00:00+00:00"
        assert read_by_contact[1] in (None, "")

    def test_legacy_rows_never_flagged(self, db: Database):
        """Rows without `message_id` MUST stay unread — the peer's
        `read_outbox_max_id` cursor is monotonic across ALL history, so
        an old interaction produces a false-positive if we trust
        'cursor > 0'. Precise-only is the honest policy."""
        cid, ids = self._seed_two_sent(db, with_message_id=False)
        stats = db.apply_read_receipts(
            [(ids[0], 99)], checked_at_iso="2026-04-19T22:00:00+00:00",
        )
        assert stats == {"precise": 0, "coarse": 0}
        df = db.send_log_df(cid)
        assert all(r["read_at"] in (None, "") for _, r in df.iterrows())

    def test_clear_coarse_read_flags(self, db: Database):
        """Migration helper wipes any old coarse-marker or
        message_id-NULL read_at values seeded before the policy change."""
        cid, ids = self._seed_two_sent(db, with_message_id=False)
        # Manually seed the kind of row the old coarse path produced.
        with db._conn() as c:
            c.execute(
                "UPDATE send_log SET read_at=? WHERE contact_id=?",
                ("2026-04-19T22:00:00+00:00 (coarse)", ids[0]),
            )
            c.execute(
                "UPDATE send_log SET read_at=? WHERE contact_id=?",
                ("2026-04-19T22:00:00+00:00", ids[1]),  # message_id still NULL
            )
        cleared = db.clear_coarse_read_flags()
        assert cleared == 2
        # Both rows should now be NULL again.
        df = db.send_log_df(cid)
        assert all(r["read_at"] in (None, "") for _, r in df.iterrows())

    def test_apply_read_receipts_idempotent(self, db: Database):
        cid, ids = self._seed_two_sent(db, with_message_id=True)
        db.apply_read_receipts(
            [(ids[0], 99)], checked_at_iso="2026-04-19T22:00:00+00:00",
        )
        # Second run of the same check touches zero rows — `read_at IS NULL`
        # excludes already-flagged rows.
        stats2 = db.apply_read_receipts(
            [(ids[0], 99)], checked_at_iso="2026-04-19T23:00:00+00:00",
        )
        assert stats2 == {"precise": 0, "coarse": 0}

    def test_read_check_targets_filters(self, db: Database):
        cid, ids = self._seed_two_sent(db, with_message_id=True)
        # Filter by a subset of contacts.
        t_subset = db.read_check_targets(
            campaign_ids=[cid], contact_ids=[ids[0]],
        )
        assert [r["contact_id"] for r in t_subset] == [ids[0]]
        # Filter by a campaign id that doesn't exist → empty.
        t_empty = db.read_check_targets(campaign_ids=[9_999])
        assert t_empty == []

    def test_read_check_targets_excludes_already_read(self, db: Database):
        cid, ids = self._seed_two_sent(db, with_message_id=True)
        db.apply_read_receipts(
            [(ids[0], 99)], checked_at_iso="2026-04-19T22:00:00+00:00",
        )
        remaining = [r["contact_id"] for r
                     in db.read_check_targets(campaign_ids=[cid])]
        # Already-read contact drops out of the next check's target set.
        assert ids[0] not in remaining
        assert ids[1] in remaining

    def test_campaign_stats_includes_read(self, db: Database):
        cid, ids = self._seed_two_sent(db, with_message_id=True)
        stats_before = db.campaign_stats(cid)
        assert stats_before["read"] == 0
        db.apply_read_receipts(
            [(ids[0], 99), (ids[1], 99)],
            checked_at_iso="2026-04-19T22:00:00+00:00",
        )
        stats_after = db.campaign_stats(cid)
        assert stats_after["read"] == 2
