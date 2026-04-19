"""Tests for the Dispatcher's validation + enqueue bookkeeping.

We don't execute the jobs (they require Telegram), we only verify the
queue-side safety: that duplicate resolves / duplicate campaigns are
refused, and that jobs are persisted with the right shape.
"""
from __future__ import annotations

import json
import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest

from core.database import Database
from core.jobs import Dispatcher
from core.rate_limiter import PacingConfig


@pytest.fixture
def db():
    with tempfile.TemporaryDirectory() as tmp:
        yield Database(Path(tmp) / "test.db")


@pytest.fixture
def dispatcher(db, monkeypatch):
    # Force the "no redis" path and stub out the thread starter so we don't
    # actually run the coroutine.
    monkeypatch.setattr("core.jobs.redis_client.is_redis_available", lambda: False)
    d = Dispatcher(db)
    d._enqueue_thread = lambda *a, **kw: None  # type: ignore
    return d


class TestEnqueueResolve:
    def test_first_call_succeeds(self, dispatcher, db):
        jid = dispatcher.enqueue_resolve(cleanup_imported=True)
        assert jid > 0
        row = db.get_job(jid)
        assert row["type"] == "resolve_contacts"
        assert row["status"] == "queued"
        payload = json.loads(row["payload_json"])
        assert payload["cleanup_imported"] is True

    def test_duplicate_resolve_refused(self, dispatcher):
        dispatcher.enqueue_resolve()
        with pytest.raises(RuntimeError, match="Resolve"):
            dispatcher.enqueue_resolve()

    def test_resolve_after_previous_done(self, dispatcher, db):
        jid = dispatcher.enqueue_resolve()
        db.mark_job_done(jid)
        # Now it's allowed again
        jid2 = dispatcher.enqueue_resolve()
        assert jid2 != jid


class TestEnqueueCampaign:
    def _pacing(self) -> PacingConfig:
        return PacingConfig(
            min_delay_sec=1, max_delay_sec=2,
            long_pause_every=10, long_pause_min_sec=5, long_pause_max_sec=10,
            daily_cap=100,
        )

    def test_first_campaign_succeeds(self, dispatcher, db):
        jid = dispatcher.enqueue_campaign(campaign_id=7, pacing=self._pacing())
        assert jid > 0
        row = db.get_job(jid)
        assert row["type"] == "run_campaign"
        payload = json.loads(row["payload_json"])
        assert payload["campaign_id"] == 7
        assert payload["dry_run"] is False
        assert payload["skip_recently_messaged_days"] == 0

    def test_same_campaign_twice_refused(self, dispatcher):
        dispatcher.enqueue_campaign(campaign_id=1, pacing=self._pacing())
        with pytest.raises(RuntimeError, match="Кампания"):
            dispatcher.enqueue_campaign(campaign_id=1, pacing=self._pacing())

    def test_different_campaigns_allowed(self, dispatcher, db):
        j1 = dispatcher.enqueue_campaign(campaign_id=1, pacing=self._pacing())
        j2 = dispatcher.enqueue_campaign(campaign_id=2, pacing=self._pacing())
        assert j1 != j2
        assert db.get_job(j1)["status"] == "queued"
        assert db.get_job(j2)["status"] == "queued"

    def test_dependency_is_persisted(self, dispatcher, db):
        parent = dispatcher.enqueue_resolve()
        child = dispatcher.enqueue_campaign(
            campaign_id=42, pacing=self._pacing(), depends_on=parent,
        )
        assert db.get_job(child)["depends_on"] == parent

    def test_same_campaign_after_previous_finishes(self, dispatcher, db):
        j1 = dispatcher.enqueue_campaign(campaign_id=10, pacing=self._pacing())
        db.mark_job_done(j1)
        # Finished — a new run is OK
        j2 = dispatcher.enqueue_campaign(campaign_id=10, pacing=self._pacing())
        assert j2 != j1

    def test_skip_days_is_carried(self, dispatcher, db):
        jid = dispatcher.enqueue_campaign(
            campaign_id=3, pacing=self._pacing(),
            skip_recently_messaged_days=14,
        )
        payload = json.loads(db.get_job(jid)["payload_json"])
        assert payload["skip_recently_messaged_days"] == 14


class TestEnqueuePrepare:
    """Combined Resolve → Validate one-click flow."""

    def _seed(self, db, *, phones: int = 0, usernames: int = 0) -> None:
        rows = []
        for i in range(phones):
            rows.append({
                "name": f"P{i}",
                "phone": f"+7900111{str(i).zfill(4)}",
                "raw_phone": f"8900111{str(i).zfill(4)}",
                "extra_json": None,
            })
        for i in range(usernames):
            rows.append({
                "name": f"U{i}",
                "phone": f"tg:handle{i}",
                "raw_phone": f"@handle{i}",
                "extra_json": None,
                "tg_username_hint": f"handle{i}",
            })
        db.upsert_contacts(rows)

    def test_prepare_enqueues_both_when_mixed(self, dispatcher, db):
        self._seed(db, phones=2, usernames=2)
        result = dispatcher.enqueue_prepare()
        assert result["skipped"] is False
        assert result["resolve_job_id"] is not None
        assert result["validate_job_id"] is not None
        # Validate must declare the Resolve job as its dependency so it
        # doesn't race — the combined flow's whole point is sequencing.
        validate_row = db.get_job(result["validate_job_id"])
        assert validate_row["depends_on"] == result["resolve_job_id"]

    def test_prepare_resolve_only_when_no_usernames(self, dispatcher, db):
        self._seed(db, phones=3, usernames=0)
        result = dispatcher.enqueue_prepare()
        assert result["resolve_job_id"] is not None
        assert result["validate_job_id"] is None
        assert result["skipped"] is False

    def test_prepare_validate_only_when_no_phones(self, dispatcher, db):
        self._seed(db, phones=0, usernames=2)
        result = dispatcher.enqueue_prepare()
        assert result["resolve_job_id"] is None
        assert result["validate_job_id"] is not None
        # No Resolve → Validate has no dependency.
        validate_row = db.get_job(result["validate_job_id"])
        assert validate_row["depends_on"] is None

    def test_prepare_noop_when_empty(self, dispatcher, db):
        # All contacts already resolved — nothing to do.
        self._seed(db, phones=1, usernames=0)
        contact = db.pending_resolve()[0]
        db.mark_resolved(contact["id"], 1234, None, None)
        result = dispatcher.enqueue_prepare()
        assert result == {
            "resolve_job_id": None,
            "validate_job_id": None,
            "skipped": True,
        }

    def test_prepare_noop_leaves_unreachable_alone(self, dispatcher, db):
        # A contact flagged `not_on_telegram` by an earlier Resolve
        # stays that way — Prepare does NOT retry it, and reports
        # skipped=True when only unreachable rows remain. Edge case:
        # user has imported 5 phones, 1 is not on TG; after Prepare
        # runs once, further clicks must not thrash the workflow.
        self._seed(db, phones=1, usernames=0)
        contact = db.pending_resolve()[0]
        # Mark as not_on_telegram (what the real Resolve job would do).
        with db._conn() as c:
            c.execute(
                "UPDATE contacts SET resolve_status='not_on_telegram' WHERE id=?",
                (contact["id"],),
            )
        result = dispatcher.enqueue_prepare()
        assert result["skipped"] is True

    def test_prepare_picks_up_error_rows(self, dispatcher, db):
        # A previous Validate that failed with a bad @handle left the
        # row in resolve_status='error'. User corrects the hint; a
        # fresh Prepare MUST re-enqueue Validate so the correction is
        # tried. (Resolve won't touch tg:* phones, so only Validate.)
        self._seed(db, phones=0, usernames=1)
        with db._conn() as c:
            # Flip the single seeded row to 'error'. `pending_resolve()`
            # excludes tg:* rows, so we fetch directly.
            c.execute(
                "UPDATE contacts SET resolve_status='error', "
                "resolve_error='bad handle'"
            )
        result = dispatcher.enqueue_prepare()
        assert result["validate_job_id"] is not None
        assert result["skipped"] is False


class TestCountPrepareWorkload:
    """The counter behind the combined button label."""

    def test_empty_db(self, db):
        w = db.count_prepare_workload()
        assert w == {"phone_resolvable": 0, "username_validatable": 0,
                     "unreachable": 0}

    def test_phone_and_username_separately(self, db):
        db.upsert_contacts([
            {"name": "A", "phone": "+79001110000",
             "raw_phone": "A", "extra_json": None},
            {"name": "B", "phone": "tg:x", "raw_phone": "@x",
             "extra_json": None, "tg_username_hint": "x"},
        ])
        w = db.count_prepare_workload()
        assert w["phone_resolvable"] == 1
        assert w["username_validatable"] == 1

    def test_unreachable_excluded_from_workload(self, db):
        db.upsert_contacts([
            {"name": "A", "phone": "+79001110000",
             "raw_phone": "A", "extra_json": None},
        ])
        row = db.pending_resolve()[0]
        with db._conn() as c:
            c.execute(
                "UPDATE contacts SET resolve_status='not_on_telegram' WHERE id=?",
                (row["id"],),
            )
        w = db.count_prepare_workload()
        assert w["phone_resolvable"] == 0
        assert w["unreachable"] == 1
