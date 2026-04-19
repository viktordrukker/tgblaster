"""Tests for the job queue state + dedup / precedence rules."""
from __future__ import annotations

import json
import tempfile
from pathlib import Path

import pytest

from core.database import Database


# --- fixtures --------------------------------------------------------------


@pytest.fixture
def db():
    with tempfile.TemporaryDirectory() as tmp:
        yield Database(Path(tmp) / "test.db")


def _rows(n=3):
    return [
        {"name": f"u{i}", "phone": f"+7900111{str(i).zfill(4)}",
         "raw_phone": f"8900111{str(i).zfill(4)}", "extra_json": None}
        for i in range(n)
    ]


# --- jobs table -----------------------------------------------------------


class TestJobsTable:
    def test_create_queued_job(self, db: Database):
        jid = db.create_job("resolve_contacts",
                            payload_json=json.dumps({"cleanup_imported": True}),
                            backend="thread")
        row = db.get_job(jid)
        assert row["status"] == "queued"
        assert row["backend"] == "thread"
        assert row["depends_on"] is None

    def test_started_done_transitions(self, db: Database):
        jid = db.create_job("resolve_contacts")
        db.mark_job_started(jid)
        assert db.get_job(jid)["status"] == "running"
        assert db.get_job(jid)["started_at"] is not None
        db.mark_job_done(jid, progress_json=json.dumps({"done": 10, "total": 10}))
        row = db.get_job(jid)
        assert row["status"] == "done"
        assert row["finished_at"] is not None
        assert "10" in (row["progress_json"] or "")

    def test_error_transition(self, db: Database):
        jid = db.create_job("run_campaign")
        db.mark_job_error(jid, "boom: floodwait")
        row = db.get_job(jid)
        assert row["status"] == "error"
        assert "boom" in (row["error"] or "")

    def test_cancelled_transition(self, db: Database):
        jid = db.create_job("run_campaign")
        db.mark_job_cancelled(jid)
        assert db.get_job(jid)["status"] == "cancelled"

    def test_has_running_job(self, db: Database):
        assert db.has_running_job("resolve_contacts") is False
        jid = db.create_job("resolve_contacts")
        assert db.has_running_job("resolve_contacts") is True
        db.mark_job_done(jid)
        assert db.has_running_job("resolve_contacts") is False

    def test_list_jobs_desc_order(self, db: Database):
        a = db.create_job("run_campaign")
        b = db.create_job("run_campaign")
        c = db.create_job("resolve_contacts")
        ids = [j["id"] for j in db.list_jobs()]
        assert ids == [c, b, a]

    def test_progress_update(self, db: Database):
        jid = db.create_job("run_campaign")
        db.update_job_progress(jid, json.dumps({"sent": 5, "total": 20}))
        payload = json.loads(db.get_job(jid)["progress_json"])
        assert payload["sent"] == 5
        # Later update overwrites
        db.update_job_progress(jid, json.dumps({"sent": 11, "total": 20}))
        assert json.loads(db.get_job(jid)["progress_json"])["sent"] == 11


class TestDependencies:
    def test_no_dependency_is_always_satisfied(self, db: Database):
        jid = db.create_job("run_campaign")
        assert db.dependency_satisfied(jid) is True

    def test_pending_dependency_blocks(self, db: Database):
        parent = db.create_job("resolve_contacts")
        child = db.create_job("run_campaign", depends_on=parent)
        assert db.dependency_satisfied(child) is False

    def test_failed_dependency_does_not_satisfy(self, db: Database):
        parent = db.create_job("resolve_contacts")
        db.mark_job_error(parent, "nope")
        child = db.create_job("run_campaign", depends_on=parent)
        # A failed parent does not count as "done"
        assert db.dependency_satisfied(child) is False

    def test_done_dependency_unblocks(self, db: Database):
        parent = db.create_job("resolve_contacts")
        db.mark_job_done(parent)
        child = db.create_job("run_campaign", depends_on=parent)
        assert db.dependency_satisfied(child) is True


class TestJobsDataFrame:
    def test_jobs_df_columns(self, db: Database):
        db.create_job("resolve_contacts")
        df = db.jobs_df()
        for col in ("id", "type", "status", "backend", "queued_at"):
            assert col in df.columns


# --- dedup / precedence ----------------------------------------------------


class TestFindDuplicateTgUsers:
    def test_two_phones_same_tg_user(self, db: Database):
        db.upsert_contacts(_rows(3))
        pending = db.pending_resolve()
        # Two phones resolved to the same TG user
        db.mark_resolved(pending[0]["id"], 100, None, None)
        db.mark_resolved(pending[1]["id"], 100, None, None)
        db.mark_resolved(pending[2]["id"], 200, None, None)

        dups = db.find_duplicate_tg_users()
        assert len(dups) == 1
        row = dups[0]
        assert row["tg_user_id"] == 100
        ids = sorted(int(x) for x in row["ids"].split(","))
        assert len(ids) == 2

    def test_no_duplicates_returns_empty(self, db: Database):
        db.upsert_contacts(_rows(2))
        for i, p in enumerate(db.pending_resolve()):
            db.mark_resolved(p["id"], 500 + i, None, None)
        assert db.find_duplicate_tg_users() == []


class TestContactsAlreadyMessaged:
    def test_ever(self, db: Database):
        db.upsert_contacts(_rows(3))
        contacts = db.pending_resolve()
        for c in contacts:
            db.mark_resolved(c["id"], c["id"] * 10, None, None)
        cid = db.create_campaign("t", "hello", None, None)
        db.record_send(cid, contacts[0]["id"], "sent")
        db.record_send(cid, contacts[1]["id"], "error", "x")  # errors don't count

        already = db.contacts_already_messaged(days=0)
        assert contacts[0]["id"] in already
        assert contacts[1]["id"] not in already
        assert contacts[2]["id"] not in already

    def test_recent_window(self, db: Database):
        db.upsert_contacts(_rows(1))
        c = db.pending_resolve()[0]
        db.mark_resolved(c["id"], 999, None, None)
        cid = db.create_campaign("t", "hello", None, None)
        db.record_send(cid, c["id"], "sent")
        # "today" is within last 30 days
        assert c["id"] in db.contacts_already_messaged(days=30)
