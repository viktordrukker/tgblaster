"""Job system: arq worker definitions + universal dispatcher.

Two backends:

* **arq + Redis** (Docker mode): jobs survive UI restarts, multiple workers
  can run, graceful retries, locks live in Redis.
* **In-process threads** (standalone mode, no Redis): thin shim that
  schedules the same coroutines on background threads.

The UI calls `dispatch.enqueue_*()` which picks the right backend.
Status is always written to the SQLite `jobs` table so the UI looks
identical in both modes.

Concurrency rules (precedence):
* Only one `resolve_contacts` job runs at a time (Redis lock).
* Only one `run_campaign` job per campaign id runs at a time (Redis lock).
* `run_campaign` jobs may declare `depends_on=resolve_job_id`; the worker
  waits up to 5 minutes for the dependency to finish, then errors out.
"""
from __future__ import annotations

import asyncio
import json
import logging
import threading
from dataclasses import asdict
from typing import Any

from arq.connections import RedisSettings
from arq import cron

import sqlite3 as _sqlite3

from . import auth, locks, redis_client
from .config import (
    load_settings,
    load_account_settings,
    settings_for_account,
)
from .database import Database
from .rate_limiter import PacingConfig
from .read_receipts import fetch_read_outbox_cursors
from .resolver import resolve_pending, validate_pending_usernames
from .sender import StopSignal, run_campaign


log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Job functions — these are the actual work.
# Each takes a `ctx` dict (arq convention) and a typed payload.
# ---------------------------------------------------------------------------

# Worker jobs can tolerate a longer retry budget than the UI — the user
# isn't staring at a spinner.
async def _connect_with_lock_retry(client) -> None:
    await auth.connect_client(client, attempts=5, backoff_sec=2.0)


def _current_executor(ctx: dict) -> str:
    """Detect whether we're being invoked by arq or by our in-process
    thread fallback. arq fills `ctx` with keys like `job_id`, `job_try`,
    `redis`; the thread path calls the job function with `ctx={}`.

    Used by the H4 backend guard to match executor vs job-row tag."""
    if not ctx:
        return "thread"
    # arq reliably populates these — any one is enough evidence.
    if any(k in ctx for k in ("job_id", "job_try", "redis", "enqueue_time")):
        return "arq"
    return "thread"


def _guard_job_backend(db: Database, job_id: int, expected: str) -> bool:
    """Phase H4 — refuse to run a job whose DB row tags a different
    backend. Prevents the race where a thread-enqueued job is later
    picked up by an arq worker (Redis came back online) or vice versa.

    Returns True if we're allowed to proceed. On mismatch, marks the
    job as error so it doesn't linger in running/queued forever."""
    row = db.get_job(int(job_id))
    if row is None:
        return False
    actual = row["backend"] or "arq"  # legacy rows default to arq
    if actual == expected:
        return True
    db.mark_job_error(
        int(job_id),
        f"backend mismatch: job tagged '{actual}', "
        f"executor is '{expected}' — refusing to double-run",
    )
    log.warning(
        "backend guard: job #%s tagged %s but reached %s executor — skipped",
        job_id, actual, expected,
    )
    return False


async def _heartbeat_loop(db: Database, job_id: int,
                           interval_sec: float = 10.0,
                           lock_to_extend: Any = None,
                           lock_ttl_sec: int = 0) -> None:
    """Keep `jobs.last_heartbeat` fresh while the caller is doing work.

    Phase H5b — also extends the Redis lock every tick when one is
    passed in. Combined with a short initial TTL this means: worker
    dies ⇒ lock expires within the TTL and the campaign is recoverable;
    worker alive ⇒ lock is continuously refreshed and holds indefinitely.

    Designed so the watchdog can tell a wedged job (no heartbeat ≥ 3 min)
    from a slow one (FloodWait at 15 min but heartbeat keeps ticking
    every 10 s)."""
    try:
        while True:
            try:
                db.update_job_heartbeat(job_id)
            except Exception:  # noqa: BLE001
                # Never let the heartbeat loop itself kill the run.
                log.warning("heartbeat update failed for job #%s", job_id,
                            exc_info=True)
            if lock_to_extend is not None and lock_ttl_sec > 0:
                try:
                    locks.extend(lock_to_extend, lock_ttl_sec)
                except Exception:  # noqa: BLE001
                    log.warning("lock extend failed for job #%s", job_id,
                                exc_info=True)
            await asyncio.sleep(interval_sec)
    except asyncio.CancelledError:
        return


def _account_for_payload(db: Database, payload: dict):
    """Resolve the AccountSettings that owns this job.

    Order of preference:
      1. payload["account_id"] — explicit (post v1.5 default).
      2. active account in the DB — fallback for pre-upgrade jobs.
      3. `.env`-sourced `load_settings()` — final fallback for tests.
    """
    account_id = payload.get("account_id")
    if account_id is not None:
        s = load_account_settings(db, int(account_id))
        if s is not None:
            return s
    s = load_account_settings(db, None)
    if s is not None:
        return s
    return load_settings()

async def resolve_contacts_job(ctx: dict, payload: dict) -> dict:
    """Resolve pending phones via MTProto. Re-entrant-safe via lock.
    Honors optional `ids` filter in the payload (inline selection)."""
    db_path = payload.get("db_path") or str(load_settings().db_path)
    job_id = payload["job_id"]
    cleanup_imported = bool(payload.get("cleanup_imported", True))
    ids = payload.get("ids") or None

    db = Database(db_path)

    executor = _current_executor(ctx)
    if not _guard_job_backend(db, job_id, executor):
        return {"status": "skipped", "reason": "backend_mismatch"}

    lock = locks.try_acquire("resolve", ttl_sec=3600, backend=executor)
    if lock is None:
        db.mark_job_error(job_id, "another resolve job is already running")
        return {"status": "skipped", "reason": "lock_held"}

    db.mark_job_started(job_id)
    db.update_job_heartbeat(job_id)
    heartbeat_task = asyncio.create_task(_heartbeat_loop(db, job_id))
    try:
        settings = _account_for_payload(db, payload)
        client = auth.get_client(settings.session_path, settings.api_id, settings.api_hash)
        await _connect_with_lock_retry(client)
        if not await client.is_user_authorized():
            db.mark_job_error(job_id, "not authorized — login first")
            return {"status": "error"}

        async def on_progress(payload: dict):
            db.update_job_progress(job_id, json.dumps(payload))

        stats = await resolve_pending(
            client, db, on_progress=on_progress,
            cleanup_imported=cleanup_imported,
            ids=ids,
        )
        # Keep the final emitted progress payload (counters + events) so
        # the UI panel still has `done` and `events` to render after
        # transition to 'done'. Passing None preserves progress_json.
        db.mark_job_done(job_id)
        return {"status": "ok", **stats}
    except Exception as e:  # noqa: BLE001
        log.exception("resolve job failed")
        db.mark_job_error(job_id, str(e))
        return {"status": "error", "error": str(e)}
    finally:
        heartbeat_task.cancel()
        try:
            await heartbeat_task
        except (asyncio.CancelledError, Exception):  # noqa: BLE001
            pass
        locks.release(lock)


async def validate_usernames_job(ctx: dict, payload: dict) -> dict:
    """Resolve pending contacts via stored @username hints. Honors
    optional `ids` filter in the payload (inline selection).

    Honors `depends_on` on its job row — used by `enqueue_prepare` to
    chain Validate after Resolve so a single "Подготовить к кампании"
    click handles both passes. Max wait 5 min; then errors out."""
    db_path = payload.get("db_path") or str(load_settings().db_path)
    job_id = payload["job_id"]
    ids = payload.get("ids") or None
    db = Database(db_path)

    # Wait for chained Resolve (if any) to finish before running Validate.
    waited = 0
    while not db.dependency_satisfied(job_id):
        if waited > 300:
            db.mark_job_error(job_id, "dependency not satisfied after 5 minutes")
            return {"status": "dependency_timeout"}
        await asyncio.sleep(2)
        waited += 2

    executor = _current_executor(ctx)
    if not _guard_job_backend(db, job_id, executor):
        return {"status": "skipped", "reason": "backend_mismatch"}

    lock = locks.try_acquire("validate_usernames", ttl_sec=3600, backend=executor)
    if lock is None:
        db.mark_job_error(job_id, "another validate job is already running")
        return {"status": "skipped", "reason": "lock_held"}

    db.mark_job_started(job_id)
    db.update_job_heartbeat(job_id)
    heartbeat_task = asyncio.create_task(_heartbeat_loop(db, job_id))
    try:
        settings = _account_for_payload(db, payload)
        client = auth.get_client(settings.session_path, settings.api_id, settings.api_hash)
        await _connect_with_lock_retry(client)
        if not await client.is_user_authorized():
            db.mark_job_error(job_id, "not authorized — login first")
            return {"status": "error"}

        async def on_progress(progress_payload: dict):
            db.update_job_progress(job_id, json.dumps(progress_payload))

        stats = await validate_pending_usernames(
            client, db, on_progress=on_progress, ids=ids,
        )
        # Keep the final emitted progress payload (counters + events) so
        # the UI panel still has `done` and `events` to render after
        # transition to 'done'. Passing None preserves progress_json.
        db.mark_job_done(job_id)
        return {"status": "ok", **stats}
    except Exception as e:  # noqa: BLE001
        log.exception("validate usernames job failed")
        db.mark_job_error(job_id, str(e))
        return {"status": "error", "error": str(e)}
    finally:
        heartbeat_task.cancel()
        try:
            await heartbeat_task
        except (asyncio.CancelledError, Exception):  # noqa: BLE001
            pass
        locks.release(lock)


async def run_campaign_job(ctx: dict, payload: dict) -> dict:
    """Run a campaign end-to-end. Locked per-campaign so it can't double-run."""
    db_path = payload.get("db_path") or str(load_settings().db_path)
    job_id = payload["job_id"]
    campaign_id = int(payload["campaign_id"])
    pacing_kwargs = payload.get("pacing", {})
    dry_run = bool(payload.get("dry_run", False))
    skip_recently_messaged_days = int(payload.get("skip_recently_messaged_days", 0))
    only_contact_ids = payload.get("ids") or None

    db = Database(db_path)

    # Block until dependency is done (max 5 minutes).
    waited = 0
    while not db.dependency_satisfied(job_id):
        if waited > 300:
            db.mark_job_error(job_id, "dependency not satisfied after 5 minutes")
            return {"status": "dependency_timeout"}
        await asyncio.sleep(2)
        waited += 2

    executor = _current_executor(ctx)
    if not _guard_job_backend(db, job_id, executor):
        return {"status": "skipped", "reason": "backend_mismatch"}

    # Phase H5b — short TTL (5 min) + heartbeat-driven auto-extend.
    # If the worker dies the lock expires within 5 min (not 24 h) and a
    # manual/automated resume can pick up the run.
    CAMPAIGN_LOCK_TTL = 300
    lock = locks.try_acquire(
        f"campaign:{campaign_id}", ttl_sec=CAMPAIGN_LOCK_TTL, backend=executor,
    )
    if lock is None:
        db.mark_job_error(job_id, "another run for this campaign is in flight")
        return {"status": "skipped", "reason": "lock_held"}

    db.mark_job_started(job_id)
    # Heartbeat from the very moment we're marked running — catches the
    # "stuck at starting" class before sender ever enters its loop.
    db.update_job_heartbeat(job_id)
    # H5b — auto-extend the short-TTL campaign lock as long as we're alive.
    heartbeat_task = asyncio.create_task(_heartbeat_loop(
        db, job_id,
        lock_to_extend=lock, lock_ttl_sec=CAMPAIGN_LOCK_TTL,
    ))
    try:
        settings = _account_for_payload(db, payload)
        client = auth.get_client(settings.session_path, settings.api_id, settings.api_hash)
        await _connect_with_lock_retry(client)
        if not await client.is_user_authorized():
            db.mark_job_error(job_id, "not authorized — login first")
            return {"status": "error"}

        # Cross-campaign dedup: pre-record skips for already-messaged contacts.
        if skip_recently_messaged_days > 0:
            already = db.contacts_already_messaged(skip_recently_messaged_days)
            for cid in already:
                db.record_send(campaign_id, cid, "skipped",
                               f"messaged within last {skip_recently_messaged_days} days")

        # In-campaign dedup on tg_user_id (two phones → same person).
        # Mark the duplicates as 'skipped' so the sender naturally bypasses them.
        for row in db.find_duplicate_tg_users():
            ids = sorted(int(x) for x in row["ids"].split(","))
            for dup_id in ids[1:]:  # keep the lowest, skip the rest
                db.record_send(campaign_id, dup_id, "skipped",
                               "deduplicated: same TG user as another contact")

        pacing = PacingConfig(**pacing_kwargs) if pacing_kwargs else PacingConfig()
        stop_signal = StopSignal()

        # Allow UI to request Pause or Stop via Redis keys. Pause →
        # campaigns.status becomes 'paused' on exit; Stop → 'stopped'.
        async def _watch_stop():
            client_redis = redis_client.get_client()
            if client_redis is None:
                return
            stop_key = f"stop:campaign:{campaign_id}"
            pause_key = f"pause:campaign:{campaign_id}"
            while not stop_signal.is_set():
                try:
                    if client_redis.get(stop_key):
                        stop_signal.set("user_stopped")
                        return
                    if client_redis.get(pause_key):
                        stop_signal.set("user_paused")
                        return
                except Exception:  # noqa: BLE001
                    pass
                # Phase H5 — 1 s poll so Pause/Stop is acted on within
                # ≤ 2 s worst-case (1 s poll + 1 s pacer chunk).
                await asyncio.sleep(1)

        async def on_progress(payload2: dict):
            db.update_job_progress(job_id, json.dumps(payload2))

        watcher = asyncio.create_task(_watch_stop())
        try:
            outcome = await run_campaign(
                client, db, campaign_id, pacing,
                on_progress=on_progress,
                stop_signal=stop_signal,
                dry_run_to_self=dry_run,
                only_contact_ids=only_contact_ids,
            )
        finally:
            watcher.cancel()

        # Phase H6 — translate sender outcome into campaign state.
        if outcome.stopped_reason == "user_paused":
            db.transition_campaign_state(campaign_id, "paused")
        elif outcome.stopped_reason == "user_stopped":
            db.transition_campaign_state(campaign_id, "stopped")
        elif outcome.stopped_reason in ("peer_flood",):
            db.transition_campaign_state(campaign_id, "stopped")
        elif not outcome.stopped_reason:
            db.transition_campaign_state(campaign_id, "done")

        db.mark_job_done(job_id)
        return {"status": "ok", **asdict(outcome)}
    except Exception as e:  # noqa: BLE001
        log.exception("campaign job failed")
        db.mark_job_error(job_id, str(e))
        return {"status": "error", "error": str(e)}
    finally:
        heartbeat_task.cancel()
        try:
            await heartbeat_task
        except (asyncio.CancelledError, Exception):  # noqa: BLE001
            pass
        locks.release(lock)
        # Clear any stop request after we're done.
        c = redis_client.get_client()
        if c is not None:
            try:
                c.delete(f"stop:campaign:{campaign_id}")
            except Exception:  # noqa: BLE001
                pass


# ---------------------------------------------------------------------------
# Phase R — manual read-receipt check.
# ---------------------------------------------------------------------------

async def check_read_receipts_job(ctx: dict, payload: dict) -> dict:
    """Manually-triggered read-status check.

    Scope filters both optional (AND-combined when both present):
      * payload['campaign_ids']: restrict to these campaigns
      * payload['contact_ids']: restrict to these contacts

    No pacing — this is a pull-only RPC (`GetPeerDialogsRequest`) and
    doesn't burn daily send budget. Single-in-flight per worker via
    the `read_check` Redis lock.
    """
    import datetime as _dt
    db_path = payload.get("db_path") or str(load_settings().db_path)
    job_id = payload["job_id"]
    campaign_ids = payload.get("campaign_ids") or None
    contact_ids = payload.get("contact_ids") or None

    db = Database(db_path)

    executor = _current_executor(ctx)
    if not _guard_job_backend(db, job_id, executor):
        return {"status": "skipped", "reason": "backend_mismatch"}

    lock = locks.try_acquire("read_check", ttl_sec=600, backend=executor)
    if lock is None:
        db.mark_job_error(job_id, "another read-check job is already running")
        return {"status": "skipped", "reason": "lock_held"}

    db.mark_job_started(job_id)
    db.update_job_heartbeat(job_id)
    heartbeat_task = asyncio.create_task(_heartbeat_loop(db, job_id))
    try:
        settings = _account_for_payload(db, payload)
        client = auth.get_client(settings.session_path, settings.api_id, settings.api_hash)
        await _connect_with_lock_retry(client)
        if not await client.is_user_authorized():
            db.mark_job_error(job_id, "not authorized — login first")
            return {"status": "error"}

        targets = db.read_check_targets(
            campaign_ids=campaign_ids, contact_ids=contact_ids,
        )
        total = len(targets)

        async def _emit(**kw):
            payload2 = {"total": total, **kw}
            db.update_job_progress(job_id, json.dumps(payload2))

        await _emit(status="starting", checked=0, precise_reads=0, skipped=0)

        if total == 0:
            db.mark_job_done(job_id)
            return {"status": "ok", "total": 0, "precise": 0,
                    "coarse": 0, "skipped": 0}

        peers = [(r["tg_user_id"], r["tg_access_hash"]) for r in targets]
        cursors, skipped_no_hash = await fetch_read_outbox_cursors(client, peers)

        # Fan the per-user cursors back to per-row updates. Each target
        # row holds one contact_id; pair it with the cursor we got for
        # its tg_user_id (default to 0 = "peer never read us" which
        # apply_read_receipts will treat as a no-op).
        updates: list[tuple[int, int]] = [
            (int(r["contact_id"]), int(cursors.get(int(r["tg_user_id"]), 0)))
            for r in targets
        ]
        checked_at_iso = _dt.datetime.now(_dt.timezone.utc).isoformat(timespec="seconds")
        stats = db.apply_read_receipts(
            updates, checked_at_iso=checked_at_iso, campaign_ids=campaign_ids,
        )

        await _emit(
            status="finished",
            checked=total,
            precise_reads=stats["precise"],
            skipped=skipped_no_hash,
        )
        db.mark_job_done(job_id)
        return {
            "status": "ok",
            "total": total,
            "precise": stats["precise"],
            "skipped": skipped_no_hash,
        }
    except Exception as e:  # noqa: BLE001
        log.exception("check_read_receipts job failed")
        db.mark_job_error(job_id, str(e))
        return {"status": "error", "error": str(e)}
    finally:
        heartbeat_task.cancel()
        try:
            await heartbeat_task
        except (asyncio.CancelledError, Exception):  # noqa: BLE001
            pass
        locks.release(lock)


# ---------------------------------------------------------------------------
# Phase 6 — cron: periodic Google Sheets sync.
# ---------------------------------------------------------------------------

async def sync_all_gsheet_sources(ctx: dict) -> dict:
    """Fires every minute. Reads the single global `sheet_sources` row
    (v1.5 rework — only one source feeds the DB). If its cadence has
    elapsed, it:

      1. Downloads the sheet (public-share CSV export).
      2. Parses using the saved column map (Contacts-page setup).
      3. `upsert_contacts` — phone is UNIQUE so re-syncs are idempotent.
      4. Computes delta = contact ids added by *this* upsert.
      5. If `auto_resolve=1` and delta is non-empty, enqueues a single
         `resolve_contacts_job(ids=delta)` so new rows move through the
         pipeline to 'resolved' without human action.
      6. Does NOT auto-send — campaigns stay user-driven and only fire
         against the already-validated DB.

    Overlapping ticks are impossible: a per-source Redis lock with TTL
    close to cadence handles that.
    """
    import datetime as _dt
    import hashlib
    import io as _io
    import json as _json
    import re as _re
    import requests

    from .csv_io import parse_with_mapping, contacts_to_db_rows

    db_path = str(load_settings().db_path)
    db = Database(db_path)
    now_iso = _dt.datetime.now(_dt.timezone.utc).isoformat(timespec="seconds")

    s = db.get_sheet_source()
    if s is None:
        return {"status": "no_source"}

    cadence = s["cadence_min"]
    if not cadence:
        return {"status": "manual_only"}

    last = s["last_synced_at"]
    if last is not None:
        try:
            last_dt = _dt.datetime.fromisoformat(last.replace("Z", "+00:00"))
            if (_dt.datetime.now(_dt.timezone.utc) - last_dt).total_seconds() < int(cadence) * 60:
                return {"status": "not_due"}
        except Exception:
            pass

    source_id = int(s["id"])
    ttl = max(min(int(cadence) * 60 - 10, 55), 5)
    lock = locks.try_acquire(f"gsheet_sync:{source_id}", ttl_sec=ttl)
    if lock is None:
        log.info("gsheet_sync:%s — lock held, skipping", source_id)
        return {"status": "lock_held"}

    try:
        try:
            column_map = _json.loads(s["column_map_json"] or "{}")
        except Exception:
            log.warning("gsheet_sync:%s — bad column_map_json", source_id)
            return {"status": "bad_map"}

        url = s["url"] or ""
        m = _re.search(r"/spreadsheets/d/([A-Za-z0-9_\-]+)", url)
        if not m:
            log.warning("gsheet_sync:%s — bad url", source_id)
            return {"status": "bad_url"}
        sheet_id = m.group(1)
        gid_match = _re.search(r"[?&#]gid=(\d+)", url)
        export_url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv"
        if gid_match:
            export_url += f"&gid={gid_match.group(1)}"

        try:
            resp = requests.get(export_url, timeout=30, allow_redirects=True)
        except requests.RequestException as e:
            log.warning("gsheet_sync:%s download failed: %s", source_id, e)
            return {"status": "network_error", "error": str(e)}
        if (
            resp.status_code != 200
            or not resp.content
            or b"<!DOCTYPE html" in resp.content[:500]
        ):
            log.warning("gsheet_sync:%s bad response HTTP %s",
                        source_id, resp.status_code)
            return {"status": "http_error", "http_status": resp.status_code}

        try:
            valid, _invalid, _dups = parse_with_mapping(
                _io.BytesIO(resp.content),
                column_map=column_map,
                default_region="UZ",
            )
        except Exception as e:  # noqa: BLE001
            log.warning("gsheet_sync:%s parse failed: %s", source_id, e)
            return {"status": "parse_error", "error": str(e)}

        before_ids = {int(i) for i in db.all_contacts_df()["id"].tolist()}
        db.upsert_contacts(contacts_to_db_rows(valid))
        after_ids = {int(i) for i in db.all_contacts_df()["id"].tolist()}
        new_ids = sorted(after_ids - before_ids)

        phones_signature = "\n".join(
            sorted(c.phone for c in valid if c.phone)
        ).encode("utf-8")
        signature = hashlib.sha1(phones_signature).hexdigest()
        db.update_sheet_source(
            last_synced_at=now_iso,
            last_seen_phone_set_hash=signature,
            rows_seen=len(valid),
        )

        auto_resolve = bool(s["auto_resolve"])
        resolve_job_id: int | None = None
        if new_ids and auto_resolve:
            try:
                disp = Dispatcher(db)  # uses DB's active account
                resolve_job_id = disp.enqueue_resolve(
                    cleanup_imported=True, ids=new_ids,
                )
                log.info(
                    "gsheet_sync:%s → %d new contacts, queued resolve #%d",
                    source_id, len(new_ids), resolve_job_id,
                )
            except RuntimeError as e:
                log.info("gsheet_sync:%s could not enqueue resolve: %s",
                         source_id, e)

        return {
            "status": "ok",
            "rows_seen": len(valid),
            "new_contact_ids": len(new_ids),
            "resolve_job_id": resolve_job_id,
        }
    finally:
        locks.release(lock)


# ---------------------------------------------------------------------------
# Phase H3b — watchdog recovery.
# ---------------------------------------------------------------------------

def _release_job_locks(job_row: dict | Any) -> None:
    """Release any Redis lock a dead job might still hold.

    Safe to call on a job that never acquired anything — `locks.release`
    checks the key existence internally. We don't know which lock key a
    resolve/validate/campaign job took without re-parsing the payload,
    so we try all three known shapes."""
    try:
        job_type = job_row["type"]
    except Exception:
        return
    try:
        payload = json.loads(job_row["payload_json"] or "{}")
    except Exception:
        payload = {}
    candidates: list[str] = []
    if job_type == "resolve_contacts":
        candidates.append("resolve")
    elif job_type == "validate_usernames":
        candidates.append("validate_usernames")
    elif job_type == "run_campaign":
        cid = payload.get("campaign_id")
        if cid is not None:
            candidates.append(f"campaign:{int(cid)}")
    c = redis_client.get_client()
    if c is None:
        return
    for key in candidates:
        try:
            c.delete(f"lock:{key}")
        except Exception:  # noqa: BLE001
            pass


def release_stuck_job(db: Database, job_id: int, reason: str) -> bool:
    """Mark a job as `error` with `reason`, release its Redis lock, and
    return True if anything changed. Used by both the watchdog cron and
    the Campaign page's manual 🧹 unstick button."""
    row = db.get_job(int(job_id))
    if row is None:
        return False
    if row["status"] not in ("queued", "running"):
        return False
    db.mark_job_error(int(job_id), reason)
    _release_job_locks(row)
    return True


async def watchdog_job(ctx: dict) -> dict:
    """Periodic sweep: find stuck queued/running jobs and recover them.

    Three signatures (all enforced via `Database.find_stale_jobs`):
      1. queued > 2 min with no started_at → arq never picked it up.
      2. running with no heartbeat for ≥ 3 min → worker died mid-send.
      3. running but `progress_json.status == 'starting'` for > 90 s →
         send loop never advanced past connect.
    """
    db_path = str(load_settings().db_path)
    db = Database(db_path)
    stale = db.find_stale_jobs()
    recovered = []
    for row in stale:
        if release_stuck_job(db, row["id"], row["reason"]):
            recovered.append(row["id"])
            log.warning("watchdog: recovered job #%s (%s)",
                        row["id"], row["reason"])
    return {"scanned": len(stale), "recovered": recovered}


# ---------------------------------------------------------------------------
# arq worker settings — `python -m arq core.jobs.WorkerSettings` runs this.
# ---------------------------------------------------------------------------

class WorkerSettings:
    functions = [resolve_contacts_job, validate_usernames_job,
                 run_campaign_job, check_read_receipts_job]
    # One cron — per-source firing filtered inside the handler. Avoids
    # arq's "one cron entry per source" trap and lets us add/remove
    # sources at runtime without worker restart. Watchdog runs every 2 min.
    cron_jobs = [
        cron(sync_all_gsheet_sources, minute=set(range(60))),
        cron(watchdog_job, minute={0, 2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22,
                                    24, 26, 28, 30, 32, 34, 36, 38, 40, 42,
                                    44, 46, 48, 50, 52, 54, 56, 58}),
    ]
    redis_settings = RedisSettings.from_dsn(redis_client.redis_url())
    job_timeout = 86400  # 24h — campaigns can be long
    max_jobs = 4
    keep_result = 3600


# ---------------------------------------------------------------------------
# Universal dispatch — picks Redis or in-process backend.
# ---------------------------------------------------------------------------

class Dispatcher:
    """Enqueue jobs without caring about the backend."""

    def __init__(self, db: Database, account_id: int | None = None):
        self.db = db
        self.account_id = account_id

    def _active_account_id(self) -> int | None:
        """Fall back to the DB's active account if one wasn't pinned at init."""
        if self.account_id is not None:
            return self.account_id
        row = self.db.get_active_account()
        return int(row["id"]) if row is not None else None

    # --- public API ----------------------------------------------------

    def enqueue_resolve(self, cleanup_imported: bool = True,
                        ids: list[int] | None = None) -> int:
        if self.db.has_running_job("resolve_contacts"):
            raise RuntimeError("Resolve уже бежит — подожди завершения")
        ids_payload = [int(x) for x in ids] if ids else None
        job_id = self.db.create_job(
            "resolve_contacts",
            payload_json=json.dumps({
                "cleanup_imported": cleanup_imported,
                "ids": ids_payload,
            }),
            backend="arq" if redis_client.is_redis_available() else "thread",
        )
        payload = {
            "job_id": job_id,
            "cleanup_imported": cleanup_imported,
            "db_path": str(self.db.path),
            "account_id": self._active_account_id(),
            "ids": ids_payload,
        }
        if redis_client.is_redis_available():
            self._enqueue_arq("resolve_contacts_job", payload)
        else:
            self._enqueue_thread(resolve_contacts_job, payload)
        return job_id

    def enqueue_validate_usernames(self, ids: list[int] | None = None,
                                   depends_on: int | None = None) -> int:
        """Enqueue the @username validate pass.

        `depends_on` lets a combined "prepare" flow wait for an earlier
        Resolve job to finish before Validate starts. Without the wait
        Validate would race Resolve for the same error rows and either
        fight for the same contact state or re-error on rows Resolve
        was about to fix."""
        if self.db.has_running_job("validate_usernames"):
            raise RuntimeError("Валидация уже бежит — подожди завершения")
        backend = "arq" if redis_client.is_redis_available() else "thread"
        ids_payload = [int(x) for x in ids] if ids else None
        job_id = self.db.create_job(
            "validate_usernames",
            payload_json=json.dumps({"ids": ids_payload}),
            backend=backend,
            depends_on=depends_on,
        )
        payload = {
            "job_id": job_id,
            "db_path": str(self.db.path),
            "account_id": self._active_account_id(),
            "ids": ids_payload,
        }
        if backend == "arq":
            self._enqueue_arq("validate_usernames_job", payload)
        else:
            self._enqueue_thread(validate_usernames_job, payload)
        return job_id

    def enqueue_prepare(self, cleanup_imported: bool = True) -> dict:
        """One-click campaign prep: enqueue Resolve + Validate as needed.

        Decides per current DB state:
          * pending phone-rows → schedule Resolve
          * pending @handle-rows → schedule Validate (chained after
            Resolve if both fire, so Validate only touches rows Resolve
            couldn't fix)

        Either step may produce *terminal* non-resolved rows — `phone
        without Telegram` becomes `not_on_telegram`, a bad @handle
        becomes `error`. Neither aborts the chain; both leave the
        campaign free to fire against whatever IS resolved. The UI
        surfaces these as "unreachable" so the user knows what was
        left behind.

        Returns `{"resolve_job_id": int|None, "validate_job_id": int|None,
                  "skipped": bool}`. `skipped=True` means nothing to do.
        """
        workload = self.db.count_prepare_workload()
        need_resolve = workload["phone_resolvable"] > 0
        need_validate = workload["username_validatable"] > 0
        if not (need_resolve or need_validate):
            return {"resolve_job_id": None, "validate_job_id": None,
                    "skipped": True}

        result = {"resolve_job_id": None, "validate_job_id": None,
                  "skipped": False}
        resolve_job_id: int | None = None
        if need_resolve:
            resolve_job_id = self.enqueue_resolve(
                cleanup_imported=cleanup_imported,
            )
            result["resolve_job_id"] = resolve_job_id
        if need_validate:
            # If Resolve is also running, chain Validate after it so the
            # two don't fight over the same 'error' rows; Validate then
            # only sees what Resolve couldn't fix.
            result["validate_job_id"] = self.enqueue_validate_usernames(
                depends_on=resolve_job_id,
            )
        return result

    def enqueue_campaign(self, campaign_id: int, pacing: PacingConfig,
                         dry_run: bool = False,
                         skip_recently_messaged_days: int = 0,
                         depends_on: int | None = None,
                         ids: list[int] | None = None) -> int:
        # One in-flight job per campaign.
        for job in self.db.list_jobs(limit=200):
            if (job["type"] == "run_campaign"
                and job["status"] in ("queued", "running")):
                payload = json.loads(job["payload_json"] or "{}")
                if payload.get("campaign_id") == campaign_id:
                    raise RuntimeError(
                        f"Кампания #{campaign_id} уже в очереди или бежит "
                        f"(job #{job['id']})"
                    )
        # Prefer the campaign's own account_id (set at create time), fall
        # back to the dispatcher's active account for pre-v1.5 rows.
        campaign_row = self.db.get_campaign(campaign_id)
        campaign_account = (
            int(campaign_row["account_id"])
            if campaign_row is not None and campaign_row["account_id"] is not None
            else self._active_account_id()
        )
        payload = {
            "campaign_id": campaign_id,
            "pacing": asdict(pacing),
            "dry_run": dry_run,
            "skip_recently_messaged_days": skip_recently_messaged_days,
            "db_path": str(self.db.path),
            "account_id": campaign_account,
            "ids": [int(x) for x in ids] if ids else None,
        }
        backend = "arq" if redis_client.is_redis_available() else "thread"
        job_id = self.db.create_job(
            "run_campaign", payload_json=json.dumps(payload),
            backend=backend, depends_on=depends_on,
        )
        payload["job_id"] = job_id
        if backend == "arq":
            self._enqueue_arq("run_campaign_job", payload)
        else:
            self._enqueue_thread(run_campaign_job, payload)
        return job_id

    def enqueue_read_check(self,
                           campaign_ids: list[int] | None = None,
                           contact_ids: list[int] | None = None) -> int:
        """Phase R — enqueue a read-receipt check.

        Scope rules (both optional, AND-combined):
          * campaign_ids=None, contact_ids=None → every sent row.
          * campaign_ids=[C]                    → only campaign C.
          * contact_ids=[K1,K2]                 → only those contacts,
                                                  all their campaigns.
          * both                                → intersection.
        """
        if self.db.has_running_job("check_read_receipts"):
            raise RuntimeError("Read-check уже бежит — подожди завершения")
        backend = "arq" if redis_client.is_redis_available() else "thread"
        cids = [int(x) for x in campaign_ids] if campaign_ids else None
        kids = [int(x) for x in contact_ids] if contact_ids else None
        job_id = self.db.create_job(
            "check_read_receipts",
            payload_json=json.dumps({
                "campaign_ids": cids,
                "contact_ids": kids,
            }),
            backend=backend,
        )
        payload = {
            "job_id": job_id,
            "campaign_ids": cids,
            "contact_ids": kids,
            "db_path": str(self.db.path),
            "account_id": self._active_account_id(),
        }
        if backend == "arq":
            self._enqueue_arq("check_read_receipts_job", payload)
        else:
            self._enqueue_thread(check_read_receipts_job, payload)
        return job_id

    def request_stop(self, campaign_id: int) -> bool:
        """Ask the worker to stop the campaign politely → status=stopped."""
        c = redis_client.get_client()
        if c is not None:
            c.set(f"stop:campaign:{campaign_id}", "1", ex=300)
            return True
        _THREAD_STOP_FLAGS[("campaign", campaign_id)] = True
        return True

    def request_pause(self, campaign_id: int) -> bool:
        """Ask the worker to pause the campaign → status=paused.
        Resume later picks up where we left off via the H2 pending-row
        + already_sent_ids skip-set."""
        c = redis_client.get_client()
        if c is not None:
            c.set(f"pause:campaign:{campaign_id}", "1", ex=300)
            return True
        _THREAD_STOP_FLAGS[("campaign", campaign_id)] = True
        return True

    def clear_campaign_signals(self, campaign_id: int) -> None:
        """Wipe any lingering pause/stop keys for this campaign — called
        right before a fresh Start so a stale pause doesn't immediately
        end the new run."""
        c = redis_client.get_client()
        if c is None:
            return
        try:
            c.delete(f"stop:campaign:{campaign_id}")
            c.delete(f"pause:campaign:{campaign_id}")
        except Exception:  # noqa: BLE001
            pass

    # --- internals -----------------------------------------------------

    def _enqueue_arq(self, function_name: str, payload: dict) -> None:
        """Push a job onto the arq Redis queue from sync code."""
        from arq import create_pool

        async def push():
            settings = WorkerSettings.redis_settings
            pool = await create_pool(settings)
            try:
                await pool.enqueue_job(function_name, payload)
            finally:
                await pool.close()

        # We're called from Streamlit (sync). Run a fresh loop.
        try:
            asyncio.run(push())
        except RuntimeError:
            # Already inside a loop — fall back to thread-runner.
            log.warning("arq enqueue from inside a loop; using thread fallback")
            self._enqueue_thread(globals()[function_name], payload)

    def _enqueue_thread(self, async_func, payload: dict) -> None:
        """Run the job on a background thread (no Redis needed)."""
        def runner():
            try:
                asyncio.run(async_func({}, payload))
            except Exception as e:  # noqa: BLE001
                log.exception("Thread job crashed: %s", e)
                try:
                    db = Database(payload.get("db_path") or load_settings().db_path)
                    db.mark_job_error(payload["job_id"], str(e))
                except Exception:  # noqa: BLE001
                    pass

        t = threading.Thread(target=runner, daemon=True,
                             name=f"job-{payload.get('job_id', '?')}")
        t.start()


# Per-process flags used by the in-process StopSignal watcher.
# Keyed by (job_type, ref_id).
_THREAD_STOP_FLAGS: dict[tuple[str, int], bool] = {}
