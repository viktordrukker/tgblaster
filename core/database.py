"""SQLite state store for campaigns, contacts, and send log.

Design goals:
- Idempotent schema creation (safe to call on every start).
- One DB file per project instance: `data/state.db` (legacy deploys
  that have `data/meetup.db` keep using it — see `core.config`).
- Only stores what's needed to resume a campaign after a crash.
"""
from __future__ import annotations

import re
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Iterator


_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


SCHEMA = """
CREATE TABLE IF NOT EXISTS contacts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    phone TEXT UNIQUE NOT NULL,          -- E.164 normalized
    name TEXT,
    raw_phone TEXT,                      -- original from CSV
    extra_json TEXT,                     -- any other CSV columns (JSON blob)
    tg_user_id INTEGER,                  -- resolved Telegram numeric ID
    tg_username TEXT,                    -- resolved username (if any)
    tg_access_hash INTEGER,              -- optional, useful for InputPeerUser
    resolved_at TEXT,                    -- ISO timestamp when resolve ran
    resolve_status TEXT DEFAULT 'pending',  -- pending | resolved | not_on_telegram | error
    resolve_error TEXT,
    created_at TEXT DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_contacts_resolve_status ON contacts(resolve_status);
CREATE INDEX IF NOT EXISTS idx_contacts_tg_user_id ON contacts(tg_user_id);

CREATE TABLE IF NOT EXISTS campaigns (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    template TEXT NOT NULL,
    image_path TEXT,
    group_link TEXT,
    status TEXT DEFAULT 'draft',         -- draft | running | paused | done
    created_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS send_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    campaign_id INTEGER NOT NULL,
    contact_id INTEGER NOT NULL,
    status TEXT NOT NULL,                -- sent | skipped | error | opted_out
    detail TEXT,
    sent_at TEXT DEFAULT (datetime('now')),
    UNIQUE(campaign_id, contact_id),
    FOREIGN KEY(campaign_id) REFERENCES campaigns(id),
    FOREIGN KEY(contact_id) REFERENCES contacts(id)
);

CREATE INDEX IF NOT EXISTS idx_send_log_campaign ON send_log(campaign_id, status);

CREATE TABLE IF NOT EXISTS opt_outs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    tg_user_id INTEGER UNIQUE NOT NULL,
    reason TEXT,
    created_at TEXT DEFAULT (datetime('now'))
);

-- Job queue / status table. Filled both by arq workers and by the
-- in-process thread runner so the UI shows the same shape regardless of
-- which backend is active.
CREATE TABLE IF NOT EXISTS jobs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    type TEXT NOT NULL,                  -- resolve_contacts | run_campaign
    payload_json TEXT,                   -- args as JSON
    status TEXT DEFAULT 'queued',        -- queued | running | done | error | cancelled
    progress_json TEXT,                  -- last progress payload
    error TEXT,
    backend TEXT,                        -- arq | thread
    depends_on INTEGER,                  -- another job id, or NULL
    queued_at TEXT DEFAULT (datetime('now')),
    started_at TEXT,
    finished_at TEXT,
    FOREIGN KEY(depends_on) REFERENCES jobs(id)
);

CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status);
CREATE INDEX IF NOT EXISTS idx_jobs_type ON jobs(type, status);

-- v1.5 — multi-account. An account is a Telegram *user* session; bots are
-- out of scope for v1.5. The .env-based single-session setup remains the
-- source of bootstrap values on first boot (see `_seed_default_account`).
CREATE TABLE IF NOT EXISTS accounts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    label TEXT UNIQUE NOT NULL,
    api_id INTEGER NOT NULL,
    api_hash TEXT NOT NULL,
    session_name TEXT NOT NULL,
    is_active INTEGER NOT NULL DEFAULT 0,
    notes TEXT,
    created_at TEXT DEFAULT (datetime('now'))
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_accounts_session_name ON accounts(session_name);

-- v1.5 — campaign-bound data sources. Today only 'gsheet'; the type column
-- is the extension point for csv_file / manual-list / crm later.
CREATE TABLE IF NOT EXISTS campaign_sources (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    campaign_id INTEGER NOT NULL REFERENCES campaigns(id),
    type TEXT NOT NULL DEFAULT 'gsheet',
    url TEXT,
    column_map_json TEXT,               -- {"phone":"col", "name":"col", "username":"col", "extra":["c1","c2"]}
    cadence_min INTEGER,                -- NULL = manual-only
    last_synced_at TEXT,
    last_seen_phone_set_hash TEXT,      -- sha1 of sorted phones last seen in the sheet
    rows_seen INTEGER NOT NULL DEFAULT 0,
    created_at TEXT DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_campaign_sources_campaign
    ON campaign_sources(campaign_id);

-- v1.5 rework — the canonical "external data source that feeds the whole
-- contacts DB". Exactly one row is expected (singleton pattern). The
-- per-campaign `campaign_sources` table above is kept for backward compat
-- but is no longer written to by the UI; cron reads from here.
CREATE TABLE IF NOT EXISTS sheet_sources (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    label TEXT,
    url TEXT,
    column_map_json TEXT,
    cadence_min INTEGER,
    last_synced_at TEXT,
    last_seen_phone_set_hash TEXT,
    rows_seen INTEGER NOT NULL DEFAULT 0,
    auto_resolve INTEGER NOT NULL DEFAULT 1,  -- 1 = enqueue resolve_contacts for new ids after sync
    created_at TEXT DEFAULT (datetime('now'))
);

-- v1.5 — saved filters (lightweight "cohort" recipe that re-runs on demand).
CREATE TABLE IF NOT EXISTS saved_filters (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT UNIQUE NOT NULL,
    filter_json TEXT NOT NULL,
    created_at TEXT DEFAULT (datetime('now'))
);

-- v1.5 — phone_aliases. When a user corrects a contact's phone (real or
-- synthetic `tg:<handle>`), the old value is recorded here so subsequent
-- syncs from an upstream source that still has the typo don't spawn a
-- duplicate — upsert_contacts consults this table first.
CREATE TABLE IF NOT EXISTS phone_aliases (
    alias_phone TEXT PRIMARY KEY,
    contact_id INTEGER NOT NULL REFERENCES contacts(id) ON DELETE CASCADE,
    created_at TEXT DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_phone_aliases_contact
    ON phone_aliases(contact_id);
"""


class Database:
    """Thin wrapper around sqlite3 with the small set of ops we need."""

    def __init__(self, path: Path | str):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        # One persistent connection per thread. Streamlit runs each
        # user-session in its own thread, and on WSL2 bind-mounts
        # `sqlite3.connect()` costs ~30 ms per call — the sidebar was
        # paying that ~5× on every page click. We amortize it to once
        # per thread by caching on threading.local.
        import threading as _threading
        self._tls = _threading.local()
        self._init_file_pragmas()
        self._ensure_schema()

    def _init_file_pragmas(self) -> None:
        """File-level PRAGMAs (WAL, synchronous) set once at init. WAL is
        persistent on the file itself; we don't need to re-assert it per
        connection."""
        try:
            conn = sqlite3.connect(self.path, timeout=5.0)
            try:
                conn.execute("PRAGMA journal_mode = WAL")
                conn.execute("PRAGMA synchronous = NORMAL")
                conn.commit()
            finally:
                conn.close()
        except sqlite3.Error:
            pass

    def _get_conn(self) -> sqlite3.Connection:
        """Return the thread-local SQLite connection, opening it lazily.

        Uses `check_same_thread=False` so we can survive Streamlit's
        fragment reruns being scheduled on helper threads — safe because
        there's still only one connection per thread, and SQLite (under
        WAL + busy_timeout) tolerates concurrent connections from
        different threads."""
        conn: sqlite3.Connection | None = getattr(self._tls, "conn", None)
        if conn is None:
            conn = sqlite3.connect(
                self.path, timeout=5.0, check_same_thread=False,
                isolation_level="DEFERRED",
            )
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA foreign_keys = ON")
            conn.execute("PRAGMA busy_timeout = 5000")
            self._tls.conn = conn
        return conn

    # ---------- low level ---------------------------------------------------

    @contextmanager
    def _conn(self) -> Iterator[sqlite3.Connection]:
        """Yield the thread-local connection inside a txn boundary.

        Commit on clean exit, rollback on exception, never close — the
        connection is reused across calls. Closing connections was the
        dominant cost on WSL2 bind-mounts.
        """
        conn = self._get_conn()
        try:
            yield conn
            conn.commit()
        except Exception:
            try:
                conn.rollback()
            except sqlite3.Error:
                pass
            raise

    @staticmethod
    def _retry_on_lock(op, attempts: int = 8, backoff: float = 0.2):
        """Run a sqlite-touching callable and retry on transient
        'database is locked' errors. The `_conn()` context manager's
        busy_timeout already covers most contention; this is the belt
        around the suspenders for the hot writes on the send path —
        `record_send`, `mark_job_*`, `update_job_progress`.

        Exponential backoff: 0.2, 0.4, 0.8, 1.6, 3.2, 6.4, 12.8, 25.6 s
        (cumulative ~50 s worst case). The earlier 4 × linear backoff
        (~3 s total) was too short under WSL2 bind-mount jitter plus
        concurrent Telethon session writes — callers gave up mid-
        contention and the UI surfaced spurious `database is locked`
        errors."""
        last_err: sqlite3.OperationalError | None = None
        import time as _time
        for attempt in range(attempts):
            try:
                return op()
            except sqlite3.OperationalError as e:
                if "locked" not in str(e).lower():
                    raise
                last_err = e
                _time.sleep(backoff * (2 ** attempt))
        if last_err is not None:
            raise last_err
        return None  # pragma: no cover - unreachable

    def _ensure_schema(self) -> None:
        with self._conn() as c:
            c.executescript(SCHEMA)
            # Idempotent migrations for columns added after the initial release.
            contacts_cols = {r[1] for r in c.execute("PRAGMA table_info(contacts)")}
            if "tg_username_hint" not in contacts_cols:
                c.execute("ALTER TABLE contacts ADD COLUMN tg_username_hint TEXT")

            # v1.5 — campaigns.account_id (nullable, FK to accounts.id).
            campaigns_cols = {r[1] for r in c.execute("PRAGMA table_info(campaigns)")}
            if "account_id" not in campaigns_cols:
                c.execute("ALTER TABLE campaigns ADD COLUMN account_id INTEGER")
            # v1.5 — campaigns.tags (JSON array of short labels).
            if "tags" not in campaigns_cols:
                c.execute("ALTER TABLE campaigns ADD COLUMN tags TEXT DEFAULT '[]'")

            # Phase H3b — jobs.last_heartbeat keeps "this worker is alive"
            # so the watchdog can tell a frozen job from a slow one.
            jobs_cols = {r[1] for r in c.execute("PRAGMA table_info(jobs)")}
            if "last_heartbeat" not in jobs_cols:
                c.execute("ALTER TABLE jobs ADD COLUMN last_heartbeat TEXT")

            # Phase H2 — idempotent sends. `random_id` is the int64 Telegram
            # uses for server-side dedup: passing the same value twice within
            # the short dedup window is a no-op. `attempt_count` tracks how
            # many times we've tried a given contact for diagnostics.
            sendlog_cols = {r[1] for r in c.execute("PRAGMA table_info(send_log)")}
            if "random_id" not in sendlog_cols:
                c.execute("ALTER TABLE send_log ADD COLUMN random_id INTEGER")
            if "attempt_count" not in sendlog_cols:
                c.execute("ALTER TABLE send_log ADD COLUMN attempt_count INTEGER NOT NULL DEFAULT 0")

            # Phase R1 — read receipts. `message_id` is Telegram's per-
            # message int we need to compare against peer.read_outbox_max_id.
            # Captured at confirm-send time (see sender.py). `read_at` is
            # NULL until a manual read-check detects it; only precise
            # matches (message_id ≤ cursor) are flagged — the earlier
            # coarse fallback produced false-positives because Telegram's
            # cursor is monotonic across ALL history, not just this send.
            if "message_id" not in sendlog_cols:
                c.execute("ALTER TABLE send_log ADD COLUMN message_id INTEGER")
            new_readat = "read_at" not in sendlog_cols
            if new_readat:
                c.execute("ALTER TABLE send_log ADD COLUMN read_at TEXT")

        # One-shot migration: wipe any rows that got a false-positive
        # `read_at` from the removed coarse fallback. Runs outside the
        # schema-setup txn so a rollback inside `executescript` doesn't
        # undo it. Idempotent — zero rows after the first run.
        if not new_readat:
            try:
                cleared = self.clear_coarse_read_flags()
                if cleared:
                    import logging as _logging
                    _logging.getLogger(__name__).info(
                        "cleared %d false-positive coarse read_at flags",
                        cleared,
                    )
            except Exception:  # noqa: BLE001
                pass

    # ---------- contacts ----------------------------------------------------

    def upsert_contacts(self, rows: Iterable[dict]) -> int:
        """Insert new contacts; skip duplicates by phone. Returns inserted count.

        Skips inserts whose phone is registered in `phone_aliases` — those
        represent typo versions the user has already corrected. For aliased
        rows we just ensure the *current* contact has a hint (back-fill if
        absent) so re-imports don't lose data.

        When a row matches an existing phone directly, an absent hint is
        also back-filled so re-syncs with a newly-added username don't
        silently drop it.
        """
        inserted = 0
        with self._conn() as c:
            for r in rows:
                phone = r["phone"]
                # 1) Alias check — this phone was the "wrong" spelling of
                #    a contact whose handle the user has since corrected.
                alias_row = c.execute(
                    "SELECT contact_id FROM phone_aliases WHERE alias_phone=?",
                    (phone,),
                ).fetchone()
                if alias_row is not None:
                    aliased_id = int(alias_row[0])
                    # Back-fill the current contact's hint if it's empty;
                    # but never overwrite a user-curated hint.
                    if r.get("tg_username_hint"):
                        c.execute(
                            """UPDATE contacts
                               SET tg_username_hint = ?
                               WHERE id = ?
                                 AND (tg_username_hint IS NULL
                                      OR tg_username_hint = '')""",
                            (r["tg_username_hint"], aliased_id),
                        )
                    continue  # do NOT create a duplicate row

                # 2) Normal insert path.
                cur = c.execute(
                    """
                    INSERT OR IGNORE INTO contacts
                      (phone, name, raw_phone, extra_json, tg_username_hint)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (
                        phone, r.get("name"), r.get("raw_phone"),
                        r.get("extra_json"), r.get("tg_username_hint"),
                    ),
                )
                if cur.rowcount:
                    inserted += cur.rowcount
                elif r.get("tg_username_hint"):
                    c.execute(
                        """UPDATE contacts
                           SET tg_username_hint = ?
                           WHERE phone = ?
                             AND (tg_username_hint IS NULL OR tg_username_hint = '')""",
                        (r["tg_username_hint"], phone),
                    )
        return inserted

    def pending_resolve(self, limit: int | None = None,
                        ids: list[int] | None = None) -> list[sqlite3.Row]:
        """Contacts eligible for phone-based resolve.

        Includes `pending` and `error` rows — the latter are retried on the
        assumption that a prior failure was transient (FloodWait) or the
        user has since corrected the phone. Excludes synthetic 'tg:…'
        placeholders (username-only rows).
        """
        with self._conn() as c:
            q = ("SELECT * FROM contacts "
                 "WHERE resolve_status IN ('pending', 'error') "
                 "AND phone NOT LIKE 'tg:%'")
            params: list = []
            if ids:
                placeholders = ",".join("?" * len(ids))
                q += f" AND id IN ({placeholders})"
                params.extend(int(x) for x in ids)
            if limit:
                q += " LIMIT ?"
                params.append(int(limit))
            return list(c.execute(q, params))

    def mark_resolved(self, contact_id: int, tg_user_id: int, username: str | None,
                      access_hash: int | None) -> None:
        with self._conn() as c:
            c.execute(
                """UPDATE contacts SET tg_user_id=?, tg_username=?, tg_access_hash=?,
                   resolved_at=?, resolve_status='resolved', resolve_error=NULL
                   WHERE id=?""",
                (tg_user_id, username, access_hash, datetime.now(timezone.utc).isoformat(), contact_id),
            )

    def mark_not_on_tg(self, contact_id: int) -> None:
        with self._conn() as c:
            c.execute(
                """UPDATE contacts SET resolve_status='not_on_telegram',
                   resolved_at=? WHERE id=?""",
                (datetime.now(timezone.utc).isoformat(), contact_id),
            )

    def mark_resolve_error(self, contact_id: int, error: str) -> None:
        with self._conn() as c:
            c.execute(
                """UPDATE contacts SET resolve_status='error', resolve_error=?,
                   resolved_at=? WHERE id=?""",
                (error[:500], datetime.now(timezone.utc).isoformat(), contact_id),
            )

    # --- cheap counts — the sidebar / KPI metrics used to do
    # `len(all_contacts_df())` which loads every row into pandas just to
    # count them. These are direct COUNT(*) / GROUP BY queries — O(1)
    # memory, no DataFrame construction. -------------------------------

    def count_contacts(self) -> int:
        with self._conn() as c:
            return int(c.execute("SELECT COUNT(*) FROM contacts").fetchone()[0])

    def count_by_resolve_status(self) -> dict[str, int]:
        """Single query returning the count for every resolve_status
        value present. Avoids four round-trips for the four KPIs."""
        result = {
            "pending": 0, "resolved": 0,
            "not_on_telegram": 0, "error": 0,
        }
        with self._conn() as c:
            for row in c.execute(
                "SELECT resolve_status, COUNT(*) FROM contacts GROUP BY resolve_status"
            ):
                result[row[0] or "pending"] = int(row[1])
        return result

    def count_resolved(self) -> int:
        with self._conn() as c:
            return int(c.execute(
                "SELECT COUNT(*) FROM contacts "
                "WHERE resolve_status='resolved' AND tg_user_id IS NOT NULL"
            ).fetchone()[0])

    def count_running_jobs(self) -> int:
        """How many jobs are currently queued or running (any type)."""
        with self._conn() as c:
            return int(c.execute(
                "SELECT COUNT(*) FROM jobs WHERE status IN ('queued', 'running')"
            ).fetchone()[0])

    def count_prepare_workload(self) -> dict[str, int]:
        """Breakdown of pending/error contacts by the preparation step
        that can move them forward. Used by the "Подготовить к кампании"
        button to decide which jobs to enqueue.

        * `phone_resolvable` — rows with a real phone (not `tg:...`)
          that need `ImportContacts`. A contact counts here even if it
          also has a @username hint — we try the phone path first.
        * `username_validatable` — rows with a `tg_username_hint` that
          need `get_entity(@handle)`. Counted separately from
          phone_resolvable so a mixed dataset shows both steps.
        * `unreachable` — rows that have already been decisively flagged
          `not_on_telegram`. Never block campaign prep; shown so the
          user knows why the count-to-send is lower than total contacts.

        Note: a contact can appear in BOTH phone_resolvable and
        username_validatable if they have both a phone AND a hint — the
        Resolve pass handles them first; Validate picks up only rows
        still in pending/error afterward.
        """
        with self._conn() as c:
            phone_resolvable = int(c.execute(
                """SELECT COUNT(*) FROM contacts
                   WHERE resolve_status IN ('pending', 'error')
                     AND phone IS NOT NULL
                     AND phone NOT LIKE 'tg:%'"""
            ).fetchone()[0])
            username_validatable = int(c.execute(
                """SELECT COUNT(*) FROM contacts
                   WHERE resolve_status IN ('pending', 'error')
                     AND tg_username_hint IS NOT NULL
                     AND tg_username_hint != ''"""
            ).fetchone()[0])
            unreachable = int(c.execute(
                """SELECT COUNT(*) FROM contacts
                   WHERE resolve_status = 'not_on_telegram'"""
            ).fetchone()[0])
        return {
            "phone_resolvable": phone_resolvable,
            "username_validatable": username_validatable,
            "unreachable": unreachable,
        }

    def all_contacts_df(self):
        import pandas as pd
        with self._conn() as c:
            return pd.read_sql_query(
                """SELECT id, name, phone, tg_user_id, tg_username,
                          tg_username_hint, resolve_status, resolve_error
                   FROM contacts ORDER BY id""",
                c,
            )

    def pending_username_validations(
        self, ids: list[int] | None = None,
    ) -> list[sqlite3.Row]:
        """Contacts with a stored username hint eligible for validation.

        Covers `pending` and `error` rows — an error row re-runs under the
        assumption that the username was corrected or the failure was
        transient (FloodWait). If `ids` is provided, restrict to that set.
        """
        with self._conn() as c:
            q = ("""SELECT * FROM contacts
                    WHERE resolve_status IN ('pending', 'error')
                      AND tg_username_hint IS NOT NULL
                      AND tg_username_hint != ''""")
            params: list = []
            if ids:
                placeholders = ",".join("?" * len(ids))
                q += f" AND id IN ({placeholders})"
                params.extend(int(x) for x in ids)
            return list(c.execute(q, params))

    def resolved_contacts(self) -> list[sqlite3.Row]:
        with self._conn() as c:
            return list(c.execute(
                "SELECT * FROM contacts WHERE resolve_status = 'resolved' AND tg_user_id IS NOT NULL"
            ))

    def delete_contacts(self, ids: Iterable[int]) -> int:
        """Delete contacts by id. Also removes matching send_log rows so
        campaign stats don't reference orphans. Returns contacts deleted."""
        id_list = [int(x) for x in ids]
        if not id_list:
            return 0
        placeholders = ",".join("?" * len(id_list))
        with self._conn() as c:
            c.execute(f"DELETE FROM send_log WHERE contact_id IN ({placeholders})", id_list)
            cur = c.execute(f"DELETE FROM contacts WHERE id IN ({placeholders})", id_list)
            return cur.rowcount

    def purge_contacts(self) -> int:
        """Delete every contact and every associated send_log row.
        Returns number of contacts deleted. Campaigns are NOT touched."""
        with self._conn() as c:
            c.execute("DELETE FROM send_log")
            c.execute("DELETE FROM phone_aliases")
            cur = c.execute("DELETE FROM contacts")
            return cur.rowcount

    # ---------- phone_aliases -----------------------------------------------

    def add_phone_alias(self, contact_id: int, alias_phone: str) -> bool:
        """Record `alias_phone` as a historical value that should now map
        to `contact_id`. Idempotent: if the alias exists for the same
        contact, it's a no-op; if it exists for a *different* contact,
        we update to the new owner (last-write-wins)."""
        alias = (alias_phone or "").strip()
        if not alias:
            return False
        with self._conn() as c:
            c.execute(
                """INSERT INTO phone_aliases (alias_phone, contact_id)
                   VALUES (?, ?)
                   ON CONFLICT(alias_phone) DO UPDATE SET contact_id=excluded.contact_id""",
                (alias, int(contact_id)),
            )
            return True

    def resolve_phone_alias(self, phone: str) -> int | None:
        """If `phone` was previously a real contact's phone but the user
        renamed it, return the current contact's id."""
        if not phone:
            return None
        with self._conn() as c:
            row = c.execute(
                "SELECT contact_id FROM phone_aliases WHERE alias_phone=?",
                (phone,),
            ).fetchone()
            return int(row[0]) if row else None

    def list_aliases_for(self, contact_id: int) -> list[str]:
        with self._conn() as c:
            return [
                r[0] for r in c.execute(
                    "SELECT alias_phone FROM phone_aliases WHERE contact_id=?",
                    (int(contact_id),),
                )
            ]

    _EDITABLE_CONTACT_FIELDS = {
        "name", "phone", "tg_username", "tg_username_hint", "tg_user_id",
    }

    def update_contact(self, contact_id: int, fields: dict) -> bool:
        """Update whitelisted fields of a contact. Returns True on change.

        Silently drops keys outside `_EDITABLE_CONTACT_FIELDS`. Empty strings
        are coerced to NULL for all columns except `phone` (which is the
        unique/primary key of a contact row).

        Edit-propagation rules (v1.5 follow-ups):

        * If `phone` is set to `tg:<X>`, `tg_username_hint` is synchronized
          to `X` (the two are two views of the same handle — if the user
          corrects the spelling in one field, the other must follow).
        * If `tg_username_hint` was edited to a new non-empty value, OR the
          `tg:<X>` phone was edited, the row's `resolve_status` is reset
          from `error` back to `pending` and `resolve_error` is cleared,
          so the validator picks it up again on the next run.
        """
        updates: dict = {}
        for k, v in (fields or {}).items():
            if k not in self._EDITABLE_CONTACT_FIELDS:
                continue
            if isinstance(v, str):
                v = v.strip()
                if not v and k != "phone":
                    v = None
            updates[k] = v
        if not updates:
            return False

        # Derive hint from phone when user edited the tg:<X> synthetic phone
        # and didn't also retype the hint explicitly.
        new_phone = updates.get("phone")
        if (
            isinstance(new_phone, str)
            and new_phone.startswith("tg:")
            and "tg_username_hint" not in updates
        ):
            handle = new_phone[len("tg:"):].strip().lstrip("@")
            if handle:
                updates["tg_username_hint"] = handle

        # Any change to the handle (hint or tg:* phone) should unstick a
        # previous error so the validator re-examines the row.
        hint_edited = "tg_username_hint" in updates
        tg_phone_edited = (
            isinstance(new_phone, str) and new_phone.startswith("tg:")
        )
        if hint_edited or tg_phone_edited:
            updates["resolve_status"] = "pending"
            updates["resolve_error"] = None
            # Allow resetting status/error via the _retryable_states path.
            extra_allowed = ("resolve_status", "resolve_error")
        else:
            extra_allowed = ()

        all_allowed = self._EDITABLE_CONTACT_FIELDS | set(extra_allowed)
        updates = {k: v for k, v in updates.items() if k in all_allowed}
        if not updates:
            return False

        # Defence-in-depth: reject any key that is not a plain identifier
        # before f-stringing it into SQL, even though `all_allowed` already
        # filtered by membership. A future allowlist typo shouldn't become
        # SQLi.
        if not all(_IDENTIFIER_RE.match(k) for k in updates):
            raise ValueError(f"unsafe column names in updates: {list(updates)!r}")
        cols = ", ".join(f"{k}=?" for k in updates)
        values = list(updates.values()) + [int(contact_id)]
        with self._conn() as c:
            # Snapshot the old phone BEFORE the update so we can alias it
            # (only relevant when the phone field is actually changing).
            old_phone: str | None = None
            if "phone" in updates:
                row = c.execute(
                    "SELECT phone FROM contacts WHERE id=?", (int(contact_id),),
                ).fetchone()
                if row is not None:
                    old_phone = row[0]
            cur = c.execute(f"UPDATE contacts SET {cols} WHERE id=?", values)
            if (
                cur.rowcount > 0
                and old_phone
                and "phone" in updates
                and old_phone != updates["phone"]
            ):
                # Register the typo value as a historical alias — future
                # syncs that still carry the typo won't spawn a duplicate.
                c.execute(
                    """INSERT INTO phone_aliases (alias_phone, contact_id)
                       VALUES (?, ?)
                       ON CONFLICT(alias_phone)
                       DO UPDATE SET contact_id=excluded.contact_id""",
                    (old_phone, int(contact_id)),
                )
            return cur.rowcount > 0

    # ---------- accounts ----------------------------------------------------

    def list_accounts(self) -> list[sqlite3.Row]:
        with self._conn() as c:
            return list(c.execute(
                "SELECT * FROM accounts ORDER BY is_active DESC, id"
            ))

    def get_account(self, account_id: int) -> sqlite3.Row | None:
        with self._conn() as c:
            cur = c.execute("SELECT * FROM accounts WHERE id=?", (int(account_id),))
            return cur.fetchone()

    def get_active_account(self) -> sqlite3.Row | None:
        with self._conn() as c:
            cur = c.execute(
                "SELECT * FROM accounts WHERE is_active=1 ORDER BY id LIMIT 1"
            )
            return cur.fetchone()

    def upsert_account(self, label: str, api_id: int, api_hash: str,
                       session_name: str, notes: str | None = None,
                       is_active: bool = False) -> int:
        """Insert or update (by unique label). Returns the row id."""
        with self._conn() as c:
            c.execute(
                """INSERT INTO accounts (label, api_id, api_hash, session_name,
                                         is_active, notes)
                   VALUES (?, ?, ?, ?, ?, ?)
                   ON CONFLICT(label) DO UPDATE SET
                       api_id=excluded.api_id,
                       api_hash=excluded.api_hash,
                       session_name=excluded.session_name,
                       notes=excluded.notes""",
                (label, int(api_id), api_hash, session_name,
                 1 if is_active else 0, notes),
            )
            row = c.execute("SELECT id FROM accounts WHERE label=?", (label,)).fetchone()
            if is_active and row is not None:
                c.execute("UPDATE accounts SET is_active=CASE id WHEN ? THEN 1 ELSE 0 END",
                          (int(row[0]),))
            return int(row[0]) if row else 0

    def set_active_account(self, account_id: int) -> None:
        with self._conn() as c:
            c.execute(
                "UPDATE accounts SET is_active=CASE id WHEN ? THEN 1 ELSE 0 END",
                (int(account_id),),
            )

    def delete_account(self, account_id: int) -> int:
        """Delete an account row. Campaigns linked to it keep the FK
        (nullable) — we null it so stats stay visible."""
        with self._conn() as c:
            c.execute("UPDATE campaigns SET account_id=NULL WHERE account_id=?",
                      (int(account_id),))
            cur = c.execute("DELETE FROM accounts WHERE id=?", (int(account_id),))
            return cur.rowcount

    # ---------- saved filters -----------------------------------------------

    def list_saved_filters(self) -> list[sqlite3.Row]:
        with self._conn() as c:
            return list(c.execute(
                "SELECT * FROM saved_filters ORDER BY name COLLATE NOCASE"
            ))

    def get_saved_filter(self, filter_id: int) -> sqlite3.Row | None:
        with self._conn() as c:
            return c.execute(
                "SELECT * FROM saved_filters WHERE id=?", (int(filter_id),),
            ).fetchone()

    def upsert_saved_filter(self, name: str, filter_json: str) -> int:
        with self._conn() as c:
            c.execute(
                """INSERT INTO saved_filters (name, filter_json)
                   VALUES (?, ?)
                   ON CONFLICT(name) DO UPDATE SET filter_json=excluded.filter_json""",
                (name.strip(), filter_json),
            )
            return int(c.execute(
                "SELECT id FROM saved_filters WHERE name=?", (name.strip(),),
            ).fetchone()[0])

    def delete_saved_filter(self, filter_id: int) -> int:
        with self._conn() as c:
            cur = c.execute(
                "DELETE FROM saved_filters WHERE id=?", (int(filter_id),),
            )
            return cur.rowcount

    def resolve_filter_to_contact_ids(self, spec: dict) -> list[int]:
        """Materialize a filter spec into a concrete ordered list of
        contact ids. Spec keys (all optional):

          resolved_only:      bool  — only resolve_status='resolved'
          has_username:       bool  — tg_username IS NOT NULL AND != ''
          not_messaged_days:  int|None — exclude contacts messaged in last N days (status='sent')
          tag_any:            [str] — include only contacts touched by campaigns with any of these tags
          tag_none:           [str] — exclude contacts touched by campaigns with any of these tags
          exclude_tg_user_ids:[int] — final hard exclude by tg_user_id
        """
        import json
        resolved_only = bool(spec.get("resolved_only", False))
        has_username = bool(spec.get("has_username", False))
        not_messaged_days = spec.get("not_messaged_days")
        tag_any = [str(t).strip() for t in (spec.get("tag_any") or []) if str(t).strip()]
        tag_none = [str(t).strip() for t in (spec.get("tag_none") or []) if str(t).strip()]
        exclude_tg = {int(x) for x in (spec.get("exclude_tg_user_ids") or [])}

        clauses = ["1=1"]
        params: list = []
        if resolved_only:
            clauses.append("resolve_status = 'resolved' AND tg_user_id IS NOT NULL")
        if has_username:
            clauses.append("tg_username IS NOT NULL AND tg_username != ''")

        with self._conn() as c:
            q = f"SELECT id, tg_user_id FROM contacts WHERE {' AND '.join(clauses)} ORDER BY id"
            rows = list(c.execute(q, params))

        # Post-SQL filters below use Python for the rare combos.
        ids = [(int(r[0]), r[1]) for r in rows]

        if not_messaged_days is not None and int(not_messaged_days) > 0:
            recent = self.contacts_already_messaged(int(not_messaged_days))
            ids = [(cid, u) for cid, u in ids if cid not in recent]
        if tag_any:
            scope = self.contact_ids_for_campaign_tags(tag_any)
            ids = [(cid, u) for cid, u in ids if cid in scope]
        if tag_none:
            scope_bad = self.contact_ids_for_campaign_tags(tag_none)
            ids = [(cid, u) for cid, u in ids if cid not in scope_bad]
        if exclude_tg:
            ids = [(cid, u) for cid, u in ids if int(u or 0) not in exclude_tg]

        return [cid for cid, _u in ids]

    # ---------- sheet_sources (global singleton) ----------------------------

    def get_sheet_source(self) -> sqlite3.Row | None:
        """Return the single global sheet source (id always 1), or None."""
        with self._conn() as c:
            return c.execute(
                "SELECT * FROM sheet_sources ORDER BY id LIMIT 1"
            ).fetchone()

    def upsert_sheet_source(
        self,
        url: str,
        column_map_json: str,
        cadence_min: int | None,
        label: str = "primary",
        auto_resolve: bool = True,
    ) -> int:
        """Replace-in-place the singleton sheet source and return its id."""
        with self._conn() as c:
            existing = c.execute(
                "SELECT id FROM sheet_sources ORDER BY id LIMIT 1"
            ).fetchone()
            if existing is None:
                cur = c.execute(
                    """INSERT INTO sheet_sources
                       (label, url, column_map_json, cadence_min, auto_resolve)
                       VALUES (?, ?, ?, ?, ?)""",
                    (label, url, column_map_json,
                     int(cadence_min) if cadence_min else None,
                     1 if auto_resolve else 0),
                )
                return cur.lastrowid
            sid = int(existing[0])
            c.execute(
                """UPDATE sheet_sources
                   SET label=?, url=?, column_map_json=?, cadence_min=?,
                       auto_resolve=?
                   WHERE id=?""",
                (label, url, column_map_json,
                 int(cadence_min) if cadence_min else None,
                 1 if auto_resolve else 0, sid),
            )
            return sid

    def update_sheet_source(self, **fields) -> bool:
        allowed = {"url", "column_map_json", "cadence_min",
                   "last_synced_at", "last_seen_phone_set_hash",
                   "rows_seen", "label", "auto_resolve"}
        clean = {k: v for k, v in fields.items() if k in allowed}
        if not clean:
            return False
        existing = self.get_sheet_source()
        if existing is None:
            return False
        cols = ", ".join(f"{k}=?" for k in clean)
        with self._conn() as c:
            cur = c.execute(
                f"UPDATE sheet_sources SET {cols} WHERE id=?",
                list(clean.values()) + [int(existing["id"])],
            )
            return cur.rowcount > 0

    def delete_sheet_source(self) -> int:
        with self._conn() as c:
            cur = c.execute("DELETE FROM sheet_sources")
            return cur.rowcount

    # ---------- campaign_sources (deprecated in v1.5 rework; kept for compat)

    # ---------- campaign_sources --------------------------------------------

    def list_campaign_sources(self, campaign_id: int | None = None) -> list[sqlite3.Row]:
        with self._conn() as c:
            if campaign_id is None:
                return list(c.execute(
                    "SELECT * FROM campaign_sources ORDER BY id"
                ))
            return list(c.execute(
                "SELECT * FROM campaign_sources WHERE campaign_id=? ORDER BY id",
                (int(campaign_id),),
            ))

    def get_campaign_source(self, source_id: int) -> sqlite3.Row | None:
        with self._conn() as c:
            return c.execute(
                "SELECT * FROM campaign_sources WHERE id=?", (int(source_id),),
            ).fetchone()

    def create_campaign_source(self, campaign_id: int, url: str,
                               column_map_json: str,
                               cadence_min: int | None = None,
                               type_: str = "gsheet") -> int:
        with self._conn() as c:
            cur = c.execute(
                """INSERT INTO campaign_sources
                   (campaign_id, type, url, column_map_json, cadence_min)
                   VALUES (?, ?, ?, ?, ?)""",
                (int(campaign_id), type_, url, column_map_json,
                 int(cadence_min) if cadence_min else None),
            )
            return cur.lastrowid

    def update_campaign_source(self, source_id: int, **fields) -> bool:
        """Updates any of: url, column_map_json, cadence_min, last_synced_at,
        last_seen_phone_set_hash, rows_seen."""
        allowed = {"url", "column_map_json", "cadence_min",
                   "last_synced_at", "last_seen_phone_set_hash", "rows_seen"}
        clean = {k: v for k, v in fields.items() if k in allowed}
        if not clean:
            return False
        cols = ", ".join(f"{k}=?" for k in clean)
        with self._conn() as c:
            cur = c.execute(
                f"UPDATE campaign_sources SET {cols} WHERE id=?",
                list(clean.values()) + [int(source_id)],
            )
            return cur.rowcount > 0

    def delete_campaign_source(self, source_id: int) -> int:
        with self._conn() as c:
            cur = c.execute(
                "DELETE FROM campaign_sources WHERE id=?", (int(source_id),),
            )
            return cur.rowcount

    # ---------- campaigns ---------------------------------------------------

    def create_campaign(self, name: str, template: str,
                        image_path: str | None, group_link: str | None,
                        account_id: int | None = None,
                        tags: list[str] | None = None) -> int:
        import json
        with self._conn() as c:
            cur = c.execute(
                "INSERT INTO campaigns (name, template, image_path, group_link, account_id, tags)"
                " VALUES (?,?,?,?,?,?)",
                (name, template, image_path, group_link,
                 int(account_id) if account_id is not None else None,
                 json.dumps(tags or [], ensure_ascii=False)),
            )
            return cur.lastrowid

    def set_campaign_tags(self, campaign_id: int, tags: list[str]) -> None:
        import json
        with self._conn() as c:
            c.execute("UPDATE campaigns SET tags=? WHERE id=?",
                      (json.dumps(tags or [], ensure_ascii=False), int(campaign_id)))

    def get_campaign_tags(self, campaign_id: int) -> list[str]:
        import json
        row = self.get_campaign(int(campaign_id))
        if row is None:
            return []
        raw = row["tags"] if "tags" in row.keys() else None
        try:
            return json.loads(raw) if raw else []
        except Exception:
            return []

    def all_known_tags(self) -> list[str]:
        """Distinct tags across every campaign, sorted, case-insensitive dedup."""
        import json
        seen: dict[str, str] = {}
        with self._conn() as c:
            for row in c.execute("SELECT tags FROM campaigns"):
                try:
                    for t in json.loads(row[0] or "[]"):
                        key = str(t).strip().lower()
                        if key and key not in seen:
                            seen[key] = str(t).strip()
                except Exception:
                    continue
        return sorted(seen.values(), key=str.lower)

    def contacts_df_with_campaign_status(self):
        """Same rows as `all_contacts_df` plus a `campaigns_status` column
        summarizing send_log per contact ('#id:last_status' joined by '; ').

        Implementation note: the previous version ran a correlated
        subquery with a `ROW_NUMBER() OVER` window *per contact row*,
        which is O(contacts × send_log_rows). This CTE version runs the
        window function once over `send_log`, filters to the latest
        status per (contact, campaign), and left-joins — O(send_log +
        contacts), and noticeably snappier as the contacts table grows.
        Also builds the DataFrame directly from a cursor rather than via
        `pd.read_sql_query` which is ~2× faster for small/medium results.
        """
        import pandas as pd
        query = """
            WITH latest AS (
                SELECT contact_id, campaign_id, status,
                       ROW_NUMBER() OVER (
                           PARTITION BY contact_id, campaign_id
                           ORDER BY sent_at DESC
                       ) AS rn
                FROM send_log
            ),
            summary AS (
                SELECT contact_id,
                       GROUP_CONCAT('#' || campaign_id || ':' || status,
                                    '; ') AS campaigns_status
                FROM latest
                WHERE rn = 1
                GROUP BY contact_id
            )
            SELECT c.id, c.name, c.phone, c.tg_user_id, c.tg_username,
                   c.tg_username_hint, c.resolve_status, c.resolve_error,
                   COALESCE(summary.campaigns_status, '') AS campaigns_status
            FROM contacts c
            LEFT JOIN summary ON summary.contact_id = c.id
            ORDER BY c.id
        """
        columns = [
            "id", "name", "phone", "tg_user_id", "tg_username",
            "tg_username_hint", "resolve_status", "resolve_error",
            "campaigns_status",
        ]
        with self._conn() as conn:
            rows = conn.execute(query).fetchall()
        return pd.DataFrame(rows, columns=columns)

    def contact_ids_for_campaign_tags(self, tags: list[str],
                                      status: str | None = None) -> set[int]:
        """Contacts that appear in send_log for any campaign tagged with
        any of the given tags. Optional status filter (sent/error/skipped/opted_out).
        Case-insensitive tag match. Empty `tags` → empty set."""
        import json
        if not tags:
            return set()
        wanted = {t.strip().lower() for t in tags if t.strip()}
        if not wanted:
            return set()
        camp_ids: list[int] = []
        with self._conn() as c:
            for row in c.execute("SELECT id, tags FROM campaigns"):
                try:
                    have = {str(x).strip().lower() for x in json.loads(row[1] or "[]")}
                except Exception:
                    have = set()
                if wanted & have:
                    camp_ids.append(int(row[0]))
            if not camp_ids:
                return set()
            placeholders = ",".join("?" * len(camp_ids))
            q = f"SELECT DISTINCT contact_id FROM send_log WHERE campaign_id IN ({placeholders})"
            params: list = list(camp_ids)
            if status:
                q += " AND status = ?"
                params.append(status)
            return {int(r[0]) for r in c.execute(q, params)}

    def update_campaign_status(self, campaign_id: int, status: str) -> None:
        with self._conn() as c:
            c.execute("UPDATE campaigns SET status=? WHERE id=?", (status, campaign_id))

    # Phase C1 — field-level edit. Whitelisted to content fields; status
    # stays under `transition_campaign_state`, tags under `set_campaign_tags`.
    _CAMPAIGN_EDITABLE_FIELDS = {"name", "template", "image_path", "group_link"}

    def update_campaign(self, campaign_id: int, **fields) -> bool:
        """Update content fields of a campaign.

        Only `name`, `template`, `image_path`, `group_link` are editable.
        `status`, `tags`, `account_id`, `created_at`, `id` are rejected —
        those have dedicated code paths (state machine / tags API).
        Passing an unknown field raises `ValueError` so callers catch
        typos instead of silently no-op'ing.

        Returns True if the row was updated (id existed + at least one
        field different), False otherwise.
        """
        bad = set(fields) - self._CAMPAIGN_EDITABLE_FIELDS
        if bad:
            raise ValueError(f"fields not editable via update_campaign: "
                             f"{sorted(bad)}")
        if not fields:
            return False
        cols = ", ".join(f"{k}=?" for k in fields)
        params = list(fields.values()) + [int(campaign_id)]
        def _do() -> bool:
            with self._conn() as c:
                cur = c.execute(
                    f"UPDATE campaigns SET {cols} WHERE id=?", params,
                )
                return bool(cur.rowcount)
        return bool(self._retry_on_lock(_do))

    def delete_campaign(self, campaign_id: int) -> bool:
        """Delete a campaign and its `send_log` history.

        Guarded by status: only `draft`, `stopped`, `done`, `error`
        campaigns can be deleted. Running/paused campaigns must be
        stopped first — deleting them would strand the worker's open
        lock and the in-flight job.

        Returns True on success, False if the id doesn't exist or the
        status guard blocks it.
        """
        def _do() -> bool:
            with self._conn() as c:
                row = c.execute(
                    "SELECT status FROM campaigns WHERE id=?",
                    (int(campaign_id),),
                ).fetchone()
                if row is None:
                    return False
                if row["status"] in ("running", "paused"):
                    return False
                # send_log first (FK isn't ON DELETE CASCADE in schema).
                c.execute(
                    "DELETE FROM send_log WHERE campaign_id=?",
                    (int(campaign_id),),
                )
                cur = c.execute(
                    "DELETE FROM campaigns WHERE id=?",
                    (int(campaign_id),),
                )
                return bool(cur.rowcount)
        return bool(self._retry_on_lock(_do))

    # Phase H6 — explicit campaign lifecycle transitions. `status` enum:
    #   draft → running → paused → running (resume)
    #                   → stopped
    #                   → done
    # Reset is driven separately via `reset_campaign_progress`.
    _CAMPAIGN_LIFECYCLE_TRANSITIONS = {
        "draft":   {"running"},
        "running": {"paused", "stopped", "done", "error"},
        "paused":  {"running", "stopped", "draft"},
        "stopped": {"running", "draft"},
        "done":    {"running", "draft"},
        "error":   {"running", "draft"},
    }

    def transition_campaign_state(self, campaign_id: int,
                                   new_state: str) -> bool:
        """Guarded state machine. Returns True if the transition applied,
        False if either the campaign doesn't exist or the transition is
        illegal from the current state. No exception → the caller can
        surface a friendly message."""
        with self._conn() as c:
            row = c.execute(
                "SELECT status FROM campaigns WHERE id=?", (int(campaign_id),),
            ).fetchone()
            if row is None:
                return False
            current = row["status"] or "draft"
            allowed = self._CAMPAIGN_LIFECYCLE_TRANSITIONS.get(current, set())
            if new_state == current:
                return True  # idempotent no-op
            if new_state not in allowed:
                return False
            c.execute(
                "UPDATE campaigns SET status=? WHERE id=?",
                (new_state, int(campaign_id)),
            )
            return True

    def send_log_contacts(self, campaign_id: int) -> list[dict]:
        """Return (contact_id, name, phone, tg_username, status) for every
        send_log row of a campaign — used by the UI's "reset contact"
        picker to re-queue specific recipients."""
        with self._conn() as c:
            rows = c.execute(
                """SELECT c.id AS contact_id, c.name, c.phone, c.tg_username,
                          s.status
                   FROM send_log s JOIN contacts c ON c.id = s.contact_id
                   WHERE s.campaign_id=? ORDER BY c.name""",
                (int(campaign_id),),
            ).fetchall()
            return [dict(r) for r in rows]

    def reset_contact_send(self, campaign_id: int,
                            contact_ids: list[int]) -> int:
        """Delete send_log rows for specific (campaign, contact) pairs so
        those contacts re-enter the send loop on the next run. Returns
        the number of rows deleted. No-op for contacts that don't have
        a row yet. Legal from any campaign state — the next run picks
        them up via `already_sent_ids` naturally."""
        if not contact_ids:
            return 0
        id_list = [int(x) for x in contact_ids]
        placeholders = ",".join("?" * len(id_list))
        with self._conn() as c:
            cur = c.execute(
                f"DELETE FROM send_log "
                f"WHERE campaign_id=? AND contact_id IN ({placeholders})",
                (int(campaign_id), *id_list),
            )
            return cur.rowcount

    def reset_campaign_progress(self, campaign_id: int) -> int:
        """Wipe every `send_log` row for a campaign AND reset the campaign
        row to 'draft'. Returns the number of log rows deleted. Only legal
        from non-running states; callers must check via `get_campaign`.
        Opt-out rows are global and intentionally preserved."""
        with self._conn() as c:
            cur = c.execute(
                "DELETE FROM send_log WHERE campaign_id=?",
                (int(campaign_id),),
            )
            deleted = cur.rowcount
            c.execute(
                "UPDATE campaigns SET status='draft' WHERE id=?",
                (int(campaign_id),),
            )
            return deleted

    def get_campaign(self, campaign_id: int) -> sqlite3.Row | None:
        with self._conn() as c:
            row = c.execute("SELECT * FROM campaigns WHERE id=?", (campaign_id,)).fetchone()
            return row

    def list_campaigns(self) -> list[sqlite3.Row]:
        with self._conn() as c:
            return list(c.execute("SELECT * FROM campaigns ORDER BY id DESC"))

    # ---------- send log ----------------------------------------------------

    def already_sent_ids(self, campaign_id: int) -> set[int]:
        """Contacts that must NOT enter the send loop again in this run.

        Only `sent` (confirmed delivery). `pending` rows are a sign a
        previous run crashed between reserve and confirm — we WANT the
        sender to re-enter them so `reserve_send` can reuse the stored
        random_id and let Telegram's server-side dedup suppress the
        retry if the original send actually landed.

        Concurrent double-run is prevented by the per-campaign Redis
        lock, not this set."""
        with self._conn() as c:
            return {row["contact_id"] for row in c.execute(
                "SELECT contact_id FROM send_log WHERE campaign_id=? AND status='sent'",
                (campaign_id,),
            )}

    def reserve_send(self, campaign_id: int, contact_id: int,
                     random_id: int) -> tuple[int, str]:
        """Reserve a slot in `send_log` BEFORE hitting Telegram.

        Atomic insert-or-ignore semantics:

          * If no row exists → inserts status='pending' with the caller's
            random_id; returns (random_id, 'reserved').
          * If an existing row has status='sent' → returns (existing_rid,
            'sent') so the caller skips.
          * If an existing row has status='pending' → this is a resume
            from a crashed previous run; returns (existing_rid, 'pending')
            so the caller re-sends with the same random_id → Telegram
            server-side dedup handles it.
          * If an existing row has status='error' or 'skipped' → treat as
            retryable: upgrade to 'pending' with a FRESH random_id (old
            rid may have been consumed) and return (new_rid, 'retry').
        """
        def _do() -> tuple[int, str]:
            with self._conn() as c:
                existing = c.execute(
                    """SELECT status, random_id, detail FROM send_log
                       WHERE campaign_id=? AND contact_id=?""",
                    (campaign_id, contact_id),
                ).fetchone()
                if existing is None:
                    c.execute(
                        """INSERT INTO send_log
                           (campaign_id, contact_id, status, random_id, attempt_count)
                           VALUES (?, ?, 'pending', ?, 1)""",
                        (campaign_id, contact_id, int(random_id)),
                    )
                    return int(random_id), "reserved"
                existing_status = existing["status"]
                existing_rid = existing["random_id"]
                existing_detail = (existing["detail"] or "") if existing is not None else ""
                if existing_status == "sent":
                    return int(existing_rid or 0), "sent"
                # Pre-fix rows whose error was a Telethon session-DB lock
                # (post-send `process_entities` failure) — the message
                # LANDED on Telegram. Retrying would duplicate. Upgrade
                # the row to 'sent' so the sender skips it.
                if (existing_status == "error"
                    and ("locked" in existing_detail.lower()
                         or "OperationalError" in existing_detail)):
                    c.execute(
                        """UPDATE send_log
                           SET status='sent',
                               detail=COALESCE(detail,'') || ' | auto-upgraded: session-lock, message already delivered'
                           WHERE campaign_id=? AND contact_id=?""",
                        (campaign_id, contact_id),
                    )
                    return int(existing_rid or 0), "sent"
                if existing_status == "pending":
                    # Re-use the previous rid so Telegram can dedup if the
                    # prior send actually landed.
                    rid = int(existing_rid) if existing_rid else int(random_id)
                    c.execute(
                        """UPDATE send_log
                           SET attempt_count = attempt_count + 1,
                               random_id = COALESCE(random_id, ?)
                           WHERE campaign_id=? AND contact_id=?""",
                        (int(random_id), campaign_id, contact_id),
                    )
                    return rid, "pending"
                # status IN ('error','skipped','opted_out') → user-driven
                # retry. REUSE the stored rid (never fresh) — the
                # previous send may have actually landed on Telegram
                # (confirm_send failed, or an unrelated 'database is
                # locked' fired after the send succeeded). Reusing rid
                # lets Telegram server-side dedup catch the duplicate.
                # If the stored rid is NULL (legacy row), fall back to
                # the caller-provided one.
                rid = int(existing_rid) if existing_rid else int(random_id)
                c.execute(
                    """UPDATE send_log
                       SET status='pending', random_id=?, detail=NULL,
                           attempt_count = attempt_count + 1,
                           sent_at=datetime('now')
                       WHERE campaign_id=? AND contact_id=?""",
                    (rid, campaign_id, contact_id),
                )
                return rid, "retry"
        return self._retry_on_lock(_do)

    def confirm_send(self, campaign_id: int, contact_id: int,
                     final_status: str, detail: str = "",
                     message_id: int | None = None) -> None:
        """Transition a reserved row to its terminal status.

        final_status ∈ {'sent', 'error', 'skipped'}. Keeps the existing
        `random_id` intact (don't wipe it — it's the dedup key for any
        future retry of the same contact).

        Phase R1 — optional `message_id` stores Telegram's per-message
        int returned by `send_message` / `send_file`. Used later by the
        manual read-check to flip `read_at` precisely. Omitted callers
        (error / skipped branches, legacy pre-R1 code) leave the column
        NULL; the read-check falls back to the coarse peer-cursor path.
        """
        def _do():
            with self._conn() as c:
                if message_id is not None:
                    c.execute(
                        """UPDATE send_log
                           SET status=?, detail=?, message_id=?,
                               sent_at=datetime('now')
                           WHERE campaign_id=? AND contact_id=?""",
                        (final_status, (detail or "")[:500], int(message_id),
                         campaign_id, contact_id),
                    )
                else:
                    c.execute(
                        """UPDATE send_log
                           SET status=?, detail=?, sent_at=datetime('now')
                           WHERE campaign_id=? AND contact_id=?""",
                        (final_status, (detail or "")[:500],
                         campaign_id, contact_id),
                    )
        self._retry_on_lock(_do)

    def record_send(self, campaign_id: int, contact_id: int, status: str, detail: str = "") -> None:
        """Back-compat wrapper — used by pre-sender paths (dedup marks,
        opt-out preloads). New send path uses reserve_send + confirm_send."""
        def _do():
            with self._conn() as c:
                c.execute(
                    """INSERT OR REPLACE INTO send_log (campaign_id, contact_id, status, detail)
                       VALUES (?, ?, ?, ?)""",
                    (campaign_id, contact_id, status, detail[:500]),
                )
        self._retry_on_lock(_do)

    def sent_today_count(self, campaign_id: int) -> int:
        with self._conn() as c:
            row = c.execute(
                """SELECT COUNT(*) AS n FROM send_log
                   WHERE campaign_id=? AND status='sent'
                   AND date(sent_at) = date('now')""",
                (campaign_id,),
            ).fetchone()
            return row["n"] if row else 0

    def campaign_stats(self, campaign_id: int) -> dict:
        with self._conn() as c:
            row = c.execute(
                """SELECT
                   COUNT(*) FILTER (WHERE status='sent') AS sent,
                   COUNT(*) FILTER (WHERE status='error') AS errors,
                   COUNT(*) FILTER (WHERE status='skipped') AS skipped,
                   COUNT(*) FILTER (WHERE status='opted_out') AS opted_out,
                   COUNT(*) FILTER (WHERE status='sent' AND read_at IS NOT NULL) AS read
                   FROM send_log WHERE campaign_id=?""",
                (campaign_id,),
            ).fetchone()
            return dict(row) if row else {
                "sent": 0, "errors": 0, "skipped": 0, "opted_out": 0, "read": 0,
            }

    def send_log_df(self, campaign_id: int):
        import pandas as pd
        with self._conn() as c:
            return pd.read_sql_query(
                """SELECT s.sent_at, c.name, c.phone, c.tg_username,
                          s.status, s.detail, s.message_id, s.read_at
                   FROM send_log s JOIN contacts c ON c.id = s.contact_id
                   WHERE s.campaign_id=? ORDER BY s.sent_at DESC""",
                c, params=(campaign_id,),
            )

    # ---------- read receipts (Phase R1) ------------------------------------

    def read_check_targets(self, campaign_ids: list[int] | None = None,
                           contact_ids: list[int] | None = None) -> list[dict]:
        """Return (contact_id, tg_user_id, tg_access_hash) rows that need a
        read-receipt check.

        Filters:
          - Only `status='sent'` rows (no point checking reads on errors).
          - Skip rows already flagged (`read_at IS NULL`).
          - Require a resolved Telegram peer (`tg_user_id IS NOT NULL`).
          - `campaign_ids` / `contact_ids` — both optional, AND-combined.

        GROUP BY contact_id because the peer-read cursor is per-user;
        the job fans one cursor out to all that user's sent rows in
        `apply_read_receipts`.
        """
        sql = [
            "SELECT sl.contact_id, c.tg_user_id, c.tg_access_hash",
            "FROM send_log sl",
            "JOIN contacts c ON c.id = sl.contact_id",
            "WHERE sl.status='sent'",
            "  AND sl.read_at IS NULL",
            "  AND c.tg_user_id IS NOT NULL",
        ]
        params: list = []
        if campaign_ids:
            placeholders = ",".join("?" * len(campaign_ids))
            sql.append(f"  AND sl.campaign_id IN ({placeholders})")
            params.extend(int(x) for x in campaign_ids)
        if contact_ids:
            placeholders = ",".join("?" * len(contact_ids))
            sql.append(f"  AND sl.contact_id IN ({placeholders})")
            params.extend(int(x) for x in contact_ids)
        sql.append("GROUP BY sl.contact_id, c.tg_user_id, c.tg_access_hash")
        with self._conn() as c:
            return [dict(row) for row in c.execute("\n".join(sql), tuple(params))]

    def apply_read_receipts(
        self,
        updates: list[tuple[int, int]],
        checked_at_iso: str,
        campaign_ids: list[int] | None = None,
    ) -> dict:
        """Apply a batch of (contact_id, read_outbox_max_id) pairs to
        send_log. Precise-only — we require `message_id` to be present
        on the row.

        Why no coarse fallback: `read_outbox_max_id` is a monotonic
        cursor that tracks the *highest* message id the peer has ever
        read from us. A historical 1-on-1 interaction (older campaign,
        test DM, anything) bumps it permanently to > 0, so "cursor > 0"
        does NOT mean "read our latest message" — it just means "read
        something, ever". Flagging legacy-no-message_id rows on that
        basis produces false-positives (confirmed in production).
        Precise-only is honest: row with `message_id` is flagged iff
        `message_id <= cursor`; rows without one stay blank.

        `campaign_ids` (optional) narrows the update so a scoped check
        doesn't stamp unrelated campaigns' rows for the same contact.

        Returns `{"precise": int, "coarse": int}`; `coarse` is always 0
        (kept in the return shape so call sites stay stable).
        """
        precise_total = 0

        def _do() -> None:
            nonlocal precise_total
            with self._conn() as c:
                for contact_id, max_id in updates:
                    max_id = int(max_id or 0)
                    if max_id <= 0:
                        continue
                    extra_filter = ""
                    extra_params: list = []
                    if campaign_ids:
                        ph = ",".join("?" * len(campaign_ids))
                        extra_filter = f" AND campaign_id IN ({ph})"
                        extra_params = [int(x) for x in campaign_ids]
                    cur = c.execute(
                        f"""UPDATE send_log
                            SET read_at=?
                            WHERE contact_id=?
                              AND status='sent'
                              AND read_at IS NULL
                              AND message_id IS NOT NULL
                              AND message_id <= ?{extra_filter}""",
                        (checked_at_iso, int(contact_id), max_id, *extra_params),
                    )
                    precise_total += cur.rowcount or 0
        self._retry_on_lock(_do)
        return {"precise": precise_total, "coarse": 0}

    def clear_coarse_read_flags(self) -> int:
        """Wipe `read_at` on rows whose flag came from the old coarse
        fallback (marker ` (coarse)` in the stored value) OR from any
        row with `message_id IS NULL`. Called once at migration time to
        erase the known false-positives the coarse path produced before
        it was removed.

        Returns the number of rows cleared."""
        def _do() -> int:
            with self._conn() as c:
                cur = c.execute(
                    """UPDATE send_log
                       SET read_at=NULL
                       WHERE read_at IS NOT NULL
                         AND (read_at LIKE '%(coarse)%'
                              OR message_id IS NULL)"""
                )
                return cur.rowcount or 0
        return self._retry_on_lock(_do) or 0

    # ---------- opt-outs ----------------------------------------------------

    def add_opt_out(self, tg_user_id: int, reason: str = "") -> None:
        with self._conn() as c:
            c.execute(
                "INSERT OR IGNORE INTO opt_outs (tg_user_id, reason) VALUES (?, ?)",
                (tg_user_id, reason[:500]),
            )

    def is_opted_out(self, tg_user_id: int) -> bool:
        with self._conn() as c:
            return c.execute(
                "SELECT 1 FROM opt_outs WHERE tg_user_id=?", (tg_user_id,)
            ).fetchone() is not None

    # ---------- dedup / precedence ------------------------------------------

    def find_duplicate_tg_users(self) -> list[sqlite3.Row]:
        """Contacts sharing a tg_user_id — two phones that resolved to the
        same Telegram user. We keep the one with the lowest contact id and
        skip the rest to avoid double-messaging.
        """
        with self._conn() as c:
            return list(c.execute("""
                SELECT tg_user_id, GROUP_CONCAT(id) AS ids, COUNT(*) AS n
                FROM contacts
                WHERE tg_user_id IS NOT NULL
                GROUP BY tg_user_id
                HAVING COUNT(*) > 1
            """))

    def contacts_already_messaged(self, days: int = 0) -> set[int]:
        """Return contact ids that were messaged in ANY campaign in the last
        N days (0 = ever). Useful to skip cross-campaign duplicates.
        """
        with self._conn() as c:
            if days <= 0:
                rows = c.execute(
                    "SELECT DISTINCT contact_id FROM send_log WHERE status='sent'"
                )
            else:
                rows = c.execute(
                    """SELECT DISTINCT contact_id FROM send_log
                       WHERE status='sent'
                       AND datetime(sent_at) >= datetime('now', ?)""",
                    (f"-{int(days)} days",),
                )
            return {row["contact_id"] for row in rows}

    # ---------- jobs --------------------------------------------------------

    def create_job(self, type_: str, payload_json: str | None = None,
                   backend: str = "thread", depends_on: int | None = None) -> int:
        with self._conn() as c:
            cur = c.execute(
                """INSERT INTO jobs (type, payload_json, backend, depends_on, status)
                   VALUES (?, ?, ?, ?, 'queued')""",
                (type_, payload_json, backend, depends_on),
            )
            return cur.lastrowid

    def mark_job_started(self, job_id: int) -> None:
        def _do():
            with self._conn() as c:
                c.execute(
                    """UPDATE jobs SET status='running', started_at=datetime('now')
                       WHERE id=?""", (job_id,),
                )
        self._retry_on_lock(_do)

    def mark_job_done(self, job_id: int, progress_json: str | None = None) -> None:
        def _do():
            with self._conn() as c:
                c.execute(
                    """UPDATE jobs SET status='done',
                       finished_at=datetime('now'),
                       progress_json=COALESCE(?, progress_json)
                       WHERE id=?""", (progress_json, job_id),
                )
        self._retry_on_lock(_do)

    def mark_job_error(self, job_id: int, error: str) -> None:
        def _do():
            with self._conn() as c:
                c.execute(
                    """UPDATE jobs SET status='error', error=?,
                       finished_at=datetime('now') WHERE id=?""",
                    (error[:1000], job_id),
                )
        self._retry_on_lock(_do)

    def mark_job_cancelled(self, job_id: int) -> None:
        def _do():
            with self._conn() as c:
                c.execute(
                    """UPDATE jobs SET status='cancelled',
                       finished_at=datetime('now') WHERE id=?""", (job_id,),
                )
        self._retry_on_lock(_do)

    def update_job_progress(self, job_id: int, progress_json: str) -> None:
        def _do():
            with self._conn() as c:
                c.execute(
                    """UPDATE jobs SET progress_json=?,
                       last_heartbeat=datetime('now') WHERE id=?""",
                    (progress_json, job_id),
                )
        self._retry_on_lock(_do)

    def update_job_heartbeat(self, job_id: int) -> None:
        """Bump `last_heartbeat` without touching progress_json. Used by the
        10-second keep-alive loop so a silent send doesn't look dead."""
        def _do():
            with self._conn() as c:
                c.execute(
                    "UPDATE jobs SET last_heartbeat=datetime('now') WHERE id=?",
                    (job_id,),
                )
        self._retry_on_lock(_do)

    def find_stale_jobs(
        self,
        stuck_queued_minutes: int = 2,
        silent_running_minutes: int = 3,
        stuck_starting_seconds: int = 90,
    ) -> list[dict]:
        """Return jobs the watchdog should recover. Each dict carries the
        `reason` string so the caller can mark the error clearly.
        Classification:

          * **stuck_queued** — queued > N min with no started_at.
          * **silent_running** — running but last_heartbeat older than N min
            (or NULL and started more than N min ago).
          * **stuck_at_starting** — running, status still 'starting' in
            progress_json > N seconds after started_at.
        """
        import json as _json
        results: list[dict] = []
        with self._conn() as c:
            # Stuck queued.
            for row in c.execute(
                f"""SELECT id, type FROM jobs
                    WHERE status='queued'
                      AND started_at IS NULL
                      AND datetime(queued_at, '+{int(stuck_queued_minutes)} minutes') <= datetime('now')"""
            ):
                results.append({
                    "id": int(row[0]), "type": row[1],
                    "reason": f"watchdog: never started after queued for {stuck_queued_minutes}+ min",
                })

            # Silent running.
            for row in c.execute(
                f"""SELECT id, type, started_at, last_heartbeat, progress_json FROM jobs
                    WHERE status='running'
                      AND (
                        (last_heartbeat IS NOT NULL
                         AND datetime(last_heartbeat, '+{int(silent_running_minutes)} minutes') <= datetime('now'))
                        OR
                        (last_heartbeat IS NULL AND started_at IS NOT NULL
                         AND datetime(started_at, '+{int(silent_running_minutes)} minutes') <= datetime('now'))
                      )"""
            ):
                results.append({
                    "id": int(row[0]), "type": row[1],
                    "reason": f"watchdog: no heartbeat for {silent_running_minutes}+ min",
                })

            # Stuck-at-starting — progress_json.status == 'starting' long
            # after the job entered running. Caught separately because a
            # job can be silent AND stuck-at-starting, and we want the
            # clearer message.
            for row in c.execute(
                f"""SELECT id, type, started_at, progress_json FROM jobs
                    WHERE status='running'
                      AND started_at IS NOT NULL
                      AND datetime(started_at, '+{int(stuck_starting_seconds)} seconds') <= datetime('now')"""
            ):
                try:
                    progress = _json.loads(row[3] or "{}")
                except Exception:
                    progress = {}
                if progress.get("status") == "starting":
                    results.append({
                        "id": int(row[0]), "type": row[1],
                        "reason": f"watchdog: stuck at 'starting' for {stuck_starting_seconds}+ s",
                    })

        # De-dupe by id, prefer the most specific reason (starting beats silent).
        seen: dict[int, dict] = {}
        for r in results:
            prior = seen.get(r["id"])
            if prior is None or "stuck at 'starting'" in r["reason"]:
                seen[r["id"]] = r
        return list(seen.values())

    def get_job(self, job_id: int) -> sqlite3.Row | None:
        with self._conn() as c:
            return c.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()

    def list_jobs(self, limit: int = 100) -> list[sqlite3.Row]:
        with self._conn() as c:
            return list(c.execute(
                "SELECT * FROM jobs ORDER BY id DESC LIMIT ?", (int(limit),)
            ))

    def has_running_job(self, type_: str) -> bool:
        with self._conn() as c:
            return c.execute(
                """SELECT 1 FROM jobs WHERE type=? AND status IN ('queued','running')
                   LIMIT 1""", (type_,),
            ).fetchone() is not None

    def dependency_satisfied(self, job_id: int) -> bool:
        """True if the job has no dependency or if the dependency is done."""
        job = self.get_job(job_id)
        if not job or job["depends_on"] is None:
            return True
        parent = self.get_job(job["depends_on"])
        return parent is not None and parent["status"] == "done"

    def jobs_df(self):
        import pandas as pd
        with self._conn() as c:
            return pd.read_sql_query(
                """SELECT id, type, status, backend, depends_on,
                   queued_at, started_at, finished_at,
                   progress_json, error
                   FROM jobs ORDER BY id DESC""",
                c,
            )
