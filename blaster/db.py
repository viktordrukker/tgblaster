"""SQLite state: contact lists, campaigns, send journal, opt-outs, run lock.

The send journal is the safety core. A row is written as 'pending'
BEFORE the Telegram call and flipped to 'sent' after it. If the process
dies in between, the row stays 'pending' and the next run marks it
'uncertain' instead of re-sending — a missed message is recoverable
(`send --retry-uncertain`), a duplicate DM is not.
"""
from __future__ import annotations

import json
import os
import socket
import sqlite3
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Iterable, Iterator

SCHEMA = """
CREATE TABLE IF NOT EXISTS contacts (
    id              INTEGER PRIMARY KEY,
    list_name       TEXT NOT NULL,
    key             TEXT NOT NULL,
    row_no          INTEGER,
    name            TEXT NOT NULL DEFAULT '',
    first_name      TEXT NOT NULL DEFAULT '',
    phone           TEXT,
    phones_json     TEXT NOT NULL DEFAULT '[]',
    username        TEXT,
    extra_json      TEXT NOT NULL DEFAULT '{}',
    status          TEXT NOT NULL DEFAULT 'new',
    tg_user_id      INTEGER,
    tg_access_hash  INTEGER,
    tg_username     TEXT,
    tg_first_name   TEXT,
    resolved_via    TEXT,
    resolve_error   TEXT,
    resolved_at     REAL,
    created_at      REAL NOT NULL,
    updated_at      REAL NOT NULL,
    UNIQUE (list_name, key)
);
CREATE INDEX IF NOT EXISTS ix_contacts_list_status ON contacts(list_name, status);
CREATE INDEX IF NOT EXISTS ix_contacts_tg ON contacts(tg_user_id);

CREATE TABLE IF NOT EXISTS campaigns (
    id            INTEGER PRIMARY KEY,
    name          TEXT NOT NULL UNIQUE,
    list_name     TEXT NOT NULL,
    template      TEXT NOT NULL,
    image_path    TEXT,
    parse_mode    TEXT NOT NULL DEFAULT 'md',
    link_preview  INTEGER NOT NULL DEFAULT 1,
    status        TEXT NOT NULL DEFAULT 'draft',
    created_at    REAL NOT NULL,
    updated_at    REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS sends (
    campaign_id   INTEGER NOT NULL REFERENCES campaigns(id) ON DELETE CASCADE,
    contact_id    INTEGER NOT NULL REFERENCES contacts(id) ON DELETE CASCADE,
    tg_user_id    INTEGER,
    status        TEXT NOT NULL,
    detail        TEXT NOT NULL DEFAULT '',
    message_id    INTEGER,
    attempts      INTEGER NOT NULL DEFAULT 0,
    created_at    REAL NOT NULL,
    updated_at    REAL NOT NULL,
    PRIMARY KEY (campaign_id, contact_id)
);
CREATE INDEX IF NOT EXISTS ix_sends_status_time ON sends(status, updated_at);
CREATE INDEX IF NOT EXISTS ix_sends_campaign_tg ON sends(campaign_id, tg_user_id);

CREATE TABLE IF NOT EXISTS optouts (
    value       TEXT PRIMARY KEY,
    reason      TEXT NOT NULL DEFAULT '',
    created_at  REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS run_lock (
    name       TEXT PRIMARY KEY,
    owner      TEXT NOT NULL,
    heartbeat  REAL NOT NULL
);
"""

# Terminal statuses: never retried automatically.
DONE_STATES = ("sent", "skipped", "uncertain")
LOCK_STALE_SEC = 180


class LockHeld(RuntimeError):
    pass


def _now() -> float:
    return time.time()


class Database:
    def __init__(self, path: str | Path):
        self.path = Path(path)
        if str(path) != ":memory:":
            self.path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(str(path), timeout=30, isolation_level=None)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA foreign_keys=ON")
        self.conn.execute("PRAGMA busy_timeout=30000")
        self.conn.executescript(SCHEMA)
        self._owner = f"{socket.gethostname()}:{os.getpid()}"

    def close(self) -> None:
        self.conn.close()

    @contextmanager
    def tx(self) -> Iterator[sqlite3.Connection]:
        self.conn.execute("BEGIN IMMEDIATE")
        try:
            yield self.conn
        except BaseException:
            self.conn.execute("ROLLBACK")
            raise
        self.conn.execute("COMMIT")

    # ------------------------------------------------------------------ #
    # Run lock — exactly one process may drive the Telegram session.
    # Two clients on one auth key can get the session revoked.
    # ------------------------------------------------------------------ #

    def acquire_lock(self, name: str = "telegram") -> None:
        with self.tx() as c:
            row = c.execute("SELECT owner, heartbeat FROM run_lock WHERE name=?", (name,)).fetchone()
            if row and row["owner"] != self._owner and _now() - row["heartbeat"] < LOCK_STALE_SEC:
                raise LockHeld(
                    f"Сессия Telegram занята процессом {row['owner']} "
                    f"(heartbeat {int(_now() - row['heartbeat'])} с назад). "
                    f"Дождись его завершения или останови его.")
            c.execute("INSERT OR REPLACE INTO run_lock(name, owner, heartbeat) VALUES (?,?,?)",
                      (name, self._owner, _now()))

    def heartbeat(self, name: str = "telegram") -> None:
        self.conn.execute("UPDATE run_lock SET heartbeat=? WHERE name=? AND owner=?",
                          (_now(), name, self._owner))

    def release_lock(self, name: str = "telegram") -> None:
        self.conn.execute("DELETE FROM run_lock WHERE name=? AND owner=?", (name, self._owner))

    # ------------------------------------------------------------------ #
    # Contacts
    # ------------------------------------------------------------------ #

    def upsert_contacts(self, list_name: str, contacts: Iterable) -> tuple[int, int]:
        """Insert new contacts, refresh table data of existing ones while
        keeping their Telegram resolution. Returns (inserted, updated)."""
        ins = upd = 0
        now = _now()
        with self.tx() as c:
            for pc in contacts:
                phone = pc.phones[0] if pc.phones else None
                existing = c.execute(
                    "SELECT id, phones_json, username FROM contacts WHERE list_name=? AND key=?",
                    (list_name, pc.key)).fetchone()
                if existing is None:
                    c.execute(
                        """INSERT INTO contacts(list_name, key, row_no, name, first_name, phone,
                               phones_json, username, extra_json, created_at, updated_at)
                           VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
                        (list_name, pc.key, pc.row_no, pc.name, pc.first_name, phone,
                         json.dumps(pc.phones), pc.username,
                         json.dumps(pc.extra, ensure_ascii=False), now, now))
                    ins += 1
                else:
                    old_user = existing["username"]
                    new_contact_info = (json.loads(existing["phones_json"]) != pc.phones
                                        or (pc.username or None) != (old_user or None))
                    c.execute(
                        """UPDATE contacts SET row_no=?, name=?, first_name=?, phone=?,
                               phones_json=?, username=?, extra_json=?, updated_at=?,
                               status = CASE WHEN ? AND status IN ('not_found','error')
                                             THEN 'new' ELSE status END
                           WHERE id=?""",
                        (pc.row_no, pc.name, pc.first_name, phone, json.dumps(pc.phones),
                         pc.username, json.dumps(pc.extra, ensure_ascii=False), now,
                         1 if new_contact_info else 0, existing["id"]))
                    upd += 1
        return ins, upd

    @staticmethod
    def _row(r: sqlite3.Row | None) -> dict | None:
        if r is None:
            return None
        d = dict(r)
        if "extra_json" in d:
            d["extra"] = json.loads(d.pop("extra_json") or "{}")
        if "phones_json" in d:
            d["phones"] = json.loads(d.pop("phones_json") or "[]")
        return d

    def contacts(self, list_name: str, status: str | None = None) -> list[dict]:
        q = "SELECT * FROM contacts WHERE list_name=?"
        args: list = [list_name]
        if status:
            q += " AND status=?"
            args.append(status)
        q += " ORDER BY row_no, id"
        return [self._row(r) for r in self.conn.execute(q, args)]

    def get_contact(self, contact_id: int) -> dict | None:
        return self._row(self.conn.execute("SELECT * FROM contacts WHERE id=?",
                                           (contact_id,)).fetchone())

    def lists(self) -> list[dict]:
        return [dict(r) for r in self.conn.execute(
            """SELECT list_name, COUNT(*) AS total,
                      SUM(status='resolved') AS resolved,
                      SUM(status='not_found') AS not_found,
                      SUM(status='error') AS errors,
                      SUM(status='new') AS pending
               FROM contacts GROUP BY list_name ORDER BY list_name""")]

    def list_stats(self, list_name: str) -> dict:
        r = self.conn.execute(
            """SELECT COUNT(*) AS total,
                      COALESCE(SUM(status='resolved'),0) AS resolved,
                      COALESCE(SUM(status='not_found'),0) AS not_found,
                      COALESCE(SUM(status='error'),0) AS errors,
                      COALESCE(SUM(status='new'),0) AS pending
               FROM contacts WHERE list_name=?""", (list_name,)).fetchone()
        return dict(r)

    def list_keys(self, list_name: str) -> set[str]:
        keys: set[str] = set()
        for r in self.conn.execute("SELECT extra_json FROM contacts WHERE list_name=?",
                                   (list_name,)):
            keys.update(json.loads(r["extra_json"] or "{}").keys())
        return keys

    def to_resolve(self, list_name: str, retry_errors: bool = False) -> list[dict]:
        states = ("new", "error") if retry_errors else ("new",)
        q = f"SELECT * FROM contacts WHERE list_name=? AND status IN ({','.join('?' * len(states))}) ORDER BY row_no"
        return [self._row(r) for r in self.conn.execute(q, (list_name, *states))]

    def mark_resolved(self, contact_id: int, user_id: int, access_hash: int | None,
                      username: str | None, first_name: str | None, via: str) -> None:
        self.conn.execute(
            """UPDATE contacts SET status='resolved', tg_user_id=?, tg_access_hash=?,
                   tg_username=?, tg_first_name=?, resolved_via=?, resolve_error=NULL,
                   resolved_at=?, updated_at=? WHERE id=?""",
            (user_id, access_hash, username, first_name, via, _now(), _now(), contact_id))

    def mark_not_found(self, contact_id: int, detail: str = "") -> None:
        self.conn.execute(
            "UPDATE contacts SET status='not_found', resolve_error=?, updated_at=? WHERE id=?",
            (detail, _now(), contact_id))

    def mark_resolve_error(self, contact_id: int, detail: str) -> None:
        self.conn.execute(
            "UPDATE contacts SET status='error', resolve_error=?, updated_at=? WHERE id=?",
            (detail[:500], _now(), contact_id))

    # ------------------------------------------------------------------ #
    # Opt-outs
    # ------------------------------------------------------------------ #

    def add_optout(self, value: str, reason: str = "") -> None:
        self.conn.execute("INSERT OR REPLACE INTO optouts(value, reason, created_at) VALUES (?,?,?)",
                          (value.strip().lower(), reason, _now()))

    def optouts(self) -> list[dict]:
        return [dict(r) for r in self.conn.execute(
            "SELECT value, reason, created_at FROM optouts ORDER BY created_at DESC")]

    def optout_values(self) -> set[str]:
        return {r[0] for r in self.conn.execute("SELECT value FROM optouts")}

    @staticmethod
    def optout_keys(contact: dict) -> set[str]:
        keys = {p.lower() for p in contact.get("phones") or []}
        for u in (contact.get("username"), contact.get("tg_username")):
            if u:
                keys.add("@" + u.lower())
        if contact.get("tg_user_id"):
            keys.add(f"id:{contact['tg_user_id']}")
        return keys

    # ------------------------------------------------------------------ #
    # Campaigns
    # ------------------------------------------------------------------ #

    def create_campaign(self, name: str, list_name: str, template: str,
                        image_path: str | None, parse_mode: str = "md",
                        link_preview: bool = True) -> int:
        now = _now()
        cur = self.conn.execute(
            """INSERT INTO campaigns(name, list_name, template, image_path, parse_mode,
                   link_preview, created_at, updated_at) VALUES (?,?,?,?,?,?,?,?)""",
            (name, list_name, template, image_path, parse_mode, int(link_preview), now, now))
        return int(cur.lastrowid)

    def update_campaign(self, campaign_id: int, **fields) -> None:
        allowed = {"template", "image_path", "parse_mode", "link_preview", "status", "list_name"}
        bad = set(fields) - allowed
        if bad:
            raise ValueError(f"cannot update {bad}")
        if not fields:
            return
        sets = ", ".join(f"{k}=?" for k in fields)
        self.conn.execute(f"UPDATE campaigns SET {sets}, updated_at=? WHERE id=?",
                          (*fields.values(), _now(), campaign_id))

    def get_campaign(self, ref: int | str) -> dict | None:
        if isinstance(ref, int) or str(ref).isdigit():
            r = self.conn.execute("SELECT * FROM campaigns WHERE id=?", (int(ref),)).fetchone()
        else:
            r = self.conn.execute("SELECT * FROM campaigns WHERE name=?", (ref,)).fetchone()
        return dict(r) if r else None

    def campaigns(self) -> list[dict]:
        return [dict(r) for r in self.conn.execute("SELECT * FROM campaigns ORDER BY id")]

    # ------------------------------------------------------------------ #
    # Send journal
    # ------------------------------------------------------------------ #

    def mark_stale_pending(self, campaign_id: int) -> int:
        """Pending rows left by a crashed run → 'uncertain' (not re-sent)."""
        cur = self.conn.execute(
            """UPDATE sends SET status='uncertain',
                   detail='прервано между отправкой и подтверждением — проверь вручную',
                   updated_at=? WHERE campaign_id=? AND status='pending'""",
            (_now(), campaign_id))
        return cur.rowcount

    def requeue(self, campaign_id: int, statuses: Iterable[str]) -> int:
        st = tuple(statuses)
        if not st:
            return 0
        cur = self.conn.execute(
            f"DELETE FROM sends WHERE campaign_id=? AND status IN ({','.join('?' * len(st))})",
            (campaign_id, *st))
        return cur.rowcount

    def targets(self, campaign_id: int) -> list[dict]:
        """Resolved contacts of the campaign's list that still need a message,
        deduplicated by Telegram user and filtered by opt-outs."""
        camp = self.get_campaign(campaign_id)
        if camp is None:
            raise ValueError(f"кампания {campaign_id} не найдена")
        handled_contacts = set()
        handled_users = set()
        for r in self.conn.execute(
                "SELECT contact_id, tg_user_id, status FROM sends WHERE campaign_id=?",
                (campaign_id,)):
            handled_contacts.add(r["contact_id"])
            if r["tg_user_id"] and r["status"] != "error":
                handled_users.add(r["tg_user_id"])
        optouts = self.optout_values()
        out = []
        for c in self.contacts(camp["list_name"], status="resolved"):
            if c["id"] in handled_contacts or c["tg_user_id"] in handled_users:
                continue
            if self.optout_keys(c) & optouts:
                continue
            handled_users.add(c["tg_user_id"])
            out.append(c)
        return out

    def reserve(self, campaign_id: int, contact: dict) -> None:
        now = _now()
        self.conn.execute(
            """INSERT INTO sends(campaign_id, contact_id, tg_user_id, status, attempts,
                   created_at, updated_at) VALUES (?,?,?,'pending',1,?,?)
               ON CONFLICT(campaign_id, contact_id) DO UPDATE SET
                   status='pending', attempts=attempts+1, updated_at=excluded.updated_at""",
            (campaign_id, contact["id"], contact.get("tg_user_id"), now, now))

    def finish(self, campaign_id: int, contact_id: int, status: str,
               detail: str = "", message_id: int | None = None) -> None:
        assert status in ("sent", "skipped", "error", "uncertain")
        self.conn.execute(
            """UPDATE sends SET status=?, detail=?, message_id=COALESCE(?, message_id),
                   updated_at=? WHERE campaign_id=? AND contact_id=?""",
            (status, detail[:500], message_id, _now(), campaign_id, contact_id))

    def release(self, campaign_id: int, contact_id: int) -> None:
        """Drop a reservation for a message that definitely did NOT go out."""
        self.conn.execute("DELETE FROM sends WHERE campaign_id=? AND contact_id=? AND status='pending'",
                          (campaign_id, contact_id))

    def sent_since(self, since_ts: float) -> int:
        return int(self.conn.execute(
            "SELECT COUNT(*) FROM sends WHERE status='sent' AND updated_at>=?",
            (since_ts,)).fetchone()[0])

    def oldest_sent_since(self, since_ts: float) -> float | None:
        r = self.conn.execute("SELECT MIN(updated_at) FROM sends WHERE status='sent' AND updated_at>=?",
                              (since_ts,)).fetchone()
        return r[0]

    def campaign_stats(self, campaign_id: int) -> dict:
        rows = self.conn.execute(
            "SELECT status, COUNT(*) n FROM sends WHERE campaign_id=? GROUP BY status",
            (campaign_id,)).fetchall()
        stats = {"sent": 0, "skipped": 0, "error": 0, "uncertain": 0, "pending": 0}
        stats.update({r["status"]: r["n"] for r in rows})
        stats["remaining"] = len(self.targets(campaign_id))
        return stats

    def report(self, campaign_id: int) -> list[dict]:
        camp = self.get_campaign(campaign_id)
        rows = self.conn.execute(
            """SELECT c.row_no, c.name, c.phone, c.username, c.status AS resolve_status,
                      c.resolve_error, c.tg_user_id, c.tg_username,
                      s.status AS send_status, s.detail, s.message_id, s.updated_at
               FROM contacts c LEFT JOIN sends s
                    ON s.contact_id=c.id AND s.campaign_id=?
               WHERE c.list_name=? ORDER BY c.row_no""",
            (campaign_id, camp["list_name"])).fetchall()
        return [dict(r) for r in rows]
