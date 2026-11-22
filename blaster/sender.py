"""Campaign runner: paced, resumable, duplicate-safe sending.

Per contact:   reserve (journal 'pending') → send → finish('sent').
Errors are classified by whether the message *could* have reached
Telegram:
  * RPC errors (Telegram answered "no")       → definitely not delivered
  * timeouts / connection drops mid-call       → 'uncertain', never auto-resent
"""
from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

from telethon import errors
from telethon.tl import types

from .config import Pacing
from .db import Database
from .template import MAX_CAPTION, MAX_TEXT, render, validate

log = logging.getLogger("blaster.sender")

SEND_TIMEOUT = 60
MAX_CONSECUTIVE_FAILURES = 5

SKIP_ERRORS = (
    errors.UserPrivacyRestrictedError,
    errors.UserIsBlockedError,
    errors.YouBlockedUserError,
    errors.InputUserDeactivatedError,
    errors.UserIsBotError,
    errors.ChatWriteForbiddenError,
    errors.PeerIdInvalidError,
)
FATAL_ERRORS = (
    errors.AuthKeyUnregisteredError,
    errors.AuthKeyDuplicatedError,
    errors.SessionRevokedError,
    errors.UserDeactivatedBanError,
    errors.UserDeactivatedError,
)
SKIP_RPC_MARKERS = ("PAYMENT", "PREMIUM", "PRIVACY")


@dataclass
class RunResult:
    sent: int = 0
    skipped: int = 0
    errors: int = 0
    uncertain: int = 0
    stopped_reason: str = ""
    remaining: int = 0

    def line(self) -> str:
        return (f"отправлено {self.sent}, пропущено {self.skipped}, ошибок {self.errors}, "
                f"неясно {self.uncertain}, осталось {self.remaining}"
                + (f" — остановка: {self.stopped_reason}" if self.stopped_reason else ""))


def _peer(contact: dict):
    if contact.get("tg_access_hash"):
        return types.InputPeerUser(contact["tg_user_id"], contact["tg_access_hash"])
    return contact["tg_user_id"]


def _label(c: dict) -> str:
    who = c.get("name") or c.get("first_name") or "—"
    handle = c.get("phone") or (f"@{c['username']}" if c.get("username") else "")
    return f"{who} ({handle})" if handle else who


def check_campaign(db: Database, camp: dict) -> list[str]:
    has_image = bool(camp.get("image_path"))
    problems = validate(camp["template"], db.list_keys(camp["list_name"]), has_image=has_image)
    if has_image and not Path(camp["image_path"]).is_file():
        problems.append(f"картинка не найдена: {camp['image_path']}")
    return problems


def rendered_too_long(text: str, has_image: bool) -> bool:
    return len(text) > (MAX_CAPTION if has_image else MAX_TEXT)


class Runner:
    def __init__(self, client, db: Database, campaign_id: int, pacing: Pacing, *,
                 stop: asyncio.Event | None = None,
                 out: Callable[[str], None] = print,
                 clock: Callable[[], float] = time.time,
                 sleep_fn=None):
        self.client = client
        self.db = db
        self.cid = campaign_id
        self.pacing = pacing
        self.stop = stop or asyncio.Event()
        self.out = out
        self.clock = clock
        self._sleep_fn = sleep_fn
        self.media = None
        self.res = RunResult()

    # -- helpers ----------------------------------------------------------
    async def sleep(self, seconds: float) -> bool:
        """Interruptible, heartbeat-keeping sleep. True if stopped."""
        if self._sleep_fn is not None:
            await self._sleep_fn(seconds)
            return self.stop.is_set()
        end = time.monotonic() + seconds
        while True:
            left = end - time.monotonic()
            if left <= 0:
                return self.stop.is_set()
            try:
                await asyncio.wait_for(self.stop.wait(), timeout=min(left, 30))
                return True
            except asyncio.TimeoutError:
                self.db.heartbeat()

    async def _ensure_connected(self) -> None:
        if not self.client.is_connected():
            await self.client.connect()

    async def _send(self, contact: dict, text: str, camp: dict):
        await self._ensure_connected()
        file = self.media or (camp["image_path"] or None)
        coro = self.client.send_message(
            _peer(contact), text,
            parse_mode=camp["parse_mode"],
            link_preview=bool(camp["link_preview"]),
            file=file,
        )
        msg = await asyncio.wait_for(coro, timeout=SEND_TIMEOUT)
        if camp["image_path"] and self.media is None:
            # Reuse the uploaded photo for every next message: no re-upload.
            self.media = getattr(msg, "photo", None) or getattr(msg, "document", None)
        return msg

    # -- main loop ----------------------------------------------------------
    async def run(self, *, limit: int | None = None, until_done: bool = False,
                  retry_uncertain: bool = False, retry_errors: bool = False) -> RunResult:
        db, res = self.db, self.res
        camp = db.get_campaign(self.cid)
        if camp is None:
            raise ValueError(f"кампания {self.cid} не найдена")
        problems = check_campaign(db, camp)
        if problems:
            raise ValueError("Кампания не прошла проверку:\n  - " + "\n  - ".join(problems))

        stale = db.mark_stale_pending(self.cid)
        if stale:
            self.out(f"⚠ {stale} отправок прошлого запуска прервались на полпути → 'uncertain' "
                     f"(не переотправляю; см. report).")
        requeue = (["uncertain"] if retry_uncertain else []) + (["error"] if retry_errors else [])
        if requeue:
            n = db.requeue(self.cid, requeue)
            self.out(f"↻ вернул в очередь {n} контактов ({', '.join(requeue)})")

        me = await self.client.get_me()
        targets = [t for t in db.targets(self.cid) if t["tg_user_id"] != getattr(me, "id", None)]
        total = len(targets)
        self.out(f"▶ «{camp['name']}»: к отправке {total}"
                 + (f", лимит запуска {limit}" if limit else ""))
        if not total:
            res.remaining = 0
            db.update_campaign(self.cid, status="done")
            return res
        db.update_campaign(self.cid, status="running")

        peer_floods = 0
        consecutive_fail = 0
        sent_in_run = 0
        idx = 0
        try:
            while idx < len(targets):
                c = targets[idx]
                if self.stop.is_set():
                    res.stopped_reason = "остановлено пользователем"
                    break
                if limit is not None and sent_in_run >= limit:
                    res.stopped_reason = f"достигнут лимит запуска ({limit})"
                    break

                wait = self.pacing.seconds_until_window()
                if wait:
                    if not until_done:
                        res.stopped_reason = "вне окна отправки (SEND_WINDOW)"
                        break
                    self.out(f"🌙 вне окна отправки — сплю {wait // 60} мин")
                    await self.sleep(wait)
                    continue

                day_ago = self.clock() - 86400
                if db.sent_since(day_ago) >= self.pacing.daily_cap:
                    if not until_done:
                        res.stopped_reason = f"дневной лимит {self.pacing.daily_cap} за 24ч"
                        break
                    oldest = db.oldest_sent_since(day_ago) or self.clock()
                    wait = max(60, int(oldest + 86400 - self.clock()) + 30)
                    self.out(f"⏸ дневной лимит {self.pacing.daily_cap} — жду {wait // 60} мин")
                    await self.sleep(wait)
                    continue

                text = render(camp["template"], c)
                if rendered_too_long(text, bool(camp["image_path"])):
                    db.reserve(self.cid, c)
                    db.finish(self.cid, c["id"], "error", "текст длиннее лимита Telegram")
                    res.errors += 1
                    idx += 1
                    continue

                db.reserve(self.cid, c)
                label = f"[{idx + 1}/{total}] {_label(c)}"
                try:
                    msg = await self._send(c, text, camp)
                except errors.FloodWaitError as e:
                    db.release(self.cid, c["id"])
                    if e.seconds > self.pacing.max_flood_wait:
                        res.stopped_reason = (f"FloodWait {e.seconds}s (> {self.pacing.max_flood_wait}s) "
                                              f"— продолжи позже той же командой")
                        break
                    self.out(f"⏱ FloodWait {e.seconds}s — жду и повторяю {label}")
                    await self.sleep(e.seconds + 5)
                    continue                          # same contact again
                except errors.PeerFloodError:
                    db.release(self.cid, c["id"])
                    peer_floods += 1
                    self.out(f"⚠ PeerFlood на {label} ({peer_floods}/{self.pacing.peer_flood_stop})")
                    if peer_floods >= self.pacing.peer_flood_stop:
                        res.stopped_reason = ("PeerFlood: Telegram ограничил аккаунт для сообщений "
                                              "незнакомым. Проверь @SpamBot, продолжи через сутки.")
                        break
                    db.reserve(self.cid, c)
                    db.finish(self.cid, c["id"], "error", "PeerFlood")
                    res.errors += 1
                    idx += 1
                    await self.sleep(self.pacing.long_pause_min)
                    continue
                except FATAL_ERRORS as e:
                    db.release(self.cid, c["id"])
                    res.stopped_reason = f"аккаунт недоступен: {type(e).__name__}"
                    break
                except SKIP_ERRORS as e:
                    db.finish(self.cid, c["id"], "skipped", type(e).__name__)
                    res.skipped += 1
                    self.out(f"– {label}: пропуск ({type(e).__name__})")
                    consecutive_fail = 0
                except errors.RPCError as e:
                    msg_txt = f"{type(e).__name__}: {e}"
                    if any(m in str(e).upper() for m in SKIP_RPC_MARKERS):
                        db.finish(self.cid, c["id"], "skipped", msg_txt)
                        res.skipped += 1
                        self.out(f"– {label}: пропуск ({msg_txt})")
                    else:
                        db.finish(self.cid, c["id"], "error", msg_txt)
                        res.errors += 1
                        consecutive_fail += 1
                        self.out(f"✗ {label}: {msg_txt}")
                except (asyncio.TimeoutError, ConnectionError, OSError) as e:
                    db.finish(self.cid, c["id"], "uncertain", f"{type(e).__name__}: {e}")
                    res.uncertain += 1
                    consecutive_fail += 1
                    self.out(f"? {label}: связь оборвалась во время отправки — 'uncertain'")
                else:
                    db.finish(self.cid, c["id"], "sent", "", getattr(msg, "id", None))
                    res.sent += 1
                    sent_in_run += 1
                    consecutive_fail = 0
                    self.out(f"✓ {label}")

                idx += 1
                if consecutive_fail >= MAX_CONSECUTIVE_FAILURES:
                    res.stopped_reason = f"{consecutive_fail} ошибок подряд — проверь сеть/аккаунт"
                    break
                db.heartbeat()
                if idx < len(targets):
                    delay = self.pacing.delay_after(sent_in_run)
                    if delay >= self.pacing.long_pause_min:
                        self.out(f"☕ длинная пауза {delay // 60} мин")
                    await self.sleep(delay)
        finally:
            res.remaining = len(db.targets(self.cid))
            db.update_campaign(self.cid, status="done" if res.remaining == 0 else "paused")
        return res
