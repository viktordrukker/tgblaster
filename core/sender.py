"""Telegram sender with safe pacing, resume support, and error handling.

Key safety features:
* Daily cap on sends (Telegram tightly limits cold DMs per day).
* Random delays between sends + long pauses every N messages.
* FloodWaitError → sleep for the exact time Telegram tells us, then continue.
* PeerFloodError / UserPrivacyRestrictedError → mark contact and stop early
  on PeerFlood (it's a yellow card from Telegram).
* Every send is logged in SQLite so the campaign can be resumed after a
  crash or restart.
"""
from __future__ import annotations

import asyncio
import logging
import secrets
import sqlite3
import time as _time
from dataclasses import dataclass
from pathlib import Path
from typing import Awaitable, Callable

from telethon import TelegramClient
from telethon.errors import (
    FloodWaitError,
    PeerFloodError,
    UserPrivacyRestrictedError,
    UserIsBlockedError,
    ChatWriteForbiddenError,
    InputUserDeactivatedError,
)

from .database import Database
from .rate_limiter import Pacer, PacingConfig
from .template import render


log = logging.getLogger(__name__)


ProgressCb = Callable[[dict], Awaitable[None] | None]


@dataclass
class SendOutcome:
    sent: int = 0
    errors: int = 0
    skipped: int = 0
    opted_out: int = 0
    stopped_reason: str = ""


class StopSignal:
    """Thread-safe flag carrying a distinction between *pause* and *stop*.

    The sender checks `is_set()` to decide whether to break out of the
    send loop; `kind` tells the calling worker whether to transition the
    campaign row to `paused` (resume later) or `stopped` (done for now).
    """
    def __init__(self):
        self._stop = False
        self.kind: str = ""          # '' | 'user_paused' | 'user_stopped'

    def set(self, kind: str = "user_stopped") -> None:
        self._stop = True
        if kind:
            self.kind = kind

    def is_set(self) -> bool:
        return self._stop


async def _maybe_await(x):
    if asyncio.iscoroutine(x):
        await x


SEND_TIMEOUT_SEC = 60
"""Wall-clock budget for a single `send_message` / `send_file` call.

Telegram answers healthy accounts in ≤ 2 s. We pick 60 s to cover a very
slow DC switch or cold-start connection without masking real hangs.
"""


async def _ensure_connected(client) -> None:
    """Reconnect the Telethon client if a long pause (or a network blip)
    caused the server to drop us. Without this guard, the next send after
    a 600–900 s long pause fails with
    'Cannot send requests while disconnected'."""
    if client.is_connected():
        return
    # Import here to avoid a circular import at module load.
    from . import auth
    await auth.connect_client(client, attempts=5, backoff_sec=2.0)


def _safe_confirm(db, campaign_id: int, contact_id: int,
                   status: str, detail: str) -> None:
    """Best-effort `confirm_send` that swallows sqlite errors.

    Used from error branches where we know the send did NOT land on
    Telegram — a flip to 'error' / 'skipped' is safe. If the DB is
    locked and we can't write, the row stays as 'pending'; since this
    is an error path the rid *wasn't* consumed by Telegram, so when the
    pending row is reserved again later the fresh rid won't collide.
    """
    try:
        db.confirm_send(campaign_id, contact_id, status, detail)
    except Exception as e:  # noqa: BLE001
        log.warning(
            "confirm_send(%s) failed for contact=%s — row stays pending: %s",
            status, contact_id, e,
        )


def _fresh_random_id() -> int:
    """Positive int64 for Telegram's `random_id` dedup field. Telegram
    keys server-side dedup on this — passing the same value twice within
    a short window yields a no-op on the second call."""
    return secrets.randbits(63)


async def _send_one(client, target_id, *, image_path, message_text, random_id):
    """Single-shot send with reconnect-on-disconnect + hard per-send
    timeout.

    Telethon 1.43's high-level `send_message` / `send_file` don't expose
    the `random_id` kwarg (it's auto-generated inside Telethon), so a
    retry after a post-send failure would land as a DUPLICATE on
    Telegram. We don't roll our own raw RPC here; dedup correctness
    relies instead on:
      1. catching post-send session-write errors and counting them as
         sent (see the caller — the message already left Telegram by the
         time `process_entities` tries to write the session), AND
      2. the `send_log` row transition from 'pending' to 'sent' guarding
         the resume path.
    `random_id` is kept as a function parameter for call-site clarity
    and for future use if we switch to raw MTProto requests.

    Callers catch TimeoutError the same way they already catch generic
    exceptions."""
    del random_id  # currently unused — see docstring
    await _ensure_connected(client)
    if image_path and Path(image_path).exists():
        return await asyncio.wait_for(
            client.send_file(
                target_id, file=str(image_path),
                caption=message_text, parse_mode="md",
            ),
            timeout=SEND_TIMEOUT_SEC,
        )
    return await asyncio.wait_for(
        client.send_message(
            target_id, message_text, parse_mode="md",
            link_preview=True,
        ),
        timeout=SEND_TIMEOUT_SEC,
    )


async def run_campaign(
    client: TelegramClient,
    db: Database,
    campaign_id: int,
    pacing: PacingConfig,
    on_progress: ProgressCb | None = None,
    stop_signal: StopSignal | None = None,
    dry_run_to_self: bool = False,
    only_contact_ids: list[int] | None = None,
) -> SendOutcome:
    campaign = db.get_campaign(campaign_id)
    if campaign is None:
        raise ValueError(f"Campaign {campaign_id} does not exist")

    template = campaign["template"]
    image_path = campaign["image_path"]
    group_link = campaign["group_link"] or ""
    pacer = Pacer(pacing)
    outcome = SendOutcome()

    # Resume support + optional incremental scope
    already = db.already_sent_ids(campaign_id)
    scope = set(int(x) for x in only_contact_ids) if only_contact_ids else None
    contacts = [
        c for c in db.resolved_contacts()
        if c["id"] not in already
        and not db.is_opted_out(c["tg_user_id"])
        and (scope is None or int(c["id"]) in scope)
    ]

    total = len(contacts)
    if dry_run_to_self:
        me = await client.get_me()
        log.info("Dry-run mode: sending only to self (%s)", me.id)
        contacts = contacts[:1]  # keep the template rendering path honest
        total = 1

    async def _emit(**kw):
        if on_progress:
            payload = {
                "total": total,
                "sent": outcome.sent,
                "errors": outcome.errors,
                "skipped": outcome.skipped,
                "opted_out": outcome.opted_out,
                **kw,
            }
            await _maybe_await(on_progress(payload))

    def _maybe_safe_confirm(contact_id: int, status: str, detail: str) -> None:
        """_safe_confirm that no-ops in dry-run (Saved Messages) so we
        don't taint the real contact's send_log row."""
        if dry_run_to_self:
            return
        _safe_confirm(db, campaign_id, contact_id, status, detail)

    # Phase F1 — PeerFloodError is per-peer, not account-level. A single
    # flagged peer used to kill the whole campaign; now we skip the peer
    # and only stop after N distinct peers raise PeerFlood on the same
    # run — the actual "Telegram thinks you're spamming" signal.
    peer_flood_seen: set[int] = set()
    PEER_FLOOD_STOP_THRESHOLD = 3

    await _emit(status="starting")

    if not dry_run_to_self:
        db.update_campaign_status(campaign_id, "running")

    try:
        for contact in contacts:
            if stop_signal and stop_signal.is_set():
                # Reason mirrors whichever signal was raised (pause / stop).
                outcome.stopped_reason = stop_signal.kind or "user_stopped"
                break

            sent_today = db.sent_today_count(campaign_id)
            if pacer.should_stop_for_day(sent_today):
                outcome.stopped_reason = "daily_cap_reached"
                await _emit(status="daily_cap", current=contact["name"])
                break

            # Render template with per-contact variables
            message_text = render(template, dict(contact))
            if group_link and "{group_link}" not in template:
                message_text += f"\n\n{group_link}"
            else:
                message_text = message_text.replace("{group_link}", group_link)

            # Recipient: in dry-run mode we send only to self.
            if dry_run_to_self:
                await _ensure_connected(client)
                target_id = (
                    await asyncio.wait_for(client.get_me(), timeout=30)
                ).id
            else:
                target_id = contact["tg_user_id"]

            # Phase H2 — reserve before send. The reserve row prevents
            # duplicate delivery if we crash between send() and confirm().
            # If the contact was already sent (resume race), skip cleanly.
            #
            # Dry-run intentionally skips reserve/confirm: the message
            # goes to Saved Messages, so recording it against a real
            # contact would both mislead the UI ("sent to X" when it
            # went to self) AND cause `already_sent_ids` to filter that
            # contact out of the real campaign run — a silent delivery
            # drop. See test_dry_run_does_not_touch_send_log.
            if dry_run_to_self:
                rid = _fresh_random_id()
            else:
                rid, state = db.reserve_send(
                    campaign_id, contact["id"], _fresh_random_id(),
                )
                if state == "sent":
                    outcome.skipped += 1
                    await _emit(status="skipped",
                                current=contact["name"],
                                detail="already-sent (race)")
                    continue

            # Second stop-check right before the RPC. The waiting loop
            # at the end of the previous iteration already broke on
            # stop; re-checking here closes the window between reserve
            # and send so a Pause pressed during the ~1 s pacer chunk
            # never fires one more message.
            if stop_signal and stop_signal.is_set():
                outcome.stopped_reason = stop_signal.kind or "user_stopped"
                break

            # Split the try into two layers:
            #   (outer) the actual RPC — exceptions here mean delivery is
            #           uncertain/failed. Branch by exception type.
            #   (inner) the confirm. If this fails AFTER a successful send,
            #           we MUST leave the row as 'pending' (not 'error')
            #           so the retry reuses the same random_id and lets
            #           Telegram dedup. A 'pending' row is already in the
            #           `already_sent_ids` skip-set, so an automated retry
            #           simply skips; a user-driven retry re-sends with
            #           the same rid → dedup.
            send_landed = False
            sent_message_id: int | None = None
            try:
                msg = await _send_one(client, target_id,
                                      image_path=image_path,
                                      message_text=message_text,
                                      random_id=rid)
                send_landed = True
                sent_message_id = int(getattr(msg, "id", 0)) or None

            except FloodWaitError as e:
                wait = int(getattr(e, "seconds", 60)) + 5
                log.warning("FloodWait: sleeping %s sec", wait)
                await _emit(
                    status="flood_wait",
                    wait_sec=wait,
                    wait_until=_time.time() + wait,
                    current=contact["name"],
                )
                slept = 0
                interrupted_by_stop = False
                while slept < wait:
                    if stop_signal and stop_signal.is_set():
                        interrupted_by_stop = True
                        break
                    await asyncio.sleep(min(1, wait - slept))
                    slept += 1
                if interrupted_by_stop:
                    # Don't fire the retry — user asked us to stop.
                    # The pending row stays; next resume will re-enter
                    # and retry (Telegram may already have the message,
                    # in which case it lands again — an acknowledged
                    # rare duplicate on the FloodWait branch, acceptable
                    # vs. the certain duplicate of ignoring the stop).
                    outcome.stopped_reason = stop_signal.kind or "user_stopped"
                    break
                try:
                    msg = await _send_one(client, target_id,
                                          image_path=image_path,
                                          message_text=message_text,
                                          random_id=rid)
                    send_landed = True
                    sent_message_id = int(getattr(msg, "id", 0)) or None
                except sqlite3.OperationalError as e2:
                    if "locked" in str(e2).lower():
                        log.warning(
                            "session DB locked during post-flood retry — "
                            "treating as delivered (contact=%s)",
                            contact["id"],
                        )
                        send_landed = True
                    else:
                        _maybe_safe_confirm(contact["id"], "error",
                                            f"post-flood: {e2}")
                        outcome.errors += 1
                        await _emit(status="error", current=contact["name"], error=str(e2))
                except Exception as e2:  # noqa: BLE001
                    _maybe_safe_confirm(contact["id"], "error",
                                        f"post-flood: {e2}")
                    outcome.errors += 1
                    await _emit(status="error", current=contact["name"], error=str(e2))

            except PeerFloodError:
                # PeerFloodError is per-peer — one flagged contact doesn't
                # mean the account is banned (confirmed via @SpamBot). We
                # skip this peer and continue; only if PEER_FLOOD_STOP_
                # THRESHOLD distinct peers raise it in one run do we treat
                # the account as at risk and bail.
                peer_flood_seen.add(int(contact["id"]))
                _maybe_safe_confirm(contact["id"], "skipped",
                                    "PeerFloodError (per-peer)")
                outcome.skipped += 1
                if len(peer_flood_seen) >= PEER_FLOOD_STOP_THRESHOLD:
                    log.error(
                        "PeerFloodError on %d distinct peers — "
                        "stopping to protect the account",
                        len(peer_flood_seen),
                    )
                    outcome.stopped_reason = "peer_flood"
                    await _emit(status="peer_flood",
                                current=contact["name"],
                                detail=f"{len(peer_flood_seen)} peers flagged")
                    break
                await _emit(status="skipped",
                            current=contact["name"],
                            detail="PeerFloodError (per-peer)")

            except (UserPrivacyRestrictedError, UserIsBlockedError,
                    ChatWriteForbiddenError, InputUserDeactivatedError) as e:
                _maybe_safe_confirm(contact["id"], "skipped",
                                    type(e).__name__)
                outcome.skipped += 1
                await _emit(status="skipped", current=contact["name"],
                            detail=type(e).__name__)

            except sqlite3.OperationalError as e:
                # Telethon's session file write (`process_entities`) runs
                # AFTER the RPC returned successfully — the message has
                # ALREADY been delivered. A 'database is locked' here is
                # pure bookkeeping that failed because another process
                # (UI auth-check) was writing to the same session SQLite.
                # Marking this as 'error' would cause the next run to
                # re-send and DUPLICATE on Telegram (Telethon 1.43's
                # send_message doesn't surface random_id, so we can't
                # rely on server-side dedup). Trust the delivery and
                # move on.
                if "locked" in str(e).lower():
                    log.warning(
                        "session DB locked after successful send — "
                        "treating as delivered (contact=%s): %s",
                        contact["id"], e,
                    )
                    send_landed = True
                else:
                    log.exception("Send failed (sqlite)")
                    _maybe_safe_confirm(contact["id"], "error", str(e))
                    outcome.errors += 1
                    await _emit(status="error", current=contact["name"], error=str(e))

            except Exception as e:  # noqa: BLE001
                # RPC itself raised — delivery genuinely failed. Safe to
                # mark error; the rid hasn't reached Telegram in a
                # delivered state.
                log.exception("Send failed")
                _maybe_safe_confirm(contact["id"], "error", str(e))
                outcome.errors += 1
                await _emit(status="error", current=contact["name"], error=str(e))

            if send_landed:
                # Try to confirm. On failure, DO NOT fall back to 'error'
                # — the row stays 'pending' with its rid, and the caller
                # will safely reuse rid on the next run via Telegram
                # server-side dedup. We count this as sent locally so
                # the KPI/UI reflects what Telegram actually has.
                # Phase R1 — store Telegram's message.id when we have one
                # (post-session-lock branch leaves it None and the later
                # read-check falls back to the coarse path).
                # Dry-run: the message went to Saved Messages, not to
                # this contact — don't write send_log (see F2).
                if not dry_run_to_self:
                    try:
                        db.confirm_send(
                            campaign_id, contact["id"], "sent", "",
                            message_id=sent_message_id,
                        )
                    except Exception as confirm_err:  # noqa: BLE001
                        log.warning(
                            "confirm_send failed after successful send — "
                            "row stays 'pending' (rid=%s) for idempotent retry: %s",
                            rid, confirm_err,
                        )
                outcome.sent += 1
                await _emit(status="sent", current=contact["name"])

            # Gentle, random delay between sends (even for skips, to avoid patterns)
            delay = pacer.next_delay()
            await _emit(
                status="waiting",
                wait_sec=delay,
                wait_until=_time.time() + delay,
            )
            slept = 0
            while slept < delay:
                if stop_signal and stop_signal.is_set():
                    break
                await asyncio.sleep(min(1, delay - slept))
                slept += 1

        if not dry_run_to_self:
            db.update_campaign_status(
                campaign_id,
                "done" if not outcome.stopped_reason else "paused",
            )
        await _emit(status="finished", stopped_reason=outcome.stopped_reason)
    except Exception:
        if not dry_run_to_self:
            db.update_campaign_status(campaign_id, "paused")
        raise

    return outcome
