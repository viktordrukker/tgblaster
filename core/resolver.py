"""Resolve phone numbers → Telegram user IDs via MTProto ImportContactsRequest.

Batches phones 100 at a time (Telegram's soft limit), applies a delay
between batches, and handles FloodWaitError gracefully.

Returns (resolved_count, not_on_tg_count, error_count).
"""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime
from typing import Awaitable, Callable, Iterable

from telethon import TelegramClient
from telethon.errors import FloodWaitError, UsernameNotOccupiedError, UsernameInvalidError
from telethon.tl.functions.contacts import ImportContactsRequest, DeleteContactsRequest
from telethon.tl.types import InputPhoneContact

from .database import Database


log = logging.getLogger(__name__)

BATCH_SIZE = 100
INTER_BATCH_SLEEP_SEC = 3
EVENTS_RING = 80  # keep last N log lines in progress payload


ProgressCb = Callable[[dict], Awaitable[None] | None]
"""Callback(payload_dict) -> optional awaitable.

Payload schema:
  done / total                — overall progress counts
  current                     — phone being processed
  resolved / not_on_telegram / errors — running counters
  events: list[str]           — last EVENTS_RING human log lines
"""


async def _maybe_await(x):
    if asyncio.iscoroutine(x):
        await x


async def resolve_pending(
    client: TelegramClient,
    db: Database,
    on_progress: ProgressCb | None = None,
    cleanup_imported: bool = True,
    ids: list[int] | None = None,
) -> dict:
    """Resolve pending contacts. If `ids` is given, scope to that subset
    (used by the inline 'resolve selected rows' action)."""
    pending = db.pending_resolve(ids=ids)
    total = len(pending)
    stats = {"total": total, "resolved": 0, "not_on_telegram": 0, "errors": 0}

    events: list[str] = []

    def _log(msg: str) -> None:
        stamp = datetime.now().strftime("%H:%M:%S")
        events.append(f"{stamp}  {msg}")
        if len(events) > EVENTS_RING:
            del events[:-EVENTS_RING]

    async def _emit(done: int, current: str = "") -> None:
        if on_progress:
            await _maybe_await(on_progress({
                "done": done,
                "total": total,
                "current": current,
                "resolved": stats["resolved"],
                "not_on_telegram": stats["not_on_telegram"],
                "errors": stats["errors"],
                "events": list(events),
            }))

    if total == 0:
        _log("Нечего ресолвить — очередь пуста.")
        await _emit(0)
        return stats

    _log(f"Старт. В очереди: {total} контактов, батчи по {BATCH_SIZE}.")
    await _emit(0)

    imported_user_ids: list[int] = []
    done = 0

    for batch_start in range(0, total, BATCH_SIZE):
        batch = pending[batch_start: batch_start + BATCH_SIZE]
        batch_num = batch_start // BATCH_SIZE + 1
        _log(f"Батч #{batch_num}: отправляю {len(batch)} номеров в ImportContacts…")
        await _emit(done)

        input_contacts = [
            InputPhoneContact(
                client_id=row["id"],
                phone=row["phone"],
                first_name=(row["name"] or f"contact_{row['id']}")[:64],
                last_name="",
            )
            for row in batch
        ]
        row_by_client_id = {row["id"]: row for row in batch}

        try:
            result = await client(ImportContactsRequest(contacts=input_contacts))
        except FloodWaitError as e:
            wait = int(getattr(e, "seconds", 30)) + 5
            _log(f"⏱ FloodWait — жду {wait} сек…")
            await _emit(done)
            await asyncio.sleep(wait)
            try:
                result = await client(ImportContactsRequest(contacts=input_contacts))
                _log("✓ Ретрай после FloodWait прошёл.")
            except Exception as e2:  # pragma: no cover
                _log(f"✗ Батч #{batch_num} упал после ретрая: {e2}")
                for row in batch:
                    db.mark_resolve_error(row["id"], f"retry failed: {e2}")
                stats["errors"] += len(batch)
                done += len(batch)
                await _emit(done)
                continue
        except Exception as e:  # noqa: BLE001
            log.exception("Import batch failed")
            _log(f"✗ Батч #{batch_num} упал: {e}")
            for row in batch:
                db.mark_resolve_error(row["id"], str(e))
            stats["errors"] += len(batch)
            done += len(batch)
            await _emit(done)
            continue

        users_by_id = {u.id: u for u in getattr(result, "users", [])}

        matched_client_ids: set[int] = set()
        for imp in getattr(result, "imported", []):
            user = users_by_id.get(imp.user_id)
            client_id = getattr(imp, "client_id", None)
            if client_id is None:
                continue
            row = row_by_client_id.get(client_id)
            if not row:
                continue
            matched_client_ids.add(client_id)
            username = getattr(user, "username", None) if user else None
            access_hash = getattr(user, "access_hash", None) if user else None
            db.mark_resolved(row["id"], imp.user_id, username, access_hash)
            imported_user_ids.append(imp.user_id)
            stats["resolved"] += 1

        retry_ids = {c for c in getattr(result, "retry_contacts", [])}
        for row in batch:
            cid = row["id"]
            phone = row["phone"]
            if cid in matched_client_ids:
                uname = next(
                    (getattr(users_by_id.get(imp.user_id), "username", None)
                     for imp in getattr(result, "imported", [])
                     if getattr(imp, "client_id", None) == cid),
                    None,
                )
                _log(f"✓ {phone} → найден {'@' + uname if uname else '(без username)'}")
            elif cid in retry_ids:
                db.mark_resolve_error(cid, "rate-limited, retry later")
                stats["errors"] += 1
                _log(f"! {phone}: rate-limited, попробуй позже")
            else:
                db.mark_not_on_tg(cid)
                stats["not_on_telegram"] += 1
                _log(f"✗ {phone}: нет в Telegram")

            done += 1
            await _emit(done, current=phone)

        await asyncio.sleep(INTER_BATCH_SLEEP_SEC)

    if cleanup_imported and imported_user_ids:
        _log(f"Чищу {len(imported_user_ids)} импортированных контактов…")
        await _emit(done)
        try:
            entities = []
            for uid in imported_user_ids:
                try:
                    entities.append(await client.get_input_entity(uid))
                except Exception:  # noqa: BLE001
                    continue
            if entities:
                await client(DeleteContactsRequest(id=entities))
            _log("✓ Контакты удалены из Telegram.")
        except Exception as e:  # noqa: BLE001
            log.warning("Cleanup of imported contacts failed: %s", e)
            _log(f"! Cleanup упал: {e}")

    _log(
        f"Готово. resolved={stats['resolved']} / "
        f"not_on_tg={stats['not_on_telegram']} / errors={stats['errors']}"
    )
    await _emit(done)
    return stats


async def resolve_one_phone(
    client: TelegramClient,
    db: Database,
    contact_id: int,
    phone: str,
    name: str = "",
    cleanup_imported: bool = True,
) -> dict:
    """Resolve a single phone immediately and update the DB row.

    Returns a dict describing the outcome:
      {"status": "resolved" | "not_on_telegram" | "error",
       "tg_user_id": int|None, "username": str|None, "error": str|None}
    """
    input_contact = InputPhoneContact(
        client_id=contact_id,
        phone=phone,
        first_name=(name or f"contact_{contact_id}")[:64],
        last_name="",
    )

    try:
        result = await client(ImportContactsRequest(contacts=[input_contact]))
    except FloodWaitError as e:
        wait = int(getattr(e, "seconds", 30))
        db.mark_resolve_error(contact_id, f"flood_wait: wait {wait}s")
        return {"status": "error", "tg_user_id": None, "username": None,
                "error": f"FloodWait: wait {wait}s"}
    except Exception as e:  # noqa: BLE001
        db.mark_resolve_error(contact_id, str(e))
        return {"status": "error", "tg_user_id": None, "username": None, "error": str(e)}

    users_by_id = {u.id: u for u in getattr(result, "users", [])}
    imported = list(getattr(result, "imported", []))

    if not imported:
        db.mark_not_on_tg(contact_id)
        return {"status": "not_on_telegram", "tg_user_id": None,
                "username": None, "error": None}

    imp = imported[0]
    user = users_by_id.get(imp.user_id)
    username = getattr(user, "username", None) if user else None
    access_hash = getattr(user, "access_hash", None) if user else None
    first_name = getattr(user, "first_name", None) if user else None
    last_name = getattr(user, "last_name", None) if user else None
    db.mark_resolved(contact_id, imp.user_id, username, access_hash)

    if cleanup_imported:
        try:
            entity = await client.get_input_entity(imp.user_id)
            await client(DeleteContactsRequest(id=[entity]))
        except Exception as e:  # noqa: BLE001
            log.warning("Cleanup of imported contact %s failed: %s", phone, e)

    return {"status": "resolved", "tg_user_id": imp.user_id,
            "username": username,
            "tg_first_name": first_name, "tg_last_name": last_name,
            "error": None}


async def resolve_one_username(
    client: TelegramClient,
    db: Database,
    username: str,
    existing_contact_id: int | None = None,
) -> dict:
    """Resolve a single @username. Inserts a synthetic-phone contact if
    needed and fills in name from the TG profile.

    Returns a dict:
      {"status": "resolved" | "not_on_telegram" | "error",
       "contact_id": int, "tg_user_id": int|None,
       "username": str|None, "tg_first_name": str|None,
       "tg_last_name": str|None, "error": str|None}
    """
    hint = (username or "").lstrip("@").strip()
    out = {"status": "error", "contact_id": existing_contact_id,
           "tg_user_id": None, "username": None,
           "tg_first_name": None, "tg_last_name": None, "error": None}

    try:
        entity = await client.get_entity(hint)
    except (UsernameNotOccupiedError, UsernameInvalidError) as e:
        out["status"] = "not_on_telegram"
        out["error"] = str(e)
        return out
    except Exception as e:  # noqa: BLE001
        out["error"] = str(e)
        return out

    user_id = getattr(entity, "id", None)
    if not user_id:
        out["error"] = "empty entity"
        return out

    tg_username = getattr(entity, "username", None) or hint
    access_hash = getattr(entity, "access_hash", None)
    first_name = getattr(entity, "first_name", None) or ""
    last_name = getattr(entity, "last_name", None) or ""

    # If we weren't handed an existing contact, create one with a synthetic
    # phone so the row can live alongside real-phone contacts.
    if existing_contact_id is None:
        synth = f"tg:{tg_username.lower()}"
        display_name = (first_name + " " + last_name).strip()
        db.upsert_contacts([{
            "phone": synth, "name": display_name or tg_username,
            "raw_phone": f"@{hint}", "tg_username_hint": tg_username,
        }])
        match = db.all_contacts_df().query("phone == @synth")
        if match.empty:
            out["error"] = "insert failed"
            return out
        existing_contact_id = int(match.iloc[0]["id"])

    db.mark_resolved(existing_contact_id, int(user_id), tg_username, access_hash)

    out.update({
        "status": "resolved",
        "contact_id": existing_contact_id,
        "tg_user_id": int(user_id),
        "username": tg_username,
        "tg_first_name": first_name or None,
        "tg_last_name": last_name or None,
    })
    return out


async def validate_pending_usernames(
    client: TelegramClient,
    db: Database,
    on_progress: ProgressCb | None = None,
    inter_call_sleep_sec: float = 0.4,
    ids: list[int] | None = None,
) -> dict:
    """Resolve every pending contact that has a stored `tg_username_hint`.
    If `ids` is given, scope to that subset.

    Emits dict-payload progress identical in shape to `resolve_pending`
    (done/total/current/resolved/not_on_telegram/errors/events), so the UI
    can render the same progress-bar + log view for both.
    """
    rows = db.pending_username_validations(ids=ids)
    total = len(rows)
    stats = {"total": total, "resolved": 0, "not_on_telegram": 0, "errors": 0}

    events: list[str] = []

    def _log(msg: str) -> None:
        stamp = datetime.now().strftime("%H:%M:%S")
        events.append(f"{stamp}  {msg}")
        if len(events) > EVENTS_RING:
            del events[:-EVENTS_RING]

    async def _emit(done: int, current: str = "") -> None:
        if on_progress:
            await _maybe_await(on_progress({
                "done": done,
                "total": total,
                "current": current,
                "resolved": stats["resolved"],
                "not_on_telegram": stats["not_on_telegram"],
                "errors": stats["errors"],
                "events": list(events),
            }))

    if total == 0:
        _log("Нечего валидировать — все @usernames уже обработаны.")
        await _emit(0)
        return stats

    _log(f"Старт валидации: {total} @username, пауза {inter_call_sleep_sec} сек между запросами.")
    await _emit(0)

    for idx, row in enumerate(rows, start=1):
        hint = row["tg_username_hint"]
        try:
            entity = await client.get_entity(hint)
        except (UsernameNotOccupiedError, UsernameInvalidError):
            db.mark_resolve_error(row["id"], f"username @{hint} not found")
            stats["not_on_telegram"] += 1
            _log(f"✗ @{hint}: не найден в Telegram")
        except FloodWaitError as e:
            wait = int(getattr(e, "seconds", 30)) + 5
            _log(f"⏱ FloodWait на @{hint} — жду {wait} сек…")
            await _emit(idx, current=hint)
            await asyncio.sleep(wait)
            try:
                entity = await client.get_entity(hint)
            except Exception as e2:  # noqa: BLE001
                db.mark_resolve_error(row["id"], f"retry failed: {e2}")
                stats["errors"] += 1
                _log(f"✗ @{hint}: ретрай после FloodWait упал: {e2}")
                await _emit(idx, current=hint)
                continue
            user_id = getattr(entity, "id", None)
            username = getattr(entity, "username", None) or hint
            access_hash = getattr(entity, "access_hash", None)
            if user_id:
                db.mark_resolved(row["id"], int(user_id), username, access_hash)
                stats["resolved"] += 1
                _log(f"✓ @{hint} → user_id={user_id}")
            else:
                db.mark_resolve_error(row["id"], "empty entity after flood wait")
                stats["errors"] += 1
                _log(f"! @{hint}: пустая сущность после ретрая")
        except Exception as e:  # noqa: BLE001
            log.warning("validate_username(%s) failed: %s", hint, e)
            db.mark_resolve_error(row["id"], str(e))
            stats["errors"] += 1
            _log(f"! @{hint}: {e}")
        else:
            user_id = getattr(entity, "id", None)
            username = getattr(entity, "username", None) or hint
            access_hash = getattr(entity, "access_hash", None)
            if user_id:
                db.mark_resolved(row["id"], int(user_id), username, access_hash)
                stats["resolved"] += 1
                _log(f"✓ @{hint} → user_id={user_id}")
            else:
                db.mark_resolve_error(row["id"], "username lookup returned no id")
                stats["errors"] += 1
                _log(f"! @{hint}: lookup вернул пусто")

        await _emit(idx, current=hint)
        await asyncio.sleep(inter_call_sleep_sec)

    _log(
        f"Готово. resolved={stats['resolved']} / "
        f"not_found={stats['not_on_telegram']} / errors={stats['errors']}"
    )
    await _emit(total)
    return stats
