"""Telethon glue: client, login, phone/username → Telegram user resolution."""
from __future__ import annotations

import asyncio
import json
import logging
import random
import re
from contextlib import asynccontextmanager
from typing import Callable

from telethon import TelegramClient, errors
from telethon.tl import functions, types

from .config import Settings
from .db import Database

log = logging.getLogger("blaster.telegram")

IMPORT_BATCH = 50
IMPORT_PAUSE = (6, 12)          # seconds between ImportContacts batches
USERNAME_PAUSE = (2.5, 5.0)     # seconds between ResolveUsername calls


def make_client(settings: Settings) -> TelegramClient:
    settings.require_api()
    settings.session_path.parent.mkdir(parents=True, exist_ok=True)
    return TelegramClient(str(settings.session_path), settings.api_id, settings.api_hash,
                          connection_retries=10, retry_delay=3, request_retries=3,
                          flood_sleep_threshold=0)


@asynccontextmanager
async def connected(settings: Settings, db: Database):
    """A connected client that holds the run lock: exactly one process may
    drive the session — two clients on one auth key can get it revoked.
    Raises db.LockHeld if a job (resolve/send) is using it."""
    db.acquire_lock()
    try:
        client = make_client(settings)
        await client.connect()
        try:
            yield client
        finally:
            if client.is_connected():          # log_out() already disconnected
                await client.disconnect()
    finally:
        db.release_lock()


# ---- step-by-step login (web UI) -------------------------------------------

async def send_code(client, phone: str) -> str:
    """Ask Telegram to send a login code. Returns phone_code_hash."""
    sent = await client.send_code_request("+" + _digits(phone))
    return sent.phone_code_hash


async def sign_in_code(client, phone: str, code: str, phone_code_hash: str):
    """The user, or None when the account has a 2FA password."""
    try:
        return await client.sign_in(phone="+" + _digits(phone), code=code.strip(),
                                    phone_code_hash=phone_code_hash)
    except errors.SessionPasswordNeededError:
        return None


async def sign_in_password(client, password: str):
    return await client.sign_in(password=password)


_EXPLAIN = {
    errors.PhoneNumberInvalidError: "Неверный номер телефона.",
    errors.PhoneNumberBannedError: "Этот номер заблокирован в Telegram.",
    errors.PhoneCodeInvalidError: "Неверный код — проверь и введи ещё раз.",
    errors.PhoneCodeExpiredError: "Код устарел — запроси новый.",
    errors.PasswordHashInvalidError: "Неверный пароль 2FA.",
    errors.ApiIdInvalidError: "TG_API_ID / TG_API_HASH неверные (.env).",
}


def explain(e: Exception) -> str:
    if isinstance(e, errors.FloodWaitError):
        return f"Telegram просит подождать {max(1, e.seconds // 60)} мин перед новой попыткой."
    for cls, text in _EXPLAIN.items():
        if isinstance(e, cls):
            return text
    return f"{type(e).__name__}: {e}"


# ---- who is logged in (cached: the UI can't connect while a job runs) -------

def _account_path(settings: Settings):
    return settings.db_path.parent / "account.json"


def save_account(settings: Settings, me) -> None:
    p = _account_path(settings)
    if me is None:
        p.unlink(missing_ok=True)
        return
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps({"id": me.id, "first_name": me.first_name or "",
                             "last_name": me.last_name or "", "username": me.username or "",
                             "phone": me.phone or ""}, ensure_ascii=False), encoding="utf-8")


def load_account(settings: Settings) -> dict | None:
    try:
        return json.loads(_account_path(settings).read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError):
        return None


class NotAuthorized(SystemExit):
    pass


async def ensure_authorized(client: TelegramClient) -> types.User:
    if not client.is_connected():
        await client.connect()
    if not await client.is_user_authorized():
        raise NotAuthorized("Сессия не авторизована. Выполни: blaster login")
    return await client.get_me()


async def interactive_login(client: TelegramClient) -> types.User:
    await client.start(
        phone=lambda: input("Телефон аккаунта-отправителя (+998…): ").strip(),
        code_callback=lambda: input("Код из Telegram: ").strip(),
        password=lambda: __import__("getpass").getpass("Пароль 2FA (если есть): "),
    )
    return await client.get_me()


def _digits(p: str | None) -> str:
    return re.sub(r"\D", "", p or "")


async def _sleep(stop: asyncio.Event | None, seconds: float,
                 heartbeat: Callable[[], None] | None = None) -> bool:
    """Interruptible sleep that keeps the run lock alive. True if interrupted."""
    stop = stop or asyncio.Event()
    loop = asyncio.get_running_loop()
    end = loop.time() + seconds
    while True:
        left = end - loop.time()
        if left <= 0:
            return stop.is_set()
        try:
            await asyncio.wait_for(stop.wait(), timeout=min(left, 30))
            return True
        except asyncio.TimeoutError:
            if heartbeat:
                heartbeat()


def _is_person(u) -> bool:
    return isinstance(u, types.User) and not u.bot and not u.deleted and not getattr(u, "is_self", False)


async def resolve_list(
    client,
    db: Database,
    list_name: str,
    *,
    retry_errors: bool = False,
    keep_contacts: bool = False,
    max_flood_wait: int = 1800,
    stop: asyncio.Event | None = None,
    out: Callable[[str], None] = print,
    pauses: tuple[tuple[float, float], tuple[float, float]] = (IMPORT_PAUSE, USERNAME_PAUSE),
) -> dict:
    """Resolve pending contacts of a list. Safe to re-run: only 'new'
    (and optionally 'error') rows are processed."""
    import_pause, user_pause = pauses
    rows = db.to_resolve(list_name, retry_errors=retry_errors)
    stats = {"total": len(rows), "resolved": 0, "not_found": 0, "errors": 0, "stopped": ""}
    if not rows:
        out("Нечего резолвить — все контакты списка уже обработаны.")
        return stats

    # Contacts already in the sender's address book: resolve them locally
    # and never touch (rename/delete) them.
    book = await client(functions.contacts.GetContactsRequest(hash=0))
    book_users = [u for u in getattr(book, "users", []) if isinstance(u, types.User)]
    preexisting_ids = {u.id for u in book_users}
    book_by_phone = {_digits(u.phone): u for u in book_users if u.phone}
    imported_new: dict[int, types.User] = {}

    unresolved: dict[int, dict] = {r["id"]: r for r in rows}

    def done(cid: int, user, via: str) -> None:
        db.mark_resolved(cid, user.id, getattr(user, "access_hash", None),
                         getattr(user, "username", None), getattr(user, "first_name", None), via)
        unresolved.pop(cid, None)
        stats["resolved"] += 1

    # ---- 0. address book ------------------------------------------------
    for r in list(unresolved.values()):
        for p in r["phones"]:
            u = book_by_phone.get(_digits(p))
            if u is not None and _is_person(u):
                done(r["id"], u, "address_book")
                break

    # ---- 1. phones via ImportContacts, primary then alternates -----------
    max_phones = max((len(r["phones"]) for r in unresolved.values()), default=0)
    for phone_idx in range(max_phones):
        queue = [(r["id"], r["phones"][phone_idx], r) for r in unresolved.values()
                 if len(r["phones"]) > phone_idx]
        i = 0
        while i < len(queue):
            if stop and stop.is_set():
                stats["stopped"] = "stopped"
                break
            batch = queue[i:i + IMPORT_BATCH]
            by_client_id = {}
            inputs = []
            for cid, phone, r in batch:
                client_id = random.getrandbits(62)
                by_client_id[client_id] = (cid, phone)
                inputs.append(types.InputPhoneContact(
                    client_id=client_id, phone=phone,
                    first_name=(r["name"] or r["first_name"] or "Contact")[:60] or "Contact",
                    last_name=""))
            try:
                res = await client(functions.contacts.ImportContactsRequest(contacts=inputs))
            except errors.FloodWaitError as e:
                if e.seconds > max_flood_wait:
                    out(f"⏱ FloodWait {e.seconds}s на ImportContacts — останавливаюсь, "
                        f"повтори resolve позже (обработанное сохранено).")
                    stats["stopped"] = f"flood_wait_{e.seconds}s"
                    break
                out(f"⏱ FloodWait {e.seconds}s — жду…")
                if await _sleep(stop, e.seconds + 5, db.heartbeat):
                    stats["stopped"] = "stopped"
                    break
                continue  # retry same batch
            except errors.RPCError as e:
                out(f"✗ ImportContacts: {e}")
                for cid, _p, _r in batch:
                    db.mark_resolve_error(cid, f"ImportContacts: {e}")
                    unresolved.pop(cid, None)
                    stats["errors"] += 1
                i += IMPORT_BATCH
                continue

            users = {u.id: u for u in getattr(res, "users", [])}
            for imp in getattr(res, "imported", []):
                cid, phone = by_client_id.get(imp.client_id, (None, None))
                u = users.get(imp.user_id)
                if cid is None or u is None or cid not in unresolved:
                    continue
                if u.id not in preexisting_ids:
                    imported_new[u.id] = u
                if _is_person(u):
                    done(cid, u, f"phone:{phone}")
            retry = set(getattr(res, "retry_contacts", []) or [])
            if retry:
                out(f"! Telegram попросил повторить {len(retry)} номеров позже (лимит импорта).")
                for client_id in retry:
                    cid, _ = by_client_id.get(client_id, (None, None))
                    if cid in unresolved:
                        db.mark_resolve_error(cid, "import rate-limited (retry_contacts)")
                        unresolved.pop(cid)
                        stats["errors"] += 1
            db.heartbeat()
            out(f"  батч номеров {i // IMPORT_BATCH + 1}: найдено всего {stats['resolved']}")
            i += IMPORT_BATCH
            if i < len(queue) and await _sleep(stop, random.uniform(*import_pause), db.heartbeat):
                stats["stopped"] = "stopped"
                break
        if stats["stopped"]:
            break

    # ---- 2. usernames ---------------------------------------------------
    if not stats["stopped"]:
        for r in [r for r in unresolved.values() if r.get("username")]:
            if stop and stop.is_set():
                stats["stopped"] = "stopped"
                break
            uname = r["username"]
            try:
                res = await client(functions.contacts.ResolveUsernameRequest(username=uname))
                users = [u for u in getattr(res, "users", []) if _is_person(u)]
                if users:
                    done(r["id"], users[0], f"username:@{uname}")
                    out(f"  ✓ @{uname}")
                else:
                    db.mark_not_found(r["id"], f"@{uname} — не пользователь (канал/бот)")
                    unresolved.pop(r["id"])
                    stats["not_found"] += 1
            except (errors.UsernameNotOccupiedError, errors.UsernameInvalidError):
                db.mark_not_found(r["id"], f"@{uname} не существует")
                unresolved.pop(r["id"])
                stats["not_found"] += 1
                out(f"  ✗ @{uname} не найден")
            except errors.FloodWaitError as e:
                out(f"⏱ FloodWait {e.seconds}s на ResolveUsername — останавливаюсь, повтори позже.")
                stats["stopped"] = f"flood_wait_{e.seconds}s"
                break
            except errors.RPCError as e:
                db.mark_resolve_error(r["id"], f"ResolveUsername: {e}")
                unresolved.pop(r["id"])
                stats["errors"] += 1
            if await _sleep(stop, random.uniform(*user_pause), db.heartbeat):
                stats["stopped"] = "stopped"
                break

    # ---- 3. whatever is left and was fully tried → not_found -------------
    if not stats["stopped"]:
        for r in list(unresolved.values()):
            how = ", ".join(r["phones"] + ([f"@{r['username']}"] if r.get("username") else []))
            db.mark_not_found(r["id"], f"нет в Telegram или скрыт настройками приватности ({how})")
            stats["not_found"] += 1
        unresolved.clear()

    # ---- 4. cleanup: remove only contacts WE added -----------------------
    if imported_new and not keep_contacts:
        ids = [types.InputUser(u.id, u.access_hash or 0) for u in imported_new.values()]
        for j in range(0, len(ids), 100):
            try:
                await client(functions.contacts.DeleteContactsRequest(id=ids[j:j + 100]))
            except errors.RPCError as e:  # noqa: PERF203
                out(f"! не смог удалить временные контакты: {e}")
        out(f"  удалил {len(ids)} временно импортированных контактов из адресной книги")
    return stats
