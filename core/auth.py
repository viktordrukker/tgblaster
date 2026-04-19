"""Thin wrappers around Telethon login flow.

The login happens in two steps:
1. `start_login` — sends code to the user's Telegram.
2. `complete_login` — the user pastes the code back; optionally password.

We keep the TelegramClient alive between calls via an in-process cache
keyed by session path. This is only safe because Streamlit runs
single-process.
"""
from __future__ import annotations

import asyncio
import sqlite3
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from telethon import TelegramClient
from telethon.errors import SessionPasswordNeededError
from telethon.sessions import SQLiteSession


class _PatientSession(SQLiteSession):
    """Telethon SQLite session that tolerates concurrent UI+worker access.

    The stock session opens SQLite with default settings: no busy_timeout
    and rollback-journal mode, so any second writer hits 'database is
    locked' immediately. We open with `timeout=30` (i.e. busy_timeout=30s)
    and switch the file to WAL so readers and writers don't block each
    other.
    """

    def _cursor(self):
        if self._conn is None:
            # `timeout=15` gives Telethon's post-send `process_entities`
            # write a generous window to land when the UI's auth-check
            # or login flow is holding the session file. An earlier
            # 3s bound was too short: under worker contention the post-
            # send bookkeeping raised 'database is locked', which our
            # sender then mis-categorized as delivery failure and the
            # next run DUPLICATED the message on Telegram (Telethon
            # 1.43's send_message auto-generates random_id, so server-
            # side dedup does not save us). 15s is still well under the
            # worker's 60s per-send RPC timeout, so it doesn't mask a
            # truly stuck send.
            self._conn = sqlite3.connect(
                self.filename,
                check_same_thread=False,
                timeout=15,
            )
            try:
                self._conn.execute("PRAGMA journal_mode=WAL")
                self._conn.execute("PRAGMA busy_timeout=15000")
            except sqlite3.Error:
                pass
        return self._conn.cursor()


_ClientKey = tuple[str, int, str]  # (session_path, api_id, api_hash)
_clients: dict[_ClientKey, TelegramClient] = {}
_pending_phones: dict[_ClientKey, tuple[str, object]] = {}
# key: (session_path_str, api_id, api_hash) -> (phone, sent_code_hash)

# Single persistent event loop for all Telethon work. A TelegramClient binds
# to the loop that first connects it; Streamlit spins up a fresh loop on each
# rerun, which otherwise triggers "asyncio event loop must not change".
_loop: Optional[asyncio.AbstractEventLoop] = None
_loop_lock = threading.Lock()


@dataclass
class LoginState:
    authorized: bool
    needs_code: bool = False
    needs_password: bool = False
    error: Optional[str] = None


def _client_key(session_path: Path | str, api_id: int, api_hash: str) -> _ClientKey:
    return (str(session_path), int(api_id), str(api_hash))


def get_client(session_path: Path | str, api_id: int, api_hash: str) -> TelegramClient:
    """Return (or create) the cached TelegramClient for these credentials.

    Cache key includes api_id/api_hash so rotating credentials on the same
    session path doesn't silently serve a client bound to stale auth. If a
    client already exists for this session path under *different* creds,
    we evict it first — the old one is unusable anyway.
    """
    key = _client_key(session_path, api_id, api_hash)
    if key not in _clients:
        # Evict any stale entry that shares the session path but had other
        # credentials cached previously.
        stale_keys = [k for k in _clients if k[0] == key[0] and k != key]
        for sk in stale_keys:
            try:
                client = _clients.pop(sk)
                if client.is_connected():
                    # Best-effort disconnect on our persistent loop.
                    try:
                        asyncio.run_coroutine_threadsafe(
                            client.disconnect(), _ensure_loop(),
                        ).result(timeout=5)
                    except Exception:
                        pass
            except Exception:
                pass
            _pending_phones.pop(sk, None)
        _clients[key] = TelegramClient(
            _PatientSession(str(session_path)), api_id, api_hash,
        )
    return _clients[key]


async def connect_client(client: TelegramClient, attempts: int = 3,
                          backoff_sec: float = 0.4) -> None:
    """Connect with retry on the recurring Telethon SQLiteSession
    'database is locked' under WAL checkpoint contention.

    Tuned for UI paths: 3 attempts × (0.4, 0.8, 1.2) s ≈ ~2.4 s worst case.
    Worker paths that can tolerate longer waits should call this with
    larger `attempts`/`backoff_sec`.
    """
    if client.is_connected():
        return
    last_err: Exception | None = None
    for attempt in range(1, attempts + 1):
        try:
            await client.connect()
            return
        except sqlite3.OperationalError as e:
            if "locked" not in str(e).lower():
                raise
            last_err = e
            await asyncio.sleep(backoff_sec * attempt)
    raise last_err or sqlite3.OperationalError("database is locked (exhausted retries)")


async def is_authorized(session_path: Path, api_id: int, api_hash: str) -> bool:
    client = get_client(session_path, api_id, api_hash)
    await connect_client(client)
    return await client.is_user_authorized()


async def start_login(session_path: Path, api_id: int, api_hash: str, phone: str) -> LoginState:
    client = get_client(session_path, api_id, api_hash)
    await connect_client(client)
    if await client.is_user_authorized():
        return LoginState(authorized=True)
    key = _client_key(session_path, api_id, api_hash)
    try:
        sent = await client.send_code_request(phone)
        _pending_phones[key] = (phone, sent.phone_code_hash)
        return LoginState(authorized=False, needs_code=True)
    except Exception as e:  # noqa: BLE001
        return LoginState(authorized=False, error=str(e))


async def complete_login(
    session_path: Path, api_id: int, api_hash: str,
    code: str, password: Optional[str] = None,
) -> LoginState:
    client = get_client(session_path, api_id, api_hash)
    await connect_client(client)
    key = _client_key(session_path, api_id, api_hash)
    pending = _pending_phones.get(key)
    if pending is None and not await client.is_user_authorized():
        return LoginState(authorized=False, error="Сначала запроси код (start_login)")
    phone, code_hash = pending if pending else ("", None)
    try:
        # Password takes priority: if the user is supplying one, the code
        # was already consumed on the previous submit that triggered
        # SessionPasswordNeededError — re-sending it would fail.
        if password:
            await client.sign_in(password=password)
        elif code and code_hash:
            await client.sign_in(phone=phone, code=code, phone_code_hash=code_hash)
        if not await client.is_user_authorized():
            return LoginState(authorized=False, error="Не авторизовался")
        _pending_phones.pop(key, None)
        return LoginState(authorized=True)
    except SessionPasswordNeededError:
        return LoginState(authorized=False, needs_password=True)
    except Exception as e:  # noqa: BLE001
        return LoginState(authorized=False, error=str(e))


async def logout(session_path: Path, api_id: int, api_hash: str) -> None:
    client = get_client(session_path, api_id, api_hash)
    if client.is_connected():
        try:
            await client.log_out()
        except Exception:  # noqa: BLE001
            pass
        await client.disconnect()
    key = _client_key(session_path, api_id, api_hash)
    _clients.pop(key, None)
    _pending_phones.pop(key, None)


def run_async(coro):
    """Run an async coroutine from sync code (Streamlit callbacks are sync).

    Always dispatches to the same background loop so a cached TelegramClient
    stays bound to one loop across Streamlit reruns.
    """
    loop = _ensure_loop()
    return asyncio.run_coroutine_threadsafe(coro, loop).result()


def _ensure_loop() -> asyncio.AbstractEventLoop:
    global _loop
    with _loop_lock:
        if _loop is not None and not _loop.is_closed():
            return _loop
        _loop = asyncio.new_event_loop()
        t = threading.Thread(
            target=_loop.run_forever,
            name="telethon-loop",
            daemon=True,
        )
        t.start()
        return _loop
