"""Distributed locks.

Two implementations, chosen at call time based on Redis availability:

* **RedisLock** — Redis SET NX EX. Safe across multiple containers.
* **LocalLock** — filelock on SQLite, safe within one process tree.

Both expose the same `try_acquire` / `release` interface so the caller
doesn't care.
"""
from __future__ import annotations

import os
import time
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from . import redis_client


_LOCAL_LOCKS: dict[str, threading.Lock] = {}
_LOCAL_LOCKS_LOCK = threading.Lock()


@dataclass
class LockHandle:
    key: str
    token: str
    owner: str  # "redis" or "local"


def _make_token() -> str:
    return f"{os.getpid()}-{time.time_ns()}"


def try_acquire(key: str, ttl_sec: int = 3600,
                backend: Optional[str] = None) -> Optional[LockHandle]:
    """Try to acquire a lock named `key`. Returns a handle or None.

    Phase H4 — `backend` pins which lock implementation to use:
      * ``"arq"``    → Redis lock only. If Redis is unavailable, fail.
      * ``"thread"`` → local threading lock only. Never consult Redis.
      * ``None``     → legacy behavior (Redis if available, else local).

    Pinning eliminates the race where a job was enqueued under one
    backend (Redis down → thread) and later executed under the other
    (Redis came back → arq worker grabs the Redis lock; the still-running
    thread holds an unrelated local lock → both run).
    """
    if backend == "thread":
        with _LOCAL_LOCKS_LOCK:
            lock = _LOCAL_LOCKS.setdefault(key, threading.Lock())
        acquired = lock.acquire(blocking=False)
        if acquired:
            return LockHandle(key=key, token="local", owner="local")
        return None

    if backend == "arq":
        client = redis_client.get_client()
        if client is None:
            return None
        token = _make_token()
        got = client.set(name=f"lock:{key}", value=token, nx=True, ex=ttl_sec)
        if got:
            return LockHandle(key=key, token=token, owner="redis")
        return None

    # Legacy path — backend unspecified, pick dynamically.
    client = redis_client.get_client()
    if client is not None:
        token = _make_token()
        got = client.set(name=f"lock:{key}", value=token, nx=True, ex=ttl_sec)
        if got:
            return LockHandle(key=key, token=token, owner="redis")
        return None

    with _LOCAL_LOCKS_LOCK:
        lock = _LOCAL_LOCKS.setdefault(key, threading.Lock())
    acquired = lock.acquire(blocking=False)
    if acquired:
        return LockHandle(key=key, token="local", owner="local")
    return None


def extend(handle: Optional[LockHandle], ttl_sec: int) -> bool:
    """Push a held lock's expiry forward. No-op on local locks (their
    lifetime is the process, which is exactly what we want).

    Returns True if the extension succeeded, False otherwise. A False
    return means we lost the lock and should treat the job as
    preempted — but the caller is free to ignore it and keep going
    (the next tick's extend will re-establish or confirm loss)."""
    if handle is None:
        return False
    if handle.owner != "redis":
        return True  # local locks don't expire; nothing to do
    client = redis_client.get_client()
    if client is None:
        return False
    # Only extend if we still hold the token — prevents stealing
    # a lock that has rolled over to another holder.
    script = """
    if redis.call('get', KEYS[1]) == ARGV[1] then
        return redis.call('expire', KEYS[1], ARGV[2])
    else
        return 0
    end
    """
    try:
        result = client.eval(script, 1, f"lock:{handle.key}",
                             handle.token, int(ttl_sec))
        return bool(int(result))
    except Exception:  # noqa: BLE001
        return False


def release(handle: LockHandle) -> None:
    """Release a previously-acquired lock."""
    if handle.owner == "redis":
        client = redis_client.get_client()
        if client is None:
            return
        # Only delete if we still hold it — avoids stealing somebody else's lock
        # that took over after TTL.
        script = """
        if redis.call('get', KEYS[1]) == ARGV[1] then
            return redis.call('del', KEYS[1])
        else
            return 0
        end
        """
        try:
            client.eval(script, 1, f"lock:{handle.key}", handle.token)
        except Exception:  # noqa: BLE001
            pass
    else:
        with _LOCAL_LOCKS_LOCK:
            lock = _LOCAL_LOCKS.get(handle.key)
        if lock is not None:
            try:
                lock.release()
            except RuntimeError:
                pass


class held:
    """Context manager sugar: `with held("campaign:5") as h:`.

    Yields the handle or None. Always safe to `release`.
    """
    def __init__(self, key: str, ttl_sec: int = 3600):
        self.key = key
        self.ttl_sec = ttl_sec
        self.handle: Optional[LockHandle] = None

    def __enter__(self):
        self.handle = try_acquire(self.key, self.ttl_sec)
        return self.handle

    def __exit__(self, *exc):
        if self.handle is not None:
            release(self.handle)
