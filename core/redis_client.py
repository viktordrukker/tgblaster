"""Redis connection helper, with graceful fallback when Redis isn't running.

The whole system is designed to work in two modes:

1. **Docker / queued mode** — Redis reachable at REDIS_URL. Jobs go through
   arq, statuses persist across restarts, locks prevent duplicate runs.
2. **Standalone mode** — no Redis. Jobs run in-process threads. Still writes
   status to the SQLite `jobs` table so the UI is identical.

`is_redis_available()` is the switch the rest of the app flips on.
"""
from __future__ import annotations

import os
import logging
from functools import lru_cache
from typing import Optional

import redis


log = logging.getLogger(__name__)


def redis_url() -> str:
    return os.getenv("REDIS_URL", "redis://localhost:6379/0")


@lru_cache(maxsize=1)
def get_client() -> Optional[redis.Redis]:
    """Return a connected Redis client, or None if Redis isn't reachable.

    Cached so repeated calls don't keep trying to connect.
    """
    url = redis_url()
    try:
        client = redis.Redis.from_url(url, socket_connect_timeout=1, socket_timeout=2)
        client.ping()
        log.info("Connected to Redis at %s", url)
        return client
    except Exception as e:  # noqa: BLE001
        log.info("Redis not reachable at %s (%s) — falling back to in-process mode", url, e)
        return None


def reset_cache() -> None:
    """Useful in tests and after a config change."""
    get_client.cache_clear()


def is_redis_available() -> bool:
    return get_client() is not None
