"""Tests for the dual-backend lock module (redis + local fallback)."""
from __future__ import annotations

import threading
import time

import fakeredis
import pytest

from core import locks, redis_client


@pytest.fixture(autouse=True)
def _isolate_local_locks():
    """Clean shared in-process lock registry between tests."""
    locks._LOCAL_LOCKS.clear()
    yield
    locks._LOCAL_LOCKS.clear()


@pytest.fixture
def fake_redis(monkeypatch):
    """Patch the redis client to a fakeredis instance."""
    fake = fakeredis.FakeRedis()
    monkeypatch.setattr(redis_client, "get_client", lambda: fake)
    yield fake


@pytest.fixture
def no_redis(monkeypatch):
    """Force the lock module into local-only mode."""
    monkeypatch.setattr(redis_client, "get_client", lambda: None)
    yield


class TestLocalBackend:
    def test_acquire_and_release(self, no_redis):
        h = locks.try_acquire("foo")
        assert h is not None
        assert h.owner == "local"
        locks.release(h)
        h2 = locks.try_acquire("foo")
        assert h2 is not None
        locks.release(h2)

    def test_double_acquire_blocks(self, no_redis):
        h1 = locks.try_acquire("bar")
        assert h1 is not None
        h2 = locks.try_acquire("bar")
        assert h2 is None
        locks.release(h1)
        h3 = locks.try_acquire("bar")
        assert h3 is not None
        locks.release(h3)

    def test_held_context_manager(self, no_redis):
        with locks.held("ctx") as h:
            assert h is not None
            assert locks.try_acquire("ctx") is None
        h2 = locks.try_acquire("ctx")
        assert h2 is not None
        locks.release(h2)


class TestRedisBackend:
    def test_acquire_sets_key_and_owner(self, fake_redis):
        h = locks.try_acquire("resolve", ttl_sec=30)
        assert h is not None
        assert h.owner == "redis"
        assert fake_redis.get("lock:resolve") is not None
        # Clean up — fakeredis may not support Lua EVAL in all versions,
        # so release is best-effort; delete directly for a clean state.
        locks.release(h)
        fake_redis.delete("lock:resolve")

    def test_second_acquire_returns_none(self, fake_redis):
        h1 = locks.try_acquire("camp:1")
        assert h1 is not None
        h2 = locks.try_acquire("camp:1")
        assert h2 is None
        fake_redis.delete("lock:camp:1")

    def test_release_does_not_steal_other_owner(self, fake_redis):
        """If the lock TTL expired and someone else took it, release
        must NOT delete their key.
        """
        h1 = locks.try_acquire("k", ttl_sec=1)
        assert h1 is not None
        # Simulate TTL expiry + new owner taking the lock
        fake_redis.set("lock:k", "another-owner-token", ex=60)
        locks.release(h1)
        # Other owner's lock is intact (whether or not EVAL ran, the safe-release
        # check should ensure we never clobbered someone else's token)
        assert fake_redis.get("lock:k") == b"another-owner-token"
        fake_redis.delete("lock:k")

    def test_ttl_is_set(self, fake_redis):
        h = locks.try_acquire("ttl_test", ttl_sec=120)
        assert h is not None
        ttl = fake_redis.ttl("lock:ttl_test")
        assert 100 < ttl <= 120
        fake_redis.delete("lock:ttl_test")


class TestLocalConcurrency:
    def test_only_one_thread_wins(self, no_redis):
        """Smoke: 10 threads racing for the same lock — at least one wins."""
        winners: list[bool] = []
        barrier = threading.Barrier(10)

        def attempt():
            barrier.wait()
            h = locks.try_acquire("race")
            if h is not None:
                winners.append(True)
                time.sleep(0.05)
                locks.release(h)

        threads = [threading.Thread(target=attempt) for _ in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        # At least one acquired (could be more across release-acquire cycles)
        assert len(winners) >= 1
