"""Tests for the Pacer and PacingConfig."""
import random
import pytest

from core.rate_limiter import Pacer, PacingConfig


class TestPacingConfig:
    def test_valid_defaults(self):
        c = PacingConfig()
        assert c.min_delay_sec >= 0
        assert c.max_delay_sec >= c.min_delay_sec
        assert c.daily_cap >= 1

    def test_rejects_inverted_range(self):
        with pytest.raises(AssertionError):
            PacingConfig(min_delay_sec=100, max_delay_sec=10)

    def test_rejects_zero_long_pause_every(self):
        with pytest.raises(AssertionError):
            PacingConfig(long_pause_every=0)


class TestPacer:
    def test_delays_in_range(self):
        cfg = PacingConfig(min_delay_sec=10, max_delay_sec=20,
                           long_pause_every=100, long_pause_min_sec=0, long_pause_max_sec=0)
        pacer = Pacer(cfg, rng=random.Random(42))
        for _ in range(10):
            d = pacer.next_delay()
            assert 10 <= d <= 20

    def test_long_pause_at_interval(self):
        cfg = PacingConfig(
            min_delay_sec=1, max_delay_sec=1,
            long_pause_every=5, long_pause_min_sec=100, long_pause_max_sec=100,
            daily_cap=1000,
        )
        pacer = Pacer(cfg, rng=random.Random(0))
        delays = [pacer.next_delay() for _ in range(10)]
        # Every 5th delay should be the long one (100), others the short (1).
        assert delays[0] == 1
        assert delays[4] == 100  # 5th
        assert delays[9] == 100  # 10th

    def test_daily_cap(self):
        cfg = PacingConfig(daily_cap=50)
        pacer = Pacer(cfg)
        assert pacer.should_stop_for_day(49) is False
        assert pacer.should_stop_for_day(50) is True
        assert pacer.should_stop_for_day(51) is True

    def test_sent_counter_monotonic(self):
        pacer = Pacer(PacingConfig())
        for i in range(1, 6):
            pacer.next_delay()
            assert pacer.sent_in_session == i
