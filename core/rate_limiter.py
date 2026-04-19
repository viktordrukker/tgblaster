"""Pacing logic for the sender — deliberately simple and pure.

Separated from the Telethon-dependent sender so it can be unit-tested
without any network or async client.
"""
from __future__ import annotations

import random
from dataclasses import dataclass


@dataclass
class PacingConfig:
    min_delay_sec: int = 30
    max_delay_sec: int = 90
    long_pause_every: int = 40
    long_pause_min_sec: int = 600
    long_pause_max_sec: int = 900
    daily_cap: int = 300

    def __post_init__(self):
        assert self.min_delay_sec >= 0
        assert self.max_delay_sec >= self.min_delay_sec
        assert self.long_pause_every >= 1
        assert self.long_pause_max_sec >= self.long_pause_min_sec >= 0
        assert self.daily_cap >= 1


class Pacer:
    """Decides how long to wait between sends.

    Stateless except for `sent_in_session`, which counts sends so we can
    trigger the periodic long pause.
    """

    def __init__(self, cfg: PacingConfig, rng: random.Random | None = None):
        self.cfg = cfg
        self.rng = rng or random.Random()
        self.sent_in_session = 0

    def next_delay(self) -> int:
        """Return seconds to wait *before* the next send."""
        self.sent_in_session += 1
        if (
            self.cfg.long_pause_every > 0
            and self.sent_in_session % self.cfg.long_pause_every == 0
        ):
            return self.rng.randint(self.cfg.long_pause_min_sec, self.cfg.long_pause_max_sec)
        return self.rng.randint(self.cfg.min_delay_sec, self.cfg.max_delay_sec)

    def should_stop_for_day(self, sent_today: int) -> bool:
        return sent_today >= self.cfg.daily_cap
