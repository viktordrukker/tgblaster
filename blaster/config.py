"""Settings from environment (and an optional .env file)."""
from __future__ import annotations

import datetime as dt
import os
import random
from dataclasses import dataclass, field
from pathlib import Path
from zoneinfo import ZoneInfo

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover
    load_dotenv = None


def _int(env: dict, key: str, default: int) -> int:
    v = env.get(key, "")
    return int(v) if str(v).strip() else default


@dataclass
class Pacing:
    min_delay: int = 40
    max_delay: int = 100
    long_pause_every: int = 30
    long_pause_min: int = 600
    long_pause_max: int = 900
    daily_cap: int = 150                 # rolling 24h, across all campaigns
    max_flood_wait: int = 1800           # longer FloodWait → stop and resume later
    peer_flood_stop: int = 2             # distinct PeerFlood peers before stopping
    window_start: dt.time = dt.time(9, 0)
    window_end: dt.time = dt.time(21, 0)
    timezone: str = "Asia/Tashkent"
    rng: random.Random = field(default_factory=random.Random, repr=False)

    def __post_init__(self):
        if not (0 <= self.min_delay <= self.max_delay):
            raise ValueError("MIN_DELAY_SEC must be ≤ MAX_DELAY_SEC")
        if self.long_pause_every < 1 or self.long_pause_min > self.long_pause_max:
            raise ValueError("bad LONG_PAUSE_* settings")
        if self.daily_cap < 1:
            raise ValueError("DAILY_CAP must be ≥ 1")
        ZoneInfo(self.timezone)  # validate

    def delay_after(self, sent_in_run: int) -> int:
        if sent_in_run and sent_in_run % self.long_pause_every == 0:
            return self.rng.randint(self.long_pause_min, self.long_pause_max)
        return self.rng.randint(self.min_delay, self.max_delay)

    def seconds_until_window(self, now: dt.datetime | None = None) -> int:
        """0 if we are inside the sending window, else seconds until it opens."""
        tz = ZoneInfo(self.timezone)
        now = (now or dt.datetime.now(tz)).astimezone(tz)
        start, end = self.window_start, self.window_end
        t = now.time()
        inside = (start <= t < end) if start < end else (t >= start or t < end)
        if start == end or inside:
            return 0
        nxt = now.replace(hour=start.hour, minute=start.minute, second=0, microsecond=0)
        if nxt <= now:
            nxt += dt.timedelta(days=1)
        return int((nxt - now).total_seconds())


def _parse_window(s: str) -> tuple[dt.time, dt.time]:
    a, b = s.split("-")
    return dt.time.fromisoformat(a.strip()), dt.time.fromisoformat(b.strip())


@dataclass
class Settings:
    api_id: int | None
    api_hash: str
    session_path: Path
    db_path: Path
    reports_dir: Path
    region: str
    pacing: Pacing

    @classmethod
    def load(cls, env: dict | None = None, root: Path | None = None) -> "Settings":
        if env is None:
            if load_dotenv:
                load_dotenv(override=False)
            env = dict(os.environ)
        root = Path(env.get("BLASTER_HOME") or root or Path.cwd())
        sessions = Path(env.get("SESSIONS_DIR") or root / "sessions")
        data = Path(env.get("DATA_DIR") or root / "data")
        session_name = env.get("TG_SESSION_NAME") or "tg_session"
        window = env.get("SEND_WINDOW") or "09:00-21:00"
        ws, we = _parse_window(window)
        pacing = Pacing(
            min_delay=_int(env, "MIN_DELAY_SEC", 40),
            max_delay=_int(env, "MAX_DELAY_SEC", 100),
            long_pause_every=_int(env, "LONG_PAUSE_EVERY", 30),
            long_pause_min=_int(env, "LONG_PAUSE_MIN_SEC", 600),
            long_pause_max=_int(env, "LONG_PAUSE_MAX_SEC", 900),
            daily_cap=_int(env, "DAILY_CAP", 150),
            max_flood_wait=_int(env, "MAX_FLOOD_WAIT_SEC", 1800),
            peer_flood_stop=_int(env, "PEER_FLOOD_STOP", 2),
            window_start=ws, window_end=we,
            timezone=env.get("TZ_SEND") or "Asia/Tashkent",
        )
        api_id = env.get("TG_API_ID", "").strip()
        return cls(
            api_id=int(api_id) if api_id else None,
            api_hash=env.get("TG_API_HASH", "").strip(),
            session_path=sessions / f"{session_name}.session",
            db_path=Path(env.get("DB_PATH") or data / "blaster.db"),
            reports_dir=data / "reports",
            region=(env.get("DEFAULT_REGION") or "UZ").upper(),
            pacing=pacing,
        )

    def require_api(self) -> None:
        if not self.api_id or not self.api_hash:
            raise SystemExit("TG_API_ID / TG_API_HASH не заданы (.env). "
                             "Получить: https://my.telegram.org → API development tools.")
