"""Configuration loaded from environment / .env file.

Everything has sensible defaults so the app starts even without .env.
"""
from __future__ import annotations

import logging
import os
import stat
import sys
from dataclasses import dataclass
from pathlib import Path

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:  # dotenv is optional at runtime
    pass

_log = logging.getLogger("tgblaster.config")


def _warn_if_env_world_readable() -> None:
    """Warn (don't fail) when .env is mode wider than 0600 on POSIX systems.

    Owner-only read is the safe posture for a file that holds api_hash and
    lives in a working directory that may be shared (e.g. bind-mounted
    into a Docker container). On Windows the Unix permission bits don't
    apply, so we skip.
    """
    if sys.platform.startswith("win"):
        return
    env_path = Path(__file__).resolve().parent.parent / ".env"
    try:
        st_info = env_path.stat()
    except FileNotFoundError:
        return
    perms = stat.S_IMODE(st_info.st_mode)
    if perms & 0o077:  # any group/other bits set
        _log.warning(
            ".env is mode %o; group/other can read credentials. "
            "Run: chmod 600 %s", perms, env_path,
        )


_warn_if_env_world_readable()


PROJECT_ROOT = Path(__file__).resolve().parent.parent
SESSIONS_DIR = PROJECT_ROOT / "sessions"
DATA_DIR = PROJECT_ROOT / "data"
UPLOADS_DIR = PROJECT_ROOT / "uploads"

for _d in (SESSIONS_DIR, DATA_DIR, UPLOADS_DIR):
    _d.mkdir(parents=True, exist_ok=True)


def _resolve_db_path() -> Path:
    """Pick the SQLite DB filename.

    Default: `data/state.db` (generic, shippable). For users with a
    legacy deployment using `data/meetup.db` (the pre-release name),
    we continue to use that file so running campaigns don't vanish.
    The env var `TG_DB_NAME` takes precedence either way.
    """
    override = os.getenv("TG_DB_NAME", "").strip()
    if override:
        return DATA_DIR / override
    legacy = DATA_DIR / "meetup.db"
    if legacy.exists():
        return legacy
    return DATA_DIR / "state.db"


def _int(env_key: str, default: int) -> int:
    raw = os.getenv(env_key)
    try:
        return int(raw) if raw not in (None, "") else default
    except (ValueError, TypeError):
        return default


@dataclass(frozen=True)
class Settings:
    api_id: int
    api_hash: str
    session_name: str
    session_path: Path
    db_path: Path
    daily_cap: int
    min_delay_sec: int
    max_delay_sec: int
    long_pause_every: int
    long_pause_min_sec: int
    long_pause_max_sec: int

    @property
    def has_credentials(self) -> bool:
        return bool(self.api_id) and bool(self.api_hash)


def load_settings() -> Settings:
    """Read environment and return an immutable Settings object."""
    api_id = _int("TG_API_ID", 0)
    api_hash = os.getenv("TG_API_HASH", "").strip()
    session_name = os.getenv("TG_SESSION_NAME", "tg_session").strip() or "tg_session"
    return Settings(
        api_id=api_id,
        api_hash=api_hash,
        session_name=session_name,
        session_path=SESSIONS_DIR / session_name,
        db_path=_resolve_db_path(),
        daily_cap=_int("DAILY_CAP", 300),
        min_delay_sec=_int("MIN_DELAY_SEC", 30),
        max_delay_sec=_int("MAX_DELAY_SEC", 90),
        long_pause_every=_int("LONG_PAUSE_EVERY", 40),
        long_pause_min_sec=_int("LONG_PAUSE_MIN_SEC", 600),
        long_pause_max_sec=_int("LONG_PAUSE_MAX_SEC", 900),
    )


def save_credentials(api_id: int, api_hash: str, session_name: str = "tg_session") -> None:
    """Persist credentials in .env so they survive restarts.

    Overwrites the file. We intentionally keep this simple — the .env file
    is gitignored. For multi-user setups you'd want a real secret store.
    """
    env_path = PROJECT_ROOT / ".env"
    contents = (
        f"TG_API_ID={int(api_id)}\n"
        f"TG_API_HASH={api_hash.strip()}\n"
        f"TG_SESSION_NAME={session_name.strip() or 'tg_session'}\n"
    )
    # Preserve any other variables the user added.
    if env_path.exists():
        existing = env_path.read_text(encoding="utf-8").splitlines()
        keep = [
            line for line in existing
            if line.strip() and not line.strip().startswith("#")
            and not line.split("=", 1)[0].strip() in {"TG_API_ID", "TG_API_HASH", "TG_SESSION_NAME"}
        ]
        if keep:
            contents += "\n" + "\n".join(keep) + "\n"
    env_path.write_text(contents, encoding="utf-8")
    # Update process env so the new value is visible without restart.
    os.environ["TG_API_ID"] = str(int(api_id))
    os.environ["TG_API_HASH"] = api_hash.strip()
    os.environ["TG_SESSION_NAME"] = session_name.strip() or "tg_session"


# ---------------------------------------------------------------------------
# v1.5 — multi-account. `accounts` table is the runtime source of truth;
# .env stays as bootstrap for first-boot migration and for backward compat.
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class AccountSettings:
    """Everything needed to drive Telethon for one TG account, plus the
    shared pacing knobs (which remain process-global for now)."""
    id: int
    label: str
    api_id: int
    api_hash: str
    session_name: str
    session_path: Path
    db_path: Path
    daily_cap: int
    min_delay_sec: int
    max_delay_sec: int
    long_pause_every: int
    long_pause_min_sec: int
    long_pause_max_sec: int

    @property
    def has_credentials(self) -> bool:
        return bool(self.api_id) and bool(self.api_hash)


def _pacing_from_env() -> dict:
    return dict(
        daily_cap=_int("DAILY_CAP", 300),
        min_delay_sec=_int("MIN_DELAY_SEC", 30),
        max_delay_sec=_int("MAX_DELAY_SEC", 90),
        long_pause_every=_int("LONG_PAUSE_EVERY", 40),
        long_pause_min_sec=_int("LONG_PAUSE_MIN_SEC", 600),
        long_pause_max_sec=_int("LONG_PAUSE_MAX_SEC", 900),
    )


def ensure_default_account_seeded(db) -> int | None:
    """If the accounts table is empty and .env has credentials, insert a
    single row labelled "default" and mark it active. Back-fills
    `campaigns.account_id` to that row for pre-v1.5 campaigns.

    Returns the seeded account id, or None if nothing was seeded.
    """
    existing = db.list_accounts()
    if existing:
        return None
    env_api_id = _int("TG_API_ID", 0)
    env_api_hash = os.getenv("TG_API_HASH", "").strip()
    env_session = os.getenv("TG_SESSION_NAME", "tg_session").strip() or "tg_session"
    if not env_api_id or not env_api_hash:
        return None
    account_id = db.upsert_account(
        label="default",
        api_id=env_api_id,
        api_hash=env_api_hash,
        session_name=env_session,
        notes=".env-seeded on first boot",
        is_active=True,
    )
    # Back-fill pre-v1.5 campaigns so filtering by account_id still works.
    with db._conn() as c:
        c.execute("UPDATE campaigns SET account_id=? WHERE account_id IS NULL",
                  (int(account_id),))
    return account_id


def settings_for_account(row) -> AccountSettings:
    """Build an AccountSettings snapshot from a DB row (sqlite3.Row or dict)."""
    pacing = _pacing_from_env()
    session_name = row["session_name"]
    return AccountSettings(
        id=int(row["id"]),
        label=row["label"],
        api_id=int(row["api_id"]),
        api_hash=row["api_hash"],
        session_name=session_name,
        session_path=SESSIONS_DIR / session_name,
        db_path=_resolve_db_path(),
        **pacing,
    )


def load_account_settings(db, account_id: int | None = None) -> AccountSettings | None:
    """Return AccountSettings for the requested account, or the active one.
    None if no accounts exist yet (caller should fall back to load_settings).
    """
    row = db.get_account(account_id) if account_id else db.get_active_account()
    if row is None:
        return None
    return settings_for_account(row)
