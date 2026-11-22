"""Shared helpers for the Streamlit pages.

Settings are re-read from .env on every rerun (so edits on the Settings
page apply at once) and never written into os.environ — background jobs
inherit a clean environment and read .env themselves.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import os
import sys
from dataclasses import dataclass
from pathlib import Path

import streamlit as st

from .. import envfile
from .. import telegram as tg
from ..config import Settings
from ..db import Database, LockHeld
from ..jobs import Jobs

STATE_LABEL = {
    "idle": "нет", "running": "идёт", "stopping": "останавливается…", "finished": "завершена",
    "paused": "на паузе", "stopped": "остановлена", "interrupted": "прервана (продолжится сама)",
}


def home() -> Path:
    return Path(os.environ.get("BLASTER_HOME") or Path.cwd())


def env_path() -> Path:
    return home() / ".env"


def env_values() -> dict:
    return {**os.environ, **envfile.read(env_path())}


@dataclass
class Ctx:
    settings: Settings
    db: Database
    jobs: Jobs

    @property
    def campaigns_dir(self) -> Path:
        return home() / "campaigns"

    @property
    def inbox(self) -> Path:
        return self.settings.db_path.parent / "inbox"


def ctx() -> Ctx:
    s = Settings.load(env=env_values(), root=home())
    return Ctx(settings=s, db=Database(s.db_path), jobs=Jobs(s.db_path.parent / "jobs"))


def blaster_argv(*args: str) -> list[str]:
    return [sys.executable, "-m", "blaster", *args]


def run_tg(c: Ctx, fn):
    """Run `await fn(client)` on a short connection holding the run lock.
    Returns (result, error_text)."""
    async def go():
        async with tg.connected(c.settings, c.db) as client:
            return await fn(client)
    try:
        return asyncio.run(go()), None
    except tg.NotAuthorized:
        return None, "Telegram не подключён — войди на странице «Подключение Telegram»."
    except LockHeld:
        return None, "Telegram сейчас занят фоновой операцией (поиск или рассылка). Дождись её или поставь на паузу."
    except SystemExit as e:                       # require_api / not authorized
        return None, str(e)
    except Exception as e:  # noqa: BLE001 — shown to the user, not swallowed
        return None, tg.explain(e)


def account_line(acc: dict | None) -> str:
    if not acc:
        return "не выполнен"
    name = f"{acc.get('first_name', '')} {acc.get('last_name', '')}".strip()
    user = f"@{acc['username']}" if acc.get("username") else "без @username"
    return f"{name} ({user}, +{acc.get('phone', '')})"


# ---- "tested" gate: real sending only after a test of the exact content -----

def fingerprint(camp: dict) -> str:
    img = camp.get("image_path") or ""
    stamp = ""
    if img and Path(img).is_file():
        st_ = Path(img).stat()
        stamp = f"{st_.st_size}:{int(st_.st_mtime)}"
    raw = "\x00".join([camp["template"], camp["parse_mode"], str(camp["link_preview"]), img, stamp,
                       camp["list_name"]])
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _tested_path(c: Ctx) -> Path:
    return c.settings.db_path.parent / "ui" / "tested.json"


def _tested(c: Ctx) -> dict:
    try:
        return json.loads(_tested_path(c).read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def mark_tested(c: Ctx, camp: dict) -> None:
    data = _tested(c)
    data[str(camp["id"])] = fingerprint(camp)
    p = _tested_path(c)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(data), encoding="utf-8")


def is_tested(c: Ctx, camp: dict) -> bool:
    return _tested(c).get(str(camp["id"])) == fingerprint(camp)


# ---- small widgets -----------------------------------------------------------

def job_caption(c: Ctx) -> str:
    stt = c.jobs.status()
    if stt.state == "idle":
        return "Фоновых операций нет"
    what = {"resolve": "Поиск в Telegram", "send": "Рассылка"}.get(stt.job.kind, stt.job.kind)
    target = stt.meta.get("campaign") or stt.meta.get("list") or ""
    code = f", код {stt.exit_code}" if stt.state == "finished" and stt.exit_code else ""
    return f"{what} «{target}»: {STATE_LABEL[stt.state]}{code}"


def show_log(c: Ctx, lines: int = 40) -> None:
    text = c.jobs.log_tail(lines)
    st.code(text or "лог пуст", language=None)
