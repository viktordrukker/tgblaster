"""Run sender.run_campaign in a background thread so Streamlit can render
live progress without blocking.

We use a queue.Queue (thread-safe) for progress events, and store a stop
signal on the runner instance so the UI can pause.
"""
from __future__ import annotations

import asyncio
import queue
import threading
from dataclasses import dataclass, field
from typing import Optional

from telethon import TelegramClient

from . import auth, sender
from .database import Database
from .rate_limiter import PacingConfig


@dataclass
class CampaignRunner:
    client: TelegramClient
    db: Database
    campaign_id: int
    pacing: PacingConfig
    dry_run_to_self: bool = False

    events: queue.Queue = field(default_factory=queue.Queue)
    stop_signal: sender.StopSignal = field(default_factory=sender.StopSignal)
    thread: Optional[threading.Thread] = None
    outcome: Optional[sender.SendOutcome] = None
    error: Optional[str] = None
    _done: threading.Event = field(default_factory=threading.Event)

    def start(self):
        if self.thread and self.thread.is_alive():
            return
        self._done.clear()
        self.thread = threading.Thread(target=self._run, daemon=True, name=f"campaign-{self.campaign_id}")
        self.thread.start()

    def _run(self):
        async def on_progress(payload: dict):
            try:
                self.events.put_nowait(dict(payload))
            except queue.Full:
                pass

        async def main():
            # Worker-grade retry budget — a dry-run is cheap and worth
            # waiting through a temporary session-file lock.
            await auth.connect_client(self.client, attempts=5, backoff_sec=2.0)
            return await sender.run_campaign(
                self.client, self.db, self.campaign_id, self.pacing,
                on_progress=on_progress,
                stop_signal=self.stop_signal,
                dry_run_to_self=self.dry_run_to_self,
            )

        try:
            # Submit to the persistent Telethon loop — the cached client
            # is bound to it, so we must not create a fresh loop here.
            self.outcome = auth.run_async(main())
        except Exception as e:  # noqa: BLE001
            self.error = str(e)
        finally:
            self._done.set()

    def stop(self):
        self.stop_signal.set()

    def is_running(self) -> bool:
        return self.thread is not None and self.thread.is_alive()

    def is_done(self) -> bool:
        return self._done.is_set()

    def drain_events(self) -> list[dict]:
        evts = []
        while True:
            try:
                evts.append(self.events.get_nowait())
            except queue.Empty:
                break
        return evts
