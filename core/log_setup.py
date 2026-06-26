"""Phase H4 (parent) — structured JSON logger with correlation IDs.
Mirror of bot-stack/bot/log_setup.py so the two stacks emit
compatible logs.

Tail-and-filter usage:
    docker compose logs -f worker | jq 'select(.job_id == 99)'
"""
from __future__ import annotations

import json
import logging
import os
import socket
import sys
import time
from contextvars import ContextVar


correlation_ctx: ContextVar[dict] = ContextVar(
    "parent_correlation", default={},
)


_HOST = os.environ.get("HOSTNAME") or socket.gethostname() or "?"
_VERSION = os.environ.get("APP_VERSION", "dev")


class JSONFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:  # noqa: A003
        out: dict = {
            "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ",
                                  time.gmtime(record.created)),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
            "host": _HOST,
            "version": _VERSION,
            "stack": "parent",
        }
        try:
            ctx = correlation_ctx.get() or {}
        except Exception:
            ctx = {}
        if ctx:
            out.update(ctx)
        if record.exc_info:
            out["exc"] = self.formatException(record.exc_info)
        return json.dumps(out, ensure_ascii=False)


def install(level: str | None = None) -> None:
    """Replace root logger handlers with a single JSON-formatted
    stdout handler. Idempotent."""
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JSONFormatter())
    root = logging.getLogger()
    root.handlers[:] = [handler]
    root.setLevel(level or os.environ.get("LOG_LEVEL", "INFO"))
    for noisy in ("aiohttp", "telethon.network"):
        logging.getLogger(noisy).setLevel(logging.WARNING)
    # arq attaches its own StreamHandler at import time, which causes
    # double-emission (plain text from arq + JSON from us). Strip arq's
    # handlers so the root JSON handler is the only sink.
    for child in ("arq.worker", "arq.queue"):
        lg = logging.getLogger(child)
        lg.handlers[:] = []
        lg.propagate = True


def push(**kwargs) -> None:
    cur = dict(correlation_ctx.get() or {})
    cur.update(kwargs)
    correlation_ctx.set(cur)


def reset() -> None:
    correlation_ctx.set({})
