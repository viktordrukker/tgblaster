# TGBlaster

**Paced, resumable, duplicate-safe Telegram broadcasts from a spreadsheet.**
One container, a web UI (plus a CLI for scripting), SQLite state. Built for communities and meetups messaging people who gave
their contacts and expect to hear from you.

> 🇷🇺 [Русская версия и подробный runbook](./README.ru.md) · ⚠️ read [DISCLAIMER.md](./DISCLAIMER.md) first

## Highlights

- **Reads almost any table**: `.xlsx`, `.xls`, `.csv`/`.tsv` (any encoding and delimiter), `.json`, or a
  Google Sheets link. It finds the header row (even under a title banner) and the phone, Telegram,
  full-name, first/last-name, greeting and consent columns, using both header keywords (RU/UZ/EN)
  and cell content. Every decision is printed and can be overridden.
- **Messy data handled**: Excel floats, national formats, several numbers per cell, `@handle`,
  `t.me/handle`, `t.me/+phone`, combined "phone or username" columns. Given names are extracted
  from full names (`Ivanov Ivan Ivanovich` → `Ivan`).
- **Resolution**: address book first, then batched phone import (with alternates), then `@username`.
  Only contacts the tool added get removed again; the sender's own contacts are never touched.
- **Safe sending**: random delays, periodic long pauses, a rolling-24h cap, a local-time send window,
  FloodWait/PeerFlood handling, and a stop after repeated failures.
- **No duplicates**: a reserve→confirm journal. If the process dies mid-send, that contact is marked
  `uncertain` and is never resent automatically. One person with two numbers gets one message.
- Opt-outs, per-contact CSV reports, a test send to Saved Messages.

## Quick start

```bash
cp .env.example .env                     # TG_API_ID / TG_API_HASH from my.telegram.org
docker compose up -d                     # → http://127.0.0.1:8501
```

Pages: Telegram login (phone → code → 2FA) · Contacts (upload or link, detected columns editable,
import on click) · Telegram lookup with live progress · Message editor with preview on real contacts and
a test to Saved Messages · Sending (start/pause/stop, log, CSV report, opt-outs) · Settings (pacing, written
to `.env`). Sending unlocks only after a test of the exact current message. Long operations run as
background processes: closing the tab does not stop them, and a send interrupted by a container restart
resumes on start.

CLI: `docker compose run --rm blaster --help`.

The full guide is in [README.ru.md](./README.ru.md).

## Development

```bash
python -m venv .venv && .venv/bin/pip install -r requirements-dev.txt
.venv/bin/python -m blaster.web      # UI on http://127.0.0.1:8501
.venv/bin/python -m pytest
```

MIT licensed.
