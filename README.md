# TGBlaster

**Self-hosted Telegram messaging tool for consent-based outreach — invite
your event registrants, contact your opted-in subscribers, or run drip
campaigns to your own customer list. Runs as three Docker services on
your laptop or a €4/mo VPS.**

> 🇷🇺 [Русская версия README](./README.ru.md)

---

## ⚠️ Read before using

Telegram **prohibits bulk messaging to users who haven't consented**.
Cold outreach to scraped or purchased contact lists will get your
account banned — and TGBlaster cannot prevent that. Use this tool only
for audiences that actively expect to hear from you.

Before your first campaign, read **[DISCLAIMER.md](./DISCLAIMER.md)** in
full. It covers Telegram's ToS, GDPR implications, and the limits of
what this software can protect you from.

---

## Screenshots

| | |
|---|---|
| ![Login](./docs/screenshots/01-login.png) | ![Contacts](./docs/screenshots/02-contacts.png) |
| ![Compose](./docs/screenshots/03-compose.png) | ![Campaign](./docs/screenshots/04-campaign.png) |
| ![Log + read receipts](./docs/screenshots/05-log.png) | ![Jobs](./docs/screenshots/06-jobs.png) |

---

## What it does

1. **Imports contacts** from CSV (flexible column names; Russian / Uzbek
   number formats normalized to E.164).
2. **Resolves phones to Telegram users** via `ImportContactsRequest`
   batched at 100/call, with automatic cleanup.
3. **Validates `@usernames`** for contacts who gave a handle but no
   phone.
4. **Personalizes messages** — `{first_name}`, `{name}`, `{group_link}`,
   plus any CSV column as `{placeholder}`.
5. **Sends at human-looking pace**: 30–90 s random gap, 10–15 min pause
   every 40 messages, configurable daily cap, automatic stop on
   `PeerFloodError`.
6. **Resume on crash**: progress is checkpointed to SQLite; a killed
   worker picks up exactly where it stopped via an idempotency journal.
7. **Triple-layer dedup**: duplicate phones in the same CSV, two phones
   that map to one Telegram user, and cross-campaign "don't re-DM this
   person for N days".
8. **Read receipts**: manually trigger a batch `GetPeerDialogsRequest`
   to flip per-message `read_at` for messages Telegram reports as read.
9. **Campaign CRUD**: edit, clone, delete past campaigns — no need to
   rebuild a template from scratch.
10. **5-device preview**: the compose screen shows how the message will
    look on iPhone, iPad, Android, Desktop, and Web Telegram clients
    (device-specific fonts, bubble radius, link colors).

---

## Architecture

```
┌─────────────────────┐      ┌──────────┐      ┌──────────────────┐
│  Streamlit UI       │◀────▶│  Redis   │◀────▶│  arq worker      │
│  (app.py, :8501)    │      │  (queue) │      │  (core/jobs.py)  │
└──────────┬──────────┘      └──────────┘      └────────┬─────────┘
           │                                             │
           └────────────────┬────────────────────────────┘
                            ▼
             ┌──────────────────────────────┐
             │  SQLite (data/state.db)      │
             │  + Telethon session          │
             │  + uploaded images           │
             └──────────────────────────────┘
```

- **UI** renders pages in Streamlit; runs Dry-run and test-fires in
  process.
- **Worker** runs real campaigns, resolve jobs, validate jobs,
  read-check jobs, and a watchdog cron.
- **Redis** holds the arq queue, distributed locks, and pause/stop
  signals.
- **SQLite** is the single source of truth for contacts, campaigns,
  send log, and jobs state.

See [docs/ARCHITECTURE.md](./docs/ARCHITECTURE.md) for the full design.

---

## Quick start (Docker)

```bash
git clone https://github.com/viktordrukker/tgblaster.git
cd tgblaster
cp .env.example .env       # then edit — see "Getting API credentials" below
docker compose up -d --build
```

Open <http://localhost:8501> and follow the 8-step flow (login →
import → resolve → compose → dry-run → campaign → log → jobs).

Stop: `docker compose down`. State in `data/`, `sessions/`, `uploads/`
survives.

### Getting Telegram API credentials

1. Open <https://my.telegram.org> → log in with the phone number you
   want to send from.
2. API development tools → create a new app (any name).
3. Copy `api_id` and `api_hash` into `.env`:
   ```ini
   TG_API_ID=123456
   TG_API_HASH=0123456789abcdef0123456789abcdef
   TG_SESSION_NAME=tg_session
   ```

See [docs/CONFIGURATION.md](./docs/CONFIGURATION.md) for every setting.

---

## Install paths

- **[docs/DEPLOYMENT.md](./docs/DEPLOYMENT.md)** — Docker local (this
  README's quick start), Hetzner VPS with Caddy + basic auth, bare-metal
  systemd units, cloud PaaS options.
- **[docs/DEVELOPMENT.md](./docs/DEVELOPMENT.md)** — running tests,
  adding a new job type, adding a UI page.
- **[docs/TROUBLESHOOTING.md](./docs/TROUBLESHOOTING.md)** — `FloodWait`,
  `PeerFlood`, "database is locked", session-file contention, and
  other recurring errors.

---

## Tech stack

- **Python 3.11**
- **Streamlit 1.40** — UI
- **Telethon 1.43** — MTProto client
- **arq 0.26** — async worker queue on Redis
- **SQLite** with WAL journal — single-node persistence
- **Docker Compose** — three-service stack

---

## Contributing

Pull requests welcome. See [CONTRIBUTING.md](./CONTRIBUTING.md) for the
workflow, commit style, and how issues are triaged.

Report security issues privately via GitHub's
[security advisory](https://github.com/viktordrukker/tgblaster/security/advisories/new) —
**not** as a public issue. See [SECURITY.md](./SECURITY.md).

---

## License

[MIT](./LICENSE) — do what you want with the code, including
commercial use. The only thing you can't do is pretend the authors
authored *your* messaging policy for you. See
**[DISCLAIMER.md](./DISCLAIMER.md)**.
