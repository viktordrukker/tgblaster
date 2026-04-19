# Architecture

TGBlaster is a three-service stack plus two local-filesystem state
stores. The design intent is single-operator, single-Telegram-account
deployments — there is no multi-tenancy, no API gateway, no external
auth. Everything runs on one host (laptop or one VPS).

## Services

```
┌───────────────────────────┐
│  app    (Streamlit UI)    │   image: built from Dockerfile
│  :8501                    │   command: streamlit run app.py
└───────────┬───────────────┘
            │
            │   enqueues arq jobs, reads job state from SQLite
            ▼
┌───────────────────────────┐      ┌──────────────────────┐
│  worker (arq)             │◀────▶│  redis               │
│  core/jobs.py             │      │  7-alpine            │
│  cron: watchdog, gsheet   │      │  used for arq queue  │
└───────────┬───────────────┘      │  + distributed locks │
            │                      └──────────────────────┘
            │   Telethon over TCP → Telegram DCs
            ▼
         ┌──────┐
         │  TG  │
         └──────┘
```

## State stores

### SQLite at `data/state.db`

Single file. WAL journal mode. Owned by both the `app` and `worker`
processes simultaneously via SQLite's file locks + busy_timeout.
Connection-per-thread (via `threading.local`) amortizes the per-call
~30ms connect cost that was dominating UI renders on WSL2 bind mounts.

Tables (abbreviated):

| Table | Purpose |
|---|---|
| `contacts` | Phone/handle + resolved `tg_user_id` + `tg_access_hash` |
| `campaigns` | Name, template, image path, group link, status, tags |
| `send_log` | Per-(campaign, contact) row: status, message_id, random_id, read_at |
| `jobs` | Queue: arq job_id, type, status, progress, heartbeat |
| `accounts` | v1.5 multi-account — one row per Telegram user |
| `opt_outs` | Never-contact list (by tg_user_id) |
| `sheet_sources` | Optional auto-import from a Google Sheet |

### Telethon session at `sessions/<name>.session`

SQLite file owned exclusively by Telethon. Contains auth key + peer
cache. Both app and worker share the same session for the same
account — Telethon's sqlite driver handles concurrent access via
busy_timeout=15s (see `core/auth.py::_PatientSession`).

### Uploaded images at `uploads/`

Plain files named by upload. Referenced by `campaigns.image_path`.

## Key state machines

### Campaign lifecycle

```
draft ──Start──▶ running ──Pause──┐
                   ▲              ▼
                   │            paused
                   └──Resume─────┤
                                 │
                   ┌──Stop──────┘
                   ▼
                 stopped
                   │
                   └──Reset──▶ draft    (send_log cleared for this campaign)
```

Implemented in `core/database.py::transition_campaign_state()`. Legal
transitions are enforced; illegal attempts return `False`.

### Idempotent send (reserve → send → confirm)

For every contact the campaign fires:

1. `reserve_send(campaign_id, contact_id, random_id)` inserts
   `send_log` row with status=`pending`, recording the pre-allocated
   `random_id` that Telegram uses for server-side dedup.
2. `_send_one()` makes the actual `SendMessage` / `SendFile` RPC,
   wrapping it in `asyncio.wait_for(timeout=60)` + `_ensure_connected`.
3. `confirm_send(campaign_id, contact_id, status, detail, message_id)`
   transitions the row to `sent` / `error` / `skipped`.

Crash / DB-lock scenarios:

- **Crash between step 2 and 3**: row stays `pending`. On resume,
  `reserve_send` returns the stored `random_id` so the retried send is
  server-side deduped by Telegram.
- **Session-lock after step 2**: Telethon's `process_entities` write
  to its own session DB fails; the RPC itself already succeeded. We
  catch this specifically and mark the row `sent` — anything else
  would cause duplicate delivery on retry.
- **`PeerFloodError` at step 2**: row marked `error`; the whole
  campaign halts; the user must wait ≥ 24h and re-evaluate pacing.

See `core/sender.py::run_campaign`.

## Jobs + watchdog

```
queued ──► running ──► done | error | cancelled
```

- Each job row has `last_heartbeat` updated by `_heartbeat_loop`
  every 10s while the job is executing.
- A watchdog cron (`core/jobs.py::watchdog_job`, every 2 min)
  recovers:
  1. queued for > 2 min with no `started_at` → worker never picked up
  2. running with no heartbeat for > 3 min → worker froze or died
  3. running with `progress.status='starting'` for > 90s → first send
     hung before any contact was attempted
- Campaign lock TTL is 5 min; the heartbeat extends it every 10s, so
  a live worker holds the lock indefinitely; a dead worker's lock
  expires within 5 min and the next resume can pick up.

## Concurrency rules

| Lock key | Holder | TTL |
|---|---|---|
| `lock:resolve` | `resolve_contacts_job` | 3600s |
| `lock:validate_usernames` | `validate_usernames_job` | 3600s |
| `lock:campaign:{id}` | `run_campaign_job` | 300s (auto-extended) |
| `lock:read_check` | `check_read_receipts_job` | 600s |
| `lock:gsheet_sync:{source_id}` | `sync_all_gsheet_sources` cron | cadence − 10s |

Backend tagging: each job row records whether it's `arq` or `thread`
(in-process fallback when Redis is unavailable). At execution time,
`_guard_job_backend` refuses to run a job whose tag doesn't match the
current executor — prevents the race where Redis recovery lets an arq
worker pick up a job already running as a thread.

## Code layout

```
core/
  auth.py            Telethon login + session management + PatientSession
  campaign_runner.py Legacy thread-runner (dry-run path only now)
  config.py          .env loading, Settings/AccountSettings dataclasses
  csv_io.py          CSV parsing, phone normalization, dedup
  database.py        SQLite schema + all query methods
  jobs.py            arq worker + Dispatcher + watchdog cron
  locks.py           Distributed locks (Redis or local-thread fallback)
  md.py              Telegram-Markdown-V1 → HTML for preview
  rate_limiter.py    Pacer with random delays + daily cap
  read_receipts.py   GetPeerDialogsRequest wrapper for manual read-check
  redis_client.py    Graceful Redis availability detection
  resolver.py        ImportContactsRequest batching + @username validation
  sender.py          run_campaign() with reserve/send/confirm
  template.py        {placeholder} rendering with CSV extras

app.py               Streamlit UI — 8 pages (Login, Contacts, Compose,
                     Dry-run, Campaign, Log, Jobs, + setup sidebar)

tests/               140 pytest cases, zero Telegram round-trips (mocked)
```
