# Configuration

All configuration lives in `.env` at the project root. Values there
override the defaults in `core/config.py`. Copy `.env.example` to
`.env` and edit.

## Required

| Variable | Meaning | How to get it |
|---|---|---|
| `TG_API_ID` | Your Telegram app's numeric ID | <https://my.telegram.org> → API development tools |
| `TG_API_HASH` | 32-char hex — app secret | Same page. Keep it private. |

## Session

| Variable | Default | Meaning |
|---|---|---|
| `TG_SESSION_NAME` | `tg_session` | File stem for the Telethon session (lives at `sessions/<name>.session`). One name per Telegram account. |
| `TG_DB_NAME` | `state.db` | SQLite file name under `data/`. Override only if you want to run multiple isolated deployments on the same host. |

## Pacing

All pacing defaults are conservative — designed to keep a warmed-up
account under the radar. New accounts should ratchet down further.

| Variable | Default | Meaning |
|---|---|---|
| `DAILY_CAP` | `300` | Max sends per 24-hour window. Telegram starts flagging around 500+/day for cold DMs. |
| `MIN_DELAY_SEC` | `30` | Lower bound on the random gap between sends. |
| `MAX_DELAY_SEC` | `90` | Upper bound. |
| `LONG_PAUSE_EVERY` | `40` | After this many sends, insert a long pause. |
| `LONG_PAUSE_MIN_SEC` | `600` | Minimum length of the long pause (10 min). |
| `LONG_PAUSE_MAX_SEC` | `900` | Maximum (15 min). |

A full campaign of 200 contacts at default pacing takes ~3–6 hours
including long pauses. That is by design.

## Docker-only

| Variable | Default | Meaning |
|---|---|---|
| `REDIS_URL` | `redis://redis:6379/0` | Only needed if you point Docker at an external Redis. The bundled `redis` service is configured automatically. |
| `IN_DOCKER` | `1` | Set by `docker-compose.yml`. Used by the app to detect Docker-mode and prefer Redis over threading fallback. |

## UI-only

Streamlit's own settings live in `.streamlit/config.toml` (not in
`.env`). Change the port, theme, or file-size cap there.

## Multi-account

The first account is seeded from `.env` on first boot. Additional
accounts are created via the UI ("Step 1 · Accounts" → "Add / update").
Each account has its own `api_id`, `api_hash`, and session file. The
sidebar dropdown picks the active account.

See `docs/ARCHITECTURE.md` for how accounts relate to campaigns.
