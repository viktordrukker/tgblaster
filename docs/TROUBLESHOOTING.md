# Troubleshooting

Grouped by what you'd search for in the logs or see in the UI.

## Login / authentication

### "Код не приходит" / no login code received

- Telegram sends the code **inside Telegram itself** if you're already
  logged in on another device, or by SMS if this is a first-time login.
- Check the Telegram app on your phone first.
- If neither arrives, the `api_id`/`api_hash` are wrong or the phone
  number is already banned.

### "Cannot send requests while disconnected"

The Telethon client got dropped by Telegram after an idle period. The
sender now calls `_ensure_connected()` before every send which
reconnects automatically. If you still see this in logs, it's a
transient network blip — the retry should succeed.

## Campaigns

### `PeerFloodError` during a send

Telegram's spam flag has been raised on your account. TGBlaster stops
the campaign immediately. Next steps:

1. **Do not restart the campaign.** Wait at least 24 hours.
2. During the wait, act normal in Telegram — have human conversations,
   respond to messages, use the account as you normally would.
3. On day 2, try a 5-contact dry-run to your own test accounts. If it
   succeeds, resume the campaign at a lower volume (e.g. 20/day) for a
   week before increasing.
4. If `PeerFloodError` repeats immediately, the account is flagged
   long-term. Consider creating a bot account via BotFather for
   future outreach — bots live under different rules (and can only DM
   users who have `/start`ed them, which is the model you should
   probably be using anyway).

### `FloodWaitError` with a large sleep

Telegram is asking you to slow down. The sender logs the wait duration
and sleeps for `seconds + 5` before retrying once. For a wait > 300s,
the long pause is visible in the campaign panel UI as a `flood_wait`
status. Don't interrupt — letting it complete is safer than restarting.

### "database is locked"

This was the most common production error before the R-phase fixes.
Today it's nearly impossible to hit for send-path writes, because:

- `_conn()` uses a thread-local connection with `busy_timeout=5000`.
- Send-log writes go through `_retry_on_lock` (exponential backoff,
  4 attempts).
- Telethon session is opened with `busy_timeout=15000` so post-send
  `process_entities` tolerates concurrent UI auth-checks.
- A session-DB lock AFTER a successful RPC is caught specifically and
  counted as sent (rather than error), preventing duplicate delivery
  on retry.

If you still see the error, upload the worker log to the issue —
it's almost certainly a path we haven't covered.

### Campaign "stuck at starting"

The worker picked up the job and set `status=running` but the first
send hasn't completed within 90s. Usually:

- The Telethon client can't connect (auth issue, network issue).
- The first contact has a stale `tg_user_id` requiring re-resolution.

The watchdog (runs every 2 min) auto-recovers by marking the job
`error`. You can also hit the 🧹 manual unstick button on the Campaign
page.

## Resolve / Validate

### "User not on Telegram" for a number that IS on Telegram

The person's privacy settings block `ImportContactsRequest` lookups
("Who can find me by my phone number: Nobody"). They're using
Telegram, but you can't find them this way. If you have their
`@username`, the Validate step will work.

### Validate marks everything as error

Probably the account is rate-limited. Wait 15 min and retry with a
smaller batch. Use the per-row Validate button on the Contacts page
instead of the bulk one.

## Database / deployment

### "Running campaign disappeared after `docker compose down`"

It didn't — state is in `data/state.db` (bind-mounted or named-volume
persistent). `docker compose down` stops containers but doesn't touch
volumes. Re-run `docker compose up -d` and the Jobs page shows the old
runs. If a campaign was `running` at down-time, the watchdog flips it
to `error` within 5 min. Resume via the 🧹 unstick button or manually
re-start from the Campaign page.

### The UI is extremely slow

Common causes:

- Windows host with WSL2 bind mounts → SQLite `connect()` is ~30ms per
  call. We amortize this via thread-local connection reuse in
  `core/database.py::_get_conn()`. If your deployment is on Linux,
  query times should be sub-millisecond.
- You have 10,000+ contacts and are using a heavy `contacts_df` query
  on every render. That's a known limitation of the current
  Contacts page; consider filtering before rendering.

### Worker keeps restarting

`docker compose logs worker --tail 100` usually shows the reason.
Common ones:

- Missing `TG_API_ID` / `TG_API_HASH`
- Redis unreachable (check the `redis` service is healthy)
- Import error because a dependency went missing — rebuild with
  `docker compose up -d --build --force-recreate worker`
