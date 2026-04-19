# Development

## Running tests

```bash
docker compose exec app pytest tests/ -x
```

Or without the container:

```bash
python -m venv .venv
source .venv/bin/activate     # Windows: .venv\Scripts\Activate.ps1
pip install -r requirements.txt
pytest tests/ -x
```

All 140 tests are offline — no Telegram round-trips, no Redis required
(fakeredis is used). Target: green in < 5 seconds on a laptop.

## Local iteration loop

```bash
# 1. Code changes in app.py / core/
# 2. Hot reload for UI changes only:
docker compose restart app       # ~3s, session kept
# 3. Full rebuild on dependency or Dockerfile change:
docker compose up -d --build     # ~15s
# 4. Worker restart for core/sender.py, core/jobs.py, core/database.py:
docker compose restart worker
```

VSCode: open the repo root; it offers to use the Dev Container
(`.devcontainer/`) which spins up the same compose stack but with
source bind-mounted for live reload.

## Layout

See the tree in `docs/ARCHITECTURE.md`. TL;DR:

- **`core/`** — all the non-UI code. Each file owns one responsibility.
- **`app.py`** — Streamlit pages. ~2700 lines. Each `render_X()`
  function is one page.
- **`tests/`** — one test file per core module plus one for each major
  behavioral surface.

## Adding a new job type

1. Write the coroutine in `core/jobs.py`:
   ```python
   async def my_new_job(ctx: dict, payload: dict) -> dict:
       db = Database(payload["db_path"])
       executor = _current_executor(ctx)
       if not _guard_job_backend(db, payload["job_id"], executor):
           return {"status": "skipped"}
       lock = locks.try_acquire("my_new", ttl_sec=600, backend=executor)
       if lock is None:
           db.mark_job_error(payload["job_id"], "another run is in flight")
           return {"status": "skipped"}
       db.mark_job_started(payload["job_id"])
       heartbeat = asyncio.create_task(_heartbeat_loop(db, payload["job_id"]))
       try:
           # ... your work ...
           db.mark_job_done(payload["job_id"])
           return {"status": "ok"}
       except Exception as e:
           db.mark_job_error(payload["job_id"], str(e))
           return {"status": "error"}
       finally:
           heartbeat.cancel()
           locks.release(lock)
   ```
2. Add it to `WorkerSettings.functions` at the bottom of `core/jobs.py`.
3. Add a `Dispatcher.enqueue_my_new()` method; copy the pattern from
   `enqueue_resolve()`.
4. Call it from a button handler in `app.py`.
5. Add it to the Log page's "active jobs" panel (same pattern as the
   existing `_render_job_panel` calls in `render_log`).

## Adding a UI page

1. Write `def render_my_page():` in `app.py` — use existing pages as
   templates.
2. Register it in the sidebar's `pages` dict (search for `"Compose message"`
   to find the registration block).
3. If the page does long-running work, dispatch via
   `dispatcher.enqueue_...()` instead of running inline — that keeps
   Streamlit's render loop responsive and gives the user a live Jobs
   panel view of progress.

## Commit style

Not enforced, but encouraged:

```
feat: add read-receipt check job
fix: session DB lock no longer marks row as error
docs: update Hetzner deployment guide
chore: bump telethon to 1.43.1
test: cover legacy coarse read-flag migration
```

Short summary (≤72 chars), optional body explaining *why* not *what*.

## Before opening a PR

- `pytest tests/ -x` → all green
- `docker compose up -d --build` succeeds
- New tests cover new behavior
- Docs updated if configuration / deployment / architecture changed
- No secret material in the diff — run `git diff | grep -iE 'api_hash|api_id|\.session'` as a sanity check

See [CONTRIBUTING.md](../CONTRIBUTING.md) for the full PR flow.
