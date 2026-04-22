# Changelog

All notable changes to TGBlaster are documented here. Format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project
uses [Semantic Versioning](https://semver.org/).

## [0.1.1] — 2026-04-23

### Security

- **UI password gate.** Optional `TGBLASTER_UI_PASSWORD` environment
  variable renders a password prompt before any other page. Unset by
  default. Compared with `secrets.compare_digest` (constant-time).
- **Loopback-only port binding.** `docker-compose.yml` now binds
  `127.0.0.1:8501:8501` instead of `0.0.0.0`. Anyone with network
  access to the host could previously drive the logged-in Telegram
  session. Remote access goes through the prod compose (Caddy
  basic-auth) or `TGBLASTER_UI_PASSWORD`.
- **Error redaction.** New `safe_error()` logs the full exception
  server-side with a short ref id and shows a redacted banner in the
  UI. Replaces every `st.error(f"... {e}")` that could leak `api_id`,
  session paths, or local stack frames.
- **Upload path-traversal guard.** `_safe_upload_path()` resolves
  every upload under `UPLOADS_DIR` and rejects paths that escape it.
- **CSV formula-injection guard.** Campaign-log CSV downloads route
  through `sanitize_for_csv_export()`, which prefixes `'` to any cell
  starting with `=`, `+`, `-`, `@`, tab, or CR so spreadsheet software
  does not execute the content.
- **SQL identifier validation.** `bulk_update_contacts` now rejects
  column names that are not plain identifiers; `LIMIT` is
  parameterised (defence-in-depth against a future allowlist typo
  becoming SQLi).
- **.env permission warning.** Startup warns when `.env` is
  group/other-readable on POSIX. Non-fatal but visible in logs.
- **CI supply-chain hardening.** GitHub Actions pinned to full commit
  SHAs; workflow-level `permissions: contents: read`; Dependabot
  enabled for pip, github-actions, and docker (weekly minor/patch).
- **Pillow 11.0.0 → 11.3.0** for the ImageMath / TIFF / BMP fix range.

### Fixed

- **Dry-run no longer taints real-run state.** Send-to-self dry-run
  used to write `send_log` rows for real contact ids and flip the
  campaign lifecycle; the next real run then skipped those contacts
  as "already sent" (silent delivery drop). Dry-run now bypasses
  `reserve_send` / `_safe_confirm` entirely and preserves the
  campaign's existing state.
- **`PeerFloodError` is per-peer.** Previously stopped the whole
  campaign on the first flagged recipient. Now skips the individual
  peer and only stops after 3 distinct peers raise `PeerFlood` in the
  same run — the actual account-level rate-limit signal.
- **Longer DB retry backoff.** `_retry_on_lock` goes from linear × 4
  attempts (~3 s total) to exponential × 8 attempts (~50 s total) so
  concurrent Telethon session writes + WSL2 bind-mount jitter no
  longer surface spurious `database is locked` errors in the UI.

### Added

- **Per-contact send-log reset.** Log tab exposes a picker that lets
  an operator return specific contacts to the queue by deleting their
  `send_log` rows. Legal from any campaign state; a running campaign
  needs a Pause → Resume cycle to pick them up because the
  already-sent cache is built in memory at run start.

## [0.1.0] — 2026-04-20

Initial public release.

[0.1.1]: https://github.com/viktordrukker/tgblaster/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/viktordrukker/tgblaster/releases/tag/v0.1.0
