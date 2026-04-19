<!--
Thanks for the PR! Fill out the sections below. The more specific, the
faster the review.
-->

## Summary

<!-- One paragraph: what changes, why it matters, what it does NOT change. -->

## Linked issue

<!-- Closes #123 / Refs #456. If this PR doesn't have a tracking issue, explain why here. -->

## Type of change

- [ ] Bug fix (restores intended behavior)
- [ ] New feature (consent-based scope — see CONTRIBUTING.md)
- [ ] Docs
- [ ] Refactor (no user-visible change)
- [ ] CI / build / release infra

## How I tested

<!--
Be specific. E.g.:
  - Added 3 unit tests in tests/test_database.py covering the new branch.
  - Ran `docker compose exec app pytest tests/ -x` — 143 pass.
  - Live-tested the UI flow in a 5-contact dry-run to myself.
-->

## Checklist

- [ ] `pytest tests/ -x` passes locally
- [ ] New behavior has test coverage
- [ ] Docs updated if config / deployment / architecture changed
- [ ] No secret material in the diff (`git diff | grep -iE 'api_hash|api_id|\.session'` empty)
- [ ] I've read [DISCLAIMER.md](../blob/main/DISCLAIMER.md) — this change does not help cold outreach, scraping, or opt-out evasion

## Screenshots / logs (if UI or behavior changed)

<!-- Drag images here, or paste a fenced log block. Redact real data. -->
