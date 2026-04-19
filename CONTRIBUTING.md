# Contributing to TGBlaster

Thanks for considering a contribution. Small docs fixes are as
welcome as features — no PR is too small.

## Before you open an issue or PR

1. **Read [DISCLAIMER.md](./DISCLAIMER.md)**. TGBlaster is for
   consent-based messaging. Issues or features that help with cold
   outreach / scraping / opt-out evasion will be closed.
2. Search existing [issues](https://github.com/viktordrukker/tgblaster/issues)
   and [discussions](https://github.com/viktordrukker/tgblaster/discussions) —
   someone may already be on it.
3. **Security bugs**: do NOT file a public issue. See [SECURITY.md](./SECURITY.md).

## Filing an issue

Use the templates — they're in `.github/ISSUE_TEMPLATE/`. GitHub
will offer them when you click "New issue":

- **🐛 Bug report** — something broke
- **✨ Feature request** — something's missing
- **💬 Question** — redirected to Discussions (use Discussions for
  usage questions and design debates; keep issues for work)

A good bug report includes: TGBlaster version or commit hash,
deployment mode (Docker / standalone / cloud), Python + Docker
versions, a minimal reproduction, and relevant logs.

## Submitting a PR

### 1. Fork and branch

```bash
git clone git@github.com:<you>/tgblaster.git
cd tgblaster
git checkout -b fix/database-lock-retry-budget
```

Branch naming — no hard rule, but `<type>/<short-slug>` works well:
`fix/…`, `feat/…`, `docs/…`, `test/…`, `chore/…`.

### 2. Make the change

- Keep the change focused. One PR = one concern.
- Write tests for new behavior — we run `pytest tests/ -x` on every PR.
- Update docs when you change configuration, deployment, or
  architecture.
- Match the existing style: short lines, concrete names,
  comment-the-why-not-the-what.

### 3. Test locally

```bash
docker compose exec app pytest tests/ -x
```

All 140+ tests must stay green. New behavior gets new tests.

### 4. Commit message style

Conventional-commit-inspired, not enforced:

```
feat: add read-receipt check job
fix: session DB lock no longer marks row as error
docs: update Hetzner deployment guide
test: cover legacy coarse read-flag migration
chore: bump telethon to 1.43.1
```

Short summary (≤ 72 chars), blank line, optional body describing
*why* (not *what* — the diff shows that).

### 5. Open the PR

Use the template. The checklist items matter — they're the same ones
the reviewer will verify.

## How issues and PRs are triaged

The maintainer's routine:

### Issues

| First 48h | Action |
|---|---|
| Triage | Assign a label: `bug`, `enhancement`, `question`, `docs`, `security`, `wontfix`, `needs-info` |
| Reproduce (bugs) | Try to repro locally; comment findings; ask for missing info if stuck |
| Discuss (features) | Decide if this fits the consent-based scope + architecture; label `accepted` or `wontfix` with a reason |
| Security | Moved to private advisory thread; not discussed publicly |

### PRs

| Stage | Expectation |
|---|---|
| CI green | Non-negotiable. If CI fails for reasons unrelated to your change, mention it — sometimes flakes happen |
| Review | Within a week for a maintainer response. Complex changes may iterate 2–3 rounds |
| Merge | Squash-merge with the PR title as the commit subject |
| Release | Usually batched; major fixes get a patch release within a few days |

### Stale policy

An issue with no activity for 60 days gets an auto-comment asking for
an update. After another 30 days it's auto-closed. Closed-stale
issues can always be reopened with new information.

## What's in scope

- Fixing bugs in the existing behavior
- Improving reliability (DB locks, FloodWait, session contention)
- Adding features that help **opt-in / consent-based** flows:
  read-receipts, better deduplication, campaign analytics, UX
- Improving deployment story (cloud, VPS, backup, monitoring)
- Docs, especially in non-English languages

## What's out of scope

- Multi-account auto-rotation to evade Telegram's limits
- Features that help with cold outreach, scraping, or opt-out evasion
- Anything that requires Telegram Premium as a hard dependency
  (Telethon + user accounts already can't access some Premium APIs)
- A public SaaS deployment story — the single-tenant model is
  deliberate; each install = one Telegram account

## License

By contributing, you agree that your contributions will be licensed
under the MIT license (see [LICENSE](./LICENSE)).
