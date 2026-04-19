# Security policy

## Reporting a vulnerability

**Do not open a public issue for security problems.** Public disclosure
gives attackers a head-start on anyone running the software.

### Preferred channel

Open a [private security advisory on GitHub](https://github.com/viktordrukker/tgblaster/security/advisories/new).
It's encrypted in transit, visible only to the maintainer, and creates
a shared place to coordinate a fix.

### What to include

- TGBlaster version / commit hash
- Deployment mode (Docker Compose, standalone, cloud)
- Reproduction steps — the shortest path from a fresh install to the
  vulnerability
- Impact assessment (what can an attacker do?)
- Suggested fix, if you have one
- Your preferred credit line for the release notes (or "anonymous")

### What to expect

- Acknowledgement within 72 hours
- A coordinated-disclosure timeline agreed in the advisory thread
  (typically 30 days for high-severity, shorter for actively exploited)
- A patched release tagged on GitHub with release notes referencing the
  advisory ID
- Public disclosure on the advisory once the patch is out

## Supported versions

Only the latest tagged release receives security fixes. If you are
running an older tag, the fix is to upgrade.

## Scope

In scope:

- Code-execution, authentication-bypass, or privilege-escalation bugs
  in the TGBlaster codebase
- Data-exfiltration paths (session-file leak, DB dump via the web UI,
  etc.)
- Any behavior that causes recipient-facing messages outside the
  operator's intent (mis-sent DMs, wrong-contact DMs)

Out of scope:

- Vulnerabilities in upstream dependencies (report those upstream;
  TGBlaster will pick up the fix when it releases)
- Denial-of-service that requires pre-auth web-UI access — the UI is
  not designed for public exposure, see [DEPLOYMENT.md](./docs/DEPLOYMENT.md#hardening-checklist)
- Social-engineering attacks on the operator

## Security-relevant configuration reminders

- The Streamlit UI has **no built-in authentication**. Never expose it
  on the public internet without a reverse proxy enforcing basic-auth
  or OAuth (see [DEPLOYMENT.md](./docs/DEPLOYMENT.md)).
- The Telethon session file (`sessions/*.session`) is a **full account
  credential**. Anyone with read access to it can impersonate your
  account. Keep it `chmod 600`, never check it into git (the default
  `.gitignore` covers this).
- The `.env` file holds API hash + id. Same hygiene.
- Backups (see [DEPLOYMENT.md](./docs/DEPLOYMENT.md#backups)) are as
  sensitive as the live data — encrypt them at rest.
