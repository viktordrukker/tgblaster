# Deployment

TGBlaster is designed for **single-operator, single-Telegram-account**
deployments. One install = one account. There is no multi-tenant
mode; it would conflict with Telethon's session model.

Pick the path that matches your setup:

- **[A. Local Docker](#a-local-docker)** — your laptop or dev machine
- **[B. Hetzner VPS](#b-hetzner-vps-or-any-linux-vps)** — €4/mo, ~20 min setup
- **[C. Bare metal systemd](#c-bare-metal--systemd)** — no Docker
- **[D. Cloud PaaS](#d-cloud-paas-flyio-railway-render-heroku)** — one-click-ish but caveats

All production paths go through the **[hardening checklist](#hardening-checklist)** at the end.

---

## A. Local Docker

This is the quick-start from the README. Don't expose to the public
internet.

```bash
git clone https://github.com/viktordrukker/tgblaster.git
cd tgblaster
cp .env.example .env      # edit: TG_API_ID, TG_API_HASH
docker compose up -d --build
```

UI at <http://localhost:8501>. Stop with `docker compose down`.

---

## B. Hetzner VPS (or any Linux VPS)

Target: Ubuntu 22.04 on a CX22 instance (2 vCPU / 4 GB / 40 GB NVMe, €4-ish).

### 1. Provision

- Spin up an Ubuntu 22.04 instance.
- Point a DNS `A` record to its IP (e.g. `tgblaster.yourdomain.com`).
- SSH in as root.

### 2. Harden SSH + firewall

```bash
# Update + essentials
apt-get update && apt-get install -y docker.io docker-compose-v2 ufw fail2ban

# UFW: only SSH + HTTP + HTTPS
ufw default deny incoming
ufw default allow outgoing
ufw allow 22/tcp
ufw allow 80/tcp
ufw allow 443/tcp
ufw --force enable

systemctl enable --now fail2ban docker
```

### 3. Deploy TGBlaster

```bash
git clone https://github.com/viktordrukker/tgblaster.git /opt/tgblaster
cd /opt/tgblaster
cp .env.example .env
```

Edit `.env`:

```ini
TG_API_ID=<from my.telegram.org>
TG_API_HASH=<from my.telegram.org>
TG_SESSION_NAME=tg_session

# Caddy basic-auth: generate with `docker run --rm caddy:2-alpine caddy hash-password`
TGBLASTER_DOMAIN=tgblaster.yourdomain.com
TGBLASTER_BASIC_AUTH_USER=admin
TGBLASTER_BASIC_AUTH_HASH='$2a$14$aBcD...'   # paste the output of caddy hash-password
```

Generate the bcrypt hash for your chosen password:

```bash
docker run --rm caddy:2-alpine caddy hash-password
# paste password twice → copy the hash into TGBLASTER_BASIC_AUTH_HASH
```

Start:

```bash
docker compose -f docker-compose.prod.yml up -d
```

### 4. First-time Telegram login

Streamlit's UI lives behind Caddy basic-auth. The first login needs a
manual code entry — Telegram sends a code to the phone you use.

1. Open `https://tgblaster.yourdomain.com` in a browser.
2. Pass basic-auth.
3. Step 1 · Accounts → enter phone → receive code via Telegram → paste.
4. If 2FA is enabled, paste that too.

From here the session persists in the `tgblaster-sessions` Docker
volume. You don't log in again unless you `docker volume rm` it.

### 5. Backups

```bash
cp docs/examples/backup.sh /opt/tgblaster/scripts/backup.sh
chmod +x /opt/tgblaster/scripts/backup.sh

# Create the backup env file
cat > /etc/tgblaster-backup.env <<'EOF'
TGBLASTER_BACKUP_PASSPHRASE='<long random string, 40+ chars>'
TGBLASTER_BACKUP_REMOTE='user@backup.example.com:/var/backups/tgblaster'
TGBLASTER_COMPOSE_DIR=/opt/tgblaster
EOF
chmod 600 /etc/tgblaster-backup.env

# Cron: nightly at 03:00 UTC
echo '0 3 * * * root /opt/tgblaster/scripts/backup.sh >> /var/log/tgblaster-backup.log 2>&1' \
    > /etc/cron.d/tgblaster-backup
```

### 6. Operational commands

```bash
# Logs (follow)
docker compose -f docker-compose.prod.yml logs -f app worker

# Update to latest release
cd /opt/tgblaster
git fetch --tags && git checkout v0.2.0      # or whatever
docker compose -f docker-compose.prod.yml up -d --build

# Restart just the worker
docker compose -f docker-compose.prod.yml restart worker

# Shell into the app
docker compose -f docker-compose.prod.yml exec app bash
```

### 7. Restore from backup

```bash
docker compose -f docker-compose.prod.yml down

# Decrypt + unpack
gpg --decrypt --passphrase "$TGBLASTER_BACKUP_PASSPHRASE" \
    tgblaster-20260419-030000.tar.gz.gpg | tar -xzf - -C /tmp/restore

# Stop, wipe volumes, restore
for vol in tgblaster-data tgblaster-sessions tgblaster-uploads; do
    docker volume rm tgblaster_$vol
    docker volume create tgblaster_$vol
    docker run --rm -v tgblaster_$vol:/dst -v /tmp/restore/$vol:/src alpine \
        sh -c 'cp -a /src/. /dst/'
done

docker compose -f docker-compose.prod.yml up -d
```

Rehearse this at least once before you need it.

---

## C. Bare metal / systemd

For users who don't want Docker. More work upfront, zero surprises
later.

```bash
# 1. Python + Redis
apt-get install -y python3.11 python3.11-venv redis-server
systemctl enable --now redis-server

# 2. App
git clone https://github.com/viktordrukker/tgblaster.git /opt/tgblaster
cd /opt/tgblaster
python3.11 -m venv .venv
./.venv/bin/pip install -r requirements.txt
cp .env.example .env   # edit

# 3. Systemd units (see contents below)
# ... put the two unit files in /etc/systemd/system/ ...
systemctl daemon-reload
systemctl enable --now tgblaster-app.service tgblaster-worker.service
```

**`/etc/systemd/system/tgblaster-app.service`:**

```ini
[Unit]
Description=TGBlaster Streamlit UI
After=network.target redis-server.service
Requires=redis-server.service

[Service]
Type=simple
User=tgblaster
WorkingDirectory=/opt/tgblaster
EnvironmentFile=/opt/tgblaster/.env
Environment="REDIS_URL=redis://127.0.0.1:6379/0"
ExecStart=/opt/tgblaster/.venv/bin/streamlit run app.py \
    --server.address 127.0.0.1 --server.port 8501
Restart=on-failure
RestartSec=5

[Install]
WantedBy=multi-user.target
```

**`/etc/systemd/system/tgblaster-worker.service`:**

```ini
[Unit]
Description=TGBlaster arq worker
After=network.target redis-server.service
Requires=redis-server.service

[Service]
Type=simple
User=tgblaster
WorkingDirectory=/opt/tgblaster
EnvironmentFile=/opt/tgblaster/.env
Environment="REDIS_URL=redis://127.0.0.1:6379/0"
ExecStart=/opt/tgblaster/.venv/bin/python -m arq core.jobs.WorkerSettings
Restart=on-failure
RestartSec=5
TimeoutStopSec=30

[Install]
WantedBy=multi-user.target
```

Then run Caddy separately (from distro package), proxy to `127.0.0.1:8501`.

---

## D. Cloud PaaS (Fly.io, Railway, Render, Heroku)

Works, but with caveats:

- **Session file persistence** — Telethon's session is a SQLite file
  that cannot live on an ephemeral filesystem. You need a persistent
  volume. Fly.io has `fly volumes`; Railway has `Volume`; Render has
  `Persistent Disk`. Budget €1–5/mo on top of the base cost.
- **Redis** — use the platform's Redis add-on (Upstash free tier works)
  or deploy Redis as a sidecar.
- **Two services** — the UI and worker are separate Docker containers.
  PaaS platforms typically bill per service.
- **No `docker-compose.prod.yml`** — you'll define the services in the
  platform's own format.

Net: Fly.io is the cheapest at ~$3–8/mo for a small deployment. But the
VPS path above is simpler and about the same cost. Only go PaaS if you
specifically don't want to manage a VPS.

---

## Hardening checklist

Before exposing TGBlaster on a public hostname, verify each item:

- [ ] HTTPS is enforced (Caddy redirects HTTP → HTTPS by default)
- [ ] Basic-auth credentials are strong (`openssl rand -base64 24`-ish)
- [ ] `ufw status` shows only 22/80/443 open
- [ ] `fail2ban-client status` is running with sshd jail active
- [ ] `.env` is `chmod 600`, owned by root
- [ ] Session file (`sessions/*.session`) is `chmod 600`
- [ ] `docker compose logs` shows no startup errors
- [ ] First login succeeded and the session persists across restart
- [ ] A dry-run test-fire to yourself works
- [ ] Backup cron ran at least once and a restore was rehearsed
- [ ] You've read [DISCLAIMER.md](../DISCLAIMER.md) and the intended
      audience is consent-based

## Things this deployment does NOT include

- **Log aggregation** — logs are in Docker's JSON driver; ship them
  to Loki / Datadog / whatever if you want retention.
- **Metrics** — no Prometheus endpoint. Add one via a sidecar if you
  want dashboards.
- **Multi-account auto-failover** — one TG account per deployment. The
  UI supports adding multiple accounts but that's a manual
  operator-level switch, not an automatic fallback.
- **Horizontal scaling** — single-node by design. Sending from two
  workers in parallel to the same campaign would fight over the
  per-campaign Redis lock (correct) but also risk duplicating the
  session's peer cache. Don't.
