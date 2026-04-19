#!/usr/bin/env bash
#
# Nightly backup for a production TGBlaster deployment.
#
# What it does:
#   1. `sqlite3 .backup` against the live state.db (WAL-safe — doesn't
#      need a stopped app).
#   2. tars the session files + state DB + uploads together.
#   3. GPG-encrypts the archive (symmetric, passphrase from env).
#   4. Uploads to a remote via rsync (or rclone — uncomment the line).
#   5. Keeps the last 14 daily snapshots locally.
#
# Run from cron as root (docker group member):
#   0 3 * * *   /opt/tgblaster/docs/examples/backup.sh >> /var/log/tgblaster-backup.log 2>&1
#
# Required env (put in /etc/tgblaster-backup.env, chmod 600):
#   TGBLASTER_BACKUP_PASSPHRASE=<long random string>
#   TGBLASTER_BACKUP_REMOTE=user@backup.example.com:/var/backups/tgblaster
#   TGBLASTER_COMPOSE_DIR=/opt/tgblaster    # where docker-compose.prod.yml lives
#
# Restore procedure is documented in docs/DEPLOYMENT.md#backups.

set -euo pipefail

# --- config ----------------------------------------------------------
ENV_FILE="${TGBLASTER_BACKUP_ENV:-/etc/tgblaster-backup.env}"
[[ -f "$ENV_FILE" ]] && source "$ENV_FILE"

: "${TGBLASTER_BACKUP_PASSPHRASE:?set TGBLASTER_BACKUP_PASSPHRASE}"
: "${TGBLASTER_COMPOSE_DIR:?set TGBLASTER_COMPOSE_DIR}"
: "${TGBLASTER_BACKUP_REMOTE:?set TGBLASTER_BACKUP_REMOTE}"

LOCAL_DIR="/var/backups/tgblaster"
KEEP_DAYS=14

# --- paths -----------------------------------------------------------
cd "$TGBLASTER_COMPOSE_DIR"
timestamp="$(date -u +'%Y%m%d-%H%M%S')"
staging="$(mktemp -d)"
trap 'rm -rf "$staging"' EXIT

# --- 1. hot-backup the DB -------------------------------------------
# Use `docker compose run` against the app image so sqlite3 is
# available; the live app keeps running through this.
docker compose -f docker-compose.prod.yml exec -T app \
    sqlite3 /app/data/state.db ".backup '/app/data/state.backup.db'"

# --- 2. snapshot volumes --------------------------------------------
# Named volumes live under /var/lib/docker/volumes/ on Linux hosts.
for vol in tgblaster-data tgblaster-sessions tgblaster-uploads; do
    src="/var/lib/docker/volumes/${TGBLASTER_COMPOSE_DIR##*/}_${vol}/_data"
    cp -a "$src" "$staging/$vol"
done

# Replace the live state.db with the hot-backup we just made.
if [[ -f "$staging/tgblaster-data/state.backup.db" ]]; then
    mv -f "$staging/tgblaster-data/state.backup.db" "$staging/tgblaster-data/state.db"
fi

# --- 3. package + encrypt -------------------------------------------
archive="$LOCAL_DIR/tgblaster-${timestamp}.tar.gz.gpg"
mkdir -p "$LOCAL_DIR"
tar -czf - -C "$staging" . | \
    gpg --batch --yes --passphrase "$TGBLASTER_BACKUP_PASSPHRASE" \
        --symmetric --cipher-algo AES256 -o "$archive"

# --- 4. ship offsite ------------------------------------------------
rsync -aP "$archive" "$TGBLASTER_BACKUP_REMOTE/"
# Alternative: rclone copy "$archive" "remote:tgblaster-backup/"

# --- 5. prune local -------------------------------------------------
find "$LOCAL_DIR" -type f -name 'tgblaster-*.tar.gz.gpg' -mtime +${KEEP_DAYS} -delete

echo "OK ${timestamp} → ${archive} (size: $(du -h "$archive" | cut -f1))"
