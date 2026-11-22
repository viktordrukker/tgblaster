# TGBlaster — Telegram broadcast: web UI (docker compose up -d) + CLI.
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    BLASTER_HOME=/app

RUN apt-get update \
    && apt-get install -y --no-install-recommends tini \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --gid 1000 app \
    && useradd --uid 1000 --gid app --create-home app

WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY blaster ./blaster
RUN mkdir -p sessions data campaigns && chown -R app:app /app
USER app
EXPOSE 8501

# tini forwards SIGINT/SIGTERM so `docker stop` lets the current send
# finish and the journal stay consistent.
ENTRYPOINT ["/usr/bin/tini", "--", "python", "-m", "blaster"]
CMD ["--help"]
