# ---------- builder ----------
FROM python:3.11-slim AS builder

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

# System deps needed to build a few wheels cleanly.
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --prefix=/install -r requirements.txt


# ---------- runtime ----------
FROM python:3.11-slim AS runtime

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app

# Non-root user for a little extra safety.
RUN groupadd --gid 1000 app && useradd --uid 1000 --gid app --shell /bin/bash --create-home app

# Minimal runtime libs (libstdc++ already in slim; nothing extra needed).
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    tini \
    && rm -rf /var/lib/apt/lists/*

# Copy installed Python packages from the builder.
COPY --from=builder /install /usr/local

WORKDIR /app
COPY --chown=app:app . /app

# Writable state dirs. In compose we mount these as volumes for persistence.
RUN mkdir -p /app/sessions /app/data /app/uploads \
    && chown -R app:app /app/sessions /app/data /app/uploads

USER app

EXPOSE 8501

# tini as PID1 for proper signal handling (Ctrl+C on compose).
ENTRYPOINT ["/usr/bin/tini", "--"]

# Default command: run Streamlit. Overridden by the worker service.
CMD ["streamlit", "run", "app.py", \
     "--server.headless=true", \
     "--server.port=8501", \
     "--server.address=0.0.0.0", \
     "--browser.gatherUsageStats=false"]

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD curl -f http://localhost:8501/_stcore/health || exit 1
