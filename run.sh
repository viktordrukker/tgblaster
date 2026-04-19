#!/usr/bin/env bash
# One-click run script for macOS/Linux.
set -euo pipefail
cd "$(dirname "$0")"

if [ ! -f ".venv/bin/python" ]; then
    echo "[1/3] Creating virtual environment..."
    python3 -m venv .venv
fi

echo "[2/3] Installing dependencies..."
.venv/bin/pip install --quiet --upgrade pip
.venv/bin/pip install --quiet -r requirements.txt

echo "[3/3] Starting Meetup TG Blaster..."
echo
echo "Open http://localhost:8501 in your browser."
echo "Press Ctrl+C to stop."
echo
exec .venv/bin/python -m streamlit run app.py
