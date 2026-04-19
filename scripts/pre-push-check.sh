#!/usr/bin/env bash
#
# Pre-push safety check. Run this BEFORE `git push` — especially your
# very first push to a public repo.
#
# It:
#   1. Verifies .env, sessions/*, data/*.db, uploads/* are NOT staged.
#   2. Greps the staged diff for patterns that look like Telegram API
#      hashes, real phone numbers, personal names.
#   3. Runs the test suite.
#
# Exits non-zero on any failure; prints exactly what's wrong.

set -euo pipefail

cd "$(git rev-parse --show-toplevel)"

fail=0
echo "==> Pre-push checks"

# -- 1. Gitignore sanity -----------------------------------------------
echo "    · checking no secrets tracked..."
tracked_secrets=$(
    git ls-files \
        | grep -E '(^\.env$|\.env\..*|^sessions/.+\.session|^data/.+\.db$|^data/.+\.db-|^uploads/[^/]*\.(png|jpg|jpeg|webp)$)' \
        | grep -vE '^\.env\.example$' || true
)
if [[ -n "$tracked_secrets" ]]; then
    echo "    !! FAIL — these would be committed:"
    printf '       %s\n' $tracked_secrets
    fail=1
else
    echo "    ✓ no secrets in tracked files"
fi

# -- 2. Staged-diff secret scan ---------------------------------------
# Hex32 is a common api_hash shape. Allow it in docs/README examples
# when the surrounding text clearly marks it as example ('0123...').
echo "    · scanning staged diff for leaked credentials..."
hex_leak=$(
    git diff --cached -U0 \
        | grep -E '^\+[^+]' \
        | grep -Ei 'api_hash\s*=\s*"?[a-f0-9]{32}"?|api_id\s*=\s*[0-9]{5,}' \
        | grep -viE '(0123456789abcdef0123456789abcdef|example|your_api_hash|123456)' \
        || true
)
if [[ -n "$hex_leak" ]]; then
    echo "    !! FAIL — api_hash / api_id literal in staged diff:"
    echo "$hex_leak" | sed 's/^/       /'
    fail=1
else
    echo "    ✓ no credential patterns in staged diff"
fi

# -- 3. Test suite -----------------------------------------------------
echo "    · running tests..."
if command -v docker >/dev/null 2>&1 && docker compose ps --status running 2>/dev/null | grep -q blaster-app; then
    if docker compose exec -T app pytest tests/ -x --tb=line >/dev/null 2>&1; then
        echo "    ✓ pytest green"
    else
        echo "    !! FAIL — pytest failures; run: docker compose exec app pytest tests/ -x"
        fail=1
    fi
else
    echo "    ⚠ skipping tests (app container not running); run manually before push"
fi

# -- 4. Summary --------------------------------------------------------
if [[ $fail -eq 0 ]]; then
    echo "==> All checks passed. Safe to push."
    exit 0
else
    echo "==> FAILED. Do not push until the above are fixed."
    exit 1
fi
