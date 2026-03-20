#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd -- "$SCRIPT_DIR/.." && pwd)"
cd "$ROOT_DIR"

if [[ -x /usr/bin/chromium ]]; then
  echo "[perf:browser] using system chromium executable: /usr/bin/chromium"
  PW_CHROMIUM_EXECUTABLE_PATH=/usr/bin/chromium \
    npx --yes --package=playwright -c 'PW_BIN="$(command -v playwright)"; PW_NODE_MODULES="$(cd "$(dirname "$PW_BIN")/.." && pwd)"; NODE_PATH="$PW_NODE_MODULES" playwright test tests/performance.browser.spec.cjs --config=playwright.perf.config.cjs --project=browser-perf --workers=1 --reporter=line --output=/tmp/abxbus-playwright-results'
else
  echo "[perf:browser] /usr/bin/chromium not found; using Playwright-managed chromium"
  npx --yes --package=playwright -c 'PW_BIN="$(command -v playwright)"; PW_NODE_MODULES="$(cd "$(dirname "$PW_BIN")/.." && pwd)"; NODE_PATH="$PW_NODE_MODULES" playwright test tests/performance.browser.spec.cjs --config=playwright.perf.config.cjs --project=browser-perf --workers=1 --reporter=line --output=/tmp/abxbus-playwright-results'
fi
