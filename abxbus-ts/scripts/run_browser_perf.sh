#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd -- "$SCRIPT_DIR/.." && pwd)"
REPO_ROOT="$(cd -- "$ROOT_DIR/.." && pwd)"
cd "$REPO_ROOT"

export ABXPKG_LIB_DIR="${ABXPKG_LIB_DIR:-$REPO_ROOT/.abxpkg/lib}"
eval "$(tests/resolve_binaries.sh browser_binaries)"

cd "$ROOT_DIR"
echo "[perf:browser] using abxpkg-resolved chromium: $ABXBUS_CHROMIUM_BIN"
PW_CHROMIUM_EXECUTABLE_PATH="$ABXBUS_CHROMIUM_BIN" \
  "$ABXBUS_PLAYWRIGHT_BIN" test tests/performance.browser.spec.cjs --config=playwright.perf.config.cjs --project=browser-perf --workers=1 --reporter=line --output=/tmp/abxbus-playwright-results
