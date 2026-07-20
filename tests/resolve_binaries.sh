#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
CONFIG_PATH="$REPO_ROOT/tests/required_binaries.json"
export ABXPKG_LIB_DIR="${ABXPKG_LIB_DIR:-$REPO_ROOT/.abxpkg/lib}"

json_to_exports() {
  uv run --project "$REPO_ROOT" python -c \
    'import json, shlex, sys; values = json.load(sys.stdin); print("\n".join(f"export {key}={shlex.quote(str(value))}" for key, value in values.items()))'
}

resolve_section() {
  uv run --project "$REPO_ROOT" --with abxpkg==1.11.266 abxpkg env --install --json \
    --deps-from="$CONFIG_PATH:$1" \
    | json_to_exports
}

if [[ "$#" -eq 0 ]]; then
  echo "usage: $0 CONFIG_SECTION [...]" >&2
  exit 2
fi

for section in "$@"; do
  section_exports="$(resolve_section "$section")"
  eval "$section_exports"
  printf '%s\n' "$section_exports"

  if [[ "$section" == "bridge_binaries" ]]; then
    postgres_bindir="$("$ABXBUS_PG_CONFIG_BIN" --bindir)"
    export PATH="$postgres_bindir:$PATH"
    postgres_exports="$(resolve_section postgres_binaries)"
    eval "$postgres_exports"
    printf '%s\n' "$postgres_exports"
  fi

  if [[ "$section" == "cross_runtime_binaries" || "$section" == "python_binary" ]]; then
    pythonpath_exports="$(
      uv run --project "$REPO_ROOT" python -c \
        'import os, shlex, site, sys; value = os.pathsep.join([sys.argv[1], *site.getsitepackages()]); print(f"export ABXBUS_PYTHONPATH={shlex.quote(value)}")' \
        "$REPO_ROOT"
    )"
    eval "$pythonpath_exports"
    printf '%s\n' "$pythonpath_exports"
  fi
done
