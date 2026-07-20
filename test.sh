#!/usr/bin/env bash
set -euo pipefail

uv run prek run --all-files

eval "$(tests/resolve_binaries.sh bridge_binaries cross_runtime_binaries)"

# Run Python and TypeScript test phases sequentially to avoid cross-runtime
# resource contention that can cause performance-threshold flakes.
uv run pytest

(
  cd abxbus-ts
  ts_test_file_count=0
  while IFS= read -r test_file; do
    ts_test_file_count=$((ts_test_file_count + 1))
    NODE_OPTIONS='--expose-gc' "$ABXBUS_NODE_BIN" --expose-gc --test --import tsx "$test_file"
  done < <(find tests -type f -name '*.test.ts' | sort)
  if [[ "$ts_test_file_count" -eq 0 ]]; then
    echo "No TypeScript test files found in abxbus-ts/tests/" >&2
    exit 1
  fi
)

shopt -s nullglob
python_example_pids=()
for example_file in examples/*.py; do
  PYTHONPATH="$ABXBUS_PYTHONPATH" timeout 120 "$ABXBUS_PYTHON_BIN" "$example_file" &
  python_example_pids+=("$!")
done
for pid in "${python_example_pids[@]}"; do
  wait "$pid"
done

(
  cd abxbus-ts
  shopt -s nullglob
  ts_example_pids=()
  for example_file in examples/*.ts; do
    timeout 120 "$ABXBUS_NODE_BIN" --import tsx "$example_file" &
    ts_example_pids+=("$!")
  done
  for pid in "${ts_example_pids[@]}"; do
    wait "$pid"
  done
)

# Perf suites are expensive and can push total runtime well past the main CI budget.
# Run them explicitly with RUN_PERF=1.
if [[ "${RUN_PERF:-0}" == "1" ]]; then
  PYTHONPATH="$ABXBUS_PYTHONPATH" "$ABXBUS_PYTHON_BIN" tests/performance_runtime.py
  (
    cd abxbus-ts
    uv run --project .. abxpkg run --install --binproviders=pnpm pnpm run perf
  )
else
  echo "Skipping perf suites (set RUN_PERF=1 to include them)."
fi
