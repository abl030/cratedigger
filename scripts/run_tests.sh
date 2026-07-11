#!/usr/bin/env bash
# Run the full gate suite into a unique, exact-worktree artifact.
# Usage: nix-shell --run "bash scripts/run_tests.sh"
set -euo pipefail

REPO_ROOT="$(git rev-parse --show-toplevel)"
ARTIFACT_DIR="$(python3 scripts/test_artifact.py start --worktree "$REPO_ROOT")"
OUT="$ARTIFACT_DIR/output.log"
COUNTS="$ARTIFACT_DIR/python-counts.json"

# Capture every gate, not only unittest output. Process substitution keeps
# each gate's real exit code visible to `set -euo pipefail`.
exec > >(tee "$OUT") 2>&1

finalize_run() {
  local status=$?
  local finalize_status=0
  trap - EXIT
  set +e
  python3 scripts/test_artifact.py finalize \
    --artifact "$ARTIFACT_DIR" \
    --worktree "$REPO_ROOT" \
    --exit-code "$status" \
    --counts-file "$COUNTS"
  finalize_status=$?
  rm -f "$COUNTS"
  if [[ "$status" -eq 0 && "$finalize_status" -ne 0 ]]; then
    status=$finalize_status
  fi
  echo ""
  echo "=== TEST ARTIFACT COMPLETE ==="
  echo "Artifact directory: $ARTIFACT_DIR"
  echo "Full output: $OUT"
  echo "Structured summary: $ARTIFACT_DIR/summary.json"
  echo "Exit status: $status"
  exit "$status"
}
trap finalize_run EXIT

echo "=== TEST ARTIFACT ==="
echo "Artifact directory: $ARTIFACT_DIR"
echo "Full output: $OUT"
echo "Structured summary: $ARTIFACT_DIR/summary.json"
echo ""

# JS syntax check
echo "=== JS syntax check ==="
for f in web/js/*.js; do
  node --check "$f" || { echo "FAIL: $f"; exit 1; }
done
echo "All JS files OK"
echo ""

# JS unit tests — glob so every tests/test_js_*.mjs on disk runs; a manual
# list drifted silently before (issue #520: test_js_grouping.mjs,
# test_js_library.mjs, test_js_release_actions.mjs were never run).
echo "=== JS unit tests ==="
for f in tests/test_js_*.mjs; do
  node "$f" || exit 1
done
echo ""

# Production-liveness sweep — source-local Ruff F401/F811 runs first, then
# aggregate vulture. Vulture's baseline lives at tools/vulture/whitelist.py;
# intentional import exports use exact redundant aliases (CLAUDE.md §
# "Finding dead code").
echo "=== Dead-code sweep ==="
bash "$(dirname "$0")/find_dead_code.sh"
echo ""

# Python tests
echo "=== Python tests ==="
python3 scripts/test_artifact.py run-python --counts-file "$COUNTS"

echo ""
echo "=== SUMMARY ==="
echo "Output saved to: $OUT"
echo ""
# Show failures/errors only
grep -E "^(ERROR|FAIL):" "$OUT" || echo "No failures."
echo ""
tail -3 "$OUT"
