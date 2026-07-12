#!/usr/bin/env bash
# Run the full gate suite into a unique, exact-worktree artifact.
# Usage: nix-shell --run "bash scripts/run_tests.sh"
set -euo pipefail

REPO_ROOT="$(git rev-parse --show-toplevel)"
ARTIFACT_DIR="$(python3 scripts/test_artifact.py start --worktree "$REPO_ROOT")"
OUT="$ARTIFACT_DIR/output.log"
COUNTS="$ARTIFACT_DIR/python-counts.json"

run_gates() {
  echo "=== TEST ARTIFACT ==="
  echo "Artifact directory: $ARTIFACT_DIR"
  echo "Full output: $OUT"
  echo "Structured summary: $ARTIFACT_DIR/summary.json"
  echo ""

  # JS syntax check
  echo "=== JS syntax check ==="
  for f in web/js/*.js; do
    node --check "$f" || { echo "FAIL: $f"; return 1; }
  done
  echo "All JS files OK"
  echo ""

  # JS unit tests — glob so every tests/test_js_*.mjs on disk runs; a manual
  # list drifted silently before (issue #520: test_js_grouping.mjs,
  # test_js_library.mjs, test_js_release_actions.mjs were never run).
  echo "=== JS unit tests ==="
  for f in tests/test_js_*.mjs; do
    node "$f"
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
}

# A real pipeline is synchronous: PIPESTATUS is read only after tee has closed
# and flushed the artifact. This preserves the gate status independently from
# capture failure and prevents a summary from racing an orphaned writer.
set +e
(
  set -euo pipefail
  run_gates
) 2>&1 | tee "$OUT"
pipeline_status=("${PIPESTATUS[@]}")
set -e
gate_status=${pipeline_status[0]}
capture_status=${pipeline_status[1]}
overall_status=$gate_status
if [[ "$capture_status" -ne 0 ]]; then
  overall_status=$capture_status
fi

set +e
python3 scripts/test_artifact.py finalize \
  --artifact "$ARTIFACT_DIR" \
  --worktree "$REPO_ROOT" \
  --gate-exit-code "$gate_status" \
  --capture-exit-code "$capture_status" \
  --counts-file "$COUNTS"
finalize_status=$?
set -e
rm -f "$COUNTS"
if [[ "$overall_status" -eq 0 && "$finalize_status" -ne 0 ]]; then
  overall_status=$finalize_status
fi

echo ""
echo "=== SUMMARY ==="
echo "Output saved to: $OUT"
echo ""
# Show failures/errors only
grep -E "^(ERROR|FAIL):" "$OUT" || echo "No failures."
echo ""
tail -3 "$OUT"

echo ""
echo "=== TEST ARTIFACT COMPLETE ==="
echo "Artifact directory: $ARTIFACT_DIR"
echo "Full output: $OUT"
echo "Structured summary: $ARTIFACT_DIR/summary.json"
echo "Gate exit status: $gate_status"
echo "Capture exit status: $capture_status"
echo "Exit status: $overall_status"
exit "$overall_status"
