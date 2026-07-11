#!/usr/bin/env bash
# Run full test suite, save output, print summary.
# Usage: nix-shell --run "bash scripts/run_tests.sh"
set -euo pipefail

OUT="/tmp/cratedigger-test-output.txt"

# Truncate up front: an early gate failure (JS/Ruff/vulture) exits before the
# Python tee ever writes $OUT, leaving the PREVIOUS run's green unittest
# output behind — "grep the output file" then reads as a false pass
# (bit the 2026-07-11 honest-metrics session mid-review).
: > "$OUT"

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
python3 -m unittest discover -s tests -t . -v 2>&1 | tee "$OUT"

echo ""
echo "=== SUMMARY ==="
echo "Output saved to: $OUT"
echo ""
# Show failures/errors only
grep -E "^(ERROR|FAIL):" "$OUT" || echo "No failures."
echo ""
tail -3 "$OUT"
