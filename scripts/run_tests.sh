#!/usr/bin/env bash
# Run full test suite, save output, print summary.
# Usage: nix-shell --run "bash scripts/run_tests.sh"
set -euo pipefail

OUT="/tmp/cratedigger-test-output.txt"

# JS syntax check
echo "=== JS syntax check ==="
for f in web/js/*.js; do
  node --check "$f" || { echo "FAIL: $f"; exit 1; }
done
echo "All JS files OK"
echo ""

# JS unit tests
echo "=== JS unit tests ==="
node tests/test_js_util.mjs || exit 1
node tests/test_js_decisions.mjs || exit 1
node tests/test_js_search_plan.mjs || exit 1
node tests/test_js_recents.mjs || exit 1
node tests/test_js_history.mjs || exit 1
node tests/test_js_pipeline.mjs || exit 1
node tests/test_js_pipeline_dashboard.mjs || exit 1
node tests/test_js_wrong_matches.mjs || exit 1
node tests/test_js_long_tail_console.mjs || exit 1
echo ""

# Dead-code sweep — fails fast on new vulture findings before the slow
# Python suite runs. Whitelist baseline lives at tools/vulture/whitelist.py;
# operator either deletes the dead code or regenerates the whitelist with
# the new entry (see CLAUDE.md § "Finding dead code").
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
