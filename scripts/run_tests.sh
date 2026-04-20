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
echo ""

# Python tests
echo "=== Python tests ==="
python3 -m unittest discover tests -v 2>&1 | tee "$OUT"

echo ""
echo "=== SUMMARY ==="
echo "Output saved to: $OUT"
echo ""
# Show failures/errors only
grep -E "^(ERROR|FAIL):" "$OUT" || echo "No failures."
echo ""
tail -3 "$OUT"
