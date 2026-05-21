#!/usr/bin/env bash
# Run the Python test suite under coverage. Writes parallel-mode data into
# build/test-coverage/ so scripts/coverage_diff.py can compare it against
# production coverage to find code that only tests exercise.
#
# Usage:
#   nix-shell --run "bash scripts/run_tests_with_coverage.sh"
#
# Output:
#   build/test-coverage/.coverage         ← combined data file
#   /tmp/cratedigger-test-output.txt      ← test output (same as run_tests.sh)

set -euo pipefail

cd "$(dirname "$0")/.."

DATA_DIR="build/test-coverage"
mkdir -p "$DATA_DIR" build

# Parallel mode handles subprocesses our tests spawn (e.g. dispatch slices
# that exec import_one.py). COVERAGE_FILE pins the directory; the .pth shim
# attaches subprocesses via COVERAGE_PROCESS_START.
export COVERAGE_FILE="$PWD/$DATA_DIR/.coverage"
export COVERAGE_PROCESS_START="$PWD/.coveragerc"

# Wipe stale data — coverage combine MERGES rather than replaces.
rm -f "$DATA_DIR"/.coverage*

OUT="/tmp/cratedigger-test-output.txt"

echo "=== JS syntax check ==="
for f in web/js/*.js; do
  node --check "$f" || { echo "FAIL: $f"; exit 1; }
done
echo "All JS files OK"
echo

echo "=== JS unit tests ==="
node tests/test_js_util.mjs || exit 1
node tests/test_js_decisions.mjs || exit 1
node tests/test_js_search_plan.mjs || exit 1
node tests/test_js_recents.mjs || exit 1
node tests/test_js_history.mjs || exit 1
node tests/test_js_wrong_matches.mjs || exit 1
echo

echo "=== Python tests under coverage ==="
coverage run --parallel-mode --rcfile=.coveragerc \
  -m unittest discover tests -v 2>&1 | tee "$OUT"

echo
echo "=== coverage combine ==="
(cd "$DATA_DIR" && coverage combine --rcfile=../../.coveragerc) || true

echo
echo "=== SUMMARY ==="
echo "Test output: $OUT"
echo "Coverage data: $DATA_DIR/.coverage"
echo
grep -E "^(ERROR|FAIL):" "$OUT" || echo "No failures."
echo
tail -3 "$OUT"
