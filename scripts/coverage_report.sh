#!/usr/bin/env bash
# Combine + report production coverage data.
#
# Production cratedigger services write parallel-mode .coverage.<host>.<pid>.<rand>
# files into services.cratedigger.coverage.dataDir (default
# /var/lib/cratedigger/coverage). This script:
#
#   1. Copies them from doc2 (or wherever) to ./build/prod-coverage/
#   2. Runs `coverage combine` to merge them into a single .coverage file
#   3. Emits a text report + HTML report (./build/coverage-html/)
#
# Usage:
#   nix-shell --run "bash scripts/coverage_report.sh"          # local data
#   nix-shell --run "bash scripts/coverage_report.sh doc2"     # ssh + rsync from doc2
#
# Combine with the test coverage diff to find code only tests exercise:
#   nix-shell --run "bash scripts/run_tests.sh"                # populates build/test-coverage/
#   nix-shell --run "bash scripts/coverage_report.sh doc2"
#   nix-shell --run "python3 scripts/coverage_diff.py"

set -euo pipefail

cd "$(dirname "$0")/.."

SRC_HOST="${1:-}"
SRC_DIR="/var/lib/cratedigger/coverage"
DEST_DIR="build/prod-coverage"

mkdir -p "$DEST_DIR" build

if [[ -n "$SRC_HOST" ]]; then
  echo "=== Pulling coverage data from ${SRC_HOST}:${SRC_DIR} ==="
  # Use sudo on the remote side because the dir is owned by cratedigger's user.
  # rsync's --rsync-path is the canonical way to escalate on the remote.
  rsync -a --info=stats2 --rsync-path="sudo rsync" \
    "${SRC_HOST}:${SRC_DIR}/" "$DEST_DIR/" \
    --include='.coverage.*' --exclude='*'
  echo
else
  echo "=== Using existing local data in ${DEST_DIR} ==="
  echo
fi

if ls "$DEST_DIR"/.coverage.* >/dev/null 2>&1; then
  echo "=== coverage combine ==="
  # `coverage combine` consumes the parallel input files and writes a single
  # .coverage. Run in DEST_DIR so the combined file lands there too.
  (cd "$DEST_DIR" && coverage combine --rcfile=../../.coveragerc) || true
  echo
elif [[ -f "$DEST_DIR/.coverage" ]]; then
  echo "=== Already-combined .coverage present — skipping combine ==="
  echo
else
  echo "No .coverage data in $DEST_DIR." >&2
  echo "Either pass a hostname to fetch from, or check that" >&2
  echo "services.cratedigger.coverage.enable = true is deployed." >&2
  exit 1
fi

# Guard against silently-empty collection. Twice now (2026-05-28 source=.,
# 2026-06-11 include globs missing the renamed store path) production wrote
# thousands of shards that tracked zero files — combine "succeeds" and the
# report is just empty. A harvest with no measured files is always a
# collection bug, never a real signal; fail loudly so it gets fixed instead
# of feeding an all-test-only diff into dead-code decisions.
MEASURED=$(python3 - "$DEST_DIR/.coverage" <<'PY'
import sys
from coverage import CoverageData
data = CoverageData(sys.argv[1])
data.read()
print(len(data.measured_files()))
PY
)
if [[ "$MEASURED" -eq 0 ]]; then
  echo "ERROR: combined coverage data measures 0 files — collection is broken." >&2
  echo "Check that .coveragerc include globs match the deployed store path" >&2
  echo "(see tests/test_coverage_config.py) and that COVERAGE_PROCESS_START" >&2
  echo "points at the same rcfile." >&2
  exit 1
fi
echo "=== coverage report ($MEASURED files measured) ==="
coverage report --rcfile=.coveragerc --data-file="$DEST_DIR/.coverage" || true
echo

echo "=== coverage html → build/coverage-html/ ==="
coverage html --rcfile=.coveragerc --data-file="$DEST_DIR/.coverage" \
  -d build/coverage-html

echo
echo "Open build/coverage-html/index.html to browse."
echo "Combined data file: $DEST_DIR/.coverage"
