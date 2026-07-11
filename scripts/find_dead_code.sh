#!/usr/bin/env bash
# Static liveness gates: source-local Ruff imports plus aggregate vulture.
#
# Default: Ruff rejects source-local F401/F811 findings, then vulture reads
#          tools/vulture/whitelist.py and reports only aggregate findings
#          introduced since that baseline.
#
# --baseline: ignore only vulture's whitelist and report every aggregate
#             candidate. Ruff remains an exact zero-new-debt gate.
#
# Usage:
#   nix-shell --run "bash scripts/find_dead_code.sh"             # diff vs whitelist
#   nix-shell --run "bash scripts/find_dead_code.sh --baseline"  # all candidates
#   nix-shell --run "bash scripts/find_dead_code.sh --confidence 80"

set -euo pipefail

cd "$(dirname "$0")/.."

USE_WHITELIST=1
CONFIDENCE=60

while [[ $# -gt 0 ]]; do
  case "$1" in
    --baseline) USE_WHITELIST=0; shift ;;
    --confidence) CONFIDENCE="$2"; shift 2 ;;
    --confidence=*) CONFIDENCE="${1#*=}"; shift ;;
    -h|--help)
      sed -n '2,17p' "$0" | sed 's/^# \?//'
      exit 0
      ;;
    *) echo "unknown flag: $1" >&2; exit 2 ;;
  esac
done

# One authored production-root list feeds both local F401 and aggregate vulture.
# Tests stay excluded: a test reference must not keep production code live.
mapfile -t SOURCES < <(sed '/^[[:space:]]*#/d; /^[[:space:]]*$/d' \
  tools/production_python_sources.txt)

VULTURE_ARGS=(--min-confidence "$CONFIDENCE")
if [[ "$USE_WHITELIST" == 1 ]]; then
  VULTURE_ARGS+=(tools/vulture/whitelist.py)
fi

echo "=== ruff source-local unused imports: ${SOURCES[*]} ==="
echo
ruff check --select F401,F811 "${SOURCES[@]}"

echo
echo "=== vulture ${VULTURE_ARGS[*]} ${SOURCES[*]} ==="
echo

# vulture exits 3 when findings are present; we want to print them and let
# the caller decide whether that's a failure.
set +e
vulture "${VULTURE_ARGS[@]}" "${SOURCES[@]}"
status=$?
set -e

if [[ "$status" -eq 0 ]]; then
  echo
  echo "No dead code found."
elif [[ "$status" -eq 3 ]]; then
  echo
  echo "Dead-code candidates above. Triage them, then either:"
  echo "  - delete the genuinely dead ones and update tools/vulture/whitelist.py"
  echo "  - re-baseline:  vulture --make-whitelist ${SOURCES[*]} > tools/vulture/whitelist.py"
fi
exit "$status"
