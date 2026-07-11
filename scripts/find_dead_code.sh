#!/usr/bin/env bash
# Static dead-code finder. Wraps vulture with sensible defaults for this repo.
#
# Default: reads tools/vulture/whitelist.py and only reports *new* findings
#          introduced since the baseline. Use this in CI / pre-commit to
#          catch drift.
#
# --baseline: ignore the whitelist and report every candidate. Use this when
#             actively hunting dead code for deletion — many findings will be
#             real, some are framework dispatch the whitelist already covers.
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

SOURCES=(
  # Deliberately production-only: a test call must not keep a dead production
  # API alive. Test-only wire/framework fields require explicit whitelist
  # entries with a rationale instead (docs/dead-code.md).
  lib
  web
  harness
  scripts
  cratedigger.py
  album_source.py
)

VULTURE_ARGS=(--min-confidence "$CONFIDENCE")
if [[ "$USE_WHITELIST" == 1 ]]; then
  VULTURE_ARGS+=(tools/vulture/whitelist.py)
fi

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
