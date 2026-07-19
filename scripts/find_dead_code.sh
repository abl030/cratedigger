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

REPO_ROOT=${CRATEDIGGER_REPO_ROOT:-"$(cd "$(dirname "$0")/.." && pwd)"}
cd "$REPO_ROOT"

USE_WHITELIST=1
CONFIDENCE=60
VULTURE_FRESHNESS_CONFIDENCE=60
VULTURE_WHITELIST_FILE=${CRATEDIGGER_VULTURE_WHITELIST_FILE:-tools/vulture/whitelist.py}
VULTURE_FRESHNESS_TMP=""

cleanup_vulture_freshness_tmp() {
  if [[ -n "$VULTURE_FRESHNESS_TMP" ]]; then
    rm -f -- "$VULTURE_FRESHNESS_TMP"
  fi
}
trap cleanup_vulture_freshness_tmp EXIT

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
SOURCE_LIST=${CRATEDIGGER_PRODUCTION_PYTHON_SOURCES_FILE:-tools/production_python_sources.txt}
if [[ "$SOURCE_LIST" != /* ]]; then
  SOURCE_LIST="$REPO_ROOT/$SOURCE_LIST"
fi
mapfile -t SOURCES < <(sed '/^[[:space:]]*#/d; /^[[:space:]]*$/d' "$SOURCE_LIST")

check_vulture_whitelist_freshness() {
  VULTURE_FRESHNESS_TMP=$(mktemp "${TMPDIR:-/tmp}/cratedigger-vulture-whitelist.XXXXXX")
  set +e
  vulture \
    --make-whitelist \
    --min-confidence "$VULTURE_FRESHNESS_CONFIDENCE" \
    "${SOURCES[@]}" > "$VULTURE_FRESHNESS_TMP"
  local raw_status=$?
  set -e
  if [[ "$raw_status" -ne 0 && "$raw_status" -ne 3 ]]; then
    echo "raw Vulture whitelist generation failed with exit $raw_status" >&2
    return 2
  fi

  if ! diff -u \
    --label committed-vulture-whitelist \
    --label generated-vulture-whitelist \
    <(sed '/^[[:space:]]*#/d; /^[[:space:]]*$/d' "$VULTURE_WHITELIST_FILE") \
    "$VULTURE_FRESHNESS_TMP" >&2; then
    echo "Vulture whitelist is not the exact confidence-60 candidate baseline" >&2
    return 3
  fi
}

VULTURE_ARGS=(--min-confidence "$CONFIDENCE")
if [[ "$USE_WHITELIST" == 1 ]]; then
  VULTURE_ARGS+=("$VULTURE_WHITELIST_FILE")
fi

echo "=== ruff source-local unused imports: ${SOURCES[*]} ==="
echo
bash scripts/find_unused_imports.sh "$SOURCE_LIST"

if [[ "$USE_WHITELIST" == 1 ]]; then
  check_vulture_whitelist_freshness
fi

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
