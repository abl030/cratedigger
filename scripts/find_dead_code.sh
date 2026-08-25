#!/usr/bin/env bash
# Static production-liveness gate: aggregate vulture.
#
# Default: vulture reads tools/vulture/whitelist.py and reports only aggregate
#          findings introduced since that baseline.
#
# --baseline: ignore only vulture's whitelist and report every aggregate
#             candidate.
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

# Vulture alone consumes the production-root list. Tests stay excluded: a test
# reference must not keep production code live.
SOURCE_LIST=${CRATEDIGGER_PRODUCTION_PYTHON_SOURCES_FILE:-tools/production_python_sources.txt}
if [[ "$SOURCE_LIST" != /* ]]; then
  SOURCE_LIST="$REPO_ROOT/$SOURCE_LIST"
fi
mapfile -t SOURCES < <(sed '/^[[:space:]]*#/d; /^[[:space:]]*$/d' "$SOURCE_LIST")

# Normalize one whitelist stream for comparison (#1266 item 1): drop
# full-line comments and blanks, strip the LINE NUMBER from each entry's
# trailing "(path:line)" location comment, and sort. The identifier, its
# kind ("unused attribute"/...), and its FILE all stay in the compared
# text — only the line number is comparison-irrelevant, because any edit
# above a whitelisted site shifts it without changing what is
# whitelisted (this broke the gate four times across #1260/#1264 for one
# entry). Sorting makes the comparison a set comparison, so two entries
# in one file swapping relative order under line drift cannot break it
# either. The committed file keeps its human-facing line comments; they
# refresh whenever the whitelist is regenerated. Two accepted precision
# losses (#1266 review findings 4/5): full-line-comment stripping now
# applies to the GENERATED stream too, so a "# unreachable code ..."
# comment vulture emits is invisible to freshness (the main run still
# reports it at confidence 100 and exits 3); and within one normalized
# group (same identifier + kind + FILE, e.g. web/server.py's ten
# _.close_connection occurrences) entries are counted, not located —
# deleting one site and adding another in the same file is invisible,
# while a changed COUNT still fails.
normalize_vulture_whitelist() {
  sed '/^[[:space:]]*#/d; /^[[:space:]]*$/d; s/:[0-9]\{1,\})[[:space:]]*$/)/' "$1" \
    | LC_ALL=C sort
}

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
    <(normalize_vulture_whitelist "$VULTURE_WHITELIST_FILE") \
    <(normalize_vulture_whitelist "$VULTURE_FRESHNESS_TMP") >&2; then
    echo "Vulture whitelist does not match the confidence-60 candidate baseline (identifier/kind/file set; line numbers are not compared)" >&2
    return 3
  fi
}

VULTURE_ARGS=(--min-confidence "$CONFIDENCE")
if [[ "$USE_WHITELIST" == 1 ]]; then
  VULTURE_ARGS+=("$VULTURE_WHITELIST_FILE")
fi

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
