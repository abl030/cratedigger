#!/usr/bin/env bash
# Source-local import liveness gate. Called by find_dead_code.sh and tests.
set -euo pipefail

REPO_ROOT=${CRATEDIGGER_REPO_ROOT:-"$(cd "$(dirname "$0")/.." && pwd)"}
SOURCE_LIST=${1:-tools/production_python_sources.txt}
if [[ "$SOURCE_LIST" != /* ]]; then
  SOURCE_LIST="$REPO_ROOT/$SOURCE_LIST"
fi

mapfile -t sources < <(sed '/^[[:space:]]*#/d; /^[[:space:]]*$/d' "$SOURCE_LIST")
if ((${#sources[@]} == 0)); then
  echo "unused-import gate: no production sources in $SOURCE_LIST" >&2
  exit 2
fi

cd "$REPO_ROOT"
ruff check \
  --select F401,F811 \
  --output-format "${CRATEDIGGER_RUFF_OUTPUT_FORMAT:-full}" \
  "${sources[@]}"
