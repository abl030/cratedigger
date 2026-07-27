#!/usr/bin/env bash
# Canonical repository-wide Ruff gate.
set -euo pipefail

REPO_ROOT=${CRATEDIGGER_REPO_ROOT:-"$(cd "$(dirname "$0")/.." && pwd)"}
cd "$REPO_ROOT"

if (($# == 0)); then
  set -- .
fi

exec ruff check \
  --output-format "${CRATEDIGGER_RUFF_OUTPUT_FORMAT:-full}" \
  "$@"
