#!/usr/bin/env bash
# Canonical web/js type gate: tsc over the checked-JavaScript project.
#
# The options live in web/js/jsconfig.json, not here, so an editor and this
# gate check the same thing. `--pretty false` pins the diagnostic format tsc
# would otherwise choose from whether stdout is a terminal —
# scripts/phase_parsers/tsc.py reads exactly one dialect.
set -euo pipefail

REPO_ROOT=${CRATEDIGGER_REPO_ROOT:-"$(cd "$(dirname "$0")/.." && pwd)"}
cd "$REPO_ROOT"

# tsc checks a project, not a file list: narrowing to one module would drop
# the cross-module inference the errors are about. There is nothing to
# forward, so an argument is a caller mistake rather than a subset request.
#
# 64 (EX_USAGE), not the 2 scripts/run_js_checks.sh uses, because 2 is one
# of tsc's own two failure codes and the phase counts it as type errors.
# 64 is outside the phase's failure_exit_codes, so a misused wrapper is
# reported as an infrastructure failure instead of a clean red gate.
if (($# > 0)); then
    printf 'usage: %s\n' "$0" >&2
    exit 64
fi

exec tsc --pretty false --project web/js/jsconfig.json
