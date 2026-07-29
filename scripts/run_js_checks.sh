#!/usr/bin/env bash
# Exhaust one JavaScript validation phase before returning its aggregate status.
set -uo pipefail

REPO_ROOT=${CRATEDIGGER_REPO_ROOT:-"$(cd "$(dirname "$0")/.." && pwd)"}
cd "$REPO_ROOT"

mode=${1:-}
status=0
case "$mode" in
    syntax)
        for file in web/js/*.js; do
            if ! node --check --input-type=module <"$file"; then
                printf 'CRATEDIGGER_JS_FAILURE\t%s\t%s\n' \
                    "$file" "node --check failed"
                status=1
            fi
        done
        ;;
    unit)
        for file in tests/test_js_*.mjs; do
            if ! node "$file"; then
                printf 'CRATEDIGGER_JS_FAILURE\t%s\t%s\n' \
                    "$file" "JavaScript test file failed"
                status=1
            fi
        done
        ;;
    *)
        printf 'usage: %s {syntax|unit}\n' "$0" >&2
        exit 2
        ;;
esac
exit "$status"
