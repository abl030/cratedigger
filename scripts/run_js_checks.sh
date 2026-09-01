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
        # Each suite reports its OWN failures, one marker per failed
        # assertion, through tests/js_harness.mjs. This loop only adds a
        # file-level marker when a suite never reached its exit path — keyed
        # on the absence of the harness's done marker rather than on the
        # absence of failure markers, so a suite that failed three assertions
        # and THEN crashed still reports the crash as its own finding.
        for file in tests/test_js_*.mjs; do
            output=$(node "$file" 2>&1)
            node_status=$?
            if [ -n "$output" ]; then
                printf '%s\n' "$output"
            fi
            if [ "$node_status" -eq 0 ]; then
                continue
            fi
            status=1
            case "$output" in
                *"CRATEDIGGER_JS_DONE"*) ;;
                *)
                    printf 'CRATEDIGGER_JS_FAILURE\t%s\t%s\n' \
                        "$file" \
                        "suite exited before reaching checker.done()"
                    ;;
            esac
        done
        ;;
    *)
        printf 'usage: %s {syntax|unit}\n' "$0" >&2
        exit 2
        ;;
esac
exit "$status"
