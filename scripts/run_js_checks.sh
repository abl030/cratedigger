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
        # assertion, through tests/js_harness.mjs. This loop adds a
        # file-level marker for the two things the harness cannot report
        # about itself, and it reads the done marker's own failure count to
        # tell them apart from an ordinary reported failure.
        #
        # The done marker, not the exit status, is what decides whether a
        # suite ran: a suite whose body is entirely inside a `/* */` block
        # never builds a checker, never registers the harness's exit guard,
        # and exits ZERO. Keying on a nonzero exit would credit that as a
        # pass (one edit disabling a whole suite, silently); keying on the
        # done marker catches it.
        for file in tests/test_js_*.mjs; do
            output=$(node "$file" 2>&1)
            node_status=$?
            if [ -n "$output" ]; then
                printf '%s\n' "$output"
            fi
            reported=$(printf '%s\n' "$output" | awk -F'\t' '
                $1 == "CRATEDIGGER_JS_DONE" { print $4; found = 1; exit }
                END { if (!found) print "none" }')
            case "$reported" in
                ''|*[!0-9]*)
                    # No done marker (or a garbled one): the suite never
                    # reached its exit path, whatever it exited with.
                    printf 'CRATEDIGGER_JS_FAILURE\t%s\t%s\n' \
                        "$file" \
                        "suite exited before reaching checker.done()"
                    status=1
                    ;;
                0)
                    # A clean finish. Any nonzero exit after one is a death
                    # the harness had already stopped watching for — its own
                    # finding, and the only place the file name still gets
                    # named at all.
                    if [ "$node_status" -ne 0 ]; then
                        printf 'CRATEDIGGER_JS_FAILURE\t%s\t%s\n' \
                            "$file" \
                            "suite exited $node_status after a clean checker.done()"
                        status=1
                    fi
                    ;;
                *)
                    # The harness already emitted one marker per failed
                    # assertion; adding a file-level one would double-report
                    # every real JS failure.
                    status=1
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
