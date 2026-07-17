#!/usr/bin/env bash
# Randomized Hypothesis fuzz burst over the generated-test modules.
#
# Hypothesis is single-threaded, so the old serial burst pegged one core and
# left the rest of the machine idle. This runner gives each generated module
# its own process and parallelises them up to the host's core count (nproc,
# whatever machine this runs on). The 20k-example `fuzz` profile budget is
# deliberate and unchanged — that depth found the live #550 manifest bug.
# See docs/generated-testing.md.
#
# Usage (always inside nix-shell, like every Python entry point):
#   nix-shell --run "bash scripts/fuzz_burst.sh"
#       -> every tests/test_*_generated.py module
#   nix-shell --run "bash scripts/fuzz_burst.sh tests.test_quality_generated tests.test_evidence_generated"
#       -> just the named modules (e.g. the quality-policy set)
#
# FUZZ_PROFILE overrides the Hypothesis profile (default: fuzz) — used by the
# script's own smoke test; real bursts never pass it.
set -u

cd "$(dirname "$0")/.."

if [ "$#" -gt 0 ]; then
    modules=("$@")
else
    modules=()
    for f in tests/test_*_generated.py; do
        modules+=("tests.$(basename "$f" .py)")
    done
fi

jobs=$(nproc)
profile="${FUZZ_PROFILE:-fuzz}"
outdir=$(mktemp -d)
trap 'rm -rf "$outdir"' EXIT
export _FUZZ_OUTDIR="$outdir"
export _FUZZ_PROFILE="$profile"

echo "fuzz burst: ${#modules[@]} generated modules, up to $jobs parallel ($jobs host cores), profile=$profile"

printf '%s\n' "${modules[@]}" | xargs -P "$jobs" -I{} bash -c '
    m="$1"
    if CRATEDIGGER_HYPOTHESIS_PROFILE="$_FUZZ_PROFILE" \
            python3 -m unittest "$m" >"$_FUZZ_OUTDIR/$m.log" 2>&1; then
        echo "PASS $m — $(grep -E "^Ran " "$_FUZZ_OUTDIR/$m.log" | tail -1)"
    else
        echo "FAIL $m"
        tail -40 "$_FUZZ_OUTDIR/$m.log"
        exit 1
    fi
' _ {}
status=$?

if [ "$status" -ne 0 ]; then
    echo "fuzz burst: FAILURES (see above)"
    exit 1
fi
echo "fuzz burst: ALL GREEN (${#modules[@]} modules)"
