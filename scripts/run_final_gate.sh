#!/usr/bin/env bash
# Run the canonical full suite with a runtime-tmpfs receipt, or inspect one.
# Usage: scripts/run_final_gate.sh [status RECEIPT]
#
# Thin wrapper only (issue #1278 item 6): the gate itself lives in
# scripts/test_substrate.py, beside the receipt format, the /proc liveness
# read, and the reapers that retire what it writes — one implementation of
# each instead of a bash copy and a Python copy that can drift. Same shape as
# scripts/run_tests.sh's own `exec python3` wrapper. This file must stay a
# wrapper: it runs OUTSIDE the Nix dev shell (the gate is what launches
# `nix develop`), which is exactly why the module it execs imports nothing
# but the standard library.
set -euo pipefail

# `$0`'s own directory, never the caller's: the gate binds its receipt to the
# worktree the operator ran it from, so the working directory must be left
# exactly as inherited.
here="$(dirname -- "$0")"

if ! command -v python3 >/dev/null 2>&1; then
    printf 'final gate requires python3 on PATH (it runs outside the Nix dev shell)\n' >&2
    exit 2
fi

exec python3 "$here/test_substrate.py" final-gate "$@"
