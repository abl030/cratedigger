#!/usr/bin/env bash
# CRATEDIGGER_SUITE_OWNS_HEADROOM=1 tells scripts/test_tmpfs.sh's shellHook
# guard to skip its own entry-time free-bytes refusal: run_targeted_tests.py
# -> run_suite() takes the SAME admission lock and headroom precondition as
# the canonical suite (issue #1111 review B1/M2), so the entry guard would
# otherwise kill a contended second run at shell startup with the old
# unnamed message instead of letting it queue on the lock.
set -euo pipefail
cd "$(dirname "$0")/.."

printf -v command '%q ' python3 scripts/run_targeted_tests.py "$@"
exec env CRATEDIGGER_SUITE_OWNS_HEADROOM=1 nix-shell --run "$command"
