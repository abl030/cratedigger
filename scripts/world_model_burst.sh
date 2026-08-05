#!/usr/bin/env bash
# Randomized real-PostgreSQL/real-Beets lifecycle hammer for issue #743.
#
# Run inside nix-shell. This intentionally remains separate from
# scripts/run_tests.sh: its default 25 x 100 stateful budget is operator work,
# not a standard-suite gate. The Python coordinator owns all isolation,
# scheduling, replay receipts, and teardown.
set -euo pipefail

cd "$(dirname "$0")/.."
unset TEST_DB_DSN CRATEDIGGER_TEST_SCHEMA_READY
exec python3 scripts/run_world_model_burst.py "$@"
