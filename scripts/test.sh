#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

printf -v command '%q ' python3 scripts/run_targeted_tests.py "$@"
exec nix-shell --run "$command"
