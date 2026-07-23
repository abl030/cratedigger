#!/usr/bin/env bash
# Compatibility entry point for the exact, property-balanced fuzz runner.
set -euo pipefail

cd "$(dirname "$0")/.."
python3 scripts/run_fuzz_tests.py "$@"
