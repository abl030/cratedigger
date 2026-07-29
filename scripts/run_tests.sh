#!/usr/bin/env bash
# Run every deterministic validation phase and publish one private evidence bundle.
# Usage: nix-shell --run "bash scripts/run_tests.sh"
set -euo pipefail

exec python3 scripts/run_test_suite.py
