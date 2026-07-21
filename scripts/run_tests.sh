#!/usr/bin/env bash
# Run the complete deterministic test suite once on the current tree.
# Usage: nix-shell --run "bash scripts/run_tests.sh"
set -euo pipefail

# JS syntax check
echo "=== JS syntax check ==="
for f in web/js/*.js; do
  node --check "$f" || { echo "FAIL: $f"; exit 1; }
done
echo "All JS files OK"
echo ""

# JS unit tests — glob so every tests/test_js_*.mjs on disk runs; a manual
# list drifted silently before (issue #520: test_js_grouping.mjs,
# test_js_library.mjs, test_js_release_actions.mjs were never run).
echo "=== JS unit tests ==="
for f in tests/test_js_*.mjs; do
  node "$f"
done
echo ""

# Production typing rules (#765 phase 2) — the four mode-independent strict
# checks (unnecessary isinstance/comparison, constant redefinition,
# deprecated APIs) run over production code only. Tests keep intentional
# protocol-conformance issubclass pins, so they are excluded here; the main
# pyrightconfig.json stays whole-repo and runs in pre-commit / final gates.
echo "=== Pyright production typing rules ==="
pyright -p pyrightconfig.production.json --threads 4
echo ""

# Production-liveness sweep — source-local Ruff F401/F811 runs first, then
# aggregate vulture. Vulture's baseline lives at tools/vulture/whitelist.py;
# intentional import exports, if any, require exact redundant-alias baselines
# (CLAUDE.md § "Finding dead code").
echo "=== Dead-code sweep ==="
bash "$(dirname "$0")/find_dead_code.sh"
echo ""

echo "=== Python tests ==="
# Four long-lived workers amortize ephemeral PostgreSQL startup while using
# the host's idle cores; each module still gets a fresh Python interpreter.
# Override with CRATEDIGGER_TEST_JOBS when diagnosing worker-specific behavior.
python3 scripts/run_python_tests.py
