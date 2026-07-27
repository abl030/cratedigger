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

# Repository-wide Python lint. Ruff's pinned 0.16 defaults plus the
# repository-selected extensions apply equally to production and tests.
echo "=== Ruff ==="
bash "$(dirname "$0")/run_ruff.sh"
echo ""

# Production-liveness sweep. Vulture's baseline lives at
# tools/vulture/whitelist.py; tests remain excluded so a test reference cannot
# keep dead production code live (CLAUDE.md § "Finding dead code").
echo "=== Dead-code sweep ==="
bash "$(dirname "$0")/find_dead_code.sh"
echo ""

echo "=== Python tests ==="
# Bounded long-lived workers amortize ephemeral PostgreSQL startup while using
# half the host up to the measured 12-worker cap; each module still gets a
# fresh Python interpreter.
# Override with CRATEDIGGER_TEST_JOBS when diagnosing worker-specific behavior.
python3 scripts/run_python_tests.py
