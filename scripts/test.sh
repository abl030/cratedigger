#!/usr/bin/env bash
# CRATEDIGGER_SUITE_OWNS_HEADROOM=1 tells scripts/test_tmpfs.sh's shellHook
# guard to skip its own entry-time free-bytes refusal: run_targeted_tests.py
# -> run_suite() takes the SAME admission lock and headroom precondition as
# the canonical suite (issue #1111 review B1/M2), so the entry guard would
# otherwise kill a contended second run at shell startup with the old
# unnamed message instead of letting it queue on the lock.
set -euo pipefail
cd "$(dirname "$0")/.."

#
# `nix develop`, not `nix-shell` (issue #1229): flake.nix's devShells.default
# IS ./nix/shell.nix -- the same derivation `shell.nix` delegates to -- so the
# environment is identical (verified: same python3 store path, same
# CRATEDIGGER_BEETS_PYTHON, and scripts/test_tmpfs.sh's shellHook still runs
# and still allocates TMPDIR). What differs is that `nix develop` evaluates a
# LOCKED FLAKE, so Nix's own eval cache applies, while `nix-shell` re-evaluates
# every time. Measured on doc1: `nix-shell --run true` 5181ms, `nix develop
# --command true` 481ms warm on a clean tree -- ~4.7s saved per invocation, on
# the single most frequent command in the dev loop. The cache key is the
# flake's own locked fingerprint, so this is Nix invalidating its own cache,
# not a hand-rolled one: a modified TRACKED file makes the tree dirty and Nix
# re-evaluates (measured 2824ms, still faster than nix-shell), while an
# untracked file does not participate in the flake source at all (490ms).
# There is deliberately no nix-shell fallback: this repo already requires
# `nix develop` for scripts/daily_beets_tip_update.sh's `.#tip` canary.
printf -v command '%q ' python3 scripts/run_targeted_tests.py "$@"
exec env CRATEDIGGER_SUITE_OWNS_HEADROOM=1 nix develop --command bash -c "$command"
