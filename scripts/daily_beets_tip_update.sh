#!/usr/bin/env bash
# Checks-only upstream tip canary. Never changes deployment authority.
#
# Runs Cratedigger's whole deterministic suite once, in a dev shell whose
# Beets, mutagen and mediafile are all at upstream tip. It used to run three
# targeted checks instead — a build, four named harness contract methods, and
# pyright — and that hand-picked list is exactly what this replaces: a test
# nobody remembered to name could not fail it. The first full-suite run found
# two real upstream removals in a test the old list did not include.
#
# The suite is deterministic (generated tests run derandomized); the fuzz
# tier deliberately stays in the nixpkgs candidate gate, not here.
set -euo pipefail

repository="${CRATEDIGGER_UPDATE_REPOSITORY:-https://github.com/abl030/cratedigger.git}"
branch="${CRATEDIGGER_UPDATE_BRANCH:-main}"
state_dir="${CRATEDIGGER_AUTOMATION_STATE_DIR:?CRATEDIGGER_AUTOMATION_STATE_DIR is required}"
mkdir -p "$state_dir"
exec 9>"$state_dir/flake-update.lock"
flock 9

work_root=$(mktemp -d "${TMPDIR:-/tmp}/cratedigger-beets-tip.XXXXXX")
checkout="$work_root/repo"
trap 'rm -rf -- "$work_root"' EXIT
unset TEST_DB_DSN BEETSDIR CRATEDIGGER_RUNTIME_CONFIG

git clone --quiet --branch "$branch" --single-branch "$repository" "$checkout"
cd "$checkout"
nix flake update beets-tip mutagen-tip mediafile-tip

# One update, one suite run: the three inputs advance together because they
# are tested together. mutagen is a dependency of both mediafile and
# music-tag, and mediafile of Beets, so the shell overrides them as one
# package set — advancing a subset would test a combination that no
# environment can actually hold.

# run_suite() owns the admission lock and the post-lock headroom
# precondition for every suite run started this way, so the shellHook's own
# entry-time free-bytes refusal defers to it (issue #1111). Every automated
# launcher of the canonical suite sets this.
export CRATEDIGGER_SUITE_OWNS_HEADROOM=1
# No scripts/memory_scope.sh containment here, deliberately: this runner is
# launched by the nixosconfig `cratedigger-beets-tip-canary` SYSTEM unit
# (Type=oneshot, User=abl030), which has no reliable user D-Bus session, so
# `systemd-run --user` would fail-open to no containment on exactly the
# unattended path that most needs it. The nightly units carry a declarative
# `MemoryMax=` on the unit itself instead -- enforced by the system manager,
# no bus required. The helper is for the interactive/agent launchers
# (scripts/test.sh, scripts/run_final_gate.sh) that do have a user session.
nix develop .#tip --command bash scripts/run_tests.sh

# Deliberately no lock commit or push. The canary's product is the signal,
# not a stored revision: it re-resolves every tip input to its branch HEAD at
# the START of each run, so a committed value is overwritten before anything
# reads it. The inputs are checks-only besides — flake.nix references
# `tipPackage` only for its derivation-path string, in assertions that hold
# for any revision and build nothing. Publishing was therefore one noise
# commit per day recording a number nobody consumes, and the sole reason an
# unrelated merge landing mid-run could reject the push and report an
# ordinary concurrent merge as a red canary.
#
# `scripts/daily_flake_update.sh` does still publish, because there the
# nixpkgs lock IS the deliverable.
echo "beets tip canary: green against upstream tip (checks-only, nothing published)"
