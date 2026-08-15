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
nix develop .#tip --command bash scripts/run_tests.sh

if git diff --quiet -- flake.lock; then
    echo "beets tip canary: flake.lock already current"
    exit 0
fi
git commit --only -m "chore(beets): refresh tip canary lock" -m "Refs #992" -- flake.lock
# Rebase the one-file lock commit onto whatever the branch is NOW. The canary
# clones, then works for minutes; any push that lands meanwhile made the old
# unconditional push fail non-fast-forward and reported an ordinary concurrent
# merge as a canary failure. A lock-only commit rebases cleanly, and a genuine
# conflict (someone else moved the same lock nodes) still fails loudly.
git pull --rebase origin "$branch"
git push origin "HEAD:refs/heads/$branch"
