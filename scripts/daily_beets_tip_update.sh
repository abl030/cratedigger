#!/usr/bin/env bash
# Checks-only upstream Beets tip canary. Never changes deployment authority.
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
nix flake update beets-tip
nix build .#checks.x86_64-linux.beetsTipBuild --print-build-logs
nix build .#checks.x86_64-linux.beetsTipContract --print-build-logs
nix build .#checks.x86_64-linux.beetsTipPyright --print-build-logs

if git diff --quiet -- flake.lock; then
    echo "beets tip canary: flake.lock already current"
    exit 0
fi
git commit --only -m "chore(beets): refresh tip canary lock" -m "Refs #992" -- flake.lock
git push origin "HEAD:refs/heads/$branch"
