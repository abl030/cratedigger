#!/usr/bin/env bash
# Daily nixpkgs-unstable compatibility gate for issue #498.
#
# This script owns repository semantics only: clone current main, update the
# lock, run every candidate gate, and push one lock-only commit when green.
# The caller owns scheduling, persistent state, and failure notification.
set -euo pipefail

repository="${CRATEDIGGER_UPDATE_REPOSITORY:-https://github.com/abl030/cratedigger.git}"
branch="${CRATEDIGGER_UPDATE_BRANCH:-main}"
state_dir="${CRATEDIGGER_AUTOMATION_STATE_DIR:?CRATEDIGGER_AUTOMATION_STATE_DIR is required}"
mirror_url="${CRATEDIGGER_MIRROR_URL:?CRATEDIGGER_MIRROR_URL is required}"

world_database="$state_dir/hypothesis/world-model"
mirror_database="$state_dir/hypothesis/mirror-world"
fuzz_database="$state_dir/hypothesis/fuzz"
fuzz_output_dir="$state_dir/fuzz-failures"

mkdir -p \
    "$world_database" \
    "$mirror_database" \
    "$fuzz_database" \
    "$fuzz_output_dir"

work_root=$(mktemp -d "${TMPDIR:-/tmp}/cratedigger-daily-update.XXXXXX")
checkout="$work_root/repo"

cleanup() {
    if [[ -n "${work_root:-}" && -d "$work_root" ]]; then
        rm -rf -- "$work_root"
    fi
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

echo "daily unstable gate: cloning $repository branch $branch"
if ! git clone --quiet --branch "$branch" --single-branch "$repository" "$checkout"; then
    echo "daily unstable gate: clone failed" >&2
    exit 1
fi
cd "$checkout"

# No unattended test may inherit authority to connect to an ambient database.
# The world wrappers repeat this guard, but the normal suite must be protected
# too because its conftest accepts TEST_DB_DSN for explicit developer use.
unset TEST_DB_DSN

echo "daily unstable gate: updating flake.lock"
if ! nix flake update; then
    echo "daily unstable gate: flake update failed" >&2
    exit 1
fi

declare -a stage_names=()
declare -a stage_statuses=()

run_stage() {
    local name="$1"
    shift
    local status

    echo ""
    echo "=== $name ==="
    if "$@"; then
        status=0
    else
        status=$?
    fi
    stage_names+=("$name")
    stage_statuses+=("$status")
}

run_stage "whole-repository Pyright" \
    nix-shell --run "pyright --threads 4"
run_stage "deterministic full suite" \
    nix-shell --run "bash scripts/run_tests.sh"
run_stage "Nix flake checks" \
    nix flake check --print-build-logs
run_stage "world-model burst" \
    env CRATEDIGGER_WORLD_DATABASE="$world_database" \
    nix-shell --run "bash scripts/world_model_burst.sh"
run_stage "generated fuzz burst" \
    env HYPOTHESIS_STORAGE_DIRECTORY="$fuzz_database" \
        CRATEDIGGER_FUZZ_OUTPUT_DIR="$fuzz_output_dir" \
    nix-shell --run "bash scripts/fuzz_burst.sh"
run_stage "mirror-harness smoke" \
    env CRATEDIGGER_WORLD_DATABASE="$mirror_database" \
        CRATEDIGGER_WORLD_ENGINE="mirror-harness" \
        CRATEDIGGER_WORLD_MIRROR_URL="$mirror_url" \
        CRATEDIGGER_WORLD_EXAMPLES="2" \
        CRATEDIGGER_WORLD_STEPS="5" \
    nix-shell --run "bash scripts/world_model_burst.sh"

echo ""
echo "=== daily candidate summary ==="
candidate_failed=0
for ((i = 0; i < ${#stage_names[@]}; i++)); do
    if [[ "${stage_statuses[$i]}" -eq 0 ]]; then
        echo "PASS ${stage_names[$i]}"
    else
        echo "FAIL ${stage_names[$i]} (exit ${stage_statuses[$i]})"
        candidate_failed=1
    fi
done

if [[ "$candidate_failed" -ne 0 ]]; then
    echo "daily unstable gate: candidate failed; flake.lock was not committed" >&2
    exit 1
fi

echo "ALL CANDIDATE GATES GREEN"
if git diff --quiet -- flake.lock; then
    echo "daily unstable gate: flake.lock already current"
    exit 0
fi

if ! git commit --only \
    -m "chore(nix): refresh unstable lock" \
    -m "Refs #498" \
    -- flake.lock; then
    echo "daily unstable gate: lock commit failed" >&2
    exit 1
fi
if ! git push origin "HEAD:refs/heads/$branch"; then
    echo "daily unstable gate: push failed; verify the remote branch state" >&2
    exit 1
fi

echo "daily unstable gate: pushed updated flake.lock"
