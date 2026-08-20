#!/usr/bin/env bash
# Daily nixpkgs-unstable compatibility gate for issue #498.
#
# This script owns repository semantics only: clone current main, update the
# lock, run every candidate gate, and push one lock-only commit when green.
# The caller owns scheduling, persistent state, and failure notification.
set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
# shellcheck source=scripts/daily_resource_monitor.sh
source "$script_dir/daily_resource_monitor.sh"

work_root=""

finalize() {
    local command_status="$?"
    local resource_status=0
    trap - EXIT INT TERM
    set +e
    # A signal may interrupt a phase transition while the parent owns the
    # sample lock. Release that descriptor before the cleanup transition so
    # the EXIT path cannot deadlock itself.
    _daily_resource_unlock
    if [[ "${_CRATEDIGGER_RESOURCE_STARTED:-0}" == 1 ]]; then
        daily_resource_monitor_set_phase cleanup
    fi
    if [[ -n "$work_root" && -d "$work_root" ]]; then
        rm -rf -- "$work_root"
    fi
    daily_resource_monitor_finish
    resource_status=$?
    # The resource receipt's validity is independent of the gate's own
    # pass/fail (issue #1214 gap 4): an invalid receipt used to be silently
    # absorbed into whatever exit code the candidate gates already produced,
    # so the one run that most needed its telemetry called out was
    # indistinguishable from an ordinary gate failure. daily_resource_
    # monitor_finish returns non-zero for BOTH status=invalid (no phase
    # breakdown at all) and status=degraded (a real but partial loss --
    # issue #1214 review C5: a single failed sample write used to flip
    # this exit code before that status even existed, and going quiet
    # about it now would be a regression on exactly the run where it
    # matters most), so this stderr call-out and exit-code promotion cover
    # both without needing to tell them apart here; the receipt's own
    # `status=` field on stdout already does that. It always fires when
    # the receipt is non-clean, including on the INT/TERM signal exits.
    # Only PROMOTING it into the process's own exit code is unchanged from
    # before: that still happens only when the gates were otherwise green
    # (command_status == 0), so a signal exit's exact code (130/143
    # below), which is contractual, is never overwritten.
    if ((resource_status != 0)); then
        echo "daily unstable gate: resource receipt degraded or invalid" \
            "(command exit $command_status)" >&2
        if ((command_status == 0)); then
            command_status=$resource_status
        fi
    fi
    exit "$command_status"
}

trap finalize EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

repository="${CRATEDIGGER_UPDATE_REPOSITORY:-https://github.com/abl030/cratedigger.git}"
branch="${CRATEDIGGER_UPDATE_BRANCH:-main}"
state_dir="${CRATEDIGGER_AUTOMATION_STATE_DIR:?CRATEDIGGER_AUTOMATION_STATE_DIR is required}"
mirror_url="${CRATEDIGGER_MIRROR_URL:?CRATEDIGGER_MIRROR_URL is required}"

mkdir -p "$state_dir"
exec 9>"$state_dir/flake-update.lock"
flock 9

world_database="$state_dir/hypothesis/world-model"
mirror_database="$state_dir/hypothesis/mirror-world"
fuzz_database="$state_dir/hypothesis/fuzz"
fuzz_output_dir="$state_dir/fuzz-failures"
overnight_fuzz_max_examples=20000

mkdir -p \
    "$world_database" \
    "$mirror_database" \
    "$fuzz_database" \
    "$fuzz_output_dir"

if ! daily_resource_monitor_start; then
    exit 1
fi

daily_resource_monitor_set_phase checkout_setup
work_root=$(mktemp -d "${TMPDIR:-/tmp}/cratedigger-daily-update.XXXXXX")
checkout="$work_root/repo"

daily_resource_monitor_set_phase clone
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

daily_resource_monitor_set_phase flake_update
echo "daily unstable gate: updating flake.lock"
if ! nix flake update nixpkgs; then
    echo "daily unstable gate: flake update failed" >&2
    exit 1
fi

declare -a stage_names=()
declare -a stage_statuses=()

run_stage() {
    local phase="$1"
    local name="$2"
    shift 2
    local status

    daily_resource_monitor_set_phase "$phase"
    echo ""
    echo "=== $name ==="
    if "$@"; then
        status=0
    else
        status=$?
    fi
    stage_names+=("$name")
    stage_statuses+=("$status")
    daily_resource_monitor_set_phase runner_overhead
}

daily_resource_monitor_set_phase runner_overhead
run_stage deterministic_suite "deterministic full suite" \
    env CRATEDIGGER_SUITE_OWNS_HEADROOM=1 \
    nix-shell --run "bash scripts/run_tests.sh"
run_stage stable_nix "stable Nix and Beets-release checks" \
    nix build .#checks.x86_64-linux.beetsStableCandidate --print-build-logs
run_stage world_model "world-model burst" \
    env CRATEDIGGER_WORLD_DATABASE="$world_database" \
    nix-shell --run "bash scripts/world_model_burst.sh"
run_stage generated_fuzz "generated fuzz burst" \
    env HYPOTHESIS_STORAGE_DIRECTORY="$fuzz_database" \
        CRATEDIGGER_FUZZ_OUTPUT_DIR="$fuzz_output_dir" \
        CRATEDIGGER_FUZZ_MAX_EXAMPLES="$overnight_fuzz_max_examples" \
    nix-shell --run "bash scripts/fuzz_burst.sh"
run_stage mirror_harness "mirror-harness smoke" \
    env CRATEDIGGER_WORLD_DATABASE="$mirror_database" \
        CRATEDIGGER_WORLD_ENGINE="mirror-harness" \
        CRATEDIGGER_WORLD_MIRROR_URL="$mirror_url" \
        CRATEDIGGER_WORLD_EXAMPLES="2" \
        CRATEDIGGER_WORLD_STEPS="5" \
    nix-shell --run "bash scripts/world_model_burst.sh"

daily_resource_monitor_set_phase candidate_summary
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

daily_resource_monitor_set_phase lock_publish
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
# Same rebase as the tip canary, and this runner needs it more: its window
# between clone and push is the whole candidate gate, tens of minutes.
if ! git pull --rebase origin "$branch"; then
    echo "daily unstable gate: lock rebase failed; verify the remote branch state" >&2
    exit 1
fi
if ! git push origin "HEAD:refs/heads/$branch"; then
    echo "daily unstable gate: push failed; verify the remote branch state" >&2
    exit 1
fi

echo "daily unstable gate: pushed updated flake.lock"
