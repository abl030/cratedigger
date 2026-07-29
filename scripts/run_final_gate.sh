#!/usr/bin/env bash
# Run one final validation gate with a runtime-tmpfs receipt, or inspect one.
set -uo pipefail

die() {
    printf '%s\n' "$*" >&2
    exit 2
}

proc_start_ticks() {
    local pid=$1 stat
    [[ -r "/proc/$pid/stat" ]] || return 1
    stat=$(<"/proc/$pid/stat")
    # Strip the comm field, which is parenthesized and may itself contain spaces.
    stat=${stat##*) }
    awk '{print $20}' <<<"$stat"
}

same_process() {
    local pid=$1 expected_ticks=$2 actual_ticks
    [[ "$pid" =~ ^[0-9]+$ && "$expected_ticks" =~ ^[0-9]+$ ]] || return 1
    actual_ticks=$(proc_start_ticks "$pid") || return 1
    [[ "$actual_ticks" == "$expected_ticks" ]]
}

runtime_dir() {
    local runtime=${XDG_RUNTIME_DIR:-/run/user/$UID} mode filesystem
    [[ -d "$runtime" && -O "$runtime" && ! -L "$runtime" ]] \
        || die "private runtime directory is unavailable or not owned by this user: $runtime"
    mode=$(stat -c '%a' "$runtime") || die "cannot inspect private runtime directory: $runtime"
    [[ "$mode" == 700 ]] || die "private runtime directory is not mode 0700: $runtime"
    filesystem=$(findmnt -no FSTYPE -T "$runtime") || die "cannot inspect runtime filesystem: $runtime"
    [[ "$filesystem" == tmpfs ]] || die "private runtime directory is not tmpfs: $runtime"
    realpath -e "$runtime"
}

receipt_field() {
    local receipt=$1 field=$2
    [[ -f "$receipt/$field" ]] || die "receipt is missing $field: $receipt"
    cat "$receipt/$field"
}

assert_current_tree_matches() {
    local receipt=$1 repo_root head current_root current_head dirty
    repo_root=$(receipt_field "$receipt" repo_root)
    head=$(receipt_field "$receipt" head)
    [[ $(receipt_field "$receipt" clean) == true ]] || die "receipt is not bound to a clean tree: $receipt"
    current_root=$(git rev-parse --show-toplevel) || die "status must run from a git worktree"
    current_head=$(git rev-parse HEAD) || die "status cannot resolve HEAD"
    dirty=$(git status --porcelain --untracked-files=all)
    [[ "$current_root" == "$repo_root" && "$current_head" == "$head" && -z "$dirty" ]] \
        || die "receipt is not for this committed clean tree: $receipt"
}

current_tree_matches() {
    local receipt=$1 repo_root head current_root current_head dirty
    repo_root=$(receipt_field "$receipt" repo_root) || return 1
    head=$(receipt_field "$receipt" head) || return 1
    [[ $(receipt_field "$receipt" clean) == true ]] || return 1
    current_root=$(git rev-parse --show-toplevel) || return 1
    current_head=$(git rev-parse HEAD) || return 1
    dirty=$(git status --porcelain --untracked-files=all)
    [[ "$current_root" == "$repo_root" && "$current_head" == "$head" && -z "$dirty" ]]
}

status_receipt() {
    local receipt=$1 runtime parent terminal helper_pid helper_ticks gate_pid gate_ticks
    runtime=$(runtime_dir)
    [[ -d "$receipt" && ! -L "$receipt" ]] || die "receipt directory is unavailable: $receipt"
    receipt=$(realpath -e "$receipt")
    parent=$(dirname "$receipt")
    [[ "$parent" == "$runtime" ]] || die "receipt is not directly beneath the private runtime directory: $receipt"
    [[ $(stat -c '%a' "$receipt") == 700 ]] || die "receipt is not mode 0700: $receipt"
    assert_current_tree_matches "$receipt"
    [[ -f "$receipt/output.log" ]] || die "receipt is missing output.log: $receipt"
    [[ -f "$receipt/command" && -f "$receipt/label" ]] || die "receipt metadata is incomplete: $receipt"
    [[ $(receipt_field "$receipt" command) == "$(gate_command "$(receipt_field "$receipt" label)")" ]] \
        || die "receipt command is not canonical for its gate label: $receipt"
    if [[ $(receipt_field "$receipt" label) == tests && -f "$receipt/terminal" ]]; then
        terminal=$(<"$receipt/terminal")
        if [[ "$terminal" == "pass 0" && ! -f "$receipt/bundle" ]]; then
            die "passing tests receipt is missing its suite bundle path: $receipt"
        fi
    fi

    if [[ -f "$receipt/terminal" ]]; then
        terminal=$(<"$receipt/terminal")
        if [[ "$terminal" =~ ^pass\ 0$ ]]; then
            printf 'pass\n'
        elif [[ "$terminal" =~ ^fail\ [1-9][0-9]*$ ]]; then
            printf 'fail\n'
        else
            die "receipt has an invalid terminal state: $receipt"
        fi
        return
    fi

    helper_pid=$(receipt_field "$receipt" helper_pid)
    helper_ticks=$(receipt_field "$receipt" helper_start_ticks)
    gate_pid=$(receipt_field "$receipt" gate_pid)
    gate_ticks=$(receipt_field "$receipt" gate_start_ticks)
    if same_process "$helper_pid" "$helper_ticks" && same_process "$gate_pid" "$gate_ticks"; then
        printf 'exact-active\n'
    else
        printf 'incomplete\n'
    fi
}

gate_command() {
    local label=$1
    case "$label" in
        pyright) printf '%s\n' 'pyright --threads 4' ;;
        tests) printf '%s\n' 'bash scripts/run_tests.sh' ;;
        *) die "gate must be pyright or tests" ;;
    esac
}

record_suite_bundle() {
    local receipt=$1 output=$2 runtime bundle resolved parent
    local -a matches
    mapfile -t matches < <(sed -n 's/^bundle: //p' "$output")
    if (( ${#matches[@]} == 0 )); then
        return 1
    fi
    if (( ${#matches[@]} != 1 )); then
        printf 'test gate published multiple bundle paths\n' >&2
        return 1
    fi
    runtime=$(runtime_dir)
    bundle=${matches[0]}
    [[ -d "$bundle" && ! -L "$bundle" ]] || {
        printf 'test gate bundle is unavailable: %s\n' "$bundle" >&2
        return 1
    }
    resolved=$(realpath -e "$bundle") || return 1
    parent=$(dirname "$resolved")
    [[ "$parent" == "$runtime" && $(stat -c '%a' "$resolved") == 700 ]] || {
        printf 'test gate bundle is not a private runtime directory: %s\n' "$bundle" >&2
        return 1
    }
    [[ -f "$resolved/summary.json" && ! -L "$resolved/summary.json" ]] || {
        printf 'test gate bundle is missing summary.json: %s\n' "$bundle" >&2
        return 1
    }
    printf '%s\n' "$resolved" >"$receipt/bundle"
}

run_gate() {
    local label=$1 runtime receipt command status helper_ticks gate_pid gate_ticks terminal_tmp
    local -a gate_argv
    runtime=$(runtime_dir)
    command=$(gate_command "$label")
    gate_argv=(nix-shell --run "$command")
    receipt=$(mktemp -d "$runtime/cratedigger-final-gate.XXXXXXXX") \
        || die "cannot create final-gate receipt beneath $runtime"
    chmod 700 "$receipt" || die "cannot secure receipt directory: $receipt"
    printf '%s\n' "$(git rev-parse --show-toplevel)" >"$receipt/repo_root" \
        || die "final gate must run from a git worktree"
    git diff --quiet && git diff --cached --quiet && [[ -z $(git ls-files --others --exclude-standard) ]] \
        || die "final gate requires a committed clean tree"
    git rev-parse HEAD >"$receipt/head"
    printf 'true\n' >"$receipt/clean"
    printf '%s\n' "$label" >"$receipt/label"
    printf '%s\n' "$command" >"$receipt/command"
    printf '%s\n' "$$" >"$receipt/helper_pid"
    helper_ticks=$(proc_start_ticks "$$") || die "cannot record helper process identity"
    printf '%s\n' "$helper_ticks" >"$receipt/helper_start_ticks"
    : >"$receipt/output.log"

    printf 'receipt: %s\n' "$receipt"
    # A signal is not command completion. Leave an inspectable incomplete receipt.
    trap 'exit 128' HUP INT TERM
    "${gate_argv[@]}" >"$receipt/output.log" 2>&1 &
    gate_pid=$!
    gate_ticks=$(proc_start_ticks "$gate_pid" || true)
    printf '%s\n' "$gate_pid" >"$receipt/gate_pid"
    printf '%s\n' "$gate_ticks" >"$receipt/gate_start_ticks"

    if wait "$gate_pid"; then
        status=0
    else
        status=$?
    fi
    if (( status >= 128 )); then
        printf 'final gate %s: incomplete (signal-shaped exit %s)\n' "$label" "$status" >&2
        return "$status"
    fi
    if [[ "$label" == tests ]] && ! record_suite_bundle "$receipt" "$receipt/output.log"; then
        if [[ "$status" == 0 ]]; then
            printf 'final gate tests: incomplete (passing suite published no valid bundle)\n' >&2
            return 2
        fi
        printf 'final gate tests: bundle unavailable for failed suite\n' >&2
    fi
    if ! current_tree_matches "$receipt"; then
        printf 'final gate %s: incomplete (tree changed before terminal receipt)\n' "$label" >&2
        return 2
    fi
    if [[ "$status" == 0 ]]; then
        printf 'pass 0\n' >"$receipt/.terminal"
    else
        printf 'fail %s\n' "$status" >"$receipt/.terminal"
    fi
    terminal_tmp="$receipt/.terminal"
    mv "$terminal_tmp" "$receipt/terminal"
    trap - HUP INT TERM
    if [[ "$status" == 0 ]]; then
        printf 'final gate %s: pass (exit 0)\n' "$label"
    else
        printf 'final gate %s: fail (exit %s)\n' "$label" "$status"
    fi
    return "$status"
}

case ${1:-} in
    pyright|tests)
        [[ $# == 1 ]] || die "usage: $0 {pyright|tests|status RECEIPT}"
        run_gate "$1"
        ;;
    status)
        [[ $# == 2 ]] || die "usage: $0 {pyright|tests|status RECEIPT}"
        status_receipt "$2"
        ;;
    *) die "usage: $0 {pyright|tests|status RECEIPT}" ;;
esac
