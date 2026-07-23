#!/usr/bin/env bash
# Allocate one isolated RAM-backed scratch directory for the dev shell.

_cleanup_cratedigger_test_tmpfs() {
    local scratch="${_CRATEDIGGER_TEST_TMPDIR:-}"
    local parent="${_CRATEDIGGER_TEST_TMP_PARENT:-}"
    if [[ -z "$scratch" || -z "$parent" ]]; then
        return 0
    fi
    if [[ "$(dirname -- "$scratch")" != "$parent" ]]; then
        echo "Refusing to clean unexpected test scratch path: $scratch" >&2
        return 1
    fi
    if [[ "$(basename -- "$scratch")" != cratedigger-tests.* ]]; then
        echo "Refusing to clean unexpected test scratch path: $scratch" >&2
        return 1
    fi
    rm -rf -- "$scratch"
}

_return_cratedigger_test_status() {
    return "$1"
}

_exit_cratedigger_test_tmpfs() {
    local exit_code="$?"
    set +e
    _cleanup_cratedigger_test_tmpfs
    if declare -F exitHandler >/dev/null; then
        _return_cratedigger_test_status "$exit_code"
        exitHandler
    fi
    return "$exit_code"
}

setup_cratedigger_test_tmpfs() {
    local default_parent="${XDG_RUNTIME_DIR:-/run/user/$(id -u)}"
    local parent="${CRATEDIGGER_TEST_RAM_ROOT:-$default_parent}"
    local minimum_bytes="${CRATEDIGGER_TEST_RAM_MIN_BYTES:-1073741824}"
    local current
    local filesystem_type
    local available_bytes
    local mode

    if [[ ! "$minimum_bytes" =~ ^[0-9]+$ ]]; then
        echo "CRATEDIGGER_TEST_RAM_MIN_BYTES must be a non-negative integer" >&2
        return 1
    fi
    if [[ ! -d "$parent" || ! -w "$parent" || ! -x "$parent" ]]; then
        echo "Test RAM root is not a writable directory: $parent" >&2
        return 1
    fi

    filesystem_type="$(stat --file-system --format=%T -- "$parent")" || return 1
    if [[ "$filesystem_type" != "tmpfs" ]]; then
        echo "Test RAM root is not tmpfs: $parent ($filesystem_type)" >&2
        return 1
    fi

    current="$(realpath -- "$parent")" || return 1
    while true; do
        mode="$(stat --format=%a -- "$current")" || return 1
        if (( (8#$mode & 8#22) != 0 )); then
            echo "Test RAM root has replaceable ancestor: $current (mode $mode)" >&2
            return 1
        fi
        if [[ "$current" == "/" ]]; then
            break
        fi
        current="$(dirname -- "$current")"
    done

    available_bytes="$(
        df -B1 --output=avail "$parent" | tail -n 1 | tr -d '[:space:]'
    )" || return 1
    if (( available_bytes < minimum_bytes )); then
        echo \
            "Test RAM root lacks headroom: $parent has $available_bytes bytes, needs $minimum_bytes" \
            >&2
        return 1
    fi

    _CRATEDIGGER_TEST_TMP_PARENT="$parent"
    _CRATEDIGGER_TEST_TMPDIR="$(
        mktemp -d "$parent/cratedigger-tests.XXXXXX"
    )" || return 1
    export TMPDIR="$_CRATEDIGGER_TEST_TMPDIR"
    trap _exit_cratedigger_test_tmpfs EXIT
}
