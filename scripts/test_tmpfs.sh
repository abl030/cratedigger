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
    local marker_writer

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

    # scripts/test.sh sets this before its own `nix develop` invocation, and
    # the final gate (scripts/test_substrate.py, reached through
    # scripts/run_final_gate.sh) sets it in the environment of the
    # `nix develop` child it launches: run_suite() (scripts/run_test_suite.py)
    # takes an exclusive admission lock and its own post-lock headroom
    # precondition for every suite run, canonical or targeted, so it is the
    # single enforcement point there. Without this skip, a second concurrently-
    # launched suite would die right here at shell entry with this unnamed
    # message instead of queueing on the lock (issue #1111 review M2) — this
    # setup still runs in full otherwise; only the free-bytes refusal defers.
    # This is inherited process environment, not an entry-time flag: a
    # NESTED nix-shell a test spawns as its own subprocess also inherits it
    # from the enclosing suite and skips the same refusal — deliberately,
    # since the enclosing run_suite() already owns headroom enforcement for
    # the whole run. No test currently nests a nix-shell this way —
    # tests/test_decision_corpus_export.py used to nest six such calls;
    # issue #1131 removed the nesting since the already-active interpreter
    # needed no re-entry. This is the SAME `if` check either way, so it is
    # not dormant: it still runs, and skips, on every suite invocation
    # started by scripts/test.sh, the final gate, and
    # scripts/daily_flake_update.sh, which all set this var before the
    # dev-shell entry they drive. Only the NESTED case — one shell
    # spawning another as its own subprocess and inheriting the var that
    # way — currently has no live example; nothing here needs to change
    # for the next test that legitimately needs it. Only a genuinely
    # interactive nix-shell entry, started outside any suite run, never has
    # this set and keeps its own entry guard.
    if [[ "${CRATEDIGGER_SUITE_OWNS_HEADROOM:-}" != "1" ]]; then
        available_bytes="$(
            df -B1 --output=avail "$parent" | tail -n 1 | tr -d '[:space:]'
        )" || return 1
        if (( available_bytes < minimum_bytes )); then
            echo \
                "Test RAM root lacks headroom: $parent has $available_bytes bytes, needs $minimum_bytes" \
                >&2
            return 1
        fi
    fi

    _CRATEDIGGER_TEST_TMP_PARENT="$parent"
    _CRATEDIGGER_TEST_TMPDIR="$(
        mktemp -d "$parent/cratedigger-tests.XXXXXX"
    )" || return 1
    export TMPDIR="$_CRATEDIGGER_TEST_TMPDIR"

    # Ownership marker (issue #1208 item 1): a "<pid> <ticks>\n" pair naming
    # THIS shell process, written immediately after mktemp so the window
    # during which the directory exists with no marker at all is as small
    # as possible. scripts/test_substrate.py::_scratch_tree_owner_dead reads
    # it on the reaping side and verifies liveness with a pid-reuse-safe
    # start-ticks comparison — never on readability alone. A missing or
    # unparseable marker (this write failing, or a reap racing the tiny
    # window before it lands) is read as "unknown, never reap", not
    # "abandoned" — fail closed, not fail open, which is why this write is
    # best-effort: a lost write leaves the tree unreaped forever rather
    # than wrongly reaped while live.
    #
    # Issue #1278 item 6: the marker's FORMAT and the /proc start-ticks read
    # behind it now live in exactly one place, beside the reader that
    # consumes them, rather than in a bash copy here that could drift from
    # it silently.
    #
    # Anchor the substrate at the repo TOP LEVEL, never $PWD — the same
    # resolution (and the same fallback) nix/shell.nix already uses for its
    # two GC roots, for the same reason (issue #1208 item 3). A shell
    # entered by path from anywhere else (`nix develop ~/cratedigger`, or
    # from a subdirectory) has a $PWD that is not the repository, so a
    # relative path would write NO marker at all there, re-opening the
    # #1208 item 1 leak this marker exists to close — and would run
    # whatever `scripts/test_substrate.py` that other directory happened
    # to contain. Residual, stated rather than defended against: when $PWD
    # is inside an UNRELATED git repository, `--show-toplevel` resolves to
    # that repository, exactly as the GC roots above already land there;
    # when it is in no repository at all, the $PWD fallback simply finds
    # no substrate and writes no marker (fail closed — that tree is then
    # never reaped, never wrongly reaped).
    #
    # The CLI is best-effort by construction (it exits 0 on every failure
    # and writes nothing), and the two guards on the call itself keep that
    # true from this side as well: `[[ -f ... ]]` skips the call when no
    # substrate is there to run, and `|| true` keeps a lost marker from
    # failing shell entry itself. Both redirections are here rather than
    # inside the CLI because a shell hook must stay silent on stdout (its
    # caller reads TMPDIR from there) and on stderr (issue #1208 review
    # D5) — and because the command may fail before the CLI runs at all
    # (no python3 on PATH), which is bash's diagnostic to suppress, not
    # the CLI's.
    marker_writer="$(
        git rev-parse --show-toplevel 2>/dev/null || echo "$PWD"
    )/scripts/test_substrate.py"
    if [[ -f "$marker_writer" ]]; then
        python3 "$marker_writer" write-owner-marker \
            "$_CRATEDIGGER_TEST_TMPDIR" "$$" >/dev/null 2>&1 || true
    fi

    trap _exit_cratedigger_test_tmpfs EXIT
}
