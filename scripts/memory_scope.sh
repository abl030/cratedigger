#!/usr/bin/env bash
# Transient-cgroup memory containment for canonical suite launches.
#
# WHY THIS EXISTS
#
# On 2026-08-19 a leaking test phase (#1214, one tmpfs world per Hypothesis
# example) drove doc1 into a GLOBAL out-of-memory event. The kernel log is
# explicit about the blast radius:
#
#     oom-kill:constraint=CONSTRAINT_NONE,...,global_oom
#
# CONSTRAINT_NONE means no cgroup limit bounded the allocation, so the kernel
# chose victims across the WHOLE machine by oom_score rather than killing the
# offender. Six processes died, every one a Python `MainThread` worker holding
# 0.7-3.0 GB, and four gate units ended `Failed with result 'oom-kill'`.
#
# The leak itself is fixed. What is NOT fixed is the shape: nothing bounds a
# suite run to its own share of the host, so the next runaway again takes out
# whatever else the operator happened to be running -- including the Claude
# daemon that owns their background agent sessions, which shares this user
# slice and would be picked off as collateral.
#
# A transient scope with a memory limit converts that global event into a
# local one: reclaim pressure applies to the suite's own cgroup first, and
# past the hard limit only the suite's processes are killed. The host, and
# everything else on it, survives.
#
# SIZING IS DERIVED, NEVER A CONSTANT
#
# A limit tuned to this homelab's 32 GiB box is precisely the
# host-specific rule this repository rejects (#1079), and `scope.md` requires
# that defaults not assume this installation. Both limits are therefore
# fractions of the host's own MemTotal.
#
# The fraction is chosen against measured evidence, not taste.
# `scripts/daily_resource_monitor.sh` telemetry from 2026-08-20 (i.e. after
# the #1214 fix, so a healthy run) recorded the `deterministic_suite` phase
# peaking at 22.41 GB of user-slice memory across 15,746 samples, against a
# 14.56 GB baseline in the same run -- the suite's OWN share is therefore
# roughly 8 GB. On a 32 GiB host this fraction gives ~22.4 GiB before a kill:
# about 2.8x measured headroom.
#
# MemoryHigh IS DELIBERATELY NOT SET. It looks like the obvious gentler first
# tier -- throttle and reclaim before killing anything -- and it is a trap
# here. Measured on doc1 while writing this file: with `-p MemoryHigh` set
# below MemoryMax and swap capped, a runaway allocation did not die at
# MemoryMax at all. It stalled in the kernel's reclaim-throttle loop for over
# 120 seconds with nothing to reclaim, and had to be killed by hand. For an
# unattended nightly gate that converts "fail fast with exit 137" into "hang
# forever and never report", which is worse than the uncontained behaviour
# this file exists to fix. A single hard limit fails promptly and legibly.
#
# MemorySwapMax=0 IS LOAD-BEARING, not belt-and-braces. MemoryMax alone does
# NOT bound a cgroup on a host with swap: it bounds RESIDENT memory, and the
# excess is reclaimed to swap rather than refused. Measured on doc1 while
# writing this file -- a 200 MiB allocation under `-p MemoryMax=64M` ran to
# completion and exited 0, because 136 MiB simply spilled into the 16 GiB
# swapfile. The same allocation with `-p MemorySwapMax=0` added was killed
# (exit 137). Without the swap limit a leaking suite balloons into swap and
# thrashes the host instead of dying quickly -- nearly as damaging as the
# global OOM this exists to prevent, and much harder to diagnose.
#
# The cost is that tmpfs pages become unevictable, so they count permanently
# against MemoryMax. That is the right trade here twice over: the measured
# suite share (~8 GB, below) already includes its tmpfs, leaving wide margin
# under the limit; and uncontrolled tmpfs growth was the #1214 failure mode
# itself, so charging it against the cap is the containment, not a side
# effect.
_CRATEDIGGER_MEMORY_MAX_PERCENT=70

# Emitted once per launch when containment cannot be established, so an
# uncontained run is always visible in gate output rather than silently
# behaving like the pre-#1214 world.
_cratedigger_memory_scope_warn() {
    printf 'memory containment unavailable (%s); running uncontained\n' "$1" >&2
}

# _CRATEDIGGER_MEMINFO_PATH and _CRATEDIGGER_CGROUP_ROOT default to the real
# kernel interfaces, so every production caller is unaffected. They exist so a
# test can pin the MemTotal -> MemoryMax percentage arithmetic against a KNOWN
# fixture, and drive the not-delegated branch, neither of which can otherwise
# be forced to a controlled value. This mirrors the `meminfo_path` seam
# `_measure_available_memory_bytes` already uses for the same reason
# (issue #1156 review F4).
_cratedigger_mem_total_bytes() {
    local key value
    while read -r key value _; do
        if [[ "$key" == "MemTotal:" ]]; then
            printf '%s\n' "$((value * 1024))"
            return 0
        fi
    done <"${_CRATEDIGGER_MEMINFO_PATH:-/proc/meminfo}"
    return 1
}

# Populate CRATEDIGGER_MEMORY_SCOPE_ARGV with a systemd-run prefix, or leave
# it empty when containment is unavailable.
#
# Fail-open is deliberate and asymmetric:
#
#   * MISSING INFRASTRUCTURE (no systemd-run, no delegated memory controller,
#     unreadable MemTotal) leaves the array empty and warns. Containment is a
#     safety net, not a correctness gate; refusing to run the suite because a
#     cgroup could not be created would trade a rare host OOM for a common
#     hard stop, and the suite's own result is unaffected by running
#     uncontained.
#
#   * AN INVALID EXPLICIT OVERRIDE fails closed with a non-zero return. The
#     operator asked for a specific limit and typo'd it; silently ignoring
#     that would run with a limit they did not choose. This mirrors
#     `headroom_floor_bytes`, which raises on a malformed
#     CRATEDIGGER_TEST_RAM_MIN_BYTES rather than falling back.
cratedigger_memory_scope_prefix() {
    CRATEDIGGER_MEMORY_SCOPE_ARGV=()

    # An inner launcher inside an already-scoped run would nest a second
    # cgroup under the first. Harmless (the outer limit still binds) but
    # pointless, and it would double the warning noise when containment is
    # unavailable.
    if [[ -n "${CRATEDIGGER_MEMORY_SCOPE_ACTIVE:-}" ]]; then
        return 0
    fi

    local mem_total max raw
    raw="${CRATEDIGGER_TEST_MEMORY_MAX_BYTES:-}"
    if [[ -n "$raw" ]]; then
        if [[ ! "$raw" =~ ^[0-9]+$ ]]; then
            printf 'CRATEDIGGER_TEST_MEMORY_MAX_BYTES must be a non-negative integer: %s\n' \
                "$raw" >&2
            return 1
        fi
        # An explicit 0 disables containment outright -- the documented
        # escape hatch for a host where the scope itself is the problem.
        if [[ "$raw" == 0 ]]; then
            return 0
        fi
        max="$raw"
    else
        if ! mem_total=$(_cratedigger_mem_total_bytes); then
            _cratedigger_memory_scope_warn "cannot read MemTotal from /proc/meminfo"
            return 0
        fi
        max=$((mem_total * _CRATEDIGGER_MEMORY_MAX_PERCENT / 100))
    fi

    if ! command -v systemd-run >/dev/null 2>&1; then
        _cratedigger_memory_scope_warn "systemd-run is not available"
        return 0
    fi

    # The user manager must have the memory controller delegated, otherwise
    # systemd-run accepts the -p flags and silently enforces nothing. Checking
    # the delegated controller list is cheaper and more precise than probing
    # with a throwaway scope, and it is the exact condition that fails on a
    # host without cgroup v2 memory delegation.
    local cgroup_root="${_CRATEDIGGER_CGROUP_ROOT:-/sys/fs/cgroup}"
    local controllers="$cgroup_root/user.slice/user-$(id -u).slice/user@$(id -u).service/cgroup.controllers"
    if [[ ! -r "$controllers" ]]; then
        _cratedigger_memory_scope_warn "user manager cgroup is unreadable: $controllers"
        return 0
    fi
    if [[ " $(<"$controllers") " != *" memory "* ]]; then
        _cratedigger_memory_scope_warn "memory controller is not delegated to the user manager"
        return 0
    fi

    CRATEDIGGER_MEMORY_SCOPE_ARGV=(
        systemd-run --user --scope --quiet --collect
        --setenv=CRATEDIGGER_MEMORY_SCOPE_ACTIVE=1
        -p "MemoryMax=$max"
        -p "MemorySwapMax=0"
    )
    return 0
}
