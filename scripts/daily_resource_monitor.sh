#!/usr/bin/env bash
# Phase-correlated cgroup and private-scratch receipts for the daily gate.
#
# The monitor's OWN bookkeeping (samples, the phase pointer, its lock) must
# never live on the filesystem it is measuring (issue #1214): on 2026-08-20
# the measured scratch tmpfs filled, the monitor's own state directory was
# mktemp'd inside that same tmpfs, its writes hit ENOSPC alongside the
# workload's, and the run's entire per-phase breakdown was lost for exactly
# the one night it was needed. State now lives under a separately-verified
# root (see daily_resource_monitor_start), proven distinct from the
# measured scratch by filesystem identity, with a fallback candidate list
# rather than a single hardcoded path.
#
# This is the whole fix. Rounds 2-4 (issue #1214 reviews F1-F9, C1-C9,
# C-F1-C-F5) layered an increasingly elaborate loss-accounting system on
# top of it -- writer identity, per-sample sequence numbers, a fifo report
# channel from the periodic loop, parent/loop drop counters, a
# `dropped_samples`/`missing_phases`/`corrupted_history_lines` receipt
# triple, a `degraded` status, a phase-history file -- and each round's fix
# for that layer re-introduced the same class of bug one call site over
# (review C-F1 found the fourth such site). That was a design smell, not a
# testing gap: the entire accounting layer existed to work around a
# failing state store. Once the state root is verified off the measured
# filesystem (above), a write failure there is a rare, genuinely
# exceptional event, not routine disk pressure -- so it is handled the
# plain way, matching what this monitor did before issue #1214's
# accounting rounds: a writer that fails just says so, in-process, and the
# run is `invalid`. No counting.
#
# **The invariant: a receipt must never say `status=valid` if anything
# failed to record.** Any failed sample write (boundary, final, or the
# periodic loop's), any failed phase-pointer write, or the loop dying
# outright all make the terminal status `invalid` -- binary, not
# quantified -- while the per-phase breakdown for whatever DID survive is
# still printed above it, because losing that breakdown for the one run
# that needed it is the entire reason this file exists (issue #1214,
# 2026-08-20). Structural failures inside `daily_resource_monitor_set_phase`
# (an invalid phase name, a stuck lock, a failed phase-pointer write, a
# failed unlock) travel as a plain in-process bash variable
# (`_CRATEDIGGER_RESOURCE_FAILURE_REASON`) rather than a marker file:
# `set_phase` and `daily_resource_monitor_finish` always run in the ONE
# parent process (nothing here is forked except the periodic loop itself),
# so a bash variable assignment -- which cannot itself fail the way a
# filesystem write can -- is sufficient and simpler than any file-based
# signal. The periodic loop is a genuinely separate, forked process; it
# has no report channel of any kind (issue #1214 review round 5's fifo was
# cut in the round-6 strip-back) -- on its first failed sample write it
# simply stops (matching this file's own pre-accounting shape), and
# `daily_resource_monitor_finish`'s ordinary `wait` on its PID observes
# that exit status directly, no inter-process signaling required.
#
# A row that lands but is malformed (a real full filesystem does not
# reject a write atomically -- issue #1214 review F2, measured; a partial
# page can land and the next append then concatenates onto its
# unterminated tail) is skipped and the summary keeps going, rather than
# discarding every other row's evidence for one corrupt line -- this is
# what makes the receipt useful under real ENOSPC pressure on the state
# store itself, the difference between "we have the phase breakdown" and
# "we have nothing."

_daily_resource_invalid() {
    local reason="$1"
    _CRATEDIGGER_RESOURCE_TERMINAL_EMITTED=1
    printf \
        'CRATEDIGGER_DAILY_RESOURCE_RECEIPT schema=1 status=invalid reason=%s\n' \
        "$reason"
    return 1
}

daily_resource_summarize_samples() {
    local samples_file="$1"
    local external_reason="${2:-}"
    local timestamp_ns phase memory_current memory_peak swap_current swap_peak
    local anon file_bytes shmem kernel scratch_bytes scratch_byte_limit
    local scratch_inodes scratch_inode_limit extra value field
    local sample_count=0 global_memory_peak=-1 global_swap_peak=-1
    local global_scratch_byte_peak=0 global_scratch_inode_peak=0
    local global_scratch_byte_limit=-1 global_scratch_inode_limit=-1
    local memory_peak_owner=""
    local previous_memory_peak=-1 previous_swap_peak=-1
    local -a phase_order=()
    local -A phase_seen=()
    local -A phase_samples=()
    local -A phase_memory_current_peak=()
    local -A phase_memory_peak_start=()
    local -A phase_memory_peak_end=()
    local -A phase_swap_current_peak=()
    local -A phase_swap_peak_start=()
    local -A phase_swap_peak_end=()
    local -A phase_anon_at_memory_peak=()
    local -A phase_file_at_memory_peak=()
    local -A phase_shmem_at_memory_peak=()
    local -A phase_kernel_at_memory_peak=()
    local -A phase_scratch_at_memory_peak=()
    local -A phase_scratch_inodes_at_memory_peak=()
    local -A phase_memory_peak_timestamp=()
    local -A phase_scratch_byte_peak=()
    local -A phase_scratch_inode_peak=()
    local malformed_row

    if [[ ! -r "$samples_file" ]]; then
        _daily_resource_invalid samples_unreadable
        return
    fi

    while IFS=$'\t' read -r \
        timestamp_ns phase memory_current memory_peak \
        swap_current swap_peak anon file_bytes shmem kernel scratch_bytes \
        scratch_byte_limit scratch_inodes scratch_inode_limit extra
    do
        # A real full filesystem does not reject a write atomically: a
        # partial page can land, and the NEXT successful append then
        # concatenates onto its unterminated tail (issue #1214 review F2,
        # measured). That produces a row with the wrong shape or
        # non-numeric values, not a missing row. Skip it rather than
        # discarding every other row's evidence for one corrupt line.
        malformed_row=0
        if [[ -n "${extra:-}" || ! "$phase" =~ ^[a-z][a-z0-9_]*$ ]]; then
            malformed_row=1
        else
            for field in \
                timestamp_ns memory_current memory_peak swap_current \
                swap_peak anon file_bytes shmem kernel scratch_bytes \
                scratch_byte_limit scratch_inodes scratch_inode_limit
            do
                value="${!field}"
                if [[ ! "$value" =~ ^[0-9]+$ ]]; then
                    malformed_row=1
                    break
                fi
            done
        fi
        if ((malformed_row)); then
            continue
        fi

        # From here every field is well-formed. The checks below are
        # cross-row/semantic invariants (limits, monotonic peaks, a
        # physically-impossible memory breakdown) rather than row-shape
        # corruption signatures; they stay hard failures -- a violation
        # here is evidence of a real measurement or logic defect, not
        # ordinary disk-pressure noise.
        if ((scratch_byte_limit == 0 || scratch_inode_limit == 0)); then
            _daily_resource_invalid scratch_limit_invalid
            return
        fi
        if ((scratch_bytes > scratch_byte_limit || scratch_inodes > scratch_inode_limit)); then
            _daily_resource_invalid scratch_usage_invalid
            return
        fi
        if ((memory_peak < memory_current || swap_peak < swap_current)); then
            _daily_resource_invalid cgroup_peak_invalid
            return
        fi
        if ((shmem > file_bytes || shmem > memory_current)); then
            _daily_resource_invalid memory_breakdown_invalid
            return
        fi
        if ((previous_memory_peak >= 0 && memory_peak < previous_memory_peak)); then
            _daily_resource_invalid memory_peak_regressed
            return
        fi
        if ((previous_swap_peak >= 0 && swap_peak < previous_swap_peak)); then
            _daily_resource_invalid swap_peak_regressed
            return
        fi
        previous_memory_peak=$memory_peak
        previous_swap_peak=$swap_peak

        if ((global_scratch_byte_limit < 0)); then
            global_scratch_byte_limit=$scratch_byte_limit
            global_scratch_inode_limit=$scratch_inode_limit
        elif ((
            scratch_byte_limit != global_scratch_byte_limit
            || scratch_inode_limit != global_scratch_inode_limit
        )); then
            _daily_resource_invalid scratch_limit_changed
            return
        fi

        if [[ -z "${phase_seen[$phase]:-}" ]]; then
            phase_seen[$phase]=1
            phase_order+=("$phase")
            phase_samples[$phase]=0
            phase_memory_current_peak[$phase]=-1
            phase_memory_peak_start[$phase]=$memory_peak
            phase_swap_current_peak[$phase]=-1
            phase_swap_peak_start[$phase]=$swap_peak
            phase_scratch_byte_peak[$phase]=0
            phase_scratch_inode_peak[$phase]=0
        fi

        phase_samples[$phase]=$((phase_samples[$phase] + 1))
        phase_memory_peak_end[$phase]=$memory_peak
        phase_swap_peak_end[$phase]=$swap_peak
        if ((memory_current > phase_memory_current_peak[$phase])); then
            phase_memory_current_peak[$phase]=$memory_current
            phase_anon_at_memory_peak[$phase]=$anon
            phase_file_at_memory_peak[$phase]=$file_bytes
            phase_shmem_at_memory_peak[$phase]=$shmem
            phase_kernel_at_memory_peak[$phase]=$kernel
            phase_scratch_at_memory_peak[$phase]=$scratch_bytes
            phase_scratch_inodes_at_memory_peak[$phase]=$scratch_inodes
            phase_memory_peak_timestamp[$phase]=$timestamp_ns
        fi
        if ((swap_current > phase_swap_current_peak[$phase])); then
            phase_swap_current_peak[$phase]=$swap_current
        fi
        if ((scratch_bytes > phase_scratch_byte_peak[$phase])); then
            phase_scratch_byte_peak[$phase]=$scratch_bytes
        fi
        if ((scratch_inodes > phase_scratch_inode_peak[$phase])); then
            phase_scratch_inode_peak[$phase]=$scratch_inodes
        fi

        if ((memory_peak > global_memory_peak)); then
            global_memory_peak=$memory_peak
            memory_peak_owner=$phase
        fi
        if ((swap_peak > global_swap_peak)); then
            global_swap_peak=$swap_peak
        fi
        if ((scratch_bytes > global_scratch_byte_peak)); then
            global_scratch_byte_peak=$scratch_bytes
        fi
        if ((scratch_inodes > global_scratch_inode_peak)); then
            global_scratch_inode_peak=$scratch_inodes
        fi
        sample_count=$((sample_count + 1))
    done < <(sort -n -k1,1 -- "$samples_file")

    if ((sample_count == 0)); then
        _daily_resource_invalid samples_empty
        return
    fi

    for phase in "${phase_order[@]}"; do
        printf '%s' 'CRATEDIGGER_DAILY_RESOURCE_PHASE schema=1'
        printf ' phase=%s samples=%d' "$phase" "${phase_samples[$phase]}"
        printf ' memory_current_peak_bytes=%d' "${phase_memory_current_peak[$phase]}"
        printf ' memory_peak_start_bytes=%d' "${phase_memory_peak_start[$phase]}"
        printf ' memory_peak_end_bytes=%d' "${phase_memory_peak_end[$phase]}"
        printf ' swap_current_peak_bytes=%d' "${phase_swap_current_peak[$phase]}"
        printf ' swap_peak_start_bytes=%d' "${phase_swap_peak_start[$phase]}"
        printf ' swap_peak_end_bytes=%d' "${phase_swap_peak_end[$phase]}"
        printf ' anon_at_memory_current_peak_bytes=%d' "${phase_anon_at_memory_peak[$phase]}"
        printf ' file_at_memory_current_peak_bytes=%d' "${phase_file_at_memory_peak[$phase]}"
        printf ' shmem_at_memory_current_peak_bytes=%d' "${phase_shmem_at_memory_peak[$phase]}"
        printf ' non_shmem_at_memory_current_peak_bytes=%d' "$((
            phase_memory_current_peak[$phase] - phase_shmem_at_memory_peak[$phase]
        ))"
        printf ' kernel_at_memory_current_peak_bytes=%d' "${phase_kernel_at_memory_peak[$phase]}"
        printf ' scratch_at_memory_current_peak_bytes=%d' "${phase_scratch_at_memory_peak[$phase]}"
        printf ' scratch_inode_at_memory_current_peak=%d' "${phase_scratch_inodes_at_memory_peak[$phase]}"
        printf ' scratch_byte_peak=%d' "${phase_scratch_byte_peak[$phase]}"
        printf ' scratch_inode_peak=%d' "${phase_scratch_inode_peak[$phase]}"
        printf ' scratch_byte_limit=%d' "$global_scratch_byte_limit"
        printf ' scratch_inode_limit=%d' "$global_scratch_inode_limit"
        printf ' memory_current_peak_timestamp_ns=%d\n' "${phase_memory_peak_timestamp[$phase]}"
    done

    # The per-phase breakdown above is printed REGARDLESS of
    # external_reason -- that is the whole point (issue #1214, the
    # 2026-08-20 incident lost exactly this breakdown). Only the terminal
    # status differs: any external failure (a failed sample write outside
    # this function, a failed phase write, the loop dying) makes it
    # invalid, binary, no partial credit for the data that did survive.
    if [[ -n "$external_reason" ]]; then
        _daily_resource_invalid "$external_reason"
        return
    fi

    printf '%s' 'CRATEDIGGER_DAILY_RESOURCE_RECEIPT schema=1 status=valid'
    printf ' samples=%d phases=%d' "$sample_count" "${#phase_order[@]}"
    printf ' memory_peak_bytes=%d memory_peak_owner=%s' \
        "$global_memory_peak" "$memory_peak_owner"
    printf ' swap_peak_bytes=%d' "$global_swap_peak"
    printf ' scratch_byte_peak=%d scratch_byte_limit=%d' \
        "$global_scratch_byte_peak" "$global_scratch_byte_limit"
    printf ' scratch_inode_peak=%d scratch_inode_limit=%d\n' \
        "$global_scratch_inode_peak" "$global_scratch_inode_limit"
}

_daily_resource_record_sample_once() {
    local phase="$1"
    local memory_current memory_peak swap_current swap_peak
    local anon="" file_bytes="" shmem="" kernel="" key value
    local blocks free_blocks block_size total_inodes free_inodes
    local scratch_bytes scratch_inodes timestamp_ns

    # Capture the ordering witness before reading metrics. Parent boundary
    # samples and the periodic child may overlap, so terminal aggregation
    # sorts by this timestamp instead of trusting concurrent append order.
    timestamp_ns="$(date +%s%N)" || return 1
    memory_current="$(<"$_CRATEDIGGER_RESOURCE_CGROUP_DIR/memory.current")" || return 1
    memory_peak="$(<"$_CRATEDIGGER_RESOURCE_CGROUP_DIR/memory.peak")" || return 1
    swap_current="$(<"$_CRATEDIGGER_RESOURCE_CGROUP_DIR/memory.swap.current")" || return 1
    swap_peak="$(<"$_CRATEDIGGER_RESOURCE_CGROUP_DIR/memory.swap.peak")" || return 1
    while read -r key value; do
        case "$key" in
            anon) anon="$value" ;;
            file) file_bytes="$value" ;;
            shmem) shmem="$value" ;;
            kernel) kernel="$value" ;;
        esac
    done < "$_CRATEDIGGER_RESOURCE_CGROUP_DIR/memory.stat" || return 1
    if [[ -z "$anon" || -z "$file_bytes" || -z "$shmem" || -z "$kernel" ]]; then
        return 1
    fi
    # memory.current and memory.stat are separate kernel reads. Churn can make
    # a cross-read breakdown internally impossible; do not publish negative
    # non-shmem attribution from that transient world.
    if ((shmem > file_bytes || shmem > memory_current)); then
        return 1
    fi
    read -r blocks free_blocks block_size total_inodes free_inodes < <(
        stat --file-system --format='%b %f %S %c %d' -- \
            "$_CRATEDIGGER_RESOURCE_SCRATCH"
    ) || return 1
    scratch_bytes=$(((blocks - free_blocks) * block_size))
    scratch_inodes=$((total_inodes - free_inodes))
    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
        "$timestamp_ns" "$phase" "$memory_current" "$memory_peak" \
        "$swap_current" "$swap_peak" "$anon" "$file_bytes" "$shmem" \
        "$kernel" "$scratch_bytes" "$((blocks * block_size))" \
        "$scratch_inodes" "$total_inodes" \
        >> "$_CRATEDIGGER_RESOURCE_SAMPLES" || return 1
}

_daily_resource_record_sample_unlocked() {
    local phase="$1" attempts=3
    while ((attempts > 0)); do
        if _daily_resource_record_sample_once "$phase"; then
            return 0
        fi
        attempts=$((attempts - 1))
        sleep 0.01
    done
    return 1
}

_daily_resource_lock() {
    if [[ "${_CRATEDIGGER_RESOURCE_LOCK_HELD:-0}" == 1 ]]; then
        return 2
    fi
    exec {_CRATEDIGGER_RESOURCE_LOCK_FD}>"$_CRATEDIGGER_RESOURCE_LOCK" \
        || return 1
    if ! flock -x -w 2 "$_CRATEDIGGER_RESOURCE_LOCK_FD"; then
        exec {_CRATEDIGGER_RESOURCE_LOCK_FD}>&-
        return 1
    fi
    _CRATEDIGGER_RESOURCE_LOCK_HELD=1
}

_daily_resource_unlock() {
    if [[ "${_CRATEDIGGER_RESOURCE_LOCK_HELD:-0}" != 1 ]]; then
        return 0
    fi
    exec {_CRATEDIGGER_RESOURCE_LOCK_FD}>&- || return 1
    _CRATEDIGGER_RESOURCE_LOCK_HELD=0
    unset _CRATEDIGGER_RESOURCE_LOCK_FD
}

_daily_resource_record_sample_locked() {
    local phase="$1"
    local status=0
    _daily_resource_lock || return 1
    _daily_resource_record_sample_unlocked "$phase" || status=1
    _daily_resource_unlock || status=1
    return "$status"
}

_daily_resource_record_current_phase_locked() {
    local phase=""
    local status=0
    _daily_resource_lock || return 1
    phase="$(<"$_CRATEDIGGER_RESOURCE_PHASE")" || status=1
    if [[ -z "$phase" ]]; then
        status=1
    elif ((status == 0)); then
        _daily_resource_record_sample_unlocked "$phase" || status=1
    fi
    _daily_resource_unlock || status=1
    return "$status"
}

_daily_resource_write_phase() {
    local phase="$1"
    local temporary="$_CRATEDIGGER_RESOURCE_PHASE.next"
    printf '%s\n' "$phase" > "$temporary" || return 1
    mv -f -- "$temporary" "$_CRATEDIGGER_RESOURCE_PHASE"
}

_daily_resource_monitor_loop() {
    # A single failed sample write (cgroup churn, or a genuine state-store
    # failure) stops the loop outright -- this is deliberately fatal, not
    # tolerated (issue #1214 round-6 strip-back): with the state root
    # verified off the measured filesystem, a write failure here is a
    # rare, exceptional event, not routine disk pressure, so there is
    # nothing useful left to quantify. daily_resource_monitor_finish's own
    # `wait` on this process's PID observes the resulting nonzero exit
    # directly; no report channel back to the parent is needed.
    while [[ ! -e "$_CRATEDIGGER_RESOURCE_STOP" ]]; do
        _daily_resource_record_current_phase_locked || return 1
        sleep 0.25
    done
}

daily_resource_monitor_start() {
    local hierarchy controllers relative="" file filesystem
    local -a state_root_candidates=() rejected_candidates=()
    local candidate candidate_filesystem_id scratch_filesystem_id state_root=""
    local reached_scratch_collision=0

    _CRATEDIGGER_RESOURCE_STARTED=starting
    _CRATEDIGGER_RESOURCE_TERMINAL_EMITTED=0
    _CRATEDIGGER_RESOURCE_FAILURE_REASON=""
    _CRATEDIGGER_RESOURCE_SCRATCH="${XDG_RUNTIME_DIR:-}"
    if [[ -z "$_CRATEDIGGER_RESOURCE_SCRATCH" \
        || ! -d "$_CRATEDIGGER_RESOURCE_SCRATCH" ]]; then
        _daily_resource_invalid scratch_unavailable
        return
    fi
    filesystem="$(
        stat --file-system --format='%T' -- "$_CRATEDIGGER_RESOURCE_SCRATCH" \
            2>/dev/null
    )" || true
    if [[ "$filesystem" != tmpfs ]]; then
        _daily_resource_invalid scratch_not_tmpfs
        return
    fi

    while IFS=: read -r hierarchy controllers relative; do
        if [[ "$hierarchy" == 0 && -z "$controllers" && "$relative" == /* ]]; then
            break
        fi
        relative=""
    done < /proc/self/cgroup
    if [[ -z "$relative" ]]; then
        _daily_resource_invalid cgroup_unavailable
        return
    fi
    _CRATEDIGGER_RESOURCE_CGROUP_DIR="/sys/fs/cgroup$relative"
    for file in \
        memory.current memory.peak memory.swap.current memory.swap.peak memory.stat
    do
        if [[ ! -r "$_CRATEDIGGER_RESOURCE_CGROUP_DIR/$file" ]]; then
            _daily_resource_invalid cgroup_metric_unreadable
            return
        fi
    done

    # The monitor's own bookkeeping must survive the exact exhaustion it
    # exists to diagnose, so it cannot live under $_CRATEDIGGER_RESOURCE_SCRATCH
    # (that IS the tmpfs under measurement -- NOT a claim that this state
    # root avoids tmpfs in general; TMPDIR itself is commonly tmpfs, this
    # repo's own dev shell included). Try the caller's own TMPDIR first,
    # then /tmp, rather than trusting either blindly: on the deployed
    # daily-checks unit TMPDIR is unset, so this resolves to the unit's
    # real host /tmp (PrivateTmp=yes); but interactively (this repo's own
    # dev shell) TMPDIR can be pointed at the SAME tmpfs as
    # $XDG_RUNTIME_DIR (issue #1214 review F9), and refusing outright with
    # no fallback made the ordinary interactive path unusable. Verified by
    # filesystem identity, not path convention: a candidate that collides
    # is rejected and the next one tried, not silently trusted.
    scratch_filesystem_id="$(
        stat --file-system --format='%i' -- "$_CRATEDIGGER_RESOURCE_SCRATCH" \
            2>/dev/null
    )" || true
    [[ -n "${TMPDIR:-}" ]] && state_root_candidates+=("$TMPDIR")
    [[ "${TMPDIR:-}" != /tmp ]] && state_root_candidates+=(/tmp)
    for candidate in "${state_root_candidates[@]}"; do
        if [[ ! -d "$candidate" || ! -w "$candidate" ]]; then
            rejected_candidates+=("$candidate: missing or not writable")
            continue
        fi
        candidate_filesystem_id="$(
            stat --file-system --format='%i' -- "$candidate" 2>/dev/null
        )" || true
        if [[ -z "$candidate_filesystem_id" || -z "$scratch_filesystem_id" ]]; then
            rejected_candidates+=("$candidate: filesystem id unavailable")
            continue
        fi
        if [[ "$candidate_filesystem_id" == "$scratch_filesystem_id" ]]; then
            rejected_candidates+=(
                "$candidate: shares \$XDG_RUNTIME_DIR's filesystem"
            )
            reached_scratch_collision=1
            continue
        fi
        state_root="$candidate"
        break
    done
    if [[ -z "$state_root" ]]; then
        printf 'daily_resource_monitor: no usable state root; tried: %s; ' \
            "$(IFS='; '; echo "${rejected_candidates[*]:-none}")" >&2
        printf 'set TMPDIR to a writable path outside XDG_RUNTIME_DIR (%s)'"'"'s filesystem\n' \
            "$_CRATEDIGGER_RESOURCE_SCRATCH" >&2
        if ((reached_scratch_collision)); then
            _daily_resource_invalid state_root_shares_scratch_filesystem
        else
            _daily_resource_invalid state_root_unavailable
        fi
        return
    fi

    _CRATEDIGGER_RESOURCE_DIR="$(
        mktemp -d \
            "$state_root/cratedigger-daily-resource.XXXXXX"
    )" || {
        _daily_resource_invalid monitor_state_unavailable
        return
    }
    _CRATEDIGGER_RESOURCE_SAMPLES="$_CRATEDIGGER_RESOURCE_DIR/samples.tsv"
    _CRATEDIGGER_RESOURCE_PHASE="$_CRATEDIGGER_RESOURCE_DIR/phase"
    _CRATEDIGGER_RESOURCE_STOP="$_CRATEDIGGER_RESOURCE_DIR/stop"
    _CRATEDIGGER_RESOURCE_LOCK="$_CRATEDIGGER_RESOURCE_DIR/sample.lock"
    : > "$_CRATEDIGGER_RESOURCE_SAMPLES"
    : > "$_CRATEDIGGER_RESOURCE_LOCK"
    _CRATEDIGGER_RESOURCE_CURRENT_PHASE=bootstrap
    if ! _daily_resource_write_phase "$_CRATEDIGGER_RESOURCE_CURRENT_PHASE" \
        || ! _daily_resource_record_sample_unlocked \
            "$_CRATEDIGGER_RESOURCE_CURRENT_PHASE";
    then
        rm -rf -- "$_CRATEDIGGER_RESOURCE_DIR"
        _daily_resource_invalid state_store_bootstrap_failed
        return
    fi
    _daily_resource_monitor_loop &
    _CRATEDIGGER_RESOURCE_PID=$!
    _CRATEDIGGER_RESOURCE_STARTED=1
}

daily_resource_monitor_set_phase() {
    local phase="$1"
    if [[ "${_CRATEDIGGER_RESOURCE_STARTED:-0}" != 1 ]]; then
        return 1
    fi
    if [[ ! "$phase" =~ ^[a-z][a-z0-9_]*$ ]]; then
        _CRATEDIGGER_RESOURCE_FAILURE_REASON=phase_invalid
        return 1
    fi
    if ! _daily_resource_lock; then
        _CRATEDIGGER_RESOURCE_FAILURE_REASON=phase_lock_failed
        return 1
    fi
    # A boundary sample write failing is a real, reportable failure (issue
    # #1214 round-6 strip-back: binary, not counted), but never a reason
    # to abandon the phase transition itself -- that would leave the
    # shared phase pointer stuck on the old phase and mis-attribute every
    # later sample on top of the one already lost.
    _daily_resource_record_sample_unlocked "$_CRATEDIGGER_RESOURCE_CURRENT_PHASE" \
        || _CRATEDIGGER_RESOURCE_FAILURE_REASON=sample_write_failed
    if ! _daily_resource_write_phase "$phase"; then
        _daily_resource_unlock
        _CRATEDIGGER_RESOURCE_FAILURE_REASON=phase_state_write_failed
        return 1
    fi
    _CRATEDIGGER_RESOURCE_CURRENT_PHASE="$phase"
    _daily_resource_record_sample_unlocked "$_CRATEDIGGER_RESOURCE_CURRENT_PHASE" \
        || _CRATEDIGGER_RESOURCE_FAILURE_REASON=sample_write_failed
    if ! _daily_resource_unlock; then
        _CRATEDIGGER_RESOURCE_FAILURE_REASON=phase_unlock_failed
        return 1
    fi
}

daily_resource_monitor_finish() {
    local monitor_status=0 summary_status=0
    if [[ "${_CRATEDIGGER_RESOURCE_STARTED:-0}" != 1 ]]; then
        if [[ -n "${_CRATEDIGGER_RESOURCE_PID:-}" ]]; then
            [[ -n "${_CRATEDIGGER_RESOURCE_STOP:-}" ]] \
                && : > "$_CRATEDIGGER_RESOURCE_STOP"
            wait "$_CRATEDIGGER_RESOURCE_PID" 2>/dev/null || true
        fi
        if [[ -n "${_CRATEDIGGER_RESOURCE_DIR:-}" \
            && -d "$_CRATEDIGGER_RESOURCE_DIR" ]]; then
            rm -rf -- "$_CRATEDIGGER_RESOURCE_DIR"
        fi
        if [[ "${_CRATEDIGGER_RESOURCE_TERMINAL_EMITTED:-0}" != 1 ]]; then
            _daily_resource_invalid monitor_not_started
        fi
        return 1
    fi
    _daily_resource_record_sample_locked "$_CRATEDIGGER_RESOURCE_CURRENT_PHASE" \
        || _CRATEDIGGER_RESOURCE_FAILURE_REASON=sample_write_failed
    : > "$_CRATEDIGGER_RESOURCE_STOP"
    if ! wait "$_CRATEDIGGER_RESOURCE_PID"; then
        monitor_status=1
    fi
    if ((monitor_status != 0)) && [[ -z "$_CRATEDIGGER_RESOURCE_FAILURE_REASON" ]]; then
        _CRATEDIGGER_RESOURCE_FAILURE_REASON=monitor_process_died
    fi
    # Always summarize -- even when a failure is already known -- so the
    # per-phase breakdown for whatever survived is still printed above the
    # terminal receipt line (issue #1214's own invariant: losing that
    # breakdown is the entire reason this file exists).
    if ! daily_resource_summarize_samples \
        "$_CRATEDIGGER_RESOURCE_SAMPLES" "$_CRATEDIGGER_RESOURCE_FAILURE_REASON";
    then
        summary_status=1
    fi
    rm -rf -- "$_CRATEDIGGER_RESOURCE_DIR"
    _CRATEDIGGER_RESOURCE_STARTED=0
    return "$summary_status"
}

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
    set -euo pipefail
    case "${1:-}" in
        summarize)
            if [[ "$#" -lt 2 || "$#" -gt 3 ]]; then
                _daily_resource_invalid summarize_arguments_invalid
                exit 1
            fi
            daily_resource_summarize_samples "$2" "${3:-}"
            ;;
        *)
            _daily_resource_invalid command_invalid
            exit 1
            ;;
    esac
fi
