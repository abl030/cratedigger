#!/usr/bin/env bash
# Phase-correlated cgroup and private-scratch receipts for the daily gate.
#
# The monitor's OWN bookkeeping (samples, the phase pointer/history, its
# lock, the loop's report channel) must never live on the filesystem it is
# measuring (issue #1214): on 2026-08-20 the measured scratch tmpfs filled,
# the monitor's own state directory was mktemp'd inside that same tmpfs,
# its writes hit ENOSPC alongside the workload's, and the run's entire
# per-phase breakdown was lost for exactly the one night it was needed.
# State now lives under a separately-verified root (see
# daily_resource_monitor_start), proven distinct from the measured scratch
# by filesystem identity, with a fallback candidate list rather than a
# single hardcoded path.
#
# Loss is REPORTED by the writer that experienced it, never inferred after
# the fact (issue #1214 review C1: gap-inference in the samples data
# cannot see loss that persists to the end of a writer's own stream --
# there is no later surviving row to reveal the hole, so a permanent
# EACCES/EROFS/lock failure that never recovers produced a false
# status=valid). The caller's own bootstrap/boundary/final samples run in
# the SAME process that prints the receipt, so their failures are counted
# in-process directly. The periodic loop is a background child that
# daily_resource_monitor_finish already `wait`s on; it counts its own
# failed attempts in-process and reports the final total to the parent
# through a kernel pipe (a fifo opened once at start, while the state
# store is known-healthy) -- never through a file write that could fail
# with the exact store it would be reporting on.
#
# Sequence numbers and writer identity remain on every row as a
# corruption/reordering signal (a shape or value that fails validation --
# the signature a real, non-atomic full-filesystem write produces, review
# F2/measured) but are no longer the source of the drop count.

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
    local phase_history_file="$2"
    local parent_dropped="${3:-0}"
    local loop_dropped="${4:-0}"
    local timestamp_ns phase memory_current memory_peak swap_current swap_peak
    local anon file_bytes shmem kernel scratch_bytes scratch_byte_limit
    local scratch_inodes scratch_inode_limit extra value field
    local writer seq
    local sample_count=0 global_memory_peak=-1 global_swap_peak=-1
    local global_scratch_byte_peak=0 global_scratch_inode_peak=0
    local global_scratch_byte_limit=-1 global_scratch_inode_limit=-1
    local memory_peak_owner=""
    local previous_memory_peak=-1 previous_swap_peak=-1
    local corrupted_rows=0 corrupted_history_lines=0
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
    local -a history_phase_order=()
    local -A history_phase_seen=()
    local history_line malformed_row

    if [[ ! "$parent_dropped" =~ ^[0-9]+$ || ! "$loop_dropped" =~ ^[0-9]+$ ]]; then
        _daily_resource_invalid summarize_arguments_invalid
        return
    fi
    if [[ ! -r "$samples_file" ]]; then
        _daily_resource_invalid samples_unreadable
        return
    fi
    if [[ ! -r "$phase_history_file" ]]; then
        _daily_resource_invalid phase_history_unreadable
        return
    fi

    while IFS= read -r history_line; do
        if [[ ! "$history_line" =~ ^[a-z][a-z0-9_]*$ ]]; then
            corrupted_history_lines=$((corrupted_history_lines + 1))
            continue
        fi
        if [[ -z "${history_phase_seen[$history_line]:-}" ]]; then
            history_phase_seen[$history_line]=1
            history_phase_order+=("$history_line")
        fi
    done < "$phase_history_file"

    while IFS=$'\t' read -r \
        writer seq timestamp_ns phase memory_current memory_peak \
        swap_current swap_peak anon file_bytes shmem kernel scratch_bytes \
        scratch_byte_limit scratch_inodes scratch_inode_limit extra
    do
        # A real full filesystem does not reject a write atomically: a
        # partial page can land, and the NEXT successful append then
        # concatenates onto its unterminated tail (issue #1214 review F2,
        # measured). That produces a row with the wrong shape or
        # non-numeric values, not a missing row. Skip and count it rather
        # than discarding every other row's evidence for one corrupt line.
        malformed_row=0
        if [[ -n "${extra:-}" \
            || ! "$writer" =~ ^(loop|parent)$ \
            || ! "$phase" =~ ^[a-z][a-z0-9_]*$ ]]
        then
            malformed_row=1
        else
            for field in \
                seq timestamp_ns memory_current memory_peak swap_current \
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
            corrupted_rows=$((corrupted_rows + 1))
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
    done < <(sort -n -k3,3 -- "$samples_file")

    if ((sample_count == 0)); then
        _daily_resource_invalid samples_empty
        return
    fi

    local -a missing_phases=()
    local hphase
    for hphase in "${history_phase_order[@]}"; do
        if [[ -z "${phase_seen[$hphase]:-}" ]]; then
            missing_phases+=("$hphase")
        fi
    done

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
    for hphase in "${missing_phases[@]}"; do
        printf 'CRATEDIGGER_DAILY_RESOURCE_PHASE_MISSING schema=1 phase=%s\n' "$hphase"
    done

    # dropped_samples counts one thing: sample attempts that produced no
    # usable data point -- reported losses from each writer (issue #1214
    # review C1) plus rows that landed but are unparseable (review F2).
    # A malformed row is counted once, here, never also inferred as a
    # sequence gap (review C4). A corrupt phase-history LINE is a
    # different kind of loss (phase-tracking integrity, not a sample) and
    # gets its own field.
    local -i dropped_samples=$((parent_dropped + loop_dropped + corrupted_rows))
    local -i missing_phase_count=${#missing_phases[@]}
    local -i degraded=0
    ((dropped_samples > 0)) && degraded=1
    ((missing_phase_count > 0)) && degraded=1
    ((corrupted_history_lines > 0)) && degraded=1

    printf '%s' 'CRATEDIGGER_DAILY_RESOURCE_RECEIPT schema=1'
    if ((degraded)); then
        printf ' status=degraded reason=partial_sample_loss'
    else
        printf ' status=valid'
    fi
    printf ' dropped_samples=%d missing_phases=%d corrupted_history_lines=%d' \
        "$dropped_samples" "$missing_phase_count" "$corrupted_history_lines"
    printf ' samples=%d phases=%d' "$sample_count" "${#phase_order[@]}"
    printf ' memory_peak_bytes=%d memory_peak_owner=%s' \
        "$global_memory_peak" "$memory_peak_owner"
    printf ' swap_peak_bytes=%d' "$global_swap_peak"
    printf ' scratch_byte_peak=%d scratch_byte_limit=%d' \
        "$global_scratch_byte_peak" "$global_scratch_byte_limit"
    printf ' scratch_inode_peak=%d scratch_inode_limit=%d\n' \
        "$global_scratch_inode_peak" "$global_scratch_inode_limit"

    # A degraded receipt is still a signal something needs attention
    # (issue #1214 review C5): treat it the same as invalid for the
    # caller's own pass/fail purposes, so daily_flake_update.sh's existing
    # "an unclean resource receipt surfaces regardless of gate status"
    # logic covers it without duplicating that decision here.
    if ((degraded)); then
        return 1
    fi
}

_daily_resource_record_sample_once() {
    local phase="$1" writer="$2" seq="$3"
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
    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
        "$writer" "$seq" "$timestamp_ns" "$phase" "$memory_current" "$memory_peak" \
        "$swap_current" "$swap_peak" "$anon" "$file_bytes" "$shmem" \
        "$kernel" "$scratch_bytes" "$((blocks * block_size))" \
        "$scratch_inodes" "$total_inodes" \
        >> "$_CRATEDIGGER_RESOURCE_SAMPLES" || return 1
}

_daily_resource_record_sample_unlocked() {
    local phase="$1" writer="$2" seq="$3" attempts=3
    while ((attempts > 0)); do
        if _daily_resource_record_sample_once "$phase" "$writer" "$seq"; then
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
    local phase="$1" writer="$2" seq="$3"
    local status=0
    _daily_resource_lock || return 1
    _daily_resource_record_sample_unlocked "$phase" "$writer" "$seq" || status=1
    _daily_resource_unlock || status=1
    return "$status"
}

_daily_resource_record_current_phase_locked() {
    local writer="$1" seq="$2"
    local phase=""
    local status=0
    _daily_resource_lock || return 1
    phase="$(<"$_CRATEDIGGER_RESOURCE_PHASE")" || status=1
    if [[ -z "$phase" ]]; then
        status=1
    elif ((status == 0)); then
        _daily_resource_record_sample_unlocked "$phase" "$writer" "$seq" || status=1
    fi
    _daily_resource_unlock || status=1
    return "$status"
}

_daily_resource_write_phase() {
    local phase="$1"
    local temporary="$_CRATEDIGGER_RESOURCE_PHASE.next"
    printf '%s\n' "$phase" > "$temporary" || return 1
    mv -f -- "$temporary" "$_CRATEDIGGER_RESOURCE_PHASE" || return 1
    # A second, independently durable write (issue #1214 review F4): a
    # phase whose every metric sample fails would otherwise vanish from
    # the receipt with no trace at all. This CAN fail on its own, after
    # the pointer above has already swapped (review C9b) -- that is folded
    # into the SAME fatal outcome as the pointer write via this function's
    # own trailing exit status, exactly like the pointer write already
    # was; it does not introduce a silent divergence, but it is a second
    # failure point, not a free one.
    printf '%s\n' "$phase" >> "$_CRATEDIGGER_RESOURCE_PHASE_HISTORY"
}

_daily_resource_monitor_loop() {
    # A single failed sample (cgroup churn, a transient state-store write
    # failure, or a permanent one) is simply not written -- the loop never
    # dies on one, it only ever stops on the parent's own stop signal. Its
    # own count of failed attempts (whatever the cause -- issue #1214
    # review C6) is reported to the parent when it exits, through the
    # report fd set up in daily_resource_monitor_start: a kernel pipe, not
    # a file, so this report cannot fail with the store it describes
    # (review C1).
    local -i seq=0 dropped=0
    while [[ ! -e "$_CRATEDIGGER_RESOURCE_STOP" ]]; do
        seq=$((seq + 1))
        _daily_resource_record_current_phase_locked loop "$seq" \
            || dropped=$((dropped + 1))
        sleep 0.25
    done
    printf '%d\n' "$dropped" >&"${_CRATEDIGGER_RESOURCE_LOOP_REPORT_FD}"
}

daily_resource_monitor_start() {
    local hierarchy controllers relative="" file filesystem
    local -a state_root_candidates=() rejected_candidates=()
    local candidate candidate_filesystem_id scratch_filesystem_id state_root=""
    local reached_scratch_collision=0

    _CRATEDIGGER_RESOURCE_STARTED=starting
    _CRATEDIGGER_RESOURCE_TERMINAL_EMITTED=0
    _CRATEDIGGER_RESOURCE_PARENT_DROPPED=0
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
    _CRATEDIGGER_RESOURCE_PHASE_HISTORY="$_CRATEDIGGER_RESOURCE_DIR/phase-history"
    _CRATEDIGGER_RESOURCE_STOP="$_CRATEDIGGER_RESOURCE_DIR/stop"
    _CRATEDIGGER_RESOURCE_FAILURE="$_CRATEDIGGER_RESOURCE_DIR/failure"
    _CRATEDIGGER_RESOURCE_LOCK="$_CRATEDIGGER_RESOURCE_DIR/sample.lock"
    _CRATEDIGGER_RESOURCE_LOOP_REPORT="$_CRATEDIGGER_RESOURCE_DIR/loop-report"
    : > "$_CRATEDIGGER_RESOURCE_SAMPLES"
    : > "$_CRATEDIGGER_RESOURCE_PHASE_HISTORY"
    : > "$_CRATEDIGGER_RESOURCE_FAILURE"
    : > "$_CRATEDIGGER_RESOURCE_LOCK"
    if ! mkfifo -m 600 "$_CRATEDIGGER_RESOURCE_LOOP_REPORT" 2>/dev/null; then
        rm -rf -- "$_CRATEDIGGER_RESOURCE_DIR"
        _daily_resource_invalid state_store_bootstrap_failed
        return
    fi
    # Opened read-write (not read-only) so this open call itself does not
    # block waiting for the loop's own writer end to connect; the SAME fd
    # then serves as the parent's read end once the loop exits. This is
    # the only place the report channel ever touches a filesystem path --
    # every later transfer through it is a pure kernel pipe (review C1).
    if ! exec {_CRATEDIGGER_RESOURCE_LOOP_REPORT_FD}<> \
        "$_CRATEDIGGER_RESOURCE_LOOP_REPORT"; then
        rm -rf -- "$_CRATEDIGGER_RESOURCE_DIR"
        _daily_resource_invalid state_store_bootstrap_failed
        return
    fi
    _CRATEDIGGER_RESOURCE_CURRENT_PHASE=bootstrap
    _CRATEDIGGER_RESOURCE_PARENT_SEQ=1
    if ! _daily_resource_write_phase "$_CRATEDIGGER_RESOURCE_CURRENT_PHASE" \
        || ! _daily_resource_record_sample_unlocked \
            "$_CRATEDIGGER_RESOURCE_CURRENT_PHASE" parent \
            "$_CRATEDIGGER_RESOURCE_PARENT_SEQ";
    then
        # { ...; } wraps the redirection -- a BARE `exec {fd}<&- 2>/dev/null`
        # (no command after exec) applies 2>/dev/null to the CURRENT SHELL
        # permanently, silently killing every later `>&2` write for the
        # rest of the process's life (caught live: it silenced
        # daily_flake_update.sh's own gap-4/C5 stderr diagnostic). Do not
        # "simplify" this back to the bare form.
        { exec {_CRATEDIGGER_RESOURCE_LOOP_REPORT_FD}<&-; } 2>/dev/null || true
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
        printf '%s\n' phase_invalid > "$_CRATEDIGGER_RESOURCE_FAILURE"
        return 1
    fi
    if ! _daily_resource_lock; then
        printf '%s\n' phase_lock_failed > "$_CRATEDIGGER_RESOURCE_FAILURE"
        return 1
    fi
    # A boundary sample is a droppable single sample, same as the periodic
    # loop's: its loss is counted directly (issue #1214 review C1), never
    # a reason to abandon the phase transition itself -- that would leave
    # the shared phase pointer stuck on the old phase and mis-attribute
    # every later sample.
    _CRATEDIGGER_RESOURCE_PARENT_SEQ=$((_CRATEDIGGER_RESOURCE_PARENT_SEQ + 1))
    _daily_resource_record_sample_unlocked "$_CRATEDIGGER_RESOURCE_CURRENT_PHASE" \
        parent "$_CRATEDIGGER_RESOURCE_PARENT_SEQ" \
        || _CRATEDIGGER_RESOURCE_PARENT_DROPPED=$((_CRATEDIGGER_RESOURCE_PARENT_DROPPED + 1))
    if ! _daily_resource_write_phase "$phase"; then
        _daily_resource_unlock
        printf '%s\n' phase_state_write_failed > "$_CRATEDIGGER_RESOURCE_FAILURE"
        return 1
    fi
    _CRATEDIGGER_RESOURCE_CURRENT_PHASE="$phase"
    _CRATEDIGGER_RESOURCE_PARENT_SEQ=$((_CRATEDIGGER_RESOURCE_PARENT_SEQ + 1))
    _daily_resource_record_sample_unlocked "$_CRATEDIGGER_RESOURCE_CURRENT_PHASE" \
        parent "$_CRATEDIGGER_RESOURCE_PARENT_SEQ" \
        || _CRATEDIGGER_RESOURCE_PARENT_DROPPED=$((_CRATEDIGGER_RESOURCE_PARENT_DROPPED + 1))
    if ! _daily_resource_unlock; then
        printf '%s\n' phase_unlock_failed > "$_CRATEDIGGER_RESOURCE_FAILURE"
        return 1
    fi
}

daily_resource_monitor_finish() {
    local monitor_status=0 summary_status=0 reason="" loop_dropped=0
    if [[ "${_CRATEDIGGER_RESOURCE_STARTED:-0}" != 1 ]]; then
        if [[ -n "${_CRATEDIGGER_RESOURCE_PID:-}" ]]; then
            [[ -n "${_CRATEDIGGER_RESOURCE_STOP:-}" ]] \
                && : > "$_CRATEDIGGER_RESOURCE_STOP"
            wait "$_CRATEDIGGER_RESOURCE_PID" 2>/dev/null || true
        fi
        if [[ -n "${_CRATEDIGGER_RESOURCE_LOOP_REPORT_FD:-}" ]]; then
            # Grouped, not bare -- see the comment at the first occurrence
            # of this pattern in daily_resource_monitor_start.
            { exec {_CRATEDIGGER_RESOURCE_LOOP_REPORT_FD}<&-; } 2>/dev/null || true
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
    _CRATEDIGGER_RESOURCE_PARENT_SEQ=$((_CRATEDIGGER_RESOURCE_PARENT_SEQ + 1))
    _daily_resource_record_sample_locked \
        "$_CRATEDIGGER_RESOURCE_CURRENT_PHASE" parent \
        "$_CRATEDIGGER_RESOURCE_PARENT_SEQ" \
        || _CRATEDIGGER_RESOURCE_PARENT_DROPPED=$((_CRATEDIGGER_RESOURCE_PARENT_DROPPED + 1))
    : > "$_CRATEDIGGER_RESOURCE_STOP"
    if ! wait "$_CRATEDIGGER_RESOURCE_PID"; then
        monitor_status=1
    fi
    if ((monitor_status == 0)); then
        if ! IFS= read -r -t 5 -u "$_CRATEDIGGER_RESOURCE_LOOP_REPORT_FD" loop_dropped \
            || [[ ! "$loop_dropped" =~ ^[0-9]+$ ]]
        then
            printf '%s\n' loop_report_unavailable > "$_CRATEDIGGER_RESOURCE_FAILURE"
        fi
    fi
    # Grouped, not bare -- see the comment at the first occurrence of this
    # pattern in daily_resource_monitor_start.
    { exec {_CRATEDIGGER_RESOURCE_LOOP_REPORT_FD}<&-; } 2>/dev/null || true
    if [[ -s "$_CRATEDIGGER_RESOURCE_FAILURE" ]]; then
        reason="$(<"$_CRATEDIGGER_RESOURCE_FAILURE")"
        _daily_resource_invalid "$reason"
        summary_status=1
    elif ((monitor_status != 0)); then
        _daily_resource_invalid monitor_process_died
        summary_status=1
    elif ! daily_resource_summarize_samples \
        "$_CRATEDIGGER_RESOURCE_SAMPLES" "$_CRATEDIGGER_RESOURCE_PHASE_HISTORY" \
        "$_CRATEDIGGER_RESOURCE_PARENT_DROPPED" "$loop_dropped";
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
            if [[ "$#" -ne 5 ]]; then
                _daily_resource_invalid summarize_arguments_invalid
                exit 1
            fi
            daily_resource_summarize_samples "$2" "$3" "$4" "$5"
            ;;
        *)
            _daily_resource_invalid command_invalid
            exit 1
            ;;
    esac
fi
