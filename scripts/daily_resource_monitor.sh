#!/usr/bin/env bash
# Phase-correlated cgroup and private-scratch receipts for the daily gate.

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

    if [[ ! -r "$samples_file" ]]; then
        _daily_resource_invalid samples_unreadable
        return
    fi

    while IFS=$'\t' read -r \
        timestamp_ns phase memory_current memory_peak swap_current swap_peak \
        anon file_bytes shmem kernel scratch_bytes scratch_byte_limit \
        scratch_inodes scratch_inode_limit extra
    do
        if [[ -n "${extra:-}" || ! "$phase" =~ ^[a-z][a-z0-9_]*$ ]]; then
            _daily_resource_invalid sample_shape_invalid
            return
        fi
        for field in \
            timestamp_ns memory_current memory_peak swap_current swap_peak \
            anon file_bytes shmem kernel scratch_bytes scratch_byte_limit \
            scratch_inodes scratch_inode_limit
        do
            value="${!field}"
            if [[ ! "$value" =~ ^[0-9]+$ ]]; then
                _daily_resource_invalid sample_value_invalid
                return
            fi
        done
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
    # samples and the periodic child may overlap, so terminal aggregation sorts
    # by this timestamp instead of trusting concurrent append order.
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
        >> "$_CRATEDIGGER_RESOURCE_SAMPLES"
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
    while [[ ! -e "$_CRATEDIGGER_RESOURCE_STOP" ]]; do
        if ! _daily_resource_record_current_phase_locked; then
            printf '%s\n' sample_failed > "$_CRATEDIGGER_RESOURCE_FAILURE"
            return 1
        fi
        sleep 0.25
    done
}

daily_resource_monitor_start() {
    local hierarchy controllers relative="" file filesystem

    _CRATEDIGGER_RESOURCE_STARTED=starting
    _CRATEDIGGER_RESOURCE_TERMINAL_EMITTED=0
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

    _CRATEDIGGER_RESOURCE_DIR="$(
        mktemp -d \
            "$_CRATEDIGGER_RESOURCE_SCRATCH/cratedigger-daily-resource.XXXXXX"
    )" || {
        _daily_resource_invalid monitor_state_unavailable
        return
    }
    _CRATEDIGGER_RESOURCE_SAMPLES="$_CRATEDIGGER_RESOURCE_DIR/samples.tsv"
    _CRATEDIGGER_RESOURCE_PHASE="$_CRATEDIGGER_RESOURCE_DIR/phase"
    _CRATEDIGGER_RESOURCE_STOP="$_CRATEDIGGER_RESOURCE_DIR/stop"
    _CRATEDIGGER_RESOURCE_FAILURE="$_CRATEDIGGER_RESOURCE_DIR/failure"
    _CRATEDIGGER_RESOURCE_LOCK="$_CRATEDIGGER_RESOURCE_DIR/sample.lock"
    : > "$_CRATEDIGGER_RESOURCE_SAMPLES"
    : > "$_CRATEDIGGER_RESOURCE_FAILURE"
    : > "$_CRATEDIGGER_RESOURCE_LOCK"
    _CRATEDIGGER_RESOURCE_CURRENT_PHASE=bootstrap
    if ! _daily_resource_write_phase "$_CRATEDIGGER_RESOURCE_CURRENT_PHASE" \
        || ! _daily_resource_record_sample_unlocked \
            "$_CRATEDIGGER_RESOURCE_CURRENT_PHASE";
    then
        rm -rf -- "$_CRATEDIGGER_RESOURCE_DIR"
        _daily_resource_invalid initial_sample_failed
        return
    fi
    _daily_resource_monitor_loop &
    _CRATEDIGGER_RESOURCE_PID=$!
    _CRATEDIGGER_RESOURCE_STARTED=1
}

daily_resource_monitor_set_phase() {
    local phase="$1"
    local status=0
    if [[ "${_CRATEDIGGER_RESOURCE_STARTED:-0}" != 1 ]]; then
        return 1
    fi
    if [[ ! "$phase" =~ ^[a-z][a-z0-9_]*$ ]]; then
        printf '%s\n' phase_invalid > "$_CRATEDIGGER_RESOURCE_FAILURE"
        return 1
    fi
    if ! _daily_resource_lock; then
        printf '%s\n' phase_transition_failed > "$_CRATEDIGGER_RESOURCE_FAILURE"
        return 1
    fi
    _daily_resource_record_sample_unlocked \
        "$_CRATEDIGGER_RESOURCE_CURRENT_PHASE" || status=1
    if ((status == 0)); then
        _daily_resource_write_phase "$phase" || status=1
    fi
    if ((status == 0)); then
        _CRATEDIGGER_RESOURCE_CURRENT_PHASE="$phase"
        _daily_resource_record_sample_unlocked \
            "$_CRATEDIGGER_RESOURCE_CURRENT_PHASE" || status=1
    fi
    _daily_resource_unlock || status=1
    if ((status != 0)); then
        printf '%s\n' phase_transition_failed > "$_CRATEDIGGER_RESOURCE_FAILURE"
        return 1
    fi
}

daily_resource_monitor_finish() {
    local monitor_status=0 summary_status=0 reason=""
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
    if ! _daily_resource_record_sample_locked \
        "$_CRATEDIGGER_RESOURCE_CURRENT_PHASE";
    then
        printf '%s\n' final_sample_failed > "$_CRATEDIGGER_RESOURCE_FAILURE"
    fi
    : > "$_CRATEDIGGER_RESOURCE_STOP"
    if ! wait "$_CRATEDIGGER_RESOURCE_PID"; then
        monitor_status=1
    fi
    if [[ -s "$_CRATEDIGGER_RESOURCE_FAILURE" ]]; then
        reason="$(<"$_CRATEDIGGER_RESOURCE_FAILURE")"
        _daily_resource_invalid "$reason"
        summary_status=1
    elif ((monitor_status != 0)); then
        _daily_resource_invalid monitor_failed
        summary_status=1
    elif ! daily_resource_summarize_samples "$_CRATEDIGGER_RESOURCE_SAMPLES"; then
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
            if [[ "$#" -ne 2 ]]; then
                _daily_resource_invalid summarize_arguments_invalid
                exit 1
            fi
            daily_resource_summarize_samples "$2"
            ;;
        *)
            _daily_resource_invalid command_invalid
            exit 1
            ;;
    esac
fi
