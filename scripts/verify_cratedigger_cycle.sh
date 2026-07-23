#!/usr/bin/env bash
# Capture and verify one exact timer-driven Cratedigger invocation.
set -euo pipefail

readonly REMOTE_HOST="${CRATEDIGGER_DEPLOY_HOST:-doc2}"
readonly UNIT='cratedigger.service'
readonly -a OPERATOR_SSH=(ssh -o IdentityAgent=none)
readonly POLL_SECONDS="${CRATEDIGGER_CYCLE_VERIFY_POLL_SECONDS:-5}"
readonly TIMEOUT_SECONDS="${CRATEDIGGER_CYCLE_VERIFY_TIMEOUT_SECONDS:-1800}"
readonly MAX_POLLS="${CRATEDIGGER_CYCLE_VERIFY_MAX_POLLS:-0}"
readonly TERMINAL_GRACE_POLLS="${CRATEDIGGER_CYCLE_VERIFY_TERMINAL_GRACE_POLLS:-3}"

CURRENT_INVOCATION=''
CURRENT_ACTIVE=''
CURRENT_SUB=''
CURRENT_RESULT=''

die() {
  printf 'verify-cratedigger-cycle: ERROR: %s\n' "$*" >&2
  return 1
}

usage() {
  cat >&2 <<'EOF'
usage:
  verify_cratedigger_cycle.sh capture-current
  verify_cratedigger_cycle.sh capture-cursor
  verify_cratedigger_cycle.sh capture-target <baseline-journal-cursor> <expected-source-store>
  verify_cratedigger_cycle.sh verify-exact <target-id> <expected-source-store>
  verify_cratedigger_cycle.sh wait <baseline-journal-cursor> <expected-source-store>
EOF
  return 64
}

validate_settings() {
  [[ "$POLL_SECONDS" =~ ^[0-9]+$ ]] \
    || die "poll seconds must be a non-negative integer: $POLL_SECONDS"
  [[ "$TIMEOUT_SECONDS" =~ ^[1-9][0-9]*$ ]] \
    || die "timeout seconds must be a positive integer: $TIMEOUT_SECONDS"
  [[ "$MAX_POLLS" =~ ^[0-9]+$ ]] \
    || die "max polls must be a non-negative integer: $MAX_POLLS"
  [[ "$TERMINAL_GRACE_POLLS" =~ ^[1-9][0-9]*$ ]] \
    || die "terminal grace polls must be a positive integer: $TERMINAL_GRACE_POLLS"
}

validate_invocation() {
  local invocation=$1
  [[ "$invocation" =~ ^[0-9a-f]{32}$ ]] \
    || die "invalid systemd InvocationID: $invocation"
}

validate_source() {
  local source=$1
  [[ "$source" =~ ^/nix/store/[0-9a-z]{32}-source$ ]] \
    || die "invalid expected source store: $source"
}

validate_cursor() {
  local cursor=$1
  [[ -n "$cursor" && "$cursor" =~ ^[A-Za-z0-9_.:=\;-]+$ ]] \
    || die "invalid systemd journal cursor: $cursor"
}

read_current_state() {
  local state
  if ! state=$("${OPERATOR_SSH[@]}" "$REMOTE_HOST" \
    'systemctl show cratedigger.service --property=InvocationID --property=ActiveState --property=SubState --property=Result'); then
    die "could not read $REMOTE_HOST $UNIT state"
    return 1
  fi
  CURRENT_INVOCATION=$(sed -n 's/^InvocationID=//p' <<<"$state")
  CURRENT_ACTIVE=$(sed -n 's/^ActiveState=//p' <<<"$state")
  CURRENT_SUB=$(sed -n 's/^SubState=//p' <<<"$state")
  CURRENT_RESULT=$(sed -n 's/^Result=//p' <<<"$state")
  if [[ -n "$CURRENT_INVOCATION" ]]; then
    validate_invocation "$CURRENT_INVOCATION"
  fi
  [[ -n "$CURRENT_ACTIVE" && -n "$CURRENT_SUB" ]] \
    || die "incomplete $UNIT state: $state"
}

read_invocation_journal() {
  local invocation=$1
  validate_invocation "$invocation"
  "${OPERATOR_SSH[@]}" "$REMOTE_HOST" \
    "sudo journalctl -u $UNIT --invocation=$invocation --no-pager -o json"
}

read_start_journal_after_cursor() {
  local cursor=$1
  validate_cursor "$cursor"
  "${OPERATOR_SSH[@]}" "$REMOTE_HOST" \
    "sudo journalctl -u $UNIT --after-cursor='$cursor' --no-pager -o json"
}

journal_has_source() {
  local journal=$1 invocation=$2 expected_source=$3
  jq -e --arg invocation "$invocation" \
    --arg script "$expected_source/cratedigger.py" '
      select(._SYSTEMD_INVOCATION_ID == $invocation)
      | select((._CMDLINE? | type) == "string")
      | select((._CMDLINE | split(" ") | index($script)) != null)
    ' >/dev/null <<<"$journal"
}

journal_source_stores() {
  local journal=$1 invocation=$2
  jq -r --arg invocation "$invocation" '
      select(._SYSTEMD_INVOCATION_ID == $invocation)
      | select((._CMDLINE? | type) == "string")
      | ._CMDLINE
      | split(" ")[]
      | select(test("^/nix/store/[0-9a-z]{32}-source/cratedigger[.]py$"))
      | sub("/cratedigger[.]py$"; "")
    ' <<<"$journal" | sort -u
}

journal_has_cycle_complete() {
  local journal=$1 invocation=$2
  jq -e --arg invocation "$invocation" '
      select(._SYSTEMD_INVOCATION_ID == $invocation)
      | select((.MESSAGE? // "") | contains("Cratedigger cycle complete"))
    ' >/dev/null <<<"$journal"
}

journal_has_deactivated_success() {
  local journal=$1 invocation=$2
  jq -e --arg invocation "$invocation" '
      select(.INVOCATION_ID == $invocation)
      | select(.MESSAGE == "cratedigger.service: Deactivated successfully.")
    ' >/dev/null <<<"$journal"
}

journal_has_finished_success() {
  local journal=$1 invocation=$2
  jq -e --arg invocation "$invocation" '
      select(.INVOCATION_ID == $invocation)
      | select(.MESSAGE == "Finished Cratedigger — Soulseek download pipeline.")
      | select(.JOB_TYPE == "start" and .JOB_RESULT == "done")
    ' >/dev/null <<<"$journal"
}

journal_has_failure() {
  local journal=$1 invocation=$2
  jq -e --arg invocation "$invocation" '
      select(.INVOCATION_ID == $invocation)
      | select(
          ((.JOB_RESULT? // "done") != "done")
          or ((.MESSAGE? // "") | test("Failed|failed with result"))
        )
    ' >/dev/null <<<"$journal"
}

missing_evidence() {
  local journal=$1 invocation=$2 expected_source=$3
  local -a missing=()
  journal_has_source "$journal" "$invocation" "$expected_source" \
    || missing+=(source)
  journal_has_cycle_complete "$journal" "$invocation" \
    || missing+=(cycle-complete)
  journal_has_deactivated_success "$journal" "$invocation" \
    || missing+=(deactivated-success)
  journal_has_finished_success "$journal" "$invocation" \
    || missing+=(finished-success)
  local rendered
  rendered=$(IFS=,; printf '%s' "${missing[*]}")
  printf '%s\n' "$rendered"
}

poll_limit_reached() {
  local polls=$1 deadline=$2
  if ((MAX_POLLS > 0 && polls >= MAX_POLLS)); then
    return 0
  fi
  ((SECONDS >= deadline))
}

capture_current() {
  read_current_state
  if [[ -n "$CURRENT_INVOCATION" ]]; then
    printf '%s\n' "$CURRENT_INVOCATION"
  else
    printf 'none\n'
  fi
}

capture_cursor() {
  local output cursor
  if ! output=$("${OPERATOR_SSH[@]}" "$REMOTE_HOST" \
    "sudo journalctl -u $UNIT -n 0 --show-cursor --no-pager"); then
    die "could not capture $REMOTE_HOST $UNIT journal cursor"
    return 1
  fi
  cursor=$(sed -n 's/^-- cursor: //p' <<<"$output")
  validate_cursor "$cursor"
  printf '%s\n' "$cursor"
}

capture_target() {
  local baseline_cursor=$1 expected_source=$2
  local deadline polls=0 candidate journal starts identified_source
  local -a candidates=()
  local -a identified_sources=()
  local -A seen=()
  validate_cursor "$baseline_cursor"
  validate_source "$expected_source"
  deadline=$((SECONDS + TIMEOUT_SECONDS))

  while true; do
    polls=$((polls + 1))
    if ! starts=$(read_start_journal_after_cursor "$baseline_cursor"); then
      die "could not read $UNIT starts after the baseline cursor"
      return 1
    fi
    while IFS= read -r candidate; do
      [[ -n "$candidate" ]] || continue
      validate_invocation "$candidate"
      if [[ -z "${seen[$candidate]:-}" ]]; then
        candidates+=("$candidate")
        seen[$candidate]=pending
      fi
    done < <(jq -r '
      select(.JOB_TYPE == "start" and .JOB_RESULT == null)
      | .INVOCATION_ID // empty
    ' <<<"$starts")

    for candidate in "${candidates[@]}"; do
      if ! journal=$(read_invocation_journal "$candidate"); then
        die "could not read journal for candidate invocation $candidate"
        return 1
      fi
      mapfile -t identified_sources < <(
        journal_source_stores "$journal" "$candidate"
      )
      if ((${#identified_sources[@]} > 1)); then
        die "candidate invocation $candidate names multiple source stores"
        return 1
      fi
      if ((${#identified_sources[@]} == 1)); then
        identified_source=${identified_sources[0]}
        if [[ "$identified_source" == "$expected_source" ]]; then
          printf '%s\n' "$candidate"
          return 0
        fi
        if [[ "${seen[$candidate]}" != reported ]]; then
          printf 'verify-cratedigger-cycle: ignoring invocation %s from source %s\n' \
            "$candidate" "$identified_source" >&2
          seen[$candidate]=reported
        fi
        continue
      fi

      # An invocation with no process record is unknown, not proof of a
      # different source. Once it terminates, return it so verify-exact fails
      # closed on its failure or incomplete evidence. While it is live, do not
      # skip ahead to any later start record.
      if journal_has_failure "$journal" "$candidate" \
        || journal_has_finished_success "$journal" "$candidate" \
        || journal_has_deactivated_success "$journal" "$candidate"; then
        printf '%s\n' "$candidate"
        return 0
      fi
      break
    done

    if poll_limit_reached "$polls" "$deadline"; then
      die "timed out waiting for an invocation from source $expected_source"
      return 1
    fi
    sleep "$POLL_SECONDS"
  done
}

verify_exact() {
  local target=$1 expected_source=$2
  local deadline polls=0 journal missing rolled_over=0 terminal_grace_polls=0
  validate_invocation "$target"
  validate_source "$expected_source"
  deadline=$((SECONDS + TIMEOUT_SECONDS))

  while true; do
    polls=$((polls + 1))
    if ! journal=$(read_invocation_journal "$target"); then
      die "could not read journal for target invocation $target"
      return 1
    fi
    read_current_state
    if [[ -n "$CURRENT_INVOCATION" && "$CURRENT_INVOCATION" != "$target" ]]; then
      rolled_over=1
    fi

    if journal_has_failure "$journal" "$target"; then
      die "target invocation $target failed"
      return 1
    fi
    missing=$(missing_evidence "$journal" "$target" "$expected_source")
    if [[ -z "$missing" ]]; then
      if ((rolled_over)); then
        printf 'verified invocation %s from %s after current unit rolled over\n' \
          "$target" "$expected_source"
      else
        printf 'verified invocation %s from %s\n' "$target" "$expected_source"
      fi
      return 0
    fi

    if [[ "$CURRENT_INVOCATION" == "$target" \
      && ("$CURRENT_ACTIVE" == failed \
        || (-n "$CURRENT_RESULT" && "$CURRENT_RESULT" != success)) ]]; then
      die "target invocation $target failed (state=$CURRENT_ACTIVE/$CURRENT_SUB result=$CURRENT_RESULT; missing=$missing)"
      return 1
    fi

    if ((rolled_over)) \
      || [[ "$CURRENT_ACTIVE" == inactive || "$CURRENT_ACTIVE" == failed ]]; then
      terminal_grace_polls=$((terminal_grace_polls + 1))
      if ((terminal_grace_polls >= TERMINAL_GRACE_POLLS)); then
        die "target invocation $target completed with incomplete evidence: $missing"
        return 1
      fi
    fi

    if poll_limit_reached "$polls" "$deadline"; then
      die "timed out verifying target invocation $target (missing=$missing)"
      return 1
    fi
    sleep "$POLL_SECONDS"
  done
}

wait_for_cycle() {
  local baseline_cursor=$1 expected_source=$2 target
  target=$(capture_target "$baseline_cursor" "$expected_source")
  verify_exact "$target" "$expected_source"
}

main() {
  validate_settings
  local command=${1:-}
  case "$command" in
    capture-current)
      (($# == 1)) || usage
      capture_current
      ;;
    capture-cursor)
      (($# == 1)) || usage
      capture_cursor
      ;;
    capture-target)
      (($# == 3)) || usage
      capture_target "$2" "$3"
      ;;
    verify-exact)
      (($# == 3)) || usage
      verify_exact "$2" "$3"
      ;;
    wait)
      (($# == 3)) || usage
      wait_for_cycle "$2" "$3"
      ;;
    *)
      usage
      ;;
  esac
}

main "$@"
