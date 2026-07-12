#!/usr/bin/env bash
# Create or recover the exact signed nixosconfig pin for one Cratedigger SHA.
set -euo pipefail

readonly RECEIPT_REF='refs/cratedigger-deploy/cratedigger-src'
readonly EXPECTED_ORIGIN='https://git.ablz.au/abl030/nixosconfig.git'
readonly TOKEN_FILE="${NIXOSCONFIG_TOKEN_FILE:-/run/secrets/forgejo/nixbot-token}"
readonly NIXOSCONFIG_REPO="${HOME}/nixosconfig"

TEMP_ROOT=''
WORKTREE=''
WORKTREE_STARTED=0
INTENDED_REV=''
INTENDED_TARGET=''
INTENDED_BASE=''

die() {
  printf 'pin-nixosconfig: ERROR: %s\n' "$*" >&2
  return 1
}

cleanup_worktree() {
  local cleanup_rc=0

  if ((WORKTREE_STARTED)); then
    if ! git -C "$NIXOSCONFIG_REPO" worktree remove --force "$WORKTREE" \
      >/dev/null 2>&1; then
      cleanup_rc=1
      rm -rf "$WORKTREE" || cleanup_rc=1
    fi
  fi
  if [[ -n "$TEMP_ROOT" ]]; then
    rm -rf "$TEMP_ROOT" || cleanup_rc=1
  fi
  return "$cleanup_rc"
}

cleanup_on_exit() {
  local exit_rc=$?
  local cleanup_rc=0

  trap - EXIT
  cleanup_worktree || cleanup_rc=$?
  if ((exit_rc != 0)) && [[ -n "$INTENDED_REV" ]]; then
    printf 'pin-nixosconfig: recover with target=%s pending=%s base=%s ref=%s\n' \
      "$INTENDED_TARGET" "$INTENDED_REV" "$INTENDED_BASE" "$RECEIPT_REF" >&2
  fi
  if ((cleanup_rc != 0)); then
    if [[ -n "$INTENDED_REV" ]]; then
      printf 'pin-nixosconfig: cleanup failed; recoverable intended revision=%s ref=%s\n' \
        "$INTENDED_REV" "$RECEIPT_REF" >&2
    else
      printf 'pin-nixosconfig: cleanup failed before a recoverable revision existed\n' \
        >&2
    fi
    if ((exit_rc == 0)); then
      exit_rc=$cleanup_rc
    fi
  fi
  exit "$exit_rc"
}

trap cleanup_on_exit EXIT
trap 'exit 129' HUP
trap 'exit 130' INT
trap 'exit 143' TERM

locked_cratedigger_revision() {
  jq -er '.nodes["cratedigger-src"].locked.rev
    | select(type == "string" and test("^[0-9a-f]{40}$"))'
}

commit_locked_revision() {
  local revision=$1
  git -C "$NIXOSCONFIG_REPO" show "${revision}:flake.lock" \
    | locked_cratedigger_revision
}

verify_ssh_signature() {
  local revision=$1
  local signature_status commit_object

  signature_status=$(git -C "$NIXOSCONFIG_REPO" \
    log -1 '--format=%G?' "$revision")
  [[ "$signature_status" == 'G' ]] \
    || die "revision $revision does not have a good signature (status=$signature_status)"
  commit_object=$(git -C "$NIXOSCONFIG_REPO" cat-file commit "$revision")
  grep -q '^gpgsig -----BEGIN SSH SIGNATURE-----$' <<<"$commit_object" \
    || die "revision $revision is not SSH-signed"
}

VERIFIED_PARENT=''
VERIFIED_TARGET=''
verify_pin_commit() {
  local revision=$1
  local ancestry commit parent extra changed_paths

  verify_ssh_signature "$revision"
  ancestry=$(git -C "$NIXOSCONFIG_REPO" rev-list --parents -n1 "$revision")
  read -r commit parent extra <<<"$ancestry"
  [[ "$commit" == "$revision" && -n "$parent" && -z "${extra:-}" ]] \
    || die "pin revision $revision must have exactly one parent"
  changed_paths=$(git -C "$NIXOSCONFIG_REPO" \
    diff-tree --no-commit-id --name-only -r "$revision")
  [[ "$changed_paths" == 'flake.lock' ]] \
    || die "pin revision $revision changes paths other than flake.lock: $changed_paths"
  VERIFIED_TARGET=$(commit_locked_revision "$revision")
  VERIFIED_PARENT=$parent
}

forgejo_with_token() (
  set -euo pipefail
  local operation=$1
  local revision=${2:-}
  local token output sha ref extra
  local -a lines

  [[ -r "$TOKEN_FILE" ]] || die "Forgejo token is not readable: $TOKEN_FILE"
  IFS= read -r token < "$TOKEN_FILE"
  [[ -n "$token" ]] || die "Forgejo token is empty: $TOKEN_FILE"
  export GIT_CONFIG_COUNT=1
  export GIT_CONFIG_KEY_0='http.https://git.ablz.au.extraHeader'
  export GIT_CONFIG_VALUE_0="Authorization: token $token"
  unset token

  if [[ "$operation" == 'push' ]]; then
    [[ "$revision" =~ ^[0-9a-f]{40}$ ]] \
      || die "invalid revision passed to Forgejo push: $revision"
    if ! git -C "$NIXOSCONFIG_REPO" push origin \
      "${revision}:refs/heads/master"; then
      die "Forgejo push rejected for revision $revision"
      return 1
    fi
  elif [[ "$operation" != 'read' ]]; then
    die "invalid Forgejo operation: $operation"
  fi

  if ! output=$(git -C "$NIXOSCONFIG_REPO" \
    ls-remote origin refs/heads/master); then
    die 'Forgejo master lookup failed'
    return 1
  fi
  mapfile -t lines <<<"$output"
  ((${#lines[@]} == 1)) \
    || die "Forgejo master lookup returned ${#lines[@]} lines"
  read -r sha ref extra <<<"${lines[0]}"
  [[ "$sha" =~ ^[0-9a-f]{40}$ && "$ref" == 'refs/heads/master' \
    && -z "${extra:-}" ]] || die "invalid Forgejo master response"
  if [[ "$operation" == 'push' && "$sha" != "$revision" ]]; then
    die "Forgejo master mismatch after push: expected=$revision actual=$sha"
  fi
  printf '%s\n' "$sha"
)

remote_master_with_token() {
  forgejo_with_token read
}

push_and_verify_with_token() {
  forgejo_with_token push "$1" >/dev/null
}

main() {
  local target_revision commit_message remote_revision receipt_revision=''
  local remote_target status_output previous_receipt=''
  local candidate_revision git_common_dir origin_url

  (($# == 2)) \
    || die 'usage: pin_nixosconfig.sh <40-hex-cratedigger-revision> <commit-message>'
  target_revision=$1
  commit_message=$2
  [[ "$target_revision" =~ ^[0-9a-f]{40}$ ]] \
    || die "Cratedigger revision must be a full 40-hex SHA: $target_revision"
  [[ -n "$commit_message" && "$commit_message" != *$'\n'* ]] \
    || die 'commit message must be one nonempty line'
  [[ "$(hostname)" == 'proxmox-vm' ]] \
    || die 'nixosconfig pinning must run on doc1 (hostname=proxmox-vm)'
  [[ -d "$NIXOSCONFIG_REPO" ]] \
    || die "nixosconfig repository not found: $NIXOSCONFIG_REPO"
  command -v git >/dev/null
  command -v nix >/dev/null
  command -v jq >/dev/null
  command -v flock >/dev/null

  origin_url=$(git -C "$NIXOSCONFIG_REPO" remote get-url origin)
  [[ "$origin_url" == "$EXPECTED_ORIGIN" ]] \
    || die "nixosconfig origin must be $EXPECTED_ORIGIN (actual=$origin_url)"
  git_common_dir=$(git -C "$NIXOSCONFIG_REPO" \
    rev-parse --path-format=absolute --git-common-dir)
  [[ "$git_common_dir" == /* && -d "$git_common_dir" ]] \
    || die "invalid nixosconfig git common directory: $git_common_dir"
  exec 9>"$git_common_dir/cratedigger-deploy-pin.lock"
  flock 9

  git -C "$NIXOSCONFIG_REPO" fetch origin \
    '+refs/heads/master:refs/remotes/origin/master'
  remote_revision=$(git -C "$NIXOSCONFIG_REPO" \
    rev-parse refs/remotes/origin/master)
  [[ "$remote_revision" =~ ^[0-9a-f]{40}$ ]] \
    || die "invalid fetched origin/master revision: $remote_revision"

  if receipt_revision=$(git -C "$NIXOSCONFIG_REPO" \
    rev-parse --verify --quiet "$RECEIPT_REF"); then
    verify_pin_commit "$receipt_revision"
    INTENDED_REV=$receipt_revision
    INTENDED_TARGET=$VERIFIED_TARGET
    INTENDED_BASE=$VERIFIED_PARENT
    previous_receipt=$receipt_revision

    if [[ "$VERIFIED_TARGET" == "$target_revision" ]]; then
      printf 'recovering pending revision: %s target=%s base=%s\n' \
        "$receipt_revision" "$VERIFIED_TARGET" "$VERIFIED_PARENT"
      remote_revision=$(remote_master_with_token)
      if [[ "$remote_revision" == "$receipt_revision" ]]; then
        printf 'remote already at pending revision: %s\n' "$receipt_revision"
        printf 'signed nixosconfig revision: %s\n' "$receipt_revision"
        return 0
      fi
      if git -C "$NIXOSCONFIG_REPO" merge-base --is-ancestor \
        "$receipt_revision" "$remote_revision"; then
        remote_target=$(commit_locked_revision "$remote_revision")
        [[ "$remote_target" == "$target_revision" ]] \
          || die "Forgejo advanced past pending=$receipt_revision but no longer pins target=$target_revision (remote=$remote_revision remote_target=$remote_target)"
        verify_ssh_signature "$remote_revision"
        printf 'remote contains pending revision: %s current=%s\n' \
          "$receipt_revision" "$remote_revision"
        printf 'signed nixosconfig revision: %s\n' "$remote_revision"
        return 0
      fi
      if [[ "$remote_revision" != "$VERIFIED_PARENT" ]]; then
        die "incompatible remote advancement: pending=$receipt_revision base=$VERIFIED_PARENT remote=$remote_revision"
      fi
      push_and_verify_with_token "$receipt_revision"
      printf 'signed nixosconfig revision: %s\n' "$receipt_revision"
      return 0
    fi

    remote_revision=$(remote_master_with_token)
    if [[ "$remote_revision" != "$receipt_revision" ]] \
      && ! git -C "$NIXOSCONFIG_REPO" merge-base --is-ancestor \
        "$receipt_revision" "$remote_revision"; then
      die "different pin is still pending: requested=$target_revision pending_target=$VERIFIED_TARGET pending=$receipt_revision base=$VERIFIED_PARENT remote=$remote_revision"
    fi
  fi

  remote_target=$(commit_locked_revision "$remote_revision")
  if [[ "$remote_target" == "$target_revision" ]]; then
    verify_ssh_signature "$remote_revision"
    [[ "$(remote_master_with_token)" == "$remote_revision" ]] \
      || die "Forgejo master changed while confirming existing pin: fetched=$remote_revision"
    printf 'Forgejo master %s already pins %s; no commit needed\n' \
      "$remote_revision" "$target_revision"
    printf 'signed nixosconfig revision: %s\n' "$remote_revision"
    return 0
  fi

  TEMP_ROOT=$(mktemp -d "${TMPDIR:-/tmp}/nixosconfig-deploy.XXXXXX")
  WORKTREE="$TEMP_ROOT/worktree"
  WORKTREE_STARTED=1
  git -C "$NIXOSCONFIG_REPO" worktree add --detach \
    "$WORKTREE" "$remote_revision"

  (
    cd "$WORKTREE"
    nix flake update cratedigger-src
  )
  status_output=$(git -C "$WORKTREE" status --porcelain)
  [[ "$status_output" == ' M flake.lock' ]] \
    || die "nix update must change only tracked flake.lock (status=$status_output)"
  [[ "$(locked_cratedigger_revision < "$WORKTREE/flake.lock")" \
    == "$target_revision" ]] \
    || die "updated flake.lock does not pin requested Cratedigger revision $target_revision"

  git -C "$WORKTREE" add flake.lock
  SSH_AUTH_SOCK='' git -C "$WORKTREE" commit -m "$commit_message"
  candidate_revision=$(git -C "$WORKTREE" rev-parse HEAD)
  verify_pin_commit "$candidate_revision"
  [[ "$VERIFIED_TARGET" == "$target_revision" \
    && "$VERIFIED_PARENT" == "$remote_revision" ]] \
    || die "new pin commit has unexpected state: revision=$candidate_revision target=$VERIFIED_TARGET base=$VERIFIED_PARENT"

  git -C "$NIXOSCONFIG_REPO" update-ref "$RECEIPT_REF" \
    "$candidate_revision" "$previous_receipt"
  INTENDED_REV=$candidate_revision
  INTENDED_TARGET=$VERIFIED_TARGET
  INTENDED_BASE=$VERIFIED_PARENT
  push_and_verify_with_token "$INTENDED_REV"
  printf 'signed nixosconfig revision: %s\n' "$INTENDED_REV"
}

main "$@"
