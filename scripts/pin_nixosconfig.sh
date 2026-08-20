#!/usr/bin/env bash
# Create or recover the exact signed nixosconfig pin for one Cratedigger SHA.
set -euo pipefail

readonly RECEIPT_REF='refs/cratedigger-deploy/cratedigger-src'
readonly PENDING_REF='refs/cratedigger-deploy/cratedigger-src-pending'
readonly EXPECTED_ORIGIN='https://git.ablz.au/abl030/nixosconfig.git'
readonly TOKEN_FILE="${NIXOSCONFIG_TOKEN_FILE:-/run/secrets/forgejo/nixbot-token}"
readonly NIXOSCONFIG_REPO="${HOME}/nixosconfig"

TEMP_ROOT=''
WORKTREE=''
WORKTREE_STARTED=0
INTENDED_REV=''
INTENDED_TARGET=''
INTENDED_BASE=''
PENDING_BASE=''
VERIFIED_REMOTE_TARGET=''

die() {
  printf 'pin-nixosconfig: ERROR: %s\n' "$*" >&2
  return 1
}

cleanup_worktree() {
  local cleanup_rc=0
  local pending_revision=''

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
  if pending_revision=$(git -C "$NIXOSCONFIG_REPO" \
    show-ref --verify --hash "$PENDING_REF" 2>/dev/null); then
    if [[ -n "$PENDING_BASE" && "$pending_revision" == "$PENDING_BASE" ]]; then
      git -C "$NIXOSCONFIG_REPO" update-ref -d \
        "$PENDING_REF" "$pending_revision" || cleanup_rc=1
    else
      printf 'pin-nixosconfig: pending candidate retained: revision=%s ref=%s\n' \
        "$pending_revision" "$PENDING_REF" >&2
    fi
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

cratedigger_override_ref() {
  # Derive the exact overridable `github:<owner>/<repo>/<revision>` flake ref
  # from flake.lock's OWN cratedigger-src `original` node, rather than
  # hardcoding `github:abl030/cratedigger` a second time here -- a second
  # hardcoded copy would silently pin the wrong repository if the flake
  # input's origin ever changed without this script being updated to match.
  # Fails closed on any input shape this script does not understand,
  # INCLUDING keys it has never heard of. A `github` flake input can also
  # carry `host` (a private mirror), `dir`, or `ref`; silently dropping any
  # of those would still build a `github:owner/repo/<revision>` ref that
  # LOOKS valid but resolves at github.com regardless of what the original
  # node actually said -- precisely the "silently pin the wrong repository"
  # failure this function exists to prevent.
  #
  # This function is itself invoked via command substitution
  # (`ref=$(cratedigger_override_ref ...)`), which runs it in a subshell.
  # Bash does not honour `set -e` for failures INSIDE a command
  # substitution unless `shopt -s inherit_errexit` is set (it is not, here
  # or anywhere else in this script) -- so every internal failure below
  # must be checked explicitly with `if ! ...; then die ...; return 1; fi`,
  # never a bare `cmd || die ...`. The bare form silently falls through to
  # the next statement instead of stopping the function.
  local lock_file=$1
  local revision=$2
  local node_type owner repo extra_keys

  # This allowlist is deliberately over-strict: a legitimate cratedigger-src
  # input could add `ref` (for example, pinning the input to
  # `github:abl030/cratedigger/main` in flake.nix) without being wrong in
  # any way this script cares about -- an explicit 40-hex --override-input
  # always supersedes a branch ref regardless, so there is no correctness
  # gap to loosen for. Don't loosen it anyway: any change to the
  # cratedigger-src input's shape in flake.nix (a new key here, in
  # particular) requires updating this allowlist to match, or every
  # subsequent pin fails here -- known constraint, not a mystery deploy
  # failure.
  if ! extra_keys=$(jq -er '(.nodes["cratedigger-src"].original | keys)
    - ["type", "owner", "repo"] | join(",")' "$lock_file"); then
    die "cratedigger-src input's original node is missing or not an object in $lock_file"
    return 1
  fi
  if [[ -n "$extra_keys" ]]; then
    die "cratedigger-src input has unrecognised keys ($extra_keys) in $lock_file"
    return 1
  fi
  if ! node_type=$(jq -er '.nodes["cratedigger-src"].original.type
    | select(type == "string")' "$lock_file"); then
    die "cratedigger-src input has no readable original.type in $lock_file"
    return 1
  fi
  if [[ "$node_type" != 'github' ]]; then
    die "cratedigger-src input type must be github (actual=$node_type)"
    return 1
  fi
  if ! owner=$(jq -er '.nodes["cratedigger-src"].original.owner
    | select(type == "string" and test("^[A-Za-z0-9._-]+$"))' "$lock_file"); then
    die "cratedigger-src input owner is missing or malformed in $lock_file"
    return 1
  fi
  if ! repo=$(jq -er '.nodes["cratedigger-src"].original.repo
    | select(type == "string" and test("^[A-Za-z0-9._-]+$"))' "$lock_file"); then
    die "cratedigger-src input repo is missing or malformed in $lock_file"
    return 1
  fi
  printf 'github:%s/%s/%s' "$owner" "$repo" "$revision"
}

verify_ssh_signature() {
  local revision=$1
  local signature_status commit_object

  if ! signature_status=$(git -C "$NIXOSCONFIG_REPO" \
    log -1 '--format=%G?' "$revision"); then
    die "could not read signature status for revision $revision"
    return 1
  fi
  case "$signature_status" in
    G)
      ;;
    B|N)
      die "revision $revision has a definitively invalid or missing signature (status=$signature_status)"
      return 2
      ;;
    *)
      die "could not establish signature validity for revision $revision (status=$signature_status)"
      return 1
      ;;
  esac
  if ! commit_object=$(git -C "$NIXOSCONFIG_REPO" \
    cat-file commit "$revision"); then
    die "could not read commit object for revision $revision"
    return 1
  fi
  if ! grep -q '^gpgsig -----BEGIN SSH SIGNATURE-----$' \
    <<<"$commit_object"; then
    die "revision $revision is not SSH-signed"
    return 2
  fi
}

VERIFIED_PARENT=''
VERIFIED_TARGET=''
verify_pin_commit() {
  local revision=$1
  local ancestry commit parent extra changed_paths signature_rc

  if verify_ssh_signature "$revision"; then
    signature_rc=0
  else
    signature_rc=$?
    return "$signature_rc"
  fi
  if ! ancestry=$(git -C "$NIXOSCONFIG_REPO" \
    rev-list --parents -n1 "$revision"); then
    die "could not read ancestry for pin revision $revision"
    return 1
  fi
  read -r commit parent extra <<<"$ancestry"
  if [[ "$commit" != "$revision" || -z "$parent" || -n "${extra:-}" ]]; then
    die "pin revision $revision must have exactly one parent"
    return 2
  fi
  if ! changed_paths=$(git -C "$NIXOSCONFIG_REPO" \
    diff-tree --no-commit-id --name-only -r "$revision"); then
    die "could not read changed paths for pin revision $revision"
    return 1
  fi
  if [[ "$changed_paths" != 'flake.lock' ]]; then
    die "pin revision $revision changes paths other than flake.lock: $changed_paths"
    return 2
  fi
  if ! VERIFIED_TARGET=$(commit_locked_revision "$revision"); then
    die "could not read cratedigger-src lock from pin revision $revision"
    return 1
  fi
  VERIFIED_PARENT=$parent
}

verify_stable_remote_pin() {
  local remote_revision=$1 remote_target
  verify_ssh_signature "$remote_revision" || return $?
  if ! remote_target=$(commit_locked_revision "$remote_revision"); then
    die "could not read cratedigger-src lock from remote revision $remote_revision"
    return 1
  fi
  [[ "$(remote_master_with_token)" == "$remote_revision" ]] \
    || die "Forgejo master changed while confirming remote pin: fetched=$remote_revision"
  VERIFIED_REMOTE_TARGET=$remote_target
}

forgejo_with_token() (
  # A caller can export SHELLOPTS=xtrace, and Git's Trace2 can be configured to
  # print selected environment variables. Both must be disabled before the
  # secret is read or placed in GIT_CONFIG_VALUE_0.
  set +x
  set -euo pipefail
  local operation=$1
  local revision=${2:-}
  local token output sha ref extra
  local trace_variable
  local -a lines

  for trace_variable in ${!GIT_TRACE@}; do
    unset "$trace_variable"
  done
  unset GIT_CURL_VERBOSE

  [[ -r "$TOKEN_FILE" ]] || die "Forgejo token is not readable: $TOKEN_FILE"
  token=$(<"$TOKEN_FILE")
  [[ -n "$token" ]] || die "Forgejo token is empty: $TOKEN_FILE"
  export GIT_CONFIG_COUNT=1
  export GIT_CONFIG_KEY_0='http.https://git.ablz.au.extraHeader'
  export GIT_CONFIG_VALUE_0="Authorization: token $token"
  unset token

  if [[ "$operation" == 'push' ]]; then
    [[ "$revision" =~ ^[0-9a-f]{40}$ ]] \
      || die "invalid revision passed to Forgejo push: $revision"
    if ! git -C "$NIXOSCONFIG_REPO" push "$EXPECTED_ORIGIN" \
      "${revision}:refs/heads/master"; then
      die "Forgejo push rejected for revision $revision"
      return 1
    fi
  elif [[ "$operation" != 'read' ]]; then
    die "invalid Forgejo operation: $operation"
  fi

  if ! output=$(git -C "$NIXOSCONFIG_REPO" \
    ls-remote "$EXPECTED_ORIGIN" refs/heads/master); then
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
  local pending_revision='' pending_parent_target remote_target
  local receipt_parent_target status_output
  local previous_receipt=''
  local candidate_revision git_common_dir origin_url verification_rc
  local fetch_url_output push_url_output cratedigger_flake_ref
  local -a fetch_urls push_urls

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

  fetch_url_output=$(git -C "$NIXOSCONFIG_REPO" \
    remote get-url --all origin)
  push_url_output=$(git -C "$NIXOSCONFIG_REPO" \
    remote get-url --push --all origin)
  mapfile -t fetch_urls <<<"$fetch_url_output"
  mapfile -t push_urls <<<"$push_url_output"
  ((${#fetch_urls[@]} == 1)) \
    || die "nixosconfig origin must have exactly one fetch URL"
  ((${#push_urls[@]} == 1)) \
    || die "nixosconfig origin must have exactly one push URL"
  origin_url=${fetch_urls[0]}
  [[ "$origin_url" == "$EXPECTED_ORIGIN" ]] \
    || die "nixosconfig fetch URL must be $EXPECTED_ORIGIN (actual=$origin_url)"
  [[ "${push_urls[0]}" == "$EXPECTED_ORIGIN" ]] \
    || die "nixosconfig push URL must be $EXPECTED_ORIGIN (actual=${push_urls[0]})"
  git_common_dir=$(git -C "$NIXOSCONFIG_REPO" \
    rev-parse --path-format=absolute --git-common-dir)
  [[ "$git_common_dir" == /* && -d "$git_common_dir" ]] \
    || die "invalid nixosconfig git common directory: $git_common_dir"
  exec 9>"$git_common_dir/cratedigger-deploy-pin.lock"
  flock 9

  git -C "$NIXOSCONFIG_REPO" fetch "$EXPECTED_ORIGIN" \
    '+refs/heads/master:refs/remotes/origin/master'
  remote_revision=$(git -C "$NIXOSCONFIG_REPO" \
    rev-parse refs/remotes/origin/master)
  [[ "$remote_revision" =~ ^[0-9a-f]{40}$ ]] \
    || die "invalid fetched origin/master revision: $remote_revision"

  if ! receipt_revision=$(git -C "$NIXOSCONFIG_REPO" \
    rev-parse --verify --quiet "$RECEIPT_REF"); then
    receipt_revision=''
  fi

  if pending_revision=$(git -C "$NIXOSCONFIG_REPO" \
    rev-parse --verify --quiet "$PENDING_REF"); then
    printf 'recovering durable pending candidate: %s ref=%s\n' \
      "$pending_revision" "$PENDING_REF"
    if verify_pin_commit "$pending_revision"; then
      :
    else
      verification_rc=$?
      if ((verification_rc == 2)); then
        git -C "$NIXOSCONFIG_REPO" update-ref -d \
          "$PENDING_REF" "$pending_revision"
        printf 'pin-nixosconfig: definitively invalid pending candidate discarded: revision=%s ref=%s\n' \
          "$pending_revision" "$PENDING_REF" >&2
      fi
      return "$verification_rc"
    fi
    [[ "$VERIFIED_TARGET" == "$target_revision" ]] \
      || die "different candidate is pending: requested=$target_revision pending_target=$VERIFIED_TARGET pending=$pending_revision ref=$PENDING_REF"

    remote_revision=$(remote_master_with_token)
    if [[ "$remote_revision" == "$VERIFIED_PARENT" ]]; then
      [[ "$(remote_master_with_token)" == "$remote_revision" ]] \
        || die "Forgejo master changed while confirming pending base: fetched=$remote_revision"
    elif [[ "$remote_revision" == "$pending_revision" ]] \
      || git -C "$NIXOSCONFIG_REPO" merge-base --is-ancestor \
        "$pending_revision" "$remote_revision"; then
      verify_stable_remote_pin "$remote_revision"
      [[ "$VERIFIED_REMOTE_TARGET" == "$VERIFIED_TARGET" ]] \
        || die "Forgejo advanced past pending=$pending_revision but no longer pins target=$VERIFIED_TARGET (remote=$remote_revision remote_target=$VERIFIED_REMOTE_TARGET)"
    else
      verify_stable_remote_pin "$remote_revision"
      if [[ "$VERIFIED_REMOTE_TARGET" != "$VERIFIED_TARGET" ]]; then
        if ! pending_parent_target=$(commit_locked_revision "$VERIFIED_PARENT"); then
          die "could not read cratedigger-src lock from pending parent $VERIFIED_PARENT"
          return 1
        fi
        [[ "$VERIFIED_REMOTE_TARGET" == "$pending_parent_target" ]] \
          || die "different candidate is pending: requested=$target_revision pending_target=$VERIFIED_TARGET pending=$pending_revision ref=$PENDING_REF"
      fi
    fi

    previous_receipt=$receipt_revision
    if [[ "$receipt_revision" != "$pending_revision" ]]; then
      git -C "$NIXOSCONFIG_REPO" update-ref "$RECEIPT_REF" \
        "$pending_revision" "$previous_receipt"
    fi
    INTENDED_REV=$pending_revision
    INTENDED_TARGET=$VERIFIED_TARGET
    INTENDED_BASE=$VERIFIED_PARENT
    git -C "$NIXOSCONFIG_REPO" update-ref -d \
      "$PENDING_REF" "$pending_revision"
    receipt_revision=$pending_revision
  fi

  if [[ -n "$receipt_revision" ]]; then
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
        verify_stable_remote_pin "$remote_revision"
        [[ "$VERIFIED_REMOTE_TARGET" == "$target_revision" ]] \
          || die "Forgejo advanced past pending=$receipt_revision but no longer pins target=$target_revision (remote=$remote_revision remote_target=$VERIFIED_REMOTE_TARGET)"
        printf 'remote contains pending revision: %s current=%s\n' \
          "$receipt_revision" "$remote_revision"
        printf 'signed nixosconfig revision: %s\n' "$remote_revision"
        return 0
      fi
      if [[ "$remote_revision" == "$VERIFIED_PARENT" ]]; then
        push_and_verify_with_token "$receipt_revision"
        printf 'signed nixosconfig revision: %s\n' "$receipt_revision"
        return 0
      fi
      verify_stable_remote_pin "$remote_revision"
      if [[ "$VERIFIED_REMOTE_TARGET" == "$target_revision" ]]; then
        printf 'remote preserves pending target: %s current=%s\n' \
          "$receipt_revision" "$remote_revision"
        printf 'signed nixosconfig revision: %s\n' "$remote_revision"
        return 0
      fi
      if ! receipt_parent_target=$(commit_locked_revision "$VERIFIED_PARENT"); then
        die "could not read cratedigger-src lock from receipt parent $VERIFIED_PARENT"
        return 1
      fi
      if [[ "$VERIFIED_REMOTE_TARGET" != "$receipt_parent_target" ]]; then
        die "incompatible remote advancement: pending=$receipt_revision base=$VERIFIED_PARENT remote=$remote_revision"
      fi
      printf 'replacing rejected pending revision: %s base=%s remote=%s\n' \
        "$receipt_revision" "$VERIFIED_PARENT" "$remote_revision"
    else
      remote_revision=$(remote_master_with_token)
      if [[ "$remote_revision" != "$receipt_revision" ]] \
        && ! git -C "$NIXOSCONFIG_REPO" merge-base --is-ancestor \
          "$receipt_revision" "$remote_revision"; then
        verify_stable_remote_pin "$remote_revision"
        [[ "$VERIFIED_REMOTE_TARGET" == "$VERIFIED_TARGET" ]] \
          || die "different pin is still pending: requested=$target_revision pending_target=$VERIFIED_TARGET pending=$receipt_revision base=$VERIFIED_PARENT remote=$remote_revision -- this pending pin never reached Forgejo master; re-run with target=$VERIFIED_TARGET first to land it, then re-run with target=$target_revision"
      fi
    fi
  fi

  if [[ -n "$VERIFIED_REMOTE_TARGET" ]]; then
    remote_target=$VERIFIED_REMOTE_TARGET
  else
    remote_target=$(commit_locked_revision "$remote_revision")
  fi
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
  PENDING_BASE=$remote_revision
  git -C "$NIXOSCONFIG_REPO" update-ref "$PENDING_REF" \
    "$remote_revision" ''
  WORKTREE_STARTED=1
  git -C "$NIXOSCONFIG_REPO" worktree add --detach \
    "$WORKTREE" "$remote_revision"
  git -C "$WORKTREE" symbolic-ref HEAD "$PENDING_REF"

  cratedigger_flake_ref=$(cratedigger_override_ref \
    "$WORKTREE/flake.lock" "$target_revision")
  (
    cd "$WORKTREE"
    # Pin the exact requested revision instead of following
    # github:abl030/cratedigger's branch tip: the input carries no fixed ref,
    # so a plain `nix flake update cratedigger-src` can only ever reproduce
    # whatever the tip happens to be right now, and any requested target that
    # is not that tip is physically unproducible. That was the root cause of
    # the #1203 pin-receipt deadlock: a pending receipt whose target had
    # fallen behind cratedigger main could re-request neither the new
    # target (blocked by the receipt-divergence guard earlier in main())
    # nor its own old target (the tip had already moved past it too), with
    # no sanctioned exit. Honest tradeoff: this also removes a real race
    # that existed before -- anyone merging to cratedigger main between this
    # helper's fetch and its own flake update used to make the run fail
    # outright; overriding the exact revision makes that race impossible by
    # construction.
    nix flake update cratedigger-src \
      --override-input cratedigger-src "$cratedigger_flake_ref"
  )
  status_output=$(git -C "$WORKTREE" status --porcelain)
  [[ "$status_output" == ' M flake.lock' ]] \
    || die "nix update must change only tracked flake.lock (status=$status_output)"
  [[ "$(locked_cratedigger_revision < "$WORKTREE/flake.lock")" \
    == "$target_revision" ]] \
    || die "updated flake.lock does not pin requested Cratedigger revision $target_revision"

  git -C "$WORKTREE" add flake.lock
  SSH_AUTH_SOCK='' git -C "$WORKTREE" commit -m "$commit_message"
  candidate_revision=$(git -C "$NIXOSCONFIG_REPO" \
    rev-parse --verify "$PENDING_REF")
  PENDING_BASE=''
  if verify_pin_commit "$candidate_revision"; then
    :
  else
    verification_rc=$?
    if ((verification_rc == 2)); then
      git -C "$NIXOSCONFIG_REPO" update-ref -d \
        "$PENDING_REF" "$candidate_revision"
    fi
    return "$verification_rc"
  fi
  [[ "$VERIFIED_TARGET" == "$target_revision" \
    && "$VERIFIED_PARENT" == "$remote_revision" ]] \
    || die "new pin commit has unexpected state: revision=$candidate_revision target=$VERIFIED_TARGET base=$VERIFIED_PARENT"

  git -C "$NIXOSCONFIG_REPO" update-ref "$RECEIPT_REF" \
    "$candidate_revision" "$previous_receipt"
  INTENDED_REV=$candidate_revision
  INTENDED_TARGET=$VERIFIED_TARGET
  INTENDED_BASE=$VERIFIED_PARENT
  git -C "$NIXOSCONFIG_REPO" update-ref -d \
    "$PENDING_REF" "$candidate_revision"
  push_and_verify_with_token "$INTENDED_REV"
  printf 'signed nixosconfig revision: %s\n' "$INTENDED_REV"
}

main "$@"
