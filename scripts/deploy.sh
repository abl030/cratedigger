#!/usr/bin/env bash
# Deploy Cratedigger to doc2 in one command.
#
# Pins nixosconfig's `cratedigger-src` flake input to one exact Cratedigger
# revision (origin/main by default), commits that one-file change SSH-signed,
# pushes it to Forgejo master through nixosconfig's own token boundary,
# triggers doc2's verified rebuild over doc1's locked-sibling fleet trigger,
# waits for that nixos-upgrade run to finish, and checks doc2 activated the
# commit we pushed. One line per stage; the upgrade journal on failure.
#
# It does not verify the change itself. cratedigger-web, -importer,
# -import-preview-worker and -youtube-ingest restart on the switch;
# cratedigger.service has restartIfChanged = false and loads the new code on
# its next timer cycle. Check what you shipped through the real CLI, API, or
# UI afterwards.
#
# Merged main also ships unattended: doc1's nightly rolling flake update pins
# the branch tip and doc2 applies it the next morning. Run this only when a
# change needs to be live now.
#
# Usage: scripts/deploy.sh [<40-hex cratedigger revision on origin/main>]
#
# Environment (defaults are production; tests override them):
#   NIXOSCONFIG_TOKEN_FILE              Forgejo push token file
#   CRATEDIGGER_DEPLOY_POLL_SECONDS     seconds between nixos-upgrade polls
#   CRATEDIGGER_DEPLOY_TIMEOUT_SECONDS  bound on the nixos-upgrade wait
set -euo pipefail

# The fleet trigger selects its own private key and must never be cached
# into the shared agent; a wedged forwarded agent also hangs `git commit`
# signing. Every ssh, fleet-deploy and git call below runs without it.
unset SSH_AUTH_SOCK

readonly DEPLOY_HOST='doc2'
readonly NIXOSCONFIG_REPO="${HOME}/nixosconfig"
readonly FORGEJO_AUTH="${NIXOSCONFIG_REPO}/scripts/forgejo-auth.sh"
readonly FORGEJO_URL='https://git.ablz.au/abl030/nixosconfig.git'
readonly TOKEN_FILE="${NIXOSCONFIG_TOKEN_FILE:-/run/secrets/forgejo/nixbot-token}"
readonly POLL_SECONDS="${CRATEDIGGER_DEPLOY_POLL_SECONDS:-10}"
readonly TIMEOUT_SECONDS="${CRATEDIGGER_DEPLOY_TIMEOUT_SECONDS:-1800}"

worktree=''

stage() {
  printf 'deploy: %s\n' "$*"
}

die() {
  # die MESSAGE [EXIT_CODE]; 2 is a precondition/usage refusal, 1 a failure.
  printf 'deploy: ERROR: %s\n' "$1" >&2
  exit "${2:-1}"
}

cleanup() {
  # The pin worktree is private and dirty by construction (a detached HEAD
  # carrying the pin commit), so --force is the only way to remove it;
  # nothing in it is ever the operator's work.
  if [[ -n "$worktree" ]]; then
    git -C "$NIXOSCONFIG_REPO" worktree remove --force "$worktree" \
      >/dev/null 2>&1 || rm -rf "$worktree"
  fi
}
trap cleanup EXIT

# --- preconditions ---------------------------------------------------------

(($# <= 1)) || die 'usage: scripts/deploy.sh [<40-hex cratedigger revision>]' 2
[[ "$(hostname)" == 'proxmox-vm' ]] \
  || die 'run this on doc1 (hostname proxmox-vm): it alone holds the Forgejo token, the signing key and the fleet trigger key' 2
for tool in git nix jq ssh fleet-deploy; do
  command -v "$tool" >/dev/null 2>&1 || die "missing tool: $tool" 2
done
git -C "$NIXOSCONFIG_REPO" rev-parse --git-dir >/dev/null 2>&1 \
  || die "nixosconfig checkout not found: $NIXOSCONFIG_REPO" 2
[[ -x "$FORGEJO_AUTH" ]] || die "Forgejo auth helper not found: $FORGEJO_AUTH" 2
[[ -r "$TOKEN_FILE" ]] || die "Forgejo token not readable: $TOKEN_FILE" 2

# --- resolve the cratedigger revision --------------------------------------

cratedigger_repo=$(git -C "$(dirname "${BASH_SOURCE[0]}")" rev-parse --show-toplevel)
git -C "$cratedigger_repo" fetch --quiet origin '+refs/heads/main:refs/remotes/origin/main'
if (($# == 1)); then
  target=$1
  [[ "$target" =~ ^[0-9a-f]{40}$ ]] \
    || die "revision must be a full 40-hex SHA: $target" 2
  git -C "$cratedigger_repo" merge-base --is-ancestor "$target" refs/remotes/origin/main \
    || die "$target is not on origin/main; deploy only merged, pushed revisions"
else
  target=$(git -C "$cratedigger_repo" rev-parse refs/remotes/origin/main)
fi
stage "cratedigger $target"

# --- pin nixosconfig -------------------------------------------------------

git -C "$NIXOSCONFIG_REPO" fetch --quiet origin '+refs/heads/master:refs/remotes/origin/master'
master=$(git -C "$NIXOSCONFIG_REPO" rev-parse refs/remotes/origin/master)
pinned=$(git -C "$NIXOSCONFIG_REPO" show "$master:flake.lock" \
  | jq -er '.nodes["cratedigger-src"].locked.rev')

if [[ "$pinned" == "$target" ]]; then
  nixosconfig_rev=$master
  stage "nixosconfig master $master already pins it"
else
  stage "pinning $pinned -> $target"
  worktree=$(mktemp -d "${TMPDIR:-/tmp}/cratedigger-deploy.XXXXXX")
  git -C "$NIXOSCONFIG_REPO" worktree add --quiet --detach "$worktree" "$master"
  # The flake ref comes from the lock's own `original` node, so a moved input
  # origin can never silently pin the wrong repository.
  flake_ref=$(jq -er '.nodes["cratedigger-src"].original
    | select(.type == "github") | "github:\(.owner)/\(.repo)"' "$worktree/flake.lock") \
    || die 'cratedigger-src is not a github flake input in flake.lock'
  (cd "$worktree" && nix flake update cratedigger-src \
    --override-input cratedigger-src "$flake_ref/$target")
  [[ "$(git -C "$worktree" status --porcelain)" == ' M flake.lock' ]] \
    || die 'nix flake update changed something other than flake.lock'
  [[ "$(jq -er '.nodes["cratedigger-src"].locked.rev' "$worktree/flake.lock")" == "$target" ]] \
    || die "flake.lock does not pin $target after the update"
  git -C "$worktree" add flake.lock
  git -C "$worktree" commit --quiet -m "cratedigger: deploy ${target:0:12}"
  nixosconfig_rev=$(git -C "$worktree" rev-parse HEAD)
  # doc2's fleet-update refuses an unsigned commit, and so would every other
  # host's nightly update until master was fixed. Fail here, before the push.
  [[ "$(git -C "$worktree" log -1 '--format=%G?' "$nixosconfig_rev")" == 'G' ]] \
    || die "pin commit $nixosconfig_rev is not verifiably SSH-signed"
  "$FORGEJO_AUTH" git-push --repo "$worktree" --remote origin \
    --expected-fetch-url "$FORGEJO_URL" --expected-push-url "$FORGEJO_URL" \
    --token-file "$TOKEN_FILE" --refspec "$nixosconfig_rev:refs/heads/master"
  remote=$("$FORGEJO_AUTH" git-ls-remote --repo "$worktree" --remote origin \
    --expected-fetch-url "$FORGEJO_URL" --expected-push-url "$FORGEJO_URL" \
    --token-file "$TOKEN_FILE" --ref refs/heads/master | cut -f1)
  [[ "$remote" == "$nixosconfig_rev" ]] \
    || die "Forgejo master is $remote, not the pushed $nixosconfig_rev"
  stage "nixosconfig $nixosconfig_rev pushed to Forgejo master"
fi

# --- trigger doc2 and wait for that run -------------------------------------

# An upgrade already running (the nightly window, or a trigger someone else
# sent) absorbs a new trigger into its own job and never mints a fresh
# invocation, so the wait below would burn its whole deadline on nothing.
# Let it finish first; it may even be shipping this very pin.
deadline=$((SECONDS + TIMEOUT_SECONDS))
announced=0
while :; do
  active=$(ssh "$DEPLOY_HOST" \
    'systemctl show nixos-upgrade.service --property=ActiveState --value') \
    || die "could not read nixos-upgrade state on $DEPLOY_HOST"
  case "$active" in
    activating|active|reloading|deactivating) ;;
    *) break ;;
  esac
  ((SECONDS < deadline)) \
    || die "timed out after ${TIMEOUT_SECONDS}s waiting for an in-flight nixos-upgrade on $DEPLOY_HOST to finish"
  if ((announced == 0)); then
    stage "nixos-upgrade already running on $DEPLOY_HOST; waiting for it to finish"
    announced=1
  fi
  sleep "$POLL_SECONDS"
done

previous=$(ssh "$DEPLOY_HOST" \
  'systemctl show nixos-upgrade.service --property=InvocationID --value') \
  || die "could not read nixos-upgrade state on $DEPLOY_HOST"
stage "triggering nixos-upgrade on $DEPLOY_HOST (previous invocation ${previous:-none})"
fleet-deploy "$DEPLOY_HOST" || die "fleet-deploy $DEPLOY_HOST failed"

deadline=$((SECONDS + TIMEOUT_SECONDS))
triggered=''
completed=0
while ((SECONDS < deadline)); do
  state=$(ssh "$DEPLOY_HOST" 'systemctl show nixos-upgrade.service \
    --property=InvocationID --property=ActiveState --property=SubState --property=Result') \
    || die "could not read nixos-upgrade state on $DEPLOY_HOST"
  invocation=$(sed -n 's/^InvocationID=//p' <<<"$state")
  active=$(sed -n 's/^ActiveState=//p' <<<"$state")
  sub=$(sed -n 's/^SubState=//p' <<<"$state")
  result=$(sed -n 's/^Result=//p' <<<"$state")
  if [[ -z "$invocation" || "$invocation" == "$previous" ]]; then
    [[ -z "$triggered" ]] || die "nixos-upgrade invocation $triggered disappeared: $state"
    sleep "$POLL_SECONDS"
    continue
  fi
  if [[ -z "$triggered" ]]; then
    triggered=$invocation
    stage "nixos-upgrade invocation $triggered started"
  elif [[ "$invocation" != "$triggered" ]]; then
    die "nixos-upgrade invocation changed mid-deploy: $state"
  fi
  if [[ "$active" == 'inactive' && "$sub" == 'dead' && "$result" == 'success' ]]; then
    completed=1
    break
  fi
  if [[ "$active" == 'failed' || "$active" == 'inactive' ]]; then
    ssh "$DEPLOY_HOST" 'journalctl -u nixos-upgrade.service -n 100 --no-pager' >&2 || true
    die "nixos-upgrade failed: $state"
  fi
  case "$active" in
    activating|active|reloading|deactivating) ;;
    *) die "unexpected nixos-upgrade state: $state" ;;
  esac
  sleep "$POLL_SECONDS"
done
if ((completed != 1)); then
  ssh "$DEPLOY_HOST" 'journalctl -u nixos-upgrade.service -n 100 --no-pager' >&2 || true
  die "timed out after ${TIMEOUT_SECONDS}s waiting for nixos-upgrade on $DEPLOY_HOST"
fi

# A green unit on a stale revision is not a deployment: the anchor is what
# doc2's verified update actually activated.
anchor=$(ssh "$DEPLOY_HOST" 'sudo cat /var/lib/fleet-update/last-verified-rev') \
  || die "could not read the fleet anchor on $DEPLOY_HOST"
[[ "$anchor" == "$nixosconfig_rev" ]] \
  || die "$DEPLOY_HOST activated nixosconfig $anchor, not $nixosconfig_rev"

stage "$DEPLOY_HOST activated nixosconfig $nixosconfig_rev = cratedigger $target in ${SECONDS}s"
stage 'workers restarted on the switch; cratedigger.service loads the new code on its next timer cycle'
stage 'check your change through the real CLI, API, or UI'
