---
name: deploy
description: Deploy a verified Cratedigger revision through GitHub, the nixosconfig Forgejo pin, and doc1's locked-sibling fleet trigger.
---

# Deploy to doc2

Push code, update the flake input on doc1, push nixosconfig to Forgejo, trigger
doc2 through doc1's locked-sibling deployment boundary, then verify the exact
revision, migrations, services, and source.

**Since the Forgejo cutover (2026-06-10), nixosconfig deploys come from FORGEJO (`git.ablz.au`), never `github:abl030/nixosconfig` — GitHub is a frozen fallback.** The cratedigger repo itself still lives on GitHub; only the nixosconfig leg changed.

**Run this workflow on doc1 (`hostname` = `proxmox-vm`).** Stop if that is not
the current host. doc1 alone has both the Forgejo push token/signing key and the
private locked-sibling trigger key.

Every SSH command below runs without `SSH_AUTH_SOCK`. The fleet trigger already
selects its private key explicitly, so this prevents that forced-command key
from being cached in the shared agent; ordinary operator commands likewise
cannot offer a previously cached trigger key. The tracked cycle verifier also
sets `IdentityAgent=none` internally. Do not simplify these boundaries back to
plain `ssh` / `fleet-deploy` calls.

## Steps

1. If the cratedigger revision is not already pushed, commit the final reviewed
tree, run the `check` skill once on that commit, then push it to GitHub. If the
revision was already pushed unchanged, do not replay the final validation:
```bash
set -euo pipefail
git add <files>
git commit -m "<message>"
# Run the check skill here when this is the first push of the revision.
git push
```

2. Before pinning, resolve the revision to pin, sanity-check the current
checkout against `origin/main`, and check whether a previous deploy was
silently dropped. **Pin `origin/main`'s own resolved tip, never a locally
computed `HEAD`.** The house workflow merges PRs as merge commits from
feature worktrees, so right after `gh pr merge` the local checkout's `HEAD`
is typically the merged branch's own head commit, not the merge commit
`origin/main` actually carries -- pinning that would silently omit anything
else merged in the meantime, and it also poisons the drop-detector below: a
locked revision that is never itself an `origin/main` commit makes
`DEPLOYED..origin/main` list at least the merge commit forever, turning the
detector into a permanent false alarm. An ancestry check of the current
checkout's `HEAD` against the resolved tip is a useful smoke test, but be
precise about what it proves: when it passes, it only means this checkout
carries nothing `origin/main` lacks -- not that any particular merge landed,
and a stale checkout already sitting exactly on `origin/main` passes
trivially without having tested this session's work at all. When it fails,
there are two real, indistinguishable-by-the-check causes: the merge
genuinely did not land (`gh pr merge --delete-branch` exits 1 from a
worktree checkout even when the remote merge itself succeeded, so its own
exit code is not evidence either way), OR this checkout carries commits that
were never meant to be pinned here -- a `.claude/memory/` commit, made
locally per CLAUDE.md's own mandate ahead of its own separate push, is the
expected example, not a corner case, since it shares this exact checkout.
Because that second cause is routine and sanctioned, a hard `exit 1` here
would block a correct deploy on a regular basis, so this stays a loud
diagnostic the reader must consciously clear rather than an automatic stop:
inspect `git log --oneline origin/main..HEAD`, and proceed only once
everything listed is either about to be pinned or is deliberately-unpushed
local work with nothing to do with this deploy. The drop check that follows
it is a report, not a gate (#1203: PR #1201 sat merged-but-undeployed for
~14 hours until an unrelated pin swept it up as an ancestor):
```bash
set -euo pipefail
CRATEDIGGER_REPO=$(git rev-parse --show-toplevel)
git -C "$CRATEDIGGER_REPO" fetch origin '+refs/heads/main:refs/remotes/origin/main'
CRATEDIGGER_REV=$(git -C "$CRATEDIGGER_REPO" rev-parse origin/main)
git -C "$CRATEDIGGER_REPO" merge-base --is-ancestor HEAD "$CRATEDIGGER_REV" || {
  echo "local HEAD is not an ancestor of origin/main -- EITHER the merge did not land, OR this checkout carries commits never meant to be pinned here (a .claude/memory/ commit is the expected example)." >&2
  echo "Inspect: git -C \"$CRATEDIGGER_REPO\" log --oneline origin/main..HEAD -- proceed only if everything listed is deliberately-unpushed local work unrelated to this deploy." >&2
}
git -C ~/nixosconfig fetch origin '+refs/heads/master:refs/remotes/origin/master'
DEPLOYED_CRATEDIGGER_REV=$(git -C ~/nixosconfig show \
  origin/master:flake.lock | jq -er '.nodes["cratedigger-src"].locked.rev')
git -C "$CRATEDIGGER_REPO" log --oneline "$DEPLOYED_CRATEDIGGER_REV..origin/main"
```
Read the range for commits this session did not just merge. It is never
empty in the ordinary case -- it always includes the very work you are about
to pin, since that is the whole reason this deploy is happening, so do not
expect (or wait for) an empty range. The only question is whether it ALSO
holds something you do not recognise: a commit from an earlier session's
merge that never got picked up is a previous deploy that was silently
dropped; deal with it before pinning.

Then invoke the checked Bash entrypoint with the exact revision to pin. The
entrypoint runs the complete nixosconfig
fetch → detached worktree → `cratedigger-src`-only lock update, pinned to the
exact requested revision rather than the input's branch tip → SSH-signature
verification → token-header Forgejo push → exact remote-SHA verification →
cleanup lifecycle. It refuses to run anywhere except doc1 and never depends on
the caller's interactive/default shell:
```bash
set -euo pipefail
CRATEDIGGER_REPO=$(git rev-parse --show-toplevel)
CRATEDIGGER_REV=$(git -C "$CRATEDIGGER_REPO" rev-parse origin/main)
"$CRATEDIGGER_REPO/scripts/pin_nixosconfig.sh" \
  "$CRATEDIGGER_REV" "cratedigger: <description>"
```

The helper commits through a private pending ref, so the signed commit becomes
durably reachable in the commit transaction itself, then promotes that exact
commit to `refs/cratedigger-deploy/cratedigger-src` before pushing. Retry the
exact same invocation after any failure: it recovers
`refs/cratedigger-deploy/cratedigger-src-pending` first; if Forgejo master is
still at the pin's parent, it pushes the already-created commit; first it
checks a stable current master before mutating either private ref, retaining a
pending candidate whenever that remote is untrusted or incompatible. If Forgejo
is already at the pending revision, it reports success without creating another
commit; and if Forgejo advanced to an incompatible revision, it fails with the
exact pending, base, and remote SHAs. A sibling receipt may advance only for a
new target when current signed Forgejo master itself verifies and pins the
receipt's exact old target. For a same-target sibling, a verified remote at
that target reports success; a verified remote at the receipt parent's target
is a rejected candidate, so the helper creates one replacement from current
master through the ordinary receipt compare-and-swap. The helper rechecks
master before either transaction. Never delete or rewrite either private
recovery ref by hand during a retry -- if a receipt's own commit never
reached Forgejo master and master has since advanced past its parent, the
retry fails with `different pin is still pending`, and the sanctioned exit
is a two-step re-run instead of a hand edit. **Before running step 1**,
confirm two things about the receipt's own target: it is still reachable
from `origin/main` (`git -C "$CRATEDIGGER_REPO" merge-base --is-ancestor
<target> origin/main` -- unlike the ordinary path above, ancestry is exactly
the right check here, because this target is deliberately not the tip), and
it does not predate a forward-only migration boundary -- currently migration
066 (processing ownership, #898): `.claude/rules/deploy.md` states plainly
that cratedigger must never be repinned to a pre-#898 source, and step 1
would push exactly that if the abandoned receipt is old enough to cross one.
If either check fails or there is any doubt, escalate instead of running
step 1. Step 1 re-runs with
`target=<the pending_target the failure names>`, replaying the ordinary
"replacing rejected pending revision" path to land that exact old revision
on Forgejo master -- possible at all only because the helper pins the
requested revision exactly, never the `cratedigger-src` branch tip. **That
landing is real and fully deployable**: doc2's unattended
`nixos-upgrade.timer` (`OnCalendar=04:00`, up to 60 minutes of
`RandomizedDelaySec`, `Persistent=true`) will pull and deploy exactly that
recovered target with no human present if nothing else happens first. Step 1
and step 2 therefore run back-to-back in the same sitting -- step 2 re-runs
with the originally intended target, which now proceeds normally because the
receipt is master -- and the sitting is not over until step 2's own output
confirms Forgejo master pins that intended target, not merely that the
command exited zero. If step 2 fails for any reason (network, an
inconclusive signature check, a dropped session), master is left pinning the
OLD recovered target: treat that as an active incident, not a state to leave
overnight, and keep re-running step 2 alone until it confirms the intended
target, or escalate before ending the session. A transient or inconclusive
verification result (for example, unavailable allowed-signers configuration)
retains the pending candidate; only a definitively bad or unsigned candidate
is discarded so a later invocation can create a valid signed pin.

The Forgejo token remains confined to the helper's fail-fast subshell
environment and must never appear in an argv value, command-line `-c`
assignment, remote URL, xtrace, or Git trace output.

3. Capture the current systemd invocations, deploy doc2 through the
forced-command locked-sibling trigger, then wait up to 30 minutes for a **new,
nonempty** `InvocationID`. Capturing before the trigger prevents an old green
upgrade result from being mistaken for this deployment. Print the current
Cratedigger invocation as pre-switch audit evidence; step 6 establishes its
verification boundary from a fresh post-switch observation. Poll the new
upgrade invocation to a terminal state. This also handles a same-revision retry:
the anchor may already equal the target, but a fresh invocation must still run.
Success means `ActiveState=inactive`, `SubState=dead`, and `Result=success` for
the new invocation; any failed, unexpected, replaced, or timed-out state is a
deploy failure:
```bash
set -euo pipefail
CRATEDIGGER_REPO=$(git rev-parse --show-toplevel)
PRE_SWITCH_CRATEDIGGER_INVOCATION=$(
  "$CRATEDIGGER_REPO/scripts/verify_cratedigger_cycle.sh" capture-current
)
printf 'PRE_SWITCH_CRATEDIGGER_INVOCATION=%s\n' \
  "$PRE_SWITCH_CRATEDIGGER_INVOCATION"
PRE_SWITCH_MIGRATE_INVOCATION=$(
  "$CRATEDIGGER_REPO/scripts/verify_cratedigger_cycle.sh" capture-migrate
)
printf 'PRE_SWITCH_MIGRATE_INVOCATION=%s\n' "$PRE_SWITCH_MIGRATE_INVOCATION"
before_state=$(env -u SSH_AUTH_SOCK ssh doc2 'systemctl show nixos-upgrade.service \
  --property=InvocationID')
PREVIOUS_INVOCATION=$(sed -n 's/^InvocationID=//p' <<<"$before_state")
env -u SSH_AUTH_SOCK fleet-deploy doc2
deadline=$((SECONDS + 1800))
triggered_invocation=""
deploy_complete=0
while ((SECONDS < deadline)); do
  if ! upgrade_state=$(env -u SSH_AUTH_SOCK ssh doc2 'systemctl show nixos-upgrade.service \
    --property=InvocationID --property=ActiveState \
    --property=SubState --property=Result'); then
    echo 'could not read doc2 nixos-upgrade state' >&2
    exit 1
  fi
  invocation=$(sed -n 's/^InvocationID=//p' <<<"$upgrade_state")
  active=$(sed -n 's/^ActiveState=//p' <<<"$upgrade_state")
  sub=$(sed -n 's/^SubState=//p' <<<"$upgrade_state")
  result=$(sed -n 's/^Result=//p' <<<"$upgrade_state")
  printf 'nixos-upgrade: InvocationID=%s ActiveState=%s SubState=%s Result=%s\n' \
    "$invocation" "$active" "$sub" "$result"
  if [[ -z "$invocation" ]]; then
    if [[ -n "$triggered_invocation" ]]; then
      echo 'triggered nixos-upgrade InvocationID disappeared' >&2
      exit 1
    fi
    sleep 5
    continue
  fi
  if [[ "$invocation" == "$PREVIOUS_INVOCATION" ]]; then
    sleep 5
    continue
  fi
  if [[ -z "$triggered_invocation" ]]; then
    triggered_invocation=$invocation
  elif [[ "$invocation" != "$triggered_invocation" ]]; then
    echo "nixos-upgrade invocation changed during deploy: $upgrade_state" >&2
    exit 1
  fi
  if [[ "$active" == inactive && "$sub" == dead && "$result" == success ]]; then
    deploy_complete=1
    break
  fi
  if [[ "$active" == failed || "$active" == inactive ]]; then
    env -u SSH_AUTH_SOCK ssh doc2 'journalctl -u nixos-upgrade.service -n 100 --no-pager' || true
    exit 1
  fi
  if [[ "$active" != activating && "$active" != active \
    && "$active" != reloading && "$active" != deactivating ]]; then
    echo "unexpected nixos-upgrade state: $upgrade_state" >&2
    exit 1
  fi
  sleep 5
done
if [[ "$deploy_complete" != 1 ]]; then
  echo 'timed out waiting for the triggered nixos-upgrade invocation' >&2
  env -u SSH_AUTH_SOCK ssh doc2 'journalctl -u nixos-upgrade.service -n 100 --no-pager' || true
  exit 1
fi
"$CRATEDIGGER_REPO/scripts/verify_cratedigger_cycle.sh" \
  verify-migrate-ran "$PRE_SWITCH_MIGRATE_INVOCATION"
```
The migrate assertion lives in this block deliberately: it needs the
pre-trigger value captured above, and every numbered step is an independent
shell. **`ActiveState=active`, `SubState=exited` and `Result=success` on
`cratedigger-db-migrate.service` are satisfied by a run from days ago**, so
they are not evidence it ran for this switch — only a changed `InvocationID`
is (issue #1161). The check applies to ordinary and strict-held deploys alike;
the metadata gate hold never stops the migrate unit.
The printed `PRE_SWITCH_CRATEDIGGER_INVOCATION` is audit evidence only. Do not
use it as the post-switch cycle baseline: timer cycles can roll while the
asynchronous fleet build is still running.

`fleet-deploy` is asynchronous. It starts doc2's verified
`nixos-upgrade.service`, which fetches Forgejo, verifies every new commit
against `hosts.nix`, builds from its root-owned clone, switches, and runs
`cratedigger-db-migrate.service`. Direct
`env -u SSH_AUTH_SOCK ssh doc2 'sudo fleet-update'` is not the normal path and
bypasses the locked-sibling trigger boundary. Do not use `nixos-rebuild switch
--flake github:...`; GitHub is stale.

4. Verify the fleet trust anchor equals the exact signed Forgejo commit printed
in step 2. A green unit with a stale anchor is not a successful deployment:
```bash
set -euo pipefail
EXPECTED_NIXOSCONFIG_REV=<full signed SHA printed by step 2>
DEPLOYED_REV=$(env -u SSH_AUTH_SOCK ssh doc2 'sudo cat /var/lib/fleet-update/last-verified-rev')
test "$DEPLOYED_REV" = "$EXPECTED_NIXOSCONFIG_REV"
```

5. Verify the applied schema and the services affected by the change. That the
migrate unit actually RAN for this switch was already proven at the end of
step 3 (`verify-migrate-ran`); this step reads what it applied. For an ordinary
deployment, verify long-running workers individually rather than assuming a
successful switch made them healthy. For a strict held deployment, do not run
the ordinary active-service check below: `verify-held` deliberately drains
those services, and the held workflow proves their state at each release
boundary instead.
```bash
set -euo pipefail
"$CRATEDIGGER_REPO/scripts/verify_cratedigger_cycle.sh" \
  verify-migrate-ran "$PRE_SWITCH_MIGRATE_INVOCATION"
migration_rows=$(env -u SSH_AUTH_SOCK ssh doc2 'set -euo pipefail; \
  export PGPASSWORD=$(sudo cat /run/secrets/cratedigger-pgpass \
    | grep "^PGPASSWORD=" | cut -d= -f2); \
  test -n "$PGPASSWORD"; pipeline-cli query -' <<'SQL'
SELECT version, name, applied_at
FROM schema_migrations
ORDER BY version DESC
LIMIT 5;
SQL
)
test -n "$migration_rows"
printf '%s\n' "$migration_rows"
processing_owner_audit=$(env -u SSH_AUTH_SOCK ssh doc2 'set -euo pipefail; \
  export PGPASSWORD=$(sudo cat /run/secrets/cratedigger-pgpass \
    | grep "^PGPASSWORD=" | cut -d= -f2); \
  test -n "$PGPASSWORD"; pipeline-cli query --json -' <<'SQL'
WITH owner_rows AS (
  SELECT request.id AS request_id,
         request.status,
         request.active_automation_import_job_id,
         job.id AS job_id,
         job.request_id AS job_request_id,
         job.job_type,
         job.status AS job_status
  FROM album_requests AS request
  LEFT JOIN import_jobs AS job
    ON job.id = request.active_automation_import_job_id
)
SELECT EXISTS (
         SELECT 1
         FROM schema_migrations
         WHERE version = 66
           AND name = 'processing_automation_owner'
       ) AS migration_066_applied,
       (
         SELECT count(*) = 1
         FROM pg_index AS index_state
         JOIN pg_class AS index_class
           ON index_class.oid = index_state.indexrelid
         JOIN pg_class AS indexed_table
           ON indexed_table.oid = index_state.indrelid
         JOIN pg_namespace AS indexed_namespace
           ON indexed_namespace.oid = indexed_table.relnamespace
         WHERE index_class.relname
                 = 'one_active_automation_import_per_request'
           AND indexed_table.relname = 'import_jobs'
           AND indexed_namespace.nspname = current_schema()
           AND index_state.indisunique
           AND index_state.indisvalid
           AND index_state.indisready
           AND index_state.indpred IS NOT NULL
       ) AS active_owner_index_ready,
       (
         SELECT count(*) = 3
         FROM pg_constraint
         WHERE conname IN (
           'album_requests_active_automation_owner_unique',
           'album_requests_active_automation_owner_fk',
           'processing_cleanup_journal_job_request_fk'
         )
           AND convalidated
           AND condeferrable
           AND condeferred
       ) AS owner_constraints_validated,
       (
         SELECT count(*) = 3
         FROM pg_trigger
         WHERE tgname IN (
           'album_requests_complete_processing_owner',
           'import_jobs_complete_processing_owner',
           'processing_cleanup_journal_exact_owner'
         )
           AND NOT tgisinternal
           AND tgenabled = 'O'
           AND tgdeferrable
           AND tginitdeferred
       ) AS owner_triggers_enabled,
       (
         SELECT count(*)
         FROM owner_rows
         WHERE (status = 'processing') IS DISTINCT FROM
               (active_automation_import_job_id IS NOT NULL)
       ) AS status_pointer_violations,
       (
         SELECT count(*)
         FROM owner_rows
         WHERE active_automation_import_job_id IS NOT NULL
           AND (
             job_id IS NULL
             OR job_request_id IS DISTINCT FROM request_id
             OR job_type IS DISTINCT FROM 'automation_import'
             OR job_status NOT IN ('queued', 'running', 'recovery_required')
           )
       ) AS bad_request_owner_violations,
       (
         SELECT count(*)
         FROM import_jobs AS job
         LEFT JOIN album_requests AS request
           ON request.id = job.request_id
          AND request.status = 'processing'
          AND request.active_automation_import_job_id = job.id
         WHERE job.job_type = 'automation_import'
           AND job.status IN ('queued', 'running', 'recovery_required')
           AND request.id IS NULL
       ) AS orphan_active_job_violations,
       (
         SELECT count(*)
         FROM processing_cleanup_journal AS journal
         LEFT JOIN album_requests AS request
           ON request.id = journal.request_id
         LEFT JOIN import_jobs AS job
           ON job.id = journal.job_id
          AND job.request_id = journal.request_id
         WHERE request.id IS NULL
            OR request.status IS DISTINCT FROM 'processing'
            OR request.active_automation_import_job_id
                 IS DISTINCT FROM journal.job_id
            OR job.id IS NULL
            OR job.job_type IS DISTINCT FROM 'automation_import'
            OR job.status NOT IN ('queued', 'running', 'recovery_required')
       ) AS bad_cleanup_journal_violations;
SQL
)
printf '%s\n' "$processing_owner_audit"
CRATEDIGGER_PROCESSING_OWNER_AUDIT="$processing_owner_audit" python3 - <<'PY'
import json
import os

rows = json.loads(os.environ["CRATEDIGGER_PROCESSING_OWNER_AUDIT"])
expected = {
    "migration_066_applied": True,
    "active_owner_index_ready": True,
    "owner_constraints_validated": True,
    "owner_triggers_enabled": True,
    "status_pointer_violations": 0,
    "bad_request_owner_violations": 0,
    "orphan_active_job_violations": 0,
    "bad_cleanup_journal_violations": 0,
}
if rows != [expected]:
    raise SystemExit(f"processing owner audit failed: {rows!r}")
PY
# Ordinary deployments only. Strict held deployments prove the deliberately
# inactive boundary after `verify-held`, then the staged active boundaries
# after `prepare-controlled` and `finish-release`.
if env -u SSH_AUTH_SOCK ssh doc2 \
  'sudo test -f /run/cratedigger-deploy-hold/receipt'; then
  printf '%s\n' \
    'strict deploy hold present: active-service check deferred to held release'
else
  service_states=$(env -u SSH_AUTH_SOCK ssh doc2 'set -euo pipefail
    for unit in cratedigger-web.service cratedigger-importer.service \
      cratedigger-import-preview-worker.service cratedigger-youtube-ingest.service; do
      state=$(systemctl is-active "$unit")
      test "$state" = active
      printf "%s=%s\n" "$unit" "$state"
    done')
  printf '%s\n' "$service_states"
fi
```

6. Derive the active wrapper from the service's `ExecStart`, then derive its
exact source store from the wrapper and verify the deployed change there
(choose a unique string in a production file). Do not glob every historical
store path: an old generation could produce a false positive. Inspect the
rendered unit/config when the NixOS module changed. After deriving and checking
the active source, capture a fresh tail cursor from the Cratedigger unit
journal as the post-switch baseline. Then enumerate ordered start records
after that cursor and capture the first invocation whose
invocation-scoped journal names that exact source and verify it through the
tracked boundary. This guarantees that the verified cycle started after a
post-switch observation even on a same-source, same-revision retry; waiting one
extra cycle is safe. The verifier requires the application cycle-complete
record plus systemd's successful deactivation and finished-job records. If the
back-to-back timer has already replaced the target `InvocationID`, it follows
the captured target through `journalctl --invocation=<ID>` instead of treating
rollover as either success or failure. The journal cursor also prevents a
short-lived failed invocation from vanishing between state polls:
```bash
set -euo pipefail
CRATEDIGGER_REPO=$(git rev-parse --show-toplevel)
CRATEDIGGER_BIN=$(env -u SSH_AUTH_SOCK ssh doc2 "systemctl show cratedigger.service \
  --property=ExecStart --value | grep -o '/nix/store/[^ ;]*/bin/cratedigger' \
  | head -1")
test -n "$CRATEDIGGER_BIN"
CRATEDIGGER_SOURCE=$(env -u SSH_AUTH_SOCK ssh doc2 "grep -o '/nix/store/[^ ]*-source/cratedigger.py' \
  '$CRATEDIGGER_BIN' | head -1 | sed 's#/cratedigger.py##'")
test -n "$CRATEDIGGER_SOURCE"
env -u SSH_AUTH_SOCK ssh doc2 "grep '<something unique>' '$CRATEDIGGER_SOURCE/<changed-file>.py'"
POST_SWITCH_CRATEDIGGER_CURSOR=$(
  "$CRATEDIGGER_REPO/scripts/verify_cratedigger_cycle.sh" capture-cursor
)
printf 'POST_SWITCH_CRATEDIGGER_CURSOR=%s\n' \
  "$POST_SWITCH_CRATEDIGGER_CURSOR"
TARGET_CRATEDIGGER_INVOCATION=$(
  "$CRATEDIGGER_REPO/scripts/verify_cratedigger_cycle.sh" capture-target \
    "$POST_SWITCH_CRATEDIGGER_CURSOR" "$CRATEDIGGER_SOURCE"
)
"$CRATEDIGGER_REPO/scripts/verify_cratedigger_cycle.sh" verify-exact \
  "$TARGET_CRATEDIGGER_INVOCATION" "$CRATEDIGGER_SOURCE"
# For nix/module.nix changes:
env -u SSH_AUTH_SOCK ssh doc2 'systemctl cat cratedigger.service'
env -u SSH_AUTH_SOCK ssh doc2 'grep "<rendered setting>" /var/lib/cratedigger/config.ini'
```

7. After live verification of a non-trivial series, run the **post-ship
reflection** (`.claude/rules/deploy.md` § "Post-ship reflection") — mine your
own session context for the debt this work surfaced (deferred review findings,
things fixed twice, duplication the series introduced, audits that could catch
review findings for free), de-dupe against open issues, and file ONE covering
issue (pattern: #573, #590) or state that nothing clears the bar.

## Holding timer-driven work across a switch

NixOS-generated units under `/etc/systemd/system` outrank ordinary runtime
masks under `/run/systemd/system`, and a timer mask does not cancel service
starts that systemd already queued. Strict holds therefore use the tracked
helper; never substitute `systemctl mask --runtime`, a service mask, or manual
link cleanup. The helper fixes the authority surface to the main, unfindable,
and metadata-gate-watchdog timers plus every metadata-gate-guarded service. It
records only the control links, manual hold, and main/YouTube start inhibitors
it created.

`acquire` masks and stops the three timers, then drains the timer-driven
producers (main, unfindable, watchdog — `TIMER_DRIVEN_PRODUCER_UNITS`) and
waits — bounded, separately from the overall service-drain timeout — for the
still-running importer/preview to empty the automation queue, **before**
taking the metadata-gate manual hold that stops the controlled workers
(#1078). Taking the hold first would stop the very workers that drain the
queue, deadlocking against the old-lifecycle preflight below.
`active_automation_jobs`/`dirty_downloading_rows` drain this way;
`recovery_required_jobs`/`malformed_enqueued_at_rows` are anomalies nothing
drains, so they still fail immediately once the hold is taken.
`cratedigger-youtube-ingest` has no timer, so nothing before the gate hold
ever asks it to stop — it is deliberately NOT drained in this pre-hold phase
(draining it there would wait the full service-drain timeout for nothing).
The pre-hold window owns no start inhibitor at all: masking already blocks
the timer trigger, but it does not block an operator manually starting
`cratedigger.service` by hand, so once the gate hold is taken, `acquire`
re-drains every unit it knows about (`SERVICE_UNITS`, not just the ones the
gate itself stopped) to catch that before reaching HELD, right before the
migration this hold gates.

Run the reviewed helper on doc2 through Python stdin so the pre-switch host does
not need this revision deployed already:

```bash
set -euo pipefail
CRATEDIGGER_REPO=$(git rev-parse --show-toplevel)
DEPLOY_HOLD="$CRATEDIGGER_REPO/scripts/cratedigger_deploy_hold.py"
CYCLE_VERIFY="$CRATEDIGGER_REPO/scripts/verify_cratedigger_cycle.sh"

# Before fleet-deploy: prove the independently deployed main/YouTube
# controlled-start prerequisite, drain producers and the automation queue,
# take authoritative masks and the gate hold, and query the old live schema --
# failing under the strict hold unless automation jobs/recovery rows/staged
# downloading rows/malformed enqueued_at witnesses are all zero.
env -u SSH_AUTH_SOCK ssh doc2 'sudo python3 - acquire' < "$DEPLOY_HOLD"
```

After the exact `nixos-upgrade.service` invocation succeeds, re-prove the same
receipt-owned boundary before any strict one-shot or state rewrite:

```bash
env -u SSH_AUTH_SOCK ssh doc2 'sudo python3 - verify-held' < "$DEPLOY_HOLD"
held_service_states=$(env -u SSH_AUTH_SOCK ssh doc2 'set -euo pipefail
  for unit in cratedigger.service cratedigger-unfindable.service \
    cratedigger-metadata-gate-watchdog.service \
    cratedigger-youtube-ingest.service cratedigger-web.service \
    cratedigger-importer.service cratedigger-import-preview-worker.service; do
    active=$(systemctl show "$unit" --property=ActiveState --value)
    sub=$(systemctl show "$unit" --property=SubState --value)
    job=$(systemctl show "$unit" --property=Job --value)
    test "$active" = inactive
    test "$sub" = dead
    test -z "$job" || test "$job" = 0
    printf "%s active=%s sub=%s job=%s\n" "$unit" "$active" "$sub" "${job:-none}"
  done')
printf '%s\n' "$held_service_states"
held_processing_boundary=$(env -u SSH_AUTH_SOCK ssh doc2 'set -euo pipefail; \
  export PGPASSWORD=$(sudo cat /run/secrets/cratedigger-pgpass \
    | grep "^PGPASSWORD=" | cut -d= -f2); \
  test -n "$PGPASSWORD"; pipeline-cli query --json -' <<'SQL'
SELECT count(*) FILTER (WHERE status = 'processing')
         AS processing_requests,
       count(*) FILTER (
         WHERE active_automation_import_job_id IS NOT NULL
       ) AS owner_pointers,
       (
         SELECT count(*)
         FROM import_jobs
         WHERE job_type = 'automation_import'
           AND status IN ('queued', 'running', 'recovery_required')
       ) AS active_automation_jobs,
       (
         SELECT count(*)
         FROM processing_cleanup_journal
       ) AS cleanup_journals
FROM album_requests;
SQL
)
printf '%s\n' "$held_processing_boundary"
CRATEDIGGER_HELD_PROCESSING_BOUNDARY="$held_processing_boundary" python3 - <<'PY'
import json
import os

rows = json.loads(os.environ["CRATEDIGGER_HELD_PROCESSING_BOUNDARY"])
expected = [{
    "processing_requests": 0,
    "owner_pointers": 0,
    "active_automation_jobs": 0,
    "cleanup_journals": 0,
}]
if rows != expected:
    raise SystemExit(f"held processing boundary is dirty: {rows!r}")
PY
# Run and reconcile the reviewed maintenance operation here.
```

Release in four evidence-gated phases. Derive `CRATEDIGGER_SOURCE` from the
active wrapper as in step 6 before capturing either cycle:

```bash
# All three timers remain masked. prepare-controlled creates separate
# receipt-owned main/YouTube inhibitors, releases the manual metadata hold,
# explicitly starts and proves the web, preview, and importer services,
# exercises an overlapping
# resume-if-clear while both producers remain inactive, then removes only the
# main inhibitor and starts one controlled main cycle. YouTube stays inhibited.
CONTROLLED_CURSOR=$("$CYCLE_VERIFY" capture-cursor)
env -u SSH_AUTH_SOCK ssh doc2 'sudo python3 - prepare-controlled' < "$DEPLOY_HOLD"
controlled_service_states=$(env -u SSH_AUTH_SOCK ssh doc2 'set -euo pipefail
  for unit in cratedigger-web.service cratedigger-importer.service \
    cratedigger-import-preview-worker.service; do
    state=$(systemctl is-active "$unit")
    test "$state" = active
    printf "%s=%s\n" "$unit" "$state"
  done
  youtube_active=$(systemctl show cratedigger-youtube-ingest.service \
    --property=ActiveState --value)
  youtube_sub=$(systemctl show cratedigger-youtube-ingest.service \
    --property=SubState --value)
  test "$youtube_active" = inactive
  test "$youtube_sub" = dead
  printf "cratedigger-youtube-ingest.service active=%s sub=%s\n" \
    "$youtube_active" "$youtube_sub"')
printf '%s\n' "$controlled_service_states"
CONTROLLED_ID=$(
  "$CYCLE_VERIFY" capture-target "$CONTROLLED_CURSOR" "$CRATEDIGGER_SOURCE"
)
"$CYCLE_VERIFY" verify-exact "$CONTROLLED_ID" "$CRATEDIGGER_SOURCE"

# Only the main timer opens. Capture its first ordinary successor before
# releasing the watchdog/unfindable timers and metadata gate.
ORDINARY_CURSOR=$("$CYCLE_VERIFY" capture-cursor)
env -u SSH_AUTH_SOCK ssh doc2 'sudo python3 - open-main-timer' < "$DEPLOY_HOLD"
ORDINARY_ID=$(
  "$CYCLE_VERIFY" capture-target "$ORDINARY_CURSOR" "$CRATEDIGGER_SOURCE"
)
env -u SSH_AUTH_SOCK ssh doc2 sudo python3 - finish-release "$ORDINARY_ID" < "$DEPLOY_HOLD"
worker_release_states=$(env -u SSH_AUTH_SOCK ssh doc2 'set -euo pipefail
  for attempt in $(seq 1 60); do
    all_active=1
    states=""
    for unit in cratedigger-web.service cratedigger-importer.service \
      cratedigger-import-preview-worker.service \
      cratedigger-youtube-ingest.service; do
      state=$(systemctl is-active "$unit" || true)
      states="${states}${unit}=${state}\n"
      if test "$state" != active; then
        all_active=0
      fi
    done
    if test "$all_active" = 1; then
      printf "%b" "$states"
      exit 0
    fi
    sleep 1
  done
  for unit in cratedigger-web.service cratedigger-importer.service \
    cratedigger-import-preview-worker.service \
    cratedigger-youtube-ingest.service; do
    systemctl show "$unit" \
      --property=ActiveState --property=SubState --property=Result
  done
  exit 1')
printf '%s\n' "$worker_release_states"
"$CYCLE_VERIFY" verify-exact "$ORDINARY_ID" "$CRATEDIGGER_SOURCE"
env -u SSH_AUTH_SOCK ssh doc2 sudo python3 - complete "$ORDINARY_ID" < "$DEPLOY_HOLD"
```

`finish-release` removes the owned YouTube inhibitor immediately before the
final metadata-gate resume, then restores watchdog/unfindable timers. Every
helper phase fails closed on an unexpected phase, stale downstream
controlled-start contract, dirty old lifecycle, pre-existing unowned
hold/link/inhibitor, changed owned object, surviving job, or wrong invocation
ID. On failure, leave the receipt and remaining masks/inhibitors in place and
inspect the exact reported boundary. Rerun an interrupted `acquire` directly
-- it resumes from whatever it already owns, including a bounded queue-drain
wait interrupted mid-poll. After a failed release phase, return safely to the
strict boundary with
`env -u SSH_AUTH_SOCK ssh doc2 'sudo python3 - recover-held' < "$DEPLOY_HOLD"` before restarting
release. Rerun an interrupted `complete` to finish its atomic retired-receipt
cleanup. Do not remove `/run/cratedigger-deploy-hold` or its
`system.control` links by hand; they are the recovery ownership record.

If `acquire` cannot or should not reach HELD -- an anomaly preflight field
(`recovery_required_jobs`/`malformed_enqueued_at_rows`, nothing drains them),
a stale controlled-start contract, or a SIGINT/dropped SSH that left the
receipt stranded partway through while the host stayed up -- `recover-held`
cannot help: it re-proves the identical, unfixable preconditions. Use
`env -u SSH_AUTH_SOCK ssh doc2 'sudo python3 - abort' < "$DEPLOY_HOLD"`
instead: it releases every object the receipt owns (gate hold, start
inhibitors, timer masks) before restarting what that ownership implies it
stopped, proving each restart before disowning the object it unblocked --
except `cratedigger.service`, a `Type=oneshot` that never reaches
active/running and so is always restarted unproven -- then removes the
receipt, returning to ordinary, unheld operation. It is safe from every
known receipt phase and never touches an object it did not own; it is the
one command in this module you run to walk away from a hold rather than
advance or re-prove it.

`abort` also survives a host reboot (#1096). The receipt under `/run` does
not survive one, but the manual gate hold and the producer start inhibitors
under `/var/lib/cratedigger-metadata-gate` are real disk state and can
outlive it, each carrying its own persistent sibling ownership marker. Run
`abort` with no receipt present exactly as above -- it adopts exactly the
objects its persistent markers own, removing/releasing every one of them
before restarting and proving active whatever they blocked (again excepting
`cratedigger.service`, restarted unproven), then clears the markers, ending
at ordinary operation. With no receipt and no persistent marker at all (an
ordinary clean boot), `abort` still refuses. `recover-held` still requires a
receipt -- the reboot recovery path is always `abort` followed by a fresh
`acquire`. See
`docs/solutions/deployment/authoritative-systemd-deploy-holds.md`.

## Database migrations

Schema is managed by versioned files in `migrations/NNN_name.sql`. The `cratedigger-db-migrate.service` oneshot unit runs the migrator (`scripts/migrate_db.py`) on every switch (fleet-update or break-glass rebuild) because `restartIfChanged = true` and `stopIfChanged = false` (#1161 — the latter routes it to switch-to-configuration's restart list, so a concurrent `systemctl start` can no longer replace its queued stop job and silently skip the run). `cratedigger-web.service` (and the other long-running workers) `requires` it, so a **failed** migration blocks them from starting — note `Requires=` on a `RemainAfterExit` oneshot is satisfied by the unit merely being active, so it cannot force a re-run and never protected against a migration that never ran. `cratedigger.service` and `cratedigger-unfindable.service` are timer-driven with `restartIfChanged = false`, so they only `wants`+`after` it (a `requires` edge would let the migrate unit's every-deploy restart SIGTERM a mid-flight cycle) and instead gate on schema currency themselves at startup (`lib/migrator.py::assert_schema_current`).

To add a schema change:
1. Create the next-numbered file: `migrations/NNN_describe_change.sql`
2. Write the change as plain SQL — no `IF NOT EXISTS` guards needed (each file runs exactly once per DB).
3. Test locally: `nix-shell --run "python3 -m unittest tests.test_migrator -v"`
4. Commit, push, deploy. The migrator picks it up automatically.

Before deploying a migration that maps, drops, renames, or constrains persisted
values, preflight the live vocabulary on doc2. Pull the live column schema
first, then run a `SELECT DISTINCT`/count query through `pipeline-cli query`
using SQL on stdin. Compare every non-NULL value with the migration's explicit
map and new CHECK domain, and record the result in the PR or issue. An
unexpected value is a stop condition: extend the reviewed migration map or
surface it for a decision; do not let the deploy discover it.

First inspect the schema:

```bash
env -u SSH_AUTH_SOCK ssh doc2 'export PGPASSWORD=$(sudo cat /run/secrets/cratedigger-pgpass \
  | grep "^PGPASSWORD=" | cut -d= -f2); pipeline-cli query -' <<'SQL'
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_name = '<table>'
ORDER BY ordinal_position;
SQL
```

Then inspect the persisted vocabulary in a separate invocation so both result
sets are rendered:

```bash
env -u SSH_AUTH_SOCK ssh doc2 'export PGPASSWORD=$(sudo cat /run/secrets/cratedigger-pgpass \
  | grep "^PGPASSWORD=" | cut -d= -f2); pipeline-cli query -' <<'SQL'
SELECT <persisted_column>, COUNT(*)
FROM <table>
GROUP BY <persisted_column>
ORDER BY <persisted_column> NULLS FIRST;
SQL
```

For destructive changes, backup first:
```bash
env -u SSH_AUTH_SOCK ssh doc2 'pg_dump -h 10.20.0.11 -U cratedigger cratedigger' > /tmp/cratedigger_backup_$(date +%Y%m%d_%H%M%S).sql
```

To run the migrator manually (e.g. after editing `migrations/` and pulling the flake on doc2 without a full rebuild):
```bash
env -u SSH_AUTH_SOCK ssh doc2 'sudo systemctl restart cratedigger-db-migrate.service'
env -u SSH_AUTH_SOCK ssh doc2 'sudo journalctl -u cratedigger-db-migrate.service -n 30'
```

## IMPORTANT
- `restartIfChanged = false` on `cratedigger.service` — deploys don't restart cratedigger itself. The back-to-back timer picks up new code on the next cycle.
- `restartIfChanged = true` plus `stopIfChanged = false` on `cratedigger-db-migrate.service` — deploys DO re-run the migrator, via a restart job a concurrent start cannot swallow (#1161). Fast no-op if nothing changed.
- To force a run: `env -u SSH_AUTH_SOCK ssh doc2 'sudo systemctl start cratedigger --no-block'` (don't block — it's a oneshot)
- Flake updates MUST happen on doc1 (has the Forgejo token at `/run/secrets/forgejo/nixbot-token` and the signing key). NEVER from doc2 or Windows.
