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

2. From the pushed Cratedigger checkout, invoke the checked Bash entrypoint
with the exact revision to pin. The entrypoint runs the complete nixosconfig
fetch → detached worktree → `cratedigger-src`-only lock update → SSH-signature
verification → token-header Forgejo push → exact remote-SHA verification →
cleanup lifecycle. It refuses to run anywhere except doc1 and never depends on
the caller's interactive/default shell:
```bash
set -euo pipefail
CRATEDIGGER_REPO=$(git rev-parse --show-toplevel)
CRATEDIGGER_REV=$(git rev-parse HEAD)
"$CRATEDIGGER_REPO/scripts/pin_nixosconfig.sh" \
  "$CRATEDIGGER_REV" "cratedigger: <description>"
```

The helper commits through a private pending ref, so the signed commit becomes
durably reachable in the commit transaction itself, then promotes that exact
commit to `refs/cratedigger-deploy/cratedigger-src` before pushing. Retry the
exact same invocation after any failure: it recovers
`refs/cratedigger-deploy/cratedigger-src-pending` first; if Forgejo master is
still at the pin's parent, it pushes the already-created commit; if Forgejo is
already at the pending revision, it reports success without creating another
commit; and if Forgejo advanced to an incompatible revision, it fails with the
exact pending, base, and remote SHAs. Never delete or rewrite either private
recovery ref by hand during a retry. A transient or inconclusive verification
result (for example, unavailable allowed-signers configuration) retains the
pending candidate; only a definitively bad or unsigned candidate is discarded
so a later invocation can create a valid signed pin.

The Forgejo token remains confined to the helper's fail-fast subshell
environment and must never appear in an argv value, command-line `-c`
assignment, remote URL, xtrace, or Git trace output.

3. Capture the current systemd invocation, deploy doc2 through the
forced-command locked-sibling trigger, then wait up to 30 minutes for a **new,
nonempty** `InvocationID`. Capturing before the trigger prevents an old green
unit result from being mistaken for this deployment. Poll that
same invocation to a terminal state. This also handles a same-revision retry:
the anchor may already equal the target, but a fresh invocation must still run.
Success means `ActiveState=inactive`, `SubState=dead`, and `Result=success` for
the new invocation; any failed, unexpected, replaced, or timed-out state is a
deploy failure:
```bash
set -euo pipefail
before_state=$(ssh doc2 'systemctl show nixos-upgrade.service \
  --property=InvocationID')
PREVIOUS_INVOCATION=$(sed -n 's/^InvocationID=//p' <<<"$before_state")
fleet-deploy doc2
deadline=$((SECONDS + 1800))
triggered_invocation=""
deploy_complete=0
while ((SECONDS < deadline)); do
  if ! upgrade_state=$(ssh doc2 'systemctl show nixos-upgrade.service \
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
    ssh doc2 'journalctl -u nixos-upgrade.service -n 100 --no-pager' || true
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
  ssh doc2 'journalctl -u nixos-upgrade.service -n 100 --no-pager' || true
  exit 1
fi
```

`fleet-deploy` is asynchronous. It starts doc2's verified
`nixos-upgrade.service`, which fetches Forgejo, verifies every new commit
against `hosts.nix`, builds from its root-owned clone, switches, and runs
`cratedigger-db-migrate.service`. Direct `ssh doc2 'sudo fleet-update'` is not
the normal path and bypasses the locked-sibling trigger boundary. Do not use
`nixos-rebuild switch --flake github:...`; GitHub is stale.

4. Verify the fleet trust anchor equals the exact signed Forgejo commit printed
in step 2. A green unit with a stale anchor is not a successful deployment:
```bash
set -euo pipefail
EXPECTED_NIXOSCONFIG_REV=<full signed SHA printed by step 2>
DEPLOYED_REV=$(ssh doc2 'sudo cat /var/lib/fleet-update/last-verified-rev')
test "$DEPLOYED_REV" = "$EXPECTED_NIXOSCONFIG_REV"
```

5. Verify migration state and the services affected by the change. The migrate
oneshot uses `RemainAfterExit`, so it must report `ActiveState=active`,
`SubState=exited`, and `Result=success`; verify long-running workers
individually rather than assuming a successful switch made them healthy:
```bash
set -euo pipefail
migration_state=$(ssh doc2 'systemctl show cratedigger-db-migrate.service \
  --property=ActiveState --property=SubState --property=Result'
)
migration_active=$(sed -n 's/^ActiveState=//p' <<<"$migration_state")
migration_sub=$(sed -n 's/^SubState=//p' <<<"$migration_state")
migration_result=$(sed -n 's/^Result=//p' <<<"$migration_state")
printf '%s\n' "$migration_state"
test "$migration_active" = active
test "$migration_sub" = exited
test "$migration_result" = success
migration_rows=$(ssh doc2 'set -euo pipefail; \
  export PGPASSWORD=$(sudo cat /run/secrets/cratedigger-pgpass \
    | grep "^PGPASSWORD=" | cut -d= -f2); \
  test -n "$PGPASSWORD"; pipeline-cli query "$(cat)"' <<'SQL'
SELECT version, name, applied_at
FROM schema_migrations
ORDER BY version DESC
LIMIT 5;
SQL
)
test -n "$migration_rows"
printf '%s\n' "$migration_rows"
service_states=$(ssh doc2 'set -euo pipefail
  for unit in cratedigger-web.service cratedigger-importer.service \
    cratedigger-import-preview-worker.service cratedigger-youtube-ingest.service; do
    state=$(systemctl is-active "$unit")
    test "$state" = active
    printf "%s=%s\n" "$unit" "$state"
  done')
printf '%s\n' "$service_states"
```

6. Derive the active wrapper from the service's `ExecStart`, then derive its
exact source store from the wrapper and verify the deployed change there
(choose a unique string in a production file). Do not glob every historical
store path: an old generation could produce a false positive. Inspect the
rendered unit/config when the NixOS module changed:
```bash
set -euo pipefail
CRATEDIGGER_BIN=$(ssh doc2 "systemctl show cratedigger.service \
  --property=ExecStart --value | grep -o '/nix/store/[^ ;]*/bin/cratedigger' \
  | head -1")
test -n "$CRATEDIGGER_BIN"
CRATEDIGGER_SOURCE=$(ssh doc2 "grep -o '/nix/store/[^ ]*-source/cratedigger.py' \
  '$CRATEDIGGER_BIN' | head -1 | sed 's#/cratedigger.py##'")
test -n "$CRATEDIGGER_SOURCE"
ssh doc2 "grep '<something unique>' '$CRATEDIGGER_SOURCE/<changed-file>.py'"
# For nix/module.nix changes:
ssh doc2 'systemctl cat cratedigger.service'
ssh doc2 'grep "<rendered setting>" /var/lib/cratedigger/config.ini'
```

7. After live verification of a non-trivial series, run the **post-ship
reflection** (`.claude/rules/deploy.md` § "Post-ship reflection") — mine your
own session context for the debt this work surfaced (deferred review findings,
things fixed twice, duplication the series introduced, audits that could catch
review findings for free), de-dupe against open issues, and file ONE covering
issue (pattern: #573, #590) or state that nothing clears the bar.

## Holding timer-driven work across a switch

A runtime systemd mask normally persists until reboot, including across a
NixOS switch. It is still imperative state, not evidence that the post-switch
system is safely held: re-apply it idempotently and verify both the timer and
its service after the switch before any one-shot or state rewrite begins.

For a hold on the two timer-driven Cratedigger jobs:

1. On doc2, runtime-mask and stop `cratedigger.timer` and
   `cratedigger-unfindable.timer`. Let any already-running oneshot finish; do
   not interrupt it to create the maintenance window.
2. Trigger the deploy and wait for the exact new `nixos-upgrade.service`
   invocation as described above.
3. Immediately runtime-mask both timers again as an idempotent safety step.
   Verify both are masked and both `cratedigger.service` and
   `cratedigger-unfindable.service` are inactive. If either service ran because
   the pre-switch hold was absent or ineffective, leave the timers masked and
   wait for that cycle to finish before touching shared state, then assess and
   record what it read or changed before continuing.
4. Run the one-shot only after those checks pass. When the maintenance work and
   its reconciliation checks are complete, unmask and start the timers.

Do not make a strict transition depend solely on imperative state, even though
the runtime mask is expected to survive the switch. When a one-shot must finish
before the new code's first cycle, either run a backwards-compatible one-shot
under the pre-switch mask and reconcile it before deploying, or deploy a
reviewed declarative timer hold in the target NixOS generation and restore the
timers in a later switch. If a new-code cycle starts before the strict one-shot,
stop and assess/recover its reads and mutations; waiting for it to finish does
not restore the precondition.

Apply the same idempotent post-switch hold verification to any long-running
worker held for a maintenance operation. Never treat a pre-switch runtime mask
alone as evidence that the post-switch system is still held.

## Database migrations

Schema is managed by versioned files in `migrations/NNN_name.sql`. The `cratedigger-db-migrate.service` oneshot unit runs the migrator (`scripts/migrate_db.py`) on every switch (fleet-update or break-glass rebuild) because `restartIfChanged = true`. `cratedigger-web.service` (and the other long-running workers) `requires` it, so a failed migration blocks them from starting. `cratedigger.service` and `cratedigger-unfindable.service` are timer-driven with `restartIfChanged = false`, so they only `wants`+`after` it (a `requires` edge would let the migrate unit's every-deploy restart SIGTERM a mid-flight cycle) and instead gate on schema currency themselves at startup (`lib/migrator.py::assert_schema_current`).

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
ssh doc2 'export PGPASSWORD=$(sudo cat /run/secrets/cratedigger-pgpass \
  | grep "^PGPASSWORD=" | cut -d= -f2); pipeline-cli query "$(cat)"' <<'SQL'
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_name = '<table>'
ORDER BY ordinal_position;
SQL
```

Then inspect the persisted vocabulary in a separate invocation so both result
sets are rendered:

```bash
ssh doc2 'export PGPASSWORD=$(sudo cat /run/secrets/cratedigger-pgpass \
  | grep "^PGPASSWORD=" | cut -d= -f2); pipeline-cli query "$(cat)"' <<'SQL'
SELECT <persisted_column>, COUNT(*)
FROM <table>
GROUP BY <persisted_column>
ORDER BY <persisted_column> NULLS FIRST;
SQL
```

For destructive changes, backup first:
```bash
ssh doc2 'pg_dump -h 10.20.0.11 -U cratedigger cratedigger' > /tmp/cratedigger_backup_$(date +%Y%m%d_%H%M%S).sql
```

To run the migrator manually (e.g. after editing `migrations/` and pulling the flake on doc2 without a full rebuild):
```bash
ssh doc2 'sudo systemctl restart cratedigger-db-migrate.service'
ssh doc2 'sudo journalctl -u cratedigger-db-migrate.service -n 30'
```

## IMPORTANT
- `restartIfChanged = false` on `cratedigger.service` — deploys don't restart cratedigger itself. The back-to-back timer picks up new code on the next cycle.
- `restartIfChanged = true` on `cratedigger-db-migrate.service` — deploys DO re-run the migrator. Fast no-op if nothing changed.
- To force a run: `ssh doc2 'sudo systemctl start cratedigger --no-block'` (don't block — it's a oneshot)
- Flake updates MUST happen on doc1 (has the Forgejo token at `/run/secrets/forgejo/nixbot-token` and the signing key). NEVER from doc2 or Windows.
