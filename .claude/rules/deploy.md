# Deployment Rules

- Merged `main` is production tomorrow morning. doc1's nightly rolling flake
  update (`rolling-flake-update.service`, 23:00) pins nixosconfig's
  `cratedigger-src` to the branch tip and pushes a signed commit to
  **Forgejo** (`git.ablz.au`); doc2's `nixos-upgrade.timer` (04:00, up to an
  hour of jitter) pulls, verifies every commit's SSH signature against
  `hosts.nix`, builds from its root-owned clone, and switches, migrations
  included. The daily Cratedigger gate on doc1 (05:05) clones `main`'s tip
  into a fresh checkout and tests it against current nixpkgs unstable,
  paging on red: it tests the tip, not the pinned revision (only the gate's
  own runner script comes from the pin), and it never gates the switch,
  which with doc2's jitter can still be running when it starts. Leave
  `main` green when you merge; nothing re-gates it before it ships.
- Deploy by hand only when a change must be live now. The whole runbook is
  `scripts/deploy.sh`, run from the shared checkout on doc1: it pins
  `cratedigger-src` to `origin/main` (or a given SHA on it) with
  `nix flake update --override-input`, never a bare `nix flake update`, which
  only follows the branch tip (#1203); commits the one-file change, refuses
  to push it unless `git log --format=%G?` reports a verified SSH signature
  (doc2 would refuse it, and so would every other host's nightly update
  until master was fixed); pushes and reads back through nixosconfig's own
  `~/nixosconfig/scripts/forgejo-auth.sh`, so the token travels as a Git config header in
  that helper's sanitized environment and never in argv, a URL, or a trace;
  triggers `fleet-deploy doc2`; waits for that `nixos-upgrade.service`
  invocation to finish; and requires `/var/lib/fleet-update/last-verified-rev`
  to equal the commit it pushed. That last check is the one that caught the
  2026-06-11 incident where the unit went green after rebuilding the frozen
  GitHub revision. Everything else that used to be verified by hand (migrate
  invocation, service states, the deployed store path, one or two timer
  cycles) is not: check the change you shipped through the real CLI, API, or
  UI instead. The deploy skill is that one command plus that sentence.
- **Since the Forgejo cutover (2026-06-10), nixosconfig deploys come from Forgejo, NEVER `github:abl030/nixosconfig` — GitHub is a frozen, stale fallback.** The cratedigger repo itself still lives on GitHub; only the nixosconfig leg changed.
- The fleet trigger key is forced-command authority, never an operator SSH
  identity. `scripts/deploy.sh` unsets `SSH_AUTH_SOCK` for its whole run so
  the key it selects explicitly is never cached into the shared agent (and
  so a wedged forwarded agent cannot hang commit signing). Do not add
  `fleet-deploy` or deploy-side `ssh` calls outside the script. Direct
  `fleet-update` on doc2 is not the normal deployment path.
- **Never pipe a result-bearing command through any downstream pipe target
  inside an `&&` chain unless `pipefail` is explicitly active** —
  `tail`/`head`/`grep` are common examples. This covers gate commands (test
  suites, fuzz bursts), pushes, deploy triggers, and one-shots alike: the
  downstream command's success can mask the real exit status.
  Long-running commands redirect output to a file and echo `$?` explicitly,
  then the file is tailed separately, as a distinct step. Incident:
  `fuzz_burst.sh 2>&1 | tail -12` run in the background masked the script's
  exit code behind `tail`'s AND buffered all output until EOF, so the
  monitoring surface read "completed, exit 0, empty output" — triggering a
  redundant second burst mid-deploy. For pushes specifically: check each
  push's exit status directly, then verify the expected remote ref resolves
  to the pushed commit before any dependent action such as `gh pr merge`.
- The NixOS module lives in this repo at `nix/module.nix` (exposed as `nixosModules.default`). The downstream wrapper at `~/nixosconfig/modules/nixos/services/cratedigger.nix` imports it via `inputs.cratedigger-src.nixosModules.default`.
- nixosconfig changes and pins MUST happen on doc1 (has the Forgejo token +
  signing key). NEVER from doc2.
- `restartIfChanged = false` on the cratedigger service — deploys don't restart it. The timer (`OnUnitInactiveSec`, back-to-back cycles) picks up new code on the next cycle. `cratedigger-web` and `cratedigger-db-migrate` use the systemd default and DO restart on switch. `cratedigger-importer.service` DOES restart on switch (`restartIfChanged = true`) and, since issue #1089, drains gracefully rather than dying mid-import — `KillMode = "mixed"` bounds that drain to `TimeoutStopSec = "10min"` worst case before falling back to a cgroup-wide SIGKILL.
- To derive what doc2 is actually running, read the active wrapper from
  `systemctl show cratedigger.service --property=ExecStart --value` and the
  exact `*-source` and `--config` store paths inside it; never glob
  historical store generations, which can produce a false positive, and
  never read `/var/lib/cratedigger/config.ini`, which no longer exists
  (#1276).
- Before deploying changes to `nix/module.nix`, run the VM check: `nix build .#checks.x86_64-linux.moduleVm`.
- **Every `nix flake update nixpkgs` in cratedigger must re-run the real-beets drift gate** (`tests/test_harness_beets2_contract.py` inside the re-pinned shell, plus the full suite): the repository lock is Cratedigger's last verified standalone reference snapshot. `scripts/daily_flake_update.sh` updates only that node. `scripts/daily_beets_tip_update.sh` separately updates only the checks-only tip node under the same state lock; neither runner supplies the deployment-owned Beets runtime package.
- Deployment consumes the pushed revision's final pre-push confirmation. Do not
  replay those checks during deploy when the revision is unchanged.

## Post-ship reflection (after live verification, before ending the session)

The end of a shipped series is the only moment its debt is cheap to see — the session context still holds what reviews caught by hand, what got fixed twice, and what scar tissue the work itself introduced. Once the session ends, that knowledge is gone and the next reviewer pays for it again. So, after a non-trivial series has merged (skip for typo-level changes):

1. **Reflect in your own context**, mining: review findings that were deferred as non-blocking; anything you fixed more than once or in more than one place; duplication or boilerplate the series itself added; "would a structural audit have caught this for free?"; process failures worth encoding as rules.
2. **Rank by value-for-effort, then de-dupe against open issues** (`gh issue list` — read the bodies of the open refactor issues, not just titles). De-dupe is mandatory; a duplicate covering issue is worse than none.
3. **File ONE covering issue** with ranked items and a suggested PR grouping (house pattern: #573 after #550, #590 after #571/#576) — or state explicitly that nothing clears the bar. The reflection is mandatory; the issue is conditional on something actually clearing it.

## Database migrations

- Schema lives in `migrations/NNN_name.sql`. The deploy unit `cratedigger-db-migrate.service` (oneshot, `restartIfChanged = true`, `stopIfChanged = false`) runs them automatically on every `nixos-rebuild switch`, BEFORE `cratedigger-web.service`, `cratedigger-importer.service`, `cratedigger-import-preview-worker.service`, and `cratedigger-youtube-ingest.service` start — those four `requires` the migrate unit, so a **failed** migration blocks them from coming up against an inconsistent schema.
- **`stopIfChanged = false` is load-bearing, not cosmetic (#1161).** With the NixOS default the migrate unit lands in switch-to-configuration's stop list AND start list. Its stop job is ordered behind every `Requires=` dependent's stop (reverse `After=`), so while a slow worker drains — the importer's `KillMode=mixed` graceful drain routinely takes seconds — that stop job is still queued, and ANY concurrent `systemctl start` (job mode `replace`, which is the default) replaces it. `RemainAfterExit` keeps the unit `active (exited)` throughout, so the replacement start hits `unit_start()`'s `-EALREADY`: ExecStart never forks and **systemd logs nothing at all**. `stopIfChanged = false` moves the unit to the restart list, and systemd's job-merge table collapses `JOB_START` into `JOB_RESTART`, so the re-run cannot be swallowed. Do not "simplify" this back to the default.
- **The rule generalizes: every `RemainAfterExit` oneshot ordered `Before=` the
  workers needs `stopIfChanged = false` (#1172 item 4).** The shape, not the
  unit, is what makes the skip possible; `Requires=`/`Wants=` from a dependent
  only decides whether something is around to fire the concurrent start that
  exploits it. The wrapper's `cratedigger-secrets-split` carried the shape
  without the trigger and was fixed as fail-closed hygiene — a silent skip
  there leaves `/run/cratedigger-secrets` stale after a sops rotation. Apply it
  when writing such a unit, not after finding the second ingredient.
- **`Requires=` on a `RemainAfterExit` oneshot cannot force a re-run.** It is satisfied by the unit merely being active, so the four workers' `requires` edge protects against a migration that FAILED, never against one that never ran. Their only real protection is that the migration now cannot be skipped.
- Nothing in the ordinary deploy proves the migrate unit ran for a given
  switch; `stopIfChanged = false` is the protection, and `ActiveState=active`
  / `SubState=exited` / `Result=success` are satisfied by a run from days
  ago. If you ever need that proof, compare the unit's `InvocationID` against
  a value captured before the switch. `cratedigger.service` and
  `cratedigger-unfindable.service` deliberately do NOT `requires` the migrate
  unit (only `wants`+`after`): both are timer-driven with
  `restartIfChanged = false`, and the migrate unit's `ExecStart` store path
  changes on every deploy, so a `requires` edge would propagate its
  every-switch restart as a SIGTERM to a mid-flight cycle. Those two instead
  gate on schema currency themselves at startup
  (`lib/migrator.py::assert_schema_current`, called from
  `cratedigger.py::main()` / `scripts/run_unfindable_detection.py::main()`) —
  a behind/missing schema still aborts them before any work runs.
- To add a schema change: drop a new numbered SQL file in `migrations/`. The next deploy applies it. No manual psql, no out-of-band steps. See `.claude/rules/pipeline-db.md` for the full workflow.
- Deploys are unattended, so a migration must apply against a running world:
  the main cycle may be mid-flight (its `restartIfChanged = false`) while the
  four workers are stopped for the switch. Write constraints over live
  lifecycle rows so they hold on whatever the running pipeline can leave
  behind, or stage them (`NOT VALID` then `VALIDATE`). There is no quiesce
  hold; the strict-hold helper that once masked the timers and drained the
  queue around migration 066 was deleted in #1378 along with the ceremony it
  served.
- Backup before any destructive migration: `ssh doc2 'pg_dump -h 10.20.0.11 -U cratedigger cratedigger' > "$CLAUDE_JOB_DIR/tmp/cratedigger_backup_$(date +%Y%m%d_%H%M%S).sql"`
- After a deploy that carried a migration, confirm it applied with `pipeline-cli query` on doc2 (`SELECT version, name, applied_at FROM schema_migrations ORDER BY version DESC LIMIT 5;`), exporting `PGPASSWORD` from `/run/secrets/cratedigger-pgpass` on doc2 and passing the SQL through stdin; never print the password or pass it from another host.
- If a migration fails, check `ssh doc2 'sudo journalctl -u cratedigger-db-migrate.service'` for the error.
- **Migration 066 (processing ownership, #898) is a hard forward-only boundary.**
  Once applied, never repin cratedigger to a pre-#898 source — not even at zero
  `processing` rows. 066 installs the `processing` status, the
  `active_automation_import_job_id` owner equivalence CHECK, a partial unique
  index over active `automation_import` jobs, deferred constraint triggers, and
  the `processing_cleanup_journal` table; a repinned pre-#898 writer violates
  them at COMMIT rather than failing cleanly. Forward-fix only.
