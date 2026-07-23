# Deployment Rules

- All code deploys via Nix flake: push cratedigger (GitHub) → `nix flake update cratedigger-src` on doc1 → commit (SSH-signed) + push nixosconfig to **Forgejo** → from doc1 run `fleet-deploy doc2` through the locked-sibling forced-command boundary, then poll and verify the asynchronous update.
- **Since the Forgejo cutover (2026-06-10), nixosconfig deploys come from Forgejo (`git.ablz.au`), NEVER `github:abl030/nixosconfig` — GitHub is a frozen, stale fallback.** The cratedigger repo itself still lives on GitHub; only the nixosconfig leg changed.
- The Forgejo push needs a token header (gh's credential helper is github.com-only). Configure it through `GIT_CONFIG_COUNT`, `GIT_CONFIG_KEY_0`, and `GIT_CONFIG_VALUE_0` in the environment, never a `git -c` argv value or remote URL. Never echo the token. The exact example is in `.claude/skills/deploy/SKILL.md`.
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
  to the pushed commit before any dependent action such as `gh pr merge` or
  `fleet-deploy`.
- `fleet-deploy doc2` asynchronously triggers doc2's `nixos-upgrade.service`; the underlying verified update checks every commit in range against the SSH signing keys in `hosts.nix`, then builds from its root-owned clone at `/var/lib/fleet-update/repo`. Capture `InvocationID` before triggering, wait within a bounded timeout for a different nonempty invocation, poll that same invocation's keyed `ActiveState`/`SubState`/`Result` to inactive/dead/success, and require `/var/lib/fleet-update/last-verified-rev` to equal the signed Forgejo commit before declaring success. Direct `fleet-update` on doc2 is not the normal deployment path.
- The fleet trigger key is forced-command authority, never an operator SSH identity. Run `fleet-deploy` and every deploy-runbook `ssh` command through `env -u SSH_AUTH_SOCK`; the explicit trigger key still works, cannot be cached into the shared agent, and a previously cached trigger key cannot consume an operator command. `scripts/verify_cratedigger_cycle.sh` independently enforces `IdentityAgent=none` on every SSH call. This boundary prevents a verification read from silently retriggering `nixos-upgrade.service` (#837).
- The NixOS module lives in this repo at `nix/module.nix` (exposed as `nixosModules.default`). The downstream wrapper at `~/nixosconfig/modules/nixos/services/cratedigger.nix` imports it via `inputs.cratedigger-src.nixosModules.default`.
- Flake updates MUST happen on doc1 (has the Forgejo token + signing key). NEVER from doc2.
- `restartIfChanged = false` on the cratedigger service — deploys don't restart it. The timer (`OnUnitInactiveSec`, back-to-back cycles) picks up new code on the next cycle. `cratedigger-web` and `cratedigger-db-migrate` use the systemd default and DO restart on switch.
- Post-switch pipeline verification first derives and checks the exact active
  source store, then captures the tail cursor of `cratedigger.service`'s
  journal as a fresh baseline. It enumerates ordered systemd start records
  after that cursor and captures the first invocation whose invocation-scoped
  journal names that source, then requires that same
  invocation's application cycle-complete record and systemd successful
  deactivation/finished-job records. Journal ordering prevents a short-lived
  failed target from disappearing between state polls. This post-switch
  observation boundary is required even for same-source, same-revision
  retries; a pre-trigger
  invocation ID is audit evidence only because cycles can roll during the
  asynchronous build. A later timer invocation replacing the unit's current
  `InvocationID` is neither success nor failure evidence for the captured
  target; verify the target itself with `journalctl --invocation=<ID>`.
- Strict migration holds use `scripts/cratedigger_deploy_hold.py`, never
  `systemctl mask --runtime`: NixOS `/etc/systemd/system` units outrank the
  ordinary `/run/systemd/system` mask location, and timer masking does not
  cancel already-queued service starts. The helper owns exact
  `/run/systemd/system.control` timer links plus a root-only receipt, drains
  exact waiting/running jobs without ever masking a service, and releases in
  controlled-cycle then ordinary-successor stages. Invocation proof remains
  owned by `scripts/verify_cratedigger_cycle.sh`.
- Always derive the active cratedigger wrapper from `systemctl show cratedigger.service --property=ExecStart --value`, extract its exact `*-source` path from the wrapper, and verify the unique source string there; never glob historical store generations, which can produce a false positive. For module changes, inspect `systemctl cat cratedigger.service` and the rendered `/var/lib/cratedigger/config.ini`.
- Before deploying changes to `nix/module.nix`, run the VM check: `nix build .#checks.x86_64-linux.moduleVm`.
- **Every `nix flake update` in cratedigger must re-run the real-beets drift gate** (`tests/test_harness_beets2_contract.py` inside the re-pinned shell, plus the full suite): the repository lock is Cratedigger's last verified standalone unstable snapshot. Fleet deliberately replaces that input edge through `cratedigger-src.inputs.nixpkgs.follows = "nixpkgs"`; there, nixosconfig's unstable pin is authoritative. `scripts/daily_flake_update.sh` automates the standalone lock update and all drift gates; no package override is permitted.
- Before the first cratedigger branch push, the agent runs whole-repo threaded
  Pyright and the full suite once on the final committed tree. Do not replay
  those gates during deploy when the pushed revision is unchanged.
- Use the `/deploy` command for the full sequence.

## Post-ship reflection (after live verification, before ending the session)

The end of a shipped series is the only moment its debt is cheap to see — the session context still holds what reviews caught by hand, what got fixed twice, and what scar tissue the work itself introduced. Once the session ends, that knowledge is gone and the next reviewer pays for it again. So, after live verification of a non-trivial series (skip for typo-level deploys):

1. **Reflect in your own context**, mining: review findings that were deferred as non-blocking; anything you fixed more than once or in more than one place; duplication or boilerplate the series itself added; "would a structural audit have caught this for free?"; process failures worth encoding as rules.
2. **Rank by value-for-effort, then de-dupe against open issues** (`gh issue list` — read the bodies of the open refactor issues, not just titles). De-dupe is mandatory; a duplicate covering issue is worse than none.
3. **File ONE covering issue** with ranked items and a suggested PR grouping (house pattern: #573 after #550, #590 after #571/#576) — or state explicitly that nothing clears the bar. The reflection is mandatory; the issue is conditional on something actually clearing it.

## Database migrations

- Schema lives in `migrations/NNN_name.sql`. The deploy unit `cratedigger-db-migrate.service` (oneshot, `restartIfChanged = true`) runs them automatically on every `nixos-rebuild switch`, BEFORE `cratedigger-web.service`, `cratedigger-importer.service`, `cratedigger-import-preview-worker.service`, and `cratedigger-youtube-ingest.service` start — those four `requires` the migrate unit, so a failed migration blocks them from coming up against an inconsistent schema. `cratedigger.service` and `cratedigger-unfindable.service` deliberately do NOT `requires` it (only `wants`+`after`): both are timer-driven with `restartIfChanged = false`, and the migrate unit's `ExecStart` store path changes on every deploy, so a `requires` edge would propagate the migrate unit's every-switch restart as a SIGTERM to a mid-flight cycle. Those two instead gate on schema currency themselves at startup (`lib/migrator.py::assert_schema_current`, called from `cratedigger.py::main()` / `scripts/run_unfindable_detection.py::main()`) — a behind/missing schema still aborts them before any work runs.
- To add a schema change: drop a new numbered SQL file in `migrations/`. The next deploy applies it. No manual psql, no out-of-band steps. See `.claude/rules/pipeline-db.md` for the full workflow.
- Backup before any destructive migration: `env -u SSH_AUTH_SOCK ssh doc2 'pg_dump -h 10.20.0.11 -U cratedigger cratedigger' > /tmp/cratedigger_backup_$(date +%Y%m%d_%H%M%S).sql`
- After deploy, verify the migration ran with `pipeline-cli query` on doc2 after exporting `PGPASSWORD` from `/run/secrets/cratedigger-pgpass`, passing the SQL through stdin as shown in `.claude/skills/deploy/SKILL.md`; never print the password or pass it from another host.
- If a migration fails, check `env -u SSH_AUTH_SOCK ssh doc2 'sudo journalctl -u cratedigger-db-migrate.service'` for the error.
