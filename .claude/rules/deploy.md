# Deployment Rules

- All code deploys via Nix flake: push cratedigger (GitHub) → `nix flake update cratedigger-src` on doc1 → commit (SSH-signed) + push nixosconfig to **Forgejo** → `ssh doc2 'sudo fleet-update'`.
- **Since the Forgejo cutover (2026-06-10), nixosconfig deploys come from Forgejo (`git.ablz.au`), NEVER `github:abl030/nixosconfig` — GitHub is a frozen, stale fallback.** The cratedigger repo itself still lives on GitHub; only the nixosconfig leg changed.
- The Forgejo push needs a token header (gh's credential helper is github.com-only): `TOKEN=$(cat /run/secrets/forgejo/nixbot-token) && git -c "http.extraHeader=Authorization: token ${TOKEN}" push origin master`. Never echo the token.
- Check each push's exit status directly. Never pipe `git push` output through
  `tail` (or any other command) inside an `&&` chain unless `pipefail` is
  explicitly active: the downstream command's success can mask a failed push.
  After a successful push, verify the expected remote ref resolves to the
  pushed commit before any dependent action such as `gh pr merge` or
  `fleet-update`.
- `fleet-update` verifies every commit in range is SSH-signed by a key in hosts.nix, then builds from its own root-owned clone at `/var/lib/fleet-update/repo`. Break-glass only: `sudo fleet-update --dry-run` (fetch + verify + checkout) followed by `sudo nixos-rebuild switch --flake /var/lib/fleet-update/repo#doc2 --no-write-lock-file --option accept-flake-config true`.
- The NixOS module lives in this repo at `nix/module.nix` (exposed as `nixosModules.default`). The downstream wrapper at `~/nixosconfig/modules/nixos/services/cratedigger.nix` imports it via `inputs.cratedigger-src.nixosModules.default`.
- Flake updates MUST happen on doc1 (has the Forgejo token + signing key). NEVER from doc2.
- `restartIfChanged = false` on the cratedigger service — deploys don't restart it. The timer (`OnUnitInactiveSec`, back-to-back cycles) picks up new code on the next cycle. `cratedigger-web` and `cratedigger-db-migrate` use the systemd default and DO restart on switch.
- Always verify deployed code: `ssh doc2 'grep "<unique string>" /nix/store/*/lib/quality/pipeline.py 2>/dev/null'`. For module changes: `ssh doc2 'cat /etc/systemd/system/cratedigger.service'` or check the rendered `/var/lib/cratedigger/config.ini`.
- Before deploying changes to `nix/module.nix`, run the VM check: `nix build .#checks.x86_64-linux.moduleVm`.
- **Every `nix flake update` in cratedigger must re-run the real-beets drift gate** (`tests/test_harness_beets2_contract.py` inside the re-pinned shell, plus the full suite): since tier-2 packaging, the flake.lock — not the consumer's nixpkgs — decides the beets version production runs. A lock bump is a beets upgrade until proven otherwise.
- After live verification of a deploy-worthy state, tag it `vYYYY.MM.DD` (suffix `-N` for same-day). The pre-push hook (`scripts/pre-push`, runs the generated-test fuzz burst then `nix flake check`) gates every push; the tag records the verified state.
- Use the `/deploy` command for the full sequence.

## Post-ship reflection (after tag, before ending the session)

The end of a shipped series is the only moment its debt is cheap to see — the session context still holds what reviews caught by hand, what got fixed twice, and what scar tissue the work itself introduced. Once the session ends, that knowledge is gone and the next reviewer pays for it again. So, after tagging a non-trivial series (skip for typo-level deploys):

1. **Reflect in your own context**, mining: review findings that were deferred as non-blocking; anything you fixed more than once or in more than one place; duplication or boilerplate the series itself added; "would a structural audit have caught this for free?"; process failures worth encoding as rules.
2. **Rank by value-for-effort, then de-dupe against open issues** (`gh issue list` — read the bodies of the open refactor issues, not just titles). De-dupe is mandatory; a duplicate covering issue is worse than none.
3. **File ONE covering issue** with ranked items and a suggested PR grouping (house pattern: #573 after #550, #590 after #571/#576) — or state explicitly that nothing clears the bar. The reflection is mandatory; the issue is conditional on something actually clearing it.

## Database migrations

- Schema lives in `migrations/NNN_name.sql`. The deploy unit `cratedigger-db-migrate.service` (oneshot, `restartIfChanged = true`) runs them automatically on every `nixos-rebuild switch`, BEFORE `cratedigger-web.service`, `cratedigger-importer.service`, `cratedigger-import-preview-worker.service`, and `cratedigger-youtube-ingest.service` start — those four `requires` the migrate unit, so a failed migration blocks them from coming up against an inconsistent schema. `cratedigger.service` and `cratedigger-unfindable.service` deliberately do NOT `requires` it (only `wants`+`after`): both are timer-driven with `restartIfChanged = false`, and the migrate unit's `ExecStart` store path changes on every deploy, so a `requires` edge would propagate the migrate unit's every-switch restart as a SIGTERM to a mid-flight cycle. Those two instead gate on schema currency themselves at startup (`lib/migrator.py::assert_schema_current`, called from `cratedigger.py::main()` / `scripts/run_unfindable_detection.py::main()`) — a behind/missing schema still aborts them before any work runs.
- To add a schema change: drop a new numbered SQL file in `migrations/`. The next deploy applies it. No manual psql, no out-of-band steps. See `.claude/rules/pipeline-db.md` for the full workflow.
- Backup before any destructive migration: `ssh doc2 'pg_dump -h 10.20.0.11 -U cratedigger cratedigger' > /tmp/cratedigger_backup_$(date +%Y%m%d_%H%M%S).sql`
- After deploy, verify the migration ran: `ssh doc2 'pipeline-cli query "SELECT * FROM schema_migrations ORDER BY version DESC LIMIT 5"'`
- If a migration fails, check `ssh doc2 'sudo journalctl -u cratedigger-db-migrate.service'` for the error.
