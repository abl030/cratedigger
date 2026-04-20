# Deployment Rules

- All code deploys via Nix flake: push cratedigger → `nix flake update cratedigger-src` on doc1 → `nixos-rebuild switch` doc2
- The NixOS module lives in this repo at `nix/module.nix` (exposed as `nixosModules.default`). The downstream wrapper at `~/nixosconfig/modules/nixos/services/cratedigger.nix` imports it via `inputs.cratedigger-src.nixosModules.default`.
- Flake updates MUST happen on doc1 (has git push credentials). NEVER from doc2.
- `restartIfChanged = false` on the cratedigger service — deploys don't restart it. The 5-min timer picks up new code on the next cycle. `cratedigger-web` and `cratedigger-db-migrate` use the systemd default and DO restart on switch.
- Always verify deployed code: `ssh doc2 'grep "<unique string>" /nix/store/*/lib/quality.py 2>/dev/null'`. For module changes: `ssh doc2 'cat /etc/systemd/system/cratedigger.service'` or check the rendered `/var/lib/cratedigger/config.ini`.
- Before deploying changes to `nix/module.nix`, run the VM check: `nix build .#checks.x86_64-linux.moduleVm`.
- Use the `/deploy` command for the full sequence.

## Database migrations

- Schema lives in `migrations/NNN_name.sql`. The deploy unit `cratedigger-db-migrate.service` (oneshot, `restartIfChanged = true`) runs them automatically on every `nixos-rebuild switch`, BEFORE `cratedigger.service` and `cratedigger-web.service` start. Both services `requires` the migrate unit, so a failed migration blocks the app from coming up against an inconsistent schema.
- To add a schema change: drop a new numbered SQL file in `migrations/`. The next deploy applies it. No manual psql, no out-of-band steps. See `.claude/rules/pipeline-db.md` for the full workflow.
- Backup before any destructive migration: `ssh doc2 'pg_dump -h 192.168.100.11 -U cratedigger cratedigger' > /tmp/cratedigger_backup_$(date +%Y%m%d_%H%M%S).sql`
- After deploy, verify the migration ran: `ssh doc2 'pipeline-cli query "SELECT * FROM schema_migrations ORDER BY version DESC LIMIT 5"'`
- If a migration fails, check `ssh doc2 'sudo journalctl -u cratedigger-db-migrate.service'` for the error.

## Post-Deploy Reflection
- After deploying non-trivial changes, spawn an Opus agent to assess: did we make the code better? Did we finish what we intended? Are there loose ends or untested paths?
- The agent should read the git log, the diff, and the relevant tests, then report findings to the user.
- This is the final quality gate — it catches "built but not wired" and "tested but not deployed" problems.
