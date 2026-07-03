# Deployment Rules

- All code deploys via Nix flake: push cratedigger (GitHub) → `nix flake update cratedigger-src` on doc1 → commit (SSH-signed) + push nixosconfig to **Forgejo** → `ssh doc2 'sudo fleet-update'`.
- **Since the Forgejo cutover (2026-06-10), nixosconfig deploys come from Forgejo (`git.ablz.au`), NEVER `github:abl030/nixosconfig` — GitHub is a frozen, stale fallback.** The cratedigger repo itself still lives on GitHub; only the nixosconfig leg changed.
- The Forgejo push needs a token header (gh's credential helper is github.com-only): `TOKEN=$(cat /run/secrets/forgejo/nixbot-token) && git -c "http.extraHeader=Authorization: token ${TOKEN}" push origin master`. Never echo the token.
- `fleet-update` verifies every commit in range is SSH-signed by a key in hosts.nix, then builds from its own root-owned clone at `/var/lib/fleet-update/repo`. Break-glass only: `sudo fleet-update --dry-run` (fetch + verify + checkout) followed by `sudo nixos-rebuild switch --flake /var/lib/fleet-update/repo#doc2 --no-write-lock-file --option accept-flake-config true`.
- The NixOS module lives in this repo at `nix/module.nix` (exposed as `nixosModules.default`). The downstream wrapper at `~/nixosconfig/modules/nixos/services/cratedigger.nix` imports it via `inputs.cratedigger-src.nixosModules.default`.
- Flake updates MUST happen on doc1 (has the Forgejo token + signing key). NEVER from doc2.
- `restartIfChanged = false` on the cratedigger service — deploys don't restart it. The 5-min timer picks up new code on the next cycle. `cratedigger-web` and `cratedigger-db-migrate` use the systemd default and DO restart on switch.
- Always verify deployed code: `ssh doc2 'grep "<unique string>" /nix/store/*/lib/quality.py 2>/dev/null'`. For module changes: `ssh doc2 'cat /etc/systemd/system/cratedigger.service'` or check the rendered `/var/lib/cratedigger/config.ini`.
- Before deploying changes to `nix/module.nix`, run the VM check: `nix build .#checks.x86_64-linux.moduleVm`.
- **Every `nix flake update` in cratedigger must re-run the real-beets drift gate** (`tests/test_harness_beets2_contract.py` inside the re-pinned shell, plus the full suite): since tier-2 packaging, the flake.lock — not the consumer's nixpkgs — decides the beets version production runs. A lock bump is a beets upgrade until proven otherwise.
- After live verification of a deploy-worthy state, tag it `vYYYY.MM.DD` (suffix `-N` for same-day). The pre-push hook (`scripts/pre-push`, runs `nix flake check`) gates every push; the tag records the verified state.
- Use the `/deploy` command for the full sequence.

## Database migrations

- Schema lives in `migrations/NNN_name.sql`. The deploy unit `cratedigger-db-migrate.service` (oneshot, `restartIfChanged = true`) runs them automatically on every `nixos-rebuild switch`, BEFORE `cratedigger.service` and `cratedigger-web.service` start. Both services `requires` the migrate unit, so a failed migration blocks the app from coming up against an inconsistent schema.
- To add a schema change: drop a new numbered SQL file in `migrations/`. The next deploy applies it. No manual psql, no out-of-band steps. See `.claude/rules/pipeline-db.md` for the full workflow.
- Backup before any destructive migration: `ssh doc2 'pg_dump -h 10.20.0.11 -U cratedigger cratedigger' > /tmp/cratedigger_backup_$(date +%Y%m%d_%H%M%S).sql`
- After deploy, verify the migration ran: `ssh doc2 'pipeline-cli query "SELECT * FROM schema_migrations ORDER BY version DESC LIMIT 5"'`
- If a migration fails, check `ssh doc2 'sudo journalctl -u cratedigger-db-migrate.service'` for the error.
