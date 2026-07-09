# Deploy to doc2

Push code, update flake input on doc1, push nixosconfig to Forgejo, run fleet-update on doc2 (which auto-runs migrations), verify.

**Since the Forgejo cutover (2026-06-10), nixosconfig deploys come from FORGEJO (`git.ablz.au`), never `github:abl030/nixosconfig` — GitHub is a frozen fallback.** The cratedigger repo itself still lives on GitHub; only the nixosconfig leg changed.

## Steps

1. Commit and push cratedigger (GitHub, unchanged):
```bash
git add <files> && git commit -m "<message>" && git push
```

2. Update flake input on doc1, commit (must be SSH-signed — `commit.gpgsign` is already on in ~/nixosconfig), and push to Forgejo. The push needs the Forgejo token as an auth header (the gh credential helper only covers github.com):
```bash
ssh doc1 'cd ~/nixosconfig && git pull && nix flake update cratedigger-src && git add flake.lock && git commit -m "cratedigger: <description>" && TOKEN=$(cat /run/secrets/forgejo/nixbot-token) && git -c "http.extraHeader=Authorization: token ${TOKEN}" push origin master'
# NOTE: If already on doc1 (hostname = proxmox-vm), run the inner command directly without ssh.
# NEVER echo the token. If ~/nixosconfig is dirty/on a feature branch, do the bump in a
# throwaway worktree off origin/master instead.
```

3. Deploy doc2 via the verified path (fetches Forgejo, verifies every commit is signed by a key in hosts.nix, builds from its own root-owned clone; also runs `cratedigger-db-migrate.service` automatically):
```bash
ssh doc2 'sudo fleet-update'
```
Do NOT use `nixos-rebuild switch --flake github:...` — GitHub is stale. Break-glass only: `ssh doc2 'sudo nixos-rebuild switch --flake /var/lib/fleet-update/repo#doc2 --no-write-lock-file --option accept-flake-config true'` after a `sudo fleet-update --dry-run` has fetched + verified the tip into the clone.

4. Verify deployed code has the change:
```bash
ssh doc2 'grep "<something unique>" /nix/store/*/lib/quality/*.py 2>/dev/null | head -1'
```

5. Verify the migration unit succeeded (especially when `migrations/` changed):
```bash
ssh doc2 'sudo systemctl status cratedigger-db-migrate.service --no-pager | head -10'
ssh doc2 'pipeline-cli query "SELECT version, name, applied_at FROM schema_migrations ORDER BY version DESC LIMIT 5"'
```

6. After live verification + tag of a non-trivial series: run the **post-ship reflection** (`.claude/rules/deploy.md` § "Post-ship reflection") — mine your own session context for the debt this work surfaced (deferred review findings, things fixed twice, duplication the series introduced, audits that could catch review findings for free), de-dupe against open issues, and file ONE covering issue (pattern: #573, #590) or state that nothing clears the bar.

## Database migrations

Schema is managed by versioned files in `migrations/NNN_name.sql`. The `cratedigger-db-migrate.service` oneshot unit runs the migrator (`scripts/migrate_db.py`) on every switch (fleet-update or break-glass rebuild) because `restartIfChanged = true`. `cratedigger-web.service` (and the other long-running workers) `requires` it, so a failed migration blocks them from starting. `cratedigger.service` and `cratedigger-unfindable.service` are timer-driven with `restartIfChanged = false`, so they only `wants`+`after` it (a `requires` edge would let the migrate unit's every-deploy restart SIGTERM a mid-flight cycle) and instead gate on schema currency themselves at startup (`lib/migrator.py::assert_schema_current`).

To add a schema change:
1. Create the next-numbered file: `migrations/NNN_describe_change.sql`
2. Write the change as plain SQL — no `IF NOT EXISTS` guards needed (each file runs exactly once per DB).
3. Test locally: `nix-shell --run "python3 -m unittest tests.test_migrator -v"`
4. Commit, push, deploy. The migrator picks it up automatically.

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
