# Deploy to doc2

Push code, update flake input on doc1, rebuild doc2 (which auto-runs migrations), verify.

## Steps

1. Commit and push:
```bash
git add <files> && git commit -m "<message>" && git push
```

2. Update flake input on doc1 and push:
```bash
ssh doc1 'cd ~/nixosconfig && nix flake update soularr-src && git add flake.lock && git commit -m "soularr: <description>" && git push'
# NOTE: If already on doc1 (hostname = proxmox-vm), run the inner command directly without ssh
```

3. Rebuild doc2 (this also runs `soularr-db-migrate.service` automatically):
```bash
ssh doc2 'sudo nixos-rebuild switch --flake github:abl030/nixosconfig#doc2 --refresh'
```

4. Verify deployed code has the change:
```bash
ssh doc2 'grep "<something unique>" /nix/store/*/lib/quality.py 2>/dev/null | head -1'
```

5. Verify the migration unit succeeded (especially when `migrations/` changed):
```bash
ssh doc2 'sudo systemctl status soularr-db-migrate.service --no-pager | head -10'
ssh doc2 'pipeline-cli query "SELECT version, name, applied_at FROM schema_migrations ORDER BY version DESC LIMIT 5"'
```

## Database migrations

Schema is managed by versioned files in `migrations/NNN_name.sql`. The `soularr-db-migrate.service` oneshot unit runs the migrator (`scripts/migrate_db.py`) on every `nixos-rebuild switch` because `restartIfChanged = true`. `soularr.service` and `soularr-web.service` both `requires` it, so a failed migration blocks the app from starting.

To add a schema change:
1. Create the next-numbered file: `migrations/NNN_describe_change.sql`
2. Write the change as plain SQL — no `IF NOT EXISTS` guards needed (each file runs exactly once per DB).
3. Test locally: `nix-shell --run "python3 -m unittest tests.test_migrator -v"`
4. Commit, push, deploy. The migrator picks it up automatically.

For destructive changes, backup first:
```bash
ssh doc2 'pg_dump -h 192.168.100.11 -U soularr soularr' > /tmp/soularr_backup_$(date +%Y%m%d_%H%M%S).sql
```

To run the migrator manually (e.g. after editing `migrations/` and pulling the flake on doc2 without a full rebuild):
```bash
ssh doc2 'sudo systemctl restart soularr-db-migrate.service'
ssh doc2 'sudo journalctl -u soularr-db-migrate.service -n 30'
```

## IMPORTANT
- `restartIfChanged = false` on `soularr.service` — deploys don't restart soularr itself. The 5-min timer picks up new code on next cycle.
- `restartIfChanged = true` on `soularr-db-migrate.service` — deploys DO re-run the migrator. Fast no-op if nothing changed.
- To force a run: `ssh doc2 'sudo systemctl start soularr --no-block'` (don't block — it's a oneshot)
- Flake updates MUST happen on doc1 (has git push credentials). NEVER from doc2 or Windows.
