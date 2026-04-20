---
paths:
  - "lib/pipeline_db.py"
  - "scripts/pipeline_cli.py"
  - "lib/migrator.py"
  - "scripts/migrate_db.py"
  - "migrations/**/*.sql"
---

# Pipeline DB Rules (PostgreSQL)

- Connection: `postgresql://cratedigger@192.168.100.11:5432/cratedigger`
- **MUST use `autocommit=True`** in `PipelineDB` — prevents idle-in-transaction deadlocks
- 4 statuses: wanted, downloading, imported, manual
- JSONB columns: use for structured audit data (`import_result`, `validation_result`)

## Schema migrations are versioned files, NOT runtime DDL

- Schema lives in `migrations/NNN_name.sql`. Files are applied in version order by `lib/migrator.py` and tracked in the `schema_migrations` table.
- The deploy systemd unit `cratedigger-db-migrate.service` runs the migrator on every `nixos-rebuild switch` (`restartIfChanged = true`). `cratedigger.service` and `cratedigger-web.service` both `requires` it, so they cannot start against an un-migrated DB.
- `PipelineDB.__init__` does NOT run DDL. There is no `run_migrations` kwarg, no `init_schema()` method. Construct it against an already-migrated DB.
- Tests get the schema applied once at session start in `tests/conftest.py` via `apply_migrations(TEST_DSN)`. Test setup helpers just `TRUNCATE` between tests.

## Adding a schema change

1. Create the next-numbered file: `migrations/NNN_describe_change.sql` (e.g. `002_add_user_score.sql`).
2. Write the change as plain SQL. Each file runs in its own transaction. **Do not** wrap statements in `IF NOT EXISTS` / `EXCEPTION WHEN duplicate_column` guards — versioned migrations only run once per DB, so guards just hide bugs.
3. The file is the contract. Once shipped, never edit it. To fix a mistake, add a new migration.
4. Run `nix-shell --run "python3 -m unittest tests.test_migrator -v"` to confirm the file parses and applies cleanly against the ephemeral PG.
5. Backup before deploying anything destructive: `ssh doc2 'pg_dump -h 192.168.100.11 -U cratedigger cratedigger' > /tmp/cratedigger_backup_$(date +%Y%m%d_%H%M%S).sql`

## What NOT to do

- Don't add DDL inside `PipelineDB` methods or anywhere outside `migrations/`. The migrator is the only path.
- Don't edit `migrations/001_initial.sql` (or any other already-shipped migration). It is frozen history.
- Don't create a `PipelineDB` instance from a script that expects to bootstrap schema. The script must run after the migration unit, or call `apply_migrations()` itself.
