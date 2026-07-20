---
paths:
  - "lib/pipeline_db.py"
  - "scripts/pipeline_cli/**/*.py"
  - "lib/migrator.py"
  - "scripts/migrate_db.py"
  - "migrations/**/*.sql"
---

# Pipeline DB Rules (PostgreSQL)

- Connection: `postgresql://cratedigger@10.20.0.11:5432/cratedigger`
- **MUST use `autocommit=True`** in `PipelineDB` — prevents idle-in-transaction deadlocks
- Active statuses: `wanted`, `downloading`, `imported`, `unsearchable`; terminal
  audit status: `replaced`. `unsearchable` is the reversible operator-owned
  search stop, not a source-cleanup state. `replaced` rows are frozen and have
  no outgoing ordinary lifecycle transition; only `supersede_request_mbid` may
  create that status.
- JSONB columns: use for structured audit data (`import_result`, `validation_result`)

## Schema migrations are versioned files, NOT runtime DDL

- Schema lives in `migrations/NNN_name.sql`. Files are applied in version order by `lib/migrator.py` and tracked in the `schema_migrations` table.
- The deploy systemd unit `cratedigger-db-migrate.service` runs the migrator on every `nixos-rebuild switch` (`restartIfChanged = true`). `cratedigger-web.service` (and the other long-running workers) `requires` it, so they cannot start against an un-migrated DB. `cratedigger.service` and `cratedigger-unfindable.service` use `wants`+`after` instead — both are timer-driven, `restartIfChanged = false`, and a `requires` edge would let the migrate unit's every-deploy restart SIGTERM a mid-flight cycle — and gate on schema currency themselves at startup (`lib/migrator.py::assert_schema_current`).
- `PipelineDB.__init__` does NOT run DDL. There is no `run_migrations` kwarg, no `init_schema()` method. Construct it against an already-migrated DB.
- Tests get the schema applied once at session start in `tests/conftest.py` via `apply_migrations(TEST_DSN)`. Test setup helpers just `TRUNCATE` between tests.

## Adding a schema change

1. Create the next-numbered file: `migrations/NNN_describe_change.sql` (e.g. `002_add_user_score.sql`).
2. Write the change as plain SQL. Each file runs in its own transaction. **Do not** wrap statements in `IF NOT EXISTS` / `EXCEPTION WHEN duplicate_column` guards — versioned migrations only run once per DB, so guards just hide bugs.
3. The file is the contract. Once shipped, never edit it. To fix a mistake, add a new migration.
4. Run `nix-shell --run "python3 -m unittest tests.test_migrator -v"` to confirm the file parses and applies cleanly against the ephemeral PG.
5. Backup before deploying anything destructive: `ssh doc2 'pg_dump -h 10.20.0.11 -U cratedigger cratedigger' > /tmp/cratedigger_backup_$(date +%Y%m%d_%H%M%S).sql`

## What NOT to do

- Don't add DDL inside `PipelineDB` methods or anywhere outside `migrations/`. The migrator is the only path.
- Don't edit `migrations/001_initial.sql` (or any other already-shipped migration). It is frozen history.
- Don't create a `PipelineDB` instance from a script that expects to bootstrap schema. The script must run after the migration unit, or call `apply_migrations()` itself.

## Typed row projections (issue #765 phase 6)

Reads and writes have different canonical typings, both column-audited:

- **Write payloads** are flat `msgspec.Struct`s whose field names ARE column
  names (`AddRequestInput`, `PersistedYoutubeRow`, … — the #546 W3 pattern),
  guarded by the subset check in `tests/test_pipeline_db_column_contract.py`.
- **Read projections** are `TypedDict`s in `lib/pipeline_db/rows.py`
  (`AlbumRequestRow` is the exemplar): rows stay plain dicts at runtime so
  the whole `row["field"]` consumer surface keeps working, while pyright
  gains per-key types. A `SELECT *` row type must match the table's columns
  EXACTLY (equality check in the same contract test), so a new migration
  column fails the suite until the row type — and the `make_request_row`
  builder — learn it in the same PR.
- The ONE adapter from cursor to row type is `album_request_row()` (per-row
  `msgspec.convert`), which validates every declared key/type at runtime —
  column-type drift raises `msgspec.ValidationError` at the boundary.
- Consumer functions that merely read a row take `Mapping[str, Any]` (a
  TypedDict is assignable to it; a plain legacy dict too). Retyping those
  consumers to the concrete row type is incremental follow-up work per
  module family, not a prerequisite.
- Deploy-window semantics: `msgspec.convert` IGNORES extra keys, so a
  migration ADDING a column never breaks the still-running old-code cycle.
  A column DROP/RENAME is effectively two-phase (old code's row type would
  raise "missing required field" mid-window) — time such migrations like
  any other destructive change.
