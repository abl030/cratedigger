# Search-Plan Iteration 2 — Deploy Runbook

Operator runbook for the search-plan iteration 2 PR series (issue
[#369](https://github.com/abl030/cratedigger/issues/369)). One section
per PR. PR1 has the only non-trivial deploy procedure — the others are
standard flake bumps and migration runs.

Origin: `docs/brainstorms/2026-05-25-search-plan-iteration-2-requirements.md`.
Plan: `docs/plans/2026-05-25-001-feat-search-plan-iteration-2-plan.md`.

---

## PR1 — Foundations (controlled backfill window)

PR1 lands schema migrations 027–031, the dual-source field resolver
service, an inline enqueue resolution path, and a backfill script that
populates every existing wanted request's new columns from MB and
Discogs.

The backfill runs **outside** `cratedigger-db-migrate.service` (per the
user's call-out resolution during brainstorm): the deploy unit's job
is schema only; data fill is operator-driven during a controlled
window with all DB-mutating services stopped. This decouples migrate
runtime from backfill runtime and gives the operator full visibility
into the data population.

Estimated total window: **15–25 minutes** wall clock, dominated by the
backfill loop (~10–15 min against ~830 wanted requests × ~12 tracks
each × MB or Discogs round-trip).

### 1. Pre-deploy

#### 1.1 Backup the pipeline DB

```bash
ssh doc2 'pg_dump -h 192.168.100.11 -U cratedigger cratedigger' \
  > /tmp/cratedigger_backup_pr1_$(date +%Y%m%d_%H%M%S).sql
```

The backfill is idempotent and retryable, but the schema changes are
forward-only. Backup before pulling the trigger.

#### 1.2 Capture baseline counts

For later verification:

```bash
ssh doc2 'sudo PGPASSWORD=$(sudo grep ^PGPASSWORD /run/secrets/cratedigger-pgpass | cut -d= -f2) pipeline-cli query --json "
SELECT
  COUNT(*) FILTER (WHERE status = '\''wanted'\'') AS wanted,
  COUNT(*) FILTER (WHERE status = '\''wanted'\'' AND artist_name IN ('\''Various Artists'\'', '\''Various'\'')) AS wanted_va,
  COUNT(*) FILTER (WHERE status = '\''wanted'\'' AND release_group_year IS NULL) AS wanted_rgy_null
FROM album_requests"'
```

Save the output. The VA count should match the post-backfill
`is_va_compilation=TRUE` count. The rgy_null count should drop
substantially after the backfill.

### 2. Deploy schema (migrations land automatically)

```bash
# 1. On the dev machine: push code that landed PR1
git push origin main

# 2. On doc1: bump cratedigger-src flake input
ssh doc1 'cd ~/nixosconfig && nix flake update cratedigger-src \
  && git add flake.lock \
  && git commit -m "cratedigger: PR1 — iteration 2 foundations" \
  && git push'

# 3. On doc2: rebuild — runs cratedigger-db-migrate (migrations 027-031
#    apply) and restarts cratedigger-web + cratedigger-importer.
ssh doc2 'sudo nixos-rebuild switch --flake github:abl030/nixosconfig#doc2 --refresh'
```

Verify migrations landed:

```bash
ssh doc2 'sudo PGPASSWORD=$(sudo grep ^PGPASSWORD /run/secrets/cratedigger-pgpass | cut -d= -f2) pipeline-cli query --json "
SELECT version FROM schema_migrations WHERE version >= 27 ORDER BY version"'
```

Expect 5 rows (27, 28, 29, 30, 31).

### 3. Controlled backfill window

#### 3.1 Stop all three DB-mutating services

Web returns 503 until restart. Stopping in this order prevents the
restart-on-fail loop from interfering with the backfill.

```bash
ssh doc2 'sudo systemctl stop cratedigger.service \
                            cratedigger-importer.service \
                            cratedigger-web.service'
```

Confirm all are inactive:

```bash
ssh doc2 'sudo systemctl is-active cratedigger.service \
                                   cratedigger-importer.service \
                                   cratedigger-web.service'
```

All three should report `inactive`.

#### 3.2 Take the backfill advisory lock + run the script

The script takes `pg_advisory_lock(0x4246494C)` (ASCII "BFIL") for its
duration. Belt-and-braces — if anything else managed to start writing
to `album_requests` mid-flight, the lock holder would observe the
conflict immediately.

```bash
# Run inside cratedigger's nix env on doc2 — same PYTHONPATH /
# psycopg2 / msgspec as production.
ssh doc2 'sudo -u cratedigger PGPASSWORD=$(sudo grep ^PGPASSWORD /run/secrets/cratedigger-pgpass | cut -d= -f2) \
  /run/current-system/sw/bin/python3 /nix/var/cratedigger/scripts/backfill_field_resolutions.py --field=all'
```

The script prints progress every 100 rows: `N / total processed,
R resolved, U unresolved`. Total runtime is dominated by per-track
MB/Discogs round-trips; expect 10–15 minutes.

A failure midway is non-fatal: the script is idempotent. Side-table
rows record the resolution attempt; re-running picks up where the
window left off (skips already-resolved fields per the retry-policy
windows in `lib/field_resolver_service.py`).

#### 3.3 Restart services in reverse dependency order

```bash
ssh doc2 'sudo systemctl start cratedigger-web.service \
                              cratedigger-importer.service \
                              cratedigger.service'
```

Confirm:

```bash
ssh doc2 'sudo systemctl is-active cratedigger-web.service \
                                   cratedigger-importer.service \
                                   cratedigger.service'
```

All three should report `active` (with `cratedigger.service` showing
`inactive` between 5-min timer fires — that's normal, the timer is the
heartbeat).

### 4. Verify

#### 4.1 Backfill coverage

```bash
ssh doc2 'sudo PGPASSWORD=$(sudo grep ^PGPASSWORD /run/secrets/cratedigger-pgpass | cut -d= -f2) pipeline-cli query --json "
SELECT
  COUNT(*) FILTER (WHERE status = '\''wanted'\'' AND release_group_year IS NULL) AS still_null_rgy,
  COUNT(*) FILTER (WHERE status = '\''wanted'\'' AND is_va_compilation = TRUE) AS va_flagged,
  COUNT(*) FILTER (WHERE status = '\''wanted'\'' AND unfindable_category = '\''one_track_structural'\'') AS one_track
FROM album_requests"'
```

Expectations (relative to the §1.2 baseline):

- `still_null_rgy`: should drop substantially — the remaining residual
  is the cohort that legitimately can't be resolved (broken Discogs
  master entries, MB releases without a release-group, etc.).
- `va_flagged`: should match the §1.2 `wanted_va` count (the 25 known
  rows credited to "Various Artists" by MBID identity).
- `one_track`: equals the count of wanted requests with exactly 1 row
  in `album_tracks`.

#### 4.2 Side-table state

```bash
ssh doc2 'sudo PGPASSWORD=$(sudo grep ^PGPASSWORD /run/secrets/cratedigger-pgpass | cut -d= -f2) pipeline-cli query --json "
SELECT field_name, status, COUNT(*) AS n
FROM album_request_field_resolutions
GROUP BY field_name, status
ORDER BY field_name, status"'
```

Distribution should show:

- `release_group_year` / `release_group_id` / `catalog_number` /
  `track_artist` field_names present.
- Mostly `resolved` rows, with `unresolved_*` rows accounting for the
  upstream-data gaps (404s for retired MB releases, Discogs masters
  with no `year`, etc.).
- No `unresolved_malformed` rows in the wanted cohort (those would
  indicate stored IDs that don't parse — investigate if present).

#### 4.3 Smoke-test inline enqueue

Add a known new request via the web UI and confirm it lands with
populated fields:

```bash
# After adding a request via the web, query its row:
ssh doc2 'sudo PGPASSWORD=$(sudo grep ^PGPASSWORD /run/secrets/cratedigger-pgpass | cut -d= -f2) pipeline-cli query --json "
SELECT id, artist_name, album_title, release_group_year, is_va_compilation
FROM album_requests
WHERE status = '\''wanted'\''
ORDER BY created_at DESC
LIMIT 5"'
```

A new MB-sourced reissue request should land with `release_group_year`
populated (or NULL with a side-table `unresolved_*` row recording the
reason — both are valid post-resolver states).

### 5. Rollback

Schema changes are forward-only — they cannot be unwound by re-deploy.
If a critical issue surfaces:

1. **Side-table data**: re-running the backfill is idempotent. If the
   data looks wrong, debug via the side-table state queries above, fix
   the resolver, ship a patch PR.
2. **Schema**: only forward fixes. Add a new migration that reverts or
   corrects whatever needs unwinding. Do not edit migrations 027–031;
   they are frozen history.
3. **Service health**: if `cratedigger-web` or `cratedigger-importer`
   fails to start after the deploy, check `journalctl -u <service>`
   and `journalctl -u cratedigger-db-migrate`. The migrate unit is
   `requires` upstream of both, so any migration failure blocks
   startup.

### 6. Known limitations

- The backfill walks ~830 requests × ~12 tracks each. If the MB or
  Discogs mirror is slow or unavailable mid-run, individual fields
  land NULL with `unresolved_mirror_unavailable` or
  `unresolved_timeout` in the side table. Re-running the backfill 24h
  later picks them up automatically (per the 1d retry window for
  transient failures).
- The `is_va_compilation` boolean is set once per row and never
  re-resolved by automated paths (R12 invariant). Operator-driven
  re-resolution lives in a follow-up plan.
- `release_group_year` and `release_group_id` resolution for Discogs
  master entries with a missing `year` field will record
  `unresolved_field_missing_upstream` (30d retry window) — these are
  permanent data gaps upstream, the retry is a courtesy in case the
  upstream data is later corrected.

---

## PR2 — Generator + matcher

Standard flake bump + rebuild. No backfill window. Bumps
`SEARCH_PLAN_GENERATOR_ID`; existing plans regenerate on the next
5-min cycle through the existing wave-capped reconciliation path.

(Procedure documented when PR2 lands.)

---

## PR3 — Detection + telemetry

Standard flake bump + rebuild. Adds a new `cratedigger-unfindable.service`
oneshot + `cratedigger-unfindable.timer` (daily). The timer runs the
first detection sweep ~24h after deploy.

(Procedure documented when PR3 lands.)

---

## PR4 — Operator surface

Standard flake bump + rebuild. No backfill. New `/api/triage/*` and
`/api/_index` endpoints come online; existing endpoints unchanged.

(Procedure documented when PR4 lands.)
