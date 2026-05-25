# Search-Plan Iteration 2 — Deploy Runbook

Operator runbook for the search-plan iteration 2 PR series (issue
[#369](https://github.com/abl030/cratedigger/issues/369)). One section
per PR. PR1 has a non-trivial deploy procedure (controlled window for a
transient agent-run one-shot); the others are standard flake bumps and
migration runs.

Origin: `docs/brainstorms/2026-05-25-search-plan-iteration-2-requirements.md`.
Plan: `docs/plans/2026-05-25-001-feat-search-plan-iteration-2-plan.md`.

**Note on backfills.** The single-operator invariant says backfills are
operator/agent-driven one-shots, not committed product code. PR1's
schema migrations include 033, which seeds the data work that's
expressible in pure SQL (the MB-canonical-MBID VA rows + the
1-track-structural categorisation). The network-dependent data work
(release_group_year / release_group_id / track_artist /
catalog_number for the cohort of NULL rows that need MB or Discogs
lookups) is run by the agent during the deploy window as a transient
heredoc — not as a committed script.

---

## PR1 — Foundations (controlled backfill window)

PR1 lands schema migrations 027–033, the dual-source field resolver
service, and an inline enqueue resolution path. Migration 033 seeds
the pure-SQL data work; the agent runs the network-dependent half.

Estimated total window: **15–25 minutes** wall clock, dominated by the
network-resolution loop (~10–15 min against ~830 wanted requests × MB
or Discogs round-trip).

### 1. Pre-deploy

#### 1.1 Backup the pipeline DB

```bash
ssh doc2 'pg_dump -h 192.168.100.11 -U cratedigger cratedigger' \
  > /tmp/cratedigger_backup_pr1_$(date +%Y%m%d_%H%M%S).sql
```

Schema changes 027–033 are forward-only. Backup before pulling the
trigger.

#### 1.2 Capture baseline counts

For later verification:

```bash
ssh doc2 'sudo PGPASSWORD=$(sudo grep ^PGPASSWORD /run/secrets/cratedigger-pgpass | cut -d= -f2) pipeline-cli query --json "
SELECT
  COUNT(*) FILTER (WHERE status = '\''wanted'\'') AS wanted,
  COUNT(*) FILTER (WHERE status = '\''wanted'\'' AND artist_name IN ('\''Various Artists'\'', '\''Various'\'')) AS wanted_va_string,
  COUNT(*) FILTER (WHERE status = '\''wanted'\'' AND mb_artist_id = '\''89ad4ac3-39f7-470e-963a-56509c546377'\'') AS wanted_va_canonical,
  COUNT(*) FILTER (WHERE status = '\''wanted'\'' AND release_group_year IS NULL) AS wanted_rgy_null
FROM album_requests"'
```

Save the output. After PR1 the `is_va_compilation=TRUE` count should
match `wanted_va_string` (all string-matched rows reach the right
identity, either via 033's canonical-MBID seed or the agent's
post-deploy MB re-resolution). The `release_group_year` NULL count
should drop substantially.

### 2. Deploy schema (all 7 migrations land automatically via cratedigger-db-migrate)

```bash
# 1. On the dev machine: push code that landed PR1
git push origin main

# 2. On doc1: bump cratedigger-src flake input
ssh doc1 'cd ~/nixosconfig && nix flake update cratedigger-src \
  && git add flake.lock \
  && git commit -m "cratedigger: PR1 — iteration 2 foundations" \
  && git push'

# 3. On doc2: rebuild — runs cratedigger-db-migrate (migrations 027-033
#    apply) and restarts cratedigger-web + cratedigger-importer.
ssh doc2 'sudo nixos-rebuild switch --flake github:abl030/nixosconfig#doc2 --refresh'
```

Verify migrations landed:

```bash
ssh doc2 'sudo PGPASSWORD=$(sudo grep ^PGPASSWORD /run/secrets/cratedigger-pgpass | cut -d= -f2) pipeline-cli query --json "
SELECT version FROM schema_migrations WHERE version >= 27 ORDER BY version"'
```

Expect 7 rows (27, 28, 29, 30, 31, 32, 33). Migration 033 has already
seeded `is_va_compilation=TRUE` for the 7 rows whose `mb_artist_id`
matches the canonical VA MBID, and `unfindable_category='one_track_structural'`
for every 1-track request with a NULL category.

### 3. Controlled window for network-dependent backfill

The remaining data work — `release_group_year`, `release_group_id`,
`track_artist`, `catalog_number` for rows still missing them, plus
the VA-string-matched rows whose `mb_artist_id` wasn't canonical
(needs MB re-resolution to confirm and flip the flag) — requires
HTTP calls to the MB and Discogs mirrors. The agent (Claude Code, or
equivalent) runs this as a transient one-shot during a window when
all three DB-mutating services are stopped.

#### 3.1 Stop all three DB-mutating services

```bash
ssh doc2 'sudo systemctl stop cratedigger.service \
                            cratedigger-importer.service \
                            cratedigger-web.service'
```

Web returns 503 until restart. Confirm all inactive:

```bash
ssh doc2 'sudo systemctl is-active cratedigger.service \
                                   cratedigger-importer.service \
                                   cratedigger-web.service'
```

#### 3.2 Agent runs the transient one-shot

The exact shape, for reproducibility — the agent generates this from
the canonical resolver service's surface; it doesn't live in
`scripts/` (per the single-operator invariant):

```python
# Run on doc2 inside the cratedigger nix env so PYTHONPATH +
# psycopg2 + msgspec + web.mb / web.discogs all resolve correctly.
# The agent generates this from the resolver service's surface at
# deploy time — function names below should be verified against the
# current lib/field_resolver_service.py and lib/pipeline_db.py:
#
# ssh doc2 'sudo -u cratedigger /nix/var/cratedigger/bin/python3 -' <<'PY'
import os
from lib.pipeline_db import PipelineDB
from lib.field_resolver_service import apply_resolve_all_result, resolve_all

dsn = os.environ["PIPELINE_DB_DSN"]
db = PipelineDB(dsn)

# Walk every wanted request via raw SQL — the side-table writes
# coming out of resolve_all are the durable record.
cur = db._execute(
    "SELECT id, mb_release_id, mb_release_group_id, mb_artist_id, "
    "discogs_release_id, artist_name, year, source "
    "FROM album_requests WHERE status = 'wanted' ORDER BY id"
)
rows = [dict(r) for r in cur.fetchall()]
total = len(rows)
print(f"backfill: walking {total} wanted requests")

for i, row in enumerate(rows, start=1):
    try:
        result = resolve_all(row, db, budget_seconds=10.0)
        apply_resolve_all_result(
            db, int(row["id"]), result,
            existing_mb_release_group_id=row.get("mb_release_group_id"),
        )
    except Exception as exc:
        print(f"  request={row['id']} FAILED: {type(exc).__name__}: {exc}")
    if i % 50 == 0:
        print(f"  {i}/{total} processed")

print("backfill: done")
PY
```

The one-shot uses the same `apply_resolve_all_result` helper that the
web + CLI add paths use (`lib/field_resolver_service.py`), so the
per-row update shape can never drift between enqueue and backfill.

Total runtime is dominated by per-track MB/Discogs round-trips;
expect 10–15 minutes wall clock against ~830 requests. The script is
transient — the agent throws it away after the window closes.

If the script crashes midway, the agent inspects the partial state
(`SELECT field_name, status, COUNT(*) FROM album_request_field_resolutions
GROUP BY field_name, status`), narrows the WHERE clause to the
unresolved cohort, and re-runs. No retry-window machinery needed —
the agent owns the orchestration.

The 10-second `budget_seconds` is generous compared to the inline
enqueue budget (3s) because this isn't user-facing latency — it's
batch resolution where a slower mirror call is fine.

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
  COUNT(*) FILTER (WHERE status = '\''wanted'\'' AND unfindable_category = '\''one_track_structural'\'') AS one_track,
  COUNT(*) FILTER (WHERE status = '\''wanted'\'' AND catalog_number IS NOT NULL) AS with_catno
FROM album_requests"'
```

Expectations vs §1.2 baseline:
- `still_null_rgy` drops substantially. The remaining residual is the
  cohort where MB and Discogs both genuinely lack the year.
- `va_flagged` matches the §1.2 `wanted_va_string` count (033 covered
  the canonical-MBID rows; the agent's one-shot re-resolved the
  string-matched-but-not-canonical rows by re-fetching MB and applying
  `detect_va_compilation`).
- `one_track` equals the count of wanted requests with exactly one
  `album_tracks` row (033 covered this entirely).
- `with_catno` is non-zero (some rows have catalog numbers in MB or
  Discogs metadata).

#### 4.2 Side-table state

```bash
ssh doc2 'sudo PGPASSWORD=$(sudo grep ^PGPASSWORD /run/secrets/cratedigger-pgpass | cut -d= -f2) pipeline-cli query --json "
SELECT field_name, status, COUNT(*) AS n
FROM album_request_field_resolutions
GROUP BY field_name, status
ORDER BY field_name, status"'
```

Distribution should show `release_group_year` / `release_group_id` /
`catalog_number` / `track_artist` field_names with mostly `resolved`
rows. The `unresolved_*` rows account for upstream-data gaps (404s
for retired MB releases, Discogs masters with no `year`, etc.).

#### 4.3 Smoke-test inline enqueue

Add a known new request via the web UI and confirm it lands with
populated fields:

```bash
ssh doc2 'sudo PGPASSWORD=$(sudo grep ^PGPASSWORD /run/secrets/cratedigger-pgpass | cut -d= -f2) pipeline-cli query --json "
SELECT id, artist_name, album_title, release_group_year,
       is_va_compilation, catalog_number
FROM album_requests
WHERE status = '\''wanted'\''
ORDER BY created_at DESC
LIMIT 5"'
```

A new MB-sourced reissue request should land with `release_group_year`
populated (or NULL with a side-table `unresolved_*` row recording the
reason). VA compilations land with `is_va_compilation=TRUE`.

### 5. Rollback

Schema changes are forward-only. If a critical issue surfaces:

1. **Side-table data**: re-run the agent's one-shot — idempotent
   because the parent column is NULL until written and `apply_resolve_all_result`
   never overwrites a non-NULL value with NULL.
2. **Schema**: forward fixes only. Add a new migration that reverts or
   corrects whatever needs unwinding. Do not edit migrations 027–033;
   they are frozen history.
3. **Service health**: if `cratedigger-web` or `cratedigger-importer`
   fails to start after the deploy, check `journalctl -u <service>`
   and `journalctl -u cratedigger-db-migrate`. The migrate unit is
   `requires` upstream of both, so any migration failure blocks
   startup.

### 6. Known limitations

- The agent's one-shot walks ~830 requests × ~12 tracks each. If the
  MB or Discogs mirror is slow or unavailable mid-run, individual
  fields land NULL with `unresolved_mirror_unavailable` or
  `unresolved_timeout` in the side table. Re-running the one-shot
  picks them up.
- `is_va_compilation` is set once at enqueue (and by 033/the
  one-shot for existing rows) and not re-resolved by automated
  product code. Operator-driven re-resolution is its own decision.
- `release_group_year` / `release_group_id` / `catalog_number` /
  `track_artist` resolution for rows whose MB or Discogs records
  genuinely lack the data records `unresolved_field_missing_upstream`.
  These are permanent data gaps upstream; the side table preserves
  the audit trail.

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
