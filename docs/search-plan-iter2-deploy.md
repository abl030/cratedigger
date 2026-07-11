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
ssh doc2 'pg_dump -h 10.20.0.11 -U cratedigger cratedigger' \
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
from web.mb import get_release_raw, get_release_group
from web.discogs import get_release as discogs_get_release

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
        # IMPORTANT: fetch upstream payloads and thread them through to
        # resolve_all. detect_va_compilation Rules 2 (Compilation rg +
        # divergent track credits) and 3 (split-artist joinphrase) both
        # require mb_release_payload. Old rows where mb_artist_id is
        # NULL (Rule 1 silenced) are silently mis-classified as
        # is_va_compilation=False if the payloads are omitted —
        # apply_resolve_all_result then writes that wrong verdict
        # unconditionally. The web add path
        # (web/routes/pipeline.py::post_pipeline_add) fetches via
        # get_release_raw and threads through; mirror that here.
        mb_payload = None
        rg_payload = None
        discogs_payload = None
        if row.get("mb_release_id"):
            mb_payload = get_release_raw(row["mb_release_id"], fresh=False)
        if row.get("mb_release_group_id"):
            try:
                rg_payload = get_release_group(row["mb_release_group_id"])
            except Exception:
                rg_payload = None
        if row.get("discogs_release_id") and not mb_payload:
            try:
                discogs_payload = discogs_get_release(
                    int(row["discogs_release_id"]), fresh=False)
            except Exception:
                discogs_payload = None
        result = resolve_all(
            row, db, budget_seconds=10.0,
            mb_release_payload=mb_payload,
            mb_release_group_payload=rg_payload,
            discogs_release_payload=discogs_payload,
        )
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
The payload-fetching pattern (`get_release_raw` + `get_release_group`
threaded through `resolve_all`) mirrors `post_pipeline_add` for the
same reason — without it, VA detection silently degrades to Rule 1
only and old rows with NULL `mb_artist_id` mis-classify (#378 burned
this on 2026-05-26).

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

## PR2 — Generator + matcher + #373 re-resolution (deploy window)

PR2 lands the generator/matcher rebuild (U6 stopword cleanup, U7
distinctiveness-ranked track fallback, U8 VA strategy mix + #373
detector tighten) and bumps `SEARCH_PLAN_GENERATOR_ID` from
`search-plan/2026-05-25-0` to `search-plan/2026-05-25-1`. PR2 is
code-only — **no schema migrations land**.

The new VA detector (Rule 2 tightened post-#373) requires per-track
artist credits to *diverge* from the album-level credit before
flipping `is_va_compilation=TRUE`. The PR1 backfill ran against the
looser pre-#373 detector and flagged ~73 wanted requests as VA; the
new detector says only ~25 of those are real VA (the other ~48 are
single-artist Compilation-typed releases — greatest-hits collections,
artist anthologies). Those 48 rows currently sit at
`is_va_compilation=TRUE` with NULL/non-diverging per-track artists,
and their post-deploy regeneration will run `_generate_va_plan` against
a degraded VA snapshot (no `va_track_artist_*` slots, just the
`no_track_artists_resolved` omission + the leftover year/catno slots)
— useless coverage.

The deploy-window heredoc below re-runs `detect_va_compilation`
against a freshly-fetched MB payload for every `is_va_compilation=TRUE`
wanted row, and flips the column back to FALSE for the rows the new
detector rejects. After the flip, the next 5-min cycle regenerates
their plans through `_generate_normal_plan` (default / literal /
literal_flac), restoring the full slot coverage.

Estimated total window: **5–10 minutes** wall clock for the
re-resolution loop (~73 rows × ~1 MB round-trip each, plus the
standard plan-regen wave on the first 1-2 cycles post-deploy).

### 1. Pre-deploy

#### 1.1 Backup the pipeline DB

```bash
ssh doc2 'pg_dump -h 10.20.0.11 -U cratedigger cratedigger' \
  > /tmp/cratedigger_backup_pr2_$(date +%Y%m%d_%H%M%S).sql
```

Even though PR2 is code-only, the heredoc below mutates rows
(`is_va_compilation` flips). Backup before the trigger.

#### 1.2 Capture baseline counts

```bash
ssh doc2 'sudo PGPASSWORD=$(sudo grep ^PGPASSWORD /run/secrets/cratedigger-pgpass | cut -d= -f2) pipeline-cli query --json "
SELECT
  COUNT(*) FILTER (WHERE is_va_compilation = TRUE) AS va_flagged,
  COUNT(*) FILTER (WHERE artist_name IN ('\''Various Artists'\'', '\''Various'\'')) AS string_va
FROM album_requests
WHERE status = '\''wanted'\''"'
```

Record the output. Pre-deploy expectation (as of 2026-05-25):
`va_flagged≈73`, `string_va≈25`. Post-heredoc expectation:
`va_flagged≈25` (only the genuine VA rows remain; the 48 single-artist
Compilation rows have flipped to FALSE).

### 2. Deploy code

```bash
# 1. On dev: push code that landed PR2
git push origin main

# 2. On doc1: bump cratedigger-src flake input
ssh doc1 'cd ~/nixosconfig && nix flake update cratedigger-src \
  && git add flake.lock \
  && git commit -m "cratedigger: PR2 — search-plan generator + matcher + #373 detector" \
  && git push'

# 3. On doc2: rebuild. cratedigger-db-migrate is a no-op (no new
#    migrations). cratedigger-web + cratedigger-importer restart.
#    cratedigger.service is restartIfChanged=false; the 5-min timer
#    picks up the new code.
ssh doc2 'sudo nixos-rebuild switch --flake github:abl030/nixosconfig#doc2 --refresh'
```

Verify the bumped `GENERATOR_ID` landed in the deployed code:

```bash
ssh doc2 'grep -n SEARCH_PLAN_GENERATOR_ID /nix/store/*/lib/search.py 2>/dev/null | head -3'
```

Expect `SEARCH_PLAN_GENERATOR_ID = "search-plan/2026-05-25-1"`.

### 3. Plan-regen wave (automatic, expect 5-15s extra wall time)

On the first 1-2 cycles after the deploy, startup reconciliation
notices every active plan's `generator_id` is stale and regenerates
them. The wave is bounded by the existing reconciliation cap (see
`lib/startup_reconciliation.py`); rows that exceed the per-cycle cap
land in subsequent cycles.

Monitor:

```bash
ssh doc2 'sudo journalctl -u cratedigger -f --since "1 min ago"'
```

Expect a one-time `regenerated N plans` log line on each of the first
1-2 cycles. After that, steady state.

### 4. Controlled window for VA re-resolution

The 48 wanted rows that flipped status under the looser pre-#373
detector now sit at `is_va_compilation=TRUE` but produce useless VA
plans (no diverging per-track artists → degraded VA snapshot).
Re-run `detect_va_compilation` against a fresh MB payload and flip
the column back to FALSE for the rows the new detector rejects.

#### 4.1 Stop all three DB-mutating services

```bash
ssh doc2 'sudo systemctl stop cratedigger.service \
                            cratedigger-importer.service \
                            cratedigger-web.service'
```

```bash
ssh doc2 'sudo systemctl is-active cratedigger.service \
                                   cratedigger-importer.service \
                                   cratedigger-web.service'
```

#### 4.2 Agent runs the transient VA re-resolution one-shot

Same model as PR1 §3.2: agent generates this from the canonical
detector surface at deploy time; it doesn't live in `scripts/` (per
the single-operator invariant). The example shape:

```python
# ssh doc2 'sudo -u cratedigger /nix/var/cratedigger/bin/python3 -' <<'PY'
import os
from lib.pipeline_db import PipelineDB
from lib.field_resolver_service import detect_va_compilation
from web import mb as mb_api

dsn = os.environ["PIPELINE_DB_DSN"]
db = PipelineDB(dsn)

# Walk every wanted row currently flagged VA. The tightened detector
# (post-#373) only flips True when per-track artist credits diverge
# from the album-level credit; the rows we're targeting flipped True
# under the looser pre-#373 rule and need re-evaluation.
cur = db._execute(
    "SELECT id, mb_release_id, mb_release_group_id, mb_artist_id, "
    "discogs_release_id, artist_name "
    "FROM album_requests "
    "WHERE status = 'wanted' AND is_va_compilation = TRUE "
    "  AND mb_release_id IS NOT NULL "
    "ORDER BY id"
)
rows = [dict(r) for r in cur.fetchall()]
total = len(rows)
flipped = 0
kept = 0
errors = 0
print(f"VA re-resolution: walking {total} rows")

for i, row in enumerate(rows, start=1):
    try:
        # Fresh MB payload (bypass the 24h cache) so the detector sees
        # the canonical per-track artist credits.
        payload = mb_api.get_release_raw(row["mb_release_id"], fresh=True)
        new_is_va = detect_va_compilation(row, mb_release_payload=payload)
        if new_is_va is False:
            db.update_request_fields(int(row["id"]), is_va_compilation=False)
            flipped += 1
        else:
            kept += 1
    except Exception as exc:
        print(f"  request={row['id']} ERROR: {type(exc).__name__}: {exc}")
        errors += 1
    if i % 25 == 0:
        print(f"  {i}/{total} processed (flipped={flipped} kept={kept} errors={errors})")

print(f"VA re-resolution: done — flipped={flipped} kept={kept} errors={errors}")
PY
```

Verify the function names against the current
`lib/field_resolver_service.py::detect_va_compilation` signature
before running — the agent is responsible for re-reading the surface
at deploy time. Errors land as `unresolved_mirror_unavailable`-style
failures on the next normal cycle; do not retry-loop here.

The 48 flipped rows do NOT need an immediate plan re-bump — their
`generator_id` was already current after §3, so the next 5-min cycle
will see `is_va_compilation=False` and run `_generate_normal_plan`
against them, replacing the degraded VA plan with the full normal
slot mix.

#### 4.3 Restart services in reverse dependency order

```bash
ssh doc2 'sudo systemctl start cratedigger-web.service \
                              cratedigger-importer.service \
                              cratedigger.service'
```

```bash
ssh doc2 'sudo systemctl is-active cratedigger-web.service \
                                   cratedigger-importer.service \
                                   cratedigger.service'
```

### 5. Verify

#### 5.1 VA flag distribution

```bash
ssh doc2 'sudo PGPASSWORD=$(sudo grep ^PGPASSWORD /run/secrets/cratedigger-pgpass | cut -d= -f2) pipeline-cli query --json "
SELECT
  COUNT(*) FILTER (WHERE is_va_compilation = TRUE) AS va_flagged,
  COUNT(*) FILTER (WHERE artist_name IN ('\''Various Artists'\'', '\''Various'\'')) AS string_va
FROM album_requests
WHERE status = '\''wanted'\''"'
```

Expectation: `va_flagged ≈ 25` (down from ~73 baseline). `string_va`
unchanged at ~25 (artist_name column wasn't touched). The two
counts should now agree because the rows that survived re-resolution
are exactly the ones with diverging per-track artists — the genuine
VA shape.

#### 5.2 Normal-plan restoration for the flipped cohort

After the next 5-min cycle, sample a few flipped rows and confirm
their active plan is normal-shape (default / literal / literal_flac
slots present):

```bash
ssh doc2 'sudo PGPASSWORD=$(sudo grep ^PGPASSWORD /run/secrets/cratedigger-pgpass | cut -d= -f2) pipeline-cli query --json "
SELECT id, artist_name, album_title, is_va_compilation
FROM album_requests
WHERE status = '\''wanted'\'' AND is_va_compilation = FALSE
  AND artist_name NOT IN ('\''Various Artists'\'', '\''Various'\'')
ORDER BY updated_at DESC
LIMIT 5"'
```

Then `pipeline-cli search-plan show <id>` for one of them — should
list ordinals labelled `default` / `literal` / `literal_flac` /
`unwild` / `track_*`, not `va_track_artist_*`.

### 6. Known limitations / what this gives us

- The VA re-resolution one-shot only runs against rows with non-NULL
  `mb_release_id`. Rows that originated from Discogs (no MB release
  ID) keep the PR1-era VA flag — Discogs VA detection was already
  Rule 1 (canonical-MBID match), so they were either flagged by 033's
  pure-SQL seed or stayed FALSE.
- Rows where the MB mirror is unreachable during the window land as
  errors and keep their pre-deploy VA flag. Re-running the heredoc
  picks them up.
- `is_va_compilation` flips do NOT touch `search_filetype_override`,
  `min_bitrate`, or any other operator-set fields. Only the column
  the detector owns.

---

## PR3 — Detection + telemetry

Code-only deploy (no schema migrations land). Wires `search_log`
forensics writes (R22-R27), materialises `album_requests.failure_class`
at plan-wrap (R28), ships the dedicated `cratedigger-unfindable.service`
oneshot + `cratedigger-unfindable.timer` for the 4-bucket
`unfindable_category` taxonomy (R18-R20), and captures the
long-tail-rescue `rescued_at` / `prior_unfindable_category` audit on
import success (R21).

Deploy date: 2026-05-26.

### 1. Pre-deploy

#### 1.1 Backup the pipeline DB

```bash
ssh doc2 'pg_dump -h 10.20.0.11 -U cratedigger cratedigger' \
  > /tmp/cratedigger_backup_pr3_$(date +%Y%m%d_%H%M%S).sql
```

PR3 mutates `album_requests` columns (`failure_class`,
`unfindable_category`, `rescued_at`, `prior_unfindable_category`) and
adds rows to `search_log` with the forensics columns populated.
Backup before pulling the trigger.

### 2. Deploy code

```bash
# 1. On dev: push code that landed PR3 (merged via PR #380)
git push origin main

# 2. On doc1: bump cratedigger-src flake input
ssh doc1 'cd ~/nixosconfig && nix flake update cratedigger-src \
  && git add flake.lock \
  && git commit -m "cratedigger: PR3 — detection + telemetry" \
  && git push'

# 3. On doc2: rebuild. cratedigger-db-migrate is a no-op (no new
#    migrations — 027-033 all shipped in PR1). cratedigger-web +
#    cratedigger-importer restart. cratedigger.service is
#    restartIfChanged=false; the 5-min timer picks up the new code.
#    cratedigger-unfindable.{service,timer} land for the first time.
ssh doc2 'sudo nixos-rebuild switch --flake github:abl030/nixosconfig#doc2 --refresh'
```

### 3. Downstream wrapper hotfix — `EnvironmentFile` for the detection unit

**Caught by the post-deploy smoke test on 2026-05-26.** The upstream
module (`nix/module.nix`) exposes the `cratedigger-unfindable.service`
shape with `Environment="PIPELINE_DB_DSN=..."` but does NOT inject
sops secret paths — secret paths are host-owned (per the
single-operator wrapper pattern used for `cratedigger.service` and
`cratedigger-web.service`). First run failed with:

```
psycopg2.OperationalError: fe_sendauth: no password supplied
```

Fix landed in the downstream wrapper
(`~/nixosconfig/modules/nixos/services/cratedigger.nix`, commit
`113c203a`):

```nix
systemd.services.cratedigger-unfindable.serviceConfig.EnvironmentFile =
  lib.mkAfter [config.sops.secrets."cratedigger-pgpass".path];
```

Mirror the pattern from the same wrapper's existing
`cratedigger.service.serviceConfig.EnvironmentFile` augmentation —
both units need libpq credentials from the same sops secret. Verify
after the fix:

```bash
ssh doc2 'sudo systemctl cat cratedigger-unfindable.service | grep EnvironmentFile'
```

Should show `EnvironmentFile=/run/secrets/cratedigger-pgpass` in the
override drop-in.

### 4. Verify

#### 4.1 Forensics columns populated on fresh cycle

After the next 5-min cycle, sample new search_log rows:

```bash
ssh doc2 'sudo PGPASSWORD=$(sudo grep ^PGPASSWORD /run/secrets/cratedigger-pgpass | cut -d= -f2) pipeline-cli query --json "
SELECT
  outcome,
  rejection_reason,
  matcher_score_top1,
  query_template,
  query_token_count,
  query_distinct_token_count,
  expected_track_count,
  result_count,
  result_count_uncapped
FROM search_log
WHERE created_at > NOW() - INTERVAL '\''10 min'\''
ORDER BY id DESC
LIMIT 5"'
```

Expect non-NULL values for the seven R22-R27 columns on every new
row. Sample post-deploy on 2026-05-26 showed a representative
`no_match` row with `rejection_reason='strict_count_mismatch'`,
`matcher_score_top1=0.0`, `query_template='{artist} {title}'`.

#### 4.2 failure_class materialisation

After a few requests have cycle-wrapped post-deploy:

```bash
ssh doc2 'sudo PGPASSWORD=$(sudo grep ^PGPASSWORD /run/secrets/cratedigger-pgpass | cut -d= -f2) pipeline-cli query --json "
SELECT failure_class, COUNT(*)
FROM album_requests
WHERE status = '\''wanted'\''
GROUP BY failure_class
ORDER BY COUNT(*) DESC"'
```

Distribution populates as wanted requests wrap their cycles; rows that
haven't yet wrapped under PR3 code stay `NULL`. Full coverage takes
~1-2 cycles per request, so steady-state is typically reached within
~24h on the 5-min cadence.

#### 4.3 First detection-job smoke test

```bash
ssh doc2 'sudo systemctl start cratedigger-unfindable.service --no-block'
ssh doc2 'sudo journalctl -u cratedigger-unfindable.service -f --since "10 min ago"'
```

First-run smoke test on 2026-05-26: 21 rows probed in ~8 min,
distribution:

| Category | Count |
|---|---|
| `album_absent_artist_present` | 18 |
| `one_track_structural` | 3 |

(The `one_track_structural` rows were already seeded by migration 033;
the run re-asserted them.) The detection unit runs K=100 rows per
batch on its daily timer with a weekly per-request cadence target —
full cohort coverage takes ~9 days at this rate.

Verify the categorisation surfaced:

```bash
ssh doc2 'sudo PGPASSWORD=$(sudo grep ^PGPASSWORD /run/secrets/cratedigger-pgpass | cut -d= -f2) pipeline-cli query --json "
SELECT unfindable_category, COUNT(*)
FROM album_requests
WHERE status = '\''wanted'\''
GROUP BY unfindable_category
ORDER BY COUNT(*) DESC NULLS LAST"'
```

#### 4.4 systemd units healthy

```bash
ssh doc2 'systemctl is-active cratedigger-unfindable.timer'
ssh doc2 'systemctl list-timers cratedigger-unfindable.timer'
```

Timer should be `active` with the next fire scheduled within ~24h +
the `RandomizedDelaySec=30min` jitter.

### 5. Known limitations / what this gives us

- **No new migrations.** All 7 schema migrations (027-033) shipped in
  PR1. PR3 is pure write-wiring + a new systemd unit pair.
- **First-cycle `failure_class` coverage is incremental.** A request
  only gets a `failure_class` when its cursor wraps under PR3 code.
  Rows with very long plans wrap less often; expect a slow ramp to
  full coverage over the first ~24h.
- **Detection cohort coverage takes ~9 days.** K=100/day × ~830
  wanted requests + weekly per-request cadence → first complete
  sweep finishes in ~9 days. Cohort distribution numbers stabilise
  after that.
- **Rescue-capture is forward-only.** Requests imported before PR3
  deploy that were categorised at the time do NOT retroactively
  populate `rescued_at` — the importer success path only captures
  rescues on imports that land under PR3 code. Forward-only by
  design.

---

## PR4 — Operator surface

Deploy date: 2026-05-26.

Standard flake bump + rebuild. No backfill, no new migrations — every
schema dependency landed in PR1's 027–032 sequence. New
`/api/triage/*` and `/api/_index` endpoints come online; existing
endpoints unchanged. `cratedigger-web.service` restarts on
`nixos-rebuild switch`; `cratedigger.service` 5-min timer is
untouched (search loop behaviour is identical).

Deploy steps (executed):

```bash
# 1. Merge PR #381 to main (GitHub "Create a merge commit")
gh pr merge 381 --merge --delete-branch

# 2. On doc1 — bump flake input
cd ~/nixosconfig
nix flake update cratedigger-src
git add flake.lock
git commit -m "cratedigger: bump to PR #381 (PR4 — operator triage surface + /api/_index)"
git push

# 3. Rebuild doc2
ssh doc2 'sudo nixos-rebuild switch --flake github:abl030/nixosconfig#doc2 --refresh'
```

`cratedigger-db-migrate.service` ran clean as a no-op (no new
migration files). `cratedigger-web`, `cratedigger-importer`, and
`cratedigger-import-preview-worker` restarted on the new closure.

### Post-deploy verification (executed 2026-05-26)

- `curl https://music.ablz.au/api/_index | jq 'length'` → 62 routes
  registered, all with non-empty descriptions (audit passes).
- `curl 'https://music.ablz.au/api/triage/list?filter=data_quality:status=unresolved_4xx_client&limit=200' | jq '.results | length'`
  → 75 — exact match to #374's reported sticky-4xx cohort count.
- `curl 'https://music.ablz.au/api/triage/list?filter=unfindable&limit=200' | jq '.results | length'`
  → 197 (80 `album_absent_artist_present` + 117 `one_track_structural`),
  matching PR3's detection output.
- Per-request: `curl https://music.ablz.au/api/triage/17` returns the
  full `TriageResult` envelope (request_meta + unfindable + 4
  field_quality entries + search_forensics with 40 searches).
- CLI parity: `pipeline-cli triage list --filter=data_quality:status=unresolved_4xx_client --limit=3 --json`
  emits the same envelope shape the API returns
  (`{filter, next_after, page_size, results}`) with production-shape
  values (`status='unresolved_4xx_client'`, `reason_code='http_400'`).
- `pipeline-cli routes --json` self-documents `triage show / triage list / routes`.

### Operator workflow

```bash
# Sticky 4xx cohort (#374) — operators investigating
# deprecated/malformed/410'd MBIDs
pipeline-cli triage list --filter=data_quality:status=unresolved_4xx_client

# Per-request triage envelope
pipeline-cli triage show 17

# Cohort listings by unfindable taxonomy
pipeline-cli triage list --filter=unfindable:album_absent_artist_present
pipeline-cli triage list --filter=unfindable:wrong_pressing_available

# Two-form data-quality filter:
#   data_quality:status=<status>  — by resolver status bucket (the cohort key)
#   data_quality:reason=<code>    — by HTTP code (e.g. http_400)
#   data_quality:<field>          — by tracked field (release_group_year etc.)
pipeline-cli triage list --filter=data_quality:reason=http_400
pipeline-cli triage list --filter=data_quality:release_group_year

# Self-documenting surfaces
curl https://music.ablz.au/api/_index | jq
pipeline-cli routes --json
```

### Known scope deferrals (post-PR4 follow-up)

- **Operator actions** (`triage replace`, `triage skip`, `triage ban`)
  — PR4 surfaces the #374 cohort as a query only. Concrete actions
  reuse `lib/mbid_replace_service.py` and `unfindable_category` writes
  but are not exposed yet. The triage envelope tells the operator
  what to do; the operator runs the existing `pipeline-cli replace` /
  `pipeline-cli set-quality ban-source` commands manually.
- **Replaced-row inclusion in cohorts is pinned**: `status='replaced'`
  audit rows are intentionally returned by `filter=all`/`unfindable`
  so operators can spot patterns across replacement history. Pinned
  by `tests/test_triage_service.py::TestListTriage.test_list_includes_replaced_rows`
  and the `lib/pipeline_db/misc.py::list_triage_page` docstring.

### Review summary

PR #381 went through `ce-code-review` with 8 reviewer personas
(correctness, testing, maintainability, project-standards,
api-contract, performance, agent-native, learnings-researcher).
22 findings surfaced; 21 applied in `8b9a217`; 1 pinned by test
(replaced-row policy) in `eb005bb`.

The headline finding (P1, correctness) was that
`data_quality:reason=<code>` filter targeted the wrong column — the
#374 cohort's `unresolved_4xx_client` bucket lives in the `status`
column per `lib/field_resolver_service.py::_classify_lookup_exception`,
while `reason_code` carries the concrete HTTP code (`http_400`,
`http_410`, etc.). Test fixtures had faked the wrong shape so green
tests masked a workflow that returned zero rows in production. The
fix added a new `data_quality:status=<status>` filter form
(additive — kept `reason=` for HTTP-code filtering) and rewrote
fixtures across `test_triage_service.py`, `test_pipeline_cli.py`,
`test_web_server.py` to production shape. Confirmed live on
2026-05-26 against the real cohort of 75 stuck requests.
