# Persisted Search Plans — Rollout & Verification

Operator runbook for the `feat: persisted search plans` cutover. Covers
pre-deploy backups, baseline capture, migration verification, first-cycle
reconciliation reading, smoke checks, before/after monitoring, the
no-new-`exhausted` invariant, rollback, and generator-id discipline.

The cutover replaces the runtime `select_variant` ladder with persisted
per-request `search_plans` and a request-owned cursor
(`active_plan_id`, `next_plan_ordinal`, `plan_cycle_count`). After the
cutover, search execution reads `get_wanted_searchable(<current
generator id>)` and consumes one plan-item per request per cycle
through one atomic guarded DB write that owns both the search-log
insert and the cursor advance/wrap.

The dashboard UI itself is **deferred** (see plan §Scope Boundaries).
What this rollout doc verifies is the dashboard-ready data + the
operator-facing CLI and API surfaces.

---

## 1. Pre-deploy

### 1.1 Backup the pipeline DB

```bash
ssh doc2 'pg_dump -h 192.168.100.11 -U cratedigger cratedigger' \
  > /tmp/cratedigger_backup_$(date +%Y%m%d_%H%M%S).sql
```

Required before any destructive migration. This migration is additive
(new tables, nullable columns) so rollback is operational repair, not
schema rollback — but keep the backup for safety.

### 1.2 Capture baselines

These are the production-snapshot signals we want to compare against
after the deploy. The plan's planning-time snapshot (584 wanted, 86
due, 498 backed off, 50,870 search_log rows, 377 exhausted in 7 days)
is sizing context, NOT a hard expectation — your numbers may differ.

Capture them yourself before the deploy with the queries below. Save
to a scratch file so the post-deploy comparison is honest.

```bash
# Wanted bucket sizing.
ssh doc2 "pipeline-cli query \"
SELECT
  COUNT(*) FILTER (WHERE status='wanted') AS wanted_total,
  COUNT(*) FILTER (WHERE status='wanted' AND (next_retry_after IS NULL OR next_retry_after <= NOW())) AS wanted_due,
  COUNT(*) FILTER (WHERE status='wanted' AND next_retry_after > NOW()) AS wanted_backed_off
FROM album_requests\""

# search_log totals + outcomes (last 24h, 7d).
ssh doc2 "pipeline-cli query \"
SELECT
  COUNT(*) AS total_rows,
  COUNT(*) FILTER (WHERE created_at >= NOW() - INTERVAL '24 hours') AS rows_24h,
  COUNT(*) FILTER (WHERE created_at >= NOW() - INTERVAL '7 days') AS rows_7d,
  COUNT(*) FILTER (WHERE outcome='exhausted' AND created_at >= NOW() - INTERVAL '7 days') AS exhausted_7d,
  COUNT(*) FILTER (WHERE outcome='found' AND created_at >= NOW() - INTERVAL '24 hours') AS found_24h,
  COUNT(*) FILTER (WHERE outcome='no_results' AND created_at >= NOW() - INTERVAL '24 hours') AS no_results_24h,
  COUNT(*) FILTER (WHERE outcome='no_match' AND created_at >= NOW() - INTERVAL '24 hours') AS no_match_24h,
  COUNT(*) FILTER (WHERE outcome IN ('error','timeout','empty_query') AND created_at >= NOW() - INTERVAL '24 hours') AS errors_24h
FROM search_log\""

# Distinct requests + average / p95 elapsed + average result_count
# + browse/match time + peers / fanout (last 24h).
ssh doc2 "pipeline-cli query \"
SELECT
  COUNT(DISTINCT request_id) AS distinct_requests_24h,
  AVG(elapsed_s) AS avg_elapsed_s,
  percentile_cont(0.95) WITHIN GROUP (ORDER BY elapsed_s) AS p95_elapsed_s,
  AVG(result_count) AS avg_result_count,
  AVG(browse_time_s) AS avg_browse_s,
  AVG(match_time_s) AS avg_match_s,
  SUM(peers_browsed) AS peers_browsed,
  SUM(fanout_waves) AS fanout_waves
FROM search_log
WHERE created_at >= NOW() - INTERVAL '24 hours'\""

# Cycle cache counters (last 24h).
ssh doc2 "pipeline-cli query \"
SELECT
  SUM(cache_pos_hits) AS cache_pos_hits,
  SUM(cache_neg_hits) AS cache_neg_hits,
  SUM(cache_misses) AS cache_misses,
  SUM(cache_errors) AS cache_errors,
  SUM(cache_write_errors) AS cache_write_errors,
  SUM(cache_fuse_tripped) AS cache_fuse_tripped
FROM cycle_metrics
WHERE created_at >= NOW() - INTERVAL '24 hours'\""

# search_attempts distribution among wanted (sanity check that the
# scheduler/backoff fields stay populated post-cutover).
ssh doc2 "pipeline-cli query \"
SELECT
  search_attempts,
  COUNT(*)
FROM album_requests
WHERE status='wanted'
GROUP BY search_attempts
ORDER BY search_attempts\""
```

---

## 2. Migration

### 2.1 How the migrate service runs

`cratedigger-db-migrate.service` is a systemd oneshot
(`restartIfChanged = true`, `RemainAfterExit = true`). It runs the
versioned SQL files under `migrations/` in order via
`scripts/migrate_db.py` on every `nixos-rebuild switch` BEFORE
`cratedigger-web.service`, `cratedigger-importer.service`, and
`cratedigger.service` start. All three services `requires` the
migrate unit, so a failed migration blocks the app from coming up
against an inconsistent schema.

`migrations/014_persisted_search_plans.sql` adds the
`search_plans` and `search_plan_items` tables, one nullable active-plan
pointer on `album_requests` (`active_plan_id`), two non-null cursor
counters (`next_plan_ordinal`, `plan_cycle_count`) with default 0, and
twelve nullable plan-context columns on `search_log`. It is fully
additive: old code can ignore the new tables and columns.

### 2.2 Verify migration application

```bash
# 014 is the new top of the migration ladder.
ssh doc2 "pipeline-cli query \"
SELECT version, applied_at FROM schema_migrations
ORDER BY version DESC LIMIT 5\""

# Plan tables exist with the expected indexes / constraints.
ssh doc2 "pipeline-cli query \"
SELECT relname FROM pg_class
WHERE relname IN ('search_plans','search_plan_items')
  AND relkind = 'r'\""

# Request cursor fields exist.
ssh doc2 "pipeline-cli query \"
SELECT column_name, data_type, is_nullable, column_default
FROM information_schema.columns
WHERE table_name = 'album_requests'
  AND column_name IN ('active_plan_id','next_plan_ordinal','plan_cycle_count')
ORDER BY column_name\""

# search_log plan-context fields exist.
ssh doc2 "pipeline-cli query \"
SELECT column_name, data_type, is_nullable
FROM information_schema.columns
WHERE table_name = 'search_log'
  AND (column_name LIKE 'plan_%'
       OR column_name IN ('execution_stage','attempt_consumed','cursor_update_status','stale_reason'))
ORDER BY column_name\""
```

### 2.3 Plan-table integrity after first reconciliation

After the first cycle has run the startup reconciliation (see §3),
verify the relational invariants the migration constraints enforce:

```bash
# Plan-table row counts.
ssh doc2 "pipeline-cli query \"
SELECT
  (SELECT COUNT(*) FROM search_plans)        AS plans,
  (SELECT COUNT(*) FROM search_plan_items)   AS plan_items,
  (SELECT COUNT(*) FROM search_plans WHERE status='active')        AS active_plans,
  (SELECT COUNT(*) FROM search_plans WHERE status='superseded')    AS superseded_plans,
  (SELECT COUNT(*) FROM search_plans WHERE status='failed_deterministic') AS failed_det,
  (SELECT COUNT(*) FROM search_plans WHERE status='failed_transient')     AS failed_trans\""

# Active-plan FK integrity. Should return 0.
ssh doc2 "pipeline-cli query \"
SELECT COUNT(*) AS dangling_active_plan_ids
FROM album_requests r
LEFT JOIN search_plans p ON r.active_plan_id = p.id
WHERE r.active_plan_id IS NOT NULL AND p.id IS NULL\""

# Active-plan ownership. Should return 0 (every active_plan must
# belong to its own request).
ssh doc2 "pipeline-cli query \"
SELECT COUNT(*) AS misowned_active_plans
FROM album_requests r
JOIN search_plans p ON r.active_plan_id = p.id
WHERE p.request_id <> r.id\""

# Contiguous ordinals on every plan. Every row should have ok=t.
ssh doc2 "pipeline-cli query \"
SELECT plan_id,
       COUNT(*) AS items,
       MAX(ordinal) AS max_ordinal,
       (MAX(ordinal) + 1 = COUNT(*)) AS ok
FROM search_plan_items
GROUP BY plan_id
HAVING (MAX(ordinal) + 1) <> COUNT(*)
LIMIT 20\""

# Wanted rows with no plan + no current-generator failure record.
# Should be 0 after reconciliation runs at least once. Non-zero is a
# stop-the-deploy signal -- inspect those rows individually.
ssh doc2 "pipeline-cli query \"
SELECT COUNT(*) AS unclassified_wanted
FROM album_requests
WHERE status = 'wanted'
  AND active_plan_id IS NULL\""
```

---

## 3. First startup reconciliation

The first time `cratedigger.service` runs after the deploy it executes
the startup reconciliation pass before Phase 2. See
`lib/startup_reconciliation.py`. Every wanted row is classified into
exactly one bucket and the totals are emitted as a single log line:

```
search_plan_reconciliation generator_id=search-plan/2026-05-08-1
  wanted_total=584
  active_current=0
  generated=580
  old_generator_replaced=0
  deterministic_failed=2
  retryable_failed=2
  skipped=0
  unclassified_no_plan=0
  duration_s=12.34
  dry_run=false
```

Field reading guide:

- `generator_id` — current `SEARCH_PLAN_GENERATOR_ID`. Pin it in change
  control: see §7.
- `wanted_total` — every wanted row, ignoring `next_retry_after` and
  page-size paging.
- `active_current` — already had a current-generator active plan
  (no-op on this row).
- `generated` — newly generated current-generator active plan.
- `old_generator_replaced` — superseded an old-generator active plan
  with a new current-generator one.
- `deterministic_failed` — generation failed sticky on the current
  generator id (e.g. `no_runnable_query`).
- `retryable_failed` — transient resolver/dependency failure; retried
  next cycle.
- `skipped` — row was deleted between the all-wanted scan and the
  service call.
- `unclassified_no_plan` — **stop-the-deploy signal**. A wanted row
  with no active plan AND no current-generator failure record. Each
  such row is also logged at `ERROR` with its `request_id` so ops can
  follow up.
- `duration_s` — wall time of the reconciliation pass.
- `dry_run` — `true` only when invoked with `--reconcile-dry-run`.

Tail the journal for the line:

```bash
ssh doc2 'sudo journalctl -u cratedigger.service --since "5 min ago" \
  | grep search_plan_reconciliation'
```

If `unclassified_no_plan > 0`, **do not advance the rollout**. Inspect
the offending request ids (also at ERROR), repair them via
`pipeline-cli search-plan regenerate <id>`, and re-run the cycle.

---

## 4. Smoke checks

Read-only first; mutating last. Successful regeneration resets
cursor/cycle to 0 — never run it casually against an active wanted
request just to inspect.

```bash
# Read-only inspection of a known-active request. Shows active plan,
# generator id, cursor, cycle count, items, provenance, failure
# states, and legacy logs.
ssh doc2 'pipeline-cli search-plan show <request_id>'
ssh doc2 'pipeline-cli search-plan show <request_id> --json'

# Spot-check schema (read-only).
ssh doc2 'pipeline-cli query "
SELECT id, status, active_plan_id, next_plan_ordinal, plan_cycle_count
FROM album_requests
WHERE id = <request_id>"'

ssh doc2 'pipeline-cli query "
SELECT plan_id, ordinal, strategy, query, canonical_query_key, repeat_group
FROM search_plan_items
WHERE plan_id = (SELECT active_plan_id FROM album_requests WHERE id = <request_id>)
ORDER BY ordinal"'

# Mutating: pick an intentionally non-wanted or known-broken request.
# Allowed for any status; only `wanted` requests are executable.
# Successful regeneration resets cursor and cycle to 0 -- this is by
# design and should NOT be run on a healthy active wanted request.
ssh doc2 'pipeline-cli search-plan regenerate <chosen_request_id>'
```

---

## 5. Doc2 deploy verification

After `nixos-rebuild switch` on doc2, confirm:

```bash
# Migrate service ran cleanly.
ssh doc2 'systemctl is-active cratedigger-db-migrate.service'
ssh doc2 'sudo journalctl -u cratedigger-db-migrate.service --since "10 min ago"'

# Web service restarted (the new GET /search-plan and POST regenerate
# routes need the new code).
ssh doc2 'systemctl status cratedigger-web.service --no-pager | head'

# Pipeline service is timer-driven oneshot. Don't restart it manually
# unless triaging.
ssh doc2 'systemctl status cratedigger.service --no-pager | head'
ssh doc2 'systemctl status cratedigger.timer --no-pager | head'

# Validate the first pipeline cycle through the reconciliation log
# AND the dashboard counts.
ssh doc2 'sudo journalctl -u cratedigger.service --since "5 min ago" \
  | grep search_plan_reconciliation'

curl -sS https://music.ablz.au/api/pipeline/dashboard | jq .plan_readiness
```

Expected:

- `cratedigger-db-migrate` exits 0; `journalctl` shows
  `014_persisted_search_plans.sql applied`.
- `cratedigger-web` is `active (running)`.
- `cratedigger.service` is `inactive` (oneshot just finished) with
  `cratedigger.timer` `active (waiting)`.
- The reconciliation summary log line is present and
  `unclassified_no_plan=0`.
- `/api/pipeline/dashboard` returns a `plan_readiness` block whose
  buckets sum to `wanted_total`.

---

## 6. Before/after monitoring windows

Top-three track-slot selection and default-repeat materialisation are
**intentional behaviour changes**. Compare yield, cost, and coverage
across these windows:

- End of cycle 1 (within 5 min of deploy)
- End of day (24h after deploy)
- Next day (48h after deploy)

Pivot dimensions:

- `plan_strategy` (`default`, `unwild`, `unwild_year`, `track_<idx>`)
- `plan_ordinal`
- `plan_canonical_query_key`
- `plan_repeat_group`

```bash
# Per-strategy yield + cost (24h).
ssh doc2 "pipeline-cli query \"
SELECT
  plan_strategy,
  COUNT(*) AS attempts,
  COUNT(*) FILTER (WHERE outcome='found') AS found,
  COUNT(*) FILTER (WHERE outcome='no_match') AS no_match,
  COUNT(*) FILTER (WHERE outcome='no_results') AS no_results,
  AVG(elapsed_s) AS avg_elapsed_s,
  AVG(browse_time_s) AS avg_browse_s,
  AVG(match_time_s) AS avg_match_s,
  COUNT(*) FILTER (WHERE cursor_update_status='stale') AS stale,
  COUNT(*) FILTER (WHERE attempt_consumed = false) AS non_consuming
FROM search_log
WHERE created_at >= NOW() - INTERVAL '24 hours'
  AND plan_id IS NOT NULL
GROUP BY plan_strategy
ORDER BY attempts DESC\""

# Per-canonical-query group rollup (24h).
ssh doc2 "pipeline-cli query \"
SELECT
  plan_canonical_query_key,
  COUNT(*) AS attempts,
  COUNT(*) FILTER (WHERE outcome='found') AS found,
  AVG(elapsed_s) AS avg_elapsed_s
FROM search_log
WHERE created_at >= NOW() - INTERVAL '24 hours'
  AND plan_canonical_query_key IS NOT NULL
GROUP BY plan_canonical_query_key
ORDER BY attempts DESC
LIMIT 30\""
```

The dashboard `searches.windows[*]` block surfaces `cursor_wraps`,
`stale_completions`, `non_consuming`, and
`cache_attribution_level='cycle_only'` for the same windows the
existing dashboard exposes. `cache_attribution_level='cycle_only'` is
not a placeholder — `search_log` has no per-search cache columns; the
dashboard cannot honestly imply per-slot cache numbers. See
`lib/pipeline_db.py::CACHE_ATTRIBUTION_CYCLE_ONLY`.

---

## 7. Verify no-new-`exhausted`

After the deploy timestamp, **new** `search_log` rows must never carry
`outcome='exhausted'`. Pre-deploy historical rows stay valid.

```bash
# Substitute the actual deploy timestamp (UTC).
DEPLOY_TS=2026-05-08T13:00:00Z

ssh doc2 "pipeline-cli query \"
SELECT
  COUNT(*) FILTER (WHERE outcome='exhausted' AND created_at < TIMESTAMP '$DEPLOY_TS') AS exhausted_before_deploy,
  COUNT(*) FILTER (WHERE outcome='exhausted' AND created_at >= TIMESTAMP '$DEPLOY_TS') AS exhausted_after_deploy_MUST_BE_ZERO
FROM search_log\""

# Cycle-wrap signal that REPLACES exhausted. Should grow as the new
# code drains plans.
ssh doc2 "pipeline-cli query \"
SELECT COUNT(*) AS cursor_wraps_after_deploy
FROM search_log
WHERE cursor_update_status = 'wrapped'
  AND created_at >= TIMESTAMP '$DEPLOY_TS'\""
```

If the second query returns non-zero, **the cutover has regressed**.
Roll back per §8 and investigate.

---

## 8. Rollback

The migration is additive and rollback-compatible:

- **Schema**: new tables (`search_plans`, `search_plan_items`) and new
  nullable columns on `album_requests` and `search_log`. Old code can
  ignore them. The `search_log.outcome` CHECK is unchanged so legacy
  `exhausted` writes remain legal under old code.
- **Pre-deploy backup**: §1.1's `pg_dump` is your destructive-recovery
  fallback. The migrations are non-destructive — you should not need
  it for this rollout — but having it gates the recovery story.
- **Repair after rollback/redeploy**: any stranded current-generator
  plan state is repaired by startup reconciliation on the next cycle
  or by `pipeline-cli search-plan regenerate <id>` for individual
  rows. There is no SQL backfill of `search_log` into plan items —
  intentionally.

Rollback procedure:

```bash
# Stop the timer so a new cycle does not run with the new code.
ssh doc2 'sudo systemctl stop cratedigger.timer cratedigger.service'

# Roll back the deploy.
ssh doc2 'sudo nixos-rebuild switch --flake .#doc2 --rollback'

# Old code starts; new tables/columns are ignored. Restart the timer.
ssh doc2 'sudo systemctl start cratedigger.timer'

# Confirm old code is running.
ssh doc2 'grep "<unique pre-cutover string>" /nix/store/*/lib/search.py 2>/dev/null'
```

After a redeploy, startup reconciliation will re-classify rows and
either supersede stale plans or recreate active plans for wanted rows
that drifted under old code. No manual psql repair is expected.

---

## 9. Generator-id discipline

`SEARCH_PLAN_GENERATOR_ID` in `lib/search.py` is the **single runtime
source** of "current generator output". Bump it (date-stamped tag,
e.g. `search-plan/2026-05-08-2`) **whenever** any of the following
change:

- generator output rules (which slots emit, in what order)
- query tokenisation
- the `STOPWORDS` set (`the`, `you`, `from`, `and` today)
- the `GENERIC_TITLE_TOKENS` set (distinctiveness blacklist; see
  `lib/search.py::score_track_distinctiveness`)
- slot ordering / ranking
- dedupe behaviour
- repeat-group identity
- provenance shape

Recent bumps:

| ID | Date | What landed |
|---|---|---|
| `search-plan/2026-05-08-1` | 2026-05-08 | Initial persisted-plan rollout |
| `search-plan/2026-05-08-2` | 2026-05-08 | First post-rollout fix |
| `search-plan/2026-05-19-1` | 2026-05-19 | Iteration 1 — entropy + matcher pre-filter |
| `search-plan/2026-05-25-1` | 2026-05-25 | Iteration 2 PR2 — `literal_lossless` retired, `catalog_number` + `track_3_artist` slots added, distinctiveness-ranked tracks with `GENERIC_TITLE_TOKENS` blacklist, VA-specific strategy mix, stopwords collapsed to single `STOPWORDS` constant. Plan-regen wave on first 1-2 cycles post-deploy is expected and bounded by existing wave caps; monitor `journalctl -u cratedigger` for the wave to clear within ~10-15 min. |
| (no bump) | 2026-05-26 | Iteration 2 PR3 — observability only; **`SEARCH_PLAN_GENERATOR_ID` deliberately not bumped**. PR3 wires the forensics writes (`rejection_reason`, `result_count_uncapped`, `query_token_count`, `query_distinct_token_count`, `expected_track_count`, `matcher_score_top1`, `query_template`) into `lib/pipeline_db.py::log_search`, materialises `album_requests.failure_class` at the cursor-wrap transaction in `lib/search_plan_service.py`, ships the dedicated `cratedigger-unfindable.service` + `cratedigger-unfindable.timer` (daily, K=100/run, ~7d per-request cadence) for the 4-bucket `unfindable_category` taxonomy, and captures `rescued_at` / `prior_unfindable_category` in the importer success path. No new migrations land (027-033 all shipped in PR1). Generator output is unchanged — none of the U11-U14 work touches `generate_search_plan` — so no plan-regen wave fires on rollout. |

Any change to those that does **not** bump the id will silently leave
old plans active under a new generator's rules. Two regression guards:

- `tests/test_search.py::test_generator_id_constant_is_pinned` pins
  the literal id AND a representative ladder snapshot. Generator
  output drift fails this test until the id is intentionally bumped.
- Startup reconciliation supersedes any active plan whose
  `generator_id` differs from the constant on the next cycle.

There is **no** automatic config-fingerprint invalidation; manual
discipline is the only currency contract (see plan §Scope Boundaries).

---

## 10. Quick checklist (printable)

- [ ] Pre-deploy backup taken (§1.1)
- [ ] Baselines captured (§1.2)
- [ ] `nixos-rebuild switch` succeeds (§5)
- [ ] `cratedigger-db-migrate` exits 0; `014_persisted_search_plans.sql` applied (§2.2)
- [ ] Plan tables, cursor fields, plan-context columns present (§2.2)
- [ ] First-cycle `search_plan_reconciliation` log line present, `unclassified_no_plan=0` (§3)
- [ ] Active-plan FK integrity, ownership, contiguous ordinals all OK (§2.3)
- [ ] `pipeline-cli search-plan show <id>` renders for at least one healthy request (§4)
- [ ] Dashboard `plan_readiness` buckets sum to `wanted_total` (§5)
- [ ] No new `outcome='exhausted'` rows after the deploy timestamp (§7)
- [ ] Strategy / ordinal / canonical-key rollups stable across end-of-cycle, end-of-day, next-day (§6)
