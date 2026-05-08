---
title: "perf: Lazy-fill peer-dir daily aggregates"
type: feat
status: completed
date: 2026-05-08
shipped: 2026-05-08
shipped_pr: https://github.com/abl030/cratedigger/pull/230
origin: https://github.com/abl030/cratedigger/issues/227
---

# perf: Lazy-fill peer-dir daily aggregates

## Outcome (2026-05-08)

Shipped in [PR #230](https://github.com/abl030/cratedigger/pull/230); deployed to doc2 the same evening.

- Migration 015 applied at `2026-05-08 13:51:00 UTC`.
- `peer_dir_daily_aggregates` populated with 13 completed-day rows on first dashboard load (Perth-local 2026-04-25 … 2026-05-07).
- `/api/pipeline/dashboard` now sub-100 ms cold and ms-warm.

**Caveat on the verification timings.** Between this morning's diagnosis (1.55 M rows / 603 MB on `peer_dir_observations`, ~7 s dashboard) and the deploy this evening, the source table was truncated to ~1 K rows / 568 KB by an out-of-band cleanup (see #227 comment). The post-deploy timings are real and the architecture is doing its job (cache populated correctly, lazy-fill works), but the dramatic before/after the plan promised — "first request post-deploy ≈ 3.6 s for backfill, subsequent loads sub-second" — is obscured by the data reduction. The perf win will manifest properly as `peer_dir_observations` grows back: completed days written once to cache, today's slice always live, no full-table scan per dashboard load.

**What still applies after this PR (tracked in #227):**

- **Q1 (lifetime totals)** is still a full-table scan. Currently fast because the source is small; will grow expensive again as the table refills (~500 K rows/day). Recommended fix per the issue: `cache_api.cached(...)` 5-min Redis wrapper on the dashboard route.
- **No Redis caching on the dashboard route** (still). Independent of Q1, this is the cheapest first defense once any sub-query starts to hurt.
- **Single-threaded `http.server`** still blocks every route behind one slow request — separate adjacent issue noted in the original #227 body.
- **`/api/pipeline/all` is uncached and 1.8 s steady-state.** Independent of `peer_dir_observations` size — surfaced during the 2026-05-08 diagnosis. Same `cache_api.cached(...)` pattern would help once it becomes a bottleneck.

The rest of this document is preserved as the historical plan that drove PR #230.

---

## Summary

Add a `peer_dir_daily_aggregates` cache table populated lazily on dashboard read so the per-day breakdown query (Q2 in #227) collapses from a 3.6 s parallel sequential scan over 1.55 M rows to cheap PK lookups for completed days plus one bounded slice for today.

---

## Problem Frame

The Pipeline dashboard's `get_peer_dir_daily_metrics()` issues two full-table aggregations against `peer_dir_observations` on every load. The per-day breakdown (Q2) accounts for ~3.6 s of the ~7 s total cost. Once a Perth-local day is over its `(new_combos, new_peers, new_dirs)` tuple is frozen — `first_seen_at` is wall-clock-stamped at insert and no late-arriving rows can land in a past day. That immutability is the lever this plan uses.

The work is scoped to Q2. Q1 (lifetime totals) is intrinsically full-scan and stays slow until a separate fix in #227.

(see origin: https://github.com/abl030/cratedigger/issues/227)

---

## Requirements

- R1. Eliminate the per-day breakdown's cost on every dashboard load after the first deploy.
- R2. Preserve the existing public response shape of `get_peer_dir_daily_metrics()` exactly — `web/js/pipeline.js` reads named keys.
- R3. Honour the Perth-local day-boundary the existing query uses; cache key is a Perth-local date, not a UTC date.
- R4. First-deploy backfill is bounded and idempotent — concurrent dashboard requests must not corrupt or duplicate cache rows.
- R5. `FakePipelineDB` mirrors the new method behavior so the contract-parity test (`TestPipelineDBFakeContract`) and the rest of the test suite stay green.
- R6. No DDL inside Python (per `.claude/rules/pipeline-db.md`) — schema changes ride a new versioned migration.

---

## Scope Boundaries

- This plan does **not** address Q1 (lifetime totals). The `COUNT(DISTINCT ...)` over the whole table is a separate problem with its own option set.
- This plan does **not** add the Redis cache wrapper to `get_pipeline_dashboard`. Those are tracked in #227.
- This plan does **not** migrate the web server from single-threaded `http.server` to `ThreadingHTTPServer`.
- This plan does **not** change the dashboard route's response shape — the cache is a query-path optimisation, not a contract change.
- This plan does **not** introduce a midnight cron, systemd timer, or background worker. Lazy-fill on dashboard read is the only population path.

### Deferred to Follow-Up Work

- Q1 lifetime-totals fix (separate decision: cache vs window vs counters): tracked in #227.
- Redis caching of `get_pipeline_dashboard`: tracked in #227.
- Threading model change for the web server: tracked in #227 *Adjacent improvements*.

---

## Context & Research

### Relevant Code and Patterns

- `lib/pipeline_db.py::get_peer_dir_daily_metrics` (line ~2641) — the slow function; signature `(self, days: int = 14) -> dict[str, Any]`. Returns `{"days": [...], "totals": {...}}`. Caller in `get_pipeline_dashboard_metrics` mutates the returned dict (`peer_dirs["heavy_queries"] = ...`); the new shape must remain a mutable dict.
- `lib/pipeline_db.py::create_successful_search_plan` (line ~3300) — reference for `psycopg2.extras.execute_values` usage, including the `old_autocommit = self.conn.autocommit; self.conn.autocommit = False; ... finally: self.conn.autocommit = old_autocommit` bracket pattern when atomicity across multiple statements is needed.
- `lib/pipeline_db.py::record_peer_dir_observations` — closest sibling: `execute_values` under autocommit with `ON CONFLICT ... DO UPDATE` on the same source table.
- `migrations/014_persisted_search_plans.sql` — recent migration; mirrors block-comment header, named constraints (`tablename_columnname_descriptor_*`), `idx_tablename_columnname[_modifier]` index naming.
- `migrations/012_peer_dir_observations.sql` — the table being optimised; check existing indexes before adding adjacent ones.
- `tests/fakes.py::FakePipelineDB.get_peer_dir_daily_metrics` (line ~2091) — already exists; mirrors signature and shape. Refactor the fake to match the new live-vs-cache split.
- `tests/test_fakes.py::TestPipelineDBFakeContract` (line ~2050) — runtime parity guard. `TestFakePipelineDBNewStubs` (line ~1245) is the home for new stub self-tests.
- `tests/test_pipeline_db.py::test_peer_dir_observations_track_first_seen_counts` (line ~1299) — pattern for seeding observations with `observed_at` overrides + asserting on returned dict.
- `tests/test_pipeline_db.py::make_db()` (line ~40) — TRUNCATE list. New cache table **must** be added so test isolation holds.
- `web/routes/pipeline.py::get_pipeline_dashboard` (line ~282) — caller. Currently no Redis wrapper; out of scope for this plan but knowledge that no upstream cache is being interfered with.
- `web/js/pipeline.js::loadDashboard()` — JS consumer. Reads `data.peer_dirs.totals.{known_combos, new_24h, known_peers, tracked_since}` and `data.peer_dirs.days[].{new_combos, new_peers, new_dirs}`. Public-shape contract.

### Institutional Learnings

- `docs/solutions/architecture/multiplexed-postgres-client-and-set-local-incompatibility.md` — never use `SET LOCAL` on the shared psycopg2 connection. Use the `old_autocommit` bracket pattern when atomicity is needed.
- `.claude/rules/pipeline-db.md` — DDL only in `migrations/`. `PipelineDB` is permanently `autocommit=True` except inside explicit transaction brackets.
- `.claude/rules/code-quality.md` — TDD; FakePipelineDB parity is enforced by `TestPipelineDBFakeContract` at test time.
- No existing lazy-fill-on-read aggregate pattern in the repo — this plan introduces it. The closest sibling pattern (`cycle_metrics`) writes once at event time, which is structurally different. Be deliberate about the shape so the next plan that needs this pattern has something clean to mirror.
- Tech: psycopg2 with `extras.execute_values`, `extras.Json`, `extras.RealDictCursor` already imported at `lib/pipeline_db.py:27`. PostgreSQL 16.13 in production.

### External References

None — local patterns and prior-art on this branch are sufficient.

---

## Key Technical Decisions

- **Cache table holds completed-day aggregates only; today's row is computed live every call.** A "completed" day is any Perth-local date strictly less than `(NOW() AT TIME ZONE 'Australia/Perth')::date`. Eliminates the cache-staleness concern entirely — completed days are immutable.
- **Cache PK is the Perth-local date, not UTC.** The existing query uses `(first_seen_at AT TIME ZONE 'Australia/Perth')::date` for day bucketing; the cache must use the same expression. UTC-vs-Perth differs by 8 hours and would silently mis-bucket day boundaries.
- **Backfill runs under autocommit with `INSERT ... ON CONFLICT DO NOTHING`, not inside a transaction.** Each cache row is independently idempotent — race-safe by construction. Transactional bracket would add complexity (Phase 1 research's concern about partial-write hazards under concurrent dashboard requests does not apply once each row is independently complete). One concurrent dashboard request sees the same backfill query land twice; `ON CONFLICT` makes the second one a no-op.
- **Backfill is one bounded `GROUP BY day` query, not per-day loops.** Detect missing days by `LEFT ANTI JOIN` against the cache (or `WHERE day NOT IN (SELECT day FROM peer_dir_daily_aggregates ...)`), then aggregate all missing days in a single scan and bulk-insert via `psycopg2.extras.execute_values`. First-deploy cost ≈ current Q2 cost (~3.6 s, single time per completed day post-deploy); subsequent loads sub-second.
- **No `computed_at` column.** Repo convention is `created_at`. Once a completed-day row is written it is by definition correct forever; staleness detection adds no value. Drop the field.
- **Public method signature unchanged.** `get_peer_dir_daily_metrics(self, days: int = 14) -> dict[str, Any]` keeps the same shape. The cache is an internal optimisation; the route, the JS, and the contract test stay unaware.
- **`make_db()` TRUNCATE list extends to the new table.** Test isolation depends on it. Forgetting this breaks test ordering silently.

---

## Open Questions

### Resolved During Planning

- **Cache vs invalidate semantics.** Resolved: completed days are immutable. No invalidation logic, no TTL, no `computed_at`.
- **Population trigger.** Resolved: lazy on dashboard read. No timer, no worker.
- **Atomicity model for the backfill.** Resolved: `ON CONFLICT DO NOTHING` under autocommit; each row idempotent.
- **Time-zone basis for the cache PK.** Resolved: Perth-local date, matching the existing query.
- **`computed_at` necessity.** Resolved: drop it. Use `created_at DEFAULT NOW()` only.

### Deferred to Implementation

- Exact column types (`int` vs `bigint` for the count columns — production max per day so far is ~585 K, well within int4 range). Implementer picks at migration-write time.
- Exact name of the new aggregate-fetching helper inside `get_peer_dir_daily_metrics` (e.g., `_completed_day_aggregates_from_cache`, `_lazy_fill_peer_dir_aggregates`). Name follows whichever shape reads cleanest in the refactored method.
- Whether to add a separate index on `(day)` beyond the implicit PK btree. Probably not — PK is the only access path.

---

## High-Level Technical Design

> *This illustrates the intended approach and is directional guidance for review, not implementation specification. The implementing agent should treat it as context, not code to reproduce.*

```mermaid
flowchart TB
    A[get_peer_dir_daily_metrics(days=14) called] --> B[Compute today_perth + window_start]
    B --> C{Read cache for window: SELECT day, new_combos, new_peers, new_dirs FROM peer_dir_daily_aggregates WHERE day BETWEEN window_start AND today_perth - 1}
    C --> D{All completed days present?}
    D -- yes --> H[Compute today's row live: 1 bounded GROUP BY query for today only]
    D -- no --> E[Compute missing completed days: 1 bounded GROUP BY query covering only the missing day set]
    E --> F[Bulk INSERT ... ON CONFLICT DO NOTHING via execute_values]
    F --> G[Re-read cache for window]
    G --> H
    H --> I[Compute totals query as before]
    I --> J[Merge cache rows + today + totals into existing dict shape]
    J --> K[Return dict]
```

The totals query (Q1) is **unchanged** — that lives outside this plan's scope.

---

## Implementation Units

### U1. Migration 015 — peer_dir_daily_aggregates table

**Goal:** Add the cache table and any necessary indexes via a versioned migration.

**Requirements:** R6.

**Dependencies:** None.

**Files:**
- Create: `migrations/015_peer_dir_daily_aggregates.sql`
- Test: `tests/test_migrator.py` (existing tests cover the migrator generally; add a smoke test for this migration if the existing pattern in `tests/test_migrator.py` warrants per-migration coverage)
- Test: `tests/test_pipeline_db.py::TestSchemaCreation.test_tables_exist` (add `assertIn('peer_dir_daily_aggregates', table_names)` to mirror the existing `peer_dir_observations` assertion)

**Approach:**
- Single `CREATE TABLE peer_dir_daily_aggregates` with columns: `day DATE PRIMARY KEY`, `new_combos INT NOT NULL`, `new_peers INT NOT NULL`, `new_dirs INT NOT NULL`, `created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()`.
- CHECK constraints: each count `>= 0`.
- No additional indexes — PK btree is the only access path.
- Block-comment header explaining the table's purpose (lazy-fill cache for completed-day Perth-local aggregates of `peer_dir_observations`) and the immutability invariant.
- No `IF NOT EXISTS` guard (per migration discipline).

**Patterns to follow:**
- Header style + naming from `migrations/014_persisted_search_plans.sql` and `migrations/012_peer_dir_observations.sql`.

**Test scenarios:**
- Happy path: migration applies cleanly to an empty schema (covered by existing `test_migrator` infrastructure).
- Edge case: re-running the migrator on an already-migrated DB does not re-apply (existing migrator behavior; verify implicitly).

**Verification:**
- `nix-shell --run "python3 -m unittest tests.test_migrator -v"` passes.
- Schema introspection shows the new table with the correct column types and PK.

---

### U2. Lazy-fill query path in `PipelineDB`

**Goal:** Refactor `get_peer_dir_daily_metrics` to read completed days from cache, lazy-fill missing days, and compute today live.

**Requirements:** R1, R2, R3, R4.

**Dependencies:** U1.

**Files:**
- Modify: `lib/pipeline_db.py`
- Test: `tests/test_pipeline_db.py`

**Approach:**
- Keep the public signature `get_peer_dir_daily_metrics(self, days: int = 14) -> dict[str, Any]` unchanged.
- Internal flow per the High-Level Technical Design: derive `today_perth` and `window_start_perth` once; cache lookup; missing-days backfill via single `GROUP BY` + `execute_values` with `ON CONFLICT DO NOTHING`; today's row from a 1-day-bounded query (see today-slice query shape below); totals query unchanged.
- After the backfill INSERT, re-read the cache in a single SELECT for the full window. (`execute_values` + `ON CONFLICT DO NOTHING` does not reliably return conflicting rows in PostgreSQL — only inserted rows. The re-read is on a 14-row table; the extra round-trip is sub-millisecond and worth the contractual clarity.)
- Today-slice query shape: pair a UTC pre-filter for index efficiency with a Perth-date predicate for boundary correctness — `WHERE first_seen_at >= today_perth_start_utc - INTERVAL '1 hour' AND (first_seen_at AT TIME ZONE 'Australia/Perth')::date = today_perth`. The pre-filter lets `idx_peer_dir_observations_first_seen` prune most rows before the TZ cast.
- Day-rollover is a correctness boundary covered by the existing test scenario; no special performance optimization needed since the post-rollover backfill is one extra cache row plus the standard SELECT re-read.
- Add the new cache table to the `make_db()` TRUNCATE list in `tests/test_pipeline_db.py`.
- Honour autocommit discipline: no autocommit-flip needed since `ON CONFLICT DO NOTHING` makes each row independently idempotent.
- Preserve mutability of the returned dict — `get_pipeline_dashboard_metrics` mutates `peer_dirs["heavy_queries"]` after this call.

**Execution note:** Test-first. The cache contract is non-trivial (TZ correctness, race safety, day-rollover) — write the failing tests against the public method first, then implement to pass them.

**Patterns to follow:**
- `_execute()` helper for single read queries; `with self.conn.cursor() as cur` for write blocks.
- `psycopg2.extras.execute_values` usage from `create_successful_search_plan` and `record_peer_dir_observations`.
- Time-zone expression `(first_seen_at AT TIME ZONE 'Australia/Perth')::date` matches the pre-change query exactly.

**Test scenarios:**
- Happy path: empty cache, 14 days of observations seeded → first call backfills 13 completed-day rows, computes today live, returns the same shape as the legacy method for the same input.
- Happy path: cache populated for all completed days → seed all completed-day rows directly via raw INSERT, then call the method and assert `SELECT COUNT(*) FROM peer_dir_daily_aggregates` is unchanged before/after; response unchanged. (Structural proxy for "no backfill query fired" — uses the test DB pattern already in the suite, no instrumentation.)
- Happy path: brand-new completed day rolls over → first call after Perth midnight backfills yesterday only; today is recomputed live.
- Happy path: only today's observations exist (zero completed days in window) → cache stays empty, today's row is correct, response shape intact.
- Edge case: zero observations entirely in the window → response shows zeros across all 14 days, no cache rows inserted, no errors.
- Edge case: gap day in middle of window (no observations on a completed day) → cache row inserted with `(0, 0, 0)`; subsequent reads hit the zero row and skip recompute.
- Edge case: Perth day-boundary correctness — observation with `first_seen_at = 2026-05-07 23:55+08:00` (Perth) lands in 2026-05-07's bucket, not 2026-05-08's. Use a deterministic `observed_at` override to test.
- Edge case: Perth midnight live-today correctness — observation with `first_seen_at = 2026-05-07 16:30 UTC` (= 2026-05-08 00:30 Perth) appears in today's live row count, not yesterday's cache row. Verifies the today-slice query's UTC-pre-filter + Perth-date-predicate combination handles the eastern edge of midnight.
- Integration: race — two dashboard reads for the same empty cache window run concurrently; `ON CONFLICT DO NOTHING` ensures the second's insert is a no-op; both return the same response.
- Error path: a DB error during the backfill `INSERT` propagates to the caller as an unhandled exception; the method does not catch it and does not return a partially-computed result. Assert the exception surfaces rather than a partial dict. (Under autocommit + `ON CONFLICT DO NOTHING` there is no transactional rollback semantics — each insert is independent — so the test is exception propagation, not rollback.)
- Contract: response shape is byte-for-byte identical to a captured pre-change response for the same input. Add a regression test that pins the dict keys explicitly.

**Verification:**
- `nix-shell --run "python3 -m unittest tests.test_pipeline_db -v"` passes, including the new tests.
- Manual: against a populated production-like DB, second call to the method completes in <100 ms (down from ~3.6 s).

---

### U3. FakePipelineDB mirror + parity guard

**Goal:** Update the in-memory fake to match the new lazy-fill semantics so the contract-parity test stays green and stateful tests using the fake observe correct day-boundary behavior.

**Requirements:** R5.

**Dependencies:** U2 (transitively requires U1).

**Files:**
- Modify: `tests/fakes.py`
- Test: `tests/test_fakes.py`

**Approach:**
- The fake's `get_peer_dir_daily_metrics` already exists and computes from `self.peer_dir_observations`. Add a `self.peer_dir_daily_aggregates: dict[date, dict]` (or list) to mirror the new cache table.
- Mirror the lazy-fill semantics: completed days come from the in-memory cache when present, otherwise computed-and-stored.
- **Bucket observations by Perth-local date, not UTC.** The existing fake at `tests/fakes.py:2123` uses `(_utcnow() - timedelta(days=idx)).date()` — UTC-based, which silently disagrees with the real method's `(first_seen_at AT TIME ZONE 'Australia/Perth')::date` on the 8-hour Perth/UTC offset. Replace UTC bucketing with `ts.astimezone(ZoneInfo('Australia/Perth')).date()` so the fake mirrors production day boundaries exactly.
- Add self-tests in `TestFakePipelineDBNewStubs` covering: (a) the cache-hit path, (b) the lazy-fill path, (c) **a Perth-boundary correctness test** seeding `observed_at = 2026-05-07 23:55 UTC` (= 2026-05-08 07:55 Perth) and asserting it lands in 2026-05-08's bucket, not 2026-05-07's.
- `TestPipelineDBFakeContract.test_fake_signatures_compatible_with_real` continues to pass because the public signature is unchanged.
- The `make_db()` TRUNCATE extension in `tests/test_pipeline_db.py` is owned by U2; U3 only resets `self.peer_dir_daily_aggregates` in the fake's reset/init path. U3 must not double-touch `tests/test_pipeline_db.py`.

**Patterns to follow:**
- Mirror conventions from `FakePipelineDB.create_successful_search_plan` (PR #225) — typed in-memory state, idempotent inserts.

**Test scenarios:**
- Happy path: fake's response shape matches real method's shape for an empty observation set.
- Happy path: seeding fake observations and calling the method twice yields the same response on the second call without recomputing completed days (assert via internal counter or behaviour proxy).
- Integration: existing tests that use `FakePipelineDB` for dashboard-related assertions still pass without modification.

**Verification:**
- `nix-shell --run "python3 -m unittest tests.test_fakes -v"` passes.
- The full `tests.test_pipeline_db` suite passes as a regression check (requires U2 complete — implied by `Dependencies: U2`; ensures fake-vs-real drift is caught).

---

### U4. Deploy + production verification

**Goal:** Ship the change to doc2 and confirm the perf win in production.

**Requirements:** R1.

**Dependencies:** U1, U2, U3.

**Files:**
- None (deploy via `/deploy` skill + flake bump on doc1).

**Approach:**
- Standard cratedigger deploy flow: commit + push the branch, merge PR, `nix flake update cratedigger-src` on doc1, `nixos-rebuild switch` doc2, watch the migrate unit succeed.
- First post-deploy dashboard load is expected to take ~3.6 s (one-time backfill of 13 historical days); record the exact wall time.
- Subsequent loads should be sub-second; record at least three samples.
- Confirm `peer_dir_daily_aggregates` row count matches the count of completed Perth-local days within the window after the first load.
- Update issue #227 with before/after timings and close the Q2 portion. Q1 stays open in #227 for follow-up.

**Test scenarios:**

Test expectation: none — deploy step.

**Verification:**
- `ssh doc2 'pipeline-cli query "SELECT version, name, applied_at FROM schema_migrations ORDER BY version DESC LIMIT 5"'` shows migration 015 applied.
- `ssh doc2 'pipeline-cli query "SELECT day, new_combos, new_peers, new_dirs FROM peer_dir_daily_aggregates ORDER BY day DESC"'` shows N completed-day rows where N matches the window.
- `time curl -sf https://music.ablz.au/api/pipeline/dashboard --max-time 30 | wc -c` returns sub-second on warm hits.
- Issue #227 comment with before/after: cold ≈ 7 s → first-post-deploy ≈ 3.6 s → warm ≈ <1 s.

---

## System-Wide Impact

- **Interaction graph:** The dashboard route, the importer-cycle code path, and the search-plan inspection route all share `PipelineDB`. Only `get_pipeline_dashboard` consumes `get_peer_dir_daily_metrics`; the change is confined.
- **Error propagation:** A DB error during the backfill propagates to the route handler as it does today (raises a 500). The `ON CONFLICT DO NOTHING` strategy means no partial-write recovery is needed.
- **State lifecycle risks:** The cache table grows by one row per completed day. After 365 days the cache holds 365 rows total — bounded.
- **API surface parity:** Public method signature and response shape unchanged. JS consumer untouched. Contract tests in `tests/test_web_server.py` for the dashboard route remain green.
- **Integration coverage:** Add at least one integration scenario where the same `PipelineDB` instance services a dashboard call after a fresh observation insert across the day boundary, to prove the live-today-row computation reflects the latest state.
- **Unchanged invariants:** `peer_dir_observations` schema, `record_peer_dir_observations` write path, dashboard route shape, JS consumer field set, Q1 totals query, autocommit discipline.

---

## Risks & Dependencies

| Risk | Mitigation |
|------|------------|
| Cache PK uses UTC instead of Perth-local date, silently mis-bucketing 8 h of rows per day boundary. | Explicit Key Technical Decision; Perth-boundary test in U2's scenarios. |
| First-deploy backfill is itself a full scan and adds load on a freshly-restarted web service. | One-time cost; bounded to ~3.6 s (current Q2 latency). U4 records the exact wall time so we know if it regresses. |
| Race between two concurrent dashboard requests both trying to backfill the same days. | `INSERT ... ON CONFLICT DO NOTHING` makes each insert idempotent; the second request's writes are no-ops. Tested in U2 integration scenarios. |
| Forgotten TRUNCATE entry breaks test isolation silently — leftover cache rows from one test bleed into another. | U2 explicitly extends `make_db()` TRUNCATE list; verification step asserts test isolation by running the suite. |
| FakePipelineDB drifts from real behavior because the cache table is not part of its state. | U3 adds the cache to the fake's state and the parity contract test catches signature drift; behavioral self-test guards shape correctness. |
| Public response shape accidentally changes (e.g., extra `created_at` field leaks into the response dict). | Contract regression test in U2 pins the exact dict keys. |
| Day rollover at Perth midnight could leave today's row staler than expected if the Perth-local date is computed inconsistently. | Single source of truth: compute `today_perth` once at the top of the method and use it for all branches. |

---

## Documentation / Operational Notes

- After deploy, comment on issue #227 with the before/after wall-times and link to the merged PR. Close the Q2 portion; leave Q1 open.
- No user-facing docs to update — the dashboard's UX is unchanged.
- No runbook changes — the new migration runs as part of the existing `cratedigger-db-migrate.service` deploy unit.

---

## Sources & References

- **Origin issue:** [#227 — Pipeline dashboard perf](https://github.com/abl030/cratedigger/issues/227)
- **Related PR:** [#225 — Persist per-request search plans](https://github.com/abl030/cratedigger/pull/225) (introduced the `psycopg2.extras.execute_values` + `Json` patterns this plan reuses)
- **Migration prior art:** `migrations/014_persisted_search_plans.sql`, `migrations/012_peer_dir_observations.sql`
- **DB pattern prior art:** `lib/pipeline_db.py::create_successful_search_plan`, `lib/pipeline_db.py::record_peer_dir_observations`
- **Fake parity prior art:** `tests/test_fakes.py::TestPipelineDBFakeContract`
- **Solution doc:** `docs/solutions/architecture/multiplexed-postgres-client-and-set-local-incompatibility.md`
- **Repo rules:** `.claude/rules/pipeline-db.md`, `.claude/rules/code-quality.md`, `.claude/rules/nix-shell.md`
