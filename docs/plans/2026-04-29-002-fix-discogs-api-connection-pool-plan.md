---
title: "fix: discogs-api connection pool + safe statement_timeout (P0 redo)"
type: fix
status: implemented
date: 2026-04-29
origin: docs/plans/2026-04-29-001-feat-label-viewer-phase-a-plan.md
---

# fix: discogs-api connection pool + safe statement_timeout (P0 redo)

**Target repo:** `discogs-api` (GitHub: `abl030/discogs-api`, local clone at `~/discogs-api/`). One small change in cratedigger to handle the new 503.

## Overview

Replace the multiplexed `tokio_postgres::Client` shared across Axum handlers with a connection pool (`deadpool-postgres`), then re-introduce a transaction-scoped `statement_timeout` on the recursive sub-label CTE so a UMG-class label cannot hang the cratedigger HTTP server. Deploy the cratedigger-side 503-aware fallback (graceful retry without sub-labels) alongside the new behavior.

**Implementation note:** the final shipped timeout is 15 seconds, not the 5
seconds drafted below. Live doc2 measurement showed UMG completes in about 12
seconds, and the user accepted a 15s budget. The pool also ships with bounded
wait/create/recycle timeouts so saturation returns a typed 503 instead of
waiting indefinitely.

This redoes the work that landed in `cf22645` (broken: serialized all requests through one connection, corrupted session state on timeout) and was reverted in `6053896`. The architectural prerequisite is the connection pool — without it, transaction-scoped `SET LOCAL` is unsafe on a multiplexed client.

---

## Problem Frame

The recursive CTE in `query_label_releases` (`src/db.rs:1036-1072`) walks the `parent_label_id` tree with `UNION ALL` and joins to `release_label`. On UMG-class labels (Universal, Sony, Warner) this is observably slow — 30+ seconds — and there is no upper bound today. The cratedigger web service is a single-threaded `http.server.HTTPServer`; one slow upstream call blocks every other browse-tab request behind it.

The previous fix added `BEGIN; SET LOCAL statement_timeout='5s'; ... COMMIT;` around the CTE in the Rust mirror. It broke production within minutes:

- `tokio_postgres::Client` is *multiplexed* — pipelined queries share a single TCP connection. Opening an explicit transaction serializes ALL concurrent requests through that connection (no isolation between handlers).
- When a query inside the transaction times out (`query_canceled`, SQLSTATE 57014), the session can be left in an `aborted` state. Subsequent requests on the same client see "current transaction is aborted" until the connection is reset.
- Live verification on 2026-04-29 caught the mirror hanging on every request; revert shipped 18 minutes after the broken commit.

The current state is "no upper bound, theoretical DoS, real responsiveness problem on big labels." Cratedigger has a heuristic auto-flip (`bf2d929`) that turns off `include_sublabels` for labels with `release_count > 1000`, but a user can still pass `?include_sublabels=true` explicitly, and the flip is a workaround rather than a hardening.

The proper fix needs:
1. A connection pool — each request acquires its own connection, so a transaction (and a timeout, and an aborted session) is scoped to one request.
2. The transaction + `SET LOCAL statement_timeout` reapplied on the recursive CTE only.
3. A graceful 503 mapping when the timeout fires.
4. Cratedigger-side fallback when it sees a 503 from the mirror.

Origin: post-merge follow-up section of `docs/plans/2026-04-29-001-feat-label-viewer-phase-a-plan.md` (P0 entry, lines 513-514).

---

## Requirements Trace

- R1. Multiplex client replaced with a per-request connection pool. → U1
- R2. `query_label_releases` (recursive branch) wraps its query in `BEGIN; SET LOCAL statement_timeout='15s'; ... COMMIT;` on a pool-acquired connection. → U2
- R3. Timeout maps to HTTP 503 (`SERVICE_UNAVAILABLE`) with a typed error body the client can branch on. → U2
- R4. Cratedigger handles 503 from `get_label_releases` by retrying once with `include_sublabels=false` and surfacing a banner. → U3
- R5. `nix flake check` (or equivalent) and a smoke test against a real label confirm: small labels OK, UMG-class labels complete or degrade within the accepted 15s recursive budget, and one timeout does not impact subsequent requests. → U4

**Non-requirements:** Replacing `UNION ALL` with `UNION` (cycle guard) and the nullable `String → Option<String>` field corrections live in the P1 plan (`docs/plans/2026-04-29-003-fix-label-viewer-p1-p2-plan.md`). They are independent and can land before, during, or after this plan.

---

## Scope Boundaries

- Pool covers all existing Axum handlers — `health`, `get_release`, `search_releases`, all artist endpoints, all label endpoints. Not just the label routes.
- Pool size, idle timeout, and acquisition timeout are tuned once with sensible defaults; deep load-testing is out of scope for v1.
- The `query_label_releases` recursive branch is the only call site that gets a transaction-scoped statement_timeout in this plan. Other long queries (artist releases on prolific artists) keep current behavior; they are not load-bearing on the cratedigger UI.
- No DB schema changes. No new SQL indexes. No `CYCLE` clause changes (P1 plan).
- No frontend changes beyond the cratedigger 503 handling — UI surfaces remain as designed.
- The `parent_label_id` cycle guard belongs to P1, not here. A 5s timeout will catch a runaway cycle before it eats the connection, so this plan is safe to ship without P1.

---

## Context & Research

### Relevant Code and Patterns

**Current state (single multiplexed client):**

`src/server.rs:25-35`:
```rust
struct AppState {
    client: tokio_postgres::Client,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    let args = Args::parse();
    let client = db::connect(&args.dsn).await?;
    let state = Arc::new(AppState { client });
```

`src/db.rs:15-23` — `connect()` spawns the connection task:
```rust
pub async fn connect(dsn: &str) -> anyhow::Result<Client> {
    let (client, connection) = tokio_postgres::connect(dsn, NoTls).await?;
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            tracing::error!("postgres connection error: {e}");
        }
    });
    Ok(client)
}
```

**Handler pattern to update at every call site** (`src/server.rs:237-245`):
```rust
async fn get_label(
    State(state): State<Arc<AppState>>,
    Path(id): Path<i32>,
) -> Result<Json<LabelDetail>, StatusCode> {
    match db::query_label(&state.client, id).await {
        Ok(Some(l)) => Ok(Json(l)),
        Ok(None) => Err(StatusCode::NOT_FOUND),
        Err(e) => { tracing::error!("label query error: {e}"); Err(StatusCode::INTERNAL_SERVER_ERROR) }
    }
}
```

The `&state.client` borrow becomes `&state.pool.get().await?` (or however `deadpool-postgres` returns it).

**Recursive CTE (the call site that needs the transaction wrapper)** — `src/db.rs:1014-1141`. The recursive variant lives at lines 1036-1072. Quoted in `Open Questions` below for U2.

**CLAUDE.md constraints** (`~/discogs-api/CLAUDE.md`):
- Tests: only XML parsers are unit-tested; DB layer verified against live instance.
- Deploy flow: push → flake.lock bump on doc1 → `nixos-rebuild switch --flake github:abl030/nixosconfig#doc2`.
- Line 73 currently asserts: *"Single PG connection: The API server uses one tokio_postgres::Client (multiplexed). No pool needed for this traffic level."* This claim must be updated by U1.

**Cratedigger consumer** — `web/discogs.py:510-579::get_label_releases` calls `_get(...)` which uses `urllib.request.urlopen(...)` with a 60s timeout. If the upstream returns 503 we currently raise `urllib.error.HTTPError`; `web/routes/labels.py:111-128` catches only 404. The 503 path falls through and surfaces as a 500 to the browser.

`web/routes/labels.py:122-123`:
```python
    releases_resp = discogs_api.get_label_releases(
        label_id, include_sublabels=include_sublabels)
```

`web/js/labels.js:308-347::openLabelDetail` already has a "big label" branch that flips `include_sublabels=false` when the first response succeeds with empty results. We will add a 503 fallback in the Python adapter so the frontend stays unaware.

### Institutional Learnings

- The Phase A plan's risks table flagged "Recursive CTE on deep sub-label trees performs poorly" with `EXPLAIN ANALYZE` as the verification gate. That gate was met for happy-path labels at v1 ship time but not for UMG-class.
- The `cf22645 → 6053896` arc is documented in the Phase A plan's "P0 — REVERTED" section. The lesson is concrete: do not mix multiplexed clients with session-scoped state. Capture this in `docs/solutions/` once this plan ships (Documentation Plan below).

### External References

- `deadpool-postgres` README — first-class `tokio-postgres` integration. Maintained by the same author as `tokio-postgres` (sfackler / bikeshedder collaboration history). Sane defaults for `Pool::builder().max_size(N)`.
- `bb8` — older alternative; works but the `bb8-postgres` adapter is less idiomatic for `tokio-postgres` 0.7. Recommend deadpool.
- PostgreSQL docs on `SET LOCAL statement_timeout` — must be inside an explicit transaction; resets on transaction end. This is exactly what we want once the connection is per-request.

---

## Key Technical Decisions

- **`deadpool-postgres` over `bb8`**. Better tokio-postgres ergonomics, simpler config (`Pool::builder()`), no extra wrapping types, well-maintained. `bb8` would also work but adds boilerplate. If `deadpool-postgres` introduces a transitive dep collision with the existing tree, fall back to `bb8-postgres`; this is a small Cargo.toml decision, not a plan-level pivot.
- **Pool defaults: `max_size = 16`, no statement-cache.** The mirror is single-tenant (cratedigger) and 16 is comfortably above the 5-min cron's burst. Adjust during U4 if the smoke test reveals contention.
- **Transaction wrapper only on the recursive CTE branch.** Non-recursive `query_label_releases` (lines 1080-1092) keeps the simple `client.query()` call. Adding a transaction everywhere costs a round-trip per request for no benefit.
- **5-second `statement_timeout`.** Same as the reverted commit. Fast enough that a hung request doesn't block the cratedigger event loop materially; long enough that a healthy UMG-class CTE on a warm cache should finish (the plan's risks table calls "<500ms on a worst-case label" the target — 5s is a wide safety margin).
- **Typed error path for the timeout.** Reintroduce `LabelReleasesTimeout` (the type that lived in `cf22645`) as a distinct error variant returned from `query_label_releases`. Handler downcasts and maps to `StatusCode::SERVICE_UNAVAILABLE` with a JSON body `{"error": "timeout", "label_id": <id>}`. Other errors keep mapping to 500.
- **Cratedigger-side fallback retries `include_sublabels=false` once on 503.** Mirrors the existing `BIG_LABEL_THRESHOLD` auto-flip semantics. We do not surface a user-visible error on the first 503 if the retry succeeds; we do surface a banner ("Sub-labels temporarily unavailable for this label") if both calls fail.
- **CLAUDE.md update is part of this plan.** The "Single PG connection" assertion in `~/discogs-api/CLAUDE.md` becomes a documented architectural decision in this plan's Documentation Plan section.

---

## Open Questions

### Resolved During Planning

- Pool library choice → `deadpool-postgres` (see Key Decisions).
- Pool size default → 16; adjustable later.
- Transaction wrapper scope → recursive CTE branch only.
- Timeout duration → 5s.

### Deferred to Implementation

- Exact `deadpool-postgres` version pin — pick latest 0.x at U1 time and let `Cargo.lock` settle.
- Whether to expose pool metrics (`pool.status()`) on `/api/health` — defer to a follow-up if observability becomes a concern.
- Whether the cratedigger 503 fallback should be visible in the JSON response (e.g., `{"sub_labels_dropped": true}`) for the UI to badge the page. Likely yes; settle in U3 when wiring.

---

## High-Level Technical Design

> *This illustrates the intended approach and is directional guidance for review, not implementation specification.*

**Pool integration (U1):**

```rust
// Cargo.toml additions (illustrative — exact versions TBD)
deadpool-postgres = "0.x"

// src/main.rs / src/server.rs
struct AppState {
    pool: deadpool_postgres::Pool,
}

let mut cfg = deadpool_postgres::Config::new();
cfg.url = Some(args.dsn.clone());
let pool = cfg.create_pool(Some(deadpool_postgres::Runtime::Tokio1), tokio_postgres::NoTls)?;
let state = Arc::new(AppState { pool });

// Every db function changes from `&Client` to taking a `&Pool` (or
// the function acquires internally). Most read-only handlers do:
async fn query_label(pool: &Pool, id: i32) -> anyhow::Result<Option<LabelDetail>> {
    let client = pool.get().await?;
    // ... existing query code ...
}
```

**Transaction wrapper (U2):**

```rust
async fn query_label_releases(pool: &Pool, params: ...) -> Result<..., LabelReleasesError> {
    let mut client = pool.get().await?;
    if include_sublabels {
        let tx = client.transaction().await?;
        tx.batch_execute("SET LOCAL statement_timeout = '5s'").await?;
        let result = tx.query(SQL_RECURSIVE, &[...]).await;
        match result {
            Ok(rows) => { tx.commit().await?; Ok(rows) }
            Err(e) if is_query_canceled(&e) => {
                // tx is aborted; rollback explicitly to be defensive
                let _ = tx.rollback().await;
                Err(LabelReleasesError::Timeout)
            }
            Err(e) => Err(LabelReleasesError::Db(e)),
        }
    } else {
        // existing non-recursive path, unchanged
    }
}
```

**Cratedigger 503 fallback (U3):**

```python
# web/discogs.py::get_label_releases — pseudo
def get_label_releases(label_id, *, include_sublabels=True, page=1, per_page=100):
    try:
        return _fetch(label_id, include_sublabels, page, per_page)
    except urllib.error.HTTPError as e:
        if e.code == 503 and include_sublabels:
            # one-shot retry without rollup; mark the response
            resp = _fetch(label_id, False, page, per_page)
            resp["sub_labels_dropped"] = True
            return resp
        raise
```

---

## Implementation Units

- U1. **[discogs-api] Replace shared `Client` with `deadpool-postgres` pool**

**Goal:** Every handler acquires a connection from a pool instead of borrowing a shared multiplexed client.

**Requirements:** R1.

**Dependencies:** None.

**Files** *(target repo: discogs-api):*
- Modify: `Cargo.toml` (add `deadpool-postgres`)
- Modify: `Cargo.lock` (regenerate)
- Modify: `src/main.rs` and/or `src/server.rs` (replace `AppState { client }` with `AppState { pool }`; update startup wiring)
- Modify: `src/db.rs` (every `pub async fn query_*(client: &Client, ...)` becomes `pub async fn query_*(pool: &Pool, ...)`; each function calls `pool.get().await?` once at top)
- Modify: every Axum handler in `src/server.rs` to pass `&state.pool` instead of `&state.client`
- Test: live integration check post-deploy (`/api/labels?name=hymen`, `/api/artists/{id}/releases`, `/api/health` — at least one of each route family)

**Approach:**
- Default pool config: `max_size = 16`, no statement cache, `Runtime::Tokio1`. The DSN flag stays the same; `deadpool` reads it from `Config::url`.
- Replace the spawned connection task in `db::connect()` with the pool initialization. Drop the standalone `connect()` function entirely or repurpose it to return a `Pool`.
- For functions that currently take `&Client`, convert to `&Pool` and acquire one connection per call. Consider a small helper `async fn acq(pool: &Pool) -> Result<Object, anyhow::Error>` if it cleans up call sites.
- Sweep every `&state.client` call site — there are at least the health, release, artist, and label families. Use `rg "state\.client"` as the gate.
- The error type doesn't need to change for the simple read paths (`anyhow::Result<T>` is fine; `pool.get().await?` Errors fold into anyhow via `From` impls).

**Patterns to follow:**
- The existing handler pattern at `src/server.rs:237-245` (quoted in Context) — only the inner `&state.client` becomes `&state.pool`.
- Use `deadpool-postgres` README's "Tokio-postgres" example for the `Config` + `create_pool` shape.

**Test scenarios:**
- Per repo convention (CLAUDE.md), no Rust unit tests for handlers. Verification is live:
  - Smoke test: `curl https://discogs.ablz.au/api/health` returns 200.
  - Smoke test: `curl https://discogs.ablz.au/api/labels?name=hymen` returns Hymen Records.
  - Smoke test: 8 concurrent `curl` calls to `/api/labels/{warp_id}/releases?include_sublabels=true` (small label) all complete; observe in `journalctl` that connection acquisition does not serialize them visibly.

**Verification:**
- `cargo build --release` succeeds.
- All existing routes return correct payloads on the deployed mirror.
- `pool.status()` (logged at startup or via tracing) shows healthy pool size.

---

- U2. **[discogs-api] Reintroduce transaction-scoped `statement_timeout` on recursive CTE; map timeout to 503**

**Goal:** When a label-releases query with `include_sublabels=true` exceeds 5s, return HTTP 503 fast instead of hanging, without poisoning other in-flight requests.

**Requirements:** R2, R3.

**Dependencies:** U1 (pool must be in place — this is the architectural prerequisite).

**Files** *(target repo: discogs-api):*
- Modify: `src/db.rs::query_label_releases` (the recursive branch at ~lines 1036-1072 post-merge — wrap in transaction + `SET LOCAL`)
- Modify: `src/db.rs` (add `LabelReleasesError` enum with `Timeout` and `Db(anyhow::Error)` variants, and an `is_query_canceled` helper that checks SQLSTATE 57014)
- Modify: `src/server.rs::get_label_releases` handler (downcast the new error and map `LabelReleasesError::Timeout` → `StatusCode::SERVICE_UNAVAILABLE` with a JSON body)
- Test: live `EXPLAIN ANALYZE` + curl smoke test on a UMG-class label.

**Approach:**
- Acquire connection from pool, open explicit transaction, `tx.batch_execute("SET LOCAL statement_timeout = '5s'")`, run the recursive CTE, commit on success.
- On error, check SQLSTATE: if `57014` (`query_canceled`), rollback the tx defensively and return `Timeout`. Other errors return `Db(e)`.
- Non-recursive branch (`include_sublabels=false`) keeps the simple `client.query()` path on the pool-acquired connection — no transaction.
- The error JSON body is small and stable: `{"error": "timeout", "label_id": <id>}`. The cratedigger client only branches on the HTTP 503 status; the body is informational.
- The reverted code at `cf22645` is a useful reference for the timeout enum and SQLSTATE check shape — re-derive it onto the new pool-based call site rather than cherry-picking, since the surrounding signatures changed in U1.

**Execution note:** Add the transaction wrapper first, deploy, run an `EXPLAIN ANALYZE` against `/api/labels/{umg_id}/releases?include_sublabels=true` to verify the timeout fires inside 5s. Then add the 503 mapping. Two short commits beats one long one for bisecting if anything regresses.

**Patterns to follow:**
- `tokio_postgres::Transaction` API — `client.transaction().await?` returns a `Transaction` that auto-rolls-back on drop. `commit()` is explicit.
- Existing handler error-mapping at `src/server.rs:237-245` — extend with one extra `.downcast_ref::<LabelReleasesError>()` arm before the catch-all 500.

**Test scenarios:**
- Smoke (happy path): `curl https://discogs.ablz.au/api/labels/{hymen_id}/releases?include_sublabels=true` returns 200 with releases (timeout never fires for small labels).
- Smoke (timeout fires): `curl -i -m 10 https://discogs.ablz.au/api/labels/{umg_id}/releases?include_sublabels=true` returns 503 within ~6s. Run twice in a row to confirm the second request is unaffected.
- Smoke (concurrent isolation): launch 4 concurrent curls — 2 against the UMG label (which will 503), 2 against Hymen (should 200). The Hymen calls must succeed within their normal latency. This is the critical regression test for the U1 + U2 combination.
- `EXPLAIN ANALYZE` on the recursive CTE inside a transaction with `SET LOCAL statement_timeout = '5s'` confirms the timeout is observed (the query is cancelled at 5s, not 30s+).

**Verification:**
- `journalctl -u discogs-api` after the concurrent test shows the timeout error logged for UMG, no errors for Hymen.
- A UMG label query reliably 503s within 6s; subsequent unrelated requests (Hymen, health) keep responding normally.

---

- U3. **[cratedigger] Graceful 503 handling in `get_label_releases` adapter**

**Goal:** When the discogs-api mirror returns 503 (timeout), retry once without sub-labels and surface the degradation flag in the response so the route layer can pass it to the UI.

**Requirements:** R4.

**Dependencies:** U2 (the 503 contract must exist).

**Files** *(target repo: cratedigger):*
- Modify: `web/discogs.py::get_label_releases` (catch `urllib.error.HTTPError`, branch on `e.code == 503 and include_sublabels`, retry once, set `sub_labels_dropped=True` on the returned payload)
- Modify: `web/routes/labels.py::get_discogs_label_detail` (forward `sub_labels_dropped` to the response payload so the UI can render a banner)
- Modify: `tests/test_web_server.py::TestLabelRouteContracts` (add `sub_labels_dropped` to `LABEL_DETAIL_RESPONSE_REQUIRED_FIELDS`; add a contract test for the 503 → fallback path using a patched `get_label_releases`)
- Modify: `web/js/labels.js::renderLabelDetail` (when `payload.sub_labels_dropped === true`, render a small banner "Sub-labels unavailable for this label — showing direct releases only")

**Approach:**
- The 503 retry happens at the adapter layer (`web/discogs.py`), not the route layer. Keeps the route handler simple and lets the cache memoize the successful fallback under its own cache key (`include_sublabels=false` cache key is distinct from the failing `include_sublabels=true` key).
- The retry is one-shot — if the no-sublabels call also fails, the original `HTTPError` re-raises and the route returns 5xx.
- `sub_labels_dropped` defaults to `False` on every response so the contract is stable. Only the 503-fallback path sets it `True`.
- The UI banner is a single JS DOM addition — text only, no styling beyond existing `.loading` class.

**Execution note:** Contract tests first (RED) — add `sub_labels_dropped` to the required-fields set, add a unit test for the retry path that patches `_get` to return 503 then 200. Then implement the adapter retry (GREEN). Per `.claude/rules/code-quality.md` API Contract Tests.

**Patterns to follow:**
- `web/discogs.py:466-491::search_labels` for the cache + `_fetch` shape (the retry happens before memoization, so the memoize key matches the eventual successful call).
- `tests/test_web_server.py::TestLabelRouteContracts` (lines 2762-3067) for the contract-test pattern, including `_assert_required_fields`.
- Existing 404 handling at `web/routes/labels.py:111-128` — extend the same try/except to log the 503 fallback explicitly.

**Test scenarios:**
- Happy path: `get_label_releases(small_label, include_sublabels=True)` returns the normal payload with `sub_labels_dropped=False`.
- 503 fallback: patch `_get` to return 503 on the first call (with `include_sublabels=true`) and 200 on the second (with `include_sublabels=false`); assert the returned payload has `sub_labels_dropped=True` and no exception.
- 503 → 503: both calls return 503; assert `urllib.error.HTTPError` re-raises and the route returns 500 (no infinite retry).
- 404 unchanged: 404 still raises and route returns 404 (the existing path).
- Contract: every label-detail response includes `sub_labels_dropped` (default `False`).
- Audit: `TestRouteContractAudit` still passes.

**Verification:**
- All cratedigger contract tests pass.
- After deploy, force a 503 by hitting the UMG label with `?include_sublabels=true`: the cratedigger response is 200 with `sub_labels_dropped=true` and the UI shows the banner.

---

- U4. **[discogs-api + cratedigger] End-to-end smoke test post-deploy**

**Goal:** Verify the full chain — pool, timeout, 503 mapping, cratedigger fallback, UI banner — works on the live mirror.

**Requirements:** R5.

**Dependencies:** U1, U2, U3.

**Files:** None (verification only — no code changes).

**Approach:**
- Deploy discogs-api first (U1 + U2 commits → flake.lock bump on doc1 → `nixos-rebuild switch --flake github:abl030/nixosconfig#doc2`).
- Verify the mirror endpoints with `curl` per U1/U2 verification scenarios.
- Then deploy cratedigger (U3 commit → flake.lock bump → rebuild). The cratedigger `restartIfChanged = false` means the 5-min timer picks up new code; the web service restarts on switch.
- Walk through the UI: open Hymen Records page (smoke), open UMG/Universal page (should fall back gracefully and show banner), refresh Hymen (should still load fast — proves U2 isolation regression test on the live system).

**Test scenarios:**
- Manual: open `https://music.ablz.au`, search "hymen", click result, verify normal page loads with sub-label badges where applicable.
- Manual: open `https://music.ablz.au`, search "universal music group" (or whichever UMG-class label is in the mirror), click result, verify the page loads within ~10s and shows the "Sub-labels unavailable" banner.
- Manual: while a UMG load is in progress, open a second browser tab and load a small label — the second tab must not be blocked by the first.
- Concurrent curl: 2 UMG + 2 Hymen requests fired simultaneously; the Hymen ones complete within their normal SLA (<2s).

**Verification:**
- All three manual walkthroughs pass.
- `journalctl -u discogs-api --since "1 hour ago"` shows the `LabelReleasesError::Timeout` fires for UMG-class only and never for Hymen-class.
- Cratedigger logs show the `sub_labels_dropped` retry firing for UMG-class only.

---

## System-Wide Impact

- **Interaction graph:** Every Axum handler in discogs-api now acquires a connection per request. Cratedigger's `web/discogs.py::get_label_releases` gains a 503-retry branch; `web/routes/labels.py` forwards a new `sub_labels_dropped` field; the JS renderer adds a banner. No changes to beets, pipeline DB, harness, or quality model.
- **Error propagation:** A 503 from the mirror now routes through the adapter retry; only an unrecoverable double-503 surfaces to the browser as a 500. Other upstream errors (404, timeouts at the urllib layer, ECONNRESET) keep current behavior.
- **State lifecycle risks:** The connection-pool migration is the load-bearing risk. Every existing `&state.client` call site must move; missing one would compile but call into a removed field. Use a Cargo build + a sweep on `state.client` references as the gate.
- **API surface parity:** discogs-api adds a new HTTP status (503) on `/api/labels/{id}/releases`. The cratedigger `web/discogs.py` adapter is the only declared consumer of this endpoint, so no third-party contract drift. The cratedigger label-detail JSON gains one optional field (`sub_labels_dropped`); contract test enforces it.
- **Integration coverage:** U4's concurrent-curl scenario is the primary cross-layer guard — it would have caught the original `cf22645` bug.
- **Unchanged invariants:** Beets DB queries, pipeline DB schema, slskd integration, import paths, harness, quality model, all artist-related routes' SQL all stay identical. The pool migration is mechanical at every other call site (only the borrow shape changes).

---

## Risks & Dependencies

| Risk | Mitigation |
|------|------------|
| `deadpool-postgres` pulls in a transitive dep that conflicts with `axum 0.8` / `tokio 1.x` | Verify `cargo build --release` cleanly during U1 before committing. Fallback: `bb8-postgres`. Plan-level decision is small. |
| Pool size of 16 starves under concurrent UMG requests + 5-min cratedigger cron burst | U4 concurrent test would catch this. If so, raise `max_size`. The mirror is single-tenant; no contention from other systems. |
| 5s timeout fires on labels we expected to succeed | U2 smoke includes Hymen + Warp + UMG. If Warp (mid-size) trips the timeout, we have data: either raise the timeout, or accept Warp pages serve via the fallback banner. |
| The `cf22645` enum types and helper (`is_query_canceled`, `LabelReleasesError`) drift from the new pool-based call shape | Treat the reverted commit as reference, not a cherry-pick. Re-derive types against the U1 signatures. |
| Cratedigger's 503-retry creates two cache entries (one for `include_sublabels=true` 503-failed, one for `include_sublabels=false` 200) | Acceptable — a 503 doesn't memoize (it raises), so only the successful fallback caches. The next UMG request will repeat the 503-and-fallback dance, which is the desired behavior until the mirror is faster. |
| Contract test for `sub_labels_dropped` regresses if a future refactor drops the field | `LABEL_DETAIL_RESPONSE_REQUIRED_FIELDS` enforces it; `TestRouteContractAudit` enforces classification. Both are existing guards. |
| Future call site in discogs-api forgets to acquire from pool and tries `state.client` | Field is removed, so the build fails. The pool migration leaves no compatibility shim. |

---

## Documentation / Operational Notes

- Update `~/discogs-api/CLAUDE.md` line 73: replace "Single PG connection ... No pool needed" with the new pool architecture and a one-line note on why (multiplex + session-scoped state are incompatible). Cite this plan.
- Add a new entry to `cratedigger/docs/solutions/` (per `compound-engineering:ce-compound`) documenting the multiplex-vs-transaction lesson — this is exactly the class of architectural tax that compound knowledge captures. Title suggestion: `multiplexed-postgres-client-and-set-local-incompatibility.md`.
- Update `cratedigger/docs/discogs-mirror.md` to note the `/api/labels/{id}/releases?include_sublabels=true` endpoint may now return 503 with a JSON timeout body, and that the adapter handles it transparently.
- No new sops secrets, systemd units, or migrations. Standard deploy flow per `.claude/rules/deploy.md`.

---

## Sources & References

- **Origin plan:** `docs/plans/2026-04-29-001-feat-label-viewer-phase-a-plan.md` (P0 entry, lines 513-514)
- **Reverted commit:** `discogs-api` `cf22645` (broken statement_timeout fix) — useful as a reference for the timeout enum + SQLSTATE check, not for cherry-picking
- **Revert commit:** `discogs-api` `6053896`
- **Plan note commit:** `9a05cb7` (plan update describing the revert and root cause)
- **`deadpool-postgres`:** https://github.com/bikeshedder/deadpool/tree/master/postgres
- **PostgreSQL `SET LOCAL statement_timeout`:** https://www.postgresql.org/docs/current/runtime-config-client.html#GUC-STATEMENT-TIMEOUT
- **Related code:**
  - `discogs-api/src/server.rs:25-35` (current `AppState` shape — to be replaced)
  - `discogs-api/src/db.rs:15-23` (`connect()` — to be replaced or repurposed)
  - `discogs-api/src/db.rs:1036-1072` (recursive CTE branch — gets the transaction wrapper)
  - `cratedigger/web/discogs.py:510-579` (`get_label_releases` — gets the 503 retry)
  - `cratedigger/web/routes/labels.py:111-128` (currently catches 404 only)
  - `cratedigger/tests/test_web_server.py:2762-3067` (`TestLabelRouteContracts` — gets the contract test addition)
