---
date: 2026-05-09
topic: cratedigger-web-rust-rewrite
supersedes: 2026-05-09-web-stack-bottle-waitress-requirements.md
status: superseded
superseded_by: ../solutions/web-rewrite-deferred-pending-runtime-redesign.md
---

# Cratedigger-Web — Rewrite in Rust (axum + sqlx)

> **Superseded.** ce-doc-review on this doc surfaced a P0 finding (feasibility persona): R11's "all 52 routes ported" silently included ~4,000 LOC of pipeline business logic that web POST handlers orchestrate from `lib/*` (`manual_import`, `import_queue`, `wrong_match_triage`, `import_preview`, `search_plan_service`, `audio_hash`, `release_cleanup`, etc.). The web layer is not the autonomous facade this doc treated it as; carving it out into a separate language requires either re-deriving the lib code (forgets accumulated bug-fixes from many incidents) or designing a new IPC boundary (new design + maintenance + cross-language debugging). Neither is a "rewrite the web layer" project — both are cratedigger-runtime-redesign projects with the web rewrite as a beneficiary. The active decision lives at `docs/solutions/web-rewrite-deferred-pending-runtime-redesign.md`: apply a minimal Python patch to kill the current wedge; defer any rewrite to be part of a future cratedigger runtime redesign (cycle-driven → static / event-driven). This doc is retained for context only — the wedge-impossibility design exploration, the axum + sqlx + rusqlite + fred research, and the parallel-run bake-in shape are useful prior art for the future rewrite when its preconditions hold.

## Summary

Rewrite `cratedigger-web` from Python (stdlib `http.server`) to Rust (axum 0.8 + tokio + sqlx for PostgreSQL + rusqlite for the beets read-only sqlite + fred for Redis + reqwest with rustls for MB/Discogs proxying + tracing + crane for the Nix build). The pipeline (`cratedigger.py` and the importer worker) stays in Python — beets is Python and that's not changing. The two processes communicate only via shared PostgreSQL, shared sqlite (read-only), shared Redis, and shared filesystem; no direct IPC. The 52 routes ship in one all-at-once binary, parallel-deployed alongside the Python service on a different port for a 1-week bake-in before cut-over. A small Python stop-gap patch ships first to stop the wedge while the rewrite is in progress.

---

## Problem Frame

`cratedigger-web` wedged silently for ~9 hours on 2026-05-08 (issue #233). systemd reported `active (running)`, the listener still accepted TCP, but every HTTP request hung forever until manual `systemctl restart`. Forensics traced the wedge to a sustained reconnect storm: the cratedigger main loop's end-of-cycle `POST /api/cache/invalidate` (`cratedigger.py:1610`) closes the socket mid-body before the server finishes writing the response, the `do_POST` catch-all treats the resulting `BrokenPipeError` as a database error and unconditionally tears down + reopens the psycopg2 connection, and ~3 unnecessary reconnects per minute compound until the single-threaded server thread blocks indefinitely in `poll()`. Sibling issue #227 had been tracking the BrokenPipe count climbing as a leading indicator.

The first attempt at a fix migrated to Bottle + waitress (`docs/brainstorms/2026-05-09-web-stack-bottle-waitress-requirements.md`). The ce-doc-review pass on that doc surfaced two structural problems with the design:

1. waitress has **no per-request timeout** — only `channel_timeout` for idle clients. The Key Decisions rationale that "waitress's request timeout makes the wedge unreachable" was factually wrong.
2. The per-thread psycopg2 connection model needed real engineering — connection-on-thread-exit cleanup, dead-connection detection beyond `conn.closed`, advisory_lock invariant inversion, autocommit enforcement — none of which the Bottle migration trivially solved.

Reconsidering the framework choice opened a wider question: why stay in Python for the web layer at all? The web UI doesn't touch beets directly; it reads `beets-library.db` as a sqlite file. The beets-as-Python constraint lives in `cratedigger.py`, not here. Going Rust replaces "engineer wedge-resistant request handling" with "use the standard tokio + tower middleware that gives wedge-impossible request handling as a property of the runtime."

The cost is a meaningful rewrite (~52 routes, 3 weeks focused). The benefit is that wedging stops being a class of failure that exists, plus end-to-end type safety (sqlx compile-time SQL validation, serde JSON contracts, rustc enforcing extractor types) that Python + pyright cannot match.

---

## Requirements

**Stack and runtime**

- R1. The web service is a single Rust binary, deployed via Nix on doc2. Built with `crane` (current consensus 2026 nixpkgs Rust integration). systemd unit stays `Type=simple` + `Restart=on-failure` + `RestartSec=5`.
- R2. The web framework is **axum 0.8**. The async runtime is **tokio** (multi-threaded). The middleware layer is **tower-http** for `TimeoutLayer`, `TraceLayer`, `CorsLayer`, `ConcurrencyLimitLayer`, and `ServeDir` for static files.
- R3. **PostgreSQL access uses `sqlx`** (with `postgres`, `runtime-tokio`, `tls-rustls`, `macros`, `migrate`, `json`, `time`, `uuid` features). Compile-time SQL validation via `query!`/`query_as!` macros, with offline-mode `.sqlx/` directory committed to the repo so Nix builds work without DB access (`SQLX_OFFLINE=true` in the build environment).
- R4. **Beets `library.db` access uses `rusqlite`** (read-only mode, WAL-aware, `busy_timeout(5s)`), wrapped in `tokio::task::spawn_blocking`. Pool size 4 via `r2d2`/`r2d2_sqlite`. Not `sqlx::Sqlite` — sqlx's single-shared-connection model composes badly with WAL + an external writer (beets touches the file occasionally).
- R5. **Redis access uses `fred`** with `enable-rustls`. Built-in pool with native reconnection handling (matters when Redis flaps during a `nixos-rebuild switch`).
- R6. **HTTP client for MB/Discogs mirror proxying uses `reqwest`** with `rustls-tls`, no OpenSSL transitive dep. Per-client timeouts: `timeout(10s)`, `connect_timeout(2s)`, `pool_idle_timeout(60s)`. One `reqwest::Client` in `AppState`, cheaply cloned per request.

**Concurrency and wedge-impossibility properties**

- R7. **Per-request timeout is enforced by the runtime**, not by application code. `tower_http::timeout::TimeoutLayer::new(Duration::from_secs(30))` as an outer layer aborts the request future at the timeout. PG `statement_timeout` is set on the pool connect options (20s) as a defence-in-depth lower bound. Both are configured; neither relies on the other being missing.
- R8. **Pool exhaustion is bounded by middleware, not by infinite queueing.** `tower::limit::ConcurrencyLimitLayer::new(40)` caps inflight requests before they reach the pool. `sqlx::PgPool::max_connections(20)` + `acquire_timeout(5s)` caps the database side. Excess requests get `503` fast, not infinite queue.
- R9. **No global mutable state.** `AppState` is constructed once in `main`, `Arc`-shared via the `State<Arc<AppState>>` extractor. No re-implementation of `_try_reconnect_db` — sqlx's pool detects broken connections, evicts them, and opens new ones on next acquire. Application code maps `sqlx::Error::Io` to a 503 via `AppError::IntoResponse`.
- R10. **Handler panics are contained.** tokio's task runtime catches panics; axum returns a 500 to the affected client; other tasks are unaffected. Application code uses `?` on a single `AppError` enum (built with `thiserror`) — no `unwrap`/`expect` on user input.

**Routes and route porting**

- R11. **All 52 existing routes are ported with functional parity.** Browse (12), labels (2), library (4), imports (12), pipeline (22). No new endpoints, no UI behaviour changes, no auth. The cratedigger main loop's `POST /api/cache/invalidate` call (`cratedigger.py:1610`) is deleted; the corresponding route is not implemented in Rust.
- R12. **JSON field naming is snake_case** end-to-end. Every public-facing struct carries `#[serde(rename_all = "snake_case")]` (the existing JS expects snake_case; rustc cannot catch a mismatch).
- R13. **Static assets** (`web/index.html`, `web/js/*.js`, any CSS) are served via `tower_http::services::ServeDir` mounted at `/`. No build step, no npm — the existing vanilla-JS frontend ships unchanged.
- R14. **Audio file Range streaming** (`/api/wrong-matches/audio/{id}` and any other range routes) uses a hand-rolled handler (~80 lines) backed by `tokio::fs::File` + `seek` + `tokio_util::io::ReaderStream`. Required behaviour preserved from the existing Python handler: `Accept-Ranges: bytes`, `Content-Range` on 206 and 416, suffix-range support (`bytes=-N`), explicit single-range-only (multi-range rejected with 416), `Cache-Control: no-cache`, `Access-Control-Allow-Origin: *` (load-bearing for the dev tunnel proxy). Reference: `axum-range` source.
- R15. **CORS policy** preserves today's `Access-Control-Allow-Origin: *` plus `OPTIONS` handling. Implemented via `tower_http::cors::CorsLayer::permissive()` on `/api/*` routes (or stricter if planning identifies a tightening opportunity).

**Local-dev, test, and observability**

- R16. **Local-dev iteration loop** is `cargo watch -x run` inside `nix develop`. Cold compile of the full stack is 3-5 minutes (one-time pain); incremental compile is 5-15 seconds. The existing `scripts/web_dev_server.py` workflow is replaced — the Rust binary serves both production and dev. The `--data prod-api` proxy mode (currently used to point a local frontend at a remote backend) is reimplemented as a CLI flag on the Rust binary.
- R17. **Test discipline** — four layers, each with first-class Rust tooling:
  - **Unit tests** — colocated `#[cfg(test)] mod tests` per module. Pure logic only.
  - **Route-level tests** — call `app.clone().oneshot(Request)` directly via `tower::ServiceExt`. No port binding, no HTTP transport. The contract-test concept (assert response shape includes required fields) is preserved, but stronger: deserialize as the typed response struct and let the compiler enforce the contract.
  - **DB integration tests** — `sqlx::test` macro creates a per-test database via PG template, runs migrations, hands the test a `PgPool`, drops at end. Requires a Postgres for testing (already available; existing `migrations/NNN_name.sql` files run unchanged).
  - **HTTP client mocking** — `wiremock-rs` for MB/Discogs mirror proxy tests.
- R18. **Route audit** — preserve the `TestRouteContractAudit` discipline that fails fast if a new route ships unclassified. Implemented as a manual `const ROUTES: &[(Method, &str)]` registry that's both the source of registration AND the source of audit truth, OR via wrapping the `Router` to expose registered paths for introspection. Either approach is acceptable; planning picks one.
- R19. **Wedge-class regression test** — a test asserts that a client closing the socket mid-response produces no journald traceback, no DB reconnect, no second body-write attempt, and the worker thread is immediately available for the next request. Implementation: raw `TcpStream` from a test, send headers + partial body, close, then assert another concurrent `reqwest` GET to a different endpoint completes normally.
- R20. **Observability** is `tracing` + `tracing-subscriber` with `EnvFilter` (RUST_LOG) and JSON formatter to stdout. journald reads JSON from stdout; `journalctl -o json | jq` works for free. `tower_http::trace::TraceLayer` gives per-request span (method, path, status, latency).

**Cut-over**

- R21. **Parallel-run bake-in.** The Rust binary deploys on a new port (e.g. `:8086`) alongside the Python service on `:8085`. A diff harness (small standalone script) hits both with the same request and compares JSON responses for a representative set of routes. Runs for at least 1 week. Once diffs are zero or expected (e.g. behaviour-changes the new design intends), the cut-over flips `music.ablz.au` to the Rust binary's port and the Python service is removed.
- R22. **Stop-gap Python patch ships first**, before the rewrite work begins, as a separate small PR against the current Python stack:
  - Delete the end-of-cycle `urlopen` to `/api/cache/invalidate` in `cratedigger.py:1610`.
  - Catch `BrokenPipeError` / `ConnectionResetError` / `ConnectionAbortedError` at the response layer of `web/server.py:do_GET`/`do_POST`; single-line warning; no DB reconnect; no second body-write.
  - The existing `_try_reconnect_db()` only fires on `psycopg2.OperationalError`, not on the catch-all.
  - Optional: switch from `HTTPServer` to `ThreadingHTTPServer` so a single slow request doesn't block all routes (low risk, two-line change, but verify no route depends on single-threading first).
  - Total: ~50 lines, an afternoon's work, kills the wedge while the Rust rewrite is in progress. No need for a separate brainstorm — ships as a `ce-debug` fix.

**Documentation and rule updates**

- R23. **`.claude/rules/web.md` is rewritten** to replace "stdlib `http.server`, vanilla JS, no build step, no npm" with the new stack: "axum + sqlx + rusqlite + fred web service in Rust, vanilla JS frontend (no npm, no build step), routes in `src/routes/*.rs`". CLAUDE.md's Web UI bullet is updated correspondingly. Lands as part of the Rust rewrite PR.
- R24. **`docs/solutions/`** gains an entry capturing the wedge → ce-doc-review → Rust pivot story for future agents. Lessons: (a) the Bottle+waitress decision was a recommendation made on a wrong assumption (waitress's timeout primitive); (b) ce-doc-review caught it before implementation; (c) framework choice should be re-pressure-tested after a researched ecosystem report.

---

## Acceptance Examples

- AE1. **Covers R7, R9, R10, R19.** A client sends a `POST /api/library/<existing real route>`, reads the status line + headers, then closes the socket before the body finishes streaming. Server-side: no traceback in journald, no DB reconnect, no `download_log` row, the worker that handled the request is immediately available. A concurrent `GET /api/pipeline/dashboard` from a different client completes normally with no degradation.
- AE2. **Covers R7, R8, R9.** A handler invokes `pool.fetch_one(...).await?` and PostgreSQL has been restarted out from under the connection. sqlx's pool detects the broken connection, evicts it, returns `Error::Io` to the handler. The handler maps to `AppError`, returns 503. The next request to the same handler acquires a fresh connection from the pool and succeeds. No global reconnect path was traversed.
- AE3. **Covers R11, R22.** After R22 ships, the cratedigger main loop completes a cycle. The end-of-cycle `finally` block does not POST to the web service. journald shows zero `BrokenPipeError` lines from the cycle.
- AE4. **Covers R14.** The Wrong Matches UI plays an audio file via the dev-server tunnel (`scripts/web_dev_server.py --data prod-api` or its Rust equivalent). The handler responds 206 with correct `Content-Range`, suffix-range support works, multi-range requests are rejected with 416 + `Content-Range: bytes */<size>`, `Access-Control-Allow-Origin: *` is present, audio plays end-to-end with seeking.
- AE5. **Covers R21.** During the bake-in week, a diff harness queries `/api/pipeline/all` and 5 representative `/api/browse/*` and `/api/library/*` routes against both the Python (`:8085`) and Rust (`:8086`) services. Expected behaviour-changes (e.g. removed routes per R11) are explicitly excluded from the diff. Remaining diffs are zero on at least 50 consecutive runs before cut-over.

---

## Success Criteria

- After cut-over: BrokenPipe count in `journalctl -u cratedigger-web` over a 24-hour window is < 5 (today's baseline is 2,355). The end-of-cycle POST is gone (R22) and disconnect handling is structural (R10).
- After cut-over: zero `cratedigger-web` wedge incidents for 30 days under normal traffic and dashboard polling. A wedge is defined as the service reporting `active` while every HTTP request hangs > 30s.
- A pathological request (mid-body close, stalled downstream call, deliberately slow handler) takes out at most one in-flight request — never the service. Verified by AE1 against the staging instance.
- The Rust binary boots in < 100ms cold (sqlx pool eager-init excluded). Dashboard latency is < 100ms warm, < 500ms cold (same as today's post-#230 numbers, not a regression).
- A downstream agent picking up `ce-plan` against this doc can name the cargo crates, the AppState shape, the route module layout, the systemd unit deltas, and the bake-in cut-over plan without inventing product behaviour, scope, or success criteria.

---

## Scope Boundaries

- Auth or login. UI stays unauthenticated; LAN-only by design.
- Frontend changes of any kind. Vanilla JS, no npm, no build pipeline, no SSR. Existing `web/js/*.js` ships unchanged.
- Migrating other services. The pipeline (`cratedigger.service`), the importer worker (`cratedigger-importer.service`), and the migrator (`cratedigger-db-migrate.service`) all stay Python. Only the web service is rewritten.
- Database schema changes. None needed. Existing `migrations/NNN_name.sql` files run via `sqlx::migrate!` macro at startup with the same ordering and idempotency guarantees as the Python migrator.
- New observability surfaces (Prometheus, structured logs, request tracing). `tracing` to journald is the baseline; revisit if specific signals become operationally needed.
- New runtime dependencies beyond the recommended cargo set in this doc. Auxiliary axum plugins, alternative DB drivers, alternative HTTP clients are out of scope. The route layer remains thin Rust over sqlx + rusqlite.
- A separate signal-based watchdog. tokio's runtime + `TimeoutLayer` is the watchdog.
- Type-state encoding of domain invariants (e.g. a beautiful `AlbumRequest<Wanted, Validated, Imported>` state machine). Existing string-status enum semantics ship unchanged via a `#[derive(Debug, Display, FromStr)]` enum. Keep the rewrite small.
- Async ORM. No `diesel-async`, no `sea-orm`. Raw SQL via sqlx is the chosen ergonomic.
- A dashboard caching wrapper (the residual #227 Q1 work). Tracked separately as a follow-on once the Rust web service is stable; deliberately not bundled with the rewrite. The Q1 cache wrapper is a different concern (perf optimization with explicit invalidation contract) and would muddy the rewrite's success-signal attribution.

---

## Key Decisions

- **axum 0.8 over actix-web, rocket, poem, salvo.** Owned by the tokio team, default modern choice, cleanest tower middleware composition, native testing story (`Router` is a `Service`, callable in tests without port binding). actix-web wins benchmarks but uses its own actor model rather than pure tower; for a 50-RPS homelab service the framework choice is dominated by ergonomics and ecosystem, not by raw throughput. rocket's cadence is sluggish; poem's ecosystem is smaller.
- **sqlx over tokio-postgres + deadpool.** Compile-time SQL validation via `query!`/`query_as!` is the type-safety win that motivated the rewrite. Offline mode (`.sqlx/` directory) makes Nix builds work without runtime DB access. Trade-off: sqlx macros can't handle dynamic `IN (...)` lists — use `ANY($1)` against an array parameter or `UNNEST` for those.
- **rusqlite over sqlx::Sqlite for the beets DB.** Read-only access to a file an external process (beets) writes occasionally needs explicit WAL handling, `SQLITE_OPEN_READ_ONLY`, and `busy_timeout`. rusqlite gives that with no surprises. Wrap calls in `spawn_blocking` so they don't park the executor.
- **fred over redis-rs for Redis.** Built-in async pool with reconnection handling. Matters because Redis flaps during `nixos-rebuild switch` on doc2.
- **reqwest with rustls, no OpenSSL.** Drops the C dep transitively; pure-Rust crypto; trivial Nix build.
- **crane over buildRustPackage / naersk for the Nix integration.** Fine-grained dep caching, current 2026 consensus, well-maintained, integrates cleanly with `rust-overlay` for toolchain pinning and with `cargo sqlx prepare` for offline mode.
- **All 52 routes in one PR, parallel-run bake-in.** A staged migration would require maintaining two services and two route registries simultaneously. Instead, ship the Rust binary on a new port, run both for a week, diff with a harness, cut over. The Python service is the rollback target during the bake-in window.
- **Stop-gap Python patch ships first as a separate PR.** Eliminates the wedge while the rewrite is in progress. ~50 lines, an afternoon. No reason to make the user suffer the wedge for 3 weeks.
- **Pipeline stays Python.** beets is Python; `cratedigger.py` invokes the beets harness via subprocess. The rewrite is scoped to the web UI only. Polyglot codebase is accepted (clean process split, no IPC except via shared DB/Redis/disk).
- **No dashboard cache wrapper in this PR.** The Q1 follow-up from #227 is a perf optimization with its own design questions (cache invalidation contract, freshness window, IPC from importer to web cache). Bundling it with the rewrite muddied the original Bottle+waitress doc and would muddy this one. Tracked as separate follow-up.

---

## Dependencies / Assumptions

- **Rust toolchain available via nixpkgs / rust-overlay.** Stable channel, recent (1.85+ for `let-else` and other niceties). Verified before implementation begins.
- **All recommended crates available on crates.io and pinned in `Cargo.lock`.** axum 0.8, sqlx 0.8, rusqlite 0.32, fred 9, reqwest 0.12, tower 0.5, tower-http 0.6, tracing 0.1, tracing-subscriber 0.3, thiserror 1, time 0.3, serde 1, serde_json 1, wiremock 0.6 (dev). Versions chosen for Apr 2026 ecosystem state per the research report; planning re-validates against current `cargo search` output.
- **The existing `migrations/NNN_name.sql` files are sqlx-compatible.** They use plain SQL (no Python-specific syntax). `sqlx::migrate!` uses the same NNN ordering convention. Verified before implementation begins.
- **PostgreSQL `max_connections` (default 100) accommodates the new pool.** With Rust web's pool max 20 + Python pipeline's connections + importer worker's connections + dev sessions, total stays well under 100.
- **PostgreSQL hostname/credentials** stay the same. The new binary reads `PIPELINE_DB_DSN` from systemd environment exactly like the Python service today.
- **`scripts/web_dev_server.py`'s proxy mode** has a Rust equivalent. The current proxy mode (`--data prod-api`) is implemented as a CLI flag on the Rust binary that mounts a `reqwest`-backed proxy handler at `/api/*`, otherwise serves local static files. This collapses the dev server into a single binary mode rather than a separate Python tool.
- **`cache.invalidate_groups` Python helper has no real internal callers** beyond the deleted HTTP endpoint. Verified during ce-doc-review on the prior brainstorm — the function is dead code post-R22. The Python pipeline's web/cache.py module is removed once the cut-over completes.

---

## Outstanding Questions

### Resolve Before Planning

- [Affects R17][User decision] What's the test-data strategy for `sqlx::test` macros — does the per-test database get migrations + a shared seed-data fixture (faster, seeds drift), or migrations + per-test inserts (slower, isolated)? The existing Python tests use `tests/fakes.py` `FakePipelineDB` for stateful collaborators; sqlx::test against a real PG is a different model and worth picking explicitly.
- [Affects R21][User decision] How long does the parallel-run bake-in last? "At least 1 week" is the floor; the user may want longer (2-4 weeks) for a curated music collection where data-correctness regressions could go unnoticed for days.

### Deferred to Planning

- [Affects R2][Technical] Worker thread count for tokio's runtime — default is `num_cpus`, doc2 has N cores. Is the default fine, or should it be capped to avoid contention with the importer worker on the same host? Likely default-is-fine; verify during planning.
- [Affects R3][Technical] sqlx pool sizing: `max_connections(20)` is a starting point. Tune during bake-in based on observed concurrent request count + query latency.
- [Affects R7, R8][Technical] TimeoutLayer + ConcurrencyLimitLayer values: 30s timeout, 40 inflight. Reasonable starting points; verify against observed behaviour during bake-in.
- [Affects R11][Technical] The two existing Python regex routes (`_FUNC_GET_PATTERNS`, `_FUNC_POST_PATTERNS`) need axum-equivalent path patterns. axum 0.8's `{param}` and `{*rest}` syntax handles most cases; one or two routes might need a custom `FromRequest` extractor. Verify each translates cleanly.
- [Affects R14][Technical] Audio Range handler can either be inlined (~80 lines) or pulled in via the `axum-range` crate. Inlining is the recommended default per the research report (the dependency surface is small, the logic is readable). Decide during planning.
- [Affects R16][Technical] Should the dev server's `--data prod-api` proxy mode use a separate axum app or a runtime flag on the production app? Both work; runtime flag is one process, separate app is cleaner. Decide during planning.
- [Affects R18][Technical] Route audit pattern: manual `const ROUTES` registry vs `Router` introspection. Decide during planning based on which is more ergonomic against axum 0.8's type system.
- [Affects R20][Technical] Does `tracing-subscriber`'s JSON formatter give the per-field structure journald wants, or is `tracing-journald` (explicit journald sink) materially better? Default to JSON-to-stdout; revisit if structured-field queries become important.
- [Affects R22][Technical] Pre-flight check before deploying the stop-gap patch: confirm no other in-process Python code calls `_try_reconnect_db()` outside `do_GET`/`do_POST` catch-alls. Quick grep, but worth doing before the patch lands.

---

## Effort Estimate

Per the Rust ecosystem research report (intermediate Rust dev):

- **Phase 1 — scaffolding** (1-2 days): cargo workspace, axum skeleton, AppState, error type, tracing, sqlx pool, rusqlite handle, fred client, reqwest client, basic Nix flake with crane, one `/health` route end-to-end.
- **Phase 2 — port routes** (1-2 weeks focused, 50-80 hours total): 52 routes. Average ~1 hr/route once patterns are established. Audio streaming and search-plan ops are the slow ones; simple GETs are 30 min.
- **Phase 3 — tests** (3-5 days): port contract tests, add `sqlx::test` integration tests, `wiremock` for proxy routes, route audit equivalent.
- **Phase 4 — Nix integration** (1-2 days): crane build with `SQLX_OFFLINE=true`, systemd unit, deploy to doc2, verify.
- **Phase 5 — bake-in** (1+ week): parallel-run, diff-harness, cut-over.

**Total: ~3 weeks focused full-time, or 4-6 weeks evening/weekend.** Phase 2 dominates and has the highest variance.

The R22 stop-gap is a separate ~half-day of work that ships independently before Phase 1 begins.
