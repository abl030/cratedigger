---
date: 2026-05-09
topic: web-stack-bottle-waitress
status: superseded
superseded_by: 2026-05-09-cratedigger-web-rust-rewrite-requirements.md
---

# Cratedigger-Web — Migrate to Bottle + Waitress

> **Superseded — both this and its successor.** During ce-doc-review, kieran-python found that R3's "waitress enforces request timeouts" claim was factually wrong (waitress has no per-request timeout primitive — only `channel_timeout` for idle clients). That undercut the Key Decisions rationale for picking waitress over gunicorn. Direction pivoted to a Rust rewrite (`2026-05-09-cratedigger-web-rust-rewrite-requirements.md`), which was in turn killed by ce-doc-review when the feasibility persona surfaced ~4,000 LOC of pipeline-lib coupling that POST handlers orchestrate (audio hashing, beets removal, search-plan generation, importer-queue interaction). The web layer is not a clean architectural seam, and no framework migration of just this layer is tractable today. The active decision is captured at `docs/solutions/web-rewrite-deferred-pending-runtime-redesign.md` — apply a minimal Python patch to kill the wedge; defer any rewrite to be part of a larger cratedigger runtime redesign. This doc is retained for context only.

---


## Summary

Replace the homegrown stdlib `http.server` web stack with a Bottle WSGI app hosted under waitress. Worker-thread isolation, per-thread psycopg2 connections, and a real exception model arrive as properties of the host instead of being engineered by hand. All ~52 routes are ported in one PR; the wedge-era defensive scaffolding (`_try_reconnect_db`, the catch-all `do_GET`/`do_POST` exception handlers, the bespoke `_FUNC_*` route registration tables, and the no-op `POST /api/cache/invalidate` round-trip) is deleted as part of the migration. Frontend stays vanilla JS, no npm, no build step.

---

## Problem Frame

`cratedigger-web` is the operator's only entry point into the curated music collection — `music.ablz.au`. On 2026-05-08 it wedged silently for ~9 hours: systemd reported `active (running)`, the listener still accepted TCP, but every HTTP request hung forever. Recovery required `systemctl restart`. Ad-hoc forensics in #233 traced the wedge to a sustained reconnect storm: the cratedigger main loop's end-of-cycle `POST /api/cache/invalidate` (cratedigger.py:1610) closes the socket mid-body before the server finishes writing the response, the `do_POST` catch-all treats the resulting `BrokenPipeError` as a database error and unconditionally tears down + reopens the psycopg2 connection, ~3 unnecessary reconnects per minute compound until the single-threaded server thread blocks indefinitely in `poll()`. Sibling issue #227 had been tracking the same BrokenPipe count climbing as a leading indicator — its 2026-05-09 update confirmed the alarm fires regardless of dashboard latency.

Three structural choices in the current design are jointly load-bearing for this failure mode and cannot be patched in isolation:

1. The web service is a single-threaded `http.server` — one stalled request blocks every other request.
2. The catch-all in `do_GET`/`do_POST` runs `_try_reconnect_db()` on every caught exception, conflating client-disconnect errors with database errors.
3. The psycopg2 connection is a process-wide global, so reconnecting it under load mid-request is the only available recovery primitive — and that primitive is what the storm abuses.

Each of these exists for a reasonable reason: stdlib only, simple deployment, one connection per process. Together they make a wedge inevitable under sustained client-disconnect traffic. The literal "stdlib `http.server`, no npm, no build step" rule in `.claude/rules/web.md` was originally a complexity-allergy guardrail, but the wedge has demonstrated that maintaining a hand-rolled request lifecycle costs more than the dependency it was meant to avoid.

---

## Requirements

**Hosting and concurrency**

- R1. The web service runs as a WSGI application hosted by `waitress` (pure-Python, no fork). Worker-thread count is small (4-8), enough that one slow request cannot head-of-line all others but small enough that psycopg2 connection count stays bounded.
- R2. Each waitress worker thread holds its own psycopg2 connection, lazily initialised on first use within that thread. There is no process-global `db` singleton.
- R3. Request timeouts are enforced by waitress, not by application code. No new signal-based watchdog is added.
- R4. The systemd unit (`cratedigger-web.service`) keeps `Type=simple` + `Restart=on-failure`. `ExecStart` invokes `waitress-serve` (or equivalent) against the WSGI app entry point; `python web/server.py` as a direct entry point is removed.

**Routing and request handling**

- R5. Routes are registered as Bottle decorators on the route functions in `web/routes/*.py`. The `_FUNC_GET_ROUTES` / `_FUNC_POST_ROUTES` / `_FUNC_GET_PATTERNS` / `_FUNC_POST_PATTERNS` registration tables in `web/server.py` are deleted.
- R6. The `Handler(BaseHTTPRequestHandler)` class with its `do_GET` / `do_POST` switchboard and inline JSON helpers (`_json`, `_html`, `_static_js`, `_error`) is deleted; equivalents live in Bottle / waitress.
- R7. Every route currently registered in `web/routes/` (browse, labels, library, imports, pipeline — 52 routes total) is ported with functional parity. No endpoints are added, removed (other than R8), renamed, or have their JSON contracts changed.
- R8. The `POST /api/cache/invalidate` endpoint is removed entirely on the server side. The corresponding `urlopen` call in the cratedigger main loop's end-of-cycle `finally` block (cratedigger.py:1610) is also removed. `cache.invalidate_groups()` itself remains a callable internal API in `web/cache.py`.
- R9. The Bottle app must serve the existing static assets (`web/index.html`, `web/js/*.js`) with the same paths, content-types, and cache headers as today.
- R10. Range requests on audio endpoints (used by the Wrong Matches audio player) continue to work end-to-end, including through the local-dev tunnel proxy.

**Exception handling**

- R11. Client-disconnect errors (`BrokenPipeError`, `ConnectionResetError`, `ConnectionAbortedError` and their WSGI-layer equivalents) are recognised distinctly from database errors. They produce no traceback in journald, no DB reconnect, and no second body-write attempt.
- R12. `_try_reconnect_db()` is removed entirely. Recovery from a stale or broken DB connection is the worker process's responsibility — a handler that hits `psycopg2.OperationalError` may either let the worker crash (waitress restarts the thread) or open a fresh connection on the next request via the lazy-init in R2.
- R13. A real `psycopg2.OperationalError` raised inside a route handler still surfaces as an HTTP 500 to the client and a single ERROR-level log line server-side. No 30-line traceback chain.

**Local-dev parity**

- R14. `scripts/web_dev_server.py` continues to host the same routes as the production service, against the same data sources (`--data live-db` and `--data prod-api` modes). It uses the same Bottle app entry point and the same WSGI host (or an equivalent local single-process option), so behaviour observed in dev matches production.

**Operator-visible perf cleanup**

- R15. `web/routes/pipeline.py::get_pipeline_dashboard` is wrapped in the existing `cache_api.cached(group="pipeline", ttl=300)` decorator (Q1 follow-up from #227). No other dashboard logic changes; this is a pure caching addition that closes the residual #227 work while the route layer is being touched.

**Test coverage**

- R16. `TestRouteContractAudit` continues to pass — every route registered with the new framework is classified in `CLASSIFIED_ROUTES`, and the audit fails fast if a new route ships unclassified.
- R17. The contract tests in `tests/test_web_server.py` continue to assert the same `REQUIRED_FIELDS` for every endpoint they cover. The `_WebServerCase` harness is rebased on the new app (it already runs the real server on a random port; the host changes from `HTTPServer` to whatever the production app uses).
- R18. A new test class covers the pathology specifically: a client closes the socket mid-body during a POST. Assertions: no DB reconnect, no `log.exception` traceback, no second body-write attempt, only a single WARNING-level log line.
- R19. A regression-guard test confirms a real `psycopg2.OperationalError` raised inside a handler still produces a 500, still logs a single ERROR line, and does not silently swallow the failure.

---

## Acceptance Examples

- AE1. **Covers R1, R3, R11, R18.** A client opens a TCP connection to the web service, sends a `POST /api/library/foo` request, reads the status line + response headers, then closes the socket before the body finishes streaming. Server-side: no traceback in journald, no DB reconnect, no entry in any `download_log` table, and the worker thread that handled the request is immediately available for the next request. Other in-flight requests (e.g. a `GET /api/pipeline/dashboard` from a separate client) complete normally.
- AE2. **Covers R1, R12.** A request handler invokes `db.execute(...)` and the connection is found to be dead (e.g. PostgreSQL was restarted). The handler raises `psycopg2.OperationalError`. Waitress returns a 500 to the client. The next request to the same worker thread opens a fresh psycopg2 connection via the R2 lazy-init and succeeds. No reconnect-on-every-exception storm is possible because no catch-all reconnect path exists.
- AE3. **Covers R8.** The cratedigger main loop completes a cycle. Its end-of-cycle `finally` block does not POST to the web service. The web service receives no inbound request; no BrokenPipeError is logged; no DB reconnect is performed.
- AE4. **Covers R10.** The Wrong Matches UI plays an audio file via the dev-server tunnel (`scripts/web_dev_server.py --data prod-api` proxying to a `live-db` backend on doc2). Range header is forwarded; seeking works; the audio plays end-to-end.

---

## Success Criteria

- After deploy: BrokenPipe count in `journalctl -u cratedigger-web` over a 24-hour window is < 5 (today's baseline is 2,355). The end-of-cycle POST is gone, so the only surviving sources are real client-side timeouts.
- After deploy: no `cratedigger-web` wedge incident occurs for 30 days under normal cratedigger main-loop traffic and dashboard polling. A wedge is defined as the service reporting `active` while every HTTP request hangs > 30s.
- A pathological request (mid-body close, stalled downstream call, deliberately slow handler) takes out at most one waitress worker thread, not the whole UI. Verified with a deliberate stall test against a staging instance.
- `_try_reconnect_db`, the `_FUNC_*` registration tables, the `Handler` class, and the `POST /api/cache/invalidate` endpoint do not appear in `web/server.py` or its replacement after the migration. `git grep` returns no matches.
- A downstream agent picking up `ce-plan` against this doc can specify the WSGI app entry point, the Bottle route mapping pattern, and the waitress invocation in the systemd unit without inventing product behaviour, scope, or success criteria.

---

## Scope Boundaries

- Auth or login. The UI stays unauthenticated; LAN-only by design.
- Frontend changes of any kind. Vanilla JS, no npm, no build pipeline, no SSR.
- Switching to async hosting (asyncio, starlette, uvicorn, hypercorn). A future option only if scale demands it; not warranted at current load.
- A separate signal-based per-request watchdog. Waitress's built-in request timeout + worker-thread isolation is the watchdog.
- BeetsDB sqlite reconfiguration (per-request open, connection pool, etc.). Worker-thread isolation handles a stall implicitly. Out of scope here; revisit if a sqlite stall is ever observed.
- Migrating other services. The `cratedigger.service` oneshot is touched only to remove its end-of-cycle POST; the importer worker (`cratedigger-importer`) and the migrator (`cratedigger-db-migrate`) are unchanged.
- Database schema changes. None needed.
- New observability (Prometheus, structured logs, request tracing). Existing journald-based logs are sufficient; revisit if the wedge alarm pattern needs to be replaced with a more direct signal.
- New runtime dependencies beyond `bottle` and `waitress`. Bottle plugins, ORM layers, request-helper packages, or other ecosystem additions are out of scope. The route layer remains thin Python over psycopg2.

---

## Key Decisions

- **Bottle over Flask.** Bottle is a single-file framework with no dependencies of its own; Flask pulls in Werkzeug, Jinja2, click, itsdangerous. For a thin route layer that already has its own template story (none — the UI is vanilla JS) and its own JSON conventions, Bottle is closer to the project's "minimize complexity" ethos. Trade-off accepted: smaller ecosystem, but the route layer doesn't need ecosystem.
- **Waitress over gunicorn.** Waitress is pure-Python and uses a thread pool, not pre-forked workers. For a homelab single-machine deployment with low RPS, pre-fork process isolation (gunicorn's main differentiator) is overkill, and the pure-Python install is one less moving piece. Trade-off accepted: a wedged thread is less isolated than a wedged worker process — but waitress's request timeout + the deletion of the catch-all reconnect makes the wedge unreachable, not merely contained.
- **Migrate all 52 routes in one PR, not staged.** A half-migrated state (some routes on `http.server`, some on Bottle) would require two route-registration paths, two exception models, and two test harnesses simultaneously. The all-at-once migration is mechanical (route bodies don't change, only their decoration and error semantics), and the contract test suite + `TestRouteContractAudit` enforce equivalence at PR-time.
- **Delete `_try_reconnect_db` rather than keep it for the new model.** Under per-thread connections + waitress worker recycling, a stale connection is recovered by the next request's lazy-init or by a worker recycle. The reconnect-in-place primitive is what enabled the storm; removing it makes the storm structurally impossible.
- **Fold the Q1 dashboard cache wrapper from #227 into this PR.** The route layer is being touched anyway and the wrapper is a one-decorator addition. Scoping it out would mean a separate small PR against a freshly migrated route module — strictly more cost.

---

## Dependencies / Assumptions

- **Bottle and waitress are available in nixpkgs.** Both are well-maintained and in the standard Python ecosystem; their absence would be surprising. Verified before implementation begins.
- **`scripts/web_dev_server.py` shares a single WSGI app entry point with production.** The current dev server reuses `web/server.py`'s handler classes; the new design assumes the WSGI app is constructable in-process by both production (under waitress) and dev (under whatever single-process host the dev server picks). This keeps dev-vs-prod behaviour parity, which the local-dev workflow in CLAUDE.md depends on.
- **Per-thread psycopg2 connections do not exhaust PostgreSQL's `max_connections`.** With 4-8 waitress threads and `max_connections = 100` on the nspawn PostgreSQL container (default), there is ample headroom. If multiple `cratedigger-web` workers ever ran (not currently planned), this would need re-evaluation.
- **The cratedigger main loop's end-of-cycle POST has no other consumer.** Removing R8's `urlopen` call breaks no external system. Verified in dialogue against issue #101's note that the endpoint has been a no-op since that issue.
- **`cache.invalidate_groups()` retained for internal callers.** Kept in `web/cache.py` because other in-process code paths may still want to invalidate cache groups; only the HTTP route and the cratedigger.py round-trip caller go away.

---

## Outstanding Questions

### Deferred to Planning

- [Affects R5][Technical] Bottle's default exception model: does the `@app.error(500)` hook need explicit suppression for the disconnect classes in R11, or does WSGI's protocol naturally short-circuit body writes after a closed-socket signal? Verify against the waitress source during planning.
- [Affects R2][Technical] Where does the per-thread connection live — `threading.local()` on the app object, a `bottle.request`-scoped attribute, or a small connection registry keyed by `threading.get_ident()`? Mechanism choice doesn't change the requirement; pick during planning based on Bottle conventions.
- [Affects R4][Technical] Does the systemd unit invoke `waitress-serve` directly, or wrap it in a thin Python entry point that constructs the app and hands it to `waitress.serve(...)` programmatically? The latter gives slightly more control over thread count, listen address, and graceful shutdown signals.
- [Affects R7][Needs research] Two routes use regex patterns rather than literal paths (`_FUNC_GET_PATTERNS`, `_FUNC_POST_PATTERNS`). Bottle supports both `<param>` placeholders and `<param:re:pattern>` regex captures; confirm during planning that every existing pattern translates cleanly.
- [Affects R14][Technical] `scripts/web_dev_server.py` currently runs the stdlib server with custom mounting for the `--data prod-api` proxy mode. Decide during planning whether the proxy mode also runs through waitress (single host pattern) or stays on a stdlib server (different host for the proxy use case). The requirement is parity of behaviour observed by the operator; the host choice is a planning detail.
