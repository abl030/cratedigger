---
title: "fix: minimal Python patch to kill the cratedigger-web wedge (#233)"
type: fix
status: active
date: 2026-05-09
origin: docs/solutions/web-rewrite-deferred-pending-runtime-redesign.md
---

# fix: minimal Python patch to kill the cratedigger-web wedge (#233)

## Summary

Kill the active `cratedigger-web` wedge (issue #233) with the smallest possible Python patch on the existing stdlib `http.server` stack. Delete the end-of-cycle no-op POST in `cratedigger.py` that triggers the storm, narrow the `do_GET`/`do_POST` catch-all in `web/server.py` to skip `_try_reconnect_db()` on client-disconnect errors, add a RED/GREEN regression test that exercises a mid-body socket close via raw socket, and (conditional on a quick audit) switch `HTTPServer` to `ThreadingHTTPServer` so a single slow request can't head-of-line every other route. No framework migration, no Rust, no architectural change.

---

## Problem Frame

Deferred all framework / language redesign per `docs/solutions/web-rewrite-deferred-pending-runtime-redesign.md` — the web layer is too coupled to pipeline lib code for a clean carve-out today. This plan ships the minimum needed to stop the active operational pain (the BrokenPipe storm and 9-hour silent wedge from #233) on the existing stack while the larger architectural questions stay deferred.

---

## Requirements

- R1. The cratedigger main loop no longer POSTs to `/api/cache/invalidate` at end of cycle (the storm trigger is gone).
- R2. A client closing its socket mid-response on either GET or POST does not trigger `_try_reconnect_db()`, does not produce a multi-line `log.exception` traceback, and does not attempt a second body-write back to the dead socket.
- R3. Real exceptions inside route handlers (e.g. `psycopg2.OperationalError`) still hit the existing catch-all path: traceback logged, DB reconnect attempted, 500 returned. The narrowing must NOT change behaviour for legitimate handler errors.
- R4. After deploy, BrokenPipe lines in `journalctl -u cratedigger-web` drop from the ~2,355/24h baseline (per #233's 2026-05-08 sample) to <5/24h.
- R5. *(Conditional)* If a quick audit confirms `web/server.py` and `web/routes/*.py` route handlers do not share per-request mutable state in race-prone ways, the listener is upgraded from `HTTPServer` to `ThreadingHTTPServer`. If the audit reveals shared mutable state, this requirement is consciously deferred — flag it explicitly in the verification notes for ce-work.

---

## Scope Boundaries

- No framework migration. The doc explicitly defers Bottle, Flask, gunicorn, waitress, axum, and every other framework swap (see origin).
- No Rust work.
- No Q1 dashboard cache wrapper from #227.
- No rewrite of the `_try_reconnect_db()` design itself. We narrow what reaches it; the function's body and its callers' broader semantics stay as-is.
- No removal of `_FUNC_GET_ROUTES` / `_FUNC_POST_ROUTES` registration tables, no rewrite of `Handler` class, no changes to route bodies.
- No new observability surfaces (Prometheus, structured logs, request tracing).

### Deferred to Follow-Up Work

- Future cratedigger runtime redesign (cycle-driven → static / event-driven), at which point the web rewrite becomes a tractable carve-out (see origin).
- Q1 dashboard cache wrapper from #227 (orthogonal perf optimization, separate ce-plan when relevant).

---

## Context & Research

### Relevant Code and Patterns

- **`cratedigger.py:1605-1617`** — the end-of-cycle `finally` block constructs `urllib.request.Request("http://localhost:8085/api/cache/invalidate", …)`, calls `urlopen(req, timeout=2)`, swallows any exception via `except Exception: pass`. The endpoint has been a documented no-op since #101 (see `web/server.py:282-298`). Deletion is dead-code removal.
- **`web/server.py:50-65`** — `_try_reconnect_db()` does `db.conn.close()` followed by `PipelineDB(_db_dsn)`. Survives unchanged.
- **`web/server.py:299-329`** — `do_GET` body, including the existing `except Exception as e: log.exception(...); _try_reconnect_db(); self._error(str(e), 500)` catch-all at lines 326-329.
- **`web/server.py:331-365`** — `do_POST` body with the equivalent catch-all at lines 362-365.
- **`web/server.py:245-252`** — `_json` writes the response body via `self.wfile.write(body)` AFTER `send_response`/`end_headers`. This is the line that raises `BrokenPipeError` when the client has already closed.
- **`web/server.py:18, 415`** — current listener is `HTTPServer(("0.0.0.0", args.port), Handler)`. The drop-in replacement is `ThreadingHTTPServer` from the same `http.server` module — no other API changes.
- **`tests/test_web_server.py:97-130`** — `_WebServerCase` harness creates a real `HTTPServer` on a random port via `_make_server()`, exposes `_get(path)` and `_post(path, body)` helpers via `urllib.request.urlopen`. Reuse for happy-path and real-error tests; for the mid-body-close test we need a separate raw-socket harness because `urlopen` doesn't support "send partial body then close."
- **`tests/test_web_server.py:911-982`** — `TestRouteContractAudit` introspects `Handler._FUNC_GET_ROUTES` etc. Unchanged by this plan; ensures any new behaviour is route-classified.

### Institutional Learnings

- **`docs/solutions/web-rewrite-deferred-pending-runtime-redesign.md`** — origin. Captures the reasoning chain that led to deferring all framework migrations.
- The same doc records the lesson that the existing catch-all is too broad: it conflates DB errors with network errors, which is exactly the mechanism this plan narrows.

### External References

- None. This is a stdlib `http.server` patch with no external library involvement.

---

## Key Technical Decisions

- **Add a typed `except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError)` clause BEFORE the existing `except Exception` rather than rewriting the catch-all.** Smallest possible diff. Keeps the existing `_try_reconnect_db` behaviour for real exceptions intact (R3 regression-guard). The narrower clause runs first in Python's `except` matching order, so disconnect errors never reach the catch-all.
- **Use `log.warning(...)` (single-line) for disconnect errors, NOT `log.exception(...)`.** The existing catch-all uses `log.exception` which emits a full traceback; for a normal client disconnect the traceback is noise and is what produces the 30-line journald-flood pattern documented in #233.
- **Do not call `self._error(str(e), 500)` in the disconnect handler.** The socket is already dead; trying to write a 500 body produces a second `BrokenPipeError` that propagates up to `socketserver._handle_request_noblock`, which is exactly the chained-traceback pattern the wedge produces. The handler returns silently after the warning log.
- **Test the mid-body-close via a raw `socket.create_connection` + `sendall` + `close` pattern, not via `urlopen`.** `urllib.request` has no API for "send headers + partial body, then close" — it always sends a complete request. The new test uses `socket` directly to write request bytes and close at a controlled point. Pattern reference: similar raw-socket testing exists in stdlib's own `test_httpservers.py` for `BaseHTTPRequestHandler` cases.
- **U3 (`ThreadingHTTPServer` swap) is optional and gated on an audit.** The audit reads `web/server.py`, `web/routes/*.py`, and any module-level state they touch (`srv.db`, `_beets`, `cache.*`) to confirm none is mutated within a request handler in a way that would race under threads. If the audit clears, the swap is a two-line change. If it doesn't, the swap is deferred and ce-work flags a follow-up issue. The wedge fix (U1+U2) does NOT depend on this swap landing.

---

## Open Questions

### Resolved During Planning

- **How to inject a mid-body socket close in tests?** Use `socket.create_connection` against the real test server (already running on a random port via `_WebServerCase`-style setup), `sendall` headers + `Content-Length: <large>`, then close before the body completes. The handler reads partial body via `self.rfile.read(length)`, attempts to JSON-decode, hits a partial-read or write the response, and `wfile.write` raises `BrokenPipeError`. Resolution: add a small raw-socket helper to a new test class; do not modify the existing `_WebServerCase` harness.
- **Where does the disconnect handler return from?** From inside `do_GET`/`do_POST`. The handler returns normally; `socketserver` then closes the connection on its end. No further action needed.
- **Do we need to log anything about the disconnect at all?** Yes — single WARNING line at low frequency to detect a future flood. `log.warning("Client disconnect on %s %s: %s", method, path, type(e).__name__)`. No traceback.

### Deferred to Implementation

- **Exact wording of the warning log line.** Implementer picks. Suggested format above is a starting point; ce-work may adjust for consistency with surrounding log calls.
- **Whether to add a third `except` clause for `OSError` with errno 32 (EPIPE) on platforms where `BrokenPipeError` doesn't fire.** Linux is the only deployment target; on Linux `BrokenPipeError` is a subclass of `OSError` and fires reliably. Not adding a fallback unless the test suite reveals one is needed.
- **U3 audit findings.** Whether the `ThreadingHTTPServer` swap actually lands depends on what the audit reveals. If it lands, ce-work updates U3's verification notes; if it's deferred, ce-work flags a follow-up issue.

---

## Implementation Units

### U1. Remove the end-of-cycle cache-invalidate POST

**Goal:** Delete the storm trigger. The `cratedigger.py:1605-1617` `finally` block POSTs to a no-op endpoint every cycle and never reads the response body — every invocation produces a `BrokenPipeError` on the server side. The endpoint has been a documented no-op since #101.

**Requirements:** R1, R4 (R4 is the after-deploy verification; this unit removes the storm source).

**Dependencies:** None.

**Files:**
- Modify: `cratedigger.py`

**Approach:**
- Delete the entire `try` block at `cratedigger.py:1605-1617` that imports `urllib.request`, constructs the `Request`, and calls `urlopen(req, timeout=2)`. Including the comment lines and the `except Exception: pass` guard.
- The remaining `finally` body (cleaning up the pipeline DB connection at lines 1619-1624 and removing the lock file at lines 1625-1627) is unchanged.
- No new code added. This is a pure deletion.

**Patterns to follow:**
- The pipeline's other end-of-cycle cleanup (the `pipeline_db_source.close()` in the same `finally` block) is the pattern: best-effort, swallow failures, don't reach out across process boundaries.

**Test scenarios:**
- Test expectation: none — pure removal of dead code that has no caller-observable behaviour. The endpoint it called is a documented no-op; no test in the suite asserts the POST is made. Verification is "after deploy, journald shows no `POST /api/cache/invalidate` lines per cycle."

**Verification:**
- `git grep "/api/cache/invalidate"` in `cratedigger.py` returns no matches after the change.
- Running the full test suite (`nix-shell --run "bash scripts/run_tests.sh"`) passes — no test depends on this POST being made.
- After deploy, `ssh doc2 'sudo journalctl -u cratedigger --since "10 min ago" | grep -c cache/invalidate'` returns 0.

---

### U2. Narrow the do_GET/do_POST catch-all to skip reconnect on client-disconnect

**Goal:** Add a typed `except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError)` clause before the existing catch-all in both `do_GET` and `do_POST`. Disconnect errors get a single WARNING log line and a clean return; they do not fire `_try_reconnect_db()` and do not attempt a second body-write to the dead socket.

**Requirements:** R2, R3.

**Dependencies:** None — independent of U1, but ships in the same PR.

**Execution note:** Test-first per `.claude/rules/code-quality.md`. Write the failing test that asserts no reconnect on mid-body close, watch it fail (RED) because today's code reconnects unconditionally, then implement the narrowing and watch it pass (GREEN). Add the regression-guard tests for the unchanged real-error path before considering the unit complete.

**Files:**
- Modify: `web/server.py` (specifically lines 299-329 for `do_GET` and lines 331-365 for `do_POST`)
- Test: `tests/test_web_server.py` (new test class — see test scenarios below)

**Approach:**
- In `do_GET`, before the existing `except Exception as e:` at line 326, add:
  - `except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError) as e:` clause
  - Body: single `log.warning("Client disconnect on GET %s: %s", path, type(e).__name__)` line, then return.
  - No call to `_try_reconnect_db()`. No call to `self._error(...)`.
- Same change in `do_POST` at line 362.
- The existing `except Exception` block remains unchanged for real exceptions (R3 regression-guard).
- Total diff is roughly 8-10 lines added across two functions.

**Patterns to follow:**
- Existing `log.warning` calls in `web/server.py` (e.g. logger.warning patterns elsewhere in the codebase) for format string style.
- Python exception-clause-ordering convention: more specific clauses before more general ones. The new clause is more specific than `except Exception` and runs first.

**Test scenarios:**

*New test class in `tests/test_web_server.py` — separate from `_WebServerCase` because raw-socket testing requires a different setup. Suggested name: `TestClientDisconnectHandling`.*

- **Happy path (regression guard for unchanged behaviour):**
  - `Happy path:` A normal `_post("/api/pipeline/status", {})` against the existing `_WebServerCase` harness — no disconnect, server returns 200 or 4xx, no `_try_reconnect_db` calls observed (assert via `mock` patch on `web.server._try_reconnect_db`).
  - `Happy path:` A normal `_get("/api/pipeline/all")` — same assertions.

- **Wedge regression (the new test, RED-first):**
  - `Wedge regression:` Open a raw socket via `socket.create_connection(("127.0.0.1", port))`. Send `POST /api/pipeline/set-intent HTTP/1.1\r\nHost: ...\r\nContent-Type: application/json\r\nContent-Length: 1024\r\n\r\n` followed by ~10 bytes of partial body, then close the socket. Assert: `_try_reconnect_db` mock is NOT called; the test's log capture (via `self.assertLogs("cratedigger-web", level="WARNING")` or equivalent) shows exactly one WARNING line containing "Client disconnect" and zero `log.exception`-style records (i.e., no record at ERROR with `exc_info` set); the server stays up and a subsequent normal request completes.
  - `Wedge regression:` Same pattern on a GET — open raw socket, send `GET /api/pipeline/log HTTP/1.1\r\n\r\n`, immediately close after status line might be partially written. Same assertions.

- **Real DB error path (regression guard for R3):**
  - `Error path:` Patch the route handler to raise `psycopg2.OperationalError("simulated PG outage")`. Make a normal `_post(...)` request. Assert: `_try_reconnect_db` IS called (mock invocation count == 1); `log.exception`-style record IS present (ERROR level with `exc_info`); response status is 500.
  - `Error path:` Same pattern via GET.

- **Real other-exception path (regression guard for unchanged behaviour):**
  - `Error path:` Patch the route handler to raise `ValueError("simulated bug")`. Make a `_post` request. Assert: `_try_reconnect_db` IS called; ERROR-level log record present; response status 500. (Existing behaviour: the broad catch-all reconnects on any exception, which is wider than ideal but is explicitly out of scope per Scope Boundaries — narrowing it further is deferred to a future cleanup. This test pins down "we did not accidentally narrow more than we intended.")

**Verification:**
- All four test categories above pass under `nix-shell --run "python3 -m unittest tests.test_web_server -v"`.
- `nix-shell --run "pyright --threads 4 web/server.py tests/test_web_server.py"` reports 0 new errors on the changed files.
- The pre-commit pyright hook passes on staged files.

---

### U3. *(Conditional)* Audit shared mutable state and switch HTTPServer → ThreadingHTTPServer

**Goal:** Prevent a single slow request from head-of-lining all other routes. The wedge fix (U1+U2) eliminates the storm; a slow downstream call (psycopg2 reconnect race, sqlite virtiofs hitch, MB mirror hang) can still wedge the single thread. ThreadingHTTPServer is a stdlib drop-in that gives one-thread-per-request isolation. **This unit lands only if the audit confirms it's safe.**

**Requirements:** R5.

**Dependencies:** U1, U2 — those land first; the audit and swap follow on the same PR if the audit clears.

**Files:**
- Modify: `web/server.py:18` (import) and `web/server.py:415` (instantiation)
- Audit-only (no edits unless issue surfaces): `web/server.py` module-level state, `web/routes/*.py` for any per-request mutation of process globals.

**Approach:**
- **Audit step (no edits yet):**
  1. Search `web/server.py` for module-level mutable state that route handlers touch: `db` (PipelineDB instance — psycopg2 conn is shared; psycopg2 connection objects are NOT thread-safe — this is the load-bearing concern), `_beets` (BeetsDB sqlite handle), `beets_db_path`, `_db_dsn`, anything in `web.cache`.
  2. For each: determine whether route handlers mutate it (write) vs read it (call methods). psycopg2 connection's `cursor()` and query methods are NOT thread-safe even for read; they hold internal protocol state.
  3. **Expected finding:** psycopg2 `db.conn` is shared and not thread-safe. Even read-only queries from two threads concurrently can corrupt the protocol state. **This is likely a blocker for the swap.** If so, defer U3 explicitly, document the finding in the PR description, and flag a follow-up issue: "ThreadingHTTPServer swap requires per-thread psycopg2 connection management — out of scope for the wedge-fix PR."
  4. If by some chance the audit reveals the connection IS protected (e.g. by an existing lock — it isn't today), the swap can land. This is unlikely.

- **Conditional swap (only if audit clears, which is unlikely):**
  - Change `from http.server import HTTPServer, BaseHTTPRequestHandler` to `from http.server import HTTPServer, ThreadingHTTPServer, BaseHTTPRequestHandler`.
  - Change `server = HTTPServer((...), Handler)` at line 415 to `server = ThreadingHTTPServer((...), Handler)`.
  - Add a brief comment near the instantiation noting the threading model.

**Patterns to follow:**
- Stdlib's `http.server.ThreadingHTTPServer` itself — it's a one-line `class ThreadingHTTPServer(socketserver.ThreadingMixIn, HTTPServer): daemon_threads = True` in the cpython source. No surprises.

**Test scenarios:**
- **If U3 lands (audit clears):**
  - `Integration:` two slow handlers in parallel — patch one route to sleep 2 seconds; make two concurrent requests via `threading.Thread`; assert both complete in roughly 2 seconds total (not 4). Verifies threading is actually serving requests in parallel.
  - `Regression:` the existing test suite still passes — most importantly the `TestRouteContractAudit` and the `TestPipelineRouteContracts` tests, which assert response shapes are unchanged.
- **If U3 is deferred:**
  - Test expectation: none — no code change shipped.
  - Verification is the audit notes themselves, captured in the PR description.

**Verification:**
- **If U3 lands:** the parallel-slow-handler test passes; the full test suite passes; `pyright` clean; after deploy, `ssh doc2 'sudo systemctl status cratedigger-web'` shows the service running normally.
- **If U3 is deferred:** PR description includes a one-paragraph audit summary explaining what shared mutable state exists, why threading would be unsafe today, and a follow-up issue link/number for the future per-thread connection design. ce-work creates the follow-up issue and pastes its URL into the PR description.

---

## System-Wide Impact

- **Interaction graph:** Only `web/server.py`'s `do_GET`/`do_POST` and `cratedigger.py`'s end-of-cycle `finally` block are touched. No middleware, no observers, no other entry points. The `_try_reconnect_db()` function is unchanged in body and call sites — only the set of exceptions that reach it is narrower.
- **Error propagation:** Disconnect errors are now caught and silently dropped with a warning. Real exceptions still propagate through the existing catch-all. No exception class that today reaches `_try_reconnect_db` stops reaching it (only `BrokenPipeError` / `ConnectionResetError` / `ConnectionAbortedError` are intercepted earlier).
- **State lifecycle risks:** None. The patch removes a no-op IPC call (U1) and adds a typed exception handler (U2). No state machines change. No DB writes change. No filesystem changes.
- **API surface parity:** No routes added, removed, or changed. JSON contracts unchanged. `TestRouteContractAudit` continues to pass with no `CLASSIFIED_ROUTES` updates needed.
- **Integration coverage:** The new `TestClientDisconnectHandling` tests cover the cross-layer scenario (raw socket → `BaseHTTPRequestHandler` → `do_GET`/`do_POST` → `_json` → `wfile.write` → `BrokenPipeError` → new typed handler). Mocks alone wouldn't prove this — need the real HTTPServer + real socket transport.
- **Unchanged invariants:** `_try_reconnect_db()`'s body and call sites are unchanged. The `Handler` class structure, `_FUNC_*_ROUTES` registration tables, and route-body code in `web/routes/*.py` are all untouched. `cratedigger.service`'s 5-min systemd timer still triggers cycles. The cratedigger main loop still finishes its cycle cleanly (the deleted POST was best-effort wrapped in `try/except Exception: pass` — its absence is invisible).

---

## Risks & Dependencies

| Risk | Mitigation |
|------|------------|
| The new disconnect handler swallows errors that were previously useful for debugging (e.g. a real network problem that today shows up as a traceback). | Single WARNING log line per disconnect preserves observability. If the BrokenPipe count climbs again post-deploy, journald shows the WARNING lines and the source path — enough to triage. |
| The raw-socket test is flaky in CI (timing-dependent). | The test asserts on observable side effects (`_try_reconnect_db` mock call count, log records) rather than on timing. Use `socket.SHUT_RDWR` + small `time.sleep(0.05)` to ensure the server has attempted the body write before the assertion runs. If still flaky, add a retry decorator. |
| U3's audit reveals psycopg2 thread-safety blocker (expected). | Plan already accommodates: defer U3, ship U1+U2 alone (the wedge fix), file follow-up. The audit is cheap (~30 min of code reading). |
| The narrowed exception clauses miss a disconnect path on a non-Linux platform. | Deploy target is Linux only (NixOS doc2). Don't add cross-platform fallbacks unless the test suite reveals one is needed. |
| U2's RED test passes accidentally before the fix lands (false negative). | RED-first discipline: run the test against unmodified code first; assert it fails on the `_try_reconnect_db` mock count. Only proceed to GREEN once RED is verified. |
| Deploy-window asymmetry: between cratedigger-web restart and next cratedigger.service tick, an in-flight cycle may POST to the now-removed endpoint. | The existing `try: urlopen(...) except Exception: pass` swallows the resulting 404 silently. Benign. Documented here so a journald reader doesn't chase it. |

---

## Documentation / Operational Notes

- **Deploy via the standard nix flake flow per `.claude/rules/deploy.md`:** push code → `nix flake update cratedigger-src` on doc1 → `nixos-rebuild switch` on doc2. `cratedigger-web.service` restarts on switch (per its `restartIfChanged` default); `cratedigger.service` does not restart (per its `restartIfChanged = false`) but the next 5-min timer tick picks up the new code.
- **Post-deploy verification (R4):** `ssh doc2 'sudo journalctl -u cratedigger-web --since "1 hour ago" | grep -c BrokenPipeError'` should return ≤2 within an hour of deploy, and `--since "24 hours ago"` should return <5 after a full day. If the count stays elevated, U2's narrowing didn't take effect — investigate.
- **Spot-check that U1's deletion took effect:** `ssh doc2 'sudo journalctl -u cratedigger --since "10 min ago" | grep cache/invalidate'` should return nothing after a cycle has run.
- **No CLAUDE.md or `.claude/rules/web.md` updates required.** Both still describe the current stack accurately. The architectural rule rewrite is part of the deferred future Rust rewrite, not this patch.

---

## Sources & References

- **Origin document:** [docs/solutions/web-rewrite-deferred-pending-runtime-redesign.md](../solutions/web-rewrite-deferred-pending-runtime-redesign.md)
- Issue #233 — original wedge incident with full forensics (run `gh issue view 233` for details).
- Issue #227 — sibling perf issue that surfaced the BrokenPipe alarm pattern.
- `cratedigger.py:1605-1617` — the storm trigger.
- `web/server.py:50-65` — `_try_reconnect_db()`.
- `web/server.py:299-329, 331-365` — `do_GET` / `do_POST` catch-alls.
- `web/server.py:245-252` — `_json()` write site that raises `BrokenPipeError`.
- `tests/test_web_server.py:97-130` — `_WebServerCase` test harness pattern to extend.
- Superseded brainstorms (retained for context): `docs/brainstorms/2026-05-09-web-stack-bottle-waitress-requirements.md`, `docs/brainstorms/2026-05-09-cratedigger-web-rust-rewrite-requirements.md`.
