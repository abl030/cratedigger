---
date: 2026-05-09
module: web
tags: architecture, rust, refactor, deferred, design-decision
problem_type: design-decision
---

# Web rewrite is deferred — `web/` is not a clean architectural seam yet

## TL;DR

Two attempts to rewrite `cratedigger-web` (Bottle + waitress; then Rust) both failed at the requirements-doc stage for the same underlying reason: **`web/routes/*.py` is not a clean architectural seam.** POST handlers orchestrate ~4,000 LOC of pipeline business logic from `lib/*` (`manual_import`, `import_queue`, `wrong_match_triage`, `import_preview`, `search_plan_service`, `audio_hash`, `release_cleanup`, `destructive_release_service`, `spectral_check`, `wrong_matches`). Carving the web layer into a separate language requires either re-deriving all of that lib code (forgets accumulated bug-fixes from many incidents) or designing a new IPC surface (new design + maintenance burden + cross-language debugging). Neither is a "rewrite the web layer" project — both are cratedigger-runtime-redesign projects with the web rewrite as a beneficiary.

**Decision:** apply the minimal Python patch to kill the current wedge (issue #233). Defer any framework or language migration until the cratedigger runtime is redesigned (cycle-driven → static / event-driven), at which point the web layer's seams will be in cleaner places and a Rust rewrite becomes a tractable carve-out.

## What triggered this

Issue #233: `cratedigger-web` wedged silently for ~9 hours on 2026-05-08. systemd reported `active (running)`, the listener still accepted TCP, but every HTTP request hung forever until manual restart. ce-debug traced the root cause to the `cratedigger.py:1610` end-of-cycle `POST /api/cache/invalidate` (no-op since #101) closing its socket mid-body, the `do_POST` catch-all in `web/server.py` treating the resulting `BrokenPipeError` as a database error and unconditionally tearing down + reopening the psycopg2 connection, and ~3 unnecessary reconnects per minute compounding until the single-threaded server thread blocked indefinitely in `poll()`. Sibling issue #227 had been tracking the BrokenPipe count climbing as a leading indicator.

The clean fix story was: replace the homegrown `http.server` + global psycopg2 + unconditional reconnect with something that has these properties as runtime guarantees rather than as engineered behaviour. That story turned out to be more complicated than it looked.

## Attempt 1: Bottle + waitress (Python WSGI)

Captured at `docs/brainstorms/2026-05-09-web-stack-bottle-waitress-requirements.md` (now superseded).

The pitch: keep Python, swap the homegrown stdlib server for a real WSGI app on a worker server. Worker isolation + per-request timeouts + per-thread psycopg2 connections via standard library mechanisms instead of `_try_reconnect_db()`.

**Killed by ce-doc-review** (kieran-python persona): R3 stated "Request timeouts are enforced by waitress, not by application code." This is **factually wrong** — waitress has no per-request timeout primitive, only `channel_timeout` for idle clients. The Key Decisions rationale for picking waitress over gunicorn (*"waitress's request timeout makes the wedge unreachable"*) rested on a capability that doesn't exist.

**Lesson:** when picking infrastructure, verify the specific capability you're depending on actually exists in the version you're picking. Don't trust framing claims from your own memory or from one source. Cost of being wrong: an entire requirements doc that the planner would have built against a phantom feature.

The prior-art ce-doc-review on this attempt also surfaced ~156 handler-coupled call sites across `web/routes/*.py` (`h._json`, `h._error`, `h.send_header`, `h.wfile.write`, `h.headers.get`, `h.rfile.read`) — even within Python the migration wasn't "mechanical decoration," it was a route-body rewrite for every endpoint.

## Attempt 2: Full Rust rewrite (axum + sqlx + rusqlite + fred)

Captured at `docs/brainstorms/2026-05-09-cratedigger-web-rust-rewrite-requirements.md` (now superseded).

The pitch: leave Python for the pipeline (beets-locked) but rewrite the web layer in Rust. Wedging stops being a class of failure (tokio + tower middleware give per-request timeouts, panic isolation, and pool-managed connections as runtime properties). End-to-end type safety via sqlx compile-time SQL validation + serde JSON contracts. A research sub-agent confirmed `axum 0.8 + sqlx + rusqlite + fred + tracing + crane` as the consensus 2026 stack.

**Killed by ce-doc-review** (feasibility persona, P0 finding): the doc treated "52 routes" as if they were thin "query DB, format JSON" handlers. Reality from `web/routes/*.py`:

- POST handlers call into ~3,900 LOC of pipeline lib code: `manual_import`, `import_queue`, `wrong_match_triage`, `import_preview`, `search_plan_service`, `audio_hash`, `release_cleanup`, `destructive_release_service`, `artist_compare`, `artist_releases`, `spectral_check`, `wrong_matches`.
- Endpoints like `post_pipeline_ban_source`, `post_pipeline_force_import`, `post_wrong_match_converge`, `post_pipeline_search_plan_regenerate` aren't thin routing — they orchestrate audio hashing, beets removal via subprocess, advisory locks, importer-job race-checks, search-plan generation.
- The Decisions tab additionally exposes pure functions from `lib/quality.py` (`dispatch_action`, `get_decision_tree`, `quality_gate_decision`) — same coupling.

A clean Rust web layer requires picking one of:

- **Port the lib code** (~4,000 LOC of business logic) → doubles the rewrite scope; re-derives subtle behaviour (the lib code carries scars from many incidents — Palo Santo data loss, Lucksmiths MBID drift, the cooldown logic, spectral verification quirks, bad-rip detection, the wedge itself); forgets accumulated bug-fixes by re-implementing from scratch.
- **IPC back to Python** for the heavy POST handlers → new design surface, new service to deploy, debugging crosses a language boundary, latency added per request.
- **Hybrid** — Rust for GETs, IPC for POSTs → captures most of the wins without re-deriving the lib, but the conceptual split is real ongoing tax.

None of these are a "rewrite the web layer" project. They're all "cratedigger architecture redesign" projects with a web rewrite as a side effect.

**Lesson:** before committing to a language/framework migration on a piece of a codebase, audit what that piece actually depends on. The web layer isn't autonomous — it's a thin facade in front of pipeline business logic. The seam between "web concerns" and "pipeline concerns" is in the wrong place to support a clean carve-out.

## The deeper observation: cratedigger isn't yet shaped for cleanly-carved services

Today's cratedigger architecture is **cycle-driven**:

- `cratedigger.service` runs every 5 minutes via systemd timer, importing all of `lib/*` synchronously into one Python process for the cycle's duration.
- `cratedigger-web.service` is a long-running stdlib HTTP server in a different process that reaches into the same `lib/*` for its POST handlers.
- `cratedigger-importer.service` is a third process that drains the import queue, also reaching into `lib/*`.

All three processes are essentially Python scripts that share `lib/*` by import. There's no IPC contract between them — the contract IS the shared Python codebase. That works fine until you try to carve one process into a different language: the import-graph dependency becomes the migration boundary, and the migration boundary turns out to be ~4,000 LOC.

A **static / event-driven** architecture would push the seams in different places:

- Pipeline state changes happen via PG `NOTIFY` channels or a queue rather than 5-min cycles.
- The importer worker is a long-running daemon that owns its corner of the lib (the import-relevant parts); other processes interact with it via well-defined commands (RPC, queue, or HTTP).
- The web layer becomes genuinely thin: it queries the pipeline DB, it issues commands to the importer daemon, it doesn't import lib code directly.
- `lib/*` decomposes naturally into "shared utilities," "importer-owned business logic," and "search/decision pure functions exposed via a stable interface."

In that architecture, the web layer is a clean carve-out target — Rust or otherwise. Today's web layer is not.

## The decision

1. **Apply the minimal Python patch** to kill the current wedge. ce-plan handles the implementation. Specifically:
   - Delete the end-of-cycle `urlopen` to `/api/cache/invalidate` in `cratedigger.py:1605-1617` (the source of the storm).
   - In `web/server.py`'s `do_GET` and `do_POST` catch-alls (lines 326-329 and 362-365), recognise client-disconnect errors (`BrokenPipeError`, `ConnectionResetError`, `ConnectionAbortedError`) **before** the existing `_try_reconnect_db()` call. Single-line warning; no reconnect; no second body-write attempt.
   - The `_try_reconnect_db()` call survives unchanged — it still fires on real exceptions (which the catch-all is too broad about, but that's a separate cleanup not required to kill the wedge).
   - Optional but cheap: switch from `HTTPServer` to `ThreadingHTTPServer` so a single slow request doesn't block all routes. Two-line change. Verify per-request handlers don't share mutable state first.

2. **Defer the framework/language migration.** Re-evaluate when:
   - The cratedigger runtime is being redesigned for other reasons (cycle-driven → static / event-driven). The redesign is the larger project; the web rewrite rides along.
   - OR a second wedge-class incident occurs that the minimal patch doesn't prevent. That would be evidence the structural concern is real, not theoretical.

3. **Capture this learning** so future-self / future-Claude doesn't re-derive it. (This document.)

## What a future Rust rewrite would need to be true

For a Rust rewrite of the web layer to be a clean carve-out, all of these need to hold:

- **The web layer's responsibilities are bounded.** It queries data and issues commands; it doesn't orchestrate business logic by importing pipeline modules.
- **The boundary between web and pipeline is an IPC contract**, not a shared Python codebase. PG NOTIFY, JSON-RPC over Unix socket, or HTTP between services — pick one and stick to it.
- **`lib/quality.py`'s pure decision functions are exposed via that contract**, not by Python import. Either the importer daemon hosts them and the web service queries via RPC, or they're a tiny library re-implemented in both languages with a shared test corpus that proves equivalence.
- **The audio hashing / beets removal / search-plan generation logic** is owned by a long-running daemon in Python, not invoked synchronously from the web layer.

When those preconditions hold, the web rewrite becomes the kind of project the Rust ecosystem is well-suited for: a thin axum + sqlx service that's mostly type-safe SQL queries and JSON serialization. Until then, the rewrite is a Trojan horse for the larger architectural change, and pretending otherwise produces requirements docs that ce-doc-review correctly rejects.

## Adjacent ideation (for a future brainstorm)

A separate brainstorm worth running, when the time is right, is **"cratedigger as a static / event-driven service rather than a cycle-driven script."** Open questions for that brainstorm:

- What replaces the 5-min systemd timer? (PG `LISTEN`/`NOTIFY` per state transition? Long-running daemon with internal scheduler? Both?)
- Where does the importer worker's responsibility end and the pipeline's begin under the new model?
- Is the harness (`harness/beets_harness.py`) hosted by the daemon or invoked per-import?
- What does the test taxonomy look like when there are no cycles to anchor "orchestration" tests against?
- How does the web layer interact with the new model?
- What's the migration path from the cycle-driven service to the static one — can they coexist for a deploy window?

That brainstorm is the right place to surface a Rust rewrite as a downstream consequence, not as the framing.

## What got reviewed and what was learned about the review process

Both attempts were saved by ce-doc-review:

- The Bottle+waitress doc would have shipped a 50-route migration built against a non-existent waitress feature. The kieran-python persona caught it in round 1 because that persona reads framework documentation, not just brainstorm prose.
- The Rust doc would have committed 3 weeks of focused work to a project whose scope was 2-3x the estimate. The feasibility persona caught it by actually counting LOC in the lib modules the web routes import, rather than trusting the doc's "thin route" framing.

**Lesson for future brainstorms:** when proposing a carve-out / migration / rewrite, the brainstorm dialogue should explicitly enumerate the imports / call-sites the carved-out piece depends on. The current `ce-brainstorm` flow doesn't force this — `ce-doc-review` catches it after-the-fact. Worth adding "what does this piece actually depend on?" as an explicit Phase 1.2 probe for migration-shaped proposals.

## Cross-references

- Issue #233 — original wedge incident with full forensics
- Issue #227 — sibling perf issue that surfaced the BrokenPipe alarm
- `docs/brainstorms/2026-05-09-web-stack-bottle-waitress-requirements.md` — superseded; the Bottle+waitress attempt
- `docs/brainstorms/2026-05-09-cratedigger-web-rust-rewrite-requirements.md` — superseded; the Rust attempt
- `cratedigger.py:1605-1617` — the end-of-cycle POST that triggers the storm
- `web/server.py:50-65` — `_try_reconnect_db()`
- `web/server.py:326-329, 362-365` — the catch-alls that fire it unconditionally
- `lib/pipeline_db.py:838-839` — the `advisory_lock` docstring that documents today's "single-threaded HTTPServer already serialises within its own session" assumption
