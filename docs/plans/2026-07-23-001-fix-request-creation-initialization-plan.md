# Request Creation Initialization — Plan (issue #791)

**Status:** implementation-ready after adversarial scope correction

**Issue:** https://github.com/abl030/cratedigger/issues/791

**Date:** 2026-07-23

## Verdict

The defect is real, but the issue overstates the normal-operation race and
combines concerns that do not need one solution.

Today the direct web/CLI Add paths commit a `wanted` request before tracks,
field-resolution attempts, and the initial search-plan outcome. A process crash
can therefore strand a `wanted` row at any of those boundaries. On the next
cycle, startup reconciliation treats every `wanted` row without an active plan
as eligible for plan generation; a crash immediately after `add_request()` can
therefore become a searchable title-only request even though its tracks and
resolution attempts were never persisted.

The ordinary in-flight search race described in the issue is narrower than
claimed. `get_wanted_searchable()` joins through a current active plan, and all
direct Add paths generate that plan after tracks and field resolution. A row
cannot search during the normal pre-plan window. The new-row Upgrade path is
the exception: it creates the plan before applying the upgrade quality fields,
so those fields belong before the final runnable transition.

Current production evidence shows no active incident:

- all 1,362 live `wanted` rows have an active plan;
- all 1,362 have at least one track;
- no `wanted`/no-plan row has search history;
- all 426 live Discogs rows are modern dual-written rows
  (`mb_release_id = discogs_release_id`); there are zero legacy
  Discogs-only rows and zero duplicate Discogs identities.

The issue's statement that every production creator already holds the RELEASE
lock is also stale. Current `main` has direct, unlocked `add_request()` calls in
the web Add/new-row Upgrade and CLI Add adapters. Modern MB and Discogs
cardinality is nevertheless protected by `UNIQUE (mb_release_id)`; lock sharing
is still useful for a clean create-or-resume service result, but it is not the
atomic-visibility mechanism.

## Corrected invariant

A request created by direct Add or the new-row Upgrade path remains in an
explicit, non-runnable `initializing` state until all initialization steps that
path owns have completed:

1. canonical request row exists;
2. canonical release tracks are persisted;
3. field values and resolution-attempt audit rows are persisted (including
   deliberate unresolved/timeout results);
4. an initial search-plan outcome is persisted (active, deterministic failure,
   or transient failure);
5. creation-only policy fields, including Upgrade quality intent, are applied.

Only then does one compare-and-set transition `initializing -> wanted` publish
the row to runnable/status-based consumers. An active plan alone never makes an
`initializing` row runnable because search eligibility still requires
`status = 'wanted'`.

An interrupted row is recoverable by reissuing the same Add/Upgrade operation.
The shared service recognizes the same exact identity in `initializing`,
idempotently replaces its tracks, upserts resolution attempts, ensures a plan
outcome exists, and retries the final transition. It never deletes the partial
row or invents an automatic irreversible cleanup policy.

## Why this design

Use the explicit lifecycle state, not a transaction spanning the current call
graph.

The existing resolver deliberately performs bounded external I/O and persists
audit rows while it runs. Search-plan persistence also owns its own explicit
transaction. Making request creation one PostgreSQL transaction would require
splitting both subsystems into prepare/persist halves and threading a
caller-owned transaction through several established APIs. That is a large
architectural refactor for a small crash window.

`initializing` reuses the current idempotent operations, gives every status-based
worker the correct exclusion automatically, preserves useful failure audit, and
provides a straightforward retry path. It is the least complicated durable
boundary, even though it adds one lifecycle value.

## Scope corrections

### In scope

- Web `POST /api/pipeline/add`, both MB and Discogs.
- Web new-row `POST /api/pipeline/upgrade`, both MB and Discogs.
- `pipeline-cli add`, both MB and Discogs.
- One shared creation service and matched CLI/API outcomes.
- RELEASE-lock serialization and an in-lock exact-identity recheck for those
  direct creators.
- Crash/resume, visibility, and concurrency tests.

### Out of scope

- **Replace.** `supersede_request_mbid()` already atomically commits the new
  row and its tracks with the old row's `replaced` transition. Replace does not
  run the add-time field resolver by established design, and the new row is not
  searchable until its plan becomes active. It does not have the dangerous
  row-without-tracks crash shape addressed here.
- **A cross-subsystem transactional creation framework.** The explicit state is
  the authoritative design; do not build both designs.
- **A new Discogs uniqueness migration.** Production has no legacy
  Discogs-only rows, and all current creators dual-write the existing unique
  `mb_release_id`. The RELEASE lock plus in-lock identity check covers the
  service boundary. A database-wide legacy identity redesign is not part of
  initialization atomicity.
- **Filtering diagnostic reads.** Generic `get_request`, unfiltered CLI list,
  search, and counts may expose `initializing` as exactly that. Runnable and
  ordinary queue cohorts must exclude it; hiding it from diagnostics would make
  crash recovery harder.
- Automatic deletion, timeout expiry, a background repair worker, or committed
  one-shot cleanup machinery.

## Key decisions

1. Add migration `063_request_initializing_status.sql`, extending only the
   `album_requests_status_check` domain with `initializing`. Existing rows need
   no backfill.
2. Add one `RequestCreationService` in `lib/request_creation_service.py`.
   Provider adapters still fetch and normalize their own source payloads, then
   feed the same typed creation input shape to the service; there is no MB to
   Discogs adapter.
3. Acquire the canonical RELEASE advisory lock before the authoritative
   identity recheck. A non-`initializing` existing row returns `exists`; an
   `initializing` row resumes; no row inserts with `status='initializing'`.
4. Keep the lock through DB initialization and the final compare-and-set. The
   release payload is fetched before lock acquisition, so the lock does not
   cover the initial mirror request. Resolver calls may still consume their
   existing bounded three-second budget. Creation therefore nests
   `RELEASE -> PLAN`; document that order and its deadlock analysis in
   `docs/advisory-locks.md` (no production path takes `PLAN -> RELEASE`).
5. Reuse `set_tracks`, `resolve_all` + `apply_resolve_all_result`, and
   `SearchPlanService.generate_for_new_request`. Do not introduce parallel
   resolution or plan-generation logic.
6. Treat a persisted successful, deterministic-failure, or transient-failure
   plan result as a completed plan step. Require `ServiceResult.plan_id` for
   every accepted outcome: `failed_transient` without a plan id is the existing
   "failure audit could not be persisted" shape and must leave the row
   `initializing`. Lock contention, an unexpected exception, or any failure
   before a plan result is durably recorded likewise leaves it initializing.
7. Apply new-row Upgrade's `search_filetype_override` and `min_bitrate` through
   the final `initializing -> wanted` transition, eliminating the current
   post-plan policy race.
8. Keep `PipelineDB.add_request()` as a low-level fixture/internal write, but
   add an AST call-site ratchet: production direct creators must go through the
   service. Tests and data fixtures may still seed rows directly.
9. Generic reads may return `initializing`; actionable Add/Upgrade adapters must
   not short-circuit it as an ordinary existing request. Reissuing the action is
   the recovery command.

## Implementation units

### U1 — Pin the invariant red-first

**Files:** `tests/test_request_creation_service.py`,
`tests/test_request_creation_generated.py`, `tests/test_pipeline_db.py`,
`tests/fakes/pipeline_db.py`.

- Add a deterministic real-PostgreSQL two-connection test that pauses creation
  after each persisted boundary and proves the observer never receives the row
  from `get_wanted_searchable()` until the final transition.
- Inject failure after row insert, tracks, resolution persistence, plan
  persistence, and immediately before finalization. Each pre-final state must
  remain `initializing`, have zero searches, and converge to exactly one
  `wanted` row when the same operation is retried.
- Add the required generated property over failure phase, MB vs Discogs
  identity, empty/non-empty optional resolved fields, and Add vs new-row Upgrade
  intent. The property drives the real service and asserts:
  `status != wanted OR initialization_complete`, never two rows per exact
  identity, and retry convergence.
- Add a known-bad checker self-test by deliberately modeling the initial insert
  as `wanted`; it must fail on the first injected boundary.
- Use real PostgreSQL for transaction visibility and advisory-lock assertions;
  use the fake only for the broader generated state space, with fake parity
  self-tests for the new status/transition.

### U2 — Add the provisional lifecycle edge

**Files:** `migrations/063_request_initializing_status.sql`,
`lib/transitions.py`, `lib/pipeline_db/requests.py`,
`lib/pipeline_db/rows.py` only if its status typing requires it,
`tests/test_migrator.py`, `tests/test_transitions.py`,
`tests/test_request_lifecycle_generated.py`, `docs/pipeline-db-schema.md`.

- Extend the status CHECK with `initializing`; do not alter existing rows.
- Add exactly one creation edge, `initializing -> wanted`, through the canonical
  transition/finalization boundary. `initializing` has no transition to
  downloading/imported/unsearchable/replaced through ordinary lifecycle APIs.
- Preserve the existing active-status graph for all other states.
- Ensure `count_by_status`, unfiltered reads, and typed row conversion accept the
  new stored value without treating it as active work.

### U3 — Centralize create-or-resume

**Files:** `lib/request_creation_service.py`,
`lib/pipeline_db/_shared.py` if a shared typed input belongs there,
`lib/field_resolver_service.py`,
`lib/search_plan_service.py` only if a small typed outcome helper is needed,
`tests/fakes/pipeline_db.py`, `docs/advisory-locks.md`.

- Define one typed input containing exact identity, canonical album metadata,
  raw source payload needed by the resolver, tracks, source, and optional
  Upgrade quality fields.
- Under RELEASE lock, re-read by exact identity and choose `exists`, `resume`,
  or `create`.
- Insert new rows as `initializing`; resume the same row idempotently.
- Persist tracks, resolve/apply fields with
  `expected_status='initializing'`, then generate the plan from the post-resolve
  tracks.
- Make the creation call strict about persistence while preserving the
  resolver's domain-level proceed-with-NULL behavior: failure to write any
  deferred resolution-attempt row, scalar field, or resolved track artist must
  leave the row `initializing`. Today `_DeferredRecorder.flush_to()` and
  `apply_resolve_all_result()` log and swallow those DB-write failures; the
  service needs a strict mode/result rather than mistaking a dropped write for
  successful initialization.
- Finalize with one compare-and-set to `wanted`, carrying Upgrade quality fields
  in that same statement.
- Return typed outcomes for `created`, `resumed`, `exists`, `busy`, and
  `initialization_failed`, including request id and a safe diagnostic. An
  ordinary resolver timeout is resolved audit data, not service failure.
- Never delete an interrupted row. Never turn an unexpected exception into
  `wanted`.

### U4 — Make adapters thin and symmetric

**Files:** `web/routes/pipeline_mutations.py`,
`scripts/pipeline_cli/album_requests.py`, relevant route/CLI tests,
`tests/test_ai_portability.py` or a focused new AST audit for direct production
`add_request()` calls.

- Replace the duplicated web/CLI add-time resolver and plan helpers with one
  service call.
- Preserve source-specific fetching/normalization, but map both providers into
  the same service input.
- Route new-row Upgrade through the same service with its quality intent; leave
  existing-row Upgrade on the current lifecycle transition path.
- Allow a preflight lookup to skip mirror I/O for a completed existing row, but
  never short-circuit `initializing`; the service must recheck again under the
  lock.
- Map service outcomes symmetrically: created/resumed success, exists success,
  busy retryable conflict, initialization failure server error with the request
  id retained for retry.
- Add a production call-site audit proving only the service (and the separate
  Replace transaction) may create request rows.

### U5 — Verification and live proof

- Focused tests while converging:
  - request-creation service + generated module;
  - PipelineDB real-PG visibility tests;
  - transitions/migrator;
  - web mutation routes and pipeline CLI.
- Review the exact final tree, commit it, then run exactly once before push:
  - `nix-shell --run "pyright --threads 4"`
  - `nix-shell --run "bash scripts/run_tests.sh"`
- Because this introduces a generated invariant, run
  `nix-shell --run "bash scripts/fuzz_burst.sh"` before push and promote any
  shrink to a named pin.
- Deploy with a strict migration hold. Verify migration 063, exact active
  source, controlled cycle, and ordinary successor cycle.
- Live verification:
  1. counts show zero stale `initializing` rows before the canary;
  2. use an operator-selected release that genuinely belongs in the collection
     (or observe the next real Add; do not add-and-delete a synthetic production
     request) and prove it reaches `wanted` with tracks, four field-resolution
     categories (resolved or explicitly unresolved), and a persisted plan
     outcome;
  3. prove it is returned by `get_wanted_searchable()` only after finalization;
  4. exercise the retry path with a disposable synthetic real-PG canary if no
     production operation naturally fails mid-creation;
  5. confirm no duplicate exact identity and no unexpected live
     `initializing` residue.

## Acceptance criteria for the corrected issue

- Direct Add and new-row Upgrade publish no `wanted` row until their owned
  initialization sequence has a persisted plan outcome.
- Search, unfindable detection, transfer ownership, and ordinary status-based
  work queues cannot act on `initializing` rows without adding bespoke reader
  filters.
- A failure at every persistence boundary is visible as `initializing`, has no
  search activity, and converges on retry to one request row.
- New-row Upgrade's quality intent is present in the same transition that makes
  the request `wanted`.
- Web and CLI use one service and have matched outcomes.
- Same-identity MB and Discogs direct creators are serialized under RELEASE and
  remain cardinality-preserving; the PostgreSQL unique constraint remains the
  final modern-row safety net.
- The invariant has a deterministic pin, generated property, known-bad
  self-test, and real-PostgreSQL visibility/concurrency proof.
