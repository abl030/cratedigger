# PostgreSQL Advisory Locks

Cratedigger uses PostgreSQL advisory locks to serialise pipeline
operations that must not run concurrently across different DB sessions.
Import entry points now enqueue `import_jobs`; the long-lived importer worker
is the intended owner of beets-mutating work. The advisory locks below remain
as defensive guards inside the existing dispatch internals until a cleanup pass
proves they are redundant. Every lock in this codebase is
**non-blocking** (`pg_try_advisory_lock`), **session-scoped** (held until
release or session close), and **reentrant within a session** (a second
acquire of the same `(namespace, key)` in the same session always
returns true).

Async import preview workers do not take the importer singleton lock because
they must not mutate beets or source folders. They claim `import_jobs` preview
work through row-level `FOR UPDATE SKIP LOCKED` semantics, persist preview
audit state, and mark jobs `evidence_ready` for the serial worker's final
action-time evidence check. Historical `would_import` rows remain display/audit
data only and are not runnable.

IMPORT/RELEASE are now part of the processing-owner correctness boundary, not
merely defensive queue-era guards. Do not remove or narrow them as queue
cleanup: the exact session, ordering, and owner rereads are what fence
filesystem/Beets work from recovery and operator invalidators.

This doc is the single source of truth for namespaces, keys, ordering,
and reentrancy. Add a new lock only after reading the rules below and
updating both the **namespace table** and the **call-site index**.

## Why advisory locks

We do not use row-level locks because the thing we're serialising on —
"don't let two processes import the same release at the same time" —
spans multiple statements across multiple tables (`album_requests`,
`download_log`, beets' own SQLite DB, and filesystem state in
`/Incoming` and `/Beets`). A row lock on `album_requests` would only
cover the row-level updates; the subprocess calls to `import_one.py`
run outside the transaction envelope.

Advisory locks are:

- Cheap (two `pg_locks` entries, no table rows touched)
- Orthogonal to row locks (don't interfere with autocommit writes)
- Easy to name (int4 pairs — we use ASCII-recognisable values so
  `pg_locks` is debuggable at a glance)
- Easy to scope (we pick per-request vs per-release based on the
  invariant we're protecting)

## Namespaces

All namespace constants live in `lib/pipeline_db/_shared.py`. The key space is
PostgreSQL's two-arg `pg_try_advisory_lock(int4, int4)` — first arg is the
namespace, second is the per-lock key.

| Namespace | Constant | Hex | ASCII | Key | Scope |
|---|---|---|---|---|---|
| Per-request import | `ADVISORY_LOCK_NAMESPACE_IMPORT` | `0x46494D50` | "FIMP" | `request_id` | Exact processing-owner session and operator invalidator fence |
| Per-release pipeline | `ADVISORY_LOCK_NAMESPACE_RELEASE` | `0x52454C45` | "RELE" | `release_id_to_lock_key(mb_release_id)` | Cross-process same-MBID serialisation |
| Importer worker | `ADVISORY_LOCK_NAMESPACE_IMPORTER` | `0x51554555` | "QUEU" | `1` | One importer process drains the beets-mutating lane |
| Per-request plan | `ADVISORY_LOCK_NAMESPACE_PLAN` | `0x504C414E` | "PLAN" | `request_id` | Search-plan generation / supersession serialisation |
| Wrong-match cleanup | `ADVISORY_LOCK_NAMESPACE_WRONG_MATCH_CLEANUP` | `0x574D434C` | "WMCL" | `wrong_match_cleanup_lock_key(request_id, download_log_id, source_path)` | Serialise deletion of one wrong-match source |
| YT ingest worker | `ADVISORY_LOCK_NAMESPACE_YOUTUBE_INGEST` | `0x59544942` | "YTIB" | `1` | One `cratedigger-youtube-ingest.service` instance drains the YT rescue queue (`download_log` rows where `source='youtube'` AND `outcome='youtube_running'`) |

The ASCII-visible hex lets `pg_locks` rows be interpreted at a glance
during debugging:

```sql
SELECT classid, objid FROM pg_locks WHERE locktype = 'advisory';
-- classid=0x46494D50 → force-import lock
-- classid=0x52454C45 → release-level lock
-- classid=0x51554555 → importer-worker singleton lock
-- classid=0x574D434C → wrong-match cleanup lock
```

### IMPORT — per-request lock

**Why**: Issue #92. A double-click on the force-import button in the
web UI could fire two HTTP POSTs that each launched the full pipeline
on the same `request_id`, writing duplicate `download_log` rows and
running `import_one.py` twice against the same files. The second caller
would crash or produce bogus state.

**Scope**: The witnessed automation handoff acquires IMPORT before creating the
owner. Preview and importer each use a dedicated non-pooled `PipelineDB`
session and retain IMPORT from the first durable owner recheck through every
filesystem/Beets mutation and the stage/terminal commit. Replace, force-import,
ban-source, request-backed library delete, direct pipeline delete, generic
lifecycle/intent/quality mutations, and owner recovery use the same lock before
their authoritative reread. A processing owner therefore makes every
incompatible action a typed, zero-mutation `processing_locked` conflict.

**Key**: The raw `request_id` (int4 auto-increment on
`album_requests.id` — fits trivially in an int4 lock key).

### RELEASE — per-MBID lock

**Why**: Issue #132 P1 / issue #133. Defends against a cross-process
race that could cause Palo Santo-*class* data loss (the 2026-04-20
incident itself had a different proximate cause — see `CLAUDE.md` §
Resolved canonical RCs — but this race is a real independent vector
worth closing).

The historical race: two processes (the auto cycle and a web force-import, or
two racing force-import clicks on sibling requests for the same MBID)
each hold their own per-request lock while targeting the same
MusicBrainz release. The harness's post-import `max(post_import_ids)`
query then picks up the *other* process's newly-inserted beets row as
"the album we just imported" and `beet remove -d`-es it — the wrong
album's files vanish.

**Scope**: Held for the duration of every `import_one.py` subprocess and its
release-specific filesystem/terminal work. `dispatch_import_core` is the
funnel. Automation acquires RELEASE inside its already-pinned IMPORT session;
force and other request-backed destructive paths use the same order. It also
serializes direct request creation in `RequestCreationService`:
Add and new-row Upgrade hold the exact release lock while rechecking identity,
persisting their provisional row, and nesting the per-request PLAN lock before
the final publication CAS.

**Key**: `release_id_to_lock_key(mb_release_id)` — a 31-bit
`zlib.crc32` mask of the (`.strip()`-normalised) release id string.
Covers both MB UUIDs and Discogs numeric IDs since both share the
`mb_release_id` column. See the docstring on `release_id_to_lock_key`
in `lib/pipeline_db/_shared.py` for the collision analysis (probability
~N²/2^31; false collision delays an unrelated release by one cycle).

### PLAN — per-request plan-generation lock

**Why**: `feat: persisted search plans` (U3). The plan-generation service is
called from CLI add, web add, startup reconciliation, and explicit
regeneration. Two concurrent calls for the same `request_id` could both
read no-active-plan, both insert a new active plan, and trip the partial
unique index on `search_plans(request_id) WHERE status='active'` —
turning a benign race into a hard failure. The lock serialises generation
attempts so only one runs end-to-end per request at a time.

**Scope**: Held by `SearchPlanService` for the duration of a single
generate-and-persist attempt (snapshot construction → generator →
`create_successful_search_plan` / `supersede_search_plan_with_replacement` /
`create_failed_search_plan`). Released before the service returns.

**Key**: The raw `request_id`. Independent from IMPORT (different
namespace) so a force-import and an explicit regeneration can run
concurrently for the same request without deadlocking.

### Request creation lock order

Direct Add and new-row Upgrade use `RequestCreationService`. It acquires
`RELEASE` first, rechecks exact identity, then calls `SearchPlanService`, which
acquires `PLAN` for the new request. The required nesting is therefore
`RELEASE → PLAN`. No production path takes `PLAN → RELEASE`; that absence is
the deadlock argument. The source mirror fetch intentionally happens before
`RELEASE`, so a slow metadata request never holds the cross-process lock.

### Request-association writers

`request_identity.acceptable_identities(row)` is the inverse-association
authority used by Library Delete: every non-`replaced` request contributes its
acquisition identity and any stored canonical survivor. A writer that adds or
removes one of those memberships holds every affected RELEASE lock so an
under-lock inverse reread remains authoritative until its exact Beets child
completes.

Request-backed writers take `IMPORT(request_id)` first, then the deduplicated
set of RELEASE **keys** in ascending numeric order. The shared
`release_identity_locks` helper deduplicates hash collisions by key and unwinds
earlier acquisitions if a later non-blocking acquire contends. A collision is a
safe, temporarily wider serialization boundary; it never permits a missing
lock. There is no reverse `RELEASE -> IMPORT` path. Direct creation has no
existing request, so its established `RELEASE -> PLAN` nesting remains the
only non-request-backed order.

| Writer | Before identities | After identities | Lock scope / contention |
|---|---|---|---|
| Direct Add / new-row Upgrade | none | acquisition | Existing RELEASE -> PLAN scope includes provisional publication and resume; audited in #1070, no gap found. |
| Canonical reconcile | acquisition + old canonical | acquisition + resolved canonical | Mirror resolution is outside locks; IMPORT then union RELEASE locks, fresh reread/CAS. Busy retries on the next sweep. |
| Canonical retire | acquisition + stored canonical | acquisition | IMPORT then before RELEASE locks, fresh reread/CAS. Busy is a typed retryable conflict. |
| Replace | old acquisition + old canonical | target acquisition | IMPORT then union RELEASE locks through old-row supersede, new-row publication, and old-library cleanup. Target collision is rechecked under lock. |
| Pipeline Delete / ghost cleanup | current acceptable identities | none | IMPORT then before RELEASE locks, fresh reread and conditional delete. Contention or a stale association is zero mutation. |
| Library Delete with pipeline purge | current acceptable identities + filed Beets identity | none | IMPORT then sorted union RELEASE locks, fresh request/Beets reread and exact child delete. A survivor-filed merged row still locks its acquisition identity before removing the pipeline row. |

### IMPORTER — worker singleton lock

**Why**: The import queue is the durable state owner, but beets mutation is
still intentionally one lane. A second accidentally-started importer must not
claim another queued job in parallel, and it must not requeue a live worker's
`running` job during startup recovery.

**Scope**: Held by `scripts/importer.py` for the full worker process lifetime
before it requeues abandoned `running` jobs or claims new jobs.

**Key**: Constant `1`. There is only one logical importer lane.

### WMCL — wrong-match cleanup lock

**Why**: Wrong Matches cleanup may be triggered from the web bulk cleanup, CLI
bulk cleanup, post-rejection download path, Converge, and failed force-import
cleanup. All of these can target the same source folder. The cleanup service
checks active import jobs before deletion, then takes this lock and checks active
jobs again before deleting the folder.

**Scope**: Held by `lib.wrong_match_cleanup_service.cleanup_wrong_match` only
around the final active-job recheck and filesystem delete/DB clear. Evaluation
against existing evidence happens before the lock so unrelated rows are not
blocked by quality classification work.

**Key**: `wrong_match_cleanup_lock_key(request_id, download_log_id,
source_path)` hashes the exact source path with the row/request identifiers.
This protects one source folder without serialising the whole Wrong Matches
queue.

## Acquisition order

Any request-backed operation that needs both locks uses **IMPORT outer,
RELEASE inner**, on one pinned database session:

```
AUTOMATION PREVIEW / IMPORT / RECOVERY
  └─ acquire IMPORT(request_id)                                ← pinned session
      └─ re-read exact processing owner and execution lease
      └─ acquire RELEASE(release_id_to_lock_key(release_id))
          └─ filesystem / validation / Beets / cleanup
          └─ exact-owner stage or terminal commit
```

The handoff itself takes IMPORT before its locked
`downloading + enqueued_at` CAS and commits the owner before any filesystem
effect. Preview and importer never reconnect inside the owner scope: their
runtime context borrows the same `PipelineDB`, an owner-session watchdog checks
that exact connection, and cancellation prevents each next mutation if the
session is lost. The persisted execution lease is liveness evidence only; the
request's owner pointer remains the authority.

Destructive operator actions follow the same order. Ban-source always has a
pipeline request and therefore takes IMPORT then RELEASE. Library-delete takes
IMPORT then RELEASE when a request exists, and RELEASE only for a library-only
album. Replace, force-import, direct pipeline delete, lifecycle/intent/quality
changes, and automated owner convergence all acquire IMPORT before the durable
owner reread.
Authority rejection is a 409 / CLI exit 4 with zero filesystem, audit, job, or
request mutation.

### Exact-session rule

An advisory lock is authority held by one PostgreSQL backend, not a token that
survives reconnect. While any advisory-lock scope is open, `PipelineDB`
disables its normal autocommit reconnect/replay convenience. A lost or replaced
connection raises `AdvisoryLockSessionLost` and the short association writer
returns its typed busy/retry result without replaying the failed statement on a
fresh backend. This applies uniformly to direct `RELEASE -> PLAN` creation and
to every request-backed `IMPORT -> RELEASE` writer. This is a **SQL
no-replay** guarantee only: it does not prove that a Beets child or filesystem
operation remained fenced while a backend died. In particular, a Replace whose
supersede already committed surfaces its runnable descendant rather than
inviting a second Replace, but D0 does not validate the external cleanup tail.
Issue #1071 is the required pre-deploy successor for cancellation/fencing of
external children and resumable post-supersede cleanup; do not infer either
property from this lock scope.

Inside a processing transaction, row locks have their own fixed order:
request, every job for that request in ID order, then cleanup journals in job
ID order. This lets deferred owner/journal integrity triggers serialize with
handoff and terminalization instead of validating an unlocked snapshot.

Startup convergence follows three asymmetric crash cases:

1. Before launch authorization, a positively dead execution may requeue the
   same exact owner.
2. After authorization, a positively dead execution records failed audit
   evidence, completes or truthfully refuses its exact journaled cleanup,
   fails the job, and returns the request to `wanted` in one terminal bundle.
   Neither heartbeat age nor path/library inference can authorize this step.
3. After the terminal bundle commits, the owner is cleared, cleanup receipt
   consumed, and the job is terminal.

Historical `recovery_required` owners use case 2 after the same exact liveness
proof. Cleanup remains inside the owner boundary, not best-effort work after
authority has been cleared.

## Contention behaviour

All acquires are non-blocking via `pg_try_advisory_lock`. On
contention:

- **IMPORT contention**: the downloader handoff leaves the exact
  `downloading + enqueued_at` incarnation untouched; an existing processor or
  operator action returns a typed busy/conflict result with no filesystem,
  request, job, audit, or policy write.
- **RELEASE contention** (automation owner): the same exact owning job
  requeues under its still-live execution and the request remains
  `processing`; it is never terminalized or detached merely because another
  request targets the release.
- **RELEASE contention** (force-import): log `FORCE-IMPORT SKIPPED`,
  return `DispatchOutcome(success=False,
  deferred=False)`, no state mutated. Same UI message as IMPORT
  contention.

Blocking acquires (`pg_advisory_lock`) are never used — they would
pin the caller's PG connection for the full duration of an unrelated
process's import (minutes) with no clear benefit.

## Reentrancy

PostgreSQL advisory locks are reentrant *within a session*. Acquiring
`(namespace, key)` twice from the same session returns true both
times; two releases are needed. Two *different* sessions never both
hold the same lock — the second caller's `pg_try_advisory_lock`
returns false.

**Scope**: reentrancy is per-session, not per-process. Processing code must
therefore thread its dedicated pinned `PipelineDB` through the runtime context
and dispatch/terminal layers. A lazy `AlbumSource` connection, pooled
replacement, or reconnect after watchdog failure is a different session:
IMPORT/RELEASE may then contend with the still-live owner rather than nest, and
the code must fail-stop. Runtime contexts borrow the pinned session and never
close it; the outer preview/importer scope owns closure after all locks and
watchdogs have stopped.

## Call-site index

| Path | File | Function | Namespace | Key expression |
|---|---|---|---|---|
| Witnessed automation handoff | `lib/pipeline_db/import_jobs.py` | `PipelineDB.handoff_automation_import` | IMPORT | `request_id` |
| Automation preview owner scope | `scripts/import_preview_worker.py` | `_process_automation_claim` | IMPORT then RELEASE in the borrowed runtime path | `request_id`; `release_id_to_lock_key(release_id)` |
| Automation importer owner scope | `scripts/importer.py` | `_process_automation_claim` | IMPORT then RELEASE | `request_id`; `release_id_to_lock_key(release_id)` |
| Automation startup convergence | `lib/pipeline_db/import_jobs.py` | `PipelineDB.recover_automation_import_job` | IMPORT then RELEASE | `request_id`; `release_id_to_lock_key(release_id)` |
| Auto + force-import dispatch | `lib/dispatch/core.py` | `dispatch_import_core` | RELEASE | `release_id_to_lock_key(mb_release_id)` |
| Direct Add / new-row Upgrade | `lib/request_creation_service.py` | `RequestCreationService.create_or_resume` | RELEASE then PLAN | `release_id_to_lock_key(creation.release_id)`; `request_id` |
| Canonical reconcile / retire | `lib/canonical_release_service.py` | `CanonicalReleaseService.reconcile_row` / `retire_request` | IMPORT then sorted RELEASE union | `request_id`; `acceptable_identities(before) ∪ acceptable_identities(after)` |
| Force-import outer | `lib/dispatch/entry_points.py` | `dispatch_import_from_db` | IMPORT | `request_id` |
| Ban-source destructive action | `lib/destructive_release_service.py` | `ban_source` | IMPORT then RELEASE | `request_id`; `release_id_to_lock_key(server release id)` |
| Library-delete destructive action | `lib/destructive_release_service.py` | `delete_release_from_library` | IMPORT then RELEASE, or RELEASE only without a pipeline row; ambiguous dual or malformed-nonempty album identity rejects before locks | server-derived pipeline request id; `release_id_to_lock_key(server release id)` |
| Replace operator action | `lib/mbid_replace_service.py` | `MbidReplaceService.replace_request_mbid` | IMPORT then sorted RELEASE union | `request_id`; old acceptable identities plus target identity |
| Direct pipeline delete | `lib/pipeline_delete_service.py` | `delete_pipeline_request` | IMPORT then sorted RELEASE before-set | `request_id`; current acceptable identities |
| Ghost imported cleanup | `scripts/cleanup_ghost_imported.py` | `cmd_apply` | delegates to direct pipeline delete | no direct DB deletion |
| Importer worker singleton | `scripts/importer.py` | `main` | IMPORTER | `1` |
| Import queue dedupe/owner commands | `lib/pipeline_db/import_jobs.py` | enqueue, claim, heartbeat, recovery, terminal helpers | unique index and IMPORT | `dedupe_key`; `request_id` |
| Plan generation | `lib/search_plan_service.py` | `SearchPlanService.generate_for_new_request` / `generate_for_request` | PLAN | `request_id` |
| Wrong-match cleanup | `lib/wrong_match_cleanup_service.py` | `cleanup_wrong_match` | WMCL | `wrong_match_cleanup_lock_key(request_id, download_log_id, resolved_path)` |

Library delete holds its locks through the pinned-Beets filesystem/metadata
postcondition and optional PostgreSQL purge. Plex/Jellyfin discovery and refresh
are slow best-effort network work and run only after both locks are released.

Plan generation acquires the PLAN lock before reading the request,
building the snapshot, invoking the generator, and persisting the
replacement plan/cursor state.

Every acquire site carries a comment linking back here. Line numbers
are intentionally omitted — grep for `advisory_lock(` to find them.
`git log -S 'advisory_lock(' -- lib/ scripts/` is the archaeology path.

## Extending

To add a new lock:

1. Pick a namespace constant with an ASCII-recognisable hex value (make
   `pg_locks` debuggable). Define it in `lib/pipeline_db/_shared.py` next to
   the existing `ADVISORY_LOCK_NAMESPACE_*` constants.
2. Decide key derivation. Natural-int keys (request_id) are trivial.
   String keys need a stable hash — use `zlib.crc32(...) & 0x7FFFFFFF`
   and follow the collision analysis pattern in
   `release_id_to_lock_key`.
3. If the new lock can be held concurrently with IMPORT or RELEASE,
   decide the ordering and document it here. Add a deadlock analysis
   in the commit message.
4. Audit every `PipelineDB(...)` construction site the acquire can
   reach. Advisory locks are **session-scoped**; if the caller runs
   through a different `PipelineDB` instance than its matching outer
   acquire, the inner `pg_try_advisory_lock` comes from a different
   session and returns False. The auto path's reentrant no-op works
   only because the same `ctx.pipeline_db_source` flows through the
   whole chain; a new lock that spans web + CLI + auto needs a
   design-level decision.
5. Add a row to the **Namespaces** and **Call-site index** tables in
   this doc.
6. Every acquire site must carry a comment referencing this doc
   (`See docs/advisory-locks.md.`).
7. Add a test in `tests/test_pipeline_db.py`'s `TestAdvisoryLock`
   class exercising the new namespace. `FakePipelineDB` already
   covers the contract side via `advisory_lock_calls` and
   `set_advisory_lock_result` (the fake records calls regardless of
   namespace — no fake update needed unless the new namespace
   requires per-key deterministic behaviour in some slice test, in
   which case extend `set_advisory_lock_result`'s callable form).
8. Verify on-host before calling it shipped. Unit tests prove the
   semantics; the cross-process story (race with the 5-minute timer,
   race with a web force-import) only manifests in a running
   pipeline. Watch `pg_locks` during a deliberate race if you are
   unsure. `nix build .#checks.x86_64-linux.moduleVm` does NOT
   exercise cross-process lock behaviour — it's a smoke test for
   module wiring only.

## Test coverage

- `tests/test_pipeline_db.py::TestAdvisoryLock` — real PG semantics:
  same-key blocking across sessions, different-key no-contention,
  cross-namespace same-key isolation, exception-safe release,
  same-session reentrancy.
- `tests/test_integration_slices.py::TestReleaseLockContention`
  and `::TestHandleValidResultReleaseLock` — release-lock contention
  on the auto path at `_handle_valid_result` and the
  `dispatch_import_core` inner site.
- `tests/test_dispatch_from_db.py` — IMPORT-lock double-acquisition
  short-circuits without writing a `download_log` row, running a
  subprocess, transitioning status, or firing cooldowns (fast-fail).
- `tests/test_fakes.py` — `FakePipelineDB.advisory_lock` records calls
  and lets tests flip acquisition results per-`(namespace, key)`.
- `tests/test_wrong_match_cleanup_service.py` — cleanup lock contention returns
  `skipped_active_job` and keeps the source folder and wrong-match pointer.
