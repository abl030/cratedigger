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
audit state, and mark only `would_import` jobs as importable for the serial
worker.

Outstanding follow-up: after the preview-gated queue has run in production,
inventory IMPORT/RELEASE lock call sites and remove any lock whose only
remaining purpose was cross-entrypoint beets import ownership. Until then, keep
the locks as defensive guards around the existing dispatch internals. Tracking
issue: <https://github.com/abl030/cratedigger/issues/169>.

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

All namespace constants live in `lib/pipeline_db.py`. The key space is
PostgreSQL's two-arg `pg_advisory_lock(int4, int4)` — first arg is the
namespace, second is the per-lock key.

| Namespace | Constant | Hex | ASCII | Key | Scope |
|---|---|---|---|---|---|
| Per-request import | `ADVISORY_LOCK_NAMESPACE_IMPORT` | `0x46494D50` | "FIMP" | `request_id` | Force/manual-import double-click protection |
| Per-release pipeline | `ADVISORY_LOCK_NAMESPACE_RELEASE` | `0x52454C45` | "RELE" | `release_id_to_lock_key(mb_release_id)` | Cross-process same-MBID serialisation |
| Importer worker | `ADVISORY_LOCK_NAMESPACE_IMPORTER` | `0x51554555` | "QUEU" | `1` | One importer process drains the beets-mutating lane |

The ASCII-visible hex lets `pg_locks` rows be interpreted at a glance
during debugging:

```sql
SELECT classid, objid FROM pg_locks WHERE locktype = 'advisory';
-- classid=0x46494D50 → force/manual-import lock
-- classid=0x52454C45 → release-level lock
-- classid=0x51554555 → importer-worker singleton lock
```

### IMPORT — per-request lock

**Why**: Issue #92. A double-click on the force-import button in the
web UI could fire two HTTP POSTs that each launched the full pipeline
on the same `request_id`, writing duplicate `download_log` rows and
running `import_one.py` twice against the same files. The second caller
would crash or produce bogus state.

**Scope**: Held by `dispatch_import_from_db` inside the importer worker.
Web and CLI force/manual paths no longer call this directly; they dedupe at
`import_jobs` enqueue time. Keep this lock until a follow-up cleanup proves the
queue invariant fully replaces the old double-click protection.

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

**Scope**: Held for the duration of every `import_one.py` subprocess
— that is, in every path that runs the harness. `dispatch_import_core`
is the funnel; both the auto path and the force/manual path go through
it.

**Key**: `release_id_to_lock_key(mb_release_id)` — a 31-bit
`zlib.crc32` mask of the (`.strip()`-normalised) release id string.
Covers both MB UUIDs and Discogs numeric IDs since both share the
`mb_release_id` column. See the docstring on `release_id_to_lock_key`
in `lib/pipeline_db.py` for the collision analysis (probability
~N²/2^31; false collision delays an unrelated release by one cycle).

### IMPORTER — worker singleton lock

**Why**: The import queue is the durable state owner, but beets mutation is
still intentionally one lane. A second accidentally-started importer must not
claim another queued job in parallel, and it must not requeue a live worker's
`running` job during startup recovery.

**Scope**: Held by `scripts/importer.py` for the full worker process lifetime
before it requeues abandoned `running` jobs or claims new jobs.

**Key**: Constant `1`. There is only one logical importer lane.

## Acquisition order

Force/manual paths hold both locks at once. **IMPORT is outer, RELEASE
is inner.** Always. A reverse nesting would risk a cross-process
deadlock if two flows acquire in opposite order, but because RELEASE is
taken by the same session further down the call graph and no other
call site acquires both locks, in practice there is only one ordering
to follow and it's the one in the code:

```
FORCE/MANUAL (dispatch_import_from_db)
  └─ acquire IMPORT(request_id)                                ← outer
      └─ _dispatch_import_from_db_locked
          └─ dispatch_import_core
              └─ acquire RELEASE(release_id_to_lock_key(mbid)) ← inner
                  └─ import_one.py subprocess
```

The auto path only holds RELEASE, and acquires it at
`_handle_valid_result` *before* `StagedAlbum.move_to(...)` runs (Codex PR #136 R4
P1 — see below). `_handle_valid_result` now calls
`dispatch_import_core` directly; its inner acquisition of the same
RELEASE key is therefore just a no-op reentrant acquire against the
outer lock already held by the auto path:

```
AUTO (_handle_valid_result in lib/download.py)
  └─ acquire RELEASE(release_id_to_lock_key(mbid))             ← outer
      └─ staged_album.move_to(...)                             ← moves files + current_path
      └─ dispatch_import_core
          └─ acquire RELEASE(...)                              ← reentrant no-op
              └─ import_one.py subprocess
```

**Why RELEASE outer at `_handle_valid_result`, not at
`dispatch_import_core`?** The staged move now mutates both filesystem
state (`slskd_download_dir/<import_folder>/` →
`beets_staging_dir/<mode>/<artist>/<album> [request-N]`) and
`active_download_state.current_path`. The staged path is request-scoped
and branch-scoped (`auto-import/` vs `post-validation/`) so crash
recovery can tell whether a row may already have launched an import
subprocess, and two same-name editions can never collide on the same
persisted `current_path`. Acquiring RELEASE first still keeps
contention as a true no-op: no directory churn, no extra JSONB write,
and no cross-process ambiguity about which local path currently owns the
album. On contention, files stay where `process_completed_album`
expects them and the next cycle simply re-enters with the same
`current_path`.

If the process dies after moving into `beets_staging_dir`, the persisted
staging root tells the next poll whether the row is on the
`auto-import/` or `post-validation/` branch. `auto-import/` retries are
blocked for manual recovery because an import subprocess may already
have started; `post-validation/` retries re-enter normally because
`StagedAlbum.move_to(...)` is idempotent when the album is already at
the staged destination. For blocked auto-import rows,
operator recovery should inspect the staged path and either finish the
import manually or reset the request explicitly.

To list candidate blocked rows, query for `status='downloading'` entries
whose persisted `current_path` already points at your staging root:

```bash
pipeline-cli query "
SELECT id,
       artist_name,
       album_title,
       active_download_state->>'current_path' AS current_path
FROM album_requests
WHERE status = 'downloading'
  AND active_download_state->>'current_path' LIKE '<beets_staging_dir>%';
"
```

## Contention behaviour

All acquires are non-blocking via `pg_try_advisory_lock`. On
contention:

- **IMPORT contention** (force/manual): log `SKIPPED: request N —
  another import is already in progress` and return a
  `DispatchOutcome(success=False, message=...)` so the UI surfaces a
  "try again shortly" toast. The second caller writes nothing.
- **RELEASE contention** (auto): log `AUTO-IMPORT DEFERRED` and return
  `DispatchOutcome(deferred=True)`. `_run_completed_processing`
  branches on `deferred` and preserves the `downloading` status with
  its `active_download_state` intact — the next cycle idempotently
  re-enters `process_completed_album` and retries exactly where we
  stopped. Codex PR #136 R3 P2/P3.
- **RELEASE contention** (force/manual): log `FORCE-IMPORT SKIPPED` /
  `MANUAL-IMPORT SKIPPED`, return `DispatchOutcome(success=False,
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

Cratedigger exploits this in the auto path: `_handle_valid_result`
acquires RELEASE, `dispatch_import_core` acquires it again
(reentrantly), the inner release is a no-op, the outer release is the
real one. The design keeps `dispatch_import_core`'s lock scope correct
for the force/manual path (where it IS the first acquisition) without
double-gating the auto path.

**Scope**: reentrancy is per-session, not per-process. A single
Cratedigger process does hold multiple `PipelineDB` instances in
practice — the auto pipeline has `phase1_source` and `phase2_source`
each owning their own session, `album_source.py` lazily opens another,
and the web server opens yet one more. Every `advisory_lock()` call
must go through the same `PipelineDB` instance as its matching outer
acquire for the reentrant no-op to apply. The auto path and the
force/manual path both thread the same
`ctx.pipeline_db_source._get_db()` / `db` reference from the outer
acquire down into `dispatch_import_core`, so they stay within one
session. If a future change opens a fresh `PipelineDB` for the inner
acquire, the second `pg_try_advisory_lock` comes from a different
session and returns False — revisit the ordering rules.

## Call-site index

| Path | File | Function | Namespace | Key expression |
|---|---|---|---|---|
| Auto-import outer | `lib/download.py` | `_handle_valid_result` | RELEASE | `release_id_to_lock_key(album_data.mb_release_id)` |
| Auto + force/manual inner | `lib/import_dispatch.py` | `dispatch_import_core` | RELEASE | `release_id_to_lock_key(mb_release_id)` |
| Force/manual outer | `lib/import_dispatch.py` | `dispatch_import_from_db` | IMPORT | `request_id` |
| Importer worker singleton | `scripts/importer.py` | `main` | IMPORTER | `1` |
| Import queue dedupe | `lib/pipeline_db.py` | `enqueue_import_job` | unique index | `dedupe_key` |

Every acquire site carries a comment linking back here. Line numbers
are intentionally omitted — grep for `advisory_lock(` to find them.
`git log -S 'advisory_lock(' -- lib/ scripts/` is the archaeology path.

## Extending

To add a new lock:

1. Pick a namespace constant with an ASCII-recognisable hex value (make
   `pg_locks` debuggable). Define it in `lib/pipeline_db.py` next to
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
