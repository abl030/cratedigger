# Code Improvement Ideas

Notes from the async-downloads implementation (2026-04-03). These are structural improvements that would make the codebase easier to work with and catch more bugs.

## 1. TestConfig factory instead of MagicMock

**Problem**: Tests create `CratediggerConfig` via `MagicMock()`, which means pyright can't check attribute names. About 10 pyright "errors" across test files are `Cannot assign to attribute "stalled_timeout" for class "CratediggerConfig"`. Real type errors are invisible in this noise.

**Fix**: Create a `TestConfig` factory that returns a real `CratediggerConfig` with test defaults:

```python
def make_test_config(**overrides) -> CratediggerConfig:
    defaults = {
        "slskd_download_dir": "/tmp/test_downloads",
        "stalled_timeout": 300,
        "remote_queue_timeout": 120,
        "beets_validation_enabled": False,
        ...
    }
    defaults.update(overrides)
    return CratediggerConfig(**defaults)
```

Tests get `ctx.cfg.stalled_timeout = 600` → `make_test_config(stalled_timeout=600)`. Pyright catches typos at write time. The mock noise vanishes.

## 2. Status exhaustiveness contract test

**Problem**: Adding `downloading` required grep-and-pray across 14 locations in 6 files (JS badge rendering, Python route iteration, CLI display, CSS classes). One missed location means a silent rendering gap.

**Fix**: A single source-of-truth constant and contract test:

```python
# lib/pipeline_db.py
VALID_STATUSES = ("wanted", "downloading", "imported", "unsearchable")

# tests/test_pipeline_db.py
def test_status_exhaustiveness(self):
    """Every status the DB allows must appear in the pipeline route iteration."""
    from web.routes.pipeline import STATUS_ITERATION_ORDER
    self.assertEqual(set(STATUS_ITERATION_ORDER), set(VALID_STATUSES))
```

JS could import a shared `STATUSES` array from a single module. When a new status is added, the contract test fails immediately, pointing at every file that needs updating.

## 3. `process_completed_album` status contract

**Problem**: `poll_active_downloads` has a "safety net" that checks whether `process_completed_album` set the DB status or not. This exists because `process_completed_album` sometimes sets status (via `mark_done`/`reject_and_requeue` in the beets validation path) and sometimes doesn't (when beets validation is disabled). The caller has to guess.

**Fix**: Return a typed result instead of a bool:

```python
@dataclass
class CompletionResult:
    success: bool
    status_set: bool  # Did this function update album_requests.status?
```

Or simpler: have `process_completed_album` always set status, even in the no-beets path. The safety net in `poll_active_downloads` becomes dead code.

## 4. Extract cratedigger.py globals into a proper entry point

**SOLVED (issue #1278 item 2, 2026-08-31)**: the module globals and closure
wrappers are gone — `main()` builds one `CratediggerContext` and hands off to
`run_cycle(ctx, *, phase1_source_factory=...)`, and
`tests/test_convergence_runner_generated.py::TestRunCycleExecutable` is
exactly the full-cycle integration test this item said was impossible.

**Deepened (issue #1313 candidate 3, 2026-09-01)**: the context's wired-in
collaborators moved into a frozen, all-required `CycleCollaborators` (and its
slskd-less sibling `WorkerCollaborators`), built by
`cratedigger.build_cycle_collaborators`; `main()`'s tail is
`run_startup_and_cycle`, which a test drives. Forgetting a collaborator at a
construction site is now a type error, so the 888-line hand-registered AST
audit that used to hold the set of construction sites is gone.

## 5. Configurable/injectable `time.sleep` in `slskd_do_enqueue`

**Problem**: `slskd_do_enqueue()` has a hardcoded `time.sleep(5)` between enqueue and status check. Every test path that exercises re-enqueue (retry logic in `poll_active_downloads`, the old `_handle_download_problems`) pays this 5-second tax per call. The poll test suite takes ~35 seconds, mostly from `time.sleep`.

**Fix**: Either:
- Accept a `delay` parameter defaulting to 5 (tests pass 0)
- Use `ctx.cfg.enqueue_poll_delay` so it's configurable
- Accept a `sleep_fn` callable (most testable but most disruptive)

## 6. Full-cycle integration test with real DB

**Problem**: The poll tests all mock the DB. State machine bugs (e.g., "downloading album accidentally becomes invisible to `get_wanted()`") can only be caught by a test that uses the real PostgreSQL DB through the full `set_downloading → poll with mocked slskd → verify status=imported` cycle.

**Fix**: One test class in `test_pipeline_db.py` or `test_integration.py` that:
1. Adds a request
2. Calls `set_downloading()` with real state
3. Verifies `get_wanted()` doesn't return it
4. Verifies `get_downloading()` does return it
5. Calls `update_status("imported")` (which NULLs `active_download_state` inline)
6. Verifies final state

This catches CHECK constraint issues, column migration bugs, and status visibility invariants that mocks hide.

## 7. ~~`failed_grab` parameter is vestigial~~ — resolved in #573

`process_completed_album` no longer accepts the unused argument; all production
and test callers use the typed completion boundary directly.
