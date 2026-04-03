# Code Improvement Ideas

Notes from the async-downloads implementation (2026-04-03). These are structural improvements that would make the codebase easier to work with and catch more bugs.

## 1. TestConfig factory instead of MagicMock

**Problem**: Tests create `SoularrConfig` via `MagicMock()`, which means pyright can't check attribute names. About 10 pyright "errors" across test files are `Cannot assign to attribute "stalled_timeout" for class "SoularrConfig"`. Real type errors are invisible in this noise.

**Fix**: Create a `TestConfig` factory that returns a real `SoularrConfig` with test defaults:

```python
def make_test_config(**overrides) -> SoularrConfig:
    defaults = {
        "slskd_download_dir": "/tmp/test_downloads",
        "stalled_timeout": 300,
        "remote_queue_timeout": 120,
        "beets_validation_enabled": False,
        ...
    }
    defaults.update(overrides)
    return SoularrConfig(**defaults)
```

Tests get `ctx.cfg.stalled_timeout = 600` → `make_test_config(stalled_timeout=600)`. Pyright catches typos at write time. The mock noise vanishes.

## 2. Status exhaustiveness contract test

**Problem**: Adding `downloading` required grep-and-pray across 14 locations in 6 files (JS badge rendering, Python route iteration, CLI display, CSS classes). One missed location means a silent rendering gap.

**Fix**: A single source-of-truth constant and contract test:

```python
# lib/pipeline_db.py
VALID_STATUSES = ("wanted", "downloading", "imported", "manual")

# tests/test_pipeline_db.py
def test_status_exhaustiveness(self):
    """Every status the DB allows must appear in the pipeline route iteration."""
    from web.routes.pipeline import STATUS_ITERATION_ORDER
    self.assertEqual(set(STATUS_ITERATION_ORDER), set(VALID_STATUSES))
```

JS could import a shared `STATUSES` array from a single module. When a new status is added, the contract test fails immediately, pointing at every file that needs updating.

## 3. `process_completed_album` status contract

**Problem**: `poll_active_downloads` has a "safety net" that checks whether `process_completed_album` set the DB status or not. This exists because `process_completed_album` sometimes sets status (via `mark_done`/`mark_failed` in the beets validation path) and sometimes doesn't (when beets validation is disabled). The caller has to guess.

**Fix**: Return a typed result instead of a bool:

```python
@dataclass
class CompletionResult:
    success: bool
    status_set: bool  # Did this function update album_requests.status?
```

Or simpler: have `process_completed_album` always set status, even in the no-beets path. The safety net in `poll_active_downloads` becomes dead code.

## 4. Extract soularr.py globals into a proper entry point

**Problem**: `soularr.py` uses module-level globals (`slskd`, `cfg`, `pipeline_db_source`, `search_cache`, `folder_cache`, `broken_user`) and thin closure wrappers (`_make_ctx()`, `cancel_and_delete()`, `grab_most_wanted()`) to bridge between the global state and `lib/` functions that take `SoularrContext`. Adding `poll_active_downloads` to `main()` required understanding which globals were initialized at which point.

**Fix**: Build `SoularrContext` once at the top of `main()` after all initialization is complete. Thread it through explicitly. Kill the closure wrappers. The search caches could live on `SoularrContext` or a new `SearchState` dataclass.

This would also make integration testing the full `main()` flow possible — currently you can't test `main()` without mocking module globals.

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
5. Calls `clear_download_state()` + `update_status("imported")`
6. Verifies final state

This catches CHECK constraint issues, column migration bugs, and status visibility invariants that mocks hide.

## 7. `failed_grab` parameter is vestigial

**Problem**: `process_completed_album` accepts `failed_grab: list[Any]` but never reads it. Every caller passes `[]`. It's been dead since the function was extracted from `soularr.py`.

**Fix**: Remove the parameter. Update the 3 callers. One-line diff per caller.
