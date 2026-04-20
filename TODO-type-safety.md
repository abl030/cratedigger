# Type Safety Cleanup — Remaining Work

## Context

On 2026-04-02 we removed dict compatibility methods from `ValidationResult`, `GrabListEntry`, and `DownloadFile`, typed 17+ `Any` params, and added `AudioValidationResult`. This exposed a live crash in `log_validation_result()` that our tests didn't catch because **tests constructed their own inputs in a different format than production**.

## Insight: contract tests, not shape tests

The crash wasn't caught because `log_validation_result` tests passed dicts while production passes `ValidationResult`. Both were internally consistent but incompatible at the boundary. Tests that construct fake inputs in the wrong type don't test the contract — they test a fiction.

**Rule**: when testing a function that receives typed data from another function, pass the REAL type (or build it with the real constructor). Don't hand-build a dict that "looks like" the type. If the function signature says `ValidationResult`, the test must pass `ValidationResult`.

This applies to every remaining untyped boundary below.

## Remaining untyped boundaries

### 1. ~~`AlbumRecord.from_db_row()` returns `dict`~~ ✅ DONE

Replaced with typed `AlbumRecord`, `ReleaseRecord`, `MediaRecord` dataclasses. All ~50 access sites in cratedigger.py and lib/download.py updated. `_get_request_id()` deleted. Tests fixed to use real constructors.

### 2. `PipelineDB.get_request()` returns `dict[str, Any]` — INTENTIONALLY LEFT AS DICT

Attempted typed `PipelineRequest` dataclass but reverted: 30-field boilerplate mirroring the DB schema, maintenance tax on every migration, and `RealDictCursor` already handles it. Not worth the complexity. See audit discussion in git history.

### 3. ~~`verify_filetype()` takes `file: Any`~~ ✅ DONE

Typed as `dict[str, Any]` (raw slskd API dicts with mixed value types — `Any` is the honest annotation).

### 4. ~~Test quality: stop passing dicts where dataclasses are expected~~ ✅ DONE

Audited all tests. Fixed `test_download.py` (2 tests passing dicts → `AlbumRecord`), `test_import_dispatch.py` (4 tests passing dicts → `MagicMock` with typed attrs), `test_web_server.py` (mock → `PipelineRequest`). Remaining `{"valid": ...}` patterns are JSON strings stored in JSONB, not typed function args.

### 5. ~~Stale comments~~ ✅ DONE

- Fixed "bridge during migration" → removed from cratedigger.py
- Fixed "Lidarr bridge" → clarified as legacy columns in pipeline_db.py
