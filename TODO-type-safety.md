# Type Safety Cleanup — Remaining Work

## Context

On 2026-04-02 we removed dict compatibility methods from `ValidationResult`, `GrabListEntry`, and `DownloadFile`, typed 17+ `Any` params, and added `AudioValidationResult`. This exposed a live crash in `log_validation_result()` that our tests didn't catch because **tests constructed their own inputs in a different format than production**.

## Insight: contract tests, not shape tests

The crash wasn't caught because `log_validation_result` tests passed dicts while production passes `ValidationResult`. Both were internally consistent but incompatible at the boundary. Tests that construct fake inputs in the wrong type don't test the contract — they test a fiction.

**Rule**: when testing a function that receives typed data from another function, pass the REAL type (or build it with the real constructor). Don't hand-build a dict that "looks like" the type. If the function signature says `ValidationResult`, the test must pass `ValidationResult`.

This applies to every remaining untyped boundary below.

## Remaining untyped boundaries

### 1. ~~`AlbumRecord.from_db_row()` returns `dict`~~ ✅ DONE

Replaced with typed `AlbumRecord`, `ReleaseRecord`, `MediaRecord` dataclasses. All ~50 access sites in soularr.py and lib/download.py updated. `_get_request_id()` deleted. Tests fixed to use real constructors.

### 2. `PipelineDB.get_request()` returns `dict[str, Any]` (lib/pipeline_db.py)

**Impact**: Callers do `req.get("min_bitrate")`, `req.get("verified_lossless")` etc. with no key validation. Misspelled keys silently return None.

**Fix**: Return a typed `PipelineRequest` dataclass.

### 3. `verify_filetype()` takes `file: Any` (lib/quality.py:611)

**Impact**: Receives slskd file dicts. No type checking on the dict shape.

**Fix**: Type as `dict[str, object]` (can't be a dataclass — these are raw API responses from slskd).

### 4. Test quality: stop passing dicts where dataclasses are expected

Grep for any test that constructs a dict and passes it to a function typed with a dataclass. These are the tests that won't catch boundary mismatches. Key pattern to find:

```
result = {"valid": True, ...}  # should be ValidationResult(valid=True, ...)
album_data = {"artist": ...}   # should be GrabListEntry(artist=...)
```

### 5. ~~Stale comments~~ ✅ DONE

- Fixed "bridge during migration" → removed from soularr.py
- Fixed "Lidarr bridge" → clarified as legacy columns in pipeline_db.py
