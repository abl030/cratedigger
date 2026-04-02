Read `TODO-type-safety.md` for full context. On 2026-04-02 we cleaned up all dict/dataclass dual-interface patterns and typed most `Any` params. The remaining work:

1. **`AlbumRecord.from_db_row()`** (album_source.py) returns a raw dict — replace with a typed `AlbumRecord` dataclass. This is the biggest item: ~50 access sites in soularr.py. Define the dataclass, update `from_db_row()`, fix all consumers, delete `_get_request_id()`.

2. **`PipelineDB.get_request()`** (lib/pipeline_db.py) returns `dict[str, Any]` — replace with a typed `PipelineRequest` dataclass.

3. **Test audit**: grep for any test passing a dict where a dataclass is expected. Key patterns: `{"valid": True, ...}` passed to functions typed `ValidationResult`, `{"artist": ...}` passed to functions typed `GrabListEntry`. Fix these to use the real constructor.

4. **Type `verify_filetype()`** param as `dict[str, object]` instead of `Any`.

5. **Clean stale comments**: "bridge during migration" in soularr.py:964, "Lidarr bridge" in pipeline_db.py:69.

Start with item 1 (AlbumRecord) since it's the root cause of the remaining `Any` types. Use the scope rule: this IS the fix, do it as one logical change. Run full test suite before committing.
