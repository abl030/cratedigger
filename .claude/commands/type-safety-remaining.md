Read `TODO-type-safety.md` for full context. Items 1 and 5 are done. Remaining:

1. **`PipelineDB.get_request()`** (lib/pipeline_db.py) returns `dict[str, Any]` — replace with a typed `PipelineRequest` dataclass.

2. **Test audit**: grep for any test passing a dict where a dataclass is expected. Key patterns: `{"valid": True, ...}` passed to functions typed `ValidationResult`, `{"artist": ...}` passed to functions typed `GrabListEntry`. Fix these to use the real constructor.

3. **Type `verify_filetype()`** param as `dict[str, object]` instead of `Any`.

Use the scope rule: this IS the fix, do it as one logical change. Run full test suite before committing.
