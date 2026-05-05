---
title: "fix: Quiet routine browse HTTP failures"
type: fix
status: completed
date: 2026-05-05
---

# fix: Quiet routine browse HTTP failures

## Summary

Treat routine slskd directory-browse HTTP failures as expected peer-state skips instead of application errors with full tracebacks. The implementation keeps browse, cache, match, and enqueue behavior unchanged while preserving traceback logging for non-routine failures.

## Requirements

- R1. Directory browse HTTP failures caused by ordinary peer/slskd state, specifically 404 missing peer directories and 5xx transient slskd server failures, must not emit traceback logs.
- R2. Routine browse HTTP failures still return an empty browse result so callers skip that directory exactly as they do today.
- R3. Unexpected failures, including non-HTTP exceptions and non-routine HTTP statuses, continue to use traceback logging so genuine bugs and configuration problems remain visible.
- R4. The fix must preserve current peer-cache semantics: routine HTTP failures are not promoted to persistent negative cache entries.

## Scope Boundaries

- Do not change browse fan-out concurrency, worker sizing, or search pipeline flow.
- Do not change slskd HTTP connection pool configuration; that work already exists separately in `lib/slskd_client.py`.
- Do not add per-wave log deduplication in this fix.
- Do not broaden persistent negative-cache writes beyond confirmed empty browse responses.

### Deferred to Follow-Up Work

- Per-wave dedupe for repeated routine failures from the same peer can be filed separately if info-level skip logs are still too noisy after traceback suppression.

## Context & Research

### Relevant Code and Patterns

- `lib/browse.py`: `_browse_one` is the narrow behavior boundary. It calls `slskd_client.users.directory`, returns `BrowseOneResult(file_dir)` for failures, and currently logs every exception with `logger.exception`.
- `lib/browse.py`: `BrowseOneResult.cache_negative` is only set when slskd returns an empty directory list; callers use that flag to write persistent negative peer-cache entries.
- `tests/test_browse.py`: existing browse fan-out tests already pin successful cache writes, empty-result negative-cache writes, exception tolerance, and coordinator behavior. New coverage should live here.
- `tests/fakes.py`: `FakeSlskdUsers.set_directory_error` can inject exceptions for a specific `(user, dir)` pair, which is enough to test HTTP and non-HTTP browse failures without network calls.
- `lib/slskd_client.py`: slskd-api response hooks raise `requests.exceptions.HTTPError`, so the browse code should classify that exception type directly rather than parsing log text.

### Institutional Learnings

- `docs/plans/2026-05-01-001-feat-browse-fanout-and-pipeline-depth-plan.md`: browse fan-out intentionally increased parallel directory requests; log handling should absorb expected peer churn without reintroducing throughput caps.
- `docs/plans/2026-05-05-001-feat-peer-cache-redis-migration-plan.md`: only confirmed empty browse responses should populate persistent negative cache. Exceptions and transport failures remain non-cacheable.

### External References

- No external research used. This is local exception classification around an existing requests-based slskd client.

## Key Technical Decisions

- Classify only known routine browse HTTP statuses as non-exceptional: 404 not found and 5xx server-side failures. Other HTTP statuses remain unexpected and keep traceback logging.
- Preserve the current browse result contract: all failure paths still return an empty `BrowseOneResult`, and only an empty successful slskd response sets `cache_negative=True`.
- Use info-level logging for routine skips, with enough peer/directory/status context for operators to understand what was skipped without flooding journalctl with tracebacks.

## Open Questions

### Resolved During Planning

- Should the fix dedupe routine failures by peer within a wave? No. First remove traceback noise; dedupe is a follow-up only if info-level logs remain noisy.
- Should routine HTTP failures write persistent negative cache entries? No. Keep negative-cache writes limited to confirmed empty browse responses.

### Deferred to Implementation

- Exact log message wording is left to implementation, as long as it includes peer, directory, and status context and does not include a traceback for routine statuses.

## Implementation Units

- U1. **Classify routine browse HTTP failures**

**Goal:** Downgrade ordinary slskd browse HTTP failures from traceback logging to expected skip logging while preserving current browse result and cache behavior.

**Requirements:** R1, R2, R3, R4

**Dependencies:** None

**Files:**
- Modify: `lib/browse.py`
- Test: `tests/test_browse.py`

**Approach:**
- Add direct classification for `requests.exceptions.HTTPError` around the `users.directory` call in `_browse_one`.
- Treat 404 not found and 5xx slskd server-side statuses as routine skips logged at info level.
- Keep all other exceptions, including non-routine HTTP statuses and invalid response shape, on the existing traceback path.
- Return the same empty browse result shape for routine HTTP failures that callers already receive for exceptions.

**Execution note:** Implement test-first. Start with failing tests for routine HTTP status handling and the unexpected-exception fallback before changing `lib/browse.py`.

**Patterns to follow:**
- Existing exception-tolerance tests in `tests/test_browse.py`.
- Existing `cache_negative` contract in `lib/browse.py`: empty successful responses are cacheable negatives; exception-shaped failures are not.

**Test scenarios:**
- Happy path: given `users.directory` raises HTTP 404 for a peer/directory, `_browse_one` returns an empty result, emits an info log, does not call exception logging, and does not set `cache_negative`.
- Happy path: given `users.directory` raises HTTP 500 or another 5xx status for a peer/directory, `_browse_one` returns an empty result, emits an info log, does not call exception logging, and does not set `cache_negative`.
- Error path: given `users.directory` raises a non-routine HTTP status such as 401 or 403, `_browse_one` returns an empty result through the existing exception path and calls exception logging.
- Error path: given `users.directory` raises `HTTPError` without a response status, `_browse_one` treats it as unexpected and calls exception logging.
- Error path: given `users.directory` raises a non-HTTP exception, `_browse_one` still calls exception logging and returns an empty result.
- Regression: given `users.directory` returns an empty list, `_browse_one` still sets `cache_negative=True` so the existing persistent negative-cache behavior is unchanged.

**Verification:**
- Tests prove routine browse HTTP failures no longer invoke traceback logging.
- Tests prove unexpected failures still invoke traceback logging.
- Existing browse fan-out tests still pass, especially cache-positive, cache-negative, exception tolerance, and global-cap coverage.

## System-Wide Impact

- **Interaction graph:** Only the single-directory browse helper changes; callers continue to receive the same success/empty result shape.
- **Error propagation:** Routine peer/slskd browse HTTP failures become info-level skip events. Unexpected failures remain exception-level logs.
- **State lifecycle risks:** The main risk is accidentally treating HTTP failures as persistent negatives. The plan requires tests that routine HTTP failures leave `cache_negative=False`.
- **Unchanged invariants:** Browse fan-out capacity, single-flight behavior, hot cache writes, Redis negative-cache semantics, match scoring, and enqueue behavior remain unchanged.

## Risks & Dependencies

| Risk | Mitigation |
|------|------------|
| Over-broad HTTPError handling hides configuration or auth bugs. | Only downgrade the statuses known to represent routine browse peer/slskd state; leave other statuses on traceback logging. |
| Log noise moves from traceback spam to info spam. | Keep the log concise and defer per-wave dedupe until there is evidence it is still needed. |
| Routine failures accidentally poison persistent negative cache. | Preserve `cache_negative=False` for exception-shaped failures and cover it in tests. |

## Documentation / Operational Notes

- No user-facing docs need to change.
- Deployment verification should inspect a normal cratedigger run on doc2 and confirm routine browse 404/5xx failures appear as concise skip logs rather than traceback blocks.

## Sources & References

- Related issue: #216
- Related code: `lib/browse.py`
- Related tests: `tests/test_browse.py`
- Related prior plans: `docs/plans/2026-05-01-001-feat-browse-fanout-and-pipeline-depth-plan.md`, `docs/plans/2026-05-05-001-feat-peer-cache-redis-migration-plan.md`
