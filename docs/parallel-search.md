# Parallel Soulseek Searches

Cratedigger can overlap Soulseek searches to cut cycle time significantly.

## Plan-driven execution (post-2026-05 cutover)

Search execution is plan-driven. Phase 2 selects wanted rows via
`PipelineDB.get_wanted_searchable(SEARCH_PLAN_GENERATOR_ID, ...)`, which
only returns wanted rows whose **active plan's `generator_id` matches
the current generator id**. Rows without an active current-generator
plan (no plan, old-generator carryover, deterministic-failed,
transient-failed) are skipped this cycle and surfaced through the
startup reconciliation summary log instead — see
`docs/persisted-search-plans-rollout.md`.

For each selected row, the executor reads the active plan's
`next_plan_ordinal` item and runs that query. The legacy "compute
variant from `search_attempts`" path is gone:

- `search_attempts` is **scheduler/backoff-only** now. It records retry
  history, but never selects which query to issue and never resets on
  plan wrap.
- Variant selection from `search_attempts` is removed. Strategy labels
  on `search_log.variant` are sourced from the plan item that actually
  ran.
- Plan wrap replaces `outcome='exhausted'`. The consumed-attempt DB
  method sets `cursor_update_status='wrapped'` and increments
  `plan_cycle_count` on the request when the final ordinal completes.
  See `docs/pipeline-db-schema.md` for the full plan/cursor schema.

## New-request capacity

Phase 2 applies one eligibility boundary before it allocates the page: the
request must be `wanted`, its retry deadline must be due, its active plan must
match the current generator, it must have no conflicting YouTube download or
import job, and its title must pass the configured title blacklist. Priority
never bypasses the existing 30/60/120/240-minute retry backoff.

A request is in the new cohort while its immutable `created_at + 24 hours` is
later than the Phase 2 snapshot time. With the production page size of 16,
Phase 2 draws up to four randomized new requests and at least 12 randomized
established requests. Other positive page sizes reserve a floor-rounded
quarter share for new work (at least one slot). Either cohort borrows slots the
other cannot fill, so an eligible row is never left idle while the page has
space. More than four due new requests still use only four production slots
while established work is available;
once a request reaches the exact 24-hour boundary it joins the established
lottery even if it has never been attempted.

For a low-volume addition, "first eligible cycle" means the next Phase 2
snapshot after its backoff expires. It does not trigger a cycle immediately or
skip the retry deadline. Manual requeue preserves the original `created_at`;
a Replace successor is a newly inserted request and receives its own 24-hour
window.

### Pre-attempt vs accepted-search consumption boundary

A plan ordinal **consumes a slot** when slskd accepts the search (or
returns a terminal state). The consumed-attempt DB method then logs
the search-log row and atomically advances or wraps the cursor.

A submission/setup failure **before** slskd accepts the search is
**non-consuming**: the executor writes a `search_log` row with
`execution_stage='pre_attempt'`, `attempt_consumed=false`,
`cursor_update_status='unchanged'`, applies retry/backoff so the
request cannot spin hot, and leaves the cursor untouched. The slot is
re-tried next cycle.

### Stale-completion guards

A successful regeneration mid-flight rewrites the active plan and
resets the cursor. The original search may complete after that — its
plan id / ordinal / cycle snapshot no longer match the request's
active state. The executor's atomic log+cursor DB method detects this
and writes a stale-completion row (`execution_stage='stale_completion'`,
`cursor_update_status='stale'`, `stale_reason=<tag>`) without mutating
the active cursor.

The same guard is enforced beyond cursor updates so a stale
found/enqueued completion cannot claim download ownership or move the
request status:

- `lib/enqueue.py` — claim guard during enqueue.
- `lib/download_ownership.py` — ownership guard before transferring a
  request to `downloading`.
- `lib/transitions.py` — guard on status transitions originating from
  search execution.

The result: stale completions are audit-only. Active request state
(scheduler/backoff, status, downloads) is only mutated by the
plan/ordinal that the request currently points at.

## Problem

Searching is the slowest part of the pipeline. Each search fires an slskd API call, sleeps 5 seconds (slskd needs time to register the search), then polls for results. With 10 albums, this takes ~20 minutes sequentially because each search blocks until results arrive before the next one starts.

## slskd Constraints (from source code analysis)

We investigated the [slskd source](https://github.com/slskd/slskd) and found two hard limits:

1. **API-level**: `SemaphoreSlim(1, 1)` on `POST /api/v0/searches` -- only one search can be **submitted** at a time. All concurrent POSTs get 429. The semaphore releases after the search is queued (~100ms), not after results arrive.

2. **Soulseek.NET level**: `maximumConcurrentSearches: 2` -- only 2 searches can be active on the Soulseek network simultaneously. Additional searches queue internally.

This means we **cannot** fire N searches concurrently. Instead, we submit them one at a time through the API, but overlap the result-waiting phase.

## Solution: Submit Sequential, Collect Parallel

```
Phase 1 — Submit (sequential, ~100ms each):
  POST search_1 → get ID₁
  POST search_2 → get ID₂
  ...
  POST search_N → get IDₙ
  |---- N * ~100ms ----|

Phase 2 — Collect (parallel, all polling at once):
  poll ID₁ + fetch results ──────>
  poll ID₂ + fetch results ──────>  } ThreadPoolExecutor
  ...
  poll IDₙ + fetch results ──────>
  |---- max(search_time) + 5s ----|

Phase 3 — Process (sequential, main thread):
  merge results → find_download → enqueue
```

**Wall time = N * 100ms (submit) + max(search_time) (collect) + process time**

vs sequential: **sum(all search times)**

With 16 albums where each search takes 30-60s, this cuts search time from ~8-16 minutes to ~60s + overhead.

### How it works

- **`_submit_plan_search(...)`** -- Sequential. Submits one persisted-plan search to slskd and retries transient submission contention with backoff.
- **`_collect_search_results(search_id, ...)`** -- Parallel. Sleeps 5s, polls search state, fetches responses, builds `SearchResult` dataclass.
- **`_search_and_queue_parallel(albums)`** -- Orchestrator. Submits all, then collects all via `ThreadPoolExecutor`, processes results as they arrive.
- **`_merge_search_result(result)`** -- Main-thread only. Merges into `search_cache` and `user_upload_speed`.

### Thread safety

Zero shared mutable state during the parallel phase:
- Submit phase is sequential (one thread)
- Collect phase threads only read `cfg` (frozen) and make stateless HTTP calls
- Each returns a `SearchResult` dataclass
- Merge and `find_download()` happen sequentially on the main thread

### SearchResult dataclass

Defined in `lib/search.py`:

```python
@dataclass
class SearchResult:
    album_id: int
    success: bool
    cache_entries: dict[str, dict[str, list[str]]]  # username -> filetype -> [dirs]
    upload_speeds: dict[str, int]                    # username -> speed
    query: str = ""
    result_count: int = 0
    elapsed_s: float = 0.0
```

## Configuration

```ini
[Search Settings]
parallel_searches = 8
number_of_albums_to_grab = 16
```

- **`parallel_searches`** -- Controls whether parallel mode is used. Set > 1 to enable, 1 for sequential. Default: 8. The actual concurrency during the collect phase is `len(albums)` (all poll simultaneously).
- **`number_of_albums_to_grab`** -- How many albums per cycle (minimum 2, so both scheduler cohorts retain capacity). With parallel collection, wall time is dominated by the slowest search, so you can increase this. The production value of 16 gives the new/established scheduler its 4/12 reserved allocation.

### Recommended pairing

| parallel_searches | number_of_albums_to_grab | Approx cycle search time |
|-------------------|--------------------------|--------------------------|
| 1 (sequential)    | 10                       | ~20 min                  |
| 8                 | 16                       | ~1-2 min                 |

## Benchmark

Run the benchmark script to measure timing on your slskd:

```bash
# Against your slskd instance:
nix-shell --run "python3 scripts/bench_parallel_search.py --host http://<slskd>:5030 --api-key <key>"

# Auto-start ephemeral Docker container (needs tests/.slskd-creds.json):
nix-shell --run "python3 scripts/bench_parallel_search.py"
```

Note: the benchmark currently fires concurrent `search_text()` calls, which works against the ephemeral container (no rate limiter state) but will 429 against a production slskd. The benchmark should be updated to use the submit-then-collect pattern.

## Files

| File | Role |
|------|------|
| `lib/search.py` | `SearchResult` dataclass |
| `cratedigger.py` | `_submit_plan_search()`, `_collect_search_results()`, `_search_and_queue_parallel()` |
| `lib/config.py` | `parallel_searches` config field |
| `scripts/bench_parallel_search.py` | Concurrency sweep benchmark |
| `tests/test_search_max_inflight.py` | Deterministic pipeline-depth and owner-thread merge tests |

## Focused testing

The deterministic focused tests exercise the parallel pipeline without an
environment gate:

```bash
nix-shell --run "python3 -m unittest tests.test_search_max_inflight -v"
```
