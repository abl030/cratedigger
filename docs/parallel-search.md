# Parallel Soulseek Searches

Soularr can overlap Soulseek searches to cut cycle time significantly.

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

- **`_submit_search(album, cfg, slskd)`** -- Sequential. Submits one search to slskd, returns `(search_id, query, album_id)`. Retries on 429 with exponential backoff.
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
- **`number_of_albums_to_grab`** -- How many albums per cycle. With parallel collection, wall time is dominated by the slowest search, so you can increase this.

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
| `soularr.py` | `_submit_search()`, `_collect_search_results()`, `_search_and_queue_parallel()` |
| `lib/config.py` | `parallel_searches` config field |
| `scripts/bench_parallel_search.py` | Concurrency sweep benchmark |
| `tests/test_slskd_live.py` | `TestParallelSearchTiming` live tests |

## Live testing

The live tests in `test_slskd_live.py` (gated behind `SLSKD_TEST_FULL=1`) verify timing and result quality:

```bash
nix-shell --run "SLSKD_TEST_FULL=1 python3 -m unittest tests.test_slskd_live.TestParallelSearchTiming -v"
```
