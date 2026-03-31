# Parallel Soulseek Searches

Soularr can fire multiple Soulseek searches concurrently instead of one at a time, cutting cycle time significantly.

## Problem

Searching is the slowest part of the pipeline. Each search fires an slskd API call, sleeps 5 seconds (slskd needs time to register the search), then polls for results. With 10 albums, this takes ~20 minutes sequentially because each search blocks until results arrive before the next one starts.

## Solution

`ThreadPoolExecutor` fires all searches at once. Results are processed (matched, enqueued) as they arrive via `as_completed()`. The download monitoring phase is unchanged -- it already handles all active downloads in a single polling loop.

### Architecture

```
Sequential (before):
  search_1 ──────> search_2 ──────> ... search_N ──────> process results
  |--- 5s+poll ---|--- 5s+poll ---|       ...        |--- 5s+poll ---|

Parallel (after):
  search_1 ──────>
  search_2 ──────>  } all fire at once, process as they complete
  ...
  search_N ──────>
  |--- max(5s+poll) ---|--- process each as it arrives ---|
```

### Thread safety

The key insight is separating I/O from state mutation:

- **`_execute_search(album, cfg, slskd_client)`** -- Thread-safe. Takes explicit args (no module globals). Returns a `SearchResult` dataclass containing cache entries and upload speeds.
- **`_merge_search_result(result)`** -- Main-thread only. Merges one `SearchResult` into the module-level `search_cache` and `user_upload_speed` dicts.
- **`find_download()` / `try_enqueue()`** -- Main-thread only. Called sequentially after each merge. No changes needed.

Zero shared mutable state during the parallel phase. No locks needed.

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

Same shape as `search_cache[album_id]`, but returned as a value instead of written to a global.

## Configuration

```ini
[Search Settings]
parallel_searches = 8
number_of_albums_to_grab = 16
```

- **`parallel_searches`** -- Max concurrent search threads. Default: 8. Set to 1 for sequential behavior.
- **`number_of_albums_to_grab`** -- How many albums to search per cycle. With parallel searches, you can increase this since wall-clock time is dominated by the slowest single search, not the sum.

### Recommended pairing

Since parallel searches overlap the 5s+poll wait, you can grab more albums per cycle without increasing cycle time:

| parallel_searches | number_of_albums_to_grab | Approx cycle search time |
|-------------------|--------------------------|--------------------------|
| 1 (sequential)    | 10                       | ~20 min                  |
| 8                 | 16                       | ~2-5 min                 |
| 8                 | 20                       | ~2-5 min                 |

## Benchmark

Run the benchmark script to find the optimal concurrency for your slskd instance:

```bash
# Against your slskd instance:
nix-shell --run "python3 scripts/bench_parallel_search.py --host http://<slskd>:5030 --api-key <key>"

# Auto-start ephemeral Docker container (needs tests/.slskd-creds.json):
nix-shell --run "python3 scripts/bench_parallel_search.py"

# Custom concurrency levels:
nix-shell --run "python3 scripts/bench_parallel_search.py --levels 1,2,4,8,16"
```

### Benchmark results (2026-03-31)

8 queries, ephemeral slskd container on Framework laptop:

```
Level    Wall  Speedup  Results
    1   70.9s    1.0x      718
    2   48.4s    1.5x      723
    4   38.3s    1.9x      723
    8   37.3s    1.9x      724   <-- default
   16   35.3s    2.0x      724
   20   45.4s    1.6x      626   <-- throttling (lost 92 results)
```

Key findings:
- **8-16 is the sweet spot**: ~2x speedup, no result degradation
- **At 20**: Soulseek started throttling -- one search returned 0 results
- **The 5s sleep is the bottleneck**: most searches complete in exactly 5s. Parallelism overlaps these sleeps.
- **In production** with 16-20 albums (many with longer search times), speedup will be larger

## Files

| File | Role |
|------|------|
| `lib/search.py` | `SearchResult` dataclass |
| `soularr.py` | `_execute_search()`, `_merge_search_result()`, `_search_and_queue_parallel()` |
| `lib/config.py` | `parallel_searches` config field |
| `scripts/bench_parallel_search.py` | Concurrency sweep benchmark |
| `tests/test_slskd_live.py` | `TestParallelSearchTiming` live tests |

## Live testing

The live tests in `test_slskd_live.py` (gated behind `SLSKD_TEST_FULL=1`) verify:

1. **Speedup**: Parallel is at least 1.5x faster than sequential
2. **Result quality**: Parallel returns comparable result counts (within 50% of sequential per query)

```bash
nix-shell --run "SLSKD_TEST_FULL=1 python3 -m unittest tests.test_slskd_live.TestParallelSearchTiming -v"
```
