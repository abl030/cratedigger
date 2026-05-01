---
date: 2026-05-01
topic: browse-fanout-and-pipeline-depth
issue: 198
---

# Browse Fan-Out and Pipeline Depth — Cycle Outlier Reduction

## Problem Frame

Cratedigger cycles routinely blow out to 27–70 minutes (worst observed: 70 min, 06:42 → 07:52 on 2026-05-01) against a 5-minute timer. The 5-min timer slips badly, downloads start later than they should, and the next cycle stacks on top of the previous one. Investigation in issue #198 traced ~16+ min of one cycle to the **browse phase** of search-and-enqueue, which iterates Soulseek peers serially even though `slskd.users.directory()` calls to **different** peers parallelize freely at the slskd and network layers.

The recently shipped un-wildcarded ladder (`feat(search): rebuild variant ladder`) makes this worse: more peers per search → more per-cycle browses → larger outliers.

Two of issue #198's three proposed fixes apply (#1 and #3); the third (persist `folder_cache`) is **already implemented and live** (`lib/cache.py`, `cratedigger.py:881-882, 961-962`, 538 MB cache file healthy on doc2). Issue #198 has been amended with two correction comments.

## Requirements

**Browse fan-out (fix #1)**

- R1. `try_enqueue` (and `try_multi_enqueue`) must browse a bounded set of top-K peers in parallel before iterating cheap matching, rather than browsing one peer at a time inside the user-iteration loop.
- R2. `K` is configurable via `cfg.browse_top_k`, default `20`. Exposed in the NixOS module the same way `searchResponseLimit` / `searchFileLimit` are exposed.
- R3. Peers are ranked for fan-out by `ctx.user_upload_speed` (the same key already used to sort the enqueue iteration), descending.
- R4. After the top-K wave returns, matching iterates peers in upload-speed order against the now-cached `folder_cache` entries, exiting on first match.
- R5. If no match is found in the top-K, browse the remaining peers in **chunks of K** (lazy tail), running matching after each chunk. Stop fanning out as soon as a match succeeds.
- R6. Each fan-out wave is bounded by a per-wave deadline (`cfg.browse_wave_deadline_s`, default `20`). Peers that haven't responded by the deadline are recorded in `ctx.broken_user` for the rest of the cycle and skipped in the lazy-tail iteration.
- R7. Global executor parallelism is capped at `cfg.browse_global_max_workers` (default `32`). When K is below the cap, fan-out is K-wide; when chunks are smaller, the executor is still bounded.
- R8. Cached entries already in `ctx.folder_cache[username]` are not re-browsed. The fan-out only includes peers whose target dir is not already cached.
- R9. The new fan-out path must reuse the existing `_browse_directories` per-user parallelism (`cfg.browse_parallelism = 4`), so a single peer with multiple candidate dirs still browses those dirs in parallel inside the wave.

**Pipeline depth (fix #3)**

- R10. `MAX_INFLIGHT` (`cratedigger.py:629`) is replaced with `cfg.search_max_inflight`, default `4`. Exposed in the NixOS module.
- R11. The pipeline submitter continues to submit POSTs sequentially through the existing 429-retry loop (the `SearchRequestLimiter` constraint hasn't changed).
- R12. The match_pool refactor (option (b) in issue #198) is **out of scope**. Defer until measurement shows the search-collection thread is still the bottleneck after R1–R10 land.

**Instrumentation (precondition for measuring success)**

- R13. Each cycle emits a summary log line with: total browse time, total search time (submit + collect), total match time, count of fan-out waves, count of peers browsed, count of peers timed out by the wave deadline.
- R14. Each fan-out wave emits a log line with: K, n_uncached, n_returned, n_timed_out, elapsed seconds.
- R15. The 538 MB JSON cache load time is logged at startup (already present, ensure it's part of the cycle summary too).

**Per-cycle browse budget (added 2026-05-01 during plan deepening)**

- R16. `cfg.browse_cycle_budget_s` (default 240 s) caps cumulative browse wall-time across all albums in a single cycle. When exceeded, remaining wanted records short-circuit (no further waves) and remain `wanted` for retry next cycle. Defends against multi-album-no-match cycles compounding into 50-min runs that would defeat fix #1's value.

## Success Criteria

- **Worst-case cycle drops below 15 min.** The 70-min outliers from un-wildcarded ladder runs disappear. (Pre-fix baseline from journalctl: 27, 40, 56, 70 min.)
- **Median cycle stays at or below current ~5–10 min.** Fan-out must not regress steady-state cycles where the cache is hot and most peers match early.
- **The 5-min timer stops slipping.** Successive cycles complete within the 5-min window or close to it, no more cycle-on-cycle stacking observed in journalctl.
- **No degradation in match success rate.** Compare match-vs-no-match counts and `failed` totals over a 24h window before and after deploy. Match rate must be within ±2%.
- **slskd survives.** No 429s on browse (there shouldn't be any — slskd doesn't gate browse), no observable degradation in slskd's own request latency or memory under fan-out load.

## Scope Boundaries

**In scope:**
- Bounded parallel browse fan-out in `try_enqueue` and `try_multi_enqueue` (R1–R9)
- `cfg.browse_top_k`, `cfg.browse_wave_deadline_s`, `cfg.browse_global_max_workers` config + NixOS module options
- Raise `MAX_INFLIGHT` 2 → 4 via `cfg.search_max_inflight` (R10–R11)
- Per-cycle and per-wave instrumentation (R13–R15)
- Tests covering fan-out shape, deadline behavior, lazy-tail iteration, global cap

**Out of scope:**
- Persisting `folder_cache` (already done — `lib/cache.py`)
- Migrating `folder_cache` to a pipeline DB table (defer until JSON load time becomes a measurable fraction of cycle time, or file size pushes 1 GB+)
- match_pool / decoupled match executor (R12)
- Tuning `cfg.browse_parallelism` (already 4, leave alone)
- Changing `search_response_limit=1000` or `search_file_limit=50000`
- LRU/size cap on the JSON cache (separate concern, file it as its own issue if needed)
- Re-architecting search submission (the existing submit-sequential / collect-parallel pattern is correct — see `docs/parallel-search.md`)

## Key Decisions

- **K = 20 by default.** With slskd not throttling browse, K = 10 was over-conservative. K = 20 is generous enough to cover most albums in the first wave (top-20 by upload speed is usually where successful matches live) without thundering-herd risk. Tunable via config.
- **Per-wave deadline = 20 s.** Soulseek.NET's TCP timeout for dead peers is ~30–60 s. A 20 s wave deadline lets the live peers complete and treats slow/dead peers as broken-for-this-cycle, vs waiting out the full TCP timeout for every dead peer.
- **Global executor cap = 32.** Higher than K so lazy-tail chunks can proceed while a top-K wave is still draining stragglers. Headroom for future K tuning without revisiting the cap.
- **`MAX_INFLIGHT` = 4.** Soulseek.NET queues searches above 2 active internally. Pipeline depth of 4 keeps the search-collection thread fed without risking 429 storms (we still submit sequentially).
- **No DB-backed cache.** The current JSON cache works (verified — 72k folder entries, 10 evictions/cycle, steady-state). The bottleneck is cold-cache misses on new peers, which is what fix #1 addresses. A DB table is speculative complexity.
- **Drop the match_pool refactor from this round.** It's the right shape *if* search submission is still gated after fix #1 lands. Measure first.

## Dependencies / Assumptions

- **slskd does not throttle browse.** Verified by reading the slskd C# source (UsersController.cs, no `SemaphoreSlim` on the browse path). Documented in `docs/slskd-internals.md`. If a future slskd version adds throttling, the global cap and K need re-tuning.
- **Soulseek.NET TCP timeout is ~30–60 s.** Not configurable from slskd. The wave deadline (R6) is our defense.
- **`ctx.user_upload_speed` is populated for every peer in `results.keys()`.** This is true today (populated during search-result merge). If a peer has no recorded speed, it sorts to the bottom — acceptable.
- **The `folder_cache` save/load tax (538 MB JSON) is amortized.** If the file grows to 1 GB+, a separate effort to LRU-cap or DB-migrate it becomes worthwhile, but is not gating this work.
- **Soulseek peer-to-peer browse latency is dominated by the peer's own response time, not network RTT to slskd.** Fan-out wins assume real per-peer latency in the 3–5 s range with a tail to 60 s, which matches observed timing.

## Outstanding Questions

- **Should `broken_user` entries from fan-out timeouts persist across cycles?** Today `broken_user` is per-cycle. Persisting timeout-derived entries for, say, 1 hour would prevent the next cycle from re-browsing the same dead peers in their lazy tail. Trade-off: a peer that comes back online stays denylisted for an hour. Recommend: leave per-cycle for v1, revisit if logs show repeat dead-peer browses.
- **Should K auto-tune based on observed first-match-rank distribution?** If 95% of matches are in top-5, K=20 is wasting browses on most albums. Static K=20 is fine for v1; auto-tune is a v2 concern once we have telemetry from R13/R14.

## Next Steps

1. Land instrumentation (R13–R15) **first**, in its own commit. Deploy and observe one full day of cycles to baseline browse vs search vs match vs JSON-load-cost percentages. This gives us numerical "before" data and validates the issue #198 measurement plan.
2. Implement R1–R9 (bounded parallel fan-out) on a feature branch, with new tests in `tests/test_enqueue.py` and `tests/test_matching.py` covering: top-K wave shape, deadline timeout behavior, lazy-tail iteration, global cap enforcement, single-peer multi-dir reuse of `browse_parallelism=4`.
3. Implement R10–R11 (`search_max_inflight`) in the same or adjacent commit.
4. Run the full test suite + pyright. Deploy. Observe 24h of journalctl cycles.
5. If outliers don't drop below 15 min: investigate whether the search-collection thread is gating despite raised `MAX_INFLIGHT`. Only at that point consider the match_pool refactor (R12 deferred).
6. If JSON cache load time emerges as the new dominant cost: separate issue for LRU-cap or DB migration.

## References

- Issue #198 (with correction comments dated 2026-05-01)
- `docs/slskd-internals.md` — slskd concurrency facts (browse unthrottled, search-submit serialized, dead-peer TCP timeout)
- `docs/parallel-search.md` — existing submit-sequential / collect-parallel search pattern
- `lib/cache.py`, `cratedigger.py:881-882, 961-962` — existing cache persistence (fix #2 from #198, already shipped)
- `lib/enqueue.py:215, 301` — sequential user-iteration loops to refactor (full function spans 196-271 and 272-390)
- `lib/browse.py:102-129` — `_browse_directories`, the per-user parallelism we keep (called from `lib/matching.py:306`)
- `cratedigger.py:629` — `MAX_INFLIGHT = 2` to replace with config
