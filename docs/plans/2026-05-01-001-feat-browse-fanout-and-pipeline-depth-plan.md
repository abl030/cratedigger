---
title: Browse Fan-Out and Pipeline Depth — Cycle Outlier Reduction
type: feat
status: active
date: 2026-05-01
deepened: 2026-05-01
origin: docs/brainstorms/browse-fanout-and-pipeline-depth-requirements.md
---

# Browse Fan-Out and Pipeline Depth — Cycle Outlier Reduction

> **Update 2026-05-02:** R6 (`browse_wave_deadline_s`) and R16 (`browse_cycle_budget_s`) were rolled back in production — both client-side caps were short-circuiting before legitimate peers responded and starving the pipeline (search timeout rate jumped from ~1% to ~35%, found rate dropped from 13.7% to 2.2% within 24h of deploy). slskd's per-peer TCP read timeout is the sole authority on hung peers now. The wave-based fan-out, top-K, and global worker cap (R5/R7) remain. The unrelated search poll timeout in `cratedigger.py` was also dropped at the same time for the same reason.

## Overview

Cratedigger cycles routinely overflow the 5-minute timer (worst observed: 70 min on 2026-05-01). The single biggest contributor is the per-album browse phase, which iterates Soulseek peers serially even though `slskd.users.directory()` parallelizes freely at the slskd and network layers. This plan introduces bounded parallel browse fan-out (top-K + lazy tail with per-wave deadlines), raises pipeline depth so search submission isn't gated by post-collection work, and lands instrumentation first so we have numerical "before" data to validate the fix against.

Issue: [#198](https://github.com/abl030/cratedigger/issues/198). Out-of-scope #2 (persist `folder_cache`) is **already shipped** in `lib/cache.py`; out-of-scope match_pool refactor is intentionally deferred (see origin: `docs/brainstorms/browse-fanout-and-pipeline-depth-requirements.md` § Out of scope, R12).

---

## Problem Frame

Today, `try_enqueue` (`lib/enqueue.py:215`) and `try_multi_enqueue` (`lib/enqueue.py:301`) iterate users sequentially in upload-speed order. For each user, `check_for_match` calls `_browse_directories` (`lib/browse.py:102-129`), which parallelizes that single user's directories (default 4 workers). Across users — serial. With `search_response_limit=1000` peers per search and ~3 dirs/user, the worst-case browse phase serializes ~3000 RTTs at 3–60 s each. Pre-filters trim heavily but outliers still hit 16+ minutes of pure browse.

Slskd does **not** throttle browse calls (verified, see `docs/slskd-internals.md`). The serialization is purely client-side, and removing it is the single highest-leverage move.

---

## Requirements Trace

- R1. Bounded top-K parallel browse + lazy tail in `try_enqueue` and `try_multi_enqueue`. *(see origin R1)*
- R2. `cfg.browse_top_k`, default 20, NixOS option exposed. *(R2)*
- R3. Rank peers for fan-out by `ctx.user_upload_speed` desc. *(R3)*
- R4. After top-K wave, iterate matching in upload-speed order against now-cached entries; exit on first match. *(R4)*
- R5. Lazy tail in chunks of K; stop fanning as soon as a match succeeds. *(R5)*
- R6. Per-wave deadline `cfg.browse_wave_deadline_s`, default 20 s; non-responders → `ctx.broken_user`. *(R6)*
- R7. `cfg.browse_global_max_workers`, default 32, caps the fan-out executor. *(R7)*
- R8. Cached `folder_cache[user][dir]` entries must not be re-browsed. *(R8)*
- R9. Single peer with multiple candidate dirs continues to use `_browse_directories` per-user parallelism. *(R9)*
- R10. `cfg.search_max_inflight`, default 4, replaces hard-coded `MAX_INFLIGHT=2`. *(R10)*
- R11. Search submission stays sequential through existing 429-retry loop. *(R11)*
- R12. match_pool refactor explicitly out of scope. *(R12)*
- R13. Cycle summary log line: total browse time, total search time, total match time, fan-out wave count, peers browsed, peers timed out. *(R13)*
- R14. Per-wave log line: K, n_uncached, n_returned, n_timed_out, elapsed s. *(R14)*
- R15. JSON cache load time logged at startup *and* in the cycle summary. *(R15)*
- R16. Per-cycle browse budget `cfg.browse_cycle_budget_s` (default 240 s). When cumulative browse wall-time across all albums in a cycle exceeds the budget, remaining wanted records short-circuit (no further waves). Defends against multi-album-no-match cycles compounding into 50-min runs. *(added during Phase 5.3 plan deepening — also added to origin requirements doc)*

---

## Scope Boundaries

- Persisting `folder_cache` (already shipped in `lib/cache.py`, `cratedigger.py:881-882, 961-962`).
- Migrating `folder_cache` to a DB table or Redis (separate issue [#201](https://github.com/abl030/cratedigger/issues/201)).
- The match_pool / decoupled match executor refactor (R12 deferred until post-deploy measurement justifies it).
- Tuning `cfg.browse_parallelism` (already 4, leave alone).
- Changing `search_response_limit` or `search_file_limit`.
- LRU/size cap on the JSON cache.
- Auto-tuning K based on first-match-rank distribution (deferred to v2).
- Cross-cycle persistence of `broken_user` from fan-out timeouts (deferred to v2).

---

## Context & Research

### Relevant Code and Patterns

- `cratedigger.py:627-757` — `_search_and_queue_parallel`: existing submit-sequential, collect-parallel ThreadPoolExecutor pattern. Uses `as_completed` over a dict-keyed in-flight set, refills pipeline as futures resolve, catches per-future exceptions. Mirror this pattern for fan-out.
- `cratedigger.py:752-755` — current cycle summary log line. Extend in U1.
- `cratedigger.py:629` — hard-coded `MAX_INFLIGHT = 2`. Replaced in U4.
- `lib/browse.py:102-129` — current `_browse_directories(dirs_to_browse, username, slskd_client, max_workers=4)`. Returns `{file_dir: directory_dict}` with failed entries omitted. Per-dir error handling in `_browse_one` (lines 85-99) catches exceptions and returns `(file_dir, None)`. Reuse `_browse_one` in U2 — the new fan-out function flattens (user, dir) tuples and submits each as a single `_browse_one` task.
- `lib/matching.py:296-322` — caller of `_browse_directories`; populates `ctx.folder_cache[user][dir]` and `ctx._folder_cache_ts`. After U3 lands, the fan-out has already populated these; the per-user call inside `check_for_match` becomes a fast cache-lookup path with no work to do.
- `lib/enqueue.py:208-269` — `try_enqueue`'s sorted-users iteration. Refactor target.
- `lib/enqueue.py:299-340` — `try_multi_enqueue`'s per-disk inner loop. Same refactor pattern.
- `lib/config.py:83, 250` — `browse_parallelism` defaults + INI parse. Pattern to mirror for new fields.
- `nix/module.nix:154-160` — INI rendering of search-settings. `nix/module.nix:579-600` — `mkOption` definitions. Pattern to mirror.
- `tests/fakes.py:189-221` — `FakeSlskdUsers.set_directory(username, dir, result)` and `set_directory_error(username, dir, error)`. Per-call recording in `directory_calls` lets us assert parallelism shape after the fact. Sufficient for happy-path testing.
- `tests/helpers.py:298-316` — `make_ctx_with_fake_db(fake_db, cfg, slskd)` wires fakes into `CratediggerContext`.

### Institutional Learnings

- `docs/slskd-internals.md` (this repo, written 2026-05-01) — slskd does not throttle browse calls; per-peer TCP timeout is ~30–60 s and is **not configurable per-call**, which is why a client-side wave deadline is the only effective tail-latency defense.
- `docs/parallel-search.md` — existing parallel-search pattern uses `MAX_INFLIGHT` to cap *active submissions*, with the search-collection thread being where browse work piles up today. U4's pipeline-depth raise targets exactly this.

### External References

- None required for this plan. slskd internals investigated via local clone at `~/code/slskd` and documented in `docs/slskd-internals.md`.

---

## Key Technical Decisions

- **Flatten (user, dir) tuples instead of nesting executors.** The fan-out submits individual `(username, file_dir)` browse calls to a single bounded pool of size `browse_global_max_workers` (default 32). This avoids the alternative of nested pools (outer K × inner `browse_parallelism`), gives natural backpressure, and lets a single global cap apply uniformly to top-K and lazy-tail waves. R9 ("reuse per-user parallelism") is honored by the math: a single user with N candidate dirs contributes N tasks to the same pool, achieving the same `browse_parallelism=4`-equivalent throughput when only one user is in the wave.
- **Manual executor lifetime, not `with ThreadPoolExecutor(...)`.** The naive `with` block calls `shutdown(wait=True)` on exit, which blocks for every running future to complete — defeating the wave deadline (a single 60 s TCP timeout would stretch the wave to 60 s wall-clock, not 20 s). Instead: create the executor explicitly, use `as_completed(futures, timeout=deadline_s)`, on `TimeoutError` mark unfinished futures as timed-out, then call `pool.shutdown(wait=False, cancel_futures=True)`. Cancels queued (not-yet-started) futures; running futures keep running but their results are abandoned. Python 3.13 (in use, verified `nix-shell` runs Python 3.13.12) supports `cancel_futures=True` (added in 3.9). Orphan threads die at process exit (cratedigger is a oneshot), but during the 5-min cycle they may still consume connections — tracked as a known acceptable cost in the Risks table.
- **Pre-create user buckets before submit.** Two futures for the same user (different dirs) race on `ctx.folder_cache.setdefault(user, {})` — CPython does not guarantee atomicity for compound `setdefault + nested-write` from concurrent threads. The fan-out pre-creates `ctx.folder_cache[user] = {}` and `ctx._folder_cache_ts[user] = {}` for every user in the wave **before** submitting any future. Then per-future writes target a stable inner dict and are safe (each `(user, dir)` is owned by exactly one future). This is the cheap, correct alternative to a per-user lock.
- **Lazy tail in chunks of K, deadlined per chunk.** Each chunk is its own wave, gets its own 20 s deadline, and matching runs after each chunk. A 1000-peer no-match album is ~50 chunks × 20 s = ~16 min ceiling — same as today's serial browse but with cheap-pre-filtered peers eliminated by then. The win is on the common case (top-K hits a match), not the pathological no-match.
- **Top-K = 20.** With slskd not throttling browse, K=10 from the brainstorm's draft was over-conservative. K=20 covers most matches in one wave; tunable.
- **Wave deadline = 20 s.** Soulseek.NET's TCP timeout is ~30–60 s for dead peers. A 20 s deadline lets healthy peers finish (typical 3–5 s) and treats laggards as broken-for-this-cycle.
- **Global cap = 32 workers.** Higher than K so lazy-tail waves can proceed while top-K stragglers are still draining (we don't actually do this — waves are sequential — but the cap leaves headroom for K tuning without revisiting infra).
- **Per-cycle browse budget = 240 s.** Caps cumulative browse wall-time across all albums in a single cycle. A no-match album can still consume up to 16.7 min on its own (1000-peer × 20 s waves), so 3 no-match albums in one cycle would chain to ~50 min and defeat the fix. The 240 s budget short-circuits remaining wanted-records once exceeded — they stay `wanted` and get retried next cycle, rather than blowing the timer for everyone. Tunable; emit a `cycle_browse_budget_exhausted` log when it triggers.
- **`broken_user` from fan-out timeouts is per-cycle only.** No cross-cycle persistence. A peer that timed out in wave 1 of an album is excluded from wave 2's matching iteration *for that album*, but next cycle starts with `broken_user` empty. This avoids "transient slowness in one wave permanently denylists a peer" failure modes. Persistent broken_user is explicitly deferred to v2 (origin: outstanding question).
- **`search_max_inflight = 4`.** Soulseek.NET queues searches above 2 active internally; pipeline depth of 4 keeps the search-collection thread fed. Submission stays sequential — `SearchRequestLimiter` rejects simultaneous POSTs with 429.
- **Instrumentation lands first as its own commit.** Gives us numerical "before" data on browse / search / match / json-load percentages before we change anything. Validates the bet and sets the success bar.
- **No new module.** The fan-out function lives in `lib/browse.py` next to `_browse_directories`; the orchestration lives in `lib/enqueue.py`. Adding `lib/fanout.py` would split closely related code.

---

## Open Questions

### Resolved During Planning

- **Where does the new fan-out function live?** `lib/browse.py`, alongside `_browse_directories`. Reuses `_browse_one` for the per-task work.
- **How is R9 (reuse per-user parallelism) honored under flattened tuples?** A user contributing N dirs gets N tasks in the global pool; with `browse_global_max_workers=32` and typical wave size, that user's dirs run effectively in parallel. Equivalent throughput to the old `browse_parallelism=4` for the single-user case.
- **Does the lazy tail fan-out span filetypes?** No. `try_enqueue` is called per `(album, allowed_filetype)` already; the cache from one filetype's pass is shared into the next via `ctx.folder_cache`, so the second filetype's `try_enqueue` finds most peers warm.

### Deferred to Implementation

- **Exact log-line keys/format.** Log line shape is part of the diff; settle when we write U1.
- **Whether `_browse_one` needs minor refactoring** to expose a per-call timeout. Current impl uses the slskd HTTP client's default; we rely on the wave-level `as_completed(timeout=)` instead, but if a single hung call blocks shutdown of the executor we may need `Future.cancel()` semantics. Confirmed at implementation time against `concurrent.futures` behavior.
- **Whether `try_multi_enqueue`'s per-disk loop should fan out across disks too** or just within each disk. Today each disk's iteration is independent. Implementer's call after seeing the refactored single-disc shape.

---

## High-Level Technical Design

> *This illustrates the intended approach and is directional guidance for review, not implementation specification.*

```
try_enqueue(all_tracks, results, allowed_filetype, ctx):
    if ctx.cycle_browse_time_s >= cfg.browse_cycle_budget_s:
        logger.info("cycle_browse_budget_exhausted: skipping album")
        return EnqueueAttempt(matched=False, ...)   # leaves request in 'wanted'

    sorted_users = sort by ctx.user_upload_speed desc
    eligible    = [u for u in sorted_users if not (cooled_down or denylisted)]

    # Wave loop
    waves       = chunked(eligible, K = cfg.browse_top_k)
    for wave in waves:
        # Build (user, dir) work items, skipping anything already cached
        work = [(u, d) for u in wave
                       for d in candidate_dirs(u)
                       if d not in ctx.folder_cache.get(u, {})]
        if work:
            t0 = time.monotonic()
            timed_out = _fanout_browse(work, ctx, cfg)   # populates ctx.folder_cache,
                                                          # returns set of (user) timed out
            ctx.cycle_browse_time_s += time.monotonic() - t0
            ctx.broken_user.update(timed_out)            # per-cycle only; never persisted

        # Match against now-warm cache, in upload-speed order
        for u in wave:
            if u in ctx.broken_user: continue
            match = check_for_match(...)        # cheap — cache is warm
            if match.matched:
                downloads = slskd_do_enqueue(...)
                if downloads: return EnqueueAttempt(matched=True, ...)

        if ctx.cycle_browse_time_s >= cfg.browse_cycle_budget_s:
            logger.info("cycle_browse_budget_exhausted: stopping waves for this album")
            break

    return EnqueueAttempt(matched=False, ...)


_fanout_browse(work_items, ctx, cfg) -> set[str]:
    """Submit (user, dir) browses to a bounded pool with a wave deadline.
       Populates ctx.folder_cache. Returns set of usernames that timed out."""
    # Step 1: pre-create user buckets BEFORE submitting any future,
    # to avoid setdefault races on shared inner dicts.
    for u, _ in work_items:
        ctx.folder_cache.setdefault(u, {})
        ctx._folder_cache_ts.setdefault(u, {})

    timed_out_users = set()
    pool = ThreadPoolExecutor(max_workers=cfg.browse_global_max_workers)
    try:
        futures = {pool.submit(_browse_one, slskd, u, d): (u, d) for (u, d) in work_items}
        try:
            for fut in as_completed(futures, timeout=cfg.browse_wave_deadline_s):
                user, file_dir = futures[fut]
                _, result = fut.result()
                if result is not None:
                    # Inner dicts already exist (step 1); per-key write is safe.
                    ctx.folder_cache[user][file_dir] = result
                    ctx._folder_cache_ts[user][file_dir] = time.time()
        except concurrent.futures.TimeoutError:
            for fut, (user, file_dir) in futures.items():
                if not fut.done():
                    timed_out_users.add(user)
    finally:
        # cancel_futures=True (Python 3.9+) cancels queued but not-yet-started
        # tasks. Running tasks keep running but their results are abandoned.
        # This is what makes the wave deadline actually bound wall-clock.
        pool.shutdown(wait=False, cancel_futures=True)
    return timed_out_users
```

---

## Implementation Units

- U1. **Per-cycle and per-wave instrumentation**

**Goal:** Land the measurement scaffolding first so we have a numerical baseline before changing fan-out behavior. Establishes the log-line shape that R13–R15 depend on.

**Requirements:** R13, R14, R15.

**Dependencies:** None.

**Files:**
- Modify: `cratedigger.py` (extend cycle-summary log line at `cratedigger.py:752-755` and add a top-of-cycle JSON-cache-load timer reading from `lib/cache.py`)
- Modify: `lib/cache.py` (record load duration on `load_caches`, expose via context or return value so the cycle summary can include it)
- Modify: `lib/browse.py` (per-call latency tracking already partially present in logs; add a small accumulator hookable by callers)
- Modify: `lib/enqueue.py` (record per-album browse / match wall time, accumulate into a cycle-level counter on `ctx`)
- Modify: `lib/context.py` (add timing fields to `CratediggerContext`: `browse_time_s`, `match_time_s`, `search_time_s`, `cache_load_s`, `peers_browsed`, `peers_timed_out`, `fanout_waves`)
- Test: `tests/test_cycle_summary.py` (new) or extend `tests/test_cratedigger.py` if it exists

**Approach:**
- Add accumulator fields to `CratediggerContext` (zero-initialised). Increment from the existing call sites in `_search_and_queue_parallel`, `try_enqueue`, `_browse_directories`, `check_for_match`, `load_caches`.
- The cycle summary log line at `cratedigger.py:752-755` reads accumulators and emits a structured single-line summary (key=value pairs for grep-ability).
- For wave-level logging (R14): defer to U2/U3 — the wave concept doesn't exist yet. U1 only adds the cycle-level summary and per-component timers.

**Execution note:** Test-first. Add a small orchestration test that runs a fake cycle and asserts the summary log line includes all required keys.

**Patterns to follow:**
- Existing summary line at `cratedigger.py:752-755`.
- Counter fields on `CratediggerContext` modeled after `negative_matches`, `broken_user`, `cooled_down_users` (existing per-cycle accumulators).

**Test scenarios:**
- Happy path: cycle runs successfully, summary log line emitted with all keys (`browse_time_s`, `search_time_s`, `match_time_s`, `cache_load_s`, `peers_browsed`, `cycle_total_s`).
- Edge case: cycle completes with zero work (no wanted records) — summary line still emitted with zeros, no exception.
- Edge case: cache file missing or corrupt — `cache_load_s` is 0 and the summary line still emits.
- Edge case: pyright passes on the new `CratediggerContext` fields.

**Verification:**
- A live cycle on doc2 emits the new summary line in `journalctl -u cratedigger`.
- Numerical breakdown (browse vs search vs match vs cache_load) is grep-able and approximately sums to total cycle time.

---

- U2. **Add fan-out config + `_fanout_browse_users` function + FakeSlskdUsers delay support**

**Goal:** Introduce the bounded parallel fan-out primitive, the four new config knobs that govern it (`browse_top_k`, `browse_wave_deadline_s`, `browse_global_max_workers`, `browse_cycle_budget_s`), and extend `FakeSlskdUsers` with synthetic-delay support so tests can exercise the deadline path without sleeping in real wall time. Pure infrastructure — no behavior change yet.

**Requirements:** R2, R6, R7, R16. Foundational for R1.

**Dependencies:** None (independent of U1, but ordering ships U1 first for the baseline measurement).

**Files:**
- Modify: `lib/config.py` (add `browse_top_k: int = 20`, `browse_wave_deadline_s: float = 20.0`, `browse_global_max_workers: int = 32`, `browse_cycle_budget_s: float = 240.0` to `CratediggerConfig`; add `getint`/`getfloat` parsing in `from_ini`)
- Modify: `nix/module.nix` (add `mkOption` definitions for `browseTopK`, `browseWaveDeadlineS`, `browseGlobalMaxWorkers`, `browseCycleBudgetS` under the `searchSettings` block; render into the INI template)
- Modify: `lib/browse.py` (add `_fanout_browse_users` function — see "Approach" below; reuse `_browse_one`)
- Modify: `tests/fakes.py` (add `set_directory_delay(username, dir, seconds)` to `FakeSlskdUsers` so the `directory()` method can `time.sleep(seconds)` before returning the registered result; default 0.0)
- Test: `tests/test_browse.py` (new file)

**Approach:**
- New function signature: `_fanout_browse_users(work_items: list[tuple[str, str]], slskd, ctx, max_workers: int, deadline_s: float) -> set[str]`. Returns set of usernames that timed out.
- **Step 1: pre-create user buckets.** Before submitting any future, iterate `work_items` and ensure `ctx.folder_cache.setdefault(user, {})` and `ctx._folder_cache_ts.setdefault(user, {})` exist for every user in the wave. This eliminates the cross-thread `setdefault + nested-write` race.
- **Step 2: submit.** Build a `{Future: (user, file_dir)}` dict via `pool.submit(_browse_one, slskd, user, dir)` for each work item.
- **Step 3: collect with deadline.** Use `as_completed(futures, timeout=deadline_s)` inside a `try/except TimeoutError`. On each successful future, write directly to `ctx.folder_cache[user][file_dir]` and `ctx._folder_cache_ts[user][file_dir]` — inner dicts already exist (step 1).
- **Step 4: harvest timeouts.** On `TimeoutError`, iterate the futures dict; any `not fut.done()` future contributes its `user` to `timed_out_users`. Do NOT call `fut.cancel()` on running futures (it doesn't actually stop them) — but do issue `pool.shutdown(wait=False, cancel_futures=True)` in `finally` to cancel queued (not-yet-started) futures and abandon waits on running ones. Python 3.13 in use (verified) — `cancel_futures` available since 3.9.
- **Manual lifetime, not `with` block.** A `with ThreadPoolExecutor(...)` exit calls `shutdown(wait=True)`, which would block on every still-running browse to complete (up to ~60 s TCP timeout per peer) and defeat the wave deadline. Manage the executor with explicit `try/finally` and `shutdown(wait=False, cancel_futures=True)`.
- **Orphan thread cost.** Running futures whose results are abandoned continue to occupy worker threads + slskd HTTP slots until their TCP timeout elapses. For a oneshot 5-min cycle this is acceptable: process exit reaps the threads. Logged as a known accepted cost (see Risks).
- **Cycle budget plumbing.** `_fanout_browse_users` itself doesn't know about the cycle budget — it's enforced at the caller (U3) by checking `ctx.cycle_browse_time_s` against `cfg.browse_cycle_budget_s` before each wave.
- Module VM check after Nix changes: `nix build .#checks.x86_64-linux.moduleVm`.

**Execution note:** Test-first. Write the orchestration tests below before implementing.

**Patterns to follow:**
- Config plumbing: mirror `search_response_limit` / `search_file_limit` end-to-end (`lib/config.py:85, 252` → `nix/module.nix:157-158, 581-600`).
- Executor primitives: mirror `_search_and_queue_parallel`'s `ThreadPoolExecutor` + `as_completed` shape (`cratedigger.py:707-757`), with the additions noted above (manual lifetime, deadline timeout, `cancel_futures=True`).
- Exception handling for individual browses: mirror `_browse_one`'s existing catch-all + log-and-return-None pattern (`lib/browse.py:85-99`).
- Fake extension precedent: `FakeSlskdUsers.set_directory_error` already adds per-call configurability — `set_directory_delay` follows the same pattern.

**Test scenarios:**
- Happy path: 5 users × 3 dirs (15 work items), all return immediately — `ctx.folder_cache` has 15 entries across 5 users, returned timed-out set is empty.
- Happy path: pre-create buckets exist before any future runs — verify by inspecting `ctx.folder_cache.keys()` immediately after submit (e.g., via a hook) and confirming all 5 users are present even before any future has resolved.
- Edge case: empty work list — function returns empty set, no executor created, no exception.
- Edge case: all peers fail with exceptions — `ctx.folder_cache` for those (user, dir) pairs has no entry (writes only happen on success); returned timed-out set is empty (failures aren't timeouts).
- Error path — deadline trips: 5 users; user A's two dirs return in 0.05 s, users B and C have one dir each delayed by 0.5 s (use `set_directory_delay`). Run with `deadline_s=0.2`. Assert: returned set is `{B, C}`, `ctx.folder_cache[A]` has 2 entries, `ctx.folder_cache[B]` and `ctx.folder_cache[C]` have 0 entries (or whichever of their dirs returned within the deadline). Test wall time < 1.0 s.
- Error path — wall-clock bound: same setup as above but with B's delay set to 5.0 s. Assert: function returns within `deadline_s + 0.1 s` (proves the manual shutdown + `cancel_futures` actually short-circuits, doesn't wait on B's 5 s task). Test wall time < 0.5 s.
- Edge case: `deadline_s=0` — `as_completed` raises `TimeoutError` immediately; all users in returned set; no entries written.
- Concurrency cap: submit 50 work items with `max_workers=4`. Use a `threading.Semaphore(4)` or counter inside the fake's `directory()` to track peak concurrent in-flight calls. Assert peak ≤ 4.
- Race regression: 1 user with 8 different dirs (so 8 concurrent futures all writing to the same user's inner dict). Run 100 iterations; assert no entries lost (`len(ctx.folder_cache[user]) == 8` every time).

**Verification:**
- `pyright` clean on `lib/browse.py`, `lib/config.py`, `tests/fakes.py`.
- VM check passes: `nix build .#checks.x86_64-linux.moduleVm`.
- New tests pass: `nix-shell --run "python3 -m unittest tests.test_browse -v"`.
- Full suite still green: `nix-shell --run "bash scripts/run_tests.sh"`.

---

- U3. **Refactor `try_enqueue` and `try_multi_enqueue` for top-K + lazy tail with cycle budget**

**Goal:** Replace the sequential per-user iteration with a wave-based fan-out + match loop. Add per-cycle budget short-circuit. Make `broken_user`-from-fan-out-timeouts cycle-only. This is the unit that delivers the user-visible cycle-time win.

**Requirements:** R1, R3, R4, R5, R8, R9, R14 (per-wave logging), R16 (cycle budget).

**Dependencies:** U2 (uses `_fanout_browse_users` and the new config fields).

**Files:**
- Modify: `lib/enqueue.py` (refactor `try_enqueue` at lines 196-271 and `try_multi_enqueue` at lines 272-390 to wave-based fan-out — verified function spans via `grep -n '^def '`)
- Modify: `lib/matching.py` (verify `check_for_match` still works correctly when `ctx.folder_cache[user][dir]` is already populated — the existing code at `lib/matching.py:300-314` should already short-circuit on cached entries via the `uncached` filter at line 300)
- Modify: `lib/context.py` (add `cycle_browse_time_s: float = 0.0` accumulator to `CratediggerContext` if not already added by U1; ensure `broken_user` documentation reflects the per-cycle scope)
- Test: `tests/test_enqueue.py` (extend with wave/lazy-tail/budget/regression scenarios) and `tests/test_matching.py` (regression — pre-populated cache path still works)

**Approach:**
- Compute eligible users (sort by upload speed, drop cooled-down + denylisted) before any browse.
- Chunk eligible into waves of `cfg.browse_top_k`.
- **Cycle budget guard:** at the start of each call to `try_enqueue` / `try_multi_enqueue` AND between waves, check `ctx.cycle_browse_time_s >= cfg.browse_cycle_budget_s`. If exceeded, log `cycle_browse_budget_exhausted` at INFO and return `EnqueueAttempt(matched=False, had_enqueue_failure=False)` — the request stays `wanted` and gets retried next cycle. This caps the worst-case per-cycle browse wall-time regardless of how many no-match albums are queued.
- For each wave: build flattened `(user, dir)` work list, skip already-cached entries (caller-side filter; `_fanout_browse_users` does not filter), call `_fanout_browse_users`, accumulate elapsed into `ctx.cycle_browse_time_s`, merge timed-out users into `ctx.broken_user`, then iterate matching in upload-speed order against the now-warm cache.
- Stop the outer wave loop as soon as a successful enqueue happens.
- **`broken_user` scope:** the existing `ctx.broken_user` list is per-cycle (created fresh on each `CratediggerContext` instantiation in `cratedigger.py:878`). Confirm and add an inline comment noting that fan-out-derived broken_user entries deliberately share this lifetime — a peer that timed out in cycle N is fully eligible in cycle N+1. Persistent broken_user is deferred to v2 (origin: outstanding question).
- Per-wave log line emitted from inside the outer loop (R14): `wave: K=20 n_uncached=58 n_returned=55 n_timed_out=3 elapsed_s=4.2`.
- For `try_multi_enqueue`: the per-disk loop wraps the wave loop; cache populated on disk 1 carries to disk 2; cycle budget is checked between discs as well as between waves.
- Extract a shared helper `_run_fanout_for_users(eligible, all_tracks, allowed_filetype, ctx) -> EnqueueAttempt` to keep `try_multi_enqueue`'s nesting readable.

**Execution note:** Test-first. Failing test for "top-K hits → only top-K browsed" before implementing. Then add the deadline-regression and budget-short-circuit tests before wiring those code paths.

**Patterns to follow:**
- Existing `try_enqueue` user-loop guards (`cooled_down_users`, `denied_users`, `_get_user_dirs`) — keep them intact, just move them upstream of the wave loop.
- Per-cycle log accumulator pattern from U1.

**Test scenarios:**
- Happy path — top-K hit: 30 users, top-5 contains the match, K=20. Assert: `_fanout_browse_users` called once with ~20 users worth of work; second wave never invoked. Returned `EnqueueAttempt.matched=True`.
- Happy path — lazy tail hit: 50 users, the match is at rank 35, K=20. Assert: `_fanout_browse_users` called twice (top-K + 1 chunk). Match found in second wave. Third wave never invoked.
- Edge case — all peers miss: 30 users, no match. Assert: `_fanout_browse_users` called twice (full coverage), `EnqueueAttempt.matched=False`, `had_enqueue_failure=False`.
- Edge case — 0 eligible users (all cooled down/denylisted): no fan-out call, returns `matched=False` immediately.
- Edge case — fewer than K eligible users: single wave with `n < K` items.
- Edge case — cached entries skipped: pre-populate `ctx.folder_cache[u][d]` for half the dirs; assert work list passed to `_fanout_browse_users` only contains uncached items.
- Cycle budget — short-circuit between albums: set `cfg.browse_cycle_budget_s=1.0`, `ctx.cycle_browse_time_s=2.0` (already over budget). Assert: function returns `matched=False` immediately, no `_fanout_browse_users` call.
- Cycle budget — short-circuit between waves: set `cfg.browse_cycle_budget_s=1.0`. After the first wave inflates `cycle_browse_time_s` past 1.0 (use a `set_directory_delay` to push wave time to 1.5 s), assert second wave never starts even though no match was found and more users remain.
- Error path — wave deadline trips on 3 users: those 3 land in `ctx.broken_user` after the wave; lazy tail (if reached) skips them in matching iteration.
- Match-rate regression (Reviewer 2 finding #6): 50 users; user X is at rank 25 with the only true match; users at rank 1–20 all hit a 25 s timeout (use delays > `browse_wave_deadline_s=0.2`). Assert: wave 1 marks ranks 1–20 as broken, wave 2 reaches X, X's match succeeds. (Pins down that timed-out peers from earlier waves don't deny X's candidate set — they're skipped, not failures-against-X.)
- Per-cycle scope — broken_user reset: run a full cycle that produces fan-out timeouts; instantiate a fresh `CratediggerContext`; assert the new context's `broken_user` is empty. (Smoke test of "no cross-cycle persistence.")
- Integration — `try_multi_enqueue` per-disk: 2-disc release, disc 1 finds match in user X, disc 2 finds match in user Y. Assert both source slots populated; `ctx.folder_cache` reused across discs (verify `_fanout_browse_users` not called twice for the same `(user, dir)`).
- Integration — successful enqueue on first match: assert `slskd_do_enqueue` only called once even when multiple users matched in the same wave (current behavior preserved).
- Integration — `had_enqueue_failure` tracking: enqueue raises; assert flag set; subsequent waves still run.

**Verification:**
- All existing `tests/test_enqueue.py` and `tests/test_matching.py` tests still pass (no behavior regression on the cache-warm matching path).
- New wave-shape, deadline, regression, and budget tests pass.
- `pyright` clean on touched files.
- Full suite: `nix-shell --run "bash scripts/run_tests.sh"`.
- Live cycle on doc2 emits the new per-wave log lines and (when applicable) the `cycle_browse_budget_exhausted` log line.

---

- U4. **Replace hard-coded `MAX_INFLIGHT` with `cfg.search_max_inflight`**

**Goal:** Raise the search submission pipeline depth from 2 to 4 by default, and make it configurable. Keeps the search-collection thread fed when browse is no longer the dominant cost.

**Requirements:** R10, R11.

**Dependencies:** None (orthogonal to U1–U3 — could ship before or after, but logically belongs in the same release).

**Files:**
- Modify: `cratedigger.py` (replace `MAX_INFLIGHT = 2` at line 629 with `cfg.search_max_inflight`; ensure all references — there's at least line 637, 707, 710 — read from `cfg`)
- Modify: `lib/config.py` (add `search_max_inflight: int = 4` to `CratediggerConfig`; parse in `from_ini`)
- Modify: `nix/module.nix` (add `searchMaxInflight` `mkOption`; render to INI)
- Test: extend `tests/test_search.py` or `tests/test_cratedigger.py` (whichever covers `_search_and_queue_parallel`) with a contract test that verifies the configured value is used.

**Approach:**
- Trivial substitution at known sites; no logic change.
- Submission stays sequential through the existing 429-retry loop in `_submit_search` (R11) — only the *pipeline depth* increases, not the simultaneous POST rate.

**Execution note:** Test-first.

**Patterns to follow:**
- Config plumbing: mirror U2.

**Test scenarios:**
- Happy path: `cfg.search_max_inflight = 4` → `_search_and_queue_parallel`'s `ThreadPoolExecutor` is created with `max_workers=4`. Verify by inspection of the executor or by counting concurrent in-flight collection futures with a fake.
- Edge case: `cfg.search_max_inflight = 1` (sequential mode) → still works, no regressions on the existing parallel-search test path.
- Edge case: `cfg.search_max_inflight = 8` → no 429 storms (slskd's `SearchRequestLimiter` is on POST only; we still submit serially).
- VM check still passes after Nix module change.

**Verification:**
- `pyright` clean.
- `nix build .#checks.x86_64-linux.moduleVm` passes.
- `journalctl -u cratedigger` after deploy shows the new "Pipelined search: N albums, 4 in flight" log line with the configured value.

---

## System-Wide Impact

- **Interaction graph:** `try_enqueue` is called by `find_download` (`lib/grab_list.py`) and `_grab_list_one_album` paths. Refactoring its internals doesn't change its signature or return shape — `EnqueueAttempt` is preserved. The new accumulators on `CratediggerContext` are additive, won't break callers that don't read them.
- **Error propagation:** `_fanout_browse_users` swallows per-task exceptions (matches `_browse_one`'s existing behavior) and surfaces only timed-out users. The wave-level deadline only catches `concurrent.futures.TimeoutError`; any other exception inside `as_completed` is per-future and handled inline. If `_fanout_browse_users` itself raises, `try_enqueue` should let it bubble — there's no defensible recovery and the cycle should fail loudly.
- **State lifecycle risks:** `ctx.folder_cache` and `ctx._folder_cache_ts` are mutated from worker threads. The naive pattern (`setdefault(user, {})` followed by `[file_dir] = result`) is **not** thread-safe across futures sharing the same user — CPython does not guarantee atomicity for compound `setdefault + nested-write`. The fan-out's Step 1 (pre-create user buckets in the calling thread before any future is submitted) eliminates this race. After Step 1, each `(user, file_dir)` pair is written by exactly one future to a stable inner dict, and Python's GIL covers the single-key assignment. Document this contract in `_fanout_browse_users`'s docstring and lock it in via the "race regression" test in U2.
- **API surface parity:** No public API change. Web UI is unaffected. CLI is unaffected.
- **Integration coverage:** Test scenarios in U3 explicitly cover the integration between fan-out and `check_for_match`'s warm-cache path. The slice test in `tests/test_integration_slices.py` should be extended with one wave-based scenario to prove end-to-end behavior with a real `_browse_one` against `FakeSlskdAPI`.
- **Unchanged invariants:** `_browse_directories` (the per-user version) is preserved as-is for any caller that still uses it. `EnqueueAttempt` shape unchanged. `try_enqueue` and `try_multi_enqueue` signatures unchanged. `ctx.folder_cache` semantics (24h TTL via `lib/cache.py`) unchanged. The 5 critical rules in `CLAUDE.md` (no `beet remove -d`, etc.) are untouched.

---

## Risks & Dependencies

| Risk | Mitigation |
|------|------------|
| Wave deadline doesn't actually bound wall-clock because `with ThreadPoolExecutor` exit calls `shutdown(wait=True)`, and `Future.cancel()` cannot stop a running task. | Manage the executor manually (no `with` block); use `shutdown(wait=False, cancel_futures=True)` (Python 3.9+, in use). Cancels queued tasks; running tasks orphan-complete in their own time but their results are abandoned. Tested in U2 by a wall-clock-bound assertion (function returns within `deadline_s + 0.1 s` even when one task takes 5 s). |
| Orphaned slskd HTTP calls keep running after the wave deadline, occupying connections + threads until their TCP timeout (~30–60 s). | Accepted cost. cratedigger is a 5-min-cycle oneshot — orphan threads either drain before cycle exit or die at process exit. slskd doesn't throttle browse, so the connection load is benign. Observe slskd memory/connection metrics post-deploy as a sanity check. |
| Threaded writes to `ctx.folder_cache` lose entries via `setdefault` race when multiple futures share the same user. | `_fanout_browse_users` pre-creates `ctx.folder_cache[user] = {}` and `ctx._folder_cache_ts[user] = {}` for every user in the wave **before** submitting any future (Step 1 in Approach). After that, each `(user, dir)` pair is written by exactly one future to a pre-existing inner dict; GIL covers the single-key assignment. Pinned by the "race regression" test in U2 (1 user × 8 dirs × 100 iterations, assert no entries lost). |
| Multi-album no-match cycles chain to >50 min, defeating the fix. | `cfg.browse_cycle_budget_s` (default 240 s) short-circuits remaining wanted-records when cumulative browse wall-time across all albums in the cycle exceeds the budget. Skipped albums stay `wanted` and retry next cycle. Tested in U3 by both inter-album and inter-wave short-circuit scenarios. |
| Wave-1 timeouts mark a transient-slow peer as broken_user, denying a later wave's match candidate. | `broken_user` is per-cycle scope only — fresh at the start of every cycle. A peer that timed out in cycle N is fully eligible in cycle N+1. Tested in U3 by the "match-rate regression" scenario (rank-25 user X still wins when ranks 1–20 timed out). Cross-cycle persistence is explicitly deferred to v2 (origin: outstanding question). |
| K=20 over-browses on early-match cases. | Telemetry from R13/R14 lets us tune K downward. Static K=20 is fine for v1; auto-tune deferred to v2. |
| `try_multi_enqueue`'s nested per-disk + per-wave structure becomes hard to reason about. | Extract a shared helper `_run_fanout_for_users(eligible_users, all_tracks, allowed_filetype, ctx) -> EnqueueAttempt` used by both single-disc and per-disc paths. |
| Lazy-tail waves cumulatively exceed cycle budget on no-match albums. | Real defense is upstream pre-filters (`negative_matches`, `broken_user`, `search_dir_audio_count`). Worst case is 1000 peers / 20 K = 50 waves × 20 s = 16.7 min — same as today's serial floor. The win is on the common case, not the adversarial worst case. |
| Raising `MAX_INFLIGHT` to 4 trips slskd's `SearchRequestLimiter` if our submission isn't strictly sequential. | The existing 429-retry loop in `_submit_search` already handles this; submission is sequential by construction. Tested via the existing parallel-search test suite. |
| New per-cycle log line is too verbose / changes existing log scrapers. | Format as one line of `key=value` pairs, additive to the existing summary. Existing scrapers parsing the old summary line continue to work. |

---

## Documentation / Operational Notes

- **Deploy isolation gate (operational).** Cratedigger deploys via Nix flake — `nix flake update cratedigger-src` on doc1 carries every commit on `main` since the last update. To preserve the "instrumentation lands first, baseline measured before fix" property, U1 must ship in its own flake bump *and* run on doc2 for ≥24 h before U2's PR is even opened. Concretely: merge U1 → bump flake on doc1 → deploy → wait 24 h → review baseline numbers in `journalctl` against the success criteria → THEN start U2. Without this gating, U1's measurement value evaporates.
- After U1 deploys + 24 h baseline window: capture browse / search / match / cache_load percentages from `journalctl -u cratedigger` and post as a "Before" comment on issue #198.
- After U2–U4 deploy: same observation window (24 h), post the "After" data on the same issue. Compare worst-case cycle, median cycle, match rate, and `cycle_browse_budget_exhausted` log frequency.
- **Soulseek.NET concurrency observation.** `docs/slskd-internals.md` notes the per-peer connection pool ceiling is opaque. After U2–U4 deploy, watch slskd's own logs for queueing or stall behavior under the new fan-out load. If we see slskd-side serialization, retune `browse_global_max_workers` downward — config knob, no code change.
- Update `docs/parallel-search.md` if the search depth raise materially changes its narrative (it shouldn't — submit-sequential / collect-parallel pattern is unchanged).
- `docs/slskd-internals.md` already documents the underlying constraints — no updates needed.
- The 538 MB `cratedigger_cache.json` and the in-memory load tax remain — separate work tracked in [#201](https://github.com/abl030/cratedigger/issues/201). U1's `cache_load_s` log line gives us the data to prioritize that effort. **Watch for cache write storm** post-fix: faster fan-out caches more peers per cycle, so the JSON file may grow faster than today. Not a blocker (24h TTL eventually evicts), but a signal to accelerate #201 if growth crosses ~1 GB.

---

## Sources & References

- **Origin document:** [docs/brainstorms/browse-fanout-and-pipeline-depth-requirements.md](../brainstorms/browse-fanout-and-pipeline-depth-requirements.md)
- **Issue:** [#198](https://github.com/abl030/cratedigger/issues/198) (with two correction comments dated 2026-05-01)
- **Related issue (out-of-scope follow-up):** [#201](https://github.com/abl030/cratedigger/issues/201) — Migrate folder_cache to Redis
- **slskd internals:** [docs/slskd-internals.md](../slskd-internals.md)
- **Existing parallel-search pattern:** [docs/parallel-search.md](../parallel-search.md)
- **Code:** `lib/browse.py:85-129`, `lib/enqueue.py:196-340`, `lib/matching.py:296-322`, `cratedigger.py:627-757, 752-755`, `lib/config.py:83-260`, `nix/module.nix:154-160, 579-600`
- **Test infrastructure:** `tests/fakes.py:189-221, 310-392`, `tests/helpers.py:298-316`
