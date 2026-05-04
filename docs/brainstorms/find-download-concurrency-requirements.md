---
date: 2026-05-04
topic: find-download-concurrency
issue: 217
---

# Find Download Concurrency — Remove Main-Thread Search Pipeline Blocking

## Summary

Completed searches should hand their results to concurrent `find_download` work immediately, then refill search slots without waiting for browse, match, or enqueue to finish. Browse pressure stays bounded by one true global browse limit, and the slskd HTTP connection pool is sized from that limit so concurrency is real rather than connection churn.

---

## Problem Frame

Issue #198 fixed the original serial browse fan-out problem inside a single album's enqueue path, and issue #213 fixed the stuck-search failure mode with a per-search progress watchdog. The remaining #217 trace shows a different bottleneck: once a search completes, the main search loop runs `find_download` synchronously before it submits the next search. During that time, search collector slots are idle even though more wanted albums are available.

This did not dominate in the older pipeline because each completed search carried a much smaller candidate surface. The search-escalation work and #198 tuning intentionally widened that surface: higher response limits, higher file limits, and more productive un-wildcarded/escalated queries now feed many more peer directories into `find_download`. The serial architecture was tolerable when each album was cheap; it is exposed now that each album can require substantial browse-and-match work.

Production logs also show repeated `Connection pool is full, discarding connection: localhost. Connection pool size: 10` warnings. The configured browse worker capacity is 32, but the slskd Python client inherits requests' default HTTP pool size of 10 unless cratedigger configures it. That mismatch means concurrency above 10 creates disposable connections and noisy logs rather than clean reusable capacity.

---

## Actors

- A1. Search pipeline: submits searches, collects slskd results, records search outcomes, and keeps the cycle moving.
- A2. `find_download` worker: browses candidate peer directories, scores matches, and enqueues a matched download when found.
- A3. Browse coordinator: enforces total browse concurrency and owns safe shared access to cached directory data.
- A4. slskd HTTP client: carries search, browse, transfer, and cancel API calls to the local slskd service.
- A5. Operator: reads cycle logs and throughput metrics to decide whether the pipeline is healthy.

---

## Key Flows

- F1. Search completion hands off to `find_download`
  - **Trigger:** A search collector finishes with usable peer results for an album.
  - **Actors:** A1, A2
  - **Steps:** The search result is merged into the album's match input, a `find_download` job is queued, and the search pipeline immediately attempts to submit the next wanted album.
  - **Outcome:** Search slots are not blocked by browse, match, or enqueue work for a prior album.
  - **Covered by:** R1, R2, R3, R4

- F2. Concurrent `find_download` jobs share browse capacity
  - **Trigger:** Multiple completed searches are ready for `find_download` work in the same cycle.
  - **Actors:** A2, A3, A4
  - **Steps:** Each `find_download` job proceeds independently, but every uncached slskd directory browse passes through one shared browse capacity limit before hitting slskd.
  - **Outcome:** Completed albums can progress concurrently without multiplying browse pressure by album count.
  - **Covered by:** R5, R6, R7, R8

- F3. Completed `find_download` results are merged and logged
  - **Trigger:** A `find_download` job returns found, no-match, or enqueue-failed.
  - **Actors:** A1, A2
  - **Steps:** The search pipeline merges the job's result into the cycle's found/failed collections, copies candidate forensics onto the corresponding search result, and logs the final search outcome.
  - **Outcome:** Existing search-log semantics and download queue behavior are preserved while work runs concurrently.
  - **Covered by:** R9, R10, R11

---

## Requirements

**Search pipeline handoff**

- R1. A successful search completion must not run `find_download` inline on the search pipeline's main completion path.
- R2. A successful search completion must queue `find_download` work and then immediately make the search slot eligible to submit the next wanted album.
- R3. Search collection concurrency and `find_download` concurrency must be decoupled: the existing search-side in-flight setting continues to control search collection only.
- R4. There must be no separate `find_download_concurrency` cap. The number of active `find_download` jobs may grow with completed successful searches in the current cycle.

**Global browse capacity**

- R5. Browse capacity must be global across the process, not local to one album or one `find_download` call.
- R6. `browse_global_max_workers` remains the browse capacity control, with the current default of 32.
- R7. When multiple `find_download` jobs run concurrently, `browse_global_max_workers=32` must mean at most 32 slskd directory browse calls in flight total.
- R8. Concurrent `find_download` jobs must share cached directory results so one worker's successful browse can be reused by later workers in the same cycle and by persisted cache saves.

**slskd HTTP connection pool**

- R9. The slskd HTTP connection pool size must be configured automatically from the concurrency settings instead of relying on requests' default pool size of 10.
- R10. The configured HTTP pool must be large enough for the global browse limit plus search and transfer headroom.
- R11. When demand exceeds the configured HTTP pool, cratedigger should backpressure rather than create and discard excess localhost connections.
- R12. Repeated `Connection pool is full, discarding connection` warnings during normal browse fan-out are a regression signal after this work lands.

**Worker isolation and result merging**

- R13. `find_download` workers must not mutate the cycle's final result collections directly.
- R14. Per-album matching state must be isolated so concurrent `find_download` jobs cannot clear or pollute each other's negative-match decisions.
- R15. Shared cache and broken-peer state must be accessed through a safe shared mechanism, not ad hoc concurrent mutation of the full cycle context.
- R16. Search outcome logging must remain anchored to the matching/enqueue result from the corresponding album, including candidate forensic data.
- R17. Concurrent enqueue side effects are allowed to remain inside `find_download`; this issue must not split candidate selection from transfer enqueue.

**Instrumentation and observability**

- R18. Cycle logs must make it possible to distinguish search collection time, `find_download` drain time, browse time, and local match/scoring time.
- R19. Logs or metrics must expose enough concurrency evidence to verify that search slots refill while earlier `find_download` jobs are still running.
- R20. The cycle summary must retain existing browse, match, peer-count, fan-out, and watchdog observability, with any new fields added only where they clarify the new concurrent flow.

---

## Acceptance Examples

- AE1. **Covers R1, R2, R3.** Given four search collector slots are in use and one search completes successfully, when its result is handed to `find_download`, the search pipeline submits or attempts to submit the next wanted album before that `find_download` job finishes.
- AE2. **Covers R4, R5, R7.** Given sixteen searches complete quickly and all require cold directory browse work, when their `find_download` jobs run, the system may have sixteen active jobs but no more than 32 total slskd directory browse calls in flight.
- AE3. **Covers R8, R14, R15.** Given two concurrent `find_download` jobs encounter the same user directory, when one job browses and caches the directory successfully, the other job can reuse the cached directory without corrupting either album's per-album negative-match state.
- AE4. **Covers R9, R10, R11, R12.** Given `browse_global_max_workers=32` and normal search/enqueue headroom, when a large fan-out cycle runs, the slskd HTTP client has a pool sized from that concurrency and does not emit repeated default-pool-size warnings.
- AE5. **Covers R16, R17.** Given a concurrent `find_download` job finds and enqueues a match, when the job completes, the corresponding search log row records `found` with its candidate forensics, and the download appears in the same downstream polling flow as before.

---

## Success Criteria

- Search pipeline idle gaps caused by synchronous `find_download` processing disappear from production traces.
- For a #217-shaped cycle, wall time moves materially toward search time plus bounded `find_download` drain time rather than search time plus the sum of every album's serial `find_download` time.
- Daily search throughput recovers toward the pre-regression baseline from the #198/#213 discussion without reducing search thoroughness.
- Match, import, and enqueue success rates do not regress because of concurrent processing.
- Normal production logs stop emitting repeated slskd HTTP connection-pool warnings during browse fan-out.
- A downstream planner can design the implementation without inventing the core behavior: search handoff, unlimited active `find_download` jobs, true global browse capacity, HTTP pool sizing, worker isolation, concurrent enqueue, and observability are all decided here.

---

## Scope Boundaries

- Do not reintroduce per-cycle gates, browse-wave deadlines, or wall-clock caps to make cycles look bounded.
- Do not reduce `search_response_limit` or `search_file_limit` to shrink the candidate surface and hide the bottleneck.
- Do not change search ladder behavior, query variants, or strict matching policy.
- Do not tune search-side in-flight depth as part of this issue.
- Do not add a separate `find_download_concurrency` configuration knob.
- Do not split `find_download` into candidate selection plus serialized enqueue for this issue.
- Do not treat connection-pool warnings as harmless expected noise after the HTTP pool is sized from concurrency.

---

## Key Decisions

- `find_download` is the concurrent work unit: this matches the current mental model of "match" while preserving the fact that the function also browses and enqueues.
- No `find_download_concurrency` cap: active work should be limited by available completed searches and by shared downstream resources, not by another album-count knob.
- Browse pressure is the real scarce resource: concurrency control belongs around total slskd directory browse calls.
- Keep browse capacity at 32: the current production setting remains the capacity target; the HTTP client must be brought up to match it instead of lowering the browse cap to the client default.
- Size the slskd HTTP pool automatically: a configured browse limit higher than 10 must not rely on requests' default connection pool.
- Allow concurrent enqueue: serializing enqueue would add a new bottleneck before there is evidence that slskd transfer enqueue requires it.

---

## Dependencies / Assumptions

- The slskd browse endpoint can tolerate the existing configured browse capacity of 32 when the HTTP client pool is sized accordingly.
- requests' default pool size of 10 is the source of the observed connection-pool warnings, not a slskd server-side concurrency limit.
- `find_download` is mostly waiting on browse and slskd API work rather than CPU-bound local scoring, so concurrent jobs should improve wall time.
- Existing download polling can handle concurrent enqueues because it already reconstructs and monitors active downloads from persisted state.
- Some shared state must remain shared because directory browse results are intentionally cached across albums and cycles; the requirement is to narrow and coordinate that sharing, not eliminate cache sharing entirely.

---

## Outstanding Questions

### Resolve Before Planning

*(none)*

### Deferred to Planning

- [Affects R8, R15][Technical] What is the smallest safe shared cache mechanism: locks around the current cache maps, a dedicated browse/cache coordinator object, or centralizing writes on one owner thread?
- [Affects R9, R10, R11][Technical] What exact HTTP pool sizing formula should be used for search and transfer headroom beyond `browse_global_max_workers`?
- [Affects R18, R19, R20][Technical] Which new timing fields best expose `find_download` queue/drain behavior without bloating the cycle summary?
