---
date: 2026-05-08
topic: persisted-search-plans
---

# Persisted Search Plans

## Summary

Replace recomputed, `search_attempts`-selected search variants with durable per-request search plans. Each wanted request gets a versioned, inspectable set of runnable search slots; the pipeline consumes one slot at a time, logs execution against that exact slot, advances a request-level cursor, and wraps through the plan indefinitely with a cycle count.

---

## Problem Frame

Cratedigger's search behavior has grown into a useful but hard-to-reason-about ladder. `lib/search.py` owns query normalization and variant selection, while `cratedigger.py` uses the mutable `album_requests.search_attempts` value to decide which variant to run next. That counter now carries multiple meanings at once: retry history, query-variant selector, cycle progress, and part of the scheduling/backoff story.

The result is hidden state. A request's next search intent is not visible as data; it is reconstructed every time from current metadata, current generator code, and a counter. When search behavior changes, historical intent is hard to audit because old logs contain query strings and variant labels but not the exact planned slot they came from. Repeated default searches, accidental duplicate queries, omitted track candidates, low-entropy token drops, and cycle wraps are all implicit in code rather than inspectable per request.

Recent Redis/cache investigation made this more obvious. The operator wants to understand how useful searches are, not merely what is currently cached or which query string ran. That requires durable search identity: the ability to expand a request and see the planned search set, where the cursor is, how many times each slot has run, how expensive each slot was, which slots returned results, which were deduped or omitted, and why the generator produced that plan.

Older brainstorms such as `docs/brainstorms/search-escalation-and-forensics-requirements.md` and `docs/brainstorms/2026-05-04-search-watchdog-requirements.md` improved search breadth, per-search reliability, and forensics. This brainstorm supersedes only the part of the old model where variant selection is recomputed from `search_attempts`. It does not change strict matching, slskd watchdog semantics, browse fan-out, or the broader acquisition pipeline.

This is not a strictly behavior-preserving persistence rewrite. Two search behavior changes are intentional and must be treated as first-class decisions: track-level search slots are capped to the three strongest normalized track queries, and the old synthetic `exhausted` search outcome is replaced by cursor wrap plus request cycle count.

---

## Actors

- A1. Search-plan generator: Produces a deterministic ordered search plan from a resolved release metadata snapshot.
- A2. Release resolver: Resolves MBID/Discogs identity into the metadata snapshot consumed by the generator.
- A3. Startup/preflight reconciler: Ensures wanted requests have a current-generator search plan before search execution begins.
- A4. Search executor: Selects the next persisted plan item, runs it, logs the result, and advances the request cursor.
- A5. Operator: Uses `pipeline-cli`, API output, logs, and a future dashboard to inspect search behavior and regenerate plans.
- A6. Future dashboard: Expands a request into search-plan state, per-item telemetry, generation failures, and provenance. The dashboard UI itself is not part of the initial implementation.

---

## Key Flows

- F1. Add-time plan generation
  - **Trigger:** A new album request is added from the web UI or CLI.
  - **Actors:** A2, A1, A5
  - **Steps:** Resolve the request's source identity into release metadata. Persist the request and track data, or otherwise use the same complete resolved payload that will be persisted. Generate a current-generator search plan from that metadata and the shared generation-affecting config. Store the plan, its items, and provenance. Attach the successful plan to the request and initialize the request cursor at the first slot. If deterministic generation fails, store the failed plan attempt and leave the request visibly wanted-but-not-searchable.
  - **Outcome:** New wanted requests are immediately inspectable, including generated search slots or a visible generation failure.
  - **Covered by:** R1-R13, R40-R43, R61-R62, R65

- F2. Startup plan reconciliation
  - **Trigger:** Cratedigger starts a cycle before search execution.
  - **Actors:** A3, A2, A1
  - **Steps:** Scan all wanted requests, independent of search page size or retry eligibility. For each request, check whether it has a plan for the current generator id. Missing or old-generator requests get a new plan attempt. Existing deterministic failed plans for the current generator id are reported but not retried. Transient resolver or dependency failures are reported and retried on later startup runs. Each request is isolated so one bad request cannot block the cycle.
  - **Outcome:** Before search execution, every wanted request has either a current successful plan or a current visible failed plan.
  - **Covered by:** R7, R9-R10, R37-R42, R44, R63-R65

- F3. One-slot search execution
  - **Trigger:** A wanted request with a successful active current plan is eligible for search.
  - **Actors:** A4
  - **Steps:** Read the request's active plan and next ordinal. Run exactly that persisted query. Write a search log row tied to the exact plan slot. Atomically record the log and cursor advance/wrap for consumed attempts. If the request leaves `wanted`, the cursor state remains on the request for any future return.
  - **Outcome:** Runtime search selection is explicit data lookup, not reinterpretation of `search_attempts`.
  - **Covered by:** R26-R32, R36, R51-R52, R66-R69

- F4. Plan wrap
  - **Trigger:** A logged search attempt consumes the final plan slot.
  - **Actors:** A4
  - **Steps:** Advance the request cursor back to the first slot and increment the request's search cycle count. Do not emit a synthetic `exhausted` search outcome or cycle-complete database event.
  - **Outcome:** Search progress is represented by request cursor state and cycle count.
  - **Covered by:** R33-R35

- F5. Explicit regeneration
  - **Trigger:** An operator invokes the CLI/API regeneration action for a request.
  - **Actors:** A5, A2, A1
  - **Steps:** Resolve metadata, create a new current-generator plan attempt even if a current failed plan already exists, mark the new successful plan active if generation succeeds, and reset the request cursor and cycle count. Preserve old plans and logs for audit.
  - **Outcome:** Bad metadata or generator bugs can be retried without bumping the global generator id.
  - **Covered by:** R45-R50

- F6. Inspection
  - **Trigger:** An operator inspects a request's search plan through CLI/API now, or a dashboard later.
  - **Actors:** A5, A6
  - **Steps:** Show current plan status, generator id, cursor, cycle count, planned slots, executable queries, provenance, omitted/deduped candidates, generation failures, and per-slot/per-query execution statistics from search logs.
  - **Outcome:** Search pathologies can be debugged from persisted state without reverse-engineering the generator or manually joining raw logs. The initial implementation produces dashboard-ready data and API/CLI output, not the dashboard UI.
  - **Covered by:** R50-R55, R71-R74

---

## Requirements

**Plan Generation**

- R1. Search-plan generation must have one authoritative public boundary that owns query construction behavior for the pipeline. This should extend or replace the existing pure search helpers rather than creating a parallel second source of query rules.
- R2. The generator must consume a resolved release metadata snapshot, not perform MBID/Discogs lookup itself. The resolver owns source identity lookup; the generator owns deterministic search-plan construction.
- R3. The metadata snapshot must include the information needed to generate current search behavior: source identity, artist, release title, year, track titles, and source type.
- R4. The generator must be deterministic for a given metadata snapshot, generator id, and generation-affecting configuration snapshot.
- R5. Query normalization, short-token handling, low-entropy token dropping, wildcard/unwild behavior, track-query construction, dedupe, and ranking must live behind the search-plan generator boundary.
- R6. Each plan must store the generator id used to produce it. The current generator id is a manually bumped integer and is the authoritative automatic-regeneration key.
- R7. Bumping the current generator id must cause wanted requests to receive new current plans during startup reconciliation. Any code or configuration change that changes generated plan output must include a generator-id bump.

**Plan Persistence**

- R8. The full generated search set must be persisted, not generated on read.
- R9. Successful and failed generation attempts must both create durable plan records.
- R10. Failed generation records must preserve enough context to explain the failure: metadata snapshot, generation error, and provenance for omitted candidates when available.
- R11. A successful plan must store one row or equivalent durable unit per runnable search slot.
- R12. A plan item must store executable fields and bounded debug provenance. Executable fields include at least ordinal, strategy, query, and canonical query key. Provenance must capture the source snapshot reference, source track index when applicable, normalized tokens, dropped tokens with reasons, dedupe winner/losers, ranking tuple, and omission reason for candidates that did not become runnable slots.
- R13. Plan items must be inspectable before execution and after execution.
- R14. Existing historical `search_log` rows do not need backfill to plan items.

**Plan Shape**

- R15. The generated plan must preserve the current album-level cycle shape: repeated default searches, then unwild, then unwild-plus-year when year is known, then track-level searches.
- R16. Default repeats must be materialized as separate plan slots according to the configured escalation threshold.
- R17. Repeated default slots must remain separate execution slots even though they share the same canonical query identity.
- R18. Accidental duplicate queries from different strategies or candidates must be collapsed inside the generator. The first or highest-priority candidate wins, and deduped alternatives are recorded in provenance.
- R19. Plan items must always be runnable search queries. Empty, invalid, skipped, or omitted candidates must not become executable plan slots.
- R20. A single-track album must skip the track tier.
- R21. Multi-track albums may contribute at most three track-level plan items.
- R22. Track candidates must be normalized, low-entropy-filtered, validated, and deduped before ranking.
- R23. The three track candidates must be selected by useful token count descending, then character count descending, then original track order ascending.
- R24. The initial low-entropy token drop list must be the current four tokens: `the`, `you`, `from`, and `and`.
- R25. Expanding the low-entropy list is out of scope for the initial persisted-plan rewrite and must be treated as a future generator-id change.

**Request Cursor and Execution**

- R26. Cursor state belongs to the request, not the plan. The request tracks the active plan, next search ordinal, and search cycle count.
- R27. Runtime search selection must use only the active plan and next search ordinal. It must not use `search_attempts` to choose a query variant.
- R28. One eligible search execution consumes exactly one persisted plan item.
- R29. The executor must advance the request cursor only after a search attempt has been logged.
- R30. Pre-attempt failures must not consume the current plan slot, but they must still produce visible failure/backoff telemetry so setup or slskd problems do not spin hot or disappear.
- R31. Search outcomes that enqueue or find a candidate must still advance the cursor after logging.
- R32. If the request leaves `wanted` and later returns to `wanted`, it must keep the same active plan, cursor, and cycle count unless a new generator id or explicit regeneration replaces the plan.
- R33. Reaching the end of the plan must wrap the cursor to the first ordinal and increment the request's search cycle count.
- R34. The new executor must not write `outcome='exhausted'`. Historical `exhausted` rows remain historical.
- R35. Plan wrap must not create a synthetic database search-log row. The cycle count is the observable wrap signal.
- R36. Scheduling/backoff state may continue to exist, but it must be separate from search-plan cursor state and must not select query variants. Cutover must preserve existing retry/backoff eligibility so deployment does not unintentionally flood the queue with immediate default searches.

**Startup and Failure Handling**

- R37. Startup/preflight must ensure every `wanted` request has a plan for the current generator id before search execution begins.
- R38. Current-generator plan coverage is required only for `wanted` requests.
- R39. Startup plan generation must be isolated per request: one generation failure must not block other wanted requests.
- R40. If a wanted request already has a deterministic failed plan for the current generator id, startup must report it but must not retry generation automatically.
- R41. Wanted requests with deterministic failed current plans must remain `wanted`, be visibly marked as failed-plan/not-searchable, excluded from normal searchable-request counts, and skipped by search execution.
- R42. Startup logs must summarize current-generator plan coverage and enumerate failed generation signals clearly enough for an operator to notice generator bugs or bad metadata.

**Regeneration and Operator Surfaces**

- R43. New requests must attempt add-time search-plan generation after the request's resolved metadata and track data are available to the generator.
- R44. Startup/preflight remains the repair/backfill mechanism for requests missing current plans.
- R45. Explicit per-request regeneration must be available through both API and `pipeline-cli`.
- R46. Explicit regeneration must retry even when a current failed plan already exists.
- R47. Explicit regeneration must create a new current-generator plan attempt, preserve old plans/logs, and reset request cursor and cycle count on success.
- R48. The API shape must be request-scoped: one read surface for a request's search plan and one action surface for regeneration.
- R49. The CLI shape must be request-scoped and include at least `search-plan show <request_id>` and `search-plan regenerate <request_id>`.
- R50. CLI/API inspection must show static plan data and execution statistics, not only raw plan rows.

**Search Logs and Audit**

- R51. Every new search log row must attach to the exact executed plan context: request, plan, plan item, ordinal, strategy, query, and canonical query key.
- R52. Search logs must snapshot the request's cycle count at execution time.
- R53. Repeated default slots must remain visible as separate executions while still supporting grouped audit views by canonical query key or repeat group.
- R54. Existing search telemetry such as result counts, elapsed time, outcomes, browse/match timings, peer counts, fanout, and candidate forensics must remain available and must be joinable/groupable by plan item.
- R55. Cache usefulness must be explicit rather than implied: if per-search cache counters are already available in the search/match metrics path, they must be persisted with the plan-item log context; if they are only cycle-level today, the initial plan rewrite must expose that limitation and defer deeper cache attribution instead of pretending per-slot cache data exists.

**Cutover, Testing, and Documentation**

- R56. The implementation should be a hard cut-over, not a dual-path feature flag.
- R57. Existing `search_attempts` must not be translated into new cursor positions. Backfilled current plans start at ordinal 0 with cycle count 0.
- R58. Tests must assert full generator output, including executable fields and provenance, so search behavior changes are visible and intentional.
- R59. Pipeline tests must cover cursor advance, wrap, logged-attempt semantics, found/enqueued cursor behavior, failed generation skip behavior, startup reconciliation, and explicit regeneration.
- R60. Documentation must be updated so future agents and operators understand search plans, generator ids, cursor/cycle semantics, and the replacement of `search_attempts`-driven variant selection.

**Operational Safety and Usefulness**

- R61. Add-time generation must use the same generation-affecting configuration source as startup and executor reconciliation. Web, CLI, and service paths must not create different plans for the same metadata under the same generator id.
- R62. Add-time generation must not run before track extraction/persistence in a way that produces an album-level-only plan for a release whose tracks are available.
- R63. Startup reconciliation must use an all-wanted request scan that ignores search page size and retry/backoff eligibility. It must not reuse only the normal searchable/wanted picker.
- R64. Resolver/dependency failures must be distinguishable from deterministic generator failures. Transient resolver, mirror, config-load, or database failures must remain retryable; deterministic no-runnable-query or generator-rule failures may be sticky for the current generator id.
- R65. Empty tracklists must not fail generation by themselves. They produce album-level-only plans unless artist/title normalization also cannot produce a runnable query.
- R66. Consumed-attempt logging and cursor advance/wrap must be atomic from the pipeline's perspective: a consumed search attempt must not be durably logged without the matching cursor update, and the cursor must not advance without the matching audit log.
- R67. Cursor advancement must validate that the request still points at the same active plan and ordinal that the search executed. A stale in-flight search from a superseded plan may be logged against its executed plan context but must not advance the new active plan cursor.
- R68. Explicit regeneration must be safe while searches are in flight: old-plan completions remain auditable, and active-plan cursor state cannot be overwritten by stale completions.
- R69. Pre-attempt failures must have a visible non-consuming path that applies appropriate scheduling/backoff without advancing the plan cursor.
- R70. Removing new `exhausted` rows must not remove operational visibility into plan wraps. CLI/API/readiness views must be able to derive wrap counts from cycle count and final-slot execution logs.
- R71. "Search usefulness" for v1 means enough data to rank plan slots and canonical query groups by observed value and cost: attempts, outcomes, found/enqueued count, no-result/no-match/error count, average and total elapsed time, average result count, browse/match/peer/fanout cost, cycle distribution, and cache counters when per-search attribution exists.
- R72. Initial dashboard readiness means durable data and API/CLI output with stable aggregate dimensions. It does not mean building a dashboard UI in this change.
- R73. Aggregate dimensions required for future usefulness views include generator id, plan status, strategy, ordinal, cycle count, canonical query key, repeat group, and failed-plan state.
- R74. The behavior change from all valid track queries to top-three normalized track queries must be observable after deploy through before/after search yield, cost, and coverage metrics. The rewrite must not hide this as a mere persistence refactor.
- R75. The generator id must have a single runtime source shared by the service, CLI, and API. Tests or review checks must make generator-output changes without an id bump visible before deploy.

---

## Acceptance Examples

- AE1. **Covers R1-R8, R43, R44.** Given a new wanted request is added for a release with artist, title, year, and tracks, when add-time generation runs, then the request receives a current-generator plan with materialized default, unwild, optional year, and up-to-three track slots, and `search-plan show` can display it before any search executes.

- AE2. **Covers R9-R13, R19, R40-R42.** Given a wanted request whose metadata cannot produce any runnable query, when generation runs, then a failed plan record is stored with error/provenance, the request remains wanted, startup reports the failed generation on later cycles without retrying it, and the search executor skips that request.

- AE3. **Covers R15-R18, R26-R28, R53.** Given the configured escalation threshold creates repeated default searches, when the plan is generated, then each default repeat is a separate ordinal slot sharing query identity metadata; accidental duplicate non-default candidates are collapsed and recorded as deduped provenance.

- AE4. **Covers R20-R24.** Given a multi-track release with noisy track titles, when the generator builds track candidates, then it drops `the`, `you`, `from`, and `and`, validates/dedupes candidates, selects at most three by token count, character count, and track order, and records skipped or deduped candidates in provenance.

- AE5. **Covers R20-R21.** Given a single-track release, when the plan is generated, then the plan includes album-level slots only and no track-level search slot is emitted.

- AE6. **Covers R27-R35, R51-R52.** Given a wanted request with a successful active plan and `next_search_ordinal=2`, when the executor runs a search, then it executes ordinal 2, writes a search log tied to that exact plan slot and cycle count, and advances to ordinal 3 only after logging.

- AE7. **Covers R29-R31.** Given slskd or local setup fails before a meaningful search attempt is logged, when the executor handles the failure, then the request's next ordinal remains unchanged; given a search logs `found` and enqueues a download, then the cursor advances normally.

- AE8. **Covers R32-R35.** Given a request consumes the final slot in its plan, when the attempt is logged, then the request wraps to ordinal 0, increments cycle count, and does not write a new `exhausted` search outcome or synthetic cycle-complete search row.

- AE9. **Covers R37-R41, R56-R57.** Given existing wanted requests after deployment, when startup/preflight runs under a new current generator id, then each wanted request gets a current plan or visible failed plan, each successful plan starts at ordinal 0 and cycle count 0, and old `search_attempts` values do not affect the new cursor.

- AE10. **Covers R45-R50.** Given an operator regenerates a request's search plan via CLI or API, when generation succeeds, then a new current plan becomes active, old plans/logs remain inspectable, cursor/cycle reset to the top, and `search-plan show` reports both plan structure and execution stats.

- AE11. **Covers R36, R54-R55.** Given scheduling/backoff fields still exist, when repeated searches fail, then scheduling may pace future eligibility, but query choice still comes only from active plan plus next ordinal; telemetry remains groupable by exact plan slot and canonical query identity.

- AE12. **Covers R61-R65.** Given a startup run happens while the MusicBrainz or Discogs resolver is temporarily unavailable, when reconciliation attempts to plan wanted requests, then those failures are reported as retryable resolver/dependency failures rather than sticky current-generator plan failures; when the resolver later recovers, startup can generate plans without manual per-request regeneration.

- AE13. **Covers R43, R61-R62, R65.** Given a web or CLI add flow resolves a release with track data, when add-time generation runs, then the generated plan sees the same track data that is persisted for the request and uses the same generation-affecting config as the service startup path.

- AE14. **Covers R29-R31, R66-R69.** Given a search starts against plan A ordinal 4 and an operator regenerates the request to plan B before the search completes, when the old search logs its result, then the log remains attached to plan A and cannot advance plan B's cursor.

- AE15. **Covers R4, R6-R7, R75.** Given a change to default repeat count, low-entropy filtering, wildcard behavior, or track ranking would change generated plan output, when that change is prepared for deploy, then the generator id changes with it and service/CLI/API all report the same current id.

- AE16. **Covers R70-R74.** Given an operator asks which search slots are useful after deployment, when they inspect CLI/API output, then they can rank plan slots and query groups by attempts, outcomes, cost, cycle distribution, wrap behavior, and cache counters where per-search attribution exists, without relying on `outcome='exhausted'` rows.

---

## Success Criteria

- Search intent is durable: for any wanted request with a successful plan, an operator can inspect the exact ordered search set before the next search runs.
- The executor no longer recomputes variant selection from `search_attempts`; query choice is a lookup against active plan and request cursor.
- Startup reliably reconciles wanted requests to the current generator id and clearly reports current failed generation signals.
- Per-request search behavior becomes debuggable: CLI/API output can show generated slots, cursor, cycle count, omitted/deduped candidates, per-slot attempts, timings, result counts, outcomes, and cache/browse usefulness.
- Search usefulness is operational, not only archival: CLI/API output can rank expensive low-yield slots and high-value slots by stable dimensions that a future dashboard can reuse.
- Deployment safety is explicit: transient resolver failures do not strand the wanted queue, consumed logs and cursor updates are atomic, and in-flight old-plan completions cannot corrupt regenerated cursor state.
- The top-three track-query behavior change is measurable after deploy through yield, cost, and coverage comparison, rather than hidden inside a persistence migration.
- Search behavior changes are explicit: generator output tests fail when normalization, ordering, dedupe, default repeats, track selection, or provenance changes without a corresponding intentional update.
- A downstream planner can implement the work without inventing product behavior around generator ownership, persistence, cursor semantics, failure handling, regeneration, or audit expectations.

---

## Scope Boundaries

- Do not change strict album matching or import acceptance policy.
- Do not change slskd response limits, browse fan-out, watchdog behavior, download validation, or import dispatch as part of this rewrite.
- Do not build the full search dashboard now. The persisted model must support it, but the first cut only needs CLI/API/readiness surfaces.
- Do not expand the low-entropy token list beyond `the`, `you`, `from`, and `and` in this change.
- Do not introduce a more complex entropy model for track ranking beyond token count, character count, and track order.
- Do not automatically regenerate plans because source metadata changed. Generator id drives automatic regeneration.
- Do not add automatic config-fingerprint invalidation. Generation-affecting config changes require an explicit generator-id bump.
- Do not translate old `search_attempts` into new cursor state.
- Do not create a dual implementation path or long-lived feature flag.
- Do not backfill historical search logs to plan items.
- Do not add runtime skip slots for omitted candidates; failed/omitted generation information belongs in provenance, not executable plan slots.
- Do not require new per-search Redis/cache instrumentation unless the implementation confirms the existing match/search metrics already expose it. The plan model should attach cache counters when available and be honest when they are only cycle-level.

---

## Key Decisions

- D1. Search-plan generation has one authoritative boundary.
- D2. The generator takes resolved release metadata, not raw MBID/Discogs lookup responsibility.
- D3. Resolver and generator remain separate so generation can be deterministic and testable.
- D4. The full generated search plan is persisted.
- D5. Search plans are versioned by a manually bumped current generator id.
- D6. Plans store enough metadata snapshot and provenance to audit why they were generated.
- D7. Request cursor state lives on the request, not the plan.
- D8. Runtime search selection uses active plan plus next ordinal.
- D9. One eligible search run consumes exactly one plan item.
- D10. Plan items are always runnable queries.
- D11. If no runnable plan can be generated, generation fails visibly instead of creating skip slots.
- D12. Failed generation still creates a durable plan record.
- D13. Failed generation keeps the request wanted.
- D14. Failed-plan wanted requests are skipped by search execution.
- D15. Existing failed plans for the current generator id are sticky at startup and are not retried automatically.
- D16. Startup reports failed generation signals clearly.
- D17. Startup reconciles wanted requests to the current generator id.
- D18. Startup generation is isolated per request.
- D19. Current-generator plan coverage is required only for wanted requests.
- D20. Add-time generation and startup/backfill generation are both required.
- D21. New generator id creates a new active plan and resets cursor to ordinal 0.
- D22. New generator id resets search cycle count.
- D23. Old plans and logs remain for audit.
- D24. Metadata changes do not automatically invalidate plans.
- D25. Explicit per-request regeneration exists.
- D26. Explicit regeneration retries current failed plans.
- D27. Regeneration is exposed through request-scoped API and `pipeline-cli`.
- D28. CLI inspection shows static plan data and execution statistics.
- D29. The initial cut is a hard cut-over, not feature-flagged.
- D30. Old `search_attempts` is not translated into cursor position.
- D31. Scheduling/backoff state remains separate from search cursor state.
- D32. The new executor does not write `exhausted`.
- D33. Plan wrap is represented by cursor reset plus cycle count increment.
- D34. Cycle count is enough; no synthetic cycle-complete search-log event is required.
- D35. Cursor advances only after a logged attempt.
- D36. Found/enqueued searches still advance the cursor.
- D37. Requests returning to wanted keep active plan, cursor, and cycle count.
- D38. Default repeats are encoded as separate plan slots.
- D39. Repeated default slots share query identity metadata for grouped auditing.
- D40. Accidental duplicate queries are collapsed by the generator.
- D41. Deduped alternatives are recorded in provenance.
- D42. Track tier is skipped for single-track albums.
- D43. Track tier is capped at three queries.
- D44. Track candidates are normalized and low-entropy-filtered before ranking.
- D45. Track selection ranks by useful token count, then character count, then source-track order.
- D46. The low-entropy token list starts with the current four tokens only.
- D47. Omitted track candidates are stored as provenance, not executable slots.
- D48. Search logs attach to exact plan context.
- D49. Search logs snapshot cycle count at execution time.
- D50. Audit views must support exact-slot and grouped-query questions.
- D51. Tests assert full generator output including provenance.
- D52. The design must be documented as a brainstorm artifact before planning/implementation.
- D53. Generation-affecting config changes require a generator-id bump; config fingerprinting is not an automatic invalidation mechanism in this version.
- D54. The generator id has one shared runtime source across service, CLI, and API.
- D55. Failed generation is classified: deterministic generator failures can be sticky, transient resolver/dependency failures are retryable.
- D56. Empty tracklists produce album-level-only plans rather than failed plans.
- D57. Add-time generation runs only after complete resolved metadata/track data is available.
- D58. Startup reconciliation scans all wanted requests, not only retry-eligible or paged searchable requests.
- D59. Search log insertion and cursor advance/wrap are one atomic consumed-attempt operation.
- D60. Stale completions from old active plans are auditable but cannot advance the current plan cursor.
- D61. Pre-attempt failures remain visible and backed off without consuming a plan slot.
- D62. Search usefulness is defined by rankable value/cost dimensions, not just by storing more rows.
- D63. Dashboard UI is deferred; dashboard-ready aggregate dimensions are in scope.
- D64. Per-search cache attribution is attached when available, but deeper cache instrumentation is not silently bundled into this rewrite.
- D65. The track-tier cap/reorder is an explicit search-behavior change that must be monitored after deploy.

---

## Dependencies / Assumptions

- Existing request metadata sources can provide the artist/title/year/tracklist snapshot needed by the generator for both MusicBrainz and Discogs requests.
- The current search ladder's default repeat count remains driven by existing configuration rather than hardcoded in the plan generator.
- Existing search-log telemetry can be extended to include plan context without losing current candidates, browse timing, match timing, and fanout fields.
- Per-search cache counters may require explicit persistence work if they are currently only accumulated at cycle level. The requirements distinguish dashboard readiness from pretending unavailable counters already exist.
- The future dashboard will expand request-level search state, so persisted plan data and CLI/API output should favor inspectability over minimal storage.
- Some old columns or fields may remain temporarily for compatibility, but they must no longer select search variants after cut-over.

---

## Outstanding Questions

### Resolve Before Planning

*(none — scope decisions resolved in dialogue)*

### Deferred to Planning

- [Affects R6-R13][Technical] Choose the exact persistence shape for search plans, plan items, failed generation records, and request cursor fields.
- [Affects R37-R42][Technical] Decide the startup reconciliation transaction boundaries and log formatting.
- [Affects R45-R50][Technical] Decide the exact CLI/API response format for plan inspection and regeneration.
- [Affects R54-R55][Technical] Decide the aggregation queries used to compute per-slot and per-query execution statistics.
- [Affects R66-R69][Technical] Decide the exact transaction/locking mechanism for atomic log+cursor advancement and stale-plan completion handling.
- [Affects R36, R56-R60][Technical] Decide the exact migration shape for legacy `search_attempts`, `next_retry_after`, and any future scheduling/backoff fields while preserving the requirement that they do not select query variants.
