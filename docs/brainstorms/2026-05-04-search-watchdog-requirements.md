---
date: 2026-05-04
topic: search-watchdog-and-cycle-gate-removal
---

# Per-Search Progress Watchdog and Cycle-Gate Removal

## Summary

Replace the `cycle_max_runtime_s` cycle-entry gate with a per-search progress watchdog. When a search hangs (slskd's own 15s state-completion timer fails to fire), the watchdog cancels it server-side at 90s and harvests whatever responses arrived as if the search had completed normally. Cycle architecture, fan-out parallelism, and existing thoroughness settings stay; only the gate moves.

---

## Problem Frame

Issue #198 set out to eliminate 27–70-minute cycle outliers caused by serial browse fan-out in `try_enqueue`. Three sub-PRs (U2/U3/U4) shipped a wave-based browse fan-out that successfully reduced median browse time from 171s to ~8s, but each successive attempt to bound cycle wall-time has paid a throughput cost:

- The first attempt (R6/R16 — `browse_wave_deadline_s` + `browse_cycle_budget_s`) starved the pipeline within 24h: search `found` rate fell from 13.7% to 2.2%, timeout rate jumped to 35%, zero imports landed in the 19h preceding rollback.
- After rolling those caps back, an 8h53m cycle hang was observed when slskd's own internal 15s search timeout failed to fire and the search-state poll loop had no other floor.
- The cure (PR #209, `cycle_max_runtime_s = 600`) gates new work at cycle entry. Today's measurements show this gate has caused a 95% reduction in daily search throughput: 3,500–4,200 searches/day pre-#198 → 99–198 today; 700–1,000 distinct requests touched/day → 94–175 today; ~250 cycles/day → ~30. The wanted queue is now searched roughly every 4–7 days instead of daily.

The pattern across both attempts is the same: the original outliers were cycles still doing useful work that needed to drain. Bounding cycle wall-time discards work rather than reducing it. The user's stated goal is not bounded cycles — it is "more searches per day, and searches that are thorough."

The single failure mode the cycle gate was actually catching — slskd never marking a search Completed — is a per-interaction problem (one stuck slskd state poll), not a per-cycle problem. The right bound applies at that interaction.

---

## Requirements

**Watchdog**
- R1. Each in-flight search has a 90-second progress deadline measured from the moment it is submitted to slskd.
- R2. When the deadline fires before slskd has marked the search `Completed`, cratedigger calls slskd's per-search cancel API (`PUT /api/v0/searches/{id}`) on a best-effort basis. A failure or 404 from the cancel call is logged and ignored; cratedigger does not retry the cancel.
- R3. After cancelling, cratedigger collects whatever peer responses slskd has gathered up to that point and processes them through the normal completion path (browse fan-out + match + outcome classification) — the watchdog is a soft completion, not an abort.
- R4. The resulting `search_log` outcome is whatever the harvested results justify (`found`, `no_match`, `no_results`, `exhausted`) — never `timeout`. The watchdog does not introduce a new outcome category.
- R5. The request whose search was watchdog-cancelled is not penalised in any way: no backoff, no retry-skip, no special status. The next cycle treats it identically to any request whose search completed via slskd's own machinery.
- R6. Per-cycle instrumentation records how many searches the watchdog cancelled (`cycle_searches_watchdog_killed=N` in the cycle-summary log line) so we can observe whether slskd's own 15s completion timer is misbehaving systemically vs once-in-a-blue-moon.

**Cycle-gate removal**
- R7. `cycle_max_runtime_s` is removed: deleted from `lib/config.py`, removed from the NixOS module's `cratedigger` option set, removed from the rendered `config.ini`, and the gate logic deleted from the search-phase loop.
- R8. The corresponding `cycle_deadline_skipped` instrumentation field is removed from the cycle-summary line.
- R9. No replacement gate is added at the cycle, process, or service level. No `RuntimeMaxSec` on the systemd unit. No fall-back wall-clock cap.

**Preserve existing posture**
- R10. The wave-based browse fan-out (`browse_top_k`, `browse_global_max_workers`, `_fanout_browse_users`) is unchanged.
- R11. `search_max_inflight=4` is unchanged. The streaming-slot submission pattern (each in-flight slot independently runs submit → poll → collect → browse → match, immediately picking up the next album when its slot frees) is unchanged.
- R12. `search_response_limit=1000` and `search_file_limit=50000` are unchanged. Full search ladder (default → unwild → v4_tracks_0 → v4_tracks_1 → exhausted) is unchanged.

---

## Acceptance Examples

- AE1. **Covers R1–R5.** Given a search submitted to slskd that has gathered 47 peer responses and is still polling at second 90, when the watchdog fires, then cratedigger calls slskd's cancel API, processes the 47 responses through the normal browse-and-match path, writes a `search_log` row with the appropriate outcome (e.g. `found` or `no_match`), and the request remains in the same status it would have been in if slskd had marked the search Completed itself. The next cycle searches it again on its normal cadence.

- AE2. **Covers R2.** Given a search that the watchdog has decided to cancel, when the `PUT /api/v0/searches/{id}` call returns 404 or 5xx, then cratedigger logs the failure at info level and proceeds with R3 unchanged. The cancel failure does not cascade into a broader error.

- AE3. **Covers R6.** Given a cycle in which 0 watchdogs fired and a cycle in which 3 fired, the cycle-summary log lines must distinguish them via `cycle_searches_watchdog_killed=0` vs `cycle_searches_watchdog_killed=3`.

- AE4. **Covers R7–R9.** Given the deployment of this change, the `cycle_max_runtime_s` config value disappears from `/var/lib/cratedigger/config.ini`, no `cycle_deadline_skipped` field appears in cycle-summary log lines, and no new wall-time gate is observable in code or systemd.

---

## Success Criteria

- **Search throughput recovers to within reach of the pre-#198 baseline.** Within 7 days of deploy, daily search count is ≥ 3,000 (target: 3,500+), and distinct requests touched per day is ≥ 600 (target: 700+).
- **The 8h-hang failure mode is bounded at ~90s per stuck search.** No cycle should spend more than ~90s waiting on any single hung slskd search; cycle wall-time is now bounded only by the longest legitimate operation, which empirically is well under 60 minutes.
- **Watchdog firing rate is observable and low-frequency.** `cycle_searches_watchdog_killed` rolls up to a daily sum that's a small fraction (target: <5%) of total searches. If it climbs above ~10% sustained, that's a signal that slskd's own state machinery is broken in a new way and warrants its own investigation.
- **No regression in match-or-import quality.** Match rate per search stays within ±2% of pre-deploy steady state. Daily import rate returns to its pre-#198 1–15/day band once the queue is being searched at the previous cadence.
- **A downstream agent can implement R1–R12 without inventing product behavior.** Specifically: the trigger condition for the watchdog (R1), the cancel mechanism (R2), the harvest-and-process semantics (R3), the outcome classification (R4), the request-state guarantee (R5), and the instrumentation field (R6) are concrete enough to plan against without further product input.

---

## Scope Boundaries

- **Match-pool refactor** (issue #198 fix-list option (b) — separate executor for browse-and-match decoupled from search submission). Browse-and-match is now <1% of cycle wall-time. The pool refactor solves a problem that no longer exists.
- **Long-running daemon architecture.** Soulseek.NET's `maximumConcurrentSearches=2` is the actual ceiling on search execution; a daemon does not raise that ceiling. The cycle-based + systemd-timer model keeps its operational benefits (clean restart on deploy, bounded memory, cycle-scoped logs) without paying a throughput cost.
- **SignalR push-subscription replacement for state polling.** Slskd exposes a SignalR hub that broadcasts state changes — a strictly better alternative to polling for the long term. Out of scope here; file as a follow-up issue.
- **Raising `search_max_inflight` past 4.** Soulseek.NET would queue any extra in-flight searches internally without running them concurrently. Not worth pursuing without measurement first.
- **Re-introducing any rolled-back cap** (R6/R16 from the predecessor brainstorm, the 75s search-poll cap, or any other client-side wall-clock guard above the per-search level). The class of bug is closed.
- **Tuning the watchdog timeout.** 90s is the chosen default and is not a knob exposed via config. If empirical data later suggests 60s or 120s is better, that is a separate change.
- **Per-variant or per-request watchdog tuning.** Same 90s applies to every search regardless of ladder variant.

---

## Key Decisions

- **Watchdog measures progress, not wall-time.** The 90s window is "no transition past `InProgress` for 90s," not "90s since submission." A search that's still receiving responses keeps going; one whose state has gone silent fires the watchdog. This avoids the false-positive class that killed the rolled-back 75s cap (firing on legitimately slow searches).

- **Watchdog timeout is 90s.** Slskd's own `searchTimeout` default is 15s (from-last-response). 90s is 6× that — generous enough that a healthy slskd will always self-complete first, tight enough to prevent multi-hour hangs.

- **No backoff for watchdog-cancelled requests.** A cancelled search likely returned partial results that we processed. The request's subsequent search history matches a normally-completed one. Treating it differently would introduce special-case state that buys nothing.

- **No outcome=timeout category.** The watchdog harvests partial responses as a normal completion. The `outcome` field of `search_log` reflects what those responses justify (`found`, `no_match`, etc). This is a deliberate departure from the rolled-back 75s cap, which marked outcome=timeout and discarded results.

- **Cancel is best-effort.** The slskd cancel API may fail; we don't care. Slskd's internal state cleanup is its own concern; our pipeline only needs to stop polling.

- **Streaming submission with `search_max_inflight=4` is the right pattern, not wave-based.** Each slot independently runs submit→collect→browse→match and picks up the next album the instant its current one finishes. Already implemented and confirmed correct.

- **No process-level safety net.** The user explicitly judged a `RuntimeMaxSec` or equivalent fallback as "extra work hey." The watchdog is the only bound. If a future failure mode escapes it, that's a new bug to investigate, not pre-emptive defense.

---

## Dependencies / Assumptions

- Slskd's `PUT /api/v0/searches/{id}` cancel endpoint exists and frees the search's state for the consumer (verified via slskd source — `SearchesController` exposes the route; cancellation transitions the search out of `InProgress`).

- Slskd returns useful partial results when polled before its own completion timer fires (i.e., the in-memory `responses: List<SearchResponse>` accumulator is observable via the existing `GET /api/v0/searches/{id}/responses` endpoint at any time, not only post-completion). This is the load-bearing assumption for R3.

- The 8h53m hang observed on 2026-05-03 is representative of the failure mode — i.e., it is a real recurring failure of slskd's internal completion timer, not a one-off triggered by a state we cannot reproduce. If the failure mode is actually rarer than we assume, this work is still net positive (the watchdog adds <1% overhead on healthy cycles); if it is more frequent, the watchdog instrumentation will tell us.

- Cratedigger's existing search-collection thread already polls slskd state in a way the watchdog can hook into without major restructuring. (Implementation detail deferred to planning.)

---

## Outstanding Questions

### Resolve Before Planning

*(none — all scope decisions resolved in dialogue)*

### Deferred to Planning

- [Affects R1, R3][Technical] How exactly the watchdog reads slskd state: polling `GET /api/v0/searches/{id}` for state transitions, or watching for new entries in `GET /api/v0/searches/{id}/responses`, or both? The product requirement is "fire if no new responses for 90s"; the exact API surface is a planning concern.

- [Affects R2][Technical] Whether the cancel `PUT` is fired from the same thread that owns the in-flight search or from a separate watchdog thread, and the locking story between the two when both might attempt to read final state.

- [Affects R6][Technical] Where the `cycle_searches_watchdog_killed` counter accumulates (per-cycle context, separate state, etc.) and how the cycle-summary log line is updated to include it.

- [Affects R12][Needs research] Whether raising `search_max_inflight` from 4 to 6 or 8 measurably improves throughput in practice. Soulseek.NET's max=2 caps active searches, but more in-flight slots could keep its internal queue filled tighter. Out of scope for this brainstorm but worth a follow-up measurement.
