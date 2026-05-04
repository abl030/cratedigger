---
title: "fix: Per-search progress watchdog and cycle-gate removal"
type: fix
status: completed
date: 2026-05-04
origin: docs/brainstorms/2026-05-04-search-watchdog-requirements.md
---

# fix: Per-search progress watchdog and cycle-gate removal

## Summary

Replace the cycle-entry gate `cycle_max_runtime_s` with a 90-second per-search wall-clock watchdog inside `_collect_search_results`. When the watchdog fires it calls slskd's stop endpoint (`PUT /api/v0/searches/{id}`), then lets the existing harvest-and-classify path run unchanged. Net effect: the 8h53m hang is bounded at ~90s, daily search throughput recovers toward the pre-#198 baseline, and no per-cycle wall-time gate remains.

---

## Problem Frame

Today's investigation showed `cycle_max_runtime_s=600` (PR #209) caused a 95% drop in daily search throughput while ostensibly defending against an 8h53m hang where slskd's own state-completion timer failed to fire. The hang is a per-interaction failure (one stuck slskd state poll) that was being addressed at the wrong layer (cycle entry). Detailed pain narrative + metrics in the origin requirements doc.

---

## Requirements

- R1. Each in-flight search has a 90-second **no-progress** deadline measured from the most recent peer-response arrival. A search that keeps receiving new responses keeps going; a search whose `responseCount` has not advanced for 90 seconds (and is still `InProgress` / `Queued`) trips the watchdog. *(see origin: docs/brainstorms/2026-05-04-search-watchdog-requirements.md R1; "Watchdog measures progress, not wall-time")*
- R2. When the deadline fires, cratedigger calls slskd's stop endpoint (`PUT /api/v0/searches/{id}`) on a best-effort basis — failures and 404s are logged and ignored.
- R3. After cancelling, the existing harvest path runs unchanged: `search_responses()` reads whatever peer responses slskd accumulated, and outcome classification proceeds normally.
- R4. The `search_log` outcome reflects what was harvested (`found` / `no_match` / `no_results` / `exhausted`) — the watchdog does not introduce a `timeout` outcome.
- R5. The request whose search was watchdog-cancelled receives no special treatment — same status, same `search_attempts` increment rules, same backoff as a slskd-completed search.
- R6. Per-cycle instrumentation records `cycle_searches_watchdog_killed=N` in the cycle-summary log line, replacing the removed `cycle_deadline_skipped` field.
- R7. Remove `cycle_max_runtime_s` from `lib/config.py`, the NixOS module's option set, the rendered `config.ini`, and all gate logic in `cratedigger.py`.
- R8. Remove `cycle_deadline_skipped` from cycle-summary output and from `CratediggerContext`.
- R9. No cycle-entry gate (no replacement for `cycle_max_runtime_s`). The watchdog from R1 is the in-band bound on stuck searches.
- R13. **Process-level safety net via systemd `RuntimeMaxSec=3600`** on `cratedigger.service`. Defense-in-depth against unknown failure modes that escape the watchdog (clock-injection bugs, TCP socket hangs blocking the watchdog itself, etc.). Never fires in healthy operation (cycles are empirically well under 60 min); SIGTERM-kills the process at 60 min wall-clock, the systemd timer fires the next cycle on schedule. Cycle-boundary checkpointing already tolerates a forced kill — the importer service owns beets writes independently and is unaffected.
- R10. Fan-out parameters unchanged (`browse_top_k`, `browse_global_max_workers`, wave-based fan-out). `search_max_inflight=4` unchanged.
- R11. Thoroughness settings unchanged (`search_response_limit=1000`, `search_file_limit=50000`, full ladder).
- R12. The 90-second deadline is hardcoded as a module-level constant — not a config knob exposed via `config.ini` or the NixOS module.

**Origin acceptance examples:** AE1 (covers R1–R5), AE2 (covers R2), AE3 (covers R6), AE4 (covers R7–R9). All are preserved as-is and used to anchor test scenarios in U1 / U3 below.

---

## Scope Boundaries

- **No `match_pool` refactor.** Browse-and-match is now <1% of cycle time per origin metrics — not the bottleneck.
- **No long-running daemon.** Soulseek.NET's `maximumConcurrentSearches=2` is the actual ceiling; daemon buys nothing.
- **No SignalR push subscription.** Out of scope; file as own follow-up issue.
- **No raise of `search_max_inflight` past 4.** Soulseek.NET would queue extras; verify this empirically in a separate spike if pursued at all.
- **No re-introduction of any rolled-back cap.** R6/R16 / 75s search-poll cap / cycle wall-time gate are all closed.
- **No watchdog timeout knob in config.** 90s is a constant. If empirical data later argues for 60s or 120s, that's a separate change.
- **No cycle-entry gate.** The watchdog is the in-band bound; `RuntimeMaxSec=3600` on the systemd unit is the out-of-band defense-in-depth (R13). What's deliberately excluded: any per-cycle, per-album, or per-search wall-clock cap that would discard work — those are the rolled-back class.

---

## Context & Research

### Relevant Code and Patterns

- **Search collection / poll loop:** `cratedigger.py:413-475` (`_collect_search_results`). Watchdog hooks into this function. Loop body at lines 434-444 polls `slskd_client.searches.state(id, False)` once per second; the watchdog adds a wall-clock check before `time.sleep(1)`.
- **Config field:** `lib/config.py:109` (`cycle_max_runtime_s: int = 600`) and `lib/config.py:283-287` (parsing). Both removed.
- **NixOS module option:** `nix/module.nix:162` (`cycle_max_runtime_s = ...`). Removed; the corresponding `cycleMaxRuntimeS` option declaration also removed.
- **Cycle-counter pattern:** `lib/context.py:67` declares `cycle_deadline_skipped: int = 0`; `cratedigger.py:925` resets at cycle start; `cratedigger.py:589, 662` increment; `cratedigger.py:987-988` formats via `lib/cycle_summary.py:format_cycle_summary`. New counter `cycle_searches_watchdog_killed` follows this exact pattern.
- **slskd_api wrapper for stop:** `slskd_api/apis/searches.py:99-107` — `searches.stop(id)` issues `PUT /api/v0/searches/{id}` and returns `bool`. The existing `searches.delete(id)` (lines 110-118) is a separate `DELETE` for record cleanup; not what we want for the watchdog.
- **Test fakes:** `tests/fakes.py:256-333` (`FakeSlskdSearches`) supports `state()`, `search_responses()`, `delete()`. Needs an additional `stop()` method matching the new wrapper. The existing `add_search(state="InProgress", responses=[...])` already supports stuck-state simulation — state will keep returning InProgress on every poll until the test changes the seeded state.
- **Existing search-collection tests:** `tests/test_*.py` — search collection has integration coverage in `tests/test_slskd_live.py::TestParallelSearchTiming` (gated behind `SLSKD_TEST_FULL=1`); orchestration tests live in the various `test_search_*.py` and `test_cratedigger_*.py` modules.

### Institutional Learnings

- **Issue #198 + this morning's investigation** — bounding cycle wall-time discards work rather than reducing it. The lesson is "bound the interaction, not the cycle." Already applied here.
- **Pre-#198 75s search-poll cap** — was correct in spirit but wrong in mechanism (fired on legitimately slow searches). The 90s wall-clock with cancel-and-harvest is the corrected version.
- **`CratediggerContext` cycle-counter convention** — counters are reset at cycle start in `cratedigger.py:920-925`, accumulated during the cycle, formatted by `lib/cycle_summary.py:format_cycle_summary`. Don't deviate from this pattern.

### External References

- **slskd source (verified 2026-05-04):** sub-agent investigation of `slskd/src/slskd/Search/API/...` confirmed `PUT /api/v0/searches/{id}` is the cancel endpoint, `DELETE` is record removal. Default `searchTimeout=15s` from-last-response. Findings written to `/tmp/slskd-research/FINDINGS.md` during the brainstorm.
- **`docs/slskd-internals.md`** — local durable reference, recently verified.

---

## Key Technical Decisions

- **Progress-based 90s from last response, not wall-clock from submission.** Per origin's Key Decisions ("Watchdog measures progress, not wall-time"), and verified during ce-doc-review: a wall-clock-from-submission watchdog would false-positive on slow-but-legit searches receiving steady responses for 60–90s (plausible at `search_response_limit=1000` / `search_file_limit=50000`). Progress-based fires only when the search has gone silent at the slskd level — exactly the 8h-hang failure mode. The implementation cost is one extra dict-read per poll iteration: `state_resp.get("responseCount", 0)` is in the existing `state(id, False)` response (verified against slskd source `Search.cs:55` — `ResponseCount` is on the Search DTO regardless of `includeResponses`). No new API call, no payload inflation, same 1Hz poll cadence.

- **Fire `searches.stop()`, not `searches.delete()`.** `stop()` (PUT) cancels server-side; `delete()` (DELETE) removes the record entirely. The existing post-collection `delete()` cleanup at line 450 still runs after harvest.

- **Post-cancel state-transition wait, then harvest.** Verified against slskd source (`SearchService.cs:340-361`): when `TryCancel` fires, slskd's `Task.Run` cleanup is async — `OperationCanceledException` propagates, state transitions to `Completed | Cancelled`, THEN `search.Responses = responses.Select(...)` runs and `Update(search)` persists. Until that cleanup completes, `search_responses(id)` returns the pre-cancel snapshot (potentially empty). We must wait for the state-transition signal before harvesting, or risk silently degrading every watchdog-fired search to `outcome=no_results`. After `stop()`, poll state at ~200ms intervals for up to 5 seconds; harvest when state shows `Completed` flag or when the post-cancel timeout elapses (whichever first). The 5s cap prevents a doubly-broken slskd from hanging us indefinitely; the 200ms inner cadence keeps end-to-end latency tight in the typical fast-cleanup case.

- **The 90s constant lives in `cratedigger.py` (or `lib/search.py`) as a module-level constant**, not in `config.ini`. Per origin R12 / scope-boundary "No watchdog timeout knob in config." If the value needs to change, it's a code-level edit + deploy, not a runtime tunable.

- **Inject a clock into `_collect_search_results` for testability.** The poll loop sleeps `time.sleep(1)` between iterations. To test 90s elapsed deterministically, the wall-clock check uses a `clock_fn=time.monotonic` parameter; tests pass a `FakeClock` that advances on demand. Production callers omit the parameter (defaults to `time.monotonic`).

- **Counter accumulation matches existing pattern.** `ctx.cycle_searches_watchdog_killed: int = 0` declared in `lib/context.py`, reset in the cycle prelude, incremented inside `_collect_search_results` when the watchdog fires (passed via the `ctx` already in scope, or returned in `SearchResult` and counted in `_merge_search_result`). Plan picks the simpler path during implementation; either is acceptable.

- **`outcome="timeout"` is gone forever.** No new outcome category is introduced. The `final_state` field of `SearchResult` (already present, captures the slskd state at break time) serves as the breadcrumb if a future analyst wants to distinguish watchdog-killed from natural-completion in the search_log.

- **Best-effort cancel.** `try/except` around `searches.stop(id)`; failure logs at info level and proceeds. Slskd 404s are not errors — the search may have just transitioned to Completed in between our last state poll and our cancel call.

- **`RuntimeMaxSec=3600` on cratedigger.service as defense-in-depth (R13).** Originally judged "extra work" during brainstorming; ce-doc-review surfaced that the cost is one line in `nix/module.nix` and the rollback history justifies belt-and-suspenders. The watchdog is the in-band bound; `RuntimeMaxSec` is the out-of-band safety net for failure modes the watchdog itself can't catch (clock-injection regression, TCP-hung `state()` blocking the watchdog's own deadline check, etc.). Never fires in healthy operation. The systemd timer's next fire continues the pipeline; importer service is independent and unaffected by a forced kill.

---

## Open Questions

### Resolved During Planning

- **Watchdog wall-clock vs progress-based** → progress-based, 90s of no-new-responses (no `responseCount` increase). Verified `responseCount` is in slskd's basic `state()` response — no extra API call. *(see Key Technical Decisions)*
- **Cancel API choice (PUT vs DELETE)** → `slskd_api.searches.stop()` (PUT). DELETE is wrong because it removes responses.
- **Counter accumulation site** → `CratediggerContext`, reset at cycle start, formatted by `format_cycle_summary`. *(see Relevant Code and Patterns / Key Technical Decisions)*
- **Watchdog as config knob?** → No. Module-level constant. *(R12 + scope boundary)*

### Deferred to Implementation

- **Where the counter is incremented** — inside `_collect_search_results` directly (passing `ctx` into it, currently it doesn't take `ctx`), or via a new field on `SearchResult` that `_merge_search_result` aggregates. Either is acceptable; pick the smaller diff at implementation time.
- **Where the 90s constant lives** — `cratedigger.py` near `_collect_search_results`, or a new `SEARCH_WATCHDOG_DEADLINE_S` in `lib/search.py`. Implementer's call.
- **Whether the cycle-summary format string changes** — `lib/cycle_summary.py` likely emits the field name as part of an f-string or kwargs dict; the implementer swaps `cycle_deadline_skipped` → `cycle_searches_watchdog_killed` in whatever shape exists there.
- **Existing test updates** — there are tests today that assert `cycle_max_runtime_s` parsing, gate behavior, or `cycle_deadline_skipped`. The implementer enumerates them via `grep -rn "cycle_max_runtime_s\|cycle_deadline_skipped" tests/` and updates / deletes as appropriate. Likely candidates: `tests/test_config.py`, `tests/test_cratedigger.py` (or wherever the gate is currently tested).

---

## High-Level Technical Design

> *This illustrates the intended approach and is directional guidance for review, not implementation specification. The implementing agent should treat it as context, not code to reproduce.*

```
_collect_search_results(search_id, query, album_id, search_cfg, slskd_client, variant_tag=None,
                        clock_fn=time.monotonic):
    final_state = None
    watchdog_fired = False
    prev_count = 0
    last_progress_at = clock_fn()

    while True:
        try:
            state_resp = slskd_client.searches.state(search_id, False)
            state = state_resp["state"]
            final_state = state
            count = state_resp.get("responseCount", 0)
            if count > prev_count:
                prev_count = count
                last_progress_at = clock_fn()
            if "Completed" in state or ("InProgress" not in state and "Queued" not in state):
                break
        except Exception:
            log warning; break

        if clock_fn() - last_progress_at >= 90.0:
            log info: "watchdog firing for search_id=<id> after 90s of no progress"
            try:
                slskd_client.searches.stop(search_id)
            except Exception:
                log info: "stop() failed; proceeding with harvest anyway"
            watchdog_fired = True
            # Post-cancel state-transition wait. slskd populates Search.Responses
            # in an async Task.Run cleanup AFTER the cancel propagates and state
            # transitions to Completed|Cancelled. Reading responses before that
            # cleanup runs returns an empty list. Bounded at 5s.
            cancel_deadline = clock_fn() + 5.0
            while clock_fn() < cancel_deadline:
                try:
                    state_resp = slskd_client.searches.state(search_id, False)
                    final_state = state_resp["state"]
                    if "Completed" in final_state:
                        break
                except Exception:
                    break
                time.sleep(0.2)
            break

        time.sleep(1)

    # --- existing harvest path runs UNCHANGED below ---
    search_results = slskd_client.searches.search_responses(search_id)
    ...
    return SearchResult(..., watchdog_fired=watchdog_fired)
```

The progress signal is `state_resp["responseCount"]` (camelCase JSON of slskd's `ResponseCount` int field on the Search DTO, populated regardless of `includeResponses`). Each time it advances, `last_progress_at` snaps to the current clock; the watchdog only fires when 90 seconds elapse without any new peer responses arriving.

The instrumentation hook is the new `watchdog_fired` flag on `SearchResult`. `_merge_search_result` (or whichever main-thread aggregation function consumes results) increments `ctx.cycle_searches_watchdog_killed` by 1 for each result with `watchdog_fired=True`.

Key invariant: the harvest path (`search_responses()` → outcome classification → `SearchResult` build) is byte-identical between watchdog-fired and slskd-completed branches. The only difference is `watchdog_fired=True` and a different `final_state` (likely `"InProgress"` instead of `"Completed"`), both of which are diagnostic only.

---

## Implementation Units

- U1. **Add per-search watchdog inside `_collect_search_results`**

**Goal:** Bound any single search's poll-loop wall-time at 90s. On expiry, call slskd's stop endpoint, mark the SearchResult as watchdog-fired, and let the existing harvest path run unchanged.

**Requirements:** R1, R2, R3, R4, R5, R12. Covers AE1, AE2.

**Dependencies:** None. Independent of U2.

**Files:**
- Modify: `cratedigger.py` (the `_collect_search_results` function around line 413)
- Modify: `lib/search.py` (add `watchdog_fired: bool = False` to `SearchResult`)
- Modify: `tests/fakes.py` (add `stop(id)` method to `FakeSlskdSearches` that records calls; extend `add_search` so tests can specify a post-cancel state-transition delay and post-cancel responses, simulating slskd's async cleanup window)
- Test: `tests/test_search_watchdog.py` (new file) — unit tests for the watchdog primitive
- Test: `tests/test_integration_slices.py` (new test class `TestSearchWatchdogSlice` exercising the full `_collect_search_results` path)

**Approach:**
- Add a 90-second no-progress deadline tracked via `state_resp["responseCount"]`. Each iteration: read current count; if greater than previous, snap `last_progress_at` to `clock_fn()`. Fire watchdog when `clock_fn() - last_progress_at >= 90.0` and the search is still in-flight.
- Inject `clock_fn=time.monotonic` parameter (defaults to `time.monotonic`); tests pass a deterministic fake.
- Wrap `slskd.searches.stop(search_id)` in `try/except` — log info on failure, never raise.
- **Post-cancel state-transition wait.** After `stop()`, poll `state()` at 200ms intervals for up to 5 seconds, breaking when `state` shows the `Completed` flag (slskd has finished its async response-persistence cleanup) or the 5s budget expires. Then run the existing harvest path. This closes the race where `search_responses(id)` returns empty because slskd hadn't yet flushed `responses` into the DB record.
- Add `watchdog_fired: bool = False` field on `SearchResult` (dataclass) so downstream aggregation can count.
- Module-level constants in `cratedigger.py` (or `lib/search.py` — implementer's call): `SEARCH_WATCHDOG_DEADLINE_S = 90.0`, `SEARCH_CANCEL_WAIT_DEADLINE_S = 5.0`, `SEARCH_CANCEL_WAIT_POLL_S = 0.2`.
- Poll cadence stays at 1Hz for the main progress loop; the post-cancel inner wait uses 5Hz for tight latency. `responseCount` is ambient in the existing `state(id, False)` response, so no extra API call; one int comparison per main-loop iteration.

**Execution note:** Test-first. Write the unit test that asserts watchdog fires at 90s with a stuck FakeSlskdSearches; watch it fail; then implement.

**Patterns to follow:**
- The existing poll loop structure at `cratedigger.py:434-444` — mirror the `try/except` shape and the `time.sleep(1)` rhythm.
- `SearchResult` dataclass shape in `lib/search.py` — add the new bool field alongside existing optionals.
- `FakeSlskdSearches.delete()` at `tests/fakes.py:331-332` — model `stop()` the same way (just record the call).

**Test scenarios:**
- Happy path — search completes normally: state transitions to `Completed` at iteration N with steadily-rising `responseCount`. Expected: poll exits via state-transition, `watchdog_fired=False`, no `stop()` call, normal `outcome`.
- Watchdog fire (no responses ever) — `state="InProgress"` indefinitely, `responseCount=0` forever. **Covers AE1 (the 8h-hang case).** Fake clock advances 90s. Expected: `stop()` called once, `watchdog_fired=True`, `outcome="no_results"` (empty harvest).
- Watchdog fire (responses then silence) — `state="InProgress"`, `responseCount` rises 0→47 over the first 10 simulated seconds, then stops at 47. **Covers AE1 (the harvest-partial case).** Fake clock advances another 90s with no further increase. Expected: watchdog fires at simulated t≈100s (10s of progress + 90s silence), `stop()` called, `watchdog_fired=True`, harvest returns the 47 responses, `outcome` reflects what the 47 justify (NOT `"timeout"`).
- **Slow-but-legit search does NOT trigger watchdog** (regression guard for the wall-clock false-positive class) — `state="InProgress"` for 120 simulated seconds, with `responseCount` rising by 1 every 5 simulated seconds throughout. Expected: watchdog never fires (each new response resets the no-progress timer); when state finally flips to `Completed`, `watchdog_fired=False`, `stop()` never called.
- **Post-cancel race — responses persisted in time** (regression guard for the silent-no_results-degrade failure mode): FakeSlskdSearches seeded with `state="InProgress"` and 23 pending responses. When `stop()` is called, the fake transitions to `state="Completed | Cancelled"` after 1 simulated second AND populates `search_responses` at the same simulated tick. Expected: post-cancel wait loop spins ~5 iterations (1s ≈ 5×200ms), state hits Completed, harvest returns the 23 responses, `outcome` reflects them (not `"no_results"`).
- **Post-cancel race — slskd hung at cleanup** (5s timeout escape): FakeSlskdSearches seeded such that `stop()` is called but state never transitions out of `InProgress` (slskd's cleanup itself is hung). Expected: post-cancel wait loop hits its 5s budget; harvest runs anyway with whatever responses are visible (could be empty); `watchdog_fired=True`; no exception.
- Watchdog fire + stop() raises: `stop()` configured to raise `Exception("network error")`. **Covers AE2.** Expected: watchdog still breaks loop, exception caught and logged at info level, harvest path runs, `watchdog_fired=True`.
- Watchdog fire + stop() returns False (404): same. Expected: same as happy watchdog-fire path. (R2: 404 logged and ignored.)
- Completion-vs-watchdog ordering: `responseCount=10` for 89s without further increases, then on the 90s poll state flips to `Completed`. Expected: state-transition exit wins, `watchdog_fired=False`, `stop()` never called. **Pin the ordering invariant**: state-transition check happens BEFORE the deadline check inside the loop body.
- Edge case — `state()` raises: deadline-eligible iteration where `state()` raises. Expected: existing exception handler at lines 441-443 catches and breaks; the deadline check doesn't run for that iteration. Existing behavior preserved.

**Verification:**
- New unit test file passes.
- Integration slice test (in `test_integration_slices.py`) exercises `_collect_search_results` end-to-end against `FakeSlskdAPI` with stuck search and produces a `SearchResult` whose `watchdog_fired=True`, `outcome` is a real outcome (not `timeout`), and `stop()` was called once.
- Pyright clean on `cratedigger.py`, `lib/search.py`, `tests/fakes.py`.

---

- U2. **Wire `cycle_searches_watchdog_killed` instrumentation; remove `cycle_deadline_skipped`**

**Goal:** Surface the watchdog firing rate via the cycle-summary log line. Remove the now-dead `cycle_deadline_skipped` accumulator.

**Requirements:** R6, R8. Covers AE3.

**Dependencies:** U1 (the `watchdog_fired` flag on `SearchResult` must exist).

**Files:**
- Modify: `lib/context.py` (remove `cycle_deadline_skipped: int = 0` at line 67; add `cycle_searches_watchdog_killed: int = 0`)
- Modify: `cratedigger.py` (cycle-prelude reset at ~line 925: drop `cycle_deadline_skipped = 0`, add `cycle_searches_watchdog_killed = 0`; aggregator increment site — likely in `_merge_search_result` or wherever results are consumed)
- Modify: `lib/cycle_summary.py` (replace the `cycle_deadline_skipped=` kwarg in `format_cycle_summary` with `cycle_searches_watchdog_killed=`)
- Test: `tests/test_cycle_summary.py` (or wherever `format_cycle_summary` is currently tested) — update format-string assertions
- Test: existing tests that reference `cycle_deadline_skipped` — update to the new field name where they're still relevant; delete where they were testing the removed gate behavior

**Approach:**
- One-for-one rename + relocation of the counter. The increment site is the substantive change: each completed `SearchResult` with `watchdog_fired=True` bumps the cycle counter by 1.
- Cycle-summary log line gets the new field; existing callers/parsers (operators reading journalctl) must be told the field renamed — call out in the PR description.

**Patterns to follow:**
- The exact pattern `cycle_deadline_skipped` followed: declare on context, reset in cycle prelude, increment at the relevant boundary, format in summary line.

**Test scenarios:**
- Cycle with 0 watchdog fires: counter is 0, summary line shows `cycle_searches_watchdog_killed=0`. **Covers AE3 (the n=0 case).**
- Cycle with 3 watchdog fires: 3 in-flight searches all hit the watchdog; counter is 3, summary line shows `cycle_searches_watchdog_killed=3`. **Covers AE3 (the n=3 case).**
- Mixed cycle: 5 searches, 1 watchdog'd, 4 completed normally. Counter is 1.
- Reset behavior: counter is 0 at cycle start regardless of prior cycle's value. (Covers the cycle-prelude reset; mirrors how `cycle_deadline_skipped` was tested.)

**Verification:**
- Cycle-summary integration test produces a log line containing `cycle_searches_watchdog_killed=N` and no `cycle_deadline_skipped=` substring.
- Pyright clean on touched files.

---

- U3. **Remove `cycle_max_runtime_s` config, NixOS option, gate logic; add `RuntimeMaxSec=3600` defense-in-depth**

**Goal:** Tear out the cycle-entry gate and all associated infrastructure. Replace it at a different layer with a process-level safety net (`RuntimeMaxSec=3600` on the systemd unit) — defense-in-depth against unknown failure modes that escape the watchdog from U1.

**Requirements:** R7, R9, R13. Covers AE4.

**Dependencies:** U1 ships first (so the watchdog is providing in-band protection before the cycle gate is removed). U2 can land before, after, or alongside U3.

**Files:**
- Modify: `lib/config.py` (remove the `cycle_max_runtime_s: int = 600` field at line 109 and its parsing at line 283-287; remove the comment about "0 / negative = opt-out")
- Modify: `nix/module.nix` (remove the `cycleMaxRuntimeS` option declaration and the `cycle_max_runtime_s = ...` line at 162; remove from `searchSettings` option block; **add `RuntimeMaxSec = "1h";` to the cratedigger.service `serviceConfig` block** alongside the existing `restartIfChanged = false`)
- Modify: `cratedigger.py` (remove the gate-firing log line at ~589 and ~592; remove the gate check around line 626 and ~662; remove ~line 920 reference; remove ~line 925 reset of `cycle_deadline_skipped` if not already done in U2)
- Test: `tests/test_config.py` — remove tests asserting `cycle_max_runtime_s` parsing
- Test: any other test asserting gate behavior — remove
- Test: VM check (`nix build .#checks.x86_64-linux.moduleVm`) confirms the rendered systemd unit includes `RuntimeMaxSec=3600` (or `1h`)

**Approach:**
- Mechanical deletion. `grep -rn "cycle_max_runtime_s\|cycleMaxRuntimeS" .` enumerates the call sites; remove every one.
- The NixOS `moduleVm` check (`nix build .#checks.x86_64-linux.moduleVm`) catches NixOS-side typos.
- After deletion, run the full test suite and confirm pyright clean.

**Execution note:** Mechanical refactor, not test-first.

**Patterns to follow:**
- Removing a config field: just delete from the dataclass and `from_ini`; no explicit migration needed (rendered `config.ini` regenerates on deploy).
- NixOS option removal: delete from `mkOption` block + the rendering. Existing deployments with the old `searchSettings.cycleMaxRuntimeS = 600` line in their config will get an "unknown option" eval error — flag this in the PR / deploy notes so the downstream `~/nixosconfig/modules/nixos/services/cratedigger.nix` can drop its setting if any.

**Test scenarios:**
- Config test: `from_ini` succeeds with a config.ini that has no `cycle_max_runtime_s` entry. **Covers AE4 (config presence).**
- Config test: `from_ini` ignores or errors cleanly if `cycle_max_runtime_s` is still present (trailing config from before the deploy) — probably ignored is fine since `getint` falls back to a default that no longer exists. **Implementer should verify this doesn't crash; an unknown ini key isn't generally fatal in our config.**
- Module test: `nix build .#checks.x86_64-linux.moduleVm` passes after the option is removed AND with `RuntimeMaxSec=1h` added. **Covers AE4 (module surface) + R13.**
- Module test: rendered systemd unit file (via `cat /etc/systemd/system/cratedigger.service` in VM check) contains `RuntimeMaxSec=` set to 1h or 3600. **Covers R13.**
- Cycle test: a full cycle that previously would have tripped the gate (long-running) now runs to completion without log message about deadline-skipping.

**Verification:**
- `grep -rn "cycle_max_runtime_s\|cycleMaxRuntimeS\|cycle_deadline_skipped" lib/ nix/ cratedigger.py tests/` returns no matches.
- Full test suite green.
- VM check green.

---

## System-Wide Impact

- **Interaction graph:** `_collect_search_results` is called from `_search_and_queue_parallel` (line 720) inside a `ThreadPoolExecutor`. The watchdog fires on a worker thread. The `searches.stop()` call is a synchronous HTTP request from that worker thread; doesn't interact with other workers' searches.
- **Error propagation:** If `searches.stop()` raises, U1's `try/except` swallows it. Downstream code must not assume `stop()` was successful. The existing `searches.delete()` cleanup at line 450 still runs unchanged on the watchdog-fired path; if `delete_searches=True` and the search was already stopped, the delete is a no-op or a 404 — already handled by existing exception flow there.
- **State lifecycle risks:** The watchdog cancels server-side via PUT but does not delete the record. The existing post-collection delete (line 450) handles cleanup. If `delete_searches=False` (operators run with this off for debugging), the search record remains in slskd's storage with state changed by the stop call — harmless, just visible.
- **API surface parity:** `SearchResult.watchdog_fired` is a new field; consumers must be ready for `False` default. `_merge_search_result` reads from `SearchResult` already; just needs the new field handled.
- **Integration coverage:** The end-to-end slice (parallel-search executor → collect → harvest → outcome → log_search) needs at least one test exercising the watchdog path so we know the integration seam holds. Covered by U1's integration slice test.
- **Unchanged invariants:** `search_max_inflight=4`, the streaming submission pattern, `search_response_limit=1000`, `search_file_limit=50000`, the variant ladder, browse fan-out (`browse_top_k`, etc.), and the entire match/spectral/import path are unchanged. The new field on `SearchResult` is the only API-shape change.

---

## Risks & Dependencies

| Risk | Mitigation |
|---|---|
| Watchdog fires falsely on a legitimately slow search. | Closed by switching to progress-based: a search receiving any new response within the last 90s keeps going. Wall-clock 90s would have false-positived; progress-based only fires when the response stream has actually gone silent — which is the failure mode we're targeting. Regression-guarded by the explicit "slow-but-legit" test scenario in U1. |
| `searches.stop()` failure leaves slskd in an undefined state (search neither cancelled nor cleaned up). | Best-effort policy; existing `delete_searches` cleanup at line 450 still runs. Soulseek.NET's max=2 slot will free at most when the search hits its own internal completion path. Worst case: one slot occupied for the full hung duration on slskd's side. The watchdog still freed our local slot, so we keep submitting from cratedigger's side. |
| Post-cancel race silently zeroes harvest (slskd's response-persistence cleanup is async; reading too early returns empty). | Verified against slskd source (`SearchService.cs:340-361`). Mitigated by the post-cancel state-transition wait (5s budget at 200ms cadence) before harvest. Regression-guarded by the "post-cancel race — responses persisted in time" test scenario in U1. |
| Removing `cycle_max_runtime_s` from the NixOS module breaks downstream deployments that explicitly set `searchSettings.cycleMaxRuntimeS = 600`. | Coordinate with `~/nixosconfig/modules/nixos/services/cratedigger.nix` on doc1 — drop the override before flake-bumping cratedigger-src. Fold into the deploy step. |
| Test infrastructure: `time.monotonic` mocking. | Inject `clock_fn` parameter (default `time.monotonic`) into `_collect_search_results`. Tests pass `FakeClock`. Production callers omit the parameter. |
| Existing tests reference `cycle_max_runtime_s` or `cycle_deadline_skipped`; missed updates fail CI. | U3's verification step is `grep -rn` to enumerate all references and confirm no remaining matches before commit. |
| Watchdog has a bug we don't catch (clock-injection regression, TCP-hung `state()` blocking the deadline check itself). | `RuntimeMaxSec=3600` (R13, U3) catches this class — kernel SIGTERMs the process at 60 min, systemd timer fires the next cycle. Watchdog is in-band; `RuntimeMaxSec` is out-of-band defense-in-depth. |

---

## Documentation / Operational Notes

- Update `docs/slskd-internals.md` with a one-line note: "cratedigger consumes `searches.stop()` (PUT) for its watchdog; `searches.delete()` (DELETE) for post-collection cleanup."
- Deploy step: the `~/nixosconfig/modules/nixos/services/cratedigger.nix` downstream wrapper may have an explicit `cycleMaxRuntimeS = 600`. Drop it before the flake bump to avoid the eval error. Confirm via `ssh doc1 'grep -rn cycleMaxRuntimeS ~/nixosconfig'`.
- Post-deploy verification: watch `cycle_searches_watchdog_killed=N` in journalctl. Healthy steady-state expected: 0–1 per cycle. > 3 sustained warrants investigation.
- The PR should reference issue #198 for context — this fix closes the throughput regression that PR #209 introduced.

---

## Sources & References

- **Origin document:** [docs/brainstorms/2026-05-04-search-watchdog-requirements.md](docs/brainstorms/2026-05-04-search-watchdog-requirements.md)
- **Related issue:** #198 (Long cycle outliers: speed up the browse phase)
- **Predecessor PRs:** #208 (U2/U3/U4 fan-out), #209 (cycle_max_runtime_s gate — being undone here)
- **Predecessor brainstorm:** [docs/brainstorms/browse-fanout-and-pipeline-depth-requirements.md](docs/brainstorms/browse-fanout-and-pipeline-depth-requirements.md) — the May 2 rollback comment under "The trade-off we now own again" is the load-bearing precedent
- **slskd source:** `slskd/src/slskd/Search/API/...` — `searchTimeout=15s` default, `PUT /api/v0/searches/{id}` is cancel, `DELETE` is record removal. Findings in `/tmp/slskd-research/FINDINGS.md` (created during today's brainstorm).
- **slskd_api Python wrapper:** `slskd_api/apis/searches.py` — `stop(id)` method wraps PUT; `delete(id)` wraps DELETE.
- **Local slskd reference:** [docs/slskd-internals.md](docs/slskd-internals.md), [docs/parallel-search.md](docs/parallel-search.md)
