---
title: "fix: Harden download ownership preclaim recovery"
type: fix
status: active
date: 2026-05-05
origin: docs/plans/2026-05-05-003-fix-download-ownership-persistence-plan.md
---

# fix: Harden download ownership preclaim recovery

## Summary

Close the remaining review findings in the download ownership preclaim work by making every reset prove that slskd accepted no work, fencing poll recovery to the current claim, canonicalizing the initial state shape, and adding the missing contract coverage around the new DB and transition seams.

---

## Problem Frame

The first implementation moves ownership before slskd enqueue, but code review found that some reset and recovery paths are still optimistic. A false/rejected enqueue result, same-cycle poll overlap, stale terminal slskd snapshots, or unverified cancellation can still clear ownership while transfers may exist.

---

## Requirements

- R1. A claimed request may reset from `downloading` to `wanted` only after a fresh slskd snapshot proves every planned transfer is absent, or after accepted transfers are verifiably cancelled.
- R2. Ambiguous, stale, failed-snapshot, failed-cancel, same-cycle, and unverified outcomes must leave the request `downloading` with enough planned state for poll recovery.
- R3. Poll recovery must not attach a new ownership claim to slskd terminal transfers older than the persisted `enqueued_at`.
- R4. Initial planned `active_download_state` must use the canonical shape, including `current_path: null` until local processing starts, while readers remain compatible with legacy missing keys.
- R5. Verified no-acceptance resets must use retry/backoff accounting through the transition seam.
- R6. New status-mutating DB helpers and fake DB stubs must be covered by direct contract tests and the direct-status-write guard.

---

## Scope Boundaries

- Do not introduce a new request status or a general orphan-reconciliation tool.
- Do not redesign slskd polling or retry re-enqueue semantics beyond the current ownership claim.
- Do not broaden worker DB access beyond the existing narrow ownership writer.
- Do not make CLI/operator UX changes in this pass; structured operator evidence can follow after the safety fixes land.

---

## Context & Research

### Relevant Code and Patterns

- `lib/enqueue.py` owns the preclaim, single-disc enqueue, multi-disc enqueue, reset, and partial-failure recovery branches.
- `lib/download.py` owns `cancel_and_delete`, `rederive_transfer_ids`, and `poll_active_downloads` resume behavior.
- `lib/quality.py` owns `ActiveDownloadState.to_json()` and legacy-compatible parsing.
- `lib/download_ownership.py` is the narrow worker-safe writer that routes status changes through `lib.transitions`.
- `lib/pipeline_db.py`, `tests/fakes.py`, `tests/test_pipeline_db.py`, `tests/test_fakes.py`, and `tests/test_request_finalization.py` are the DB seam contract surface.

### Institutional Learnings

- `docs/advisory-locks.md` establishes the witness-before-side-effect pattern: retry only after the DB can distinguish safe retry from unsafe duplicate work.
- `docs/solutions/testing/mocked-contract-tests-miss-helper-mirror-integration-bugs.md` warns that mocked helper tests miss cross-boundary behavior; this fix needs at least one fresh-context poll/restart slice.

### External References

- None. This is local pipeline reliability work.

---

## Key Technical Decisions

- Treat `SlskdEnqueueOutcome(status="rejected")` as "not accepted by the direct call", not as durable proof that no transfer exists. The reset path must independently prove absence using the same username/filename planned state.
- Use the persisted `enqueued_at` as the claim boundary. Poll and recovery helpers may ignore stale terminal records older than that boundary when they are deciding whether the current claim owns a transfer.
- Keep `current_path` explicit in the JSON payload with `null`. This separates canonical planned rows from legacy missing-key rows while preserving reader compatibility.
- Verified no-acceptance remains a retryable download attempt. It should increment download attempts and set backoff just like other automatic `downloading -> wanted` paths.

---

## Open Questions

### Resolved During Planning

- Should rejected enqueue reset immediately? No. Reset only after a snapshot proves absence.
- Should poll use the timestamp fence globally? Yes, for rows reconstructed from persisted active state so the current claim cannot bind to old terminal transfers.
- Should cancellation success rely on the absence of exceptions? No. A falsey cancel result or a still-present transfer leaves ownership in place.

### Deferred to Implementation

- Exact helper names for verified absence and post-cancel verification.
- Whether the same-cycle poll guard uses a small age grace or the same timestamp-fenced absence proof. The implementation should pick the least invasive option that prevents fresh preclaim resets.

---

## Implementation Units

- U1. **Prove no-acceptance before reset**

**Goal:** Replace direct rejected-outcome resets with verified absence checks for single-disc and multi-disc paths.

**Requirements:** R1, R2

**Dependencies:** None

**Files:**
- Modify: `lib/enqueue.py`
- Test: `tests/test_enqueue_fanout.py`

**Approach:**
- Add a helper that re-derives planned transfer IDs from a fresh bulk snapshot using `claim.enqueued_at`.
- Reset to `wanted` only when the snapshot succeeds and no planned transfer is present.
- On snapshot failure or any matching transfer, persist/keep the planned state and return the owned downloads for poll recovery.
- Use the helper for single-disc rejected outcomes and multi-disc first-disc rejected outcomes.

**Execution note:** Test-first: add a rejected outcome with a matching transfer in the fake slskd snapshot before changing the reset branch.

**Patterns to follow:**
- `rederive_transfer_ids` and `_leave_claim_for_poll_recovery` in `lib/download.py` / `lib/enqueue.py`.
- `test_ambiguous_enqueue_failure_stays_downloading_for_poll_recovery` in `tests/test_enqueue_fanout.py`.

**Test scenarios:**
- Error path: rejected single-disc outcome plus matching snapshot leaves the row `downloading`.
- Happy path: rejected single-disc outcome plus empty snapshot resets to `wanted`.
- Error path: rejected first-disc multi-disc outcome plus matching snapshot leaves the row `downloading`.
- Error path: rejected outcome plus snapshot failure leaves the row `downloading`.

**Verification:**
- No reset branch treats `rejected` alone as proof of no side effect.

---

- U2. **Verify partial multi-disc cancellation**

**Goal:** Ensure partial multi-disc failure clears ownership only after accepted transfers are actually cancelled or absent.

**Requirements:** R1, R2

**Dependencies:** U1

**Files:**
- Modify: `lib/download.py`
- Modify: `lib/enqueue.py`
- Modify: `tests/fakes.py`
- Test: `tests/test_enqueue_fanout.py`

**Approach:**
- Make `cancel_and_delete` treat a falsey `cancel_download` result as failure.
- After cancelling accepted files, verify via a post-cancel snapshot that every planned transfer is absent or terminal-cancelled after the claim boundary.
- If verification fails, persist the planned/enriched state and leave the row `downloading`.

**Execution note:** Characterize the current falsey-cancel behavior first so the regression is pinned.

**Patterns to follow:**
- `_handle_claimed_partial_failure` in `lib/enqueue.py`.
- `FakeSlskdTransfers.cancel_download` in `tests/fakes.py`.

**Test scenarios:**
- Error path: accepted first disc, rejected second disc, cancel returns false leaves `downloading`.
- Error path: accepted first disc, rejected second disc, post-cancel snapshot still shows an active transfer leaves `downloading`.
- Happy path: accepted first disc, rejected second disc, cancel succeeds and post-cancel snapshot proves absence resets to `wanted`.

**Verification:**
- Partial multi-disc cleanup has a proven outcome before clearing `active_download_state`.

---

- U3. **Fence poll recovery and fresh preclaims**

**Goal:** Prevent poll recovery from using stale terminal transfers or immediately timing out a fresh preclaim before slskd can register transfers.

**Requirements:** R2, R3

**Dependencies:** U1

**Files:**
- Modify: `lib/download.py`
- Test: `tests/test_download.py`
- Test: `tests/test_enqueue_fanout.py`

**Approach:**
- Pass `not_before=state.enqueued_at` when poll re-derives transfer IDs from persisted state.
- Add a small same-cycle/fresh-claim guard so rows with no visible transfer immediately after claim are skipped for a later poll rather than reset to `wanted`.
- Keep legacy completed-transfer behavior where it is still valid for older rows; the timestamp fence should distinguish current claims from stale records.

**Test scenarios:**
- Error path: persisted planned row plus stale terminal snapshot older than `enqueued_at` does not bind the stale transfer.
- Race path: fresh planned row with an empty snapshot remains `downloading` instead of resetting to `wanted`.
- Integration: accepted enqueue with missing IDs, fresh context, and a later poll snapshot re-derives IDs from persisted planned state.

**Verification:**
- Poll recovery honors the ownership claim boundary.

---

- U4. **Canonical state, retry accounting, and seam coverage**

**Goal:** Bring serialization, retry/backoff behavior, DB contracts, and transition guard tests in line with the new ownership seam.

**Requirements:** R4, R5, R6

**Dependencies:** None

**Files:**
- Modify: `lib/quality.py`
- Modify: `lib/download_ownership.py`
- Modify: `tests/test_download.py`
- Modify: `tests/test_enqueue_fanout.py`
- Modify: `tests/test_pipeline_db.py`
- Modify: `tests/test_fakes.py`
- Modify: `tests/test_request_finalization.py`

**Approach:**
- Serialize `current_path` even when it is `None`; keep `from_dict` tolerant of missing legacy keys.
- Pass `attempt_type="download"` on verified no-acceptance reset.
- Add real/fake DB tests for guarded reset and guarded state update behavior.
- Extend the AST direct-write guard to include `reset_downloading_to_wanted`.

**Test scenarios:**
- Happy path: initial planned state JSON contains `current_path: null`.
- Happy path: verified no-acceptance increments download attempts and sets retry/backoff metadata.
- Contract: `reset_downloading_to_wanted` succeeds only from `downloading`, clears active state, preserves counters, and returns bool.
- Contract: `update_download_state_if_downloading` updates only `downloading` rows and returns bool.
- Contract: direct `reset_downloading_to_wanted` calls outside `lib.transitions` are rejected by the AST guard.

**Verification:**
- The JSON contract and status seam are enforced by tests, not just code review.

---

## System-Wide Impact

- **Interaction graph:** Phase 2 preclaim writes can now be observed by Phase 1 poll in the same process, so poll must treat very fresh planned rows conservatively.
- **Error propagation:** Failed snapshots, failed cancels, and guard races should flow to `downloading` recovery, not retryable `wanted`.
- **State lifecycle risks:** The main risk is clearing `active_download_state` too early; every reset path must be proven.
- **API surface parity:** `slskd_do_enqueue` remains the low-level helper for retry re-enqueue and should not trigger ownership writes.
- **Integration coverage:** Unit tests must be backed by at least one fresh-context poll/recovery slice.
- **Unchanged invariants:** Request status writes still flow through `lib.transitions`; no migrations or new statuses are introduced.

---

## Risks & Dependencies

| Risk | Mitigation |
|------|------------|
| Snapshot verification is too conservative and leaves more rows `downloading` | Prefer conservative ownership over duplicate/orphan transfers; existing poll/timeout/operator recovery owns follow-up |
| Timestamp filtering breaks valid completed-transfer processing | Apply the fence to current persisted claims and keep tests for terminal snapshot handling |
| Additional tests slow the suite | Keep new cases focused in existing modules and run the full suite once at the end |

---

## Sources & References

- Origin document: `docs/plans/2026-05-05-003-fix-download-ownership-persistence-plan.md`
- Related issue: #219
- Related code: `lib/enqueue.py`, `lib/download.py`, `lib/quality.py`, `lib/download_ownership.py`, `lib/pipeline_db.py`, `lib/transitions.py`
- Related learning: `docs/advisory-locks.md`
- Related learning: `docs/solutions/testing/mocked-contract-tests-miss-helper-mirror-integration-bugs.md`
