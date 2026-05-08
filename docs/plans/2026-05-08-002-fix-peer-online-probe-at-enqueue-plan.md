---
title: Peer Online Probe at Enqueue + User-Offline Classification
type: fix
status: active
date: 2026-05-08
origin: docs/brainstorms/2026-05-08-peer-online-probe-at-enqueue-requirements.md
---

# Peer Online Probe at Enqueue + User-Offline Classification

## Summary

Two-part fix that ships together. Part 1 stops misclassifying slskd's `User offline` HTTPError as an "ambiguous" enqueue outcome — instead we treat it as a verifiable rejection, reset the claim immediately, and write a `download_log` row in the same cycle. Part 2 layers a one-call online-status probe (`slskd.users.status(username).presence`) just before each enqueue attempt so we never even try an enqueue against a peer we know is offline. No new persistence — no Redis key, no cooldown row, no in-process state. The match loop simply advances to the next eligible (user, dir).

---

## Problem Frame

Documented in `docs/brainstorms/2026-05-08-peer-online-probe-at-enqueue-requirements.md`. Concrete recurrence: 9 `download_log` rows since 2026-05-05 with `error_message='all transfers vanished from slskd'`; the triggering case is request 2540 (Mercury Rev — Deserter's Songs, user `pooyork`) on 2026-05-08. Root cause is that the Redis peer cache (now serving 1000+ hits/cycle) returns valid directory listings for users who have since gone offline; today's "ambiguous" path leaves the row in `downloading` for 60+ seconds before timing out with a misleading vanish message.

---

## Requirements

- R1. After Part 1 + Part 2 deploy, the user-offline failure mode no longer produces `error_message='all transfers vanished from slskd'` on `download_log`. The cycle either advances to the next eligible peer or ends with no enqueue.
- R2. When slskd raises `HTTPError` with body `"User <name> appears to be offline"` from `transfers.enqueue`, `slskd_enqueue_with_outcome` returns `SlskdEnqueueOutcome(status="rejected")`. Other exception shapes (connection errors, generic 5xx without the offline marker, unrelated 4xx) continue to return `status="unknown"`.
- R3. The `rejected` branch of `try_enqueue` writes a `download_log` row recording the soulseek user, filetype, and a non-misleading error message.
- R4. Before each `transfers.enqueue` call, `try_enqueue` consults `slskd.users.status(username).presence`. If the result is `"Offline"`, the candidate is skipped without claiming download ownership; the loop advances to the next `_iter_wave_matches` yield.
- R5. `"Online"` and `"Away"` both proceed to enqueue. If `users.status()` raises, `try_enqueue` falls through to enqueue and lets Part 1's classification handle the outcome.
- R6. No persistence is added — no new Redis key, no `user_cooldowns` row, no `CratediggerContext` field tied to presence.

---

## Scope Boundaries

- Probing presence for every eligible user before matching — explicitly out (one HTTP call per candidate would multiply into 1000s per cycle).
- Caching presence results anywhere — explicitly out per origin doc.
- New cooldown/denylist semantics tied to offline status — explicitly out.
- Touching `peer_dir` / `peer_dir_neg` cache contents — explicitly out.
- Web UI / CLI surface for "currently offline" — explicitly out.
- Redirecting other existing `_leave_claim_for_poll_recovery` call sites (multi-disc partial enqueue, ambiguous network errors). Those keep current semantics; only the user-offline subset moves to `rejected`.

---

## Context & Research

### Relevant Code and Patterns

- `lib/download.py:slskd_enqueue_with_outcome` (≈ lines 433–499) — the single seam that converts slskd-api responses into `SlskdEnqueueOutcome`. `SlskdEnqueueOutcome` is a frozen dataclass at lines 372–377 with `status: Literal["accepted", "rejected", "unknown"]`.
- `lib/enqueue.py:try_enqueue` (≈ lines 825–970) — the per-album match loop. The `_iter_wave_matches` generator yields `(username, match_result, wave_idx)` per matched candidate; the existing `accepted` / `rejected` / `unknown` branching lives at lines 889–943.
- `lib/enqueue.py:_reset_claim_after_verified_no_acceptance` (line 529) calls `writer.reset_after_no_acceptance(request_id)` (defined at `lib/download_ownership.py:109`) which only flips status `downloading → wanted`. It does NOT write a `download_log`. Today the `rejected` branch in `try_enqueue` is effectively dead — slskd-api raises `HTTPError` (via `session.hooks` in `slskd_api/client.py:60`) for non-2xx, so `enqueue()` never returns falsy. After Part 1, the `rejected` branch becomes the live user-offline path and needs the log.
- `lib/download.py:_timeout_album` (≈ lines 1815–1843) — canonical example of the "reset to wanted + write download_log + apply cooldown counter" pattern. Uses `_build_download_info(entry)` then `db.log_download(request_id, soulseek_username, filetype, outcome, error_message)`.
- `tests/fakes.py:FakeSlskdUsers` (lines 259–315) already exposes `directory()`, `set_directory()`, `set_directory_error()`. Needs a `status()` method + `set_status()` / `set_status_error()` helpers, mirroring the directory pattern.
- `tests/fakes.py:FakeSlskdTransfers` already supports configurable per-call errors — the same shape is what we want for `users.status`.
- `nix/store/.../slskd_api/apis/users.py:status` returns `UserStatus = {"presence": Literal["Online", "Away", "Offline"], "isPrivileged": bool}`.

### Institutional Learnings

- Wire-boundary types must use `msgspec.Struct`, not `@dataclass`, when they cross JSON. The slskd `UserStatus` payload IS a wire boundary — when we add a typed wrapper around `users.status()` (if needed), it should be a `msgspec.Struct`. (`.claude/rules/code-quality.md` § "Wire-boundary types".) For Part 1, however, `SlskdEnqueueOutcome` is purely internal — staying as a `@dataclass` is correct.
- "All scripts deploy via Nix, no manual cp" (`CLAUDE.md` § Critical rules). The fix lands by `nix flake update cratedigger-src` on doc1, then `nixos-rebuild switch` on doc2. `cratedigger.service` has `restartIfChanged = false`, so the next 5-min timer cycle picks up the new code automatically.
- `FakePipelineDB` records `download_logs` with an `assert_log()` helper — orchestration tests should use that rather than poking internals.

### External References

- slskd-api `transfers.enqueue` source: `nix/store/.../slskd_api/apis/transfers.py:93-105` — HTTP POST, `response.ok` returned, but `session.hooks` raises on non-2xx so callers always see `HTTPError`.
- slskd-api `users.status` source: `nix/store/.../slskd_api/apis/users.py:76-82` — HTTP GET, returns parsed JSON.

---

## Key Technical Decisions

- **Detect "user offline" by HTTP body match, not status code.** slskd may use 400 / 500 / 504 across versions for `UserOfflineException`, but the response body consistently contains `"appears to be offline"` (verified in slskd journal at 2026-05-08T20:18:39). Matching on the body is more durable than matching on status code, and we keep an exception-catch fast path: only inspect the body inside the `requests.exceptions.HTTPError` branch.
- **Probe at match-loop level, not inside `slskd_enqueue_with_outcome`.** The probe needs access to the per-candidate iteration in `try_enqueue` so the "Offline → continue to next candidate" semantics work cleanly. Pushing it into `slskd_enqueue_with_outcome` would force that helper to know about looping, which it shouldn't.
- **Probe BEFORE `_claim_initial_download_ownership`, not after.** Claiming flips the request to `downloading`. If we probed after claiming and found offline, we'd have to undo the claim — adds a lossy reset path. Probing first means offline candidates never trigger a claim.
- **No typed `UserStatusMessage` wrapper.** `slskd.users.status()` returns a `TypedDict` with two fields and we only read one. Adding a `msgspec.Struct` wrapper is overkill for a single use site. If the surface grows later (e.g., we start using `isPrivileged`), revisit then.
- **`Away` counts as Online.** Per origin §"Dependencies / Assumptions" — Soulseek peers marked Away can still serve uploads. If field experience contradicts this, narrow to `Online` only — single-line change.
- **`download_log` outcome value for the rejected path.** Use `outcome="user_offline"` rather than reusing `outcome="timeout"` so historical timeout rows remain distinguishable. The existing schema doesn't constrain `outcome` values (free text), and the web UI only renders the column verbatim.

---

## Open Questions

### Resolved During Planning

- *Where exactly does the probe live in `try_enqueue`?* — between the empty-files `continue` check (≈ line 867) and `_claim_initial_download_ownership` (line 868).
- *Does the rejected branch already write a `download_log` row?* — No. It only resets via `_reset_claim_after_verified_no_acceptance`. Plan adds the log write in U3.
- *Do we need a new outcome enum value?* — No. `SlskdEnqueueOutcome.status` already has `"rejected"`. We're widening the conditions under which it fires, not adding a new state.

### Deferred to Implementation

- Exact body-match string. Plan recommends `"appears to be offline"` (case-insensitive, substring). The implementer may tighten or loosen after eyeballing live response bodies — the test scenarios pin behavior either way.
- Whether `_reset_claim_after_verified_no_acceptance` itself should take an optional `download_log_payload` parameter, vs. having `try_enqueue` write the log inline. Implementer's call; both are acceptable.

---

## Implementation Units

### U1. Extend `FakeSlskdUsers` with `status()` + helpers

**Goal:** Provide the test surface needed for both Part 1 and Part 2 before any production code changes land.

**Requirements:** Supports tests for R2, R4, R5.

**Dependencies:** None.

**Files:**
- Modify: `tests/fakes.py`
- Test: `tests/test_fakes.py`

**Approach:**
- Add `status_calls: list[str]`, `_statuses: dict[str, str]`, `_status_errors: dict[str, Exception]` to `FakeSlskdUsers.__init__`.
- Add `set_status(username, presence)`, `set_status_error(username, error)`, and `status(username)` method.
- Default presence when not set should be `"Online"` so existing orchestration tests that don't yet care about presence remain green.
- `status()` records the call in `status_calls`, raises configured error if any, otherwise returns `{"presence": <set or "Online">, "isPrivileged": False}` matching slskd-api's `UserStatus` TypedDict shape.

**Execution note:** Test-first. Add `tests/test_fakes.py` cases that exercise the new API before wiring it into orchestration tests downstream.

**Patterns to follow:**
- Mirror the existing `set_directory` / `set_directory_error` / `directory_calls` shape on the same class (lines 259–315).

**Test scenarios:**
- Happy path: `set_status('foo', 'Online')` → `api.users.status('foo')` returns dict with `presence='Online'`. Recorded in `status_calls`.
- Happy path: `set_status('foo', 'Offline')` → `api.users.status('foo')` returns `presence='Offline'`.
- Happy path: `set_status('foo', 'Away')` → `api.users.status('foo')` returns `presence='Away'`.
- Edge case: no `set_status` called → `api.users.status('whoever')` returns default `presence='Online'` (so legacy tests don't need updating).
- Error path: `set_status_error('foo', requests.HTTPError(...))` → `api.users.status('foo')` raises that exception. Call still recorded in `status_calls` (assert ordering by inspection).

**Verification:**
- `nix-shell --run "python3 -m unittest tests.test_fakes -v"` passes.
- Pyright clean on `tests/fakes.py`.

---

### U2. Classify `User offline` HTTPError as `rejected` in `slskd_enqueue_with_outcome`

**Goal:** Part 1 — convert the slskd user-offline rejection from "unknown / ambiguous" to a verifiable `SlskdEnqueueOutcome(status="rejected")`. Pure-function change.

**Requirements:** R2.

**Dependencies:** U1 (test fakes).

**Files:**
- Modify: `lib/download.py`
- Test: `tests/test_download.py`

**Approach:**
- Replace the bare `except Exception` at lib/download.py:442 with a tiered catch:
  1. `except requests.exceptions.HTTPError as e:` — inspect `e.response.text` (or `e.response.json()` if structured) for `"appears to be offline"` (case-insensitive substring). On match, return `SlskdEnqueueOutcome(status="rejected")`. Otherwise return `SlskdEnqueueOutcome(status="unknown")`.
  2. `except Exception:` — preserve current behavior, return `SlskdEnqueueOutcome(status="unknown")`.
- Defensive: if `e.response is None` (rare), treat as `"unknown"`.
- Keep the `logger.debug("Enqueue failed", exc_info=True)` line — diagnostic value for the non-offline branches.

**Execution note:** Test-first. Add the failing tests in `tests/test_download.py` before touching `lib/download.py`.

**Patterns to follow:**
- The existing `slskd_enqueue_with_outcome` skeleton — keep the function body shape and the `enqueue` / `not enqueue` branches intact. Only the exception handler changes.

**Test scenarios:**
- Happy path: `transfers.enqueue` raises `requests.HTTPError` whose `response.text` contains `"User pooyork appears to be offline"` → `slskd_enqueue_with_outcome(...)` returns `SlskdEnqueueOutcome(status="rejected", downloads=None)`.
- Happy path: same but `response.text` is `"user FOO appears to BE OFFLINE"` (mixed case) → still `rejected`. Confirms case-insensitive substring match.
- Edge case: `HTTPError` whose `response.text` is `"internal server error"` (no offline marker) → `status="unknown"`.
- Edge case: `HTTPError` with `response is None` → `status="unknown"`. Defensive guard.
- Error path: `enqueue` raises `requests.exceptions.ConnectionError` (not HTTPError) → `status="unknown"`. Generic exception path preserved.
- Regression: `enqueue` returns falsy (e.g., `False`) → still returns `status="rejected"` per existing line 446 (this branch may be unreachable in practice but the contract is preserved).
- Regression: `enqueue` returns truthy + `_get_all_downloads_snapshot` reconciles all IDs → still returns `status="accepted"` with the full `downloads` list.

**Verification:**
- All new tests pass; `tests.test_download` suite stays green.
- Pyright clean on `lib/download.py`.

---

### U3. Write `download_log` row when `try_enqueue` takes the `rejected` path

**Goal:** Part 1 wiring — when U2 starts producing `status="rejected"` for live user-offline cases, surface the failure in `download_log` immediately rather than silently resetting to `wanted`.

**Requirements:** R1, R3.

**Dependencies:** U1, U2.

**Files:**
- Modify: `lib/enqueue.py`
- Test: `tests/test_enqueue.py`

**Approach:**
- In `try_enqueue` at the `if outcome.status == "rejected":` branch (≈ line 907), after calling `_reset_claim_after_verified_no_acceptance`, also write a `download_log` row via the same `db.log_download(...)` call shape used by `_timeout_album`.
- Keep the log write narrowly scoped to the user-offline-shaped reject. Field values:
  - `request_id = claim.request_id`
  - `soulseek_username = username` (the loop-local variable)
  - `filetype = allowed_filetype`
  - `outcome = "user_offline"`
  - `error_message = "user offline at enqueue"`
- Do NOT call `db.check_and_apply_cooldown(username)` here — origin §Out of scope explicitly excludes new cooldown semantics.
- No change to the other call sites of `_reset_claim_after_verified_no_acceptance` (line 620, multi-disc partial cancel) — that's a different scenario and does not get a log here.

**Execution note:** Test-first.

**Patterns to follow:**
- `_timeout_album` in `lib/download.py:1815-1832` — same `db.log_download(...)` call shape.
- `FakePipelineDB.assert_log(self, idx, **expected)` for the test assertion (`tests/fakes.py`).

**Test scenarios:**
- Happy path (orchestration): single eligible user, `FakeSlskdAPI.transfers.enqueue` raises offline-shaped `HTTPError`. After `try_enqueue` returns, request status is `wanted`, `FakePipelineDB.download_logs` has exactly one row with `outcome="user_offline"`, `soulseek_username="pooyork"`, `filetype="flac 16/44.1"`, `error_message="user offline at enqueue"`.
- Regression: `transfers.enqueue` returns truthy + reconciles IDs → no `download_log` row written for the success path; request transitions `wanted → downloading`.
- Regression: `transfers.enqueue` raises generic `ConnectionError` (status="unknown", ambiguous branch) → no `download_log` row written here, claim left for poll-cycle recovery as today. Confirms we didn't widen the log site.
- Edge case: `_reset_claim_after_verified_no_acceptance` returns the "couldn't prove no acceptance" `claim.entry.files` value (verified-no-acceptance failed) → log is still written (the failure is observable; the residual claim is the system's safety net but the user deserves the log entry).

**Verification:**
- New orchestration tests pass.
- `pipeline-cli show <id>` for a future user-offline failure shows the row with the new outcome.
- Pyright clean on `lib/enqueue.py`.

---

### U4. Add `users.status` probe in `try_enqueue` match loop

**Goal:** Part 2 — gate every match candidate on `slskd.users.status(username).presence` before claim + enqueue. Offline candidates skip cleanly to the next match.

**Requirements:** R4, R5, R6.

**Dependencies:** U1.

**Files:**
- Modify: `lib/enqueue.py`
- Test: `tests/test_enqueue.py`

**Approach:**
- Insert the probe between the empty-files `continue` check (≈ lib/enqueue.py:867) and `_claim_initial_download_ownership` (line 868).
- New helper at module scope: `def _peer_is_eligible_for_enqueue(username: str, ctx: CratediggerContext) -> bool`. Calls `ctx.slskd.users.status(username)`, reads `presence`, returns `False` only when `presence == "Offline"`. Treats `"Online"` and `"Away"` as eligible. On exception, log at DEBUG with `exc_info=True` and return `True` (fall through to enqueue — Part 1's classification is the safety net, per R5).
- In the loop: `if not _peer_is_eligible_for_enqueue(username, ctx): logger.info("peer offline at enqueue: skipping %s for album %s", username, album_id); continue`.
- The `continue` here advances the `for ... in _iter_wave_matches(...)` loop. No claim is made, no log is written, no slskd transfer call is issued.
- This change does NOT touch `_eligible_user_dirs` or any other earlier filter — origin explicitly rules out probing all eligible candidates.

**Execution note:** Test-first.

**Patterns to follow:**
- Logging style mirrors `_log_album_browse` and other INFO lines at the same scope.
- `cooldowns.py:get_cooled_down_users` is the existing analog of "skip a user" — but lives at the eligible-filter layer, not the per-match layer. The new probe is intentionally NOT integrated there.

**Test scenarios:**
- Happy path: single online candidate. `FakeSlskdUsers.set_status('foo', 'Online')`. `try_enqueue` calls `users.status('foo')` once, then proceeds to enqueue. Assert `users.status_calls == ['foo']` and `transfers.enqueue` was called once.
- Happy path: single Away candidate. `set_status('foo', 'Away')`. Probe is called, treated as online, enqueue proceeds. Confirms Away → Online behavior.
- Happy path (skip): single Offline candidate. `set_status('foo', 'Offline')`. After `try_enqueue` returns, `users.status_calls == ['foo']`, `transfers.enqueue` was NOT called, no claim was made (FakePipelineDB request status remains `wanted`), no `download_log` row written.
- Happy path (failover): two ranked candidates. User A offline, user B online. Assert `users.status_calls == ['A', 'B']`, `transfers.enqueue` called once with B, B's claim made and persisted, A never claimed. Request status `wanted → downloading`.
- Error path: `users.status` raises `HTTPError`. Probe falls through, `transfers.enqueue` is called as today. If enqueue then succeeds, request transitions `wanted → downloading`; if enqueue raises offline-shaped `HTTPError`, U2 + U3 take over (status="rejected" + log). Confirms R5.
- Edge case: probe returns Offline but `_iter_wave_matches` has no further candidates. `try_enqueue` returns `EnqueueAttempt(matched=False, ...)` (or whatever the loop's exhausted-without-success path produces today). No claim, no log, request stays `wanted`.
- Edge case: probe never sees an offline user across multiple matches → existing behavior unchanged. Verifies the probe is non-disruptive in the steady state.

**Verification:**
- New orchestration tests pass.
- Live verification post-deploy: `pipeline-cli show <future-request>` for a user that was matched-but-skipped shows no spurious download_log row, and the request's search forensics show subsequent peers attempted.
- Pyright clean on `lib/enqueue.py`.

---

### U5. Integration slice: two-user fan-out, first offline, second online

**Goal:** End-to-end coverage that Part 1 + Part 2 compose correctly. Real `try_enqueue`, real `slskd_enqueue_with_outcome`, real `_reset_claim_after_verified_no_acceptance` — only the network edges (slskd, DB) are faked.

**Requirements:** R1, R3, R4 (composed).

**Dependencies:** U2, U3, U4.

**Files:**
- Test: `tests/test_integration_slices.py`

**Approach:**
- Slice mirrors existing `TestDispatchThroughQualityGate` shape — patch only `_get_all_downloads_snapshot` (since we're not testing the snapshot polling) and rely on real code from `try_enqueue` down.
- Build two ranked candidates via `FakeSlskdAPI` directory data and standard search-result builders. Configure `FakeSlskdUsers.set_status('A', 'Offline')` and `set_status('B', 'Online')`. Configure `FakeSlskdAPI.transfers.enqueue` to succeed for B.
- Run `try_enqueue(...)`. Assert:
  - `users.status_calls` records both A and B in order.
  - `transfers.enqueue` was called exactly once, for user B, with B's planned files.
  - The request transitions `wanted → downloading` against B's user.
  - No `download_log` row was written (offline skip is silent; only a hard rejection writes a log).
- Add a sibling test where BOTH users are offline. Assert: enqueue never called, no claim made, request stays `wanted`, no `download_log` row, `EnqueueAttempt.matched` reflects the no-match outcome.
- Add a third sibling test where probe says A is online (cache lying / status endpoint stale) but `transfers.enqueue` then raises offline-shaped `HTTPError`. Assert: U2 + U3 path runs — request returns to `wanted`, one `download_log` row with `outcome="user_offline"`. Confirms the safety-net composition.

**Execution note:** Test-first.

**Patterns to follow:**
- `tests/test_integration_slices.py:TestDispatchThroughQualityGate` for slice scaffolding.
- `tests/helpers.py:make_ctx_with_fake_db` to wire `FakePipelineDB` into a `CratediggerContext`.

**Test scenarios:**
- *(scenarios are described in the Approach section above as the slice's three composed cases)*

**Verification:**
- `nix-shell --run "python3 -m unittest tests.test_integration_slices -v"` passes including the three new cases.
- `bash scripts/run_tests.sh` full suite green; pyright clean.

---

## System-Wide Impact

- **Interaction graph:** Touches `try_enqueue` (the per-album match loop) and `slskd_enqueue_with_outcome` (the slskd seam). Other call sites of `_reset_claim_after_verified_no_acceptance` (multi-disc partial cancel at lib/enqueue.py:620, line 1174) are intentionally NOT modified.
- **Error propagation:** When `users.status()` raises, the probe falls through to enqueue. If enqueue then succeeds, normal flow continues. If enqueue raises offline-shaped `HTTPError`, U2 reclassifies to `rejected` and U3 writes the log. Worst-case is identical to today's behavior with one extra HTTP round trip.
- **State lifecycle risks:** Probe runs BEFORE claim, so an offline candidate never causes a `wanted → downloading` flip that needs unwinding. The existing rejected branch already handles the unwind path for the safety-net case.
- **API surface parity:** No web/API changes. CLI `pipeline-cli show <id>` will start showing `outcome="user_offline"` rows for the user-offline failure mode going forward — that's a deliberate diagnostic improvement, not a breaking change.
- **Integration coverage:** U5 covers the cross-layer composition. Unit tests at U2 / U4 validate the seams independently.
- **Unchanged invariants:** `SlskdEnqueueOutcome` shape, the `accepted` / `unknown` branches of `try_enqueue`, the `_iter_wave_matches` generator contract, all peer-cache code, all cooldown code (`user_cooldowns`, `cooled_down_users`), and the web UI all stay unchanged.

---

## Risks & Dependencies

| Risk | Mitigation |
|------|------------|
| slskd's user-offline error body changes shape across versions | Substring match on `"appears to be offline"` is more durable than status-code matching. If it ever changes, U2 quietly degrades to `status="unknown"` (today's behavior) — no regression vs current state. |
| `users.status()` is slow for never-browsed users | Per origin §Dependencies, slskd holds peer connection state from prior browse calls and the directory listing came from that channel — status is fast for any user we've already matched. The probe runs only for matched candidates, not every eligible user. |
| `Away` peers reject uploads in practice | Single-line change to narrow probe predicate to `presence == "Online"` only. Easy to reverse if observed. |
| Adding a `download_log` row in the rejected branch could surprise UI consumers expecting only timeout/success rows | The web UI renders `outcome` as free text. New `outcome="user_offline"` rows just appear with that label. No schema change. |
| `users.status()` HTTP errors flood logs | Logged at DEBUG with `exc_info=True`. INFO line for the offline-skip path is a separate, intentional diagnostic. |

---

## Documentation / Operational Notes

- No CLAUDE.md / docs update required — neither pipeline behavior nor decision architecture changes shape.
- Deploy via the standard flow in `.claude/rules/deploy.md`: push cratedigger → `nix flake update cratedigger-src` on doc1 → `nixos-rebuild switch` doc2. `cratedigger.service` has `restartIfChanged = false`; the next 5-min timer cycle picks up the new code automatically. No DB migration.
- Post-deploy verification: monitor `download_log` for the next 24 h. Expect: zero new `error_message='all transfers vanished from slskd'` rows attributable to peer offline-status (rare snapshot-fetch failures at poll time may still produce that message — those are unrelated and stay in scope today).

---

## Sources & References

- **Origin document:** `docs/brainstorms/2026-05-08-peer-online-probe-at-enqueue-requirements.md`
- Diagnostic transcript: chat session dated 2026-05-08 covering request 2540 (Mercury Rev — Deserter's Songs, user `pooyork`).
- Related code: `lib/download.py:slskd_enqueue_with_outcome`, `lib/enqueue.py:try_enqueue`, `lib/download_ownership.py:reset_after_no_acceptance`, `lib/download.py:_timeout_album`, `tests/fakes.py:FakeSlskdUsers`.
- slskd-api source paths (read during planning): `slskd_api/apis/transfers.py:93-105`, `slskd_api/apis/users.py:76-82`, `slskd_api/client.py:60`.
