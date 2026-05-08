# Peer Online Probe at Enqueue — Requirements

**Date:** 2026-05-08
**Status:** Ready for planning
**Companion fix:** Part 1 — classify slskd `User offline` HTTPError as `rejected` rather than `unknown` in `slskd_enqueue_with_outcome` (this document is Part 2 of that work; both parts ship together).

## Problem

The Redis peer cache has grown large (>1000 hits/cycle). Cached directory listings remain valid for users who were online when we last browsed but have since gone offline. When Phase 2 picks one of those users as the winning match and calls `slskd.transfers.enqueue`, slskd attempts the upload, finds the peer offline, and returns an HTTP error. Today we misclassify that as an "ambiguous" outcome, leave the request in `downloading`, and time out 60+ seconds later with a misleading `all transfers vanished from slskd` log row. Concrete recurrence: 9 such `download_log` rows since 2026-05-05; the triggering case (request 2540, Mercury Rev — Deserter's Songs, user `pooyork`) is in the diagnostic transcript dated 2026-05-08.

## Goal

Don't enqueue against a user who is provably offline. Find out before we waste the call.

## Approach

Add one slskd HTTP probe immediately before each enqueue attempt. If the user is `Offline`, treat the matched candidate as a non-starter and continue the existing match-loop iteration to the next eligible (user, dir) pair. No new state, no caching, no cooldowns.

## Behaviour

1. After a match is selected (in `lib/enqueue.py`'s per-(user, dir) iteration, just before `_enqueue_with_claim_outcome`), call `ctx.slskd.users.status(username).presence`.
2. If `presence == "Online"` or `presence == "Away"`, proceed with the existing enqueue flow unchanged.
3. If `presence == "Offline"`, log at INFO (`peer offline at enqueue: skipping <user> for album <id>`), discard this candidate, and let the surrounding loop pick the next eligible (user, dir) pair. The download-ownership claim must NOT be set for an offline candidate — only the user that we actually attempt to enqueue against gets a claim.
4. If `users.status()` raises an exception (transient slskd error, unknown user, etc.), proceed to enqueue anyway. Part 1's `user_offline → rejected` classification is the safety net: a downstream rejection still resets the claim cleanly within the same cycle.
5. No persistence. No Redis key. No `user_cooldowns` row. Next cycle starts fresh and may re-select the same user — that is intentional and acceptable given the request volume.

## Scope Boundaries

**In scope:**
- One `users.status()` call per match candidate that survives matching, located in the enqueue path between match selection and `transfers.enqueue`.
- INFO log line on offline skip so the operator can see it in the cycle journal.

**Out of scope (deliberately rejected during brainstorm):**
- Probing every eligible user before matching — 1000+ extra API calls per cycle, ruled out.
- Caching presence in Redis or anywhere else.
- Any new cooldown or denylist semantics tied to offline status.
- Touching the existing `peer_dir` / `peer_dir_neg` cache contents.
- Surfacing presence in the web UI.

## Success Criteria

- A request whose top-ranked match is currently offline does not write a `download_log` row with `error_message='all transfers vanished from slskd'`. Instead, the cycle either enqueues the next eligible candidate or simply ends with no enqueue (same as today's "no match this cycle" path).
- The Mercury Rev / pooyork class of failure stops appearing in `download_log` once Part 1 + Part 2 are deployed.
- No measurable increase in cycle time. (One extra HTTP call per album-with-a-match is negligible compared to existing browse fan-out.)

## Dependencies / Assumptions

- slskd `GET /users/{username}/status` returns sub-second for users it has previously interacted with. Assumed true because slskd holds the peer connection state from prior browse calls; the directory listings we matched on came through that same channel.
- "Away" means the peer is reachable for uploads. Treating Away as Online in this design; if real-world experience proves otherwise, narrow to `Online` only — single-line change.
- slskd-api raises `requests.exceptions.HTTPError` for non-2xx responses (verified — `session.hooks` in `slskd_api/client.py:60`). Caller treats any `users.status()` exception as "unknown, attempt enqueue".

## Test Plan

- **Unit (seam):** in `tests/test_enqueue.py`, add a fake `slskd.users.status` to `FakeSlskdAPI` with configurable return value. Assert that when `presence='Offline'`, `transfers.enqueue` is never called for that user, the download-ownership claim is not set, and the loop advances to the next candidate.
- **Unit (seam):** when `users.status` raises, `transfers.enqueue` IS called (fall-through path).
- **Orchestration:** end-to-end through `lib.enqueue` with two ranked candidates — first offline, second online. Assert that the request transitions from `wanted` to `downloading` against the second user, and no `download_log` row records the first as failed (we never tried to enqueue them).
- **Pyright:** `FakeSlskdAPI.users` extension should remain typed.

## Open Questions

None remaining for product behaviour. Implementation decisions (where exactly the probe lives in `lib/enqueue.py`'s match loop; whether `Away` warrants a config knob) belong in the planning step.
