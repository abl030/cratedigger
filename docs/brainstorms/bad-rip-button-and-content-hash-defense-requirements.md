---
date: 2026-04-29
topic: bad-rip-button-and-content-hash-defense
---

# Bad-Rip Button + Content-Hash Defense

## Problem Frame

Pre-release leaks of *wrong* audio (engineered fakes — distance 0.000, spectral genuine, but not the real album) are landing in the curated library. The exemplar: Humansin — "Remember the", a 12-track 2026 album added 2026-04-28, force-imported as M4A 320 from user `H@rco` at 2026-04-28 03:34 with distance 0.000 and spectral classification "genuine". The user discovered it was wrong by listening.

Existing infrastructure today:
- The `ban-source` backend (`web/routes/pipeline.py:761` `post_pipeline_ban_source`) already does denylist + `remove_and_reset_release` + requeue-to-wanted.
- `banSource()` is exported from `web/js/library.js:306` and wired into `window.banSource` (`web/js/main.js:92`).
- **No library row actually invokes it** — the function and route are wired but unreachable from the UI. (Verified: `grep -rn "banSource\|onclick.*ban" web/` finds zero call sites.)

The existing spectral and distance gates cannot catch this class of attack — the bad audio was a clean encode of *something*, just not the right something. The user's only durable signal is their own ears.

This brainstorm covers two halves of issue #188:
1. Wire a "Bad rip" button into the library row, on top of the existing backend.
2. When the button fires, capture an audio-only content hash so any other Soulseek seeder reseeding the same bytes is rejected at validation — without paying CPU cost on every import.

---

## Actors

- A1. **Curator** (the user): listens to imported albums; recognises bad rips; presses the button.
- A2. **Web UI**: surfaces the button per library row; calls the existing `ban-source` route extended with hash-capture.
- A3. **Importer / pre-import gate**: at validation time on future downloads, checks candidate files' audio-content hashes against the known-bad list and rejects on match.

---

## Key Flows

- F1. **Mark imported album as bad rip**
  - **Trigger:** A1 clicks "Bad rip" on a library row.
  - **Actors:** A1, A2.
  - **Steps:** (1) UI confirms with the username pulled from the most recent successful `download_log` for this `request_id`. (2) Backend computes audio-only content hash for every imported track (tags + cover art stripped before hashing). (3) Backend persists hashes as known-bad keyed to `(request_id, reported_username)` with the reason. (4) Existing flow continues: `add_denylist`, `remove_and_reset_release`, requeue-to-wanted preserving the user's `search_filetype_override` + `min_bitrate`.
  - **Outcome:** Album is gone from beets, request is back on `wanted`, the offending uploader is denylisted for this album, and every track's audio bytes are now recorded as known-bad globally.
  - **Covered by:** R1, R2, R3, R4, R5, R6, R7.

- F2. **Future Soulseek seeder reuploads the same bad bytes**
  - **Trigger:** A new search for the (now requeued) album returns a candidate from any user whose file's audio-only hash matches a known-bad entry.
  - **Actors:** A3.
  - **Steps:** (1) Candidate downloads. (2) Pre-import gate computes the audio-only hash of each track. (3) Any track matching a known-bad hash → reject the candidate, write a `download_log` row whose `validation_result` records the matched bad-hash row id, no auto-import. (4) The supplying user is denylisted for *this* request (consistent with how spectral-reject denylisting works today).
  - **Outcome:** The bad rip "ripples" through Soulseek harmlessly — every reseeder is automatically rejected and denylisted per album, and the curator never has to listen to it again.
  - **Covered by:** R8, R9, R10.

---

## Requirements

**Library button (Step 1 — wiring)**
- R1. Every library row whose request has at least one successful import (i.e., resolvable username) shall expose a "Bad rip" button.
- R2. Clicking the button shall confirm with copy that names the resolved username and warns that files will be removed from beets and the album requeued. Existing copy at `web/js/library.js:307` is acceptable as a starting point.
- R3. The button shall resolve `username` server-side from the most recent successful `download_log` row for that `request_id`. The client shall not be required to know the username.

**Bad-rip recording (Step 2 — capture)**
- R4. Before any file removal or beet remove, the backend shall compute an audio-content hash for every track in the imported album. The hash shall ignore all tags (ID3, Vorbis comments, MP4 metadata atoms) and embedded artwork — only the audio frames contribute.
- R5. The hash shall be recorded in a new `bad_audio_hashes` table (schema details deferred to planning) with at minimum: hash value, audio format, source `request_id`, `reported_username`, `reason`, and `reported_at`.
- R6. If hash computation fails for any track, the route shall continue with denylist + remove (preserving Step 1 behaviour) and surface the partial-failure to the UI in the response body so the curator can investigate. A hash-capture failure must not block the ban.
- R7. Hash recording shall happen on the existing `ban-source` route — no second endpoint, no parallel code path. Existing `cleanup_errors` response shape may be extended; new fields stay additive.

**Pre-import defense (Step 2 — enforcement)**
- R8. The pre-import gate (`lib/preimport.py`) shall compute the audio-only hash for each candidate track at validation time and reject the candidate if any track's hash matches a `bad_audio_hashes` row.
- R9. A bad-hash rejection shall write a normal `download_log` row whose `validation_result` records the matched hash and the originating `request_id` of the report, so the validation log can show "rejected because audio-hash matches known bad rip from request #N".
- R10. A bad-hash rejection shall denylist the supplying user for the current request (same mechanism as a spectral reject's per-request denylist).

---

## Acceptance Examples

- AE1. **Covers R1, R2, R3.** Given the Humansin row in library with `H@rco` as the most recent successful `download_log` username, when the curator clicks "Bad rip" and confirms, the UI dispatches `POST /api/pipeline/ban-source` with the `request_id` and `mb_release_id` only — the server resolves `H@rco` itself.

- AE2. **Covers R4, R5, R7.** Given a 12-track imported M4A album, when the bad-rip button fires, 12 audio-only hashes are inserted into `bad_audio_hashes` with `reported_username = H@rco`, even if the user later re-tags or re-arts the imported files post-import (because the hash ignores tags).

- AE3. **Covers R8, R9, R10.** Given a `bad_audio_hashes` row from F1, when a future search returns a candidate from user `OtherSeeder` whose track-1 audio bytes are identical to the recorded bad-hash, the candidate is rejected pre-import, a `download_log` row is written with `outcome='rejected'` and `validation_result.matched_bad_hash_id` populated, and `OtherSeeder` is denylisted for the current request.

- AE4. **Covers R6.** Given a bad-rip click on an album with one corrupted file that fails hash computation (e.g., truncated header), when the route runs, the denylist + remove + requeue still complete, the response includes a `hash_capture_errors` field listing the failed track, and the other tracks' hashes are recorded normally.

---

## Success Criteria

- The curator can mark a bad rip and have it removed + requeued in **one click + one confirmation**, with zero CLI fallback for the common case.
- After a bad-rip report, **every subsequent Soulseek search candidate from any user that contains the same audio bytes is rejected at validation**, with a download_log entry making the reason auditable.
- CPU cost on the import pipeline scales with **active validations only** (one hash per candidate track), not with library size or import history. The system never fingerprints or hashes "every album that comes in" speculatively — hashing happens only when there's a known-bad list to check against, and known-bad entries only exist after a curator click.
- The `ban-source` backend remains the single mutating entry point for this flow — no parallel code paths.
- A downstream agent (planner or implementer) can read this doc and know exactly which UI surface, which backend route, which DB table, and which pre-import hook to touch, without re-deciding product behaviour.

---

## Scope Boundaries

- **No automatic fingerprinting on import.** Speculative AcoustID/Chromaprint capture on every successful import is explicitly rejected — wasted CPU for a defense that only matters once a curator has flagged something.
- **No track-count / duration sanity gate** for new releases. The Humansin case had distance 0.000 — sanity gates would have passed. Approach C from the brainstorm is dead.
- **No per-user cross-album "trust score" or global cooldown threshold.** Per-user denylisting stays per-request. If `H@rco` reseeds *different* bad audio for *different* albums, each click is one report — we accept that. Can be revisited if data shows repeat offenders dominate.
- **No fuzzy / perceptual hash.** Exact-byte audio-frame hash only (e.g., SHA-256 over tag-stripped audio frames). Re-encoding by an attacker is a known evasion path; we consciously accept that and treat the hash as a cheap exact-match defense, not a fingerprint.
- **No retroactive library scan.** When a curator marks an album bad, we do not scan their existing library for matching hashes. v1 only protects future imports.
- **Not a discovery tool.** The system does not try to detect bad rips on its own. Discovery remains a human signal.
- **No new "report bad rip from validation log" surface.** v1 is library-row only. The validation-log / wrong-matches review tab can be added later if needed.

---

## Key Decisions

- **Audio-only hash, not whole-file hash.** Tags vary per uploader (renames, art swaps, encoder fields); audio frames are what attackers reuse. Stripping metadata before hashing maximises ripple-stop coverage.
- **Hash on ban, check on every validation.** Asymmetric cost: hashing happens once per track per ban click + once per candidate track per validation. We never store hashes for "good" imports — the table only contains known-bad entries.
- **Per-track hashes, not per-album.** A bad uploader who keeps 11 of 12 good tracks and swaps one fake into another album would still trigger on the swapped track. Album-level concatenation hashes would miss that.
- **Reuse the existing `ban-source` route.** Per `.claude/rules/code-quality.md` "No Parallel Code Paths" — extend the route, don't fork it. Hash-capture is added inline before the existing denylist/remove call.
- **Hash failure ≠ ban failure.** The Step-1 button must work even if a track is corrupt. Surface the partial outcome in the response (analogous to existing `cleanup_errors`).
- **Resolve username server-side.** Frontend should not have to know which user supplied an album. The library row already has `request_id`; the server can pull the most recent successful `download_log` username and use it. Cleaner UI, less drift.

---

## Dependencies / Assumptions

- **Assumption (verified):** `web/routes/pipeline.py::post_pipeline_ban_source` already does denylist + remove-and-reset + requeue-to-wanted, and `banSource` is exported and on `window`. Verified via grep on 2026-04-29.
- **Assumption (verified):** No library row currently invokes `banSource`. Verified via `grep -rn "banSource\|onclick.*ban" web/` returning only the export, the import, and the `window.*` binding — no call sites.
- **Assumption:** ffmpeg (already in the nix-shell dev shell and runtime) can produce a deterministic audio-only byte stream per format (FLAC's STREAMINFO MD5 is canonical; MP3/M4A/OGG via `-c copy -map_metadata -1` plus stripping container framing). Exact tooling is a planning concern.
- **Assumption:** The pipeline DB allows adding a new table via a numbered migration; pre-import gate has a hook point in `lib/preimport.py` where hashes can be checked alongside spectral/audio-integrity. Both align with current architecture per CLAUDE.md and `lib/preimport.py:302+`.
- **Dependency:** Whatever audio-content-hash function is chosen must be stable across hosts (doc1 and doc2 will both compute hashes — doc2 at validation, doc1 occasionally for diagnostics).

---

## Outstanding Questions

### Resolve Before Planning

- *(none — product behaviour is settled.)*

### Deferred to Planning

- [Affects R4][Technical] Exact per-format recipe for "audio-only hash" — whether to lean on FLAC's STREAMINFO MD5 directly, use `ffmpeg -map 0:a -c copy -map_metadata -1 -f <fmt> -` piped to SHA-256, or extract raw decoded PCM. Tradeoff: decoded PCM is fully format-agnostic but is far more CPU; container-stripped frames are cheap but format-specific. Pick during planning with a measurement.
- [Affects R5][Technical] `bad_audio_hashes` schema: track per-track hashes, format, duration; whether to store one row per track or a JSONB array per report; indexing strategy for `O(1)` lookup at validation.
- [Affects R8][Needs research] Where in `lib/preimport.py` the hash check fits relative to existing spectral and audio-integrity gates. Likely *before* spectral (cheaper to reject early), but verify against current ordering.
- [Affects R6][Technical] Response shape extension for `ban-source` — add `hash_capture_errors` alongside existing `cleanup_errors` vs. one unified `partial_failures` block. Both work; planning picks.
- [Affects F1][Technical] Whether to also denylist the *username* who seeded the bad rip across the wider `bad_audio_hashes` lookup (i.e., on hash match in F2, we already know the original `reported_username`; should the new uploader inherit any signal?). v1 keeps it strictly per-request denylist; planning may revisit.

---

## Next Steps

`-> /ce-plan` for structured implementation planning.
