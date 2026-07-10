---
name: project-550-complete
description: Issue
metadata: 
  node_type: memory
  type: project
  originSessionId: 74997152-9b39-4880-91d2-1ebba99b202f
---

Issue #550 (multi-disc partial manifests / false untracked_audio) CLOSED 2026-07-09, tagged v2026.07.09.

- Defect #1 (partial-disc manifests): PR #557. Defect #2 (canonical-folder accumulation): PRs #560/#561.
- Defect #3 (on-disk orphan reaper): PR #566 — `reap_disk_orphans` in lib/slskd_transfers.py, Phase 0, `ORPHAN_MIN_AGE_DAYS=7` constant. Protected: `failed_imports/` quarantine (lives INSIDE the download root!) + downloading rows' stamped paths + canonical folders. Fail-closed: undecodable `active_download_state` aborts the sweep (`DiskReapOwnershipError`). Derivation-parity guard vs materialize is load-bearing (mutant-qualified, #546 drift class). First sweep reclaimed 148 GB / 8,137 files; steady state silent.
- Defect #4 (false-green Wrong Match): PR #567 — no unmeasured distance is ever recorded as a number; `distance: float | None` end-to-end. Deploy-window backfill nulled 1,776 column + 42 JSONB + 1,344 album_requests fabricated zeros. Post-match scenarios (e.g. `downgrade`) deliberately NOT backfilled — column 0.0 there can be a real measurement.
- MANIFEST-TRACE (PR #549) kept permanently as log-only seam observability.
- slskd's incomplete dir is `<download_dir>/incomplete` — under the reaper's root; safe via age rail (live partials have fresh mtimes) and week-old partials are genuinely abandoned.
- Request 2812 still `wanted` at close (seeders offline) — never-stop-searching; next grab validates against a reaped-clean world. Monitor for the rescue.
- Workflow reused: sonnet implementers in parallel worktrees + opus review loops at orchestrator; both PRs needed exactly one fix round. See [[feedback-pin-and-fuzz-pair]], [[feedback-review-loop-at-orchestrator]].
