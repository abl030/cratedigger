---
name: project-146-slskd-events-rollout
description: "#146 slskd events refactor: COMPLETE & DEPLOYED 2026-07-02 (all 3 phases, PRs #464/#471/#472/#473, issue closed); event stamp is the only file-location source"
metadata: 
  node_type: memory
  type: project
  originSessionId: 5d6d12b6-0510-4967-8e58-68ad6ff0058b
---

Issue #146 CLOSED 2026-07-02 — all three phases shipped and deployed the same day (PRs #464 phase 1, #471 phase 2, #472 phase 3, #473 post-deploy fix; migrations 041 + 042).

Durable facts for future work:

- **Event stamp is the ONLY completed-file location source.** `lib/slskd_events.py::ingest_download_file_events` stamps `DownloadFileComplete.localFilename` onto `active_download_state.files[].local_path` each poll cycle. Unstamped at materialize = hard failure (`EVENT-PATH MISSING`); poller retries within `PROCESSING_MATERIALIZE_GRACE_S` (1h) then self-heals to `wanted` (`materialize_failure_action` pure fn; `None`=guarded manual recovery is never auto-reset).
- **Module layout after the split:** `lib/download.py` (poll state machine), `lib/download_processing.py` (staging/materialization/validation dispatch), `lib/slskd_transfers.py` (enqueue/status/cancel/transfer-ID), `lib/slskd_events.py` (feed ingestion). Test patch-target convention: poll tests patch `lib.download.*` bindings; youtube/preview-worker paths patch `lib.download_processing.*`; mock-audit alias `dp_mod` → lib.download_processing.
- **`cancel_and_delete` is event-driven** — per-file deletes at stamped paths + fresh `recent_completion_paths` page lookup, empty-dir pruning only, bounds-checked. Never rmtree by inference.
- **The verify loop paid off twice (test-fidelity Rule A):** phase-3's grace escape shipped `outcome='error'` past a permissive `FakePipelineDB` and crashed on `download_log_outcome_check` in prod. Fix (PR #473) made the fake mirror the CHECK (`DOWNLOAD_LOG_OUTCOMES` in tests/fakes/pipeline_db.py — keep in sync with migrations), which immediately exposed lib/enqueue.py's `outcome='user_offline'` as a second latent crash → migration 042 widened the CHECK. **When adding a download_log outcome value: migration first, then DOWNLOAD_LOG_OUTCOMES.**
- **msgspec trap:** `ValidationError` is a subclass of `DecodeError`; catching only ValidationError around `msgspec.json.decode` lets malformed (non-JSON) payloads escape. Catch `DecodeError` at wire boundaries.
- The three pre-bootstrap stuck rows (BT 5603, Peter Broderick 4587, Mighty Boosh 2812) self-healed post-deploy: reset to wanted with `failed` log rows; re-downloads on backoff will arrive stamped. If any of them wedge again it's a NEW bug, not this one.
- Deliberately NOT done: retyping legacy dict shapes (downloads snapshot / search responses / browse dirs) into msgspec Structs — out of scope.
