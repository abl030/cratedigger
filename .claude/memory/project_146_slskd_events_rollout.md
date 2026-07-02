---
name: project-146-slskd-events-rollout
description: "#146 slskd events refactor: phase 1 shipped in PR #464 (2026-07-02); phase 2 gated on a week of clean EVENT-PATH COMPARE logs; phase 3 deletes the resolver"
metadata: 
  node_type: memory
  type: project
  originSessionId: 5d6d12b6-0510-4967-8e58-68ad6ff0058b
---

Issue #146 (use slskd DownloadFileComplete events instead of reverse-engineering paths), state as of 2026-07-02:

- **DEPLOYED 2026-07-02 07:52 AWST** (merge `1698a72`, nixosconfig `d1eeed24`): migration 041 applied, cursor bootstrapped, EVENT-PATH COMPARE flowing. **Phase-2 gate check due ~2026-07-09**: `journalctl -u cratedigger --since "-7d" | grep "EVENT-PATH COMPARE" | grep -c "match=False"` must be 0. Two rows (5603 BT This Binary Universe + a Peter Broderick disc-2) are stuck in a PRE-EXISTING multi-disc-aliasing retry loop whose completions predate the cursor — they log `no event local_path` forever until re-downloaded; that's expected noise, not a gate failure, and is itself the motivating bug class for phase 2.
- **Phase 1 shipped in PR #464** (branch `feat/146-slskd-client-events`, 4 commits): typed in-repo client `lib/slskd_client.py` (the `slskd-api` PyPI dep + `nix/slskd-api.nix` are GONE), event ingestion `lib/slskd_events.py` + migration 041 `slskd_event_cursor`, `local_path` on `DownloadFile`/`ActiveDownloadFileState`, side-by-side logging in `process_completed_album`. Resolver (`resolve_slskd_local_path`) still wins.
- **Phase 2 gate**: a week of prod logs after deploy with zero `EVENT-PATH COMPARE .* match=False`; `resolver_miss=True` lines are evidence FOR the event stream, not mismatches. Then flip the move to prefer `file.local_path`.
- **Phase 3**: delete `resolve_slskd_local_path`, `_TICKS_SUFFIX`, size-match logic, their tests. Also wire `cancel_and_delete` to `DownloadDirectoryComplete` (`decode_download_directory_complete` already ships, vulture-whitelisted).
- Wire facts: `/api/v0/events` `type` param does NOT filter on slskd 0.24.5 (filter client-side); envelope `data` is a JSON string (double-decode); feed newest-first; ~389k events retained, no pruning observed. Cursor = (last_event_id, last_event_timestamp raw ISO string), 20-page catch-up cap with `cursor_gap` telemetry.
- Deliberately NOT done: retyping legacy dict shapes (downloads snapshot / search responses / browse dirs) into Structs — engine-wide (~850 tests), out of scope per the issue's own ~200-line client estimate.

**Why:** phases 2/3 need a future session to check the logs and finish; the gate command and semantics live in the 2026-07-02 comment on issue #146.
