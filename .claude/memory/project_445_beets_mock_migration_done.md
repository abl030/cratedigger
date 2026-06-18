---
name: project-445-beets-mock-migration-done
description: "2026-06-12, PRs"
metadata: 
  node_type: memory
  type: project
  originSessionId: 3f922cce-be9b-4361-9d6d-060aff555a44
---

Issue #445 items 1 and 4 shipped 2026-06-12 (PRs #446, #447, #448, all merged):

- `WEB_BEETS_MOCK_BASELINE` in `tests/_mock_audit_scanner.py` is permanently EMPTY — tests/web has zero beets-collaborator MagicMocks. Migrated fakes are named `self.beets_db` (the ratchet counts `mock_beets*`/`self._beets`/`self.beets`, not `beets_db`).
- `FakePipelineDB` enforces UNIQUE(mb_release_id) (seed/add/update paths) and sequence-faithful download_log id minting. `make_request_row` default mbid is id-derived (`test-mbid-{id:04d}`; id=1 keeps the old literal).
- `FakeBeetsDB` now covers the web-route surface: `get_album_ids_by_mbids`, `get_tracks_by_mb_release_id`, `search_albums`, `get_recent`, `get_album_detail`, `get_min_bitrate`, and `locate` with a typed `queue_locate_results` API (rejects production-impossible ReleaseLocations). `album_exists`/`get_min_bitrate` route through one `_presence` helper (queued locate head → explicit seed → album-ids store → default), mirroring production's single locate seam (issue #121).

**Why:** the dual-review pattern (adversarial agent + background Codex per batch) again found disjoint things — keep using both; Codex reads production SQL/route code against fake mirrors, the adversarial agent finds ratchet evasions and impossible-state expressibility.

**How to apply:** items 2 and 5 also shipped 2026-06-12 (PRs #449 deployed live, #450): `PipelineDB.get_download_log_counts()` + `get_pipeline_overlay()` replaced the inline route/overlay SQL (FakeCursor queueing deleted from those consumers), and `tests/fakes` is now a package (`pipeline_db`/`beets`/`slskd`/`ytmusic`/`lookups`/`cursors`/`rows`/`_shared` — flat `from tests.fakes import X` surface preserved; patch `tests.fakes.pipeline_db._utcnow`, not the package binding). Item 3 (dual-module-load) shipped 2026-06-12 in PR #451 — issue #445 is fully closed; see [[project-445-complete-dual-load-killed]]. Dashboard shared-helpers bullet deliberately parked as low-urgency.
