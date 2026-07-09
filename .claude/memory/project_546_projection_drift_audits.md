---
name: project_546_projection_drift_audits
description: Issue
metadata: 
  node_type: memory
  type: project
  originSessionId: bb231f0d-996e-4ed4-8cad-72394343526b
---

Issue #546 (batched hygiene/correctness follow-ups from #522/#523) has 4 work items, each its own PR, built via the [[project_501_510_refactor_batch]] strategy (sonnet-implement review-banned → opus-review-loop → orchestrator-verify → merge).

- **W1 — self-enforcing read-projection parity audit: SHIPPED 2026-07-08 as draft PR #551** (branch `worktree-issue-546-w1-read-parity`). Built `tests/read_projection_registry.py` (`enumerate_read_mirrors()` + `PARITY_REGISTRY` 29 seeders + `ALLOWLIST` 25), `TestReadProjectionRegistryParity` (real-PG driver in `tests/test_pipeline_db.py`), and `tests/test_read_projection_audit.py` (self-enforcing completeness — every FakePipelineDB `get_/list_/search_/find_/fetch_` mirror ∈ exactly one bucket). Partition: 67 = 29 registry + 25 allowlist + 13 hand-covered. Surfaced+fixed ONE drift (fake side): `get_search_plan_stats_history` leaked 8 U11 forensics cols; narrowed to production's 26-col projection. Reviewers caught a drift-hiding allowlist entry (`get_legacy_search_log_summary` = real 9-col projection) + a universe gap (`search_requests` = live-route `SELECT *`) — both fixed. Extends [[feedback_test_fidelity_meta_pattern]] Rule A to reads.
- **W2 — write-side coverage audit: ALREADY DONE.** The issue says "still not built," but it exists as `tests/test_pipeline_db_write_audit.py` (shipped 2026-06-12, predates the issue). Nothing to build; don't rebuild it.
- **W4 — `web/routes/pipeline.py` split → `pipeline_mutations.py`: SHIPPED + MERGED 2026-07-08 as PR #552.** 8 `post_pipeline_*` handlers + 8 Pydantic models + `finalize_request`/`mb_api`/`discogs_api`/`hash_audio_content`/`resolve_failed_path` DI seams moved (byte-identical); pipeline.py 1590→518. Contract tests already lived in `tests/web/test_routes_pipeline_mutations.py`; retargeted `@patch` targets + `_mock_audit_scanner.py` regexes to the new module. `_serialize_import_job` stayed in pipeline.py (shared, imported back).
- **W3 — Struct-typed `upsert_youtube_album_mapping` write interface: NOT STARTED (scoped, ready to dispatch).** Smaller than the issue feared — only ONE `upsert_*` still takes `list[dict]` (`upsert_album_quality_evidence` already takes a Struct, `upsert_slskd_event_cursor` is scalar). The `PersistedYoutubeRow` msgspec.Struct ALREADY EXISTS (`lib/youtube_album_service.py:367`, already used on the READ path via `msgspec.convert` at :1457). W3 = change `upsert_youtube_album_mapping(rows: list[dict])` → `list[PersistedYoutubeRow]`, derive the INSERT column list from Struct fields (album_title-class bug becomes impossible), update the caller construction (`youtube_album_service.py` ~813-848 builds dicts; empty caller :649 stays `[]`) + the fake, and extend `tests/test_pipeline_db_column_contract.py` (assert Struct fields ⊆ live `youtube_album_mappings` columns).
- **Deploy: NOT DONE.** Plan is ONE deploy at the very end after W3 merges (W1+W4+W3 together). W1 (#551) + W4 (#552) are merged to main but NOT yet deployed to doc2.
