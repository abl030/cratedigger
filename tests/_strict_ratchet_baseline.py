"""GENERATED baseline for the strict-coverage ratchet (#784).

Do not edit counts by hand. Regenerate after reducing strict
errors with:

    nix-shell --run "python3 -m tests._strict_ratchet_scanner" \
        > tests/_strict_ratchet_baseline.py
"""

STRICT_RATCHET_BASELINE: dict[str, int] = {
    "harness/beets_harness.py": 1,
    "lib/audio_hash.py": 3,
    "lib/beets_db.py": 3,
    "lib/beets_delete.py": 9,
    "lib/browse.py": 3,
    "lib/config.py": 35,
    "lib/context.py": 12,
    "lib/disk_coverage_service.py": 1,
    "lib/dispatch/outcome_actions.py": 1,
    "lib/dispatch/post_import.py": 3,
    "lib/dispatch/quality_gate.py": 2,
    "lib/dispatch/types.py": 1,
    "lib/download_reconstruction.py": 3,
    "lib/download_rejection.py": 9,
    "lib/field_resolver_service.py": 62,
    "lib/import_preview.py": 1,
    "lib/import_queue.py": 2,
    "lib/matching.py": 9,
    "lib/measurement.py": 6,
    "lib/peer_cache.py": 1,
    "lib/quality/__init__.py": 2,
    "lib/quality/decisions.py": 8,
    "lib/quality/download_state.py": 6,
    "lib/quality/evidence_types.py": 1,
    "lib/quality/import_result_types.py": 67,
    "lib/quality/pipeline.py": 77,
    "lib/quality/wire_types.py": 4,
    "lib/search.py": 51,
    "lib/search_exec.py": 1,
    "lib/search_plan_inspection.py": 53,
    "lib/search_plan_service.py": 9,
    "lib/terminal_outcomes.py": 1,
    "lib/transitions.py": 1,
    "lib/unfindable_detection_service.py": 3,
    "lib/v0_probe.py": 1,
    "lib/validation_envelope.py": 1,
    "lib/world_invariants.py": 1,
    "lib/wrong_match_cleanup_service.py": 1,
    "lib/youtube_album_service.py": 146,
    "lib/youtube_ingest_service.py": 14,
    "scripts/pipeline_cli/quality.py": 1,
    "tools/generate-ai-adapters.py": 14,
}
