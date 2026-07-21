"""GENERATED baseline for the strict-coverage ratchet (#784).

Do not edit counts by hand. Regenerate after reducing strict
errors with:

    nix-shell --run "python3 -m tests._strict_ratchet_scanner" \
        > tests/_strict_ratchet_baseline.py
"""

STRICT_RATCHET_BASELINE: dict[str, int] = {
    "lib/audio_hash.py": 3,
    "lib/beets_db.py": 3,
    "lib/beets_delete.py": 9,
    "lib/browse.py": 3,
    "lib/context.py": 12,
    "lib/disk_coverage_service.py": 1,
    "lib/dispatch/outcome_actions.py": 1,
    "lib/dispatch/post_import.py": 3,
    "lib/dispatch/quality_gate.py": 2,
    "lib/dispatch/types.py": 1,
    "lib/download_reconstruction.py": 3,
    "lib/download_rejection.py": 9,
    "lib/import_queue.py": 2,
    "lib/matching.py": 9,
    "lib/measurement.py": 6,
    "lib/peer_cache.py": 1,
    "lib/search_exec.py": 1,
    "lib/search_plan_service.py": 9,
    "lib/terminal_outcomes.py": 1,
    "lib/transitions.py": 1,
    "lib/unfindable_detection_service.py": 3,
    "lib/v0_probe.py": 1,
    "lib/validation_envelope.py": 1,
    "lib/world_invariants.py": 1,
    "lib/wrong_match_cleanup_service.py": 1,
}
