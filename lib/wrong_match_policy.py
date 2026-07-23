"""Neutral rejection taxonomy for the operator's Wrong Matches worklist.

Wrong Matches is specifically a candidate/pressing-match review surface.
Folder/audio-integrity facts and spectral-quality rejects have separate
recovery paths, so they must not appear in the worklist or enter its automatic
cleanup path.
"""

from __future__ import annotations


WRONG_MATCH_QUARANTINE_DIR = "wrong_matches"


PREIMPORT_FACT_REJECTION_SCENARIOS: frozenset[str] = frozenset({
    "audio_corrupt",
    "bad_audio_hash",
    "nested_layout",
    "empty_fileset",
    "mixed_source",
})

WRONG_MATCH_EXCLUDED_REJECTION_SCENARIOS: frozenset[str] = frozenset({
    *PREIMPORT_FACT_REJECTION_SCENARIOS,
    "spectral_reject",
})


def rejection_scenario_is_wrong_match_candidate(scenario: str | None) -> bool:
    """Return whether a rejected candidate belongs in pressing-match review."""
    return scenario not in WRONG_MATCH_EXCLUDED_REJECTION_SCENARIOS
