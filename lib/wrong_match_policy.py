"""Neutral rejection taxonomy for the operator's Wrong Matches worklist.

Wrong Matches is specifically a candidate/pressing-match review surface.
Folder/audio-integrity facts and spectral-quality rejects have separate
recovery paths, so they must not appear in the worklist or enter its automatic
cleanup path.

Two distinct questions live here, answered by two distinct predicates
(issue #1077, D1/D4/D6):

- **Worklist visibility** (``rejection_scenario_is_wrong_match_candidate``):
  does a quarantined folder belong in the operator's Wrong Matches review
  surface? This stays a small exclusion set — everything is visible except
  the folder/audio-integrity facts and the quality-only spectral reject,
  none of which quarantine with a reviewable folder any more (D3). The
  predicate still matters for the historical cohort of rows quarantined
  before that fix.
- **Cleanup-lane admission** (``rejection_scenario_is_delete_eligible``):
  may a kept, banned, visible folder be evaluated for deletion by
  ``lib.wrong_match_cleanup_service.cleanup_wrong_match``? This is an
  explicit allowlist, not a fail-open exclusion set: only a genuine
  candidate/pressing-match judgement may enter that lane. World failures
  with a reviewable folder (``untracked_audio``, ``request_missing_mbid``,
  ``request_missing_request_id``), every unknown/novel scenario string, and
  ``None`` are NEVER delete-eligible — they stay kept, banned, and visible
  until an operator acts, or until a different proof authorizes deletion
  (force-import success consuming its own source, or proven audio
  corruption — neither of which routes through this lane).
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

# D6 (issue #1077): the cleanup lane ("evaluate and possibly delete") is an
# explicit allowlist. Only these four genuine candidate/pressing-match
# judgements may reach `cleanup_wrong_match`. Derive this set from the
# producer audit in `docs/rejection-routing.md` before widening it — a new
# entry here is a decision that a fresh reducer evaluation may delete that
# class of folder, not a taxonomy convenience.
DELETE_ELIGIBLE_REJECTION_SCENARIOS: frozenset[str] = frozenset({
    "extra_tracks",
    "high_distance",
    "mbid_not_found",
    "no_choose_match",
})


def rejection_scenario_is_wrong_match_candidate(scenario: str | None) -> bool:
    """Return whether a rejected candidate belongs in pressing-match review.

    Governs Wrong Matches worklist visibility and quarantine target-directory
    placement. Does NOT govern cleanup-lane delete-eligibility — see
    ``rejection_scenario_is_delete_eligible``.
    """
    return scenario not in WRONG_MATCH_EXCLUDED_REJECTION_SCENARIOS


def rejection_scenario_is_delete_eligible(scenario: str | None) -> bool:
    """Return whether a scenario may enter the evaluate-and-possibly-delete
    cleanup lane (``lib.wrong_match_cleanup_service.cleanup_wrong_match``).

    Explicit allowlist: unknown/novel scenario strings and ``None`` default
    to False (never delete-eligible), not True. A folder that fails this
    check is never even looked at by the reducer; it stays kept, banned, and
    visible until an operator acts or a different proof (force-import
    success, proven corruption) authorizes its removal outside this lane.
    """
    return scenario in DELETE_ELIGIBLE_REJECTION_SCENARIOS
