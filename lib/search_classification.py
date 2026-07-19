"""Pure failure-class classification for completed search-plan cycles (U12).

When a request's cursor wraps (the cycle completes), we summarise the
searches that ran during that cycle into one of five buckets:

* ``A_zero_results_dominant`` — > 80% of consumed attempts returned
  ``no_results`` (Soulseek had nothing).
* ``B_cands_never_match`` — every consumed attempt produced candidates
  (i.e. ``no_results`` was a minority) but none of them matched
  (``found`` count is zero).
* ``D_found_but_no_import`` — at least one attempt was ``found`` but the
  request status is still ``wanted`` (the find never converted to an
  import, presumably blocked downstream).
* ``E_mixed`` — fits none of A/B/D cleanly.
* ``resolved`` — the request moved past ``wanted`` during the cycle (we
  see this when the request status is, e.g., ``imported``, ``downloading``,
  or ``unsearchable``).

The classifier is a pure function so the wrap-time DB writer can reuse it
and any future triage / dry-run surface (e.g. a PR4 inspector) can
re-classify on-the-fly from already-fetched ``search_log`` rows without
touching production code paths.

A cycle with **no consumed attempts** returns ``None`` so the caller can
preserve any previously-classified ``failure_class`` instead of
overwriting it with a degenerate "no signal" verdict. The DB writer
treats ``None`` as "leave the column alone".

See:
* ``docs/brainstorms/2026-05-25-search-plan-iteration-2-requirements.md``
  (R28 — failure-class taxonomy)
* ``docs/plans/2026-05-25-001-feat-search-plan-iteration-2-plan.md``
  (U12 — wrap-time materialisation)
"""

from __future__ import annotations

from dataclasses import dataclass

# Failure-class constants. These mirror the CHECK constraint on
# ``album_requests.failure_class`` (migration 028).
FAILURE_CLASS_A_ZERO_RESULTS_DOMINANT = "A_zero_results_dominant"
FAILURE_CLASS_B_CANDS_NEVER_MATCH = "B_cands_never_match"
FAILURE_CLASS_D_FOUND_BUT_NO_IMPORT = "D_found_but_no_import"
FAILURE_CLASS_E_MIXED = "E_mixed"
FAILURE_CLASS_RESOLVED = "resolved"

ALL_FAILURE_CLASSES: tuple[str, ...] = (
    FAILURE_CLASS_A_ZERO_RESULTS_DOMINANT,
    FAILURE_CLASS_B_CANDS_NEVER_MATCH,
    FAILURE_CLASS_D_FOUND_BUT_NO_IMPORT,
    FAILURE_CLASS_E_MIXED,
    FAILURE_CLASS_RESOLVED,
)

# Status indicating the request never converted to a non-wanted state.
# Used to disambiguate "found but no import" (D) from "resolved".
_STATUS_WANTED = "wanted"

# Search-log outcome strings the classifier reasons about. Mirrors the
# CHECK constraint on ``search_log.outcome``.
#
# ``no_match`` is the only outcome where the matcher saw candidates and
# rejected them — it is the positive evidence for B. ``timeout`` /
# ``error`` / ``empty_query`` rows produced no candidates at all and
# fall into E rather than B (B is specifically "candidates existed,
# none matched", not "no found and no zero-results").
_OUTCOME_FOUND = "found"
_OUTCOME_NO_RESULTS = "no_results"
_OUTCOME_NO_MATCH = "no_match"

# Strict inequality boundary for A: a cycle dominates with zero-results
# only when the ratio exceeds 80%. Exactly 80% does NOT trip A so the
# bucket reflects a genuinely overwhelming "Soulseek had nothing"
# pattern rather than a 4-of-5 borderline.
_ZERO_RESULTS_DOMINANT_THRESHOLD = 0.8


@dataclass(frozen=True)
class SearchSummary:
    """One consumed search attempt's classification signal.

    Pure-internal type — constructed by the DB layer from
    ``search_log`` rows (reading the ``outcome`` and ``rejection_reason``
    columns) and consumed in-process by ``classify_failure_class``. Never
    crosses a wire boundary, so plain ``@dataclass(frozen=True)`` is the
    right tool here per `.claude/rules/code-quality.md` § "Wire-boundary
    types". Keeping the type narrow protects callers from accidentally
    pulling other forensics columns into the classifier's contract —
    only these two fields drive the verdict today.

    The classifier only sees consumed attempts (``attempt_consumed =
    TRUE``). Stale completions and pre-attempt failures are filtered
    out before construction so the bucket reflects what the plan
    actually achieved, not what the executor tried and discarded.
    """

    outcome: str
    rejection_reason: str | None = None


def classify_failure_class(
    searches: list[SearchSummary],
    *,
    current_status: str,
) -> str | None:
    """Return the failure-class bucket for one completed cycle, or ``None``.

    ``searches`` is the list of consumed attempts from the cycle that
    just wrapped. ``current_status`` is the request's status at
    classification time (read from ``album_requests.status`` inside
    the same transaction as the wrap).

    Returns one of the five ``FAILURE_CLASS_*`` constants, or ``None``
    when the cycle produced no signal (empty ``searches``) and the
    classifier should leave the existing value alone.

    Decision order (intentional — earlier branches dominate):

    1. ``current_status != 'wanted'``  → ``resolved`` (the cycle was
       overtaken by an import / operator search stop / download).
    2. Empty ``searches``               → ``None`` (no signal).
    3. ``found`` count >= 1 (and still ``wanted``)
                                        → ``D_found_but_no_import``.
    4. ``no_results`` ratio > 0.80      → ``A_zero_results_dominant``.
    5. Every consumed attempt was ``no_match`` (candidates existed,
       none matched)                  → ``B_cands_never_match``.
    6. Otherwise                       → ``E_mixed``.
    """
    # Branch 1: the cycle was resolved mid-flight. We trust the status
    # rather than searching for the import telemetry — once the request
    # has moved past ``wanted`` the cycle's outcome is, by definition,
    # resolved regardless of how it got there.
    if current_status != _STATUS_WANTED:
        return FAILURE_CLASS_RESOLVED

    # Branch 2: no signal — leave the column alone.
    if not searches:
        return None

    total = len(searches)
    found_count = sum(1 for s in searches if s.outcome == _OUTCOME_FOUND)
    no_results_count = sum(
        1 for s in searches if s.outcome == _OUTCOME_NO_RESULTS
    )
    no_match_count = sum(
        1 for s in searches if s.outcome == _OUTCOME_NO_MATCH
    )

    # Branch 3: at least one ``found`` but the request hasn't converted.
    # This is the "we located it but couldn't import" signature.
    if found_count >= 1:
        return FAILURE_CLASS_D_FOUND_BUT_NO_IMPORT

    # Branch 4: Soulseek dominantly had nothing for these queries.
    if (no_results_count / total) > _ZERO_RESULTS_DOMINANT_THRESHOLD:
        return FAILURE_CLASS_A_ZERO_RESULTS_DOMINANT

    # Branch 5: every consumed attempt was ``no_match``. The matcher
    # saw candidates on every search and rejected them all — that's
    # the B signature. ``rejection_reason`` may vary across rows
    # (strict_count_mismatch, avg_ratio_low, ...) but they all
    # aggregate here by the same "candidates existed, none matched"
    # pattern. We require ``no_match_count == total`` rather than just
    # "no zero-results", because a cycle full of ``timeout`` /
    # ``error`` rows had no candidates either and is more honestly
    # described as E_mixed (something went wrong, not "we have
    # rejected matches").
    if no_match_count == total:
        return FAILURE_CLASS_B_CANDS_NEVER_MATCH

    # Branch 6: any other shape — mixed no_results + no_match without
    # dominance, pure timeouts, error-heavy cycles, etc.
    return FAILURE_CLASS_E_MIXED
