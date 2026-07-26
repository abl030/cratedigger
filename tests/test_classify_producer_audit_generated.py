#!/usr/bin/env python3
"""Generated properties for ``web/classify.py``'s decision copy (issue #882).

The pins in ``tests/test_classify_producer_audit.py`` prove the exact
scenarios; these properties patrol the world space around them, per
``.claude/rules/code-quality.md`` § "Pin+fuzz PAIR rule".

Invariants:

C1. **Every string that renders a decision claim is true of what the
    PRODUCER actually does with that decision.** The expectations are
    derived from ``lib.quality.dispatch_action`` and
    ``lib.quality.dispatch_actions.decision_denylists`` — the real
    importer's own policy — never from the presenter's copy, so the two
    must independently agree. Applied to the verdict AND the summary, and
    through both renderers of the same claim (``_rejection_verdict`` and
    ``_quality_verdict_from_import_result``): issue #868's lesson is that
    a retracted claim survives in the OTHER renderer and in the adjective
    form ("could not read" fixed, "unreadable" shipped), so a
    verdict-scoped check proves nothing.

C2. **A decision name no producer emits is never rewritten into a
    different FACT.** It reaches the operator as the token itself, spelled
    for a human. This is the property twin of the producer audit: the
    audit proves no fabricated literal is matched today, this proves the
    module does not manufacture a sentence for one tomorrow. It is the
    invariant ``no_candidates`` violated — fluent copy for a string
    nothing produces, while the producing string (``mbid_not_found``, 50
    live rows) fell through to the raw token.

    The comparison is against an INDEPENDENT restatement of the
    presentation-only transform (``humanized``), not against
    ``_humanize_token`` itself; comparing production to production would
    make the property follow a producer that started inventing.

C3. **A decision name classify DOES match renders words, not its own raw
    token.** The other half of the same defect.

Known limitation (issue #882 review N6): C1's forbidden vocabulary is a
DENYLIST of phrasings, so a novel wording of a false claim — "…; the hunt
goes on" for a proof-locked request — escapes it. That is inherent to
checking prose; the backstop is the exact-string pins in
``tests/test_classify_producer_audit.py``, which fail the moment the
sentence changes at all. Widen the denylist when a new phrasing ships;
do not mistake it for a semantic check.

Every checker is a module-level function returning a violation string (or
``None``), so ``TestInvariantCheckersTripOnViolations`` can call it
directly with a planted violation.
"""

from __future__ import annotations

import os
import sys
import unittest

import msgspec
from hypothesis import assume, example, given, strategies as st

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import tests._hypothesis_profiles  # noqa: F401  (loads the active profile)

from lib.quality import dispatch_action
from lib.quality.dispatch_actions import decision_denylists
from tests.helpers import make_import_result
from tests.test_classify_producer_audit import classify_match_targets
from web.classify import LogEntry, classify_log_entry

# ---------------------------------------------------------------------------
# Worlds — derived from the audit's own discovery, never hand-listed
# ---------------------------------------------------------------------------

# The subjects whose literals are pipeline decision names. Each is a key
# the producer audit has already traced to a producing module; taking the
# world space from the same discovery means a new decision literal enters
# these properties the moment it enters the module.
DECISION_SUBJECTS = (
    "scenario",
    "ir.decision",
    "_entry_decision(entry)",
    "_entry_rejection_decision(entry)",
    "entry.beets_scenario",
    "triage_preview_decision",
)


def classify_decision_literals() -> tuple[str, ...]:
    """Every decision name ``web/classify.py`` claims to render copy for."""
    targets = classify_match_targets()
    return tuple(sorted({
        literal
        for subject in DECISION_SUBJECTS
        for literal in targets.get(subject, ())
    }))


DECISION_LITERALS = classify_decision_literals()
REJECTION_SCENARIO_LITERALS = tuple(sorted(
    classify_match_targets().get("scenario", ())))

# Peer names from an alphabet no claim word contains, so "does the summary
# repeat a claim?" stays a decidable question.
_PEER_NAMES = st.text(alphabet="QXZJKVW0123456789", min_size=5, max_size=12)


# ---------------------------------------------------------------------------
# Claim families — derived from the producer's action, not from the copy
# ---------------------------------------------------------------------------

FAMILY_ACQUISITION_COMPLETE = "acquisition_complete"
FAMILY_SEARCH_CONTINUES = "search_continues"
FAMILY_DENYLISTS_THE_SOURCE = "denylists_the_source"
FAMILY_LEAVES_THE_SOURCE_ALONE = "leaves_the_source_alone"
FAMILY_ACCEPTED_THE_CANDIDATE = "accepted_the_candidate"


def decision_claim_families(decision: str) -> frozenset[str]:
    """The claim families a decision belongs to, per the PRODUCER.

    ``dispatch_action`` is what the importer really does; whether the
    request stays imported and whether the source is banned are facts, not
    presentation choices, so the copy has to agree with them.
    """
    action = dispatch_action(decision)
    families: set[str] = set()
    if action.preserve_imported:
        families.add(FAMILY_ACQUISITION_COMPLETE)
    elif action.record_rejection:
        families.add(FAMILY_SEARCH_CONTINUES)
    if action.mark_done:
        families.add(FAMILY_ACCEPTED_THE_CANDIDATE)
    if decision_denylists(decision):
        families.add(FAMILY_DENYLISTS_THE_SOURCE)
    else:
        families.add(FAMILY_LEAVES_THE_SOURCE_ALONE)
    return frozenset(families)


# Each family declares the words it may NOT use, paired with the member
# that contradicts each one. Grammatical variants are listed separately and
# deliberately: "could not read" was fixed in one renderer while the
# adjective "unreadable" shipped in another (issue #868 review F1), so a
# family that bans a claim bans every form the module can spell it in.
FORBIDDEN_FAMILY_CLAIMS: dict[str, tuple[tuple[str, str], ...]] = {
    FAMILY_ACQUISITION_COMPLETE: (
        ("searching continues",
         "verified_lossless_locked sets preserve_imported: the request stays "
         "imported and no search is open"),
        ("searching for better", "the participle form of the same claim"),
        ("still searching", "the progressive form of the same claim"),
        ("returned to the queue",
         "nothing is requeued when the import is preserved"),
    ),
    FAMILY_SEARCH_CONTINUES: (
        ("acquisition is complete",
         "downgrade returns the request to wanted — the search is still open"),
        ("acquisition complete", "the bare-adjective form of the same claim"),
        ("no further search", "the negative form of the same claim"),
    ),
    FAMILY_LEAVES_THE_SOURCE_ALONE: (
        ("denylisted",
         "nested_layout is a folder-shape reject: dispatch_action bans nobody"),
        ("denylisting", "the gerund form of the same claim"),
        ("source banned", "the plain-English form of the same claim"),
    ),
    FAMILY_DENYLISTS_THE_SOURCE: (
        ("no denylist",
         "downgrade denylists the source, so the reassuring parenthetical "
         "verified_lossless_locked earns would be false here"),
        ("without denylisting", "the participle form of the same claim"),
        ("source kept", "the plain-English form of the same claim"),
    ),
    FAMILY_ACCEPTED_THE_CANDIDATE: (
        ("not better than",
         "transcode_upgrade sets mark_done: the candidate was imported, not "
         "out-ranked"),
        ("declined", "the passive form of the same retracted claim"),
        ("rejected", "the blunt form of the same retracted claim"),
    ),
}


def check_decision_claim(
    decision: str,
    claim: str,
    *,
    where: str,
) -> str | None:
    """C1 — any string that speaks for a decision must be action-true.

    ``claim`` is deliberately not "the verdict": the same sentence reaches
    the operator through ``_rejection_verdict``, through
    ``_quality_verdict_from_import_result``, and again through the
    collapsed summary line — which is the line they actually read. Every
    string that speaks for the decision answers here.
    """
    lowered = claim.casefold()
    for family in sorted(decision_claim_families(decision)):
        for forbidden, why in FORBIDDEN_FAMILY_CLAIMS.get(family, ()):
            if forbidden in lowered:
                return (
                    f"{where} for {decision} ({family}) claims "
                    f"{forbidden!r} ({why}): {claim!r}"
                )
    return None


def humanized(token: str) -> str:
    """The presentation-only transform, restated independently.

    Deliberately not a call to ``web.classify._humanize_token``: a checker
    that asks production what production should have said cannot catch
    production inventing. Separators become spaces; nothing else changes,
    so no new fact can enter.
    """
    return token.replace("_", " ").replace("-", " ").strip()


def check_unproduced_name_is_not_rewritten(
    scenario: str,
    verdict: str,
) -> str | None:
    """C2 — copy is invented only for names a producer actually emits.

    The defect this patrols: a branch keyed on a string no producer emits
    replaced the operator's own evidence with a different, wrong fact. The
    bar is the FACT, not the exact bytes — spelling ``import_failed`` as
    "import failed" adds no claim, which is why this compares against the
    independent ``humanized`` restatement rather than the raw token
    (issue #882 review N1).
    """
    if verdict != humanized(scenario):
        return (
            f"an unregistered decision name was rewritten: {scenario!r} -> "
            f"{verdict!r}"
        )
    return None


def check_matched_name_renders_words(
    scenario: str,
    verdict: str,
) -> str | None:
    """C3 — a name the module matches renders copy, not the token itself.

    A matched name is one the module claims to have something to say
    about; falling through to the humanized token means the claim is
    missing (``mbid_not_found``, 50 live rows, before #882).
    """
    if verdict.strip() in (scenario, humanized(scenario)):
        return (
            f"{scenario!r} is matched by the module but still renders as its "
            f"own machine token: {verdict!r}"
        )
    return None


# ---------------------------------------------------------------------------
# World builders
# ---------------------------------------------------------------------------

def _entry_for(
    decision: str,
    *,
    outcome: str,
    username: str | None = None,
    with_import_result: bool = False,
) -> LogEntry:
    import_result = (
        msgspec.to_builtins(make_import_result(decision=decision))
        if with_import_result
        else None
    )
    return LogEntry(
        id=1,
        request_id=2,
        outcome=outcome,
        beets_scenario=decision,
        soulseek_username=username,
        import_result=import_result,
    )


def _outcomes_for(decision: str) -> tuple[str, ...]:
    """The download_log outcomes this decision can actually be logged under."""
    action = dispatch_action(decision)
    if action.mark_done:
        return ("success",)
    # A rejection is recorded as ``rejected``; the import-phase failures
    # that carry the same decision are logged as ``failed`` and reach the
    # SECOND renderer of the same claim.
    return ("rejected", "failed")


# ---------------------------------------------------------------------------
# Properties
# ---------------------------------------------------------------------------

class TestDecisionClaimsMatchTheProducersAction(unittest.TestCase):
    """C1 — issue #882 item 4."""

    def test_the_world_space_is_derived_and_non_trivial(self) -> None:
        self.assertGreater(len(DECISION_LITERALS), 10)
        self.assertIn("verified_lossless_locked", DECISION_LITERALS)
        self.assertIn("provisional_lossless_upgrade", DECISION_LITERALS)

    @given(
        decision=st.sampled_from(DECISION_LITERALS),
        data=st.data(),
        username=st.one_of(st.none(), _PEER_NAMES),
        with_import_result=st.booleans(),
    )
    @example(
        decision="verified_lossless_locked", data=None,
        username="QXZJK", with_import_result=False,
    )
    @example(
        decision="lossless_source_locked", data=None,
        username=None, with_import_result=True,
    )
    @example(
        decision="provisional_lossless_upgrade", data=None,
        username="QXZJK", with_import_result=True,
    )
    def test_no_rendered_claim_contradicts_the_producers_action(
        self,
        decision: str,
        data: st.DataObject | None,
        username: str | None,
        with_import_result: bool,
    ) -> None:
        outcomes = _outcomes_for(decision)
        chosen = (
            outcomes if data is None else (data.draw(st.sampled_from(outcomes)),)
        )
        for outcome in chosen:
            classified = classify_log_entry(_entry_for(
                decision,
                outcome=outcome,
                username=username,
                with_import_result=with_import_result,
            ))
            for where, claim in (
                (f"{outcome} verdict", classified.verdict),
                (f"{outcome} summary", classified.summary),
            ):
                violation = check_decision_claim(decision, claim, where=where)
                self.assertIsNone(violation, violation)

    @given(
        decision=st.sampled_from(DECISION_LITERALS),
        username=st.one_of(st.none(), _PEER_NAMES),
    )
    def test_the_collapsed_summary_carries_the_verdict(
        self, decision: str, username: str | None,
    ) -> None:
        """The list row is the line the operator reads (issue #868 #12)."""
        for outcome in _outcomes_for(decision):
            classified = classify_log_entry(
                _entry_for(decision, outcome=outcome, username=username))
            if classified.badge == "Imported":
                continue
            self.assertIn(classified.verdict, classified.summary)


class TestUnproducedNamesAreNeverRewritten(unittest.TestCase):
    """C2 — the property twin of the producer audit."""

    @given(scenario=st.text(min_size=1, max_size=60))
    @example(scenario="no_candidates")
    @example(scenario="stale_path_cleared")
    @example(scenario="extra_tracks")
    def test_an_unmatched_decision_name_reaches_the_operator_unedited(
        self, scenario: str,
    ) -> None:
        assume(scenario not in DECISION_LITERALS)
        assume(scenario.strip() == scenario and scenario.strip())
        # A token that humanizes away entirely ("___") legitimately falls
        # back to the generic "Rejected"; there is no fact to preserve.
        assume(humanized(scenario))
        classified = classify_log_entry(
            LogEntry(id=1, request_id=2, outcome="rejected",
                     beets_scenario=scenario))
        violation = check_unproduced_name_is_not_rewritten(
            scenario, classified.verdict)
        self.assertIsNone(violation, violation)


class TestMatchedNamesRenderWords(unittest.TestCase):
    """C3 — the other half of the ``no_candidates`` defect."""

    @given(scenario=st.sampled_from(REJECTION_SCENARIO_LITERALS))
    @example(scenario="mbid_not_found")
    def test_every_matched_rejection_scenario_renders_a_sentence(
        self, scenario: str,
    ) -> None:
        classified = classify_log_entry(
            LogEntry(id=1, request_id=2, outcome="rejected",
                     beets_scenario=scenario))
        violation = check_matched_name_renders_words(
            scenario, classified.verdict)
        self.assertIsNone(violation, violation)


# ---------------------------------------------------------------------------
# Known-bad self-tests — a checker that cannot fail proves nothing
# ---------------------------------------------------------------------------

class TestInvariantCheckersTripOnViolations(unittest.TestCase):

    def test_family_derivation_follows_the_producer(self) -> None:
        self.assertIn(
            FAMILY_ACQUISITION_COMPLETE,
            decision_claim_families("verified_lossless_locked"),
        )
        self.assertIn(
            FAMILY_LEAVES_THE_SOURCE_ALONE,
            decision_claim_families("verified_lossless_locked"),
        )
        self.assertIn(
            FAMILY_SEARCH_CONTINUES, decision_claim_families("downgrade"))
        self.assertIn(
            FAMILY_DENYLISTS_THE_SOURCE, decision_claim_families("downgrade"))
        self.assertIn(
            FAMILY_ACCEPTED_THE_CANDIDATE,
            decision_claim_families("transcode_upgrade"),
        )

    def test_claim_checker_trips_on_a_lock_that_claims_to_keep_searching(self):
        self.assertIsNotNone(check_decision_claim(
            "verified_lossless_locked",
            "Verified lossless already on disk; searching continues",
            where="planted verdict",
        ))
        # …and on the participle form that would survive a literal fix.
        self.assertIsNotNone(check_decision_claim(
            "verified_lossless_locked",
            "Verified lossless already on disk; searching for better",
            where="planted verdict",
        ))
        self.assertIsNone(check_decision_claim(
            "verified_lossless_locked",
            "Verified lossless already on disk; automatic candidate declined "
            "(no denylist); acquisition is complete",
            where="real verdict",
        ))

    def test_claim_checker_trips_on_a_reject_that_claims_completion(self) -> None:
        self.assertIsNotNone(check_decision_claim(
            "downgrade",
            "Quality not better than on-disk copy; acquisition is complete",
            where="planted verdict",
        ))

    def test_claim_checker_trips_on_both_denylist_claims(self) -> None:
        self.assertIsNotNone(check_decision_claim(
            "nested_layout",
            "Nested folder layout; source denylisted",
            where="planted verdict",
        ))
        self.assertIsNotNone(check_decision_claim(
            "downgrade",
            "Quality not better than on-disk copy (no denylist)",
            where="planted verdict",
        ))
        self.assertIsNone(check_decision_claim(
            "downgrade",
            "Quality not better than on-disk copy; searching continues",
            where="real verdict",
        ))

    def test_claim_checker_trips_on_an_accepted_candidate_called_rejected(self):
        self.assertIsNotNone(check_decision_claim(
            "transcode_upgrade",
            "Transcode at 245kbps — rejected as not better than on-disk copy",
            where="planted verdict",
        ))
        self.assertIsNone(check_decision_claim(
            "transcode_upgrade",
            "Transcode at 245kbps — imported as upgrade, searching for better",
            where="real verdict",
        ))

    def test_claim_checker_reads_the_summary_too(self) -> None:
        """The verdict-only version of this check would pass here."""
        self.assertIsNotNone(check_decision_claim(
            "verified_lossless_locked",
            "Verified lossless already on disk · searching continues · QXZJK",
            where="planted summary",
        ))

    def test_passthrough_checker_trips_on_a_manufactured_sentence(self) -> None:
        self.assertIsNotNone(check_unproduced_name_is_not_rewritten(
            "no_candidates", "No MusicBrainz match found"))
        # Spelling the token for a human adds no fact, so it passes …
        self.assertIsNone(check_unproduced_name_is_not_rewritten(
            "no_candidates", "no candidates"))
        # … while any other sentence, however plausible, does not.
        self.assertIsNotNone(check_unproduced_name_is_not_rewritten(
            "no_candidates", "Rejected: no usable candidate was found"))

    def test_the_humanize_restatement_is_independent_of_production(self) -> None:
        """N1: comparing production to production proves nothing."""
        self.assertEqual(humanized("quality_evidence_action_failed"),
                         "quality evidence action failed")
        self.assertEqual(humanized("exception"), "exception")
        self.assertEqual(humanized("stale-path"), "stale path")
        self.assertEqual(humanized("  x_y  "), "x y")

    def test_raw_token_checker_trips_on_the_other_half_of_the_defect(self) -> None:
        self.assertIsNotNone(check_matched_name_renders_words(
            "mbid_not_found", "mbid_not_found"))
        # The humanized token is still no claim — a matched name owes copy.
        self.assertIsNotNone(check_matched_name_renders_words(
            "mbid_not_found", "mbid not found"))
        self.assertIsNone(check_matched_name_renders_words(
            "mbid_not_found",
            "Requested release ID not among the match candidates"))


if __name__ == "__main__":
    unittest.main()
