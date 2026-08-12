"""Producer audit for Wrong Matches rejection-scenario classification.

Issue #1077, D6: the cleanup lane ("evaluate and possibly delete") is an
explicit allowlist (``lib.wrong_match_policy.DELETE_ELIGIBLE_REJECTION_
SCENARIOS``), not a fail-open exclusion set. That allowlist, and the
worklist-visibility exclusion set beside it
(``WRONG_MATCH_EXCLUDED_REJECTION_SCENARIOS``), are hand-written registries —
exactly the shape that goes stale silently when a new rejection scenario
ships without anyone updating them (`.claude/rules/test-fidelity.md` Rule C).

This module closes that gap the same way ``tests/test_classify_producer_
audit.py`` closed it for Recents copy: derive the set of scenario literals
production actually SPELLS at the two admission surfaces
(``lib.download_rejection._handle_rejected_result`` /
``_reject_request_auto_import``, fed by ``lib/beets.py`` and
``lib/download_validation.py``; and the force-import manifest guard,
``lib/dispatch/manifest_guard.py``, which writes its own ``failed_path``
directly) by introspection, and fail closed on anything discovered but not
classified into one of:

* ``DELETE_ELIGIBLE_REJECTION_SCENARIOS`` — may reach the cleanup reducer.
* ``WRONG_MATCH_EXCLUDED_REJECTION_SCENARIOS`` — folder/audio-integrity facts
  and the quality-only spectral reject; never quarantined at all (D3).
* ``VISIBLE_NOT_DELETE_ELIGIBLE`` — world failures with a reviewable folder:
  kept, banned, shown, but the reducer never even looks at them.
* ``NEVER_REACHES_ADMISSION`` — spelled by a producer, but structurally
  cannot reach either admission surface (``strong_match`` only fires on a
  VALID beets match, and the rejection helpers this audit's producers feed
  are reached only on an invalid one).
* ``HISTORICAL_SCENARIOS`` — no current writer; registered with the live
  evidence a reviewer can falsify.

Bounded and syntactic, per ``.claude/rules/code-quality.md`` § "Semantic
source scanners are prohibited": the scan recognises exactly two shapes
(``scenario=<literal>`` keyword arguments and ``<expr>.scenario =
<literal>`` attribute assignment) plus module-level ``*_SCENARIO`` string
constants, in a registered file list. It does not infer control flow, does
not decide reachability from arbitrary code shapes, and a discovery outside
those shapes is simply invisible to it — the same trade-off the classify
audit's docstring names explicitly.
"""

from __future__ import annotations

import ast
import functools
import os
import unittest
from collections.abc import Mapping, Sequence
from dataclasses import dataclass

from lib.wrong_match_policy import (
    DELETE_ELIGIBLE_REJECTION_SCENARIOS,
    WRONG_MATCH_EXCLUDED_REJECTION_SCENARIOS,
)

_REPO_ROOT = os.path.join(os.path.dirname(__file__), "..")

# The exact files this audit trusts to spell a Wrong-Match-admission-relevant
# scenario literal. Widening this list widens the evidence base for every
# literal classified here — see the module docstring.
_PRODUCER_FILES: tuple[str, ...] = (
    "lib/beets.py",
    "lib/download_validation.py",
    "lib/download_processing.py",
    "lib/dispatch/manifest_guard.py",
)

# World failures with a reviewable folder (issue #1077, D4): kept + banned +
# shown, but never delete-eligible. ``validation_error`` (beets itself
# errored), ``incomplete_fileset`` / ``unverifiable_source`` (the force-lane
# manifest guard's own two reject labels, distinct producer from the
# automation lane's ``untracked_audio``) share the same routing.
VISIBLE_NOT_DELETE_ELIGIBLE: frozenset[str] = frozenset({
    "untracked_audio",
    "request_missing_mbid",
    "request_missing_request_id",
    "validation_error",
    "incomplete_fileset",
    "unverifiable_source",
})

# Spelled by a registered producer, but structurally cannot reach either
# admission surface: ``lib/beets.py`` sets ``strong_match`` in the same
# statement it sets ``valid = True`` — the ONE place ``valid`` is ever set —
# so a `ValidationResult` carrying it can never be the rejected result
# `_handle_rejected_result` / `_reject_request_auto_import` receive.
NEVER_REACHES_ADMISSION: frozenset[str] = frozenset({"strong_match"})


@dataclass(frozen=True)
class _Historical:
    """A scenario a past revision could write; kept for live-row evidence."""

    source: str
    reason: str


HISTORICAL_SCENARIOS: dict[str, _Historical] = {
    "abandoned_auto_import": _Historical(
        source="download_log.beets_scenario",
        reason=(
            "historical interrupted-request auto-import cleanup rows; "
            "current exact-owner processing never writes this scenario. "
            "Only reader: lib/pipeline_db/misc.py:262 (a WHERE-clause "
            "exclusion, not a writer)."
        ),
    ),
}


# ---------------------------------------------------------------------------
# Discovery — bounded AST scan, plus module-level *_SCENARIO constants
# ---------------------------------------------------------------------------

class _ScenarioLiteralScan(ast.NodeVisitor):
    """The bounded grammar: ``scenario=`` kwargs and ``.scenario =`` writes."""

    def __init__(self) -> None:
        self.literals: set[str] = set()

    def visit_Call(self, node: ast.Call) -> None:
        for keyword in node.keywords:
            if (
                keyword.arg == "scenario"
                and isinstance(keyword.value, ast.Constant)
                and isinstance(keyword.value.value, str)
            ):
                self.literals.add(keyword.value.value)
        self.generic_visit(node)

    def visit_Assign(self, node: ast.Assign) -> None:
        if (
            isinstance(node.value, ast.Constant)
            and isinstance(node.value.value, str)
        ):
            for target in node.targets:
                if (
                    isinstance(target, ast.Attribute)
                    and target.attr == "scenario"
                ):
                    self.literals.add(node.value.value)
        self.generic_visit(node)


def scenario_literals_in_source(source: str) -> set[str]:
    """Every scenario literal one production file spells, by bounded grammar."""
    scan = _ScenarioLiteralScan()
    scan.visit(ast.parse(source))
    return scan.literals


@functools.cache
def _source(relpath: str) -> str:
    with open(os.path.join(_REPO_ROOT, relpath), encoding="utf-8") as handle:
        return handle.read()


def discovered_scenarios(relpaths: Sequence[str] = _PRODUCER_FILES) -> set[str]:
    """Every scenario literal spelled across the registered producer files."""
    found: set[str] = set()
    for relpath in relpaths:
        found |= scenario_literals_in_source(_source(relpath))
    return found


def module_scenario_constants(relpath: str) -> set[str]:
    """Module-level ``*_SCENARIO`` string constants (e.g. ``lib/beets.py``'s
    ``NO_CHOOSE_MATCH_SCENARIO`` / ``VALIDATION_ERROR_SCENARIO``) — spelled
    as a bare assignment, not a ``scenario=`` keyword, so the bounded Call/
    Attribute scan above cannot see them; this is the module-level half."""
    tree = ast.parse(_source(relpath))
    found: set[str] = set()
    for node in ast.walk(tree):
        if not isinstance(node, ast.Assign):
            continue
        if not (
            isinstance(node.value, ast.Constant)
            and isinstance(node.value.value, str)
        ):
            continue
        for target in node.targets:
            if isinstance(target, ast.Name) and target.id.endswith("_SCENARIO"):
                found.add(node.value.value)
    return found


def every_discovered_scenario() -> set[str]:
    scenarios = discovered_scenarios()
    scenarios |= module_scenario_constants("lib/beets.py")
    return scenarios


# ---------------------------------------------------------------------------
# Classification check
# ---------------------------------------------------------------------------

def classification_violation(
    scenario: str,
    *,
    delete_eligible: frozenset[str] | None = None,
    excluded: frozenset[str] | None = None,
    visible_not_eligible: frozenset[str] | None = None,
    never_reaches: frozenset[str] | None = None,
    historical: Mapping[str, _Historical] | None = None,
) -> str | None:
    """Return why ``scenario`` is unclassified, or ``None`` when it is real.

    Module-level so the known-bad self-test can hand it a fabricated
    registry exactly the way the discovery-driven checks use the real one.
    """
    buckets = (
        (DELETE_ELIGIBLE_REJECTION_SCENARIOS if delete_eligible is None
         else delete_eligible),
        (WRONG_MATCH_EXCLUDED_REJECTION_SCENARIOS if excluded is None
         else excluded),
        (VISIBLE_NOT_DELETE_ELIGIBLE if visible_not_eligible is None
         else visible_not_eligible),
        (NEVER_REACHES_ADMISSION if never_reaches is None else never_reaches),
    )
    known_historical = HISTORICAL_SCENARIOS if historical is None else historical
    if scenario in known_historical:
        record = known_historical[scenario]
        if not record.source or not record.reason:
            return (
                f"historical scenario {scenario!r} carries no falsifiable "
                "evidence"
            )
        return None
    memberships = sum(1 for bucket in buckets if scenario in bucket)
    if memberships == 0:
        return (
            f"{scenario!r} is spelled by a registered producer but is not "
            "classified into any Wrong Matches routing bucket — is it "
            "delete-eligible, worklist-excluded, visible-but-not-eligible, "
            "or structurally unreachable?"
        )
    if memberships > 1:
        return (
            f"{scenario!r} is classified into {memberships} routing buckets "
            "at once — exactly one must be authoritative"
        )
    return None


class TestEveryWrongMatchScenarioHasAClassification(unittest.TestCase):
    def test_no_discovered_scenario_is_unclassified(self) -> None:
        violations = [
            violation
            for scenario in sorted(every_discovered_scenario())
            if (violation := classification_violation(scenario)) is not None
        ]
        self.assertEqual(violations, [])

    def test_every_registered_bucket_scenario_still_exists_or_is_historical(
        self,
    ) -> None:
        """A registry entry nothing spells any more is stale registry debt."""
        discovered = every_discovered_scenario()
        for scenario in (
            *DELETE_ELIGIBLE_REJECTION_SCENARIOS,
            *VISIBLE_NOT_DELETE_ELIGIBLE,
            *NEVER_REACHES_ADMISSION,
        ):
            with self.subTest(scenario):
                self.assertIn(
                    scenario, discovered,
                    f"{scenario!r} is registered but no producer spells it",
                )
        # WRONG_MATCH_EXCLUDED_REJECTION_SCENARIOS additionally covers
        # spectral_reject and the four other folder facts, produced by the
        # quality decision pipeline rather than these four files — audited
        # separately by tests/test_classify_producer_audit.py's ``scenario``
        # subject. Only audio_corrupt is this audit's own producer.
        self.assertIn("audio_corrupt", discovered)

    def test_historical_scenarios_are_registered_with_falsifiable_evidence(
        self,
    ) -> None:
        for scenario, record in HISTORICAL_SCENARIOS.items():
            with self.subTest(scenario):
                self.assertTrue(record.source)
                self.assertTrue(record.reason)


class TestTheAuditIsFailClosed(unittest.TestCase):
    """Known-bad self-tests: a checker that cannot fail proves nothing."""

    def test_a_fabricated_scenario_is_discovered_and_rejected(self) -> None:
        planted = 'ValidationResult(scenario="the_pressing_went_sideways")\n'
        discovered = scenario_literals_in_source(planted)
        self.assertIn("the_pressing_went_sideways", discovered)
        self.assertIsNotNone(
            classification_violation("the_pressing_went_sideways")
        )

    def test_an_attribute_write_is_discovered_and_rejected(self) -> None:
        planted = 'result.scenario = "the_pressing_went_sideways"\n'
        discovered = scenario_literals_in_source(planted)
        self.assertIn("the_pressing_went_sideways", discovered)
        self.assertIsNotNone(
            classification_violation("the_pressing_went_sideways")
        )

    def test_a_classified_scenario_passes(self) -> None:
        self.assertIsNone(classification_violation("high_distance"))
        self.assertIsNone(classification_violation("audio_corrupt"))
        self.assertIsNone(classification_violation("untracked_audio"))
        self.assertIsNone(classification_violation("strong_match"))
        self.assertIsNone(classification_violation("abandoned_auto_import"))

    def test_a_scenario_double_booked_into_two_buckets_is_rejected(self) -> None:
        self.assertIsNotNone(classification_violation(
            "high_distance",
            delete_eligible=frozenset({"high_distance"}),
            excluded=frozenset({"high_distance"}),
        ))

    def test_a_historical_entry_needs_source_and_reason(self) -> None:
        self.assertIsNotNone(classification_violation(
            "invented", historical={"invented": _Historical(source="", reason="")},
        ))
        self.assertIsNone(classification_violation(
            "invented",
            historical={"invented": _Historical(
                source="download_log.beets_scenario", reason="real reason",
            )},
        ))

    def test_a_scenario_named_only_in_a_comment_is_not_discovered(self) -> None:
        """A mention is not a spelling — mirrors test-fidelity.md Rule C."""
        source = (
            "# scenario=\"the_pressing_went_sideways\" used to fire here\n"
            'ValidationResult(scenario="high_distance")\n'
        )
        discovered = scenario_literals_in_source(source)
        self.assertNotIn("the_pressing_went_sideways", discovered)
        self.assertIn("high_distance", discovered)

    def test_module_scenario_constants_are_discovered(self) -> None:
        constants = module_scenario_constants("lib/beets.py")
        self.assertIn("no_choose_match", constants)
        self.assertIn("validation_error", constants)


if __name__ == "__main__":
    unittest.main()
