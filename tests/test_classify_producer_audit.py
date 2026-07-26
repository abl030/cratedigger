#!/usr/bin/env python3
"""Producer audit for ``web/classify.py``'s decision-name copy (issue #882).

Every decision-name literal the Recents classifier matches against is a
CLAIM about what some producer emits, and issue #868's defect was exactly
such a claim going unchecked: copy shipped fluent and wrong because its
trigger literal existed nowhere in the codebase, and every fixture fed the
literal by hand (``.claude/rules/test-fidelity.md`` Rule B — the fixture
was more permissive than production).

``web/classify.py`` had the same shape and no audit. This module supplies
one:

* **Discovery is derived, never hand-listed.** ``classify_match_targets``
  reads the module — its inline comparison grammar via AST, its
  module-level string tables via ``vars`` — so a new match target cannot
  ship unregistered. Anything discovered and unregistered FAILS.
* **Evidence is a spelling, not a mention.** A producer proves a literal
  by SPELLING it as a string literal (``spelled_string_literals`` parses,
  it does not grep). A mention in a comment is not a spelling, and a
  docstring that mentions the token is one long literal that never equals
  it — precisely the hiding place a fabricated trigger would use.
* **Historical literals are registered as such.** A string a past revision
  emitted and live rows still carry is legitimate; it just has no producer
  and must say so, with the live evidence written down.

What it found on the first run (issue #882, verified against the live
pipeline DB on 2026-07-26):

* ``no_candidates`` — no producer anywhere, zero live rows, yet it carried
  the fluent sentence "No MusicBrainz match found". The real producing
  literal is ``lib/beets.py``'s ``mbid_not_found`` (50 live rejected rows),
  which fell through to the raw-token fallback. Both the key and the claim
  were wrong: the producer records that the requested ID was absent from
  the candidate set, not that nothing matched.
* ``stale_path_cleared`` / ``stale_path_clear_failed`` — planned in a 2026-04
  plan doc, never produced, zero live rows.
"""

import ast
import functools
import os
import re
import sys
import unittest
from collections.abc import Mapping, Sequence
from dataclasses import dataclass

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import web.classify as classify
from lib.quality import dispatch_action
from lib.quality.dispatch_actions import decision_denylists
from web.classify import LogEntry, classify_log_entry

_REPO_ROOT = os.path.join(os.path.dirname(__file__), "..")
_CLASSIFY_RELPATH = "web/classify.py"


# ---------------------------------------------------------------------------
# The registry — what each match target claims, and who can produce it
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class _Producers:
    """Who can emit the literals compared against one match target."""

    files: tuple[str, ...]
    """Production files that must SPELL the literal. Empty means the
    literals are historical and must each carry a live-evidence note."""
    why: str
    match: str = "exact"
    """``exact`` or ``prefix`` — ``prefix`` is for ``.startswith`` targets,
    where the literal is a fragment of what the producer spells."""
    casefold: bool = False
    """Set only where the module itself changes the case before comparing;
    the reason belongs in ``why``."""


_QUALITY_DECISIONS = (
    "lib/quality/dispatch_actions.py",
    "lib/quality/decisions.py",
    "lib/quality/pipeline.py",
)
_REJECTION_SCENARIOS = _QUALITY_DECISIONS + (
    "lib/dispatch/core.py",
    "lib/beets.py",
)
_TRIAGE_ACTIONS = (
    "lib/wrong_match_cleanup_service.py",
    "lib/wrong_match_delete_service.py",
)

MATCH_SUBJECTS: dict[str, _Producers] = {
    "_entry_decision(entry)": _Producers(
        _QUALITY_DECISIONS,
        "the ImportResult decision / beets_scenario a pipeline decision writes",
    ),
    "_entry_rejection_decision(entry)": _Producers(
        _QUALITY_DECISIONS,
        "a rejection-recording pipeline decision name",
    ),
    "ir.decision": _Producers(
        _QUALITY_DECISIONS, "the persisted ImportResult decision name",
    ),
    "triage_preview_decision": _Producers(
        ("lib/quality/pipeline.py",),
        "classify_full_pipeline_decision's reason slot",
    ),
    "scenario": _Producers(
        _REJECTION_SCENARIOS,
        "the rejection scenario persisted on download_log.beets_scenario",
    ),
    "scenario.startswith": _Producers(
        _QUALITY_DECISIONS,
        "a shared prefix of the suspect-lossless decision names",
        match="prefix",
    ),
    "entry.beets_scenario": _Producers(
        _QUALITY_DECISIONS, "the transcode decision names",
    ),
    "action": _Producers(
        _TRIAGE_ACTIONS, "a persisted wrong-match triage action",
    ),
    "triage_action": _Producers(
        _TRIAGE_ACTIONS, "a persisted wrong-match triage action",
    ),
    "entry.outcome": _Producers(
        ("lib/pipeline_db/download_log.py",),
        "the DownloadLogOutcome taxonomy, mirrored by the schema CHECK",
    ),
    "entry.request_status": _Producers(
        ("lib/transitions.py",), "the album_requests lifecycle statuses",
    ),
    "job.status": _Producers(
        ("lib/import_queue.py",), "IMPORT_JOB_STATUSES",
    ),
    "preview": _Producers(
        ("lib/import_queue.py",), "IMPORT_JOB_PREVIEW_STATUSES",
    ),
    "basis.branch": _Producers(
        ("lib/quality/compare.py", "lib/quality/evidence_types.py"),
        "the branch compare_quality records on QualityComparisonBasis",
    ),
    "basis.verdict": _Producers(
        ("lib/quality/compare.py",), "the verdict compare_quality records",
    ),
    "metric": _Producers(
        ("lib/quality/compare.py",),
        "the metric label compare_quality selected",
    ),
    "entry.spectral_grade": _Producers(
        ("lib/spectral_check.py",), "a spectral grade the analyser assigns",
    ),
    "entry.original_filetype.lower()": _Producers(
        ("lib/quality/filetypes.py",), "a codec token, already lower-cased",
    ),
    "fmt": _Producers(
        ("lib/quality/filetypes.py",),
        "a codec token that _quality_label_from_bitrate upper-cases before "
        "comparing, so the producer's lower-case spelling is the evidence",
        casefold=True,
    ),
    "badge": _Producers(
        (_CLASSIFY_RELPATH,), "this module's own badge copy",
    ),
    "badge.startswith": _Producers(
        (_CLASSIFY_RELPATH,),
        "the prefix of this module's own triage badge copy",
        match="prefix",
    ),
}

HISTORICAL_LITERALS: dict[str, str] = {
    "album_name_mismatch": (
        "emitted by a pre-2026-03-24 revision; 1 live download_log row "
        "(2026-03-24), no producer since"
    ),
    "preview_backfilled": (
        "emitted by the 2026-04 preview backfill; 59 live wrong_match_triage "
        "rows (2026-04-26 to 2026-04-28), no producer since"
    ),
}


# ---------------------------------------------------------------------------
# Producer evidence — a spelling, not a mention
# ---------------------------------------------------------------------------

def spelled_string_literals(source: str) -> frozenset[str]:
    """Every string a Python source actually SPELLS as a literal.

    Parsed, not grepped. A comment is not a spelling at all, and a
    docstring mentioning a token is one long literal that never equals it —
    which is exactly how a fabricated trigger would hide from a whole-file
    substring check (issue #868).
    """
    return frozenset(
        node.value
        for node in ast.walk(ast.parse(source))
        if isinstance(node, ast.Constant) and isinstance(node.value, str)
    )


@functools.cache
def _spellings(relpath: str) -> frozenset[str]:
    with open(os.path.join(_REPO_ROOT, relpath), encoding="utf-8") as handle:
        return spelled_string_literals(handle.read())


def producer_spellings(
    relpaths: "Sequence[str]",
) -> dict[str, frozenset[str]]:
    """Read the spellings of each named production file."""
    return {relpath: _spellings(relpath) for relpath in relpaths}


def _is_spelled(
    literal: str,
    producers: _Producers,
    spellings: "Mapping[str, frozenset[str]]",
) -> bool:
    needle = literal.casefold() if producers.casefold else literal
    for relpath in producers.files:
        for value in spellings.get(relpath, frozenset()):
            candidate = value.casefold() if producers.casefold else value
            if producers.match == "prefix":
                if candidate.startswith(needle):
                    return True
            elif candidate == needle:
                return True
    return False


def check_literal_has_a_producer(
    literal: str,
    producers: _Producers,
    spellings: "Mapping[str, frozenset[str]]",
    historical: "Mapping[str, str] | None" = None,
) -> str | None:
    """Return why this literal is unproducible, or None when it is real.

    Module-level so the known-bad self-tests can hand it the exact
    fabricated entries that shipped.
    """
    known = HISTORICAL_LITERALS if historical is None else historical
    spelled = _is_spelled(literal, producers, spellings)
    note = known.get(literal)
    if note is not None:
        if len(note) < 20:
            return f"historical literal {literal!r} carries no live evidence"
        if spelled:
            return (
                f"{literal!r} is registered historical but a producer spells "
                "it again — retire the historical entry"
            )
        return None
    if not producers.files:
        return (
            f"{literal!r} names no producer and is not registered as "
            "historical with live evidence"
        )
    if not spelled:
        return f"no producer spells {literal!r} ({producers.why})"
    return None


def check_subject_is_registered(
    subject: str,
    registry: "Mapping[str, _Producers] | None" = None,
) -> str | None:
    """Return why a discovered match target is unaccounted for, or None."""
    known = MATCH_SUBJECTS if registry is None else registry
    if subject in known:
        return None
    return (
        f"{subject!r} is matched against string literals but names no "
        "producer — register it or the copy behind it is unverifiable"
    )


def check_match_target(
    subject: str,
    literals: "Sequence[str]",
    registry: "Mapping[str, _Producers] | None" = None,
    historical: "Mapping[str, str] | None" = None,
) -> list[str]:
    """Everything unaccounted for about one discovered match target.

    The composite the audit runs and the known-bad self-tests plant
    against: an unregistered subject and an unproducible literal are the
    two ways a fabricated trigger enters the module, and both answer here.
    """
    unregistered = check_subject_is_registered(subject, registry)
    if unregistered is not None:
        return [unregistered]
    producers = (MATCH_SUBJECTS if registry is None else registry)[subject]
    spellings = producer_spellings(producers.files)
    return [
        violation
        for literal in literals
        if (violation := check_literal_has_a_producer(
            literal, producers, spellings, historical)) is not None
    ]


# ---------------------------------------------------------------------------
# Discovery — derived from the module, never hand-listed
# ---------------------------------------------------------------------------

_MATCH_OPS = (ast.Eq, ast.NotEq, ast.In, ast.NotIn)
_PREFIX_METHODS = ("startswith", "endswith")


def _constant_strings(node: ast.AST) -> tuple[str, ...]:
    """Every string constant this operand offers as a match target.

    Deliberately wide: a container mixing names and literals still exposes
    each literal it spells, so nothing hides behind a sibling.
    """
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return (node.value,)
    if isinstance(node, (ast.Tuple, ast.List, ast.Set)):
        return tuple(
            element.value
            for element in node.elts
            if isinstance(element, ast.Constant)
            and isinstance(element.value, str)
        )
    return ()


def inline_match_targets(source: str) -> dict[str, tuple[str, ...]]:
    """String literals ``web/classify.py`` compares an operand against.

    The module's idiom is an inline ``==`` / ``in`` chain against a local
    (``scenario``, ``action``, ``preview``), not a module-level table — so
    that is the grammar this reads, keyed by the operand's source text:

    * ``Compare`` with ``==`` / ``!=`` / ``in`` / ``not in``, either side;
    * ``.startswith(...)`` / ``.endswith(...)`` with literal arguments;
    * ``match`` / ``case "literal"``.

    Bounded and syntactic: it collects the literals a comparison spells, it
    does not try to infer what they mean.
    """
    targets: dict[str, tuple[str, ...]] = {}

    def record(subject: str, values: "Sequence[str]") -> None:
        if values:
            targets[subject] = tuple(
                dict.fromkeys(targets.get(subject, ()) + tuple(values)))

    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.Compare):
            for op, comparator in zip(node.ops, node.comparators):
                if not isinstance(op, _MATCH_OPS):
                    continue
                record(ast.unparse(node.left), _constant_strings(comparator))
                record(ast.unparse(comparator), _constant_strings(node.left))
        elif (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr in _PREFIX_METHODS
        ):
            subject = f"{ast.unparse(node.func.value)}.{node.func.attr}"
            for argument in node.args:
                record(subject, _constant_strings(argument))
        elif isinstance(node, ast.Match):
            subject = ast.unparse(node.subject)
            for case in node.cases:
                for pattern in ast.walk(case.pattern):
                    if isinstance(pattern, ast.MatchValue):
                        record(subject, _constant_strings(pattern.value))
    return targets


def module_level_match_targets(
    namespace: "Mapping[str, object]",
) -> dict[str, tuple[str, ...]]:
    """String tables a module holds at module level.

    ``web/classify.py`` holds none today — every match target is inline —
    but a copy table is the shape issue #868's fabricated trigger took, so
    the scan answers for it in every form it can take: bare ``str``, dict
    keys, tuple / list / set / frozenset members, and compiled patterns.
    Anything unrecognised is a match target until it is registered.
    """
    targets: dict[str, tuple[str, ...]] = {}
    for name, value in namespace.items():
        if name.startswith("__"):
            continue
        if isinstance(value, str):
            targets[name] = (value,)
        elif isinstance(value, re.Pattern):
            pattern = value.pattern
            if isinstance(pattern, str):
                targets[name] = (pattern,)
        elif isinstance(value, dict):
            keys = tuple(key for key in value if isinstance(key, str))
            if keys:
                targets[name] = keys
        elif isinstance(value, (tuple, list, set, frozenset)):
            items = tuple(item for item in value if isinstance(item, str))
            if items:
                targets[name] = items
    return targets


def classify_match_targets(
    source: str | None = None,
    namespace: "Mapping[str, object] | None" = None,
) -> dict[str, tuple[str, ...]]:
    """Every string ``web/classify.py`` can match a producer value against."""
    if source is None:
        with open(
            os.path.join(_REPO_ROOT, _CLASSIFY_RELPATH), encoding="utf-8",
        ) as handle:
            source = handle.read()
    scope = vars(classify) if namespace is None else namespace
    targets = inline_match_targets(source)
    targets.update(module_level_match_targets(scope))
    return targets


# ---------------------------------------------------------------------------
# The audit
# ---------------------------------------------------------------------------

class TestEveryClassifyMatchTargetHasAProducer(unittest.TestCase):
    """The class fix behind issue #868's fabricated-copy defect, applied to
    the surface issue #882 nominated: ``web/classify.py``."""

    def test_no_match_target_is_unaccounted_for(self) -> None:
        """The whole contract, through the composite the self-tests plant
        against. The two tests below are the same audit split for a sharper
        failure message."""
        violations = [
            violation
            for subject, literals in classify_match_targets().items()
            for violation in check_match_target(subject, literals)
        ]
        self.assertEqual(violations, [])

    def test_every_discovered_subject_is_registered(self) -> None:
        violations = [
            violation
            for subject in classify_match_targets()
            if (violation := check_subject_is_registered(subject)) is not None
        ]
        self.assertEqual(violations, [])

    def test_every_matched_literal_is_spelled_by_its_producer(self) -> None:
        targets = classify_match_targets()
        for subject, literals in sorted(targets.items()):
            producers = MATCH_SUBJECTS[subject]
            spellings = producer_spellings(producers.files)
            for literal in literals:
                with self.subTest(subject=subject, literal=literal):
                    violation = check_literal_has_a_producer(
                        literal, producers, spellings)
                    self.assertIsNone(violation, violation)

    def test_every_registered_subject_is_still_in_the_module(self) -> None:
        """A registry entry for a target that no longer exists is stale."""
        targets = classify_match_targets()
        for subject, producers in MATCH_SUBJECTS.items():
            with self.subTest(subject):
                self.assertIn(subject, targets, f"{subject} is registered but gone")
                self.assertGreater(len(producers.why), 12, "entries carry a reason")

    def test_every_historical_literal_is_still_matched(self) -> None:
        """A historical exemption for a literal nobody matches is dead."""
        matched = {
            literal
            for literals in classify_match_targets().values()
            for literal in literals
        }
        for literal, note in HISTORICAL_LITERALS.items():
            with self.subTest(literal):
                self.assertIn(literal, matched)
                self.assertGreater(len(note), 20, "historical entries carry evidence")


class TestTheAuditIsFailClosed(unittest.TestCase):
    """Known-bad self-tests: a checker that cannot fail proves nothing."""

    _INVENTED = "the_pressing_went_sideways"

    def _planted_inline_sources(self) -> dict[str, str]:
        """Every inline form, planted onto the module's REAL subjects.

        Planting under ``scenario`` / ``action`` is the realistic
        regression: someone adds one more ``if`` to an existing chain.
        """
        invented = self._INVENTED
        return {
            "equality": f"if scenario == {invented!r}:\n    pass\n",
            "inequality": f"if scenario != {invented!r}:\n    pass\n",
            "tuple membership": f"if scenario in ({invented!r},):\n    pass\n",
            "list membership": f"if scenario in [{invented!r}]:\n    pass\n",
            "set membership": f"if scenario in {{{invented!r}}}:\n    pass\n",
            "literal on the left": f"if {invented!r} == scenario:\n    pass\n",
            "startswith": f"if scenario.startswith({invented!r}):\n    pass\n",
            "endswith": f"if scenario.endswith({invented!r}):\n    pass\n",
            "match case": (
                f"match scenario:\n    case {invented!r}:\n        pass\n"
            ),
            "mixed container": (
                f"if scenario in (OTHER, {invented!r}):\n    pass\n"
            ),
        }

    def _planted_module_tables(self) -> dict[str, object]:
        invented = self._INVENTED
        return {
            "_FABRICATED_COPY": {invented: "The pressing is unwell"},
            "_FABRICATED_PREFIX": invented,
            "_FABRICATED_TUPLE": (invented,),
            "_FABRICATED_LIST": [invented],
            "_FABRICATED_SET": {invented},
            "_FABRICATED_FROZENSET": frozenset({invented}),
            "_FABRICATED_RE": re.compile(f"^{invented}$"),
        }

    def test_every_inline_shape_a_fabricated_trigger_can_take_is_caught(self):
        """The module's own idiom is inline, so every inline form answers.

        Issue #868 review F3: the narrower scan saw dicts and tuples only,
        and a bare constant feeding ``.startswith`` shipped silently.
        """
        for description, source in self._planted_inline_sources().items():
            with self.subTest(description):
                targets = inline_match_targets(source)
                found = [
                    (subject, literals)
                    for subject, literals in targets.items()
                    if self._INVENTED in literals
                ]
                self.assertTrue(found, f"{description} was not discovered")
                for subject, literals in found:
                    self.assertTrue(
                        check_match_target(subject, literals),
                        f"{description} slipped past the audit",
                    )

    def test_a_brand_new_inline_subject_fails_closed(self) -> None:
        """A whole new match chain cannot ship unregistered."""
        targets = inline_match_targets("if invented_field == 'anything':\n    pass\n")
        self.assertIn("invented_field", targets)
        self.assertTrue(check_match_target("invented_field", ("anything",)))

    def test_every_module_table_shape_is_caught(self) -> None:
        for name, value in self._planted_module_tables().items():
            with self.subTest(name):
                targets = module_level_match_targets({name: value})
                self.assertIn(name, targets, f"{name} was not discovered")
                self.assertTrue(
                    any(self._INVENTED in found for found in targets[name]),
                    f"{name} was discovered without its literal",
                )
                self.assertTrue(
                    check_match_target(name, targets[name]),
                    f"{name} slipped past the audit",
                )

    def test_the_composite_scan_fails_closed_on_both_halves(self) -> None:
        """The entry point the audit uses, not just its halves."""
        targets = classify_match_targets(
            source=f"if scenario == {self._INVENTED!r}:\n    pass\n",
            namespace={"_FABRICATED_COPY": {self._INVENTED: "copy"}},
        )
        self.assertIn("_FABRICATED_COPY", targets)
        self.assertIn("scenario", targets)
        self.assertTrue(check_match_target(
            "_FABRICATED_COPY", targets["_FABRICATED_COPY"]))
        self.assertTrue(check_match_target("scenario", targets["scenario"]))

    def test_the_producer_check_rejects_the_literal_that_shipped(self) -> None:
        """``no_candidates``: fluent copy, zero rows, no producer (#882)."""
        producers = MATCH_SUBJECTS["scenario"]
        spellings = producer_spellings(producers.files)
        self.assertIsNotNone(check_literal_has_a_producer(
            "no_candidates", producers, spellings))
        self.assertIsNotNone(check_literal_has_a_producer(
            "stale_path_cleared", MATCH_SUBJECTS["action"],
            producer_spellings(MATCH_SUBJECTS["action"].files)))
        # …and the literal that replaced it passes.
        self.assertIsNone(check_literal_has_a_producer(
            "mbid_not_found", producers, spellings))

    def test_a_mention_in_a_comment_or_docstring_is_not_a_producer(self) -> None:
        """The evidence bar the #868 substring check could not clear."""
        source = (
            '"""A docstring that talks about invented_reason at length."""\n'
            "# invented_reason is discussed here too\n"
            "VALUE = 'a_real_literal'\n"
        )
        spelled = spelled_string_literals(source)
        self.assertIn("a_real_literal", spelled)
        self.assertNotIn("invented_reason", spelled)
        producers = _Producers(("fake.py",), "planted")
        self.assertIsNotNone(check_literal_has_a_producer(
            "invented_reason", producers, {"fake.py": spelled}, historical={}))
        self.assertIsNone(check_literal_has_a_producer(
            "a_real_literal", producers, {"fake.py": spelled}, historical={}))

    def test_a_historical_entry_cannot_launder_a_fabricated_literal(self) -> None:
        producers = _Producers((), "no producer")
        spellings: dict[str, frozenset[str]] = {}
        self.assertIsNotNone(check_literal_has_a_producer(
            "invented", producers, spellings, historical={}))
        self.assertIsNotNone(check_literal_has_a_producer(
            "invented", producers, spellings, historical={"invented": "old"}))
        self.assertIsNone(check_literal_has_a_producer(
            "invented", producers, spellings,
            historical={"invented": "12 live rows on 2026-03-24, none since"}))
        # A "historical" literal a producer spells again is a stale entry.
        self.assertIsNotNone(check_literal_has_a_producer(
            "invented",
            _Producers(("fake.py",), "planted"),
            {"fake.py": frozenset({"invented"})},
            historical={"invented": "12 live rows on 2026-03-24, none since"},
        ))

    def test_prefix_and_casefold_relaxations_still_require_a_spelling(self):
        prefix = _Producers(("fake.py",), "planted", match="prefix")
        spellings = {"fake.py": frozenset({"suspect_lossless_downgrade"})}
        self.assertIsNone(check_literal_has_a_producer(
            "suspect_lossless", prefix, spellings, historical={}))
        self.assertIsNotNone(check_literal_has_a_producer(
            "suspect_flac", prefix, spellings, historical={}))
        folded = _Producers(("fake.py",), "planted", casefold=True)
        self.assertIsNone(check_literal_has_a_producer(
            "FLAC", folded, {"fake.py": frozenset({"flac"})}, historical={}))
        self.assertIsNotNone(check_literal_has_a_producer(
            "FLAK", folded, {"fake.py": frozenset({"flac"})}, historical={}))


# ---------------------------------------------------------------------------
# The copy the audit corrected, and the claims it must keep making
# ---------------------------------------------------------------------------

def _rejected(scenario: str) -> LogEntry:
    return LogEntry(
        id=1, request_id=2, outcome="rejected", beets_scenario=scenario)


class TestFabricatedCopyIsGone(unittest.TestCase):
    """Issue #882: the literals the audit convicted, pinned by outcome."""

    def test_the_real_producer_literal_now_carries_the_copy(self) -> None:
        """50 live rejected rows carry ``mbid_not_found`` (2026-07-26)."""
        classified = classify_log_entry(_rejected("mbid_not_found"))
        self.assertEqual(
            classified.verdict,
            "Requested release ID not among the match candidates",
        )
        # The producer records an absence from the candidate SET, so the
        # copy must not claim the stronger fact that nothing matched.
        self.assertNotIn("No MusicBrainz match", classified.verdict)

    def test_the_fabricated_key_no_longer_manufactures_a_sentence(self) -> None:
        classified = classify_log_entry(_rejected("no_candidates"))
        self.assertEqual(classified.verdict, "no_candidates")

    def test_the_unproduced_triage_actions_no_longer_have_labels(self) -> None:
        for action in ("stale_path_cleared", "stale_path_clear_failed"):
            with self.subTest(action):
                self.assertEqual(
                    classify._wrong_match_action_label(action),
                    action.replace("_", " "),
                )

    def test_the_historical_literals_keep_their_copy(self) -> None:
        """One live row from 2026-03-24 still renders as a sentence."""
        self.assertEqual(
            classify_log_entry(_rejected("album_name_mismatch")).verdict,
            "Album name mismatch",
        )
        self.assertEqual(
            classify._wrong_match_action_label("preview_backfilled"),
            "previewed",
        )


class TestQualityVerdictCopyMatchesTheProducersAction(unittest.TestCase):
    """Issue #882 item 4, deterministic half.

    Each rejection sentence is checked against the two facts the PRODUCER
    records for that decision — ``dispatch_action(...).preserve_imported``
    (does the request stay imported?) and ``decision_denylists(...)`` (is
    the source banned?). The generated twin in
    ``tests/test_classify_producer_audit_generated.py`` patrols the world
    around these.
    """

    def test_a_proof_lock_says_complete_because_the_request_stays_imported(self):
        action = dispatch_action("verified_lossless_locked")
        self.assertTrue(action.preserve_imported)
        self.assertFalse(decision_denylists("verified_lossless_locked"))
        verdict = classify_log_entry(_rejected("verified_lossless_locked")).verdict
        self.assertEqual(
            verdict,
            "Verified lossless already on disk; automatic candidate declined "
            "(no denylist); acquisition is complete",
        )
        self.assertNotIn("searching continues", verdict)

    def test_a_source_lock_says_searching_continues_and_admits_the_denylist(self):
        self.assertFalse(dispatch_action("lossless_source_locked").preserve_imported)
        self.assertTrue(decision_denylists("lossless_source_locked"))
        verdict = classify_log_entry(_rejected("lossless_source_locked")).verdict
        self.assertIn("searching continues", verdict)
        self.assertNotIn("no denylist", verdict.casefold())
        self.assertNotIn("acquisition is complete", verdict)

    def test_a_quality_downgrade_never_claims_acquisition_is_complete(self):
        for scenario in ("downgrade", "quality_downgrade", "transcode_downgrade",
                         "suspect_lossless_downgrade",
                         "suspect_lossless_probe_missing"):
            with self.subTest(scenario):
                verdict = classify_log_entry(_rejected(scenario)).verdict
                self.assertIn("searching continues", verdict)
                self.assertNotIn("acquisition is complete", verdict)

    def test_a_no_denylist_reject_never_claims_the_source_was_denylisted(self):
        for scenario in ("nested_layout", "high_distance", "album_name_mismatch"):
            with self.subTest(scenario):
                self.assertFalse(decision_denylists(scenario))
                verdict = classify_log_entry(_rejected(scenario)).verdict
                self.assertNotIn("denylist", verdict.casefold())


if __name__ == "__main__":
    unittest.main()
