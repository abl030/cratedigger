"""Producer audit for ``web/classify.py``'s decision-name copy (issue #882).

Every decision-name literal the Recents classifier matches against is a
CLAIM about what some producer emits, and issue #868's defect was exactly
such a claim going unchecked: copy shipped fluent and wrong because its
trigger literal existed nowhere in the codebase, and every fixture fed the
literal by hand (``.claude/rules/test-fidelity.md`` Rule C — the fixture
was more permissive than production).

``web/classify.py`` had the same shape and no audit. This module supplies
one:

* **Discovery is derived, never hand-listed.** ``classify_match_targets``
  reads the module — its inline comparison grammar and its literal
  containers at ANY scope via AST, plus its module-level values via
  ``vars`` — so a new match target cannot ship unregistered. Anything
  discovered and neither registered nor exempted FAILS.
* **Evidence is a spelling, not a mention.** A producer proves a literal
  by SPELLING it as a string literal (``spelled_string_literals`` parses,
  it does not grep). A comment is not a spelling, and a docstring that
  mentions the token is one long literal that never equals it — precisely
  the hiding place a fabricated trigger would use.
* **Historical literals are registered as such**, with structured live
  evidence (source expression, row count, last-seen date) so a reviewer
  has one falsifiable query rather than a sentence of prose.

What it found on the first run (issue #882, verified against the live
pipeline DB on 2026-07-26):

* ``no_candidates`` — no producer anywhere, zero live rows, yet it carried
  the fluent sentence "No MusicBrainz match found". The real producing
  literal is ``lib/beets.py``'s ``mbid_not_found`` (50 live rejected rows),
  which fell through to the raw-token fallback.
* ``stale_path_cleared`` / ``stale_path_clear_failed`` — planned in a 2026-04
  plan doc, never produced, zero live rows.

Two bounds this audit deliberately does NOT close, because closing them
means inferring runtime semantics from source
(``.claude/rules/code-quality.md`` § "Semantic source scanners are
prohibited"):

* Evidence is FILE-level, not field-level. A literal counts as produced
  when a registered producer FILE spells it, even if the value it spells
  belongs to a different field's vocabulary. ``scenario == "genuine"``
  therefore passes, because ``"genuine"`` is a spectral grade spelled in
  one of the files registered for that subject.
* A copy table reachable without ever being named (built by a helper call,
  say) is outside the grammar. Assignments at any scope and literal tables
  used in place are inside it.
"""
import ast
import datetime
import functools
import os
import re
import sys
import unittest
from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import ClassVar

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import msgspec

from lib.quality import (
    CandidateSummary,
    HarnessTrackInfo,
    ValidationResult,
    dispatch_action,
)
from lib.quality.dispatch_actions import decision_denylists
from tests.helpers import make_import_result
from web import classify
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
    literals are historical and must each carry structured live evidence."""
    why: str
    match: str = "exact"
    """``exact`` or ``prefix`` — ``prefix`` is for ``.startswith`` targets,
    where the literal is a fragment of what the producer spells."""
    casefold: bool = False
    """Set only where the module itself changes the case before comparing;
    the reason belongs in ``why``."""
    min_occurrences: int = 1
    """Raised above 1 for a subject that names ITSELF as its producer,
    where the comparison site is one of the spellings it would otherwise
    count as its own evidence — a tautology that lets a typo certify
    itself."""


_QUALITY_DECISIONS = (
    "lib/quality/dispatch_actions.py",
    "lib/quality/decisions.py",
    "lib/quality/pipeline.py",
)
_REJECTION_SCENARIOS = _QUALITY_DECISIONS + (
    "lib/dispatch/core.py",
    "lib/beets.py",
    # The importer subprocess is a first-class producer of this column: its
    # ``ImportResult.decision`` becomes ``beets_scenario`` (and wins over it
    # in ``_entry_rejection_decision`` whenever ``dispatch_action`` records a
    # rejection). ``import_failed`` / ``mbid_missing`` / ``crash`` /
    # ``quality_evidence_action_failed`` are spelled ONLY here — the mention
    # in ``lib/quality/dispatch_actions.py`` is a comment, which this audit
    # deliberately does not count as a spelling.
    "harness/import_one.py",
    # The two manifest guards that reject before beets is consulted.
    "lib/download_validation.py",
    "lib/dispatch/manifest_guard.py",
    # Widening this tuple widens the FILE-level evidence base for EVERY
    # literal under ``scenario`` — the first bound named in the module
    # docstring. That is the price of the bound and it is paid knowingly:
    # each file added here really does write ``download_log.beets_scenario``
    # values, which is the whole subject.
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
    "SPECTRAL_TRANSCODE_GRADES": _Producers(
        ("lib/spectral_check.py",),
        "the two accusing spectral grades classify_album assigns — the "
        "membership test that gates issue #829 PR4's audit-only flag reads "
        "the shared frozenset rather than restating the names",
    ),
    "fmt": _Producers(
        ("lib/quality/filetypes.py",),
        "a codec token that _quality_label_from_bitrate upper-cases before "
        "comparing, so the producer's lower-case spelling is the evidence",
        casefold=True,
    ),
    "badge": _Producers(
        (_CLASSIFY_RELPATH,),
        "this module's own badge copy — self-produced, so the comparison "
        "site alone is not evidence and a second spelling is required",
        min_occurrences=2,
    ),
    "badge.startswith": _Producers(
        (_CLASSIFY_RELPATH,),
        "the prefix of this module's own triage badge copy — self-produced, "
        "so a second spelling is required",
        match="prefix",
        min_occurrences=2,
    ),
}


@dataclass(frozen=True)
class _Historical:
    """A literal a past revision emitted, kept because live rows carry it.

    Structured rather than prose so a reviewer can falsify the entry with
    one query instead of trusting a sentence: ``source`` is the exact
    column or JSON expression, ``row_count`` and ``last_seen`` are what it
    returned when the entry was written.
    """

    source: str
    row_count: int
    last_seen: str


HISTORICAL_LITERALS: dict[str, _Historical] = {
    "album_name_mismatch": _Historical(
        source="download_log.beets_scenario",
        row_count=1,
        last_seen="2026-03-24",
    ),
    "preview_backfilled": _Historical(
        source="download_log.validation_result->'wrong_match_triage'->>'action'",
        row_count=59,
        last_seen="2026-04-28",
    ),
}


NON_MATCH_TARGETS: dict[str, str] = {
    # Output copy this module RETURNS for the frontend to branch on, not a
    # value it matches against a producer. web/classify.py is the sole
    # producer of both tokens; the JS side's equality against them is
    # pinned by tests/test_js_history.mjs.
    "ACCUSATION_WITHHELD_AUDIT_ONLY_CODEC": (
        "output copy: the reason token this module emits when a resolved "
        "audit-only codec's grade is withheld"
    ),
    "ACCUSATION_WITHHELD_CODEC_UNRESOLVED": (
        "output copy: the reason token this module emits when no codec "
        "family could be resolved at all"
    ),
    # Output copy assigned for return, never compared against a producer
    # value. The badge vocabulary they carry IS audited — as the ``badge``
    # subject, where this module is its own registered producer.
    "classify_import_job_display.(badge_class, border_color)": (
        "output copy: the badge class and border colour this module returns "
        "for a preview status, not a value it matches against"
    ),
    "classify_import_job_display.(badge, badge_class, border_color)": (
        "output copy: the badge, class and border colour this module returns "
        "for a preview status, not a value it matches against"
    ),
    # Not decision copy at all: an INBOUND column-alias map, whose keys are
    # SQL ``AS`` names rather than values any producer emits. A file-level
    # spelling check cannot see them — every candidate-evidence alias lives
    # inside one long static SQL literal, the exact "mention, not a
    # spelling" shape this audit refuses to accept as evidence. The
    # stronger check exists instead: ``tests/test_pipeline_db_column_
    # contract.py::TestRenderAliasMap`` reads the aliases straight out of
    # ``_CANDIDATE_EVIDENCE_COLUMNS`` and requires every key to be one of
    # them and every target to be a real ``LogEntry`` field.
    "_ROW_FIELD_ALIASES": (
        "inbound SQL column-alias map, not decision copy: pinned against "
        "the live SELECT block by the pipeline-db column contract"
    ),
}


# ---------------------------------------------------------------------------
# Producer evidence — a spelling, not a mention
# ---------------------------------------------------------------------------

def spelled_string_literals(source: str) -> Counter[str]:
    """How many times a Python source actually SPELLS each string literal.

    Parsed, not grepped. A comment is not a spelling at all, and a
    docstring mentioning a token is one long literal that never equals it —
    which is exactly how a fabricated trigger would hide from a whole-file
    substring check (issue #868). Counted rather than merely collected so a
    module that names itself as its own producer cannot certify a literal
    on the strength of the comparison site alone.
    """
    return Counter(
        node.value
        for node in ast.walk(ast.parse(source))
        if isinstance(node, ast.Constant) and isinstance(node.value, str)
    )


@functools.cache
def _spellings(relpath: str) -> Counter[str]:
    with open(os.path.join(_REPO_ROOT, relpath), encoding="utf-8") as handle:
        return spelled_string_literals(handle.read())


def producer_spellings(
    relpaths: Sequence[str],
) -> dict[str, Counter[str]]:
    """Read the spellings of each named production file."""
    return {relpath: _spellings(relpath) for relpath in relpaths}


def spelled_count(
    literal: str,
    producers: _Producers,
    spellings: Mapping[str, Counter[str]],
) -> int:
    """How many spellings across the registered FILES back this literal."""
    needle = literal.casefold() if producers.casefold else literal
    total = 0
    for relpath in producers.files:
        for value, count in spellings.get(relpath, Counter()).items():
            candidate = value.casefold() if producers.casefold else value
            if producers.match == "prefix":
                if candidate.startswith(needle):
                    total += count
            elif candidate == needle:
                total += count
    return total


def check_literal_has_a_producer(
    literal: str,
    producers: _Producers,
    spellings: Mapping[str, Counter[str]],
    historical: Mapping[str, _Historical] | None = None,
) -> str | None:
    """Return why this literal is unproducible, or None when it is real.

    Module-level so the known-bad self-tests can hand it the exact
    fabricated entries that shipped.
    """
    known = HISTORICAL_LITERALS if historical is None else historical
    count = spelled_count(literal, producers, spellings)
    record = known.get(literal)
    if record is not None:
        if record.row_count < 1 or not record.source:
            return (
                f"historical literal {literal!r} claims no live rows — an "
                "entry nothing carries is dead copy, not history"
            )
        try:
            datetime.date.fromisoformat(record.last_seen)
        except ValueError:
            return (
                f"historical literal {literal!r} has an unusable last_seen "
                f"{record.last_seen!r}; a reviewer cannot falsify it"
            )
        if count:
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
    if count < producers.min_occurrences:
        return (
            f"no producer file spells {literal!r} "
            f"{producers.min_occurrences} time(s) ({producers.why})"
        )
    return None


def check_subject_is_registered(
    subject: str,
    registry: Mapping[str, _Producers] | None = None,
    exemptions: Mapping[str, str] | None = None,
) -> str | None:
    """Return why a discovered match target is unaccounted for, or None."""
    known = MATCH_SUBJECTS if registry is None else registry
    exempt = NON_MATCH_TARGETS if exemptions is None else exemptions
    if subject in known or subject in exempt:
        return None
    return (
        f"{subject!r} is matched against string literals but names no "
        "producer — register it or the copy behind it is unverifiable"
    )


def check_match_target(
    subject: str,
    literals: Sequence[str],
    registry: Mapping[str, _Producers] | None = None,
    historical: Mapping[str, _Historical] | None = None,
    exemptions: Mapping[str, str] | None = None,
) -> list[str]:
    """Everything unaccounted for about one discovered match target.

    The composite the audit runs and the known-bad self-tests plant
    against: an unregistered subject and an unproducible literal are the
    two ways a fabricated trigger enters the module, and both answer here.
    """
    unregistered = check_subject_is_registered(subject, registry, exemptions)
    if unregistered is not None:
        return [unregistered]
    exempt = NON_MATCH_TARGETS if exemptions is None else exemptions
    if subject in exempt:
        return []
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
_INLINE_TABLE = "<inline table>"


def _constant_strings(node: ast.AST) -> tuple[str, ...]:
    """Every string constant this node offers as a match target.

    Deliberately wide: a container mixing names and literals still exposes
    each literal it spells, so nothing hides behind a sibling.
    """
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return (node.value,)
    if isinstance(node, ast.Dict):
        return tuple(
            key.value
            for key in node.keys
            if isinstance(key, ast.Constant) and isinstance(key.value, str)
        )
    if isinstance(node, (ast.Tuple, ast.List, ast.Set)):
        return tuple(
            element.value
            for element in node.elts
            if isinstance(element, ast.Constant)
            and isinstance(element.value, str)
        )
    return ()


def _is_literal_container(node: ast.AST | None) -> bool:
    return isinstance(node, (ast.Dict, ast.Tuple, ast.List, ast.Set))


class _MatchTargetScan(ast.NodeVisitor):
    """The bounded grammar of "a literal this module can match against".

    Keyed by the operand's source text for comparisons, and by the
    enclosing scope plus the assigned name for containers — so a
    function-local table is a DIFFERENT subject from a module constant of
    the same name, and neither can hide behind the other.
    """

    def __init__(self) -> None:
        self.targets: dict[str, tuple[str, ...]] = {}
        self._scope: list[str] = []

    # -- recording ----------------------------------------------------
    def _record(self, subject: str, values: Sequence[str]) -> None:
        if values:
            self.targets[subject] = tuple(dict.fromkeys(
                self.targets.get(subject, ()) + tuple(values)))

    def _qualified(self, name: str) -> str:
        return ".".join([*self._scope, name])

    def _scoped(self, name: str, node: ast.AST) -> None:
        self._scope.append(name)
        self.generic_visit(node)
        self._scope.pop()

    # -- scopes -------------------------------------------------------
    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        self._scoped(node.name, node)

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
        self._scoped(node.name, node)

    def visit_ClassDef(self, node: ast.ClassDef) -> None:
        self._scoped(node.name, node)

    # -- inline comparisons -------------------------------------------
    def visit_Compare(self, node: ast.Compare) -> None:
        for op, comparator in zip(node.ops, node.comparators, strict=True):
            if isinstance(op, _MATCH_OPS):
                self._record(
                    ast.unparse(node.left), _constant_strings(comparator))
                self._record(
                    ast.unparse(comparator), _constant_strings(node.left))
        self.generic_visit(node)

    def visit_Call(self, node: ast.Call) -> None:
        func = node.func
        if isinstance(func, ast.Attribute) and func.attr in _PREFIX_METHODS:
            subject = f"{ast.unparse(func.value)}.{func.attr}"
            for argument in node.args:
                self._record(subject, _constant_strings(argument))
        self.generic_visit(node)

    def visit_Match(self, node: ast.Match) -> None:
        subject = ast.unparse(node.subject)
        for case in node.cases:
            for pattern in ast.walk(case.pattern):
                if isinstance(pattern, ast.MatchValue):
                    self._record(subject, _constant_strings(pattern.value))
        self.generic_visit(node)

    # -- literal containers, at ANY scope -----------------------------
    def visit_Assign(self, node: ast.Assign) -> None:
        if _is_literal_container(node.value):
            values = _constant_strings(node.value)
            for target in node.targets:
                self._record(self._qualified(ast.unparse(target)), values)
        self.generic_visit(node)

    def visit_AnnAssign(self, node: ast.AnnAssign) -> None:
        if node.value is not None and _is_literal_container(node.value):
            self._record(
                self._qualified(ast.unparse(node.target)),
                _constant_strings(node.value),
            )
        self.generic_visit(node)

    def visit_Subscript(self, node: ast.Subscript) -> None:
        if _is_literal_container(node.value):
            self._record(
                self._qualified(_INLINE_TABLE), _constant_strings(node.value))
        self.generic_visit(node)

    def visit_Attribute(self, node: ast.Attribute) -> None:
        if _is_literal_container(node.value):
            self._record(
                self._qualified(_INLINE_TABLE), _constant_strings(node.value))
        self.generic_visit(node)


def source_match_targets(source: str) -> dict[str, tuple[str, ...]]:
    """String literals ``web/classify.py`` can match a producer value against.

    The module's idiom is an inline ``==`` / ``in`` chain against a local
    (``scenario``, ``action``, ``preview``), so that is the primary
    grammar, keyed by the operand's source text:

    * ``Compare`` with ``==`` / ``!=`` / ``in`` / ``not in``, either side;
    * ``.startswith(...)`` / ``.endswith(...)`` with literal arguments;
    * ``match`` / ``case "literal"``.

    A copy table is the OTHER shape a fabricated trigger takes, so literal
    dict / tuple / list / set containers are read too — at ANY scope, not
    just module level. Collapsing part of a long ``if``-chain into a
    function-local table is the single most obvious refactor in this file,
    and a module-scope-only scan walks straight past it (issue #882 review
    B2).

    Bounded and syntactic throughout: it collects the literals a
    comparison or a container spells; it does not infer what they mean.
    """
    scan = _MatchTargetScan()
    scan.visit(ast.parse(source))
    return scan.targets


def module_level_match_targets(
    namespace: Mapping[str, object],
) -> dict[str, tuple[str, ...]]:
    """String tables a module holds at module level, by runtime value.

    Complements the AST scan rather than duplicating it: this half sees a
    module constant however it was built — a comprehension, an import, a
    ``frozenset(get_args(...))`` — where the AST half only sees literals.
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
    namespace: Mapping[str, object] | None = None,
) -> dict[str, tuple[str, ...]]:
    """Every string ``web/classify.py`` can match a producer value against."""
    if source is None:
        with open(
            os.path.join(_REPO_ROOT, _CLASSIFY_RELPATH), encoding="utf-8",
        ) as handle:
            source = handle.read()
    scope = vars(classify) if namespace is None else namespace
    targets = source_match_targets(source)
    # Merge, never replace: a module constant sharing a name with an inline
    # subject would otherwise silently drop that subject's literals
    # (issue #882 review N8).
    for name, literals in module_level_match_targets(scope).items():
        targets[name] = tuple(dict.fromkeys(targets.get(name, ()) + literals))
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
            if subject in NON_MATCH_TARGETS:
                continue
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

    def test_every_exemption_is_still_in_the_module_and_carries_a_reason(self):
        targets = classify_match_targets()
        for subject, reason in NON_MATCH_TARGETS.items():
            with self.subTest(subject):
                self.assertIn(subject, targets, f"{subject} is exempt but gone")
                self.assertGreater(len(reason), 20, "exemptions carry a reason")

    def test_every_historical_literal_is_still_matched(self) -> None:
        """A historical exemption for a literal nobody matches is dead."""
        matched = {
            literal
            for literals in classify_match_targets().values()
            for literal in literals
        }
        for literal, record in HISTORICAL_LITERALS.items():
            with self.subTest(literal):
                self.assertIn(literal, matched)
                self.assertGreaterEqual(record.row_count, 1)
                self.assertTrue(record.source)
                datetime.date.fromisoformat(record.last_seen)


class TestTheAuditIsFailClosed(unittest.TestCase):
    """Known-bad self-tests: a checker that cannot fail proves nothing."""

    _INVENTED = "the_pressing_went_sideways"

    def _planted_inline_sources(self) -> dict[str, str]:
        """Every inline form, planted onto the module's REAL subjects.

        Planting under ``scenario`` is the realistic regression: someone
        adds one more ``if`` to an existing chain.
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

    def _planted_local_tables(self) -> dict[str, str]:
        """The refactor shape a module-scope-only scan walks straight past.

        ``_rejection_verdict`` is a long ``if``-chain; collapsing part of
        it into a function-local table is the obvious tidy-up, and an
        ADDITIVE local table leaves every module-scope check green — three
        of these survived the earlier scan (issue #882 review B2).
        """
        invented = self._INVENTED
        return {
            "function-local dict": (
                "def _rejection_verdict(entry):\n"
                f"    _LOCAL_COPY = {{{invented!r}: 'The pressing is unwell'}}\n"
                "    if scenario in _LOCAL_COPY:\n"
                "        return _LOCAL_COPY[scenario]\n"
            ),
            "function-local tuple": (
                "def _rejection_verdict(entry):\n"
                f"    _LOCAL_NAMES = ({invented!r},)\n"
                "    if scenario in _LOCAL_NAMES:\n"
                "        return 'The pressing is unwell'\n"
            ),
            "function-local dict.get": (
                "def _rejection_verdict(entry):\n"
                f"    _LOCAL_COPY = {{{invented!r}: 'The pressing is unwell'}}\n"
                "    hit = _LOCAL_COPY.get(scenario)\n"
                "    if hit:\n"
                "        return hit\n"
            ),
            "annotated function-local table": (
                "def _rejection_verdict(entry):\n"
                "    _LOCAL_COPY: dict[str, str] = "
                f"{{{invented!r}: 'The pressing is unwell'}}\n"
                "    return _LOCAL_COPY.get(scenario)\n"
            ),
            "function-local list": (
                "def _rejection_verdict(entry):\n"
                f"    _LOCAL_NAMES = [{invented!r}]\n"
                "    if scenario in _LOCAL_NAMES:\n"
                "        return 'unwell'\n"
            ),
            "unassigned table subscripted in place": (
                "def _rejection_verdict(entry):\n"
                f"    return {{{invented!r}: 'unwell'}}[scenario]\n"
            ),
            "unassigned table with .get": (
                "def _rejection_verdict(entry):\n"
                f"    return {{{invented!r}: 'unwell'}}.get(scenario)\n"
            ),
            "nested-scope table": (
                "def _rejection_verdict(entry):\n"
                "    def _inner():\n"
                f"        _LOCAL_COPY = {{{invented!r}: 'unwell'}}\n"
                "        return _LOCAL_COPY\n"
                "    return _inner().get(scenario)\n"
            ),
            "module-level control": (
                f"_MODULE_COPY = {{{invented!r}: 'unwell'}}\n"
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

    def _assert_planted_source_is_caught(
        self, description: str, source: str,
    ) -> None:
        targets = source_match_targets(source)
        found = [
            (subject, literals)
            for subject, literals in targets.items()
            if self._INVENTED in literals
        ]
        self.assertTrue(found, f"{description} was not discovered")
        for subject, literals in found:
            self.assertTrue(
                check_match_target(subject, literals),
                f"{description} slipped past the audit as {subject!r}",
            )

    def test_every_inline_shape_a_fabricated_trigger_can_take_is_caught(self):
        """The module's own idiom is inline, so every inline form answers.

        Issue #868 review F3: the narrower scan saw dicts and tuples only,
        and a bare constant feeding ``.startswith`` shipped silently.
        """
        for description, source in self._planted_inline_sources().items():
            with self.subTest(description):
                self._assert_planted_source_is_caught(description, source)

    def test_a_copy_table_at_any_scope_is_caught(self) -> None:
        """Issue #882 review B2."""
        for description, source in self._planted_local_tables().items():
            with self.subTest(description):
                self._assert_planted_source_is_caught(description, source)

    def test_a_brand_new_inline_subject_fails_closed(self) -> None:
        """A whole new match chain cannot ship unregistered."""
        targets = source_match_targets("if invented_field == 'anything':\n    pass\n")
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

    def test_a_name_collision_never_drops_the_inline_subject(self) -> None:
        """Review N8: the halves merge, so a module constant sharing a name
        with an inline subject cannot hide that subject's literals."""
        targets = classify_match_targets(
            source=f"if fmt == {self._INVENTED!r}:\n    pass\n",
            namespace={"fmt": "FLAC"},
        )
        self.assertIn(self._INVENTED, targets["fmt"])
        self.assertIn("FLAC", targets["fmt"])
        self.assertTrue(check_match_target("fmt", targets["fmt"]))

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
        self.assertEqual(spelled["a_real_literal"], 1)
        self.assertEqual(spelled["invented_reason"], 0)
        producers = _Producers(("fake.py",), "planted")
        self.assertIsNotNone(check_literal_has_a_producer(
            "invented_reason", producers, {"fake.py": spelled}, historical={}))
        self.assertIsNone(check_literal_has_a_producer(
            "a_real_literal", producers, {"fake.py": spelled}, historical={}))

    def test_a_self_registered_subject_cannot_certify_its_own_typo(self) -> None:
        """Review N4: with the comparison site as the only spelling, a
        badge typo would prove itself."""
        typo = _Producers(
            (_CLASSIFY_RELPATH,), "self-produced", min_occurrences=2)
        spellings = producer_spellings((_CLASSIFY_RELPATH,))
        self.assertIsNotNone(check_literal_has_a_producer(
            "Triaged · download delted", typo, spellings, historical={}))
        self.assertIsNone(check_literal_has_a_producer(
            "Imported", typo, spellings, historical={}))
        # One spelling is enough for a subject produced elsewhere — the
        # raised bar is specific to self-certification.
        loose = _Producers(("fake.py",), "planted")
        self.assertIsNone(check_literal_has_a_producer(
            "Triaged · download delted", loose,
            {"fake.py": Counter({"Triaged · download delted": 1})},
            historical={}))

    def test_a_historical_entry_needs_structured_falsifiable_evidence(self):
        """Review N5: prose is not evidence — a plausible sentence let a
        brand-new invention register as history."""
        producers = _Producers((), "no producer")
        spellings: dict[str, Counter[str]] = {}
        self.assertIsNotNone(check_literal_has_a_producer(
            "invented", producers, spellings, historical={}))
        # No live rows: the entry describes copy nothing can reach.
        self.assertIsNotNone(check_literal_has_a_producer(
            "invented", producers, spellings,
            historical={"invented": _Historical(
                source="download_log.beets_scenario", row_count=0,
                last_seen="2026-03-24")},
        ))
        # No queryable source expression.
        self.assertIsNotNone(check_literal_has_a_producer(
            "invented", producers, spellings,
            historical={"invented": _Historical(
                source="", row_count=12, last_seen="2026-03-24")},
        ))
        # A date a reviewer cannot check against.
        self.assertIsNotNone(check_literal_has_a_producer(
            "invented", producers, spellings,
            historical={"invented": _Historical(
                source="download_log.beets_scenario", row_count=12,
                last_seen="a while back")},
        ))
        self.assertIsNone(check_literal_has_a_producer(
            "invented", producers, spellings,
            historical={"invented": _Historical(
                source="download_log.beets_scenario", row_count=12,
                last_seen="2026-03-24")},
        ))
        # A "historical" literal a producer spells again is a stale entry.
        self.assertIsNotNone(check_literal_has_a_producer(
            "invented",
            _Producers(("fake.py",), "planted"),
            {"fake.py": Counter({"invented": 1})},
            historical={"invented": _Historical(
                source="download_log.beets_scenario", row_count=12,
                last_seen="2026-03-24")},
        ))

    def test_prefix_and_casefold_relaxations_still_require_a_spelling(self):
        prefix = _Producers(("fake.py",), "planted", match="prefix")
        spellings = {"fake.py": Counter({"suspect_lossless_downgrade": 1})}
        self.assertIsNone(check_literal_has_a_producer(
            "suspect_lossless", prefix, spellings, historical={}))
        self.assertIsNotNone(check_literal_has_a_producer(
            "suspect_flac", prefix, spellings, historical={}))
        folded = _Producers(("fake.py",), "planted", casefold=True)
        self.assertIsNone(check_literal_has_a_producer(
            "FLAC", folded, {"fake.py": Counter({"flac": 1})}, historical={}))
        self.assertIsNotNone(check_literal_has_a_producer(
            "FLAK", folded, {"fake.py": Counter({"flac": 1})}, historical={}))


# ---------------------------------------------------------------------------
# The copy the audit corrected, and the claims it must keep making
# ---------------------------------------------------------------------------

def _rejected(scenario: str, validation_result: object = None) -> LogEntry:
    return LogEntry(
        id=1, request_id=2, outcome="rejected", beets_scenario=scenario,
        validation_result=(
            validation_result  # pyright: ignore[reportArgumentType]
        ),
    )


def mbid_not_found_blob(candidate_count: int) -> dict[str, object]:
    """The blob ``lib/beets.py`` writes beside ``mbid_not_found``.

    Built from the producer's own Struct, not a hand-rolled dict
    (``.claude/rules/test-fidelity.md`` Rule C). ``items`` and
    ``recommendation`` are populated because the ``choose_match`` handler
    always sets them before it decides ``mbid_found`` — that is exactly
    what makes them proof beets ran.
    """
    result = ValidationResult(
        valid=False,
        scenario="mbid_not_found",
        mbid_found=False,
        detail="Target MBID rel-1 not in candidates",
        recommendation="none",
        local_track_count=3,
        items=[{"title": f"track {index}"} for index in range(3)],
        candidate_count=candidate_count,
        candidates=[
            CandidateSummary(mbid=f"other-{index}", distance=0.4)
            for index in range(candidate_count)
        ],
    )
    blob: dict[str, object] = msgspec.to_builtins(result)
    return blob


def _rejected_with_error(
    scenario: str, recorded: str, *, with_import_result: bool = True,
) -> LogEntry:
    """A rejection row shaped the way its producer persists one.

    ``harness/import_one.py`` sets ``ImportResult.error`` and the rejection
    writer denormalizes it into ``error_message``; every live
    ``import_failed`` (47), ``crash`` (11), ``mbid_missing`` (10) and
    ``quality_evidence_action_failed`` (2) row carries both, byte-equal.
    ``lib/dispatch/core.py``'s ``exception`` rows carry no ImportResult at
    all, which is what the flag is for.
    """
    entry = _rejected(scenario)
    entry.error_message = recorded
    if with_import_result:
        entry.import_result = msgspec.to_builtins(
            make_import_result(decision=scenario, error=recorded))
    return entry


def extra_tracks_blob(unmatched_tracks: int) -> dict[str, object]:
    """The blob ``lib/beets.py`` writes beside ``extra_tracks``.

    Built from the producer's own Structs (``.claude/rules/test-fidelity.md``
    Rule C). The handler flags the requested release's candidate with
    ``is_target`` and persists beets' ``extra_tracks`` — the release tracks
    its item assignment left unmatched — which is the same array it counts
    for its ``detail`` string.
    """
    result = ValidationResult(
        valid=False,
        scenario="extra_tracks",
        mbid_found=True,
        distance=0.03,
        detail=f"MB has {unmatched_tracks} more tracks than local files",
        recommendation="strong",
        local_track_count=9,
        items=[{"title": f"track {index}"} for index in range(9)],
        candidate_count=1,
        candidates=[CandidateSummary(
            mbid="rel-1",
            distance=0.03,
            is_target=True,
            extra_tracks=[
                HarnessTrackInfo(title=f"unmatched {index}")
                for index in range(unmatched_tracks)
            ],
        )],
    )
    blob: dict[str, object] = msgspec.to_builtins(result)
    return blob


def synthesized_rejection_blob(scenario: str) -> dict[str, object]:
    """A rejection stub built WITHOUT beets — the F4 hazard shape.

    Exactly what ``lib/download_rejection.py`` and
    ``lib/dispatch/outcome_actions.py`` construct: distance, scenario and
    detail only, leaving ``candidates``/``items``/``recommendation`` at
    their defaults. On 2026-07-26, 911 of the 929 live rows carrying a
    zero-candidate validation blob have this shape.
    """
    blob: dict[str, object] = msgspec.to_builtins(ValidationResult(
        distance=0.02, scenario=scenario, detail="synthesized rejection"))
    return blob


class TestFabricatedCopyIsGone(unittest.TestCase):
    """Issue #882: the literals the audit convicted, pinned by outcome."""

    def test_a_populated_candidate_set_reads_as_a_pressing_mismatch(self) -> None:
        """32 of the 50 live rows (2026-07-26): one candidate, not ours."""
        classified = classify_log_entry(
            _rejected("mbid_not_found", mbid_not_found_blob(1)))
        self.assertEqual(
            classified.verdict,
            "Requested release ID not among the match candidates",
        )
        self.assertNotIn("No MusicBrainz match", classified.verdict)

    def test_an_empty_candidate_set_names_the_release_id_lookup(self) -> None:
        """The other 18 live rows carry an empty set — a release-ID lookup
        that came back with nothing, not a pressing mismatch.

        It names the LOOKUP, not the folder: ``lib/beets.py`` always passes
        ``--search-id``, so beets' ``tag_album`` takes its ``if
        search_ids:`` branch and derives candidates from
        ``albums_for_ids`` alone. Live confirmation that the folder is not
        the discriminator: all 13 requests behind those 18 rows have
        sibling attempts on the SAME release ID that did return candidates
        (issue #882 review F1).
        """
        classified = classify_log_entry(
            _rejected("mbid_not_found", mbid_not_found_blob(0)))
        self.assertEqual(
            classified.verdict,
            "Beets returned no match candidates for the requested release ID",
        )
        lowered = classified.verdict.casefold()
        # The original falsehood must not come back in any form: the
        # producer records an empty CANDIDATE SET, never that no match
        # exists anywhere.
        for retracted in (
            "no musicbrainz match", "no match found", "no match exists",
            "no matching release",
        ):
            self.assertNotIn(retracted, lowered)
        # Nor may it point the operator at the folder, which cannot have
        # decided this, or name MusicBrainz — two of the 18 requested
        # Discogs release IDs.
        for misdirection in ("folder", "musicbrainz"):
            self.assertNotIn(misdirection, lowered)

    def test_a_row_without_a_beets_verdict_uses_the_general_sentence(self) -> None:
        """No beets verdict means neither arm is PROVEN, so the general
        sentence stands — it is the weaker claim and is true of an empty
        candidate set as well as a populated one."""
        for blob in (
            None,
            {"scenario": "mbid_not_found"},
            "not json at all",
            synthesized_rejection_blob("mbid_not_found"),
        ):
            with self.subTest(blob=blob):
                classified = classify_log_entry(_rejected("mbid_not_found", blob))
                self.assertEqual(
                    classified.verdict,
                    "Requested release ID not among the match candidates",
                )

    def test_a_synthesized_stub_never_claims_beets_returned_nothing(self) -> None:
        """Review F4: 911 of the 929 live rows with a zero-candidate
        validation blob carry a beets-shaped rejection stub that beets
        never produced. An empty ``candidates`` list there is the Struct
        default, not a verdict.

        The gate therefore requires positive proof beets RAN — the
        ``items`` and ``recommendation`` its ``choose_match`` handler
        writes — and each half is load-bearing on its own.
        """
        stub = synthesized_rejection_blob("mbid_not_found")
        self.assertEqual(stub["candidates"], [])
        self.assertEqual(stub["items"], [])
        self.assertIsNone(stub["recommendation"])
        self.assertFalse(classify._beets_returned_no_candidates(
            _rejected("mbid_not_found", stub)))
        # A stub dressed up with only one of the two signals still fails.
        for partial in ({"items": [{"title": "t"}]}, {"recommendation": "none"}):
            with self.subTest(partial=partial):
                self.assertFalse(classify._beets_returned_no_candidates(
                    _rejected("mbid_not_found", {**stub, **partial})))
        # Both together, with an empty candidate list, is the real thing.
        self.assertTrue(classify._beets_returned_no_candidates(
            _rejected("mbid_not_found", mbid_not_found_blob(0))))
        # …and a real run that DID return candidates is not this arm.
        self.assertFalse(classify._beets_returned_no_candidates(
            _rejected("mbid_not_found", mbid_not_found_blob(1))))

    def test_the_fabricated_key_no_longer_manufactures_a_sentence(self) -> None:
        classified = classify_log_entry(_rejected("no_candidates"))
        self.assertEqual(classified.verdict, "no candidates")

    def test_the_unproduced_triage_actions_no_longer_have_labels(self) -> None:
        for action in ("stale_path_cleared", "stale_path_clear_failed"):
            with self.subTest(action):
                self.assertEqual(
                    classify._wrong_match_action_label(action),
                    action.replace("_", " "),
                )

    def test_the_unmatched_run_literals_name_a_real_producer(self) -> None:
        """Issue #888's two new sentences needed no registry entry, and the
        audit is what proves that rather than the author asserting it:
        ``scenario`` already names ``lib/beets.py`` as a producer, and
        ``lib/beets.py`` spells both literals — taken here from the
        producer's own exported constants, never retyped
        (``.claude/rules/test-fidelity.md`` Rule C).

        The end-to-end pins — REAL ``beets_validate`` runs producing each
        scenario and the classifier rendering its verdict — live in
        ``tests/test_beets_harness_session.py``.
        """
        from lib.beets import (
            NO_CHOOSE_MATCH_SCENARIO,
            VALIDATION_ERROR_SCENARIO,
        )

        producers = MATCH_SUBJECTS["scenario"]
        self.assertIn("lib/beets.py", producers.files)
        spellings = producer_spellings(producers.files)
        for literal in (NO_CHOOSE_MATCH_SCENARIO, VALIDATION_ERROR_SCENARIO):
            with self.subTest(literal):
                self.assertIsNone(check_literal_has_a_producer(
                    literal, producers, spellings))
                self.assertIn(
                    literal,
                    classify_match_targets()["scenario"],
                    "the classifier no longer matches a literal it renders "
                    "copy for",
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


class TestUnhandledScenariosReadAsWords(unittest.TestCase):
    """One module, one fallback doctrine (#882 review).

    ``_wrong_match_action_label`` has always humanized its unhandled
    tokens; ``_rejection_verdict`` dumped the raw machine token, so live
    rows read as ``extra_tracks`` / ``import_failed``. Humanizing invents
    no fact — it is the token itself, spelled for a human.

    Six of the seven tokens this class used to cover earned real copy in
    issue #888 PR4 and moved to
    ``TestProducibleRejectionScenariosNameTheirProducersFact``. What is
    left is the case the fallback exists FOR: a token classify matches
    nothing on, which reaches the operator as words rather than as a
    machine string.

    ``changed_rows`` is measured, not assumed: it is how many live rows
    RENDER this text, from replaying both classifiers over all 36,303
    ``download_log`` rows on 2026-07-26. It is NOT the count of rows
    carrying that ``beets_scenario`` — ``_entry_rejection_decision``
    prefers a rejection-recording ``ImportResult`` decision, so a row can
    render under a different token than its own column holds. 477 rows
    carry ``strong_match``; 5 render "strong match", and 3 more render
    under ``import_failed`` because their ``ImportResult.decision`` says so
    (issue #882 review F5 — the same by-scenario-vs-by-rendered-row trap,
    caught a second time).
    """

    CASES: ClassVar = [
        ("strong_match", "strong match", 5),
    ]

    def test_live_raw_token_scenarios_now_read_as_words(self) -> None:
        for scenario, expected, changed_rows in self.CASES:
            with self.subTest(scenario, changed_rows=changed_rows):
                self.assertEqual(
                    classify_log_entry(_rejected(scenario)).verdict, expected)

    def test_an_empty_scenario_still_says_rejected(self) -> None:
        self.assertEqual(classify_log_entry(_rejected("")).verdict, "Rejected")


class TestProducibleRejectionScenariosNameTheirProducersFact(
    unittest.TestCase,
):
    """Issue #888 PR4: the seven live rejections that had no copy.

    Each one fell through to ``_humanize_token``, so the operator read
    "extra tracks" / "import failed" / "untracked audio" — the machine
    token spelled for a human, which names the discriminator and explains
    nothing. Every sentence below claims exactly what the producer records
    at the site that writes the scenario, and no more.

    The triggers are persisted enum values: four are written by the
    ``harness/import_one.py`` subprocess and two by workers that reject
    before beets is consulted, so they cannot be produced in-process. That
    is the case ``.claude/rules/test-fidelity.md`` Rule C admits a literal
    for, on the condition that a producer audit traces it — which
    ``test_every_producible_literal_is_traced_to_its_producer`` does here,
    through the same checker the audit itself runs.

    Live row counts (2026-07-26), measured by driving the real
    ``_rejection_verdict`` over every live decision × scenario pair:
    ``import_failed`` 47, ``extra_tracks`` 45, ``exception`` 19,
    ``untracked_audio`` 14, ``crash`` 11, ``mbid_missing`` 10,
    ``quality_evidence_action_failed`` 2 — 148 rows in total.
    """

    #: literal -> live rejected rows that RENDER under it.
    LIVE_ROWS: ClassVar = {
        "import_failed": 47,
        "extra_tracks": 45,
        "exception": 19,
        "untracked_audio": 14,
        "crash": 11,
        "mbid_missing": 10,
        "quality_evidence_action_failed": 2,
    }

    def test_every_producible_literal_is_traced_to_its_producer(self) -> None:
        """Rule C's condition for allowing a literal trigger at all."""
        producers = MATCH_SUBJECTS["scenario"]
        spellings = producer_spellings(producers.files)
        matched = classify_match_targets()["scenario"]
        for literal in self.LIVE_ROWS:
            with self.subTest(literal):
                self.assertIsNone(
                    check_literal_has_a_producer(
                        literal, producers, spellings))
                self.assertIn(
                    literal, matched,
                    "the classifier no longer matches a literal it renders "
                    "copy for")

    # -- lib/beets.py, the choose_match handler ------------------------
    def test_extra_tracks_counts_the_producers_own_unmatched_tracks(self):
        """``lib/beets.py`` writes this the moment the requested release IS
        the matched candidate and beets left tracks of it unassigned.

        The count is read from the producer's own ``extra_tracks`` array on
        the target candidate — the array it counted to compose its own
        ``detail`` — so nothing parses the sentence. All 45 live rows carry
        that array, and its length equals the number in their persisted
        detail on every one of them.
        """
        classified = classify_log_entry(
            _rejected("extra_tracks", extra_tracks_blob(3)))
        self.assertEqual(
            classified.verdict,
            "Requested release has 3 tracks with no matching local file")

    def test_extra_tracks_agrees_in_number_with_one_missing_track(self) -> None:
        """29 of the 45 live rows are a single unmatched track."""
        classified = classify_log_entry(
            _rejected("extra_tracks", extra_tracks_blob(1)))
        self.assertEqual(
            classified.verdict,
            "Requested release has 1 track with no matching local file")

    def test_extra_tracks_without_the_array_states_only_the_shape(self) -> None:
        """No structured evidence, no number — never a guessed one."""
        for blob in (None, {"scenario": "extra_tracks"}, "not json at all",
                     synthesized_rejection_blob("extra_tracks")):
            with self.subTest(blob=blob):
                self.assertEqual(
                    classify_log_entry(_rejected("extra_tracks", blob)).verdict,
                    "Requested release has tracks with no matching local file")

    # -- harness/import_one.py ------------------------------------------
    def test_import_failed_quotes_the_importers_own_recorded_reason(self) -> None:
        """Five producer sites share this decision and differ only in the
        reason each records, so the reason IS the discriminator."""
        recorded = (
            "Post-import: release 07d51bc7 has multiple beets album rows "
            "[10583, 19190]")
        classified = classify_log_entry(_rejected_with_error(
            "import_failed", recorded))
        self.assertEqual(classified.verdict, f"Import failed: {recorded}")

    def test_import_failed_without_a_reason_claims_only_the_end_state(self):
        self.assertEqual(
            classify_log_entry(_rejected("import_failed")).verdict,
            "Import did not leave beets in the expected state")

    def test_crash_quotes_the_unhandled_exception_it_recorded(self) -> None:
        """``import_one.py``'s top-level envelope records
        ``f"{type(exc).__name__}: {exc}"`` and nothing else."""
        recorded = "FileNotFoundError: [Errno 2] No such file or directory: 'beet'"
        classified = classify_log_entry(_rejected_with_error("crash", recorded))
        self.assertEqual(classified.verdict, f"Import crashed: {recorded}")

    def test_crash_without_a_reason_still_names_the_unhandled_exception(self):
        self.assertEqual(
            classify_log_entry(_rejected("crash")).verdict,
            "Import crashed with an unhandled exception")

    def test_mbid_missing_names_the_import_candidate_set_it_was_absent_from(
        self,
    ) -> None:
        """rc=4 has ONE producer site: ``run_import`` answered ``skip``
        because the requested release was not among the candidates beets
        offered at import, so nothing was applied.

        Unlike ``import_failed`` the decision name already fixes the
        reason, so the sentence states it instead of quoting a recorded
        string that adds nothing — all 10 live rows record the pre-#865
        fallback "Harness returned rc=4", which the card still shows as its
        Detail row.
        """
        expected = (
            "Requested release ID was not among the import candidates; "
            "nothing was applied")
        self.assertEqual(
            classify_log_entry(_rejected("mbid_missing")).verdict, expected)
        self.assertEqual(
            classify_log_entry(
                _rejected_with_error("mbid_missing", "Harness returned rc=4")
            ).verdict,
            expected,
            "a recorded string that adds no fact must not enter the sentence")

    def test_quality_evidence_action_failure_says_beets_never_ran(self) -> None:
        """Both producer sites ``_emit_and_exit`` before ``run_import``."""
        recorded = "10 opus 128 conversions failed"
        classified = classify_log_entry(_rejected_with_error(
            "quality_evidence_action_failed", recorded))
        self.assertEqual(
            classified.verdict,
            f"Quality-evidence action failed before beets ran: {recorded}")
        self.assertEqual(
            classify_log_entry(
                _rejected("quality_evidence_action_failed")).verdict,
            "Quality-evidence action failed before beets ran")

    # -- the manifest guards --------------------------------------------
    def test_untracked_audio_claims_a_mismatch_not_extra_audio(self) -> None:
        """``check_audio_manifest`` reports extra AND missing audio, and
        ``_check_staged_audio_manifest`` labels either one
        ``untracked_audio`` — so "contains extra audio" would be false of a
        source that is merely short, however the live rows read today."""
        verdict = classify_log_entry(_rejected("untracked_audio")).verdict
        self.assertEqual(
            verdict, "Import folder does not match the selected audio manifest")
        self.assertNotIn("extra", verdict.casefold())

    # -- lib/dispatch/core.py -------------------------------------------
    def test_exception_says_where_the_traceback_went(self) -> None:
        """The producer logs the traceback and persists only the words
        "exception" / "unhandled exception in auto-import", so quoting the
        row would hand the operator the token back."""
        self.assertEqual(
            classify_log_entry(_rejected_with_error("exception", "exception")
                               ).verdict,
            "Auto-import raised an unhandled exception; the traceback is in "
            "the service log")

    # -- the shared bound ------------------------------------------------
    def test_a_long_recorded_reason_is_bounded_not_dumped(self) -> None:
        """The verdict is the collapsed list row; one 4KB beets traceback
        must not become the operator's whole worklist line."""
        recorded = "sqlite3.OperationalError: " + ("x" * 900)
        verdict = classify_log_entry(
            _rejected_with_error("import_failed", recorded)).verdict
        self.assertTrue(verdict.startswith("Import failed: sqlite3."))
        self.assertLess(len(verdict), len(recorded))
        self.assertTrue(verdict.endswith("…"))


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
