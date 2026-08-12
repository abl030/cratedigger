"""Producer audit for copy-PIN marker constants (issue #1111, item 2).

``tests/test_protected_path_truth_generated.py`` polices the Wrong Matches
explorer's operator copy with module-level constants like
``_REFUSAL_COPY = "could not be read"``, checked with ``marker in html``
against real production-rendered text. Issue #1086's review found that one
of the two paragraphs ``_REFUSAL_COPY`` was written to police read *"The
server could not read this folder's contents"* — no "be" — so the marker
had never matched that paragraph. The checker was silently inert for it,
before AND after the fix it was meant to enforce, because the SAME html
blob also carried a different, already-correct sentence (the per-entry lead
notice) that satisfied the aggregate ``marker in html`` check on its own.
Found only because an implementer noticed a fix pass a checker that could
not have been exercising it.

``.claude/rules/test-fidelity.md`` Rule C already requires a copy pin's
*trigger* (the input fed INTO a producer) to come from the producer. This
module is the inverse direction, for the *marker the assertion matches
copy AGAINST*: every copy-marker constant in the participating test
modules must be a substring of a string literal some named production
module actually SPELLS — parsed, never grepped, so a mention in a comment
or docstring cannot masquerade as production output.

Shape, modelled on ``tests/test_wrong_match_scenario_producer_audit.py``
and ``tests/test_classify_producer_audit.py``:

* **The marker set is DERIVED, not hand-listed.** ``every_discovered_
  marker`` walks the MODULE-LEVEL statements of each file in
  ``_PARTICIPATING_TEST_MODULES`` (an explicit, small, reviewable list —
  same bound as those two modules' own producer-file lists) via AST, and
  collects every bare string constant whose name ends in one of
  ``_MARKER_NAME_SUFFIXES`` — the two suffixes actually in use today
  (``_COPY``, ``_QUALIFIER``; surveyed by grepping the whole ``tests/``
  tree for ``_COPY``/``_QUALIFIER``-suffixed constants matched via
  ``in html`` / ``in text`` / ``assertIn`` against real rendered copy —
  the survey also found ``_ACTION_COPY`` in
  ``tests/test_path_authority_generated.py``, but that constant is a
  FIXTURE PATH fed as an INPUT, never matched against rendered output, so
  it is correctly excluded by not being in ``_PARTICIPATING_TEST_MODULES``
  rather than by a name-based carve-out). A marker suffix outside that
  bounded set is invisible to this audit, the same trade-off
  ``test_wrong_match_scenario_producer_audit.py``'s docstring names for
  its own ``scenario=``/``.scenario =`` grammar.
* **A discovered marker with no classification FAILS CLOSED.** Every
  marker found by that scan must be registered in exactly one of
  ``_MARKER_PRODUCERS`` (a copy-pin marker, with the production files that
  back it) or ``_EXEMPT_MARKERS`` (discovered by the same name grammar but
  not itself matched against rendered copy, with the reason) —
  ``check_marker_is_classified`` fails on anything in neither.
  ``_EXEMPT_MARKERS`` is currently empty: every ``_COPY``/``_QUALIFIER``
  constant the one participating module declares today IS real matched
  copy (the file's two ``_WORKER``-suffixed constants, JS harness source
  for the Node worker, don't even reach discovery — their names fail the
  suffix grammar, proven by
  ``TestTheAuditIsFailClosed.test_worker_source_constants_are_not_discovered``).
  The bucket stays wired and self-tested (with an injected fabricated
  entry) for the day a real one is needed, the same escape hatch
  ``test_failure_presentation.py``'s ``_NON_TRIGGER_CONSTANTS`` and
  ``test_wrong_match_scenario_producer_audit.py``'s exclusion sets are.
* **Evidence is a spelling, not a mention, in BOTH languages.**
  ``_python_literal_fragments`` walks Python source via ``ast`` and
  additionally excludes bare string-literal EXPRESSION STATEMENTS
  (docstrings, and loose descriptive strings like
  ``lib/fs_authority.py``'s ``FsAuthorityCode`` annotation) — narrower than
  ``tests/test_classify_producer_audit.py``'s own ``spelled_string_
  literals``, because THAT module matches a literal against a producer
  fragment by EXACT or PREFIX equality, while this audit matches by
  SUBSTRING (the shape issue #1111 explicitly asks for) — substring
  matching is far more likely to accidentally hit a short marker phrase
  buried in a long prose paragraph, exactly the "mention in a docstring"
  hiding place Rule C warns about. ``_js_literal_fragments`` is a
  hand-rolled, bounded JS lexer (no parser library is available in the
  nix shell, and none is a repo dependency — this project ships no
  build step) that recognises exactly: quoted strings, template literals
  split at ``${...}`` boundaries, `//`/`/* */` comments (including
  JSDoc's `/** */`, skipped only outside any string/template state so a
  literal `//` inside a string is never misread as a comment), and JS's
  regex-vs-division ambiguity via the standard "does the previous
  significant token allow a regex here" heuristic — required, not
  decorative, because every registered JS producer file below uses
  `.replace(/"/g, ...)`-shaped regex literals whose PATTERN contains a
  literal quote character.
* **F-strings and template literals decompose into fragments, the same
  way.** Python's own AST already splits an f-string into one
  ``ast.Constant`` per literal segment between ``{...}`` placeholders —
  ``_python_literal_fragments`` needs no special f-string handling at all
  for that reason, it is a free consequence of walking ``ast.Constant``
  nodes. ``_js_literal_fragments`` reproduces the same decomposition by
  hand for JS template literals, splitting at each top-level ``${`` /
  matching ``}`` pair. Both keep the literal TEXT and drop the
  interpolated expression, which is exactly what this audit needs: the
  marker is matched against the text a producer actually prints, not
  against the placeholder.
* **Registry of marker to producer files is explicit and small.**
  ``_MARKER_PRODUCERS`` names, per marker, the exact production files
  that spell it — never repo-wide discovery (a marker this short would
  match prose almost anywhere).

Found on the first run: nothing currently inert — every discovered marker
already resolves against its registered producer(s) (verified below,
``TestEveryMarkerHasAProducer``). The class guard exists so the NEXT typo
in this family fails a test instead of shipping fluent and wrong for a
second time.
"""

from __future__ import annotations

import ast
import functools
import os
import unittest
from collections.abc import Mapping, Sequence
from dataclasses import dataclass

_REPO_ROOT = os.path.join(os.path.dirname(__file__), "..")

#: The bounded naming grammar. Surveyed against the whole ``tests/`` tree
#: (grep for ``_COPY``/``_QUALIFIER``-suffixed module constants, then read
#: every usage site to confirm it is matched via containment against real
#: rendered copy, not e.g. a fixture path such as ``_ACTION_COPY``).
_MARKER_NAME_SUFFIXES: tuple[str, ...] = ("_COPY", "_QUALIFIER")

#: The test modules this audit trusts to declare copy-pin markers. Widening
#: this list widens which files' module-level constants are scanned at
#: all — the same bound ``test_wrong_match_scenario_producer_audit.py``'s
#: ``_PRODUCER_FILES`` and ``test_classify_producer_audit.py``'s producer
#: registries both use, for the same reason: not repo-wide discovery.
_PARTICIPATING_TEST_MODULES: tuple[str, ...] = (
    "tests/test_protected_path_truth_generated.py",
)


@dataclass(frozen=True)
class _Producers:
    """Which production files can spell a copy-pin marker, and why."""

    files: tuple[str, ...]
    why: str


# ---------------------------------------------------------------------------
# The registry — every marker discovered above, classified
# ---------------------------------------------------------------------------

#: Copy-pin markers: matched via containment against real rendered/
#: computed copy, so they must be a substring of something a registered
#: production file actually spells.
_MARKER_PRODUCERS: dict[str, _Producers] = {
    "_LOAD_FAILURE_COPY": _Producers(
        files=("web/js/util.js",),
        why=(
            "wrongMatchExplorerFailureCopy's fallback sentence for a "
            "status it does not special-case"
        ),
    ),
    "_REFUSAL_COPY": _Producers(
        files=(
            "web/js/util.js",
            "web/js/wrong-matches.js",
            "lib/fs_authority.py",
        ),
        why=(
            "the whole-root 503 lead sentence (web/js/util.js), the "
            "per-entry and empty-state world-failure wording "
            "(web/js/wrong-matches.js), and the echoed "
            "unreadable_reason_text('open_failed', ...) "
            "(lib/fs_authority.py) all spell it — issue #1086's fix"
        ),
    ),
    "_CONTAINMENT_REFUSAL_COPY": _Producers(
        files=("web/js/wrong-matches.js",),
        why="the per-entry and empty-state containment-refusal wording",
    ),
    "_NOT_EMPTY_COPY": _Producers(
        files=("web/js/wrong-matches.js",),
        why="both empty-state branches deny the empty read, one wording",
    ),
    "_WHOLE_ROOT_UNAVAILABLE_LOAD_COPY": _Producers(
        files=("web/js/util.js",),
        why="wrongMatchExplorerFailureCopy's 503 branch",
    ),
    "_INCOMPLETE_BADGE_COPY": _Producers(
        files=("web/js/replace_picker.js",),
        why="formatDistanceBadge's partial-read qualifier suffix",
    ),
    "_CONTAINMENT_QUALIFIER": _Producers(
        files=("web/js/replace_picker.js",),
        why="distanceIncompleteQualifier's containment branch",
    ),
    "_WORLD_FAILURE_QUALIFIER": _Producers(
        files=("web/js/replace_picker.js", "lib/fs_authority.py"),
        why=(
            "distanceIncompleteQualifier's world-failure branch, and "
            "unreadable_reason_text's own suffix wording"
        ),
    ),
}

#: Would hold any marker discovered by the same name grammar
#: (``*_COPY``/``*_QUALIFIER``) that is NOT itself matched against
#: rendered copy (a producer-file claim would be meaningless for it) —
#: currently empty; see the module docstring for why, and
#: ``TestTheAuditIsFailClosed`` for the injected-override self-test that
#: proves the mechanism without a live example.
_EXEMPT_MARKERS: dict[str, str] = {}


# ---------------------------------------------------------------------------
# Discovery — bounded AST scan of the participating test modules
# ---------------------------------------------------------------------------

@functools.cache
def _read_source(relpath: str) -> str:
    with open(os.path.join(_REPO_ROOT, relpath), encoding="utf-8") as handle:
        return handle.read()


def discovered_markers(relpath: str) -> dict[str, str]:
    """Module-level ``*_COPY``/``*_QUALIFIER`` string constants in one file.

    Restricted to ``tree.body`` (direct children of the module) rather
    than a full ``ast.walk`` — "module-level" means exactly that, and it
    keeps a function-local variable that happens to share the suffix (none
    exist today) from being mistaken for a copy-pin marker.
    """
    tree = ast.parse(_read_source(relpath))
    found: dict[str, str] = {}
    for node in tree.body:
        if not isinstance(node, ast.Assign):
            continue
        if not (
            isinstance(node.value, ast.Constant)
            and isinstance(node.value.value, str)
        ):
            continue
        for target in node.targets:
            if (
                isinstance(target, ast.Name)
                and target.id.endswith(_MARKER_NAME_SUFFIXES)
            ):
                found[target.id] = node.value.value
    return found


def every_discovered_marker(
    relpaths: Sequence[str] = _PARTICIPATING_TEST_MODULES,
) -> dict[str, str]:
    found: dict[str, str] = {}
    for relpath in relpaths:
        found.update(discovered_markers(relpath))
    return found


# ---------------------------------------------------------------------------
# Producer evidence — literal fragments a production file actually spells
# ---------------------------------------------------------------------------

class _ProducedPythonLiteralScan(ast.NodeVisitor):
    """Every string a Python module EMITS — not one it merely DOCUMENTS.

    A bare string-literal expression STATEMENT (a module/class/function
    docstring, or a loose descriptive string such as
    ``lib/fs_authority.py``'s ``FsAuthorityCode`` type-alias annotation) is
    prose, never itself produced at runtime — skip that whole subtree so
    it cannot supply a false substring match. Every other string
    (assignment values, f-string fragments, call arguments, dict/list
    entries, return values, ...) is collected normally via
    ``generic_visit``.
    """

    def __init__(self) -> None:
        self.fragments: list[str] = []

    def visit_Expr(self, node: ast.Expr) -> None:
        if isinstance(node.value, ast.Constant) and isinstance(
            node.value.value, str
        ):
            return
        self.generic_visit(node)

    def visit_Constant(self, node: ast.Constant) -> None:
        if isinstance(node.value, str):
            self.fragments.append(node.value)


def _python_literal_fragments(source: str) -> list[str]:
    scan = _ProducedPythonLiteralScan()
    scan.visit(ast.parse(source))
    return scan.fragments


#: Characters after which a bare ``/`` is JS regex-literal syntax rather
#: than division — the standard lexer heuristic: an operator, an opening
#: bracket, a comma, or (via ``None``) the start of the source.
_JS_REGEX_MAY_FOLLOW: frozenset[str] = frozenset("([{,;:!&|=?+-*%^~<>\n")


def _js_literal_fragments(source: str) -> list[str]:
    """Every string/template-literal TEXT a JS module actually emits.

    A hand-rolled, deliberately bounded lexer — not a JS parser, and no
    control-flow or data-flow inference
    (``.claude/rules/code-quality.md`` § "Semantic source scanners are
    prohibited"). No JS parser library is available in this repo's nix
    shell, and the project ships no build step / npm dependency to add
    one. It recognises exactly:

    * single- and double-quoted strings (with ``\\``-escapes);
    * template literals, split into literal fragments at top-level
      ``${...}`` interpolation boundaries — the template-literal
      analogue of an f-string's ``ast.Constant`` fragments, see
      ``_python_literal_fragments``'s docstring;
    * ``//`` line comments and ``/* ... */`` block comments (including
      JSDoc's ``/** */``, which starts with ``/*``) — recognised only
      OUTSIDE any string/template state, so a literal ``//`` or ``/*``
      inside a string is never mistaken for a comment start;
    * JS's regex-vs-division ambiguity, via the same heuristic real JS
      lexers use: a bare ``/`` starts a regex literal only when the
      previous significant character cannot end an expression (see
      ``_JS_REGEX_MAY_FOLLOW``) or nothing has been seen yet — otherwise
      it is division and left untouched. This is required, not
      decorative: every registered JS producer file below spells
      ``.replace(/"/g, ...)``-shaped regex literals whose PATTERN
      contains a literal quote character, which a naive "any quote
      starts a string" scan misreads as an unterminated string
      consuming the rest of the file
      (``TestTheAuditIsFailClosed.test_js_regex_literal_does_not_
      swallow_the_rest_of_the_file`` below proves the fix; run it
      against a reverted lexer to see the failure it guards).

    Not handled, because neither shape occurs in any registered producer
    file (and both would need real JS parsing, not a bounded lexer): a
    template literal nested inside another template literal's
    ``${...}`` interpolation whose OWN literal text contains an
    unbalanced ``{``/``}``, and a comment token appearing inside a
    regex-literal character class.
    """
    fragments: list[str] = []
    i = 0
    n = len(source)
    prev_significant: str | None = None

    def skip_regex(start: int) -> int:
        j = start + 1
        in_class = False
        while j < n:
            c = source[j]
            if c == "\\" and j + 1 < n:
                j += 2
                continue
            if c == "[":
                in_class = True
            elif c == "]":
                in_class = False
            elif c == "/" and not in_class:
                j += 1
                break
            elif c == "\n":
                return start + 1  # unterminated on this line; not a regex
            j += 1
        while j < n and source[j].isalpha():
            j += 1  # trailing flags
        return j

    while i < n:
        ch = source[i]
        if ch == "/" and i + 1 < n and source[i + 1] == "/":
            i += 2
            while i < n and source[i] != "\n":
                i += 1
            continue
        if ch == "/" and i + 1 < n and source[i + 1] == "*":
            i += 2
            while i + 1 < n and not (
                source[i] == "*" and source[i + 1] == "/"
            ):
                i += 1
            i += 2
            continue
        if ch == "/" and (
            prev_significant is None
            or prev_significant in _JS_REGEX_MAY_FOLLOW
        ):
            new_i = skip_regex(i)
            if new_i != i + 1:
                i = new_i
                prev_significant = "/"
                continue
        if ch in ("'", '"'):
            quote = ch
            i += 1
            buf: list[str] = []
            while i < n and source[i] != quote:
                if source[i] == "\\" and i + 1 < n:
                    buf.append(source[i + 1])
                    i += 2
                    continue
                buf.append(source[i])
                i += 1
            i += 1
            fragments.append("".join(buf))
            prev_significant = quote
            continue
        if ch == "`":
            i += 1
            buf = []
            while i < n:
                if source[i] == "\\" and i + 1 < n:
                    buf.append(source[i + 1])
                    i += 2
                    continue
                if source[i] == "`":
                    break
                if source[i] == "$" and i + 1 < n and source[i + 1] == "{":
                    fragments.append("".join(buf))
                    buf = []
                    i += 2
                    depth = 1
                    while i < n and depth > 0:
                        if source[i] == "{":
                            depth += 1
                        elif source[i] == "}":
                            depth -= 1
                        i += 1
                    continue
                buf.append(source[i])
                i += 1
            fragments.append("".join(buf))
            i += 1
            prev_significant = "`"
            continue
        if not ch.isspace():
            prev_significant = ch
        i += 1
    return fragments


def production_literal_fragments(relpath: str) -> tuple[str, ...]:
    """Every literal a registered production file spells, by extension."""
    source = _read_source(relpath)
    if relpath.endswith(".py"):
        return tuple(_python_literal_fragments(source))
    if relpath.endswith(".js"):
        return tuple(_js_literal_fragments(source))
    raise ValueError(f"no literal extractor registered for {relpath!r}")


# ---------------------------------------------------------------------------
# Classification checks
# ---------------------------------------------------------------------------

def check_marker_is_classified(
    name: str,
    *,
    producers: Mapping[str, _Producers] | None = None,
    exempt: Mapping[str, str] | None = None,
) -> str | None:
    """Return why a discovered marker is unaccounted for, or ``None``.

    Module-level, with injectable ``producers``/``exempt`` overrides
    (defaulting to the real registries), so the known-bad self-tests can
    prove the exemption bucket's mechanics without needing a live entry.
    """
    known_producers = _MARKER_PRODUCERS if producers is None else producers
    known_exempt = _EXEMPT_MARKERS if exempt is None else exempt
    if name in known_producers or name in known_exempt:
        return None
    return (
        f"{name!r} is a module-level constant matching the "
        f"{_MARKER_NAME_SUFFIXES} naming grammar in a participating test "
        "module, but is registered neither in _MARKER_PRODUCERS nor "
        "_EXEMPT_MARKERS — classify it"
    )


def check_marker_has_a_producer(
    marker_value: str,
    producers: _Producers,
    *,
    fragments_by_file: Mapping[str, Sequence[str]] | None = None,
) -> str | None:
    """Return why ``marker_value`` is unproducible, or ``None`` when real.

    Module-level so the known-bad self-tests can hand it a fabricated
    ``fragments_by_file`` exactly the way the discovery-driven checks use
    the real files.
    """
    for relpath in producers.files:
        if fragments_by_file is not None:
            fragments: Sequence[str] = fragments_by_file.get(relpath, ())
        else:
            try:
                fragments = production_literal_fragments(relpath)
            except OSError:
                return f"{relpath} does not exist"
        if any(marker_value in fragment for fragment in fragments):
            return None
    return (
        f"{marker_value!r} is not a substring of any string literal "
        f"spelled by {producers.files} ({producers.why})"
    )


# ---------------------------------------------------------------------------
# The affirmative tests — every REAL discovered marker, against REAL files
# ---------------------------------------------------------------------------

class TestEveryMarkerHasAProducer(unittest.TestCase):
    def test_no_discovered_marker_is_unclassified(self) -> None:
        violations = [
            violation
            for name in sorted(every_discovered_marker())
            if (violation := check_marker_is_classified(name)) is not None
        ]
        self.assertEqual(violations, [])

    def test_every_classified_marker_is_a_substring_of_a_real_producer(
        self,
    ) -> None:
        markers = every_discovered_marker()
        for name, producers in sorted(_MARKER_PRODUCERS.items()):
            with self.subTest(name):
                self.assertIn(
                    name, markers,
                    f"{name!r} is registered but no participating test "
                    "module declares it any more — stale registry entry",
                )
                violation = check_marker_has_a_producer(
                    markers[name], producers)
                self.assertIsNone(violation, violation)

    def test_every_exempt_marker_still_exists_and_carries_a_reason(
        self,
    ) -> None:
        markers = every_discovered_marker()
        for name, reason in _EXEMPT_MARKERS.items():
            with self.subTest(name):
                self.assertIn(
                    name, markers,
                    f"{name!r} is exempted but no participating test "
                    "module declares it any more — stale exemption",
                )
                self.assertGreater(
                    len(reason), 8, "exemptions carry a real reason")


# ---------------------------------------------------------------------------
# Known-bad self-tests: a checker that cannot fail proves nothing
# ---------------------------------------------------------------------------

class TestTheAuditIsFailClosed(unittest.TestCase):
    def test_a_marker_with_no_producer_is_rejected(self) -> None:
        violation = check_marker_has_a_producer(
            "the pressing skipped sideways",
            _Producers(files=("lib/fs_authority.py",), why="real file"),
        )
        assert violation is not None
        self.assertIn("the pressing skipped sideways", violation)

    def test_an_undeclared_producer_file_is_rejected(self) -> None:
        violation = check_marker_has_a_producer(
            "anything",
            _Producers(
                files=("lib/this_file_does_not_exist.py",), why="typo"),
        )
        assert violation is not None
        self.assertIn("does not exist", violation)

    def test_an_unclassified_marker_is_rejected(self) -> None:
        violation = check_marker_is_classified("_INVENTED_SIDEWAYS_COPY")
        assert violation is not None
        self.assertIn("_INVENTED_SIDEWAYS_COPY", violation)

    def test_a_classified_marker_passes(self) -> None:
        self.assertIsNone(check_marker_is_classified("_REFUSAL_COPY"))

    def test_a_marker_registered_only_as_exempt_passes(self) -> None:
        """Proves the (currently-empty) exemption bucket's mechanics.

        ``_EXEMPT_MARKERS`` has no live members today (every ``_COPY``/
        ``_QUALIFIER`` constant the one participating module declares IS
        real matched copy) — the injectable ``exempt=`` override lets this
        self-test exercise the bucket without needing a live example, the
        same way ``check_marker_has_a_producer``'s ``fragments_by_file``
        override does for producer evidence.
        """
        self.assertIsNone(check_marker_is_classified(
            "_FIXTURE_PATH_COPY",
            exempt={"_FIXTURE_PATH_COPY": "an input fixture, not matched copy"},
        ))

    def test_an_unrecognised_extension_has_no_extractor(self) -> None:
        with self.assertRaises(ValueError):
            production_literal_fragments("web/index.html")

    def test_worker_source_constants_are_not_discovered(self) -> None:
        """The ``_WORKER`` suffix fails the naming grammar entirely.

        ``_EXPLORER_BROWSER_WORKER``/``_DISTANCE_BADGE_WORKER`` are JS
        harness source embedded as Python string constants — real
        module-level string constants in the participating file, but
        never candidates for classification at all, because discovery is
        bounded to the ``_COPY``/``_QUALIFIER`` suffix grammar.
        """
        markers = discovered_markers(
            "tests/test_protected_path_truth_generated.py")
        self.assertNotIn("_EXPLORER_BROWSER_WORKER", markers)
        self.assertNotIn("_DISTANCE_BADGE_WORKER", markers)

    def test_a_python_docstring_mention_is_not_a_spelling(self) -> None:
        """A prose paragraph containing the marker's words is not output.

        Reconstructs the exact risk this audit's substring matching adds
        over ``tests/test_classify_producer_audit.py``'s exact/prefix
        matching: a long docstring that happens to CONTAIN a short marker
        phrase must not count as a producer.
        """
        source = (
            '"""This module explains that the folder could not be read '
            'when the disk misbehaves, as prose, never as output."""\n'
            "REAL_MESSAGE = 'genuinely different text'\n"
        )
        fragments = _python_literal_fragments(source)
        self.assertTrue(
            all("could not be read" not in f for f in fragments),
            f"the docstring mention leaked into the spelled fragments: "
            f"{fragments!r}",
        )
        self.assertIn("genuinely different text", fragments)

    def test_a_js_comment_mention_is_not_a_spelling(self) -> None:
        source = (
            "// the old copy used to say 'could not be read' here\n"
            "/* still describing 'could not be read' in a block comment */\n"
            "const realCopy = 'a totally different sentence';\n"
        )
        fragments = _js_literal_fragments(source)
        self.assertTrue(
            all("could not be read" not in f for f in fragments),
            f"a comment mention leaked into the spelled fragments: "
            f"{fragments!r}",
        )
        self.assertIn("a totally different sentence", fragments)

    def test_js_regex_literal_does_not_swallow_the_rest_of_the_file(
        self,
    ) -> None:
        """Fail-closed proof for the regex-vs-string ambiguity.

        Every registered JS producer file spells a
        ``.replace(/"/g, ...)``-shaped regex literal. Without the
        regex heuristic, the ``"`` inside the pattern starts a bogus
        string that never finds its closing quote on the same line and
        swallows everything after it — including the real marker two
        lines later. Reverting the ``_JS_REGEX_MAY_FOLLOW`` branch
        reproduces exactly that failure.
        """
        source = (
            "function esc(s) {\n"
            "  return s.replace(/\"/g, '&quot;');\n"
            "}\n"
            "const notice = 'could not be read';\n"
        )
        fragments = _js_literal_fragments(source)
        self.assertIn("&quot;", fragments)
        self.assertIn("could not be read", fragments)

    def test_js_template_literal_splits_at_interpolation_like_an_fstring(
        self,
    ) -> None:
        source = "const s = `entries ${count} could not be read here`;\n"
        fragments = _js_literal_fragments(source)
        self.assertIn("entries ", fragments)
        self.assertIn(" could not be read here", fragments)
        self.assertTrue(all("${" not in f for f in fragments))

    def test_the_1086_incident_reconstructed(self) -> None:
        """The exact historical world (commit ``846c54bd``), isolated.

        The real ``web/js/wrong-matches.js`` file also spelled the
        CORRECT "could not be read" wording elsewhere (the per-entry
        lead sentence) even while this empty-state paragraph carried the
        typo — so checking the marker against the WHOLE FILE would not,
        on its own, have caught this specific bug (this audit's
        producer-file evidence is FILE-level, the same deliberately-not-
        closed bound ``tests/test_classify_producer_audit.py``'s
        docstring names for itself). What this test proves instead is
        that the audit's LOGIC is sound: isolated to just the paragraph
        that was actually wrong, ``check_marker_has_a_producer`` rejects
        the pre-fix wording and accepts the post-fix wording.
        """
        producers = _Producers(
            files=("wrong-matches.js",),
            why="the empty-state paragraph alone",
        )
        pre_fix_source = (
            "const emptyText = 'The server could not read this "
            "folder\\u2019s contents, so no listing is available. This "
            "is NOT evidence that the folder is empty.';\n"
        )
        post_fix_source = (
            "const emptyText = 'This folder\\u2019s contents could not "
            "be read, so no listing is available. This is NOT evidence "
            "that the folder is empty.';\n"
        )
        pre_fix_fragments = {
            "wrong-matches.js": _js_literal_fragments(pre_fix_source)}
        post_fix_fragments = {
            "wrong-matches.js": _js_literal_fragments(post_fix_source)}

        violation = check_marker_has_a_producer(
            "could not be read", producers,
            fragments_by_file=pre_fix_fragments,
        )
        self.assertIsNotNone(
            violation,
            "the pre-fix paragraph never says 'could not be read' — the "
            "audit must reject it",
        )

        fixed = check_marker_has_a_producer(
            "could not be read", producers,
            fragments_by_file=post_fix_fragments,
        )
        self.assertIsNone(
            fixed,
            "the post-fix paragraph does say 'could not be read' — the "
            "audit must accept it",
        )


if __name__ == "__main__":
    unittest.main()
