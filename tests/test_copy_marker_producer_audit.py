"""Producer audit for copy-PIN marker constants (issue #1111, item 2).

**What this guard catches, stated plainly:** a copy-pin marker constant
(``_REFUSAL_COPY = "could not be read"`` and its siblings in
``tests/test_protected_path_truth_generated.py``, checked with ``marker in
html`` against real production-rendered text) that NO registered production
file spells ANYWHERE — a fully-inert marker, a producer whose text was
deleted or reworded without updating the marker, a producer file renamed
without updating the registry, or the marker's sole spelling drifting away
character-for-character. Evidence is FILE-level: a marker resolves the
moment *any* string literal in *any* of its registered producer files
contains it as a substring, not the specific sentence a docstring says it
was written to police.

**What this guard does NOT catch — stated equally plainly, because a
reviewer proved it empirically (running the checker against the real
pre-fix tree at commit ``dc0b7c2f^`` and getting a clean pass):** the
issue #1086 SAME-FILE variant. ``_REFUSAL_COPY`` polices two DIFFERENT
sentences in ``web/js/wrong-matches.js`` — the per-entry lead notice and
the empty-state paragraph. Pre-fix, the empty-state paragraph read *"The
server could not read this folder's contents"* — no "be" — while the
lead notice, a few lines away in the SAME file, already correctly said
"could not be read". File-level evidence resolves the marker through the
CORRECT sentence regardless of what the WRONG sentence said, so this guard
would not have caught issue #1086's specific defect on its own; it was
found by an implementer noticing a fix pass a checker that could not have
been exercising it. Tightening to site/sentence-level attribution would
mean inferring WHICH statement a marker is "supposed" to police from
control flow — the prohibited semantic-scanner shape
(``.claude/rules/code-quality.md`` § "Semantic source scanners are
prohibited") — so this audit deliberately keeps file-level granularity and
names the limit instead of pretending to close it.
``TestTheAuditIsFailClosed.test_the_1086_full_file_evidence_resolves_green_a_known_limit``
proves the limit with the real historical text; the paired
``test_a_marker_absent_from_every_registered_file_is_red`` proves the
class this guard DOES catch — a marker with no producer at all.

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
  marker`` walks the MODULE-LEVEL ``Assign``/``AnnAssign`` statements of
  each file in ``_PARTICIPATING_TEST_MODULES`` (an explicit, small,
  reviewable list — same bound as those two modules' own producer-file
  lists) via AST, and collects every constant whose name ends in one of
  ``_MARKER_NAME_SUFFIXES`` — the two suffixes actually in use today
  (``_COPY``, ``_QUALIFIER``; surveyed by grepping the whole ``tests/``
  tree for ``_COPY``/``_QUALIFIER``-suffixed constants matched via
  ``in html`` / ``in text`` / ``assertIn`` against real rendered copy —
  the survey also found ``_ACTION_COPY`` in
  ``tests/test_path_authority_generated.py``, but that constant is a
  FIXTURE PATH fed as an INPUT, never matched against rendered output, so
  it is correctly excluded by not being in ``_PARTICIPATING_TEST_MODULES``
  rather than by a name-based carve-out). A marker suffix outside that
  bounded set, OR a participating file this audit does not register, is
  invisible to it — the same trade-off
  ``test_wrong_match_scenario_producer_audit.py``'s docstring names for
  its own ``scenario=``/``.scenario =`` grammar.
* **A matched name whose value the audit cannot read FAILS CLOSED.** A
  plain string (or bytes, decoded) ``ast.Constant`` resolves normally,
  including the ``str``-annotated ``AnnAssign`` idiom
  (``_FUTURE_COPY: str = "..."``, this module's own house style). Anything
  else with a matching name — a binary-op concatenation
  (``_SPLIT_COPY = "a" + "b"``), an f-call, a bare annotation with no
  value — is DISCOVERED (the name matched the grammar) but its value comes
  back ``None``: ``check_marker_value_is_resolvable`` fails closed on it
  rather than silently dropping it from the population, which is exactly
  the "checker was silently inert" failure mode this audit exists to
  prevent, reintroduced inside its own discovery step.
* **A discovered marker with no classification FAILS CLOSED.** Every
  marker found by that scan must be registered in exactly one of
  ``_MARKER_PRODUCERS`` (a copy-pin marker, with the production files that
  back it) or ``_EXEMPT_MARKERS`` (discovered by the same name grammar but
  not itself matched against rendered copy, with the reason) —
  ``check_marker_is_classified`` fails on anything in neither.
  ``_EXEMPT_MARKERS`` is currently empty: every ``_COPY``/``_QUALIFIER``
  constant the participating modules declare today IS real matched copy
  (the JS-harness ``_WORKER``-suffixed constants don't even reach
  discovery — their names fail the suffix grammar, proven by
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
  significant token allow a regex here" heuristic — required for
  ``web/js/util.js`` and ``web/js/replace_picker.js``, both of which spell
  ``.replace(/"/g, ...)``-shaped regex literals whose PATTERN contains a
  literal quote character (``web/js/wrong-matches.js``'s own regex
  literals never contain a quote, so the heuristic is inert-but-harmless
  for that particular file — it is still exercised by the other two).
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
``TestEveryMarkerHasAProducer``), modulo the FILE-level bound named above.
The class guard exists so the NEXT fully-inert marker fails a test instead
of shipping fluent and wrong for a second time.
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
#: registries both use, for the same reason: not repo-wide discovery. This
#: is the SECOND bound alongside ``_MARKER_NAME_SUFFIXES``: a marker must
#: both live in a registered file AND match the name grammar to be seen at
#: all — issue #1111 review found ``INSECURE_AUTH_WARNING`` (renamed here
#: to ``INSECURE_AUTH_WARNING_COPY`` to fit the grammar) sitting outside
#: both bounds, matched via ``assertNotIn``/``.count()`` against real
#: server responses in two test modules that were not registered.
_PARTICIPATING_TEST_MODULES: tuple[str, ...] = (
    "tests/test_protected_path_truth_generated.py",
    "tests/test_web_dev_server.py",
    "tests/web/test_server_endpoints.py",
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
    "INSECURE_AUTH_WARNING_COPY": _Producers(
        files=("web/server.py",),
        why=(
            "web/server.py:54's own INSECURE_AUTH_WARNING module "
            "constant, logged via log.critical(...) at startup. The "
            "identical sentence is ALSO duplicated (not derived from "
            "this constant) as static text in web/index.html's insecure- "
            "footer block, which is what the test bodies' assertNotIn/"
            ".count() checks actually render — issue #1111 review "
            "deliberately registers only the server.py copy rather than "
            "adding an .html literal extractor, accepting that the two "
            "copies could in principle drift apart independently. A live "
            "instance of the inert-marker class the review found sitting "
            "outside both of this audit's bounds (unregistered "
            "participating modules, no _COPY suffix)"
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


def _constant_marker_value(node: ast.expr | None) -> str | None:
    """A discovered marker's value, or ``None`` when it cannot be read.

    Only a plain ``ast.Constant`` resolves: a ``str`` value is used as-is;
    a ``bytes`` value is UTF-8-decoded (issue #1111 review MAJOR-4 —
    ``tests/web/test_server_endpoints.py``'s copy of the same marker is a
    ``bytes`` literal, matched against an HTTP response body). Anything
    else — a binary-op concatenation (``"a" + "b"``), a call, an f-string,
    a bare annotation with no value at all (``node is None``) — is NOT
    resolved. The caller still records the NAME as discovered; only the
    value comes back ``None``, so a matching name can never silently
    vanish from the population the way a value-blind filter would.
    """
    if isinstance(node, ast.Constant):
        if isinstance(node.value, str):
            return node.value
        if isinstance(node.value, bytes):
            try:
                return node.value.decode("utf-8")
            except UnicodeDecodeError:
                return None
    return None


def discovered_markers_in_source(source: str) -> dict[str, str | None]:
    """Module-level ``*_COPY``/``*_QUALIFIER`` constants in one source.

    Restricted to ``tree.body`` (direct children of the module) rather
    than a full ``ast.walk`` — "module-level" means exactly that, and it
    keeps a function-local variable that happens to share the suffix (none
    exist today) from being mistaken for a copy-pin marker. Matches BOTH
    ``ast.Assign`` (``_FOO_COPY = "..."``) and ``ast.AnnAssign``
    (``_FOO_COPY: str = "..."``, the house-style typed-constant idiom used
    elsewhere in this very module) — issue #1111 review MAJOR-2: the
    Assign-only scan silently could not discover its own idiom.
    """
    tree = ast.parse(source)
    found: dict[str, str | None] = {}
    for node in tree.body:
        if isinstance(node, ast.Assign):
            targets: list[ast.expr] = list(node.targets)
            value_node: ast.expr | None = node.value
        elif isinstance(node, ast.AnnAssign):
            targets = [node.target]
            value_node = node.value
        else:
            continue
        for target in targets:
            if not (
                isinstance(target, ast.Name)
                and target.id.endswith(_MARKER_NAME_SUFFIXES)
            ):
                continue
            found[target.id] = _constant_marker_value(value_node)
    return found


def discovered_markers(relpath: str) -> dict[str, str | None]:
    """``discovered_markers_in_source`` against one registered file's text."""
    return discovered_markers_in_source(_read_source(relpath))


def every_discovered_marker(
    relpaths: Sequence[str] = _PARTICIPATING_TEST_MODULES,
) -> dict[str, str | None]:
    found: dict[str, str | None] = {}
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
#: bracket, a comma, or (via ``None``) the start of the source. Whitespace,
#: including a newline, is NEVER recorded into ``prev_significant`` (see
#: the main loop's ``if not ch.isspace(): prev_significant = ch``), so a
#: whitespace character has no membership test here at all — issue #1111
#: review MINOR-8: an earlier version of this set also listed ``"\n"``,
#: which ``prev_significant`` can never actually hold, dead by
#: construction.
_JS_REGEX_MAY_FOLLOW: frozenset[str] = frozenset("([{,;:!&|=?+-*%^~<>")


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
      inside a string is never mistaken for a comment start
      (``TestTheAuditIsFailClosed.test_a_literal_double_slash_inside_a_
      string_is_not_a_comment`` proves it), and a character-class member
      inside a regex literal (``/[//]/``, ``/[/*]/``, ``/[*/]/``,
      ``/["']/``) is never mistaken for a comment OR a string start
      either, because ``skip_regex`` below tracks whether it is currently
      inside a ``[...]`` class before deciding what any of those
      characters mean
      (``TestTheAuditIsFailClosed.test_regex_character_classes_are_
      lexed_correctly`` proves all four probe shapes);
    * JS's regex-vs-division ambiguity, via the same heuristic real JS
      lexers use: a bare ``/`` starts a regex literal only when the
      previous significant character cannot end an expression (see
      ``_JS_REGEX_MAY_FOLLOW``) or nothing has been seen yet — otherwise
      it is division and left untouched. This is required for, not
      decorative to, ``web/js/util.js`` and ``web/js/replace_picker.js``
      — both spell ``.replace(/"/g, ...)``-shaped regex literals whose
      PATTERN contains a literal quote character, which a naive "any
      quote starts a string" scan misreads as an unterminated string
      consuming the rest of the file
      (``TestTheAuditIsFailClosed.test_js_regex_literal_does_not_
      swallow_the_rest_of_the_file`` below proves the fix; run it
      against a reverted lexer to see the failure it guards).
      ``web/js/wrong-matches.js``'s own regex literals never contain a
      quote in their pattern, so the heuristic is inert-but-harmless for
      that particular file — issue #1111 review MAJOR-3 caught an
      earlier version of this docstring overclaiming "every registered
      JS producer file" here.

    Not handled, because it does not occur in any registered producer
    file (and would need real JS parsing, not a bounded lexer): a
    template literal nested inside another template literal's
    ``${...}`` interpolation whose OWN literal text contains an
    unbalanced ``{``/``}``. Unlike the fail-closed classification checks
    above, a mis-lex here (or any lexer bug) FAILS OPEN, not closed: a
    comment or string boundary the lexer gets wrong degrades to
    substring-matching over MIS-LEXED fragments, which could PROMOTE
    stray text into a false "spelled" fragment (satisfying a marker that
    is not really produced) rather than merely dropping a real one — the
    opposite direction from this audit's own headline fail-closed
    contract, and worth naming as a residual, honestly asymmetric risk
    (issue #1111 review MINOR-6).
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

def check_marker_value_is_resolvable(name: str, value: str | None) -> str | None:
    """Return why ``name``'s discovered value cannot be checked, or ``None``.

    ``value`` is ``discovered_markers_in_source``'s output for this name —
    ``None`` means the name matched the ``*_COPY``/``*_QUALIFIER`` grammar
    but its assigned value was not a plain string/bytes ``ast.Constant``
    (issue #1111 review MAJOR-2: a binary-op concatenation, a bare
    annotation with no value, or any other non-constant shape). Module-
    level so the known-bad self-test can hand it a fabricated
    ``(name, None)`` pair directly.
    """
    if value is not None:
        return None
    return (
        f"{name!r} matches the {_MARKER_NAME_SUFFIXES} naming grammar but "
        "its value is not a plain string/bytes constant (e.g. a binary-op "
        "concatenation, or a bare annotation with no value at all) — the "
        "audit cannot read what it is supposed to match, so it fails "
        "closed as undiscoverable-but-conventioned rather than silently "
        "dropping the name from the population"
    )


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
    the real files. An unrecognised producer-file extension is caught HERE
    (issue #1111 review MINOR-7) rather than escaping as a raw
    ``ValueError`` from ``production_literal_fragments`` — the whole point
    of this function is to be the one place a bad registry entry turns
    into a named, fail-closed violation instead of a traceback.
    """
    for relpath in producers.files:
        if fragments_by_file is not None:
            fragments: Sequence[str] = fragments_by_file.get(relpath, ())
        else:
            try:
                fragments = production_literal_fragments(relpath)
            except OSError:
                return f"{relpath} does not exist"
            except ValueError as exc:
                return f"{relpath}: {exc}"
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
    def test_every_discovered_marker_has_a_resolvable_value(self) -> None:
        """Issue #1111 review MAJOR-2's fail-closed gate, over real files.

        A future ``_FUTURE_COPY = "a" + "b"``-shaped marker in a
        participating module trips HERE, by name, before anything else in
        this class even looks at its (unreadable) value.
        """
        violations = [
            violation
            for name, value in sorted(every_discovered_marker().items())
            if (violation := check_marker_value_is_resolvable(
                name, value)) is not None
        ]
        self.assertEqual(violations, [])

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
                value = markers[name]
                # Every currently-registered marker resolves to a real
                # string (proven by test_every_discovered_marker_has_a_
                # resolvable_value above); narrow for Pyright rather than
                # widening check_marker_has_a_producer's signature to
                # accept None, which would let an unresolvable value
                # silently short-circuit into "no producer found" instead
                # of the dedicated MAJOR-2 violation.
                assert value is not None, (
                    f"{name!r} is registered as a copy-pin marker but its "
                    "discovered value is unresolvable"
                )
                violation = check_marker_has_a_producer(value, producers)
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
        ``_QUALIFIER`` constant the participating modules declare IS real
        matched copy) — the injectable ``exempt=`` override lets this
        self-test exercise the bucket without needing a live example, the
        same way ``check_marker_has_a_producer``'s ``fragments_by_file``
        override does for producer evidence.
        """
        self.assertIsNone(check_marker_is_classified(
            "_FIXTURE_PATH_COPY",
            exempt={"_FIXTURE_PATH_COPY": "an input fixture, not matched copy"},
        ))

    def test_an_unrecognised_extension_is_a_fail_closed_violation(
        self,
    ) -> None:
        """Issue #1111 review MINOR-7.

        A bad registry entry (wrong extension) must surface as THIS
        audit's own violation message at ``check_marker_has_a_producer``
        — the boundary a caller is supposed to be able to trust — not an
        uncaught ``ValueError`` traceback escaping from the extractor
        helper underneath it.
        """
        violation = check_marker_has_a_producer(
            "anything",
            _Producers(
                files=("web/index.html",), why="wrong extension, test only"),
        )
        assert violation is not None
        self.assertRegex(violation, r"no literal extractor registered")

    def test_an_annotated_assignment_marker_is_discovered(self) -> None:
        """The house ``_FOO_COPY: str = "..."`` idiom must resolve.

        Issue #1111 review MAJOR-2: the ``Assign``-only scan could not
        discover its own idiom — ``_MARKER_NAME_SUFFIXES`` a few dozen
        lines up in THIS module is itself an ``AnnAssign`` — the #1086
        "checker silently inert for the exact shape it was meant to
        police" failure mode, reintroduced inside its own fix.
        """
        source = '_FUTURE_COPY: str = "future copy text"\n'
        markers = discovered_markers_in_source(source)
        self.assertEqual(markers, {"_FUTURE_COPY": "future copy text"})

    def test_a_binop_marker_value_is_undiscoverable_and_fails_closed(
        self,
    ) -> None:
        source = '_SPLIT_COPY = "a" + "b"\n'
        markers = discovered_markers_in_source(source)
        self.assertIn("_SPLIT_COPY", markers)
        self.assertIsNone(markers["_SPLIT_COPY"])
        violation = check_marker_value_is_resolvable(
            "_SPLIT_COPY", markers["_SPLIT_COPY"])
        assert violation is not None
        self.assertIn("_SPLIT_COPY", violation)

    def test_a_bare_annotation_with_no_value_fails_closed(self) -> None:
        source = "_BARE_COPY: str\n"
        markers = discovered_markers_in_source(source)
        self.assertIn("_BARE_COPY", markers)
        self.assertIsNone(markers["_BARE_COPY"])

    def test_a_bytes_constant_marker_is_decoded_and_discovered(self) -> None:
        """Issue #1111 review MAJOR-4.

        ``tests/web/test_server_endpoints.py``'s copy of
        ``INSECURE_AUTH_WARNING_COPY`` is a ``bytes`` literal, matched
        against an HTTP response body — the audit decodes it rather than
        treating a non-``str`` ``Constant`` as unresolvable, so the SAME
        wording registered once under ``_MARKER_PRODUCERS`` covers both
        the ``str`` and ``bytes`` copies of the marker.
        """
        source = 'INSECURE_AUTH_WARNING_COPY = (\n    b"decoded text"\n)\n'
        markers = discovered_markers_in_source(source)
        self.assertEqual(
            markers, {"INSECURE_AUTH_WARNING_COPY": "decoded text"})

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
        """Issue #1111 review MINOR-5.

        The block comment spans MULTIPLE lines deliberately: a ONE-LINE
        ``/* ... */`` starting right after a preceding ``//`` line is
        ALSO swallowed by the regex-literal heuristic (``prev_significant
        is None`` right there, since nothing but comments preceded it),
        so a one-line fixture cannot distinguish "block-comment handling
        works" from "the regex heuristic happened to eat the same span
        for an unrelated reason" — the exact per-clause Q1 failure the
        original version of this test had. A newline inside the comment
        forces ``skip_regex`` to bail (it never matches a regex across a
        line break), so with a multi-line comment the block-comment
        branch is the ONLY thing that can still exclude this text —
        reverting it here reproduces a real leak, proven independently.
        """
        source = (
            "// the old copy used to say 'could not be read' here\n"
            "/* still describing\n"
            "   'could not be read'\n"
            "   across several lines of one block comment */\n"
            "const realCopy = 'a totally different sentence';\n"
        )
        fragments = _js_literal_fragments(source)
        self.assertTrue(
            all("could not be read" not in f for f in fragments),
            f"a comment mention leaked into the spelled fragments: "
            f"{fragments!r}",
        )
        self.assertIn("a totally different sentence", fragments)

    def test_a_literal_double_slash_inside_a_string_is_not_a_comment(
        self,
    ) -> None:
        """The docstring's claim, proven: `//` inside a string stays text.

        Issue #1111 review MINOR-5 named this as a missing test — a URL
        containing `//` is the natural real-world shape.
        """
        source = "const url = 'https://example.com/could-not-be-read';\n"
        fragments = _js_literal_fragments(source)
        self.assertIn(
            "https://example.com/could-not-be-read", fragments)

    def test_js_regex_literal_does_not_swallow_the_rest_of_the_file(
        self,
    ) -> None:
        """Fail-closed proof for the regex-vs-string ambiguity.

        ``web/js/util.js`` and ``web/js/replace_picker.js`` spell
        ``.replace(/"/g, ...)``-shaped regex literals (issue #1111
        review MAJOR-3: ``web/js/wrong-matches.js`` does NOT — its own
        regex literals never contain a quote in the pattern, so this
        synthetic minimal source stands in for the two files that
        genuinely exercise the branch, rather than naming "every
        registered JS producer file" as an earlier version of this
        docstring wrongly claimed). Without the regex heuristic, the
        ``"`` inside the pattern starts a bogus string that never finds
        its closing quote on the same line and swallows everything
        after it — including the real marker two lines later. Reverting
        the ``_JS_REGEX_MAY_FOLLOW`` branch reproduces exactly that
        failure.
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

    def test_regex_character_classes_are_lexed_correctly(self) -> None:
        """Issue #1111 review MINOR-6 — the reviewer's four probe shapes,
        plus one adversarial case that actually falsifies ``in_class``.

        An earlier version of this module's docstring wrongly listed "a
        comment token appearing inside a regex character class" as an
        UNHANDLED shape. ``skip_regex``'s ``in_class`` tracking already
        handles it, and the first four cases prove the reviewer's exact
        probes are lexed correctly. But those four alone do NOT falsify a
        broken ``in_class``: removing it entirely and re-running these
        four still passes, because none of their classes contains a quote
        positioned so the (wrongly) premature regex end lands ON a quote
        character — the lexer "resynchronises" on the real marker string
        a few characters later regardless. The fifth case,
        ``/[/"]/``, does contain that combination: with ``in_class``
        broken, the internal ``/`` ends the "regex" one character early,
        landing on the ``"`` and starting a bogus double-quoted string
        that runs to end-of-source, swallowing the real single-quoted
        marker text into one garbled fragment — so ``marker in fragments``
        flips from true to false. This is the case that actually kills a
        planted ``in_class``-removal mutant; the first four are retained
        as direct positive proof of the reviewer's own probes.
        """
        cases = (
            ("slash pair", "/[//]/", "after slash-pair class"),
            ("slash-star", "/[/*]/", "after slash-star class"),
            ("star-slash", "/[*/]/", "after star-slash class"),
            ("quote pair", "/[\"']/", "after quote-pair class"),
            ("slash then quote", "/[/\"]/", "marker text here"),
        )
        for label, pattern, marker in cases:
            with self.subTest(label):
                source = f"x({pattern}, '{marker}');\n"
                fragments = _js_literal_fragments(source)
                self.assertIn(marker, fragments)
                self.assertTrue(
                    all("[" not in f and "]" not in f for f in fragments),
                    f"the character class leaked into a fragment: "
                    f"{fragments!r}",
                )

    def test_js_template_literal_splits_at_interpolation_like_an_fstring(
        self,
    ) -> None:
        source = "const s = `entries ${count} could not be read here`;\n"
        fragments = _js_literal_fragments(source)
        self.assertIn("entries ", fragments)
        self.assertIn(" could not be read here", fragments)
        self.assertTrue(all("${" not in f for f in fragments))

    def test_the_1086_full_file_evidence_resolves_green_a_known_limit(
        self,
    ) -> None:
        """GREEN on a faithful reconstruction of the real pre-fix world.

        Issue #1111 review MAJOR-1: an earlier version of this test used
        a single HAND-TYPED sentence as the entire fixture — a world the
        real audit can never construct, because
        ``web/js/wrong-matches.js`` at commit ``dc0b7c2f^`` (the #1086
        pre-fix tree) ALSO spelled the CORRECT "could not be read"
        wording a few lines away, in the per-entry lead notice. This
        fixture is instead a VERBATIM excerpt of that real historical
        file — both statements, copied character-for-character via
        ``git show dc0b7c2f^:web/js/wrong-matches.js`` — so the
        file-level evidence genuinely mirrors what the real audit would
        have seen. It resolves GREEN, proving empirically (not by
        argument) that this audit's file-level granularity would NOT, on
        its own, have caught issue #1086's specific defect. See the
        module docstring for why that is a deliberately accepted, named
        limit rather than a gap closed with site-level attribution — and
        the paired test below for the class this audit DOES catch.
        """
        # Verbatim excerpt of web/js/wrong-matches.js at commit
        # dc0b7c2f^ (lines 311-313 and 334-336 of that revision):
        #   - the per-entry lead notice, CORRECT even pre-fix
        #   - the empty-state paragraph, the actual #1086 defect
        pre_fix_wrong_matches_excerpt = (
            "  const unreadableLead = unreadableIsContainment\n"
            "    ? `${unreadableCount} entr${unreadableCount === 1 ? "
            "'y was' : 'ies were'} refused (not read) as a containment "
            "decision`\n"
            "    : `${unreadableCount} entr${unreadableCount === 1 ? "
            "'y' : 'ies'} could not be read`;\n"
            "  if (files.length === 0) {\n"
            "    const emptyText = unreadableCount > 0\n"
            "      ? 'The server could not read this folder\\u2019s "
            "contents, so no listing is available. This is NOT evidence "
            "that the folder is empty.'\n"
        )
        fragments = {
            "wrong-matches.js": _js_literal_fragments(
                pre_fix_wrong_matches_excerpt),
        }
        producers = _Producers(
            files=("wrong-matches.js",),
            why="verbatim dc0b7c2f^ excerpt — see this test's docstring",
        )
        violation = check_marker_has_a_producer(
            "could not be read", producers, fragments_by_file=fragments)
        self.assertIsNone(
            violation,
            "the real pre-fix file's correct lead sentence should still "
            "satisfy the marker at file-level, demonstrating the known "
            "limit named in the module docstring",
        )

    def test_a_marker_absent_from_every_registered_file_is_red(
        self,
    ) -> None:
        """RED — the class this guard DOES catch: no producer anywhere.

        Paired with the GREEN test above (issue #1111 review MAJOR-1): a
        fully-inert marker — every registered file's wording rephrased
        so NONE of them spell it any more (a producer deletion, rename,
        or sole-spelling drift) — is exactly what this audit's
        file-level evidence is built to catch, and does.
        """
        drifted_fragments = {
            "wrong-matches.js": [
                "entries were flagged unreadable",
                "This folder is currently inaccessible.",
            ],
            "util.js": ["the storage refused or failed"],
        }
        producers = _Producers(
            files=("wrong-matches.js", "util.js"),
            why="every producer's wording rephrased away from the marker",
        )
        violation = check_marker_has_a_producer(
            "could not be read", producers,
            fragments_by_file=drifted_fragments,
        )
        self.assertIsNotNone(
            violation,
            "no registered file spells the marker any more — the audit "
            "must reject it",
        )


if __name__ == "__main__":
    unittest.main()
