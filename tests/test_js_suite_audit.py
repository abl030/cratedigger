"""Audit: the shape of the JavaScript test surface — suites and the modules
they import.

Three rules live here. Every suite on disk must be reached by the canonical
full suite; every suite must be on the one shared harness; and no `web/js`
module may ship a `__test__` bag.

See issue #537. PR #531 fixed a hardcoded ``node tests/test_js_X.mjs`` list
in ``scripts/run_tests.sh`` that had silently stopped covering three suites
(issue #520) — a glob (``for f in tests/test_js_*.mjs``) replaced it. This
audit is the JS analogue of ``tests/test_skip_audit.py``: it fails the
suite the moment that gap could reopen, whichever shape it takes:

    1. Someone reverts the glob to a hardcoded list that misses a file.
    2. Someone removes or narrows the glob so JS suites stop running
       altogether.

The parser tolerates both an explicit ``node tests/test_js_X.mjs`` line and
a glob-driven ``for`` loop whose body actually invokes the interpreter on
the loop variable — so a future reshuffle of ``run_tests.sh`` that still
exercises every suite does not false-fail this audit.
"""

from __future__ import annotations

import fnmatch
import glob
import os
import re
import unittest
from pathlib import Path

from tests._source_pins import pinned_source
from tests.structural_audits.js_ast import bare_global_assignments

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RUN_TESTS_SH = os.path.join(REPO_ROOT, "scripts", "run_tests.sh")
RUN_TEST_SUITE = os.path.join(REPO_ROOT, "scripts", "run_test_suite.py")
RUN_JS_CHECKS = os.path.join(REPO_ROOT, "scripts", "run_js_checks.sh")
TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
WEB_JS_DIR = os.path.join(REPO_ROOT, "web", "js")

# An explicit, hardcoded invocation: node tests/test_js_foo.mjs
_EXPLICIT_NODE_RE = re.compile(
    r'node\s+"?(tests/test_js_[A-Za-z0-9_]+\.mjs)"?'
)

# A shell "for VAR in PATTERN; do" loop whose pattern names JS test suites.
_FOR_GLOB_RE = re.compile(
    r'for\s+(\w+)\s+in\s+([^\s;]*test_js_[^\s;]*\.mjs)\s*;\s*do'
)

# How many lines after the "for" line to scan for a node invocation of the
# loop variable — generous enough for a multi-line loop body, small enough
# to stay tied to the loop that declared the variable.
_LOOP_BODY_WINDOW = 8


def _js_suite_names_on_disk() -> set[str]:
    """Every tests/test_js_*.mjs file that exists right now."""
    return {
        os.path.basename(p)
        for p in glob.glob(os.path.join(TESTS_DIR, "test_js_*.mjs"))
    }


def covered_js_suite_names(script_text: str, suite_names: set[str]) -> set[str]:
    """Return the subset of ``suite_names`` that ``script_text`` runs.

    Recognises two independent coverage shapes and unions their results:
    explicit hardcoded invocations, and glob-driven for loops that
    demonstrably invoke the interpreter on their loop variable.
    """
    covered: set[str] = set()

    for m in _EXPLICIT_NODE_RE.finditer(script_text):
        covered.add(os.path.basename(m.group(1)))

    lines = script_text.splitlines()
    for lineno, line in enumerate(lines):
        glob_match = _FOR_GLOB_RE.search(line)
        if glob_match is None:
            continue
        loop_var, pattern = glob_match.group(1), glob_match.group(2)
        body = "\n".join(lines[lineno:lineno + _LOOP_BODY_WINDOW])
        invokes_loop_var = re.search(
            r'node\s+"?\$\{?' + re.escape(loop_var) + r'\}?"?', body
        )
        if invokes_loop_var is None:
            continue  # the loop exists but never hands $VAR to node
        pattern_basename = os.path.basename(pattern)
        for name in suite_names:
            if fnmatch.fnmatch(name, pattern_basename):
                covered.add(name)

    return covered


# The three shapes a suite must have to be on the shared harness at all:
# it imports the module, builds a checker from its own module URL, and
# reaches the single exit path. Deliberately bounded syntax -- this is a
# local syntactic fact, not an attempt to understand the file.
_HARNESS_IMPORT_RE = re.compile(
    r"^import \{[^}]*\bsuite\b[^}]*\} from '\./js_harness\.mjs';$", re.MULTILINE
)
_HARNESS_SUITE_RE = re.compile(r"^const \w+ = suite\(import\.meta\.url\);$", re.MULTILINE)
_HARNESS_DONE_RE = re.compile(r"^\w+\.done\(\);$", re.MULTILINE)

# The two abandoned idioms. A suite carrying either is running its own
# harness again, whatever else it also does.
_OWN_COUNTER_RE = re.compile(r"^let (?:passed|failed) = 0;$", re.MULTILINE)
_NODE_ASSERT_RE = re.compile(r"^import .*from 'node:assert(?:/strict)?';$", re.MULTILINE)


def harness_violations(name: str, source: str) -> list[str]:
    """Every way ``source`` fails to be a suite on the shared harness.

    Accumulating rather than short-circuiting, so the known-bad self-tests
    below can prove each clause trips on its own world instead of only
    whichever happens to be checked first.
    """
    violations: list[str] = []
    if _HARNESS_IMPORT_RE.search(source) is None:
        violations.append(f"{name} does not import suite from ./js_harness.mjs")
    if _HARNESS_SUITE_RE.search(source) is None:
        violations.append(f"{name} never calls suite(import.meta.url)")
    if _HARNESS_DONE_RE.search(source) is None:
        violations.append(f"{name} never reaches the harness exit path")
    if _OWN_COUNTER_RE.search(source) is not None:
        violations.append(f"{name} declares its own passed/failed counter")
    if _NODE_ASSERT_RE.search(source) is not None:
        violations.append(f"{name} imports node:assert instead of the harness")
    return violations


# The retired bag, in every position it could return to. One identifier,
# checked as a whole word anywhere in the file rather than only in an
# `export const` — `export let`, `export { renderEntry as __test__ }` and a
# re-export through another module are the same construct spelled
# differently. Comments count too, since a comment is how the convention
# taught itself to the next author; see `test_bag_violations` for the raw-read
# that clause depends on.
_TEST_BAG_RE = re.compile(r"(?<![\w$])__test__(?![\w$])")


# The syntax phase's own glob over the production modules, read out of the
# script rather than hand-typed here — same reason
# `test_the_harness_module_is_not_itself_run_as_a_suite` reads its glob out.
_WEB_JS_GLOB_RE = re.compile(r"for\s+\w+\s+in\s+(web/js/[^\s;]*\.js)\s*;\s*do")
# `_FOR_GLOB_RE` above captures the loop variable too; this is the same
# suite glob with one group, so `_script_glob` gets a plain string.
_SUITE_GLOB_RE = re.compile(r"for\s+\w+\s+in\s+([^\s;]*test_js_[^\s;]*\.mjs)\s*;\s*do")


def _script_glob(pattern_re: re.Pattern[str]) -> str:
    """The one glob in `run_js_checks.sh` matching ``pattern_re``."""
    script = pinned_source(Path(RUN_JS_CHECKS))
    patterns = pattern_re.findall(script)
    if len(patterns) != 1:
        raise AssertionError(
            f"expected exactly one {pattern_re.pattern!r} glob in "
            f"run_js_checks.sh, got {patterns}"
        )
    return patterns[0]


def _files_matching(pattern: str) -> set[str]:
    """Basenames the repository-relative shell glob ``pattern`` matches."""
    return {
        os.path.basename(p) for p in glob.glob(os.path.join(REPO_ROOT, pattern))
    }


def _listed_files(directory: str, suffix: str) -> set[str]:
    """Basenames in ``directory`` ending in ``suffix``, by plain listing.

    The independent witness for a scan's completeness. Asserting only that
    a scanned set is NONEMPTY leaves a narrowing invisible: with the glob
    cut to one filename and a real bag shipped in another module, the whole
    class stayed green (mutant runner, A5b). Two enumerations that must
    agree turn that into a one-line failure.
    """
    return {
        name for name in os.listdir(directory)
        if name.endswith(suffix) and os.path.isfile(os.path.join(directory, name))
    }


def test_bag_violations(name: str, source: str) -> list[str]:
    """Every mention of the retired ``__test__`` convention in ``source``.

    ``source`` must be RAW file text, not ``pinned_source`` output. The
    house helper strips full-line comments so a POSITIVE pin cannot be
    satisfied by commented-out code (#1172, #1186). This clause fails on
    PRESENCE, so the same strip inverts it: a comment would hide the
    violation instead of failing to prove one. Measured — a file whose
    only mention is ``// exported via `__test__` `` reaches this function
    as an empty line through ``pinned_source`` and returns ``[]``.
    """
    violations: list[str] = []
    if _TEST_BAG_RE.search(source) is not None:
        violations.append(f"{name} mentions the retired __test__ bag")
    return violations


def _raw_source(path: Path) -> str:
    """Read ``path`` verbatim. See ``test_bag_violations`` for why."""
    return path.read_text(encoding="utf-8")


# The one suite allowed to assign a global bare. `tests/test_js_harness.mjs`
# tests `stubGlobals` itself: proving that an existing global is restored by
# identity, and that a second restore does not re-apply a stale value, both
# need a global set OUTSIDE the helper under test. Each of its three sites is
# paired with its own `delete`.
_BARE_GLOBAL_ASSIGNMENT_ALLOWED = frozenset({"test_js_harness.mjs"})


def global_assignment_violations(name: str, source: str) -> list[str]:
    """Every global a suite mutates without handing it back.

    `stubGlobals` is the only sanctioned way, and since issue #1346 the
    harness releases what it installed at the next `section()` and at
    `done()` — so a stub reaches exactly the block that installed it. A bare
    assignment opts out of that silently: it survives every later section,
    and a section that installs no `fetch` of its own answers from whichever
    mock ran last. 104 sites across six suites were in that state, including
    two whole files with no restore of any kind.
    """
    if name in _BARE_GLOBAL_ASSIGNMENT_ALLOWED:
        return []
    return [
        f"{name}:{found.line} assigns {found.root}.{found.key} directly; "
        "install it with stubGlobals({...}) instead"
        for found in bare_global_assignments(source, origin=name)
    ]


class TestNoModuleShipsATestBag(unittest.TestCase):
    """`web/js` modules export names, not test bags (issue #1313).

    Seven modules used to carry ``export const __test__ = {…}``, one object
    each, listing 137 module-private names between them for the Node
    suites. Nothing about the shape survived measurement. Fifty-six of the
    entries were already named exports, listed a second time hundreds of
    lines from their declaration. Sixteen more were exported for nobody.
    Four renamed the function on the way out
    (``pollImportJob: _pollImportJob``), so production and tests spelled
    the same thing two ways. And nothing enforced the "these are private"
    claim the bag made — it is not a language construct, just an object.

    Every name a suite uses is now a named export at its declaration. This
    audit is what stops the convention coming back: it was the house shape
    for a year and three plan documents still describe it.
    """

    def test_no_web_js_module_mentions_the_bag(self) -> None:
        """Every module the syntax phase checks, not merely one of them.

        The scanned set is the `run_js_checks.sh` glob's own matches, so
        this clause has no glob of its own to narrow. An earlier version
        globbed `web/js/*.js` here and asserted only that the result was
        nonempty; narrowing it to a single filename while shipping a real
        bag in another module left the whole class green (mutant runner,
        A5b).
        """
        names = sorted(_files_matching(_script_glob(_WEB_JS_GLOB_RE)))
        self.assertEqual(
            set(names),
            _listed_files(WEB_JS_DIR, ".js"),
            "the scanned set must be every web/js module, not a subset",
        )
        violations: list[str] = []
        for name in names:
            source = _raw_source(Path(WEB_JS_DIR) / name)
            violations.extend(test_bag_violations(f"web/js/{name}", source))
        self.assertEqual(
            violations,
            [],
            "web/js modules export names directly, never a __test__ bag: "
            + "; ".join(violations),
        )

    def test_no_js_suite_reaches_for_a_bag(self) -> None:
        """The suite-side mirror.

        Clause one is the load-bearing half — a snippet elsewhere can only
        import a bag that some module exports. This clause catches the
        author who writes the import first and would otherwise get a bare
        ESM resolution error with no explanation of the convention behind
        it.
        """
        names = sorted(_files_matching(_script_glob(_SUITE_GLOB_RE)))
        self.assertEqual(
            set(names),
            _js_suite_names_on_disk(),
            "the scanned set must be every JS suite, not a subset",
        )
        violations: list[str] = []
        for name in names:
            source = _raw_source(Path(TESTS_DIR) / name)
            violations.extend(test_bag_violations(f"tests/{name}", source))
        self.assertEqual(
            violations,
            [],
            "JS suites import names directly, never a __test__ bag: "
            + "; ".join(violations),
        )

    BAG_SPELLINGS = (
        ("the original object literal", "export const __test__ = {\n  renderEntry,\n};\n"),
        ("a let instead of a const", "export let __test__ = { renderEntry };\n"),
        ("an alias on the way out", "export { renderEntry as __test__ };\n"),
        ("the import side", "import { __test__ } from '../web/js/thing.js';\n"),
        ("prose teaching the convention", "// Helpers are exported via `__test__`.\n"),
    )

    def test_the_clause_trips_on_every_spelling_of_the_bag(self) -> None:
        for label, source in self.BAG_SPELLINGS:
            with self.subTest(spelling=label):
                violations = test_bag_violations("web/js/thing.js", source)
                self.assertEqual(len(violations), 1, f"{label} was not caught")
                self.assertIn("retired __test__ bag", violations[0])

    def test_a_named_export_and_a_lookalike_identifier_trip_nothing(self) -> None:
        source = (
            "export function renderEntry(e) { return `<b>${e}</b>`; }\n"
            "const __test__helper = 1;\n"
            "const my__test__ = 2;\n"
            "export const tests = { renderEntry };\n"
        )
        self.assertEqual(test_bag_violations("web/js/thing.js", source), [])


class TestEveryJsSuiteUsesTheSharedHarness(unittest.TestCase):
    """One harness, one idiom (issue #1313 candidate 6).

    Before the shared module every suite hand-rolled its own assertion
    helpers -- two incompatible idioms across 23 files, ~500 duplicated
    lines, and failure reporting the coordinator could only read at FILE
    granularity. Nothing but this audit stops the next suite from starting
    a third idiom, because a bespoke harness passes perfectly well on its
    own.
    """

    def test_every_suite_on_disk_is_on_the_shared_harness(self) -> None:
        names = sorted(_js_suite_names_on_disk())
        self.assertTrue(names, "no tests/test_js_*.mjs files found")
        violations: list[str] = []
        for name in names:
            source = pinned_source(Path(TESTS_DIR) / name)
            violations.extend(harness_violations(name, source))
        self.assertEqual(
            violations,
            [],
            "JavaScript suites must use tests/js_harness.mjs: "
            + "; ".join(violations),
        )

    def test_the_harness_module_itself_is_present(self) -> None:
        harness = Path(TESTS_DIR) / "js_harness.mjs"
        self.assertTrue(harness.is_file(), "tests/js_harness.mjs is missing")
        source = pinned_source(harness)
        self.assertIn("export function suite(", source)
        self.assertIn("CRATEDIGGER_JS_FAILURE", source)
        self.assertIn("CRATEDIGGER_JS_DONE", source)

    def test_the_harness_module_is_not_itself_run_as_a_suite(self) -> None:
        """The harness must not match the glob `run_js_checks.sh` really uses.

        The pattern is READ OUT of the script rather than hand-typed: an
        earlier version compared two literals
        (``fnmatch("js_harness.mjs", "test_js_*.mjs")``) and so proved
        nothing about the script at all — the constant-versus-hand-typed-
        literal shape `.claude/rules/code-quality.md` names.

        Two different widenings reach two different clauses here, and the
        distinction was mis-stated once (round-2 review, claim 1):

        - ``tests/*js*.mjs`` does not contain the literal ``test_js_``, so
          ``_FOR_GLOB_RE`` never matches it and the CARDINALITY assertion
          fires ("got []"). That is a real kill, but not by the fnmatch
          clause.
        - ``tests/*[test_js_]*.mjs`` does match the regex AND matches
          ``js_harness.mjs``, so it is what actually reaches the fnmatch
          assertion below.

        Both are covered: this test kills the first, and
        ``test_a_glob_matching_the_harness_is_refused`` pins the second
        against the parser directly.
        """
        script = pinned_source(Path(RUN_JS_CHECKS))
        patterns = [
            os.path.basename(m.group(2)) for m in _FOR_GLOB_RE.finditer(script)
        ]
        self.assertEqual(
            len(patterns),
            1,
            f"expected exactly one JS-suite glob in the script, got {patterns}",
        )
        self.assertFalse(
            fnmatch.fnmatch("js_harness.mjs", patterns[0]),
            f"the script's glob {patterns[0]!r} would run the harness module "
            "as if it were a suite",
        )
        self.assertNotIn("js_harness.mjs", _js_suite_names_on_disk())

    def test_a_glob_matching_the_harness_is_refused(self) -> None:
        """The fnmatch clause itself, driven by a glob the parser accepts.

        `tests/*[test_js_]*.mjs` contains the literal `test_js_`, so
        `_FOR_GLOB_RE` matches it and the cardinality assertion passes --
        which is what makes this the world that reaches the fnmatch clause
        rather than short-circuiting before it (round-2 review, mutant M42).
        """
        script = (
            "for file in tests/*[test_js_]*.mjs; do\n"
            '    node "$file"\n'
            "done\n"
        )
        patterns = [
            os.path.basename(m.group(2)) for m in _FOR_GLOB_RE.finditer(script)
        ]
        self.assertEqual(len(patterns), 1, "the parser must accept this glob")
        self.assertTrue(
            fnmatch.fnmatch("js_harness.mjs", patterns[0]),
            "this world exists precisely because the harness DOES match it",
        )
        self.assertTrue(
            covered_js_suite_names(script, {"test_js_util.mjs"}),
            "and the glob still covers real suites, so nothing else objects",
        )

    KNOWN_BAD = (
        (
            "no harness import",
            "const t = suite(import.meta.url);\nt.done();\n",
            "does not import suite",
        ),
        (
            "imported but never constructed",
            "import { suite } from './js_harness.mjs';\nt.done();\n",
            "never calls suite(import.meta.url)",
        ),
        (
            "constructed but never finished",
            (
                "import { suite } from './js_harness.mjs';\n"
                "const t = suite(import.meta.url);\n"
            ),
            "never reaches the harness exit path",
        ),
        (
            "a third idiom smuggled in beside the harness",
            (
                "import { suite } from './js_harness.mjs';\n"
                "const t = suite(import.meta.url);\n"
                "let passed = 0;\n"
                "t.done();\n"
            ),
            "declares its own passed/failed counter",
        ),
        (
            "node:assert smuggled in beside the harness",
            (
                "import assert from 'node:assert/strict';\n"
                "import { suite } from './js_harness.mjs';\n"
                "const t = suite(import.meta.url);\n"
                "t.done();\n"
            ),
            "imports node:assert instead of the harness",
        ),
    )

    def test_each_clause_trips_on_its_own_known_bad_world(self) -> None:
        for label, source, expected in self.KNOWN_BAD:
            with self.subTest(world=label):
                violations = harness_violations("test_js_x.mjs", source)
                self.assertEqual(
                    len(violations),
                    1,
                    f"{label} should trip exactly one clause, got {violations}",
                )
                self.assertIn(expected, violations[0])

    def test_a_conforming_world_trips_nothing(self) -> None:
        source = (
            "import { renderThing } from '../web/js/thing.js';\n"
            "\n"
            "import { suite } from './js_harness.mjs';\n"
            "\n"
            "const t = suite(import.meta.url);\n"
            "\n"
            "t.section('renderThing()');\n"
            "t.equal(renderThing(), 'x', 'it renders');\n"
            "\n"
            "t.done();\n"
        )
        self.assertEqual(harness_violations("test_js_x.mjs", source), [])


class TestNoSuiteMutatesAGlobalWithoutRestoringIt(unittest.TestCase):
    """A stub belongs to the block that installed it (issue #1346).

    Six suites assigned `globalThis.fetch` and friends bare, 104 sites, and
    restored nothing: `test_js_browse.mjs` and `test_js_pipeline.mjs`
    contained no restore of any kind, so every section after the first ran
    against whichever mock happened to be installed last. Nothing failed
    from it, because each section that used `fetch` also installed one —
    the leak was latent, one forgotten line from being real, and invisible
    to every check the repository had.

    The sweep put all 104 behind `stubGlobals`, whose restore the harness
    now owns. This audit is what stops the next one: a bare assignment
    passes perfectly well on its own, exactly like a bespoke harness did.
    """

    def test_no_suite_assigns_a_global_directly(self) -> None:
        names = sorted(_js_suite_names_on_disk())
        self.assertTrue(names, "no tests/test_js_*.mjs files found")
        violations: list[str] = []
        for name in names:
            source = _raw_source(Path(TESTS_DIR) / name)
            violations.extend(global_assignment_violations(name, source))
        self.assertEqual(
            violations,
            [],
            "install browser globals with stubGlobals({...}): "
            + "; ".join(violations),
        )

    def test_the_harness_module_itself_assigns_no_global(self) -> None:
        """`js_harness.mjs` is not a suite, so the scan above skips it.

        It writes `globalThis[key]` from inside `stubGlobals`, through a
        computed subscript the scanner reports as `[computed]` — so this
        names the exact expected count rather than asserting zero.
        """
        found = bare_global_assignments(
            _raw_source(Path(TESTS_DIR) / "js_harness.mjs"),
            origin="js_harness.mjs",
        )
        self.assertEqual(
            [(item.root, item.key) for item in found],
            [("globalThis", "[computed]"), ("globalThis", "[computed]")],
            "stubGlobals installs and restores through globalThis[key]; a "
            "third global write in the harness is a new mutation path",
        )

    def test_the_allowlisted_suite_really_needs_its_allowance(self) -> None:
        """A stale allowance is worse than none — it hides a real regression."""
        for name in sorted(_BARE_GLOBAL_ASSIGNMENT_ALLOWED):
            with self.subTest(suite=name):
                source = _raw_source(Path(TESTS_DIR) / name)
                self.assertTrue(
                    bare_global_assignments(source, origin=name),
                    f"{name} is allowlisted but assigns no global; drop it "
                    "from _BARE_GLOBAL_ASSIGNMENT_ALLOWED",
                )
                self.assertEqual(
                    global_assignment_violations(name, source),
                    [],
                    f"the allowance for {name} is not being applied",
                )

    # One world per spelling the clause must catch. Each is minimal: the
    # only global write in it is the one named.
    KNOWN_BAD = (
        ("member assignment", "globalThis.fetch = async () => ({});\n",
         "assigns globalThis.fetch directly"),
        ("computed key", "globalThis['fetch'] = async () => ({});\n",
         "assigns globalThis.[computed] directly"),
        ("augmented assignment", "globalThis.counter += 1;\n",
         "assigns globalThis.counter directly"),
        ("the node `global` alias", "global.confirm = () => true;\n",
         "assigns global.confirm directly"),
        ("the `self` alias", "self.document = {};\n",
         "assigns self.document directly"),
        ("inside a block", "{\n  globalThis.fetch = null;\n}\n",
         "assigns globalThis.fetch directly"),
        ("inside a helper function",
         "function install() {\n  globalThis.document = {};\n}\n",
         "assigns globalThis.document directly"),
    )

    def test_each_spelling_trips_the_clause(self) -> None:
        for label, source, expected in self.KNOWN_BAD:
            with self.subTest(world=label):
                violations = global_assignment_violations(
                    "test_js_x.mjs", source
                )
                self.assertEqual(
                    len(violations),
                    1,
                    f"{label} should report exactly one site, got {violations}",
                )
                self.assertIn(expected, violations[0])

    def test_a_conforming_world_trips_nothing(self) -> None:
        """Everything a suite legitimately does that LOOKS like the bad shape.

        Without this the clause could report every property assignment in
        the tree and still pass its known-bad worlds. The `window.toast`
        line is the live case: three suites write onto a `window` stub they
        installed a line earlier, and none of them mutates anything shared.
        """
        source = (
            "import { stubGlobals, suite } from './js_harness.mjs';\n"
            "const t = suite(import.meta.url);\n"
            "const globals = stubGlobals({ window: {}, fetch: null });\n"
            "window.toast = () => {};\n"
            "const state = { fetch: null };\n"
            "state.fetch = async () => ({});\n"
            "const alias = globalThis;\n"
            "let localThis = 0;\n"
            "localThis = 1;\n"
            "// globalThis.fetch = 'a comment is not a write';\n"
            "globals.restore();\n"
            "t.done();\n"
        )
        self.assertEqual(global_assignment_violations("test_js_x.mjs", source), [])
        self.assertIn("alias", source, "the alias line is present but unreported")


class TestJsSuiteAudit(unittest.TestCase):
    """Every tests/test_js_*.mjs file must run every scripts/run_tests.sh pass."""

    def setUp(self) -> None:
        self.wrapper_text = pinned_source(Path(RUN_TESTS_SH))
        self.coordinator_text = pinned_source(Path(RUN_TEST_SUITE))
        self.script_text = pinned_source(Path(RUN_JS_CHECKS))

    def test_every_js_suite_on_disk_is_covered(self) -> None:
        suite_names = _js_suite_names_on_disk()
        self.assertTrue(
            suite_names,
            "no tests/test_js_*.mjs files found — the fixture set that "
            "backs this audit is gone",
        )
        self.assertIn("scripts/run_test_suite.py", self.wrapper_text)
        self.assertIn('"scripts/run_js_checks.sh", "unit"', self.coordinator_text)
        covered = covered_js_suite_names(self.script_text, suite_names)
        missing = sorted(suite_names - covered)
        self.assertEqual(
            missing, [],
            "run_tests.sh does not reach these JS suites — issue #520/#537 "
            "gap has reopened: " + ", ".join(missing),
        )

    def test_parser_flags_a_suite_missing_from_a_hardcoded_list(self) -> None:
        """RED-case proof: a reverted hardcoded list that drops a suite must
        be caught without touching the real run_tests.sh."""
        suite_names = {"test_js_util.mjs", "test_js_pipeline.mjs", "test_js_new_thing.mjs"}
        fake_script = (
            "#!/usr/bin/env bash\n"
            "echo === JS unit tests ===\n"
            "node tests/test_js_util.mjs || exit 1\n"
            "node tests/test_js_pipeline.mjs || exit 1\n"
        )
        covered = covered_js_suite_names(fake_script, suite_names)
        self.assertEqual(covered, {"test_js_util.mjs", "test_js_pipeline.mjs"})
        self.assertNotIn("test_js_new_thing.mjs", covered)

    def test_parser_flags_a_glob_that_never_invokes_its_loop_variable(self) -> None:
        """RED-case proof: a glob loop that iterates but forgets to actually
        run the interpreter must not be credited as coverage."""
        suite_names = {"test_js_util.mjs"}
        fake_script = (
            "#!/usr/bin/env bash\n"
            "for f in tests/test_js_*.mjs; do\n"
            "  echo \"found $f\"\n"
            "done\n"
        )
        covered = covered_js_suite_names(fake_script, suite_names)
        self.assertEqual(covered, set())

    def test_parser_credits_a_genuine_glob_loop(self) -> None:
        """A real glob loop that hands $f to node covers every matching file."""
        suite_names = {"test_js_util.mjs", "test_js_pipeline.mjs"}
        fake_script = (
            "#!/usr/bin/env bash\n"
            "for f in tests/test_js_*.mjs; do\n"
            "  node \"$f\" || exit 1\n"
            "done\n"
        )
        covered = covered_js_suite_names(fake_script, suite_names)
        self.assertEqual(covered, suite_names)


if __name__ == "__main__":
    unittest.main()
