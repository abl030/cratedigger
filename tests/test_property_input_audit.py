"""Bounded audit: every Hypothesis property must use every input it draws.

Issue #882 item 5 — "unfalsifiable generated properties are undetectable
today". During the #868 series one generated property invoked no production
symbol at all: it ``del``'d its generated input and then asserted a relation
between two test-local constants, under a "real materialize worlds, real
filesystem" docstring banner. It survived 0 of 7 planted mutants, including
one that deleted a containment check outright and failed 25 other tests. It
was fixed in that series and a manual sweep found no second instance —
nothing prevented the next one.

The invariant this audit enforces
--------------------------------
**Every input a Hypothesis property draws must be referenced by that
property.** A drawn input that is never loaded — never mentioned, only
``del``'d, or only assigned over — means the generated world cannot reach
the assertion, so the property patrols nothing. That was exactly the #868
property's tell.

This is a **bounded syntactic fact** checked with the stdlib ``ast`` module:
which parameter names does the decorator bind, and does a ``Load`` of each
name appear anywhere in the function body. There is no data-flow analysis,
no inference about what the body *does* with the value, and no attempt to
reproduce Python scoping. It stays inside
``.claude/rules/code-quality.md`` § "Semantic source scanners are
prohibited": one local syntactic fact, one deliberately bounded grammar,
fail-closed on everything outside it.

The criterion that was REJECTED (do not re-propose it)
------------------------------------------------------
Issue #882 item 5 proposed a different criterion: *"every property in
``tests/test_*_generated.py`` must reference at least one production
symbol."* It was measured over all 352 properties in those modules on
``origin/main`` before this audit was written (337 ``@given`` + 9 ``@rule``
+ 6 ``@invariant``): **44 of them (12.5%) flag falsely.** They genuinely
drive production code through channels no symbol-reference scan can see —

* properties that drive production JavaScript by passing a production module
  path inside a **string** to a ``node`` subprocess
  (``tests/test_js_ast_generated.py``,
  ``tests/test_owned_section_expansion_generated.py``);
* properties that subprocess a production script;
* ``RuleBasedStateMachine`` rules that reach production only through
  ``self.<attr>`` (``tests/test_request_lifecycle_generated.py``).

Making that criterion work would require a registry of
production-paths-embedded-in-strings plus subprocess argv analysis — i.e.
exactly the repository-wide semantic scanner the rules forbid. It is not a
tuning problem; it is the wrong criterion. Keep this paragraph: the
measurement is the expensive part of the finding.

The adopted criterion, by contrast, measured over the whole ``tests/`` tree
at that same revision: 383 property-shaped functions (351 ``@given``, 23
``@rule``, 1 ``@initialize``, 8 ``@invariant``), of which 375 are in scope
here; zero unclassifiable shapes; and exactly **one** flagged function — the
deliberate known-bad self-test in ``tests/test_quality_generated.py``, whose
planted decider now RECEIVES the world it ignores, so it uses its drawn
inputs and the allowlist below is **permanently empty**.

Independently confirmed against the real defect: the audit run over the
pre-fix ``tests/test_materialize_evidence_generated.py``
(``git show 20f309ac^``) flags
``test_distinct_causes_never_share_a_reason`` on its unused ``leaf`` input.
Note what that does and does not prove — see the one-directional caveat in
``docs/generated-testing.md``.

Scope: ``@given``, ``@rule``, ``@initialize``
--------------------------------------------
``@given``, ``hypothesis.stateful.rule`` and ``hypothesis.stateful.initialize``
all bind drawn values to the decorated function's parameters, so all three
are in scope. ``@rule``/``@initialize``'s ``target=`` / ``targets=`` keywords
name Bundles for the *return* value and bind no parameter, so they are not
drawn inputs.

``@invariant`` is deliberately **out** of scope: it binds no arguments at
all. Its signature in the pinned Hypothesis 6.156.1 is
``invariant(*, check_during_init: bool = False)`` — one setting, no
strategies, and passing a strategy (``@invariant(world=st.none())``) raises
``TypeError``. So an invariant method has no drawn inputs and the check would
be vacuous, while treating its setting as a drawn input would fail closed on
a perfectly ordinary member: a false positive, not a guard.

Known limits — the ceiling, stated so it is not mistaken for coverage
--------------------------------------------------------------------
Every one of these is a **miss** (fail-open), never a false build break, and
each has **zero live instances** in ``tests/`` today. Closing them needs the
data-flow, scope, or alias tracking that
``.claude/rules/code-quality.md`` forbids, and its "Good enough is a valid
stopping condition" clause applies: the production contract is stated
plainly and behaviour tests pin the known failure modes. Do not extend this
audit syntax case by syntax case.

1. **The diagnostic-position escape — the likeliest one.** A drawn input
   loaded only inside an assertion message passes:
   ``assert A == B, f"world={world}"``, ``self.assertEqual(a, b, world)``,
   ``with self.subTest(world=world):``. It is one keystroke out of a trip
   and it *reads* as an improvement (better failure text) — the #868 fix
   itself added exactly such an f-string. A ``Load`` is a ``Load``; this
   audit does not judge position.
2. **Binding constructs that mask an unused input**, the same family as the
   rebind limit: ``for world in ...``, ``with ... as world``,
   ``except E as world``, and comprehension targets — ``[world for world in
   xs]`` is a separate scope that never touches the drawn name, yet leaves a
   ``Load`` of it behind.
3. **Discovery is by decorator NAME only.** ``assert_unaliased_property_imports``
   fails closed on ``from hypothesis import given as g``, but a module-level
   rebind (``_g = given``), a local wrapper (``def my_given(**kw): return
   given(**kw)``), or ``partial(given)`` evade discovery silently. These are
   deliberate-circumvention shapes and are outside the grammar by
   construction.
4. A property constructed by calling ``given(...)(func)`` instead of
   decorating is outside the grammar; a drawn name shadowed by a nested
   function's own parameter reads as used; and a name rebound before it is
   loaded reads as used.

What it does catch, always: a drawn name with no ``Load`` anywhere in the
body — never mentioned, only ``del``'d, only assigned over.

The fail-closed edge cuts the other way too: ``@given(*strategies)`` and
``@given(**strategies)`` are unclassifiable with no allowlist escape, so a
future DRY idiom such as ``@given(**_COMMON_STRATEGIES)`` is a hard build
break until this audit is extended to map it. That is the intended trade —
an unmappable decorator must never pass silently.
"""

from __future__ import annotations

import ast
import functools
import inspect
import unittest
from dataclasses import dataclass
from pathlib import Path

from hypothesis import strategies as st
from hypothesis.stateful import invariant

import tests._hypothesis_profiles  # noqa: F401 - loads the active profile

REPO_ROOT = Path(__file__).resolve().parent.parent
TESTS_DIR = REPO_ROOT / "tests"

#: Decorators that bind generated values to the decorated function's
#: parameters. See the module docstring for why ``invariant`` is excluded.
PROPERTY_DECORATORS = frozenset({"given", "rule", "initialize"})

#: Stateful decorators whose ``target``/``targets`` keywords route the return
#: value to a Bundle instead of binding a parameter.
_STATEFUL_DECORATORS = frozenset({"rule", "initialize"})
_BUNDLE_ROUTING_KEYWORDS = frozenset({"target", "targets"})

#: Every Hypothesis name whose import must stay unaliased for discovery to
#: be honest — the in-scope decorators plus ``invariant``, so an alias cannot
#: quietly rename a member out of the grammar either.
_UNALIASABLE_IMPORTS = PROPERTY_DECORATORS | {"invariant"}

_IMPLICIT_FIRST_PARAMETERS = frozenset({"self", "cls"})

# EMPTY, and armed. The one property that used to flag (the
# planted-bad-decider self-test in
# ``tests/test_quality_generated.py``) now passes its world to a decider that
# ignores it, which models the planted defect more faithfully than discarding
# the world did. If an entry ever becomes necessary, key it
# ``<repo-relative path>::<qualname>`` (line shifts must not break it) and give
# it one line of rationale — the list is the contract.
PROPERTY_INPUT_ALLOWLIST: dict[str, str] = {}


@dataclass(frozen=True)
class PropertyFunction:
    """One discovered Hypothesis property and its drawn-input verdict."""

    relpath: str
    qualname: str
    lineno: int
    drawn_inputs: tuple[str, ...]
    unused_inputs: tuple[str, ...]
    deleted_inputs: tuple[str, ...]
    unclassified_reason: str | None

    @property
    def key(self) -> str:
        return f"{self.relpath}::{self.qualname}"


def _decorator_name(node: ast.expr) -> str | None:
    """Return the trailing attribute/name of a decorator expression."""
    target = node.func if isinstance(node, ast.Call) else node
    if isinstance(target, ast.Name):
        return target.id
    if isinstance(target, ast.Attribute):
        return target.attr
    return None


def _eligible_parameters(func: ast.FunctionDef | ast.AsyncFunctionDef) -> list[str]:
    """Positional parameters Hypothesis can fill, minus ``self``/``cls``."""
    positional = [arg.arg for arg in (*func.args.posonlyargs, *func.args.args)]
    if positional and positional[0] in _IMPLICIT_FIRST_PARAMETERS:
        positional = positional[1:]
    return positional


def _drawn_inputs(
    func: ast.FunctionDef | ast.AsyncFunctionDef,
    decorators: list[ast.expr],
) -> tuple[tuple[str, ...], str | None]:
    """Return the parameters the decorators bind, or a fail-closed reason."""
    if func.args.vararg is not None or func.args.kwarg is not None:
        return (), "variadic signature cannot be mapped to drawn inputs"

    eligible = _eligible_parameters(func)
    keyword_only = [arg.arg for arg in func.args.kwonlyargs]
    drawn: list[str] = []

    for decorator in decorators:
        name = _decorator_name(decorator)
        if not isinstance(decorator, ast.Call):
            return (), f"bare @{name} decorator is not a call"
        if any(isinstance(arg, ast.Starred) for arg in decorator.args):
            return (), f"@{name} uses *args"
        if any(kw.arg is None for kw in decorator.keywords):
            return (), f"@{name} uses **kwargs"

        routing = (
            _BUNDLE_ROUTING_KEYWORDS
            if name in _STATEFUL_DECORATORS
            else frozenset()
        )
        keyword_names = [
            kw.arg
            for kw in decorator.keywords
            if kw.arg is not None and kw.arg not in routing
        ]
        positional = list(decorator.args)

        if positional and name in _STATEFUL_DECORATORS:
            return (), f"@{name} takes keyword strategies only"
        if positional and keyword_names:
            return (), f"@{name} mixes positional and keyword strategies"
        if len(positional) > len(eligible):
            return (), (
                f"@{name} draws {len(positional)} positional strategies for "
                f"{len(eligible)} eligible parameters"
            )
        if positional:
            drawn.extend(eligible[len(eligible) - len(positional):])
        for keyword_name in keyword_names:
            if keyword_name not in eligible and keyword_name not in keyword_only:
                return (), f"@{name} draws {keyword_name!r}, which is not a parameter"
        drawn.extend(keyword_names)

    seen: dict[str, None] = {}
    for name in drawn:
        seen.setdefault(name, None)
    return tuple(seen), None


def _names_by_context(
    func: ast.FunctionDef | ast.AsyncFunctionDef,
) -> tuple[frozenset[str], frozenset[str]]:
    """Return ``(loaded, deleted)`` names anywhere in the function body."""
    loaded: set[str] = set()
    deleted: set[str] = set()
    for statement in func.body:
        for node in ast.walk(statement):
            if not isinstance(node, ast.Name):
                continue
            if isinstance(node.ctx, ast.Load):
                loaded.add(node.id)
            elif isinstance(node.ctx, ast.Del):
                deleted.add(node.id)
    return frozenset(loaded), frozenset(deleted)


def _walk_scopes(
    node: ast.AST,
    prefix: str,
    relpath: str,
    found: list[PropertyFunction],
) -> None:
    for child in ast.iter_child_nodes(node):
        if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            qualname = f"{prefix}.{child.name}" if prefix else child.name
        else:
            qualname = prefix
        if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef)):
            decorators = [
                decorator
                for decorator in child.decorator_list
                if _decorator_name(decorator) in PROPERTY_DECORATORS
            ]
            if decorators:
                drawn, reason = _drawn_inputs(child, decorators)
                loaded, deleted = _names_by_context(child)
                found.append(PropertyFunction(
                    relpath=relpath,
                    qualname=qualname,
                    lineno=child.lineno,
                    drawn_inputs=drawn,
                    unused_inputs=tuple(
                        name for name in drawn if name not in loaded
                    ),
                    deleted_inputs=tuple(
                        name for name in drawn if name in deleted
                    ),
                    unclassified_reason=reason,
                ))
        _walk_scopes(child, qualname, relpath, found)


def _find_in_tree(tree: ast.Module, relpath: str) -> tuple[PropertyFunction, ...]:
    found: list[PropertyFunction] = []
    _walk_scopes(tree, "", relpath, found)
    return tuple(found)


def find_property_functions(source: str, relpath: str) -> tuple[PropertyFunction, ...]:
    """Return every Hypothesis property declared in one source file."""
    return _find_in_tree(ast.parse(source), relpath)


def _check_unaliased_imports(tree: ast.Module, relpath: str) -> None:
    for node in ast.walk(tree):
        if not isinstance(node, ast.ImportFrom):
            continue
        module = node.module or ""
        if module != "hypothesis" and not module.startswith("hypothesis."):
            continue
        for alias in node.names:
            assert not (alias.name in _UNALIASABLE_IMPORTS and alias.asname), (
                f"{relpath}: Hypothesis decorator {alias.name!r} is imported as "
                f"{alias.asname!r}; the drawn-input audit discovers properties by "
                "decorator name, so aliasing it would hide the property"
            )


def assert_unaliased_property_imports(source: str, relpath: str) -> None:
    """Reject aliased Hypothesis decorator imports, which evade discovery."""
    _check_unaliased_imports(ast.parse(source), relpath)


@functools.cache
def scan_test_tree() -> tuple[PropertyFunction, ...]:
    """Return every Hypothesis property under ``tests/``, imports checked.

    Parses each file once and caches the whole sweep: several tests need the
    live verdict and the tree does not change within a run.
    """
    found: list[PropertyFunction] = []
    for path in sorted(TESTS_DIR.rglob("*.py")):
        if "__pycache__" in path.parts:
            continue
        relpath = path.relative_to(REPO_ROOT).as_posix()
        tree = ast.parse(path.read_text(encoding="utf-8"))
        _check_unaliased_imports(tree, relpath)
        found.extend(_find_in_tree(tree, relpath))
    return tuple(found)


def assert_every_drawn_input_used(
    properties: tuple[PropertyFunction, ...],
    allowlist: dict[str, str],
) -> None:
    """Require each property to use every input it draws, allowlist exact."""
    unclassified = [
        f"{prop.key} (line {prop.lineno}): {prop.unclassified_reason}"
        for prop in properties
        if prop.unclassified_reason is not None
    ]
    assert not unclassified, (
        "unclassifiable Hypothesis property shapes (the drawn-input audit fails "
        f"closed on shapes it cannot map): {unclassified!r}"
    )

    flagged = {prop.key: prop for prop in properties if prop.unused_inputs}
    unexpected = sorted(key for key in flagged if key not in allowlist)
    stale = sorted(key for key in allowlist if key not in flagged)
    details = [
        f"{flagged[key].key} (line {flagged[key].lineno}) never uses "
        f"{list(flagged[key].unused_inputs)!r}"
        + (
            f" (discarded by del: {list(flagged[key].deleted_inputs)!r})"
            if flagged[key].deleted_inputs
            else ""
        )
        for key in unexpected
    ]
    assert not unexpected and not stale, (
        f"Hypothesis properties that ignore a drawn input: {details!r}; "
        f"stale allowlist entries that no longer flag: {stale!r}"
    )


class TestPropertyInputAudit(unittest.TestCase):
    def test_live_tree_uses_every_drawn_input(self) -> None:
        properties = scan_test_tree()

        # A discovery break must not pass vacuously.
        self.assertGreater(len(properties), 300, "property discovery collapsed")
        assert_every_drawn_input_used(properties, PROPERTY_INPUT_ALLOWLIST)

    def test_allowlist_is_an_empty_armed_ratchet(self) -> None:
        self.assertEqual(
            PROPERTY_INPUT_ALLOWLIST,
            {},
            "the drawn-input allowlist is a permanently empty ratchet: a new "
            "entry must be keyed '<repo-relative path>::<qualname>' and carry "
            "one line of rationale, and must be argued for rather than added "
            "to silence a trip",
        )

    def test_planted_bad_decider_property_uses_the_world_it_ignores(self) -> None:
        key = (
            "tests/test_quality_generated.py::TestInvariantCheckersTripOnViolations."
            "test_hypothesis_harness_detects_planted_bad_decider.prop"
        )
        by_key = {prop.key: prop for prop in scan_test_tree()}

        self.assertIn(key, by_key)
        self.assertEqual(by_key[key].drawn_inputs, ("album", "download"))
        self.assertEqual(by_key[key].unused_inputs, ())
        self.assertEqual(by_key[key].deleted_inputs, ())

    def test_deleted_drawn_input_is_flagged(self) -> None:
        source = (
            "@given(album=albums(), download=downloads())\n"
            "def prop(album, download):\n"
            "    del album, download\n"
            "    assert planted()\n"
        )

        (prop,) = find_property_functions(source, "tests/test_planted.py")

        self.assertEqual(prop.unused_inputs, ("album", "download"))
        self.assertEqual(prop.deleted_inputs, ("album", "download"))
        with self.assertRaisesRegex(AssertionError, "discarded by del"):
            assert_every_drawn_input_used((prop,), {})

    def test_never_mentioned_drawn_input_is_flagged(self) -> None:
        source = (
            "@given(world=worlds())\n"
            "def prop(world):\n"
            "    assert CONSTANT_A == CONSTANT_B\n"
        )

        (prop,) = find_property_functions(source, "tests/test_planted.py")

        self.assertEqual(prop.unused_inputs, ("world",))
        self.assertEqual(prop.deleted_inputs, ())
        with self.assertRaisesRegex(AssertionError, "never uses"):
            assert_every_drawn_input_used((prop,), {})

    def test_store_only_drawn_input_is_flagged(self) -> None:
        source = (
            "@given(world=worlds())\n"
            "def prop(world):\n"
            "    world = fixed_world()\n"
            "    assert check(FIXED_WORLD)\n"
        )

        (prop,) = find_property_functions(source, "tests/test_planted.py")

        self.assertEqual(prop.unused_inputs, ("world",))
        self.assertEqual(prop.deleted_inputs, ())

    def test_rebinding_then_loading_reads_as_used(self) -> None:
        """Documented limit: a Load anywhere counts, without flow analysis."""
        source = (
            "@given(world=worlds())\n"
            "def prop(world):\n"
            "    world = fixed_world()\n"
            "    assert check(world)\n"
        )

        (prop,) = find_property_functions(source, "tests/test_planted.py")

        self.assertEqual(prop.unused_inputs, ())

    def test_ordinary_properties_use_their_inputs(self) -> None:
        cases = {
            "keyword": (
                "@given(world=worlds())\n"
                "def prop(world):\n"
                "    assert check(world)\n"
            ),
            "positional": (
                "@given(worlds(), seeds())\n"
                "def prop(world, seed):\n"
                "    assert check(world, seed)\n"
            ),
            "method": (
                "class TestX:\n"
                "    @given(world=worlds())\n"
                "    def test_prop(self, world):\n"
                "        assert check(world)\n"
            ),
            "interactive_data": (
                "@given(data=st.data())\n"
                "def prop(data):\n"
                "    assert check(data.draw(worlds()))\n"
            ),
            "closure_use": (
                "@given(world=worlds())\n"
                "def prop(world):\n"
                "    def inner():\n"
                "        return world\n"
                "    assert check(inner)\n"
            ),
            "example_pin": (
                "@example(world=1)\n"
                "@given(world=worlds())\n"
                "def prop(world):\n"
                "    assert check(world)\n"
            ),
            "stateful_rule": (
                "class Machine(RuleBasedStateMachine):\n"
                "    @rule(target=rows, world=worlds())\n"
                "    def add(self, world):\n"
                "        return self.db.add(world)\n"
            ),
            "keyword_only": (
                "@given(world=worlds())\n"
                "def prop(*, world):\n"
                "    assert check(world)\n"
            ),
        }

        for label, source in cases.items():
            with self.subTest(label=label):
                (prop,) = find_property_functions(source, "tests/test_ok.py")

                self.assertIsNone(prop.unclassified_reason)
                self.assertEqual(prop.unused_inputs, ())
                assert_every_drawn_input_used((prop,), {})

    def test_bundle_routing_keywords_are_not_drawn_inputs(self) -> None:
        source = (
            "class Machine(RuleBasedStateMachine):\n"
            "    @rule(target=rows, targets=(rows,), world=worlds())\n"
            "    def add(self, world):\n"
            "        return self.db.add(world)\n"
        )

        (prop,) = find_property_functions(source, "tests/test_ok.py")

        self.assertEqual(prop.drawn_inputs, ("world",))
        self.assertIsNone(prop.unclassified_reason)

    def test_given_target_keyword_stays_a_drawn_input(self) -> None:
        source = (
            "@given(target=worlds())\n"
            "def prop(target):\n"
            "    del target\n"
        )

        (prop,) = find_property_functions(source, "tests/test_planted.py")

        self.assertEqual(prop.drawn_inputs, ("target",))
        self.assertEqual(prop.unused_inputs, ("target",))

    def test_invariant_members_are_deliberately_out_of_scope(self) -> None:
        source = (
            "class Machine(RuleBasedStateMachine):\n"
            "    @invariant(check_during_init=True)\n"
            "    def holds(self):\n"
            "        assert True\n"
        )

        self.assertEqual(find_property_functions(source, "tests/test_ok.py"), ())

    def test_invariant_binds_no_strategies_in_the_pinned_hypothesis(self) -> None:
        """The reason ``@invariant`` is out of scope, checked not asserted."""
        parameters = inspect.signature(invariant).parameters

        self.assertEqual(list(parameters), ["check_during_init"])
        with self.assertRaises(TypeError):
            invariant(world=st.none())  # pyright: ignore[reportCallIssue]

    def test_unclassifiable_shapes_fail_closed(self) -> None:
        cases = {
            "bare_decorator": (
                ("@given\n"
                "def prop(world):\n"
                "    assert check(world)\n"),
                "bare @given",
            ),
            "star_args": (
                ("@given(*strategies)\n"
                "def prop(world):\n"
                "    assert check(world)\n"),
                r"@given uses \*args",
            ),
            "star_star_kwargs": (
                ("@given(**strategies)\n"
                "def prop(world):\n"
                "    assert check(world)\n"),
                r"@given uses \*\*kwargs",
            ),
            "mixed_positional_keyword": (
                ("@given(worlds(), seed=seeds())\n"
                "def prop(world, seed):\n"
                "    assert check(world, seed)\n"),
                "mixes positional and keyword",
            ),
            "too_many_positional": (
                ("@given(worlds(), seeds())\n"
                "def prop(world):\n"
                "    assert check(world)\n"),
                "2 positional strategies for 1 eligible",
            ),
            "keyword_is_not_a_parameter": (
                ("@given(planet=worlds())\n"
                "def prop(world):\n"
                "    assert check(world)\n"),
                "'planet', which is not a parameter",
            ),
            "variadic_signature": (
                ("@given(worlds())\n"
                "def prop(*args):\n"
                "    assert check(args)\n"),
                "variadic signature",
            ),
            "positional_stateful_rule": (
                ("class Machine(RuleBasedStateMachine):\n"
                "    @rule(worlds())\n"
                "    def add(self, world):\n"
                "        return self.db.add(world)\n"),
                "keyword strategies only",
            ),
        }

        for label, (source, expected) in cases.items():
            with self.subTest(label=label):
                (prop,) = find_property_functions(source, "tests/test_odd.py")

                self.assertIsNotNone(prop.unclassified_reason)
                self.assertRegex(str(prop.unclassified_reason), expected)
                with self.assertRaisesRegex(AssertionError, "unclassifiable"):
                    assert_every_drawn_input_used((prop,), {})

    def test_unclassifiable_shape_is_not_excused_by_the_allowlist(self) -> None:
        source = (
            "@given\n"
            "def prop(world):\n"
            "    assert check(world)\n"
        )
        (prop,) = find_property_functions(source, "tests/test_odd.py")

        with self.assertRaisesRegex(AssertionError, "unclassifiable"):
            assert_every_drawn_input_used((prop,), {prop.key: "excused"})

    def test_stale_allowlist_entry_fails(self) -> None:
        source = (
            "@given(world=worlds())\n"
            "def prop(world):\n"
            "    assert check(world)\n"
        )
        (prop,) = find_property_functions(source, "tests/test_ok.py")

        with self.assertRaisesRegex(AssertionError, "stale allowlist entries"):
            assert_every_drawn_input_used(
                (prop,),
                {"tests/test_ok.py::gone": "no longer flags"},
            )

    def test_allowlist_entries_carry_a_rationale(self) -> None:
        for key, rationale in PROPERTY_INPUT_ALLOWLIST.items():
            with self.subTest(key=key):
                self.assertIn("::", key)
                self.assertGreater(len(rationale), 40)

    def test_aliased_decorator_import_fails_closed(self) -> None:
        cases = {
            "given": "from hypothesis import given as g\n",
            "rule": "from hypothesis.stateful import rule as r\n",
            "invariant": "from hypothesis.stateful import invariant as inv\n",
        }

        for label, source in cases.items():
            with self.subTest(label=label), self.assertRaisesRegex(AssertionError, "imported as"):
                assert_unaliased_property_imports(source, "tests/test_alias.py")

    def test_ordinary_hypothesis_imports_are_accepted(self) -> None:
        source = (
            "from hypothesis import example, given, settings, strategies as st\n"
            "from hypothesis.stateful import RuleBasedStateMachine, rule\n"
            "import tests._hypothesis_profiles\n"
        )

        assert_unaliased_property_imports(source, "tests/test_ok.py")

    def test_nested_property_is_discovered_by_qualname(self) -> None:
        source = (
            "class TestHarness:\n"
            "    def test_planted_decider(self):\n"
            "        @given(world=worlds())\n"
            "        def prop(world):\n"
            "            del world\n"
            "        with self.assertRaises(AssertionError):\n"
            "            prop()\n"
        )

        (prop,) = find_property_functions(source, "tests/test_planted.py")

        self.assertEqual(
            prop.key,
            "tests/test_planted.py::TestHarness.test_planted_decider.prop",
        )
        self.assertEqual(prop.unused_inputs, ("world",))


if __name__ == "__main__":
    unittest.main()
