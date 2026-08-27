"""Audit: every production ``CratediggerContext(...)`` site is registered
(issue #1278 item 3).

PR #1280 introduced, and then caught, a regression of exactly one shape: a
SECOND ``CratediggerContext`` construction — the inline one in
``_run_phase1`` — silently omitted ``download_ownership``, which made every
download-timeout cleanup fail closed under the ownership gate that same PR
had just introduced. It was found by the PR's own independent review
(commit ``14b8f533``), NOT by any test: the whole suite was green with the
defect present, because nothing constrained the SET of construction sites.
A new site could appear, or an existing one lose a collaborator, without a
single test noticing.

``PRODUCTION_CONTEXT_CONSTRUCTIONS`` below is that missing constraint. It
is DATA a human maintains, one entry per SCOPE, each declaring how many
constructions that scope holds, whether they wire ``download_ownership``,
and why. The audit's only job is to hold the registry and the tree in exact
agreement, in every direction:

1. every registered scope still holds a construction (else it is a STALE
   entry); and
2. every discovered scope is registered (else it is an UNREGISTERED site —
   the #1280 shape, failing closed at test time instead of in production);
   and
3. a registered scope holds exactly the declared NUMBER of constructions,
   each of whose ``download_ownership=`` kwarg matches the declaration.

Clause 3's count is not decoration. Sites are keyed by scope chain, not by
line number, so the key survives ordinary edits — but that means two
constructions in ONE function share a key. Until issue #1278's review this
module collapsed them (``sites[site.key] = site``, last one wins), so a
function holding a correct construction and a collaborator-less second one
reported no violation at all. #1280's second construction happened to live
in its own helper (``_run_phase1``); had it been inline in ``main``, the
audit written to catch it would have been green.

**The grammar is deliberately bounded** (`.claude/rules/code-quality.md`
§ "Semantic source scanners are prohibited"). This parses with ``ast`` and
recognises exactly two spellings of a construction — ``CratediggerContext(
...)`` and ``<something>.CratediggerContext(...)`` — plus the enclosing
``def``/``class`` chain the site sits in. It infers nothing about
reachability, values, or runtime behaviour: whether the collaborator a site
passes is the RIGHT one is a question for that site's own test
(``tests/test_cycle_summary.py::TestPhase1ContextForwarding``,
``tests/test_convergence_runner_generated.py::TestPhase1ContextCallSite``),
not for this audit.

Aliasing is closed syntactically rather than semantically: binding the class
to another name is itself a violation, because a construction under that
name would be spelled outside the call grammar above. Four binding forms are
recognised — ``from lib.context import CratediggerContext as X``, ``X =
CratediggerContext``, ``X = <something>.CratediggerContext``, and the
annotated ``X: type = CratediggerContext`` (either value spelling). That is
the whole claim; it is a list of forms, not a proof of exhaustiveness.
Bindings through a data structure (``{"c": CratediggerContext}``, a tuple
unpack, a parameter default) and genuinely dynamic construction
(``globals()[...]``, ``functools.partial``) are out of grammar and
undetected — recognising them would require inferring runtime semantics from
arbitrary source, which is the prohibited shape. Nothing in this repository
does any of them, and review owns that boundary.

Deterministic only: this is test infrastructure auditing production shape,
so per `.claude/rules/code-quality.md` § "Never property-test the test
machinery" there is no generated property here.
"""

from __future__ import annotations

import ast
import unittest
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import ClassVar

from tests._typing_ratchet_scanner import iter_production_paths

_CLASS_NAME = "CratediggerContext"


@dataclass(frozen=True)
class ContextSite:
    """One registered production SCOPE that constructs ``CratediggerContext``.

    ``wires_download_ownership`` is the declaration the audit enforces
    against the source; ``why`` is the human's reason, enforced by nobody
    and read by the next author deciding whether a new site needs it.

    ``constructions`` is how many constructions this scope holds — 1 for
    every current entry, and the reason a second construction sneaked into
    an already-registered function cannot pass unnoticed. Every one of them
    must match ``wires_download_ownership``: a scope wanting two
    constructions that disagree has to say so by splitting them into
    separate functions, which is also how it earns separate reasons.
    """

    wires_download_ownership: bool
    why: str
    constructions: int = 1


#: Every production ``CratediggerContext(...)`` construction, keyed
#: ``"<repo-relative path>::<enclosing def/class chain>"`` (``<module>``
#: for a top-level one). Hand-maintained: adding a construction means
#: adding an entry here — or bumping an existing entry's
#: ``constructions`` — and stating, in ``why``, whether the new context
#: can reach a destructive slskd call.
PRODUCTION_CONTEXT_CONSTRUCTIONS: Mapping[str, ContextSite] = {
    "cratedigger.py::main": ContextSite(
        wires_download_ownership=True,
        why=(
            "The cycle owner. Constructs the one DownloadOwnershipWriter "
            "every other context borrows; without it nothing in the cycle "
            "can prove ledger ownership and every destructive slskd path "
            "fails closed."
        ),
    ),
    "cratedigger.py::build_phase1_context": ContextSite(
        wires_download_ownership=True,
        why=(
            "Phase 1 reaches lib.download._timeout_album -> "
            "cancel_and_delete, which is ownership-gated. The inline "
            "construction this helper replaced omitted the collaborator, "
            "failing every timeout cleanup closed (#1278) -- the defect "
            "this registry exists to make impossible to repeat."
        ),
    ),
    "lib/enqueue.py::prepare_find_download_context": ContextSite(
        wires_download_ownership=True,
        why=(
            "Every find-download worker context. Supplies five of the six "
            "cancel_and_delete call sites, and forwards the owner's writer "
            "by reference (it opens a fresh DB handle per operation "
            "precisely so worker threads can share one instance)."
        ),
    ),
    "scripts/importer.py::_build_runtime_context": ContextSite(
        wires_download_ownership=False,
        why=(
            "The serial importer's runtime context. Has no path to a "
            "destructive slskd call -- it carries slskd=None -- so the "
            "collaborator would be unreachable. Adding a slskd client here "
            "means adding the writer in the same change."
        ),
    ),
    "scripts/import_preview_worker.py::_materialize_automation_authority": (
        ContextSite(
            wires_download_ownership=False,
            why=(
                "The preview worker's materialization context, also "
                "slskd=None. Same reasoning, same condition on any future "
                "change that gives it a slskd client."
            ),
        )
    ),
}


@dataclass(frozen=True)
class DiscoveredSite:
    """One construction found in the tree, with its own source location."""

    key: str
    lineno: int
    wires_download_ownership: bool


def _sites_in_module(rel_path: str, source: str) -> list[DiscoveredSite]:
    """Every ``CratediggerContext(...)`` call in one module's source.

    Walks the tree keeping the enclosing ``def``/``class`` names as the
    site's qualname, so the key survives the line-number churn any edit
    to the file produces.
    """
    found: list[DiscoveredSite] = []

    def visit(node: ast.AST, scope: tuple[str, ...]) -> None:
        for child in ast.iter_child_nodes(node):
            if isinstance(
                child, ast.FunctionDef | ast.AsyncFunctionDef | ast.ClassDef,
            ):
                visit(child, (*scope, child.name))
                continue
            if isinstance(child, ast.Call) and _is_context_call(child.func):
                found.append(DiscoveredSite(
                    key=f"{rel_path}::{'.'.join(scope) or '<module>'}",
                    lineno=child.lineno,
                    wires_download_ownership=any(
                        kw.arg == "download_ownership"
                        for kw in child.keywords
                    ),
                ))
            visit(child, scope)

    visit(ast.parse(source), ())
    return found


def _is_context_call(func: ast.expr) -> bool:
    """The two recognised spellings, and no others (bounded grammar)."""
    if isinstance(func, ast.Name):
        return func.id == _CLASS_NAME
    if isinstance(func, ast.Attribute):
        return func.attr == _CLASS_NAME
    return False


def _names_the_class(value: ast.expr | None) -> bool:
    """Whether this expression IS the class, in the two spellings
    ``_is_context_call`` recognises for a call -- bare ``CratediggerContext``
    or ``<something>.CratediggerContext``. Anything else (a subscript, a
    call, a dict literal holding it) is out of grammar, deliberately.
    """
    if isinstance(value, ast.Name):
        return value.id == _CLASS_NAME
    if isinstance(value, ast.Attribute):
        return value.attr == _CLASS_NAME
    return False


def _alias_bindings(rel_path: str, source: str) -> list[str]:
    """Names the class is bound to OTHER than its own.

    An alias would let a construction be spelled under a name
    ``_is_context_call`` cannot recognise, so aliasing is itself the
    violation -- a syntactic fact, not an inference about what the alias
    is later used for.

    Four forms, and only four (issue #1278 review F7): the aliased
    ``from ... import ... as X``, and an ``X = ...`` / ``X: type = ...``
    assignment whose value names the class either bare or through an
    attribute. Attribute values and annotated assignments used to slip
    through -- neither is dynamic, both are ordinary Python, and the
    module docstring previously claimed no construction could hide from
    this function at all.
    """
    violations: list[str] = []
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.ImportFrom):
            violations.extend(
                f"{rel_path}:{node.lineno}: imports {_CLASS_NAME} as "
                f"{alias.asname!r} -- construct it under its own name so "
                "the construction-site audit can see the site"
                for alias in node.names
                if alias.name == _CLASS_NAME and alias.asname is not None
            )
        elif (
            isinstance(node, ast.Assign | ast.AnnAssign)
            and _names_the_class(node.value)
        ):
            violations.append(
                f"{rel_path}:{node.lineno}: rebinds {_CLASS_NAME} to another "
                "name -- construct it under its own name so the "
                "construction-site audit can see the site"
            )
    return violations


def discover_production_context_sites() -> dict[str, list[DiscoveredSite]]:
    """Every production construction, grouped under the registry's keys.

    A LIST per key, never one site per key: two constructions in one
    function share a scope chain, and the earlier ``sites[site.key] =
    site`` assignment silently dropped all but the last of them (see the
    module docstring's clause 3).

    Uses the typing ratchet's own production walker rather than a second
    one -- it already prunes ``tests/``, ``docs/``, and every hidden
    directory (``.claude/worktrees`` above all: an unpruned walk crawls
    thousands of stale worktree files).
    """
    found: list[DiscoveredSite] = []
    for rel_path, abs_path in iter_production_paths():
        with open(abs_path, encoding="utf-8") as handle:
            source = handle.read()
        if _CLASS_NAME not in source:
            continue
        found.extend(_sites_in_module(rel_path, source))
    return group_sites(found)


def group_sites(
    sites: Sequence[DiscoveredSite],
) -> dict[str, list[DiscoveredSite]]:
    """Group discovered sites by key, keeping every one of them.

    Its own function so the keep-all-of-them behaviour is directly
    testable: the collapse this replaces lived in one assignment inside a
    walker that only runs against the whole real tree.
    """
    grouped: dict[str, list[DiscoveredSite]] = {}
    for site in sites:
        grouped.setdefault(site.key, []).append(site)
    return grouped


def discover_alias_bindings() -> list[str]:
    """Every alias binding of the class across production code."""
    violations: list[str] = []
    for rel_path, abs_path in iter_production_paths():
        with open(abs_path, encoding="utf-8") as handle:
            source = handle.read()
        if _CLASS_NAME not in source:
            continue
        violations.extend(_alias_bindings(rel_path, source))
    return violations


def registry_violations(
    registry: Mapping[str, ContextSite],
    discovered: Mapping[str, Sequence[DiscoveredSite]],
) -> list[str]:
    """Where the registry and the tree disagree, one message per fact.

    Accumulating so no clause can mask another: a change that both adds an
    unregistered site AND drops a collaborator elsewhere reports both. A
    scope whose construction COUNT is wrong reports that as well as any
    kwarg mismatch among its sites -- the two are different edits.
    """
    violations: list[str] = []
    for key in sorted(set(discovered) - set(registry)):
        lines = ", ".join(str(site.lineno) for site in discovered[key])
        violations.append(
            f"UNREGISTERED CratediggerContext construction at {key} "
            f"(line {lines}). Add it to "
            "PRODUCTION_CONTEXT_CONSTRUCTIONS, declaring whether this "
            "context can reach a destructive slskd call and therefore "
            "needs download_ownership."
        )
    for key in sorted(set(registry) - set(discovered)):
        violations.append(
            f"STALE registry entry {key}: no CratediggerContext "
            "construction is there any more. Remove the entry."
        )
    for key in sorted(set(registry) & set(discovered)):
        entry = registry[key]
        sites = sorted(discovered[key], key=lambda site: site.lineno)
        if len(sites) != entry.constructions:
            lines = ", ".join(str(site.lineno) for site in sites)
            violations.append(
                f"{key} holds {len(sites)} CratediggerContext "
                f"construction(s) (line {lines}), but the registry "
                f"declares constructions={entry.constructions}. Every one "
                "of them needs a declared answer on download_ownership."
            )
        for site in sites:
            if site.wires_download_ownership != entry.wires_download_ownership:
                violations.append(
                    f"{key} (line {site.lineno}) "
                    f"{'lost' if entry.wires_download_ownership else 'gained'}"
                    " its download_ownership= kwarg: the registry declares "
                    f"wires_download_ownership="
                    f"{entry.wires_download_ownership}, the source says "
                    f"{site.wires_download_ownership}."
                )
    return violations


class TestProductionContextConstructionRegistryIsExact(unittest.TestCase):
    """The real registry, verified against the real production tree."""

    def test_registry_matches_the_tree(self) -> None:
        self.assertEqual(
            registry_violations(
                PRODUCTION_CONTEXT_CONSTRUCTIONS,
                discover_production_context_sites(),
            ),
            [],
        )

    def test_the_class_is_never_bound_to_another_name(self) -> None:
        self.assertEqual(discover_alias_bindings(), [])

    def test_every_registered_site_states_a_reason(self) -> None:
        """``why`` is the entry's whole value to the next author; an empty
        one turns the registry into a checkbox."""
        for key, site in sorted(PRODUCTION_CONTEXT_CONSTRUCTIONS.items()):
            with self.subTest(site=key):
                self.assertGreater(len(site.why.split()), 8, key)

    def test_the_owner_and_worker_contexts_are_registered_as_wiring_it(
        self,
    ) -> None:
        """A must-still-work floor under the registry itself.

        The audit above proves registry and tree AGREE; it cannot notice
        an author who flips a declaration to False and deletes the kwarg
        in the same commit. These three sites reach ``cancel_and_delete``
        and the answer for them is not a judgement call, so pin it here
        rather than leaving it to the diff.
        """
        for key in (
            "cratedigger.py::main",
            "cratedigger.py::build_phase1_context",
            "lib/enqueue.py::prepare_find_download_context",
        ):
            with self.subTest(site=key):
                self.assertTrue(
                    PRODUCTION_CONTEXT_CONSTRUCTIONS[
                        key].wires_download_ownership)


class TestRegistryCheckerTripsOnViolations(unittest.TestCase):
    """Known-bad self-tests: one named world per clause, asserting that
    clause's own message. Every clause is reachable from a real edit --
    add a construction, delete one, or drop the kwarg."""

    REGISTERED: ClassVar[dict[str, ContextSite]] = {
        "lib/thing.py::build": ContextSite(True, "wires it"),
    }

    def _discovered(
        self, **overrides: list[DiscoveredSite],
    ) -> dict[str, list[DiscoveredSite]]:
        base = {
            "lib/thing.py::build": [DiscoveredSite(
                key="lib/thing.py::build",
                lineno=10,
                wires_download_ownership=True,
            )],
        }
        base.update(overrides)
        return base

    def test_unregistered_site_clause(self) -> None:
        discovered = self._discovered(**{
            "lib/other.py::make": [DiscoveredSite(
                key="lib/other.py::make",
                lineno=42,
                wires_download_ownership=False,
            )],
        })

        violations = registry_violations(self.REGISTERED, discovered)

        self.assertEqual(len(violations), 1, violations)
        self.assertIn(
            "UNREGISTERED CratediggerContext construction at "
            "lib/other.py::make (line 42).",
            violations[0])

    def test_stale_registry_entry_clause(self) -> None:
        violations = registry_violations(self.REGISTERED, {})

        self.assertEqual(
            violations,
            [(
                "STALE registry entry lib/thing.py::build: no "
                "CratediggerContext construction is there any more. "
                "Remove the entry."
            )],
        )

    def test_lost_kwarg_clause(self) -> None:
        discovered = self._discovered(**{
            "lib/thing.py::build": [DiscoveredSite(
                key="lib/thing.py::build",
                lineno=10,
                wires_download_ownership=False,
            )],
        })

        violations = registry_violations(self.REGISTERED, discovered)

        self.assertEqual(
            violations,
            [(
                "lib/thing.py::build (line 10) lost its download_ownership= "
                "kwarg: the registry declares wires_download_ownership=True, "
                "the source says False."
            )],
        )

    def test_construction_count_clause(self) -> None:
        """The #1278 review F6 world: a SECOND construction appears inside
        an already-registered function, so it shares the registered key.

        Before the count clause the discovery dict collapsed the two
        (``sites[site.key] = site``) and this reported nothing at all --
        the exact shape (a collaborator-less second construction) the
        module was written to catch.
        """
        discovered = self._discovered(**{
            "lib/thing.py::build": [
                DiscoveredSite(
                    key="lib/thing.py::build",
                    lineno=10,
                    wires_download_ownership=True,
                ),
                DiscoveredSite(
                    key="lib/thing.py::build",
                    lineno=14,
                    wires_download_ownership=False,
                ),
            ],
        })

        violations = registry_violations(self.REGISTERED, discovered)

        self.assertEqual(
            violations,
            [
                (
                    "lib/thing.py::build holds 2 CratediggerContext "
                    "construction(s) (line 10, 14), but the registry "
                    "declares constructions=1. Every one of them needs a "
                    "declared answer on download_ownership."
                ),
                (
                    "lib/thing.py::build (line 14) lost its "
                    "download_ownership= kwarg: the registry declares "
                    "wires_download_ownership=True, the source says False."
                ),
            ],
        )

    def test_a_conforming_second_construction_is_accepted_when_declared(
        self,
    ) -> None:
        """Must-still-work: the count is a declaration, not a ban."""
        registered = {
            "lib/thing.py::build": ContextSite(True, "wires it", 2),
        }
        discovered = self._discovered(**{
            "lib/thing.py::build": [
                DiscoveredSite(
                    key="lib/thing.py::build",
                    lineno=10,
                    wires_download_ownership=True,
                ),
                DiscoveredSite(
                    key="lib/thing.py::build",
                    lineno=14,
                    wires_download_ownership=True,
                ),
            ],
        })

        self.assertEqual(registry_violations(registered, discovered), [])

    def test_gained_kwarg_clause(self) -> None:
        registered = {"lib/thing.py::build": ContextSite(False, "cannot")}

        violations = registry_violations(registered, self._discovered())

        self.assertEqual(
            violations,
            [(
                "lib/thing.py::build (line 10) gained its download_ownership= "
                "kwarg: the registry declares wires_download_ownership=False, "
                "the source says True."
            )],
        )

    def test_every_clause_reports_when_several_are_violated(self) -> None:
        """Accumulating, not short-circuiting: one edit can trip several."""
        discovered = self._discovered(**{
            "lib/thing.py::build": [DiscoveredSite(
                key="lib/thing.py::build",
                lineno=10,
                wires_download_ownership=False,
            )],
            "lib/other.py::make": [DiscoveredSite(
                key="lib/other.py::make",
                lineno=42,
                wires_download_ownership=False,
            )],
        })
        registered = dict(self.REGISTERED)
        registered["lib/gone.py::old"] = ContextSite(True, "removed")

        violations = registry_violations(registered, discovered)

        self.assertEqual(len(violations), 3, violations)
        self.assertTrue(violations[0].startswith("UNREGISTERED"))
        self.assertTrue(violations[1].startswith("STALE"))
        self.assertIn("lost its download_ownership=", violations[2])

    def test_conforming_registry_reports_nothing(self) -> None:
        """Must-still-work: an exact registry raises no violation."""
        self.assertEqual(
            registry_violations(self.REGISTERED, self._discovered()), [])


class TestSiteDiscoveryGrammar(unittest.TestCase):
    """The bounded parse itself, on sources this test owns."""

    def test_both_recognised_spellings_are_found_with_their_scope(self) -> None:
        source = (
            "class Outer:\n"
            "    def build(self):\n"
            "        return CratediggerContext(cfg=1, download_ownership=w)\n"
            "\n"
            "def helper():\n"
            "    return context.CratediggerContext(cfg=1)\n"
            "\n"
            "TOP = CratediggerContext(cfg=1)\n"
        )

        sites = {s.key: s for s in _sites_in_module("lib/x.py", source)}

        self.assertEqual(
            set(sites),
            {
                "lib/x.py::Outer.build",
                "lib/x.py::helper",
                "lib/x.py::<module>",
            },
        )
        self.assertTrue(sites["lib/x.py::Outer.build"].wires_download_ownership)
        self.assertFalse(sites["lib/x.py::helper"].wires_download_ownership)

    def test_the_class_definition_itself_is_not_a_construction(self) -> None:
        source = (
            "class CratediggerContext:\n"
            "    pass\n"
        )
        self.assertEqual(_sites_in_module("lib/context.py", source), [])

    def test_two_constructions_in_one_function_both_survive_grouping(
        self,
    ) -> None:
        """Issue #1278 review F6, at the seam that actually lost them.

        Two constructions in one ``def`` share a scope chain, so they
        share a key. ``discover_production_context_sites`` used to assign
        ``sites[site.key] = site`` and keep only the last, which made a
        collaborator-less second construction invisible to every clause.
        """
        source = (
            "def main():\n"
            "    a = CratediggerContext(cfg=1, download_ownership=w)\n"
            "    b = CratediggerContext(cfg=1)\n"
            "    return a, b\n"
        )

        grouped = group_sites(_sites_in_module("cratedigger.py", source))

        self.assertEqual(list(grouped), ["cratedigger.py::main"])
        self.assertEqual(
            [
                (site.lineno, site.wires_download_ownership)
                for site in grouped["cratedigger.py::main"]
            ],
            [(2, True), (3, False)],
        )

    def test_a_nested_construction_keeps_its_full_scope_chain(self) -> None:
        source = (
            "def outer():\n"
            "    def inner():\n"
            "        return CratediggerContext(cfg=1)\n"
            "    return inner\n"
        )

        sites = _sites_in_module("lib/x.py", source)

        self.assertEqual([s.key for s in sites], ["lib/x.py::outer.inner"])

    def test_aliasing_the_class_is_a_violation(self) -> None:
        import_alias = (
            "from lib.context import CratediggerContext as Ctx\n"
        )
        rebind = (
            "from lib.context import CratediggerContext\n"
            "Ctx = CratediggerContext\n"
        )

        self.assertEqual(
            _alias_bindings("lib/x.py", import_alias),
            [(
                "lib/x.py:1: imports CratediggerContext as 'Ctx' -- "
                "construct it under its own name so the construction-site "
                "audit can see the site"
            )],
        )
        self.assertEqual(
            _alias_bindings("lib/x.py", rebind),
            [(
                "lib/x.py:2: rebinds CratediggerContext to another name -- "
                "construct it under its own name so the construction-site "
                "audit can see the site"
            )],
        )

    def test_attribute_and_annotated_rebinds_are_violations(self) -> None:
        """Issue #1278 review F7: neither form is dynamic, and both used
        to slip through while the module claimed a construction could
        never be spelled under a name it cannot see."""
        attribute_rebind = (
            "from lib import context\n"
            "Ctx = context.CratediggerContext\n"
        )
        annotated_rebind = (
            "from lib.context import CratediggerContext\n"
            "Ctx: type = CratediggerContext\n"
        )
        annotated_attribute_rebind = (
            "from lib import context\n"
            "Ctx: type = context.CratediggerContext\n"
        )
        expected = (
            "lib/x.py:2: rebinds CratediggerContext to another name -- "
            "construct it under its own name so the construction-site "
            "audit can see the site"
        )

        for label, source in (
            ("X = pkg.CratediggerContext", attribute_rebind),
            ("X: type = CratediggerContext", annotated_rebind),
            ("X: type = pkg.CratediggerContext", annotated_attribute_rebind),
        ):
            with self.subTest(form=label):
                self.assertEqual(_alias_bindings("lib/x.py", source),
                                 [expected])

    def test_a_plain_import_is_not_an_alias(self) -> None:
        """Must-still-work: the ordinary import every site uses."""
        self.assertEqual(
            _alias_bindings(
                "lib/x.py",
                "from lib.context import CratediggerContext\n"),
            [])

    def test_an_ordinary_annotated_field_is_not_an_alias(self) -> None:
        """Must-still-work: an annotated assignment whose value is not the
        class, and a bare annotation with no value at all -- both ordinary
        in ``lib/context.py`` itself."""
        for source in (
            "download_ownership: DownloadOwnershipWriter | None = None\n",
            "ctx: CratediggerContext\n",
            "def f() -> CratediggerContext: ...\n",
            "from lib import context\nCtx = context.SomethingElse\n",
        ):
            with self.subTest(source=source.strip()):
                self.assertEqual(_alias_bindings("lib/x.py", source), [])


if __name__ == "__main__":
    unittest.main()
