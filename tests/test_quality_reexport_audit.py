"""Audit: every `lib.quality` re-export has a caller (issue #1313).

`lib/quality/__init__.py` is a compat shim. The #477 split moved the
quality monolith into submodules and kept a package-level re-export so
`from lib.quality import X` went on working. `.claude/rules/scope.md`
bans compat shims outright and grants this one no exception; issue #1313
is where it is argued for, as the shim worth tolerating. Tolerable or
not, it only earns its keep while it carries names something asks for,
and nothing re-checked that. It had grown to 288 names, 75 of which no
module outside the package reached through the package at all.

This audit is the guard, not the cleanup. It asks one question per
exported name: does any `.py` file outside `lib/quality/` reach that name
THROUGH the package? A name nobody reaches that way is a re-export with
no caller, and the fix is to delete the entry, not to invent a caller.

**Grammar.** Three spellings count as a reference, all read off the AST
of every `.py` file in this checkout outside `lib/quality/`:

* `from lib.quality import NAME` (absolute, `level == 0`; an `as` alias
  does not change which name was asked for)
* attribute access on a binding of the package itself, from either
  `import lib.quality` (then `lib.quality.NAME`) or `from lib import
  quality` (then `quality.NAME`), including an `as` alias of either
* a string literal that IS `lib.quality.NAME`, which is how
  `unittest.mock.patch` names a target

The string clause matches the whole literal, never a substring. A
docstring that mentions the dotted path in passing is not a caller, and
issue #1313's own scan was unreliable for exactly that reason. Exactness
also stops `lib.quality.full_pipeline_decision_from_evidence` crediting
`full_pipeline_decision`, which containment would, in eight
prefix-sharing pairs.

It is fail-open legislation rather than a live clause: measured on the
trimmed tree, zero of the 213 retained names are kept alive by it alone,
and no module patches a `lib.quality` target today. It stays because
deleting it would make the first such caller see this audit demand the
removal of a name it uses; its known-bad world below proves it trips.

`from lib.quality.<submodule> import NAME` deliberately does NOT count.
That import bypasses the package namespace entirely, which is the whole
point: it is what a caller does once the re-export is gone, so counting
it would make the audit unfalsifiable.

The grammar is bounded and syntactic on purpose (`.claude/rules/
code-quality.md` § "Semantic source scanners are prohibited"), and its
blind spots are real. It does no alias tracking beyond the two package
bindings above, so `getattr(quality, "NAME")` is invisible, and so is
the shape this repository actually uses elsewhere: a module path and an
attribute name held as two separate strings and joined at runtime, the
way `lib/convergence.py::resolve_convergence_target` takes them. A
caller writing either would see this audit demand the deletion of a name
it uses. `from lib.quality import *` is the one shape that makes the
question unanswerable rather than merely awkward, and it fails closed
with its own message instead of silently marking every name live.
"""

from __future__ import annotations

import ast
import tempfile
import unittest
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from pathlib import Path

import lib.quality

REPO_ROOT = Path(__file__).resolve().parent.parent
PACKAGE = "lib.quality"
PACKAGE_DIR = "lib/quality/"


#: Directories under the repository root that hold a DIFFERENT checkout's
#: Python, not this one's: agent worktrees, mutmut's mutated copy, Nix build
#: results. Walking into any of them answers the demand question about
#: somebody else's source.
_FOREIGN_TREES = frozenset({".claude", "mutants", "result", ".git", ".direnv"})


def _python_sources(repo_root: Path) -> tuple[Path, ...]:
    """Every `.py` file belonging to this checkout.

    A tree walk rather than `git ls-files`, deliberately: mutmut runs the
    suite from inside an untracked `mutants/` copy, where `git ls-files`
    returns nothing at all. The audit does not go quietly green on that
    empty file set — every export reads as an orphan and it fails — but
    it fails accusing the export list of carrying 213 dead entries, which
    sends the reader to the wrong file. Issue #1329 hit the same trap and
    recorded it: a test that shells out to git is invisible to the
    catalog by construction.
    """
    return tuple(
        path
        for path in repo_root.rglob("*.py")
        if not (
            set(path.relative_to(repo_root).parts) & _FOREIGN_TREES
        )
    )


def _package_bindings(tree: ast.Module) -> set[str]:
    """Local names bound to the `lib.quality` module object itself."""
    bindings: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name == PACKAGE:
                    # `import lib.quality` binds `lib`; `... as q` binds `q`.
                    bindings.add(alias.asname or "lib")
        elif (
            isinstance(node, ast.ImportFrom)
            and node.level == 0
            and node.module == "lib"
        ):
            for alias in node.names:
                if alias.name == "quality":
                    bindings.add(alias.asname or "quality")
    return bindings


def _attribute_names(tree: ast.Module, bindings: set[str]) -> Iterator[str]:
    """Attributes read off a package binding: `quality.X`, `lib.quality.X`."""
    if not bindings:
        return
    for node in ast.walk(tree):
        if not isinstance(node, ast.Attribute):
            continue
        base = node.value
        # `q.NAME` where q is the package, or `lib.quality.NAME`.
        if (isinstance(base, ast.Name) and base.id in bindings) or (
            isinstance(base, ast.Attribute)
            and base.attr == "quality"
            and isinstance(base.value, ast.Name)
            and base.value.id in bindings
        ):
            yield node.attr


@dataclass(frozen=True)
class Source:
    """One candidate file, read and parsed once."""

    where: str
    tree: ast.Module


def load_sources(paths: Iterable[Path]) -> tuple[Source, ...]:
    """Read and parse every file that could possibly hold a reference.

    Every spelling this grammar recognises needs the word `quality`
    somewhere: `lib.quality`, `from lib import quality`, `import
    lib.quality`. Skipping the rest before parsing is what keeps this
    audit affordable, and it must be the LOOSE word rather than the
    dotted path, which the `from lib import quality as q` form never
    spells.

    Both scans share the result. Reading and parsing the tree twice, once
    per scan, is the same answer for double the wall clock, and this is a
    `test_*_audit` module: it runs on every selection, not just the ones
    that touch `lib/quality/`.
    """
    sources: list[Source] = []
    for path in paths:
        text = path.read_text(encoding="utf-8", errors="replace")
        if "quality" not in text:
            continue
        sources.append(Source(where=path.as_posix(), tree=ast.parse(text)))
    return tuple(sources)


def star_import_files(sources: Iterable[Source]) -> tuple[str, ...]:
    """Files that star-import the package, which this grammar cannot read."""
    found: list[str] = []
    for source in sources:
        for node in ast.walk(source.tree):
            if (
                isinstance(node, ast.ImportFrom)
                and node.level == 0
                and node.module == PACKAGE
                and any(alias.name == "*" for alias in node.names)
            ):
                found.append(source.where)
                break
    return tuple(found)


def referenced_through_package(
    sources: Iterable[Source], names: Iterable[str]
) -> dict[str, tuple[str, ...]]:
    """`{exported name: files reaching it through the package}`.

    Every requested name is a key, so a name with an empty tuple is the
    finding rather than a missing entry the caller has to notice.
    """
    wanted = set(names)
    #: `"lib.quality.NAME"` → `NAME`, for the string-target clause. A
    #: patch target is the WHOLE string, so this is an exact lookup rather
    #: than a containment test over 213 names. Containment fails twice: a
    #: docstring that merely mentions the dotted path would keep a name
    #: alive on prose (the exact flaw issue #1313 warned its own scan had),
    #: and `lib.quality.full_pipeline_decision_from_evidence` would credit
    #: `full_pipeline_decision` too, since one exported name is a prefix of
    #: another in eight pairs.
    dotted = {f"{PACKAGE}.{name}": name for name in wanted}
    hits: dict[str, set[str]] = {name: set() for name in wanted}
    for source in sources:
        tree = source.tree
        where = source.where
        for node in ast.walk(tree):
            if (
                isinstance(node, ast.ImportFrom)
                and node.level == 0
                and node.module == PACKAGE
            ):
                for alias in node.names:
                    if alias.name in wanted:
                        hits[alias.name].add(where)
            elif isinstance(node, ast.Constant) and isinstance(node.value, str):
                target = dotted.get(node.value)
                if target is not None:
                    hits[target].add(where)
        for attribute in _attribute_names(tree, _package_bindings(tree)):
            if attribute in wanted:
                hits[attribute].add(where)
    return {name: tuple(sorted(found)) for name, found in hits.items()}


class TestQualityReexportHasCallers(unittest.TestCase):
    """The package re-export carries exactly the names something asks for."""

    files: tuple[Path, ...]
    sources: tuple[Source, ...]

    @classmethod
    def setUpClass(cls) -> None:
        cls.files = tuple(
            path
            for path in _python_sources(REPO_ROOT)
            if not path.relative_to(REPO_ROOT).as_posix().startswith(PACKAGE_DIR)
        )
        cls.sources = load_sources(cls.files)

    def test_no_module_star_imports_the_package(self) -> None:
        self.assertEqual(
            star_import_files(self.sources),
            (),
            "`from lib.quality import *` makes per-name demand unanswerable; "
            "import the names you use",
        )

    def test_every_exported_name_is_reached_through_the_package(self) -> None:
        exported = tuple(lib.quality.__all__)
        references = referenced_through_package(self.sources, exported)
        orphans = sorted(name for name, found in references.items() if not found)
        self.assertEqual(
            orphans,
            [],
            f"{len(orphans)} of {len(exported)} lib.quality re-exports have no "
            "caller through the package. Delete them from the import block and "
            "__all__; a caller that wants one imports it from the submodule.",
        )

    def test_every_exported_name_is_actually_bound(self) -> None:
        """`__all__` and the import block above it are edited by hand.

        A second net, not the only one: measured against pyright 1.1.412,
        an unbound `__all__` entry is already `reportUnsupportedDunderAll`
        — a warning in the whole-repo contract and an error in the
        production-strict one, so the suite's pyright phase fails on it
        too. Kept because it costs one `hasattr` loop and says what went
        wrong in the vocabulary of the file it went wrong in.
        """
        missing = [
            name for name in lib.quality.__all__ if not hasattr(lib.quality, name)
        ]
        self.assertEqual(
            missing,
            [],
            "__all__ names something the import block does not bind",
        )

    def test_the_scan_finds_a_real_population(self) -> None:
        """A file set this audit cannot read must not read as a clean pass.

        The first draft enumerated files with `git ls-files`, which returns
        nothing from inside mutmut's untracked `mutants/` copy. Per-name
        emptiness makes that loud rather than silent — zero files means
        every name is an orphan — but the failure then blames the export
        list for a broken walk. This pins the walk itself.
        """
        seen = {path.relative_to(REPO_ROOT).as_posix() for path in self.files}
        self.assertGreater(len(self.files), 500)
        # One production root and one test root, because the demand
        # question is answered from both and a filter that swallowed
        # either would still leave the count above the floor.
        self.assertIn("lib/dispatch/core.py", seen)
        self.assertIn("tests/test_quality_decisions.py", seen)

    def test_the_walk_stops_at_other_checkouts(self) -> None:
        """A sibling agent worktree's copy of this repo is not a caller.

        Against a synthetic root, not this one. Asserting the real walk
        returned nothing foreign passes whether or not the filter exists,
        because this checkout happens to hold no `.py` under any of those
        directories — a pin no mutant can reach, which is how the first
        version of it survived dropping the filter outright.
        """
        root = Path(self.enterContext(tempfile.TemporaryDirectory()))
        foreign = [
            f"{name}/lib/theirs.py" for name in sorted(_FOREIGN_TREES)
        ]
        # A foreign tree at depth, not only at the root: a nix `result`
        # symlink or a nested checkout can sit anywhere, and matching on
        # `parts[0]` alone would walk straight into it.
        foreign.append("vendor/thing/result/lib/built.py")
        for relative in ("lib/mine.py", *foreign):
            target = root / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text("from lib.quality import quality_rank\n")
        self.assertEqual(
            [path.relative_to(root).as_posix() for path in _python_sources(root)],
            ["lib/mine.py"],
        )


class TestReferenceGrammar(unittest.TestCase):
    """One known-bad or must-still-work world per clause of the grammar."""

    def _world(self, source: str) -> Path:
        directory = Path(self.enterContext(tempfile.TemporaryDirectory()))
        path = directory / "world.py"
        path.write_text(source, encoding="utf-8")
        return path

    def _scan(
        self, source: str, names: tuple[str, ...]
    ) -> dict[str, tuple[str, ...]]:
        return referenced_through_package(
            load_sources((self._world(source),)), names
        )

    def test_direct_from_import_counts(self) -> None:
        found = self._scan(
            "from lib.quality import quality_rank\n", ("quality_rank",)
        )
        self.assertEqual(len(found["quality_rank"]), 1)

    def test_aliased_from_import_counts_under_its_real_name(self) -> None:
        found = self._scan(
            "from lib.quality import quality_rank as rank\n",
            ("quality_rank", "rank"),
        )
        self.assertEqual(len(found["quality_rank"]), 1)
        self.assertEqual(found["rank"], ())

    def test_attribute_on_a_dotted_package_import_counts(self) -> None:
        found = self._scan(
            "import lib.quality\n\nvalue = lib.quality.quality_rank\n",
            ("quality_rank",),
        )
        self.assertEqual(len(found["quality_rank"]), 1)

    def test_attribute_on_a_from_lib_import_counts(self) -> None:
        found = self._scan(
            "from lib import quality as q\n\nvalue = q.quality_rank\n",
            ("quality_rank",),
        )
        self.assertEqual(len(found["quality_rank"]), 1)

    def test_string_literal_target_counts(self) -> None:
        """The motivating shape is a `mock.patch` target, spelled as data.

        The world is built without the literal call because
        `tests/test_mock_audit.py` scans this file for patch targets and
        would read a synthetic one as a real stateful-mock site.
        """
        found = self._scan(
            'TARGET = "lib.quality.quality_rank"\n', ("quality_rank",)
        )
        self.assertEqual(len(found["quality_rank"]), 1)

    def test_submodule_import_is_not_demand_on_the_package(self) -> None:
        """The known-bad world: counting this would make the audit vacuous."""
        found = self._scan(
            "from lib.quality.ranks import quality_rank\n", ("quality_rank",)
        )
        self.assertEqual(found["quality_rank"], ())

    def test_attribute_on_an_unrelated_binding_is_not_demand(self) -> None:
        """A DIFFERENT module's attribute, in a file that also imports ours.

        The `import lib.quality` line is what makes this world reach the
        membership check at all. Without it there are no bindings, so
        `_attribute_names` returns at its own guard and nothing downstream
        runs — which is how the first version of this test passed while
        deleting `base.id in bindings` outright (review mutant M19).
        """
        found = self._scan(
            "import lib.quality\nimport other\n\nvalue = other.quality_rank\n",
            ("quality_rank",),
        )
        self.assertEqual(found["quality_rank"], ())

    def test_a_file_with_no_package_binding_reads_no_attributes(self) -> None:
        """The guard the world above deliberately steps past."""
        found = self._scan(
            "import other\n\nvalue = other.quality_rank\n", ("quality_rank",)
        )
        self.assertEqual(found["quality_rank"], ())

    def test_star_import_is_reported_rather_than_counted(self) -> None:
        sources = load_sources((self._world("from lib.quality import *\n"),))
        self.assertEqual(len(star_import_files(sources)), 1)
        self.assertEqual(
            referenced_through_package(sources, ("quality_rank",))["quality_rank"],
            (),
        )


if __name__ == "__main__":
    unittest.main()
