"""Audit: every `lib.quality` re-export has a caller (issue #1313).

`lib/quality/__init__.py` is a compat shim. The #477 split moved the
quality monolith into submodules and kept a package-level re-export so
`from lib.quality import X` went on working; CLAUDE.md records it as the
one shim the single-operator rule tolerates. A shim earns that tolerance
only while it carries names something asks for, and nothing re-checked
that. It had grown to 288 names, 75 of which no module outside the
package reached through the package at all.

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
* a string literal containing `lib.quality.NAME`, which is how
  `unittest.mock.patch` names a target

`from lib.quality.<submodule> import NAME` deliberately does NOT count.
That import bypasses the package namespace entirely, which is the whole
point: it is what a caller does once the re-export is gone, so counting
it would make the audit unfalsifiable.

The grammar is bounded and syntactic on purpose (`.claude/rules/
code-quality.md` § "Semantic source scanners are prohibited"). It does no
alias tracking beyond the two direct package bindings above: `import lib`
followed by `lib.quality.NAME` is recognised, but stashing the module in
a dict or a local and reaching the name through that is not. A caller
writing one of those would see this audit demand the deletion of a name
it uses — so `from lib.quality import *`, the one shape that makes the
question unanswerable rather than merely awkward, fails closed with its
own message instead of silently marking all 288 names live.
"""

from __future__ import annotations

import ast
import tempfile
import unittest
from collections.abc import Iterable, Iterator
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
    returns nothing at all and this audit would pass on an empty file set
    while reporting all 213 exports orphaned. Issue #1329 hit the same
    trap and recorded it — a test that shells out to git is invisible to
    the catalog by construction.
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


def star_import_files(files: Iterable[Path]) -> tuple[str, ...]:
    """Files that star-import the package, which this grammar cannot read."""
    found: list[str] = []
    for path in files:
        tree = ast.parse(path.read_text(encoding="utf-8", errors="replace"))
        for node in ast.walk(tree):
            if (
                isinstance(node, ast.ImportFrom)
                and node.level == 0
                and node.module == PACKAGE
                and any(alias.name == "*" for alias in node.names)
            ):
                found.append(path.as_posix())
                break
    return tuple(found)


def referenced_through_package(
    files: Iterable[Path], names: Iterable[str]
) -> dict[str, tuple[str, ...]]:
    """`{exported name: files reaching it through the package}`.

    Every requested name is a key, so a name with an empty tuple is the
    finding rather than a missing entry the caller has to notice.
    """
    wanted = set(names)
    hits: dict[str, set[str]] = {name: set() for name in wanted}
    for path in files:
        source = path.read_text(encoding="utf-8", errors="replace")
        # Every spelling this grammar recognises needs the word somewhere:
        # `lib.quality`, `from lib import quality`, `import lib.quality`.
        # Keying the skip on the dotted path instead would drop the
        # `from lib import quality as q` form, which never spells it.
        if "quality" not in source:
            continue
        tree = ast.parse(source)
        where = path.as_posix()
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
                for name in wanted:
                    if f"{PACKAGE}.{name}" in node.value:
                        hits[name].add(where)
        for attribute in _attribute_names(tree, _package_bindings(tree)):
            if attribute in wanted:
                hits[attribute].add(where)
    return {name: tuple(sorted(found)) for name, found in hits.items()}


class TestQualityReexportHasCallers(unittest.TestCase):
    """The package re-export carries exactly the names something asks for."""

    files: tuple[Path, ...]

    @classmethod
    def setUpClass(cls) -> None:
        cls.files = tuple(
            path
            for path in _python_sources(REPO_ROOT)
            if not path.relative_to(REPO_ROOT).as_posix().startswith(PACKAGE_DIR)
        )

    def test_no_module_star_imports_the_package(self) -> None:
        self.assertEqual(
            star_import_files(self.files),
            (),
            "`from lib.quality import *` makes per-name demand unanswerable; "
            "import the names you use",
        )

    def test_every_exported_name_is_reached_through_the_package(self) -> None:
        exported = tuple(lib.quality.__all__)
        references = referenced_through_package(self.files, exported)
        orphans = sorted(name for name, found in references.items() if not found)
        self.assertEqual(
            orphans,
            [],
            f"{len(orphans)} of {len(exported)} lib.quality re-exports have no "
            "caller through the package. Delete them from the import block and "
            "__all__; a caller that wants one imports it from the submodule.",
        )

    def test_every_exported_name_is_actually_bound(self) -> None:
        """`__all__` and the import block above it are edited by hand."""
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
        self.assertGreater(len(self.files), 500)
        self.assertIn(
            "lib/dispatch/core.py",
            {path.relative_to(REPO_ROOT).as_posix() for path in self.files},
        )

    def test_the_walk_stops_at_other_checkouts(self) -> None:
        """A sibling agent worktree's copy of this repo is not a caller.

        Against a synthetic root, not this one. Asserting the real walk
        returned nothing foreign passes whether or not the filter exists,
        because this checkout happens to hold no `.py` under any of those
        directories — a pin no mutant can reach, which is how the first
        version of it survived dropping the filter outright.
        """
        root = Path(self.enterContext(tempfile.TemporaryDirectory()))
        for relative in (
            "lib/mine.py",
            ".claude/worktrees/agent-x/lib/theirs.py",
            "mutants/lib/mutated.py",
            "result/lib/built.py",
        ):
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
        return referenced_through_package((self._world(source),), names)

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
        found = self._scan(
            "import other\n\nvalue = other.quality_rank\n", ("quality_rank",)
        )
        self.assertEqual(found["quality_rank"], ())

    def test_star_import_is_reported_rather_than_counted(self) -> None:
        world = self._world("from lib.quality import *\n")
        self.assertEqual(len(star_import_files((world,))), 1)
        self.assertEqual(
            referenced_through_package((world,), ("quality_rank",))["quality_rank"],
            (),
        )


if __name__ == "__main__":
    unittest.main()
