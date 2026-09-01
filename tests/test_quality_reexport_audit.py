"""Audit: every `lib.quality` re-export has a caller (issue #1313).

`lib/quality/__init__.py` is a compat shim. The #477 split moved the
quality monolith into submodules and kept a package-level re-export so
`from lib.quality import X` went on working; CLAUDE.md records it as the
one shim the single-operator rule tolerates. A shim earns that tolerance
only while it carries names something asks for, and nothing re-checked
that. It had grown to 288 names, 75 of which no module outside the
package imported through the package at all — every one of those 75
already imported straight from its own submodule somewhere, so the
package entry was pure redundancy a reader has to walk past.

This audit is the guard, not the cleanup. It asks one question per
exported name: does any `.py` file outside `lib/quality/` reach that name
THROUGH the package? A name nobody reaches that way is a re-export with
no caller, and the fix is to delete the entry, not to invent a caller.

**Grammar.** Three spellings count as a reference, all read off the AST
of every tracked `.py` file outside `lib/quality/`:

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
import subprocess
import tempfile
import unittest
from collections.abc import Iterable, Iterator
from pathlib import Path

import lib.quality

REPO_ROOT = Path(__file__).resolve().parent.parent
PACKAGE = "lib.quality"
PACKAGE_DIR = "lib/quality/"


def _tracked_python_files(repo_root: Path) -> tuple[Path, ...]:
    """Every tracked `.py` file, from git rather than a tree walk.

    A tree walk under this repository also finds `.claude/worktrees/`
    copies of it and mutmut's `mutants/` tree, so it would answer the
    question about a different checkout's source.
    """
    listing = subprocess.run(
        ["git", "-C", str(repo_root), "ls-files", "-z", "*.py"],
        capture_output=True,
        text=True,
        check=True,
    ).stdout
    return tuple(repo_root / name for name in listing.split("\0") if name)


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
            for path in _tracked_python_files(REPO_ROOT)
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
