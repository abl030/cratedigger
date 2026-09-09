"""The Beets-interpreter harness files import no Cratedigger package.

Three files under ``harness/`` run inside the deployment-owned Beets
interpreter, not Cratedigger's: `beets_harness.py` is what
`run_beets_harness.sh` execs, and it loads `beets_compat.py` and
`discogs_patches.py`. That interpreter has `beets`, `msgspec` and
`requests` on its path and none of Cratedigger's own third-party
dependencies, so an import of `lib` or `web` from one of these three
files breaks the import child rather than failing a test. Exactly where
it breaks depends on the invocation: often at the import itself, and
where the repo root happens to be on `sys.path`, deeper in the imported
module's own transitive closure instead.

`harness/import_one.py` and `harness/delete_album.py` are deliberately NOT
in this set. They run in Cratedigger's own interpreter and import `lib`
modules by the dozen by design; issue #1389 measured that and recorded it
rather than legislating a contract the harness root has never held.

Scope is exactly the three registered files, parsed for their import
statements. This is a static audit of a local syntactic fact over a named
file set — not a repository-wide import-graph analyzer (§ "Semantic source
scanners are prohibited"), and deterministic-only because its subject is a
static audit.
"""

from __future__ import annotations

import ast
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent

#: The files `run_beets_harness.sh` puts inside the Beets interpreter.
#: `beets_harness.py` is the entry point; the other two are what it loads.
BEETS_INTERPRETER_FILES: tuple[str, ...] = (
    "harness/beets_harness.py",
    "harness/beets_compat.py",
    "harness/discogs_patches.py",
)

#: The two Cratedigger top-level packages that interpreter cannot import.
CRATEDIGGER_PACKAGES = frozenset({"lib", "web"})


def cratedigger_package_imports(source: str, *, filename: str) -> list[str]:
    """Every `lib`/`web` module name one file's import statements name.

    Reads ALL import statements, including ones guarded by
    ``if TYPE_CHECKING:`` — a type-only import of a Cratedigger module is
    still this file depending on a package its interpreter does not have,
    and the guard only decides whether the failure is immediate.

    Relative imports are skipped: `harness/` is the deepest package a
    relative import from these files can reach, so one can never name
    `lib` or `web`. Matching is on the ROOT segment, so a module whose
    name merely starts with those letters (`library`, `webbrowser`) is not
    a finding.
    """
    found: list[str] = []
    for node in ast.walk(ast.parse(source, filename)):
        if isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name.split(".")[0] in CRATEDIGGER_PACKAGES:
                    found.append(alias.name)
        elif isinstance(node, ast.ImportFrom):
            if node.level:
                continue
            module = node.module or ""
            if module.split(".")[0] in CRATEDIGGER_PACKAGES:
                found.append(module)
    return sorted(found)


class TestBeetsInterpreterFilesImportNoCratediggerPackage(unittest.TestCase):
    def test_every_registered_file_exists(self) -> None:
        """A renamed or deleted file must not silently vacate the audit."""
        for relative_path in BEETS_INTERPRETER_FILES:
            with self.subTest(path=relative_path):
                self.assertTrue(
                    (REPO_ROOT / relative_path).is_file(),
                    f"{relative_path} is registered as a Beets-interpreter "
                    "file but does not exist",
                )

    def test_no_registered_file_imports_lib_or_web(self) -> None:
        for relative_path in BEETS_INTERPRETER_FILES:
            with self.subTest(path=relative_path):
                source = (REPO_ROOT / relative_path).read_text(encoding="utf-8")

                self.assertEqual(
                    cratedigger_package_imports(
                        source, filename=relative_path),
                    [],
                    f"{relative_path} runs inside the Beets interpreter, "
                    "which has no Cratedigger package on its path",
                )


class TestCheckerTripsOnViolations(unittest.TestCase):
    """One known-bad world per import shape the checker must report."""

    def test_plain_import_of_a_cratedigger_package(self) -> None:
        self.assertEqual(
            cratedigger_package_imports(
                "import lib.quality\n", filename="probe.py"),
            ["lib.quality"],
        )

    def test_aliased_import_of_a_cratedigger_package(self) -> None:
        self.assertEqual(
            cratedigger_package_imports(
                "import web.classify as classify\n", filename="probe.py"),
            ["web.classify"],
        )

    def test_from_import_of_a_cratedigger_submodule(self) -> None:
        self.assertEqual(
            cratedigger_package_imports(
                "from lib.mb_api import get_release\n", filename="probe.py"),
            ["lib.mb_api"],
        )

    def test_from_package_import_of_a_submodule_name(self) -> None:
        self.assertEqual(
            cratedigger_package_imports(
                "from web import classify\n", filename="probe.py"),
            ["web"],
        )

    def test_a_type_checking_guarded_import_still_counts(self) -> None:
        self.assertEqual(
            cratedigger_package_imports(
                "from typing import TYPE_CHECKING\n\n"
                "if TYPE_CHECKING:\n"
                "    from lib.quality import ImportResult\n",
                filename="probe.py",
            ),
            ["lib.quality"],
        )

    def test_a_function_local_import_still_counts(self) -> None:
        self.assertEqual(
            cratedigger_package_imports(
                "def load():\n"
                "    import lib.beets\n\n"
                "    return lib.beets\n",
                filename="probe.py",
            ),
            ["lib.beets"],
        )


class TestCheckerIsQuietWhereTheFileIsCorrect(unittest.TestCase):
    """The shapes these three files really use must never be reported."""

    def test_the_real_beets_and_stdlib_imports_are_not_findings(self) -> None:
        self.assertEqual(
            cratedigger_package_imports(
                "import importlib\n"
                "import msgspec\n"
                "import requests\n"
                "from beets import config, library, plugins\n"
                "from beets.autotag import AlbumInfo\n"
                "from harness import beets_compat, discogs_patches\n"
                "from harness.beets_compat import duplicate_action\n",
                filename="probe.py",
            ),
            [],
        )

    def test_a_name_that_merely_starts_with_a_package_name_is_not_a_finding(
        self,
    ) -> None:
        """Root-SEGMENT matching, not a prefix match."""
        self.assertEqual(
            cratedigger_package_imports(
                "import webbrowser\n"
                "import library\n"
                "from liberal import thing\n"
                "from webcolors import name_to_rgb\n",
                filename="probe.py",
            ),
            [],
        )

    def test_a_relative_import_is_not_a_finding(self) -> None:
        """`harness/` is the deepest package these files can reach.

        The last two lines are what make the ``node.level`` skip
        load-bearing. The first two do not: their ``node.module`` is
        ``None`` / ``"beets_compat"``, which no root-segment check would
        report anyway, so with the skip deleted they still return ``[]``.
        A sibling named ``lib`` or ``web`` inside ``harness/`` is the only
        world that tells the two apart.
        """
        self.assertEqual(
            cratedigger_package_imports(
                "from . import beets_compat\n"
                "from .beets_compat import duplicate_action\n"
                "from .lib import helper\n"
                "from .web.mb import get_release\n",
                filename="probe.py",
            ),
            [],
        )


if __name__ == "__main__":
    unittest.main()
