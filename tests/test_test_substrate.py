"""Pin the shared test-runtime substrate's standard-library-only boundary.

``scripts/test_substrate.py`` is the one home for admission, headroom,
``/proc`` liveness, reaping, the final gate, and the on-disk names those
formats use (issue #1278 item 6). ``scripts/run_final_gate.sh`` execs it
OUTSIDE any Nix dev shell — that gate is what launches ``nix develop`` in
the first place — with whatever bare ``python3`` is on PATH, so the module
has to stay importable with nothing but the standard library: no
``msgspec``, no ``lib/``, no ``tests/``. (``scripts/test_tmpfs.sh``'s shell
hook also runs it, as a subprocess whose ``python3`` IS the dev shell's own;
that caller does not need this boundary, the gate does.)

Two independent instruments, because either alone is weak. The AST audit is a
deliberately bounded, single-file syntactic check (it reads one named file and
looks at import statements only — never an inference about runtime semantics,
per code-quality.md's semantic-scanner prohibition). The subprocess import
drives the real module with third-party ``site-packages`` entries stripped
from ``sys.path``, which is what actually proves the boundary holds; its own
control imports ``scripts/run_test_suite.py`` the same way and requires it to
FAIL, so a green result there is evidence the harness discriminates rather
than evidence that everything imports.
"""

from __future__ import annotations

import ast
import subprocess
import sys
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
SUBSTRATE = REPO_ROOT / "scripts" / "test_substrate.py"


def substrate_import_violations(source: str, *, label: str) -> list[str]:
    """Every import in ``source`` that is not a plain standard-library import.

    Accumulates rather than raising on the first offender, so one clause can
    never mask another (code-quality.md, "prefer an accumulating list of
    violations over a short-circuiting raise chain").

    Walks the WHOLE tree, not just module scope: a deferred
    ``import msgspec`` inside a function would evade a top-level-only check
    while breaking the module for a bare interpreter just the same, at the
    exact moment the function is called.

    Fails closed on a relative import: ``from . import x`` names a sibling
    this audit cannot resolve to a distribution, and the substrate has no
    business importing one.
    """
    violations: list[str] = []
    for node in ast.walk(ast.parse(source, filename=label)):
        if isinstance(node, ast.Import):
            for alias in node.names:
                root = alias.name.split(".")[0]
                if root not in sys.stdlib_module_names:
                    violations.append(
                        f"{label}:{node.lineno}: non-stdlib import {alias.name}"
                    )
        elif isinstance(node, ast.ImportFrom):
            if node.level or node.module is None:
                violations.append(
                    f"{label}:{node.lineno}: relative import is not resolvable"
                )
                continue
            root = node.module.split(".")[0]
            if root not in sys.stdlib_module_names:
                violations.append(
                    f"{label}:{node.lineno}: non-stdlib import {node.module}"
                )
    return violations


def _import_with_site_packages_stripped(module: str) -> subprocess.CompletedProcess[str]:
    """Import ``module`` in a child whose ``sys.path`` has no site-packages.

    The dev shell's interpreter carries every third-party dependency in its
    own ``site-packages`` (``python3 -I`` still finds ``msgspec`` there), so
    isolation flags alone prove nothing. Removing those entries leaves the
    standard library plus this repository — exactly the world a bare
    ``python3`` outside the Nix shell would see.
    """
    program = (
        "import sys\n"
        "sys.path = [entry for entry in sys.path"
        " if 'site-packages' not in entry]\n"
        f"sys.path.insert(0, {str(REPO_ROOT)!r})\n"
        f"import {module} as loaded\n"
        "print(loaded.__name__)\n"
    )
    return subprocess.run(
        [sys.executable, "-c", program],
        capture_output=True,
        text=True,
        check=False,
        cwd=REPO_ROOT,
    )


class TestSubstrateImportsOnlyTheStandardLibrary(unittest.TestCase):
    def test_the_real_module_has_no_non_stdlib_import(self) -> None:
        self.assertEqual(
            substrate_import_violations(
                SUBSTRATE.read_text(encoding="utf-8"),
                label="scripts/test_substrate.py",
            ),
            [],
        )

    def test_the_module_imports_with_site_packages_stripped(self) -> None:
        completed = _import_with_site_packages_stripped("scripts.test_substrate")

        self.assertEqual(completed.returncode, 0, completed.stderr)
        self.assertEqual(completed.stdout.strip(), "scripts.test_substrate")

    def test_the_stripped_import_control_really_discriminates(self) -> None:
        """Known-bad for the subprocess harness itself: the suite coordinator
        DOES depend on msgspec, so it must fail under the same stripped path.
        Without this, a green sibling above could mean "site-packages was
        never actually stripped" rather than "the substrate is clean"."""
        completed = _import_with_site_packages_stripped("scripts.run_test_suite")

        self.assertNotEqual(completed.returncode, 0)
        self.assertIn("msgspec", completed.stderr)


class TestSubstrateImportAuditTripsOnViolations(unittest.TestCase):
    """One known-bad world per clause of ``substrate_import_violations``."""

    def test_top_level_non_stdlib_import_is_named(self) -> None:
        violations = substrate_import_violations(
            "import os\nimport msgspec\n", label="planted.py"
        )

        self.assertEqual(violations, ["planted.py:2: non-stdlib import msgspec"])

    def test_non_stdlib_from_import_is_named(self) -> None:
        violations = substrate_import_violations(
            "from lib.quality import pipeline\n", label="planted.py"
        )

        self.assertEqual(violations, ["planted.py:1: non-stdlib import lib.quality"])

    def test_relative_import_fails_closed(self) -> None:
        violations = substrate_import_violations(
            "from . import sibling\n", label="planted.py"
        )

        self.assertEqual(
            violations, ["planted.py:1: relative import is not resolvable"]
        )

    def test_a_deferred_import_inside_a_function_is_still_caught(self) -> None:
        violations = substrate_import_violations(
            "def load():\n    import msgspec\n    return msgspec\n",
            label="planted.py",
        )

        self.assertEqual(violations, ["planted.py:2: non-stdlib import msgspec"])

    def test_every_offender_is_reported_not_just_the_first(self) -> None:
        """The accumulating shape itself: an earlier violation must not hide
        a later one, which a short-circuiting checker would do."""
        violations = substrate_import_violations(
            "import msgspec\nfrom . import sibling\nfrom tests import helpers\n",
            label="planted.py",
        )

        self.assertEqual(
            violations,
            [
                "planted.py:1: non-stdlib import msgspec",
                "planted.py:2: relative import is not resolvable",
                "planted.py:3: non-stdlib import tests",
            ],
        )

    def test_a_clean_stdlib_only_module_reports_nothing(self) -> None:
        """Must-still-work: the audit does not reject legitimate imports —
        plain, dotted, aliased, and ``__future__`` alike."""
        violations = substrate_import_violations(
            "from __future__ import annotations\n"
            "import fcntl\n"
            "import os.path as ospath\n"
            "from collections.abc import Callable\n"
            "from pathlib import Path\n",
            label="planted.py",
        )

        self.assertEqual(violations, [])


if __name__ == "__main__":
    unittest.main()
