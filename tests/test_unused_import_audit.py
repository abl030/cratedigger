"""Contracts for the production-only Ruff unused-import gate."""

from __future__ import annotations

import json
from pathlib import Path
import subprocess
import tempfile
import unittest


RUFF_RULES = "F401,F811"
EXPECTED_PRODUCTION_ROOTS = (
    "lib",
    "web",
    "harness",
    "scripts",
    "cratedigger.py",
    "album_source.py",
)
LEGACY_EXPORT_SURFACES = (
    "cratedigger.py",
    "lib/pipeline_db/_shared.py",
    "scripts/pipeline_cli/__init__.py",
)


def ruff_findings(sources: dict[str, str]) -> tuple[dict[str, object], ...]:
    """Run the real pinned Ruff over a synthetic source world."""
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        paths: list[str] = []
        for relative_path, source in sources.items():
            path = root / relative_path
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(source, encoding="utf-8")
            paths.append(relative_path)
        result = subprocess.run(
            [
                "ruff",
                "check",
                "--select",
                RUFF_RULES,
                "--output-format",
                "json",
                *paths,
            ],
            cwd=root,
            text=True,
            capture_output=True,
            check=False,
        )
    if result.returncode not in {0, 1}:
        raise AssertionError(result.stderr or result.stdout)
    return tuple(json.loads(result.stdout))


def assert_import_liveness(
    findings: tuple[dict[str, object], ...],
    *,
    relative_path: str,
    import_is_live: bool,
) -> None:
    matching = [
        finding
        for finding in findings
        if Path(str(finding["filename"])).as_posix().endswith(relative_path)
        and finding["code"] in {"F401", "F811"}
    ]
    assert bool(matching) is not import_is_live


class TestUnusedImportAudit(unittest.TestCase):
    def test_peer_name_use_does_not_keep_an_import_live(self) -> None:
        findings = ruff_findings({
            "lib/importing.py": "from dependency import shared_name\n",
            "lib/peer.py": "shared_name = object()\nprint(shared_name)\n",
        })

        assert_import_liveness(
            findings,
            relative_path="lib/importing.py",
            import_is_live=False,
        )

    def test_scope_control_flow_annotations_and_exports_use_real_ruff(self) -> None:
        cases = {
            "unused.py": "import dependency\n",
            "parameter_shadow.py": (
                "import dependency\n"
                "def inspect(dependency): return dependency\n"
            ),
            "rebound.py": (
                "import dependency\n"
                "dependency = object()\nprint(dependency)\n"
            ),
            "comprehension.py": (
                "import dependency\n"
                "values = [dependency for dependency in candidates]\n"
            ),
            "nested_global.py": (
                "import dependency\n"
                "def inspect(): return dependency\n"
            ),
            "try_branch.py": (
                "try:\n    import dependency\n"
                "except ImportError:\n    dependency = fallback\n"
                "print(dependency)\n"
            ),
            "match_branch.py": (
                "match selector:\n"
                "    case True:\n        import dependency\n"
                "    case _:\n        dependency = fallback\n"
                "print(dependency)\n"
            ),
            "annotation.py": (
                "from __future__ import annotations\n"
                "from dependency import Model\n"
                "def load(value: 'Model') -> Model: return value\n"
            ),
            "export.py": "from dependency import Public\n__all__ = ['Public']\n",
            "side_effect.py": "import register_plugins\n",
            "explicit_export.py": "import register_plugins as register_plugins\n",
        }
        findings = ruff_findings({f"lib/{path}": source for path, source in cases.items()})

        for path in (
            "unused.py",
            "parameter_shadow.py",
            "rebound.py",
            "comprehension.py",
            "side_effect.py",
        ):
            with self.subTest(path=path):
                assert_import_liveness(
                    findings,
                    relative_path=f"lib/{path}",
                    import_is_live=False,
                )
        for path in (
            "nested_global.py",
            "try_branch.py",
            "match_branch.py",
            "annotation.py",
            "export.py",
            "explicit_export.py",
        ):
            with self.subTest(path=path):
                assert_import_liveness(
                    findings,
                    relative_path=f"lib/{path}",
                    import_is_live=True,
                )

    def test_each_legacy_surface_still_rejects_a_new_unused_import(self) -> None:
        repo_root = Path(__file__).resolve().parent.parent
        for relative_path in LEGACY_EXPORT_SURFACES:
            with self.subTest(relative_path=relative_path):
                source = (repo_root / relative_path).read_text(encoding="utf-8")
                findings = ruff_findings({
                    relative_path: source + "\nimport planted_unused_dependency\n",
                })
                self.assertTrue(any(
                    finding["code"] == "F401"
                    and "planted_unused_dependency" in str(finding["message"])
                    for finding in findings
                ))

    def test_one_authored_root_list_feeds_both_gates_and_excludes_tests(self) -> None:
        roots = tuple(
            line.strip()
            for line in Path("tools/production_python_sources.txt")
            .read_text(encoding="utf-8")
            .splitlines()
            if line.strip() and not line.lstrip().startswith("#")
        )
        script = Path("scripts/find_dead_code.sh").read_text(encoding="utf-8")

        self.assertEqual(roots, EXPECTED_PRODUCTION_ROOTS)
        self.assertNotIn("tests", roots)
        self.assertIn("tools/production_python_sources.txt", script)
        self.assertIn("ruff check --select F401,F811", script)
        self.assertIn('vulture "${VULTURE_ARGS[@]}" "${SOURCES[@]}"', script)


if __name__ == "__main__":
    unittest.main()
