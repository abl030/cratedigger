"""Contracts for the production-only Ruff unused-import gate."""

from __future__ import annotations

import ast
from collections import Counter
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest


EXPECTED_PRODUCTION_ROOTS = (
    "lib",
    "web",
    "harness",
    "scripts",
    "cratedigger.py",
    "album_source.py",
)
REDUNDANT_ALIAS_AUDIT_SURFACES = (
    "cratedigger.py",
    "scripts/pipeline_cli/__init__.py",
)
EXPECTED_REDUNDANT_ALIASES = {
    "cratedigger.py": frozenset(),
    "scripts/pipeline_cli/__init__.py": frozenset(),
}
REPO_ROOT = Path(__file__).resolve().parent.parent


def _write_source_world(root: Path, sources: dict[str, str]) -> Path:
    paths: list[str] = []
    for relative_path, source in sources.items():
        path = root / relative_path
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(source, encoding="utf-8")
        paths.append(path.as_posix())
    source_list = root / "production-sources.txt"
    source_list.write_text("\n".join(paths) + "\n", encoding="utf-8")
    return source_list


def ruff_findings(sources: dict[str, str]) -> tuple[dict[str, object], ...]:
    """Run the production source-local command over a synthetic world."""
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        source_list = _write_source_world(root, sources)
        env = dict(os.environ)
        env["CRATEDIGGER_RUFF_OUTPUT_FORMAT"] = "json"
        result = subprocess.run(
            ["bash", "scripts/find_unused_imports.sh", str(source_list)],
            cwd=REPO_ROOT,
            env=env,
            text=True,
            capture_output=True,
            check=False,
        )
    if result.returncode not in {0, 1}:
        raise AssertionError(result.stderr or result.stdout)
    return tuple(json.loads(result.stdout))


def run_full_dead_code_gate(
    sources: dict[str, str],
    *,
    runner_source: str | None = None,
) -> subprocess.CompletedProcess[str]:
    """Run the actual production wrapper, optionally with a planted mutant."""
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        source_list = _write_source_world(root, sources)
        runner = REPO_ROOT / "scripts/find_dead_code.sh"
        if runner_source is not None:
            runner = root / "find_dead_code.sh"
            runner.write_text(runner_source, encoding="utf-8")
        env = dict(os.environ)
        env["CRATEDIGGER_REPO_ROOT"] = str(REPO_ROOT)
        env["CRATEDIGGER_PRODUCTION_PYTHON_SOURCES_FILE"] = str(source_list)
        return subprocess.run(
            ["bash", str(runner)],
            cwd=REPO_ROOT,
            env=env,
            text=True,
            capture_output=True,
            check=False,
        )


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


def assert_dead_code_gate_rejects(result: subprocess.CompletedProcess[str]) -> None:
    """Assert the production wrapper enforces a source-local failure."""
    assert result.returncode != 0, result.stdout + result.stderr


def redundant_aliases(source: str) -> tuple[tuple[str, str], ...]:
    """Return exact redundant import-alias identities."""
    aliases: list[tuple[str, str]] = []
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.ImportFrom):
            module = "." * node.level + (node.module or "")
            aliases.extend(
                (module, alias.name)
                for alias in node.names
                if alias.asname == alias.name
            )
        elif isinstance(node, ast.Import):
            aliases.extend(
                ("", alias.name)
                for alias in node.names
                if alias.asname == alias.name
            )
    return tuple(aliases)


def assert_redundant_alias_baseline(
    source: str,
    expected: frozenset[tuple[str, str]],
) -> None:
    """Require the exact intentional redundant-alias baseline."""
    actual = redundant_aliases(source)
    actual_set = frozenset(actual)
    duplicates = sorted(
        alias for alias, occurrences in Counter(actual).items() if occurrences > 1
    )
    unexpected = actual_set - expected
    stale = expected - actual_set
    assert not unexpected and not stale and not duplicates, (
        f"unexpected redundant aliases: {sorted(unexpected)!r}; "
        f"stale expected aliases: {sorted(stale)!r}; "
        f"duplicate redundant aliases: {duplicates!r}"
    )


class TestUnusedImportAudit(unittest.TestCase):
    def test_legacy_redundant_alias_baseline_is_exact(self) -> None:
        self.assertEqual(
            set(EXPECTED_REDUNDANT_ALIASES),
            set(REDUNDANT_ALIAS_AUDIT_SURFACES),
        )
        for relative_path, expected in EXPECTED_REDUNDANT_ALIASES.items():
            with self.subTest(relative_path=relative_path):
                source = (REPO_ROOT / relative_path).read_text(encoding="utf-8")
                assert_redundant_alias_baseline(source, expected)

    def test_redundant_alias_checker_rejects_a_peer_masked_expansion(self) -> None:
        source = "from dependency import shared_name as shared_name\n"
        peer_source = "shared_name = object()\nprint(shared_name)\n"
        findings = ruff_findings({
            "lib/importing.py": source,
            "lib/peer.py": peer_source,
        })
        assert_import_liveness(
            findings,
            relative_path="lib/importing.py",
            import_is_live=True,
        )

        with self.assertRaisesRegex(AssertionError, "unexpected redundant aliases"):
            assert_redundant_alias_baseline(source, frozenset())

    def test_redundant_alias_checker_rejects_a_duplicate_identity(self) -> None:
        import_line = "from dependency import shared_name as shared_name\n"
        expected = frozenset({("dependency", "shared_name")})

        with self.assertRaisesRegex(AssertionError, "duplicate redundant aliases"):
            assert_redundant_alias_baseline(import_line * 2, expected)

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

    def test_actual_production_wrapper_rejects_cross_module_name_masking(self) -> None:
        sources = {
            "lib/importing.py": "from dependency import shared_name\n",
            "lib/peer.py": "shared_name = object()\nprint(shared_name)\n",
        }

        result = run_full_dead_code_gate(sources)

        assert_dead_code_gate_rejects(result)

    def test_checker_kills_a_non_enforcing_production_wrapper_mutant(self) -> None:
        sources = {
            "lib/importing.py": "from dependency import shared_name\n",
            "lib/peer.py": "shared_name = object()\nprint(shared_name)\n",
        }
        runner_source = Path("scripts/find_dead_code.sh").read_text(encoding="utf-8")
        enforcing_call = 'bash scripts/find_unused_imports.sh "$SOURCE_LIST"'
        self.assertIn(enforcing_call, runner_source)
        mutant = runner_source.replace(
            enforcing_call,
            enforcing_call + " || true",
            1,
        )

        result = run_full_dead_code_gate(sources, runner_source=mutant)

        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        with self.assertRaises(AssertionError):
            assert_dead_code_gate_rejects(result)

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

    def test_each_audited_surface_still_rejects_a_new_unused_import(self) -> None:
        for relative_path in REDUNDANT_ALIAS_AUDIT_SURFACES:
            with self.subTest(relative_path=relative_path):
                source = (REPO_ROOT / relative_path).read_text(encoding="utf-8")
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
        self.assertIn('bash scripts/find_unused_imports.sh "$SOURCE_LIST"', script)
        self.assertIn('vulture "${VULTURE_ARGS[@]}" "${SOURCES[@]}"', script)


if __name__ == "__main__":
    unittest.main()
