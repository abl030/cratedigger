"""Generated startup-boundary properties and known-bad self-tests."""

from __future__ import annotations

import ast
import logging
import os
import sys
import tempfile
import unittest
from collections.abc import Callable
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Literal
from unittest.mock import patch

from hypothesis import given
from hypothesis import strategies as st

import cratedigger
import tests._hypothesis_profiles  # noqa: F401
from lib.beets_config_contract import (
    BeetsAuthority,
    BeetsConfigReport,
    BeetsPluginContract,
    BeetsRole,
    ContractFinding,
)
from lib.beets_startup import BeetsStartupError, enforce_beets_startup
from lib.config import CratediggerConfig, read_runtime_config
from scripts import import_preview_worker, importer
from tests._typing_ratchet_scanner import iter_production_paths
from web import server

REPO = Path(__file__).resolve().parent.parent
ROLES: tuple[BeetsRole, ...] = ("main", "importer", "preview", "web")


class _FirstApplicationEffect(Exception):
    pass


@dataclass(frozen=True)
class _EntrypointCase:
    role: BeetsRole
    entrypoint: Callable[[], int]
    guard_path: str
    install_path: str


ENTRYPOINT_CASES: tuple[_EntrypointCase, ...] = (
    _EntrypointCase(
        "main",
        cratedigger.main,
        "cratedigger.enforce_beets_startup",
        "cratedigger.install_admitted_runtime_config",
    ),
    _EntrypointCase(
        "importer",
        importer.main,
        "scripts.importer.enforce_beets_startup",
        "scripts.importer.install_admitted_runtime_config",
    ),
    _EntrypointCase(
        "preview",
        import_preview_worker.main,
        "scripts.import_preview_worker.enforce_beets_startup",
        "scripts.import_preview_worker.install_admitted_runtime_config",
    ),
    _EntrypointCase(
        "web",
        server.main,
        "web.server.enforce_beets_startup",
        "web.server.install_admitted_runtime_config",
    ),
)

_Mutant = Literal["none", "permissive", "late", "omitted", "post_override"]


def _entrypoint_argv(
    role: BeetsRole,
    config_path: str,
    runtime_dir: str,
) -> list[str]:
    common = ["--config", config_path, "--runtime-dir", runtime_dir]
    if role == "main":
        return ["cratedigger.py", *common, "--no-lock-file"]
    if role == "importer":
        return ["importer.py", *common, "--once"]
    if role == "preview":
        return ["import_preview_worker.py", *common, "--once"]
    return [
        "server.py",
        *common,
        "--canonical-origin", "https://music.example",
        "--dev-port", "0",
    ]


def _run_real_entrypoint(
    case: _EntrypointCase,
    *,
    mutant: _Mutant = "none",
) -> tuple[str, ...]:
    """Run one actual main through a mutated guard/first-effect boundary."""
    events: list[str] = []
    prior_main_config = cratedigger.cfg
    admitted = CratediggerConfig(
        beets_config_dir="/admitted/beets",
        beets_library_db="/admitted/library.db",
        beets_directory="/admitted/music",
        beets_state_file="/admitted/state.pickle",
        beets_python=sys.executable,
        beets_secret_include="/admitted/secret.yaml",
    )
    with tempfile.TemporaryDirectory() as runtime_dir:
        config_path = os.path.join(runtime_dir, "runtime.ini")
        Path(config_path).write_text(
            "[Pipeline DB]\nenabled = true\n",
            encoding="utf-8",
        )

        def guard(**kwargs: object) -> CratediggerConfig:
            if mutant == "permissive":
                events.append("permissive_load")
                return read_runtime_config(str(kwargs["config_path"]))
            if mutant in ("late", "omitted"):
                return admitted
            events.append("check")
            if mutant == "post_override":
                return replace(
                    admitted,
                    beets_library_db="/post-check/override.db",
                )
            return admitted

        def first_effect(
            _config_path: str,
            installed: CratediggerConfig,
        ) -> None:
            if installed.beets_library_db != admitted.beets_library_db:
                events.append("authority_override")
            events.append("effect")
            if mutant == "late":
                events.append("check")
            raise _FirstApplicationEffect

        try:
            with (
                patch.object(
                    sys,
                    "argv",
                    _entrypoint_argv(case.role, config_path, runtime_dir),
                ),
                patch(case.guard_path, side_effect=guard),
                patch(case.install_path, side_effect=first_effect),
                self_raises(_FirstApplicationEffect),
            ):
                case.entrypoint()
        finally:
            cratedigger.cfg = prior_main_config
    return tuple(events)


def self_raises(exception: type[BaseException]):
    """Return an assertion context without coupling helpers to a TestCase."""
    return unittest.TestCase().assertRaises(exception)


def assert_real_entrypoint_guarded(events: tuple[str, ...]) -> None:
    if events != ("check", "effect"):
        raise AssertionError(
            "startup must check exactly once before its first effect and "
            f"preserve admitted authority; observed {events!r}"
        )


class TestGeneratedStartupBoundary(unittest.TestCase):
    @given(
        role=st.sampled_from(ROLES),
        outcome=st.sampled_from((
            "admitted",
            "warning",
            "hard",
            "load_error",
            "load_value_error",
            "check_value_error",
        )),
        code=st.text(
            alphabet=st.characters(whitelist_categories=("Ll", "Nd")),
            min_size=1,
            max_size=24,
        ),
    )
    def test_real_startup_adapter_enforces_every_generated_role_and_result(
        self,
        role: BeetsRole,
        outcome: str,
        code: str,
    ) -> None:
        cfg = CratediggerConfig()
        finding = ContractFinding(code=code, message="bounded diagnostic")
        report = BeetsConfigReport(
            ok=outcome not in ("hard", "load_error", "load_value_error"),
            role=role,
            authority=BeetsAuthority(
                config_dir="/beets",
                library="/library.db",
                directory="/music",
                state_file="/state.pickle",
                python="/python",
                secret_include="/secret.yaml",
                beets_version="2.12.0",
                beets_package="/package/beets",
            ),
            plugin_contract=BeetsPluginContract(),
            hard_failures=(finding,) if outcome == "hard" else (),
            warnings=(finding,) if outcome == "warning" else (),
            fingerprint="f" * 64,
        )
        startup_logger = logging.getLogger("test.generated-beets-startup")
        load_effect: BaseException | None = None
        if outcome == "load_error":
            load_effect = OSError("generated native load failure")
        elif outcome == "load_value_error":
            load_effect = ValueError("generated invalid runtime value")
        check_effect = (
            ValueError("generated checker programming defect")
            if outcome == "check_value_error"
            else None
        )
        with (
            patch.object(startup_logger, "disabled", True),
            patch(
                "lib.beets_startup.read_runtime_config_strict",
                side_effect=load_effect,
                return_value=cfg,
            ) as strict_load,
            patch(
                "lib.beets_startup.check_beets_config",
                side_effect=check_effect,
                return_value=report,
            ) as check,
        ):
            if outcome in ("hard", "load_error", "load_value_error"):
                with self.assertRaises(BeetsStartupError):
                    enforce_beets_startup(
                        role=role,
                        config_path="/immutable/runtime.ini",
                        runtime_dir="/mutable/state",
                        logger=startup_logger,
                    )
            elif outcome == "check_value_error":
                with self.assertRaisesRegex(
                    ValueError,
                    "checker programming defect",
                ):
                    enforce_beets_startup(
                        role=role,
                        config_path="/immutable/runtime.ini",
                        runtime_dir="/mutable/state",
                        logger=startup_logger,
                    )
            else:
                admitted = enforce_beets_startup(
                    role=role,
                    config_path="/immutable/runtime.ini",
                    runtime_dir="/mutable/state",
                    logger=startup_logger,
                )
                self.assertEqual(admitted.beets_config_dir, "/beets")
                self.assertEqual(admitted.beets_library_db, "/library.db")

        strict_load.assert_called_once_with(
            "/immutable/runtime.ini", "/mutable/state",
        )
        if outcome in ("load_error", "load_value_error"):
            check.assert_not_called()
        else:
            check.assert_called_once_with(cfg, role=role)

    @given(case=st.sampled_from(ENTRYPOINT_CASES))
    def test_every_real_entrypoint_checks_before_its_first_effect(
        self,
        case: _EntrypointCase,
    ) -> None:
        assert_real_entrypoint_guarded(_run_real_entrypoint(case))

    def test_known_bad_permissive_loader_mutant_trips(self) -> None:
        with self.assertRaisesRegex(AssertionError, "startup must check"):
            assert_real_entrypoint_guarded(
                _run_real_entrypoint(ENTRYPOINT_CASES[1], mutant="permissive")
            )

    def test_known_bad_late_check_mutant_trips(self) -> None:
        with self.assertRaisesRegex(AssertionError, "startup must check"):
            assert_real_entrypoint_guarded(
                _run_real_entrypoint(ENTRYPOINT_CASES[3], mutant="late")
            )

    def test_known_bad_omitted_entrypoint_mutant_trips(self) -> None:
        with self.assertRaisesRegex(AssertionError, "startup must check"):
            assert_real_entrypoint_guarded(
                _run_real_entrypoint(ENTRYPOINT_CASES[2], mutant="omitted")
            )

    def test_known_bad_post_check_override_mutant_trips(self) -> None:
        with self.assertRaisesRegex(AssertionError, "startup must check"):
            assert_real_entrypoint_guarded(
                _run_real_entrypoint(
                    ENTRYPOINT_CASES[0],
                    mutant="post_override",
                )
            )


def _production_python_paths() -> tuple[Path, ...]:
    return tuple(Path(path) for _relative, path in iter_production_paths())


class _CanonicalCallVisitor(ast.NodeVisitor):
    """Resolve direct, aliased, and qualified imports for contract calls."""

    def __init__(self) -> None:
        self.aliases: dict[str, str] = {}
        self.calls: list[str] = []
        self.imports: list[str] = []

    def visit_Import(self, node: ast.Import) -> None:
        for alias in node.names:
            binding = alias.asname or alias.name.split(".")[0]
            self.aliases[binding] = alias.name if alias.asname else binding
            self.imports.append(alias.name)
        self.generic_visit(node)

    def visit_ImportFrom(self, node: ast.ImportFrom) -> None:
        module = node.module or ""
        for alias in node.names:
            canonical = f"{module}.{alias.name}" if module else alias.name
            self.aliases[alias.asname or alias.name] = canonical
            self.imports.append(canonical)
        self.generic_visit(node)

    @staticmethod
    def _dotted(node: ast.expr) -> str | None:
        if isinstance(node, ast.Name):
            return node.id
        if isinstance(node, ast.Attribute):
            prefix = _CanonicalCallVisitor._dotted(node.value)
            return f"{prefix}.{node.attr}" if prefix else None
        return None

    def visit_Call(self, node: ast.Call) -> None:
        dotted = self._dotted(node.func)
        if dotted is not None:
            head, separator, tail = dotted.partition(".")
            canonical_head = self.aliases.get(head, head)
            self.calls.append(
                canonical_head + (separator + tail if separator else "")
            )
        self.generic_visit(node)


def _audit_production_calls() -> tuple[dict[str, list[str]], dict[str, list[str]]]:
    targets = (
        "lib.beets_startup.enforce_beets_startup",
        "lib.beets_config_contract.check_beets_config",
        "lib.beets_config_contract.validate_beets_config",
    )
    calls = {target: [] for target in targets}
    imports = {target: [] for target in targets}
    for path in _production_python_paths():
        relative = str(path.relative_to(REPO))
        visitor = _CanonicalCallVisitor()
        visitor.visit(ast.parse(path.read_text(encoding="utf-8"), filename=relative))
        for target in targets:
            if target in visitor.calls:
                calls[target].append(relative)
            if target in visitor.imports:
                imports[target].append(relative)
        for module_target in (
            "lib.beets_startup",
            "lib.beets_config_contract",
        ):
            if module_target not in visitor.imports:
                continue
            for target in targets:
                if target.startswith(module_target + ".") and target in visitor.calls:
                    imports[target].append(relative)
    return calls, imports


class TestStartupCheckerAstBoundary(unittest.TestCase):
    def test_only_exact_startup_and_standalone_callsites_exist(self) -> None:
        calls, imports = _audit_production_calls()
        startup = "lib.beets_startup.enforce_beets_startup"
        checker = "lib.beets_config_contract.check_beets_config"
        retired = "lib.beets_config_contract.validate_beets_config"
        startup_paths = [
            "cratedigger.py",
            "scripts/import_preview_worker.py",
            "scripts/importer.py",
            "web/server.py",
        ]
        checker_paths = [
            "lib/beets_startup.py",
            "scripts/check_beets_config.py",
        ]
        self.assertEqual(sorted(calls[startup]), startup_paths)
        self.assertEqual(sorted(set(imports[startup])), startup_paths)
        self.assertEqual(sorted(calls[checker]), checker_paths)
        self.assertEqual(sorted(set(imports[checker])), checker_paths)
        self.assertEqual(calls[retired], [])
        self.assertEqual(imports[retired], [])

    def test_web_has_no_retired_post_check_authority_flags(self) -> None:
        tree = ast.parse(
            (REPO / "web" / "server.py").read_text(encoding="utf-8"),
        )
        strings = {
            node.value
            for node in ast.walk(tree)
            if isinstance(node, ast.Constant) and isinstance(node.value, str)
        }
        self.assertNotIn("--beets-db", strings)
        self.assertNotIn("--beets-directory", strings)


if __name__ == "__main__":
    unittest.main()
