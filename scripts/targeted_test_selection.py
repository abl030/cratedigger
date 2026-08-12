"""Select explicit, adjacent, and repository-wide targeted tests."""

from __future__ import annotations

import subprocess
from collections import Counter
from collections.abc import Iterable, Sequence
from pathlib import Path, PurePosixPath

ALWAYS_AMBIENT_TESTS = (
    "tests.test_typing_ratchet",
)

PIPELINE_DB_NEIGHBOURS = (
    "tests.test_pipeline_db",
    "tests.test_fakes",
    "tests.test_pipeline_db_write_audit",
    "tests.test_read_projection_audit",
    "tests.test_pipeline_db_column_contract",
)
ROUTE_NEIGHBOURS = (
    "tests.web.test_route_audit",
    "tests.test_pydantic_route_audit",
    "tests.test_js_payload_contract_audit",
)
WEB_TEST_HARNESS_NEIGHBOURS = (
    *ROUTE_NEIGHBOURS,
    "tests.web.test_server_endpoints",
)
STRUCTURAL_AUDIT_NEIGHBOURS = (
    "tests.test_deploy_pin_script",
    "tests.test_ffmpeg_audio_map_audit",
    "tests.test_js_ast_audits",
    "tests.test_js_payload_contract_audit",
    "tests.test_js_window_bindings",
)
WORLD_MODEL_NEIGHBOURS = (
    "tests.test_world_census_seeds",
    "tests.test_world_model_burst",
    "tests.test_world_model_coordinator",
    "tests.test_parallel_test_runner",
    "tests.test_hypothesis_profile_audit",
)

EXACT_PATH_NEIGHBOURS: dict[str, tuple[str, ...]] = {
    "pyrightconfig.json": (
        "tests.test_pyright_checks",
    ),
    "pyrightconfig.production.json": (
        "tests.test_pyright_checks",
    ),
    "scripts/run_pyright_checks.py": (
        "tests.test_pyright_checks",
    ),
    "scripts/run_python_tests.py": (
        "tests.test_parallel_test_runner",
    ),
    "scripts/run_test_suite.py": (
        "tests.test_suite_coordinator",
    ),
    "scripts/run_targeted_tests.py": (
        "tests.test_targeted_test_selection",
    ),
    "scripts/targeted_test_selection.py": (
        "tests.test_targeted_test_selection",
    ),
    "scripts/test.sh": (
        "tests.test_targeted_test_selection",
    ),
    # Shared tests/ infrastructure (issue #1081): none of these are
    # discoverable test modules themselves, so each needs an explicit
    # mapping to the test(s) that actually exercise it. tests/fakes/,
    # tests/web/, tests/structural_audits/, and tests/world_model/ are
    # covered by prefix rules below instead of listed file-by-file here.
    "tests/audio_fixtures.py": (
        "tests.test_conversion_e2e",
        "tests.test_media_readiness",
    ),
    "tests/beets_config_startup_support.py": (
        "tests.test_beets_config_startup",
        "tests.test_beets_config_startup_entrypoints",
    ),
    "tests/beets_world.py": (
        "tests.test_beets_world_config",
        "tests.test_destructive_authority",
        "tests.test_harness_beets2_contract",
    ),
    "tests/conftest.py": (
        "tests.test_parallel_test_runner",
        "tests.test_pipeline_db",
    ),
    "tests/_docs_reference_audit.py": (
        "tests.test_docs_audit",
    ),
    "tests/ephemeral_slskd.py": (
        # No test drives EphemeralSlskd directly today — its only consumer
        # is the dev benchmarking script scripts/bench_parallel_search.py,
        # which has no dedicated test of its own either. Nearest existing
        # benchmarking-tooling test, kept until real coverage exists.
        "tests.test_bench_artist_cold",
    ),
    "tests/finite_domain.py": (
        "tests.test_finite_domain",
    ),
    "tests/finite_domain_metadata.py": (
        "tests.test_finite_domain",
    ),
    "tests/fixtures/build_cd_rip_proof_fixture.py": (
        "tests.test_cd_rip_js_fixture",
    ),
    "tests/harness_test_support.py": (
        "tests.test_harness_test_support",
    ),
    "tests/helpers.py": (
        "tests.test_quality_decisions",
        "tests.test_dispatch_core",
        "tests.test_integration_slices",
    ),
    "tests/_hypothesis_profiles.py": (
        "tests.test_hypothesis_profile_audit",
    ),
    "tests/__init__.py": (
        "tests.test_util",
        "tests.test_beets_config_startup",
    ),
    "tests/_lambda_audit.py": (
        "tests.test_lambda_audit",
    ),
    "tests/_mock_audit_scanner.py": (
        "tests.test_mock_audit",
    ),
    "tests/node_jsonl_worker.py": (
        "tests.test_node_jsonl_worker",
        "tests.test_generated_node_worker_audit",
    ),
    "tests/read_projection_registry.py": (
        "tests.test_pipeline_db",
        "tests.test_read_projection_audit",
    ),
    "tests/ruff_lsp_worker.py": (
        "tests.test_ruff_lsp_worker",
    ),
    "tests/_tests_typing_ratchet_baseline.py": (
        "tests.test_typing_ratchet",
    ),
    "tests/_typing_ratchet_baseline.py": (
        "tests.test_typing_ratchet",
    ),
    "tests/_typing_ratchet_scanner.py": (
        "tests.test_typing_ratchet",
    ),
}


def _module_path(module: str, repo_root: Path) -> Path:
    return repo_root.joinpath(*module.split(".")).with_suffix(".py")


def _existing_module(module: str, repo_root: Path) -> str | None:
    return module if _module_path(module, repo_root).is_file() else None


def _selector_module(selector: str, repo_root: Path) -> str | None:
    parts = selector.split(".")
    for length in range(len(parts), 0, -1):
        module = ".".join(parts[:length])
        if _module_path(module, repo_root).is_file():
            return module
    return None


def _paired_module(module: str, repo_root: Path) -> str | None:
    sibling = (
        module.removesuffix("_generated")
        if module.endswith("_generated")
        else f"{module}_generated"
    )
    return _existing_module(sibling, repo_root)


def _path_module(path: PurePosixPath) -> str | None:
    """Return the dotted module name, but only for a discoverable test module.

    A shared test-infrastructure file (basename not ``test_*.py``) is not a
    runnable unittest target — the parallel runner's ``discover_test_modules``
    only ever finds ``test*.py`` files, so emitting a selector for anything
    else produces an ``unknown test selector`` crash deep in the runner
    (issue #1081). Shared infrastructure gets its selectors from
    ``EXACT_PATH_NEIGHBOURS`` / prefix rules instead.
    """
    if path.suffix != ".py" or not path.parts:
        return None
    if not path.stem.startswith("test"):
        return None
    parts = (*path.parts[:-1], path.stem)
    if any(not part.isidentifier() for part in parts):
        return None
    return ".".join(parts)


def ambient_test_modules(repo_root: Path) -> tuple[str, ...]:
    """Discover every global audit plus explicitly named ratchets."""
    audit_modules = (
        module
        for path in sorted((repo_root / "tests").rglob("test_*_audit.py"))
        if (module := _path_module(PurePosixPath(path.relative_to(repo_root))))
        is not None
    )
    return tuple(dict.fromkeys((*ALWAYS_AMBIENT_TESTS, *audit_modules)))


def _direct_test_candidates(path: PurePosixPath) -> tuple[str, ...]:
    if path.suffix != ".py":
        return ()
    stem = path.stem
    if path.parts[:1] == ("lib",):
        return (f"tests.test_{stem}",)
    if path.parts[:1] == ("scripts",):
        return (f"tests.test_{stem}",)
    if path.parts[:1] == ("web",) and path.parts[:2] != ("web", "routes"):
        return (f"tests.test_web_{stem}", f"tests.web.test_{stem}")
    if len(path.parts) == 1:
        return (f"tests.test_{stem}",)
    return ()


def _changed_path_neighbours(
    relative_path: str,
    repo_root: Path,
) -> tuple[str, ...]:
    path = PurePosixPath(relative_path)
    neighbours: list[str] = list(EXACT_PATH_NEIGHBOURS.get(relative_path, ()))
    module = _path_module(path)
    if module is not None and module.startswith("tests."):
        neighbours.append(module)
    for candidate in _direct_test_candidates(path):
        if _existing_module(candidate, repo_root) is not None:
            neighbours.append(candidate)
    if relative_path.startswith("lib/pipeline_db/"):
        neighbours.extend(PIPELINE_DB_NEIGHBOURS)
    if relative_path.startswith("migrations/"):
        neighbours.extend((*PIPELINE_DB_NEIGHBOURS, "tests.test_migrator"))
    if relative_path.startswith("tests/fakes/"):
        neighbours.append("tests.test_fakes")
    if relative_path.startswith("tests/web/"):
        neighbours.extend(WEB_TEST_HARNESS_NEIGHBOURS)
    if relative_path.startswith("tests/structural_audits/"):
        neighbours.extend(STRUCTURAL_AUDIT_NEIGHBOURS)
    if relative_path.startswith("tests/world_model/"):
        neighbours.extend(WORLD_MODEL_NEIGHBOURS)
    if relative_path.startswith("web/routes/"):
        neighbours.extend(ROUTE_NEIGHBOURS)
        route_test = f"tests.web.test_routes_{path.stem}"
        if _existing_module(route_test, repo_root) is not None:
            neighbours.append(route_test)
    if relative_path.startswith("nix/") or relative_path in {
        "flake.nix",
        "flake.lock",
    }:
        neighbours.append("tests.test_nix_module")
    if relative_path.startswith("harness/") or relative_path == "lib/beets.py":
        neighbours.append("tests.test_harness_beets2_contract")
    if relative_path.startswith("lib/quality/"):
        neighbours.extend(
            (
                "tests.test_quality_decisions",
                "tests.test_quality_classification",
                "tests.test_quality_generated",
            )
        )
    if path.suffix == ".py" and path.parts[:1] == ("tests",) and not neighbours:
        # A non-test .py file under tests/ with no direct self-selector, no
        # EXACT_PATH_NEIGHBOURS entry, and no matching prefix rule is shared
        # test infrastructure nobody has mapped to a consuming test. Silently
        # dropping it under-selects — the more dangerous failure for a test
        # selector, since the run reports green having exercised nothing
        # relevant to the change (issue #1081). Fail closed and name the
        # file so whoever touches it adds the mapping.
        raise ValueError(
            f"unmapped shared test module: {relative_path} — add an "
            "EXACT_PATH_NEIGHBOURS entry or a prefix rule for it in "
            "scripts/targeted_test_selection.py"
        )
    return tuple(neighbours)


def _ordered_unique(values: Iterable[str]) -> tuple[str, ...]:
    return tuple(dict.fromkeys(values))


def assert_selection_complete(
    selection: Sequence[str],
    required: Sequence[str],
) -> None:
    """Fail with exact identities when selection loses or repeats a target."""
    duplicates = sorted(
        value for value, count in Counter(selection).items() if count > 1
    )
    if duplicates:
        raise AssertionError(f"duplicate targeted tests: {', '.join(duplicates)}")
    missing = sorted(set(required) - set(selection))
    if missing:
        raise AssertionError(f"missing targeted tests: {', '.join(missing)}")


def expand_test_selection(
    explicit: Sequence[str],
    *,
    changed_paths: Sequence[str],
    repo_root: Path,
) -> tuple[str, ...]:
    """Expand explicit tests with changed-path neighbours and ambient gates."""
    selected: list[str] = []
    paired_roots: list[str] = []
    for selector in explicit:
        module = _selector_module(selector, repo_root)
        if module is None:
            raise ValueError(f"unknown test selector: {selector}")
        selected.append(selector)
        paired_roots.append(module)
    for relative_path in changed_paths:
        neighbours = _changed_path_neighbours(relative_path, repo_root)
        selected.extend(neighbours)
        paired_roots.extend(neighbours)
    for module in _ordered_unique(paired_roots):
        sibling = _paired_module(module, repo_root)
        if sibling is not None:
            selected.append(sibling)
    ambient = ambient_test_modules(repo_root)
    selected.extend(ambient)
    expanded = _ordered_unique(selected)
    assert_selection_complete(expanded, ambient)
    return expanded


def _git_output(repo_root: Path, *args: str) -> tuple[str, ...]:
    completed = subprocess.run(
        ["git", *args],
        cwd=repo_root,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        check=False,
    )
    if completed.returncode != 0:
        detail = completed.stderr.strip()
        raise RuntimeError(f"git {' '.join(args)} failed: {detail}")
    return tuple(line for line in completed.stdout.splitlines() if line)


def changed_paths_from_git(
    repo_root: Path,
    *,
    base_ref: str,
) -> tuple[str, ...]:
    """Return committed, staged, unstaged, and untracked changed paths."""
    return _ordered_unique(
        (
            *_git_output(
                repo_root,
                "diff",
                "--name-only",
                "--diff-filter=ACMR",
                f"{base_ref}...HEAD",
            ),
            *_git_output(
                repo_root,
                "diff",
                "--name-only",
                "--diff-filter=ACMR",
            ),
            *_git_output(
                repo_root,
                "diff",
                "--cached",
                "--name-only",
                "--diff-filter=ACMR",
            ),
            *_git_output(repo_root, "ls-files", "--others", "--exclude-standard"),
        )
    )
