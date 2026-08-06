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
    if path.suffix != ".py" or not path.parts:
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
