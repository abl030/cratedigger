"""Select explicit, adjacent, and repository-wide targeted tests."""

from __future__ import annotations

import subprocess
from collections import Counter
from collections.abc import Iterable, Mapping, Sequence
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
# Every tests/web/*.py module that actually imports tests/web/_harness.py
# (verified by grep, 2026-08 review round). tests.web.test_routes_health is
# the one real exception — it needs no fake DB. Do NOT pad this with
# ROUTE_NEIGHBOURS' static audits (test_pydantic_route_audit,
# test_js_payload_contract_audit): those scan web/routes/*.py, never import
# _harness, and listing them here would look like coverage without being any.
# Used as the value for both tests/web/_harness.py and tests/web/__init__.py
# in EXACT_PATH_NEIGHBOURS below — _harness.py and __init__.py are the only
# two non-test files under tests/web/, so a prefix rule buys nothing here
# and would be one more rule that defeats the self-maintaining property (a
# third non-test file added later should fail closed, not silently inherit
# this list).
WEB_TEST_HARNESS_NEIGHBOURS = (
    "tests.web.test_request_security",
    "tests.web.test_route_audit",
    "tests.web.test_routes_beets_distance",
    "tests.web.test_routes_browse",
    "tests.web.test_routes_imports",
    "tests.web.test_routes_labels",
    "tests.web.test_routes_library",
    "tests.web.test_routes_long_tail",
    "tests.web.test_routes_pipeline",
    "tests.web.test_routes_pipeline_dashboard",
    "tests.web.test_routes_pipeline_mutations",
    "tests.web.test_routes_release_identity",
    "tests.web.test_routes_search_plan",
    "tests.web.test_routes_triage",
    "tests.web.test_routes_world_audit",
    "tests.web.test_routes_youtube",
    "tests.web.test_server_cache",
    "tests.web.test_server_endpoints",
    "tests.web.test_server_threading",
    "tests.web.test_static_assets",
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
    # tests.world_model.state_machine is the ONLY target that actually
    # imports and drives support.py and (transitively) state_machine.py — the
    # five modules above only assert command strings, test-id strings, or
    # census_seeds in isolation (verified 2026-08 review round). It is
    # deliberately excluded from unittest's own test*.py discovery glob and
    # added back by complete_test_modules(); ~18s wall per selection
    # (measured) because it drives real dispatch_import_core/BeetsDB/
    # ban_source rules against a fresh ephemeral PostgreSQL + Beets. That
    # cost is the honest price of actually covering this file — a cheaper
    # neighbour set would be fake coverage. mirror_harness.py is NOT covered
    # by this list — nothing in the repo imports it (see
    # SHARED_MODULES_WITHOUT_COVERAGE).
    "tests.world_model.state_machine",
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
    # mapping to the test(s) that actually exercise it. tests/structural_audits/
    # and tests/world_model/ are covered by prefix rules below instead of
    # listed file-by-file here — tests/fakes/ and tests/web/ are NOT (a
    # per-file entry here always wins over the tests/fakes/ prefix rule,
    # which only adds tests.test_fakes on top). Sorted by path (ASCII, so
    # underscore-prefixed entries sort first).
    "tests/__init__.py": (
        # Sets the ambient BEETSDIR default + writes the suite's minimal
        # config.yaml. tests.test_util's TestBeetsSubprocessEnv exhaustively
        # patches BEETSDIR itself for every case, so it never exercises this
        # file's ambient default. These are real callers of
        # beets_subprocess_env() that rely on the inherited default without
        # patching it first.
        "tests.test_beets_validation",
        "tests.test_dispatch_from_db",
        "tests.test_beets_retag",
        "tests.test_run_beets_harness_script",
        "tests.test_beets_config_startup",
    ),
    "tests/_docs_reference_audit.py": (
        "tests.test_docs_audit",
    ),
    "tests/_hypothesis_profiles.py": (
        # test_hypothesis_profile_audit is a pure AST audit (ast, os,
        # unittest, dataclasses only) — it checks that OTHER modules import
        # this one and structurally excludes the profile module itself
        # (PROFILE_MODULE_RELPATH), so it can never exercise this module's
        # own content and does not belong in this mapping.
        # test_parallel_test_runner actually imports the module and
        # exercises assert_hypothesis_deadlines_disabled against it, so a
        # re-enabled deadline changes its observable behavior.
        # test_import_result_legacy_generated is a real, cheap @given test
        # that runs under whatever tier this module loads — a collapsed
        # max_examples changes how MANY times it runs but is not itself an
        # observable failure (HypothesisStatsRecorder never fails a run on
        # example count alone), so it does not by itself prove a max_examples
        # regression; it is kept for the deadline coverage above.
        "tests.test_parallel_test_runner",
        "tests.test_import_result_legacy_generated",
    ),
    "tests/_lambda_audit.py": (
        "tests.test_lambda_audit",
    ),
    "tests/_mock_audit_scanner.py": (
        "tests.test_mock_audit",
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
    "tests/fakes/beets_contract.py": (
        "tests.test_beets_config_startup",
        "tests.test_beets_config_startup_generated",
        "tests.test_beets_config_contract",
        "tests.test_beets_config_contract_integration",
    ),
    "tests/fakes/daily_flake_update.py": (
        "tests.test_daily_flake_update",
        "tests.test_daily_beets_tip_update",
    ),
    "tests/fakes/deploy_cycle.py": (
        "tests.test_deploy_cycle_verifier",
        "tests.test_deploy_cycle_verifier_generated",
    ),
    "tests/fakes/deploy_hold.py": (
        "tests.test_deploy_hold",
        "tests.test_deploy_hold_generated",
    ),
    "tests/fakes/deploy_pin.py": (
        "tests.test_deploy_pin_script",
        "tests.test_deploy_pin_generated",
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
        "tests.test_harness_beets2_contract",
    ),
    "tests/helpers.py": (
        "tests.test_quality_decisions",
        "tests.test_dispatch_core",
        "tests.test_integration_slices",
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
    "tests/web/__init__.py": WEB_TEST_HARNESS_NEIGHBOURS,
    "tests/web/_harness.py": WEB_TEST_HARNESS_NEIGHBOURS,
}

#: Shared tests/ modules with NO real consuming test today — an admitted,
#: named gap, not a silent under-selection. A change to one of these selects
#: only the ambient gates (_changed_path_neighbours returns early, before
#: EXACT_PATH_NEIGHBOURS or any prefix rule can contribute a lookalike
#: neighbour). _assert_no_double_registration below fails at import time if
#: a path is ever registered here AND in EXACT_PATH_NEIGHBOURS — the two
#: registries must stay disjoint, since an early return would otherwise
#: silently discard a real, hand-authored mapping for the same file. Do not
#: add an entry here to silence the fail-closed check in
#: _changed_path_neighbours — only for a module genuinely reviewed and found
#: to have no consumer. That "no consumer" claim is not merely reviewer say-
#: so: tests/test_negative_coverage_audit.py mechanically enforces it —
#: any real `import`/`from ... import ...` statement anywhere under tests/
#: naming a registered module's dotted path fails the audit, naming both
#: the registry entry and the importing file (issue #1095).
SHARED_MODULES_WITHOUT_COVERAGE: dict[str, str] = {
    "tests/ephemeral_slskd.py": (
        "No test drives EphemeralSlskd directly. Its only consumer is the "
        "dev benchmarking script scripts/bench_parallel_search.py, which "
        "has no dedicated test of its own either (2026-08 review round, "
        "issue #1081)."
    ),
    "tests/world_model/mirror_harness.py": (
        "Nothing imports mirror_harness.py — the only references are the "
        "module-name string in scripts/run_world_model_burst.py (loaded "
        "dynamically only for --engine mirror-harness) and the daily-gate "
        "phase label in tests/test_daily_flake_update.py. The daily mirror-"
        "harness burst phase is its only real driver, and that is a "
        "scheduled operational run, not a selectable unittest target "
        "(2026-08 review round, issue #1081)."
    ),
}


def _assert_no_double_registration(
    exact_path_neighbours: Mapping[str, tuple[str, ...]],
    shared_modules_without_coverage: Mapping[str, str],
) -> None:
    """A path cannot be both a real mapping and an admitted coverage gap.

    _changed_path_neighbours returns early for any path in
    SHARED_MODULES_WITHOUT_COVERAGE, before EXACT_PATH_NEIGHBOURS or any
    prefix rule runs — so a path present in BOTH registries would silently
    discard its real, hand-authored EXACT_PATH_NEIGHBOURS mapping (issue
    #1081 review round: the tree-walking pin's assertion that a registered
    path selects nothing was a tautology of the early-return condition and
    could never have caught this on its own). Fail at import time instead of
    merely detecting it later — the contradiction becomes impossible to ship.
    """
    contradictions = sorted(
        set(exact_path_neighbours) & set(shared_modules_without_coverage)
    )
    if contradictions:
        raise ValueError(
            "path(s) registered in both EXACT_PATH_NEIGHBOURS and "
            f"SHARED_MODULES_WITHOUT_COVERAGE: {', '.join(contradictions)}"
        )


_assert_no_double_registration(EXACT_PATH_NEIGHBOURS, SHARED_MODULES_WITHOUT_COVERAGE)


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
    if relative_path in SHARED_MODULES_WITHOUT_COVERAGE:
        # An admitted gap always selects nothing beyond ambient, regardless
        # of what a prefix rule below would otherwise contribute — a
        # registration must not be a lookalike neighbour set (issue #1081
        # review round: mirror_harness.py was registered but the
        # tests/world_model/ prefix rule below would still have populated
        # WORLD_MODEL_NEIGHBOURS for it).
        return ()
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
        # file so whoever touches it adds the mapping — or, if it genuinely
        # has none, registers it in SHARED_MODULES_WITHOUT_COVERAGE (that
        # registration is a reviewed admission, not something this error
        # should advertise as the easy way out).
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
