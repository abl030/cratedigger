"""Audit: every root's coverage registry stays exact (issue #1278 item 9).

`scripts/targeted_test_selection.py::ROOT_COVERAGE_RULES` is the table
behind the fail-closed selection contract: one row per repository root that
polices under-selection (`tests/`, `lib/`, `scripts/`), each naming the file
suffixes it covers, its admitted-gap registry, and the exact message an
unmapped path raises. This module replaces the two structurally identical
twin audits (`tests/test_lib_selection_coverage_audit.py` and
`tests/test_scripts_selection_coverage_audit.py`, eleven matching methods
differing essentially by the token `lib`↔`scripts`) with one parameterized
audit that derives its rows FROM that table — no root is named by hand, so
a fourth row is audited the moment it is added.

Four registry contracts, all driving the REAL resolution functions rather
than a reimplementation:

0. the table's own scope-deciding columns match anchors held OUTSIDE the
   table (`EXPECTED_SUFFIXES`, `EXPECTED_ADMITTED_SELECTS_NOTHING`, and
   `registry_name` against the object it labels) — every other contract
   here derives its rows from that table, so a column that decides scope
   cannot also be its own authority;
1. every registered path still resolves zero neighbours (else it is a STALE
   admission and must be removed);
2. every real file under a rule's root, with one of its suffixes, either
   resolves at least one neighbour or is admitted in that rule's registry,
   AND every neighbour it resolves is a runnable `tests.` module that
   exists;
3. every registry entry is well formed — a non-empty rationale, and a path
   that still exists on disk.

Contracts 1 and 2 run over `AUDITED_RULES` only — the `lib/` and
`scripts/` rows. `SHARED_MODULES_WITHOUT_COVERAGE` gets contracts 0 and 3,
and its ROW additionally drives the three known-bad self-tests that iterate
the whole table (both unmapped-path probes and the no-false-admission
probe); its own both-directions exactness lives in
`tests/test_targeted_test_selection.py`'s tree walk plus
`tests/test_negative_coverage_audit.py`.

Plus three contracts on `EXACT_PATH_NEIGHBOURS` itself, which the twin
audits could not see at all:

A. every dotted module a mapping names really exists on disk — since issue
   #1313 this also covers every fixed module a `SELECTION_RULES` row names,
   which no audit could see while those were inline literals inside two
   if-chains;
B. no entry is fully redundant with what the path would resolve WITHOUT it;
C. every entry whose deletion the fail-closed rules could NOT catch carries
   an explicit pin here.

Contract C answers the gap that motivated this module: the twins only ever
enforced "resolves ≥ 1 neighbour", which `_direct_test_candidates`' basename
probe satisfies trivially — so deleting a hand-authored entry for any file
that also has a basename-matched test module was invisible to them
(measured: deleting `scripts/test_substrate.py`'s entry survived the whole
scripts audit).

This is deliberately test infrastructure (selection machinery), so — per
`.claude/rules/code-quality.md` § "Never property-test the test machinery" —
it is a deterministic audit, no generated property.
"""

from __future__ import annotations

import contextlib
import io
import re
import unittest
from collections.abc import Mapping
from pathlib import Path, PurePosixPath

from scripts import targeted_test_selection
from scripts.targeted_test_selection import (
    ADMITTED_GAP_MESSAGE,
    ALWAYS_AMBIENT_TESTS,
    EXACT_PATH_NEIGHBOURS,
    ROOT_COVERAGE_RULES,
    SELECTION_RULES,
    RootCoverageRule,
    _assert_registries_disjoint,
    _changed_path_neighbours,
    _direct_test_candidates,
    _existing_module,
    _resolve_neighbours,
)

REPO_ROOT = Path(__file__).resolve().parents[1]

#: Every row's expected `admitted_selects_nothing`, held OUTSIDE the table
#: (issue #1278 item 9 review F1). `AUDITED_RULES` below derives its scope
#: from that same column, so without this anchor flipping the lib row to
#: True would silently drop the lib half of the seven tests that iterate
#: `AUDITED_RULES` AND regress production (a registered lib gap would
#: early-return before resolution, killing the #1199 "selects new coverage
#: immediately" property and the stderr note) — measured green before this
#: anchor existed. Keyed by root, so a new row with no entry here fails
#: with a KeyError rather than being unconstrained.
EXPECTED_ADMITTED_SELECTS_NOTHING: dict[str, bool] = {
    "tests": True,
    "lib": False,
    "scripts": False,
}

#: The other scope-deciding column, anchored the same way and for the same
#: reason: `suffixes` decides which files a row polices AT ALL, so dropping
#: `.sh` from the scripts row un-polices all sixteen shell wrappers at once
#: — none of them would fail closed again. Before this anchor that edit was
#: caught by the behaviour pin in tests/test_targeted_test_selection.py
#: and, incidentally, by contract C's pin population noticing two entries
#: (run_final_gate.sh, test.sh) had become maskable — but nothing in this
#: module named the column itself. Keyed by root, so a new row with no
#: entry here fails with a KeyError rather than being unconstrained.
EXPECTED_SUFFIXES: dict[str, tuple[str, ...]] = {
    "tests": (".py",),
    "lib": (".py",),
    "scripts": (".py", ".sh"),
}

#: The rows whose admitted gaps do NOT early-return: full resolution runs
#: first, so a registration can go stale the moment a path gains real
#: coverage, and the loud stderr line is what a registered gap emits. The
#: `tests/` row early-returns instead (issue #1081), so it has neither a
#: stale-drift risk of that shape nor a stderr line; its own tree walk and
#: unmapped-path pin live in tests/test_targeted_test_selection.py.
AUDITED_RULES: tuple[RootCoverageRule, ...] = tuple(
    rule for rule in ROOT_COVERAGE_RULES if not rule.admitted_selects_nothing
)


def shared_neighbour_sets() -> dict[str, tuple[str, ...]]:
    """Every fixed neighbour tuple the production module holds.

    Two sources, both DATA, neither hand-listed (issue #1278 item 9 review
    F7): the `*_NEIGHBOURS`-named module attributes plus
    `ALWAYS_AMBIENT_TESTS`, and every `SELECTION_RULES` row's `neighbours`.
    A new shared tuple or a new rule is covered by contract A the moment it
    is added. `EXACT_PATH_NEIGHBOURS` matches the name convention but is a
    dict, so the `isinstance` filter keeps it out — its per-path entries are
    fed to contract A separately.

    The rules half is new with issue #1313, and it closes what this
    docstring used to record as a deliberate hole: the module names spelled
    as inline literals inside `_resolve_neighbours`' own prefix rules were
    outside every audit, failing only downstream at the first selection that
    hit the rule ("unknown test selector"). As rows they are ordinary data,
    reachable by the same introspection — no source scan required, which is
    why the hole was worth closing this way rather than with a scanner.

    Still outside its reach, and correctly so: a row's `derived` templates.
    They are formatted with a changed file's stem, so there is no fixed name
    to check; `contribute` existence-checks each one at resolution time
    instead, and `explain` reports the misses.
    """
    sets: dict[str, tuple[str, ...]] = {
        "ALWAYS_AMBIENT_TESTS": ALWAYS_AMBIENT_TESTS,
    }
    for name in dir(targeted_test_selection):
        if not name.endswith("_NEIGHBOURS"):
            continue
        value = getattr(targeted_test_selection, name)
        if isinstance(value, tuple):
            sets[name] = value
    for rule in SELECTION_RULES:
        if rule.neighbours:
            sets[f"SELECTION_RULES[{rule.name}]"] = rule.neighbours
    return sets

#: Per-root subdirectory for the NESTED unmapped probe. A top-level probe
#: alone cannot distinguish the real `path.parts[:1] == (root,)` guard from
#: a narrower `len(path.parts) == 2` mutant (issue #1199 review F2), so each
#: probe goes one level deeper. Each directory is chosen to dodge every
#: `PREFIX_RULES` row (`tests/fakes/`,
#: `tests/structural_audits/`, `tests/world_model/`, `lib/pipeline_db/`,
#: `lib/quality/` all resolve neighbours unconditionally and would make the
#: probe resolve rather than raise). Keyed by root, so a new rule with no
#: entry here fails with a KeyError rather than silently skipping.
NESTED_PROBE_DIRS: dict[str, str] = {
    "tests": "_probe_dir",
    "lib": "dispatch",
    "scripts": "pipeline_cli",
}

#: The literal phrase each row's unmapped message must open with. Held
#: outside the table so a reworded row is caught here rather than agreeing
#: with itself.
UNMAPPED_MESSAGE_MARKERS: dict[str, str] = {
    "tests": "unmapped shared test module",
    "lib": "unmapped lib module",
    "scripts": "unmapped scripts module",
}

#: Per-root fabricated path for the stale-registration self-test: a path the
#: real mechanisms genuinely resolve, so registering it as a zero-neighbour
#: gap is a contradiction the checker must report. `lib/pipeline_db/` is
#: resolved by a prefix rule that fires whether or not the file exists;
#: `scripts/targeted_test_selection.py` is resolved by the basename probe.
STALE_PROBE_PATHS: dict[str, tuple[str, str]] = {
    "lib": ("lib/pipeline_db/_selection_probe.py", "tests.test_pipeline_db"),
    "scripts": (
        "scripts/targeted_test_selection.py",
        "tests.test_targeted_test_selection",
    ),
}

#: Contract C. Every EXACT_PATH_NEIGHBOURS entry whose silent deletion no
#: fail-closed rule would notice — because the path still resolves something
#: without it, or because no rule polices its root and suffix at all — maps
#: here to that entry's exact expected neighbour tuple. Measured
#: 2026-08-31 by `maskable_entry_paths` below, never hand-curated.
#:
#: What this buys: deleting one of these entries in isolation goes RED here,
#: naming the path. What it does NOT buy: deleting the entry AND its pin in
#: one edit stays review-owned — a deliberate two-place diff, exactly the
#: typing ratchet's own boundary (`.claude/rules/code-quality.md`
#: § "Typing enforcement"). The point is that single-place deletion can
#: never again be silent.
MASKABLE_ENTRY_PINS: dict[str, tuple[str, ...]] = {
    # No ROOT_COVERAGE_RULES row polices `.mjs` (the `tests/` row is
    # `.py`-only) and no basename probe can reach a `.mjs` file, so
    # deleting the shared JS harness's entry would silently stop selecting
    # the audit that enforces its idiom and the coordinator tests that own
    # its failure-marker contract (issue #1313 candidate 6).
    "tests/js_harness.mjs": (
        "tests.test_js_suite_audit",
        "tests.test_suite_coordinator",
    ),
    # No rule polices a top-level file, so nothing catches this deletion.
    "cratedigger.py": (
        "tests.test_slskd_searches",
        "tests.test_search_exec",
        "tests.test_cycle_startup",
    ),
    # The basename probe resolves tests.test_download regardless,
    # masking the loss of the harvest DB-propagation coverage (#1312).
    "lib/download.py": (
        "tests.test_download",
        "tests.test_slskd_sweep_exception_contracts",
        "tests.test_convergence_runner_generated",
    ),
    # The basename probe resolves tests.test_slskd_searches regardless,
    # masking the loss of the sweep-exception-contract module (#1312).
    "lib/slskd_searches.py": (
        "tests.test_slskd_searches",
        "tests.test_slskd_sweep_exception_contracts",
    ),
    # The basename probe resolves tests.test_measurement regardless,
    # masking the loss of the only module covering the public
    # diagnostic_from_stderr helper #1313 moved here (tests/
    # test_measurement_observability.py).
    "lib/measurement.py": (
        "tests.test_measurement",
        "tests.test_measurement_observability",
    ),
    # The harness/ prefix rule resolves tests.test_harness_beets2_contract
    # regardless (and beets_compat.py's basename probe its own era pins),
    # masking the loss of the duplicates-seam composition coverage.
    "harness/beets_compat.py": ("tests.test_harness_duplicate_lookup",),
    # The harness/ prefix rule resolves tests.test_harness_beets2_contract
    # regardless, masking the loss of the moved Discogs family.
    "harness/discogs_patches.py": (
        "tests.test_discogs_subtracks",
        "tests.test_discogs_subtracks_generated",
        "tests.test_discogs_subtracks_e2e",
        "tests.test_discogs_cover_art_fallback",
        "tests.test_discogs_cover_art_fallback_generated",
    ),
    # The harness/ prefix rule resolves tests.test_harness_beets2_contract
    # regardless, masking the loss of everything else: no basename probe
    # reaches a harness/ path, so this entry is the ONLY thing selecting
    # the import child's own stage, argv, and force coverage.
    "harness/import_one.py": (
        "tests.test_disambiguation",
        "tests.test_import_one_stages",
        "tests.test_import_one_request_generated",
        "tests.test_import_one_argparse_audit",
        "tests.test_force_import",
    ),
    # Basename probes resolve tests.test_beets_child(_generated).
    "lib/beets_child.py": (
        "tests.test_beets_delete",
        "tests.test_beets_retag",
        "tests.test_beets_tag_sync",
        "tests.test_merge_rekey",
    ),
    # The lib/quality/ prefix rule resolves the three quality-decision
    # modules, which are not what this entry names (#1278 item 8).
    "lib/quality/wire_types.py": (
        "tests.test_validation_result",
        "tests.test_beets_validation",
        "tests.test_beets_harness_session",
    ),
    # The lib/pipeline_db/ prefix rule resolves PIPELINE_DB_NEIGHBOURS.
    "lib/pipeline_db/decisions.py": (
        "tests.test_pipeline_db_decisions",
        "tests.test_pipeline_db_decisions_generated",
    ),
    # Basename probe resolves tests.test_surface_outcomes.
    "lib/surface_outcomes.py": (
        "tests.test_surface_outcomes",
        "tests.test_incomplete_mark_service",
        "tests.test_youtube_ingest_service",
        "tests.test_youtube_album_service",
        "tests.test_force_import_service",
        "tests.test_local_import_service",
        "tests.test_pipeline_cli_api_mutations",
        "tests.test_pipeline_cli_api_mutations_generated",
    ),
    # No rule polices a top-level .json file.
    "pyrightconfig.json": ("tests.test_pyright_checks",),
    "pyrightconfig.production.json": ("tests.test_pyright_checks",),
    # Basename probe resolves tests.test_test_substrate, which pins only the
    # stdlib-only import boundary — this is the entry whose deletion the
    # old scripts audit demonstrably survived.
    "scripts/test_substrate.py": (
        "tests.test_final_gate_receipt",
        "tests.test_fuzz_burst",
        "tests.test_parallel_test_runner",
        "tests.test_suite_coordinator",
        "tests.test_targeted_test_selection",
        "tests.test_test_substrate",
        "tests.test_test_tmpfs",
        "tests.test_world_model_coordinator",
    ),
    # The tests/fakes/ prefix rule resolves tests.test_fakes, which loads
    # none of these five fakes (issue #1081 review round 2).
    "tests/fakes/beets_contract.py": (
        "tests.test_beets_config_startup",
        "tests.test_beets_config_startup_generated",
        "tests.test_beets_config_contract",
        "tests.test_beets_config_contract_integration",
        "tests.test_beets_config_contract_generated",
        "tests.test_beets_config_contract_regressions_generated",
        "tests.test_beets_contract_world_lifetime",
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
    # No rule polices web/ at all, so nothing catches these deletions.
    "web/server.py": (
        "tests.web.test_server_endpoints",
        "tests.web.test_server_threading",
        "tests.web.test_server_cache",
        "tests.web.test_request_security",
        "tests.web.test_runtime",
        "tests.test_beets_config_startup",
    ),
    # The basename probe still resolves tests.web.test_runtime, masking
    # the loss of the two HTTP-boundary modules.
    "web/runtime.py": (
        "tests.web.test_runtime",
        "tests.web.test_server_threading",
        "tests.web.test_server_endpoints",
    ),
    # No rule polices web/ at all.
    "web/discogs.py": (
        "tests.test_discogs_api",
        "tests.test_discogs_api_generated",
        "tests.test_web_dev_server",
        "tests.test_discogs_artist_concurrency",
    ),
    "web/wrong_match_file_service.py": (
        "tests.test_wrong_match_file_service",
        "tests.web.test_routes_imports",
        "tests.test_path_authority_generated",
        "tests.test_protected_path_truth_generated",
        "tests.test_render_differential",
    ),
    "web/wrong_match_queue_view.py": (
        "tests.web.test_wrong_match_queue_view",
        "tests.web.test_routes_imports",
    ),
}


def stale_selection_gaps(rule: RootCoverageRule, repo_root: Path) -> list[str]:
    """One message per STALE entry in ``rule``'s registry — a path
    registered as "resolves zero neighbours" that now resolves real ones.
    Drives the REAL `_changed_path_neighbours`, never a reimplementation.
    """
    violations: list[str] = []
    for path in sorted(rule.registry):
        neighbours = _changed_path_neighbours(path, repo_root)
        if neighbours:
            violations.append(
                f"{path} is registered in {rule.registry_name} as resolving "
                f"zero test neighbours, but now resolves: "
                f"{', '.join(neighbours)}"
            )
    return violations


def fallback_only_neighbours(
    relative_path: str,
    repo_root: Path,
) -> tuple[str, ...]:
    """What ``relative_path`` resolves WITHOUT its hand-authored entry.

    Uses `_resolve_neighbours`' own kwarg-DI seam with an empty mapping, so
    the real self-selector, basename probes, and prefix rules all run — no
    patching of the production dict, and no reimplementation of them here.
    """
    return tuple(
        _resolve_neighbours(
            relative_path,
            PurePosixPath(relative_path),
            repo_root,
            exact_path_neighbours={},
        )
    )


def maskable_entry_paths(
    exact_path_neighbours: Mapping[str, tuple[str, ...]],
    repo_root: Path,
) -> set[str]:
    """Entry paths whose silent deletion no fail-closed rule would catch.

    An entry is maskable when the path still resolves something without it
    (the fallback masks the loss), or when no `ROOT_COVERAGE_RULES` row
    polices that root and suffix at all (nothing is watching in the first
    place).
    """
    maskable: set[str] = set()
    for relative_path in exact_path_neighbours:
        path = PurePosixPath(relative_path)
        policed = any(rule.covers(path) for rule in ROOT_COVERAGE_RULES)
        if fallback_only_neighbours(relative_path, repo_root) or not policed:
            maskable.add(relative_path)
    return maskable


def redundant_entry_violations(
    exact_path_neighbours: Mapping[str, tuple[str, ...]],
    repo_root: Path,
) -> list[str]:
    """One message per entry that adds nothing to its own fallback."""
    violations: list[str] = []
    for relative_path, neighbours in sorted(exact_path_neighbours.items()):
        fallback = fallback_only_neighbours(relative_path, repo_root)
        if set(neighbours) <= set(fallback):
            violations.append(
                f"{relative_path} is fully redundant: its entry "
                f"({', '.join(neighbours)}) is already resolved without it "
                f"({', '.join(fallback)}) — delete the entry"
            )
    return violations


def selector_violations(
    relative_path: str,
    neighbours: tuple[str, ...],
    repo_root: Path,
) -> list[str]:
    """One message per resolved neighbour that is not a runnable target.

    A selector must be a dotted module under `tests.` AND exist on disk —
    anything else crashes the parallel runner with `unknown test selector`
    deep inside `select_test_targets`, which is issue #1081's founding
    defect. Uses the production module's own `_existing_module`, never a
    reimplementation of its path arithmetic.
    """
    violations: list[str] = []
    for neighbour in neighbours:
        if not neighbour.startswith("tests."):
            violations.append(
                f"{relative_path} resolves {neighbour}, which is not a "
                "tests.* module and cannot be run as a selector"
            )
        elif _existing_module(neighbour, repo_root) is None:
            violations.append(
                f"{relative_path} resolves {neighbour}, which has no "
                "module file"
            )
    return violations


def missing_neighbour_modules(
    named_neighbour_sets: Mapping[str, tuple[str, ...]],
    repo_root: Path,
) -> list[str]:
    """One message per named module that does not exist on disk."""
    violations: list[str] = []
    for name, modules in sorted(named_neighbour_sets.items()):
        for module in modules:
            if _existing_module(module, repo_root) is None:
                violations.append(
                    f"{name} names {module}, which has no module file"
                )
    return violations


class TestRootCoverageTableIsWellFormed(unittest.TestCase):
    """The table's own columns, anchored outside the table.

    Every other class here derives its scope from `ROOT_COVERAGE_RULES`, so
    a column that decides scope has to be pinned against something that is
    not the table itself (issue #1278 item 9 review F1/M46).
    """

    def test_every_row_has_its_expected_early_return_behaviour(self) -> None:
        for rule in ROOT_COVERAGE_RULES:
            with self.subTest(root=rule.root):
                self.assertEqual(
                    rule.admitted_selects_nothing,
                    EXPECTED_ADMITTED_SELECTS_NOTHING[rule.root],
                    f"{rule.root}: flipping this column silently changes "
                    "which rows this module audits AND when production "
                    "honours a registered gap",
                )

    def test_every_row_polices_its_expected_suffixes(self) -> None:
        for rule in ROOT_COVERAGE_RULES:
            with self.subTest(root=rule.root):
                self.assertEqual(
                    rule.suffixes,
                    EXPECTED_SUFFIXES[rule.root],
                    f"{rule.root}: this column decides which files the row "
                    "polices at all — narrowing it silently un-polices "
                    "every file with the dropped suffix",
                )

    def test_the_audited_row_set_is_not_empty(self) -> None:
        """Floor for the derived scope. Deliberately outside every per-rule
        loop: an empty `AUDITED_RULES` makes each of those loops a no-op
        that passes, including the tree walk's own `assertTrue(files)`.
        """
        self.assertTrue(
            AUDITED_RULES,
            "no row has admitted_selects_nothing=False — every "
            "registry-exactness test in this module just became a no-op",
        )

    def test_every_row_labels_the_registry_it_actually_holds(self) -> None:
        """`registry_name` is the identity in every diagnostic this module
        and production emit, and nothing else checks it against the object
        it names (review M46). Object identity, so no registry contents are
        duplicated here.
        """
        for rule in ROOT_COVERAGE_RULES:
            with self.subTest(root=rule.root):
                self.assertIs(
                    rule.registry,
                    getattr(targeted_test_selection, rule.registry_name),
                    f"{rule.root} row is labelled {rule.registry_name} but "
                    "holds a different registry object",
                )


class TestRootCoverageRegistriesAreExact(unittest.TestCase):
    """Each audited row's registry, verified against the real resolution."""

    def test_no_registered_gap_is_stale(self) -> None:
        for rule in AUDITED_RULES:
            with self.subTest(root=rule.root):
                violations = stale_selection_gaps(rule, REPO_ROOT)
                self.assertEqual(
                    violations,
                    [],
                    f"A path registered in {rule.registry_name} as a "
                    "zero-neighbour gap now resolves real coverage — remove "
                    "the stale registration:\n  " + "\n  ".join(violations),
                )

    def test_every_registry_has_something_real_to_check(self) -> None:
        for rule in AUDITED_RULES:
            with self.subTest(root=rule.root):
                self.assertTrue(rule.registry, rule.registry_name)

    def test_every_policed_file_resolves_or_is_registered(self) -> None:
        """Tree-walking pin: every real file under an audited root, with one
        of that row's suffixes, either resolves at least one neighbour or is
        admitted in its registry, AND every neighbour it does resolve is a
        runnable test module. Calls the REAL `_changed_path_neighbours` for
        every file — an unregistered zero-neighbour file raises `ValueError`
        naming itself, which `subTest` reports as that file's own failure
        without stopping the walk.

        The second half is what the deleted twins lacked (review M48): they
        threw the resolved tuple away, so deleting the
        `module.startswith("tests.")` self-selector guard (in
        `resolve_attributed_neighbours` since #1313) — which makes
        `scripts/test_substrate.py` emit the bogus selector
        `scripts.test_substrate`, the exact #1081 founding defect of an
        unrunnable selector reaching the runner — left every test green.
        `selector_violations` checks it without the runner's expensive
        discovery subprocess; the tests/-side walk in
        tests/test_targeted_test_selection.py keeps the heavier
        `select_test_targets` proof.
        """
        for rule in AUDITED_RULES:
            for suffix in rule.suffixes:
                files = sorted(
                    path
                    for path in (REPO_ROOT / rule.root).rglob(f"*{suffix}")
                    if "__pycache__" not in path.parts
                )
                with self.subTest(root=rule.root, suffix=suffix):
                    self.assertTrue(
                        files,
                        f"expected {rule.root}/**/*{suffix} files to exist",
                    )
                for path in files:
                    relative = path.relative_to(REPO_ROOT).as_posix()
                    with self.subTest(path=relative):
                        neighbours = _changed_path_neighbours(
                            relative, REPO_ROOT
                        )
                        violations = selector_violations(
                            relative, neighbours, REPO_ROOT
                        )
                        self.assertEqual(
                            violations, [], "\n  ".join(violations)
                        )


class TestEveryRegistryEntryIsWellFormed(unittest.TestCase):
    """Hygiene across ALL THREE registries, including the early-returning
    `tests/` one — a registration without a reason is indistinguishable
    from a lazy bypass (#1081 review round 2, MUST FIX 6 part 2), and a
    path deleted out from under an entry leaves a dead key nothing else
    catches.
    """

    def test_every_registered_path_carries_a_non_empty_rationale(self) -> None:
        for rule in ROOT_COVERAGE_RULES:
            for path, rationale in rule.registry.items():
                with self.subTest(registry=rule.registry_name, path=path):
                    self.assertTrue(
                        rationale.strip(),
                        f"{path} is registered with an empty rationale",
                    )

    def test_every_registered_path_still_exists_on_disk(self) -> None:
        for rule in ROOT_COVERAGE_RULES:
            for path in rule.registry:
                with self.subTest(registry=rule.registry_name, path=path):
                    self.assertTrue(
                        (REPO_ROOT / path).is_file(),
                        f"registered gap does not exist on disk: {path}",
                    )


class TestExactPathNeighbourMappings(unittest.TestCase):
    """Contracts A and B on `EXACT_PATH_NEIGHBOURS` itself."""

    def test_every_named_neighbour_module_exists(self) -> None:
        """Contract A: a mapping that names a module nobody wrote resolves
        to nothing at run time — the entry looks like coverage and selects
        none. Covers the per-path entries plus every shared tuple the
        production module exports, derived by introspection rather than
        hand-listed (see `shared_neighbour_sets`, which also records what
        stays outside this contract).
        """
        violations = missing_neighbour_modules(
            {**EXACT_PATH_NEIGHBOURS, **shared_neighbour_sets()}, REPO_ROOT
        )
        self.assertEqual(violations, [], "\n  ".join(violations))

    def test_the_shared_tuple_sweep_is_not_vacuous(self) -> None:
        """The introspection above is only as good as what it finds: an
        empty or collapsed sweep would make contract A silently narrower
        than its docstring claims. Both halves are checked, because either
        can collapse independently — dropping the `SELECTION_RULES` loop
        would restore the exact inline-literal hole issue #1313 closed, and
        every other test here would stay green.
        """
        sets = shared_neighbour_sets()

        self.assertIn("ALWAYS_AMBIENT_TESTS", sets)
        self.assertIn("WEB_TEST_HARNESS_NEIGHBOURS", sets)
        self.assertNotIn("EXACT_PATH_NEIGHBOURS", sets)
        self.assertIn("SELECTION_RULES[prefix:lib/quality/]", sets)
        self.assertEqual(
            len([name for name in sets if name.startswith("SELECTION_RULES[")]),
            len([rule for rule in SELECTION_RULES if rule.neighbours]),
        )
        self.assertTrue(all(sets.values()), sets)

    def test_no_entry_is_fully_redundant_with_its_own_fallback(self) -> None:
        """Contract B: an entry whose modules the path already resolves
        without it is stale data — it survives every other check while
        teaching a reader that the mapping is load-bearing when it is not.

        Deliberately ambient-BLIND (issue #1278 item 9 review F5, orchestrator
        ruling). `scripts/find_dead_code.sh` and `scripts/run_ruff.sh` name
        `tests.test_unused_import_audit`, which `ambient_test_modules`
        already appends to every selection — so those two entries add no
        marginal selection at all. They are kept, and permitted here,
        because their real job is to suppress the fail-closed raise with a
        TRUTHFUL pointer at the module that genuinely runs both wrappers as
        bash subprocesses; a pseudo-gap admission in
        `SCRIPTS_MODULES_WITHOUT_SELECTION_COVERAGE` would claim "no
        coverage", which is false. Widening the fallback here to
        `fallback ∪ ambient` would force deleting eight truthful entries,
        six of them pre-existing `tests/_*` ones, for no selection change.
        """
        violations = redundant_entry_violations(EXACT_PATH_NEIGHBOURS, REPO_ROOT)
        self.assertEqual(violations, [], "\n  ".join(violations))


class TestMaskableEntryPins(unittest.TestCase):
    """Contract C: an entry no fail-closed rule protects carries a pin."""

    def test_pin_keys_are_exactly_the_measured_maskable_set(self) -> None:
        measured = maskable_entry_paths(EXACT_PATH_NEIGHBOURS, REPO_ROOT)
        pinned = set(MASKABLE_ENTRY_PINS)

        unpinned = sorted(measured - pinned)
        self.assertEqual(
            unpinned,
            [],
            "EXACT_PATH_NEIGHBOURS entries whose deletion nothing would "
            "catch — add a MASKABLE_ENTRY_PINS pin for each: "
            + ", ".join(unpinned),
        )
        stale = sorted(pinned - measured)
        self.assertEqual(
            stale,
            [],
            "MASKABLE_ENTRY_PINS pins a path a fail-closed rule now "
            "protects on its own — remove the stale pin: " + ", ".join(stale),
        )

    def test_every_pin_matches_the_live_entry(self) -> None:
        """The assertion ordinary maintenance hits first: adding a neighbour
        to a pinned entry fails here until the pin is updated too.
        """
        for path, expected in MASKABLE_ENTRY_PINS.items():
            with self.subTest(path=path):
                self.assertEqual(
                    EXACT_PATH_NEIGHBOURS.get(path),
                    expected,
                    f"{path}'s EXACT_PATH_NEIGHBOURS entry and its "
                    "MASKABLE_ENTRY_PINS pin disagree. No fail-closed rule "
                    "protects this entry, so the pin is what makes a "
                    "deletion visible — changing the entry is a deliberate "
                    "two-place edit: update the pin in this file to match.",
                )

    def test_live_resolution_covers_every_pinned_neighbour(self) -> None:
        """Drives the REAL resolution, not the mapping: a change that stops
        consulting `EXACT_PATH_NEIGHBOURS` (or drops a path's entry) fails
        here even though the pin above still matches the dict.
        """
        for path, expected in MASKABLE_ENTRY_PINS.items():
            with self.subTest(path=path):
                resolved = set(_changed_path_neighbours(path, REPO_ROOT))
                self.assertTrue(
                    set(expected) <= resolved,
                    f"{path} no longer resolves {sorted(set(expected) - resolved)}",
                )


class TestSelectionCoverageCheckersTripOnViolations(unittest.TestCase):
    """Known-bad self-tests, one per clause.

    The unmapped-path, stale-registration, redundancy, missing-module and
    maskability probes drive entirely synthetic paths and registries never
    present in the real module-level data. The three must-still-work
    controls (`admitted gap logs`, `registered gap does not raise`, `stale
    checker quiet on a genuine gap`) deliberately drive each audited row's
    real registry — they prove a real admitted gap behaves correctly, and
    depend on today's registries holding a genuine, non-stale gap, which
    `TestRootCoverageRegistriesAreExact` independently proves.
    """

    def test_unmapped_top_level_path_fails_closed_with_its_own_message(
        self,
    ) -> None:
        """Every row, every suffix it polices: an unmapped path raises with
        THAT row's message. Building the expectation from the row the probe
        belongs to is what catches two rows' messages being swapped. The
        probe need not exist on disk — `_changed_path_neighbours` never
        stats its own target, only candidate test modules.
        """
        for rule in ROOT_COVERAGE_RULES:
            marker = UNMAPPED_MESSAGE_MARKERS[rule.root]
            self.assertTrue(rule.unmapped_message.startswith(marker))
            for suffix in rule.suffixes:
                probe = f"{rule.root}/_totally_unmapped_probe{suffix}"
                expected = rule.unmapped_message.format(path=probe)
                with (
                    self.subTest(root=rule.root, suffix=suffix),
                    self.assertRaisesRegex(ValueError, re.escape(expected)),
                ):
                    _changed_path_neighbours(probe, REPO_ROOT)

    def test_unmapped_nested_path_fails_closed_with_its_name(self) -> None:
        """The top-level probe above cannot distinguish the real
        `path.parts[:1] == (root,)` guard from a narrower
        `len(path.parts) == 2` mutant (issue #1199 review F2). Each probe
        here is nested one level deeper, in a directory no prefix rule
        covers, so only the real first-component guard makes it raise.
        """
        for rule in ROOT_COVERAGE_RULES:
            nested_dir = NESTED_PROBE_DIRS[rule.root]
            for suffix in rule.suffixes:
                probe = (
                    f"{rule.root}/{nested_dir}/_unmapped_nested_probe{suffix}"
                )
                with (
                    self.subTest(path=probe),
                    self.assertRaisesRegex(ValueError, re.escape(probe)),
                ):
                    _changed_path_neighbours(probe, REPO_ROOT)

    def test_admitted_gap_log_names_the_path_and_its_rationale(self) -> None:
        """The loud stderr line must name BOTH the registered path AND its
        registered rationale — not merely print SOME line.
        """
        for rule in AUDITED_RULES:
            registered = next(iter(rule.registry))
            rationale = rule.registry[registered]
            buffer = io.StringIO()

            with self.subTest(root=rule.root, path=registered):
                with contextlib.redirect_stderr(buffer):
                    result = _changed_path_neighbours(registered, REPO_ROOT)

                self.assertEqual(result, ())
                emitted = buffer.getvalue()
                self.assertIn(registered, emitted)
                self.assertIn(rationale, emitted)

    def test_registered_gap_with_zero_neighbours_does_not_raise(self) -> None:
        """Must-still-work: a genuinely zero-neighbour path that IS
        registered proceeds (ambient-only selection), never raises.
        """
        for rule in AUDITED_RULES:
            registered = next(iter(rule.registry))
            with self.subTest(root=rule.root, path=registered):
                self.assertEqual(
                    _changed_path_neighbours(registered, REPO_ROOT), ()
                )

    def test_stale_registration_checker_trips_on_a_planted_stale_entry(
        self,
    ) -> None:
        """A fabricated registry entry naming a path the real mechanisms DO
        resolve must be reported as stale, naming the path, the registry,
        and what it now resolves to. The real registries are never touched.
        """
        for rule in AUDITED_RULES:
            probe, expected_module = STALE_PROBE_PATHS[rule.root]
            fabricated = RootCoverageRule(
                root=rule.root,
                suffixes=rule.suffixes,
                registry={probe: "synthetic stale entry for self-test"},
                registry_name=rule.registry_name,
                admitted_selects_nothing=rule.admitted_selects_nothing,
                unmapped_message=rule.unmapped_message,
            )

            with self.subTest(root=rule.root):
                violations = stale_selection_gaps(fabricated, REPO_ROOT)

                self.assertEqual(len(violations), 1)
                self.assertIn(probe, violations[0])
                self.assertIn(rule.registry_name, violations[0])
                self.assertIn(expected_module, violations[0])

    def test_stale_registration_checker_is_quiet_on_a_genuine_gap(self) -> None:
        """Must-still-work: a fabricated registry entry that genuinely
        resolves zero neighbours produces no violation.
        """
        for rule in AUDITED_RULES:
            registered = next(iter(rule.registry))
            fabricated = RootCoverageRule(
                root=rule.root,
                suffixes=rule.suffixes,
                registry={registered: "synthetic rationale for self-test"},
                registry_name=rule.registry_name,
                admitted_selects_nothing=rule.admitted_selects_nothing,
                unmapped_message=rule.unmapped_message,
            )

            with self.subTest(root=rule.root, path=registered):
                self.assertEqual(stale_selection_gaps(fabricated, REPO_ROOT), [])

    def test_missing_module_checker_names_the_mapping_and_the_module(
        self,
    ) -> None:
        """Contract A trips on a fabricated mapping naming a module that
        does not exist, and stays quiet on one that does.
        """
        violations = missing_neighbour_modules(
            {"_probe": ("tests.test_no_such_module_anywhere",)}, REPO_ROOT
        )

        self.assertEqual(len(violations), 1)
        self.assertIn("_probe", violations[0])
        self.assertIn("tests.test_no_such_module_anywhere", violations[0])
        self.assertEqual(
            missing_neighbour_modules(
                {"_probe": ("tests.test_targeted_test_selection",)}, REPO_ROOT
            ),
            [],
        )

    def test_redundancy_checker_names_a_fully_masked_entry(self) -> None:
        """Contract B trips on a fabricated entry whose modules the path's
        own prefix rule already resolves, and stays quiet on one that adds
        something the fallback does not.
        """
        masked = redundant_entry_violations(
            {"lib/pipeline_db/_probe.py": ("tests.test_pipeline_db",)},
            REPO_ROOT,
        )

        self.assertEqual(len(masked), 1)
        self.assertIn("lib/pipeline_db/_probe.py", masked[0])
        self.assertIn("fully redundant", masked[0])
        self.assertEqual(
            redundant_entry_violations(
                {
                    "lib/pipeline_db/_probe.py": (
                        "tests.test_targeted_test_selection",
                    )
                },
                REPO_ROOT,
            ),
            [],
        )

    def test_no_admitted_gap_line_is_printed_for_an_unmapped_path(
        self,
    ) -> None:
        """The stderr note claims a REVIEWED admission. A path that is not
        registered must never get one (review M42): a mutant printing it
        with a fabricated rationale right before the raise was otherwise
        green — operator-facing copy asserting an admission nobody made.

        Iterates every suffix each row polices, like the two sibling
        unmapped-path pins. Probing only `suffixes[0]` left a narrower
        mutant alive: one gating that fabricated note on
        `relative_path.endswith(".sh")` survived the whole suite, because
        the scripts row's first suffix is `.py`.
        """
        marker = "admitted selection gap"
        self.assertIn(marker, ADMITTED_GAP_MESSAGE)

        for rule in ROOT_COVERAGE_RULES:
            for suffix in rule.suffixes:
                probe = f"{rule.root}/_unregistered_probe{suffix}"
                buffer = io.StringIO()
                with self.subTest(root=rule.root, suffix=suffix):
                    with (
                        contextlib.redirect_stderr(buffer),
                        self.assertRaises(ValueError),
                    ):
                        _changed_path_neighbours(probe, REPO_ROOT)
                    self.assertNotIn(marker, buffer.getvalue())

    def test_shell_probe_is_scoped_to_the_scripts_root(self) -> None:
        """The `.sh` basename probe is deliberately scripts-only, and that
        scoping was prose-only (review M26): widening it to every root is
        latent today — harness/run_beets_harness.sh and three docs/research
        wrappers have no matching test module — so a widened probe would
        resolve nothing and stay green until one is added.
        """
        self.assertEqual(
            _direct_test_candidates(
                PurePosixPath("harness/run_beets_harness.sh")
            ),
            (),
        )
        self.assertEqual(
            _direct_test_candidates(PurePosixPath("scripts/fuzz_burst.sh")),
            ("tests.test_fuzz_burst",),
        )

    def test_double_registration_guard_reaches_every_row(self) -> None:
        """`_assert_registries_disjoint` must run for the WHOLE table, not
        just its first row (review M24): truncating the real import-time
        loop to `ROOT_COVERAGE_RULES[:1]` was green. The fabricated table
        puts its contradiction in the LAST row, so only a full sweep raises.
        """
        contradiction = {"lib/_double_registered.py": ("tests.test_fakes",)}
        clean_rule = RootCoverageRule(
            root="tests",
            suffixes=(".py",),
            registry={},
            registry_name="CLEAN_PROBE_REGISTRY",
            admitted_selects_nothing=True,
            unmapped_message="unmapped: {path}",
        )
        last_rule = RootCoverageRule(
            root="lib",
            suffixes=(".py",),
            registry={"lib/_double_registered.py": "synthetic gap"},
            registry_name="LAST_ROW_PROBE_REGISTRY",
            admitted_selects_nothing=False,
            unmapped_message="unmapped: {path}",
        )

        with self.assertRaisesRegex(ValueError, "LAST_ROW_PROBE_REGISTRY"):
            _assert_registries_disjoint(
                (clean_rule, clean_rule, last_rule), contradiction
            )

        _assert_registries_disjoint((clean_rule, clean_rule), contradiction)

    def test_selector_checker_rejects_a_non_test_or_missing_selector(
        self,
    ) -> None:
        """Both clauses of `selector_violations`, each with its own
        message: a selector outside `tests.` (the #1081 founding defect
        shape, `scripts.test_substrate`) and one that is dotted correctly
        but names no file. A real, runnable selector produces nothing.
        """
        outside = selector_violations(
            "scripts/test_substrate.py", ("scripts.test_substrate",), REPO_ROOT
        )
        self.assertEqual(len(outside), 1)
        self.assertIn("scripts.test_substrate", outside[0])
        self.assertIn("not a tests.* module", outside[0])

        missing = selector_violations(
            "lib/probe.py", ("tests.test_no_such_module_anywhere",), REPO_ROOT
        )
        self.assertEqual(len(missing), 1)
        self.assertIn("has no module file", missing[0])

        self.assertEqual(
            selector_violations(
                "lib/probe.py", ("tests.test_targeted_test_selection",), REPO_ROOT
            ),
            [],
        )

    def test_maskability_checker_separates_masked_from_policed_entries(
        self,
    ) -> None:
        """Contract C's own measurement: a path a rule polices whose
        fallback is empty is NOT maskable (its deletion raises), while an
        otherwise identical path with a non-empty fallback, and one no rule
        polices at all, both are.
        """
        fabricated = {
            # Policed by the scripts/ row, resolves nothing without its
            # entry — deleting it fails closed, so it needs no pin.
            "scripts/_policed_probe.py": ("tests.test_pipeline_db",),
            # Policed, but the lib/pipeline_db/ prefix rule keeps resolving.
            "lib/pipeline_db/_masked_probe.py": ("tests.test_pipeline_db",),
            # No rule polices web/ at all.
            "web/_unpoliced_probe.py": ("tests.test_pipeline_db",),
        }

        self.assertEqual(
            maskable_entry_paths(fabricated, REPO_ROOT),
            {"lib/pipeline_db/_masked_probe.py", "web/_unpoliced_probe.py"},
        )


if __name__ == "__main__":
    unittest.main()
