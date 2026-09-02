"""Audit: every root's coverage registry stays exact (issue #1278 item 9).

`scripts/targeted_test_selection.py::ROOT_COVERAGE_RULES` is the table
behind the fail-closed selection contract: one row per repository root that
polices under-selection, each naming the file suffixes it covers, its
admitted-gap registry, and the exact message an unmapped path raises.
Started with three rows (`tests/`, `lib/`, `scripts/`); issue #1355 item 8
carried the same contract to every remaining production root —
`migrations/`, `nix/`, `web/`, `harness/`, and the top level — for eight
rows total. This module replaces the two structurally identical
twin audits (`tests/test_lib_selection_coverage_audit.py` and
`tests/test_scripts_selection_coverage_audit.py`, eleven matching methods
differing essentially by the token `lib`↔`scripts`) with one parameterized
audit that derives its rows FROM that table — no root is named by hand, so
a ninth row is audited the moment it is added.

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

Contracts 1 and 2 run over `AUDITED_RULES` only — every row except `tests/`
(the one row whose registry early-returns; see `EXPECTED_ADMITTED_SELECTS_
NOTHING`). `SHARED_MODULES_WITHOUT_COVERAGE` gets contracts 0 and 3,
and its ROW additionally drives the three known-bad self-tests that iterate
the whole table (both unmapped-path probes and the no-false-admission
probe); its own both-directions exactness lives in
`tests/test_targeted_test_selection.py`'s tree walk plus
`tests/test_negative_coverage_audit.py`.

Plus four contracts on the hand-authored selection data itself, which the
twin audits could not see at all:

A. every dotted module a mapping names really exists on disk — since issue
   #1313 this also covers every fixed module a `SELECTION_RULES` row names,
   which no audit could see while those were inline literals inside two
   if-chains;
B. no entry is fully redundant with what the path would resolve WITHOUT it;
C. every entry whose deletion the fail-closed rules could NOT catch carries
   an explicit pin here;
D. so does every `SELECTION_RULES` row, measured the same way.

Contract C answers the gap that motivated this module: the twins only ever
enforced "resolves ≥ 1 neighbour", which `_direct_test_candidates`' basename
probe satisfies trivially — so deleting a hand-authored entry for any file
that also has a basename-matched test module was invisible to them
(measured: deleting `scripts/test_substrate.py`'s entry survived the whole
scripts audit).

Contract D is C at the other granularity, and it closes issue #1331's first
residual: a rule row is data exactly as an entry is, and deleting one was
silent wherever nothing raised in consequence. Before issue #1355 item 8,
`migrations/`, `nix/`, `web/`, `harness/` and the top level had no
`ROOT_COVERAGE_RULES` row at all, so nothing could raise there regardless of
what a row's deletion cost — though "nothing raises" was never the same as
"every matched file goes quiet": `prefix:harness/` also matches
`lib/beets.py` through its `exact_paths`, and that one already failed
closed on the pre-existing lib/ row. Item 8 gave those five roots a row too,
which caught most — but not all — of that population: `basename:web/*.py`,
`basename:<top-level>.py`, `prefix:migrations/`, and `prefix:web/routes/`
are now fully caught (removed from `MASKABLE_RULE_PINS` entirely), while
`prefix:nix/` and `prefix:harness/` still silently lose SOME files — the
ones that carry their own hand-authored `EXACT_PATH_NEIGHBOURS` entry (which
keeps resolving something even with the row gone) or, for `nix/`, the two
top-level `flake.nix`/`flake.lock` files its `exact_paths` also matches,
which the `nix/` root row's own `covers()` cannot reach (they are not UNDER
`nix/`) and the `<top-level>` row does not either (it polices only `.py`).
Over a policed root the loss is silent at the files
something else still resolves for, whether that is a basename probe, another
prefix rule, or a hand-authored entry.

Where C asks "what does this path resolve without its entry", D asks "what
does this row's every file resolve without the row", through the same kind of
DI seam and the real fail-closed contract rather than a reimplementation of
either.

This is deliberately test infrastructure (selection machinery), so — per
`.claude/rules/code-quality.md` § "Never property-test the test machinery" —
it is a deterministic audit, no generated property.
"""

from __future__ import annotations

import contextlib
import dataclasses
import functools
import io
import re
import unittest
from collections.abc import Mapping, Sequence
from pathlib import Path, PurePosixPath

from scripts import targeted_test_selection
from scripts.targeted_test_selection import (
    ADMITTED_GAP_MESSAGE,
    ALWAYS_AMBIENT_TESTS,
    BASENAME_RULES,
    EXACT_PATH_NEIGHBOURS,
    PREFIX_RULES,
    ROOT_COVERAGE_RULES,
    SELECTION_RULES,
    RootCoverageRule,
    SelectionRule,
    _assert_registries_disjoint,
    _changed_path_neighbours,
    _direct_test_candidates,
    _existing_module,
    _resolve_neighbours,
)

REPO_ROOT = Path(__file__).resolve().parents[1]

#: What `lost_channels` calls a row's fixed `neighbours` tuple. Its derived
#: templates are their own channels, named by the template itself.
NEIGHBOURS_CHANNEL = "neighbours"

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
    "migrations": False,
    "nix": False,
    "web": False,
    "harness": False,
    "<top-level>": False,
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
    "migrations": (".sql",),
    "nix": (".nix", ".json"),
    "web": (".py",),
    "harness": (".py", ".sh"),
    "<top-level>": (".py",),
}

#: Whether an audited row's registry may legitimately hold zero entries
#: (issue #1355 item 8). `migrations/`, `nix/`, and `harness/` are each
#: matched by an unconditional prefix rule with no suffix filter — every
#: real or synthetic path under those roots resolves real neighbours today
#: (measured 2026-09-02), so there is no genuine gap to register, and
#: fabricating one to satisfy an "always non-empty" rule would be a false
#: admission. `<top-level>` is empty for a different, ordinary reason: both
#: currently-tracked top-level `.py` files already resolve. `lib`,
#: `scripts`, and `web` are expected non-empty because each names at least
#: one real, currently-unresolved file. Anchored outside `ROOT_COVERAGE_
#: RULES` for the same reason as the two columns above: a row silently
#: emptied of its only real entry must be caught here, not waved through by
#: its own now-empty registry agreeing with itself.
EXPECTED_REGISTRY_MAY_BE_EMPTY: dict[str, bool] = {
    "lib": False,
    "scripts": False,
    "migrations": True,
    "nix": True,
    "web": False,
    "harness": True,
    "<top-level>": True,
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
#: `PREFIX_RULES` row under that root (`tests/fakes/`,
#: `tests/structural_audits/`, `tests/world_model/`, `lib/pipeline_db/`,
#: `lib/quality/`, `scripts/phase_parsers/`, `web/routes/` all resolve
#: neighbours unconditionally and would make the probe resolve rather than
#: raise). Keyed by root, so a new rule with no
#: entry here fails with a KeyError rather than silently skipping — except
#: `UNCONDITIONALLY_SHADOWED_ROOTS` and `top_level` rows, which
#: `test_unmapped_nested_path_fails_closed_with_its_name` skips before ever
#: indexing this dict.
NESTED_PROBE_DIRS: dict[str, str] = {
    "tests": "_probe_dir",
    "lib": "dispatch",
    "scripts": "pipeline_cli",
    "web": "_probe_dir",
}

#: The literal phrase each row's unmapped message must open with. Held
#: outside the table so a reworded row is caught here rather than agreeing
#: with itself.
UNMAPPED_MESSAGE_MARKERS: dict[str, str] = {
    "tests": "unmapped shared test module",
    "lib": "unmapped lib module",
    "scripts": "unmapped scripts module",
    "migrations": "unmapped migration",
    "nix": "unmapped nix module",
    "web": "unmapped web module",
    "harness": "unmapped harness module",
    "<top-level>": "unmapped top-level module",
}

#: Roots whose ROOT_COVERAGE_RULES row is shadowed by an UNCONDITIONAL
#: SELECTION_RULES prefix rule (no suffix filter, fixed `neighbours`) —
#: migrations/, nix/, and harness/ (issue #1355 item 8). Every real or
#: synthetic path under these roots resolves through the named prefix rule
#: today, so no probe — nested or not — can make the root row itself raise
#: while that rule stays in SELECTION_RULES. The two generic probe tests
#: below skip these roots for exactly that reason;
#: `test_root_rule_catches_an_unconditionally_shadowed_root_if_its_prefix_rule_is_removed`
#: proves their real protective purpose instead, through the same `without=`
#: DI seam `TestMaskableRulePins` uses to measure a row's deletion.
UNCONDITIONALLY_SHADOWED_ROOTS: dict[str, str] = {
    "migrations": "prefix:migrations/",
    "nix": "prefix:nix/",
    "harness": "prefix:harness/",
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
    "migrations": ("migrations/001_initial.sql", "tests.test_migrator"),
    "nix": ("nix/module.nix", "tests.test_nix_module"),
    "web": ("web/cache.py", "tests.test_web_cache"),
    "harness": ("harness/import_one.py", "tests.test_harness_beets2_contract"),
    "<top-level>": ("album_source.py", "tests.test_album_source"),
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
    # Basename probes resolve tests.test_current_library_evidence and its
    # generated sibling, masking the loss of the other nine importers.
    "lib/current_library_evidence.py": (
        "tests.test_current_library_evidence",
        "tests.test_current_library_evidence_generated",
        "tests.test_download",
        "tests.test_import_preview",
        "tests.test_import_queue",
        "tests.test_preview_failure_evidence_generated",
        "tests.test_quality_evidence_fingerprint",
        "tests.test_quality_evidence_fingerprint_generated",
        "tests.test_quality_lineage_generated",
        "tests.test_spectral_attempt_audit_generated",
        "tests.test_terminal_outcome_callers",
    ),
    # The basename probe resolves tests.test_quality_gate, masking the loss
    # of the dispatch-composition and end-to-end integration coverage (#1321).
    "lib/dispatch/quality_gate.py": (
        "tests.test_quality_gate",
        "tests.test_import_dispatch",
        "tests.test_integration_slices",
    ),
    # The dedicated basename candidate would mask deletion of the seven
    # established dispatch/integration/authority neighbours (#1321).
    "lib/dispatch/evidence_gate.py": (
        "tests.test_evidence_gate",
        "tests.test_dispatch_core",
        "tests.test_dispatch_from_db",
        "tests.test_import_dispatch",
        "tests.test_import_queue",
        "tests.test_integration_slices",
        "tests.test_sidecar_service",
        "tests.test_current_evidence_authority_generated",
    ),
    # The dedicated basename candidate masks deletion of the wider outcome
    # writer/caller/world-model selection established by #1321.
    "lib/dispatch/outcome_actions.py": (
        "tests.test_outcome_actions",
        "tests.test_outcome_actions_generated",
        "tests.test_do_mark",
        "tests.test_import_dispatch",
        "tests.test_integration_slices",
        "tests.test_terminal_outcome_callers",
        "tests.test_album_source",
        "tests.test_importer_job_kinds",
        "tests.test_import_manifest",
        "tests.test_dispatch_from_db",
        "tests.test_dispatch_core",
        "tests.test_dispatch_outcomes_generated",
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
    # none of the fakes below (issue #1081 review round 2).
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
    # Same prefix rule, one layer up: it resolves tests.test_fakes and the
    # derived tests.test_fakes_subprocess_env, masking the loss of the three
    # fixtures whose subprocess environment this module decides.
    "tests/fakes/subprocess_env.py": (
        "tests.test_daily_flake_update",
        "tests.test_daily_beets_tip_update",
        "tests.test_deploy_cycle_verifier",
        "tests.test_deploy_cycle_verifier_generated",
        "tests.test_deploy_pin_script",
        "tests.test_deploy_pin_generated",
    ),
    # The basename probe still resolves tests.web.test_runtime, masking
    # the loss of the two HTTP-boundary modules.
    "web/runtime.py": (
        "tests.web.test_runtime",
        "tests.test_web_runtime_generated",
        "tests.web.test_server_threading",
        "tests.web.test_server_endpoints",
    ),
    "web/wrong_match_queue_view.py": (
        "tests.web.test_wrong_match_queue_view",
        "tests.web.test_routes_imports",
    ),
}


#: Contract D. Every `SELECTION_RULES` row whose deletion at least one file
#: would not report — the row-level twin of `MASKABLE_ENTRY_PINS` above —
#: mapped to sample paths and the exact modules each one stops selecting
#: without the row. Measured by `silently_lost_selection`, never
#: hand-curated.
#:
#: One path proves the deletion visible; a row is pinned at more than one
#: only where it needs more than one to cover every channel it can silently
#: lose (`test_every_pin_covers_every_channel_the_row_can_silently_lose`).
#: The rows absent from this table are the three where no matched file loses
#: anything silently: `basename:scripts/*.sh`,
#: `prefix:tests/structural_audits/` and `prefix:tests/world_model/`. That is
#: not the same as "every matched file reports the deletion" (review P3):
#: ten of the sixteen shell wrappers report nothing because they lose
#: nothing, and `tests/world_model/mirror_harness.py` is a registered gap
#: that early-returns either way. What the contract needs is the property
#: stated here, not the stronger one.
#:
#: Modules are compared sorted, so a reordered `*_NEIGHBOURS` tuple is not a
#: pin edit. Changing what one of those tuples CONTAINS is: the pipeline-DB
#: set is watched by two rows here, `prefix:lib/pipeline_db/` and
#: `prefix:migrations/`, which is the point — a shared tuple's membership
#: decides what several roots select.
MASKABLE_RULE_PINS: dict[str, dict[str, tuple[str, ...]]] = {
    # Both templates in one path: verdict_tiers.py has a deterministic
    # module and a generated one, and the lib/quality/ prefix rule keeps
    # resolving, so nothing raises when this row goes.
    "basename:lib/*.py": {
        "lib/quality/verdict_tiers.py": (
            "tests.test_verdict_tiers",
            "tests.test_verdict_tiers_generated",
        ),
    },
    # Issue #1331's own case, from the other side. Deleting this row makes
    # twelve scripts/ files raise and this one quietly stop selecting
    # tests.test_pyright_checks — a module written for
    # scripts/run_pyright_checks.py that never loads a phase parser. The
    # coverage is wrong-subject either way; the pin is that the row's
    # deletion cannot be silent anywhere.
    "basename:scripts/*.py": {
        "scripts/phase_parsers/pyright_checks.py": ("tests.test_pyright_checks",),
    },
    # The basename probe keeps resolving a cluster's own tests, so the loss
    # of the shared boundary contracts is silent. transfer_ledger.py covers
    # both channels: the five neighbours and the mirrored fake's self-tests.
    "prefix:lib/pipeline_db/": {
        "lib/pipeline_db/transfer_ledger.py": (
            "tests.test_fakes",
            "tests.test_fakes_transfer_ledger",
            "tests.test_pipeline_db",
            "tests.test_pipeline_db_column_contract",
            "tests.test_pipeline_db_write_audit",
            "tests.test_read_projection_audit",
        ),
    },
    # Silent only for the fakes that also carry a hand-authored entry; every
    # other fake fails closed on the tests/ row. Both channels are losable
    # here, and it took an unrelated PR to show it: this entry once said the
    # derived tests.test_fakes_<stem> channel could not be, on the reasoning
    # that a fake with a cluster sibling has no entry. #1313 batch C then
    # landed subprocess_env.py with BOTH, and this contract went red on the
    # merge — which is the whole point of measuring instead of reasoning.
    "prefix:tests/fakes/": {
        "tests/fakes/beets_contract.py": ("tests.test_fakes",),
        "tests/fakes/subprocess_env.py": (
            "tests.test_fakes",
            "tests.test_fakes_subprocess_env",
        ),
    },
    # The residual that opened this contract: five parsers fail closed
    # without their row and pyright_checks.py resolves the colliding
    # basename module instead.
    "prefix:scripts/phase_parsers/": {
        "scripts/phase_parsers/pyright_checks.py": (
            "tests.test_phase_parsers",
            "tests.test_suite_coordinator",
        ),
    },
    # The nix/ ROOT_COVERAGE_RULES row (issue #1355 item 8) now catches
    # nix/module.nix going silent on its own — but `exact_paths=("flake.nix",
    # "flake.lock")` reaches two files this row ALSO matches that live at
    # the TOP LEVEL, not under nix/, so the nix/ root row's `covers()` never
    # applies to them and the `<top-level>` row polices only `.py`. Neither
    # file this row's own suffix set (`.nix`) could name even exists at the
    # top level — flake.nix's suffix happens to be `.nix` too, which is a
    # coincidence of the two roots sharing a suffix, not a reason either
    # root rule reaches it.
    "prefix:nix/": {
        "flake.nix": ("tests.test_nix_module",),
    },
    # The harness/ ROOT_COVERAGE_RULES row (issue #1355 item 8) now catches
    # three of the six harness/ files going silent (beets_harness.py,
    # delete_album.py, run_beets_harness.sh have no other entry). The other
    # three carry their own hand-authored EXACT_PATH_NEIGHBOURS entry, which
    # keeps resolving something even with this row gone — so THEIR loss of
    # tests.test_harness_beets2_contract specifically stays silent.
    # lib/beets.py, this row's `exact_paths` sibling, was already caught by
    # the pre-existing lib/ row before this change (a different mechanism
    # entirely, unaffected by adding the harness/ row).
    "prefix:harness/": {
        "harness/import_one.py": ("tests.test_harness_beets2_contract",),
    },
    # Silent for four of the fifteen quality modules; the other eleven fail
    # closed on the lib/ row. Three of the four are masked by their own
    # basename probe (a `_generated` sibling exists). wire_types.py, the path
    # pinned here, is masked by its hand-authored EXACT_PATH_NEIGHBOURS entry
    # instead — its basename probe resolves nothing (review P2).
    "prefix:lib/quality/": {
        "lib/quality/wire_types.py": (
            "tests.test_quality_classification",
            "tests.test_quality_decisions",
            "tests.test_quality_generated",
        ),
    },
}


#: The `SelectionRule` fields that decide WHICH paths a row matches, derived
#: from the dataclass rather than hand-listed, so a new matcher field joins
#: the frozen set the moment it exists. `neighbours` and `derived` are
#: excluded because they decide what a matched path GETS, which
#: `MASKABLE_RULE_PINS` already watches.
MATCHER_FIELDS: tuple[str, ...] = tuple(
    field.name
    for field in dataclasses.fields(SelectionRule)
    if field.name not in {"name", "description", "neighbours", "derived"}
)


def rule_matcher(rule: SelectionRule) -> dict[str, object]:
    """A row's path conditions, minus every field left at its default.

    Dropping the defaults keeps the frozen copy in `MASKABLE_RULE_MATCHERS`
    short enough to read, and loses nothing: a field that gains a value, or
    loses one, changes the dict either way.
    """
    defaults = {
        field.name: field.default for field in dataclasses.fields(SelectionRule)
    }
    return {
        name: getattr(rule, name)
        for name in MATCHER_FIELDS
        if getattr(rule, name) != defaults[name]
    }


#: Contract D's second half, and the answer to the review's one high finding.
#: `MASKABLE_RULE_PINS` proves a row still CONTRIBUTES what it should to the
#: paths it names. It says nothing about the paths it no longer matches:
#: adding `excluded_prefixes=("web/routes/library", "web/routes/triage")` to
#: `prefix:web/routes/` left `web/routes/triage.py` selecting nothing at all,
#: with all 125 tests green (measured, review runner M30). Over a policed
#: root the root rule catches that; over `migrations/`, `nix/`, `web/`,
#: `harness/` and the top level nothing does, and those are exactly the roots
#: contract D exists for.
#:
#: So the matchers of every pinned row are frozen here. The measurement side
#: cannot catch this on its own — `rule_candidate_paths` derives its walk
#: FROM the matchers, so a narrowed row shrinks the population it is judged
#: against rather than showing a loss. An external anchor is the only thing
#: that can, the same reasoning `EXPECTED_SUFFIXES` records for
#: `ROOT_COVERAGE_RULES` one table over.
MASKABLE_RULE_MATCHERS: dict[str, dict[str, object]] = {
    "basename:lib/*.py": {"root": "lib", "suffixes": (".py",)},
    "basename:scripts/*.py": {"root": "scripts", "suffixes": (".py",)},
    "prefix:lib/pipeline_db/": {"prefixes": ("lib/pipeline_db/",)},
    "prefix:tests/fakes/": {"prefixes": ("tests/fakes/",)},
    "prefix:scripts/phase_parsers/": {"prefixes": ("scripts/phase_parsers/",)},
    "prefix:nix/": {
        "prefixes": ("nix/",),
        "exact_paths": ("flake.nix", "flake.lock"),
    },
    "prefix:harness/": {
        "prefixes": ("harness/",),
        "exact_paths": ("lib/beets.py",),
    },
    "prefix:lib/quality/": {"prefixes": ("lib/quality/",)},
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


def rule_candidate_paths(
    rule: SelectionRule, repo_root: Path
) -> tuple[str, ...]:
    """Every real file ``rule`` matches, walked from the rule's own matchers.

    Walking what the row itself constrains keeps this cheap and keeps
    `.claude/worktrees/` stale checkouts structurally unreachable (the
    #520/#543 shape) — no row names the repository root, and the `top_level`
    sweep uses `iterdir`, which does not descend.

    A prefix is a STRING prefix, not necessarily a directory: `matches` uses
    `startswith`, so `web/routes/p` matches real files while naming no
    directory. Walking `repo_root / prefix` alone therefore found nothing for
    such a row and reported it clean — a fail-open inside a contract whose
    whole premise is fail-closed measurement (review P1). The walk starts at
    the deepest ancestor that IS a directory and lets `matches` filter, and a
    prefix with no such ancestor below the repository root contributes
    nothing rather than escalating to a repository-wide walk.

    ``exact_paths`` are taken verbatim, existence unchecked: the resolver
    never stats its own target (only candidate test modules), so a
    fabricated path is a legitimate probe — which is what the known-bad
    self-tests below need.

    A row that matches no real file at all is not this function's problem to
    report; `TestEveryRuleIsLive` refuses it, so the emptiness cannot pass
    for "nothing to measure".
    """
    walked: list[str] = []
    for prefix in (*rule.prefixes, *([f"{rule.root}/"] if rule.root else ())):
        base = repo_root / prefix
        while not base.is_dir() and base != repo_root:
            base = base.parent
        if base == repo_root:
            continue
        walked.extend(
            path.relative_to(repo_root).as_posix()
            for path in base.rglob("*")
            if path.is_file() and "__pycache__" not in path.parts
        )
    if rule.top_level:
        walked.extend(path.name for path in repo_root.iterdir() if path.is_file())
    walked.extend(rule.exact_paths)
    return tuple(
        sorted(
            {
                relative
                for relative in walked
                if rule.matches(relative, PurePosixPath(relative))
            }
        )
    )


def selection_or_refusal(
    relative_path: str,
    repo_root: Path,
    *,
    basename_rules: Sequence[SelectionRule] = BASENAME_RULES,
    prefix_rules: Sequence[SelectionRule] = PREFIX_RULES,
    without: SelectionRule | None = None,
) -> tuple[str, ...] | None:
    """What selection resolves for ``relative_path``, or None when the
    fail-closed contract refuses the path instead.

    ``without`` drops one row from both tables through the production DI
    seam, so this measures the REAL resolver and the REAL raise rather than
    reimplementing either. With no row named, the two comprehensions are
    identity and this is an ordinary resolution. The table kwargs default to
    production's own and are replaced only by the known-bad self-tests,
    which need a fabricated row to be IN the table before removing it can
    mean anything.

    The resolver's admitted-gap stderr note is swallowed: this runs over
    every file a rule matches, and the registered gaps among them would bury
    the run in notes that say nothing about rules.
    """
    buffer = io.StringIO()
    try:
        with contextlib.redirect_stderr(buffer):
            return _changed_path_neighbours(
                relative_path,
                repo_root,
                basename_rules=tuple(
                    row for row in basename_rules if row is not without
                ),
                prefix_rules=tuple(
                    row for row in prefix_rules if row is not without
                ),
            )
    except ValueError:
        return None


def silently_lost_selection(
    rule: SelectionRule,
    repo_root: Path,
    *,
    basename_rules: Sequence[SelectionRule] = BASENAME_RULES,
    prefix_rules: Sequence[SelectionRule] = PREFIX_RULES,
) -> dict[str, tuple[str, ...]]:
    """Path → what it stops selecting if ``rule`` is deleted, for every file
    whose loss no fail-closed rule would report.

    A file is silent when deleting the row makes nothing RAISE while the
    modules the row contributed are gone. Two ways to reach that, the same
    disjunction `maskable_entry_paths` states for an entry: something else
    still resolves for the path, or no `ROOT_COVERAGE_RULES` row polices its
    root and suffix, in which case resolving nothing at all is silent too.
    The second is the bulk of it here — 125 of the silent files resolve
    exactly zero without their row (82 migrations, 22 routes, 13 nix/flake,
    4 web, 3 harness, and album_source.py) — which the earlier wording
    ("leaves it resolving something") flatly contradicted (review P4).

    The first shape is what issue #1331 found: five of the six
    `scripts/phase_parsers/` files fail closed without their row, and
    `pyright_checks.py` quietly resolves `tests.test_pyright_checks` instead,
    a module written for `scripts/run_pyright_checks.py` that never loads a
    parser.

    ``rule`` must be in the tables being measured, BY IDENTITY — the same
    comparison `selection_or_refusal` removes it with. A row that is not in
    them contributes to neither side, so every file would come back unchanged
    and this would report a clean sheet for a row it never measured, the exact
    vacuous-green shape a self-test is most likely to write by accident. An
    `in` test would have admitted a probe merely EQUAL to a real row, which
    removal by identity then would not remove (review P8).
    """
    if not any(
        rule is row for row in (*basename_rules, *prefix_rules)
    ):
        raise AssertionError(
            f"{rule.name} is not in the tables being measured, so removing "
            "it changes nothing and this measurement means nothing"
        )
    losses: dict[str, tuple[str, ...]] = {}
    for relative in rule_candidate_paths(rule, repo_root):
        selected = selection_or_refusal(
            relative,
            repo_root,
            basename_rules=basename_rules,
            prefix_rules=prefix_rules,
        )
        if selected is None:
            continue
        without = selection_or_refusal(
            relative,
            repo_root,
            basename_rules=basename_rules,
            prefix_rules=prefix_rules,
            without=rule,
        )
        if without is None:
            continue
        lost = tuple(module for module in selected if module not in without)
        if lost:
            losses[relative] = lost
    return losses


@functools.cache
def measured_rule_losses(
    rule: SelectionRule, repo_root: Path
) -> tuple[tuple[str, tuple[str, ...]], ...]:
    """`silently_lost_selection` as a cached, hashable pair sequence.

    Three contracts below ask the same question of the same fifteen rows,
    and the measurement walks a few hundred files twice per row.
    `SelectionRule` is a frozen dataclass of tuples, so it hashes.
    """
    return tuple(sorted(silently_lost_selection(rule, repo_root).items()))


def maskable_rule_names(
    rules: Sequence[SelectionRule], repo_root: Path
) -> set[str]:
    """Names of the rows whose deletion at least one file would not report."""
    return {
        rule.name for rule in rules if measured_rule_losses(rule, repo_root)
    }


def lost_channels(
    rule: SelectionRule, relative_path: str, lost: Sequence[str]
) -> tuple[set[str], list[str]]:
    """``(channels, unattributed)`` for one path's lost modules.

    A row contributes through named channels: its fixed `neighbours` tuple,
    and one channel per `derived` template. Naming them is what makes a pin
    table's adequacy mechanical instead of a judgement — a row with two
    templates needs a pinned path per template, or one template's loss stays
    invisible.

    ``unattributed`` fails the contract closed: a lost module belongs to the
    deleted row by construction, so one that matches no channel means this
    attribution no longer understands how the row contributes.
    """
    stem = PurePosixPath(relative_path).stem
    channels: set[str] = set()
    unattributed: list[str] = []
    for module in lost:
        matched = False
        if module in rule.neighbours:
            channels.add(NEIGHBOURS_CHANNEL)
            matched = True
        for template in rule.derived:
            if module == template.format(stem=stem):
                channels.add(template)
                matched = True
        if not matched:
            unattributed.append(module)
    return channels, unattributed


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
        """Non-empty unless `EXPECTED_REGISTRY_MAY_BE_EMPTY` admits it.

        A row backed by an unconditional prefix rule (migrations/, nix/,
        harness/) has no genuine gap to register today, and `<top-level>`
        currently has none either — see that anchor's own comment. Every
        other audited row must still name something real: an
        "always non-empty" registry that quietly emptied would be exactly
        as invisible as it was before this anchor existed.
        """
        for rule in AUDITED_RULES:
            with self.subTest(root=rule.root):
                if EXPECTED_REGISTRY_MAY_BE_EMPTY[rule.root]:
                    continue
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
                if rule.top_level:
                    # A top-level row's files are direct children of the
                    # repository root, never a recursive walk — mirrors
                    # `rule_candidate_paths`' own `iterdir()` handling of
                    # `SelectionRule.top_level`.
                    files = sorted(
                        path
                        for path in REPO_ROOT.iterdir()
                        if path.is_file() and path.suffix == suffix
                    )
                else:
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
                    if rule.top_level:
                        # Pins the walk itself, not just what it resolves:
                        # a `rglob("*")` regression here would still find
                        # every OTHER root's files resolving something
                        # (they are all separately policed), so nothing
                        # would raise and the loop below would stay quiet
                        # (issue #1355 item 8 review, mutant runner finding
                        # F1 — measured survivor, not a hypothetical one).
                        self.assertTrue(
                            all(len(p.relative_to(REPO_ROOT).parts) == 1
                                for p in files),
                            "top-level walk returned a nested path — "
                            "this branch must use iterdir(), never rglob()",
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


class TestEveryRuleIsLive(unittest.TestCase):
    """A row that matches nothing, or gives nothing to anything it matches.

    Contract D measures what a row's files lose without it, so a row with no
    files measures clean and demands no pin — the fail-open half of review
    P1. `prefixes` is a string prefix and nothing checks it names anything
    real, so `lib/pipline_db/` (typo intended) would be inert in production
    AND clean here. This is also the row-level answer to what contract B
    gives an entry (review P11): an entry whose modules the fallback already
    resolves is refused, and so now is a row that contributes to nothing.
    """

    def test_every_rule_matches_at_least_one_real_file(self) -> None:
        for rule in SELECTION_RULES:
            with self.subTest(rule=rule.name):
                self.assertTrue(
                    rule_candidate_paths(rule, REPO_ROOT),
                    f"{rule.name} matches no file in the repository — its "
                    "prefixes, root or exact paths name nothing that exists, "
                    "so it selects nothing in production and measures clean "
                    "in every contract here",
                )

    def test_every_rule_contributes_to_at_least_one_file_it_matches(
        self,
    ) -> None:
        """Matching is not contributing: a row whose every derived template
        misses on disk matches files and hands them nothing.
        """
        for rule in SELECTION_RULES:
            paths = rule_candidate_paths(rule, REPO_ROOT)
            contributing = [
                path
                for path in paths
                if rule.contribute(PurePosixPath(path), REPO_ROOT)[0]
            ]
            with self.subTest(rule=rule.name):
                self.assertTrue(
                    contributing,
                    f"{rule.name} matches {len(paths)} files and contributes "
                    "a module to none of them — it reads as coverage in the "
                    "table while selecting nothing",
                )


class TestMaskableRulePins(unittest.TestCase):
    """Contract D: a rule row whose deletion nothing reports carries a pin.

    Contract C answers this for `EXACT_PATH_NEIGHBOURS`; a `SELECTION_RULES`
    row had no equivalent (issue #1331 residual 1), so deleting one was
    silent for exactly the files whose basename collides with an existing
    test module — and, before issue #1355 item 8 gave every root a
    `ROOT_COVERAGE_RULES` row, completely silent for a row over
    `migrations/`, `nix/`, `web/`, `harness/`, or the top level too. Item 8
    closed most of that: only `prefix:nix/` and `prefix:harness/` still
    silently lose SOME of their matched files today (their own comments in
    `MASKABLE_RULE_PINS` below say exactly which, and why).
    """

    def test_pin_keys_are_exactly_the_measured_maskable_rule_set(self) -> None:
        measured = maskable_rule_names(SELECTION_RULES, REPO_ROOT)
        pinned = set(MASKABLE_RULE_PINS)

        unpinned = sorted(measured - pinned)
        self.assertEqual(
            unpinned,
            [],
            "SELECTION_RULES rows whose deletion at least one file would not "
            "report — add a MASKABLE_RULE_PINS pin for each: "
            + ", ".join(unpinned),
        )
        stale = sorted(pinned - measured)
        self.assertEqual(
            stale,
            [],
            "MASKABLE_RULE_PINS pins a row every matched file now reports "
            "the deletion of — remove the stale pin: " + ", ".join(stale),
        )

    def test_every_pinned_path_still_loses_exactly_its_pinned_modules(
        self,
    ) -> None:
        """The assertion ordinary maintenance hits first, and the one that
        drives the real resolver twice: deleting the row, dropping a module
        from its `neighbours`, breaking a `derived` template, or narrowing
        its matchers past the pinned path all change what that path loses.
        """
        rules = {rule.name: rule for rule in SELECTION_RULES}
        for name, expected in MASKABLE_RULE_PINS.items():
            rule = rules.get(name)
            with self.subTest(rule=name):
                self.assertIsNotNone(
                    rule,
                    f"{name} is pinned here but no longer exists in "
                    "SELECTION_RULES. No fail-closed rule protects this row, "
                    "so the pin is what makes its deletion visible — removing "
                    "the row is a deliberate two-place edit.",
                )
                assert rule is not None
                measured = {
                    path: tuple(sorted(lost))
                    for path, lost in measured_rule_losses(rule, REPO_ROOT)
                }
                self.assertEqual(
                    {path: measured.get(path) for path in expected},
                    {
                        path: tuple(sorted(lost))
                        for path, lost in expected.items()
                    },
                    f"{name}'s pinned paths and what they really lose "
                    "disagree — update the pin in this file to match, or "
                    "restore what the row contributed.",
                )

    def test_every_pinned_rows_matchers_are_frozen(self) -> None:
        """The loss pins watch what a row gives the paths it names; this
        watches which paths it matches at all.

        Narrowing a row over an unpoliced root deletes real selection with
        nothing else objecting, and the loss pins cannot see it: their
        measurement walks the row's OWN matchers, so narrowing shrinks the
        population rather than showing a loss. Both tables cover the same
        rows, so a pin added to one and not the other is red here.
        """
        self.assertEqual(
            sorted(MASKABLE_RULE_MATCHERS),
            sorted(MASKABLE_RULE_PINS),
            "every maskable row needs both a loss pin and a frozen matcher",
        )

        rules = {rule.name: rule for rule in SELECTION_RULES}
        for name, expected in MASKABLE_RULE_MATCHERS.items():
            rule = rules.get(name)
            if rule is None:
                # The sibling test owns the "row is gone" message.
                continue
            with self.subTest(rule=name):
                self.assertEqual(
                    rule_matcher(rule),
                    expected,
                    f"{name}'s path conditions changed. No fail-closed rule "
                    "watches which files this row matches, so narrowing it "
                    "silently drops whatever stopped matching — update the "
                    "frozen matcher here deliberately, or restore the row.",
                )

    def test_every_pin_covers_every_channel_the_row_can_silently_lose(
        self,
    ) -> None:
        """A row contributes through its fixed `neighbours` and one channel
        per `derived` template, and a pin only watches the channels its own
        paths lose. `basename:web/*.py` is why this exists: it derives both
        `tests.test_web_<stem>` and `tests.web.test_<stem>`, and no single
        file loses both, so pinning one path leaves the other template's
        loss invisible.
        """
        rules = {rule.name: rule for rule in SELECTION_RULES}
        for name, expected in MASKABLE_RULE_PINS.items():
            rule = rules.get(name)
            if rule is None:
                # A pin for a row that no longer exists is the sibling test's
                # message, and a KeyError here would only bury it.
                continue
            measured = dict(measured_rule_losses(rule, REPO_ROOT))
            losable: set[str] = set()
            unattributed: list[str] = []
            for path, lost in measured.items():
                channels, unknown = lost_channels(rule, path, lost)
                losable |= channels
                unattributed.extend(f"{path}: {module}" for module in unknown)
            pinned: set[str] = set()
            for path, lost in expected.items():
                channels, _ = lost_channels(rule, path, lost)
                pinned |= channels

            with self.subTest(rule=name):
                self.assertEqual(
                    unattributed,
                    [],
                    f"{name} silently loses modules this attribution cannot "
                    "trace to the row's neighbours or a derived template: "
                    + ", ".join(unattributed),
                )
                self.assertEqual(
                    pinned,
                    losable,
                    f"{name}'s pins watch {sorted(pinned)} but the row can "
                    f"silently lose {sorted(losable)} — pin a path per "
                    "unwatched channel",
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
    `TestRootCoverageRegistriesAreExact` independently proves. A row whose
    registry is legitimately empty (`EXPECTED_REGISTRY_MAY_BE_EMPTY`) has no
    real gap to drive and is skipped in those three controls — at least one
    other row still exercises each control, which is what proves the
    mechanism itself, not any one row's data.
    """

    def test_unmapped_top_level_path_fails_closed_with_its_own_message(
        self,
    ) -> None:
        """Every row, every suffix it polices: an unmapped path raises with
        THAT row's message. Building the expectation from the row the probe
        belongs to is what catches two rows' messages being swapped. The
        probe need not exist on disk — `_changed_path_neighbours` never
        stats its own target, only candidate test modules.

        Two carve-outs, both explained where they are DATA, not here:
        `UNCONDITIONALLY_SHADOWED_ROOTS` roots can never make this probe
        raise while their shadowing prefix rule exists (proved instead by
        `test_root_rule_catches_an_unconditionally_shadowed_root_if_its_prefix_rule_is_removed`),
        and a `top_level` row's probe has no directory component to prefix
        with the root name.
        """
        for rule in ROOT_COVERAGE_RULES:
            if rule.root in UNCONDITIONALLY_SHADOWED_ROOTS:
                continue
            marker = UNMAPPED_MESSAGE_MARKERS[rule.root]
            self.assertTrue(rule.unmapped_message.startswith(marker))
            for suffix in rule.suffixes:
                probe = (
                    f"_totally_unmapped_probe{suffix}"
                    if rule.top_level
                    else f"{rule.root}/_totally_unmapped_probe{suffix}"
                )
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

        Excludes `UNCONDITIONALLY_SHADOWED_ROOTS` for the same reason as the
        sibling test above. Excludes `top_level` rows too: `covers()`
        requires `len(path.parts) == 1` for such a row, so a nested path can
        never be one of ITS OWN matches in the first place — there is no
        "one level deeper" for a file with no directory component at all.
        """
        for rule in ROOT_COVERAGE_RULES:
            if rule.root in UNCONDITIONALLY_SHADOWED_ROOTS or rule.top_level:
                continue
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

    def test_root_rule_catches_an_unconditionally_shadowed_root_if_its_prefix_rule_is_removed(
        self,
    ) -> None:
        """Proves the real value of a `UNCONDITIONALLY_SHADOWED_ROOTS` row:
        drop its shadowing prefix rule through the same `without=` DI seam
        `TestMaskableRulePins` uses, and the root row now catches every file
        in that root going silently under-selected — exactly what would
        happen in production if that prefix rule were ever deleted or
        narrowed. `selection_or_refusal` returns `None` on the fail-closed
        raise and never touches the real production tables.
        """
        rules_by_name = {rule.name: rule for rule in SELECTION_RULES}
        for root, shadowing_rule_name in UNCONDITIONALLY_SHADOWED_ROOTS.items():
            rule = next(r for r in ROOT_COVERAGE_RULES if r.root == root)
            shadowing_rule = rules_by_name[shadowing_rule_name]
            for suffix in rule.suffixes:
                probe = f"{root}/_shadow_removed_probe{suffix}"
                with self.subTest(root=root, suffix=suffix):
                    self.assertIsNone(
                        selection_or_refusal(
                            probe, REPO_ROOT, without=shadowing_rule
                        )
                    )

    def test_admitted_gap_log_names_the_path_and_its_rationale(self) -> None:
        """The loud stderr line must name BOTH the registered path AND its
        registered rationale — not merely print SOME line.

        Skips a row whose registry is legitimately empty (see
        `EXPECTED_REGISTRY_MAY_BE_EMPTY`) — there is no real registered
        entry to grab, so this row has nothing to prove here.
        """
        for rule in AUDITED_RULES:
            if not rule.registry:
                continue
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

        Skips a row whose registry is legitimately empty (see
        `EXPECTED_REGISTRY_MAY_BE_EMPTY`) — there is no real registered
        entry to grab, so this row has nothing to prove here.
        """
        for rule in AUDITED_RULES:
            if not rule.registry:
                continue
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
                top_level=rule.top_level,
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

        Skips a row whose registry is legitimately empty (see
        `EXPECTED_REGISTRY_MAY_BE_EMPTY`) — there is no real zero-neighbour
        path to build the fabricated entry from, so this row has nothing to
        prove here.
        """
        for rule in AUDITED_RULES:
            if not rule.registry:
                continue
            registered = next(iter(rule.registry))
            fabricated = RootCoverageRule(
                root=rule.root,
                suffixes=rule.suffixes,
                registry={registered: "synthetic rationale for self-test"},
                registry_name=rule.registry_name,
                admitted_selects_nothing=rule.admitted_selects_nothing,
                unmapped_message=rule.unmapped_message,
                top_level=rule.top_level,
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

        Skips `UNCONDITIONALLY_SHADOWED_ROOTS` for the same reason as the
        two sibling unmapped-path pins: no probe can raise there while the
        shadowing prefix rule exists.
        """
        marker = "admitted selection gap"
        self.assertIn(marker, ADMITTED_GAP_MESSAGE)

        for rule in ROOT_COVERAGE_RULES:
            if rule.root in UNCONDITIONALLY_SHADOWED_ROOTS:
                continue
            for suffix in rule.suffixes:
                probe = (
                    f"_unregistered_probe{suffix}"
                    if rule.top_level
                    else f"{rule.root}/_unregistered_probe{suffix}"
                )
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

    def test_silent_loss_checker_reports_a_row_whose_deletion_nothing_catches(
        self,
    ) -> None:
        """Contract D's measurement, driven by a probe row added to the real
        prefix table. Removing it leaves `web/cache.py` resolving its own
        basename module, and no rule polices web/ — so the probe's neighbour
        vanishes with nothing raising, which is the whole shape.
        """
        probe = SelectionRule(
            name="prefix:_silent_probe",
            description="probe",
            exact_paths=("web/cache.py",),
            neighbours=("tests.test_fakes",),
        )

        losses = silently_lost_selection(
            probe, REPO_ROOT, prefix_rules=(*PREFIX_RULES, probe)
        )

        self.assertEqual(losses, {"web/cache.py": ("tests.test_fakes",)})

    def test_silent_loss_checker_is_quiet_when_the_deletion_fails_closed(
        self,
    ) -> None:
        """Must-still-work. The same probe over a `lib/` path resolves
        nothing without its row, and the lib/ row raises — a deletion
        nobody could miss, so it is not a silent loss and needs no pin.
        """
        probe = SelectionRule(
            name="prefix:_policed_probe",
            description="probe",
            exact_paths=("lib/_deletion_probe.py",),
            neighbours=("tests.test_fakes",),
        )

        self.assertEqual(
            silently_lost_selection(
                probe, REPO_ROOT, prefix_rules=(*PREFIX_RULES, probe)
            ),
            {},
        )

    def test_silent_loss_checker_is_quiet_when_another_rule_supplies_it(
        self,
    ) -> None:
        """Must-still-work, and the other direction of "quiet": nothing
        raises here either, but the real lib/pipeline_db/ row supplies the
        probe's only module, so deleting the probe loses nothing at all.
        """
        probe = SelectionRule(
            name="prefix:_duplicate_probe",
            description="probe",
            exact_paths=("lib/pipeline_db/decisions.py",),
            neighbours=("tests.test_pipeline_db",),
        )

        self.assertEqual(
            silently_lost_selection(
                probe, REPO_ROOT, prefix_rules=(*PREFIX_RULES, probe)
            ),
            {},
        )

    def test_selection_or_refusal_swallows_only_the_fail_closed_refusal(
        self,
    ) -> None:
        """Widening its `except ValueError` to `except Exception` survived
        every other test (review runner F2), and the helper's whole job is
        telling a fail-closed refusal apart from a resolution: anything else
        swallowed would quietly reclassify a broken resolver as "this path
        refuses" and drop rows out of the maskable set.

        A `derived` template naming a field the formatter has no value for
        raises `KeyError` inside the real `render_derived`, which is a
        producible world rather than a patched one.
        """
        exploding = SelectionRule(
            name="prefix:_exploding_probe",
            description="probe",
            exact_paths=("lib/_exploding_probe.py",),
            derived=("tests.test_{no_such_field}",),
        )

        with self.assertRaises(KeyError):
            selection_or_refusal(
                "lib/_exploding_probe.py",
                REPO_ROOT,
                prefix_rules=(*PREFIX_RULES, exploding),
            )

    def test_silent_loss_checker_skips_a_path_already_failing_closed(
        self,
    ) -> None:
        """The fourth clause, `selected is None`: a path the row matches that
        ALREADY refuses with the row present has no selection to lose, and
        counting it would invent a loss out of a pre-existing refusal.

        The probe row contributes nothing (its derived template names no
        module on disk), so `lib/_pre_refused_probe.py` resolves zero WITH the
        row and the lib/ row raises. Deleting the clause is not merely a wrong
        answer here, it is a `TypeError` on the `None` — which is what makes
        this assertion evidence the clause runs at all. No real row reaches
        this state today (measured: zero such paths across all fifteen), so
        the clause is fail-closed legislation rather than live coverage.
        """
        probe = SelectionRule(
            name="prefix:_pre_refused_probe",
            description="probe",
            exact_paths=("lib/_pre_refused_probe.py",),
            derived=("tests.test_no_such_module_{stem}",),
        )

        self.assertEqual(
            silently_lost_selection(
                probe, REPO_ROOT, prefix_rules=(*PREFIX_RULES, probe)
            ),
            {},
        )

    def test_silent_loss_checker_refuses_a_row_outside_the_measured_tables(
        self,
    ) -> None:
        """A row the tables do not hold contributes to neither side, so the
        measurement would report a clean sheet for a row it never measured.
        Loud instead.
        """
        probe = SelectionRule(
            name="prefix:_absent_probe",
            description="probe",
            exact_paths=("web/cache.py",),
            neighbours=("tests.test_fakes",),
        )

        with self.assertRaisesRegex(
            AssertionError, r"prefix:_absent_probe is not in the tables"
        ):
            silently_lost_selection(probe, REPO_ROOT)

    def test_candidate_paths_walk_the_row_and_take_exact_paths_verbatim(
        self,
    ) -> None:
        """The walk is derived from the row's own matchers, so a row that
        constrains nothing real yields nothing to measure. `exact_paths` are
        the deliberate exception: the resolver never stats its own target,
        and the probes above depend on a path that does not exist.
        """
        parsers = SelectionRule(
            name="prefix:_walk_probe",
            description="probe",
            prefixes=("scripts/phase_parsers/",),
            neighbours=("tests.test_fakes",),
        )
        absent = SelectionRule(
            name="prefix:_absent_path_probe",
            description="probe",
            exact_paths=("lib/_deletion_probe.py",),
            neighbours=("tests.test_fakes",),
        )

        self.assertEqual(
            rule_candidate_paths(parsers, REPO_ROOT),
            (
                "scripts/phase_parsers/__init__.py",
                "scripts/phase_parsers/dead_code.py",
                "scripts/phase_parsers/js_checks.py",
                "scripts/phase_parsers/pyright_checks.py",
                "scripts/phase_parsers/python_tests.py",
                "scripts/phase_parsers/ruff.py",
            ),
        )
        self.assertEqual(
            rule_candidate_paths(absent, REPO_ROOT), ("lib/_deletion_probe.py",)
        )

    def test_an_inert_row_walks_nothing_or_contributes_nothing(self) -> None:
        """Both clauses `TestEveryRuleIsLive` asserts, shown false.

        A prefix with a typo names no directory and no file, so the walk is
        empty — the fail-open review P1 found, since an empty walk is also an
        empty loss map and therefore a clean measurement. And a row CAN match
        real files while giving them nothing, when its only channel is a
        derived template that misses on disk.
        """
        typo = SelectionRule(
            name="prefix:_typo_probe",
            description="probe",
            prefixes=("lib/pipline_db/",),
            neighbours=("tests.test_fakes",),
        )
        self.assertEqual(rule_candidate_paths(typo, REPO_ROOT), ())

        derived_only = SelectionRule(
            name="prefix:_derived_miss_probe",
            description="probe",
            prefixes=("scripts/phase_parsers/",),
            derived=("tests.test_no_such_module_{stem}",),
        )
        walked = rule_candidate_paths(derived_only, REPO_ROOT)

        self.assertEqual(len(walked), 6)
        self.assertEqual(
            [
                path
                for path in walked
                if derived_only.contribute(PurePosixPath(path), REPO_ROOT)[0]
            ],
            [],
        )

    def test_channel_attribution_names_both_channels_and_fails_closed(
        self,
    ) -> None:
        """Every clause of `lost_channels`: a fixed neighbour, a derived
        template resolved against the path's own stem, and a module from
        neither, which the contract must refuse rather than ignore.
        """
        rule = SelectionRule(
            name="prefix:_channel_probe",
            description="probe",
            prefixes=("lib/pipeline_db/",),
            neighbours=("tests.test_fakes",),
            derived=("tests.test_fakes_{stem}",),
        )

        channels, unattributed = lost_channels(
            rule,
            "lib/pipeline_db/transfer_ledger.py",
            (
                "tests.test_fakes",
                "tests.test_fakes_transfer_ledger",
                "tests.test_pipeline_db",
            ),
        )

        self.assertEqual(
            channels, {NEIGHBOURS_CHANNEL, "tests.test_fakes_{stem}"}
        )
        self.assertEqual(unattributed, ["tests.test_pipeline_db"])
        self.assertEqual(
            lost_channels(
                rule, "lib/pipeline_db/evidence.py", ("tests.test_fakes",)
            ),
            ({NEIGHBOURS_CHANNEL}, []),
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
            # The web/ row polices only `.py` (issue #1355 item 8) — `.js`
            # selection is not governed by this mechanism at all (the
            # ambient JS phase runs unconditionally), so no rule polices
            # this root+suffix combination.
            "web/_unpoliced_probe.js": ("tests.test_pipeline_db",),
        }

        self.assertEqual(
            maskable_entry_paths(fabricated, REPO_ROOT),
            {"lib/pipeline_db/_masked_probe.py", "web/_unpoliced_probe.js"},
        )


if __name__ == "__main__":
    unittest.main()
