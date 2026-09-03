"""Select explicit, adjacent, and repository-wide targeted tests."""

from __future__ import annotations

import argparse
import contextlib
import io
import subprocess
import sys
from collections import Counter
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
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
    "tests.web.test_routes_retag_divergence_audit",
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
    "lib/pipeline_db/decisions.py": (
        # The lib/pipeline_db/ prefix rule below already adds the heavy
        # PIPELINE_DB_NEIGHBOURS set, so this file is never zero-neighbour
        # — but the two modules that actually pin its four pure rules are
        # named for the package, not the file stem, so
        # _direct_test_candidates' basename probes (tests.test_decisions /
        # tests.test_decisions_generated) miss them entirely.
        "tests.test_pipeline_db_decisions",
        "tests.test_pipeline_db_decisions_generated",
    ),
    "lib/convergence.py": (
        # Was an admitted zero-neighbour gap (issue #1199, measured
        # 2026-08-19): there is no tests.test_convergence, so the basename
        # probes found nothing. Its real coverage lives in the runner /
        # registry module, whose name the probes cannot derive.
        "tests.test_convergence_runner_generated",
    ),
    "lib/enqueue.py": (
        # Was an admitted zero-neighbour gap even though its outer-adapter
        # pins and generated behaviour contracts live under names the
        # basename probe cannot derive (issue #1306).
        "tests.test_enqueue_fanout",
        "tests.test_enqueue_admission_generated",
        "tests.test_multidisc_manifest_generated",
        "tests.test_cross_request_enqueue_guard_generated",
    ),
    "lib/slskd_transfer_ledger.py": (
        # Was an admitted zero-neighbour gap (issue #1199): no
        # tests.test_slskd_transfer_ledger. The registered cycle step's
        # failure-reachability pin lives with the convergence runner, and
        # its DB-failure-propagation contract pin with the sweep
        # exception contracts (issue #1312).
        "tests.test_convergence_runner_generated",
        "tests.test_slskd_sweep_exception_contracts",
    ),
    "lib/slskd_transfers.py": (
        # Was an admitted zero-neighbour gap (issue #1199): no
        # tests.test_slskd_transfers, and the module's behavior is split
        # across the sweeps' own test homes. Not a claim of full
        # coverage — these are the deterministic and generated modules
        # whose subjects a solo slskd_transfers.py diff most plausibly
        # regresses: enqueue/orphan/purge pins (test_download), the
        # disk reaper's invariants, cancel_and_delete's C3/C4 ownership
        # properties, the completed-purge properties, and the five
        # sweeps' settled exception contracts (issue #1312), plus the
        # T1 write-ahead-ordering property whose subject is this
        # module's own slskd_enqueue_with_outcome (the ONE production
        # enqueue call site).
        "tests.test_download",
        "tests.test_disk_reaper_generated",
        "tests.test_convergence_ledger_generated",
        "tests.test_completed_purge_generated",
        "tests.test_transfer_ledger_generated",
        "tests.test_slskd_sweep_exception_contracts",
    ),
    "lib/download.py": (
        # The basename probe resolves tests.test_download on its own;
        # this entry adds the two modules that pin
        # harvest_terminal_transfer_evidence's DB-propagation contract —
        # the sweep-internal pin and the composed reachability row
        # (issue #1312 round-2 reader finding R2: without them a solo
        # lib/download.py diff that swallows harvest's seam runs green).
        # Maskable — pinned in MASKABLE_ENTRY_PINS.
        "tests.test_download",
        "tests.test_slskd_sweep_exception_contracts",
        "tests.test_convergence_runner_generated",
    ),
    "lib/ephemeral_postgres.py": (
        # Was an admitted zero-neighbour gap (issue #1199) caused by a
        # basename mismatch: the module's real coverage has always lived
        # in tests/test_ephemeral_pg.py (server-option argv pins,
        # transition-seed refusal), which the tests.test_ephemeral_postgres
        # probe cannot derive. tests.test_pipeline_db carries the live
        # session clock-frame pin (TestEphemeralPostgresClockFrame) plus
        # every real-PG round trip this cluster exists to host.
        "tests.test_ephemeral_pg",
        "tests.test_pipeline_db",
    ),
    "lib/download_ownership.py": (
        # Was an admitted zero-neighbour gap (issue #1199): no
        # tests.test_download_ownership. The ownership writer/reader
        # port's real coverage: the DownloadOwnershipDB parity tests and
        # claim/confirm orchestration live in test_download; the T1
        # write-ahead property drives DownloadOwnershipWriter directly;
        # the cross-request guard property drives its conflict-check
        # session (issue #1312 reader finding F5).
        "tests.test_download",
        "tests.test_transfer_ledger_generated",
        "tests.test_cross_request_enqueue_guard_generated",
    ),
    "lib/slskd_searches.py": (
        # The basename probe resolves tests.test_slskd_searches on its
        # own; this entry adds the sweep-exception-contract module whose
        # pins drive converge_slskd_searches's DB seams directly (issue
        # #1312). Maskable — pinned in MASKABLE_ENTRY_PINS.
        "tests.test_slskd_searches",
        "tests.test_slskd_sweep_exception_contracts",
    ),
    "lib/measurement.py": (
        # The basename probe resolves tests.test_measurement on its own, so
        # this file is never zero-neighbour — but the ONLY coverage of
        # diagnostic_from_stderr / STDERR_DIAGNOSTIC_MAX_CHARS (a generated
        # property plus its known-bad checker self-test) lives in a module
        # named for the concern, not the file stem. Issue #1313 moved that
        # helper here from lib/import_preview.py and made it public, so its
        # tests must follow the code. Maskable — pinned in
        # MASKABLE_ENTRY_PINS.
        "tests.test_measurement",
        "tests.test_measurement_observability",
    ),
    "lib/preview_snapshot.py": (
        # Issue #1313 split the private-preview snapshot and job-scoped
        # action-copy lifecycles out of lib/import_preview.py. There is no
        # tests.test_preview_snapshot: this code has always been covered by
        # modules named for the boundary it guards rather than for the file
        # it lived in, so it is zero-neighbour without this entry and
        # deleting the entry fails closed on the lib/ root rule rather than
        # silently under-selecting — no MASKABLE_ENTRY_PINS pin needed.
        #
        # The list is DERIVED, not curated: every test module that imports a
        # name from lib.preview_snapshot, measured by AST over tests/ at the
        # time of the split. Re-derive it the same way rather than adding
        # what looks related — the first draft of this entry named
        # tests.test_processing_cancellation_generated, which drives only
        # lib/staged_album.py and references nothing here, while omitting
        # five modules whose imports this same commit rewrote.
        #
        # Two deliberate omissions. tests/world_model/support.py imports
        # force_action_copy_path, but its only drivers are the world-model
        # state machine and mirror harness, whose target is the world-model
        # burst rather than an ordinary selection. And
        # tests/test_automation_startup_recovery.py reads as coverage and is
        # not: it stubs the force_action_copy_path_fn DI seam and never
        # imports the production function.
        "tests.test_path_authority",
        "tests.test_path_authority_generated",
        "tests.test_processing_cancellation",
        "tests.test_import_queue",
        "tests.test_import_queue_generated",
        "tests.test_import_operation_fence",
        "tests.test_import_operation_fence_generated",
        "tests.test_importer_job_kinds",
        "tests.test_integration_slices",
        "tests.test_local_import_lane",
        "tests.test_pipeline_db",
    ),
    "lib/current_library_evidence.py": (
        # The basename probes DO resolve this one — tests.test_current_
        # library_evidence and its generated sibling both exist — so it was
        # never zero-neighbour and never failed closed. That is exactly the
        # problem: editing the module selected 2 of the 11 test modules that
        # import from it, silently, and PR #1334's retarget of
        # tests.test_spectral_attempt_audit_generated onto
        # resolve_current_library_evidence made the gap worse rather than
        # visible. Deleting this entry leaves the two basename modules
        # resolving, so nothing here fails closed — hence the
        # MASKABLE_ENTRY_PINS pin in tests/test_selection_coverage_audit.py.
        #
        # DERIVED, not curated (the #1334 lesson): every test module holding
        # an `import ... from lib.current_library_evidence`, measured by AST
        # over tests/. Re-derive it the same way. One deliberate omission,
        # the same one lib/preview_snapshot.py's entry makes:
        # tests/world_model/support.py and state_machine.py import
        # HaveEnrichment and two sibling names, but their only drivers are
        # the world-model state machine and mirror harness, whose target is
        # the world-model burst rather than an ordinary selection.
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
    "lib/dispatch/quality_gate.py": (
        # The basename probe resolves tests.test_quality_gate, masking the
        # loss of the two integration modules that exercise this seam through
        # dispatch composition and end-to-end import slices (#1321).
        "tests.test_quality_gate",
        "tests.test_import_dispatch",
        "tests.test_integration_slices",
    ),
    "lib/dispatch/evidence_gate.py": (
        # The dispatch/ subdirectory defeats basename derivation for the
        # established integration homes. Keep the grouped contract plus every
        # deterministic production boundary used by the #1321 catalog.
        "tests.test_evidence_gate",
        "tests.test_dispatch_core",
        "tests.test_dispatch_from_db",
        "tests.test_import_dispatch",
        "tests.test_import_queue",
        "tests.test_integration_slices",
        "tests.test_sidecar_service",
        "tests.test_current_evidence_authority_generated",
    ),
    "lib/dispatch/outcome_actions.py": (
        # Grouped writer contracts plus every deterministic caller boundary
        # used by the #1321 catalog; the generated module patrols the full
        # dispatch/lifecycle world space outside per-mutant execution.
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
    "lib/startup_reconciliation.py": (
        # Was an admitted zero-neighbour gap (issue #1199, measured
        # 2026-08-19): there is no tests.test_startup_reconciliation, so
        # the basename probes found nothing. Its real coverage has always
        # lived in TestStartupReconciliationSlice, which drives
        # reconcile_search_plans end to end against FakePipelineDB plus
        # the real SearchPlanService — including the dry-run bucket
        # classifier this file's #1278 item 7 change delegates.
        "tests.test_integration_slices",
    ),
    "pyrightconfig.json": (
        "tests.test_pyright_checks",
    ),
    "pyrightconfig.production.json": (
        "tests.test_pyright_checks",
    ),
    "scripts/run_final_gate.sh": (
        # Issue #1278 item 6: this file is now a thin wrapper that execs
        # scripts/test_substrate.py's `final-gate` subcommand, and before
        # this entry, editing the wrapper (or deleting the exec line
        # outright) selected no test at all. The .sh basename probe added
        # in item 9 does not rescue it either — there is no
        # tests/test_run_final_gate.py; the module that drives the real
        # wrapper end to end with a fake `nix` on PATH is named for the
        # receipt, not the file.
        "tests.test_final_gate_receipt",
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
    # scripts/targeted_test_selection.py needs no entry: its basename probe
    # already resolves exactly tests.test_targeted_test_selection, which is
    # all its deleted entry ever named (issue #1278 item 9, contract B).
    "scripts/test.sh": (
        "tests.test_targeted_test_selection",
    ),
    "scripts/test_substrate.py": (
        # Issue #1278 item 6: the shared test-runtime substrate (admission,
        # headroom, /proc liveness, scratch/bundle/receipt reaping, the
        # on-disk name spellings) extracted out of scripts/run_test_suite.py.
        # _direct_test_candidates already derives tests.test_test_substrate
        # from this basename, but that module only pins the stdlib-only
        # import boundary — the behaviour these functions actually own is
        # driven by the modules named here, which no basename probe can
        # reach. Every test module that imports from scripts.test_substrate
        # is listed (measured by grep, not guessed), because a substrate
        # change can break any of them: tests.test_suite_coordinator
        # (admission lock, holder identity, headroom floors, both reapers),
        # tests.test_test_tmpfs (the real ".owner" marker written by
        # scripts/test_tmpfs.sh, read back through _scratch_tree_owner_dead),
        # tests.test_fuzz_burst / tests.test_world_model_coordinator (the
        # two bursts' real headroom preconditions and the shared
        # exhaustion identity), tests.test_parallel_test_runner (the same
        # identity mid-run) and tests.test_targeted_test_selection (the
        # admission lockfile a targeted run waits on).
        # tests.test_final_gate_receipt joined the list in the item-6
        # follow-up PR, when the final gate itself moved here out of bash:
        # scripts/run_final_gate.sh is now a wrapper, so the receipt
        # format, the gate's argv, its signal semantics and the whole
        # status ladder are this file's behaviour and nothing else drives
        # them.
        # tests.test_targeted_test_selection also pins this very entry's
        # resolved selection, so deleting it goes RED there instead of
        # falling silently back to the basename candidate.
        "tests.test_final_gate_receipt",
        "tests.test_fuzz_burst",
        "tests.test_parallel_test_runner",
        "tests.test_suite_coordinator",
        "tests.test_targeted_test_selection",
        "tests.test_test_substrate",
        "tests.test_test_tmpfs",
        "tests.test_world_model_coordinator",
    ),
    # scripts/test_tmpfs.sh needed a hand-written entry under issue #1208
    # review D1, when a solo producer-side edit here (the /proc field index,
    # $$ vs $PPID, the marker filename/delimiter) selected nothing at all.
    # The .sh basename probe added in #1278 item 9 now resolves exactly the
    # tests.test_test_tmpfs that entry named, so the entry was pure
    # redundancy and is gone (contract B); the selection it produced is
    # unchanged.
    # Shared tests/ infrastructure (issue #1081): none of these are
    # discoverable test modules themselves, so each needs an explicit
    # mapping to the test(s) that actually exercise it. tests/structural_audits/
    # and tests/world_model/ are covered by prefix rules below instead of
    # listed file-by-file here — tests/fakes/ and tests/web/ are NOT (a
    # per-file entry here always wins over the tests/fakes/ prefix rule,
    # which only adds tests.test_fakes plus any derived
    # tests.test_fakes_<stem> on top). Sorted by path (ASCII, so
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
    "tests/_source_pins.py": (
        # tests.test_source_pins covers the reader exhaustively, but a
        # behaviour change here (stripping too much, or too little) surfaces
        # as a pin failing in a consumer, so every consumer is listed. The
        # module decides what a whole-file source pin can SEE — silently
        # widening it puts every one of these back where #1172/#1186 found
        # them, satisfied by commented-out text.
        "tests.test_source_pins",
        "tests.test_daily_flake_update",
        "tests.test_deploy_cycle_verifier",
        "tests.test_deploy_hold",
        "tests.test_deploy_pin_script",
        "tests.test_docs_audit",
        "tests.test_fuzz_burst",
        "tests.test_issue_573_boundaries",
        "tests.test_issue_633_boundaries",
        "tests.test_js_suite_audit",
        "tests.test_nix_module",
        "tests.test_parallel_test_runner",
        "tests.test_startup_write_probe_generated",
        "tests.test_targeted_test_selection",
        "tests.test_test_tmpfs",
        "tests.test_unused_import_audit",
        "tests.test_world_model_burst",
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
    # Dispatch/import-lane cluster split out of tests/helpers.py (issue
    # #1278, "worth exploring" item 5). ``tests.test_dispatch_request`` is
    # the direct pin of the ``make_dispatch_request`` builder itself; the
    # other three drive the claim/handoff/finalize bridges and the dispatch
    # seam stubs through real production entry points.
    "tests/dispatch_helpers.py": (
        "tests.test_dispatch_request",
        "tests.test_dispatch_core",
        "tests.test_import_queue",
        "tests.test_integration_slices",
    ),
    # AlbumQualityEvidence-family builders split out of tests/helpers.py
    # (same #1278 item). The parity builders' consumers are the contract
    # (the hand-written parity tests and the generated parity property);
    # test_quality_decisions and test_dispatch_core are heavy real
    # consumers of make_audio_corrupt_validation_report and
    # make_album_quality_evidence respectively.
    "tests/evidence_helpers.py": (
        "tests.test_quality_classification",
        "tests.test_quality_generated",
        "tests.test_quality_decisions",
        "tests.test_dispatch_core",
    ),
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
    # The environment the three fake-command fixtures above hand their
    # subprocesses (issue #1313, 1329-2). This entry and the tests/fakes/
    # prefix rule are UNIONED, not overridden, so the prefix half already
    # contributes tests.test_fakes and the derived
    # tests.test_fakes_subprocess_env and neither is repeated here; what it
    # cannot reach is the three fixtures whose behaviour this module
    # decides. DERIVED, not curated: every test module importing any of
    # daily_flake_update, deploy_cycle, or deploy_pin, which is the union of
    # those three entries. Maskable — pinned in MASKABLE_ENTRY_PINS.
    "tests/fakes/subprocess_env.py": (
        "tests.test_daily_flake_update",
        "tests.test_daily_beets_tip_update",
        "tests.test_deploy_cycle_verifier",
        "tests.test_deploy_cycle_verifier_generated",
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
        "tests.test_dispatch_core",
        "tests.test_integration_slices",
        # ``make_socket_file``'s own end-to-end pin, including the
        # longer-than-``sun_path`` world that the daily gate's deeper
        # scratch root exposed and no local run reaches.
        "tests.test_path_authority",
    ),
    "tests/node_jsonl_worker.py": (
        "tests.test_node_jsonl_worker",
        "tests.test_generated_node_worker_audit",
    ),
    # Shared guard for every test that intentionally signals "the parent
    # process" (issue #1250). Not itself a discoverable test module, and
    # no prefix rule covers a bare tests/*.py file, so it needs an
    # explicit mapping: the direct unit/per-clause tests plus the
    # repo-wide bounded audit that rejects any OTHER unguarded getppid()
    # kill call site.
    "tests/parent_signal_guard.py": (
        "tests.test_parent_signal_guard",
        "tests.test_parent_signal_guard_audit",
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
    # Production web/*.py files whose real coverage lives under a test
    # module name ``_direct_test_candidates`` cannot derive (neither
    # tests.test_web_<stem> nor tests.web.test_<stem> exists on disk for
    # these). Without an explicit entry, a solo change to one of these
    # selects only the ambient audits — issue #1099 review found this
    # exact gap for wrong_match_file_service.py, whose real coverage is
    # spread across a dedicated classifier unit test, the route contract
    # tests, two generated composition/fuzz modules, and the render
    # differential's own render-target tests.
    "web/wrong_match_file_service.py": (
        "tests.test_wrong_match_file_service",
        "tests.web.test_routes_imports",
        "tests.test_path_authority_generated",
        "tests.test_protected_path_truth_generated",
        "tests.test_render_differential",
    ),
    # The queue projection's direct tests ARE probe-derivable
    # (tests.web.test_wrong_match_queue_view), but a solo change to the
    # projection also regresses the /api/wrong-matches contract, which
    # lives with the route module's tests — a name no probe can reach
    # from this path (#1278 "worth exploring" item 4).
    "web/wrong_match_queue_view.py": (
        "tests.web.test_wrong_match_queue_view",
        "tests.web.test_routes_imports",
    ),
    # web/discogs.py has no derivable neighbours: neither
    # tests.test_web_discogs nor tests.web.test_discogs exists on disk
    # (same mechanism as wrong_match_file_service.py above). It also
    # carries a coupling no derivation can see: the release cache-key
    # string is pinned by a bare literal in tests/test_web_dev_server.py,
    # and the #1262 v2→v3 bump shipped a RED tree that diff-derived
    # selection called green — both independent reviewers caught it,
    # selection could not (issue #1263 item 2). This list is a qualified
    # subset, not full coverage: several other modules import
    # web.discogs directly (test_discogs_fail_closed, the artist-bulk /
    # pressing-provenance / artist-compare generated modules) — the
    # entry names the modules whose subjects a solo web/discogs.py diff
    # most plausibly regresses, mutant-qualified at review time
    # (formats/status, artist releases, cache key, mirror concurrency).
    "web/discogs.py": (
        "tests.test_discogs_api",
        "tests.test_discogs_api_generated",
        "tests.test_web_dev_server",
        "tests.test_discogs_artist_concurrency",
    ),
    # web/server.py resolves nothing on its own: neither tests.test_server
    # nor tests.web.test_server exists (found while moving its module
    # globals onto WebRuntime, #1313). At the time, no ROOT_COVERAGE_RULES
    # row policed ``web/`` either, so the gap was doubly silent; #1355 item
    # 8 added that row, which now fails closed if this entry is ever
    # deleted (measured in MASKABLE_ENTRY_PINS — this path is no longer
    # pinned there). The named modules are the ones a solo change to this
    # file most plausibly regresses: the Handler's dispatch/keep-alive/
    # threading behaviour, the request-security envelope it applies before
    # dispatch, and main()'s admission ordering.
    "web/server.py": (
        "tests.web.test_server_endpoints",
        "tests.web.test_server_threading",
        "tests.web.test_server_cache",
        "tests.web.test_request_security",
        "tests.web.test_runtime",
        "tests.test_beets_config_startup",
    ),
    # web/runtime.py's own module IS probe-derivable
    # (tests.web.test_runtime), but the runtime is what every route reads,
    # so a solo change to it also regresses the per-thread handle contract
    # and the HTTP-level boundary tests that drive real requests through it.
    # tests.test_web_runtime_generated is named because the sibling
    # pairing cannot find it: expand_test_selection derives the
    # _generated name from the deterministic module it pairs with, and
    # this module's deterministic tests live at tests/web/test_runtime.py
    # while the property lives at the tests/ root (#1313 batch A).
    "web/runtime.py": (
        "tests.web.test_runtime",
        "tests.test_web_runtime_generated",
        "tests.web.test_server_threading",
        "tests.web.test_server_endpoints",
    ),
    # The nine entries below (including web/index_document.py, appended
    # after library_artist_service.py) were found by issue #1355 item 8's
    # population of WEB_MODULES_WITHOUT_SELECTION_COVERAGE: each resolved
    # zero neighbours (neither tests.test_web_<stem> nor
    # tests.web.test_<stem> exists), but each has real coverage under a
    # name the basename probe cannot derive. Every neighbour below was
    # verified by READING the referencing test — its own import statement
    # where one exists, the real call path where it does not (api_bases.py's
    # tests.test_pipeline_cli entry, and tests.test_web_dev_server, a named
    # deterministic sibling that imports nothing from the module itself) —
    # never by grepping the filename. web/overlay.py's stem collides with
    # the unrelated tests/test_overlay.py (which imports
    # web.routes._overlay, not web.overlay), so a bare tests.test_<stem>
    # probe is NOT trustworthy here and this file deliberately widens no
    # derived template to add it. (A pre-review draft of this same series
    # DID grep instead of reading for web/index_document.py, scoped to
    # tests/*.py only — missing tests/web/*.py entirely — and wrongly
    # admitted it as a registry gap; see that entry's own comment.)
    "web/classify.py": (
        # Verified: tests/test_classify_producer_audit.py imports
        # web.classify directly (Rule C's own producer audit for this
        # exact module).
        "tests.test_classify_producer_audit",
        "tests.test_classify_producer_audit_generated",
    ),
    "web/mb.py": (
        # Verified: tests/test_mb_api.py's import block reads
        # ``from web.mb import (...)``.
        "tests.test_mb_api",
    ),
    "web/artist_search.py": (
        # Verified: tests/test_artist_identity_search_generated.py imports
        # merge_exact_artist_identities from web.artist_search.
        "tests.test_artist_identity_search_generated",
    ),
    "web/download_history_view.py": (
        # Verified: both modules import build_recents_download_log_rows (or
        # its siblings) from web.download_history_view directly.
        "tests.test_web_recents",
        "tests.test_web_recents_generated",
    ),
    "web/api_bases.py": (
        # Verified by reading each import. tests/test_mb_artist_pagination_
        # generated.py imports PUBLIC_MB_WS2_BASE directly and asserts
        # against it. tests/test_web_dev_server_generated.py imports the
        # whole module, reads PUBLIC_MB_ORIGIN, and mutates
        # web.api_bases.PUBLIC_MB_WS2_BASE as a seam — its deterministic
        # sibling tests/test_web_dev_server.py is named alongside it.
        # tests/test_pipeline_cli.py::
        # test_non_quarantine_main_still_configures_mirror_api_bases is the
        # only one that reaches configure_api_bases_from_runtime_config
        # itself, the module's one process-startup wiring function (see its
        # own module docstring) — not a claim that it is the only name any
        # test imports from this module, which the first two entries below
        # disprove.
        "tests.test_mb_artist_pagination_generated",
        "tests.test_web_dev_server_generated",
        "tests.test_web_dev_server",
        "tests.test_pipeline_cli",
    ),
    "web/library_album_row.py": (
        # Verified: tests/test_library_album_row.py imports
        # LibraryAlbumRow from web.library_album_row.
        "tests.test_library_album_row",
    ),
    "web/library_album_detail_service.py": (
        # Verified: tests/test_library_album_detail_service.py's import
        # block reads ``from web.library_album_detail_service import``.
        "tests.test_library_album_detail_service",
        # Issue #1355 item 6: build_library_album_detail's upgrade_queued
        # key calls web.library_album_row._pipeline_upgrade_queued, the
        # shared owner also called by LibraryAlbumRow.from_pipeline_request
        # and .with_pipeline_request. The basename probe only ever looks
        # for tests.test_library_album_detail_service_generated (no such
        # module), so a solo edit here would otherwise select the parity
        # property's absence -- the exact list/detail drift this item
        # exists to prevent.
        "tests.test_library_album_row_generated",
    ),
    "web/library_artist_service.py": (
        # Verified: tests/test_library_artist_service.py's import block
        # reads ``from web.library_artist_service import``.
        "tests.test_library_artist_service",
    ),
    # web/index_document.py was WRONGLY admitted as a registry gap in an
    # earlier draft of this same change — a grep scoped to tests/*.py
    # (non-recursive) missed tests/web/*.py entirely (issue #1355 item 8
    # review, reader finding 1). tests/web/test_server_endpoints.py imports
    # render_index_document directly in two tests: a Hypothesis property
    # asserting the footer-selection logic tracks only explicit insecure
    # mode, and a pin on its RuntimeError for a duplicate footer marker.
    "web/index_document.py": (
        "tests.web.test_server_endpoints",
    ),
    # cratedigger.py is a single top-level file (``len(path.parts) == 1``),
    # so ``_direct_test_candidates`` looks for ``tests.test_cratedigger`` —
    # which does not exist; the module's behavior is split across dozens of
    # test files instead (test_slskd_searches.py, test_integration_slices.py,
    # test_search_max_inflight.py, ...). Without an explicit entry a solo
    # cratedigger.py change selects ZERO behavior modules, only ambient
    # audits. This is not a claim of full coverage — it closes the specific
    # gap issue #1112 review exposed: ``_submit_plan_search``'s strongest
    # pins (write-ahead ledger ordering, the exact 6-attempt/1-2-4-8-8s
    # budget, the widened 409+429 retryable-status set, the malformed-
    # response-body containment fix) live in test_slskd_searches.py, and
    # test_search_exec.py pins the shared ``submit_search_with_retry``
    # policy shape ``_submit_plan_search``'s own policy construction
    # depends on.
    # ``tests.test_cycle_startup`` closes a second, adjacent gap found by
    # #1313: the startup/cycle hand-off (``run_startup_and_cycle``,
    # ``build_cycle_collaborators``, ``_run_phase1``) had NO selected
    # behavior module at all, because the claims about it lived in
    # ``tests/test_convergence_runner_generated.py`` and nothing mapped
    # cratedigger.py to it.
    "cratedigger.py": (
        "tests.test_slskd_searches",
        "tests.test_search_exec",
        "tests.test_cycle_startup",
    ),
    # lib/dispatch/entry_points.py lives under lib/ so
    # _direct_test_candidates fires, but it only ever probes
    # tests.test_entry_points (derived from the basename, not the full
    # path) — which does not exist. Because the file is under lib/, not
    # tests/, the fail-closed check at the bottom of
    # _changed_path_neighbours (which only fires for an unmapped tests/
    # module) never catches the resulting under-selection either, so a
    # diff touching only this file silently selected zero of its real
    # behavior coverage (issue #1196 item 4, noticed during PR1 review for
    # #1178 but moot there after a redesign).
    #
    # Every listed module below was qualified by fault injection, not grep
    # alone: a ``raise RuntimeError`` planted as the first executable
    # statement of ``dispatch_import_from_db`` was run against each one.
    # ``tests.test_force_import_gates`` was the false positive that started
    # this correction (#1196 review round) — its only references to this
    # module are docstring lines saying its dispatch-via-legacy-branch
    # coverage MOVED OUT after the U4 importer-never-measures refactor
    # (2026-05-15-002); the planted mutant survives its full 29-test run
    # untouched, so it is NOT listed here.
    #
    # Killed by the planted mutant (real dynamic-execution coverage):
    # tests.test_dispatch_from_db is entry_points.py's own dedicated test
    # module (imports it directly as ``dispatch_entry_points_module``);
    # tests.test_force_import_merge_redirect and tests.test_import_manifest
    # import ``dispatch_import_from_db`` from ``lib.dispatch``'s re-export
    # and call it directly (test_import_manifest at 5 call sites);
    # tests.test_integration_slices and tests.test_import_queue exercise it
    # through a real dispatch/executor path — test_import_queue's
    # ``TestImporterWorker`` end-to-end tests
    # (test_corrupt_force_action_bans_and_deletes_wrong_match_source,
    # test_force_action_manifest_drift_requeues_before_terminal_audit,
    # test_force_import_extra_audio_keeps_wm_and_operator_status_end_to_end,
    # ...) drive a real worker/executor loop against the real function; the
    # file's many ``patch("lib.dispatch.dispatch_import_from_db", ...)``
    # targets belong to OTHER, unit-level test classes in the same file
    # that bypass it deliberately.
    #
    # tests.test_issue_573_boundaries is a genuine consumer — it parses
    # this file's own AST and pins the exact keyword-argument shape of its
    # call to ``dispatch_import_core`` — but it is a STATIC structural
    # audit that never executes ``dispatch_import_from_db``, so the planted
    # runtime mutant does NOT kill it (confirmed: exit 0, all 9 tests pass
    # with the mutant live). Kept here anyway because it is still real,
    # valuable coverage of a different regression class at this exact call
    # site (a dropped/positional-ised kwarg), not because it substitutes
    # for the dynamic-execution coverage above.
    #
    # Verified-but-expensive consumers deliberately excluded from this list
    # for selection cost (each independently confirmed to kill the same
    # planted mutant, so the exclusion is a cost decision, not a coverage
    # gap): tests.test_pipeline_db (real-PostgreSQL, 563 tests),
    # tests.test_spectral_attempt_audit_generated, tests.test_merge_rekey_generated
    # (indirectly, via tests.test_force_import_merge_redirect.force_dispatch),
    # and tests.world_model.state_machine (indirectly, via
    # tests/world_model/support.py's LifecycleWorld.force_import_request,
    # real-PostgreSQL + real Beets, ~33s).
    #
    # Two more additions, issue #1246 item 1: PR #1245 added
    # tests.test_dispatch_outcomes_generated's
    # ``TestGeneratedLaneDistanceAudit`` specifically to patrol the
    # ``distance_threshold is not None`` lane discriminator this file's
    # ``_dispatch_import_from_db_locked`` uses to decide whether the
    # accept-path terminal audit records a measured distance or NULL
    # (issue #1211) — and tests.test_local_import_lane, which pins the
    # caller-side (``scripts/importer.py``) contract that discriminator
    # depends on as an invariant (which lane passes which threshold).
    # Measured on 2026-08-22 (this issue's own repro): before this entry,
    # editing only this file selected neither. Qualified by fault
    # injection, flipping the discriminator's ``is not None`` to
    # ``is None``: tests.test_dispatch_outcomes_generated's
    # ``TestGeneratedLaneDistanceAudit`` DOES kill it (real dynamic
    # execution through the real ``dispatch_import_from_db`` — confirmed
    # RED, ``0.42 != None`` on the force/present cell). tests.test_local_
    # import_lane does NOT kill it (confirmed: exit 0, all 9 tests pass
    # with the mutant live) — every one of its ``execute_import_job``
    # calls injects ``force_dispatch_fn=<recorder>``, so it never runs
    # this file's real code at all. Kept anyway, for the same reason as
    # tests.test_issue_573_boundaries above: it is real, valuable coverage
    # of a different regression class — the caller-side pairing this
    # discriminator's own correctness assumes as a precondition (only
    # local-import's caller ever passes a non-None threshold, and only
    # after its own strict-validation guard already passed) — not a
    # substitute for the dynamic-execution coverage above.
    # lib/dispatch/types.py holds the dispatch interface itself — constants,
    # value carriers, ``DispatchRequest``, the ``DispatchDB`` port, and the
    # two callable seams. ``_direct_test_candidates`` derives
    # ``tests.test_types`` from the basename, which does not exist. The
    # dedicated deterministic contract is tests.test_dispatch_types; the
    # request/core and generated outcome suites remain complementary caller
    # and world-space coverage, not substitutes for that durable boundary.
    "lib/dispatch/types.py": (
        "tests.test_dispatch_types",
        "tests.test_dispatch_request",
        "tests.test_dispatch_core",
        "tests.test_dispatch_outcomes_generated",
    ),
    "lib/dispatch/entry_points.py": (
        "tests.test_dispatch_from_db",
        "tests.test_force_import_merge_redirect",
        "tests.test_integration_slices",
        "tests.test_import_manifest",
        "tests.test_import_queue",
        "tests.test_issue_573_boundaries",
        "tests.test_dispatch_outcomes_generated",
        "tests.test_local_import_lane",
    ),
    # harness/beets_compat.py's basename probe resolves its own era pins,
    # but the duplicates-query seam's composition pin — the real harness
    # driving album_duplicates_query — lives here (#1278 wx6).
    "harness/beets_compat.py": (
        "tests.test_harness_duplicate_lookup",
    ),
    # harness/discogs_patches.py has no basename-matched test module: its
    # coverage is the Discogs subtrack/heading/cover-art families that
    # moved with it out of beets_compat.py (#1278 wx6).
    "harness/discogs_patches.py": (
        "tests.test_discogs_subtracks",
        "tests.test_discogs_subtracks_generated",
        "tests.test_discogs_subtracks_e2e",
        "tests.test_discogs_cover_art_fallback",
        "tests.test_discogs_cover_art_fallback_generated",
    ),
    # harness/import_one.py has NO basename probe: _direct_test_candidates
    # covers lib/, scripts/ and top-level files, and a harness/ path matches
    # none of them, so before this list the file resolved to exactly
    # tests.test_disambiguation plus the harness/ prefix rule's
    # tests.test_harness_beets2_contract — editing the privileged Beets
    # mutation child ran neither its 250-test stage module nor its
    # --force tests (#1313, the argv-adapter item). The entry that used to
    # sit here claimed those probes existed; they never did.
    #
    # test_disambiguation also pins the SECOND harness pass's argv
    # (--preserve-discogs-flat-subtracks on the retry) — exactly the
    # construction lib/beets_child.py::harness_session_argv now owns
    # (#1278 item 4, PR 2 review) — and, since #1313, the apply-time
    # distance ceiling's only apply-versus-reject coverage.
    "harness/import_one.py": (
        "tests.test_disambiguation",
        "tests.test_import_one_stages",
        "tests.test_import_one_request_generated",
        "tests.test_import_one_argparse_audit",
        "tests.test_force_import",
    ),
    # lib/quality/wire_types.py holds the harness wire Structs (#1278
    # item 8). The lib/quality/ prefix rule selects only the quality
    # decision tests; the required/optional split's construction pins and
    # the decode-boundary consumers live in these three (the key-set audit
    # itself is ambient — test_*_audit discovery — and needs no entry).
    "lib/quality/wire_types.py": (
        "tests.test_validation_result",
        "tests.test_beets_validation",
        "tests.test_beets_harness_session",
    ),
    # lib/surface_outcomes.py is the repository-wide status/exit
    # convention (#1278): a change to it must run the services whose exit
    # maps derive from it and the CLI relay adapters, not only its own
    # unit tests — basename probing alone selected exactly
    # tests.test_surface_outcomes (founding-PR reader finding R8).
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
    # lib/beets_child.py is the shared spawner UNDER the three
    # run-to-completion Beets mutation lanes (#1278 item 4). Its own two
    # modules arrive via the basename probes; the lane suites — one of them
    # the destructive delete child's — are what prove a spawner change did
    # not move a lane outcome, the same reasoning as lib/dispatch/types.py
    # above (#1277).
    "lib/beets_child.py": (
        "tests.test_beets_delete",
        "tests.test_beets_retag",
        "tests.test_beets_tag_sync",
        "tests.test_merge_rekey",
    ),
    # lib/discogs_positions.py has no tests.test_discogs_positions module:
    # its deterministic pins live with its two production consumers (the
    # get_release adapter pins in tests.test_discogs_api, the search-worker
    # fallback-writer pins in tests.test_album_source) and its generated
    # properties in tests.test_discogs_api_generated (issue #1261).
    "lib/discogs_positions.py": (
        "tests.test_discogs_api",
        "tests.test_discogs_api_generated",
        "tests.test_album_source",
    ),
    # scripts/importer.py and scripts/import_preview_worker.py live under
    # scripts/, so _direct_test_candidates probes tests.test_importer and
    # tests.test_import_preview_worker respectively -- neither exists, so
    # both resolved ZERO real neighbours. Unlike the lib/ case above,
    # scripts/ is not covered by _changed_path_neighbours's fail-closed
    # check either (it only fires for path.parts[:1] == ("lib",)), so
    # this was SILENT under-selection, not an admitted gap -- and it
    # meant tests.test_import_queue's TestCleanupTerminalForceAction
    # FailsClosed did not run when its subject, scripts/importer.py,
    # changed.
    #
    # Method: a ``raise RuntimeError`` planted as the first executable
    # statement of each file's own central, every-job-type entry point
    # (``process_claimed_job`` for importer.py -- "the single
    # queue-outcome mapper all four job types route through", per its
    # own docstring; ``process_claimed_preview_job`` for import_preview_
    # worker.py, its direct analogue), run against every module found by
    # grepping for real imports of the module under test, PLUS every
    # module that reaches the entry point indirectly through
    # tests/dispatch_helpers.py's finalize_claimed_dispatch bridge (a
    # search grep alone cannot find, since those modules never spell the
    # module's own name). This list is a QUALIFIED SUBSET chosen for coverage value,
    # NOT a complete kill set -- an import-name grep is structurally
    # incomplete for the bridge-reached case, and other real consumers
    # likely exist beyond what either search turned up. Preference order
    # among confirmed killers: a generated property whose own subject is
    # behavior these two files own outranks a deterministic slice that
    # merely passes through them on the way to exercising something else.
    #
    # importer.py -- confirmed killed and included: tests.test_import_
    # dispatch, tests.test_import_operation_fence, tests.test_import_
    # queue (this file's own pin lives here), tests.test_integration_
    # slices, tests.test_local_import_lane, tests.test_terminal_outcomes,
    # tests.test_dispatch_outcomes_generated (patrols this file's own
    # lane discriminator -- see the entry_points.py entry above),
    # tests.test_force_import_service_generated, tests.test_import_job_
    # lifecycle_generated, tests.test_processing_lifecycle_generated,
    # tests.test_spectral_attempt_audit_generated, tests.test_wrong_
    # match_post_commit_generated. Confirmed killed but deliberately
    # excluded for selection cost or relevance, mirroring the entry_
    # points.py entry's own precedent: tests.test_pipeline_db
    # (real-PostgreSQL, 574 tests, ~23s -- the two failures it produces
    # under the mutant are indirect, a real child process failing to
    # reach an expected barrier, not a direct assertion on the mutant,
    # confirmed genuine by reverting the mutant and observing both pass
    # cleanly at baseline); tests.test_dispatch_core (a deterministic
    # slice whose own subject is lib/dispatch/core.py's orchestration,
    # reached via the finalize_claimed_dispatch bridge -- it touches this
    # file rather than patrolling behavior this file owns). Confirmed NOT
    # killed (real imports exist but exercise other surfaces of the
    # module, or bypass this entry point via their own DI seams):
    # tests.test_importer_graceful_shutdown, tests.test_merge_rekey,
    # tests.test_importer_runtime_context, tests.test_automation_
    # startup_recovery, tests.test_beets_config_startup.
    #
    # import_preview_worker.py -- confirmed killed and included:
    # tests.test_import_queue, tests.test_integration_slices (both
    # already listed above for importer.py -- real, independent coverage
    # of the preview worker's own module, not double-counted),
    # tests.test_issue_1030_postgres_slice, tests.test_terminal_outcome_
    # callers, tests.test_evidence_generated, tests.test_path_authority_
    # generated, tests.test_preview_failure_evidence_generated,
    # tests.test_spectral_attempt_audit_generated (kills both this
    # file's mutant and importer.py's -- real, independent coverage of
    # each). Confirmed NOT killed: tests.test_import_preview,
    # tests.test_import_result, tests.test_beets_config_startup_
    # entrypoints.
    # tests.test_importer_job_kinds (issue #1278) was added with the
    # per-kind adapter registry it pins; the basename probe still resolves
    # nothing for this path, so it is named here like every other neighbour.
    "scripts/importer.py": (
        "tests.test_importer_job_kinds",
        "tests.test_import_dispatch",
        "tests.test_import_operation_fence",
        "tests.test_import_queue",
        "tests.test_integration_slices",
        "tests.test_local_import_lane",
        "tests.test_terminal_outcomes",
        "tests.test_dispatch_outcomes_generated",
        "tests.test_force_import_service_generated",
        "tests.test_import_job_lifecycle_generated",
        "tests.test_processing_lifecycle_generated",
        "tests.test_spectral_attempt_audit_generated",
        "tests.test_wrong_match_post_commit_generated",
    ),
    # tests.test_preview_claim_routes (issue #1313) was added with the
    # per-job-type claim-route registry it pins — the preview lane's twin of
    # tests.test_importer_job_kinds above. The basename probe still resolves
    # nothing for this path, so it is named here like every other neighbour.
    "scripts/import_preview_worker.py": (
        "tests.test_preview_claim_routes",
        "tests.test_import_queue",
        "tests.test_integration_slices",
        "tests.test_issue_1030_postgres_slice",
        "tests.test_terminal_outcome_callers",
        "tests.test_evidence_generated",
        "tests.test_path_authority_generated",
        "tests.test_preview_failure_evidence_generated",
        "tests.test_spectral_attempt_audit_generated",
    ),
    # Issue #1248: 10 of the 11 scripts/*.py files that resolve zero
    # neighbours have real coverage -- `_direct_test_candidates` only ever
    # probes `tests.test_<basename>`, which misses coverage filed under a
    # differently-named test module or reached only via subprocess/dynamic
    # load. Each entry below was verified by reading the referencing test
    # file, not just grepping the filename (a plain grep for several of
    # these names alone would have produced false positives from comment/
    # docstring mentions or from the string, unrelated
    # lib.beets_config_contract.check_beets_config function that
    # scripts/check_beets_config.py itself imports and wraps).
    "scripts/audit_issue_references.py": (
        # tests.test_issue_reference_contract:10 --
        # "from scripts.audit_issue_references import find_closing_issue_references"
        "tests.test_issue_reference_contract",
    ),
    "scripts/check_beets_config.py": (
        # tests.test_beets_config_contract_integration launches this file
        # as a real subprocess (TestStandaloneBeetsConfigChecker._run,
        # `subprocess.run([sys.executable, "scripts/check_beets_config.py",
        # ...])`) and separately imports its CheckerResult wire type.
        # tests.test_beets_config_startup_generated does the same real
        # subprocess launch to assert redacted-secret behaviour on a load
        # failure. Neither is the unrelated
        # lib.beets_config_contract.check_beets_config function this
        # script imports and wraps -- that shared function's own ~90 call
        # sites across test_beets_config_contract*.py and
        # test_harness_beets2_contract.py exercise
        # lib/beets_config_contract.py, not this file.
        "tests.test_beets_config_contract_integration",
        "tests.test_beets_config_startup_generated",
    ),
    "scripts/cratedigger_deploy_hold.py": (
        # tests.test_deploy_hold: "import scripts.cratedigger_deploy_hold
        # as deploy_hold_module" + a real-module import block.
        # tests.test_deploy_hold_generated:
        # "from scripts.cratedigger_deploy_hold import (...)".
        "tests.test_deploy_hold",
        "tests.test_deploy_hold_generated",
    ),
    "scripts/plex_dupes_audit.py": (
        # tests.test_plex_dupes_scripts: "from scripts import
        # plex_dupes_audit, plex_dupes_merge" plus real calls
        # (plex_dupes_audit.fetch_children, ._parse_children_xml,
        # ._load_albums) -- the one test module covering both plex_dupes
        # scripts, contrary to this issue's own opening measurement, which
        # assumed the plex_dupes_* pair was a genuine gap without reading
        # this file first.
        "tests.test_plex_dupes_scripts",
    ),
    "scripts/plex_dupes_merge.py": (
        # Same tests.test_plex_dupes_scripts module -- real call:
        # plex_dupes_merge.merge("1", ["2"], "merge-token").
        "tests.test_plex_dupes_scripts",
    ),
    "scripts/refresh_beets_compat_releases.py": (
        # Both test modules load this file via
        # importlib.util.spec_from_file_location (not a dotted import --
        # presumably because the module needs loading under two distinct
        # synthetic names, "beets_compat_releases" and
        # "beets_compat_releases_generated", for the deterministic/
        # generated split) and execute it for real.
        "tests.test_beets_compat_releases",
        "tests.test_beets_compat_releases_generated",
    ),
    "scripts/run_fuzz_tests.py": (
        # tests.test_fuzz_burst: "from scripts.run_fuzz_tests import
        # (...)" plus RUNNER = .../run_fuzz_tests.py driving real
        # subprocess bursts. tests.test_parallel_test_runner: "from
        # scripts.run_fuzz_tests import (...)".
        # tests.test_unused_import_audit.py and
        # tests.test_hypothesis_profile_audit.py also mention this
        # filename but only as a string/comment (a TID251 ruff-rule entry
        # keyed by path, and prose citing where a real bug lived) --
        # neither imports or executes the module, so neither is listed.
        "tests.test_fuzz_burst",
        "tests.test_parallel_test_runner",
    ),
    "scripts/run_library_completeness_census.py": (
        # tests.test_library_completeness_snapshot: "from scripts import
        # run_library_completeness_census as census" +
        # "from scripts.run_library_completeness_census import
        # publish_library_completeness_census" -- fault-injection
        # confirmed (a runtime raise planted as the first statement of
        # publish_library_completeness_census fails this module with 2
        # errors). tests.test_beets_config_startup_entrypoints: real
        # subprocess exec, "subprocess.run([sys.executable,
        # 'scripts/run_library_completeness_census.py', ...])", but
        # against a deliberately invalid config -- it proves the module's
        # main()/config-load path fails closed before
        # publish_library_completeness_census ever runs, so the SAME
        # mutant above does NOT reach it (confirmed: 6/6 pass with the
        # mutant live). Kept as a real, independent neighbour for the
        # startup/config-load code this module owns, not because it
        # kills every mutant in the census body.
        "tests.test_library_completeness_snapshot",
        "tests.test_beets_config_startup_entrypoints",
    ),
    "scripts/run_world_model_burst.py": (
        # tests.test_world_model_coordinator:
        # "from scripts.run_world_model_burst import (...)" including
        # build_targets, called directly at 5 sites -- fault-injection
        # confirmed (a runtime raise planted as the first statement of
        # build_targets fails this module with 3 failures/5 errors).
        # tests.test_ephemeral_pg only imports the IN_PROCESS_JOB_CAP
        # constant, never a function -- confirmed it does NOT kill the
        # same mutant (14/14 pass with the mutant live). Kept anyway: it
        # is real, independent module-level-import coverage (an
        # import-time break in this file would still fail it), just not
        # of build_targets specifically. tests.test_world_model_burst.py,
        # tests.test_negative_coverage_audit.py, and
        # tests.test_fuzz_burst.py also mention this filename but only in
        # a string literal/comment/docstring, never a real import, so
        # none of the three is listed.
        "tests.test_world_model_coordinator",
        "tests.test_ephemeral_pg",
    ),
    "scripts/world_audit_debt_gate.py": (
        # tests.test_world_audit_debt:
        # "from scripts.world_audit_debt_gate import run".
        "tests.test_world_audit_debt",
    ),
    # scripts/pipeline_cli/*.py: 19 of the 20 files in this package
    # (everything except beets_distance.py, whose basename-derived
    # candidate tests.test_beets_distance already exists) resolve zero
    # neighbours -- _direct_test_candidates derives tests.test_<basename>
    # from ONLY the basename, ignoring the pipeline_cli/ subdirectory
    # component entirely, the same nested-lib/ shape lib/dispatch/core.py
    # had under #1199. Of those 19, __main__.py is the one genuine gap
    # (its own SCRIPTS_MODULES_WITHOUT_SELECTION_COVERAGE entry, defined
    # further down this file, has the rationale); the other 18 get real
    # EXACT_PATH_NEIGHBOURS entries below.
    # tests.test_pipeline_cli.py (8k+ lines) is this
    # package's real, dedicated coverage: scripts/pipeline_cli/__init__.py
    # re-exports every cmd_* handler, and tests.test_pipeline_cli.py does
    # "from scripts import pipeline_cli" then calls
    # pipeline_cli.cmd_<name>(db, args) directly -- the SAME function
    # object the owning family module defines (Python re-export is a name
    # binding, not a copy), so the call is real execution of that module's
    # code. Verified by grep for a direct call to at least one cmd_*
    # (or, for cli.py, pipeline_cli.main(...); for routes_meta.py,
    # _build_parser()) owned by each module below, confirmed present in
    # tests.test_pipeline_cli.py. _format.py has no direct call of its own
    # -- its own docstring names it as shared helpers used by query, show,
    # quality, search_plan, triage, replace, beets_distance, and long_tail,
    # every one of which IS called directly above, so _format.py's code
    # runs for real whenever any of those command tests run.
    #
    # TWO CARVE-OUTS from the "direct cmd_* call" claim above, both owning
    # no cmd_* of the shape just described:
    # - api_mutations.py: its own six cmd_* handlers are NEVER called by
    #   tests.test_pipeline_cli (fault-injection confirmed: a mutant in
    #   all six survives at exit 0). What IS real there is
    #   api_mutations._post, a shared helper OTHER modules' handlers call
    #   through -- see its own entry below for the exact evidence and
    #   where the six handlers themselves ARE covered.
    # - __init__.py: it owns no cmd_* of its own at all (only re-exports).
    #   Its coverage is import-level -- "from scripts import pipeline_cli"
    #   runs this file's top-level code (the imports + __all__ list)
    #   merely by being imported, which every test in
    #   tests.test_pipeline_cli.py does at module load.
    "scripts/pipeline_cli/__init__.py": (
        "tests.test_pipeline_cli",
    ),
    "scripts/pipeline_cli/_format.py": (
        "tests.test_pipeline_cli",
    ),
    "scripts/pipeline_cli/album_requests.py": (
        "tests.test_pipeline_cli",
    ),
    "scripts/pipeline_cli/api_mutations.py": (
        # NOT covered by tests.test_pipeline_cli the way the general block
        # comment above describes for its siblings: this module's own six
        # cmd_* handlers (cmd_merge_rekey, cmd_pipeline_delete,
        # cmd_resolve_rg, cmd_set_quality, cmd_upgrade,
        # cmd_wrong_match_converge) are NEVER called there -- confirmed by
        # fault injection, a mutant raising in all six survives
        # tests.test_pipeline_cli at exit 0. What tests.test_pipeline_cli
        # DOES exercise for real is api_mutations._post, the shared HTTP
        # POST helper OTHER family modules' handlers call through -- e.g.
        # wrong_match.py's cmd_wrong_match_triage /
        # cmd_wrong_match_triage_cancel, tested at
        # tests/test_pipeline_cli.py:1409-1454 and :1741-1757 via
        # "real_post = api_mutations._post" then
        # patch.object(api_mutations, "_post", <wrapper calling real_post>)
        # -- fault-injection confirmed (a mutant in _post fails
        # tests.test_pipeline_cli). The six handlers themselves are
        # covered by tests.test_pipeline_cli_api_mutations: its _run()
        # helper dict-dispatches all six by name (TestApiMutationCli._run,
        # exercised by
        # test_each_command_preserves_canonical_method_path_and_body's
        # six-case loop plus several other tests) and its generated
        # sibling.
        "tests.test_pipeline_cli",
        "tests.test_pipeline_cli_api_mutations",
        "tests.test_pipeline_cli_api_mutations_generated",
    ),
    "scripts/pipeline_cli/audit.py": (
        "tests.test_pipeline_cli",
    ),
    "scripts/pipeline_cli/cli.py": (
        "tests.test_pipeline_cli",
        "tests.test_pipeline_cli_api_mutations",
    ),
    "scripts/pipeline_cli/destructive.py": (
        "tests.test_pipeline_cli",
    ),
    "scripts/pipeline_cli/imports.py": (
        "tests.test_pipeline_cli",
        "tests.test_pipeline_cli_api_mutations",
    ),
    "scripts/pipeline_cli/long_tail.py": (
        "tests.test_pipeline_cli",
    ),
    "scripts/pipeline_cli/quality.py": (
        "tests.test_pipeline_cli",
    ),
    "scripts/pipeline_cli/query.py": (
        "tests.test_pipeline_cli",
    ),
    "scripts/pipeline_cli/replace.py": (
        "tests.test_pipeline_cli",
    ),
    "scripts/pipeline_cli/routes_meta.py": (
        "tests.test_pipeline_cli",
        "tests.test_pipeline_cli_api_mutations",
    ),
    "scripts/pipeline_cli/search_plan.py": (
        "tests.test_pipeline_cli",
    ),
    "scripts/pipeline_cli/show.py": (
        "tests.test_pipeline_cli",
    ),
    "scripts/pipeline_cli/triage.py": (
        "tests.test_pipeline_cli",
    ),
    "scripts/pipeline_cli/wrong_match.py": (
        "tests.test_pipeline_cli",
        "tests.test_pipeline_cli_api_mutations",
    ),
    "scripts/pipeline_cli/youtube.py": (
        "tests.test_pipeline_cli",
        "tests.test_pipeline_cli_api_mutations",
    ),
    # scripts/**/*.sh (issue #1278 item 9). The sixteen shell wrappers had
    # no fail-closed selection story at all: `_direct_test_candidates` only
    # ever probed .py paths, so any wrapper without a hand-written entry
    # resolved zero neighbours SILENTLY -- thirteen of the sixteen,
    # measured 2026-08-31. scripts/run_final_gate.sh is the instance that
    # was actually paid for: on main's history the ONLY commit that ever
    # added its entry is item 6's PR2 (0c3bae8e), the same commit that made
    # it a wrapper -- so through item 6's PR1 and everything earlier, an
    # edit to that file selected nothing. The scripts/ root rule now
    # polices `.sh` too, and the same basename probe resolves five of those
    # thirteen (daily_beets_tip_update, daily_flake_update,
    # daily_resource_monitor, fuzz_burst, world_model_burst -- each has a
    # real tests/test_<stem>.py; test_tmpfs.sh is a sixth probe-resolved
    # wrapper, but it had an entry and so was never among the thirteen).
    # The six below need an explicit entry, and
    # scripts/lint.sh + scripts/mcp-playwright.sh are admitted gaps in
    # SCRIPTS_MODULES_WITHOUT_SELECTION_COVERAGE.
    #
    # Every entry was verified by READING the referencing test, never by
    # grepping the filename: for each, the named module either executes the
    # wrapper as a real subprocess or reads its source and asserts against
    # its content. A filename in a comment, docstring, or an assertion on
    # some OTHER file's command string does not qualify -- which is why
    # tests.test_final_gate_receipt is absent from the run_tests.sh entry
    # (it asserts the literal "bash scripts/run_tests.sh" as the GATE's
    # argv, never reading this file), and tests.test_suite_coordinator is
    # absent from find_dead_code.sh / run_ruff.sh (it likewise pins only
    # the coordinator's own command tuples).
    "scripts/find_dead_code.sh": (
        # tests.test_unused_import_audit's run_full_dead_code_gate and
        # run_vulture_freshness_world both `subprocess.run(["bash",
        # <this file>])` for real, and two of its tests read this file's
        # source and plant mutants in it (the freshness-wiring call, the
        # --make-whitelist invocation).
        "tests.test_unused_import_audit",
    ),
    "scripts/pin_nixosconfig.sh": (
        # Both modules run the real script through the deploy-pin fake
        # command harness, which `subprocess.run([str(script), ...])`s the
        # path it is handed -- `SCRIPT = REPO_ROOT/"scripts"/
        # "pin_nixosconfig.sh"`, then `fake.run(SCRIPT)` / `fake.popen(
        # SCRIPT)` at dozens of sites. The deterministic module also reads
        # this file's own source for its shell-contract audit (shebang,
        # zero contract violations, `flock 9` before `worktree add`).
        "tests.test_deploy_pin_script",
        "tests.test_deploy_pin_generated",
    ),
    "tests/js_harness.mjs": (
        # The shared JavaScript test harness (issue #1313 candidate 6).
        # No ROOT_COVERAGE_RULES row polices `.mjs` -- the `tests/` row
        # covers `.py` only -- and there is no basename probe that could
        # reach a `.mjs` file, so without this entry editing the harness
        # selects nothing but the always-discovered audits. The two named
        # modules are the ones a solo harness change really regresses:
        # test_js_suite_audit parses every suite for the harness idiom,
        # and test_suite_coordinator owns the CRATEDIGGER_JS_FAILURE
        # marker contract this module emits (identity -> owner + rerun
        # derivation, and the done-marker fallback rule). The harness's
        # own behaviour tests live in tests/test_js_harness.mjs, which the
        # js-unit phase runs unconditionally.
        # Maskable -- pinned in MASKABLE_ENTRY_PINS.
        "tests.test_js_suite_audit",
        "tests.test_suite_coordinator",
    ),
    "scripts/run_js_checks.sh": (
        # tests.test_js_suite_audit parses this file's source to prove
        # every tests/test_js_*.mjs suite is actually reached.
        # tests.test_suite_coordinator executes it for real in both modes
        # (`[str(JS_HELPER), mode]`), against a fake node and once against
        # the real one.
        "tests.test_js_suite_audit",
        "tests.test_suite_coordinator",
    ),
    "scripts/run_ruff.sh": (
        # tests.test_unused_import_audit runs `bash scripts/run_ruff.sh`
        # for real (ruff_findings, run_ruff_gate, the TID251 stdin
        # control) and plants a non-enforcing mutant in a copy of its
        # source.
        "tests.test_unused_import_audit",
    ),
    "scripts/run_tests.sh": (
        # All three read this file's source and pin a distinct property of
        # it: test_js_suite_audit that it still reaches the coordinator,
        # test_parallel_test_runner the exact `exec python3
        # scripts/run_test_suite.py` line, test_world_model_burst that the
        # standard suite runs neither burst script nor the world-model
        # module directly. tests.test_unused_import_audit pins the same
        # coordinator line the first two already cover and is excluded for
        # cost (it runs real Ruff and Vulture subprocesses).
        "tests.test_js_suite_audit",
        "tests.test_parallel_test_runner",
        "tests.test_world_model_burst",
    ),
    "scripts/verify_cratedigger_cycle.sh": (
        # Both modules run the real verifier through the deploy-cycle fake
        # command harness, which `subprocess.run([str(script), *args])`s
        # the path it is handed (`fake.run(SCRIPT, "capture-migrate")`,
        # `fake.run(SCRIPT, "verify-migrate-ran", ...)`, ...). Neither
        # reads this file's source -- their only `pinned_source` calls
        # target the deploy SKILL, not the verifier -- so the coverage
        # here is real execution, nothing else.
        "tests.test_deploy_cycle_verifier",
        "tests.test_deploy_cycle_verifier_generated",
    ),
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
#: naming a registered module — by its dotted path, or by the repository's
#: sanctioned bare-leaf `sys.path.append(dirname(__file__))` convention —
#: fails the audit, naming both the registry entry and the importing file
#: (issue #1095).
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


#: Changed `lib/**/*.py` files whose full neighbour resolution (EXACT_PATH_
#: NEIGHBOURS + prefix rules + direct candidates that actually exist) yields
#: ZERO test modules — the lib/ twin of SHARED_MODULES_WITHOUT_COVERAGE
#: (issue #1199 item 1, the durable fix behind #1196 item 4). Unlike the
#: tests/-side registry, _changed_path_neighbours does NOT early-return for
#: a path listed here: the full resolution still runs, so a module that
#: later gains real neighbours (an EXACT_PATH_NEIGHBOURS entry, a new prefix
#: rule, or a newly-created tests.test_<stem> module) SELECTS them
#: immediately, on its very next diff, with no code change here — but the
#: registration itself goes stale the moment that happens and the entry
#: must then be deleted, which tests/test_selection_coverage_audit.py
#: enforces (issue #1199 review F1: caught by adding a real test module for
#: a registered path and observing the audit go RED demanding removal — a
#: prior version of this comment claimed "no code change needed to un-admit
#: it" at all, which was false; selection self-corrects, the registry does
#: not). tests/test_selection_coverage_audit.py proves both directions: a
#: registered path that now resolves neighbours (stale — must be removed),
#: and a lib/**/*.py file with zero neighbours that is NOT registered here
#: (must be added, or given real coverage). Population is a fresh
#: measurement (driving the real resolution function), never hand-curated —
#: hand-curated neighbour lists are exactly the mechanism that produced
#: #1196's own false-claim correction round.
LIB_MODULES_WITHOUT_SELECTION_COVERAGE: dict[str, str] = {
    "lib/artist_catalogue.py": (
        "measured 2026-08-19: zero neighbours -- tests.test_artist_catalogue "
        "does not exist and no EXACT_PATH_NEIGHBOURS/prefix rule covers it "
        "(issue #1199)"
    ),
    "lib/banding.py": (
        "measured 2026-08-19: zero neighbours -- tests.test_banding does "
        "not exist and no EXACT_PATH_NEIGHBOURS/prefix rule covers it "
        "(issue #1199)"
    ),
    "lib/beets_startup.py": (
        "measured 2026-08-19: zero neighbours -- tests.test_beets_startup "
        "does not exist and no EXACT_PATH_NEIGHBOURS/prefix rule covers it "
        "(issue #1199)"
    ),
    "lib/destructive_release_service.py": (
        "measured 2026-08-19: zero neighbours -- "
        "tests.test_destructive_release_service does not exist and no "
        "EXACT_PATH_NEIGHBOURS/prefix rule covers it (issue #1199)"
    ),
    "lib/dispatch/__init__.py": (
        "measured 2026-08-19: zero neighbours -- _direct_test_candidates "
        "derives tests.test___init__ from the basename, which does not "
        "exist and no EXACT_PATH_NEIGHBOURS/prefix rule covers it "
        "(issue #1199)"
    ),
    "lib/dispatch/core.py": (
        "measured 2026-08-19: zero neighbours -- _direct_test_candidates "
        "derives tests.test_core from the basename (ignoring the dispatch/ "
        "subdirectory), which does not exist; no EXACT_PATH_NEIGHBOURS/"
        "prefix rule covers it either (issue #1199)"
    ),
    "lib/dispatch/helpers.py": (
        "measured 2026-08-19: zero neighbours -- basename-derived "
        "tests.test_helpers does not exist and no EXACT_PATH_NEIGHBOURS/"
        "prefix rule covers it (issue #1199)"
    ),
    "lib/dispatch/manifest_guard.py": (
        "measured 2026-08-19: zero neighbours -- basename-derived "
        "tests.test_manifest_guard does not exist and no EXACT_PATH_"
        "NEIGHBOURS/prefix rule covers it (issue #1199)"
    ),

    "lib/dispatch/post_import.py": (
        "measured 2026-08-19: zero neighbours -- basename-derived "
        "tests.test_post_import does not exist and no EXACT_PATH_"
        "NEIGHBOURS/prefix rule covers it (issue #1199)"
    ),

    "lib/download_materialization.py": (
        "measured 2026-08-19: zero neighbours -- "
        "tests.test_download_materialization does not exist and no "
        "EXACT_PATH_NEIGHBOURS/prefix rule covers it (issue #1199)"
    ),
    "lib/download_processing.py": (
        "measured 2026-08-19: zero neighbours -- "
        "tests.test_download_processing does not exist and no EXACT_PATH_"
        "NEIGHBOURS/prefix rule covers it (issue #1199)"
    ),
    "lib/download_reconstruction.py": (
        "measured 2026-08-19: zero neighbours -- "
        "tests.test_download_reconstruction does not exist and no "
        "EXACT_PATH_NEIGHBOURS/prefix rule covers it (issue #1199)"
    ),
    "lib/download_rejection.py": (
        "measured 2026-08-19: zero neighbours -- "
        "tests.test_download_rejection does not exist and no EXACT_PATH_"
        "NEIGHBOURS/prefix rule covers it (issue #1199)"
    ),
    "lib/download_validation.py": (
        "measured 2026-08-19: zero neighbours -- "
        "tests.test_download_validation does not exist and no EXACT_PATH_"
        "NEIGHBOURS/prefix rule covers it (issue #1199)"
    ),
    "lib/evidence_action_file.py": (
        "measured 2026-08-19: zero neighbours -- "
        "tests.test_evidence_action_file does not exist and no EXACT_PATH_"
        "NEIGHBOURS/prefix rule covers it (issue #1199)"
    ),
    "lib/fs_authority.py": (
        "measured 2026-08-19: zero neighbours -- tests.test_fs_authority "
        "does not exist and no EXACT_PATH_NEIGHBOURS/prefix rule covers it "
        "(issue #1199)"
    ),
    "lib/import_job_recovery_service.py": (
        "measured 2026-08-19: zero neighbours -- "
        "tests.test_import_job_recovery_service does not exist and no "
        "EXACT_PATH_NEIGHBOURS/prefix rule covers it (issue #1199)"
    ),
    "lib/release_snapshot.py": (
        "measured 2026-08-19: zero neighbours -- "
        "tests.test_release_snapshot does not exist and no EXACT_PATH_"
        "NEIGHBOURS/prefix rule covers it (issue #1199)"
    ),
    "lib/replace_status.py": (
        "measured 2026-08-19: zero neighbours -- tests.test_replace_status "
        "does not exist and no EXACT_PATH_NEIGHBOURS/prefix rule covers it "
        "(issue #1199)"
    ),
    "lib/search_plan_inspection.py": (
        "measured 2026-08-19: zero neighbours -- "
        "tests.test_search_plan_inspection does not exist and no "
        "EXACT_PATH_NEIGHBOURS/prefix rule covers it (issue #1199)"
    ),
    "lib/v0_probe.py": (
        "measured 2026-08-19: zero neighbours -- tests.test_v0_probe does "
        "not exist and no EXACT_PATH_NEIGHBOURS/prefix rule covers it "
        "(issue #1199)"
    ),
    "lib/wrong_match_delete_service.py": (
        "measured 2026-08-19: zero neighbours -- "
        "tests.test_wrong_match_delete_service does not exist and no "
        "EXACT_PATH_NEIGHBOURS/prefix rule covers it (issue #1199)"
    ),
    "lib/wrong_matches.py": (
        "measured 2026-08-19: zero neighbours -- tests.test_wrong_matches "
        "does not exist and no EXACT_PATH_NEIGHBOURS/prefix rule covers it "
        "(issue #1199)"
    ),
}


#: Changed `scripts/**/*.py` and `scripts/**/*.sh` files whose full
#: neighbour resolution
#: (EXACT_PATH_NEIGHBOURS + prefix rules + direct candidates that actually
#: exist) yields ZERO test modules -- the scripts/ twin of
#: LIB_MODULES_WITHOUT_SELECTION_COVERAGE (issue #1248; the `.sh` half
#: joined in #1278 item 9, when the shell wrappers gained the same
#: fail-closed treatment their `.py` siblings already had).
#: `_direct_test_candidates` probes only `tests.test_<basename>` for a
#: scripts/ path (no `_generated` sibling probe, unlike lib/), so a
#: script whose real coverage
#: lives under a differently-named test module, or whose basename collides
#: with a sibling file in a subdirectory `_direct_test_candidates` cannot
#: see (it ignores every path component except the basename), resolves zero
#: neighbours from that mechanism alone. Mirrors the lib/ registry's
#: non-early-return shape: `_changed_path_neighbours` does NOT return early
#: for a path listed here -- the full resolution already ran, so a script
#: that later gains a real EXACT_PATH_NEIGHBOURS entry, prefix rule, or
#: `tests.test_<stem>` module selects it immediately with no code change
#: here, and the stale registration is what
#: `tests/test_selection_coverage_audit.py` then demands be deleted.
#:
#: Population is a fresh measurement (driving the real resolution function
#: plus a grep-and-read pass over every candidate test file to confirm REAL
#: import/exec, not a docstring or comment mention -- see that same file's
#: audit test for the mechanical proof), never hand-curated. Of the 11
#: top-level `scripts/*.py` files issue #1248 found resolving zero
#: neighbours, measurement showed 10 have real test coverage reachable only
#: through a missing EXACT_PATH_NEIGHBOURS entry (added below) -- only
#: `bench_parallel_search.py` is a genuine gap. The same sweep found a
#: second, undercounted cohort: 19 of 20 `scripts/pipeline_cli/*.py` files
#: also resolve zero neighbours (`_direct_test_candidates` derives
#: `tests.test_<basename>` from ONLY the basename, ignoring the
#: `pipeline_cli/` subdirectory, exactly the nested-lib/ shape
#: `lib/dispatch/core.py` had under #1199) despite the package having
#: extensive real coverage in `tests/test_pipeline_cli.py` -- every command-
#: family module is re-exported by `scripts/pipeline_cli/__init__.py` and
#: called there as `pipeline_cli.cmd_<name>(...)`, the SAME function object
#: the family module defines, so the call is real execution of that
#: module's code, not a lookalike. `scripts/pipeline_cli/beets_distance.py`
#: is the one file in that package whose basename-derived candidate
#: (`tests.test_beets_distance`) already exists, so it needs no entry.
#: `scripts/pipeline_cli/__main__.py` is a genuine gap: its own docstring
#: states nothing does `import scripts.pipeline_cli.__main__` (running it
#: as a script sets `sys.path[0]` to its own directory, the #445 hazard its
#: bootstrap works around), so no dotted-import test module can drive it.
SCRIPTS_MODULES_WITHOUT_SELECTION_COVERAGE: dict[str, str] = {
    "scripts/bench_parallel_search.py": (
        "measured 2026-08-22: zero neighbours -- "
        "tests.test_bench_parallel_search does not exist; the only "
        "reference anywhere under tests/ is "
        "a comment in tests/conftest.py naming it as the dev benchmarking "
        "script an optional fallback exists for, not an import or exec "
        "(issue #1248)"
    ),
    "scripts/pipeline_cli/__main__.py": (
        "measured 2026-08-22: zero neighbours -- tests.test___main__ does "
        "not exist and no EXACT_PATH_NEIGHBOURS/prefix rule covers it; the "
        "module's own docstring states nothing imports it under a dotted "
        "name (script-mode-only entry shim, the #445 sys.path[0] hazard), "
        "so no dotted-import test module can drive its bootstrap or its "
        "delegation into scripts.pipeline_cli.cli.main (issue #1248)"
    ),
    "scripts/lint.sh": (
        "measured 2026-08-31: zero neighbours -- tests.test_lint does not "
        "exist, and a repository-wide grep for the string 'lint.sh' "
        "matches nothing outside this file: no test, no doc, no Nix "
        "module, no other script, and not the wrapper itself either. "
        "It is a "
        "bare developer convenience wrapper around `nix-shell --run "
        "pyright` on a hand-typed file list; the canonical typing "
        "contracts run through scripts/run_pyright_checks.py instead "
        "(issue #1278 item 9)"
    ),
    "scripts/mcp-playwright.sh": (
        "measured 2026-08-31: zero neighbours -- tests.test_mcp-playwright "
        "is not even a legal module name, and nothing under tests/ "
        "mentions this file at all, so no test depends on its contents. "
        "Its consumers are agent/docs surfaces -- .mcp.json's `command` "
        "string and the generated .codex/config.toml / "
        ".codex/agents/playwright.toml beside it, "
        ".claude/agents/playwright.md, docs/playwright-mcp.md, and two "
        ".claude/memory notes -- which invoke it by PATH, describe its "
        "behaviour in prose, or both; none of them is a selectable test, "
        "so the binary-name resolution and CDP/headless mode selection "
        "inside it are exercised only by really launching an MCP server "
        "(issue #1278 item 9)"
    ),
}


#: `migrations/`'s admitted-gap registry (issue #1355 item 8). Empty:
#: `prefix:migrations/` matches every path under `migrations/` regardless of
#: suffix, so every one of the 82 tracked `.sql` files resolves real
#: neighbours today (measured 2026-09-02). The row exists to fail closed the
#: day that prefix rule is narrowed or deleted, not because a current
#: migration is under-covered.
MIGRATIONS_WITHOUT_SELECTION_COVERAGE: dict[str, str] = {}

#: `nix/`'s admitted-gap registry (issue #1355 item 8). Empty for the same
#: reason as migrations/ above: `prefix:nix/` matches every path under
#: `nix/` regardless of suffix, so all 11 tracked `.nix`/`.json` files
#: resolve today (measured 2026-09-02).
NIX_MODULES_WITHOUT_SELECTION_COVERAGE: dict[str, str] = {}

#: `harness/`'s admitted-gap registry (issue #1355 item 8). Empty for the
#: same reason: `prefix:harness/` matches every path under `harness/`
#: regardless of suffix, so all 6 tracked `.py`/`.sh` files resolve today
#: (measured 2026-09-02).
HARNESS_MODULES_WITHOUT_SELECTION_COVERAGE: dict[str, str] = {}

#: `web/`'s admitted-gap registry (issue #1355 item 8). Unlike migrations/
#: nix/harness above, web/*.py coverage is basename-derived (existence of
#: tests.test_web_<stem> / tests.web.test_<stem>), so it genuinely misses
#: real files. Measured 2026-09-02: 41 tracked web/*.py files exist; 22 are
#: under web/routes/, covered unconditionally by prefix:web/routes/. Of the
#: remaining 19, 10 resolved zero neighbours. Nine had real coverage under
#: a name the basename probe cannot derive and got an EXACT_PATH_NEIGHBOURS
#: entry instead (classify.py, mb.py, artist_search.py, download_history_view.py,
#: api_bases.py, library_album_row.py, library_album_detail_service.py,
#: library_artist_service.py, index_document.py — see those entries below
#: for the verified real consumer of each; index_document.py's own entry
#: was added after review found the first draft had wrongly admitted it as
#: a gap on a grep that missed tests/web/*.py). This one is the genuine gap.
#:
#: `.js` files under web/js/ are deliberately NOT policed by this row (see
#: the ROOT_COVERAGE_RULES comment on the web row): targeted selection only
#: narrows which PYTHON test modules a run selects, and
#: scripts/run_targeted_tests.py runs the complete JavaScript phase
#: unconditionally on every targeted invocation regardless of what Python
#: selection returns — the same reason no ROOT_COVERAGE_RULES row has ever
#: policed tests/js_harness.mjs's own `.mjs` suffix.
WEB_MODULES_WITHOUT_SELECTION_COVERAGE: dict[str, str] = {
    "web/__init__.py": (
        "measured 2026-09-02: zero neighbours -- the file is empty (0 "
        "bytes), so there is no logic to regress and no test module was "
        "ever written for it"
    ),
}

#: Top-level's admitted-gap registry (issue #1355 item 8). Empty: both
#: tracked top-level `.py` files (album_source.py via the basename probe,
#: cratedigger.py via its EXACT_PATH_NEIGHBOURS entry below) resolve real
#: neighbours today (measured 2026-09-02).
TOP_LEVEL_MODULES_WITHOUT_SELECTION_COVERAGE: dict[str, str] = {}


#: The stderr line a registered, genuinely zero-neighbour path logs instead
#: of raising. One template for every non-early-returning rule below, so
#: the rules that use it cannot drift in wording.
ADMITTED_GAP_MESSAGE = (
    "admitted selection gap: {path} resolves zero test neighbours "
    "({rationale})"
)


@dataclass(frozen=True)
class RootCoverageRule:
    """One repository root's fail-closed zero-neighbour contract.

    The three roots that first policed under-selection (`tests/`, `lib/`,
    `scripts/`) had three structurally identical branches in
    `_changed_path_neighbours`, each with its own registry and its own
    error string (issue #1278 item 9). This is that shape as data: a root,
    the file suffixes it polices, its admitted-gap registry, and the exact
    message an unmapped path raises. Issue #1355 item 8 carried the same
    shape to every remaining production root — `migrations/`, `nix/`,
    `web/`, `harness/`, and the top level — rather than adding a second
    selection mechanism. The registries themselves stay hand-maintained
    data — nothing here infers coverage from an import graph.

    Every column is behavioural — this is a table production reads, not
    documentation. Two of them decide this table's own SCOPE, which is why
    `tests/test_selection_coverage_audit.py` anchors both against values
    held outside the table (`EXPECTED_SUFFIXES`,
    `EXPECTED_ADMITTED_SELECTS_NOTHING`): `suffixes` decides which files a
    root polices at all (`.sh` joined the `scripts/` row in item 9), and
    `admitted_selects_nothing` decides WHEN a registered path is honoured
    AND which rows that audit examines — so without the anchors a table
    edit could quietly vacate its own policing.
    The `tests/` registry early-returns BEFORE resolution
    (a registration must not be a lookalike neighbour set — issue #1081),
    so a registered `tests/` path never reaches the post-resolution branch
    at all. Every other row's registry does not early-return: full
    resolution runs first, and a registered path merely logs
    `ADMITTED_GAP_MESSAGE` on the way out, so a path that later gains real
    coverage selects it immediately (issues #1199, #1248).

    `top_level` (issue #1355 item 8) makes a row match a bare filename with
    no directory component — `path.parts[:1] == (self.root,)` can never be
    true for such a path, since a top-level file's ONLY path component is
    its own name, never a literal root string. Mirrors the identical field
    on `SelectionRule` (`basename:<top-level>.py`); `root` on a top-level
    row is a display label only (`"<top-level>"`, matching that naming
    convention), never a real directory `covers` walks.
    """

    root: str
    suffixes: tuple[str, ...]
    registry: Mapping[str, str]
    registry_name: str
    admitted_selects_nothing: bool
    unmapped_message: str
    top_level: bool = False

    def covers(self, path: PurePosixPath) -> bool:
        """True when this rule polices ``path``'s root and file suffix."""
        if self.top_level:
            return len(path.parts) == 1 and path.suffix in self.suffixes
        return path.parts[:1] == (self.root,) and path.suffix in self.suffixes


#: Root → admitted-gap registry → fail-closed behaviour, one row per root.
#: `_changed_path_neighbours` and the import-time double-registration guard
#: both loop this table, and
#: `tests/test_selection_coverage_audit.py` parameterizes over it rather
#: than naming roots by hand.
ROOT_COVERAGE_RULES: tuple[RootCoverageRule, ...] = (
    RootCoverageRule(
        root="tests",
        suffixes=(".py",),
        registry=SHARED_MODULES_WITHOUT_COVERAGE,
        registry_name="SHARED_MODULES_WITHOUT_COVERAGE",
        admitted_selects_nothing=True,
        # Names only the real mapping mechanisms — never advertises the
        # admitted-gap registry as the easy way out (a registration there
        # is a reviewed admission, issue #1081).
        unmapped_message=(
            "unmapped shared test module: {path} — add an "
            "EXACT_PATH_NEIGHBOURS entry or a prefix rule for it in "
            "scripts/targeted_test_selection.py"
        ),
    ),
    RootCoverageRule(
        root="lib",
        suffixes=(".py",),
        registry=LIB_MODULES_WITHOUT_SELECTION_COVERAGE,
        registry_name="LIB_MODULES_WITHOUT_SELECTION_COVERAGE",
        admitted_selects_nothing=False,
        unmapped_message=(
            "unmapped lib module: {path} resolves zero test "
            "neighbours — add an EXACT_PATH_NEIGHBOURS entry or a "
            "prefix rule for it in scripts/targeted_test_selection.py"
        ),
    ),
    RootCoverageRule(
        root="scripts",
        # ``.sh`` joined this row in issue #1278 item 9: the shell wrappers
        # are entry points with no fail-closed selection story at all until
        # then. scripts/run_final_gate.sh is the measured case — on main's
        # history the ONLY commit that ever gave it an entry is item 6's
        # PR2 (0c3bae8e), the same commit that turned it into a wrapper, so
        # through item 6's PR1 and everything before it an edit to that file
        # selected nothing and no audit noticed. ``lib/`` and ``tests/``
        # hold no ``.sh`` files, so the suffix stays on this row rather than
        # becoming a global default.
        suffixes=(".py", ".sh"),
        registry=SCRIPTS_MODULES_WITHOUT_SELECTION_COVERAGE,
        registry_name="SCRIPTS_MODULES_WITHOUT_SELECTION_COVERAGE",
        admitted_selects_nothing=False,
        unmapped_message=(
            "unmapped scripts module: {path} resolves zero "
            "test neighbours — add an EXACT_PATH_NEIGHBOURS entry or a "
            "prefix rule for it in scripts/targeted_test_selection.py"
        ),
    ),
    # The five rows below (issue #1355 item 8) carry the same fail-closed
    # policy to every remaining production root. migrations/, nix/ and
    # harness/ are each matched by an UNCONDITIONAL prefix rule
    # (`prefix:migrations/`, `prefix:nix/`, `prefix:harness/` — no suffix
    # filter, fixed `neighbours`), so under today's SELECTION_RULES no real
    # or synthetic path under these roots can ever resolve zero: these three
    # rows are currently dormant coverage, not currently-firing coverage.
    # Their value is provable only by removing the shadowing prefix rule
    # through the DI seam (see
    # tests/test_selection_coverage_audit.py's
    # test_root_rule_catches_an_unconditionally_shadowed_root_if_its_prefix_rule_is_removed)
    # — deleting one of those three prefix rules today would silently
    # under-select every file in its root; after this change it raises
    # instead. web/ and the top level are different: their coverage is
    # basename-derived (existence of a differently-shaped test module), so
    # a real orphaned file resolves zero TODAY, and these two rows fire on
    # real, unmodified selection.
    RootCoverageRule(
        root="migrations",
        suffixes=(".sql",),
        registry=MIGRATIONS_WITHOUT_SELECTION_COVERAGE,
        registry_name="MIGRATIONS_WITHOUT_SELECTION_COVERAGE",
        admitted_selects_nothing=False,
        unmapped_message=(
            "unmapped migration: {path} resolves zero test "
            "neighbours — add an EXACT_PATH_NEIGHBOURS entry or a "
            "prefix rule for it in scripts/targeted_test_selection.py"
        ),
    ),
    RootCoverageRule(
        root="nix",
        suffixes=(".nix", ".json"),
        registry=NIX_MODULES_WITHOUT_SELECTION_COVERAGE,
        registry_name="NIX_MODULES_WITHOUT_SELECTION_COVERAGE",
        admitted_selects_nothing=False,
        unmapped_message=(
            "unmapped nix module: {path} resolves zero test "
            "neighbours — add an EXACT_PATH_NEIGHBOURS entry or a "
            "prefix rule for it in scripts/targeted_test_selection.py"
        ),
    ),
    RootCoverageRule(
        root="web",
        # `.js` is deliberately excluded — see the WEB_MODULES_WITHOUT_
        # SELECTION_COVERAGE comment above: JavaScript selection is not
        # governed by this Python-module-neighbour mechanism at all, so
        # policing it here would raise for files the ambient JS phase
        # already covers in full on every targeted run.
        suffixes=(".py",),
        registry=WEB_MODULES_WITHOUT_SELECTION_COVERAGE,
        registry_name="WEB_MODULES_WITHOUT_SELECTION_COVERAGE",
        admitted_selects_nothing=False,
        unmapped_message=(
            "unmapped web module: {path} resolves zero test "
            "neighbours — add an EXACT_PATH_NEIGHBOURS entry or a "
            "prefix rule for it in scripts/targeted_test_selection.py"
        ),
    ),
    RootCoverageRule(
        root="harness",
        suffixes=(".py", ".sh"),
        registry=HARNESS_MODULES_WITHOUT_SELECTION_COVERAGE,
        registry_name="HARNESS_MODULES_WITHOUT_SELECTION_COVERAGE",
        admitted_selects_nothing=False,
        unmapped_message=(
            "unmapped harness module: {path} resolves zero test "
            "neighbours — add an EXACT_PATH_NEIGHBOURS entry or a "
            "prefix rule for it in scripts/targeted_test_selection.py"
        ),
    ),
    RootCoverageRule(
        root="<top-level>",
        top_level=True,
        suffixes=(".py",),
        registry=TOP_LEVEL_MODULES_WITHOUT_SELECTION_COVERAGE,
        registry_name="TOP_LEVEL_MODULES_WITHOUT_SELECTION_COVERAGE",
        admitted_selects_nothing=False,
        unmapped_message=(
            "unmapped top-level module: {path} resolves zero test "
            "neighbours — add an EXACT_PATH_NEIGHBOURS entry or a "
            "prefix rule for it in scripts/targeted_test_selection.py"
        ),
    ),
)


@dataclass(frozen=True)
class SelectionRule:
    """One path-matched neighbour contribution, as data (issue #1313).

    The basename conventions and the directory rules used to be hand-written
    ``if`` branches: all six in ``_direct_test_candidates``, and nine of the
    twelve in ``_resolve_neighbours`` (the other three are the self-selector
    and two existence checks, none of which is a rule) — so the modules they
    name were invisible to
    `tests/test_selection_coverage_audit.py`'s contract A — that audit's
    own docstring recorded the inline literals as out of reach. As rows
    they are ordinary data, audited like `EXACT_PATH_NEIGHBOURS` and
    `ROOT_COVERAGE_RULES`, and `explain` can name the rule behind every
    selected module.

    This changes NO selection outcome. It is not a coverage inference of
    any kind: a row still names its modules by hand, and nothing here reads
    an import graph.

    Matching is an AND of independent conditions, each unconstrained when
    its field is empty:

    - ``prefixes`` / ``exact_paths`` — together one OR'd path test, the
      shape the `nix/` + `flake.nix` and `harness/` + `lib/beets.py` rules
      already had;
    - ``root`` — ``path.parts[:1] == (root,)``, the same first-component
      guard `RootCoverageRule.covers` uses;
    - ``top_level`` — ``len(path.parts) == 1``;
    - ``suffixes`` — the file suffix, which is what keeps the `.sh` probe
      off `.py` files and vice versa;
    - ``excluded_prefixes`` — subtracted last, so `web/routes/` stays out
      of the `web/` basename rule.

    A row contributes ``neighbours`` verbatim, then whichever of its
    ``derived`` templates (formatted with the changed file's ``stem``)
    names a module that exists on disk. A row with no matcher at all would
    match every path, so `_assert_selection_rules_well_formed` refuses one
    at import time.
    """

    name: str
    description: str
    prefixes: tuple[str, ...] = ()
    exact_paths: tuple[str, ...] = ()
    root: str | None = None
    top_level: bool = False
    suffixes: tuple[str, ...] = ()
    excluded_prefixes: tuple[str, ...] = ()
    neighbours: tuple[str, ...] = ()
    derived: tuple[str, ...] = ()

    def matches(self, relative_path: str, path: PurePosixPath) -> bool:
        """True when this rule's every stated condition holds for ``path``."""
        if (self.prefixes or self.exact_paths) and not (
            any(relative_path.startswith(p) for p in self.prefixes)
            or relative_path in self.exact_paths
        ):
            return False
        if self.root is not None and path.parts[:1] != (self.root,):
            return False
        if self.top_level and len(path.parts) != 1:
            return False
        if self.suffixes and path.suffix not in self.suffixes:
            return False
        return not any(
            relative_path.startswith(excluded)
            for excluded in self.excluded_prefixes
        )

    def render_derived(self, path: PurePosixPath) -> tuple[str, ...]:
        """This rule's derived module names for ``path``, unchecked.

        Existence is the caller's business: `_direct_test_candidates`
        deliberately returns unchecked candidates (its own contract since
        issue #1081), while `contribute` splits them into real and missing.
        """
        return tuple(
            template.format(stem=path.stem) for template in self.derived
        )

    def contribute(
        self, path: PurePosixPath, repo_root: Path
    ) -> tuple[tuple[str, ...], tuple[str, ...]]:
        """``(modules, unresolved)`` — what this rule adds, and what it wanted.

        ``unresolved`` is what makes `explain` useful: a derived name with
        no module file on disk is the single most common reason a path
        resolves less than its author expected.
        """
        existing, unresolved = _split_existing(
            self.render_derived(path), repo_root
        )
        return (*self.neighbours, *existing), unresolved


#: Basename conventions: "a file at this path is covered by a test module
#: named after its stem". Mutually exclusive by construction — disjoint
#: roots, disjoint suffixes within a root, and a top-level rule no rooted
#: path can reach — so AT MOST one row matches any path (most paths match
#: none) and `_basename_rule` can speak of "the" matching rule.
#: `tests/test_targeted_test_selection.py` pins that over every real file
#: under a root these rules can reach, plus shapes the tree does not hold.
#: Every candidate is checked for real existence before it is selected, so a
#: row claims a naming convention, never evidence that the matched module
#: executes the file.
BASENAME_RULES: tuple[SelectionRule, ...] = (
    SelectionRule(
        name="basename:scripts/*.sh",
        description=(
            "a shell wrapper under scripts/ is driven by tests/test_<stem>.py"
        ),
        root="scripts",
        suffixes=(".sh",),
        derived=("tests.test_{stem}",),
        # Issue #1278 item 9. Five of the sixteen wrappers already had a
        # matching tests/test_<stem>.py and needed no hand-written entry
        # once this probe existed; scripts/test_tmpfs.sh had an entry
        # naming exactly the module this probe finds, deleted as redundant
        # in the same change. Deliberately scripts-only: widening it to
        # every root is latent (harness/run_beets_harness.sh and three
        # docs/research wrappers have no matching module), pinned by
        # test_shell_probe_is_scoped_to_the_scripts_root.
    ),
    SelectionRule(
        name="basename:lib/*.py",
        description=(
            "a lib module is covered by tests/test_<stem>.py and its "
            "generated sibling"
        ),
        root="lib",
        suffixes=(".py",),
        derived=("tests.test_{stem}", "tests.test_{stem}_generated"),
        # The _generated probe is the same mechanical basename-only shape
        # as its deterministic twin (issue #1199 review F5): a lib/ file
        # whose ONLY coverage is a generated module resolves too.
    ),
    SelectionRule(
        name="basename:scripts/*.py",
        description="a script is covered by tests/test_<stem>.py",
        root="scripts",
        suffixes=(".py",),
        derived=("tests.test_{stem}",),
        # No _generated sibling probe here, unlike lib/ above.
    ),
    SelectionRule(
        name="basename:web/*.py",
        description=(
            "a non-route web module is covered by tests/test_web_<stem>.py "
            "or tests/web/test_<stem>.py"
        ),
        root="web",
        suffixes=(".py",),
        excluded_prefixes=("web/routes/",),
        derived=("tests.test_web_{stem}", "tests.web.test_{stem}"),
        # web/routes/ is excluded because its own prefix rule below already
        # derives tests.web.test_routes_<stem> plus ROUTE_NEIGHBOURS.
    ),
    SelectionRule(
        name="basename:<top-level>.py",
        description=(
            "a top-level module is covered by tests/test_<stem>.py"
        ),
        top_level=True,
        suffixes=(".py",),
        derived=("tests.test_{stem}",),
        # Two top-level .py files exist. album_source.py is the one this row
        # actually serves -- tests/test_album_source.py exists, so the row
        # resolves it with no entry. cratedigger.py is the other, and has no
        # tests.test_cratedigger, which is why it carries a hand-written
        # EXACT_PATH_NEIGHBOURS entry instead.
    ),
)

#: Directory rules: "anything under here also regresses these modules".
#: Unlike the basename stage, which takes THE one matching row, this loop
#: accumulates every row that matches. No two of today's rows can both match
#: (no row's prefix or exact path is a prefix of another's), so that
#: difference is currently unobservable and the row order below is not
#: pinned by anything — what IS pinned is the stage order: table entry, then
#: self-selector, then basename, then these.
#:
#: No row here constrains `root`, `top_level` or `suffixes`, so `matches`
#: short-circuits before it ever reads the `PurePosixPath` argument for one
#: of these rows. That is why passing a bogus `path` to a prefix row's
#: `matches` is an equivalent mutation today (issue #1329 residual 8, and a
#: mutmut survivor in every breadth pass over this file). It stops being
#: equivalent the moment a row uses one of those three fields — this note is
#: the warning that the safety is a property of the data below, not of the
#: matcher.
PREFIX_RULES: tuple[SelectionRule, ...] = (
    SelectionRule(
        name="prefix:lib/pipeline_db/",
        description=(
            "a production DB cluster regresses the shared boundary "
            "contracts and its own mirrored fake's self-tests"
        ),
        prefixes=("lib/pipeline_db/",),
        neighbours=PIPELINE_DB_NEIGHBOURS,
        derived=("tests.test_fakes_{stem}",),
        # tests/fakes/pipeline_db/ mirrors this package module for module
        # (#1313 candidate 4), so ONE derived name serves both sides: the
        # tests/fakes/ row below spells the same template. It ADDS
        # precision and is not the floor — PIPELINE_DB_NEIGHBOURS already
        # carries tests.test_fakes, which holds the cross-cluster tests and
        # the fake-to-production signature contract, so a cluster with no
        # sibling module yet still resolves a real consumer.
    ),
    SelectionRule(
        name="prefix:migrations/",
        description="a migration regresses the DB contracts and the migrator",
        prefixes=("migrations/",),
        neighbours=(*PIPELINE_DB_NEIGHBOURS, "tests.test_migrator"),
    ),
    SelectionRule(
        name="prefix:tests/fakes/",
        description=(
            "a fake regresses the fake-to-production contract and, when it "
            "has one, its own cluster's self-tests"
        ),
        prefixes=("tests/fakes/",),
        neighbours=("tests.test_fakes",),
        derived=("tests.test_fakes_{stem}",),
        # The derived half is why editing tests/fakes/beets.py reaches
        # tests.test_fakes_beets: since the #1313 split, tests/test_fakes.py
        # does not name FakeBeetsDB at all, so the fixed half alone would
        # select a module that never loads the fake being edited — the same
        # shape as the deploy_hold instance in issue #1081 review round 2.
    ),
    SelectionRule(
        name="prefix:scripts/phase_parsers/",
        description=(
            "a phase log parser regresses its own dialect tests and the "
            "coordinator that composes them"
        ),
        prefixes=("scripts/phase_parsers/",),
        neighbours=("tests.test_phase_parsers", "tests.test_suite_coordinator"),
        # Issue #1313. The basename probe derives tests.test_<stem> from the
        # FILE name only, so scripts/phase_parsers/ruff.py looks for a
        # nonexistent tests/test_ruff.py and the scripts/ root rule fails the
        # path closed. Both named modules are real consumers rather than a
        # floor: the dialect tests drive each parser directly, and the
        # coordinator's own suite proves the callable a PhaseSpec names is
        # the one that reads its log.
    ),
    SelectionRule(
        name="prefix:tests/structural_audits/",
        description="shared structural-audit support regresses its audits",
        prefixes=("tests/structural_audits/",),
        neighbours=STRUCTURAL_AUDIT_NEIGHBOURS,
    ),
    SelectionRule(
        name="prefix:tests/world_model/",
        description="world-model support regresses the burst and its drivers",
        prefixes=("tests/world_model/",),
        neighbours=WORLD_MODEL_NEIGHBOURS,
    ),
    SelectionRule(
        name="prefix:web/routes/",
        description=(
            "a route regresses the route audits and its own contract tests"
        ),
        prefixes=("web/routes/",),
        neighbours=ROUTE_NEIGHBOURS,
        derived=("tests.web.test_routes_{stem}",),
    ),
    SelectionRule(
        name="prefix:nix/",
        description="Nix module or flake pin regresses the module contract",
        prefixes=("nix/",),
        exact_paths=("flake.nix", "flake.lock"),
        neighbours=("tests.test_nix_module",),
    ),
    SelectionRule(
        name="prefix:harness/",
        description="harness code regresses the real-beets drift gate",
        prefixes=("harness/",),
        exact_paths=("lib/beets.py",),
        neighbours=("tests.test_harness_beets2_contract",),
    ),
    SelectionRule(
        name="prefix:lib/quality/",
        description="a quality module regresses the decision album test set",
        prefixes=("lib/quality/",),
        neighbours=(
            "tests.test_quality_decisions",
            "tests.test_quality_classification",
            "tests.test_quality_generated",
        ),
    ),
)

#: Every path-matched rule, in resolution order: the basename stage first,
#: then the directory rules. `_resolve_neighbours` consumes exactly this
#: order, `explain` enumerates it, and
#: `tests/test_selection_coverage_audit.py`'s contract A now checks the
#: modules these rows name — which no audit could see while they were
#: inline literals inside two if-chains.
SELECTION_RULES: tuple[SelectionRule, ...] = (*BASENAME_RULES, *PREFIX_RULES)

#: What `resolve_attributed_neighbours` calls the two mechanisms that are
#: not `SELECTION_RULES` rows, so `explain` names every source the same way.
EXACT_TABLE_SOURCE = "table:EXACT_PATH_NEIGHBOURS"
SELF_SELECTOR_SOURCE = "self-selector"


@dataclass(frozen=True)
class NeighbourSource:
    """One mechanism's contribution to a path's neighbours, with its name."""

    name: str
    description: str
    modules: tuple[str, ...]
    unresolved: tuple[str, ...] = ()


def _assert_selection_rules_well_formed(
    rules: Sequence[SelectionRule],
) -> None:
    """Every row names and describes itself uniquely, matches something, and
    adds something.

    Four ways a row can be silently wrong. A row with no matcher matches
    EVERY path, so its modules would join every selection in the repository.
    A row with neither ``neighbours`` nor ``derived`` matches paths and
    contributes nothing, which reads as coverage and is not. A duplicate
    ``name`` makes `explain`'s attribution ambiguous and lets one row's pin
    silently stand in for another's.

    A duplicate ``description`` is the subtlest, and it is why this clause
    exists rather than leaving descriptions to review: `explain` prints one
    beside every attributed module, so a row wearing a neighbour's sentence
    tells an operator that a route file matched a rule for non-route files.
    The review round's mutant did exactly that and nothing failed.
    """
    seen: set[str] = set()
    described: set[str] = set()
    for rule in rules:
        if rule.name in seen:
            raise ValueError(f"duplicate SelectionRule name: {rule.name}")
        seen.add(rule.name)
        if rule.description in described:
            raise ValueError(
                f"duplicate SelectionRule description on {rule.name}: "
                f"{rule.description}"
            )
        described.add(rule.description)
        if not (
            rule.prefixes
            or rule.exact_paths
            or rule.root is not None
            or rule.top_level
        ):
            raise ValueError(
                f"SelectionRule {rule.name} constrains no path and would "
                "match every file in the repository"
            )
        if not (rule.neighbours or rule.derived):
            raise ValueError(
                f"SelectionRule {rule.name} contributes no test module"
            )


_assert_selection_rules_well_formed(SELECTION_RULES)


def _assert_no_double_registration(
    exact_path_neighbours: Mapping[str, tuple[str, ...]],
    admitted_gap_registry: Mapping[str, str],
    *,
    gap_registry_name: str = "SHARED_MODULES_WITHOUT_COVERAGE",
) -> None:
    """A path cannot be both a real mapping and an admitted coverage gap.

    Shared shape for both admitted-gap registries (SHARED_MODULES_WITHOUT_
    COVERAGE on the tests/ side, LIB_MODULES_WITHOUT_SELECTION_COVERAGE on
    the lib/ side): _changed_path_neighbours returns early for any path in
    SHARED_MODULES_WITHOUT_COVERAGE, before EXACT_PATH_NEIGHBOURS or any
    prefix rule runs — so a path present in BOTH registries would silently
    discard its real, hand-authored EXACT_PATH_NEIGHBOURS mapping (issue
    #1081 review round: the tree-walking pin's assertion that a registered
    path selects nothing was a tautology of the early-return condition and
    could never have caught this on its own). LIB_MODULES_WITHOUT_SELECTION_
    COVERAGE does not early-return, but the same contradiction — a path
    claiming zero neighbours while EXACT_PATH_NEIGHBOURS gives it real ones
    — is still nonsensical and worth failing on at import time rather than
    merely detecting it later.
    """
    contradictions = sorted(
        set(exact_path_neighbours) & set(admitted_gap_registry)
    )
    if contradictions:
        raise ValueError(
            f"path(s) registered in both EXACT_PATH_NEIGHBOURS and "
            f"{gap_registry_name}: {', '.join(contradictions)}"
        )


def _assert_registries_disjoint(
    rules: Sequence[RootCoverageRule],
    exact_path_neighbours: Mapping[str, tuple[str, ...]],
) -> None:
    """Run the double-registration guard for EVERY row of a rule table.

    A function rather than an inline import-time loop so a self-test can
    drive it with a fabricated table whose contradiction sits in the LAST
    row — truncating the real loop to its first row was otherwise green
    (issue #1278 item 9 review M24).
    """
    for rule in rules:
        _assert_no_double_registration(
            exact_path_neighbours,
            rule.registry,
            gap_registry_name=rule.registry_name,
        )


_assert_registries_disjoint(ROOT_COVERAGE_RULES, EXACT_PATH_NEIGHBOURS)


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


def _split_existing(
    candidates: Iterable[str], repo_root: Path
) -> tuple[tuple[str, ...], tuple[str, ...]]:
    """Partition derived module names into those on disk and those not.

    One splitter for both callers — `SelectionRule.contribute` and the
    basename stage in `resolve_attributed_neighbours` — so "what counts as
    resolvable" cannot come to mean two things.
    """
    existing: list[str] = []
    missing: list[str] = []
    for candidate in candidates:
        if _existing_module(candidate, repo_root) is None:
            missing.append(candidate)
        else:
            existing.append(candidate)
    return tuple(existing), tuple(missing)


def _basename_rule(
    path: PurePosixPath,
    *,
    basename_rules: Sequence[SelectionRule] = BASENAME_RULES,
) -> SelectionRule | None:
    """The one matching row of ``basename_rules``, if any.

    The rows are mutually exclusive by construction — disjoint roots,
    disjoint suffixes within a root, and a top-level rule no rooted path can
    reach — so "the matching rule" is well defined and this is not a
    first-match tie-break. `tests/test_targeted_test_selection.py` pins that
    over every tracked file, because the claim is what lets `explain`
    attribute a basename hit to one named rule.

    ``basename_rules`` is a kwarg-DI seam; see `_resolve_neighbours`.
    """
    relative_path = path.as_posix()
    for rule in basename_rules:
        if rule.matches(relative_path, path):
            return rule
    return None


def _direct_test_candidates(
    path: PurePosixPath,
    *,
    basename_rules: Sequence[SelectionRule] = BASENAME_RULES,
) -> tuple[str, ...]:
    """The basename-convention module names for ``path``, existence unchecked.

    The caller checks each candidate really exists via `_existing_module`,
    so this claims nothing beyond a naming convention: never evidence that
    the matched module executes or reads the file.
    """
    rule = _basename_rule(path, basename_rules=basename_rules)
    return () if rule is None else rule.render_derived(path)


def resolve_attributed_neighbours(
    relative_path: str,
    path: PurePosixPath,
    repo_root: Path,
    *,
    exact_path_neighbours: Mapping[str, tuple[str, ...]] = EXACT_PATH_NEIGHBOURS,
    basename_rules: Sequence[SelectionRule] = BASENAME_RULES,
    prefix_rules: Sequence[SelectionRule] = PREFIX_RULES,
) -> tuple[NeighbourSource, ...]:
    """Every mechanism's contribution, named, in resolution order.

    `_resolve_neighbours` is the flattening of this, so the two cannot drift
    — and `explain` can answer "which rule selected this module", which
    previously meant reading the file or diffing a run (issue #1313).

    A source appears for every mechanism that MATCHED, including one that
    contributed nothing because its derived module does not exist: that case
    is the most useful thing `explain` reports.

    The three table kwargs are DI seams; see `_resolve_neighbours`.
    """
    sources: list[NeighbourSource] = []
    entry = exact_path_neighbours.get(relative_path, ())
    if entry:
        sources.append(
            NeighbourSource(
                EXACT_TABLE_SOURCE,
                "hand-authored entry for this exact path",
                tuple(entry),
            )
        )
    module = _path_module(path)
    if module is not None and module.startswith("tests."):
        sources.append(
            NeighbourSource(
                SELF_SELECTOR_SOURCE,
                "the changed file is itself a runnable test module",
                (module,),
            )
        )
    basename_rule = _basename_rule(path, basename_rules=basename_rules)
    if basename_rule is not None:
        # Deliberately routed through `_direct_test_candidates` rather than
        # `basename_rule.contribute`, which would be one call rather than
        # two: that function is the name every admitted-gap rationale and
        # registry comment in this file cites for the basename probe,
        # including two dated measurement records, so it stays the
        # production candidate source instead of becoming a test-only
        # synonym for the same five rows.
        modules, unresolved = _split_existing(
            _direct_test_candidates(path, basename_rules=basename_rules),
            repo_root,
        )
        sources.append(
            NeighbourSource(
                basename_rule.name,
                basename_rule.description,
                modules,
                unresolved,
            )
        )
    for rule in prefix_rules:
        if not rule.matches(relative_path, path):
            continue
        modules, unresolved = rule.contribute(path, repo_root)
        sources.append(
            NeighbourSource(rule.name, rule.description, modules, unresolved)
        )
    return tuple(sources)


def _resolve_neighbours(
    relative_path: str,
    path: PurePosixPath,
    repo_root: Path,
    *,
    exact_path_neighbours: Mapping[str, tuple[str, ...]] = EXACT_PATH_NEIGHBOURS,
    basename_rules: Sequence[SelectionRule] = BASENAME_RULES,
    prefix_rules: Sequence[SelectionRule] = PREFIX_RULES,
) -> list[str]:
    """The full EXACT_PATH_NEIGHBOURS + self-selector + direct-candidate +
    prefix-rule resolution, with NO admitted-gap registry's fail-closed check
    applied yet. Split out of _changed_path_neighbours so every root rule's
    fail-closed check can run against the SAME raw result, and so a one-off
    measurement (issue #1199 item 1's registry population) can call this
    directly without tripping the fail-closed raise for every
    still-unregistered zero-neighbour file.

    Three kwarg-DI seams, one per table, each answering the same question
    about a different mechanism: what does this path resolve WITHOUT that
    piece of hand-authored data? ``exact_path_neighbours`` (issue #1278 item
    9) takes an empty mapping to drop a path's entry;
    ``basename_rules``/``prefix_rules`` (issue #1313) take the table minus
    one row to drop a rule. Both questions have the same purpose —
    tests/test_selection_coverage_audit.py's maskable-entry and
    maskable-rule pins measure deletion visibility through the real resolver
    rather than reimplementing it. They are definition-time defaults, so a
    replacement must be passed explicitly; patching the module binding does
    not reach them.
    """
    return [
        module
        for source in resolve_attributed_neighbours(
            relative_path,
            path,
            repo_root,
            exact_path_neighbours=exact_path_neighbours,
            basename_rules=basename_rules,
            prefix_rules=prefix_rules,
        )
        for module in source.modules
    ]


def _changed_path_neighbours(
    relative_path: str,
    repo_root: Path,
    *,
    basename_rules: Sequence[SelectionRule] = BASENAME_RULES,
    prefix_rules: Sequence[SelectionRule] = PREFIX_RULES,
) -> tuple[str, ...]:
    """One path's selection, with every root rule's fail-closed check applied.

    ``basename_rules``/``prefix_rules`` are the same DI seams
    `_resolve_neighbours` documents, forwarded so a caller can ask the whole
    contract — resolution AND the raise — what deleting one rule row would
    do. `_resolve_neighbours` alone answers only half of that, and the
    fail-closed half is what decides whether a deletion would be noticed.
    """
    path = PurePosixPath(relative_path)
    for rule in ROOT_COVERAGE_RULES:
        if rule.admitted_selects_nothing and relative_path in rule.registry:
            # An admitted gap on such a rule always selects nothing beyond
            # ambient, regardless of what a prefix rule would otherwise
            # contribute — a registration must not be a lookalike neighbour
            # set (issue #1081 review round: mirror_harness.py was
            # registered but the tests/world_model/ prefix rule would still
            # have populated WORLD_MODEL_NEIGHBOURS for it).
            return ()
    neighbours = _resolve_neighbours(
        relative_path,
        path,
        repo_root,
        basename_rules=basename_rules,
        prefix_rules=prefix_rules,
    )
    if not neighbours:
        for rule in ROOT_COVERAGE_RULES:
            # A file this rule polices that resolves zero test neighbours
            # under-selects — the more dangerous failure for a test
            # selector, since the run reports green having exercised
            # nothing relevant to the change (issue #1081). Fail closed and
            # name the file, unless it is an admitted, reviewed gap.
            if not rule.covers(path):
                continue
            rationale = rule.registry.get(relative_path)
            if rationale is None:
                raise ValueError(
                    rule.unmapped_message.format(path=relative_path)
                )
            # Reachable only for a rule that does NOT early-return its
            # admitted gaps (lib/, scripts/): selection proceeds — ambient
            # gates still run — but logs loudly naming the gap, so a
            # registration that later gains real coverage selects it with
            # no code change here. An admitted_selects_nothing rule's
            # registered paths returned () above and never reach this.
            print(
                ADMITTED_GAP_MESSAGE.format(
                    path=relative_path, rationale=rationale
                ),
                file=sys.stderr,
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


def _root_coverage_lines(
    relative_path: str,
    path: PurePosixPath,
    repo_root: Path,
) -> tuple[str, ...]:
    """How the fail-closed contract treats this path, and what it selects.

    Reads `ROOT_COVERAGE_RULES` as data and calls the REAL
    `_changed_path_neighbours` for the verdict, then applies the REAL
    `_paired_module` on top, which is the other half of what
    `expand_test_selection` does per path. Neither is reimplemented here, so
    the reported selection cannot disagree with a run — except for the
    ambient audits, which are named rather than listed because they are the
    same on every path. That resolver call writes the admitted-gap note to
    stderr; it is swallowed because this report already states the gap and
    its rationale in full.
    """
    lines: list[str] = []
    covering = [rule for rule in ROOT_COVERAGE_RULES if rule.covers(path)]
    if not covering:
        lines.append(
            "  policed by: nothing — no ROOT_COVERAGE_RULES row covers this "
            "root and suffix, so resolving zero neighbours here is silent"
        )
    for rule in covering:
        # Every current row's root strings are distinct and a `top_level`
        # row can never also match a rooted path (issue #1355 item 8
        # review), so `covering` holds at most one rule for every one of
        # the 1,647 tracked files, measured directly — not an invariant
        # anything here enforces. A future row that overlaps another
        # (e.g. a second `web` row policing a different suffix) would make
        # this `continue` start mattering; it would still be correct, since
        # each matching rule's own message belongs on its own line.
        rationale = rule.registry.get(relative_path)
        if rationale is None:
            lines.append(
                f"  policed by: the {rule.root}/ row — zero neighbours here "
                f"raises unless {rule.registry_name} admits the path"
            )
            continue
        lines.append(f"  admitted gap in {rule.registry_name}: {rationale}")
        if rule.admitted_selects_nothing:
            lines.append(
                "      this registry selects NOTHING at all: every module "
                "above is discarded before resolution, so a registration "
                "cannot be a lookalike neighbour set (issue #1081)"
            )
    buffer = io.StringIO()
    try:
        with contextlib.redirect_stderr(buffer):
            selected = _changed_path_neighbours(relative_path, repo_root)
    except ValueError as exc:
        lines.append(f"  selects: NOTHING — this path fails closed: {exc}")
        return tuple(lines)
    # expand_test_selection adds a deterministic/generated sibling for every
    # module it selects, which no rule contributes and no source above can
    # name. Reporting the neighbours alone under-stated what really runs on
    # 254 of 1,619 tracked paths (measured: tests 149, migrations 82,
    # scripts 13, web 5, lib 4, top level 1) -- migrations/*.sql pull in
    # tests.test_migrator_generated, and a lib/ module whose only coverage is
    # a generated sibling gets it from here, not from a rule. It never
    # under-stated in the dangerous direction: of those 254, zero resolve no
    # neighbours at all, so the report never said "nothing" while something
    # ran.
    paired = [
        sibling
        for module in _ordered_unique(selected)
        if (sibling := _paired_module(module, repo_root)) is not None
    ]
    if paired:
        lines.append(
            "  paired siblings — added by expand_test_selection, not by any "
            "rule:"
        )
        lines.extend(f"      {module}" for module in paired)
    # De-duplicated because that is what actually runs: expand_test_selection
    # collapses repeats before the runner sees them, so a module two
    # mechanisms both name is one target. The per-source breakdown above
    # still shows both mechanisms naming it.
    unique = _ordered_unique((*selected, *paired))
    lines.append(
        "  selects: "
        + (", ".join(unique) if unique else "nothing beyond the ambient gates")
    )
    lines.append(
        "      plus every ambient audit and ratchet, which run on every "
        "selection regardless of the path"
    )
    return tuple(lines)


def explain_path(relative_path: str, repo_root: Path) -> tuple[str, ...]:
    """Report lines explaining how selection resolves one path.

    Answers the question the selection machinery could not answer before
    (issue #1313): which mechanism contributed each selected module, which
    matched but found nothing on disk, and whether the fail-closed contract
    is watching this path at all. The path need not exist — the resolver
    never stats its own target, only candidate test modules — so a path can
    be explained before the file is written.
    """
    path = PurePosixPath(relative_path)
    lines = [relative_path]
    sources = resolve_attributed_neighbours(relative_path, path, repo_root)
    if not sources:
        lines.append("  (no mechanism matched this path)")
    for source in sources:
        lines.append(f"  {source.name} — {source.description}")
        lines.extend(f"      {module}" for module in source.modules)
        lines.extend(
            f"      {module}  (no module file on disk)"
            for module in source.unresolved
        )
    lines.extend(_root_coverage_lines(relative_path, path, repo_root))
    return tuple(lines)


def main(argv: Sequence[str] | None = None) -> int:
    """`explain` a path's selection: which rule contributes which module."""
    parser = argparse.ArgumentParser(
        prog="targeted_test_selection",
        description=(
            "Explain how targeted test selection resolves a repository path."
        ),
    )
    subcommands = parser.add_subparsers(dest="command", required=True)
    explain = subcommands.add_parser(
        "explain",
        help="show which rule contributes each test module for a path",
    )
    explain.add_argument(
        "paths",
        nargs="+",
        metavar="PATH",
        help="repository-relative path, e.g. lib/download.py",
    )
    args = parser.parse_args(argv)

    repo_root = Path(__file__).resolve().parent.parent
    for index, relative_path in enumerate(args.paths):
        if index:
            print()
        for line in explain_path(relative_path, repo_root):
            print(line)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
