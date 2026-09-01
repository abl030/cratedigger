"""Select explicit, adjacent, and repository-wide targeted tests."""

from __future__ import annotations

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
        # failure-reachability pin (the module's only direct test driver)
        # lives with the convergence runner.
        "tests.test_convergence_runner_generated",
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
    "cratedigger.py": (
        "tests.test_slskd_searches",
        "tests.test_search_exec",
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
    # lib/dispatch/types.py holds the dispatch interface itself — the
    # ``DispatchRequest`` description, the ``DispatchDB`` port, and the
    # ``DispatchCoreFn`` seam (issue #1277). ``_direct_test_candidates``
    # derives ``tests.test_types`` from the basename, which does not exist;
    # its own dedicated module is tests.test_dispatch_request, and the two
    # dispatch behaviour suites are what prove a change to those types did
    # not move an outcome.
    "lib/dispatch/types.py": (
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
    # harness/import_one.py's basename probes resolve its stage tests, but
    # tests/test_disambiguation.py is what pins the SECOND harness pass's
    # argv (--preserve-discogs-flat-subtracks on the retry) — exactly the
    # construction lib/beets_child.py::harness_session_argv now owns
    # (#1278 item 4, PR 2 review).
    "harness/import_one.py": (
        "tests.test_disambiguation",
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
    "lib/dispatch/evidence_gate.py": (
        "measured 2026-08-19: zero neighbours -- basename-derived "
        "tests.test_evidence_gate does not exist and no EXACT_PATH_"
        "NEIGHBOURS/prefix rule covers it (issue #1199)"
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
    "lib/dispatch/outcome_actions.py": (
        "measured 2026-08-19: zero neighbours -- basename-derived "
        "tests.test_outcome_actions does not exist and no EXACT_PATH_"
        "NEIGHBOURS/prefix rule covers it (issue #1199)"
    ),
    "lib/dispatch/post_import.py": (
        "measured 2026-08-19: zero neighbours -- basename-derived "
        "tests.test_post_import does not exist and no EXACT_PATH_"
        "NEIGHBOURS/prefix rule covers it (issue #1199)"
    ),
    "lib/dispatch/quality_gate.py": (
        "measured 2026-08-19: zero neighbours -- basename-derived "
        "tests.test_quality_gate does not exist and no EXACT_PATH_"
        "NEIGHBOURS/prefix rule covers it (issue #1199)"
    ),
    "lib/dispatch/subprocess_runner.py": (
        "measured 2026-08-19: zero neighbours -- basename-derived "
        "tests.test_subprocess_runner does not exist and no EXACT_PATH_"
        "NEIGHBOURS/prefix rule covers it (issue #1199)"
    ),
    "lib/download_materialization.py": (
        "measured 2026-08-19: zero neighbours -- "
        "tests.test_download_materialization does not exist and no "
        "EXACT_PATH_NEIGHBOURS/prefix rule covers it (issue #1199)"
    ),
    "lib/download_ownership.py": (
        "measured 2026-08-19: zero neighbours -- "
        "tests.test_download_ownership does not exist and no EXACT_PATH_"
        "NEIGHBOURS/prefix rule covers it (issue #1199)"
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
    "lib/ephemeral_postgres.py": (
        "measured 2026-08-19: zero neighbours -- "
        "tests.test_ephemeral_postgres does not exist and no EXACT_PATH_"
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
    "lib/slskd_transfers.py": (
        "measured 2026-08-19: zero neighbours -- "
        "tests.test_slskd_transfers does not exist and no EXACT_PATH_"
        "NEIGHBOURS/prefix rule covers it (issue #1199)"
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

    The three roots that police under-selection (`tests/`, `lib/`,
    `scripts/`) had three structurally identical branches in
    `_changed_path_neighbours`, each with its own registry and its own
    error string (issue #1278 item 9). This is that shape as data: a root,
    the file suffixes it polices, its admitted-gap registry, and the exact
    message an unmapped path raises. The registries themselves stay
    hand-maintained data — nothing here infers coverage from an import
    graph.

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
    at all. The `lib/` and `scripts/` registries do not early-return: full
    resolution runs first, and a registered path merely logs
    `ADMITTED_GAP_MESSAGE` on the way out, so a path that later gains real
    coverage selects it immediately (issues #1199, #1248).
    """

    root: str
    suffixes: tuple[str, ...]
    registry: Mapping[str, str]
    registry_name: str
    admitted_selects_nothing: bool
    unmapped_message: str

    def covers(self, path: PurePosixPath) -> bool:
        """True when this rule polices ``path``'s root and file suffix."""
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
)


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


def _direct_test_candidates(path: PurePosixPath) -> tuple[str, ...]:
    stem = path.stem
    if path.parts[:1] == ("scripts",) and path.suffix == ".sh":
        # The same mechanical basename-only probe every .py root gets, for
        # the shell wrappers under scripts/ (issue #1278 item 9). Five of
        # the sixteen already have a tests/test_<stem>.py that drives them
        # and needed no entry once this probe existed; scripts/test_tmpfs.sh
        # had a hand-written entry naming exactly the module this probe now
        # finds, and it was deleted as redundant in the same change. The
        # caller still checks the candidate really exists via
        # _existing_module, so this claims nothing: it is a naming
        # convention, not evidence that the matched module executes or
        # reads the wrapper.
        return (f"tests.test_{stem}",)
    if path.suffix != ".py":
        return ()
    if path.parts[:1] == ("lib",):
        # tests.test_<stem>_generated is the same mechanical basename-only
        # probe as tests.test_<stem> above, just for the generated sibling
        # (issue #1199 review F5) -- both are checked for real existence by
        # the caller via _existing_module before being added, so this is
        # not hand-curation: a lib/ file whose ONLY coverage is a generated
        # module (no deterministic tests.test_<stem>) now resolves too.
        return (f"tests.test_{stem}", f"tests.test_{stem}_generated")
    if path.parts[:1] == ("scripts",):
        return (f"tests.test_{stem}",)
    if path.parts[:1] == ("web",) and path.parts[:2] != ("web", "routes"):
        return (f"tests.test_web_{stem}", f"tests.web.test_{stem}")
    if len(path.parts) == 1:
        return (f"tests.test_{stem}",)
    return ()


def _resolve_neighbours(
    relative_path: str,
    path: PurePosixPath,
    repo_root: Path,
    *,
    exact_path_neighbours: Mapping[str, tuple[str, ...]] = EXACT_PATH_NEIGHBOURS,
) -> list[str]:
    """The full EXACT_PATH_NEIGHBOURS + self-selector + direct-candidate +
    prefix-rule resolution, with NO admitted-gap registry's fail-closed check
    applied yet. Split out of _changed_path_neighbours so every root rule's
    fail-closed check can run against the SAME raw result, and so a one-off
    measurement (issue #1199 item 1's registry population) can call this
    directly without tripping the fail-closed raise for every
    still-unregistered zero-neighbour file.

    ``exact_path_neighbours`` is a kwarg-DI seam (issue #1278 item 9): pass
    an empty mapping to measure what a path resolves WITHOUT its
    hand-authored entry — the "would deleting this entry be visible?"
    question tests/test_selection_coverage_audit.py's maskable-entry pins
    exist to answer. It is a definition-time default, so a replacement must
    be passed explicitly; patching the module binding does not reach it.
    """
    neighbours: list[str] = list(exact_path_neighbours.get(relative_path, ()))
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
    return neighbours


def _changed_path_neighbours(
    relative_path: str,
    repo_root: Path,
) -> tuple[str, ...]:
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
    neighbours = _resolve_neighbours(relative_path, path, repo_root)
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
