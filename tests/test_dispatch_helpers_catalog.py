"""Direct deterministic contracts for the dispatch helper catalog slice."""

from __future__ import annotations

import os
import tempfile
import unittest

from lib import transitions
from lib.dispatch.helpers import (
    _build_download_info,
    _cleanup_staged_dir,
    _guard_failure_detail,
    _log_postflight_bad_extensions,
    _populate_dl_info_from_import_result,
    _v0_probe_log_fields,
)
from lib.dispatch.post_import import (
    _apply_or_stage_denylists,
    _apply_or_stage_transition,
    _apply_post_import_search_action,
    _resolve_post_import_search_policy,
    _run_or_stage_quality_gate,
)
from lib.dispatch.quality_gate import QualityGatePlan
from lib.quality import (
    DownloadInfo,
    DuplicateRemoveGuardInfo,
    ImportResult,
    PostflightInfo,
    V0ProbeEvidence,
)
from lib.quality.decisions import PostImportSearchAction
from lib.terminal_outcomes import PendingImportTerminalOutcome, TerminalDownloadAudit
from tests.fakes import FakePipelineDB
from tests.helpers import make_download_file, make_grab_list_entry, make_import_result


class TestDispatchHelpersCatalog(unittest.TestCase):
    def test_v0_probe_log_fields_projects_both_sides_and_empty_side(self):
        probe = V0ProbeEvidence(
            kind="candidate",
            min_bitrate_kbps=120,
            avg_bitrate_kbps=180,
            median_bitrate_kbps=170,
        )
        existing = V0ProbeEvidence(
            kind="existing",
            min_bitrate_kbps=100,
            avg_bitrate_kbps=150,
            median_bitrate_kbps=140,
        )
        fields = _v0_probe_log_fields(
            DownloadInfo(v0_probe=probe, existing_v0_probe=existing)
        )
        self.assertEqual(fields["v0_probe_kind"], "candidate")
        self.assertEqual(fields["v0_probe_min_bitrate"], 120)
        self.assertEqual(fields["v0_probe_avg_bitrate"], 180)
        self.assertEqual(fields["v0_probe_median_bitrate"], 170)
        self.assertEqual(fields["existing_v0_probe_kind"], "existing")
        self.assertEqual(fields["existing_v0_probe_min_bitrate"], 100)
        self.assertEqual(fields["existing_v0_probe_avg_bitrate"], 150)
        self.assertEqual(fields["existing_v0_probe_median_bitrate"], 140)
        self.assertIsNone(_v0_probe_log_fields(DownloadInfo())["v0_probe_kind"])

    def test_guard_failure_detail_includes_count_only_when_nonzero(self):
        no_guard = ImportResult(decision="import_failed", error="child failed")
        self.assertEqual(_guard_failure_detail(no_guard), "child failed")
        zero_guard = ImportResult(
            decision="import_failed",
            error="ignored",
            postflight=PostflightInfo(
                duplicate_remove_guard=DuplicateRemoveGuardInfo(
                    reason="duplicate", message="same album", duplicate_count=0
                )
            ),
        )
        self.assertEqual(_guard_failure_detail(zero_guard), "duplicate: same album")
        guarded = ImportResult(
            decision="import_failed",
            error="ignored",
            postflight=PostflightInfo(
                duplicate_remove_guard=DuplicateRemoveGuardInfo(
                    reason="duplicate", message="same album", duplicate_count=2
                )
            ),
        )
        self.assertEqual(
            _guard_failure_detail(guarded), "duplicate: same album (duplicates=2)"
        )

    def test_build_download_info_empty_and_metadata_aggregates(self):
        self.assertEqual(
            _build_download_info(make_grab_list_entry(files=[])), DownloadInfo()
        )
        entry = make_grab_list_entry(
            files=[
                make_download_file(
                    filename="disc.a.FLAC",
                    username="z",
                    bitRate=900,
                    sampleRate=44100,
                    bitDepth=16,
                    isVariableBitRate=False,
                ),
                make_download_file(
                    filename="b.mp3",
                    username="a",
                    bitRate=700,
                    sampleRate=48000,
                    bitDepth=24,
                    isVariableBitRate=True,
                ),
            ]
        )
        info = _build_download_info(entry)
        self.assertEqual(info.username, "a, z")
        self.assertEqual(info.contributor_usernames, ("a", "z"))
        self.assertEqual(info.filetype, "flac, mp3")
        self.assertEqual(info.bitrate, 700)
        self.assertNotEqual(info.bitrate, 900)
        self.assertEqual(info.sample_rate, 48000)
        self.assertEqual(info.bit_depth, 24)
        self.assertTrue(info.is_vbr)

    def test_populate_uses_materialized_measurement_and_final_format(self):
        info = DownloadInfo(filetype="flac")
        result = ImportResult(
            decision="import",
            source_measurement=make_import_result(
                new_min_bitrate=191
            ).source_measurement,
            materialized_measurement=make_import_result(
                new_min_bitrate=102
            ).source_measurement,
            current_measurement=make_import_result(
                new_min_bitrate=88, spectral_grade="B",
                spectral_bitrate=166
            ).source_measurement,
            final_format="opus",
        )
        _populate_dl_info_from_import_result(info, result)
        self.assertEqual(info.bitrate, 102000)
        self.assertEqual(info.actual_min_bitrate, 102)
        self.assertEqual(info.final_format, "opus")
        current = info.current_spectral
        self.assertIsNotNone(current)
        assert current is not None
        self.assertEqual(current.grade, "B")
        self.assertEqual(current.bitrate_kbps, 166)
        self.assertEqual(info.existing_min_bitrate, 88)

    def test_cleanup_preserves_protected_parent_but_prunes_disposable_parent(self):
        with tempfile.TemporaryDirectory() as root:
            protected, disposable = (
                os.path.join(root, "protected"),
                os.path.join(root, "disposable"),
            )
            protected_album, disposable_album = (
                os.path.join(protected, "album"),
                os.path.join(disposable, "album"),
            )
            os.makedirs(protected_album)
            os.makedirs(disposable_album)
            _cleanup_staged_dir(
                protected_album, protected_parents=frozenset({protected})
            )
            _cleanup_staged_dir(disposable_album)
            self.assertTrue(os.path.isdir(protected))
            self.assertFalse(os.path.exists(disposable))


class TestPostImportCatalog(unittest.TestCase):
    def test_resolve_policy_attributes_peers_only_for_denylisted_decision(self):
        files = [make_download_file(filename="a.mp3", username="peer")]
        action, deny, users, returned = _resolve_post_import_search_policy(
            decision="requeue_upgrade", files=files, fallback_username="fallback"
        )
        self.assertIsNotNone(action)
        self.assertTrue(deny)
        self.assertEqual(users, {"peer", "fallback"})
        self.assertEqual(returned, files)
        _, deny, users, _ = _resolve_post_import_search_policy(
            decision="import", files=files, fallback_username="fallback"
        )
        self.assertFalse(deny)
        self.assertEqual(users, set())

    def test_quality_gate_runs_immediately_or_stages_plan(self):
        calls = []

        def gate(**kwargs):
            calls.append(kwargs)
            self.assertEqual(kwargs["db"].__class__, FakePipelineDB)
            self.assertEqual(kwargs["request_id"], 7)
            self.assertEqual(kwargs["marker"], "immediate")

        self.assertIsNone(
            _run_or_stage_quality_gate(
                gate, None, db=FakePipelineDB(), request_id=7, marker="immediate"
            )
        )
        self.assertEqual(calls[0]["request_id"], 7)
        db = FakePipelineDB()
        pending = PendingImportTerminalOutcome(
            request_id=7,
            import_job_id=8,
            initial_transition=transitions.RequestTransition.to_imported(),
            audit=TerminalDownloadAudit(outcome="success"),
        )

        def plan_gate(**kwargs):
            self.assertIs(kwargs["db"], db)
            self.assertEqual(kwargs["request_id"], 7)
            self.assertIs(kwargs["apply"], False)
            self.assertEqual(kwargs["marker"], "passed")
            return QualityGatePlan(
                transition=transitions.RequestTransition.to_imported(),
                successful_terminal_acceptance=True,
            )

        result = _run_or_stage_quality_gate(
            plan_gate, pending, db=db, request_id=7, marker="passed"
        )
        self.assertIsNotNone(result)
        assert result is not None
        self.assertTrue(result.successful_terminal_acceptance)
        self.assertEqual(result.post_audit_transitions[0], pending.initial_transition)

    def test_apply_or_stage_transition_forwards_db_and_request(self):
        db = FakePipelineDB()
        transition = transitions.RequestTransition.to_wanted(from_status="imported")
        calls = []

        def finalize(_db, request_id, applied):
            calls.append((_db, request_id, applied))
            return True

        import lib.dispatch.post_import as post_import_module

        original = post_import_module.finalize_request
        post_import_module.finalize_request = finalize
        try:
            self.assertIsNone(_apply_or_stage_transition(db, 19, None, transition))
        finally:
            post_import_module.finalize_request = original

        self.assertEqual(calls, [(db, 19, transition)])

    def test_apply_post_import_action_is_noop_for_unknown_and_finalizes_wanted(self):
        db = FakePipelineDB()
        self.assertIsNone(
            _apply_post_import_search_action(
                db,
                request_id=7,
                pending=None,
                decision="unknown",
                search_action=None,
                mark_done=False,
                new_bitrate=None,
            )
        )
        db.seed_request(
            {"id": 7, "status": "imported", "search_filetype_override": "mp3,v0"}
        )
        calls = []

        def finalize(_db, request_id, transition):
            calls.append((_db, request_id, transition))
            return True

        import lib.dispatch.post_import as post_import_module

        original = post_import_module.finalize_request
        post_import_module.finalize_request = finalize
        try:
            _apply_post_import_search_action(
                db,
                request_id=7,
                pending=None,
                decision="requeue_upgrade",
                search_action=PostImportSearchAction("wanted", "lossless", True),
                mark_done=True,
                new_bitrate=245,
            )
        finally:
            post_import_module.finalize_request = original

        self.assertEqual(len(calls), 1)
        self.assertIs(calls[0][0], db)
        self.assertEqual(calls[0][2].from_status, "imported")
        self.assertEqual(calls[0][2].fields["search_filetype_override"], "lossless")
        self.assertEqual(calls[0][2].fields["min_bitrate"], 245)
        self.assertEqual(calls[0][1], 7)

        calls.clear()
        post_import_module.finalize_request = finalize
        try:
            _apply_post_import_search_action(
                db, request_id=7, pending=None, decision="requeue_upgrade",
                search_action=PostImportSearchAction("wanted", "lossless", True),
                mark_done=False, new_bitrate=245,
            )
        finally:
            post_import_module.finalize_request = original
        self.assertNotIn("min_bitrate", calls[0][2].fields)

        calls.clear()
        post_import_module.finalize_request = finalize
        try:
            _apply_post_import_search_action(
                db, request_id=7, pending=None, decision="requeue_upgrade",
                search_action=PostImportSearchAction("wanted", "lossless", True),
                mark_done=True, new_bitrate=None,
            )
        finally:
            post_import_module.finalize_request = original
        self.assertNotIn("min_bitrate", calls[0][2].fields)


class TestPostImportMutationPins(unittest.TestCase):
    def test_denylist_immediate_records_sorted_users_and_cooldown(self):
        db = FakePipelineDB()
        db.set_cooldown_result(True)
        cooled = set()
        _apply_or_stage_denylists(db, 7, None, {"z", "a"}, "bad", cooled)
        self.assertEqual(
            [(x.request_id, x.username, x.reason) for x in db.denylist],
            [(7, "a", "bad"), (7, "z", "bad")],
        )
        self.assertEqual(db.cooldowns_applied, ["a", "z"])
        self.assertEqual(cooled, {"a", "z"})

    def test_denylist_immediate_with_no_cooldown_tracking_never_checks_cooldown(
        self,
    ):
        """Issue #1355 item A2 mutant-runner finding: ``cooled_down_users
        is None`` (the force-import lane, which never tracks cooldowns)
        must skip the cooldown check entirely — not just skip reporting
        it. A caller that never asked to track cooldowns must not have
        its peers cooled down as a side effect of denylisting them."""
        db = FakePipelineDB()
        db.set_cooldown_result(True)
        _apply_or_stage_denylists(db, 7, None, {"z", "a"}, "bad", None)
        self.assertEqual(
            [(x.request_id, x.username, x.reason) for x in db.denylist],
            [(7, "a", "bad"), (7, "z", "bad")],
        )
        self.assertEqual(db.cooldowns_applied, [])

    def test_denylist_staged_preserves_reason_and_cooldown_intent(self):
        pending = PendingImportTerminalOutcome(
            request_id=7,
            import_job_id=8,
            initial_transition=transitions.RequestTransition.to_imported(),
            audit=TerminalDownloadAudit(outcome="success"),
        )
        result = _apply_or_stage_denylists(
            FakePipelineDB(), 7, pending, {"peer"}, "bad", None
        )
        self.assertIsNotNone(result)
        assert result is not None
        self.assertEqual(result.denylists[0].username, "peer")
        self.assertEqual(result.denylists[0].reason, "bad")
        self.assertTrue(result.denylists[0].apply_cooldown)

    def test_postflight_bad_extensions_logs_all_context_and_skips_empty(self):
        clean = ImportResult(decision="import", postflight=PostflightInfo())
        _log_postflight_bad_extensions(
            ir=clean, mode="force", request_id=7, label="Album"
        )
        warned = ImportResult(
            decision="import",
            postflight=PostflightInfo(bad_extensions=["a.tmp", "b.exe"]),
        )
        with self.assertLogs("cratedigger", level="ERROR") as logs:
            _log_postflight_bad_extensions(
                ir=warned, mode="force", request_id=7, label="Album"
            )
        self.assertIn("force", logs.output[0])
        self.assertIn("request_id=7", logs.output[0])
        self.assertIn("label=Album", logs.output[0])
        self.assertIn("a.tmp, b.exe", logs.output[0])

    def test_quality_gate_non_plan_keeps_pending_and_invalid_status_is_rejected(self):
        pending = PendingImportTerminalOutcome(
            request_id=7,
            import_job_id=8,
            initial_transition=transitions.RequestTransition.to_imported(),
            audit=TerminalDownloadAudit(outcome="success"),
        )
        result = _run_or_stage_quality_gate(
            lambda **_kwargs: object(), pending, db=FakePipelineDB(), request_id=7
        )
        self.assertIs(result, pending)
        with self.assertRaises(ValueError) as caught:
            _apply_post_import_search_action(
                FakePipelineDB(),
                request_id=7,
                pending=None,
                decision="accept",
                search_action=PostImportSearchAction("imported", None, False),
                mark_done=False,
                new_bitrate=None,
            )
        self.assertEqual(
            str(caught.exception),
            "requeueing import decision mapped to non-wanted "
            "status: accept -> imported",
        )


if __name__ == "__main__":
    unittest.main()
