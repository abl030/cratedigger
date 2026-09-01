import json
import os
import tempfile
import unittest
from typing import Any, cast
from unittest.mock import patch

from lib.dispatch import (
    DISPATCH_CODE_IMPORT_MANIFEST_REJECTED,
    DispatchOutcome,
    dispatch_import_from_db,
)
from lib.dispatch.manifest_guard import (
    _guard_force_import_audio_manifest,
    _guard_reject,
)
from lib.dispatch.types import ImportAttemptResult
from lib.grab_list import DownloadFile
from lib.import_execution import ExecutionCancelled
from lib.import_manifest import (
    _observe_leftovers,
    check_audio_manifest,
    move_failed_import_curated,
    tracked_audio_paths_for_downloads,
)
from lib.import_queue import IMPORT_JOB_FORCE
from lib.quality_evidence import snapshot_audio_files
from tests.dispatch_helpers import claim_next_import_job
from tests.evidence_helpers import make_album_quality_evidence
from tests.fakes import FakePipelineDB
from tests.helpers import make_request_row


class TestImportManifest(unittest.TestCase):
    def test_check_audio_manifest_reports_untracked_audio(self):
        with tempfile.TemporaryDirectory() as root:
            open(os.path.join(root, "01.flac"), "wb").close()
            open(os.path.join(root, "bonus.opus"), "wb").close()
            open(os.path.join(root, "cover.jpg"), "wb").close()

            check = check_audio_manifest(root, ["01.flac"])

        self.assertFalse(check.ok)
        self.assertEqual(check.extra_audio, ["bonus.opus"])
        self.assertEqual(check.missing_audio, [])

    def test_curated_failed_import_keeps_extra_audio_and_sidecars_together(self):
        """Issue #1077, F1/Extra 2: every production caller is a kept,
        worklist-visible rejection, so ``_allocate_target`` no longer
        branches on scenario — everything lands under ``wrong_matches/``.
        Extra tracks a real production caller could never leave behind (the
        pre-beets manifest guard proves an exact audio match before Lane A
        is reachable) simply move with the rest of the folder now; there is
        no second, silent quarantine destination to split them into."""
        with tempfile.TemporaryDirectory() as parent:
            source = os.path.join(parent, "Album")
            os.mkdir(source)
            open(os.path.join(source, "01.flac"), "wb").close()
            open(os.path.join(source, "bonus.opus"), "wb").close()
            open(os.path.join(source, "cover.jpg"), "wb").close()

            result = move_failed_import_curated(
                source,
                allowed_audio=["01.flac", "bonus.opus"],
                scenario="high_distance",
            )

            self.assertIsNotNone(result)
            assert result is not None
            self.assertIsNone(result.anomaly)
            self.assertEqual(
                result.target_path,
                os.path.join(parent, "wrong_matches", "Album"),
            )
            self.assertTrue(
                os.path.exists(os.path.join(result.target_path, "01.flac")))
            self.assertTrue(
                os.path.exists(os.path.join(result.target_path, "cover.jpg")))
            self.assertTrue(
                os.path.exists(os.path.join(result.target_path, "bonus.opus")))

    def test_curated_failed_import_prunes_benign_empty_directory_skeleton(self):
        """Issue #1077, B1 (round-2 review blocker): the move loop relocates
        FILES via ``os.walk`` but never removes the directory skeletons it
        walked through. A benign non-audio ``Scans/`` subdirectory with an
        otherwise-exact audio manifest — the reviewer's exact reproduction
        against the real Lane A entry point — used to trip the leftover
        check on an EMPTY shell and raise post-mutation, even though
        nothing untracked actually survived. It must complete cleanly with
        no anomaly, and the sidecar's own contents move to the destination
        too."""
        with tempfile.TemporaryDirectory() as parent:
            source = os.path.join(parent, "Album")
            os.mkdir(source)
            open(os.path.join(source, "01.flac"), "wb").close()
            scans_dir = os.path.join(source, "Scans")
            os.mkdir(scans_dir)
            open(os.path.join(scans_dir, "front.jpg"), "wb").close()

            result = move_failed_import_curated(
                source,
                allowed_audio=["01.flac"],
                scenario="high_distance",
            )

            self.assertIsNotNone(result)
            assert result is not None
            self.assertIsNone(result.anomaly)
            self.assertEqual(
                result.target_path,
                os.path.join(parent, "wrong_matches", "Album"),
            )
            self.assertTrue(
                os.path.exists(os.path.join(result.target_path, "01.flac")))
            self.assertTrue(os.path.exists(
                os.path.join(result.target_path, "Scans", "front.jpg")))
            self.assertFalse(os.path.exists(source))

    def test_curated_failed_import_sweeps_genuine_residue_instead_of_raising(self):
        """Issue #1077, B1 (round-2 review blocker): even genuinely
        unexpected leftover content — a caller passing an ``allowed_audio``
        set narrower than what is actually on disk — must never raise
        post-mutation. Before this fix, a raise here left zero
        download_log rows, zero denylist writes, no requeue, and the album
        stranded in ``wrong_matches/`` with no DB row: the exact invisible-
        quarantine pathology this issue kills. Now it sweeps the residue
        into the SAME destination and records an anomaly the caller folds
        into the persisted detail — never a stack trace."""
        with tempfile.TemporaryDirectory() as parent:
            source = os.path.join(parent, "Album")
            os.mkdir(source)
            open(os.path.join(source, "01.flac"), "wb").close()
            open(os.path.join(source, "bonus.opus"), "wb").close()

            result = move_failed_import_curated(
                source,
                allowed_audio=["01.flac"],
                scenario="high_distance",
            )

            self.assertIsNotNone(result)
            assert result is not None
            self.assertIsNotNone(result.anomaly)
            assert result.anomaly is not None
            self.assertIn("swept into", result.anomaly)
            self.assertEqual(
                result.target_path,
                os.path.join(parent, "wrong_matches", "Album"),
            )
            # Kept implies visible (D1): everything lands under the SAME
            # destination, nothing split outside it.
            self.assertTrue(
                os.path.exists(os.path.join(result.target_path, "01.flac")))
            self.assertTrue(os.path.exists(
                os.path.join(result.target_path, "bonus.opus")))
            self.assertFalse(os.path.exists(source))

    def test_sweep_cancellation_propagates_instead_of_being_swallowed(self):
        """Issue #1077, R3-5 (round-3 review): ``ExecutionCancelled`` is a
        ``RuntimeError`` subclass, so the sweep call site's pre-existing
        bare ``except Exception:`` would silently swallow a real
        cancellation as though it were an ordinary sweep failure — worst-
        casing it toward "leftovers present" instead of letting it
        interrupt the pipeline. A dedicated ``except ExecutionCancelled:
        raise`` above that handler (and every other new except block added
        for R3-1) must let it through unchanged.

        Drives a REAL ``CancellationToken`` rather than patching one of our
        own functions (mocks are leaf-seam only — code-quality.md): the
        checkpoint cancels itself the moment ``01.flac`` (the only file the
        main move loop is allowed to touch) has actually landed at the
        destination, which is exactly the first checkpoint reached inside
        ``_sweep_residue_into_destination`` for the untracked
        ``bonus.opus`` leftover — real production code decides when the
        interruption lands, not a hand-picked call count."""
        from lib.import_execution import CancellationToken

        with tempfile.TemporaryDirectory() as parent:
            source = os.path.join(parent, "Album")
            os.mkdir(source)
            open(os.path.join(source, "01.flac"), "wb").close()
            open(os.path.join(source, "bonus.opus"), "wb").close()
            moved_marker = os.path.join(
                parent, "wrong_matches", "Album", "01.flac")

            token = CancellationToken()

            def before_mutation():
                if os.path.exists(moved_marker):
                    token.cancel("test cancellation mid-sweep")
                token.raise_if_cancelled()

            with self.assertRaises(ExecutionCancelled):
                move_failed_import_curated(
                    source,
                    allowed_audio=["01.flac"],
                    scenario="high_distance",
                    before_mutation=before_mutation,
                )

    def test_integrity_rejection_lands_in_wrong_matches_quarantine(self):
        """Issue #1077, F1: ``_allocate_target`` no longer branches on
        scenario — the historical ``failed_imports`` (non-``bad_files``)
        destination had no producer left once ``audio_corrupt`` moved to
        ban+delete, so every scenario this mover ever sees now lands under
        the single ``wrong_matches/`` quarantine root."""
        with tempfile.TemporaryDirectory() as parent:
            source = os.path.join(parent, "Album")
            os.mkdir(source)
            open(os.path.join(source, "01.flac"), "wb").close()

            result = move_failed_import_curated(
                source,
                allowed_audio=["01.flac"],
                scenario="bad_audio_hash",
            )

            assert result is not None
            self.assertEqual(
                result.target_path,
                os.path.join(parent, "wrong_matches", "Album"),
            )

    def test_spectral_rejection_lands_in_wrong_matches_quarantine(self):
        """Issue #1077, D3/F1: the ``bad_files`` sub-routing is gone —
        ``_BAD_FILE_SCENARIOS`` was audio_corrupt's and spectral_reject's
        only consumer, and neither ever reaches this curated mover in
        production any more (audio_corrupt bans + deletes outright;
        spectral_reject was never quarantined, only immediately cleaned
        up). This pins the pure function's current behavior for the
        historical scenario string: the single ``wrong_matches/`` bucket
        every scenario this mover sees now gets."""
        with tempfile.TemporaryDirectory() as parent:
            source = os.path.join(parent, "Album")
            os.mkdir(source)
            open(os.path.join(source, "01.flac"), "wb").close()

            result = move_failed_import_curated(
                source,
                allowed_audio=["01.flac"],
                scenario="spectral_reject",
            )

            assert result is not None
            self.assertEqual(
                result.target_path,
                os.path.join(parent, "wrong_matches", "Album"),
            )

    def test_download_manifest_uses_staged_filenames(self):
        files = [
            DownloadFile(
                filename=r"remote\01.flac",
                id="",
                file_dir="",
                username="peer",
                size=1,
            ),
            DownloadFile(
                filename=r"remote\02.opus",
                id="",
                file_dir="",
                username="peer",
                size=1,
                disk_no=2,
                disk_count=2,
            ),
        ]

        self.assertEqual(
            tracked_audio_paths_for_downloads(files),
            ["01.flac", "Disk 2 - 02.opus"],
        )

    def test_validation_manifest_recovers_pre_move_absolute_items(self):
        from lib.import_manifest import tracked_audio_paths_from_validation_items

        with tempfile.TemporaryDirectory() as parent:
            staging = os.path.join(parent, "Incoming", "Album")
            failed = os.path.join(parent, "failed_imports", "Album")
            os.makedirs(staging)
            os.makedirs(failed)

            paths = tracked_audio_paths_from_validation_items(
                [{"path": os.path.join(staging, "01 Perth.flac")}],
                root=failed,
            )

        self.assertEqual(paths, ["01 Perth.flac"])


class TestObserveLeftovers(unittest.TestCase):
    """Issue #1077, R3-1/R4-2: the shared best-effort leftover-presence
    check every post-move statement in ``move_failed_import_curated`` now
    routes through — pinned directly since two call sites inherit its
    contract. Tri-state (``"empty"``/``"present"``/``"unverified"``,
    round-4 review): a shallow "any entry" check used to report
    ``"present"`` for a directory node holding no real files, and a failed
    read used to be worst-cased identically to confirmed content — both
    composed a false "untracked content" claim upstream. This function has
    no ``before_mutation`` checkpoint of its own (it never mutates), so it
    has no ``ExecutionCancelled`` handling to pin here — see
    ``TestImportManifest.test_sweep_cancellation_propagates_instead_of_being_swallowed``
    for the real, load-bearing cancellation checkpoints inside
    ``move_failed_import_curated`` itself."""

    def test_real_empty_directory_reports_no_leftovers(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            self.assertEqual(
                _observe_leftovers(tmpdir, context="test"), "empty")

    def test_real_non_empty_directory_reports_leftovers(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            open(os.path.join(tmpdir, "leftover.txt"), "wb").close()
            self.assertEqual(
                _observe_leftovers(tmpdir, context="test"), "present")

    def test_empty_directory_skeleton_reports_no_leftovers(self):
        """Issue #1077, R4-2: a directory NODE with no real file anywhere
        in its subtree must never read as "present" — only actual files
        do. This is what lets a prune failure on a benign empty skeleton
        (the R3-1 pin's world) report ``"empty"`` rather than falsely
        claiming untracked content."""
        with tempfile.TemporaryDirectory() as tmpdir:
            os.makedirs(os.path.join(tmpdir, "Scans", "nested"))
            self.assertEqual(
                _observe_leftovers(tmpdir, context="test"), "empty")

    def test_unreadable_subdirectory_reports_unverified(self):
        """Issue #1077, R4-2: a REAL EACCES-shaped read failure — a
        sub-directory this process cannot list — must report
        ``"unverified"``, never ``"present"``. Worst-casing an unreadable
        world to "present" is what let the caller compose a false
        "untracked content" accusation for a transient failure the
        reviewer proved could coincide with a genuinely clean move."""
        with tempfile.TemporaryDirectory() as tmpdir:
            blocked = os.path.join(tmpdir, "blocked")
            os.makedirs(blocked)
            os.chmod(blocked, 0o000)
            try:
                self.assertEqual(
                    _observe_leftovers(tmpdir, context="test"), "unverified")
            finally:
                os.chmod(blocked, 0o700)

    def test_walk_exception_reports_unverified(self):
        """A failure the walk itself cannot even start from (rather than
        one ``onerror`` observes mid-walk) also worst-cases to
        ``"unverified"``, never a silent ``"empty"`` or a false
        ``"present"``."""
        with patch("os.walk", side_effect=OSError("simulated walk failure")):
            self.assertEqual(
                _observe_leftovers("/nonexistent/path", context="test"),
                "unverified",
            )


class TestForceImportManifestGuard(unittest.TestCase):
    @staticmethod
    def _persist_deferred_terminal(
        db: FakePipelineDB,
        outcome: DispatchOutcome,
    ) -> None:
        terminal = outcome.terminal_outcome
        if terminal is not None:
            from tests.dispatch_helpers import finalize_claimed_dispatch

            job = db.get_import_job(terminal.import_job_id)
            assert job is not None
            finalize_claimed_dispatch(db, job, outcome)

    @staticmethod
    def _claimed_job(
        db: FakePipelineDB,
        failed_path: str,
        *,
        download_log_id: int | None = None,
        preview_result: dict[str, object] | None = None,
    ) -> int:
        job = db.enqueue_import_job(
            IMPORT_JOB_FORCE,
            request_id=42,
            payload={
                "download_log_id": download_log_id or 1,
                "failed_path": failed_path,
            },
        )
        request = db.get_request(42)
        assert request is not None
        mb_release_id = request["mb_release_id"]
        assert isinstance(mb_release_id, str)
        evidence = make_album_quality_evidence(
            mb_release_id=mb_release_id,
            source_path=failed_path,
            files=snapshot_audio_files(failed_path),
        )
        db.upsert_album_quality_evidence(evidence)
        persisted = db.find_album_quality_evidence(
            mb_release_id=evidence.mb_release_id,
            snapshot_fingerprint=evidence.snapshot_fingerprint,
        )
        assert persisted is not None and persisted.id is not None
        db.set_import_job_candidate_evidence(job.id, persisted.id)
        ready = db.mark_import_job_preview_importable(
            job.id,
            preview_result=preview_result or {},
        )
        assert ready is not None
        claimed = claim_next_import_job(db, worker_id="manifest-guard")
        assert claimed is not None and claimed.id == job.id
        return job.id


    def test_manifest_rejection_persists_complete_audit_and_outcome(self):
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=42, status="wanted", mb_release_id="mb-42"))
        outcome = _guard_reject(
            cast(Any, db), request_id=42, failed_path="/action-copy/Album",
            audit_source_path="/operator/Album", source_username="peer",
            attempt_result=ImportAttemptResult(None), detail="manifest mismatch",
            scenario="untracked_audio", distance=0.42, import_job_id=None,
            source_download_log_id=23,
        )
        self.assertFalse(outcome.success)
        self.assertEqual(outcome.message, "manifest mismatch")
        self.assertEqual(outcome.code, DISPATCH_CODE_IMPORT_MANIFEST_REJECTED)
        self.assertEqual(len(db.download_logs), 1)
        row = db.download_logs[0]
        self.assertEqual(row.soulseek_username, "peer")
        self.assertEqual(row.outcome, "rejected")
        self.assertEqual(row.beets_distance, 0.42)
        self.assertEqual(row.beets_scenario, "untracked_audio")
        self.assertEqual(row.beets_detail, "manifest mismatch")
        self.assertEqual(row.staged_path, "/action-copy/Album")
        self.assertEqual(row.source_download_log_id, 23)
        self.assertEqual(
            json.loads(row.validation_result)["failed_path"], "/operator/Album"
        )

    def test_guard_reject_passes_the_complete_audit_contract(self):
        db = FakePipelineDB()
        attempt = ImportAttemptResult(None)
        with patch("lib.dispatch.manifest_guard._record_rejection_and_maybe_requeue", return_value=17) as record:
            outcome = _guard_reject(
                cast(Any, db),
                request_id=42,
                failed_path="/action-copy/Album",
                audit_source_path="/operator/Album",
                source_username="peer",
                attempt_result=attempt,
                detail="manifest mismatch",
                scenario="untracked_audio",
                distance=0.42,
                import_job_id=9,
                source_download_log_id=23,
            )

        self.assertIs(outcome.success, False)
        self.assertEqual(outcome.code, DISPATCH_CODE_IMPORT_MANIFEST_REJECTED)
        self.assertIs(record.call_args.args[0], db)
        self.assertEqual(record.call_args.args[1], 42)
        kwargs = record.call_args.kwargs
        self.assertEqual(kwargs["detail"], "manifest mismatch")
        self.assertIsNone(kwargs["error"])
        self.assertIs(kwargs["requeue"], False)
        self.assertEqual(kwargs["outcome_label"], "rejected")
        self.assertEqual(kwargs["staged_path"], "/action-copy/Album")
        self.assertIs(kwargs["attempt_result"], attempt)
        self.assertEqual(kwargs["import_job_id"], 9)
        self.assertEqual(kwargs["source_download_log_id"], 23)
        validation = json.loads(kwargs["validation_result"])
        self.assertEqual(validation["distance"], 0.42)
        self.assertEqual(validation["scenario"], "untracked_audio")
        self.assertEqual(validation["detail"], "manifest mismatch")
        self.assertEqual(validation["failed_path"], "/operator/Album")

    def test_manifest_guard_forwards_every_rejection_argument(self):
        for actual_names, expected_count, expected_scenario, expected_detail in (
            (
                ("01.mp3",),
                2,
                "incomplete_fileset",
                "Force import source has 1 audio files but the request expects 2; source audio: 01.mp3",
            ),
            (
                ("01.mp3", "bonus.mp3"),
                1,
                "untracked_audio",
                "Force import source has 2 audio files but the request expects 1; source audio: 01.mp3, bonus.mp3",
            ),
        ):
            db = FakePipelineDB()
            db.seed_request(make_request_row(id=42, status="unsearchable"))
            db.set_tracks(42, [
                {"track_number": index, "title": str(index)}
                for index in range(1, expected_count + 1)
            ])
            with tempfile.TemporaryDirectory() as root:
                for name in actual_names:
                    open(os.path.join(root, name), "wb").close()
                attempt_result = ImportAttemptResult(None)
                with patch(
                    "lib.dispatch.manifest_guard._guard_reject",
                    return_value=DispatchOutcome(False, "rejected"),
                ) as reject:
                    outcome = _guard_force_import_audio_manifest(
                        cast(Any, db),
                        request_id=42,
                        failed_path=root,
                        audit_source_path="/operator/Album",
                        download_log_id=23,
                        source_username="peer",
                        attempt_result=attempt_result,
                        import_job_id=9,
                    )

            self.assertIsNotNone(outcome)
            self.assertEqual(reject.call_count, 1)
            kwargs = reject.call_args.kwargs
            self.assertIs(reject.call_args.args[0], db)
            self.assertEqual(kwargs["request_id"], 42)
            self.assertEqual(kwargs["failed_path"], root)
            self.assertEqual(kwargs["audit_source_path"], "/operator/Album")
            self.assertEqual(kwargs["source_username"], "peer")
            self.assertIs(kwargs["attempt_result"], attempt_result)
            self.assertEqual(kwargs["scenario"], expected_scenario)
            self.assertEqual(kwargs["detail"], expected_detail)
            self.assertEqual(kwargs["import_job_id"], 9)
            self.assertEqual(kwargs["source_download_log_id"], 23)

    def test_force_import_rejects_audio_not_in_origin_manifest(self):
        import msgspec

        from lib.quality import ImportResult, SpectralAnalysisDetail, SpectralDetail

        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42,
            mb_release_id="mbid-123",
            status="unsearchable",
            artist_name="Bon Iver",
            album_title="Bon Iver",
        ))
        with tempfile.TemporaryDirectory() as root:
            open(os.path.join(root, "01 Perth.flac"), "wb").close()
            open(os.path.join(root, "12 Wash.opus"), "wb").close()
            log_id = db.log_download(
                42,
                outcome="rejected",
                validation_result={
                    "failed_path": root,
                    "items": [{"path": os.path.join(root, "01 Perth.flac")}],
                },
            )
            audit = SpectralDetail(
                candidate=SpectralAnalysisDetail(
                    attempted=True, grade="suspect", bitrate_kbps=96),
                existing=SpectralAnalysisDetail(
                    attempted=True, grade="genuine", bitrate_kbps=245),
            )
            preview_import_result = msgspec.to_builtins(
                ImportResult(spectral=audit))
            assert isinstance(preview_import_result, dict)
            job_id = self._claimed_job(
                db,
                root,
                download_log_id=log_id,
                preview_result={"import_result": preview_import_result},
            )

            outcome = dispatch_import_from_db(
                cast(Any, db),
                request_id=42,
                failed_path=root,
                import_job_id=job_id,
                download_log_id=log_id,
            )

        self._persist_deferred_terminal(db, outcome)
        self.assertFalse(outcome.success)
        # Extra/untracked audio: keep the Wrong Matches entry for operator
        # review (the importer skips cleanup on this code).
        self.assertEqual(outcome.code, DISPATCH_CODE_IMPORT_MANIFEST_REJECTED)
        self.assertEqual(
            outcome.message,
            "Force import source does not match the original "
            "selected audio manifest: extra audio: 12 Wash.opus",
        )
        # The candidate fact is recorded, but the operator-owned search stop
        # and the Wrong Matches entry are both preserved.
        self.assertEqual(db.request(42)["status"], "unsearchable")
        outcomes = [(log.outcome, log.beets_scenario) for log in db.download_logs]
        self.assertIn(("rejected", "untracked_audio"), outcomes)
        rejection = next(
            log for log in db.download_logs
            if log.outcome == "rejected" and log.beets_scenario == "untracked_audio"
        )
        self.assertEqual(rejection.source_download_log_id, log_id)
        self.assertIsNotNone(rejection.import_result)
        assert rejection.import_result is not None
        self.assertEqual(
            ImportResult.from_json(rejection.import_result).spectral,
            audit,
        )
        # Operator's folder choice, not the peer's fault — never denylist.
        self.assertEqual(len(db.denylist), 0)

    def test_force_import_trusts_exact_origin_manifest_over_metadata_count(self):
        """One physical composite may represent multiple Discogs components.

        The validation-time audio manifest is the file authority. Request
        metadata counts logical release components and therefore must remain
        only the no-manifest fallback.
        """
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42,
            mb_release_id="mbid-123",
            status="unsearchable",
        ))
        db.set_tracks(42, [{"track_number": 1, "title": "One"}])

        with tempfile.TemporaryDirectory() as root:
            open(os.path.join(root, "01.flac"), "wb").close()
            open(os.path.join(root, "bonus.flac"), "wb").close()
            log_id = db.log_download(
                42,
                outcome="rejected",
                validation_result={
                    "failed_path": root,
                    "items": [
                        {"path": os.path.join(root, "01.flac")},
                        {"path": os.path.join(root, "bonus.flac")},
                    ],
                },
            )
            outcome = _guard_force_import_audio_manifest(
                cast(Any, db),
                request_id=42,
                failed_path=root,
                download_log_id=log_id,
                source_username=None,
                attempt_result=ImportAttemptResult(None),
            )

        self.assertIsNone(outcome)

    def test_force_import_without_manifest_accepts_matching_track_count(self):
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=42, status="unsearchable"))
        db.set_tracks(42, [{"track_number": 1, "title": "One"}])
        with tempfile.TemporaryDirectory() as root:
            open(os.path.join(root, "01.mp3"), "wb").close()
            with patch("lib.dispatch.manifest_guard._guard_reject") as reject:
                outcome = _guard_force_import_audio_manifest(
                    cast(Any, db), request_id=42, failed_path=root,
                    download_log_id=None, source_username=None,
                    attempt_result=ImportAttemptResult(None), import_job_id=None,
                )
        self.assertIsNone(outcome)
        reject.assert_not_called()

    def test_force_import_without_origin_manifest_rejects_track_count_mismatch(self):
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42,
            mb_release_id="mbid-123",
            status="unsearchable",
        ))
        db.set_tracks(42, [
            {"track_number": 1, "title": "One"},
            {"track_number": 2, "title": "Two"},
        ])

        with tempfile.TemporaryDirectory() as root:
            open(os.path.join(root, "01.mp3"), "wb").close()
            open(os.path.join(root, "02.mp3"), "wb").close()
            open(os.path.join(root, "bonus.mp3"), "wb").close()
            job_id = self._claimed_job(db, root)

            outcome = dispatch_import_from_db(
                cast(Any, db),
                request_id=42,
                failed_path=root,
                import_job_id=job_id,
            )

        self._persist_deferred_terminal(db, outcome)
        self.assertFalse(outcome.success)
        self.assertEqual(outcome.code, DISPATCH_CODE_IMPORT_MANIFEST_REJECTED)
        self.assertEqual(
            outcome.message,
            "Force import source has 3 audio files but the request expects 2; "
            "source audio: 01.mp3, 02.mp3, bonus.mp3",
        )
        self.assertEqual(db.request(42)["status"], "unsearchable")
        outcomes = [(log.outcome, log.beets_scenario) for log in db.download_logs]
        self.assertIn(("rejected", "untracked_audio"), outcomes)
        self.assertEqual(len(db.denylist), 0)

    def test_force_import_without_manifest_or_tracks_keeps_wm_and_status(self):
        """No manifest and no track rows for a non-empty source: we can't
        verify the folder, so it fails closed against beets AND keeps the
        Wrong Matches entry and operator-owned request status for review."""
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42,
            mb_release_id="mbid-123",
            status="unsearchable",
        ))

        with tempfile.TemporaryDirectory() as root:
            open(os.path.join(root, "01.mp3"), "wb").close()
            job_id = self._claimed_job(db, root)

            outcome = dispatch_import_from_db(
                cast(Any, db),
                request_id=42,
                failed_path=root,
                import_job_id=job_id,
            )

        self._persist_deferred_terminal(db, outcome)
        self.assertFalse(outcome.success)
        self.assertEqual(outcome.code, DISPATCH_CODE_IMPORT_MANIFEST_REJECTED)
        self.assertEqual(
            outcome.message,
            "Force import requires either an origin audio manifest or "
            "request track rows; refusing to pass an unowned folder to beets",
        )
        self.assertEqual(db.request(42)["status"], "unsearchable")
        outcomes = [(log.outcome, log.beets_scenario) for log in db.download_logs]
        self.assertIn(("rejected", "unverifiable_source"), outcomes)
        self.assertEqual(len(db.denylist), 0)

    def test_undercount_without_manifest_preserves_operator_status(self):
        """Issue #387: an under-count source (fewer audio files than the
        request expects, no extra files) is a missing-audio integrity fault.
        The guard preserves the operator search stop and returns
        ``IMPORT_MANIFEST_REJECTED`` so the
        importer PRESERVES the operator's partial audio (it is not 'nothing
        to inspect' — there are real files on disk)."""
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42,
            mb_release_id="mbid-123",
            status="unsearchable",
        ))
        db.set_tracks(42, [
            {"track_number": 1, "title": "One"},
            {"track_number": 2, "title": "Two"},
        ])

        with tempfile.TemporaryDirectory() as root:
            open(os.path.join(root, "01.mp3"), "wb").close()
            job_id = self._claimed_job(db, root)

            outcome = dispatch_import_from_db(
                cast(Any, db),
                request_id=42,
                failed_path=root,
                import_job_id=job_id,
            )

        self._persist_deferred_terminal(db, outcome)
        self.assertFalse(outcome.success)
        # Preserve-folder code (importer skips deletion) — a non-empty source
        # must never route through the rmtree-ing QUALITY_PIPELINE_REJECTED.
        self.assertEqual(outcome.code, DISPATCH_CODE_IMPORT_MANIFEST_REJECTED)
        self.assertEqual(
            outcome.message,
            "Force import source has 1 audio files but the request expects 2; "
            "source audio: 01.mp3",
        )
        self.assertEqual(db.request(42)["status"], "unsearchable")
        outcomes = [(log.outcome, log.beets_scenario) for log in db.download_logs]
        self.assertIn(("rejected", "incomplete_fileset"), outcomes)
        # Missing audio is not the peer's fault — never denylist.
        self.assertEqual(len(db.denylist), 0)

    def test_manifest_subset_preserves_operator_status(self):
        """Issue #387: the on-disk folder is a strict subset of the validated
        origin manifest (some validated tracks went missing, no extra audio).
        Missing audio preserves the operator stop and folder, not the
        untracked-audio framing."""
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42,
            mb_release_id="mbid-123",
            status="unsearchable",
        ))
        db.set_tracks(42, [
            {"track_number": 1, "title": "One"},
            {"track_number": 2, "title": "Two"},
        ])

        with tempfile.TemporaryDirectory() as root:
            open(os.path.join(root, "01.mp3"), "wb").close()
            log_id = db.log_download(
                42,
                outcome="rejected",
                validation_result={
                    "failed_path": root,
                    "items": [
                        {"path": os.path.join(root, "01.mp3")},
                        {"path": os.path.join(root, "02.mp3")},
                    ],
                },
            )
            job_id = self._claimed_job(db, root, download_log_id=log_id)

            outcome = dispatch_import_from_db(
                cast(Any, db),
                request_id=42,
                failed_path=root,
                import_job_id=job_id,
                download_log_id=log_id,
            )

        self._persist_deferred_terminal(db, outcome)
        self.assertFalse(outcome.success)
        self.assertEqual(outcome.code, DISPATCH_CODE_IMPORT_MANIFEST_REJECTED)
        self.assertEqual(
            outcome.message,
            "Force import source is missing validated audio: missing audio: 02.mp3",
        )
        self.assertEqual(db.request(42)["status"], "unsearchable")
        outcomes = [(log.outcome, log.beets_scenario) for log in db.download_logs]
        self.assertIn(("rejected", "incomplete_fileset"), outcomes)
        self.assertEqual(len(db.denylist), 0)
