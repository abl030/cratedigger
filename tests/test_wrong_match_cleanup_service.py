"""Tests for evidence-only Wrong Matches cleanup."""

from __future__ import annotations

import os
import shutil
import tempfile
import types
import unittest
from datetime import datetime, timezone
from unittest.mock import patch

import msgspec

from lib.quality import (
    AlbumQualityEvidence,
    AlbumQualityEvidenceFile,
    AudioQualityMeasurement,
    QualityRankConfig,
    VerifiedLosslessProof,
)
from lib.quality_evidence import snapshot_fingerprint
from lib.import_evidence import (
    ActionEvidenceProvenance,
    CurrentEvidenceActionResult,
)
from lib.wrong_match_cleanup_service import (
    OUTCOME_DELETE_FAILED,
    OUTCOME_DELETED,
    OUTCOME_DELETED_VERIFIED_LOSSLESS_PARENT,
    OUTCOME_KEPT_WOULD_IMPORT,
    OUTCOME_SKIPPED_ACTIVE_JOB,
    OUTCOME_SKIPPED_CANDIDATE_EVIDENCE_MISSING,
    OUTCOME_SKIPPED_CANDIDATE_EVIDENCE_STALE,
    OUTCOME_SKIPPED_CURRENT_EVIDENCE_FAILED,
    OUTCOME_SKIPPED_CURRENT_EVIDENCE_MISSING,
    OUTCOME_SKIPPED_MISSING_PATH,
    OUTCOME_SKIPPED_OPERATIONAL,
    cleanup_all_wrong_matches,
    cleanup_wrong_match,
)
from tests.fakes import FakePipelineDB
from tests.helpers import make_request_row


def _cfg() -> types.SimpleNamespace:
    return types.SimpleNamespace(
        quality_ranks=QualityRankConfig.defaults(),
        verified_lossless_target="",
    )


def _make_source(root: str, name: str) -> str:
    failed_root = os.path.join(root, "failed_imports")
    os.makedirs(failed_root, exist_ok=True)
    source = os.path.join(failed_root, name)
    os.mkdir(source)
    with open(os.path.join(source, "01.mp3"), "wb") as handle:
        handle.write(b"audio")
    return source


def _evidence_files(source: str) -> list[AlbumQualityEvidenceFile]:
    full = os.path.join(source, "01.mp3")
    stat = os.stat(full)
    return [
        AlbumQualityEvidenceFile(
            relative_path="01.mp3",
            size_bytes=int(stat.st_size),
            mtime_ns=int(stat.st_mtime_ns),
            extension="mp3",
            container="mp3",
            codec="mp3",
        )
    ]


def _evidence(
    source: str,
    *,
    mb_release_id: str = "mbid-1",
    audio_corrupt: bool = False,
) -> AlbumQualityEvidence:
    files = _evidence_files(source)
    return AlbumQualityEvidence(
        mb_release_id=mb_release_id,
        snapshot_fingerprint=snapshot_fingerprint(files),
        source_path=source,
        measurement=AudioQualityMeasurement(
            min_bitrate_kbps=245,
            avg_bitrate_kbps=256,
            median_bitrate_kbps=252,
            format="mp3 v0",
            spectral_grade="genuine",
        ),
        measured_at=datetime(2026, 5, 1, tzinfo=timezone.utc),
        files=files,
        codec="mp3",
        container="mp3",
        storage_format="mp3 v0",
        audio_corrupt=audio_corrupt,
        audio_file_count=len(files),
        filetype_band="mp3",
        folder_layout="flat",
    )


def _store_evidence(
    db: FakePipelineDB,
    evidence: AlbumQualityEvidence,
) -> int:
    db.upsert_album_quality_evidence(evidence)
    stored = db.find_album_quality_evidence(
        mb_release_id=evidence.mb_release_id,
        snapshot_fingerprint=evidence.snapshot_fingerprint,
    )
    assert stored is not None and stored.id is not None
    return stored.id


def _log_wrong_match(
    db: FakePipelineDB,
    request_id: int,
    source: str,
) -> int:
    return db.log_download(
        request_id,
        outcome="rejected",
        validation_result={
            "scenario": "wrong_match",
            "failed_path": source,
        },
    )


class WrongMatchCleanupServiceTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.mkdtemp()
        self.db = FakePipelineDB()
        self.db.seed_request(make_request_row(
            id=1,
            status="wanted",
            mb_release_id="mbid-1",
        ))
        # Default: Beets has no album for this MBID. Individual tests override
        # via self._set_current_evidence_helper(...) to drive specific branches.
        self._current_evidence_helper = lambda *_args, **_kwargs: None
        helper_patch = patch(
            "lib.wrong_match_cleanup_service.load_current_evidence_for_action",
            side_effect=lambda *a, **kw: self._current_evidence_helper(*a, **kw),
        )
        self.addCleanup(helper_patch.stop)
        self.mock_current_evidence_helper = helper_patch.start()

    def _set_current_evidence_helper(self, fn) -> None:
        self._current_evidence_helper = fn

    def tearDown(self) -> None:
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_missing_download_log_fk_does_not_use_sibling_import_job_evidence(self) -> None:
        source = _make_source(self.tmp, "sparse-source")
        log_id = _log_wrong_match(self.db, 1, source)

        job = self.db.enqueue_import_job(
            "automation_import",
            request_id=1,
            payload={},
        )
        evidence_id = _store_evidence(
            self.db,
            _evidence(source, audio_corrupt=True),
        )
        self.db.set_import_job_candidate_evidence(job.id, evidence_id)
        self.db.mark_import_job_completed(job.id)

        result = cleanup_wrong_match(self.db, log_id, cfg=_cfg())

        self.assertEqual(result.outcome, OUTCOME_SKIPPED_CANDIDATE_EVIDENCE_MISSING)
        self.assertTrue(os.path.isdir(source))
        vr = self.db.download_logs[-1].validation_result
        self.assertIn("failed_path", vr)

    def test_bulk_summary_deletes_only_confident_rejects(self) -> None:
        delete_source = _make_source(self.tmp, "delete-source")
        keep_source = _make_source(self.tmp, "keep-source")
        stale_source = _make_source(self.tmp, "stale-source")

        delete_id = _log_wrong_match(self.db, 1, delete_source)
        self.db.set_download_log_candidate_evidence(
            delete_id,
            _store_evidence(
                self.db,
                _evidence(
                    delete_source,
                    mb_release_id="mbid-delete",
                    audio_corrupt=True,
                ),
            ),
        )

        keep_id = _log_wrong_match(self.db, 1, keep_source)
        self.db.set_download_log_candidate_evidence(
            keep_id,
            _store_evidence(
                self.db,
                _evidence(keep_source, mb_release_id="mbid-keep"),
            ),
        )

        stale_id = _log_wrong_match(self.db, 1, stale_source)
        self.db.set_download_log_candidate_evidence(
            stale_id,
            _store_evidence(self.db, _evidence(stale_source, mb_release_id="mbid-stale")),
        )
        with open(os.path.join(stale_source, "02.mp3"), "wb") as handle:
            handle.write(b"changed")

        missing_source = _make_source(self.tmp, "missing-evidence")
        _log_wrong_match(self.db, 1, missing_source)

        summary = cleanup_all_wrong_matches(
            self.db,
            confirm_all_wrong_matches=True,
            cfg=_cfg(),
        )

        self.assertEqual(summary.deleted, 1)
        self.assertEqual(summary.kept_would_import, 1)
        self.assertEqual(summary.skipped_candidate_evidence_stale, 1)
        self.assertEqual(summary.skipped_candidate_evidence_missing, 1)
        self.assertFalse(os.path.exists(delete_source))
        self.assertTrue(os.path.isdir(keep_source))
        self.assertTrue(os.path.isdir(stale_source))
        self.assertTrue(os.path.isdir(missing_source))

    def test_final_outcomes_persist_recents_triage_audit(self) -> None:
        delete_source = _make_source(self.tmp, "audit-delete-source")
        keep_source = _make_source(self.tmp, "audit-keep-source")

        delete_id = _log_wrong_match(self.db, 1, delete_source)
        self.db.set_download_log_candidate_evidence(
            delete_id,
            _store_evidence(
                self.db,
                _evidence(
                    delete_source,
                    mb_release_id="audit-delete",
                    audio_corrupt=True,
                ),
            ),
        )

        keep_id = _log_wrong_match(self.db, 1, keep_source)
        self.db.set_download_log_candidate_evidence(
            keep_id,
            _store_evidence(
                self.db,
                _evidence(keep_source, mb_release_id="audit-keep"),
            ),
        )

        cleanup_all_wrong_matches(
            self.db,
            confirm_all_wrong_matches=True,
            cfg=_cfg(),
        )

        by_id = {row.id: row for row in self.db.download_logs}
        deleted_triage = by_id[delete_id].validation_result["wrong_match_triage"]
        kept_triage = by_id[keep_id].validation_result["wrong_match_triage"]

        self.assertEqual(deleted_triage["action"], "deleted_reject")
        self.assertEqual(deleted_triage["outcome"], OUTCOME_DELETED)
        self.assertEqual(deleted_triage["preview_verdict"], "confident_reject")
        self.assertEqual(deleted_triage["preview_decision"], "audio_corrupt")
        self.assertIn("preimport_audio:reject_corrupt",
                      deleted_triage["stage_chain"])

        self.assertEqual(kept_triage["action"], OUTCOME_KEPT_WOULD_IMPORT)
        self.assertEqual(kept_triage["preview_verdict"], "would_import")
        self.assertFalse(os.path.exists(delete_source))
        self.assertTrue(os.path.isdir(keep_source))

    def test_active_matching_import_job_skips_before_delete(self) -> None:
        source = _make_source(self.tmp, "active-source")
        log_id = _log_wrong_match(self.db, 1, source)
        self.db.set_download_log_candidate_evidence(
            log_id,
            _store_evidence(self.db, _evidence(source, audio_corrupt=True)),
        )
        self.db.enqueue_import_job(
            "force_import",
            request_id=1,
            payload={"download_log_id": log_id, "failed_path": source},
        )

        result = cleanup_wrong_match(self.db, log_id, cfg=_cfg())

        self.assertEqual(result.outcome, OUTCOME_SKIPPED_ACTIVE_JOB)
        self.assertTrue(os.path.isdir(source))

    def test_authorized_delete_clears_same_request_exact_path_duplicates_only(self) -> None:
        source = _make_source(self.tmp, "duplicate-source")
        other_source = _make_source(self.tmp, "other-source")

        older_id = _log_wrong_match(self.db, 1, source)
        newest_id = _log_wrong_match(self.db, 1, source)
        other_path_id = _log_wrong_match(self.db, 1, other_source)
        self.db.seed_request(make_request_row(
            id=2,
            status="wanted",
            mb_release_id="mbid-2",
        ))
        other_request_id = _log_wrong_match(self.db, 2, source)

        self.db.set_download_log_candidate_evidence(
            newest_id,
            _store_evidence(self.db, _evidence(source, audio_corrupt=True)),
        )

        result = cleanup_wrong_match(self.db, newest_id, cfg=_cfg())

        self.assertEqual(result.outcome, OUTCOME_DELETED)
        self.assertEqual(result.cleared_rows, 2)

        by_id = {row.id: row for row in self.db.download_logs}
        self.assertNotIn("failed_path", by_id[older_id].validation_result)
        self.assertNotIn("failed_path", by_id[newest_id].validation_result)
        self.assertIn("failed_path", by_id[other_path_id].validation_result)
        self.assertIn("failed_path", by_id[other_request_id].validation_result)

    def test_lock_contention_is_reported_as_active_job_skip(self) -> None:
        source = _make_source(self.tmp, "locked-source")
        log_id = _log_wrong_match(self.db, 1, source)
        self.db.set_download_log_candidate_evidence(
            log_id,
            _store_evidence(self.db, _evidence(source, audio_corrupt=True)),
        )
        self.db.set_advisory_lock_result(False)

        result = cleanup_wrong_match(self.db, log_id, cfg=_cfg())

        self.assertEqual(result.outcome, OUTCOME_SKIPPED_ACTIVE_JOB)
        self.assertTrue(os.path.isdir(source))

    def test_active_job_that_appears_under_lock_skips_delete(self) -> None:
        source = _make_source(self.tmp, "under-lock-active-source")
        log_id = _log_wrong_match(self.db, 1, source)
        self.db.set_download_log_candidate_evidence(
            log_id,
            _store_evidence(self.db, _evidence(source, audio_corrupt=True)),
        )
        job = self.db.enqueue_import_job(
            "force_import",
            request_id=1,
            payload={"download_log_id": log_id, "failed_path": source},
        )
        calls = 0

        def active_jobs(**_kwargs):
            nonlocal calls
            calls += 1
            return [] if calls == 1 else [job]

        self.db.list_active_import_jobs_for_wrong_match = active_jobs
        with patch(
            "lib.wrong_match_cleanup_service.cleanup_wrong_match_source",
        ) as cleanup_source:
            result = cleanup_wrong_match(self.db, log_id, cfg=_cfg())

        self.assertEqual(result.outcome, OUTCOME_SKIPPED_ACTIVE_JOB)
        self.assertEqual(calls, 2)
        cleanup_source.assert_not_called()
        self.assertTrue(os.path.isdir(source))

    def test_helper_unavailable_non_failclose_skips_with_missing_outcome(self) -> None:
        """Helper signals current evidence unloadable but recoverable; skip not fail."""
        source = _make_source(self.tmp, "helper-unavailable-source")
        log_id = _log_wrong_match(self.db, 1, source)
        self.db.set_download_log_candidate_evidence(
            log_id,
            _store_evidence(self.db, _evidence(source, audio_corrupt=True)),
        )
        self._set_current_evidence_helper(
            lambda *_a, **_kw: CurrentEvidenceActionResult(
                evidence=None,
                provenance=ActionEvidenceProvenance(
                    current_status="missing",
                    fallback_reason="no current evidence found",
                    fail_closed=False,
                ),
            )
        )

        result = cleanup_wrong_match(self.db, log_id, cfg=_cfg())

        self.assertEqual(result.outcome, OUTCOME_SKIPPED_CURRENT_EVIDENCE_MISSING)
        self.assertTrue(os.path.isdir(source))
        self.assertIn("failed_path", self.db.download_logs[-1].validation_result)

    def test_delete_failure_is_reported_at_service_layer(self) -> None:
        source = _make_source(self.tmp, "delete-failure-source")
        log_id = _log_wrong_match(self.db, 1, source)
        self.db.set_download_log_candidate_evidence(
            log_id,
            _store_evidence(self.db, _evidence(source, audio_corrupt=True)),
        )
        cleanup = types.SimpleNamespace(
            success=False,
            error="permission denied",
            path_missing=False,
            cleared_rows=0,
            deleted_path=None,
        )

        with patch(
            "lib.wrong_match_cleanup_service.cleanup_wrong_match_source",
            return_value=cleanup,
        ):
            summary = cleanup_all_wrong_matches(
                self.db,
                confirm_all_wrong_matches=True,
                cfg=_cfg(),
            )

        self.assertEqual(summary.delete_failed, 1)
        self.assertEqual(summary.results[0].outcome, OUTCOME_DELETE_FAILED)
        self.assertTrue(os.path.isdir(source))
        self.assertIn("failed_path", self.db.download_logs[-1].validation_result)

    def test_delete_race_is_reported_at_service_layer_without_clearing(self) -> None:
        source = _make_source(self.tmp, "delete-race-source")
        log_id = _log_wrong_match(self.db, 1, source)
        self.db.set_download_log_candidate_evidence(
            log_id,
            _store_evidence(self.db, _evidence(source, audio_corrupt=True)),
        )
        cleanup = types.SimpleNamespace(
            success=True,
            error=None,
            path_missing=True,
            cleared_rows=0,
            deleted_path=None,
        )

        with patch(
            "lib.wrong_match_cleanup_service.cleanup_wrong_match_source",
            return_value=cleanup,
        ):
            summary = cleanup_all_wrong_matches(
                self.db,
                confirm_all_wrong_matches=True,
                cfg=_cfg(),
            )

        self.assertEqual(summary.skipped_missing_path, 1)
        self.assertEqual(summary.results[0].outcome, OUTCOME_SKIPPED_MISSING_PATH)
        self.assertIn("failed_path", self.db.download_logs[-1].validation_result)

    def test_operational_failure_is_counted_at_service_layer(self) -> None:
        source = _make_source(self.tmp, "operational-source")
        log_id = _log_wrong_match(self.db, 1, source)
        self.db.set_download_log_candidate_evidence(
            log_id,
            _store_evidence(self.db, _evidence(source, audio_corrupt=True)),
        )

        def raise_get_entry(_download_log_id: int):
            raise RuntimeError("db unavailable")

        self.db.get_download_log_entry = raise_get_entry

        summary = cleanup_all_wrong_matches(
            self.db,
            confirm_all_wrong_matches=True,
            cfg=_cfg(),
        )

        self.assertEqual(summary.skipped_operational, 1)
        self.assertEqual(summary.results[0].outcome, OUTCOME_SKIPPED_OPERATIONAL)
        self.assertTrue(os.path.isdir(source))

    def test_bulk_requires_explicit_confirmation(self) -> None:
        with self.assertRaisesRegex(ValueError, "confirm_all_wrong_matches"):
            cleanup_all_wrong_matches(self.db, cfg=_cfg())

    def test_wanted_row_uses_current_evidence_from_beets(self) -> None:
        """Parts & Labor: wanted row whose MBID is in Beets feeds current evidence to the reducer."""
        # Before the fix, cleanup short-circuited current=None for wanted rows
        # (no imported_path, status != 'imported'), so the reducer never saw the
        # parent quality and a downgrade candidate could pass as would_import.
        source = _make_source(self.tmp, "wanted-current-source")
        log_id = _log_wrong_match(self.db, 1, source)
        self.db.set_download_log_candidate_evidence(
            log_id,
            _store_evidence(self.db, _evidence(source)),
        )
        current = _evidence(source, mb_release_id="mbid-1")
        self._set_current_evidence_helper(
            lambda *_a, **_kw: CurrentEvidenceActionResult(
                evidence=current,
                provenance=ActionEvidenceProvenance(current_status="loaded"),
            )
        )

        cleanup = types.SimpleNamespace(
            success=True,
            error=None,
            path_missing=False,
            cleared_rows=1,
            deleted_path=source,
        )
        reject_decision = {
            "preimport_audio": None,
            "preimport_nested": None,
            "preimport_bad_hash": None,
            "preimport_empty_fileset": None,
            "stage0_spectral_gate": None,
            "stage1_spectral": None,
            "stage2_import": "reject",
            "stage3_quality_gate": "reject",
            "final_status": "wanted",
            "imported": False,
            "denylisted": False,
            "keep_searching": True,
            "target_final_format": None,
            "verified_lossless": False,
        }
        with patch(
            "lib.wrong_match_cleanup_service.full_pipeline_decision_from_evidence",
            return_value=reject_decision,
        ) as decider, patch(
            "lib.wrong_match_cleanup_service.classify_full_pipeline_decision",
            return_value=("confident_reject", True, "downgrade"),
        ), patch(
            "lib.wrong_match_cleanup_service.evidence_decision_name",
            return_value="downgrade",
        ), patch(
            "lib.wrong_match_cleanup_service.cleanup_wrong_match_source",
            return_value=cleanup,
        ):
            result = cleanup_wrong_match(self.db, log_id, cfg=_cfg())

        self.assertEqual(result.outcome, OUTCOME_DELETED)
        decider.assert_called_once()
        _candidate_arg, current_arg = decider.call_args.args[:2]
        self.assertIs(current_arg, current)
        self.mock_current_evidence_helper.assert_called_once()
        helper_kwargs = self.mock_current_evidence_helper.call_args.kwargs
        self.assertEqual(helper_kwargs["mb_release_id"], "mbid-1")
        self.assertEqual(helper_kwargs["request_id"], 1)

    def test_beets_absent_passes_current_none_to_reducer(self) -> None:
        """Helper returns None (no Beets album) → reducer sees current=None."""
        source = _make_source(self.tmp, "beets-absent-source")
        log_id = _log_wrong_match(self.db, 1, source)
        self.db.set_download_log_candidate_evidence(
            log_id,
            _store_evidence(self.db, _evidence(source)),
        )
        self._set_current_evidence_helper(lambda *_a, **_kw: None)

        with patch(
            "lib.wrong_match_cleanup_service.full_pipeline_decision_from_evidence",
            wraps=__import__(
                "lib.wrong_match_cleanup_service",
                fromlist=["full_pipeline_decision_from_evidence"],
            ).full_pipeline_decision_from_evidence,
        ) as decider:
            result = cleanup_wrong_match(self.db, log_id, cfg=_cfg())

        self.assertEqual(result.outcome, OUTCOME_KEPT_WOULD_IMPORT)
        self.assertTrue(os.path.isdir(source))
        decider.assert_called_once()
        _candidate_arg, current_arg = decider.call_args.args[:2]
        self.assertIsNone(current_arg)

    def test_beets_present_but_fail_closed_skips(self) -> None:
        """Helper signals current evidence unloadable; reducer never runs."""
        source = _make_source(self.tmp, "fail-closed-source")
        log_id = _log_wrong_match(self.db, 1, source)
        self.db.set_download_log_candidate_evidence(
            log_id,
            _store_evidence(self.db, _evidence(source, audio_corrupt=True)),
        )
        self._set_current_evidence_helper(
            lambda *_a, **_kw: CurrentEvidenceActionResult(
                evidence=None,
                provenance=ActionEvidenceProvenance(
                    current_status="failed",
                    fallback_reason="RuntimeError: boom",
                    fail_closed=True,
                ),
            )
        )

        with patch(
            "lib.wrong_match_cleanup_service.full_pipeline_decision_from_evidence",
        ) as decider:
            result = cleanup_wrong_match(self.db, log_id, cfg=_cfg())

        self.assertEqual(result.outcome, OUTCOME_SKIPPED_CURRENT_EVIDENCE_FAILED)
        self.assertTrue(os.path.isdir(source))
        decider.assert_not_called()
        self.assertIsNotNone(result.reason)
        assert result.reason is not None
        self.assertIn("RuntimeError", result.reason)
        self.assertIn("boom", result.reason)

    def test_verified_lossless_parent_short_circuits_to_deletion(self) -> None:
        """Verified-lossless current → cleanup deletes without calling the reducer."""
        source = _make_source(self.tmp, "verified-lossless-parent-source")
        log_id = _log_wrong_match(self.db, 1, source)
        self.db.set_download_log_candidate_evidence(
            log_id,
            _store_evidence(self.db, _evidence(source)),
        )
        current = msgspec.structs.replace(
            _evidence(source, mb_release_id="mbid-1"),
            verified_lossless_proof=VerifiedLosslessProof(
                proof_origin="library",
                source="library_audit",
                classifier="verified_lossless",
                detail="parent_lossless_proof",
            ),
        )
        self._set_current_evidence_helper(
            lambda *_a, **_kw: CurrentEvidenceActionResult(
                evidence=current,
                provenance=ActionEvidenceProvenance(current_status="loaded"),
            )
        )

        cleanup = types.SimpleNamespace(
            success=True,
            error=None,
            path_missing=False,
            cleared_rows=1,
            deleted_path=source,
        )
        with patch(
            "lib.wrong_match_cleanup_service.full_pipeline_decision_from_evidence",
        ) as decider, patch(
            "lib.wrong_match_cleanup_service.cleanup_wrong_match_source",
            return_value=cleanup,
        ) as cleanup_call:
            result = cleanup_wrong_match(self.db, log_id, cfg=_cfg())

        self.assertEqual(
            result.outcome,
            OUTCOME_DELETED_VERIFIED_LOSSLESS_PARENT,
        )
        self.assertTrue(result.success)
        self.assertEqual(result.reason, "parent_album_verified_lossless")
        self.assertEqual(result.verdict, "confident_reject")
        self.assertEqual(result.preview_decision, "verified_lossless_parent")
        self.assertTrue(result.cleanup_eligible)
        self.assertEqual(decider.call_count, 0)
        cleanup_call.assert_called_once()

    def test_candidate_missing_bails_before_verified_lossless_check(self) -> None:
        """Missing candidate evidence skips before the verified-lossless branch fires."""
        source = _make_source(self.tmp, "candidate-missing-source")
        log_id = _log_wrong_match(self.db, 1, source)
        # NOTE: no set_download_log_candidate_evidence → candidate loader returns None.
        current = msgspec.structs.replace(
            _evidence(source, mb_release_id="mbid-1"),
            verified_lossless_proof=VerifiedLosslessProof(
                proof_origin="library",
                source="library_audit",
                classifier="verified_lossless",
            ),
        )
        self._set_current_evidence_helper(
            lambda *_a, **_kw: CurrentEvidenceActionResult(
                evidence=current,
                provenance=ActionEvidenceProvenance(current_status="loaded"),
            )
        )

        with patch(
            "lib.wrong_match_cleanup_service.cleanup_wrong_match_source",
        ) as cleanup_call:
            result = cleanup_wrong_match(self.db, log_id, cfg=_cfg())

        self.assertEqual(
            result.outcome,
            OUTCOME_SKIPPED_CANDIDATE_EVIDENCE_MISSING,
        )
        self.mock_current_evidence_helper.assert_not_called()
        cleanup_call.assert_not_called()
        self.assertTrue(os.path.isdir(source))

    def test_current_without_verified_lossless_reaches_reducer(self) -> None:
        """Non-verified-lossless current evidence still flows through the reducer."""
        source = _make_source(self.tmp, "no-verified-lossless-source")
        log_id = _log_wrong_match(self.db, 1, source)
        self.db.set_download_log_candidate_evidence(
            log_id,
            _store_evidence(self.db, _evidence(source)),
        )
        current = _evidence(source, mb_release_id="mbid-1")
        self.assertIsNone(current.verified_lossless_proof)
        self._set_current_evidence_helper(
            lambda *_a, **_kw: CurrentEvidenceActionResult(
                evidence=current,
                provenance=ActionEvidenceProvenance(current_status="loaded"),
            )
        )

        with patch(
            "lib.wrong_match_cleanup_service.full_pipeline_decision_from_evidence",
            wraps=__import__(
                "lib.wrong_match_cleanup_service",
                fromlist=["full_pipeline_decision_from_evidence"],
            ).full_pipeline_decision_from_evidence,
        ) as decider:
            result = cleanup_wrong_match(self.db, log_id, cfg=_cfg())

        self.assertEqual(decider.call_count, 1)
        _candidate_arg, current_arg = decider.call_args.args[:2]
        self.assertIs(current_arg, current)
        # The reducer was reached, which is the contract under test. The
        # specific outcome depends on candidate-vs-current comparison; the
        # only outcome this test rules out is the verified-lossless short-
        # circuit.
        self.assertNotEqual(
            result.outcome,
            OUTCOME_DELETED_VERIFIED_LOSSLESS_PARENT,
        )

    def test_mbid_missing_passes_current_none_to_reducer(self) -> None:
        """Request without an MBID skips the helper and feeds current=None."""
        self.db.seed_request(make_request_row(
            id=2,
            status="wanted",
            mb_release_id=None,
        ))
        source = _make_source(self.tmp, "no-mbid-source")
        log_id = _log_wrong_match(self.db, 2, source)
        self.db.set_download_log_candidate_evidence(
            log_id,
            _store_evidence(self.db, _evidence(source, mb_release_id="mbid-x")),
        )

        with patch(
            "lib.wrong_match_cleanup_service.full_pipeline_decision_from_evidence",
            wraps=__import__(
                "lib.wrong_match_cleanup_service",
                fromlist=["full_pipeline_decision_from_evidence"],
            ).full_pipeline_decision_from_evidence,
        ) as decider:
            result = cleanup_wrong_match(self.db, log_id, cfg=_cfg())

        self.mock_current_evidence_helper.assert_not_called()
        decider.assert_called_once()
        _candidate_arg, current_arg = decider.call_args.args[:2]
        self.assertIsNone(current_arg)
        self.assertEqual(result.outcome, OUTCOME_KEPT_WOULD_IMPORT)


if __name__ == "__main__":
    unittest.main()
