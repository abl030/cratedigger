"""Tests for evidence-only Wrong Matches cleanup."""

from __future__ import annotations

import os
import shutil
import tempfile
import types
import unittest
from datetime import UTC, datetime
from typing import TYPE_CHECKING
from unittest.mock import patch

import msgspec

from lib import wrong_match_cleanup_service
from lib.import_evidence import (
    ActionEvidenceProvenance,
    CurrentEvidenceActionResult,
)
from lib.import_execution import CancellationToken
from lib.import_preview import (
    PREVIEW_VERDICT_EVIDENCE_READY,
    PREVIEW_VERDICT_MEASUREMENT_FAILED,
    ImportPreviewResult,
)
from lib.import_queue import IMPORT_JOB_FORCE
from lib.quality import (
    AlbumQualityEvidence,
    AlbumQualityEvidenceFile,
    AlbumQualityV0Metric,
    AudioQualityMeasurement,
    QualityRankConfig,
    VerifiedLosslessProof,
    legacy_unrecorded_audio_validation_report,
)
from lib.quality_evidence import snapshot_audio_files, snapshot_fingerprint
from lib.spectral_check import SPECTRAL_MEASUREMENT_VERSION
from lib.validation_envelope import decode_validation_envelope
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
from lib.wrong_matches import WrongMatchCleanupResult
from tests.fakes import FakePipelineDB
from tests.helpers import (
    make_audio_corrupt_validation_report,
    make_request_row,
)
from web.download_history_view import build_recents_download_log_rows


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
    matched_bad_audio_hash: bool = False,
    files: list[AlbumQualityEvidenceFile] | None = None,
) -> AlbumQualityEvidence:
    if files is None:
        files = _evidence_files(source)
    if audio_corrupt and files:
        files = [
            msgspec.structs.replace(file, decode_ok=index != 0)
            for index, file in enumerate(files)
        ]
    return AlbumQualityEvidence(
        mb_release_id=mb_release_id,
        snapshot_fingerprint=snapshot_fingerprint(files),
        source_path=source,
        measurement=AudioQualityMeasurement(
            min_bitrate_kbps=245,
            avg_bitrate_kbps=256,
            median_bitrate_kbps=252,
            format="MP3",
            spectral_grade="genuine",
            spectral_subject="source",
            spectral_provenance="measured",
            spectral_measurement_version=SPECTRAL_MEASUREMENT_VERSION,
        ),
        measured_at=datetime(2026, 5, 1, tzinfo=UTC),
        files=files,
        codec="mp3",
        container="mp3",
        storage_format="MP3",
        audio_validation=(
            make_audio_corrupt_validation_report(
                files[0].relative_path if files else "",
                files_checked=max(1, len(files)),
            )
            if audio_corrupt
            else legacy_unrecorded_audio_validation_report()
        ),
        audio_corrupt=audio_corrupt,
        audio_file_count=len(files),
        filetype_band="mp3",
        folder_layout="flat",
        matched_bad_audio_hash_id=1 if matched_bad_audio_hash else None,
        matched_bad_audio_hash_path=(
            files[0].relative_path if matched_bad_audio_hash and files else None
        ),
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


class _RefreshStub:
    """Stub for the ``preview_fn`` DI seam (issue #271 stale-evidence refresh).

    Mirrors the ``measure_and_persist_candidate_evidence`` contract: on
    success it persists candidate evidence and re-points the download_log
    FK, and it returns a real ``ImportPreviewResult`` either way.
    """

    def __init__(
        self,
        *,
        persist_evidence: AlbumQualityEvidence | None = None,
        verdict: str = PREVIEW_VERDICT_EVIDENCE_READY,
        reason: str | None = None,
    ) -> None:
        self.persist_evidence = persist_evidence
        self.verdict = verdict
        self.reason = reason
        self.calls: list[dict[str, object]] = []

    def __call__(self, db, *, request_id, path, download_log_id):
        self.calls.append({
            "request_id": request_id,
            "path": path,
            "download_log_id": download_log_id,
        })
        if self.persist_evidence is not None:
            db.set_download_log_candidate_evidence(
                download_log_id,
                _store_evidence(db, self.persist_evidence),
            )
        return ImportPreviewResult(
            mode="path", verdict=self.verdict, reason=self.reason,
        )


def _refresh_stub(**kwargs) -> _RefreshStub:
    return _RefreshStub(**kwargs)


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


def _patch_current_evidence_helper(fn=None):
    """The ONE ``MULTILINE_PATCH_BASELINE``-counted occurrence
    (``tests/_mock_audit_scanner.py``) of patching
    ``load_current_evidence_for_action`` directly — every caller that
    needs to control current (installed-Beets) evidence reuses this
    function instead of writing a second ``patch(...)`` call, which
    would grow the ratchet. ``fn`` defaults to "Beets has no album for
    this MBID"."""
    return patch(
        "lib.wrong_match_cleanup_service.load_current_evidence_for_action",
        side_effect=fn if fn is not None else (lambda *_a, **_kw: None),
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
        helper_patch = _patch_current_evidence_helper(
            lambda *a, **kw: self._current_evidence_helper(*a, **kw),
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
            IMPORT_JOB_FORCE,
            request_id=1,
            dedupe_key=f"force:wrong-match-cleanup:{log_id}",
            payload={
                "download_log_id": log_id,
                "failed_path": source,
            },
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
                    matched_bad_audio_hash=True,
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
            preview_fn=_refresh_stub(
                verdict=PREVIEW_VERDICT_MEASUREMENT_FAILED,
                reason="snapshot_stale",
            ),
        )

        self.assertEqual(summary.deleted, 1)
        self.assertEqual(summary.kept_would_import, 1)
        self.assertEqual(summary.skipped_candidate_evidence_stale, 1)
        self.assertEqual(summary.skipped_candidate_evidence_missing, 1)
        self.assertFalse(os.path.exists(delete_source))
        self.assertTrue(os.path.isdir(keep_source))
        self.assertTrue(os.path.isdir(stale_source))
        self.assertTrue(os.path.isdir(missing_source))

    def test_bulk_processes_candidate_audio_corrupt_rows(self) -> None:
        """Issue #1077, F2: a kept row with corrupt-flagged linked evidence
        must stay VISIBLE to bulk cleanup, not vanish from the queue.

        ``wrong_match_row_is_visible`` used to hide any row whose linked
        candidate evidence carried ``audio_corrupt=True`` regardless of the
        scenario that actually rejected it — a garbled grab attached to an
        ordinary ``wrong_match`` scenario became kept + banned + invisible
        forever (D1 violation: kept implies visible). The reducer's own
        audio_corrupt branch is a confident, cleanup-eligible reject, so once
        the row is visible it is processed straight through to deletion.
        """
        source = _make_source(self.tmp, "candidate-corrupt-source")
        log_id = _log_wrong_match(self.db, 1, source)
        self.db.set_download_log_candidate_evidence(
            log_id,
            _store_evidence(self.db, _evidence(source, audio_corrupt=True)),
        )

        summary = cleanup_all_wrong_matches(
            self.db, confirm_all_wrong_matches=True, cfg=_cfg(),
        )

        self.assertEqual(summary.processed, 1)
        self.assertEqual(summary.deleted, 1)
        self.assertEqual(len(summary.results), 1)
        result = summary.results[0]
        self.assertEqual(result.outcome, OUTCOME_DELETED)
        self.assertTrue(result.success)
        self.assertEqual(result.reason, "audio_corrupt")
        self.assertFalse(os.path.exists(source))

    def _make_stale_row(self, name: str) -> tuple[str, int]:
        """Wrong-match row whose evidence predates a late-arriving file."""
        source = _make_source(self.tmp, name)
        log_id = _log_wrong_match(self.db, 1, source)
        self.db.set_download_log_candidate_evidence(
            log_id,
            _store_evidence(self.db, _evidence(source)),
        )
        with open(os.path.join(source, "02.mp3"), "wb") as handle:
            handle.write(b"late arrival")
        return source, log_id

    def test_stale_evidence_refreshes_then_classifies(self) -> None:
        """Issue #271: stale candidate evidence re-measures and re-decides."""
        source, log_id = self._make_stale_row("refresh-source")
        fresh = _evidence(
            source,
            audio_corrupt=True,
            files=snapshot_audio_files(source),
        )
        preview_fn = _refresh_stub(persist_evidence=fresh)

        result = cleanup_wrong_match(
            self.db, log_id, cfg=_cfg(), preview_fn=preview_fn,
        )

        self.assertEqual(result.outcome, OUTCOME_DELETED)
        self.assertFalse(os.path.exists(source))
        self.assertEqual(len(preview_fn.calls), 1)
        call = preview_fn.calls[0]
        self.assertEqual(call["request_id"], 1)
        self.assertEqual(call["path"], source)
        self.assertEqual(call["download_log_id"], log_id)

    def test_stale_refresh_failure_keeps_stale_skip(self) -> None:
        source, log_id = self._make_stale_row("refresh-fail-source")
        preview_fn = _refresh_stub(
            verdict=PREVIEW_VERDICT_MEASUREMENT_FAILED,
            reason="materialization_error",
        )

        result = cleanup_wrong_match(
            self.db, log_id, cfg=_cfg(), preview_fn=preview_fn,
        )

        self.assertEqual(
            result.outcome, OUTCOME_SKIPPED_CANDIDATE_EVIDENCE_STALE,
        )
        self.assertIn("evidence_refresh_failed", result.reason or "")
        self.assertIn("materialization_error", result.reason or "")
        self.assertTrue(os.path.isdir(source))

    def test_stale_refresh_crash_keeps_stale_skip(self) -> None:
        source, log_id = self._make_stale_row("refresh-crash-source")

        def crashing_preview(db, **_kwargs):
            raise RuntimeError("preview blew up")

        result = cleanup_wrong_match(
            self.db, log_id, cfg=_cfg(), preview_fn=crashing_preview,
        )

        self.assertEqual(
            result.outcome, OUTCOME_SKIPPED_CANDIDATE_EVIDENCE_STALE,
        )
        self.assertIn("evidence_refresh_crashed", result.reason or "")
        self.assertTrue(os.path.isdir(source))

    def test_stale_after_refresh_skips_without_looping(self) -> None:
        """Refresh that leaves evidence stale must not retry forever."""
        source, log_id = self._make_stale_row("still-stale-source")
        # evidence_ready verdict but nothing persisted: reload stays stale.
        preview_fn = _refresh_stub()

        result = cleanup_wrong_match(
            self.db, log_id, cfg=_cfg(), preview_fn=preview_fn,
        )

        self.assertEqual(
            result.outcome, OUTCOME_SKIPPED_CANDIDATE_EVIDENCE_STALE,
        )
        self.assertIn("after_refresh", result.reason or "")
        self.assertEqual(len(preview_fn.calls), 1)
        self.assertTrue(os.path.isdir(source))

    def test_fresh_evidence_does_not_invoke_refresh(self) -> None:
        source = _make_source(self.tmp, "fresh-no-refresh-source")
        log_id = _log_wrong_match(self.db, 1, source)
        self.db.set_download_log_candidate_evidence(
            log_id,
            _store_evidence(self.db, _evidence(source, audio_corrupt=True)),
        )
        preview_fn = _refresh_stub()

        result = cleanup_wrong_match(
            self.db, log_id, cfg=_cfg(), preview_fn=preview_fn,
        )

        self.assertEqual(result.outcome, OUTCOME_DELETED)
        self.assertEqual(preview_fn.calls, [])

    def test_missing_candidate_evidence_is_not_refreshed(self) -> None:
        """Refresh covers stale only — missing-FK rows stay manual (#271)."""
        source = _make_source(self.tmp, "missing-no-refresh-source")
        log_id = _log_wrong_match(self.db, 1, source)
        preview_fn = _refresh_stub()

        result = cleanup_wrong_match(
            self.db, log_id, cfg=_cfg(), preview_fn=preview_fn,
        )

        self.assertEqual(
            result.outcome, OUTCOME_SKIPPED_CANDIDATE_EVIDENCE_MISSING,
        )
        self.assertEqual(preview_fn.calls, [])
        self.assertTrue(os.path.isdir(source))

    def test_stuck_skip_outcomes_persist_recents_triage_audit(self) -> None:
        """Issue #271: stuck skips leave an audit row the UI can render."""
        _source, log_id = self._make_stale_row("audit-stale-source")
        preview_fn = _refresh_stub(
            verdict=PREVIEW_VERDICT_MEASUREMENT_FAILED,
            reason="snapshot_stale",
        )

        cleanup_wrong_match(self.db, log_id, cfg=_cfg(), preview_fn=preview_fn)

        by_id = {row.id: row for row in self.db.download_logs}
        triage = by_id[log_id].validation_result["wrong_match_triage"]
        self.assertEqual(
            triage["outcome"], OUTCOME_SKIPPED_CANDIDATE_EVIDENCE_STALE,
        )
        self.assertEqual(
            triage["action"], OUTCOME_SKIPPED_CANDIDATE_EVIDENCE_STALE,
        )
        # success=False is a default and omit_defaults keeps it out of the
        # JSONB; the typed decode restores it.
        decoded = decode_validation_envelope(
            by_id[log_id].validation_result).wrong_match_triage
        assert decoded is not None
        self.assertFalse(decoded.success)
        self.assertIn("evidence_refresh_failed", triage["reason"])

        missing_source = _make_source(self.tmp, "audit-missing-source")
        missing_id = _log_wrong_match(self.db, 1, missing_source)
        cleanup_wrong_match(
            self.db, missing_id, cfg=_cfg(), preview_fn=preview_fn,
        )
        by_id = {row.id: row for row in self.db.download_logs}
        triage = by_id[missing_id].validation_result["wrong_match_triage"]
        self.assertEqual(
            triage["outcome"], OUTCOME_SKIPPED_CANDIDATE_EVIDENCE_MISSING,
        )

    def test_transient_skip_outcomes_do_not_persist_triage_audit(self) -> None:
        source = _make_source(self.tmp, "audit-transient-source")
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
        by_id = {row.id: row for row in self.db.download_logs}
        self.assertNotIn(
            "wrong_match_triage", by_id[log_id].validation_result,
        )

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
                    matched_bad_audio_hash=True,
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
        self.assertEqual(deleted_triage["preview_decision"], "bad_audio_hash")
        self.assertIn("preimport_bad_hash:reject_bad_hash",
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
            _store_evidence(self.db, _evidence(source, matched_bad_audio_hash=True)),
        )
        cleanup = WrongMatchCleanupResult(
            download_log_id=log_id,
            entry_found=True,
            error="permission denied",
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
            _store_evidence(self.db, _evidence(source, matched_bad_audio_hash=True)),
        )
        cleanup = WrongMatchCleanupResult(
            download_log_id=log_id,
            entry_found=True,
            path_missing=True,
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
        # Issue #271: missing-path skips are stuck states — audited.
        triage = self.db.download_logs[-1].validation_result["wrong_match_triage"]
        self.assertEqual(triage["outcome"], OUTCOME_SKIPPED_MISSING_PATH)

    def test_operational_failure_is_counted_at_service_layer(self) -> None:
        source = _make_source(self.tmp, "operational-source")
        log_id = _log_wrong_match(self.db, 1, source)
        self.db.set_download_log_candidate_evidence(
            log_id,
            _store_evidence(self.db, _evidence(source, matched_bad_audio_hash=True)),
        )

        def raise_get_entry(log_id: int):
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
        # (status != 'imported'), so the reducer never saw the
        # parent quality and a downgrade candidate could pass as would_import.
        source = _make_source(self.tmp, "wanted-current-source")
        log_id = _log_wrong_match(self.db, 1, source)
        candidate = msgspec.structs.replace(
            _evidence(source),
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=96,
                avg_bitrate_kbps=128,
                median_bitrate_kbps=126,
                format="MP3",
                spectral_grade="genuine",
                spectral_subject="source",
                spectral_provenance="measured",
                spectral_measurement_version=SPECTRAL_MEASUREMENT_VERSION,
            ),
        )
        self.db.set_download_log_candidate_evidence(
            log_id,
            _store_evidence(self.db, candidate),
        )
        current = msgspec.structs.replace(
            _evidence(source, mb_release_id="mbid-1"),
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=245,
                avg_bitrate_kbps=256,
                median_bitrate_kbps=252,
                format="MP3",
                spectral_grade="genuine",
                spectral_subject="installed",
                spectral_provenance="measured",
                spectral_measurement_version=SPECTRAL_MEASUREMENT_VERSION,
            ),
        )
        self._set_current_evidence_helper(
            lambda *_a, **_kw: CurrentEvidenceActionResult(
                evidence=current,
                provenance=ActionEvidenceProvenance(current_status="loaded"),
            )
        )

        result = cleanup_wrong_match(self.db, log_id, cfg=_cfg())

        self.assertEqual(result.outcome, OUTCOME_DELETED)
        self.assertFalse(os.path.exists(source))
        self.mock_current_evidence_helper.assert_called_once()
        helper_kwargs = self.mock_current_evidence_helper.call_args.kwargs
        self.assertEqual(helper_kwargs["mb_release_id"], "mbid-1")
        self.assertEqual(helper_kwargs["request_id"], 1)
        audit = decode_validation_envelope(
            next(row for row in self.db.download_logs if row.id == log_id)
            .validation_result
        ).wrong_match_triage
        assert audit is not None
        self.assertEqual(audit.candidate_measurement, candidate.measurement)
        self.assertEqual(audit.current_measurement, current.measurement)
        self.assertEqual(audit.preview_decision, "downgrade")

    def test_v1_cleanup_audit_classification_keeps_only_spectral_and_v0(self) -> None:
        """Cleanup audit cannot resurrect target-projected v1 source facts."""
        source = _make_source(self.tmp, "v1-audit-source")
        log_id = _log_wrong_match(self.db, 1, source)
        candidate = msgspec.structs.replace(
            _evidence(source),
            lineage_version=1,
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=121,
                avg_bitrate_kbps=128,
                median_bitrate_kbps=127,
                format="OPUS 128",
                spectral_grade="likely_transcode",
                spectral_bitrate_kbps=96,
                spectral_subject="source",
                spectral_provenance="measured",
                spectral_measurement_version=SPECTRAL_MEASUREMENT_VERSION,
            ),
            storage_format="OPUS 128",
            v0_metric=AlbumQualityV0Metric(
                min_bitrate_kbps=201,
                avg_bitrate_kbps=259,
                median_bitrate_kbps=255,
                subject="installed",
            ),
        )
        self.db.set_download_log_candidate_evidence(
            log_id, _store_evidence(self.db, candidate))
        current = msgspec.structs.replace(
            _evidence(source, mb_release_id="mbid-1"),
            lineage_version=1,
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=195,
                avg_bitrate_kbps=320,
                median_bitrate_kbps=320,
                format="MP3 V0",
                spectral_grade="genuine",
                spectral_bitrate_kbps=160,
                spectral_subject="installed",
                spectral_provenance="measured",
            ),
            storage_format="MP3 V0",
            v0_metric=AlbumQualityV0Metric(
                min_bitrate_kbps=202,
                avg_bitrate_kbps=260,
                median_bitrate_kbps=256,
                subject="installed",
            ),
        )
        self._set_current_evidence_helper(
            lambda *_a, **_kw: CurrentEvidenceActionResult(
                evidence=current,
                provenance=ActionEvidenceProvenance(current_status="loaded"),
            )
        )
        result = cleanup_wrong_match(self.db, log_id, cfg=_cfg())

        self.assertEqual(result.outcome, OUTCOME_DELETED)
        self.assertFalse(os.path.exists(source))
        raw = self.db.get_download_log_entry(log_id)
        assert raw is not None
        audit = decode_validation_envelope(
            raw["validation_result"]
        ).wrong_match_triage
        assert audit is not None
        assert audit.candidate_measurement is not None
        assert audit.current_measurement is not None
        self.assertIsNone(audit.candidate_measurement.format)
        self.assertIsNone(audit.candidate_measurement.avg_bitrate_kbps)
        self.assertEqual(audit.candidate_measurement.spectral_grade,
                         "likely_transcode")
        self.assertIsNone(audit.current_measurement.format)
        self.assertIsNone(audit.current_measurement.avg_bitrate_kbps)
        self.assertEqual(audit.current_measurement.spectral_grade, "genuine")
        self.assertIsNone(audit.comparison_basis)
        self.assertIsNotNone(audit.candidate_v0_probe)
        self.assertIsNotNone(audit.current_v0_probe)

        classified = build_recents_download_log_rows([dict(raw)])[0]
        self.assertIsNone(classified["source_format"])
        self.assertIsNone(classified["source_min_bitrate"])
        self.assertIsNone(classified["source_avg_bitrate"])
        self.assertIsNone(classified["existing_format"])
        self.assertIsNone(classified["existing_min_bitrate"])
        self.assertIsNone(classified["comparison_basis"])
        self.assertEqual(classified["spectral_grade"], "likely_transcode")
        self.assertEqual(classified["existing_spectral_grade"], "genuine")
        self.assertEqual(classified["v0_probe_avg_bitrate"], 259)
        self.assertEqual(classified["existing_v0_probe_avg_bitrate"], 260)

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
        # Issue #271: current-evidence skips are stuck states — audited.
        by_id = {row.id: row for row in self.db.download_logs}
        triage = by_id[log_id].validation_result["wrong_match_triage"]
        self.assertEqual(
            triage["outcome"], OUTCOME_SKIPPED_CURRENT_EVIDENCE_FAILED,
        )

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
                provenance="measured",
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

        cleanup = WrongMatchCleanupResult(
            download_log_id=log_id,
            entry_found=True,
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
        self.assertEqual(len(self.db.advisory_lock_calls), 1,
                         "verified-lossless short-circuit must acquire the WMCL advisory lock")

    def test_replace_during_lossless_lock_cleanup_is_not_reported_success(self) -> None:
        class NarrowingRaceDB(FakePipelineDB):
            def __init__(self) -> None:
                super().__init__()
                self.raced = False

            def update_request_fields(
                self,
                request_id: int,
                *,
                expected_status: str | None = None,
                **fields: object,
            ) -> bool:
                if "search_filetype_override" in fields and not self.raced:
                    self.raced = True
                    self.supersede_request_mbid(
                        request_id,
                        new_mb_release_id="replace-during-cleanup-descendant",
                        new_mb_release_group_id=None,
                        new_mb_artist_id=None,
                        new_artist_name="Correct Artist",
                        new_album_title="Correct pressing",
                        new_year=None,
                        new_country=None,
                        new_tracks=[],
                    )
                return super().update_request_fields(
                    request_id,
                    expected_status=expected_status,
                    **fields,
                )

        self.db = NarrowingRaceDB()
        self.db.seed_request(make_request_row(
            id=1,
            status="wanted",
            mb_release_id="mbid-1",
        ))
        source = _make_source(self.tmp, "replace-during-cleanup")
        log_id = _log_wrong_match(self.db, 1, source)
        self.db.set_download_log_candidate_evidence(
            log_id,
            _store_evidence(self.db, _evidence(source)),
        )
        current = msgspec.structs.replace(
            _evidence(source),
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=100,
                avg_bitrate_kbps=119,
                median_bitrate_kbps=115,
                format="Opus",
                is_cbr=False,
            ),
            storage_format="Opus",
            codec="opus",
            container="ogg",
            v0_metric=AlbumQualityV0Metric(
                min_bitrate_kbps=210,
                avg_bitrate_kbps=240,
                median_bitrate_kbps=235,
                subject="source",
                provenance="measured",
            ),
        )
        self._set_current_evidence_helper(
            lambda *_a, **_kw: CurrentEvidenceActionResult(
                evidence=current,
                provenance=ActionEvidenceProvenance(current_status="loaded"),
            )
        )

        result = cleanup_wrong_match(self.db, log_id, cfg=_cfg())

        self.assertEqual(result.outcome, OUTCOME_SKIPPED_OPERATIONAL)
        self.assertFalse(result.success)
        self.assertEqual(result.reason, "request_changed_during_cleanup")
        self.assertEqual(result.preview_decision, "lossless_source_locked")
        self.assertFalse(os.path.exists(source))
        row = self.db.request(1)
        self.assertEqual(row["status"], "replaced")
        self.assertIsNone(row.get("search_filetype_override"))

    def test_backfilled_verified_lossless_routes_through_reducer(self) -> None:
        """Carried proof remains decisive without using the cleanup shortcut."""
        source = _make_source(self.tmp, "backfilled-verified-lossless-source")
        log_id = _log_wrong_match(self.db, 1, source)
        self.db.set_download_log_candidate_evidence(
            log_id,
            _store_evidence(self.db, _evidence(source)),
        )
        current = msgspec.structs.replace(
            _evidence(source, mb_release_id="mbid-1"),
            verified_lossless_proof=VerifiedLosslessProof(
                provenance="measured",
                source="library_audit",
                classifier="verified_lossless",
            ),
        )
        self._set_current_evidence_helper(
            lambda *_a, **_kw: CurrentEvidenceActionResult(
                evidence=current,
                provenance=ActionEvidenceProvenance(current_status="backfilled"),
            )
        )

        with patch(
            "lib.wrong_match_cleanup_service.full_pipeline_decision_from_evidence",
            wraps=__import__(
                "lib.wrong_match_cleanup_service",
                fromlist=["full_pipeline_decision_from_evidence"],
            ).full_pipeline_decision_from_evidence,
        ) as decider, patch(
            "lib.wrong_match_cleanup_service.cleanup_wrong_match_source",
        ) as cleanup_call:
            cleanup_call.return_value = WrongMatchCleanupResult(
                download_log_id=log_id,
                entry_found=True,
                request_id=1,
                deleted_path=source,
            )
            result = cleanup_wrong_match(self.db, log_id, cfg=_cfg())

        self.assertNotEqual(result.outcome, OUTCOME_DELETED_VERIFIED_LOSSLESS_PARENT)
        self.assertEqual(result.outcome, OUTCOME_DELETED)
        self.assertEqual(decider.call_count, 1)
        cleanup_call.assert_called_once()
        self.assertTrue(os.path.isdir(source))

    def test_candidate_missing_bails_before_verified_lossless_check(self) -> None:
        """Missing candidate evidence skips before the verified-lossless branch fires."""
        source = _make_source(self.tmp, "candidate-missing-source")
        log_id = _log_wrong_match(self.db, 1, source)
        # NOTE: no set_download_log_candidate_evidence → candidate loader returns None.
        current = msgspec.structs.replace(
            _evidence(source, mb_release_id="mbid-1"),
            verified_lossless_proof=VerifiedLosslessProof(
                provenance="measured",
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


if TYPE_CHECKING:
    from typing import cast

    from lib.pipeline_db import PipelineDB
    from lib.wrong_match_cleanup_service import WrongMatchCleanupDB as _CleanupDB

    # Static parity proof: production callers pass db as Any, so without
    # these assignments pyright never structurally checks the impls against
    # the protocol. A signature mismatch fails pyright here.
    _pipeline_db_satisfies_cleanup_protocol: _CleanupDB = cast("PipelineDB", None)
    _fake_db_satisfies_cleanup_protocol: _CleanupDB = cast("FakePipelineDB", None)


class TestOperatorIncompleteMarkKeepsTheFolder(unittest.TestCase):
    """Issue #1241, request 1852 — Dirt Dress *Theme Songs* (Discogs 4738671).

    The live incident, driven end-to-end through the reducer that actually
    deleted it. beets rejected the candidate as ``high_distance``
    (distance=0.1667), the row landed in Wrong Matches, and the cleanup
    reducer asked the quality question. The installed copy is 4 files /
    719.4 s of AAC ~128 with 700 s of declared program (Discogs position
    1B, "Peter and the Wolf") simply absent; the candidate is 5 files /
    1419.9 s of MP3 ~196 and complete. The comparison said ``equivalent``,
    the reducer read that as ``downgrade``, and deleted the only copy of
    the missing half hour.

    Under #1241 the OPERATOR marks the request incomplete
    (``album_requests.marked_incomplete_at``). With the mark set and the
    row's persisted scenario proving the candidate whole, the shared
    decider disregards the installed side, the decision is import-class,
    and the reducer KEEPS the folder — visible in the worklist for a
    one-click force import.

    On the peer: it IS still denylisted, and deliberately so. That write
    happens in Lane A at beets-reject time
    (``album_source.py::reject_and_requeue``), not here — the reducer
    writes no denylist at all — and it is what stops the next cycle
    re-downloading the identical bytes from the identical peer while the
    operator decides. ``kept + banned + visible`` is issue #1077's
    invariant verbatim (CLAUDE.md), so nothing in that contract moves.
    """

    def setUp(self) -> None:
        self.tmp = tempfile.mkdtemp()
        self.db = FakePipelineDB()
        self.db.seed_request(make_request_row(
            id=1, status="wanted", mb_release_id="mbid-1",
        ))
        self._current_evidence_helper = lambda *_a, **_kw: None
        helper_patch = _patch_current_evidence_helper(
            lambda *a, **kw: self._current_evidence_helper(*a, **kw),
        )
        self.addCleanup(helper_patch.stop)
        helper_patch.start()

    def tearDown(self) -> None:
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _seed(
        self, *, scenario: str, marked: bool, current_proof: bool = False,
    ) -> tuple[str, int]:
        if marked:
            self.db.set_marked_incomplete(1, marked=True)
        source = _make_source(self.tmp, f"dirt-dress-{scenario}")
        log_id = self.db.log_download(
            1,
            outcome="rejected",
            validation_result={
                "scenario": scenario,
                "distance": 0.1667,
                "failed_path": source,
            },
        )
        candidate = msgspec.structs.replace(
            _evidence(source),
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=196,
                avg_bitrate_kbps=196,
                median_bitrate_kbps=196,
                format="MP3",
            ),
            storage_format="MP3",
        )
        self.db.set_download_log_candidate_evidence(
            log_id, _store_evidence(self.db, candidate))
        current = msgspec.structs.replace(
            _evidence(source, mb_release_id="mbid-1"),
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=128,
                avg_bitrate_kbps=128,
                median_bitrate_kbps=128,
                format="AAC",
            ),
            codec="aac",
            container="m4a",
            storage_format="AAC",
            filetype_band="aac",
        )
        if current_proof:
            current = msgspec.structs.replace(
                current,
                verified_lossless_proof=VerifiedLosslessProof(
                    provenance="measured",
                    source="library_audit",
                    classifier="verified_lossless",
                    detail="parent_lossless_proof",
                ),
            )
        self._current_evidence_helper = (
            lambda *_a, **_kw: CurrentEvidenceActionResult(
                evidence=current,
                provenance=ActionEvidenceProvenance(current_status="loaded"),
            )
        )
        return source, log_id

    def _triage(self, log_id: int):
        raw = self.db.get_download_log_entry(log_id)
        assert raw is not None
        audit = decode_validation_envelope(
            raw["validation_result"]
        ).wrong_match_triage
        assert audit is not None
        return audit

    def test_high_distance_against_a_marked_install_is_kept(self) -> None:
        source, log_id = self._seed(scenario="high_distance", marked=True)

        def _unreachable(*_a: object, **_kw: object) -> None:
            raise AssertionError(
                "the reducer must not reach the deletion helper when the "
                "operator's mark makes the candidate import-class"
            )

        with patch.object(
            wrong_match_cleanup_service,
            "_perform_cleanup_deletion",
            _unreachable,
        ):
            result = cleanup_wrong_match(self.db, log_id, cfg=_cfg())

        self.assertEqual(result.outcome, OUTCOME_KEPT_WOULD_IMPORT)
        self.assertEqual(result.verdict, "would_import")
        self.assertFalse(result.cleanup_eligible)
        self.assertTrue(os.path.isdir(source))
        self.assertTrue(os.path.exists(os.path.join(source, "01.mp3")))

        audit = self._triage(log_id)
        self.assertEqual(audit.action, OUTCOME_KEPT_WOULD_IMPORT)
        self.assertFalse(audit.success)
        self.assertEqual(audit.preview_decision, "import")
        self.assertFalse(audit.cleanup_eligible)
        self.assertIn("stage2_import:import", audit.stage_chain)
        # No comparison ran — the installed side was disregarded, so the
        # audit honestly carries no basis rather than a fabricated one.
        self.assertIsNone(audit.comparison_basis)

    def test_extra_tracks_against_a_marked_install_still_deletes(
        self,
    ) -> None:
        """The load-bearing candidate conjunct, at the reducer.

        ``extra_tracks`` is one of the four delete-eligible scenarios, and
        it is beets' proof that the CANDIDATE is missing a declared track.
        An incomplete candidate must never "upgrade" a marked install, so
        this row keeps today's destructive outcome.
        """
        source, log_id = self._seed(scenario="extra_tracks", marked=True)

        result = cleanup_wrong_match(self.db, log_id, cfg=_cfg())

        self.assertEqual(result.outcome, OUTCOME_DELETED)
        self.assertFalse(os.path.exists(source))
        self.assertEqual(self._triage(log_id).preview_decision, "downgrade")

    def test_high_distance_against_an_unmarked_install_still_deletes(
        self,
    ) -> None:
        """Control: the mark is the trigger, not the scenario."""
        source, log_id = self._seed(scenario="high_distance", marked=False)

        result = cleanup_wrong_match(self.db, log_id, cfg=_cfg())

        self.assertEqual(result.outcome, OUTCOME_DELETED)
        self.assertFalse(os.path.exists(source))
        self.assertEqual(self._triage(log_id).preview_decision, "downgrade")

    def test_marked_install_disarms_the_verified_lossless_parent_shortcut(
        self,
    ) -> None:
        """A proof-LOCKED parent normally short-circuits deletion before the
        decider ever runs (``TestVerifiedLosslessShortCircuit``). With the
        mark set and this row's candidate proven whole, the shortcut's
        "guaranteed to lose the upgrade gate" premise no longer holds, so
        the full decider runs, disregards the locked installed side, and
        the folder is KEPT."""
        source, log_id = self._seed(
            scenario="high_distance", marked=True, current_proof=True,
        )

        result = cleanup_wrong_match(self.db, log_id, cfg=_cfg())

        self.assertEqual(result.outcome, OUTCOME_KEPT_WOULD_IMPORT)
        self.assertNotEqual(
            result.outcome, OUTCOME_DELETED_VERIFIED_LOSSLESS_PARENT)
        self.assertTrue(os.path.isdir(source))
        self.assertEqual(self._triage(log_id).preview_decision, "import")

    def test_unmarked_install_keeps_the_verified_lossless_parent_shortcut(
        self,
    ) -> None:
        """Control: without the mark the shortcut still deletes — the
        disarm is keyed on the operator's mark, never on the proof."""
        source, log_id = self._seed(
            scenario="high_distance", marked=False, current_proof=True,
        )

        result = cleanup_wrong_match(self.db, log_id, cfg=_cfg())

        self.assertEqual(
            result.outcome, OUTCOME_DELETED_VERIFIED_LOSSLESS_PARENT)
        self.assertFalse(os.path.exists(source))


class TestCleanupDBProtocolParity(unittest.TestCase):
    """#409: PipelineDB and FakePipelineDB must satisfy WrongMatchCleanupDB.

    issubclass on the runtime_checkable Protocol guards method *presence*;
    signature parity is pyright's job (the protocol-typed params make every
    call site a structural check, plus the TYPE_CHECKING assignment above).
    """

    def test_pipeline_db_satisfies_protocol(self) -> None:
        from lib.pipeline_db import PipelineDB
        from lib.wrong_match_cleanup_service import WrongMatchCleanupDB

        self.assertTrue(issubclass(PipelineDB, WrongMatchCleanupDB))

    def test_fake_pipeline_db_satisfies_protocol(self) -> None:
        from lib.wrong_match_cleanup_service import WrongMatchCleanupDB

        self.assertTrue(issubclass(FakePipelineDB, WrongMatchCleanupDB))


class TestSummaryOutcomeContract(unittest.TestCase):
    """Guard: OUTCOME_KEYS and the Summary's count fields cannot drift (#411)."""

    def test_outcome_keys_equal_struct_count_fields(self) -> None:
        from lib.wrong_match_cleanup_service import (
            OUTCOME_KEYS,
            WrongMatchCleanupSummary,
        )

        count_fields = (
            set(WrongMatchCleanupSummary.__struct_fields__)
            - {"processed", "results", "cancelled"}
        )
        self.assertEqual(set(OUTCOME_KEYS), count_fields)

    def test_summary_counts_each_outcome_into_its_field(self) -> None:
        from lib.wrong_match_cleanup_service import (
            OUTCOME_KEYS,
            WrongMatchCleanupOutcome,
            _summary,
        )

        for key in OUTCOME_KEYS:
            with self.subTest(outcome=key):
                summary = _summary([
                    WrongMatchCleanupOutcome(download_log_id=1, outcome=key),
                ])
                self.assertEqual(getattr(summary, key), 1)
                self.assertEqual(summary.processed, 1)
                for other in OUTCOME_KEYS:
                    if other != key:
                        self.assertEqual(getattr(summary, other), 0)

    def test_summary_preserves_results_and_ignores_unknown_outcomes(self) -> None:
        from lib.wrong_match_cleanup_service import (
            OUTCOME_DELETED,
            WrongMatchCleanupOutcome,
            _summary,
        )

        results = [
            WrongMatchCleanupOutcome(download_log_id=1, outcome=OUTCOME_DELETED),
            WrongMatchCleanupOutcome(download_log_id=2, outcome="not_a_real_outcome"),
        ]
        summary = _summary(results)
        self.assertEqual(summary.processed, 2)
        self.assertEqual(summary.deleted, 1)
        self.assertEqual(summary.results, tuple(results))


class TestCancellation(unittest.TestCase):
    """Issue #1083 — deterministic pins per stop point.

    Reuse the token, not the raise: ``cleanup_all_wrong_matches`` reads
    ``cancellation_token.cancelled`` as a plain boolean and breaks
    between rows; it never calls ``raise_if_cancelled()``. A generated
    property patrolling the "never a partially-deleted directory"
    invariant over arbitrary queues lives in
    ``tests/test_wrong_match_cleanup_service_generated.py``.
    """

    def setUp(self) -> None:
        self.tmp = tempfile.mkdtemp()
        self.db = FakePipelineDB()
        self.db.seed_request(make_request_row(
            id=1, status="wanted", mb_release_id="mbid-1",
        ))
        helper_patch = _patch_current_evidence_helper()
        self.addCleanup(helper_patch.stop)
        helper_patch.start()

    def tearDown(self) -> None:
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _confident_reject_row(self, name: str) -> tuple[str, int]:
        """A row whose candidate is a guaranteed confident_reject —
        deterministic deletion with no dependency on current evidence."""
        source = _make_source(self.tmp, name)
        log_id = _log_wrong_match(self.db, 1, source)
        self.db.set_download_log_candidate_evidence(
            log_id,
            _store_evidence(
                self.db,
                _evidence(
                    source,
                    mb_release_id=f"mbid-{name}",
                    matched_bad_audio_hash=True,
                ),
            ),
        )
        return source, log_id

    def test_cancel_before_first_row_processes_nothing(self) -> None:
        source, _log_id = self._confident_reject_row("before-first")
        token = CancellationToken()
        token.cancel("operator_stop")

        summary = cleanup_all_wrong_matches(
            self.db, confirm_all_wrong_matches=True, cfg=_cfg(),
            cancellation_token=token,
        )

        self.assertTrue(summary.cancelled)
        self.assertEqual(summary.processed, 0)
        self.assertEqual(summary.results, ())
        self.assertTrue(os.path.isdir(source))
        # Nothing ran, so nothing was audited either.
        self.assertNotIn(
            "wrong_match_triage",
            self.db.download_logs[-1].validation_result,
        )

    def test_cancel_mid_queue_stops_after_the_current_row(self) -> None:
        sources_by_id: dict[int, str] = {}
        for name in ("mid-1", "mid-2", "mid-3"):
            source, log_id = self._confident_reject_row(name)
            sources_by_id[log_id] = source
        # Don't assume queue order -- ask the real projection, exactly
        # like the service does, and cancel after whichever row it
        # processes first.
        ordered_ids = [
            row["download_log_id"] for row in self.db.get_wrong_matches()
        ]
        self.assertEqual(len(ordered_ids), 3)
        first_id, second_id, third_id = ordered_ids
        token = CancellationToken()
        real_cleanup_wrong_match = wrong_match_cleanup_service.cleanup_wrong_match

        def cancel_after_first(db, download_log_id, **kwargs):
            result = real_cleanup_wrong_match(db, download_log_id, **kwargs)
            if download_log_id == first_id:
                token.cancel("operator_stop")
            return result

        with patch(
            "lib.wrong_match_cleanup_service.cleanup_wrong_match",
            side_effect=cancel_after_first,
        ):
            summary = cleanup_all_wrong_matches(
                self.db, confirm_all_wrong_matches=True, cfg=_cfg(),
                cancellation_token=token,
            )

        self.assertTrue(summary.cancelled)
        self.assertEqual(summary.processed, 1)
        self.assertEqual(len(summary.results), 1)
        self.assertEqual(summary.results[0].download_log_id, first_id)
        self.assertEqual(summary.deleted, 1)
        # Exactly the row before the stop was deleted and audited; the
        # rest are untouched -- the partial summary is never discarded.
        self.assertFalse(os.path.exists(sources_by_id[first_id]))
        self.assertTrue(os.path.isdir(sources_by_id[second_id]))
        self.assertTrue(os.path.isdir(sources_by_id[third_id]))
        audited = {row.id: row for row in self.db.download_logs}
        self.assertIn(
            "wrong_match_triage", audited[first_id].validation_result)
        self.assertNotIn(
            "wrong_match_triage", audited[second_id].validation_result)
        self.assertNotIn(
            "wrong_match_triage", audited[third_id].validation_result)

    def test_cancel_after_last_row_reports_full_completion_not_cancelled(
        self,
    ) -> None:
        """Cancelling exactly as the last row finishes leaves nothing for
        the loop to stop -- it never re-checks the token once iteration
        is over, so the summary reports the honest complete run rather
        than claiming a cancellation it never observed."""
        sources_by_id: dict[int, str] = {}
        for name in ("last-1", "last-2"):
            source, log_id = self._confident_reject_row(name)
            sources_by_id[log_id] = source
        ordered_ids = [
            row["download_log_id"] for row in self.db.get_wrong_matches()
        ]
        self.assertEqual(len(ordered_ids), 2)
        last_id = ordered_ids[-1]
        token = CancellationToken()
        real_cleanup_wrong_match = wrong_match_cleanup_service.cleanup_wrong_match

        def cancel_after_last(db, download_log_id, **kwargs):
            result = real_cleanup_wrong_match(db, download_log_id, **kwargs)
            if download_log_id == last_id:
                token.cancel("operator_stop")
            return result

        with patch(
            "lib.wrong_match_cleanup_service.cleanup_wrong_match",
            side_effect=cancel_after_last,
        ):
            summary = cleanup_all_wrong_matches(
                self.db, confirm_all_wrong_matches=True, cfg=_cfg(),
                cancellation_token=token,
            )

        self.assertFalse(summary.cancelled)
        self.assertEqual(summary.processed, 2)
        self.assertEqual(summary.deleted, 2)
        for source in sources_by_id.values():
            self.assertFalse(os.path.exists(source))
        # The request landed on the token -- it was just too late to stop
        # anything, which is exactly the race this pin proves is safe.
        self.assertTrue(token.cancelled)

    def test_cancel_requires_a_break_not_a_raise(self) -> None:
        """Fault-injection guard (code-quality.md): a cancellation path
        that raises instead of breaking would unwind out of
        ``cleanup_all_wrong_matches`` before it can return a summary at
        all, losing the partial ``results`` list this issue exists to
        preserve. Proving the function returns a real, populated summary
        under an already-cancelled token is the regression guard for
        that trap."""
        source, _log_id = self._confident_reject_row("no-raise")
        token = CancellationToken()
        token.cancel("operator_stop")

        # No assertRaises here on purpose: the whole point is that this
        # call must return normally. If a future change reused the raise
        # (``raise_if_cancelled()``) instead of the token's plain boolean,
        # this call would raise ``ExecutionCancelled`` and the test would
        # fail loudly with that traceback, not swallow it.
        summary = cleanup_all_wrong_matches(
            self.db, confirm_all_wrong_matches=True, cfg=_cfg(),
            cancellation_token=token,
        )
        self.assertTrue(summary.cancelled)
        self.assertTrue(os.path.isdir(source))


if __name__ == "__main__":
    unittest.main()
