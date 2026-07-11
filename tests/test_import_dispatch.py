"""Tests for lib/dispatch/ — auto-import decision tree.

Orchestration tests (TestDispatchImport, TestQualityGate*) use FakePipelineDB
and assert domain state. Seam tests (TestOverrideMinBitrate, TestOpus*,
TestTargetFormat*) exercise the surviving auto-import seam in
``lib.download_validation._handle_valid_result`` and the core subprocess wiring.
Pure function tests (TestPopulateDlInfo*, TestCleanupStagedDir) test in/out.
"""

import os
import shutil
import subprocess as sp
import tempfile
import unittest
from unittest.mock import MagicMock, patch

from lib.config import CratediggerConfig
from lib.quality import (DownloadInfo, ImportResult, ConversionInfo,
                         DuplicateRemoveCandidate, DuplicateRemoveGuardInfo,
                         AudioQualityMeasurement, PostflightInfo,
                         QUALITY_UPGRADE_TIERS, QUALITY_FLAC_ONLY,
                         V0_PROBE_LOSSLESS_SOURCE, V0ProbeEvidence,
                         ValidationResult)
from tests.fakes import FakePipelineDB
from tests.helpers import (
    RecordingQualityGate,
    make_ctx_with_fake_db,
    make_import_result,
    make_request_row,
    noop_quality_gate,
    patch_dispatch_externals,
)


# --- Local helpers for auto-import seam tests ---

def _make_album_data(artist="Test Artist", title="Test Album",
                     mb_release_id="test-mbid", db_request_id=42,
                     db_source="request"):
    """Build a mock GrabListEntry."""
    mock = MagicMock()
    mock.artist = artist
    mock.title = title
    mock.mb_release_id = mb_release_id
    mock.db_request_id = db_request_id
    mock.db_source = db_source
    mock.db_target_format = None
    mock.files = [MagicMock(
        username="user1",
        filename="01 - Track.mp3",
        bitRate=None,
        sampleRate=None,
        bitDepth=None,
        isVariableBitRate=None,
    )]
    return mock


def _make_ctx():
    """Build a CratediggerContext wired to a seeded FakePipelineDB.

    The DB is seeded with request id 42 in ``downloading`` status — the
    auto-import dispatch path expects to find an owning request. The
    config remains a ``MagicMock`` because the tests only read a handful
    of attributes from it; ``cfg`` is not a stateful-collaborator name
    in the audit's heuristic.
    """
    cfg = MagicMock()
    cfg.beets_harness_path = "/nix/store/fake/harness/run_beets_harness.sh"
    cfg.beets_distance_threshold = 0.15
    cfg.beets_staging_dir = "/tmp/staging"
    cfg.verified_lossless_target = ""
    cfg.quality_ranks.to_json.return_value = "{}"
    fake_db = FakePipelineDB()
    fake_db.seed_request(make_request_row(id=42, status="downloading"))
    ctx = make_ctx_with_fake_db(fake_db, cfg=cfg)
    ctx.cooled_down_users = set()
    return ctx


def _make_bv_result(distance=0.05):
    """Build a mock beets validation result with attribute access."""
    mock = MagicMock()
    mock.distance = distance
    mock.scenario = "strong_match"
    mock.detail = None
    mock.error = None
    mock.to_json.return_value = '{"valid": true}'
    return mock


_HARNESS = "/nix/store/fake/harness/run_beets_harness.sh"


def _dispatch_valid_result_cmd(
    *,
    album_data=None,
    ctx=None,
    db_fields=None,
    ir=None,
):
    """Run the surviving auto-import seam and return the harness argv."""
    from lib.download_validation import _handle_valid_result
    from lib.staged_album import StagedAlbum

    album_data = album_data or _make_album_data()
    ctx = ctx or _make_ctx()
    if db_fields is not None:
        # Reseed request 42 with the test-supplied row shape. The default
        # _make_ctx() ships a downloading row keyed by id=42; tests that
        # need a different shape pass ``db_fields`` and we overwrite.
        # Force id=42 so ``_handle_valid_result`` finds the override when
        # looking up by ``album_data.db_request_id``.
        override = dict(db_fields)
        override["id"] = album_data.db_request_id
        fake_db = ctx.pipeline_db_source._get_db()
        fake_db.seed_request(override)
    bv_result = _make_bv_result()
    ir = ir or make_import_result(decision="import")

    with tempfile.TemporaryDirectory() as tmpdir:
        source_dir = os.path.join(tmpdir, "import")
        os.makedirs(source_dir)
        with open(os.path.join(source_dir, "01 - Track.mp3"), "w", encoding="utf-8") as fp:
            fp.write("fake audio")

        # Drive the real ``stage_to_ai_path`` by pointing the staging dir at
        # the tempdir. ``StagedAlbum.move_to`` creates the destination
        # directory itself, so we just need the staging root to exist.
        ctx.cfg.beets_staging_dir = tmpdir

        with patch("lib.download_validation.log_validation_result"), \
             patch_dispatch_externals() as ext, \
             patch("lib.dispatch.subprocess_runner.parse_import_result", return_value=ir):
            _handle_valid_result(
                album_data,
                bv_result,
                StagedAlbum(
                    current_path=source_dir,
                    request_id=album_data.db_request_id,
                ),
                ctx,
                quality_gate_fn=noop_quality_gate,
            )
            return ext.run.call_args[0][0]


class TestPopulateDlInfoFromImportResult(unittest.TestCase):

    def test_converted_flac_to_v0(self):
        from lib.dispatch import _populate_dl_info_from_import_result
        dl = DownloadInfo(filetype="flac")
        ir = make_import_result(was_converted=True, original_filetype="flac",
                                target_filetype="mp3", new_min_bitrate=245)
        _populate_dl_info_from_import_result(dl, ir)
        self.assertTrue(dl.was_converted)
        self.assertEqual(dl.original_filetype, "flac")
        self.assertEqual(dl.slskd_filetype, "flac")
        self.assertEqual(dl.actual_filetype, "mp3")
        self.assertTrue(dl.is_vbr)
        self.assertEqual(dl.bitrate, 245000)
        assert dl.download_spectral is not None
        self.assertEqual(dl.download_spectral.grade, "genuine")

    def test_no_conversion(self):
        from lib.dispatch import _populate_dl_info_from_import_result
        dl = DownloadInfo(filetype="mp3")
        ir = make_import_result(was_converted=False, new_min_bitrate=320)
        _populate_dl_info_from_import_result(dl, ir)
        self.assertFalse(dl.was_converted)
        self.assertEqual(dl.slskd_filetype, "mp3")
        self.assertEqual(dl.actual_filetype, "mp3")

    def test_populates_actual_min_bitrate_from_new_measurement(self):
        """Point-in-time min bitrate must land in dl.actual_min_bitrate so the
        download_log column is non-NULL. Recents UI relies on this column to
        render per-row 'upgrade X to Y' verdicts — when NULL the UI silently
        falls through to album_requests.min_bitrate (current state), painting
        every historical row with the latest value.
        Live reproducer: request 1055, rows 3628/3631 both have NULL column
        despite JSONB carrying 119 and 162.
        """
        from lib.dispatch import _populate_dl_info_from_import_result
        dl = DownloadInfo(filetype="mp3")
        ir = make_import_result(was_converted=False, new_min_bitrate=119)
        _populate_dl_info_from_import_result(dl, ir)
        self.assertEqual(dl.actual_min_bitrate, 119)

    def test_populates_actual_min_bitrate_for_flac_conversion(self):
        """Same guarantee for the FLAC→V0 conversion path — the V0 min bitrate
        is the point-in-time value and must land on the column."""
        from lib.dispatch import _populate_dl_info_from_import_result
        dl = DownloadInfo(filetype="flac")
        ir = make_import_result(was_converted=True, original_filetype="flac",
                                target_filetype="mp3", new_min_bitrate=245)
        _populate_dl_info_from_import_result(dl, ir)
        self.assertEqual(dl.actual_min_bitrate, 245)

    def test_leaves_actual_min_bitrate_none_when_measurement_missing(self):
        """If there's no new_measurement in the ImportResult, we don't
        fabricate a value — NULL is the honest signal for consumers."""
        from lib.dispatch import _populate_dl_info_from_import_result
        dl = DownloadInfo(filetype="mp3")
        ir = ImportResult(decision="import_failed", new_measurement=None)
        _populate_dl_info_from_import_result(dl, ir)
        self.assertIsNone(dl.actual_min_bitrate)

    def test_populates_v0_probe_evidence(self):
        from lib.dispatch import _populate_dl_info_from_import_result
        dl = DownloadInfo(filetype="flac")
        probe = V0ProbeEvidence(
            kind=V0_PROBE_LOSSLESS_SOURCE,
            min_bitrate_kbps=165,
            avg_bitrate_kbps=228,
            median_bitrate_kbps=225,
        )
        existing = V0ProbeEvidence(
            kind=V0_PROBE_LOSSLESS_SOURCE,
            min_bitrate_kbps=128,
            avg_bitrate_kbps=171,
            median_bitrate_kbps=169,
        )
        ir = make_import_result(
            was_converted=True,
            original_filetype="flac",
            target_filetype="mp3",
            v0_probe=probe,
            existing_v0_probe=existing,
        )

        _populate_dl_info_from_import_result(dl, ir)

        self.assertEqual(dl.v0_probe, probe)
        self.assertEqual(dl.existing_v0_probe, existing)


class TestCleanupStagedDir(unittest.TestCase):

    def test_removes_dir_and_empty_parent(self):
        from lib.dispatch import _cleanup_staged_dir
        tmpdir = tempfile.mkdtemp()
        try:
            parent = os.path.join(tmpdir, "Artist")
            staged = os.path.join(parent, "Album")
            os.makedirs(staged)
            open(os.path.join(staged, "track.mp3"), "w").close()
            _cleanup_staged_dir(staged)
            self.assertFalse(os.path.exists(staged))
            self.assertFalse(os.path.exists(parent))
        finally:
            shutil.rmtree(tmpdir, ignore_errors=True)

    def test_preserves_nonempty_parent(self):
        from lib.dispatch import _cleanup_staged_dir
        tmpdir = tempfile.mkdtemp()
        try:
            parent = os.path.join(tmpdir, "Artist")
            staged = os.path.join(parent, "Album1")
            other = os.path.join(parent, "Album2")
            os.makedirs(staged)
            os.makedirs(other)
            _cleanup_staged_dir(staged)
            self.assertFalse(os.path.exists(staged))
            self.assertTrue(os.path.exists(parent))
        finally:
            shutil.rmtree(tmpdir, ignore_errors=True)


class TestRecordRejectionAndRequeueSeam(unittest.TestCase):
    """Seam tests for the shared rejection finalizer."""

    @patch("lib.dispatch.outcome_actions.finalize_request")
    def test_requeue_defers_from_status_lookup_to_finalize_request(
        self,
        mock_finalize,
    ) -> None:
        from lib.dispatch import _record_rejection_and_maybe_requeue

        db = FakePipelineDB()
        db.seed_request(make_request_row(id=42, status="manual"))

        _record_rejection_and_maybe_requeue(
            db,  # type: ignore[arg-type]
            42,
            DownloadInfo(username="user1"),
            detail="too low",
            error=None,
            validation_result=ValidationResult(
                distance=0.5,
                scenario="quality_downgrade",
                detail="too low",
            ).to_json(),
            requeue=True,
        )

        mock_finalize.assert_called_once()
        _db_arg, request_id, outcome = mock_finalize.call_args.args
        self.assertEqual(request_id, 42)
        self.assertIsNone(outcome.from_status)
        self.assertIsNone(outcome.attempt_type)
        self.assertEqual(db.request(42)["validation_attempts"], 1)

    def test_requeue_only_forwards_fields_persisted_by_wanted_transition(self) -> None:
        from lib.dispatch import _record_rejection_and_maybe_requeue

        db = FakePipelineDB()
        db.seed_request(make_request_row(id=42, status="downloading"))

        _record_rejection_and_maybe_requeue(
            db,  # type: ignore[arg-type]
            42,
            DownloadInfo(username="user1"),
            detail="too low",
            error=None,
            validation_result=ValidationResult(
                distance=0.5,
                scenario="quality_downgrade",
                detail="too low",
            ).to_json(),
            requeue=True,
            search_filetype_override="flac,mp3 v0",
        )

        row = db.request(42)
        self.assertEqual(row["status"], "wanted")
        self.assertEqual(row["search_filetype_override"], "flac,mp3 v0")
        self.assertIsNone(row["beets_distance"])
        self.assertIsNone(row["beets_scenario"])


class TestRejectImportFromEvidenceDecision(unittest.TestCase):
    """Evidence-decision rejections must populate download_log columns.

    Bug: ``_reject_import_from_evidence_decision`` built ``ImportResult``
    JSON for the JSONB column but skipped
    ``_populate_dl_info_from_import_result``, so every top-level
    quality column landed NULL. The Recents UI rendered just
    ``"downgrade · username"`` instead of the full quality verdict.

    Live reproducer: download_log id 14570 — Faux Pas - Entropy Begins
    at Home, decision=downgrade, new=127kbps mp3 likely_transcode,
    existing=192kbps mp3 cbr. JSONB had everything; columns were all
    NULL.
    """

    def test_evidence_rejection_populates_download_log_columns(self) -> None:
        from lib.dispatch import _reject_import_from_evidence_decision
        from lib.quality import AudioQualityMeasurement, ImportResult

        db = FakePipelineDB()
        db.seed_request(make_request_row(id=42, status="downloading"))
        dl_info = DownloadInfo(filetype="mp3", username="user1")
        ir = ImportResult(
            decision="downgrade",
            new_measurement=AudioQualityMeasurement(
                min_bitrate_kbps=127,
                avg_bitrate_kbps=127,
                median_bitrate_kbps=128,
                format="MP3",
                spectral_grade="likely_transcode",
                spectral_bitrate_kbps=128,
            ),
            existing_measurement=AudioQualityMeasurement(
                min_bitrate_kbps=192,
                avg_bitrate_kbps=192,
                median_bitrate_kbps=192,
                format="MP3",
                is_cbr=True,
            ),
        )

        with patch_dispatch_externals():
            _reject_import_from_evidence_decision(
                db=db,  # type: ignore[arg-type]
                request_id=42,
                dl_info=dl_info,
                import_result=ir,
                distance=0.1279,
                decision="downgrade",
                detail="import-time persisted evidence rejected candidate",
                requeue_on_failure=True,
                validation_result=None,
                staged_path="/tmp/cratedigger-evidence-reject-test",
                scenario="downgrade",
                files=None,
                source_path_cleanup_scenario="downgrade",
                cooled_down_users=None,
            )

        self.assertEqual(len(db.download_logs), 1)
        log = db.download_logs[0]
        self.assertEqual(log.outcome, "rejected")
        self.assertEqual(log.beets_scenario, "downgrade")
        self.assertEqual(log.beets_distance, 0.1279)
        # Top-level quality columns the UI reads.
        self.assertEqual(log.extra["actual_filetype"], "mp3")
        self.assertEqual(log.extra["slskd_filetype"], "mp3")
        self.assertEqual(log.extra["bitrate"], 127_000)
        self.assertEqual(log.extra["actual_min_bitrate"], 127)
        self.assertEqual(log.extra["spectral_grade"], "likely_transcode")
        self.assertEqual(log.extra["spectral_bitrate"], 128)
        self.assertEqual(log.extra["existing_min_bitrate"], 192)
        self.assertEqual(log.extra["existing_spectral_bitrate"], None)
        # The full ImportResult is still serialized into the JSONB.
        self.assertIsNotNone(log.import_result)


class TestRejectImportFromEvidenceDecisionForcedRequeue(unittest.TestCase):
    """U11 invariant: the four folder/audio-integrity facts always self-heal.

    ``_PREIMPORT_FACT_REJECT_DECISIONS`` lists the four decisions the deleted
    ``_route_preimport_decision_reject`` helper used to handle. After the
    fold-in, ``_reject_import_from_evidence_decision`` is the single helper
    for all rejects and must preserve the old "always requeue on four-fact
    reject" rule — even when the caller passes ``requeue_on_failure=False``
    (force/manual paths). Spectral/quality-side rejects (e.g., ``downgrade``)
    honor the caller's flag normally; only the four-fact reasons override.
    """

    FOUR_FACT_DECISIONS = ["audio_corrupt", "bad_audio_hash", "nested_layout", "empty_fileset"]

    def _reject(self, *, decision: str, requeue_on_failure: bool):
        from lib.dispatch import _reject_import_from_evidence_decision
        from lib.quality import AudioQualityMeasurement, ImportResult

        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42, status="downloading",
            mb_release_id="test-mbid",
        ))
        dl_info = DownloadInfo(filetype="mp3", username="user1")
        ir = ImportResult(
            decision=decision,
            new_measurement=AudioQualityMeasurement(
                min_bitrate_kbps=320,
                avg_bitrate_kbps=320,
                median_bitrate_kbps=320,
                format="MP3",
                is_cbr=True,
            ),
        )
        with patch_dispatch_externals():
            _reject_import_from_evidence_decision(
                db=db,  # type: ignore[arg-type]
                request_id=42,
                dl_info=dl_info,
                import_result=ir,
                distance=0.0,
                decision=decision,
                detail=f"test {decision}",
                requeue_on_failure=requeue_on_failure,
                validation_result=None,
                staged_path="/tmp/cratedigger-forced-requeue-test",
                scenario=decision,
                files=None,
                source_path_cleanup_scenario=decision,
                cooled_down_users=None,
            )
        return db

    def test_four_fact_rejects_force_requeue_even_when_caller_says_no(self) -> None:
        for decision in self.FOUR_FACT_DECISIONS:
            with self.subTest(decision=decision):
                db = self._reject(decision=decision, requeue_on_failure=False)
                # The request must be back in 'wanted' — self-heal invariant.
                self.assertEqual(
                    db.request(42)["status"],
                    "wanted",
                    f"{decision} reject with requeue_on_failure=False must "
                    f"still self-heal the request back to 'wanted' (the "
                    f"album is still desired; only this source is bad).",
                )

    def test_four_fact_rejects_also_requeue_when_caller_says_yes(self) -> None:
        # Baseline: requeue_on_failure=True keeps the same self-heal behavior.
        for decision in self.FOUR_FACT_DECISIONS:
            with self.subTest(decision=decision):
                db = self._reject(decision=decision, requeue_on_failure=True)
                self.assertEqual(db.request(42)["status"], "wanted")

    def test_quality_reject_honors_requeue_flag(self) -> None:
        # Non-four-fact reject (downgrade) must NOT be force-requeued.
        # When the caller passes requeue_on_failure=False the request stays
        # in its current status — the operator chose to act on this source.
        db = self._reject(decision="downgrade", requeue_on_failure=False)
        self.assertEqual(db.request(42)["status"], "downloading")


class TestDispatchImport(unittest.TestCase):
    """Orchestration tests — assert domain state via FakePipelineDB."""

    _SENTINEL = object()

    def _dispatch(self, ir=_SENTINEL, request_overrides=None):
        from lib.dispatch import dispatch_import_core
        if ir is self._SENTINEL:
            ir = make_import_result(decision="import")

        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42, status="downloading",
            **(request_overrides or {}),
        ))
        cfg = CratediggerConfig(
            beets_harness_path=_HARNESS,
            pipeline_db_enabled=True,
        )
        dl_info = DownloadInfo(filetype="mp3")

        mock_gate = RecordingQualityGate()
        tmpdir = tempfile.mkdtemp()
        try:
            with patch_dispatch_externals() as ext, \
                 patch("lib.dispatch.subprocess_runner.parse_import_result", return_value=ir):
                dispatch_import_core(
                    path=tmpdir,
                    mb_release_id="test-mbid",
                    request_id=42,
                    label="Test Artist - Test Album",
                    beets_harness_path=_HARNESS,
                    db=db,  # type: ignore[arg-type]
                    dl_info=dl_info,
                    distance=0.05,
                    scenario="strong_match",
                    files=[MagicMock(username="user1",
                                     filename="01 - Track.mp3")],
                    cfg=cfg,
                    quality_gate_fn=mock_gate,
                )
        finally:
            shutil.rmtree(tmpdir, ignore_errors=True)

        return {
            "db": db,
            "mock_cleanup": ext.cleanup,
            "mock_meelo": ext.meelo,
            "mock_jellyfin": ext.jellyfin,
            "mock_gate": mock_gate,
        }

    def test_import_success(self):
        ir = make_import_result(decision="import")
        r = self._dispatch(ir)
        self.assertEqual(r["db"].request(42)["status"], "imported")
        self.assertEqual(len(r["db"].download_logs), 1)
        self.assertEqual(r["db"].download_logs[0].outcome, "success")
        r["mock_meelo"].assert_called_once()
        r["mock_jellyfin"].assert_called_once()
        r["mock_cleanup"].assert_called_once()
        r["mock_gate"].assert_called_once()

    def test_import_with_bad_extensions_logs_error_and_persists_jsonb(self):
        from lib.quality import ImportResult

        ir = make_import_result(decision="import")
        ir.postflight.bad_extensions = ["01 Track.bak"]

        with self.assertLogs("cratedigger", level="ERROR") as logs:
            r = self._dispatch(ir)

        self.assertIn("POSTFLIGHT BAD EXTENSIONS", "\n".join(logs.output))
        raw = r["db"].download_logs[0].import_result
        assert isinstance(raw, str)
        persisted = ImportResult.from_json(raw)
        self.assertEqual(persisted.postflight.bad_extensions,
                         ["01 Track.bak"])

    def test_preflight_existing(self):
        ir = make_import_result(decision="preflight_existing")
        r = self._dispatch(ir)
        self.assertEqual(r["db"].request(42)["status"], "imported")
        self.assertEqual(r["db"].download_logs[0].outcome, "success")
        r["mock_meelo"].assert_called_once()

    def test_import_with_upgrade_delta(self):
        ir = make_import_result(decision="import", new_min_bitrate=245,
                                prev_min_bitrate=192)
        r = self._dispatch(ir)
        self.assertEqual(r["db"].request(42)["status"], "imported")

    def test_import_clears_stale_current_source_probe(self):
        ir = make_import_result(decision="import", new_min_bitrate=245)
        r = self._dispatch(ir, request_overrides={
            "current_lossless_source_v0_probe_min_bitrate": 165,
            "current_lossless_source_v0_probe_avg_bitrate": 228,
            "current_lossless_source_v0_probe_median_bitrate": 225,
        })

        row = r["db"].request(42)
        self.assertIsNone(row["current_lossless_source_v0_probe_min_bitrate"])
        self.assertIsNone(row["current_lossless_source_v0_probe_avg_bitrate"])
        self.assertIsNone(row["current_lossless_source_v0_probe_median_bitrate"])

    def test_preflight_existing_preserves_current_source_probe(self):
        ir = make_import_result(decision="preflight_existing")
        r = self._dispatch(ir, request_overrides={
            "current_lossless_source_v0_probe_min_bitrate": 165,
            "current_lossless_source_v0_probe_avg_bitrate": 228,
            "current_lossless_source_v0_probe_median_bitrate": 225,
        })

        row = r["db"].request(42)
        self.assertEqual(row["current_lossless_source_v0_probe_min_bitrate"], 165)
        self.assertEqual(row["current_lossless_source_v0_probe_avg_bitrate"], 228)
        self.assertEqual(row["current_lossless_source_v0_probe_median_bitrate"], 225)

    def test_downgrade_rejected(self):
        ir = make_import_result(decision="downgrade", new_min_bitrate=192,
                                prev_min_bitrate=320)
        r = self._dispatch(ir)
        self.assertEqual(r["db"].download_logs[0].outcome, "rejected")
        self.assertEqual(r["db"].request(42)["status"], "wanted")
        self.assertTrue(len(r["db"].denylist) > 0)
        r["mock_cleanup"].assert_called_once()

    def test_downgrade_passes_narrowed_override_to_transition(self):
        ir = make_import_result(decision="downgrade", new_min_bitrate=320,
                                prev_min_bitrate=320)
        r = self._dispatch(ir, request_overrides={
            "search_filetype_override": "flac,mp3 v0,mp3 320",
        })
        self.assertEqual(
            r["db"].request(42)["search_filetype_override"], "flac,mp3 v0")

    def test_downgrade_preserves_override_when_tier_not_matched(self):
        ir = make_import_result(decision="downgrade", new_min_bitrate=320,
                                prev_min_bitrate=320)
        r = self._dispatch(ir, request_overrides={
            "search_filetype_override": "flac",
        })
        # No narrowing: "mp3 320" tier not in "flac"-only override
        # reset_to_wanted without search_filetype_override → preserved
        # The override should not have been changed from what reset_to_wanted sets
        override = r["db"].request(42)["search_filetype_override"]
        # narrowing returns None when no tier matches, so reset_to_wanted
        # doesn't pass search_filetype_override, preserving the original "flac"
        self.assertEqual(override, "flac")

    def test_transcode_upgrade(self):
        ir = make_import_result(decision="transcode_upgrade",
                                new_min_bitrate=227)
        r = self._dispatch(ir)
        self.assertEqual(r["db"].download_logs[0].outcome, "success")
        self.assertEqual(r["db"].request(42)["status"], "wanted")
        self.assertTrue(len(r["db"].denylist) > 0)
        r["mock_meelo"].assert_called_once()

    def test_transcode_downgrade(self):
        ir = make_import_result(decision="transcode_downgrade",
                                new_min_bitrate=190)
        r = self._dispatch(ir)
        self.assertEqual(r["db"].download_logs[0].outcome, "rejected")
        self.assertTrue(len(r["db"].denylist) > 0)
        self.assertEqual(r["db"].request(42)["status"], "wanted")

    def test_provisional_lossless_upgrade_imports_requeues_and_persists_probe(self):
        probe = V0ProbeEvidence(
            kind=V0_PROBE_LOSSLESS_SOURCE,
            min_bitrate_kbps=165,
            avg_bitrate_kbps=228,
            median_bitrate_kbps=225,
        )
        ir = make_import_result(
            decision="provisional_lossless_upgrade",
            new_min_bitrate=128,
            spectral_grade="suspect",
            spectral_bitrate=160,
            verified_lossless=False,
            final_format="opus 128",
            v0_probe=probe,
        )

        r = self._dispatch(ir)

        row = r["db"].request(42)
        self.assertEqual(row["status"], "wanted")
        self.assertFalse(row["verified_lossless"])
        self.assertEqual(row["current_lossless_source_v0_probe_avg_bitrate"], 228)
        self.assertEqual(row["search_filetype_override"], QUALITY_UPGRADE_TIERS)
        self.assertEqual(r["db"].download_logs[0].outcome, "success")
        self.assertEqual(r["db"].download_logs[0].beets_scenario,
                         "provisional_lossless_upgrade")
        self.assertEqual(r["db"].download_logs[0].extra["v0_probe_avg_bitrate"],
                         228)
        self.assertTrue(len(r["db"].denylist) > 0)
        r["mock_meelo"].assert_called_once()

    def test_suspect_lossless_downgrade_rejects_without_probe_update(self):
        probe = V0ProbeEvidence(
            kind=V0_PROBE_LOSSLESS_SOURCE,
            min_bitrate_kbps=165,
            avg_bitrate_kbps=175,
            median_bitrate_kbps=174,
        )
        existing = V0ProbeEvidence(
            kind=V0_PROBE_LOSSLESS_SOURCE,
            min_bitrate_kbps=128,
            avg_bitrate_kbps=171,
            median_bitrate_kbps=169,
        )
        ir = make_import_result(
            decision="suspect_lossless_downgrade",
            new_min_bitrate=128,
            spectral_grade="suspect",
            spectral_bitrate=160,
            verified_lossless=False,
            v0_probe=probe,
            existing_v0_probe=existing,
        )

        r = self._dispatch(ir, request_overrides={
            "current_lossless_source_v0_probe_avg_bitrate": 171,
        })

        row = r["db"].request(42)
        self.assertEqual(row["status"], "wanted")
        self.assertEqual(row["current_lossless_source_v0_probe_avg_bitrate"], 171)
        self.assertEqual(row["validation_attempts"], 1)
        self.assertIsNotNone(row["next_retry_after"])
        self.assertEqual(r["db"].download_logs[0].outcome, "rejected")
        self.assertEqual(r["db"].download_logs[0].beets_scenario,
                         "suspect_lossless_downgrade")
        self.assertEqual(r["db"].download_logs[0].extra["v0_probe_avg_bitrate"],
                         175)
        self.assertEqual(
            r["db"].download_logs[0].extra["existing_v0_probe_avg_bitrate"],
            171,
        )
        self.assertTrue(len(r["db"].denylist) > 0)

    def test_suspect_lossless_probe_missing_requeues_without_probe_update(self):
        ir = make_import_result(
            decision="suspect_lossless_probe_missing",
            new_min_bitrate=128,
            spectral_grade="suspect",
            spectral_bitrate=160,
            verified_lossless=False,
            error="suspect lossless source lacks a comparable V0 probe",
        )

        r = self._dispatch(ir, request_overrides={
            "current_lossless_source_v0_probe_min_bitrate": 128,
            "current_lossless_source_v0_probe_avg_bitrate": 171,
            "current_lossless_source_v0_probe_median_bitrate": 169,
        })

        row = r["db"].request(42)
        self.assertEqual(row["status"], "wanted")
        self.assertEqual(row["current_lossless_source_v0_probe_avg_bitrate"], 171)
        self.assertEqual(row["validation_attempts"], 1)
        self.assertIsNotNone(row["next_retry_after"])
        self.assertEqual(r["db"].download_logs[0].outcome, "rejected")
        self.assertEqual(r["db"].download_logs[0].beets_scenario,
                         "suspect_lossless_probe_missing")
        self.assertIn(
            "comparable V0 probe",
            r["db"].download_logs[0].beets_detail,
        )
        self.assertTrue(len(r["db"].denylist) > 0)

    def test_lossless_source_locked_rejects_lossy_candidate(self):
        # Wire-boundary test: import_one.py emits decision=lossless_source_locked
        # for a lossy candidate the gate refused to compare against an
        # existing lossless-source V0 probe. Dispatch must:
        #   - record a rejected download_log with beets_scenario=lossless_source_locked
        #   - put a human-readable detail referencing the existing probe
        #   - clear ir.error from the stored row (it's a domain rejection, not a crash)
        #   - denylist + requeue the request to wanted
        existing = V0ProbeEvidence(
            kind=V0_PROBE_LOSSLESS_SOURCE,
            min_bitrate_kbps=210,
            avg_bitrate_kbps=240,
            median_bitrate_kbps=235,
        )
        ir = make_import_result(
            decision="lossless_source_locked",
            new_min_bitrate=176,
            spectral_grade="likely_transcode",
            spectral_bitrate=128,
            verified_lossless=False,
            existing_v0_probe=existing,
            error=("existing has lossless-source V0 probe 240kbps; lossy "
                   "candidate cannot produce comparable evidence"),
        )

        r = self._dispatch(ir, request_overrides={
            "current_lossless_source_v0_probe_avg_bitrate": 240,
        })

        row = r["db"].request(42)
        self.assertEqual(row["status"], "wanted")
        self.assertEqual(row["current_lossless_source_v0_probe_avg_bitrate"], 240)
        self.assertEqual(r["db"].download_logs[0].outcome, "rejected")
        self.assertEqual(r["db"].download_logs[0].beets_scenario,
                         "lossless_source_locked")
        self.assertIn(
            "240",
            r["db"].download_logs[0].beets_detail or "",
        )
        # ir.error is suppressed for lossless_source_locked — domain rejections
        # should not bleed into the error_message column (mirrors suspect_lossless_*).
        self.assertIsNone(r["db"].download_logs[0].error_message)
        self.assertTrue(len(r["db"].denylist) > 0)

    def test_error_decision(self):
        ir = make_import_result(decision="conversion_failed",
                                error="ffmpeg failed")
        r = self._dispatch(ir)
        self.assertEqual(r["db"].download_logs[0].outcome, "rejected")

    def test_duplicate_remove_guard_failure_denylists_and_quarantines(self):
        from lib.dispatch import dispatch_import_core

        staging_root = tempfile.mkdtemp()
        source = os.path.join(staging_root, "auto-import", "Artist", "Album")
        os.makedirs(source)
        with open(os.path.join(source, "track.mp3"), "w", encoding="utf-8") as f:
            f.write("x")

        guard = DuplicateRemoveGuardInfo(
            reason="duplicate_count_not_one",
            target_source="musicbrainz",
            target_release_id="test-mbid",
            duplicate_count=2,
            message="beets reported 2 duplicate albums; expected exactly 1",
            candidates=[
                DuplicateRemoveCandidate(
                    beets_album_id=100,
                    mb_albumid="test-mbid",
                    album_path="/Beets/Artist/Album",
                    item_count=10,
                ),
                DuplicateRemoveCandidate(
                    beets_album_id=101,
                    mb_albumid="other-mbid",
                    album_path="/Beets/Artist/Album [2006]",
                    item_count=11,
                ),
            ],
        )
        ir = ImportResult(
            exit_code=7,
            decision="duplicate_remove_guard_failed",
            error=guard.message,
            postflight=PostflightInfo(duplicate_remove_guard=guard),
        )

        db = FakePipelineDB()
        db.seed_request(make_request_row(id=42, status="downloading"))
        cfg = CratediggerConfig(
            beets_harness_path=_HARNESS,
            beets_staging_dir=staging_root,
            pipeline_db_enabled=True,
        )
        try:
            with patch_dispatch_externals() as ext, \
                 patch("lib.dispatch.subprocess_runner.parse_import_result", return_value=ir):
                dispatch_import_core(
                    path=source,
                    mb_release_id="test-mbid",
                    request_id=42,
                    label="Artist - Album",
                    beets_harness_path=_HARNESS,
                    db=db,  # type: ignore[arg-type]
                    dl_info=DownloadInfo(filetype="mp3", username="user1"),
                    distance=0.05,
                    scenario="strong_match",
                    files=[],
                    cfg=cfg,
                    requeue_on_failure=True,
                )
        finally:
            shutil.rmtree(staging_root, ignore_errors=True)

        self.assertEqual(db.download_logs[0].outcome, "rejected")
        self.assertEqual(db.download_logs[0].beets_scenario,
                         "duplicate_remove_guard_failed")
        self.assertEqual(db.request(42)["status"], "wanted")
        self.assertNotEqual(db.request(42)["status"], "manual")
        self.assertEqual(len(db.denylist), 1)
        self.assertEqual(db.denylist[0].username, "user1")
        ext.cleanup.assert_not_called()

        persisted = ImportResult.from_json(db.download_logs[0].import_result)
        persisted_guard = persisted.postflight.duplicate_remove_guard
        assert persisted_guard is not None
        self.assertIsNotNone(persisted_guard.quarantine_path)
        assert persisted_guard.quarantine_path is not None
        self.assertIn("duplicate-remove-guard",
                      persisted_guard.quarantine_path)
        self.assertFalse(os.path.exists(source))

    def test_no_json_result(self):
        r = self._dispatch(None)
        self.assertEqual(len(r["db"].download_logs), 1)
        self.assertEqual(r["db"].download_logs[0].outcome, "failed")

    def test_timeout(self):
        from lib.dispatch import dispatch_import_core
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=42, status="downloading"))

        with patch("lib.dispatch.subprocess_runner.sp.run",
                   side_effect=sp.TimeoutExpired(cmd="test", timeout=1800)):
            dispatch_import_core(
                path="/tmp/dest", mb_release_id="test-mbid",
                request_id=42, label="Test",
                beets_harness_path=_HARNESS,
                db=db,  # type: ignore[arg-type]
                dl_info=DownloadInfo(filetype="mp3"),
            )

        self.assertEqual(len(db.download_logs), 1)
        self.assertEqual(db.download_logs[0].outcome, "failed")

    def test_exception(self):
        from lib.dispatch import dispatch_import_core
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=42, status="downloading"))

        with patch("lib.dispatch.subprocess_runner.sp.run",
                   side_effect=RuntimeError("boom")):
            dispatch_import_core(
                path="/tmp/dest", mb_release_id="test-mbid",
                request_id=42, label="Test",
                beets_harness_path=_HARNESS,
                db=db,  # type: ignore[arg-type]
                dl_info=DownloadInfo(filetype="mp3"),
            )

        self.assertEqual(len(db.download_logs), 1)
        self.assertEqual(db.download_logs[0].outcome, "failed")


class TestImportDispatchRescueCapture(unittest.TestCase):
    """U14: long-tail-rescue audit columns populated atomically on import.

    When ``dispatch_import_core`` flips a request to ``imported`` and
    that request was previously categorised unfindable, the importer
    must capture the rescue event (``rescued_at``,
    ``prior_unfindable_category``) in the same atomic write as the
    status flip.

    Verifies the wiring through ``apply_transition`` →
    ``mark_imported_with_rescue`` on the FakePipelineDB; the real-PG
    atomicity contract lives in
    ``tests/test_pipeline_db.py::TestMarkImportedWithRescue`` and
    ``tests/test_integration_slices.py::TestRescueCaptureSlice``.
    """

    _HARNESS_PATH = _HARNESS

    def _dispatch_with_unfindable(self, *, prior_category, rescued_at=None,
                                  prior_rescue_category=None):
        """Drive a successful import on a previously-unfindable request."""
        from lib.dispatch import dispatch_import_core
        from datetime import datetime, timezone

        ir = make_import_result(decision="import")
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42, status="downloading",
        ))
        # Seed the row's unfindable state directly so the test starts
        # from the "categorised, just finished downloading" shape.
        if prior_category is not None:
            db._requests[42]["unfindable_category"] = prior_category
            db._requests[42]["unfindable_categorised_at"] = datetime(
                2026, 5, 20, tzinfo=timezone.utc)
        if rescued_at is not None:
            db._requests[42]["rescued_at"] = rescued_at
        if prior_rescue_category is not None:
            db._requests[42]["prior_unfindable_category"] = (
                prior_rescue_category)
        cfg = CratediggerConfig(
            beets_harness_path=self._HARNESS_PATH,
            pipeline_db_enabled=True,
        )

        tmpdir = tempfile.mkdtemp()
        try:
            with patch_dispatch_externals(), \
                 patch("lib.dispatch.subprocess_runner.parse_import_result",
                       return_value=ir):
                dispatch_import_core(
                    path=tmpdir,
                    mb_release_id="test-mbid",
                    request_id=42,
                    label="Rescue Artist - Album",
                    beets_harness_path=self._HARNESS_PATH,
                    db=db,  # type: ignore[arg-type]
                    dl_info=DownloadInfo(filetype="mp3"),
                    distance=0.05,
                    scenario="strong_match",
                    files=[MagicMock(username="u1",
                                     filename="01 - T.mp3")],
                    cfg=cfg,
                    quality_gate_fn=noop_quality_gate,
                )
        finally:
            shutil.rmtree(tmpdir, ignore_errors=True)
        return db

    def test_import_captures_rescue_when_unfindable_category_was_set(self):
        for category in (
            "artist_absent",
            "album_absent_artist_present",
            "one_track_structural",
            "wrong_pressing_available",
        ):
            with self.subTest(category=category):
                db = self._dispatch_with_unfindable(prior_category=category)
                row = db.request(42)
                self.assertEqual(row["status"], "imported")
                self.assertIsNone(row["unfindable_category"])
                self.assertEqual(
                    row["prior_unfindable_category"], category)
                self.assertIsNotNone(row["rescued_at"])

    def test_import_without_prior_unfindable_does_not_stamp_rescue(self):
        db = self._dispatch_with_unfindable(prior_category=None)
        row = db.request(42)
        self.assertEqual(row["status"], "imported")
        self.assertIsNone(row["rescued_at"])
        self.assertIsNone(row["prior_unfindable_category"])
        self.assertIsNone(row["unfindable_category"])

    def test_re_import_after_prior_rescue_does_not_overwrite_audit_columns(
        self,
    ):
        """One-shot capture — first rescue wins forever."""
        from datetime import datetime, timezone

        original_rescue_at = datetime(2026, 1, 15, tzinfo=timezone.utc)
        db = self._dispatch_with_unfindable(
            prior_category="album_absent_artist_present",
            rescued_at=original_rescue_at,
            prior_rescue_category="artist_absent",
        )
        row = db.request(42)
        self.assertEqual(row["status"], "imported")
        self.assertEqual(row["rescued_at"], original_rescue_at)
        self.assertEqual(row["prior_unfindable_category"], "artist_absent")
        # The current (later) category still gets cleared.
        self.assertIsNone(row["unfindable_category"])


class TestOverrideMinBitrate(unittest.TestCase):
    """Seam tests — subprocess arg wiring for --override-min-bitrate.

    Tests the surviving auto-import seam's override computation.

    The override must be grade-aware: spectral bitrate only participates when
    current_spectral_grade is in {suspect, likely_transcode}. Genuine/marginal/
    None grades must leave the container bitrate untouched — see issue #61.
    """

    def _get_override_value(self, db_fields):
        cmd = _dispatch_valid_result_cmd(db_fields=db_fields)

        for i, arg in enumerate(cmd):
            if arg == "--override-min-bitrate" and i + 1 < len(cmd):
                return int(cmd[i + 1])
        return None

    # (description, min_bitrate, current_spectral_bitrate, current_spectral_grade, expected)
    CASES = [
        ("suspect spectral lower wins",             320, 128, "suspect",          128),
        ("likely_transcode spectral lower wins",    320, 128, "likely_transcode", 128),
        ("genuine spectral ignored even if lower",  320, 128, "genuine",          320),
        ("marginal spectral ignored even if lower", 320, 128, "marginal",         320),
        ("grade None ignores spectral",             320, 128, None,               320),
        ("suspect grade but spectral higher",       192, 256, "suspect",          192),
        ("no spectral, grade genuine",              320, None, "genuine",         320),
        ("no spectral, grade None",                 320, None, None,              320),
        ("no container no spectral",                None, None, None,             None),
        ("no container, suspect spectral",          None, 128, "suspect",         128),
        ("no container, genuine spectral ignored",  None, 128, "genuine",         None),
    ]

    def test_override_from_db_table(self):
        for desc, min_br, spectral_br, grade, expected in self.CASES:
            with self.subTest(desc=desc):
                row = make_request_row(
                    min_bitrate=min_br,
                    current_spectral_bitrate=spectral_br,
                    current_spectral_grade=grade,
                )
                self.assertEqual(
                    self._get_override_value(row), expected,
                    f"{desc}: override from min_bitrate={min_br!r} "
                    f"spectral_bitrate={spectral_br!r} grade={grade!r} "
                    f"expected {expected!r}",
                )


class TestDispatchRankConfigArgv(unittest.TestCase):
    """Seam test — harness argv must carry --quality-rank-config JSON.

    Verifies the QualityRankConfig round-trips through the subprocess
    boundary unchanged, so the harness's rank classification matches the
    caller's runtime config. Will break if import_one becomes a library
    call (#48) or if QualityRankConfig.to_json() changes shape.
    """

    def _run_dispatch_capture_cmd(self, cfg_obj):
        """Call dispatch_import_core with cfg_obj, return captured argv."""
        from lib.dispatch import dispatch_import_core
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=42, status="downloading"))
        ir = make_import_result(decision="import")

        with patch_dispatch_externals() as ext, \
             patch("lib.dispatch.subprocess_runner.parse_import_result", return_value=ir):
            dispatch_import_core(
                path="/tmp/dest", mb_release_id="mbid-1",
                request_id=42, label="Test Artist - Test Album",
                beets_harness_path=_HARNESS,
                cfg=cfg_obj,
                db=db,  # type: ignore[arg-type]
                dl_info=DownloadInfo(filetype="mp3"),
                files=[MagicMock(username="user1", filename="01.mp3")],
                quality_gate_fn=noop_quality_gate,
            )
            return ext.run.call_args[0][0]

    def _extract_rank_config_json(self, cmd):
        for i, arg in enumerate(cmd):
            if arg == "--quality-rank-config" and i + 1 < len(cmd):
                return cmd[i + 1]
        return None

    def test_default_cfg_serializes_to_argv(self):
        """Default QualityRankConfig → argv contains the round-trip JSON."""
        from lib.config import CratediggerConfig
        from lib.quality import QualityRankConfig
        cfg = CratediggerConfig(beets_harness_path=_HARNESS)
        cmd = self._run_dispatch_capture_cmd(cfg)
        raw = self._extract_rank_config_json(cmd)
        self.assertIsNotNone(raw)
        assert raw is not None  # for pyright
        # Round-trip must produce an equal QualityRankConfig
        restored = QualityRankConfig.from_json(raw)
        self.assertEqual(restored, cfg.quality_ranks)

    def test_custom_cfg_serializes_to_argv(self):
        """Custom gate_min_rank + metric survive the argv round-trip."""
        from lib.config import CratediggerConfig
        from lib.quality import (QualityRank, QualityRankConfig,
                                 RankBitrateMetric)
        custom_ranks = QualityRankConfig(
            bitrate_metric=RankBitrateMetric.MIN,
            gate_min_rank=QualityRank.GOOD,
            within_rank_tolerance_kbps=15,
        )
        cfg = CratediggerConfig(
            beets_harness_path=_HARNESS, quality_ranks=custom_ranks)
        cmd = self._run_dispatch_capture_cmd(cfg)
        raw = self._extract_rank_config_json(cmd)
        self.assertIsNotNone(raw)
        assert raw is not None  # for pyright
        restored = QualityRankConfig.from_json(raw)
        self.assertEqual(restored.bitrate_metric, RankBitrateMetric.MIN)
        self.assertEqual(restored.gate_min_rank, QualityRank.GOOD)
        self.assertEqual(restored.within_rank_tolerance_kbps, 15)

    def test_missing_cfg_omits_argv(self):
        """When cfg=None, the --quality-rank-config argv is not emitted.

        Harness falls back to QualityRankConfig.defaults() in that case.
        """
        cmd = self._run_dispatch_capture_cmd(None)
        self.assertNotIn("--quality-rank-config", cmd)

    def test_existing_v0_probe_state_serializes_to_argv(self):
        row = make_request_row(
            status="downloading",
            current_lossless_source_v0_probe_min_bitrate=128,
            current_lossless_source_v0_probe_avg_bitrate=171,
            current_lossless_source_v0_probe_median_bitrate=169,
        )

        cmd = _dispatch_valid_result_cmd(db_fields=row)

        self.assertIn("--existing-v0-probe-min-bitrate", cmd)
        self.assertEqual(
            cmd[cmd.index("--existing-v0-probe-min-bitrate") + 1], "128")
        self.assertIn("--existing-v0-probe-avg-bitrate", cmd)
        self.assertEqual(
            cmd[cmd.index("--existing-v0-probe-avg-bitrate") + 1], "171")
        self.assertIn("--existing-v0-probe-median-bitrate", cmd)
        self.assertEqual(
            cmd[cmd.index("--existing-v0-probe-median-bitrate") + 1], "169")


class TestLoadQualityGateState(unittest.TestCase):
    """Direct tests for the shared quality-gate state adapter."""

    def test_uses_full_request_row_to_apply_final_format(self):
        """The shared adapter must honor persisted final_format labels."""
        from lib.beets_db import AlbumInfo
        from lib.dispatch import load_quality_gate_state
        from lib.quality import QualityRankConfig

        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42,
            status="wanted",
            mb_release_id="mbid-123",
            verified_lossless=True,
            final_format="mp3 v0",
            current_spectral_grade="genuine",
            current_spectral_bitrate=96,
        ))

        with patch("lib.beets_db.BeetsDB") as mock_beets_cls:
            mock_beets = MagicMock()
            mock_beets.__enter__ = MagicMock(return_value=mock_beets)
            mock_beets.__exit__ = MagicMock(return_value=False)
            mock_beets.get_album_info.return_value = AlbumInfo(
                album_id=1,
                track_count=10,
                min_bitrate_kbps=207,
                avg_bitrate_kbps=207,
                format="MP3",
                is_cbr=False,
                album_path="/Beets/Artist/Album",
            )
            mock_beets_cls.return_value = mock_beets

            state = load_quality_gate_state(
                request_id=42,
                db=db,  # type: ignore[arg-type]
                quality_ranks=QualityRankConfig.defaults(),
            )

        self.assertIsNotNone(state)
        assert state is not None
        self.assertEqual(state.min_bitrate_kbps, 207)
        self.assertEqual(state.measurement.format, "mp3 v0")
        self.assertEqual(state.measurement.avg_bitrate_kbps, 207)
        self.assertFalse(state.measurement.is_cbr)
        self.assertTrue(state.measurement.verified_lossless)
        self.assertIsNone(state.spectral_bitrate_kbps)


class TestQualityGateUsesIntent(unittest.TestCase):
    """Orchestration tests for _check_quality_gate_core via FakePipelineDB.

    Each scenario constructs a real :class:`lib.beets_db.AlbumInfo` whose
    measurement produces the desired ``quality_gate_decision`` branch when
    classified by the real (un-stubbed) decision function. See
    ``tests/test_quality_decisions.py::TestQualityGateDecision.CASES`` for
    the canonical input → decision table — these tests pick inputs from the
    same table so the orchestration test exercises the same code path the
    decision unit tests pin.
    """

    def _run_quality_gate(self, *, info, **extra_req_fields):
        """Drive ``_check_quality_gate_core`` with a real ``AlbumInfo`` and the
        real ``quality_gate_decision`` (no patch on the pure decision)."""
        from lib.dispatch import _check_quality_gate_core
        db = FakePipelineDB()
        merged = {"status": "imported", "current_spectral_bitrate": None,
                  "verified_lossless": False}
        merged.update(extra_req_fields)
        db.seed_request(make_request_row(id=42, **merged))

        with patch("lib.beets_db.BeetsDB") as mock_beets_cls:
            mock_beets = MagicMock()
            mock_beets.__enter__ = MagicMock(return_value=mock_beets)
            mock_beets.__exit__ = MagicMock(return_value=False)
            mock_beets.get_album_info.return_value = info
            mock_beets_cls.return_value = mock_beets
            _check_quality_gate_core(
                mb_id="test-mbid", label="Test Artist - Test Album",
                request_id=42,
                files=[MagicMock(username="user1", filename="01.mp3")],
                db=db,  # type: ignore[arg-type]
            )

        return db

    @staticmethod
    def _bare_mp3_vbr_low():
        """MP3 VBR at 150 kbps → ACCEPTABLE < EXCELLENT → requeue_upgrade.

        Matches the pinned "bare MP3 VBR below rank" case in
        TestQualityGateDecision.CASES.
        """
        from lib.beets_db import AlbumInfo
        return AlbumInfo(
            album_id=1, track_count=10,
            min_bitrate_kbps=150, avg_bitrate_kbps=150,
            format="MP3", is_cbr=False,
            album_path="/Beets/Artist/Album",
        )

    @staticmethod
    def _cbr_320_unverified():
        """CBR 320 unverified → TRANSPARENT but CBR + !verified → requeue_lossless.

        Matches the pinned "bare MP3 CBR 320 unverified" case.
        """
        from lib.beets_db import AlbumInfo
        return AlbumInfo(
            album_id=1, track_count=10,
            min_bitrate_kbps=320, avg_bitrate_kbps=320,
            format="MP3", is_cbr=True,
            album_path="/Beets/Artist/Album",
        )

    def test_no_mb_id_returns_early(self):
        """Empty mb_id should return without doing anything."""
        from lib.dispatch import _check_quality_gate_core
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=42, status="imported"))
        _check_quality_gate_core(
            mb_id="", label="Test", request_id=42, files=[],
            db=db)  # type: ignore[arg-type]
        # Status unchanged — gate returned early
        self.assertEqual(db.request(42)["status"], "imported")

    def test_requeue_upgrade_uses_intent(self):
        db = self._run_quality_gate(info=self._bare_mp3_vbr_low())
        row = db.request(42)
        self.assertEqual(row["status"], "wanted")
        self.assertEqual(row["search_filetype_override"], QUALITY_UPGRADE_TIERS)

    def test_requeue_upgrade_verified_lossless_still_requeues(self):
        # verified_lossless=True on a low-bitrate MP3 still classifies below
        # EXCELLENT because measurement_rank reads avg/min, not the lossless
        # flag — gate_rank only skips the spectral clamp for verified rows.
        db = self._run_quality_gate(
            info=self._bare_mp3_vbr_low(), verified_lossless=True)
        row = db.request(42)
        self.assertEqual(row["status"], "wanted")
        self.assertEqual(row["search_filetype_override"], QUALITY_UPGRADE_TIERS)

    def test_requeue_upgrade_verified_lossless_denylist_reason_preserved(self):
        db = self._run_quality_gate(
            info=self._bare_mp3_vbr_low(),
            verified_lossless=True,
            current_spectral_grade=None,
        )
        self.assertEqual(len(db.denylist), 1)
        self.assertIn("quality gate", db.denylist[0].reason or "")

    def test_requeue_upgrade_denylist_reason_is_rank_aware(self):
        """Denylist reason text must reflect the actual rank/threshold, not the
        legacy hardcoded 210kbps constant.

        The reason is persisted to the DB and surfaces in operator-facing
        history. Before this fix, every requeue_upgrade row was tagged with
        '< 210kbps' regardless of cfg.gate_min_rank. After the fix the reason
        carries the rank name + the configured gate threshold.
        """
        from lib.dispatch import _check_quality_gate_core
        from lib.quality import QualityRankConfig
        from lib.beets_db import AlbumInfo

        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42, status="imported", verified_lossless=False,
            current_spectral_bitrate=None, current_spectral_grade=None,
            mb_release_id="mbid-low",
        ))

        # Bare MP3 at 150kbps avg → ACCEPTABLE rank, below default EXCELLENT gate.
        with patch("lib.beets_db.BeetsDB") as mock_beets_cls:
            mock_beets = MagicMock()
            mock_beets.__enter__ = MagicMock(return_value=mock_beets)
            mock_beets.__exit__ = MagicMock(return_value=False)
            mock_beets.get_album_info.return_value = AlbumInfo(
                album_id=1, track_count=10,
                min_bitrate_kbps=150, avg_bitrate_kbps=150,
                format="MP3", is_cbr=False,
                album_path="/Beets/Artist/Album",
            )
            mock_beets_cls.return_value = mock_beets
            _check_quality_gate_core(
                mb_id="mbid-low", label="Artist - Album",
                request_id=42,
                files=[MagicMock(username="loweruser", filename="01.mp3")],
                db=db,  # type: ignore[arg-type]
                quality_ranks=QualityRankConfig.defaults(),
            )

        self.assertEqual(len(db.denylist), 1)
        reason = db.denylist[0].reason or ""
        # New format: includes the actual rank name and gate threshold
        self.assertIn("ACCEPTABLE", reason,
                      f"reason should name the actual rank, got: {reason}")
        self.assertIn("EXCELLENT", reason,
                      f"reason should name the configured gate threshold, got: {reason}")
        # Old format pinned the legacy 210kbps constant — must NOT appear
        self.assertNotIn("< 210kbps", reason,
                         f"reason still references legacy 210kbps: {reason}")

    def test_requeue_upgrade_denylist_reason_honours_custom_gate(self):
        """Custom gate_min_rank=GOOD must surface in the persisted reason.

        Verifies the reason text actually threads cfg through to the
        denylist entry, not just the decision.
        """
        from lib.dispatch import _check_quality_gate_core
        from lib.quality import QualityRankConfig, QualityRank
        from lib.beets_db import AlbumInfo

        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=43, status="imported", verified_lossless=False,
            current_spectral_bitrate=None, current_spectral_grade=None,
            mb_release_id="mbid-mid",
        ))

        # 140 kbps avg → ACCEPTABLE under any cfg, below GOOD threshold.
        with patch("lib.beets_db.BeetsDB") as mock_beets_cls:
            mock_beets = MagicMock()
            mock_beets.__enter__ = MagicMock(return_value=mock_beets)
            mock_beets.__exit__ = MagicMock(return_value=False)
            mock_beets.get_album_info.return_value = AlbumInfo(
                album_id=1, track_count=10,
                min_bitrate_kbps=140, avg_bitrate_kbps=140,
                format="MP3", is_cbr=False,
                album_path="/Beets/Artist/Album",
            )
            mock_beets_cls.return_value = mock_beets
            _check_quality_gate_core(
                mb_id="mbid-mid", label="Artist - Album",
                request_id=43,
                files=[MagicMock(username="middleuser", filename="01.mp3")],
                db=db,  # type: ignore[arg-type]
                quality_ranks=QualityRankConfig(gate_min_rank=QualityRank.GOOD),
            )

        self.assertEqual(len(db.denylist), 1)
        reason = db.denylist[0].reason or ""
        self.assertIn("GOOD", reason,
                      f"reason should name the custom gate_min_rank=GOOD, got: {reason}")

    def test_requeue_lossless_uses_intent(self):
        db = self._run_quality_gate(info=self._cbr_320_unverified())
        row = db.request(42)
        self.assertEqual(row["status"], "wanted")
        self.assertEqual(row["search_filetype_override"], QUALITY_FLAC_ONLY)

    def test_quality_gate_reads_current_spectral_not_last_download(self):
        """Quality gate must use current_spectral_bitrate (what's on disk),
        not last_download_spectral_bitrate (stale from a previous download).

        Observable proof: a stale last_download_* of "likely_transcode" at
        180 kbps WOULD clamp an MP3 VBR 226 album down to GOOD and requeue.
        Reading current_* (None) leaves the rank at EXCELLENT → accept,
        and the request row stays ``imported``.
        """
        from lib.dispatch import _check_quality_gate_core
        from lib.beets_db import AlbumInfo
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42, status="imported",
            last_download_spectral_bitrate=180,
            last_download_spectral_grade="likely_transcode",
            current_spectral_bitrate=None,
            current_spectral_grade=None,
            verified_lossless=False,
        ))

        info = AlbumInfo(
            album_id=1, track_count=10,
            min_bitrate_kbps=226, avg_bitrate_kbps=226,
            format="MP3", is_cbr=False,
            album_path="/Beets/Artist/Album",
        )
        with patch("lib.beets_db.BeetsDB") as mock_beets_cls:
            mock_beets = MagicMock()
            mock_beets.__enter__ = MagicMock(return_value=mock_beets)
            mock_beets.__exit__ = MagicMock(return_value=False)
            mock_beets.get_album_info.return_value = info
            mock_beets_cls.return_value = mock_beets
            _check_quality_gate_core(
                mb_id="test-mbid", label="Test Artist - Test Album",
                request_id=42, files=[],
                db=db)  # type: ignore[arg-type]

        self.assertEqual(
            db.request(42)["status"], "imported",
            "stale last_download_* spectral must not influence the gate; "
            "current_spectral_* is None, so the gate sees EXCELLENT and accepts")

    def test_genuine_v0_replacing_transcode_accepted(self):
        """Genuine V0 replacing a transcode should be accepted, not requeued."""
        from lib.dispatch import _check_quality_gate_core
        from lib.beets_db import AlbumInfo

        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42, status="imported",
            last_download_spectral_bitrate=None,
            last_download_spectral_grade="genuine",
            current_spectral_bitrate=None,
            current_spectral_grade="genuine",
            verified_lossless=False,
        ))

        info = AlbumInfo(
            album_id=1, track_count=10,
            min_bitrate_kbps=226, avg_bitrate_kbps=226,
            format="MP3", is_cbr=False,
            album_path="/Beets/Artist/Album",
        )
        with patch("lib.beets_db.BeetsDB") as mock_beets_cls:
            mock_beets = MagicMock()
            mock_beets.__enter__ = MagicMock(return_value=mock_beets)
            mock_beets.__exit__ = MagicMock(return_value=False)
            mock_beets.get_album_info.return_value = info
            mock_beets_cls.return_value = mock_beets
            _check_quality_gate_core(
                mb_id="test-mbid", label="Test Artist - Test Album",
                request_id=42, files=[],
                db=db)  # type: ignore[arg-type]

        # Should stay imported (not requeued) — rank is EXCELLENT with no
        # spectral clamp because the grade is "genuine".
        self.assertEqual(db.request(42)["status"], "imported")

    def test_quality_gate_uses_likely_transcode_spectral(self):
        """likely_transcode album grade must feed into the gate, not just suspect.

        Regression for issue #61: _check_quality_gate_core previously only
        accepted "suspect", silently ignoring the album-level "likely_transcode"
        grade produced by classify_album when >=60% of tracks are suspect.

        Observable proof: with spectral=180 and grade="likely_transcode",
        the spectral clamp pulls the MP3 VBR 226 rank from EXCELLENT down to
        GOOD, which is < EXCELLENT (gate_min) → requeue_upgrade. Without
        the clamp the status would stay ``imported``.
        """
        from lib.dispatch import _check_quality_gate_core
        from lib.beets_db import AlbumInfo

        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42, status="imported",
            current_spectral_grade="likely_transcode",
            current_spectral_bitrate=180,
            verified_lossless=False,
        ))
        info = AlbumInfo(
            album_id=1, track_count=10,
            min_bitrate_kbps=226, avg_bitrate_kbps=226,
            format="MP3", is_cbr=False,
            album_path="/Beets/Artist/Album",
        )
        with patch("lib.beets_db.BeetsDB") as mock_beets_cls:
            mock_beets = MagicMock()
            mock_beets.__enter__ = MagicMock(return_value=mock_beets)
            mock_beets.__exit__ = MagicMock(return_value=False)
            mock_beets.get_album_info.return_value = info
            mock_beets_cls.return_value = mock_beets
            _check_quality_gate_core(
                mb_id="test-mbid", label="Test Artist - Test Album",
                request_id=42, files=[],
                db=db)  # type: ignore[arg-type]

        self.assertEqual(
            db.request(42)["status"], "wanted",
            "likely_transcode spectral=180 must clamp the gate rank below "
            "EXCELLENT and trigger requeue_upgrade")

    def test_quality_gate_ignores_genuine_low_spectral(self):
        """Genuine grade with low spectral estimate must NOT lower the gate bitrate.

        Guards the original #31 fix: a lo-fi genuine V0 (e.g. ~160kbps cliff
        estimate) must not trigger a requeue loop when beets reports 226kbps.
        Observable: ``compute_effective_override_bitrate`` returns the
        container bitrate for non-transcode grades, so the gate sees a
        clean EXCELLENT rank and the request stays imported.
        """
        from lib.dispatch import _check_quality_gate_core
        from lib.beets_db import AlbumInfo

        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42, status="imported",
            current_spectral_grade="genuine",
            current_spectral_bitrate=160,
            verified_lossless=False,
        ))
        info = AlbumInfo(
            album_id=1, track_count=10,
            min_bitrate_kbps=226, avg_bitrate_kbps=226,
            format="MP3", is_cbr=False,
            album_path="/Beets/Artist/Album",
        )
        with patch("lib.beets_db.BeetsDB") as mock_beets_cls:
            mock_beets = MagicMock()
            mock_beets.__enter__ = MagicMock(return_value=mock_beets)
            mock_beets.__exit__ = MagicMock(return_value=False)
            mock_beets.get_album_info.return_value = info
            mock_beets_cls.return_value = mock_beets
            _check_quality_gate_core(
                mb_id="test-mbid", label="Test Artist - Test Album",
                request_id=42, files=[],
                db=db)  # type: ignore[arg-type]

        self.assertEqual(db.request(42)["status"], "imported")

    def test_dispatch_requeue_uses_intent(self):
        """Transcode-upgrade requeue path uses quality constants."""
        from lib.dispatch import dispatch_import_core
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=42, status="downloading"))
        ir = make_import_result(decision="transcode_upgrade",
                                new_min_bitrate=227)

        with patch_dispatch_externals(), \
             patch("lib.dispatch.subprocess_runner.parse_import_result", return_value=ir):
            dispatch_import_core(
                path="/tmp/dest", mb_release_id="test-mbid",
                request_id=42, label="Test",
                beets_harness_path=_HARNESS,
                db=db,  # type: ignore[arg-type]
                dl_info=DownloadInfo(filetype="mp3"),
                files=[MagicMock(username="user1", filename="01.mp3")],
                quality_gate_fn=noop_quality_gate,
            )

        row = db.request(42)
        self.assertEqual(row["status"], "wanted")
        self.assertEqual(row["search_filetype_override"], QUALITY_UPGRADE_TIERS)


class TestQualityGatePreservesTargetFormat(unittest.TestCase):
    """Quality gate accept must clear search_filetype_override but preserve target_format."""

    def _run_quality_gate_accept(self, target_format="flac"):
        """Drive a real accept via FLAC verified-lossless input — no decision stub."""
        from lib.dispatch import _check_quality_gate_core
        from lib.beets_db import AlbumInfo

        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42, status="imported",
            target_format=target_format,
            verified_lossless=True,
            current_spectral_bitrate=None,
            search_filetype_override="lossless",  # should be cleared
        ))

        # FLAC → measurement_rank returns LOSSLESS regardless of bitrate, so
        # the real quality_gate_decision accepts.
        info = AlbumInfo(
            album_id=1, track_count=10,
            min_bitrate_kbps=900, avg_bitrate_kbps=900,
            format="FLAC", is_cbr=False,
            album_path="/Beets/Artist/Album",
        )
        with patch("lib.beets_db.BeetsDB") as mock_beets_cls:
            mock_beets = MagicMock()
            mock_beets.__enter__ = MagicMock(return_value=mock_beets)
            mock_beets.__exit__ = MagicMock(return_value=False)
            mock_beets.get_album_info.return_value = info
            mock_beets_cls.return_value = mock_beets
            _check_quality_gate_core(
                mb_id="test-mbid", label="Test Artist - Test Album",
                request_id=42, files=[],
                db=db)  # type: ignore[arg-type]

        return db

    def test_accept_clears_search_override_not_target_format(self):
        db = self._run_quality_gate_accept(target_format="flac")
        row = db.request(42)
        self.assertIsNone(row["search_filetype_override"])
        self.assertEqual(row["target_format"], "flac")
        self.assertEqual(row["status"], "imported")


class TestOpusConversionDispatch(unittest.TestCase):
    """Seam tests — --verified-lossless-target flag wiring.

    Exercised through the surviving auto-import seam in lib.download.
    """

    def _get_cmd(self, verified_lossless_target=""):
        album_data = _make_album_data()
        ctx = _make_ctx()
        ctx.cfg.verified_lossless_target = verified_lossless_target
        ir = make_import_result(decision="import", was_converted=True,
                                original_filetype="flac", target_filetype="mp3")
        return _dispatch_valid_result_cmd(album_data=album_data, ctx=ctx, ir=ir)

    def test_target_flag_passed_when_set(self):
        cmd = self._get_cmd(verified_lossless_target="opus 128")
        self.assertIn("--verified-lossless-target", cmd)
        idx = cmd.index("--verified-lossless-target")
        self.assertEqual(cmd[idx + 1], "opus 128")

    def test_target_flag_not_passed_when_empty(self):
        cmd = self._get_cmd(verified_lossless_target="")
        self.assertNotIn("--verified-lossless-target", cmd)

    def test_opus_import_result_populates_dl_info(self):
        from lib.dispatch import _populate_dl_info_from_import_result
        dl = DownloadInfo(filetype="flac")
        ir = ImportResult(
            decision="import",
            final_format="opus 128",
            v0_verification_bitrate=247,
            new_measurement=AudioQualityMeasurement(
                min_bitrate_kbps=128, verified_lossless=True,
                was_converted_from="flac"),
            conversion=ConversionInfo(
                was_converted=True, original_filetype="flac",
                target_filetype="opus", final_format="opus 128"),
        )
        _populate_dl_info_from_import_result(dl, ir)
        self.assertEqual(dl.actual_filetype, "opus")
        self.assertEqual(dl.slskd_filetype, "flac")
        self.assertTrue(dl.is_vbr)
        self.assertEqual(dl.bitrate, 128000)
        self.assertEqual(dl.final_format, "opus 128")


class TestTargetFormatDispatch(unittest.TestCase):
    """Seam tests — --target-format flag wiring.

    Exercised through the surviving auto-import seam in lib.download.
    """

    def _get_cmd(self, target_format=None):
        album_data = _make_album_data()
        album_data.db_target_format = target_format
        ctx = _make_ctx()
        ctx.cfg.verified_lossless_target = ""
        ir = make_import_result(decision="import")
        return _dispatch_valid_result_cmd(album_data=album_data, ctx=ctx, ir=ir)

    def test_target_format_passed_when_set(self):
        cmd = self._get_cmd(target_format="flac")
        self.assertIn("--target-format", cmd)
        idx = cmd.index("--target-format")
        self.assertEqual(cmd[idx + 1], "flac")

    def test_target_format_not_passed_when_none(self):
        cmd = self._get_cmd(target_format=None)
        self.assertNotIn("--target-format", cmd)


if __name__ == "__main__":
    unittest.main()
