"""Tests for lib/import_dispatch.py — auto-import decision tree.

Orchestration tests (TestDispatchImport, TestQualityGate*) use FakePipelineDB
and assert domain state. Seam tests (TestOverrideMinBitrate, TestOpus*,
TestTargetFormat*) test subprocess argv and adapter wiring via MagicMock.
Pure function tests (TestPopulateDlInfo*, TestCleanupStagedDir) test in/out.
"""

import os
import shutil
import subprocess as sp
import tempfile
import unittest
from unittest.mock import MagicMock, patch

from lib.config import SoularrConfig
from lib.quality import (DownloadInfo, ImportResult, ConversionInfo,
                         AudioQualityMeasurement,
                         QUALITY_UPGRADE_TIERS, QUALITY_FLAC_ONLY)
from tests.fakes import FakePipelineDB
from tests.helpers import make_import_result, make_request_row, patch_dispatch_externals


# --- Local helpers for seam tests that call dispatch_import() (adapter) ---

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
    mock.files = [MagicMock(username="user1", filename="01 - Track.mp3")]
    return mock


def _make_ctx():
    """Build a mock SoularrContext."""
    ctx = MagicMock()
    ctx.cfg.beets_harness_path = "/nix/store/fake/harness/run_beets_harness.sh"
    ctx.cfg.beets_distance_threshold = 0.15
    ctx.cooled_down_users = set()
    ctx.pipeline_db_source = MagicMock()
    db_mock = MagicMock()
    db_mock.get_request.return_value = make_request_row(status="downloading")
    ctx.pipeline_db_source._get_db.return_value = db_mock
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


class TestPopulateDlInfoFromImportResult(unittest.TestCase):

    def test_converted_flac_to_v0(self):
        from lib.import_dispatch import _populate_dl_info_from_import_result
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
        from lib.import_dispatch import _populate_dl_info_from_import_result
        dl = DownloadInfo(filetype="mp3")
        ir = make_import_result(was_converted=False, new_min_bitrate=320)
        _populate_dl_info_from_import_result(dl, ir)
        self.assertFalse(dl.was_converted)
        self.assertEqual(dl.slskd_filetype, "mp3")
        self.assertEqual(dl.actual_filetype, "mp3")


class TestCleanupStagedDir(unittest.TestCase):

    def test_removes_dir_and_empty_parent(self):
        from lib.import_dispatch import _cleanup_staged_dir
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
        from lib.import_dispatch import _cleanup_staged_dir
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


class TestDispatchImport(unittest.TestCase):
    """Orchestration tests — assert domain state via FakePipelineDB."""

    _SENTINEL = object()

    def _dispatch(self, ir=_SENTINEL, request_overrides=None):
        from lib.import_dispatch import dispatch_import_core
        if ir is self._SENTINEL:
            ir = make_import_result(decision="import")

        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42, status="downloading",
            **(request_overrides or {}),
        ))
        cfg = SoularrConfig(
            beets_harness_path=_HARNESS,
            pipeline_db_enabled=True,
        )
        dl_info = DownloadInfo(filetype="mp3")

        tmpdir = tempfile.mkdtemp()
        try:
            with patch_dispatch_externals() as ext, \
                 patch("lib.import_dispatch._check_quality_gate_core") as mock_gate, \
                 patch("lib.import_dispatch.parse_import_result", return_value=ir):
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

    def test_error_decision(self):
        ir = make_import_result(decision="conversion_failed",
                                error="ffmpeg failed")
        r = self._dispatch(ir)
        self.assertEqual(r["db"].download_logs[0].outcome, "rejected")

    def test_no_json_result(self):
        r = self._dispatch(None)
        self.assertEqual(len(r["db"].download_logs), 1)
        self.assertEqual(r["db"].download_logs[0].outcome, "failed")

    def test_timeout(self):
        from lib.import_dispatch import dispatch_import_core
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=42, status="downloading"))

        with patch("lib.import_dispatch.sp.run",
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
        from lib.import_dispatch import dispatch_import_core
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=42, status="downloading"))

        with patch("lib.import_dispatch.sp.run",
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


class TestOverrideMinBitrate(unittest.TestCase):
    """Seam tests — subprocess arg wiring for --override-min-bitrate.

    Tests the dispatch_import() adapter's override computation. Will break
    if import_one becomes a library call (#48).

    The override must be grade-aware: spectral bitrate only participates when
    current_spectral_grade is in {suspect, likely_transcode}. Genuine/marginal/
    None grades must leave the container bitrate untouched — see issue #61.
    """

    def _get_override_value(self, db_fields):
        from lib.import_dispatch import dispatch_import
        album_data = _make_album_data()
        ctx = _make_ctx()
        db_mock = ctx.pipeline_db_source._get_db.return_value
        db_mock.get_request.return_value = db_fields
        bv_result = _make_bv_result()
        dl_info = DownloadInfo(filetype="mp3")
        ir = make_import_result(decision="import")

        with patch_dispatch_externals() as ext, \
             patch("lib.import_dispatch._check_quality_gate_core"), \
             patch("lib.import_dispatch.parse_import_result", return_value=ir):
            dispatch_import(album_data, bv_result, "/tmp/dest", dl_info,
                            42, ctx)
            cmd = ext.run.call_args[0][0]

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
        from lib.import_dispatch import dispatch_import_core
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=42, status="downloading"))
        ir = make_import_result(decision="import")

        with patch_dispatch_externals() as ext, \
             patch("lib.import_dispatch._check_quality_gate_core"), \
             patch("lib.import_dispatch.parse_import_result", return_value=ir):
            dispatch_import_core(
                path="/tmp/dest", mb_release_id="mbid-1",
                request_id=42, label="Test Artist - Test Album",
                beets_harness_path=_HARNESS,
                cfg=cfg_obj,
                db=db,  # type: ignore[arg-type]
                dl_info=DownloadInfo(filetype="mp3"),
                files=[MagicMock(username="user1", filename="01.mp3")],
            )
            return ext.run.call_args[0][0]

    def _extract_rank_config_json(self, cmd):
        for i, arg in enumerate(cmd):
            if arg == "--quality-rank-config" and i + 1 < len(cmd):
                return cmd[i + 1]
        return None

    def test_default_cfg_serializes_to_argv(self):
        """Default QualityRankConfig → argv contains the round-trip JSON."""
        from lib.config import SoularrConfig
        from lib.quality import QualityRankConfig
        cfg = SoularrConfig(beets_harness_path=_HARNESS)
        cmd = self._run_dispatch_capture_cmd(cfg)
        raw = self._extract_rank_config_json(cmd)
        self.assertIsNotNone(raw)
        assert raw is not None  # for pyright
        # Round-trip must produce an equal QualityRankConfig
        restored = QualityRankConfig.from_json(raw)
        self.assertEqual(restored, cfg.quality_ranks)

    def test_custom_cfg_serializes_to_argv(self):
        """Custom gate_min_rank + metric survive the argv round-trip."""
        from lib.config import SoularrConfig
        from lib.quality import (QualityRank, QualityRankConfig,
                                 RankBitrateMetric)
        custom_ranks = QualityRankConfig(
            bitrate_metric=RankBitrateMetric.MIN,
            gate_min_rank=QualityRank.GOOD,
            within_rank_tolerance_kbps=15,
        )
        cfg = SoularrConfig(
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


class TestLoadQualityGateState(unittest.TestCase):
    """Direct tests for the shared quality-gate state adapter."""

    def test_uses_full_request_row_to_apply_final_format(self):
        """The shared adapter must honor persisted final_format labels."""
        from lib.beets_db import AlbumInfo
        from lib.import_dispatch import load_quality_gate_state
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
    """Orchestration tests for _check_quality_gate_core via FakePipelineDB."""

    def _run_quality_gate(self, gate_decision, **extra_req_fields):
        from lib.import_dispatch import _check_quality_gate_core
        db = FakePipelineDB()
        merged = {"status": "imported", "current_spectral_bitrate": None,
                  "verified_lossless": False}
        merged.update(extra_req_fields)
        db.seed_request(make_request_row(id=42, **merged))

        with patch("lib.beets_db.BeetsDB") as mock_beets_cls, \
             patch("lib.quality.quality_gate_decision",
                   return_value=gate_decision):
            mock_beets = MagicMock()
            mock_beets.__enter__ = MagicMock(return_value=mock_beets)
            mock_beets.__exit__ = MagicMock(return_value=False)
            mock_beets.get_album_info.return_value = MagicMock(
                min_bitrate_kbps=192, is_cbr=True)
            mock_beets_cls.return_value = mock_beets
            _check_quality_gate_core(
                mb_id="test-mbid", label="Test Artist - Test Album",
                request_id=42,
                files=[MagicMock(username="user1", filename="01.mp3")],
                db=db,  # type: ignore[arg-type]
            )

        return db

    def test_no_mb_id_returns_early(self):
        """Empty mb_id should return without doing anything."""
        from lib.import_dispatch import _check_quality_gate_core
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=42, status="imported"))
        _check_quality_gate_core(
            mb_id="", label="Test", request_id=42, files=[],
            db=db)  # type: ignore[arg-type]
        # Status unchanged — gate returned early
        self.assertEqual(db.request(42)["status"], "imported")

    def test_requeue_upgrade_uses_intent(self):
        db = self._run_quality_gate("requeue_upgrade")
        row = db.request(42)
        self.assertEqual(row["status"], "wanted")
        self.assertEqual(row["search_filetype_override"], QUALITY_UPGRADE_TIERS)

    def test_requeue_upgrade_verified_lossless_still_requeues(self):
        db = self._run_quality_gate("requeue_upgrade", verified_lossless=True)
        row = db.request(42)
        self.assertEqual(row["status"], "wanted")
        self.assertEqual(row["search_filetype_override"], QUALITY_UPGRADE_TIERS)

    def test_requeue_upgrade_verified_lossless_denylist_reason_preserved(self):
        db = self._run_quality_gate(
            "requeue_upgrade",
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
        from lib.import_dispatch import _check_quality_gate_core
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
        from lib.import_dispatch import _check_quality_gate_core
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
        db = self._run_quality_gate("requeue_lossless")
        row = db.request(42)
        self.assertEqual(row["status"], "wanted")
        self.assertEqual(row["search_filetype_override"], QUALITY_FLAC_ONLY)

    def test_quality_gate_reads_current_spectral_not_last_download(self):
        """Quality gate must use current_spectral_bitrate (what's on disk),
        not last_download_spectral_bitrate (stale from a previous download)."""
        from lib.import_dispatch import _check_quality_gate_core
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42, status="imported",
            last_download_spectral_bitrate=192,
            current_spectral_bitrate=None,
            verified_lossless=False,
        ))

        captured_measurement = {}

        def capture_decision(measurement, cfg=None):
            captured_measurement["m"] = measurement
            return "accept"

        with patch("lib.beets_db.BeetsDB") as mock_beets_cls, \
             patch("lib.quality.quality_gate_decision",
                   side_effect=capture_decision):
            mock_beets = MagicMock()
            mock_beets.__enter__ = MagicMock(return_value=mock_beets)
            mock_beets.__exit__ = MagicMock(return_value=False)
            mock_beets.get_album_info.return_value = MagicMock(
                min_bitrate_kbps=226, is_cbr=False)
            mock_beets_cls.return_value = mock_beets
            _check_quality_gate_core(
                mb_id="test-mbid", label="Test Artist - Test Album",
                request_id=42, files=[],
                db=db)  # type: ignore[arg-type]

        m = captured_measurement["m"]
        self.assertIsNone(m.spectral_bitrate_kbps,
                          "quality gate should use current_spectral_bitrate, "
                          "not stale last_download_spectral_bitrate")

    def test_genuine_v0_replacing_transcode_accepted(self):
        """Genuine V0 replacing a transcode should be accepted, not requeued."""
        from lib.import_dispatch import _check_quality_gate_core
        from lib.quality import quality_gate_decision

        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42, status="imported",
            last_download_spectral_bitrate=None,
            last_download_spectral_grade="genuine",
            current_spectral_bitrate=None,
            current_spectral_grade="genuine",
            verified_lossless=False,
        ))

        captured = {}

        def capture_and_decide(measurement, cfg=None):
            captured["m"] = measurement
            return quality_gate_decision(measurement)

        with patch("lib.beets_db.BeetsDB") as mock_beets_cls, \
             patch("lib.quality.quality_gate_decision",
                   side_effect=capture_and_decide):
            mock_beets = MagicMock()
            mock_beets.__enter__ = MagicMock(return_value=mock_beets)
            mock_beets.__exit__ = MagicMock(return_value=False)
            mock_beets.get_album_info.return_value = MagicMock(
                min_bitrate_kbps=226, avg_bitrate_kbps=226,
                format="MP3", is_cbr=False)
            mock_beets_cls.return_value = mock_beets
            _check_quality_gate_core(
                mb_id="test-mbid", label="Test Artist - Test Album",
                request_id=42, files=[],
                db=db)  # type: ignore[arg-type]

        m = captured["m"]
        self.assertEqual(m.min_bitrate_kbps, 226)
        self.assertFalse(m.is_cbr)
        self.assertIsNone(m.spectral_bitrate_kbps)
        self.assertEqual(quality_gate_decision(m), "accept")
        # Should stay imported (not requeued)
        self.assertEqual(db.request(42)["status"], "imported")

    def _capture_gate_measurement(self, *, current_spectral_grade,
                                  current_spectral_bitrate,
                                  beets_min_bitrate_kbps):
        """Run _check_quality_gate_core and capture the AudioQualityMeasurement."""
        from lib.import_dispatch import _check_quality_gate_core
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42, status="imported",
            current_spectral_grade=current_spectral_grade,
            current_spectral_bitrate=current_spectral_bitrate,
            verified_lossless=False,
        ))
        captured = {}

        def capture(measurement, cfg=None):
            captured["m"] = measurement
            return "accept"

        with patch("lib.beets_db.BeetsDB") as mock_beets_cls, \
             patch("lib.quality.quality_gate_decision", side_effect=capture):
            mock_beets = MagicMock()
            mock_beets.__enter__ = MagicMock(return_value=mock_beets)
            mock_beets.__exit__ = MagicMock(return_value=False)
            mock_beets.get_album_info.return_value = MagicMock(
                min_bitrate_kbps=beets_min_bitrate_kbps, is_cbr=False)
            mock_beets_cls.return_value = mock_beets
            _check_quality_gate_core(
                mb_id="test-mbid", label="Test Artist - Test Album",
                request_id=42, files=[],
                db=db)  # type: ignore[arg-type]
        return db, captured["m"]

    def test_quality_gate_uses_likely_transcode_spectral(self):
        """likely_transcode album grade must feed into the gate, not just suspect.

        Regression for issue #61: _check_quality_gate_core previously only
        accepted "suspect", silently ignoring the album-level "likely_transcode"
        grade produced by classify_album when >=60% of tracks are suspect.
        """
        _, m = self._capture_gate_measurement(
            current_spectral_grade="likely_transcode",
            current_spectral_bitrate=180,
            beets_min_bitrate_kbps=226,
        )
        self.assertEqual(m.spectral_bitrate_kbps, 180)

    def test_quality_gate_ignores_genuine_low_spectral(self):
        """Genuine grade with low spectral estimate must NOT lower the gate bitrate.

        Guards the original #31 fix: a lo-fi genuine V0 (e.g. ~160kbps cliff
        estimate) must not trigger a requeue loop when beets reports 226kbps.
        """
        db, m = self._capture_gate_measurement(
            current_spectral_grade="genuine",
            current_spectral_bitrate=160,
            beets_min_bitrate_kbps=226,
        )
        self.assertIsNone(m.spectral_bitrate_kbps)
        self.assertEqual(db.request(42)["status"], "imported")

    def test_dispatch_requeue_uses_intent(self):
        """Transcode-upgrade requeue path uses quality constants."""
        from lib.import_dispatch import dispatch_import_core
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=42, status="downloading"))
        ir = make_import_result(decision="transcode_upgrade",
                                new_min_bitrate=227)

        with patch_dispatch_externals(), \
             patch("lib.import_dispatch._check_quality_gate_core"), \
             patch("lib.import_dispatch.parse_import_result", return_value=ir):
            dispatch_import_core(
                path="/tmp/dest", mb_release_id="test-mbid",
                request_id=42, label="Test",
                beets_harness_path=_HARNESS,
                db=db,  # type: ignore[arg-type]
                dl_info=DownloadInfo(filetype="mp3"),
                files=[MagicMock(username="user1", filename="01.mp3")],
            )

        row = db.request(42)
        self.assertEqual(row["status"], "wanted")
        self.assertEqual(row["search_filetype_override"], QUALITY_UPGRADE_TIERS)


class TestQualityGatePreservesTargetFormat(unittest.TestCase):
    """Quality gate accept must clear search_filetype_override but preserve target_format."""

    def _run_quality_gate_accept(self, target_format="flac"):
        from lib.import_dispatch import _check_quality_gate_core
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42, status="imported",
            target_format=target_format,
            verified_lossless=True,
            current_spectral_bitrate=None,
            search_filetype_override="lossless",  # should be cleared
        ))

        with patch("lib.beets_db.BeetsDB") as mock_beets_cls, \
             patch("lib.quality.quality_gate_decision",
                   return_value="accept"):
            mock_beets = MagicMock()
            mock_beets.__enter__ = MagicMock(return_value=mock_beets)
            mock_beets.__exit__ = MagicMock(return_value=False)
            mock_beets.get_album_info.return_value = MagicMock(
                min_bitrate_kbps=255, is_cbr=False)
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

    Will break if import_one becomes a library call (#48).
    """

    def _get_cmd(self, verified_lossless_target=""):
        from lib.import_dispatch import dispatch_import
        album_data = _make_album_data()
        ctx = _make_ctx()
        ctx.cfg.verified_lossless_target = verified_lossless_target
        bv_result = _make_bv_result()
        dl_info = DownloadInfo(filetype="flac")
        ir = make_import_result(decision="import", was_converted=True,
                                original_filetype="flac", target_filetype="mp3")

        with patch_dispatch_externals() as ext, \
             patch("lib.import_dispatch._check_quality_gate_core"), \
             patch("lib.import_dispatch.parse_import_result", return_value=ir):
            dispatch_import(album_data, bv_result, "/tmp/dest", dl_info,
                            42, ctx)
            return ext.run.call_args[0][0]

    def test_target_flag_passed_when_set(self):
        cmd = self._get_cmd(verified_lossless_target="opus 128")
        self.assertIn("--verified-lossless-target", cmd)
        idx = cmd.index("--verified-lossless-target")
        self.assertEqual(cmd[idx + 1], "opus 128")

    def test_target_flag_not_passed_when_empty(self):
        cmd = self._get_cmd(verified_lossless_target="")
        self.assertNotIn("--verified-lossless-target", cmd)

    def test_opus_import_result_populates_dl_info(self):
        from lib.import_dispatch import _populate_dl_info_from_import_result
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

    Will break if import_one becomes a library call (#48).
    """

    def _get_cmd(self, target_format=None):
        from lib.import_dispatch import dispatch_import
        album_data = _make_album_data()
        album_data.db_target_format = target_format
        ctx = _make_ctx()
        ctx.cfg.verified_lossless_target = ""
        bv_result = _make_bv_result()
        dl_info = DownloadInfo(filetype="flac")
        ir = make_import_result(decision="import")

        with patch_dispatch_externals() as ext, \
             patch("lib.import_dispatch._check_quality_gate_core"), \
             patch("lib.import_dispatch.parse_import_result", return_value=ir):
            dispatch_import(album_data, bv_result, "/tmp/dest", dl_info,
                            42, ctx)
            return ext.run.call_args[0][0]

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
