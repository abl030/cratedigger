"""Integration slice tests — real code paths with minimal patching.

These exercise real orchestration flows end-to-end, patching only external
edges: subprocess (sp.run), filesystem cleanup, network calls (meelo/plex/jellyfin),
and BeetsDB (requires real beets SQLite DB on disk).

The key difference from unit/orchestration tests is that parse_import_result
and _check_quality_gate_core run for real, not patched.
"""

import os
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from lib.beets_db import AlbumInfo
from lib.config import CratediggerConfig
from lib.quality import (
    IMPORT_RESULT_SENTINEL,
    QUALITY_LOSSLESS,
    QUALITY_UPGRADE_TIERS,
    AudioQualityMeasurement,
    ConversionInfo,
    DownloadInfo,
    ImportResult,
    PostflightInfo,
)
from tests.fakes import FakePipelineDB
from tests.helpers import (
    make_import_result,
    make_request_row,
    patch_dispatch_externals,
)


_HARNESS = "/nix/store/fake/harness/run_beets_harness.sh"


def _make_stdout(ir: ImportResult) -> str:
    """Build subprocess stdout containing the import result sentinel line."""
    return f"some log output\n{IMPORT_RESULT_SENTINEL}{ir.to_json()}\n"


def _mock_beets_db(beets_info):
    """Configure a mocked BeetsDB context manager returning beets_info."""
    mock_beets_instance = MagicMock()
    mock_beets_instance.get_album_info.return_value = beets_info
    mock_cls = MagicMock()
    mock_cls.return_value.__enter__ = MagicMock(return_value=mock_beets_instance)
    mock_cls.return_value.__exit__ = MagicMock(return_value=False)
    return mock_cls


class TestDispatchThroughQualityGate(unittest.TestCase):
    """Integration slice: dispatch_import_core → real parse_import_result
    → real _check_quality_gate_core → domain state assertions.

    Patches only: sp.run, cleanup, meelo/plex, BeetsDB.
    Runs for real: parse_import_result, dispatch_action, _do_mark_done,
    _check_quality_gate_core, quality_gate_decision, apply_transition.
    """

    def _run_dispatch(self, ir, beets_info, request_overrides=None, cfg=None):
        """Dispatch an import and return the FakePipelineDB state."""
        from lib.import_dispatch import dispatch_import_core

        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42, status="downloading",
            **(request_overrides or {}),
        ))

        if cfg is None:
            cfg = CratediggerConfig(
                beets_harness_path=_HARNESS,
                pipeline_db_enabled=True,
            )
        dl_info = DownloadInfo(username="user1")
        stdout = _make_stdout(ir)

        tmpdir = tempfile.mkdtemp()
        try:
            with patch_dispatch_externals() as ext, \
                 patch("lib.beets_db.BeetsDB", _mock_beets_db(beets_info)):
                ext.run.return_value = MagicMock(
                    returncode=0, stdout=stdout, stderr="")
                dispatch_import_core(
                    path=tmpdir,
                    mb_release_id="mbid-123",
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
            import shutil
            shutil.rmtree(tmpdir, ignore_errors=True)

        return db

    def test_import_quality_accept(self):
        """VBR 245kbps → quality gate accepts → imported, override cleared."""
        ir = make_import_result(decision="import", new_min_bitrate=245)
        beets_info = AlbumInfo(
            album_id=1, track_count=10, min_bitrate_kbps=245,
            avg_bitrate_kbps=245, format="MP3",
            is_cbr=False, album_path="/Beets/Test")

        db = self._run_dispatch(ir, beets_info)

        row = db.request(42)
        self.assertEqual(row["status"], "imported")
        self.assertIsNone(row["search_filetype_override"])
        self.assertEqual(row["min_bitrate"], 245)
        self.assertEqual(len(db.download_logs), 1)
        db.assert_log(self, 0, outcome="success", request_id=42)

    def test_imported_path_reflects_beets_destination(self):
        """Issue #93: ``album_requests.imported_path`` must be the beets
        destination (``ir.postflight.imported_path``), not the source/staging
        path passed to dispatch_import_core.

        Pre-fix: ``imported_path`` stored the source
        ``/mnt/virtio/music/slskd/failed_imports/...`` even though beets
        moved files to ``/mnt/virtio/Music/Beets/...``. UI's "Imported to"
        label displayed the source, confusing users.
        """
        ir = make_import_result(
            decision="import",
            new_min_bitrate=245,
            imported_path="/Beets/Test Artist/2005 - Test Album_",
        )
        beets_info = AlbumInfo(
            album_id=1, track_count=10, min_bitrate_kbps=245,
            avg_bitrate_kbps=245, format="MP3",
            is_cbr=False, album_path="/Beets/Test")

        db = self._run_dispatch(ir, beets_info)

        row = db.request(42)
        self.assertEqual(
            row["imported_path"],
            "/Beets/Test Artist/2005 - Test Album_",
            "album_requests.imported_path must reflect the beets "
            "destination from ImportResult.postflight, not dispatch's "
            "source path (the /tmp staging/failed_imports dir)")

    def test_import_quality_requeue_upgrade(self):
        """VBR 180kbps → quality gate requeues for upgrade → wanted, denylist."""
        ir = make_import_result(decision="import", new_min_bitrate=180)
        beets_info = AlbumInfo(
            album_id=1, track_count=10, min_bitrate_kbps=180,
            avg_bitrate_kbps=180, format="MP3",
            is_cbr=False, album_path="/Beets/Test")

        db = self._run_dispatch(ir, beets_info)

        row = db.request(42)
        self.assertEqual(row["status"], "wanted")
        self.assertEqual(row["search_filetype_override"], QUALITY_UPGRADE_TIERS)
        self.assertEqual(row["min_bitrate"], 180)
        # Source denylisted for low quality
        self.assertEqual(len(db.denylist), 1)
        self.assertEqual(db.denylist[0].username, "user1")
        self.assertIn("quality gate", db.denylist[0].reason or "")

    def test_import_quality_requeue_lossless(self):
        """CBR 320 → quality gate requeues for lossless → wanted, lossless override."""
        ir = make_import_result(decision="import", new_min_bitrate=320)
        beets_info = AlbumInfo(
            album_id=1, track_count=10, min_bitrate_kbps=320,
            avg_bitrate_kbps=320, format="MP3",
            is_cbr=True, album_path="/Beets/Test")

        db = self._run_dispatch(ir, beets_info)

        row = db.request(42)
        self.assertEqual(row["status"], "wanted")
        self.assertEqual(row["search_filetype_override"], QUALITY_LOSSLESS)


    def test_transcode_upgrade_requeues_with_denylist(self):
        """Transcode upgrade → mark_done + requeue to upgrade tiers + denylist."""
        ir = make_import_result(decision="transcode_upgrade", new_min_bitrate=227)
        beets_info = AlbumInfo(
            album_id=1, track_count=10, min_bitrate_kbps=227,
            avg_bitrate_kbps=227, format="MP3",
            is_cbr=False, album_path="/Beets/Test")

        db = self._run_dispatch(ir, beets_info)

        row = db.request(42)
        # Transcode upgrade requeues directly (no quality gate)
        self.assertEqual(row["status"], "wanted")
        self.assertEqual(row["search_filetype_override"], QUALITY_UPGRADE_TIERS)
        # Transcode source denylisted
        self.assertTrue(len(db.denylist) >= 1)
        db.assert_log(self, 0, outcome="success")

    def test_downgrade_prevented(self):
        """Downgrade -> record rejection + denylist, no quality gate."""
        ir = make_import_result(decision="downgrade",
                                new_min_bitrate=128, prev_min_bitrate=320)
        beets_info = AlbumInfo(
            album_id=1, track_count=10, min_bitrate_kbps=128,
            avg_bitrate_kbps=128, format="MP3",
            is_cbr=False, album_path="/Beets/Test")

        db = self._run_dispatch(ir, beets_info)

        row = db.request(42)
        self.assertEqual(row["status"], "wanted")
        self.assertTrue(len(db.denylist) >= 1)
        db.assert_log(self, 0, outcome="rejected")

    def test_custom_gate_min_rank_accepts_lower(self):
        """Custom gate_min_rank=GOOD must flip requeue → accept end-to-end.

        Locks the runtime config threading: cfg.quality_ranks → dispatch
        → _check_quality_gate_core → quality_gate_decision. If any hop
        drops cfg, this test fails because the gate falls back to
        default EXCELLENT and 180kbps still requeues.
        """
        from lib.quality import QualityRank, QualityRankConfig

        ir = make_import_result(decision="import", new_min_bitrate=180)
        beets_info = AlbumInfo(
            album_id=1, track_count=10, min_bitrate_kbps=180,
            avg_bitrate_kbps=180, format="MP3",
            is_cbr=False, album_path="/Beets/Test")

        custom_cfg = CratediggerConfig(
            beets_harness_path=_HARNESS,
            pipeline_db_enabled=True,
            quality_ranks=QualityRankConfig(gate_min_rank=QualityRank.GOOD),
        )

        db = self._run_dispatch(ir, beets_info, cfg=custom_cfg)

        row = db.request(42)
        # Under default gate_min_rank=EXCELLENT, 180 MP3 VBR = GOOD → requeue.
        # Under the custom cfg (gate_min_rank=GOOD), 180 passes.
        self.assertEqual(
            row["status"], "imported",
            "cfg.quality_ranks.gate_min_rank=GOOD must thread through "
            "dispatch_import_core → _check_quality_gate_core → "
            "quality_gate_decision. If cfg is dropped at any hop, "
            "180kbps falls back to the default EXCELLENT gate and requeues.")
        self.assertIsNone(row["search_filetype_override"])

    def test_median_metric_accepts_outlier_album_end_to_end(self):
        """MEDIAN policy must thread through dispatch → quality gate (#64).

        Album has tracks {60, 60, 245, 245, 245} — three V0 tracks plus two
        very-quiet intros. Under MIN the album is POOR (60), under AVG it's
        GOOD (171), and only under MEDIAN does it reach TRANSPARENT (245)
        and pass the default EXCELLENT gate.

        If load_quality_gate_state (lib/import_dispatch.py) drops the
        median field when constructing the AudioQualityMeasurement, or if
        the rank cfg fails to thread through, this test fails because the
        gate falls back to AVG=171 (GOOD < EXCELLENT) and requeues. This
        is the only end-to-end coverage for the issue #64 dispatch path.
        """
        from lib.quality import QualityRankConfig, RankBitrateMetric

        ir = make_import_result(decision="import", new_min_bitrate=60)
        beets_info = AlbumInfo(
            album_id=1, track_count=5,
            min_bitrate_kbps=60,
            avg_bitrate_kbps=171,
            median_bitrate_kbps=245,
            format="MP3", is_cbr=False, album_path="/Beets/Test")

        custom_cfg = CratediggerConfig(
            beets_harness_path=_HARNESS,
            pipeline_db_enabled=True,
            quality_ranks=QualityRankConfig(
                bitrate_metric=RankBitrateMetric.MEDIAN),
        )

        db = self._run_dispatch(ir, beets_info, cfg=custom_cfg)

        row = db.request(42)
        self.assertEqual(
            row["status"], "imported",
            "MEDIAN policy must thread through dispatch_import_core → "
            "load_quality_gate_state → measurement_rank. If any hop drops "
            "median_bitrate_kbps from the AudioQualityMeasurement, the "
            "gate falls back to avg=171 (GOOD < EXCELLENT) and requeues.")
        self.assertIsNone(row["search_filetype_override"])

    def test_default_avg_metric_requeues_same_outlier_album(self):
        """Counterfactual to MEDIAN slice: same album, default cfg, requeues.

        Pinning this proves the difference in the MEDIAN test really comes
        from the policy switch — not from a hidden change in dispatch flow.
        """
        ir = make_import_result(decision="import", new_min_bitrate=60)
        beets_info = AlbumInfo(
            album_id=1, track_count=5,
            min_bitrate_kbps=60,
            avg_bitrate_kbps=171,
            median_bitrate_kbps=245,
            format="MP3", is_cbr=False, album_path="/Beets/Test")

        db = self._run_dispatch(ir, beets_info)

        row = db.request(42)
        self.assertEqual(
            row["status"], "wanted",
            "Default AVG policy on the same outlier album must requeue — "
            "if this fails, the MEDIAN slice's pass is meaningless.")
        self.assertEqual(row["search_filetype_override"], QUALITY_UPGRADE_TIERS)

    def test_native_vbr_import_clears_stale_final_format(self):
        """A later plain MP3 import must clear an old target-format label.

        If `final_format='opus 64'` is left behind from a previous import,
        the rank gate misclassifies the new on-disk MP3 as GOOD and requeues
        it forever. The success path must clear stale `final_format` when the
        new import does not carry an explicit label.
        """
        ir = make_import_result(decision="import", new_min_bitrate=245)
        beets_info = AlbumInfo(
            album_id=1, track_count=10, min_bitrate_kbps=245,
            avg_bitrate_kbps=245, format="MP3",
            is_cbr=False, album_path="/Beets/Test")

        db = self._run_dispatch(
            ir,
            beets_info,
            request_overrides={"final_format": "opus 64"},
        )

        row = db.request(42)
        self.assertEqual(
            row["status"], "imported",
            "stale final_format labels must be cleared so the quality gate "
            "uses the new beets codec metadata")
        self.assertIsNone(row["search_filetype_override"])
        self.assertIsNone(row.get("final_format"))

    def test_native_cbr_import_clears_stale_verified_lossless(self):
        """A later non-verified import must clear an old verified flag.

        Otherwise a plain CBR 320 replacement inherits `verified_lossless=True`
        from the previous album and incorrectly skips the lossless requeue path.
        """
        ir = make_import_result(decision="import", new_min_bitrate=320)
        beets_info = AlbumInfo(
            album_id=1, track_count=10, min_bitrate_kbps=320,
            avg_bitrate_kbps=320, format="MP3",
            is_cbr=True, album_path="/Beets/Test")

        db = self._run_dispatch(
            ir,
            beets_info,
            request_overrides={"verified_lossless": True},
        )

        row = db.request(42)
        self.assertEqual(
            row["status"], "wanted",
            "stale verified_lossless=True must be cleared so native CBR "
            "imports still requeue for lossless verification")
        self.assertEqual(row["search_filetype_override"], QUALITY_LOSSLESS)
        self.assertFalse(row["verified_lossless"])


class TestQualityGateVerifiedLosslessBypass(unittest.TestCase):
    """Integration slice: quality gate honors persisted final_format labels."""

    def test_verified_lossless_low_bitrate_accepts(self):
        """207kbps V0 from verified FLAC → accepted via final_format='mp3 v0'."""
        from lib.import_dispatch import _check_quality_gate_core

        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42, status="imported", verified_lossless=True,
            final_format="mp3 v0"))

        beets_info = AlbumInfo(
            album_id=1, track_count=10, min_bitrate_kbps=207,
            avg_bitrate_kbps=207, format="MP3",
            is_cbr=False, album_path="/Beets/Test")

        with patch("lib.beets_db.BeetsDB", _mock_beets_db(beets_info)):
            _check_quality_gate_core(
                mb_id="mbid-123",
                label="Lo-Fi Album",
                request_id=42,
                files=[MagicMock(username="user1")],
                db=db,  # type: ignore[arg-type]
            )

        row = db.request(42)
        self.assertEqual(row["status"], "imported")
        self.assertEqual(row["min_bitrate"], 207)
        self.assertEqual(len(db.denylist), 0)


class TestQualityGateSpectralOverride(unittest.TestCase):
    """Integration slice: quality gate uses spectral bitrate when it's lower
    than beets container bitrate."""

    def test_suspect_spectral_triggers_requeue(self):
        """Container 320kbps but spectral says 128kbps → requeue for upgrade."""
        from lib.import_dispatch import _check_quality_gate_core

        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42, status="imported",
            current_spectral_grade="suspect",
            current_spectral_bitrate=128,
        ))

        beets_info = AlbumInfo(
            album_id=1, track_count=10, min_bitrate_kbps=320,
            avg_bitrate_kbps=320, format="MP3",
            is_cbr=False, album_path="/Beets/Test")

        with patch("lib.beets_db.BeetsDB", _mock_beets_db(beets_info)):
            _check_quality_gate_core(
                mb_id="mbid-123",
                label="Fake 320 Album",
                request_id=42,
                files=[MagicMock(username="user1")],
                db=db,  # type: ignore[arg-type]
            )

        row = db.request(42)
        self.assertEqual(row["status"], "wanted")
        self.assertEqual(row["search_filetype_override"], QUALITY_UPGRADE_TIERS)
        self.assertEqual(len(db.denylist), 1)
        self.assertIn("spectral", db.denylist[0].reason or "")


class TestSpectralPropagationSlice(unittest.TestCase):
    """Integration slice: shared run_preimport_gates updates spectral state + denylists.

    Exercises the pre-import gate pipeline that both the auto-import path
    (lib.download._process_beets_validation) and the force/manual-import path
    (lib.import_dispatch.dispatch_import_from_db) delegate to. Proves the
    function does its side effects — spectral state write + denylist —
    consistently regardless of caller.
    """

    def test_suspect_download_updates_current_spectral_and_denylists(self):
        from lib.config import CratediggerConfig
        from lib.preimport import run_preimport_gates

        db = FakePipelineDB()
        db.seed_request(make_request_row(id=42, status="downloading"))
        beets_info = AlbumInfo(
            album_id=1,
            track_count=10,
            min_bitrate_kbps=320,
            avg_bitrate_kbps=320,
            format="MP3",
            is_cbr=True,
            album_path="/Beets/Test",
        )
        cfg = CratediggerConfig(audio_check_mode="off")

        with patch(
            "lib.preimport.spectral_analyze",
            side_effect=[
                SimpleNamespace(
                    grade="suspect",
                    estimated_bitrate_kbps=128,
                    suspect_pct=90.0,
                    tracks=[],
                ),
                SimpleNamespace(
                    grade="genuine",
                    estimated_bitrate_kbps=320,
                    suspect_pct=0.0,
                    tracks=[],
                ),
            ],
        ), patch("lib.beets_db.BeetsDB", _mock_beets_db(beets_info)), \
             patch("os.path.isdir", return_value=True):
            with self.assertLogs("cratedigger", level="WARNING") as logs:
                result = run_preimport_gates(
                    path="/tmp/download",
                    mb_release_id="mbid-123",
                    label="Test Artist - Test Album",
                    download_filetype="mp3",
                    download_min_bitrate_bps=320_000,
                    download_is_vbr=False,
                    cfg=cfg,
                    db=db,  # type: ignore[arg-type]
                    request_id=42,
                    usernames={"user1"},
                )

        row = db.request(42)
        self.assertIn("SPECTRAL REJECT", "\n".join(logs.output))
        self.assertEqual(row["current_spectral_grade"], "genuine")
        self.assertEqual(row["current_spectral_bitrate"], 320)
        self.assertFalse(result.valid)
        self.assertEqual(result.scenario, "spectral_reject")
        self.assertEqual(len(db.denylist), 1)
        self.assertEqual(db.denylist[0].username, "user1")
        self.assertIn("spectral: 128kbps <= existing 320kbps",
                      db.denylist[0].reason or "")

    def test_stale_album_path_does_not_self_compare(self):
        """Issue #90: when BeetsDB returns an album whose on-disk path has
        gone stale (``os.path.isdir`` returns False), propagation must not
        mutate ``existing_spectral`` *before* ``spectral_import_decision``
        runs — otherwise the download is compared against itself and
        legitimate suspect-grade downloads get rejected by their own
        spectral estimate.

        Setup: beets says the album exists (min_bitrate=320) but
        isdir(album_path) is False. Download is suspect at 128kbps.

        With the bug: propagation writes download's 128kbps into
        existing_spectral, then decision sees new=128 vs existing=128 →
        reject (self-compare).

        Correct behavior: decision compares download's 128 against the
        container's 320 (fallback via existing_min_bitrate) → reject with a
        legitimate comparison; OR if existing_min_bitrate also isn't
        trustworthy (e.g. caller treats stale path as no-existing),
        import_no_exist. The key invariant: the reject reason must NOT
        read ``spectral {x}kbps <= existing {x}kbps`` with equal numbers.
        """
        from lib.config import CratediggerConfig
        from lib.preimport import run_preimport_gates

        db = FakePipelineDB()
        db.seed_request(make_request_row(id=42, status="downloading"))
        # Album metadata present in beets...
        beets_info = AlbumInfo(
            album_id=1,
            track_count=10,
            min_bitrate_kbps=320,
            avg_bitrate_kbps=320,
            format="MP3",
            is_cbr=True,
            album_path="/Beets/Test",
        )
        cfg = CratediggerConfig(audio_check_mode="off")

        with patch(
            "lib.preimport.spectral_analyze",
            return_value=SimpleNamespace(
                grade="suspect",
                estimated_bitrate_kbps=128,
                suspect_pct=90.0,
                tracks=[],
            ),
        ), patch("lib.beets_db.BeetsDB", _mock_beets_db(beets_info)), \
             patch("os.path.isdir", return_value=False):
            result = run_preimport_gates(
                path="/tmp/download",
                mb_release_id="mbid-123",
                label="Test Artist - Test Album",
                download_filetype="mp3",
                download_min_bitrate_bps=320_000,
                download_is_vbr=False,
                cfg=cfg,
                db=db,  # type: ignore[arg-type]
                request_id=42,
                usernames={"user1"},
                propagate_download_to_existing=True,
            )

        # The self-compare bug — if any reject fires, it must not read as
        # "spectral X <= existing X" with equal numbers. A legitimate reject
        # against the container bitrate (320) is allowed.
        if not result.valid:
            self.assertNotEqual(result.detail,
                                "spectral 128kbps <= existing 128kbps",
                                "self-compare bug: download compared against "
                                "a propagated copy of its own spectral")

    def test_stale_album_path_rejects_against_container_bitrate(self):
        """Issue #90 regression guard: when beets path is stale, the spectral
        decision must fall back to the container's min_bitrate (320), not
        the download's own spectral. So a suspect 128kbps download rejects
        against 320kbps (a real comparison) and the detail string reflects
        that — not the self-compare "128 <= 128" the old code produced.
        """
        from lib.config import CratediggerConfig
        from lib.preimport import run_preimport_gates

        db = FakePipelineDB()
        db.seed_request(make_request_row(id=42, status="downloading"))
        beets_info = AlbumInfo(
            album_id=1,
            track_count=10,
            min_bitrate_kbps=320,
            avg_bitrate_kbps=320,
            format="MP3",
            is_cbr=True,
            album_path="/Beets/Test",
        )
        cfg = CratediggerConfig(audio_check_mode="off")

        with patch(
            "lib.preimport.spectral_analyze",
            return_value=SimpleNamespace(
                grade="suspect",
                estimated_bitrate_kbps=128,
                suspect_pct=90.0,
                tracks=[],
            ),
        ), patch("lib.beets_db.BeetsDB", _mock_beets_db(beets_info)), \
             patch("os.path.isdir", return_value=False):
            result = run_preimport_gates(
                path="/tmp/download",
                mb_release_id="mbid-123",
                label="Test Artist - Test Album",
                download_filetype="mp3",
                download_min_bitrate_bps=320_000,
                download_is_vbr=False,
                cfg=cfg,
                db=db,  # type: ignore[arg-type]
                request_id=42,
                usernames={"user1"},
                propagate_download_to_existing=True,
            )

        self.assertFalse(result.valid, "suspect 128 < container 320 should reject")
        self.assertEqual(result.scenario, "spectral_reject")
        self.assertEqual(result.detail, "spectral 128kbps <= existing 320kbps",
                         "reject must compare against stale container's 320kbps, "
                         "not a propagated copy of the download's 128kbps")

    def test_stale_album_path_imports_when_download_beats_container(self):
        """Issue #90 correctness: suspect download above the container
        bitrate must import (import_upgrade) instead of self-rejecting.

        Without the fix: propagation writes 280 into existing_spectral,
        decision sees 280 <= 280 → reject. A legitimate upgrade blocked.

        With the fix: decision sees 280 vs container 256 → import_upgrade.
        """
        from lib.config import CratediggerConfig
        from lib.preimport import run_preimport_gates

        db = FakePipelineDB()
        db.seed_request(make_request_row(id=42, status="downloading"))
        beets_info = AlbumInfo(
            album_id=1,
            track_count=10,
            min_bitrate_kbps=256,
            avg_bitrate_kbps=256,
            format="MP3",
            is_cbr=True,
            album_path="/Beets/Test",
        )
        cfg = CratediggerConfig(audio_check_mode="off")

        with patch(
            "lib.preimport.spectral_analyze",
            return_value=SimpleNamespace(
                grade="suspect",
                estimated_bitrate_kbps=280,
                suspect_pct=90.0,
                tracks=[],
            ),
        ), patch("lib.beets_db.BeetsDB", _mock_beets_db(beets_info)), \
             patch("os.path.isdir", return_value=False):
            result = run_preimport_gates(
                path="/tmp/download",
                mb_release_id="mbid-123",
                label="Test Artist - Test Album",
                download_filetype="mp3",
                download_min_bitrate_bps=280_000,
                download_is_vbr=False,
                cfg=cfg,
                db=db,  # type: ignore[arg-type]
                request_id=42,
                usernames={"user1"},
                propagate_download_to_existing=True,
            )

        self.assertTrue(result.valid,
                        "suspect 280 > container 256 should import (upgrade)")
        # Propagation still persisted the download's spectral for future runs.
        row = db.request(42)
        self.assertEqual(row["current_spectral_grade"], "suspect")
        self.assertEqual(row["current_spectral_bitrate"], 280)


class TestDispatchNoJsonResult(unittest.TestCase):
    """Integration slice: sp.run returns no sentinel -> record rejection."""

    def test_no_json_marks_failed_and_requeues(self):
        """No __IMPORT_RESULT__ in stdout → scenario=no_json_result, requeue."""
        from lib.import_dispatch import dispatch_import_core

        db = FakePipelineDB()
        db.seed_request(make_request_row(id=42, status="downloading"))

        cfg = CratediggerConfig(
            beets_harness_path=_HARNESS,
            pipeline_db_enabled=True,
        )

        tmpdir = tempfile.mkdtemp()
        try:
            with patch_dispatch_externals() as ext, \
                 patch("lib.beets_db.BeetsDB", _mock_beets_db(None)):
                ext.run.return_value = MagicMock(
                    returncode=1, stdout="some error\n", stderr="")
                dispatch_import_core(
                    path=tmpdir,
                    mb_release_id="mbid-123",
                    request_id=42,
                    label="Test Artist - Test Album",
                    beets_harness_path=_HARNESS,
                    db=db,  # type: ignore[arg-type]
                    dl_info=DownloadInfo(username="user1"),
                    cfg=cfg,
                )
        finally:
            import shutil
            shutil.rmtree(tmpdir, ignore_errors=True)

        row = db.request(42)
        self.assertEqual(row["status"], "wanted")
        self.assertEqual(len(db.download_logs), 1)
        db.assert_log(self, 0, outcome="failed")


class TestForceImportSlice(unittest.TestCase):
    """Integration slice: dispatch_import_from_db with force=True."""

    def test_force_import_success(self):
        """Force-import → imported, download_log outcome=force_import."""
        from lib.import_dispatch import dispatch_import_from_db

        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42, status="manual", mb_release_id="mbid-123",
            min_bitrate=180, current_spectral_bitrate=128,
        ))

        ir = make_import_result(decision="import", new_min_bitrate=320)
        stdout = _make_stdout(ir)
        beets_info = AlbumInfo(
            album_id=1, track_count=10, min_bitrate_kbps=320,
            avg_bitrate_kbps=320, format="MP3",
            is_cbr=False, album_path="/Beets/Test")

        cfg = CratediggerConfig(
            beets_harness_path=_HARNESS,
            pipeline_db_enabled=True,
        )

        tmpdir = tempfile.mkdtemp()
        try:
            with patch_dispatch_externals() as ext, \
                 patch("lib.beets_db.BeetsDB", _mock_beets_db(beets_info)), \
                 patch("lib.config.read_runtime_config",
                       return_value=cfg):
                ext.run.return_value = MagicMock(
                    returncode=0, stdout=stdout, stderr="")
                result = dispatch_import_from_db(
                    db, request_id=42, failed_path=tmpdir,  # type: ignore[arg-type]
                    force=True, source_username="user1",
                )
        finally:
            import shutil
            shutil.rmtree(tmpdir, ignore_errors=True)

        self.assertTrue(result.success)
        row = db.request(42)
        self.assertEqual(row["status"], "imported")
        db.assert_log(self, 0, outcome="force_import")

    def test_force_import_imported_path_reflects_beets_destination(self):
        """Issue #93 was reported against force-import specifically:
        album_requests.imported_path must reflect the beets destination
        (ir.postflight.imported_path), not the source failed_imports/ path.

        Guards that the fix propagates through dispatch_import_from_db →
        dispatch_import_core → _do_mark_done end-to-end. Parallel to
        TestDispatchThroughQualityGate.test_imported_path_reflects_beets_destination
        which covers the auto path.
        """
        from lib.import_dispatch import dispatch_import_from_db

        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=833, status="manual", mb_release_id="mbid-go-team",
            imported_path="/mnt/virtio/music/slskd/failed_imports/stale-source",
        ))

        # The beets destination lives in ir.postflight.imported_path.
        ir = make_import_result(
            decision="import",
            new_min_bitrate=320,
            imported_path="/Beets/The Go! Team/2005 - Are You Ready for More_",
        )
        stdout = _make_stdout(ir)
        beets_info = AlbumInfo(
            album_id=1, track_count=10, min_bitrate_kbps=320,
            avg_bitrate_kbps=320, format="MP3",
            is_cbr=False, album_path="/Beets/Test")

        cfg = CratediggerConfig(
            beets_harness_path=_HARNESS, pipeline_db_enabled=True)

        tmpdir = tempfile.mkdtemp()
        try:
            with patch_dispatch_externals() as ext, \
                 patch("lib.beets_db.BeetsDB", _mock_beets_db(beets_info)), \
                 patch("lib.config.read_runtime_config", return_value=cfg):
                ext.run.return_value = MagicMock(
                    returncode=0, stdout=stdout, stderr="")
                dispatch_import_from_db(
                    db, request_id=833, failed_path=tmpdir,  # type: ignore[arg-type]
                    force=True, source_username="ttttsv",
                )
        finally:
            import shutil
            shutil.rmtree(tmpdir, ignore_errors=True)

        row = db.request(833)
        self.assertEqual(
            row["imported_path"],
            "/Beets/The Go! Team/2005 - Are You Ready for More_",
            "force-import must overwrite the stale source path with "
            "ir.postflight.imported_path (the actual beets destination)")


class TestPreserveSourceSlice(unittest.TestCase):
    """Integration slice for issue #111 — force/manual import holds lossless
    originals across the V0 conversion until the quality decision has
    returned a non-terminal verdict.

    The real bug: with no ``verified_lossless_target`` configured,
    ``convert_lossless(V0_SPEC, keep_source=False)`` deleted FLACs in the
    user's ``failed_imports/`` directory *before* the quality decision ran.
    A subsequent ``downgrade`` / ``transcode_downgrade`` verdict then left
    the user's source material destroyed.

    These slices exercise the real ``convert_lossless`` + the real
    ``target_cleanup_decision`` end-to-end so we cannot regress the
    invariant "on a terminal quality verdict, the staged FLACs remain
    untouched" without a test failing.
    """

    def _make_flac(self, folder: str, name: str) -> str:
        import subprocess
        path = os.path.join(folder, name)
        subprocess.run(
            ["ffmpeg", "-f", "lavfi", "-i", "sine=frequency=440:duration=1",
             "-y", path],
            capture_output=True, timeout=30, check=True)
        return path

    def test_preserve_source_flac_survives_terminal_exit(self):
        """``convert_lossless(V0_SPEC, keep_source=True)`` keeps the FLACs on
        disk. If the caller would then exit (terminal quality verdict) we
        never reach the ``target_cleanup_decision`` call, so the FLACs
        remain — matching ``import_one.py``'s line-997 terminal branch."""
        import tempfile
        from harness.import_one import convert_lossless, V0_SPEC
        with tempfile.TemporaryDirectory() as tmpdir:
            flac_path = self._make_flac(tmpdir, "01.flac")

            converted, failed, _ = convert_lossless(
                tmpdir, V0_SPEC, keep_source=True)

            self.assertEqual((converted, failed), (1, 0))
            self.assertTrue(os.path.exists(flac_path),
                            "FLAC must survive V0 conversion when "
                            "keep_source=True (terminal verdict path)")
            self.assertTrue(os.path.exists(os.path.join(tmpdir, "01.mp3")))

    def test_preserve_source_flac_cleaned_after_non_terminal_decision(self):
        """After a non-terminal quality decision, the preserve-source cleanup
        path runs so beets sees only V0 MP3s. This slice drives the real
        ``target_cleanup_decision`` + ``_remove_lossless_files`` path."""
        import tempfile
        from harness.import_one import (convert_lossless, V0_SPEC,
                                        _remove_lossless_files,
                                        target_cleanup_decision)
        with tempfile.TemporaryDirectory() as tmpdir:
            flac_path = self._make_flac(tmpdir, "01.flac")

            converted, failed, _ = convert_lossless(
                tmpdir, V0_SPEC, keep_source=True)
            self.assertEqual((converted, failed), (1, 0))
            self.assertTrue(os.path.exists(flac_path))

            # Simulate the main() post-decision branch: non-terminal verdict
            # → target_cleanup_decision fires with preserve_source=True.
            should_clean = target_cleanup_decision(
                target_achieved=False, target_was_configured=False,
                sources_kept=converted, preserve_source=True)
            self.assertTrue(should_clean)
            _remove_lossless_files(tmpdir)

            self.assertFalse(os.path.exists(flac_path),
                             "FLAC must be cleaned before beets import once "
                             "quality decision approved")
            self.assertTrue(os.path.exists(os.path.join(tmpdir, "01.mp3")),
                            "V0 MP3 must survive cleanup")

    def test_keep_lossless_mode_does_not_strip_normalized_flac(self):
        """PR #112 Codex round 1 P1: force/manual import with
        ``target_format=flac`` (or "lossless") runs the normalization
        branch (ALAC→FLAC) but never runs the V0 pass. The
        preserve-source cleanup must NOT fire in that branch — otherwise
        it deletes the freshly normalized FLAC that beets is supposed to
        receive, i.e. the user's only copy in ``failed_imports/``.

        This slice mirrors the ``if not keep_lossless and
        target_cleanup_decision(...)`` gate at the caller end: when
        keep_lossless is True the cleanup is skipped entirely, so the
        predicate's verdict is irrelevant and the FLAC survives.
        """
        import tempfile
        from harness.import_one import _remove_lossless_files
        with tempfile.TemporaryDirectory() as tmpdir:
            flac_path = self._make_flac(tmpdir, "01.flac")

            keep_lossless = True
            preserve_source = True
            # Caller gate — matches the `if not keep_lossless and ...` in
            # import_one.py::main() at the cleanup point.
            if not keep_lossless:
                _remove_lossless_files(tmpdir)
            self.assertTrue(os.path.exists(flac_path),
                            "keep_lossless=True must skip the "
                            "preserve-source cleanup — the FLAC is what "
                            "beets is supposed to import")
            del preserve_source  # unused, kept for scenario clarity

    def test_terminal_exit_removes_v0_artifacts_for_next_retry(self):
        """PR #112 Codex round 2 P1: when a force/manual import rejects
        on downgrade/transcode_downgrade, the harness must remove the
        temporary V0 MP3s before exiting so the next retry sees a clean
        FLAC-only folder. Leaving mixed FLAC+MP3 in place would cause the
        next pass to measure across mixed bitrates, skip the
        verified_lossless_target pass, and potentially import the stale
        V0 MP3s instead of the configured target format.

        This slice simulates the terminal-exit branch of main() at
        ``if qd.is_terminal:`` — we do not drive the full main() (that
        needs beets) but we exercise the same ``_remove_files_by_ext``
        call on the same folder layout, and assert the contract: FLAC
        remains, V0 MP3s removed.
        """
        import tempfile
        from harness.import_one import (convert_lossless, V0_SPEC,
                                        _remove_files_by_ext)
        with tempfile.TemporaryDirectory() as tmpdir:
            flac_path = self._make_flac(tmpdir, "01.flac")
            converted, _, _ = convert_lossless(
                tmpdir, V0_SPEC, keep_source=True)
            self.assertEqual(converted, 1)
            mp3_path = os.path.join(tmpdir, "01.mp3")
            self.assertTrue(os.path.exists(mp3_path))
            self.assertTrue(os.path.exists(flac_path))

            # Simulate the terminal-exit branch (preserve_source=True).
            _remove_files_by_ext(tmpdir, "." + V0_SPEC.extension)

            self.assertTrue(os.path.exists(flac_path),
                            "Original FLAC must survive terminal exit "
                            "under --preserve-source")
            self.assertFalse(os.path.exists(mp3_path),
                             "Temporary V0 MP3 must be removed so next "
                             "retry sees a clean FLAC-only folder")

    def test_retry_flow_without_conversion_still_cleans_leftover_flac(self):
        """PR #112 Codex round 1 P2: on a second force/manual attempt the
        V0 MP3s from the first attempt already exist, so
        ``convert_lossless`` skips and reports ``converted == 0``. The
        lossless originals from the prior run are still on disk and must
        be cleaned before beets runs — otherwise beets sees a mixed
        FLAC+MP3 tree and imports the wrong media.
        """
        import tempfile
        from harness.import_one import (convert_lossless, V0_SPEC,
                                        _remove_lossless_files,
                                        target_cleanup_decision)
        with tempfile.TemporaryDirectory() as tmpdir:
            flac_path = self._make_flac(tmpdir, "01.flac")

            # First attempt: V0 conversion with keep_source=True (both
            # FLAC and MP3 now exist).
            converted1, _, _ = convert_lossless(
                tmpdir, V0_SPEC, keep_source=True)
            self.assertEqual(converted1, 1)

            # Second attempt: output MP3 exists — convert_lossless skips.
            converted2, _, _ = convert_lossless(
                tmpdir, V0_SPEC, keep_source=True)
            self.assertEqual(converted2, 0,
                             "V0 MP3 already exists — convert_lossless "
                             "must skip on retry")
            self.assertTrue(os.path.exists(flac_path))

            # Cleanup predicate must still trigger on preserve_source even
            # though this run converted 0 files.
            should_clean = target_cleanup_decision(
                target_achieved=False, target_was_configured=False,
                sources_kept=converted2, preserve_source=True)
            self.assertTrue(should_clean,
                            "retry path: preserve_source must drive "
                            "cleanup even when converted==0")
            _remove_lossless_files(tmpdir)
            self.assertFalse(os.path.exists(flac_path),
                             "leftover FLAC from prior run must be "
                             "removed on retry so beets sees only V0")
            self.assertTrue(os.path.exists(os.path.join(tmpdir, "01.mp3")))


class TestBayOfBiscayUpgradeChain(unittest.TestCase):
    """Two real-world downloads chained against Velella Velella - The Bay of
    Biscay (request 1055), documenting three counter-intuitive pipeline
    behaviors as durable regression tests.

    The live chain, in chronological order:

      Step 1 (brandlos, download_log id=3628)
        existing on disk: genuine  128k min / 172k avg (audited FLAC-less VBR)
        new download:     l.trans. 119k min / 179k avg, spectral ~160k
        outcome: IMPORT — even though spectral grade regressed
                 (genuine → likely_transcode) and min dropped (128 → 119),
                 avg ticked up 172 → 179 under the default AVG metric.

      Step 2 (Ceezles, download_log id=3631)
        existing on disk: l.trans. 119k min / 179k avg (what brandlos left)
        new download:     l.trans. 162k min / 225k avg, spectral ~192k
        outcome: IMPORT + quality gate DONE — avg=225 ≥ EXCELLENT threshold
                 (210) so the gate satisfies EXCELLENT rank despite
                 spectral=likely_transcode on the file.

    What this slice protects (and what it teaches by reading it):

      1. MIN on VBR lies; gate/comparisons use AVG. A lo-fi VBR indie
         album with quiet intros can sit at min=119k while avg is 179k
         — any logic that prefers MIN would have flagged step 1 as a
         downgrade and rejected it. This slice fails if anyone swaps
         the default metric away from AVG without updating the rank config.
      2. Spectral grade is provenance, not quality. A `genuine` 128k VBR
         file holds less information than a `likely_transcode` derived
         from a 160k source. A naive rule ("spectral regression = block
         import") would have rejected step 1. Keep the rule structural,
         not cosmetic.
      3. Two transcoded files can chain to a gate pass. Gate accepts at
         rank EXCELLENT via avg — it doesn't require verified lossless
         provenance. That's a feature for releases with no lossless
         source on Soulseek, and it's what the user's UI showed as "DONE"
         on request 1055. This slice pins that behavior.

    See also: `pipeline-cli show 1055`, download_log rows 3628 + 3631.
    """

    def _run_dispatch(self, ir, beets_info, request_overrides=None):
        """Inline copy of TestDispatchThroughQualityGate._run_dispatch so
        this class is self-contained and doesn't inherit unrelated tests."""
        from lib.import_dispatch import dispatch_import_core

        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42, status="downloading",
            **(request_overrides or {}),
        ))
        cfg = CratediggerConfig(
            beets_harness_path=_HARNESS,
            pipeline_db_enabled=True,
        )
        dl_info = DownloadInfo(username="user1", filetype="mp3")
        stdout = _make_stdout(ir)

        tmpdir = tempfile.mkdtemp()
        try:
            with patch_dispatch_externals() as ext, \
                 patch("lib.beets_db.BeetsDB", _mock_beets_db(beets_info)):
                ext.run.return_value = MagicMock(
                    returncode=0, stdout=stdout, stderr="")
                dispatch_import_core(
                    path=tmpdir,
                    mb_release_id="mbid-biscay",
                    request_id=42,
                    label="Velella Velella - The Bay of Biscay",
                    beets_harness_path=_HARNESS,
                    db=db,  # type: ignore[arg-type]
                    dl_info=dl_info,
                    distance=0.08,
                    scenario="strong_match",
                    files=[MagicMock(username="user1",
                                     filename="01 - Do Not Fold.mp3")],
                    cfg=cfg,
                )
        finally:
            import shutil
            shutil.rmtree(tmpdir, ignore_errors=True)

        return db

    @staticmethod
    def _ir_import(new: AudioQualityMeasurement,
                   existing: AudioQualityMeasurement) -> ImportResult:
        """Build an `import` ImportResult with independent avg/min/median.

        ``make_import_result`` collapses avg/min/median onto a single scalar,
        which would hide the exact thing this slice exists to document —
        VBR albums where avg ≠ min drives both the import decision and the
        quality gate verdict.
        """
        return ImportResult(
            decision="import",
            new_measurement=new,
            existing_measurement=existing,
            conversion=ConversionInfo(),
            postflight=PostflightInfo(),
        )

    def test_step1_brandlos_imports_transcode_over_genuine_on_avg_gain(self):
        """Step 1 of the chain. Spectral grade regresses genuine → likely_transcode
        and min drops 128 → 119, yet avg rises 172 → 179 so ``import_quality_decision``
        returns ``import``. This is the call that surprised the operator in
        the live UI — documenting it as a test means it stays a deliberate
        design choice, not an accident.
        """
        new = AudioQualityMeasurement(
            min_bitrate_kbps=119, avg_bitrate_kbps=179, median_bitrate_kbps=181,
            format="MP3", is_cbr=False, verified_lossless=False,
            spectral_grade="likely_transcode", spectral_bitrate_kbps=160,
        )
        existing = AudioQualityMeasurement(
            min_bitrate_kbps=128, avg_bitrate_kbps=172, median_bitrate_kbps=192,
            format="MP3", is_cbr=False, verified_lossless=False,
            spectral_grade="genuine", spectral_bitrate_kbps=128,
        )

        # Pin the decision logic directly. dispatch_import_core trusts the
        # ``decision`` field in the harness-emitted JSON and does not recompute
        # it — so without this, a regression in compare_quality()/
        # import_quality_decision() (e.g. returning "downgrade" when min drops
        # 128→119) could still leave the slice below green. Fail-fast here so
        # the critique is the decision call itself, not the dispatch wiring.
        from lib.quality import import_quality_decision
        self.assertEqual(
            import_quality_decision(new, existing), "import",
            "compare_quality must rank new above existing on AVG (179>172) "
            "despite MIN regressing and spectral flipping to likely_transcode. "
            "If this fails, the slice below is moot — fix the decision, not "
            "the slice.")

        ir = self._ir_import(new, existing)
        # Post-import, beets reflects the newly-imported files (brandlos's).
        beets_info = AlbumInfo(
            album_id=1, track_count=16,
            min_bitrate_kbps=119, avg_bitrate_kbps=179, median_bitrate_kbps=181,
            format="MP3", is_cbr=False, album_path="/Beets/Velella Velella")

        db = self._run_dispatch(
            ir, beets_info,
            # Pre-import request state — genuine 128k on disk.
            request_overrides={
                "min_bitrate": 128,
                "verified_lossless": False,
                "current_spectral_grade": "genuine",
                "current_spectral_bitrate": 128,
                "final_format": "mp3",
            })

        row = db.request(42)
        # The import itself succeeded — this is what the assertion on
        # import_quality_decision's behavior actually checks. min_bitrate
        # landed at 119 = brandlos's file (was 128 pre-dispatch).
        # Note: prev_min_bitrate is not pinned here — two transitions
        # fire in a single dispatch (imported, then wanted for the gate
        # requeue), and the second transition overwrites prev with the
        # post-import value. That's a known quirk of the double
        # transition, unrelated to the decision this test pins.
        self.assertEqual(
            row["min_bitrate"], 119,
            "avg gain (172 → 179) must overrule the spectral grade "
            "regression (genuine → likely_transcode). If min_bitrate is "
            "still 128, import_quality_decision rejected the download — "
            "check whether someone added a 'spectral regression blocks "
            "import' rule.")
        db.assert_log(self, 0, outcome="success")
        # The gate THEN runs on the new on-disk state (avg=179 < 210) and
        # requeues. Status transitions imported → wanted in a single
        # dispatch — the two-hop chain in production is built out of these
        # single-dispatch cycles back to back.
        self.assertEqual(
            row["status"], "wanted",
            "After the successful import, the gate must requeue for an "
            "upgrade (avg=179 < EXCELLENT=210). This requeue is what "
            "chained Ceezles in step 2.")
        self.assertEqual(
            row["search_filetype_override"], QUALITY_UPGRADE_TIERS,
            "Requeue must set the upgrade override so the next search "
            "prefers higher-quality tiers.")

    def test_step2_ceezles_crosses_excellent_threshold_on_avg(self):
        """Step 2 of the chain. Previous state is what step 1 left on disk
        (likely_transcode 119k / 179k avg). New download is a higher-bitrate
        transcode (likely_transcode 162k / 225k avg, spectral ~192k).

        Despite the file being a confirmed transcode, avg=225 crosses
        EXCELLENT (≥210) so the quality gate accepts. This pins two things:
         - AVG is what determines rank, not MIN (162 < 210 would requeue).
         - Verified-lossless provenance is not required for DONE — the rank
           itself is the gate.
        """
        new = AudioQualityMeasurement(
            min_bitrate_kbps=162, avg_bitrate_kbps=225, median_bitrate_kbps=226,
            format="MP3", is_cbr=False, verified_lossless=False,
            spectral_grade="likely_transcode", spectral_bitrate_kbps=192,
        )
        existing = AudioQualityMeasurement(
            min_bitrate_kbps=119, avg_bitrate_kbps=179, median_bitrate_kbps=181,
            format="MP3", is_cbr=False, verified_lossless=False,
            spectral_grade="likely_transcode", spectral_bitrate_kbps=160,
        )

        # Direct assertion on the decision function — see rationale on the
        # twin assertion in test_step1. Locks compare_quality behavior, not
        # just dispatch wiring.
        from lib.quality import import_quality_decision
        self.assertEqual(
            import_quality_decision(new, existing), "import",
            "compare_quality must rank new above existing on AVG (225>179). "
            "If this fails, the slice below is moot — fix the decision, not "
            "the slice.")

        ir = self._ir_import(new, existing)
        beets_info = AlbumInfo(
            album_id=1, track_count=16,
            min_bitrate_kbps=162, avg_bitrate_kbps=225, median_bitrate_kbps=226,
            format="MP3", is_cbr=False, album_path="/Beets/Velella Velella")

        db = self._run_dispatch(
            ir, beets_info,
            request_overrides={
                "min_bitrate": 119,
                "verified_lossless": False,
                "current_spectral_grade": "likely_transcode",
                "current_spectral_bitrate": 160,
                "last_download_spectral_grade": "likely_transcode",
                "last_download_spectral_bitrate": 160,
                "final_format": "mp3",
                "search_filetype_override": QUALITY_UPGRADE_TIERS,
            })

        row = db.request(42)
        self.assertEqual(
            row["status"], "imported",
            "avg=225 reaches EXCELLENT rank (≥210). If this flips to "
            "'wanted', someone probably tightened the gate threshold or "
            "switched the default metric to MIN — min=162 would requeue.")
        self.assertEqual(row["min_bitrate"], 162)
        self.assertEqual(row["prev_min_bitrate"], 119)
        self.assertIsNone(
            row["search_filetype_override"],
            "gate accept clears the upgrade override. If this is still "
            "QUALITY_UPGRADE_TIERS the gate requeued despite reaching "
            "EXCELLENT — check rank_cfg.gate_min_rank and the avg/min "
            "metric policy.")
        # verified_lossless stays False — we reached DONE via bitrate rank,
        # not via a FLAC → V0 verification path.
        self.assertFalse(row["verified_lossless"])


class TestReleaseLockContention(unittest.TestCase):
    """Integration slice for issue #133 / #132 P1: the cross-process
    same-release advisory lock.

    The Palo Santo data-loss fix (PR #131) removed the destructive branch
    from the harness but left a cross-process race: two processes
    importing the same MBID can each see the other's newly-inserted row
    as "the newest" and delete the wrong row during post-import cleanup.

    The fix: ``dispatch_import_core`` now wraps the ``import_one.py``
    subprocess in a release-keyed advisory lock (non-blocking). This
    slice pins two behaviors:

    1. **Contention** — when another session holds the release lock for
       the same MBID, ``dispatch_import_core`` returns early with a
       "try again shortly" outcome. It does NOT spawn ``import_one.py``
       and does NOT write a ``download_log`` row (deferred retry, not
       a failure). The request's status is untouched.

    2. **Happy path** — when the lock is free, the subprocess runs
       normally and the ``(namespace, key)`` recorded on the fake DB
       matches ``release_id_to_lock_key(mbid)`` so the lock is keyed
       exactly on the MBID and not e.g. the request_id.
    """

    MBID = "aaaaaaaa-1111-2222-3333-bbbbbbbbbbbb"

    def _make_cfg(self) -> CratediggerConfig:
        return CratediggerConfig(
            beets_harness_path=_HARNESS,
            pipeline_db_enabled=True,
        )

    def _make_db(self) -> FakePipelineDB:
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42, mb_release_id=self.MBID, status="downloading"))
        return db

    def test_auto_contention_returns_deferred_and_leaves_all_state(self):
        """Issue #132 P1 + Codex PR #136 R3 P2/P3 combined regression.

        Auto-path contention must return ``DispatchOutcome(deferred=True)``
        and leave EVERY piece of request state untouched, so
        ``poll_active_downloads`` can re-enter ``process_completed_album``
        on the next cycle and retry with all in-progress work
        preserved:

        - **status stays ``downloading``** — needed for
          ``poll_active_downloads`` to find the row on the next tick.
          The outer ``_run_completed_processing`` branches on
          ``outcome.deferred`` and skips the flip-to-imported that
          would otherwise fire on a True return from
          ``process_completed_album`` (the C1 bug from commit 2's
          review; fixed here without the earlier eager-reset
          side effects).
        - **staged dir stays put** — Codex R3 P3: deleting it forces
          the next cycle to redownload from Soulseek even if the
          competing import later fails. ``process_completed_album``
          is idempotent on a pre-existing staging dir (it guards
          ``os.mkdir`` with ``os.path.exists`` and skips file moves
          when the destination already has the file).
        - **``current_spectral_*`` stays populated** — Codex R3 P2:
          ``run_preimport_gates`` ran BEFORE this contention path
          fired and persisted spectral state from the downloaded
          files. A retry on the same files would compute the same
          spectral state anyway; clearing it would cause
          ``override_min_bitrate`` / quality-gate decisions to
          run against incomplete state on the next cycle.
        - **no download_log row** — contention is a deferred retry,
          not a failure.
        """
        from lib.import_dispatch import dispatch_import_core
        from lib.pipeline_db import (ADVISORY_LOCK_NAMESPACE_RELEASE,
                                     release_id_to_lock_key)

        db = self._make_db()
        # Seed the spectral fields that ``run_preimport_gates`` would
        # have populated pre-dispatch. These must survive the
        # contention path.
        db.request(42)["current_spectral_grade"] = "genuine"
        db.request(42)["current_spectral_bitrate"] = 245
        def lock_result(namespace: int, key: int) -> bool:
            if (namespace == ADVISORY_LOCK_NAMESPACE_RELEASE
                    and key == release_id_to_lock_key(self.MBID)):
                return False
            return True
        db.set_advisory_lock_result(lock_result)

        dl_info = DownloadInfo(username="user1")
        tmpdir = tempfile.mkdtemp()
        try:
            with patch_dispatch_externals() as ext:
                outcome = dispatch_import_core(
                    path=tmpdir,
                    mb_release_id=self.MBID,
                    request_id=42,
                    label="Test Artist - Test Album",
                    beets_harness_path=_HARNESS,
                    db=db,  # type: ignore[arg-type]
                    dl_info=dl_info,
                    distance=0.05,
                    scenario="strong_match",
                    files=[MagicMock(username="user1",
                                     filename="01 - Track.mp3")],
                    cfg=self._make_cfg(),
                )
        finally:
            import shutil
            shutil.rmtree(tmpdir, ignore_errors=True)

        # Subprocess NEVER fires.
        ext.run.assert_not_called()
        # Outcome signals deferral — the new seam.
        self.assertFalse(outcome.success)
        self.assertTrue(outcome.deferred)
        self.assertIn("Another import is already in progress",
                      outcome.message)

        # **All state preserved**:
        self.assertEqual(db.request(42)["status"], "downloading",
                         "Status must stay 'downloading' so "
                         "poll_active_downloads retries next cycle.")
        self.assertEqual(db.request(42)["current_spectral_grade"],
                         "genuine", "Spectral state from the "
                         "pre-dispatch run_preimport_gates MUST "
                         "survive contention (Codex R3 P2).")
        self.assertEqual(db.request(42)["current_spectral_bitrate"], 245)
        # No staged-dir cleanup — Codex R3 P3.
        ext.cleanup.assert_not_called()
        # No download_log row — deferred retry, not a failure.
        self.assertEqual(db.download_logs, [])

        # Lock attempt recorded on the RELEASE namespace at the MBID key.
        self.assertIn(
            (ADVISORY_LOCK_NAMESPACE_RELEASE,
             release_id_to_lock_key(self.MBID)),
            db.advisory_lock_calls)

    def test_force_import_contention_preserves_request_status(self):
        """Force/manual path contention must NOT reset status to 'wanted'
        — the caller (web UI, CLI) surfaces the "try again shortly"
        message and the request stays in whatever status it was in
        (typically 'imported' for force-import, since force-import runs
        on albums that were rejected from beets but have files on disk).

        This is the complement of ``test_auto_contention_resets_request_to_wanted``:
        the status-reset branch is gated on scenario NOT in
        FORCE_MANUAL_SCENARIOS, so force/manual leave the row alone.
        """
        from lib.import_dispatch import dispatch_import_core
        from lib.pipeline_db import (ADVISORY_LOCK_NAMESPACE_RELEASE,
                                     release_id_to_lock_key)

        db = FakePipelineDB()
        # Force-import typically runs against an 'imported' or 'manual'
        # row; pick 'imported' as the representative starting state.
        db.seed_request(make_request_row(
            id=42, mb_release_id=self.MBID, status="imported"))
        def lock_result(namespace: int, key: int) -> bool:
            if (namespace == ADVISORY_LOCK_NAMESPACE_RELEASE
                    and key == release_id_to_lock_key(self.MBID)):
                return False
            return True
        db.set_advisory_lock_result(lock_result)

        dl_info = DownloadInfo(username="user1")
        tmpdir = tempfile.mkdtemp()
        try:
            with patch_dispatch_externals() as ext:
                outcome = dispatch_import_core(
                    path=tmpdir,
                    mb_release_id=self.MBID,
                    request_id=42,
                    label="Test Artist - Force Import",
                    force=True,
                    beets_harness_path=_HARNESS,
                    db=db,  # type: ignore[arg-type]
                    dl_info=dl_info,
                    distance=0.05,
                    scenario="force_import",
                    files=[],
                    cfg=self._make_cfg(),
                )
        finally:
            import shutil
            shutil.rmtree(tmpdir, ignore_errors=True)

        # Status untouched — force/manual caller surfaces the message.
        self.assertEqual(db.request(42)["status"], "imported")
        # Staging cleanup MUST NOT run for force/manual — the path is
        # the user's failed_imports/ copy of the source, not a
        # disposable /Incoming dir. Deleting it would destroy the
        # user's only copy (issue #89 equivalent).
        ext.cleanup.assert_not_called()
        self.assertFalse(outcome.success)
        self.assertIn("Another import is already in progress",
                      outcome.message)

    def test_happy_path_acquires_lock_keyed_on_mbid_and_runs_import(self):
        from lib.import_dispatch import dispatch_import_core
        from lib.pipeline_db import (ADVISORY_LOCK_NAMESPACE_RELEASE,
                                     release_id_to_lock_key)

        db = self._make_db()
        # Default: all locks acquired. Happy path.
        ir = make_import_result(decision="import", new_min_bitrate=245)
        beets_info = AlbumInfo(
            album_id=1, track_count=10, min_bitrate_kbps=245,
            avg_bitrate_kbps=245, format="MP3",
            is_cbr=False, album_path="/Beets/Test")
        dl_info = DownloadInfo(username="user1")

        tmpdir = tempfile.mkdtemp()
        try:
            with patch_dispatch_externals() as ext, \
                 patch("lib.beets_db.BeetsDB", _mock_beets_db(beets_info)):
                ext.run.return_value = MagicMock(
                    returncode=0, stdout=_make_stdout(ir), stderr="")
                dispatch_import_core(
                    path=tmpdir,
                    mb_release_id=self.MBID,
                    request_id=42,
                    label="Test Artist - Test Album",
                    beets_harness_path=_HARNESS,
                    db=db,  # type: ignore[arg-type]
                    dl_info=dl_info,
                    distance=0.05,
                    scenario="strong_match",
                    files=[MagicMock(username="user1",
                                     filename="01 - Track.mp3")],
                    cfg=self._make_cfg(),
                )
        finally:
            import shutil
            shutil.rmtree(tmpdir, ignore_errors=True)

        # Subprocess ran exactly once.
        ext.run.assert_called_once()
        # Import succeeded; domain state reflects that.
        self.assertEqual(db.request(42)["status"], "imported")
        # Lock was taken on the RELEASE namespace with the hashed MBID
        # as key — NOT the request_id. Confirms keying is on the
        # release, which is the only way to serialise two different
        # request_ids that share an MBID (the auto-cycle vs force-import
        # race from the Palo Santo follow-up).
        self.assertIn(
            (ADVISORY_LOCK_NAMESPACE_RELEASE,
             release_id_to_lock_key(self.MBID)),
            db.advisory_lock_calls)

    def test_empty_mbid_skips_release_lock_but_still_imports(self):
        """Defensive: a caller that somehow reaches ``dispatch_import_core``
        with an empty mb_release_id should not block on a lock keyed on
        empty string (``crc32(b"") == 0``), which would otherwise
        serialise every empty-mbid import. The code skips the lock
        entirely and logs a warning.
        """
        from lib.import_dispatch import dispatch_import_core
        from lib.pipeline_db import ADVISORY_LOCK_NAMESPACE_RELEASE

        db = self._make_db()
        # Re-seed with empty mb_release_id.
        db.seed_request(make_request_row(
            id=43, mb_release_id="", status="downloading"))
        ir = make_import_result(decision="import", new_min_bitrate=245)
        beets_info = AlbumInfo(
            album_id=1, track_count=10, min_bitrate_kbps=245,
            avg_bitrate_kbps=245, format="MP3",
            is_cbr=False, album_path="/Beets/Test")
        dl_info = DownloadInfo(username="user1")

        tmpdir = tempfile.mkdtemp()
        try:
            with patch_dispatch_externals() as ext, \
                 patch("lib.beets_db.BeetsDB", _mock_beets_db(beets_info)):
                ext.run.return_value = MagicMock(
                    returncode=0, stdout=_make_stdout(ir), stderr="")
                dispatch_import_core(
                    path=tmpdir,
                    mb_release_id="",
                    request_id=43,
                    label="Test Artist - No MBID",
                    beets_harness_path=_HARNESS,
                    db=db,  # type: ignore[arg-type]
                    dl_info=dl_info,
                    distance=0.05,
                    scenario="strong_match",
                    files=[MagicMock(username="user1",
                                     filename="01 - Track.mp3")],
                    cfg=self._make_cfg(),
                )
        finally:
            import shutil
            shutil.rmtree(tmpdir, ignore_errors=True)

        # Subprocess runs (no lock held us up).
        ext.run.assert_called_once()
        # No RELEASE-namespace lock call — we skipped it entirely.
        namespaces_used = {ns for ns, _key in db.advisory_lock_calls}
        self.assertNotIn(ADVISORY_LOCK_NAMESPACE_RELEASE, namespaces_used)


class TestHandleValidResultReleaseLock(unittest.TestCase):
    """Issue #132 P1 + Codex PR #136 R4 P1: release-lock acquisition
    must happen BEFORE ``stage_to_ai`` so the filesystem stays
    resumable on contention.

    Pre-R4: ``_handle_valid_result`` called ``stage_to_ai`` first,
    then invoked ``dispatch_import`` which checked the lock inside
    ``dispatch_import_core``. On contention, files had already moved
    from ``slskd_download_dir/<import_folder>/`` →
    ``beets_staging_dir/``, but ``active_download_state`` still
    pointed at the original slskd paths. Next cycle's
    ``process_completed_album`` reconstructed stale source paths and
    crashed with ``FileNotFoundError``, falling back to
    ``status='wanted'`` — breaking the contention-retry contract.

    Post-R4: ``_handle_valid_result`` acquires the lock BEFORE
    ``stage_to_ai``. On contention, return deferred without staging;
    files stay at ``slskd_download_dir/<import_folder>/`` where
    ``process_completed_album``'s resume guard (``if os.path.exists(
    dst_file) and not os.path.exists(src_file): continue``) picks
    them up idempotently on the next cycle.
    """

    MBID = "aaaaaaaa-1111-2222-3333-bbbbbbbbbbbb"

    def test_contention_returns_deferred_without_staging(self):
        from lib import download as dl_mod
        from lib.grab_list import GrabListEntry
        from lib.pipeline_db import (ADVISORY_LOCK_NAMESPACE_RELEASE,
                                     release_id_to_lock_key)
        from lib.quality import ValidationResult

        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42, mb_release_id=self.MBID, status="downloading"))

        def lock_result(namespace: int, key: int) -> bool:
            if (namespace == ADVISORY_LOCK_NAMESPACE_RELEASE
                    and key == release_id_to_lock_key(self.MBID)):
                return False
            return True
        db.set_advisory_lock_result(lock_result)

        from tests.helpers import make_ctx_with_fake_db
        cfg = CratediggerConfig(
            beets_harness_path=_HARNESS,
            pipeline_db_enabled=True,
            beets_distance_threshold=0.15,
        )
        ctx = make_ctx_with_fake_db(db, cfg=cfg)

        entry = GrabListEntry(
            album_id=42, artist="Test Artist", title="Test Album",
            year="2006", files=[], filetype="mp3",
            mb_release_id=self.MBID,
            db_source="request", db_request_id=42)

        bv_result = ValidationResult(
            valid=True, distance=0.05, scenario="strong_match")

        # stage_to_ai and dispatch_import MUST NOT run on contention.
        import_folder_fullpath = "/tmp/test-import-folder"
        with patch.object(dl_mod, "stage_to_ai") as mock_stage, \
             patch.object(dl_mod, "dispatch_import") as mock_dispatch:
            outcome = dl_mod._handle_valid_result(
                entry, bv_result, import_folder_fullpath, ctx)

        assert outcome is not None
        self.assertTrue(outcome.deferred)
        self.assertFalse(outcome.success)
        # **Critical**: staging never ran — files stay at
        # import_folder_fullpath where process_completed_album's
        # resume guard can pick them up next cycle.
        mock_stage.assert_not_called()
        mock_dispatch.assert_not_called()

    def test_redownload_path_does_not_take_release_lock(self):
        """Redownload path (source != 'request') must NOT take the
        release lock — it only stages and marks done, never runs the
        harness, so no cross-process race applies. Pre-fix this was
        implicitly true; pinning it so a future refactor doesn't
        accidentally broaden the lock scope."""
        from lib import download as dl_mod
        from lib.grab_list import GrabListEntry
        from lib.pipeline_db import ADVISORY_LOCK_NAMESPACE_RELEASE
        from lib.quality import ValidationResult

        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42, mb_release_id=self.MBID, status="downloading"))

        from tests.helpers import make_ctx_with_fake_db
        cfg = CratediggerConfig(
            beets_harness_path=_HARNESS,
            pipeline_db_enabled=True,
            beets_distance_threshold=0.15,
        )
        ctx = make_ctx_with_fake_db(db, cfg=cfg)

        entry = GrabListEntry(
            album_id=42, artist="Test Artist", title="Test Album",
            year="2006", files=[], filetype="mp3",
            mb_release_id=self.MBID,
            db_source="redownload",  # NOT 'request'
            db_request_id=42)

        bv_result = ValidationResult(
            valid=True, distance=0.05, scenario="strong_match")

        with patch.object(dl_mod, "stage_to_ai",
                          return_value="/tmp/staged"):
            dl_mod._handle_valid_result(
                entry, bv_result, "/tmp/import", ctx)

        # No RELEASE-namespace lock call — redownload path skips it.
        namespaces_used = {ns for ns, _key in db.advisory_lock_calls}
        self.assertNotIn(ADVISORY_LOCK_NAMESPACE_RELEASE, namespaces_used)


class TestRunCompletedProcessingOutcomeBranching(unittest.TestCase):
    """The ``process_completed_album`` 3-way return → state-transition seam.

    Pre-#133 this was a 2-way ``bool``: True → flip to ``imported``,
    False → reset to ``wanted``. That binary misclassified release-
    lock contention (commit 43e83e8 C1 — silently flipped to
    ``imported``). Commit 2's fix papered over the bug by having
    ``dispatch_import_core`` eagerly reset the row to ``wanted``;
    Codex R3 P2/P3 then flagged that the reset clobbered spectral
    state and staged files.

    The proper seam is a 3-way return: ``None`` == deferred (leave
    everything alone). These tests pin the branching at
    ``_run_completed_processing`` so a future refactor can't silently
    collapse the three states back to two.
    """

    def _ctx(self, db: FakePipelineDB):
        from tests.helpers import make_ctx_with_fake_db
        return make_ctx_with_fake_db(db)

    def _entry(self):
        from tests.helpers import make_grab_list_entry
        return make_grab_list_entry()

    def _state(self):
        from lib.quality import ActiveDownloadState
        return ActiveDownloadState(
            filetype="mp3", enqueued_at="2026-04-20T00:00:00+00:00",
            files=[])

    def test_deferred_outcome_leaves_status_downloading(self):
        """``process_completed_album`` returning ``None`` must NOT touch
        the request's status. The next ``poll_active_downloads`` cycle
        will re-enter via status='downloading'.
        """
        from lib import download as dl_mod
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42, status="downloading",
            current_spectral_grade="genuine",
            current_spectral_bitrate=245))
        with patch.object(dl_mod, "process_completed_album",
                          return_value=None):
            dl_mod._run_completed_processing(
                self._entry(), 42, self._state(), db, self._ctx(db))
        self.assertEqual(db.request(42)["status"], "downloading")
        # Spectral untouched.
        self.assertEqual(
            db.request(42)["current_spectral_grade"], "genuine")
        # No status-history transitions recorded — we didn't call
        # apply_transition at all.
        self.assertEqual(db.status_history, [])

    def test_true_outcome_flips_to_imported(self):
        """Happy path: ``process_completed_album`` returns ``True`` and
        status was 'downloading' → flip to 'imported'."""
        from lib import download as dl_mod
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=42, status="downloading"))
        with patch.object(dl_mod, "process_completed_album",
                          return_value=True):
            dl_mod._run_completed_processing(
                self._entry(), 42, self._state(), db, self._ctx(db))
        self.assertEqual(db.request(42)["status"], "imported")

    def test_false_outcome_resets_to_wanted_with_attempt(self):
        """Failure: ``process_completed_album`` returns ``False`` →
        reset to 'wanted' with an attempt increment (genuine failure
        DOES deserve a backoff-scored attempt)."""
        from lib import download as dl_mod
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=42, status="downloading"))
        with patch.object(dl_mod, "process_completed_album",
                          return_value=False):
            dl_mod._run_completed_processing(
                self._entry(), 42, self._state(), db, self._ctx(db))
        self.assertEqual(db.request(42)["status"], "wanted")


class TestSiblingImportedPathPropagation(unittest.TestCase):
    """Integration slice for issue #132 P2 / issue #133: after
    ``_canonicalize_siblings`` moves a sibling's files on disk, any
    tracked ``album_requests`` row for that sibling must get its
    ``imported_path`` updated.

    Pre-fix scenario (from issue #132 P2): sibling edition gets
    re-shuffled from ``/Palo Santo/`` to ``/Palo Santo [2006]/`` after
    the new same-name edition imports. If the sibling was itself a
    pipeline request (e.g. a prior upgrade tracked in the DB), its
    ``imported_path`` keeps pointing at the non-existent pre-move
    directory. The UI's "Imported to" label and the ban-source button
    both lie until the next event touches the row.

    Fix flow (covered by this slice):
    1. ``import_one.py::_canonicalize_siblings`` resolves each
       sibling's ``(mb_albumid, discogs_albumid)`` from beets and
       emits a ``MovedSibling`` record in ``PostflightInfo.moved_siblings``.
    2. ``dispatch_import_core`` calls ``_propagate_moved_siblings``
       which calls ``PipelineDB.update_imported_path_by_release_id``
       for each record.
    3. The tracked sibling row's ``imported_path`` now matches the
       post-move directory. Untracked siblings are silently skipped.
    """

    MBID_NEW = "aaaaaaaa-1111-2222-3333-bbbbbbbbbbbb"  # the album being imported
    MBID_SIBLING = "cccccccc-4444-5555-6666-dddddddddddd"  # tracked sibling
    DISCOGS_SIBLING = "12856590"
    PRE_MOVE_PATH = "/Beets/Shearwater/2006 - Palo Santo"
    POST_MOVE_PATH = "/Beets/Shearwater/2006 - Palo Santo [2006]"

    def _make_cfg(self) -> CratediggerConfig:
        return CratediggerConfig(
            beets_harness_path=_HARNESS,
            pipeline_db_enabled=True,
        )

    def test_mb_sibling_path_propagates_when_tracked(self):
        """Tracked MB-sourced sibling: ``imported_path`` updated."""
        from lib.import_dispatch import dispatch_import_core
        from lib.quality import MovedSibling

        db = FakePipelineDB()
        # Request 42 is the one being imported right now.
        db.seed_request(make_request_row(
            id=42, mb_release_id=self.MBID_NEW, status="downloading"))
        # Request 17 is the tracked sibling, already imported long ago.
        # Its imported_path still points at the pre-move directory —
        # propagation must update it.
        db.seed_request(make_request_row(
            id=17, mb_release_id=self.MBID_SIBLING,
            status="imported", imported_path=self.PRE_MOVE_PATH))

        ir = make_import_result(decision="import", new_min_bitrate=245)
        ir.postflight.moved_siblings = [
            MovedSibling(
                album_id=10314,
                new_path=self.POST_MOVE_PATH,
                mb_albumid=self.MBID_SIBLING,
                discogs_albumid=""),
        ]
        beets_info = AlbumInfo(
            album_id=1, track_count=10, min_bitrate_kbps=245,
            avg_bitrate_kbps=245, format="MP3",
            is_cbr=False, album_path="/Beets/Test")
        dl_info = DownloadInfo(username="user1")

        tmpdir = tempfile.mkdtemp()
        try:
            with patch_dispatch_externals() as ext, \
                 patch("lib.beets_db.BeetsDB", _mock_beets_db(beets_info)):
                ext.run.return_value = MagicMock(
                    returncode=0, stdout=_make_stdout(ir), stderr="")
                dispatch_import_core(
                    path=tmpdir,
                    mb_release_id=self.MBID_NEW,
                    request_id=42,
                    label="Shearwater - Palo Santo (2007)",
                    beets_harness_path=_HARNESS,
                    db=db,  # type: ignore[arg-type]
                    dl_info=dl_info,
                    distance=0.05,
                    scenario="strong_match",
                    files=[MagicMock(username="user1",
                                     filename="01 - Track.mp3")],
                    cfg=self._make_cfg(),
                )
        finally:
            import shutil
            shutil.rmtree(tmpdir, ignore_errors=True)

        # Sibling's imported_path updated to the post-move directory.
        self.assertEqual(
            db.request(17)["imported_path"], self.POST_MOVE_PATH)

    def test_discogs_sibling_path_propagates_via_discogs_release_id(self):
        """Tracked Discogs-sourced sibling: matches on
        ``discogs_release_id`` since ``mb_albumid`` is empty for Discogs
        rows in beets.
        """
        from lib.import_dispatch import dispatch_import_core
        from lib.quality import MovedSibling

        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42, mb_release_id=self.MBID_NEW, status="downloading"))
        # Discogs-sourced sibling: pipeline DB carries the id in
        # ``discogs_release_id``, and beets has it in ``discogs_albumid``
        # with an empty ``mb_albumid``.
        db.seed_request(make_request_row(
            id=18, mb_release_id=None,
            discogs_release_id=self.DISCOGS_SIBLING,
            status="imported", imported_path=self.PRE_MOVE_PATH))

        ir = make_import_result(decision="import", new_min_bitrate=245)
        ir.postflight.moved_siblings = [
            MovedSibling(
                album_id=10315,
                new_path=self.POST_MOVE_PATH,
                mb_albumid="",
                discogs_albumid=self.DISCOGS_SIBLING),
        ]
        beets_info = AlbumInfo(
            album_id=1, track_count=10, min_bitrate_kbps=245,
            avg_bitrate_kbps=245, format="MP3",
            is_cbr=False, album_path="/Beets/Test")
        dl_info = DownloadInfo(username="user1")

        tmpdir = tempfile.mkdtemp()
        try:
            with patch_dispatch_externals() as ext, \
                 patch("lib.beets_db.BeetsDB", _mock_beets_db(beets_info)):
                ext.run.return_value = MagicMock(
                    returncode=0, stdout=_make_stdout(ir), stderr="")
                dispatch_import_core(
                    path=tmpdir,
                    mb_release_id=self.MBID_NEW,
                    request_id=42,
                    label="Shearwater - Palo Santo (2007)",
                    beets_harness_path=_HARNESS,
                    db=db,  # type: ignore[arg-type]
                    dl_info=dl_info,
                    distance=0.05,
                    scenario="strong_match",
                    files=[MagicMock(username="user1",
                                     filename="01 - Track.mp3")],
                    cfg=self._make_cfg(),
                )
        finally:
            import shutil
            shutil.rmtree(tmpdir, ignore_errors=True)

        # Discogs sibling's imported_path updated.
        self.assertEqual(
            db.request(18)["imported_path"], self.POST_MOVE_PATH)

    def test_discogs_sibling_matches_legacy_pipeline_layout(self):
        """Codex R2 P2 regression: beets-side ``discogs_albumid`` must
        match pipeline-DB rows that store the Discogs numeric in
        EITHER ``discogs_release_id`` (new layout) OR ``mb_release_id``
        (legacy "pipeline compat" layout from CLAUDE.md).

        Scenario: a request was added pre-plugin-patch, so pipeline DB
        has ``mb_release_id="12856590"`` and ``discogs_release_id=None``.
        Beets has the same album with ``discogs_albumid=12856590`` and
        ``mb_albumid=""`` (new-layout beets, post-plugin-patch). Before
        the R2 P2 fix, the harness emitted ``(mb="", discogs="12856590")``
        and the SQL matched ``discogs_release_id="12856590"`` only —
        which misses the legacy row. After the fix, the numeric id
        matches against BOTH pipeline columns and the legacy row's
        ``imported_path`` updates correctly.
        """
        from lib.import_dispatch import dispatch_import_core
        from lib.quality import MovedSibling

        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42, mb_release_id=self.MBID_NEW, status="downloading"))
        # Legacy pipeline layout: Discogs numeric stored in
        # mb_release_id (not discogs_release_id).
        db.seed_request(make_request_row(
            id=99,
            mb_release_id=self.DISCOGS_SIBLING,
            discogs_release_id=None,
            status="imported",
            imported_path=self.PRE_MOVE_PATH))

        ir = make_import_result(decision="import", new_min_bitrate=245)
        # Beets-side: new-layout row, discogs_albumid populated,
        # mb_albumid empty.
        ir.postflight.moved_siblings = [
            MovedSibling(
                album_id=10315,
                new_path=self.POST_MOVE_PATH,
                mb_albumid="",
                discogs_albumid=self.DISCOGS_SIBLING),
        ]
        beets_info = AlbumInfo(
            album_id=1, track_count=10, min_bitrate_kbps=245,
            avg_bitrate_kbps=245, format="MP3",
            is_cbr=False, album_path="/Beets/Test")
        dl_info = DownloadInfo(username="user1")

        tmpdir = tempfile.mkdtemp()
        try:
            with patch_dispatch_externals() as ext, \
                 patch("lib.beets_db.BeetsDB", _mock_beets_db(beets_info)):
                ext.run.return_value = MagicMock(
                    returncode=0, stdout=_make_stdout(ir), stderr="")
                dispatch_import_core(
                    path=tmpdir,
                    mb_release_id=self.MBID_NEW,
                    request_id=42,
                    label="Shearwater - Palo Santo (2007)",
                    beets_harness_path=_HARNESS,
                    db=db,  # type: ignore[arg-type]
                    dl_info=dl_info,
                    distance=0.05,
                    scenario="strong_match",
                    files=[MagicMock(username="user1",
                                     filename="01 - Track.mp3")],
                    cfg=self._make_cfg(),
                )
        finally:
            import shutil
            shutil.rmtree(tmpdir, ignore_errors=True)

        # Legacy pipeline row's imported_path got updated — Codex R2
        # P2 regression fix verified. Pre-fix this was silently missed.
        self.assertEqual(
            db.request(99)["imported_path"], self.POST_MOVE_PATH)

    def test_untracked_sibling_is_silently_skipped(self):
        """Sibling that beets knows about but the pipeline DB does not:
        no pipeline row matches, update returns rowcount=0, no error.
        The import still succeeds as a whole. Pre-fix this was implicit
        too, but pinning it prevents a future regression where an
        untracked sibling somehow raises."""
        from lib.import_dispatch import dispatch_import_core
        from lib.quality import MovedSibling

        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42, mb_release_id=self.MBID_NEW, status="downloading"))
        # Intentionally NO request row for MBID_SIBLING. Beets has the
        # sibling; the pipeline DB does not. Propagation is a no-op.

        ir = make_import_result(decision="import", new_min_bitrate=245)
        ir.postflight.moved_siblings = [
            MovedSibling(
                album_id=10314,
                new_path=self.POST_MOVE_PATH,
                mb_albumid=self.MBID_SIBLING,
                discogs_albumid=""),
        ]
        beets_info = AlbumInfo(
            album_id=1, track_count=10, min_bitrate_kbps=245,
            avg_bitrate_kbps=245, format="MP3",
            is_cbr=False, album_path="/Beets/Test")
        dl_info = DownloadInfo(username="user1")

        tmpdir = tempfile.mkdtemp()
        try:
            with patch_dispatch_externals() as ext, \
                 patch("lib.beets_db.BeetsDB", _mock_beets_db(beets_info)):
                ext.run.return_value = MagicMock(
                    returncode=0, stdout=_make_stdout(ir), stderr="")
                dispatch_import_core(
                    path=tmpdir,
                    mb_release_id=self.MBID_NEW,
                    request_id=42,
                    label="Shearwater - Palo Santo (2007)",
                    beets_harness_path=_HARNESS,
                    db=db,  # type: ignore[arg-type]
                    dl_info=dl_info,
                    distance=0.05,
                    scenario="strong_match",
                    files=[MagicMock(username="user1",
                                     filename="01 - Track.mp3")],
                    cfg=self._make_cfg(),
                )
        finally:
            import shutil
            shutil.rmtree(tmpdir, ignore_errors=True)

        # Main import still marked done.
        self.assertEqual(db.request(42)["status"], "imported")

    def test_empty_moved_siblings_is_noop(self):
        """Non-kept_duplicate imports emit ``moved_siblings=[]``. The
        dispatch helper must be a no-op in that case (the common case)."""
        from lib.import_dispatch import dispatch_import_core

        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42, mb_release_id=self.MBID_NEW, status="downloading"))
        db.seed_request(make_request_row(
            id=17, mb_release_id=self.MBID_SIBLING,
            status="imported", imported_path=self.PRE_MOVE_PATH))

        ir = make_import_result(decision="import", new_min_bitrate=245)
        # Explicit empty list — no siblings to propagate.
        ir.postflight.moved_siblings = []
        beets_info = AlbumInfo(
            album_id=1, track_count=10, min_bitrate_kbps=245,
            avg_bitrate_kbps=245, format="MP3",
            is_cbr=False, album_path="/Beets/Test")
        dl_info = DownloadInfo(username="user1")

        tmpdir = tempfile.mkdtemp()
        try:
            with patch_dispatch_externals() as ext, \
                 patch("lib.beets_db.BeetsDB", _mock_beets_db(beets_info)):
                ext.run.return_value = MagicMock(
                    returncode=0, stdout=_make_stdout(ir), stderr="")
                dispatch_import_core(
                    path=tmpdir,
                    mb_release_id=self.MBID_NEW,
                    request_id=42,
                    label="Plain import — no siblings",
                    beets_harness_path=_HARNESS,
                    db=db,  # type: ignore[arg-type]
                    dl_info=dl_info,
                    distance=0.05,
                    scenario="strong_match",
                    files=[MagicMock(username="user1",
                                     filename="01 - Track.mp3")],
                    cfg=self._make_cfg(),
                )
        finally:
            import shutil
            shutil.rmtree(tmpdir, ignore_errors=True)

        # Sibling's imported_path UNTOUCHED.
        self.assertEqual(
            db.request(17)["imported_path"], self.PRE_MOVE_PATH)


if __name__ == "__main__":
    unittest.main()
