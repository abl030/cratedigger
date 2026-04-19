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
from lib.config import SoularrConfig
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
            cfg = SoularrConfig(
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

        custom_cfg = SoularrConfig(
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

        custom_cfg = SoularrConfig(
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
        from lib.config import SoularrConfig
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
        cfg = SoularrConfig(audio_check_mode="off")

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
            with self.assertLogs("soularr", level="WARNING") as logs:
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
        from lib.config import SoularrConfig
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
        cfg = SoularrConfig(audio_check_mode="off")

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
        from lib.config import SoularrConfig
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
        cfg = SoularrConfig(audio_check_mode="off")

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
        from lib.config import SoularrConfig
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
        cfg = SoularrConfig(audio_check_mode="off")

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

        cfg = SoularrConfig(
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

        cfg = SoularrConfig(
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

        cfg = SoularrConfig(
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
        cfg = SoularrConfig(
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


if __name__ == "__main__":
    unittest.main()
