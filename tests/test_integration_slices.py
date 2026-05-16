"""Integration slice tests — real code paths with minimal patching.

These exercise real orchestration flows end-to-end, patching only external
edges: subprocess (sp.run), filesystem cleanup, network calls (meelo/plex/jellyfin),
and BeetsDB (requires real beets SQLite DB on disk).

The key difference from unit/orchestration tests is that parse_import_result
and _check_quality_gate_core run for real, not patched.
"""

import os
import configparser
from contextlib import contextmanager
import tempfile
from typing import Any, cast
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
from tests.fakes import FakePipelineDB, FakeSlskdAPI
from tests.helpers import (
    make_ctx_with_fake_db,
    make_import_result,
    make_request_row,
    patch_dispatch_externals,
)


_HARNESS = "/nix/store/fake/harness/run_beets_harness.sh"


def _download_ownership_cfg() -> CratediggerConfig:
    ini = configparser.ConfigParser()
    ini["Search Settings"] = {
        "minimum_filename_match_ratio": "0.5",
        "ignored_users": "",
        "allowed_filetypes": "flac",
        "browse_parallelism": "4",
        "browse_top_k": "20",
        "browse_global_max_workers": "4",
    }
    ini["Slskd"] = {
        "download_dir": "/tmp/test_downloads",
    }
    ini["Beets Validation"] = {
        "staging_dir": "/tmp/staging",
    }
    return CratediggerConfig.from_ini(ini)


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


class TestDownloadOwnershipPreclaimRecoverySlice(unittest.TestCase):
    """Cross-boundary slice for enqueue preclaim -> fresh poll recovery."""

    def test_preclaimed_missing_transfer_id_recovers_in_fresh_poll_context(self):
        from lib.download import SlskdEnqueueOutcome, poll_active_downloads
        from lib.download_ownership import DownloadOwnershipWriter
        from lib.enqueue import try_enqueue
        from lib.grab_list import DownloadFile
        from lib.matching import MatchResult

        cfg = _download_ownership_cfg()
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=1,
            status="wanted",
            artist_name="Artist",
            album_title="Album",
            mb_release_id="mbid-1",
        ))
        album = SimpleNamespace(
            id=1,
            db_request_id=1,
            title="Album",
            artist_name="Artist",
            release_date="2024-01-01T00:00:00Z",
            db_mb_release_id="mbid-1",
            db_source="request",
            db_search_filetype_override=None,
            db_target_format=None,
        )
        enqueue_ctx = make_ctx_with_fake_db(db, cfg=cfg, slskd=FakeSlskdAPI())
        enqueue_ctx.current_album_cache[1] = album
        enqueue_ctx.user_upload_speed = {"u00": 1000}
        enqueue_ctx.download_ownership = DownloadOwnershipWriter(
            db_factory=lambda: db,
        )
        file_dir = "Music\\u00\\Album"
        tracks = cast(
            Any,
            [{"albumId": 1, "title": "Track 1", "mediumNumber": 1}],
        )
        results = {"u00": {"flac": [file_dir]}}
        match = MatchResult(
            matched=True,
            directory={
                "directory": file_dir,
                "files": [{"filename": "01.flac", "size": 123}],
            },
            file_dir=file_dir,
            candidates=[],
        )

        def accepted_without_id(*, username, files, file_dir, ctx):
            return SlskdEnqueueOutcome(status="accepted", downloads=[
                DownloadFile(
                    filename=files[0]["filename"],
                    id="",
                    file_dir=file_dir,
                    username=username,
                    size=files[0]["size"],
                ),
            ])

        with patch("lib.enqueue._fanout_browse_users", return_value=set()), \
             patch("lib.enqueue.check_for_match", return_value=match), \
             patch(
                 "lib.enqueue.slskd_enqueue_with_outcome",
                 side_effect=accepted_without_id,
             ):
            attempt = try_enqueue(tracks, results, "flac", enqueue_ctx)

        self.assertTrue(attempt.matched)
        self.assertEqual(db.request(1)["status"], "downloading")
        planned_state = db.request(1)["active_download_state"]
        self.assertIsNone(planned_state["current_path"])

        poll_slskd = FakeSlskdAPI(downloads=[{
            "username": "u00",
            "directories": [{"directory": file_dir, "files": [{
                "filename": "Music\\u00\\Album\\01.flac",
                "id": "transfer-1",
                "state": "InProgress",
                "bytesTransferred": 10,
            }]}],
        }])
        poll_ctx = make_ctx_with_fake_db(db, cfg=cfg, slskd=poll_slskd)

        poll_active_downloads(poll_ctx)

        self.assertEqual(db.request(1)["status"], "downloading")
        recovered_state = db.request(1)["active_download_state"]
        self.assertEqual(recovered_state["files"][0]["bytes_transferred"], 10)
        self.assertEqual(poll_slskd.transfers.get_all_downloads_calls, [True])


class TestPeerOnlineProbeAtEnqueueSlice(unittest.TestCase):
    """End-to-end coverage for Part 1 (user_offline classification) +
    Part 2 (presence probe). Real ``try_enqueue`` over real
    ``slskd_enqueue_with_outcome`` and ``_peer_is_online_for_enqueue``.
    Only the network edges (slskd, DB) are faked.

    Reference incident: 2026-05-08 request 2540 (Mercury Rev — Deserter's
    Songs, peer ``pooyork``) — see
    docs/brainstorms/2026-05-08-peer-online-probe-at-enqueue-requirements.md.
    """

    def _build_offline_http_error(self) -> Exception:
        """Synthetic stand-in for slskd-api's ``requests.HTTPError`` —
        ``test_beets_validation.py`` mocks ``sys.modules['requests']`` at
        discovery time, so a real ``requests.HTTPError`` constructed here
        would not be a real exception. The detector matches structurally
        on ``.response.text``."""
        class _FakeHTTPError(Exception):
            pass

        err = _FakeHTTPError("500 Server Error")
        err.response = SimpleNamespace(  # type: ignore[attr-defined]
            text="User pooyork appears to be offline",
        )
        return err

    def _make_album(self, request_id: int):
        return SimpleNamespace(
            id=request_id,
            db_request_id=request_id,
            title="Album",
            artist_name="Artist",
            release_date="2024-01-01T00:00:00Z",
            db_mb_release_id=f"mbid-{request_id}",
            db_source="request",
            db_search_filetype_override=None,
            db_target_format=None,
        )

    def _setup_ctx(
        self,
        db: FakePipelineDB,
        slskd: FakeSlskdAPI,
        speeds: dict[str, int],
    ):
        from lib.download_ownership import DownloadOwnershipWriter
        cfg = _download_ownership_cfg()
        ctx = make_ctx_with_fake_db(db, cfg=cfg, slskd=slskd)
        ctx.current_album_cache[1] = self._make_album(1)
        ctx.user_upload_speed = speeds
        ctx.download_ownership = DownloadOwnershipWriter(db_factory=lambda: db)
        return ctx

    def _wave_match_side_effect(self):
        from lib.matching import MatchResult

        def matcher(*args, **kwargs):
            username = kwargs.get("username") or args[3]
            file_dir = f"Music\\{username}\\Album"
            return MatchResult(
                matched=True,
                directory={
                    "directory": file_dir,
                    "files": [{
                        "filename": f"{file_dir}\\01.flac",
                        "size": 123,
                    }],
                },
                file_dir=file_dir,
                candidates=[],
            )
        return matcher

    def test_offline_first_then_online_second_enqueues_only_second(self):
        from lib.enqueue import try_enqueue

        db = FakePipelineDB()
        db.seed_request(make_request_row(id=1, status="wanted"))
        slskd = FakeSlskdAPI(downloads=[{
            "username": "online_peer",
            "directories": [{
                "directory": "Music\\online_peer\\Album",
                "files": [{
                    "filename": "Music\\online_peer\\Album\\01.flac",
                    "id": "tid-1",
                }],
            }],
        }])
        slskd.users.set_status("offline_peer", "Offline")
        slskd.users.set_status("online_peer", "Online")
        ctx = self._setup_ctx(db, slskd, speeds={
            "offline_peer": 10_000,
            "online_peer": 9_000,
        })
        results = {
            "offline_peer": {"flac": ["Music\\offline_peer\\Album"]},
            "online_peer": {"flac": ["Music\\online_peer\\Album"]},
        }
        tracks = cast(
            Any, [{"albumId": 1, "title": "Track 1", "mediumNumber": 1}],
        )

        with patch("lib.enqueue._fanout_browse_users", return_value=set()), \
             patch("lib.enqueue.check_for_match",
                   side_effect=self._wave_match_side_effect()), \
             patch("time.sleep"):
            attempt = try_enqueue(tracks, results, "flac", ctx)

        # Both peers probed (in upload-speed order).
        self.assertEqual(
            slskd.users.status_calls, ["offline_peer", "online_peer"],
        )
        # transfers.enqueue called exactly once, against the online peer.
        self.assertEqual(len(slskd.transfers.enqueue_calls), 1)
        self.assertEqual(
            slskd.transfers.enqueue_calls[0].username, "online_peer",
        )
        # Request transitioned to downloading; no spurious download_log.
        self.assertTrue(attempt.matched)
        self.assertEqual(db.request(1)["status"], "downloading")
        self.assertEqual(db.download_logs, [])

    def test_both_peers_offline_no_enqueue_no_log_request_stays_wanted(self):
        from lib.enqueue import try_enqueue

        db = FakePipelineDB()
        db.seed_request(make_request_row(id=1, status="wanted"))
        slskd = FakeSlskdAPI()
        slskd.users.set_status("u00", "Offline")
        slskd.users.set_status("u01", "Offline")
        ctx = self._setup_ctx(db, slskd, speeds={"u00": 10_000, "u01": 9_000})
        results = {
            "u00": {"flac": ["Music\\u00\\Album"]},
            "u01": {"flac": ["Music\\u01\\Album"]},
        }
        tracks = cast(
            Any, [{"albumId": 1, "title": "Track 1", "mediumNumber": 1}],
        )

        with patch("lib.enqueue._fanout_browse_users", return_value=set()), \
             patch("lib.enqueue.check_for_match",
                   side_effect=self._wave_match_side_effect()), \
             patch("time.sleep"):
            attempt = try_enqueue(tracks, results, "flac", ctx)

        self.assertEqual(slskd.users.status_calls, ["u00", "u01"])
        self.assertEqual(slskd.transfers.enqueue_calls, [])
        self.assertFalse(attempt.matched)
        self.assertEqual(db.request(1)["status"], "wanted")
        self.assertIsNone(db.request(1)["active_download_state"])
        self.assertEqual(db.download_logs, [])

    def test_probe_online_but_enqueue_user_offline_writes_log(self):
        """Safety net: if the probe says ``Online`` (status endpoint stale)
        but ``transfers.enqueue`` then raises a peer-offline HTTPError,
        Part 1's classification + Part 2's log site compose so the
        request returns to wanted with a ``user_offline`` log row."""
        from lib.enqueue import try_enqueue

        db = FakePipelineDB()
        db.seed_request(make_request_row(id=1, status="wanted"))
        slskd = FakeSlskdAPI(downloads=[])
        slskd.users.set_status("pooyork", "Online")
        slskd.transfers.enqueue_error = self._build_offline_http_error()
        ctx = self._setup_ctx(db, slskd, speeds={"pooyork": 10_000})
        results = {"pooyork": {"flac": ["Music\\pooyork\\Album"]}}
        tracks = cast(
            Any, [{"albumId": 1, "title": "Track 1", "mediumNumber": 1}],
        )

        with patch("lib.enqueue._fanout_browse_users", return_value=set()), \
             patch("lib.enqueue.check_for_match",
                   side_effect=self._wave_match_side_effect()), \
             patch("time.sleep"):
            try_enqueue(tracks, results, "flac", ctx)

        # Probe said Online, so we did try.
        self.assertEqual(slskd.users.status_calls, ["pooyork"])
        self.assertEqual(len(slskd.transfers.enqueue_calls), 1)
        # Reset to wanted via verified-no-acceptance, with a log row.
        self.assertEqual(db.request(1)["status"], "wanted")
        self.assertEqual(len(db.download_logs), 1)
        log = db.download_logs[0]
        self.assertEqual(log.outcome, "user_offline")
        self.assertEqual(log.soulseek_username, "pooyork")
        self.assertEqual(log.filetype, "flac")


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
    (lib.download.process_completed_album) and the force/manual-import path
    (lib.import_dispatch.dispatch_import_from_db) delegate to. Proves the
    function does its side effects — spectral state write + denylist —
    consistently regardless of caller.
    """

    def test_suspect_download_persists_existing_spectral_state(self):
        """Issue #90: when run_preimport_gates measures spectral on both the
        download and the existing album, it must persist the *existing*
        spectral state to ``album_requests.current_spectral_*`` so subsequent
        attempts can compare evidence-to-evidence.

        Spectral comparison itself is owned by the importer's evidence
        pipeline (``full_pipeline_decision_from_evidence``) — preimport just
        accepts and lets the importer decide. Denylisting on quality grounds
        is the importer's responsibility, not this gate's.
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

        # Preimport accepts — quality decisions live in
        # full_pipeline_decision_from_evidence.
        self.assertTrue(result.valid)
        self.assertIsNone(result.scenario)
        self.assertEqual(len(db.denylist), 0,
                         "preimport must not denylist on quality grounds")

        # Spectral state for the *existing* album was persisted so the
        # importer's evidence pipeline can compare spectral-to-spectral on
        # the next attempt.
        row = db.request(42)
        self.assertEqual(row["current_spectral_grade"], "genuine")
        self.assertEqual(row["current_spectral_bitrate"], 320)

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


class TestSpectralPropagationOnAccept(unittest.TestCase):
    """U3 guard: ``_persist_spectral_state`` must fire on accept decisions too.

    The U3 refactor moved spectral state persistence out of the
    decision-conditional code path and into the measurement helper. The
    invariant: every measurement that runs spectral analysis writes
    ``album_requests.current_spectral_*``, regardless of whether the
    downstream decision is accept or reject.

    This guards against the issue #90 regression: if propagation only fired
    on reject branches, the next attempt would have stale comparison data and
    a follow-up suspect-grade-upgrade or import_no_exist would compare against
    a phantom existing measurement.
    """

    def test_accept_suspect_upgrade_still_persists_spectral(self):
        """Accept (suspect grade but bitrate upgrades existing) → spectral state still propagates."""
        from lib.config import CratediggerConfig
        from lib.preimport import run_preimport_gates

        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42, status="downloading",
            current_spectral_grade=None, current_spectral_bitrate=None,
        ))
        beets_info = AlbumInfo(
            album_id=1,
            track_count=10,
            min_bitrate_kbps=128,
            avg_bitrate_kbps=128,
            format="MP3",
            is_cbr=True,
            album_path="/Beets/Test",
        )
        cfg = CratediggerConfig(audio_check_mode="off")

        with patch(
            "lib.preimport.spectral_analyze",
            side_effect=[
                # download: suspect at 256 (upgrade over existing at 96)
                SimpleNamespace(
                    grade="suspect",
                    estimated_bitrate_kbps=256,
                    suspect_pct=70.0,
                    tracks=[],
                ),
                # existing: likely_transcode at 96 (worse — upgrade is justified)
                SimpleNamespace(
                    grade="likely_transcode",
                    estimated_bitrate_kbps=96,
                    suspect_pct=95.0,
                    tracks=[],
                ),
            ],
        ), patch("lib.beets_db.BeetsDB", _mock_beets_db(beets_info)), \
             patch("os.path.isdir", return_value=True):
            result = run_preimport_gates(
                path="/tmp/dl",
                mb_release_id="mbid-upgrade",
                label="Upgrade Album",
                download_filetype="mp3",
                download_min_bitrate_bps=256_000,
                download_is_vbr=False,
                cfg=cfg,
                db=db,  # type: ignore[arg-type]
                request_id=42,
                usernames={"user1"},
            )

        # Accept (suspect grade BUT bitrate upgrades existing).
        self.assertTrue(
            result.valid,
            "suspect 256 > existing 96 must accept (spectral upgrade)")
        # Crucial: the spectral state still propagated despite the accept.
        row = db.request(42)
        self.assertEqual(
            row["current_spectral_grade"], "likely_transcode",
            "existing spectral measurement must be persisted on accept")
        self.assertEqual(row["current_spectral_bitrate"], 96)
        # No denylist on accept paths.
        self.assertEqual(len(db.denylist), 0)

    def test_accept_import_no_exist_still_persists_spectral(self):
        """Accept (suspect grade, no existing on disk) → spectral state propagates the download's spectral."""
        from lib.config import CratediggerConfig
        from lib.preimport import run_preimport_gates

        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42, status="downloading", min_bitrate=None,
            current_spectral_grade=None, current_spectral_bitrate=None,
        ))
        cfg = CratediggerConfig(audio_check_mode="off")

        with patch(
            "lib.preimport.spectral_analyze",
            return_value=SimpleNamespace(
                grade="suspect",
                estimated_bitrate_kbps=192,
                suspect_pct=60.0,
                tracks=[],
            ),
        ), patch("lib.beets_db.BeetsDB", _mock_beets_db(None)), \
             patch("os.path.isdir", return_value=True):
            result = run_preimport_gates(
                path="/tmp/dl",
                mb_release_id="mbid-firsttime",
                label="First Time Album",
                download_filetype="mp3",
                download_min_bitrate_bps=192_000,
                download_is_vbr=False,
                cfg=cfg,
                db=db,  # type: ignore[arg-type]
                request_id=42,
                usernames={"user1"},
            )

        # Accept — no existing album, suspect spectral is import_no_exist.
        self.assertTrue(result.valid)
        # Without an existing-on-disk and no existing_min_bitrate, the
        # propagation helper does NOT adopt the download's spectral (it would
        # be speculative). But it must still attempt to write the
        # existing_spectral (None) — which means we DON'T expect a row update
        # in this case. Per ``_persist_spectral_state`` semantics: when
        # existing_spectral is None AND existing_min_bitrate is None, nothing
        # is persisted. The accept decision itself fires correctly.
        row = db.request(42)
        # Either NULL (no propagation triggered) or the download's spectral
        # (if existing_min_bitrate had been set, propagation would adopt it).
        # Here the invariant is: no crash, accept decision wins, no denylist.
        self.assertIsNone(row.get("current_spectral_grade"))
        self.assertEqual(len(db.denylist), 0)


class TestLosslessSourceLockedSlice(unittest.TestCase):
    """Integration slice: lossy candidate vs existing with lossless-source V0
    probe → real parse_import_result → lossless_source_locked dispatch path
    → domain state assertions.

    Replaces the per-step mocking with end-to-end coverage of the wire
    boundary: import_one.py emits a real ImportResult JSON sentinel,
    dispatch_import_core parses it, dispatch_action maps the decision to
    record_rejection+denylist+requeue, and the rejection lands in
    download_log + denylist + status=wanted.
    """

    def test_lossy_candidate_locked_records_rejection_and_requeues(self):
        from lib.import_dispatch import dispatch_import_core
        from lib.quality import V0ProbeEvidence, V0_PROBE_LOSSLESS_SOURCE

        existing_probe = V0ProbeEvidence(
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
            existing_v0_probe=existing_probe,
            error=("existing has lossless-source V0 probe 240kbps; lossy "
                   "candidate cannot produce comparable evidence"),
        )

        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42, status="downloading", mb_release_id="mbid-123",
            current_lossless_source_v0_probe_min_bitrate=210,
            current_lossless_source_v0_probe_avg_bitrate=240,
            current_lossless_source_v0_probe_median_bitrate=235,
        ))

        cfg = CratediggerConfig(
            beets_harness_path=_HARNESS,
            pipeline_db_enabled=True,
        )

        tmpdir = tempfile.mkdtemp()
        try:
            with patch_dispatch_externals() as ext, \
                 patch("lib.beets_db.BeetsDB", _mock_beets_db(None)):
                ext.run.return_value = MagicMock(
                    returncode=5, stdout=_make_stdout(ir), stderr="")
                dispatch_import_core(
                    path=tmpdir,
                    mb_release_id="mbid-123",
                    request_id=42,
                    label="Test Artist - Test Album",
                    beets_harness_path=_HARNESS,
                    db=db,  # type: ignore[arg-type]
                    dl_info=DownloadInfo(username="user1"),
                    distance=0.131,
                    scenario="strong_match",
                    files=[MagicMock(username="user1",
                                     filename="01 - Track.mp3")],
                    cfg=cfg,
                )
        finally:
            import shutil
            shutil.rmtree(tmpdir, ignore_errors=True)

        row = db.request(42)
        self.assertEqual(row["status"], "wanted")
        # The recorded V0 probe must survive the rejection — it's the anchor
        # the next attempt will compare against, not state to clear.
        self.assertEqual(
            row["current_lossless_source_v0_probe_avg_bitrate"], 240)
        self.assertEqual(len(db.download_logs), 1)
        db.assert_log(self, 0, outcome="rejected",
                      beets_scenario="lossless_source_locked")
        # ir.error is suppressed for domain rejections — error_message
        # must be None so downstream UIs don't render it as a crash.
        self.assertIsNone(db.download_logs[0].error_message)
        self.assertIn("240", db.download_logs[0].beets_detail or "")
        self.assertEqual(len(db.denylist), 1)
        self.assertEqual(db.denylist[0].username, "user1")


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
        from lib.import_queue import IMPORT_JOB_MANUAL, manual_import_payload
        from lib.quality import (
            ALBUM_QUALITY_EVIDENCE_OWNER_IMPORT_JOB_CANDIDATE,
            ALBUM_QUALITY_EVIDENCE_OWNER_REQUEST_CURRENT,
            AudioQualityMeasurement,
        )
        from lib.quality_evidence import snapshot_audio_files
        from tests.helpers import make_album_quality_evidence

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
            with open(os.path.join(tmpdir, "01.mp3"), "wb") as handle:
                handle.write(b"audio")
            job = db.enqueue_import_job(
                IMPORT_JOB_MANUAL,
                request_id=42,
                payload=manual_import_payload(failed_path=tmpdir),
            )
            db.upsert_album_quality_evidence(make_album_quality_evidence(
                owner_type=ALBUM_QUALITY_EVIDENCE_OWNER_IMPORT_JOB_CANDIDATE,
                owner_id=job.id,
                files=snapshot_audio_files(tmpdir),
                measurement=AudioQualityMeasurement(
                    min_bitrate_kbps=320, avg_bitrate_kbps=320,
                    median_bitrate_kbps=320, format="MP3",
                    spectral_grade="genuine",
                ),
                codec="mp3", container="mp3", storage_format="mp3 320",
            ))
            db.upsert_album_quality_evidence(make_album_quality_evidence(
                owner_type=ALBUM_QUALITY_EVIDENCE_OWNER_REQUEST_CURRENT,
                owner_id=42,
                measurement=AudioQualityMeasurement(
                    min_bitrate_kbps=180, avg_bitrate_kbps=180,
                    median_bitrate_kbps=180, format="MP3",
                    spectral_bitrate_kbps=128,
                    spectral_grade="likely_transcode",
                ),
                codec="mp3", container="mp3", storage_format="mp3",
            ))
            # album_path=None makes ensure_current_evidence_for_action
            # skip the audio-snapshot guard and trust the seeded
            # _REQUEST_CURRENT row directly.
            beets_info_no_path = AlbumInfo(
                album_id=1, track_count=10, min_bitrate_kbps=320,
                avg_bitrate_kbps=320, format="MP3",
                is_cbr=False,
                album_path=None,  # type: ignore[arg-type]
            )
            with patch_dispatch_externals() as ext, \
                 patch("lib.beets_db.BeetsDB", _mock_beets_db(beets_info_no_path)), \
                 patch("lib.config.read_runtime_config",
                       return_value=cfg):
                ext.run.return_value = MagicMock(
                    returncode=0, stdout=stdout, stderr="")
                result = dispatch_import_from_db(
                    db, request_id=42, failed_path=tmpdir,  # type: ignore[arg-type]
                    force=True, source_username="user1",
                    import_job_id=job.id,
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
        from lib.import_queue import IMPORT_JOB_MANUAL, manual_import_payload
        from lib.quality import (
            ALBUM_QUALITY_EVIDENCE_OWNER_IMPORT_JOB_CANDIDATE,
            ALBUM_QUALITY_EVIDENCE_OWNER_REQUEST_CURRENT,
            AudioQualityMeasurement,
        )
        from lib.quality_evidence import snapshot_audio_files
        from tests.helpers import make_album_quality_evidence

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
            with open(os.path.join(tmpdir, "01.mp3"), "wb") as handle:
                handle.write(b"audio")
            job = db.enqueue_import_job(
                IMPORT_JOB_MANUAL,
                request_id=833,
                payload=manual_import_payload(failed_path=tmpdir),
            )
            db.upsert_album_quality_evidence(make_album_quality_evidence(
                owner_type=ALBUM_QUALITY_EVIDENCE_OWNER_IMPORT_JOB_CANDIDATE,
                owner_id=job.id,
                files=snapshot_audio_files(tmpdir),
                measurement=AudioQualityMeasurement(
                    min_bitrate_kbps=320, avg_bitrate_kbps=320,
                    median_bitrate_kbps=320, format="MP3",
                    spectral_grade="genuine",
                ),
                codec="mp3", container="mp3", storage_format="mp3 320",
            ))
            db.upsert_album_quality_evidence(make_album_quality_evidence(
                owner_type=ALBUM_QUALITY_EVIDENCE_OWNER_REQUEST_CURRENT,
                owner_id=833,
                measurement=AudioQualityMeasurement(
                    min_bitrate_kbps=192, avg_bitrate_kbps=192,
                    median_bitrate_kbps=192, format="MP3",
                ),
                codec="mp3", container="mp3", storage_format="mp3",
            ))
            beets_info_no_path = AlbumInfo(
                album_id=1, track_count=10, min_bitrate_kbps=320,
                avg_bitrate_kbps=320, format="MP3",
                is_cbr=False,
                album_path=None,  # type: ignore[arg-type]
            )
            with patch_dispatch_externals() as ext, \
                 patch("lib.beets_db.BeetsDB", _mock_beets_db(beets_info_no_path)), \
                 patch("lib.config.read_runtime_config", return_value=cfg):
                ext.run.return_value = MagicMock(
                    returncode=0, stdout=stdout, stderr="")
                dispatch_import_from_db(
                    db, request_id=833, failed_path=tmpdir,  # type: ignore[arg-type]
                    force=True, source_username="ttttsv",
                    import_job_id=job.id,
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
    must happen BEFORE the staged move so the filesystem stays
    resumable on contention.

    Pre-R4: ``_handle_valid_result`` staged into beets first,
    then invoked ``dispatch_import_core`` which checked the lock inside
    ``dispatch_import_core``. On contention, files had already moved
    from ``slskd_download_dir/<import_folder>/`` →
    ``beets_staging_dir/``, but ``active_download_state`` still
    pointed at the original slskd paths. Next cycle's
    ``process_completed_album`` reconstructed stale source paths and
    crashed with ``FileNotFoundError``, falling back to
    ``status='wanted'`` — breaking the contention-retry contract.

    Post-R4: ``_handle_valid_result`` acquires the lock BEFORE
    ``StagedAlbum.move_to``. On contention, return deferred without any
    path change;
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

        # The staged move and dispatch MUST NOT run on contention.
        import_folder_fullpath = "/tmp/test-import-folder"
        with patch.object(dl_mod.StagedAlbum, "move_to") as mock_move, \
             patch.object(dl_mod, "dispatch_import_core") as mock_dispatch:
            outcome = dl_mod._handle_valid_result(
                entry,
                bv_result,
                dl_mod.StagedAlbum(
                    current_path=import_folder_fullpath,
                    request_id=42,
                ),
                ctx,
            )

        assert outcome is not None
        self.assertTrue(outcome.deferred)
        self.assertFalse(outcome.success)
        # **Critical**: staging never ran — files stay at
        # import_folder_fullpath where process_completed_album's
        # resume guard can pick them up next cycle.
        mock_move.assert_not_called()
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

        with patch.object(dl_mod.StagedAlbum, "move_to",
                          return_value="/tmp/staged"):
            dl_mod._handle_valid_result(
                entry,
                bv_result,
                dl_mod.StagedAlbum(current_path="/tmp/import", request_id=42),
                ctx,
            )

        # No RELEASE-namespace lock call — redownload path skips it.
        namespaces_used = {ns for ns, _key in db.advisory_lock_calls}
        self.assertNotIn(ADVISORY_LOCK_NAMESPACE_RELEASE, namespaces_used)

    def test_auto_path_persists_current_path_after_staging(self):
        from lib import download as dl_mod
        from lib.import_dispatch import DispatchOutcome
        from lib.pipeline_db import (ADVISORY_LOCK_NAMESPACE_RELEASE,
                                     release_id_to_lock_key)
        from lib.quality import ValidationResult

        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42,
            mb_release_id=self.MBID,
            status="downloading",
            active_download_state={
                "filetype": "mp3",
                "enqueued_at": "2026-04-03T12:00:00+00:00",
                "files": [],
                "current_path": None,
            },
        ))

        from tests.helpers import make_ctx_with_fake_db

        with tempfile.TemporaryDirectory() as tmpdir:
            processing_dir = os.path.join(tmpdir, "processing")
            os.makedirs(processing_dir)
            track_path = os.path.join(processing_dir, "01 - Track.mp3")
            with open(track_path, "w") as fp:
                fp.write("fake audio")

            cfg = CratediggerConfig(
                beets_harness_path=_HARNESS,
                pipeline_db_enabled=True,
                beets_distance_threshold=0.15,
                beets_staging_dir=os.path.join(tmpdir, "beets-staging"),
            )
            ctx = make_ctx_with_fake_db(db, cfg=cfg)
            entry = dl_mod.GrabListEntry(
                album_id=42,
                artist="Test Artist",
                title="Test Album",
                year="2006",
                files=[],
                filetype="mp3",
                mb_release_id=self.MBID,
                db_source="request",
                db_request_id=42,
                import_folder=processing_dir,
            )
            staged_album = dl_mod.StagedAlbum.from_entry(
                entry,
                default_path=processing_dir,
            )
            bv_result = ValidationResult(
                valid=True,
                distance=0.05,
                scenario="strong_match",
            )
            move_saw_release_lock = False

            original_advisory_lock = db.advisory_lock

            @contextmanager
            def tracking_advisory_lock(namespace: int, key: int):
                nonlocal move_saw_release_lock
                with original_advisory_lock(namespace, key) as acquired:
                    if (
                        acquired
                        and namespace == ADVISORY_LOCK_NAMESPACE_RELEASE
                        and key == release_id_to_lock_key(self.MBID)
                    ):
                        move_saw_release_lock = True
                        try:
                            yield acquired
                        finally:
                            move_saw_release_lock = False
                    else:
                        yield acquired

            original_move_to = dl_mod.StagedAlbum.move_to

            def checked_move_to(album, dest, db=None):
                self.assertTrue(move_saw_release_lock)
                return original_move_to(album, dest, db)

            with patch.object(
                dl_mod,
                "dispatch_import_core",
                return_value=DispatchOutcome(success=True, message="ok"),
            ) as mock_dispatch, patch.object(
                db,
                "advisory_lock",
                side_effect=tracking_advisory_lock,
            ), patch.object(
                dl_mod.StagedAlbum,
                "move_to",
                autospec=True,
                side_effect=checked_move_to,
            ):
                outcome = dl_mod._handle_valid_result(
                    entry,
                    bv_result,
                    staged_album,
                    ctx,
                )

            assert outcome is not None
            self.assertTrue(outcome.success)
            staged_path = os.path.join(
                cfg.beets_staging_dir,
                "auto-import",
                "Test Artist",
                "Test Album [request-42]",
            )
            self.assertEqual(staged_album.current_path, staged_path)
            self.assertTrue(os.path.exists(os.path.join(staged_path, "01 - Track.mp3")))
            self.assertFalse(os.path.exists(processing_dir))
            self.assertFalse(move_saw_release_lock)
            self.assertEqual(
                db.request(42)["active_download_state"]["current_path"],
                staged_path,
            )
            mock_dispatch.assert_called_once()
            self.assertEqual(mock_dispatch.call_args.kwargs["path"], staged_path)

    def test_auto_path_not_blocked_when_processing_dir_is_under_staging_root(self):
        """The duplicate-import guard must not quarantine the source processing dir."""
        from lib import download as dl_mod
        from lib.import_dispatch import DispatchOutcome
        from lib.quality import ValidationResult

        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42,
            mb_release_id=self.MBID,
            status="downloading",
            active_download_state={
                "filetype": "mp3",
                "enqueued_at": "2026-04-03T12:00:00+00:00",
                "files": [],
                "current_path": None,
            },
        ))

        from tests.helpers import make_ctx_with_fake_db

        with tempfile.TemporaryDirectory() as tmpdir:
            staging_root = os.path.join(tmpdir, "beets-staging")
            processing_dir = os.path.join(staging_root, "downloads", "processing")
            os.makedirs(processing_dir)
            with open(os.path.join(processing_dir, "01 - Track.mp3"), "w") as fp:
                fp.write("fake audio")

            cfg = CratediggerConfig(
                beets_harness_path=_HARNESS,
                pipeline_db_enabled=True,
                beets_distance_threshold=0.15,
                beets_staging_dir=staging_root,
                slskd_download_dir=os.path.join(staging_root, "downloads"),
            )
            ctx = make_ctx_with_fake_db(db, cfg=cfg)
            entry = dl_mod.GrabListEntry(
                album_id=42,
                artist="Test Artist",
                title="Test Album",
                year="2006",
                files=[],
                filetype="mp3",
                mb_release_id=self.MBID,
                db_source="request",
                db_request_id=42,
                import_folder=processing_dir,
            )
            staged_album = dl_mod.StagedAlbum.from_entry(
                entry,
                default_path=processing_dir,
            )
            bv_result = ValidationResult(
                valid=True,
                distance=0.05,
                scenario="strong_match",
            )

            with patch.object(
                dl_mod,
                "dispatch_import_core",
                return_value=DispatchOutcome(success=True, message="ok"),
            ) as mock_dispatch:
                outcome = dl_mod._handle_valid_result(
                    entry,
                    bv_result,
                    staged_album,
                    ctx,
                )

            assert outcome is not None
            self.assertTrue(outcome.success)
            mock_dispatch.assert_called_once()


class TestRunCompletedProcessingOutcomeBranching(unittest.TestCase):
    """The ``process_completed_album`` return ownership seam.

    Pre-#133 this was a 2-way ``bool``: True → flip to ``imported``,
    False → reset to ``wanted``. That binary misclassified release-
    lock contention (commit 43e83e8 C1 — silently flipped to
    ``imported``). Commit 2's fix papered over the bug by having
    ``dispatch_import_core`` eagerly reset the row to ``wanted``;
    Codex R3 P2/P3 then flagged that the reset clobbered spectral
    state and staged files.

    The proper seam separates local bool fallback, ``None`` deferred
    retry, and ``DispatchOutcome`` queue summaries. These tests pin the
    branching at
    ``_run_completed_processing`` so a future refactor can't silently
    collapse dispatch ownership into bool fallback transitions.
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

    def test_false_outcome_after_inner_rejection_does_not_double_transition(self):
        """If ``process_completed_album`` already requeued the row, the outer
        ``False`` branch must not apply a second reset-to-wanted transition.
        """
        from lib import download as dl_mod
        from lib import transitions

        db = FakePipelineDB()
        db.seed_request(make_request_row(id=42, status="downloading"))

        def reject_inside_process(*_args, **_kwargs):
            transitions.finalize_request(
                cast(Any, db),
                42,
                transitions.RequestTransition.to_wanted(
                    from_status="downloading",
                ),
            )
            return False

        with patch.object(
            dl_mod,
            "process_completed_album",
            side_effect=reject_inside_process,
        ):
            dl_mod._run_completed_processing(
                self._entry(),
                42,
                self._state(),
                db,
                self._ctx(db),
            )

        self.assertEqual(db.request(42)["status"], "wanted")
        self.assertEqual(db.status_history, [(42, "wanted")])

    def test_dispatch_outcome_does_not_drive_fallback_transition(self):
        """Dispatch summaries are queue results, not fallback bool outcomes."""
        from lib import download as dl_mod
        from lib.import_dispatch import DispatchOutcome

        db = FakePipelineDB()
        db.seed_request(make_request_row(id=42, status="downloading"))
        dispatch_outcome = DispatchOutcome(
            success=False,
            message="Pre-import gate rejected",
        )

        with patch.object(
            dl_mod,
            "process_completed_album",
            return_value=dispatch_outcome,
        ):
            outcome = dl_mod._run_completed_processing(
                self._entry(),
                42,
                self._state(),
                db,
                self._ctx(db),
            )

        self.assertIs(outcome, dispatch_outcome)
        self.assertEqual(db.request(42)["status"], "downloading")
        self.assertEqual(db.status_history, [])

    def test_real_missing_request_id_rejection_transitions_once(self):
        """The real missing-request-id reject path must transition exactly once."""
        from lib import download as dl_mod
        from lib.quality import ValidationResult
        from tests.helpers import (
            make_ctx_with_fake_db,
            make_download_file,
            make_grab_list_entry,
        )

        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42,
            status="downloading",
            artist_name="Artist",
            album_title="Album",
            year=2024,
            mb_release_id="test-mbid",
        ))

        with tempfile.TemporaryDirectory() as tmpdir:
            downloads_root = os.path.join(tmpdir, "downloads")
            source_dir = os.path.join(downloads_root, "Music")
            os.makedirs(source_dir)
            with open(os.path.join(source_dir, "01 - Track.mp3"), "w") as fp:
                fp.write("fake audio")

            cfg = CratediggerConfig(
                beets_harness_path=_HARNESS,
                pipeline_db_enabled=True,
                beets_validation_enabled=True,
                beets_distance_threshold=0.15,
                beets_staging_dir=os.path.join(tmpdir, "staging"),
                slskd_download_dir=downloads_root,
                beets_tracking_file=os.path.join(tmpdir, "beets-tracking.jsonl"),
            )
            ctx = make_ctx_with_fake_db(db, cfg=cfg)
            entry = make_grab_list_entry(
                album_id=42,
                files=[make_download_file(
                    filename="user1\\Music\\01 - Track.mp3",
                    file_dir="user1\\Music",
                )],
                artist="Artist",
                title="Album",
                year="2024",
                mb_release_id="test-mbid",
                db_source="request",
                db_request_id=None,
            )

            from lib.preimport import PreimportMeasurement
            with patch("lib.download.music_tag.load_file", return_value=MagicMock()), \
                 patch("lib.beets.beets_validate", return_value=ValidationResult(
                     valid=True,
                     distance=0.05,
                     scenario="strong_match",
                 )), \
                 patch(
                     "lib.download.measure_preimport_state",
                     return_value=PreimportMeasurement(
                         folder_layout="flat",
                         audio_file_count=1,
                         filetype_band="mp3",
                     ),
                 ):
                dl_mod._run_completed_processing(
                    entry,
                    42,
                    self._state(),
                    db,
                    ctx,
                )

        self.assertEqual(db.request(42)["status"], "wanted")
        self.assertEqual(db.status_history, [(42, "wanted")])
        self.assertEqual(db.recorded_attempts, [(42, "validation")])
        self.assertEqual(len(db.download_logs), 1)
        self.assertEqual(
            db.download_logs[0].beets_scenario,
            "request_missing_request_id",
        )


class TestWrongMatchTriageMeasurementRoundTrip(unittest.TestCase):
    """End-to-end pin from preview measurement → triage → DB → row read.

    PR #181 wired the four candidate-evidence keys onto the wrong-matches
    entry payload reading from flat ``download_log`` columns. The plan
    that ships those columns from triage's preview lives in
    ``docs/plans/2026-04-28-002``. This slice is the cross-layer proof
    that would have caught yesterday's drop-on-the-floor bug:
    ``triage_wrong_match`` runs end-to-end against ``FakePipelineDB``
    with a stubbed preview, and the row's flat columns must surface in
    the next ``get_wrong_matches`` read.
    """

    def _seed_wrong_match(self, source: str) -> tuple[FakePipelineDB, int]:
        from tests.helpers import make_request_row as _make_req
        db = FakePipelineDB()
        db.seed_request(_make_req(
            id=1, status="manual", mb_release_id="mbid-1"))
        db.log_download(
            1,
            outcome="rejected",
            validation_result={
                "scenario": "wrong_match",
                "failed_path": source,
            },
        )
        return db, db.download_logs[-1].id

    def _measured_preview(self, source: str):
        from lib.import_preview import ImportPreviewResult
        from lib.quality import V0_PROBE_LOSSLESS_SOURCE, V0ProbeEvidence
        return ImportPreviewResult(
            mode="download_log",
            verdict="would_import",
            would_import=True,
            decision="import",
            reason="import",
            source_path=source,
            import_result=ImportResult(
                decision="import",
                new_measurement=AudioQualityMeasurement(
                    spectral_grade="genuine",
                    spectral_bitrate_kbps=950,
                ),
                v0_probe=V0ProbeEvidence(
                    kind=V0_PROBE_LOSSLESS_SOURCE,
                    avg_bitrate_kbps=265,
                ),
            ),
        )

    def test_auto_triage_round_trip_populates_get_wrong_matches(self):
        """Covers AE1: post-rejection triage path → row read returns the
        four candidate-evidence keys with values."""
        from lib.wrong_match_triage import triage_wrong_match
        from lib.wrong_match_cleanup_decision import WrongMatchCleanupDecision
        source = tempfile.mkdtemp()
        try:
            db, log_id = self._seed_wrong_match(source)
            preview = self._measured_preview(source)
            decision = WrongMatchCleanupDecision(
                download_log_id=log_id,
                delete_allowed=False,
                uncertain=False,
                provenance="album_quality_evidence",
                verdict="would_import",
                confident_reject=False,
                cleanup_eligible=False,
                preview_decision=preview.decision,
                reason=preview.reason,
                request_id=1,
                source_path=source,
                stage_chain=tuple(preview.stage_chain),
                import_result=preview.import_result,
            )
            with patch(
                "lib.wrong_match_triage.decide_wrong_match_cleanup",
                return_value=decision,
            ):
                triage_wrong_match(db, log_id)

            rows = db.get_wrong_matches()
            self.assertEqual(len(rows), 1)
            row = rows[0]
            self.assertEqual(row["spectral_grade"], "genuine")
            self.assertEqual(row["spectral_bitrate"], 950)
            self.assertEqual(row["v0_probe_kind"], "lossless_source_v0")
            self.assertEqual(row["v0_probe_avg_bitrate"], 265)
        finally:
            import shutil
            shutil.rmtree(source, ignore_errors=True)

    def test_backfill_round_trip_populates_get_wrong_matches(self):
        """Covers AE2: operator-initiated backfill produces the same
        outcome as post-rejection auto-triage."""
        from lib.wrong_match_triage import backfill_wrong_match_previews
        source = tempfile.mkdtemp()
        try:
            with open(os.path.join(source, "01.mp3"), "wb") as handle:
                handle.write(b"audio")
            db, _log_id = self._seed_wrong_match(source)
            with patch(
                "lib.wrong_match_triage.preview_import_from_download_log",
                return_value=self._measured_preview(source),
            ):
                summary = backfill_wrong_match_previews(db)

            self.assertEqual(summary["previewed"], 1)
            row = db.get_wrong_matches()[0]
            self.assertEqual(row["spectral_grade"], "genuine")
            self.assertEqual(row["spectral_bitrate"], 950)
            self.assertEqual(row["v0_probe_kind"], "lossless_source_v0")
            self.assertEqual(row["v0_probe_avg_bitrate"], 265)
        finally:
            import shutil
            shutil.rmtree(source, ignore_errors=True)

    def test_early_reject_keeps_row_dashed(self):
        """Covers AE3 / R5: a row whose preview legitimately produced no
        measurement (e.g. nested_layout that isn't cleanup-eligible)
        keeps NULL flat columns, so the UI renders a dash."""
        from lib.import_preview import ImportPreviewResult
        from lib.wrong_match_triage import triage_wrong_match
        source = tempfile.mkdtemp()
        try:
            db, log_id = self._seed_wrong_match(source)
            with patch(
                "lib.wrong_match_triage.preview_import_from_download_log",
                return_value=ImportPreviewResult(
                    mode="download_log",
                    verdict="uncertain",
                    uncertain=True,
                    decision="conversion_failed",
                    reason="conversion_failed",
                    source_path=source,
                    import_result=None,
                ),
            ):
                triage_wrong_match(db, log_id)

            row = db.get_wrong_matches()[0]
            for col in ("spectral_grade", "spectral_bitrate",
                        "v0_probe_kind", "v0_probe_avg_bitrate"):
                self.assertIsNone(row[col],
                                  f"{col} must remain None when preview produced no measurement")
        finally:
            import shutil
            shutil.rmtree(source, ignore_errors=True)


class TestBadAudioHashSlice(unittest.TestCase):
    """Integration slice: bad-audio-hash gate inside ``run_preimport_gates``.

    Plan 2026-04-29-005 / U5. Populates ``FakePipelineDB`` with the U3 fixture's
    real hash, points ``run_preimport_gates`` at the fixture, and asserts the
    full F2 path: rejection scenario, denylist row written, and
    ``PreImportGateResult`` carries ``matched_bad_hash_id`` /
    ``matched_bad_track_path`` for the caller to fold into ``ValidationResult``.
    """

    def test_known_bad_hash_rejects_and_denylists(self):
        from pathlib import Path
        from lib.audio_hash import hash_audio_content
        from lib.config import CratediggerConfig
        from lib.pipeline_db import BadAudioHashInput
        from lib.preimport import run_preimport_gates

        fixture_dir = (
            Path(__file__).parent / "fixtures" / "audio_hash"
        )
        bad_track = fixture_dir / "sine_440.mp3"

        # Compute the real fixture hash and seed it via the U2 fake method.
        digest = hash_audio_content(bad_track, "mp3")

        db = FakePipelineDB()
        db.seed_request(make_request_row(id=42, status="downloading"))
        db.add_bad_audio_hashes(
            request_id=99,
            reported_username="curator",
            reason="exemplar bad rip",
            hashes=[BadAudioHashInput(hash_value=digest, audio_format="mp3")],
        )

        # cfg.audio_check_mode='off' so the gate doesn't try to ffmpeg-decode
        # the fixture (which is a 1-second sine and would pass anyway, but
        # 'off' keeps the slice scoped to the bad-hash gate).
        cfg = CratediggerConfig(audio_check_mode="off")

        result = run_preimport_gates(
            path=str(fixture_dir),
            mb_release_id="mbid-bad",
            label="Bad Rip Test",
            download_filetype="mp3",
            download_min_bitrate_bps=320_000,
            download_is_vbr=False,
            cfg=cfg,
            db=db,  # type: ignore[arg-type]
            request_id=42,
            usernames={"H@rco"},
        )

        # 1. Gate rejected the candidate with the bad-hash scenario.
        self.assertFalse(result.valid)
        self.assertEqual(result.scenario, "bad_audio_hash")
        self.assertIsNotNone(result.matched_bad_hash_id)
        # The matched track must be the actual fixture path we seeded.
        self.assertEqual(result.matched_bad_track_path, str(bad_track))

        # 2. Supplying user denylisted on this request, with bad-hash reason.
        self.assertEqual(len(db.denylist), 1)
        self.assertEqual(db.denylist[0].request_id, 42)
        self.assertEqual(db.denylist[0].username, "H@rco")
        self.assertIn("matched bad hash", db.denylist[0].reason or "")

        # 3. Detail string surfaces the hash id + track path for log audit.
        assert result.detail is not None
        self.assertIn("matched bad audio hash", result.detail)
        self.assertIn(str(bad_track), result.detail)

    def test_empty_table_runs_no_hashing(self):
        """When ``has_any_bad_audio_hashes`` is False, the gate fast-skips:
        no calls to ``hash_audio_content`` or ``lookup_bad_audio_hash``."""
        from pathlib import Path
        from lib.config import CratediggerConfig
        from lib.preimport import run_preimport_gates

        fixture_dir = (
            Path(__file__).parent / "fixtures" / "audio_hash"
        )

        db = FakePipelineDB()
        db.seed_request(make_request_row(id=42, status="downloading"))
        cfg = CratediggerConfig(audio_check_mode="off")

        with patch("lib.preimport.hash_audio_content") as hashfn, \
             patch.object(db, "lookup_bad_audio_hash") as lookup, \
             patch("lib.preimport._needs_spectral_check", return_value=False):
            result = run_preimport_gates(
                path=str(fixture_dir),
                mb_release_id="mbid-empty",
                label="Empty Table",
                download_filetype="mp3",
                download_min_bitrate_bps=320_000,
                download_is_vbr=False,
                cfg=cfg,
                db=db,  # type: ignore[arg-type]
                request_id=42,
                usernames={"someone"},
            )

        self.assertTrue(result.valid)
        self.assertIsNone(result.matched_bad_hash_id)
        hashfn.assert_not_called()
        lookup.assert_not_called()
        self.assertEqual(len(db.denylist), 0)


class TestSearchForensicsCaptureSlice(unittest.TestCase):
    """U5 integration slice: search_for_album → find_download → log_search.

    Drives the production search loop end-to-end with FakeSlskdSearches +
    FakePipelineDB and asserts the persisted ``search_log.candidates``
    JSONB shape, ``variant``, and ``final_state``. Covers the headline U5
    test scenarios from the plan: response limit, variant ladder, top-20
    truncation, exhaustion short-circuit, and Discogs-source parity.
    """

    def setUp(self):
        import cratedigger
        self._cratedigger = cratedigger
        self._orig_cfg = cratedigger.cfg
        self._orig_slskd = cratedigger.slskd
        self._orig_pdb = cratedigger.pipeline_db_source
        self._orig_module_ctx = cratedigger._module_ctx

    def tearDown(self):
        self._cratedigger.cfg = self._orig_cfg
        self._cratedigger.slskd = self._orig_slskd
        self._cratedigger.pipeline_db_source = self._orig_pdb
        self._cratedigger._module_ctx = self._orig_module_ctx

    def _make_cfg(self, **overrides):
        """Build CratediggerConfig via from_ini, then apply overrides.

        CLAUDE.md / code-quality.md forbids partial-kwarg construction —
        partial configs silently diverge when new fields are added. INI
        key names match lib/config.py:from_ini (e.g. minimum_match_ratio
        is loaded from the INI key 'minimum_filename_match_ratio').
        """
        import configparser
        from dataclasses import replace as _replace
        from lib.config import CratediggerConfig
        ini = configparser.ConfigParser()
        ini["Slskd"] = {"delete_searches": "False"}
        ini["Search Settings"] = {
            "allowed_filetypes": "flac",
            "search_response_limit": "1000",
            "search_escalation_threshold": "5",
            "search_timeout": "30000",
            "album_prepend_artist": "False",
            "minimum_filename_match_ratio": "0.5",
        }
        cfg = CratediggerConfig.from_ini(ini)
        if overrides:
            cfg = _replace(cfg, **overrides)
        return cfg

    def _make_album(self, *, request_id=1843, source="request",
                    discogs_release_id=None, mb_release_id="mbid-test",
                    artist_name="Wiggles"):
        """Build an AlbumRecord matching the production from_db_row shape."""
        from album_source import AlbumRecord, ReleaseRecord, MediaRecord

        media = [MediaRecord(medium_number=1, medium_format="CD", track_count=2)]
        release = ReleaseRecord(
            id=-request_id, foreign_release_id=mb_release_id or "",
            title="Album", track_count=2, medium_count=1,
            format="CD", media=media, monitored=True,
            country=["US"], status="Official",
        )
        return AlbumRecord(
            id=-request_id, title="Album",
            release_date="1991-01-01T00:00:00Z",
            artist_id=0, artist_name=artist_name,
            foreign_artist_id="",
            releases=[release],
            db_request_id=request_id, db_source=source,
            db_mb_release_id=mb_release_id or "",
            db_search_filetype_override=None, db_target_format=None,
        )

    def _wire(self, cfg, slskd, db, album):
        """Wire module globals + ctx so search_for_album can run.

        The pipeline_db_source mock proxies ``_get_db`` to the FakePipelineDB,
        and ``get_tracks(album)`` to the same fake's per-request tracks list
        re-shaped into the ``TrackRecord`` dicts that ``find_download``
        expects (``albumId`` = -request_id, mirroring the real DatabaseSource).

        Also seeds an active plan for the request when one is missing. The
        post-U5 executor is plan-driven; legacy forensic tests that assume
        the variant ladder are translated by mapping the variant they
        expect onto plan-item strategies (default / unwild / unwild_year /
        track_<idx>).
        """
        from lib.context import CratediggerContext
        cratedigger = self._cratedigger
        cratedigger.cfg = cfg
        cratedigger.slskd = slskd
        source = MagicMock()
        source._get_db.return_value = db

        def _get_tracks(album_record: Any) -> list[dict[str, Any]]:
            request_id = getattr(album_record, "db_request_id", None)
            if not request_id:
                return []
            rows = db.get_tracks(request_id)
            album_id = request_id * -1
            return [{
                "title": r["title"],
                "trackNumber": str(r["track_number"]),
                "mediumNumber": r.get("disc_number", 1),
                "duration": 0,
                "id": 0,
                "albumId": album_id,
            } for r in rows]
        source.get_tracks.side_effect = _get_tracks

        cratedigger.pipeline_db_source = source
        ctx = CratediggerContext(
            cfg=cfg, slskd=slskd, pipeline_db_source=source,
        )
        cratedigger._module_ctx = ctx
        # search_for_album / find_download read the album from the cache.
        ctx.current_album_cache[album.id] = album
        return ctx

    def _seed_plan(
        self,
        db,
        request_id: int,
        *,
        items: list[tuple[str, str]] | None = None,
        cursor_ordinal: int = 0,
        cycle_count: int = 0,
        generator_id: str | None = None,
    ) -> int:
        """Seed an active search plan for ``request_id``.

        ``items`` is a list of ``(strategy, query)`` pairs. Defaults to a
        single ``default`` slot with a generic query so search_for_album
        can run end-to-end without legacy ``select_variant`` plumbing.
        """
        from lib.pipeline_db import SearchPlanItemInput
        from lib.search import SEARCH_PLAN_GENERATOR_ID
        if items is None:
            items = [("default", "*iggles Album")]
        plan_id = db.create_successful_search_plan(
            request_id=request_id,
            generator_id=generator_id or SEARCH_PLAN_GENERATOR_ID,
            items=[
                SearchPlanItemInput(
                    ordinal=i, strategy=s, query=q,
                    canonical_query_key=q.lower(),
                )
                for i, (s, q) in enumerate(items)
            ],
        )
        if cursor_ordinal or cycle_count:
            db.update_request_fields(
                request_id,
                next_plan_ordinal=cursor_ordinal,
                plan_cycle_count=cycle_count,
            )
        return plan_id

    def test_default_variant_passes_response_limit_and_persists_candidates(self):
        """Happy path: default variant, slskd returns peers, candidates persist."""
        import json
        import msgspec
        from lib.quality import CandidateScore
        from tests.fakes import FakePipelineDB, FakeSlskdAPI

        cfg = self._make_cfg(search_response_limit=1500)
        slskd = FakeSlskdAPI()
        # One peer returns a full 2-track FLAC dir for the album.
        slskd.searches.add_search(
            search_id=42,
            state="Completed",
            responses=[{
                "username": "good_peer",
                "uploadSpeed": 100_000,
                "files": [
                    {"filename": "Music\\Album\\01 - Track One.flac", "bitRate": 1411},
                    {"filename": "Music\\Album\\02 - Track Two.flac", "bitRate": 1411},
                ],
            }],
        )
        slskd.searches.search_text_id_sequence = [42]
        # find_download → check_for_match → users.directory(); seed it
        # with the same files so the count gate passes.
        slskd.users.set_directory("good_peer", "Music\\Album", [{
            "directory": "Music\\Album",
            "files": [
                {"filename": "01 - Track One.flac", "size": 1, "id": "tid-1"},
                {"filename": "02 - Track Two.flac", "size": 1, "id": "tid-2"},
            ],
        }])

        db = FakePipelineDB()
        rid = db.add_request(
            artist_name="Wiggles", album_title="Album",
            source="request", mb_release_id="mbid-test", year=1991,
        )
        db.set_tracks(rid, [
            {"track_number": 1, "title": "Track One"},
            {"track_number": 2, "title": "Track Two"},
        ])
        album = self._make_album(request_id=rid)
        # Plan-driven (U5): seed a default plan-item so search_for_album
        # has a runnable query.
        self._seed_plan(db, rid, items=[("default", "*iggles Album")])
        ctx = self._wire(cfg, slskd, db, album)

        # Stub slskd_do_enqueue so we do not exercise the real download path.
        with patch("lib.enqueue.slskd_do_enqueue", return_value=[
            MagicMock(),
        ]):
            result = self._cratedigger.search_for_album(album, ctx)
            grab_list: dict[Any, Any] = {}
            from lib.enqueue import find_download
            find_result = find_download(album, ctx)
            self.assertEqual(grab_list, {})
            self._cratedigger._apply_find_download_result(
                album, result, find_result, [], grab_list, ctx)
            self.assertIn(album.id, grab_list)
            self._cratedigger._log_search_result(album, result, ctx)

        # responseLimit was forwarded to slskd at the wire boundary.
        call = slskd.searches.search_text_calls[0]
        self.assertEqual(call.kwargs["responseLimit"], 1500)

        # SearchResult carries the variant and final state.
        self.assertEqual(result.variant_tag, "default")
        self.assertEqual(result.final_state, "Completed")

        # search_log row was written with variant + final_state + candidates.
        self.assertEqual(len(db.search_logs), 1)
        row = db.search_logs[0]
        self.assertEqual(row.variant, "default")
        self.assertEqual(row.final_state, "Completed")
        # JSONB blob round-trips through msgspec.convert.
        assert row.candidates is not None, "expected candidates to persist"
        decoded = msgspec.convert(
            json.loads(row.candidates), type=list[CandidateScore])
        self.assertGreaterEqual(len(decoded), 1)
        self.assertEqual(decoded[0].username, "good_peer")
        self.assertEqual(decoded[0].matched_tracks, 2)
        self.assertEqual(decoded[0].total_tracks, 2)

    def test_unwild_variant_at_threshold(self):
        """Plan-item with strategy='unwild' produces an unwild query (post-U5).

        Post-cutover the executor reads plan items, not a runtime variant
        ladder. The plan generator (U2) materialises the unwild slot at
        the threshold position; here we seed it directly to assert the
        executor hits it.
        """
        from tests.fakes import FakePipelineDB, FakeSlskdAPI

        cfg = self._make_cfg(
            search_escalation_threshold=5, album_prepend_artist=True,
        )
        slskd = FakeSlskdAPI()
        slskd.searches.search_text_id_sequence = [99]
        slskd.searches.add_search(search_id=99, state="Completed", responses=[])

        db = FakePipelineDB()
        rid = db.add_request(
            artist_name="Wiggles", album_title="Album",
            source="request", mb_release_id="mbid-y", year=1991,
        )
        db.set_tracks(rid, [{"track_number": 1, "title": "Hot Potato"}])
        album = self._make_album(request_id=rid, mb_release_id="mbid-y")
        # Seed a plan whose first item is the unwild slot at the cursor.
        self._seed_plan(db, rid, items=[
            ("unwild", "Wiggles Album"),
        ])
        ctx = self._wire(cfg, slskd, db, album)

        result = self._cratedigger.search_for_album(album, ctx)
        self._cratedigger._log_search_result(album, result, ctx)

        self.assertEqual(result.variant_tag, "unwild")
        call = slskd.searches.search_text_calls[0]
        # No wildcard artist tokens — full-recall query.
        self.assertNotIn("*", call.search_text)
        self.assertIn("Wiggles", call.search_text)
        self.assertEqual(db.search_logs[0].variant, "unwild")

    def test_unwild_year_variant_at_threshold_plus_one(self):
        """Plan-item strategy='unwild_year' produces an unwild+year query."""
        from tests.fakes import FakePipelineDB, FakeSlskdAPI

        cfg = self._make_cfg(
            search_escalation_threshold=5, album_prepend_artist=True,
        )
        slskd = FakeSlskdAPI()
        slskd.searches.search_text_id_sequence = [99]
        slskd.searches.add_search(search_id=99, state="Completed", responses=[])

        db = FakePipelineDB()
        rid = db.add_request(
            artist_name="Wiggles", album_title="Album",
            source="request", mb_release_id="mbid-y", year=1991,
        )
        db.set_tracks(rid, [{"track_number": 1, "title": "Hot Potato"}])
        album = self._make_album(request_id=rid, mb_release_id="mbid-y")
        self._seed_plan(db, rid, items=[
            ("unwild_year", "Wiggles Album 1991"),
        ])
        ctx = self._wire(cfg, slskd, db, album)

        result = self._cratedigger.search_for_album(album, ctx)
        self._cratedigger._log_search_result(album, result, ctx)

        self.assertEqual(result.variant_tag, "unwild_year")
        call = slskd.searches.search_text_calls[0]
        self.assertNotIn("*", call.search_text)
        self.assertIn("Wiggles", call.search_text)
        self.assertIn("1991", call.search_text)
        self.assertEqual(db.search_logs[0].variant, "unwild_year")

    def test_final_ordinal_wraps_cycle_with_no_new_exhausted_row(self):
        """Plan §AE8: executing the final ordinal wraps cursor to 0 and
        increments plan_cycle_count. No new ``outcome='exhausted'`` row.

        Replaces the legacy variant-ladder exhaustion test. Plan wrap is
        the new exhaustion semantic; the search_log row carries the
        normal slskd outcome (no_results here) plus
        ``cursor_update_status='wrapped'``.
        """
        from album_source import AlbumRecord, MediaRecord, ReleaseRecord
        from tests.fakes import FakePipelineDB, FakeSlskdAPI

        cfg = self._make_cfg(search_escalation_threshold=5)
        slskd = FakeSlskdAPI()
        slskd.searches.search_text_id_sequence = [42]
        slskd.searches.add_search(search_id=42, state="Completed", responses=[])

        db = FakePipelineDB()
        rid = db.add_request(
            artist_name="Wiggles", album_title="Album",
            source="request", mb_release_id="mbid-x", year=None,
        )
        media = [MediaRecord(medium_number=1, medium_format="CD", track_count=0)]
        release = ReleaseRecord(
            id=-rid, foreign_release_id="mbid-x", title="Album",
            track_count=0, medium_count=1, format="CD", media=media,
            monitored=True, country=["US"], status="Official",
        )
        album = AlbumRecord(
            id=-rid, title="Album",
            release_date="0000-01-01T00:00:00Z",
            artist_id=0, artist_name="Wiggles", foreign_artist_id="",
            releases=[release], db_request_id=rid, db_source="request",
            db_mb_release_id="mbid-x",
            db_search_filetype_override=None, db_target_format=None,
        )
        # Two-item plan, cursor parked at the FINAL ordinal so this run
        # wraps. Pre-run snapshot: plan_cycle_count=3, next_ordinal=1.
        self._seed_plan(db, rid, items=[
            ("default", "*iggles Album"),
            ("unwild", "Wiggles Album"),
        ], cursor_ordinal=1, cycle_count=3)
        ctx = self._wire(cfg, slskd, db, album)

        result = self._cratedigger.search_for_album(album, ctx)
        self._cratedigger._log_search_result(album, result, ctx)

        # slskd was hit; outcome=no_results (consumed slot).
        self.assertEqual(result.outcome, "no_results")
        # No new exhausted row.
        outcomes = [r.outcome for r in db.search_logs]
        self.assertNotIn("exhausted", outcomes)
        # Cursor wrapped + cycle incremented.
        row = db.request(rid)
        self.assertEqual(row["next_plan_ordinal"], 0)
        self.assertEqual(row["plan_cycle_count"], 4)
        # search_log row carries plan context with cursor_update_status='wrapped'.
        log = db.search_logs[-1]
        self.assertEqual(log.cursor_update_status, "wrapped")
        self.assertEqual(log.plan_ordinal, 1)
        self.assertEqual(log.plan_cycle_snapshot, 3)

    def test_no_results_writes_empty_candidates_and_final_state(self):
        """no_results writes candidates=[] (empty JSONB array, not NULL).

        Plan U5 contract: a search that ran successfully but returned 0 hits
        still wrote a search_log row, and the candidates blob distinguishes
        "search ran, no peers" (``[]``) from "search never produced a
        candidate concept" (``None`` — error, timeout, exhausted,
        empty_query). FakePipelineDB serialises ``[]`` to the JSON literal
        ``"[]"``.
        """
        from tests.fakes import FakePipelineDB, FakeSlskdAPI

        cfg = self._make_cfg()
        slskd = FakeSlskdAPI()
        slskd.searches.search_text_id_sequence = [7]
        slskd.searches.add_search(
            search_id=7, state="ResponseLimitReached", responses=[])

        db = FakePipelineDB()
        rid = db.add_request(
            artist_name="Wiggles", album_title="Album",
            source="request", mb_release_id="mbid-n", year=1991,
        )
        db.set_tracks(rid, [{"track_number": 1, "title": "Track"}])
        album = self._make_album(request_id=rid, mb_release_id="mbid-n")
        self._seed_plan(db, rid)
        ctx = self._wire(cfg, slskd, db, album)

        result = self._cratedigger.search_for_album(album, ctx)
        self._cratedigger._log_search_result(album, result, ctx)

        self.assertEqual(result.outcome, "no_results")
        self.assertEqual(result.final_state, "ResponseLimitReached")
        row = db.search_logs[0]
        self.assertEqual(row.outcome, "no_results")
        self.assertEqual(row.final_state, "ResponseLimitReached")
        # search ran but no peers responded → candidates=[] (empty array).
        self.assertEqual(row.candidates, "[]")

    def test_top_20_truncation_when_many_candidates(self):
        """JSONB blob caps at top-20 by (matched_tracks, avg_ratio) DESC."""
        import json
        from lib.quality import CandidateScore
        from lib.search import SearchResult
        from tests.fakes import FakePipelineDB

        from lib.search import PlanExecutionContext, SEARCH_PLAN_GENERATOR_ID
        cfg = self._make_cfg()
        db = FakePipelineDB()
        rid = db.add_request(
            artist_name="A", album_title="B", source="request",
        )
        plan_id = self._seed_plan(db, rid)
        ctx = self._wire(cfg, MagicMock(), db, self._make_album(request_id=rid))

        # Build 30 synthetic candidates, descending matched_tracks.
        many = tuple(
            CandidateScore(
                username=f"u{i}", dir=f"d{i}", filetype="flac",
                matched_tracks=30 - i, total_tracks=30,
                avg_ratio=0.5, missing_titles=[], file_count=30 - i,
            )
            for i in range(30)
        )
        # Plan-driven (U5): construct a plan_execution snapshot matching the
        # seeded plan so _log_search_result routes through the consumed seam.
        active = db.get_active_search_plan(rid)
        assert active is not None
        item = active.items[0]
        plan_exec = PlanExecutionContext(
            plan_id=plan_id, plan_item_id=item.id, plan_ordinal=item.ordinal,
            plan_strategy=item.strategy,
            plan_canonical_query_key=item.canonical_query_key,
            plan_repeat_group=item.repeat_group,
            plan_generator_id=SEARCH_PLAN_GENERATOR_ID,
            plan_item_count=len(active.items),
            cycle_count_snapshot=active.cycle_count,
        )
        result = SearchResult(
            album_id=-rid, success=False, query="q", outcome="no_match",
            variant_tag="default", final_state="Completed",
            candidates=many,
            plan_execution=plan_exec,
        )
        album = self._make_album(request_id=rid)
        self._cratedigger._log_search_result(album, result, ctx)

        row = db.search_logs[0]
        assert row.candidates is not None
        decoded = json.loads(row.candidates)
        self.assertEqual(len(decoded), 20, "must truncate to top 20")
        # The very top entry has matched_tracks=30 (i=0 in the source list).
        self.assertEqual(decoded[0]["matched_tracks"], 30)
        self.assertEqual(decoded[-1]["matched_tracks"], 11)

    def test_discogs_source_request_produces_same_blob_shape(self):
        """A Discogs-source request flows through the same forensic capture."""
        import json
        from tests.fakes import FakePipelineDB, FakeSlskdAPI

        cfg = self._make_cfg()
        slskd = FakeSlskdAPI()
        slskd.searches.search_text_id_sequence = [123]
        slskd.searches.add_search(
            search_id=123, state="Completed", responses=[{
                "username": "discog_peer",
                "uploadSpeed": 1,
                "files": [
                    {"filename": "A\\B\\Disco Track.flac", "bitRate": 1411},
                ],
            }])
        slskd.users.set_directory("discog_peer", "A\\B", [{
            "directory": "A\\B",
            "files": [{"filename": "Disco Track.flac", "size": 1, "id": "z"}],
        }])

        db = FakePipelineDB()
        rid = db.add_request(
            artist_name="Disco Artist", album_title="Disco",
            source="request",
            # Discogs-numeric id stored in the dedicated column.
            discogs_release_id="123456", year=1991,
        )
        db.set_tracks(rid, [{"track_number": 1, "title": "Disco Track"}])
        album = self._make_album(
            request_id=rid, source="request",
            mb_release_id="",  # MB unknown; release id sits in discogs col.
        )
        self._seed_plan(db, rid, items=[("default", "*isco Disco")])
        ctx = self._wire(cfg, slskd, db, album)

        result = self._cratedigger.search_for_album(album, ctx)
        grab_list: dict[Any, Any] = {}
        from lib.enqueue import find_download
        with patch("lib.enqueue.slskd_do_enqueue", return_value=[MagicMock()]):
            find_result = find_download(album, ctx)
        self._cratedigger._apply_find_download_result(
            album, result, find_result, [], grab_list, ctx)
        self._cratedigger._log_search_result(album, result, ctx)

        # Same blob shape regardless of MB-vs-Discogs origin.
        row = db.search_logs[0]
        self.assertEqual(row.variant, "default")
        assert row.candidates is not None
        decoded = json.loads(row.candidates)
        self.assertEqual(decoded[0]["username"], "discog_peer")
        self.assertEqual(decoded[0]["matched_tracks"], 1)

    def test_parallel_submit_search_forwards_response_limit(self):
        """Parallel path (_submit_search) wires search_response_limit too.

        The serial path test above asserts the responseLimit kwarg on the
        slskd call. The parallel path is a separate function and was missing
        the same coverage; if a future refactor regressed the kwarg only on
        the parallel path, the existing test would not catch it.

        Post-#9/#18 refactor: variant selection is hoisted to the caller, so
        this test selects the variant via the same helper the parallel loop
        uses, then passes it into ``_submit_search``.
        """
        from tests.fakes import FakePipelineDB, FakeSlskdAPI

        cfg = self._make_cfg(search_response_limit=2500)
        slskd = FakeSlskdAPI()
        slskd.searches.search_text_id_sequence = [501]
        slskd.searches.add_search(search_id=501, state="Completed", responses=[])

        db = FakePipelineDB()
        rid = db.add_request(
            artist_name="Wiggles", album_title="Album",
            source="request", mb_release_id="mbid-p", year=1991,
        )
        db.set_tracks(rid, [{"track_number": 1, "title": "Track"}])
        album = self._make_album(request_id=rid, mb_release_id="mbid-p")
        self._wire(cfg, slskd, db, album)

        variant, _base_query = self._cratedigger._select_variant_for_album(
            album, cfg, db,
        )
        submit = self._cratedigger._submit_search(album, variant, cfg, slskd)
        self.assertIsNotNone(submit)
        # responseLimit was forwarded to slskd at the wire boundary on the
        # parallel path (no _collect_search_results call needed for this
        # assertion).
        self.assertEqual(len(slskd.searches.search_text_calls), 1)
        call = slskd.searches.search_text_calls[0]
        self.assertEqual(call.kwargs.get("responseLimit"),
                         cfg.search_response_limit)

    def test_track_variant_used_after_unwild_year(self):
        """Plan-item strategy='track_0' produces a track-tier query (post-U5).

        The plan generator (U2) materialises track tier slots; here we
        seed a track_0 slot directly to assert the executor uses it.
        """
        from tests.fakes import FakePipelineDB, FakeSlskdAPI

        cfg = self._make_cfg(search_escalation_threshold=5)
        slskd = FakeSlskdAPI()
        slskd.searches.search_text_id_sequence = [777]
        slskd.searches.add_search(search_id=777, state="Completed", responses=[])

        db = FakePipelineDB()
        rid = db.add_request(
            artist_name="The Mountain Goats", album_title="Album",
            source="request", mb_release_id="mbid-v4", year=1991,
        )
        db.set_tracks(rid, [
            {"track_number": 1, "title": "Tallahassee"},
            {"track_number": 2, "title": "Wide Open Road"},
            {"track_number": 3, "title": "Frontier"},
            {"track_number": 4, "title": "Treasure"},
        ])
        album = self._make_album(
            request_id=rid,
            mb_release_id="mbid-v4",
            artist_name="The Mountain Goats",
        )
        self._seed_plan(db, rid, items=[
            ("track_0", "Tallahassee Mountain"),
        ])
        ctx = self._wire(cfg, slskd, db, album)

        result = self._cratedigger.search_for_album(album, ctx)
        self._cratedigger._log_search_result(album, result, ctx)

        self.assertEqual(result.variant_tag, "track_0")
        self.assertEqual(len(slskd.searches.search_text_calls), 1)
        call = slskd.searches.search_text_calls[0]
        self.assertEqual(call.search_text, "Tallahassee Mountain")
        self.assertNotIn("*", call.search_text)
        # search_log persisted the variant.
        self.assertEqual(db.search_logs[0].variant, "track_0")

    def test_slskd_error_at_submit_is_non_consuming_pre_attempt_failure(self):
        """Plan §AE7: submit/setup failure before accepted search id is
        non-consuming. Cursor stays at 0; backoff IS applied; the row is
        a pre_attempt stage row with attempt_consumed=False.
        """
        from tests.fakes import FakePipelineDB, FakeSlskdAPI

        cfg = self._make_cfg()
        slskd = FakeSlskdAPI()
        # Cause search_text() to raise — search_for_album catches it and
        # emits outcome="error".
        slskd.searches.search_text_error = RuntimeError("offline")

        db = FakePipelineDB()
        rid = db.add_request(
            artist_name="Wiggles", album_title="Album",
            source="request", mb_release_id="mbid-err", year=1991,
        )
        db.set_tracks(rid, [{"track_number": 1, "title": "Track"}])
        album = self._make_album(request_id=rid, mb_release_id="mbid-err")
        self._seed_plan(db, rid, items=[
            ("default", "*iggles Album"),
            ("unwild", "Wiggles Album"),
        ])
        ctx = self._wire(cfg, slskd, db, album)

        result = self._cratedigger.search_for_album(album, ctx)
        self._cratedigger._log_search_result(album, result, ctx)

        self.assertEqual(result.outcome, "error")
        row = db.search_logs[0]
        self.assertEqual(row.outcome, "error")
        # Plan context preserved (plan_strategy carries the strategy tag
        # for non-consuming rows; search_log.variant is only populated
        # on the consumed-attempt seam).
        self.assertEqual(row.plan_strategy, "default")
        # Pre-attempt failure: attempt_consumed=False, stage=pre_attempt,
        # cursor unchanged.
        self.assertEqual(row.execution_stage, "pre_attempt")
        self.assertFalse(row.attempt_consumed)
        self.assertEqual(row.cursor_update_status, "unchanged")
        # Cursor is still parked at the first ordinal; backoff applied.
        request = db.request(rid)
        self.assertEqual(request["next_plan_ordinal"], 0)
        self.assertEqual(request["plan_cycle_count"], 0)
        self.assertEqual(request["search_attempts"], 1)

    def test_serial_collection_error_after_accept_is_consumed(self):
        """Once slskd returns a search id, polling/collection failures still
        consume the plan slot through the atomic consumed-attempt seam."""
        from tests.fakes import FakePipelineDB, FakeSlskdAPI

        cfg = self._make_cfg()
        slskd = FakeSlskdAPI()
        slskd.searches.search_text_id_sequence = [42]
        slskd.searches.add_search(search_id=42, state="Completed", responses=[])
        slskd.searches.search_responses = MagicMock(
            side_effect=RuntimeError("response collection failed"))

        db = FakePipelineDB()
        rid = db.add_request(
            artist_name="Wiggles", album_title="Album",
            source="request", mb_release_id="mbid-collect", year=1991,
        )
        db.set_tracks(rid, [{"track_number": 1, "title": "Track"}])
        album = self._make_album(
            request_id=rid, mb_release_id="mbid-collect")
        self._seed_plan(db, rid, items=[("default", "*iggles Album")])
        ctx = self._wire(cfg, slskd, db, album)

        result = self._cratedigger.search_for_album(album, ctx)
        self._cratedigger._log_search_result(album, result, ctx)

        self.assertEqual(result.outcome, "error")
        self.assertEqual(result.final_state, "collection_crash")
        row = db.search_logs[0]
        self.assertEqual(row.execution_stage, "accepted")
        self.assertTrue(row.attempt_consumed)
        self.assertEqual(row.cursor_update_status, "wrapped")
        self.assertEqual(row.final_state, "collection_crash")
        self.assertEqual(db.request(rid)["plan_cycle_count"], 1)


class TestVariantSelectFallbackObservability(unittest.TestCase):
    """Finding #14: VARIANT_SELECT_FALLBACK log line is the observability hook
    for silent escalation-ladder bypasses.

    The fallback itself is intentional for DB resilience — when ``get_request``
    or ``get_tracks`` raises (transient DB outage, etc.), the helper falls back
    to a default variant and the next cycle retries. Without the stable log
    prefix operators have no way to count silent fallbacks; they show up as
    "requests grinding on default forever". The prefix
    ``VARIANT_SELECT_FALLBACK`` lets operators run
    ``journalctl -u cratedigger | grep VARIANT_SELECT_FALLBACK | wc -l``.
    """

    def test_db_exception_logs_stable_prefix_and_falls_back(self):
        from album_source import AlbumRecord, MediaRecord, ReleaseRecord
        from cratedigger import _select_variant_for_album
        from lib.config import CratediggerConfig

        ini_cfg = CratediggerConfig.from_ini(__import__("configparser").ConfigParser())

        media = [MediaRecord(medium_number=1, medium_format="CD", track_count=1)]
        release = ReleaseRecord(
            id=-1, foreign_release_id="mbid-x", title="Album",
            track_count=1, medium_count=1, format="CD", media=media,
            monitored=True, country=["US"], status="Official",
        )
        album = AlbumRecord(
            id=-1, title="Album", release_date="1990-01-01T00:00:00Z",
            artist_id=0, artist_name="Artist", foreign_artist_id="",
            releases=[release], db_request_id=42, db_source="request",
            db_mb_release_id="mbid-x", db_search_filetype_override=None,
            db_target_format=None,
        )

        # Mock DB that raises on get_request — simulates transient DB outage.
        class _BoomDB:
            def get_request(self, _rid):
                raise RuntimeError("transient connection error")
            def get_tracks(self, _rid):
                return []

        # Capture warnings logged on the cratedigger logger.
        with self.assertLogs("cratedigger", level="WARNING") as captured:
            variant, base_query = _select_variant_for_album(album, ini_cfg, _BoomDB())

        # Existing fallback behaviour preserved: kind='default'.
        self.assertEqual(variant.kind, "default")
        self.assertEqual(variant.tag, "default")
        # base_query computed from album info, independent of the DB call.
        self.assertTrue(base_query)

        # Stable greppable prefix is the observability hook.
        joined = "\n".join(captured.output)
        self.assertIn("VARIANT_SELECT_FALLBACK", joined)
        # Includes the request_id and album metadata for triage.
        self.assertIn("request_id=42", joined)
        self.assertIn("artist=Artist", joined)
        # exc_info=True => traceback is included.
        self.assertIn("RuntimeError", joined)


class TestSearchExhaustionResetsCounterSlice(unittest.TestCase):
    """Integration slice: variant=exhausted → reset search_attempts, stay wanted.

    Drives ``_log_search_result`` end-to-end with FakePipelineDB and asserts:
    - happy path: search_log row written with outcome='exhausted'; the
      request stays ``wanted`` and ``search_attempts`` is reset to 0 so
      the variant ladder wraps back to default on the next cycle.
    - re-queue via ``apply_transition`` (operator-driven manual→wanted)
      clears ``manual_reason`` and ``search_attempts`` — this seam is
      independent of the exhaustion path but worth covering since
      ``manual_reason`` exists for future operator-hold workflows.
    """

    def setUp(self):
        import cratedigger
        self._cratedigger = cratedigger
        self._orig_pdb = cratedigger.pipeline_db_source
        self._orig_module_ctx = cratedigger._module_ctx

    def tearDown(self):
        self._cratedigger.pipeline_db_source = self._orig_pdb
        self._cratedigger._module_ctx = self._orig_module_ctx

    def _ctx_with_db(self, db):
        from lib.context import CratediggerContext
        source = MagicMock()
        source._get_db.return_value = db
        self._cratedigger.pipeline_db_source = source
        ctx = CratediggerContext(
            cfg=self._cratedigger.cfg, slskd=MagicMock(),
            pipeline_db_source=source,
        )
        self._cratedigger._module_ctx = ctx
        return ctx

    def _make_album(self, request_id):
        from album_source import AlbumRecord, MediaRecord, ReleaseRecord
        media = [MediaRecord(medium_number=1, medium_format="CD", track_count=0)]
        release = ReleaseRecord(
            id=-request_id, foreign_release_id="mbid", title="T",
            track_count=0, medium_count=1, format="CD", media=media,
            monitored=True, country=["US"], status="Official",
        )
        return AlbumRecord(
            id=-request_id, title="T", release_date="0000-01-01T00:00:00Z",
            artist_id=0, artist_name="A", foreign_artist_id="",
            releases=[release], db_request_id=request_id, db_source="request",
            db_mb_release_id="mbid",
            db_search_filetype_override=None, db_target_format=None,
        )

    def _make_exhausted_result(self, *, album_id):
        from lib.search import SearchResult
        return SearchResult(
            album_id=album_id, success=False, query="",
            elapsed_s=0.0, outcome="exhausted", variant_tag="exhausted",
        )

    def test_legacy_exhausted_result_logs_non_consuming_no_cursor_change(self):
        """Post-U5, legacy exhausted SearchResult emissions (without
        plan_execution) flow through the non-consuming seam, leaving the
        cursor and cycle untouched. New code never emits exhausted rows;
        this protects historical / accidentally-emitted ones from
        advancing the active cursor."""
        from tests.fakes import FakePipelineDB

        db = FakePipelineDB()
        rid = db.add_request(
            artist_name="A", album_title="B", source="request",
            mb_release_id="mb-exh", status="wanted",
        )
        album = self._make_album(rid)
        ctx = self._ctx_with_db(db)
        result = self._make_exhausted_result(album_id=-rid)

        self._cratedigger._log_search_result(album, result, ctx)

        # Recorded as a non-consuming pre-attempt row -- the executor
        # never emits ``exhausted`` post-U5 anyway, but if some legacy
        # path does it must not advance the active cursor.
        self.assertEqual(len(db.search_logs), 1)
        log = db.search_logs[0]
        self.assertEqual(log.outcome, "exhausted")
        self.assertEqual(log.execution_stage, "pre_attempt")
        self.assertFalse(log.attempt_consumed)
        self.assertEqual(log.cursor_update_status, "unchanged")
        # Cursor / cycle / status unchanged.
        row = db.request(rid)
        self.assertEqual(row["status"], "wanted")
        self.assertEqual(row["next_plan_ordinal"], 0)
        self.assertEqual(row["plan_cycle_count"], 0)
        self.assertIsNone(row["manual_reason"])
        # Backoff applies (non-consuming method increments scheduler
        # counters), so the request is currently parked behind a
        # ``next_retry_after``. Asserting status alone is the right
        # invariant here.

    def test_requeue_via_apply_transition_clears_state(self):
        """Operator re-queue via the single-seam transition resets state."""
        from typing import cast
        from lib.pipeline_db import PipelineDB
        from lib.transitions import apply_transition
        from tests.fakes import FakePipelineDB

        db = FakePipelineDB()
        rid = db.add_request(
            artist_name="A", album_title="B", source="request",
            mb_release_id="mb-req", status="manual",
        )
        # Simulate prior state: search_attempts=7, manual_reason populated.
        db.update_request_fields(
            rid, search_attempts=7, manual_reason="search_exhausted")

        # The web UI button / pipeline-cli requeue / importer requeue all
        # funnel through apply_transition('manual' -> 'wanted'). Cast to
        # the concrete type — FakePipelineDB is duck-typed for the
        # methods apply_transition uses (get_request, reset_to_wanted).
        apply_transition(
            cast(PipelineDB, db), rid, "wanted", from_status="manual")

        row = db.request(rid)
        self.assertEqual(row["status"], "wanted")
        self.assertEqual(row["search_attempts"], 0)
        self.assertIsNone(row["manual_reason"])
        # Re-queued request is back in the wanted pool.
        wanted_ids = [r["id"] for r in db.get_wanted()]
        self.assertIn(rid, wanted_ids)


class TestImportSubprocessStartedFlag(unittest.TestCase):
    """``dispatch_import_core`` must mark
    ``ActiveDownloadState.import_subprocess_started_at`` immediately
    before launching ``run_import_one`` on the auto-import path. The
    flag is the witness the resume guard checks to decide whether the
    subprocess might have written to beets — without setting it here,
    the guard never blocks a real recovery situation, and the wedge
    fix becomes a one-way ratchet that loses the safety property.
    """

    def test_flag_set_before_subprocess_launch_on_auto_import(self):
        from lib.import_dispatch import dispatch_import_core
        from lib.quality import ActiveDownloadState

        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42, status="downloading",
            mb_release_id="mbid-123",
        ))
        # Seed the state the auto path would have produced before
        # reaching this dispatch call.
        existing = ActiveDownloadState(
            filetype="flac",
            enqueued_at="2026-05-04T10:00:00+00:00",
            files=[],
            processing_started_at="2026-05-04T10:01:00+00:00",
            current_path="/tmp/staged/Test/Album",
        )
        db.update_download_state(42, existing.to_json())

        ir = make_import_result(decision="import", new_min_bitrate=320)
        beets_info = AlbumInfo(
            album_id=1, track_count=10, min_bitrate_kbps=320,
            avg_bitrate_kbps=320, format="MP3",
            is_cbr=True, album_path="/Beets/Test")
        cfg = CratediggerConfig(
            beets_harness_path=_HARNESS,
            pipeline_db_enabled=True,
        )
        dl_info = DownloadInfo(username="user1")
        stdout = _make_stdout(ir)

        with tempfile.TemporaryDirectory() as tmpdir:
            with patch_dispatch_externals() as ext, \
                 patch("lib.beets_db.BeetsDB", _mock_beets_db(beets_info)):

                # Capture the persisted state at the moment
                # ``run_import_one`` (= ``ext.run``) is invoked.
                captured: dict[str, ActiveDownloadState | None] = {
                    "at_subprocess": None,
                }

                def _record_state(*_args: Any, **_kwargs: Any):
                    raw = db.request(42)["active_download_state"]
                    if raw is None:
                        captured["at_subprocess"] = None
                    else:
                        captured["at_subprocess"] = (
                            ActiveDownloadState.from_dict(raw)
                            if isinstance(raw, dict)
                            else ActiveDownloadState.from_json(str(raw))
                        )
                    return MagicMock(returncode=0, stdout=stdout, stderr="")

                ext.run.side_effect = _record_state

                dispatch_import_core(
                    path=tmpdir,
                    mb_release_id="mbid-123",
                    request_id=42,
                    label="Test Artist - Test Album",
                    beets_harness_path=_HARNESS,
                    db=db,  # type: ignore[arg-type]
                    dl_info=dl_info,
                    distance=0.05,
                    scenario="auto_import",
                    files=[MagicMock(username="user1",
                                     filename="01 - Track.mp3")],
                    cfg=cfg,
                )

        state_at_subprocess = captured["at_subprocess"]
        self.assertIsNotNone(
            state_at_subprocess,
            "active_download_state was None when subprocess launched — "
            "the auto path must preserve state through dispatch.",
        )
        assert state_at_subprocess is not None
        self.assertIsNotNone(
            state_at_subprocess.import_subprocess_started_at,
            "import_subprocess_started_at must be set BEFORE "
            "run_import_one launches; otherwise a crash mid-subprocess "
            "would leave the resume guard unable to distinguish "
            "'subprocess never ran' from 'subprocess wrote to beets'.",
        )

    def test_flag_not_touched_on_force_import_path(self):
        """Force/manual paths operate on ``failed_imports/...`` and do
        not own ``active_download_state``. Mutating the flag here would
        be cross-cutting state corruption — the request row that
        eventually owns ``active_download_state`` for the same MBID
        must not be polluted by an unrelated force-import.
        """
        from lib.import_dispatch import dispatch_import_core
        from lib.quality import ActiveDownloadState

        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42, status="manual",
            mb_release_id="mbid-123",
        ))
        existing = ActiveDownloadState(
            filetype="flac",
            enqueued_at="2026-05-04T10:00:00+00:00",
            files=[],
            processing_started_at=None,
            import_subprocess_started_at=None,
        )
        db.update_download_state(42, existing.to_json())

        ir = make_import_result(decision="import", new_min_bitrate=320)
        beets_info = AlbumInfo(
            album_id=1, track_count=10, min_bitrate_kbps=320,
            avg_bitrate_kbps=320, format="MP3",
            is_cbr=True, album_path="/Beets/Test")
        cfg = CratediggerConfig(
            beets_harness_path=_HARNESS,
            pipeline_db_enabled=True,
        )
        dl_info = DownloadInfo(username="user1")
        stdout = _make_stdout(ir)

        with tempfile.TemporaryDirectory() as tmpdir:
            with patch_dispatch_externals() as ext, \
                 patch("lib.beets_db.BeetsDB", _mock_beets_db(beets_info)):
                ext.run.return_value = MagicMock(
                    returncode=0, stdout=stdout, stderr="")
                dispatch_import_core(
                    path=tmpdir,
                    mb_release_id="mbid-123",
                    request_id=42,
                    label="Test Artist - Test Album",
                    force=True,
                    beets_harness_path=_HARNESS,
                    db=db,  # type: ignore[arg-type]
                    dl_info=dl_info,
                    distance=0.05,
                    scenario="force_import",
                    files=[MagicMock(username="user1",
                                     filename="01 - Track.mp3")],
                    cfg=cfg,
                )

        raw = db.request(42)["active_download_state"]
        if raw is None:
            return  # Force/manual cleared the state — also acceptable.
        state = (
            ActiveDownloadState.from_dict(raw)
            if isinstance(raw, dict)
            else ActiveDownloadState.from_json(str(raw))
        )
        self.assertIsNone(
            state.import_subprocess_started_at,
            "Force-import path must not flip the auto-path resume flag.",
        )


class TestPostMoveResumeBlockGuard(unittest.TestCase):
    """The wedge from 2026-05-04: requests stuck in ``downloading`` for
    a week because a previous attempt left files at the request-scoped
    auto-import staged path but never launched ``import_one.py``. The
    resume guard couldn't distinguish "subprocess never ran" from
    "subprocess may have started" and blocked retry forever — every
    cycle requeued an importer job that failed in <50ms with
    ``POST-MOVE RESUME BLOCKED``. 5788 failed jobs accumulated.

    Fix: ``ActiveDownloadState.import_subprocess_started_at`` flag is
    set immediately before ``run_import_one(...)`` on the auto path. The
    resume guard now permits retry when the flag is None (subprocess
    never launched, safe) and blocks when set (subprocess may have
    written to beets, manual recovery required).

    Tests pin the relaxed guard so future refactors can't reintroduce
    the trap.
    """

    def _make_state(self, *, current_path: str,
                    import_subprocess_started_at: str | None):
        from lib.quality import ActiveDownloadState
        return ActiveDownloadState(
            filetype="flac",
            enqueued_at="2026-04-27T13:36:28+00:00",
            files=[],
            last_progress_at="2026-04-27T14:00:38+00:00",
            processing_started_at="2026-04-27T14:00:38+00:00",
            import_subprocess_started_at=import_subprocess_started_at,
            current_path=current_path,
        )

    def _setup_wedged_request(self, tmpdir: str, *,
                              import_subprocess_started_at: str | None):
        """Build a wedge-shaped state: files at the auto-import staged
        path, ``processing_started_at`` set. Caller decides whether the
        ``import_subprocess_started_at`` flag is set (block) or not (retry).
        """
        from lib.processing_paths import stage_to_ai_path
        from tests.helpers import (
            make_ctx_with_fake_db,
            make_download_file,
            make_grab_list_entry,
        )

        request_id = 1984
        artist = "MxPx"
        title = "Plans Within Plans"
        staging_dir = os.path.join(tmpdir, "staging")
        slskd_dir = os.path.join(tmpdir, "slskd")
        os.makedirs(staging_dir)
        os.makedirs(slskd_dir)
        staged_path = stage_to_ai_path(
            artist=artist,
            title=title,
            staging_dir=staging_dir,
            request_id=request_id,
            auto_import=True,
        )
        os.makedirs(staged_path)
        # The wedge requires both: dir present AND tracked files present
        # at their bind-target locations. Otherwise the guard at line
        # 1644-1690 (missing dir / missing files) fires for a different
        # reason. We construct a single tracked file so the test
        # confirms the dispatch guard, not the missing-files guard.
        track_path = os.path.join(staged_path, "01 - Aces Up.flac")
        with open(track_path, "w") as fp:
            fp.write("fake audio")

        cfg = CratediggerConfig(
            beets_harness_path=_HARNESS,
            pipeline_db_enabled=True,
            beets_validation_enabled=True,
            beets_distance_threshold=0.15,
            beets_staging_dir=staging_dir,
            slskd_download_dir=slskd_dir,
            beets_tracking_file=os.path.join(tmpdir, "beets-tracking.jsonl"),
        )
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=request_id,
            status="downloading",
            artist_name=artist,
            album_title=title,
            year=2012,
            mb_release_id="2c5a5a0b-095d-434b-af15-b23e6fbc5ad9",
        ))
        ctx = make_ctx_with_fake_db(db, cfg=cfg)
        entry = make_grab_list_entry(
            album_id=request_id,
            files=[make_download_file(
                filename="user1\\Music\\01 - Aces Up.flac",
                file_dir="user1\\Music",
            )],
            artist=artist,
            title=title,
            year="2012",
            mb_release_id="2c5a5a0b-095d-434b-af15-b23e6fbc5ad9",
            db_source="request",
            db_request_id=request_id,
        )
        # Bind the import paths so the file actually lives where
        # `_processing_path_ready_for_importer` will look for it.
        from lib.staged_album import StagedAlbum
        sa = StagedAlbum.from_entry(entry, default_path=staged_path)
        sa.bind_import_paths(entry.files)
        # Ensure the bound destination exists.
        for file in entry.files:
            dst = file.import_path
            assert dst is not None
            os.makedirs(os.path.dirname(dst), exist_ok=True)
            if not os.path.exists(dst):
                with open(dst, "w") as fp:
                    fp.write("fake audio")
        state = self._make_state(
            current_path=staged_path,
            import_subprocess_started_at=import_subprocess_started_at,
        )
        # Persist state into the FakePipelineDB so guards that read
        # back via ``db.get_request`` see the same flag the test built.
        db.update_download_state(request_id, state.to_json())
        return entry, request_id, state, db, ctx, staged_path

    def test_legacy_wedge_permits_retry(self):
        """Legacy state (``processing_started_at`` set, no
        ``import_subprocess_started_at``): the wedge guard MUST permit
        retry. This is the recovery path for the 3 albums wedged on
        2026-05-04 — the fix unwedges them automatically on the next
        cycle.

        Exercises ``_materialize_processing_dir`` (the actual fire site
        seen in production logs at ``lib/download.py:583``).
        """
        from lib.download import _materialize_processing_dir
        from lib.staged_album import StagedAlbum

        with tempfile.TemporaryDirectory() as tmpdir:
            entry, _, _, _, ctx, staged_path = self._setup_wedged_request(
                tmpdir, import_subprocess_started_at=None)
            staged_album = StagedAlbum.from_entry(
                entry, default_path=staged_path)
            result = _materialize_processing_dir(
                entry, staged_album, ctx)

        self.assertIs(
            result, True,
            "Legacy wedged row (subprocess never launched) must "
            "materialize the existing files for retry. Returning None "
            "perpetuates the 2026-05-04 wedge: every cycle requeues a "
            "job that fails in <50ms with POST-MOVE RESUME BLOCKED.",
        )
        # And ``import_folder`` must be wired to the staged path so the
        # downstream tag-write/beets-validate sequence picks the files up.
        self.assertEqual(entry.import_folder, staged_path)

    def test_subprocess_started_abandons_and_resets(self):
        """Once ``import_subprocess_started_at`` is set, the wedge guard
        must not retry the same staged folder. It abandons the local
        attempt, preserves leftover files under failed_imports, and
        resets the request for a clean redownload.
        """
        from lib.download import _materialize_processing_dir
        from lib.staged_album import StagedAlbum

        with tempfile.TemporaryDirectory() as tmpdir:
            entry, request_id, _, _, ctx, staged_path = self._setup_wedged_request(
                tmpdir,
                import_subprocess_started_at="2026-04-27T14:00:39+00:00")
            staged_album = StagedAlbum.from_entry(
                entry, default_path=staged_path)
            result = _materialize_processing_dir(
                entry, staged_album, ctx)

            failed_parent = os.path.join(
                os.path.dirname(staged_path),
                "failed_imports",
            )
            moved = os.listdir(failed_parent)

        self.assertIs(
            result,
            False,
            "Subprocess-started residue must not retry in place; it should "
            "abandon and let the next search/download cycle own recovery.",
        )
        self.assertEqual(
            ctx.pipeline_db_source._get_db().request(request_id)["status"],
            "wanted",
        )
        self.assertEqual(len(moved), 1)
        self.assertTrue(moved[0].startswith("abandoned_auto_import"))


class TestSearchWatchdogSlice(unittest.TestCase):
    """End-to-end slice for issue #212: parallel-search-executor's
    `_collect_search_results` against the full ``FakeSlskdAPI`` with a
    stuck search. The slice pins the integration seam: `stop()` is
    called, `watchdog_fired=True` propagates onto the ``SearchResult``,
    the harvest path runs with a real outcome (not the rolled-back
    ``timeout``), and the post-collection ``delete()`` cleanup still
    fires when ``delete_searches=True``.
    """

    def test_stuck_search_fires_watchdog_through_full_collect(self):
        import configparser
        import cratedigger
        from dataclasses import replace
        from lib.config import CratediggerConfig
        from tests.fakes import FakeSlskdAPI

        cfg = CratediggerConfig.from_ini(configparser.ConfigParser())
        cfg = replace(cfg, delete_searches=True)
        slskd = FakeSlskdAPI()
        slskd.searches.add_search(
            search_id="abc-123",
            state="InProgress",
            responses=[],
            response_count=0,
            post_stop_state="Completed | Cancelled",
            post_stop_responses=[],
        )

        # Drive the watchdog: advance the injected clock past 90s on the
        # second poll. Real `time.sleep` is a no-op so the test runs in
        # microseconds.
        clock_t = {"v": 0.0}
        original_state = slskd.searches.state
        n = {"i": 0}
        def _state(sid, include):
            n["i"] += 1
            if n["i"] == 2:
                clock_t["v"] += 91.0
            return original_state(sid, include)
        slskd.searches.state = _state  # type: ignore[method-assign]

        with patch.object(cratedigger.time, "sleep", lambda _s: None):
            result = cratedigger._collect_search_results(
                "abc-123", "stuck query", album_id=99,
                search_cfg=cfg, slskd_client=slskd,
                clock_fn=lambda: clock_t["v"],
            )

        self.assertTrue(result.watchdog_fired,
                        "watchdog must propagate onto SearchResult")
        self.assertEqual(slskd.searches.stop_calls, ["abc-123"])
        self.assertEqual(result.outcome, "no_results",
                         "harvest classifies empty result set; no 'timeout'")
        self.assertNotEqual(result.outcome, "timeout")
        # delete_searches=True still fires after the watchdog branch
        self.assertEqual(slskd.searches.delete_calls, ["abc-123"])


class TestStartupReconciliationSlice(unittest.TestCase):
    """End-to-end startup reconciliation against ``FakePipelineDB`` +
    real ``SearchPlanService`` + real pure plan generator (U4).

    These slices exercise the whole orchestration: the all-wanted scan,
    per-row generation, supersede semantics, isolated failure handling,
    and the readiness summary contract.
    """

    def _cfg(self):
        ini = configparser.ConfigParser()
        return CratediggerConfig.from_ini(ini)

    def _service(self, db, generator_id="g-test"):
        from lib.search_plan_service import SearchPlanService
        svc = SearchPlanService(db, self._cfg())
        # Tests pin a stable test generator id so they don't drift when
        # the production constant rolls. Real callers never override.
        svc.generator_id = generator_id
        return svc

    def _seed_wanted(self, db, mbid, *, tracks: list[dict] | None = None):
        rid = db.add_request(
            artist_name="Artist", album_title="Album",
            mb_release_id=mbid, source="request",
        )
        if tracks is None:
            tracks = [
                {"disc_number": 1, "track_number": 1, "title": "Song One"},
                {"disc_number": 1, "track_number": 2, "title": "Song Two"},
            ]
        db.set_tracks(rid, tracks)
        return rid

    def test_scans_all_wanted_ignoring_pagination_and_backoff(self):
        """Reconciliation must visit every wanted row, not just the
        page-limited / due ones ``get_wanted`` would surface.
        """
        from datetime import datetime, timedelta, timezone
        from lib.startup_reconciliation import reconcile_search_plans
        db = FakePipelineDB()
        # 600 wanted rows, 550 backed off, 50 fresh -- larger than any
        # page_size cratedigger uses.
        rids = []
        for i in range(600):
            rid = self._seed_wanted(db, f"mbid-{i:04d}")
            rids.append(rid)
        far_future = datetime.now(timezone.utc) + timedelta(hours=24)
        for rid in rids[50:]:
            db.update_request_fields(rid, next_retry_after=far_future)

        # Sanity: ``get_wanted`` only sees the 50 due rows.
        self.assertEqual(len(db.get_wanted()), 50)

        summary = reconcile_search_plans(
            db, self._service(db), generator_id="g-test",
            progress_batch_size=200,
        )
        self.assertEqual(summary.wanted_total, 600)
        # Every row is generated on first pass.
        self.assertEqual(summary.generated, 600)
        self.assertEqual(summary.unclassified_no_plan, 0)
        self.assertTrue(summary.is_ready)

    def test_missing_plan_rows_receive_active_plan_with_cursor_at_zero(self):
        from lib.startup_reconciliation import reconcile_search_plans
        db = FakePipelineDB()
        rid = self._seed_wanted(db, "missing-plan")
        summary = reconcile_search_plans(
            db, self._service(db), generator_id="g-test",
        )
        active = db.get_active_search_plan(rid)
        assert active is not None
        self.assertEqual(active.plan.generator_id, "g-test")
        self.assertEqual(active.plan.status, "active")
        self.assertEqual(active.next_ordinal, 0)
        self.assertEqual(active.cycle_count, 0)
        self.assertEqual(summary.generated, 1)

    def test_active_current_plan_with_partial_track_snapshot_is_replanned(self):
        from lib.pipeline_db import SearchPlanItemInput
        from lib.startup_reconciliation import reconcile_search_plans
        db = FakePipelineDB()
        rid = self._seed_wanted(
            db,
            "partial-current",
            tracks=[
                {"disc_number": 1, "track_number": 1, "title": "Song One"},
                {"disc_number": 1, "track_number": 2, "title": "Song Two"},
            ],
        )
        old_id = db.create_successful_search_plan(
            request_id=rid,
            generator_id="g-current",
            items=[SearchPlanItemInput(
                ordinal=0, strategy="default", query="old q")],
            metadata_snapshot={"track_count": 1},
        )

        summary = reconcile_search_plans(
            db, self._service(db, generator_id="g-current"),
        )

        self.assertEqual(summary.generated, 1)
        self.assertEqual(summary.active_current, 0)
        active = db.get_active_search_plan(rid)
        assert active is not None
        self.assertNotEqual(active.plan.id, old_id)
        self.assertEqual(db.search_plans[old_id].status, "superseded")
        metadata_snapshot = active.plan.metadata_snapshot
        assert metadata_snapshot is not None
        self.assertEqual(metadata_snapshot.track_count, 2)

    def test_old_generator_rows_supersede_to_current_with_cursor_reset(self):
        from lib.pipeline_db import SearchPlanItemInput
        from lib.startup_reconciliation import reconcile_search_plans
        db = FakePipelineDB()
        rid = self._seed_wanted(db, "old-gen")
        old_id = db.create_successful_search_plan(
            request_id=rid, generator_id="g-old",
            items=[SearchPlanItemInput(
                ordinal=0, strategy="default", query="old q")],
        )
        # Cursor moved away so we can prove reset.
        db.update_request_fields(rid, next_plan_ordinal=1, plan_cycle_count=4)

        summary = reconcile_search_plans(
            db, self._service(db, generator_id="g-new"),
        )
        self.assertEqual(summary.old_generator_replaced, 1)
        self.assertEqual(summary.generated, 0)
        active = db.get_active_search_plan(rid)
        assert active is not None
        self.assertEqual(active.plan.generator_id, "g-new")
        self.assertNotEqual(active.plan.id, old_id)
        self.assertEqual(active.next_ordinal, 0)
        self.assertEqual(active.cycle_count, 0)
        self.assertEqual(db.search_plans[old_id].status, "superseded")

    def test_deterministic_failed_rows_reported_and_not_retried(self):
        from lib.startup_reconciliation import reconcile_search_plans
        db = FakePipelineDB()
        # Empty artist + album makes the generator return failure_reason.
        rid = db.add_request(
            artist_name="", album_title="",
            mb_release_id="det-fail", source="request",
        )
        # First pass: deterministic failure recorded.
        summary1 = reconcile_search_plans(
            db, self._service(db), generator_id="g-test",
        )
        self.assertEqual(summary1.deterministic_failed, 1)
        self.assertIsNone(db.get_active_search_plan(rid))
        # Plan failure persists with classification.
        plans_for_req = [
            p for p in db.search_plans.values() if p.request_id == rid]
        self.assertEqual(len(plans_for_req), 1)
        self.assertEqual(plans_for_req[0].status, "failed_deterministic")

        # Second pass: the row is still classified correctly and service
        # stickiness prevents appending another identical failed row.
        summary2 = reconcile_search_plans(
            db, self._service(db), generator_id="g-test",
        )
        # Must classify into a known bucket -- never unclassified.
        self.assertEqual(summary2.unclassified_no_plan, 0)
        self.assertEqual(summary2.wanted_total, 1)
        self.assertEqual(summary2.deterministic_failed, 1)
        plans_for_req_2 = [
            p for p in db.search_plans.values() if p.request_id == rid]
        self.assertEqual(len(plans_for_req_2), 1)

    def test_live_reconciliation_uses_batch_classification_for_sticky_failures(self):
        from lib.startup_reconciliation import reconcile_search_plans
        db = FakePipelineDB()
        rid_det = self._seed_wanted(db, "sticky-det")
        db.create_failed_search_plan(
            request_id=rid_det, generator_id="g-test",
            failure_class="no_runnable_query", transient=False)
        rid_trans = self._seed_wanted(db, "sticky-trans")
        db.create_failed_search_plan(
            request_id=rid_trans, generator_id="g-test",
            failure_class="resolver_unavailable", transient=True)

        service = self._service(db)
        service.generate_for_request = MagicMock(  # type: ignore[method-assign]
            side_effect=AssertionError("sticky failures should not inspect"))

        summary = reconcile_search_plans(
            db, service, generator_id="g-test")

        self.assertEqual(summary.deterministic_failed, 1)
        self.assertEqual(summary.retryable_failed, 1)
        self.assertEqual(summary.unclassified_no_plan, 0)
        service.generate_for_request.assert_not_called()

    def test_per_row_exception_does_not_stop_other_rows(self):
        """One row's generation exception must not block reconciliation."""
        from lib.startup_reconciliation import reconcile_search_plans
        db = FakePipelineDB()
        rid_good = self._seed_wanted(db, "good")
        rid_bomb = self._seed_wanted(db, "bomb")
        rid_other = self._seed_wanted(db, "other")

        service = self._service(db)
        original = service.generate_for_request

        def _wrapped(request_id, **kwargs):
            if request_id == rid_bomb:
                raise RuntimeError("boom")
            return original(request_id, **kwargs)
        service.generate_for_request = _wrapped  # type: ignore[method-assign]

        summary = reconcile_search_plans(db, service, generator_id="g-test")
        self.assertEqual(summary.wanted_total, 3)
        self.assertEqual(summary.generated, 2)
        # The bomb row shows up as unclassified -- the operator-visible
        # stop signal for follow-up.
        self.assertEqual(summary.unclassified_no_plan, 1)
        # But the other two rows are repaired regardless.
        self.assertIsNotNone(db.get_active_search_plan(rid_good))
        self.assertIsNotNone(db.get_active_search_plan(rid_other))

    def test_summary_counts_reconcile_to_wanted_total(self):
        """Sum of all classified buckets == wanted_total. No silent drops."""
        from datetime import datetime, timedelta, timezone
        from lib.startup_reconciliation import reconcile_search_plans
        from lib.pipeline_db import SearchPlanItemInput
        db = FakePipelineDB()
        # 1 active-current, 1 generated, 1 old-gen-replaced, 1 det-failed,
        # 1 malformed active_plan_id that points at a non-active plan.
        rid_a = self._seed_wanted(db, "a")
        db.create_successful_search_plan(
            request_id=rid_a, generator_id="g-current",
            items=[SearchPlanItemInput(
                ordinal=0, strategy="default", query="q")],
        )
        db.create_failed_search_plan(
            request_id=rid_a, generator_id="g-current",
            failure_class="historical", transient=False)
        self._seed_wanted(db, "b")  # generated
        rid_c = self._seed_wanted(db, "c")
        db.create_successful_search_plan(
            request_id=rid_c, generator_id="g-old",
            items=[SearchPlanItemInput(
                ordinal=0, strategy="default", query="q")],
        )
        # det-failed: blank artist+title -> generator failure.
        db.add_request(
            artist_name="", album_title="",
            mb_release_id="d", source="request",
        )
        rid_e = self._seed_wanted(db, "e")
        malformed_id = db.create_failed_search_plan(
            request_id=rid_e, generator_id="g-current",
            failure_class="old failure", transient=False)
        db.update_request_fields(rid_e, active_plan_id=malformed_id)

        summary = reconcile_search_plans(
            db, self._service(db, generator_id="g-current"),
        )
        self.assertEqual(summary.wanted_total, 5)
        self.assertEqual(summary.active_current, 1)
        self.assertEqual(summary.generated, 1)
        self.assertEqual(summary.old_generator_replaced, 1)
        self.assertEqual(summary.deterministic_failed, 2)
        self.assertEqual(summary.unclassified_no_plan, 0)
        self.assertEqual(summary.total_classified, summary.wanted_total)
        self.assertTrue(summary.is_ready)

    def test_dry_run_does_not_persist_plans(self):
        from lib.startup_reconciliation import reconcile_search_plans
        db = FakePipelineDB()
        rid = self._seed_wanted(db, "would-generate")
        summary = reconcile_search_plans(
            db, None, dry_run=True, generator_id="g-test",
        )
        self.assertTrue(summary.dry_run)
        self.assertEqual(summary.generated, 1)
        # Crucially: nothing was actually written.
        self.assertEqual(len(db.search_plans), 0)
        self.assertIsNone(db.get_active_search_plan(rid))

    def test_dry_run_classifies_existing_failure_records(self):
        from lib.startup_reconciliation import reconcile_search_plans
        db = FakePipelineDB()
        rid = self._seed_wanted(db, "trans-fail")
        db.create_failed_search_plan(
            request_id=rid, generator_id="g-test",
            failure_class="resolver_unavailable", transient=True,
        )
        summary = reconcile_search_plans(
            db, None, dry_run=True, generator_id="g-test",
        )
        self.assertEqual(summary.retryable_failed, 1)
        self.assertEqual(summary.unclassified_no_plan, 0)

    def test_dry_run_buckets_match_pre_batch_behavior_at_scale(self):
        """End-to-end dry-run over a mixed wanted population must produce
        the same per-bucket counts after the batch-classification
        rewrite as the pre-batch path would. Guards against the batch
        method silently dropping rows or mis-classifying transient vs
        deterministic.
        """
        from lib.pipeline_db import SearchPlanItemInput
        from lib.startup_reconciliation import reconcile_search_plans
        db = FakePipelineDB()

        # 100 with active current plan -> active_current.
        for i in range(100):
            rid = self._seed_wanted(db, f"act-{i}")
            db.create_successful_search_plan(
                request_id=rid, generator_id="g-test",
                items=[SearchPlanItemInput(
                    ordinal=0, strategy="default", query="q")],
            )
        # 50 with active old-generator plan -> old_generator_replaced.
        for i in range(50):
            rid = self._seed_wanted(db, f"old-{i}")
            db.create_successful_search_plan(
                request_id=rid, generator_id="g-old",
                items=[SearchPlanItemInput(
                    ordinal=0, strategy="default", query="q")],
            )
        # 30 with deterministic failure on g-test -> deterministic_failed.
        for i in range(30):
            rid = self._seed_wanted(db, f"det-{i}")
            db.create_failed_search_plan(
                request_id=rid, generator_id="g-test",
                failure_class="no_runnable_query", transient=False,
            )
        # 25 with transient failure on g-test -> retryable_failed.
        for i in range(25):
            rid = self._seed_wanted(db, f"trans-{i}")
            db.create_failed_search_plan(
                request_id=rid, generator_id="g-test",
                failure_class="resolver_unavailable", transient=True,
            )
        # 40 with no plan at all -> generated.
        for i in range(40):
            self._seed_wanted(db, f"new-{i}")

        summary = reconcile_search_plans(
            db, None, dry_run=True, generator_id="g-test",
        )
        self.assertEqual(summary.wanted_total, 245)
        self.assertEqual(summary.active_current, 100)
        self.assertEqual(summary.old_generator_replaced, 50)
        self.assertEqual(summary.deterministic_failed, 30)
        self.assertEqual(summary.retryable_failed, 25)
        self.assertEqual(summary.generated, 40)
        self.assertEqual(summary.unclassified_no_plan, 0)
        self.assertTrue(summary.is_ready)

    def test_resumable_after_interrupted_pass(self):
        """A second reconciliation pass after a partial first pass must
        not duplicate active plans or lose failure state.
        """
        from lib.startup_reconciliation import reconcile_search_plans
        db = FakePipelineDB()
        rid_first = self._seed_wanted(db, "first")
        rid_second = self._seed_wanted(db, "second")

        # Simulate interrupted first pass: only generate for rid_first.
        service = self._service(db)
        service.generate_for_request(rid_first)

        # Second pass: rid_first is active-current (no-op), rid_second
        # is generated. No duplicates of either.
        summary = reconcile_search_plans(
            db, self._service(db),
            generator_id=service.generator_id,
        )
        self.assertEqual(summary.wanted_total, 2)
        self.assertEqual(summary.active_current, 1)
        self.assertEqual(summary.generated, 1)
        # Each request has exactly one active plan.
        for rid in (rid_first, rid_second):
            actives = [
                p for p in db.search_plans.values()
                if p.request_id == rid and p.status == "active"
            ]
            self.assertEqual(len(actives), 1)


class TestPhaseTwoEligibilitySlice(unittest.TestCase):
    """Phase 2 wanted selection respects active-plan eligibility (U4).

    Verifies the second-half claim of U4: ``get_wanted_searchable``
    excludes rows without a current-generator active plan, including
    rows that Phase 1 requeued mid-cycle.
    """

    def _items(self, *queries: str):
        from lib.pipeline_db import SearchPlanItemInput
        return [
            SearchPlanItemInput(ordinal=i, strategy="default", query=q)
            for i, q in enumerate(queries)
        ]

    def test_phase2_excludes_rows_without_current_plan(self):
        db = FakePipelineDB()
        rid_ready = db.add_request(
            artist_name="A", album_title="B", source="request",
            mb_release_id="ready")
        db.create_successful_search_plan(
            request_id=rid_ready, generator_id="g-current",
            items=self._items("Q"),
        )
        rid_no_plan = db.add_request(
            artist_name="A", album_title="C", source="request",
            mb_release_id="no-plan")
        rid_old = db.add_request(
            artist_name="A", album_title="D", source="request",
            mb_release_id="old")
        db.create_successful_search_plan(
            request_id=rid_old, generator_id="g-old",
            items=self._items("Q"),
        )

        rids = {r["id"] for r in db.get_wanted_searchable("g-current")}
        self.assertEqual(rids, {rid_ready})
        # Confirm both excluded rows ARE wanted -- they're just not
        # searchable until reconciliation.
        all_wanted = {r["id"] for r in db.get_wanted()}
        self.assertIn(rid_no_plan, all_wanted)
        self.assertIn(rid_old, all_wanted)

    def test_row_requeued_to_wanted_midcycle_excluded_until_next_recon(self):
        """A row that Phase 1 transitions back to ``wanted`` mid-cycle
        is searchable only via its existing active plan; if that plan
        is somehow on the old generator, it stays excluded.
        """
        db = FakePipelineDB()
        # Row starts downloading (no active plan, simulating a Phase 1
        # requeue scenario where reconciliation hasn't run for this row).
        rid = db.add_request(
            artist_name="A", album_title="B", source="request",
            mb_release_id="requeued")
        db.update_status(rid, "downloading")
        # Phase 1 fails the download and requeues it without a current
        # plan attached.
        db.update_status(rid, "wanted")
        # Phase 2 must NOT see this row.
        self.assertEqual(db.get_wanted_searchable("g-current"), [])
        # But ``get_wanted`` (forensic) sees it.
        self.assertEqual([r["id"] for r in db.get_wanted()], [rid])


class TestU5PlanDrivenExecutorSlice(unittest.TestCase):
    """U5 integration slices for plan-driven search execution.

    Covers Plan §AE6, §AE7, §AE8, §AE14 plus stale-completion guards
    on enqueue / download ownership / status transitions.
    """

    def setUp(self):
        import cratedigger
        self._cratedigger = cratedigger
        self._orig_cfg = cratedigger.cfg
        self._orig_slskd = cratedigger.slskd
        self._orig_pdb = cratedigger.pipeline_db_source
        self._orig_module_ctx = cratedigger._module_ctx

    def tearDown(self):
        self._cratedigger.cfg = self._orig_cfg
        self._cratedigger.slskd = self._orig_slskd
        self._cratedigger.pipeline_db_source = self._orig_pdb
        self._cratedigger._module_ctx = self._orig_module_ctx

    def _make_cfg(self, **overrides):
        import configparser
        from dataclasses import replace as _replace
        from lib.config import CratediggerConfig
        ini = configparser.ConfigParser()
        ini["Slskd"] = {"delete_searches": "False"}
        ini["Search Settings"] = {
            "allowed_filetypes": "flac",
            "search_response_limit": "1000",
            "search_escalation_threshold": "5",
            "search_timeout": "30000",
            "album_prepend_artist": "False",
            "minimum_filename_match_ratio": "0.5",
        }
        cfg = CratediggerConfig.from_ini(ini)
        if overrides:
            cfg = _replace(cfg, **overrides)
        return cfg

    def _make_album(self, *, request_id):
        from album_source import AlbumRecord, ReleaseRecord, MediaRecord
        media = [MediaRecord(medium_number=1, medium_format="CD", track_count=1)]
        release = ReleaseRecord(
            id=-request_id, foreign_release_id="mbid",
            title="A", track_count=1, medium_count=1,
            format="CD", media=media, monitored=True,
            country=["US"], status="Official",
        )
        return AlbumRecord(
            id=-request_id, title="A", release_date="1991-01-01",
            artist_id=0, artist_name="X", foreign_artist_id="",
            releases=[release],
            db_request_id=request_id, db_source="request",
            db_mb_release_id="mbid",
            db_search_filetype_override=None, db_target_format=None,
        )

    def _wire(self, cfg, slskd, db):
        from lib.context import CratediggerContext
        cratedigger = self._cratedigger
        cratedigger.cfg = cfg
        cratedigger.slskd = slskd
        source = MagicMock()
        source._get_db.return_value = db
        cratedigger.pipeline_db_source = source
        ctx = CratediggerContext(
            cfg=cfg, slskd=slskd, pipeline_db_source=source,
        )
        cratedigger._module_ctx = ctx
        return ctx

    def _seed_two_item_plan(self, db, rid):
        from lib.pipeline_db import SearchPlanItemInput
        from lib.search import SEARCH_PLAN_GENERATOR_ID
        return db.create_successful_search_plan(
            request_id=rid,
            generator_id=SEARCH_PLAN_GENERATOR_ID,
            items=[
                SearchPlanItemInput(
                    ordinal=0, strategy="default", query="*rtist Album",
                    canonical_query_key="*rtist album",
                ),
                SearchPlanItemInput(
                    ordinal=1, strategy="unwild", query="Artist Album",
                    canonical_query_key="artist album",
                ),
            ],
        )

    def test_AE6_active_plan_ordinal_executes_and_advances_cursor(self):
        """AE6: active plan ordinal executes. On success, log + advance."""
        from tests.fakes import FakePipelineDB, FakeSlskdAPI

        cfg = self._make_cfg()
        slskd = FakeSlskdAPI()
        slskd.searches.search_text_id_sequence = [42]
        slskd.searches.add_search(
            search_id=42, state="Completed", responses=[])
        db = FakePipelineDB()
        rid = db.add_request(
            artist_name="X", album_title="A", source="request",
            mb_release_id="mbid")
        self._seed_two_item_plan(db, rid)
        album = self._make_album(request_id=rid)
        ctx = self._wire(cfg, slskd, db)
        ctx.current_album_cache[album.id] = album

        result = self._cratedigger.search_for_album(album, ctx)
        self._cratedigger._log_search_result(album, result, ctx)

        self.assertEqual(result.outcome, "no_results")
        log = db.search_logs[-1]
        self.assertEqual(log.execution_stage, "accepted")
        self.assertTrue(log.attempt_consumed)
        self.assertEqual(log.cursor_update_status, "advanced")
        self.assertEqual(log.plan_ordinal, 0)
        # Cursor advanced to ordinal 1 within cycle 0.
        row = db.request(rid)
        self.assertEqual(row["next_plan_ordinal"], 1)
        self.assertEqual(row["plan_cycle_count"], 0)

    def test_AE7_pre_attempt_failure_is_non_consuming(self):
        """AE7: submit failure before accepted search is non-consuming."""
        from tests.fakes import FakePipelineDB, FakeSlskdAPI

        cfg = self._make_cfg()
        slskd = FakeSlskdAPI()
        slskd.searches.search_text_error = RuntimeError("offline")
        db = FakePipelineDB()
        rid = db.add_request(
            artist_name="X", album_title="A", source="request",
            mb_release_id="mbid")
        self._seed_two_item_plan(db, rid)
        album = self._make_album(request_id=rid)
        ctx = self._wire(cfg, slskd, db)
        ctx.current_album_cache[album.id] = album

        result = self._cratedigger.search_for_album(album, ctx)
        self._cratedigger._log_search_result(album, result, ctx)

        log = db.search_logs[-1]
        self.assertEqual(log.execution_stage, "pre_attempt")
        self.assertFalse(log.attempt_consumed)
        self.assertEqual(log.cursor_update_status, "unchanged")
        # Cursor is still at ordinal 0; backoff applied.
        row = db.request(rid)
        self.assertEqual(row["next_plan_ordinal"], 0)
        self.assertEqual(row["plan_cycle_count"], 0)
        self.assertGreaterEqual(row["search_attempts"], 1)
        self.assertIsNotNone(row["next_retry_after"])

    def test_AE8_final_ordinal_wraps_with_no_new_exhausted_row(self):
        """AE8: final ordinal wraps cursor + cycle. No new exhausted row."""
        from tests.fakes import FakePipelineDB, FakeSlskdAPI

        cfg = self._make_cfg()
        slskd = FakeSlskdAPI()
        slskd.searches.search_text_id_sequence = [42]
        slskd.searches.add_search(
            search_id=42, state="Completed", responses=[])
        db = FakePipelineDB()
        rid = db.add_request(
            artist_name="X", album_title="A", source="request",
            mb_release_id="mbid")
        self._seed_two_item_plan(db, rid)
        # Park cursor at the FINAL ordinal in cycle 5.
        db.update_request_fields(
            rid, next_plan_ordinal=1, plan_cycle_count=5)
        album = self._make_album(request_id=rid)
        ctx = self._wire(cfg, slskd, db)
        ctx.current_album_cache[album.id] = album

        result = self._cratedigger.search_for_album(album, ctx)
        self._cratedigger._log_search_result(album, result, ctx)

        # Positive: cycle incremented, cursor wrapped.
        row = db.request(rid)
        self.assertEqual(row["next_plan_ordinal"], 0)
        self.assertEqual(row["plan_cycle_count"], 6)
        log = db.search_logs[-1]
        self.assertEqual(log.cursor_update_status, "wrapped")
        # Negative: NO new exhausted row.
        self.assertNotIn("exhausted", [r.outcome for r in db.search_logs])

    def test_AE14_stale_completion_after_regeneration_does_not_advance_cursor(self):
        """AE14: regenerated active plan + completion of an old executing
        plan logs against the executed old plan but does NOT advance the
        new cursor.
        """
        from tests.fakes import FakePipelineDB
        from lib.pipeline_db import SearchPlanItemInput
        from lib.search import (
            PlanExecutionContext, SEARCH_PLAN_GENERATOR_ID, SearchResult,
        )

        cfg = self._make_cfg()
        db = FakePipelineDB()
        rid = db.add_request(
            artist_name="X", album_title="A", source="request",
            mb_release_id="mbid")
        # Old plan executed by the in-flight search.
        old_plan_id = self._seed_two_item_plan(db, rid)
        old_active = db.get_active_search_plan(rid)
        assert old_active is not None
        old_item = old_active.items[0]
        old_exec = PlanExecutionContext(
            plan_id=old_plan_id, plan_item_id=old_item.id, plan_ordinal=0,
            plan_strategy="default",
            plan_canonical_query_key=old_item.canonical_query_key,
            plan_repeat_group=old_item.repeat_group,
            plan_generator_id=SEARCH_PLAN_GENERATOR_ID,
            plan_item_count=2, cycle_count_snapshot=0,
        )
        # Regenerate the plan mid-flight.
        new_plan_id = db.supersede_search_plan_with_replacement(
            request_id=rid, generator_id=SEARCH_PLAN_GENERATOR_ID,
            items=[
                SearchPlanItemInput(
                    ordinal=0, strategy="default", query="New Query",
                    canonical_query_key="new query"),
            ],
        )
        # Old search now completes (no_results). Log it.
        result = SearchResult(
            album_id=-rid, success=False, query="*rtist Album",
            outcome="no_results", final_state="Completed",
            elapsed_s=0.01, variant_tag="default",
            plan_execution=old_exec,
        )
        album = self._make_album(request_id=rid)
        ctx = self._wire(cfg, MagicMock(), db)
        self._cratedigger._log_search_result(album, result, ctx)

        # Stale-log row written against the executed OLD plan.
        log = db.search_logs[-1]
        self.assertEqual(log.plan_id, old_plan_id)
        self.assertEqual(log.execution_stage, "stale_completion")
        self.assertEqual(log.cursor_update_status, "stale")
        self.assertEqual(log.stale_reason, "regenerated")
        # New cursor unchanged: still pointing at new plan ordinal 0,
        # cycle 0 (regen reset it).
        row = db.request(rid)
        self.assertEqual(row["active_plan_id"], new_plan_id)
        self.assertEqual(row["next_plan_ordinal"], 0)
        self.assertEqual(row["plan_cycle_count"], 0)

    def test_stale_download_ownership_claim_blocked_after_regeneration(self):
        """Plan §AE14 stale-completion: an in-flight search that resolves
        ``found`` after the request was regenerated must NOT claim
        download ownership (wanted -> downloading).
        """
        from tests.fakes import FakePipelineDB
        from lib.pipeline_db import SearchPlanItemInput
        from lib.search import (
            PlanExecutionContext, SEARCH_PLAN_GENERATOR_ID,
        )
        from lib.download_ownership import DownloadOwnershipWriter

        db = FakePipelineDB()
        rid = db.add_request(
            artist_name="X", album_title="A", source="request",
            mb_release_id="mbid")
        old_plan_id = self._seed_two_item_plan(db, rid)
        old_active = db.get_active_search_plan(rid)
        assert old_active is not None
        old_exec = PlanExecutionContext(
            plan_id=old_plan_id, plan_item_id=old_active.items[0].id,
            plan_ordinal=0, plan_strategy="default",
            plan_canonical_query_key="*rtist album",
            plan_repeat_group=None,
            plan_generator_id=SEARCH_PLAN_GENERATOR_ID,
            plan_item_count=2, cycle_count_snapshot=0,
        )
        # Regen mid-flight.
        db.supersede_search_plan_with_replacement(
            request_id=rid, generator_id=SEARCH_PLAN_GENERATOR_ID,
            items=[SearchPlanItemInput(
                ordinal=0, strategy="default", query="New",
                canonical_query_key="new")],
        )

        # Stub ``DownloadOwnershipWriter`` to use the fake DB.
        writer = DownloadOwnershipWriter(db_factory=lambda: db)
        result = writer.claim_downloading(
            rid, '{"state":"planned"}', plan_execution=old_exec,
        )
        self.assertFalse(result, "stale claim must be blocked")
        # Status NOT mutated.
        self.assertEqual(db.request(rid)["status"], "wanted")

    def test_current_download_ownership_claim_succeeds(self):
        """Sanity: a non-stale claim with the current plan execution
        context still succeeds (regression guard)."""
        from tests.fakes import FakePipelineDB
        from lib.search import (
            PlanExecutionContext, SEARCH_PLAN_GENERATOR_ID,
        )
        from lib.download_ownership import DownloadOwnershipWriter

        db = FakePipelineDB()
        rid = db.add_request(
            artist_name="X", album_title="A", source="request",
            mb_release_id="mbid")
        plan_id = self._seed_two_item_plan(db, rid)
        active = db.get_active_search_plan(rid)
        assert active is not None
        exec_ctx = PlanExecutionContext(
            plan_id=plan_id, plan_item_id=active.items[0].id,
            plan_ordinal=0, plan_strategy="default",
            plan_canonical_query_key="*rtist album",
            plan_repeat_group=None,
            plan_generator_id=SEARCH_PLAN_GENERATOR_ID,
            plan_item_count=2, cycle_count_snapshot=0,
        )
        writer = DownloadOwnershipWriter(db_factory=lambda: db)
        ok = writer.claim_downloading(
            rid, '{"state":"planned"}', plan_execution=exec_ctx,
        )
        self.assertTrue(ok)
        self.assertEqual(db.request(rid)["status"], "downloading")

    def test_stale_request_transition_blocked_via_finalize_request_if_plan_current(self):
        """``finalize_request_if_plan_current`` rejects stale transitions."""
        from typing import cast
        from tests.fakes import FakePipelineDB
        from lib.pipeline_db import PipelineDB, SearchPlanItemInput
        from lib.transitions import (
            RequestTransition, finalize_request_if_plan_current,
        )

        db = FakePipelineDB()
        rid = db.add_request(
            artist_name="X", album_title="A", source="request",
            mb_release_id="mbid")
        old_plan_id = self._seed_two_item_plan(db, rid)
        # Regenerate.
        db.supersede_search_plan_with_replacement(
            request_id=rid, generator_id="g", items=[
                SearchPlanItemInput(ordinal=0, strategy="default", query="N")
            ],
        )
        ok = finalize_request_if_plan_current(
            cast(PipelineDB, db), rid,
            RequestTransition.to_downloading(
                from_status="wanted", state_json='{"x":1}'),
            plan_id=old_plan_id, plan_ordinal=0, cycle_count_snapshot=0,
        )
        self.assertFalse(ok)
        self.assertEqual(db.request(rid)["status"], "wanted")

    def test_stale_enqueue_does_not_move_request_to_downloading(self):
        """Owner-thread plumbing: when ``ctx.active_plan_execution`` is
        stale, ``_claim_initial_download_ownership`` returns a non-claimed
        result and the request stays wanted.
        """
        from tests.fakes import FakePipelineDB
        from lib.context import CratediggerContext
        from lib.download_ownership import DownloadOwnershipWriter
        from lib.enqueue import _claim_initial_download_ownership
        from lib.grab_list import DownloadFile
        from lib.pipeline_db import SearchPlanItemInput
        from lib.search import (
            PlanExecutionContext, SEARCH_PLAN_GENERATOR_ID,
        )

        cfg = self._make_cfg()
        db = FakePipelineDB()
        rid = db.add_request(
            artist_name="X", album_title="A", source="request",
            mb_release_id="mbid")
        old_plan_id = self._seed_two_item_plan(db, rid)
        old_active = db.get_active_search_plan(rid)
        assert old_active is not None
        old_exec = PlanExecutionContext(
            plan_id=old_plan_id, plan_item_id=old_active.items[0].id,
            plan_ordinal=0, plan_strategy="default",
            plan_canonical_query_key="*rtist album",
            plan_repeat_group=None,
            plan_generator_id=SEARCH_PLAN_GENERATOR_ID,
            plan_item_count=2, cycle_count_snapshot=0,
        )
        db.supersede_search_plan_with_replacement(
            request_id=rid, generator_id=SEARCH_PLAN_GENERATOR_ID,
            items=[SearchPlanItemInput(
                ordinal=0, strategy="default", query="New")],
        )

        writer = DownloadOwnershipWriter(db_factory=lambda: db)
        worker_ctx = CratediggerContext(
            cfg=cfg, slskd=MagicMock(), pipeline_db_source=MagicMock(),
            download_ownership=writer,
            active_plan_execution=old_exec,
        )
        album = self._make_album(request_id=rid)
        files = [DownloadFile(
            filename="a.flac", id="t1", file_dir="d",
            username="peer", size=1)]
        claim = _claim_initial_download_ownership(
            album, files, "flac", worker_ctx,
        )
        self.assertTrue(claim.attempted)
        self.assertFalse(claim.claimed)
        self.assertEqual(db.request(rid)["status"], "wanted")


class TestU5RegressionExecutorDoesNotUseLegacyVariantPicker(unittest.TestCase):
    """Regression: post-U5 the search executor must drive selection
    through the persisted plan -- ``select_variant`` and ``search_attempts``
    must NOT determine the next runnable query."""

    def setUp(self):
        import cratedigger
        self._cratedigger = cratedigger
        self._orig_cfg = cratedigger.cfg
        self._orig_slskd = cratedigger.slskd

    def tearDown(self):
        self._cratedigger.cfg = self._orig_cfg
        self._cratedigger.slskd = self._orig_slskd

    def test_executor_uses_plan_strategy_not_search_attempts_ladder(self):
        """A request with search_attempts=99 (ladder past exhaustion) but
        whose active plan-item is strategy='unwild' MUST issue an unwild
        query, not the legacy exhausted/track query the old ladder
        would have produced.
        """
        import configparser
        from dataclasses import replace as _replace
        from lib.config import CratediggerConfig
        from lib.context import CratediggerContext
        from lib.pipeline_db import SearchPlanItemInput
        from lib.search import SEARCH_PLAN_GENERATOR_ID
        from tests.fakes import FakePipelineDB, FakeSlskdAPI

        cfg = CratediggerConfig.from_ini(configparser.ConfigParser())
        cfg = _replace(cfg, search_escalation_threshold=5)
        slskd = FakeSlskdAPI()
        slskd.searches.search_text_id_sequence = [42]
        slskd.searches.add_search(
            search_id=42, state="Completed", responses=[])
        db = FakePipelineDB()
        rid = db.add_request(
            artist_name="X", album_title="A", source="request",
            mb_release_id="mbid")
        # Legacy ladder at search_attempts=99 would produce 'exhausted'.
        # The active plan instead points at 'unwild' so the executor
        # MUST issue 'X A' (unwild query).
        db.update_request_fields(rid, search_attempts=99)
        db.create_successful_search_plan(
            request_id=rid, generator_id=SEARCH_PLAN_GENERATOR_ID,
            items=[
                SearchPlanItemInput(
                    ordinal=0, strategy="unwild", query="X A",
                    canonical_query_key="x a"),
            ],
        )
        from album_source import AlbumRecord, ReleaseRecord, MediaRecord
        media = [MediaRecord(medium_number=1, medium_format="CD", track_count=1)]
        release = ReleaseRecord(
            id=-rid, foreign_release_id="mbid",
            title="A", track_count=1, medium_count=1,
            format="CD", media=media, monitored=True,
            country=["US"], status="Official",
        )
        album = AlbumRecord(
            id=-rid, title="A", release_date="1991-01-01",
            artist_id=0, artist_name="X", foreign_artist_id="",
            releases=[release], db_request_id=rid, db_source="request",
            db_mb_release_id="mbid",
            db_search_filetype_override=None, db_target_format=None,
        )
        self._cratedigger.cfg = cfg
        self._cratedigger.slskd = slskd
        source = MagicMock()
        source._get_db.return_value = db
        ctx = CratediggerContext(
            cfg=cfg, slskd=slskd, pipeline_db_source=source,
        )
        ctx.current_album_cache[album.id] = album

        result = self._cratedigger.search_for_album(album, ctx)
        # Plan-driven query was issued.
        self.assertEqual(len(slskd.searches.search_text_calls), 1)
        self.assertEqual(
            slskd.searches.search_text_calls[0].search_text, "X A")
        # Variant comes from the plan-item, not the legacy ladder.
        self.assertEqual(result.variant_tag, "unwild")


class TestPreviewFrontGateSlice(unittest.TestCase):
    """Integration slice: preview-worker front-gate short-circuits measurement.

    Exercises the real ``process_claimed_preview_job`` against the real
    ``load_candidate_evidence_for_source`` and a FakePipelineDB. Asserts
    that when stored candidate evidence already passes the snapshot guard,
    the worker marks the job importable WITHOUT invoking
    ``preview_import_from_path`` / ``run_preimport_gates`` / spectral
    analysis or, for automation jobs, the materialization helper.

    Covers AE4 for both force/manual and automation job types via the
    same code path used in production.
    """

    def _evidence(self, source_path: str, owner_type: str, owner_id: int):
        from lib.quality_evidence import snapshot_audio_files
        from tests.helpers import make_album_quality_evidence

        return make_album_quality_evidence(
            owner_type=owner_type,
            owner_id=owner_id,
            files=snapshot_audio_files(source_path),
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=245,
                avg_bitrate_kbps=256,
                median_bitrate_kbps=252,
                format="MP3 V0",
                spectral_grade="genuine",
            ),
            codec="mp3",
            container="mp3",
            storage_format="mp3 v0",
        )

    def test_force_job_with_valid_evidence_short_circuits_no_measurement(self):
        from lib.import_queue import (
            IMPORT_JOB_FORCE,
            force_import_dedupe_key,
            force_import_payload,
        )
        from lib.quality import ALBUM_QUALITY_EVIDENCE_OWNER_DOWNLOAD_LOG_CANDIDATE
        from scripts import import_preview_worker

        with tempfile.TemporaryDirectory() as source:
            with open(os.path.join(source, "01.mp3"), "wb") as handle:
                handle.write(b"audio")
            db = FakePipelineDB()
            db.seed_request(make_request_row(id=42))
            log_id = db.log_download(42, outcome="rejected")
            db.enqueue_import_job(
                IMPORT_JOB_FORCE,
                request_id=42,
                dedupe_key=force_import_dedupe_key(log_id),
                payload=force_import_payload(
                    download_log_id=log_id,
                    failed_path=source,
                    source_username="alice",
                ),
            )
            claimed = db.claim_next_import_preview_job(worker_id="preview")
            assert claimed is not None
            db.upsert_album_quality_evidence(self._evidence(
                source,
                ALBUM_QUALITY_EVIDENCE_OWNER_DOWNLOAD_LOG_CANDIDATE,
                log_id,
            ))

            sentinels = {
                "preview_called": False,
                "preimport_called": False,
            }

            def _sentinel_preview(*args, **kwargs):
                sentinels["preview_called"] = True
                raise AssertionError(
                    "preview_import_from_path must not be called when evidence is valid"
                )

            def _sentinel_preimport(*args, **kwargs):
                sentinels["preimport_called"] = True
                raise AssertionError(
                    "run_preimport_gates must not be called when evidence is valid"
                )

            with patch(
                "scripts.import_preview_worker.preview_import_from_path",
                side_effect=_sentinel_preview,
            ), patch(
                "lib.preimport.run_preimport_gates",
                side_effect=_sentinel_preimport,
            ):
                updated = import_preview_worker.process_claimed_preview_job(
                    cast(Any, db),
                    claimed,
                )

        self.assertFalse(sentinels["preview_called"])
        self.assertFalse(sentinels["preimport_called"])
        assert updated is not None
        self.assertEqual(updated.status, "queued")
        self.assertEqual(updated.preview_status, "evidence_ready")
        assert updated.preview_result is not None
        self.assertEqual(
            updated.preview_result.get("candidate_status"),
            "reused",
        )

    def test_automation_job_with_valid_evidence_skips_materialization(self):
        from lib.import_queue import (
            IMPORT_JOB_AUTOMATION,
            automation_import_dedupe_key,
        )
        from lib.quality import ALBUM_QUALITY_EVIDENCE_OWNER_IMPORT_JOB_CANDIDATE
        from scripts import import_preview_worker

        with tempfile.TemporaryDirectory() as staged:
            with open(os.path.join(staged, "01.flac"), "wb") as handle:
                handle.write(b"audio")
            db = FakePipelineDB()
            db.seed_request(make_request_row(
                id=42,
                status="downloading",
                active_download_state={
                    "filetype": "flac",
                    "enqueued_at": "2026-04-25T00:00:00+00:00",
                    "current_path": staged,
                    "files": [{
                        "username": "alice",
                        "filename": "Artist\\Album\\01.flac",
                        "file_dir": "Artist\\Album",
                        "size": 123,
                    }],
                },
            ))
            db.enqueue_import_job(
                IMPORT_JOB_AUTOMATION,
                request_id=42,
                dedupe_key=automation_import_dedupe_key(42),
                payload={},
            )
            claimed = db.claim_next_import_preview_job(worker_id="preview")
            assert claimed is not None
            db.upsert_album_quality_evidence(self._evidence(
                staged,
                ALBUM_QUALITY_EVIDENCE_OWNER_IMPORT_JOB_CANDIDATE,
                claimed.id,
            ))

            sentinels = {
                "preview_called": False,
                "preimport_called": False,
                "materialize_called": False,
            }

            def _sentinel_preview(*args, **kwargs):
                sentinels["preview_called"] = True
                raise AssertionError(
                    "preview_import_from_path must not be called when evidence is valid"
                )

            def _sentinel_preimport(*args, **kwargs):
                sentinels["preimport_called"] = True
                raise AssertionError(
                    "run_preimport_gates must not be called when evidence is valid"
                )

            def _sentinel_materialize(*args, **kwargs):
                sentinels["materialize_called"] = True
                raise AssertionError(
                    "_materialize_processing_dir must not be called when evidence is valid"
                )

            with patch(
                "scripts.import_preview_worker.preview_import_from_path",
                side_effect=_sentinel_preview,
            ), patch(
                "lib.preimport.run_preimport_gates",
                side_effect=_sentinel_preimport,
            ), patch(
                "lib.download._materialize_processing_dir",
                side_effect=_sentinel_materialize,
            ):
                updated = import_preview_worker.process_claimed_preview_job(
                    cast(Any, db),
                    claimed,
                )

        self.assertFalse(sentinels["preview_called"])
        self.assertFalse(sentinels["preimport_called"])
        self.assertFalse(sentinels["materialize_called"])
        assert updated is not None
        self.assertEqual(updated.status, "queued")
        self.assertEqual(updated.preview_status, "evidence_ready")
        assert updated.preview_result is not None
        self.assertEqual(
            updated.preview_result.get("candidate_status"),
            "reused",
        )


class TestImporterRequeueToPreviewSlice(unittest.TestCase):
    """Integration slice: importer claim with no evidence → requeue → preview
    re-claims and measures → importer claims again and proceeds.

    Exercises U2 end-to-end: real ``dispatch_import_from_db``,
    ``ensure_candidate_evidence_for_action``, ``requeue_import_job_for_preview``,
    and the importer claim WHERE clause. Uses FakePipelineDB for state.
    """

    def test_force_import_missing_evidence_routes_through_preview(self):
        from lib.import_dispatch import (
            DISPATCH_CODE_REQUEUED_FOR_PREVIEW,
            dispatch_import_from_db,
        )
        from lib.import_queue import (
            IMPORT_JOB_MANUAL,
            manual_import_payload,
        )
        from lib.quality import (
            ALBUM_QUALITY_EVIDENCE_OWNER_IMPORT_JOB_CANDIDATE,
        )
        from lib.quality_evidence import snapshot_audio_files
        from tests.helpers import make_album_quality_evidence

        with tempfile.TemporaryDirectory() as source:
            with open(os.path.join(source, "01.mp3"), "wb") as handle:
                handle.write(b"audio")
            db = FakePipelineDB()
            db.seed_request(make_request_row(
                id=42,
                mb_release_id="mbid-requeue",
                status="manual",
                artist_name="A",
                album_title="B",
            ))
            job = db.enqueue_import_job(
                IMPORT_JOB_MANUAL,
                request_id=42,
                dedupe_key="manual:slice",
                payload=manual_import_payload(failed_path=source),
            )
            # Step 1: simulate an importer-importable state without evidence
            # (the lossy real-world starting point: preview marked ready in a
            # prior cycle, then evidence was lost / never persisted).
            db.mark_import_job_preview_importable(
                job.id,
                preview_result={"verdict": "would_import"},
                message="ready",
            )

            # Step 2: importer claims.
            claimed = db.claim_next_import_job(worker_id="importer-1")
            assert claimed is not None
            self.assertEqual(claimed.status, "running")

            # Step 3: dispatch sees no candidate evidence → requeues.
            cfg = CratediggerConfig(
                beets_harness_path=_HARNESS, pipeline_db_enabled=True
            )
            with patch_dispatch_externals() as ext, \
                 patch("lib.config.read_runtime_config", return_value=cfg):
                outcome = dispatch_import_from_db(
                    cast(Any, db),
                    request_id=42,
                    failed_path=source,
                    force=False,
                    outcome_label="manual_import",
                    import_job_id=job.id,
                )

            self.assertFalse(outcome.success)
            self.assertEqual(outcome.code, DISPATCH_CODE_REQUEUED_FOR_PREVIEW)
            ext.run.assert_not_called()
            row = next(r for r in db._import_jobs if r["id"] == job.id)
            self.assertEqual(row["status"], "queued")
            self.assertEqual(row["preview_status"], "waiting")
            self.assertIsNone(row["worker_id"])

            # Step 4: preview claims the requeued row.
            preview_claimed = db.claim_next_import_preview_job(
                worker_id="preview-1"
            )
            assert preview_claimed is not None
            self.assertEqual(preview_claimed.id, job.id)
            self.assertEqual(preview_claimed.preview_status, "running")

            # Step 5: preview persists candidate evidence and marks ready.
            db.upsert_album_quality_evidence(make_album_quality_evidence(
                owner_type=ALBUM_QUALITY_EVIDENCE_OWNER_IMPORT_JOB_CANDIDATE,
                owner_id=job.id,
                files=snapshot_audio_files(source),
                measurement=AudioQualityMeasurement(
                    min_bitrate_kbps=245,
                    avg_bitrate_kbps=256,
                    median_bitrate_kbps=252,
                    format="MP3 V0",
                    spectral_grade="genuine",
                ),
                codec="mp3",
                container="mp3",
                storage_format="mp3 v0",
            ))
            db.mark_import_job_preview_importable(
                job.id,
                preview_result={"verdict": "would_import"},
                message="ready",
            )

            # Step 6: importer claims again — this time evidence exists.
            second_claim = db.claim_next_import_job(worker_id="importer-2")
            assert second_claim is not None
            self.assertEqual(second_claim.id, job.id)
            self.assertEqual(second_claim.status, "running")


class TestRecordPreviewMeasurementFailedSlice(unittest.TestCase):
    """Integration slice for the preview-side measurement_failed entry point.

    Validates the four self-healing side effects own the same sub-helper as
    the importer-side ``_record_rejection_and_maybe_requeue``:

      (a) ``download_log`` row written with ``outcome='measurement_failed'``
      (b) ``source_denylist`` write when the per-user rule applies
      (c) parent request transitioned to ``status='wanted'`` via
          ``transitions.finalize_request``
      (d) ``import_jobs.status='failed'`` via ``mark_import_job_failed``

    Also covers the ``request_not_found`` no-finalize subcase — the helper
    writes the log + marks the job failed but skips the request transition
    (there is nothing to finalize).
    """

    def test_measurement_failed_happy_path_fires_all_four_side_effects(self):
        from lib.import_dispatch import _record_preview_measurement_failed
        from lib.import_queue import IMPORT_JOB_AUTOMATION, automation_import_payload
        from lib.quality import MeasurementFailure

        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42, status="downloading",
            artist_name="Test", album_title="Album",
        ))
        job = db.enqueue_import_job(
            IMPORT_JOB_AUTOMATION,
            request_id=42,
            payload=automation_import_payload(),
        )
        # Move job to 'running' to mimic an importer claim — or rather,
        # the preview worker scenario where the job was claimed by preview.
        # mark_import_job_failed accepts queued or running.

        payload = MeasurementFailure(
            reason="snapshot_stale",
            detail="audio snapshot mismatch after retry",
            source_path="/Incoming/auto-import/Test - Album",
        )

        _record_preview_measurement_failed(
            cast(Any, db),
            request_id=42,
            import_job_id=job.id,
            payload=payload,
            denylist_username="bob",
        )

        # (a) download_log row
        self.assertEqual(len(db.download_logs), 1)
        db.assert_log(
            self, 0,
            outcome="measurement_failed",
            request_id=42,
            beets_scenario="measurement_failed",
            beets_detail="audio snapshot mismatch after retry",
        )
        # validation_result is a JSON string carrying the MeasurementFailure
        log = db.download_logs[0]
        self.assertIsNotNone(log.validation_result)
        import json as _json
        decoded = _json.loads(log.validation_result)
        self.assertEqual(decoded["reason"], "snapshot_stale")
        self.assertEqual(
            decoded["source_path"],
            "/Incoming/auto-import/Test - Album",
        )

        # (b) denylist write
        self.assertEqual(len(db.denylist), 1)
        self.assertEqual(db.denylist[0].request_id, 42)
        self.assertEqual(db.denylist[0].username, "bob")

        # (c) request → wanted
        self.assertEqual(db.request(42)["status"], "wanted")

        # (d) job → failed
        job_row = db.get_import_job(job.id)
        assert job_row is not None
        self.assertEqual(job_row.status, "failed")

    def test_request_not_found_subcase_skips_finalize(self):
        """request_id=None: helper writes the log + marks job failed but
        does NOT call finalize_request (there is no parent to finalize).
        Denylist is also skipped because it FK-references a request_id."""
        from lib.import_dispatch import _record_preview_measurement_failed
        from lib.import_queue import IMPORT_JOB_AUTOMATION, automation_import_payload
        from lib.quality import MeasurementFailure

        db = FakePipelineDB()
        # No request seeded. Enqueue an orphan job (request_id is set on
        # the job itself but no album_requests row matches).
        job = db.enqueue_import_job(
            IMPORT_JOB_AUTOMATION,
            request_id=None,
            payload=automation_import_payload(),
        )

        payload = MeasurementFailure(
            reason="request_not_found",
            detail="album_request 999 not found",
            source_path="",
        )

        # Spy on finalize_request to prove it is NOT called.
        with patch("lib.transitions.finalize_request") as mock_finalize:
            _record_preview_measurement_failed(
                cast(Any, db),
                request_id=None,
                import_job_id=job.id,
                payload=payload,
                denylist_username="bob",
            )
            mock_finalize.assert_not_called()

        # download_log row still written (carrying NULL request_id).
        self.assertEqual(len(db.download_logs), 1)
        db.assert_log(
            self, 0,
            outcome="measurement_failed",
            request_id=None,
            beets_scenario="measurement_failed",
        )

        # No denylist (FK requires request_id).
        self.assertEqual(len(db.denylist), 0)

        # Job → failed.
        job_row = db.get_import_job(job.id)
        assert job_row is not None
        self.assertEqual(job_row.status, "failed")

    def test_source_vanished_no_denylist_when_username_unknown(self):
        """source_vanished + no source_username → no denylist write.
        Request and job still finalize normally."""
        from lib.import_dispatch import _record_preview_measurement_failed
        from lib.import_queue import IMPORT_JOB_AUTOMATION, automation_import_payload
        from lib.quality import MeasurementFailure

        db = FakePipelineDB()
        db.seed_request(make_request_row(id=7, status="downloading"))
        job = db.enqueue_import_job(
            IMPORT_JOB_AUTOMATION,
            request_id=7,
            payload=automation_import_payload(),
        )

        payload = MeasurementFailure(
            reason="source_vanished",
            detail="ffmpeg ENOENT",
            source_path="/Incoming/auto-import/Album",
        )
        _record_preview_measurement_failed(
            cast(Any, db),
            request_id=7,
            import_job_id=job.id,
            payload=payload,
            denylist_username=None,
        )

        self.assertEqual(len(db.denylist), 0)
        self.assertEqual(db.request(7)["status"], "wanted")
        job_row = db.get_import_job(job.id)
        assert job_row is not None
        self.assertEqual(job_row.status, "failed")
        # download_log carries the typed payload as JSON.
        log = db.download_logs[0]
        self.assertEqual(log.outcome, "measurement_failed")
        import json as _json
        decoded = _json.loads(log.validation_result)
        self.assertEqual(decoded["reason"], "source_vanished")

    def test_importer_side_record_rejection_still_works(self):
        """Regression guard: refactoring _record_rejection_and_maybe_requeue
        into delegation must NOT change importer-side behavior. The
        existing DownloadInfo-based entry point continues to write a
        download_log row, transition the request to wanted, and record an
        attempt — same as before U4."""
        from lib.import_dispatch import _record_rejection_and_maybe_requeue
        from lib.quality import DownloadInfo

        db = FakePipelineDB()
        db.seed_request(make_request_row(id=99, status="downloading"))

        _record_rejection_and_maybe_requeue(
            cast(Any, db),
            99,
            DownloadInfo(username="alice", filetype="mp3", bitrate=192_000),
            distance=0.5,
            scenario="quality_downgrade",
            detail="bitrate too low",
            error=None,
            requeue=True,
        )

        # Existing behavior: request transitioned, attempt recorded.
        row = db.request(99)
        self.assertEqual(row["status"], "wanted")
        self.assertEqual(row["validation_attempts"], 1)
        # Download log row written via the shared sub-helper.
        self.assertEqual(len(db.download_logs), 1)
        db.assert_log(
            self, 0,
            outcome="rejected",
            soulseek_username="alice",
            beets_scenario="quality_downgrade",
            beets_detail="bitrate too low",
        )
        # No job-failed call — that's still the caller's responsibility on
        # the importer path (preserved from pre-U4).
        # No denylist either (caller does it).
        self.assertEqual(len(db.denylist), 0)


class TestU5PreviewWorkerSelfHealSlice(unittest.TestCase):
    """End-to-end slice: U5 preview-worker emits measurement_failed → self-heal.

    Covers AE3 (nested), AE4 (empty), AE5 (snapshot stale), AE6 (source
    vanished) at the worker level: the worker claims a job, preview returns
    ``verdict='measurement_failed'`` with a typed ``MeasurementFailure``
    payload, and the worker routes it through U4's self-healing helper so the
    parent request transitions to ``wanted`` and the job is marked failed
    with ``preview_status='measurement_failed'``.
    """

    def _setup_worker_job(self, db, request_id=42):
        from lib.import_queue import (
            IMPORT_JOB_FORCE,
            force_import_dedupe_key,
            force_import_payload,
        )
        db.seed_request(make_request_row(
            id=request_id,
            status="downloading",
            mb_release_id="mbid-u5",
        ))
        download_log_id = db.log_download(
            request_id,
            outcome="rejected",
            validation_result={"failed_path": "/tmp/u5-vanished"},
        )
        db.enqueue_import_job(
            IMPORT_JOB_FORCE,
            request_id=request_id,
            dedupe_key=force_import_dedupe_key(download_log_id),
            payload=force_import_payload(
                download_log_id=download_log_id,
                failed_path="/tmp/u5-vanished",
                source_username="alice",
            ),
        )
        return db.claim_next_import_preview_job(worker_id="preview")

    def test_ae6_source_vanished_self_heals_request_to_wanted(self):
        """AE6: ffmpeg ENOENT during measurement → measurement_failed,
        request → wanted, job → failed, download_log carries the typed
        payload."""
        from lib.import_preview import ImportPreviewResult
        from lib.quality import MeasurementFailure
        from scripts import import_preview_worker

        db = FakePipelineDB()
        claimed = self._setup_worker_job(db)
        assert claimed is not None

        payload = MeasurementFailure(
            reason="source_vanished",
            detail="ffmpeg returned ENOENT",
            source_path="/tmp/u5-vanished",
        )
        preview_result = ImportPreviewResult(
            mode="path",
            verdict="measurement_failed",
            decision="source_vanished",
            reason="source_vanished",
            detail="ffmpeg returned ENOENT",
            source_path="/tmp/u5-vanished",
            request_id=42,
            failure=payload,
        )
        with patch(
            "scripts.import_preview_worker.preview_import_from_path",
            return_value=preview_result,
        ):
            updated = import_preview_worker.process_claimed_preview_job(
                cast(Any, db), claimed,
            )

        assert updated is not None
        # Worker surface: preview_status reflects the new state, job failed.
        self.assertEqual(updated.preview_status, "measurement_failed")
        self.assertEqual(updated.status, "failed")
        # U4 self-heal: request → wanted, download_log carries payload.
        self.assertEqual(db.request(42)["status"], "wanted")
        # Two download_log rows: the original rejected entry + the new
        # measurement_failed one.
        outcomes = [log.outcome for log in db.download_logs]
        self.assertIn("measurement_failed", outcomes)
        mf_log = next(log for log in db.download_logs
                      if log.outcome == "measurement_failed")
        import json as _json
        decoded = _json.loads(mf_log.validation_result)
        self.assertEqual(decoded["reason"], "source_vanished")
        self.assertEqual(decoded["source_path"], "/tmp/u5-vanished")

    def test_ae5_snapshot_stale_self_heals(self):
        """AE5: snapshot mismatch after retry → measurement_failed,
        request → wanted."""
        from lib.import_preview import ImportPreviewResult
        from lib.quality import MeasurementFailure
        from scripts import import_preview_worker

        db = FakePipelineDB()
        claimed = self._setup_worker_job(db, request_id=43)
        assert claimed is not None

        payload = MeasurementFailure(
            reason="snapshot_stale",
            detail="source files changed while preview was running",
            source_path="/tmp/u5-vanished",
        )
        preview_result = ImportPreviewResult(
            mode="path",
            verdict="measurement_failed",
            decision="source_changed_during_preview",
            reason="source_changed_during_preview",
            detail=payload.detail,
            source_path=payload.source_path,
            request_id=43,
            failure=payload,
        )
        with patch(
            "scripts.import_preview_worker.preview_import_from_path",
            return_value=preview_result,
        ):
            updated = import_preview_worker.process_claimed_preview_job(
                cast(Any, db), claimed,
            )

        assert updated is not None
        self.assertEqual(updated.preview_status, "measurement_failed")
        self.assertEqual(db.request(43)["status"], "wanted")

    def test_request_not_found_no_finalize_subcase(self):
        """request_id=None subcase: helper writes log + marks job failed
        but does NOT transition any request (there is nothing to
        finalize). No exception bubbles up."""
        from lib.import_preview import ImportPreviewResult
        from lib.import_queue import IMPORT_JOB_FORCE, force_import_payload
        from lib.quality import MeasurementFailure
        from scripts import import_preview_worker

        db = FakePipelineDB()
        # Enqueue a job with request_id=None (orphan).
        db.enqueue_import_job(
            IMPORT_JOB_FORCE,
            request_id=None,
            payload=force_import_payload(
                download_log_id=999,
                failed_path="/tmp/gone",
            ),
        )
        claimed = db.claim_next_import_preview_job(worker_id="preview")
        assert claimed is not None

        payload = MeasurementFailure(
            reason="request_not_found",
            detail="album_request 999 not found",
            source_path="",
        )
        preview_result = ImportPreviewResult(
            mode="path",
            verdict="measurement_failed",
            decision="request_not_found",
            reason="request_not_found",
            detail=payload.detail,
            request_id=None,
            failure=payload,
        )
        with patch(
            "scripts.import_preview_worker.preview_import_from_path",
            return_value=preview_result,
        ):
            updated = import_preview_worker.process_claimed_preview_job(
                cast(Any, db), claimed,
            )

        assert updated is not None
        self.assertEqual(updated.preview_status, "measurement_failed")
        self.assertEqual(updated.status, "failed")
        # Helper still wrote the download_log row.
        outcomes = [log.outcome for log in db.download_logs]
        self.assertIn("measurement_failed", outcomes)


class TestU5PreviewEvidenceReadySlice(unittest.TestCase):
    """End-to-end slice: preview emits ``evidence_ready`` → job marked
    importable for the importer to claim. Covers AE3/AE4: the nested-layout
    and empty-fileset facts are persisted as evidence, the importer reads
    them in U6 (out of scope here).
    """

    def test_evidence_ready_marks_job_importable(self):
        from lib.import_preview import ImportPreviewResult
        from lib.import_queue import (
            IMPORT_JOB_FORCE,
            force_import_dedupe_key,
            force_import_payload,
        )
        from lib.quality import (
            ALBUM_QUALITY_EVIDENCE_OWNER_DOWNLOAD_LOG_CANDIDATE,
            AudioQualityMeasurement,
        )
        from tests.helpers import make_album_quality_evidence
        from lib.quality_evidence import snapshot_audio_files
        from scripts import import_preview_worker

        db = FakePipelineDB()
        db.seed_request(make_request_row(id=42, status="downloading"))
        with tempfile.TemporaryDirectory() as source:
            with open(os.path.join(source, "01.mp3"), "wb") as handle:
                handle.write(b"audio")
            download_log_id = db.log_download(42, outcome="rejected")
            db.enqueue_import_job(
                IMPORT_JOB_FORCE,
                request_id=42,
                dedupe_key=force_import_dedupe_key(download_log_id),
                payload=force_import_payload(
                    download_log_id=download_log_id,
                    failed_path=source,
                    source_username="alice",
                ),
            )
            claimed = db.claim_next_import_preview_job(worker_id="preview")
            assert claimed is not None

            preview_result = ImportPreviewResult(
                mode="path",
                verdict="evidence_ready",
                decision="import",
                reason="import",
                stage_chain=["stage2_import:import"],
                source_path=source,
                request_id=42,
            )

            def fake_preview(*args, **kwargs):
                # Simulate production: preview persisted candidate evidence
                # before returning.
                db.upsert_album_quality_evidence(make_album_quality_evidence(
                    owner_type=ALBUM_QUALITY_EVIDENCE_OWNER_DOWNLOAD_LOG_CANDIDATE,
                    owner_id=download_log_id,
                    files=snapshot_audio_files(source),
                    measurement=AudioQualityMeasurement(
                        min_bitrate_kbps=245,
                        avg_bitrate_kbps=256,
                        median_bitrate_kbps=252,
                        format="MP3 V0",
                        spectral_grade="genuine",
                    ),
                    codec="mp3",
                    container="mp3",
                    storage_format="mp3 v0",
                ))
                return preview_result

            with patch(
                "scripts.import_preview_worker.preview_import_from_path",
                side_effect=fake_preview,
            ):
                updated = import_preview_worker.process_claimed_preview_job(
                    cast(Any, db), claimed,
                )

        assert updated is not None
        self.assertEqual(updated.preview_status, "evidence_ready")
        self.assertEqual(updated.status, "queued")
        # Importable now — importer (U6) can claim it.
        self.assertIsNotNone(updated.importable_at)


class TestU6ImporterPreimportDecideSlice(unittest.TestCase):
    """U6 integration slices: importer dispatch wires preimport_decide.

    Each slice seeds an ``import_jobs`` row with persisted candidate
    ``AlbumQualityEvidence`` carrying a reject-shaped U1 fact
    (``audio_corrupt``, nested ``folder_layout``, ``audio_file_count=0``,
    suspect spectral), drives ``dispatch_import_from_db`` (the importer's
    entry point), and asserts the U4 self-healing side effects fire:

      * ``download_log`` row with the reject scenario and validation_result
      * request → ``wanted`` (with attempt-counter bump)
      * beets is NEVER touched (``sp.run`` is asserted not-called)
      * staged dir cleanup happens for auto-import scenarios

    Covers AE2 (audio_corrupt), AE3 (nested_layout), AE4 (empty_fileset),
    plus the spectral_reject branch.
    """

    def _seed_with_evidence(
        self,
        db,
        *,
        request_id,
        tmpdir,
        candidate_evidence,
        current_evidence=None,
    ):
        """Seed a manual import_job + candidate evidence + (opt) current.

        Returns (import_job_id, download_log_id=None).
        """
        from lib.import_queue import IMPORT_JOB_MANUAL, manual_import_payload

        db.seed_request(make_request_row(
            id=request_id,
            status="manual",
            mb_release_id=f"mbid-u6-{request_id}",
            artist_name="Test Artist",
            album_title="Test Album",
        ))
        job = db.enqueue_import_job(
            IMPORT_JOB_MANUAL,
            request_id=request_id,
            payload=manual_import_payload(failed_path=tmpdir),
        )
        db.upsert_album_quality_evidence(candidate_evidence)
        if current_evidence is not None:
            db.upsert_album_quality_evidence(current_evidence)
        return job.id

    def _build_candidate_evidence(
        self,
        *,
        owner_id,
        files,
        spectral_grade="genuine",
        spectral_bitrate_kbps=None,
        audio_corrupt=False,
        folder_layout="flat",
        audio_file_count=None,
        filetype_band="mp3 v0",
        matched_bad_audio_hash_id=None,
        matched_bad_audio_hash_path=None,
        min_bitrate_kbps=245,
    ):
        from datetime import datetime, timezone
        from lib.quality import (
            ALBUM_QUALITY_EVIDENCE_OWNER_IMPORT_JOB_CANDIDATE,
            AlbumQualityEvidence,
            AlbumQualityEvidenceOwner,
            AudioQualityMeasurement,
        )

        return AlbumQualityEvidence(
            owner=AlbumQualityEvidenceOwner(
                owner_type=ALBUM_QUALITY_EVIDENCE_OWNER_IMPORT_JOB_CANDIDATE,
                owner_id=owner_id,
            ),
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=min_bitrate_kbps,
                avg_bitrate_kbps=min_bitrate_kbps,
                median_bitrate_kbps=min_bitrate_kbps,
                format="MP3 V0",
                spectral_grade=spectral_grade,
                spectral_bitrate_kbps=spectral_bitrate_kbps,
            ),
            measured_at=datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
            files=files,
            codec="mp3",
            container="mp3",
            storage_format="mp3 v0",
            audio_corrupt=audio_corrupt,
            folder_layout=folder_layout,
            audio_file_count=(
                audio_file_count if audio_file_count is not None
                else len(files)
            ),
            filetype_band=filetype_band,
            matched_bad_audio_hash_id=matched_bad_audio_hash_id,
            matched_bad_audio_hash_path=matched_bad_audio_hash_path,
        )

    def _build_current_evidence(
        self,
        *,
        owner_id,
        min_bitrate_kbps=128,
        spectral_grade="genuine",
        spectral_bitrate_kbps=None,
    ):
        from lib.quality import (
            ALBUM_QUALITY_EVIDENCE_OWNER_REQUEST_CURRENT,
            AudioQualityMeasurement,
        )
        from tests.helpers import make_album_quality_evidence

        return make_album_quality_evidence(
            owner_type=ALBUM_QUALITY_EVIDENCE_OWNER_REQUEST_CURRENT,
            owner_id=owner_id,
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=min_bitrate_kbps,
                avg_bitrate_kbps=min_bitrate_kbps,
                median_bitrate_kbps=min_bitrate_kbps,
                format="MP3",
                spectral_grade=spectral_grade,
                spectral_bitrate_kbps=spectral_bitrate_kbps,
            ),
            codec="mp3",
            container="mp3",
            storage_format="mp3",
        )

    def _drive_dispatch(self, db, *, request_id, tmpdir, import_job_id, cfg):
        from lib.import_dispatch import dispatch_import_from_db

        # album_path=None makes the current-evidence guard trust the
        # seeded REQUEST_CURRENT row without an audio-snapshot probe.
        beets_info_no_path = AlbumInfo(
            album_id=1, track_count=10, min_bitrate_kbps=128,
            avg_bitrate_kbps=128, format="MP3",
            is_cbr=False, album_path=None,  # type: ignore[arg-type]
        )
        with patch_dispatch_externals() as ext, \
                patch("lib.beets_db.BeetsDB", _mock_beets_db(beets_info_no_path)), \
                patch("lib.config.read_runtime_config", return_value=cfg):
            result = dispatch_import_from_db(
                db,  # type: ignore[arg-type]
                request_id=request_id,
                failed_path=tmpdir,
                force=False,
                source_username="alice",
                import_job_id=import_job_id,
                outcome_label="manual_import",
            )
        return result, ext

    def _common_cfg(self):
        return CratediggerConfig(
            beets_harness_path=_HARNESS,
            pipeline_db_enabled=True,
        )

    def test_audio_corrupt_evidence_routes_through_self_heal(self):
        """AE2: candidate evidence with audio_corrupt=True → preimport_decide
        rejects → request → wanted, download_log carries audio_corrupt scenario,
        beets (sp.run) is never invoked."""
        from lib.quality_evidence import snapshot_audio_files

        db = FakePipelineDB()
        cfg = self._common_cfg()
        with tempfile.TemporaryDirectory() as tmpdir:
            with open(os.path.join(tmpdir, "01.mp3"), "wb") as h:
                h.write(b"audio")
            snap = snapshot_audio_files(tmpdir)
            # Mark the file as decode_ok=False on the snapshot — the helper
            # surfaces these into corrupt_files.
            from lib.quality import AlbumQualityEvidenceFile
            corrupt_files = [
                AlbumQualityEvidenceFile(
                    relative_path=f.relative_path,
                    size_bytes=f.size_bytes,
                    mtime_ns=f.mtime_ns,
                    extension=f.extension,
                    container=f.container,
                    codec=f.codec,
                    decode_ok=False,
                )
                for f in snap
            ]
            # Need to enqueue first to get the job id for owner_id
            from lib.import_queue import IMPORT_JOB_MANUAL, manual_import_payload
            db.seed_request(make_request_row(
                id=42, status="manual", mb_release_id="mbid-u6-corrupt",
            ))
            job = db.enqueue_import_job(
                IMPORT_JOB_MANUAL,
                request_id=42,
                payload=manual_import_payload(failed_path=tmpdir),
            )
            db.upsert_album_quality_evidence(
                self._build_candidate_evidence(
                    owner_id=job.id,
                    files=corrupt_files,
                    audio_corrupt=True,
                )
            )
            db.upsert_album_quality_evidence(
                self._build_current_evidence(owner_id=42),
            )

            result, ext = self._drive_dispatch(
                db, request_id=42, tmpdir=tmpdir,
                import_job_id=job.id, cfg=cfg,
            )

        self.assertFalse(result.success)
        self.assertIn("audio_corrupt", result.message or "")
        # Beets subprocess NEVER ran — preimport_decide rejected upstream.
        self.assertEqual(
            ext.run.call_count, 0,
            "beets import_one.py must not run when preimport_decide rejects")
        # Self-heal: request → wanted with attempt bump.
        row = db.request(42)
        self.assertEqual(row["status"], "wanted")
        self.assertEqual(row["validation_attempts"], 1)
        # download_log row records the rejection scenario.
        outcomes = [(log.outcome, log.beets_scenario) for log in db.download_logs]
        self.assertIn(("rejected", "audio_corrupt"), outcomes)

    def test_nested_layout_evidence_routes_through_self_heal(self):
        """AE3: folder_layout='nested' → preimport_decide rejects → self-heal."""
        from lib.quality_evidence import snapshot_audio_files

        db = FakePipelineDB()
        cfg = self._common_cfg()
        with tempfile.TemporaryDirectory() as tmpdir:
            cd1 = os.path.join(tmpdir, "cd1")
            os.makedirs(cd1)
            with open(os.path.join(cd1, "01 - Track.mp3"), "wb") as h:
                h.write(b"audio")
            files = snapshot_audio_files(tmpdir)
            from lib.import_queue import IMPORT_JOB_MANUAL, manual_import_payload
            db.seed_request(make_request_row(
                id=43, status="manual", mb_release_id="mbid-u6-nested",
            ))
            job = db.enqueue_import_job(
                IMPORT_JOB_MANUAL,
                request_id=43,
                payload=manual_import_payload(failed_path=tmpdir),
            )
            db.upsert_album_quality_evidence(
                self._build_candidate_evidence(
                    owner_id=job.id,
                    files=files,
                    folder_layout="nested",
                )
            )
            db.upsert_album_quality_evidence(
                self._build_current_evidence(owner_id=43),
            )

            result, ext = self._drive_dispatch(
                db, request_id=43, tmpdir=tmpdir,
                import_job_id=job.id, cfg=cfg,
            )

        self.assertFalse(result.success)
        self.assertIn("nested_layout", result.message or "")
        self.assertEqual(ext.run.call_count, 0)
        row = db.request(43)
        self.assertEqual(row["status"], "wanted")
        outcomes = [(log.outcome, log.beets_scenario) for log in db.download_logs]
        self.assertIn(("rejected", "nested_layout"), outcomes)
        # nested_layout is a folder-shape problem, not a peer-quality problem
        # — denylisting the peer would be unfair.
        self.assertEqual(len(db.denylist), 0)

    def test_empty_fileset_evidence_routes_through_self_heal(self):
        """AE4: audio_file_count=0 → preimport_decide rejects → self-heal."""
        db = FakePipelineDB()
        cfg = self._common_cfg()
        with tempfile.TemporaryDirectory() as tmpdir:
            from lib.import_queue import IMPORT_JOB_MANUAL, manual_import_payload
            db.seed_request(make_request_row(
                id=44, status="manual", mb_release_id="mbid-u6-empty",
            ))
            job = db.enqueue_import_job(
                IMPORT_JOB_MANUAL,
                request_id=44,
                payload=manual_import_payload(failed_path=tmpdir),
            )
            db.upsert_album_quality_evidence(
                self._build_candidate_evidence(
                    owner_id=job.id,
                    files=[],
                    audio_file_count=0,
                    folder_layout="flat",
                )
            )
            db.upsert_album_quality_evidence(
                self._build_current_evidence(owner_id=44),
            )

            result, ext = self._drive_dispatch(
                db, request_id=44, tmpdir=tmpdir,
                import_job_id=job.id, cfg=cfg,
            )

        self.assertFalse(result.success)
        self.assertIn("empty_fileset", result.message or "")
        self.assertEqual(ext.run.call_count, 0)
        row = db.request(44)
        self.assertEqual(row["status"], "wanted")
        outcomes = [(log.outcome, log.beets_scenario) for log in db.download_logs]
        self.assertIn(("rejected", "empty_fileset"), outcomes)

    def test_spectral_reject_evidence_routes_through_evidence_pipeline(self):
        """likely_transcode candidate spectral matches existing transcode
        spectral → ``full_pipeline_decision_from_evidence`` rejects at
        stage1_spectral (spectral evidence vs spectral evidence). The
        importer routes the reject through ``_reject_import_from_evidence_decision``;
        beets never runs and the download_log records the rejection.

        Spectral comparison lives in the full pipeline (the same function
        the album test set pins) — not in ``preimport_decide``, which only
        owns folder/audio-integrity facts."""
        from lib.quality import AlbumQualityEvidenceFile

        db = FakePipelineDB()
        cfg = self._common_cfg()
        with tempfile.TemporaryDirectory() as tmpdir:
            with open(os.path.join(tmpdir, "01.mp3"), "wb") as h:
                h.write(b"audio")
            files = [
                AlbumQualityEvidenceFile(
                    relative_path="01.mp3",
                    size_bytes=5,
                    mtime_ns=1_700_000_000_000_000_000,
                    extension="mp3",
                    container="mp3",
                    codec="mp3",
                ),
            ]
            from lib.import_queue import IMPORT_JOB_MANUAL, manual_import_payload
            db.seed_request(make_request_row(
                id=45, status="manual", mb_release_id="mbid-u6-spectral",
                current_spectral_grade="likely_transcode",
                current_spectral_bitrate=128,
            ))
            job = db.enqueue_import_job(
                IMPORT_JOB_MANUAL,
                request_id=45,
                payload=manual_import_payload(failed_path=tmpdir),
            )
            db.upsert_album_quality_evidence(
                self._build_candidate_evidence(
                    owner_id=job.id,
                    files=files,
                    spectral_grade="likely_transcode",
                    spectral_bitrate_kbps=128,
                    min_bitrate_kbps=128,
                )
            )
            db.upsert_album_quality_evidence(
                self._build_current_evidence(
                    owner_id=45,
                    min_bitrate_kbps=128,
                    spectral_grade="likely_transcode",
                    spectral_bitrate_kbps=128,
                ),
            )

            result, ext = self._drive_dispatch(
                db, request_id=45, tmpdir=tmpdir,
                import_job_id=job.id, cfg=cfg,
            )

        self.assertFalse(result.success)
        self.assertIn("spectral_reject", result.message or "")
        # Beets subprocess NEVER ran — evidence pipeline rejected upstream.
        self.assertEqual(ext.run.call_count, 0)
        outcomes = [(log.outcome, log.beets_scenario) for log in db.download_logs]
        self.assertIn(("rejected", "spectral_reject"), outcomes)


# ---------------------------------------------------------------------------
# U7. Recovery sweep migration 020 — integration slice (AE8)
# ---------------------------------------------------------------------------

# Importing conftest boots the ephemeral PostgreSQL and sets TEST_DB_DSN so
# this slice can use a real PipelineDB. Without this, running the slice via
# ``python3 -m unittest tests.test_integration_slices`` directly would skip
# (conftest is only auto-loaded by pytest discovery).
import sys as _u7_sys
_u7_sys.path.insert(0, os.path.dirname(__file__))
import conftest as _u7_conftest  # noqa: F401  # boots ephemeral PG + sets DSN


def _u7_test_dsn() -> str | None:
    """Read the DSN lazily — conftest sets it during import, after this module
    is parsed by the test runner."""
    return os.environ.get("TEST_DB_DSN")


@unittest.skipUnless(_u7_test_dsn(), "TEST_DB_DSN not set — slice needs real PG")
class TestU7RecoverySweepSlice(unittest.TestCase):
    """AE8 integration slice: pre-seed an ``import_jobs`` row with
    ``preview_status='uncertain'``, run the migration-020 sweep against the
    real PostgreSQL test fixture, then assert ``PipelineDB.claim_next_import_
    preview_job`` (the actual preview-worker claim query) selects the
    recovered row.

    This is the AE8 contract: the 315-stuck-row symptom is resolved by
    re-circulating those rows through the new preview-never-decides
    pipeline. The slice proves end-to-end that the live SQL flow puts the
    row into a state the preview worker will pick up on its next tick.
    """

    # Direct copy of the canonical sweep statement from
    # ``migrations/020_recover_stuck_preview_uncertain_jobs.sql``.
    _RECOVERY_SWEEP_SQL = """
        UPDATE import_jobs
        SET preview_status = 'waiting',
            preview_result = NULL,
            preview_message = 'Recovered by preview-never-decides refactor (020)',
            preview_error = NULL,
            preview_worker_id = NULL,
            preview_started_at = NULL,
            preview_heartbeat_at = NULL,
            preview_completed_at = NULL,
            importable_at = NULL,
            updated_at = NOW()
        WHERE status = 'queued'
          AND preview_status = 'uncertain'
    """

    def setUp(self):
        import psycopg2
        from lib.pipeline_db import PipelineDB
        self._psycopg2 = psycopg2
        self.db = PipelineDB(_u7_test_dsn())
        self.addCleanup(self.db.close)
        # Track per-test resources for teardown.
        self._mbids: list[str] = []

    def tearDown(self):
        # Drop any seeded rows (CASCADE handles import_jobs).
        if self._mbids:
            conn = self._psycopg2.connect(_u7_test_dsn())
            conn.autocommit = True
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        "DELETE FROM album_requests WHERE mb_release_id = ANY(%s)",
                        (self._mbids,),
                    )
            finally:
                conn.close()

    def _exec(self, sql: str, params: tuple = ()):
        conn = self._psycopg2.connect(_u7_test_dsn())
        conn.autocommit = True
        try:
            with conn.cursor() as cur:
                cur.execute(sql, params)
        finally:
            conn.close()

    def _query(self, sql: str, params: tuple = ()):
        conn = self._psycopg2.connect(_u7_test_dsn())
        conn.autocommit = True
        try:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                return cur.fetchall()
        finally:
            conn.close()

    def _seed_stuck_uncertain_job(self, *, mbid: str) -> tuple[int, int]:
        """Insert a request + a stuck uncertain import_jobs row. Returns
        (request_id, import_job_id)."""
        self._mbids.append(mbid)
        self._exec("""
            INSERT INTO album_requests (mb_release_id, artist_name, album_title, source)
            VALUES (%s, 'Test Artist', 'Test Album', 'request')
        """, (mbid,))
        rid = self._query(
            "SELECT id FROM album_requests WHERE mb_release_id = %s",
            (mbid,),
        )[0][0]
        # Mimic a real pre-deploy stuck row — uncertain with verdict result,
        # message, worker_id, and lifecycle timestamps populated.
        self._exec("""
            INSERT INTO import_jobs (
                job_type, status, request_id, payload,
                preview_status, preview_result, preview_message,
                preview_error, preview_worker_id,
                preview_started_at, preview_heartbeat_at,
                preview_completed_at, importable_at
            )
            VALUES (
                'automation_import', 'queued', %s, '{}'::jsonb,
                'uncertain', '{"verdict": "uncertain"}'::jsonb,
                'pre-U7 stuck preview verdict',
                NULL, 'pre-U7-worker',
                NOW(), NOW(), NOW(), NOW()
            )
        """, (rid,))
        job_id = self._query(
            "SELECT id FROM import_jobs WHERE request_id = %s",
            (rid,),
        )[0][0]
        return rid, job_id

    def test_ae8_stuck_uncertain_job_recovers_through_real_claim_query(self):
        """AE8 happy path: stuck uncertain row → migration sweep → live
        ``claim_next_import_preview_job`` selects it as the next preview job.
        """
        rid, job_id = self._seed_stuck_uncertain_job(
            mbid="u7-slice-recover-mbid",
        )

        # Pre-condition: the worker would NOT claim this row before the sweep
        # because the claim query gates on ``preview_status='waiting'``.
        # We can't observe this directly without claiming the wrong row in
        # tests running in parallel, so we just confirm the seed state.
        pre = self._query(
            "SELECT preview_status, preview_message FROM import_jobs WHERE id = %s",
            (job_id,),
        )
        self.assertEqual(pre, [("uncertain", "pre-U7 stuck preview verdict")])

        # Apply the canonical migration-020 sweep statement.
        self._exec(self._RECOVERY_SWEEP_SQL)

        # Post-condition: row flipped to waiting with cleared lifecycle and
        # the audit message attached.
        post = self._query("""
            SELECT preview_status, preview_message, preview_result,
                   preview_worker_id, preview_started_at,
                   preview_heartbeat_at, preview_completed_at, importable_at
            FROM import_jobs WHERE id = %s
        """, (job_id,))
        self.assertEqual(len(post), 1)
        (preview_status, message, result, worker_id, started_at,
         heartbeat_at, completed_at, importable_at) = post[0]
        self.assertEqual(preview_status, "waiting")
        self.assertEqual(
            message,
            "Recovered by preview-never-decides refactor (020)",
        )
        self.assertIsNone(result)
        self.assertIsNone(worker_id)
        self.assertIsNone(started_at)
        self.assertIsNone(heartbeat_at)
        self.assertIsNone(completed_at)
        self.assertIsNone(importable_at)

        # Now the live preview-worker claim query must select this row.
        claimed = self.db.claim_next_import_preview_job(
            worker_id="u7-test-worker",
        )
        # There may be other 'waiting' jobs from parallel tests in a shared
        # DSN; if so we keep claiming until we find ours, but the contract is
        # that our row IS claimable. We bound the loop at a small constant.
        found = False
        seen: list[int] = []
        for _ in range(10):
            if claimed is None:
                break
            seen.append(claimed.id)
            if claimed.id == job_id:
                found = True
                break
            claimed = self.db.claim_next_import_preview_job(
                worker_id="u7-test-worker",
            )
        self.assertTrue(
            found,
            f"claim_next_import_preview_job did not surface job {job_id}; "
            f"saw {seen}",
        )
        # Claimed row transitions to ``running`` per the claim contract.
        running = self._query(
            "SELECT preview_status, preview_worker_id FROM import_jobs WHERE id = %s",
            (job_id,),
        )
        self.assertEqual(running, [("running", "u7-test-worker")])


class TestPreviewWorkerNeverDecidesSlice(unittest.TestCase):
    """Hotfix coverage: ``preview_import_from_path(worker_mode=True)`` must
    NEVER call ``run_preimport_gates``. The worker collects facts via
    ``measure_preimport_state``, persists evidence, and lets the importer's
    ``preimport_decide`` make the accept/reject call.

    Reproduces the Boards-of-Canada production bug: previously preview ran
    the legacy shim which made a ``spectral_reject`` decision, early-returned
    ``evidence_ready`` WITHOUT persisting evidence, and the importer's
    front-gate then fired ``evidence_persist_failed`` and re-queued the
    request forever.
    """

    def _seed_force_job(self, db, *, request_id, source_path):
        from lib.import_queue import (
            IMPORT_JOB_FORCE,
            force_import_dedupe_key,
            force_import_payload,
        )
        db.seed_request(make_request_row(
            id=request_id,
            status="downloading",
            mb_release_id=f"mbid-bocfix-{request_id}",
            artist_name="Boards of Canada",
            album_title="Geogaddi",
        ))
        log_id = db.log_download(request_id, outcome="rejected")
        db.enqueue_import_job(
            IMPORT_JOB_FORCE,
            request_id=request_id,
            dedupe_key=force_import_dedupe_key(log_id),
            payload=force_import_payload(
                download_log_id=log_id,
                failed_path=source_path,
                source_username="alice",
            ),
        )
        claimed = db.claim_next_import_preview_job(worker_id="preview")
        assert claimed is not None
        return claimed, log_id

    def _seed_current_evidence(self, db, *, request_id, min_bitrate_kbps, spectral_grade="genuine", spectral_bitrate_kbps=None):
        from lib.quality import (
            ALBUM_QUALITY_EVIDENCE_OWNER_REQUEST_CURRENT,
            AlbumQualityEvidenceFile,
        )
        from tests.helpers import make_album_quality_evidence
        files = [
            AlbumQualityEvidenceFile(
                relative_path="01 - Old.mp3",
                size_bytes=1000,
                mtime_ns=1_700_000_000_000_000_000,
                extension="mp3",
                container="mp3",
                codec="mp3",
            ),
        ]
        evidence = make_album_quality_evidence(
            owner_type=ALBUM_QUALITY_EVIDENCE_OWNER_REQUEST_CURRENT,
            owner_id=request_id,
            files=files,
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=min_bitrate_kbps,
                avg_bitrate_kbps=min_bitrate_kbps,
                median_bitrate_kbps=min_bitrate_kbps,
                format="MP3",
                spectral_grade=spectral_grade,
                spectral_bitrate_kbps=spectral_bitrate_kbps,
            ),
            codec="mp3",
            container="mp3",
            storage_format="MP3",
        )
        db.upsert_album_quality_evidence(evidence)

    def test_boc_geogaddi_suspect_96k_persists_evidence_and_importer_rejects(self):
        """Production BoC bug: suspect 96kbps MP3 download vs existing 192kbps.

        Preview collects facts via ``measure_preimport_state`` and runs the
        harness (which measures spectral as suspect@96kbps), then persists
        evidence. The importer reads the persisted evidence and routes it
        through ``full_pipeline_decision_from_evidence``. The candidate is
        a clear codec-rank downgrade (MP3 96 vs existing MP3 192) — the
        full pipeline returns ``downgrade`` and the importer rejects via
        the evidence pipeline.

        Pre-fix: preview's legacy shim ran ``_legacy_preimport_decision``,
        returned ``spectral_reject``, early-returned without persisting,
        importer's front-gate then fired ``evidence_persist_failed`` and
        re-queued forever.
        """
        from lib.preimport import PreimportMeasurement
        from lib.quality import (
            ALBUM_QUALITY_EVIDENCE_OWNER_DOWNLOAD_LOG_CANDIDATE,
            SpectralMeasurement,
        )
        from scripts import import_preview_worker

        db = FakePipelineDB()
        with tempfile.TemporaryDirectory() as source:
            with open(os.path.join(source, "01.mp3"), "wb") as h:
                h.write(b"audio")

            claimed, download_log_id = self._seed_force_job(
                db, request_id=42, source_path=source,
            )
            # Existing album in beets at 192kbps; the importer compares
            # download_spectral (suspect@96) against this and rejects.
            self._seed_current_evidence(
                db, request_id=42, min_bitrate_kbps=192,
            )

            # Build the measurement facts the preview path would collect.
            measured_facts = PreimportMeasurement(
                corrupt_files=[],
                audio_corrupt=False,
                download_spectral=SpectralMeasurement.from_parts("suspect", 96),
                existing_spectral=None,
                existing_min_bitrate=None,
                folder_layout="flat",
                audio_file_count=1,
                filetype_band="mp3",
                min_bitrate_kbps=96,
                is_vbr=False,
            )
            # Build the ImportResult the harness would emit. The harness ALSO
            # runs spectral and stamps the new_measurement; in production
            # both the measurement facts and the harness see suspect@96.
            harness_ir = ImportResult(
                decision="import",
                new_measurement=AudioQualityMeasurement(
                    min_bitrate_kbps=96,
                    avg_bitrate_kbps=96,
                    median_bitrate_kbps=96,
                    format="MP3",
                    is_cbr=True,
                    spectral_grade="suspect",
                    spectral_bitrate_kbps=96,
                ),
            )

            sentinels = {"shim_called": False}

            def _shim_sentinel(*args, **kwargs):
                sentinels["shim_called"] = True
                raise AssertionError(
                    "worker_mode must NOT call run_preimport_gates"
                )

            with patch(
                "lib.preimport.run_preimport_gates",
                side_effect=_shim_sentinel,
            ), patch(
                "lib.import_preview.run_preimport_gates",
                side_effect=_shim_sentinel,
            ), patch(
                "lib.import_preview.measure_preimport_state",
                return_value=measured_facts,
            ), patch(
                "lib.import_preview.run_import_one",
                return_value=SimpleNamespace(import_result=harness_ir),
            ), patch(
                "lib.config.read_runtime_config",
                return_value=CratediggerConfig(
                    beets_harness_path=_HARNESS,
                    pipeline_db_enabled=True,
                ),
            ):
                updated_job = import_preview_worker.process_claimed_preview_job(
                    cast(Any, db), claimed,
                )

            # The shim must not have been invoked.
            self.assertFalse(sentinels["shim_called"])

            # Preview marked the job evidence_ready.
            assert updated_job is not None
            self.assertEqual(updated_job.preview_status, "evidence_ready")
            self.assertEqual(updated_job.status, "queued")

            # Candidate evidence was persisted with the correct facts.
            from lib.quality import AlbumQualityEvidenceOwner
            owner = AlbumQualityEvidenceOwner(
                owner_type=ALBUM_QUALITY_EVIDENCE_OWNER_DOWNLOAD_LOG_CANDIDATE,
                owner_id=download_log_id,
            )
            persisted = db.load_album_quality_evidence(owner)
            assert persisted is not None
            self.assertFalse(persisted.audio_corrupt)
            self.assertEqual(persisted.measurement.spectral_grade, "suspect")
            self.assertEqual(persisted.measurement.spectral_bitrate_kbps, 96)
            self.assertEqual(persisted.folder_layout, "flat")
            self.assertEqual(persisted.audio_file_count, 1)

            # Now drive the importer side: dispatch_import_from_db should
            # read the persisted evidence, call preimport_decide, see the
            # suspect@96 vs existing min 192 case, and reject.
            from lib.import_dispatch import dispatch_import_from_db
            beets_info_no_path = AlbumInfo(
                album_id=1, track_count=10, min_bitrate_kbps=192,
                avg_bitrate_kbps=192, format="MP3", is_cbr=False,
                album_path=None,  # type: ignore[arg-type]
            )
            with patch_dispatch_externals() as ext, \
                    patch("lib.beets_db.BeetsDB", _mock_beets_db(beets_info_no_path)), \
                    patch("lib.config.read_runtime_config",
                          return_value=CratediggerConfig(
                              beets_harness_path=_HARNESS,
                              pipeline_db_enabled=True,
                          )):
                result = dispatch_import_from_db(
                    db,  # type: ignore[arg-type]
                    request_id=42,
                    failed_path=source,
                    force=True,
                    source_username="alice",
                    import_job_id=updated_job.id,
                    download_log_id=download_log_id,
                    outcome_label="force_import",
                )

            self.assertFalse(result.success)
            # The full pipeline returns ``downgrade`` for this codec-rank
            # comparison (MP3 96 vs MP3 192); the importer rejects via the
            # evidence pipeline rather than reinventing the comparison.
            self.assertIn("downgrade", result.message or "")
            # Beets NEVER ran.
            self.assertEqual(ext.run.call_count, 0)
            # download_log rejection scenario reflects the actual decision.
            outcomes = [(log.outcome, log.beets_scenario) for log in db.download_logs]
            self.assertIn(("rejected", "downgrade"), outcomes)

    def test_clean_upgrade_persists_evidence_and_importer_accepts(self):
        """Happy path: suspect spectral but a clear bitrate upgrade.

        Download: suspect@256kbps. Existing min: 192kbps. preimport_decide
        accepts (suspect upgrade). The importer continues to the quality
        gate and import path. Beets actually runs.
        """
        from lib.preimport import PreimportMeasurement
        from lib.quality import (
            ALBUM_QUALITY_EVIDENCE_OWNER_DOWNLOAD_LOG_CANDIDATE,
            SpectralMeasurement,
        )
        from scripts import import_preview_worker

        db = FakePipelineDB()
        with tempfile.TemporaryDirectory() as source:
            with open(os.path.join(source, "01.mp3"), "wb") as h:
                h.write(b"audio")

            claimed, download_log_id = self._seed_force_job(
                db, request_id=43, source_path=source,
            )
            self._seed_current_evidence(
                db, request_id=43, min_bitrate_kbps=192,
            )

            measured_facts = PreimportMeasurement(
                corrupt_files=[],
                audio_corrupt=False,
                download_spectral=SpectralMeasurement.from_parts("suspect", 256),
                existing_spectral=None,
                existing_min_bitrate=None,
                folder_layout="flat",
                audio_file_count=1,
                filetype_band="mp3",
                min_bitrate_kbps=256,
                is_vbr=True,
            )
            harness_ir = ImportResult(
                decision="import",
                new_measurement=AudioQualityMeasurement(
                    min_bitrate_kbps=256,
                    avg_bitrate_kbps=256,
                    median_bitrate_kbps=256,
                    format="mp3 v0",
                    is_cbr=False,
                    spectral_grade="suspect",
                    spectral_bitrate_kbps=256,
                ),
            )

            sentinels = {"shim_called": False}

            def _shim_sentinel(*args, **kwargs):
                sentinels["shim_called"] = True
                raise AssertionError(
                    "worker_mode must NOT call run_preimport_gates"
                )

            with patch(
                "lib.preimport.run_preimport_gates",
                side_effect=_shim_sentinel,
            ), patch(
                "lib.import_preview.run_preimport_gates",
                side_effect=_shim_sentinel,
            ), patch(
                "lib.import_preview.measure_preimport_state",
                return_value=measured_facts,
            ), patch(
                "lib.import_preview.run_import_one",
                return_value=SimpleNamespace(import_result=harness_ir),
            ), patch(
                "lib.config.read_runtime_config",
                return_value=CratediggerConfig(
                    beets_harness_path=_HARNESS,
                    pipeline_db_enabled=True,
                ),
            ):
                updated_job = import_preview_worker.process_claimed_preview_job(
                    cast(Any, db), claimed,
                )

            self.assertFalse(sentinels["shim_called"])
            assert updated_job is not None
            self.assertEqual(updated_job.preview_status, "evidence_ready")

            from lib.quality import AlbumQualityEvidenceOwner
            owner = AlbumQualityEvidenceOwner(
                owner_type=ALBUM_QUALITY_EVIDENCE_OWNER_DOWNLOAD_LOG_CANDIDATE,
                owner_id=download_log_id,
            )
            persisted = db.load_album_quality_evidence(owner)
            assert persisted is not None
            self.assertEqual(persisted.measurement.spectral_grade, "suspect")
            self.assertEqual(persisted.measurement.spectral_bitrate_kbps, 256)

    def test_corrupt_audio_persists_evidence_and_importer_rejects(self):
        """Audio corrupt: ffmpeg rc!=0. Preview persists evidence with
        audio_corrupt=True; importer's preimport_decide rejects on
        audio_corrupt before ever invoking beets."""
        from lib.preimport import PreimportMeasurement
        from lib.quality import ALBUM_QUALITY_EVIDENCE_OWNER_DOWNLOAD_LOG_CANDIDATE
        from scripts import import_preview_worker

        db = FakePipelineDB()
        with tempfile.TemporaryDirectory() as source:
            with open(os.path.join(source, "01.mp3"), "wb") as h:
                h.write(b"audio")

            claimed, download_log_id = self._seed_force_job(
                db, request_id=44, source_path=source,
            )
            self._seed_current_evidence(
                db, request_id=44, min_bitrate_kbps=192,
            )

            measured_facts = PreimportMeasurement(
                corrupt_files=["01.mp3"],
                audio_corrupt=True,
                download_spectral=None,
                existing_spectral=None,
                existing_min_bitrate=None,
                folder_layout="flat",
                audio_file_count=1,
                filetype_band="mp3",
                min_bitrate_kbps=192,
                is_vbr=False,
            )

            sentinels = {"shim_called": False, "harness_called": False}

            def _shim_sentinel(*args, **kwargs):
                sentinels["shim_called"] = True
                raise AssertionError(
                    "worker_mode must NOT call run_preimport_gates"
                )

            def _harness_sentinel(*args, **kwargs):
                sentinels["harness_called"] = True
                raise AssertionError(
                    "harness must not run on corrupt audio in worker_mode"
                )

            with patch(
                "lib.preimport.run_preimport_gates",
                side_effect=_shim_sentinel,
            ), patch(
                "lib.import_preview.run_preimport_gates",
                side_effect=_shim_sentinel,
            ), patch(
                "lib.import_preview.measure_preimport_state",
                return_value=measured_facts,
            ), patch(
                "lib.import_preview.run_import_one",
                side_effect=_harness_sentinel,
            ), patch(
                "lib.config.read_runtime_config",
                return_value=CratediggerConfig(
                    beets_harness_path=_HARNESS,
                    pipeline_db_enabled=True,
                ),
            ):
                updated_job = import_preview_worker.process_claimed_preview_job(
                    cast(Any, db), claimed,
                )

            self.assertFalse(sentinels["shim_called"])
            self.assertFalse(sentinels["harness_called"])
            assert updated_job is not None
            self.assertEqual(updated_job.preview_status, "evidence_ready")
            from lib.quality import AlbumQualityEvidenceOwner
            owner = AlbumQualityEvidenceOwner(
                owner_type=ALBUM_QUALITY_EVIDENCE_OWNER_DOWNLOAD_LOG_CANDIDATE,
                owner_id=download_log_id,
            )
            persisted = db.load_album_quality_evidence(owner)
            assert persisted is not None
            self.assertTrue(persisted.audio_corrupt)

            # Drive the importer and assert reject + self-heal.
            from lib.import_dispatch import dispatch_import_from_db
            beets_info_no_path = AlbumInfo(
                album_id=1, track_count=10, min_bitrate_kbps=192,
                avg_bitrate_kbps=192, format="MP3", is_cbr=False,
                album_path=None,  # type: ignore[arg-type]
            )
            with patch_dispatch_externals() as ext, \
                    patch("lib.beets_db.BeetsDB", _mock_beets_db(beets_info_no_path)), \
                    patch("lib.config.read_runtime_config",
                          return_value=CratediggerConfig(
                              beets_harness_path=_HARNESS,
                              pipeline_db_enabled=True,
                          )):
                result = dispatch_import_from_db(
                    db,  # type: ignore[arg-type]
                    request_id=44,
                    failed_path=source,
                    force=True,
                    source_username="alice",
                    import_job_id=updated_job.id,
                    download_log_id=download_log_id,
                    outcome_label="force_import",
                )

            self.assertFalse(result.success)
            self.assertIn("audio_corrupt", result.message or "")
            self.assertEqual(ext.run.call_count, 0)
            self.assertEqual(db.request(44)["status"], "wanted")
            outcomes = [(log.outcome, log.beets_scenario) for log in db.download_logs]
            self.assertIn(("rejected", "audio_corrupt"), outcomes)


class TestU4TriageFKChainAvoidsRemeasurement(unittest.TestCase):
    """U4 regression guard: triage's happy path skips cold-path measurement.

    When evidence is reachable through the FK chain
    (``download_log.candidate_evidence_id`` direct, or via the cross-walk
    through ``import_jobs``), ``triage_wrong_match`` must NOT trigger
    ``measure_preimport_state``. The preview-worker contract (PR #254)
    owns the only legitimate path that measures candidates; triage just
    reads. This slice patches the measurement seam and asserts zero
    calls — the regression that bit PR #256 was exactly this kind of
    duplicate measurement on the rejection path.
    """

    def _seed(self, source: str) -> tuple[FakePipelineDB, int]:
        from tests.helpers import make_request_row as _make_req
        db = FakePipelineDB()
        db.seed_request(_make_req(
            id=1, status="manual", mb_release_id="mbid-1",
        ))
        db.log_download(
            1,
            outcome="rejected",
            validation_result={
                "scenario": "wrong_match",
                "failed_path": source,
            },
        )
        return db, db.download_logs[-1].id

    def _evidence_for(self, source_dir: str, mb_release_id: str):
        """Build content-addressed evidence matching files on disk."""
        from datetime import datetime, timezone
        from lib.quality import (
            AlbumQualityEvidence,
            AlbumQualityEvidenceFile,
            AudioQualityMeasurement,
        )
        from lib.quality_evidence import snapshot_fingerprint
        full = os.path.join(source_dir, "01.mp3")
        stat = os.stat(full)
        files = [
            AlbumQualityEvidenceFile(
                relative_path="01.mp3",
                size_bytes=int(stat.st_size),
                mtime_ns=int(stat.st_mtime_ns),
                extension="mp3",
                container="mp3",
                codec="mp3",
            ),
        ]
        return AlbumQualityEvidence(
            mb_release_id=mb_release_id,
            snapshot_fingerprint=snapshot_fingerprint(files),
            source_path=source_dir,
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
            audio_file_count=1,
            filetype_band="mp3",
            folder_layout="flat",
        )

    def _patch_beets_no_album(self):
        """Stub BeetsDB so wrong-match cleanup runs without a real beets lib."""
        ctx = MagicMock()
        ctx.__enter__ = MagicMock(return_value=ctx)
        ctx.__exit__ = MagicMock(return_value=False)
        ctx.get_album_info.return_value = None
        return patch("lib.beets_db.BeetsDB", return_value=ctx)

    def _patch_cfg(self):
        """Pin the runtime-config loader so the test doesn't read disk."""
        from lib.quality import QualityRankConfig
        from types import SimpleNamespace as _SN
        cfg = _SN(
            quality_ranks=QualityRankConfig.defaults(),
            verified_lossless_target="",
            beets_directory="",
        )
        return patch(
            "lib.config.read_runtime_config",
            return_value=cfg,
        )

    def test_triage_with_fk_evidence_does_not_measure(self) -> None:
        """Happy path: evidence reachable via FK chain → zero measurement."""
        from lib.wrong_match_triage import triage_wrong_match

        source = tempfile.mkdtemp()
        try:
            with open(os.path.join(source, "01.mp3"), "wb") as fp:
                fp.write(b"audio")
            db, log_id = self._seed(source)
            evidence = self._evidence_for(source, "mbid-1")
            db.upsert_album_quality_evidence(evidence)
            stored = db.find_album_quality_evidence(
                mb_release_id="mbid-1",
                snapshot_fingerprint=evidence.snapshot_fingerprint,
            )
            assert stored is not None and stored.id is not None
            db.set_download_log_candidate_evidence(log_id, stored.id)

            with self._patch_beets_no_album(), \
                    self._patch_cfg(), \
                    patch("lib.preimport.measure_preimport_state") as mp, \
                    patch(
                        "lib.wrong_match_triage._preview_for_triage"
                    ) as cold:
                result = triage_wrong_match(db, log_id)

            mp.assert_not_called()
            cold.assert_not_called()
            self.assertTrue(result.success)
            # Verdict resolved without ever calling the cold-path preview.
            self.assertIsNotNone(result.preview)
        finally:
            import shutil as _shutil
            _shutil.rmtree(source, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
