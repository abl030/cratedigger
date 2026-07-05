"""Tests for lib/download.py — download processing functions.

Tests _build_download_info, cancel_and_delete, slskd_download_status,
downloads_all_done, poll_active_downloads, grab_most_wanted.

Pre-import measurement behavior (audio integrity + spectral analysis) is
shared with the force/manual import paths and tested directly against
``lib.measurement.measure_preimport_state`` in ``tests/test_measurement.py``
and end-to-end through
``tests/test_integration_slices.py::TestSpectralPropagationSlice``.
"""

import unittest
from unittest.mock import MagicMock, patch, PropertyMock
import logging
import os
import shutil
import tempfile
import time
from datetime import datetime, timezone, timedelta
from typing import Any, TYPE_CHECKING, cast

from lib.download_processing import Materialized, MaterializeFailed, MaterializeGuarded
from lib.slskd_client import TransferSnapshot
from tests.helpers import (
    make_ctx_with_fake_db,
    make_download_directory,
    make_download_file,
    make_download_user,
    make_grab_list_entry,
    make_request_row,
    make_transfer_snapshot,
)
from tests.fakes import FakePipelineDB, FakePipelineDBSource, FakeSlskdAPI


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _make_ctx(cfg=None, slskd=None, pipeline_db_source=None):
    """Build a mock CratediggerContext."""
    from lib.context import CratediggerContext
    if cfg is None:
        cfg = MagicMock()
        cfg.slskd_download_dir = "/tmp/test_downloads"
        cfg.beets_validation_enabled = False
        cfg.beets_distance_threshold = 0.15
        cfg.beets_staging_dir = "/tmp/staging"
        cfg.beets_harness_path = "/tmp/harness"
        cfg.audio_check_mode = "normal"
        cfg.stalled_timeout = 300
        cfg.remote_queue_timeout = 120
        cfg.slskd_host_url = "http://localhost:5030"
        cfg.slskd_url_base = "/"
        cfg.pipeline_db_enabled = True
        cfg.meelo_url = None
    if slskd is None:
        slskd = FakeSlskdAPI()
    if pipeline_db_source is None:
        pipeline_db_source = FakePipelineDBSource()
    return CratediggerContext(cfg=cfg, slskd=slskd,
                          pipeline_db_source=pipeline_db_source)


class TestBuildDownloadInfo(unittest.TestCase):

    def test_basic(self):
        from lib.dispatch import _build_download_info
        files = [make_download_file(bitRate=320, sampleRate=44100)]
        album = make_grab_list_entry(files=files)
        dl = _build_download_info(album)
        self.assertEqual(dl.username, "user1")
        self.assertEqual(dl.filetype, "mp3")
        self.assertEqual(dl.bitrate, 320)
        self.assertEqual(dl.sample_rate, 44100)

    def test_empty_files(self):
        from lib.dispatch import _build_download_info
        album = make_grab_list_entry(files=[])
        dl = _build_download_info(album)
        self.assertIsNone(dl.username)
        self.assertIsNone(dl.filetype)

    def test_multi_user(self):
        from lib.dispatch import _build_download_info
        files = [
            make_download_file(username="beta_user"),
            make_download_file(username="alpha_user"),
        ]
        album = make_grab_list_entry(files=files)
        dl = _build_download_info(album)
        self.assertEqual(dl.username, "alpha_user, beta_user")


class TestPostRejectionWrongMatchTriage(unittest.TestCase):
    def test_runs_cleanup_for_new_wrong_match_log_row(self):
        from lib.download_processing import _run_post_rejection_wrong_match_cleanup

        db = FakePipelineDB()
        ctx = make_ctx_with_fake_db(db)

        with patch("lib.wrong_match_cleanup_service.cleanup_wrong_match") as cleanup:
            result = _run_post_rejection_wrong_match_cleanup(
                ctx,
                123,
                scenario="high_distance",
            )

        cleanup.assert_called_once_with(
            db,
            123,
            ignore_import_job_id=None,
        )
        self.assertIs(result, cleanup.return_value)

    def test_copies_import_job_candidate_evidence_before_cleanup(self):
        from lib.download_processing import _run_post_rejection_wrong_match_cleanup

        db = FakePipelineDB()
        db.seed_request(make_request_row(id=1, status="wanted"))
        log_id = db.log_download(
            1,
            outcome="rejected",
            validation_result={"scenario": "wrong_match", "failed_path": "/tmp/source"},
        )
        job = db.enqueue_import_job(
            "automation_import",
            request_id=1,
            payload={},
        )
        db.set_import_job_candidate_evidence(job.id, 44)
        ctx = make_ctx_with_fake_db(db)

        with patch("lib.wrong_match_cleanup_service.cleanup_wrong_match") as cleanup:
            result = _run_post_rejection_wrong_match_cleanup(
                ctx,
                log_id,
                scenario="high_distance",
                import_job_id=job.id,
            )

        self.assertEqual(db.get_download_log_candidate_evidence_id(log_id), 44)
        cleanup.assert_called_once_with(
            db,
            log_id,
            ignore_import_job_id=job.id,
        )
        self.assertIs(result, cleanup.return_value)

    def test_skips_bad_file_rejections(self):
        from lib.download_processing import _run_post_rejection_wrong_match_cleanup

        db = FakePipelineDB()
        ctx = make_ctx_with_fake_db(db)

        with patch("lib.wrong_match_cleanup_service.cleanup_wrong_match") as cleanup:
            result = _run_post_rejection_wrong_match_cleanup(
                ctx,
                123,
                scenario="spectral_reject",
            )

        cleanup.assert_not_called()
        self.assertIsNone(result)

    def test_rejected_download_handler_triggers_triage_after_logging(self):
        from lib.download_processing import _handle_rejected_result
        from lib.quality import ValidationResult
        from lib.staged_album import StagedAlbum
        import tempfile

        class Source:
            def __init__(self, db):
                self.db = db
                self.rejected = False
                self.reject_args = None
                self.reject_kwargs = None

            def _get_db(self):
                return self.db

            def reject_and_requeue(self, *args, **kwargs):
                self.rejected = True
                self.reject_args = args
                self.reject_kwargs = kwargs
                return 77

        db = FakePipelineDB()
        source = Source(db)
        ctx = _make_ctx(pipeline_db_source=source)

        with tempfile.TemporaryDirectory() as tmpdir:
            current_path = os.path.join(tmpdir, "Artist - Album")
            os.makedirs(current_path)
            with open(os.path.join(current_path, "01 - Track.mp3"), "w",
                      encoding="utf-8") as fp:
                fp.write("audio")
            cfg = cast(Any, ctx.cfg)
            cfg.beets_tracking_file = os.path.join(tmpdir, "tracking.jsonl")
            album = make_grab_list_entry(
                files=[make_download_file(username="user1")],
                artist="Artist",
                title="Album",
                mb_release_id="test-mbid",
                db_request_id=42,
            )
            result = ValidationResult(
                valid=False,
                distance=0.4,
                scenario="high_distance",
                detail="too far",
            )

            with patch("lib.wrong_match_cleanup_service.cleanup_wrong_match") as cleanup:
                _handle_rejected_result(
                    album,
                    result,
                    StagedAlbum(current_path=current_path, request_id=42),
                    ctx,
                )

        self.assertTrue(source.rejected)
        assert source.reject_args is not None
        stored = source.reject_args[1]
        self.assertEqual(stored.source_dirs, ["user1\\Music"])
        cleanup.assert_called_once_with(
            db,
            77,
            ignore_import_job_id=None,
        )


class TestRequestScopedAutoImportPath(unittest.TestCase):

    CASES = [
        (
            "under auto-import root with request suffix",
            "/tmp/staging/auto-import/Artist/Album [request-42]",
            "/tmp/staging",
            True,
        ),
        (
            "under auto-import root without request suffix",
            "/tmp/staging/auto-import/Artist/Album",
            "/tmp/staging",
            False,
        ),
        (
            "request suffix outside auto-import root",
            "/tmp/downloads/Artist/Album [request-42]",
            "/tmp/staging",
            False,
        ),
        (
            "request suffix under post-validation root",
            "/tmp/staging/post-validation/Artist/Album [request-42]",
            "/tmp/staging",
            False,
        ),
    ]

    def test_matches_only_request_scoped_auto_import_paths(self):
        from lib.download_processing import _is_request_scoped_auto_import_path

        for desc, current_path, staging_dir, expected in self.CASES:
            with self.subTest(desc=desc):
                self.assertEqual(
                    _is_request_scoped_auto_import_path(
                        current_path=current_path,
                        staging_dir=staging_dir,
                    ),
                    expected,
                )


class TestResolveRequestRejectionId(unittest.TestCase):

    def test_refuses_release_id_presence_mismatch(self):
        from lib.download_processing import _resolved_request_rejection_id

        for desc, row_mbid, album_mbid in [
            ("row missing mbid", "", "test-mbid"),
            ("album missing mbid", "test-mbid", ""),
        ]:
            with self.subTest(desc=desc):
                db = FakePipelineDB()
                db.seed_request(make_request_row(
                    id=1,
                    status="downloading",
                    artist_name="Artist",
                    album_title="Album",
                    year=2024,
                    mb_release_id=row_mbid,
                ))
                cfg = cast(Any, _make_ctx().cfg)
                ctx = make_ctx_with_fake_db(db, cfg=cfg)
                album = make_grab_list_entry(
                    album_id=1,
                    artist="Artist",
                    title="Album",
                    year="2024",
                    mb_release_id=album_mbid,
                    db_request_id=None,
                    db_source="request",
                )

                resolved_db, request_id = _resolved_request_rejection_id(album, ctx)

                self.assertIs(resolved_db, db)
                self.assertIsNone(request_id)


## TestGatherSpectralContext and TestCheckQualityGateDecision removed:
## - TestGatherSpectralContext never called the function it claimed to test —
##   it reimplemented the condition logic in test code and asserted on that.
## - TestCheckQualityGateDecision duplicated tests already in
##   test_quality_decisions.py::TestQualityGateDecision.


# === NEW tests for functions moving to lib/download.py ===

class TestDownloadsAllDone(unittest.TestCase):
    """downloads_all_done is pure logic — test all branches."""

    def test_all_succeeded(self):
        from lib.slskd_transfers import downloads_all_done
        files = [make_download_file(), make_download_file()]
        files[0].status = make_transfer_snapshot(state="Completed, Succeeded")
        files[1].status = make_transfer_snapshot(state="Completed, Succeeded")
        done, problems, queued = downloads_all_done(files)
        self.assertTrue(done)
        self.assertIsNone(problems)
        self.assertEqual(queued, 0)

    def test_one_errored(self):
        from lib.slskd_transfers import downloads_all_done
        files = [make_download_file(), make_download_file()]
        files[0].status = make_transfer_snapshot(state="Completed, Succeeded")
        files[1].status = make_transfer_snapshot(state="Completed, Errored")
        done, problems, queued = downloads_all_done(files)
        self.assertFalse(done)
        self.assertIsNotNone(problems)
        assert problems is not None
        self.assertEqual(len(problems), 1)
        self.assertEqual(queued, 0)

    def test_queued_remotely(self):
        from lib.slskd_transfers import downloads_all_done
        files = [make_download_file(), make_download_file()]
        files[0].status = make_transfer_snapshot(state="Completed, Succeeded")
        files[1].status = make_transfer_snapshot(state="Queued, Remotely")
        done, problems, queued = downloads_all_done(files)
        self.assertFalse(done)
        self.assertIsNone(problems)
        self.assertEqual(queued, 1)

    def test_all_error_states(self):
        """Every error state should appear in problems list."""
        from lib.slskd_transfers import downloads_all_done
        error_states = [
            "Completed, Cancelled",
            "Completed, TimedOut",
            "Completed, Errored",
            "Completed, Rejected",
            "Completed, Aborted",
        ]
        for state in error_states:
            files = [make_download_file()]
            files[0].status = make_transfer_snapshot(state=state)
            done, problems, _ = downloads_all_done(files)
            self.assertFalse(done, f"state={state} should not be done")
            self.assertIsNotNone(problems, f"state={state} should be a problem")

    def test_none_status_skipped(self):
        from lib.slskd_transfers import downloads_all_done
        files = [make_download_file()]
        files[0].status = None
        done, problems, queued = downloads_all_done(files)
        # None status means we can't confirm done
        self.assertTrue(done)  # loop body skips None
        self.assertIsNone(problems)


class TestCancelAndDelete(unittest.TestCase):
    """cancel_and_delete deletes completed payloads at their authoritative
    (event-derived) local paths — never at inferred folder locations
    (issue #146 phase 3; kills the shared-``CD1/`` rmtree hazard)."""

    def _ctx(self, slskd=None):
        slskd = slskd or FakeSlskdAPI()
        ctx = _make_ctx(slskd=slskd)
        tmpdir = tempfile.mkdtemp(prefix="cratedigger-cancel-test-")
        self.addCleanup(shutil.rmtree, tmpdir, ignore_errors=True)
        cast(Any, ctx.cfg).slskd_download_dir = tmpdir
        return ctx, slskd, tmpdir

    def _file_event(self, slskd, *, id, username, filename, local_filename):
        import json as _json
        return slskd.events.make_event(
            id=id, timestamp="2026-07-02T10:00:00.0000000Z",
            type="DownloadFileComplete",
            data=_json.dumps({
                "version": 0,
                "localFilename": local_filename,
                "remoteFilename": filename,
                "transfer": {
                    "id": f"tid-{id}", "username": username,
                    "filename": filename, "size": 10,
                },
            }))

    def _dir_event(self, slskd, *, id, username, remote_dir, local_dir):
        import json as _json
        return slskd.events.make_event(
            id=id, timestamp="2026-07-02T10:00:00.0000000Z",
            type="DownloadDirectoryComplete",
            data=_json.dumps({
                "version": 0,
                "localDirectoryName": local_dir,
                "remoteDirectoryName": remote_dir,
                "username": username,
            }))

    def test_cancels_and_deletes_stamped_file_pruning_empty_dir(self):
        from lib.slskd_transfers import cancel_and_delete
        ctx, slskd, tmpdir = self._ctx()
        local_dir = os.path.join(tmpdir, "Album Folder")
        os.makedirs(local_dir)
        local_path = os.path.join(local_dir, "01 - Track.mp3")
        with open(local_path, "w") as fp:
            fp.write("x")
        f = make_download_file(file_dir="someuser\\Album Folder")
        f.local_path = local_path

        ok = cancel_and_delete([f], ctx)

        self.assertTrue(ok)
        self.assertEqual(
            [(call.username, call.id)
             for call in slskd.transfers.cancel_download_calls],
            [("user1", "file-id-1")],
        )
        self.assertFalse(os.path.exists(local_path))
        self.assertFalse(os.path.isdir(local_dir))

    def test_shared_dir_survives_when_other_files_remain(self):
        """The CD1 regression: deleting one request's file must not take
        an unrelated sibling in the same on-disk folder with it."""
        from lib.slskd_transfers import cancel_and_delete
        ctx, _, tmpdir = self._ctx()
        local_dir = os.path.join(tmpdir, "CD1")
        os.makedirs(local_dir)
        mine = os.path.join(local_dir, "01 - Mine.mp3")
        theirs = os.path.join(local_dir, "01 - Theirs.mp3")
        for path in (mine, theirs):
            with open(path, "w") as fp:
                fp.write("x")
        f = make_download_file(file_dir="someuser\\CD1")
        f.local_path = mine

        cancel_and_delete([f], ctx)

        self.assertFalse(os.path.exists(mine))
        self.assertTrue(os.path.exists(theirs))
        self.assertTrue(os.path.isdir(local_dir))

    def test_unstamped_file_resolved_via_fresh_events(self):
        """A file that completed after the cycle's ingest pass has no
        stamp yet — a fresh events-page lookup still finds its payload."""
        from lib.slskd_transfers import cancel_and_delete
        ctx, slskd, tmpdir = self._ctx()
        local_dir = os.path.join(tmpdir, "Album Folder")
        os.makedirs(local_dir)
        local_path = os.path.join(local_dir, "01 - Track.mp3")
        with open(local_path, "w") as fp:
            fp.write("x")
        f = make_download_file(
            filename="someuser\\Music\\Album Folder\\01 - Track.mp3",
            file_dir="someuser\\Music\\Album Folder")
        slskd.events.set_events([self._file_event(
            slskd, id="ev-1", username=f.username, filename=f.filename,
            local_filename=local_path)])

        cancel_and_delete([f], ctx)

        self.assertFalse(os.path.exists(local_path))
        self.assertFalse(os.path.isdir(local_dir))

    def test_directory_event_prunes_empty_authoritative_dir(self):
        from lib.slskd_transfers import cancel_and_delete
        ctx, slskd, tmpdir = self._ctx()
        local_dir = os.path.join(tmpdir, "Album Folder")
        os.makedirs(local_dir)  # already emptied by an earlier cleanup
        f = make_download_file(
            filename="someuser\\Music\\Album Folder\\01 - Track.mp3",
            file_dir="someuser\\Music\\Album Folder")
        slskd.events.set_events([self._dir_event(
            slskd, id="ev-1", username=f.username,
            remote_dir=f.file_dir, local_dir=local_dir)])

        cancel_and_delete([f], ctx)

        self.assertFalse(os.path.isdir(local_dir))

    def test_never_deletes_outside_download_root(self):
        from lib.slskd_transfers import cancel_and_delete
        ctx, _, _ = self._ctx()
        outside = tempfile.mkdtemp(prefix="cratedigger-outside-")
        self.addCleanup(shutil.rmtree, outside, ignore_errors=True)
        stray = os.path.join(outside, "01 - Track.mp3")
        with open(stray, "w") as fp:
            fp.write("x")
        f = make_download_file()
        f.local_path = stray

        cancel_and_delete([f], ctx)

        self.assertTrue(os.path.exists(stray))

    def test_cancel_failure_continues(self):
        """Should not raise if cancel_download throws."""
        from lib.slskd_transfers import cancel_and_delete
        slskd = FakeSlskdAPI()
        slskd.transfers.cancel_download_error = Exception("network error")
        ctx, slskd, _ = self._ctx(slskd)
        f = make_download_file()
        with self.assertLogs("cratedigger", level="WARNING") as logs:
            ok = cancel_and_delete([f], ctx)  # should not raise
        self.assertFalse(ok)
        self.assertIn("Failed to cancel download", "\n".join(logs.output))
        self.assertEqual(
            [(call.username, call.id)
             for call in slskd.transfers.cancel_download_calls],
            [("user1", "file-id-1")],
        )

    def test_events_lookup_failure_never_blocks_cancel(self):
        from lib.slskd_transfers import cancel_and_delete
        slskd = FakeSlskdAPI()
        slskd.events.list_error = RuntimeError("events down")
        ctx, slskd, _ = self._ctx(slskd)
        f = make_download_file()  # unstamped → triggers the events lookup

        ok = cancel_and_delete([f], ctx)  # must not raise

        self.assertTrue(ok)
        self.assertEqual(len(slskd.transfers.cancel_download_calls), 1)


class TestSlskdDownloadStatus(unittest.TestCase):
    """slskd_download_status matches locally against a pre-fetched bulk
    snapshot — issue #508 removed the per-file network fallback (dead:
    every live caller already passes ``snapshot=cycle_snapshot``)."""

    def test_bulk_snapshot_populates_status(self):
        """When snapshot is provided, use match_transfer instead of per-file API."""
        from lib.slskd_transfers import slskd_download_status
        f = make_download_file(filename="Music\\01 - Track.mp3", username="user1")
        snapshot = [make_download_user(username="user1", directories=[
            make_download_directory(directory="", files=[
                make_transfer_snapshot(
                    filename="Music\\01 - Track.mp3",
                    id="file-id-1",
                    state="Completed, Succeeded",
                    size=5000000,
                ),
            ]),
        ])]
        ok = slskd_download_status([f], snapshot=snapshot)
        self.assertTrue(ok)
        self.assertIsNotNone(f.status)
        assert f.status is not None
        self.assertEqual(f.status.state, "Completed, Succeeded")

    def test_bulk_snapshot_file_not_found(self):
        """When snapshot doesn't contain the file, status is None, returns False."""
        from lib.slskd_transfers import slskd_download_status
        f = make_download_file(filename="Music\\missing.mp3", username="user1")
        snapshot = [make_download_user(username="user1", directories=[
            make_download_directory(directory="", files=[]),
        ])]
        ok = slskd_download_status([f], snapshot=snapshot)
        self.assertFalse(ok)
        self.assertIsNone(f.status)


class TestSlskdDoEnqueue(unittest.TestCase):

    def test_successful_enqueue(self):
        from lib.slskd_transfers import slskd_do_enqueue
        slskd = FakeSlskdAPI(downloads=[{
            "username": "user1",
            "directories": [{
                "directory": "user1\\Music",
                "files": [{"filename": "track.mp3", "id": "new-id"}],
            }],
        }])
        ctx = _make_ctx(slskd=slskd)
        files = [{"filename": "track.mp3", "size": 5000000}]
        with patch("time.sleep"):
            result = slskd_do_enqueue("user1", files, "user1\\Music", ctx)
        self.assertIsNotNone(result)
        assert result is not None
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].id, "new-id")
        self.assertEqual(slskd.transfers.enqueue_calls[0].files, files)
        self.assertEqual(slskd.transfers.get_all_downloads_calls, [True])

    def test_enqueue_failure_returns_none(self):
        from lib.slskd_transfers import slskd_do_enqueue
        slskd = FakeSlskdAPI()
        slskd.transfers.enqueue_error = Exception("fail")
        ctx = _make_ctx(slskd=slskd)
        with patch("time.sleep"):
            result = slskd_do_enqueue("user1", [], "dir", ctx)
        self.assertIsNone(result)

    def test_enqueue_polls_until_ids_found(self):
        """Transfer IDs appear on 2nd poll — should resolve in 2 iterations, not 5s."""
        from lib.slskd_transfers import slskd_do_enqueue
        snapshot_with_id = [{
            "username": "user1",
            "directories": [{"files": [{"filename": "track.mp3", "id": "tid-1"}]}],
        }]
        snapshot_without_id = [{
            "username": "user1",
            "directories": [{"files": []}],
        }]
        slskd = FakeSlskdAPI(
            download_snapshots=[snapshot_without_id, snapshot_with_id])
        ctx = _make_ctx(slskd=slskd)
        files = [{"filename": "track.mp3", "size": 5000000}]
        with patch("time.sleep"):
            result = slskd_do_enqueue("user1", files, "user1\\Music", ctx)
        self.assertIsNotNone(result)
        assert result is not None
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].id, "tid-1")
        # Should have polled twice
        self.assertEqual(len(slskd.transfers.get_all_downloads_calls), 2)

    def test_enqueue_timeout_tracks_files_without_transfer_ids(self):
        """Transfer IDs never appear — accepted enqueue is tracked for re-derivation."""
        from lib.slskd_transfers import slskd_do_enqueue
        # Never returns the transfer ID
        slskd = FakeSlskdAPI(downloads=[{
            "username": "user1",
            "directories": [{"files": []}],
        }])
        ctx = _make_ctx(slskd=slskd)
        files = [{"filename": "track.mp3", "size": 5000000}]
        with patch("time.sleep"):
            result = slskd_do_enqueue("user1", files, "user1\\Music", ctx)
        self.assertIsNotNone(result)
        assert result is not None
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].filename, "track.mp3")
        self.assertEqual(result[0].id, "")

    def test_enqueue_partial_transfer_reconciliation_tracks_every_file(self):
        """Accepted enqueue with partial IDs still tracks every requested file."""
        from lib.slskd_transfers import slskd_do_enqueue
        slskd = FakeSlskdAPI(downloads=[{
            "username": "user1",
            "directories": [{
                "directory": "user1\\Music",
                "files": [{"filename": "track1.mp3", "id": "new-id"}],
            }],
        }])
        ctx = _make_ctx(slskd=slskd)
        files = [
            {"filename": "track1.mp3", "size": 5000000},
            {"filename": "track2.mp3", "size": 5000000},
        ]
        with patch("time.sleep"):
            result = slskd_do_enqueue("user1", files, "user1\\Music", ctx)

        self.assertIsNotNone(result)
        assert result is not None
        self.assertEqual([download.filename for download in result], [
            "track1.mp3",
            "track2.mp3",
        ])
        self.assertEqual([download.id for download in result], ["new-id", ""])
        self.assertEqual(slskd.transfers.cancel_download_calls, [])


class TestSlskdEnqueueWithOutcome(unittest.TestCase):
    """slskd_enqueue_with_outcome must distinguish a hard 'user offline'
    HTTP rejection (verifiable, transfers will never appear) from a
    generic exception (transient, retry next cycle)."""

    def _make_offline_http_error(self, body: str) -> Exception:
        """Build a ``requests.HTTPError`` whose ``.response.text`` mirrors
        what slskd returns when the peer is offline. The detector matches
        structurally on ``.response.text``."""
        from types import SimpleNamespace
        import requests

        err = requests.HTTPError("500 Server Error")
        err.response = SimpleNamespace(text=body)  # type: ignore[attr-defined]
        return err

    def test_offline_http_error_body_returns_rejected(self):
        """The canonical slskd response body 'User pooyork appears to be
        offline' must be classified as rejected — the safety net that
        unblocks the 60s vanish timeout."""
        from lib.slskd_transfers import slskd_enqueue_with_outcome
        slskd = FakeSlskdAPI()
        slskd.transfers.enqueue_error = self._make_offline_http_error(
            "User pooyork appears to be offline")
        ctx = _make_ctx(slskd=slskd)

        with patch("time.sleep"):
            outcome = slskd_enqueue_with_outcome(
                "pooyork", [{"filename": "track.flac", "size": 1}],
                "pooyork\\Music", ctx)

        self.assertEqual(outcome.status, "rejected")
        self.assertIsNone(outcome.downloads)

    def test_offline_http_error_case_insensitive(self):
        """Body match must tolerate slskd-version body casing variants."""
        from lib.slskd_transfers import slskd_enqueue_with_outcome
        slskd = FakeSlskdAPI()
        slskd.transfers.enqueue_error = self._make_offline_http_error(
            "User FOO Appears To BE OFFLINE")
        ctx = _make_ctx(slskd=slskd)

        with patch("time.sleep"):
            outcome = slskd_enqueue_with_outcome(
                "foo", [{"filename": "t.flac", "size": 1}], "foo\\m", ctx)

        self.assertEqual(outcome.status, "rejected")

    def test_non_offline_http_error_returns_unknown(self):
        """HTTPError without the offline marker is still unknown — the
        request stays downloading for poll-cycle recovery."""
        from lib.slskd_transfers import slskd_enqueue_with_outcome
        slskd = FakeSlskdAPI()
        slskd.transfers.enqueue_error = self._make_offline_http_error(
            "internal server error")
        ctx = _make_ctx(slskd=slskd)

        with patch("time.sleep"):
            outcome = slskd_enqueue_with_outcome(
                "user1", [{"filename": "t.flac", "size": 1}], "user1\\m", ctx)

        self.assertEqual(outcome.status, "unknown")

    def test_http_error_with_no_response_returns_unknown(self):
        """Defensive: HTTPError without an attached response should not
        crash — fall through to unknown."""
        from lib.slskd_transfers import slskd_enqueue_with_outcome
        slskd = FakeSlskdAPI()
        slskd.transfers.enqueue_error = Exception("synthetic — no .response")
        ctx = _make_ctx(slskd=slskd)

        with patch("time.sleep"):
            outcome = slskd_enqueue_with_outcome(
                "user1", [{"filename": "t.flac", "size": 1}], "user1\\m", ctx)

        self.assertEqual(outcome.status, "unknown")

    def test_connection_error_returns_unknown(self):
        """Generic non-HTTPError exceptions (network drop, etc.) stay in
        the unknown / ambiguous bucket — only the verifiable user-offline
        body promotes to rejected."""
        from lib.slskd_transfers import slskd_enqueue_with_outcome

        class _ConnectionError(Exception):
            """Synthetic stand-in. ``test_beets_validation.py`` mocks
            ``sys.modules['requests']`` so importing real
            ``requests.ConnectionError`` here yields a non-class type."""
            pass

        slskd = FakeSlskdAPI()
        slskd.transfers.enqueue_error = _ConnectionError("dropped")
        ctx = _make_ctx(slskd=slskd)

        with patch("time.sleep"):
            outcome = slskd_enqueue_with_outcome(
                "user1", [{"filename": "t.flac", "size": 1}], "user1\\m", ctx)

        self.assertEqual(outcome.status, "unknown")

    def test_falsy_enqueue_response_still_returns_rejected(self):
        """Regression guard: when slskd-api ever returns falsy (rather
        than raising), the existing rejected branch still fires."""
        from lib.slskd_transfers import slskd_enqueue_with_outcome
        slskd = FakeSlskdAPI()
        slskd.transfers.enqueue_result = False
        ctx = _make_ctx(slskd=slskd)

        with patch("time.sleep"):
            outcome = slskd_enqueue_with_outcome(
                "user1", [{"filename": "t.flac", "size": 1}], "user1\\m", ctx)

        self.assertEqual(outcome.status, "rejected")

    def test_successful_enqueue_still_returns_accepted(self):
        """Regression guard: the happy path is unchanged."""
        from lib.slskd_transfers import slskd_enqueue_with_outcome
        slskd = FakeSlskdAPI(downloads=[{
            "username": "user1",
            "directories": [{
                "directory": "user1\\Music",
                "files": [{"filename": "track.mp3", "id": "tid-1"}],
            }],
        }])
        ctx = _make_ctx(slskd=slskd)

        with patch("time.sleep"):
            outcome = slskd_enqueue_with_outcome(
                "user1", [{"filename": "track.mp3", "size": 5000000}],
                "user1\\Music", ctx)

        self.assertEqual(outcome.status, "accepted")
        self.assertIsNotNone(outcome.downloads)
        assert outcome.downloads is not None
        self.assertEqual(outcome.downloads[0].id, "tid-1")


class TestGrabMostWanted(unittest.TestCase):
    """grab_most_wanted enqueues and persists state, no blocking monitor."""

    def test_no_albums_returns_zero(self):
        from lib.download import grab_most_wanted
        ctx = _make_ctx()
        search_fn = MagicMock(return_value=({}, [], []))
        count = grab_most_wanted([], search_fn, ctx)
        self.assertEqual(count, 0)

    def test_failed_search_counted(self):
        from lib.download import grab_most_wanted
        from album_source import AlbumRecord
        ctx = _make_ctx()
        failed_album = AlbumRecord(
            id=-1, title="Album", release_date="2024-01-01T00:00:00Z",
            artist_id=0, artist_name="Artist", foreign_artist_id="",
            releases=[], db_request_id=1, db_source="request",
            db_mb_release_id="", db_search_filetype_override=None, db_target_format=None,
        )
        search_fn = MagicMock(return_value=({}, [failed_album], []))
        count = grab_most_wanted([], search_fn, ctx)
        self.assertEqual(count, 1)

    def test_failed_grab_counted(self):
        from lib.download import grab_most_wanted
        from album_source import AlbumRecord
        ctx = _make_ctx()
        failed_album = AlbumRecord(
            id=-1, title="Album", release_date="2024-01-01T00:00:00Z",
            artist_id=0, artist_name="Artist", foreign_artist_id="",
            releases=[], db_request_id=1, db_source="request",
            db_mb_release_id="", db_search_filetype_override=None, db_target_format=None,
        )
        search_fn = MagicMock(return_value=({}, [], [failed_album]))
        count = grab_most_wanted([], search_fn, ctx)
        self.assertEqual(count, 1)

    def test_sets_downloading_status(self):
        """After enqueue, album_requests.status = 'downloading'."""
        from lib.download import grab_most_wanted
        entry = make_grab_list_entry(
            album_id=1,
            filetype="flac",
            title="T",
            artist="A",
            year="2020",
            mb_release_id="mbid",
            db_request_id=42,
            db_source="request",
            files=[make_download_file(
                filename="u\\M\\01.flac",
                id="tid-1",
                file_dir="u\\M",
                username="user1",
                size=30000000,
            )],
        )
        fake_db = FakePipelineDB()
        fake_db.seed_request(make_request_row(id=42, status="wanted"))
        ctx = make_ctx_with_fake_db(fake_db)
        search_fn = MagicMock(return_value=({1: entry}, [], []))
        grab_most_wanted([], search_fn, ctx)
        row = fake_db.request(42)
        self.assertEqual(row["status"], "downloading")
        self.assertEqual(fake_db.status_history, [(42, "downloading")])

    def test_writes_active_download_state(self):
        """JSONB written with correct structure."""
        from lib.download import grab_most_wanted
        import json
        entry = make_grab_list_entry(
            album_id=1,
            filetype="mp3 v0",
            title="T",
            artist="A",
            year="2020",
            mb_release_id="mbid",
            db_request_id=42,
            db_source="request",
            files=[make_download_file(
                filename="u\\M\\01.mp3",
                id="tid-1",
                file_dir="u\\M",
                username="user1",
                size=5000000,
            )],
        )
        fake_db = FakePipelineDB()
        fake_db.seed_request(make_request_row(id=42, status="wanted"))
        ctx = make_ctx_with_fake_db(fake_db)
        search_fn = MagicMock(return_value=({1: entry}, [], []))
        grab_most_wanted([], search_fn, ctx)
        state_raw = fake_db.request(42)["active_download_state"]
        assert isinstance(state_raw, str)
        state = json.loads(state_raw)
        self.assertEqual(state["filetype"], "mp3 v0")
        self.assertEqual(len(state["files"]), 1)

    def test_no_blocking_monitor(self):
        """grab_most_wanted returns immediately without blocking."""
        from lib.download import grab_most_wanted
        import time as _time
        entry = make_grab_list_entry(
            album_id=1,
            filetype="flac",
            title="T",
            artist="A",
            year="2020",
            mb_release_id="mbid",
            db_request_id=42,
            db_source="request",
            files=[make_download_file(
                filename="u\\M\\01.flac",
                id="tid-1",
                file_dir="u\\M",
                username="user1",
                size=30000000,
            )],
        )
        fake_db = FakePipelineDB()
        fake_db.seed_request(make_request_row(id=42, status="wanted"))
        ctx = make_ctx_with_fake_db(fake_db)
        search_fn = MagicMock(return_value=({1: entry}, [], []))
        start = _time.time()
        grab_most_wanted([], search_fn, ctx)
        elapsed = _time.time() - start
        self.assertLess(elapsed, 2.0)  # Must return fast (no blocking loop)


class TestMatchTransferId(unittest.TestCase):
    """Test match_transfer_id() — find slskd transfer ID by filename."""

    def test_exact_filename_match(self):
        from lib.slskd_transfers import match_transfer_id
        downloads = make_download_user(directories=[
            make_download_directory(directory="user\\Music", files=[
                make_transfer_snapshot(filename="user\\Music\\01.flac", id="abc-123"),
                make_transfer_snapshot(filename="user\\Music\\02.flac", id="def-456"),
            ]),
        ])
        result = match_transfer_id(downloads, "user\\Music\\01.flac")
        self.assertEqual(result, "abc-123")

    def test_not_found(self):
        from lib.slskd_transfers import match_transfer_id
        downloads = make_download_user(directories=[
            make_download_directory(directory="user\\Music", files=[]),
        ])
        result = match_transfer_id(downloads, "user\\Music\\missing.flac")
        self.assertIsNone(result)

    def test_multi_directory(self):
        from lib.slskd_transfers import match_transfer_id
        downloads = make_download_user(directories=[
            make_download_directory(directory="d1", files=[
                make_transfer_snapshot(filename="d1\\01.flac", id="id-1"),
            ]),
            make_download_directory(directory="d2", files=[
                make_transfer_snapshot(filename="d2\\01.flac", id="id-2"),
            ]),
        ])
        result = match_transfer_id(downloads, "d2\\01.flac")
        self.assertEqual(result, "id-2")

    def test_bulk_downloads_respects_username(self):
        from lib.slskd_transfers import match_transfer_id
        downloads = [
            make_download_user(username="Mr. Odd", directories=[
                make_download_directory(directory="a", files=[
                    make_transfer_snapshot(filename="shared\\01.flac", id="wrong-id"),
                ]),
            ]),
            make_download_user(username="Miick Starr", directories=[
                make_download_directory(directory="b", files=[
                    make_transfer_snapshot(filename="shared\\01.flac", id="right-id"),
                ]),
            ]),
        ]
        result = match_transfer_id(
            downloads,
            "shared\\01.flac",
            username="Miick Starr",
        )
        self.assertEqual(result, "right-id")

    def test_bulk_downloads_prefers_active_over_old_completed(self):
        from lib.slskd_transfers import match_transfer
        downloads = [
            make_download_user(username="user1", directories=[
                make_download_directory(directory="d", files=[
                    make_transfer_snapshot(
                        filename="shared\\01.flac",
                        id="completed-id",
                        state="Completed, Succeeded",
                        ended_at="2026-04-03T21:00:00+00:00",
                    ),
                    make_transfer_snapshot(
                        filename="shared\\01.flac",
                        id="active-id",
                        state="InProgress",
                        started_at="2026-04-03T22:00:00+00:00",
                    ),
                ]),
            ]),
        ]
        result = match_transfer(downloads, "shared\\01.flac", username="user1")
        assert result is not None
        self.assertEqual(result.id, "active-id")

    def test_bulk_downloads_prefers_latest_successful_attempt(self):
        from lib.slskd_transfers import match_transfer
        downloads = [
            make_download_user(username="user1", directories=[
                make_download_directory(directory="d", files=[
                    make_transfer_snapshot(
                        filename="shared\\01.flac",
                        id="old-cancelled",
                        state="Completed, Cancelled",
                        ended_at="2026-04-03T20:00:00+00:00",
                    ),
                    make_transfer_snapshot(
                        filename="shared\\01.flac",
                        id="new-succeeded",
                        state="Completed, Succeeded",
                        ended_at="2026-04-03T21:00:00+00:00",
                    ),
                ]),
            ]),
        ]
        result = match_transfer(downloads, "shared\\01.flac", username="user1")
        assert result is not None
        self.assertEqual(result.id, "new-succeeded")


class TestRederiveTransferIds(unittest.TestCase):
    """Test rederive_transfer_ids() — re-derive IDs from slskd API."""

    def test_updates_files_in_place(self):
        from lib.slskd_transfers import rederive_transfer_ids
        from lib.grab_list import GrabListEntry, DownloadFile
        entry = GrabListEntry(
            album_id=1, files=[
                DownloadFile(filename="u\\M\\01.flac", id="", file_dir="u\\M",
                             username="user1", size=1000),
                DownloadFile(filename="u\\M\\02.flac", id="", file_dir="u\\M",
                             username="user1", size=2000),
            ],
            filetype="flac", title="T", artist="A", year="2020",
            mb_release_id="mbid",
        )
        slskd = FakeSlskdAPI(downloads=[{
            "username": "user1",
            "directories": [{"directory": "u\\M", "files": [
                {"filename": "u\\M\\01.flac", "id": "new-id-1"},
                {"filename": "u\\M\\02.flac", "id": "new-id-2"},
            ]}],
        }])
        rederive_transfer_ids(entry, slskd)
        self.assertEqual(entry.files[0].id, "new-id-1")
        self.assertEqual(entry.files[1].id, "new-id-2")
        self.assertEqual(slskd.transfers.get_all_downloads_calls, [True])

    def test_missing_transfer_keeps_empty_id(self):
        from lib.slskd_transfers import rederive_transfer_ids
        from lib.grab_list import GrabListEntry, DownloadFile
        entry = GrabListEntry(
            album_id=1, files=[
                DownloadFile(filename="u\\M\\01.flac", id="", file_dir="u\\M",
                             username="user1", size=1000),
            ],
            filetype="flac", title="T", artist="A", year="2020",
            mb_release_id="mbid",
        )
        slskd = FakeSlskdAPI(downloads=[{
            "username": "user1",
            "directories": [{"directory": "u\\M", "files": []}],
        }])
        rederive_transfer_ids(entry, slskd)
        self.assertEqual(entry.files[0].id, "")

    def test_uses_bulk_downloads_for_spacey_usernames(self):
        from lib.slskd_transfers import rederive_transfer_ids
        from lib.grab_list import GrabListEntry, DownloadFile
        entry = GrabListEntry(
            album_id=1,
            files=[
                DownloadFile(
                    filename="Miick Starr\\Album\\01.flac",
                    id="",
                    file_dir="Miick Starr\\Album",
                    username="Miick Starr",
                    size=1000,
                ),
                DownloadFile(
                    filename="Mr. Odd\\Album\\01.flac",
                    id="",
                    file_dir="Mr. Odd\\Album",
                    username="Mr. Odd",
                    size=1000,
                ),
            ],
            filetype="flac",
            title="T",
            artist="A",
            year="2020",
            mb_release_id="mbid",
        )
        slskd = FakeSlskdAPI(downloads=[
            {
                "username": "Mr. Odd",
                "directories": [{"directory": "Mr. Odd\\Album", "files": [
                    {"filename": "Mr. Odd\\Album\\01.flac", "id": "odd-id"},
                ]}],
            },
            {
                "username": "Miick Starr",
                "directories": [{"directory": "Miick Starr\\Album", "files": [
                    {"filename": "Miick Starr\\Album\\01.flac", "id": "starr-id"},
                ]}],
            },
        ])

        rederive_transfer_ids(entry, slskd)

        self.assertEqual(entry.files[0].id, "starr-id")
        self.assertEqual(entry.files[1].id, "odd-id")

    def test_terminal_snapshot_sets_file_status(self):
        from lib.slskd_transfers import rederive_transfer_ids
        from lib.grab_list import GrabListEntry, DownloadFile
        entry = GrabListEntry(
            album_id=1,
            files=[
                DownloadFile(
                    filename="user1\\Album\\01.flac",
                    id="",
                    file_dir="user1\\Album",
                    username="user1",
                    size=1000,
                ),
            ],
            filetype="flac",
            title="T",
            artist="A",
            year="2020",
            mb_release_id="mbid",
        )
        slskd = FakeSlskdAPI(downloads=[{
            "username": "user1",
            "directories": [{"directory": "user1\\Album", "files": [
                {
                    "filename": "user1\\Album\\01.flac",
                    "id": "done-id",
                    "state": "Completed, Succeeded",
                    "bytesTransferred": 1000,
                },
            ]}],
        }])

        rederive_transfer_ids(entry, slskd)

        self.assertEqual(entry.files[0].id, "done-id")
        status = entry.files[0].status
        self.assertIsNotNone(status)
        assert status is not None
        self.assertEqual(status.state, "Completed, Succeeded")

    def test_not_before_ignores_stale_terminal_transfer(self):
        from lib.slskd_transfers import rederive_transfer_ids
        from lib.grab_list import GrabListEntry, DownloadFile
        entry = GrabListEntry(
            album_id=1,
            files=[
                DownloadFile(
                    filename="user1\\Album\\01.flac",
                    id="",
                    file_dir="user1\\Album",
                    username="user1",
                    size=1000,
                ),
            ],
            filetype="flac",
            title="T",
            artist="A",
            year="2020",
            mb_release_id="mbid",
        )
        slskd = FakeSlskdAPI(downloads=[{
            "username": "user1",
            "directories": [{"directory": "user1\\Album", "files": [
                {
                    "filename": "user1\\Album\\01.flac",
                    "id": "old-completed",
                    "state": "Completed, Succeeded",
                    "endedAt": "2026-05-05T01:00:00+00:00",
                },
            ]}],
        }])

        rederive_transfer_ids(
            entry,
            slskd,
            not_before="2026-05-05T02:00:00+00:00",
        )

        self.assertEqual(entry.files[0].id, "")
        self.assertIsNone(entry.files[0].status)


class TestProcessCompletedAlbumReturnOwnership(unittest.TestCase):
    """Test process_completed_album return ownership."""

    def test_returns_true_on_success(self):
        """Successful file move + processing returns Completed."""
        from lib.download_processing import Completed, process_completed_album
        import tempfile, os
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create source file
            src_dir = os.path.join(tmpdir, "source_dir")
            os.makedirs(src_dir)
            src_file = os.path.join(src_dir, "01 - Track.mp3")
            with open(src_file, "w") as f:
                f.write("fake audio")

            files = [make_download_file(filename="source_dir\\01 - Track.mp3",
                                        file_dir="source_dir")]
            files[0].local_path = src_file
            album = make_grab_list_entry(files=files, mb_release_id="")
            ctx = _make_ctx()
            cfg = cast(Any, ctx.cfg)
            cfg.slskd_download_dir = tmpdir
            cfg.beets_validation_enabled = False
            result = process_completed_album(album, [], ctx, import_job_id=1)
            self.assertIsInstance(result, Completed)

    def test_dispatch_outcome_summary_is_returned_to_queue_owner(
        self,
    ):
        """Auto-import summaries must survive for the importer queue result."""
        from lib.download_processing import CompletionDispatched, process_completed_album
        from lib.dispatch import DispatchOutcome
        import tempfile, os

        with tempfile.TemporaryDirectory() as tmpdir:
            src_dir = os.path.join(tmpdir, "source_dir")
            os.makedirs(src_dir)
            src_file = os.path.join(src_dir, "01 - Track.mp3")
            with open(src_file, "w") as f:
                f.write("fake audio")

            files = [make_download_file(filename="source_dir\\01 - Track.mp3",
                                        file_dir="source_dir")]
            files[0].local_path = src_file
            album = make_grab_list_entry(files=files, mb_release_id="test-mbid")
            ctx = _make_ctx()
            cfg = cast(Any, ctx.cfg)
            cfg.slskd_download_dir = tmpdir
            cfg.beets_validation_enabled = True
            stub_outcome = DispatchOutcome(
                success=True,
                message="Import successful",
            )
            validate_calls: list[dict] = []

            def _stub_validate(*args, **kwargs):
                validate_calls.append(kwargs)
                return stub_outcome

            result = process_completed_album(
                album, [], ctx, import_job_id=1, validate_fn=_stub_validate,
            )

            assert isinstance(result, CompletionDispatched)
            self.assertIs(result.outcome, stub_outcome)
            self.assertEqual(len(validate_calls), 1)

    @patch("lib.beets.beets_validate")
    def test_beets_rejection_summary_is_returned_to_queue_owner(
        self,
        mock_beets_validate,
    ):
        """Validation rejections must fail the queue job, not look completed."""
        from lib.download_processing import CompletionDispatched, process_completed_album
        from lib.quality import ValidationResult
        import tempfile

        with tempfile.TemporaryDirectory() as tmpdir:
            downloads_root = os.path.join(tmpdir, "downloads")
            source_dir = os.path.join(downloads_root, "Music")
            os.makedirs(source_dir)
            source_file = os.path.join(source_dir, "01 - Track.mp3")
            with open(source_file, "w") as f:
                f.write("fake audio")

            db = FakePipelineDB()
            db.seed_request(make_request_row(
                id=42,
                status="downloading",
                artist_name="Artist",
                album_title="Album",
                year=2024,
                mb_release_id="test-mbid",
            ))
            cfg = cast(Any, _make_ctx().cfg)
            cfg.slskd_download_dir = downloads_root
            cfg.beets_validation_enabled = True
            cfg.beets_tracking_file = os.path.join(tmpdir, "beets-tracking.jsonl")
            ctx = make_ctx_with_fake_db(db, cfg=cfg)
            mock_beets_validate.return_value = ValidationResult(
                valid=False,
                distance=0.1919,
                scenario="high_distance",
                detail="distance=0.1919",
            )
            stamped = make_download_file(
                filename="user1\\Music\\01 - Track.mp3",
                file_dir="user1\\Music",
            )
            stamped.local_path = source_file
            album = make_grab_list_entry(
                files=[stamped],
                artist="Artist",
                title="Album",
                year="2024",
                mb_release_id="test-mbid",
                db_request_id=42,
                db_source="request",
            )

            result = process_completed_album(album, [], ctx, import_job_id=1)

            assert isinstance(result, CompletionDispatched)
            outcome = result.outcome
            self.assertFalse(outcome.success)
            self.assertFalse(outcome.deferred)
            self.assertEqual(
                outcome.message,
                "Rejected: high_distance - distance=0.1919",
            )
            source = ctx.pipeline_db_source
            assert isinstance(source, FakePipelineDBSource)
            self.assertEqual(len(source.reject_and_requeue_calls), 1)

    def test_returns_false_on_file_move_failure(self):
        """A mid-album move failure returns CompletionFailed and rolls back
        the already-moved files to their stamped sources."""
        from lib.download_processing import CompletionFailed, process_completed_album
        import tempfile, os
        with tempfile.TemporaryDirectory() as tmpdir:
            src_dir = os.path.join(tmpdir, "Music")
            os.makedirs(src_dir)
            srcs = []
            files = []
            for i in (1, 2):
                src = os.path.join(src_dir, f"0{i} - Track.mp3")
                with open(src, "w") as f:
                    f.write("fake audio")
                srcs.append(src)
                file = make_download_file(
                    filename=f"user1\\Music\\0{i} - Track.mp3",
                    file_dir="user1\\Music",
                )
                file.local_path = src
                files.append(file)
            album = make_grab_list_entry(files=files, mb_release_id="")
            ctx = _make_ctx()
            cfg = cast(Any, ctx.cfg)
            cfg.slskd_download_dir = tmpdir
            cfg.beets_validation_enabled = False

            real_move = shutil.move

            def _failing_move(src, dst, *args, **kwargs):
                if src == srcs[1]:
                    raise OSError("disk full")
                return real_move(src, dst, *args, **kwargs)

            with patch("lib.download_processing.shutil.move", side_effect=_failing_move):
                result = process_completed_album(
                    album, [], ctx, import_job_id=1)

            self.assertIsInstance(result, CompletionFailed)
            # Rollback restored the first file to its stamped source.
            self.assertTrue(os.path.exists(srcs[0]))
            self.assertTrue(os.path.exists(srcs[1]))

    def test_resumes_from_persisted_current_path(self):
        """A post-move retry must process the persisted current_path, not slskd."""
        from lib.download_processing import Completed, process_completed_album
        import tempfile
        with tempfile.TemporaryDirectory() as tmpdir:
            resumed_path = os.path.join(tmpdir, "staging", "Artist", "Album")
            os.makedirs(resumed_path)
            resumed_file = os.path.join(resumed_path, "01 - Track.mp3")
            with open(resumed_file, "w") as f:
                f.write("fake audio")

            files = [make_download_file(
                filename="user1\\Music\\01 - Track.mp3",
                file_dir="user1\\Music",
                size=len("fake audio"),
            )]
            album = make_grab_list_entry(
                files=files,
                artist="Artist",
                title="Album",
                year="2024",
                mb_release_id="",
            )
            album.import_folder = resumed_path
            ctx = _make_ctx()
            cfg = cast(Any, ctx.cfg)
            cfg.slskd_download_dir = os.path.join(tmpdir, "downloads")
            cfg.beets_validation_enabled = False
            os.makedirs(cfg.slskd_download_dir, exist_ok=True)

            result = process_completed_album(album, [], ctx, import_job_id=1)

            self.assertIsInstance(result, Completed)
            self.assertEqual(files[0].import_path, resumed_file)
            self.assertTrue(os.path.exists(resumed_file))

    def test_resumes_multi_disc_from_persisted_current_path(self):
        """Resume must preserve the staged multi-disc filenames on disk."""
        from lib.download_processing import Completed, process_completed_album
        import tempfile
        with tempfile.TemporaryDirectory() as tmpdir:
            resumed_path = os.path.join(tmpdir, "staging", "Artist", "Album")
            os.makedirs(resumed_path)
            resumed_file = os.path.join(resumed_path, "Disk 2 - 01 - Track.flac")
            with open(resumed_file, "w") as f:
                f.write("fake audio")

            file = make_download_file(
                filename="user1\\CD2\\01 - Track.flac",
                file_dir="user1\\CD2",
                size=len("fake audio"),
            )
            file.disk_no = 2
            file.disk_count = 2
            album = make_grab_list_entry(
                files=[file],
                artist="Artist",
                title="Album",
                year="2024",
                mb_release_id="",
            )
            album.import_folder = resumed_path
            ctx = _make_ctx()
            cfg = cast(Any, ctx.cfg)
            cfg.slskd_download_dir = os.path.join(tmpdir, "downloads")
            cfg.beets_validation_enabled = False
            os.makedirs(cfg.slskd_download_dir, exist_ok=True)

            result = process_completed_album(album, [], ctx, import_job_id=1)

            self.assertIsInstance(result, Completed)
            self.assertEqual(file.import_path, resumed_file)
            self.assertTrue(os.path.exists(resumed_file))

    def test_persists_canonical_current_path_for_fresh_materialization(self):
        """The first local materialization must persist the canonical path to DB."""
        from lib.download_processing import Completed, process_completed_album
        import tempfile
        with tempfile.TemporaryDirectory() as tmpdir:
            fake_db = FakePipelineDB()
            fake_db.seed_request(make_request_row(
                id=42,
                status="downloading",
                active_download_state={"filetype": "mp3", "files": []},
            ))
            source_dir = os.path.join(tmpdir, "downloads", "Music")
            os.makedirs(source_dir)
            source_file = os.path.join(source_dir, "01 - Track.mp3")
            with open(source_file, "w") as f:
                f.write("fake audio")

            stamped = make_download_file(
                filename="user1\\Music\\01 - Track.mp3",
                file_dir="user1\\Music",
            )
            stamped.local_path = source_file
            album = make_grab_list_entry(
                files=[stamped],
                artist="Artist",
                title="Album",
                year="2024",
                mb_release_id="",
                db_request_id=42,
            )
            cfg = cast(Any, _make_ctx().cfg)
            cfg.slskd_download_dir = os.path.join(tmpdir, "downloads")
            cfg.beets_validation_enabled = False
            ctx = make_ctx_with_fake_db(fake_db, cfg=cfg)

            result = process_completed_album(album, [], ctx, import_job_id=1)

            self.assertIsInstance(result, Completed)
            self.assertEqual(
                fake_db.request(42)["active_download_state"]["current_path"],
                os.path.join(tmpdir, "downloads", "Artist - Album (2024)"),
            )

    @patch("lib.beets.beets_validate")
    def test_returns_none_for_post_move_auto_import_retry(
        self,
        mock_beets_validate,
    ):
        """Post-move auto-import retries must stop before re-dispatch."""
        from lib.download_processing import CompletionDeferred, process_completed_album
        from lib.quality import ValidationResult
        from lib.processing_paths import stage_to_ai_path
        import tempfile
        with tempfile.TemporaryDirectory() as tmpdir:
            staging_root = os.path.join(tmpdir, "staging")
            resumed_path = stage_to_ai_path(
                artist="Artist",
                title="Album",
                staging_dir=staging_root,
                request_id=42,
                auto_import=True,
            )
            os.makedirs(resumed_path)
            with open(os.path.join(resumed_path, "01 - Track.mp3"), "w") as f:
                f.write("fake audio")

            album = make_grab_list_entry(
                files=[make_download_file(
                    filename="user1\\Music\\01 - Track.mp3",
                    file_dir="user1\\Music",
                )],
                artist="Artist",
                title="Album",
                year="2024",
                mb_release_id="test-mbid",
                db_request_id=42,
                db_source="request",
            )
            album.import_folder = resumed_path
            ctx = _make_ctx()
            cfg = cast(Any, ctx.cfg)
            cfg.slskd_download_dir = os.path.join(tmpdir, "downloads")
            cfg.beets_staging_dir = staging_root
            cfg.beets_validation_enabled = True
            cfg.beets_tracking_file = os.path.join(tmpdir, "beets-tracking.jsonl")
            os.makedirs(cfg.slskd_download_dir, exist_ok=True)
            mock_beets_validate.return_value = ValidationResult(
                valid=True,
                distance=0.05,
                scenario="strong_match",
            )

            dispatch_calls: list[dict] = []
            with self.assertLogs("cratedigger", level="ERROR") as logs:
                result = process_completed_album(
                    album, [], ctx, import_job_id=1,
                    dispatch_fn=lambda **kw: dispatch_calls.append(kw) or None,
                )

            self.assertIsInstance(result, CompletionDeferred)
            self.assertEqual(dispatch_calls, [])
            self.assertIn("POST-MOVE RESUME BLOCKED", "\n".join(logs.output))

    def test_request_scoped_staged_path_without_request_id_blocks_manual_recovery(
        self,
    ):
        """Request-scoped auto-import staging without request id must stay blocked."""
        from lib.download_processing import CompletionDeferred, process_completed_album
        from lib.processing_paths import stage_to_ai_path
        import tempfile
        with tempfile.TemporaryDirectory() as tmpdir:
            staging_root = os.path.join(tmpdir, "staging")
            resumed_path = stage_to_ai_path(
                artist="Artist",
                title="Album",
                staging_dir=staging_root,
                request_id=42,
                auto_import=True,
            )
            os.makedirs(resumed_path)
            with open(os.path.join(resumed_path, "01 - Track.mp3"), "w") as f:
                f.write("fake audio")

            album = make_grab_list_entry(
                files=[make_download_file(
                    filename="user1\\Music\\01 - Track.mp3",
                    file_dir="user1\\Music",
                )],
                artist="Artist",
                title="Album",
                year="2024",
                mb_release_id="",
                db_request_id=None,
                db_source="request",
            )
            album.import_folder = resumed_path
            ctx = _make_ctx()
            cfg = cast(Any, ctx.cfg)
            cfg.slskd_download_dir = os.path.join(tmpdir, "downloads")
            cfg.beets_staging_dir = staging_root
            cfg.beets_validation_enabled = False
            os.makedirs(cfg.slskd_download_dir, exist_ok=True)

            with self.assertLogs("cratedigger", level="ERROR") as logs:
                result = process_completed_album(album, [], ctx, import_job_id=1)

            self.assertIsInstance(result, CompletionDeferred)
            self.assertIn("missing db_request_id", "\n".join(logs.output))

    def test_post_validation_staged_path_without_request_id_still_resumes(
        self,
    ):
        """Post-validation staging remains resumable without the auto-import guard."""
        from lib.download_processing import Completed, process_completed_album
        from lib.processing_paths import stage_to_ai_path
        import tempfile
        with tempfile.TemporaryDirectory() as tmpdir:
            staging_root = os.path.join(tmpdir, "staging")
            resumed_path = stage_to_ai_path(
                artist="Artist",
                title="Album",
                staging_dir=staging_root,
                request_id=42,
                auto_import=False,
            )
            os.makedirs(resumed_path)
            resumed_file = os.path.join(resumed_path, "01 - Track.mp3")
            with open(resumed_file, "w") as f:
                f.write("fake audio")

            album = make_grab_list_entry(
                files=[make_download_file(
                    filename="user1\\Music\\01 - Track.mp3",
                    file_dir="user1\\Music",
                )],
                artist="Artist",
                title="Album",
                year="2024",
                mb_release_id="",
                db_request_id=None,
                db_source="redownload",
            )
            album.import_folder = resumed_path
            ctx = _make_ctx()
            cfg = cast(Any, ctx.cfg)
            cfg.slskd_download_dir = os.path.join(tmpdir, "downloads")
            cfg.beets_staging_dir = staging_root
            cfg.beets_validation_enabled = False
            os.makedirs(cfg.slskd_download_dir, exist_ok=True)

            result = process_completed_album(album, [], ctx, import_job_id=1)

            self.assertIsInstance(result, Completed)
            self.assertEqual(album.files[0].import_path, resumed_file)

    @patch("lib.beets.beets_validate")
    def test_returns_none_for_legacy_shared_staged_retry(
        self,
        mock_beets_validate,
    ):
        """Legacy shared staged retries must stop before validation reruns."""
        from lib.download_processing import CompletionDeferred, process_completed_album
        import tempfile
        with tempfile.TemporaryDirectory() as tmpdir:
            staging_root = os.path.join(tmpdir, "staging")
            resumed_path = os.path.join(staging_root, "Artist", "Album")
            os.makedirs(resumed_path)
            with open(os.path.join(resumed_path, "01 - Track.mp3"), "w") as f:
                f.write("fake audio")

            album = make_grab_list_entry(
                files=[make_download_file(
                    filename="user1\\Music\\01 - Track.mp3",
                    file_dir="user1\\Music",
                )],
                artist="Artist",
                title="Album",
                year="2024",
                mb_release_id="test-mbid",
                db_request_id=42,
                db_source="request",
            )
            album.import_folder = resumed_path
            ctx = _make_ctx()
            cfg = cast(Any, ctx.cfg)
            cfg.slskd_download_dir = os.path.join(tmpdir, "downloads")
            cfg.beets_staging_dir = staging_root
            cfg.beets_validation_enabled = True
            cfg.beets_tracking_file = os.path.join(tmpdir, "beets-tracking.jsonl")
            os.makedirs(cfg.slskd_download_dir, exist_ok=True)

            dispatch_calls: list[dict] = []
            with self.assertLogs("cratedigger", level="ERROR") as logs:
                result = process_completed_album(
                    album, [], ctx, import_job_id=1,
                    dispatch_fn=lambda **kw: dispatch_calls.append(kw) or None,
                )

            self.assertIsInstance(result, CompletionDeferred)
            mock_beets_validate.assert_not_called()
            self.assertEqual(dispatch_calls, [])
            self.assertIn("legacy shared staged path", "\n".join(logs.output))

    def test_returns_false_when_persisted_current_path_missing_dir(self):
        """Resume must fail closed when the persisted directory no longer exists."""
        from lib.download_processing import CompletionFailed, process_completed_album
        import tempfile
        with tempfile.TemporaryDirectory() as tmpdir:
            album = make_grab_list_entry(
                files=[make_download_file(
                    filename="user1\\Music\\01 - Track.mp3",
                    file_dir="user1\\Music",
                )],
                artist="Artist",
                title="Album",
                year="2024",
                mb_release_id="",
            )
            album.import_folder = os.path.join(tmpdir, "missing")
            ctx = _make_ctx()
            cfg = cast(Any, ctx.cfg)
            cfg.slskd_download_dir = os.path.join(tmpdir, "downloads")
            cfg.beets_validation_enabled = False

            result = process_completed_album(album, [], ctx, import_job_id=1)

            self.assertIsInstance(result, CompletionFailed)

    def test_returns_false_when_persisted_current_path_missing_file(self):
        """Resume dir must contain every tracked file before processing continues."""
        from lib.download_processing import CompletionFailed, process_completed_album
        import tempfile
        with tempfile.TemporaryDirectory() as tmpdir:
            resumed_path = os.path.join(tmpdir, "staging", "Artist", "Album")
            os.makedirs(resumed_path)
            album = make_grab_list_entry(
                files=[make_download_file(
                    filename="user1\\Music\\01 - Track.mp3",
                    file_dir="user1\\Music",
                )],
                artist="Artist",
                title="Album",
                year="2024",
                mb_release_id="",
            )
            album.import_folder = resumed_path
            ctx = _make_ctx()
            cfg = cast(Any, ctx.cfg)
            cfg.slskd_download_dir = os.path.join(tmpdir, "downloads")
            cfg.beets_validation_enabled = False
            os.makedirs(cfg.slskd_download_dir, exist_ok=True)

            result = process_completed_album(album, [], ctx, import_job_id=1)

            self.assertIsInstance(result, CompletionFailed)


class TestHandleValidResultMissingMbid(unittest.TestCase):
    """_handle_valid_result guards for request rows without an MBID."""

    def test_request_source_without_mbid_requeues_instead_of_marking_done(self):
        """Request rows without an MBID must requeue, not mark imported."""
        from lib.download_processing import _handle_valid_result
        from lib.staged_album import StagedAlbum
        import tempfile

        album = make_grab_list_entry(
            files=[make_download_file()],
            mb_release_id="",
            db_source="request",
            db_request_id=42,
        )
        bv_result = MagicMock()
        bv_result.distance = 0.05
        bv_result.scenario = "strong_match"
        bv_result.to_json.return_value = '{"valid": true}'

        db = FakePipelineDB()
        db.seed_request(make_request_row(id=42, status="downloading"))
        ctx = make_ctx_with_fake_db(db)
        ctx.cfg.beets_distance_threshold = 0.15

        with tempfile.TemporaryDirectory() as tmpdir, \
             patch("lib.download_processing.log_validation_result"):
            import_dir = os.path.join(tmpdir, "Test Artist - Test Album")
            os.makedirs(import_dir)
            with open(os.path.join(import_dir, "01 - Track.mp3"), "w",
                      encoding="utf-8") as fp:
                fp.write("x")

            outcome = _handle_valid_result(
                album,
                bv_result,
                StagedAlbum(current_path=import_dir, request_id=42),
                ctx,
            )

            assert outcome is not None
            self.assertFalse(outcome.success)
            self.assertFalse(os.path.exists(import_dir))
            failed_dir = os.path.join(tmpdir, "failed_imports")
            self.assertTrue(os.path.isdir(failed_dir))
            self.assertEqual(len(os.listdir(failed_dir)), 1)

        self.assertEqual(db.request(42)["status"], "wanted")
        self.assertEqual(db.request(42)["validation_attempts"], 1)
        self.assertEqual(len(db.download_logs), 1)


class TestEventPathMaterialization(unittest.TestCase):
    """Issue #146 phase 3: the event-stream local_path is the ONLY source
    of file locations. An unstamped file (with no already-moved evidence)
    is a hard failure — the resolver fallback is gone.
    """

    FNAME = "04 How To Disappear Completely.mp3"

    def _album(self, tmpdir, *, local_path):
        files = [make_download_file(
            filename=f"@@wcren\\Music\\Radiohead\\Kid A\\{self.FNAME}",
            file_dir="@@wcren\\Music\\Radiohead\\Kid A",
            size=len("fake audio"),
        )]
        files[0].local_path = local_path
        album = make_grab_list_entry(
            files=files, mb_release_id="", artist="Radiohead",
            title="Kid A", year="2000")
        ctx = _make_ctx()
        cfg = cast(Any, ctx.cfg)
        cfg.slskd_download_dir = tmpdir
        cfg.beets_validation_enabled = False
        return album, ctx

    def _moved(self, tmpdir):
        return os.listdir(os.path.join(tmpdir, "Radiohead - Kid A (2000)"))

    def test_stamped_file_moves_with_clean_basename(self):
        # slskd placed the file at an arbitrary event-reported location
        # with a collision suffix; the move follows the stamp and the
        # destination keeps the clean remote basename.
        from lib.download_processing import Completed, process_completed_album
        import tempfile
        with tempfile.TemporaryDirectory() as tmpdir:
            event_path = os.path.join(
                tmpdir, "somewhere-unrelated",
                "04 How To Disappear Completely_638827305447447018.mp3")
            os.makedirs(os.path.dirname(event_path))
            with open(event_path, "w") as fp:
                fp.write("fake audio")
            album, ctx = self._album(tmpdir, local_path=event_path)

            result = process_completed_album(album, [], ctx, import_job_id=1)

            self.assertIsInstance(result, Completed)
            self.assertFalse(os.path.exists(event_path))
            self.assertEqual(self._moved(tmpdir), [self.FNAME])

    def test_stamped_forward_slash_remote_path_keeps_basename(self):
        # Destination basename extraction accepts slash-normalized remote
        # paths regardless of where the stamped source lives.
        from lib.download_processing import Completed, process_completed_album
        import tempfile
        with tempfile.TemporaryDirectory() as tmpdir:
            src = os.path.join(tmpdir, "Kid A", self.FNAME)
            os.makedirs(os.path.dirname(src))
            with open(src, "w") as fp:
                fp.write("fake audio")
            files = [make_download_file(
                filename=f"@@wcren/Music/Radiohead/Kid A/{self.FNAME}",
                file_dir="@@wcren/Music/Radiohead/Kid A",
                size=len("fake audio"),
            )]
            files[0].local_path = src
            album = make_grab_list_entry(
                files=files, mb_release_id="", artist="Radiohead",
                title="Kid A", year="2000")
            ctx = _make_ctx()
            cfg = cast(Any, ctx.cfg)
            cfg.slskd_download_dir = tmpdir
            cfg.beets_validation_enabled = False

            result = process_completed_album(album, [], ctx, import_job_id=1)

            self.assertIsInstance(result, Completed)
            self.assertEqual(self._moved(tmpdir), [self.FNAME])

    def test_unstamped_file_is_hard_failure(self):
        # No event was ever ingested for this file (pre-bootstrap
        # completion or cursor gap) — hard failure with diagnostics, no
        # guessing at on-disk locations.
        from lib.download_processing import CompletionFailed, process_completed_album
        import tempfile
        with tempfile.TemporaryDirectory() as tmpdir:
            # File IS on disk at the historical inferred location — the
            # point is that phase 3 no longer looks there.
            src = os.path.join(tmpdir, "Kid A", self.FNAME)
            os.makedirs(os.path.dirname(src))
            with open(src, "w") as fp:
                fp.write("fake audio")
            album, ctx = self._album(tmpdir, local_path=None)

            with self.assertLogs("cratedigger", level=logging.ERROR) as logs:
                result = process_completed_album(
                    album, [], ctx, import_job_id=1)

            self.assertIsInstance(result, CompletionFailed)
            self.assertTrue(os.path.exists(src))
            joined = "\n".join(logs.output)
            self.assertIn("EVENT-PATH MISSING", joined)
            self.assertIn("not_stamped", joined)

    def test_stale_stamp_without_dst_is_hard_failure(self):
        # Stamped path vanished and the destination has no already-moved
        # copy — hard failure, diagnostics name the stale stamp.
        from lib.download_processing import CompletionFailed, process_completed_album
        import tempfile
        with tempfile.TemporaryDirectory() as tmpdir:
            album, ctx = self._album(
                tmpdir, local_path=os.path.join(tmpdir, "gone", "x.mp3"))

            with self.assertLogs("cratedigger", level=logging.ERROR) as logs:
                result = process_completed_album(
                    album, [], ctx, import_job_id=1)

            self.assertIsInstance(result, CompletionFailed)
            joined = "\n".join(logs.output)
            self.assertIn("EVENT-PATH MISSING", joined)
            self.assertIn("stale_stamp", joined)

    def test_mixed_album_fails_preflight_without_moving_stamped_file(self):
        """One stamped file, one unstamped: the pre-flight check fails the
        album BEFORE any move, so the stamped file stays at its event
        location — no move-then-rollback churn."""
        from lib.download_processing import CompletionFailed, process_completed_album
        import tempfile
        with tempfile.TemporaryDirectory() as tmpdir:
            event_src = os.path.join(
                tmpdir, "somewhere-unrelated", "01 Track One.mp3")
            os.makedirs(os.path.dirname(event_src))
            with open(event_src, "w") as fp:
                fp.write("fake audio")
            files = [
                make_download_file(
                    filename="@@w\\Music\\R\\Kid A\\01 Track One.mp3",
                    file_dir="@@w\\Music\\R\\Kid A",
                    size=len("fake audio"),
                ),
                make_download_file(
                    filename="@@w\\Music\\R\\Kid A\\02 Track Two.mp3",
                    file_dir="@@w\\Music\\R\\Kid A",
                    size=10,
                ),
            ]
            files[0].local_path = event_src
            album = make_grab_list_entry(
                files=files, mb_release_id="", artist="Radiohead",
                title="Kid A", year="2000")
            ctx = _make_ctx()
            cfg = cast(Any, ctx.cfg)
            cfg.slskd_download_dir = tmpdir
            cfg.beets_validation_enabled = False

            with self.assertLogs("cratedigger", level=logging.ERROR) as logs:
                result = process_completed_album(
                    album, [], ctx, import_job_id=1)

            self.assertIsInstance(result, CompletionFailed)
            self.assertTrue(os.path.exists(event_src))
            self.assertIn("EVENT-PATH MISSING", "\n".join(logs.output))
            dst_dir = os.path.join(tmpdir, "Radiohead - Kid A (2000)")
            self.assertTrue(
                not os.path.isdir(dst_dir) or os.listdir(dst_dir) == [])

    def test_already_moved_resume_still_skips(self):
        # Crash-resume: dst exists, the stamped source is gone. The file
        # counts as already moved; processing succeeds.
        from lib.download_processing import Completed, process_completed_album
        import tempfile
        with tempfile.TemporaryDirectory() as tmpdir:
            dst_dir = os.path.join(tmpdir, "Radiohead - Kid A (2000)")
            os.makedirs(dst_dir)
            with open(os.path.join(dst_dir, self.FNAME), "w") as fp:
                fp.write("fake audio")
            album, ctx = self._album(
                tmpdir, local_path=os.path.join(tmpdir, "Kid A", self.FNAME))

            result = process_completed_album(album, [], ctx, import_job_id=1)

            self.assertIsInstance(result, Completed)
            self.assertEqual(self._moved(tmpdir), [self.FNAME])

    def test_unstamped_already_moved_resume_still_skips(self):
        # Even an unstamped file counts as already moved when the
        # destination copy exists — pre-flight must not hard-fail it.
        from lib.download_processing import Completed, process_completed_album
        import tempfile
        with tempfile.TemporaryDirectory() as tmpdir:
            dst_dir = os.path.join(tmpdir, "Radiohead - Kid A (2000)")
            os.makedirs(dst_dir)
            with open(os.path.join(dst_dir, self.FNAME), "w") as fp:
                fp.write("fake audio")
            album, ctx = self._album(tmpdir, local_path=None)

            result = process_completed_album(album, [], ctx, import_job_id=1)

            self.assertIsInstance(result, Completed)
            self.assertEqual(self._moved(tmpdir), [self.FNAME])


class TestMaterializeFailureAction(unittest.TestCase):
    """Pure decision table for the poller's materialize-failure escape.

    Cases pin the ownership-protocol tags (#474): ``MaterializeGuarded``
    (historical bare ``None``) always "leave"s regardless of age;
    ``MaterializeFailed`` (historical bare ``False``) "retry"s within
    the grace window and "reset"s past it; ``Materialized`` (historical
    bare ``True``) also "leave"s — callers only invoke this function
    after already excluding the success case, but the no-op answer must
    still hold so a future caller that skips the exclusion check fails
    safe rather than auto-resetting a successful materialization.
    """

    NOW = datetime(2026, 7, 2, 12, 0, 0, tzinfo=timezone.utc)
    OLD = (NOW - timedelta(hours=2)).isoformat()
    FRESH = (NOW - timedelta(minutes=5)).isoformat()

    CASES = [
        ("guarded_fresh_leaves",
         MaterializeGuarded(detail="release_lock_held"), FRESH, "leave"),
        ("guarded_old_leaves_manual_recovery_alone",
         MaterializeGuarded(detail="release_lock_held"), OLD, "leave"),
        ("materialized_leaves_as_no_op_safety_net",
         Materialized(), OLD, "leave"),
        ("failed_fresh_retries",
         MaterializeFailed(reason="staged_path_missing"), FRESH, "retry"),
        ("failed_old_resets",
         MaterializeFailed(reason="staged_path_missing"), OLD, "reset"),
        ("failed_no_start_retries",
         MaterializeFailed(reason="staged_path_missing"), None, "retry"),
        ("failed_unparseable_start_retries",
         MaterializeFailed(reason="staged_path_missing"), "not-a-date", "retry"),
        ("naive_timestamp_treated_utc",
         MaterializeFailed(reason="staged_path_missing"),
         (NOW - timedelta(hours=2)).replace(tzinfo=None).isoformat(), "reset"),
    ]

    def test_decision_table(self):
        from lib.download import materialize_failure_action
        for desc, materialized, started, expected in self.CASES:
            with self.subTest(desc=desc):
                self.assertEqual(
                    materialize_failure_action(
                        materialized, started, self.NOW),
                    expected)


class TestPollActiveDownloads(unittest.TestCase):
    """Test poll_active_downloads() — core polling function."""

    def _make_downloading_row(self, request_id=1, state_dict=None):
        """Build a mock album_requests row with status='downloading'."""
        if state_dict is None:
            state_dict = {
                "filetype": "flac",
                "enqueued_at": _utc_now_iso(),
                "files": [
                    {"username": "user1", "filename": "user1\\Music\\01.flac",
                     "file_dir": "user1\\Music", "size": 30000000},
                ],
            }
        return {
            "id": request_id,
            "album_title": "Test Album",
            "artist_name": "Test Artist",
            "year": 2020,
            # Per-row mbid — the fake enforces UNIQUE(mb_release_id).
            "mb_release_id": f"test-mbid-{request_id}",
            "source": "request",
            "search_filetype_override": None,
            "target_format": None,
            "status": "downloading",
            "active_download_state": state_dict,
        }

    def _make_poll_ctx(self, downloading_rows=None, slskd_downloads=None):
        """Build context with fake DB + fake slskd for polling."""
        if slskd_downloads is None:
            # Default: return transfers that match the files
            slskd_downloads = [{
                "username": "user1",
                "directories": [{"directory": "user1\\Music", "files": [
                    {
                        "filename": "user1\\Music\\01.flac",
                        "id": "tid-1",
                        "state": "InProgress",
                        "bytesTransferred": 1,
                    },
                ]}],
            }]
        cfg = cast(Any, _make_ctx().cfg)
        tmpdir = tempfile.mkdtemp(prefix="cratedigger-poll-test-")
        self.addCleanup(shutil.rmtree, tmpdir, ignore_errors=True)
        cfg.slskd_download_dir = os.path.join(tmpdir, "downloads")
        cfg.beets_staging_dir = os.path.join(tmpdir, "staging")
        os.makedirs(cfg.slskd_download_dir, exist_ok=True)
        os.makedirs(cfg.beets_staging_dir, exist_ok=True)
        # Stamp BEFORE seeding — seed_request deep-copies the row.
        for row in downloading_rows or []:
            raw_state = row.get("active_download_state")
            if not isinstance(raw_state, dict):
                continue
            for file_state in raw_state.get("files") or []:
                if not isinstance(file_state, dict):
                    continue
                file_dir = str(file_state.get("file_dir") or "")
                filename = str(file_state.get("filename") or "")
                folder_leaf = file_dir.replace("/", "\\").split("\\")[-1]
                basename = filename.replace("/", "\\").split("\\")[-1]
                if not folder_leaf or not basename:
                    continue
                local_dir = os.path.join(cfg.slskd_download_dir, folder_leaf)
                os.makedirs(local_dir, exist_ok=True)
                local_path = os.path.join(local_dir, basename)
                with open(local_path, "wb") as fp:
                    fp.write(b"test audio")
                # Production shape post-#146: every completed file carries
                # its event-stamped local_path. Tests exercising unstamped
                # behaviour opt out with an explicit "local_path": None.
                if "local_path" not in file_state:
                    file_state["local_path"] = local_path
        fake_db = FakePipelineDB()
        for row in downloading_rows or []:
            fake_db.seed_request(row)
        ctx = make_ctx_with_fake_db(
            fake_db,
            cfg=cfg,
            slskd=FakeSlskdAPI(downloads=slskd_downloads),
        )
        return ctx, fake_db

    def _download_state(self, fake_db: FakePipelineDB, request_id: int = 1):
        state = fake_db.request(request_id)["active_download_state"]
        assert isinstance(state, dict)
        return state

    def test_poll_ingests_events_and_persists_local_path(self):
        """Issue #146 phase 1 wiring: a DownloadFileComplete event seen at
        poll time lands as ``local_path`` in the persisted state."""
        import json as _json
        from lib.download import poll_active_downloads
        row = self._make_downloading_row()
        ctx, fake_db = self._make_poll_ctx(downloading_rows=[row])
        fake_db.upsert_slskd_event_cursor(
            "ev-cursor", "2026-07-01T00:00:00.0000000Z")
        slskd = cast(Any, ctx.slskd)
        slskd.events.set_events([
            slskd.events.make_event(
                id="ev-1", timestamp="2026-07-01T10:00:00.0000000Z",
                type="DownloadFileComplete",
                data=_json.dumps({
                    "version": 0,
                    "localFilename": "/dl/Music/01.flac",
                    "remoteFilename": "user1\\Music\\01.flac",
                    "transfer": {
                        "id": "tid-1", "username": "user1",
                        "filename": "user1\\Music\\01.flac",
                        "size": 30000000,
                    },
                })),
            slskd.events.make_event(
                id="ev-cursor", timestamp="2026-07-01T00:00:00.0000000Z",
                type="Noise", data="{}"),
        ])

        poll_active_downloads(ctx)

        state = self._download_state(fake_db)
        self.assertEqual(state["files"][0]["local_path"], "/dl/Music/01.flac")
        cursor = fake_db.get_slskd_event_cursor()
        assert cursor is not None
        self.assertEqual(cursor["last_event_id"], "ev-1")

    def test_poll_fetches_snapshot_before_ingesting_events(self):
        """Ordering pin (#146 phase 2): the transfer snapshot is taken
        BEFORE event ingestion, so any transfer the snapshot shows
        Completed already has its DownloadFileComplete event in the feed
        — closing the same-cycle race that made healthy completions
        process unstamped."""
        from lib.download import poll_active_downloads
        row = self._make_downloading_row()
        ctx, _ = self._make_poll_ctx(downloading_rows=[row])

        poll_active_downloads(ctx)

        call_log = cast(Any, ctx.slskd).call_log
        self.assertIn("transfers.get_all_downloads", call_log)
        self.assertIn("events.list", call_log)
        self.assertLess(
            call_log.index("transfers.get_all_downloads"),
            call_log.index("events.list"))

    def test_poll_survives_events_api_failure(self):
        """Events API outage stamps nothing this cycle — polling continues."""
        from lib.download import poll_active_downloads
        row = self._make_downloading_row(state_dict={
            "filetype": "flac",
            "enqueued_at": _utc_now_iso(),
            "files": [
                {"username": "user1", "filename": "user1\\Music\\01.flac",
                 "file_dir": "user1\\Music", "size": 30000000,
                 "local_path": None},
            ],
        })
        ctx, fake_db = self._make_poll_ctx(downloading_rows=[row])
        cast(Any, ctx.slskd).events.list_error = RuntimeError("events down")

        poll_active_downloads(ctx)  # must not raise

        # Row still got polled (transfer id re-derived, state persisted).
        self.assertIsNone(
            self._download_state(fake_db)["files"][0].get("local_path"))

    def test_poll_active_no_downloading(self):
        """No downloading albums → no-op."""
        from lib.download import poll_active_downloads
        ctx, fake_db = self._make_poll_ctx(downloading_rows=[])
        poll_active_downloads(ctx)
        self.assertEqual(fake_db._import_jobs, [])
        self.assertEqual(fake_db.download_logs, [])

    def test_poll_active_all_complete(self):
        """1 downloading album, all files complete → enqueues importer job."""
        from lib.download import poll_active_downloads
        from lib.import_queue import IMPORT_JOB_AUTOMATION
        row = self._make_downloading_row()
        ctx, fake_db = self._make_poll_ctx(
            downloading_rows=[row],
            slskd_downloads=[{
                "username": "user1",
                "directories": [{"directory": "user1\\Music", "files": [{
                    "filename": "user1\\Music\\01.flac",
                    "id": "tid-1",
                    "state": "Completed, Succeeded",
                    "bytesTransferred": 30000000,
                }]}],
            }],
        )

        poll_active_downloads(ctx)

        self.assertGreaterEqual(len(fake_db.update_download_state_calls), 1)
        self.assertEqual(fake_db.request(1)["status"], "downloading")
        self.assertIsNotNone(self._download_state(fake_db)["current_path"])
        jobs = fake_db.list_import_jobs()
        self.assertEqual(len(jobs), 1)
        self.assertEqual(jobs[0].job_type, IMPORT_JOB_AUTOMATION)
        self.assertEqual(jobs[0].request_id, 1)

    def test_poll_completed_unstamped_within_grace_retries_next_cycle(self):
        """A completed album whose files never got stamped stays
        'downloading' while the materialize grace window is open — the
        DownloadFileComplete event may still be ingested next cycle
        (completion-vs-event-write race)."""
        from lib.download import poll_active_downloads
        row = self._make_downloading_row(state_dict={
            "filetype": "flac",
            "enqueued_at": _utc_now_iso(),
            "files": [
                {"username": "user1", "filename": "user1\\Music\\01.flac",
                 "file_dir": "user1\\Music", "size": 30000000,
                 "local_path": None},
            ],
        })
        ctx, fake_db = self._make_poll_ctx(
            downloading_rows=[row],
            slskd_downloads=[{
                "username": "user1",
                "directories": [{"directory": "user1\\Music", "files": [{
                    "filename": "user1\\Music\\01.flac",
                    "id": "tid-1",
                    "state": "Completed, Succeeded",
                    "bytesTransferred": 30000000,
                }]}],
            }],
        )

        poll_active_downloads(ctx)

        self.assertEqual(fake_db.request(1)["status"], "downloading")
        self.assertEqual(fake_db._import_jobs, [])
        self.assertEqual(fake_db.download_logs, [])

    def test_poll_completed_unstamped_past_grace_resets_to_wanted(self):
        """Once the grace window expires with the file still unstamped
        (event permanently lost: pre-bootstrap completion or cursor gap),
        the request self-heals via re-download instead of retrying the
        materialize forever."""
        from lib.download import poll_active_downloads
        old = (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat()
        row = self._make_downloading_row(state_dict={
            "filetype": "flac",
            "enqueued_at": old,
            "last_progress_at": _utc_now_iso(),
            "processing_started_at": old,
            "files": [
                {"username": "user1", "filename": "user1\\Music\\01.flac",
                 "file_dir": "user1\\Music", "size": 30000000,
                 "local_path": None,
                 "last_state": "Completed, Succeeded",
                 "bytes_transferred": 30000000},
            ],
        })
        ctx, fake_db = self._make_poll_ctx(
            downloading_rows=[row],
            slskd_downloads=[{
                "username": "user1",
                "directories": [{"directory": "user1\\Music", "files": [{
                    "filename": "user1\\Music\\01.flac",
                    "id": "tid-1",
                    "state": "Completed, Succeeded",
                    "bytesTransferred": 30000000,
                }]}],
            }],
        )

        with self.assertLogs("cratedigger", level=logging.ERROR) as logs:
            poll_active_downloads(ctx)

        self.assertEqual(fake_db.request(1)["status"], "wanted")
        self.assertEqual(fake_db._import_jobs, [])
        self.assertEqual(len(fake_db.download_logs), 1)
        self.assertEqual(fake_db.download_logs[0].outcome, "failed")
        self.assertIn("EVENT-PATH MISSING", "\n".join(logs.output))

    def test_poll_active_all_complete_uses_async_preview_gate(self):
        """Completed automation downloads are materialized before preview."""
        from lib.download import poll_active_downloads

        row = self._make_downloading_row()
        ctx, fake_db = self._make_poll_ctx(
            downloading_rows=[row],
            slskd_downloads=[{
                "username": "user1",
                "directories": [{"directory": "user1\\Music", "files": [{
                    "filename": "user1\\Music\\01.flac",
                    "id": "tid-1",
                    "state": "Completed, Succeeded",
                    "bytesTransferred": 30000000,
                }]}],
            }],
        )

        poll_active_downloads(ctx)

        jobs = fake_db.list_import_jobs()
        self.assertEqual(len(jobs), 1)
        self.assertEqual(jobs[0].preview_status, "waiting")
        self.assertIsNone(jobs[0].preview_message)
        claimed = fake_db.claim_next_import_preview_job(worker_id="preview")
        assert claimed is not None
        self.assertEqual(claimed.id, jobs[0].id)

    def test_poll_active_all_complete_no_beets(self):
        """beets_validation_enabled=False still queues importer ownership."""
        from lib.download import poll_active_downloads
        row = self._make_downloading_row()
        ctx, fake_db = self._make_poll_ctx(
            downloading_rows=[row],
            slskd_downloads=[{
                "username": "user1",
                "directories": [{"directory": "user1\\Music", "files": [{
                    "filename": "user1\\Music\\01.flac",
                    "id": "tid-1",
                    "state": "Completed, Succeeded",
                    "bytesTransferred": 30000000,
                }]}],
            }],
        )

        poll_active_downloads(ctx)

        self.assertGreaterEqual(len(fake_db.update_download_state_calls), 1)
        self.assertEqual(fake_db.request(1)["status"], "downloading")
        self.assertIsNotNone(fake_db.request(1)["active_download_state"])
        self.assertEqual(len(fake_db.list_import_jobs()), 1)

    def test_poll_active_timeout(self):
        """No byte/state progress for stalled_timeout → cancel, log, reset to wanted."""
        from lib.download import poll_active_downloads
        stale = "2020-01-01T00:00:00+00:00"
        state_dict = {
            "filetype": "flac",
            "enqueued_at": stale,
            "last_progress_at": stale,
            "files": [
                {"username": "user1", "filename": "user1\\Music\\01.flac",
                 "file_dir": "user1\\Music", "size": 30000000,
                 "bytes_transferred": 12345, "last_state": "InProgress"},
            ],
        }
        row = self._make_downloading_row(state_dict=state_dict)
        ctx, fake_db = self._make_poll_ctx(
            downloading_rows=[row],
            slskd_downloads=[{
                "username": "user1",
                "directories": [{"directory": "user1\\Music", "files": [{
                    "filename": "user1\\Music\\01.flac",
                    "id": "tid-1",
                    "state": "InProgress",
                    "bytesTransferred": 12345,
                }]}],
            }],
        )

        with patch("lib.download.cancel_and_delete"):
            poll_active_downloads(ctx)

        fake_db.assert_log(self, 0, outcome="timeout")
        self.assertEqual(fake_db.request(1)["status"], "wanted")
        self.assertEqual(fake_db.recorded_attempts, [(1, "download")])

    def test_poll_active_old_album_with_progress_does_not_timeout(self):
        """Fresh byte progress should refresh stall timer even for an old album."""
        from lib.download import poll_active_downloads
        stale = "2020-01-01T00:00:00+00:00"
        state_dict = {
            "filetype": "flac",
            "enqueued_at": stale,
            "last_progress_at": stale,
            "files": [
                {"username": "user1", "filename": "user1\\Music\\01.flac",
                 "file_dir": "user1\\Music", "size": 30000000,
                 "bytes_transferred": 12345, "last_state": "InProgress"},
            ],
        }
        row = self._make_downloading_row(state_dict=state_dict)
        ctx, fake_db = self._make_poll_ctx(
            downloading_rows=[row],
            slskd_downloads=[{
                "username": "user1",
                "directories": [{"directory": "user1\\Music", "files": [{
                    "filename": "user1\\Music\\01.flac",
                    "id": "tid-1",
                    "state": "InProgress",
                    "bytesTransferred": 22345,
                }]}],
            }],
        )

        poll_active_downloads(ctx)

        self.assertEqual(fake_db.download_logs, [])
        self.assertEqual(len(fake_db.update_download_state_calls), 1)
        persisted = self._download_state(fake_db)
        self.assertEqual(persisted["files"][0]["bytes_transferred"], 22345)
        self.assertIsNotNone(persisted["last_progress_at"])

    def test_poll_active_transfer_vanished_all(self):
        """slskd returns no matching transfers → treat as timeout."""
        from lib.download import poll_active_downloads
        row = self._make_downloading_row(state_dict={
            "filetype": "flac",
            "enqueued_at": (
                datetime.now(timezone.utc) - timedelta(minutes=2)
            ).isoformat(),
            "files": [
                {"username": "user1", "filename": "user1\\Music\\01.flac",
                 "file_dir": "user1\\Music", "size": 30000000},
            ],
        })
        ctx, fake_db = self._make_poll_ctx(
            downloading_rows=[row],
            slskd_downloads=[{
                "username": "user1",
                "directories": [{"directory": "user1\\Music", "files": []}],
            }],
        )
        with patch("lib.download.cancel_and_delete"):
            poll_active_downloads(ctx)

        fake_db.assert_log(self, 0, outcome="timeout")
        self.assertEqual(fake_db.request(1)["status"], "wanted")

    def test_poll_active_uses_persisted_terminal_failures_when_snapshot_drops_rows(self):
        """Terminal slskd failures remain actionable after removed rows disappear.

        Live elgoognplus M4A failures reached ``Completed, Rejected`` /
        ``Completed, Errored`` with zero bytes. A later poll snapshot no
        longer exposed those terminal rows, and the old code collapsed that
        persisted evidence into the misleading ``all transfers vanished``
        timeout.
        """
        from lib.download import poll_active_downloads
        row = self._make_downloading_row(state_dict={
            "filetype": "m4a",
            "enqueued_at": (
                datetime.now(timezone.utc) - timedelta(minutes=2)
            ).isoformat(),
            "files": [
                {
                    "username": "elgoognplus",
                    "filename": (
                        "Music\\78 Saab\\Crossed Lines\\01 No Illusions.m4a"
                    ),
                    "file_dir": "Music\\78 Saab\\Crossed Lines",
                    "size": 26799968,
                    "last_state": "Completed, Rejected",
                    "bytes_transferred": 0,
                },
                {
                    "username": "elgoognplus",
                    "filename": (
                        "Music\\78 Saab\\Crossed Lines\\02 Cops.m4a"
                    ),
                    "file_dir": "Music\\78 Saab\\Crossed Lines",
                    "size": 29382804,
                    "last_state": "Completed, Errored",
                    "bytes_transferred": 0,
                },
            ],
        })
        ctx, fake_db = self._make_poll_ctx(
            downloading_rows=[row],
            slskd_downloads=[{
                "username": "elgoognplus",
                "directories": [{
                    "directory": "Music\\78 Saab\\Crossed Lines",
                    "files": [],
                }],
            }],
        )

        with patch("lib.download.cancel_and_delete"):
            poll_active_downloads(ctx)

        fake_db.assert_log(self, 0, outcome="timeout")
        log = fake_db.download_logs[0]
        self.assertEqual(log.error_message, "all 2 files errored")
        self.assertNotEqual(log.error_message, "all transfers vanished from slskd")
        self.assertEqual(log.filetype, "m4a")
        self.assertEqual(log.soulseek_username, "elgoognplus")
        self.assertEqual(fake_db.request(1)["status"], "wanted")
        self.assertEqual(fake_db.cooldowns_applied, ["elgoognplus"])
        self.assertEqual(fake_db.denylist, [])

    def test_poll_active_fresh_preclaim_with_empty_snapshot_stays_downloading(self):
        """A same-cycle preclaim should not be reset before slskd registers transfers."""
        from lib.download import poll_active_downloads
        row = self._make_downloading_row()
        ctx, fake_db = self._make_poll_ctx(
            downloading_rows=[row],
            slskd_downloads=[{
                "username": "user1",
                "directories": [{"directory": "user1\\Music", "files": []}],
            }],
        )

        poll_active_downloads(ctx)

        self.assertEqual(fake_db.download_logs, [])
        self.assertEqual(fake_db.request(1)["status"], "downloading")

    def test_poll_active_completed_removed_transfer_uses_snapshot_status(self):
        """Completed transfers from includeRemoved=true should enqueue, not timeout."""
        from lib.download import poll_active_downloads
        row = self._make_downloading_row(state_dict={
            "filetype": "flac",
            "enqueued_at": "2026-04-03T20:00:00+00:00",
            "files": [
                {"username": "user1", "filename": "user1\\Music\\01.flac",
                 "file_dir": "user1\\Music", "size": 30000000},
            ],
        })
        ctx, fake_db = self._make_poll_ctx(
            downloading_rows=[row],
            slskd_downloads=[{
                "username": "user1",
                "directories": [{"directory": "user1\\Music", "files": [
                    {
                        "filename": "user1\\Music\\01.flac",
                        "id": "done-id",
                        "state": "Completed, Succeeded",
                        "bytesTransferred": 30000000,
                        "endedAt": "2026-04-03T21:00:00+00:00",
                    },
                ]}],
            }],
        )

        with patch("lib.download.slskd_download_status") as mock_status:
            poll_active_downloads(ctx)

        mock_status.assert_not_called()
        self.assertEqual(fake_db.download_logs, [])
        self.assertEqual(fake_db.request(1)["status"], "downloading")
        self.assertEqual(len(fake_db.list_import_jobs()), 1)

    def test_poll_active_restored_completed_success_queues_importer(self):
        """Persisted success remains complete after slskd drops removed rows."""
        from lib.download import poll_active_downloads
        row = self._make_downloading_row(state_dict={
            "filetype": "m4a",
            "enqueued_at": (
                datetime.now(timezone.utc) - timedelta(minutes=2)
            ).isoformat(),
            "files": [
                {
                    "username": "elgoognplus",
                    "filename": (
                        "Music\\78 Saab\\Crossed Lines\\01 No Illusions.m4a"
                    ),
                    "file_dir": "Music\\78 Saab\\Crossed Lines",
                    "size": 26799968,
                    "last_state": "Completed, Succeeded",
                    "bytes_transferred": 26799968,
                },
            ],
        })
        ctx, fake_db = self._make_poll_ctx(
            downloading_rows=[row],
            slskd_downloads=[{
                "username": "elgoognplus",
                "directories": [{
                    "directory": "Music\\78 Saab\\Crossed Lines",
                    "files": [],
                }],
            }],
        )

        with patch("lib.download.slskd_download_status") as mock_status:
            poll_active_downloads(ctx)

        mock_status.assert_not_called()
        self.assertEqual(fake_db.download_logs, [])
        self.assertEqual(fake_db.status_history, [])
        self.assertEqual(fake_db.cooldowns_applied, [])
        self.assertEqual(fake_db.denylist, [])
        self.assertEqual(fake_db.request(1)["status"], "downloading")
        self.assertEqual(len(fake_db.list_import_jobs(request_id=1)), 1)

    def test_poll_ignores_stale_terminal_transfer_before_claim(self):
        """A new claim must not bind to an older includeRemoved terminal transfer."""
        from lib.download import poll_active_downloads
        now = datetime.now(timezone.utc)
        row = self._make_downloading_row(state_dict={
            "filetype": "flac",
            "enqueued_at": (now - timedelta(minutes=2)).isoformat(),
            "files": [
                {"username": "user1", "filename": "user1\\Music\\01.flac",
                 "file_dir": "user1\\Music", "size": 30000000},
            ],
        })
        ctx, fake_db = self._make_poll_ctx(
            downloading_rows=[row],
            slskd_downloads=[{
                "username": "user1",
                "directories": [{"directory": "user1\\Music", "files": [{
                    "filename": "user1\\Music\\01.flac",
                    "id": "old-done-id",
                    "state": "Completed, Succeeded",
                    "bytesTransferred": 30000000,
                    "endedAt": (now - timedelta(hours=1)).isoformat(),
                }]}],
            }],
        )

        with patch("lib.download.cancel_and_delete"):
            poll_active_downloads(ctx)

        fake_db.assert_log(self, 0, outcome="timeout")
        self.assertEqual(fake_db.request(1)["status"], "wanted")
        self.assertEqual(fake_db.list_import_jobs(), [])

    def test_poll_active_in_progress(self):
        """Files still downloading with fresh state transition → persist progress snapshot."""
        from lib.download import poll_active_downloads
        row = self._make_downloading_row()
        ctx, fake_db = self._make_poll_ctx(
            downloading_rows=[row],
            slskd_downloads=[{
                "username": "user1",
                "directories": [{"directory": "user1\\Music", "files": [{
                    "filename": "user1\\Music\\01.flac",
                    "id": "tid-1",
                    "state": "InProgress",
                    "bytesTransferred": 2048,
                }]}],
            }],
        )

        poll_active_downloads(ctx)

        # Should NOT process or timeout
        self.assertEqual(len(fake_db.update_download_state_calls), 1)
        self.assertEqual(fake_db.download_logs, [])
        self.assertEqual(fake_db.request(1)["status"], "downloading")

    def test_poll_active_multiple_albums(self):
        """2 albums: 1 completes, 1 in progress → correct handling."""
        from lib.download import poll_active_downloads
        row1 = self._make_downloading_row(request_id=1)
        state2 = {
            "filetype": "mp3 v0",
            "enqueued_at": _utc_now_iso(),
            "files": [
                {"username": "user2", "filename": "user2\\Music\\01.mp3",
                 "file_dir": "user2\\Music", "size": 5000000},
            ],
        }
        row2 = self._make_downloading_row(request_id=2, state_dict=state2)
        row2["album_title"] = "Album 2"
        row2["artist_name"] = "Artist 2"

        ctx, fake_db = self._make_poll_ctx(
            downloading_rows=[row1, row2],
            slskd_downloads=[
                {
                    "username": "user1",
                    "directories": [{"directory": "user1\\Music", "files": [{
                        "filename": "user1\\Music\\01.flac",
                        "id": "tid-1",
                        "state": "Completed, Succeeded",
                        "bytesTransferred": 30000000,
                    }]}],
                },
                {
                    "username": "user2",
                    "directories": [{"directory": "user2\\Music", "files": [{
                        "filename": "user2\\Music\\01.mp3",
                        "id": "tid-2",
                        "state": "InProgress",
                        "bytesTransferred": 2048,
                    }]}],
                },
            ],
        )

        # slskd returns transfers for both users
        self.assertEqual(cast(FakeSlskdAPI, ctx.slskd).transfers.get_all_downloads_calls, [])

        poll_active_downloads(ctx)

        # Album 1 persists processing/materialization, album 2 persists progress.
        self.assertEqual(len(fake_db.update_download_state_calls), 3)
        update_request_ids = [
            request_id for request_id, _ in fake_db.update_download_state_calls
        ]
        self.assertEqual(update_request_ids, [1, 1, 2])
        self.assertIsNotNone(fake_db.request(1)["active_download_state"])
        self.assertIsNotNone(self._download_state(fake_db, 2)["last_progress_at"])
        self.assertEqual(len(fake_db.list_import_jobs(request_id=1)), 1)

    def test_poll_crash_recovery_no_state(self):
        """Downloading album with no active_download_state → reset to wanted."""
        from lib.download import poll_active_downloads
        row = self._make_downloading_row()
        row["active_download_state"] = None  # Simulates crash
        ctx, fake_db = self._make_poll_ctx(downloading_rows=[row])

        poll_active_downloads(ctx)

        # apply_transition calls reset_to_wanted for downloading→wanted
        self.assertEqual(fake_db.request(1)["status"], "wanted")
        self.assertEqual(fake_db.status_history, [(1, "wanted")])

    def test_poll_active_all_errors(self):
        """All files errored → timeout the album.

        ``poll_active_downloads`` enqueues an import job rather than
        calling ``process_completed_album`` directly, so the absence of a
        new ``import_jobs`` row is the observable contract for "no
        downstream processing started".
        """
        from lib.download import poll_active_downloads
        row = self._make_downloading_row()
        ctx, fake_db = self._make_poll_ctx(
            downloading_rows=[row],
            slskd_downloads=[{
                "username": "user1",
                "directories": [{"directory": "user1\\Music", "files": [
                    {
                        "filename": "user1\\Music\\01.flac",
                        "id": "tid-1",
                        "state": "Completed, Errored",
                    },
                ]}],
            }],
        )
        with patch("lib.download.cancel_and_delete"):
            poll_active_downloads(ctx)

        self.assertEqual(fake_db._import_jobs, [])
        fake_db.assert_log(self, 0, outcome="timeout")
        self.assertEqual(fake_db.request(1)["status"], "wanted")

    def test_poll_active_remote_queue_timeout(self):
        """All files queued remotely past timeout → timeout."""
        from lib.download import poll_active_downloads
        # enqueued long enough ago to exceed remote_queue_timeout but not stalled_timeout
        from datetime import datetime, timezone, timedelta
        past = (datetime.now(timezone.utc) - timedelta(seconds=200)).isoformat()
        state_dict = {
            "filetype": "flac",
            "enqueued_at": past,
            "files": [
                {"username": "user1", "filename": "user1\\Music\\01.flac",
                 "file_dir": "user1\\Music", "size": 30000000},
            ],
        }
        row = self._make_downloading_row(state_dict=state_dict)
        ctx, fake_db = self._make_poll_ctx(
            downloading_rows=[row],
            slskd_downloads=[{
                "username": "user1",
                "directories": [{"directory": "user1\\Music", "files": [{
                    "filename": "user1\\Music\\01.flac",
                    "id": "tid-1",
                    "state": "Queued, Remotely",
                }]}],
            }],
        )
        cfg = cast(Any, ctx.cfg)
        cfg.remote_queue_timeout = 120  # 2 minutes
        cfg.stalled_timeout = 600  # 10 minutes (not exceeded)

        with patch("lib.download.cancel_and_delete"):
            poll_active_downloads(ctx)

        fake_db.assert_log(self, 0, outcome="timeout")
        self.assertEqual(fake_db.request(1)["status"], "wanted")

    def test_poll_active_remote_queue_does_not_use_stalled_timeout(self):
        """Fully remote-queued albums should not hit stalled_timeout first."""
        from lib.download import poll_active_downloads
        now = datetime.now(timezone.utc)
        enqueued_at = (now - timedelta(seconds=200)).isoformat()
        stale_progress = (now - timedelta(seconds=1200)).isoformat()
        state_dict = {
            "filetype": "flac",
            "enqueued_at": enqueued_at,
            "last_progress_at": stale_progress,
            "files": [
                {"username": "user1", "filename": "user1\\Music\\01.flac",
                 "file_dir": "user1\\Music", "size": 30000000},
            ],
        }
        row = self._make_downloading_row(state_dict=state_dict)
        ctx, fake_db = self._make_poll_ctx(
            downloading_rows=[row],
            slskd_downloads=[{
                "username": "user1",
                "directories": [{"directory": "user1\\Music", "files": [{
                    "filename": "user1\\Music\\01.flac",
                    "id": "tid-1",
                    "state": "Queued, Remotely",
                }]}],
            }],
        )
        cfg = cast(Any, ctx.cfg)
        cfg.remote_queue_timeout = 3600
        cfg.stalled_timeout = 120

        with patch("lib.download.cancel_and_delete"):
            poll_active_downloads(ctx)

        self.assertEqual(fake_db.download_logs, [])
        self.assertEqual(fake_db.update_download_state_calls, [])
        self.assertEqual(fake_db.request(1)["status"], "downloading")

    def test_poll_transfer_vanished_partial(self):
        """7/12 files vanish → treated as errors, not complete.

        ``poll_active_downloads`` only kicks off downstream processing
        via the import-job queue, so the assertions below check the
        observable contract (no new import_jobs row, no download_log).
        """
        from lib.download import poll_active_downloads
        # 12 files, only 5 have transfers in slskd
        files = []
        for i in range(12):
            files.append({"username": "user1",
                          "filename": f"user1\\Music\\{i:02d}.flac",
                          "file_dir": "user1\\Music", "size": 30000000})
        state_dict = {
            "filetype": "flac",
            "enqueued_at": _utc_now_iso(),
            "files": files,
        }
        row = self._make_downloading_row(state_dict=state_dict)
        ctx, fake_db = self._make_poll_ctx(downloading_rows=[row])

        # Only files 0-4 have transfers in slskd
        slskd_files = [
            {
                "filename": f"user1\\Music\\{i:02d}.flac",
                "id": f"tid-{i}",
                "state": "InProgress",
            }
            for i in range(5)
        ]
        slskd = cast(FakeSlskdAPI, ctx.slskd)
        slskd.set_downloads([{
            "username": "user1",
            "directories": [{"directory": "user1\\Music", "files": slskd_files}],
        }])
        slskd.transfers.enqueue_result = False

        with self.assertLogs("cratedigger", level="WARNING") as logs:
            poll_active_downloads(ctx)

        # Should NOT process — 7 files vanished (errored), album not complete
        self.assertEqual(fake_db._import_jobs, [])
        self.assertEqual(
            "\n".join(logs.output).count("Failed to re-enqueue file"),
            7,
        )
        self.assertEqual(len(slskd.transfers.enqueue_calls), 7)
        self.assertEqual(fake_db.download_logs, [])
        self.assertEqual(fake_db.request(1)["status"], "downloading")


    def test_poll_active_partial_errors_with_retry(self):
        """Some files errored, retries available → re-enqueue those files."""
        from lib.download import poll_active_downloads
        # 3 files: 2 complete, 1 errored
        state_dict = {
            "filetype": "flac",
            "enqueued_at": _utc_now_iso(),
            "files": [
                {"username": "user1", "filename": "user1\\Music\\01.flac",
                 "file_dir": "user1\\Music", "size": 30000000},
                {"username": "user1", "filename": "user1\\Music\\02.flac",
                 "file_dir": "user1\\Music", "size": 25000000},
                {"username": "user1", "filename": "user1\\Music\\03.flac",
                 "file_dir": "user1\\Music", "size": 20000000},
            ],
        }
        row = self._make_downloading_row(state_dict=state_dict)
        ctx, fake_db = self._make_poll_ctx(
            downloading_rows=[row],
            slskd_downloads=[],
        )

        # First snapshot is the poll cycle; second snapshot is the
        # post-requeue transfer-id lookup.
        poll_snapshot = [{
            "username": "user1",
            "directories": [{"directory": "user1\\Music", "files": [
                {
                    "filename": "user1\\Music\\01.flac",
                    "id": "tid-1",
                    "state": "Completed, Succeeded",
                },
                {
                    "filename": "user1\\Music\\02.flac",
                    "id": "tid-2",
                    "state": "Completed, Succeeded",
                },
                {
                    "filename": "user1\\Music\\03.flac",
                    "id": "tid-3",
                    "state": "Completed, Errored",
                },
            ]}],
        }]
        requeue_snapshot = [{
            "username": "user1",
            "directories": [{"directory": "user1\\Music", "files": [{
                "filename": "user1\\Music\\03.flac",
                "id": "new-tid-3",
                "state": "Queued, Locally",
            }]}],
        }]
        slskd = cast(FakeSlskdAPI, ctx.slskd)
        slskd.queue_download_snapshots(poll_snapshot, requeue_snapshot)

        with patch("time.sleep"):
            poll_active_downloads(ctx)

        # Should NOT process (not all done) and NOT timeout
        self.assertEqual(fake_db._import_jobs, [])
        self.assertEqual(fake_db.download_logs, [])
        # Should re-enqueue the errored file
        self.assertEqual(len(slskd.transfers.enqueue_calls), 1)
        enqueue_call = slskd.transfers.enqueue_calls[0]
        self.assertEqual(enqueue_call.username, "user1")
        self.assertEqual(
            enqueue_call.files,
            [{"filename": "user1\\Music\\03.flac", "size": 20000000}],
        )
        persisted = self._download_state(fake_db)
        self.assertEqual(persisted["files"][2]["retry_count"], 1)

    def test_poll_active_get_all_downloads_api_error_waits_for_next_cycle(self):
        """Transient bulk-download API failures must not be treated as vanished transfers."""
        from lib.download import poll_active_downloads
        row = self._make_downloading_row()
        ctx, fake_db = self._make_poll_ctx(downloading_rows=[row])
        cast(FakeSlskdAPI, ctx.slskd).transfers.get_all_downloads_error = (
            RuntimeError("temporary slskd failure")
        )

        with patch("lib.download.cancel_and_delete") as mock_cancel:
            poll_active_downloads(ctx)

        mock_cancel.assert_not_called()
        self.assertEqual(fake_db.download_logs, [])
        self.assertEqual(fake_db.update_download_state_calls, [])
        self.assertEqual(fake_db.request(1)["status"], "downloading")
        self.assertEqual(fake_db.status_history, [])

    def test_poll_active_completion_queues_and_persists_processing_state(self):
        """Completion should leave persisted state for the importer to resume."""
        from lib.download import poll_active_downloads
        row = self._make_downloading_row()
        ctx, fake_db = self._make_poll_ctx(
            downloading_rows=[row],
            slskd_downloads=[{
                "username": "user1",
                "directories": [{"directory": "user1\\Music", "files": [{
                    "filename": "user1\\Music\\01.flac",
                    "id": "tid-1",
                    "state": "Completed, Succeeded",
                    "bytesTransferred": 30000000,
                }]}],
            }],
        )

        poll_active_downloads(ctx)

        self.assertEqual(fake_db.download_logs, [])
        self.assertEqual(fake_db.status_history, [])
        self.assertEqual(len(fake_db.update_download_state_calls), 2)
        persisted = self._download_state(fake_db)
        self.assertIsNotNone(persisted["processing_started_at"])
        self.assertIsNotNone(persisted["current_path"])
        self.assertTrue(
            persisted["current_path"].endswith("Test Artist - Test Album (2020)")
        )
        self.assertEqual(len(fake_db.list_import_jobs(request_id=1)), 1)

    def test_poll_resume_processing_queues_persisted_current_path(self):
        """Resume path keeps the post-move directory for the importer."""
        from lib.download import poll_active_downloads
        from lib.processing_paths import stage_to_ai_path
        import tempfile
        with tempfile.TemporaryDirectory() as tmpdir:
            staging_root = os.path.join(tmpdir, "staging")
            current_path = stage_to_ai_path(
                artist="Test Artist",
                title="Test Album",
                staging_dir=staging_root,
                request_id=1,
                auto_import=False,
            )
            os.makedirs(current_path)
            with open(os.path.join(current_path, "01.flac"), "w") as fp:
                fp.write("audio")
            row = self._make_downloading_row(state_dict={
                "filetype": "flac",
                "enqueued_at": _utc_now_iso(),
                "processing_started_at": _utc_now_iso(),
                "current_path": current_path,
                "files": [
                    {"username": "user1", "filename": "user1\\Music\\01.flac",
                     "file_dir": "user1\\Music", "size": 30000000},
                ],
            })
            ctx, _fake_db = self._make_poll_ctx(downloading_rows=[row], slskd_downloads=[])
            cfg = cast(Any, ctx.cfg)
            cfg.beets_staging_dir = staging_root

            poll_active_downloads(ctx)

            self.assertEqual(
                _fake_db.request(1)["active_download_state"]["current_path"],
                current_path,
            )
            self.assertEqual(len(_fake_db.list_import_jobs(request_id=1)), 1)

    def test_poll_legacy_processing_row_uses_canonical_fallback(self):
        """Legacy mid-processing rows without current_path still resume canonically."""
        from lib.download import poll_active_downloads
        from lib.processing_paths import canonical_processing_path
        import tempfile
        with tempfile.TemporaryDirectory() as tmpdir:
            downloads_root = os.path.join(tmpdir, "downloads")
            canonical_path = canonical_processing_path(
                artist="Test Artist",
                title="Test Album",
                year="2020",
                slskd_download_dir=downloads_root,
            )
            os.makedirs(canonical_path)
            with open(os.path.join(canonical_path, "01.flac"), "w") as fp:
                fp.write("audio")
            row = self._make_downloading_row(state_dict={
                "filetype": "flac",
                "enqueued_at": _utc_now_iso(),
                "processing_started_at": _utc_now_iso(),
                "files": [
                    {"username": "user1", "filename": "user1\\Music\\01.flac",
                     "file_dir": "user1\\Music", "size": 30000000},
                ],
            })
            ctx, _fake_db = self._make_poll_ctx(downloading_rows=[row], slskd_downloads=[])
            cfg = cast(Any, ctx.cfg)
            cfg.slskd_download_dir = downloads_root

            poll_active_downloads(ctx)

            self.assertEqual(
                _fake_db.request(1)["active_download_state"]["current_path"],
                canonical_path,
            )
            self.assertGreaterEqual(
                len(_fake_db.update_download_state_current_path_calls),
                1,
            )
            self.assertEqual(
                _fake_db.update_download_state_current_path_calls[-1],
                (1, canonical_path),
            )
            self.assertEqual(len(_fake_db.list_import_jobs(request_id=1)), 1)

    def test_poll_mid_processing_row_uses_request_scoped_staging_fallback(self):
        """Move/persist crashes should recover from request-scoped staged dirs."""
        from lib.download import poll_active_downloads
        from lib.processing_paths import stage_to_ai_path
        import tempfile
        with tempfile.TemporaryDirectory() as tmpdir:
            staging_root = os.path.join(tmpdir, "staging")
            staged_path = stage_to_ai_path(
                artist="Test Artist",
                title="Test Album",
                staging_dir=staging_root,
                request_id=1,
                auto_import=True,
            )
            os.makedirs(staged_path)
            with open(os.path.join(staged_path, "01.flac"), "w") as fp:
                fp.write("audio")

            row = self._make_downloading_row(state_dict={
                "filetype": "flac",
                "enqueued_at": _utc_now_iso(),
                "processing_started_at": _utc_now_iso(),
                "files": [
                    {"username": "user1", "filename": "user1\\Music\\01.flac",
                     "file_dir": "user1\\Music", "size": 30000000},
                ],
            })
            ctx, fake_db = self._make_poll_ctx(downloading_rows=[row], slskd_downloads=[])
            cfg = cast(Any, ctx.cfg)
            cfg.slskd_download_dir = os.path.join(tmpdir, "downloads")
            cfg.beets_staging_dir = staging_root

            poll_active_downloads(ctx)

            self.assertEqual(
                fake_db.request(1)["active_download_state"]["current_path"],
                staged_path,
            )
            self.assertEqual(fake_db.status_history, [])
            self.assertEqual(len(fake_db.list_import_jobs(request_id=1)), 1)

    def test_poll_stale_canonical_current_path_uses_request_scoped_staging_fallback(
        self,
    ):
        """A stale canonical current_path must recover to the staged location."""
        from lib.download import poll_active_downloads
        from lib.processing_paths import canonical_processing_path, stage_to_ai_path
        import tempfile
        with tempfile.TemporaryDirectory() as tmpdir:
            downloads_root = os.path.join(tmpdir, "downloads")
            staging_root = os.path.join(tmpdir, "staging")
            canonical_path = canonical_processing_path(
                artist="Test Artist",
                title="Test Album",
                year="2020",
                slskd_download_dir=downloads_root,
            )
            staged_path = stage_to_ai_path(
                artist="Test Artist",
                title="Test Album",
                staging_dir=staging_root,
                request_id=1,
                auto_import=True,
            )
            os.makedirs(staged_path)
            with open(os.path.join(staged_path, "01.flac"), "w") as fp:
                fp.write("audio")

            row = self._make_downloading_row(state_dict={
                "filetype": "flac",
                "enqueued_at": _utc_now_iso(),
                "processing_started_at": _utc_now_iso(),
                "current_path": canonical_path,
                "files": [
                    {"username": "user1", "filename": "user1\\Music\\01.flac",
                     "file_dir": "user1\\Music", "size": 30000000},
                ],
            })
            ctx, fake_db = self._make_poll_ctx(downloading_rows=[row], slskd_downloads=[])
            cfg = cast(Any, ctx.cfg)
            cfg.slskd_download_dir = downloads_root
            cfg.beets_staging_dir = staging_root

            poll_active_downloads(ctx)

            self.assertEqual(
                fake_db.request(1)["active_download_state"]["current_path"],
                staged_path,
            )
            self.assertEqual(len(fake_db.list_import_jobs(request_id=1)), 1)

    def test_poll_legacy_processing_row_blocks_on_ambiguous_staged_dir(self):
        """Legacy rows must not guess a shared staged dir as current_path."""
        from lib.download import poll_active_downloads
        import tempfile
        with tempfile.TemporaryDirectory() as tmpdir:
            staging_root = os.path.join(tmpdir, "staging")
            staged_path = os.path.join(staging_root, "Test Artist", "Test Album")
            os.makedirs(staged_path)
            with open(os.path.join(staged_path, "01.flac"), "w") as fp:
                fp.write("audio")

            row = self._make_downloading_row(state_dict={
                "filetype": "flac",
                "enqueued_at": _utc_now_iso(),
                "processing_started_at": _utc_now_iso(),
                "files": [
                    {"username": "user1", "filename": "user1\\Music\\01.flac",
                     "file_dir": "user1\\Music", "size": 30000000},
                ],
            })
            ctx, fake_db = self._make_poll_ctx(downloading_rows=[row], slskd_downloads=[])
            cfg = cast(Any, ctx.cfg)
            cfg.slskd_download_dir = os.path.join(tmpdir, "downloads")
            cfg.beets_staging_dir = staging_root

            with self.assertLogs("cratedigger", level="ERROR") as logs:
                poll_active_downloads(ctx)

            self.assertEqual(fake_db.request(1)["status"], "downloading")
            self.assertIsNone(fake_db.request(1)["active_download_state"].get("current_path"))
            self.assertEqual(fake_db.update_download_state_calls, [])
            self.assertIn(
                "LEGACY STAGED RESUME BLOCKED",
                "\n".join(logs.output),
            )

    def test_poll_legacy_processing_row_blocks_when_canonical_and_legacy_stage_both_exist(self):
        """Split legacy state must not pick one side and requeue the other."""
        from lib.download import poll_active_downloads
        import tempfile
        with tempfile.TemporaryDirectory() as tmpdir:
            canonical_path = os.path.join(
                tmpdir, "downloads", "Test Artist - Test Album (2020)")
            os.makedirs(canonical_path)
            with open(os.path.join(canonical_path, "01.flac"), "w") as fp:
                fp.write("audio")

            staging_root = os.path.join(tmpdir, "staging")
            staged_path = os.path.join(staging_root, "Test Artist", "Test Album")
            os.makedirs(staged_path)
            with open(os.path.join(staged_path, "01.flac"), "w") as fp:
                fp.write("audio")

            row = self._make_downloading_row(state_dict={
                "filetype": "flac",
                "enqueued_at": _utc_now_iso(),
                "processing_started_at": _utc_now_iso(),
                "files": [
                    {"username": "user1", "filename": "user1\\Music\\01.flac",
                     "file_dir": "user1\\Music", "size": 30000000},
                ],
            })
            ctx, fake_db = self._make_poll_ctx(downloading_rows=[row], slskd_downloads=[])
            cfg = cast(Any, ctx.cfg)
            cfg.slskd_download_dir = os.path.join(tmpdir, "downloads")
            cfg.beets_staging_dir = staging_root

            with self.assertLogs("cratedigger", level="ERROR") as logs:
                poll_active_downloads(ctx)

            self.assertEqual(fake_db.request(1)["status"], "downloading")
            self.assertIsNone(fake_db.request(1)["active_download_state"].get("current_path"))
            self.assertEqual(fake_db.update_download_state_calls, [])
            self.assertIn("MID-PROCESS RESUME BLOCKED", "\n".join(logs.output))

    def test_poll_missing_persisted_current_path_resets_to_wanted(self):
        """Missing persisted staging dirs should fail closed back to wanted."""
        from lib.download import poll_active_downloads
        import tempfile
        with tempfile.TemporaryDirectory() as tmpdir:
            row = self._make_downloading_row(state_dict={
                "filetype": "flac",
                "enqueued_at": _utc_now_iso(),
                "processing_started_at": _utc_now_iso(),
                "current_path": os.path.join(tmpdir, "missing"),
                "files": [
                    {"username": "user1", "filename": "user1\\Music\\01.flac",
                     "file_dir": "user1\\Music", "size": 30000000},
                ],
            })
            ctx, fake_db = self._make_poll_ctx(downloading_rows=[row], slskd_downloads=[])

            poll_active_downloads(ctx)

            self.assertEqual(fake_db.request(1)["status"], "wanted")
            self.assertIn((1, "wanted"), fake_db.status_history)

    def test_poll_missing_canonical_processing_path_queues_importer(self):
        """Missing canonical path can be pre-materialization, not post-move loss."""
        from lib.download import poll_active_downloads
        from lib.processing_paths import canonical_processing_path
        import tempfile
        with tempfile.TemporaryDirectory() as tmpdir:
            download_root = os.path.join(tmpdir, "downloads")
            current_path = canonical_processing_path(
                artist="Test Artist",
                title="Test Album",
                year="2020",
                slskd_download_dir=download_root,
            )
            row = self._make_downloading_row(state_dict={
                "filetype": "flac",
                "enqueued_at": _utc_now_iso(),
                "processing_started_at": _utc_now_iso(),
                "current_path": current_path,
                "files": [
                    {"username": "user1", "filename": "user1\\Music\\01.flac",
                     "file_dir": "user1\\Music", "size": 30000000},
                ],
            })
            ctx, fake_db = self._make_poll_ctx(downloading_rows=[row], slskd_downloads=[])
            cfg = cast(Any, ctx.cfg)
            cfg.slskd_download_dir = download_root
            source_dir = os.path.join(download_root, "Music")
            os.makedirs(source_dir, exist_ok=True)
            with open(os.path.join(source_dir, "01.flac"), "wb") as fp:
                fp.write(b"test audio")

            poll_active_downloads(ctx)

            self.assertEqual(fake_db.request(1)["status"], "downloading")
            self.assertEqual(fake_db.status_history, [])
            self.assertEqual(len(fake_db.list_import_jobs(request_id=1)), 1)

    def test_poll_post_move_staged_path_without_validation_queues(self):
        """Staged retries are queued for importer ownership."""
        from lib.download import poll_active_downloads
        from lib.processing_paths import stage_to_ai_path
        import tempfile
        with tempfile.TemporaryDirectory() as tmpdir:
            staging_root = os.path.join(tmpdir, "staging")
            resumed_path = stage_to_ai_path(
                artist="Test Artist",
                title="Test Album",
                staging_dir=staging_root,
                request_id=1,
                auto_import=False,
            )
            os.makedirs(resumed_path)
            with open(os.path.join(resumed_path, "01.flac"), "w") as fp:
                fp.write("audio")

            row = self._make_downloading_row(state_dict={
                "filetype": "flac",
                "enqueued_at": _utc_now_iso(),
                "processing_started_at": _utc_now_iso(),
                "current_path": resumed_path,
                "files": [
                    {"username": "user1", "filename": "user1\\Music\\01.flac",
                     "file_dir": "user1\\Music", "size": 30000000},
                ],
            })
            ctx, fake_db = self._make_poll_ctx(downloading_rows=[row], slskd_downloads=[])
            cfg = cast(Any, ctx.cfg)
            cfg.beets_staging_dir = staging_root

            poll_active_downloads(ctx)

            self.assertEqual(fake_db.request(1)["status"], "downloading")
            self.assertEqual(fake_db.status_history, [])
            self.assertEqual(len(fake_db.list_import_jobs(request_id=1)), 1)
            self.assertEqual(
                fake_db.request(1)["active_download_state"]["current_path"],
                resumed_path,
            )

    def test_poll_post_move_staged_path_with_missing_file_abandons_and_resets(self):
        """Subprocess-started auto-import residue is abandoned for redownload."""
        from lib.download import poll_active_downloads
        from lib.processing_paths import stage_to_ai_path
        import tempfile
        with tempfile.TemporaryDirectory() as tmpdir:
            staging_root = os.path.join(tmpdir, "staging")
            resumed_path = stage_to_ai_path(
                artist="Test Artist",
                title="Test Album",
                staging_dir=staging_root,
                request_id=1,
                auto_import=True,
            )
            os.makedirs(resumed_path)

            row = self._make_downloading_row(state_dict={
                "filetype": "flac",
                "enqueued_at": _utc_now_iso(),
                "processing_started_at": _utc_now_iso(),
                "import_subprocess_started_at": _utc_now_iso(),
                "current_path": resumed_path,
                "files": [
                    {"username": "user1", "filename": "user1\\Music\\01.flac",
                     "file_dir": "user1\\Music", "size": 30000000},
                ],
            })
            ctx, fake_db = self._make_poll_ctx(downloading_rows=[row], slskd_downloads=[])
            cfg = cast(Any, ctx.cfg)
            cfg.beets_staging_dir = staging_root

            poll_active_downloads(ctx)

            self.assertEqual(fake_db.request(1)["status"], "wanted")
            self.assertIn((1, "wanted"), fake_db.status_history)
            self.assertIsNone(fake_db.request(1)["active_download_state"])
            failed_parent = os.path.join(
                os.path.dirname(resumed_path),
                "failed_imports",
            )
            self.assertTrue(os.path.isdir(failed_parent))
            moved = os.listdir(failed_parent)
            self.assertEqual(len(moved), 1)
            self.assertTrue(moved[0].startswith("abandoned_auto_import"))
            self.assertEqual(len(fake_db.download_logs), 1)
            fake_db.assert_log(
                self,
                0,
                outcome="failed",
                beets_scenario="abandoned_auto_import",
            )
            self.assertIn(
                "Abandoned interrupted auto-import",
                fake_db.download_logs[0].error_message or "",
            )
            self.assertEqual(fake_db.denylist, [])
            self.assertEqual(fake_db.cooldowns_applied, [])

    def test_poll_subprocess_started_auto_import_waits_for_active_manual_job(self):
        """Any active import job owns the request, not just automation jobs."""
        from lib.download import poll_active_downloads
        from lib.import_queue import IMPORT_JOB_MANUAL, manual_import_payload
        from lib.processing_paths import stage_to_ai_path
        import tempfile
        with tempfile.TemporaryDirectory() as tmpdir:
            staging_root = os.path.join(tmpdir, "staging")
            resumed_path = stage_to_ai_path(
                artist="Test Artist",
                title="Test Album",
                staging_dir=staging_root,
                request_id=1,
                auto_import=True,
            )
            os.makedirs(resumed_path)
            with open(os.path.join(resumed_path, "01.opus"), "w") as fp:
                fp.write("converted audio")

            row = self._make_downloading_row(state_dict={
                "filetype": "flac",
                "enqueued_at": _utc_now_iso(),
                "processing_started_at": _utc_now_iso(),
                "import_subprocess_started_at": _utc_now_iso(),
                "current_path": resumed_path,
                "files": [
                    {"username": "user1", "filename": "user1\\Music\\01.flac",
                     "file_dir": "user1\\Music", "size": 30000000},
                ],
            })
            ctx, fake_db = self._make_poll_ctx(downloading_rows=[row], slskd_downloads=[])
            cfg = cast(Any, ctx.cfg)
            cfg.beets_staging_dir = staging_root
            fake_db.enqueue_import_job(
                IMPORT_JOB_MANUAL,
                request_id=1,
                dedupe_key="manual:1",
                payload=manual_import_payload(failed_path=resumed_path),
            )

            poll_active_downloads(ctx)

            self.assertEqual(fake_db.request(1)["status"], "downloading")
            self.assertTrue(os.path.exists(resumed_path))
            self.assertEqual(fake_db.download_logs, [])
            self.assertEqual(fake_db.status_history, [])

    def test_poll_abandon_waits_when_release_lock_is_held(self):
        """A held release lock means a live importer still owns the path."""
        from lib.download import poll_active_downloads
        from lib.processing_paths import stage_to_ai_path
        import tempfile
        with tempfile.TemporaryDirectory() as tmpdir:
            staging_root = os.path.join(tmpdir, "staging")
            resumed_path = stage_to_ai_path(
                artist="Test Artist",
                title="Test Album",
                staging_dir=staging_root,
                request_id=1,
                auto_import=True,
            )
            os.makedirs(resumed_path)

            row = self._make_downloading_row(state_dict={
                "filetype": "flac",
                "enqueued_at": _utc_now_iso(),
                "processing_started_at": _utc_now_iso(),
                "import_subprocess_started_at": _utc_now_iso(),
                "current_path": resumed_path,
                "files": [
                    {"username": "user1", "filename": "user1\\Music\\01.flac",
                     "file_dir": "user1\\Music", "size": 30000000},
                ],
            })
            ctx, fake_db = self._make_poll_ctx(downloading_rows=[row], slskd_downloads=[])
            cfg = cast(Any, ctx.cfg)
            cfg.beets_staging_dir = staging_root
            fake_db.set_advisory_lock_result(False)

            poll_active_downloads(ctx)

            self.assertEqual(fake_db.request(1)["status"], "downloading")
            self.assertTrue(os.path.exists(resumed_path))
            self.assertEqual(fake_db.download_logs, [])
            self.assertEqual(fake_db.status_history, [])

    def test_poll_abandon_rolls_back_move_when_db_guard_fails(self):
        """If the guarded DB commit loses ownership, restore the staged dir."""
        from lib.download import poll_active_downloads
        from lib.processing_paths import stage_to_ai_path
        import tempfile
        with tempfile.TemporaryDirectory() as tmpdir:
            staging_root = os.path.join(tmpdir, "staging")
            resumed_path = stage_to_ai_path(
                artist="Test Artist",
                title="Test Album",
                staging_dir=staging_root,
                request_id=1,
                auto_import=True,
            )
            os.makedirs(resumed_path)

            row = self._make_downloading_row(state_dict={
                "filetype": "flac",
                "enqueued_at": _utc_now_iso(),
                "processing_started_at": _utc_now_iso(),
                "import_subprocess_started_at": _utc_now_iso(),
                "current_path": resumed_path,
                "files": [
                    {"username": "user1", "filename": "user1\\Music\\01.flac",
                     "file_dir": "user1\\Music", "size": 30000000},
                ],
            })
            ctx, fake_db = self._make_poll_ctx(downloading_rows=[row], slskd_downloads=[])
            cfg = cast(Any, ctx.cfg)
            cfg.beets_staging_dir = staging_root
            fake_db.abandon_auto_import_request = lambda **_kwargs: None  # type: ignore[method-assign]

            poll_active_downloads(ctx)

            self.assertEqual(fake_db.request(1)["status"], "downloading")
            self.assertTrue(os.path.exists(resumed_path))
            self.assertEqual(fake_db.download_logs, [])
            self.assertEqual(fake_db.status_history, [])

    def test_poll_abandon_blocks_when_path_liveness_is_unknown(self):
        """Stat errors are not treated as confirmed missing staged paths."""
        from lib.download import poll_active_downloads
        from lib.processing_paths import stage_to_ai_path
        import tempfile
        with tempfile.TemporaryDirectory() as tmpdir:
            staging_root = os.path.join(tmpdir, "staging")
            resumed_path = stage_to_ai_path(
                artist="Test Artist",
                title="Test Album",
                staging_dir=staging_root,
                request_id=1,
                auto_import=True,
            )
            os.makedirs(resumed_path)

            row = self._make_downloading_row(state_dict={
                "filetype": "flac",
                "enqueued_at": _utc_now_iso(),
                "processing_started_at": _utc_now_iso(),
                "import_subprocess_started_at": _utc_now_iso(),
                "current_path": resumed_path,
                "files": [
                    {"username": "user1", "filename": "user1\\Music\\01.flac",
                     "file_dir": "user1\\Music", "size": 30000000},
                ],
            })
            ctx, fake_db = self._make_poll_ctx(downloading_rows=[row], slskd_downloads=[])
            cfg = cast(Any, ctx.cfg)
            cfg.beets_staging_dir = staging_root
            real_stat = os.stat

            def stat_or_fail(path, *args, **kwargs):
                if path == resumed_path:
                    raise OSError("mount unavailable")
                return real_stat(path, *args, **kwargs)

            with patch("lib.download_processing.os.stat", side_effect=stat_or_fail):
                poll_active_downloads(ctx)

            self.assertEqual(fake_db.request(1)["status"], "downloading")
            self.assertTrue(os.path.exists(resumed_path))
            self.assertEqual(fake_db.download_logs, [])
            self.assertEqual(fake_db.status_history, [])

    def test_poll_post_move_auto_import_path_with_missing_dir_abandons_and_resets(self):
        """Missing subprocess-started auto-import staging dir is retryable."""
        from lib.download import poll_active_downloads
        from lib.processing_paths import stage_to_ai_path
        import tempfile
        with tempfile.TemporaryDirectory() as tmpdir:
            staging_root = os.path.join(tmpdir, "staging")
            resumed_path = stage_to_ai_path(
                artist="Test Artist",
                title="Test Album",
                staging_dir=staging_root,
                request_id=1,
                auto_import=True,
            )

            row = self._make_downloading_row(state_dict={
                "filetype": "flac",
                "enqueued_at": _utc_now_iso(),
                "processing_started_at": _utc_now_iso(),
                "import_subprocess_started_at": _utc_now_iso(),
                "current_path": resumed_path,
                "files": [
                    {"username": "user1", "filename": "user1\\Music\\01.flac",
                     "file_dir": "user1\\Music", "size": 30000000},
                ],
            })
            ctx, fake_db = self._make_poll_ctx(downloading_rows=[row], slskd_downloads=[])
            cfg = cast(Any, ctx.cfg)
            cfg.beets_staging_dir = staging_root

            poll_active_downloads(ctx)

            self.assertEqual(fake_db.request(1)["status"], "wanted")
            self.assertIn((1, "wanted"), fake_db.status_history)
            self.assertIsNone(fake_db.request(1)["active_download_state"])
            self.assertEqual(len(fake_db.download_logs), 1)
            fake_db.assert_log(
                self,
                0,
                outcome="failed",
                beets_scenario="abandoned_auto_import",
            )
            self.assertIsNone(fake_db.download_logs[0].validation_result)

    def test_poll_post_move_staged_missing_file_resets_when_subprocess_never_started(self):
        """Counterpart: when ``import_subprocess_started_at`` is None,
        a missing file at the staged path is just stale residue from a
        crash before subprocess launch. Reset to ``wanted`` so the
        request can be re-searched. This is the recovery path the
        2026-05-04 wedge was missing.
        """
        from lib.download import poll_active_downloads
        from lib.processing_paths import stage_to_ai_path
        import tempfile
        with tempfile.TemporaryDirectory() as tmpdir:
            staging_root = os.path.join(tmpdir, "staging")
            resumed_path = stage_to_ai_path(
                artist="Test Artist",
                title="Test Album",
                staging_dir=staging_root,
                request_id=1,
                auto_import=True,
            )
            os.makedirs(resumed_path)
            # No file present at the bound import path — file is missing.
            row = self._make_downloading_row(state_dict={
                "filetype": "flac",
                "enqueued_at": _utc_now_iso(),
                "processing_started_at": _utc_now_iso(),
                # NO ``import_subprocess_started_at`` — subprocess
                # never launched. This is the legacy wedge shape.
                "current_path": resumed_path,
                "files": [
                    {"username": "user1", "filename": "user1\\Music\\01.flac",
                     "file_dir": "user1\\Music", "size": 30000000},
                ],
            })
            ctx, fake_db = self._make_poll_ctx(downloading_rows=[row], slskd_downloads=[])
            cfg = cast(Any, ctx.cfg)
            cfg.beets_staging_dir = staging_root

            poll_active_downloads(ctx)

            self.assertEqual(
                fake_db.request(1)["status"], "wanted",
                "Subprocess never launched + missing files = stale "
                "crash residue; must reset to wanted, not block forever.",
            )

    def test_poll_no_redownload_window(self):
        """Album stays 'downloading' while queued for importer."""
        from lib.download import poll_active_downloads
        row = self._make_downloading_row()
        ctx, fake_db = self._make_poll_ctx(
            downloading_rows=[row],
            slskd_downloads=[{
                "username": "user1",
                "directories": [{"directory": "user1\\Music", "files": [{
                    "filename": "user1\\Music\\01.flac",
                    "id": "tid-1",
                    "state": "Completed, Succeeded",
                    "bytesTransferred": 30000000,
                }]}],
            }],
        )

        poll_active_downloads(ctx)

        self.assertGreaterEqual(len(fake_db.update_download_state_calls), 1)
        self.assertNotIn((1, "wanted"), fake_db.status_history)
        self.assertEqual(fake_db.request(1)["status"], "downloading")
        self.assertEqual(len(fake_db.list_import_jobs(request_id=1)), 1)

    def test_poll_overlong_album_title_does_not_starve_other_rows(self):
        """ENAMETOOLONG in canonical-path makedirs must not abort the loop.

        Real failure: a Sade row with 240+ char artist + title produced a
        canonical path > ext4's 255-byte component limit. ``os.makedirs``
        raised ``OSError(36)`` which propagated through
        ``_enqueue_completed_processing`` and killed the per-row for-loop
        in ``poll_active_downloads``. Because ``get_downloading()`` orders
        by ``updated_at ASC``, a single poison row starved every later
        row from getting its import job enqueued.
        """
        from lib.download import poll_active_downloads
        long_name = "X" * 250
        poison = self._make_downloading_row(
            request_id=1,
            state_dict={
                "filetype": "flac",
                "enqueued_at": _utc_now_iso(),
                "processing_started_at": _utc_now_iso(),
                "files": [
                    {"username": "user1", "filename": "user1\\Music\\01.flac",
                     "file_dir": "user1\\Music", "size": 30000000,
                     "last_state": "Completed, Succeeded"},
                ],
            },
        )
        poison["artist_name"] = long_name
        poison["album_title"] = long_name

        healthy = self._make_downloading_row(
            request_id=2,
            state_dict={
                "filetype": "flac",
                "enqueued_at": _utc_now_iso(),
                "processing_started_at": _utc_now_iso(),
                "files": [
                    {"username": "user1", "filename": "user1\\Music\\02.flac",
                     "file_dir": "user1\\Music", "size": 30000000,
                     "last_state": "Completed, Succeeded"},
                ],
            },
        )

        ctx, fake_db = self._make_poll_ctx(
            downloading_rows=[poison, healthy], slskd_downloads=[],
        )

        # Must not raise — the poison row's ENAMETOOLONG must be caught
        # inside _materialize_processing_dir (or by the per-row guard).
        poll_active_downloads(ctx)

        self.assertEqual(
            len(fake_db.list_import_jobs(request_id=2)), 1,
            "Healthy row never got an import job — poison row killed the loop",
        )
        self.assertEqual(fake_db.request(1)["status"], "downloading")
        self.assertEqual(len(fake_db.list_import_jobs(request_id=1)), 0)

    def test_poll_continues_after_per_row_unexpected_exception(self):
        """Belt-and-braces: any unhandled per-row exception must not abort the loop.

        Even if a future change reintroduces an uncaught exception in the
        materialize / enqueue path, the per-row guard in
        ``poll_active_downloads`` must contain it so other rows still
        process.
        """
        from lib.download import poll_active_downloads
        from lib import download as download_module

        poison = self._make_downloading_row(
            request_id=1,
            state_dict={
                "filetype": "flac",
                "enqueued_at": _utc_now_iso(),
                "processing_started_at": _utc_now_iso(),
                "files": [
                    {"username": "user1", "filename": "user1\\Music\\01.flac",
                     "file_dir": "user1\\Music", "size": 30000000,
                     "last_state": "Completed, Succeeded"},
                ],
            },
        )
        healthy = self._make_downloading_row(
            request_id=2,
            state_dict={
                "filetype": "flac",
                "enqueued_at": _utc_now_iso(),
                "processing_started_at": _utc_now_iso(),
                "files": [
                    {"username": "user1", "filename": "user1\\Music\\02.flac",
                     "file_dir": "user1\\Music", "size": 30000000,
                     "last_state": "Completed, Succeeded"},
                ],
            },
        )

        ctx, fake_db = self._make_poll_ctx(
            downloading_rows=[poison, healthy], slskd_downloads=[],
        )

        real_enqueue = download_module._enqueue_completed_processing

        def selectively_explode(entry, request_id, state, db, ctx):
            if request_id == 1:
                raise RuntimeError("synthetic kaboom for test")
            return real_enqueue(entry, request_id, state, db, ctx)

        with patch.object(
            download_module,
            "_enqueue_completed_processing",
            side_effect=selectively_explode,
        ):
            poll_active_downloads(ctx)

        self.assertEqual(
            len(fake_db.list_import_jobs(request_id=2)), 1,
            "Per-row exception aborted the loop; healthy row never got "
            "an import job",
        )


class TestBuildActiveDownloadState(unittest.TestCase):
    """Test build_active_download_state() — GrabListEntry → ActiveDownloadState."""

    def test_basic(self):
        from lib.download import build_active_download_state
        from lib.grab_list import GrabListEntry, DownloadFile
        entry = GrabListEntry(
            album_id=1, filetype="flac", title="T", artist="A", year="2020",
            mb_release_id="mbid",
            files=[
                DownloadFile(filename="u\\M\\01.flac", id="tid-1",
                             file_dir="u\\M", username="user1", size=30000000),
            ],
        )
        state = build_active_download_state(entry)
        self.assertEqual(state.filetype, "flac")
        self.assertIsNotNone(state.enqueued_at)
        self.assertEqual(len(state.files), 1)
        self.assertEqual(state.files[0].username, "user1")
        self.assertEqual(state.files[0].filename, "u\\M\\01.flac")
        self.assertEqual(state.files[0].size, 30000000)
        self.assertEqual(state.files[0].retry_count, 0)

    def test_multi_disc(self):
        from lib.download import build_active_download_state
        from lib.grab_list import GrabListEntry, DownloadFile
        entry = GrabListEntry(
            album_id=1, filetype="flac", title="T", artist="A", year="2020",
            mb_release_id="mbid",
            files=[
                DownloadFile(filename="u\\M\\D1-01.flac", id="tid-1",
                             file_dir="u\\M", username="user1", size=30000000,
                             disk_no=1, disk_count=2),
            ],
        )
        state = build_active_download_state(entry)
        self.assertEqual(state.files[0].disk_no, 1)
        self.assertEqual(state.files[0].disk_count, 2)

    def test_persists_retry_count(self):
        from lib.download import build_active_download_state
        from lib.grab_list import GrabListEntry, DownloadFile
        entry = GrabListEntry(
            album_id=1, filetype="flac", title="T", artist="A", year="2020",
            mb_release_id="mbid",
            files=[
                DownloadFile(filename="u\\M\\01.flac", id="tid-1",
                             file_dir="u\\M", username="user1", size=30000000,
                             retry=4),
            ],
        )
        state = build_active_download_state(entry)
        self.assertEqual(state.files[0].retry_count, 4)

    def test_persists_progress_fields(self):
        from lib.download import build_active_download_state
        from lib.grab_list import GrabListEntry, DownloadFile
        entry = GrabListEntry(
            album_id=1, filetype="flac", title="T", artist="A", year="2020",
            mb_release_id="mbid",
            files=[
                DownloadFile(
                    filename="u\\M\\01.flac", id="tid-1",
                    file_dir="u\\M", username="user1", size=30000000,
                    bytes_transferred=2048, last_state="InProgress",
                ),
            ],
        )
        state = build_active_download_state(
            entry,
            enqueued_at="2026-04-03T12:00:00+00:00",
            last_progress_at="2026-04-03T12:05:00+00:00",
        )
        self.assertEqual(state.last_progress_at, "2026-04-03T12:05:00+00:00")
        self.assertEqual(state.files[0].bytes_transferred, 2048)
        self.assertEqual(state.files[0].last_state, "InProgress")

    def test_enqueued_at_is_utc_iso(self):
        from lib.download import build_active_download_state
        from lib.grab_list import GrabListEntry, DownloadFile
        from datetime import datetime as dt, timezone as tz
        entry = GrabListEntry(
            album_id=1, filetype="flac", title="T", artist="A", year="2020",
            mb_release_id="mbid", files=[
                DownloadFile(filename="u\\M\\01.flac", id="tid-1",
                             file_dir="u\\M", username="user1", size=1000),
            ],
        )
        state = build_active_download_state(entry)
        parsed = dt.fromisoformat(state.enqueued_at)
        self.assertEqual(parsed.tzinfo, tz.utc)
        self.assertEqual(state.last_progress_at, state.enqueued_at)

    def test_uses_import_folder_as_current_path(self):
        from lib.download import build_active_download_state
        from lib.grab_list import GrabListEntry, DownloadFile
        entry = GrabListEntry(
            album_id=1,
            filetype="flac",
            title="T",
            artist="A",
            year="2020",
            mb_release_id="mbid",
            import_folder="/tmp/staged/A/T",
            files=[
                DownloadFile(
                    filename="u\\M\\01.flac", id="tid-1",
                    file_dir="u\\M", username="user1", size=1000,
                ),
            ],
        )
        state = build_active_download_state(entry)
        self.assertEqual(state.current_path, "/tmp/staged/A/T")


class TestReconstructGrabListEntry(unittest.TestCase):
    """Test reconstruct_grab_list_entry() — rebuild GrabListEntry from DB row + state."""

    def test_reconstruct_basic(self):
        from lib.download import reconstruct_grab_list_entry
        from lib.quality import ActiveDownloadState, ActiveDownloadFileState
        state = ActiveDownloadState(
            filetype="flac",
            enqueued_at="2026-04-03T12:00:00+00:00",
            files=[
                ActiveDownloadFileState(
                    username="user1", filename="user1\\Music\\01.flac",
                    file_dir="user1\\Music", size=30000000,
                ),
            ],
        )
        request = {
            "id": 42,
            "album_title": "Test Album",
            "artist_name": "Test Artist",
            "year": 2020,
            "mb_release_id": "test-mbid",
            "source": "request",
            "search_filetype_override": None,
            "target_format": None,
        }
        entry = reconstruct_grab_list_entry(request, state)
        self.assertEqual(entry.album_id, 42)
        self.assertEqual(entry.title, "Test Album")
        self.assertEqual(entry.artist, "Test Artist")
        self.assertEqual(entry.year, "2020")
        self.assertEqual(entry.filetype, "flac")
        self.assertEqual(entry.mb_release_id, "test-mbid")
        self.assertEqual(entry.db_request_id, 42)
        self.assertEqual(entry.db_source, "request")
        self.assertEqual(len(entry.files), 1)
        self.assertEqual(entry.files[0].filename, "user1\\Music\\01.flac")
        self.assertEqual(entry.files[0].id, "")  # Must be re-derived
        self.assertEqual(entry.files[0].retry, 0)

    def test_reconstruct_multi_disc(self):
        from lib.download import reconstruct_grab_list_entry
        from lib.quality import ActiveDownloadState, ActiveDownloadFileState
        state = ActiveDownloadState(
            filetype="flac",
            enqueued_at="2026-04-03T12:00:00+00:00",
            files=[
                ActiveDownloadFileState(
                    username="user1", filename="user1\\Music\\D1-01.flac",
                    file_dir="user1\\Music", size=30000000,
                    disk_no=1, disk_count=2,
                ),
                ActiveDownloadFileState(
                    username="user1", filename="user1\\Music\\D2-01.flac",
                    file_dir="user1\\Music", size=25000000,
                    disk_no=2, disk_count=2,
                ),
            ],
        )
        request = {"id": 10, "album_title": "B", "artist_name": "A",
                   "year": 2020, "mb_release_id": "mbid", "source": "request",
                   "search_filetype_override": None, "target_format": None}
        entry = reconstruct_grab_list_entry(request, state)
        self.assertEqual(entry.files[0].disk_no, 1)
        self.assertEqual(entry.files[0].disk_count, 2)
        self.assertEqual(entry.files[1].disk_no, 2)

    def test_reconstruct_search_filetype_override(self):
        from lib.download import reconstruct_grab_list_entry
        from lib.quality import ActiveDownloadState
        state = ActiveDownloadState(filetype="flac", enqueued_at="now", files=[])
        request = {"id": 10, "album_title": "B", "artist_name": "A",
                   "year": 2020, "mb_release_id": "mbid", "source": "request",
                   "search_filetype_override": "flac", "target_format": None}
        entry = reconstruct_grab_list_entry(request, state)
        self.assertEqual(entry.db_search_filetype_override, "flac")

    def test_reconstruct_retry_count(self):
        from lib.download import reconstruct_grab_list_entry
        from lib.quality import ActiveDownloadState, ActiveDownloadFileState
        state = ActiveDownloadState(
            filetype="flac",
            enqueued_at="now",
            files=[
                ActiveDownloadFileState(
                    username="user1", filename="user1\\Music\\01.flac",
                    file_dir="user1\\Music", size=30000000, retry_count=5,
                ),
            ],
        )
        request = {"id": 10, "album_title": "B", "artist_name": "A",
                   "year": 2020, "mb_release_id": "mbid", "source": "request",
                   "search_filetype_override": None, "target_format": None}
        entry = reconstruct_grab_list_entry(request, state)
        self.assertEqual(entry.files[0].retry, 5)

    def test_reconstruct_progress_fields(self):
        from lib.download import reconstruct_grab_list_entry
        from lib.quality import ActiveDownloadState, ActiveDownloadFileState
        state = ActiveDownloadState(
            filetype="flac",
            enqueued_at="now",
            last_progress_at="2026-04-03T12:05:00+00:00",
            files=[
                ActiveDownloadFileState(
                    username="user1",
                    filename="user1\\Music\\01.flac",
                    file_dir="user1\\Music",
                    size=30000000,
                    bytes_transferred=4096,
                    last_state="InProgress",
                ),
            ],
        )
        request = {"id": 10, "album_title": "B", "artist_name": "A",
                   "year": 2020, "mb_release_id": "mbid", "source": "request",
                   "search_filetype_override": None, "target_format": None}
        entry = reconstruct_grab_list_entry(request, state)
        self.assertEqual(entry.files[0].bytes_transferred, 4096)
        self.assertEqual(entry.files[0].last_state, "InProgress")
        self.assertIsNone(entry.files[0].status)

    def test_reconstruct_restores_terminal_status_from_persisted_state(self):
        """Once slskd reports a terminal state, later snapshot gaps must not
        erase that evidence."""
        from lib.download import reconstruct_grab_list_entry
        from lib.quality import ActiveDownloadState, ActiveDownloadFileState
        state = ActiveDownloadState(
            filetype="m4a",
            enqueued_at="now",
            files=[
                ActiveDownloadFileState(
                    username="elgoognplus",
                    filename="Music\\78 Saab\\Crossed Lines\\01 No Illusions.m4a",
                    file_dir="Music\\78 Saab\\Crossed Lines",
                    size=26799968,
                    bytes_transferred=0,
                    last_state="Completed, Rejected",
                ),
            ],
        )
        request = {"id": 10, "album_title": "Crossed Lines",
                   "artist_name": "78 Saab", "year": 2004,
                   "mb_release_id": "mbid", "source": "request",
                   "search_filetype_override": None, "target_format": None}

        entry = reconstruct_grab_list_entry(request, state)

        self.assertEqual(
            entry.files[0].status,
            TransferSnapshot(state="Completed, Rejected", bytes_transferred=0),
        )

    def test_reconstruct_missing_year(self):
        from lib.download import reconstruct_grab_list_entry
        from lib.quality import ActiveDownloadState
        state = ActiveDownloadState(filetype="flac", enqueued_at="now", files=[])
        request = {"id": 10, "album_title": "B", "artist_name": "A",
                   "year": None, "mb_release_id": "mbid", "source": "request",
                   "search_filetype_override": None, "target_format": None}
        entry = reconstruct_grab_list_entry(request, state)
        self.assertEqual(entry.year, "")

    def test_reconstruct_current_path_to_import_folder(self):
        from lib.download import reconstruct_grab_list_entry
        from lib.quality import ActiveDownloadState
        state = ActiveDownloadState(
            filetype="flac",
            enqueued_at="now",
            current_path="/tmp/staged/A/B",
            files=[],
        )
        request = {"id": 10, "album_title": "B", "artist_name": "A",
                   "year": 2020, "mb_release_id": "mbid", "source": "request",
                   "search_filetype_override": None, "target_format": None}
        entry = reconstruct_grab_list_entry(request, state)
        self.assertEqual(entry.import_folder, "/tmp/staged/A/B")

    def test_reconstruct_fallback_current_path(self):
        from lib.download import reconstruct_grab_list_entry
        from lib.quality import ActiveDownloadState
        state = ActiveDownloadState(
            filetype="flac",
            enqueued_at="now",
            current_path="/tmp/legacy/A/B",
            files=[],
        )
        request = {"id": 10, "album_title": "B", "artist_name": "A",
                   "year": 2020, "mb_release_id": "mbid", "source": "request",
                   "search_filetype_override": None, "target_format": None}
        entry = reconstruct_grab_list_entry(request, state)
        self.assertEqual(entry.import_folder, "/tmp/legacy/A/B")


# ============================================================================
# _compute_rejection_backfill — orchestration test for cfg threading
# ============================================================================
#
# Pins that ctx.cfg.quality_ranks actually reaches rejection_backfill_override.
# Pure-function tests in test_quality_decisions.py cover the decision logic
# itself; this test guards the wiring layer between download.py and
# lib/quality/ so a future refactor can't silently drop the cfg argument.

class TestComputeRejectionBackfillCfgThreading(unittest.TestCase):
    """_compute_rejection_backfill must thread ctx.cfg.quality_ranks through
    to rejection_backfill_override."""

    def _setup(self, *, gate_min_rank, on_disk_min_bitrate=180):
        """Build the fixtures: fake DB with a genuine non-lossless request,
        a ctx with the requested gate_min_rank, and a mock BeetsDB returning
        an MP3 VBR album at on_disk_min_bitrate."""
        from lib.quality import QualityRankConfig
        from lib.beets_db import AlbumInfo

        fake_db = FakePipelineDB()
        fake_db.seed_request(make_request_row(
            id=42,
            mb_release_id="mbid-test",
            current_spectral_grade="genuine",
            verified_lossless=False,
            search_filetype_override=None,
        ))

        # Real CratediggerConfig is heavy; a SimpleNamespace with quality_ranks
        # is enough — _compute_rejection_backfill only reads ctx.cfg.quality_ranks.
        from types import SimpleNamespace
        cfg = SimpleNamespace(
            quality_ranks=QualityRankConfig(gate_min_rank=gate_min_rank),
        )
        ctx = make_ctx_with_fake_db(fake_db, cfg=cfg)

        album_data = make_grab_list_entry(
            db_request_id=42,
            db_search_filetype_override=None,
            mb_release_id="mbid-test",
        )

        beets_info = AlbumInfo(
            album_id=1, track_count=10,
            min_bitrate_kbps=on_disk_min_bitrate,
            avg_bitrate_kbps=on_disk_min_bitrate,
            format="MP3", is_cbr=False,
            album_path="/Beets/A/B",
        )
        return album_data, ctx, beets_info

    def _run(self, album_data, ctx, beets_info):
        from lib.download_processing import _compute_rejection_backfill
        with patch("lib.beets_db.BeetsDB") as mock_beets_cls:
            mock_beets = MagicMock()
            mock_beets.__enter__ = MagicMock(return_value=mock_beets)
            mock_beets.__exit__ = MagicMock(return_value=False)
            mock_beets.get_album_info.return_value = beets_info
            mock_beets_cls.return_value = mock_beets
            return _compute_rejection_backfill(album_data, ctx)

    def test_lenient_gate_min_rank_fires_backfill(self):
        """gate_min_rank=GOOD: 180kbps VBR genuine → GOOD rank → backfill fires."""
        from lib.quality import QualityRank, QUALITY_LOSSLESS
        album_data, ctx, beets_info = self._setup(
            gate_min_rank=QualityRank.GOOD)
        result = self._run(album_data, ctx, beets_info)
        self.assertEqual(result, QUALITY_LOSSLESS,
                         "lenient cfg must reach rejection_backfill_override")

    def test_default_gate_min_rank_blocks_backfill(self):
        """Default gate_min_rank=EXCELLENT: same 180kbps blocks (GOOD < EXCELLENT)."""
        from lib.quality import QualityRank
        album_data, ctx, beets_info = self._setup(
            gate_min_rank=QualityRank.EXCELLENT)
        result = self._run(album_data, ctx, beets_info)
        self.assertIsNone(result,
                          "strict cfg must also reach rejection_backfill_override "
                          "— if cfg threading were broken, both branches would "
                          "use the same default and this assertion would silently pass")


if TYPE_CHECKING:
    from lib.download import DownloadDB as _DownloadDB
    from lib.download_ownership import DownloadOwnershipDB as _OwnershipDB
    from lib.pipeline_db import PipelineDB

    # Static parity proof (#409) — see the matching block in
    # tests/test_wrong_match_cleanup_service.py for the rationale.
    _pipeline_db_satisfies_download_protocol: _DownloadDB = cast("PipelineDB", None)
    _fake_db_satisfies_download_protocol: _DownloadDB = cast("FakePipelineDB", None)
    _pipeline_db_satisfies_ownership_protocol: _OwnershipDB = cast("PipelineDB", None)
    _fake_db_satisfies_ownership_protocol: _OwnershipDB = cast("FakePipelineDB", None)


class TestDownloadDBProtocolParity(unittest.TestCase):
    """#409: both impls must satisfy DownloadDB and DownloadOwnershipDB."""

    def test_pipeline_db_satisfies_protocols(self) -> None:
        from lib.download import DownloadDB
        from lib.download_ownership import DownloadOwnershipDB
        from lib.pipeline_db import PipelineDB

        self.assertTrue(issubclass(PipelineDB, DownloadDB))
        self.assertTrue(issubclass(PipelineDB, DownloadOwnershipDB))

    def test_fake_pipeline_db_satisfies_protocols(self) -> None:
        from lib.download import DownloadDB
        from lib.download_ownership import DownloadOwnershipDB

        self.assertTrue(issubclass(FakePipelineDB, DownloadDB))
        self.assertTrue(issubclass(FakePipelineDB, DownloadOwnershipDB))

    def test_protocols_extend_transitions_protocol(self) -> None:
        """Both forward their handle into transitions.finalize_request."""
        from lib.download import DownloadDB
        from lib.download_ownership import DownloadOwnershipDB
        from lib.transitions import TransitionsDB

        self.assertTrue(issubclass(DownloadDB, TransitionsDB))
        self.assertTrue(issubclass(DownloadOwnershipDB, TransitionsDB))


class TestConvergeSlskdOrphans(unittest.TestCase):
    """#278 Phase 0 convergence: cancel live transfers no downloading row owns."""

    OWNED_FILE = "Music\\Owned\\01.flac"
    ORPHAN_FILE = "Music\\Orphan\\01.flac"

    def _make_ctx(self, slskd, rows=()):
        fake_db = FakePipelineDB()
        for row in rows:
            fake_db.seed_request(row)
        return make_ctx_with_fake_db(fake_db, slskd=slskd)

    def _seed_slskd(self):
        slskd = FakeSlskdAPI()
        slskd.add_transfer(username="peer1", directory="Music\\Owned",
                           filename=self.OWNED_FILE, id="t-owned",
                           state="InProgress")
        slskd.add_transfer(username="peer2", directory="Music\\Orphan",
                           filename=self.ORPHAN_FILE, id="t-orphan",
                           state="Queued, Remotely")
        slskd.add_transfer(username="peer3", directory="Music\\Done",
                           filename="Music\\Done\\01.flac", id="t-done",
                           state="Completed, Succeeded")
        return slskd

    def _owning_row(self):
        return make_request_row(
            id=1, status="downloading",
            active_download_state={
                "filetype": "flac",
                "files": [{"username": "peer1",
                           "filename": self.OWNED_FILE}]})

    def test_cancels_only_live_unowned_transfers(self):
        from lib.slskd_transfers import converge_slskd_orphans
        slskd = self._seed_slskd()
        ctx = self._make_ctx(slskd, rows=[self._owning_row()])

        cancelled = converge_slskd_orphans(ctx)

        self.assertEqual(cancelled, 1)
        calls = slskd.transfers.cancel_download_calls
        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0].username, "peer2")
        self.assertEqual(calls[0].id, "t-orphan")

    def test_snapshot_fetch_excludes_removed_transfers(self):
        """#479 item 3: convergence only needs live transfers — trim the
        payload by requesting includeRemoved=False (it filters
        Completed* itself, so removed/terminal history is dead weight)."""
        from lib.slskd_transfers import converge_slskd_orphans
        slskd = self._seed_slskd()
        ctx = self._make_ctx(slskd, rows=[self._owning_row()])

        converge_slskd_orphans(ctx)

        self.assertEqual(slskd.transfers.get_all_downloads_calls, [False])

    def test_no_downloading_rows_cancels_stranded_transfer(self):
        """The Replace scenario: zero downloading rows, one live transfer."""
        from lib.slskd_transfers import converge_slskd_orphans
        slskd = self._seed_slskd()
        ctx = self._make_ctx(slskd, rows=[])

        cancelled = converge_slskd_orphans(ctx)

        self.assertEqual(cancelled, 2)
        cancelled_ids = {c.id for c in slskd.transfers.cancel_download_calls}
        self.assertEqual(cancelled_ids, {"t-owned", "t-orphan"})

    def test_snapshot_failure_cancels_nothing(self):
        from lib.slskd_transfers import converge_slskd_orphans
        slskd = self._seed_slskd()
        slskd.transfers.get_all_downloads_error = RuntimeError("slskd down")
        ctx = self._make_ctx(slskd, rows=[])

        cancelled = converge_slskd_orphans(ctx)

        self.assertEqual(cancelled, 0)
        self.assertEqual(slskd.transfers.cancel_download_calls, [])

    def test_cancel_error_does_not_abort_remaining_orphans(self):
        from lib.slskd_transfers import converge_slskd_orphans
        slskd = self._seed_slskd()
        slskd.transfers.cancel_download_error = RuntimeError("cancel failed")
        ctx = self._make_ctx(slskd, rows=[])

        cancelled = converge_slskd_orphans(ctx)

        self.assertEqual(cancelled, 0)
        # Both live orphans were still attempted despite the first failure.
        self.assertEqual(len(slskd.transfers.cancel_download_calls), 2)

    def test_clean_state_is_a_noop(self):
        from lib.slskd_transfers import converge_slskd_orphans
        slskd = FakeSlskdAPI()
        slskd.add_transfer(username="peer1", directory="Music\\Owned",
                           filename=self.OWNED_FILE, id="t-owned",
                           state="InProgress")
        ctx = self._make_ctx(slskd, rows=[self._owning_row()])

        cancelled = converge_slskd_orphans(ctx)

        self.assertEqual(cancelled, 0)
        self.assertEqual(slskd.transfers.cancel_download_calls, [])


if __name__ == "__main__":
    unittest.main()
