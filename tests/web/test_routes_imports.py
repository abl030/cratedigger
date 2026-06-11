#!/usr/bin/env python3
"""Contract tests for web/routes/imports.py: manual import + wrong matches.

Split from tests/test_web_server.py (#408). Shared harness in
tests/web/_harness.py.
"""

import copy
from datetime import datetime, timezone
import json
import os
import sys
import tempfile
import unittest
from unittest.mock import patch
from urllib.request import urlopen, Request
from urllib.error import HTTPError


sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from tests.web._harness import (
    _MOCK_PIPELINE_REQUEST,
    _DEFAULT_WRONG_MATCH_ROW,
    _DEFAULT_WRONG_MATCH_ENTRY,
    _assert_required_fields,
    _WebServerCase,
    _fresh_triage_runner,
    _make_server,
)

from lib.manual_import import FolderInfo
from lib.import_queue import ImportJob
from tests.helpers import make_request_row


class TestManualImportRouteContracts(_WebServerCase):
    """Contract tests for manual import routes."""

    FOLDER_REQUIRED_FIELDS = {"name", "path", "artist", "album", "file_count", "match"}
    MATCH_REQUIRED_FIELDS = {"request_id", "artist", "album", "mb_release_id", "score"}
    IMPORT_REQUIRED_FIELDS = {"status", "message", "request_id", "artist", "album"}

    def setUp(self) -> None:
        self.mock_db.get_request.return_value = _MOCK_PIPELINE_REQUEST
        self.mock_db.get_by_status.side_effect = None

    @patch("web.routes.imports.scan_complete_folder")
    def test_manual_import_scan_contract(self, mock_scan):
        # Drive the real ``match_folders_to_requests`` against folder +
        # request inputs that share an artist/album token set so the
        # fuzzy matcher produces a high-confidence FolderMatch. Patching
        # the matcher away would hide its contract — score field,
        # match-or-skip threshold, sort order — from this test.
        folder = FolderInfo(
            name="Test Artist - Test Album",
            path="/complete/Test Artist - Test Album",
            artist="Test Artist",
            album="Test Album",
            file_count=10,
        )
        mock_scan.return_value = [folder]
        self.mock_db.get_by_status.return_value = [
            make_request_row(
                id=100,
                status="wanted",
                mb_release_id="abc-123",
                artist_name="Test Artist",
                album_title="Test Album",
            ),
        ]

        status, data = self._get("/api/manual-import/scan")

        self.assertEqual(status, 200)
        _assert_required_fields(self, data, {"folders", "wanted_count"},
                                "manual import scan response")
        _assert_required_fields(self, data["folders"][0], self.FOLDER_REQUIRED_FIELDS,
                                "manual import folder")
        _assert_required_fields(self, data["folders"][0]["match"], self.MATCH_REQUIRED_FIELDS,
                                "manual import match")
        # Real matcher should report a perfect score for identical
        # artist/album tokens — this is the actual production contract.
        self.assertEqual(data["folders"][0]["match"]["request_id"], 100)
        self.assertGreaterEqual(data["folders"][0]["match"]["score"], 0.99)

    @patch("web.routes.imports.resolve_failed_path",
           return_value="/complete/Test Artist - Test Album")
    def test_manual_import_post_contract(self, _mock_resolve):
        status, data = self._post(
            "/api/manual-import/import",
            {"request_id": 100, "path": "/complete/Test Artist - Test Album"},
        )

        self.assertEqual(status, 202)
        _assert_required_fields(self, data, self.IMPORT_REQUIRED_FIELDS,
                                "manual import response")


class TestWrongMatchesContract(unittest.TestCase):
    """Contract tests: /api/wrong-matches returns grouped-by-release shape.

    Issue #113: every rejection with a failed_path must be reachable. The
    route returns ``{groups: [{request_id, artist, album, mb_release_id,
    in_library, pending_count, entries: [...]}]}`` so the frontend can
    collapse by release and expand to per-candidate actions.
    """

    @classmethod
    def setUpClass(cls):
        cls.server, cls.port, cls.mock_db = _make_server()
        cls.base = f"http://127.0.0.1:{cls.port}"

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()

    def _get(self, path: str) -> tuple[int, dict]:
        url = f"{self.base}{path}"
        try:
            resp = urlopen(url)
            return resp.status, json.loads(resp.read())
        except HTTPError as e:
            return e.code, json.loads(e.read())

    def _post(self, path: str, body: dict) -> tuple[int, dict]:
        url = f"{self.base}{path}"
        data = json.dumps(body).encode()
        req = Request(url, data=data, headers={"Content-Type": "application/json"})
        try:
            resp = urlopen(req)
            return resp.status, json.loads(resp.read())
        except HTTPError as e:
            return e.code, json.loads(e.read())

    def setUp(self) -> None:
        self.mock_db.get_request.return_value = copy.deepcopy(_MOCK_PIPELINE_REQUEST)
        self.mock_db.get_wrong_matches.side_effect = None
        self.mock_db.get_wrong_matches.return_value = [copy.deepcopy(_DEFAULT_WRONG_MATCH_ROW)]
        self.mock_db.get_download_log_entry.reset_mock()
        self.mock_db.get_download_log_entry.side_effect = None
        self.mock_db.get_download_log_entry.return_value = copy.deepcopy(_DEFAULT_WRONG_MATCH_ENTRY)
        self.mock_db.clear_wrong_match_path.reset_mock()
        self.mock_db.clear_wrong_match_path.return_value = True
        self.mock_db.clear_wrong_match_paths.reset_mock()
        self.mock_db.clear_wrong_match_paths.return_value = 1
        self.mock_db.enqueue_import_job.reset_mock()
        self.mock_db.enqueue_import_job.side_effect = None
        self.mock_db.enqueue_import_job.return_value = self._job(
            77, 100, 42, "/mnt/virtio/music/slskd/failed_imports/Test")
        self.mock_db.get_download_history_batch.return_value = {}
        self.mock_db.list_active_import_jobs_for_wrong_match.return_value = []
        # Default: treat every failed_path as existing so the group survives
        # filtering. Individual tests override this to exercise missing-file
        # and mixed-existence cases. Converge deletion is service-backed.
        # Regression-guard sentinel: import cleanup_wrong_match into the route
        # module's namespace so the converge tests can assert it is NOT called.
        # See project_converge_operator_authority memory + post_wrong_match_converge
        # docstring — converge must route deletion through delete_wrong_match,
        # never through cleanup_wrong_match.
        from lib.wrong_match_cleanup_service import cleanup_wrong_match as _cwm_sentinel
        import web.routes.imports as _imports_mod
        _imports_mod.cleanup_wrong_match = _cwm_sentinel  # pyright: ignore[reportAttributeAccessIssue]
        cleanup_patch = patch(
            "web.routes.imports.cleanup_wrong_match",
            side_effect=lambda _db, lid: self._cleanup_result(lid),
        )
        manual_cleanup_patch = patch(
            "web.routes.imports.delete_wrong_match",
            side_effect=lambda _db, lid, **_kwargs: self._manual_cleanup_result(lid),
        )
        manual_group_cleanup_patch = patch(
            "web.routes.imports.delete_wrong_match_group",
            side_effect=lambda _db, rid: self._manual_group_cleanup_result(rid),
        )
        resolve_patch = patch("web.routes.imports.resolve_failed_path",
                              side_effect=lambda p: p if p else None)
        self.mock_cleanup = cleanup_patch.start()
        self.mock_manual_cleanup = manual_cleanup_patch.start()
        self.mock_manual_group_cleanup = manual_group_cleanup_patch.start()
        self.mock_resolve_failed_path = resolve_patch.start()
        self.addCleanup(cleanup_patch.stop)
        self.addCleanup(manual_cleanup_patch.stop)
        self.addCleanup(manual_group_cleanup_patch.stop)
        self.addCleanup(resolve_patch.stop)
        self.addCleanup(lambda: delattr(_imports_mod, "cleanup_wrong_match"))

    GROUP_REQUIRED_FIELDS = {
        "request_id", "artist", "album", "mb_release_id",
        # Release-group id surfaces so the frontend can render the
        # Replace button (R7) — it asks "what RG is this row in?".
        "mb_release_group_id",
        "in_library", "pending_count", "entries",
        # Quality summary for the collapsed card (issue: "show quality on disk").
        "status", "min_bitrate", "format", "verified_lossless",
        "current_spectral_grade", "current_spectral_bitrate",
        "quality_label", "quality_rank",
        # Summary of the last successful import for the request — tells the
        # user what's actually on disk, not the most recent attempt.
        "latest_import",
    }
    ENTRY_REQUIRED_FIELDS = {
        "download_log_id", "soulseek_username", "failed_path", "files_exist",
        "distance", "scenario", "detail", "source_dirs", "candidate", "local_items",
        # Per-candidate stored evidence (R1+R2 of the spectral-evidence
        # plan) — surfaced from download_log so the operator can eyeball
        # candidates by audio quality. Always present in the payload;
        # values are None when the underlying row lacks evidence.
        "spectral_grade", "spectral_bitrate",
        "v0_probe_kind", "v0_probe_avg_bitrate",
        # Storage format + min bitrate + computed quality rank — read
        # from album_quality_evidence via download_log.candidate_evidence_id
        # so wrong-match rows show their actual codec/rank instead of
        # dashes from the legacy denorm columns. Drives entry sort order.
        "format", "min_bitrate", "verified_lossless", "quality_rank",
    }
    DELETE_RESULT_REQUIRED_FIELDS = {
        "status", "download_log_id", "outcome", "success", "request_id",
        "entry_found", "visible", "raw_failed_path", "failed_path_hint",
        "resolved_path", "deleted_path", "path_missing", "cleared_rows",
        "skipped", "reason", "error",
    }
    DELETE_GROUP_REQUIRED_FIELDS = {
        "status", "request_id", "outcome", "success", "processed", "deleted",
        "deleted_paths", "cleared", "skipped", "errors", "remaining",
        "group_empty", "results",
    }

    GROUP_FIELD_TYPES = {
        "request_id": int,
        "artist": str,
        "album": str,
        "in_library": bool,
        "pending_count": int,
        "entries": list,
        "status": str,
        "verified_lossless": bool,
    }
    ENTRY_FIELD_TYPES = {
        "download_log_id": int,
        "failed_path": str,
        "files_exist": bool,
        "distance": (int, float, type(None)),
        "source_dirs": list,
    }

    def _row(self, download_log_id: int, request_id: int, username: str,
             failed_path: str, artist: str = "Test Artist",
             album: str = "Test Album",
             mb_release_id: str | None = "abc-123",
             scenario: str = "high_distance",
             distance: float = 0.25) -> dict:
        row = copy.deepcopy(_DEFAULT_WRONG_MATCH_ROW)
        row["download_log_id"] = download_log_id
        row["request_id"] = request_id
        row["artist_name"] = artist
        row["album_title"] = album
        row["mb_release_id"] = mb_release_id
        row["soulseek_username"] = username
        row["validation_result"]["failed_path"] = failed_path
        row["validation_result"]["scenario"] = scenario
        row["validation_result"]["distance"] = distance
        row["validation_result"]["candidates"][0]["distance"] = distance
        return row

    def _entry(self, download_log_id: int, request_id: int,
               failed_path: str) -> dict:
        return {
            "id": download_log_id,
            "request_id": request_id,
            "validation_result": {
                "failed_path": failed_path,
                "scenario": "high_distance",
            },
        }

    def _job(self, job_id: int, request_id: int, download_log_id: int,
             failed_path: str, *, deduped: bool = False) -> ImportJob:
        return ImportJob(
            id=job_id,
            job_type="force_import",
            status="queued",
            request_id=request_id,
            dedupe_key=f"force_import:download_log:{download_log_id}",
            payload={
                "download_log_id": download_log_id,
                "failed_path": failed_path,
            },
            result=None,
            message="Import queued",
            error=None,
            attempts=0,
            worker_id=None,
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc),
            started_at=None,
            heartbeat_at=None,
            completed_at=None,
            deduped=deduped,
        )

    def _cleanup_result(self, log_id: int, *, outcome: str = "deleted"):
        from lib.wrong_match_cleanup_service import (
            OUTCOME_DELETED,
            WrongMatchCleanupOutcome,
        )

        return WrongMatchCleanupOutcome(
            download_log_id=log_id,
            outcome=outcome,
            success=outcome == OUTCOME_DELETED,
            verdict="confident_reject" if outcome == OUTCOME_DELETED else "uncertain",
            cleanup_eligible=outcome == OUTCOME_DELETED,
            cleared_rows=1 if outcome == OUTCOME_DELETED else 0,
        )

    def _manual_cleanup_result(self, log_id: int):
        from lib.wrong_match_delete_service import (
            OUTCOME_DELETED,
            WrongMatchDeleteResult,
        )

        return WrongMatchDeleteResult(
            download_log_id=log_id,
            outcome=OUTCOME_DELETED,
            success=True,
            entry_found=True,
            visible=True,
            request_id=42,
            raw_failed_path="/mnt/virtio/music/slskd/failed_imports/Test",
            resolved_path="/mnt/virtio/music/slskd/failed_imports/Test",
            deleted_path="/mnt/virtio/music/slskd/failed_imports/Test",
            cleared_rows=1,
        )

    def _manual_group_cleanup_result(self, request_id: int):
        from lib.wrong_match_delete_service import WrongMatchDeleteSummary

        results = (
            self._manual_cleanup_result(100),
            self._manual_cleanup_result(101),
        )
        return WrongMatchDeleteSummary(
            request_id=request_id,
            outcome="deleted",
            success=True,
            processed=2,
            deleted=2,
            deleted_paths=2,
            cleared=2,
            skipped=0,
            errors=0,
            remaining=0,
            group_empty=True,
            results=results,
        )

    def test_response_has_groups(self):
        """RED for issue #113: payload must be {groups: [...]}, not {entries: [...]}."""
        status, data = self._get("/api/wrong-matches")
        self.assertEqual(status, 200)
        self.assertIn("groups", data,
                      "Response must expose a `groups` array keyed by release.")

    def test_group_has_required_fields_and_types(self):
        status, data = self._get("/api/wrong-matches")
        self.assertGreater(len(data["groups"]), 0)
        for group in data["groups"]:
            _assert_required_fields(
                self, group, self.GROUP_REQUIRED_FIELDS,
                f"group request={group.get('request_id')}")
            for field, expected_type in self.GROUP_FIELD_TYPES.items():
                self.assertIsInstance(
                    group[field], expected_type,
                    f"group.{field}={group[field]!r} should be {expected_type}")

    def test_entry_has_required_fields_and_types(self):
        status, data = self._get("/api/wrong-matches")
        for group in data["groups"]:
            self.assertGreater(len(group["entries"]), 0)
            for entry in group["entries"]:
                _assert_required_fields(
                    self, entry, self.ENTRY_REQUIRED_FIELDS,
                    f"entry dl_id={entry.get('download_log_id')}")
                for field, expected_type in self.ENTRY_FIELD_TYPES.items():
                    self.assertIsInstance(
                        entry[field], expected_type,
                        f"entry.{field}={entry[field]!r} should be {expected_type}")

    def test_entry_surfaces_stored_spectral_and_v0_probe_evidence(self):
        """Covers AE1 — per-candidate stored evidence reaches the row payload.

        Plumbs the four per-attempt download_log columns
        (spectral_grade/spectral_bitrate/v0_probe_kind/v0_probe_avg_bitrate)
        from get_wrong_matches() through to the entry dict so the operator
        can eyeball candidates by audio quality.
        """
        row = copy.deepcopy(_DEFAULT_WRONG_MATCH_ROW)
        row["spectral_grade"] = "suspect"
        row["spectral_bitrate"] = 320
        row["v0_probe_kind"] = "lossless_source_v0"
        row["v0_probe_avg_bitrate"] = 265
        self.mock_db.get_wrong_matches.return_value = [row]

        _, data = self._get("/api/wrong-matches")
        entry = data["groups"][0]["entries"][0]
        self.assertEqual(entry["spectral_grade"], "suspect")
        self.assertEqual(entry["spectral_bitrate"], 320)
        self.assertEqual(entry["v0_probe_kind"], "lossless_source_v0")
        self.assertEqual(entry["v0_probe_avg_bitrate"], 265)

    def test_entry_surfaces_preserved_source_dirs(self):
        row = copy.deepcopy(_DEFAULT_WRONG_MATCH_ROW)
        row["validation_result"]["source_dirs"] = [
            "baduser\\Artist\\Album",
            "baduser\\Artist\\Album\\CD2",
        ]
        self.mock_db.get_wrong_matches.return_value = [row]

        _, data = self._get("/api/wrong-matches")
        entry = data["groups"][0]["entries"][0]
        self.assertEqual(
            entry["source_dirs"],
            ["baduser\\Artist\\Album", "baduser\\Artist\\Album\\CD2"],
        )

    def test_wrong_match_explorer_lists_audio_files_and_source_dirs(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            track_path = os.path.join(tmpdir, "01 - Track.mp3")
            with open(track_path, "wb") as handle:
                handle.write(b"fake mp3 bytes")

            self.mock_db.get_download_log_entry.return_value = {
                "id": 42,
                "request_id": 100,
                "validation_result": {
                    "failed_path": tmpdir,
                    "source_dirs": ["baduser\\Artist\\Album"],
                },
            }

            status, data = self._get("/api/wrong-matches/explorer?download_log_id=42")

        self.assertEqual(status, 200)
        self.assertEqual(data["status"], "ok")
        self.assertEqual(data["source_dirs"], ["baduser\\Artist\\Album"])
        self.assertEqual(data["audio_file_count"], 1)
        self.assertEqual(data["files"][0]["relative_path"], "01 - Track.mp3")
        self.assertTrue(data["files"][0]["playable"])
        self.assertIn("/api/wrong-matches/audio?download_log_id=42", data["files"][0]["stream_url"])

    def test_wrong_match_explorer_normalizes_raw_id3_tags_and_skips_artwork_frames(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            track_path = os.path.join(tmpdir, "01 - Track.mp3")
            with open(track_path, "wb") as handle:
                handle.write(b"fake mp3 bytes")

            self.mock_db.get_download_log_entry.return_value = {
                "id": 42,
                "request_id": 100,
                "validation_result": {
                    "failed_path": tmpdir,
                },
            }

            class _FakeInfo:
                length = 181.0
                bitrate = 320000

            class _FakeAudio:
                tags = {
                    "APIC:": ["embedded cover art"],
                    "TALB": ["Shut Up And Listen To Majosha"],
                    "TCON": ["Funk Rock"],
                    "TDRC": ["1989"],
                    "TPE1": ["Majosha"],
                    "TPE2": ["Majosha"],
                    "TPOS": ["2"],
                    "TXXX:MusicBrainz Album Id": ["20f1e791-34cd-4b47-8783-51492b90218a"],
                }
                info = _FakeInfo()

            with patch("mutagen.File", return_value=_FakeAudio()):
                status, data = self._get("/api/wrong-matches/explorer?download_log_id=42")

        self.assertEqual(status, 200)
        tags = data["files"][0]["tags"]
        self.assertNotIn("apic:", tags)
        self.assertEqual(tags["album"], ["Shut Up And Listen To Majosha"])
        self.assertEqual(tags["genre"], ["Funk Rock"])
        self.assertEqual(tags["date"], ["1989"])
        self.assertEqual(tags["artist"], ["Majosha"])
        self.assertEqual(tags["albumartist"], ["Majosha"])
        self.assertEqual(tags["discnumber"], ["2"])
        self.assertEqual(
            tags["musicbrainz_albumid"],
            ["20f1e791-34cd-4b47-8783-51492b90218a"],
        )

    def test_wrong_match_explorer_returns_files_in_beets_matched_order(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            for filename in ("a.mp3", "b.mp3", "c.mp3"):
                with open(os.path.join(tmpdir, filename), "wb") as handle:
                    handle.write(b"fake mp3 bytes")

            self.mock_db.get_download_log_entry.return_value = {
                "id": 42,
                "request_id": 100,
                "validation_result": {
                    "failed_path": tmpdir,
                    "candidates": [{
                        "is_target": True,
                        "mapping": [
                            {
                                "item": {"path": "c.mp3", "title": "Third", "track": 12, "disc": 1},
                                "track": {"medium_index": 1, "medium": 1, "title": "Target One"},
                            },
                            {
                                "item": {"path": "a.mp3", "title": "First", "track": 1, "disc": 1},
                                "track": {"medium_index": 2, "medium": 1, "title": "Target Two"},
                            },
                            {
                                "item": {"path": "b.mp3", "title": "Second", "track": 7, "disc": 1},
                                "track": {"medium_index": 3, "medium": 1, "title": "Target Three"},
                            },
                        ],
                    }],
                },
            }

            class _FakeInfo:
                length = 181.0
                bitrate = 320000

            def _fake_audio(path: str):
                basename = os.path.basename(path)

                class _FakeAudio:
                    info = _FakeInfo()
                    if basename == "a.mp3":
                        tags = {"title": ["First"], "tracknumber": ["1/14"], "discnumber": ["1/1"]}
                    elif basename == "b.mp3":
                        tags = {"title": ["Second"], "tracknumber": ["7/14"], "discnumber": ["1/1"]}
                    else:
                        tags = {"title": ["Third"], "tracknumber": ["12/14"], "discnumber": ["1/1"]}

                return _FakeAudio()

            with patch("mutagen.File", side_effect=_fake_audio):
                status, data = self._get("/api/wrong-matches/explorer?download_log_id=42")

        self.assertEqual(status, 200)
        self.assertEqual(data["ordered_by"], "matched")
        self.assertEqual(
            [file["relative_path"] for file in data["files"]],
            ["c.mp3", "a.mp3", "b.mp3"],
        )
        self.assertEqual(
            [file["matched_order"] for file in data["files"]],
            [1, 2, 3],
        )

    def test_wrong_match_audio_supports_byte_ranges(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            track_path = os.path.join(tmpdir, "01 - Track.mp3")
            with open(track_path, "wb") as handle:
                handle.write(b"abcdef")

            self.mock_db.get_download_log_entry.return_value = {
                "id": 42,
                "request_id": 100,
                "validation_result": {
                    "failed_path": tmpdir,
                },
            }

            req = Request(
                f"{self.base}/api/wrong-matches/audio?download_log_id=42&path=01%20-%20Track.mp3",
                headers={"Range": "bytes=1-3"},
            )
            with urlopen(req) as resp:
                body = resp.read()
                status = resp.status
                content_range = resp.headers["Content-Range"]
                accept_ranges = resp.headers["Accept-Ranges"]

        self.assertEqual(status, 206)
        self.assertEqual(body, b"bcd")
        self.assertEqual(content_range, "bytes 1-3/6")
        self.assertEqual(accept_ranges, "bytes")

    def test_entry_evidence_keys_present_when_null(self):
        """Covers AE2 — missing evidence is missing data, not a trigger.

        Legacy rows lacking spectral and V0 probe evidence still produce
        the four keys with ``None`` values (never absent), and the entry
        payload exposes no preview action / preview button / async
        preview hook (R3 — this feature does not introduce a preview
        workflow).
        """
        # _DEFAULT_WRONG_MATCH_ROW already has all four evidence fields
        # set to None; this test pins that the resulting entry mirrors that.
        _, data = self._get("/api/wrong-matches")
        entry = data["groups"][0]["entries"][0]
        self.assertEqual(entry["source_dirs"], [])
        for field in ("spectral_grade", "spectral_bitrate",
                      "v0_probe_kind", "v0_probe_avg_bitrate"):
            self.assertIn(field, entry)
            self.assertIsNone(entry[field])
        # R3 regression guard: no preview-related keys leak into the
        # entry dict as part of this feature.
        for key in entry.keys():
            self.assertFalse(
                key.lower().startswith("preview"),
                f"entry exposed unexpected preview-related key: {key!r}")

    def test_entry_surfaces_evidence_derived_quality(self):
        """Per-candidate format/bitrate/rank come from album_quality_evidence.

        get_wrong_matches() LEFT JOINs the evidence row addressed by
        download_log.candidate_evidence_id; the route layer surfaces
        storage_format → entry.format, min_bitrate_kbps → entry.min_bitrate,
        verified_lossless → entry.verified_lossless, and computes
        quality_rank from format + bitrate via compute_library_rank.
        """
        row = copy.deepcopy(_DEFAULT_WRONG_MATCH_ROW)
        row["evidence_storage_format"] = "FLAC"
        row["evidence_min_bitrate"] = 0
        row["evidence_verified_lossless"] = True
        self.mock_db.get_wrong_matches.return_value = [row]

        _, data = self._get("/api/wrong-matches")
        entry = data["groups"][0]["entries"][0]
        self.assertEqual(entry["format"], "FLAC")
        self.assertEqual(entry["min_bitrate"], 0)
        self.assertTrue(entry["verified_lossless"])
        self.assertEqual(entry["quality_rank"], "lossless")

    def test_entries_sort_best_quality_first(self):
        """Entries within a group sort lossless → transparent → ... → unknown.

        Mixed-quality reject queue: FLAC, MP3 320, MP3 192, opus 128, and
        an evidence-less row. The frontend operator wants the best
        candidate at the top so they can force-import without scrolling.
        """
        def _row(log_id: int, fmt: str | None, kbps: int | None) -> dict:
            r = self._row(log_id, 770, f"user{log_id}", f"/fi/p{log_id}",
                          artist="A", album="B", mb_release_id="mb-x",
                          distance=0.20)
            r["evidence_storage_format"] = fmt
            r["evidence_min_bitrate"] = kbps
            r["evidence_verified_lossless"] = fmt == "FLAC"
            return r

        self.mock_db.get_wrong_matches.return_value = [
            _row(901, None,   None),   # unknown
            _row(902, "opus", 128),    # transparent
            _row(903, "MP3",  320),    # transparent
            _row(904, "FLAC", 0),      # lossless
            _row(905, "MP3",  192),    # good
        ]
        with patch("web.routes.imports.resolve_failed_path",
                   side_effect=lambda p: p):
            status, data = self._get("/api/wrong-matches")
        self.assertEqual(status, 200)
        entries = data["groups"][0]["entries"]
        ranks = [e["quality_rank"] for e in entries]
        ids = [e["download_log_id"] for e in entries]
        # Lossless first, then transparent (two tied — broken by id desc),
        # then good, then unknown last.
        self.assertEqual(
            ranks,
            ["lossless", "transparent", "transparent", "good", "unknown"],
            f"unexpected rank order {ranks} (ids={ids})")
        self.assertEqual(entries[0]["download_log_id"], 904)
        self.assertEqual(entries[-1]["download_log_id"], 901)

    def test_multiple_rejections_for_same_request_collapse_to_single_group(self):
        """RED for issue #113: 3 rejections on one request → 1 group with 3 entries."""
        self.mock_db.get_wrong_matches.return_value = [
            self._row(3584, 515, "ascalaphid", "/fi/path_9"),
            self._row(3565, 515, "gatybfb",    "/fi/path_8"),
            self._row(3559, 515, "jazzush",    "/fi/path_7"),
        ]
        with patch("web.routes.imports.resolve_failed_path",
                   side_effect=lambda p: p):
            status, data = self._get("/api/wrong-matches")
        self.assertEqual(status, 200)
        groups = data["groups"]
        self.assertEqual(len(groups), 1,
                         "3 rejections on one request must collapse to 1 group.")
        group = groups[0]
        self.assertEqual(group["request_id"], 515)
        self.assertEqual(len(group["entries"]), 3)
        self.assertEqual(group["pending_count"], 3)
        ids = [e["download_log_id"] for e in group["entries"]]
        self.assertEqual(ids, [3584, 3565, 3559],
                         "Entries must be ordered newest download_log_id first.")

    def test_multiple_releases_return_separate_groups(self):
        self.mock_db.get_wrong_matches.return_value = [
            self._row(200, 1, "u1", "/fi/a", artist="A1", album="B1",
                      mb_release_id="mb-1"),
            self._row(201, 1, "u2", "/fi/b", artist="A1", album="B1",
                      mb_release_id="mb-1"),
            self._row(300, 2, "u3", "/fi/c", artist="A2", album="B2",
                      mb_release_id="mb-2"),
        ]
        with patch("web.routes.imports.resolve_failed_path",
                   side_effect=lambda p: p):
            status, data = self._get("/api/wrong-matches")
        self.assertEqual(status, 200)
        groups = data["groups"]
        self.assertEqual(len(groups), 2)
        by_req = {g["request_id"]: g for g in groups}
        self.assertEqual(len(by_req[1]["entries"]), 2)
        self.assertEqual(len(by_req[2]["entries"]), 1)

    @patch("web.server.check_beets_library_detail",
           return_value={"abc-123": {"beets_format": "MP3",
                                     "beets_bitrate": 207,
                                     "beets_tracks": 12}})
    def test_group_shows_current_quality_when_imported(self, _mock_beets):
        """Imported album: quality_label, quality_rank, verified_lossless reflect on-disk state."""
        row = self._row(42, 100, "testuser", "/fi/Test")
        row["request_status"] = "imported"
        row["request_min_bitrate"] = 207
        row["request_verified_lossless"] = True
        row["request_current_spectral_grade"] = "genuine"
        row["request_imported_path"] = "/mnt/virtio/Music/Beets/Artist/Album"
        self.mock_db.get_wrong_matches.return_value = [row]
        status, data = self._get("/api/wrong-matches")
        group = data["groups"][0]
        self.assertEqual(group["status"], "imported")
        self.assertEqual(group["min_bitrate"], 207)
        self.assertTrue(group["verified_lossless"])
        self.assertEqual(group["current_spectral_grade"], "genuine")
        self.assertEqual(group["format"], "MP3")
        # `quality_label` is bitrate-only: 207 kbps lands in the V2 band on
        # the label function (V0 starts at ≥220). The rank is independent —
        # it applies `compute_library_rank` which uses the codec-aware tiers.
        self.assertIsInstance(group["quality_label"], str)
        self.assertTrue(group["quality_label"].startswith("MP3"))
        self.assertIsInstance(group["quality_rank"], str)

    def test_group_shows_nothing_on_disk_when_wanted(self):
        """Wanted album: no files in library yet — fields are null, label signals 'not on disk'."""
        row = self._row(42, 100, "testuser", "/fi/Test")
        row["request_status"] = "wanted"
        row["request_min_bitrate"] = None
        row["request_verified_lossless"] = False
        self.mock_db.get_wrong_matches.return_value = [row]
        status, data = self._get("/api/wrong-matches")
        group = data["groups"][0]
        self.assertEqual(group["status"], "wanted")
        self.assertIsNone(group["min_bitrate"])
        self.assertFalse(group["verified_lossless"])
        # No on-disk state → label and rank may be None; the frontend can render
        # a 'not on disk' badge from `status` and absent label.
        self.assertTrue(group["quality_label"] is None or isinstance(group["quality_label"], str))

    def test_group_hides_stale_quality_when_not_in_beets(self):
        """Pipeline DB can hold stale on-disk fields after beet remove.

        After a ban-source path, ``album_requests`` rows can keep the
        ``min_bitrate`` / ``current_spectral_*`` values from a prior import
        even though ``beet remove -d`` has wiped the files. The wrong-matches
        card must not surface those ghost fields — otherwise the user sees
        "320k likely_transcode" for a release with nothing on disk and
        force-imports based on false quality data.
        """
        row = self._row(42, 100, "testuser", "/fi/Test")
        row["request_status"] = "wanted"
        row["request_min_bitrate"] = 320                  # stale
        row["request_verified_lossless"] = False
        row["request_current_spectral_grade"] = "likely_transcode"  # stale
        row["request_current_spectral_bitrate"] = 160                # stale
        self.mock_db.get_wrong_matches.return_value = [row]
        # No beets mock — _is_in_beets returns False, so every on-disk
        # field in the response should reflect "nothing on disk".
        status, data = self._get("/api/wrong-matches")
        self.assertEqual(status, 200)
        group = data["groups"][0]
        self.assertFalse(group["in_library"],
                         "Precondition: test requires album absent from beets.")
        self.assertIsNone(group["min_bitrate"],
                          "min_bitrate must not leak from stale DB when not in beets.")
        self.assertIsNone(group["current_spectral_grade"],
                          "current_spectral_grade must not leak from stale DB.")
        self.assertIsNone(group["current_spectral_bitrate"],
                          "current_spectral_bitrate must not leak from stale DB.")
        self.assertFalse(group["verified_lossless"],
                         "verified_lossless must read False when nothing is on disk.")

    @patch("web.server.check_beets_by_artist_album",
           create=True, return_value=12)
    @patch("web.server.check_beets_library_detail", return_value={})
    def test_group_in_library_false_when_mbid_not_in_beets(
            self, _mock_detail, _mock_fuzzy):
        """No exact MBID hit → ``in_library`` is False, quality blanks.

        Issue #123: the old behavior was a fuzzy artist+album fallback
        that turned on the badge for a sibling pressing match. That
        conflated identity and presence and silently attributed stale
        pipeline DB quality fields to whatever row fuzzy happened to
        catch. After deleting the fuzzy path, 'in library' means
        'beets holds this exact release ID' and nothing else.

        The fuzzy shim is mocked with ``create=True`` so the test is
        RED against the current code (which would call it and flip the
        badge on) and GREEN after the deletion (the call site vanishes,
        so the mock sits unused). If a user has an untagged legacy copy
        of the album, the honest UI answer is 'not in library' — re-tag
        it or add it to the pipeline.
        """
        row = self._row(42, 100, "testuser", "/fi/Test",
                         mb_release_id="aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
        row["request_status"] = "imported"
        row["request_min_bitrate"] = 245
        row["request_verified_lossless"] = True
        row["request_current_spectral_grade"] = "genuine"
        row["request_current_spectral_bitrate"] = None
        self.mock_db.get_wrong_matches.return_value = [row]

        status, data = self._get("/api/wrong-matches")
        self.assertEqual(status, 200)
        group = data["groups"][0]
        self.assertFalse(
            group["in_library"],
            "Issue #123: no exact ID match → in_library False "
            "(fuzzy fallback was deleted).")
        self.assertIsNone(group["min_bitrate"])
        self.assertFalse(group["verified_lossless"])
        self.assertIsNone(group["current_spectral_grade"])
        self.assertIsNone(group["quality_label"])
        self.assertIsNone(group["quality_rank"])

    @patch("web.server.check_beets_by_artist_album",
           create=True, return_value=12)
    @patch("web.server.check_beets_library_detail", return_value={})
    def test_group_in_library_false_for_mbidless_request(
            self, _mock_detail, _mock_fuzzy):
        """Request with no MBID → always ``in_library`` False (issue #123).

        A request that never had an MBID (edge case — shouldn't happen
        in current flows but persists in old rows) cannot pattern-match
        anything exact. After fuzzy deletion, the only honest answer
        is 'not in library' — even if a fuzzy artist+album shim would
        have returned a match (mocked here with ``create=True`` so the
        test is RED against the current code).
        """
        row = self._row(42, 100, "testuser", "/fi/Test", mb_release_id=None)
        row["request_status"] = "imported"
        row["request_min_bitrate"] = 245
        row["request_verified_lossless"] = True
        row["request_current_spectral_grade"] = "genuine"
        self.mock_db.get_wrong_matches.return_value = [row]

        status, data = self._get("/api/wrong-matches")
        self.assertEqual(status, 200)
        group = data["groups"][0]
        self.assertFalse(group["in_library"])
        self.assertIsNone(group["min_bitrate"])
        self.assertFalse(group["verified_lossless"])
        self.assertIsNone(group["current_spectral_grade"])

    def test_group_latest_import_picks_most_recent_success(self):
        """latest_import shows the last successful import, not the newest attempt.

        A rejection that happened after a successful import doesn't change what
        beets has — the earlier success is still what's on disk.
        """
        row = self._row(42, 100, "testuser", "/fi/Test")
        self.mock_db.get_wrong_matches.return_value = [row]
        self.mock_db.get_download_history_batch.return_value = {
            100: [
                # Newest = rejected (a later force-import attempt that failed).
                {"id": 999, "outcome": "rejected",
                 "created_at": "2026-04-19T09:00:00+00:00",
                 "soulseek_username": "newestuser",
                 "actual_filetype": "mp3", "actual_min_bitrate": 192,
                 "beets_scenario": "high_distance"},
                # Then an older force_import — this is what's actually on disk.
                {"id": 900, "outcome": "force_import",
                 "created_at": "2026-04-10T09:00:00+00:00",
                 "soulseek_username": "forceuser",
                 "actual_filetype": "mp3", "actual_min_bitrate": 207,
                 "beets_scenario": "force_import"},
                {"id": 800, "outcome": "success",
                 "created_at": "2026-03-10T12:00:00+00:00",
                 "soulseek_username": "olderuser",
                 "actual_filetype": "flac", "actual_min_bitrate": 900},
            ],
        }
        status, data = self._get("/api/wrong-matches")
        group = data["groups"][0]
        latest = group["latest_import"]
        self.assertIsNotNone(latest)
        self.assertEqual(latest["id"], 900,
                         "Must pick the most recent success/force/manual import, "
                         "not the newest rejection.")
        self.assertEqual(latest["outcome"], "force_import")
        self.assertEqual(latest["soulseek_username"], "forceuser")

    def test_group_latest_import_none_when_never_imported(self):
        """Release that has only rejections → latest_import is None."""
        row = self._row(42, 100, "testuser", "/fi/Test")
        self.mock_db.get_wrong_matches.return_value = [row]
        self.mock_db.get_download_history_batch.return_value = {
            100: [
                {"id": 999, "outcome": "rejected",
                 "created_at": "2026-04-19T09:00:00+00:00",
                 "soulseek_username": "u1"},
                {"id": 998, "outcome": "timeout",
                 "created_at": "2026-04-18T09:00:00+00:00",
                 "soulseek_username": "u2"},
            ],
        }
        status, data = self._get("/api/wrong-matches")
        group = data["groups"][0]
        self.assertIsNone(group["latest_import"])

    def test_group_latest_import_none_when_batch_empty(self):
        """Edge case: no history rows at all → latest_import is None."""
        row = self._row(42, 100, "testuser", "/fi/Test")
        self.mock_db.get_wrong_matches.return_value = [row]
        self.mock_db.get_download_history_batch.return_value = {}
        status, data = self._get("/api/wrong-matches")
        group = data["groups"][0]
        self.assertIsNone(group["latest_import"])

    def test_group_dropped_when_no_entries_have_existing_files(self):
        """If every entry's files are gone, the group is excluded from the UI."""
        self.mock_db.get_wrong_matches.return_value = [
            self._row(10, 5, "u1", "/gone/a"),
            self._row(11, 5, "u2", "/gone/b"),
        ]
        with patch("web.routes.imports.resolve_failed_path", return_value=None):
            status, data = self._get("/api/wrong-matches")
        self.assertEqual(status, 200)
        self.assertEqual(data["groups"], [])

    def test_group_pending_count_reflects_existing_entries_only(self):
        """pending_count counts entries with files still on disk."""
        self.mock_db.get_wrong_matches.return_value = [
            self._row(20, 7, "present", "/on-disk/a"),
            self._row(21, 7, "missing", "/gone/b"),
        ]
        with patch("web.routes.imports.resolve_failed_path",
                   side_effect=lambda p: p if p.startswith("/on-disk") else None):
            status, data = self._get("/api/wrong-matches")
        self.assertEqual(status, 200)
        groups = data["groups"]
        self.assertEqual(len(groups), 1)
        group = groups[0]
        self.assertEqual(group["pending_count"], 1)
        self.assertEqual([e["download_log_id"] for e in group["entries"]], [20])

    def test_candidate_has_distance_breakdown(self):
        status, data = self._get("/api/wrong-matches")
        entry = data["groups"][0]["entries"][0]
        candidate = entry["candidate"]
        self.assertIsNotNone(candidate)
        self.assertIn("distance_breakdown", candidate)
        self.assertIn("mapping", candidate)

    @patch("web.routes.imports.resolve_failed_path",
           return_value="/mnt/virtio/music/slskd/failed_imports/Test")
    def test_relative_failed_path_uses_resolved_path(self, _mock_resolve):
        row = copy.deepcopy(_DEFAULT_WRONG_MATCH_ROW)
        row["validation_result"]["failed_path"] = "failed_imports/Test"
        self.mock_db.get_wrong_matches.return_value = [row]

        status, data = self._get("/api/wrong-matches")

        self.assertEqual(status, 200)
        entry = data["groups"][0]["entries"][0]
        self.assertTrue(entry["files_exist"])
        self.assertEqual(entry["failed_path"],
                         "/mnt/virtio/music/slskd/failed_imports/Test")

    def test_manual_delete_route_deletes_single_wrong_match(self):
        status, data = self._post(
            "/api/wrong-matches/delete",
            {"download_log_id": 42},
        )

        self.assertEqual(status, 200)
        _assert_required_fields(
            self,
            data,
            self.DELETE_RESULT_REQUIRED_FIELDS,
            "wrong-match delete response",
        )
        self.assertEqual(data["status"], "ok")
        self.assertTrue(data["success"])
        self.assertEqual(data["deleted_path"],
                         "/mnt/virtio/music/slskd/failed_imports/Test")
        self.mock_manual_cleanup.assert_called_once_with(
            self.mock_db,
            42,
            require_visible=True,
        )

    def test_manual_delete_route_blocks_active_import_job(self):
        from lib.wrong_match_delete_service import (
            OUTCOME_SKIPPED_ACTIVE_JOB,
            WrongMatchDeleteResult,
        )

        self.mock_manual_cleanup.side_effect = None
        self.mock_manual_cleanup.return_value = WrongMatchDeleteResult(
            download_log_id=42,
            outcome=OUTCOME_SKIPPED_ACTIVE_JOB,
            skipped=True,
            reason="active_import_job",
        )

        status, data = self._post(
            "/api/wrong-matches/delete",
            {"download_log_id": 42},
        )

        self.assertEqual(status, 409)
        self.assertEqual(data["error"], "active_import_job")
        self.mock_manual_cleanup.assert_called_once_with(
            self.mock_db,
            42,
            require_visible=True,
        )

    def test_manual_delete_route_reports_lock_contention_as_retryable(self):
        from lib.wrong_match_delete_service import (
            OUTCOME_SKIPPED_LOCKED,
            WrongMatchDeleteResult,
        )

        self.mock_manual_cleanup.side_effect = None
        self.mock_manual_cleanup.return_value = WrongMatchDeleteResult(
            download_log_id=42,
            outcome=OUTCOME_SKIPPED_LOCKED,
            skipped=True,
            reason="cleanup_lock_unavailable",
        )

        status, data = self._post(
            "/api/wrong-matches/delete",
            {"download_log_id": 42},
        )

        self.assertEqual(status, 503)
        self.assertEqual(data["error"], "cleanup_lock_unavailable")

    def test_manual_delete_group_deletes_request_rows(self):
        status, data = self._post(
            "/api/wrong-matches/delete-group",
            {"request_id": 42},
        )

        self.assertEqual(status, 200)
        _assert_required_fields(
            self,
            data,
            self.DELETE_GROUP_REQUIRED_FIELDS,
            "wrong-match delete-group response",
        )
        self.assertEqual(data["status"], "ok")
        self.assertTrue(data["success"])
        self.assertEqual(data["processed"], 2)
        self.assertEqual(data["deleted"], 2)
        self.assertEqual(data["cleared"], 2)
        self.assertEqual(data["deleted_paths"], 2)
        self.assertTrue(data["group_empty"])
        self.mock_manual_group_cleanup.assert_called_once_with(self.mock_db, 42)

    def test_manual_delete_group_reports_partial_when_rows_are_skipped(self):
        from lib.wrong_match_delete_service import (
            OUTCOME_SKIPPED_ACTIVE_JOB,
            WrongMatchDeleteResult,
            WrongMatchDeleteSummary,
        )

        skipped = WrongMatchDeleteResult(
            download_log_id=100,
            outcome=OUTCOME_SKIPPED_ACTIVE_JOB,
            success=False,
            request_id=42,
            skipped=True,
            reason="active_import_job",
        )
        self.mock_manual_group_cleanup.side_effect = None
        self.mock_manual_group_cleanup.return_value = WrongMatchDeleteSummary(
            request_id=42,
            outcome="partial",
            success=False,
            processed=1,
            deleted=0,
            deleted_paths=0,
            cleared=0,
            skipped=1,
            errors=0,
            remaining=1,
            group_empty=False,
            results=(skipped,),
        )

        status, data = self._post(
            "/api/wrong-matches/delete-group",
            {"request_id": 42},
        )

        self.assertEqual(status, 409)
        self.assertEqual(data["status"], "partial")
        self.assertFalse(data["success"])
        self.assertEqual(data["skipped"], 1)
        self.assertEqual(data["remaining"], 1)

    def test_retired_heuristic_delete_routes_are_removed(self):
        for path in (
            "/api/wrong-matches/delete-transparent-non-flac",
            "/api/wrong-matches/delete-lossless-opus",
        ):
            with self.subTest(path=path):
                status, _data = self._post(path, {})
                self.assertEqual(status, 404)

    def test_bulk_triage_requires_full_queue_confirmation(self):
        status, data = self._post("/api/wrong-matches/triage", {})

        self.assertEqual(status, 400)
        self.assertIn("confirm_all_wrong_matches", data.get("message") or data.get("error") or "")

    @patch("web.routes.imports.cleanup_all_wrong_matches")
    def test_bulk_triage_runs_full_wrong_matches_queue(self, mock_cleanup):
        from lib.wrong_match_cleanup_service import WrongMatchCleanupSummary

        runner = _fresh_triage_runner(self)
        mock_cleanup.return_value = WrongMatchCleanupSummary(
            processed=3,
            deleted=2,
            kept_would_import=1,
        )

        status, data = self._post(
            "/api/wrong-matches/triage",
            {"confirm_all_wrong_matches": True},
        )

        self.assertEqual(status, 202)
        self.assertEqual(data["status"], "started")

        runner.join(timeout=5)
        mock_cleanup.assert_called_once_with(
            self.mock_db,
            confirm_all_wrong_matches=True,
        )

        status, data = self._get("/api/wrong-matches/triage/status")
        self.assertEqual(status, 200)
        self.assertEqual(data["state"], "completed")
        self.assertEqual(data["summary"]["processed"], 3)
        self.assertEqual(data["summary"]["deleted"], 2)

    def test_groups_in_beets_still_shown(self):
        """Wrong matches still appear when the release is already in the library."""
        status, data = self._get("/api/wrong-matches")
        self.assertEqual(status, 200)
        self.assertGreater(len(data["groups"]), 0)

    def test_converge_queues_green_candidates_and_deletes_unmatched(self):
        """Converge queues green rows and deletes high-distance leftovers."""
        self.mock_db.get_wrong_matches.return_value = [
            self._row(100, 42, "u1", "/fi/a", distance=0.167),
            self._row(101, 42, "u2", "/fi/b", distance=0.180),
            self._row(102, 42, "u3", "/fi/c", distance=0.226),
            self._row(200, 99, "other", "/fi/other", distance=0.100),
        ]
        self.mock_db.get_wrong_matches.return_value[0]["validation_result"]["source_dirs"] = [
            "u1\\Artist\\Album",
        ]
        entries = {
            100: self._entry(100, 42, "/fi/a"),
            101: self._entry(101, 42, "/fi/b"),
            102: self._entry(102, 42, "/fi/c"),
        }
        self.mock_db.get_download_log_entry.side_effect = (
            lambda lid: copy.deepcopy(entries[lid])
        )
        self.mock_db.enqueue_import_job.side_effect = [
            self._job(900, 42, 100, "/fi/a"),
            self._job(901, 42, 101, "/fi/b"),
        ]
        def manual_delete_after_enqueue(_db, log_id, **_kwargs):
            self.assertEqual(self.mock_db.enqueue_import_job.call_count, 2)
            return self._manual_cleanup_result(log_id)

        self.mock_manual_cleanup.side_effect = manual_delete_after_enqueue

        status, data = self._post("/api/wrong-matches/converge", {
            "request_id": 42,
            "threshold_milli": 180,
            "delete_unmatched": False,
        })

        self.assertEqual(status, 202)
        self.assertEqual(data["status"], "ok")
        self.assertEqual(data["queued"], 2)
        self.assertEqual(data["selected_count"], 2)
        self.assertEqual(data["unmatched_count"], 1)
        self.assertTrue(data["delete_unmatched"])
        self.assertEqual(data["deleted"], 1)
        self.assertEqual(data["dismissed"], 0)
        self.assertEqual(data["remaining"], 2)
        self.assertFalse(data["group_empty"])
        self.assertEqual(
            {item["download_log_id"] for item in data["selected"]},
            {100, 101},
        )
        self.assertEqual(
            [call.kwargs["dedupe_key"]
             for call in self.mock_db.enqueue_import_job.call_args_list],
            [
                "force_import:download_log:100",
                "force_import:download_log:101",
            ],
        )
        self.mock_db.clear_wrong_match_paths.assert_not_called()
        self.assertEqual(
            self.mock_db.enqueue_import_job.call_args_list[0].kwargs["payload"]["source_dirs"],
            ["u1\\Artist\\Album"],
        )
        self.mock_manual_cleanup.assert_called_once_with(self.mock_db, 102, require_visible=True)
        self.mock_cleanup.assert_not_called()

    def test_converge_deletes_unmatched_when_legacy_client_requests_it(self):
        """Legacy true payloads still delete non-green rows while selected rows stay visible."""
        self.mock_db.get_wrong_matches.return_value = [
            self._row(100, 42, "u1", "/fi/a", distance=0.167),
            self._row(101, 42, "u2", "/fi/b", distance=0.180),
            self._row(102, 42, "u3", "/fi/c", distance=0.226),
        ]
        entries = {
            100: self._entry(100, 42, "/fi/a"),
            101: self._entry(101, 42, "/fi/b"),
            102: self._entry(102, 42, "/fi/c"),
        }
        self.mock_db.get_download_log_entry.side_effect = (
            lambda lid: copy.deepcopy(entries[lid])
        )
        self.mock_db.enqueue_import_job.side_effect = [
            self._job(900, 42, 100, "/fi/a"),
            self._job(901, 42, 101, "/fi/b"),
        ]

        status, data = self._post("/api/wrong-matches/converge", {
            "request_id": 42,
            "threshold_milli": 180,
            "delete_unmatched": True,
        })

        self.assertEqual(status, 202)
        self.assertEqual(data["queued"], 2)
        self.assertEqual(data["deleted"], 1)
        self.assertEqual(data["remaining"], 2)
        self.assertFalse(data["group_empty"])
        self.mock_manual_cleanup.assert_called_once_with(self.mock_db, 102, require_visible=True)
        self.mock_cleanup.assert_not_called()

    def test_converge_deletes_unmatched_unconditionally_without_classifier(self):
        """Operator-authority contract: converge does NOT route deletion through cleanup_wrong_match.

        Regression guard for the issue where unmatched rows with kept_would_import
        or stale-evidence verdicts would silently stay visible because cleanup's
        evidence-based classifier blocked deletion. Converge has already collected
        operator intent; the unmatched row dies.
        """
        self.mock_db.get_wrong_matches.return_value = [
            self._row(100, 42, "u1", "/fi/a", distance=0.167),
            self._row(102, 42, "u3", "/fi/c", distance=0.226),
        ]
        entries = {
            100: self._entry(100, 42, "/fi/a"),
            102: self._entry(102, 42, "/fi/c"),
        }
        self.mock_db.get_download_log_entry.side_effect = (
            lambda lid: copy.deepcopy(entries[lid])
        )
        self.mock_db.enqueue_import_job.return_value = self._job(900, 42, 100, "/fi/a")

        status, data = self._post("/api/wrong-matches/converge", {
            "request_id": 42,
            "threshold_milli": 180,
            "delete_unmatched": True,
        })

        self.assertEqual(status, 202)
        self.assertEqual(data["deleted"], 1)
        self.assertEqual(data["remaining"], 1)
        self.mock_manual_cleanup.assert_called_once_with(self.mock_db, 102, require_visible=True)
        self.mock_cleanup.assert_not_called()

    def test_converge_skips_missing_green_files(self):
        """A green row with no surviving failed_path is not queued or dismissed."""
        self.mock_db.get_wrong_matches.return_value = [
            self._row(100, 42, "u1", "/gone/a", distance=0.167),
        ]

        with patch("web.routes.imports.resolve_failed_path", return_value=None):
            status, data = self._post("/api/wrong-matches/converge", {
                "request_id": 42,
                "threshold_milli": 180,
                "delete_unmatched": False,
            })

        self.assertEqual(status, 202)
        self.assertEqual(data["queued"], 0)
        self.assertEqual(data["remaining"], 1)
        self.assertEqual(data["skipped"], [
            {"download_log_id": 100, "reason": "files_missing"},
        ])
        self.mock_db.enqueue_import_job.assert_not_called()
        self.mock_db.clear_wrong_match_paths.assert_not_called()
        self.mock_cleanup.assert_not_called()

    def test_converge_reports_deduped_jobs(self):
        """Existing active force-import jobs still count as selected but remain visible."""
        self.mock_db.get_wrong_matches.return_value = [
            self._row(100, 42, "u1", "/fi/a", distance=0.167),
        ]
        self.mock_db.get_download_log_entry.return_value = self._entry(100, 42, "/fi/a")
        self.mock_db.enqueue_import_job.return_value = self._job(
            900, 42, 100, "/fi/a", deduped=True)

        status, data = self._post("/api/wrong-matches/converge", {
            "request_id": 42,
            "threshold_milli": 180,
        })

        self.assertEqual(status, 202)
        self.assertEqual(data["queued"], 1)
        self.assertEqual(data["deduped"], 1)
        self.assertTrue(data["selected"][0]["deduped"])
        self.assertEqual(data["dismissed"], 0)
        self.assertEqual(data["remaining"], 1)

    def test_converge_missing_request_id_returns_error(self):
        status, _data = self._post("/api/wrong-matches/converge", {})
        self.assertEqual(status, 400)

if __name__ == "__main__":
    unittest.main()
