#!/usr/bin/env python3
"""Contract tests for web/routes/imports.py wrong-match surfaces.

Split from tests/test_web_server.py (#408). Shared harness in
tests/web/_harness.py.
"""

import copy
import json
import os
import sys
import tempfile
import unittest
from io import IOBase
from unittest.mock import patch
from types import SimpleNamespace
from urllib.request import urlopen, Request
from urllib.error import HTTPError


sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from tests.web._harness import (
    _DEFAULT_WRONG_MATCH_VALIDATION,
    _assert_required_fields,
    _FakeDbWebServerCase,
    _fresh_triage_runner,
)

from tests.helpers import make_request_row


class TestWrongMatchesContract(_FakeDbWebServerCase):
    """Contract tests: /api/wrong-matches returns grouped-by-release shape.

    Issue #113: every rejection with a failed_path must be reachable. The
    route returns ``{groups: [{request_id, artist, album, mb_release_id,
    in_library, pending_count, entries: [...]}]}`` so the frontend can
    collapse by release and expand to per-candidate actions.
    """

    def setUp(self) -> None:
        super().setUp()
        # Default group: request 100 + one rejected row pinned to log id
        # 42 — the id many URLs / dedupe keys in this class reference.
        self.default_log_id = self._seed_wrong_match(
            download_log_id=42, request_id=100, username="testuser",
            failed_path="/mnt/virtio/music/slskd/failed_imports/Test",
        )
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
        "status", "min_bitrate", "avg_bitrate", "format", "verified_lossless",
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
        # Storage format + explicit min/avg bitrates + computed quality rank — read
        # from album_quality_evidence via download_log.candidate_evidence_id
        # so wrong-match rows show their actual codec/rank instead of
        # dashes from the legacy denorm columns. Drives entry sort order.
        "format", "min_bitrate", "avg_bitrate", "verified_lossless", "quality_rank",
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

    def _reseed_request(self, request_id: int, **overrides: object) -> None:
        """Adjust fixture state through the fake's explicit setup seam."""
        row = self.db.get_request(request_id)
        assert row is not None
        self.db.seed_request({**row, **overrides})

    def _wrong_match_runtime_config(self, slskd_root: str):
        """Point descriptor-rooted explorer tests at their temp quarantine."""
        return patch(
            "web.wrong_match_file_service.read_runtime_config",
            return_value=SimpleNamespace(
                slskd_download_dir=slskd_root,
                beets_staging_dir=os.path.join(slskd_root, "Incoming"),
                processing_dir=os.path.join(slskd_root, "processing"),
            ),
        )

    def _seed_wrong_match(
        self, *,
        download_log_id: int | None = None,
        request_id: int = 100,
        username: str = "testuser",
        failed_path: str = "/mnt/virtio/music/slskd/failed_imports/Test",
        artist: str = "Test Artist",
        album: str = "Test Album",
        mb_release_id: str | None = "abc-123",
        scenario: str = "high_distance",
        distance: float | None = 0.25,
        request_overrides: dict | None = None,
        validation_overrides: dict | None = None,
        log_overrides: dict | None = None,
    ) -> int:
        """Seed a request + one rejected download_log row that the fake's
        REAL get_wrong_matches query surfaces (the legacy harness fed the
        joined row shape straight into a mock). Returns the log id;
        ``download_log_id`` pins it for tests whose URLs / dedupe keys
        hardcode ids."""
        if self.db.get_request(request_id) is None:
            self.db.seed_request(make_request_row(
                id=request_id, status="wanted", artist_name=artist,
                album_title=album, mb_release_id=mb_release_id,
                mb_release_group_id="rg-abc-123",
                min_bitrate=None, verified_lossless=False,
                current_spectral_grade=None,
                current_spectral_bitrate=None,
                **(request_overrides or {}),
            ))
        elif request_overrides:
            self._reseed_request(request_id, **request_overrides)
        vr = copy.deepcopy(_DEFAULT_WRONG_MATCH_VALIDATION)
        vr["failed_path"] = failed_path
        vr["scenario"] = scenario
        vr["distance"] = distance
        vr["candidates"][0]["distance"] = distance
        vr["soulseek_username"] = username
        vr.update(validation_overrides or {})
        if download_log_id is not None:
            # Forward pin only — the fake's id-mint guard raises if the
            # pinned id collides with or precedes an existing id.
            self.db._next_download_log_id = download_log_id - 1
        return self.db.log_download(
            request_id, outcome="rejected", soulseek_username=username,
            validation_result=vr, **(log_overrides or {}),
        )

    def _seed_entry_evidence(
        self, log_id: int, *,
        storage_format: str | None = None,
        min_bitrate: int | None = None,
        avg_bitrate: int | None = None,
        verified_lossless: bool = False,
        spectral_grade: str | None = None,
        spectral_bitrate: int | None = None,
        v0_probe_kind: str | None = None,
        v0_probe_avg_bitrate: int | None = None,
        source_codec: str | None = "mp3",
        source_container: str | None = "mp3",
        target_format: str | None = None,
        lineage_version: int = 3,
    ) -> None:
        """Attach a real album_quality_evidence row to a download_log row
        — the route reads it through the fake's LEFT-JOIN mirror."""
        from lib.quality import (
            AlbumQualityV0Metric,
            AudioQualityMeasurement,
            VerifiedLosslessProof,
        )
        from tests.helpers import make_album_quality_evidence
        evidence = make_album_quality_evidence(
            mb_release_id=f"ev-{log_id}",
            storage_format=storage_format,
            codec=source_codec,
            container=source_container,
            target_format=target_format,
            lineage_version=lineage_version,
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=min_bitrate,
                avg_bitrate_kbps=(
                    avg_bitrate if avg_bitrate is not None else min_bitrate
                ),
                format=storage_format,
                spectral_grade=spectral_grade,
                spectral_bitrate_kbps=spectral_bitrate,
                spectral_subject=("source" if spectral_grade is not None else None),
                spectral_provenance=(
                    "measured" if spectral_grade is not None else None
                ),
            ),
            verified_lossless_proof=VerifiedLosslessProof(
                provenance="measured", source="seeded",
                classifier="contract-test",
            ) if verified_lossless else None,
            v0_metric=AlbumQualityV0Metric(
                avg_bitrate_kbps=v0_probe_avg_bitrate,
                subject=(
                    "source"
                    if v0_probe_kind in ("lossless_source", "lossless_source_v0")
                    else "installed"
                ),
            ) if (v0_probe_kind or v0_probe_avg_bitrate) else None,
        )
        self.db.upsert_album_quality_evidence(evidence)
        stored = self.db.find_album_quality_evidence(
            mb_release_id=evidence.mb_release_id,
            snapshot_fingerprint=evidence.snapshot_fingerprint)
        assert stored is not None and stored.id is not None
        self.db.set_download_log_candidate_evidence(log_id, stored.id)

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

    def test_default_wrong_matches_excludes_replaced_audit_rows(self):
        self._seed_wrong_match(request_id=101, mb_release_id="abc-101")
        self._reseed_request(101, status="replaced")

        status, data = self._get("/api/wrong-matches")

        self.assertEqual(status, 200)
        self.assertNotIn(101, [group["request_id"] for group in data["groups"]])

    def test_include_replaced_true_preserves_explicit_history_view(self):
        self._seed_wrong_match(request_id=101, mb_release_id="abc-101")
        self._reseed_request(101, status="replaced")

        status, data = self._get("/api/wrong-matches?include_replaced=true")

        self.assertEqual(status, 200)
        self.assertIn(101, [group["request_id"] for group in data["groups"]])

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

    def test_untracked_audio_entry_has_null_distance_and_is_not_green(self):
        """Issue #550 defect #4: a pre-match reject (no beets distance was
        ever measured) must serialize ``distance: None`` — not a
        fabricated ``0.0`` the UI would render as a false-green candidate.
        """
        self._seed_wrong_match(
            download_log_id=777, request_id=777, username="u1",
            failed_path="/fi/untracked-777", distance=None,
            scenario="untracked_audio", mb_release_id="mb-777",
        )
        status, data = self._get("/api/wrong-matches")
        self.assertEqual(status, 200)
        group = next(g for g in data["groups"] if g["request_id"] == 777)
        entry = next(e for e in group["entries"]
                     if e["download_log_id"] == 777)
        self.assertIsNone(entry["distance"])

        from lib.validation_envelope import decode_validation_envelope
        from web.routes.imports import _is_green_distance
        log_entry = self.db.get_download_log_entry(777)
        assert log_entry is not None
        vr = decode_validation_envelope(log_entry["validation_result"])
        self.assertIsNone(vr.distance)
        self.assertFalse(
            _is_green_distance(vr, 180),
            "a null (unmeasured) distance must never render green",
        )

    def test_entry_surfaces_stored_spectral_and_v0_probe_evidence(self):
        """Covers AE1 — per-candidate stored evidence reaches the row payload.

        Plumbs the four per-attempt download_log columns
        (spectral_grade/spectral_bitrate/v0_probe_kind/v0_probe_avg_bitrate)
        from get_wrong_matches() through to the entry dict so the operator
        can eyeball candidates by audio quality.
        """
        # Legacy denorm columns (evidence rows reject legacy probe
        # kinds like lossless_source_v0) — the COALESCE path the route
        # falls back to for pre-evidence rows.
        self.db.delete_request(100)
        self._seed_wrong_match(download_log_id=43, log_overrides=dict(
            spectral_grade="suspect", spectral_bitrate=320,
            v0_probe_kind="lossless_source_v0", v0_probe_avg_bitrate=265,
        ))

        _, data = self._get("/api/wrong-matches")
        entry = data["groups"][0]["entries"][0]
        self.assertEqual(entry["spectral_grade"], "suspect")
        self.assertEqual(entry["spectral_bitrate"], 320)
        self.assertEqual(entry["v0_probe_kind"], "lossless_source_v0")
        self.assertEqual(entry["v0_probe_avg_bitrate"], 265)

    def test_entry_surfaces_preserved_source_dirs(self):
        self.db.delete_request(100)  # replace the setUp row wholesale
        self._seed_wrong_match(
            download_log_id=43,
            validation_overrides={"source_dirs": [
                "baduser\\Artist\\Album",
                "baduser\\Artist\\Album\\CD2",
            ]},
        )

        _, data = self._get("/api/wrong-matches")
        entry = data["groups"][0]["entries"][0]
        self.assertEqual(
            entry["source_dirs"],
            ["baduser\\Artist\\Album", "baduser\\Artist\\Album\\CD2"],
        )

    def test_wrong_match_explorer_lists_audio_files_and_source_dirs(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            failed_dir = os.path.join(tmpdir, "failed_imports", "Test")
            os.makedirs(failed_dir)
            track_path = os.path.join(failed_dir, "01 - Track.mp3")
            with open(track_path, "wb") as handle:
                handle.write(b"fake mp3 bytes")

            log_id = self.db.log_download(
                100, outcome="rejected",
                validation_result={
                    "failed_path": failed_dir,
                    "source_dirs": ["baduser\\Artist\\Album"],
                },
            )

            with self._wrong_match_runtime_config(tmpdir):
                status, data = self._get(
                    f"/api/wrong-matches/explorer?download_log_id={log_id}")

        self.assertEqual(status, 200)
        self.assertEqual(data["status"], "ok")
        self.assertEqual(data["source_dirs"], ["baduser\\Artist\\Album"])
        self.assertEqual(data["audio_file_count"], 1)
        self.assertEqual(data["files"][0]["relative_path"], "01 - Track.mp3")
        self.assertTrue(data["files"][0]["playable"])
        self.assertIn(
            f"/api/wrong-matches/audio?download_log_id={log_id}",
            data["files"][0]["stream_url"])

    def test_wrong_match_explorer_caps_total_entries_across_tree(self):
        """A broad nested quarantine tree stops at one global entry budget."""
        with tempfile.TemporaryDirectory() as tmpdir:
            failed_dir = os.path.join(tmpdir, "failed_imports", "Test")
            os.makedirs(failed_dir)
            for index in range(4):
                nested = os.path.join(failed_dir, f"disc-{index}")
                os.mkdir(nested)
                with open(os.path.join(nested, "01.mp3"), "wb") as handle:
                    handle.write(b"audio")
            log_id = self.db.log_download(
                100, outcome="rejected",
                validation_result={"failed_path": failed_dir},
            )

            with patch(
                "web.wrong_match_file_service._EXPLORER_MAX_ENTRIES", 3,
            ), self._wrong_match_runtime_config(tmpdir):
                status, data = self._get(
                    f"/api/wrong-matches/explorer?download_log_id={log_id}")

        self.assertEqual(status, 200)
        self.assertTrue(data["partial"])
        self.assertEqual(data["truncated_reason"], "entry_limit")
        self.assertEqual(data["files"], [])

    def test_wrong_match_explorer_normalizes_raw_id3_tags_and_skips_artwork_frames(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            failed_dir = os.path.join(tmpdir, "failed_imports", "Test")
            os.makedirs(failed_dir)
            track_path = os.path.join(failed_dir, "01 - Track.mp3")
            with open(track_path, "wb") as handle:
                handle.write(b"fake mp3 bytes")

            log_id = self.db.log_download(
                100, outcome="rejected",
                validation_result={"failed_path": failed_dir},
            )

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

            with patch("mutagen.File", return_value=_FakeAudio()), \
                    self._wrong_match_runtime_config(tmpdir):
                status, data = self._get(
                    f"/api/wrong-matches/explorer?download_log_id={log_id}")

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
            failed_dir = os.path.join(tmpdir, "failed_imports", "Test")
            os.makedirs(failed_dir)
            for filename in ("a.mp3", "b.mp3", "c.mp3"):
                with open(os.path.join(failed_dir, filename), "wb") as handle:
                    handle.write(b"fake mp3 bytes")

            log_id = self.db.log_download(
                100, outcome="rejected",
                validation_result={
                    "failed_path": failed_dir,
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
            )

            class _FakeInfo:
                length = 181.0
                bitrate = 320000

            def _fake_audio(source: object, **_kwargs: object):
                # Explorer deliberately hands Mutagen the already-open fd;
                # recover the test fixture basename from that descriptor.
                assert isinstance(source, IOBase)
                basename = os.path.basename(os.readlink(
                    f"/proc/self/fd/{source.fileno()}"))

                class _FakeAudio:
                    info = _FakeInfo()
                    if basename == "a.mp3":
                        tags = {"title": ["First"], "tracknumber": ["1/14"], "discnumber": ["1/1"]}
                    elif basename == "b.mp3":
                        tags = {"title": ["Second"], "tracknumber": ["7/14"], "discnumber": ["1/1"]}
                    else:
                        tags = {"title": ["Third"], "tracknumber": ["12/14"], "discnumber": ["1/1"]}

                return _FakeAudio()

            with patch("mutagen.File", side_effect=_fake_audio), \
                    self._wrong_match_runtime_config(tmpdir):
                status, data = self._get(
                    f"/api/wrong-matches/explorer?download_log_id={log_id}")

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

    def test_wrong_match_audio_short_read_closes_keepalive_connection(self):
        """A file truncated mid-stream writes fewer bytes than the
        declared Content-Length; the server must close the keep-alive
        socket instead of letting the next response desync (#427)."""
        import http.client
        import tempfile
        import os as _os

        with tempfile.TemporaryDirectory() as tmpdir:
            failed_dir = _os.path.join(tmpdir, "failed_imports", "Test")
            _os.makedirs(failed_dir)
            track_path = _os.path.join(failed_dir, "01 - Track.mp3")
            with open(track_path, "wb") as handle:
                handle.write(b"abcdef")

            log_id = self.db.log_download(
                100, outcome="rejected",
                validation_result={"failed_path": failed_dir},
            )

            conn = http.client.HTTPConnection(
                "127.0.0.1", self.port, timeout=10)
            try:
                # Pretend the already-open file was four bytes longer than
                # it is.  The route intentionally reads and serves one FD;
                # this patch exposes the short-read close branch without
                # reintroducing a path/stat race.
                from lib.fs_authority import (
                    OpenedRegularFile,
                    open_directory_path,
                    open_regular_relative,
                )
                with open_directory_path(failed_dir) as root_fd:
                    actual = open_regular_relative(root_fd, "01 - Track.mp3")
                    opened = OpenedRegularFile(
                        fd=actual.fd,
                        parent_fd=actual.parent_fd,
                        name=actual.name,
                        stat_result=os.stat_result(
                            (0, 0, 0, 0, 0, 0, 10, 0, 0, 0)),
                    )
                    with patch(
                        "web.routes.imports.resolve_wrong_match_stream_file",
                        return_value=(opened, "audio/mpeg"),
                    ):
                        conn.request(
                            "GET",
                            "/api/wrong-matches/audio"
                            f"?download_log_id={log_id}"
                            "&path=01%20-%20Track.mp3",
                        )
                        resp = conn.getresponse()
                        self.assertEqual(resp.status, 200)
                        self.assertEqual(
                            resp.getheader("Content-Length"), "10")
                        # Server closed after the short body: the client
                        # sees an incomplete read promptly rather than
                        # blocking for 4 bytes that will never come.
                        with self.assertRaises(http.client.IncompleteRead):
                            resp.read()
            finally:
                conn.close()

    def test_wrong_match_audio_supports_byte_ranges(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            failed_dir = os.path.join(tmpdir, "failed_imports", "Test")
            os.makedirs(failed_dir)
            track_path = os.path.join(failed_dir, "01 - Track.mp3")
            with open(track_path, "wb") as handle:
                handle.write(b"abcdef")

            log_id = self.db.log_download(
                100, outcome="rejected",
                validation_result={"failed_path": failed_dir},
            )

            req = Request(
                f"{self.base}/api/wrong-matches/audio"
                f"?download_log_id={log_id}&path=01%20-%20Track.mp3",
                headers={"Range": "bytes=1-3"},
            )
            with self._wrong_match_runtime_config(tmpdir):
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
        # The setUp row carries no evidence; this test pins that the
        # resulting entry still emits all four keys as None.
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
        storage_format → entry.format, min/avg bitrate → entry.min/avg_bitrate,
        verified_lossless → entry.verified_lossless, and computes
        quality_rank from format + average via compute_library_rank.
        """
        self._seed_entry_evidence(
            self.default_log_id,
            storage_format="FLAC", min_bitrate=0, verified_lossless=True,
        )

        _, data = self._get("/api/wrong-matches")
        entry = data["groups"][0]["entries"][0]
        self.assertEqual(entry["format"], "FLAC")
        self.assertEqual(entry["min_bitrate"], 0)
        self.assertEqual(entry["avg_bitrate"], 0)
        self.assertTrue(entry["verified_lossless"])
        self.assertEqual(entry["quality_rank"], "lossless")

    def test_entry_keeps_gas_source_target_and_v0_probe_separate(self):
        """A FLAC V0 probe must not wear the configured Opus label."""
        self._seed_entry_evidence(
            self.default_log_id,
            storage_format="FLAC",
            min_bitrate=191,
            avg_bitrate=224,
            source_codec="flac",
            source_container="flac",
            target_format="opus 128",
            v0_probe_kind="lossless_source",
            v0_probe_avg_bitrate=224,
        )

        _, data = self._get("/api/wrong-matches")
        entry = data["groups"][0]["entries"][0]
        self.assertEqual(entry["source_codec"], "flac")
        self.assertEqual(entry["source_container"], "flac")
        self.assertEqual(
            entry["target_format"], "opus 128",
            "v3 evidence carries explicit target policy",
        )
        self.assertEqual(entry["format"], "FLAC")
        self.assertEqual(entry["quality_lineage_version"], 3)
        self.assertEqual(entry["v0_probe_avg_bitrate"], 224)

    def test_legacy_evidence_uses_marked_storage_projection_only(self):
        self._seed_entry_evidence(
            self.default_log_id,
            storage_format="opus 128",
            min_bitrate=191,
            avg_bitrate=224,
            target_format=None,
            lineage_version=1,
        )

        _, data = self._get("/api/wrong-matches")
        entry = data["groups"][0]["entries"][0]
        self.assertEqual(entry["target_format"], "opus 128")
        self.assertEqual(entry["quality_lineage_version"], 1)

    def test_entries_sort_best_quality_first(self):
        """Entries within a group sort lossless → transparent → ... → unknown.

        Mixed-quality reject queue: FLAC, MP3 320, MP3 192, opus 128, and
        an evidence-less row. The frontend operator wants the best
        candidate at the top so they can force-import without scrolling.
        """
        self.db.delete_request(100)  # drop the setUp group
        def _seed(log_id: int, fmt: str | None, kbps: int | None) -> None:
            self._seed_wrong_match(
                download_log_id=log_id, request_id=770,
                username=f"user{log_id}", failed_path=f"/fi/p{log_id}",
                artist="A", album="B", mb_release_id="mb-x",
                distance=0.20)
            if fmt is not None:
                self._seed_entry_evidence(
                    log_id, storage_format=fmt, min_bitrate=kbps,
                    verified_lossless=fmt == "FLAC")

        _seed(901, None,   None)   # unknown
        _seed(902, "opus", 128)    # transparent
        _seed(903, "MP3",  320)    # transparent
        _seed(904, "FLAC", 0)      # lossless
        _seed(905, "MP3",  192)    # good
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
        self.db.delete_request(100)  # drop the setUp group
        self._seed_wrong_match(download_log_id=3559, request_id=515,
                               username="jazzush", failed_path="/fi/path_7")
        self._seed_wrong_match(download_log_id=3565, request_id=515,
                               username="gatybfb", failed_path="/fi/path_8")
        self._seed_wrong_match(download_log_id=3584, request_id=515,
                               username="ascalaphid", failed_path="/fi/path_9")
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
        self.db.delete_request(100)  # drop the setUp group
        self._seed_wrong_match(download_log_id=200, request_id=1,
                               username="u1", failed_path="/fi/a",
                               artist="A1", album="B1", mb_release_id="mb-1")
        self._seed_wrong_match(download_log_id=201, request_id=1,
                               username="u2", failed_path="/fi/b",
                               artist="A1", album="B1", mb_release_id="mb-1")
        self._seed_wrong_match(download_log_id=300, request_id=2,
                               username="u3", failed_path="/fi/c",
                               artist="A2", album="B2", mb_release_id="mb-2")
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
                                     "beets_bitrate": 194,
                                     "beets_avg_bitrate": 288,
                                     "beets_tracks": 12}})
    def test_group_shows_current_quality_when_imported(self, _mock_beets):
        """Imported album: quality_label, quality_rank, verified_lossless reflect on-disk state."""
        self.assertTrue(self.db.mark_imported_with_rescue(
            100, expected_status="wanted", min_bitrate=207,
            verified_lossless=True, current_spectral_grade="genuine"))
        status, data = self._get("/api/wrong-matches")
        group = data["groups"][0]
        self.assertEqual(group["status"], "imported")
        self.assertEqual(group["min_bitrate"], 194)
        self.assertEqual(group["avg_bitrate"], 288)
        self.assertTrue(group["verified_lossless"])
        self.assertEqual(group["current_spectral_grade"], "genuine")
        self.assertEqual(group["format"], "MP3")
        self.assertEqual(group["quality_label"], "MP3 V0")
        self.assertEqual(group["quality_rank"], "transparent")

    def test_group_shows_nothing_on_disk_when_wanted(self):
        """Wanted album: no files in library yet — fields are null, label signals 'not on disk'."""
        # setUp's request 100 is already wanted with no on-disk quality.
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
        self.db.update_request_fields(
            100, min_bitrate=320,                             # stale
            verified_lossless=False,
            current_spectral_grade="likely_transcode",        # stale
            current_spectral_bitrate=160)                     # stale
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
        self._reseed_request(
            100, status="imported",
            mb_release_id="aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
            min_bitrate=245, verified_lossless=True,
            current_spectral_grade="genuine",
            current_spectral_bitrate=None)

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
        self._reseed_request(
            100, status="imported", mb_release_id=None,
            min_bitrate=245, verified_lossless=True,
            current_spectral_grade="genuine")

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
        # Real history, oldest → newest: success, then a force_import,
        # then a rejected attempt newest. The summary must pick the
        # force_import (most recent import), not the newest rejection.
        self.db.log_download(
            100, outcome="success", soulseek_username="olderuser",
            actual_filetype="flac", actual_min_bitrate=900)
        forced_id = self.db.log_download(
            100, outcome="force_import", soulseek_username="forceuser",
            actual_filetype="mp3", actual_min_bitrate=207,
            beets_scenario="force_import")
        self.db.log_download(
            100, outcome="rejected", soulseek_username="newestuser",
            actual_filetype="mp3", actual_min_bitrate=192,
            beets_scenario="high_distance")
        status, data = self._get("/api/wrong-matches")
        group = data["groups"][0]
        latest = group["latest_import"]
        self.assertIsNotNone(latest)
        self.assertEqual(latest["id"], forced_id,
                         "Must pick the most recent active or historical import, "
                         "not the newest rejection.")
        self.assertEqual(latest["outcome"], "force_import")
        self.assertEqual(latest["soulseek_username"], "forceuser")

    def test_group_latest_import_none_when_never_imported(self):
        """Release that has only rejections → latest_import is None."""
        self.db.log_download(100, outcome="timeout",
                             soulseek_username="u2")
        self.db.log_download(100, outcome="rejected",
                             soulseek_username="u1")
        status, data = self._get("/api/wrong-matches")
        group = data["groups"][0]
        self.assertIsNone(group["latest_import"])

    def test_group_latest_import_none_when_batch_empty(self):
        """Edge case: no history rows at all → latest_import is None."""
        # Only the setUp rejection exists — no import history at all.
        status, data = self._get("/api/wrong-matches")
        group = data["groups"][0]
        self.assertIsNone(group["latest_import"])

    def test_group_dropped_when_no_entries_have_existing_files(self):
        """If every entry's files are gone, the group is excluded from the UI."""
        self.db.delete_request(100)  # drop the setUp group
        self._seed_wrong_match(download_log_id=10, request_id=5,
                               username="u1", failed_path="/gone/a")
        self._seed_wrong_match(download_log_id=11, request_id=5,
                               username="u2", failed_path="/gone/b")
        with patch("web.routes.imports.resolve_failed_path", return_value=None):
            status, data = self._get("/api/wrong-matches")
        self.assertEqual(status, 200)
        self.assertEqual(data["groups"], [])

    def test_group_pending_count_reflects_existing_entries_only(self):
        """pending_count counts entries with files still on disk."""
        self.db.delete_request(100)  # drop the setUp group
        self._seed_wrong_match(download_log_id=20, request_id=7,
                               username="present", failed_path="/on-disk/a")
        self._seed_wrong_match(download_log_id=21, request_id=7,
                               username="missing", failed_path="/gone/b")
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
        self.db.delete_request(100)  # replace the setUp row wholesale
        self._seed_wrong_match(download_log_id=43,
                               failed_path="failed_imports/Test")

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
            self.db,
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
            self.db,
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
        self.mock_manual_group_cleanup.assert_called_once_with(self.db, 42)

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
            self.db,
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
        self._seed_wrong_match(
            download_log_id=100, request_id=42, username="u1",
            failed_path="/fi/a", distance=0.167, mb_release_id="mb-42",
            validation_overrides={"source_dirs": ["u1\\Artist\\Album"]})
        self._seed_wrong_match(
            download_log_id=101, request_id=42, username="u2",
            failed_path="/fi/b", distance=0.180, mb_release_id="mb-42")
        self._seed_wrong_match(
            download_log_id=102, request_id=42, username="u3",
            failed_path="/fi/c", distance=0.226, mb_release_id="mb-42")
        self._seed_wrong_match(
            download_log_id=200, request_id=99, username="other",
            failed_path="/fi/other", distance=0.100, mb_release_id="mb-99")

        def manual_delete_after_enqueue(_db, log_id, **_kwargs):
            # Both green rows were queued BEFORE the unmatched delete ran.
            self.assertEqual(len(self.db.list_import_jobs()), 2)
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
        jobs = {j.dedupe_key: j for j in self.db.list_import_jobs()}
        self.assertEqual(
            set(jobs),
            {
                "force_import:download_log:100",
                "force_import:download_log:101",
            },
        )
        # EVERY queued row stays visible: failed_path survives on both.
        for lid, path in ((100, "/fi/a"), (101, "/fi/b")):
            entry = self.db.get_download_log_entry(lid)
            assert entry is not None
            vr = entry["validation_result"]
            assert vr is not None
            self.assertEqual(vr["failed_path"], path)
        self.assertEqual(
            jobs["force_import:download_log:100"]
            .payload["source_dirs"],
            ["u1\\Artist\\Album"],
        )
        self.mock_manual_cleanup.assert_called_once_with(self.db, 102, require_visible=True)
        self.mock_cleanup.assert_not_called()

    def test_converge_deletes_unmatched_when_legacy_client_requests_it(self):
        """Legacy true payloads still delete non-green rows while selected rows stay visible."""
        self._seed_wrong_match(
            download_log_id=100, request_id=42, username="u1",
            failed_path="/fi/a", distance=0.167, mb_release_id="mb-42")
        self._seed_wrong_match(
            download_log_id=101, request_id=42, username="u2",
            failed_path="/fi/b", distance=0.180, mb_release_id="mb-42")
        self._seed_wrong_match(
            download_log_id=102, request_id=42, username="u3",
            failed_path="/fi/c", distance=0.226, mb_release_id="mb-42")

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
        self.mock_manual_cleanup.assert_called_once_with(self.db, 102, require_visible=True)
        self.mock_cleanup.assert_not_called()

    def test_converge_deletes_unmatched_unconditionally_without_classifier(self):
        """Operator-authority contract: converge does NOT route deletion through cleanup_wrong_match.

        Regression guard for the issue where unmatched rows with kept_would_import
        or stale-evidence verdicts would silently stay visible because cleanup's
        evidence-based classifier blocked deletion. Converge has already collected
        operator intent; the unmatched row dies.
        """
        self._seed_wrong_match(
            download_log_id=100, request_id=42, username="u1",
            failed_path="/fi/a", distance=0.167, mb_release_id="mb-42")
        self._seed_wrong_match(
            download_log_id=102, request_id=42, username="u3",
            failed_path="/fi/c", distance=0.226, mb_release_id="mb-42")

        status, data = self._post("/api/wrong-matches/converge", {
            "request_id": 42,
            "threshold_milli": 180,
            "delete_unmatched": True,
        })

        self.assertEqual(status, 202)
        self.assertEqual(data["deleted"], 1)
        self.assertEqual(data["remaining"], 1)
        self.mock_manual_cleanup.assert_called_once_with(self.db, 102, require_visible=True)
        self.mock_cleanup.assert_not_called()

    def test_converge_skips_missing_green_files(self):
        """A green row with no surviving failed_path is not queued or dismissed."""
        self._seed_wrong_match(
            download_log_id=100, request_id=42, username="u1",
            failed_path="/gone/a", distance=0.167, mb_release_id="mb-42")

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
        self.assertEqual(self.db.list_import_jobs(), [])
        # The row stays visible — failed_path survives.
        entry_100 = self.db.get_download_log_entry(100)
        assert entry_100 is not None
        vr_100 = entry_100["validation_result"]
        assert vr_100 is not None
        self.assertIn("failed_path", vr_100)
        self.mock_cleanup.assert_not_called()

    def test_converge_reports_deduped_jobs(self):
        """Existing active force-import jobs still count as selected but remain visible."""
        self._seed_wrong_match(
            download_log_id=100, request_id=42, username="u1",
            failed_path="/fi/a", distance=0.167, mb_release_id="mb-42")
        # Pre-existing active job with the same dedupe key — converge's
        # enqueue dedupes against it for real.
        self.db.enqueue_import_job(
            "force_import", request_id=42,
            dedupe_key="force_import:download_log:100",
            payload={"download_log_id": 100, "failed_path": "/fi/a"},
        )

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
