#!/usr/bin/env python3
"""Direct tests for the `/api/beets/album/<id>` detail seam."""

from __future__ import annotations

import json
from datetime import datetime, timezone
import unittest

from tests.fakes import FakePipelineDB
from tests.helpers import make_request_row
from web.library_album_detail_service import (
    build_library_album_detail,
    load_library_album_detail,
)


RELEASE_ID = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"


def _track(**overrides: object) -> dict[str, object]:
    track: dict[str, object] = {
        "id": 11,
        "artist": "Test Artist",
        "disc": 1,
        "track": 1,
        "title": "Track 1",
        "length": 240.5,
        "format": "MP3",
        "bitrate": 320000,
        "samplerate": 44100,
        "bitdepth": 16,
        "path": "/music/Test Artist/Test Album/01 Track 1.mp3",
    }
    track.update(overrides)
    return track


def _beets_detail(**overrides: object) -> dict[str, object]:
    detail: dict[str, object] = {
        "id": 7,
        "album": "Test Album",
        "artist": "Test Artist",
        "year": 2024,
        "mb_albumid": RELEASE_ID,
        "type": "album",
        "label": "Test Label",
        "country": "US",
        "added": 1773651901.0,
        "artpath": "/music/Test Artist/Test Album/cover.jpg",
        "tracks": [_track(), _track(track=2, title="Track 2")],
        "path": "/music/Test Artist/Test Album",
        "source": "musicbrainz",
    }
    detail.update(overrides)
    return detail


def _history_row(**overrides: object) -> dict[str, object]:
    row: dict[str, object] = {
        "id": 1,
        "request_id": 42,
        "outcome": "success",
        "created_at": datetime(2026, 3, 30, 12, 0, 0, tzinfo=timezone.utc),
        "soulseek_username": "testuser",
        "beets_scenario": "strong_match",
        "beets_distance": 0.012,
        "bitrate": 320000,
        "slskd_filetype": "mp3",
        "slskd_bitrate": 320000,
        "actual_filetype": "mp3",
        "actual_min_bitrate": 320,
        "spectral_grade": None,
        "spectral_bitrate": None,
        "existing_min_bitrate": None,
        "existing_spectral_bitrate": None,
        "import_result": None,
        "validation_result": None,
        "source": "request",
    }
    row.update(overrides)
    return row


class _StubLibraryLookup:
    def __init__(self, detail: dict[str, object] | None) -> None:
        self._detail = detail
        self.calls: list[int] = []

    def get_album_detail(self, album_id: int) -> dict[str, object] | None:
        self.calls.append(album_id)
        if self._detail is None:
            return None
        return dict(self._detail)


class TestLibraryAlbumDetailService(unittest.TestCase):
    def test_build_library_album_detail_backfills_contract_fields_from_tracks(self) -> None:
        detail = build_library_album_detail(
            detail_row=_beets_detail(
                tracks=[
                    _track(track=1, bitrate=320000),
                    _track(track=2, title="Track 2", bitrate=256000),
                ],
                source="",
                mb_releasegroupid=None,
                release_group_title="",
                formats="",
                min_bitrate=None,
            ),
            pipeline_request=None,
            download_history=[],
        )

        self.assertEqual(detail.track_count, 2)
        self.assertIsNone(detail.mb_releasegroupid)
        self.assertEqual(detail.release_group_title, "Test Album")
        self.assertEqual(detail.formats, "MP3")
        self.assertEqual(detail.min_bitrate, 256000)
        self.assertEqual(detail.source, "musicbrainz")
        self.assertIsNone(detail.pipeline_id)
        self.assertFalse(detail.upgrade_queued)
        self.assertEqual(detail.download_history, [])

    def test_build_library_album_detail_overlays_pipeline_state_and_history(self) -> None:
        detail = build_library_album_detail(
            detail_row=_beets_detail(),
            pipeline_request=make_request_row(
                id=42,
                mb_release_id=RELEASE_ID,
                status="wanted",
                source="request",
                min_bitrate=320,
                search_filetype_override="flac",
                target_format="lossless",
            ),
            download_history=[_history_row(
                import_result={
                    "version": 2,
                    "decision": "import",
                    "postflight": {
                        "disambiguation_failure": {
                            "reason": "timeout",
                            "detail": "timeout after 120s",
                        },
                    },
                },
                validation_result={"detail": "distance too high"},
            )],
        )

        self.assertEqual(detail.pipeline_id, 42)
        self.assertEqual(detail.pipeline_status, "wanted")
        self.assertEqual(detail.pipeline_source, "request")
        self.assertEqual(detail.pipeline_min_bitrate, 320)
        self.assertEqual(detail.search_filetype_override, "flac")
        self.assertEqual(detail.target_format, "lossless")
        self.assertTrue(detail.upgrade_queued)
        self.assertEqual(len(detail.download_history), 1)
        self.assertEqual(detail.download_history[0].soulseek_username, "testuser")
        self.assertEqual(detail.download_history[0].beets_scenario, "strong_match")
        self.assertEqual(detail.download_history[0].downloaded_label, "MP3 320")
        self.assertTrue(detail.download_history[0].verdict)
        self.assertEqual(detail.download_history[0].actual_min_bitrate, 320)
        self.assertEqual(detail.download_history[0].slskd_bitrate, 320000)
        self.assertEqual(detail.download_history[0].disambiguation_failure, "timeout")
        self.assertEqual(
            detail.download_history[0].disambiguation_detail,
            "timeout after 120s",
        )
        self.assertEqual(
            detail.download_history[0].validation_result,
            {"detail": "distance too high"},
        )
        self.assertEqual(detail.download_history[0].source, "request")

    def test_build_library_album_detail_preserves_nullable_legacy_fields(self) -> None:
        detail = build_library_album_detail(
            detail_row=_beets_detail(
                added=None,
                tracks=[
                    _track(
                        disc=None,
                        track=None,
                        title=None,
                    ),
                ],
            ),
            pipeline_request=None,
            download_history=[],
        )

        self.assertIsNone(detail.added)
        self.assertEqual(len(detail.tracks), 1)
        self.assertEqual(detail.artpath, "/music/Test Artist/Test Album/cover.jpg")
        self.assertEqual(detail.tracks[0].id, 11)
        self.assertEqual(detail.tracks[0].artist, "Test Artist")
        self.assertIsNone(detail.tracks[0].disc)
        self.assertIsNone(detail.tracks[0].track)
        self.assertIsNone(detail.tracks[0].title)
        self.assertEqual(
            detail.tracks[0].path,
            "/music/Test Artist/Test Album/01 Track 1.mp3",
        )
        self.assertEqual(detail.download_history, [])

    def test_build_library_album_detail_preserves_string_added_shape(self) -> None:
        detail = build_library_album_detail(
            detail_row=_beets_detail(added="2026-03-30T12:00:00+00:00"),
            pipeline_request=None,
            download_history=[],
        )

        self.assertEqual(detail.added, "2026-03-30T12:00:00+00:00")

    def test_build_library_album_detail_handles_missing_beets_format_keys(self) -> None:
        track_without_format = _track(track=2, title="Track 2")
        del track_without_format["format"]
        detail = build_library_album_detail(
            detail_row=_beets_detail(
                tracks=[_track(), track_without_format],
                source="",
                min_bitrate=None,
            ),
            pipeline_request=None,
            download_history=[],
        )

        self.assertEqual(detail.formats, "MP3")
        self.assertEqual(detail.min_bitrate, 320000)
        self.assertIsNone(detail.tracks[1].format)

    def test_load_library_album_detail_resolves_discogs_request(self) -> None:
        fake_db = FakePipelineDB()
        fake_db.seed_request(make_request_row(
            id=55,
            mb_release_id="12856590",
            discogs_release_id="12856590",
            artist_name="Test Artist",
            album_title="Discogs Import",
            source="request",
            status="wanted",
        ))
        fake_db.log_download(
            55,
            outcome="success",
            soulseek_username="discogs-user",
            beets_scenario="strong_match",
            beets_distance=0.01,
            actual_filetype="mp3",
            actual_min_bitrate=245,
            slskd_filetype="mp3",
            slskd_bitrate=245000,
        )
        lookup = _StubLibraryLookup(_beets_detail(
            album="Discogs Import",
            mb_albumid="12856590",
            source="discogs",
        ))

        detail = load_library_album_detail(
            library_lookup=lookup,
            pipeline_db=fake_db,
            album_id=7,
        )

        self.assertIsNotNone(detail)
        assert detail is not None
        self.assertEqual(lookup.calls, [7])
        self.assertEqual(detail.mb_albumid, "12856590")
        self.assertEqual(detail.source, "discogs")
        self.assertEqual(detail.pipeline_id, 55)
        self.assertEqual(detail.download_history[0].soulseek_username, "discogs-user")

    def test_load_library_album_detail_preserves_unknown_release_ids(self) -> None:
        fake_db = FakePipelineDB()
        fake_db.seed_request(make_request_row(
            id=77,
            mb_release_id="fixture-release-1",
            artist_name="Fixture Artist",
            album_title="Fixture Album",
            source="request",
            status="wanted",
        ))
        lookup = _StubLibraryLookup(_beets_detail(
            album="Fixture Album",
            artist="Fixture Artist",
            mb_albumid="fixture-release-1",
            source="unknown",
        ))

        detail = load_library_album_detail(
            library_lookup=lookup,
            pipeline_db=fake_db,
            album_id=7,
        )

        self.assertIsNotNone(detail)
        assert detail is not None
        self.assertEqual(detail.mb_albumid, "fixture-release-1")
        self.assertEqual(detail.source, "unknown")
        self.assertEqual(detail.pipeline_id, 77)

    def test_load_library_album_detail_prefers_mb_albumid_for_unknown_legacy_ids(self) -> None:
        fake_db = FakePipelineDB()
        fake_db.seed_request(make_request_row(
            id=90,
            mb_release_id="fixture-mb-id",
            artist_name="Fixture Artist",
            album_title="Fixture Album",
            source="request",
            status="wanted",
        ))
        fake_db.seed_request(make_request_row(
            id=91,
            mb_release_id="fixture-discogs-id",
            artist_name="Fixture Artist",
            album_title="Fixture Album",
            source="request",
            status="wanted",
        ))
        lookup = _StubLibraryLookup(_beets_detail(
            album="Fixture Album",
            artist="Fixture Artist",
            mb_albumid="fixture-mb-id",
            discogs_albumid="fixture-discogs-id",
            source="unknown",
        ))

        detail = load_library_album_detail(
            library_lookup=lookup,
            pipeline_db=fake_db,
            album_id=7,
        )

        self.assertIsNotNone(detail)
        assert detail is not None
        self.assertEqual(detail.mb_albumid, "fixture-mb-id")
        self.assertEqual(detail.pipeline_id, 90)

    def test_load_library_album_detail_preserves_string_history_json_blobs(self) -> None:
        fake_db = FakePipelineDB()
        fake_db.seed_request(make_request_row(
            id=88,
            mb_release_id=RELEASE_ID,
            artist_name="Test Artist",
            album_title="Test Album",
            source="request",
            status="wanted",
        ))
        fake_db.log_download(
            88,
            outcome="success",
            import_result=json.dumps({
                "version": 2,
                "exit_code": 0,
                "decision": "import",
            }),
            validation_result=json.dumps({
                "failed_path": "/mnt/virtio/music/slskd/failed_imports/Test",
            }),
        )
        lookup = _StubLibraryLookup(_beets_detail())

        detail = load_library_album_detail(
            library_lookup=lookup,
            pipeline_db=fake_db,
            album_id=7,
        )

        self.assertIsNotNone(detail)
        assert detail is not None
        self.assertEqual(
            detail.download_history[0].import_result,
            json.dumps({
                "version": 2,
                "exit_code": 0,
                "decision": "import",
            }),
        )
        self.assertEqual(
            detail.download_history[0].validation_result,
            json.dumps({
                "failed_path": "/mnt/virtio/music/slskd/failed_imports/Test",
            }),
        )

    def test_load_library_album_detail_returns_none_when_album_missing(self) -> None:
        lookup = _StubLibraryLookup(None)

        detail = load_library_album_detail(
            library_lookup=lookup,
            pipeline_db=FakePipelineDB(),
            album_id=99,
        )

        self.assertEqual(lookup.calls, [99])
        self.assertIsNone(detail)
