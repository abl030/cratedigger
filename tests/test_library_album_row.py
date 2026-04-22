#!/usr/bin/env python3
"""Tests for the typed /api/library/artist album-row contract."""

from __future__ import annotations

from datetime import datetime, timezone
import unittest

import msgspec

from web.library_album_row import LibraryAlbumRow


def _valid_row_dict(**overrides: object) -> dict[str, object]:
    row: dict[str, object] = {
        "id": 7,
        "album": "Test Album",
        "artist": "Test Artist",
        "year": 2024,
        "mb_albumid": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
        "track_count": 10,
        "mb_releasegroupid": "11111111-1111-1111-1111-111111111111",
        "release_group_title": "Test Album",
        "added": 1773651901.0,
        "formats": "MP3",
        "min_bitrate": 320000,
        "type": "album",
        "label": "Test Label",
        "country": "US",
        "source": "musicbrainz",
        "in_library": True,
        "beets_album_id": 7,
        "pipeline_status": None,
        "pipeline_id": None,
        "upgrade_queued": False,
        "library_rank": "transparent",
    }
    row.update(overrides)
    return row


class TestLibraryAlbumRow(unittest.TestCase):
    def test_from_beets_album_with_pipeline_none_keeps_library_defaults(self) -> None:
        row = LibraryAlbumRow.from_beets_album_with_pipeline(
            {
                "id": 7,
                "album": "Test Album",
                "artist": "Test Artist",
                "year": 2024,
                "mb_albumid": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
                "discogs_albumid": None,
                "track_count": 10,
                "mb_releasegroupid": "11111111-1111-1111-1111-111111111111",
                "release_group_title": "Test Album",
                "added": 1773651901.0,
                "formats": "MP3",
                "min_bitrate": 320000,
                "type": "album",
                "label": "Test Label",
                "country": "US",
            },
            pipeline_row=None,
            rank_fn=lambda _fmt, _kbps: "transparent",
        )

        self.assertIsNone(row.pipeline_status)
        self.assertIsNone(row.pipeline_id)
        self.assertFalse(row.upgrade_queued)
        self.assertEqual(row.library_rank, "transparent")

    def test_from_beets_album_normalizes_discogs_frontend_id(self) -> None:
        row = LibraryAlbumRow.from_beets_album(
            {
                "id": 8,
                "album": "Discogs Import",
                "artist": "Test Artist",
                "year": 2001,
                "mb_albumid": None,
                "discogs_albumid": "12856590",
                "track_count": 10,
                "mb_releasegroupid": None,
                "release_group_title": "Discogs Import",
                "added": 1773651902.0,
                "formats": "MP3",
                "min_bitrate": 320000,
                "type": "album",
                "label": "Test Label",
                "country": "AU",
            },
            rank_fn=lambda _fmt, _kbps: "transparent",
        )

        self.assertEqual(row.mb_albumid, "12856590")
        self.assertEqual(row.source, "discogs")
        self.assertTrue(row.in_library)
        self.assertEqual(row.beets_album_id, 8)
        self.assertEqual(row.library_rank, "transparent")

    def test_from_pipeline_request_mb_path_uses_mb_release_id(self) -> None:
        row = LibraryAlbumRow.from_pipeline_request(
            {
                "id": 41,
                "artist_name": "Test Artist",
                "album_title": "Wanted Album",
                "year": 2024,
                "country": "US",
                "format": "CD",
                "source": "request",
                "status": "wanted",
                "min_bitrate": 320,
                "mb_release_id": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
                "discogs_release_id": None,
                "mb_release_group_id": "11111111-1111-1111-1111-111111111111",
                "created_at": datetime(2026, 4, 1, 3, 47, 54, tzinfo=timezone.utc),
                "search_filetype_override": None,
                "target_format": None,
            },
            track_count=10,
        )

        self.assertEqual(row.mb_albumid, "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
        self.assertEqual(row.source, "request")
        self.assertEqual(row.pipeline_id, 41)

    def test_from_pipeline_request_owns_placeholder_fields(self) -> None:
        row = LibraryAlbumRow.from_pipeline_request(
            {
                "id": 42,
                "artist_name": "Test Artist",
                "album_title": "Wanted Album",
                "year": 2024,
                "country": "US",
                "format": "CD",
                "source": "request",
                "status": "wanted",
                "min_bitrate": 320,
                "mb_release_id": None,
                "discogs_release_id": "12856590",
                "mb_release_group_id": None,
                "created_at": datetime(2026, 4, 1, 3, 47, 54, tzinfo=timezone.utc),
                "search_filetype_override": "flac",
                "target_format": None,
            },
            track_count=10,
        )

        self.assertEqual(row.mb_albumid, "12856590")
        self.assertEqual(row.release_group_title, "Wanted Album")
        self.assertEqual(row.min_bitrate, 320000)
        self.assertEqual(row.source, "request")
        self.assertFalse(row.in_library)
        self.assertIsNone(row.beets_album_id)
        self.assertEqual(row.pipeline_status, "wanted")
        self.assertEqual(row.pipeline_id, 42)
        self.assertTrue(row.upgrade_queued)
        self.assertIsNone(row.library_rank)

    def test_from_pipeline_request_defaults_missing_source_to_unknown(self) -> None:
        row = LibraryAlbumRow.from_pipeline_request(
            {
                "id": 42,
                "artist_name": "Test Artist",
                "album_title": "Wanted Album",
                "year": 2024,
                "country": "US",
                "format": "CD",
                "source": None,
                "status": "wanted",
                "min_bitrate": 320,
                "mb_release_id": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
                "discogs_release_id": None,
                "mb_release_group_id": None,
                "created_at": datetime(2026, 4, 1, 3, 47, 54, tzinfo=timezone.utc),
                "search_filetype_override": None,
                "target_format": None,
            },
            track_count=10,
        )

        self.assertEqual(row.source, "unknown")

    def test_from_pipeline_request_rejects_missing_album_title(self) -> None:
        with self.assertRaises(msgspec.ValidationError):
            LibraryAlbumRow.from_pipeline_request(
                {
                    "id": 42,
                    "artist_name": "Test Artist",
                    "album_title": None,
                    "year": 2024,
                    "country": "US",
                    "format": "CD",
                    "source": "request",
                    "status": "wanted",
                    "min_bitrate": 320,
                    "mb_release_id": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
                    "discogs_release_id": None,
                    "mb_release_group_id": None,
                    "created_at": datetime(2026, 4, 1, 3, 47, 54, tzinfo=timezone.utc),
                    "search_filetype_override": None,
                    "target_format": None,
                },
                track_count=10,
            )

    def test_from_pipeline_request_rejects_invalid_created_at(self) -> None:
        with self.assertRaises(TypeError):
            LibraryAlbumRow.from_pipeline_request(
                {
                    "id": 42,
                    "artist_name": "Test Artist",
                    "album_title": "Wanted Album",
                    "year": 2024,
                    "country": "US",
                    "format": "CD",
                    "source": "request",
                    "status": "wanted",
                    "min_bitrate": 320,
                    "mb_release_id": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
                    "discogs_release_id": None,
                    "mb_release_group_id": None,
                    "created_at": "2026-04-01T03:47:54Z",
                    "search_filetype_override": None,
                    "target_format": None,
                },
                track_count=10,
            )

    def test_with_pipeline_request_overlays_pipeline_state(self) -> None:
        row = LibraryAlbumRow.from_beets_album(
            {
                "id": 7,
                "album": "Test Album",
                "artist": "Test Artist",
                "year": 2024,
                "mb_albumid": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
                "discogs_albumid": None,
                "track_count": 10,
                "mb_releasegroupid": "11111111-1111-1111-1111-111111111111",
                "release_group_title": "Test Album",
                "added": 1773651901.0,
                "formats": "MP3",
                "min_bitrate": 320000,
                "type": "album",
                "label": "Test Label",
                "country": "US",
            },
            rank_fn=lambda _fmt, _kbps: "transparent",
        ).with_pipeline_request(
            {
                "id": 42,
                "status": "wanted",
                "search_filetype_override": "flac",
                "target_format": None,
            }
        )

        self.assertEqual(row.pipeline_status, "wanted")
        self.assertEqual(row.pipeline_id, 42)
        self.assertTrue(row.upgrade_queued)

    def test_wire_boundary_rejects_wrong_field_type(self) -> None:
        with self.assertRaises(msgspec.ValidationError):
            msgspec.convert(
                _valid_row_dict(id="7"),
                type=LibraryAlbumRow,
            )
