"""Tests for the typed /api/library/artist album-row contract."""

from __future__ import annotations

import unittest
from datetime import UTC, datetime

import msgspec

from lib.release_identity import ReleaseIdentity
from tests.helpers import make_request_row
from web.library_album_row import LibraryAlbumRow, _pipeline_upgrade_queued


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
        "avg_bitrate": 320000,
        "type": "album",
        "label": "Test Label",
        "country": "US",
        "source": "musicbrainz",
        "in_library": True,
        "beets_album_id": 7,
        "pipeline_status": None,
        "pipeline_id": None,
        "processing_owner": None,
        "upgrade_queued": False,
        "library_rank": "transparent",
        "has_captured_history": False,
        "pipeline_verified_lossless": False,
        "pipeline_provisional": False,
    }
    row.update(overrides)
    return row


class TestLibraryAlbumRow(unittest.TestCase):
    def test_request_6039_rank_uses_average_and_preserves_floor(self) -> None:
        seen: list[tuple[str | None, int | None]] = []

        def rank_fn(fmt: str | None, kbps: int | None) -> str:
            seen.append((fmt, kbps))
            return "transparent"

        row = LibraryAlbumRow.from_beets_album(
            {
                "id": 6039,
                "album": "Request 6039",
                "artist": "Test Artist",
                "year": 2024,
                "mb_albumid": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
                "discogs_albumid": None,
                "track_count": 3,
                "mb_releasegroupid": "11111111-1111-1111-1111-111111111111",
                "release_group_title": "Request 6039",
                "added": 1773651901.0,
                "formats": "MP3",
                "min_bitrate": 194000,
                "avg_bitrate": 288000,
                "type": "album",
                "label": "Test Label",
                "country": "AU",
            },
            rank_fn=rank_fn,
        )

        self.assertEqual(row.min_bitrate, 194000)
        self.assertEqual(row.avg_bitrate, 288000)
        self.assertEqual(seen, [("MP3", 288)])

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
                "avg_bitrate": 320000,
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
        self.assertFalse(row.has_captured_history)
        self.assertFalse(row.pipeline_verified_lossless)
        self.assertFalse(row.pipeline_provisional)

    def test_from_beets_album_with_pipeline_applies_overlay(self) -> None:
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
                "avg_bitrate": 320000,
                "type": "album",
                "label": "Test Label",
                "country": "US",
            },
            pipeline_row={
                "id": 42,
                "status": "processing",
                "processing_owner": {
                    "job_id": 9,
                    "status": "queued",
                    "preview_status": "waiting",
                },
                "search_filetype_override": None,
                "target_format": None,
                "has_captured_history": True,
                "verified_lossless": True,
                "provisional_lossless": False,
            },
            attached_identity=ReleaseIdentity(
                source="musicbrainz",
                release_id="aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
            ),
            rank_fn=lambda _fmt, _kbps: "transparent",
        )

        self.assertEqual(row.pipeline_status, "processing")
        self.assertEqual(row.pipeline_id, 42)
        owner = row.processing_owner
        assert owner is not None
        self.assertEqual(owner.job_id, 9)
        self.assertFalse(row.upgrade_queued)
        self.assertTrue(row.has_captured_history)
        self.assertTrue(row.pipeline_verified_lossless)
        self.assertFalse(row.pipeline_provisional)
        self.assertNotIn("cd_rip_verification", row.to_dict())

    def test_from_beets_album_with_pipeline_rejects_unobserved_attachment(
        self,
    ) -> None:
        with self.assertRaisesRegex(ValueError, "not observed on Beets album"):
            LibraryAlbumRow.from_beets_album_with_pipeline(
                {
                    "id": 7,
                    "album": "Test Album",
                    "artist": "Test Artist",
                    "year": 2024,
                    "mb_albumid": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
                    "discogs_albumid": "12856590",
                    "track_count": 10,
                    "mb_releasegroupid": None,
                    "release_group_title": "Test Album",
                    "added": 1773651901.0,
                    "formats": "MP3",
                    "min_bitrate": 320000,
                    "avg_bitrate": 320000,
                    "type": "album",
                    "label": "Test Label",
                    "country": "US",
                },
                pipeline_row={
                    "id": 42,
                    "status": "wanted",
                    "processing_owner": None,
                    "search_filetype_override": None,
                    "target_format": None,
                    "has_captured_history": False,
                    "verified_lossless": False,
                    "provisional_lossless": False,
                },
                attached_identity=ReleaseIdentity(
                    source="discogs",
                    release_id="12856591",
                ),
                rank_fn=lambda _fmt, _kbps: "transparent",
            )

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
                "avg_bitrate": 320000,
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

    def test_from_beets_album_falls_back_release_group_title_to_album(self) -> None:
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
                "release_group_title": None,
                "added": 1773651901.0,
                "formats": "MP3",
                "min_bitrate": 320000,
                "avg_bitrate": 320000,
                "type": "album",
                "label": "Test Label",
                "country": "US",
            },
            rank_fn=lambda _fmt, _kbps: "transparent",
        )

        self.assertEqual(row.release_group_title, "Test Album")

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
                "created_at": datetime(2026, 4, 1, 3, 47, 54, tzinfo=UTC),
                "search_filetype_override": None,
                "target_format": None,
                "has_captured_history": True,
                "verified_lossless": False,
                "provisional_lossless": True,
            },
            track_count=10,
        )

        self.assertEqual(row.mb_albumid, "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
        self.assertEqual(row.source, "request")
        self.assertEqual(row.pipeline_id, 41)
        self.assertTrue(row.has_captured_history)
        self.assertFalse(row.pipeline_verified_lossless)
        self.assertTrue(row.pipeline_provisional)

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
                "created_at": datetime(2026, 4, 1, 3, 47, 54, tzinfo=UTC),
                "search_filetype_override": "flac",
                "target_format": None,
                "has_captured_history": True,
                "verified_lossless": True,
                "provisional_lossless": False,
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
        self.assertTrue(row.has_captured_history)
        self.assertTrue(row.pipeline_verified_lossless)
        self.assertFalse(row.pipeline_provisional)

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
                "created_at": datetime(2026, 4, 1, 3, 47, 54, tzinfo=UTC),
                "search_filetype_override": None,
                "target_format": None,
                "has_captured_history": False,
                "verified_lossless": False,
                "provisional_lossless": False,
            },
            track_count=10,
        )

        self.assertEqual(row.source, "unknown")

    def test_invalid_pipeline_identity_has_no_actionable_id_or_source(self) -> None:
        for label, identity_fields in (
            ("malformed", {
                "mb_release_id": "not-a-release-id",
                "discogs_release_id": None,
            }),
            ("conflicting", {
                "mb_release_id": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
                "discogs_release_id": "12856590",
            }),
            ("identityless", {
                "mb_release_id": None,
                "discogs_release_id": None,
            }),
        ):
            with self.subTest(label=label):
                raw = make_request_row(
                    id=42,
                    artist_name="Test Artist",
                    album_title="Wanted Album",
                    source="request",
                    status="wanted",
                    **identity_fields,
                )
                raw.update({
                    "has_captured_history": False,
                    "verified_lossless": False,
                    "provisional_lossless": False,
                })

                row = LibraryAlbumRow.from_pipeline_request(raw, track_count=0)

                self.assertIsNone(row.mb_albumid)
                self.assertEqual(row.source, "unknown")
                self.assertEqual(row.pipeline_id, 42)

    def test_from_beets_album_missing_bitrate_passes_zero_to_rank_fn(self) -> None:
        seen: list[tuple[str | None, int | None]] = []

        def rank_fn(fmt: str | None, kbps: int | None) -> str:
            seen.append((fmt, kbps))
            return "poor"

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
                "min_bitrate": None,
                "avg_bitrate": None,
                "type": "album",
                "label": "Test Label",
                "country": "US",
            },
            rank_fn=rank_fn,
        )

        self.assertEqual(seen, [("MP3", 0)])
        self.assertEqual(row.library_rank, "poor")

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
                    "created_at": datetime(2026, 4, 1, 3, 47, 54, tzinfo=UTC),
                    "search_filetype_override": None,
                    "target_format": None,
                    "has_captured_history": False,
                    "verified_lossless": False,
                    "provisional_lossless": False,
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
                    "has_captured_history": False,
                    "verified_lossless": False,
                    "provisional_lossless": False,
                },
                track_count=10,
            )

    def test_from_pipeline_request_rejects_missing_created_at(self) -> None:
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
                    "created_at": None,
                    "search_filetype_override": None,
                    "target_format": None,
                    "has_captured_history": False,
                    "verified_lossless": False,
                    "provisional_lossless": False,
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
                "avg_bitrate": 320000,
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
                "has_captured_history": False,
                "verified_lossless": False,
                "provisional_lossless": False,
            }
        )

        self.assertEqual(row.pipeline_status, "wanted")
        self.assertEqual(row.pipeline_id, 42)
        self.assertTrue(row.upgrade_queued)
        self.assertFalse(row.has_captured_history)
        self.assertFalse(row.pipeline_verified_lossless)
        self.assertFalse(row.pipeline_provisional)

    def test_wire_boundary_rejects_wrong_field_type(self) -> None:
        with self.assertRaises(msgspec.ValidationError):
            msgspec.convert(
                _valid_row_dict(id="7"),
                type=LibraryAlbumRow,
            )


class TestPipelineUpgradeQueued(unittest.TestCase):
    """`_pipeline_upgrade_queued` is the one owner both the list-row and
    detail projections call (issue #1355 item 6)."""

    CASES: tuple[tuple[str, dict[str, object] | None, bool], ...] = (
        ("no row", None, False),
        (
            "status not wanted",
            make_request_row(
                status="imported", search_filetype_override="flac",
                target_format=None,
            ),
            False,
        ),
        (
            "override only",
            make_request_row(
                status="wanted", search_filetype_override="flac",
                target_format=None,
            ),
            True,
        ),
        (
            "target format only",
            make_request_row(
                status="wanted", search_filetype_override=None,
                target_format="lossless",
            ),
            True,
        ),
        (
            "both override and target format",
            make_request_row(
                status="wanted", search_filetype_override="flac",
                target_format="lossless",
            ),
            True,
        ),
        (
            "neither override nor target format",
            make_request_row(
                status="wanted", search_filetype_override=None,
                target_format=None,
            ),
            False,
        ),
        (
            # The owner decides by truthiness (`x or y`); a present-but-
            # empty override must resolve exactly like an absent one, not
            # like a real value.
            "empty-string override is not an upgrade",
            make_request_row(
                status="wanted", search_filetype_override="",
                target_format=None,
            ),
            False,
        ),
        (
            "empty-string target format is not an upgrade",
            make_request_row(
                status="wanted", search_filetype_override=None,
                target_format="",
            ),
            False,
        ),
    )

    def test_branches(self) -> None:
        for desc, row, expected in self.CASES:
            with self.subTest(desc=desc):
                self.assertEqual(_pipeline_upgrade_queued(row), expected)
