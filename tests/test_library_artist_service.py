#!/usr/bin/env python3
"""Direct tests for the `/api/library/artist` merge / dedup seam."""

from __future__ import annotations

from datetime import datetime, timezone
import unittest

from tests.fakes import FakePipelineDB
from tests.helpers import make_request_row
from web.library_artist_service import (
    build_library_artist_rows,
    list_library_artist_rows,
)


ARTIST_ID = "664c3e0e-42d8-48c1-b209-1efca19c0325"
RELEASE_ID = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
RG_ID = "11111111-1111-1111-1111-111111111111"


def _rank(_fmt: str | None, _kbps: int | None) -> str:
    return "transparent"


def _beets_album(**overrides: object) -> dict[str, object]:
    album: dict[str, object] = {
        "id": 7,
        "album": "Test Album",
        "artist": "Test Artist",
        "year": 2024,
        "mb_albumid": RELEASE_ID,
        "discogs_albumid": None,
        "track_count": 10,
        "mb_releasegroupid": RG_ID,
        "release_group_title": "Test Album",
        "added": 1773651901.0,
        "formats": "MP3",
        "min_bitrate": 320000,
        "type": "album",
        "label": "Test Label",
        "country": "US",
    }
    album.update(overrides)
    return album


class _StubLibraryLookup:
    def __init__(self, albums: list[dict[str, object]]) -> None:
        self._albums = albums
        self.calls: list[tuple[str, str]] = []

    def get_library_artist(
        self,
        artist_name: str,
        mb_artist_id: str = "",
    ) -> list[dict[str, object]]:
        self.calls.append((artist_name, mb_artist_id))
        return list(self._albums)


class _RaceAwareLibraryLookup:
    def __init__(self) -> None:
        self.pipeline_read = False
        self.calls: list[str] = []

    def get_library_artist(
        self,
        artist_name: str,
        mb_artist_id: str = "",
    ) -> list[dict[str, object]]:
        self.calls.append(f"library:{artist_name}:{mb_artist_id}")
        if not self.pipeline_read:
            return []
        return [_beets_album()]


class _RaceAwarePipelineDB:
    def __init__(self, lookup: _RaceAwareLibraryLookup) -> None:
        self._lookup = lookup
        self.calls: list[str] = []

    def list_requests_by_artist(
        self,
        artist_name: str,
        mb_artist_id: str = "",
    ) -> list[dict[str, object]]:
        self.calls.append(f"pipeline:{artist_name}:{mb_artist_id}")
        self._lookup.pipeline_read = True
        return [make_request_row(
            id=42,
            mb_release_id=RELEASE_ID,
            artist_name="Test Artist",
            album_title="Test Album",
            status="wanted",
        )]

    def get_track_counts(self, request_ids: list[int]) -> dict[int, int]:
        self.calls.append(f"track_counts:{request_ids}")
        return {42: 10}


class TestLibraryArtistService(unittest.TestCase):
    def test_list_library_artist_rows_includes_pipeline_only_request(self) -> None:
        fake_db = FakePipelineDB()
        fake_db.seed_request(make_request_row(
            id=42,
            mb_release_id=RELEASE_ID,
            mb_release_group_id=RG_ID,
            mb_artist_id=ARTIST_ID,
            artist_name="Test Artist",
            album_title="Wanted Album",
            year=2024,
            country="US",
            format="CD",
            source="request",
            status="wanted",
            min_bitrate=320,
            created_at=datetime(2026, 4, 1, 3, 47, 54, tzinfo=timezone.utc),
            search_filetype_override="flac",
        ))
        fake_db.set_tracks(42, [
            {"track_number": i + 1, "title": f"Track {i + 1}"}
            for i in range(10)
        ])
        lookup = _StubLibraryLookup([])

        rows = list_library_artist_rows(
            library_lookup=lookup,
            pipeline_db=fake_db,
            artist_name="Test Artist",
            mb_artist_id=ARTIST_ID,
            rank_fn=_rank,
        )

        self.assertEqual(lookup.calls, [("Test Artist", ARTIST_ID)])
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].album, "Wanted Album")
        self.assertEqual(rows[0].track_count, 10)
        self.assertEqual(rows[0].pipeline_id, 42)
        self.assertFalse(rows[0].in_library)
        self.assertTrue(rows[0].upgrade_queued)

    def test_list_library_artist_rows_allows_missing_pipeline_db(self) -> None:
        lookup = _StubLibraryLookup([_beets_album()])

        rows = list_library_artist_rows(
            library_lookup=lookup,
            pipeline_db=None,
            artist_name="Test Artist",
            mb_artist_id=ARTIST_ID,
            rank_fn=_rank,
        )

        self.assertEqual(lookup.calls, [("Test Artist", ARTIST_ID)])
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].id, 7)
        self.assertTrue(rows[0].in_library)
        self.assertIsNone(rows[0].pipeline_id)

    def test_list_library_artist_rows_reads_pipeline_before_beets_lookup(self) -> None:
        lookup = _RaceAwareLibraryLookup()
        pipeline_db = _RaceAwarePipelineDB(lookup)

        rows = list_library_artist_rows(
            library_lookup=lookup,
            pipeline_db=pipeline_db,
            artist_name="Test Artist",
            mb_artist_id=ARTIST_ID,
            rank_fn=_rank,
        )

        self.assertEqual(
            pipeline_db.calls,
            [f"pipeline:Test Artist:{ARTIST_ID}", "track_counts:[42]"],
        )
        self.assertEqual(lookup.calls, [f"library:Test Artist:{ARTIST_ID}"])
        self.assertEqual(len(rows), 1)
        self.assertTrue(rows[0].in_library)
        self.assertEqual(rows[0].pipeline_id, 42)

    def test_build_library_artist_rows_rejects_non_int_request_id(self) -> None:
        with self.assertRaisesRegex(TypeError, "int id"):
            build_library_artist_rows(
                library_albums=[],
                pipeline_rows=[make_request_row(id="42")],
                track_counts={},
                rank_fn=_rank,
            )

    def test_build_library_artist_rows_keeps_pipeline_row_without_release_identity(self) -> None:
        rows = build_library_artist_rows(
            library_albums=[],
            pipeline_rows=[make_request_row(
                id=77,
                mb_release_id=None,
                discogs_release_id=None,
                artist_name="Test Artist",
                album_title="Unidentified Request",
                status="wanted",
            )],
            track_counts={77: 3},
            rank_fn=_rank,
        )

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].pipeline_id, 77)
        self.assertIsNone(rows[0].mb_albumid)
        self.assertFalse(rows[0].in_library)
        self.assertEqual(rows[0].track_count, 3)

    def test_build_library_artist_rows_overlays_pipeline_state_on_beets_row(self) -> None:
        rows = build_library_artist_rows(
            library_albums=[_beets_album()],
            pipeline_rows=[make_request_row(
                id=42,
                mb_release_id=RELEASE_ID,
                artist_name="Test Artist",
                album_title="Test Album",
                status="wanted",
                search_filetype_override="flac",
            )],
            track_counts={42: 10},
            rank_fn=_rank,
        )

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].id, 7)
        self.assertTrue(rows[0].in_library)
        self.assertEqual(rows[0].pipeline_id, 42)
        self.assertEqual(rows[0].pipeline_status, "wanted")
        self.assertTrue(rows[0].upgrade_queued)

    def test_build_library_artist_rows_dedups_discogs_pipeline_row(self) -> None:
        rows = build_library_artist_rows(
            library_albums=[_beets_album(
                id=8,
                album="Discogs Import",
                year=2001,
                mb_albumid=None,
                discogs_albumid="12856590",
                mb_releasegroupid=None,
                release_group_title="Discogs Import",
                added=1773651902.0,
                country="AU",
            )],
            pipeline_rows=[make_request_row(
                id=55,
                mb_release_id=None,
                discogs_release_id="12856590",
                artist_name="Test Artist",
                album_title="Discogs Import",
                source="request",
                status="wanted",
                created_at=datetime(2026, 4, 1, 3, 47, 54, tzinfo=timezone.utc),
            )],
            track_counts={55: 0},
            rank_fn=_rank,
        )

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].id, 8)
        self.assertEqual(rows[0].mb_albumid, "12856590")
        self.assertEqual(rows[0].pipeline_id, 55)
        self.assertTrue(rows[0].in_library)

    def test_build_library_artist_rows_merges_multiple_beets_and_pipeline_rows(self) -> None:
        rows = build_library_artist_rows(
            library_albums=[
                _beets_album(
                    id=1,
                    album="1995 Library Album",
                    year=1995,
                    mb_albumid="11111111-1111-1111-1111-111111111111",
                    release_group_title="1995 Library Album",
                    added=1773651800.0,
                ),
                _beets_album(
                    id=2,
                    album="2001 Library Album",
                    year=2001,
                    mb_albumid="22222222-2222-2222-2222-222222222222",
                    release_group_title="2001 Library Album",
                    added=1773651900.0,
                ),
            ],
            pipeline_rows=[
                make_request_row(
                    id=31,
                    mb_release_id="33333333-3333-3333-3333-333333333333",
                    artist_name="Test Artist",
                    album_title="1997 Pipeline Album",
                    year=1997,
                    status="wanted",
                ),
                make_request_row(
                    id=32,
                    mb_release_id="44444444-4444-4444-4444-444444444444",
                    artist_name="Test Artist",
                    album_title="2003 Pipeline Album",
                    year=2003,
                    status="wanted",
                ),
            ],
            track_counts={31: 9, 32: 11},
            rank_fn=_rank,
        )

        self.assertEqual([row.album for row in rows], [
            "1995 Library Album",
            "1997 Pipeline Album",
            "2001 Library Album",
            "2003 Pipeline Album",
        ])
        self.assertEqual(
            [row.pipeline_id for row in rows],
            [None, 31, None, 32],
        )

    def test_build_library_artist_rows_ignores_discogs_zero_sentinel(self) -> None:
        rows = build_library_artist_rows(
            library_albums=[_beets_album(
                id=10,
                album="Unknown Import",
                year=2002,
                mb_albumid="",
                discogs_albumid="0",
                track_count=8,
                mb_releasegroupid=None,
                release_group_title="Unknown Import",
                added=1773651904.0,
                min_bitrate=192000,
                country="AU",
            )],
            pipeline_rows=[],
            track_counts={},
            rank_fn=_rank,
        )

        self.assertEqual(len(rows), 1)
        self.assertIsNone(rows[0].mb_albumid)
        self.assertIsNone(rows[0].pipeline_id)

    def test_build_library_artist_rows_sorts_merged_rows(self) -> None:
        rows = build_library_artist_rows(
            library_albums=[_beets_album(
                id=9,
                album="Later Library Album",
                year=2005,
                track_count=11,
                release_group_title="Later Library Album",
                added=1773651903.0,
            )],
            pipeline_rows=[make_request_row(
                id=50,
                mb_release_id="bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
                mb_release_group_id="22222222-2222-2222-2222-222222222222",
                mb_artist_id=ARTIST_ID,
                artist_name="Test Artist",
                album_title="Older Request",
                year=1997,
                status="wanted",
                created_at=datetime(2026, 4, 1, 3, 47, 54, tzinfo=timezone.utc),
            )],
            track_counts={50: 0},
            rank_fn=_rank,
        )

        self.assertEqual([row.album for row in rows], [
            "Older Request",
            "Later Library Album",
        ])
