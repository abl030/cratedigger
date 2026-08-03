"""Direct tests for the `/api/library/artist` merge / dedup seam."""

from __future__ import annotations

import unittest
from datetime import UTC, datetime
from typing import cast

import msgspec

from lib.convergence_service import ConvergenceSignal
from lib.pipeline_db.rows import ArtistRequestRow
from lib.release_identity import ConflictingReleaseIdentityError
from tests.fakes import FakePipelineDB
from tests.helpers import make_request_row
from web.library_album_row import AmbiguousLibraryRequestAttachmentError
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
        "avg_bitrate": 320000,
        "type": "album",
        "label": "Test Label",
        "country": "US",
    }
    album.update(overrides)
    return album


def _artist_request(**overrides: object) -> dict[str, object]:
    row: dict[str, object] = make_request_row()
    row.update({
        "has_captured_history": False,
        "verified_lossless": False,
        "provisional_lossless": False,
    })
    row.update(overrides)
    return row


class _StubLibraryLookup:
    def __init__(
        self,
        albums: list[dict[str, object]],
        *,
        exact_albums: list[dict[str, object]] | None = None,
    ) -> None:
        self._albums = albums
        self._exact_albums = exact_albums if exact_albums is not None else albums
        self.calls: list[tuple[str, str]] = []

    def get_library_artist(
        self,
        artist_name: str,
        mb_artist_id: str = "",
    ) -> list[dict[str, object]]:
        self.calls.append((artist_name, mb_artist_id))
        return list(self._albums)

    def get_library_releases(
        self,
        release_ids: list[str],
    ) -> list[dict[str, object]]:
        wanted = set(release_ids)
        return [
            album for album in self._exact_albums
            if album.get("mb_albumid") in wanted
            or album.get("discogs_albumid") in wanted
        ]


class _FailingLibraryLookup:
    def __init__(self, calls: list[str]) -> None:
        self._calls = calls

    def get_library_artist(
        self,
        artist_name: str,
        mb_artist_id: str = "",
    ) -> list[dict[str, object]]:
        self._calls.append(f"library:{artist_name}:{mb_artist_id}")
        raise OSError("synthetic Beets read failure")

    def get_library_releases(
        self,
        release_ids: list[str],
    ) -> list[dict[str, object]]:
        raise AssertionError("artist-scoped Beets read should fail first")


class _RecordingPipelineDB:
    def __init__(self, calls: list[str], row: ArtistRequestRow) -> None:
        self._calls = calls
        self._row = row

    def list_requests_by_artist(
        self,
        artist_name: str,
        mb_artist_id: str = "",
    ) -> list[ArtistRequestRow]:
        self._calls.append(f"pipeline:{artist_name}:{mb_artist_id}")
        return [self._row]

    def get_track_counts(self, request_ids: list[int]) -> dict[int, int]:
        self._calls.append(f"track_counts:{request_ids}")
        return {int(self._row["id"]): 10}

    def list_library_request_candidates(
        self,
        release_ids: list[str],
    ) -> list[ArtistRequestRow]:
        self._calls.append(f"candidates:{release_ids}")
        return [self._row] if self._row["mb_release_id"] in release_ids else []

    def get_convergence_signals(
        self, request_ids: list[int] | None = None,
    ) -> dict[int, ConvergenceSignal]:
        self._calls.append(f"convergence:{request_ids}")
        return {}


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

    def get_library_releases(
        self,
        release_ids: list[str],
    ) -> list[dict[str, object]]:
        self.calls.append(f"releases:{release_ids}")
        return [_beets_album()] if RELEASE_ID in release_ids else []


class _RaceAwarePipelineDB:
    def __init__(self, lookup: _RaceAwareLibraryLookup) -> None:
        self._lookup = lookup
        self.calls: list[str] = []

    def list_requests_by_artist(
        self,
        artist_name: str,
        mb_artist_id: str = "",
    ) -> list[ArtistRequestRow]:
        self.calls.append(f"pipeline:{artist_name}:{mb_artist_id}")
        self._lookup.pipeline_read = True
        return cast("list[ArtistRequestRow]", [_artist_request(
            id=42,
            mb_release_id=RELEASE_ID,
            artist_name="Test Artist",
            album_title="Test Album",
            status="wanted",
        )])

    def get_track_counts(self, request_ids: list[int]) -> dict[int, int]:
        self.calls.append(f"track_counts:{request_ids}")
        return {42: 10}

    def list_library_request_candidates(
        self,
        release_ids: list[str],
    ) -> list[ArtistRequestRow]:
        self.calls.append(f"candidates:{release_ids}")
        request = msgspec.convert(_artist_request(
            id=42,
            mb_release_id=RELEASE_ID,
            artist_name="Test Artist",
            album_title="Test Album",
            status="wanted",
            processing_owner=None,
        ), type=ArtistRequestRow)
        return [request] if RELEASE_ID in release_ids else []

    def get_convergence_signals(
        self, request_ids: list[int] | None = None,
    ) -> dict[int, ConvergenceSignal]:
        self.calls.append(f"convergence:{request_ids}")
        return {}


class TestLibraryArtistService(unittest.TestCase):
    def test_list_library_artist_rows_includes_pipeline_only_request(self) -> None:
        fake_db = FakePipelineDB()
        fake_db.seed_request(_artist_request(
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
            created_at=datetime(2026, 4, 1, 3, 47, 54, tzinfo=UTC),
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

    def test_pipeline_only_capture_keeps_history_proof_and_missing_presence_separate(
        self,
    ) -> None:
        rows = build_library_artist_rows(
            library_albums=[],
            pipeline_rows=[_artist_request(
                id=42,
                mb_release_id=RELEASE_ID,
                artist_name="Test Artist",
                album_title="Captured Album",
                status="wanted",
                has_captured_history=True,
                verified_lossless=True,
                provisional_lossless=False,
            )],
            track_counts={42: 10},
            rank_fn=_rank,
        )

        self.assertEqual(len(rows), 1)
        self.assertFalse(rows[0].in_library)
        self.assertTrue(rows[0].has_captured_history)
        self.assertTrue(rows[0].pipeline_verified_lossless)
        self.assertFalse(rows[0].pipeline_provisional)
        self.assertEqual(rows[0].pipeline_status, "wanted")
        self.assertIsNone(rows[0].library_rank)

    def test_beets_only_album_is_held_and_untracked_without_capture_inference(
        self,
    ) -> None:
        rows = build_library_artist_rows(
            library_albums=[_beets_album()],
            pipeline_rows=[],
            track_counts={},
            rank_fn=_rank,
        )

        self.assertEqual(len(rows), 1)
        self.assertTrue(rows[0].in_library)
        self.assertIsNone(rows[0].pipeline_id)
        self.assertFalse(rows[0].has_captured_history)
        self.assertFalse(rows[0].pipeline_verified_lossless)
        self.assertFalse(rows[0].pipeline_provisional)

    def test_dual_tagged_album_attaches_discogs_request_once(self) -> None:
        rows = build_library_artist_rows(
            library_albums=[_beets_album(discogs_albumid="12856590")],
            pipeline_rows=[_artist_request(
                id=42,
                mb_release_id=None,
                discogs_release_id="12856590",
                artist_name="Test Artist",
                album_title="Test Album",
                status="wanted",
                has_captured_history=True,
                verified_lossless=True,
                provisional_lossless=False,
            )],
            track_counts={42: 10},
            rank_fn=_rank,
        )

        self.assertEqual(len(rows), 1)
        self.assertTrue(rows[0].in_library)
        self.assertEqual(rows[0].to_dict()["mb_albumid"], "12856590")
        self.assertEqual(rows[0].source, "discogs")
        self.assertEqual(rows[0].pipeline_id, 42)
        self.assertTrue(rows[0].has_captured_history)
        self.assertTrue(rows[0].pipeline_verified_lossless)

    def test_dual_tagged_album_rejects_two_exact_request_attachments(self) -> None:
        with self.assertRaisesRegex(
            AmbiguousLibraryRequestAttachmentError,
            "musicbrainz.*discogs",
        ):
            build_library_artist_rows(
                library_albums=[_beets_album(discogs_albumid="12856590")],
                pipeline_rows=[
                    _artist_request(
                        id=42,
                        mb_release_id=RELEASE_ID,
                        discogs_release_id=None,
                        artist_name="Test Artist",
                        album_title="Test Album",
                        status="wanted",
                    ),
                    _artist_request(
                        id=43,
                        mb_release_id=None,
                        discogs_release_id="12856590",
                        artist_name="Test Artist",
                        album_title="Test Album",
                        status="wanted",
                    ),
                ],
                track_counts={42: 10, 43: 10},
                rank_fn=_rank,
            )

    def test_duplicate_same_identity_requests_are_ambiguous(self) -> None:
        with self.assertRaises(AmbiguousLibraryRequestAttachmentError) as raised:
            build_library_artist_rows(
                library_albums=[_beets_album(
                    mb_albumid=None,
                    discogs_albumid="12856590",
                )],
                pipeline_rows=[
                    _artist_request(
                        id=request_id,
                        mb_release_id=None,
                        discogs_release_id="12856590",
                        artist_name="Test Artist",
                        album_title="Test Album",
                        status="wanted",
                    )
                    for request_id in (42, 43)
                ],
                track_counts={42: 10, 43: 10},
                rank_fn=_rank,
            )

        self.assertEqual(raised.exception.request_ids, (42, 43))

    def test_malformed_pipeline_rows_stay_visible_but_unattached(self) -> None:
        malformed_rows = [
            _artist_request(
                id=42,
                mb_release_id="not-a-release-id",
                discogs_release_id=None,
                artist_name="Test Artist",
                album_title="Malformed",
                status="wanted",
            ),
            _artist_request(
                id=43,
                mb_release_id=RELEASE_ID,
                discogs_release_id="12856590",
                artist_name="Test Artist",
                album_title="Conflicting",
                status="wanted",
            ),
            _artist_request(
                id=44,
                mb_release_id=None,
                discogs_release_id=None,
                artist_name="Test Artist",
                album_title="Identityless",
                status="wanted",
            ),
        ]

        rows = build_library_artist_rows(
            library_albums=[_beets_album()],
            pipeline_rows=malformed_rows,
            track_counts={42: 1, 43: 1, 44: 1},
            rank_fn=_rank,
        )

        self.assertEqual(len(rows), 4)
        library_row = next(row for row in rows if row.in_library)
        self.assertIsNone(library_row.pipeline_id)
        for row in (row for row in rows if not row.in_library):
            self.assertIsNone(row.mb_albumid)
            self.assertEqual(row.source, "unknown")
            self.assertIsNotNone(row.pipeline_id)

    def test_conflicting_numeric_beets_identity_never_renders(self) -> None:
        with self.assertRaisesRegex(
            ConflictingReleaseIdentityError,
            "12856590 != 12856591",
        ):
            build_library_artist_rows(
                library_albums=[_beets_album(
                    mb_albumid="12856590",
                    discogs_albumid="12856591",
                )],
                pipeline_rows=[],
                track_counts={},
                rank_fn=_rank,
            )

    def test_replaced_request_remains_an_exact_pipeline_history_row(self) -> None:
        rows = build_library_artist_rows(
            library_albums=[],
            pipeline_rows=[_artist_request(
                id=77,
                mb_release_id=RELEASE_ID,
                artist_name="Test Artist",
                album_title="Superseded Pressing",
                status="replaced",
                has_captured_history=True,
                verified_lossless=True,
                provisional_lossless=False,
            )],
            track_counts={77: 10},
            rank_fn=_rank,
        )

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].pipeline_status, "replaced")
        self.assertTrue(rows[0].has_captured_history)
        self.assertFalse(rows[0].in_library)

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
            [
                f"pipeline:Test Artist:{ARTIST_ID}",
                "track_counts:[42]",
                f"candidates:['{RELEASE_ID}']",
                "convergence:[42]",
            ],
        )
        self.assertEqual(lookup.calls, [f"library:Test Artist:{ARTIST_ID}"])
        self.assertEqual(len(rows), 1)
        self.assertTrue(rows[0].in_library)
        self.assertEqual(rows[0].pipeline_id, 42)

    def test_exact_release_beats_drifted_beets_artist_tags(self) -> None:
        fake_db = FakePipelineDB()
        fake_db.seed_request(_artist_request(
            id=42,
            mb_release_id=RELEASE_ID,
            mb_artist_id=ARTIST_ID,
            artist_name="Test Artist",
            album_title="Test Album",
            status="wanted",
        ))
        lookup = _StubLibraryLookup(
            [],
            exact_albums=[_beets_album(artist="Drifted Beets Tag")],
        )

        rows = list_library_artist_rows(
            library_lookup=lookup,
            pipeline_db=fake_db,
            artist_name="Test Artist",
            mb_artist_id=ARTIST_ID,
            rank_fn=_rank,
        )

        self.assertEqual(len(rows), 1)
        self.assertTrue(rows[0].in_library)
        self.assertEqual(rows[0].pipeline_id, 42)
        self.assertEqual(rows[0].artist, "Drifted Beets Tag")

    def test_exact_release_beats_drifted_request_artist_metadata(self) -> None:
        fake_db = FakePipelineDB()
        fake_db.seed_request(_artist_request(
            id=43,
            mb_release_id=RELEASE_ID,
            mb_artist_id="22222222-2222-4222-8222-222222222222",
            artist_name="Old Request Tag",
            album_title="Test Album",
            status="wanted",
            processing_owner=None,
            has_captured_history=True,
        ))
        fake_db.log_download(43, outcome="success")
        lookup = _StubLibraryLookup([_beets_album()])

        rows = list_library_artist_rows(
            library_lookup=lookup,
            pipeline_db=fake_db,
            artist_name="Test Artist",
            mb_artist_id=ARTIST_ID,
            rank_fn=_rank,
        )

        self.assertEqual(len(rows), 1)
        self.assertTrue(rows[0].in_library)
        self.assertEqual(rows[0].pipeline_id, 43)
        self.assertTrue(rows[0].has_captured_history)

    def test_batch_candidates_expose_drifted_duplicate_request(self) -> None:
        fake_db = FakePipelineDB()
        fake_db.seed_request(_artist_request(
            id=43,
            mb_release_id=None,
            discogs_release_id="12856590",
            mb_artist_id=ARTIST_ID,
            artist_name="Test Artist",
            album_title="Test Album",
            status="wanted",
        ))
        fake_db.seed_request(_artist_request(
            id=44,
            mb_release_id=None,
            discogs_release_id="12856590",
            mb_artist_id="22222222-2222-4222-8222-222222222222",
            artist_name="Old Request Tag",
            album_title="Test Album",
            status="wanted",
        ))
        lookup = _StubLibraryLookup([_beets_album(
            mb_albumid=None,
            discogs_albumid="12856590",
        )])

        with self.assertRaises(AmbiguousLibraryRequestAttachmentError) as raised:
            list_library_artist_rows(
                library_lookup=lookup,
                pipeline_db=fake_db,
                artist_name="Test Artist",
                mb_artist_id=ARTIST_ID,
                rank_fn=_rank,
            )

        self.assertEqual(raised.exception.request_ids, (43, 44))

    def test_beets_read_failure_propagates_after_pipeline_snapshot(self) -> None:
        calls: list[str] = []
        request = msgspec.convert(_artist_request(
            id=42,
            mb_release_id=RELEASE_ID,
            artist_name="Test Artist",
            album_title="Captured Album",
            status="wanted",
            processing_owner=None,
            has_captured_history=True,
            verified_lossless=True,
            provisional_lossless=False,
        ), type=ArtistRequestRow)
        pipeline_db = _RecordingPipelineDB(calls, request)

        with self.assertRaisesRegex(OSError, "Beets read failure"):
            list_library_artist_rows(
                library_lookup=_FailingLibraryLookup(calls),
                pipeline_db=pipeline_db,
                artist_name="Test Artist",
                mb_artist_id=ARTIST_ID,
                rank_fn=_rank,
            )

        self.assertEqual(calls, [
            f"pipeline:Test Artist:{ARTIST_ID}",
            "track_counts:[42]",
            f"library:Test Artist:{ARTIST_ID}",
        ])

    def test_build_library_artist_rows_rejects_non_int_request_id(self) -> None:
        with self.assertRaisesRegex(TypeError, "int id"):
            build_library_artist_rows(
                library_albums=[],
                pipeline_rows=[_artist_request(id="42")],
                track_counts={},
                rank_fn=_rank,
            )

    def test_build_library_artist_rows_keeps_pipeline_row_without_release_identity(self) -> None:
        rows = build_library_artist_rows(
            library_albums=[],
            pipeline_rows=[_artist_request(
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
            pipeline_rows=[_artist_request(
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
            pipeline_rows=[_artist_request(
                id=55,
                mb_release_id=None,
                discogs_release_id="12856590",
                artist_name="Test Artist",
                album_title="Discogs Import",
                source="request",
                status="wanted",
                created_at=datetime(2026, 4, 1, 3, 47, 54, tzinfo=UTC),
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
                _artist_request(
                    id=31,
                    mb_release_id="33333333-3333-3333-3333-333333333333",
                    artist_name="Test Artist",
                    album_title="1997 Pipeline Album",
                    year=1997,
                    status="wanted",
                ),
                _artist_request(
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
            pipeline_rows=[_artist_request(
                id=50,
                mb_release_id="bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
                mb_release_group_id="22222222-2222-2222-2222-222222222222",
                mb_artist_id=ARTIST_ID,
                artist_name="Test Artist",
                album_title="Older Request",
                year=1997,
                status="wanted",
                created_at=datetime(2026, 4, 1, 3, 47, 54, tzinfo=UTC),
            )],
            track_counts={50: 0},
            rank_fn=_rank,
        )

        self.assertEqual([row.album for row in rows], [
            "Older Request",
            "Later Library Album",
        ])
