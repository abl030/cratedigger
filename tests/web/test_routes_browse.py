#!/usr/bin/env python3
"""Contract tests for web/routes/browse.py: search, browse, library artist.

Split from tests/test_web_server.py (#408). Shared harness in
tests/web/_harness.py.
"""

from datetime import datetime, timezone
import email.message
import os
import sys
import tempfile
import unittest
from unittest.mock import patch
from urllib.error import HTTPError


sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from tests.web._harness import _assert_required_fields, _FakeDbWebServerCase

from tests.fakes import FakeBeetsDB, FakePipelineDB
from tests.helpers import make_request_row
from web.library_album_row import LibraryAlbumRow


class TestBrowseRouteContracts(_FakeDbWebServerCase):
    """Contract tests for browse and MusicBrainz-backed routes."""

    ARTIST_SEARCH_REQUIRED_FIELDS = {"id", "name", "disambiguation"}
    RELEASE_SEARCH_REQUIRED_FIELDS = {
        "id", "title", "artist_id", "artist_name", "primary_type",
    }
    ARTIST_RG_REQUIRED_FIELDS = {
        "id", "title", "type", "secondary_types", "first_release_date",
        "artist_credit", "primary_artist_id", "has_official",
    }
    LIBRARY_ALBUM_REQUIRED_FIELDS = set(LibraryAlbumRow.__struct_fields__)
    RELEASE_GROUP_REQUIRED_FIELDS = {
        "id", "title", "country", "date", "format", "track_count", "status",
        "in_library", "beets_album_id", "pipeline_status", "pipeline_id",
    }
    RELEASE_DETAIL_REQUIRED_FIELDS = {
        "id", "title", "tracks", "in_library", "beets_album_id",
        "pipeline_status", "pipeline_id",
    }
    RELEASE_TRACK_REQUIRED_FIELDS = {
        "disc_number", "track_number", "title", "length_seconds",
    }
    DISAMBIGUATE_RESPONSE_REQUIRED_FIELDS = {
        "artist_id", "artist_name", "release_groups",
    }
    DISAMBIGUATE_RG_REQUIRED_FIELDS = {
        "release_group_id", "title", "primary_type", "first_date",
        "release_ids", "pressings", "track_count", "unique_track_count",
        "covered_by", "library_status", "pipeline_status", "pipeline_id",
        "tracks",
    }
    DISAMBIGUATE_PRESSING_REQUIRED_FIELDS = {
        "release_id", "title", "date", "format", "track_count", "country",
        "recording_ids", "in_library", "beets_album_id", "pipeline_status",
        "pipeline_id",
    }
    DISAMBIGUATE_TRACK_REQUIRED_FIELDS = {
        "recording_id", "title", "unique", "also_on",
    }

    ARTIST_ID = "664c3e0e-42d8-48c1-b209-1efca19c0325"
    RELEASE_ID = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
    RG_ID = "11111111-1111-1111-1111-111111111111"

    def test_artist_search_contract(self):
        with patch("web.server.mb_api") as mock_mb:
            mock_mb.search_artists.return_value = [
                {"id": self.ARTIST_ID, "name": "Test Artist", "disambiguation": ""},
            ]
            status, data = self._get("/api/search?q=test")

        self.assertEqual(status, 200)
        _assert_required_fields(self, data, {"artists"}, "artist search response")
        _assert_required_fields(self, data["artists"][0], self.ARTIST_SEARCH_REQUIRED_FIELDS,
                                "artist search result")

    def test_release_search_contract(self):
        with patch("web.server.mb_api") as mock_mb:
            mock_mb.search_release_groups.return_value = [
                {
                    "id": self.RG_ID,
                    "title": "Test Album",
                    "artist_id": self.ARTIST_ID,
                    "artist_name": "Test Artist",
                    "primary_type": "Album",
                },
            ]
            status, data = self._get("/api/search?q=test&type=release")

        self.assertEqual(status, 200)
        _assert_required_fields(self, data, {"release_groups"}, "release search response")
        _assert_required_fields(self, data["release_groups"][0],
                                self.RELEASE_SEARCH_REQUIRED_FIELDS,
                                "release search result")

    def test_library_artist_route_contract(self):
        album = {
            "id": 7,
            "album": "Test Album",
            "artist": "Test Artist",
            "year": 2024,
            "mb_albumid": self.RELEASE_ID,
            "track_count": 10,
            "mb_releasegroupid": self.RG_ID,
            "release_group_title": "Test Album",
            "added": 1773651901.0,
            "formats": "MP3",
            "min_bitrate": 320000,
            "type": "album",
            "label": "Test Label",
            "country": "US",
            "source": "musicbrainz",
        }
        with patch("web.server.get_library_artist", return_value=[album]):
            status, data = self._get(
                f"/api/library/artist?name=Test%20Artist&mbid={self.ARTIST_ID}"
            )

        self.assertEqual(status, 200)
        _assert_required_fields(self, data, {"albums"}, "library artist response")
        _assert_required_fields(self, data["albums"][0], self.LIBRARY_ALBUM_REQUIRED_FIELDS,
                                "library artist album")

    def test_library_artist_route_includes_pipeline_only_requests(self):
        import web.server as srv

        fake_db = FakePipelineDB()
        fake_db.seed_request(make_request_row(
            id=42,
            mb_release_id=self.RELEASE_ID,
            mb_release_group_id=self.RG_ID,
            mb_artist_id=self.ARTIST_ID,
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

        with patch.object(srv, "db", fake_db), \
                patch("web.server.get_library_artist", return_value=[]):
            status, data = self._get(
                f"/api/library/artist?name=Test%20Artist&mbid={self.ARTIST_ID}"
            )

        self.assertEqual(status, 200)
        self.assertEqual(len(data["albums"]), 1)
        _assert_required_fields(self, data["albums"][0], self.LIBRARY_ALBUM_REQUIRED_FIELDS,
                                "pipeline-only library artist album")
        self.assertEqual(data["albums"][0]["album"], "Wanted Album")
        self.assertFalse(data["albums"][0]["in_library"])
        self.assertEqual(data["albums"][0]["pipeline_status"], "wanted")
        self.assertEqual(data["albums"][0]["pipeline_id"], 42)
        self.assertIsNone(data["albums"][0]["beets_album_id"])
        self.assertIsNone(data["albums"][0]["library_rank"])
        self.assertEqual(data["albums"][0]["release_group_title"], "Wanted Album")

    def test_library_artist_route_dedups_pipeline_row_when_beets_row_has_same_release_id(self):
        import web.server as srv

        fake_db = FakePipelineDB()
        fake_db.seed_request(make_request_row(
            id=42,
            mb_release_id=self.RELEASE_ID,
            mb_release_group_id=self.RG_ID,
            mb_artist_id=self.ARTIST_ID,
            artist_name="Test Artist",
            album_title="Duplicate Pipeline Row",
            status="wanted",
            created_at=datetime(2026, 4, 1, 3, 47, 54, tzinfo=timezone.utc),
        ))
        beets_album = {
            "id": 7,
            "album": "Test Album",
            "artist": "Test Artist",
            "year": 2024,
            "mb_albumid": self.RELEASE_ID,
            "track_count": 10,
            "mb_releasegroupid": self.RG_ID,
            "release_group_title": "Test Album",
            "added": 1773651901.0,
            "formats": "MP3",
            "min_bitrate": 320000,
            "type": "album",
            "label": "Test Label",
            "country": "US",
            "source": "musicbrainz",
        }

        with patch.object(srv, "db", fake_db), \
                patch("web.server.get_library_artist", return_value=[beets_album]):
            status, data = self._get(
                f"/api/library/artist?name=Test%20Artist&mbid={self.ARTIST_ID}"
            )

        self.assertEqual(status, 200)
        self.assertEqual(len(data["albums"]), 1)
        self.assertEqual(data["albums"][0]["id"], 7)
        self.assertEqual(data["albums"][0]["pipeline_id"], 42)
        self.assertTrue(data["albums"][0]["in_library"])

    def test_library_artist_route_dedups_discogs_pipeline_row_when_beets_row_has_same_discogs_id(self):
        import web.server as srv

        fake_db = FakePipelineDB()
        fake_db.seed_request(make_request_row(
            id=55,
            mb_release_id=None,
            discogs_release_id="12856590",
            mb_artist_id=None,
            artist_name="Test Artist",
            album_title="Discogs Import",
            source="request",
            status="wanted",
            created_at=datetime(2026, 4, 1, 3, 47, 54, tzinfo=timezone.utc),
        ))
        beets_album = {
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
            "source": "discogs",
        }

        with patch.object(srv, "db", fake_db), \
                patch("web.server.get_library_artist", return_value=[beets_album]):
            status, data = self._get(
                f"/api/library/artist?name=Test%20Artist&mbid={self.ARTIST_ID}"
            )

        self.assertEqual(status, 200)
        self.assertEqual(len(data["albums"]), 1)
        self.assertEqual(data["albums"][0]["id"], 8)
        self.assertEqual(data["albums"][0]["mb_albumid"], "12856590")
        self.assertEqual(data["albums"][0]["pipeline_id"], 55)
        self.assertTrue(data["albums"][0]["in_library"])

    def test_library_artist_route_ignores_discogs_zero_sentinel_on_blank_row(self):
        import web.server as srv

        beets_album = {
            "id": 10,
            "album": "Unknown Import",
            "artist": "Test Artist",
            "year": 2002,
            "mb_albumid": "",
            "discogs_albumid": "0",
            "track_count": 8,
            "mb_releasegroupid": None,
            "release_group_title": "Unknown Import",
            "added": 1773651904.0,
            "formats": "MP3",
            "min_bitrate": 192000,
            "type": "album",
            "label": "Test Label",
            "country": "AU",
            "source": "unknown",
        }

        with patch.object(srv, "db", FakePipelineDB()), \
                patch("web.server.get_library_artist", return_value=[beets_album]):
            status, data = self._get(
                f"/api/library/artist?name=Test%20Artist&mbid={self.ARTIST_ID}"
            )

        self.assertEqual(status, 200)
        self.assertEqual(len(data["albums"]), 1)
        self.assertIsNone(data["albums"][0]["mb_albumid"])
        self.assertIsNone(data["albums"][0]["pipeline_id"])

    def test_library_artist_route_sorts_merged_rows_after_dedup(self):
        import web.server as srv

        fake_db = FakePipelineDB()
        fake_db.seed_request(make_request_row(
            id=50,
            mb_release_id="bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
            mb_release_group_id="22222222-2222-2222-2222-222222222222",
            mb_artist_id=self.ARTIST_ID,
            artist_name="Test Artist",
            album_title="Older Request",
            year=1997,
            status="wanted",
            created_at=datetime(2026, 4, 1, 3, 47, 54, tzinfo=timezone.utc),
        ))
        beets_album = {
            "id": 9,
            "album": "Later Library Album",
            "artist": "Test Artist",
            "year": 2005,
            "mb_albumid": self.RELEASE_ID,
            "track_count": 11,
            "mb_releasegroupid": self.RG_ID,
            "release_group_title": "Later Library Album",
            "added": 1773651903.0,
            "formats": "MP3",
            "min_bitrate": 320000,
            "type": "album",
            "label": "Test Label",
            "country": "US",
            "source": "musicbrainz",
        }

        with patch.object(srv, "db", fake_db), \
                patch("web.server.get_library_artist", return_value=[beets_album]):
            status, data = self._get(
                f"/api/library/artist?name=Test%20Artist&mbid={self.ARTIST_ID}"
            )

        self.assertEqual(status, 200)
        self.assertEqual([row["album"] for row in data["albums"]], [
            "Older Request",
            "Later Library Album",
        ])

    def test_artist_compare_contract(self):
        """Compare endpoint returns mb_artist, discogs_artist, and three buckets."""
        mb_rg = {
            "id": self.RG_ID,
            "title": "OK Computer",
            "type": "Album",
            "secondary_types": [],
            "first_release_date": "1997-05-21",
            "artist_credit": "Radiohead",
            "primary_artist_id": self.ARTIST_ID,
        }
        discogs_rg = {
            "id": "21491",
            "title": "OK Computer",
            "type": "Album",
            "secondary_types": [],
            "first_release_date": "1997",
            "artist_credit": "Radiohead",
            "primary_artist_id": "3840",
        }
        with patch("web.server.mb_api") as mock_mb, \
                patch("web.routes.browse.discogs_api") as mock_dg:
            mock_mb.search_artists.return_value = [{"id": self.ARTIST_ID, "name": "Radiohead"}]
            mock_mb.get_artist_release_groups.return_value = [mb_rg]
            mock_mb.get_official_release_group_ids.return_value = {self.RG_ID}
            mock_mb.get_artist_name.return_value = "Radiohead"
            mock_dg.search_artists.return_value = [{"id": "3840", "name": "Radiohead"}]
            mock_dg.get_artist_releases.return_value = [discogs_rg]
            mock_dg.get_artist_name.return_value = "Radiohead"
            status, data = self._get("/api/artist/compare?name=Radiohead")

        self.assertEqual(status, 200)
        _assert_required_fields(
            self, data,
            {"mb_artist", "discogs_artist", "both", "mb_only", "discogs_only"},
            "artist compare response",
        )
        # Same title + same year → matched
        self.assertEqual(len(data["both"]), 1)
        self.assertEqual(data["mb_only"], [])
        self.assertEqual(data["discogs_only"], [])
        self.assertEqual(data["both"][0]["mb"]["id"], self.RG_ID)
        self.assertEqual(data["both"][0]["discogs"]["id"], "21491")
        # Bootleg classification flows through to frontend.
        self.assertTrue(data["both"][0]["mb"]["has_official"])

    def test_artist_compare_marks_bootleg_only_rgs(self):
        """Release groups absent from get_official_release_group_ids land
        with has_official=False so the frontend can route them into the
        Bootleg-only collapsible section."""
        official_rg = {
            "id": self.RG_ID, "title": "Real Album", "type": "Album",
            "secondary_types": [], "first_release_date": "1997",
            "artist_credit": "Artist", "primary_artist_id": self.ARTIST_ID,
        }
        bootleg_rg = {
            "id": "00000000-0000-0000-0000-000000000099",
            "title": "Live Bootleg 99", "type": "Album",
            "secondary_types": [], "first_release_date": "1999",
            "artist_credit": "Artist", "primary_artist_id": self.ARTIST_ID,
        }
        with patch("web.server.mb_api") as mock_mb, \
                patch("web.routes.browse.discogs_api") as mock_dg:
            mock_mb.search_artists.return_value = [{"id": self.ARTIST_ID, "name": "Artist"}]
            mock_mb.get_artist_release_groups.return_value = [official_rg, bootleg_rg]
            mock_mb.get_official_release_group_ids.return_value = {self.RG_ID}
            mock_mb.get_artist_name.return_value = "Artist"
            mock_dg.search_artists.return_value = []
            mock_dg.get_artist_releases.return_value = []
            mock_dg.get_artist_name.return_value = ""
            status, data = self._get("/api/artist/compare?name=Artist")

        self.assertEqual(status, 200)
        # Both RGs land in mb_only (no Discogs counterpart). Both carry
        # has_official so the frontend can split them.
        self.assertEqual(len(data["mb_only"]), 2)
        by_id = {r["id"]: r for r in data["mb_only"]}
        self.assertTrue(by_id[self.RG_ID]["has_official"])
        self.assertFalse(by_id["00000000-0000-0000-0000-000000000099"]["has_official"])

    def test_artist_release_groups_contract(self):
        release_group = {
            "id": self.RG_ID,
            "title": "Test Album",
            "type": "Album",
            "secondary_types": [],
            "first_release_date": "2024-01-01",
            "artist_credit": "Test Artist",
            "primary_artist_id": self.ARTIST_ID,
        }
        with patch("web.server.mb_api") as mock_mb:
            mock_mb.get_artist_release_groups.return_value = [release_group]
            mock_mb.get_official_release_group_ids.return_value = {self.RG_ID}
            status, data = self._get(f"/api/artist/{self.ARTIST_ID}")

        self.assertEqual(status, 200)
        _assert_required_fields(self, data, {"release_groups"}, "artist response")
        _assert_required_fields(self, data["release_groups"][0], self.ARTIST_RG_REQUIRED_FIELDS,
                                "artist release group")

    def test_artist_release_groups_in_library_when_name_passed(self):
        """When the frontend passes ?name=, each RG gets in_library: bool
        based on a beets lookup. Without name, the field stays absent
        (backwards-compatible)."""
        release_group = {
            "id": self.RG_ID, "title": "Owned Album", "type": "Album",
            "secondary_types": [], "first_release_date": "2024",
            "artist_credit": "Test Artist", "primary_artist_id": self.ARTIST_ID,
        }
        owned_album = {
            "mb_albumid": "00000000-0000-0000-0000-000000000001",
            "mb_releasegroupid": self.RG_ID,
            "album": "Owned Album",
        }
        with patch("web.server.mb_api") as mock_mb, \
                patch("web.server.get_library_artist", return_value=[owned_album]):
            mock_mb.get_artist_release_groups.return_value = [release_group]
            mock_mb.get_official_release_group_ids.return_value = {self.RG_ID}
            status, data = self._get(
                f"/api/artist/{self.ARTIST_ID}?name=Test%20Artist"
            )

        self.assertEqual(status, 200)
        self.assertTrue(data["release_groups"][0]["in_library"])

    def test_release_group_contract(self):
        release = {
            "id": self.RELEASE_ID,
            "title": "Test Album",
            "country": "US",
            "date": "2024-01-01",
            "format": "CD",
            "track_count": 10,
            "status": "Official",
        }
        beets_db = FakeBeetsDB()
        beets_db.set_album_ids_for_release(self.RELEASE_ID, [7])
        beets_db.set_mbid_detail(self.RELEASE_ID, {})
        with patch("web.server.mb_api") as mock_mb, \
                patch("web.server.check_beets_library", return_value={self.RELEASE_ID}), \
                patch("web.server._beets_db", return_value=beets_db), \
                patch("web.server.check_pipeline",
                      return_value={self.RELEASE_ID: {"id": 42, "status": "wanted"}}):
            mock_mb.get_release_group_releases.return_value = {"releases": [release]}
            status, data = self._get(f"/api/release-group/{self.RG_ID}")

        self.assertEqual(status, 200)
        _assert_required_fields(self, data, {"releases"}, "release group response")
        _assert_required_fields(self, data["releases"][0], self.RELEASE_GROUP_REQUIRED_FIELDS,
                                "release group release")
        self.assertEqual(data["releases"][0]["beets_album_id"], 7)

    def test_release_detail_contract(self):
        release = {
            "id": self.RELEASE_ID,
            "title": "Test Album",
            "tracks": [
                {
                    "disc_number": 1,
                    "track_number": 1,
                    "title": "Track",
                    "length_seconds": 180,
                },
            ],
        }
        beets_db = FakeBeetsDB()
        beets_db.set_album_ids_for_release(self.RELEASE_ID, [7])
        beets_db.set_mbid_detail(self.RELEASE_ID, {})
        self.db.seed_request(make_request_row(
            id=42, status="wanted", mb_release_id=self.RELEASE_ID,
        ))
        with patch("web.server.mb_api") as mock_mb, \
                patch("web.server.check_beets_library", return_value={self.RELEASE_ID}), \
                patch("web.server._beets_db", return_value=beets_db):
            mock_mb.get_release.return_value = release
            status, data = self._get(f"/api/release/{self.RELEASE_ID}")

        self.assertEqual(status, 200)
        _assert_required_fields(self, data, self.RELEASE_DETAIL_REQUIRED_FIELDS,
                                "release detail response")
        _assert_required_fields(self, data["tracks"][0], self.RELEASE_TRACK_REQUIRED_FIELDS,
                                "release detail track")
        self.assertEqual(data["beets_album_id"], 7)

    def test_release_detail_includes_beets_tracks_when_in_library(self):
        """In-library release with beets item rows → the payload carries
        ``beets_tracks`` (the per-track format/bitrate table the
        frontend renders under the release)."""
        release = {
            "id": self.RELEASE_ID,
            "title": "Test Album",
            "tracks": [{"disc_number": 1, "track_number": 1,
                        "title": "Track", "length_seconds": 180}],
        }
        beets_track = {
            "title": "Track", "track": 1, "disc": 1, "length": 180.0,
            "format": "FLAC", "bitrate": 1100000,
            "samplerate": 44100, "bitdepth": 16,
        }
        beets_db = FakeBeetsDB()
        beets_db.set_album_ids_for_release(self.RELEASE_ID, [7])
        beets_db.set_mbid_detail(self.RELEASE_ID, {})
        beets_db.set_tracks_for_release(self.RELEASE_ID, [beets_track])
        with patch("web.server.mb_api") as mock_mb, \
                patch("web.server.check_beets_library", return_value={self.RELEASE_ID}), \
                patch("web.server._beets_db", return_value=beets_db):
            mock_mb.get_release.return_value = release
            status, data = self._get(f"/api/release/{self.RELEASE_ID}")

        self.assertEqual(status, 200)
        self.assertEqual(data["beets_tracks"], [beets_track])

    @patch("web.routes.browse.discogs_api.get_release")
    def test_release_detail_numeric_id_forwards_to_discogs(self, mock_discogs_get):
        beets_db = FakeBeetsDB()
        beets_db.set_album_ids_for_release("12856590", [8])
        beets_db.set_mbid_detail("12856590", {})
        self.db.seed_request(make_request_row(
            id=42, status="wanted", mb_release_id="12856590", discogs_release_id="12856590",
        ))
        mock_discogs_get.return_value = {
            "id": "12856590",
            "title": "Discogs Album",
            "tracks": [
                {
                    "disc_number": 1,
                    "track_number": 1,
                    "title": "Track",
                    "length_seconds": 180,
                },
            ],
        }
        with patch("web.server.mb_api") as mock_mb, \
                patch("web.server.check_beets_library", return_value={"12856590"}), \
                patch("web.server._beets_db", return_value=beets_db):
            status, data = self._get("/api/release/0012856590")

        self.assertEqual(status, 200)
        mock_discogs_get.assert_called_once_with(12856590)
        mock_mb.get_release.assert_not_called()
        _assert_required_fields(self, data, self.RELEASE_DETAIL_REQUIRED_FIELDS,
                                "release detail response (discogs forward)")
        _assert_required_fields(self, data["tracks"][0], self.RELEASE_TRACK_REQUIRED_FIELDS,
                                "release detail track (discogs forward)")
        self.assertEqual(data["beets_album_id"], 8)

    @patch("web.routes.browse.discogs_api.get_master_releases")
    def test_release_group_numeric_id_forwards_to_discogs(self, mock_discogs_master):
        """#501 item 1: a numeric id in the release-group route is a
        Discogs master id, not an MB release-group UUID — the route
        must dispatch to the Discogs master endpoint (mirrors
        test_release_detail_numeric_id_forwards_to_discogs above) rather
        than firing a doomed MB lookup."""
        beets_db = FakeBeetsDB()
        beets_db.set_album_ids_for_release("21491", [8])
        beets_db.set_mbid_detail("21491", {})
        mock_discogs_master.return_value = {
            "title": "OK Computer",
            "type": "Album",
            "first_release_date": "1997",
            "artist_credit": "Radiohead",
            "primary_artist_id": "3840",
            "releases": [
                {
                    "id": "21491",
                    "title": "OK Computer",
                    "date": "1997",
                    "country": "Europe",
                    "status": "Official",
                    "track_count": 12,
                    "format": "CD",
                    "media_count": 1,
                    "labels": [],
                },
            ],
        }
        with patch("web.server.mb_api") as mock_mb, \
                patch("web.server.check_beets_library", return_value={"21491"}), \
                patch("web.server._beets_db", return_value=beets_db), \
                patch("web.server.check_pipeline",
                      return_value={"21491": {"id": 42, "status": "wanted"}}):
            status, data = self._get("/api/release-group/0021491")

        self.assertEqual(status, 200)
        mock_discogs_master.assert_called_once_with(21491)
        mock_mb.get_release_group_releases.assert_not_called()
        _assert_required_fields(self, data, {"releases"},
                                "release group response (discogs forward)")
        _assert_required_fields(self, data["releases"][0], self.RELEASE_GROUP_REQUIRED_FIELDS,
                                "release group release (discogs forward)")
        self.assertEqual(data["releases"][0]["beets_album_id"], 8)

    def test_artist_disambiguate_contract(self):
        fake_releases = [
            {
                "id": self.RELEASE_ID,
                "title": "Test Album",
                "date": "2024-01-01",
                "country": "US",
                "status": "Official",
                "release-group": {
                    "id": self.RG_ID,
                    "title": "Test Album",
                    "primary-type": "Album",
                    "secondary-types": [],
                },
                "media": [{
                    "position": 1,
                    "format": "CD",
                    "track-count": 1,
                    "tracks": [
                        {"position": 1, "number": "1", "title": "Track",
                         "recording": {"id": "rec-1", "title": "Track"}},
                    ],
                }],
            },
        ]
        with patch("web.server.mb_api") as mock_mb, \
                patch("web.server.check_beets_library", return_value=set()), \
                patch("web.server.check_pipeline", return_value={}):
            mock_mb.get_artist_releases_with_recordings.return_value = fake_releases
            mock_mb.get_artist_name.return_value = "Test Artist"
            status, data = self._get(f"/api/artist/{self.ARTIST_ID}/disambiguate")

        self.assertEqual(status, 200)
        _assert_required_fields(self, data, self.DISAMBIGUATE_RESPONSE_REQUIRED_FIELDS,
                                "disambiguate response")
        rg = data["release_groups"][0]
        _assert_required_fields(self, rg, self.DISAMBIGUATE_RG_REQUIRED_FIELDS,
                                "disambiguate release group")
        _assert_required_fields(self, rg["pressings"][0], self.DISAMBIGUATE_PRESSING_REQUIRED_FIELDS,
                                "disambiguate pressing")
        _assert_required_fields(self, rg["tracks"][0], self.DISAMBIGUATE_TRACK_REQUIRED_FIELDS,
                                "disambiguate track")


class TestDiscogsBrowseRouteContracts(_FakeDbWebServerCase):
    """Contract tests for Discogs browse routes."""

    DISCOGS_SEARCH_REQUIRED_FIELDS = {
        "id", "title", "artist_name", "artist_id",
        "primary_type", "first_release_date",
    }
    DISCOGS_MASTER_RELEASE_REQUIRED_FIELDS = {
        "id", "title", "country", "format",
        "in_library", "beets_album_id", "pipeline_status", "pipeline_id",
    }
    DISCOGS_RELEASE_REQUIRED_FIELDS = {
        "id", "title", "artist_name", "tracks",
        "in_library", "beets_album_id", "pipeline_status", "pipeline_id",
    }
    DISCOGS_ARTIST_REQUIRED_FIELDS = {
        "artist_id", "artist_name", "release_groups",
    }

    def test_discogs_routes_return_503_mirror_required_when_base_unset(self):
        """R13: no mirror configured -> a clear mirror-required 503 from the
        REAL web/discogs.py (raised at URL construction, before any network),
        not a broken upstream fetch. Discogs browse is mirror-required; MB
        browse is unaffected."""
        for path in ("/api/discogs/search?q=test",
                     "/api/discogs/artist/3840",
                     "/api/discogs/master/21491",
                     "/api/discogs/release/21491",
                     "/api/discogs/label/search?q=warp",
                     "/api/discogs/label/757"):
            with self.subTest(path=path):
                status, data = self._get(path)
                self.assertEqual(status, 503, data)
                self.assertIn("mirror", data["error"].lower())

    def test_discogs_search_release_contract(self):
        with patch("web.routes.browse.discogs_api") as mock_dg:
            mock_dg.search_releases.return_value = [
                {
                    "id": "21491",
                    "title": "OK Computer",
                    "artist_id": "3840",
                    "artist_name": "Radiohead",
                    "primary_type": "Album",
                    "first_release_date": "1997",
                    "artist_disambiguation": "",
                    "score": 9,
                    "is_master": True,
                    "discogs_release_id": "83182",
                },
            ]
            status, data = self._get("/api/discogs/search?q=ok+computer&type=release")

        self.assertEqual(status, 200)
        _assert_required_fields(self, data, {"release_groups"}, "discogs search response")
        _assert_required_fields(self, data["release_groups"][0],
                                self.DISCOGS_SEARCH_REQUIRED_FIELDS,
                                "discogs search result")

    def test_discogs_search_artist_contract(self):
        with patch("web.routes.browse.discogs_api") as mock_dg:
            mock_dg.search_artists.return_value = [
                {"id": "3840", "name": "Radiohead", "disambiguation": "", "score": 100},
            ]
            status, data = self._get("/api/discogs/search?q=radiohead&type=artist")

        self.assertEqual(status, 200)
        _assert_required_fields(self, data, {"artists"}, "discogs artist search response")

    def test_discogs_artist_contract(self):
        with patch("web.routes.browse.discogs_api") as mock_dg:
            mock_dg.get_artist_name.return_value = "Radiohead"
            mock_dg.get_artist_releases.return_value = [
                {
                    "id": "21491",
                    "title": "OK Computer",
                    "type": "Album",
                    "secondary_types": [],
                    "first_release_date": "1997",
                    "artist_credit": "Radiohead",
                    "primary_artist_id": "3840",
                },
            ]
            status, data = self._get("/api/discogs/artist/3840")

        self.assertEqual(status, 200)
        _assert_required_fields(self, data, self.DISCOGS_ARTIST_REQUIRED_FIELDS,
                                "discogs artist response")

    def test_discogs_master_contract(self):
        beets_db = FakeBeetsDB()
        beets_db.set_album_ids_for_release("83182", [9])
        beets_db.set_mbid_detail("83182", {})
        with patch("web.routes.browse.discogs_api") as mock_dg, \
                patch("web.server.check_beets_library", return_value={"83182"}), \
                patch("web.server._beets_db", return_value=beets_db), \
                patch("web.server.check_pipeline", return_value={}):
            mock_dg.get_master_releases.return_value = {
                "title": "OK Computer",
                "type": "Album",
                "first_release_date": "1997",
                "artist_credit": "Radiohead",
                "primary_artist_id": "3840",
                "releases": [
                    {
                        "id": "83182",
                        "title": "OK Computer",
                        "date": "1997",
                        "country": "Europe",
                        "status": "Official",
                        "track_count": 12,
                        "format": "CD",
                        "media_count": 1,
                        "labels": [],
                    },
                ],
            }
            status, data = self._get("/api/discogs/master/21491")

        self.assertEqual(status, 200)
        _assert_required_fields(self, data["releases"][0],
                                self.DISCOGS_MASTER_RELEASE_REQUIRED_FIELDS,
                                "discogs master release")
        self.assertEqual(data["releases"][0]["beets_album_id"], 9)

    def test_discogs_release_contract(self):
        beets_db = FakeBeetsDB()
        beets_db.set_album_ids_for_release("83182", [10])
        beets_db.set_mbid_detail("83182", {})
        with patch("web.routes.browse.discogs_api") as mock_dg, \
                patch("web.server.check_beets_library", return_value={"83182"}), \
                patch("web.server._beets_db", return_value=beets_db):
            mock_dg.get_release.return_value = {
                "id": "83182",
                "title": "OK Computer",
                "artist_name": "Radiohead",
                "artist_id": "3840",
                "release_group_id": "21491",
                "date": "1997",
                "year": 1997,
                "country": "Europe",
                "status": "Official",
                "tracks": [
                    {"disc_number": 1, "track_number": 1, "title": "Airbag", "length_seconds": 284},
                ],
                "labels": [],
                "formats": [],
            }
            status, data = self._get("/api/discogs/release/83182")

        self.assertEqual(status, 200)
        _assert_required_fields(self, data, self.DISCOGS_RELEASE_REQUIRED_FIELDS,
                                "discogs release detail")
        self.assertEqual(data["beets_album_id"], 10)


class TestSearchByIdResolveContract(_FakeDbWebServerCase):
    """Contract tests for /api/browse/resolve — the search-by-ID resolver."""

    REQUIRED_FIELDS = {
        "source", "kind", "artist_id", "artist_name",
        "is_va", "expand_id", "leaf_id",
    }

    MB_RELEASE_ID = "c1f6a2c9-bcba-4e69-96f5-233c85b2830a"
    MB_RG_ID = "11111111-1111-1111-1111-111111111111"
    MB_ARTIST_ID = "664c3e0e-42d8-48c1-b209-1efca19c0325"
    MB_VA_MBID = "89ad4ac3-39f7-470e-963a-56509c546377"

    def test_mb_release_resolved(self):
        """Happy path: ?source=mb&id=<mbid>&kind=release returns leaf shape."""
        with patch("web.server.mb_api") as mock_mb:
            mock_mb.get_release.return_value = {
                "id": self.MB_RELEASE_ID,
                "title": "Test Release",
                "artist_id": self.MB_ARTIST_ID,
                "artist_name": "Test Artist",
                "release_group_id": self.MB_RG_ID,
            }
            status, data = self._get(
                f"/api/browse/resolve?source=mb&id={self.MB_RELEASE_ID}&kind=release")

        self.assertEqual(status, 200)
        _assert_required_fields(self, data, self.REQUIRED_FIELDS, "resolve response")
        self.assertEqual(data["source"], "mb")
        self.assertEqual(data["kind"], "release")
        self.assertEqual(data["artist_id"], self.MB_ARTIST_ID)
        self.assertEqual(data["artist_name"], "Test Artist")
        self.assertFalse(data["is_va"])
        self.assertEqual(data["expand_id"], self.MB_RG_ID)
        self.assertEqual(data["leaf_id"], self.MB_RELEASE_ID)

    def test_mb_release_group_resolved(self):
        """Happy path: ?source=mb&id=<mbid>&kind=release-group returns group shape."""
        with patch("web.server.mb_api") as mock_mb:
            mock_mb.get_release_group.return_value = {
                "id": self.MB_RG_ID,
                "title": "Test RG",
                "artist_id": self.MB_ARTIST_ID,
                "artist_name": "Test Artist",
            }
            status, data = self._get(
                f"/api/browse/resolve?source=mb&id={self.MB_RG_ID}&kind=release-group")

        self.assertEqual(status, 200)
        _assert_required_fields(self, data, self.REQUIRED_FIELDS, "resolve response")
        self.assertEqual(data["kind"], "release-group")
        self.assertEqual(data["expand_id"], self.MB_RG_ID)
        self.assertIsNone(data["leaf_id"])

    def test_discogs_release_resolved_with_master(self):
        """Discogs release with non-null master_id → leaf shape, expand=master."""
        with patch("web.routes.browse.discogs_api") as mock_dg:
            mock_dg.get_release.return_value = {
                "id": "32457180",
                "title": "Rock Christmas",
                "artist_id": "194",
                "artist_name": "Various",
                "release_group_id": "3673686",
            }
            status, data = self._get(
                "/api/browse/resolve?source=discogs&id=32457180&kind=release")

        self.assertEqual(status, 200)
        _assert_required_fields(self, data, self.REQUIRED_FIELDS, "resolve response")
        self.assertEqual(data["source"], "discogs")
        self.assertEqual(data["kind"], "release")
        self.assertEqual(data["expand_id"], "3673686")
        self.assertEqual(data["leaf_id"], "32457180")
        # artists[0].id == 194 → VA
        self.assertTrue(data["is_va"])

    def test_discogs_master_resolved(self):
        """Discogs master ID → group shape, no leaf."""
        with patch("web.routes.browse.discogs_api") as mock_dg:
            mock_dg.get_master_releases.return_value = {
                "title": "Some Master",
                "type": "Album",
                "first_release_date": "1997",
                "artist_credit": "Real Artist",
                "primary_artist_id": "3840",
                "releases": [],
            }
            status, data = self._get(
                "/api/browse/resolve?source=discogs&id=3673686&kind=master")

        self.assertEqual(status, 200)
        _assert_required_fields(self, data, self.REQUIRED_FIELDS, "resolve response")
        self.assertEqual(data["kind"], "master")
        self.assertEqual(data["expand_id"], "3673686")
        self.assertIsNone(data["leaf_id"])
        self.assertFalse(data["is_va"])

    def test_discogs_masterless_release(self):
        """Masterless Discogs release: release_group_id is None → expand=leaf."""
        with patch("web.routes.browse.discogs_api") as mock_dg:
            mock_dg.get_release.return_value = {
                "id": "999",
                "title": "Masterless",
                "artist_id": "3840",
                "artist_name": "Some Artist",
                "release_group_id": None,
            }
            status, data = self._get(
                "/api/browse/resolve?source=discogs&id=999&kind=release")

        self.assertEqual(status, 200)
        # When master_id is None, the bare release is its own expand target
        # so the artist view rings the masterless rg row in place.
        self.assertEqual(data["expand_id"], "999")
        self.assertEqual(data["leaf_id"], "999")

    def test_mb_va_release(self):
        """MB release whose artist matches VA_MBID → is_va: true."""
        with patch("web.server.mb_api") as mock_mb:
            mock_mb.get_release.return_value = {
                "id": self.MB_RELEASE_ID,
                "title": "VA Comp",
                "artist_id": self.MB_VA_MBID,
                "artist_name": "Various Artists",
                "release_group_id": self.MB_RG_ID,
            }
            status, data = self._get(
                f"/api/browse/resolve?source=mb&id={self.MB_RELEASE_ID}&kind=release")

        self.assertEqual(status, 200)
        self.assertTrue(data["is_va"])

    def test_unknown_kind_falls_back_mb_release_to_rg(self):
        """kind=unknown: leaf endpoint 404 → falls back to release-group."""
        with patch("web.server.mb_api") as mock_mb:
            from urllib.error import HTTPError
            mock_mb.get_release.side_effect = HTTPError(
                url="x", code=404, msg="Not Found", hdrs=email.message.Message(), fp=None)
            mock_mb.get_release_group.return_value = {
                "id": self.MB_RG_ID,
                "title": "RG",
                "artist_id": self.MB_ARTIST_ID,
                "artist_name": "Artist",
            }
            status, data = self._get(
                f"/api/browse/resolve?source=mb&id={self.MB_RG_ID}&kind=unknown")

        self.assertEqual(status, 200)
        self.assertEqual(data["kind"], "release-group")
        # Confirms TWO upstream calls: release tried, then release-group
        self.assertEqual(mock_mb.get_release.call_count, 1)
        self.assertEqual(mock_mb.get_release_group.call_count, 1)

    def test_unknown_kind_falls_back_discogs_release_to_master(self):
        with patch("web.routes.browse.discogs_api") as mock_dg:
            from urllib.error import HTTPError
            mock_dg.get_release.side_effect = HTTPError(
                url="x", code=404, msg="Not Found", hdrs=email.message.Message(), fp=None)
            mock_dg.get_master_releases.return_value = {
                "title": "M", "type": "Album", "first_release_date": "1997",
                "artist_credit": "Artist", "primary_artist_id": "3840",
                "releases": [],
            }
            status, data = self._get(
                "/api/browse/resolve?source=discogs&id=3673686&kind=unknown")

        self.assertEqual(status, 200)
        self.assertEqual(data["kind"], "master")

    def test_kind_hint_release_does_not_probe_group_on_404(self):
        """kind=release explicit: 404 returns 404 immediately, no group probe.

        Guards the URL-disambiguation optimisation from regressing into
        always-probe-both behaviour. If the URL said 'release', we trust it.
        """
        with patch("web.server.mb_api") as mock_mb:
            from urllib.error import HTTPError
            mock_mb.get_release.side_effect = HTTPError(
                url="x", code=404, msg="Not Found", hdrs=email.message.Message(), fp=None)
            status, data = self._get(
                f"/api/browse/resolve?source=mb&id={self.MB_RG_ID}&kind=release")

        self.assertEqual(status, 404)
        # release-group endpoint MUST NOT have been called
        self.assertEqual(mock_mb.get_release_group.call_count, 0)

    def test_not_found_both_endpoints(self):
        with patch("web.server.mb_api") as mock_mb:
            from urllib.error import HTTPError
            mock_mb.get_release.side_effect = HTTPError(
                url="x", code=404, msg="Not Found", hdrs=email.message.Message(), fp=None)
            mock_mb.get_release_group.side_effect = HTTPError(
                url="x", code=404, msg="Not Found", hdrs=email.message.Message(), fp=None)
            status, data = self._get(
                f"/api/browse/resolve?source=mb&id={self.MB_RELEASE_ID}&kind=unknown")

        self.assertEqual(status, 404)
        self.assertIn("error", data)

    def test_missing_id(self):
        status, data = self._get("/api/browse/resolve?source=mb")
        self.assertEqual(status, 400)
        self.assertIn("error", data)

    def test_missing_source(self):
        status, data = self._get(f"/api/browse/resolve?id={self.MB_RELEASE_ID}")
        self.assertEqual(status, 400)

    def test_invalid_source(self):
        status, data = self._get(
            f"/api/browse/resolve?source=apple&id={self.MB_RELEASE_ID}")
        self.assertEqual(status, 400)

    def test_invalid_kind(self):
        status, data = self._get(
            f"/api/browse/resolve?source=mb&id={self.MB_RELEASE_ID}&kind=garbage")
        self.assertEqual(status, 400)


class TestLibraryArtistContract(unittest.TestCase):
    """Contract tests: get_library_artist() returns all fields the frontend needs."""

    @classmethod
    def setUpClass(cls):
        import sqlite3
        import tempfile
        cls._tmpdir = tempfile.mkdtemp()
        cls._db_path = os.path.join(cls._tmpdir, "beets.db")
        conn = sqlite3.connect(cls._db_path)
        conn.executescript("""
            CREATE TABLE albums (
                id INTEGER PRIMARY KEY,
                album TEXT, albumartist TEXT, year INTEGER,
                mb_albumid TEXT, discogs_albumid TEXT,
                mb_albumartistid TEXT, mb_albumartistids TEXT,
                mb_releasegroupid TEXT, release_group_title TEXT,
                added REAL, albumtype TEXT, label TEXT, country TEXT,
                format TEXT, artpath BLOB
            );
            CREATE TABLE items (
                id INTEGER PRIMARY KEY, album_id INTEGER,
                bitrate INTEGER, path BLOB, title TEXT, artist TEXT,
                track INTEGER, disc INTEGER, length REAL, format TEXT,
                samplerate INTEGER, bitdepth INTEGER
            );
            INSERT INTO albums (id, album, albumartist, year, mb_albumid,
                mb_albumartistid, mb_releasegroupid, release_group_title,
                added, albumtype, label, country)
            VALUES (1, 'Tallahassee', 'The Mountain Goats', 2002,
                'aaaa-bbbb-cccc', 'dddd-eeee-ffff',
                '1111-2222-3333', 'Tallahassee',
                1773651901.0, 'album', '4AD', 'US');
            INSERT INTO albums (id, album, albumartist, year, mb_albumid,
                mb_albumartistid, mb_releasegroupid, release_group_title,
                added, albumtype, label, country)
            VALUES (2, 'Tallahassee (Deluxe)', 'The Mountain Goats', 2002,
                'xxxx-yyyy-zzzz', 'dddd-eeee-ffff',
                '1111-2222-3333', 'Tallahassee',
                1773651902.0, 'album', '4AD', 'US');
            INSERT INTO items (album_id, bitrate, path, format)
                VALUES (1, 245000, X'2F612F622E6D7033', 'MP3');
            INSERT INTO items (album_id, bitrate, path, format)
                VALUES (2, 320000, X'2F612F632E6D7033', 'MP3');
        """)
        conn.close()

        # Patch the beets DB into server module
        import web.server as srv
        from lib.beets_db import BeetsDB
        cls._beets = BeetsDB(cls._db_path)
        cls._orig_beets = srv._beets
        srv._beets = cls._beets

    @classmethod
    def tearDownClass(cls):
        import web.server as srv
        srv._beets = cls._orig_beets
        import shutil
        shutil.rmtree(cls._tmpdir, ignore_errors=True)

    # Fields the frontend (library.js, discography.js) requires for rendering.
    # These must match _album_row_to_dict() output — the single source of truth.
    REQUIRED_FIELDS = {
        "id", "album", "artist", "year", "mb_albumid", "track_count",
        "mb_releasegroupid", "release_group_title", "added",
        "formats", "min_bitrate", "type", "label", "country", "source",
    }

    FIELD_TYPES = {
        "id": int, "album": str, "artist": str, "year": int,
        "track_count": int, "min_bitrate": int, "added": float,
    }

    def test_response_has_all_required_fields(self):
        """Every album dict must include all fields the frontend JS uses."""
        import web.server as srv
        albums = srv.get_library_artist("Mountain Goats", "dddd-eeee-ffff")
        self.assertEqual(len(albums), 2)
        for album in albums:
            missing = self.REQUIRED_FIELDS - set(album.keys())
            self.assertFalse(missing,
                f"Album '{album.get('album')}' missing fields: {missing}")
            # Verify types for critical fields
            for field, expected_type in self.FIELD_TYPES.items():
                self.assertIsInstance(album[field], expected_type,
                    f"{field}={album[field]!r} should be {expected_type}")

    def test_release_group_fields_populated(self):
        """mb_releasegroupid and release_group_title must have actual values."""
        import web.server as srv
        albums = srv.get_library_artist("Mountain Goats", "dddd-eeee-ffff")
        for album in albums:
            self.assertIsNotNone(album["mb_releasegroupid"])
            self.assertNotEqual(album["mb_releasegroupid"], "")
            self.assertIsNotNone(album["release_group_title"])

    def test_releases_group_by_release_group_id(self):
        """Two pressings of same release group should share the same rgid."""
        import web.server as srv
        albums = srv.get_library_artist("Mountain Goats", "dddd-eeee-ffff")
        rg_ids = {a["mb_releasegroupid"] for a in albums}
        self.assertEqual(len(rg_ids), 1, "Both pressings should share one release group")
        self.assertEqual(rg_ids.pop(), "1111-2222-3333")

    def test_name_only_lookup(self):
        """Lookup by name only (no mbid) also returns all required fields."""
        import web.server as srv
        albums = srv.get_library_artist("Mountain Goats")
        self.assertGreater(len(albums), 0)
        for album in albums:
            missing = self.REQUIRED_FIELDS - set(album.keys())
            self.assertFalse(missing,
                f"Album '{album.get('album')}' missing fields: {missing}")

if __name__ == "__main__":
    unittest.main()
