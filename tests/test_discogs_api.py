"""Unit tests for web/discogs.py — Discogs mirror API wrapper."""

import json
import os
import sys
import unittest
from unittest.mock import patch, MagicMock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from web.discogs import (
    _parse_duration,
    _parse_position,
    _parse_year,
    _primary_artist_name,
    get_artist_releases,
    get_release,
    get_master_releases,
    search_releases,
    search_artists,
    get_artist_name,
)


class TestParseDuration(unittest.TestCase):
    CASES = [
        ("normal", "4:44", 284.0),
        ("short", "0:30", 30.0),
        ("long", "1:02:15", 3735.0),
        ("empty", "", None),
        ("none", None, None),
        ("invalid", "abc", None),
    ]

    def test_parse_duration(self):
        for desc, input_val, expected in self.CASES:
            with self.subTest(desc=desc):
                self.assertEqual(_parse_duration(input_val), expected)


class TestParsePosition(unittest.TestCase):
    CASES = [
        ("simple number", "3", (1, 3)),
        ("cd disc-track", "2-5", (2, 5)),
        ("vinyl side", "A1", (1, 1)),
        ("vinyl side B", "B3", (2, 3)),
        ("empty", "", (1, 0)),
    ]

    def test_parse_position(self):
        for desc, input_val, expected in self.CASES:
            with self.subTest(desc=desc):
                self.assertEqual(_parse_position(input_val), expected)


class TestParseYear(unittest.TestCase):
    CASES = [
        ("full date", "1997-06-16", 1997),
        ("year only", "2020", 2020),
        ("empty", "", None),
        ("none", None, None),
    ]

    def test_parse_year(self):
        for desc, input_val, expected in self.CASES:
            with self.subTest(desc=desc):
                self.assertEqual(_parse_year(input_val), expected)


class TestPrimaryArtistName(unittest.TestCase):
    def test_with_artists(self):
        self.assertEqual(
            _primary_artist_name([{"id": 1, "name": "Radiohead"}]),
            "Radiohead",
        )

    def test_empty(self):
        self.assertEqual(_primary_artist_name([]), "Unknown")


def _mock_urlopen(response_data):
    """Create a mock for urllib.request.urlopen that returns JSON data."""
    mock_resp = MagicMock()
    mock_resp.read.return_value = json.dumps(response_data).encode()
    mock_resp.__enter__ = lambda s: s
    mock_resp.__exit__ = MagicMock(return_value=False)
    return patch("web.discogs.urllib.request.urlopen", return_value=mock_resp)


class TestGetRelease(unittest.TestCase):
    RELEASE_DATA = {
        "id": 83182,
        "title": "OK Computer",
        "country": "Europe",
        "released": "1997-06-16",
        "master_id": 21491,
        "artists": [{"id": 3840, "name": "Radiohead", "role": "", "anv": ""}],
        "labels": [{"id": 2294, "name": "Parlophone", "catno": "NODATA 02"}],
        "formats": [{"name": "CD", "qty": 1, "descriptions": "Album"}],
        "tracks": [
            {"position": "1", "title": "Airbag", "duration": "4:44", "artists": []},
            {"position": "2", "title": "Paranoid Android", "duration": "6:23", "artists": []},
        ],
    }

    def test_normalizes_release(self):
        with _mock_urlopen(self.RELEASE_DATA):
            result = get_release(83182)

        self.assertEqual(result["id"], "83182")
        self.assertEqual(result["title"], "OK Computer")
        self.assertEqual(result["artist_name"], "Radiohead")
        self.assertEqual(result["artist_id"], "3840")
        self.assertEqual(result["release_group_id"], "21491")
        self.assertEqual(result["year"], 1997)
        self.assertEqual(result["country"], "Europe")
        self.assertEqual(len(result["tracks"]), 2)
        self.assertEqual(result["tracks"][0]["title"], "Airbag")
        self.assertEqual(result["tracks"][0]["disc_number"], 1)
        self.assertEqual(result["tracks"][0]["track_number"], 1)
        self.assertEqual(result["tracks"][0]["length_seconds"], 284.0)


class TestGetMasterReleases(unittest.TestCase):
    MASTER_DATA = {
        "id": 21491,
        "title": "OK Computer",
        "year": 1997,
        "main_release_id": 4950798,
        "primary_type": "Album",
        "first_release_date": "1997",
        "artist_credit": "Radiohead",
        "primary_artist_id": 3840,
        "artists": [{"id": 3840, "name": "Radiohead"}],
        "releases": [
            {
                "id": 83182,
                "title": "OK Computer",
                "country": "Europe",
                "released": "1997-06-16",
                "track_count": 12,
                "formats": [{"name": "CD", "qty": 1}],
                "labels": [{"id": 2294, "name": "Parlophone", "catno": "X"}],
            },
            {
                "id": 105704,
                "title": "OK Computer",
                "country": "US",
                "released": "1997-07-01",
                "track_count": 12,
                "formats": [{"name": "CD", "qty": 1}],
                "labels": [],
            },
        ],
    }

    def test_normalizes_master(self):
        with _mock_urlopen(self.MASTER_DATA):
            result = get_master_releases(21491)

        self.assertEqual(result["title"], "OK Computer")
        self.assertEqual(result["type"], "Album")
        self.assertEqual(result["first_release_date"], "1997")
        self.assertEqual(result["artist_credit"], "Radiohead")
        self.assertEqual(result["primary_artist_id"], "3840")
        self.assertEqual(len(result["releases"]), 2)
        self.assertEqual(result["releases"][0]["id"], "83182")
        self.assertEqual(result["releases"][0]["country"], "Europe")
        self.assertEqual(result["releases"][0]["format"], "CD")
        self.assertEqual(result["releases"][0]["date"], "1997-06-16")
        self.assertEqual(result["releases"][0]["track_count"], 12)

    def test_track_count_defaults_to_zero_when_missing(self):
        """Discogs CC0 dump occasionally lacks tracklists; fall back to 0
        rather than the old format-quantity fudge that displayed '1t'."""
        master = {
            "id": 1,
            "title": "Sparse",
            "releases": [
                {"id": 99, "title": "Sparse", "country": "AU",
                 "formats": [{"name": "CD", "qty": 1}], "labels": []},
            ],
        }
        with _mock_urlopen(master):
            result = get_master_releases(1)
        self.assertEqual(result["releases"][0]["track_count"], 0)
        self.assertEqual(result["releases"][0]["date"], "")


class TestSearchReleases(unittest.TestCase):
    SEARCH_DATA = {
        "results": [
            {
                "id": 83182,
                "title": "OK Computer",
                "master_id": 21491,
                "master_title": "OK Computer",
                "master_first_released": "1997",
                "primary_type": "Album",
                "score": 0.099,
                "released": "1997-06-16",
                "artists": [{"id": 3840, "name": "Radiohead"}],
            },
            {
                "id": 105704,
                "title": "OK Computer (US)",
                "master_id": 21491,
                "master_title": "OK Computer",
                "master_first_released": "1997",
                "primary_type": "Album",
                "score": 0.05,
                "released": "1997-07-01",
                "artists": [{"id": 3840, "name": "Radiohead"}],
            },
            {
                "id": 999,
                "title": "OK Computer Demos",
                "master_id": None,
                "primary_type": "Other",
                "score": 0.02,
                "released": "1996",
                "artists": [{"id": 3840, "name": "Radiohead"}],
            },
        ],
    }

    def test_deduplicates_by_master_with_master_metadata(self):
        with _mock_urlopen(self.SEARCH_DATA):
            results = search_releases("OK Computer")

        # 1 master (deduped) + 1 masterless = 2 entries
        self.assertEqual(len(results), 2)
        first = results[0]
        self.assertEqual(first["id"], "21491")
        self.assertEqual(first["title"], "OK Computer")  # master_title, not per-release title
        self.assertEqual(first["primary_type"], "Album")
        self.assertEqual(first["first_release_date"], "1997")  # master_first_released
        self.assertEqual(first["artist_name"], "Radiohead")
        self.assertTrue(first["is_master"])
        self.assertEqual(first["score"], 9)  # int(0.099 * 100)
        self.assertEqual(first["discogs_release_id"], "83182")

        masterless = results[1]
        self.assertEqual(masterless["id"], "999")
        self.assertEqual(masterless["title"], "OK Computer Demos")
        self.assertFalse(masterless["is_master"])
        self.assertEqual(masterless["first_release_date"], "1996")  # falls back to released


class TestSearchArtists(unittest.TestCase):
    """search_artists() now hits /api/artists?name= (real artist-name index)."""

    ARTIST_SEARCH_DATA = {
        "results": [
            {
                "id": 3840,
                "name": "Radiohead",
                "profile": "British alternative rock band...",
                "score": 0.06079271,
            },
            {
                "id": 104129,
                "name": "Radioheads",
                "profile": "",
                "score": 0.06079271,
            },
        ],
        "total": 6,
        "page": 1,
        "per_page": 25,
    }

    def test_returns_name_matched_artists(self):
        with _mock_urlopen(self.ARTIST_SEARCH_DATA) as mock:
            results = search_artists("Radiohead")

        # Verify the new endpoint was hit (not the old release-search hack)
        called_url = mock.call_args[0][0].full_url
        self.assertIn("/api/artists?name=", called_url)
        self.assertNotIn("/api/search?artist=", called_url)

        self.assertEqual(len(results), 2)
        self.assertEqual(results[0]["id"], "3840")
        self.assertEqual(results[0]["name"], "Radiohead")
        self.assertEqual(results[0]["disambiguation"], "")  # left empty intentionally
        self.assertEqual(results[0]["score"], 6)  # int(0.06 * 100)
        self.assertEqual(results[1]["name"], "Radioheads")


class TestGetArtistReleases(unittest.TestCase):
    """get_artist_releases() now hits /api/artists/{id}/masters (master-grouped)."""

    MASTERS_DATA = {
        "results": [
            {
                "id": 21481,
                "title": "Creep",
                "type": "EP",
                "first_release_date": "1992",
                "artist_credit": "Radiohead",
                "primary_artist_id": 3840,
                "is_masterless": False,
            },
            {
                "id": 13344,
                "title": "Pablo Honey",
                "type": "Album",
                "first_release_date": "1993",
                "artist_credit": "Radiohead",
                "primary_artist_id": 3840,
                "is_masterless": False,
            },
            {
                "id": "release-83182",
                "title": "Stupid Car (demo)",
                "type": "Other",
                "first_release_date": "1993",
                "artist_credit": "Radiohead",
                "primary_artist_id": 3840,
                "is_masterless": True,
            },
        ],
        "total": 3,
        "page": 1,
        "per_page": 100,
    }

    def test_normalizes_master_discography(self):
        with _mock_urlopen(self.MASTERS_DATA) as mock:
            results = get_artist_releases(3840)

        called_url = mock.call_args[0][0].full_url
        self.assertIn("/api/artists/3840/masters", called_url)

        self.assertEqual(len(results), 3)

        album = next(r for r in results if r["title"] == "Pablo Honey")
        self.assertEqual(album["id"], "13344")
        self.assertEqual(album["type"], "Album")
        self.assertEqual(album["first_release_date"], "1993")
        self.assertEqual(album["artist_credit"], "Radiohead")
        self.assertEqual(album["primary_artist_id"], "3840")
        self.assertEqual(album["secondary_types"], [])
        self.assertNotIn("is_masterless", album)  # only set when True

        masterless = next(r for r in results if r["title"] == "Stupid Car (demo)")
        self.assertEqual(masterless["id"], "83182")  # "release-" prefix stripped
        self.assertEqual(masterless["discogs_release_id"], "83182")
        self.assertTrue(masterless["is_masterless"])


class TestGetArtistName(unittest.TestCase):
    def test_returns_name(self):
        with _mock_urlopen({"id": 3840, "name": "Radiohead"}):
            self.assertEqual(get_artist_name(3840), "Radiohead")


if __name__ == "__main__":
    unittest.main()
