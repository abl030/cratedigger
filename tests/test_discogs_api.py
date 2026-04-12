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
        "releases": [
            {
                "id": 83182,
                "title": "OK Computer",
                "country": "Europe",
                "formats": [{"name": "CD", "qty": 1}],
                "labels": [{"id": 2294, "name": "Parlophone", "catno": "X"}],
            },
            {
                "id": 105704,
                "title": "OK Computer",
                "country": "US",
                "formats": [{"name": "CD", "qty": 1}],
                "labels": [],
            },
        ],
    }

    def test_normalizes_master(self):
        with _mock_urlopen(self.MASTER_DATA):
            result = get_master_releases(21491)

        self.assertEqual(result["title"], "OK Computer")
        self.assertEqual(len(result["releases"]), 2)
        self.assertEqual(result["releases"][0]["id"], "83182")
        self.assertEqual(result["releases"][0]["country"], "Europe")
        self.assertEqual(result["releases"][0]["format"], "CD")


class TestSearchReleases(unittest.TestCase):
    SEARCH_DATA = {
        "results": [
            {
                "id": 83182,
                "title": "OK Computer",
                "master_id": 21491,
                "released": "1997-06-16",
                "artists": [{"id": 3840, "name": "Radiohead"}],
            },
            {
                "id": 105704,
                "title": "OK Computer",
                "master_id": 21491,
                "released": "1997-07-01",
                "artists": [{"id": 3840, "name": "Radiohead"}],
            },
        ],
    }

    def test_deduplicates_by_master(self):
        with _mock_urlopen(self.SEARCH_DATA):
            results = search_releases("OK Computer")

        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["id"], "21491")
        self.assertEqual(results[0]["artist_name"], "Radiohead")
        self.assertTrue(results[0]["is_master"])


class TestSearchArtists(unittest.TestCase):
    SEARCH_DATA = {
        "results": [
            {
                "id": 1,
                "title": "Album A",
                "artists": [{"id": 3840, "name": "Radiohead"}],
            },
            {
                "id": 2,
                "title": "Album B",
                "artists": [{"id": 3840, "name": "Radiohead"}],
            },
        ],
    }

    def test_deduplicates_artists(self):
        with _mock_urlopen(self.SEARCH_DATA):
            results = search_artists("Radiohead")

        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["name"], "Radiohead")
        self.assertEqual(results[0]["id"], "3840")


class TestGetArtistName(unittest.TestCase):
    def test_returns_name(self):
        with _mock_urlopen({"id": 3840, "name": "Radiohead"}):
            self.assertEqual(get_artist_name(3840), "Radiohead")


if __name__ == "__main__":
    unittest.main()
