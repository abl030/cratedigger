"""Unit tests for web/discogs.py — Discogs mirror API wrapper."""

import json
import os
import sys
import unittest
from unittest.mock import patch, MagicMock

import msgspec

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
    search_labels,
    get_label,
    get_label_releases,
    LabelEntity,
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


# ── Label adapter tests (U3) ────────────────────────────────────────────


class TestSearchLabels(unittest.TestCase):
    """search_labels() hits /api/labels?name= and returns LabelEntity list."""

    LABEL_SEARCH_DATA = {
        "results": [
            {
                "id": 2294,
                "name": "Parlophone",
                "profile": "British record label founded in 1896.",
                "parent_label_id": None,
                "parent_label_name": None,
                "release_count": 18452,
                "score": 0.087,
            },
            {
                "id": 25693,
                "name": "Parlophone Records Ltd.",
                "profile": "Subsidiary trading name.",
                "parent_label_id": 2294,
                "parent_label_name": "Parlophone",
                "release_count": 412,
                "score": 0.072,
            },
        ],
        "total": 2,
        "page": 1,
        "per_page": 25,
    }

    def test_returns_label_entities(self):
        with _mock_urlopen(self.LABEL_SEARCH_DATA) as mock:
            results = search_labels("Parlophone")

        called_url = mock.call_args[0][0].full_url
        self.assertIn("/api/labels?name=", called_url)

        self.assertEqual(len(results), 2)
        first = results[0]
        self.assertIsInstance(first, LabelEntity)
        self.assertEqual(first.source, "discogs")
        self.assertEqual(first.id, "2294")  # int → str coercion
        self.assertEqual(first.name, "Parlophone")
        self.assertIsNone(first.country)  # discogs has no country column
        self.assertEqual(first.profile, "British record label founded in 1896.")
        self.assertIsNone(first.parent_label_id)
        self.assertIsNone(first.parent_label_name)
        self.assertEqual(first.release_count, 18452)

        sub = results[1]
        self.assertEqual(sub.id, "25693")
        self.assertEqual(sub.parent_label_id, "2294")  # int → str coercion
        self.assertEqual(sub.parent_label_name, "Parlophone")

    def test_empty_results_returns_empty_list(self):
        with _mock_urlopen({"results": [], "total": 0, "page": 1, "per_page": 25}):
            results = search_labels("zzzzznosuchlabel")
        self.assertEqual(results, [])

    def test_wire_boundary_validates_release_count_int(self):
        """RED-first regression guard: a release_count arriving as a STRING
        instead of int must raise msgspec.ValidationError at the boundary.
        Per .claude/rules/code-quality.md, every wire-boundary type owes
        at least one test that proves it actually catches drift."""
        bad = {
            "results": [
                {
                    "id": 2294,
                    "name": "Parlophone",
                    "profile": "x",
                    "parent_label_id": None,
                    "parent_label_name": None,
                    "release_count": "18452",  # WRONG: string, not int
                    "score": 0.087,
                },
            ],
            "total": 1,
            "page": 1,
            "per_page": 25,
        }
        with _mock_urlopen(bad):
            with self.assertRaises(msgspec.ValidationError):
                search_labels("Parlophone")


class TestGetLabel(unittest.TestCase):
    """get_label() hits /api/labels/{id} and returns a LabelEntity."""

    TOP_LEVEL_DATA = {
        "id": 2294,
        "name": "Parlophone",
        "profile": "British record label.",
        "contactinfo": "",
        "data_quality": "Correct",
        "parent_label_id": None,
        "parent_label_name": None,
        "total_releases": 18452,
        "sub_labels": [
            {"id": 25693, "name": "Parlophone Records Ltd.", "release_count": 412},
        ],
    }

    SUB_LABEL_DATA = {
        "id": 25693,
        "name": "Parlophone Records Ltd.",
        "profile": "",
        "contactinfo": "",
        "data_quality": "Needs Vote",
        "parent_label_id": 2294,
        "parent_label_name": "Parlophone",
        "total_releases": 412,
        "sub_labels": [],
    }

    def test_top_level_label(self):
        with _mock_urlopen(self.TOP_LEVEL_DATA) as mock:
            entity = get_label(2294)

        called_url = mock.call_args[0][0].full_url
        self.assertIn("/api/labels/2294", called_url)

        self.assertIsInstance(entity, LabelEntity)
        self.assertEqual(entity.source, "discogs")
        self.assertEqual(entity.id, "2294")
        self.assertEqual(entity.name, "Parlophone")
        self.assertIsNone(entity.country)
        self.assertEqual(entity.profile, "British record label.")
        self.assertIsNone(entity.parent_label_id)
        self.assertIsNone(entity.parent_label_name)
        self.assertEqual(entity.release_count, 18452)  # comes from total_releases

    def test_sub_label_has_parent(self):
        with _mock_urlopen(self.SUB_LABEL_DATA):
            entity = get_label(25693)

        self.assertEqual(entity.parent_label_id, "2294")
        self.assertEqual(entity.parent_label_name, "Parlophone")
        self.assertEqual(entity.release_count, 412)


class TestGetLabelReleases(unittest.TestCase):
    """get_label_releases() hits /api/labels/{id}/releases."""

    RELEASES_DATA = {
        "results": [
            {
                "id": 83182,
                "title": "OK Computer",
                "country": "Europe",
                "released": "1997-06-16",
                "master_id": 21491,
                "master_title": "OK Computer",
                "master_first_released": "1997",
                "primary_type": "Album",
                "via_label_id": 2294,
                "sub_label_name": None,
                "artists": [{"id": 3840, "name": "Radiohead", "role": "", "anv": ""}],
                "labels": [{"id": 2294, "name": "Parlophone", "catno": "NODATA 02"}],
                "formats": [
                    {"name": "CD", "qty": 1, "descriptions": "Album", "free_text": ""}
                ],
            },
            {
                "id": 999111,
                "title": "Some Sub-label Release",
                "country": "UK",
                "released": "2001",
                "master_id": None,
                "primary_type": "Single",
                "via_label_id": 25693,
                "sub_label_name": "Parlophone Records Ltd.",
                "artists": [{"id": 1, "name": "Various", "role": "", "anv": ""}],
                "labels": [
                    {"id": 25693, "name": "Parlophone Records Ltd.", "catno": "PRL 1"}
                ],
                "formats": [
                    {"name": "Vinyl", "qty": 1, "descriptions": "7\"", "free_text": ""}
                ],
            },
        ],
        "pagination": {"page": 1, "per_page": 100, "pages": 1, "items": 2},
        "include_sublabels": True,
    }

    def test_returns_release_rows(self):
        with _mock_urlopen(self.RELEASES_DATA) as mock:
            payload = get_label_releases(2294, include_sublabels=True, page=1, per_page=100)

        called_url = mock.call_args[0][0].full_url
        self.assertIn("/api/labels/2294/releases", called_url)
        self.assertIn("include_sublabels=true", called_url)
        self.assertIn("page=1", called_url)
        self.assertIn("per_page=100", called_url)

        self.assertIn("results", payload)
        self.assertIn("pagination", payload)
        self.assertIn("include_sublabels", payload)
        self.assertTrue(payload["include_sublabels"])
        self.assertEqual(payload["pagination"]["items"], 2)

        rows = payload["results"]
        self.assertEqual(len(rows), 2)

        direct = rows[0]
        # Match shape used by web/discogs.py::get_master_releases / get_release
        # so the U4 route layer can overlay library/pipeline state without
        # renaming fields. ID is stringified, year derived from `released`,
        # primary_artist_id surfaces for cross-source overlay.
        self.assertEqual(direct["id"], "83182")
        self.assertEqual(direct["title"], "OK Computer")
        self.assertEqual(direct["primary_type"], "Album")
        self.assertEqual(direct["country"], "Europe")
        self.assertEqual(direct["date"], "1997-06-16")
        self.assertEqual(direct["year"], 1997)
        self.assertEqual(direct["release_group_id"], "21491")
        self.assertEqual(direct["master_title"], "OK Computer")
        self.assertEqual(direct["master_first_released"], "1997")
        self.assertEqual(direct["artist_name"], "Radiohead")
        self.assertEqual(direct["artist_id"], "3840")
        self.assertEqual(direct["via_label_id"], "2294")
        self.assertIsNone(direct["sub_label_name"])  # direct-parent release
        self.assertEqual(direct["format"], "CD")

        sub = rows[1]
        self.assertEqual(sub["id"], "999111")
        self.assertEqual(sub["sub_label_name"], "Parlophone Records Ltd.")
        self.assertEqual(sub["via_label_id"], "25693")
        self.assertIsNone(sub["release_group_id"])  # masterless
        self.assertEqual(sub["primary_type"], "Single")
        self.assertEqual(sub["format"], "Vinyl")

    def test_default_pagination_kwargs(self):
        with _mock_urlopen(self.RELEASES_DATA) as mock:
            get_label_releases(2294)

        called_url = mock.call_args[0][0].full_url
        # Defaults per signature: include_sublabels=True, page=1, per_page=100
        self.assertIn("include_sublabels=true", called_url)
        self.assertIn("page=1", called_url)
        self.assertIn("per_page=100", called_url)

    def test_include_sublabels_false_passes_through(self):
        with _mock_urlopen({**self.RELEASES_DATA, "include_sublabels": False}) as mock:
            payload = get_label_releases(2294, include_sublabels=False)
        called_url = mock.call_args[0][0].full_url
        self.assertIn("include_sublabels=false", called_url)
        self.assertFalse(payload["include_sublabels"])

    def test_sub_labels_dropped_default_false(self):
        """Plan 003 U4: every successful response carries
        `sub_labels_dropped` so the contract is stable. Default False."""
        with _mock_urlopen(self.RELEASES_DATA):
            payload = get_label_releases(2294, include_sublabels=True)
        self.assertIn("sub_labels_dropped", payload)
        self.assertFalse(payload["sub_labels_dropped"])

    def test_503_falls_back_to_no_sublabels(self):
        """Plan 003 U4: when the upstream returns 503 (timeout) and the
        caller asked for sub-labels, the adapter retries once with
        include_sublabels=False and flags the response."""
        from urllib.error import HTTPError
        from io import BytesIO

        # First call (sub=true) raises 503; second call (sub=false) succeeds.
        success_resp = MagicMock()
        success_resp.read.return_value = json.dumps(
            {**self.RELEASES_DATA, "include_sublabels": False}).encode()
        success_resp.__enter__ = lambda s: s
        success_resp.__exit__ = MagicMock(return_value=False)

        def _urlopen(req, *_args, **_kwargs):
            if "include_sublabels=true" in req.full_url:
                raise HTTPError(
                    req.full_url, 503, "Service Unavailable",
                    hdrs=None,  # type: ignore[arg-type]
                    fp=BytesIO(b'{"error":"timeout"}'))
            return success_resp

        with patch("web.discogs.urllib.request.urlopen", side_effect=_urlopen):
            payload = get_label_releases(99887766, include_sublabels=True)

        self.assertTrue(payload["sub_labels_dropped"])
        # Fallback fetch ran and surfaced its successful payload
        self.assertFalse(payload["include_sublabels"])
        self.assertEqual(len(payload["results"]), 2)

    def test_503_then_503_reraises(self):
        """Plan 003 U4: if the fallback also 503s, the original HTTPError
        re-raises. No infinite retry."""
        from urllib.error import HTTPError
        from io import BytesIO

        def _always_503(req, *_args, **_kwargs):
            raise HTTPError(
                req.full_url, 503, "Service Unavailable",
                hdrs=None,  # type: ignore[arg-type]
                fp=BytesIO(b'{"error":"timeout"}'))

        with patch("web.discogs.urllib.request.urlopen", side_effect=_always_503):
            with self.assertRaises(HTTPError):
                get_label_releases(99887765, include_sublabels=True)

    def test_503_when_sub_labels_already_false_reraises(self):
        """Plan 003 U4: 503 with include_sublabels=False has nothing to fall
        back to — re-raise."""
        from urllib.error import HTTPError
        from io import BytesIO

        def _503(req, *_args, **_kwargs):
            raise HTTPError(
                req.full_url, 503, "Service Unavailable",
                hdrs=None,  # type: ignore[arg-type]
                fp=BytesIO(b'{"error":"timeout"}'))

        with patch("web.discogs.urllib.request.urlopen", side_effect=_503):
            with self.assertRaises(HTTPError):
                get_label_releases(99887764, include_sublabels=False)

    def test_404_propagates_unchanged(self):
        """Plan 003 U4: 404 surfaces as 404 (existing route maps it). The
        503 retry must not swallow other HTTP errors."""
        from urllib.error import HTTPError
        from io import BytesIO

        def _404(req, *_args, **_kwargs):
            raise HTTPError(
                req.full_url, 404, "Not Found",
                hdrs=None,  # type: ignore[arg-type]
                fp=BytesIO(b'{"error":"not found"}'))

        with patch("web.discogs.urllib.request.urlopen", side_effect=_404):
            with self.assertRaises(HTTPError):
                get_label_releases(99887763, include_sublabels=True)


if __name__ == "__main__":
    unittest.main()
