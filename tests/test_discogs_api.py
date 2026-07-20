"""Unit tests for web/discogs.py — Discogs mirror API wrapper."""

import json
import os
import sys
import unittest
import urllib.parse
from typing import TypeGuard
from unittest.mock import patch, MagicMock

import msgspec

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import web.discogs


def _is_dict(value: object) -> TypeGuard[dict[str, object]]:
    """Narrow one of ``get_release()``/``get_master_releases()``'s
    ``dict[str, object]`` nested values for a test assertion."""
    return isinstance(value, dict)


def _is_list(value: object) -> TypeGuard[list[object]]:
    """Narrow one of ``get_release()``/``get_master_releases()``'s
    ``dict[str, object]`` nested list values for a test assertion."""
    return isinstance(value, list)


def setUpModule() -> None:
    # These tests exercise the REAL web/discogs.py with urlopen patched.
    # Since tier-2 U6 the module ships with NO default base (Discogs is
    # mirror-required, R13) — give the suite a synthetic mirror origin so
    # URL construction proceeds; assertions check paths, not the origin.
    web.discogs.DISCOGS_API_BASE = "https://discogs-mirror.test"


def tearDownModule() -> None:
    web.discogs.DISCOGS_API_BASE = None


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
        tracks = result["tracks"]
        assert _is_list(tracks)
        self.assertEqual(len(tracks), 2)
        track0 = tracks[0]
        assert _is_dict(track0)
        self.assertEqual(track0["title"], "Airbag")
        self.assertEqual(track0["disc_number"], 1)
        self.assertEqual(track0["track_number"], 1)
        self.assertEqual(track0["length_seconds"], 284.0)


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
                "formats": [{"name": "CD", "qty": 1, "descriptions": "Album, Promo"}],
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
        releases = result["releases"]
        assert _is_list(releases)
        self.assertEqual(len(releases), 2)
        release0 = releases[0]
        assert _is_dict(release0)
        self.assertEqual(release0["id"], "83182")
        self.assertEqual(release0["country"], "Europe")
        self.assertEqual(release0["format"], "CD")
        self.assertEqual(release0["date"], "1997-06-16")
        self.assertEqual(release0["track_count"], 12)
        self.assertEqual(release0["status"], "Official")
        release1 = releases[1]
        assert _is_dict(release1)
        self.assertEqual(release1["status"], "Promotion")

    def test_master_children_derive_unofficial_and_mixed_status(self):
        master = {
            "id": 1,
            "title": "Evidence",
            "releases": [
                {
                    "id": 1, "title": "Unofficial", "formats": [{
                        "name": "CD", "qty": 1,
                        "descriptions": "Album, Unofficial Release",
                    }],
                },
                {
                    "id": 2, "title": "Mixed", "formats": [{
                        "name": "CD", "qty": 1,
                        "descriptions": "Album, Promo, Unofficial Release",
                    }],
                },
            ],
        }
        with _mock_urlopen(master):
            result = get_master_releases(1)
        releases = result["releases"]
        assert _is_list(releases)
        statuses: list[object] = []
        for row in releases:
            assert _is_dict(row)
            statuses.append(row["status"])
        self.assertEqual(
            statuses,
            ["Bootleg", "Bootleg / Promo"],
        )

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
        releases = result["releases"]
        assert _is_list(releases)
        release0 = releases[0]
        assert _is_dict(release0)
        self.assertEqual(release0["track_count"], 0)
        self.assertEqual(release0["date"], "")


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

    def test_long_query_uses_bounded_cache_key(self):
        long_query = "r" * 250
        with patch("web.discogs._cache.memoize_meta", return_value=[]) as memo:
            search_releases(long_query)

        cache_key = memo.call_args[0][0]
        self.assertTrue(cache_key.startswith("discogs:search:releases:"))
        self.assertIn(f":#{len(long_query)}:", cache_key)
        self.assertLess(len(cache_key), len(f"discogs:search:releases:{long_query}"))


class TestSearchReleasesVaRewrite(unittest.TestCase):
    """VA-token handling in the Discogs title search (#199).

    The dump's VA artist (id 194) has no name row, so "Various Artists"
    tokens can never match — pre-fix they ANDed into the title match and
    returned zero results. The fix strips the tokens from the title and
    pins the mirror's ``artist_id=194`` exact filter so the mirror itself
    returns only VA-credited releases.
    """

    SEARCH_DATA = {
        "results": [
            {
                "id": 32457180,
                "title": "Rock Christmas (The Very Best Of)",
                "master_id": 3673686,
                "master_title": "Rock Christmas (The Very Best Of)",
                "master_first_released": "1992",
                "primary_type": "Album",
                "score": 0.10,
                "released": "2024",
                "artists": [{"id": 194, "name": "Various"}],
            },
        ],
    }

    def _requested_qs(self, mock_urlopen) -> dict:
        url = mock_urlopen.call_args[0][0].full_url
        return urllib.parse.parse_qs(urllib.parse.urlparse(url).query)

    def test_va_query_strips_tokens_and_pins_artist_id(self):
        with _mock_urlopen(self.SEARCH_DATA) as m:
            search_releases("Rock Christmas Various Artists")
        qs = self._requested_qs(m)
        self.assertEqual(qs["title"][0], "Rock Christmas")
        self.assertEqual(qs["artist_id"][0], "194")

    def test_plain_query_sends_no_artist_id(self):
        with _mock_urlopen(self.SEARCH_DATA) as m:
            search_releases("Rock Christmas")
        qs = self._requested_qs(m)
        self.assertEqual(qs["title"][0], "Rock Christmas")
        self.assertNotIn("artist_id", qs)

    def test_va_only_query_keeps_raw_title_and_no_pin(self):
        # No title remainder after the strip — keep the raw passthrough
        # rather than pinning artist_id with an empty title (which would
        # make the mirror scan every one of artist 194's releases).
        with _mock_urlopen(self.SEARCH_DATA) as m:
            search_releases("Various Artists")
        qs = self._requested_qs(m)
        self.assertEqual(qs["title"][0], "Various Artists")
        self.assertNotIn("artist_id", qs)

    def _cache_key_for(self, query: str) -> str:
        with patch("web.discogs._cache.memoize_meta", return_value=[]) as memo:
            search_releases(query)
        return memo.call_args[0][0]

    def test_va_query_uses_distinct_cache_key(self):
        # The artist_id-pinned fetch is a different upstream query than
        # the bare-title fetch, so it must NOT collide with the plain
        # "Rock Christmas" cache entry.
        va_key = self._cache_key_for("Rock Christmas Various Artists")
        plain_key = self._cache_key_for("Rock Christmas")
        self.assertNotEqual(va_key, plain_key)
        self.assertTrue(va_key.startswith("discogs:search:releases:"))

    def test_va_flag_cannot_be_forged_by_user_text(self):
        # The va discriminator sits before the user query text, so a plain
        # query crafted to look like the VA key's tail must not collide.
        va_key = self._cache_key_for("Rock Christmas Various Artists")
        for adversarial in ("Rock Christmas:va", "va=1:Rock Christmas",
                            "Rock Christmas va=1"):
            self.assertNotEqual(self._cache_key_for(adversarial), va_key)


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
        called_url = mock.call_args_list[0][0][0].full_url
        self.assertIn("/api/artists?name=", called_url)
        self.assertNotIn("/api/search?artist=", called_url)

        self.assertEqual(len(results), 2)
        self.assertEqual(results[0]["id"], "3840")
        self.assertEqual(results[0]["name"], "Radiohead")
        self.assertEqual(results[0]["disambiguation"], "")  # left empty intentionally
        self.assertEqual(results[0]["score"], 6)  # int(0.06 * 100)
        self.assertEqual(results[1]["name"], "Radioheads")

    def test_long_query_uses_bounded_cache_key(self):
        long_query = "a" * 250
        with patch("web.discogs._cache.memoize_meta", return_value=[]) as memo:
            search_artists(long_query)

        cache_key = memo.call_args[0][0]
        self.assertTrue(cache_key.startswith("discogs:search:artists:"))
        self.assertIn(f":#{len(long_query)}:", cache_key)
        self.assertLess(len(cache_key), len(f"discogs:search:artists:{long_query}"))

    def test_exact_four_tet_search_surfaces_symbol_alias(self):
        symbol_name = "⣎⡇ꉺლ༽இ•̛)ྀ◞ ༎ຶ ༽ৣৢ؞ৢ؞ؖ ꉺლ"
        search_data = {
            "results": [
                {"id": 3543, "name": "Four Tet", "score": 1.0},
                {"id": 2039081, "name": "The Urge Four Tet", "score": 0.09},
            ],
        }
        detail = {
            "id": 3543,
            "name": "Four Tet",
            "aliases": [
                {"id": 60342, "name": "Kieran Hebden"},
                {"id": 6400214, "name": symbol_name},
            ],
        }
        with _mock_urlopen_by_url({
            "/api/artists?name=Four%20Tet": search_data,
            "/api/artists/3543": detail,
        }):
            results = search_artists("Four Tet")

        self.assertEqual(
            [row["id"] for row in results[:4]],
            ["3543", "60342", "6400214", "2039081"],
        )
        self.assertEqual(results[2]["name"], symbol_name)


def _mock_urlopen_by_url(responses: dict):
    """Mock urllib.request.urlopen with per-URL-substring responses.

    ``responses`` is a dict mapping a substring of the URL to the JSON payload
    that should come back. Each match is independent — callers can mock
    /masters and /appearances with different bodies in the same context.
    """

    def _side_effect(req, *args, **kwargs):
        url = req.full_url
        for needle, payload in responses.items():
            if needle in url:
                mock_resp = MagicMock()
                mock_resp.read.return_value = json.dumps(payload).encode()
                mock_resp.__enter__ = lambda s: s
                mock_resp.__exit__ = MagicMock(return_value=False)
                return mock_resp
        raise AssertionError(f"no mock response configured for URL: {url}")

    return patch("web.discogs.urllib.request.urlopen", side_effect=_side_effect)


class TestGetArtistReleases(unittest.TestCase):
    """get_artist_releases() merges /masters + /appearances from the mirror."""

    MASTERS_DATA = {
        "results": [
            {
                "id": 21481,
                "title": "Creep",
                "type": "EP",
                "primary_types": ["EP", "Single"],
                "format_qualifiers": ["12\"", "EP"],
                "provenance": ["ordinary", "promo"],
                "first_release_date": "1992",
                "artist_credit": "Radiohead",
                "primary_artist_id": 3840,
                "is_masterless": False,
            },
            {
                "id": 13344,
                "title": "Pablo Honey",
                "type": "Album",
                "primary_types": ["Album"],
                "format_qualifiers": ["Album", "LP"],
                "provenance": ["ordinary"],
                "first_release_date": "1993",
                "artist_credit": "Radiohead",
                "primary_artist_id": 3840,
                "is_masterless": False,
            },
            {
                "id": "release-83182",
                "title": "Stupid Car (demo)",
                "type": "Other",
                "primary_types": [],
                "format_qualifiers": ["Unofficial Release"],
                "provenance": ["unofficial"],
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

    EMPTY_APPEARANCES = {"results": [], "total": 0, "page": 1, "per_page": 1}

    def _assert_incomplete_envelope_rejected(
        self, *, endpoint: str, payload: dict,
    ) -> None:
        responses = {
            "/masters": self.MASTERS_DATA,
            "/appearances": self.EMPTY_APPEARANCES,
        }
        responses[endpoint] = payload
        with _mock_urlopen_by_url(responses), self.assertRaises(
            web.discogs.DiscogsArtistCatalogueIncomplete,
        ):
            get_artist_releases(3840)

    def test_rejects_truncated_masters_envelope(self):
        self._assert_incomplete_envelope_rejected(
            endpoint="/masters",
            payload={**self.MASTERS_DATA, "total": 4},
        )

    def test_rejects_nonfirst_masters_page(self):
        self._assert_incomplete_envelope_rejected(
            endpoint="/masters",
            payload={**self.MASTERS_DATA, "page": 2},
        )

    def test_rejects_truncated_appearances_envelope(self):
        self._assert_incomplete_envelope_rejected(
            endpoint="/appearances",
            payload={**self.EMPTY_APPEARANCES, "total": 1},
        )

    def test_rejects_nonfirst_appearances_page(self):
        self._assert_incomplete_envelope_rejected(
            endpoint="/appearances",
            payload={**self.EMPTY_APPEARANCES, "page": 2},
        )

    def test_normalizes_master_discography(self):
        with _mock_urlopen_by_url({
            "/masters": self.MASTERS_DATA,
            "/appearances": self.EMPTY_APPEARANCES,
        }) as mock, patch(
            "web.discogs._cache.memoize_meta",
            side_effect=lambda _key, fetch: fetch(),
        ) as memo:
            results = msgspec.to_builtins(get_artist_releases(3840))

        called_urls = [c.args[0].full_url for c in mock.call_args_list]
        self.assertEqual(
            called_urls,
            [
                "https://discogs-mirror.test/api/artists/3840/masters/all",
                "https://discogs-mirror.test/api/artists/3840/appearances",
            ],
            "cold artist metadata must use one explicit fail-loud bulk request",
        )
        self.assertEqual(
            memo.call_args.args[0], "discogs:artist:3840:releases:v7",
        )

        self.assertEqual(len(results), 3)

        album = next(r for r in results if r["title"] == "Pablo Honey")
        self.assertEqual(album["id"], "13344")
        self.assertEqual(album["type"], "Album")
        self.assertEqual(album["source"], "discogs")
        self.assertEqual(album["identity_kind"], "work")
        self.assertEqual(album["primary_types"], ["Album"])
        self.assertEqual(album["first_release_date"], "1993")
        self.assertEqual(album["artist_credit"], "Radiohead")
        self.assertEqual(album["primary_artist_id"], "3840")
        self.assertEqual(album["secondary_types"], [])
        self.assertIs(album["is_appearance"], False)
        self.assertEqual(album["provenance"], ["ordinary"])

        masterless = next(r for r in results if r["title"] == "Stupid Car (demo)")
        self.assertEqual(masterless["id"], "83182")  # "release-" prefix stripped
        self.assertEqual(masterless.get("discogs_release_id"), "83182")
        self.assertEqual(masterless["primary_types"], [])
        self.assertEqual(masterless["identity_kind"], "release")
        self.assertEqual(masterless["provenance"], ["unofficial"])

    def test_appearances_merged_and_classified_as_non_primary(self):
        appearances = {
            "results": [
                {
                    "id": 555,
                    "title": "Indie 1996",
                    "type": "Album",
                    "primary_types": ["Album"],
                    "format_qualifiers": ["Album"],
                    "provenance": ["ordinary"],
                    "first_release_date": "1996",
                    "artist_credit": "Various",
                    "primary_artist_id": 194,
                    "is_masterless": False,
                },
            ],
            "total": 1,
            "page": 1,
            "per_page": 1,
        }
        with _mock_urlopen_by_url({
            "/masters": self.MASTERS_DATA,
            "/appearances": appearances,
        }):
            results = msgspec.to_builtins(get_artist_releases(3840))
        comp = next(r for r in results if r["title"] == "Indie 1996")
        self.assertEqual(comp["primary_artist_id"], "194")
        self.assertEqual(comp["artist_credit"], "Various")
        self.assertIs(comp["is_appearance"], True)
        # The JS classifier reads primary_artist_id !== artist_id to route into
        # the Appearances section — so it must NOT equal the queried artist id.
        self.assertNotEqual(comp["primary_artist_id"], "3840")
        self.assertEqual(len(results), 4)

    def test_dedup_masters_wins_over_appearances(self):
        """When a master shows up in BOTH endpoints (split release where the
        artist is a primary credit on one release and a track-only credit on
        a sibling release in the same master), the /masters classification
        wins — we don't downgrade an own-work master to an appearance."""
        appearance_dup = {
            "results": [
                {
                    "id": 13344,  # same master id as Pablo Honey in /masters
                    "title": "Pablo Honey (Various comp version)",
                    "type": "Album",
                    "primary_types": ["Album"],
                    "format_qualifiers": ["Album"],
                    "provenance": ["ordinary"],
                    "first_release_date": "1993",
                    "artist_credit": "Various",
                    "primary_artist_id": 194,
                    "is_masterless": False,
                },
            ],
            "total": 1,
            "page": 1,
            "per_page": 1,
        }
        with _mock_urlopen_by_url({
            "/masters": self.MASTERS_DATA,
            "/appearances": appearance_dup,
        }):
            results = msgspec.to_builtins(get_artist_releases(3840))
        self.assertEqual(len(results), 3)
        pablo = next(r for r in results if r["id"] == "13344")
        self.assertEqual(pablo["artist_credit"], "Radiohead")
        self.assertEqual(pablo["primary_artist_id"], "3840")
        self.assertIs(pablo["is_appearance"], False)

    def test_duplicate_primary_credit_rows_keep_first_projection(self):
        """Duplicate release_artist credits are one catalogue identity."""
        duplicate = {
            **self.MASTERS_DATA,
            "results": [
                self.MASTERS_DATA["results"][0],
                {
                    **self.MASTERS_DATA["results"][0],
                    "title": "duplicate credit must not replace the first",
                },
            ],
            "total": 2,
        }
        with _mock_urlopen_by_url({
            "/masters": duplicate,
            "/appearances": self.EMPTY_APPEARANCES,
        }):
            results = msgspec.to_builtins(get_artist_releases(3840))

        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["title"], "Creep")

    def test_master_and_same_numeric_masterless_release_both_survive(self):
        """Master and release ids occupy separate Discogs namespaces."""
        masters = {
            **self.MASTERS_DATA,
            "results": [
                self.MASTERS_DATA["results"][0],
                {
                    **self.MASTERS_DATA["results"][2],
                    "id": "release-21481",
                },
            ],
            "total": 2,
        }
        with _mock_urlopen_by_url({
            "/masters": masters,
            "/appearances": self.EMPTY_APPEARANCES,
        }):
            results = msgspec.to_builtins(get_artist_releases(3840))

        collisions = [row for row in results if row["id"] == "21481"]
        self.assertEqual(len(collisions), 2)
        self.assertEqual(
            [row["identity_kind"] for row in collisions],
            ["work", "release"],
        )

    def test_rejects_malformed_or_nonpositive_artist_identity_ids(self):
        for raw_id, is_masterless in (
            ("foo", True),
            ("release-", True),
            ("release-abc", True),
            ("release-0", True),
            ("release--1", True),
            (0, False),
            (-1, False),
            ("122", False),
        ):
            with self.subTest(raw_id=raw_id, is_masterless=is_masterless):
                invalid_row = {
                    **self.MASTERS_DATA["results"][0],
                    "id": raw_id,
                    "is_masterless": is_masterless,
                }
                payload = {
                    **self.MASTERS_DATA,
                    "results": [invalid_row],
                    "total": 1,
                }
                with _mock_urlopen_by_url({
                    "/masters": payload,
                    "/appearances": self.EMPTY_APPEARANCES,
                }), self.assertRaises(ValueError):
                    get_artist_releases(3840)

    def test_missing_primary_types_is_rejected_at_boundary(self):
        invalid = {
            **self.MASTERS_DATA,
            "results": [
                {
                    key: value
                    for key, value in self.MASTERS_DATA["results"][0].items()
                    if key != "primary_types"
                },
            ],
            "total": 1,
        }
        with _mock_urlopen_by_url({
            "/masters": invalid,
            "/appearances": self.EMPTY_APPEARANCES,
        }):
            with self.assertRaises(msgspec.ValidationError):
                get_artist_releases(3840)

    def test_wrong_primary_types_element_is_rejected_at_boundary(self):
        invalid = {
            **self.MASTERS_DATA,
            "results": [
                {
                    **self.MASTERS_DATA["results"][0],
                    "primary_types": [7],
                    "format_qualifiers": ["Album"],
                    "provenance": ["ordinary"],
                },
            ],
            "total": 1,
        }
        with _mock_urlopen_by_url({
            "/masters": invalid,
            "/appearances": self.EMPTY_APPEARANCES,
        }):
            with self.assertRaises(msgspec.ValidationError):
                get_artist_releases(3840)

    def test_missing_provenance_is_rejected_at_boundary(self):
        invalid_row = {
            key: value
            for key, value in self.MASTERS_DATA["results"][0].items()
            if key != "provenance"
        }
        invalid = {
            **self.MASTERS_DATA,
            "results": [invalid_row],
            "total": 1,
        }
        with _mock_urlopen_by_url({
            "/masters": invalid,
            "/appearances": self.EMPTY_APPEARANCES,
        }), self.assertRaises(msgspec.ValidationError):
            get_artist_releases(3840)

    def test_wrong_provenance_element_is_rejected_at_boundary(self):
        invalid = {
            **self.MASTERS_DATA,
            "results": [{
                **self.MASTERS_DATA["results"][0],
                "provenance": [7],
            }],
            "total": 1,
        }
        with _mock_urlopen_by_url({
            "/masters": invalid,
            "/appearances": self.EMPTY_APPEARANCES,
        }), self.assertRaises(msgspec.ValidationError):
            get_artist_releases(3840)

    def test_invalid_appearance_row_is_rejected_at_boundary(self):
        invalid_appearances = {
            "results": [{
                "id": 555,
                "title": "Sampler",
                "type": "Album",
                "primary_types": ["Compilation"],
                "format_qualifiers": ["Compilation"],
                "provenance": ["ordinary"],
                "first_release_date": "2001",
                "artist_credit": "Various",
                "primary_artist_id": 194,
                "is_masterless": False,
            }],
            "total": 1,
            "page": 1,
            "per_page": 1,
        }
        with _mock_urlopen_by_url({
            "/masters": self.MASTERS_DATA,
            "/appearances": invalid_appearances,
        }):
            with self.assertRaises(msgspec.ValidationError):
                get_artist_releases(3840)

    def test_null_primary_artist_id_normalizes_to_empty_string(self):
        null_artist = {
            "results": [{
                "id": 60,
                "title": "Mixed appearance master",
                "type": "EP",
                "primary_types": ["EP", "Single"],
                "format_qualifiers": ["EP"],
                "provenance": ["ordinary"],
                "first_release_date": "2005",
                "artist_credit": "",
                "primary_artist_id": None,
                "is_masterless": False,
            }],
            "total": 1,
            "page": 1,
            "per_page": 100,
        }
        with _mock_urlopen_by_url({
            "/masters": null_artist,
            "/appearances": self.EMPTY_APPEARANCES,
        }):
            results = msgspec.to_builtins(get_artist_releases(3840))
        self.assertEqual(results[0]["primary_artist_id"], "")


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

    def test_long_query_uses_bounded_distinct_cache_key(self):
        q1 = "x" * 250
        q2 = ("x" * 200) + ("y" * 50)

        with patch("web.discogs._cache.memoize_meta", return_value=[]) as memo:
            search_labels(q1)
            search_labels(q2)

        key1 = memo.call_args_list[0].args[0]
        key2 = memo.call_args_list[1].args[0]
        self.assertNotEqual(key1, key2)
        self.assertIn(f":#{len(q1)}:", key1)
        self.assertIn(f":#{len(q2)}:", key2)
        self.assertLess(len(key1), len(f"discogs:search:labels:{q1}:p=1:pp=25"))


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
        self.assertEqual(entity.sub_labels, [
            {"id": 25693, "name": "Parlophone Records Ltd.", "release_count": 412},
        ])

    def test_sub_label_has_parent(self):
        with _mock_urlopen(self.SUB_LABEL_DATA):
            entity = get_label(25693)

        self.assertEqual(entity.parent_label_id, "2294")
        self.assertEqual(entity.parent_label_name, "Parlophone")
        self.assertEqual(entity.release_count, 412)
        self.assertEqual(entity.sub_labels, [])

    def test_rejects_non_numeric_label_id(self):
        with self.assertRaises(AssertionError):
            get_label("../etc/passwd")

        with self.assertRaises(AssertionError):
            get_label("123 OR 1=1")


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
                "label_id": 2294,
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
                "label_id": 25693,
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
        pagination = payload["pagination"]
        assert _is_dict(pagination)
        self.assertEqual(pagination["items"], 2)

        rows = payload["results"]
        assert _is_list(rows)
        self.assertEqual(len(rows), 2)

        direct = rows[0]
        assert _is_dict(direct)
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
        self.assertEqual(direct["label_id"], "2294")
        self.assertEqual(direct["via_label_id"], "2294")
        self.assertIsNone(direct["sub_label_name"])  # direct-parent release
        self.assertEqual(direct["format"], "CD")

        sub = rows[1]
        assert _is_dict(sub)
        self.assertEqual(sub["id"], "999111")
        self.assertEqual(sub["sub_label_name"], "Parlophone Records Ltd.")
        self.assertEqual(sub["label_id"], "25693")
        self.assertEqual(sub["via_label_id"], "25693")
        self.assertIsNone(sub["release_group_id"])  # masterless
        self.assertEqual(sub["primary_type"], "Single")
        self.assertEqual(sub["format"], "Vinyl")

    def test_accepts_legacy_via_label_id_payload(self):
        legacy = json.loads(json.dumps(self.RELEASES_DATA))
        for row in legacy["results"]:
            row["via_label_id"] = row.pop("label_id")

        with _mock_urlopen(legacy):
            payload = get_label_releases(112294, include_sublabels=True)

        results = payload["results"]
        assert _is_list(results)
        result0 = results[0]
        assert _is_dict(result0)
        self.assertEqual(result0["label_id"], "2294")
        self.assertEqual(result0["via_label_id"], "2294")

    def test_default_pagination_kwargs(self):
        with _mock_urlopen(self.RELEASES_DATA) as mock:
            get_label_releases(2294)

        called_url = mock.call_args[0][0].full_url
        # Defaults per signature: include_sublabels=True, page=1, per_page=100
        self.assertIn("include_sublabels=true", called_url)
        self.assertIn("page=1", called_url)
        self.assertIn("per_page=100", called_url)

    def test_rejects_non_numeric_label_id(self):
        with self.assertRaises(AssertionError):
            get_label_releases("../etc/passwd")

        with self.assertRaises(AssertionError):
            get_label_releases("123 OR 1=1")

    def test_include_sublabels_false_passes_through(self):
        with _mock_urlopen({**self.RELEASES_DATA, "include_sublabels": False}) as mock:
            payload = get_label_releases(2294, include_sublabels=False)
        called_url = mock.call_args[0][0].full_url
        self.assertIn("include_sublabels=false", called_url)
        self.assertFalse(payload["include_sublabels"])

    def test_sub_labels_dropped_default_false(self):
        """Plan 002 U3: every successful response carries
        `sub_labels_dropped` so the contract is stable. Default False."""
        with _mock_urlopen(self.RELEASES_DATA):
            payload = get_label_releases(2294, include_sublabels=True)
        self.assertIn("sub_labels_dropped", payload)
        self.assertFalse(payload["sub_labels_dropped"])

    def test_503_falls_back_to_no_sublabels(self):
        """Plan 002 U3: when the upstream returns 503 (timeout) and the
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

        seen_urls = []

        def _urlopen(req, *_args, **_kwargs):
            seen_urls.append(req.full_url)
            if "include_sublabels=true" in req.full_url:
                raise HTTPError(
                    req.full_url, 503, "Service Unavailable",
                    hdrs=None,  # type: ignore[arg-type]
                    fp=BytesIO(b'{"error":"timeout"}'))
            return success_resp

        with patch("web.discogs.urllib.request.urlopen", side_effect=_urlopen):
            payload = get_label_releases(
                99887766, include_sublabels=True, page=3, per_page=50)

        self.assertTrue(payload["sub_labels_dropped"])
        # Fallback fetch ran and surfaced its successful payload
        self.assertFalse(payload["include_sublabels"])
        fallback_results = payload["results"]
        assert _is_list(fallback_results)
        self.assertEqual(len(fallback_results), 2)
        self.assertIn("include_sublabels=true", seen_urls[0])
        self.assertIn("page=3", seen_urls[0])
        self.assertIn("per_page=50", seen_urls[0])
        self.assertIn("include_sublabels=false", seen_urls[1])
        self.assertIn("page=3", seen_urls[1])
        self.assertIn("per_page=50", seen_urls[1])

    def test_timeout_falls_back_to_no_sublabels(self):
        success_resp = MagicMock()
        success_resp.read.return_value = json.dumps(
            {**self.RELEASES_DATA, "include_sublabels": False}).encode()
        success_resp.__enter__ = lambda s: s
        success_resp.__exit__ = MagicMock(return_value=False)

        def _urlopen(req, *_args, **_kwargs):
            if "include_sublabels=true" in req.full_url:
                raise TimeoutError("timed out")
            return success_resp

        with patch("web.discogs.urllib.request.urlopen", side_effect=_urlopen):
            payload = get_label_releases(99887762, include_sublabels=True)

        self.assertTrue(payload["sub_labels_dropped"])
        self.assertFalse(payload["include_sublabels"])

    def test_include_sublabels_uses_bounded_timeout(self):
        seen_timeouts = []

        def _urlopen(req, *_args, **kwargs):
            seen_timeouts.append(kwargs.get("timeout"))
            mock_resp = MagicMock()
            mock_resp.read.return_value = json.dumps(self.RELEASES_DATA).encode()
            mock_resp.__enter__ = lambda s: s
            mock_resp.__exit__ = MagicMock(return_value=False)
            return mock_resp

        with patch("web.discogs._cache.memoize_meta",
                   side_effect=lambda _key, fn: fn()), \
                patch("web.discogs.urllib.request.urlopen", side_effect=_urlopen):
            get_label_releases(99887761, include_sublabels=True)
            get_label_releases(99887760, include_sublabels=False)

        self.assertEqual(seen_timeouts, [20, 60])

    def test_503_then_503_reraises(self):
        """Plan 002 U3: if the fallback also 503s, the original HTTPError
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
        """Plan 002 U3: 503 with include_sublabels=False has nothing to fall
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
        """Plan 002 U3: 404 surfaces as 404 (existing route maps it). The
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
