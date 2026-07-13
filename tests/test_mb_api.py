"""Seam tests for web/mb.py search builders.

Mirrors the urlopen-mock pattern of tests/test_discogs_api.py: patch the
leaf urllib seam and assert on the URL the builder constructs. The VA
rewrite cases are the RED tests for issue #199 — a query carrying
"Various Artists" tokens must pin `arid:<VA MBID>` instead of letting
Lucene treat the tokens as title terms.
"""

import json
import unittest
import urllib.parse
from unittest.mock import MagicMock, patch

from lib.va_identity import MB_VA_ARTIST_MBID
from web.mb import get_artist_release_groups, search_release_groups


def _mock_urlopen(response_data):
    """Patch web.mb's urlopen to return canned JSON; capture the Request."""
    mock_resp = MagicMock()
    mock_resp.read.return_value = json.dumps(response_data).encode()
    mock_resp.__enter__ = lambda s: s
    mock_resp.__exit__ = MagicMock(return_value=False)
    return patch("web.mb.urllib.request.urlopen", return_value=mock_resp)


def _mock_urlopen_by_fragment(responses):
    """Return the payload whose URL fragment matches the request."""
    def _side_effect(request, **_kwargs):
        for fragment, payload in responses.items():
            if fragment in request.full_url:
                mock_resp = MagicMock()
                mock_resp.read.return_value = json.dumps(payload).encode()
                mock_resp.__enter__ = lambda s: s
                mock_resp.__exit__ = MagicMock(return_value=False)
                return mock_resp
        raise AssertionError(f"no response for {request.full_url}")

    return patch("web.mb.urllib.request.urlopen", side_effect=_side_effect)


_EMPTY = {"releases": []}

_ONE_RELEASE = {
    "releases": [
        {
            "id": "rel-1",
            "title": "Rock Christmas: The Very Best Of",
            "score": 100,
            "date": "2024",
            "release-group": {
                "id": "rg-1",
                "title": "Rock Christmas: The Very Best Of",
                "primary-type": "Album",
                "first-release-date": "2024",
            },
            "artist-credit": [
                {"artist": {"id": MB_VA_ARTIST_MBID, "name": "Various Artists",
                            "disambiguation": "add compilations to this artist"}},
            ],
        },
    ],
}


def _requested_query(mock_urlopen: MagicMock) -> str:
    """Extract the decoded ?query= value from the captured Request."""
    url = mock_urlopen.call_args[0][0].full_url
    qs = urllib.parse.urlparse(url).query
    return urllib.parse.parse_qs(qs)["query"][0]


class TestSearchReleaseGroupsVaRewrite(unittest.TestCase):
    def test_va_query_pins_arid_and_strips_tokens(self) -> None:
        with _mock_urlopen(_EMPTY) as m:
            search_release_groups("Rock Christmas Various Artists")
        q = _requested_query(m)
        self.assertEqual(q, f"arid:{MB_VA_ARTIST_MBID} AND (Rock Christmas)")

    def test_plain_query_passes_through_unchanged(self) -> None:
        with _mock_urlopen(_EMPTY) as m:
            search_release_groups("Rock Christmas")
        self.assertEqual(_requested_query(m), "Rock Christmas")

    def test_va_only_query_falls_back_to_raw(self) -> None:
        # "Various Artists" alone leaves no title remainder; an arid-only
        # pin would return 25 arbitrary VA releases, which is no more
        # useful than today's behaviour — keep the raw passthrough.
        with _mock_urlopen(_EMPTY) as m:
            search_release_groups("Various Artists")
        self.assertEqual(_requested_query(m), "Various Artists")

    def test_title_containing_various_is_not_rewritten(self) -> None:
        with _mock_urlopen(_EMPTY) as m:
            search_release_groups("Various Positions")
        self.assertEqual(_requested_query(m), "Various Positions")

    def test_cache_key_uses_effective_query(self) -> None:
        # Pre-fix VA queries cached junk/empty results under the raw
        # string; keying on the rewritten query bypasses those entries.
        with patch("web.mb._cache.memoize_meta", return_value=[]) as memo:
            search_release_groups("Rock Christmas Various Artists")
        key = memo.call_args[0][0]
        self.assertEqual(
            key,
            "mb:search:release_groups:"
            f"arid:{MB_VA_ARTIST_MBID} AND (Rock Christmas)",
        )

    def test_va_results_normalized_like_plain_results(self) -> None:
        with _mock_urlopen(_ONE_RELEASE):
            results = search_release_groups("Rock Christmas Various Artists")
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["id"], "rg-1")
        self.assertEqual(results[0]["artist_name"], "Various Artists")
        self.assertEqual(results[0]["primary_type"], "Album")
        self.assertEqual(results[0]["score"], 100)


class TestArtistReleaseGroupsWithAppearances(unittest.TestCase):
    ARTIST_ID = "4fa9413b-7c10-4342-8ddb-b1cd8e82f9e1"
    OWN_RG = "fdb22921-b4c5-3c49-b2d0-85cb69eec1f1"
    APPEARANCE_RG = "2e3dd447-ac5e-3b60-b44c-f9e6000ba6e7"

    DIRECT = {
        "release-group-count": 1,
        "release-groups": [{
            "id": OWN_RG,
            "title": "The Pointless Gift",
            "primary-type": "Album",
            "secondary-types": [],
            "first-release-date": "2000-12-05",
            "artist-credit": [{
                "name": "Deloris",
                "artist": {"id": ARTIST_ID, "name": "Deloris"},
            }],
        }],
    }
    TRACK_APPEARANCES = {
        "release-count": 2,
        "releases": [
            {
                "id": "appearance-release",
                "release-group": {
                    "id": APPEARANCE_RG,
                    "title": "The Big Noise",
                    "primary-type": "Album",
                    "secondary-types": ["Compilation"],
                    "first-release-date": "2003-09-06",
                    "artist-credit": [{
                        "name": "Artists in Support of Make Trade Fair",
                        "artist": {
                            "id": MB_VA_ARTIST_MBID,
                            "name": "Various Artists",
                        },
                    }],
                },
            },
            {
                "id": "duplicate-own-release",
                "release-group": DIRECT["release-groups"][0],
            },
        ],
    }

    def test_track_artist_release_groups_are_preserved_as_appearances(self):
        with _mock_urlopen_by_fragment({
            "/release-group?artist=": self.DIRECT,
            "/release?track_artist=": self.TRACK_APPEARANCES,
        }) as mock:
            rows = get_artist_release_groups(self.ARTIST_ID)

        called = [call.args[0].full_url for call in mock.call_args_list]
        self.assertTrue(any("/release?track_artist=" in url for url in called))
        self.assertEqual(len(rows), 2)
        by_id = {row["id"]: row for row in rows}
        self.assertIs(by_id[self.OWN_RG]["is_appearance"], False)
        self.assertIs(by_id[self.APPEARANCE_RG]["is_appearance"], True)
        self.assertEqual(
            by_id[self.APPEARANCE_RG]["artist_credit"],
            "Artists in Support of Make Trade Fair",
        )
        self.assertEqual(
            by_id[self.APPEARANCE_RG]["primary_artist_id"],
            MB_VA_ARTIST_MBID,
        )
        self.assertEqual(
            by_id[self.APPEARANCE_RG]["secondary_types"],
            ["Compilation"],
        )


if __name__ == "__main__":
    unittest.main()
