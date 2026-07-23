"""Seam tests for web/mb.py search builders.

Mirrors the urlopen-mock pattern of tests/test_discogs_api.py: patch the
leaf urllib seam and assert on the URL the builder constructs. The VA
rewrite cases are the RED tests for issue #199 — a query carrying
"Various Artists" tokens must pin `arid:<VA MBID>` instead of letting
Lucene treat the tokens as title terms.
"""

import json
import string
import unittest
import urllib.parse
from unittest.mock import MagicMock, patch

from hypothesis import given, strategies as st

from tests import _hypothesis_profiles  # noqa: F401 — registers active profile
from lib.va_identity import MB_VA_ARTIST_MBID
from web.mb import (
    _quote_mb_identifier,
    get_artist_name,
    get_artist_release_groups,
    get_release,
    get_release_group,
    search_artists,
    search_release_groups,
)


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


def _mock_urlopen_by_url(responses: dict[str, dict]):
    """Return canned JSON selected by a substring of each requested URL."""
    def _side_effect(req, *args, **kwargs):
        for needle, payload in responses.items():
            if needle in req.full_url:
                mock_resp = MagicMock()
                mock_resp.read.return_value = json.dumps(payload).encode()
                mock_resp.__enter__ = lambda s: s
                mock_resp.__exit__ = MagicMock(return_value=False)
                return mock_resp
        raise AssertionError(f"unexpected URL: {req.full_url}")

    return patch("web.mb.urllib.request.urlopen", side_effect=_side_effect)


def assert_identifier_urls_quoted(identifier: str, urls: list[str]) -> None:
    """Assert representative MB builders keep one identifier in one component."""
    quoted = _quote_mb_identifier(identifier)
    required = (
        f"/release/{quoted}?",
        f"/release-group/{quoted}?",
        f"/artist/{quoted}?",
        f"artist={quoted}",
    )
    if not all(any(fragment in url for url in urls) for fragment in required):
        raise AssertionError(f"identifier escaped its URL component: {urls!r}")


class TestMusicBrainzIdentifierUrlQuoting(unittest.TestCase):
    def test_release_identifier_is_quoted_as_one_path_component(self) -> None:
        identifier = "release/../?inc=evil&fmt=xml"
        with _mock_urlopen({}) as mock_urlopen:
            get_release(identifier, fresh=True)

        url = mock_urlopen.call_args.args[0].full_url
        self.assertIn(
            "/release/release%2F..%2F%3Finc%3Devil%26fmt%3Dxml?",
            url,
        )
        self.assertNotIn("?inc=evil", url)

    def test_artist_identifier_is_quoted_as_one_query_value(self) -> None:
        identifier = "artist&inc=evil"
        with _mock_urlopen({}) as mock_urlopen:
            get_artist_release_groups(identifier)

        urls = [call.args[0].full_url for call in mock_urlopen.call_args_list]
        self.assertEqual(len(urls), 3)
        self.assertTrue(all("artist=artist%26inc%3Devil" in url for url in urls))
        self.assertTrue(all("artist=artist&inc=evil" not in url for url in urls))

    def test_identifier_url_checker_rejects_known_bad_component(self) -> None:
        """Fault qualification: raw query syntax cannot satisfy the oracle."""
        with self.assertRaisesRegex(AssertionError, "escaped its URL component"):
            assert_identifier_urls_quoted(
                "artist&inc=evil", ["https://mb.invalid/release?artist=artist&inc=evil"],
            )

    @given(identifier=st.text(
        alphabet=string.ascii_letters + string.digits + "/?&#%= +",
        min_size=1,
        max_size=64,
    ))
    def test_arbitrary_identifier_quoting_flows_through_representative_builders(
        self, identifier: str,
    ) -> None:
        """Actual path and query builders all delegate identifier quoting."""
        with patch(
            "web.mb._cache.memoize_meta", side_effect=lambda _key, fetch, **_kw: fetch(),
        ), _mock_urlopen({}) as mock_urlopen, patch(
            "web.mb._quote_mb_identifier", wraps=_quote_mb_identifier,
        ) as quote_identifier:
            get_release(identifier, fresh=True)
            get_release_group(identifier)
            get_artist_name(identifier)
            get_artist_release_groups(identifier)

        urls = [call.args[0].full_url for call in mock_urlopen.call_args_list]
        self.assertEqual(quote_identifier.call_count, len(urls))
        self.assertTrue(all(call.args == (identifier,) for call in quote_identifier.call_args_list))
        assert_identifier_urls_quoted(identifier, urls)


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


class TestSearchArtistsRelatedIdentities(unittest.TestCase):
    def test_exact_four_tet_search_surfaces_symbol_identity(self) -> None:
        four_tet_id = "3bcff06f-675a-451f-9075-99e8657047e8"
        person_id = "cb661251-3bc2-4373-bd7c-4b1531275c4c"
        symbol_id = "2d9745dd-5dc6-4145-9453-fec582cfa9b8"
        symbol_name = "⣎⡇ꉺლ༽இ•̛)ྀ◞ ༎ຶ ༽ৣৢ؞ৢ؞ؖ ꉺლ"
        responses = {
            "/artist?query=Four%20Tet": {
                "artists": [
                    {"id": four_tet_id, "name": "Four Tet", "score": 100},
                    {"id": "other", "name": "Four Tops", "score": 45},
                ],
            },
            f"/artist/{four_tet_id}": {
                "id": four_tet_id,
                "name": "Four Tet",
                "relations": [{
                    "type": "is person",
                    "direction": "backward",
                    "artist": {"id": person_id, "name": "Kieran Hebden"},
                }],
            },
            f"/artist/{person_id}": {
                "id": person_id,
                "name": "Kieran Hebden",
                "relations": [
                    {
                        "type": "is person", "direction": "forward",
                        "artist": {"id": four_tet_id, "name": "Four Tet"},
                    },
                    {
                        "type": "is person", "direction": "forward",
                        "artist": {
                            "id": symbol_id, "name": symbol_name,
                            "disambiguation": "Kieran Hebden",
                        },
                    },
                ],
            },
        }

        with _mock_urlopen_by_url(responses):
            results = search_artists("Four Tet")

        self.assertEqual(
            [row["id"] for row in results],
            [four_tet_id, person_id, symbol_id, "other"],
        )
        self.assertEqual(results[2]["name"], symbol_name)
        self.assertEqual(results[2]["disambiguation"], "Kieran Hebden")


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
                "status": "Official",
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
                "status": "Bootleg",
                "release-group": DIRECT["release-groups"][0],
            },
        ],
    }
    DIRECT_RELEASES = {
        "release-count": 3,
        "releases": [
            {
                "id": "own-official",
                "status": "Official",
                "release-group": {"id": OWN_RG},
            },
            {
                "id": "own-promo",
                "status": "Promotion",
                "release-group": {"id": OWN_RG},
            },
            {
                "id": "unsupported-status",
                "status": "Pseudo-Release",
                "release-group": {"id": APPEARANCE_RG},
            },
        ],
    }

    def test_track_artist_release_groups_are_preserved_as_appearances(self):
        with _mock_urlopen_by_fragment({
            "/release-group?artist=": self.DIRECT,
            "/release?artist=": self.DIRECT_RELEASES,
            "/release?track_artist=": self.TRACK_APPEARANCES,
        }) as mock:
            rows = get_artist_release_groups(self.ARTIST_ID)

        called = [call.args[0].full_url for call in mock.call_args_list]
        self.assertTrue(any("/release?track_artist=" in url for url in called))
        self.assertEqual(len(rows), 2)
        by_id = {row.id: row for row in rows}
        self.assertIs(by_id[self.OWN_RG].is_appearance, False)
        self.assertIs(by_id[self.APPEARANCE_RG].is_appearance, True)
        self.assertEqual(
            by_id[self.APPEARANCE_RG].artist_credit,
            "Artists in Support of Make Trade Fair",
        )
        self.assertEqual(
            by_id[self.APPEARANCE_RG].primary_artist_id,
            MB_VA_ARTIST_MBID,
        )
        self.assertEqual(
            by_id[self.APPEARANCE_RG].secondary_types,
            ["Compilation"],
        )
        self.assertEqual(
            by_id[self.OWN_RG].provenance,
            ["ordinary", "promo", "unofficial"],
        )
        self.assertEqual(by_id[self.APPEARANCE_RG].provenance, ["ordinary"])

    def test_unknown_or_null_release_status_does_not_become_unofficial(self):
        direct_releases = {
            "release-count": 2,
            "releases": [
                {"status": None, "release-group": {"id": self.OWN_RG}},
                {"status": "Pseudo-Release", "release-group": {"id": self.OWN_RG}},
            ],
        }
        with _mock_urlopen_by_fragment({
            "/release-group?artist=": self.DIRECT,
            "/release?artist=": direct_releases,
            "/release?track_artist=": {"release-count": 0, "releases": []},
        }):
            rows = get_artist_release_groups(self.ARTIST_ID)

        self.assertEqual(rows[0].provenance, [])

    def test_null_primary_type_normalizes_to_empty_structural_evidence(self):
        artist_id = "00000000-0000-0000-0000-000000000695"
        release_group = {
            "id": "00000000-0000-0000-0000-000000000696",
            "title": "Unclassified Work",
            "primary-type": None,
            "secondary-types": [],
            "first-release-date": None,
            "artist-credit": [],
        }
        with _mock_urlopen_by_fragment({
            "/release-group?artist=": {
                "release-group-count": 1,
                "release-groups": [release_group],
            },
            "/release?artist=": {
                "release-count": 0,
                "releases": [],
            },
            "/release?track_artist=": {
                "release-count": 0,
                "releases": [],
            },
        }):
            rows = get_artist_release_groups(artist_id)

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].type, "")
        self.assertEqual(rows[0].primary_types, [])
        self.assertEqual(rows[0].first_release_date, "")


if __name__ == "__main__":
    unittest.main()
