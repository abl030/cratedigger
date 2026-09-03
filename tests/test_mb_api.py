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
import uuid
from typing import ClassVar
from unittest.mock import MagicMock, patch

import msgspec
from hypothesis import given
from hypothesis import strategies as st

import tests._hypothesis_profiles  # noqa: F401 — registers active profile
from lib.va_identity import MB_VA_ARTIST_MBID
from web.mb import (
    _MBArtistCreditName,
    _MBArtistRef,
    _MBReleaseGroupRef,
    _normalize_artist_release_group,
    _quote_mb_identifier,
    get_artist_name,
    get_artist_release_groups,
    get_artist_releases_with_recordings,
    get_release,
    get_release_group,
    get_release_group_releases,
    get_release_group_year,
    search_artists,
    search_release_groups,
)

# URL-construction tests use the public default base but never intend to test
# pacing. Bypass the policy function itself: a no-op sleep would still mutate
# the real module-global public schedule and contaminate later timing tests.
_public_pacing_patch = patch("web.mb._wait_for_public_musicbrainz", lambda _url: None)


def setUpModule() -> None:
    _public_pacing_patch.start()


def tearDownModule() -> None:
    _public_pacing_patch.stop()


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
    def test_identifier_quote_is_the_stdlib_component_encoding(
        self, identifier: str,
    ) -> None:
        """One identifier must remain exactly one URL component."""
        self.assertEqual(
            _quote_mb_identifier(identifier),
            urllib.parse.quote(identifier, safe=""),
        )

    @given(value=st.uuids())
    def test_fresh_identifier_quoting_flows_through_representative_builders(
        self, value: uuid.UUID,
    ) -> None:
        """Actual path and query builders preserve a hostile fresh identifier."""
        identifier = f"{value}/?inc=evil&fmt=xml"
        with _mock_urlopen({}) as mock_urlopen:
            get_release(identifier, fresh=True)
            get_release_group(identifier)
            get_artist_name(identifier)
            get_artist_release_groups(identifier)

        urls = [call.args[0].full_url for call in mock_urlopen.call_args_list]
        self.assertEqual(len(urls), 6)
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
        self.assertEqual(results[0], {
            "id": "rg-1",
            "title": "Rock Christmas: The Very Best Of",
            "primary_type": "Album",
            "first_release_date": "2024",
            "artist_id": MB_VA_ARTIST_MBID,
            "artist_name": "Various Artists",
            "artist_disambiguation": "add compilations to this artist",
            "score": 100,
        })


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

    DIRECT: ClassVar = {
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
    TRACK_APPEARANCES: ClassVar = {
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
    DIRECT_RELEASES: ClassVar = {
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
                {"id": "release-null-status", "status": None, "release-group": {"id": self.OWN_RG}},
                {"id": "release-pseudo", "status": "Pseudo-Release", "release-group": {"id": self.OWN_RG}},
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


class TestNormalizeArtistReleaseGroup(unittest.TestCase):
    """``_normalize_artist_release_group`` field-by-field — the existing
    ``TestArtistReleaseGroupsWithAppearances`` above never asserts
    ``.title``/``.type``/``.first_release_date``/``.primary_types``, and
    never exercises a multi-name ``artist-credit`` join."""

    def test_full_field_mapping_with_multi_artist_credit(self) -> None:
        rg = _MBReleaseGroupRef(
            id="rg-multi",
            title="Split Release",
            primary_type="EP",
            secondary_types=["Live"],
            first_release_date="2003-05-01",
            artist_credit=[
                _MBArtistCreditName(
                    name="Artist A",
                    artist=_MBArtistRef(id="artist-a-id", name="Artist A"),
                ),
                _MBArtistCreditName(
                    name="Artist B",
                    artist=_MBArtistRef(id="artist-b-id", name="Artist B"),
                ),
            ],
        )
        row = _normalize_artist_release_group(rg, is_appearance=True)
        self.assertEqual(row.id, "rg-multi")
        self.assertEqual(row.title, "Split Release")
        self.assertEqual(row.type, "EP")
        self.assertEqual(row.primary_types, ["EP"])
        self.assertEqual(row.secondary_types, ["Live"])
        self.assertEqual(row.first_release_date, "2003-05-01")
        self.assertEqual(row.artist_credit, "Artist A / Artist B")
        self.assertEqual(row.primary_artist_id, "artist-a-id")
        self.assertIs(row.is_appearance, True)

    def test_album_type_maps_to_the_single_structural_type(self) -> None:
        rg = _MBReleaseGroupRef(id="rg-album", primary_type="Album")
        row = _normalize_artist_release_group(rg, is_appearance=False)
        self.assertEqual(row.type, "Album")
        self.assertEqual(row.primary_types, ["Album"])


class TestGetReleaseTolerantOfLiveMirrorNulls(unittest.TestCase):
    """Issue #1355 item 5 review finding F1 — a live-mirror census found
    2.3%/1.1% of real full-release lookups carry an explicit JSON
    ``null`` ``country``/``status``. ``_MBReleaseFullStruct`` used to
    declare both plain ``str``, so ``get_release`` raised
    ``msgspec.ValidationError`` (a 500) on every one of those releases —
    a live regression this pin reproduces and then proves fixed."""

    def test_null_country_and_status_pass_through_like_before(self) -> None:
        payload = {
            "id": "rel-null-cs", "title": "Bootleg Pressing", "date": "1999",
            "country": None, "status": "Bootleg",
            "media": [],
        }
        with _mock_urlopen(payload):
            result = get_release("rel-null-cs", fresh=True)
        self.assertIsNone(result["country"])
        self.assertEqual(result["status"], "Bootleg")

    def test_get_artist_releases_with_recordings_omits_null_and_absent_nested_fields(
        self,
    ) -> None:
        """The plain-dict contract lib.artist_releases consumes must
        never carry an explicit null where a genuinely-absent key was
        expected (review finding F2) — a null/absent release-group,
        country, status, or track recording must be OMITTED, not
        present-as-None, or lib.artist_releases's .get(key, {}) calls
        return None instead of {} and crash on the next .get()."""
        artist_id = "artist-null-fields"
        canonical = {
            "release-count": 2,
            "releases": [{"id": "rel-null"}, {"id": "rel-populated"}],
        }
        detailed = {
            "release-count": 2,
            "releases": [
                {
                    "id": "rel-null", "title": "No Release Group", "date": "2020",
                    "country": None, "status": None,
                    "media": [{
                        "position": 1,
                        "tracks": [{"position": 1, "title": "Track One"}],
                    }],
                },
                {
                    "id": "rel-populated", "title": "Has Everything", "date": "2021",
                    "country": "GB", "status": "Official",
                    "release-group": {"id": "rg-1"},
                    "media": [{
                        "position": 1,
                        "tracks": [{
                            "position": 1, "title": "Track Two",
                            "recording": {"id": "rec-1"},
                        }],
                    }],
                },
            ],
        }
        with _mock_urlopen_by_fragment({
            f"/release?artist={artist_id}&fmt=json": canonical,
            f"/release?artist={artist_id}&inc=recordings": detailed,
        }):
            releases = get_artist_releases_with_recordings(artist_id)
        self.assertEqual(len(releases), 2)
        null_release, populated_release = releases

        # Null/absent fields are OMITTED, not present-as-None.
        self.assertNotIn("country", null_release)
        self.assertNotIn("status", null_release)
        self.assertNotIn("release-group", null_release)
        null_media = null_release.get("media", [])
        null_track = null_media[0].get("tracks", [])[0]
        self.assertNotIn("recording", null_track)

        # Must-still-work: a genuinely populated sibling survives with
        # its real values, not swept up by the same deletion logic.
        self.assertEqual(populated_release.get("country"), "GB")
        self.assertEqual(populated_release.get("status"), "Official")
        rg = populated_release.get("release-group")
        assert isinstance(rg, dict)
        self.assertEqual(rg.get("id"), "rg-1")
        populated_media = populated_release.get("media", [])
        populated_track = populated_media[0].get("tracks", [])[0]
        recording = populated_track.get("recording")
        assert isinstance(recording, dict)
        self.assertEqual(recording.get("id"), "rec-1")


class TestGetReleaseNormalizesFullPayload(unittest.TestCase):
    """``get_release``/``_strip_release`` field-by-field, against a real
    full-release shape (``inc=recordings+artist-credits+media+release-
    groups+labels``) — mirrors ``tests/test_discogs_api.py::TestGetRelease
    .test_normalizes_release``, MB's twin had no equivalent before issue
    #1355 item 5 rewrote ``_strip_release`` onto ``_MBReleaseFullStruct``."""

    RELEASE_DATA: ClassVar = {
        "id": "rel-full",
        "title": "The Bends",
        "date": "1995-03-13",
        "country": "GB",
        "status": "Official",
        "artist-credit": [{
            "name": "Radiohead",
            "artist": {"id": "a74b1b7f-71a5-4011-9441-d0b5e4122711", "name": "Radiohead"},
        }],
        "release-group": {"id": "rg-the-bends"},
        "media": [{
            "position": 1,
            "format": "CD",
            "tracks": [
                {
                    "position": 1, "number": "1", "title": "Planet Telex",
                    "length": 264000,
                    "recording": {"id": "rec-1", "length": 264000},
                },
                {
                    "position": 2, "number": "2", "title": "The Bends",
                    "length": 240000,
                    "recording": {"id": "rec-2"},
                },
            ],
        }],
    }

    def test_normalizes_release(self) -> None:
        with _mock_urlopen(self.RELEASE_DATA):
            result = get_release("rel-full", fresh=True)

        self.assertEqual(result["id"], "rel-full")
        self.assertEqual(result["title"], "The Bends")
        self.assertEqual(result["artist_name"], "Radiohead")
        self.assertEqual(result["artist_id"], "a74b1b7f-71a5-4011-9441-d0b5e4122711")
        self.assertEqual(result["release_group_id"], "rg-the-bends")
        self.assertEqual(result["date"], "1995-03-13")
        self.assertEqual(result["year"], 1995)
        self.assertEqual(result["country"], "GB")
        self.assertEqual(result["status"], "Official")
        tracks = result["tracks"]
        assert isinstance(tracks, list)
        self.assertEqual(len(tracks), 2)
        self.assertEqual(tracks[0], {
            "disc_number": 1, "track_number": 1, "title": "Planet Telex",
            "length_seconds": 264.0,
        })
        # Second track's recording carries no explicit length — falls back
        # to the track-level length, not the recording's (absent) one.
        self.assertEqual(tracks[1], {
            "disc_number": 1, "track_number": 2, "title": "The Bends",
            "length_seconds": 240.0,
        })

    def test_missing_artist_credit_and_release_group_normalize_to_unknown(
        self,
    ) -> None:
        bare = {
            "id": "rel-bare", "title": "Bare Release",
            "date": "2001", "country": "", "status": "",
            "media": [],
        }
        with _mock_urlopen(bare):
            result = get_release("rel-bare", fresh=True)
        self.assertEqual(result["artist_name"], "Unknown")
        self.assertIsNone(result["artist_id"])
        self.assertIsNone(result["release_group_id"])
        self.assertEqual(result["tracks"], [])

    def test_pregap_becomes_track_zero(self) -> None:
        with_pregap = {
            "id": "rel-pregap", "title": "Hidden Intro", "date": "2010",
            "country": "US", "status": "Official",
            "media": [{
                "position": 1, "format": "CD",
                "pregap": {"title": "Untitled", "length": 5000},
                "tracks": [
                    {"position": 1, "number": "1", "title": "Real Track", "length": 180000},
                ],
            }],
        }
        with _mock_urlopen(with_pregap):
            result = get_release("rel-pregap", fresh=True)
        tracks = result["tracks"]
        assert isinstance(tracks, list)
        self.assertEqual(tracks[0], {
            "disc_number": 1, "track_number": 0, "title": "Untitled",
            "length_seconds": 5.0,
        })
        self.assertEqual(tracks[1]["track_number"], 1)

    def test_vinyl_track_number_falls_back_to_the_printed_label(self) -> None:
        """A track missing ``position`` falls back to parsing ``number``
        (MB's printed label, e.g. ``"A1"`` for vinyl) as an int when it
        can, else keeps the literal label string rather than losing it."""
        no_position = {
            "id": "rel-vinyl", "title": "Vinyl Only", "date": "1999",
            "country": "US", "status": "Official",
            "media": [{
                "position": 1, "format": "Vinyl",
                "tracks": [
                    {"number": "7", "title": "Numeric Fallback"},
                    {"number": "A1", "title": "Side Label"},
                ],
            }],
        }
        with _mock_urlopen(no_position):
            result = get_release("rel-vinyl", fresh=True)
        tracks = result["tracks"]
        assert isinstance(tracks, list)
        self.assertEqual(tracks[0]["track_number"], 7)
        self.assertEqual(tracks[1]["track_number"], "A1")


class TestGetReleaseGroupNormalizesFullPayload(unittest.TestCase):
    """``get_release_group`` — no other test in this suite exercises its
    real field-population behaviour end to end."""

    def test_normalizes_full_payload(self) -> None:
        payload = {
            "id": "rg-1",
            "title": "In Rainbows",
            "primary-type": "Album",
            "first-release-date": "2007-10-10",
            "artist-credit": [{
                "name": "Radiohead",
                "artist": {"id": "a74b1b7f-71a5-4011-9441-d0b5e4122711", "name": "Radiohead"},
            }],
        }
        with _mock_urlopen(payload):
            result = get_release_group("rg-1")
        self.assertEqual(result, {
            "id": "rg-1",
            "title": "In Rainbows",
            "type": "Album",
            "first_release_date": "2007-10-10",
            "artist_id": "a74b1b7f-71a5-4011-9441-d0b5e4122711",
            "artist_name": "Radiohead",
        })

    def test_missing_artist_credit_normalizes_to_empty_ref(self) -> None:
        payload = {"id": "rg-2", "title": "No Credit", "primary-type": "Album"}
        with _mock_urlopen(payload):
            result = get_release_group("rg-2")
        self.assertEqual(result["artist_id"], "")
        self.assertEqual(result["artist_name"], "")


class TestGetReleaseGroupReleasesNormalizesFullPayload(unittest.TestCase):
    """``get_release_group_releases`` — no other test in this suite
    exercises its real field-population behaviour end to end."""

    def test_normalizes_full_payload(self) -> None:
        with _mock_urlopen_by_fragment({
            "/release-group/rg-releases?fmt=json": {
                "id": "rg-releases", "title": "OK Computer", "primary-type": "Album",
            },
            "/release?release-group=": {
                "release-count": 1,
                "releases": [{
                    "id": "rel-1", "title": "OK Computer", "date": "1997-06-16",
                    "country": "GB", "status": "Official",
                    "media": [
                        {"format": "CD", "track-count": 12},
                    ],
                }],
            },
        }):
            result = get_release_group_releases("rg-releases")
        self.assertEqual(result["title"], "OK Computer")
        self.assertEqual(result["type"], "Album")
        releases = result["releases"]
        assert isinstance(releases, list)
        self.assertEqual(len(releases), 1)
        self.assertEqual(releases[0], {
            "id": "rel-1", "title": "OK Computer", "date": "1997-06-16",
            "country": "GB", "status": "Official",
            "track_count": 12, "format": "CD", "media_count": 1,
        })

    def test_null_country_and_status_normalize_to_empty_string(self) -> None:
        with _mock_urlopen_by_fragment({
            "/release-group/rg-null?fmt=json": {"id": "rg-null", "title": "T"},
            "/release?release-group=": {
                "release-count": 1,
                "releases": [{
                    "id": "rel-1", "title": "T", "date": "", "country": None,
                    "status": None, "media": [],
                }],
            },
        }):
            result = get_release_group_releases("rg-null")
        releases = result["releases"]
        assert isinstance(releases, list)
        row = releases[0]
        assert isinstance(row, dict)
        self.assertEqual(row["country"], "")
        self.assertEqual(row["status"], "")


class TestWireBoundaryValidation(unittest.TestCase):
    """Issue #1355 item 5 — every general-purpose MB endpoint now decodes
    through a strict ``msgspec.Struct``. One RED test per newly-decoded
    endpoint family: feed a real field the wrong wire type and assert
    ``msgspec.ValidationError`` fires at the boundary rather than a
    ``.get()`` silently tolerating it."""

    def test_search_release_groups_rejects_non_int_score(self) -> None:
        bad = {
            "releases": [{
                "id": "rel-bad-score",
                "title": "Bad Score",
                "date": "2024",
                "score": "not-an-int",
                "release-group": {"id": "rg-bad-score"},
            }],
        }
        with _mock_urlopen(bad), self.assertRaises(msgspec.ValidationError):
            search_release_groups("bad score query")

    def test_search_artists_rejects_non_list_artists(self) -> None:
        bad = {"artists": "not-a-list"}
        with _mock_urlopen(bad), self.assertRaises(msgspec.ValidationError):
            search_artists("bad artists query")

    def test_get_artist_release_groups_rejects_non_int_count(self) -> None:
        bad_rg_browse = {"release-group-count": "not-an-int", "release-groups": []}
        with _mock_urlopen_by_fragment({
            "/release-group?artist=": bad_rg_browse,
            "/release?artist=": {"release-count": 0, "releases": []},
            "/release?track_artist=": {"release-count": 0, "releases": []},
        }), self.assertRaises(msgspec.ValidationError):
            get_artist_release_groups("artist-bad-rg-count")

    def test_get_release_group_rejects_non_str_id(self) -> None:
        bad = {"id": 12345, "title": "Bad Id"}
        with _mock_urlopen(bad), self.assertRaises(msgspec.ValidationError):
            get_release_group("rg-bad-id")

    def test_get_release_group_year_rejects_non_str_date(self) -> None:
        bad = {"id": "rg-bad-date", "first-release-date": 2024}
        with _mock_urlopen(bad), self.assertRaises(msgspec.ValidationError):
            get_release_group_year("rg-bad-date")

    def test_get_release_group_releases_rejects_non_int_track_count(self) -> None:
        meta = {"id": "rg-bad-tc", "title": "T", "primary-type": "Album"}
        bad_releases = {
            "release-count": 1,
            "releases": [{
                "id": "rel-1", "title": "T", "date": "2024",
                "country": "XW", "status": "Official",
                "media": [{"format": "CD", "track-count": "twelve"}],
            }],
        }
        with _mock_urlopen_by_fragment({
            "/release-group/rg-bad-tc?fmt=json": meta,
            "/release?release-group=": bad_releases,
        }), self.assertRaises(msgspec.ValidationError):
            get_release_group_releases("rg-bad-tc")

    def test_get_release_rejects_non_str_title(self) -> None:
        bad = {"id": "rel-bad-title", "title": 12345, "date": "2024"}
        with _mock_urlopen(bad), self.assertRaises(msgspec.ValidationError):
            get_release("rel-bad-title", fresh=True)

    def test_get_release_rejects_non_str_track_number(self) -> None:
        """``number`` is MB's printed vinyl-side label ("A1"/"B2") — a
        string at the wire. An int there (the historical TypedDict's
        false claim) must be rejected, not silently accepted."""
        bad = {
            "id": "rel-bad-number", "title": "T", "date": "2024",
            "media": [{"position": 1, "tracks": [{"number": 7, "title": "X"}]}],
        }
        with _mock_urlopen(bad), self.assertRaises(msgspec.ValidationError):
            get_release("rel-bad-number", fresh=True)

    def test_get_artist_name_rejects_non_str_name(self) -> None:
        bad = {"id": "artist-bad-name", "name": 12345}
        with _mock_urlopen(bad), self.assertRaises(msgspec.ValidationError):
            get_artist_name("artist-bad-name")

    def test_get_artist_releases_with_recordings_rejects_non_str_release_id(
        self,
    ) -> None:
        canonical = {
            "release-count": 1,
            "releases": [{"id": "rel-1"}],
        }
        bad_detail = {
            "release-count": 1,
            "releases": [{"id": 999}],
        }
        with _mock_urlopen_by_fragment({
            "/release?artist=artist-bad-recordings&fmt=json": canonical,
            "/release?artist=artist-bad-recordings&inc=recordings": bad_detail,
        }), self.assertRaises(msgspec.ValidationError):
            get_artist_releases_with_recordings("artist-bad-recordings")


if __name__ == "__main__":
    unittest.main()
