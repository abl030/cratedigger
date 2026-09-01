"""Self-tests for ``tests/fakes/ytmusic.py``'s FakeYTMusic.

Split out of ``tests/test_fakes.py`` (#1313) so each cluster of the
fake has its sibling tests beside it.
"""
import unittest

from tests.fakes import (
    FakeYTMusic,
)


class TestFakeYTMusic(unittest.TestCase):
    """Self-test for the FakeYTMusic stub (U5).

    FakeYTMusic mirrors the slice of ``ytmusicapi.YTMusic`` the YouTube album
    resolver service uses: ``search`` + ``get_album``. It supports per-query
    canned results, one-shot failure injection (mirroring FakeSlskdAPI), and
    call recording so service tests can assert N+1 fan-out shape.
    """

    def test_search_returns_canned_results_for_matching_query(self):
        yt = FakeYTMusic()
        canned = [{"browseId": "MPREb_abc", "title": "Test Album",
                   "artists": [{"name": "Artist"}], "year": "2020"}]
        yt.set_search("artist title", canned)

        result = yt.search("artist title", filter="albums", limit=20)

        self.assertEqual(result, canned)

    def test_search_returns_empty_list_for_unconfigured_query(self):
        yt = FakeYTMusic()

        result = yt.search("never configured", filter="albums")

        self.assertEqual(result, [])

    def test_get_album_returns_canned_response_for_matching_browse_id(self):
        yt = FakeYTMusic()
        canned = {"title": "Test Album", "audioPlaylistId": "OLAK5uy_xxx",
                  "tracks": []}
        yt.set_album("MPREb_abc", canned)

        result = yt.get_album("MPREb_abc")

        self.assertEqual(result, canned)

    def test_get_album_raises_server_error_for_unconfigured_browse_id(self):
        """Mirrors real ytmusicapi behavior: non-existent albums raise."""
        from ytmusicapi.exceptions import YTMusicServerError
        yt = FakeYTMusic()

        with self.assertRaises(YTMusicServerError):
            yt.get_album("MPREb_does_not_exist")

    def test_search_failure_injection_is_one_shot_server_error(self):
        from ytmusicapi.exceptions import YTMusicServerError
        yt = FakeYTMusic()
        yt.set_search("flaky", [{"browseId": "MPREb_z"}])
        yt.set_search_error("flaky", YTMusicServerError("upstream 503"))

        with self.assertRaises(YTMusicServerError):
            yt.search("flaky", filter="albums")
        # Second call: queued exception is gone, canned result is returned.
        self.assertEqual(
            yt.search("flaky", filter="albums"),
            [{"browseId": "MPREb_z"}],
        )

    def test_search_failure_injection_is_one_shot_user_error(self):
        from ytmusicapi.exceptions import YTMusicUserError
        yt = FakeYTMusic()
        yt.set_search_error("bad", YTMusicUserError("malformed query"))

        with self.assertRaises(YTMusicUserError):
            yt.search("bad", filter="albums")
        # Second call falls back to the empty default.
        self.assertEqual(yt.search("bad", filter="albums"), [])

    def test_search_failure_injection_is_one_shot_timeout(self):
        import requests
        yt = FakeYTMusic()
        yt.set_search_error("slow", requests.Timeout("read timed out"))

        with self.assertRaises(requests.Timeout):
            yt.search("slow", filter="albums")
        self.assertEqual(yt.search("slow", filter="albums"), [])

    def test_search_failure_injection_is_one_shot_connection_error(self):
        import requests
        yt = FakeYTMusic()
        yt.set_search_error("dropped", requests.ConnectionError("ECONNRESET"))

        with self.assertRaises(requests.ConnectionError):
            yt.search("dropped", filter="albums")
        self.assertEqual(yt.search("dropped", filter="albums"), [])

    def test_search_failure_injection_is_one_shot_key_error(self):
        """KeyError simulates ytmusicapi parser drift."""
        yt = FakeYTMusic()
        yt.set_search_error("parse_fail", KeyError("tabs"))

        with self.assertRaises(KeyError):
            yt.search("parse_fail", filter="albums")
        self.assertEqual(yt.search("parse_fail", filter="albums"), [])

    def test_get_album_failure_injection_is_one_shot_server_error(self):
        from ytmusicapi.exceptions import YTMusicServerError
        yt = FakeYTMusic()
        yt.set_album("MPREb_x", {"title": "X", "tracks": []})
        yt.set_album_error("MPREb_x", YTMusicServerError("upstream 503"))

        with self.assertRaises(YTMusicServerError):
            yt.get_album("MPREb_x")
        # Second call: canned response returns.
        self.assertEqual(yt.get_album("MPREb_x"), {"title": "X", "tracks": []})

    def test_get_album_failure_injection_is_one_shot_user_error(self):
        from ytmusicapi.exceptions import YTMusicUserError
        yt = FakeYTMusic()
        yt.set_album("MPREb_y", {"title": "Y", "tracks": []})
        yt.set_album_error("MPREb_y", YTMusicUserError("bad request"))

        with self.assertRaises(YTMusicUserError):
            yt.get_album("MPREb_y")
        self.assertEqual(yt.get_album("MPREb_y"), {"title": "Y", "tracks": []})

    def test_get_album_failure_injection_is_one_shot_timeout(self):
        import requests
        yt = FakeYTMusic()
        yt.set_album("MPREb_z", {"title": "Z", "tracks": []})
        yt.set_album_error("MPREb_z", requests.Timeout("slow"))

        with self.assertRaises(requests.Timeout):
            yt.get_album("MPREb_z")
        self.assertEqual(yt.get_album("MPREb_z"), {"title": "Z", "tracks": []})

    def test_get_album_failure_injection_is_one_shot_connection_error(self):
        import requests
        yt = FakeYTMusic()
        yt.set_album("MPREb_q", {"title": "Q", "tracks": []})
        yt.set_album_error("MPREb_q", requests.ConnectionError("ECONNRESET"))

        with self.assertRaises(requests.ConnectionError):
            yt.get_album("MPREb_q")
        self.assertEqual(yt.get_album("MPREb_q"), {"title": "Q", "tracks": []})

    def test_get_album_failure_injection_is_one_shot_key_error(self):
        yt = FakeYTMusic()
        yt.set_album("MPREb_p", {"title": "P", "tracks": []})
        yt.set_album_error("MPREb_p", KeyError("tracks"))

        with self.assertRaises(KeyError):
            yt.get_album("MPREb_p")
        self.assertEqual(yt.get_album("MPREb_p"), {"title": "P", "tracks": []})

    def test_search_records_call_arguments(self):
        yt = FakeYTMusic()

        yt.search("first query", filter="albums", limit=20)
        yt.search("second", filter=None, limit=5)

        self.assertEqual(len(yt.search_calls), 2)
        self.assertEqual(yt.search_calls[0]["query"], "first query")
        self.assertEqual(yt.search_calls[0]["filter"], "albums")
        self.assertEqual(yt.search_calls[0]["limit"], 20)
        self.assertEqual(yt.search_calls[1]["query"], "second")
        self.assertEqual(yt.search_calls[1]["filter"], None)
        self.assertEqual(yt.search_calls[1]["limit"], 5)

    def test_get_album_records_call_arguments(self):
        yt = FakeYTMusic()
        yt.set_album("MPREb_a", {"title": "A", "tracks": []})
        yt.set_album("MPREb_b", {"title": "B", "tracks": []})

        yt.get_album("MPREb_a")
        yt.get_album("MPREb_b")

        self.assertEqual(len(yt.get_album_calls), 2)
        self.assertEqual(yt.get_album_calls[0]["browseId"], "MPREb_a")
        self.assertEqual(yt.get_album_calls[1]["browseId"], "MPREb_b")

    def test_call_recording_captures_failed_calls_too(self):
        """Calls are recorded even when they raise — like FakeSlskdAPI."""
        from ytmusicapi.exceptions import YTMusicServerError
        yt = FakeYTMusic()
        yt.set_search_error("boom", YTMusicServerError("nope"))

        with self.assertRaises(YTMusicServerError):
            yt.search("boom", filter="albums")

        self.assertEqual(yt.search_calls[0]["query"], "boom")

    def test_make_album_fixture_produces_expected_top_level_shape(self):
        fixture = FakeYTMusic.make_album_fixture(
            audio_playlist_id="OLAK5uy_xxx",
            title="Test Album",
            artists=[{"name": "Artist", "id": "UCxxx"}],
            year="2020",
            tracks=[],
        )

        expected_top_keys = {
            "title", "type", "thumbnails", "description", "artists",
            "year", "trackCount", "duration", "duration_seconds",
            "audioPlaylistId", "tracks", "other_versions",
        }
        self.assertEqual(set(fixture.keys()), expected_top_keys)
        self.assertEqual(fixture["title"], "Test Album")
        self.assertEqual(fixture["audioPlaylistId"], "OLAK5uy_xxx")
        self.assertEqual(fixture["year"], "2020")
        self.assertEqual(fixture["trackCount"], 0)
        self.assertEqual(fixture["tracks"], [])
        self.assertEqual(fixture["other_versions"], [])

    def test_make_album_fixture_track_shape(self):
        track = {
            "videoId": "vid_1", "title": "Track 1",
            "artists": [{"name": "Artist", "id": "UCxxx"}],
            "album": {"name": "Test Album", "id": "MPREb_abc"},
            "duration": "3:14",
            "duration_seconds": 194,
            "trackNumber": 1,
            "isAvailable": True,
            "isExplicit": False,
            "likeStatus": "INDIFFERENT",
            "thumbnails": [],
            "feedbackTokens": {"add": None, "remove": None},
            "creditsBrowseId": None,
        }
        fixture = FakeYTMusic.make_album_fixture(
            audio_playlist_id="OLAK5uy_xxx",
            title="Test Album",
            artists=[{"name": "Artist", "id": "UCxxx"}],
            year="2020",
            tracks=[track],
        )

        expected_track_keys = {
            "videoId", "title", "artists", "album", "duration",
            "duration_seconds", "trackNumber", "isAvailable", "isExplicit",
            "likeStatus", "thumbnails", "feedbackTokens", "creditsBrowseId",
        }
        self.assertEqual(fixture["trackCount"], 1)
        self.assertEqual(set(fixture["tracks"][0].keys()), expected_track_keys)

    def test_make_album_fixture_other_versions_shape(self):
        other = {
            "browseId": "MPREb_other",
            "title": "Test Album (Deluxe)",
            "artists": [{"name": "Artist", "id": "UCxxx"}],
            "year": "2021",
            "thumbnails": [],
            "isExplicit": False,
        }
        fixture = FakeYTMusic.make_album_fixture(
            audio_playlist_id="OLAK5uy_xxx",
            title="Test Album",
            artists=[{"name": "Artist", "id": "UCxxx"}],
            year="2020",
            tracks=[],
            other_versions=[other],
        )

        expected_other_keys = {
            "browseId", "title", "artists", "year", "thumbnails", "isExplicit",
        }
        self.assertEqual(len(fixture["other_versions"]), 1)
        self.assertEqual(
            set(fixture["other_versions"][0].keys()), expected_other_keys,
        )

    def test_make_album_fixture_round_trips_through_set_album(self):
        """The fixture shape is what set_album / get_album exchange."""
        yt = FakeYTMusic()
        fixture = FakeYTMusic.make_album_fixture(
            audio_playlist_id="OLAK5uy_xxx",
            title="Test Album",
            artists=[{"name": "Artist", "id": "UCxxx"}],
            year="2020",
            tracks=[],
        )

        yt.set_album("MPREb_abc", fixture)

        self.assertEqual(yt.get_album("MPREb_abc"), fixture)


if __name__ == "__main__":
    unittest.main()
