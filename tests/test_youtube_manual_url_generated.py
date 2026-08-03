"""Generated patrol for manual YouTube playlist URL selection (#1016).

Invariant: when an admitted YouTube URL carries both ``v`` and ``list``, the
resolver must fetch and persist the complete playlist.  It must never silently
fall back to the single video's album.  The deterministic pin is the real
Loon Lake URL in ``tests.test_youtube_album_service``; this property drives the
outer service boundary over generated valid video/playlist IDs and URL hosts.
"""

from __future__ import annotations

import string
import unittest

from hypothesis import given
from hypothesis import strategies as st

import tests._hypothesis_profiles  # noqa: F401
from lib.beets_distance import BeetsDistanceResult
from lib.youtube_album_service import (
    ResolvedDistance,
    ResolvedYoutubeRelease,
    YoutubeAlbumResolverResult,
    resolve_youtube_album,
)
from tests.fakes import FakeDiscogsLookup, FakePipelineDB, FakeYTMusic, http_error

_RELEASE_ID = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
_GROUP_ID = "11111111-1111-1111-1111-111111111111"
_ID = st.text(alphabet=string.ascii_letters + string.digits + "_-",
              min_size=1, max_size=24)


def _distance(*, mbid: str, **_kwargs: object) -> BeetsDistanceResult:
    return BeetsDistanceResult(
        outcome="ok", distance=0.0, matched_tracks=1,
        total_local_tracks=1, total_mb_tracks=1,
        extra_local_tracks=0, extra_mb_tracks=0, components={},
        candidate_mbid=mbid, candidate_release_group_id=_GROUP_ID,
        request_release_group_id=_GROUP_ID,
    )


def _discogs_404(_identifier: str) -> dict[str, object]:
    raise http_error(404, url="http://discogs.test/missing")


def _resolve(url: str, playlist_id: str) -> tuple[
    YoutubeAlbumResolverResult, FakeYTMusic, FakePipelineDB,
]:
    release: dict[str, object] = {
        "id": _RELEASE_ID,
        "release_group_id": _GROUP_ID,
        "artist_name": "Archivist Artist",
        "title": "Vanishing Album",
        "year": 2026,
        "tracks": [{
            "disc_number": 1, "track_number": 1,
            "title": "Only Track", "length_seconds": 120.0,
        }],
    }
    yt = FakeYTMusic()
    yt.set_playlist(playlist_id, {
        "title": "channel upload title",
        "tracks": [{
            "videoId": "playlist-track",
            "title": "Archivist Artist - Only Track (Official Video)",
            "duration_seconds": 120,
        }],
    })
    pdb = FakePipelineDB()
    result = resolve_youtube_album(
        _RELEASE_ID, pdb=pdb,
        mb_get_release=lambda _identifier: release,
        mb_get_release_group_releases=lambda _identifier: {
            "releases": [{"id": _RELEASE_ID}],
        },
        discogs_get_release=FakeDiscogsLookup(),
        discogs_get_master_releases=_discogs_404,
        yt_client=yt, distance_fn=_distance,
        sleep_fn=lambda _seconds: None, watch_url=url,
        deadline_seconds=-1,
    )
    return result, yt, pdb


def assert_playlist_wins(
    result: YoutubeAlbumResolverResult,
    yt: FakeYTMusic,
    pdb: FakePipelineDB,
    playlist_id: str,
) -> None:
    """Check the observable service result and external-call selection."""
    if result.outcome != "ok":
        raise AssertionError(f"resolver outcome was {result.outcome!r}")
    if yt.get_playlist_calls != [{"playlistId": playlist_id, "limit": None}]:
        raise AssertionError(f"playlist calls were {yt.get_playlist_calls!r}")
    if yt.get_watch_playlist_calls:
        raise AssertionError(f"watch fallback ran: {yt.get_watch_playlist_calls!r}")
    if len(result.youtube_releases) != 1:
        raise AssertionError("manual playlist did not produce one matrix row")
    release = result.youtube_releases[0]
    if release.yt_browse_id != playlist_id:
        raise AssertionError(f"selected {release.yt_browse_id!r}, not playlist")
    if release.yt_audio_playlist_id != playlist_id:
        raise AssertionError("playlist identity was not retained for ingest")
    if [track.title for track in release.tracks] != ["Only Track"]:
        raise AssertionError("presentation-only title adornments were not removed")
    stored = pdb.get_youtube_album_mapping(_GROUP_ID, "mb")
    if stored is None or len(stored) != 1:
        raise AssertionError("manual playlist did not persist one matrix row")
    if stored[0].get("yt_browse_id") != playlist_id:
        raise AssertionError("persisted row lost the playlist identity")


class TestPlaylistSelectionChecker(unittest.TestCase):
    """Known-bad self-test proving the generated invariant can fail."""

    def test_rejects_single_video_fallback_mutant(self) -> None:
        yt = FakeYTMusic()
        yt.get_watch_playlist_calls.append({"videoId": "video-only"})
        wrong = YoutubeAlbumResolverResult(
            outcome="ok",
            youtube_releases=[ResolvedYoutubeRelease(
                yt_browse_id="video-only",
                yt_url="https://music.youtube.com/watch?v=video-only",
                track_count=1,
                tracks=[],
                distances=[ResolvedDistance(mbid=_RELEASE_ID, outcome="ok")],
            )],
        )
        with self.assertRaises(AssertionError):
            assert_playlist_wins(
                wrong, yt, FakePipelineDB(), "playlist-must-win")


class TestManualPlaylistSelectionGenerated(unittest.TestCase):

    @given(
        host=st.sampled_from([
            "youtube.com", "www.youtube.com", "music.youtube.com",
        ]),
        video_id=_ID,
        playlist_id=_ID,
        list_first=st.booleans(),
    )
    def test_list_parameter_wins_over_video(
        self, host: str, video_id: str, playlist_id: str, list_first: bool,
    ) -> None:
        query = (
            f"list={playlist_id}&v={video_id}"
            if list_first else f"v={video_id}&list={playlist_id}"
        )
        result, yt, pdb = _resolve(
            f"https://{host}/watch?{query}", playlist_id)
        assert_playlist_wins(result, yt, pdb, playlist_id)
