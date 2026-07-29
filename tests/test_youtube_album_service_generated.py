"""Generated proofs for YouTube resolver deadlines and durable truth."""

from __future__ import annotations

import unittest
from dataclasses import dataclass
from typing import Never

import requests
from hypothesis import example, given
from hypothesis import strategies as st
from ytmusicapi.exceptions import YTMusicError, YTMusicServerError, YTMusicUserError

import tests._hypothesis_profiles  # noqa: F401
from lib.beets_distance import BeetsDistanceResult
from lib.pipeline_db import PersistedYoutubeRow
from lib.youtube_album_service import (
    YoutubeAlbumResolverResult,
    _cached_search,
    _fetch_mb_siblings,
    resolve_youtube_album,
)
from tests.fakes import FakePipelineDB, FakeYTMusic

_DEADLINE_SECONDS = 60.0
_MB_RG = "11111111-1111-1111-1111-111111111111"
_MB_RELEASE = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
_MB_RELEASE_B = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
_YT_QUERY = "Generated Artist Generated Album"
_YT_BROWSE_ID = "MPREb-generated"
_YT_BROKEN_BROWSE_ID = "MPREb-generated-broken"
_YT_CACHED_BROWSE_ID = "MPREb-generated-cached"


@dataclass(frozen=True)
class SiblingFetchObservation:
    """Observable boundary facts from one generated sibling-fetch world."""

    durations: tuple[int, ...]
    started_ids: tuple[str, ...]
    returned_ids: tuple[str, ...]
    complete: bool


def assert_sibling_fetch_deadline(observation: SiblingFetchObservation) -> None:
    """No fetch starts after breach and completeness is explicit."""
    elapsed = 0
    expected_started = 0
    expected_complete = True
    for duration in observation.durations:
        if elapsed >= _DEADLINE_SECONDS:
            expected_complete = False
            break
        expected_started += 1
        elapsed += duration
        if elapsed >= _DEADLINE_SECONDS:
            expected_complete = False
            break

    expected_ids = tuple(
        f"sibling-{index}" for index in range(expected_started)
    )
    if observation.started_ids != expected_ids:
        raise AssertionError(
            "a sibling fetch started after the first observed deadline "
            f"breach: expected={expected_ids!r}, "
            f"actual={observation.started_ids!r}"
        )
    if observation.returned_ids != expected_ids:
        raise AssertionError(
            "successful sibling results drifted from the attempted prefix: "
            f"expected={expected_ids!r}, "
            f"actual={observation.returned_ids!r}"
        )
    if observation.complete is not expected_complete:
        raise AssertionError(
            "sibling-fetch completeness drifted: "
            f"expected={expected_complete!r}, "
            f"actual={observation.complete!r}"
        )


@dataclass(frozen=True)
class CacheDeadlineWorld:
    """Durations that cross the deadline at one cached-search boundary."""

    breach_after: str
    redis_get_seconds: int
    yt_search_seconds: int


@dataclass(frozen=True)
class CacheDeadlineObservation:
    """Collaborators launched around the first observed deadline breach."""

    breach_after: str
    yt_search_calls: int
    redis_set_calls: int
    final_check_stage: str | None


def assert_cached_search_deadline(
    observation: CacheDeadlineObservation,
) -> None:
    """A cache-read breach blocks YT; a YT breach blocks the cache write."""
    expected_search_calls = 0 if observation.breach_after == "redis_get" else 1
    expected_stage = (
        "after the YT search cache read"
        if observation.breach_after == "redis_get"
        else "after YT search"
    )
    if observation.yt_search_calls != expected_search_calls:
        raise AssertionError(
            "YT search launch drifted after the observed boundary: "
            f"expected={expected_search_calls}, "
            f"actual={observation.yt_search_calls}"
        )
    if observation.redis_set_calls != 0:
        raise AssertionError(
            "Redis cache write started after the observed deadline breach"
        )
    if observation.final_check_stage != expected_stage:
        raise AssertionError(
            "deadline was observed at the wrong collaborator boundary: "
            f"expected={expected_stage!r}, "
            f"actual={observation.final_check_stage!r}"
        )


@st.composite
def cache_deadline_worlds(draw: st.DrawFn) -> CacheDeadlineWorld:
    """Generate only useful worlds that breach after Redis get or YT search."""
    breach_after = draw(st.sampled_from(("redis_get", "yt_search")))
    if breach_after == "redis_get":
        redis_get_seconds = draw(st.integers(min_value=60, max_value=90))
        yt_search_seconds = draw(st.integers(min_value=0, max_value=90))
    else:
        redis_get_seconds = draw(st.integers(min_value=0, max_value=59))
        yt_search_seconds = draw(
            st.integers(
                min_value=int(_DEADLINE_SECONDS) - redis_get_seconds,
                max_value=90,
            )
        )
    return CacheDeadlineWorld(
        breach_after=breach_after,
        redis_get_seconds=redis_get_seconds,
        yt_search_seconds=yt_search_seconds,
    )


class _ObservedDeadline(Exception):
    """Generated-test control flow raised at the first breached check."""


class _GeneratedClock:
    def __init__(self) -> None:
        self.now = 0.0

    def advance(self, seconds: float) -> None:
        self.now += seconds


class _TimedCache:
    def __init__(self, clock: _GeneratedClock, get_seconds: int) -> None:
        self._clock = clock
        self._get_seconds = get_seconds
        self.get_calls = 0
        self.set_calls = 0

    def get(self, key: str) -> bytes | None:
        del key
        self.get_calls += 1
        self._clock.advance(self._get_seconds)
        return None

    def set(self, key: str, value: bytes, ttl_seconds: int) -> None:
        del key, value, ttl_seconds
        self.set_calls += 1


class _TimedYT:
    def __init__(self, clock: _GeneratedClock, search_seconds: int) -> None:
        self._clock = clock
        self._search_seconds = search_seconds
        self.search_calls = 0

    def search(
        self,
        _query: str,
        *,
        filter: str,
        limit: int,
    ) -> list[dict[str, object]]:
        del filter, limit
        self.search_calls += 1
        self._clock.advance(self._search_seconds)
        return []


class TestYoutubeAlbumCacheDeadlineGenerated(unittest.TestCase):
    @given(world=cache_deadline_worlds())
    @example(
        world=CacheDeadlineWorld(
            breach_after="redis_get",
            redis_get_seconds=60,
            yt_search_seconds=90,
        )
    )
    @example(
        world=CacheDeadlineWorld(
            breach_after="yt_search",
            redis_get_seconds=59,
            yt_search_seconds=1,
        )
    )
    def test_real_cached_search_stops_at_first_observed_breach(
        self,
        world: CacheDeadlineWorld,
    ) -> None:
        clock = _GeneratedClock()
        cache = _TimedCache(clock, world.redis_get_seconds)
        yt = _TimedYT(clock, world.yt_search_seconds)
        check_stages: list[str] = []

        def _deadline_check(stage: str) -> None:
            check_stages.append(stage)
            if clock.now >= _DEADLINE_SECONDS:
                raise _ObservedDeadline(stage)

        with self.assertRaises(_ObservedDeadline):
            _cached_search(
                yt,
                cache,
                _YT_QUERY,
                "albums",
                10,
                deadline_check=_deadline_check,
            )

        assert_cached_search_deadline(CacheDeadlineObservation(
            breach_after=world.breach_after,
            yt_search_calls=yt.search_calls,
            redis_set_calls=cache.set_calls,
            final_check_stage=check_stages[-1] if check_stages else None,
        ))


@dataclass(frozen=True)
class IncompleteMatrixObservation:
    """Service outcome and durable writes for incomplete upstream YT truth."""

    source_shape: str
    outcome: str
    durable_write_sizes: tuple[int, ...]
    durable_cache_present: bool


def assert_incomplete_matrix_not_persisted(
    observation: IncompleteMatrixObservation,
) -> None:
    """Malformed or unscoreable non-empty YT truth fails without ``[]``."""
    if observation.outcome != "youtube_parse_failed":
        raise AssertionError(
            f"{observation.source_shape} was not surfaced as a parse failure: "
            f"{observation.outcome!r}"
        )
    if observation.durable_write_sizes:
        raise AssertionError(
            f"{observation.source_shape} produced durable writes: "
            f"{observation.durable_write_sizes!r}"
        )
    if observation.durable_cache_present:
        raise AssertionError(
            f"{observation.source_shape} created false durable cache truth"
        )


class _MappingWriteDB(FakePipelineDB):
    def __init__(self) -> None:
        super().__init__()
        self.mapping_write_sizes: list[int] = []

    def upsert_youtube_album_mapping(
        self,
        release_group_identifier: str,
        source: str,
        rows: list[PersistedYoutubeRow],
    ) -> None:
        self.mapping_write_sizes.append(len(rows))
        super().upsert_youtube_album_mapping(
            release_group_identifier,
            source,
            rows,
        )


def _mb_release(identifier: str) -> dict[str, object] | None:
    if identifier == _MB_RG:
        return None
    return {
        "id": identifier,
        "title": "Generated Album",
        "artist_name": "Generated Artist",
        "release_group_id": _MB_RG,
        "year": 1996,
        "tracks": [
            {
                "disc_number": 1,
                "track_number": 1,
                "title": "Generated Track",
                "length_seconds": 180.0,
            }
        ],
    }


def _mb_group(identifier: str) -> dict[str, object] | None:
    if identifier != _MB_RG:
        return None
    return {
        "title": "Generated Album",
        "type": "Album",
        "releases": [{"id": _MB_RELEASE}],
    }


def _empty_lookup(_identifier: str) -> None:
    return None


def _unexpected_distance(**_kwargs: object) -> Never:
    raise AssertionError("incomplete YT truth must fail before scoring")


def _resolve_generated_yt(
    yt: FakeYTMusic,
    pdb: _MappingWriteDB,
) -> YoutubeAlbumResolverResult:
    return resolve_youtube_album(
        _MB_RG,
        pdb=pdb,
        mb_get_release=_mb_release,
        mb_get_release_group_releases=_mb_group,
        discogs_get_release=_empty_lookup,
        discogs_get_master_releases=_empty_lookup,
        yt_client=yt,
        distance_fn=_unexpected_distance,
        cache=None,
        sleep_fn=lambda _seconds: None,
    )


@dataclass(frozen=True)
class IncompleteCollectionWorld:
    """One generated incomplete-matrix cause and cache posture."""

    cause: str
    has_durable_cache: bool
    scoring_seconds: int = 0
    album_error_kind: str = ""


@dataclass(frozen=True)
class IncompleteCollectionObservation:
    """Selectable response and durable-truth facts after incomplete work."""

    cause: str
    had_durable_cache: bool
    outcome: str
    from_cache: bool
    returned_browse_ids: tuple[str, ...]
    durable_write_sizes: tuple[int, ...]
    durable_browse_ids: tuple[str, ...]
    expected_cached_browse_ids: tuple[str, ...]


def assert_incomplete_collection_preserves_durable_truth(
    observation: IncompleteCollectionObservation,
) -> None:
    """Incomplete work is a typed failure or an exact complete-cache fallback."""
    if observation.durable_write_sizes:
        raise AssertionError(
            f"{observation.cause} replaced durable truth with incomplete work: "
            f"{observation.durable_write_sizes!r}"
        )
    if observation.had_durable_cache:
        if observation.outcome != "ok" or not observation.from_cache:
            raise AssertionError(
                f"{observation.cause} did not use the complete cache fallback: "
                f"outcome={observation.outcome!r}, "
                f"from_cache={observation.from_cache!r}"
            )
        if (
            observation.returned_browse_ids
            != observation.expected_cached_browse_ids
        ):
            raise AssertionError(
                f"{observation.cause} exposed incomplete refresh rows instead "
                "of the complete cached matrix"
            )
        if (
            observation.durable_browse_ids
            != observation.expected_cached_browse_ids
        ):
            raise AssertionError(
                f"{observation.cause} changed the complete durable matrix"
            )
        return
    if observation.outcome == "ok" or observation.from_cache:
        raise AssertionError(
            f"{observation.cause} surfaced incomplete uncached work as success"
        )
    if observation.returned_browse_ids:
        raise AssertionError(
            f"{observation.cause} exposed unpersisted selectable releases: "
            f"{observation.returned_browse_ids!r}"
        )
    if observation.durable_browse_ids:
        raise AssertionError(
            f"{observation.cause} created false durable truth"
        )


@st.composite
def incomplete_collection_worlds(
    draw: st.DrawFn,
) -> IncompleteCollectionWorld:
    cause = draw(st.sampled_from(("scoring_deadline", "yt_sibling_error")))
    has_durable_cache = draw(st.booleans())
    if cause == "scoring_deadline":
        return IncompleteCollectionWorld(
            cause=cause,
            has_durable_cache=has_durable_cache,
            scoring_seconds=draw(st.integers(min_value=60, max_value=90)),
        )
    return IncompleteCollectionWorld(
        cause=cause,
        has_durable_cache=has_durable_cache,
        album_error_kind=draw(st.sampled_from((
            "server",
            "user",
            "ytmusic",
            "timeout",
            "connection",
            "key",
            "index",
        ))),
    )


def _generated_album_error(kind: str) -> Exception:
    if kind == "server":
        return YTMusicServerError("Server returned HTTP 500: generated")
    if kind == "user":
        return YTMusicUserError("generated client failure")
    if kind == "ytmusic":
        return YTMusicError("generated parser failure")
    if kind == "timeout":
        return requests.Timeout("generated timeout")
    if kind == "connection":
        return requests.ConnectionError("generated connection failure")
    if kind == "key":
        return KeyError("generated schema drift")
    if kind == "index":
        return IndexError("generated schema drift")
    raise AssertionError(f"unknown generated album error kind: {kind}")


def _seed_generated_complete_cache(pdb: _MappingWriteDB) -> None:
    pdb.seed_youtube_album_mapping(_MB_RG, "mb", [{
        "yt_browse_id": _YT_CACHED_BROWSE_ID,
        "yt_audio_playlist_id": "OLAK5uy-generated-cached",
        "yt_url": (
            "https://music.youtube.com/playlist"
            "?list=OLAK5uy-generated-cached"
        ),
        "yt_year": 1996,
        "yt_track_count": 1,
        "album_title": "Generated Album",
        "album_artist": "Generated Artist",
        "yt_tracks": [{
            "title": "Generated Track",
            "artists": [{"name": "Generated Artist"}],
            "length_seconds": 180.0,
            "track_number": 1,
            "disc_number": 1,
            "video_id": "generated-cached-video",
        }],
        "distances": [{
            "mbid": _MB_RELEASE,
            "outcome": "ok",
            "distance": 0.05,
            "components": {"tracks": 0.05},
            "matched_tracks": 1,
            "total_local_tracks": 1,
            "total_mb_tracks": 1,
            "extra_local_tracks": 0,
            "extra_mb_tracks": 0,
            "error_message": None,
        }],
    }])


def _resolve_incomplete_collection(
    world: IncompleteCollectionWorld,
    pdb: _MappingWriteDB,
) -> YoutubeAlbumResolverResult:
    clock = _GeneratedClock()
    yt = FakeYTMusic()
    yt.set_search(
        _YT_QUERY,
        [{
            "browseId": _YT_BROWSE_ID,
            "title": "Generated Album",
            "year": "1996",
            "trackCount": 1,
        }],
    )
    other_versions = (
        [{"browseId": _YT_BROKEN_BROWSE_ID, "year": "2008"}]
        if world.cause == "yt_sibling_error"
        else []
    )
    yt.set_album(
        _YT_BROWSE_ID,
        FakeYTMusic.make_album_fixture(
            audio_playlist_id="OLAK5uy-generated",
            title="Generated Album",
            artists=[{"name": "Generated Artist"}],
            year="1996",
            tracks=[{
                "title": "Generated Track",
                "artists": [{"name": "Generated Artist"}],
                "duration_seconds": 180,
                "track_number": 1,
                "disc_number": 1,
                "videoId": "generated-video",
            }],
            other_versions=other_versions,
        ),
    )
    if world.cause == "yt_sibling_error":
        yt.set_album_error(
            _YT_BROKEN_BROWSE_ID,
            _generated_album_error(world.album_error_kind),
        )

    sibling_ids = (
        (_MB_RELEASE, _MB_RELEASE_B)
        if world.cause == "scoring_deadline"
        else (_MB_RELEASE,)
    )

    def _group(identifier: str) -> dict[str, object] | None:
        if identifier != _MB_RG:
            return None
        return {
            "title": "Generated Album",
            "type": "Album",
            "releases": [{"id": sibling_id} for sibling_id in sibling_ids],
        }

    def _distance(*, mbid: str, **_kwargs: object) -> BeetsDistanceResult:
        if world.cause == "scoring_deadline":
            clock.advance(world.scoring_seconds)
        return BeetsDistanceResult(
            outcome="ok",
            distance=0.05,
            matched_tracks=1,
            total_local_tracks=1,
            total_mb_tracks=1,
            extra_local_tracks=0,
            extra_mb_tracks=0,
            components={"tracks": 0.05},
            candidate_mbid=mbid,
            candidate_release_group_id=_MB_RG,
            request_release_group_id=_MB_RG,
        )

    return resolve_youtube_album(
        _MB_RG,
        pdb=pdb,
        mb_get_release=_mb_release,
        mb_get_release_group_releases=_group,
        discogs_get_release=_empty_lookup,
        discogs_get_master_releases=_empty_lookup,
        yt_client=yt,
        distance_fn=_distance,
        cache=None,
        refresh=True,
        sleep_fn=lambda _seconds: None,
        deadline_seconds=_DEADLINE_SECONDS,
        monotonic_fn=lambda: clock.now,
    )


@st.composite
def malformed_search_rows(
    draw: st.DrawFn,
) -> tuple[dict[str, object], ...]:
    """Non-empty search rows whose browseId is absent or unusable."""
    count = draw(st.integers(min_value=1, max_value=5))
    rows: list[dict[str, object]] = []
    for _ in range(count):
        row: dict[str, object] = {
            "title": draw(
                st.text(
                    alphabet=st.characters(
                        whitelist_categories=("L", "N", "Zs")
                    ),
                    max_size=20,
                )
            ),
            "trackCount": draw(st.integers(min_value=0, max_value=30)),
        }
        if draw(st.booleans()):
            row["browseId"] = draw(st.sampled_from((None, "", 0, False)))
        rows.append(row)
    return tuple(rows)


_UNSYNTHESIZABLE_TRACKS = st.lists(
    st.one_of(
        st.none(),
        st.booleans(),
        st.integers(min_value=-2, max_value=2),
        st.text(max_size=8),
    ),
    min_size=0,
    max_size=6,
)


class TestIncompleteMatrixPersistenceGenerated(unittest.TestCase):
    @given(rows=malformed_search_rows())
    @example(rows=({"title": "present", "browseId": ""},))
    def test_nonempty_search_without_usable_browse_id_never_writes_empty(
        self,
        rows: tuple[dict[str, object], ...],
    ) -> None:
        yt = FakeYTMusic()
        yt.set_search(_YT_QUERY, list(rows))
        pdb = _MappingWriteDB()

        result = _resolve_generated_yt(yt, pdb)

        assert_incomplete_matrix_not_persisted(IncompleteMatrixObservation(
            source_shape="malformed_search",
            outcome=result.outcome,
            durable_write_sizes=tuple(pdb.mapping_write_sizes),
            durable_cache_present=(
                pdb.get_youtube_album_mapping(_MB_RG, "mb") is not None
            ),
        ))

    @given(tracks=_UNSYNTHESIZABLE_TRACKS)
    @example(tracks=[])
    def test_album_without_synthesizable_tracks_never_writes_empty(
        self,
        tracks: list[object],
    ) -> None:
        yt = FakeYTMusic()
        yt.set_search(
            _YT_QUERY,
            [{
                "browseId": _YT_BROWSE_ID,
                "title": "Generated Album",
                "year": "1996",
                "trackCount": 1,
            }],
        )
        yt.set_album(
            _YT_BROWSE_ID,
            {
                "audioPlaylistId": "OLAK5uy-generated",
                "title": "Generated Album",
                "artists": [{"name": "Generated Artist"}],
                "year": "1996",
                "tracks": tracks,
                "other_versions": [],
            },
        )
        pdb = _MappingWriteDB()

        result = _resolve_generated_yt(yt, pdb)

        assert_incomplete_matrix_not_persisted(IncompleteMatrixObservation(
            source_shape="unscoreable_album",
            outcome=result.outcome,
            durable_write_sizes=tuple(pdb.mapping_write_sizes),
            durable_cache_present=(
                pdb.get_youtube_album_mapping(_MB_RG, "mb") is not None
            ),
        ))

    @given(world=incomplete_collection_worlds())
    @example(world=IncompleteCollectionWorld(
        cause="scoring_deadline",
        has_durable_cache=False,
        scoring_seconds=60,
    ))
    @example(world=IncompleteCollectionWorld(
        cause="yt_sibling_error",
        has_durable_cache=True,
        album_error_kind="server",
    ))
    def test_incomplete_collection_is_never_selectable_or_persisted(
        self,
        world: IncompleteCollectionWorld,
    ) -> None:
        pdb = _MappingWriteDB()
        expected_cached_ids: tuple[str, ...] = ()
        if world.has_durable_cache:
            _seed_generated_complete_cache(pdb)
            expected_cached_ids = (_YT_CACHED_BROWSE_ID,)

        result = _resolve_incomplete_collection(world, pdb)
        durable = pdb.get_youtube_album_mapping(_MB_RG, "mb") or []

        assert_incomplete_collection_preserves_durable_truth(
            IncompleteCollectionObservation(
                cause=world.cause,
                had_durable_cache=world.has_durable_cache,
                outcome=result.outcome,
                from_cache=result.from_cache,
                returned_browse_ids=tuple(
                    release.yt_browse_id
                    for release in result.youtube_releases
                ),
                durable_write_sizes=tuple(pdb.mapping_write_sizes),
                durable_browse_ids=tuple(
                    str(row["yt_browse_id"]) for row in durable
                ),
                expected_cached_browse_ids=expected_cached_ids,
            )
        )


class TestYoutubeAlbumSiblingDeadlineGenerated(unittest.TestCase):
    @given(
        durations=st.lists(
            st.integers(min_value=0, max_value=90),
            min_size=1,
            max_size=12,
        ).map(tuple)
    )
    @example(durations=(55, 55, 55, 55, 55))
    def test_real_sibling_fetch_stops_at_first_observed_breach(
        self,
        durations: tuple[int, ...],
    ) -> None:
        now = 0.0
        started_ids: list[str] = []
        summaries: list[object] = [
            {"id": f"sibling-{index}"}
            for index in range(len(durations))
        ]

        def _clock_breached() -> bool:
            return now >= _DEADLINE_SECONDS

        def _fetch(identifier: str) -> dict[str, object]:
            nonlocal now
            duration = durations[len(started_ids)]
            started_ids.append(identifier)
            now += duration
            return {
                "id": identifier,
                "title": "Generated album",
                "artist_name": "Generated artist",
            }

        result = _fetch_mb_siblings(
            summaries,
            "mb",
            _fetch,
            lambda _identifier: None,
            deadline_breached=_clock_breached,
        )
        assert_sibling_fetch_deadline(SiblingFetchObservation(
            durations=durations,
            started_ids=tuple(started_ids),
            returned_ids=tuple(str(row["id"]) for row in result.records),
            complete=result.complete,
        ))


class TestInvariantCheckersTripOnViolations(unittest.TestCase):
    def test_cache_deadline_checker_rejects_post_breach_calls(self) -> None:
        mutants = (
            CacheDeadlineObservation(
                breach_after="redis_get",
                yt_search_calls=1,
                redis_set_calls=0,
                final_check_stage="after the YT search cache read",
            ),
            CacheDeadlineObservation(
                breach_after="yt_search",
                yt_search_calls=1,
                redis_set_calls=1,
                final_check_stage="after YT search",
            ),
        )
        for mutant in mutants:
            with self.subTest(mutant=mutant), self.assertRaises(AssertionError):
                assert_cached_search_deadline(mutant)

    def test_persistence_checker_rejects_false_empty_truth_mutants(self) -> None:
        mutants = (
            IncompleteMatrixObservation(
                source_shape="malformed_search",
                outcome="ok",
                durable_write_sizes=(0,),
                durable_cache_present=True,
            ),
            IncompleteMatrixObservation(
                source_shape="unscoreable_album",
                outcome="youtube_parse_failed",
                durable_write_sizes=(0,),
                durable_cache_present=True,
            ),
        )
        for mutant in mutants:
            with self.subTest(mutant=mutant), self.assertRaises(AssertionError):
                assert_incomplete_matrix_not_persisted(mutant)

    def test_collection_checker_rejects_selectable_and_truncating_mutants(
        self,
    ) -> None:
        mutants = (
            IncompleteCollectionObservation(
                cause="scoring_deadline",
                had_durable_cache=False,
                outcome="ok",
                from_cache=False,
                returned_browse_ids=(_YT_BROWSE_ID,),
                durable_write_sizes=(),
                durable_browse_ids=(),
                expected_cached_browse_ids=(),
            ),
            IncompleteCollectionObservation(
                cause="yt_sibling_error",
                had_durable_cache=True,
                outcome="ok",
                from_cache=False,
                returned_browse_ids=(_YT_BROWSE_ID,),
                durable_write_sizes=(1,),
                durable_browse_ids=(_YT_BROWSE_ID,),
                expected_cached_browse_ids=(_YT_CACHED_BROWSE_ID,),
            ),
        )
        for mutant in mutants:
            with self.subTest(mutant=mutant), self.assertRaises(AssertionError):
                assert_incomplete_collection_preserves_durable_truth(mutant)

    def test_checker_rejects_extra_fetch_and_false_completion_mutants(
        self,
    ) -> None:
        mutants = (
            SiblingFetchObservation(
                durations=(55, 55, 55),
                started_ids=("sibling-0", "sibling-1", "sibling-2"),
                returned_ids=("sibling-0",),
                complete=False,
            ),
            SiblingFetchObservation(
                durations=(55, 55),
                started_ids=("sibling-0", "sibling-1"),
                returned_ids=("sibling-0", "sibling-1"),
                complete=True,
            ),
            SiblingFetchObservation(
                durations=(55, 55),
                started_ids=("sibling-0", "sibling-1"),
                returned_ids=("sibling-0",),
                complete=False,
            ),
        )
        for mutant in mutants:
            with self.subTest(mutant=mutant), self.assertRaises(AssertionError):
                assert_sibling_fetch_deadline(mutant)
