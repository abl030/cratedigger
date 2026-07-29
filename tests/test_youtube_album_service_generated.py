"""Generated retry-exhaustion contract for the YouTube resolver boundary."""

from __future__ import annotations

import copy
import unittest

import requests
from hypothesis import example, given, settings
from hypothesis import strategies as st

import tests._hypothesis_profiles  # noqa: F401 - loads active profile
from lib.youtube_album_service import (
    ResolvedYoutubeRelease,
    YoutubeAlbumResolverResult,
)
from tests.test_youtube_transport import (
    RETRYABLE_METHODS,
    RETRYABLE_STATUSES,
    RetryObservation,
    RetryWorld,
    assert_retry_invariants,
    expected_cached_releases,
    run_retry_world,
)


@st.composite
def retry_worlds(draw: st.DrawFn) -> RetryWorld:
    return RetryWorld(
        status=draw(st.sampled_from(RETRYABLE_STATUSES)),
        method=draw(st.sampled_from(RETRYABLE_METHODS)),
        cache_posture=draw(st.sampled_from(("absent", "empty", "nonempty"))),
        operation_site=draw(st.sampled_from(("search", "seed", "sibling"))),
    )


def _minimal_release(browse_id: str) -> ResolvedYoutubeRelease:
    return ResolvedYoutubeRelease(
        yt_browse_id=browse_id,
        yt_url=f"https://music.youtube.com/browse/{browse_id}",
        track_count=0,
        tracks=[],
        distances=[],
    )


def _good_observation(world: RetryWorld) -> RetryObservation:
    if world.operation_site == "sibling":
        result = YoutubeAlbumResolverResult(
            outcome="ok",
            release_group_identifier="11111111-1111-1111-1111-111111111111",
            source="mb",
            from_cache=False,
            youtube_releases=[
                _minimal_release("MPREb-seed"),
                _minimal_release("MPREb-healthy"),
            ],
        )
        return RetryObservation(
            world=world,
            result=result,
            escaped_exception=None,
            attempts=4,
            upsert_calls=1,
            durable_before=None,
            durable_after=[],
        )

    if world.cache_posture == "nonempty":
        result = YoutubeAlbumResolverResult(
            outcome="ok",
            release_group_identifier="11111111-1111-1111-1111-111111111111",
            source="mb",
            from_cache=True,
            youtube_releases=expected_cached_releases(),
            error_message=(
                "unresolved_mirror_unavailable: serving from cache"),
        )
        durable: list[dict[str, object]] | None = [{"sentinel": "unchanged"}]
    else:
        result = YoutubeAlbumResolverResult(
            outcome="unresolved_mirror_unavailable",
            release_group_identifier="11111111-1111-1111-1111-111111111111",
            source="mb",
            youtube_releases=[],
            error_message="YT retries exhausted",
        )
        durable = [] if world.cache_posture == "empty" else None
    return RetryObservation(
        world=world,
        result=result,
        escaped_exception=None,
        attempts=4,
        upsert_calls=0,
        durable_before=copy.deepcopy(durable),
        durable_after=copy.deepcopy(durable),
    )


class TestYoutubeRetryExhaustionGenerated(unittest.TestCase):
    @settings(max_examples=30, deadline=None)
    @given(world=retry_worlds())
    @example(world=RetryWorld(429, "POST", "absent", "search"))
    @example(world=RetryWorld(500, "GET", "nonempty", "seed"))
    @example(world=RetryWorld(502, "POST", "empty", "seed"))
    @example(world=RetryWorld(503, "GET", "absent", "search"))
    @example(world=RetryWorld(504, "POST", "nonempty", "sibling"))
    def test_real_adapter_obeys_world_specific_retry_and_cache_contract(
        self,
        world: RetryWorld,
    ) -> None:
        assert_retry_invariants(run_retry_world(world))


class TestRetryInvariantCheckerTripsOnKnownBad(unittest.TestCase):
    def test_rejects_escaped_retry_error(self) -> None:
        observation = _good_observation(
            RetryWorld(503, "GET", "absent", "search"))
        observation.result = None
        observation.escaped_exception = requests.exceptions.RetryError(
            "opaque exhaustion")
        with self.assertRaises(AssertionError):
            assert_retry_invariants(observation)

    def test_rejects_generic_failure_outcome(self) -> None:
        observation = _good_observation(
            RetryWorld(503, "GET", "absent", "search"))
        observation.result = YoutubeAlbumResolverResult(
            outcome="transient",
            error_message="generic failure",
        )
        with self.assertRaises(AssertionError):
            assert_retry_invariants(observation)

    def test_rejects_wrong_result_type(self) -> None:
        observation = _good_observation(
            RetryWorld(503, "GET", "absent", "search"))
        observation.result = {"outcome": "unresolved_mirror_unavailable"}
        with self.assertRaises(TypeError):
            assert_retry_invariants(observation)

    def test_rejects_wrong_nonempty_fallback_matrix(self) -> None:
        observation = _good_observation(
            RetryWorld(429, "POST", "nonempty", "seed"))
        assert isinstance(
            observation.result, YoutubeAlbumResolverResult)
        observation.result.youtube_releases = [
            _minimal_release("MPREb-wrong")]
        with self.assertRaises(AssertionError):
            assert_retry_invariants(observation)

    def test_rejects_write_before_outer_return(self) -> None:
        for posture in ("nonempty", "empty", "absent"):
            with self.subTest(cache_posture=posture):
                observation = _good_observation(RetryWorld(
                    503, "GET", posture, "search"))
                observation.upsert_calls = 1
                observation.durable_after = [{"sentinel": "rewritten"}]
                with self.assertRaises(AssertionError):
                    assert_retry_invariants(observation)

    def test_rejects_sibling_overreach(self) -> None:
        observation = _good_observation(
            RetryWorld(503, "GET", "absent", "sibling"))
        assert isinstance(
            observation.result, YoutubeAlbumResolverResult)
        observation.result.youtube_releases = [
            _minimal_release("MPREb-seed")]
        with self.assertRaises(AssertionError):
            assert_retry_invariants(observation)

    def test_rejects_bypassed_attempt_count(self) -> None:
        observation = _good_observation(
            RetryWorld(503, "GET", "absent", "search"))
        observation.attempts = 1
        with self.assertRaises(AssertionError):
            assert_retry_invariants(observation)


if __name__ == "__main__":
    unittest.main()
