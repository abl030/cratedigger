"""Generated all-or-nothing replacement proof for CD-SEC-18."""

from __future__ import annotations

import os
import sys
import unittest
import uuid
from typing import Any

sys.path.append(os.path.dirname(__file__))
import conftest  # noqa: F401 -- sets TEST_DB_DSN

import psycopg2
from hypothesis import example, given, strategies as st

from lib.pipeline_db import PipelineDB
import tests._hypothesis_profiles  # noqa: F401


TEST_DSN = os.environ.get("TEST_DB_DSN")
_TEXT = st.text(
    alphabet=st.characters(blacklist_categories=("Cs", "Cc")),
    min_size=1,
    max_size=30,
)


def assert_old_tracklist_survived(
    before: list[dict[str, Any]],
    after: list[dict[str, Any]],
) -> None:
    """A failed replacement may not expose a partial new tracklist."""
    if after != before:
        raise AssertionError(
            f"old tracklist was not preserved: before={before!r}, after={after!r}",
        )


class TestTrackReplacementCheckerTripsOnKnownBadState(unittest.TestCase):
    def test_rejects_a_partial_replacement(self) -> None:
        with self.assertRaisesRegex(AssertionError, "old tracklist"):
            assert_old_tracklist_survived(
                [{"track_number": 1, "title": "Old"}],
                [{"track_number": 1, "title": "New"}],
            )


class TestTrackReplacementAtomicityGenerated(unittest.TestCase):
    db: PipelineDB

    @classmethod
    def setUpClass(cls) -> None:
        cls.db = PipelineDB(TEST_DSN)

    @classmethod
    def tearDownClass(cls) -> None:
        cls.db.close()

    @given(
        old_one=_TEXT,
        old_two=_TEXT,
        old_artist=st.one_of(st.none(), _TEXT),
        length_one=st.integers(min_value=0, max_value=10_000),
        length_two=st.integers(min_value=0, max_value=10_000),
    )
    @example(
        old_one="Old One",
        old_two="Old Two",
        old_artist="Old Artist",
        length_one=111,
        length_two=222,
    )
    def test_later_not_null_failure_preserves_every_old_track(
        self,
        old_one: str,
        old_two: str,
        old_artist: str | None,
        length_one: int,
        length_two: int,
    ) -> None:
        request_id = self.db.add_request(
            mb_release_id=f"track-replacement-{uuid.uuid4()}",
            artist_name="Generated Artist",
            album_title="Generated Album",
            source="request",
        )
        old_tracks = [
            {
                "disc_number": 1,
                "track_number": 1,
                "title": old_one,
                "length_seconds": length_one,
                "track_artist": old_artist,
            },
            {
                "disc_number": 1,
                "track_number": 2,
                "title": old_two,
                "length_seconds": length_two,
                "track_artist": None,
            },
        ]
        self.db.set_tracks(request_id, old_tracks)

        with self.assertRaises(psycopg2.IntegrityError):
            self.db.set_tracks(request_id, [
                {
                    "disc_number": 1,
                    "track_number": 1,
                    "title": "New first row",
                    "length_seconds": 1,
                    "track_artist": "New Artist",
                },
                {
                    "disc_number": 1,
                    "track_number": 2,
                    "title": None,
                    "length_seconds": 2,
                    "track_artist": "Broken later row",
                },
            ])

        assert_old_tracklist_survived(
            old_tracks,
            self.db.get_tracks(request_id),
        )
