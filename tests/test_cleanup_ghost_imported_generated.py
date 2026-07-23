"""Generated fail-closed identity laws for ghost-row cleanup."""

from __future__ import annotations

from uuid import UUID
from typing import cast
import unittest

from hypothesis import given, strategies as st

from lib.beets_db import BeetsDB
from lib.release_identity import ReleaseIdentity
from scripts.cleanup_ghost_imported import classify_imported_rows
from tests.fakes import FakeBeetsDB


_MBIDS = st.binary(min_size=16, max_size=16).map(
    lambda raw: str(UUID(bytes=raw))
)


class TestGeneratedGhostCleanupAuthority(unittest.TestCase):
    @given(
        mbid=_MBIDS,
        discogs_id=st.integers(min_value=1, max_value=2_000_000_000),
    )
    def test_conflicting_identity_fields_are_never_auto_deleted(
        self,
        mbid: str,
        discogs_id: int,
    ) -> None:
        row: dict[str, object] = {
            "id": 1,
            "mb_release_id": mbid,
            "discogs_release_id": str(discogs_id),
            "artist_name": "Conflict",
            "album_title": "Manual Review",
        }

        ghosts, manual_review = classify_imported_rows(
            [row],
            cast(BeetsDB, FakeBeetsDB()),
        )

        self.assertEqual(ghosts, [])
        self.assertEqual(manual_review, [row])

    @given(
        mbid=_MBIDS,
        discogs_id=st.integers(min_value=1, max_value=2_000_000_000),
    )
    def test_known_bad_permissive_parser_accepts_the_conflict(
        self,
        mbid: str,
        discogs_id: int,
    ) -> None:
        self.assertIsNotNone(
            ReleaseIdentity.from_fields(mbid, str(discogs_id)),
        )


if __name__ == "__main__":
    unittest.main()
