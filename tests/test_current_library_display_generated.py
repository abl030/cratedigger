#!/usr/bin/env python3
"""Generated authority laws for current-library request displays."""

from __future__ import annotations

import os
import unittest

from hypothesis import example, given, strategies as st
import msgspec

import tests._hypothesis_profiles  # noqa: F401
from lib.current_library_display import (
    CurrentLibraryDisplay,
    CurrentLibraryUnavailableDisplay,
    CurrentLibraryUniqueDisplay,
    current_library_display,
    resolve_request_current_library,
)
from tests.fakes import FakeBeetsDB


MB_ID = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
DISCOGS_ID = "12856590"


def assert_display_authority(
    display: CurrentLibraryDisplay,
    *,
    expected_state: str,
    expected_path: str | None,
    poisoned_cache: str,
) -> None:
    """Executable law: only a fresh unique resolver result exposes a path."""

    state = msgspec.to_builtins(display)["state"]
    if state != expected_state:
        raise AssertionError(f"display state drifted: {state!r}")
    actual_path = getattr(display, "path", None)
    if actual_path != expected_path:
        raise AssertionError("display path did not come from fresh Beets authority")
    if actual_path == poisoned_cache:
        raise AssertionError("display trusted album_requests.imported_path")


class TestCurrentLibraryDisplayGenerated(unittest.TestCase):
    @given(
        source=st.sampled_from(("mb", "discogs_modern", "discogs_legacy")),
        cardinality=st.integers(min_value=0, max_value=2),
        stale_segment=st.text(
            alphabet=st.characters(
                whitelist_categories=("Ll", "Lu", "Nd"),
                blacklist_characters=("/", "\\", "\x00"),
            ),
            min_size=1,
            max_size=20,
        ),
        moved_segment=st.text(
            alphabet=st.characters(
                whitelist_categories=("Ll", "Lu", "Nd"),
                blacklist_characters=("/", "\\", "\x00"),
            ),
            min_size=1,
            max_size=20,
        ),
    )
    @example(
        source="discogs_legacy",
        cardinality=1,
        stale_segment="old-cache",
        moved_segment="Beyonce-current",
    )
    def test_cache_never_changes_exact_typed_display(
        self,
        source: str,
        cardinality: int,
        stale_segment: str,
        moved_segment: str,
    ) -> None:
        release_id = MB_ID if source == "mb" else DISCOGS_ID
        row: dict[str, object] = {
            "mb_release_id": release_id,
            "discogs_release_id": (
                DISCOGS_ID if source == "discogs_modern" else None
            ),
            "imported_path": f"/poisoned/{stale_segment}",
        }
        beets = FakeBeetsDB(library_root="/library")
        album_ids = list(range(100, 100 + cardinality))
        beets.set_album_ids_for_release(release_id, album_ids)
        expected_path = None
        if cardinality == 1:
            beets.set_item_paths(
                release_id,
                [(1001, f"/library/{moved_segment}/01.flac")],
            )
            expected_path = os.path.join("/library", moved_segment)

        display = current_library_display(
            resolve_request_current_library(row, beets),
        )

        expected_state = (
            "missing" if cardinality == 0
            else "unique" if cardinality == 1
            else "ambiguous"
        )
        assert_display_authority(
            display,
            expected_state=expected_state,
            expected_path=expected_path,
            poisoned_cache=str(row["imported_path"]),
        )

    def test_conflicting_request_ids_never_reach_beets(self) -> None:
        beets = FakeBeetsDB()
        display = current_library_display(resolve_request_current_library({
            "mb_release_id": MB_ID,
            "discogs_release_id": DISCOGS_ID,
            "imported_path": "/poisoned/cache",
        }, beets))
        self.assertIsInstance(display, CurrentLibraryUnavailableDisplay)
        assert isinstance(display, CurrentLibraryUnavailableDisplay)
        self.assertEqual(msgspec.to_builtins(display)["state"], "unavailable")
        self.assertEqual(display.reason, "conflicting_request_identity")
        self.assertEqual(beets.resolve_current_release_calls, [])

    def test_checker_rejects_the_cached_path_mutant(self) -> None:
        mutant = CurrentLibraryUniqueDisplay(
            release_source="musicbrainz",
            release_id=MB_ID,
            album_id=1,
            path="/poisoned/cache",
        )
        with self.assertRaisesRegex(AssertionError, "trusted album_requests"):
            assert_display_authority(
                mutant,
                expected_state="unique",
                expected_path="/poisoned/cache",
                poisoned_cache="/poisoned/cache",
            )


if __name__ == "__main__":
    unittest.main()
