"""Generated authority laws for current-library request displays."""

from __future__ import annotations

import os
import unittest

import msgspec
from hypothesis import example, given
from hypothesis import strategies as st

import tests._hypothesis_profiles  # noqa: F401
from lib.current_library_display import (
    CurrentLibraryDisplay,
    CurrentLibraryUnavailableDisplay,
    CurrentLibraryUniqueDisplay,
    current_library_display,
    resolve_request_current_library,
)
from tests.fakes import FakeBeetsDB
from tests.helpers import make_request_row
from web.library_album_row import LibraryAlbumRow
from web.library_artist_service import build_library_artist_rows

MB_ID = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
DISCOGS_ID = "12856590"


def assert_display_authority(
    display: CurrentLibraryDisplay,
    *,
    expected_state: str,
    expected_path: str | None,
) -> None:
    """Executable law: only a fresh unique resolver result exposes a path."""

    state = msgspec.to_builtins(display)["state"]
    if state != expected_state:
        raise AssertionError(f"display state drifted: {state!r}")
    actual_path = getattr(display, "path", None)
    if actual_path != expected_path:
        raise AssertionError("display path did not come from fresh Beets authority")


def assert_independent_library_facts(
    row: LibraryAlbumRow,
    *,
    held: bool,
    tracked: bool,
    captured: bool,
    verified: bool,
    provisional: bool,
) -> None:
    """Executable law: presence, tracking, acquisition, and proof do not infer peers."""
    expected = {
        "held": (row.in_library, held),
        "tracked": (row.pipeline_id is not None, tracked),
        "captured": (row.has_captured_history, captured if tracked else False),
        "verified": (row.pipeline_verified_lossless, verified if tracked else False),
        "provisional": (row.pipeline_provisional, provisional if tracked else False),
        "installed_quality": (row.library_rank is not None, held),
    }
    for fact, (actual, wanted) in expected.items():
        if actual != wanted:
            raise AssertionError(
                f"{fact} fact drifted: actual={actual!r}, expected={wanted!r}"
            )


def _fact_beets_album(source: str) -> dict[str, object]:
    return {
        "id": 7,
        "album": "Independent Facts",
        "artist": "Boundary Archivist",
        "year": 2001,
        "mb_albumid": MB_ID if source == "mb" else None,
        "discogs_albumid": DISCOGS_ID if source == "discogs" else None,
        "track_count": 2,
        "mb_releasegroupid": "11111111-1111-1111-1111-111111111111",
        "release_group_title": "Independent Facts",
        "added": 1773651901.0,
        "formats": "FLAC",
        "min_bitrate": 811000,
        "avg_bitrate": 922000,
        "type": "album",
        "label": "Archive",
        "country": "AU",
    }


class TestCurrentLibraryDisplayGenerated(unittest.TestCase):
    @given(
        source=st.sampled_from(("mb", "discogs_modern", "discogs_legacy")),
        cardinality=st.integers(min_value=0, max_value=2),
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
        moved_segment="Beyonce-current",
    )
    def test_exact_typed_display_follows_current_beets_cardinality(
        self,
        source: str,
        cardinality: int,
        moved_segment: str,
    ) -> None:
        release_id = MB_ID if source == "mb" else DISCOGS_ID
        row: dict[str, object] = {
            "mb_release_id": release_id,
            "discogs_release_id": (
                DISCOGS_ID if source == "discogs_modern" else None
            ),
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
        )

    def test_conflicting_request_ids_never_reach_beets(self) -> None:
        beets = FakeBeetsDB()
        display = current_library_display(resolve_request_current_library({
            "mb_release_id": MB_ID,
            "discogs_release_id": DISCOGS_ID,
        }, beets))
        self.assertIsInstance(display, CurrentLibraryUnavailableDisplay)
        assert isinstance(display, CurrentLibraryUnavailableDisplay)
        self.assertEqual(msgspec.to_builtins(display)["state"], "unavailable")
        self.assertEqual(display.reason, "conflicting_request_identity")
        self.assertEqual(beets.resolve_current_release_calls, [])

    def test_checker_rejects_a_non_authoritative_path_mutant(self) -> None:
        mutant = CurrentLibraryUniqueDisplay(
            release_source="musicbrainz",
            release_id=MB_ID,
            album_id=1,
            path="/poisoned/cache",
        )
        with self.assertRaisesRegex(AssertionError, "fresh Beets authority"):
            assert_display_authority(
                mutant,
                expected_state="unique",
                expected_path="/library/current",
            )


class TestIndependentLibraryFactsGenerated(unittest.TestCase):
    @given(
        source=st.sampled_from(("mb", "discogs")),
        shape=st.sampled_from(("held_untracked", "missing_tracked", "held_tracked")),
        status=st.sampled_from(("wanted", "imported", "replaced")),
        captured=st.booleans(),
        proof=st.sampled_from(("none", "verified", "provisional")),
    )
    @example(
        source="mb",
        shape="missing_tracked",
        status="wanted",
        captured=True,
        proof="verified",
    )
    @example(
        source="discogs",
        shape="held_untracked",
        status="replaced",
        captured=True,
        proof="provisional",
    )
    def test_exact_identity_merge_preserves_each_fact_independently(
        self,
        source: str,
        shape: str,
        status: str,
        captured: bool,
        proof: str,
    ) -> None:
        held = shape != "missing_tracked"
        tracked = shape != "held_untracked"
        verified = proof == "verified"
        provisional = proof == "provisional"
        library_albums = [_fact_beets_album(source)] if held else []
        pipeline_rows = []
        if tracked:
            pipeline_rows.append(make_request_row(
                id=42,
                artist_name="Boundary Archivist",
                album_title="Independent Facts",
                mb_release_id=MB_ID if source == "mb" else None,
                discogs_release_id=DISCOGS_ID if source == "discogs" else None,
                status=status,
                has_captured_history=captured,
                verified_lossless=verified,
                provisional_lossless=provisional,
            ))

        rows = build_library_artist_rows(
            library_albums=library_albums,
            pipeline_rows=pipeline_rows,
            track_counts={42: 2} if tracked else {},
            rank_fn=lambda _format, _bitrate: "lossless",
        )

        self.assertEqual(len(rows), 1)
        assert_independent_library_facts(
            rows[0],
            held=held,
            tracked=tracked,
            captured=captured,
            verified=verified,
            provisional=provisional,
        )

    def test_checker_rejects_presence_to_capture_inference_mutant(self) -> None:
        row = LibraryAlbumRow.from_beets_album(
            _fact_beets_album("mb"),
            rank_fn=lambda _format, _bitrate: "lossless",
        )
        mutant = msgspec.structs.replace(row, has_captured_history=True)

        with self.assertRaisesRegex(AssertionError, "captured fact drifted"):
            assert_independent_library_facts(
                mutant,
                held=True,
                tracked=False,
                captured=False,
                verified=False,
                provisional=False,
            )


if __name__ == "__main__":
    unittest.main()
