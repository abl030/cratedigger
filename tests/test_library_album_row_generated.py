"""Generated parity property for the shared upgrade_queued decision.

Issue #1355 item 6: `_pipeline_upgrade_queued` in `web/library_album_row.py`
is the one owner both the list-row projection
(`LibraryAlbumRow.from_pipeline_request`) and the detail projection
(`web/library_album_detail_service.py::build_library_album_detail`) call.
This drives both real outer adapters over generated pipeline rows and
asserts they never disagree on `upgrade_queued`.
"""
from __future__ import annotations

import unittest

from hypothesis import example, given
from hypothesis import strategies as st

import tests._hypothesis_profiles  # noqa: F401
from tests.helpers import make_request_row
from web.library_album_detail_service import build_library_album_detail
from web.library_album_row import LibraryAlbumRow

_STATUSES = (
    "initializing", "wanted", "downloading", "processing",
    "imported", "unsearchable", "replaced",
)
_OVERRIDES = (None, "flac", "mp3")
_TARGET_FORMATS = (None, "lossless", "default")
# A well-formed MB UUID so the detail service's identity-attachment guard
# resolves cleanly; identity is not the decision under test, only the three
# fields below are.
_MB_RELEASE_ID = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"


def upgrade_queued_agreement_violations(
    list_value: bool, detail_value: bool,
) -> list[str]:
    """Independent checker clause, self-tested below (Q1 + Q3)."""
    violations: list[str] = []
    if list_value != detail_value:
        violations.append(
            "list-row and detail projections disagree on upgrade_queued: "
            f"list={list_value!r} detail={detail_value!r}"
        )
    return violations


@st.composite
def _upgrade_queued_worlds(draw: st.DrawFn) -> dict[str, object]:
    return {
        "status": draw(st.sampled_from(_STATUSES)),
        "search_filetype_override": draw(st.sampled_from(_OVERRIDES)),
        "target_format": draw(st.sampled_from(_TARGET_FORMATS)),
    }


def _pipeline_row(world: dict[str, object]) -> dict[str, object]:
    return {
        **make_request_row(
            mb_release_id=_MB_RELEASE_ID,
            status=world["status"],
            search_filetype_override=world["search_filetype_override"],
            target_format=world["target_format"],
        ),
        "has_captured_history": False,
        "provisional_lossless": False,
    }


def _list_row_upgrade_queued(row: dict[str, object]) -> bool:
    return LibraryAlbumRow.from_pipeline_request(
        row, track_count=0,
    ).upgrade_queued


def _detail_upgrade_queued(row: dict[str, object]) -> bool:
    detail_row: dict[str, object] = {
        "id": 7,
        "album": "Test Album",
        "artist": "Test Artist",
        "mb_albumid": row.get("mb_release_id"),
        "discogs_albumid": row.get("discogs_release_id"),
        "tracks": [],
    }
    detail = build_library_album_detail(
        detail_row=detail_row,
        pipeline_request=row,
        download_history=[],
    )
    return detail.upgrade_queued


class TestUpgradeQueuedProjectionsAgree(unittest.TestCase):
    """The list-row and detail projections must never disagree."""

    @example(world={
        "status": "wanted", "search_filetype_override": "flac",
        "target_format": None,
    })
    @example(world={
        "status": "wanted", "search_filetype_override": None,
        "target_format": "lossless",
    })
    @example(world={
        "status": "wanted", "search_filetype_override": None,
        "target_format": None,
    })
    @example(world={
        "status": "imported", "search_filetype_override": "flac",
        "target_format": "lossless",
    })
    @given(world=_upgrade_queued_worlds())
    def test_projections_agree(self, world: dict[str, object]) -> None:
        row = _pipeline_row(world)
        list_value = _list_row_upgrade_queued(row)
        detail_value = _detail_upgrade_queued(row)
        violations = upgrade_queued_agreement_violations(
            list_value, detail_value)
        self.assertEqual(violations, [], violations)


class TestUpgradeQueuedCheckerClause(unittest.TestCase):
    """Known-bad self-test for the one checker clause above."""

    def test_clause_trips_on_disagreement(self) -> None:
        violations = upgrade_queued_agreement_violations(True, False)
        self.assertEqual(len(violations), 1)
        self.assertIn("disagree", violations[0])

    def test_clause_stays_quiet_when_projections_agree(self) -> None:
        for value in (True, False):
            with self.subTest(value=value):
                self.assertEqual(
                    upgrade_queued_agreement_violations(value, value), [])
