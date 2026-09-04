"""Generated parity property for the shared upgrade_queued decision.

Issue #1355 item 6: `_pipeline_upgrade_queued` in `web/library_album_row.py`
is the one owner every projection that surfaces `upgrade_queued` calls:
the pipeline-only list row (`LibraryAlbumRow.from_pipeline_request`), the
in-library row overlay (`LibraryAlbumRow.with_pipeline_request` -- the
overlay step behind the adapter an operator actually sees paired with the
detail page, `LibraryAlbumRow.from_beets_album_with_pipeline`, called by
`web/library_artist_service.py::build_library_artist_rows`), and the
detail projection
(`web/library_album_detail_service.py::build_library_album_detail`). This
drives all four real functions over generated pipeline rows and asserts
none of them ever disagrees with the detail projection on
`upgrade_queued`.

Batch F, F2 (issue #1355 residual triage round 2): the property used to
stop one hop short of the outermost real adapter,
`LibraryAlbumRow.from_beets_album_with_pipeline` -- it drove
`from_beets_album` and `with_pipeline_request` directly instead of the
overlay adapter that composes them, so a defect in the composition itself
(the identity-attachment step between the two calls) had no arm to catch
it. `_overlay_adapter_upgrade_queued` now drives that adapter directly.
"""
from __future__ import annotations

import unittest

from hypothesis import example, given
from hypothesis import strategies as st

import tests._hypothesis_profiles  # noqa: F401
from lib.release_identity import ReleaseIdentity
from tests.helpers import make_request_row
from web.library_album_detail_service import build_library_album_detail
from web.library_album_row import LibraryAlbumRow

_STATUSES = (
    "initializing", "wanted", "downloading", "processing",
    "imported", "unsearchable", "replaced",
)
# Empty string is included alongside None and real values so the
# adapters are exercised on this shape too. This property only checks
# that the adapters AGREE with each other, not that "" resolves like
# None -- all three call the same owner, so an owner-level truthiness
# bug moves every adapter together and this property stays green
# either way. The deterministic subTest table in
# tests/test_library_album_row.py is what pins "" to False against the
# owner directly.
_OVERRIDES = (None, "", "flac", "mp3")
_TARGET_FORMATS = (None, "", "lossless", "default")
# A well-formed MB UUID so the detail service's identity-attachment guard
# resolves cleanly; identity is not the decision under test, only the three
# fields below are.
_MB_RELEASE_ID = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"


def upgrade_queued_agreement_violations(
    left_value: bool, right_value: bool, *, left_label: str,
) -> list[str]:
    """Independent checker clause, self-tested below (Q1 + Q3).

    ``left_label`` names which adapter ``left_value`` came from, so a
    generated failure names the diverging adapter instead of a bare
    ``left=... right=...`` pair a reader has to re-derive by hand.
    """
    violations: list[str] = []
    if left_value != right_value:
        violations.append(
            f"{left_label} disagrees with the detail projection on "
            f"upgrade_queued: {left_label}={left_value!r} "
            f"detail={right_value!r}"
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
    """The pipeline-only list row (`from_pipeline_request`)."""
    return LibraryAlbumRow.from_pipeline_request(
        row, track_count=0,
    ).upgrade_queued


def _attached_row_upgrade_queued(row: dict[str, object]) -> bool:
    """The in-library row overlay (`with_pipeline_request`) -- the sibling
    an operator actually sees paired with the detail page for one beets
    album, distinct from the pipeline-only list row above."""
    base = LibraryAlbumRow.from_beets_album(
        {
            "id": 7,
            "album": "Test Album",
            "artist": "Test Artist",
            "track_count": 1,
            "added": 0.0,
            # Present so this synthetic album is a combination the real
            # entry point (LibraryAlbumRow.from_beets_album_with_pipeline)
            # could actually observe -- an untracked beets album has no
            # release identity, and its identity guard would reject an
            # attached pipeline row otherwise. upgrade_queued itself does
            # not read this field.
            "mb_albumid": _MB_RELEASE_ID,
        },
        rank_fn=lambda _fmt, _kbps: "transparent",
    )
    return base.with_pipeline_request(row).upgrade_queued


def _overlay_adapter_upgrade_queued(row: dict[str, object]) -> bool:
    """The true outermost overlay adapter (`from_beets_album_with_pipeline`)
    -- one hop past `with_pipeline_request`, and the one
    `build_library_artist_rows` actually calls for an in-library album
    with an attached pipeline request."""
    album = {
        "id": 7,
        "album": "Test Album",
        "artist": "Test Artist",
        "track_count": 1,
        "added": 0.0,
        "mb_albumid": _MB_RELEASE_ID,
    }
    identity = ReleaseIdentity.from_id(_MB_RELEASE_ID)
    assert identity is not None
    return LibraryAlbumRow.from_beets_album_with_pipeline(
        album,
        pipeline_row=row,
        rank_fn=lambda _fmt, _kbps: "transparent",
        attached_identity=identity,
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
    """No real projection may disagree with the detail page on
    `upgrade_queued`."""

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
    @example(world={
        "status": "wanted", "search_filetype_override": "",
        "target_format": None,
    })
    @example(world={
        "status": "wanted", "search_filetype_override": None,
        "target_format": "",
    })
    @given(world=_upgrade_queued_worlds())
    def test_projections_agree(self, world: dict[str, object]) -> None:
        row = _pipeline_row(world)
        detail_value = _detail_upgrade_queued(row)
        list_value = _list_row_upgrade_queued(row)
        attached_value = _attached_row_upgrade_queued(row)
        overlay_value = _overlay_adapter_upgrade_queued(row)
        violations = [
            *upgrade_queued_agreement_violations(
                list_value, detail_value, left_label="from_pipeline_request"),
            *upgrade_queued_agreement_violations(
                attached_value, detail_value, left_label="with_pipeline_request"),
            *upgrade_queued_agreement_violations(
                overlay_value, detail_value,
                left_label="from_beets_album_with_pipeline"),
        ]
        self.assertEqual(violations, [], violations)


class TestUpgradeQueuedCheckerClause(unittest.TestCase):
    """Known-bad self-test for the one checker clause above."""

    def test_clause_trips_on_disagreement(self) -> None:
        violations = upgrade_queued_agreement_violations(
            True, False, left_label="from_pipeline_request")
        self.assertEqual(len(violations), 1)
        self.assertIn("disagree", violations[0])
        self.assertIn("from_pipeline_request", violations[0])

    def test_clause_stays_quiet_when_projections_agree(self) -> None:
        for value in (True, False):
            with self.subTest(value=value):
                self.assertEqual(
                    upgrade_queued_agreement_violations(
                        value, value, left_label="from_pipeline_request"),
                    [],
                )
