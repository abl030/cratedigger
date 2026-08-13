"""Generated properties for the retag ``-W`` divergence census (#1093 item 1).

The pins in ``tests/test_retag_divergence_audit.py`` prove the exact
branches; these properties drive the REAL ``scan_retag_divergence`` over
combinations of (per-album DB identity x arbitrary item tag outcomes) and
patrol the world space around them.

Why this surface earns a property: the census is the ONLY visibility this
repository has into the ``-W`` residual (issue #1093 item 1) — a wrong
classification here means the cohort silently under- or over-reports, which
is exactly the failure mode the instrument exists to prevent. A "clean"
report the operator trusts is only as good as these invariants.

Invariants patrolled — each is a module-level checker so the known-bad
self-tests below can call it directly:

V1  An item classified ``unreadable`` never carries a tag value; every
    other item always does. Fail-closed is a structural property of the
    report, not just the pure classifier.
V2  Only a non-agreeing album ever appears in ``report.albums``.
V3  The report's counts are internally consistent: ``albums_unreadable``
    and ``albums_empty`` are mutually exclusive and both bounded by the
    listed albums; ``albums_diverging`` and
    ``albums_file_tag_present_db_absent`` are each independently bounded;
    ``status == "divergence_found"`` iff at least one of those two
    independent counts is nonzero; ``status == "clean"`` iff no album is
    listed; ``items_unreadable`` never exceeds ``items_read``.
V4  The scan's counts and per-item classification honour exactly what the
    world was constructed to be: every row is scanned, every item path is
    read, a zero-item album is always listed ``"empty"``, an all-agreeing
    album is never listed, any other album's items classify exactly as its
    construction intended, AND the album's DISPLAY class
    (``album_class``) matches a precedence judgment computed
    INDEPENDENTLY of ``lib.retag_divergence_audit``'s own
    ``album_class_from_items``/``_ALBUM_CLASS_PRECEDENCE`` — #1093 review
    finding 1 (M1): the previous version of this check compared the report
    against the SAME production function that built it, so a mutant
    reordering the real precedence tuple still passed every test.

#1093 review finding 1 (M2): ``ITEM_DESIGNS`` previously had no "the DB
names an identity but the file tag is BLANK" design — exactly what a file
``-W`` never wrote a tag to looks like — so a mutant collapsing that branch
to "agrees" survived. ``blank`` below closes that gap.
"""

from __future__ import annotations

import unittest
from collections.abc import Callable, Sequence
from typing import Literal

from hypothesis import example, given, settings
from hypothesis import strategies as st

import tests._hypothesis_profiles  # noqa: F401  (loads the active profile)
from lib.beets_db import BeetsAlbumIdentityRow
from lib.retag_divergence_audit import (
    RetagDivergenceAlbum,
    RetagDivergenceAlbumClass,
    RetagDivergenceCounts,
    RetagDivergenceItem,
    RetagDivergenceItemClass,
    RetagDivergenceReport,
    scan_retag_divergence,
)
from tests.fakes import FakeBeetsDB
from tests.test_beets_retag import MERGED, SURVIVOR
from tests.test_retag_divergence_audit import _read_tag_from_map

# ---------------------------------------------------------------------------
# Invariant checkers — module level so the known-bad self-tests can call them
# ---------------------------------------------------------------------------


def check_fail_closed_unreadable_never_carries_a_tag(
    report: RetagDivergenceReport,
) -> None:
    """V1."""
    for album in report.albums:
        for item in album.items:
            if item.item_class == "unreadable":
                if item.file_mb_albumid is not None:
                    raise AssertionError(
                        f"unreadable item {item.path!r} carries tag value "
                        f"{item.file_mb_albumid!r} — a file we cannot read "
                        "must never report a tag"
                    )
            elif item.file_mb_albumid is None:
                raise AssertionError(
                    f"readable item {item.path!r} (class "
                    f"{item.item_class!r}) carries no tag value"
                )


def check_only_nonagreeing_albums_are_listed(
    report: RetagDivergenceReport,
) -> None:
    """V2."""
    for album in report.albums:
        if album.album_class == "agrees":
            raise AssertionError(
                f"album {album.album_id} is listed with class 'agrees' — "
                "only non-agreeing albums may be listed"
            )


def check_counts_are_internally_consistent(
    report: RetagDivergenceReport,
) -> None:
    """V3."""
    counts = report.counts
    if counts.albums_unreadable + counts.albums_empty > len(report.albums):
        raise AssertionError(
            f"albums_unreadable {counts.albums_unreadable} + albums_empty "
            f"{counts.albums_empty} exceeds {len(report.albums)} listed "
            "albums — these two display classes are mutually exclusive"
        )
    if counts.albums_diverging > len(report.albums):
        raise AssertionError(
            f"albums_diverging {counts.albums_diverging} exceeds "
            f"{len(report.albums)} listed albums"
        )
    if counts.albums_file_tag_present_db_absent > len(report.albums):
        raise AssertionError(
            "albums_file_tag_present_db_absent "
            f"{counts.albums_file_tag_present_db_absent} exceeds "
            f"{len(report.albums)} listed albums"
        )
    has_independent_divergence = bool(
        counts.albums_diverging or counts.albums_file_tag_present_db_absent,
    )
    if (report.status == "divergence_found") != has_independent_divergence:
        raise AssertionError(
            f"status {report.status!r} disagrees with the independent "
            f"divergence counts (albums_diverging={counts.albums_diverging}, "
            "albums_file_tag_present_db_absent="
            f"{counts.albums_file_tag_present_db_absent})"
        )
    if (report.status == "clean") != (not report.albums):
        raise AssertionError(
            f"status {report.status!r} disagrees with listed albums "
            f"{[a.album_id for a in report.albums]!r}"
        )
    if counts.items_unreadable > counts.items_read:
        raise AssertionError(
            f"items_unreadable {counts.items_unreadable} exceeds "
            f"items_read {counts.items_read}"
        )


#: Worst-first precedence, hand-written independently of
#: ``lib.retag_divergence_audit._ALBUM_CLASS_PRECEDENCE`` — #1093 review
#: finding 1 (M1). Deliberately duplicated rather than imported: importing
#: the production tuple here would let a mutant that reorders it pass this
#: check too, which is exactly the "agree by construction" shape the
#: review rejected.
def _expected_album_class_from_items(
    expected_item_classes: Sequence[str],
) -> str:
    if not expected_item_classes:
        return "empty"
    if "unreadable" in expected_item_classes:
        return "unreadable"
    if "diverges" in expected_item_classes:
        return "diverges"
    if "file_tag_present_db_absent" in expected_item_classes:
        return "file_tag_present_db_absent"
    return "agrees"


def check_world_construction_is_honored(
    report: RetagDivergenceReport,
    expectations: dict[int, list[str]],
    *,
    total_rows: int,
    total_items: int,
) -> None:
    """V4."""
    if report.counts.albums_scanned != total_rows:
        raise AssertionError(
            f"albums_scanned {report.counts.albums_scanned} does not "
            f"match the {total_rows} rows fed to the scan"
        )
    if report.counts.items_read != total_items:
        raise AssertionError(
            f"items_read {report.counts.items_read} does not match the "
            f"{total_items} item paths fed to the scan"
        )
    listed: dict[int, RetagDivergenceAlbum] = {
        album.album_id: album for album in report.albums
    }
    for album_id, expected_classes in expectations.items():
        expected_album_class = _expected_album_class_from_items(
            expected_classes,
        )
        if expected_album_class == "empty":
            album = listed.get(album_id)
            if album is None or album.album_class != "empty":
                raise AssertionError(
                    f"album {album_id} has zero items but is not listed "
                    "as the 'empty' class"
                )
            continue
        if expected_album_class == "agrees":
            if album_id in listed:
                raise AssertionError(
                    f"album {album_id} was constructed to agree on every "
                    f"item but was listed as "
                    f"{listed[album_id].album_class!r}"
                )
            continue
        album = listed.get(album_id)
        if album is None:
            raise AssertionError(
                f"album {album_id} was constructed with a non-agreeing "
                "item but never appears in the report"
            )
        actual_classes = [item.item_class for item in album.items]
        if actual_classes != expected_classes:
            raise AssertionError(
                f"album {album_id} items classified {actual_classes!r}, "
                f"expected {expected_classes!r} from its construction"
            )
        if album.album_class != expected_album_class:
            raise AssertionError(
                f"album {album_id} reports display class "
                f"{album.album_class!r}, but its independently-judged "
                f"precedence expects {expected_album_class!r} "
                f"(items: {expected_classes!r})"
            )


# ---------------------------------------------------------------------------
# Strategies over the world space — no plausibility filters
# ---------------------------------------------------------------------------

#: What the ALBUM row's own DB identity looks like.
DB_STATES = st.sampled_from(["survivor", "merged", "absent"])

#: What one installed file's tag looks like relative to its album's DB
#: identity: written to match it, written to a DIFFERENT known identity,
#: left BLANK (never written — the shape a real ``-W``-skipped file has),
#: or unreadable outright.
ITEM_DESIGNS = st.sampled_from(["match", "mismatch_known", "blank", "unreadable"])

#: One album: its DB state, and 0-4 items built from those designs.
ALBUM_STRATEGY = st.tuples(
    DB_STATES,
    st.lists(ITEM_DESIGNS, min_size=0, max_size=4),
)

#: 0-4 albums make up one world.
WORLD_STRATEGY = st.lists(ALBUM_STRATEGY, min_size=0, max_size=4)

_DB_VALUES = {"survivor": SURVIVOR, "merged": MERGED, "absent": ""}


def _build_world(
    albums: Sequence[tuple[str, list[str]]],
) -> tuple[
    list[BeetsAlbumIdentityRow],
    Callable[[str], str],
    dict[int, list[str]],
]:
    """Turn a generated world into scan inputs plus the expected per-item
    classification for every non-agreeing album (see V4)."""
    rows: list[BeetsAlbumIdentityRow] = []
    read_map: dict[str, str | Exception] = {}
    expectations: dict[int, list[str]] = {}

    for album_index, (db_state, designs) in enumerate(albums):
        album_id = album_index + 1
        db_value = _DB_VALUES[db_state]
        item_paths: list[str] = []
        expected_classes: list[str] = []
        for item_index, design in enumerate(designs):
            path = f"/library/album-{album_id}/item-{item_index}.mp3"
            item_paths.append(path)
            if design == "unreadable":
                read_map[path] = OSError("planted unreadable")
                expected_classes.append("unreadable")
                continue
            if design == "match":
                read_map[path] = db_value
                expected_classes.append("agrees")
                continue
            if design == "blank":
                # The file's tag was never written — exactly what a real
                # ``-W``-skipped write leaves behind on a fresh file.
                read_map[path] = ""
                expected_classes.append("diverges" if db_value else "agrees")
                continue
            # mismatch_known: a DIFFERENT known-nonempty identity than the
            # album's own DB value, whatever that value is.
            other = MERGED if db_value != MERGED else SURVIVOR
            read_map[path] = other
            expected_classes.append(
                "diverges" if db_value else "file_tag_present_db_absent",
            )
        rows.append(BeetsAlbumIdentityRow(
            album_id=album_id, mb_albumid=db_value,
            item_paths=tuple(item_paths),
        ))
        expectations[album_id] = expected_classes

    return rows, _read_tag_from_map(read_map), expectations


class TestRetagDivergenceProperties(unittest.TestCase):
    """V1-V4 over every world, driving the real ``scan_retag_divergence``."""

    @settings(deadline=None)
    @given(albums=WORLD_STRATEGY)
    @example(albums=[])
    @example(albums=[("survivor", [])])
    @example(albums=[("survivor", ["match", "match"])])
    # The exact #1093 item 1 shape: DB moved to the survivor, every file
    # still names the merged-away id.
    @example(albums=[("survivor", ["mismatch_known", "mismatch_known"])])
    @example(albums=[("absent", ["mismatch_known"])])
    @example(albums=[("survivor", ["unreadable"])])
    # M1: unreadable outranking diverges in display precedence, over a
    # world where BOTH are present on one album.
    @example(albums=[("survivor", ["unreadable", "mismatch_known"])])
    # M2: a blank (never-written) tag against a present DB identity.
    @example(albums=[("survivor", ["blank"])])
    def test_every_world_upholds_the_census_invariants(
        self, albums: list[tuple[str, list[str]]],
    ) -> None:
        rows, read_tag, expectations = _build_world(albums)
        total_items = sum(len(designs) for _state, designs in albums)
        beets = FakeBeetsDB()
        beets.set_album_mb_identities(rows)

        report = scan_retag_divergence(beets, read_tag=read_tag)

        check_fail_closed_unreadable_never_carries_a_tag(report)
        check_only_nonagreeing_albums_are_listed(report)
        check_counts_are_internally_consistent(report)
        check_world_construction_is_honored(
            report, expectations,
            total_rows=len(rows), total_items=total_items,
        )


def _make_item(
    item_class: RetagDivergenceItemClass, *, tag: str | None = "x",
) -> RetagDivergenceItem:
    return RetagDivergenceItem(
        path="/x.mp3", item_class=item_class,
        file_mb_albumid=tag, detail=(None if tag is not None else "boom"),
    )


def _make_album(
    *,
    album_id: int = 1,
    album_class: RetagDivergenceAlbumClass = "diverges",
    items: tuple[RetagDivergenceItem, ...] = (_make_item("diverges"),),
) -> RetagDivergenceAlbum:
    return RetagDivergenceAlbum(
        album_id=album_id, db_mb_albumid=SURVIVOR, album_class=album_class,
        item_count=len(items), items=items,
    )


#: Module-level singleton default — Ruff (B008) forbids a function call
#: directly in a parameter default.
_EMPTY_COUNTS = RetagDivergenceCounts(0, 0, 0, 0, 0, 0, 0)


def _make_report(
    *,
    status: Literal[
        "clean", "divergence_found", "incomplete", "beets_unavailable",
    ] = "clean",
    counts: RetagDivergenceCounts = _EMPTY_COUNTS,
    albums: tuple[RetagDivergenceAlbum, ...] = (),
) -> RetagDivergenceReport:
    return RetagDivergenceReport(
        status=status, complete=True, counts=counts, albums=albums,
    )


class TestInvariantCheckersTripOnViolations(unittest.TestCase):
    """Every checker clause owes a planted violation proving it can fail."""

    _item = staticmethod(_make_item)
    _album = staticmethod(_make_album)
    _report = staticmethod(_make_report)

    # V1
    def test_v1_unreadable_item_carrying_a_tag_is_rejected(self) -> None:
        report = self._report(
            status="divergence_found",
            counts=RetagDivergenceCounts(1, 1, 1, 0, 0, 1, 0),
            albums=(self._album(
                album_class="unreadable",
                items=(self._item("unreadable", tag="leaked"),),
            ),),
        )
        with self.assertRaisesRegex(
            AssertionError, "carries tag value",
        ):
            check_fail_closed_unreadable_never_carries_a_tag(report)

    def test_v1_readable_item_missing_a_tag_is_rejected(self) -> None:
        report = self._report(
            status="divergence_found",
            counts=RetagDivergenceCounts(1, 1, 0, 1, 0, 0, 0),
            albums=(self._album(items=(self._item("diverges", tag=None),)),),
        )
        with self.assertRaisesRegex(
            AssertionError, "carries no tag value",
        ):
            check_fail_closed_unreadable_never_carries_a_tag(report)

    # V2
    def test_v2_an_agreeing_album_listed_is_rejected(self) -> None:
        report = self._report(
            status="divergence_found",
            counts=RetagDivergenceCounts(1, 1, 0, 0, 0, 0, 0),
            albums=(self._album(album_class="agrees"),),
        )
        with self.assertRaisesRegex(
            AssertionError, "listed with class 'agrees'",
        ):
            check_only_nonagreeing_albums_are_listed(report)

    # V3
    def test_v3_unreadable_plus_empty_exceeding_listed_is_rejected(self) -> None:
        report = self._report(
            status="divergence_found",
            counts=RetagDivergenceCounts(1, 1, 1, 1, 0, 1, 1),
            albums=(self._album(),),
        )
        with self.assertRaisesRegex(
            AssertionError, "albums_unreadable .* albums_empty .* exceeds",
        ):
            check_counts_are_internally_consistent(report)

    def test_v3_albums_diverging_exceeding_listed_is_rejected(self) -> None:
        report = self._report(
            status="divergence_found",
            counts=RetagDivergenceCounts(1, 1, 0, 2, 0, 0, 0),
            albums=(self._album(),),
        )
        with self.assertRaisesRegex(
            AssertionError, "albums_diverging .* exceeds",
        ):
            check_counts_are_internally_consistent(report)

    def test_v3_albums_absent_exceeding_listed_is_rejected(self) -> None:
        report = self._report(
            status="divergence_found",
            counts=RetagDivergenceCounts(1, 1, 0, 0, 2, 0, 0),
            albums=(self._album(album_class="file_tag_present_db_absent"),),
        )
        with self.assertRaisesRegex(
            AssertionError, "albums_file_tag_present_db_absent .* exceeds",
        ):
            check_counts_are_internally_consistent(report)

    def test_v3_status_disagreeing_with_independent_counts_is_rejected(
        self,
    ) -> None:
        # albums_diverging says "found", status says otherwise.
        report = self._report(
            status="incomplete",
            counts=RetagDivergenceCounts(1, 1, 0, 1, 0, 0, 0),
            albums=(self._album(),),
        )
        with self.assertRaisesRegex(
            AssertionError, "disagrees with the independent divergence counts",
        ):
            check_counts_are_internally_consistent(report)

    def test_v3_status_disagreeing_with_albums_is_rejected(self) -> None:
        report = self._report(
            status="clean",
            counts=RetagDivergenceCounts(1, 1, 0, 0, 0, 1, 0),
            albums=(self._album(album_class="unreadable"),),
        )
        with self.assertRaisesRegex(
            AssertionError, "disagrees with listed albums",
        ):
            check_counts_are_internally_consistent(report)

    def test_v3_unreadable_exceeding_items_read_is_rejected(self) -> None:
        report = self._report(
            status="clean",
            counts=RetagDivergenceCounts(1, 1, 2, 0, 0, 0, 0),
            albums=(),
        )
        with self.assertRaisesRegex(
            AssertionError, "items_unreadable .* exceeds items_read",
        ):
            check_counts_are_internally_consistent(report)

    # V4
    def test_v4_albums_scanned_mismatch_is_rejected(self) -> None:
        report = self._report()
        with self.assertRaisesRegex(
            AssertionError, "albums_scanned .* does not match",
        ):
            check_world_construction_is_honored(
                report, {}, total_rows=3, total_items=0,
            )

    def test_v4_items_read_mismatch_is_rejected(self) -> None:
        report = self._report()
        with self.assertRaisesRegex(
            AssertionError, "items_read .* does not match",
        ):
            check_world_construction_is_honored(
                report, {}, total_rows=0, total_items=3,
            )

    def test_v4_zero_item_album_not_listed_empty_is_rejected(self) -> None:
        report = self._report()
        with self.assertRaisesRegex(
            AssertionError, "not listed as the 'empty' class",
        ):
            check_world_construction_is_honored(
                report, {7: []}, total_rows=0, total_items=0,
            )

    def test_v4_all_agreeing_album_incorrectly_listed_is_rejected(self) -> None:
        report = self._report(
            status="divergence_found",
            counts=RetagDivergenceCounts(1, 1, 0, 1, 0, 0, 0),
            albums=(self._album(album_id=7),),
        )
        with self.assertRaisesRegex(
            AssertionError, "constructed to agree on every item",
        ):
            check_world_construction_is_honored(
                report, {7: ["agrees"]}, total_rows=1, total_items=1,
            )

    def test_v4_item_classes_mismatch_is_rejected(self) -> None:
        report = self._report(
            status="divergence_found",
            counts=RetagDivergenceCounts(1, 1, 0, 1, 0, 0, 0),
            albums=(self._album(album_id=7),),
        )
        with self.assertRaisesRegex(
            AssertionError, "items classified",
        ):
            check_world_construction_is_honored(
                report, {7: ["file_tag_present_db_absent"]},
                total_rows=1, total_items=1,
            )

    def test_v4_display_class_disagreeing_with_independent_precedence_is_rejected(
        self,
    ) -> None:
        """#1093 review finding 1 (M1) — the exact regression this clause
        exists to catch: an album whose items imply ``unreadable`` by
        precedence, but whose reported ``album_class`` claims otherwise."""
        report = self._report(
            status="divergence_found",
            counts=RetagDivergenceCounts(1, 2, 1, 1, 0, 0, 0),
            albums=(self._album(
                album_id=7,
                album_class="diverges",  # WRONG — unreadable outranks it
                items=(
                    self._item("unreadable", tag=None),
                    self._item("diverges"),
                ),
            ),),
        )
        with self.assertRaisesRegex(
            AssertionError, "independently-judged precedence expects",
        ):
            check_world_construction_is_honored(
                report, {7: ["unreadable", "diverges"]},
                total_rows=1, total_items=2,
            )


if __name__ == "__main__":
    unittest.main()
