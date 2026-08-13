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
    independent counts is nonzero; ``items_unreadable`` never exceeds
    ``items_scanned``; ``items_refused`` never exceeds ``items_unreadable``.
    ``status == "clean"`` iff THREE conditions hold together — nothing
    listed, ``complete`` is ``True``, AND ``after_album_id`` (the cursor
    this call was GIVEN, not ``next_after_album_id``) is ``None`` — a
    resumed or truncated call is never allowed to claim the whole-library
    verdict it did not compute (#1093 review round 5, finding 1).
V4  The scan's counts and per-item classification honour exactly what the
    world was constructed to be: every row is scanned, every item path is
    scanned (whether read or refused), every refused path is counted in
    ``items_refused``, a zero-item album is always listed ``"empty"``, an
    all-agreeing album is never listed, any other album's items classify
    exactly as its construction intended (in production's read-items-then-
    refused-items order), AND the album's DISPLAY class (``album_class``)
    matches a precedence judgment computed INDEPENDENTLY of
    ``lib.retag_divergence_audit``'s own
    ``album_class_from_items``/``_ALBUM_CLASS_PRECEDENCE`` — #1093 review
    round 2, finding 1 (M1): the previous version of this check compared
    the report against the SAME production function that built it, so a
    mutant reordering the real precedence tuple still passed every test.
V5  A cursor-resumed chain (``TestChainedResumeProperties``) visits every
    album a single unbounded census finds, EXACTLY once, with matching
    classification — the resume mechanism itself neither skips nor
    repeats an album, and pagination changes nothing about what the
    census finds (#1093 review round 5, finding 2).

#1093 review round 2, finding 1 (M2): ``ITEM_DESIGNS`` previously had no
"the DB names an identity but the file tag is BLANK" design — exactly what
a file ``-W`` never wrote a tag to looks like — so a mutant collapsing that
branch to "agrees" survived. ``blank`` closes that gap.

#1093 review round 3, finding 2: ``ITEM_DESIGNS`` also had no "path refused
by containment" design, so ``_build_album``'s ``refused_items`` branch
(``lib/beets_db.py``/``lib/retag_divergence_audit.py``) was unreachable by
this property — a mutant dropping refused items entirely (``items =
read_items``, silently losing exactly the out-of-root files finding 7
exists to surface) survived the shipped property, survived with every
``@example`` removed, and survived ``fuzz`` at 2000 examples; only the
deterministic pin in ``tests/test_retag_divergence_audit.py`` caught it.
``refused`` below closes that gap.

#1093 review round 5, finding 2: the round-4 resume cursor
(``after_album_id``/``next_after_album_id``) shipped with deterministic
pins ONLY — no property exercised it at all, so five planted cursor
mutants (filter ``>``->``>=``, the cursor silently ignored, ``next_after_
album_id`` always ``None``, ``next_after_album_id = last + 1`` skipping an
album, ``next_after_album_id = last - 1`` repeating one) all survived.
``TestChainedResumeProperties`` closes that gap for the CURSOR MECHANISM.
It does NOT, on its own, close finding 1's Q2 gap: every album in its
strategy is constructed to guarantee a genuine divergence (so per-page
coverage is observable via ``report.albums``), which means
``has_divergence`` is always ``True`` there and a reverted finding-1 fix
(the ``or after_album_id is not None`` clause below V3) is never even
reached — a planted mutant proved this survives that property outright.
``TestRetagDivergenceProperties.test_a_cursor_that_filters_nothing_still_
forbids_clean`` closes THAT gap separately, using ``after_album_id=0``
(smaller than every real album id, so it filters out nothing — see
``_LIBRARY_START_CURSOR``) to isolate "a cursor is set at all" from
"the cursor changed what was scanned".
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
    # ``clean`` is gated on THREE conditions, not just "nothing listed"
    # (#1093 review round 5, finding 1): nothing listed, the scan ran to
    # completion, and it started from the TRUE beginning (no resume
    # cursor). A response claiming ``clean`` while any of those three is
    # false is exactly the defect the round-5 review found — a resumed or
    # truncated call cannot vouch for the whole library.
    if report.status == "clean":
        if report.albums:
            raise AssertionError(
                f"status 'clean' but albums "
                f"{[a.album_id for a in report.albums]!r} are listed"
            )
        if not report.complete:
            raise AssertionError(
                "status 'clean' but report.complete is False — a "
                "truncated scan can never be clean"
            )
        if report.after_album_id is not None:
            raise AssertionError(
                f"status 'clean' but after_album_id={report.after_album_id!r}"
                " — a resumed call can never claim clean for the whole "
                "library (#1093 review round 5, finding 1)"
            )
    elif (
        not has_independent_divergence
        and not report.albums
        and report.complete
        and report.after_album_id is None
    ):
        raise AssertionError(
            f"status {report.status!r} should be 'clean' — nothing "
            "listed, no divergence, complete, and started from the true "
            "beginning"
        )
    if counts.items_unreadable > counts.items_scanned:
        raise AssertionError(
            f"items_unreadable {counts.items_unreadable} exceeds "
            f"items_scanned {counts.items_scanned}"
        )
    if counts.items_refused > counts.items_unreadable:
        raise AssertionError(
            f"items_refused {counts.items_refused} exceeds "
            f"items_unreadable {counts.items_unreadable} — every refused "
            "path is classified unreadable, a subset relationship"
        )


#: Worst-first precedence, hand-written independently of
#: ``lib.retag_divergence_audit._ALBUM_CLASS_PRECEDENCE`` — #1093 review
#: round 2, finding 1 (M1). Deliberately duplicated rather than imported:
#: importing the production tuple here would let a mutant that reorders it
#: pass this check too, which is exactly the "agree by construction" shape
#: the review rejected.
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
    total_refused: int,
) -> None:
    """V4."""
    if report.counts.albums_scanned != total_rows:
        raise AssertionError(
            f"albums_scanned {report.counts.albums_scanned} does not "
            f"match the {total_rows} rows fed to the scan"
        )
    if report.counts.items_scanned != total_items:
        raise AssertionError(
            f"items_scanned {report.counts.items_scanned} does not match "
            f"the {total_items} item paths fed to the scan"
        )
    if report.counts.items_refused != total_refused:
        raise AssertionError(
            f"items_refused {report.counts.items_refused} does not match "
            f"the {total_refused} refused paths fed to the scan"
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


def check_chained_walk_matches_unbounded_census(
    visited: Sequence[RetagDivergenceAlbum],
    baseline: dict[int, RetagDivergenceAlbum],
    *,
    terminated: bool,
) -> None:
    """V5. A cursor-resumed chain must visit exactly the albums a single
    unbounded census finds, each EXACTLY ONCE, with matching
    classification — proving the resume cursor neither skips nor repeats
    an album, and that pagination changes nothing about what the census
    finds (#1093 review round 5, finding 2)."""
    if not terminated:
        raise AssertionError(
            "chained walk did not terminate within the expected page bound"
        )
    seen_ids = [album.album_id for album in visited]
    if len(seen_ids) != len(set(seen_ids)):
        duplicates = sorted({i for i in seen_ids if seen_ids.count(i) > 1})
        raise AssertionError(
            f"chained walk visited album(s) {duplicates} more than once"
        )
    if set(seen_ids) != set(baseline):
        missing = set(baseline) - set(seen_ids)
        extra = set(seen_ids) - set(baseline)
        raise AssertionError(
            f"chained walk coverage {sorted(seen_ids)} does not match the "
            f"unbounded census {sorted(baseline)} "
            f"(missing={sorted(missing)}, extra={sorted(extra)})"
        )
    for album in visited:
        expected_class = baseline[album.album_id].album_class
        if album.album_class != expected_class:
            raise AssertionError(
                f"album {album.album_id} classified {album.album_class!r} "
                f"by the chain but {expected_class!r} by the unbounded "
                "census"
            )


# ---------------------------------------------------------------------------
# Strategies over the world space — no plausibility filters
# ---------------------------------------------------------------------------

#: What the ALBUM row's own DB identity looks like.
DB_STATES = st.sampled_from(["survivor", "merged", "absent"])

#: What one installed file's tag looks like relative to its album's DB
#: identity: written to match it, written to a DIFFERENT known identity,
#: left BLANK (never written — the shape a real ``-W``-skipped file has),
#: unreadable outright, or REFUSED by path containment before any read is
#: attempted (#1093 review round 3, finding 2 — the out-of-root shape
#: finding 7 exists to catch; distinct from "unreadable" because it never
#: reaches ``read_tag`` at all, exercising ``BeetsAlbumIdentityRow.
#: refused_paths`` rather than ``item_paths``).
ITEM_DESIGNS = st.sampled_from(
    ["match", "mismatch_known", "blank", "unreadable", "refused"],
)

#: One album: its DB state, and 0-4 items built from those designs.
ALBUM_STRATEGY = st.tuples(
    DB_STATES,
    st.lists(ITEM_DESIGNS, min_size=0, max_size=4),
)

#: 0-4 albums make up one world.
WORLD_STRATEGY = st.lists(ALBUM_STRATEGY, min_size=0, max_size=4)

#: One album for the chained-resume property (V5): ALWAYS carries a
#: guaranteed-divergent ``mismatch_known`` item, plus 0-2 extra items of
#: any design, so every constructed album is certain to appear in
#: ``report.albums`` — the chained walk can then identify per-page
#: coverage purely from ``report.albums`` without needing a second,
#: separately-maintained id-tracking channel (#1093 review round 5,
#: finding 2).
_CURSOR_ALBUM_STRATEGY = st.tuples(
    DB_STATES,
    st.lists(ITEM_DESIGNS, min_size=0, max_size=2),
).map(lambda t: (t[0], ["mismatch_known", *t[1]]))

#: 1-6 guaranteed-divergent albums make up one chained-resume world.
CURSOR_WORLD_STRATEGY = st.lists(
    _CURSOR_ALBUM_STRATEGY, min_size=1, max_size=6,
)

_DB_VALUES = {"survivor": SURVIVOR, "merged": MERGED, "absent": ""}


def _build_world(
    albums: Sequence[tuple[str, list[str]]],
) -> tuple[
    list[BeetsAlbumIdentityRow],
    Callable[[str], str],
    dict[int, list[str]],
]:
    """Turn a generated world into scan inputs plus the expected per-item
    classification for every non-agreeing album (see V4).

    ``expectations[album_id]`` is built in PRODUCTION order — every read
    (non-refused) item first, in construction order, THEN every refused
    item — because ``lib.retag_divergence_audit._build_album`` concatenates
    ``read_items + refused_items`` regardless of how the two were
    interleaved in ``designs``.
    """
    rows: list[BeetsAlbumIdentityRow] = []
    read_map: dict[str, str | Exception] = {}
    expectations: dict[int, list[str]] = {}

    for album_index, (db_state, designs) in enumerate(albums):
        album_id = album_index + 1
        db_value = _DB_VALUES[db_state]
        item_paths: list[str] = []
        refused_paths: list[str] = []
        expected_read_classes: list[str] = []
        expected_refused_classes: list[str] = []
        for item_index, design in enumerate(designs):
            path = f"/library/album-{album_id}/item-{item_index}.mp3"
            if design == "refused":
                refused_paths.append(path)
                expected_refused_classes.append("unreadable")
                continue
            item_paths.append(path)
            if design == "unreadable":
                read_map[path] = OSError("planted unreadable")
                expected_read_classes.append("unreadable")
                continue
            if design == "match":
                read_map[path] = db_value
                expected_read_classes.append("agrees")
                continue
            if design == "blank":
                # The file's tag was never written — exactly what a real
                # ``-W``-skipped write leaves behind on a fresh file.
                read_map[path] = ""
                expected_read_classes.append(
                    "diverges" if db_value else "agrees")
                continue
            # mismatch_known: a DIFFERENT known-nonempty identity than the
            # album's own DB value, whatever that value is.
            other = MERGED if db_value != MERGED else SURVIVOR
            read_map[path] = other
            expected_read_classes.append(
                "diverges" if db_value else "file_tag_present_db_absent",
            )
        rows.append(BeetsAlbumIdentityRow(
            album_id=album_id, mb_albumid=db_value,
            item_paths=tuple(item_paths),
            refused_paths=tuple(refused_paths),
        ))
        expectations[album_id] = expected_read_classes + expected_refused_classes

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
    # Round-3 finding 2: a refused (out-of-root) path alone, and mixed
    # with an otherwise-agreeing item — proves refused items are neither
    # silently dropped nor counted as agreeing.
    @example(albums=[("survivor", ["refused"])])
    @example(albums=[("survivor", ["refused", "match"])])
    def test_every_world_upholds_the_census_invariants(
        self, albums: list[tuple[str, list[str]]],
    ) -> None:
        rows, read_tag, expectations = _build_world(albums)
        total_items = sum(len(designs) for _state, designs in albums)
        total_refused = sum(
            1
            for _state, designs in albums
            for design in designs
            if design == "refused"
        )
        beets = FakeBeetsDB()
        beets.set_album_mb_identities(rows)

        report = scan_retag_divergence(beets, read_tag=read_tag)

        check_fail_closed_unreadable_never_carries_a_tag(report)
        check_only_nonagreeing_albums_are_listed(report)
        check_counts_are_internally_consistent(report)
        check_world_construction_is_honored(
            report, expectations,
            total_rows=len(rows), total_items=total_items,
            total_refused=total_refused,
        )

    @settings(deadline=None)
    @given(albums=WORLD_STRATEGY)
    @example(albums=[])
    @example(albums=[("survivor", ["match"])])
    def test_a_cursor_that_filters_nothing_still_forbids_clean(
        self, albums: list[tuple[str, list[str]]],
    ) -> None:
        """#1093 review round 5, finding 1, Q2 proof — ``TestChainedResume
        Properties`` below forces every album to diverge (to make per-page
        coverage observable via ``report.albums``), which means
        ``has_divergence`` is always ``True`` there and the mutated
        ``elif`` branch this clause lives in is never even reached: that
        property alone does NOT kill a reverted N1 fix. This property
        closes that gap directly: ``after_album_id=0`` is smaller than
        every real album id (see ``_LIBRARY_START_CURSOR``), so it filters
        out NOTHING — the scanned albums and their classification are
        IDENTICAL to the unfiltered call — yet a resumed call must still
        never report ``clean``, proving the gating is on the cursor being
        set at all, not on anything it changes about the scan."""
        rows, read_tag, _expectations = _build_world(albums)
        beets = FakeBeetsDB()
        beets.set_album_mb_identities(rows)

        unfiltered = scan_retag_divergence(beets, read_tag=read_tag)
        resumed = scan_retag_divergence(
            beets, read_tag=read_tag, after_album_id=0,
        )

        check_counts_are_internally_consistent(unfiltered)
        check_counts_are_internally_consistent(resumed)
        if unfiltered.status == "clean" and resumed.status != "incomplete":
            raise AssertionError(
                f"an unfiltered scan over this world is 'clean', but the "
                f"identically-scanned resumed call reports "
                f"{resumed.status!r} instead of 'incomplete'"
            )


def _one_album_budget_clock() -> Callable[[], float]:
    """A FRESH clock per call (no state shared across calls in a chain, so
    alignment can never drift): the first call establishes the deadline,
    the second (first per-album check) reads as still within budget, the
    third (second per-album check, only reached if a second row remains)
    reads as expired — exactly one album's worth of budget per call."""
    calls = {"n": 0}

    def time_fn() -> float:
        calls["n"] += 1
        return 0.0 if calls["n"] <= 2 else 100.0

    return time_fn


class TestChainedResumeProperties(unittest.TestCase):
    """#1093 review round 5, finding 2 — the pin+property PAIR for the
    resume cursor itself (round 4 shipped the cursor with pins only). Walks
    the REAL ``scan_retag_divergence`` one album per page via
    ``after_album_id``/``next_after_album_id`` and proves the chain
    reconstructs exactly the same census a single unbounded call finds."""

    @settings(deadline=None)
    @given(albums=CURSOR_WORLD_STRATEGY)
    @example(albums=[("survivor", [])])
    @example(albums=[("survivor", []), ("merged", [])])
    @example(albums=[("survivor", []), ("merged", []), ("absent", [])])
    def test_chained_walk_matches_the_unbounded_census(
        self, albums: list[tuple[str, list[str]]],
    ) -> None:
        rows, read_tag, _expectations = _build_world(albums)
        beets = FakeBeetsDB()
        beets.set_album_mb_identities(rows)

        baseline = scan_retag_divergence(beets, read_tag=read_tag)
        baseline_by_id = {album.album_id: album for album in baseline.albums}

        visited: list[RetagDivergenceAlbum] = []
        after_album_id: int | None = None
        terminated = False
        for _ in range(len(rows) + 3):  # generous bound over page count
            report = scan_retag_divergence(
                beets, read_tag=read_tag,
                deadline_seconds=1.0, time_fn=_one_album_budget_clock(),
                after_album_id=after_album_id,
            )
            # Every page's own report must still satisfy every V1-V3
            # invariant, including the round-5 ``clean``-gating clauses —
            # this is what catches a REVERTED N1 fix: a resumed page
            # (``after_album_id is not None``) that wrongly reports
            # ``clean`` trips V3 directly.
            check_fail_closed_unreadable_never_carries_a_tag(report)
            check_only_nonagreeing_albums_are_listed(report)
            check_counts_are_internally_consistent(report)
            visited.extend(report.albums)
            if report.next_after_album_id is None:
                terminated = True
                break
            after_album_id = report.next_after_album_id

        check_chained_walk_matches_unbounded_census(
            visited, baseline_by_id, terminated=terminated,
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
_EMPTY_COUNTS = RetagDivergenceCounts(0, 0, 0, 0, 0, 0, 0, 0)


def _make_report(
    *,
    status: Literal[
        "clean", "divergence_found", "incomplete", "beets_unavailable",
    ] = "clean",
    counts: RetagDivergenceCounts = _EMPTY_COUNTS,
    albums: tuple[RetagDivergenceAlbum, ...] = (),
    complete: bool = True,
    after_album_id: int | None = None,
) -> RetagDivergenceReport:
    return RetagDivergenceReport(
        status=status, complete=complete, counts=counts, albums=albums,
        after_album_id=after_album_id,
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
            counts=RetagDivergenceCounts(1, 1, 0, 1, 0, 0, 1, 0),
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
            counts=RetagDivergenceCounts(1, 1, 0, 0, 1, 0, 0, 0),
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
            counts=RetagDivergenceCounts(1, 1, 0, 0, 0, 0, 0, 0),
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
            counts=RetagDivergenceCounts(1, 1, 0, 1, 1, 0, 1, 1),
            albums=(self._album(),),
        )
        with self.assertRaisesRegex(
            AssertionError, "albums_unreadable .* albums_empty .* exceeds",
        ):
            check_counts_are_internally_consistent(report)

    def test_v3_albums_diverging_exceeding_listed_is_rejected(self) -> None:
        report = self._report(
            status="divergence_found",
            counts=RetagDivergenceCounts(1, 1, 0, 0, 2, 0, 0, 0),
            albums=(self._album(),),
        )
        with self.assertRaisesRegex(
            AssertionError, "albums_diverging .* exceeds",
        ):
            check_counts_are_internally_consistent(report)

    def test_v3_albums_absent_exceeding_listed_is_rejected(self) -> None:
        report = self._report(
            status="divergence_found",
            counts=RetagDivergenceCounts(1, 1, 0, 0, 0, 2, 0, 0),
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
            counts=RetagDivergenceCounts(1, 1, 0, 0, 1, 0, 0, 0),
            albums=(self._album(),),
        )
        with self.assertRaisesRegex(
            AssertionError, "disagrees with the independent divergence counts",
        ):
            check_counts_are_internally_consistent(report)

    def test_v3_status_disagreeing_with_albums_is_rejected(self) -> None:
        report = self._report(
            status="clean",
            counts=RetagDivergenceCounts(1, 1, 0, 0, 0, 0, 1, 0),
            albums=(self._album(album_class="unreadable"),),
        )
        with self.assertRaisesRegex(
            AssertionError, "'clean' but albums .* are listed",
        ):
            check_counts_are_internally_consistent(report)

    def test_v3_clean_status_while_incomplete_is_rejected(self) -> None:
        """#1093 review round 5, finding 1 — a truncated scan (``complete
        =False``) can never claim ``clean``, even with nothing listed."""
        report = self._report(status="clean", complete=False)
        with self.assertRaisesRegex(
            AssertionError, "'clean' but report.complete is False",
        ):
            check_counts_are_internally_consistent(report)

    def test_v3_clean_status_with_a_cursor_set_is_rejected(self) -> None:
        """#1093 review round 5, finding 1 — the exact defect shape: a
        resumed call (``after_album_id is not None``) claiming ``clean``
        for a range it did not start scanning from the true beginning."""
        report = self._report(status="clean", after_album_id=5)
        with self.assertRaisesRegex(
            AssertionError, "'clean' but after_album_id=5",
        ):
            check_counts_are_internally_consistent(report)

    def test_v3_non_clean_status_that_should_have_been_clean_is_rejected(
        self,
    ) -> None:
        """The converse of the two clauses above: nothing listed, no
        divergence, complete, no cursor — this MUST be reported ``clean``;
        anything else under-reports a genuine clean census."""
        report = self._report(status="incomplete")
        with self.assertRaisesRegex(
            AssertionError, "should be 'clean'",
        ):
            check_counts_are_internally_consistent(report)

    def test_v3_unreadable_exceeding_items_scanned_is_rejected(self) -> None:
        report = self._report(
            status="clean",
            counts=RetagDivergenceCounts(1, 1, 0, 2, 0, 0, 0, 0),
            albums=(),
        )
        with self.assertRaisesRegex(
            AssertionError, "items_unreadable .* exceeds items_scanned",
        ):
            check_counts_are_internally_consistent(report)

    def test_v3_refused_exceeding_unreadable_is_rejected(self) -> None:
        """#1093 review round 4, finding 2 — ``items_refused`` is a subset
        of ``items_unreadable``; it can never exceed it."""
        report = self._report(
            status="clean",
            counts=RetagDivergenceCounts(1, 2, 2, 1, 0, 0, 0, 0),
            albums=(),
        )
        with self.assertRaisesRegex(
            AssertionError, "items_refused .* exceeds items_unreadable",
        ):
            check_counts_are_internally_consistent(report)

    # V4
    def test_v4_albums_scanned_mismatch_is_rejected(self) -> None:
        report = self._report()
        with self.assertRaisesRegex(
            AssertionError, "albums_scanned .* does not match",
        ):
            check_world_construction_is_honored(
                report, {}, total_rows=3, total_items=0, total_refused=0,
            )

    def test_v4_items_scanned_mismatch_is_rejected(self) -> None:
        report = self._report()
        with self.assertRaisesRegex(
            AssertionError, "items_scanned .* does not match",
        ):
            check_world_construction_is_honored(
                report, {}, total_rows=0, total_items=3, total_refused=0,
            )

    def test_v4_items_refused_mismatch_is_rejected(self) -> None:
        """#1093 review round 4, finding 2."""
        report = self._report()
        with self.assertRaisesRegex(
            AssertionError, "items_refused .* does not match",
        ):
            check_world_construction_is_honored(
                report, {}, total_rows=0, total_items=0, total_refused=3,
            )

    def test_v4_zero_item_album_not_listed_empty_is_rejected(self) -> None:
        report = self._report()
        with self.assertRaisesRegex(
            AssertionError, "not listed as the 'empty' class",
        ):
            check_world_construction_is_honored(
                report, {7: []}, total_rows=0, total_items=0, total_refused=0,
            )

    def test_v4_all_agreeing_album_incorrectly_listed_is_rejected(self) -> None:
        report = self._report(
            status="divergence_found",
            counts=RetagDivergenceCounts(1, 1, 0, 0, 1, 0, 0, 0),
            albums=(self._album(album_id=7),),
        )
        with self.assertRaisesRegex(
            AssertionError, "constructed to agree on every item",
        ):
            check_world_construction_is_honored(
                report, {7: ["agrees"]},
                total_rows=1, total_items=1, total_refused=0,
            )

    def test_v4_item_classes_mismatch_is_rejected(self) -> None:
        report = self._report(
            status="divergence_found",
            counts=RetagDivergenceCounts(1, 1, 0, 0, 1, 0, 0, 0),
            albums=(self._album(album_id=7),),
        )
        with self.assertRaisesRegex(
            AssertionError, "items classified",
        ):
            check_world_construction_is_honored(
                report, {7: ["file_tag_present_db_absent"]},
                total_rows=1, total_items=1, total_refused=0,
            )

    def test_v4_display_class_disagreeing_with_independent_precedence_is_rejected(
        self,
    ) -> None:
        """#1093 review round 2, finding 1 (M1) — the exact regression this
        clause exists to catch: an album whose items imply ``unreadable``
        by precedence, but whose reported ``album_class`` claims
        otherwise."""
        report = self._report(
            status="divergence_found",
            counts=RetagDivergenceCounts(1, 2, 0, 1, 1, 0, 0, 0),
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
                total_rows=1, total_items=2, total_refused=0,
            )

    # V5
    def test_v5_non_terminating_chain_is_rejected(self) -> None:
        with self.assertRaisesRegex(
            AssertionError, "did not terminate",
        ):
            check_chained_walk_matches_unbounded_census(
                [], {}, terminated=False,
            )

    def test_v5_repeated_album_is_rejected(self) -> None:
        album = self._album(album_id=7)
        with self.assertRaisesRegex(
            AssertionError, "visited album\\(s\\) \\[7\\] more than once",
        ):
            check_chained_walk_matches_unbounded_census(
                [album, album], {7: album}, terminated=True,
            )

    def test_v5_coverage_mismatch_is_rejected(self) -> None:
        visited_album = self._album(album_id=7)
        missing_album = self._album(album_id=8)
        with self.assertRaisesRegex(
            AssertionError, "does not match the unbounded census",
        ):
            check_chained_walk_matches_unbounded_census(
                [visited_album],
                {7: visited_album, 8: missing_album},
                terminated=True,
            )

    def test_v5_classification_mismatch_is_rejected(self) -> None:
        visited_album = self._album(album_id=7, album_class="diverges")
        baseline_album = self._album(album_id=7, album_class="unreadable")
        with self.assertRaisesRegex(
            AssertionError, "classified 'diverges' by the chain but "
            "'unreadable' by the unbounded census",
        ):
            check_chained_walk_matches_unbounded_census(
                [visited_album], {7: baseline_album}, terminated=True,
            )


if __name__ == "__main__":
    unittest.main()
