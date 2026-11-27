"""Generated properties for the other-pressing check (issue #1445).

Drive the real ``lib.other_pressing_check.check_other_pressings`` over a
fake download_log row and a fake mirror that serves PLANTED worlds, paging
the release group 25 at a time exactly as MusicBrainz does.

Worlds are planted, never recomputed as an oracle: every listed length,
file length and disc table of contents is a whole number of half seconds
(exact in binary, so a gap of exactly 2.0s is a real, reachable world), and
each part of the world remembers the role it was built to play.

**Pressings world.** A one- or two-disc target whose files either fit its
own listing (every gap within 2.0s, exactly 2.0 included) or miss it on one
track by 2.5 to 30s; the target's listed length sometimes lives only on the
recording. Up to 35 siblings, so the group spans two pages, each planted as
``same`` (the target's listing), ``fits`` (the files within 2.0s, some
titles renamed), ``off`` (one track 2.5 to 30s from its file), ``missing``
(one track with no length anywhere) or ``count`` (one track more or
fewer), sometimes split over two media, and sometimes listed again on a
later page.

**Disc world.** A one-disc target whose files fit its listing within 1s or
miss it on one track by 10 to 30s, with up to three disc IDs each planted
to fit the listing, fit the files, or fit neither (one track 40s out), or a
digital medium that cannot carry one.

Invariants: a copy that fits its own listing is never said to match
another pressing, and every sibling the files fit is then reported as
indistinguishable; otherwise the matches are exactly the planted ``fits``
siblings with exactly their renamed positions; a release listed twice is
counted once; the listing and the files are each judged against every
attached disc exactly as planted; and no sentence claims the listed
lengths were typed in by hand.
"""

from __future__ import annotations

import unittest
from typing import NamedTuple

import msgspec
from hypothesis import given
from hypothesis import strategies as st

import tests._hypothesis_profiles  # noqa: F401 - registers suite/fuzz
from lib.mb_api import MBDiscRelease, MBDiscReleasePage
from lib.other_pressing_check import (
    FilesAgainstDisc,
    MediumDiscStatus,
    PressingCheck,
    check_other_pressings,
)

ROLE_SAME = "same"
ROLE_FITS = "fits"
ROLE_OFF = "off"
ROLE_MISSING = "missing"
ROLE_COUNT = "count"
ROLE_BLANK = "blank"

DISC_LISTING = "listing"
DISC_FILES = "files"
DISC_NEITHER = "neither"

_TARGET_ID = "00000000-0000-0000-0000-00000000aaaa"
_GROUP_ID = "00000000-0000-0000-0000-00000000bbbb"
_FITS_OWN = "This copy fits this release's"
_DOES_NOT_FIT = "This copy does not fit this release's"

#: Half-second steps: a gap of exactly 2.0 is representable and drawn.
_within_two = st.integers(min_value=-4, max_value=4).map(lambda n: n * 0.5)
_off_by = st.integers(min_value=5, max_value=60).map(lambda n: n * 0.5)
_sign = st.sampled_from([-1.0, 1.0])


def _sibling_id(n: int) -> str:
    return f"00000000-0000-0000-0001-{n:012d}"


class PressingsWorld(NamedTuple):
    row: dict[str, object]
    group: list[dict[str, object]]
    fits_own: bool
    #: ids planted ``fits``, with their renamed (medium, position) pairs.
    fits: dict[str, list[tuple[int, int]]]
    #: ids planted ``same``.
    same: set[str]
    #: ids planted ``blank``: they fit the files only through recordings.
    blank: set[str]


class DiscWorld(NamedTuple):
    row: dict[str, object]
    group: list[dict[str, object]]
    fits_own: bool
    digital: bool
    kinds: list[str]
    #: The track planted with no listed length at all, if any.
    missing: tuple[int, int] | None = None
    #: The track the files were planted off, when they do not fit.
    off_key: tuple[int, int] | None = None


def _row(files: dict[tuple[int, int], float]) -> dict[str, object]:
    return {
        "id": 1, "outcome": "success",
        "validation_result": {"candidates": [{
            "album_id": _TARGET_ID, "is_target": True,
            "extra_items": [], "extra_tracks": [],
            "mapping": [
                {"item": {"length": seconds},
                 "track": {"medium": medium, "medium_index": position}}
                for (medium, position), seconds in files.items()
            ],
        }]},
    }


def _track(
    position: int, title: str, ms: int | None, *, on_recording: bool = False,
) -> dict[str, object]:
    if on_recording:
        return {"position": position, "title": title, "length": None,
                "recording": {"length": ms}}
    return {"position": position, "title": title, "length": ms,
            "recording": {"length": None}}


@st.composite
def pressings_worlds(draw: st.DrawFn) -> PressingsWorld:
    sizes = draw(st.lists(
        st.integers(min_value=1, max_value=4), min_size=1, max_size=2))
    keys = [(m, p) for m, size in enumerate(sizes, 1) for p in range(1, size + 1)]
    listed = {key: draw(st.integers(min_value=120, max_value=800)) * 0.5
              for key in keys}
    fits_own = draw(st.booleans())
    files = {key: listed[key] + draw(_within_two) for key in keys}
    if not fits_own:
        off_key = draw(st.sampled_from(keys))
        files[off_key] = listed[off_key] + draw(_sign) * draw(_off_by)
    titles = {key: f"Song {key[0]}-{key[1]}" for key in keys}
    target = {
        "id": _TARGET_ID, "title": "Album", "country": "GB", "date": "2004",
        "release-group": {"id": _GROUP_ID, "title": "Album"},
        "media": [
            {"position": m, "format": "CD", "discs": [], "tracks": [
                _track(p, titles[(m, p)], round(listed[(m, p)] * 1000),
                       on_recording=draw(st.booleans()))
                for p in range(1, size + 1)]}
            for m, size in enumerate(sizes, 1)
        ],
    }

    roles = draw(st.lists(
        st.sampled_from([ROLE_SAME, ROLE_FITS, ROLE_OFF, ROLE_MISSING,
                         ROLE_COUNT, ROLE_BLANK]),
        max_size=35))
    group: list[dict[str, object]] = [target]
    fits: dict[str, list[tuple[int, int]]] = {}
    same: set[str] = set()
    blank: set[str] = set()
    for n, role in enumerate(roles, 1):
        sibling_id = _sibling_id(n)
        names = [titles[key] for key in keys]
        if role == ROLE_SAME:
            lengths: list[float | None] = [listed[key] for key in keys]
            same.add(sibling_id)
        else:
            lengths = [files[key] + draw(_within_two) for key in keys]
        renamed: list[tuple[int, int]] = []
        if role == ROLE_FITS:
            for at, key in enumerate(keys):
                if draw(st.booleans()):
                    names[at] = f"Other {at}"
                    renamed.append(key)
            fits[sibling_id] = renamed
        elif role == ROLE_OFF:
            at = draw(st.integers(min_value=0, max_value=len(keys) - 1))
            lengths[at] = files[keys[at]] + draw(_sign) * draw(_off_by)
        elif role == ROLE_MISSING:
            at = draw(st.integers(min_value=0, max_value=len(keys) - 1))
            lengths[at] = None
        elif role == ROLE_COUNT:
            if len(lengths) > 1 and draw(st.booleans()):
                lengths, names = lengths[:-1], names[:-1]
            else:
                lengths, names = [*lengths, 200.0], [*names, "Bonus"]
        # ``blank``: the files' lengths, but some or all only on the
        # recordings, which every release using them shares.
        blank_at: set[int] = set()
        if role == ROLE_BLANK:
            blank.add(sibling_id)
            blank_at = set(draw(st.lists(
                st.integers(min_value=0, max_value=len(keys) - 1),
                min_size=1, unique=True)))
        tracks = [
            _track(i, name,
                   None if length is None else round(length * 1000),
                   on_recording=(i - 1) in blank_at)
            for i, (name, length) in enumerate(zip(names, lengths, strict=True), 1)
        ]
        split = (
            draw(st.integers(min_value=1, max_value=len(tracks) - 1))
            if len(tracks) > 1 and draw(st.booleans()) else len(tracks))
        media = [{"position": 1, "format": "CD", "discs": [],
                  "tracks": tracks[:split]}]
        if split < len(tracks):
            media.append({"position": 2, "format": "CD", "discs": [],
                          "tracks": [
                              {**t, "position": i}
                              for i, t in enumerate(tracks[split:], 1)]})
        group.append({"id": sibling_id, "title": "Album", "country": "US",
                      "date": "2005", "media": media})
    if len(group) > 1 and draw(st.booleans()):
        group.append(draw(st.sampled_from(group[1:])))
    return PressingsWorld(_row(files), group, fits_own, fits, same, blank)


def _toc(seconds: list[float], disc_id: str) -> dict[str, object]:
    offsets: list[int] = []
    cursor = 150
    for length in seconds:
        offsets.append(cursor)
        cursor += round(length * 75)
    return {"id": disc_id, "offset-count": len(seconds), "sectors": cursor,
            "offsets": offsets}


@st.composite
def disc_worlds(draw: st.DrawFn) -> DiscWorld:
    size = draw(st.integers(min_value=1, max_value=4))
    keys = [(1, p) for p in range(1, size + 1)]
    listed = {key: draw(st.integers(min_value=120, max_value=800)) * 0.5
              for key in keys}
    fits_own = draw(st.booleans())
    one_or_less = st.integers(min_value=-2, max_value=2).map(lambda n: n * 0.5)
    files = {key: listed[key] + draw(one_or_less) for key in keys}
    off_key: tuple[int, int] | None = None
    if not fits_own:
        off_key = draw(st.sampled_from(keys))
        files[off_key] = listed[off_key] + draw(_sign) * draw(
            st.integers(min_value=20, max_value=60).map(lambda n: n * 0.5))
    # A track the release lists no length for at all (not even on the
    # recording): unknown, never a mismatch.
    missing = draw(st.one_of(st.none(), st.sampled_from(keys)))
    digital = draw(st.booleans()) and draw(st.booleans())
    kinds = [] if digital else draw(st.lists(
        st.sampled_from([DISC_LISTING, DISC_FILES, DISC_NEITHER]),
        max_size=3))
    discs: list[dict[str, object]] = []
    for n, kind in enumerate(kinds, 1):
        half = st.integers(min_value=-1, max_value=1).map(lambda m: m * 0.5)
        if kind == DISC_LISTING:
            seconds = [listed[key] + draw(half) for key in keys]
        elif kind == DISC_FILES:
            seconds = [files[key] + draw(half) for key in keys]
        else:
            seconds = [listed[key] for key in keys]
            seconds[0] += 40.0
        discs.append(_toc(seconds, f"disc-{n}"))
    target = {
        "id": _TARGET_ID, "title": "Album", "country": "GB", "date": "2004",
        "release-group": {"id": _GROUP_ID, "title": "Album"},
        "media": [{
            "position": 1, "format": "Digital Media" if digital else "CD",
            "discs": discs,
            "tracks": [
                _track(p, f"Song {p}",
                       None if (1, p) == missing
                       else round(listed[(1, p)] * 1000))
                for _, p in keys]}],
    }
    return DiscWorld(
        _row(files), [target], fits_own, digital, kinds, missing, off_key)


class _Log:
    def __init__(self, row: dict[str, object]) -> None:
        self.row = row

    def get_download_log_entry(self, log_id: int) -> dict[str, object] | None:
        return self.row if log_id == 1 else None


class _Mirror:
    def __init__(self, group: list[dict[str, object]]) -> None:
        self.group = group

    def release(self, release_id: str, *, timeout: float) -> MBDiscRelease:
        return msgspec.convert(self.group[0], type=MBDiscRelease)

    def release_group_page(
        self, release_group_id: str, *, offset: int, limit: int,
        timeout: float,
    ) -> MBDiscReleasePage:
        return msgspec.convert(
            {"release-count": len(self.group),
             "releases": self.group[offset:offset + limit]},
            type=MBDiscReleasePage)


def run(row: dict[str, object], group: list[dict[str, object]]) -> PressingCheck:
    return check_other_pressings(
        1, pdb=_Log(row), mirror=_Mirror(group), clock=lambda: 0.0)


def assert_pressings_match_planted(
    world: PressingsWorld, result: PressingCheck,
) -> None:
    reported = [m.release_id for m in result.matching_pressings] + [
        m.release_id for m in result.same_listing_pressings]
    if len(reported) != len(set(reported)):
        raise AssertionError(f"a release was reported twice: {sorted(reported)}")
    if result.fits_own_listing is not world.fits_own:
        raise AssertionError(
            f"fits own listing {result.fits_own_listing} but planted "
            f"{world.fits_own}")
    matched = {
        m.release_id: [(d.medium, d.position) for d in m.title_differences]
        for m in result.matching_pressings}
    same = {m.release_id for m in result.same_listing_pressings}
    blank = {m.release_id for m in result.recording_only_pressings}
    if blank != world.blank:
        raise AssertionError(
            f"not comparable (blank lengths) {sorted(blank)} but planted "
            f"{sorted(world.blank)}")
    if world.fits_own:
        if matched:
            raise AssertionError(
                f"a copy fitting its own listing was said to match {sorted(matched)}")
        expected_same = world.same | set(world.fits)
        if same != expected_same:
            raise AssertionError(
                f"indistinguishable pressings {sorted(same)} but planted "
                f"{sorted(expected_same)}")
    else:
        if set(matched) != set(world.fits):
            raise AssertionError(
                f"matched pressings {sorted(matched)} but planted "
                f"{sorted(world.fits)}")
        for release_id, renamed in world.fits.items():
            if matched[release_id] != renamed:
                raise AssertionError(
                    f"title differences for {release_id} were "
                    f"{matched[release_id]} but planted {renamed}")
        if same:
            raise AssertionError(
                f"indistinguishable pressings {sorted(same)} reported for a "
                "copy that does not fit its own listing")
    lead = result.sentences[0] if result.sentences else ""
    expected_lead = _FITS_OWN if world.fits_own else _DOES_NOT_FIT
    if not lead.startswith(expected_lead):
        raise AssertionError(f"lead sentence {lead!r} for fits_own={world.fits_own}")
    if world.fits_own and any(
            s.startswith("This copy matches") for s in result.sentences):
        raise AssertionError("a 'matches' sentence for a copy fitting its own listing")


def assert_discs_match_planted(world: DiscWorld, result: PressingCheck) -> None:
    lengths = result.target_lengths
    if lengths is None:
        raise AssertionError("no target lengths")
    (medium,) = lengths.media
    if world.digital:
        expected_status = MediumDiscStatus.NO_DISC_FORMAT
    elif not world.kinds:
        expected_status = MediumDiscStatus.NO_DISC_ID
    else:
        expected_status = MediumDiscStatus.CHECKED
    if medium.status != expected_status:
        raise AssertionError(f"disc status {medium.status} but planted {expected_status}")
    listing_fits = world.missing is None and (
        DISC_LISTING in world.kinds
        or (world.fits_own and DISC_FILES in world.kinds))
    if (medium.listing_disc_id is not None) != listing_fits:
        raise AssertionError(
            f"listing disc {medium.listing_disc_id} but planted "
            f"listing_fits={listing_fits}")
    if not world.kinds:
        expected_against = None
    elif listing_fits and world.fits_own:
        expected_against = FilesAgainstDisc.LISTING_DISC
    elif DISC_FILES in world.kinds or (
            world.fits_own and DISC_LISTING in world.kinds):
        expected_against = FilesAgainstDisc.OTHER_DISC
    else:
        expected_against = FilesAgainstDisc.NO_DISC
    if medium.files_against_disc != expected_against:
        raise AssertionError(
            f"files against disc {medium.files_against_disc} but planted "
            f"{expected_against}")
    if any("typed in by hand" in s for s in result.sentences):
        raise AssertionError("a sentence claims the lengths were typed in by hand")

    def contradicts(kind: str) -> bool:
        """Does a disc of this planted kind contradict a LISTED length?"""
        if kind == DISC_NEITHER:
            return world.missing != (1, 1)
        if kind == DISC_FILES:
            return not world.fits_own and world.off_key != world.missing
        return False

    expected_wrong = bool(world.kinds) and not listing_fits and all(
        contradicts(kind) for kind in world.kinds)
    said_wrong = any("may be wrong" in s for s in result.sentences)
    if said_wrong != expected_wrong:
        raise AssertionError(
            f"'listing may be wrong' said={said_wrong} but planted "
            f"wrong={expected_wrong}")


class TestOtherPressingCheckGenerated(unittest.TestCase):
    @given(world=pressings_worlds())
    def test_pressings_match_planted_world(self, world: PressingsWorld) -> None:
        assert_pressings_match_planted(world, run(world.row, world.group))

    @given(world=disc_worlds())
    def test_discs_match_planted_world(self, world: DiscWorld) -> None:
        assert_discs_match_planted(world, run(world.row, world.group))


def _fits_own_world() -> PressingsWorld:
    """Two tracks the files fit exactly; one sibling planted ``same``."""
    target = {
        "id": _TARGET_ID, "title": "A", "country": "GB", "date": "2004",
        "release-group": {"id": _GROUP_ID, "title": "A"},
        "media": [{"position": 1, "format": "CD", "discs": [], "tracks": [
            _track(1, "One", 100_000), _track(2, "Two", 200_000)]}],
    }
    sibling = {"id": _sibling_id(1), "title": "A", "country": "US",
               "date": "2005", "media": [{"position": 1, "format": "CD",
                                          "discs": [], "tracks": [
                   _track(1, "One", 100_000), _track(2, "Two", 200_000)]}]}
    return PressingsWorld(
        _row({(1, 1): 100.0, (1, 2): 200.0}), [target, sibling], True, {},
        {_sibling_id(1)}, set())


def _disc_world() -> DiscWorld:
    """One track, one disc planted to fit the listing, files fit too."""
    target = {
        "id": _TARGET_ID, "title": "A", "country": "GB", "date": "2004",
        "release-group": {"id": _GROUP_ID, "title": "A"},
        "media": [{"position": 1, "format": "CD",
                   "discs": [_toc([100.0], "d")],
                   "tracks": [_track(1, "One", 100_000)]}],
    }
    return DiscWorld(_row({(1, 1): 100.0}), [target], True, False, [DISC_LISTING])


class TestCheckerTripsOnViolations(unittest.TestCase):
    """Known-bad self-tests, one per clause: the real result of a small
    world against a planting changed in one respect only, or a result
    changed in one field only."""

    def setUp(self) -> None:
        self.pw = _fits_own_world()
        self.pr = run(self.pw.row, self.pw.group)
        self.dw = _disc_world()
        self.dr = run(self.dw.row, self.dw.group)

    def test_the_small_worlds_pass(self) -> None:
        assert_pressings_match_planted(self.pw, self.pr)
        assert_discs_match_planted(self.dw, self.dr)

    def test_clause_reported_twice_trips(self) -> None:
        twice = self.pr.same_listing_pressings * 2
        wrong = msgspec.structs.replace(self.pr, same_listing_pressings=twice)
        with self.assertRaisesRegex(AssertionError, "reported twice"):
            assert_pressings_match_planted(self.pw, wrong)

    def test_clause_fits_own_trips(self) -> None:
        with self.assertRaisesRegex(AssertionError, "fits own listing"):
            assert_pressings_match_planted(
                self.pw._replace(fits_own=False), self.pr)

    def test_clause_said_to_match_while_fitting_own_trips(self) -> None:
        wrong = msgspec.structs.replace(
            self.pr, matching_pressings=self.pr.same_listing_pressings,
            same_listing_pressings=[])
        with self.assertRaisesRegex(AssertionError, "was said to match"):
            assert_pressings_match_planted(self.pw, wrong)

    def test_clause_indistinguishable_set_trips(self) -> None:
        with self.assertRaisesRegex(AssertionError, "indistinguishable pressings"):
            assert_pressings_match_planted(self.pw._replace(same=set()), self.pr)

    def test_clause_matched_set_trips(self) -> None:
        world = self.pw._replace(fits_own=False, fits={"x": []}, same=set())
        wrong = msgspec.structs.replace(
            self.pr, fits_own_listing=False, same_listing_pressings=[])
        with self.assertRaisesRegex(AssertionError, "matched pressings"):
            assert_pressings_match_planted(world, wrong)

    def test_clause_title_differences_trips(self) -> None:
        match = self.pr.same_listing_pressings[0]
        world = self.pw._replace(
            fits_own=False, fits={match.release_id: [(1, 1)]}, same=set())
        wrong = msgspec.structs.replace(
            self.pr, fits_own_listing=False, matching_pressings=[match],
            same_listing_pressings=[])
        with self.assertRaisesRegex(AssertionError, "title differences"):
            assert_pressings_match_planted(world, wrong)

    def test_clause_indistinguishable_when_not_fitting_trips(self) -> None:
        world = self.pw._replace(fits_own=False, same=set())
        wrong = msgspec.structs.replace(self.pr, fits_own_listing=False)
        with self.assertRaisesRegex(AssertionError, "does not fit its own"):
            assert_pressings_match_planted(world, wrong)

    def test_clause_lead_sentence_trips(self) -> None:
        wrong = msgspec.structs.replace(self.pr, sentences=["Something else."])
        with self.assertRaisesRegex(AssertionError, "lead sentence"):
            assert_pressings_match_planted(self.pw, wrong)

    def test_clause_matches_sentence_trips(self) -> None:
        wrong = msgspec.structs.replace(
            self.pr, sentences=[_FITS_OWN, "This copy matches another pressing."])
        with self.assertRaisesRegex(AssertionError, "a 'matches' sentence"):
            assert_pressings_match_planted(self.pw, wrong)

    def test_clause_disc_status_trips(self) -> None:
        with self.assertRaisesRegex(AssertionError, "disc status"):
            assert_discs_match_planted(self.dw._replace(kinds=[]), self.dr)

    def test_clause_listing_disc_trips(self) -> None:
        with self.assertRaisesRegex(AssertionError, "listing disc"):
            assert_discs_match_planted(
                self.dw._replace(kinds=[DISC_NEITHER]), self.dr)

    def test_clause_files_against_disc_trips(self) -> None:
        with self.assertRaisesRegex(AssertionError, "files against disc"):
            assert_discs_match_planted(
                self.dw._replace(fits_own=False), self.dr)

    def test_clause_blank_lengths_trips(self) -> None:
        with self.assertRaisesRegex(AssertionError, "not comparable"):
            assert_pressings_match_planted(
                self.pw._replace(blank={"x"}), self.pr)

    def test_clause_listing_may_be_wrong_trips(self) -> None:
        wrong = msgspec.structs.replace(
            self.dr, sentences=["The listing may be wrong."])
        with self.assertRaisesRegex(AssertionError, "may be wrong"):
            assert_discs_match_planted(self.dw, wrong)

    def test_clause_typed_by_hand_trips(self) -> None:
        wrong = msgspec.structs.replace(
            self.dr, sentences=["The listed lengths were typed in by hand."])
        with self.assertRaisesRegex(AssertionError, "typed in by hand"):
            assert_discs_match_planted(self.dw, wrong)


if __name__ == "__main__":
    unittest.main()
