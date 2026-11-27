"""The on-demand "matches another pressing" check (issue #1445).

Every MusicBrainz response here is a REAL capture from the mirror, trimmed
to the fields the check reads (``tests/fixtures/other_pressings/<id>.json``,
one file per download_log row, with that row's selected-candidate mapping
from the pipeline DB; typed in ``tests/other_pressings_fixtures.py``). The
fake mirror only serves those captures; it never invents a release. Worlds
the captures cannot show (an over-cap group, a failing mirror, identical
titles, competing disc IDs) are built by editing a capture or, for the
smallest disc-ID worlds, written out in the same wire shape, and each test
says what it is.
"""

from __future__ import annotations

import copy
import io
import json
import threading
import unittest
import urllib.error
from unittest.mock import patch

import msgspec

from lib import mb_api
from lib.mb_api import MBDiscRelease, MBDiscReleasePage
from lib.other_pressing_check import (
    PER_REQUEST_TIMEOUT_SECONDS,
    RELEASE_GROUP_CAP,
    RELEASE_GROUP_PAGE_SIZE,
    TOTAL_BUDGET_SECONDS,
    FilesAgainstDisc,
    MediumDiscStatus,
    PressingCheck,
    PressingCheckOutcome,
    _normalize_title,
    check_other_pressings,
)
from tests.other_pressings_fixtures import (
    BOB,
    DEER_TICK,
    FAIRWAYS,
    KURT_VILE,
    RAZORLIGHT,
    DiscJSON,
    FixtureJSON,
    PairJSON,
    ReleaseJSON,
    TrackJSON,
    load_fixture,
    not_found,
    releases,
    validation_result,
)


def _fixture(log_id: int) -> FixtureJSON:
    return load_fixture(log_id)


def _releases(fixture: FixtureJSON) -> list[ReleaseJSON]:
    return releases(fixture)


def _log_row(
    fixture: FixtureJSON, *, album_id: str | None = None,
    outcome: str = "success",
) -> dict[str, object]:
    """The download_log row the DB returns, in the persisted wire shape."""
    return {
        "id": fixture["download_log_id"],
        "outcome": outcome,
        "validation_result": validation_result(fixture, album_id=album_id),
    }


def _listed_ms(track: TrackJSON) -> int | None:
    """The listed length as the check reads it: the track's, else its
    recording's."""
    if track["length"]:
        return track["length"]
    recording = track.get("recording")
    return recording["length"] if recording else None


def _set_files_to_listing(fixture: FixtureJSON) -> None:
    """Every mapped file exactly the target's listed length."""
    listed = {
        (medium["position"], track["position"] or index): _listed_ms(track)
        for medium in fixture["target"]["media"]
        for index, track in enumerate(medium["tracks"], 1)
    }
    for pair in fixture["candidate"]["mapping"]:
        key = (pair["track"]["medium"], pair["track"]["medium_index"])
        length = listed[key]
        assert length is not None
        pair["item"]["length"] = length / 1000


def _copy_listing(source: ReleaseJSON, onto: ReleaseJSON) -> None:
    lengths = [_listed_ms(t) for m in source["media"] for t in m["tracks"]]
    tracks = [t for m in onto["media"] for t in m["tracks"]]
    for track, length in zip(tracks, lengths, strict=True):
        track["length"] = length


class FakeLog:
    def __init__(self, rows: dict[int, dict[str, object]]) -> None:
        self.rows = rows

    def get_download_log_entry(self, log_id: int) -> dict[str, object] | None:
        return self.rows.get(log_id)


class FakeClock:
    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now


class FixtureMirror:
    """Serves the captured responses; records every call and its timeout.

    Pages are served by slicing the group's releases at the requested
    offset, as MusicBrainz does, from ``group`` (default: every captured
    page in order) and ``release_count`` (default: the captured count)."""

    def __init__(
        self, fixture: FixtureJSON, *, clock: FakeClock | None = None,
        seconds_per_call: float = 0.0,
        group: list[ReleaseJSON] | None = None,
        release_count: int | None = None,
    ) -> None:
        self.target = fixture["target"]
        self.group = group if group is not None else _releases(fixture)
        self.release_count = (
            release_count if release_count is not None
            else fixture["release_group_pages"][0]["release-count"])
        self.calls: list[tuple[str, int | None, float]] = []
        self.clock = clock
        self.seconds_per_call = seconds_per_call
        self.fail_with: BaseException | None = None
        self.fail_pages_with: BaseException | None = None

    def _tick(self) -> None:
        if self.clock is not None:
            self.clock.now += self.seconds_per_call

    def release(self, release_id: str, *, timeout: float) -> MBDiscRelease:
        self.calls.append(("release", None, timeout))
        self._tick()
        if self.fail_with is not None:
            raise self.fail_with
        if release_id != self.target["id"]:
            raise not_found(f"http://mirror/release/{release_id}")
        return msgspec.convert(self.target, type=MBDiscRelease)

    def release_group_page(
        self, release_group_id: str, *, offset: int, limit: int,
        timeout: float,
    ) -> MBDiscReleasePage:
        self.calls.append(("page", offset, timeout))
        self._tick()
        if self.fail_pages_with is not None:
            raise self.fail_pages_with
        assert limit == RELEASE_GROUP_PAGE_SIZE
        return msgspec.convert(
            {"release-count": self.release_count,
             "releases": self.group[offset:offset + limit]},
            type=MBDiscReleasePage)


def _run(
    log_id: int, *, fixture: FixtureJSON | None = None,
    row: dict[str, object] | None = None,
    mirror: FixtureMirror | None = None, clock: FakeClock | None = None,
) -> tuple[PressingCheck, FixtureMirror]:
    fixture = fixture if fixture is not None else _fixture(log_id)
    mirror = mirror if mirror is not None else FixtureMirror(fixture)
    log = FakeLog({log_id: row if row is not None else _log_row(fixture)})
    result = check_other_pressings(
        log_id, pdb=log, mirror=mirror, clock=clock or FakeClock())
    return result, mirror


# --- small written-out worlds for disc IDs --------------------------------

_SMALL_TARGET = "00000000-0000-0000-0000-00000000aaaa"


def _disc(disc_id: str, seconds: list[float]) -> DiscJSON:
    offsets: list[int] = []
    cursor = 150
    for length in seconds:
        offsets.append(cursor)
        cursor += round(length * 75)
    return {"id": disc_id, "offset-count": len(seconds), "sectors": cursor,
            "offsets": offsets}


def _small_world(
    *, listed: list[float], files: list[float], discs: list[DiscJSON],
    fmt: str = "CD",
) -> FixtureJSON:
    tracks: list[TrackJSON] = [
        {"position": n, "number": str(n), "title": f"Song {n}",
         "length": round(seconds * 1000)}
        for n, seconds in enumerate(listed, 1)
    ]
    target: ReleaseJSON = {
        "id": _SMALL_TARGET, "title": "Album", "country": "GB",
        "date": "2004",
        "media": [{"position": 1, "format": fmt, "track-count": len(tracks),
                   "discs": discs, "tracks": tracks}],
        "release-group": {"id": "00000000-0000-0000-0000-00000000bbbb",
                          "title": "Album"},
    }
    mapping: list[PairJSON] = [
        {"item": {"path": f"{n:02d}.flac", "title": f"Song {n}", "track": n,
                  "disc": 1, "length": seconds},
         "track": {"title": f"Song {n}", "medium": 1, "medium_index": n,
                   "medium_total": len(files), "length": listed[n - 1]}}
        for n, seconds in enumerate(files, 1)
    ]
    return {
        "download_log_id": 1,
        "candidate": {"album_id": _SMALL_TARGET, "extra_items": 0,
                      "extra_tracks": 0, "mapping": mapping},
        "target": target,
        "release_group_pages": [{"release-count": 1, "releases": [target]}],
    }


class TestRealCaptures(unittest.TestCase):
    def test_razorlight_matches_two_sibling_pressings_with_other_titles(
        self,
    ) -> None:
        result, _ = _run(RAZORLIGHT)
        self.assertEqual(result.outcome, PressingCheckOutcome.CHECKED)
        self.assertIs(result.fits_own_listing, False)
        self.assertEqual(
            [p.release_id[:8] for p in result.matching_pressings],
            ["07039d4c", "d63463aa"],
        )
        self.assertEqual(result.same_listing_pressings, [])
        cd = result.matching_pressings[1]
        self.assertEqual(cd.country, "GB")
        self.assertEqual(cd.date, "2004-06-28")
        self.assertEqual(cd.formats, ["CD"])
        self.assertEqual(cd.disc_id_count, 2)
        self.assertEqual(
            cd.url,
            "https://musicbrainz.org/release/d63463aa-2636-3c34-a919-f13a6f7a5e2d",
        )
        self.assertEqual(
            [(d.medium, d.position, d.pressing_title, d.release_title)
             for d in cd.title_differences],
            [(1, 12, "To the Sea", "Hang By, Hang By"),
             (1, 13, "Fall, Fall, Fall", "To the Sea")],
        )
        self.assertEqual(result.sentences, [
            ("This copy does not fit this release's own listing on tracks "
             "9, 12 and 13."),
            ('This copy matches 2 other pressings: UK 12" Vinyl, 2004 and '
             "UK CD, 2004. All 13 tracks line up within two seconds on each."),
            ("Both have 'To the Sea' and 'Fall, Fall, Fall' where this "
             "release lists 'Hang By, Hang By' and 'To the Sea'."),
            ("No disc ID is attached to this release, so there is no disc to "
             "check the listed lengths against."),
        ])
        assert result.target_lengths is not None
        self.assertFalse(result.target_lengths.has_disc_id)
        self.assertEqual(result.releases_in_group, 11)
        self.assertEqual(result.releases_checked, 10)
        self.assertFalse(result.capped)

    def test_a_sibling_missing_a_length_is_never_a_match(self) -> None:
        """The capture already holds one (151617f8, a GB CD with a track
        nobody timed); blanking one track of the matching GB CD removes it
        too."""
        fixture = _fixture(RAZORLIGHT)
        for release in _releases(fixture):
            if release["id"].startswith("d63463aa"):
                track = release["media"][0]["tracks"][4]
                track["length"] = None
                track["recording"] = {"length": None}
        result, _ = _run(RAZORLIGHT, fixture=fixture)
        self.assertEqual(
            [p.release_id[:8] for p in result.matching_pressings],
            ["07039d4c"])
        self.assertEqual(result.sentences[1:3], [
            ('This copy matches another pressing: UK 12" Vinyl, 2004. All 13 '
             "tracks line up within two seconds."),
            ("That pressing has 'To the Sea' and 'Fall, Fall, Fall' where "
             "this release lists 'Hang By, Hang By' and 'To the Sea'."),
        ])

    def test_kurt_vile_listing_matches_the_disc_and_the_files_do_not(
        self,
    ) -> None:
        result, _ = _run(KURT_VILE)
        self.assertEqual(result.matching_pressings, [])
        lengths = result.target_lengths
        assert lengths is not None
        (medium,) = lengths.media
        self.assertEqual(medium.status, MediumDiscStatus.CHECKED)
        self.assertEqual(medium.listing_disc_id, "QnrEobUrBLxC2TduRg3YQUDQYNs-")
        self.assertEqual(medium.files_against_disc, FilesAgainstDisc.NO_DISC)
        self.assertEqual(
            [t.disc_seconds for t in medium.discs[0].tracks],
            [338.1, 289.9, 336.3, 393.1, 228.6, 273.1])
        self.assertEqual(result.sentences, [
            ("This copy does not fit this release's own listing on tracks 2, "
             "3 and 5."),
            "No other pressing in this release group matches.",
            "The listed lengths match the disc ID attached to this release.",
            "This copy does not match that disc on tracks 2, 3 and 5.",
        ])

    def test_fairways_far_and_a_few_seconds_off(self) -> None:
        """Tracks 2 and 5 are a minute off the disc; track 10 is 4.2s off."""
        result, _ = _run(FAIRWAYS)
        self.assertEqual(result.sentences[2:], [
            "The listed lengths match the disc ID attached to this release.",
            ("This copy does not match that disc on tracks 2 and 5, and is a "
             "few seconds off on track 10."),
        ])

    def test_deer_tick_digital_target_has_no_disc_to_check(self) -> None:
        """Every sibling lists the last track as 'Miss K. / [silence] /
        Mr. Cigarette' where the digital target lists 'Miss K.': the
        titles DO differ in the real data, and the sentence says so."""
        result, _ = _run(DEER_TICK)
        self.assertEqual(len(result.matching_pressings), 3)
        self.assertEqual(result.sentences, [
            "This copy does not fit this release's own listing on track 12.",
            ('This copy matches 3 other pressings: Europe 12" Vinyl, 2012; '
             "US CD, 2011 and Europe CD, 2012. All 12 tracks line up within "
             "two seconds on each."),
            ("All three have 'Miss K. / [silence] / Mr. Cigarette' where "
             "this release lists 'Miss K.'."),
            ("This is a digital release, so there is no disc to check the "
             "listed lengths against."),
        ])

    def test_identical_titles_say_the_listings_differ_only_in_length(
        self,
    ) -> None:
        """Deer Tick capture with each sibling's last title set to the
        target's."""
        fixture = _fixture(DEER_TICK)
        for release in _releases(fixture):
            release["media"][0]["tracks"][-1]["title"] = "Miss K."
        result, _ = _run(DEER_TICK, fixture=fixture)
        self.assertEqual(result.sentences[2], (
            "Their titles are the same as this release's, so the listings "
            "differ only in length. This may be the same audio described "
            "differently (for example where a hidden track is counted), "
            "not necessarily a different disc."))

    def test_mixed_title_groups_are_described_per_group(self) -> None:
        """Razorlight capture with the vinyl's titles set to the target's."""
        fixture = _fixture(RAZORLIGHT)
        target_titles = [
            t["title"] for m in fixture["target"]["media"] for t in m["tracks"]]
        for release in _releases(fixture):
            if release["id"].startswith("07039d4c"):
                for track, title in zip(
                        release["media"][0]["tracks"], target_titles,
                        strict=True):
                    track["title"] = title
        result, _ = _run(RAZORLIGHT, fixture=fixture)
        self.assertEqual(result.sentences[2:5], [
            ('The UK 12" Vinyl, 2004 has the same titles as this release, so '
             "those listings differ only in length."),
            ("The UK CD, 2004 has 'To the Sea' and 'Fall, Fall, Fall' where "
             "this release lists 'Hang By, Hang By' and 'To the Sea'."),
            ("This may be the same audio described differently (for example "
             "where a hidden track is counted), not necessarily a different "
             "disc."),
        ])

    def test_punctuation_and_case_are_not_title_differences(self) -> None:
        fixture = _fixture(RAZORLIGHT)
        target_titles = [
            t["title"] for m in fixture["target"]["media"] for t in m["tracks"]]
        for release in _releases(fixture):
            if release["id"].startswith("07039d4c"):
                for track, title in zip(
                        release["media"][0]["tracks"], target_titles,
                        strict=True):
                    track["title"] = "… " + title.replace(",", "").upper()
        result, _ = _run(RAZORLIGHT, fixture=fixture)
        vinyl = result.matching_pressings[0]
        self.assertEqual(vinyl.release_id[:8], "07039d4c")
        self.assertEqual(vinyl.title_differences, [])

    def test_identical_descriptions_collapse(self) -> None:
        """Deer Tick capture with the US CD relabelled as the European CD."""
        fixture = _fixture(DEER_TICK)
        for release in _releases(fixture):
            if release["id"].startswith("065c1c8e"):
                release["country"] = "XE"
                release["date"] = "2012-04-02"
        result, _ = _run(DEER_TICK, fixture=fixture)
        self.assertTrue(result.sentences[1].startswith(
            'This copy matches 3 other pressings: Europe 12" Vinyl, 2012 and '
            "Europe CD, 2012 (2 releases). "))

    def test_multi_disc_release_per_disc_sentences(self) -> None:
        """B.o.B: disc 1 is a CD whose listing matches its disc ID and the
        files match it; disc 2 is a DVD, which cannot carry a disc ID."""
        result, _ = _run(BOB)
        lengths = result.target_lengths
        assert lengths is not None
        self.assertEqual(
            [m.status for m in lengths.media],
            [MediumDiscStatus.CHECKED, MediumDiscStatus.NO_DISC_FORMAT])
        self.assertEqual(result.sentences, [
            ("This copy does not fit this release's own listing on track 1 on "
             "disc 2."),
            "No other pressing in this release group matches.",
            "Disc 1's listed lengths match the disc ID attached to it.",
            "This copy matches that disc on every track.",
            ("Disc 2 is a DVD-Video, so there is no disc to check its listed "
             "lengths against."),
        ])

    def test_a_few_seconds_off_is_up_to_five_seconds(self) -> None:
        """B.o.B capture with track 3's file 5.0s, then 5.1s, longer."""
        for extra, expected in (
            (5.0, "This copy is a few seconds off that disc on track 3."),
            (5.1, "This copy does not match that disc on track 3."),
        ):
            with self.subTest(extra=extra):
                fixture = _fixture(BOB)
                for pair in fixture["candidate"]["mapping"]:
                    if (pair["track"]["medium"],
                            pair["track"]["medium_index"]) == (1, 3):
                        pair["item"]["length"] = 213.9 + extra
                result, _ = _run(BOB, fixture=fixture)
                self.assertEqual(result.sentences[3], expected)


class TestOwnListingFirst(unittest.TestCase):
    """Review item 3: a copy that fits its own release is never reported
    as matching another pressing."""

    def test_all_identical_is_indistinguishable_not_a_finding(self) -> None:
        """Kurt Vile capture with the files set to the target's listing and
        the digital sibling's listing copied from the target."""
        fixture = _fixture(KURT_VILE)
        _set_files_to_listing(fixture)
        for release in _releases(fixture):
            if release["id"] != fixture["target"]["id"]:
                _copy_listing(fixture["target"], release)
        result, _ = _run(KURT_VILE, fixture=fixture)
        self.assertIs(result.fits_own_listing, True)
        self.assertEqual(result.matching_pressings, [])
        self.assertEqual(len(result.same_listing_pressings), 1)
        self.assertEqual(result.sentences, [
            "This copy fits this release's own listing on every track.",
            ("1 other pressing has the same listing, so track lengths cannot "
             "tell them apart."),
            "The listed lengths match the disc ID attached to this release.",
            "This copy matches that disc on every track.",
        ])

    def test_held_copy_that_fits_its_target(self) -> None:
        """A held upgrade row (rejected) whose new copy fits the target:
        Razorlight capture, files set to the listing, both 2004 GB
        pressings given the target's listing. The capture's third GB CD
        (151617f8) already lists the same lengths through its recordings."""
        fixture = _fixture(RAZORLIGHT)
        _set_files_to_listing(fixture)
        for release in _releases(fixture):
            if release["id"][:8] in ("07039d4c", "d63463aa"):
                _copy_listing(fixture["target"], release)
        row = _log_row(fixture, outcome="rejected")
        result, _ = _run(RAZORLIGHT, fixture=fixture, row=row)
        self.assertEqual(result.matching_pressings, [])
        self.assertEqual(result.sentences[:2], [
            "This copy fits this release's own listing on every track.",
            ("3 other pressings have the same listing, so track lengths "
             "cannot tell them apart."),
        ])
        self.assertNotIn("matches", " ".join(result.sentences[:2]))

    def test_fits_own_with_no_other_pressing_alike(self) -> None:
        fixture = _small_world(listed=[200, 200], files=[201, 199], discs=[])
        result, _ = _run(1, fixture=fixture)
        self.assertEqual(result.sentences[:2], [
            "This copy fits this release's own listing on every track.",
            "No other pressing in this release group has the same listing.",
        ])


class TestDiscIds(unittest.TestCase):
    """Review items 1 and 2: the LISTING and the FILES are each compared
    with every attached disc, and absence of a disc ID claims nothing."""

    def test_listing_off_the_disc_the_files_match(self) -> None:
        """The reviewer's world: listed 200/200, disc 400/400, files
        400/400."""
        fixture = _small_world(
            listed=[200, 200], files=[400, 400],
            discs=[_disc("D", [400, 400])])
        result, _ = _run(1, fixture=fixture)
        assert result.target_lengths is not None
        (medium,) = result.target_lengths.media
        self.assertIsNone(medium.listing_disc_id)
        self.assertEqual(medium.files_against_disc, FilesAgainstDisc.OTHER_DISC)
        self.assertEqual(result.sentences[2:], [
            ("A disc ID is attached, but the listed lengths do not match it on "
             "tracks 1 and 2, so the listing may be wrong."),
            "This copy matches the attached disc on every track.",
        ])

    def test_two_disc_ids_one_fits_the_listing_one_the_files(self) -> None:
        fixture = _small_world(
            listed=[200, 200], files=[400, 400],
            discs=[_disc("A", [200, 200]), _disc("B", [400, 400])])
        result, _ = _run(1, fixture=fixture)
        assert result.target_lengths is not None
        (medium,) = result.target_lengths.media
        self.assertEqual(medium.listing_disc_id, "A")
        self.assertEqual(medium.files_disc_id, "B")
        self.assertEqual(result.sentences[2:], [
            ("The listed lengths match one of the 2 disc IDs attached to this "
             "release."),
            "This copy matches a different attached disc than the listing does.",
        ])

    def test_two_disc_ids_neither_fits_the_listing_nor_the_files(self) -> None:
        fixture = _small_world(
            listed=[200, 200], files=[300, 300],
            discs=[_disc("A", [250, 250]), _disc("B", [303, 330])])
        result, _ = _run(1, fixture=fixture)
        self.assertEqual(result.sentences[2:], [
            ("2 disc IDs are attached, but the listed lengths do not match any "
             "of them, so the listing may be wrong."),
            ("This copy does not match the closest of the 2 attached discs on "
             "track 2, and is a few seconds off on track 1."),
        ])

    def test_vinyl_and_unknown_format_and_cd_without_disc_id(self) -> None:
        for fmt, expected in (
            ('12" Vinyl', ("This is a vinyl record, so there is no disc to "
                          "check the listed lengths against.")),
            ("Cassette", ("This is a cassette, so there is no disc to check "
                         "the listed lengths against.")),
            ("CD", ("No disc ID is attached to this release, so there is no "
                   "disc to check the listed lengths against.")),
        ):
            with self.subTest(fmt=fmt):
                fixture = _small_world(
                    listed=[200, 200], files=[200, 200], discs=[], fmt=fmt)
                result, _ = _run(1, fixture=fixture)
                self.assertEqual(result.sentences[-1], expected)
                self.assertNotIn("typed in by hand", " ".join(result.sentences))

    def test_mixed_media_are_never_lumped_together(self) -> None:
        """Two CDs: disc 1's disc ID has the wrong track count, disc 2 has
        none."""
        fixture = _small_world(listed=[200, 200], files=[200, 200], discs=[])
        target = fixture["target"]
        second = copy.deepcopy(target["media"][0])
        second["position"] = 2
        target["media"][0]["discs"] = [_disc("X", [100, 100, 200])]
        target["media"].append(second)
        for n in (1, 2):
            fixture["candidate"]["mapping"].append({
                "item": {"path": f"2-{n}.flac", "title": None, "track": n,
                         "disc": 2, "length": 200.0},
                "track": {"title": f"Song {n}", "medium": 2, "medium_index": n,
                          "medium_total": 2, "length": 200.0},
            })
        result, _ = _run(1, fixture=fixture)
        self.assertEqual(result.sentences[-2:], [
            ("Disc 1 has a disc ID attached, but its track layout does not fit "
             "that disc's tracks, so it cannot be used to check the listed "
             "lengths."),
            ("No disc ID is attached to disc 2, so there is no disc to check "
             "its listed lengths against."),
        ])


class TestTitles(unittest.TestCase):
    """Review item 4."""

    def test_combining_marks_keep_titles_apart(self) -> None:
        for a, b in (("कि", "का"), ("สวัสดี", "สวสด"), ("?", "!")):
            with self.subTest(a=a, b=b):
                self.assertNotEqual(_normalize_title(a), _normalize_title(b))

    def test_punctuation_and_case_still_fold(self) -> None:
        for a, b in (
            ("東京", "東京!"), ("Привет, мир", "привет мир"),
            ("Γειά σου", "γειά σου!"), ("Drip, Drip, Drip", "Drip Drip Drip"),
            ("...and", "… And"),
        ):
            with self.subTest(a=a, b=b):
                self.assertEqual(_normalize_title(a), _normalize_title(b))


class TestBoundaries(unittest.TestCase):
    def test_discogs_target_is_not_available_and_never_calls_the_mirror(
        self,
    ) -> None:
        fixture = _fixture(RAZORLIGHT)
        row = _log_row(fixture, album_id="1234567")
        result, mirror = _run(RAZORLIGHT, row=row)
        self.assertEqual(result.outcome, PressingCheckOutcome.DISCOGS_RELEASE)
        self.assertEqual(result.sentences, [
            "Checking other pressings is not available for Discogs releases."])
        self.assertEqual(mirror.calls, [])

    def test_release_id_is_normalized_before_any_url(self) -> None:
        fixture = _fixture(RAZORLIGHT)
        row = _log_row(
            fixture, album_id="  5AA7F80C-BC26-4EFC-9AAC-317B7E2A84EB ")
        result, _ = _run(RAZORLIGHT, row=row)
        self.assertEqual(result.outcome, PressingCheckOutcome.CHECKED)
        self.assertEqual(
            result.target_url,
            "https://musicbrainz.org/release/5aa7f80c-bc26-4efc-9aac-317b7e2a84eb")

    def test_missing_download_log(self) -> None:
        fixture = _fixture(RAZORLIGHT)
        mirror = FixtureMirror(fixture)
        result = check_other_pressings(
            999, pdb=FakeLog({}), mirror=mirror, clock=FakeClock())
        self.assertEqual(
            result.outcome, PressingCheckOutcome.DOWNLOAD_LOG_NOT_FOUND)
        self.assertEqual(mirror.calls, [])

    def test_no_selected_candidate(self) -> None:
        fixture = _fixture(RAZORLIGHT)
        row = _log_row(fixture)
        row["validation_result"] = {"candidates": []}
        result, mirror = _run(RAZORLIGHT, row=row)
        self.assertEqual(
            result.outcome, PressingCheckOutcome.NO_MATCHED_RELEASE)
        self.assertEqual(mirror.calls, [])

    def test_release_gone_from_musicbrainz(self) -> None:
        fixture = _fixture(RAZORLIGHT)
        row = _log_row(fixture, album_id="00000000-0000-0000-0000-000000000000")
        result, _ = _run(RAZORLIGHT, row=row)
        self.assertEqual(result.outcome, PressingCheckOutcome.RELEASE_NOT_FOUND)

    def test_unmapped_file_skips_the_sibling_comparison_only(self) -> None:
        fixture = _fixture(KURT_VILE)
        fixture["candidate"]["extra_items"] = 1
        result, _ = _run(KURT_VILE, fixture=fixture)
        self.assertFalse(result.compared_with_pressings)
        self.assertIsNone(result.fits_own_listing)
        self.assertEqual(result.sentences, [
            ("Not every file lines up with a track on this release, so other "
             "pressings could not be compared."),
            "The listed lengths match the disc ID attached to this release.",
            "This copy does not match that disc on tracks 2, 3 and 5.",
        ])

    def test_a_release_listed_twice_across_pages_counts_once(self) -> None:
        """Review item 6: the GB CD appears again at the start of the next
        page, as browse can do."""
        fixture = _fixture(RAZORLIGHT)
        group = _releases(fixture)
        cd = next(r for r in group if r["id"].startswith("d63463aa"))
        padded = group + [copy.deepcopy(group[0]) for _ in range(14)]
        for n, release in enumerate(padded[len(group):]):
            release["id"] = f"{n:08d}-0000-0000-0000-000000000000"
            release["media"][0]["tracks"] = release["media"][0]["tracks"][:3]
        group = [*padded, copy.deepcopy(cd)]
        mirror = FixtureMirror(fixture, group=group, release_count=len(group))
        result, _ = _run(RAZORLIGHT, fixture=fixture, mirror=mirror)
        self.assertEqual(
            [p.release_id[:8] for p in result.matching_pressings],
            ["07039d4c", "d63463aa"])
        self.assertTrue(result.sentences[1].startswith(
            'This copy matches 2 other pressings: UK 12" Vinyl, 2004 and '
            "UK CD, 2004."))
        self.assertEqual(result.releases_checked, len(group) - 2)

    def test_over_the_cap_reads_only_the_first_pages_and_says_so(self) -> None:
        """Kurt Vile's sibling repeated with fresh ids and a declared count
        of 400."""
        fixture = _fixture(KURT_VILE)
        sibling = next(
            r for r in _releases(fixture)
            if r["id"] != fixture["target"]["id"])
        group: list[ReleaseJSON] = []
        for n in range(RELEASE_GROUP_CAP + 50):
            release = copy.deepcopy(sibling)
            release["id"] = f"{n:08d}-0000-0000-0000-000000000000"
            group.append(release)
        mirror = FixtureMirror(fixture, group=group, release_count=400)
        result, _ = _run(KURT_VILE, fixture=fixture, mirror=mirror)
        self.assertEqual(
            [offset for kind, offset, _ in mirror.calls if kind == "page"],
            list(range(0, RELEASE_GROUP_CAP, RELEASE_GROUP_PAGE_SIZE)))
        self.assertTrue(result.capped)
        self.assertEqual(result.releases_checked, RELEASE_GROUP_CAP)
        self.assertEqual(result.releases_in_group, 400)
        self.assertIn(
            "Only the first 150 of 400 releases in this release group were "
            "checked.", result.sentences)

    def test_mirror_unreachable(self) -> None:
        fixture = _fixture(RAZORLIGHT)
        mirror = FixtureMirror(fixture)
        mirror.fail_with = urllib.error.URLError("connection refused")
        result, _ = _run(RAZORLIGHT, mirror=mirror)
        self.assertEqual(
            result.outcome, PressingCheckOutcome.MIRROR_UNAVAILABLE)
        self.assertEqual(result.sentences, [
            ("Could not reach MusicBrainz, so other pressings were not "
             "checked. Try again later.")])

    def test_mirror_timeout_and_bad_json_are_unavailable_too(self) -> None:
        for error in (TimeoutError("timed out"), ConnectionResetError(),
                      json.JSONDecodeError("bad", "", 0),
                      msgspec.ValidationError("bad shape"),
                      mb_api.MirrorSlotTimeout("busy")):
            with self.subTest(error=type(error).__name__):
                fixture = _fixture(RAZORLIGHT)
                mirror = FixtureMirror(fixture)
                mirror.fail_pages_with = error
                result, _ = _run(RAZORLIGHT, mirror=mirror)
                self.assertEqual(
                    result.outcome, PressingCheckOutcome.MIRROR_UNAVAILABLE)

    def test_total_budget_stops_a_slow_mirror(self) -> None:
        """Each call takes 20s of a 30s budget: the third never starts, and
        no call is given more than a third of what is left (slot wait, one
        attempt and one retry)."""
        fixture = _fixture(KURT_VILE)
        group = _releases(fixture) * 40
        clock = FakeClock()
        mirror = FixtureMirror(
            fixture, clock=clock, seconds_per_call=20.0, group=group,
            release_count=100)
        result, _ = _run(KURT_VILE, fixture=fixture, mirror=mirror, clock=clock)
        self.assertEqual(
            result.outcome, PressingCheckOutcome.MIRROR_UNAVAILABLE)
        self.assertEqual(len(mirror.calls), 2)
        self.assertEqual(
            [timeout for _, _, timeout in mirror.calls], [
                min(PER_REQUEST_TIMEOUT_SECONDS, TOTAL_BUDGET_SECONDS / 3),
                min(PER_REQUEST_TIMEOUT_SECONDS,
                    (TOTAL_BUDGET_SECONDS - 20) / 3),
            ])

    def test_result_round_trips_as_json(self) -> None:
        result, _ = _run(RAZORLIGHT)
        payload = msgspec.to_builtins(result)
        self.assertEqual(payload["outcome"], "checked")
        self.assertEqual(
            msgspec.convert(payload, type=PressingCheck), result)


class TestMirrorClient(unittest.TestCase):
    """The two reads go through ``lib.mb_api._get`` with the configured
    base, quoted ids, the caller's timeout as both the request and the
    slot budget, and strict decoding."""

    def test_release_lookup_url_timeout_and_decode(self) -> None:
        fixture = _fixture(KURT_VILE)
        calls: list[tuple[str, float, float | None]] = []

        def fake_get(
            url: str, *, timeout: float, slot_timeout: float | None = None,
        ) -> object:
            calls.append((url, timeout, slot_timeout))
            return fixture["target"]

        with patch.object(mb_api, "MB_API_BASE", "http://mirror/ws/2"), \
                patch("lib.mb_api._get", side_effect=fake_get):
            release = mb_api.get_release_with_discids(
                fixture["target"]["id"], timeout=4.0)
        self.assertEqual(calls, [(
            ("http://mirror/ws/2/release/4c16e298-5042-4f31-8695-12c8ba481c9a"
             "?inc=recordings+media+discids+release-groups&fmt=json"),
            4.0, 4.0)])
        self.assertEqual(release.media[0].discs[0].offset_count, 6)

    def test_release_group_page_url_and_decode(self) -> None:
        fixture = _fixture(RAZORLIGHT)
        calls: list[tuple[str, float, float | None]] = []

        def fake_get(
            url: str, *, timeout: float, slot_timeout: float | None = None,
        ) -> object:
            calls.append((url, timeout, slot_timeout))
            return fixture["release_group_pages"][0]

        with patch.object(mb_api, "MB_API_BASE", "http://mirror/ws/2"), \
                patch("lib.mb_api._get", side_effect=fake_get):
            page = mb_api.browse_release_group_with_discids(
                "e94f5295-41d1-3a45-b6a7-63ab6f518ee3", offset=25, limit=25,
                timeout=3.5)
        self.assertEqual(calls, [(
            ("http://mirror/ws/2/release?release-group="
             "e94f5295-41d1-3a45-b6a7-63ab6f518ee3&inc=recordings+media+discids"
             "&limit=25&offset=25&fmt=json"), 3.5, 3.5)])
        self.assertEqual(page.release_count, 11)

    def test_an_error_object_in_a_200_is_not_a_release(self) -> None:
        """Review item 7: ``{"error": ...}`` with status 200 fails to
        decode, for the release and for a page, and the check says the
        mirror could not be asked."""
        for body in ({"error": "Not Found"}, {"id": "", "media": []}):
            with self.subTest(body=body), \
                    patch("lib.mb_api._get", return_value=body):
                fixture = _fixture(RAZORLIGHT)
                result = check_other_pressings(
                    RAZORLIGHT, pdb=FakeLog({RAZORLIGHT: _log_row(fixture)}),
                    clock=FakeClock())
                self.assertEqual(
                    result.outcome, PressingCheckOutcome.MIRROR_UNAVAILABLE)
        with self.assertRaises(msgspec.ValidationError):
            msgspec.convert({"error": "x"}, type=MBDiscReleasePage)

    def test_get_hands_the_timeout_to_urlopen(self) -> None:
        seen: list[float] = []

        def fake_urlopen(request: object, timeout: float) -> io.BytesIO:
            seen.append(timeout)
            return io.BytesIO(b'{"id": "x", "media": []}')

        with patch.object(mb_api, "MB_API_BASE", "http://mirror/ws/2"), \
                patch("urllib.request.urlopen", side_effect=fake_urlopen):
            mb_api.get_release_with_discids(
                "4c16e298-5042-4f31-8695-12c8ba481c9a", timeout=2.5)
        self.assertEqual(seen, [2.5])

    def test_a_busy_mirror_slot_ends_within_the_budget(self) -> None:
        """Review item 5: every slot for the mirror is held elsewhere. The
        budgeted read gives up after its slot timeout; the check reports
        the mirror unreachable instead of waiting."""
        base = "http://slot-test.invalid/ws/2"
        semaphore = mb_api._mirror_semaphore(base)
        held = 0
        while semaphore.acquire(blocking=False):
            held += 1
        try:
            with patch.object(mb_api, "MB_API_BASE", base), \
                    patch("urllib.request.urlopen") as urlopen:
                with self.assertRaises(mb_api.MirrorSlotTimeout):
                    mb_api.get_release_with_discids(
                        "4c16e298-5042-4f31-8695-12c8ba481c9a", timeout=0.05)
                fixture = _fixture(RAZORLIGHT)
                done = threading.Event()
                results: list[PressingCheck] = []

                def run() -> None:
                    results.append(check_other_pressings(
                        RAZORLIGHT,
                        pdb=FakeLog({RAZORLIGHT: _log_row(fixture)}),
                        clock=iter([0.0, 29.9, 29.9, 29.9]).__next__))
                    done.set()

                worker = threading.Thread(target=run)
                worker.start()
                self.assertTrue(done.wait(timeout=10))
                worker.join()
                self.assertEqual(
                    results[0].outcome, PressingCheckOutcome.MIRROR_UNAVAILABLE)
                urlopen.assert_not_called()
        finally:
            for _ in range(held):
                semaphore.release()

    def test_callers_without_a_budget_still_wait_for_a_slot(self) -> None:
        """Existing callers pass no slot timeout and are unchanged: they
        wait, and proceed once a slot frees."""
        base = "http://slot-wait.invalid/ws/2"
        semaphore = mb_api._mirror_semaphore(base)
        held = 0
        while semaphore.acquire(blocking=False):
            held += 1
        released = threading.Timer(0.2, semaphore.release)
        try:
            with patch("urllib.request.urlopen",
                       return_value=io.BytesIO(b"{}")):
                released.start()
                self.assertEqual(mb_api._get(f"{base}/release/x"), {})
        finally:
            released.join()
            for _ in range(held - 1):
                semaphore.release()


if __name__ == "__main__":
    unittest.main()
