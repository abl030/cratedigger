"""The on-demand "matches another pressing" check (issue #1445).

Every MusicBrainz response here is a REAL capture from the mirror, trimmed
to the fields the check reads (``tests/fixtures/other_pressings/<id>.json``,
one file per download_log row, with that row's selected-candidate mapping
from the pipeline DB; typed in ``tests/other_pressings_fixtures.py``). The
fake mirror only serves those captures; it never invents a release. Worlds
the captures cannot show (an over-cap group, a failing mirror, identical
titles) are built by editing a capture, and each test says what it
changed.
"""

from __future__ import annotations

import copy
import io
import json
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
    PressingCheck,
    PressingCheckOutcome,
    check_other_pressings,
)
from tests.other_pressings_fixtures import (
    BOB,
    DEER_TICK,
    FAIRWAYS,
    KURT_VILE,
    RAZORLIGHT,
    FixtureJSON,
    PageJSON,
    ReleaseJSON,
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
) -> dict[str, object]:
    """The download_log row the DB returns, in the persisted wire shape."""
    return {
        "id": fixture["download_log_id"],
        "outcome": "success",
        "validation_result": validation_result(fixture, album_id=album_id),
    }


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
    """Serves the captured responses; records every call and its timeout."""

    def __init__(
        self, fixture: FixtureJSON, *, clock: FakeClock | None = None,
        seconds_per_call: float = 0.0,
    ) -> None:
        self.target = fixture["target"]
        self.pages = fixture["release_group_pages"]
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
        index = offset // RELEASE_GROUP_PAGE_SIZE
        page: PageJSON = self.pages[index] if index < len(self.pages) else {
            "release-count": self.pages[0]["release-count"], "releases": []}
        return msgspec.convert(page, type=MBDiscReleasePage)


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


class TestRealCaptures(unittest.TestCase):
    def test_razorlight_matches_two_sibling_pressings_with_other_titles(
        self,
    ) -> None:
        result, _ = _run(RAZORLIGHT)
        self.assertEqual(result.outcome, PressingCheckOutcome.CHECKED)
        self.assertEqual(
            [p.release_id[:8] for p in result.matching_pressings],
            ["07039d4c", "d63463aa"],
        )
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
        self.assertLessEqual(cd.worst_offset_seconds, 2.0)
        self.assertEqual(result.sentences, [
            ('This copy matches 2 other pressings: UK 12" Vinyl, 2004 and '
             "UK CD, 2004. All 13 tracks line up within two seconds on each."),
            ("Both have 'To the Sea' and 'Fall, Fall, Fall' where this "
             "release lists 'Hang By, Hang By' and 'To the Sea'."),
            "The listed lengths were typed in by hand and may be wrong.",
        ])
        self.assertIsNotNone(result.target_lengths)
        assert result.target_lengths is not None
        self.assertFalse(result.target_lengths.has_disc_id)
        self.assertEqual(result.releases_in_group, 11)
        self.assertFalse(result.capped)

    def test_a_sibling_missing_a_length_is_never_a_match(self) -> None:
        """The capture already holds one (151617f8, a GB CD with a track
        nobody timed); blanking one track of the matching GB CD removes it
        too."""
        fixture = _fixture(RAZORLIGHT)
        for release in _releases(fixture):
            if release["id"].startswith("d63463aa"):
                release["media"][0]["tracks"][4]["length"] = None
        result, _ = _run(RAZORLIGHT, fixture=fixture)
        self.assertEqual(
            [p.release_id[:8] for p in result.matching_pressings],
            ["07039d4c"],
        )
        self.assertTrue(result.sentences[0].startswith(
            'This copy matches another pressing: UK 12" Vinyl, 2004. All 13 '
            "tracks line up within two seconds."))
        self.assertEqual(
            result.sentences[1],
            "That pressing has 'To the Sea' and 'Fall, Fall, Fall' where "
            "this release lists 'Hang By, Hang By' and 'To the Sea'.",
        )

    def test_kurt_vile_no_sibling_and_the_disc_contradicts_the_files(
        self,
    ) -> None:
        result, _ = _run(KURT_VILE)
        self.assertEqual(result.outcome, PressingCheckOutcome.CHECKED)
        self.assertEqual(result.matching_pressings, [])
        lengths = result.target_lengths
        assert lengths is not None
        self.assertTrue(lengths.has_disc_id)
        self.assertEqual(lengths.disc_ids, ["QnrEobUrBLxC2TduRg3YQUDQYNs-"])
        self.assertEqual(
            [t.disc_seconds for t in lengths.tracks],
            [338.1, 289.9, 336.3, 393.1, 228.6, 273.1],
        )
        self.assertFalse(lengths.files_agree)
        self.assertEqual(
            [t.position for t in lengths.tracks if t.agrees is False],
            [2, 3, 5])
        self.assertEqual(result.sentences, [
            "No other pressing in this release group matches.",
            ("The listed lengths come from a real disc, so they can be "
             "trusted. This copy does not match that disc on tracks 2, 3 "
             "and 5."),
        ])

    def test_fairways_disc_contradicts_two_tracks_and_drifts_on_one(
        self,
    ) -> None:
        """Tracks 2 and 5 are a minute off the disc; track 10 is 4.2s
        off, the size of a gap-handling difference, and is said so."""
        result, _ = _run(FAIRWAYS)
        self.assertEqual(result.matching_pressings, [])
        self.assertEqual(result.sentences[1], (
            "The listed lengths come from a real disc, so they can be "
            "trusted. This copy does not match that disc on tracks 2 and 5, "
            "and is a few seconds off on track 10."))

    def test_deer_tick_hidden_track_listed_inside_the_last_title(self) -> None:
        """Every sibling lists the last track as 'Miss K. / [silence] /
        Mr. Cigarette' where the digital target lists 'Miss K.': the
        titles DO differ in the real data, and the sentence says so."""
        result, _ = _run(DEER_TICK)
        self.assertEqual(len(result.matching_pressings), 3)
        self.assertEqual(result.sentences[0], (
            'This copy matches 3 other pressings: Europe 12" Vinyl, 2012; '
            "US CD, 2011 and Europe CD, 2012. All 12 tracks line up within "
            "two seconds on each."))
        self.assertEqual(result.sentences[1], (
            "All three have 'Miss K. / [silence] / Mr. Cigarette' where "
            "this release lists 'Miss K.'."))

    def test_identical_titles_say_the_listings_differ_only_in_length(
        self,
    ) -> None:
        """Deer Tick capture with each sibling's last title set to the
        target's, the world the brief described."""
        fixture = _fixture(DEER_TICK)
        for release in _releases(fixture):
            release["media"][0]["tracks"][-1]["title"] = "Miss K."
        result, _ = _run(DEER_TICK, fixture=fixture)
        self.assertEqual(len(result.matching_pressings), 3)
        self.assertEqual(result.sentences[1], (
            "Their titles are the same as this release's, so the listings "
            "differ only in length. This may be the same audio described "
            "differently (for example where a hidden track is counted), "
            "not necessarily a different disc."))

    def test_mixed_title_groups_are_described_per_group(self) -> None:
        """Razorlight capture with the vinyl's titles set to the target's:
        one pressing differs only in length, the other in titles too."""
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
        self.assertEqual(result.sentences[1:4], [
            ('The UK 12" Vinyl, 2004 has the same titles as this release, so '
             "those listings differ only in length."),
            ("The UK CD, 2004 has 'To the Sea' and 'Fall, Fall, Fall' where "
             "this release lists 'Hang By, Hang By' and 'To the Sea'."),
            ("This may be the same audio described differently (for example "
             "where a hidden track is counted), not necessarily a different "
             "disc."),
        ])

    def test_punctuation_and_case_are_not_title_differences(self) -> None:
        """Razorlight capture with the vinyl's titles set to the target's,
        re-punctuated and re-cased the way real listings disagree
        ('Drip, Drip, Drip' / 'Drip Drip Drip', '...and' / '… And')."""
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
        self.assertTrue(result.sentences[0].startswith(
            'This copy matches 3 other pressings: Europe 12" Vinyl, 2012 and '
            "Europe CD, 2012 (2 releases). "))

    def test_a_few_seconds_off_the_disc_is_not_called_a_mismatch(
        self,
    ) -> None:
        """B.o.B capture with track 3's file 3s longer than the disc."""
        fixture = _fixture(BOB)
        for pair in fixture["candidate"]["mapping"]:
            if (pair["track"]["medium"], pair["track"]["medium_index"]) == (1, 3):
                pair["item"]["length"] += 3.0
        result, _ = _run(BOB, fixture=fixture)
        self.assertEqual(result.sentences[1], (
            "The listed lengths for disc 1 come from a real disc, so they "
            "can be trusted; the others were typed in by hand and may be "
            "wrong. This copy is a few seconds off that disc on track 3 on "
            "disc 1, which can come from how the gaps between tracks were "
            "ripped."))

    def test_multi_disc_release_with_a_disc_id_on_disc_one_only(self) -> None:
        result, _ = _run(BOB)
        self.assertEqual(result.matching_pressings, [])
        lengths = result.target_lengths
        assert lengths is not None
        self.assertTrue(lengths.has_disc_id)
        self.assertEqual(lengths.media_with_disc, [1])
        self.assertEqual(lengths.media_without_disc, [2])
        self.assertTrue(lengths.files_agree)
        self.assertEqual(result.sentences[1], (
            "The listed lengths for disc 1 come from a real disc, so they "
            "can be trusted; the others were typed in by hand and may be "
            "wrong. This copy matches that disc on every track."))


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
        self.assertEqual(result.sentences, [
            "This attempt has no matched release to compare against."])
        self.assertEqual(mirror.calls, [])

    def test_release_gone_from_musicbrainz(self) -> None:
        fixture = _fixture(RAZORLIGHT)
        row = _log_row(fixture, album_id="00000000-0000-0000-0000-000000000000")
        result, _ = _run(RAZORLIGHT, row=row)
        self.assertEqual(result.outcome, PressingCheckOutcome.RELEASE_NOT_FOUND)
        self.assertEqual(result.sentences, [
            ("MusicBrainz no longer has this release, so other pressings "
             "could not be checked.")])

    def test_unmapped_file_skips_the_sibling_comparison_only(self) -> None:
        fixture = _fixture(KURT_VILE)
        fixture["candidate"]["extra_items"] = 1
        result, _ = _run(KURT_VILE, fixture=fixture)
        self.assertEqual(result.outcome, PressingCheckOutcome.CHECKED)
        self.assertFalse(result.compared_with_pressings)
        self.assertEqual(result.sentences[0], (
            "Not every file lines up with a track on this release, so other "
            "pressings could not be compared."))
        self.assertIn("does not match that disc", result.sentences[1])

    def test_over_the_cap_reads_only_the_first_pages_and_says_so(self) -> None:
        """Kurt Vile's two-release page repeated with fresh ids and a
        declared count of 400."""
        fixture = _fixture(KURT_VILE)
        sibling = next(
            r for r in _releases(fixture)
            if r["id"] != fixture["target"]["id"])
        pages = []
        for page_index in range(RELEASE_GROUP_CAP // RELEASE_GROUP_PAGE_SIZE + 2):
            releases = []
            for n in range(RELEASE_GROUP_PAGE_SIZE):
                release = copy.deepcopy(sibling)
                release["id"] = f"{page_index:04d}{n:04d}-0000-0000-0000-000000000000"
                releases.append(release)
            pages.append({"release-count": 400, "releases": releases})
        fixture["release_group_pages"] = pages
        result, mirror = _run(KURT_VILE, fixture=fixture)
        self.assertEqual(
            [offset for kind, offset, _ in mirror.calls if kind == "page"],
            list(range(0, RELEASE_GROUP_CAP, RELEASE_GROUP_PAGE_SIZE)),
        )
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
                      msgspec.ValidationError("bad shape")):
            with self.subTest(error=type(error).__name__):
                fixture = _fixture(RAZORLIGHT)
                mirror = FixtureMirror(fixture)
                mirror.fail_pages_with = error
                result, _ = _run(RAZORLIGHT, mirror=mirror)
                self.assertEqual(
                    result.outcome, PressingCheckOutcome.MIRROR_UNAVAILABLE)

    def test_total_budget_stops_a_slow_mirror(self) -> None:
        """Each call takes 20s of a 30s budget: the third never starts,
        and no call is given more than half of what is left."""
        fixture = _fixture(KURT_VILE)
        fixture["release_group_pages"] = [
            {"release-count": 100, "releases": _releases(fixture)}
            for _ in range(4)
        ]
        clock = FakeClock()
        mirror = FixtureMirror(fixture, clock=clock, seconds_per_call=20.0)
        result, _ = _run(KURT_VILE, fixture=fixture, mirror=mirror, clock=clock)
        self.assertEqual(
            result.outcome, PressingCheckOutcome.MIRROR_UNAVAILABLE)
        self.assertEqual(len(mirror.calls), 2)
        timeouts = [timeout for _, _, timeout in mirror.calls]
        self.assertEqual(timeouts, [
            min(PER_REQUEST_TIMEOUT_SECONDS, TOTAL_BUDGET_SECONDS / 2),
            min(PER_REQUEST_TIMEOUT_SECONDS, (TOTAL_BUDGET_SECONDS - 20) / 2),
        ])

    def test_result_round_trips_as_json(self) -> None:
        result, _ = _run(RAZORLIGHT)
        payload = msgspec.to_builtins(result)
        self.assertEqual(payload["outcome"], "checked")
        self.assertEqual(
            msgspec.convert(payload, type=PressingCheck), result)


class TestMirrorClient(unittest.TestCase):
    """The two reads go through ``lib.mb_api._get`` with the configured
    base, quoted ids and the caller's timeout."""

    def test_release_lookup_url_timeout_and_decode(self) -> None:
        fixture = _fixture(KURT_VILE)
        calls: list[tuple[str, float]] = []

        def fake_get(url: str, *, timeout: float) -> object:
            calls.append((url, timeout))
            return fixture["target"]

        with patch.object(mb_api, "MB_API_BASE", "http://mirror/ws/2"), \
                patch("lib.mb_api._get", side_effect=fake_get):
            release = mb_api.get_release_with_discids(
                fixture["target"]["id"], timeout=4.0)
        self.assertEqual(calls, [(
            ("http://mirror/ws/2/release/4c16e298-5042-4f31-8695-12c8ba481c9a"
             "?inc=recordings+media+discids+release-groups&fmt=json"), 4.0)])
        self.assertEqual(release.media[0].discs[0].offset_count, 6)
        assert release.release_group is not None
        self.assertEqual(
            release.release_group.id, "c51ee20a-882c-4a29-9ec0-6f5a0d075fc6")

    def test_release_group_page_url_and_decode(self) -> None:
        fixture = _fixture(RAZORLIGHT)
        calls: list[tuple[str, float]] = []

        def fake_get(url: str, *, timeout: float) -> object:
            calls.append((url, timeout))
            return fixture["release_group_pages"][0]

        with patch.object(mb_api, "MB_API_BASE", "http://mirror/ws/2"), \
                patch("lib.mb_api._get", side_effect=fake_get):
            page = mb_api.browse_release_group_with_discids(
                "e94f5295-41d1-3a45-b6a7-63ab6f518ee3", offset=25, limit=25,
                timeout=3.5)
        self.assertEqual(calls, [(
            ("http://mirror/ws/2/release?release-group="
             "e94f5295-41d1-3a45-b6a7-63ab6f518ee3&inc=recordings+media+discids"
             "&limit=25&offset=25&fmt=json"), 3.5)])
        self.assertEqual(page.release_count, 11)
        self.assertEqual(len(page.releases), 11)

    def test_get_hands_the_timeout_to_urlopen(self) -> None:
        seen: list[float] = []

        def fake_urlopen(request: object, timeout: float) -> io.BytesIO:
            seen.append(timeout)
            return io.BytesIO(b"{}")

        with patch.object(mb_api, "MB_API_BASE", "http://mirror/ws/2"), \
                patch("urllib.request.urlopen", side_effect=fake_urlopen):
            mb_api.get_release_with_discids(
                "4c16e298-5042-4f31-8695-12c8ba481c9a", timeout=2.5)
        self.assertEqual(seen, [2.5])


if __name__ == "__main__":
    unittest.main()
