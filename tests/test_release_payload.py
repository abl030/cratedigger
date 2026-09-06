"""Unit tests for the shared mirror-payload field narrowers.

Lifted out of ``web/routes/pipeline_mutations.py`` (where the add flow
already read the payload this way) so the Replace service can persist the
same fields through the same narrowing (issue #1366). The contracts that
matter: a wrong-shaped field reads as ABSENT (default / ``None`` / ``[]``),
never raises, never coerces; a well-shaped field passes through untouched.
"""

from __future__ import annotations

import unittest

from lib.release_payload import (
    release_int_or_none,
    release_str,
    release_str_or_none,
    release_tracks,
)


class TestReleaseScalars(unittest.TestCase):
    def test_release_str(self) -> None:
        cases: list[tuple[str, dict[str, object], str, str]] = [
            ("present", {"title": "Absolution"}, "title", "Absolution"),
            ("empty string stays empty", {"title": ""}, "title", ""),
            ("missing reads as default", {}, "title", ""),
            ("None reads as default", {"title": None}, "title", ""),
            ("int reads as default", {"title": 7}, "title", ""),
        ]
        for desc, payload, key, expected in cases:
            with self.subTest(desc=desc):
                self.assertEqual(release_str(payload, key), expected)
        self.assertEqual(release_str({}, "title", "Unknown"), "Unknown")
        self.assertEqual(release_str({"title": 7}, "title", "Unknown"), "Unknown")

    def test_release_str_or_none(self) -> None:
        cases: list[tuple[str, dict[str, object], str | None]] = [
            ("present", {"artist_id": "art-1"}, "art-1"),
            ("missing", {}, None),
            ("None", {"artist_id": None}, None),
            ("int is not coerced", {"artist_id": 42}, None),
            ("empty string passes through", {"artist_id": ""}, ""),
        ]
        for desc, payload, expected in cases:
            with self.subTest(desc=desc):
                self.assertEqual(
                    release_str_or_none(payload, "artist_id"), expected,
                )

    def test_release_int_or_none(self) -> None:
        cases: list[tuple[str, dict[str, object], int | None]] = [
            ("present", {"year": 2003}, 2003),
            ("missing", {}, None),
            ("None", {"year": None}, None),
            ("numeric string is not coerced", {"year": "2003"}, None),
            ("zero passes through", {"year": 0}, 0),
        ]
        for desc, payload, expected in cases:
            with self.subTest(desc=desc):
                self.assertEqual(release_int_or_none(payload, "year"), expected)


class TestReleaseTracks(unittest.TestCase):
    def test_well_formed_list_passes_through_untouched(self) -> None:
        tracks: list[dict[str, object]] = [
            {"disc_number": 1, "track_number": 1, "title": "Apocalypse Please"},
            {"disc_number": 1, "track_number": 2, "title": "Time Is Running Out"},
        ]
        self.assertEqual(release_tracks({"tracks": tracks}), tracks)
        self.assertIs(release_tracks({"tracks": tracks})[0], tracks[0])

    def test_absent_or_wrong_shape_reads_as_empty(self) -> None:
        cases: list[tuple[str, dict[str, object]]] = [
            ("missing key", {}),
            ("None", {"tracks": None}),
            ("not a list", {"tracks": {"disc_number": 1}}),
            ("not even iterable", {"tracks": 7}),
            ("malformed member drops the whole field",
             {"tracks": [{"disc_number": 1}, "not a track"]}),
        ]
        for desc, payload in cases:
            with self.subTest(desc=desc):
                self.assertEqual(release_tracks(payload), [])


if __name__ == "__main__":
    unittest.main()
