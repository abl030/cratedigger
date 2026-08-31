"""Tests for track title cross-check — catches wrong pressings with different tracklists.

TDD: these tests are written FIRST, then the functions are implemented until green.
"""

import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from lib.util import (
    _extract_title_from_filename,
    _normalize_title,
    _track_titles_cross_check,
)

# === Helper to build test data ===

def make_tracks(titles):
    """Build release-track dicts from a list of titles."""
    return [{"title": t, "trackNumber": str(i + 1), "mediumNumber": 1} for i, t in enumerate(titles)]


def make_slskd_files(filenames):
    """Build slskd file dicts from a list of filenames."""
    return [{"filename": f} for f in filenames]


# Twenty-track tolerance fixture (see TestCrossCheckToleranceBoundary).
# ``_TOLERANCE_MATCHED`` titles get an exact filename each;
# ``_TOLERANCE_FOREIGN`` titles get none, so each one is a mismatch. Every
# foreign title scores below 0.45 against every supplied filename (measured),
# well clear of the 0.5 match threshold, so the mismatch COUNT is exact rather
# than a near-threshold accident.
_TOLERANCE_MATCHED = [
    "Harbour Lights", "Velvet Morning", "Copper Wire", "Glass Elevator",
    "Northern Kingdom", "Paper Aeroplane", "Quiet Machinery", "Rust and Bone",
    "Silver Wolves", "Tangerine Sky", "Underwater Ballroom", "Winter Postcard",
    "Yellow Submarine Dream", "Zephyr Boulevard", "Amber Corridor",
    "Bicycle Thieves",
]
_TOLERANCE_FOREIGN = [
    "Frozen Cathedral", "Ostrich Nebula Quintet", "Kyoto Pawnshop",
    "Gravel Hymnbook", "Plum Duff Waltz",
]


def make_tolerance_world(mismatches):
    """A 20-track release where exactly ``mismatches`` titles have no file."""
    matched = _TOLERANCE_MATCHED[:20 - mismatches]
    expected = matched + _TOLERANCE_FOREIGN[:mismatches]
    assert len(expected) == 20, "the divisor pins depend on exactly 20 tracks"
    files = [f"{i + 1:02d} - {t}.mp3" for i, t in enumerate(matched)]
    return make_tracks(expected), make_slskd_files(files)


# === Title normalization tests ===

class TestNormalizeTitle(unittest.TestCase):
    def test_strips_punctuation_but_keeps_apostrophe_and_ampersand(self):
        # The `[^\w\s'&]` pass: parentheses, the comma, the period, the em
        # dash and the bang all become whitespace and collapse away. The
        # character class whitelists ' and &, so those two survive.
        assert _normalize_title(
            "Undone (The Sweater Song), Pt. 2 — Live!"
        ) == "undone the sweater song pt 2 live"
        assert _normalize_title(
            "Rock & Roll Ain't Noise Pollution"
        ) == "rock & roll ain't noise pollution"


# === Filename extraction tests ===

class TestExtractTitleFromFilename(unittest.TestCase):
    def test_standard_dash(self):
        assert _extract_title_from_filename("01 - Enter Sandman.mp3") == "enter sandman"

    def test_standard_dot(self):
        assert _extract_title_from_filename("01. Enter Sandman.mp3") == "enter sandman"

    def test_underscore(self):
        assert _extract_title_from_filename("01_Enter_Sandman.flac") == "enter sandman"

    def test_no_separator(self):
        # "01 Enter Sandman.mp3" — number followed by space
        result = _extract_title_from_filename("01 Enter Sandman.mp3")
        assert "enter sandman" in result

    def test_artist_prefix(self):
        # "Metallica - 01 - Enter Sandman.mp3"
        result = _extract_title_from_filename("Metallica - 01 - Enter Sandman.mp3")
        assert "enter sandman" in result

    def test_no_track_number(self):
        # Just a bare title
        result = _extract_title_from_filename("Enter Sandman.mp3")
        assert "enter sandman" in result

    def test_flac_extension(self):
        result = _extract_title_from_filename("01 - Enter Sandman.flac")
        assert "enter sandman" in result

    def test_unicode(self):
        # NFKD splits "ù" into "u" + a combining grave (U+0300); the
        # `[^\w\s'&]` pass then drops the combining mark, so "Où" -> "ou".
        assert _extract_title_from_filename(
            "03 - Où est la plage.mp3") == "ou est la plage"


# === Cross-check: correct matches should PASS ===

class TestCrossCheckPass(unittest.TestCase):
    def test_metallica_correct(self):
        tracks = make_tracks([
            "Enter Sandman", "Sad but True", "Holier Than Thou",
            "The Unforgiven", "Wherever I May Roam",
        ])
        files = make_slskd_files([
            "01 - Enter Sandman.mp3", "02 - Sad but True.mp3",
            "03 - Holier Than Thou.mp3", "04 - The Unforgiven.mp3",
            "05 - Wherever I May Roam.mp3",
        ])
        assert _track_titles_cross_check(tracks, files) == True

    def test_weezer_blue_correct(self):
        tracks = make_tracks([
            "My Name Is Jonas", "No One Else",
            "The World Has Turned and Left Me Here",
            "Buddy Holly", "Undone – The Sweater Song",
        ])
        files = make_slskd_files([
            "01 - My Name Is Jonas.mp3", "02 - No One Else.mp3",
            "03 - The World Has Turned And Left Me Here.mp3",
            "04 - Buddy Holly.mp3", "05 - Undone (The Sweater Song).mp3",
        ])
        assert _track_titles_cross_check(tracks, files) == True

    def test_slight_title_variation(self):
        """One file is renamed, yet every expected title still finds a match."""
        tracks = make_tracks([
            "Track One", "Track Two", "Track Three",
            "Track Four", "Track Five",
            "Track Six", "Track Seven", "Track Eight",
            "Track Nine", "Track Ten",
        ])
        files = make_slskd_files([
            "01 - Track One.mp3", "02 - Track Two.mp3", "03 - Track Three.mp3",
            "04 - Track Four.mp3", "05 - Track Five.mp3",
            "06 - Track Six.mp3", "07 - Track Seven.mp3", "08 - Track Eight.mp3",
            "09 - Track Nine.mp3", "10 - Completely Different Name.mp3",
        ])
        # NOT a tolerance case, despite the renamed file: 0 mismatches
        # (measured — "track ten" still matches "track three" at 0.80, well
        # over the 0.5 bar, because these titles are mutually similar). This
        # is another all-match pass case; the tolerance divisor itself is
        # pinned by TestCrossCheckToleranceBoundary.
        assert _track_titles_cross_check(tracks, files) == True

    def test_bff_correct_bsides(self):
        tracks = make_tracks([
            "Battle of Who Could Care Less",
            "Champagne Supernova",
            "Theme From 'Dr. Pyser'",
        ])
        files = make_slskd_files([
            "01 - Battle Of Who Could Care Less.mp3",
            "02 - Champagne Supernova (Live).mp3",
            "03 - Theme From 'Dr. Pyser' (Live).mp3",
        ])
        assert _track_titles_cross_check(tracks, files) == True


# === Cross-check: wrong matches should FAIL ===

class TestCrossCheckFail(unittest.TestCase):
    def test_weezer_green_vs_blue(self):
        """Green Album files matched against Blue Album expected tracks → FAIL."""
        blue_tracks = make_tracks([
            "My Name Is Jonas", "No One Else",
            "The World Has Turned and Left Me Here",
            "Buddy Holly", "Undone – The Sweater Song",
            "Surf Wax America", "Say It Ain't So",
            "In the Garage", "Holiday", "Only in Dreams",
        ])
        green_files = make_slskd_files([
            "01 - Don't Let Go.mp3", "02 - Photograph.mp3",
            "03 - Hash Pipe.mp3", "04 - Island in the Sun.mp3",
            "05 - Crab.mp3", "06 - Knock-Down Drag-Out.mp3",
            "07 - Smile.mp3", "08 - Simple Pages.mp3",
            "09 - Glorious Day.mp3", "10 - O Girlfriend.mp3",
        ])
        assert _track_titles_cross_check(blue_tracks, green_files) == False

    def test_bff_wrong_bsides(self):
        """Wrong Ben Folds Five pressing — same A-side, different B-sides → FAIL."""
        expected = make_tracks([
            "Battle of Who Could Care Less",
            "Hava Nagila",
            "For Those of Ya'll Who Wear Fannie Packs",
        ])
        wrong_files = make_slskd_files([
            "01 - Battle Of Who Could Care Less.mp3",
            "02 - Champagne Supernova (Live).mp3",
            "03 - Theme From 'Dr. Pyser' (Live).mp3",
        ])
        # 2/3 tracks don't match = 67% > 20% → FAIL
        assert _track_titles_cross_check(expected, wrong_files) == False

    def test_completely_different_album(self):
        """Completely different album → FAIL."""
        tracks = make_tracks(["Song A", "Song B", "Song C"])
        files = make_slskd_files([
            "01 - Totally Different.mp3",
            "02 - Nothing Similar.mp3",
            "03 - Wrong Album Entirely.mp3",
        ])
        assert _track_titles_cross_check(tracks, files) == False


# === Cross-check: the tolerance divisor itself ===

class TestCrossCheckToleranceBoundary(unittest.TestCase):
    """Pin ``max_allowed = max(1, len(expected) // 5)`` in BOTH directions.

    The other cross-check fixtures cannot constrain it. The 3- and 5-track
    ones are floored to an allowance of 1 by ``max(1, ...)`` whichever divisor
    is used; the two 10-track ones carry 0 and 8 mismatches (measured), which
    land the same side of an allowance of 1, 2 or 3 alike. So `// 3` and
    `// 10` both survive every one of them. At 20 expected tracks the
    allowance is 4, and these two worlds straddle it.
    """

    def test_four_of_twenty_missing_is_within_tolerance(self):
        # 4 mismatches, allowance 4 → PASS. A stricter divisor (// 10 → 2)
        # would reject this legitimate release.
        tracks, files = make_tolerance_world(4)
        assert _track_titles_cross_check(tracks, files) == True

    def test_five_of_twenty_missing_exceeds_tolerance(self):
        # 5 mismatches, allowance 4 → FAIL. A looser divisor (// 3 → 6)
        # would wave this wrong pressing through.
        tracks, files = make_tolerance_world(5)
        assert _track_titles_cross_check(tracks, files) == False


if __name__ == "__main__":
    unittest.main()
