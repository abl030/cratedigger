"""Tests for track title cross-check — catches wrong pressings with different tracklists.

TDD: these tests are written FIRST, then the functions are implemented until green.
"""

import ast
import difflib
import logging
import os
import re
import sys
import unicodedata

# Extract functions from soularr.py without importing the full module
def _extract_functions(*names):
    """Extract named functions from soularr.py via AST."""
    soularr_path = os.path.join(os.path.dirname(__file__), "..", "soularr.py")
    with open(soularr_path) as f:
        source = f.read()

    tree = ast.parse(source)
    ns = {
        "logger": logging.getLogger("test"),
        "logging": logging,
        "re": re,
        "unicodedata": unicodedata,
        "difflib": difflib,
    }

    for node in ast.iter_child_nodes(tree):
        if isinstance(node, ast.FunctionDef) and node.name in names:
            func_source = ast.get_source_segment(source, node)
            exec(compile(func_source, "soularr.py", "exec"), ns)

    return tuple(ns[name] for name in names)


_normalize_title, _extract_title_from_filename, _track_titles_cross_check = _extract_functions(
    "_normalize_title", "_extract_title_from_filename", "_track_titles_cross_check"
)


# === Helper to build test data ===

def make_tracks(titles):
    """Build Lidarr-shaped track dicts from a list of titles."""
    return [{"title": t, "trackNumber": str(i + 1), "mediumNumber": 1} for i, t in enumerate(titles)]


def make_slskd_files(filenames):
    """Build slskd file dicts from a list of filenames."""
    return [{"filename": f} for f in filenames]


# === Filename extraction tests ===

class TestExtractTitleFromFilename:
    def test_standard_dash(self):
        assert _extract_title_from_filename("01 - Enter Sandman.mp3") == _normalize_title("Enter Sandman")

    def test_standard_dot(self):
        assert _extract_title_from_filename("01. Enter Sandman.mp3") == _normalize_title("Enter Sandman")

    def test_underscore(self):
        assert _extract_title_from_filename("01_Enter_Sandman.flac") == _normalize_title("Enter Sandman")

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
        result = _extract_title_from_filename("03 - Où est la plage.mp3")
        # Should normalize accents
        assert "ou est la plage" in result or "ou est la plage" in result


# === Cross-check: correct matches should PASS ===

class TestCrossCheckPass:
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
        """One track has a slightly different title — should still pass (tolerance)."""
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
        # 1/10 mismatch = 10% < 20% threshold → should pass
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

class TestCrossCheckFail:
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


if __name__ == "__main__":
    import pytest
    pytest.main([__file__, "-v"])
