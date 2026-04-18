"""Unit tests for lib.artist_compare — fuzzy MB+Discogs discography merge."""

import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from lib.artist_compare import (
    CompareBuckets,
    extract_year,
    merge_discographies,
    normalize_title,
)


def _mb(title: str, year: str = "", id: str = "rg") -> dict:
    return {"id": id, "title": title, "first_release_date": year, "type": "Album"}


def _dg(title: str, year: str = "", id: str = "1") -> dict:
    return {"id": id, "title": title, "first_release_date": year, "type": "Album"}


class TestNormalizeTitle(unittest.TestCase):
    CASES = [
        ("lowercase", "OK Computer", "okcomputer"),
        ("special chars", "Text_Bomb", "textbomb"),
        ("punctuation", "Ok. Computer!", "okcomputer"),
        ("empty", "", ""),
        ("whitespace only", "   ", ""),
        ("unicode-ish", "Sigur Rós", "sigurrs"),  # non-ascii stripped
    ]

    def test_normalize(self):
        for desc, input_val, expected in self.CASES:
            with self.subTest(desc=desc):
                self.assertEqual(normalize_title(input_val), expected)


class TestExtractYear(unittest.TestCase):
    CASES = [
        ("full date", "1997-06-16", 1997),
        ("year only", "1997", 1997),
        ("partial date", "1997-06-00", 1997),  # discogs unknown-day convention
        ("empty", "", None),
        ("malformed", "abcd", None),
    ]

    def test_extract(self):
        for desc, input_val, expected in self.CASES:
            with self.subTest(desc=desc):
                self.assertEqual(extract_year(input_val), expected)


class TestMergeDiscographies(unittest.TestCase):
    def test_exact_match(self):
        result = merge_discographies(
            [_mb("OK Computer", "1997")],
            [_dg("OK Computer", "1997")],
        )
        self.assertEqual(len(result.both), 1)
        self.assertEqual(result.mb_only, [])
        self.assertEqual(result.discogs_only, [])

    def test_year_within_tolerance(self):
        # 2000 vs 2002 — agent observed this exact case for Blueline Medic
        result = merge_discographies(
            [_mb("A Working Title in Green", "2002")],
            [_dg("A Working Title In Green", "2000")],
        )
        self.assertEqual(len(result.both), 1)

    def test_year_outside_tolerance(self):
        result = merge_discographies(
            [_mb("Some Album", "1997")],
            [_dg("Some Album", "2005")],
        )
        self.assertEqual(result.both, [])
        self.assertEqual(len(result.mb_only), 1)
        self.assertEqual(len(result.discogs_only), 1)

    def test_case_and_punctuation_normalization(self):
        # "Text_Bomb" vs "Text Bomb" — agent observed this.
        result = merge_discographies(
            [_mb("Text Bomb", "2003")],
            [_dg("Text_Bomb", "2003")],
        )
        self.assertEqual(len(result.both), 1)

    def test_different_titles(self):
        result = merge_discographies(
            [_mb("Album A", "2000")],
            [_dg("Album B", "2000")],
        )
        self.assertEqual(len(result.mb_only), 1)
        self.assertEqual(len(result.discogs_only), 1)

    def test_mb_only(self):
        result = merge_discographies(
            [_mb("Sleepyhead", "2003")],
            [],
        )
        self.assertEqual(len(result.mb_only), 1)
        self.assertEqual(result.both, [])
        self.assertEqual(result.discogs_only, [])

    def test_discogs_only(self):
        result = merge_discographies(
            [],
            [_dg("Bootleg Live 2001", "2001")],
        )
        self.assertEqual(len(result.discogs_only), 1)
        self.assertEqual(result.both, [])
        self.assertEqual(result.mb_only, [])

    def test_both_years_unknown_matches_on_title(self):
        result = merge_discographies(
            [_mb("Some Album", "")],
            [_dg("Some Album", "")],
        )
        self.assertEqual(len(result.both), 1)

    def test_one_year_unknown_skips_merge(self):
        # Conservative: don't merge when only one side has a year. Could be a
        # different release or the same — we don't know, so don't risk hiding
        # data behind a false merge.
        result = merge_discographies(
            [_mb("Some Album", "1997")],
            [_dg("Some Album", "")],
        )
        self.assertEqual(result.both, [])
        self.assertEqual(len(result.mb_only), 1)
        self.assertEqual(len(result.discogs_only), 1)

    def test_each_discogs_entry_matches_at_most_once(self):
        # If MB has the same title twice and Discogs has it once, only one MB
        # entry should pair with the Discogs row; the other goes to mb_only.
        result = merge_discographies(
            [_mb("Compilation", "2000", id="rg1"),
             _mb("Compilation", "2010", id="rg2")],
            [_dg("Compilation", "2000")],
        )
        self.assertEqual(len(result.both), 1)
        self.assertEqual(result.both[0]["mb"]["id"], "rg1")
        self.assertEqual(len(result.mb_only), 1)
        self.assertEqual(result.mb_only[0]["id"], "rg2")

    def test_returns_dataclass(self):
        result = merge_discographies([], [])
        self.assertIsInstance(result, CompareBuckets)


if __name__ == "__main__":
    unittest.main()
