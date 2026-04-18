"""Unit tests for lib.artist_compare — fuzzy MB+Discogs discography merge."""

import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from lib.artist_compare import (
    CompareBuckets,
    annotate_in_library,
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

    def test_any_year_mismatch_splits(self):
        """Exact year required. Year-tolerance produced false positives
        like MB Album 1964 matching Discogs EP 1963 for 'Twist and Shout'."""
        result = merge_discographies(
            [_mb("A Working Title in Green", "2002")],
            [_dg("A Working Title In Green", "2000")],
        )
        self.assertEqual(result.both, [])
        self.assertEqual(len(result.mb_only), 1)
        self.assertEqual(len(result.discogs_only), 1)

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

    def test_dedupes_case_only_discogs_duplicates(self):
        """The Beatles 'Twist and Shout' case from the user — two Discogs
        masters with case-only title difference should collapse into one
        cross-source row, not produce a bonus discogs_only entry."""
        mb_rg = _mb("Twist and Shout", "1964", id="mb-1")
        dg_a = _dg("Twist and Shout", "1964", id="dg-A")
        dg_b = _dg("Twist And Shout", "1964", id="dg-B")  # capitalised And
        result = merge_discographies([mb_rg], [dg_a, dg_b])
        self.assertEqual(len(result.both), 1)
        self.assertEqual(result.both[0]["mb"]["id"], "mb-1")
        self.assertEqual(result.both[0]["discogs"]["id"], "dg-A")  # first survives
        self.assertEqual(result.discogs_only, [])
        self.assertEqual(result.mb_only, [])

    def test_within_source_dedup_preserves_unique_titles(self):
        """Dedup must not collapse genuinely different titles."""
        result = merge_discographies(
            [],
            [_dg("Album One", "2000"), _dg("Album Two", "2000")],
        )
        self.assertEqual(len(result.discogs_only), 2)

    def test_within_source_dedup_respects_year_distance(self):
        """Same title in years far apart (e.g. self-titled re-release a
        decade later) stays as two rows."""
        result = merge_discographies(
            [],
            [_dg("Self Titled", "1990"), _dg("Self Titled", "2010")],
        )
        self.assertEqual(len(result.discogs_only), 2)

    def test_within_source_dedup_keeps_different_types(self):
        """EP 1963 and Album 1963 of the same name are legitimately
        different release groups even when the title normalises to the
        same thing. Dedup must NOT collapse them."""
        ep = {**_dg("Twist And Shout", "1963", id="dg-ep"), "type": "EP"}
        album = {**_dg("Twist And Shout", "1963", id="dg-album"), "type": "Album"}
        result = merge_discographies([], [ep, album])
        self.assertEqual(len(result.discogs_only), 2)

    def test_beatles_mb_album_picks_discogs_album_over_ep(self):
        """The root user bug: MB 'Twist and Shout' Album 1964 was
        matching Discogs 'Twist And Shout' EP 1963 (within year
        tolerance, first in input order) instead of Discogs Album 1964.
        Exact-year requirement + same-type scoring fixes it."""
        mb = {**_mb("Twist and Shout", "1964", id="mb-album"), "type": "Album"}
        dg_ep_63 = {**_dg("Twist And Shout", "1963", id="dg-ep"), "type": "EP"}
        dg_single_64 = {**_dg("Twist And Shout", "1964", id="dg-single"), "type": "Single"}
        dg_album_64 = {**_dg("Twist And Shout", "1964", id="dg-album"), "type": "Album"}
        # EP comes first in input order — previously would have matched.
        result = merge_discographies([mb], [dg_ep_63, dg_single_64, dg_album_64])
        self.assertEqual(len(result.both), 1)
        self.assertEqual(result.both[0]["discogs"]["id"], "dg-album")
        # EP and Single stay as discogs-only rows
        self.assertEqual({r["id"] for r in result.discogs_only},
                         {"dg-ep", "dg-single"})

    def test_exact_year_without_same_type_still_matches(self):
        """When only one exact-year candidate exists, it matches even
        if types don't match — type is a preference, not a requirement."""
        mb = {**_mb("Side Project", "2020", id="mb-1"), "type": "Album"}
        dg = {**_dg("Side Project", "2020", id="dg-1"), "type": "EP"}
        result = merge_discographies([mb], [dg])
        self.assertEqual(len(result.both), 1)
        self.assertEqual(result.both[0]["discogs"]["id"], "dg-1")


class TestAnnotateInLibrary(unittest.TestCase):
    def test_mb_row_matched_by_release_group_id(self):
        rg = {"id": "rg-uuid", "title": "OK Computer"}
        lib = [{"mb_releasegroupid": "rg-uuid", "album": "Different Title",
                "formats": "MP3", "min_bitrate": 245000}]
        annotate_in_library([rg], [], lib)
        self.assertTrue(rg["in_library"])
        self.assertEqual(rg["library_format"], "MP3")
        self.assertEqual(rg["library_min_bitrate"], 245)

    def test_mb_row_matched_by_title_when_no_rgid(self):
        rg = {"id": "rg-uuid", "title": "OK Computer"}
        lib = [{"mb_releasegroupid": None, "album": "OK Computer"}]
        annotate_in_library([rg], [], lib)
        self.assertTrue(rg["in_library"])

    def test_mb_row_unmatched(self):
        rg = {"id": "rg-uuid", "title": "Unowned"}
        lib = [{"mb_releasegroupid": "other", "album": "Other Album"}]
        annotate_in_library([rg], [], lib)
        self.assertFalse(rg["in_library"])

    def test_discogs_row_matched_by_mb_albumid(self):
        # Beets stores numeric Discogs release IDs in mb_albumid for
        # Discogs-imported albums.
        master = {"id": "12345", "title": "OK Computer"}
        lib = [{"mb_albumid": "12345", "album": "Different Title"}]
        annotate_in_library([], [master], lib)
        self.assertTrue(master["in_library"])

    def test_discogs_row_matched_by_title(self):
        master = {"id": "99999", "title": "OK Computer"}
        lib = [{"mb_albumid": "different-uuid", "album": "OK Computer"}]
        annotate_in_library([], [master], lib)
        self.assertTrue(master["in_library"])

    def test_title_normalization(self):
        # "Text_Bomb" library title matches "Text Bomb" RG title
        rg = {"id": "rg", "title": "Text Bomb"}
        lib = [{"mb_releasegroupid": None, "album": "Text_Bomb"}]
        annotate_in_library([rg], [], lib)
        self.assertTrue(rg["in_library"])

    def test_empty_library(self):
        rg = {"id": "rg", "title": "X"}
        master = {"id": "1", "title": "X"}
        annotate_in_library([rg], [master], [])
        self.assertFalse(rg["in_library"])
        self.assertFalse(master["in_library"])

    def test_handles_missing_fields(self):
        # Library album with no mb_releasegroupid / mb_albumid / album —
        # should not crash.
        annotate_in_library(
            [{"id": "rg", "title": "X"}],
            [{"id": "1", "title": "X"}],
            [{}],
        )

    def test_rank_fn_invoked_on_match(self):
        """Caller plugs in a codec-aware rank fn; the matched row picks
        up library_rank from it."""
        rg = {"id": "rg", "title": "X"}
        lib = [{"mb_releasegroupid": "rg", "album": "X",
                "formats": "Opus", "min_bitrate": 128000}]
        # Stub rank_fn — would be the real quality_rank wrapper in prod
        def rank_fn(fmt, kbps):
            return "transparent" if (fmt == "Opus" and kbps == 128) else "unknown"
        annotate_in_library([rg], [], lib, rank_fn=rank_fn)
        self.assertEqual(rg["library_rank"], "transparent")

    def test_no_quality_fields_when_unmatched(self):
        rg = {"id": "rg", "title": "Unowned"}
        annotate_in_library([rg], [], [])
        self.assertFalse(rg["in_library"])
        self.assertNotIn("library_format", rg)
        self.assertNotIn("library_min_bitrate", rg)
        self.assertNotIn("library_rank", rg)


if __name__ == "__main__":
    unittest.main()
