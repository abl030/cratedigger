"""Tests for search query builder."""

import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from lib.search import (
    build_query, strip_special_chars, strip_short_tokens,
    wildcard_artist_tokens, cap_tokens,
)


class TestStripSpecialChars(unittest.TestCase):

    def test_apostrophes(self):
        self.assertEqual(strip_special_chars("Pink's"), "Pink s")

    def test_brackets(self):
        self.assertEqual(strip_special_chars("Album (Deluxe)"), "Album Deluxe")

    def test_underscores(self):
        self.assertEqual(strip_special_chars("Euro_EP"), "Euro EP")

    def test_commas(self):
        self.assertEqual(strip_special_chars("Picture a Hum, Can't Hear a Sound"),
                         "Picture a Hum Can t Hear a Sound")

    def test_periods(self):
        self.assertEqual(strip_special_chars("Vol. 2"), "Vol 2")

    def test_colons(self):
        self.assertEqual(strip_special_chars("Ambient 3: Day of Radiance"),
                         "Ambient 3 Day of Radiance")

    def test_semicolons(self):
        self.assertEqual(strip_special_chars("A; B"), "A B")

    def test_plus_tilde_pipe(self):
        self.assertEqual(strip_special_chars("A + B ~ C | D"), "A B C D")

    def test_ellipsis(self):
        self.assertEqual(strip_special_chars("...I Care Because You Do"),
                         "I Care Because You Do")

    def test_slash(self):
        self.assertEqual(strip_special_chars("Smile / Karma Package Deal"),
                         "Smile Karma Package Deal")

    def test_clean_passthrough(self):
        self.assertEqual(strip_special_chars("Mountain Goats"), "Mountain Goats")

    def test_hyphens_preserved(self):
        """Hyphens are common in titles and don't poison searches."""
        self.assertEqual(strip_special_chars("Self-Titled"), "Self-Titled")

    def test_multiple_spaces_collapsed(self):
        self.assertEqual(strip_special_chars("A  &  B"), "A B")


class TestStripShortTokens(unittest.TestCase):

    def test_drops_short(self):
        self.assertEqual(strip_short_tokens(["A", "Tribe", "Called", "Quest"]),
                         ["Tribe", "Called", "Quest"])

    def test_keeps_three_char(self):
        self.assertEqual(strip_short_tokens(["New", "Order"]), ["New", "Order"])

    def test_all_short_keeps_originals(self):
        self.assertEqual(strip_short_tokens(["If", "So"]), ["If", "So"])

    def test_drops_two_char(self):
        self.assertEqual(strip_short_tokens(["Of", "The", "Sun"]), ["The", "Sun"])


class TestWildcardArtistTokens(unittest.TestCase):

    def test_basic(self):
        self.assertEqual(wildcard_artist_tokens(["Mountain", "Goats"]),
                         ["*ountain", "*oats"])

    def test_short_artist(self):
        self.assertEqual(wildcard_artist_tokens(["AFI"]), ["*FI"])

    def test_beatles(self):
        self.assertEqual(wildcard_artist_tokens(["Beatles"]), ["*eatles"])

    def test_single_char_dropped(self):
        self.assertEqual(wildcard_artist_tokens(["A", "Band"]), ["*and"])

    def test_two_char(self):
        self.assertEqual(wildcard_artist_tokens(["UK"]), ["*K"])


class TestCapTokens(unittest.TestCase):

    def test_under_limit(self):
        self.assertEqual(cap_tokens(["a", "b", "c"], 4), ["a", "b", "c"])

    def test_at_limit(self):
        self.assertEqual(cap_tokens(["a", "b", "c", "d"], 4), ["a", "b", "c", "d"])

    def test_over_limit_drops_shortest(self):
        tokens = ["Animal", "Collective", "Merriweather", "Post", "Pavilion"]
        result = cap_tokens(tokens, 4)
        self.assertEqual(len(result), 4)
        self.assertNotIn("Post", result)  # shortest, dropped
        # Order preserved
        self.assertEqual(result, ["Animal", "Collective", "Merriweather", "Pavilion"])

    def test_preserves_order(self):
        tokens = ["The", "Mountain", "Goats", "Tallahassee", "Extra"]
        result = cap_tokens(tokens, 4)
        self.assertEqual(result, ["Mountain", "Goats", "Tallahassee", "Extra"])


class TestBuildQuery(unittest.TestCase):

    def test_basic(self):
        q = build_query("The Mountain Goats", "Tallahassee")
        # "The" stripped (<=2? no, 3 chars)... actually "The" is 3 chars, stays
        # Artist: The Mountain Goats → *he *ountain *oats
        # Title: Tallahassee
        # Total 4 tokens, at cap
        self.assertEqual(q, "*he *ountain *oats Tallahassee")

    def test_beatles(self):
        q = build_query("The Beatles", "Abbey Road")
        # *he *eatles Abbey Road — 4 tokens
        self.assertEqual(q, "*he *eatles Abbey Road")

    def test_afi(self):
        q = build_query("AFI", "Sing the Sorrow")
        # AFI → *FI (short tokens in title: "the" stays at 3 chars)
        # *FI Sing Sorrow — "the" dropped as <=2? No, "the" is 3.
        # *FI Sing the Sorrow — 4 tokens
        self.assertEqual(q, "*FI Sing the Sorrow")

    def test_long_title_caps_tokens(self):
        q = build_query("Animal Collective", "Merriweather Post Pavilion")
        assert q is not None
        # Artist: *nimal *ollective
        # Title: Merriweather Post Pavilion
        # Total: 5 tokens, cap at 4 → drop "Post" (shortest)
        self.assertIn("*nimal", q)
        self.assertNotIn("Post", q)
        self.assertEqual(len(q.split()), 4)

    def test_punctuation_stripped(self):
        q = build_query("P!nk", "Can't Get Enough")
        assert q is not None
        # P!nk → "P nk" after stripping → tokens ["P", "nk"]
        # strip_short_tokens: both <=2, keep originals → ["P", "nk"]
        # wildcard: "P" dropped (single char), "nk" → "*k"
        self.assertIn("*k", q)
        self.assertNotIn("!", q)

    def test_short_tokens_in_artist_dropped(self):
        q = build_query("A Tribe Called Quest", "The Low End Theory")
        assert q is not None
        # "A" stripped as short token from artist
        # Artist tokens: Tribe Called Quest → *ribe *alled *uest
        # Title tokens: The Low End Theory → "The", "Low", "End", "Theory"
        # strip short: all >=3, kept
        # Total: 7 tokens, cap at 4 → keep longest
        self.assertEqual(len(q.split()), 4)
        self.assertIn("*ribe", q)

    def test_returns_none_for_empty(self):
        q = build_query("", "")
        self.assertIsNone(q)

    def test_kanye(self):
        q = build_query("Kanye West", "My Beautiful Dark Twisted Fantasy")
        assert q is not None
        self.assertIn("*anye", q)
        self.assertEqual(len(q.split()), 4)  # capped
        # "*est" gets dropped as shortest token during cap — that's fine

    def test_single_word_title(self):
        q = build_query("Beyoncé", "Lemonade")
        assert q is not None
        # Beyoncé → strip special (é stays, it's not in the regex)
        # → *eyoncé Lemonade
        self.assertIn("*eyoncé", q)
        self.assertIn("Lemonade", q)

    def test_prince(self):
        q = build_query("Prince", "Purple Rain")
        self.assertEqual(q, "*rince Purple Rain")

    def test_self_titled_dedup(self):
        q = build_query("The Castiles", "The Castiles Live (Vol. 1)")
        assert q is not None
        # "Castiles" from title should be dropped (duplicate of artist)
        # "The" from title also dropped (duplicate)
        self.assertNotIn("Castiles", q.replace("*astiles", ""))
        self.assertIn("*astiles", q)
        self.assertIn("Live", q)
        self.assertIn("Vol", q)

    def test_self_titled_exact(self):
        q = build_query("Weezer", "Weezer")
        # Album title "Weezer" is duplicate of artist — only wildcarded version remains
        self.assertEqual(q, "*eezer")

    def test_various_artists_dropped(self):
        q = build_query("Various Artists", "Shelflife Collection")
        assert q is not None
        self.assertEqual(q, "Shelflife Collection")
        self.assertNotIn("*arious", q)

    def test_comma_in_title_stripped(self):
        """78 Saab bug: comma in 'Hum,' caused 0 search results."""
        q = build_query("78 Saab", "Picture a Hum, Can't Hear a Sound")
        assert q is not None
        self.assertNotIn(",", q)
        self.assertIn("*aab", q)

    def test_ellipsis_in_title(self):
        q = build_query("Aphex Twin", "...I Care Because You Do")
        assert q is not None
        self.assertNotIn(".", q)

    def test_no_prepend(self):
        q = build_query("The Beatles", "Abbey Road", prepend_artist=False)
        assert q is not None
        self.assertEqual(q, "Abbey Road")
        self.assertNotIn("*eatles", q)


if __name__ == "__main__":
    unittest.main()
