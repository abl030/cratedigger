"""Tests for search query builder."""

import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from lib.search import (
    build_query, strip_special_chars, strip_short_tokens,
    wildcard_artist_tokens, cap_tokens,
    SearchVariant, _distinctive_token_pool, select_variant,
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


class TestDistinctiveTokenPool(unittest.TestCase):
    """Token pool ordering: dedup case-insensitive, sort length-desc, drop short."""

    def test_basic_length_desc_sort(self):
        pool = _distinctive_token_pool(["Tallahassee", "Idylls", "Peg"])
        # All >2 chars; sorted by length desc
        self.assertEqual(pool, ["Tallahassee", "Idylls", "Peg"])

    def test_drops_short_tokens(self):
        # Tokens of length <=2 are dropped
        pool = _distinctive_token_pool(["Of A Hum", "In It"])
        # "Of", "A", "In", "It" all dropped; "Hum" kept (3 chars)
        self.assertEqual(pool, ["Hum"])

    def test_dedupe_case_insensitive_preserves_first_seen(self):
        # First-seen casing is preserved
        pool = _distinctive_token_pool(["Hello World", "hello there", "WORLD peace"])
        # "Hello" (kept), "World" (kept), "hello" dropped (dup), "there" kept,
        # "WORLD" dropped (dup), "peace" kept
        # By length desc: "there"(5), "peace"(5), "Hello"(5), "World"(5)
        # Stable secondary alphabetical lowercase: hello, peace, there, world
        self.assertEqual(set(pool), {"Hello", "World", "there", "peace"})
        # All kept in case from first-seen
        self.assertIn("Hello", pool)
        self.assertIn("World", pool)
        self.assertNotIn("hello", pool)
        self.assertNotIn("WORLD", pool)

    def test_strips_special_chars(self):
        pool = _distinctive_token_pool(["Don't Stop", "Hum, Sound"])
        # "Don" "t" "Stop" "Hum" "Sound" → drop t (1 char)
        # Remaining: ["Don", "Stop", "Hum", "Sound"] → length desc
        self.assertEqual(pool, ["Sound", "Stop", "Don", "Hum"])

    def test_empty_input(self):
        self.assertEqual(_distinctive_token_pool([]), [])

    def test_all_short_tokens(self):
        # If every token is length <=2, pool is empty
        self.assertEqual(_distinctive_token_pool(["A B", "If So", "Of It"]), [])

    def test_sort_is_stable_deterministic(self):
        # Same-length tokens should be ordered deterministically (alpha lowercase)
        pool = _distinctive_token_pool(["Delta Alpha Gamma"])
        # All length 5, secondary sort alphabetical lowercase
        self.assertEqual(pool, ["Alpha", "Delta", "Gamma"])


class TestSelectVariant(unittest.TestCase):
    """Variant generator ladder — pure decision logic via subTest table."""

    # Reusable token pools
    POOL_BIG = ["Tallahassee", "Idylls", "Frontier", "Treasure", "Going", "Peg"]
    # _distinctive_token_pool sorted: Tallahassee(11), Treasure(8), Frontier(8),
    # Idylls(6), Going(5), Peg(3) → stable alpha for ties
    # → ["Tallahassee", "Frontier", "Treasure", "Idylls", "Going", "Peg"]

    POOL_SEVEN = ["Aaaaaaa", "Bbbbbb", "Ccccc", "Dddd", "Eee", "Fff", "Ggg"]
    # length: 7,6,5,4,3,3,3 → sorted desc + alpha lowercase tie-break
    # → ["Aaaaaaa", "Bbbbbb", "Ccccc", "Dddd", "Eee", "Fff", "Ggg"]

    def test_default_branch_attempts_below_threshold(self):
        # cycles 0-4 with threshold=5 all return default
        cases = [
            ("attempt_0", 0),
            ("attempt_1", 1),
            ("attempt_2", 2),
            ("attempt_3", 3),
            ("attempt_4", 4),
        ]
        for desc, attempts in cases:
            with self.subTest(desc=desc, attempts=attempts):
                v = select_variant(
                    search_attempts=attempts,
                    threshold=5,
                    base_query="*ountain *oats Tallahassee",
                    year="1991",
                    track_titles=["Tallahassee", "Idylls"],
                )
                self.assertEqual(v.kind, "default")
                self.assertEqual(v.query, "*ountain *oats Tallahassee")
                self.assertEqual(v.tag, "default")
                self.assertIsNone(v.slice_index)

    def test_v1_year_at_threshold_with_known_year(self):
        v = select_variant(
            search_attempts=5,
            threshold=5,
            base_query="*he *eatles Abbey Road",
            year="1969",
            track_titles=["Something", "Octopus's Garden"],
        )
        self.assertEqual(v.kind, "v1_year")
        self.assertEqual(v.query, "*he *eatles Abbey Road 1969")
        self.assertEqual(v.tag, "v1_year")
        self.assertIsNone(v.slice_index)

    def test_v1_year_with_long_date_uses_4char_prefix(self):
        v = select_variant(
            search_attempts=5,
            threshold=5,
            base_query="base",
            year="1991-08-01",
            track_titles=["Track"],
        )
        self.assertEqual(v.kind, "v1_year")
        self.assertEqual(v.query, "base 1991")
        self.assertEqual(v.tag, "v1_year")

    def test_v4_first_slice_after_v1(self):
        # cycle 6 with year known → V4 slice 0 (first 3 tokens of pool)
        v = select_variant(
            search_attempts=6,
            threshold=5,
            base_query="base",
            year="1991",
            track_titles=self.POOL_BIG,
        )
        self.assertEqual(v.kind, "v4_tracks")
        self.assertEqual(v.tag, "v4_tracks_0")
        self.assertEqual(v.slice_index, 0)
        # Pool: Tallahassee, Frontier, Treasure, Idylls, Going, Peg
        # First 3
        self.assertEqual(v.query, "Tallahassee Frontier Treasure")

    def test_v4_second_slice(self):
        v = select_variant(
            search_attempts=7,
            threshold=5,
            base_query="base",
            year="1991",
            track_titles=self.POOL_BIG,
        )
        self.assertEqual(v.kind, "v4_tracks")
        self.assertEqual(v.tag, "v4_tracks_1")
        self.assertEqual(v.slice_index, 1)
        self.assertEqual(v.query, "Idylls Going Peg")

    def test_year_none_skips_v1_goes_to_v4(self):
        # cycle 5 with year=None → V4 slice 0 directly
        v = select_variant(
            search_attempts=5,
            threshold=5,
            base_query="base",
            year=None,
            track_titles=self.POOL_BIG,
        )
        self.assertEqual(v.kind, "v4_tracks")
        self.assertEqual(v.tag, "v4_tracks_0")
        self.assertEqual(v.slice_index, 0)
        self.assertEqual(v.query, "Tallahassee Frontier Treasure")

    def test_year_0000_treated_as_unknown(self):
        # Literal "0000" string → unknown → skip V1
        v = select_variant(
            search_attempts=5,
            threshold=5,
            base_query="base",
            year="0000",
            track_titles=self.POOL_BIG,
        )
        self.assertEqual(v.kind, "v4_tracks")
        self.assertEqual(v.tag, "v4_tracks_0")
        self.assertEqual(v.slice_index, 0)

    def test_year_0000_dash_treated_as_unknown(self):
        # "0000-00-00" → also unknown
        v = select_variant(
            search_attempts=5,
            threshold=5,
            base_query="base",
            year="0000-00-00",
            track_titles=self.POOL_BIG,
        )
        self.assertEqual(v.kind, "v4_tracks")
        self.assertEqual(v.tag, "v4_tracks_0")
        self.assertEqual(v.slice_index, 0)

    def test_malformed_year_strings_treated_as_unknown(self):
        """Year strings that aren't a 4-char numeric prefix → V4, not V1.

        Adversarial review A2: the original ``_year_is_known`` only checked
        ``startswith("0000")`` so "0", "", whitespace, "unknown", and short
        numeric prefixes like "199" all leaked through and produced a V1
        query that appended a meaningless year token.
        """
        bad_years = [
            ("single_digit", "0"),
            ("empty_string", ""),
            ("whitespace_only", "   "),
            ("non_numeric", "unknown"),
            ("three_digit_prefix", "199"),
        ]
        for desc, year in bad_years:
            with self.subTest(desc=desc, year=repr(year)):
                v = select_variant(
                    search_attempts=5,
                    threshold=5,
                    base_query="base",
                    year=year,
                    track_titles=self.POOL_BIG,
                )
                self.assertEqual(v.kind, "v4_tracks")
                self.assertEqual(v.tag, "v4_tracks_0")
                self.assertEqual(v.slice_index, 0)

    def test_empty_tracks_with_year_v1_then_exhausted(self):
        # cycle 5: year present → v1_year (still works without tracks)
        v5 = select_variant(
            search_attempts=5,
            threshold=5,
            base_query="base",
            year="1991",
            track_titles=[],
        )
        self.assertEqual(v5.kind, "v1_year")
        # cycle 6: V4 with empty pool → exhausted
        v6 = select_variant(
            search_attempts=6,
            threshold=5,
            base_query="base",
            year="1991",
            track_titles=[],
        )
        self.assertEqual(v6.kind, "exhausted")
        self.assertIsNone(v6.query)
        self.assertEqual(v6.tag, "exhausted")
        self.assertIsNone(v6.slice_index)

    def test_empty_tracks_no_year_immediate_exhausted(self):
        # cycle 5, year=None → skip V1 → V4 with empty pool → exhausted
        v = select_variant(
            search_attempts=5,
            threshold=5,
            base_query="base",
            year=None,
            track_titles=[],
        )
        self.assertEqual(v.kind, "exhausted")
        self.assertIsNone(v.query)
        self.assertEqual(v.tag, "exhausted")

    def test_dedup_after_pool_overshoots(self):
        # All-duplicate-after-dedupe titles → small pool
        v = select_variant(
            search_attempts=10,  # esc_idx=5, v4_idx=4, slice_start=12
            threshold=5,
            base_query="base",
            year="1991",
            track_titles=["Hello hello HELLO", "hello"],
        )
        # Pool: ["Hello"] (only 1 distinct token after case-insensitive dedup)
        # slice_start = 12 (or any large index) >= 1 → exhausted
        self.assertEqual(v.kind, "exhausted")

    def test_cycle_100_with_3_token_pool_exhausted(self):
        v = select_variant(
            search_attempts=100,
            threshold=5,
            base_query="base",
            year="1991",
            track_titles=["Alpha Beta Gamma"],
        )
        # Pool size 3 → only one V4 slice (slice 0)
        # esc_idx=95, v4_idx=94 → way past pool
        self.assertEqual(v.kind, "exhausted")

    def test_seven_token_pool_with_year_full_ladder(self):
        # POOL_SEVEN length 7. With year present:
        # cycle 5 → v1_year
        # cycle 6 → v4_tracks_0 tokens 0-2
        # cycle 7 → v4_tracks_1 tokens 3-5
        # cycle 8 → v4_tracks_2 tokens 6-6 (single token)
        # cycle 9 → exhausted
        cases = [
            (5, "v1_year", None, "base 1991"),
            (6, "v4_tracks", 0, "Aaaaaaa Bbbbbb Ccccc"),
            (7, "v4_tracks", 1, "Dddd Eee Fff"),
            (8, "v4_tracks", 2, "Ggg"),
            (9, "exhausted", None, None),
        ]
        for attempts, kind, slice_idx, expected_query in cases:
            with self.subTest(attempts=attempts, kind=kind):
                v = select_variant(
                    search_attempts=attempts,
                    threshold=5,
                    base_query="base",
                    year="1991",
                    track_titles=self.POOL_SEVEN,
                )
                self.assertEqual(v.kind, kind)
                self.assertEqual(v.slice_index, slice_idx)
                self.assertEqual(v.query, expected_query)

    def test_v4_tag_format_exact(self):
        # Tag for V4 must be exactly "v4_tracks_<idx>"
        for idx in [0, 1, 2, 5, 17]:
            with self.subTest(idx=idx):
                attempts = 5 + 1 + idx  # threshold + v1 + idx
                # Build a pool large enough for the slice
                pool_titles = [f"Token{n:03d}xxxx" for n in range((idx + 1) * 3)]
                v = select_variant(
                    search_attempts=attempts,
                    threshold=5,
                    base_query="base",
                    year="1991",
                    track_titles=pool_titles,
                )
                self.assertEqual(v.kind, "v4_tracks")
                self.assertEqual(v.tag, f"v4_tracks_{idx}")
                self.assertEqual(v.slice_index, idx)


if __name__ == "__main__":
    unittest.main()
