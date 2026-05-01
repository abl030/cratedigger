"""Tests for search query builder."""

import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from lib.search import (
    build_query, strip_special_chars, strip_short_tokens,
    wildcard_artist_tokens, cap_tokens,
    _per_track_queries, select_variant,
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

    def test_wildcard_artist_false_keeps_artist_literal(self):
        # wildcard_artist=False: artist prepended but NOT wildcarded.
        # Used by the un-wildcarded escalation tier — the wildcarded form
        # bypasses Soulseek's artist banlist but is silently dropped by many
        # peer clients, costing ~95% of recall.
        q = build_query("The Wiggles", "The Wiggles", wildcard_artist=False)
        self.assertEqual(q, "The Wiggles")

    def test_wildcard_artist_false_non_self_titled(self):
        q = build_query(
            "The Beatles", "Abbey Road", wildcard_artist=False,
        )
        self.assertEqual(q, "The Beatles Abbey Road")
        self.assertNotIn("*eatles", q or "")


class TestPerTrackQueries(unittest.TestCase):
    """Per-track queries: cleaned title tokens, no artist, no wildcards.

    Each track title becomes one full query; no AND-mash across multiple
    tracks. The album-match scoring step (sub-count gate + filename ratio
    + cross-check) disambiguates wrong albums after slskd responses come
    back, so we want maximal recall per query.
    """

    def test_basic_titles_in_original_order(self):
        # Cleaned with strip_special_chars; preserves source-tracklist order.
        out = _per_track_queries([
            "Get Ready to Wiggle",
            "Rock-A-Bye Your Bear",
            "Dorothy the Dinosaur",
        ])
        # Short tokens (<=2) dropped: "to" → drop
        # Token cap = MAX_SEARCH_TOKENS (4)
        self.assertEqual(out, [
            "Get Ready Wiggle",
            "Rock-A-Bye Your Bear",
            "Dorothy the Dinosaur",
        ])

    def test_strips_punctuation(self):
        # Apostrophes / commas / periods stripped via strip_special_chars
        out = _per_track_queries(["Don't Stop", "Hum, Sound"])
        self.assertEqual(out, ["Don Stop", "Hum Sound"])

    def test_dedupes_case_insensitively(self):
        # Wiggles tracklist has two "Archie's Theme" entries — emit once.
        out = _per_track_queries([
            "Archie's Theme", "ARCHIE'S theme", "Glub Glub Train",
        ])
        self.assertEqual(out, ["Archie Theme", "Glub Glub Train"])

    def test_skips_titles_that_clean_to_empty(self):
        # "??" → no alpha after strip_special_chars → query empty → skip
        out = _per_track_queries(["??", "Real Track"])
        self.assertEqual(out, ["Real Track"])

    def test_drops_short_tokens(self):
        # "A" (1 char) and "Go" (2 chars) dropped, "He" (2 chars) dropped.
        # "A-Wooing" stays — hyphen kept by strip_special_chars.
        out = _per_track_queries(["A Froggy He Would A-Wooing Go"])
        self.assertEqual(out, ["Froggy Would A-Wooing"])

    def test_caps_long_titles_at_max_tokens(self):
        # MAX_SEARCH_TOKENS = 4 — keep the four longest, restore original order.
        # Tokens: One(3), Two(3), Three(5), Four(4), Five(4), Six(3), Seven(5).
        # Longest 4 = {Three, Seven, Four, Five}; restored order in source.
        out = _per_track_queries(["One Two Three Four Five Six Seven"])
        self.assertEqual(out, ["Three Four Five Seven"])

    def test_empty_input(self):
        self.assertEqual(_per_track_queries([]), [])

    def test_all_short_tokens_title_falls_back(self):
        # strip_short_tokens keeps originals when ALL tokens are short
        out = _per_track_queries(["Of It"])
        self.assertEqual(out, ["Of It"])


class TestSelectVariant(unittest.TestCase):
    """Variant generator ladder — pure decision logic.

    New ladder (post-Wiggles-1991 forensics):
      cycle < threshold       → default     (wildcarded base)
      cycle == threshold      → unwild      (un-wildcarded base)
      cycle == threshold + 1  → unwild_year (un-wild base + year, if known)
      cycle == threshold + N  → track_<i>   (one bare track title per cycle)
      pool drained            → exhausted

    The wildcarded default form bypasses Soulseek's per-peer artist banlist
    but is silently dropped by ~95% of peer clients (live A/B test:
    `the wiggles 1991` → 241 hits vs `*he *iggles 1991` → 14 hits). The
    un-wildcarded tiers re-acquire that recall before the per-track tier
    fans out to peers who only share single tracks.
    """

    WIGGLES_TITLES = [
        "Get Ready to Wiggle",
        "Rock-A-Bye Your Bear",
        "Dorothy the Dinosaur",
        "Mischief the Monkey",
    ]

    def test_default_branch_attempts_below_threshold(self):
        # Cycles 0..threshold-1 all emit the wildcarded default.
        for attempts in range(5):
            with self.subTest(attempts=attempts):
                v = select_variant(
                    search_attempts=attempts,
                    threshold=5,
                    base_query="*he *iggles",
                    base_query_unwild="The Wiggles",
                    year="1991",
                    track_titles=self.WIGGLES_TITLES,
                )
                self.assertEqual(v.kind, "default")
                self.assertEqual(v.query, "*he *iggles")
                self.assertEqual(v.tag, "default")
                self.assertIsNone(v.slice_index)

    def test_unwild_at_threshold(self):
        # Cycle == threshold: emit the un-wildcarded base query.
        v = select_variant(
            search_attempts=5,
            threshold=5,
            base_query="*he *iggles",
            base_query_unwild="The Wiggles",
            year="1991",
            track_titles=self.WIGGLES_TITLES,
        )
        self.assertEqual(v.kind, "unwild")
        self.assertEqual(v.query, "The Wiggles")
        self.assertEqual(v.tag, "unwild")
        self.assertIsNone(v.slice_index)

    def test_unwild_year_at_threshold_plus_one(self):
        v = select_variant(
            search_attempts=6,
            threshold=5,
            base_query="*he *iggles",
            base_query_unwild="The Wiggles",
            year="1991",
            track_titles=self.WIGGLES_TITLES,
        )
        self.assertEqual(v.kind, "unwild_year")
        self.assertEqual(v.query, "The Wiggles 1991")
        self.assertEqual(v.tag, "unwild_year")

    def test_unwild_year_uses_4char_prefix(self):
        v = select_variant(
            search_attempts=6,
            threshold=5,
            base_query="*ase",
            base_query_unwild="base",
            year="1991-08-01",
            track_titles=self.WIGGLES_TITLES,
        )
        self.assertEqual(v.kind, "unwild_year")
        self.assertEqual(v.query, "base 1991")

    def test_track_tier_after_unwild_year(self):
        # Cycle threshold+2 → track_0 (first track title).
        v = select_variant(
            search_attempts=7,
            threshold=5,
            base_query="*he *iggles",
            base_query_unwild="The Wiggles",
            year="1991",
            track_titles=self.WIGGLES_TITLES,
        )
        self.assertEqual(v.kind, "track")
        self.assertEqual(v.tag, "track_0")
        self.assertEqual(v.slice_index, 0)
        # First Wiggles title cleaned: "Get Ready to Wiggle" → "Get Ready Wiggle"
        # ("to" dropped as short token).
        self.assertEqual(v.query, "Get Ready Wiggle")

    def test_track_tier_advances_per_cycle(self):
        # Each cycle past unwild_year emits the next track title.
        cases = [
            (7, "track_0", 0, "Get Ready Wiggle"),
            (8, "track_1", 1, "Rock-A-Bye Your Bear"),
            (9, "track_2", 2, "Dorothy the Dinosaur"),
            (10, "track_3", 3, "Mischief the Monkey"),
            (11, "exhausted", None, None),
        ]
        for attempts, expected_tag, expected_slice, expected_query in cases:
            with self.subTest(attempts=attempts, tag=expected_tag):
                v = select_variant(
                    search_attempts=attempts,
                    threshold=5,
                    base_query="*he *iggles",
                    base_query_unwild="The Wiggles",
                    year="1991",
                    track_titles=self.WIGGLES_TITLES,
                )
                self.assertEqual(v.tag, expected_tag)
                self.assertEqual(v.slice_index, expected_slice)
                self.assertEqual(v.query, expected_query)

    def test_year_unknown_skips_unwild_year_tier(self):
        # year=None: cycle threshold → unwild, threshold+1 → track_0
        v_unwild = select_variant(
            search_attempts=5,
            threshold=5,
            base_query="*ase",
            base_query_unwild="base",
            year=None,
            track_titles=self.WIGGLES_TITLES,
        )
        self.assertEqual(v_unwild.kind, "unwild")

        v_track0 = select_variant(
            search_attempts=6,
            threshold=5,
            base_query="*ase",
            base_query_unwild="base",
            year=None,
            track_titles=self.WIGGLES_TITLES,
        )
        self.assertEqual(v_track0.kind, "track")
        self.assertEqual(v_track0.tag, "track_0")
        self.assertEqual(v_track0.query, "Get Ready Wiggle")

    def test_year_0000_treated_as_unknown(self):
        # "0000" / "0000-00-00" → MB-fallback unknown → skip unwild_year.
        for bad_year in ("0000", "0000-00-00"):
            with self.subTest(year=bad_year):
                v = select_variant(
                    search_attempts=6,
                    threshold=5,
                    base_query="*ase",
                    base_query_unwild="base",
                    year=bad_year,
                    track_titles=self.WIGGLES_TITLES,
                )
                # threshold+1 with year unknown → track_0, not unwild_year.
                self.assertEqual(v.kind, "track")
                self.assertEqual(v.tag, "track_0")

    def test_malformed_year_strings_treated_as_unknown(self):
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
                    search_attempts=6,
                    threshold=5,
                    base_query="*ase",
                    base_query_unwild="base",
                    year=year,
                    track_titles=self.WIGGLES_TITLES,
                )
                self.assertEqual(v.kind, "track")
                self.assertEqual(v.tag, "track_0")

    def test_empty_tracks_with_year_unwild_then_unwild_year_then_exhausted(self):
        # No tracks but year known: unwild + unwild_year fire, then exhausted.
        v5 = select_variant(
            search_attempts=5,
            threshold=5,
            base_query="*ase",
            base_query_unwild="base",
            year="1991",
            track_titles=[],
        )
        self.assertEqual(v5.kind, "unwild")
        v6 = select_variant(
            search_attempts=6,
            threshold=5,
            base_query="*ase",
            base_query_unwild="base",
            year="1991",
            track_titles=[],
        )
        self.assertEqual(v6.kind, "unwild_year")
        v7 = select_variant(
            search_attempts=7,
            threshold=5,
            base_query="*ase",
            base_query_unwild="base",
            year="1991",
            track_titles=[],
        )
        self.assertEqual(v7.kind, "exhausted")

    def test_empty_tracks_no_year_unwild_then_exhausted(self):
        v5 = select_variant(
            search_attempts=5,
            threshold=5,
            base_query="*ase",
            base_query_unwild="base",
            year=None,
            track_titles=[],
        )
        self.assertEqual(v5.kind, "unwild")
        v6 = select_variant(
            search_attempts=6,
            threshold=5,
            base_query="*ase",
            base_query_unwild="base",
            year=None,
            track_titles=[],
        )
        self.assertEqual(v6.kind, "exhausted")

    def test_single_track_skips_track_tier(self):
        """Albums with one track skip the per-track tier entirely.

        Lone track-title queries match too many unrelated albums on Soulseek
        that happen to share a track name; the 0.15 distance gate lets them
        through. This was the rationale behind suppressing the old V4 tier
        for single-track albums and still applies to the new track tier.
        """
        # Cycle 5: unwild still useful (full-recall artist query).
        v5 = select_variant(
            search_attempts=5,
            threshold=5,
            base_query="*ase",
            base_query_unwild="base",
            year="1991",
            track_titles=["Lonely Track"],
        )
        self.assertEqual(v5.kind, "unwild")
        # Cycle 6: unwild_year still useful.
        v6 = select_variant(
            search_attempts=6,
            threshold=5,
            base_query="*ase",
            base_query_unwild="base",
            year="1991",
            track_titles=["Lonely Track"],
        )
        self.assertEqual(v6.kind, "unwild_year")
        # Cycle 7: would be track_0 — must short-circuit to exhausted.
        v7 = select_variant(
            search_attempts=7,
            threshold=5,
            base_query="*ase",
            base_query_unwild="base",
            year="1991",
            track_titles=["Lonely Track"],
        )
        self.assertEqual(v7.kind, "exhausted")
        self.assertIsNone(v7.query)
        self.assertEqual(v7.tag, "exhausted")

    def test_single_track_no_year_unwild_then_exhausted(self):
        # year=None, 1 track: unwild → exhausted (no unwild_year, no track tier).
        v5 = select_variant(
            search_attempts=5,
            threshold=5,
            base_query="*ase",
            base_query_unwild="base",
            year=None,
            track_titles=["Only Song"],
        )
        self.assertEqual(v5.kind, "unwild")
        v6 = select_variant(
            search_attempts=6,
            threshold=5,
            base_query="*ase",
            base_query_unwild="base",
            year=None,
            track_titles=["Only Song"],
        )
        self.assertEqual(v6.kind, "exhausted")

    def test_dedup_drops_repeated_titles_in_track_tier(self):
        # Wiggles tracklist has duplicate "Archie's Theme" — emit once only.
        titles = [
            "Get Ready to Wiggle",
            "Archie's Theme",
            "ARCHIE'S theme",
            "Glub Glub Train",
        ]
        cases = [
            (7, "track_0", "Get Ready Wiggle"),
            (8, "track_1", "Archie Theme"),
            (9, "track_2", "Glub Glub Train"),
            (10, "exhausted", None),
        ]
        for attempts, expected_tag, expected_query in cases:
            with self.subTest(attempts=attempts, tag=expected_tag):
                v = select_variant(
                    search_attempts=attempts,
                    threshold=5,
                    base_query="*ase",
                    base_query_unwild="base",
                    year="1991",
                    track_titles=titles,
                )
                self.assertEqual(v.tag, expected_tag)
                self.assertEqual(v.query, expected_query)

    def test_track_tag_format_exact(self):
        # Tag for track tier must be exactly "track_<idx>".
        titles = [f"Distinct{n:03d}xxxx" for n in range(20)]
        for idx in (0, 1, 2, 5, 17):
            with self.subTest(idx=idx):
                attempts = 5 + 2 + idx  # threshold + unwild + unwild_year + idx
                v = select_variant(
                    search_attempts=attempts,
                    threshold=5,
                    base_query="*ase",
                    base_query_unwild="base",
                    year="1991",
                    track_titles=titles,
                )
                self.assertEqual(v.kind, "track")
                self.assertEqual(v.tag, f"track_{idx}")
                self.assertEqual(v.slice_index, idx)

if __name__ == "__main__":
    unittest.main()
