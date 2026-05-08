"""Tests for search query builder."""

import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from lib.search import (
    build_query, strip_special_chars, strip_short_tokens,
    wildcard_artist_tokens, cap_tokens,
    _normalize_query_tokens, _per_track_queries, select_variant,
    generate_search_plan, ReleaseSnapshot, SearchPlanConfig,
    SearchPlan, SearchPlanItem,
    SEARCH_PLAN_GENERATOR_ID,
    PLAN_STATUS_SUCCESS, PLAN_STATUS_GENERATION_FAILED,
    MAX_TRACK_SLOTS_PER_PLAN,
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


class TestNormalizeQueryTokens(unittest.TestCase):

    def test_normalizes_tokens(self):
        cases = [
            ("empty", [], []),
            ("all_low_entropy", ["The", "you", "From", "and"], []),
            ("case_dedupes", ["Love", "love", "LOVE"], ["Love"]),
            ("stopword_and_dedupe", ["The", "Love", "love", "from"], ["Love"]),
            ("preserves_order", ["One", "Two", "one", "Three"], ["One", "Two", "Three"]),
        ]
        for name, tokens, expected in cases:
            with self.subTest(name=name):
                self.assertEqual(_normalize_query_tokens(tokens), expected)

    def test_can_preserve_all_low_entropy_identity(self):
        cases = [
            (["The", "the"], ["The"]),
            (["The", "You"], ["The", "You"]),
        ]
        for tokens, expected in cases:
            with self.subTest(tokens=tokens):
                self.assertEqual(
                    _normalize_query_tokens(
                        tokens,
                        preserve_all_low_entropy=True,
                    ),
                    expected,
                )


class TestBuildQuery(unittest.TestCase):

    def test_basic(self):
        q = build_query("The Mountain Goats", "Tallahassee")
        # "The" has too little search entropy and is stripped before wildcarding.
        self.assertEqual(q, "*ountain *oats Tallahassee")

    def test_beatles(self):
        q = build_query("The Beatles", "Abbey Road")
        self.assertEqual(q, "*eatles Abbey Road")

    def test_afi(self):
        q = build_query("AFI", "Sing the Sorrow")
        self.assertEqual(q, "*FI Sing Sorrow")

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

    def test_returns_none_when_title_normalization_empties_query(self):
        q = build_query("Various Artists", "The You")
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
        # peer clients.
        q = build_query("The Wiggles", "The Wiggles", wildcard_artist=False)
        self.assertEqual(q, "Wiggles")
        q = build_query("Duran Duran", "Duran Duran", wildcard_artist=False)
        self.assertEqual(q, "Duran")

    def test_wildcard_artist_false_non_self_titled(self):
        q = build_query(
            "The Beatles", "Abbey Road", wildcard_artist=False,
        )
        self.assertEqual(q, "Beatles Abbey Road")
        self.assertNotIn("*eatles", q or "")

    def test_low_entropy_title_tokens_dropped(self):
        q = build_query("Videotape", "The Moon")
        self.assertEqual(q, "*ideotape Moon")
        q = build_query("Turnstyle", "You Know")
        self.assertEqual(q, "*urnstyle Know")
        q = build_query("John & Jehn", "And Run")
        self.assertEqual(q, "*ohn *ehn Run")

    def test_repeated_title_tokens_deduped(self):
        q = build_query("Kanye West", "Love Love Love")
        self.assertEqual(q, "*anye *est Love")

    def test_all_low_entropy_artist_tokens_preserved(self):
        q = build_query("The The", "Soul Mining")
        self.assertEqual(q, "*he Soul Mining")
        q = build_query("You You", "Album")
        self.assertEqual(q, "*ou Album")


class TestPerTrackQueries(unittest.TestCase):
    """Per-track queries: cleaned title tokens, no wildcards.

    Each track title becomes one full query; no AND-mash across multiple
    tracks. The album-match scoring step (sub-count gate + filename ratio
    + cross-check) disambiguates wrong albums after slskd responses come
    back, so we want maximal recall per query. Single-token titles append
    one distinct literal artist token for entropy or are skipped.
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
            "Dorothy Dinosaur",
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
        self.assertEqual(out, ["Archie Theme", "Glub Train"])

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

    def test_enriches_single_token_titles_with_artist_token(self):
        # Bare one-word track searches like "Sweet" or "Tallahassee" are too
        # broad when the artist can add distinct entropy. Ties keep source
        # order, so "Dallas Crane" contributes "Dallas".
        out = _per_track_queries([
            "Sweet",
            "Twenty Four Seven",
            "Tallahassee",
            "Go",
        ], artist_name="Dallas Crane")
        self.assertEqual(out, [
            "Sweet Dallas",
            "Twenty Four Seven",
            "Tallahassee Dallas",
            "Go Dallas",
        ])

    def test_single_token_artist_entropy_prefers_longest_artist_token(self):
        out = _per_track_queries(["Tallahassee"], artist_name="The Mountain Goats")
        self.assertEqual(out, ["Tallahassee Mountain"])

    def test_low_entropy_track_tokens_are_dropped_before_enrichment(self):
        out = _per_track_queries([
            "You Know",
            "From You",
            "The Truth",
            "The Fall",
            "And Run",
        ], artist_name="The Beatles")
        self.assertEqual(out, [
            "Know Beatles",
            "Truth Beatles",
            "Fall Beatles",
            "Run Beatles",
        ])

    def test_repeated_track_tokens_are_deduped_before_enrichment(self):
        out = _per_track_queries(["Love Love Love"], artist_name="Big Thief")
        self.assertEqual(out, ["Love Thief"])

    def test_repeated_track_tokens_skip_when_no_artist_entropy_remains(self):
        out = _per_track_queries(["Lord Lord Lord"], artist_name="Ye")
        self.assertEqual(out, [])

    def test_repeated_track_tokens_skip_when_artist_is_same_token(self):
        out = _per_track_queries(["Love Love Love"], artist_name="Love")
        self.assertEqual(out, [])

    def test_single_token_artist_entropy_must_be_distinct(self):
        out = _per_track_queries(["Fall"], artist_name="The Fall")
        self.assertEqual(out, [])

    def test_single_token_track_skips_without_distinct_artist_entropy(self):
        out = _per_track_queries(["Moon"], artist_name="Various Artists")
        self.assertEqual(out, [])


class TestSelectVariant(unittest.TestCase):
    """Variant generator ladder — pure decision logic.

    New ladder (post-Wiggles-1991 forensics):
      cycle < threshold       → default     (wildcarded base)
      cycle == threshold      → unwild      (un-wildcarded base)
      cycle == threshold + 1  → unwild_year (un-wild base + year, if known)
      cycle == threshold + N  → track_<i>   (one track query per cycle)
      pool drained            → exhausted

    The wildcarded default form bypasses Soulseek's per-peer artist banlist
    but is silently dropped by many peer clients. The
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
                    base_query="*iggles",
                    base_query_unwild="Wiggles",
                    year="1991",
                    track_titles=self.WIGGLES_TITLES,
                )
                self.assertEqual(v.kind, "default")
                self.assertEqual(v.query, "*iggles")
                self.assertEqual(v.tag, "default")
                self.assertIsNone(v.slice_index)

    def test_unwild_at_threshold(self):
        # Cycle == threshold: emit the un-wildcarded base query.
        v = select_variant(
            search_attempts=5,
            threshold=5,
            base_query="*iggles",
            base_query_unwild="Wiggles",
            year="1991",
            track_titles=self.WIGGLES_TITLES,
        )
        self.assertEqual(v.kind, "unwild")
        self.assertEqual(v.query, "Wiggles")
        self.assertEqual(v.tag, "unwild")
        self.assertIsNone(v.slice_index)

    def test_unwild_year_at_threshold_plus_one(self):
        v = select_variant(
            search_attempts=6,
            threshold=5,
            base_query="*iggles",
            base_query_unwild="Wiggles",
            year="1991",
            track_titles=self.WIGGLES_TITLES,
        )
        self.assertEqual(v.kind, "unwild_year")
        self.assertEqual(v.query, "Wiggles 1991")
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
            base_query="*iggles",
            base_query_unwild="Wiggles",
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
            (9, "track_2", 2, "Dorothy Dinosaur"),
            (10, "track_3", 3, "Mischief Monkey"),
            (11, "exhausted", None, None),
        ]
        for attempts, expected_tag, expected_slice, expected_query in cases:
            with self.subTest(attempts=attempts, tag=expected_tag):
                v = select_variant(
                    search_attempts=attempts,
                    threshold=5,
                    base_query="*iggles",
                    base_query_unwild="Wiggles",
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
            (9, "track_2", "Glub Train"),
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

    def test_track_tier_enriches_single_token_titles(self):
        titles = [
            "Sweet",
            "Twenty Four Seven",
            "Go",
            "Drawn Together",
        ]
        cases = [
            (7, "track_0", "Sweet Dallas"),
            (8, "track_1", "Twenty Four Seven"),
            (9, "track_2", "Go Dallas"),
            (10, "track_3", "Drawn Together"),
            (11, "exhausted", None),
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
                    artist_name="Dallas Crane",
                )
                self.assertEqual(v.tag, expected_tag)
                self.assertEqual(v.query, expected_query)

    def test_track_tier_keeps_advancing_when_all_track_titles_are_one_word(self):
        titles = ["Sweet", "Go", "Sun", "Tallahassee"]
        cases = [
            (7, "track_0", "Sweet Dallas"),
            (8, "track_1", "Go Dallas"),
            (9, "track_2", "Sun Dallas"),
            (10, "track_3", "Tallahassee Dallas"),
            (11, "exhausted", None),
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
                    artist_name="Dallas Crane",
                )
                self.assertEqual(v.tag, expected_tag)
                self.assertEqual(v.query, expected_query)

    def test_track_tag_format_exact(self):
        # Tag for track tier must be exactly "track_<idx>".
        titles = [f"Distinct{n:03d}xxxx Song" for n in range(20)]
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

class TestGenerateSearchPlan(unittest.TestCase):
    """Pure search-plan generator (U2 — persisted search plans).

    Asserts full plan output, strategy ordering, canonical query keys,
    repeat groups, omitted candidates, dedupe provenance, low-entropy
    drop recording, generation-failure path, and the generator id
    constant.
    """

    WIGGLES_TITLES = (
        "Get Ready to Wiggle",
        "Rock-A-Bye Your Bear",
        "Dorothy the Dinosaur",
        "Mischief the Monkey",
    )

    def _cfg(self, threshold: int = 5, max_track_slots: int = 3) -> SearchPlanConfig:
        return SearchPlanConfig(
            escalation_threshold=threshold,
            max_track_slots=max_track_slots,
        )

    def _snapshot(
        self,
        *,
        artist: str = "The Wiggles",
        title: str = "The Wiggles",
        year: str | None = "1991",
        track_titles: tuple[str, ...] = WIGGLES_TITLES,
        redownload: bool = False,
        prepend_artist: bool = True,
    ) -> ReleaseSnapshot:
        return ReleaseSnapshot(
            artist_name=artist,
            title=title,
            year=year,
            track_titles=track_titles,
            redownload=redownload,
            prepend_artist=prepend_artist,
        )

    # --- happy path: typical multi-track release with year -----------------

    def test_typical_release_emits_full_ladder(self):
        plan = generate_search_plan(self._snapshot(), self._cfg(threshold=5))

        self.assertIsInstance(plan, SearchPlan)
        self.assertEqual(plan.status, PLAN_STATUS_SUCCESS)
        self.assertIsNone(plan.failure_reason)
        self.assertEqual(plan.generator_id, SEARCH_PLAN_GENERATOR_ID)

        # 5 default + 1 unwild + 1 unwild_year + 3 track = 10 slots
        self.assertEqual(len(plan.items), 10)

        # Slot ordering and strategies
        strategies = [it.strategy for it in plan.items]
        self.assertEqual(strategies, [
            "default", "default", "default", "default", "default",
            "unwild",
            "unwild_year",
            "track_0", "track_1", "track_2",
        ])

        # Ordinals are 0..9 contiguous
        self.assertEqual([it.ordinal for it in plan.items], list(range(10)))

        # Default repeats share repeat group; non-defaults have own group
        self.assertEqual(
            [it.repeat_group for it in plan.items[:5]],
            ["default"] * 5,
        )
        self.assertEqual(plan.items[5].repeat_group, "unwild")
        self.assertEqual(plan.items[6].repeat_group, "unwild_year")
        self.assertEqual(plan.items[7].repeat_group, "track_0")

        # All items have non-empty queries and canonical keys
        for it in plan.items:
            self.assertTrue(it.query)
            self.assertEqual(it.canonical_query_key, " ".join(it.query.lower().split()))

        # Repeated-default queries are identical and share canonical key
        default_keys = {it.canonical_query_key for it in plan.items[:5]}
        self.assertEqual(len(default_keys), 1)

        # Default = wildcarded base; unwild = un-wildcarded.
        self.assertEqual(plan.items[0].query, "*iggles")
        self.assertEqual(plan.items[5].query, "Wiggles")
        self.assertEqual(plan.items[6].query, "Wiggles 1991")

    def test_repeated_default_repeat_index_in_provenance(self):
        plan = generate_search_plan(self._snapshot(), self._cfg(threshold=3))
        # First three slots are defaults with repeat_index 0/1/2
        repeat_indexes = [
            it.provenance.get("repeat_index") for it in plan.items[:3]
        ]
        self.assertEqual(repeat_indexes, [0, 1, 2])

    # --- year unknown ------------------------------------------------------

    def test_unknown_year_skips_unwild_year_and_records_omission(self):
        plan = generate_search_plan(
            self._snapshot(year=None), self._cfg(threshold=2),
        )
        self.assertEqual(plan.status, PLAN_STATUS_SUCCESS)
        strategies = [it.strategy for it in plan.items]
        # 2 default + 1 unwild + 3 track. unwild_year omitted.
        self.assertEqual(strategies, [
            "default", "default", "unwild",
            "track_0", "track_1", "track_2",
        ])
        omitted = plan.provenance["omitted_candidates"]
        unwild_year_omits = [o for o in omitted if o["strategy"] == "unwild_year"]
        self.assertEqual(len(unwild_year_omits), 1)
        self.assertEqual(unwild_year_omits[0]["reason"], "year_unknown")

    def test_year_0000_treated_as_unknown(self):
        plan = generate_search_plan(
            self._snapshot(year="0000"), self._cfg(threshold=1),
        )
        strategies = [it.strategy for it in plan.items]
        self.assertNotIn("unwild_year", strategies)

    # --- single-track album skips track tier ------------------------------

    def test_single_track_album_has_no_track_slots(self):
        plan = generate_search_plan(
            self._snapshot(track_titles=("Lonely Track",)),
            self._cfg(threshold=2),
        )
        self.assertEqual(plan.status, PLAN_STATUS_SUCCESS)
        strategies = [it.strategy for it in plan.items]
        self.assertEqual(strategies, ["default", "default", "unwild", "unwild_year"])
        # Skip recorded in provenance
        omitted = plan.provenance["omitted_candidates"]
        self.assertTrue(
            any(o["strategy"] == "track_*" and o["reason"] == "single_track_album"
                for o in omitted),
            f"missing single_track_album omission: {omitted!r}",
        )

    # --- empty tracklist still emits album-level slots --------------------

    def test_empty_tracklist_still_emits_album_slots(self):
        plan = generate_search_plan(
            self._snapshot(track_titles=()),
            self._cfg(threshold=1),
        )
        self.assertEqual(plan.status, PLAN_STATUS_SUCCESS)
        strategies = [it.strategy for it in plan.items]
        self.assertEqual(strategies, ["default", "unwild", "unwild_year"])
        # No track-tier omission record (no tracks at all)
        omitted = plan.provenance["omitted_candidates"]
        self.assertFalse(
            any(o.get("strategy") == "track_*" for o in omitted),
            f"unexpected track omissions for empty tracklist: {omitted!r}",
        )

    # --- all-low-entropy artist preserves identity fallback --------------

    def test_all_low_entropy_artist_preserves_identity(self):
        plan = generate_search_plan(
            self._snapshot(
                artist="The The", title="Soul Mining", track_titles=(),
                year="1983",
            ),
            self._cfg(threshold=1),
        )
        # Default query mirrors build_query("The The", "Soul Mining"):
        # "*he Soul Mining" — artist identity preserved despite "the" being
        # low-entropy.
        self.assertEqual(plan.status, PLAN_STATUS_SUCCESS)
        self.assertEqual(plan.items[0].query, "*he Soul Mining")
        self.assertEqual(plan.items[1].query, "The Soul Mining")

    # --- low-entropy token drop recording --------------------------------

    def test_low_entropy_tokens_dropped_and_recorded(self):
        plan = generate_search_plan(
            ReleaseSnapshot(
                artist_name="John & Jehn",
                title="And Run From The You",
                year="2000",
                track_titles=("Truth From You",),
                prepend_artist=True,
            ),
            self._cfg(threshold=1),
        )
        dropped = plan.provenance["dropped_low_entropy_tokens"]
        # All four bannable tokens appeared in the inputs; sorted set reported.
        self.assertEqual(set(dropped), {"and", "from", "the", "you"})
        # Default query has no low-entropy tokens.
        for token in ("And", "From", "The", "You", "and", "from", "the", "you"):
            self.assertNotIn(token, plan.items[0].query.split())

    # --- repeated tokens within a query collapse before canonicalization -

    def test_repeated_tokens_within_query_collapse(self):
        plan = generate_search_plan(
            self._snapshot(
                artist="Kanye West",
                title="Love Love Love",
                track_titles=(),
                year=None,
            ),
            self._cfg(threshold=1),
        )
        # build_query already dedupes "Love Love Love" → "Love" inside title.
        self.assertEqual(plan.items[0].query, "*anye *est Love")
        # Canonical key reflects the deduped form.
        self.assertEqual(plan.items[0].canonical_query_key, "*anye *est love")

    # --- cross-strategy dedupe: keep first, record loser -----------------

    def test_cross_strategy_duplicate_keeps_first_records_loser(self):
        # When wildcarded and un-wildcarded queries coincide (e.g. an artist
        # with no wildcardable tokens), unwild_year still differs because of
        # the year suffix. To force a same-key collision across strategies,
        # use a release where the unwild and the unwild_year base differ
        # only by year, but force a track query that collides with the
        # default base.
        #
        # Simpler construction: two strategies with identical canonical
        # query. We exploit the fact that build_query returns the same string
        # for an artist with no wildcardable content. Use a Various-Artists
        # release whose title is a single distinct token; then a track named
        # exactly that single title produces the same canonical query.
        plan = generate_search_plan(
            ReleaseSnapshot(
                artist_name="Various Artists",
                title="Compilation",
                year="2010",
                track_titles=(
                    "Compilation",  # collides with default canonical key
                    "Other Track Title Here",
                    "Yet Another Distinct Track",
                ),
                prepend_artist=True,
            ),
            self._cfg(threshold=1),
        )
        # default = "Compilation"; unwild = "Compilation"; identical canonical key
        # so unwild is deduped against default.
        self.assertEqual(plan.items[0].strategy, "default")
        self.assertEqual(plan.items[0].canonical_query_key, "compilation")
        strategies = [it.strategy for it in plan.items]
        # unwild MUST be deduped (it'd be identical to "compilation")
        self.assertNotIn("unwild", strategies)
        # The dedupe loser is recorded in provenance with the ordinal it
        # would have taken (1 — immediately after the single default).
        losers = plan.provenance["dedupe_losers"]
        self.assertTrue(
            any(L["winner_strategy"] == "default"
                and L["loser_strategy"] == "unwild"
                and L["canonical_query_key"] == "compilation"
                and L["would_have_been_ordinal"] == 1
                for L in losers),
            f"dedupe loser not recorded: {losers!r}",
        )

    # --- track ranking ties break by source-track order ------------------

    def test_track_ranking_ties_break_by_source_order(self):
        # All four tracks have identical useful-token count AND identical
        # char count after cleaning, so the tiebreaker is source-track order.
        # Two tokens of length 5 + space = 11 chars each.
        plan = generate_search_plan(
            self._snapshot(
                artist="Dallas Crane",
                title="Album",
                year=None,
                track_titles=(
                    "Alpha Sigma",   # 11 chars, 2 tokens
                    "Bravo Delta",   # 11 chars, 2 tokens
                    "Echo1 Foxxx",   # 11 chars, 2 tokens
                    "Golfa Hotel",   # 11 chars, 2 tokens
                ),
            ),
            self._cfg(threshold=1, max_track_slots=3),
        )
        track_items = [it for it in plan.items if it.strategy.startswith("track_")]
        # Sanity: same token count, same char count → source-order
        for it in track_items:
            self.assertEqual(len(it.query.split()), 2)
            self.assertEqual(len(it.query), 11)
        self.assertEqual(len(track_items), 3)
        self.assertEqual(track_items[0].query, "Alpha Sigma")
        self.assertEqual(track_items[1].query, "Bravo Delta")
        self.assertEqual(track_items[2].query, "Echo1 Foxxx")
        # source_track_index recorded in provenance
        self.assertEqual(track_items[0].provenance["source_track_index"], 0)
        self.assertEqual(track_items[1].provenance["source_track_index"], 1)
        self.assertEqual(track_items[2].provenance["source_track_index"], 2)
        # The fourth track must appear in plan-level omissions.
        omitted = plan.provenance["omitted_candidates"]
        excess = [o for o in omitted if o.get("strategy") == "track_excess"]
        self.assertEqual(len(excess), 1)
        self.assertEqual(excess[0]["source_track_index"], 3)
        self.assertEqual(excess[0]["query"], "Golfa Hotel")

    def test_track_ranking_orders_by_useful_tokens_then_chars(self):
        # Make tracks with different post-clean shapes to exercise the rank
        # order: useful-token count desc, then char count desc, then source
        # index asc. No artist enrichment happens here — every cleaned track
        # has >=2 tokens — so the ranking inputs are predictable.
        plan = generate_search_plan(
            self._snapshot(
                artist="Some Artist",
                title="Album",
                year=None,
                track_titles=(
                    "Aaa Bbbbbbb",                  # 2 tokens, 11 chars
                    "One Two Three Four Tokens",    # 4 tokens (capped from 5)
                    "Aaaaa Bbbbbbbbbbb",            # 2 tokens, 17 chars
                    "One Two",                      # 2 tokens, 7 chars
                ),
            ),
            self._cfg(threshold=1, max_track_slots=3),
        )
        track_items = [it for it in plan.items if it.strategy.startswith("track_")]
        self.assertEqual(len(track_items), 3)
        # First slot is the 4-token query (most useful tokens).
        self.assertEqual(track_items[0].provenance["source_track_index"], 1)
        self.assertEqual(len(track_items[0].query.split()), 4)
        # Among 2-token queries, the longer wins on char count, then ties
        # break by source index. 17-char wins, then 11-char wins.
        self.assertEqual(track_items[1].provenance["source_track_index"], 2)
        self.assertEqual(track_items[2].provenance["source_track_index"], 0)

    # --- generation failure: no runnable query ---------------------------

    def test_no_runnable_query_returns_deterministic_failure(self):
        # Empty artist + empty title + empty tracks → no candidates runnable.
        plan = generate_search_plan(
            ReleaseSnapshot(
                artist_name="",
                title="",
                year=None,
                track_titles=(),
                prepend_artist=True,
            ),
            self._cfg(threshold=3),
        )
        self.assertEqual(plan.status, PLAN_STATUS_GENERATION_FAILED)
        self.assertEqual(plan.failure_reason, "no_runnable_query")
        self.assertEqual(plan.items, ())
        # Provenance still populated with omitted candidates and snapshot
        # signature, so failed plans are debuggable.
        self.assertGreater(len(plan.provenance["omitted_candidates"]), 0)
        sig = plan.provenance["snapshot_signature"]
        self.assertEqual(sig["artist_name"], "")
        self.assertEqual(sig["title"], "")
        self.assertEqual(sig["track_count"], 0)
        self.assertFalse(sig["redownload"])

    def test_unrunnable_album_with_runnable_track_still_succeeds(self):
        # Empty artist+title but two distinct track candidates with multi-token
        # queries → multi-track album, track tier produces runnable items so
        # the plan succeeds even without album-level slots.
        plan = generate_search_plan(
            ReleaseSnapshot(
                artist_name="",
                title="",
                year=None,
                track_titles=("Distinct Track Title", "Another Real Song"),
                prepend_artist=True,
            ),
            self._cfg(threshold=2),
        )
        self.assertEqual(plan.status, PLAN_STATUS_SUCCESS)
        # Only track slots; default and unwild candidates omitted.
        strategies = [it.strategy for it in plan.items]
        self.assertEqual(strategies, ["track_0", "track_1"])
        omitted = plan.provenance["omitted_candidates"]
        self.assertTrue(
            any(o["strategy"] == "default" and o["reason"] == "empty_base_query"
                for o in omitted),
            f"missing default empty omission: {omitted!r}",
        )
        self.assertTrue(
            any(o["strategy"] == "unwild" and o["reason"] == "empty_unwild_query"
                for o in omitted),
            f"missing unwild empty omission: {omitted!r}",
        )

    # --- redownload flag preserved in snapshot signature -----------------

    def test_redownload_flag_preserved_in_snapshot_signature(self):
        plan = generate_search_plan(
            self._snapshot(redownload=True), self._cfg(threshold=1),
        )
        sig = plan.provenance["snapshot_signature"]
        self.assertTrue(sig["redownload"])

    # --- generator id contract test --------------------------------------

    def test_generator_id_constant_is_pinned(self):
        """Changing generator output requires bumping `SEARCH_PLAN_GENERATOR_ID`.

        This explicit assertion is the contract: if the generator's behavior
        changes (token rules, ladder, repeat groups, dedupe, provenance
        shape), the test author MUST also bump the id constant. U3
        (service) and U4 (reconciliation) read this constant — drift
        breaks the persisted plan currentness check.
        """
        # Snapshot output of a known release. Bumping any of these
        # expectations should require the id below to change too.
        plan = generate_search_plan(self._snapshot(), self._cfg(threshold=2))
        self.assertEqual(plan.generator_id, "search-plan/2026-05-08-1")
        self.assertEqual(plan.generator_id, SEARCH_PLAN_GENERATOR_ID)
        # Pin the slot ladder shape and queries.
        self.assertEqual(
            [(it.strategy, it.query) for it in plan.items],
            [
                ("default", "*iggles"),
                ("default", "*iggles"),
                ("unwild", "Wiggles"),
                ("unwild_year", "Wiggles 1991"),
                ("track_0", "Rock-A-Bye Your Bear"),
                ("track_1", "Get Ready Wiggle"),
                ("track_2", "Dorothy Dinosaur"),
            ],
        )

    # --- structural sanity ------------------------------------------------

    def test_all_items_runnable_and_have_canonical_keys(self):
        plan = generate_search_plan(self._snapshot(), self._cfg(threshold=2))
        self.assertEqual(plan.status, PLAN_STATUS_SUCCESS)
        for it in plan.items:
            self.assertIsInstance(it, SearchPlanItem)
            self.assertTrue(it.query, f"empty query at ordinal {it.ordinal}")
            self.assertTrue(it.canonical_query_key)
            self.assertTrue(it.repeat_group)
            self.assertEqual(
                it.canonical_query_key,
                " ".join(it.query.lower().split()),
            )

    def test_max_track_slots_constant_default(self):
        # Sanity guard so we don't accidentally drift the public default.
        self.assertEqual(MAX_TRACK_SLOTS_PER_PLAN, 3)
        self.assertEqual(SearchPlanConfig().max_track_slots, 3)


if __name__ == "__main__":
    unittest.main()
