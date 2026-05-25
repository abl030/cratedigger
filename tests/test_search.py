"""Tests for search query builder."""

import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from lib.search import (
    strip_special_chars,
    strip_stopwords,
    STOPWORDS,
    wildcard_artist_tokens, cap_tokens,
    _normalize_query_tokens,
    generate_search_plan, ReleaseSnapshot, SearchPlanConfig,
    SearchPlan, SearchPlanItem,
    SEARCH_PLAN_GENERATOR_ID,
    PLAN_STATUS_SUCCESS, PLAN_STATUS_GENERATION_FAILED,
    MAX_TRACK_SLOTS_PER_PLAN,
    score_track_distinctiveness,
    GENERIC_TITLE_TOKENS,
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


class TestStripStopwords(unittest.TestCase):
    """Behavior regression for the single canonical stopword helper.

    Live observed strings, not synthetic edge cases. See U6 of the
    search-plan iter2 plan: any future change to the stopword set or
    the strip helper MUST keep these passing or call out the change
    explicitly in the PR.
    """

    CASES = [
        ("drops_the", ["the", "beatles"], ["beatles"]),
        ("case_insensitive", ["The", "Beatles"], ["Beatles"]),
        # "to" and "a" are NOT in STOPWORDS — verifies the set stays narrow.
        (
            "preserves_non_stopwords",
            ["how", "to", "disappear", "completely"],
            ["how", "to", "disappear", "completely"],
        ),
        (
            "preserves_a_and_yourself",
            ["have", "yourself", "a", "merry", "little", "christmas"],
            ["have", "yourself", "a", "merry", "little", "christmas"],
        ),
        ("empty", [], []),
        ("all_stopwords", ["the", "and"], []),
        ("mixed", ["The", "Love", "from", "Above"], ["Love", "Above"]),
    ]

    def test_strip_cases(self):
        for name, tokens, expected in self.CASES:
            with self.subTest(name=name):
                self.assertEqual(strip_stopwords(tokens), expected)

    def test_stopwords_set_is_narrow(self):
        """Locks the live shape: 4 entries, no surprise additions."""
        self.assertEqual(STOPWORDS, frozenset({"the", "you", "from", "and"}))


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


class TestGenerateSearchPlan(unittest.TestCase):
    """Pure search-plan generator (U5 of search-plan-entropy).

    Asserts full plan output, strategy ordering, canonical query keys,
    repeat groups, omitted candidates, dedupe provenance, low-entropy
    drop recording, generation-failure path, self-titled mix, the
    conditional release-group-year slot, and the generator id constant.
    """

    KID_A_TITLES = (
        "Everything in Its Right Place",
        "Kid A",
        "The National Anthem",
        "How to Disappear Completely",
        "Treefingers",
        "Optimistic",
    )

    def _cfg(self, threshold: int = 5, max_track_slots: int = 4) -> SearchPlanConfig:
        # ``escalation_threshold`` is preserved on SearchPlanConfig for
        # backwards-compat but U5 collapsed default repetition to a single
        # slot; the threshold value no longer affects slot count.
        # PR2 U8 (R3): max_track_slots default bumped from 3 to 4 so
        # the non-VA mix emits track_0_artist through track_3_artist.
        return SearchPlanConfig(
            escalation_threshold=threshold,
            max_track_slots=max_track_slots,
        )

    def _snapshot(
        self,
        *,
        artist: str = "Radiohead",
        title: str = "Kid A",
        year: str | None = "2008",
        track_titles: tuple[str, ...] = KID_A_TITLES,
        redownload: bool = False,
        prepend_artist: bool = True,
        release_group_year: int | None = 2000,
    ) -> ReleaseSnapshot:
        return ReleaseSnapshot(
            artist_name=artist,
            title=title,
            year=year,
            track_titles=track_titles,
            redownload=redownload,
            prepend_artist=prepend_artist,
            release_group_year=release_group_year,
        )

    # --- happy path: typical multi-track release with year + rg_year ---

    def test_typical_release_emits_full_ladder(self):
        plan = generate_search_plan(self._snapshot(), self._cfg())

        self.assertIsInstance(plan, SearchPlan)
        self.assertEqual(plan.status, PLAN_STATUS_SUCCESS)
        self.assertIsNone(plan.failure_reason)
        self.assertEqual(plan.generator_id, SEARCH_PLAN_GENERATOR_ID)

        # PR2 U8: literal_lossless dropped (R1); catalog_number omitted
        # here because the fixture has no catalog_number set; max track
        # slots bumped from 3 to 4 (R3).
        # 1 default + 1 literal + 1 literal_flac + 1 unwild_year
        # + 1 unwild_rg_year + 4 track = 9 slots
        strategies = [it.strategy for it in plan.items]
        self.assertEqual(strategies, [
            "default",
            "literal",
            "literal_flac",
            "unwild_year",
            "unwild_rg_year",
            "track_0_artist",
            "track_1_artist",
            "track_2_artist",
            "track_3_artist",
        ])

        # Ordinals contiguous from 0
        self.assertEqual(
            [it.ordinal for it in plan.items],
            list(range(len(plan.items))),
        )

        # Repeat group equals strategy for every non-default slot, and
        # each slot's repeat group is its own (no more shared default
        # repeat group).
        for it in plan.items:
            self.assertEqual(it.repeat_group, it.strategy)

        # All items runnable + canonical keys consistent.
        for it in plan.items:
            self.assertTrue(it.query)
            self.assertEqual(
                it.canonical_query_key,
                " ".join(it.query.lower().split()),
            )

        # Concrete query shapes for Radiohead / Kid A / 2008 / rg=2000.
        by_strategy = {it.strategy: it.query for it in plan.items}
        self.assertEqual(by_strategy["default"], "*adiohead Kid A")
        self.assertEqual(by_strategy["literal"], "Radiohead Kid A")
        self.assertEqual(by_strategy["literal_flac"], "Radiohead Kid A FLAC")
        self.assertNotIn("literal_lossless", by_strategy)
        self.assertEqual(by_strategy["unwild_year"], "Radiohead Kid A 2008")
        self.assertEqual(by_strategy["unwild_rg_year"], "Radiohead Kid A 2000")
        # Artist-prepended track slots — wildcarded artist token.
        self.assertTrue(
            by_strategy["track_0_artist"].startswith("*adiohead "),
            by_strategy["track_0_artist"],
        )

    def test_no_more_five_slot_default_repetition(self):
        """U5 R4: the five-slot default repetition is gone — one default only."""
        plan = generate_search_plan(self._snapshot(), self._cfg(threshold=5))
        default_slots = [
            it for it in plan.items if it.strategy == "default"
        ]
        self.assertEqual(len(default_slots), 1)
        # ``repeat_index`` provenance no longer recorded (no repetition).
        self.assertNotIn("repeat_index", default_slots[0].provenance)

    # --- year unknown ------------------------------------------------------

    def test_unknown_year_skips_unwild_year_and_records_omission(self):
        plan = generate_search_plan(
            self._snapshot(year=None, release_group_year=None),
            self._cfg(),
        )
        self.assertEqual(plan.status, PLAN_STATUS_SUCCESS)
        strategies = [it.strategy for it in plan.items]
        self.assertNotIn("unwild_year", strategies)
        self.assertNotIn("unwild_rg_year", strategies)
        omitted = plan.provenance["omitted_candidates"]
        unwild_year_omits = [
            o for o in omitted if o["strategy"] == "unwild_year"
        ]
        self.assertEqual(len(unwild_year_omits), 1)
        self.assertEqual(unwild_year_omits[0]["reason"], "year_unknown")

    def test_year_0000_treated_as_unknown(self):
        plan = generate_search_plan(
            self._snapshot(year="0000"), self._cfg(),
        )
        strategies = [it.strategy for it in plan.items]
        self.assertNotIn("unwild_year", strategies)

    # --- release-group year ----------------------------------------------

    def test_rg_year_slot_emitted_when_differs_from_year(self):
        plan = generate_search_plan(
            self._snapshot(year="2008", release_group_year=2000),
            self._cfg(),
        )
        strategies = [it.strategy for it in plan.items]
        self.assertIn("unwild_rg_year", strategies)
        by_strategy = {it.strategy: it.query for it in plan.items}
        self.assertEqual(by_strategy["unwild_rg_year"], "Radiohead Kid A 2000")
        # Provenance carries rg_year for debuggability.
        rg_item = next(it for it in plan.items
                       if it.strategy == "unwild_rg_year")
        self.assertEqual(rg_item.provenance.get("release_group_year"), 2000)
        # Plan-level provenance records rg_year too.
        self.assertEqual(plan.provenance.get("release_group_year"), 2000)

    def test_rg_year_slot_omitted_when_matches_year(self):
        plan = generate_search_plan(
            self._snapshot(year="2010", release_group_year=2010),
            self._cfg(),
        )
        strategies = [it.strategy for it in plan.items]
        self.assertNotIn("unwild_rg_year", strategies)
        omitted = plan.provenance["omitted_candidates"]
        self.assertTrue(
            any(o["strategy"] == "unwild_rg_year"
                and o["reason"] == "release_group_year_matches_year"
                for o in omitted),
            f"missing rg_year matches omission: {omitted!r}",
        )

    def test_rg_year_slot_omitted_when_rg_year_missing(self):
        plan = generate_search_plan(
            self._snapshot(release_group_year=None),
            self._cfg(),
        )
        strategies = [it.strategy for it in plan.items]
        self.assertNotIn("unwild_rg_year", strategies)
        omitted = plan.provenance["omitted_candidates"]
        self.assertTrue(
            any(o["strategy"] == "unwild_rg_year"
                and o["reason"] == "release_group_year_unknown"
                for o in omitted),
        )
        self.assertNotIn("release_group_year", plan.provenance)

    def test_rg_year_slot_omitted_when_year_unknown(self):
        plan = generate_search_plan(
            self._snapshot(year=None, release_group_year=2000),
            self._cfg(),
        )
        strategies = [it.strategy for it in plan.items]
        self.assertNotIn("unwild_rg_year", strategies)

    # --- format-hint slots unconditional ---------------------------------

    def test_format_hint_slots_present_when_years_match(self):
        plan = generate_search_plan(
            self._snapshot(
                artist="Darren Hanlon",
                title="I Will Love You at All",
                year="2010",
                release_group_year=2010,
            ),
            self._cfg(),
        )
        strategies = [it.strategy for it in plan.items]
        self.assertIn("literal_flac", strategies)
        # PR2 U8 (R1): literal_lossless retired — must not appear.
        self.assertNotIn("literal_lossless", strategies)
        self.assertNotIn("unwild_rg_year", strategies)

    # --- short-token drop removed ----------------------------------------

    def test_short_tokens_preserved_in_title(self):
        """Kid A's 'A' must survive; previously short-drop removed it."""
        plan = generate_search_plan(self._snapshot(), self._cfg())
        by_strategy = {it.strategy: it.query for it in plan.items}
        # Literal slot preserves the bare 'A'.
        self.assertIn(" A", by_strategy["literal"])
        self.assertEqual(by_strategy["literal"], "Radiohead Kid A")

    def test_short_tokens_preserved_for_bon_iver_numeric(self):
        plan = generate_search_plan(
            self._snapshot(
                artist="Bon Iver",
                title="22, a Million",
                track_titles=(),
                year="2016",
                release_group_year=2016,
            ),
            self._cfg(),
        )
        by_strategy = {it.strategy: it.query for it in plan.items}
        # 22 (2 chars) and a (1 char) must both survive the post-clean
        # pipeline. The order is preserved (source order), cap=4.
        self.assertEqual(by_strategy["default"], "*on *ver 22 Million")
        # Literal slot keeps the same body shape (un-wildcarded).
        self.assertEqual(by_strategy["literal"], "Bon Iver 22 Million")

    # --- self-titled detection -------------------------------------------

    def test_selftitled_release_uses_dedicated_mix(self):
        plan = generate_search_plan(
            ReleaseSnapshot(
                artist_name="Willow",
                title="Willow",
                year="2007",
                track_titles=(
                    "And Finally I Can Breathe",
                    "When the Sea Called Our Names",
                    "Stay Forever",
                    "Going Going Gone",
                ),
                prepend_artist=True,
                release_group_year=2007,
            ),
            self._cfg(),
        )
        self.assertEqual(plan.status, PLAN_STATUS_SUCCESS)
        strategies = [it.strategy for it in plan.items]
        # No default / literal / literal_flac / unwild_year slots — they
        # would collapse to bare artist for self-titled releases.
        for s in ("default", "literal", "literal_flac",
                  "literal_lossless", "unwild_year", "unwild_rg_year"):
            self.assertNotIn(s, strategies)
        # Dedicated selftitled slots emitted.
        self.assertIn("selftitled_artist_track_0", strategies)
        self.assertIn("selftitled_artist_track_0_flac", strategies)
        self.assertIn("selftitled_artist_year", strategies)
        # Concrete queries.
        by_strategy = {it.strategy: it.query for it in plan.items}
        # First selftitled track query = literal artist + literal track,
        # capped to MAX_SEARCH_TOKENS (longest first). "And"/"I" survive
        # short-drop removal but cap may drop them.
        self.assertTrue(
            by_strategy["selftitled_artist_track_0"].startswith("Willow"),
            by_strategy["selftitled_artist_track_0"],
        )
        self.assertIn("FLAC", by_strategy["selftitled_artist_track_0_flac"])
        self.assertEqual(by_strategy["selftitled_artist_year"], "Willow 2007")
        # Provenance flag set.
        self.assertTrue(plan.provenance.get("selftitled"))

    def test_selftitled_token_subset_mountains_case(self):
        """Mountains / Mountains Mountains Mountains → both normalize to {mountains}."""
        plan = generate_search_plan(
            ReleaseSnapshot(
                artist_name="Mountains",
                title="Mountains Mountains Mountains",
                year="2009",
                track_titles=(
                    "Bountiful Spreading",
                    "Telescope",
                    "Sheets Two",
                ),
                prepend_artist=True,
                release_group_year=2009,
            ),
            self._cfg(),
        )
        self.assertTrue(plan.provenance.get("selftitled"))
        strategies = [it.strategy for it in plan.items]
        self.assertNotIn("default", strategies)
        self.assertIn("selftitled_artist_track_0", strategies)

    def test_selftitled_negative_willow_tree(self):
        """Willow / Willow Tree → title has 'tree'; NOT self-titled."""
        plan = generate_search_plan(
            ReleaseSnapshot(
                artist_name="Willow",
                title="Willow Tree",
                year="2007",
                track_titles=("Branches", "Roots", "Leaves"),
                prepend_artist=True,
                release_group_year=2007,
            ),
            self._cfg(),
        )
        self.assertFalse(plan.provenance.get("selftitled"))
        strategies = [it.strategy for it in plan.items]
        self.assertIn("default", strategies)
        self.assertIn("literal", strategies)

    def test_selftitled_case_insensitive(self):
        plan = generate_search_plan(
            ReleaseSnapshot(
                artist_name="WILLOW",
                title="Willow",
                year="2007",
                track_titles=("Sample A", "Sample B"),
                prepend_artist=True,
            ),
            self._cfg(),
        )
        self.assertTrue(plan.provenance.get("selftitled"))

    # --- single-track album skips track tier ------------------------------

    def test_single_track_album_has_no_track_slots(self):
        plan = generate_search_plan(
            self._snapshot(track_titles=("Lonely Track",)),
            self._cfg(),
        )
        self.assertEqual(plan.status, PLAN_STATUS_SUCCESS)
        strategies = [it.strategy for it in plan.items]
        # No track_* slots for single-track albums.
        self.assertFalse(any(s.startswith("track_") for s in strategies))
        # Album-level slots still present.
        self.assertIn("default", strategies)
        self.assertIn("literal", strategies)
        # Skip recorded in provenance.
        omitted = plan.provenance["omitted_candidates"]
        self.assertTrue(
            any(o["strategy"] == "track_*"
                and o["reason"] == "single_track_album"
                for o in omitted),
            f"missing single_track_album omission: {omitted!r}",
        )

    def test_fewer_than_three_tracks_emits_fewer_track_slots(self):
        plan = generate_search_plan(
            self._snapshot(
                track_titles=("First Song Title", "Second Track Name"),
            ),
            self._cfg(),
        )
        track_slots = [
            it for it in plan.items if it.strategy.startswith("track_")
        ]
        self.assertEqual(len(track_slots), 2)

    # --- empty tracklist still emits album-level slots --------------------

    def test_empty_tracklist_still_emits_album_slots(self):
        plan = generate_search_plan(
            self._snapshot(track_titles=()),
            self._cfg(),
        )
        self.assertEqual(plan.status, PLAN_STATUS_SUCCESS)
        strategies = [it.strategy for it in plan.items]
        self.assertIn("default", strategies)
        self.assertIn("literal", strategies)
        self.assertIn("literal_flac", strategies)
        # PR2 U8 (R1): literal_lossless retired — must not appear.
        self.assertNotIn("literal_lossless", strategies)
        self.assertIn("unwild_year", strategies)
        # No track slots and no track-tier omission record for empty.
        self.assertFalse(any(s.startswith("track_") for s in strategies))
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
        # Various Artists collapses to no wildcardable artist tokens, so
        # the default slot is just "Compilation"; the literal slot is
        # also "Compilation" — identical canonical key. The literal slot
        # is deduped against the default slot.
        plan = generate_search_plan(
            ReleaseSnapshot(
                artist_name="Various Artists",
                title="Compilation",
                year="2010",
                track_titles=(
                    "Other Track Title Here",
                    "Yet Another Distinct Track",
                ),
                prepend_artist=True,
                release_group_year=2010,
            ),
            self._cfg(),
        )
        # default = "Compilation"; literal = "Compilation"; identical
        # canonical key so literal is deduped against default.
        self.assertEqual(plan.items[0].strategy, "default")
        self.assertEqual(plan.items[0].canonical_query_key, "compilation")
        strategies = [it.strategy for it in plan.items]
        self.assertNotIn("literal", strategies)
        losers = plan.provenance["dedupe_losers"]
        self.assertTrue(
            any(L["winner_strategy"] == "default"
                and L["loser_strategy"] == "literal"
                and L["canonical_query_key"] == "compilation"
                and L["would_have_been_ordinal"] == 1
                for L in losers),
            f"dedupe loser not recorded: {losers!r}",
        )

    # --- track ranking ties break by source-track order ------------------

    def test_track_ranking_ties_break_by_source_order(self):
        # All four tracks have identical useful-token count AND identical
        # char count after cleaning. Artist prepending adds the same
        # tokens to every track, so the relative rank stays source-order.
        plan = generate_search_plan(
            self._snapshot(
                artist="Dallas Crane",
                title="Album",
                year=None,
                release_group_year=None,
                track_titles=(
                    "Alpha Sigma",   # 11 chars, 2 tokens
                    "Bravo Delta",   # 11 chars, 2 tokens
                    "Echo1 Foxxx",   # 11 chars, 2 tokens
                    "Golfa Hotel",   # 11 chars, 2 tokens
                ),
            ),
            self._cfg(max_track_slots=3),
        )
        track_items = [
            it for it in plan.items if it.strategy.startswith("track_")
        ]
        self.assertEqual(len(track_items), 3)
        # Each query is "<*allas> <*rane> <Tok> <Tok>" capped to 4 tokens.
        for it in track_items:
            self.assertEqual(len(it.query.split()), 4)
            self.assertTrue(it.query.startswith("*allas *rane "))
        # First three tracks survive source-order ranking; fourth is
        # bumped to omitted_candidates as excess.
        self.assertEqual(track_items[0].provenance["source_track_index"], 0)
        self.assertEqual(track_items[1].provenance["source_track_index"], 1)
        self.assertEqual(track_items[2].provenance["source_track_index"], 2)
        omitted = plan.provenance["omitted_candidates"]
        excess = [
            o for o in omitted if o.get("strategy") == "track_excess"
        ]
        self.assertEqual(len(excess), 1)
        self.assertEqual(excess[0]["source_track_index"], 3)

    def test_track_ranking_orders_by_distinctiveness_then_chars(self):
        # U7: ranking is now driven by raw-title distinctiveness
        # (longest non-generic token * non-generic token count). The
        # rendered-query char count survives only as a tiebreaker.
        # Previously this test encoded the rendered-query token-count
        # ranker; under that ranker the top pick was idx=2 ("Aaaaa
        # Bbbbbbbbbbb") on char-count alone. Under U7 it's idx=1
        # ("One Two Three Four Tokens") because more non-generic tokens
        # beat a single long token.
        plan = generate_search_plan(
            self._snapshot(
                artist="Some Artist",
                title="Album",
                year=None,
                release_group_year=None,
                track_titles=(
                    "Aaaaa Bbbbbbb",                # 2 tokens, longest 7 → 14
                    "One Two Three Four Tokens",    # 5 tokens, longest 6 → 30
                    "Aaaaa Bbbbbbbbbbb",            # 2 tokens, longest 11 → 22
                    "Six Seven",                    # 2 tokens, longest 5 → 10
                ),
            ),
            self._cfg(max_track_slots=3),
        )
        track_items = [
            it for it in plan.items if it.strategy.startswith("track_")
        ]
        self.assertEqual(len(track_items), 3)
        # Distinctiveness ordering: idx=1 (30), idx=2 (22), idx=0 (14).
        self.assertEqual(track_items[0].provenance["source_track_index"], 1)
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
        # Empty artist+title but two distinct track candidates with
        # multi-token queries → multi-track album, track tier produces
        # runnable items so the plan succeeds even without album-level
        # slots. Empty artist also collapses the artist-prepend prefix
        # in per-track candidates, so the track queries are bare titles.
        plan = generate_search_plan(
            ReleaseSnapshot(
                artist_name="",
                title="",
                year=None,
                track_titles=("Distinct Track Title", "Another Real Song"),
                prepend_artist=True,
            ),
            self._cfg(),
        )
        self.assertEqual(plan.status, PLAN_STATUS_SUCCESS)
        # Only track slots; album-level candidates all omitted.
        strategies = [it.strategy for it in plan.items]
        self.assertEqual(strategies, ["track_0_artist", "track_1_artist"])
        omitted = plan.provenance["omitted_candidates"]
        self.assertTrue(
            any(o["strategy"] == "default"
                and o["reason"] == "empty_default_query"
                for o in omitted),
            f"missing default empty omission: {omitted!r}",
        )
        self.assertTrue(
            any(o["strategy"] == "literal"
                and o["reason"] == "empty_literal_query"
                for o in omitted),
            f"missing literal empty omission: {omitted!r}",
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

        This explicit assertion is the contract: if the generator's
        behavior changes (token rules, ladder, repeat groups, dedupe,
        provenance shape), the test author MUST also bump the id
        constant. The service layer and reconciliation read this
        constant — drift breaks the persisted plan currentness check.
        """
        # Snapshot output of a known release. Bumping any of these
        # expectations should require the id below to change too.
        # PR2 U8 (2026-05-25): GENERATOR_ID bumped from
        # "search-plan/2026-05-19-1" to "search-plan/2026-05-25-1"
        # alongside the strategy mix overhaul.
        plan = generate_search_plan(self._snapshot(), self._cfg())
        self.assertEqual(plan.generator_id, "search-plan/2026-05-25-1")
        self.assertEqual(plan.generator_id, SEARCH_PLAN_GENERATOR_ID)
        # Pin the slot ladder shape and queries for Radiohead / Kid A /
        # year=2008 / rg_year=2000. PR2 U8: literal_lossless dropped
        # (R1); no catalog_number on the fixture so that slot is
        # omitted; max_track_slots=4 (PR2 U8 R3) so the fixture's 6
        # tracks now produce track_0..track_3.
        self.assertEqual(
            [(it.strategy, it.query) for it in plan.items],
            [
                ("default", "*adiohead Kid A"),
                ("literal", "Radiohead Kid A"),
                ("literal_flac", "Radiohead Kid A FLAC"),
                ("unwild_year", "Radiohead Kid A 2008"),
                ("unwild_rg_year", "Radiohead Kid A 2000"),
                # U7 distinctiveness scoring re-orders the top-N tracks
                # by raw-title score (longest_non_generic_token *
                # num_non_generic_tokens). For Kid A's fixture:
                #   Everything in Its Right Place → 10 * 5 = 50
                #   How to Disappear Completely   → 10 * 4 = 40
                #   The National Anthem           →  8 * 3 = 24
                #   Treefingers                   → 11 * 1 = 11
                #   Optimistic                    → 10 * 1 = 10
                ("track_0_artist",
                 "*adiohead Everything Right Place"),
                ("track_1_artist",
                 "*adiohead How Disappear Completely"),
                ("track_2_artist", "*adiohead National Anthem"),
                ("track_3_artist", "*adiohead Treefingers"),
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
        # Sanity guard so we don't accidentally drift the public
        # default. PR2 U8 (R3) bumped from 3 to 4 so the non-VA mix
        # emits track_0_artist through track_3_artist.
        self.assertEqual(MAX_TRACK_SLOTS_PER_PLAN, 4)
        self.assertEqual(SearchPlanConfig().max_track_slots, 4)


class TestScoreTrackDistinctiveness(unittest.TestCase):
    """Pure scorer for U7 distinctiveness ranking.

    Score = ``len(longest_non_generic_token) * num_non_generic_tokens``.
    Generic tokens (``GENERIC_TITLE_TOKENS``) and ``Track \\d+`` style
    placeholders are excluded from both factors. Stopwords are NOT
    specially demoted.
    """

    # (description, title, expected_score)
    CASES = [
        ("empty", "", 0.0),
        ("whitespace_only_collapses_to_empty_tokens", "   ", 0.0),
        ("generic_track_n", "Track 7", 0.0),
        ("generic_track_n_lowercase", "track 12", 0.0),
        ("generic_track_n_no_space", "Track7", 0.0),
        ("all_generic_single", "Theme", 0.0),
        ("all_generic_motion_picture_soundtrack",
         "Motion Picture Soundtrack", 0.0),
        ("untitled_alone", "Untitled", 0.0),
        # "Untitled 4" — "Untitled" is generic, "4" survives as a
        # non-generic 1-char token.
        ("untitled_with_number", "Untitled 4", 1.0),
        # Single distinctive token — score = len(token) * 1.
        ("single_word_aerial", "Aerial", 6.0),
        ("single_word_treefingers", "Treefingers", 11.0),
        # Kid A canonical: longest non-generic = "Everything" (10),
        # count = 5 (Everything/in/Its/Right/Place; "in"/"Its" are
        # stopword-ish but not in GENERIC_TITLE_TOKENS, so they count).
        ("kid_a_everything", "Everything in Its Right Place", 50.0),
        # How to Disappear Completely: longest = "Completely" (10),
        # count = 4 (How/to/Disappear/Completely).
        ("kid_a_disappear", "How to Disappear Completely", 40.0),
        # The National Anthem: longest = "National" (8), count = 3.
        ("kid_a_national_anthem", "The National Anthem", 24.0),
        # Mixed generic + distinctive: "Intro Song" → "intro" drops,
        # "Song" survives (4 * 1).
        ("mixed_generic_intro_song", "Intro Song", 4.0),
    ]

    def test_scores(self):
        for desc, title, expected in self.CASES:
            with self.subTest(desc=desc, title=title):
                self.assertEqual(
                    score_track_distinctiveness(title), expected,
                )

    def test_motion_picture_soundtrack_lower_than_distinctive(self):
        """Canonical bad case from the plan: 'Motion Picture Soundtrack'
        must score strictly lower than a distinctive title."""
        self.assertLess(
            score_track_distinctiveness("Motion Picture Soundtrack"),
            score_track_distinctiveness("Everything in Its Right Place"),
        )

    def test_generic_token_set_pinned(self):
        """Sanity guard: changing GENERIC_TITLE_TOKENS is a generator-
        affecting change. Bump SEARCH_PLAN_GENERATOR_ID at the same time."""
        self.assertEqual(GENERIC_TITLE_TOKENS, frozenset({
            "intro", "outro", "interlude", "untitled", "overture", "theme",
            "motion", "picture", "soundtrack", "prelude", "reprise",
        }))


class TestGeneratorUsesDistinctivenessScoring(unittest.TestCase):
    """Integration: generate_search_plan honours U7 distinctiveness.

    AE1-partial coverage from the plan: a Kid A snapshot's emitted
    ``track_*_artist`` slots must NOT include the 'Motion Picture
    Soundtrack' rendering.
    """

    KID_A_FULL = (
        "Everything in Its Right Place",
        "Kid A",
        "The National Anthem",
        "How to Disappear Completely",
        "Treefingers",
        "Optimistic",
        "In Limbo",
        "Idioteque",
        "Morning Bell",
        "Motion Picture Soundtrack",
    )

    def test_kid_a_full_tracklist_excludes_motion_picture_soundtrack(self):
        plan = generate_search_plan(
            ReleaseSnapshot(
                artist_name="Radiohead",
                title="Kid A",
                year="2000",
                track_titles=self.KID_A_FULL,
                prepend_artist=True,
                release_group_year=2000,
            ),
            SearchPlanConfig(),
        )
        self.assertEqual(plan.status, PLAN_STATUS_SUCCESS)
        track_items = [
            it for it in plan.items if it.strategy.startswith("track_")
        ]
        # PR2 U8 (R3): MAX_TRACK_SLOTS_PER_PLAN bumped from 3 to 4.
        self.assertEqual(len(track_items), 4)
        track_queries = [it.query for it in track_items]
        for q in track_queries:
            self.assertNotIn(
                "Motion", q,
                f"'Motion Picture Soundtrack' leaked into track slot: {q!r}",
            )
            self.assertNotIn("Soundtrack", q, q)
        # And the two highest-scoring distinctive titles MUST be present.
        joined = " | ".join(track_queries)
        self.assertIn("Everything", joined)
        self.assertIn("Disappear", joined)


class TestGenerateSearchPlanSlotMix(unittest.TestCase):
    """PR2 U8 — strategy mix overhaul.

    SubTest matrix covering the new slot composition rules:
    - non-VA × catno-present / catno-absent / catno-too-short
    - non-VA × rg_year ↔ year combinations
    - VA × volume-marker × track_artists present
    - VA × no-volume-marker
    - VA × no-track-artists (degradation)
    - literal_lossless never appears (R1)
    """

    KID_A = (
        "Everything in Its Right Place",
        "Kid A",
        "The National Anthem",
        "How to Disappear Completely",
        "Treefingers",
        "Optimistic",
    )

    # Per-case spec: (description, snapshot kwargs, expected_present,
    # expected_absent). Each scenario asserts the listed strategies
    # are present (or all-present, by membership) and the listed
    # strategies are absent. Slot ordering pinned separately in the
    # GENERATOR_ID pin test.
    CASES = [
        (
            "non_va_catno_present_rg_year_present",
            {
                "artist_name": "Radiohead",
                "title": "Kid A",
                "year": "2008",
                "track_titles": KID_A,
                "prepend_artist": True,
                "release_group_year": 2000,
                "catalog_number": "STRMRT-001",
                "is_va_compilation": False,
            },
            ["default", "literal", "literal_flac",
             "unwild_year", "unwild_rg_year", "catalog_number",
             "track_0_artist", "track_1_artist",
             "track_2_artist", "track_3_artist"],
            ["literal_lossless", "va_track_artist_0",
             "compilation_series"],
        ),
        (
            "non_va_catno_absent",
            {
                "artist_name": "Radiohead",
                "title": "Kid A",
                "year": "2008",
                "track_titles": KID_A,
                "prepend_artist": True,
                "release_group_year": 2000,
                "catalog_number": None,
                "is_va_compilation": False,
            },
            ["default", "literal", "literal_flac",
             "unwild_year", "unwild_rg_year"],
            ["catalog_number", "literal_lossless"],
        ),
        (
            "non_va_catno_too_short",
            {
                "artist_name": "Radiohead",
                "title": "Kid A",
                "year": "2008",
                "track_titles": KID_A,
                "prepend_artist": True,
                "release_group_year": 2000,
                "catalog_number": "100",
                "is_va_compilation": False,
            },
            ["default", "literal", "literal_flac",
             "unwild_year", "unwild_rg_year"],
            ["catalog_number", "literal_lossless"],
        ),
        (
            "non_va_rg_year_matches_year",
            {
                "artist_name": "Radiohead",
                "title": "Kid A",
                "year": "2000",
                "track_titles": KID_A,
                "prepend_artist": True,
                "release_group_year": 2000,
                "catalog_number": "STRMRT-001",
                "is_va_compilation": False,
            },
            ["default", "literal", "literal_flac",
             "unwild_year", "catalog_number"],
            ["unwild_rg_year", "literal_lossless"],
        ),
        (
            "va_volume_marker_track_artists_present",
            {
                "artist_name": "Various Artists",
                "title": "Now That's What I Call Music #100",
                "year": "2018",
                "track_titles": ("Sunshine", "Moonlight", "Starlight"),
                "track_artists": ("Cat A", "Dog B", "Bird C"),
                "prepend_artist": True,
                "release_group_year": 2018,
                "catalog_number": "NOW-100-01",
                "is_va_compilation": True,
            },
            ["va_track_artist_0", "va_track_artist_1",
             "va_track_artist_2",
             "compilation_series",
             "unwild_year", "catalog_number"],
            ["default", "literal", "literal_flac",
             "literal_lossless", "track_0_artist"],
        ),
        (
            "va_no_volume_marker",
            {
                "artist_name": "Various Artists",
                "title": "Surf Rock Compilation Disc",
                "year": "1995",
                "track_titles": ("Misirlou", "Wipe Out", "Pipeline"),
                "track_artists": ("Dick Dale", "Surfaris", "Chantays"),
                "prepend_artist": True,
                "is_va_compilation": True,
            },
            ["va_track_artist_0",
             "va_track_artist_1",
             "va_track_artist_2",
             "unwild_year"],
            ["compilation_series", "default", "literal",
             "literal_flac", "literal_lossless"],
        ),
        (
            "va_no_track_artists_degrades_gracefully",
            {
                "artist_name": "Various Artists",
                "title": "Niche Label Sampler",
                "year": "2005",
                "track_titles": ("Track One", "Track Two", "Track Three"),
                "track_artists": (),
                "prepend_artist": True,
                "catalog_number": "LBL-SAMP-2005",
                "is_va_compilation": True,
            },
            ["unwild_year", "catalog_number"],
            ["va_track_artist_0", "default", "literal",
             "literal_flac", "literal_lossless",
             "compilation_series"],
        ),
        (
            "va_all_null_track_artists_degrades_gracefully",
            {
                "artist_name": "Various Artists",
                "title": "Another Sampler",
                "year": "2010",
                "track_titles": ("Track One", "Track Two", "Track Three"),
                "track_artists": (None, None, None),
                "prepend_artist": True,
                "is_va_compilation": True,
            },
            ["unwild_year"],
            ["va_track_artist_0", "default", "literal",
             "literal_flac", "literal_lossless"],
        ),
    ]

    def test_slot_mix_matrix(self):
        for desc, kwargs, present, absent in self.CASES:
            with self.subTest(desc=desc):
                plan = generate_search_plan(
                    ReleaseSnapshot(**kwargs),
                    SearchPlanConfig(),
                )
                self.assertEqual(plan.status, PLAN_STATUS_SUCCESS,
                                 f"{desc}: {plan.failure_reason!r}")
                strategies = {it.strategy for it in plan.items}
                for s in present:
                    self.assertIn(
                        s, strategies,
                        f"{desc}: expected {s!r} in {strategies!r}",
                    )
                for s in absent:
                    self.assertNotIn(
                        s, strategies,
                        f"{desc}: did not expect {s!r} in {strategies!r}",
                    )

    def test_va_branch_records_is_va_compilation_in_provenance(self):
        plan = generate_search_plan(
            ReleaseSnapshot(
                artist_name="Various Artists",
                title="Some VA Comp",
                year="2010",
                track_titles=("Track A", "Track B"),
                track_artists=("Artist X", "Artist Y"),
                prepend_artist=True,
                is_va_compilation=True,
            ),
            SearchPlanConfig(),
        )
        self.assertTrue(plan.provenance.get("is_va_compilation"))

    def test_va_branch_omits_no_track_artists_resolved_when_all_null(self):
        plan = generate_search_plan(
            ReleaseSnapshot(
                artist_name="Various Artists",
                title="Empty Comp",
                year="2010",
                track_titles=("Track A", "Track B"),
                track_artists=(None, None),
                prepend_artist=True,
                is_va_compilation=True,
            ),
            SearchPlanConfig(),
        )
        omitted = plan.provenance["omitted_candidates"]
        self.assertTrue(
            any(o["strategy"] == "va_track_artist_*"
                and o["reason"] == "no_track_artists_resolved"
                for o in omitted),
            f"missing no_track_artists_resolved omission: {omitted!r}",
        )

    def test_catalog_number_query_shape_non_va(self):
        plan = generate_search_plan(
            ReleaseSnapshot(
                artist_name="Some Artist",
                title="Their Album",
                year="2020",
                track_titles=(),
                prepend_artist=True,
                catalog_number="STRMRT-001",
                is_va_compilation=False,
            ),
            SearchPlanConfig(),
        )
        by_strategy = {it.strategy: it.query for it in plan.items}
        self.assertIn("catalog_number", by_strategy)
        # Artist tokens prepended, catno carried verbatim through
        # _build_query cap (longest-first).
        q = by_strategy["catalog_number"]
        self.assertIn("STRMRT-001", q)

    def test_catalog_number_too_short_omitted_with_reason(self):
        plan = generate_search_plan(
            ReleaseSnapshot(
                artist_name="Some Artist",
                title="Their Album",
                year="2020",
                track_titles=(),
                prepend_artist=True,
                catalog_number="100",  # below CATALOG_NUMBER_MIN_LENGTH
                is_va_compilation=False,
            ),
            SearchPlanConfig(),
        )
        strategies = [it.strategy for it in plan.items]
        self.assertNotIn("catalog_number", strategies)
        omitted = plan.provenance["omitted_candidates"]
        catno_omits = [
            o for o in omitted if o["strategy"] == "catalog_number"
        ]
        self.assertEqual(len(catno_omits), 1)
        self.assertEqual(catno_omits[0]["reason"], "catalog_number_too_short")

    def test_catalog_number_empty_string_treated_as_unknown(self):
        """Defensive: an empty-string catno (vs NULL) is treated as unknown."""
        plan = generate_search_plan(
            ReleaseSnapshot(
                artist_name="Some Artist",
                title="Their Album",
                year="2020",
                track_titles=(),
                prepend_artist=True,
                catalog_number="",
                is_va_compilation=False,
            ),
            SearchPlanConfig(),
        )
        strategies = [it.strategy for it in plan.items]
        self.assertNotIn("catalog_number", strategies)
        omitted = plan.provenance["omitted_candidates"]
        self.assertTrue(
            any(o["strategy"] == "catalog_number"
                and o["reason"] == "catalog_number_unknown"
                for o in omitted),
        )

    def test_compilation_series_volume_markers_match(self):
        """The compilation_series detector regex must match these
        canonical anthology-series titles (Vol N, Volume N, #N)."""
        for title in (
            "Now That's What I Call Music #100",
            "Greatest Surf Hits Vol 3",
            "Surf Hits Volume 12",
            "Surf Hits Vol. 12",
            "Comp #1",
        ):
            with self.subTest(title=title):
                plan = generate_search_plan(
                    ReleaseSnapshot(
                        artist_name="Various Artists",
                        title=title,
                        year="2010",
                        track_titles=("A", "B"),
                        track_artists=("X", "Y"),
                        prepend_artist=True,
                        is_va_compilation=True,
                    ),
                    SearchPlanConfig(),
                )
                strategies = [it.strategy for it in plan.items]
                self.assertIn("compilation_series", strategies,
                              f"expected match for {title!r}")

    def test_compilation_series_no_volume_marker_omitted(self):
        plan = generate_search_plan(
            ReleaseSnapshot(
                artist_name="Various Artists",
                title="Compilation Album",
                year="2010",
                track_titles=("A", "B"),
                track_artists=("X", "Y"),
                prepend_artist=True,
                is_va_compilation=True,
            ),
            SearchPlanConfig(),
        )
        omitted = plan.provenance["omitted_candidates"]
        self.assertTrue(
            any(o["strategy"] == "compilation_series"
                and o["reason"] == "no_volume_marker"
                for o in omitted),
        )


if __name__ == "__main__":
    unittest.main()
