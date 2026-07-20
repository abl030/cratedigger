"""Unit tests for lib.artist_compare — fuzzy MB+Discogs discography merge."""

import os
import sys
import unittest
from typing import cast

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from lib.artist_catalogue import ArtistCatalogueRow, ArtistStructuralType
from lib.artist_compare import (
    CompareBuckets,
    annotate_in_library,
    extract_year,
    merge_discographies,
    normalize_title,
)


def _mb(
    title: str,
    year: str = "",
    id: str = "rg",
    *,
    type_: str = "Album",
    is_appearance: bool = False,
) -> ArtistCatalogueRow:
    primary_types: list[ArtistStructuralType] = (
        [cast(ArtistStructuralType, type_)]
        if type_ in {"Album", "EP", "Single"} else []
    )
    return ArtistCatalogueRow(
        id=id,
        title=title,
        first_release_date=year,
        type=type_,
        source="mb",
        identity_kind="work",
        primary_types=primary_types,
        secondary_types=[],
        format_qualifiers=[],
        provenance=["ordinary"],
        artist_credit="Artist",
        primary_artist_id="artist-id",
        is_appearance=is_appearance,
    )


def _dg(
    title: str,
    year: str = "",
    id: str = "1",
    *,
    type_: str = "Album",
    primary_types: list[ArtistStructuralType] | None = None,
    is_appearance: bool = False,
) -> ArtistCatalogueRow:
    inferred_types: list[ArtistStructuralType] = (
        [cast(ArtistStructuralType, type_)]
        if primary_types is None and type_ in {"Album", "EP", "Single"}
        else (primary_types or [])
    )
    return ArtistCatalogueRow(
        id=id,
        title=title,
        first_release_date=year,
        type=type_,
        source="discogs",
        identity_kind="work",
        primary_types=inferred_types,
        secondary_types=[],
        format_qualifiers=[],
        provenance=["ordinary"],
        artist_credit="Artist",
        primary_artist_id="artist-id",
        is_appearance=is_appearance,
    )


class TestNormalizeTitle(unittest.TestCase):
    CASES = [
        ("lowercase", "OK Computer", "okcomputer"),
        ("special chars", "Text_Bomb", "textbomb"),
        ("punctuation", "Ok. Computer!", "okcomputer"),
        ("empty", "", ""),
        ("whitespace only", "   ", ""),
        ("unicode-ish", "Sigur Rós", "sigurrs"),  # non-ascii stripped
        ("multiplication sign", "12 × 5", "12x5"),
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
    def test_masterless_release_can_associate_without_losing_release_identity(self):
        mb = _mb("Shared Title", "2000", id="mb-work")
        release = _dg("Shared Title", "2000", id="discogs-release")
        release.identity_kind = "release"

        result = merge_discographies([mb], [release])

        self.assertEqual(len(result.both), 1)
        self.assertEqual(result.both[0].mb.id, "mb-work")
        self.assertEqual(result.both[0].discogs.id, "discogs-release")
        self.assertEqual(result.both[0].discogs.identity_kind, "release")
        self.assertEqual(result.mb_unpaired, [])
        self.assertEqual(result.discogs_unpaired, [])
        self.assertEqual(result.discogs_ungrouped_releases, [])

    def test_unknown_provenance_does_not_block_known_ordinary_release(self):
        mb = _mb("The Split", "1998", id="mb-split", type_="Other")
        mb.provenance = []
        release = _dg(
            "The Split", "1998", id="discogs-split", type_="Other",
            primary_types=[],
        )
        release.identity_kind = "release"
        release.provenance = ["ordinary"]

        result = merge_discographies([mb], [release])

        self.assertEqual(len(result.both), 1)
        self.assertEqual(result.both[0].discogs.id, "discogs-split")

    def test_mixed_ordinary_does_not_pair_with_pure_exceptional_release(self):
        mb = _mb("Shared Title", "2000", id="mb-mixed")
        mb.provenance = ["ordinary", "unofficial"]
        release = _dg("Shared Title", "2000", id="discogs-unofficial")
        release.identity_kind = "release"
        release.provenance = ["unofficial"]

        result = merge_discographies([mb], [release])

        self.assertEqual(result.both, [])
        self.assertEqual([row.id for row in result.mb_unpaired], ["mb-mixed"])
        self.assertEqual(
            [row.id for row in result.discogs_ungrouped_releases],
            ["discogs-unofficial"],
        )

    def test_multiplication_sign_title_variant_pairs_exactly(self):
        result = merge_discographies(
            [_mb("12 X 5", "1964", id="mb-12x5")],
            [_dg("12 × 5", "1964", id="discogs-12x5")],
        )

        self.assertEqual(len(result.both), 1)
        self.assertEqual(result.both[0].discogs.id, "discogs-12x5")

    def test_equal_evidence_prefers_master_over_release_in_both_orders(self):
        for release_first in (False, True):
            with self.subTest(release_first=release_first):
                master = _dg("Shared Title", "2000", id="master")
                release = _dg("Shared Title", "2000", id="release")
                release.identity_kind = "release"
                candidates = (
                    [release, master] if release_first else [master, release]
                )

                result = merge_discographies(
                    [_mb("Shared Title", "2000", id="mb")], candidates,
                )

                self.assertEqual(result.both[0].discogs.id, "master")
                self.assertEqual(
                    [row.id for row in result.discogs_ungrouped_releases],
                    ["release"],
                )

    def test_masterless_release_respects_every_conservative_guard(self):
        cases = [
            ("provenance", {"provenance": ["unofficial"]}),
            ("appearance", {"is_appearance": True}),
            ("structural type", {"primary_types": ["EP"]}),
            ("year", {"first_release_date": "2004"}),
        ]
        for label, mutation in cases:
            with self.subTest(label=label):
                mb = _mb("Shared Title", "2000", id="mb-work")
                release = _dg(
                    "Shared Title", "2000", id="discogs-release",
                )
                release.identity_kind = "release"
                for field, value in mutation.items():
                    setattr(release, field, value)

                result = merge_discographies([mb], [release])

                self.assertEqual(result.both, [])
                self.assertEqual(
                    [row.id for row in result.mb_unpaired], ["mb-work"],
                )
                self.assertEqual(result.discogs_unpaired, [])
                self.assertEqual(
                    [row.id for row in result.discogs_ungrouped_releases],
                    ["discogs-release"],
                )

    def test_deloris_exact_five_pair_world(self):
        mb_rows = [
            _mb("Fraulein", "1998", id="1c9e2970-b221-30ab-93c6-7896b52a240b"),
            _mb(
                "The Point In The War When We Knew We Were Lost / "
                "Mapped Out In Our Thoughts",
                "1998", id="54748ae6-a05e-4eb4-81e6-0576e126a9a9",
                type_="Other",
            ),
            _mb(
                "The Pointless Gift", "2000-12-05",
                id="fdb22921-b4c5-3c49-b2d0-85cb69eec1f1",
            ),
            _mb(
                "Fake Our Deaths", "2004-08-31",
                id="c47860b5-6afd-3a30-af3a-bafe6efc21b5",
            ),
            _mb(
                "Ten Lives", "2006-10-28",
                id="93c519f2-92e9-3ef2-aa3a-8754c9d8a2b3",
            ),
        ]
        mb_rows[1].provenance = []
        discogs_rows = [
            _dg("Fraulein", "1998", id="3938744"),
            _dg(
                "The Point In The War When We Knew We Were Lost / "
                "Mapped Out In Our Thoughts",
                "1998", id="461708", type_="Other", primary_types=[],
            ),
            _dg("The Pointless Gift", "2001", id="3088588"),
            _dg(
                "Fake Our Deaths", "2004-08-30", id="5087639",
                type_="Other", primary_types=[],
            ),
            _dg(
                "Ten Lives", "2006-10-28", id="5087646",
                type_="Other", primary_types=[],
            ),
        ]
        for row in discogs_rows:
            row.identity_kind = "release"

        result = merge_discographies(mb_rows, discogs_rows)

        self.assertEqual(len(result.both), 5)
        self.assertEqual(
            [(pair.mb.title, pair.discogs.id) for pair in result.both],
            [
                ("Fraulein", "3938744"),
                (
                    "The Point In The War When We Knew We Were Lost / "
                    "Mapped Out In Our Thoughts",
                    "461708",
                ),
                ("The Pointless Gift", "3088588"),
                ("Fake Our Deaths", "5087639"),
                ("Ten Lives", "5087646"),
            ],
        )
        self.assertTrue(all(
            pair.discogs.identity_kind == "release" for pair in result.both
        ))
        self.assertEqual(result.mb_unpaired, [])
        self.assertEqual(result.discogs_unpaired, [])
        self.assertEqual(result.discogs_ungrouped_releases, [])

    def test_same_title_work_with_different_provenance_stays_unpaired(self):
        mb = _mb("Shared Title", "2000", id="mb-work")
        mb.identity_kind = "work"
        mb.provenance = ["ordinary"]
        discogs = _dg("Shared Title", "2000", id="discogs-work")
        discogs.identity_kind = "work"
        discogs.provenance = ["unofficial"]

        result = merge_discographies([mb], [discogs])

        self.assertEqual(result.both, [])
        self.assertEqual([row.id for row in result.mb_unpaired], ["mb-work"])
        self.assertEqual(
            [row.id for row in result.discogs_unpaired], ["discogs-work"]
        )

    def test_exact_match(self):
        result = merge_discographies(
            [_mb("OK Computer", "1997")],
            [_dg("OK Computer", "1997")],
        )
        self.assertEqual(len(result.both), 1)
        self.assertEqual(result.mb_unpaired, [])
        self.assertEqual(result.discogs_unpaired, [])

    def test_year_delta_greater_than_one_splits(self):
        result = merge_discographies(
            [_mb("A Working Title in Green", "2002")],
            [_dg("A Working Title In Green", "2000")],
        )
        self.assertEqual(result.both, [])
        self.assertEqual(len(result.mb_unpaired), 1)
        self.assertEqual(len(result.discogs_unpaired), 1)

    def test_year_outside_tolerance(self):
        result = merge_discographies(
            [_mb("Some Album", "1997")],
            [_dg("Some Album", "2005")],
        )
        self.assertEqual(result.both, [])
        self.assertEqual(len(result.mb_unpaired), 1)
        self.assertEqual(len(result.discogs_unpaired), 1)

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
        self.assertEqual(len(result.mb_unpaired), 1)
        self.assertEqual(len(result.discogs_unpaired), 1)

    def test_mb_unpaired(self):
        result = merge_discographies(
            [_mb("Sleepyhead", "2003")],
            [],
        )
        self.assertEqual(len(result.mb_unpaired), 1)
        self.assertEqual(result.both, [])
        self.assertEqual(result.discogs_unpaired, [])

    def test_discogs_unpaired(self):
        result = merge_discographies(
            [],
            [_dg("Bootleg Live 2001", "2001")],
        )
        self.assertEqual(len(result.discogs_unpaired), 1)
        self.assertEqual(result.both, [])
        self.assertEqual(result.mb_unpaired, [])

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
        self.assertEqual(len(result.mb_unpaired), 1)
        self.assertEqual(len(result.discogs_unpaired), 1)

    def test_each_discogs_entry_matches_at_most_once(self):
        # If MB has the same title twice and Discogs has it once, only one MB
        # entry should pair with the Discogs row; the other stays unpaired.
        result = merge_discographies(
            [_mb("Compilation", "2000", id="rg1"),
             _mb("Compilation", "2010", id="rg2")],
            [_dg("Compilation", "2000")],
        )
        self.assertEqual(len(result.both), 1)
        self.assertEqual(result.both[0].mb.id, "rg1")
        self.assertEqual(len(result.mb_unpaired), 1)
        self.assertEqual(result.mb_unpaired[0].id, "rg2")

    def test_returns_dataclass(self):
        result = merge_discographies([], [])
        self.assertIsInstance(result, CompareBuckets)

    def test_distinct_case_variant_master_ids_are_conserved(self):
        """Distinct Discogs master identities are never silently deleted."""
        mb_rg = _mb("Twist and Shout", "1964", id="mb-1")
        dg_a = _dg("Twist and Shout", "1964", id="dg-A")
        dg_b = _dg("Twist And Shout", "1964", id="dg-B")  # capitalised And
        result = merge_discographies([mb_rg], [dg_a, dg_b])
        self.assertEqual(len(result.both), 1)
        self.assertEqual(result.both[0].mb.id, "mb-1")
        self.assertEqual(result.both[0].discogs.id, "dg-A")  # first survives
        self.assertEqual([row.id for row in result.discogs_unpaired], ["dg-B"])
        self.assertEqual(result.mb_unpaired, [])

    def test_distinct_source_ids_preserve_unique_titles(self):
        """Conservation must not collapse genuinely different titles."""
        result = merge_discographies(
            [],
            [_dg("Album One", "2000"), _dg("Album Two", "2000")],
        )
        self.assertEqual(len(result.discogs_unpaired), 2)

    def test_distinct_source_ids_preserve_far_apart_years(self):
        """Same title in years far apart (e.g. self-titled re-release a
        decade later) stays as two rows."""
        result = merge_discographies(
            [],
            [_dg("Self Titled", "1990"), _dg("Self Titled", "2010")],
        )
        self.assertEqual(len(result.discogs_unpaired), 2)

    def test_distinct_source_ids_preserve_different_types(self):
        """EP 1963 and Album 1963 of the same name are legitimately
        different release groups even when the title normalises to the
        same thing. Conservation must NOT collapse them."""
        ep = _dg("Twist And Shout", "1963", id="dg-ep", type_="EP")
        album = _dg("Twist And Shout", "1963", id="dg-album", type_="Album")
        result = merge_discographies([], [ep, album])
        self.assertEqual(len(result.discogs_unpaired), 2)

    def test_beatles_mb_album_picks_discogs_album_over_ep(self):
        """Structural type boundaries preserve the three distinct works."""
        mb = _mb("Twist and Shout", "1964", id="mb-album", type_="Album")
        dg_ep_63 = _dg("Twist And Shout", "1963", id="dg-ep", type_="EP")
        dg_single_64 = _dg("Twist And Shout", "1964", id="dg-single", type_="Single")
        dg_album_64 = _dg("Twist And Shout", "1964", id="dg-album", type_="Album")
        result = merge_discographies([mb], [dg_ep_63, dg_single_64, dg_album_64])
        self.assertEqual(len(result.both), 1)
        self.assertEqual(result.both[0].discogs.id, "dg-album")
        # EP and Single stay as discogs-only rows
        self.assertEqual({r.id for r in result.discogs_unpaired},
                         {"dg-ep", "dg-single"})

    def test_known_disjoint_exact_year_stays_separate(self):
        mb = _mb("Side Project", "2020", id="mb-1", type_="Album")
        dg = _dg("Side Project", "2020", id="dg-1", type_="EP")
        result = merge_discographies([mb], [dg])
        self.assertEqual(result.both, [])

    def test_pointless_gift_adjacent_album_years_pair(self):
        result = merge_discographies(
            [_mb("The Pointless Gift", "2000-12-05", id="mb-pointless")],
            [_dg("The Pointless Gift", "2001", id="dg-pointless")],
        )
        self.assertEqual(len(result.both), 1)
        self.assertEqual(result.both[0].discogs.id, "dg-pointless")

    def test_mixed_discogs_types_overlap_mb_single(self):
        result = merge_discographies(
            [_mb("Mystery of Love", "2017", type_="Single")],
            [_dg("Mystery of Love", "2018", primary_types=["EP", "Single"])],
        )
        self.assertEqual(len(result.both), 1)

    def test_unknown_discogs_type_exact_year_can_pair(self):
        result = merge_discographies(
            [_mb("Compilation", "2004")],
            [_dg("Compilation", "2004", type_="Album", primary_types=[])],
        )
        self.assertEqual(len(result.both), 1)

    def test_legacy_discogs_scalar_does_not_authorize_adjacent_year(self):
        result = merge_discographies(
            [_mb("Compilation", "2004")],
            [_dg("Compilation", "2005", type_="Album", primary_types=[])],
        )
        self.assertEqual(result.both, [])

    def test_appearance_and_mainline_never_pair(self):
        result = merge_discographies(
            [_mb("Indie Sampler", "2001", is_appearance=True)],
            [_dg("Indie Sampler", "2001", is_appearance=False)],
        )
        self.assertEqual(result.both, [])

    def test_exact_year_candidate_beats_adjacent_year(self):
        result = merge_discographies(
            [_mb("Airbag", "1998", type_="EP")],
            [
                _dg("Airbag", "1997", id="adjacent", type_="EP"),
                _dg("Airbag", "1998", id="exact", type_="EP"),
            ],
        )
        self.assertEqual(result.both[0].discogs.id, "exact")

    def test_exact_edge_beats_earlier_mb_adjacent_edge(self):
        result = merge_discographies(
            [
                _mb("Airbag", "1997", id="adjacent-mb", type_="EP"),
                _mb("Airbag", "1998", id="exact-mb", type_="EP"),
            ],
            [_dg("Airbag", "1998", id="discogs", type_="EP")],
        )
        self.assertEqual(result.both[0].mb.id, "exact-mb")
        self.assertEqual(result.mb_unpaired[0].id, "adjacent-mb")

    def test_stable_first_input_tie_policy(self):
        result = merge_discographies(
            [_mb("Same", "2000")],
            [
                _dg("Same", "2000", id="first", primary_types=["Album", "EP"]),
                _dg("Same", "2000", id="second", primary_types=["Album", "Single"]),
            ],
        )
        self.assertEqual(result.both[0].discogs.id, "first")

    def test_overlapping_type_evidence_wins_exact_year_tie(self):
        result = merge_discographies(
            [_mb("Same", "2000", type_="Album")],
            [
                _dg("Same", "2000", id="unknown", primary_types=[]),
                _dg("Same", "2000", id="overlap", primary_types=["Album"]),
            ],
        )
        self.assertEqual(result.both[0].discogs.id, "overlap")

    def test_distinct_source_ids_preserve_appearance_boundary(self):
        result = merge_discographies(
            [],
            [
                _dg("Sampler", "2000", id="main", is_appearance=False),
                _dg("Sampler", "2000", id="appearance", is_appearance=True),
            ],
        )
        self.assertEqual(len(result.discogs_unpaired), 2)

    def test_distinct_source_ids_preserve_different_structural_sets(self):
        result = merge_discographies(
            [],
            [
                _dg("Mixed Master", "2000", id="single", primary_types=["Single"]),
                _dg("Mixed Master", "2000", id="mixed", primary_types=["EP", "Single"]),
            ],
        )
        self.assertEqual(len(result.discogs_unpaired), 2)

    def test_distinct_discogs_unknown_type_ids_are_conserved(self):
        result = merge_discographies(
            [],
            [
                _dg(
                    "Unknown Evidence", "2000", id="album",
                    type_="Album", primary_types=[],
                ),
                _dg(
                    "Unknown Evidence", "2000", id="ep",
                    type_="EP", primary_types=[],
                ),
            ],
        )
        self.assertEqual(len(result.discogs_unpaired), 2)

    def test_distinct_mb_ids_preserve_type_and_appearance_boundaries(self):
        result = merge_discographies(
            [
                _mb("Same", "2000", id="album", type_="Album"),
                _mb("Same", "2000", id="ep", type_="EP"),
                _mb(
                    "Same", "2000", id="appearance", type_="Album",
                    is_appearance=True,
                ),
            ],
            [],
        )
        self.assertEqual(len(result.mb_unpaired), 3)

    def test_distinct_mb_unknown_type_ids_are_conserved(self):
        result = merge_discographies(
            [
                _mb("Unknown Evidence", "2000", id="other", type_="Other"),
                _mb(
                    "Unknown Evidence", "2000", id="compilation",
                    type_="Compilation",
                ),
            ],
            [],
        )
        self.assertEqual(len(result.mb_unpaired), 2)


class TestAnnotateInLibrary(unittest.TestCase):
    def test_mb_row_matched_by_release_group_id(self):
        rg = _mb("OK Computer", id="rg-uuid")
        lib = [{"mb_releasegroupid": "rg-uuid", "album": "Different Title",
                "formats": "MP3", "min_bitrate": 245000}]
        annotate_in_library([rg], [], lib)
        self.assertTrue(rg.in_library)
        self.assertEqual(rg.library_format, "MP3")
        self.assertEqual(rg.library_min_bitrate, 245)

    def test_mb_row_is_not_matched_by_title_without_rgid(self):
        rg = _mb("OK Computer", id="rg-uuid")
        lib = [{"mb_releasegroupid": None, "album": "OK Computer"}]
        annotate_in_library([rg], [], lib)
        self.assertFalse(rg.in_library)

    def test_mb_row_unmatched(self):
        rg = _mb("Unowned", id="rg-uuid")
        lib: list[dict[str, object]] = [{"mb_releasegroupid": "other", "album": "Other Album"}]
        annotate_in_library([rg], [], lib)
        self.assertFalse(rg.in_library)

    def test_discogs_row_matched_by_mb_albumid(self):
        # Beets stores numeric Discogs release IDs in mb_albumid for
        # Discogs-imported albums.
        master = _dg("OK Computer", id="12345")
        master.identity_kind = "release"
        lib: list[dict[str, object]] = [{"mb_albumid": "12345", "album": "Different Title"}]
        annotate_in_library([], [master], lib)
        self.assertTrue(master.in_library)

    def test_discogs_row_is_not_matched_by_title(self):
        master = _dg("OK Computer", id="99999")
        master.identity_kind = "release"
        lib: list[dict[str, object]] = [{"mb_albumid": "different-uuid", "album": "OK Computer"}]
        annotate_in_library([], [master], lib)
        self.assertFalse(master.in_library)

    def test_title_normalization_never_establishes_ownership(self):
        rg = _mb("Text Bomb")
        lib = [{"mb_releasegroupid": None, "album": "Text_Bomb"}]
        annotate_in_library([rg], [], lib)
        self.assertFalse(rg.in_library)

    def test_empty_library(self):
        rg = _mb("X")
        master = _dg("X")
        annotate_in_library([rg], [master], [])
        self.assertFalse(rg.in_library)
        self.assertFalse(master.in_library)

    def test_handles_library_row_with_missing_identity_fields(self):
        # Library album with no mb_releasegroupid / mb_albumid / album —
        # should not crash.
        annotate_in_library(
            [_mb("X")],
            [_dg("X")],
            [{}],
        )

    def test_rank_fn_invoked_on_match(self):
        """Caller plugs in a codec-aware rank fn; the matched row picks
        up library_rank from it."""
        rg = _mb("X")
        lib = [{"mb_releasegroupid": "rg", "album": "X",
                "formats": "Opus", "min_bitrate": 96000,
                "avg_bitrate": 128000}]
        # Stub rank_fn — would be the real quality_rank wrapper in prod
        def rank_fn(fmt, kbps):
            return "transparent" if (fmt == "Opus" and kbps == 128) else "unknown"
        annotate_in_library([rg], [], lib, rank_fn=rank_fn)
        self.assertEqual(rg.library_rank, "transparent")

    def test_request_6039_rank_uses_average_and_preserves_floor(self):
        rg = _mb("Request 6039", id="rg-6039")
        lib = [{
            "mb_releasegroupid": "rg-6039",
            "album": "Request 6039",
            "formats": "MP3",
            "min_bitrate": 194000,
            "avg_bitrate": 288000,
        }]
        seen = []

        def rank_fn(fmt, kbps):
            seen.append((fmt, kbps))
            return "transparent"

        annotate_in_library([rg], [], lib, rank_fn=rank_fn)

        self.assertEqual(rg.library_min_bitrate, 194)
        self.assertEqual(rg.library_avg_bitrate, 288)
        self.assertEqual(seen, [("MP3", 288)])

    def test_no_quality_fields_when_unmatched(self):
        rg = _mb("Unowned")
        annotate_in_library([rg], [], [])
        self.assertFalse(rg.in_library)
        self.assertIsNone(rg.library_format)
        self.assertIsNone(rg.library_min_bitrate)
        self.assertIsNone(rg.library_rank)

    def test_rolling_stones_self_titled_bootleg_is_not_owned_by_title(self):
        bootleg_group = _mb(
            "The Rolling Stones",
            id="a41cc3e2-4e34-4a93-968b-283cfeb87f7b",
        )
        bootleg_group.provenance = ["unofficial"]
        actual_library_album = {
            "mb_releasegroupid": "919b8534-f40f-3a07-be48-0a9095ab5ada",
            "mb_albumid": "088fe5c7-d58f-4868-b1a9-548e590a5a35",
            "album": "The Rolling Stones",
            "formats": "Opus",
            "min_bitrate": 112292,
            "avg_bitrate": 122563,
        }

        annotate_in_library([bootleg_group], [], [actual_library_album])

        self.assertIs(bootleg_group.in_library, False)
        self.assertIsNone(bootleg_group.library_avg_bitrate)

    def test_discogs_master_does_not_claim_owned_leaf_release_id(self):
        master = _dg("The Rolling Stones", id="9715")
        library_leaf: dict[str, object] = {
            "mb_albumid": "9715",
            "album": "Different release title",
        }

        annotate_in_library([], [master], [library_leaf])

        self.assertIs(master.in_library, False)

    def test_discogs_release_unit_matches_exact_release_id(self):
        release = _dg("A pressing", id="9715")
        release.identity_kind = "release"
        library_leaf: dict[str, object] = {
            "mb_albumid": "9715",
            "album": "Different release title",
        }

        annotate_in_library([], [release], [library_leaf])

        self.assertIs(release.in_library, True)


if __name__ == "__main__":
    unittest.main()
