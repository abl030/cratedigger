"""Upgrade guard: compare a candidate with the installed copy (issue #1445 item 3).

Pure unit tests for ``lib.quality.installed_copy_guard``. The Fairways world
is the real request 1649: installed copy = download_log 3523, candidate =
download_log 42692 (lengths in seconds, read from their stored mappings).
"""

from __future__ import annotations

import math
import unittest

import msgspec

from lib.quality.installed_copy_guard import (
    HOLD_BASIS_DECLARED_PROGRAM,
    HOLD_BASIS_INSTALLED_COPY,
    UPGRADE_GUARD_TRACK_LENGTH_SECONDS,
    CandidateTrackLength,
    InstalledCopyComparison,
    InstalledTrackLength,
    compare_installed_copy,
    compare_with_declared_program,
    held_upgrade_sentence,
)

# (index, title, declared, installed 3523, candidate 42692)
FAIRWAYS: tuple[tuple[int, str, float, float, float], ...] = (
    (1, "A Song for Jenni", 227.4, 227.4, 227.3),
    (2, "Secretive Girl", 126.8, 126.8, 205.9),
    (3, "Close to Me", 196.9, 196.9, 196.8),
    (4, "Quarter to Seven", 157.7, 157.7, 157.8),
    (5, "Postcard Picture Perfect", 259.9, 259.9, 198.3),
    (6, "K-L-M Line", 229.1, 229.1, 229.1),
    (7, "Get It Right", 152.2, 152.2, 152.3),
    (8, "Darling, Don't You Think?", 257.4, 257.4, 257.3),
    (9, "Phthalo Blue", 165.0, 165.0, 165.2),
    (10, "Let's Go", 365.1, 365.1, 369.3),
)


def fairways() -> tuple[list[CandidateTrackLength], list[InstalledTrackLength]]:
    candidate = [
        CandidateTrackLength(
            key=f"rt-{index}", medium=1, medium_index=index, title=title,
            declared_seconds=declared, candidate_seconds=new,
        )
        for index, title, declared, _old, new in FAIRWAYS
    ]
    installed = [
        InstalledTrackLength(key=f"rt-{index}", installed_seconds=old)
        for index, _title, _declared, old, _new in FAIRWAYS
    ]
    return candidate, installed


def one(
    *, declared: float, installed: float, candidate: float,
    key: str = "k", component_count: int = 1, medium_index: int | None = 2,
    medium: int | None = 1,
) -> InstalledCopyComparison:
    return compare_installed_copy(
        [CandidateTrackLength(
            key=key, medium=medium, medium_index=medium_index, title="Song",
            declared_seconds=declared, candidate_seconds=candidate,
            component_count=component_count,
        )],
        [InstalledTrackLength(key=key, installed_seconds=installed)],
    )


class TestCompareInstalledCopy(unittest.TestCase):
    def test_fairways_holds_on_tracks_2_and_5(self) -> None:
        candidate, installed = fairways()
        result = compare_installed_copy(candidate, installed)
        self.assertTrue(result.holds)
        self.assertEqual(result.compared_count, 10)
        self.assertEqual(
            [row.medium_index for row in result.rows if row.tripped], [2, 5])
        self.assertEqual(result.threshold_seconds, 30.0)

    def test_exactly_at_threshold_does_not_trip(self) -> None:
        self.assertFalse(one(
            declared=100, installed=100,
            candidate=100 + UPGRADE_GUARD_TRACK_LENGTH_SECONDS).holds)
        self.assertTrue(one(
            declared=100, installed=100,
            candidate=100 + UPGRADE_GUARD_TRACK_LENGTH_SECONDS + 0.1).holds)

    def test_candidate_strictly_closer_to_release_proceeds(self) -> None:
        # The installed copy is the wrong song; the new one is right.
        self.assertFalse(
            one(declared=127, installed=206, candidate=127).holds)

    def test_equally_close_is_not_strictly_closer(self) -> None:
        self.assertTrue(
            one(declared=150, installed=110, candidate=190).holds)

    def test_no_declared_length_holds(self) -> None:
        self.assertTrue(one(declared=0, installed=127, candidate=206).holds)
        self.assertTrue(
            one(declared=math.nan, installed=127, candidate=206).holds)

    def test_unusable_copy_lengths_skip_the_track(self) -> None:
        for bad in (0.0, -5.0, math.nan, math.inf):
            with self.subTest(bad=bad):
                self.assertFalse(
                    one(declared=127, installed=bad, candidate=206).holds)
                self.assertFalse(
                    one(declared=127, installed=127, candidate=bad).holds)

    def test_discogs_composite_is_skipped(self) -> None:
        result = one(
            declared=127, installed=127, candidate=600, component_count=2)
        self.assertFalse(result.holds)
        self.assertEqual(result.compared_count, 0)

    def test_track_missing_from_installed_copy_is_skipped(self) -> None:
        result = compare_installed_copy(
            [CandidateTrackLength(
                key="a", medium=1, medium_index=1, title="",
                declared_seconds=100, candidate_seconds=300)],
            [InstalledTrackLength(key="b", installed_seconds=100)],
        )
        self.assertFalse(result.holds)
        self.assertEqual(result.compared_count, 0)
        self.assertIsNone(result.rows[0].installed_seconds)

    def test_blank_or_duplicate_keys_are_never_compared(self) -> None:
        blank = one(declared=127, installed=127, candidate=206, key="")
        self.assertFalse(blank.holds)
        duplicate = compare_installed_copy(
            [CandidateTrackLength(
                key="k", medium=1, medium_index=1, title="",
                declared_seconds=100, candidate_seconds=300)],
            [InstalledTrackLength(key="k", installed_seconds=100),
             InstalledTrackLength(key="k", installed_seconds=300)],
        )
        self.assertFalse(duplicate.holds)
        self.assertEqual(duplicate.compared_count, 0)

    def test_rows_are_in_disc_order_and_round_trip(self) -> None:
        candidate, installed = fairways()
        result = compare_installed_copy(
            list(reversed(candidate)), installed)
        self.assertEqual(
            [row.medium_index for row in result.rows], list(range(1, 11)))
        decoded = msgspec.json.decode(
            msgspec.json.encode(result), type=InstalledCopyComparison)
        self.assertEqual(decoded, result)


class TestHeldUpgradeSentence(unittest.TestCase):
    def test_fairways_new_copy_is_the_odd_one_out(self) -> None:
        candidate, installed = fairways()
        self.assertEqual(
            held_upgrade_sentence(compare_installed_copy(candidate, installed)),
            "Kept your current copy. On the new copy track 2 runs 3:26 and "
            "track 5 runs 3:18. Your current copy and the release both say "
            "2:07 and 4:20, so the new copy is the odd one out.",
        )

    def test_release_lists_no_length(self) -> None:
        self.assertEqual(
            held_upgrade_sentence(
                one(declared=0, installed=127, candidate=206)),
            "Kept your current copy. Track 2 runs 3:26 on the new copy but "
            "2:07 on your current copy. The release lists no length for it, "
            "so it does not say which is right.",
        )

    def test_release_matches_neither_copy(self) -> None:
        self.assertEqual(
            held_upgrade_sentence(
                one(declared=170, installed=127, candidate=215)),
            "Kept your current copy. Track 2 runs 3:35 on the new copy and "
            "2:07 on your current copy, but the release says 2:50. Neither "
            "copy matches the release there.",
        )

    def test_more_than_three_tracks_names_three_and_counts(self) -> None:
        tracks = [
            CandidateTrackLength(
                key=str(index), medium=1, medium_index=index, title="",
                declared_seconds=100, candidate_seconds=200)
            for index in range(1, 6)
        ]
        installed = [
            InstalledTrackLength(key=str(index), installed_seconds=100)
            for index in range(1, 6)
        ]
        sentence = held_upgrade_sentence(
            compare_installed_copy(tracks, installed))
        self.assertIn("track 1 runs 3:20, track 2 runs 3:20 and track 3", sentence)
        self.assertNotIn("track 4", sentence)
        self.assertTrue(sentence.endswith("2 more tracks differ too."))

    def test_multi_disc_names_the_disc(self) -> None:
        result = compare_installed_copy(
            [
                CandidateTrackLength(
                    key="a", medium=1, medium_index=1, title="",
                    declared_seconds=100, candidate_seconds=100),
                CandidateTrackLength(
                    key="b", medium=2, medium_index=3, title="",
                    declared_seconds=0, candidate_seconds=300),
            ],
            [InstalledTrackLength(key="a", installed_seconds=100),
             InstalledTrackLength(key="b", installed_seconds=100)],
        )
        self.assertIn("Track 3 on disc 2 runs 5:00", held_upgrade_sentence(result))

    def test_sentence_never_names_internal_identifiers(self) -> None:
        candidate, installed = fairways()
        sentence = held_upgrade_sentence(
            compare_installed_copy(candidate, installed))
        for token in ("rt-", "_", "download_log", "mbid"):
            self.assertNotIn(token, sentence)


def eels(
    *, last_candidate: float = 193.0,
) -> list[CandidateTrackLength]:
    """EELS (issue #1471 evidence): last track 3:13 against 15:05 listed."""
    tracks = [(1, "Opening", 200.0, 200.4), (2, "Middle", 180.0, 179.8)]
    tracks.append((3, "Closing", 905.0, last_candidate))
    return [
        CandidateTrackLength(
            key=f"rt-{index}", medium=1, medium_index=index, title=title,
            declared_seconds=declared, candidate_seconds=candidate,
        )
        for index, title, declared, candidate in tracks
    ]


class TestCompareWithDeclaredProgram(unittest.TestCase):
    """Issue #1471 gap 2: the yardstick when the copy is marked incomplete."""

    def test_missing_end_of_last_track_holds(self) -> None:
        comparison = compare_with_declared_program(eels(), None)
        self.assertTrue(comparison.holds)
        self.assertEqual(comparison.basis, HOLD_BASIS_DECLARED_PROGRAM)
        self.assertEqual(
            [row.medium_index for row in comparison.rows if row.tripped], [3])
        self.assertEqual(comparison.compared_count, 3)

    def test_a_complete_candidate_proceeds(self) -> None:
        comparison = compare_with_declared_program(
            eels(last_candidate=906.0), None)
        self.assertFalse(comparison.holds)

    def test_longer_than_listed_never_trips(self) -> None:
        """An unlisted hidden track kept in the file is not short."""
        comparison = compare_with_declared_program(
            eels(last_candidate=1400.0), None)
        self.assertFalse(comparison.holds)

    def test_exactly_at_threshold_does_not_trip(self) -> None:
        comparison = compare_with_declared_program(
            eels(last_candidate=905.0 - UPGRADE_GUARD_TRACK_LENGTH_SECONDS),
            None,
        )
        self.assertFalse(comparison.holds)

    def test_no_declared_length_or_composite_is_never_compared(self) -> None:
        tracks = [
            CandidateTrackLength(
                key="a", medium=1, medium_index=1, title="Unlisted",
                declared_seconds=0.0, candidate_seconds=10.0,
            ),
            CandidateTrackLength(
                key="b", medium=1, medium_index=2, title="Medley",
                declared_seconds=600.0, candidate_seconds=10.0,
                component_count=3,
            ),
        ]
        comparison = compare_with_declared_program(tracks, None)
        self.assertEqual(comparison.compared_count, 0)
        self.assertFalse(comparison.holds)

    def test_installed_lengths_are_shown_but_never_decide(self) -> None:
        installed = [
            InstalledTrackLength(key="rt-3", installed_seconds=193.2),
        ]
        comparison = compare_with_declared_program(eels(), installed)
        self.assertTrue(comparison.holds)
        last = comparison.rows[-1]
        self.assertEqual(last.installed_seconds, 193.2)
        self.assertEqual(
            compare_with_declared_program(eels(), None).holds,
            comparison.holds,
        )

    def test_sentence_names_the_short_track_against_the_release(self) -> None:
        sentence = held_upgrade_sentence(
            compare_with_declared_program(eels(), None))
        self.assertEqual(
            sentence,
            "Kept your current copy, which you marked incomplete. The new "
            "copy is short too: track 3 runs 3:13 where the release says "
            "15:05.",
        )

    def test_basis_round_trips_and_legacy_rows_decode_as_installed(self) -> None:
        comparison = compare_with_declared_program(eels(), None)
        decoded = msgspec.json.decode(
            msgspec.json.encode(comparison), type=InstalledCopyComparison)
        self.assertEqual(decoded, comparison)
        legacy = msgspec.to_builtins(
            compare_installed_copy(*fairways()))
        assert isinstance(legacy, dict)
        legacy.pop("basis")
        self.assertEqual(
            msgspec.convert(legacy, type=InstalledCopyComparison).basis,
            HOLD_BASIS_INSTALLED_COPY,
        )


class TestUpgradeHeldVocabulary(unittest.TestCase):
    """One token, three readers: the decision, the scenario, the policy."""

    def test_scenario_and_decision_share_one_token(self) -> None:
        from lib.quality.decisions import DECISION_UPGRADE_HELD
        from lib.validation_envelope import scenario_covers_declared_program
        from lib.wrong_match_policy import (
            UPGRADE_HELD_SCENARIO,
            rejection_scenario_is_delete_eligible,
            rejection_scenario_is_wrong_match_candidate,
        )

        self.assertEqual(UPGRADE_HELD_SCENARIO, DECISION_UPGRADE_HELD)
        self.assertTrue(
            rejection_scenario_is_wrong_match_candidate(UPGRADE_HELD_SCENARIO))
        self.assertFalse(
            rejection_scenario_is_delete_eligible(UPGRADE_HELD_SCENARIO))
        # It was a strong_match: a force import of the held row is covered.
        self.assertTrue(scenario_covers_declared_program(UPGRADE_HELD_SCENARIO))

    def test_dispatch_action_keeps_the_folder_and_bans(self) -> None:
        from lib.quality import dispatch_action

        action = dispatch_action("upgrade_held")
        self.assertTrue(action.record_rejection)
        self.assertTrue(action.denylist)
        self.assertFalse(action.cleanup)
        self.assertFalse(action.mark_done)


if __name__ == "__main__":
    unittest.main()
