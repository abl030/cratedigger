"""Tests for guarded beets duplicate replacement in import_one.py.

Covers:
- run_import() answering remove only for one same-release duplicate
- run_import() failing closed when beets reports unsafe duplicate sets
- run_import() returning success for normal imports with no duplicate callback
"""

import json
import os
import tempfile
import unittest
from unittest.mock import MagicMock, patch

from harness.import_one import DEFAULT_MAX_DISTANCE, apply_max_distance
from lib.beets import FORCE_IMPORT_DISTANCE_THRESHOLD

#: A candidate distance that sits ABOVE the ordinary ceiling and below the
#: force override, so the ceiling alone decides apply-versus-reject. Every
#: other fixture in this module sits under both, which is exactly why the
#: comparison had no behavioural coverage before.
DECIDING_DISTANCE = 0.9


def _make_harness_proc(messages: list[dict]) -> MagicMock:
    """Create a mock Popen that emits a sequence of JSON messages on stdout.

    Each message is a JSON line. After all messages, readline() returns "".
    """
    proc = MagicMock()
    proc.pid = 12345
    proc.stdin = MagicMock()
    proc.stderr = MagicMock()
    proc.stderr.read.return_value = ""

    lines = [json.dumps(m) + "\n" for m in messages] + [""]
    stdout_mock = MagicMock()
    stdout_mock.fileno.return_value = 99
    stdout_mock.readline = MagicMock(side_effect=lines)
    proc.stdout = stdout_mock

    proc.poll.return_value = 0
    proc.wait.return_value = 0
    return proc


TARGET_MBID = "aaaaaaaa-1111-2222-3333-bbbbbbbbbbbb"
OTHER_MBID = "cccccccc-4444-5555-6666-dddddddddddd"


def _choose_match(**candidate_overrides: object) -> dict:
    """A complete choose_match message with one candidate.

    Every required wire key is present (#1278 item 8) so the strict
    ``ChooseMatchMessage`` decode inside ``run_import`` succeeds and each
    test exercises the behaviour it names, not a schema violation.
    """
    candidate: dict = {
        "album_id": TARGET_MBID,
        "distance": 0.05,
        "artist": "The National",
        "album": "High Violet",
        "data_source": "MusicBrainz",
        "tracks": [],
        "mapping": [],
        "extra_items": [],
        "extra_tracks": [],
    }
    candidate.update(candidate_overrides)
    return {
        "type": "choose_match",
        "task_id": 0,
        "path": "/tmp/test",
        "cur_artist": str(candidate["artist"]),
        "cur_album": str(candidate["album"]),
        "item_count": 10,
        "items": [],
        "recommendation": "strong",
        "candidate_count": 1,
        "candidates": [candidate],
    }


def _coverage_message(
    *,
    mapped_paths: list[str],
    extra_paths: list[str],
    distance: float = 0.01,
    data_source: str = "Discogs",
) -> dict:
    admitted = ["A1.flac", "A2.1.flac", "A2.2.flac"]
    composite_path = (
        "A2.1.flac"
        if data_source == "Discogs" and "A2.2.flac" in extra_paths
        else None
    )
    return {
        "type": "choose_match",
        "task_id": 0,
        "path": "/tmp/test",
        "item_count": len(admitted),
        "items": [
            {
                "path": path,
                "length": 6.0 if path == composite_path else 0.0,
            }
            for path in admitted
        ],
        "cur_artist": "David Bowie",
        "cur_album": "David Bowie",
        "recommendation": "strong",
        "candidate_count": 1,
        "candidates": [{
            "album_id": "2823685",
            "distance": distance,
            "artist": "David Bowie",
            "album": "David Bowie",
            "data_source": data_source,
            "tracks": [],
            "extra_tracks": [],
            "mapping": [
                {
                    "item": {
                        "path": path,
                        "length": 6.0 if path == composite_path else 0.0,
                    },
                    "track": {
                        "title": path,
                        "length": 18.0 if path == composite_path else 0.0,
                        "discogs_indexed_component_count": (
                            2 if path == composite_path else 1
                        ),
                    },
                }
                for path in mapped_paths
            ],
            "extra_items": [{"path": path} for path in extra_paths],
        }],
    }


class TestApplyDistanceCeiling(unittest.TestCase):
    """The ceiling decides apply versus reject — the outcome, not a kwarg.

    Everything else that asserts a ceiling in this repo asserts the VALUE
    handed to ``run_import``. That is a proxy: until these tests landed, no
    test in the tree ever took the ``dist > max_distance`` branch, so a
    ceiling of 0.5 and a ceiling of 999 were behaviourally identical
    everywhere and every ceiling assertion patrolled a bystander.
    """

    @patch("harness.import_one.select.select", return_value=([99], [], []))
    @patch("harness.import_one.subprocess.Popen")
    def test_over_ceiling_candidate_is_rejected_not_applied(
        self,
        mock_popen,
        _mock_select,
    ):
        from harness import import_one

        proc = _make_harness_proc([_choose_match(distance=DECIDING_DISTANCE)])
        mock_popen.return_value = proc

        outcome = import_one.run_import(
            "/tmp/test", TARGET_MBID, max_distance=DEFAULT_MAX_DISTANCE)

        self.assertEqual(outcome.exit_code, 2)
        self.assertIn("exceeded", outcome.failure_reason or "")
        self.assertEqual(outcome.applied_distance, DECIDING_DISTANCE)
        writes = "".join(call.args[0] for call in proc.stdin.write.call_args_list)
        self.assertIn('"skip"', writes)
        self.assertNotIn('"apply"', writes)

    @patch("harness.import_one.select.select", return_value=([99], [], []))
    @patch("harness.import_one.subprocess.Popen")
    def test_the_same_candidate_applies_under_the_force_ceiling(
        self,
        mock_popen,
        _mock_select,
    ):
        """Same world, one changed input: the force override applies it."""
        from harness import import_one

        proc = _make_harness_proc([_choose_match(distance=DECIDING_DISTANCE)])
        mock_popen.return_value = proc

        outcome = import_one.run_import(
            "/tmp/test",
            TARGET_MBID,
            max_distance=apply_max_distance(force=True),
        )

        self.assertEqual(outcome.exit_code, 0)
        self.assertIsNone(outcome.failure_reason)
        self.assertEqual(outcome.applied_distance, DECIDING_DISTANCE)
        writes = "".join(call.args[0] for call in proc.stdin.write.call_args_list)
        self.assertIn('"apply"', writes)

    @patch("harness.import_one.select.select", return_value=([99], [], []))
    @patch("harness.import_one.subprocess.Popen")
    def test_bowie_retry_applies_a_high_distance_candidate_under_the_force_ceiling(
        self,
        mock_popen,
        _mock_select,
    ):
        """The Discogs retry runs under the SAME ceiling as the first pass.

        Reverting the retry to ``DEFAULT_MAX_DISTANCE`` makes the second
        pass skip a candidate the first pass was authorized to apply — the
        exact defect ``run_import``'s docstring legislates against, and
        invisible to every other test because no other fixture's distance
        sits between the two ceilings.
        """
        from harness import import_one

        default = _make_harness_proc([_coverage_message(
            mapped_paths=["A1.flac", "A2.1.flac"],
            extra_paths=["A2.2.flac"],
        )])
        expanded = _make_harness_proc([_coverage_message(
            mapped_paths=["A1.flac", "A2.1.flac", "A2.2.flac"],
            extra_paths=[],
            distance=DECIDING_DISTANCE,
        )])
        mock_popen.side_effect = [default, expanded]

        outcome = import_one.run_import(
            "/tmp/test",
            "2823685",
            max_distance=FORCE_IMPORT_DISTANCE_THRESHOLD,
        )

        self.assertEqual(outcome.exit_code, 0)
        self.assertEqual(outcome.applied_distance, DECIDING_DISTANCE)
        self.assertIn(
            '"apply"',
            "".join(call.args[0] for call in expanded.stdin.write.call_args_list),
        )


class TestRunImportAudioCoverage(unittest.TestCase):
    @patch("harness.import_one.select.select", return_value=([99], [], []))
    @patch("harness.import_one.subprocess.Popen")
    def test_action_inventory_catches_audio_beets_did_not_admit(
        self,
        mock_popen,
        _mock_select,
    ):
        from harness import import_one

        with tempfile.TemporaryDirectory() as album:
            for name in ("A1.flac", "A2.flac"):
                open(os.path.join(album, name), "wb").close()
            message = _coverage_message(
                mapped_paths=["A1.flac"],
                extra_paths=[],
                data_source="MusicBrainz",
            )
            message["path"] = album
            message["item_count"] = 1
            message["items"] = [{"path": "A1.flac"}]
            proc = _make_harness_proc([message])
            mock_popen.return_value = proc

            outcome = import_one.run_import(
                album, "2823685", max_distance=DEFAULT_MAX_DISTANCE)

        self.assertEqual(outcome.exit_code, 2)
        self.assertIn("A2.flac", outcome.failure_reason or "")
        writes = "".join(call.args[0] for call in proc.stdin.write.call_args_list)
        self.assertNotIn('"apply"', writes)

    @patch("harness.import_one.select.select", return_value=([99], [], []))
    @patch("harness.import_one.subprocess.Popen")
    def test_bowie_retries_then_applies_complete_flat_mapping(
        self,
        mock_popen,
        _mock_select,
    ):
        from harness import import_one

        default = _make_harness_proc([_coverage_message(
            mapped_paths=["A1.flac", "A2.1.flac"],
            extra_paths=["A2.2.flac"],
        )])
        expanded = _make_harness_proc([_coverage_message(
            mapped_paths=["A1.flac", "A2.1.flac", "A2.2.flac"],
            extra_paths=[],
        )])
        mock_popen.side_effect = [default, expanded]

        outcome = import_one.run_import(
            "/tmp/test", "2823685", max_distance=DEFAULT_MAX_DISTANCE)

        self.assertEqual(outcome.exit_code, 0)
        self.assertEqual(outcome.admitted_audio_count, 3)
        self.assertEqual(outcome.applied_audio_count, 3)
        self.assertIn(
            '"skip"',
            "".join(call.args[0] for call in default.stdin.write.call_args_list),
        )
        self.assertIn(
            '"apply"',
            "".join(call.args[0] for call in expanded.stdin.write.call_args_list),
        )
        second_cmd = mock_popen.call_args_list[1].args[0]
        self.assertIn("--preserve-discogs-flat-subtracks", second_cmd)

    @patch("harness.import_one.select.select", return_value=([99], [], []))
    @patch("harness.import_one.subprocess.Popen")
    def test_force_distance_override_still_rejects_discarded_audio(
        self,
        mock_popen,
        _mock_select,
    ):
        """The coverage guard returns BEFORE the distance ceiling is read.

        That ordering is the whole point: a force import raises the ceiling
        to 999, and this proves the audio-loss refusal is not reached around
        by it. The consequence is that ``max_distance``'s VALUE is
        irrelevant here — the comparison never runs. Its own behaviour lives
        in ``TestApplyDistanceCeiling``.
        """
        from harness import import_one

        default = _make_harness_proc([_coverage_message(
            mapped_paths=["A1.flac", "A2.1.flac"],
            extra_paths=["A2.2.flac"],
            distance=900.0,
        )])
        expanded = _make_harness_proc([_coverage_message(
            mapped_paths=["A1.flac", "A2.1.flac"],
            extra_paths=["A2.2.flac"],
            distance=900.0,
        )])
        mock_popen.side_effect = [default, expanded]

        outcome = import_one.run_import(
            "/tmp/test",
            "2823685",
            max_distance=import_one.apply_max_distance(force=True),
        )

        self.assertEqual(outcome.exit_code, 2)
        self.assertIn("discard admitted audio", outcome.failure_reason or "")
        writes = "".join(
            call.args[0]
            for proc in (default, expanded)
            for call in proc.stdin.write.call_args_list
        )
        self.assertNotIn('"apply"', writes)

    @patch("harness.import_one.select.select", return_value=([99], [], []))
    @patch("harness.import_one.subprocess.Popen")
    def test_non_discogs_incomplete_mapping_never_retries_or_applies(
        self,
        mock_popen,
        _mock_select,
    ):
        from harness import import_one

        proc = _make_harness_proc([_coverage_message(
            mapped_paths=["A1.flac", "A2.1.flac"],
            extra_paths=["A2.2.flac"],
            data_source="MusicBrainz",
        )])
        mock_popen.return_value = proc

        outcome = import_one.run_import(
            "/tmp/test", "2823685", max_distance=DEFAULT_MAX_DISTANCE)

        self.assertEqual(outcome.exit_code, 2)
        self.assertEqual(mock_popen.call_count, 1)
        writes = "".join(call.args[0] for call in proc.stdin.write.call_args_list)
        self.assertNotIn('"apply"', writes)


class TestRunImportDuplicateGuard(unittest.TestCase):
    """Test that run_import gates Beets duplicate removal by release identity."""

    @patch("harness.import_one.select.select")
    @patch("harness.import_one.subprocess.Popen")
    def test_different_edition_fails_duplicate_remove_guard(self, mock_popen, mock_select):
        """Different-release duplicate sets fail before beets remove."""
        from harness import import_one

        messages = [
            {"type": "resolve_duplicate", "duplicate_candidates": [
                {"beets_album_id": 10, "mb_albumid": OTHER_MBID,
                 "album_path": "/Beets/Other", "item_count": 11},
            ]},
            _choose_match(),
        ]
        proc = _make_harness_proc(messages)
        mock_popen.return_value = proc
        # select.select always says stdout is ready
        mock_select.return_value = ([99], [], [])

        outcome = import_one.run_import(
            "/tmp/test", TARGET_MBID, max_distance=DEFAULT_MAX_DISTANCE)

        self.assertEqual(outcome.exit_code,
                         import_one.DUPLICATE_REMOVE_GUARD_EXIT_CODE)
        self.assertIsNotNone(outcome.duplicate_remove_guard)
        assert outcome.duplicate_remove_guard is not None
        self.assertEqual(outcome.duplicate_remove_guard.reason,
                         "release_identity_mismatch")
        writes = "".join(
            call.args[0] for call in proc.stdin.write.call_args_list)
        self.assertIn('"skip"', writes)
        self.assertNotIn('"remove"', writes)

    @patch("harness.import_one.select.select")
    @patch("harness.import_one.subprocess.Popen")
    def test_same_mbid_single_duplicate_allows_beets_remove(self, mock_popen, mock_select):
        """One same-release duplicate answers remove."""
        from harness import import_one

        messages = [
            {"type": "resolve_duplicate", "duplicate_candidates": [
                {"beets_album_id": 10, "mb_albumid": TARGET_MBID,
                 "album_path": "/Beets/Target", "item_count": 10},
            ]},
            _choose_match(),
        ]
        proc = _make_harness_proc(messages)
        mock_popen.return_value = proc
        mock_select.return_value = ([99], [], [])

        outcome = import_one.run_import(
            "/tmp/test", TARGET_MBID, max_distance=DEFAULT_MAX_DISTANCE)

        self.assertEqual(outcome.exit_code, 0)
        self.assertTrue(outcome.beets_owned_replacement)
        writes = "".join(
            call.args[0] for call in proc.stdin.write.call_args_list)
        self.assertIn('"remove"', writes)

    @patch("harness.import_one.select.select")
    @patch("harness.import_one.subprocess.Popen")
    def test_no_duplicate_not_kept(self, mock_popen, mock_select):
        """Normal import without duplicate resolution succeeds."""
        from harness import import_one

        messages = [
            _choose_match(distance=0.02),
        ]
        proc = _make_harness_proc(messages)
        mock_popen.return_value = proc
        mock_select.return_value = ([99], [], [])

        outcome = import_one.run_import(
            "/tmp/test", TARGET_MBID, max_distance=DEFAULT_MAX_DISTANCE)

        self.assertEqual(outcome.exit_code, 0)
        self.assertFalse(outcome.beets_owned_replacement)

    @patch("harness.import_one.os.killpg")
    @patch("harness.import_one.os.getpgid", return_value=12345)
    @patch("harness.import_one.select.select")
    @patch("harness.import_one.subprocess.Popen")
    def test_timeout_returns_import_failure(self, mock_popen, mock_select,
                                            mock_getpgid, mock_killpg):
        """On timeout, run_import returns an import failure."""
        from harness import import_one

        proc = MagicMock()
        proc.pid = 12345
        proc.stdin = MagicMock()
        proc.stdout = MagicMock()
        proc.stdout.fileno.return_value = 99
        proc.stderr = MagicMock()
        proc.stderr.read.return_value = ""
        proc.wait.return_value = 1
        mock_popen.return_value = proc
        # select returns empty = timeout
        mock_select.return_value = ([], [], [])

        outcome = import_one.run_import(
            "/tmp/test", TARGET_MBID, max_distance=DEFAULT_MAX_DISTANCE)

        self.assertEqual(outcome.exit_code, 2)

    @patch("harness.import_one.select.select")
    @patch("harness.import_one.subprocess.Popen")
    def test_skip_returns_mbid_missing(self, mock_popen, mock_select):
        """When MBID is not found in candidates, the import is skipped."""
        from harness import import_one

        messages = [
            _choose_match(album_id="wrong-mbid", distance=0.02,
                          artist="X", album="Y"),
        ]
        proc = _make_harness_proc(messages)
        mock_popen.return_value = proc
        mock_select.return_value = ([99], [], [])

        outcome = import_one.run_import(
            "/tmp/test", TARGET_MBID, max_distance=DEFAULT_MAX_DISTANCE)

        self.assertEqual(outcome.exit_code, 4)

    @patch("harness.import_one.select.select")
    @patch("harness.import_one.subprocess.Popen")
    def test_harness_nonzero_after_apply_returns_error(self, mock_popen, mock_select):
        """A harness crash after applying a candidate must still fail run_import."""
        from harness import import_one

        messages = [
            _choose_match(distance=0.02),
        ]
        proc = _make_harness_proc(messages)
        proc.poll.return_value = 2
        proc.wait.return_value = 2
        proc.stderr.read.return_value = (
            "beets.dbcore.db.DBAccessError: attempt to write a readonly database\n"
        )
        mock_popen.return_value = proc
        mock_select.return_value = ([99], [], [])

        outcome = import_one.run_import(
            "/tmp/test", TARGET_MBID, max_distance=DEFAULT_MAX_DISTANCE)

        self.assertEqual(outcome.exit_code, 2)
        self.assertIn("readonly database", "\n".join(outcome.beets_lines))

    @patch("harness.import_one.select.select")
    @patch("harness.import_one.subprocess.Popen")
    def test_matched_candidate_schema_violation_fails_with_exit_2(
            self, mock_popen, mock_select):
        """A matched candidate the strict decode refuses is a failed import.

        The target matches by raw ``album_id``, so the strict
        ``ChooseMatchMessage`` convert is reached; with a required wire
        key missing (#1278 item 8) it must surface as exit 2 with the
        schema violation named — never be swallowed as a skip-and-continue
        (that mutant survived the whole suite before this pin existed).
        """
        from harness import import_one

        message = _choose_match()
        del message["candidates"][0]["data_source"]
        proc = _make_harness_proc([message])
        mock_popen.return_value = proc
        mock_select.return_value = ([99], [], [])

        outcome = import_one.run_import(
            "/tmp/test", TARGET_MBID, max_distance=DEFAULT_MAX_DISTANCE)

        self.assertEqual(outcome.exit_code, 2)
        assert outcome.failure_reason is not None
        self.assertIn("schema violation", outcome.failure_reason)
        self.assertIn("data_source", outcome.failure_reason)
        writes = "".join(
            call.args[0] for call in proc.stdin.write.call_args_list)
        self.assertIn('"skip"', writes)


class TestHarnessDuplicateRemoveGuard(unittest.TestCase):
    """Invariant: ``remove`` crosses the wire only for one exact duplicate."""

    @patch("harness.import_one.select.select")
    @patch("harness.import_one.subprocess.Popen")
    def test_same_mbid_only(self, mock_popen, mock_select):
        from harness import import_one

        messages = [
            {"type": "resolve_duplicate", "duplicate_candidates": [
                {"beets_album_id": 10, "mb_albumid": TARGET_MBID},
            ]},
            _choose_match(artist="X", album="Y"),
        ]
        proc = _make_harness_proc(messages)
        mock_popen.return_value = proc
        mock_select.return_value = ([99], [], [])

        import_one.run_import(
            "/tmp/test", TARGET_MBID, max_distance=DEFAULT_MAX_DISTANCE)

        writes = "".join(
            call.args[0] for call in proc.stdin.write.call_args_list)
        self.assertIn('"remove"', writes)

    @patch("harness.import_one.select.select")
    @patch("harness.import_one.subprocess.Popen")
    def test_different_mbid_only(self, mock_popen, mock_select):
        from harness import import_one

        messages = [
            {"type": "resolve_duplicate", "duplicate_candidates": [
                {"beets_album_id": 10, "mb_albumid": OTHER_MBID},
            ]},
            _choose_match(artist="X", album="Y"),
        ]
        proc = _make_harness_proc(messages)
        mock_popen.return_value = proc
        mock_select.return_value = ([99], [], [])

        import_one.run_import(
            "/tmp/test", TARGET_MBID, max_distance=DEFAULT_MAX_DISTANCE)

        writes = "".join(
            call.args[0] for call in proc.stdin.write.call_args_list)
        self.assertNotIn('"remove"', writes)
        self.assertIn('"skip"', writes)

    @patch("harness.import_one.select.select")
    @patch("harness.import_one.subprocess.Popen")
    def test_palo_santo_mixed_dup_mbids_preserves_sibling(
            self, mock_popen, mock_select):
        """Palo Santo shape: target plus sibling duplicates fail closed."""
        from harness import import_one

        messages = [
            {"type": "resolve_duplicate", "duplicate_candidates": [
                {"beets_album_id": 10, "mb_albumid": TARGET_MBID},
                {"beets_album_id": 11, "mb_albumid": OTHER_MBID},
            ]},
            _choose_match(artist="X", album="Y"),
        ]
        proc = _make_harness_proc(messages)
        mock_popen.return_value = proc
        mock_select.return_value = ([99], [], [])

        outcome = import_one.run_import(
            "/tmp/test", TARGET_MBID, max_distance=DEFAULT_MAX_DISTANCE)

        self.assertEqual(outcome.exit_code,
                         import_one.DUPLICATE_REMOVE_GUARD_EXIT_CODE)
        assert outcome.duplicate_remove_guard is not None
        self.assertEqual(outcome.duplicate_remove_guard.reason,
                         "duplicate_count_not_one")
        writes = "".join(
            call.args[0] for call in proc.stdin.write.call_args_list)
        self.assertNotIn('"remove"', writes)
        self.assertIn('"skip"', writes)


if __name__ == "__main__":
    unittest.main()
