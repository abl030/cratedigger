"""Tests for beets validation pipeline in cratedigger.

Since cratedigger.py has heavy external dependencies (slskd_api, music_tag),
we mock at the module level before importing, or test via subprocess simulation.
"""

import inspect
import json
import os
import shutil
import sys
import tempfile
import unittest
from collections.abc import Iterator
from unittest.mock import MagicMock, patch

# Heavy third-party deps (``requests``, ``music_tag``, ``slskd_api``)
# used to be mocked here at module-discovery time, before the dev shell
# (nix/package.nix) was complete. Now they are real packages provided
# by ``nix-shell`` and importing them is harmless. Keeping the mocks
# would also pollute ``sys.modules['requests']`` for every subsequent
# test in alphabetical order — tripping up exception-catching code
# (e.g. ``lib.youtube_album_service``) that uses real
# ``requests.Timeout`` / ``ConnectionError`` exception classes.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from lib import beets as lib_beets
from lib.beets import _beets_validate_once, apply_candidate_scenario, beets_validate
from lib.beets_child import spawn_harness_session
from lib.grab_list import GrabListEntry
from lib.processing_paths import stage_to_ai_path
from lib.quality import (
    CandidateSummary,
    HarnessItem,
    HarnessTrackInfo,
    TrackMapping,
    ValidationResult,
)
from lib.staged_album import StagedAlbum
from lib.util import log_validation_result
from tests.helpers import make_candidate_summary


def complete_candidate(**overrides: object) -> dict[str, object]:
    """One candidate with every required wire key present (#1278 item 8)."""
    base: dict[str, object] = {
        "index": 0,
        "distance": 0.05,
        "artist": "Test Artist",
        "album": "Test Album",
        "album_id": "12345678-1234-1234-1234-123456789abc",
        "data_source": "MusicBrainz",
        "year": 2020,
        "country": "US",
        "track_count": 10,
        "albumstatus": "Official",
        "tracks": [],
        "mapping": [],
        "extra_items": [],
        "extra_tracks": [],
    }
    base.update(overrides)
    return base


def complete_msg(candidates: list[dict[str, object]], **overrides: object) -> str:
    """A choose_match message carrying every required wire key."""
    base: dict[str, object] = {
        "type": "choose_match",
        "task_id": 0,
        "path": "/test/path",
        "cur_artist": "Test Artist",
        "cur_album": "Test Album",
        "item_count": 10,
        "items": [],
        "recommendation": "strong",
        "candidate_count": len(candidates),
        "candidates": candidates,
    }
    base.update(overrides)
    return json.dumps(base)


def make_choose_match_msg(mb_release_id, distance, extra_candidates=None):
    """Build a choose_match JSON message with the given MBID and distance."""
    candidates = [complete_candidate(album_id=mb_release_id, distance=distance)]
    if extra_candidates:
        candidates.extend(extra_candidates)
    return complete_msg(candidates)


def make_session_end():
    return json.dumps({"type": "session_end"})


def make_should_resume():
    return json.dumps({"type": "should_resume", "path": "/test/path"})


def make_coverage_choose_match_msg(
    release_id: str,
    *,
    mapped_paths: list[str],
    extra_paths: list[str],
    data_source: str = "Discogs",
    composite_path: str | None = None,
    composite_local_length: float = 0.0,
    composite_program_length: float = 0.0,
    composite_duration_complete: bool = True,
    item_paths: list[str] | None = None,
) -> str:
    if item_paths is None:
        item_paths = [
            "01 Space Oddity.flac",
            "02 Unwashed And Somewhat Slightly Dazed.flac",
            "03 Don't Sit Down.flac",
        ]
    items = [
        {
            "path": path,
            "length": (
                composite_local_length if path == composite_path else 0.0
            ),
        }
        for path in item_paths
    ]
    mapping = [
        {
            "item": {
                "path": path,
                "length": (
                    composite_local_length if path == composite_path else 0.0
                ),
            },
            "track": {
                "title": path,
                "length": (
                    composite_program_length if path == composite_path else 0.0
                ),
                "discogs_indexed_component_count": (
                    2 if path == composite_path else 1
                ),
                "discogs_indexed_duration_complete": (
                    composite_duration_complete
                    if path == composite_path
                    else True
                ),
            },
        }
        for path in mapped_paths
    ]
    return complete_msg(
        [complete_candidate(
            distance=0.01,
            artist="David Bowie",
            album="David Bowie",
            album_id=release_id,
            data_source=data_source,
            track_count=len(mapping),
            mapping=mapping,
            extra_items=[{"path": path} for path in extra_paths],
        )],
        cur_artist="David Bowie",
        cur_album="David Bowie",
        item_count=len(items),
        items=items,
    )


class FakeHarnessStdin:
    """Typed stand-in for the session's decision pipe; records writes."""

    def __init__(self) -> None:
        self.written: list[str] = []

    def write(self, data: str, /) -> int:
        self.written.append(data)
        return len(data)

    def flush(self) -> None:
        return None


class FakeHarnessStderr:
    def __init__(self, text: str = "") -> None:
        self._text = text

    def read(self) -> str:
        return self._text


class FakeHarnessSession:
    """Typed ``lib.beets_child.HarnessSession`` fake — replaces the
    MagicMock procs the retired ``lib.beets.sp.Popen`` module-attribute
    patches used to feed past the seam."""

    stdin: FakeHarnessStdin
    stdout: Iterator[str]
    stderr: FakeHarnessStderr

    def __init__(
        self,
        lines: list[str],
        *,
        stderr: str = "",
        returncode: int = 0,
    ) -> None:
        self.stdin = FakeHarnessStdin()
        self.stdout = iter(lines)
        self.stderr = FakeHarnessStderr(stderr)
        self._returncode = returncode

    def wait(self, timeout: float | None = None) -> int:
        return self._returncode

    def terminate(self) -> None:
        return None

    def kill(self) -> None:
        return None


class RecordingSpawn:
    """Injected ``spawn`` seam: queued sessions plus recorded argv."""

    def __init__(
        self,
        *sessions: FakeHarnessSession,
        raises: Exception | None = None,
    ) -> None:
        self._sessions = list(sessions)
        self._raises = raises
        self.calls: list[list[str]] = []

    def __call__(self, argv: list[str]) -> FakeHarnessSession:
        self.calls.append(list(argv))
        if self._raises is not None:
            raise self._raises
        return self._sessions.pop(0)

    @property
    def call_count(self) -> int:
        return len(self.calls)


def make_validation_proc(message: str) -> FakeHarnessSession:
    return FakeHarnessSession([message + "\n", make_session_end() + "\n"])


class TestBeetsValidate(unittest.TestCase):
    """Test beets_validate() through the injected ``spawn`` seam.

    Every fake session's ``stderr.read()`` returns a real string: since
    issue #888 ``beets_validate`` PERSISTS what that call returns into
    ``validation_result.harness_session``, so a mock object there would feed
    into a JSONB audit field — test infrastructure more permissive than
    production (``.claude/rules/test-fidelity.md`` Rule B). The pins that
    drive a REAL harness subprocess over that boundary live in
    ``tests/test_beets_harness_session.py``.
    """

    HARNESS = "/fake/harness.sh"

    def test_discogs_bowie_retries_flat_subtracks_for_complete_mapping(
        self,
    ):
        release_id = "2823685"
        default = make_validation_proc(make_coverage_choose_match_msg(
            release_id,
            mapped_paths=[
                "01 Space Oddity.flac",
                "02 Unwashed And Somewhat Slightly Dazed.flac",
            ],
            extra_paths=["03 Don't Sit Down.flac"],
            composite_path="02 Unwashed And Somewhat Slightly Dazed.flac",
            composite_local_length=369.0,
            composite_program_length=408.0,
        ))
        expanded = make_validation_proc(make_coverage_choose_match_msg(
            release_id,
            mapped_paths=[
                "01 Space Oddity.flac",
                "02 Unwashed And Somewhat Slightly Dazed.flac",
                "03 Don't Sit Down.flac",
            ],
            extra_paths=[],
        ))
        spawn = RecordingSpawn(default, expanded)

        result = beets_validate(
            self.HARNESS, "/test/album", release_id, 0.15, spawn=spawn,
        )

        self.assertTrue(result.valid)
        self.assertEqual(result.scenario, "strong_match")
        self.assertEqual(len(result.candidates[0].mapping), 3)
        self.assertEqual(spawn.call_count, 2)
        self.assertIn("--preserve-discogs-flat-subtracks", spawn.calls[1])

    def test_unkle_style_unmapped_file_with_no_reported_extra_retries_flat(
        self,
    ):
        """Issue #1237's second confirmation shape: 13 admitted files, one
        genuinely UNMAPPED with zero ``extra_items`` reported (unlike Bowie's
        shape, which Beets reports as an extra item) -- the retry trigger
        must fire on ``unmapped_paths`` alone, not only on
        ``reported_extra_paths``.
        """
        release_id = "2823685"
        item_paths = [f"{n:02d} Track {n}.flac" for n in range(1, 14)]
        composite_path = item_paths[1]
        default = make_validation_proc(make_coverage_choose_match_msg(
            release_id,
            item_paths=item_paths,
            mapped_paths=item_paths[:12],
            extra_paths=[],
            composite_path=composite_path,
            composite_local_length=100.0,
            composite_program_length=220.0,
        ))
        expanded = make_validation_proc(make_coverage_choose_match_msg(
            release_id,
            item_paths=item_paths,
            mapped_paths=item_paths,
            extra_paths=[],
        ))
        spawn = RecordingSpawn(default, expanded)

        result = beets_validate(
            self.HARNESS, "/test/album", release_id, 0.15, spawn=spawn,
        )

        self.assertTrue(result.valid)
        self.assertEqual(result.scenario, "strong_match")
        self.assertEqual(len(result.candidates[0].mapping), 13)
        self.assertEqual(spawn.call_count, 2)
        self.assertIn("--preserve-discogs-flat-subtracks", spawn.calls[1])

    def test_overlapping_composite_validates_without_retry_and_carries_evidence(
        self,
    ):
        """Issue #1237: a composite duration disagreement with an otherwise
        COMPLETE mapping (no unmapped/extra audio) must validate on the
        FIRST pass -- no retry needed, no rejection -- while still
        persisting the observation on the result.
        """
        release_id = "2823685"
        composite_path = "02 Unwashed And Somewhat Slightly Dazed.flac"
        proc = make_validation_proc(make_coverage_choose_match_msg(
            release_id,
            mapped_paths=[
                "01 Space Oddity.flac",
                composite_path,
                "03 Don't Sit Down.flac",
            ],
            extra_paths=[],
            composite_path=composite_path,
            composite_local_length=579.7,
            composite_program_length=781.0,
        ))
        spawn = RecordingSpawn(proc)

        result = beets_validate(
            self.HARNESS, "/test/album", release_id, 0.15, spawn=spawn,
        )

        self.assertTrue(result.valid, result.to_json())
        self.assertEqual(result.scenario, "strong_match")
        self.assertEqual(spawn.call_count, 1)
        self.assertEqual(
            result.incomplete_composite_paths,
            [f"{composite_path} (local=579.7s, indexed_program=781.0s)"],
        )

    def test_force_distance_override_cannot_bypass_incomplete_mapping(
        self,
    ):
        release_id = "2823685"
        default = make_validation_proc(make_coverage_choose_match_msg(
            release_id,
            mapped_paths=[
                "01 Space Oddity.flac",
                "02 Unwashed And Somewhat Slightly Dazed.flac",
            ],
            extra_paths=["03 Don't Sit Down.flac"],
            composite_path="02 Unwashed And Somewhat Slightly Dazed.flac",
            composite_local_length=369.0,
            composite_program_length=408.0,
        ))
        still_incomplete = make_validation_proc(make_coverage_choose_match_msg(
            release_id,
            mapped_paths=[
                "01 Space Oddity.flac",
                "02 Unwashed And Somewhat Slightly Dazed.flac",
            ],
            extra_paths=["03 Don't Sit Down.flac"],
            composite_path="02 Unwashed And Somewhat Slightly Dazed.flac",
            composite_local_length=369.0,
            composite_program_length=408.0,
        ))
        spawn = RecordingSpawn(default, still_incomplete)

        result = beets_validate(
            self.HARNESS, "/test/album", release_id, 999.0, spawn=spawn,
        )

        self.assertFalse(result.valid)
        self.assertEqual(result.scenario, "unmapped_audio")
        self.assertIn("Don't Sit Down", result.detail or "")

    def test_complete_composite_plus_extra_audio_does_not_retry_flat(
        self,
    ):
        release_id = "2823685"
        proc = make_validation_proc(make_coverage_choose_match_msg(
            release_id,
            mapped_paths=[
                "01 Space Oddity.flac",
                "02 Unwashed And Somewhat Slightly Dazed.flac",
            ],
            extra_paths=["03 Don't Sit Down.flac"],
            composite_path="02 Unwashed And Somewhat Slightly Dazed.flac",
            composite_local_length=408.0,
            composite_program_length=408.0,
        ))
        spawn = RecordingSpawn(proc)

        result = beets_validate(
            self.HARNESS, "/test/album", release_id, 999.0, spawn=spawn,
        )

        self.assertFalse(result.valid)
        self.assertEqual(result.scenario, "unmapped_audio")
        self.assertIn("beets extra_items", result.detail or "")
        self.assertEqual(spawn.call_count, 1)

    def test_non_discogs_incomplete_mapping_fails_without_retry(self):
        release_id = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
        proc = make_validation_proc(make_coverage_choose_match_msg(
            release_id,
            mapped_paths=[
                "01 Space Oddity.flac",
                "02 Unwashed And Somewhat Slightly Dazed.flac",
            ],
            extra_paths=["03 Don't Sit Down.flac"],
            data_source="MusicBrainz",
        ))
        spawn = RecordingSpawn(proc)

        result = beets_validate(
            self.HARNESS, "/test/album", release_id, 0.15, spawn=spawn,
        )

        self.assertFalse(result.valid)
        self.assertEqual(result.scenario, "unmapped_audio")
        self.assertEqual(spawn.call_count, 1)

    def test_good_match(self):
        """Distance 0.05 with threshold 0.15 → valid=True."""
        mbid = "12345678-1234-1234-1234-123456789abc"
        proc = make_validation_proc(make_choose_match_msg(mbid, 0.05))
        spawn = RecordingSpawn(proc)

        result = beets_validate(
            self.HARNESS, "/test/album", mbid, 0.15, spawn=spawn,
        )

        self.assertTrue(result.valid)
        self.assertTrue(result.mbid_found)
        assert result.distance is not None
        self.assertAlmostEqual(result.distance, 0.05)
        self.assertIsNone(result.error)
        # Verify skip was sent (dry-run)
        self.assertEqual(proc.stdin.written[-1], '{"action":"skip"}\n')

    def test_high_distance(self):
        """Distance 0.30 with threshold 0.15 → valid=False."""
        mbid = "12345678-1234-1234-1234-123456789abc"
        spawn = RecordingSpawn(
            make_validation_proc(make_choose_match_msg(mbid, 0.30)),
        )

        result = beets_validate(
            self.HARNESS, "/test/album", mbid, 0.15, spawn=spawn,
        )

        self.assertFalse(result.valid)
        self.assertTrue(result.mbid_found)
        assert result.distance is not None
        self.assertAlmostEqual(result.distance, 0.30)

    def test_mbid_not_found(self):
        """Target MBID not in candidates → valid=False, mbid_found=False."""
        target_mbid = "aaaaaaaa-1111-2222-3333-444444444444"
        wrong_mbid = "bbbbbbbb-1111-2222-3333-444444444444"
        spawn = RecordingSpawn(
            make_validation_proc(make_choose_match_msg(wrong_mbid, 0.05)),
        )

        result = beets_validate(
            self.HARNESS, "/test/album", target_mbid, 0.15, spawn=spawn,
        )

        self.assertFalse(result.valid)
        self.assertFalse(result.mbid_found)
        self.assertIsNone(result.distance)
        self.assertEqual(result.scenario, "mbid_not_found")

    def test_a_long_shared_prefix_is_not_the_target(self):
        """Strict pressing identity: the candidate match is full-string
        equality, never a prefix — two real Discogs releases can share
        eight leading digits (review round 2, mutant-runner survivor M12)."""
        target = "208513400"
        near_miss = "208513401"
        spawn = RecordingSpawn(
            make_validation_proc(make_choose_match_msg(near_miss, 0.02)),
        )

        result = beets_validate(
            self.HARNESS, "/test/album", target, 0.15, spawn=spawn,
        )

        self.assertFalse(result.mbid_found)
        self.assertFalse(result.valid)
        self.assertEqual(result.scenario, "mbid_not_found")

    def test_no_candidates(self):
        """Empty candidates list → valid=False."""
        spawn = RecordingSpawn(make_validation_proc(
            complete_msg([], path="/test"),
        ))

        result = beets_validate(
            self.HARNESS, "/test/album", "some-mbid", 0.15, spawn=spawn,
        )

        self.assertFalse(result.valid)
        self.assertFalse(result.mbid_found)
        self.assertEqual(result.scenario, "mbid_not_found")

    def test_subprocess_start_failure(self):
        """Harness fails to start → valid=False, error set, and the run
        still owes its issue-#888 evidence stamp (scenario + session
        evidence) — the branch is a named validation_error, not a bare
        error string (review round 2, mutant-runner observation M14)."""
        spawn = RecordingSpawn(raises=FileNotFoundError("No such file"))

        result = beets_validate(
            self.HARNESS, "/test/album", "some-mbid", 0.15, spawn=spawn,
        )

        self.assertFalse(result.valid)
        assert result.error is not None
        self.assertIn("Failed to start harness", result.error)
        self.assertEqual(result.scenario, "validation_error")
        self.assertIsNotNone(result.harness_session)

    def test_a_hanging_session_is_killed_and_named_a_timeout(self):
        """The hang-kill wiring end to end through the seam: a session
        whose stdout never yields must be killed at
        HARNESS_SESSION_TIMEOUT_SECONDS (patched short here) and the run
        named a timeout (review round 2, mutant-runner survivor M13)."""
        import threading

        class _BlockingStdout:
            def __init__(self) -> None:
                self.released = threading.Event()

            def __iter__(self) -> "_BlockingStdout":
                return self

            def __next__(self) -> str:
                if self.released.wait(timeout=30):
                    raise StopIteration
                raise AssertionError("timeout kill never released the read")

        class _HangingSession(FakeHarnessSession):
            def __init__(self) -> None:
                super().__init__([])
                self.blocking = _BlockingStdout()
                self.stdout = self.blocking
                self.kill_calls = 0

            def kill(self) -> None:
                self.kill_calls += 1
                self.blocking.released.set()

        session = _HangingSession()
        spawn = RecordingSpawn(session)
        with patch.object(
            lib_beets, "HARNESS_SESSION_TIMEOUT_SECONDS", 0.05,
        ):
            result = beets_validate(
                self.HARNESS, "/test/album", "some-mbid", 0.15, spawn=spawn,
            )

        self.assertEqual(session.kill_calls, 1)
        self.assertEqual(result.error, "Harness timed out after 0.05s")
        self.assertEqual(result.scenario, "validation_error")
        self.assertFalse(result.valid)

    def test_long_stderr_logged_in_full(self):
        """A multi-frame Python traceback in harness stderr must be logged
        in full, not silently truncated. Without the full text, operators
        cannot diagnose ``library.Library()`` crashes (or any other harness
        startup failure) from journald — this is the exact condition that
        hid the 2026-05-04 Psilodump crash root cause.
        """
        # Real-world traceback from harness library.Library() crash is
        # 1500-2500 chars. Any truncation below this hides the actual
        # cause, which appears in the deepest frames.
        long_stderr = (
            "Traceback (most recent call last):\n"
            + "\n".join(
                f'  File "/nix/store/abc{i:04d}-python3-3.13.12-env/lib/'
                f'python3.13/site-packages/beets/library.py", line {i}, '
                f'in __init__\n    self._connect()  # frame {i}'
                for i in range(20)
            )
            + "\nsqlite3.OperationalError: database is locked\n"
        )
        self.assertGreater(len(long_stderr), 500,
                           "test fixture must exceed the old [:500] slice")

        mbid = "12345678-1234-1234-1234-123456789abc"
        # Harness crashed before sending any JSON: no stdout lines.
        proc = FakeHarnessSession([], stderr=long_stderr, returncode=1)
        spawn = RecordingSpawn(proc)

        with self.assertLogs("cratedigger", level="WARNING") as cm:
            beets_validate(self.HARNESS, "/test/album", mbid, 0.15, spawn=spawn)

        stderr_logs = [r for r in cm.output if "stderr" in r]
        self.assertTrue(stderr_logs,
                        "no stderr log line was emitted")
        joined = "\n".join(stderr_logs)
        # The deepest frames carry the actual cause; assert the FULL
        # traceback is present, not just the first ~500 chars.
        self.assertIn("frame 19", joined,
                      "deep stack frame missing — stderr was truncated")
        self.assertIn("sqlite3.OperationalError: database is locked",
                      joined,
                      "the actual exception line was truncated away")

    def test_validation_session_argv_is_the_pretend_shape(self) -> None:
        """Validation must NEVER run a real-import session: the spawned
        argv is exactly the ``--pretend`` dry-run shape, with the target
        release and album path as the final tokens
        (``lib/beets_child.py::harness_session_argv`` owns the shape)."""
        mbid = "12345678-1234-1234-1234-123456789abc"
        spawn = RecordingSpawn(
            make_validation_proc(make_choose_match_msg(mbid, 0.05)),
        )

        beets_validate(self.HARNESS, "/test/album", mbid, 0.15, spawn=spawn)

        self.assertEqual(spawn.calls[0], [
            self.HARNESS, "--pretend", "--noincremental",
            "--search-id", mbid, "/test/album",
        ])

    def test_production_spawner_is_the_captured_default(self) -> None:
        """Regression guard for the Blueline Medic 0-candidates incident
        class (download_log ids 3604-3616, requests 1710/1711): a harness
        child resolving beets config from the wrong environment returns 0
        candidates for every ``--search-id``. Every test here injects
        ``spawn``, so the one thing no test exercises is that production
        still gets the real spawner — a definition-time default patching
        could never replace. ``spawn_harness_session``'s own env/pipes/text
        behavior is proven against a REAL child in
        ``tests/test_beets_child.py``; the historical hardcoded
        ``HOME=/home/abl030`` assertion described the deleted Home-Manager
        impersonation (tier-2 plan R6) and only ever passed host-dependently.
        """
        for fn in (beets_validate, _beets_validate_once):
            with self.subTest(fn=fn.__name__):
                default = inspect.signature(fn).parameters["spawn"].default
                self.assertIs(default, spawn_harness_session)

    def test_handles_should_resume_then_choose_match(self):
        """should_resume followed by choose_match → handles both correctly."""
        mbid = "12345678-1234-1234-1234-123456789abc"
        proc = FakeHarnessSession([
            make_should_resume() + "\n",
            make_choose_match_msg(mbid, 0.03) + "\n",
            make_session_end() + "\n",
        ])
        spawn = RecordingSpawn(proc)

        result = beets_validate(
            self.HARNESS, "/test/album", mbid, 0.15, spawn=spawn,
        )

        self.assertTrue(result.valid)
        # Two skip calls: one for should_resume, one for choose_match —
        # and both must actually SAY skip: a session that answered a
        # resume/duplicate prompt with "apply" would act inside a
        # --pretend validation (review round 2, mutant-runner survivor M10).
        self.assertEqual(proc.stdin.written, ['{"action":"skip"}\n'] * 2)

    def test_exact_threshold(self):
        """Distance exactly at threshold → valid=True."""
        mbid = "12345678-1234-1234-1234-123456789abc"
        spawn = RecordingSpawn(
            make_validation_proc(make_choose_match_msg(mbid, 0.15)),
        )

        result = beets_validate(
            self.HARNESS, "/test/album", mbid, 0.15, spawn=spawn,
        )

        self.assertTrue(result.valid)  # <= threshold

    def test_just_above_threshold(self):
        """Distance 0.1501 is above 0.15 → valid=False."""
        mbid = "12345678-1234-1234-1234-123456789abc"
        spawn = RecordingSpawn(
            make_validation_proc(make_choose_match_msg(mbid, 0.1501)),
        )

        result = beets_validate(
            self.HARNESS, "/test/album", mbid, 0.15, spawn=spawn,
        )

        self.assertFalse(result.valid)
        self.assertEqual(result.scenario, "high_distance")

    def test_above_hard_limit(self):
        """Distance above 0.30 hard limit → valid=False."""
        mbid = "12345678-1234-1234-1234-123456789abc"
        spawn = RecordingSpawn(
            make_validation_proc(make_choose_match_msg(mbid, 0.35)),
        )

        result = beets_validate(
            self.HARNESS, "/test/album", mbid, 0.15, spawn=spawn,
        )

        self.assertFalse(result.valid)
        self.assertEqual(result.scenario, "high_distance")


    def test_extra_tracks_rejected(self):
        """MB has more tracks than local files → valid=False even at low distance."""
        mbid = "12345678-1234-1234-1234-123456789abc"
        candidates = [complete_candidate(
            distance=0.02, album_id=mbid, track_count=12,
            extra_tracks=[{"title": "Bonus 1"}, {"title": "Bonus 2"}],
        )]
        msg = complete_msg(candidates)
        spawn = RecordingSpawn(make_validation_proc(msg))

        result = beets_validate(
            self.HARNESS, "/test/album", mbid, 0.15, spawn=spawn,
        )

        self.assertFalse(result.valid)
        self.assertEqual(result.scenario, "extra_tracks")

    def test_non_official_accepted_if_match(self):
        """Non-official release (bootleg/promo) with good match → valid=True."""
        mbid = "12345678-1234-1234-1234-123456789abc"
        candidates = [complete_candidate(
            album_id=mbid, albumstatus="Bootleg",
        )]
        msg = complete_msg(candidates)
        spawn = RecordingSpawn(make_validation_proc(msg))

        result = beets_validate(
            self.HARNESS, "/test/album", mbid, 0.15, spawn=spawn,
        )

        self.assertTrue(result.valid)

    def test_discogs_numeric_str_album_id_matches(self):
        """Harness-emitted numeric-str album_id matches str target_mbid.

        This is the happy path for Discogs: beets' plugin returns
        album_id as int, the harness normalises it to str via
        `_id_str`, and msgspec decodes it straight into .mbid. The
        strict-typed boundary does not need to coerce — the harness
        has already done its job.
        """
        target_mbid = "2085134"  # numeric Discogs ID stored as str in DB
        candidates = [complete_candidate(
            artist="Blueline Medic", album="The Apology Wars",
            album_id="2085134",  # harness-emitted str
            year=2001, country="AU", track_count=11,
            data_source="Discogs",
        )]
        msg = complete_msg(
            candidates,
            cur_artist="Blueline Medic", cur_album="The Apology Wars",
            item_count=11,
        )
        spawn = RecordingSpawn(make_validation_proc(msg))

        result = beets_validate(
            self.HARNESS, "/test/album", target_mbid, 0.15, spawn=spawn,
        )

        self.assertTrue(result.mbid_found)
        self.assertTrue(result.valid)
        assert result.distance is not None
        self.assertAlmostEqual(result.distance, 0.05)
        self.assertEqual(result.scenario, "strong_match")
        self.assertTrue(result.candidates[0].is_target)
        self.assertEqual(result.candidates[0].mbid, "2085134")
        self.assertIsNone(result.error)

    def test_int_album_id_trips_msgspec_boundary(self):
        """Regression guard for PR #98: an int album_id on the wire
        should never reach downstream consumers. msgspec.convert raises
        ValidationError, beets_validate surfaces it as result.error,
        and returns an empty (invalid) result — loud failure instead of
        the silent `mbid_not_found` miss that was the original bug.

        In production the harness-side `_id_str` normalises all IDs to
        str so this path is unreachable; if it ever trips, the harness
        has regressed and we want to know immediately.
        """
        target_mbid = "2085134"
        # int album_id — the shape of the live bug. Every other wire key is
        # complete, so the ValidationError is specifically the type drift.
        candidates = [complete_candidate(
            artist="X", album="Y", album_id=2085134, track_count=11,
        )]
        msg = complete_msg(
            candidates, cur_artist="X", cur_album="Y", item_count=11,
        )
        spawn = RecordingSpawn(make_validation_proc(msg))

        result = beets_validate(
            self.HARNESS, "/test/album", target_mbid, 0.15, spawn=spawn,
        )

        self.assertFalse(result.valid)
        self.assertFalse(result.mbid_found)
        self.assertIsNotNone(result.error)
        # Error message should name the offending field so operators
        # can see exactly what broke.
        assert result.error is not None  # for pyright
        self.assertIn("album_id", result.error)

    def test_artist_collab_match(self):
        """Collab credit — MBID matches and distance is good → valid=True."""
        mbid = "12345678-1234-1234-1234-123456789abc"
        candidates = [complete_candidate(
            distance=0.06, artist="Action Bronson & Party Supplies",
            album="Blue Chips", album_id=mbid, year=2012, track_count=16,
        )]
        msg = complete_msg(
            candidates,
            cur_artist="Action Bronson", cur_album="Blue Chips",
            item_count=16,
        )
        spawn = RecordingSpawn(make_validation_proc(msg))

        result = beets_validate(
            self.HARNESS, "/test/album", mbid, 0.15, spawn=spawn,
        )

        self.assertTrue(result.valid)
        self.assertEqual(result.scenario, "strong_match")


def _make_album_data(**overrides):
    """Build a minimal GrabListEntry for tests that need album_data."""
    defaults = {
        "album_id": 0, "files": [], "filetype": "mp3", "title": "Test Album",
        "artist": "Test Artist", "year": "2024", "mb_release_id": "",
    }
    defaults.update(overrides)
    return GrabListEntry(**defaults)


class TestApplyCandidateScenarioIdempotence(unittest.TestCase):
    """``apply_candidate_scenario`` must leave no stale field behind when
    called a second time on the SAME ``ValidationResult`` (issue #1237
    review C8/D4/E4/G4). ``lib/download_validation.py``'s merge-redirect
    seam is exactly one such caller, and its OWN call is always the first
    on a given object (D4): it is reached only when ``result.scenario ==
    "mbid_not_found"``, a value this function itself never produces. That
    is not the same as "no caller ever reaches a SECOND call" --
    ``lib/beets.py::_beets_validate_once`` processes every ``choose_match``
    message the harness sends within one session, with last-wins field
    overwrites on the SAME shared ``result``, calling
    ``apply_candidate_scenario`` once per message whose candidate matches
    the target MBID; beets can emit more than one ``choose_match`` in one
    session (``HarnessImportSession.choose_match`` in
    ``harness/beets_harness.py`` numbers each with an incrementing
    ``task_id``, evidence the underlying protocol supports more than one
    per session). Beets' own ``albums_in_dir`` (``beets/importer/tasks.py``)
    is NOT one task per subdirectory as a rule, though: it deliberately
    COLLAPSES a multi-disc layout (``CD1``/``CD2``, ``Disc 1``/``Disc 2``,
    and similar numbered-marker siblings) into a single task -- the most
    common shape a directory with more than one subdirectory takes in this
    pipeline. The mechanism above is real for two genuinely UNRELATED
    album subdirectories under one target path (not a recognised multi-
    disc pattern), which is what a second ``apply_candidate_scenario``
    call actually requires -- a DIFFERENT concept from the later, separate
    ``nested_layout`` quality-decision fact (spelled in
    ``lib/quality/pipeline.py`` and ``lib/import_preview.py``), which is
    computed downstream of validation and names a different failure. A
    second ``apply_candidate_scenario`` call on the same object is
    therefore reachable inside ``beets_validate`` itself whenever more
    than one task's candidates match the requested MBID -- exactly the
    case this test guards, independent of whether a specific live album
    is known to exercise it today.
    """

    def _composite_candidate(self, *, local_length: float, indexed_length: float) -> CandidateSummary:
        path = "composite.flac"
        return make_candidate_summary(
            mbid="release",
            data_source="Discogs",
            mapping=[TrackMapping(
                item=HarnessItem(path=path, length=local_length),
                track=HarnessTrackInfo(
                    title="composite", length=indexed_length,
                    discogs_indexed_component_count=2,
                    discogs_indexed_duration_complete=True,
                ),
            )],
        )

    def test_second_call_clears_first_calls_composite_evidence(self) -> None:
        result = ValidationResult(items=[{"path": "composite.flac", "length": 100.0}])
        first = self._composite_candidate(local_length=100.0, indexed_length=200.0)
        apply_candidate_scenario(result, first, 0.15)
        self.assertEqual(
            result.incomplete_composite_paths,
            ["composite.flac (local=100.0s, indexed_program=200.0s)"],
        )

        second = self._composite_candidate(local_length=200.0, indexed_length=200.0)
        apply_candidate_scenario(result, second, 0.15)
        self.assertEqual(result.incomplete_composite_paths, [])


class TestStagedAlbumMoveTo(unittest.TestCase):
    """Test staging-path construction plus directory move semantics."""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_creates_correct_structure(self):
        """Files moved to staging_dir/Artist/Album/."""
        source = os.path.join(self.tmpdir, "source")
        staging = os.path.join(self.tmpdir, "staging")
        os.makedirs(source)
        os.makedirs(staging)

        for name in ["01 - Track.flac", "02 - Track.flac", "cover.jpg"]:
            open(os.path.join(source, name), "w").close()

        album_data = _make_album_data(artist="Test Artist", title="Test Album")
        dest = stage_to_ai_path(
            artist=album_data.artist,
            title=album_data.title,
            staging_dir=staging,
        )
        staged_album = StagedAlbum(current_path=source)
        staged_album.move_to(dest)

        self.assertEqual(dest, os.path.join(staging, "Test Artist", "Test Album"))
        self.assertTrue(os.path.exists(os.path.join(dest, "01 - Track.flac")))
        self.assertTrue(os.path.exists(os.path.join(dest, "02 - Track.flac")))
        self.assertTrue(os.path.exists(os.path.join(dest, "cover.jpg")))

    def test_cleans_source(self):
        """Source directory removed after staging."""
        source = os.path.join(self.tmpdir, "source")
        staging = os.path.join(self.tmpdir, "staging")
        os.makedirs(source)
        os.makedirs(staging)
        open(os.path.join(source, "track.flac"), "w").close()

        album_data = _make_album_data(artist="Artist", title="Album")
        dest = stage_to_ai_path(
            artist=album_data.artist,
            title=album_data.title,
            staging_dir=staging,
        )
        StagedAlbum(current_path=source).move_to(dest)

        self.assertFalse(os.path.exists(source))

    def test_sanitizes_names(self):
        """Special characters in artist/album are sanitized."""
        source = os.path.join(self.tmpdir, "source")
        staging = os.path.join(self.tmpdir, "staging")
        os.makedirs(source)
        os.makedirs(staging)
        open(os.path.join(source, "track.flac"), "w").close()

        album_data = _make_album_data(artist='Test: "Artist"', title="Album/Title?")
        dest = stage_to_ai_path(
            artist=album_data.artist,
            title=album_data.title,
            staging_dir=staging,
        )
        StagedAlbum(current_path=source).move_to(dest)

        # sanitize_processing_folder_name removes <>:"/\|?*
        self.assertNotIn(":", os.path.basename(os.path.dirname(dest)))
        self.assertNotIn("?", os.path.basename(dest))
        self.assertTrue(os.path.exists(dest))


class TestLogValidationResult(unittest.TestCase):
    """Test log_validation_result() JSONL output."""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self.tracking_file = os.path.join(self.tmpdir, "tracking.jsonl")
        self.mock_cfg = MagicMock()
        self.mock_cfg.beets_tracking_file = self.tracking_file

    def tearDown(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_appends_staged_entry(self):
        """Staged result writes correct JSONL."""
        album_data = _make_album_data(
            artist="Test Artist", title="Test Album",
            mb_release_id="abc-123", album_id=42,
        )
        result = ValidationResult(valid=True, distance=0.05, mbid_found=True)

        log_validation_result(album_data, result, self.mock_cfg,
                              dest_path="/AI/Test Artist/Test Album")

        with open(self.tracking_file) as f:
            entry = json.loads(f.readline())

        self.assertEqual(entry["status"], "staged")
        self.assertEqual(entry["artist"], "Test Artist")
        self.assertEqual(entry["mb_release_id"], "abc-123")
        self.assertEqual(entry["distance"], 0.05)
        self.assertEqual(entry["dest_path"], "/AI/Test Artist/Test Album")

    def test_appends_rejected_entry(self):
        """Rejected result writes status=rejected."""
        album_data = _make_album_data(artist="A", title="B", album_id=1)
        result = ValidationResult(valid=False, distance=0.40, mbid_found=True)

        log_validation_result(album_data, result, self.mock_cfg)

        with open(self.tracking_file) as f:
            entry = json.loads(f.readline())

        self.assertEqual(entry["status"], "rejected")
        self.assertIsNone(entry["dest_path"])

    def test_appends_multiple_entries(self):
        """Multiple calls append to same file."""
        album_data = _make_album_data(artist="A", title="B", album_id=1)
        result = ValidationResult(valid=True, distance=0.01)

        log_validation_result(album_data, result, self.mock_cfg, dest_path="/dest1")
        log_validation_result(album_data, result, self.mock_cfg, dest_path="/dest2")

        with open(self.tracking_file) as f:
            lines = f.readlines()

        self.assertEqual(len(lines), 2)


if __name__ == "__main__":
    unittest.main()
