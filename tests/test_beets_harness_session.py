#!/usr/bin/env python3
"""Deterministic pins for the harness-session evidence contract (issue #888).

**The invariants this module owns.**

1. *No silent rejection.* ``beets_validate`` NEVER returns a result whose
   ``scenario`` is ``None``. Either a ``choose_match`` was decoded and
   decided, or the run is named ``no_choose_match``.
2. *Evidence accompanies the name.* ``harness_session`` is populated on
   exactly the ``no_choose_match`` results and on no others, and those
   results always carry a non-empty ``detail``.
3. *The evidence is observation, not inference.* It records the harness
   message types, whether a ``session_end`` was announced, and the stderr
   tail — never a claimed cause.
4. *Naming the scenario does not move the download.* ``no_choose_match``
   stays a Wrong Matches candidate, exactly as the unnamed ``None`` was, so
   the quarantine tree and the operator worklist are unchanged.
5. *The operator-facing copy is reachable from the producer.* The verdict
   Recents renders is triggered by the string ``beets_validate`` actually
   writes, produced here by running it.

The live defect these pin: 276 ``download_log`` rows across 215 requests,
2026-04-19 to 2026-07-21, carried ``outcome='rejected'`` with a NULL
``beets_scenario``, NULL ``error_message`` and NULL ``beets_detail``. All
276 have ``validation_result.target_mbid`` set (only ``lib/beets.py`` writes
it), ``error`` null, ``recommendation`` null and ``local_track_count`` null
— the pristine ``ValidationResult(target_mbid=…)`` that ``beets_validate``
returns when its stdout loop finishes without a ``choose_match``. The
operator saw the bare word "Rejected"; 181 of the 276 were later re-previewed
as importable.

**Why a real subprocess.** ``.claude/rules/test-fidelity.md`` Rule B: the
process boundary is the one leaf seam ``beets_validate`` owns, and the
evidence it now persists comes from that boundary's real stdout, stderr and
EOF behaviour. A ``MagicMock`` proc would hand a mock object straight into a
JSONB audit field and prove nothing about it.
"""

from __future__ import annotations

import os
import stat
import tempfile
import unittest
from collections.abc import Sequence

import msgspec

from lib.beets import (
    NO_CHOOSE_MATCH_SCENARIO,
    _STDERR_TAIL_CHARS,
    beets_validate,
)
from lib.import_manifest import move_failed_import_curated
from lib.quality import ValidationResult
from lib.wrong_match_policy import (
    WRONG_MATCH_QUARANTINE_DIR,
    rejection_scenario_is_wrong_match_candidate,
)
from lib.wrong_matches import wrong_match_row_is_visible
from web.classify import LogEntry, classify_log_entry


TARGET_MBID = "aaaaaaaa-1111-2222-3333-444444444444"

#: The verdict Recents must render for a run that offered no match. Pinned
#: here so the copy has exactly one spelling in the tests.
NO_CHOOSE_MATCH_VERDICT = "Beets ended without offering a match to review"


# ---------------------------------------------------------------------------
# The fake harness — a real executable at the real process boundary
# ---------------------------------------------------------------------------

#: Emitted stdout, then stdout is CLOSED so the parent's read loop reaches
#: EOF; then stderr, then stderr is closed. Closing stdout first is what
#: keeps a large stderr from deadlocking against a parent that only drains
#: stderr after the stdout loop. The trailing sleep keeps the process alive
#: so the parent's ``{"action":"skip"}`` writes never hit a closed pipe —
#: ``beets_validate``'s ``finally`` terminates it immediately afterwards.
_HARNESS_TEMPLATE = """#!/bin/sh
cat {stdout_file}
exec 1>&-
cat {stderr_file} >&2
exec 2>&-
sleep 20
"""


def write_fake_harness(
    directory: str,
    *,
    stdout_lines: Sequence[str],
    stderr_text: str = "",
) -> str:
    """Write an executable stand-in for ``run_beets_harness.sh``.

    Returns the harness path to hand to ``beets_validate``. The lines are
    emitted verbatim, so a caller can plant malformed JSON, blank lines, or
    nothing at all.
    """
    stdout_file = os.path.join(directory, "harness_stdout.txt")
    stderr_file = os.path.join(directory, "harness_stderr.txt")
    body = "".join(f"{line}\n" for line in stdout_lines)
    with open(stdout_file, "w", encoding="utf-8") as handle:
        handle.write(body)
    with open(stderr_file, "w", encoding="utf-8") as handle:
        handle.write(stderr_text)

    harness_path = os.path.join(directory, "fake_harness.sh")
    with open(harness_path, "w", encoding="utf-8") as handle:
        handle.write(_HARNESS_TEMPLATE.format(
            stdout_file=_shell_quote(stdout_file),
            stderr_file=_shell_quote(stderr_file),
        ))
    os.chmod(harness_path, os.stat(harness_path).st_mode | stat.S_IEXEC)
    return harness_path


def _shell_quote(path: str) -> str:
    return "'" + path.replace("'", "'\\''") + "'"


def run_fake_harness(
    stdout_lines: Sequence[str],
    *,
    stderr_text: str = "",
    target_mbid: str = TARGET_MBID,
    distance_threshold: float = 0.15,
) -> ValidationResult:
    """Drive the REAL ``beets_validate`` against a real fake harness."""
    with tempfile.TemporaryDirectory() as tmpdir:
        album_dir = os.path.join(tmpdir, "Artist - Album")
        os.makedirs(album_dir)
        harness_path = write_fake_harness(
            tmpdir, stdout_lines=stdout_lines, stderr_text=stderr_text)
        return beets_validate(
            harness_path, album_dir, target_mbid, distance_threshold)


def choose_match_line(
    *,
    album_id: str = TARGET_MBID,
    distance: float = 0.05,
    item_count: int = 3,
    extra_tracks: int = 0,
) -> str:
    """One well-formed ``choose_match`` message, as the harness emits it."""
    candidate: dict[str, object] = {
        "index": 0,
        "distance": distance,
        "artist": "Artist",
        "album": "Album",
        "album_id": album_id,
        "track_count": item_count,
        "albumstatus": "Official",
        "extra_tracks": [
            {"title": f"Bonus {index}"} for index in range(extra_tracks)
        ],
    }
    return msgspec.json.encode({
        "type": "choose_match",
        "task_id": 0,
        "path": "/staged/Artist - Album",
        "cur_artist": "Artist",
        "cur_album": "Album",
        "item_count": item_count,
        "items": [
            {"title": f"Track {index}"} for index in range(item_count)
        ],
        "recommendation": "strong",
        "candidates": [candidate],
    }).decode()


SESSION_END_LINE = '{"type": "session_end"}'
SHOULD_RESUME_LINE = '{"type": "should_resume", "path": "/staged"}'

#: A ``choose_match`` the strict wire boundary refuses: ``album_id`` is an
#: int, the PR #98 shape. The message arrives but is never processed.
UNDECODABLE_CHOOSE_MATCH_LINE = (
    '{"type": "choose_match", "task_id": 0, "path": "/staged", '
    '"candidates": [{"album_id": 2085134, "distance": 0.05}]}'
)


# ---------------------------------------------------------------------------
# Invariant checkers — module level, so the known-bad self-tests can call them
# ---------------------------------------------------------------------------

DECIDED_SCENARIOS = frozenset({
    "strong_match", "high_distance", "extra_tracks", "mbid_not_found",
})


def assert_scenario_is_always_named(result: ValidationResult) -> None:
    """Invariant 1: no result leaves ``beets_validate`` unnamed."""
    assert result.scenario is not None, (
        "beets_validate returned a result with no scenario — the silent "
        "rejection of issue #888")
    assert result.scenario in DECIDED_SCENARIOS | {NO_CHOOSE_MATCH_SCENARIO}, (
        f"unknown scenario {result.scenario!r}")


def assert_evidence_accompanies_the_name(result: ValidationResult) -> None:
    """Invariant 2: ``harness_session`` marks exactly the unmatched runs."""
    named_no_match = result.scenario == NO_CHOOSE_MATCH_SCENARIO
    assert (result.harness_session is not None) == named_no_match, (
        f"scenario {result.scenario!r} and harness_session "
        f"{result.harness_session!r} disagree")
    if not named_no_match:
        return
    assert result.detail, "a no_choose_match result must explain itself"
    session = result.harness_session
    assert session is not None
    assert list(dict.fromkeys(session.message_types)) == session.message_types, (
        "message_types must be ordered-unique")
    if session.session_end_seen:
        assert "session_end" in session.message_types, (
            "session_end_seen without the message type recorded")
    if not session.message_types:
        assert "harness messages: none" in result.detail, (
            "a harness that said nothing must say so in the detail")
    for message_type in session.message_types:
        assert message_type in result.detail, (
            f"detail omits observed message type {message_type!r}")
    if session.stderr_tail is not None:
        assert len(session.stderr_tail) <= _STDERR_TAIL_CHARS
        assert session.stderr_tail.strip(), "an empty tail must be None"


#: The exact clause ``beets_validate`` composes for itself. Everything
#: after the first ``;`` is quoted from the harness or from the exception
#: text, so only this leading clause is OURS to be wrong in.
NO_MATCH_DETAIL_PREFIX = (
    "beets harness ended without offering a match to review "
    "(harness messages: "
)


def assert_evidence_claims_no_cause(result: ValidationResult) -> None:
    """Invariant 3: our own copy states an observation, never a diagnosis.

    Pinned as an exact prefix rather than a keyword scan so that changing
    the sentence forces a visit to this doctrine, and so that words the
    HARNESS chose (its message types, its traceback) are never mistaken for
    a claim Cratedigger made.
    """
    if result.scenario != NO_CHOOSE_MATCH_SCENARIO:
        return
    detail = result.detail or ""
    assert detail.startswith(NO_MATCH_DETAIL_PREFIX), (
        f"the no-match detail no longer opens with the observation: {detail!r}")
    ours = detail.split(";")[0].casefold()
    for forbidden in (
        "no importable audio", "crashed", "because", "corrupt",
        "the folder is empty", "beets could not read",
    ):
        assert forbidden not in ours, (
            f"the no-match detail claims a cause it cannot know: {forbidden!r}")


def assert_stays_a_wrong_match_candidate(scenario: str | None) -> None:
    """Invariant 4: naming the scenario does not reroute the download."""
    assert rejection_scenario_is_wrong_match_candidate(scenario), (
        f"{scenario!r} left the Wrong Matches taxonomy — the download would "
        "move quarantine trees and vanish from the operator worklist")


def assert_result_round_trips(result: ValidationResult) -> None:
    """The blob really is persistable: JSONB in, JSONB out, unchanged."""
    decoded = ValidationResult.from_json(result.to_json())
    assert decoded.scenario == result.scenario
    assert decoded.detail == result.detail
    assert decoded.harness_session == result.harness_session


# ---------------------------------------------------------------------------
# Pins
# ---------------------------------------------------------------------------

class TestSessionsThatOfferNoMatch(unittest.TestCase):
    """The exact worlds the live cohort is consistent with."""

    def _assert_all_invariants(self, result: ValidationResult) -> None:
        assert_scenario_is_always_named(result)
        assert_evidence_accompanies_the_name(result)
        assert_evidence_claims_no_cause(result)
        assert_stays_a_wrong_match_candidate(result.scenario)
        assert_result_round_trips(result)

    def test_a_clean_session_that_offered_nothing_is_named_and_evidenced(self):
        """The harness ran, announced its session end, and never offered a
        match — the shape consistent with a folder beets found nothing
        importable in."""
        result = run_fake_harness([SESSION_END_LINE])

        self.assertEqual(result.scenario, NO_CHOOSE_MATCH_SCENARIO)
        self.assertIsNone(result.error)
        session = result.harness_session
        assert session is not None
        self.assertEqual(session.message_types, ["session_end"])
        self.assertTrue(session.session_end_seen)
        self.assertIsNone(session.stderr_tail)
        assert result.detail is not None
        self.assertIn("session_end", result.detail)
        self._assert_all_invariants(result)

    def test_a_harness_that_died_before_speaking_keeps_its_traceback(self):
        """The 2026-06-28/29 shape: 254 of the 276 live rows. Nothing on
        stdout, a Python traceback on stderr — which used to reach the
        journal and nowhere else."""
        traceback = (
            "Traceback (most recent call last):\n"
            '  File "/nix/store/xxx/beets/library.py", line 42, in __init__\n'
            "    self._connect()\n"
            "sqlite3.OperationalError: database is locked\n"
        )
        result = run_fake_harness([], stderr_text=traceback)

        self.assertEqual(result.scenario, NO_CHOOSE_MATCH_SCENARIO)
        session = result.harness_session
        assert session is not None
        self.assertEqual(session.message_types, [])
        self.assertFalse(session.session_end_seen)
        assert session.stderr_tail is not None
        self.assertIn("sqlite3.OperationalError: database is locked",
                      session.stderr_tail)
        assert result.detail is not None
        self.assertIn("harness messages: none", result.detail)
        # The single most diagnostic line — the bottom of the traceback —
        # rides along in the column the card shows.
        self.assertIn("sqlite3.OperationalError: database is locked",
                      result.detail)
        self._assert_all_invariants(result)

    def test_a_long_stderr_keeps_its_tail_not_its_head(self):
        """A traceback's cause is at the BOTTOM; the bounded copy keeps it."""
        noise = "\n".join(f"  frame {index}" for index in range(4000))
        traceback = f"{noise}\nValueError: the actual cause\n"
        result = run_fake_harness([], stderr_text=traceback)

        session = result.harness_session
        assert session is not None
        assert session.stderr_tail is not None
        self.assertLessEqual(len(session.stderr_tail), _STDERR_TAIL_CHARS)
        self.assertTrue(session.stderr_tail.endswith("ValueError: the actual cause"))
        self._assert_all_invariants(result)

    def test_an_undecodable_choose_match_is_not_a_processed_match(self):
        """The PR #98 int-``album_id`` shape. The message arrived, so it is
        recorded — but nothing was decided, so the run still owes its
        no-match evidence, alongside the schema-violation error."""
        result = run_fake_harness(
            [UNDECODABLE_CHOOSE_MATCH_LINE, SESSION_END_LINE])

        self.assertEqual(result.scenario, NO_CHOOSE_MATCH_SCENARIO)
        assert result.error is not None
        self.assertIn("album_id", result.error)
        session = result.harness_session
        assert session is not None
        self.assertEqual(session.message_types, ["choose_match", "session_end"])
        assert result.detail is not None
        self.assertIn("harness schema violation", result.detail)
        self._assert_all_invariants(result)

    def test_a_harness_that_cannot_start_is_named_too(self):
        """No process, no match, and the reason still reaches the row."""
        with tempfile.TemporaryDirectory() as tmpdir:
            album_dir = os.path.join(tmpdir, "Artist - Album")
            os.makedirs(album_dir)
            result = beets_validate(
                os.path.join(tmpdir, "does-not-exist.sh"),
                album_dir, TARGET_MBID, 0.15)

        self.assertEqual(result.scenario, NO_CHOOSE_MATCH_SCENARIO)
        assert result.error is not None
        self.assertIn("Failed to start harness", result.error)
        assert result.detail is not None
        self.assertIn("Failed to start harness", result.detail)
        self._assert_all_invariants(result)

    def test_non_json_chatter_alone_still_offers_no_match(self):
        result = run_fake_harness(
            ["", "beets: importing …", "not json at all", SESSION_END_LINE])

        self.assertEqual(result.scenario, NO_CHOOSE_MATCH_SCENARIO)
        session = result.harness_session
        assert session is not None
        self.assertEqual(session.message_types, ["session_end"])
        self._assert_all_invariants(result)


class TestDecidedMatchesAreUnchanged(unittest.TestCase):
    """Must-still-work: naming the gap must not touch the ordinary paths."""

    CASES = [
        ("strong match", TARGET_MBID, 0.05, 0, True, "strong_match"),
        ("high distance", TARGET_MBID, 0.4, 0, False, "high_distance"),
        ("extra tracks", TARGET_MBID, 0.02, 2, False, "extra_tracks"),
        ("other pressing", "bbbb-2222", 0.05, 0, False, "mbid_not_found"),
    ]

    def test_every_decided_scenario_keeps_its_name_and_no_evidence(self):
        for desc, album_id, distance, extra, valid, scenario in self.CASES:
            with self.subTest(desc):
                result = run_fake_harness([
                    choose_match_line(
                        album_id=album_id,
                        distance=distance,
                        extra_tracks=extra,
                    ),
                    SESSION_END_LINE,
                ])
                self.assertEqual(result.scenario, scenario)
                self.assertEqual(result.valid, valid)
                self.assertIsNone(
                    result.harness_session,
                    "a decided match must carry no no-match evidence")
                assert_scenario_is_always_named(result)
                assert_evidence_accompanies_the_name(result)
                assert_result_round_trips(result)

    def test_a_match_after_harness_chatter_is_still_decided(self):
        result = run_fake_harness([
            SHOULD_RESUME_LINE, choose_match_line(), SESSION_END_LINE])
        self.assertEqual(result.scenario, "strong_match")
        self.assertTrue(result.valid)
        self.assertIsNone(result.harness_session)

    def test_a_decodable_match_after_an_undecodable_one_still_decides(self):
        """The schema-violation branch continues the loop; a later,
        well-formed message must still be processed and must clear the
        no-match stamp."""
        result = run_fake_harness([
            UNDECODABLE_CHOOSE_MATCH_LINE, choose_match_line(),
            SESSION_END_LINE,
        ])
        self.assertEqual(result.scenario, "strong_match")
        self.assertIsNone(result.harness_session)


class TestNamingTheScenarioDoesNotMoveTheDownload(unittest.TestCase):
    """Invariant 4, composed: real producer → real quarantine allocator."""

    def _quarantine(self, scenario: str | None, root: str) -> str:
        album_dir = os.path.join(root, "source", "Artist - Album")
        os.makedirs(album_dir)
        track = os.path.join(album_dir, "01 - Track.mp3")
        with open(track, "w", encoding="utf-8") as handle:
            handle.write("audio")
        target = move_failed_import_curated(
            album_dir,
            allowed_audio=["01 - Track.mp3"],
            scenario=scenario,
            quarantine_root=os.path.join(root, "quarantine"),
        )
        assert target is not None
        return target

    def test_the_named_scenario_lands_where_the_unnamed_one_did(self):
        """``None`` and ``no_choose_match`` are both Wrong Matches
        candidates, so the tree the download lands in is byte-identical —
        the naming is routing-neutral by construction."""
        produced = run_fake_harness([SESSION_END_LINE]).scenario
        self.assertEqual(produced, NO_CHOOSE_MATCH_SCENARIO)
        with tempfile.TemporaryDirectory() as tmpdir:
            named = self._quarantine(
                produced, os.path.join(tmpdir, "named"))
            unnamed = self._quarantine(
                None, os.path.join(tmpdir, "unnamed"))
        self.assertEqual(
            os.path.relpath(named, os.path.join(tmpdir, "named")),
            os.path.relpath(unnamed, os.path.join(tmpdir, "unnamed")),
        )
        self.assertIn(f"{os.sep}{WRONG_MATCH_QUARANTINE_DIR}{os.sep}", named)
        self.assertNotIn(f"{os.sep}failed_imports{os.sep}", named)

    def test_the_row_stays_in_the_operator_worklist(self):
        produced = run_fake_harness([SESSION_END_LINE]).scenario
        for scenario in (None, produced):
            with self.subTest(scenario=scenario):
                self.assertTrue(wrong_match_row_is_visible({
                    "request_status": "wanted",
                    "validation_result": {
                        "failed_path": "/quarantine/wrong_matches/Album",
                        "scenario": scenario,
                    },
                    "candidate_audio_corrupt": False,
                    "terminal_import_decision": None,
                }))


class TestTheCopyIsReachableFromTheProducer(unittest.TestCase):
    """Rule C, in its strongest form: the trigger is produced, not typed."""

    def _classified_verdict(self, result: ValidationResult) -> str:
        entry = LogEntry(
            id=1,
            request_id=2,
            outcome="rejected",
            beets_scenario=result.scenario,
            beets_detail=result.detail,
            error_message=result.error,
            validation_result=result.to_json(),
        )
        return classify_log_entry(entry).verdict

    def test_a_real_no_match_run_renders_the_named_verdict(self):
        result = run_fake_harness([SESSION_END_LINE])
        self.assertEqual(
            self._classified_verdict(result), NO_CHOOSE_MATCH_VERDICT)

    def test_the_verdict_never_claims_a_cause_the_row_cannot_prove(self):
        verdict = self._classified_verdict(run_fake_harness([SESSION_END_LINE]))
        lowered = verdict.casefold()
        for overclaim in (
            "no audio", "corrupt", "crashed", "empty folder", "no candidates",
            "musicbrainz", "denylist",
        ):
            self.assertNotIn(overclaim, lowered)

    def test_the_row_no_longer_reads_as_the_bare_word_rejected(self):
        """The whole defect, end to end: before #888 this row's scenario was
        NULL and the classifier's fallback rendered "Rejected"."""
        result = run_fake_harness([SESSION_END_LINE])
        self.assertNotEqual(self._classified_verdict(result), "Rejected")
        unnamed = LogEntry(
            id=1, request_id=2, outcome="rejected", beets_scenario=None)
        self.assertEqual(classify_log_entry(unnamed).verdict, "Rejected")


class TestInvariantCheckersTripOnViolations(unittest.TestCase):
    """Known-bad self-tests: a checker that cannot fail proves nothing."""

    def test_an_unnamed_scenario_trips_the_name_checker(self) -> None:
        with self.assertRaises(AssertionError):
            assert_scenario_is_always_named(ValidationResult())

    def test_an_invented_scenario_trips_the_name_checker(self) -> None:
        with self.assertRaises(AssertionError):
            assert_scenario_is_always_named(
                ValidationResult(scenario="validation_rejected"))

    def test_a_named_gap_without_evidence_trips_the_evidence_checker(self):
        with self.assertRaises(AssertionError):
            assert_evidence_accompanies_the_name(ValidationResult(
                scenario=NO_CHOOSE_MATCH_SCENARIO, detail="something"))

    def test_a_decided_match_carrying_evidence_trips_the_checker(self) -> None:
        from lib.quality import HarnessSessionEvidence

        with self.assertRaises(AssertionError):
            assert_evidence_accompanies_the_name(ValidationResult(
                scenario="strong_match",
                harness_session=HarnessSessionEvidence()))

    def test_a_named_gap_without_a_detail_trips_the_checker(self) -> None:
        from lib.quality import HarnessSessionEvidence

        with self.assertRaises(AssertionError):
            assert_evidence_accompanies_the_name(ValidationResult(
                scenario=NO_CHOOSE_MATCH_SCENARIO,
                harness_session=HarnessSessionEvidence()))

    def test_an_unrecorded_message_type_trips_the_checker(self) -> None:
        from lib.quality import HarnessSessionEvidence

        with self.assertRaises(AssertionError):
            assert_evidence_accompanies_the_name(ValidationResult(
                scenario=NO_CHOOSE_MATCH_SCENARIO,
                detail="beets harness ended without offering a match",
                harness_session=HarnessSessionEvidence(
                    message_types=["session_end"], session_end_seen=True)))

    def test_a_claimed_cause_trips_the_no_inference_checker(self) -> None:
        from lib.quality import HarnessSessionEvidence

        with self.assertRaises(AssertionError):
            assert_evidence_claims_no_cause(ValidationResult(
                scenario=NO_CHOOSE_MATCH_SCENARIO,
                detail="No importable audio in the folder",
                harness_session=HarnessSessionEvidence()))
        # …and a diagnosis smuggled INTO the observation clause, where the
        # prefix check alone would not see it.
        with self.assertRaises(AssertionError):
            assert_evidence_claims_no_cause(ValidationResult(
                scenario=NO_CHOOSE_MATCH_SCENARIO,
                detail=(
                    f"{NO_MATCH_DETAIL_PREFIX}none) because the folder was "
                    "corrupt"
                ),
                harness_session=HarnessSessionEvidence()))

    def test_the_harness_own_words_are_not_treated_as_our_claim(self) -> None:
        """A traceback that happens to say "corrupt" is the harness
        talking, quoted after the ``;`` — not a cause we asserted."""
        from lib.quality import HarnessSessionEvidence

        assert_evidence_claims_no_cause(ValidationResult(
            scenario=NO_CHOOSE_MATCH_SCENARIO,
            detail=(
                f"{NO_MATCH_DETAIL_PREFIX}none); harness stderr ended: "
                "sqlite3.DatabaseError: database disk image is corrupt"
            ),
            harness_session=HarnessSessionEvidence()))

    def test_leaving_the_wrong_match_taxonomy_trips_the_routing_checker(self):
        with self.assertRaises(AssertionError):
            assert_stays_a_wrong_match_candidate("spectral_reject")

    def test_a_blob_that_drops_the_evidence_trips_the_round_trip_checker(self):
        from lib.quality import HarnessSessionEvidence

        class _Lossy(ValidationResult):
            def to_json(self) -> str:
                return msgspec.json.encode({
                    "scenario": self.scenario, "detail": self.detail}).decode()

        with self.assertRaises(AssertionError):
            assert_result_round_trips(_Lossy(
                scenario=NO_CHOOSE_MATCH_SCENARIO,
                detail="d",
                harness_session=HarnessSessionEvidence(
                    message_types=["session_end"])))


if __name__ == "__main__":
    unittest.main()
