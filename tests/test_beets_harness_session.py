"""Deterministic pins for the harness-session evidence contract (issue #888).

**The invariants this module owns.**

1. *No silent rejection.* ``beets_validate`` NEVER returns a result whose
   ``scenario`` is ``None``. Either a ``choose_match`` was decoded and
   decided, or the run is named for how it reached no reviewed match.
2. *The name matches the error state.* A run that recorded no error is
   ``no_choose_match`` — beets offered nothing. A run that recorded one is
   ``validation_error`` — validation did not complete. The split is not
   cosmetic: the strict-decode refusal is a world where beets DID offer a
   match and Cratedigger declined to decode it, so naming it
   ``no_choose_match`` would assert the opposite of what happened.
3. *Evidence accompanies the name.* ``harness_session`` is populated on
   exactly those two outcomes and on no others; they always carry a
   non-empty, BOUNDED ``detail``; and they are never ``valid`` — a run with
   no reviewed match can never be an auto-import candidate.
4. *The evidence is observation, not inference.* It records the harness
   message types, whether a ``session_end`` was announced, and the stderr
   tail — never a claimed cause. Our own clause is the first ``;``-separated
   segment, so wire-controlled text can never be read as our assertion.
5. *Naming the scenario does not move the download.* Both names stay Wrong
   Matches candidates, exactly as the unnamed ``None`` was, so the
   quarantine tree and the operator worklist are unchanged.
6. *The operator-facing copy is reachable from the producer.* The verdict
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
from typing import ClassVar

import msgspec

from lib.beets import (
    _DETAIL_MAX_CHARS,
    _STDERR_LINE_CHARS,
    _STDERR_TAIL_CHARS,
    NO_CHOOSE_MATCH_CLAUSE,
    NO_CHOOSE_MATCH_SCENARIO,
    VALIDATION_ERROR_CLAUSE,
    VALIDATION_ERROR_SCENARIO,
    beets_validate,
)
from lib.import_manifest import move_failed_import_curated
from lib.quality import HarnessSessionEvidence, ValidationResult
from lib.wrong_match_policy import (
    WRONG_MATCH_QUARANTINE_DIR,
    rejection_scenario_is_wrong_match_candidate,
)
from lib.wrong_matches import wrong_match_row_is_visible
from web.classify import LogEntry, classify_log_entry

TARGET_MBID = "aaaaaaaa-1111-2222-3333-444444444444"

#: The clause each unmatched scenario composes for itself, taken from the
#: producer rather than retyped (``.claude/rules/test-fidelity.md`` Rule C).
UNMATCHED_CLAUSES: dict[str, str] = {
    NO_CHOOSE_MATCH_SCENARIO: NO_CHOOSE_MATCH_CLAUSE,
    VALIDATION_ERROR_SCENARIO: VALIDATION_ERROR_CLAUSE,
}

#: The verdicts Recents must render. Pinned here so each has exactly one
#: spelling across the test pair.
NO_CHOOSE_MATCH_VERDICT = "Beets ended without offering a match to review"
VALIDATION_ERROR_VERDICT_PREFIX = (
    "Validation failed before a match could be reviewed"
)


# ---------------------------------------------------------------------------
# The fake harness — a real executable at the real process boundary
# ---------------------------------------------------------------------------

#: Emitted stdout, then stdout is CLOSED so the parent's read loop reaches
#: EOF; then stderr, then stderr is closed. Closing stdout first is what
#: keeps a large stderr from deadlocking against a parent that only drains
#: stderr after the stdout loop. The trailing sleep keeps the process alive
#: so the parent's ``{"action":"skip"}`` writes never hit a closed pipe —
#: ``exec`` makes that wait the exact process ``Popen`` owns, so
#: ``beets_validate``'s ``finally`` terminates it without orphaning a child.
_HARNESS_TEMPLATE = """#!/bin/sh
cat {stdout_file}
exec 1>&-
cat {stderr_file} >&2
exec 2>&-
{terminal_action}
"""


def _shell_quote(path: str) -> str:
    return "'" + path.replace("'", "'\\''") + "'"


def write_fake_harness(
    directory: str,
    *,
    stdout_lines: Sequence[str],
    stderr_text: str = "",
    process_returncode: int | None = None,
) -> str:
    """Write an executable stand-in for ``run_beets_harness.sh``.

    Returns the harness path to hand to ``beets_validate``. The lines are
    emitted verbatim, so a caller can plant malformed JSON, blank lines, or
    nothing at all. A negative ``process_returncode`` makes the harness
    signal itself, so ``Popen`` observes a real negative return code — but
    ONLY for a signal no ancestor can be holding: ``SIG_IGN`` and the
    blocked mask are both inherited across ``exec``, and a held signal
    turns the requested death into an ordinary exit 0. Pass
    ``-tests.test_import_one_stages.FAKE_HARNESS_FATAL_SIGNAL``, which
    documents the whole trap, rather than picking a signal number.
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
            terminal_action=(
                "exec sleep 20"
                if process_returncode is None
                else (
                    f"kill -{-process_returncode} $$"
                    if process_returncode < 0
                    else f"exit {process_returncode}"
                )
            ),
        ))
    os.chmod(harness_path, os.stat(harness_path).st_mode | stat.S_IEXEC)
    return harness_path


def assert_fake_harness_wait_is_owned(harness_script: str) -> None:
    """The terminal wait must replace the shell that ``Popen`` owns."""
    commands = {line.strip() for line in harness_script.splitlines()}
    if "sleep 20" in commands or "exec sleep 20" not in commands:
        raise AssertionError(
            "fake harness must exec its terminal wait so terminate() owns it"
        )


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
    items = [
        {
            "path": f"/staged/Artist - Album/{index:02d} Track.flac",
            "title": f"Track {index}",
        }
        for index in range(item_count)
    ]
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
        "mapping": [
            {
                "item": item,
                "track": {"title": item["title"]},
            }
            for item in items
        ],
    }
    return msgspec.json.encode({
        "type": "choose_match",
        "task_id": 0,
        "path": "/staged/Artist - Album",
        "cur_artist": "Artist",
        "cur_album": "Album",
        "item_count": item_count,
        "items": items,
        "recommendation": "strong",
        "candidates": [candidate],
    }).decode()


SESSION_END_LINE = '{"type": "session_end"}'
SHOULD_RESUME_LINE = '{"type": "should_resume", "path": "/staged"}'

#: A ``choose_match`` the strict wire boundary refuses: ``album_id`` is an
#: int, the PR #98 shape. Beets OFFERED a match here — our decoder declined
#: it — which is the whole reason this world gets its own scenario name.
UNDECODABLE_CHOOSE_MATCH_LINE = (
    '{"type": "choose_match", "task_id": 0, "path": "/staged", '
    '"candidates": [{"album_id": 2085134, "distance": 0.05}]}'
)

#: A harness message whose ``type`` is not a string. Nothing in production
#: emits it, but the guard that skips it is real and load-bearing: without
#: it the value reaches ``", ".join(...)`` inside ``_record_unmatched_run``,
#: which runs OUTSIDE the read loop's try/except — so it propagates out of
#: ``beets_validate`` and fails the import job (issue #888 review F5).
NON_STRING_TYPE_LINE = '{"type": 5}'


# ---------------------------------------------------------------------------
# Invariant checkers — module level, so the known-bad self-tests can call them
# ---------------------------------------------------------------------------

DECIDED_SCENARIOS = frozenset({
    "strong_match", "high_distance", "extra_tracks", "mbid_not_found",
})
UNMATCHED_SCENARIOS = frozenset(UNMATCHED_CLAUSES)


def assert_scenario_is_always_named(result: ValidationResult) -> None:
    """Invariant 1: no result leaves ``beets_validate`` unnamed."""
    assert result.scenario is not None, (
        "beets_validate returned a result with no scenario — the silent "
        "rejection of issue #888")
    assert result.scenario in DECIDED_SCENARIOS | UNMATCHED_SCENARIOS, (
        f"unknown scenario {result.scenario!r}")


def assert_the_name_matches_the_error_state(result: ValidationResult) -> None:
    """Invariant 2: the name a run gets is decided by whether it errored.

    ``error is None`` is the same discriminator the issue-#888 RCA used to
    separate the genuine 276 from the four error branches, so production
    uses it too. Without this, the strict-decode refusal — where beets DID
    offer a match — renders a headline asserting beets offered nothing, at
    exactly the moment (a beets field-type change) that would hit every
    album at once.
    """
    if result.scenario not in UNMATCHED_SCENARIOS:
        return
    if result.error is None:
        assert result.scenario == NO_CHOOSE_MATCH_SCENARIO, (
            f"a run with no recorded error was named {result.scenario!r}")
    else:
        assert result.scenario == VALIDATION_ERROR_SCENARIO, (
            f"a run that recorded {result.error!r} was named "
            f"{result.scenario!r}, which asserts beets offered nothing")


def assert_evidence_accompanies_the_name(result: ValidationResult) -> None:
    """Invariant 3: ``harness_session`` marks exactly the unmatched runs."""
    unmatched = result.scenario in UNMATCHED_SCENARIOS
    assert (result.harness_session is not None) == unmatched, (
        f"scenario {result.scenario!r} and harness_session "
        f"{result.harness_session!r} disagree")
    if not unmatched:
        return
    # A run that never reviewed a match can never be an auto-import
    # candidate: ``lib/download_validation.py`` routes on ``valid`` alone and
    # never consults the scenario, so a True here would send an unmatched
    # download straight into dispatch (issue #888 review F2).
    assert not result.valid, (
        "an unmatched run must never be valid — it would enter auto-import "
        "dispatch, which routes on bv_result.valid alone")
    assert result.detail, "an unmatched result must explain itself"
    assert len(result.detail) <= _DETAIL_MAX_CHARS, (
        f"detail is {len(result.detail)} chars; it reaches "
        "download_log.beets_detail and the Recents card")
    session = result.harness_session
    assert session is not None
    assert list(dict.fromkeys(session.message_types)) == session.message_types, (
        "message_types must be ordered-unique")
    if session.session_end_seen:
        assert "session_end" in session.message_types, (
            "session_end_seen without the message type recorded")
    assert "harness messages: " in result.detail, (
        "the detail must name what the harness said")
    if not session.message_types:
        assert "harness messages: none" in result.detail, (
            "a harness that said nothing must say so in the detail")
    elif not result.detail.endswith("…"):
        # Skipped only for a truncated detail, where a tail was dropped by
        # design; the untruncated case must account for every type.
        for message_type in session.message_types:
            assert message_type in result.detail, (
                f"detail omits observed message type {message_type!r}")
    if session.stderr_tail is not None:
        assert len(session.stderr_tail) <= _STDERR_TAIL_CHARS
        assert session.stderr_tail.strip(), "an empty tail must be None"


def assert_evidence_claims_no_cause(result: ValidationResult) -> None:
    """Invariant 4: our own clause is an observation, and stands alone.

    The clause is pinned by EQUALITY against the producer's own constant,
    and it is segment 0 of the ``;``-separated detail — so it contains no
    interpolated, wire-controlled text at all. Words the HARNESS chose (its
    message type names, its traceback) live after the first ``;`` and can
    never be mistaken for a claim Cratedigger made (issue #888 review F6).
    """
    if result.scenario not in UNMATCHED_SCENARIOS:
        return
    detail = result.detail or ""
    expected = UNMATCHED_CLAUSES[result.scenario]
    assert detail.split(";")[0] == expected, (
        f"the {result.scenario} detail no longer opens with its own clause: "
        f"{detail!r}")
    lowered = expected.casefold()
    for forbidden in (
        "no importable audio", "crashed", "because", "corrupt",
        "the folder is empty", "beets could not read",
    ):
        assert forbidden not in lowered, (
            f"the clause claims a cause it cannot know: {forbidden!r}")


def assert_stays_a_wrong_match_candidate(scenario: str | None) -> None:
    """Invariant 5: naming the scenario does not reroute the download."""
    assert rejection_scenario_is_wrong_match_candidate(scenario), (
        f"{scenario!r} left the Wrong Matches taxonomy — the download would "
        "move quarantine trees and vanish from the operator worklist")


def assert_result_round_trips(result: ValidationResult) -> None:
    """The blob really is persistable: JSONB in, JSONB out, unchanged."""
    decoded = ValidationResult.from_json(result.to_json())
    assert decoded.scenario == result.scenario
    assert decoded.detail == result.detail
    assert decoded.harness_session == result.harness_session


def assert_every_invariant(result: ValidationResult) -> None:
    """Every invariant this module owns, against one real run."""
    assert_scenario_is_always_named(result)
    assert_the_name_matches_the_error_state(result)
    assert_evidence_accompanies_the_name(result)
    assert_evidence_claims_no_cause(result)
    assert_stays_a_wrong_match_candidate(result.scenario)
    assert_result_round_trips(result)


# ---------------------------------------------------------------------------
# Pins
# ---------------------------------------------------------------------------

class TestFakeHarnessProcessLifecycle(unittest.TestCase):
    def test_terminal_wait_replaces_the_exact_harness_process(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            harness_path = write_fake_harness(
                tmpdir,
                stdout_lines=[SESSION_END_LINE],
            )
            with open(harness_path, encoding="utf-8") as handle:
                assert_fake_harness_wait_is_owned(handle.read())

    def test_unowned_terminal_wait_is_rejected(self) -> None:
        with self.assertRaisesRegex(AssertionError, "terminate.*owns"):
            assert_fake_harness_wait_is_owned("#!/bin/sh\nsleep 20\n")


class TestSessionsThatOfferedNothing(unittest.TestCase):
    """``no_choose_match``: the run completed and offered no match."""

    def test_a_clean_session_that_offered_nothing_is_named_and_evidenced(self):
        """The harness ran, announced its session end, recorded no error, and
        never offered a match — the shape the live 276 are consistent with."""
        result = run_fake_harness([SESSION_END_LINE])

        self.assertEqual(result.scenario, NO_CHOOSE_MATCH_SCENARIO)
        self.assertIsNone(result.error)
        session = result.harness_session
        assert session is not None
        self.assertEqual(session.message_types, ["session_end"])
        self.assertTrue(session.session_end_seen)
        self.assertIsNone(session.stderr_tail)
        assert result.detail is not None
        self.assertTrue(result.detail.startswith(NO_CHOOSE_MATCH_CLAUSE))
        self.assertIn("session_end", result.detail)
        assert_every_invariant(result)

    def test_non_json_chatter_alone_still_offers_no_match(self):
        result = run_fake_harness(
            ["", "beets: importing …", "not json at all", SESSION_END_LINE])

        self.assertEqual(result.scenario, NO_CHOOSE_MATCH_SCENARIO)
        session = result.harness_session
        assert session is not None
        self.assertEqual(session.message_types, ["session_end"])
        assert_every_invariant(result)

    def test_a_non_string_message_type_is_skipped_not_recorded(self):
        """Issue #888 review F5: without the ``isinstance(msg_type, str)``
        guard the int reaches ``", ".join(...)`` in ``_record_unmatched_run``
        and raises outside the read loop's try/except, propagating out of
        ``beets_validate`` instead of producing a row."""
        result = run_fake_harness([NON_STRING_TYPE_LINE, SESSION_END_LINE])

        self.assertEqual(result.scenario, NO_CHOOSE_MATCH_SCENARIO)
        self.assertIsNone(result.error)
        session = result.harness_session
        assert session is not None
        self.assertEqual(session.message_types, ["session_end"])
        assert_every_invariant(result)

    def test_session_end_makes_later_malformed_output_unobservable(self):
        suffixes = (
            ("non-object JSON", "[1, 2, 3]"),
            ("undecodable choose_match", UNDECODABLE_CHOOSE_MATCH_LINE),
        )
        for name, suffix in suffixes:
            with self.subTest(name=name):
                result = run_fake_harness([SESSION_END_LINE, suffix])

                self.assertEqual(result.scenario, NO_CHOOSE_MATCH_SCENARIO)
                self.assertIsNone(result.error)
                session = result.harness_session
                assert session is not None
                self.assertEqual(session.message_types, ["session_end"])
                self.assertTrue(session.session_end_seen)
                assert_every_invariant(result)

    def test_a_quiet_harness_that_said_nothing_at_all_is_named(self):
        result = run_fake_harness([])

        self.assertEqual(result.scenario, NO_CHOOSE_MATCH_SCENARIO)
        session = result.harness_session
        assert session is not None
        self.assertEqual(session.message_types, [])
        self.assertFalse(session.session_end_seen)
        assert result.detail is not None
        self.assertIn("harness messages: none", result.detail)
        assert_every_invariant(result)

    def test_a_crashing_harness_keeps_its_traceback(self):
        """The 2026-06-28/29 shape: 254 of the 276 live rows. Nothing on
        stdout, a Python traceback on stderr — which used to reach the
        journal and nowhere else. ``beets_validate`` records no error of its
        own for this world (the process simply said nothing), so it keeps the
        ``no_choose_match`` name and the traceback rides in the evidence."""
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
        assert session.stderr_tail is not None
        self.assertIn("sqlite3.OperationalError: database is locked",
                      session.stderr_tail)
        assert result.detail is not None
        self.assertIn("sqlite3.OperationalError: database is locked",
                      result.detail)
        assert_every_invariant(result)


class TestRunsThatRecordedAnError(unittest.TestCase):
    """``validation_error``: validation did not complete (issue #888 F1)."""

    def test_an_undecodable_choose_match_never_claims_beets_offered_nothing(self):
        """The PR #98 int-``album_id`` shape, and the blocker behind the
        split: beets DID offer a match here — the strict wire decode refused
        it. ``harness_session.message_types`` says ``choose_match`` in the
        same row, so a headline claiming nothing was offered would be
        contradicted by its own evidence."""
        result = run_fake_harness(
            [UNDECODABLE_CHOOSE_MATCH_LINE, SESSION_END_LINE])

        self.assertEqual(result.scenario, VALIDATION_ERROR_SCENARIO)
        assert result.error is not None
        self.assertIn("album_id", result.error)
        session = result.harness_session
        assert session is not None
        self.assertEqual(session.message_types, ["choose_match", "session_end"])
        assert result.detail is not None
        self.assertTrue(result.detail.startswith(VALIDATION_ERROR_CLAUSE))
        self.assertIn("harness schema violation", result.detail)
        self.assertNotIn("without offering a match", result.detail)
        assert_every_invariant(result)

    def test_a_harness_that_cannot_start_is_a_validation_error(self):
        """No process, no match, and the reason still reaches the row."""
        with tempfile.TemporaryDirectory() as tmpdir:
            album_dir = os.path.join(tmpdir, "Artist - Album")
            os.makedirs(album_dir)
            result = beets_validate(
                os.path.join(tmpdir, "does-not-exist.sh"),
                album_dir, TARGET_MBID, 0.15)

        self.assertEqual(result.scenario, VALIDATION_ERROR_SCENARIO)
        assert result.error is not None
        self.assertIn("Failed to start harness", result.error)
        assert result.detail is not None
        self.assertTrue(result.detail.startswith(VALIDATION_ERROR_CLAUSE))
        self.assertIn("Failed to start harness", result.detail)
        assert_every_invariant(result)

    def test_a_non_object_json_line_raises_inside_the_loop_and_is_named(self):
        """The read loop's ``except Exception`` branch: ``msg.get`` on a
        JSON array raises, the loop ends, and the error is recorded."""
        result = run_fake_harness(["[1, 2, 3]", SESSION_END_LINE])

        self.assertEqual(result.scenario, VALIDATION_ERROR_SCENARIO)
        assert result.error is not None
        assert result.detail is not None
        self.assertTrue(result.detail.startswith(VALIDATION_ERROR_CLAUSE))
        assert_every_invariant(result)


class TestThePersistedTextIsBounded(unittest.TestCase):
    """Issue #888 review F3: ``detail`` reaches a DB column and a card."""

    def test_a_long_stderr_keeps_its_tail_not_its_head(self):
        """A traceback's cause is at the BOTTOM; the bounded audit keeps it."""
        noise = "\n".join(f"  frame {index}" for index in range(4000))
        traceback = f"{noise}\nValueError: the actual cause\n"
        result = run_fake_harness([], stderr_text=traceback)

        session = result.harness_session
        assert session is not None
        assert session.stderr_tail is not None
        self.assertLessEqual(len(session.stderr_tail), _STDERR_TAIL_CHARS)
        self.assertTrue(
            session.stderr_tail.endswith("ValueError: the actual cause"))
        assert_every_invariant(result)

    def test_a_half_megabyte_single_line_stderr_does_not_reach_the_card(self):
        """Measured live shape: one 500 KB newline-free stderr line put
        500,128 chars into ``detail`` and 504,666 into the JSONB blob."""
        giant = "E" * 500_000
        result = run_fake_harness([], stderr_text=f"ValueError: {giant}\n")

        assert result.detail is not None
        self.assertLessEqual(len(result.detail), _DETAIL_MAX_CHARS)
        session = result.harness_session
        assert session is not None
        assert session.stderr_tail is not None
        self.assertLessEqual(len(session.stderr_tail), _STDERR_TAIL_CHARS)
        # The bounded hint keeps the informative FRONT of the line…
        self.assertIn("ValueError: EEE", result.detail)
        # …and the whole persisted row stays a sane size.
        self.assertLess(len(result.to_json()), 16_000)
        assert_every_invariant(result)

    def test_the_stderr_hint_is_capped_independently_of_the_detail(self):
        result = run_fake_harness([], stderr_text="X" * 5_000 + "\n")
        assert result.detail is not None
        hint = result.detail.split("harness stderr ended: ")[-1]
        self.assertLessEqual(len(hint), _STDERR_LINE_CHARS)

    def test_a_harness_spamming_distinct_message_types_is_capped(self):
        """The bound the per-line stderr cap does NOT cover: a harness that
        emits hundreds of distinct ``type`` values puts all of them in the
        messages segment. Only the whole-detail cap keeps that out of the
        column and the card — a mutant deleting it survives every
        stderr-shaped world (issue #888 review F3, verified by injection)."""
        lines = [
            f'{{"type": "future_message_variant_{index:03d}"}}'
            for index in range(300)
        ]
        result = run_fake_harness([*lines, SESSION_END_LINE])

        session = result.harness_session
        assert session is not None
        self.assertEqual(len(session.message_types), 301)
        assert result.detail is not None
        self.assertGreater(
            len("; ".join(session.message_types)), _DETAIL_MAX_CHARS,
            "fixture must exceed the cap or it proves nothing")
        self.assertLessEqual(len(result.detail), _DETAIL_MAX_CHARS)
        self.assertTrue(result.detail.startswith(NO_CHOOSE_MATCH_CLAUSE))
        assert_every_invariant(result)


class TestDecidedMatchesAreUnchanged(unittest.TestCase):
    """Must-still-work: naming the gaps must not touch the ordinary paths."""

    CASES: ClassVar = [
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
                    "a decided match must carry no unmatched-run evidence")
                assert_scenario_is_always_named(result)
                assert_the_name_matches_the_error_state(result)
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
        unmatched stamp — including the ``validation_error`` name — even
        though the error text is legitimately retained."""
        result = run_fake_harness([
            UNDECODABLE_CHOOSE_MATCH_LINE, choose_match_line(),
            SESSION_END_LINE,
        ])
        self.assertEqual(result.scenario, "strong_match")
        self.assertIsNone(result.harness_session)
        self.assertIsNotNone(result.error)


class TestNamingTheScenarioDoesNotMoveTheDownload(unittest.TestCase):
    """Invariant 5, composed: real producer → real quarantine allocator."""

    def _quarantine(self, scenario: str | None, root: str) -> str:
        album_dir = os.path.join(root, "source", "Artist - Album")
        os.makedirs(album_dir)
        track = os.path.join(album_dir, "01 - Track.mp3")
        with open(track, "w", encoding="utf-8") as handle:
            handle.write("audio")
        result = move_failed_import_curated(
            album_dir,
            allowed_audio=["01 - Track.mp3"],
            scenario=scenario,
            quarantine_root=os.path.join(root, "quarantine"),
        )
        assert result is not None
        return result.target_path

    def test_both_named_scenarios_land_where_the_unnamed_one_did(self):
        """``None`` and both new names are Wrong Matches candidates, so the
        tree the download lands in is byte-identical — the naming is
        routing-neutral by construction."""
        produced = [
            run_fake_harness([SESSION_END_LINE]).scenario,
            run_fake_harness([UNDECODABLE_CHOOSE_MATCH_LINE]).scenario,
        ]
        self.assertEqual(
            produced, [NO_CHOOSE_MATCH_SCENARIO, VALIDATION_ERROR_SCENARIO])
        with tempfile.TemporaryDirectory() as tmpdir:
            unnamed_root = os.path.join(tmpdir, "unnamed")
            unnamed = os.path.relpath(
                self._quarantine(None, unnamed_root), unnamed_root)
            for index, scenario in enumerate(produced):
                with self.subTest(scenario=scenario):
                    root = os.path.join(tmpdir, f"named{index}")
                    named = self._quarantine(scenario, root)
                    self.assertEqual(os.path.relpath(named, root), unnamed)
                    self.assertIn(
                        f"{os.sep}{WRONG_MATCH_QUARANTINE_DIR}{os.sep}", named)
                    self.assertNotIn(f"{os.sep}failed_imports{os.sep}", named)

    def test_the_row_stays_in_the_operator_worklist(self):
        for scenario in (
            None, NO_CHOOSE_MATCH_SCENARIO, VALIDATION_ERROR_SCENARIO,
        ):
            with self.subTest(scenario=scenario):
                self.assertTrue(wrong_match_row_is_visible({
                    "request_status": "wanted",
                    "validation_result": {
                        "failed_path": "/quarantine/wrong_matches/Album",
                        "scenario": scenario,
                    },
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

    def test_a_real_validation_error_run_names_the_recorded_error(self):
        """The headline says what happened and quotes the producer's own
        error — it never says beets offered nothing, which for this world
        would be false."""
        result = run_fake_harness([UNDECODABLE_CHOOSE_MATCH_LINE])
        verdict = self._classified_verdict(result)

        self.assertTrue(verdict.startswith(VALIDATION_ERROR_VERDICT_PREFIX))
        self.assertIn("album_id", verdict)
        self.assertNotIn("without offering a match", verdict)
        assert result.error is not None
        self.assertIn(result.error, verdict)

    def test_the_verdicts_never_claim_a_cause_the_row_cannot_prove(self):
        for lines in ([SESSION_END_LINE], [UNDECODABLE_CHOOSE_MATCH_LINE]):
            with self.subTest(lines=lines):
                verdict = self._classified_verdict(run_fake_harness(lines))
                # Only OUR sentence is scanned; anything after the ':' is
                # the producer's recorded error, quoted verbatim.
                ours = verdict.split(":")[0].casefold()
                for overclaim in (
                    "no audio", "corrupt", "crashed", "empty folder",
                    "no candidates", "musicbrainz", "denylist",
                ):
                    self.assertNotIn(overclaim, ours)

    def test_the_row_no_longer_reads_as_the_bare_word_rejected(self):
        """The whole defect, end to end: before #888 both scenarios were
        NULL and the classifier's fallback rendered "Rejected"."""
        for lines in ([SESSION_END_LINE], [UNDECODABLE_CHOOSE_MATCH_LINE]):
            with self.subTest(lines=lines):
                self.assertNotEqual(
                    self._classified_verdict(run_fake_harness(lines)),
                    "Rejected")
        unnamed = LogEntry(
            id=1, request_id=2, outcome="rejected", beets_scenario=None)
        self.assertEqual(classify_log_entry(unnamed).verdict, "Rejected")

    def test_a_validation_error_without_a_recorded_message_still_reads(self):
        """``error_message`` is NULL on no live row today, but the verdict
        must not render a dangling colon if one ever appears."""
        self.assertEqual(
            classify_log_entry(LogEntry(
                id=1, request_id=2, outcome="rejected",
                beets_scenario=VALIDATION_ERROR_SCENARIO)).verdict,
            VALIDATION_ERROR_VERDICT_PREFIX,
        )


class TestInvariantCheckersTripOnViolations(unittest.TestCase):
    """Known-bad self-tests: a checker that cannot fail proves nothing."""

    def test_an_unnamed_scenario_trips_the_name_checker(self) -> None:
        with self.assertRaises(AssertionError):
            assert_scenario_is_always_named(ValidationResult())

    def test_an_invented_scenario_trips_the_name_checker(self) -> None:
        with self.assertRaises(AssertionError):
            assert_scenario_is_always_named(
                ValidationResult(scenario="validation_rejected"))

    def test_merging_the_error_worlds_trips_the_error_state_checker(self):
        """The exact regression F1 convicted: an errored run wearing the
        name that asserts beets offered nothing."""
        with self.assertRaises(AssertionError):
            assert_the_name_matches_the_error_state(ValidationResult(
                scenario=NO_CHOOSE_MATCH_SCENARIO,
                error="harness schema violation: Expected `str`, got `int`"))
        # …and the mirror: a clean run wearing the error name.
        with self.assertRaises(AssertionError):
            assert_the_name_matches_the_error_state(
                ValidationResult(scenario=VALIDATION_ERROR_SCENARIO))
        # The real pairings pass, so the checker is not vacuous.
        assert_the_name_matches_the_error_state(
            ValidationResult(scenario=NO_CHOOSE_MATCH_SCENARIO))
        assert_the_name_matches_the_error_state(
            ValidationResult(scenario=VALIDATION_ERROR_SCENARIO, error="boom"))

    def test_a_valid_unmatched_run_trips_the_evidence_checker(self) -> None:
        """Issue #888 review F2: ``lib/download_validation.py`` routes on
        ``bv_result.valid`` alone, so a True here sends a run that reviewed
        no match into auto-import dispatch."""
        with self.assertRaises(AssertionError):
            assert_evidence_accompanies_the_name(ValidationResult(
                valid=True,
                scenario=NO_CHOOSE_MATCH_SCENARIO,
                detail=NO_CHOOSE_MATCH_CLAUSE,
                harness_session=HarnessSessionEvidence()))

    def test_a_named_gap_without_evidence_trips_the_evidence_checker(self):
        with self.assertRaises(AssertionError):
            assert_evidence_accompanies_the_name(ValidationResult(
                scenario=NO_CHOOSE_MATCH_SCENARIO, detail="something"))

    def test_a_decided_match_carrying_evidence_trips_the_checker(self) -> None:
        with self.assertRaises(AssertionError):
            assert_evidence_accompanies_the_name(ValidationResult(
                scenario="strong_match",
                harness_session=HarnessSessionEvidence()))

    def test_a_named_gap_without_a_detail_trips_the_checker(self) -> None:
        with self.assertRaises(AssertionError):
            assert_evidence_accompanies_the_name(ValidationResult(
                scenario=NO_CHOOSE_MATCH_SCENARIO,
                harness_session=HarnessSessionEvidence()))

    def test_an_unbounded_detail_trips_the_checker(self) -> None:
        with self.assertRaises(AssertionError):
            assert_evidence_accompanies_the_name(ValidationResult(
                scenario=NO_CHOOSE_MATCH_SCENARIO,
                detail="E" * (_DETAIL_MAX_CHARS + 1),
                harness_session=HarnessSessionEvidence()))

    def test_an_unrecorded_message_type_trips_the_checker(self) -> None:
        with self.assertRaises(AssertionError):
            assert_evidence_accompanies_the_name(ValidationResult(
                scenario=NO_CHOOSE_MATCH_SCENARIO,
                detail=NO_CHOOSE_MATCH_CLAUSE,
                harness_session=HarnessSessionEvidence(
                    message_types=["session_end"], session_end_seen=True)))

    def test_a_claimed_cause_trips_the_no_inference_checker(self) -> None:
        with self.assertRaises(AssertionError):
            assert_evidence_claims_no_cause(ValidationResult(
                scenario=NO_CHOOSE_MATCH_SCENARIO,
                detail="No importable audio in the folder",
                harness_session=HarnessSessionEvidence()))

    def test_the_harness_own_words_are_not_treated_as_our_claim(self) -> None:
        """A traceback that happens to say "corrupt" is the harness
        talking, quoted after the ``;`` — not a cause we asserted."""
        assert_evidence_claims_no_cause(ValidationResult(
            scenario=NO_CHOOSE_MATCH_SCENARIO,
            detail=(
                f"{NO_CHOOSE_MATCH_CLAUSE}; harness messages: none; "
                "harness stderr ended: sqlite3.DatabaseError: database disk "
                "image is corrupt"
            ),
            harness_session=HarnessSessionEvidence()))

    def test_swapping_the_two_clauses_trips_the_no_inference_checker(self):
        with self.assertRaises(AssertionError):
            assert_evidence_claims_no_cause(ValidationResult(
                scenario=VALIDATION_ERROR_SCENARIO,
                error="boom",
                detail=f"{NO_CHOOSE_MATCH_CLAUSE}; harness messages: none",
                harness_session=HarnessSessionEvidence()))

    def test_leaving_the_wrong_match_taxonomy_trips_the_routing_checker(self):
        with self.assertRaises(AssertionError):
            assert_stays_a_wrong_match_candidate("spectral_reject")

    def test_a_blob_that_drops_the_evidence_trips_the_round_trip_checker(self):
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
