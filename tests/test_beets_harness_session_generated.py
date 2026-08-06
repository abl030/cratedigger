"""Generated patrol for the harness-session evidence contract (issue #888).

The deterministic pins in ``tests/test_beets_harness_session.py`` name the
exact worlds the 276-row live cohort is consistent with. These properties
patrol the space around them: ANY harness transcript — chatter, malformed
JSON, non-object JSON, a non-string message type, an undecodable
``choose_match``, no output at all — driven through the REAL
``beets_validate`` against a REAL fake-harness subprocess, asserting that a
rejection can never again leave the operator with nothing, and never wear a
name its own evidence contradicts.

The seam is the process boundary and nothing else: the harness is a real
executable, the pipes are real pipes, the stderr is really read at EOF.
Mocking ``sp.Popen`` here would prove nothing about the evidence the fix
persists, which is derived entirely from that boundary
(``.claude/rules/test-fidelity.md`` Rule B).

Profiles and promotion policy: tests/_hypothesis_profiles.py and
docs/generated-testing.md.
"""

from __future__ import annotations

import tempfile
import unittest

from hypothesis import example, given, settings
from hypothesis import strategies as st

import tests._hypothesis_profiles  # noqa: F401 - loads the active profile
from lib.beets import (
    _DETAIL_MAX_CHARS,
    _STDERR_TAIL_CHARS,
    NO_CHOOSE_MATCH_SCENARIO,
    VALIDATION_ERROR_SCENARIO,
)
from lib.quality import ValidationResult
from lib.validation_envelope import derive_validation_log_columns
from tests.test_beets_harness_session import (
    DECIDED_SCENARIOS,
    NO_CHOOSE_MATCH_VERDICT,
    NON_STRING_TYPE_LINE,
    SESSION_END_LINE,
    SHOULD_RESUME_LINE,
    TARGET_MBID,
    UNDECODABLE_CHOOSE_MATCH_LINE,
    UNMATCHED_SCENARIOS,
    VALIDATION_ERROR_VERDICT_PREFIX,
    assert_evidence_accompanies_the_name,
    assert_evidence_claims_no_cause,
    assert_fake_harness_wait_is_owned,
    assert_result_round_trips,
    assert_scenario_is_always_named,
    assert_stays_a_wrong_match_candidate,
    assert_the_name_matches_the_error_state,
    choose_match_line,
    run_fake_harness,
    write_fake_harness,
)
from web.classify import LogEntry, classify_log_entry

# ---------------------------------------------------------------------------
# Transcript worlds
# ---------------------------------------------------------------------------

#: One harness stdout line each, keyed by the fact the line contributes.
#: ``valid_choose_match`` is the ONLY kind that can end in a decided match;
#: every other kind is something a harness can say on the way to saying
#: nothing useful.
LINE_KINDS: dict[str, str] = {
    "valid_choose_match": choose_match_line(),
    "undecodable_choose_match": UNDECODABLE_CHOOSE_MATCH_LINE,
    "should_resume": SHOULD_RESUME_LINE,
    "choose_item": '{"type": "choose_item", "path": "/staged"}',
    "resolve_duplicate": '{"type": "resolve_duplicate", "path": "/staged"}',
    "session_end": SESSION_END_LINE,
    "unknown_type": '{"type": "some_future_message"}',
    "typeless_object": '{"path": "/staged"}',
    # Issue #888 review F5: the ``isinstance(msg_type, str)`` guard was
    # unpatrolled — no test in the repo emitted a non-string ``type``. With
    # the guard removed this world raises out of ``beets_validate`` entirely.
    "non_string_type": NON_STRING_TYPE_LINE,
    "non_object_json": "[1, 2, 3]",
    "garbage": "beets: importing /staged/Artist - Album",
    "blank": "",
}

#: The message ``type`` each kind puts on the wire, when it puts a STRING
#: one there. ``non_string_type`` deliberately maps to ``None``: production
#: must never record it, which is exactly what the guard is for.
KIND_MESSAGE_TYPE: dict[str, str | None] = {
    "valid_choose_match": "choose_match",
    "undecodable_choose_match": "choose_match",
    "should_resume": "should_resume",
    "choose_item": "choose_item",
    "resolve_duplicate": "resolve_duplicate",
    "session_end": "session_end",
    "unknown_type": "some_future_message",
    "typeless_object": None,
    "non_string_type": None,
    "non_object_json": None,
    "garbage": None,
    "blank": None,
}

#: Kinds that can precede a ``choose_match`` without ending the read loop —
#: the worlds where the match stays reachable, used by the must-still-work
#: property. ``non_object_json`` is deliberately absent: it raises inside the
#: loop and ends it.
_BENIGN_KINDS = (
    "should_resume", "choose_item", "resolve_duplicate", "unknown_type",
    "typeless_object", "non_string_type", "garbage", "blank",
)

#: Kinds that make ``beets_validate`` record an error of its own, so the run
#: must be named ``validation_error`` rather than ``no_choose_match``.
_ERRORING_KINDS = frozenset({"undecodable_choose_match", "non_object_json"})

_STDERR_ALPHABET = st.characters(min_codepoint=32, max_codepoint=126)

TRANSCRIPTS = st.lists(st.sampled_from(sorted(LINE_KINDS)), max_size=6)
STDERR_TEXTS = st.one_of(
    st.just(""),
    st.text(alphabet=_STDERR_ALPHABET, max_size=120),
    st.lists(
        st.text(alphabet=_STDERR_ALPHABET, max_size=40), max_size=6,
    ).map(lambda lines: "\n".join(lines)),
)


def render_transcript(kinds: list[str]) -> list[str]:
    """The stdout lines a harness of these kinds writes, in order."""
    return [LINE_KINDS[kind] for kind in kinds]


def controller_consumed_prefix(kinds: list[str]) -> list[str]:
    """Kinds the controller can observe before its first terminal event."""
    consumed: list[str] = []
    for kind in kinds:
        consumed.append(kind)
        if kind in ("session_end", "non_object_json"):
            break
    return consumed


def emitted_message_types(kinds: list[str]) -> set[str]:
    """Every ``type`` this transcript could possibly put on the wire."""
    return {
        message_type
        for kind in kinds
        if (message_type := KIND_MESSAGE_TYPE[kind]) is not None
    }


# ---------------------------------------------------------------------------
# Checkers owned by this module
# ---------------------------------------------------------------------------

def assert_no_match_named_exactly_when_none_was_offered(
    result: ValidationResult,
    kinds: list[str],
) -> None:
    """Independent oracle for WHICH runs get an unmatched name.

    One direction is checkable without modelling production's loop at all:
    a transcript that never contains a decodable ``choose_match`` cannot
    possibly produce a decided match, so the run must be named
    ``no_choose_match`` or ``validation_error``. The contrapositive — a
    decided scenario implies the transcript held one — is the same statement
    and is checked with it.

    The converse (a decodable ``choose_match`` ALWAYS decides) is
    deliberately NOT asserted here: an earlier erroring line can end the
    read loop first. That direction is pinned by
    ``TestADecodableMatchStillDecides``, over transcripts constructed so the
    match is reachable.
    """
    offered = "valid_choose_match" in kinds
    if not offered:
        assert result.scenario in UNMATCHED_SCENARIOS, (
            f"no decodable choose_match was on the wire, yet the run was "
            f"named {result.scenario!r}")
    if result.scenario in DECIDED_SCENARIOS:
        assert offered, (
            f"decided {result.scenario!r} without a decodable choose_match")


def assert_an_erroring_transcript_is_never_called_matchless(
    result: ValidationResult,
    kinds: list[str],
) -> None:
    """The F1 invariant, oracled from the TRANSCRIPT rather than the result.

    ``assert_the_name_matches_the_error_state`` reads ``result.error``,
    which production also writes — so on its own it could be satisfied by a
    producer that stopped recording errors. This checker is keyed on the
    world instead: a transcript containing a line that production must fail
    on can never be reported as "beets offered nothing".
    """
    if not (_ERRORING_KINDS & set(kinds)):
        return
    if result.scenario not in UNMATCHED_SCENARIOS:
        return
    assert result.error is not None, (
        f"transcript {kinds} contains a line beets_validate must fail on, "
        "yet no error was recorded")
    assert result.scenario == VALIDATION_ERROR_SCENARIO, (
        f"transcript {kinds} errored, yet the run was named "
        f"{result.scenario!r} — a headline asserting beets offered nothing")


def assert_recorded_types_came_from_the_wire(
    result: ValidationResult,
    kinds: list[str],
) -> None:
    """The audit never invents a message the harness did not send.

    Also the F5 guard's oracle: ``non_string_type`` contributes no entry
    here, so a production change that recorded it would be caught as an
    unexplained type — if the ``", ".join`` did not raise first.
    """
    session = result.harness_session
    if session is None:
        return
    emitted = emitted_message_types(kinds)
    unexplained = [
        message_type for message_type in session.message_types
        if message_type not in emitted
    ]
    assert not unexplained, (
        f"harness_session records types nothing emitted: {unexplained}")
    if session.session_end_seen:
        assert "session_end" in emitted, (
            "session_end_seen without a session_end on the wire")


def assert_stderr_tail_is_a_real_tail(
    result: ValidationResult,
    stderr_text: str,
) -> None:
    """The persisted copy is a suffix of what the harness really wrote."""
    session = result.harness_session
    if session is None or session.stderr_tail is None:
        return
    assert stderr_text.strip().endswith(session.stderr_tail), (
        "stderr_tail is not a suffix of the harness's stderr")


def assert_column_projection_names_the_scenario(
    result: ValidationResult,
) -> None:
    """The denormalized ``download_log.beets_scenario`` is no longer NULL.

    The whole cohort was found by ``beets_scenario IS NULL``; the fix is
    only real if the projection the writer applies to the blob carries the
    name into that column.
    """
    _distance, scenario = derive_validation_log_columns(result.to_json())
    assert scenario == result.scenario, (
        f"column projection produced {scenario!r} for a "
        f"{result.scenario!r} result")


def assert_the_row_explains_itself(
    result: ValidationResult,
    *,
    expected_no_match_verdict: str = NO_CHOOSE_MATCH_VERDICT,
    expected_error_prefix: str = VALIDATION_ERROR_VERDICT_PREFIX,
) -> str:
    """Recents renders a sentence, never the bare word the defect showed.

    The expectations are kwarg-injected so the known-bad self-tests can hand
    them sentences production never produces, rather than patching this
    module's own constants.
    """
    verdict = classify_log_entry(LogEntry(
        id=1,
        request_id=2,
        outcome="rejected",
        beets_scenario=result.scenario,
        beets_detail=result.detail,
        error_message=result.error,
        validation_result=result.to_json(),
    )).verdict
    assert verdict != "Rejected", (
        "the rejection still reads as the bare word 'Rejected'")
    if result.scenario == NO_CHOOSE_MATCH_SCENARIO:
        assert verdict == expected_no_match_verdict, (
            f"a no-match run rendered {verdict!r}")
    if result.scenario == VALIDATION_ERROR_SCENARIO:
        assert verdict.startswith(expected_error_prefix), (
            f"a validation-error run rendered {verdict!r}")
        assert "without offering a match" not in verdict, (
            "an errored run's headline claims beets offered nothing")
    return verdict


# ---------------------------------------------------------------------------
# Properties
# ---------------------------------------------------------------------------

class TestFakeHarnessProcessLifecycleGenerated(unittest.TestCase):
    @settings(max_examples=25)
    @given(
        lines=st.lists(st.text(max_size=40), max_size=4),
        stderr_text=st.text(max_size=80),
    )
    def test_every_fake_harness_owns_its_terminal_wait(
        self,
        lines: list[str],
        stderr_text: str,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            harness_path = write_fake_harness(
                tmpdir,
                stdout_lines=lines,
                stderr_text=stderr_text,
            )
            with open(harness_path, encoding="utf-8") as handle:
                assert_fake_harness_wait_is_owned(handle.read())


class TestNoHarnessTranscriptEverRejectsSilently(unittest.TestCase):
    """The invariant the 276 live rows violated, over the world space."""

    @example(kinds=[], stderr_text="")
    @example(kinds=["session_end"], stderr_text="")
    @example(kinds=[], stderr_text="sqlite3.OperationalError: database is locked")
    @example(
        kinds=["undecodable_choose_match", "session_end"], stderr_text="")
    @example(kinds=["non_object_json", "valid_choose_match"], stderr_text="")
    @example(kinds=["session_end", "non_object_json"], stderr_text="")
    @example(
        kinds=["session_end", "undecodable_choose_match"], stderr_text="")
    @example(kinds=["non_string_type", "session_end"], stderr_text="")
    @example(kinds=["garbage", "blank", "typeless_object"], stderr_text="")
    @given(kinds=TRANSCRIPTS, stderr_text=STDERR_TEXTS)
    def test_every_transcript_produces_a_named_explained_result(
        self,
        kinds: list[str],
        stderr_text: str,
    ) -> None:
        result = run_fake_harness(
            render_transcript(kinds), stderr_text=stderr_text)
        consumed = controller_consumed_prefix(kinds)

        assert_scenario_is_always_named(result)
        assert_the_name_matches_the_error_state(result)
        assert_an_erroring_transcript_is_never_called_matchless(
            result, consumed)
        assert_evidence_accompanies_the_name(result)
        assert_evidence_claims_no_cause(result)
        assert_no_match_named_exactly_when_none_was_offered(result, consumed)
        assert_recorded_types_came_from_the_wire(result, consumed)
        assert_stderr_tail_is_a_real_tail(result, stderr_text)
        assert_stays_a_wrong_match_candidate(result.scenario)
        assert_result_round_trips(result)
        assert_column_projection_names_the_scenario(result)
        assert_the_row_explains_itself(result)


class TestThePersistedTextIsAlwaysBounded(unittest.TestCase):
    """Issue #888 review F3: ``detail`` reaches a DB column and a card.

    The world space that matters here is NOT the transcript vocabulary —
    it is volume, from either of the two unbounded sources: a harness that
    emits many distinct ``type`` values, and a single enormous stderr line.
    A mutant deleting the whole-detail cap survives every stderr-shaped
    world, because the per-line cap already covers those; only the message
    flood reaches it.
    """

    @example(type_count=0, stderr_line_length=0)
    @example(type_count=300, stderr_line_length=0)
    @example(type_count=0, stderr_line_length=500_000)
    @example(type_count=300, stderr_line_length=20_000)
    @given(
        type_count=st.integers(min_value=0, max_value=400),
        stderr_line_length=st.integers(min_value=0, max_value=20_000),
    )
    def test_no_harness_volume_can_grow_the_persisted_detail(
        self,
        type_count: int,
        stderr_line_length: int,
    ) -> None:
        lines = [
            f'{{"type": "future_message_variant_{index:04d}"}}'
            for index in range(type_count)
        ]
        result = run_fake_harness(
            [*lines, SESSION_END_LINE],
            stderr_text=("Z" * stderr_line_length),
        )

        assert result.detail is not None
        self.assertLessEqual(len(result.detail), _DETAIL_MAX_CHARS)
        session = result.harness_session
        assert session is not None
        self.assertEqual(len(session.message_types), type_count + 1)
        if session.stderr_tail is not None:
            self.assertLessEqual(len(session.stderr_tail), _STDERR_TAIL_CHARS)
        assert_evidence_accompanies_the_name(result)
        assert_evidence_claims_no_cause(result)


class TestADecodableMatchStillDecides(unittest.TestCase):
    """Must-still-work: the ordinary paths are untouched by the fix."""

    @example(
        before=[], after=[], album_id=TARGET_MBID, distance=0.05,
        extra_tracks=0,
    )
    @example(
        before=["should_resume"], after=["session_end"],
        album_id=TARGET_MBID, distance=0.9, extra_tracks=0,
    )
    @example(
        before=["undecodable_choose_match"], after=[],
        album_id="other-pressing", distance=0.05, extra_tracks=0,
    )
    @example(
        before=["non_string_type"], after=[], album_id=TARGET_MBID,
        distance=0.05, extra_tracks=0,
    )
    @given(
        before=st.lists(st.sampled_from(_BENIGN_KINDS), max_size=3),
        after=st.lists(
            st.sampled_from((*_BENIGN_KINDS, "session_end")), max_size=3),
        album_id=st.sampled_from((TARGET_MBID, "other-pressing")),
        distance=st.floats(min_value=0.0, max_value=1.0),
        extra_tracks=st.integers(min_value=0, max_value=3),
    )
    def test_a_reachable_choose_match_is_always_decided(
        self,
        before: list[str],
        after: list[str],
        album_id: str,
        distance: float,
        extra_tracks: int,
    ) -> None:
        """Every world here keeps the match reachable — nothing before it
        can end the read loop — so production must decide, and must not
        stamp the unmatched evidence over a real verdict."""
        lines = [
            *render_transcript(before),
            choose_match_line(
                album_id=album_id,
                distance=distance,
                extra_tracks=extra_tracks,
            ),
            *render_transcript(after),
        ]
        result = run_fake_harness(lines)

        self.assertIn(result.scenario, DECIDED_SCENARIOS)
        self.assertIsNone(
            result.harness_session,
            "a decided match must not carry unmatched-run evidence")
        assert_scenario_is_always_named(result)
        assert_the_name_matches_the_error_state(result)
        assert_evidence_accompanies_the_name(result)
        assert_column_projection_names_the_scenario(result)
        self.assertEqual(
            result.valid,
            album_id == TARGET_MBID and extra_tracks == 0 and distance <= 0.15,
        )

    @example(before=["undecodable_choose_match"])
    @given(before=st.lists(st.sampled_from(_BENIGN_KINDS), max_size=3))
    def test_the_unmatched_stamp_never_overwrites_a_decided_detail(
        self,
        before: list[str],
    ) -> None:
        lines = [
            *render_transcript(before),
            LINE_KINDS["undecodable_choose_match"],
            choose_match_line(distance=0.05),
            SESSION_END_LINE,
        ]
        result = run_fake_harness(lines)
        self.assertEqual(result.scenario, "strong_match")
        assert result.detail is not None
        self.assertIn("distance=", result.detail)
        self.assertNotIn("without offering a match", result.detail)
        self.assertNotIn("did not complete", result.detail)


class TestCheckersTripOnViolations(unittest.TestCase):
    """Known-bad self-tests for the checkers this module owns."""

    def test_unowned_terminal_wait_is_rejected(self) -> None:
        with self.assertRaisesRegex(AssertionError, "terminate.*owns"):
            assert_fake_harness_wait_is_owned("#!/bin/sh\nsleep 20\n")

    def test_consumed_prefix_stops_only_at_controller_terminal_kinds(
        self,
    ) -> None:
        cases = (
            (
                ["session_end", "non_object_json"],
                ["session_end"],
            ),
            (
                ["non_object_json", "session_end"],
                ["non_object_json"],
            ),
            (
                ["undecodable_choose_match", "session_end", "garbage"],
                ["undecodable_choose_match", "session_end"],
            ),
        )
        for kinds, expected in cases:
            with self.subTest(kinds=kinds):
                self.assertEqual(controller_consumed_prefix(kinds), expected)

    def test_the_oracle_rejects_a_silent_run(self) -> None:
        with self.assertRaises(AssertionError):
            assert_no_match_named_exactly_when_none_was_offered(
                ValidationResult(scenario=None), ["session_end"])

    def test_the_oracle_rejects_a_decision_without_a_match_on_the_wire(self):
        with self.assertRaises(AssertionError):
            assert_no_match_named_exactly_when_none_was_offered(
                ValidationResult(scenario="strong_match"), ["session_end"])

    def test_the_oracle_accepts_the_real_pairings(self) -> None:
        assert_no_match_named_exactly_when_none_was_offered(
            ValidationResult(scenario=NO_CHOOSE_MATCH_SCENARIO), ["blank"])
        assert_no_match_named_exactly_when_none_was_offered(
            ValidationResult(scenario=VALIDATION_ERROR_SCENARIO),
            ["non_object_json"])
        assert_no_match_named_exactly_when_none_was_offered(
            ValidationResult(scenario="strong_match"), ["valid_choose_match"])

    def test_the_transcript_oracle_rejects_the_merged_taxonomy(self) -> None:
        """Issue #888 review F1, oracled from the world: a transcript that
        production must fail on can never be named ``no_choose_match``."""
        with self.assertRaises(AssertionError):
            assert_an_erroring_transcript_is_never_called_matchless(
                ValidationResult(
                    scenario=NO_CHOOSE_MATCH_SCENARIO,
                    error="harness schema violation: Expected `str`, got `int`"),
                ["undecodable_choose_match"])
        # …and a producer that quietly stopped recording the error at all.
        with self.assertRaises(AssertionError):
            assert_an_erroring_transcript_is_never_called_matchless(
                ValidationResult(scenario=NO_CHOOSE_MATCH_SCENARIO),
                ["non_object_json"])
        # The real pairing passes.
        assert_an_erroring_transcript_is_never_called_matchless(
            ValidationResult(scenario=VALIDATION_ERROR_SCENARIO, error="boom"),
            ["undecodable_choose_match"])
        # A clean transcript is out of scope for this checker.
        assert_an_erroring_transcript_is_never_called_matchless(
            ValidationResult(scenario=NO_CHOOSE_MATCH_SCENARIO), ["session_end"])

    def test_an_invented_message_type_trips_the_wire_checker(self) -> None:
        from lib.quality import HarnessSessionEvidence

        with self.assertRaises(AssertionError):
            assert_recorded_types_came_from_the_wire(
                ValidationResult(
                    scenario=NO_CHOOSE_MATCH_SCENARIO,
                    harness_session=HarnessSessionEvidence(
                        message_types=["choose_match"])),
                ["blank"])

    def test_recording_a_non_string_type_trips_the_wire_checker(self) -> None:
        """F5's oracle: ``non_string_type`` contributes no wire type, so a
        production change that recorded one is unexplained."""
        from lib.quality import HarnessSessionEvidence

        with self.assertRaises(AssertionError):
            assert_recorded_types_came_from_the_wire(
                ValidationResult(
                    scenario=NO_CHOOSE_MATCH_SCENARIO,
                    harness_session=HarnessSessionEvidence(
                        message_types=["5"])),
                ["non_string_type"])

    def test_an_invented_session_end_trips_the_wire_checker(self) -> None:
        from lib.quality import HarnessSessionEvidence

        with self.assertRaises(AssertionError):
            assert_recorded_types_came_from_the_wire(
                ValidationResult(
                    scenario=NO_CHOOSE_MATCH_SCENARIO,
                    harness_session=HarnessSessionEvidence(
                        session_end_seen=True)),
                ["blank"])

    def test_a_fabricated_stderr_tail_trips_the_tail_checker(self) -> None:
        from lib.quality import HarnessSessionEvidence

        with self.assertRaises(AssertionError):
            assert_stderr_tail_is_a_real_tail(
                ValidationResult(
                    scenario=NO_CHOOSE_MATCH_SCENARIO,
                    harness_session=HarnessSessionEvidence(
                        stderr_tail="something the harness never said")),
                "a real traceback")

    def test_a_blob_the_projection_cannot_read_trips_the_column_checker(self):
        class _Unnamed(ValidationResult):
            def to_json(self) -> str:
                return '{"scenario": null, "distance": null}'

        with self.assertRaises(AssertionError):
            assert_column_projection_names_the_scenario(
                _Unnamed(scenario=NO_CHOOSE_MATCH_SCENARIO))

    def test_a_bare_rejected_verdict_trips_the_copy_checker(self) -> None:
        with self.assertRaises(AssertionError):
            assert_the_row_explains_itself(ValidationResult(scenario=None))

    def test_a_wrong_sentence_trips_the_copy_checker(self) -> None:
        with self.assertRaises(AssertionError):
            assert_the_row_explains_itself(
                ValidationResult(scenario=NO_CHOOSE_MATCH_SCENARIO),
                expected_no_match_verdict="A sentence nothing produces",
            )
        with self.assertRaises(AssertionError):
            assert_the_row_explains_itself(
                ValidationResult(scenario=VALIDATION_ERROR_SCENARIO,
                                 error="boom"),
                expected_error_prefix="A prefix nothing produces",
            )
        # …and the real sentences pass, so the checker is not vacuous.
        self.assertEqual(
            assert_the_row_explains_itself(
                ValidationResult(scenario=NO_CHOOSE_MATCH_SCENARIO)),
            NO_CHOOSE_MATCH_VERDICT,
        )
        self.assertTrue(
            assert_the_row_explains_itself(
                ValidationResult(scenario=VALIDATION_ERROR_SCENARIO,
                                 error="boom"),
            ).startswith(VALIDATION_ERROR_VERDICT_PREFIX),
        )


if __name__ == "__main__":
    unittest.main()
