#!/usr/bin/env python3
"""Measurement-conversion observability — generated-first bug hunt.

THE BUG (diagnosed at the code level): a preview measurement's
``conversion_failed`` (and sibling ``no_json_result`` /
``missing_new_measurement``) failure only ever persisted — and logged — the
AGGREGATE reason (e.g. "11 FLAC files failed to convert"). The per-file
ffmpeg diagnostic that ``convert_lossless`` (``harness/import_one.py``)
prints to stderr (``[FAIL] <file>: <ffmpeg tail>``) IS captured into
``ImportOneRun.stderr`` by
``lib/dispatch/subprocess_runner.py::run_import_one`` — but
``lib/import_preview.py::_measurement_failed_result`` never read it, so it
reached neither the journal (unlike the importer's ``dispatch_import_core``,
which streams ``run.stderr`` line-by-line) nor the DB
(``import_jobs.preview_result`` / ``download_log.validation_result``).

INVARIANTS:

1. **Observability.** A ``measurement_failed`` preview result built from an
   ``ImportOneRun`` whose stderr carries per-file ffmpeg ``[FAIL]`` lines
   must persist THAT diagnostic — not merely the aggregate count — in both
   ``MeasurementFailure.detail`` (the JSONB-persisted payload) and a
   WARNING-level journal log line.
2. **Robustness.** ``convert_lossless`` over readable lossless sources with
   a writable output dir always succeeds (``failed == 0``); any per-file
   failure records a non-empty, real diagnostic (never a silent tally).

Real ffmpeg is used throughout (the dev shell provides it) — no mocks of
our own logic, only real subprocess calls and real temp files, per
"MOCKS: LEAF-SEAM ONLY".
"""

import contextlib
import io
import os
import shutil
import subprocess
import sys
import tempfile
import unittest
from dataclasses import dataclass

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import tests._hypothesis_profiles  # noqa: F401  (loads the active profile)

from hypothesis import given
from hypothesis import strategies as st

from harness.import_one import V0_SPEC, convert_lossless
from lib.dispatch.types import ImportOneRun
from lib.import_preview import (
    PREVIEW_VERDICT_MEASUREMENT_FAILED,
    _STDERR_DIAGNOSTIC_MAX_CHARS,
    _diagnostic_from_stderr,
    _measurement_failed_result,
)
from tests.helpers import make_import_result


def _write_sine_flac(path: str, duration: float = 0.3) -> None:
    """Real ffmpeg-generated tiny FLAC fixture — no synthetic bytes."""
    subprocess.run(
        ["ffmpeg", "-y", "-f", "lavfi",
         "-i", f"sine=frequency=440:duration={duration}",
         "-c:a", "flac", path],
        capture_output=True, check=True,
    )


def _write_corrupt_flac(path: str) -> None:
    """A file real ffmpeg cannot decode, but which passes the
    extension-based ``_is_lossless_file`` check in harness/import_one.py —
    the realistic failure mode for a truncated/corrupt Soulseek grab."""
    with open(path, "wb") as f:
        f.write(b"not actually flac audio data" * 4)


# ===========================================================================
# Invariant checker — module-level function so the known-bad self-test can
# call it directly and prove it trips (RED/GREEN guarantee).
# ===========================================================================

def assert_measurement_failed_carries_tool_diagnostic(
    result, *, stderr_had_fail_marker: bool,
) -> None:
    """Observability invariant: when the triggering subprocess stderr had a
    per-file ``[FAIL]`` line, the persisted failure detail must carry it —
    not merely the aggregate decision/count.
    """
    assert result.verdict == PREVIEW_VERDICT_MEASUREMENT_FAILED
    assert result.failure is not None
    detail = result.failure.detail
    if stderr_had_fail_marker and "[FAIL]" not in detail:
        raise AssertionError(
            "measurement_failed detail dropped the per-file tool "
            f"diagnostic: {detail!r}"
        )


# ===========================================================================
# Invariant 2 — convert_lossless robustness (real ffmpeg, real files)
# ===========================================================================

class TestConvertLosslessRobustness(unittest.TestCase):

    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="cratedigger-test-convert-")
        self.addCleanup(shutil.rmtree, self.tmp, ignore_errors=True)

    def test_all_good_files_convert_clean(self):
        for i in range(3):
            _write_sine_flac(os.path.join(self.tmp, f"{i:02d}.flac"))
        converted, failed, orig_ext, _channels = convert_lossless(self.tmp, V0_SPEC)
        self.assertEqual(converted, 3)
        self.assertEqual(failed, 0)
        self.assertEqual(orig_ext, "flac")

    def test_pin_eleven_corrupt_files_all_fail_with_real_ffmpeg_reason(self):
        """Deterministic pin: the exact diagnosed shape — ~11 files, all
        conversion_failed, and the real per-file ffmpeg reason is
        recoverable from convert_lossless's own stderr output."""
        for i in range(11):
            _write_corrupt_flac(os.path.join(self.tmp, f"{i:02d}.flac"))
        buf = io.StringIO()
        with contextlib.redirect_stderr(buf):
            converted, failed, _orig_ext, _channels = convert_lossless(self.tmp, V0_SPEC)
        self.assertEqual(converted, 0)
        self.assertEqual(failed, 11)
        stderr_text = buf.getvalue()
        fail_lines = [ln for ln in stderr_text.splitlines() if "[FAIL]" in ln]
        self.assertEqual(len(fail_lines), 11, stderr_text)
        # Every line names its own file and carries non-trivial ffmpeg
        # output — this is the real per-file reason the bug discarded.
        for ln in fail_lines:
            self.assertRegex(ln, r"\[FAIL\] \d\d\.flac: .+")

    def test_readonly_output_dir_fails_with_permission_reason(self):
        """Fault-injection sweep item: a read-only album directory is a
        second real, deterministic failure mode (distinct from a corrupt
        source file) — surfaced while probing convert_lossless directly,
        reported per the debugging brief even though it's not the fix."""
        _write_sine_flac(os.path.join(self.tmp, "ok.flac"))
        os.chmod(self.tmp, 0o555)
        try:
            buf = io.StringIO()
            with contextlib.redirect_stderr(buf):
                converted, failed, _orig_ext, _channels = convert_lossless(self.tmp, V0_SPEC)
        finally:
            os.chmod(self.tmp, 0o755)
        self.assertEqual(converted, 0)
        self.assertEqual(failed, 1)
        self.assertIn("Permission denied", buf.getvalue())


# ===========================================================================
# Invariant 1 — the fix: _measurement_failed_result threads real stderr
# through to both the persisted detail and the journal.
# ===========================================================================

class TestMeasurementFailedObservability(unittest.TestCase):
    """Drives ``_measurement_failed_result`` with a faithfully-constructed
    ``ImportOneRun`` whose ``.stderr`` is REAL ffmpeg output captured from
    an actual ``convert_lossless`` failure (not a fabricated string) —
    mirroring exactly what ``run_import_one`` captures in production and
    exactly how ``measure_and_persist_candidate_evidence``'s
    ``conversion_failed`` branch calls the function under test.
    """

    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="cratedigger-test-observability-")
        self.addCleanup(shutil.rmtree, self.tmp, ignore_errors=True)
        for i in range(3):
            _write_corrupt_flac(os.path.join(self.tmp, f"{i:02d}.flac"))
        buf = io.StringIO()
        with contextlib.redirect_stderr(buf):
            converted, failed, _orig_ext, _channels = convert_lossless(self.tmp, V0_SPEC)
        assert converted == 0 and failed == 3, (converted, failed)
        self.real_stderr = buf.getvalue()
        assert "[FAIL]" in self.real_stderr
        self.aggregate_error = "3 FLAC files failed to convert"
        self.import_result = make_import_result(
            decision="conversion_failed",
            error=self.aggregate_error,
        )

    def _build_run(self) -> ImportOneRun:
        return ImportOneRun(
            command=("import_one.py", self.tmp, "mb-release-id"),
            returncode=1,
            stdout=self.import_result.to_sentinel_line(),
            stderr=self.real_stderr,
            import_result=self.import_result,
        )

    def _call_conversion_failed_site(self, run: ImportOneRun, *, thread_stderr: bool):
        """Mirrors the exact call shape of the conversion_failed branch in
        measure_and_persist_candidate_evidence (lib/import_preview.py)."""
        return _measurement_failed_result(
            mode="path",
            reason="measurement_crashed",
            decision="conversion_failed",
            detail=self.aggregate_error,
            request_id=101,
            download_log_id=None,
            source_path=self.tmp,
            import_result=run.import_result,
            subprocess_stderr=run.stderr if thread_stderr else None,
        )

    def test_green_conversion_failed_result_carries_real_ffmpeg_reason(self):
        run = self._build_run()
        result = self._call_conversion_failed_site(run, thread_stderr=True)
        assert_measurement_failed_carries_tool_diagnostic(
            result, stderr_had_fail_marker=True)
        assert result.failure is not None
        # The aggregate reason survives too — this augments, not replaces.
        self.assertIn(self.aggregate_error, result.failure.detail)

    def test_red_known_bad_omitting_stderr_loses_the_diagnostic(self):
        """Known-bad self-test: the PRE-FIX call shape (no
        ``subprocess_stderr``) must trip the checker — proving the checker
        is load-bearing, not a tautology that always passes."""
        run = self._build_run()
        result = self._call_conversion_failed_site(run, thread_stderr=False)
        with self.assertRaises(AssertionError):
            assert_measurement_failed_carries_tool_diagnostic(
                result, stderr_had_fail_marker=True)
        assert result.failure is not None
        # Sanity: the old behavior still carries the aggregate reason —
        # only the per-file diagnostic is missing.
        self.assertEqual(result.failure.detail, self.aggregate_error)

    def test_journal_receives_warning_with_diagnostic(self):
        run = self._build_run()
        with self.assertLogs("cratedigger", level="WARNING") as cm:
            self._call_conversion_failed_site(run, thread_stderr=True)
        joined = " ".join(cm.output)
        self.assertIn("[FAIL]", joined)

    def test_no_stderr_available_falls_back_to_aggregate_only(self):
        """When there truly is no stderr (e.g. subprocess produced none),
        detail is just the aggregate — no crash, no phantom diagnostic."""
        result = _measurement_failed_result(
            mode="path",
            reason="measurement_crashed",
            decision="conversion_failed",
            detail="3 FLAC files failed to convert",
            request_id=101,
            source_path=self.tmp,
            subprocess_stderr="",
        )
        assert result.failure is not None
        self.assertEqual(
            result.failure.detail, "3 FLAC files failed to convert")


# ===========================================================================
# Generated property — _diagnostic_from_stderr over generated stderr worlds
# ===========================================================================

@dataclass(frozen=True)
class StderrWorld:
    blob: str
    expect_nonempty: bool
    expect_fail_marker: bool


# Bounded, realistic line content — real import_one.py [FAIL] lines are a
# filename plus a ~200-char ffmpeg stderr tail, so these caps comfortably
# cover production shape while staying well under
# _STDERR_DIAGNOSTIC_MAX_CHARS per line (the whole-line-preservation
# guarantee needs no single generated line to approach the char budget).
_LINE_BODY = st.text(
    alphabet=st.characters(blacklist_categories=("Cs",), blacklist_characters="\n\r"),
    min_size=0, max_size=150,
)
_FILENAME_TEXT = st.text(
    alphabet=st.characters(blacklist_categories=("Cs", "Cc"), blacklist_characters="\n\r:"),
    min_size=0, max_size=40,
)


@st.composite
def stderr_worlds(draw) -> StderrWorld:
    kind = draw(st.sampled_from(
        ("empty", "whitespace", "no_fail", "with_fail",
         "huge_no_fail", "huge_with_fail")
    ))
    if kind == "empty":
        return StderrWorld("", False, False)
    if kind == "whitespace":
        blob = draw(st.text(alphabet=" \t\n", min_size=1, max_size=20))
        return StderrWorld(blob, False, False)

    has_fail = kind in ("with_fail", "huge_with_fail")
    is_huge = kind in ("huge_no_fail", "huge_with_fail")
    n_lines = draw(st.integers(min_value=1, max_value=6))
    reps = draw(st.integers(min_value=8, max_value=60)) if is_huge else 1

    lines: list[str] = []
    for _ in range(n_lines):
        body = draw(_LINE_BODY)
        if has_fail:
            fname = draw(_FILENAME_TEXT)
            lines.append(f"  [FAIL] {fname}: {body}")
        else:
            lines.append(f"  some ffmpeg trace line: {body}")
    blob = "\n".join(lines * reps)
    return StderrWorld(blob, True, has_fail)


def assert_diagnostic_extraction_invariants(world: StderrWorld, diag: str) -> None:
    """Invariant checker for ``_diagnostic_from_stderr``'s contract — a
    module-level function (not inlined in the property test) so the
    known-bad self-test below can call it directly with a planted
    violation and prove it actually trips.

    * bounded — never longer than the persisted/logged char budget
    * non-empty iff the source stderr had real (non-whitespace) content
    * preserves the ``[FAIL]`` signal whenever the source carried one
    """
    if len(diag) > _STDERR_DIAGNOSTIC_MAX_CHARS:
        raise AssertionError(
            f"diagnostic exceeds the {_STDERR_DIAGNOSTIC_MAX_CHARS}-char "
            f"bound: {len(diag)} chars")
    if bool(diag) != world.expect_nonempty:
        raise AssertionError(
            f"non-emptiness mismatch: diag={diag!r} "
            f"expect_nonempty={world.expect_nonempty}")
    if world.expect_fail_marker and "[FAIL]" not in diag:
        raise AssertionError(
            f"[FAIL] marker present in source but missing from "
            f"diagnostic: {diag!r}")


class TestDiagnosticFromStderrGenerated(unittest.TestCase):
    """Property: over arbitrary stderr blobs (empty, whitespace-only, wild
    unicode, huge repeated content, with/without [FAIL] markers),
    ``_diagnostic_from_stderr`` never throws, is bounded in size, is
    non-empty iff the input had real content, and preserves the [FAIL]
    signal whenever it was present in the source.
    """

    @given(world=stderr_worlds())
    def test_diagnostic_extraction_invariants(self, world: StderrWorld):
        diag = _diagnostic_from_stderr(world.blob)  # must never throw
        assert_diagnostic_extraction_invariants(world, diag)

    def test_known_bad_checker_trips_on_planted_violations(self):
        """Known-bad self-test: feed the checker planted-bad diagnostics
        directly (bypassing ``_diagnostic_from_stderr`` entirely) and prove
        each invariant actually trips rather than passing vacuously."""
        fail_world = StderrWorld(
            blob="[FAIL] x.flac: boom", expect_nonempty=True,
            expect_fail_marker=True)
        with self.assertRaises(AssertionError):
            # [FAIL] marker was expected but the planted diagnostic lacks it.
            assert_diagnostic_extraction_invariants(fail_world, "some other text")
        with self.assertRaises(AssertionError):
            # Non-emptiness mismatch: expected empty, planted non-empty.
            assert_diagnostic_extraction_invariants(
                StderrWorld(blob="", expect_nonempty=False, expect_fail_marker=False),
                "unexpected content",
            )
        with self.assertRaises(AssertionError):
            # Bound violation: planted diagnostic exceeds the char budget.
            assert_diagnostic_extraction_invariants(
                StderrWorld(blob="x", expect_nonempty=True, expect_fail_marker=False),
                "x" * (_STDERR_DIAGNOSTIC_MAX_CHARS + 1),
            )
        # Sanity: a genuinely correct diagnostic does NOT trip the checker.
        assert_diagnostic_extraction_invariants(fail_world, "[FAIL] x.flac: boom")


if __name__ == "__main__":
    unittest.main()
