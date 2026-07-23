#!/usr/bin/env python3
"""Typed and bounded observability for validation and album conversion.

Real FFmpeg fixtures prove that a failed conversion is transactional, retains
every source, emits one concise summary, and crosses JSON with bounded typed
diagnostics. A separate generated property covers the legacy bounded-stderr
fallback used only when the harness cannot return a typed result.
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

import msgspec

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import tests._hypothesis_profiles  # noqa: F401  (loads the active profile)

from hypothesis import given, settings
from hypothesis import strategies as st

from harness.import_one import V0_SPEC, convert_lossless
from lib.import_preview import (
    _STDERR_DIAGNOSTIC_MAX_CHARS,
    _diagnostic_from_stderr,
    _measurement_failed_result,
)
from lib.quality import (
    AudioToolDiagnostic,
    AudioValidationReport,
    ConversionInfo,
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

def assert_conversion_failure_carries_typed_diagnostic(
    conversion: ConversionInfo,
) -> None:
    """Every failed conversion has a bounded machine-readable explanation."""
    if conversion.failed and not conversion.diagnostics:
        raise AssertionError("failed conversion has no typed diagnostic")
    for diagnostic in conversion.diagnostics:
        if not diagnostic.category:
            raise AssertionError("conversion diagnostic has no category")
        if len(diagnostic.stderr_excerpt.encode("utf-8")) > 2048:
            raise AssertionError("conversion diagnostic excerpt is unbounded")


def assert_failed_batch_retains_sources(
    folder: str,
    source_names: list[str],
) -> None:
    """A failed album transaction retains every source and no derivative."""
    missing = [
        name for name in source_names
        if not os.path.exists(os.path.join(folder, name))
    ]
    if missing:
        raise AssertionError(f"failed conversion removed sources: {missing}")
    if any(name.endswith(".mp3") for name in os.listdir(folder)):
        raise AssertionError("failed conversion installed a partial derivative")


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
        """Eleven failures retain typed reasons without raw stderr logging."""
        for i in range(11):
            _write_corrupt_flac(os.path.join(self.tmp, f"{i:02d}.flac"))
        buf = io.StringIO()
        audit = ConversionInfo()
        with contextlib.redirect_stderr(buf):
            converted, failed, _orig_ext, _channels = convert_lossless(
                self.tmp,
                V0_SPEC,
                audit=audit,
            )
        self.assertEqual(converted, 0)
        self.assertEqual(failed, 11)
        audit.failed = failed
        assert_conversion_failure_carries_typed_diagnostic(audit)
        self.assertEqual(len(audit.diagnostics), 11)
        self.assertTrue(all(
            diagnostic.stderr_sha256 for diagnostic in audit.diagnostics
        ))
        self.assertEqual(
            audit.source_validation.outcome
            if audit.source_validation is not None else None,
            "audio_corrupt",
        )
        self.assertTrue(all(
            os.path.exists(os.path.join(self.tmp, f"{i:02d}.flac"))
            for i in range(11)
        ))
        self.assertFalse(any(name.endswith(".mp3") for name in os.listdir(self.tmp)))
        stderr_text = buf.getvalue()
        self.assertNotIn("[FAIL]", stderr_text)
        self.assertEqual(stderr_text.count("[CONVERT]"), 1)

    def test_mixed_success_failure_is_album_transactional(self):
        source_names = ["01-good.flac", "02-bad.flac"]
        _write_sine_flac(os.path.join(self.tmp, source_names[0]))
        _write_corrupt_flac(os.path.join(self.tmp, source_names[1]))
        audit = ConversionInfo()

        converted, failed, _extension, _channels = convert_lossless(
            self.tmp,
            V0_SPEC,
            audit=audit,
        )

        self.assertEqual((converted, failed), (0, 1))
        assert_failed_batch_retains_sources(self.tmp, source_names)
        self.assertFalse(any(
            name.startswith(".cratedigger-convert-")
            for name in os.listdir(self.tmp)
        ))

    @settings(max_examples=8, deadline=None)
    @given(
        good_count=st.integers(min_value=1, max_value=3),
        corrupt_first=st.booleans(),
    )
    def test_generated_failed_batch_retains_every_source(
        self,
        good_count: int,
        corrupt_first: bool,
    ):
        with tempfile.TemporaryDirectory() as folder:
            good_names = [
                f"{index:02d}-good.flac" for index in range(good_count)
            ]
            for name in good_names:
                _write_sine_flac(os.path.join(folder, name), duration=0.05)
            bad_name = "00-bad.flac" if corrupt_first else "99-bad.flac"
            _write_corrupt_flac(os.path.join(folder, bad_name))
            source_names = good_names + [bad_name]

            converted, failed, _extension, _channels = convert_lossless(
                folder,
                V0_SPEC,
                audit=ConversionInfo(),
            )

            self.assertEqual(converted, 0)
            self.assertEqual(failed, 1)
            assert_failed_batch_retains_sources(folder, source_names)

    def test_transaction_checker_trips_on_planted_partial_commit(self):
        _write_corrupt_flac(os.path.join(self.tmp, "01.flac"))
        os.remove(os.path.join(self.tmp, "01.flac"))
        with self.assertRaises(AssertionError):
            assert_failed_batch_retains_sources(self.tmp, ["01.flac"])

    def test_readonly_output_dir_fails_with_permission_reason(self):
        """Fault-injection sweep item: a read-only album directory is a
        second real, deterministic failure mode (distinct from a corrupt
        source file) — surfaced while probing convert_lossless directly,
        reported per the debugging brief even though it's not the fix."""
        _write_sine_flac(os.path.join(self.tmp, "ok.flac"))
        os.chmod(self.tmp, 0o555)
        try:
            buf = io.StringIO()
            audit = ConversionInfo()
            with contextlib.redirect_stderr(buf):
                converted, failed, _orig_ext, _channels = convert_lossless(
                    self.tmp,
                    V0_SPEC,
                    audit=audit,
                )
        finally:
            os.chmod(self.tmp, 0o755)
        self.assertEqual(converted, 0)
        self.assertEqual(failed, 1)
        self.assertEqual(audit.diagnostics[0].category, "read_error")
        self.assertIn("Permission denied", audit.diagnostics[0].stderr_excerpt)
        self.assertNotIn("Permission denied", buf.getvalue())


# ===========================================================================
# Invariant 1 — the fix: _measurement_failed_result threads real stderr
# through to both the persisted detail and the journal.
# ===========================================================================

class TestMeasurementFailedObservability(unittest.TestCase):
    """Typed conversion and world-failure audits cross the JSON boundary."""

    def test_conversion_diagnostic_round_trip(self):
        report = AudioValidationReport(
            outcome="measurement_failed",
            diagnostics=[
                AudioToolDiagnostic(
                    relative_path="01.flac",
                    category="process_unavailable",
                    stderr_excerpt="ffmpeg missing",
                    stderr_bytes=14,
                    stderr_sha256="c" * 64,
                ),
            ],
        )
        import_result = make_import_result(
            decision="conversion_failed",
            error="conversion failed",
        )
        import_result.conversion.failed = 1
        import_result.conversion.diagnostics = [
            AudioToolDiagnostic(
                relative_path="01.flac",
                category="process_unavailable",
                stderr_excerpt="ffmpeg missing",
            ),
        ]
        import_result.conversion.source_validation = report

        restored = type(import_result).from_dict(
            msgspec.json.decode(msgspec.json.encode(import_result))
        )
        self.assertEqual(restored.conversion, import_result.conversion)

    def test_world_failure_report_is_persisted_without_raw_stderr(self):
        report = AudioValidationReport(
            outcome="measurement_failed",
            diagnostics=[
                AudioToolDiagnostic(
                    relative_path="01.flac",
                    category="process_unavailable",
                    stderr_excerpt="ffmpeg missing",
                ),
            ],
        )
        result = _measurement_failed_result(
            mode="path",
            reason="measurement_crashed",
            decision="conversion_failed",
            detail="conversion failed",
            source_path="/album",
            audio_validation=report,
        )
        assert result.failure is not None
        self.assertEqual(result.failure.audio_validation, report)
        self.assertEqual(result.failure.detail, "conversion failed")

    def test_known_bad_missing_typed_diagnostic_trips_checker(self):
        with self.assertRaises(AssertionError):
            assert_conversion_failure_carries_typed_diagnostic(
                ConversionInfo(failed=1),
            )


# ===========================================================================
# Generated property — _diagnostic_from_stderr over generated stderr worlds
# ===========================================================================

@dataclass(frozen=True)
class StderrWorld:
    blob: str
    expect_nonempty: bool


# Bounded arbitrary line content for the untyped harness-crash fallback.
_LINE_BODY = st.text(
    alphabet=st.characters(blacklist_categories=("Cs",), blacklist_characters="\n\r"),
    min_size=0, max_size=150,
)
@st.composite
def stderr_worlds(draw) -> StderrWorld:
    kind = draw(st.sampled_from(
        ("empty", "whitespace", "ordinary", "huge")
    ))
    if kind == "empty":
        return StderrWorld("", False)
    if kind == "whitespace":
        blob = draw(st.text(alphabet=" \t\n", min_size=1, max_size=20))
        return StderrWorld(blob, False)

    is_huge = kind == "huge"
    n_lines = draw(st.integers(min_value=1, max_value=6))
    reps = draw(st.integers(min_value=8, max_value=60)) if is_huge else 1

    lines: list[str] = []
    for _ in range(n_lines):
        body = draw(_LINE_BODY)
        lines.append(f"  harness trace line: {body}")
    blob = "\n".join(lines * reps)
    return StderrWorld(blob, True)


def assert_diagnostic_extraction_invariants(world: StderrWorld, diag: str) -> None:
    """Invariant checker for ``_diagnostic_from_stderr``'s contract — a
    module-level function (not inlined in the property test) so the
    known-bad self-test below can call it directly with a planted
    violation and prove it actually trips.

    * bounded — never longer than the persisted/logged char budget
    * non-empty iff the source stderr had real (non-whitespace) content
    """
    if len(diag) > _STDERR_DIAGNOSTIC_MAX_CHARS:
        raise AssertionError(
            f"diagnostic exceeds the {_STDERR_DIAGNOSTIC_MAX_CHARS}-char "
            f"bound: {len(diag)} chars")
    if bool(diag) != world.expect_nonempty:
        raise AssertionError(
            f"non-emptiness mismatch: diag={diag!r} "
            f"expect_nonempty={world.expect_nonempty}")


class TestDiagnosticFromStderrGenerated(unittest.TestCase):
    """Property: over arbitrary stderr blobs (empty, whitespace-only, wild
    unicode, and huge repeated content),
    ``_diagnostic_from_stderr`` never throws, is bounded in size, is
    non-empty exactly when the input has real content.
    """

    @given(world=stderr_worlds())
    def test_diagnostic_extraction_invariants(self, world: StderrWorld):
        diag = _diagnostic_from_stderr(world.blob)  # must never throw
        assert_diagnostic_extraction_invariants(world, diag)

    def test_known_bad_checker_trips_on_planted_violations(self):
        """Known-bad self-test: feed the checker planted-bad diagnostics
        directly (bypassing ``_diagnostic_from_stderr`` entirely) and prove
        each invariant actually trips rather than passing vacuously."""
        content_world = StderrWorld(
            blob="harness crashed",
            expect_nonempty=True,
        )
        with self.assertRaises(AssertionError):
            # Non-emptiness mismatch: expected empty, planted non-empty.
            assert_diagnostic_extraction_invariants(
                StderrWorld(blob="", expect_nonempty=False),
                "unexpected content",
            )
        with self.assertRaises(AssertionError):
            # Bound violation: planted diagnostic exceeds the char budget.
            assert_diagnostic_extraction_invariants(
                content_world,
                "x" * (_STDERR_DIAGNOSTIC_MAX_CHARS + 1),
            )
        # Sanity: a genuinely correct diagnostic does NOT trip the checker.
        assert_diagnostic_extraction_invariants(
            content_world,
            "harness crashed",
        )


if __name__ == "__main__":
    unittest.main()
