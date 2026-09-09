"""Generated invariants for ``lib.measurement.measure_preimport_state``.

The candidate spectral scan boundary (issue #1378 item 3). A nested-layout
candidate is a confident reject on a folder fact in every decider — the
classify surface's four-fact block, ``candidate_preimport_reject_fact``, and
``full_pipeline_decision_from_evidence`` all answer ``nested_layout`` without
reading a spectral field — so scanning it changes no outcome and only costs
wall clock. Measured on doc2 before the skip, on the synchronous classify
surface the cost motivates: 29s for a 12-track MP3 album, 22s of it the
candidate scan.

Deterministic pins live in
``tests/test_measurement.py::TestMeasurePreimportState`` (the gate itself)
and
``tests/test_import_preview.py::TestNestedCandidateSpectralCostThroughTheLane``
(the composition with the real classify lane). This property patrols the
world space around them: track count, how many of those tracks sit below the
album root (0, some, or all — a MIXED album is what separates ``any`` from
``all`` in the layout derivation), codec eligibility in both directions, and
whether the audio-integrity gate fired first — the branch that must keep
scanning, because issue #1030 pinned a corrupt candidate's attempt audit as
evidence.

World scope, stated so a later reader knows what these clauses do NOT
police. Every world seeds an EMPTY curator bad-hash table, so the bad-hash
gate never fires here; that branch's own precedence is patrolled by
``tests/test_import_preview_generated.py``. And no world supplies a
``cd_rip_verify_fn``, so ``cd_rip_verification`` is always ``None`` — the
other conjunct of the gate under test. The flat clause reads it anyway, so
widening the harness toward the persist lane cannot turn a legitimately
CD-rip-proven album into an accusation.
"""

import contextlib
import os
import shutil
import tempfile
import unittest
from pathlib import Path
from typing import Literal
from unittest.mock import patch

import msgspec
from hypothesis import example, given
from hypothesis import strategies as st

import tests._hypothesis_profiles  # noqa: F401  (loads active profile)
from lib.config import CratediggerConfig
from lib.measurement import (
    ExistingSpectralAuditLookup,
    PreimportMeasurement,
    measure_preimport_state,
)
from lib.quality import (
    AudioToolDiagnostic,
    AudioValidationReport,
    SpectralAnalysisDetail,
    SpectralDetail,
    SpectralMeasurement,
)
from lib.spectral_check import SPECTRAL_MEASUREMENT_VERSION
from lib.util import AudioValidationResult
from tests.fakes import FakePipelineDB

_FIXTURES = Path(__file__).parent / "fixtures" / "audio_hash"

# (extension, download_filetype, spectral-eligible). Eligibility is written
# down here rather than read back from ``_needs_spectral_check``: a clause
# that asked production what production should do would agree with any
# mutant of it.
_CODECS = (
    ("mp3", "mp3", True),
    ("flac", "flac", True),
    ("ogg", "ogg", False),
)


def spectral_scan_boundary_violations(
    *,
    layout: Literal["flat", "nested"],
    eligible: bool,
    audio_corrupt: bool,
    measurement: PreimportMeasurement,
    scanned: list[str],
) -> list[str]:
    """Accumulating checker: every clause evaluates, so clause ordering can
    never mask a second violation in the same world.

    ``scanned`` records one entry per candidate-side analyzer call.
    """
    violations: list[str] = []
    candidate = measurement.spectral_audit.candidate
    attempted = bool(candidate is not None and candidate.attempted)

    if measurement.folder_layout != layout:
        violations.append(
            f"world laid the album out {layout!r} but measurement reported "
            f"{measurement.folder_layout!r}"
        )
    if audio_corrupt and not scanned:
        violations.append(
            "a corrupt candidate lost its attempt audit — issue #1030 pinned "
            "that scan as evidence and the layout skip must not reach it"
        )
    if not audio_corrupt and layout == "nested" and scanned:
        violations.append(
            f"a nested candidate paid {len(scanned)} spectral scan(s) to be "
            "told to flatten the folder"
        )
    if not audio_corrupt and layout == "nested" and (
        attempted or measurement.download_spectral is not None
    ):
        violations.append(
            "a nested candidate carries a spectral grade nothing reads"
        )
    if (
        not audio_corrupt
        and layout == "flat"
        and eligible
        # A measured CD-rip proof legitimately stops the scan too, and it is
        # the other conjunct of the very expression under test. No world here
        # supplies ``cd_rip_verify_fn``, so this reads ``None`` today — it is
        # here so that widening the harness toward the persist lane, which
        # does supply one, cannot make this clause accuse correct code
        # (reader finding, PR #1386; the #1332 shape).
        and measurement.cd_rip_verification is None
        and not scanned
    ):
        violations.append(
            "a flat spectral-eligible candidate was not scanned at all"
        )
    if not audio_corrupt and not eligible and scanned:
        # The other direction of the eligibility dimension. Without it the
        # ineligible codec row only ever keeps the clause above quiet, and a
        # mutant pinning ``lossless_candidate=True`` at the gate scanned a
        # flat Ogg album with this property still green (PR #1386).
        violations.append(
            f"an uncalibrated codec paid {len(scanned)} spectral scan(s); "
            "only MP3 and lossless candidates have a cliff policy"
        )
    if (
        attempted
        and candidate is not None
        and candidate.grade is not None
        and measurement.download_spectral is None
    ):
        # The payoff half of this property's own title: a scan that happened
        # must reach the policy-facing measurement. A mutant nulling
        # ``download_spectral`` on the flat path left every other clause
        # quiet (PR #1386).
        violations.append(
            "a measured candidate grade never reached download_spectral"
        )
    return violations


def _corrupt_result(relative_path: str) -> AudioValidationResult:
    return AudioValidationResult(
        AudioValidationReport(
            outcome="audio_corrupt",
            files_checked=1,
            files_failed=1,
            diagnostics=[AudioToolDiagnostic(
                relative_path=relative_path,
                category="decode_error",
                return_code=69,
                stderr_excerpt="invalid sync code",
            )],
        ),
        failed_paths=(relative_path,),
    )


class TestSpectralScanBoundaryGenerated(unittest.TestCase):
    """The REAL ``measure_preimport_state`` over real trees of real fixture
    audio.

    Four injections, all through the function's own sanctioned kwarg-DI
    seams: the sox/ffmpeg spectral analyzer, the ffmpeg integrity check (for
    corrupt worlds only), the BeetsDB exact-release lookup, and the bad-hash
    gate's DB port. The empty exact-release lookup is what makes the
    checker's "one entry per candidate-side analyzer call" true — one
    ``analyze`` serves both sides, and only an empty lookup keeps HAVE calls
    out of ``scanned``. The HAVE side's own collection under a skipped
    candidate scan is pinned separately, in
    ``tests/test_measurement.py::test_nested_layout_still_collects_the_have_side``."""

    @given(
        codec_index=st.integers(min_value=0, max_value=len(_CODECS) - 1),
        audio_corrupt=st.booleans(),
        track_count=st.integers(min_value=1, max_value=3),
        nested_tracks=st.integers(min_value=0, max_value=3),
    )
    # A MIXED album — some tracks at the root, some below it — is the world
    # that separates ``any`` from ``all`` in the layout derivation. An
    # earlier strategy drew a boolean and put every track in one
    # subdirectory, where the two are indistinguishable, and a mutant runner
    # proved a mixed FLAC album flipped to ``flat`` and paid the scan with
    # this property still green (PR #1386).
    @example(codec_index=1, audio_corrupt=False, track_count=2, nested_tracks=1)
    @example(codec_index=0, audio_corrupt=False, track_count=2, nested_tracks=1)
    @example(codec_index=0, audio_corrupt=False, track_count=2, nested_tracks=2)
    @example(codec_index=1, audio_corrupt=False, track_count=1, nested_tracks=1)
    @example(codec_index=0, audio_corrupt=True, track_count=1, nested_tracks=1)
    @example(codec_index=0, audio_corrupt=False, track_count=1, nested_tracks=0)
    @example(codec_index=2, audio_corrupt=False, track_count=1, nested_tracks=0)
    @example(codec_index=2, audio_corrupt=False, track_count=1, nested_tracks=1)
    def test_only_a_flat_eligible_candidate_pays_for_a_spectral_scan(
        self,
        codec_index: int,
        audio_corrupt: bool,
        track_count: int,
        nested_tracks: int,
    ) -> None:
        extension, download_filetype, eligible = _CODECS[codec_index]
        nested_tracks = min(nested_tracks, track_count)
        layout: Literal["flat", "nested"] = (
            "nested" if nested_tracks else "flat"
        )
        scanned: list[str] = []

        def analyze(path: str) -> SpectralAnalysisDetail:
            scanned.append(path)
            return SpectralAnalysisDetail(
                attempted=True,
                grade="genuine",
                bitrate_kbps=320,
                spectral_measurement_version=SPECTRAL_MEASUREMENT_VERSION,
            )

        # try/finally, NOT addCleanup: inside a ``@given`` body addCleanup
        # defers removal past every example and leaks one tmpfs world per
        # example (issue #1214; enforced by test_given_body_cleanup_audit).
        source = tempfile.mkdtemp()
        try:
            subdirectory = os.path.join(source, "Disc 1")
            if nested_tracks:
                os.makedirs(subdirectory, exist_ok=True)
            for track in range(track_count):
                shutil.copy(
                    _FIXTURES / f"sine_440.{extension}",
                    os.path.join(
                        subdirectory if track < nested_tracks else source,
                        f"{track + 1:02d}.{extension}",
                    ),
                )

            # Clean worlds run the real checker with checking off; corrupt
            # worlds replace it outright, which is how the world reaches the
            # audio_corrupt branch with genuinely decodable fixture audio.
            integrity = (
                patch(
                    "lib.measurement.validate_audio",
                    return_value=_corrupt_result(f"01.{extension}"),
                )
                if audio_corrupt
                else contextlib.nullcontext()
            )
            with integrity:
                measurement = measure_preimport_state(
                    path=source,
                    mb_release_id="mbid-1378",
                    label="Artist - Album",
                    download_filetype=download_filetype,
                    download_min_bitrate_bps=320_000,
                    download_is_vbr=False,
                    cfg=CratediggerConfig(audio_check_mode="off"),
                    bad_hash_db=FakePipelineDB(),
                    spectral_detail_analyzer=analyze,
                    existing_spectral_resolver=(
                        lambda _release: ExistingSpectralAuditLookup()
                    ),
                )
        finally:
            shutil.rmtree(source, ignore_errors=True)

        violations = spectral_scan_boundary_violations(
            layout=layout,
            eligible=eligible,
            audio_corrupt=audio_corrupt,
            measurement=measurement,
            scanned=scanned,
        )
        self.assertEqual(violations, [])


class TestSpectralScanBoundaryCheckerTripsOnViolations(unittest.TestCase):
    """Known-bad self-tests. Per clause: one minimal world that makes THAT
    clause's condition true while every earlier clause passes, asserting
    that clause's own message; plus worlds where the same clauses' inputs
    look suspicious and production is right, asserting silence."""

    def _measurement(
        self,
        *,
        layout: Literal["flat", "nested"] = "flat",
        attempted: bool = True,
        grade: str | None = None,
        download_spectral: SpectralMeasurement | None = None,
    ) -> PreimportMeasurement:
        return PreimportMeasurement(
            folder_layout=layout,
            audio_file_count=1,
            download_spectral=download_spectral,
            spectral_audit=SpectralDetail(
                candidate=SpectralAnalysisDetail(
                    attempted=attempted, grade=grade,
                ),
                existing=SpectralAnalysisDetail(attempted=False),
            ),
        )

    def test_layout_clause_trips(self):
        violations = spectral_scan_boundary_violations(
            layout="nested", eligible=True, audio_corrupt=False,
            measurement=self._measurement(layout="flat", attempted=False),
            scanned=[],
        )
        self.assertEqual(len(violations), 1)
        self.assertIn("but measurement reported 'flat'", violations[0])

    def test_corrupt_attempt_audit_clause_trips(self):
        violations = spectral_scan_boundary_violations(
            layout="flat", eligible=True, audio_corrupt=True,
            measurement=self._measurement(layout="flat", attempted=False),
            scanned=[],
        )
        self.assertEqual(len(violations), 1)
        self.assertIn("lost its attempt audit", violations[0])

    def test_nested_scan_clause_trips(self):
        violations = spectral_scan_boundary_violations(
            layout="nested", eligible=True, audio_corrupt=False,
            measurement=self._measurement(layout="nested", attempted=False),
            scanned=["/candidate"],
        )
        self.assertEqual(len(violations), 1)
        self.assertIn("paid 1 spectral scan(s)", violations[0])

    def test_nested_grade_clause_trips(self):
        """A grade with no recorded scan — the shape a mutant that projected
        a stale audit instead of measuring would produce."""
        violations = spectral_scan_boundary_violations(
            layout="nested", eligible=True, audio_corrupt=False,
            measurement=self._measurement(layout="nested", attempted=True),
            scanned=[],
        )
        self.assertEqual(len(violations), 1)
        self.assertIn("carries a spectral grade nothing reads", violations[0])

    def test_flat_eligible_clause_trips(self):
        violations = spectral_scan_boundary_violations(
            layout="flat", eligible=True, audio_corrupt=False,
            measurement=self._measurement(layout="flat", attempted=False),
            scanned=[],
        )
        self.assertEqual(len(violations), 1)
        self.assertIn("was not scanned at all", violations[0])

    def test_uncalibrated_codec_clause_trips(self):
        violations = spectral_scan_boundary_violations(
            layout="flat", eligible=False, audio_corrupt=False,
            measurement=self._measurement(layout="flat", attempted=False),
            scanned=["/candidate"],
        )
        self.assertEqual(len(violations), 1)
        self.assertIn("an uncalibrated codec paid 1 spectral scan(s)",
                      violations[0])

    def test_uncalibrated_corrupt_scan_is_quiet(self):
        """Q3 for the eligibility clause: the corrupt branch collects its
        attempt audit whatever the codec, so a scanned corrupt Ogg album is
        correct code."""
        violations = spectral_scan_boundary_violations(
            layout="flat", eligible=False, audio_corrupt=True,
            measurement=self._measurement(layout="flat", attempted=True),
            scanned=["/candidate"],
        )
        self.assertEqual(violations, [])

    def test_measured_grade_reaches_download_spectral_clause_trips(self):
        violations = spectral_scan_boundary_violations(
            layout="flat", eligible=True, audio_corrupt=False,
            measurement=self._measurement(
                layout="flat", attempted=True, grade="genuine",
            ),
            scanned=["/candidate"],
        )
        self.assertEqual(len(violations), 1)
        self.assertIn("never reached download_spectral", violations[0])

    def test_attempted_without_a_grade_is_quiet(self):
        """Q3 for the payoff clause: an attempted scan that came back with no
        grade legitimately projects to ``None``
        (``spectral_measurement_from_attempt`` returns ``None`` on a missing
        grade), so the clause reads the grade rather than ``attempted``."""
        violations = spectral_scan_boundary_violations(
            layout="flat", eligible=True, audio_corrupt=False,
            measurement=self._measurement(
                layout="flat", attempted=True, grade=None,
            ),
            scanned=["/candidate"],
        )
        self.assertEqual(violations, [])

    def test_flat_cd_rip_proven_unscanned_is_quiet(self):
        """Q3 for the flat clause's other conjunct: a measured CD-rip proof
        legitimately stops the scan, so an unscanned flat lossless album
        carrying one is correct code."""
        from lib.quality import (
            AccurateRipBitMatch,
            CdRipBitVerification,
            CdTocIdentity,
        )

        proven = msgspec.structs.replace(
            self._measurement(layout="flat", attempted=False),
            cd_rip_verification=CdRipBitVerification(
                toc=CdTocIdentity([0], 470, "ar-id", "mb-disc"),
                accuraterip=AccurateRipBitMatch(
                    provider="accuraterip",
                    url="https://www.accuraterip.com/example.bin",
                    checksum_version="arv1",
                    read_offset_samples=0,
                    track_confidences=[8],
                    track_checksums=[0x12345678],
                    response_sha256="a" * 64,
                ),
            ),
        )
        violations = spectral_scan_boundary_violations(
            layout="flat", eligible=True, audio_corrupt=False,
            measurement=proven, scanned=[],
        )
        self.assertEqual(violations, [])

    def test_flat_ineligible_unscanned_is_quiet(self):
        """Q3 for the flat clause: an Ogg album is legitimately unscanned —
        no calibrated cliff policy exists for it — and the clause reads
        ``eligible`` precisely so it does not accuse that correct code."""
        violations = spectral_scan_boundary_violations(
            layout="flat", eligible=False, audio_corrupt=False,
            measurement=self._measurement(layout="flat", attempted=False),
            scanned=[],
        )
        self.assertEqual(violations, [])

    def test_corrupt_nested_scan_is_quiet(self):
        """Q3 for the two nested clauses: a corrupt candidate keeps its
        attempt audit whatever its layout, so a scan in a nested corrupt
        world is correct code and must not be accused."""
        violations = spectral_scan_boundary_violations(
            layout="nested", eligible=True, audio_corrupt=True,
            measurement=self._measurement(layout="nested", attempted=True),
            scanned=["/candidate"],
        )
        self.assertEqual(violations, [])

    def test_clean_flat_scanned_world_is_quiet(self):
        violations = spectral_scan_boundary_violations(
            layout="flat", eligible=True, audio_corrupt=False,
            measurement=self._measurement(
                layout="flat", attempted=True, grade="genuine",
                download_spectral=SpectralMeasurement.from_parts(
                    "genuine", 320,
                ),
            ),
            scanned=["/candidate"],
        )
        self.assertEqual(violations, [])

    def test_accumulation_reports_every_tripped_clause(self):
        violations = spectral_scan_boundary_violations(
            layout="nested", eligible=True, audio_corrupt=False,
            measurement=self._measurement(layout="flat", attempted=True),
            scanned=["/candidate"],
        )
        self.assertEqual(len(violations), 3)


if __name__ == "__main__":
    unittest.main()
