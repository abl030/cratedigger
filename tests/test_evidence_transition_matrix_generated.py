"""Generated transition matrix for evidence persistence and admission (#1030)."""

from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path
from typing import Literal

from hypothesis import example, given, settings
from hypothesis import strategies as st

import tests._hypothesis_profiles  # noqa: F401
from lib.measurement import PreimportMeasurement, spectral_measurement_from_attempt
from lib.quality import (
    CURRENT_EVIDENCE_LINEAGE_VERSION,
    AlbumQualityEvidence,
    AlbumQualityEvidenceFile,
    AudioQualityMeasurement,
    SpectralAnalysisDetail,
    SpectralDetail,
    evidence_decision_name,
    full_pipeline_decision_from_evidence,
)
from lib.quality_evidence import (
    SpectralWriteIntent,
    current_evidence_preserves_source_spectral,
    load_candidate_evidence_for_decision,
    load_candidate_evidence_for_source,
    persist_candidate_evidence_from_measurement,
    snapshot_audio_files,
)
from lib.spectral_check import SPECTRAL_MEASUREMENT_VERSION
from tests.evidence_helpers import (
    make_album_quality_evidence,
    make_audio_corrupt_validation_report,
)
from tests.fakes import FakePipelineDB
from tests.helpers import make_request_row
from tests.test_pipeline_db import make_db, requires_postgres

EvidenceRole = Literal["candidate", "current", "dual"]
Generation = Literal["null", "old", "current", "future"]
LineageShape = Literal[
    "legacy_untyped",
    "source_measured",
    "source_carried",
    "installed_measured",
]
EarlyFact = Literal[
    "none", "corrupt", "bad_hash", "nested", "empty", "mixed",
]
FreshAudit = Literal["success", "error", "absent"]


def evidence_transition_violation(
    *,
    source_present: bool,
    early_fact: EarlyFact,
    fresh_audit: FreshAudit,
    collision: bool,
    role: EvidenceRole,
    preserve_current_source_spectral: bool,
    old_generation: int | None,
    canonical_grade: str | None,
    canonical_generation: int | None,
    decision_ready: bool,
    decision_name: str | None,
    quality_observed: bool = True,
) -> str | None:
    """Name one violated #1030 transition invariant for shrinking/mutation."""
    terminal = early_fact != "none"
    if not source_present:
        return None if not decision_ready else "missing source was admitted"
    if fresh_audit == "error" and not terminal:
        return None if not decision_ready else "failed spectral attempt was admitted"
    if terminal and not decision_ready:
        return "terminal early fact was blocked before the unified decider"
    # Issue #1355 item 2: an early reject's readiness never depends on
    # quality having been observed, but a candidate with NEITHER a durable
    # reject fact NOR a quality measurement is genuinely incomplete and must
    # still fail closed — reached only once ``fresh_audit == "error"`` has
    # already been handled above, so this clause never contradicts it.
    if not terminal and not quality_observed and decision_ready:
        return (
            "incomplete candidate admitted without a reject fact or "
            "quality measurement"
        )
    if terminal and decision_name != _decision_name_for_early_fact(early_fact):
        return "unified decider did not receive the persisted early fact"
    if (
        collision
        and role == "candidate"
        and fresh_audit == "error"
        and canonical_grade is not None
    ):
        return "candidate-only exact attempt retained a stale spectral tuple"
    if (
        collision
        and role in {"current", "dual"}
        and fresh_audit == "success"
        and not preserve_current_source_spectral
        and (canonical_grade, canonical_generation) != (
            "genuine",
            SPECTRAL_MEASUREMENT_VERSION,
        )
    ):
        return "current-owned remeasurable source spectral did not refresh"
    if (
        collision
        and role in {"current", "dual"}
        and fresh_audit == "success"
        and preserve_current_source_spectral
        and (canonical_grade, canonical_generation) != (
            "suspect",
            old_generation,
        )
    ):
        return "dual-role candidate overwrote current source-carried spectral"
    return None


def _generation_value(generation: Generation) -> int | None:
    if generation == "null":
        return None
    if generation == "old":
        return max(0, SPECTRAL_MEASUREMENT_VERSION - 1)
    if generation == "current":
        return SPECTRAL_MEASUREMENT_VERSION
    return SPECTRAL_MEASUREMENT_VERSION + 1


def _old_measurement(
    lineage_shape: LineageShape,
    generation: Generation,
) -> tuple[AudioQualityMeasurement, int]:
    subject: Literal["source", "installed"] | None
    provenance: Literal["measured", "carried"] | None
    lineage = 4
    converted_from = None
    if lineage_shape == "legacy_untyped":
        lineage = 3
        subject = None
        provenance = None
    elif lineage_shape == "source_measured":
        subject = "source"
        provenance = "measured"
    elif lineage_shape == "source_carried":
        subject = "source"
        provenance = "carried"
        converted_from = "flac"
    else:
        subject = "installed"
        provenance = "measured"
    return AudioQualityMeasurement(
        min_bitrate_kbps=128,
        avg_bitrate_kbps=130,
        median_bitrate_kbps=129,
        format="Opus",
        spectral_grade="suspect",
        spectral_bitrate_kbps=96,
        spectral_subject=subject,
        spectral_provenance=provenance,
        spectral_measurement_version=_generation_value(generation),
        was_converted_from=converted_from,
    ), lineage


def _fresh_detail(fresh_audit: FreshAudit) -> SpectralAnalysisDetail:
    if fresh_audit == "success":
        return SpectralAnalysisDetail(
            attempted=True,
            grade="genuine",
            bitrate_kbps=192,
            spectral_measurement_version=SPECTRAL_MEASUREMENT_VERSION,
        )
    if fresh_audit == "error":
        return SpectralAnalysisDetail(
            attempted=True,
            error="RuntimeError: analyzer failed",
            spectral_measurement_version=SPECTRAL_MEASUREMENT_VERSION,
        )
    return SpectralAnalysisDetail(attempted=False)


def _decision_name_for_early_fact(early_fact: EarlyFact) -> str:
    return {
        "corrupt": "audio_corrupt",
        "bad_hash": "bad_audio_hash",
        "nested": "nested_layout",
        "empty": "empty_fileset",
        "mixed": "mixed_source",
    }[early_fact]


class TestEvidenceTransitionMatrixGenerated(unittest.TestCase):
    @example(
        role="candidate",
        generation="null",
        source_present=True,
        lineage_shape="source_measured",
        collision=True,
        early_fact="corrupt",
        fresh_audit="success",
        quality_observed=True,
    )
    @example(
        role="dual",
        generation="old",
        source_present=True,
        lineage_shape="source_carried",
        collision=True,
        early_fact="none",
        fresh_audit="success",
        quality_observed=True,
    )
    @example(
        # Issue #1355 item 2's regression world: the preview worker never
        # ran the harness, so quality is genuinely unmeasured. The
        # candidate must still reach the unified decider on the reject
        # fact alone.
        role="candidate",
        generation="null",
        source_present=True,
        lineage_shape="source_measured",
        collision=True,
        early_fact="corrupt",
        fresh_audit="success",
        quality_observed=False,
    )
    @example(
        # The converse: no reject fact and no quality measurement is
        # genuinely incomplete and must still fail closed.
        role="candidate",
        generation="null",
        source_present=True,
        lineage_shape="source_measured",
        collision=True,
        early_fact="none",
        fresh_audit="success",
        quality_observed=False,
    )
    @given(
        role=st.sampled_from(("candidate", "current", "dual")),
        generation=st.sampled_from(("null", "old", "current", "future")),
        source_present=st.booleans(),
        lineage_shape=st.sampled_from((
            "legacy_untyped",
            "source_measured",
            "source_carried",
            "installed_measured",
        )),
        collision=st.booleans(),
        early_fact=st.sampled_from((
            "none", "corrupt", "bad_hash", "nested", "empty", "mixed",
        )),
        fresh_audit=st.sampled_from(("success", "error", "absent")),
        quality_observed=st.booleans(),
    )
    def test_persist_reload_admit_transition_matrix(
        self,
        role: EvidenceRole,
        generation: Generation,
        source_present: bool,
        lineage_shape: LineageShape,
        collision: bool,
        early_fact: EarlyFact,
        fresh_audit: FreshAudit,
        quality_observed: bool,
    ) -> None:
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=42, mb_release_id="matrix-release"))
        download_log_id = db.log_download(request_id=42, outcome="rejected")
        with tempfile.TemporaryDirectory() as root:
            source = Path(root, "candidate")
            source.mkdir()
            if early_fact != "empty":
                Path(source, "01.opus").write_bytes(b"candidate")
            if early_fact == "mixed":
                Path(source, "02.flac").write_bytes(b"candidate-lossless")
            candidate_files = snapshot_audio_files(str(source))

            old_measurement, lineage = _old_measurement(
                lineage_shape,
                generation,
            )
            old_files = candidate_files
            if not collision:
                old_files = [
                    AlbumQualityEvidenceFile(
                        relative_path="old.opus",
                        size_bytes=3,
                        mtime_ns=7,
                        extension="opus",
                        container="opus",
                        codec="opus",
                    )
                ] if candidate_files else []
            old = make_album_quality_evidence(
                mb_release_id="matrix-release",
                source_path=str(source),
                files=old_files,
                measurement=old_measurement,
                codec="opus",
                container="opus",
                storage_format="Opus",
                lineage_version=lineage,
                preserve_spectral_measurement_version=True,
            )
            db.upsert_album_quality_evidence(old)
            stored_old = db.find_album_quality_evidence(
                mb_release_id=old.mb_release_id,
                snapshot_fingerprint=old.snapshot_fingerprint,
            )
            assert stored_old is not None and stored_old.id is not None
            if role in {"candidate", "dual"}:
                db.set_download_log_candidate_evidence(
                    download_log_id,
                    stored_old.id,
                )
            if role in {"current", "dual"}:
                assert db.set_request_current_evidence(42, stored_old.id)
            preserve_current_source_spectral = (
                current_evidence_preserves_source_spectral(stored_old)
            )

            detail = _fresh_detail(fresh_audit)
            corrupt = early_fact == "corrupt"
            files_for_attempt = [] if early_fact == "empty" else candidate_files
            measurement = PreimportMeasurement(
                corrupt_files=(
                    ["01.opus"] if corrupt else []
                ),
                audio_validation=(
                    make_audio_corrupt_validation_report("01.opus")
                    if corrupt
                    else PreimportMeasurement().audio_validation
                ),
                audio_corrupt=corrupt,
                audio_error="decode error" if corrupt else None,
                matched_bad_hash_id=(1 if early_fact == "bad_hash" else None),
                matched_bad_track_path=(
                    "01.opus" if early_fact == "bad_hash" else None
                ),
                download_spectral=spectral_measurement_from_attempt(detail),
                folder_layout=("nested" if early_fact == "nested" else "flat"),
                audio_file_count=len(files_for_attempt),
                filetype_band="opus" if files_for_attempt else "",
                min_bitrate_kbps=(128 if quality_observed else None),
                spectral_audit=SpectralDetail(
                    candidate=detail,
                    existing=SpectralAnalysisDetail(attempted=False),
                ),
            )
            persisted = persist_candidate_evidence_from_measurement(
                db,
                mb_release_id="matrix-release",
                source_path=str(source),
                measurement=measurement,
                download_log_id=download_log_id,
                files=files_for_attempt,
            )
            self.assertEqual(persisted.status, "ready")
            receipt = persisted.persistence_receipt
            assert receipt is not None

            if not source_present:
                for file in candidate_files:
                    os.unlink(Path(source, file.relative_path))
                source.rmdir()

            cache = load_candidate_evidence_for_source(
                db,
                source_path=str(source),
                download_log_id=download_log_id,
            )
            admitted = load_candidate_evidence_for_decision(
                db,
                source_path=str(source),
                download_log_id=download_log_id,
                persistence_receipt=receipt,
            )
            decision_name = None
            if admitted.evidence is not None:
                decision_name = evidence_decision_name(
                    full_pipeline_decision_from_evidence(admitted.evidence),
                )
            canonical = db.load_album_quality_evidence_by_id(
                receipt.evidence_id,
            )
            assert canonical is not None

            violation = evidence_transition_violation(
                source_present=source_present,
                early_fact=early_fact,
                fresh_audit=fresh_audit,
                collision=collision,
                role=role,
                preserve_current_source_spectral=(
                    preserve_current_source_spectral
                ),
                old_generation=_generation_value(generation),
                canonical_grade=canonical.measurement.spectral_grade,
                canonical_generation=(
                    canonical.measurement.spectral_measurement_version
                ),
                decision_ready=admitted.evidence is not None,
                decision_name=decision_name,
                quality_observed=quality_observed,
            )
            self.assertIsNone(violation, violation)

            # Cache reuse never consumes the attempt receipt. Any canonical
            # tuple it sees remains generation-strict, including source-carried
            # tuples that are only exceptional on the installed HAVE side.
            canonical_has_spectral = (
                canonical.measurement.spectral_grade is not None
                or canonical.measurement.spectral_bitrate_kbps is not None
            )
            canonical_current = (
                canonical.measurement.spectral_measurement_version
                == SPECTRAL_MEASUREMENT_VERSION
            )
            if source_present:
                # Cache admission also needs a quality measurement UNLESS a
                # durable reject fact already makes it irrelevant (issue
                # #1355 item 2) — cache mode never bypasses the spectral-
                # generation check itself, only the quality-completeness one
                # that check is layered on top of.
                quality_ready = early_fact != "none" or quality_observed
                self.assertEqual(
                    cache.evidence is not None,
                    quality_ready and (not canonical_has_spectral or canonical_current),
                )
            else:
                self.assertIsNone(cache.evidence)

    def test_known_bad_unconditional_generation_gate_is_qualified(self):
        violation = evidence_transition_violation(
            source_present=True,
            early_fact="corrupt",
            fresh_audit="success",
            collision=True,
            role="candidate",
            preserve_current_source_spectral=False,
            old_generation=None,
            canonical_grade="suspect",
            canonical_generation=None,
            decision_ready=False,
            decision_name=None,
        )
        self.assertIn("terminal early fact", violation or "")

    def test_known_bad_incomplete_candidate_without_a_fact_is_qualified(self):
        """Q1: the issue #1355 item 2 converse clause trips on a genuinely
        incomplete candidate (no reject fact, no quality) that was admitted
        anyway."""
        violation = evidence_transition_violation(
            source_present=True,
            early_fact="none",
            fresh_audit="success",
            collision=True,
            role="candidate",
            preserve_current_source_spectral=False,
            old_generation=None,
            canonical_grade=None,
            canonical_generation=None,
            decision_ready=True,
            decision_name=None,
            quality_observed=False,
        )
        self.assertIn(
            "incomplete candidate admitted without a reject fact or "
            "quality measurement",
            violation or "",
        )

    def test_known_bad_incomplete_candidate_clause_stays_quiet_when_correct(
        self,
    ):
        """Q3: the clause added above must not fire on either of production's
        two correct answers — a genuinely incomplete non-terminal candidate
        correctly refused (``decision_ready=False``), or a terminal reject
        fact correctly admitted despite unmeasured quality (issue #1355
        item 2's fix)."""
        still_incomplete = evidence_transition_violation(
            source_present=True,
            early_fact="none",
            fresh_audit="success",
            collision=True,
            role="candidate",
            preserve_current_source_spectral=False,
            old_generation=None,
            canonical_grade=None,
            canonical_generation=None,
            decision_ready=False,
            decision_name=None,
            quality_observed=False,
        )
        self.assertIsNone(still_incomplete)

        fact_admitted_unmeasured = evidence_transition_violation(
            source_present=True,
            early_fact="corrupt",
            fresh_audit="success",
            collision=True,
            role="candidate",
            preserve_current_source_spectral=False,
            old_generation=None,
            canonical_grade=None,
            canonical_generation=None,
            decision_ready=True,
            decision_name="audio_corrupt",
            quality_observed=False,
        )
        self.assertIsNone(fact_admitted_unmeasured)

    def test_known_bad_stale_tuple_merge_is_qualified(self):
        violation = evidence_transition_violation(
            source_present=True,
            early_fact="corrupt",
            fresh_audit="error",
            collision=True,
            role="candidate",
            preserve_current_source_spectral=False,
            old_generation=None,
            canonical_grade="suspect",
            canonical_generation=None,
            decision_ready=True,
            decision_name="audio_corrupt",
        )
        self.assertIn("retained a stale spectral tuple", violation or "")

    def test_known_bad_overbroad_current_source_preservation_is_qualified(self):
        violation = evidence_transition_violation(
            source_present=True,
            early_fact="none",
            fresh_audit="success",
            collision=True,
            role="current",
            preserve_current_source_spectral=False,
            old_generation=None,
            canonical_grade="suspect",
            canonical_generation=None,
            decision_ready=True,
            decision_name="import",
        )
        self.assertIn("remeasurable source spectral", violation or "")


StoredSpectralShape = Literal[
    "preserved_source", "native_source", "installed_measured", "legacy_v1",
]
IncomingSpectralShape = Literal[
    "grade_present", "grade_absent", "preserved_source_now",
]

_WE1_FILES = [
    AlbumQualityEvidenceFile(
        relative_path="01.mp3",
        size_bytes=128_000,
        mtime_ns=1_700_000_000_000_000_000,
        extension="mp3",
        container="mp3",
        codec="mp3",
    ),
]


def _we1_stored_evidence(
    shape: StoredSpectralShape,
    mb_release_id: str,
) -> AlbumQualityEvidence:
    """Build a stored row exercising one gate of ``spectral_write_decision``.

    Every shape shares the same files/container/format so a same-address
    incoming write below always hits ``ON CONFLICT DO UPDATE`` rather than
    a fresh insert.
    """
    if shape == "preserved_source":
        # R19-shaped: a known-lossy mp3 derivative of a recorded lossless
        # source. Only protected from replacement while current-owned.
        # The capture facts (cliff_hz/codec_family/ultrasonic_deficit_db)
        # carry a sentinel distinct from every other shape so a mutant that
        # drops one of the eight columns from the shared decision becomes
        # an observable mismatch, not a None-vs-None non-event.
        measurement = AudioQualityMeasurement(
            min_bitrate_kbps=128, avg_bitrate_kbps=130, median_bitrate_kbps=129,
            format="MP3", spectral_grade="likely_transcode",
            spectral_bitrate_kbps=96, spectral_subject="source",
            spectral_provenance="carried", was_converted_from="flac",
            cliff_hz=15000, codec_family="mp3", ultrasonic_deficit_db=12.5,
        )
        lineage_version = 4
    elif shape == "native_source":
        # Source-subject but NOT a recorded lossless derivative: ordinary
        # remeasurable source spectral, no R19 protection.
        measurement = AudioQualityMeasurement(
            min_bitrate_kbps=128, avg_bitrate_kbps=130, median_bitrate_kbps=129,
            format="MP3", spectral_grade="suspect",
            spectral_bitrate_kbps=96, spectral_subject="source",
            spectral_provenance="measured", was_converted_from=None,
            spectral_measurement_version=SPECTRAL_MEASUREMENT_VERSION,
            cliff_hz=16000, codec_family="mp3", ultrasonic_deficit_db=8.0,
        )
        lineage_version = 4
    elif shape == "installed_measured":
        # An ordinary installed-subject measurement: R19 never applies to
        # the stored side (subject != source), but its subject is exactly
        # what disjunct 4 reads.
        measurement = AudioQualityMeasurement(
            min_bitrate_kbps=192, avg_bitrate_kbps=192, median_bitrate_kbps=192,
            format="MP3", spectral_grade="genuine",
            spectral_bitrate_kbps=192, spectral_subject="installed",
            spectral_provenance="measured", was_converted_from=None,
            spectral_measurement_version=SPECTRAL_MEASUREMENT_VERSION,
            cliff_hz=20000, codec_family="mp3", ultrasonic_deficit_db=1.0,
        )
        lineage_version = 4
    else:  # legacy_v1
        measurement = AudioQualityMeasurement(
            min_bitrate_kbps=64, avg_bitrate_kbps=64, median_bitrate_kbps=64,
            format="MP3", spectral_grade="marginal",
            spectral_bitrate_kbps=64, spectral_subject="installed",
            spectral_provenance="measured", was_converted_from=None,
            spectral_measurement_version=SPECTRAL_MEASUREMENT_VERSION,
            cliff_hz=14000, codec_family="mp3", ultrasonic_deficit_db=5.0,
        )
        lineage_version = 1
    return make_album_quality_evidence(
        mb_release_id=mb_release_id,
        files=_WE1_FILES,
        codec="mp3",
        container="mp3",
        storage_format="MP3",
        measurement=measurement,
        lineage_version=lineage_version,
        preserve_spectral_measurement_version=True,
    )


def _we1_incoming_evidence(
    shape: IncomingSpectralShape,
    stored: AlbumQualityEvidence,
) -> AlbumQualityEvidence:
    """A same-address rewrite of ``stored`` carrying one incoming shape."""
    if shape == "grade_present":
        measurement = AudioQualityMeasurement(
            min_bitrate_kbps=200, avg_bitrate_kbps=200, median_bitrate_kbps=200,
            format="MP3", spectral_grade="genuine",
            spectral_bitrate_kbps=200, spectral_subject="installed",
            spectral_provenance="measured", was_converted_from=None,
            spectral_measurement_version=SPECTRAL_MEASUREMENT_VERSION,
            cliff_hz=21000, codec_family="mp3", ultrasonic_deficit_db=0.5,
        )
    elif shape == "grade_absent":
        # A row with no grade cannot carry any capture fact (validation:
        # "spectral capture facts ... require a spectral grade").
        measurement = AudioQualityMeasurement(
            min_bitrate_kbps=200, avg_bitrate_kbps=200, median_bitrate_kbps=200,
            format="MP3", spectral_grade=None,
        )
    else:  # preserved_source_now
        measurement = AudioQualityMeasurement(
            min_bitrate_kbps=128, avg_bitrate_kbps=130, median_bitrate_kbps=129,
            format="MP3", spectral_grade="genuine",
            spectral_bitrate_kbps=192, spectral_subject="source",
            spectral_provenance="carried", was_converted_from="flac",
            spectral_measurement_version=SPECTRAL_MEASUREMENT_VERSION,
            cliff_hz=22000, codec_family="mp3", ultrasonic_deficit_db=0.2,
        )
    return make_album_quality_evidence(
        mb_release_id=stored.mb_release_id,
        files=stored.files,
        codec=stored.codec,
        container=stored.container,
        storage_format=stored.storage_format,
        measurement=measurement,
        lineage_version=CURRENT_EVIDENCE_LINEAGE_VERSION,
        preserve_spectral_measurement_version=True,
    )


def _we1_spectral_tuple(evidence: AlbumQualityEvidence) -> tuple[object, ...]:
    m = evidence.measurement
    return (
        m.spectral_grade,
        m.spectral_bitrate_kbps,
        m.spectral_subject,
        m.spectral_provenance,
        m.cliff_hz,
        m.codec_family,
        m.ultrasonic_deficit_db,
        m.spectral_measurement_version,
    )


@requires_postgres
class TestSpectralPreservationSqlFakeParity(unittest.TestCase):
    """Issue #1355 WE1: the real SQL decision and the Fake's Python mirror
    of the same policy must agree.

    ``lib.pipeline_db.evidence`` computes the eight-column spectral-tuple
    preserve/replace decision once per statement in a
    ``spectral_write_decision`` CTE; ``tests.fakes.pipeline_db.evidence``
    computes the identical policy independently in Python. Driving both
    real adapters over the same generated stored/incoming world and
    asserting they persist the same spectral tuple is stronger evidence
    than re-deriving the formula a third time inside this test.
    """

    @settings(max_examples=20, deadline=None)
    @example(
        # The A gate: an R19-shaped, current-owned stored tuple survives a
        # replace-intent write carrying a fresh grade (the "dual role"
        # world real-PG pin `test_candidate_attempt_cannot_overwrite_dual_
        # role_source_spectral` already covers deterministically).
        stored_shape="preserved_source", current_owned=True,
        incoming_shape="grade_present", intent="replace",
    )
    @example(
        # Disjunct 2: a non-R19 source measurement stays freely
        # remeasurable (the real-PG pin `test_candidate_attempt_refreshes_
        # current_owned_native_source_spectral`'s world).
        stored_shape="native_source", current_owned=True,
        incoming_shape="grade_present", intent="replace",
    )
    @example(
        # Disjunct 1: a legacy pre-v4 row is rebuilt wholesale even when
        # the incoming writer carries no grade at all.
        stored_shape="legacy_v1", current_owned=False,
        incoming_shape="grade_absent", intent="merge",
    )
    @example(
        # Disjunct 3: a replace-intent write against an R19-shaped stored
        # tuple that is NOT current-owned (a stale candidate-only cache
        # collision) always replaces, R19 shape notwithstanding.
        stored_shape="preserved_source", current_owned=False,
        incoming_shape="grade_absent", intent="replace",
    )
    @example(
        # Disjunct 4: an installed-subject stored tuple is stale by
        # definition once the incoming write is itself the R19-shaped
        # derivative.
        stored_shape="installed_measured", current_owned=True,
        incoming_shape="preserved_source_now", intent="replace",
    )
    @given(
        stored_shape=st.sampled_from((
            "preserved_source", "native_source", "installed_measured",
            "legacy_v1",
        )),
        current_owned=st.booleans(),
        incoming_shape=st.sampled_from((
            "grade_present", "grade_absent", "preserved_source_now",
        )),
        intent=st.sampled_from(("merge", "replace")),
    )
    def test_real_pg_and_fake_agree_on_spectral_tuple(
        self,
        stored_shape: StoredSpectralShape,
        current_owned: bool,
        incoming_shape: IncomingSpectralShape,
        intent: SpectralWriteIntent,
    ) -> None:
        mb_release_id = "we1-spectral-parity"
        stored = _we1_stored_evidence(stored_shape, mb_release_id)
        incoming = _we1_incoming_evidence(incoming_shape, stored)

        # Issue #1214: a real-PG handle constructed inside a @given body
        # must not bind its lifetime to the enclosing test METHOD via
        # ``self.addCleanup`` — Hypothesis re-executes this body once per
        # example, so that would leak one live connection per example
        # until the whole method returns. ``try/finally`` binds it to the
        # EXAMPLE instead, matching
        # ``TestGeneratedConvergencePipelineDB.test_sql_derivation_
        # matches_reference_model``'s own pattern in
        # ``tests/test_convergence_pipeline_db_generated.py``.
        pg = make_db()
        try:
            results: dict[str, AlbumQualityEvidence] = {}
            for name, db in (("pg", pg), ("fake", FakePipelineDB())):
                db.upsert_album_quality_evidence(stored)
                found = db.find_album_quality_evidence(
                    mb_release_id=mb_release_id,
                    snapshot_fingerprint=stored.snapshot_fingerprint,
                )
                assert found is not None and found.id is not None
                if current_owned:
                    request_id = db.add_request(
                        artist_name="WE1 parity",
                        album_title=mb_release_id,
                        source="request",
                    )
                    self.assertTrue(
                        db.set_request_current_evidence(request_id, found.id)
                    )
                db.upsert_album_quality_evidence(
                    incoming, spectral_write_intent=intent,
                )
                after = db.find_album_quality_evidence(
                    mb_release_id=mb_release_id,
                    snapshot_fingerprint=stored.snapshot_fingerprint,
                )
                assert after is not None
                results[name] = after

            self.assertEqual(
                _we1_spectral_tuple(results["pg"]),
                _we1_spectral_tuple(results["fake"]),
            )
        finally:
            pg.close()
