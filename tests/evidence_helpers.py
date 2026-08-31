"""Canonical builders for ``AlbumQualityEvidence``-family test rows.

Split out of ``tests/helpers.py`` (issue #1278, "worth exploring" item 5):
everything here builds the content-addressed quality-evidence shapes the
decision pipeline consumes — production-shaped evidence rows, corrupt-audio
reports, AAC-lattice captures, and the two parity-world builders shared by
the hand-written parity tests and the generated parity property.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, datetime

import msgspec

from lib.quality import (
    CURRENT_EVIDENCE_LINEAGE_VERSION,
    DECISION_LOSSLESS_SOURCE_LOCKED,
    DECISION_PROVISIONAL_LOSSLESS_UPGRADE,
    DECISION_SUSPECT_LOSSLESS_DOWNGRADE,
    DECISION_SUSPECT_LOSSLESS_PROBE_MISSING,
    EVIDENCE_PROVENANCE_CARRIED,
    EVIDENCE_PROVENANCE_MEASURED,
    EVIDENCE_SUBJECT_INSTALLED,
    EVIDENCE_SUBJECT_SOURCE,
    AacLatticeCapture,
    AlbumQualityEvidence,
    AlbumQualityEvidenceFile,
    AlbumQualityV0Metric,
    AudioQualityMeasurement,
    AudioToolDiagnostic,
    AudioValidationReport,
    CdRipBitVerification,
    CodecFamily,
    TargetQualityContract,
    VerifiedLosslessProof,
    legacy_unrecorded_audio_validation_report,
)
from lib.quality_evidence import snapshot_fingerprint


def make_album_quality_evidence(
    *,
    mb_release_id: str = "test-mbid-0001",
    source_path: str = "/tmp/test-staged",
    measured_at: datetime | None = None,
    files: list[AlbumQualityEvidenceFile] | None = None,
    measurement: AudioQualityMeasurement | None = None,
    v0_metric: AlbumQualityV0Metric | None = None,
    verified_lossless_proof: VerifiedLosslessProof | None = None,
    cd_rip_verification: CdRipBitVerification | None = None,
    codec: str | None = "mp3",
    container: str | None = "mp3",
    storage_format: str | None = "MP3",
    target_format: str | None = None,
    target_is_cbr: bool | None = None,
    lineage_version: int = CURRENT_EVIDENCE_LINEAGE_VERSION,
    on_disk_v0_research_attempted: bool = False,
    current_enrichment_required: bool = False,
    preserve_spectral_measurement_version: bool = False,
    audio_corrupt: bool = False,
    audio_error: str | None = None,
    audio_validation: AudioValidationReport | None = None,
    aac_lattice: AacLatticeCapture | None = None,
) -> AlbumQualityEvidence:
    """Build production-shaped active album-quality evidence.

    Migration 021: evidence is content-addressed by
    ``(mb_release_id, snapshot_fingerprint)``. The fingerprint is computed
    from ``files`` using the canonical helper, so the builder always
    produces a self-consistent row.
    """
    if measured_at is None:
        measured_at = datetime(2026, 1, 1, 0, 0, 0, tzinfo=UTC)
    if files is None:
        files = [
            AlbumQualityEvidenceFile(
                relative_path="01 - Track.mp3",
                size_bytes=123456,
                mtime_ns=1_700_000_000_000_000_000,
                extension="mp3",
                container="mp3",
                codec="mp3",
            ),
        ]
    if measurement is None:
        measurement = AudioQualityMeasurement(
            min_bitrate_kbps=245,
            avg_bitrate_kbps=256,
            median_bitrate_kbps=252,
            format="MP3",
            spectral_grade="genuine",
            spectral_bitrate_kbps=None,
        )
    if (
        lineage_version >= 4
        and measurement.spectral_grade is not None
        and measurement.spectral_subject is None
    ):
        measurement = msgspec.structs.replace(
            measurement,
            spectral_subject=EVIDENCE_SUBJECT_INSTALLED,
            spectral_provenance=EVIDENCE_PROVENANCE_MEASURED,
        )
    if (
        lineage_version >= 4
        and measurement.spectral_grade is not None
        and measurement.spectral_measurement_version is None
        and not preserve_spectral_measurement_version
    ):
        from lib.spectral_check import SPECTRAL_MEASUREMENT_VERSION

        # This helper builds active production-shaped evidence by default.
        # Tests of legacy generations must opt in explicitly so an accidental
        # unstamped fixture cannot masquerade as reusable current evidence.
        measurement = msgspec.structs.replace(
            measurement,
            spectral_measurement_version=SPECTRAL_MEASUREMENT_VERSION,
        )
    if audio_validation is None and audio_corrupt:
        audio_validation = make_audio_corrupt_validation_report(
            files[0].relative_path if files else "",
            detail=audio_error or "synthetic decode failure",
            files_checked=len(files),
        )
        if files:
            files = [
                msgspec.structs.replace(file, decode_ok=index != 0)
                for index, file in enumerate(files)
            ]
    return AlbumQualityEvidence(
        mb_release_id=mb_release_id,
        snapshot_fingerprint=snapshot_fingerprint(files),
        source_path=source_path,
        measurement=measurement,
        measured_at=measured_at,
        files=files,
        codec=codec,
        container=container,
        storage_format=storage_format,
        target_format=target_format,
        target_is_cbr=(
            target_is_cbr
            if target_is_cbr is not None
            else (
                TargetQualityContract.from_explicit_label(target_format).is_cbr
                if target_format is not None
                else None
            )
        ),
        lineage_version=lineage_version,
        v0_metric=v0_metric,
        on_disk_v0_research_attempted=on_disk_v0_research_attempted,
        current_enrichment_required=current_enrichment_required,
        verified_lossless_proof=verified_lossless_proof,
        cd_rip_verification=cd_rip_verification,
        audio_validation=(
            audio_validation
            if audio_validation is not None
            else legacy_unrecorded_audio_validation_report()
        ),
        audio_corrupt=audio_corrupt,
        audio_error=audio_error,
        aac_lattice=aac_lattice,
    )


def make_audio_corrupt_validation_report(
    relative_path: str,
    *,
    detail: str = "synthetic decode failure",
    return_code: int = 69,
    files_checked: int = 1,
) -> AudioValidationReport:
    """Build one production-shaped corrupt-audio report for tests."""
    return AudioValidationReport(
        outcome="audio_corrupt",
        files_checked=files_checked,
        files_failed=1,
        diagnostics=[
            AudioToolDiagnostic(
                relative_path=relative_path,
                category="decode_error",
                return_code=return_code,
                stderr_excerpt=detail,
            ),
        ],
    )


#: Every ``stage2_import`` decision produced by the provisional-lossless
#: lane (``lib/quality/decisions.py::provisional_lossless_decision``) — the
#: lane the V0 trust override routes an album AROUND. Membership is the
#: observable answer to "which lane decided this album", which the v3
#: ultrasonic leg must never change (issue #829 Phase 5 PR3): three of the
#: four are confident rejects that also denylist the offering peer, so a
#: leg that could re-route into this lane would turn a withheld proof into
#: a discarded album. Spelled from the production constants, once, for the
#: pins in ``tests/test_quality_classification.py`` and the property in
#: ``tests/test_quality_generated.py``.
PROVISIONAL_LANE_DECISIONS = frozenset({
    DECISION_PROVISIONAL_LOSSLESS_UPGRADE,
    DECISION_SUSPECT_LOSSLESS_DOWNGRADE,
    DECISION_SUSPECT_LOSSLESS_PROBE_MISSING,
    DECISION_LOSSLESS_SOURCE_LOCKED,
})


def make_aac_lattice_capture(
    tracks: Sequence[tuple[int | None, float | None]],
    *,
    proba: float = 0.12,
    error: str = "AacLatticeUnsupportedRateError: unsupported sample rate 96 kHz",
) -> AacLatticeCapture:
    """Build an AAC-lattice capture through the PRODUCTION derivation.

    ``tracks`` is one ``(offset, z)`` pair per track, in the deterministic
    filename order ``lib/aac_lattice.py`` scores them in; a pair whose
    offset is ``None`` records a per-track detector failure instead of a
    score, exactly as ``measure_album_aac_lattice`` does.

    The album statistics (``modal_offset``/``modal_count``/
    ``scored_tracks``/``max_z``) are ALWAYS derived by
    ``AacLatticeCapture.from_tracks`` — the one function production uses —
    never written by hand. A test that hand-set them could assert on an
    album statistic no measurement can produce (test-fidelity.md Rule C).
    """
    from lib.quality import AacLatticeTrackScore

    rows: list[AacLatticeTrackScore] = []
    for index, (offset, z) in enumerate(tracks):
        filename = f"{index + 1:02d}.flac"
        if offset is None:
            rows.append(AacLatticeTrackScore(filename=filename, error=error))
            continue
        rows.append(AacLatticeTrackScore(
            filename=filename, offset=offset, z=z, proba=proba,
        ))
    return AacLatticeCapture.from_tracks(rows)


def build_parity_candidate_evidence(
    *,
    is_flac: bool,
    min_bitrate: int,
    is_cbr: bool,
    avg_bitrate: int | None = None,
    spectral_grade: str | None = None,
    spectral_bitrate: int | None = None,
    candidate_v0_probe_avg: int | None = None,
    candidate_v0_probe_min: int | None = None,
    native_codec: str = "mp3",
    native_format: str = "MP3",
    mb_release_id: str = "mbid-parity-candidate",
    audio_corrupt: bool = False,
    folder_layout: str = "flat",
    audio_file_count: int | None = None,
    matched_bad_audio_hash_id: int | None = None,
    matched_bad_audio_hash_path: str | None = None,
    snapshot_fingerprint: str = "sha256:candidate-fingerprint",
    cliff_hz: int | None = None,
    codec_family: CodecFamily | None = None,
    filetype_band: str | None = None,
    ultrasonic_deficit_db: float | None = None,
    spectral_measurement_version: int | None = 2,
    was_converted_from: str | None = None,
    lossless_container: str = "flac",
    lossless_codec: str = "flac",
    aac_lattice: AacLatticeCapture | None = None,
) -> AlbumQualityEvidence:
    """Build an ``AlbumQualityEvidence`` candidate row matching the
    simulator's flat-kwargs shape (post-U2/U3 schema).

    This is the canonical simulator-world → evidence-row mapping. The
    hand-written parity tests in ``tests/test_quality_classification.py``
    and the generated parity property in ``tests/test_quality_generated.py``
    both consume it, so a divergence between the decision twins can never
    hide behind two different world encodings.

    Deliberately NO ``post_conversion_min_bitrate`` parameter (#1278
    helpers-split residual 2): the post-conversion projection is never
    evidence-row data — on the evidence side it travels through
    ``AlbumQualityEvidenceDecisionFacts.post_conversion_min_bitrate`` (or
    derives from ``candidate_v0_probe_min`` when the facts leave it
    None). The builder used to accept and silently discard it — a
    misleading channel, now a loud TypeError.

    Honestly stated: this builder does NOT enforce world equality with a
    simulator twin. A parity site that gives the simulator a
    post-conversion value must itself convey it on the evidence side
    (facts, or the V0 probe min) — several historical twins never did
    and agree on outcome anyway (e.g. the mountain-goats-bride pair:
    simulator gets ``post_conversion_min_bitrate=214``, evidence side
    derives None). Tightening those worlds is quality-core parity work,
    not this helper's job.
    """
    # Candidate evidence always describes the downloaded source bytes.
    # Conversion policy/output stay on the target contract and decision facts;
    # a temporary V0 probe must never make a FLAC source wear an MP3 label.
    # The two lossless branches (converting and kept-on-disk) built the
    # identical row and were only ever spelled apart; PR3's
    # container/codec split made that literally true, so they are one.
    if is_flac:
        container = lossless_container
        codec = lossless_codec
        storage_format = lossless_codec
        measurement = AudioQualityMeasurement(
            min_bitrate_kbps=min_bitrate or 900,
            avg_bitrate_kbps=min_bitrate or 900,
            median_bitrate_kbps=min_bitrate or 900,
            format=lossless_codec.upper(),
            is_cbr=False,
            spectral_grade=spectral_grade,
            spectral_bitrate_kbps=spectral_bitrate,
            spectral_subject=(
                EVIDENCE_SUBJECT_SOURCE if spectral_grade is not None else None
            ),
            spectral_provenance=(
                EVIDENCE_PROVENANCE_MEASURED
                if spectral_grade is not None else None
            ),
        )
    else:
        container = codec = native_codec
        storage_format = native_format.lower()
        _avg = avg_bitrate if avg_bitrate is not None else min_bitrate
        measurement = AudioQualityMeasurement(
            min_bitrate_kbps=min_bitrate,
            avg_bitrate_kbps=_avg,
            median_bitrate_kbps=_avg,
            format=native_format,
            is_cbr=is_cbr,
            spectral_grade=spectral_grade,
            spectral_bitrate_kbps=spectral_bitrate,
            spectral_subject=(
                EVIDENCE_SUBJECT_SOURCE if spectral_grade is not None else None
            ),
            spectral_provenance=(
                EVIDENCE_PROVENANCE_MEASURED
                if spectral_grade is not None else None
            ),
        )

    # issue #829 Phase 5 PR1 capture. Stamped after the branch above so all
    # three candidate shapes carry it identically; a row with no spectral
    # grade may not carry these facts at all (evidence-row validation).
    if spectral_grade is not None and (
        cliff_hz is not None
        or codec_family is not None
        or ultrasonic_deficit_db is not None
        or was_converted_from is not None
    ):
        # ``spectral_measurement_version`` defaults to 2 (the PR1+ capture
        # code) because that is what any producer of these facts stamps.
        # A caller pins it to None to build the legacy world explicitly.
        measurement = msgspec.structs.replace(
            measurement,
            cliff_hz=cliff_hz,
            codec_family=codec_family,
            ultrasonic_deficit_db=ultrasonic_deficit_db,
            was_converted_from=was_converted_from,
            spectral_measurement_version=spectral_measurement_version,
        )

    v0_metric = None
    if candidate_v0_probe_avg is not None or candidate_v0_probe_min is not None:
        v0_metric = AlbumQualityV0Metric(
            min_bitrate_kbps=candidate_v0_probe_min,
            avg_bitrate_kbps=candidate_v0_probe_avg,
            median_bitrate_kbps=candidate_v0_probe_avg,
            subject=EVIDENCE_SUBJECT_SOURCE,
            provenance=EVIDENCE_PROVENANCE_MEASURED,
        )

    files = [AlbumQualityEvidenceFile(
        relative_path=f"01.{container}",
        size_bytes=1, mtime_ns=1,
        extension=container, container=container, codec=codec,
    )]
    audio_validation = legacy_unrecorded_audio_validation_report()
    if audio_corrupt:
        files = [msgspec.structs.replace(files[0], decode_ok=False)]
        audio_validation = make_audio_corrupt_validation_report(
            files[0].relative_path,
        )
    # ``audio_file_count`` defaults to len(files) for the standard
    # parity scenarios. Tests covering empty_fileset explicitly pass
    # ``audio_file_count=0`` and override ``files`` separately.
    return AlbumQualityEvidence(
        mb_release_id=mb_release_id,
        snapshot_fingerprint=snapshot_fingerprint,
        source_path="/Incoming/auto-import/candidate",
        measurement=measurement,
        measured_at=datetime(2026, 5, 16, tzinfo=UTC),
        files=files,
        codec=codec,
        container=container,
        storage_format=storage_format,
        v0_metric=v0_metric,
        audio_validation=audio_validation,
        audio_corrupt=audio_corrupt,
        folder_layout=folder_layout,
        audio_file_count=(
            audio_file_count if audio_file_count is not None else len(files)
        ),
        filetype_band=(
            filetype_band if filetype_band is not None else storage_format
        ),
        matched_bad_audio_hash_id=matched_bad_audio_hash_id,
        matched_bad_audio_hash_path=matched_bad_audio_hash_path,
        aac_lattice=aac_lattice,
    )


def build_parity_current_evidence(
    *,
    min_bitrate: int | None,
    avg_bitrate: int | None = None,
    format: str = "MP3",
    is_cbr: bool = False,
    spectral_grade: str | None = None,
    spectral_bitrate: int | None = None,
    mb_release_id: str = "mbid-parity-candidate",
    v0_metric: AlbumQualityV0Metric | None = None,
    matched_bad_audio_hash_id: int | None = None,
    matched_bad_audio_hash_path: str | None = None,
    cliff_hz: int | None = None,
    codec_family: CodecFamily | None = None,
    filetype_band: str | None = None,
    was_converted_from: str | None = None,
) -> AlbumQualityEvidence | None:
    """Build the existing-album evidence row for parity scenarios.

    Returns ``None`` when ``min_bitrate`` is ``None`` — the fresh-request
    shape where no current album exists.

    ``was_converted_from`` builds the R19 converted-lineage shape (issue
    #1204 defect 1's amended invariant): a row whose ``format`` names the
    on-disk DERIVATIVE (e.g. an MP3 converted from FLAC) but whose spectral
    facts describe the pre-conversion SOURCE. Matching production
    (``resolve_measured_codec_family``'s ``converted`` branch requires
    BOTH), setting it also switches ``spectral_subject`` from the ordinary
    installed-row default to ``EVIDENCE_SUBJECT_SOURCE`` and
    ``spectral_provenance`` to ``EVIDENCE_PROVENANCE_CARRIED`` — the
    overwhelming majority live shape for this R19 cohort specifically
    (15,333 of 15,368 rows; this is NOT the same population as the
    general verified-lossless-proof carried-provenance count elsewhere
    in this file).
    """
    if min_bitrate is None:
        return None

    container = format.lower().split()[0]
    files = [AlbumQualityEvidenceFile(
        relative_path=f"01.{container}",
        size_bytes=1, mtime_ns=1,
        extension=container, container=container, codec=container,
    )]
    return AlbumQualityEvidence(
        mb_release_id=mb_release_id,
        snapshot_fingerprint="sha256:current-fingerprint",
        source_path="/Beets/current",
        measurement=AudioQualityMeasurement(
            min_bitrate_kbps=min_bitrate,
            avg_bitrate_kbps=avg_bitrate if avg_bitrate is not None else min_bitrate,
            median_bitrate_kbps=avg_bitrate if avg_bitrate is not None else min_bitrate,
            format=format,
            is_cbr=is_cbr,
            spectral_grade=spectral_grade,
            spectral_bitrate_kbps=spectral_bitrate,
            spectral_subject=(
                (
                    EVIDENCE_SUBJECT_SOURCE
                    if was_converted_from is not None
                    else EVIDENCE_SUBJECT_INSTALLED
                )
                if spectral_grade is not None
                else None
            ),
            spectral_provenance=(
                (
                    EVIDENCE_PROVENANCE_CARRIED
                    if was_converted_from is not None
                    else EVIDENCE_PROVENANCE_MEASURED
                )
                if spectral_grade is not None else None
            ),
            cliff_hz=cliff_hz if spectral_grade is not None else None,
            codec_family=codec_family if spectral_grade is not None else None,
            was_converted_from=(
                was_converted_from if spectral_grade is not None else None
            ),
            spectral_measurement_version=(
                2
                if spectral_grade is not None
                and (cliff_hz is not None or codec_family is not None)
                else None
            ),
        ),
        measured_at=datetime(2026, 5, 16, tzinfo=UTC),
        files=files,
        codec=container,
        container=container,
        storage_format=format.lower(),
        audio_file_count=len(files),
        filetype_band=(
            filetype_band if filetype_band is not None else format.lower()
        ),
        v0_metric=v0_metric,
        matched_bad_audio_hash_id=matched_bad_audio_hash_id,
        matched_bad_audio_hash_path=matched_bad_audio_hash_path,
    )
