"""AlbumQualityEvidence family + AudioQualityMeasurement (content-addressed evidence Structs).

Extracted verbatim from the monolithic ``lib/quality.py`` (issue #477).
Pure move: every definition is AST-identical to the original.
"""

from datetime import datetime
from typing import Literal, Optional
import msgspec


V0_PROBE_LOSSLESS_SOURCE = "lossless_source_v0"
V0_PROBE_NATIVE_LOSSY_RESEARCH = "native_lossy_research_v0"
V0_PROBE_ON_DISK_RESEARCH = "on_disk_research_v0"
V0_PROBE_KINDS = frozenset({
    V0_PROBE_LOSSLESS_SOURCE,
    V0_PROBE_NATIVE_LOSSY_RESEARCH,
    V0_PROBE_ON_DISK_RESEARCH,
})

EvidenceSubject = Literal["installed", "source"]
EvidenceProvenance = Literal["measured", "carried"]
EVIDENCE_SUBJECT_INSTALLED: EvidenceSubject = "installed"
EVIDENCE_SUBJECT_SOURCE: EvidenceSubject = "source"
EVIDENCE_PROVENANCE_MEASURED: EvidenceProvenance = "measured"
EVIDENCE_PROVENANCE_CARRIED: EvidenceProvenance = "carried"


# ---------------------------------------------------------------------------
# Audio quality measurement — ground truth from ffprobe + spectral
# ---------------------------------------------------------------------------

class AudioQualityMeasurement(msgspec.Struct, frozen=True):
    """What we actually measured about a set of audio files.

    Ground truth from ffprobe and spectral analysis. Used by decision functions
    to compare new downloads against existing files and determine quality gate
    outcomes. Wire-boundary type per ``.claude/rules/code-quality.md`` —
    appears in ``ImportResult.{source_measurement,current_measurement}`` and
    crosses both the harness stdout and ``download_log.import_result`` JSONB
    boundaries.

    Fields:
        min_bitrate_kbps:      min per-track bitrate (kbps), None if unmeasurable
        avg_bitrate_kbps:      mean per-track bitrate (kbps), None if unmeasured.
                               Preferred by the rank model for VBR codecs — see
                               RankBitrateMetric and measurement_rank(). Additive;
                               legacy callers that only populate min_bitrate_kbps
                               still work (measurement_rank() falls back to min).
        median_bitrate_kbps:   median per-track bitrate (kbps), None if
                               unmeasured. Used when
                               RankBitrateMetric.MEDIAN is configured —
                               robust against per-track outliers (intro/outro
                               silence, hidden tracks, very short interludes)
                               that can pull MIN or AVG away from the typical
                               track quality. measurement_rank() falls back
                               to min when this is None.
        format:                measured source/output codec or container label,
                               such as a bare codec string from ffprobe or Beets
                               ("MP3", "Opus", "FLAC", "AAC"). Projected target
                               labels belong in ``TargetQualityContract``.
                               None means the measured codec is unknown.
        is_cbr:                True if all tracks have the same bitrate
        spectral_grade:        spectral analysis result (genuine/marginal/suspect)
        spectral_bitrate_kbps: estimated original bitrate from spectral cliff
        spectral_subject:      bytes the spectral fact describes
        spectral_provenance:   whether the spectral fact was measured or carried
        was_converted_from:    output-only lineage: source format before
                               conversion (flac/m4a/wav). New source
                               measurements leave this None.
    """
    min_bitrate_kbps: Optional[int] = None
    avg_bitrate_kbps: Optional[int] = None
    median_bitrate_kbps: Optional[int] = None
    format: Optional[str] = None
    is_cbr: bool = False
    spectral_grade: Optional[str] = None
    spectral_bitrate_kbps: Optional[int] = None
    spectral_subject: EvidenceSubject | None = None
    spectral_provenance: EvidenceProvenance | None = None
    was_converted_from: Optional[str] = None

    def new_row_validation_errors(
        self,
        *,
        source: bool = False,
        two_axis: bool = True,
    ) -> list[str]:
        """Validate the two-axis measurement shape emitted by v4 writers."""

        errors: list[str] = []
        if self.format is not None:
            label = self.format.strip()
            if not label or len(label.split()) != 1:
                errors.append(
                    "measurement.format must be a bare measured codec label"
                )
        if source and self.was_converted_from is not None:
            errors.append(
                "source measurement must not carry was_converted_from"
            )
        if not two_axis:
            return errors
        if self.spectral_subject not in (
            None,
            EVIDENCE_SUBJECT_INSTALLED,
            EVIDENCE_SUBJECT_SOURCE,
        ):
            errors.append("spectral subject must be installed or source")
        if self.spectral_provenance not in (
            None,
            EVIDENCE_PROVENANCE_MEASURED,
            EVIDENCE_PROVENANCE_CARRIED,
        ):
            errors.append("spectral provenance must be measured or carried")
        if self.spectral_grade is None:
            if self.spectral_bitrate_kbps is not None:
                errors.append(
                    "spectral bitrate requires a spectral grade"
                )
            if self.spectral_subject is not None or self.spectral_provenance is not None:
                errors.append(
                    "spectral markers require a spectral grade"
                )
        elif self.spectral_subject is None or self.spectral_provenance is None:
            errors.append(
                "spectral grade requires subject and provenance"
            )
        if (
            self.spectral_subject == EVIDENCE_SUBJECT_INSTALLED
            and self.spectral_provenance == EVIDENCE_PROVENANCE_CARRIED
        ):
            errors.append("installed spectral evidence cannot be carried")
        return errors


class TargetQualityContract(msgspec.Struct, frozen=True):
    """Configured quality of a projected/materialized target.

    A contract is policy, not a measurement.  Its explicit label drives rank
    classification without borrowing bitrate statistics from the source or a
    temporary V0 probe.
    """

    format: str
    is_cbr: bool

    @classmethod
    def from_explicit_label(
        cls,
        format_hint: str,
    ) -> "TargetQualityContract":
        """Build policy from a self-describing target label.

        Bare ``MP3`` is deliberately rejected because it does not declare CBR
        versus VBR.  Callers with a measured projection must use
        :meth:`from_projection` instead.
        """

        parts = format_hint.strip().lower().split()
        if parts == ["mp3"]:
            raise ValueError(
                "bare MP3 target contract requires a measured projection"
            )
        is_cbr = (
            len(parts) == 2
            and parts[0] == "mp3"
            and parts[1].isdigit()
        )
        return cls(
            format=format_hint,
            is_cbr=is_cbr,
        )

    @classmethod
    def from_projection(
        cls,
        format_hint: str,
        *,
        projected_is_cbr: bool,
    ) -> "TargetQualityContract":
        """Build policy with a required independently measured target mode.

        Bare ``MP3`` consumes the projection.  Explicit labels remain
        authoritative and cannot be contradicted by the measured mode.
        """

        parts = format_hint.strip().lower().split()
        if parts == ["mp3"]:
            return cls(format=format_hint, is_cbr=projected_is_cbr)
        return cls.from_explicit_label(format_hint)


_NONCOMPARABLE_NEUTRAL_V0_PROBE_KIND = "neutral_v0_research"


class AlbumQualityEvidenceFile(msgspec.Struct, frozen=True):
    """One active file-snapshot row used to guard evidence freshness."""

    relative_path: str
    size_bytes: int
    mtime_ns: int
    extension: str
    container: str
    codec: str | None = None
    # decode_ok is per-file evidence that the measurement helper produces:
    # True if ffmpeg returned rc=0 against this file's audio stream, False
    # otherwise. Migration 019 default is TRUE so legacy rows decoded into
    # this Struct shape are non-corrupt by default — the decision function
    # only rejects when at least one file's ``decode_ok`` is False.
    decode_ok: bool = True

    def validation_errors(self) -> list[str]:
        errors: list[str] = []
        if not self.relative_path or self.relative_path.startswith("/"):
            errors.append("relative_path must be a non-empty relative path")
        if not isinstance(self.size_bytes, int) or self.size_bytes < 0:
            errors.append(f"{self.relative_path}: size_bytes must be >= 0")
        if not isinstance(self.mtime_ns, int) or self.mtime_ns < 0:
            errors.append(f"{self.relative_path}: mtime_ns must be >= 0")
        if not self.extension:
            errors.append(f"{self.relative_path}: extension is required")
        if not self.container:
            errors.append(f"{self.relative_path}: container is required")
        return errors


class AlbumQualityV0Metric(msgspec.Struct, frozen=True):
    """Neutral V0 probe metric plus subject and provenance.

    This deliberately does not carry the old policy-shaped probe ``kind``.
    Action code can interpret source/proof provenance later, but the durable
    evidence row remains a neutral measurement.
    """

    subject: EvidenceSubject
    provenance: EvidenceProvenance = EVIDENCE_PROVENANCE_MEASURED
    min_bitrate_kbps: int | None = None
    avg_bitrate_kbps: int | None = None
    median_bitrate_kbps: int | None = None

    def validation_errors(self) -> list[str]:
        errors: list[str] = []
        if self.subject not in (
            EVIDENCE_SUBJECT_INSTALLED,
            EVIDENCE_SUBJECT_SOURCE,
        ):
            errors.append("v0 subject must be installed or source")
        if self.provenance not in (
            EVIDENCE_PROVENANCE_MEASURED,
            EVIDENCE_PROVENANCE_CARRIED,
        ):
            errors.append("v0 provenance must be measured or carried")
        if (
            self.min_bitrate_kbps is None
            and self.avg_bitrate_kbps is None
            and self.median_bitrate_kbps is None
        ):
            errors.append("v0_metric must include at least one bitrate metric")
        if (
            self.subject == EVIDENCE_SUBJECT_INSTALLED
            and self.provenance == EVIDENCE_PROVENANCE_CARRIED
        ):
            errors.append("installed v0 evidence cannot be carried")
        return errors


class VerifiedLosslessProof(msgspec.Struct, frozen=True):
    """Provenance for a true verified-lossless classification."""

    provenance: EvidenceProvenance
    source: str
    classifier: str
    detail: str | None = None

    def validation_errors(self) -> list[str]:
        errors: list[str] = []
        if self.provenance not in (
            EVIDENCE_PROVENANCE_MEASURED,
            EVIDENCE_PROVENANCE_CARRIED,
        ):
            errors.append("verified_lossless provenance must be measured or carried")
        if not self.source:
            errors.append("verified_lossless source is required")
        if not self.classifier:
            errors.append("verified_lossless classifier is required")
        return errors


class AlbumQualityEvidence(msgspec.Struct, frozen=True):
    """Active neutral album-quality evidence for candidates and current files.

    The evidence wraps ``AudioQualityMeasurement`` instead of duplicating its
    policy-facing facts. Snapshot rows and intrinsic provenance live here;
    action provenance such as reused/recomputed/backfilled/fallback outcomes
    belongs to preview/import/cleanup result surfaces, not to this durable row.

    Identity is content-addressed by ``(mb_release_id, snapshot_fingerprint)``
    after migration 021. ``id`` is the surrogate PK populated after upsert.
    Addressing (which entity points at this row) lives on the addressing
    entity: ``import_jobs.candidate_evidence_id``,
    ``download_log.candidate_evidence_id``, ``album_requests.current_evidence_id``.
    """

    mb_release_id: str
    snapshot_fingerprint: str
    source_path: str
    measurement: AudioQualityMeasurement
    measured_at: datetime
    id: int | None = None
    files: list[AlbumQualityEvidenceFile] = msgspec.field(default_factory=list)
    codec: str | None = None
    container: str | None = None
    storage_format: str | None = None
    target_format: str | None = None
    # Album-wide bitrate mode of the projected target/probe.  This is a
    # contract fact, not the downloaded source or materialized-output mode.
    target_is_cbr: bool | None = None
    # Migration 050 marks the interpretation of storage/target fields.
    # Historical rows are v1/v3; every two-axis writer emits v4.
    lineage_version: int = 4
    v0_metric: AlbumQualityV0Metric | None = None
    # Preview-owned, content-snapshot-local idempotence marker. A failed or
    # empty on-disk V0 research probe is still an attempt; import/cleanup
    # consumers never execute the probe and policy never reads this flag.
    on_disk_v0_research_attempted: bool = False
    # A changed installed snapshot is linked before its neutral enrichment
    # completes so the async writers can address the exact new evidence row.
    # This durable bit keeps every action retry fail-closed until the required
    # spectral/V0 facts either survive as source facts or are measured anew.
    current_enrichment_required: bool = False
    verified_lossless_proof: VerifiedLosslessProof | None = None
    # U1 (migration 019) preview-evidence facts. The unified decider
    # ``full_pipeline_decision_from_evidence`` reads these as typed facts
    # via its four-fact early-exit reject branches (U11) — never derives
    # them from snapshot files. SQL defaults (FALSE, 'flat', 0, '') keep
    # legacy rows decoding into a safe shape that the decision function
    # rejects only when explicit reject-shaped facts are present.
    audio_corrupt: bool = False
    # Exact album-level decoder diagnostic. Per-file ``decode_ok`` remains
    # the structured identity of the failed files.
    audio_error: str | None = None
    folder_layout: str = "flat"
    audio_file_count: int = 0
    filetype_band: str = ""
    matched_bad_audio_hash_id: int | None = None
    matched_bad_audio_hash_path: str | None = None

    def sorted_for_storage(self) -> "AlbumQualityEvidence":
        return AlbumQualityEvidence(
            mb_release_id=self.mb_release_id,
            snapshot_fingerprint=self.snapshot_fingerprint,
            source_path=self.source_path,
            measurement=self.measurement,
            measured_at=self.measured_at,
            id=self.id,
            files=sorted(self.files, key=lambda f: f.relative_path),
            codec=self.codec,
            container=self.container,
            storage_format=self.storage_format,
            target_format=self.target_format,
            target_is_cbr=self.target_is_cbr,
            lineage_version=self.lineage_version,
            v0_metric=self.v0_metric,
            on_disk_v0_research_attempted=(
                self.on_disk_v0_research_attempted
            ),
            current_enrichment_required=self.current_enrichment_required,
            verified_lossless_proof=self.verified_lossless_proof,
            audio_corrupt=self.audio_corrupt,
            audio_error=self.audio_error,
            folder_layout=self.folder_layout,
            audio_file_count=self.audio_file_count,
            filetype_band=self.filetype_band,
            matched_bad_audio_hash_id=self.matched_bad_audio_hash_id,
            matched_bad_audio_hash_path=self.matched_bad_audio_hash_path,
        )

    def storage_validation_errors(self) -> list[str]:
        errors: list[str] = []
        if not self.mb_release_id:
            errors.append("mb_release_id must be a non-empty string")
        if not self.snapshot_fingerprint:
            errors.append("snapshot_fingerprint must be a non-empty string")
        if self.measured_at is None:
            errors.append("measured_at is required")
        if self.lineage_version not in (1, 3, 4):
            errors.append("lineage_version must be 1, 3, or 4")
        if self.lineage_version >= 3:
            errors.extend(self.measurement.new_row_validation_errors(
                two_axis=self.lineage_version == 4,
            ))
            if (self.target_format is None) != (self.target_is_cbr is None):
                errors.append(
                    "target_format and target_is_cbr must be set together"
                )
            if self.storage_format is not None:
                storage_label = self.storage_format.strip()
                if not storage_label or len(storage_label.split()) != 1:
                    errors.append(
                        "storage_format must be a bare measured codec label"
                    )
                measurement_label = (
                    self.measurement.format.strip().lower()
                    if self.measurement.format is not None
                    else None
                )
                if (
                    measurement_label is not None
                    and storage_label.lower() != measurement_label
                ):
                    errors.append(
                        "storage_format must match measurement.format"
                    )
        # Empty snapshot is a storable fact ONLY when audio_file_count=0
        # (the explicit empty-inventory signal). When a fileset is present
        # but ``files`` is empty, the evidence row is incomplete.
        if not self.files and self.audio_file_count != 0:
            errors.append("at least one snapshot file is required")
        if self.folder_layout not in ("flat", "nested"):
            errors.append(
                f"folder_layout must be 'flat' or 'nested': {self.folder_layout!r}"
            )
        if not isinstance(self.audio_file_count, int) or self.audio_file_count < 0:
            errors.append("audio_file_count must be >= 0")
        if (self.matched_bad_audio_hash_id is None) != (
            self.matched_bad_audio_hash_path is None
        ):
            errors.append(
                "matched_bad_audio_hash_id and matched_bad_audio_hash_path "
                "must be set together or both NULL"
            )
        relative_paths: set[str] = set()
        for file in self.files:
            errors.extend(file.validation_errors())
            if file.relative_path in relative_paths:
                errors.append(
                    f"duplicate snapshot relative_path: {file.relative_path}"
                )
            relative_paths.add(file.relative_path)
        if self.lineage_version == 4:
            if self.v0_metric is not None:
                errors.extend(self.v0_metric.validation_errors())
        if self.lineage_version == 4 and self.verified_lossless_proof is not None:
            errors.extend(self.verified_lossless_proof.validation_errors())
        return errors

    def policy_incomplete_reasons(self) -> list[str]:
        """Return reasons this row is not ready for action reducers."""

        reasons = self.storage_validation_errors()
        if not self.source_path.strip():
            # A row without a recorded path can never be re-verified against
            # disk nor completed by HAVE enrichment (every persist guard
            # compares the scanned path against ``source_path``), so it must
            # be rebuilt rather than used as decision authority
            # (download_log 37206: a blank-path legacy backfill kept the
            # French Quarter import spectrally blind forever).
            reasons.append("source_path is required")
        if self.measurement.format is None:
            reasons.append("measurement.format is required")
        if (
            self.measurement.min_bitrate_kbps is None
            and self.measurement.avg_bitrate_kbps is None
            and self.measurement.median_bitrate_kbps is None
        ):
            reasons.append("at least one measurement bitrate metric is required")
        return reasons


class V0ProbeEvidence(msgspec.Struct, frozen=True):
    """MP3 V0 probe metrics used as source-lineage evidence.

    ``kind`` is intentionally explicit because not every V0 probe is eligible
    for policy decisions. Only ``lossless_source_v0`` proves the candidate came
    from a supported lossless-container source. Native-lossy and on-disk probes
    are research evidence in v1.
    """

    kind: str = ""
    min_bitrate_kbps: Optional[int] = None
    avg_bitrate_kbps: Optional[int] = None
    median_bitrate_kbps: Optional[int] = None


def is_comparable_lossless_source_probe(
    probe: V0ProbeEvidence | None,
) -> bool:
    return (
        probe is not None
        and probe.kind == V0_PROBE_LOSSLESS_SOURCE
        and probe.avg_bitrate_kbps is not None
    )


class QualityComparisonBasis(msgspec.Struct, frozen=True):
    """The comparison ``compare_quality()`` actually performed — persisted so
    the UI renders the decision's own story instead of re-deriving one.

    Emitted per-branch from inside ``compare_quality()``: the branch tag names
    which rule fired, and ``new_value_kbps`` / ``existing_value_kbps`` are the
    numbers that DECIDED that branch (spectral-clamped values on a clamped
    rank comparison, raw configured-metric values on a same-rank tiebreak).
    Consumers reading ``(metric, value)`` pairs must suppress the metric
    label when ``branch == "rank" and spectral_clamped`` — the value there
    is ``min(metric, spectral floor)``, not the named statistic.
    ``new_metric`` / ``existing_metric`` name the per-side statistic actually
    classified — ``measurement_rank()`` falls back to min when the configured
    metric is unmeasured, and a basis claiming "avg" for a min value would be
    the same class of display lie this type exists to kill (request 6039:
    a genuine avg-196→288 rank upgrade rendered as "MP3 V2 to MP3 V2"
    because every UI label re-derived from min bitrate).
    An explicit codec label uses ``contract`` instead: its declared bitrate is
    policy, not a measured statistic and especially not a temporary V0 probe.

    ``verified_lossless_bypass`` is set by ``import_quality_decision()``, not
    ``compare_quality()`` — True only when the bypass CHANGED the outcome
    (an "equivalent" verdict imported because the source was verified
    lossless), never merely because the flag was present.

    Wire-boundary type per ``.claude/rules/code-quality.md`` — crosses the
    harness stdout and ``download_log.import_result`` JSONB boundaries inside
    ``ImportResult``. Optional there; rows predating the field decode as None
    and the UI falls back to the legacy min-based labels.
    """

    verdict: str  # "better" | "worse" | "equivalent"
    branch: str   # see COMPARISON_BASIS_BRANCHES
    new_rank: str
    existing_rank: str
    new_metric: str = "min"        # "min" | "avg" | "median" | "contract"
    existing_metric: str = "min"
    new_value_kbps: Optional[int] = None
    existing_value_kbps: Optional[int] = None
    new_format: Optional[str] = None
    existing_format: Optional[str] = None
    spectral_clamped: bool = False
    tolerance_kbps: Optional[int] = None
    verified_lossless_bypass: bool = False


COMPARISON_BASIS_BRANCHES: frozenset[str] = frozenset({
    "rank",                        # ranks differ — the primary key decided
    "lossless_same_rank",          # both LOSSLESS: equivalent by identity
    "cross_family_same_rank",      # same rank, different codec family
    "label_contract_same_rank",    # same rank, explicit label is authoritative
    "metric_tiebreak",             # same rank, raw metric delta vs tolerance
    "metric_missing",              # same rank, a side has no classifiable value
    "transcode_rank_regression",   # transcode-grade candidate regresses real rank
})
"""Every branch tag ``compare_quality()`` may emit. The generated
basis-consistency property patrols this taxonomy against the decision."""


SPECTRAL_TRANSCODE_GRADES: frozenset[str] = frozenset({"suspect", "likely_transcode"})
"""Spectral grades that authorize the spectral bitrate as an override input.

Only these grades mean "this is a transcode and the spectral cliff is a
legitimate low-bound on original quality". Genuine/marginal/error/None/unknown
grades must leave the container bitrate untouched — a genuine lo-fi file
(e.g. Mountain Goats boombox) can produce a low spectral cliff estimate that
is NOT a quality signal and would falsely drag the import comparison down.
See issue #61 for the motivating incident.
"""
