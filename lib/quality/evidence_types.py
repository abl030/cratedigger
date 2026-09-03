"""AlbumQualityEvidence family + AudioQualityMeasurement (content-addressed evidence Structs).

Extracted verbatim from the monolithic ``lib/quality.py`` (issue #477).
Pure move: every definition is AST-identical to the original.
"""

import re
from datetime import datetime
from typing import Literal, Self

import msgspec

from lib.quality.audio_validation import (
    AudioValidationReport,
    legacy_unrecorded_audio_validation_report,
    validate_audio_validation_report,
)

V0_PROBE_LOSSLESS_SOURCE = "lossless_source_v0"
V0_PROBE_NATIVE_LOSSY_RESEARCH = "native_lossy_research_v0"
V0_PROBE_ON_DISK_RESEARCH = "on_disk_research_v0"

EvidenceSubject = Literal["installed", "source"]
EvidenceProvenance = Literal["measured", "carried"]
EVIDENCE_SUBJECT_INSTALLED: EvidenceSubject = "installed"
EVIDENCE_SUBJECT_SOURCE: EvidenceSubject = "source"
EVIDENCE_PROVENANCE_MEASURED: EvidenceProvenance = "measured"
EVIDENCE_PROVENANCE_CARRIED: EvidenceProvenance = "carried"

# A CD database match proves source-bit identity, not merely the absence of a
# lossy spectral signature.  Keep its classifier distinct from every spectral
# generation so persisted proof rows remain self-describing.
CD_RIP_BIT_VERIFIED_CLASSIFIER = "cd_rip_bit_verified_v1"

# issue #829 Phase 5 PR1 — the six measured codec families
# ``AudioQualityMeasurement.codec_family`` is restricted to (mirrored by
# migration 065's CHECK constraint). A Literal here, not a bare ``str``,
# so msgspec catches a drifted value at the wire boundary (harness stdout,
# JSONB) instead of psycopg2 raising a CheckViolation 500 on write.
CodecFamily = Literal["mp3", "aac", "opus", "vorbis", "lossless", "other"]
CODEC_FAMILY_MP3: CodecFamily = "mp3"
CODEC_FAMILY_AAC: CodecFamily = "aac"
CODEC_FAMILY_OPUS: CodecFamily = "opus"
CODEC_FAMILY_VORBIS: CodecFamily = "vorbis"
CODEC_FAMILY_LOSSLESS: CodecFamily = "lossless"
CODEC_FAMILY_OTHER: CodecFamily = "other"


# ---------------------------------------------------------------------------
# Audio quality measurement — ground truth from ffprobe + spectral
# ---------------------------------------------------------------------------

class AudioQualityMeasurement(
    msgspec.Struct, frozen=True, forbid_unknown_fields=True
):
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
    min_bitrate_kbps: int | None = None
    avg_bitrate_kbps: int | None = None
    median_bitrate_kbps: int | None = None
    format: str | None = None
    is_cbr: bool = False
    spectral_grade: str | None = None
    spectral_bitrate_kbps: int | None = None
    spectral_subject: EvidenceSubject | None = None
    spectral_provenance: EvidenceProvenance | None = None
    was_converted_from: str | None = None
    # issue #829 Phase 5 PR1 — measured facts captured alongside the
    # spectral tuple above (same subject/provenance, same measurement
    # pass). Pure passengers: no decision reads them in this PR.
    #   cliff_hz:                 raw in-window cliff frequency (Hz) —
    #                              exactly what detect_cliff() returns,
    #                              vs. spectral_bitrate_kbps's bucketed
    #                              interpretation of the same fact.
    #   codec_family:              mp3/aac/opus/vorbis/lossless/other.
    #   ultrasonic_deficit_db:     level-invariant ultrasonic deficit
    #                              (PR3's proof-leg statistic).
    #   spectral_measurement_version: 2 for rows measured by the PR1+
    #                              spectral_check code; None for legacy
    #                              rows (forward-only, no backfill).
    cliff_hz: int | None = None
    codec_family: CodecFamily | None = None
    ultrasonic_deficit_db: float | None = None
    spectral_measurement_version: int | None = None

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
            # issue #829 Phase 5 PR1: cliff_hz/codec_family/
            # ultrasonic_deficit_db/spectral_measurement_version are
            # measured in the SAME pass as spectral_grade — a row with no
            # grade cannot legitimately carry any of them. Note this is
            # deliberately one-directional: a spectral_grade WITHOUT these
            # four fields stays valid (every pre-PR1 legacy row, forward-
            # only per scope.md).
            if (
                self.cliff_hz is not None
                or self.codec_family is not None
                or self.ultrasonic_deficit_db is not None
                or self.spectral_measurement_version is not None
            ):
                errors.append(
                    "spectral capture facts (cliff_hz/codec_family/"
                    "ultrasonic_deficit_db/spectral_measurement_version) "
                    "require a spectral grade"
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


class TargetQualityContract(
    msgspec.Struct, frozen=True, forbid_unknown_fields=True
):
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
    ) -> Self:
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
    ) -> Self:
        """Build policy with a required independently measured target mode.

        Bare ``MP3`` consumes the projection.  Explicit labels remain
        authoritative and cannot be contradicted by the measured mode.
        """

        parts = format_hint.strip().lower().split()
        if parts == ["mp3"]:
            return cls(format=format_hint, is_cbr=projected_is_cbr)
        return cls.from_explicit_label(format_hint)


_NONCOMPARABLE_NEUTRAL_V0_PROBE_KIND = "neutral_v0_research"


class AlbumQualityEvidenceFile(
    msgspec.Struct, frozen=True, forbid_unknown_fields=True
):
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
        if self.size_bytes < 0:
            errors.append(f"{self.relative_path}: size_bytes must be >= 0")
        if self.mtime_ns < 0:
            errors.append(f"{self.relative_path}: mtime_ns must be >= 0")
        if not self.extension:
            errors.append(f"{self.relative_path}: extension is required")
        if not self.container:
            errors.append(f"{self.relative_path}: container is required")
        return errors


class AlbumQualityV0Metric(
    msgspec.Struct, frozen=True, forbid_unknown_fields=True
):
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


class AacLatticeTrackScore(
    msgspec.Struct, frozen=True, forbid_unknown_fields=True
):
    """One track's AAC MDCT-frame-lattice measurement, or why it has none.

    Wire-boundary type: the rows of the
    ``album_quality_evidence.aac_lattice_tracks`` JSONB array (issue #829
    AAC-lattice leg PR-A). Exactly one of the two shapes is legal — a scored
    triple, or an ``error`` string. A failure is EVIDENCE, not an absence:
    96 kHz input has no scalefactor-band table at all, and recording that is
    how the operator can tell "no lattice found" from "never looked".

    ``offset`` is the argmax MDCT frame offset in samples (0-1023), ``z`` the
    contrast of the sweep's peak against its own median, ``proba`` the
    detector statistic at that peak. The detector, and the measured meaning
    of all three, live in ``lib/aac_lattice.py``.
    """

    filename: str
    offset: int | None = None
    z: float | None = None
    proba: float | None = None
    error: str | None = None

    def validation_errors(self) -> list[str]:
        errors: list[str] = []
        if not self.filename:
            errors.append("aac lattice track filename is required")
        scored = (self.offset, self.z, self.proba)
        if self.error is None:
            if any(value is None for value in scored):
                errors.append(
                    f"{self.filename}: a scored lattice track needs "
                    "offset, z and proba"
                )
            elif not 0 <= (self.offset or 0) < 1024:
                errors.append(
                    f"{self.filename}: lattice offset must be within 0-1023"
                )
        elif any(value is not None for value in scored):
            errors.append(
                f"{self.filename}: a failed lattice track carries no statistics"
            )
        return errors


class AacLatticeCapture(
    msgspec.Struct, frozen=True, forbid_unknown_fields=True
):
    """One album's AAC-lattice measurement: per-track rows plus the album
    statistics the offset-concentration rule reads.

    Persisted across five ``album_quality_evidence`` columns (``tracks`` as
    JSONB, the rest as scalars so SQL can aggregate them). The whole capture
    is NULL for a row that was never measured, which is distinct from a row
    measured with ``scored_tracks = 0``.

    ``modal_offset``/``modal_count`` are the album's most-repeated recovered
    offset and how many tracks recovered it — the parameter-free statistic
    behind "k >= 4 tracks share one MDCT frame offset"
    (``docs/research/calibration-data/derrien-refinement/README.md``). PR-A
    only captures them; no decision reads this type.
    """

    tracks: list[AacLatticeTrackScore] = msgspec.field(
        default_factory=list[AacLatticeTrackScore]
    )
    modal_offset: int | None = None
    modal_count: int | None = None
    scored_tracks: int = 0
    max_z: float | None = None

    @classmethod
    def from_tracks(
        cls,
        tracks: "list[AacLatticeTrackScore]",
    ) -> "AacLatticeCapture":
        """Derive the album statistics from per-track rows.

        Ties on the modal offset break to the LOWEST offset so the same
        track population always yields the same album statistic — the
        measurement must be a function of the audio, not of dict ordering.
        """
        scored = [
            track for track in tracks
            if track.error is None and track.offset is not None
        ]
        counts: dict[int, int] = {}
        for track in scored:
            offset = track.offset
            if offset is not None:
                counts[offset] = counts.get(offset, 0) + 1
        modal_offset: int | None = None
        modal_count: int | None = None
        if counts:
            modal_count = max(counts.values())
            modal_offset = min(
                offset for offset, count in counts.items()
                if count == modal_count
            )
        z_values = [track.z for track in scored if track.z is not None]
        return cls(
            tracks=list(tracks),
            modal_offset=modal_offset,
            modal_count=modal_count,
            scored_tracks=len(scored),
            max_z=max(z_values) if z_values else None,
        )

    def validation_errors(self) -> list[str]:
        errors: list[str] = []
        for track in self.tracks:
            errors.extend(track.validation_errors())
        scored = sum(
            1 for track in self.tracks
            if track.error is None and track.offset is not None
        )
        if self.scored_tracks != scored:
            errors.append(
                "aac_lattice scored_tracks must count the scored track rows: "
                f"{self.scored_tracks} != {scored}"
            )
        album_stats = (self.modal_offset, self.modal_count, self.max_z)
        if scored == 0:
            if any(value is not None for value in album_stats):
                errors.append(
                    "aac_lattice album statistics require a scored track"
                )
        elif any(value is None for value in album_stats):
            errors.append(
                "aac_lattice album statistics are missing for a scored album"
            )
        elif not 1 <= (self.modal_count or 0) <= scored:
            errors.append(
                "aac_lattice modal_count must be between 1 and scored_tracks"
            )
        return errors


class VerifiedLosslessProof(
    msgspec.Struct, frozen=True, forbid_unknown_fields=True
):
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


class CdTocIdentity(msgspec.Struct, frozen=True):
    """The exact CD-shaped source identity submitted to both providers."""

    track_offsets_sectors: list[int]
    leadout_sector: int
    accuraterip_id: str
    musicbrainz_disc_id: str

    def validation_errors(self) -> list[str]:
        errors: list[str] = []
        if not self.track_offsets_sectors or len(self.track_offsets_sectors) > 99:
            errors.append("CD TOC needs at least one track")
        elif self.track_offsets_sectors[0] != 0:
            errors.append("CD TOC first track must start at sector zero")
        if any(
            right <= left
            for left, right in zip(
                self.track_offsets_sectors,
                self.track_offsets_sectors[1:],
                strict=False,
            )
        ):
            errors.append("CD TOC track offsets must be strictly increasing")
        if (
            self.track_offsets_sectors
            and self.leadout_sector <= self.track_offsets_sectors[-1]
        ):
            errors.append("CD TOC leadout must follow the last track")
        if self.leadout_sector <= 0 or self.leadout_sector >= 2**32:
            errors.append("CD TOC leadout must fit positive uint32")
        if any(
            offset < 0 or offset >= 2**32
            for offset in self.track_offsets_sectors
        ):
            errors.append("CD TOC offsets must fit uint32")
        if not self.accuraterip_id or not self.musicbrainz_disc_id:
            errors.append("CD TOC provider identities are required")
        return errors


class AccurateRipBitMatch(msgspec.Struct, frozen=True):
    """All-track AccurateRip match at one exact drive read offset."""

    provider: Literal["accuraterip"]
    url: str
    checksum_version: Literal["arv1", "arv2"]
    read_offset_samples: int
    track_confidences: list[int]
    track_checksums: list[int]
    response_sha256: str

    def validation_errors(self, track_count: int) -> list[str]:
        errors: list[str] = []
        if self.provider != "accuraterip":
            errors.append("AccurateRip provider tag is invalid")
        if self.checksum_version not in ("arv1", "arv2"):
            errors.append("AccurateRip checksum version is invalid")
        if not self.url.startswith("https://"):
            errors.append("AccurateRip provider URL must use HTTPS")
        if len(self.track_confidences) != track_count:
            errors.append("AccurateRip confidence must cover every track")
        if any(confidence <= 0 for confidence in self.track_confidences):
            errors.append("AccurateRip confidence must be positive")
        if len(self.track_checksums) != track_count:
            errors.append("AccurateRip checksum must cover every track")
        if any(checksum < 0 or checksum > 0xFFFFFFFF
               for checksum in self.track_checksums):
            errors.append("AccurateRip checksums must fit uint32")
        if not -5000 <= self.read_offset_samples <= 5000:
            errors.append("AccurateRip read offset exceeds the admitted radius")
        if re.fullmatch(r"[0-9a-f]{64}", self.response_sha256) is None:
            errors.append("AccurateRip response SHA-256 must be lowercase hex")
        return errors


class CtdbWholeDiscMatch(msgspec.Struct, frozen=True):
    """Exact CTDB whole-disc CRC match; track-only matches never reach here."""

    provider: Literal["ctdb"]
    url: str
    entry_id: str
    confidence: int
    crc32: int
    stride_samples: int
    response_toc_sectors: list[int]
    response_toc_shift_sectors: int
    response_sha256: str

    def validation_errors(self) -> list[str]:
        errors: list[str] = []
        if self.provider != "ctdb":
            errors.append("CTDB provider tag is invalid")
        if not self.url.startswith("https://"):
            errors.append("CTDB provider URL must use HTTPS")
        if not self.entry_id:
            errors.append("CTDB entry id is required")
        if self.confidence <= 0:
            errors.append("CTDB confidence must be positive")
        if not 0 <= self.crc32 <= 0xFFFFFFFF:
            errors.append("CTDB CRC32 must fit uint32")
        if self.stride_samples != 5880:
            errors.append("CTDB stride must be 5880 samples")
        if len(self.response_toc_sectors) < 2:
            errors.append("CTDB response TOC must include a track and leadout")
        if any(value < 0 or value >= 2**32
               for value in self.response_toc_sectors):
            errors.append("CTDB response TOC sectors must fit uint32")
        if not 0 <= self.response_toc_shift_sectors < 2**32:
            errors.append("CTDB response TOC shift must fit uint32")
        if re.fullmatch(r"[0-9a-f]{64}", self.response_sha256) is None:
            errors.append("CTDB response SHA-256 must be lowercase hex")
        return errors


class CdRipBitVerification(msgspec.Struct, frozen=True):
    """Positive-only source-authenticity evidence from CD rip databases.

    The evidence always describes the downloaded source.  When it is carried
    to an installed row after conversion, ``provenance`` changes to
    ``carried`` but ``source_format`` and the provider result still describe
    the original source rather than the derivative bytes.
    """

    algorithm: Literal["cd-rip-bit-verifier-v1"] = "cd-rip-bit-verifier-v1"
    provenance: EvidenceProvenance = EVIDENCE_PROVENANCE_MEASURED
    source_format: Literal["flac", "alac"] = "flac"
    toc: CdTocIdentity = msgspec.field(
        default_factory=lambda: CdTocIdentity([], 0, "", "")
    )
    accuraterip: AccurateRipBitMatch | None = None
    ctdb: CtdbWholeDiscMatch | None = None

    def validation_errors(self) -> list[str]:
        errors = self.toc.validation_errors()
        if self.algorithm != "cd-rip-bit-verifier-v1":
            errors.append("CD rip algorithm is invalid")
        if self.source_format not in ("flac", "alac"):
            errors.append("CD rip source format is invalid")
        if self.provenance not in (
            EVIDENCE_PROVENANCE_MEASURED,
            EVIDENCE_PROVENANCE_CARRIED,
        ):
            errors.append("CD rip provenance must be measured or carried")
        if self.accuraterip is None and self.ctdb is None:
            errors.append("CD rip verification requires a positive provider match")
        if self.accuraterip is not None:
            errors.extend(
                self.accuraterip.validation_errors(
                    len(self.toc.track_offsets_sectors)
                )
            )
        if self.ctdb is not None:
            errors.extend(self.ctdb.validation_errors())
            expected_toc = [
                *self.toc.track_offsets_sectors,
                self.toc.leadout_sector,
            ]
            normalized_toc = [
                sector - self.ctdb.response_toc_shift_sectors
                for sector in self.ctdb.response_toc_sectors
            ]
            if normalized_toc != expected_toc:
                errors.append(
                    "CTDB response TOC must normalize exactly to submitted TOC"
                )
        return errors

    def verified_lossless_proof(self) -> VerifiedLosslessProof:
        providers: list[str] = []
        if self.accuraterip is not None:
            providers.append(
                f"AccurateRip {self.accuraterip.checksum_version.upper()} "
                f"offset {self.accuraterip.read_offset_samples:+d}"
            )
        if self.ctdb is not None:
            providers.append(
                f"CTDB whole-disc confidence {self.ctdb.confidence}"
            )
        return VerifiedLosslessProof(
            provenance=self.provenance,
            source=self.source_format,
            classifier=CD_RIP_BIT_VERIFIED_CLASSIFIER,
            detail="; ".join(providers),
        )


def cd_rip_proof_pair_validation_errors(
    cd_rip: CdRipBitVerification | None,
    proof: VerifiedLosslessProof | None,
) -> list[str]:
    """Validate the structured CD fact and its exact scalar projection."""
    if cd_rip is None:
        if proof is not None and proof.classifier == CD_RIP_BIT_VERIFIED_CLASSIFIER:
            return ["CD rip verified-lossless proof requires structured evidence"]
        return []
    errors = cd_rip.validation_errors()
    expected = cd_rip.verified_lossless_proof()
    if proof != expected:
        errors.append("CD rip verification requires its exact scalar proof")
    return errors


#: The lineage versions whose facts describe the bytes of the attempt or
#: album holding the row. Migration 050 marks everything older as v1
#: precisely because its measurement may be a projected target rather than a
#: fact about a source, and migration 021 §6b cross-walked those older rows
#: onto whichever content-addressed evidence row their release already had —
#: so a v1 row can belong to a sibling attempt entirely.
#: ``lineage_version`` is constrained to 1, 3, 4 or 5.
SOURCE_SEMANTIC_LINEAGE_VERSIONS: tuple[int, ...] = (3, 4, 5)

#: The lineage every writer emits today. A row below it reports itself stale
#: (``lib.quality_evidence.current_evidence_rebuild_reasons``) and is rebuilt
#: from live Beets facts before it decides anything, which is how a policy
#: change to *derivation* reaches existing rows without a backfill. Bumped to 5
#: by issue #1145: a v4 MP3 row was ranked against one of two band tables 75
#: kbps apart, chosen by an inferred encoding mode; a v5 row is ranked against
#: the single ``mp3`` table. The two-axis fact vocabulary is unchanged between
#: 4 and 5 — every ``lineage_version < 4`` merge predicate in
#: ``lib.pipeline_db.evidence`` means "predates that vocabulary" and stays at
#: 4 deliberately, because widening it would replace (not merge) the preserved
#: spectral and V0 tuples on every v4 row's rebuild.
CURRENT_EVIDENCE_LINEAGE_VERSION = 5


def evidence_is_source_semantic(lineage_version: object) -> bool:
    """Whether this evidence row's facts describe its holder's own bytes.

    ONE spelling, because it gates two different kinds of fact for the same
    reason. The measurement facts have always been gated on it (a v1
    measurement may be a projected target). The minted verified-lossless
    proof is now gated on it too, and by exactly this predicate rather than
    a second copy of the rule: three operator surfaces state that proof —
    the Recents verdict, the Recents expanded card and ``pipeline-cli
    quality``'s "proved by" line — and all three were ungated. Recents put
    "MP3 320, verified lossless" on a never-converted MP3 wearing its FLAC
    sibling's proof; the CLI attributed a cross-walked sibling's proof on
    4,910 live requests.

    Provenance is deliberately NOT part of this rule.
    ``EVIDENCE_PROVENANCE_CARRIED`` does not mean "another album's proof" —
    ``lib/quality_evidence.py`` writes it when the just-imported candidate's
    OWN proof is carried onto the library row for that same album, which is
    the documented lossless-source-gated propagation. Refusing it would
    delete a true "proved by" line from 2,241 live requests, which is the
    population the line exists for.

    Deliberately takes ``object``: the overlay reads the value off a raw
    joined row dict where it is untyped, and an unexpected value must fail
    CLOSED to "not source-semantic" rather than reach a comparison that
    assumes a shape.
    """
    return lineage_version in SOURCE_SEMANTIC_LINEAGE_VERSIONS


class AlbumQualityEvidence(
    msgspec.Struct, frozen=True, forbid_unknown_fields=True
):
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
    files: list[AlbumQualityEvidenceFile] = msgspec.field(
        default_factory=list[AlbumQualityEvidenceFile]
    )
    codec: str | None = None
    container: str | None = None
    storage_format: str | None = None
    target_format: str | None = None
    # Album-wide bitrate mode of the projected target/probe.  This is a
    # contract fact, not the downloaded source or materialized-output mode.
    target_is_cbr: bool | None = None
    # Migration 050 marks the interpretation of storage/target fields.
    # Historical rows are v1/v3; the two-axis vocabulary starts at v4.
    # v5 (issue #1145) keeps that vocabulary exactly and re-derives the MP3
    # rank: the bump exists so every v4 row rebuilds through the single
    # collapsed ladder instead of needing a backfill.
    lineage_version: int = CURRENT_EVIDENCE_LINEAGE_VERSION
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
    cd_rip_verification: CdRipBitVerification | None = None
    audio_validation: AudioValidationReport = msgspec.field(
        default_factory=legacy_unrecorded_audio_validation_report,
    )
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
    # issue #829 AAC-lattice leg PR-A capture. Measured by the preview
    # worker on the promotion-plausible cohort only (lossless containers
    # whose album spectral grade is genuine/marginal) because it costs tens
    # of seconds of CPU per track. NULL means never measured; a capture with
    # ``scored_tracks == 0`` means measured and nothing scored. No decision
    # reads it in PR-A.
    aac_lattice: AacLatticeCapture | None = None

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
            cd_rip_verification=self.cd_rip_verification,
            audio_validation=self.audio_validation,
            audio_corrupt=self.audio_corrupt,
            audio_error=self.audio_error,
            folder_layout=self.folder_layout,
            audio_file_count=self.audio_file_count,
            filetype_band=self.filetype_band,
            matched_bad_audio_hash_id=self.matched_bad_audio_hash_id,
            matched_bad_audio_hash_path=self.matched_bad_audio_hash_path,
            aac_lattice=self.aac_lattice,
        )

    def storage_validation_errors(self) -> list[str]:
        errors: list[str] = []
        try:
            validate_audio_validation_report(self.audio_validation)
        except ValueError as exc:
            errors.append(str(exc))
        if self.audio_validation.outcome == "measurement_failed":
            errors.append(
                "measurement_failed cannot be stored as content evidence"
            )
        report_is_corrupt = self.audio_validation.outcome in {
            "audio_corrupt",
            "legacy_failure",
        }
        if self.audio_corrupt != report_is_corrupt:
            errors.append(
                "audio_corrupt must agree with audio_validation outcome"
            )
        failed_snapshot_paths = {
            file.relative_path for file in self.files if not file.decode_ok
        }
        for diagnostic in self.audio_validation.diagnostics:
            if (
                self.audio_validation.outcome == "audio_corrupt"
                and diagnostic.relative_path
                and diagnostic.relative_path not in failed_snapshot_paths
            ):
                errors.append(
                    "audio validation diagnostic path is not marked "
                    f"decode_ok=false: {diagnostic.relative_path}"
                )
        if (
            self.audio_validation.outcome == "passed"
            and failed_snapshot_paths
        ):
            errors.append(
                "passed audio validation cannot carry decode_ok=false files"
            )
        if not self.mb_release_id:
            errors.append("mb_release_id must be a non-empty string")
        if not self.snapshot_fingerprint:
            errors.append("snapshot_fingerprint must be a non-empty string")
        if self.lineage_version not in (1, 3, 4, 5):
            errors.append("lineage_version must be 1, 3, 4, or 5")
        if self.lineage_version >= 3:
            errors.extend(self.measurement.new_row_validation_errors(
                two_axis=self.lineage_version >= 4,
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
        errors.extend(cd_rip_proof_pair_validation_errors(
            self.cd_rip_verification,
            self.verified_lossless_proof,
        ))
        # Empty snapshot is a storable fact ONLY when audio_file_count=0
        # (the explicit empty-inventory signal). When a fileset is present
        # but ``files`` is empty, the evidence row is incomplete.
        if not self.files and self.audio_file_count != 0:
            errors.append("at least one snapshot file is required")
        if self.folder_layout not in ("flat", "nested"):
            errors.append(
                f"folder_layout must be 'flat' or 'nested': {self.folder_layout!r}"
            )
        if self.audio_file_count < 0:
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
        if self.lineage_version >= 4 and self.v0_metric is not None:
            errors.extend(self.v0_metric.validation_errors())
        if self.lineage_version >= 4 and self.verified_lossless_proof is not None:
            errors.extend(self.verified_lossless_proof.validation_errors())
        if self.aac_lattice is not None:
            errors.extend(self.aac_lattice.validation_errors())
        return errors

    def policy_incomplete_reasons(
        self, *, require_quality_measurement: bool = True,
    ) -> list[str]:
        """Return reasons this row is not ready for action reducers.

        ``require_quality_measurement=False`` (issue #1355 item 2) drops the
        format/bitrate checks below for a row whose durable folder/audio-
        integrity facts already make quality measurement irrelevant to the
        decision that will consume it — a corrupt, bad-hash, nested, or
        empty-fileset candidate the preview worker persisted without ever
        running the harness (``lib.quality_evidence.evidence_from_measurement``).
        Callers pass ``False`` only when they have independently established
        that fact (``lib.quality.pipeline.candidate_preimport_reject_fact``);
        this method has no opinion on when that is true. Every other check
        — structural validity and a recorded ``source_path`` — still applies
        regardless, so a row that is merely malformed, or one with no
        quality measurement AND no reject fact, still fails closed exactly
        as before.
        """

        reasons = self.storage_validation_errors()
        if not self.source_path.strip():
            # A row without a recorded path can never be re-verified against
            # disk nor completed by HAVE enrichment (every persist guard
            # compares the scanned path against ``source_path``), so it must
            # be rebuilt rather than used as decision authority
            # (download_log 37206: a blank-path legacy backfill kept the
            # French Quarter import spectrally blind forever).
            reasons.append("source_path is required")
        if not require_quality_measurement:
            return reasons
        if self.measurement.format is None:
            reasons.append("measurement.format is required")
        if (
            self.measurement.min_bitrate_kbps is None
            and self.measurement.avg_bitrate_kbps is None
            and self.measurement.median_bitrate_kbps is None
        ):
            reasons.append("at least one measurement bitrate metric is required")
        return reasons


class V0ProbeEvidence(
    msgspec.Struct, frozen=True, forbid_unknown_fields=True
):
    """MP3 V0 probe metrics used as source-lineage evidence.

    ``kind`` is intentionally explicit because not every V0 probe is eligible
    for policy decisions. Only ``lossless_source_v0`` proves the candidate came
    from a supported lossless-container source. Native-lossy and on-disk probes
    are research evidence in v1.
    """

    kind: str = ""
    min_bitrate_kbps: int | None = None
    avg_bitrate_kbps: int | None = None
    median_bitrate_kbps: int | None = None


def is_comparable_lossless_source_probe(
    probe: V0ProbeEvidence | None,
) -> bool:
    return (
        probe is not None
        and probe.kind == V0_PROBE_LOSSLESS_SOURCE
        and probe.avg_bitrate_kbps is not None
    )


class QualityComparisonBasis(
    msgspec.Struct, frozen=True, forbid_unknown_fields=True
):
    """The comparison ``compare_quality()`` actually performed — persisted so
    the UI renders the decision's own story instead of re-deriving one.

    Emitted per-branch from inside ``compare_quality()``: the branch tag names
    which rule fired, and ``new_value_kbps`` / ``existing_value_kbps`` are the
    numbers that DECIDED that branch (spectral-clamped values on a clamped
    rank comparison, raw configured-metric values on a same-rank tiebreak).
    Consumers reading ``(metric, value)`` pairs must suppress the metric
    label when ``spectral_clamped and branch in ("rank", "spectral_tiebreak")``
    — the value there is ``min(metric, spectral floor)``, not the named
    statistic.
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
    new_value_kbps: int | None = None
    existing_value_kbps: int | None = None
    new_format: str | None = None
    existing_format: str | None = None
    spectral_clamped: bool = False
    tolerance_kbps: int | None = None
    verified_lossless_bypass: bool = False


COMPARISON_BASIS_BRANCHES: frozenset[str] = frozenset({
    "rank",                        # ranks differ — the primary key decided
    "rank_within_tolerance",       # ranks differ but by less than
                                   # within_rank_tolerance_kbps of measured
                                   # bitrate — same family, both bare labels,
                                   # no spectral clamp (issue #1145)
    "lossless_same_rank",          # both LOSSLESS: equivalent by identity
    "cross_family_same_rank",      # same rank, different codec family
    "label_contract_same_rank",    # same rank, explicit label is authoritative
    "spectral_tiebreak",           # same rank, differing clamped spectral values decide
    "spectral_candidate_bound",    # candidate class vs known-clean HAVE raw metric
    "spectral_existing_bound",     # known-clean candidate raw metric vs HAVE class
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


SPECTRAL_AFFIRMATIVE_GRADES: frozenset[str] = frozenset({"genuine", "marginal"})
"""Spectral grades that positively establish a non-transcode encode.

These are deliberately narrower than "not transcode": an absent, failed, or
unknown measurement cannot license a one-sided spectral comparison against a
classed encode.
"""
