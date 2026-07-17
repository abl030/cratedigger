"""ImportResult + postflight Structs and the stdout sentinel parser.

Extracted verbatim from the monolithic ``lib/quality.py`` (issue #477).
Pure move: every definition is AST-identical to the original.
"""

import json
from typing import Any, Optional
import msgspec

from lib.quality.evidence_types import (
    AudioQualityMeasurement,
    EVIDENCE_PROVENANCE_CARRIED,
    EVIDENCE_PROVENANCE_MEASURED,
    EVIDENCE_SUBJECT_INSTALLED,
    EVIDENCE_SUBJECT_SOURCE,
    QualityComparisonBasis,
    TargetQualityContract,
    V0ProbeEvidence,
    VerifiedLosslessProof,
)


IMPORT_RESULT_SENTINEL = "__IMPORT_RESULT__"


# ---------------------------------------------------------------------------
# Structured result from import_one.py
# ---------------------------------------------------------------------------

class ConversionInfo(msgspec.Struct):
    """FLAC→V0 conversion details and process artifacts.

    Wire-boundary type per ``.claude/rules/code-quality.md`` — nested in
    ``ImportResult`` which crosses both the harness stdout and JSONB edges.
    """
    converted: int = 0
    failed: int = 0
    was_converted: bool = False
    original_filetype: Optional[str] = None
    target_filetype: Optional[str] = None
    post_conversion_min_bitrate: Optional[int] = None  # min bitrate after lossless→V0
    is_transcode: bool = False  # True if FLAC was actually a transcode
    final_format: Optional[str] = None  # e.g. "opus 128", "mp3 v2", "aac 128"
    # Source channel count read off the first source file before conversion.
    # ``> 2`` means the ffmpeg invocation downmixed multichannel → stereo;
    # 5.1(side) FLAC otherwise breaks libopus outright (Mott / r3852). None
    # for legacy rows or when the probe fails.
    source_channels: Optional[int] = None


class SpectralTrackDetail(msgspec.Struct, frozen=True):
    """One track from an attempt-local spectral analysis."""

    grade: str
    hf_deficit_db: float = 0.0
    cliff_detected: bool = False
    cliff_freq_hz: Optional[int] = None
    estimated_bitrate_kbps: Optional[int] = None
    error: Optional[str] = None


class SpectralAnalysisDetail(msgspec.Struct, frozen=True):
    """Complete audit result for one side of an import attempt."""

    attempted: bool = False
    grade: Optional[str] = None
    bitrate_kbps: Optional[int] = None
    suspect_pct: Optional[float] = None
    per_track: list[SpectralTrackDetail] = msgspec.field(default_factory=list)
    error: Optional[str] = None


class SpectralDetail(msgspec.Struct):
    """Per-track spectral analysis detail.

    The album-level spectral grades and bitrates now live on
    AudioQualityMeasurement (source/current measurements on ImportResult).
    This carries the per-track detail data that doesn't fit on a measurement.
    Wire-boundary type per ``.claude/rules/code-quality.md``.
    """
    cliff_freq_hz: Optional[int] = None
    suspect_pct: float = 0.0
    per_track: list[SpectralTrackDetail] = []
    existing_suspect_pct: float = 0.0
    # Attempt-local display audit. These are deliberately disjoint from
    # source/current measurements, which remain the decision inputs.
    candidate: Optional[SpectralAnalysisDetail] = None
    existing: Optional[SpectralAnalysisDetail] = None


# Issue #133: ``DisambiguationFailure`` / ``SelectorFailure`` were two
# duplicated types with identical shape, scattered across lib.quality
# and lib.release_cleanup. They are now a single
# ``lib.beets_album_op.BeetsOpFailure``; these aliases preserve existing
# imports (``from lib.quality import DisambiguationFailure`` in the
# harness, tests/helpers.py, etc). The unified type added a ``selector``
# field (default ``""``) so old JSON rows with only ``{reason, detail}``
# still decode cleanly via the ``msgspec.convert(d, type=ImportResult)``
# call in ``ImportResult.from_dict`` — msgspec defaults fill in any
# missing key on the nested Struct.
from lib.beets_album_op import BeetsOpFailure as DisambiguationFailure


class MovedSibling(msgspec.Struct, frozen=True):
    """Legacy issue #132 P2 / issue #133 record of a sibling album whose
    ``beet move`` relocated its files during post-import canonicalization.

    New imports do not emit this: Beets now owns atomic replacement and
    Cratedigger no longer runs post-import sibling canonicalization. The type
    remains so old ``download_log.import_result`` JSONB rows keep decoding.

    ``album_id`` is the beets numeric primary key for the sibling.
    ``new_path`` is the on-disk directory after the move.
    ``mb_albumid`` / ``discogs_albumid`` are the two columns from
    beets' ``albums`` table at emit time — the harness resolves them
    so the dispatcher doesn't need a second beets DB connection when
    propagating the new path to the pipeline DB.

    Every field mattered for propagation: if the sibling's release id
    matched a tracked ``album_requests`` row, its ``imported_path`` was
    updated so the UI stopped pointing at the pre-move directory.

    Wire-boundary type per ``.claude/rules/code-quality.md`` §
    "Wire-boundary types". Decoded from harness stdout JSON AND from
    ``download_log.import_result`` JSONB on every web API read. The
    strict-typed decode via ``msgspec.convert(d, type=ImportResult)``
    in ``ImportResult.from_dict`` raises ``msgspec.ValidationError``
    if a future harness change emits ``album_id`` as a string or
    drops a required field, rather than silently corrupting
    downstream state (the PR #98 / issue #99 lesson). Encoded
    symmetrically via ``msgspec.json.encode`` — same policy both
    directions.
    """
    album_id: int
    new_path: str
    mb_albumid: str = ""
    discogs_albumid: str = ""


class DuplicateRemoveCandidate(msgspec.Struct, frozen=True):
    """One beets album that ``get_duplicate_action`` said Beets would remove."""

    beets_album_id: Optional[int] = None
    mb_albumid: str = ""
    discogs_albumid: str = ""
    album_path: str = ""
    item_count: int = 0
    albumartist: str = ""
    album: str = ""


class DuplicateRemoveGuardInfo(msgspec.Struct):
    """Guard outcome for Beets-owned duplicate replacement.

    Populated when Cratedigger refuses to answer ``remove`` because Beets'
    duplicate callback exposed an unsafe would-remove set.
    """

    reason: str = ""
    target_source: str = ""
    target_release_id: str = ""
    duplicate_count: int = 0
    candidates: list[DuplicateRemoveCandidate] = []
    message: str = ""
    quarantine_path: Optional[str] = None
    quarantine_error: Optional[str] = None


class PostflightInfo(msgspec.Struct):
    """Beets post-import verification data.

    Wire-boundary type per ``.claude/rules/code-quality.md`` — nested
    in ``ImportResult.postflight``, flows through ``download_log``.
    """
    beets_id: Optional[int] = None
    track_count: Optional[int] = None
    imported_path: Optional[str] = None
    bad_extensions: list[str] = []  # files with non-audio extensions
    # Legacy issue #127 / #132 fields. New imports do not run post-import
    # ``beet move``; these remain for old import-result rows and web recents.
    disambiguated: bool = False
    disambiguation_failure: Optional[DisambiguationFailure] = None
    moved_siblings: list[MovedSibling] = []
    duplicate_remove_guard: Optional[DuplicateRemoveGuardInfo] = None
    # Beets albums the dup-guard ALLOWED beets to remove during this import —
    # the replaced pre-upgrade copies. Their ``album_path``s are where the
    # album lived before a path-changing upgrade, which is what the Jellyfin
    # pin capture needs to find the pre-upgrade items (Jellyfin item identity
    # is a hash of the path). Empty for genuinely-new imports.
    replaced_albums: list[DuplicateRemoveCandidate] = []


class QualityEvidenceActionProvenance(msgspec.Struct, frozen=True):
    """Provenance for action-time quality evidence acquisition."""

    candidate_status: str | None = None
    current_status: str | None = None
    snapshot_status: str | None = None
    fallback_reason: str | None = None


class ImportResult(msgspec.Struct):
    """Structured result emitted by import_one.py as JSON.

    Carries every piece of data that crosses the subprocess boundary
    from import_one.py back to cratedigger.py. Stored in download_log.import_result
    for complete auditability.

    ``source_measurement`` / ``current_measurement`` carry the downloaded
    source/current state. ``target_quality_contract`` is policy, while
    ``materialized_measurement`` is deliberately
    separate: it describes the bytes that actually landed in Beets after any
    target conversion. A lossless candidate may be decided through a
    temporary MP3 V0 probe, then stored as Opus; collapsing those measurements
    makes a V0 bitrate wear an Opus label.

    Wire-boundary type per ``.claude/rules/code-quality.md``: encode via
    ``msgspec.json.encode``, decode via ``msgspec.convert`` — symmetric.
    The pre-#141 asymmetry (``json.dumps(asdict(self))`` outbound,
    ``msgspec.convert`` inbound) forced ``MovedSibling`` et al to be
    ``@dataclass`` so ``asdict`` could recurse; unifying on
    ``msgspec.json.encode`` let every type become a Struct with one rule.
    """
    version: int = 4
    exit_code: int = 0
    decision: Optional[str] = None      # from import_quality_decision() or error label
    already_in_beets: bool = False
    source_measurement: Optional[AudioQualityMeasurement] = None
    verified_lossless_proof: Optional[VerifiedLosslessProof] = None
    current_measurement: Optional[AudioQualityMeasurement] = None
    target_quality_contract: Optional[TargetQualityContract] = None
    materialized_measurement: Optional[AudioQualityMeasurement] = None
    # Set only by the quarantined v1/v2/v3 reader. New v4 producers never
    # infer lineage from historical equality or label heuristics.
    legacy_projection_version: Optional[int] = None
    conversion: ConversionInfo = msgspec.field(default_factory=ConversionInfo)
    spectral: SpectralDetail = msgspec.field(default_factory=SpectralDetail)
    postflight: PostflightInfo = msgspec.field(default_factory=PostflightInfo)
    beets_log: list[str] = []  # beets stderr lines from import
    error: Optional[str] = None
    # Target-conversion audit trail — V0 bitrate that proved genuineness
    v0_verification_bitrate: Optional[int] = None
    final_format: Optional[str] = None  # configured target, None means keep V0/MP3
    preview: bool = False              # True for no-mutation import preview
    v0_probe: Optional[V0ProbeEvidence] = None
    existing_v0_probe: Optional[V0ProbeEvidence] = None
    quality_evidence_provenance: QualityEvidenceActionProvenance = msgspec.field(
        default_factory=QualityEvidenceActionProvenance
    )
    # The comparison the decision actually performed (request 6039: the UI
    # re-derived "MP3 V2 to MP3 V2" from min bitrate while the decider ranked
    # on avg). None on rows predating the field and when no existing album
    # was compared — the UI falls back to the legacy min-based labels.
    comparison_basis: Optional[QualityComparisonBasis] = None

    def to_json(self) -> str:
        """Serialize to JSON string via msgspec.json.encode."""
        if self.version != 4:
            raise ValueError("new ImportResult rows must use version 4")
        self.validate_new_row()
        return msgspec.json.encode(self).decode()

    def validate_new_row(self) -> None:
        """Reject ambiguous facts on every v4 producer/persistence boundary."""

        if self.version != 4:
            raise ValueError("new ImportResult rows must use version 4")
        if self.legacy_projection_version is not None:
            raise ValueError(
                "legacy_projection_version is reserved for the v1/v2/v3 reader"
            )
        target = self.target_quality_contract
        if target is not None and not target.format.strip():
            raise ValueError("target_quality_contract.format is required")
        for field_name, measurement, source in (
            ("source_measurement", self.source_measurement, True),
            ("current_measurement", self.current_measurement, False),
            ("materialized_measurement", self.materialized_measurement, False),
        ):
            if measurement is None:
                continue
            errors = measurement.new_row_validation_errors(source=source)
            if errors:
                raise ValueError(f"{field_name}: {'; '.join(errors)}")
        proof = self.verified_lossless_proof
        if proof is not None:
            errors = proof.validation_errors()
            if errors:
                raise ValueError(
                    f"verified_lossless_proof: {'; '.join(errors)}"
                )

    def to_sentinel_line(self) -> str:
        """Format as the stdout sentinel line for subprocess communication."""
        return IMPORT_RESULT_SENTINEL + self.to_json()

    @classmethod
    def _migrate_v1(cls, d: dict) -> "ImportResult":
        """Project version 1 (QualityInfo + SpectralInfo) into the v4 model.

        v1 rows in production (~226 on doc2 as of 2026-04) carry
        ``quality`` and ``spectral`` sub-objects instead of measurements.
        This method first reconstructs the historical v2 shape, then routes it
        through the quarantined legacy projection in ``from_dict``.
        """
        quality = d.get("quality") or {}
        spectral = d.get("spectral") or {}
        conv_d = dict(d.get("conversion") or {})

        # Migrate process fields from QualityInfo → ConversionInfo
        conv_d.setdefault("post_conversion_min_bitrate",
                          quality.get("post_conversion_min_bitrate"))
        conv_d.setdefault("is_transcode", quality.get("is_transcode", False))

        # Build measurements from scattered fields. v1 rows predate the
        # avg/median bitrate fields (issue #60 / #64) — leaving them at the
        # default None makes measurement_rank() fall back to min, which is
        # the same behavior the v1 row was originally classified under.
        new_measurement: dict[str, Any] = {
            "min_bitrate_kbps": quality.get("new_min_bitrate"),
            "spectral_grade": spectral.get("grade"),
            "spectral_bitrate_kbps": spectral.get("bitrate"),
            "verified_lossless": quality.get(
                "will_be_verified_lossless", False),
            "was_converted_from": (conv_d.get("original_filetype")
                                   if conv_d.get("was_converted") else None),
        }
        existing_measurement: Optional[dict[str, Any]] = None
        if quality.get("prev_min_bitrate") is not None:
            existing_measurement = {
                "min_bitrate_kbps": quality.get("prev_min_bitrate"),
                "spectral_grade": spectral.get("existing_grade"),
                "spectral_bitrate_kbps": spectral.get("existing_bitrate"),
            }

        normalised: dict[str, Any] = {
            "version": 2,
            "exit_code": d.get("exit_code", 0),
            "decision": d.get("decision"),
            "already_in_beets": d.get("already_in_beets", False),
            "new_measurement": new_measurement,
            "existing_measurement": existing_measurement,
            "conversion": conv_d,
            "spectral": {
                "cliff_freq_hz": spectral.get("cliff_freq_hz"),
                "suspect_pct": spectral.get("suspect_pct", 0.0),
                "per_track": spectral.get("per_track", []),
                "existing_suspect_pct": spectral.get(
                    "existing_suspect_pct", 0.0),
            },
            "postflight": d.get("postflight") or {},
            "beets_log": d.get("beets_log", []),
            "error": d.get("error"),
        }
        return cls._project_legacy_v2(normalised, source_version=1)

    @classmethod
    def _project_legacy_v2(
        cls,
        d: dict[str, Any],
        *,
        source_version: int = 2,
    ) -> "ImportResult":
        """Quarantine the ambiguous v1/v2 measurement shape.

        Historical ``new_measurement`` sometimes combined V0-probe numbers
        with a target label.  Preserve that historical projection for reads,
        mark its origin explicitly, and never run this adapter for v4 rows.
        """

        projected = cls._normalise_legacy_postflight(d)
        projected["version"] = 4
        projected["legacy_projection_version"] = source_version
        projected["source_measurement"] = projected.pop("new_measurement", None)
        projected["current_measurement"] = projected.pop(
            "existing_measurement", None
        )
        projected.setdefault("target_quality_contract", None)
        source_measurement = projected.get("source_measurement")
        legacy_verified = False
        if isinstance(source_measurement, dict):
            legacy_verified = bool(source_measurement.pop("verified_lossless", False))
        if legacy_verified:
            conversion = projected.get("conversion")
            original_filetype = (
                conversion.get("original_filetype")
                if isinstance(conversion, dict)
                else None
            )
            proof = VerifiedLosslessProof(
                provenance=EVIDENCE_PROVENANCE_MEASURED,
                source=original_filetype or "lossless_source",
                classifier="legacy_import_result",
            )
            projected["verified_lossless_proof"] = msgspec.to_builtins(proof)
        return msgspec.convert(projected, type=cls)

    @classmethod
    def _project_legacy_v3(cls, d: dict[str, Any]) -> "ImportResult":
        """Project persisted v3 facts into the explicit v4 two-axis model.

        The base v3 writer put verified-lossless on the source measurement and
        emitted spectral facts without subject/provenance markers. A short-lived
        v3 proof shape also used ``proof_origin``. Preserve those audit facts,
        mark the byte subject directly from the measurement slot, and quarantine
        the projection so it can never be re-emitted as a new row.
        """

        projected = dict(d)
        projected["version"] = 4
        projected["legacy_projection_version"] = 3

        conversion = projected.get("conversion")
        original_filetype = (
            conversion.get("original_filetype")
            if isinstance(conversion, dict)
            else None
        )
        legacy_verified = False
        for field_name, subject in (
            ("source_measurement", EVIDENCE_SUBJECT_SOURCE),
            ("current_measurement", EVIDENCE_SUBJECT_INSTALLED),
            ("materialized_measurement", EVIDENCE_SUBJECT_INSTALLED),
        ):
            raw_measurement = projected.get(field_name)
            if not isinstance(raw_measurement, dict):
                continue
            measurement = dict(raw_measurement)
            if field_name == "source_measurement":
                legacy_verified = bool(
                    measurement.pop("verified_lossless", False)
                )
            else:
                measurement.pop("verified_lossless", None)
            if (
                measurement.get("spectral_grade") is not None
                or measurement.get("spectral_bitrate_kbps") is not None
            ):
                measurement.setdefault("spectral_subject", subject)
                measurement.setdefault(
                    "spectral_provenance", EVIDENCE_PROVENANCE_MEASURED
                )
            projected[field_name] = measurement

        raw_proof = projected.get("verified_lossless_proof")
        if isinstance(raw_proof, dict):
            proof = dict(raw_proof)
            proof_origin = proof.pop("proof_origin", None)
            if proof.get("provenance") is None and proof_origin is not None:
                proof["provenance"] = (
                    EVIDENCE_PROVENANCE_CARRIED
                    if proof_origin == "legacy_request_seed"
                    else EVIDENCE_PROVENANCE_MEASURED
                )
            projected["verified_lossless_proof"] = proof
        elif legacy_verified:
            source_measurement = projected.get("source_measurement")
            converted_from = (
                source_measurement.get("was_converted_from")
                if isinstance(source_measurement, dict)
                else None
            )
            projected["verified_lossless_proof"] = msgspec.to_builtins(
                VerifiedLosslessProof(
                    provenance=EVIDENCE_PROVENANCE_MEASURED,
                    source=converted_from or original_filetype or "lossless_source",
                    classifier="legacy_import_result",
                )
            )

        return msgspec.convert(projected, type=cls)

    @staticmethod
    def _normalise_legacy_postflight(d: dict[str, Any]) -> dict[str, Any]:
        """Preserve only the malformed-row tolerances required by v1/v2."""

        projected = dict(d)
        if "postflight" not in projected:
            return projected
        pf = projected["postflight"]
        if not isinstance(pf, dict):
            projected["postflight"] = {}
            return projected
        if (
            "moved_siblings" in pf
            and not isinstance(pf["moved_siblings"], list)
        ):
            pf = {**pf, "moved_siblings": []}
        guard = pf.get("duplicate_remove_guard")
        if guard is not None:
            if not isinstance(guard, dict):
                pf = {**pf, "duplicate_remove_guard": None}
            elif not isinstance(guard.get("candidates", []), list):
                pf = {
                    **pf,
                    "duplicate_remove_guard": {
                        **guard,
                        "candidates": [],
                    },
                }
        projected["postflight"] = pf
        return projected

    @classmethod
    def from_dict(cls, d: dict) -> "ImportResult":
        """Construct from a dict (e.g. parsed JSON).

        Handles historical v1/v2/v3 rows through quarantined projections and
        decodes v4 rows strictly. The legacy path uses ``msgspec.convert`` for
        typed decode; two pre-convert hedges preserve the
        pre-#141 ``_postflight_from_dict`` tolerance:

        1. A falsy non-object ``postflight`` (``null``, ``[]``, ``""``) —
           observed on very old malformed rows — is coerced to ``{}``
           so the PostflightInfo defaults materialise. Pre-#141 the
           loader's ``if not d: return PostflightInfo()`` guard treated
           these as absent; strict ``msgspec.convert`` would raise
           ``ValidationError`` and the callers we patched in the codex
           P2 fix would swallow it and drop the whole ``ImportResult``,
           silently losing every other field.
        2. A non-list ``postflight.moved_siblings`` (malformed legacy
           JSONB) falls back to ``[]``.

        Both hedges fire only inside the historical reader for data shapes
        observed in production. New v4 rows receive no malformed-row repair.
        """
        # Old format: has "quality" key, no "new_measurement".
        if "quality" in d and "new_measurement" not in d:
            return cls._migrate_v1(d)
        version = d.get("version", 2 if "new_measurement" in d else 3)
        if version not in (2, 3, 4):
            raise ValueError(f"unsupported ImportResult version: {version!r}")
        if version in (3, 4) and (
            "new_measurement" in d or "existing_measurement" in d
        ):
            raise ValueError(
                f"v{version} ImportResult must use source/current measurements"
            )
        if version in (3, 4) and d.get("legacy_projection_version") is not None:
            raise ValueError(
                "legacy_projection_version is reserved for the v1/v2/v3 reader"
            )
        if version == 2 or "new_measurement" in d:
            return cls._project_legacy_v2(d)
        if version == 3:
            return cls._project_legacy_v3(d)
        for field_name in (
            "source_measurement",
            "current_measurement",
            "materialized_measurement",
        ):
            measurement = d.get(field_name)
            if isinstance(measurement, dict) and "verified_lossless" in measurement:
                raise ValueError(
                    f"v4 {field_name} cannot carry verified_lossless"
                )
        proof = d.get("verified_lossless_proof")
        if isinstance(proof, dict) and "proof_origin" in proof:
            raise ValueError(
                "v4 verified_lossless_proof must use provenance"
            )
        result = msgspec.convert(d, type=cls)
        result.validate_new_row()
        return result

    @classmethod
    def from_json(cls, s: str) -> "ImportResult":
        """Deserialize from JSON string."""
        return cls.from_dict(json.loads(s))


def parse_import_result(stdout_text: str) -> Optional[ImportResult]:
    """Extract ImportResult from import_one.py stdout.

    Scans from the last line backward for the sentinel prefix.
    Returns None if no result found (crash, old version, etc) —
    including when the payload fails strict typed decode post-#141
    (``msgspec.ValidationError``). The callers treat a None here the
    same as "no sentinel line", degrading gracefully instead of
    crashing the whole cycle on a single bad harness emission.
    """
    for line in reversed(stdout_text.strip().split("\n")):
        if line.startswith(IMPORT_RESULT_SENTINEL):
            try:
                return ImportResult.from_json(line[len(IMPORT_RESULT_SENTINEL):])
            except (json.JSONDecodeError, TypeError, KeyError, ValueError,
                    msgspec.ValidationError):
                return None
    return None
