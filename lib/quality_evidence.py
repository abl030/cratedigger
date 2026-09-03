"""Album-quality evidence construction and persistence helpers."""

from __future__ import annotations

import hashlib
import json
import math
import os
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, Literal, Protocol, runtime_checkable

import msgspec

from lib.evidence_media_identity import (
    EVIDENCE_LOSSLESS_CONTAINERS,
    EVIDENCE_LOSSY_CONTAINERS,
    authoritative_lossy_media_pair,
    is_lossless_evidence_codec,
)
from lib.quality import (
    CURRENT_EVIDENCE_LINEAGE_VERSION,
    EVIDENCE_PROVENANCE_CARRIED,
    EVIDENCE_PROVENANCE_MEASURED,
    EVIDENCE_SUBJECT_INSTALLED,
    EVIDENCE_SUBJECT_SOURCE,
    V0_PROBE_LOSSLESS_SOURCE,
    V0_PROBE_NATIVE_LOSSY_RESEARCH,
    V0_PROBE_ON_DISK_RESEARCH,
    AlbumQualityEvidence,
    AlbumQualityEvidenceFile,
    AlbumQualityV0Metric,
    AudioQualityMeasurement,
    CdRipBitVerification,
    CodecFamily,
    EvidenceProvenance,
    EvidenceSubject,
    ImportResult,
    SpectralAnalysisDetail,
    V0ProbeEvidence,
    VerifiedLosslessProof,
    candidate_preimport_reject_fact,
    legacy_unrecorded_audio_validation_report,
)

if TYPE_CHECKING:
    from lib.beets_db import AlbumInfo, CurrentBeetsUnique
    from lib.import_execution import ExecutionLeaseSnapshot
    from lib.import_queue import ImportJob
    from lib.measurement import PreimportMeasurement
    from lib.pipeline_db.rows import AlbumRequestRow


@runtime_checkable
class QualityEvidenceDB(Protocol):
    """The PipelineDB surface the evidence persist/load helpers use (#409).

    Shared by ``lib/import_evidence.py`` (which forwards its handle into
    these loaders) and extended by ``WrongMatchCleanupDB`` for the same
    reason. Parity tests live in ``tests/test_quality_evidence.py``.
    """

    def get_request(self, request_id: int) -> AlbumRequestRow | None: ...

    def get_import_job(self, job_id: int) -> ImportJob | None: ...

    def upsert_album_quality_evidence(
        self,
        evidence: AlbumQualityEvidence,
        *,
        spectral_write_intent: SpectralWriteIntent = "merge",
    ) -> None: ...

    def find_album_quality_evidence(
        self,
        *,
        mb_release_id: str,
        snapshot_fingerprint: str,
    ) -> AlbumQualityEvidence | None: ...

    def load_album_quality_evidence_by_id(
        self, evidence_id: int | None,
    ) -> AlbumQualityEvidence | None: ...

    def set_import_job_candidate_evidence(
        self,
        import_job_id: int,
        evidence_id: int | None,
        *,
        expected_execution_lease: ExecutionLeaseSnapshot | None = None,
    ) -> bool: ...

    def set_download_log_candidate_evidence(
        self,
        download_log_id: int,
        evidence_id: int | None,
        *,
        direct_attribution: bool = False,
        contributor_usernames: Sequence[str] | None = None,
    ) -> None: ...

    def set_request_current_evidence(
        self,
        request_id: int,
        evidence_id: int | None,
        *,
        expected_status: str | None = None,
    ) -> bool: ...

    def get_import_job_candidate_evidence_id(
        self, import_job_id: int,
    ) -> int | None: ...

    def get_download_log_candidate_evidence_id(
        self, download_log_id: int,
    ) -> int | None: ...

    def get_request_current_evidence_id(
        self, request_id: int,
    ) -> int | None: ...


_AUDIO_EXTENSIONS = {
    ".aac",
    ".aiff",
    ".alac",
    ".ape",
    ".flac",
    ".m4a",
    ".mp3",
    ".ogg",
    ".opus",
    ".wav",
    ".wma",
}

_V0_SUBJECT: dict[str, EvidenceSubject] = {
    V0_PROBE_LOSSLESS_SOURCE: EVIDENCE_SUBJECT_SOURCE,
    V0_PROBE_NATIVE_LOSSY_RESEARCH: EVIDENCE_SUBJECT_INSTALLED,
    V0_PROBE_ON_DISK_RESEARCH: EVIDENCE_SUBJECT_INSTALLED,
}

SpectralWriteIntent = Literal["merge", "replace"]
CandidateSpectralAttemptOutcome = Literal[
    "not_attempted",
    "measured",
    "failed",
    "empty",
]


class CandidateEvidencePersistenceReceipt(
    msgspec.Struct,
    frozen=True,
    forbid_unknown_fields=True,
):
    """Durable witness that one exact candidate attempt reached its FK.

    The canonical evidence row is a shared content-addressed cache and may be
    dual-owned by candidate and current FKs.  This receipt records what this
    attempt actually established without pretending that cache eligibility is
    the same thing as persistence completion.
    """

    evidence_id: int
    snapshot_fingerprint: str
    spectral_write_intent: SpectralWriteIntent
    spectral_outcome: CandidateSpectralAttemptOutcome
    spectral_grade: str | None = None
    spectral_bitrate_kbps: int | None = None
    spectral_subject: EvidenceSubject | None = None
    spectral_provenance: EvidenceProvenance | None = None
    cliff_hz: int | None = None
    codec_family: CodecFamily | None = None
    ultrasonic_deficit_db: float | None = None
    spectral_measurement_version: int | None = None


_RECEIPT_SPECTRAL_FIELDS = (
    "spectral_grade",
    "spectral_bitrate_kbps",
    "spectral_subject",
    "spectral_provenance",
    "cliff_hz",
    "codec_family",
    "ultrasonic_deficit_db",
    "spectral_measurement_version",
)
_RECEIPT_CODEC_FAMILIES = frozenset({
    "mp3", "aac", "opus", "vorbis", "lossless", "other",
})
_POLICY_USABLE_SPECTRAL_GRADES = frozenset({
    "genuine",
    "marginal",
    "suspect",
    "likely_transcode",
})


def _is_nonempty_receipt_string(value: object) -> bool:
    return isinstance(value, str) and bool(value.strip())


def _is_receipt_string(value: object) -> bool:
    return isinstance(value, str)


def _is_valid_receipt_spectral_grade(value: object) -> bool:
    return isinstance(value, str) and value in _POLICY_USABLE_SPECTRAL_GRADES


def _is_valid_receipt_codec_family(value: object) -> bool:
    return (
        value is None
        or isinstance(value, str) and value in _RECEIPT_CODEC_FAMILIES
    )


def _is_finite_receipt_number(value: object) -> bool:
    return (
        not isinstance(value, bool)
        and isinstance(value, (int, float))
        and math.isfinite(value)
    )


def candidate_evidence_persistence_receipt_semantic_error(
    receipt: CandidateEvidencePersistenceReceipt,
) -> str | None:
    """Return why an exact-attempt receipt is semantically impossible.

    Strict Struct decoding proves only field types.  This validator is the one
    authority for the intent/outcome/tuple state machine at construction,
    preview completion, persisted JSONB decode, projection, and decision load.
    """
    if type(receipt.evidence_id) is not int or receipt.evidence_id <= 0:
        return "evidence_id must be positive"
    if not _is_nonempty_receipt_string(receipt.snapshot_fingerprint):
        return "snapshot_fingerprint must be non-empty"
    if (
        not _is_receipt_string(receipt.spectral_write_intent)
        or not _is_receipt_string(receipt.spectral_outcome)
    ):
        return "spectral intent and outcome must be strings"
    has_spectral_field = any(
        getattr(receipt, field) is not None
        for field in _RECEIPT_SPECTRAL_FIELDS
    )
    if (
        receipt.spectral_write_intent == "merge"
        and receipt.spectral_outcome == "not_attempted"
    ):
        if has_spectral_field:
            return "merge/not_attempted must not carry spectral fields"
        return None
    if receipt.spectral_write_intent != "replace":
        return "only merge/not_attempted or replace outcomes are valid"
    if receipt.spectral_outcome in {"failed", "empty"}:
        if has_spectral_field:
            return f"replace/{receipt.spectral_outcome} must not carry spectral fields"
        return None
    if receipt.spectral_outcome != "measured":
        return "replace requires measured, failed, or empty outcome"
    if not _is_valid_receipt_spectral_grade(receipt.spectral_grade):
        return "replace/measured requires a valid spectral grade"
    if receipt.spectral_subject != EVIDENCE_SUBJECT_SOURCE:
        return "replace/measured requires source spectral subject"
    if receipt.spectral_provenance != EVIDENCE_PROVENANCE_MEASURED:
        return "replace/measured requires measured spectral provenance"
    if (
        type(receipt.spectral_measurement_version) is not int
        or not spectral_measurement_generation_is_current(receipt)
    ):
        return "replace/measured requires the current analyzer generation"
    if (
        receipt.spectral_bitrate_kbps is not None
        and (
            type(receipt.spectral_bitrate_kbps) is not int
            or receipt.spectral_bitrate_kbps <= 0
        )
    ):
        return "replace/measured spectral bitrate must be positive"
    if (
        receipt.cliff_hz is not None
        and (type(receipt.cliff_hz) is not int or receipt.cliff_hz <= 0)
    ):
        return "replace/measured cliff must be positive"
    if not _is_valid_receipt_codec_family(receipt.codec_family):
        return "replace/measured codec family is invalid"
    if (
        receipt.ultrasonic_deficit_db is not None
        and not _is_finite_receipt_number(receipt.ultrasonic_deficit_db)
    ):
        return "replace/measured ultrasonic deficit must be finite"
    return None


class SnapshotAudioFilesError(OSError):
    """Raised when a source fileset cannot be snapshotted completely."""


@dataclass(frozen=True)
class EvidenceBuildResult:
    """Result of trying to build evidence from a fileset."""

    evidence: AlbumQualityEvidence | None
    status: str
    reason: str | None = None
    current_album_path: str | None = None
    persistence_receipt: CandidateEvidencePersistenceReceipt | None = None

    @property
    def available(self) -> bool:
        return self.evidence is not None


def spectral_measurement_generation_is_current(
    measurement: (
        AudioQualityMeasurement
        | SpectralAnalysisDetail
        | CandidateEvidencePersistenceReceipt
    ),
) -> bool:
    """Whether a spectral fact has this running analyzer's generation.

    This is a pure freshness check, not a policy-authorization decision.
    ``current_spectral_evidence_policy_usable`` additionally requires a
    recognized grade and admits the narrow preserved-source exception for
    bytes that the installed derivative cannot regenerate.
    """

    # Keep the producer as the authority for its generation stamp without
    # making every evidence consumer import the analyzer at module load time.
    from lib.spectral_check import SPECTRAL_MEASUREMENT_VERSION

    return (
        measurement.spectral_measurement_version
        == SPECTRAL_MEASUREMENT_VERSION
    )


def current_evidence_preserves_source_spectral(
    evidence: AlbumQualityEvidence,
) -> bool:
    """Whether the installed bytes are an irreplaceable lossy derivative.

    A source V0 anchor or lossless proof describes provenance, not whether the
    current library bytes can be measured again.  The cross-generation source
    exception is only sound when a recorded lossless source was converted into
    one known lossy installed codec.  The file extension is not enough here:
    an ``.m4a`` container can hold native ALAC as well as AAC.  Native
    lossless, mixed, and unresolved files remain remeasurable and must retain
    generation strictness.
    """

    converted_from = (evidence.measurement.was_converted_from or "").lower()
    if (
        not is_lossless_evidence_codec(converted_from)
        or evidence.measurement.spectral_subject != EVIDENCE_SUBJECT_SOURCE
        or not evidence.files
        or any(not file.container or not file.codec for file in evidence.files)
    ):
        return False
    containers = {file.container.lower() for file in evidence.files if file.container}
    # ``storage_format`` and the measurement are codec facts; snapshot
    # containers are not (notably, .m4a can be AAC or ALAC). Treat missing or
    # conflicting labels as unresolved rather than preserving an old source
    # grade, then authorize their exact shared media pair.
    formats = {
        label.strip().lower()
        for label in (evidence.storage_format, evidence.measurement.format)
        if label is not None and label.strip()
    }
    return (
        len(containers) == 1
        and len(formats) == 1
        and authoritative_lossy_media_pair(
            next(iter(containers)), next(iter(formats)),
        )
    )


def current_spectral_evidence_policy_usable(
    evidence: AlbumQualityEvidence,
) -> bool:
    """Whether a current-evidence spectral grade can reach policy.

    Current analyzer generation is normally required because the current
    bytes can be measured again.  R19's carried source subject is the narrow
    exception: its lossless source no longer exists, so a recognized grade is
    still policy evidence across a legacy, old, or future generation.  The
    stored subject and generation remain historical facts; this predicate
    only decides whether the tuple may be consumed.
    """

    measurement = evidence.measurement
    return (
        measurement.spectral_grade in _POLICY_USABLE_SPECTRAL_GRADES
        and (
            spectral_measurement_generation_is_current(measurement)
            or (
                measurement.spectral_subject == EVIDENCE_SUBJECT_SOURCE
                and current_evidence_preserves_source_spectral(evidence)
            )
        )
    )


def candidate_evidence_for_policy(
    evidence: AlbumQualityEvidence,
) -> AlbumQualityEvidence:
    """Project a canonical evidence row into candidate-source semantics."""
    measurement = evidence.measurement
    if measurement.was_converted_from is None:
        return evidence
    return msgspec.structs.replace(
        evidence,
        measurement=msgspec.structs.replace(
            measurement,
            was_converted_from=None,
        ),
    )


def candidate_evidence_from_persistence_receipt(
    evidence: AlbumQualityEvidence,
    receipt: CandidateEvidencePersistenceReceipt,
) -> AlbumQualityEvidence:
    """Project a dual-owned canonical row into this candidate attempt.

    A current-owned row may retain an irreplaceable source tuple in storage.
    The receipt is the exact-attempt witness that lets candidate policy see
    the fresh derivative tuple without rewriting that current audit history.
    """
    receipt_error = candidate_evidence_persistence_receipt_semantic_error(
        receipt
    )
    if receipt_error is not None:
        raise ValueError(f"receipt semantic invalid: {receipt_error}")
    candidate = candidate_evidence_for_policy(evidence)
    if receipt.spectral_write_intent != "replace":
        return candidate
    return msgspec.structs.replace(
        candidate,
        measurement=msgspec.structs.replace(
            candidate.measurement,
            spectral_grade=receipt.spectral_grade,
            spectral_bitrate_kbps=receipt.spectral_bitrate_kbps,
            spectral_subject=receipt.spectral_subject,
            spectral_provenance=receipt.spectral_provenance,
            cliff_hz=receipt.cliff_hz,
            codec_family=receipt.codec_family,
            ultrasonic_deficit_db=receipt.ultrasonic_deficit_db,
            spectral_measurement_version=(
                receipt.spectral_measurement_version
            ),
        ),
    )


def current_evidence_for_policy(
    evidence: AlbumQualityEvidence,
) -> AlbumQualityEvidence:
    """Withhold spectral tuples that current policy cannot consume.

    The stored tuple remains durable audit history.  An irreplaceable carried
    source grade remains policy-usable across generations; ordinary installed
    evidence still requires the running analyzer generation.
    """

    measurement = evidence.measurement
    has_spectral = (
        measurement.spectral_grade is not None
        or measurement.spectral_bitrate_kbps is not None
    )
    if (
        not has_spectral
        or current_spectral_evidence_policy_usable(evidence)
    ):
        return evidence
    return msgspec.structs.replace(
        evidence,
        measurement=msgspec.structs.replace(
            measurement,
            spectral_grade=None,
            spectral_bitrate_kbps=None,
            spectral_subject=None,
            spectral_provenance=None,
            cliff_hz=None,
            codec_family=None,
            ultrasonic_deficit_db=None,
            spectral_measurement_version=None,
        ),
    )


def current_evidence_rebuild_reasons(
    evidence: AlbumQualityEvidence,
) -> list[str]:
    """Return reasons a current-library snapshot must be measured again."""
    reasons = evidence.policy_incomplete_reasons()
    if evidence.lineage_version != CURRENT_EVIDENCE_LINEAGE_VERSION:
        reasons.append(
            f"lineage_version {evidence.lineage_version} must be rebuilt as "
            f"{CURRENT_EVIDENCE_LINEAGE_VERSION}"
        )
    measurement = evidence.measurement
    if (
        (
            measurement.spectral_grade is not None
            or measurement.spectral_bitrate_kbps is not None
        )
        and not spectral_measurement_generation_is_current(measurement)
        and not current_spectral_evidence_policy_usable(evidence)
    ):
        reasons.append(
            "spectral measurement generation is not current"
        )
    return reasons


def derive_folder_layout(files: list[AlbumQualityEvidenceFile]) -> str:
    """Return 'nested' if any snapshot file lives in a subdirectory.

    Pure helper used by U1's evidence-construction sites. ``relative_path``
    is always a relative POSIX-shaped path; a forward slash anywhere in it
    indicates a multi-disc / nested layout that the decision function
    rejects in U6.
    """

    for file in files:
        if "/" in file.relative_path:
            return "nested"
    return "flat"


def derive_filetype_band(files: list[AlbumQualityEvidenceFile]) -> str:
    """Classify a snapshot fileset into a coarse filetype band.

    Returns one of ``""`` (empty fileset), ``"flac"``, ``"mp3"``,
    ``"mixed_lossless"``, ``"mixed_lossy"``, or ``"mixed"`` (lossy + lossless
    combined). Container is the discriminator — codec is too noisy.
    """

    if not files:
        return ""
    containers = {file.container.lower() for file in files if file.container}
    if not containers:
        return ""
    if len(containers) == 1:
        return next(iter(containers))
    lossless_hits = containers & EVIDENCE_LOSSLESS_CONTAINERS
    lossy_hits = containers & EVIDENCE_LOSSY_CONTAINERS
    if lossless_hits and lossy_hits:
        return "mixed"
    if lossless_hits:
        return "mixed_lossless"
    if lossy_hits:
        return "mixed_lossy"
    return "mixed"


def snapshot_audio_files(root: str) -> list[AlbumQualityEvidenceFile]:
    """Build sorted active snapshot rows for audio files under ``root``."""

    if not os.path.isdir(root):
        return []
    files: list[AlbumQualityEvidenceFile] = []
    walk_errors: list[str] = []

    def onerror(exc: OSError) -> None:
        walk_errors.append(str(exc))

    for dirpath, _dirnames, filenames in os.walk(root, onerror=onerror):
        for filename in filenames:
            ext = os.path.splitext(filename)[1].lower()
            if ext not in _AUDIO_EXTENSIONS:
                continue
            full_path = os.path.join(dirpath, filename)
            try:
                stat = os.stat(full_path)
            except OSError as exc:
                raise SnapshotAudioFilesError(
                    f"could not stat audio file {full_path}: {exc}"
                ) from exc
            relative_path = os.path.relpath(full_path, root)
            container = ext.lstrip(".")
            files.append(
                AlbumQualityEvidenceFile(
                    relative_path=relative_path,
                    size_bytes=int(stat.st_size),
                    mtime_ns=int(stat.st_mtime_ns),
                    extension=container,
                    container=container,
                    codec=container,
                )
            )
    if walk_errors:
        raise SnapshotAudioFilesError("; ".join(walk_errors))
    return sorted(files, key=lambda f: f.relative_path)


def snapshot_fingerprint(files: list[AlbumQualityEvidenceFile]) -> str:
    """SHA-256 fingerprint of an audio inventory used as the evidence row key.

    This is the canonical addressing key for ``album_quality_evidence`` after
    the rekey landed in plan ``2026-05-16-002`` (U1/U2/U3). The exact formula
    is load-bearing: U2's SQL migration computes the same hash from each
    row's ``album_quality_evidence_files`` records, so a Python-vs-SQL drift
    here would scramble post-deploy lookup and break dedupe.

    Formula (must be mirrored exactly by U2's migration):

    1. For each file, build a tuple ``[relative_path, size_bytes, extension,
       container, codec]`` as a JSON array. ``codec`` may be ``None`` and is
       rendered as JSON ``null``.
    2. Sort the per-file tuples by ``relative_path`` ascending.
    3. JSON-encode the sorted list with ``sort_keys=False``,
       ``separators=(",", ":")`` (no whitespace), ``ensure_ascii=False``.
       Each file becomes e.g. ``["track01.flac",12345,"flac","flac","flac"]``.
    4. SHA-256 hex digest of the UTF-8 bytes of that JSON string.

    Fields chosen mirror ``_snapshot_match_key`` so freshness and identity
    stay coherent. ``mtime_ns`` is deliberately excluded — see the
    ``_snapshot_match_key`` docstring for why (ID3 tag mutation, virtiofs
    flake). ``decode_ok`` is excluded too: it is per-file evidence written
    by the measurement gate, not an identity attribute.

    The empty list hashes the JSON encoding of ``[]`` (``"[]"`` → a stable,
    defined 64-char digest), not an error.
    """

    payload: list[list[Any]] = sorted(
        (
            [
                file.relative_path,
                file.size_bytes,
                file.extension,
                file.container,
                file.codec,
            ]
            for file in files
        ),
        key=lambda row: row[0],
    )
    encoded = json.dumps(
        payload,
        sort_keys=False,
        separators=(",", ":"),
        ensure_ascii=False,
    )
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def fingerprint_album_path(album_path: str) -> str | None:
    """Fresh-Beets-authority content fingerprint of one album directory, or
    ``None`` when nothing is there to witness (#1089 NOTE-H / NOTE-I,
    review round 3).

    This is the sole high-level composition of ``snapshot_audio_files`` and
    ``snapshot_fingerprint`` for consumers that need to know whether an
    album path can currently witness an evidence fingerprint.

    Deliberately DIFFERENT from ``snapshot_fingerprint([])``'s own
    contract: that primitive treats the empty list as a well-defined,
    stable digest (its own docstring — a legitimate answer for a caller
    that wants to compare "zero files now" against "zero files before").
    This higher-level function is for a different question — "is this
    directory currently witnessable at all" — and answers ``None`` for a
    vanished or genuinely-empty album directory, never the empty-list
    digest: a linked evidence row whose OWN recorded fingerprint happens to
    be that exact empty-list digest (a plausible, if degenerate, historical
    value) must never silently read as "matches" against an album that is
    actually gone. An installed album with zero audio files is not a
    witnessable survivor.

    Raises ``SnapshotAudioFilesError`` exactly like
    ``snapshot_audio_files`` itself for a genuine walk/stat failure — this
    function does not swallow that; only the "walked cleanly and found
    nothing" case becomes ``None``.
    """
    files = snapshot_audio_files(album_path)
    if not files:
        return None
    return snapshot_fingerprint(files)


def _snapshot_match_key(
    file: AlbumQualityEvidenceFile,
) -> tuple[str, int, str, str, str | None]:
    """Stable identity tuple for snapshot equality.

    Excludes ``mtime_ns`` because virtiofs has been observed to return
    slightly different ``st_mtime_ns`` between back-to-back ``stat``
    calls on the same file. Size + path + extension/container/codec is
    sufficient to detect any content change that matters here.
    ``mtime_ns`` stays in the persisted struct as a forensic field but
    does not gate freshness.
    """
    return (
        file.relative_path,
        file.size_bytes,
        file.extension,
        file.container,
        file.codec,
    )


def audio_snapshot_matches(
    root: str,
    files: list[AlbumQualityEvidenceFile],
) -> bool:
    """Return whether ``root`` still has the recorded active audio snapshot.

    Compares on stable identity (path/size/codec) only. See
    :func:`_snapshot_match_key` for why ``mtime_ns`` is excluded.
    """

    if not os.path.isdir(root):
        return False
    try:
        current = snapshot_audio_files(root)
    except OSError:
        return False
    if not os.path.isdir(root):
        return False
    expected = sorted(files, key=lambda f: f.relative_path)
    return [_snapshot_match_key(f) for f in current] == [
        _snapshot_match_key(f) for f in expected
    ]


def neutral_v0_metric_from_probe(
    probe: V0ProbeEvidence | None,
) -> AlbumQualityV0Metric | None:
    """Convert legacy probe evidence into the neutral persisted shape."""

    if probe is None:
        return None
    try:
        subject = _V0_SUBJECT[probe.kind]
    except KeyError as exc:
        raise ValueError(f"unknown V0 probe kind: {probe.kind!r}") from exc
    return AlbumQualityV0Metric(
        subject=subject,
        provenance=EVIDENCE_PROVENANCE_MEASURED,
        min_bitrate_kbps=probe.min_bitrate_kbps,
        avg_bitrate_kbps=probe.avg_bitrate_kbps,
        median_bitrate_kbps=probe.median_bitrate_kbps,
    )


def audit_v0_probe_from_metric(
    metric: AlbumQualityV0Metric | None,
) -> V0ProbeEvidence | None:
    """Build ``V0ProbeEvidence`` from a persisted v0_metric for audit/log use.

    Returns a probe for *any* metric with the exact persisted audit kind for
    its source lineage. Policy code that needs a comparable probe must keep
    filtering via :func:`is_comparable_lossless_source_probe` — this helper
    exists so audit/UI surfaces can read a probe from *every* download,
    including native-lossy and on-disk research probes.
    """

    if metric is None:
        return None
    # download_log.v0_probe_kind CHECK constraint (migration 007) only
    # accepts the three persisted audit kinds. ``neutral_v0_research`` is an
    # in-memory policy marker and must never be written to the DB.
    kind = (
        V0_PROBE_LOSSLESS_SOURCE
        if metric.subject == EVIDENCE_SUBJECT_SOURCE
        else V0_PROBE_NATIVE_LOSSY_RESEARCH
    )
    return V0ProbeEvidence(
        kind=kind,
        min_bitrate_kbps=metric.min_bitrate_kbps,
        avg_bitrate_kbps=metric.avg_bitrate_kbps,
        median_bitrate_kbps=metric.median_bitrate_kbps,
    )


def _apply_measurement_facts_to_files(
    files: list[AlbumQualityEvidenceFile],
    measurement: PreimportMeasurement,
) -> list[AlbumQualityEvidenceFile]:
    """Stamp ``decode_ok=False`` on snapshot files listed in measurement.corrupt_files.

    ``snapshot_audio_files`` defaults ``decode_ok=True`` because the snapshot
    helper does not run ffmpeg. The preimport measurement is the authority on
    audio integrity, so when it reports corrupt files we propagate that fact
    into the snapshot rows before persisting evidence. This lets the
    importer's ``full_pipeline_decision_from_evidence`` (U11) consume
    ``decode_ok=False`` flags as the per-file evidence for ``audio_corrupt``.
    """
    if not measurement.corrupt_files:
        return files
    corrupt_set = set(measurement.corrupt_files)
    out: list[AlbumQualityEvidenceFile] = []
    for f in files:
        if f.relative_path in corrupt_set:
            out.append(AlbumQualityEvidenceFile(
                relative_path=f.relative_path,
                size_bytes=f.size_bytes,
                mtime_ns=f.mtime_ns,
                extension=f.extension,
                container=f.container,
                codec=f.codec,
                decode_ok=False,
            ))
        else:
            out.append(f)
    return out


def _filetype_band_to_format(filetype_band: str) -> str | None:
    """Derive an ``AudioQualityMeasurement.format`` label from a filetype band.

    Used for the measurement-only evidence path (audio_corrupt / bad_hash /
    nested / empty), where the harness never ran and there is no measured
    format string. ``None`` on an empty band is the honest answer (issue
    #1355 item 2): the importer's readiness gate no longer requires a format
    on these rows. For mixed filetypes we pick the dominant lossless/lossy
    container.
    """
    band = (filetype_band or "").strip().lower()
    if not band:
        return None
    if band in ("flac", "alac", "wav", "aiff", "ape"):
        return band.upper()
    if band in ("mp3", "aac", "m4a", "ogg", "opus", "wma"):
        return band.upper()
    if band == "mixed_lossless":
        return "FLAC"
    if band == "mixed_lossy":
        return "MP3"
    if band == "mixed":
        return "MP3"
    # Comma-separated extensions from inspect_local_files (e.g. "mp3, flac")
    first = band.split(",")[0].strip()
    if first:
        return first.upper()
    return None


def evidence_from_import_result(
    *,
    mb_release_id: str,
    source_path: str,
    import_result: ImportResult | None,
    measured_at: datetime | None = None,
    files: list[AlbumQualityEvidenceFile] | None = None,
    measurement: PreimportMeasurement | None = None,
) -> EvidenceBuildResult:
    """Build candidate evidence from an ``ImportResult`` and source folder.

    When ``measurement`` (a ``PreimportMeasurement``) is supplied, its U1
    facts (``audio_corrupt``, ``folder_layout``, ``audio_file_count``,
    ``filetype_band``, ``matched_bad_audio_hash_*``) override the values
    derived from the snapshot files. The measurement is the authority for
    these facts because it ran the real gates (ffmpeg decode, mp3val,
    bad-hash lookup) — the snapshot helper only knows file sizes and paths.
    """

    if import_result is not None and import_result.decision == "crash":
        # A crashed harness run emits whatever partial result it had built
        # when the exception fired — fields set before the crash look
        # complete while everything after it is silently absent (the
        # 2026-07-18 incident persisted proof-less candidate rows this
        # way). Fail closed: crashed results never become evidence.
        return EvidenceBuildResult(
            None,
            "crashed_result",
            import_result.error or "harness crashed mid-measurement",
        )
    if import_result is None or import_result.source_measurement is None:
        return EvidenceBuildResult(None, "incomplete", "missing source measurement")
    try:
        import_result.validate_new_row()
    except ValueError as exc:
        return EvidenceBuildResult(None, "incomplete", str(exc))
    if files is None:
        try:
            files = snapshot_audio_files(source_path)
        except OSError as exc:
            return EvidenceBuildResult(None, "failed", str(exc))
    if not files:
        return EvidenceBuildResult(None, "empty_fileset", "no audio files found")
    if measurement is not None and measurement.audio_corrupt:
        files = _apply_measurement_facts_to_files(files, measurement)
    audio_measurement = import_result.source_measurement
    target_contract = import_result.target_quality_contract
    # V3 target policy is owned by the harness result. The request row often
    # has no explicit target because the configured verified-lossless target
    # supplies it; trusting the request here loses the contract end-to-end.
    target_format = (
        target_contract.format if target_contract is not None else None
    )
    target_is_cbr = (
        target_contract.is_cbr if target_contract is not None else None
    )
    cd_rip_verification = (
        measurement.cd_rip_verification if measurement is not None else None
    )
    proof = (
        cd_rip_verification.verified_lossless_proof()
        if cd_rip_verification is not None
        else import_result.verified_lossless_proof
    )
    audio_corrupt = any(not file.decode_ok for file in files)
    if measurement is not None:
        audio_corrupt = audio_corrupt or measurement.audio_corrupt
        audio_error = measurement.audio_error
        folder_layout = measurement.folder_layout
        audio_file_count = (
            measurement.audio_file_count
            if measurement.audio_file_count else len(files)
        )
        filetype_band = (
            measurement.filetype_band or derive_filetype_band(files)
        )
        matched_bad_hash_id = measurement.matched_bad_hash_id
        matched_bad_hash_path = measurement.matched_bad_track_path
        aac_lattice = measurement.aac_lattice
    else:
        audio_error = None
        folder_layout = derive_folder_layout(files)
        audio_file_count = len(files)
        filetype_band = derive_filetype_band(files)
        matched_bad_hash_id = None
        matched_bad_hash_path = None
        aac_lattice = None
    evidence = AlbumQualityEvidence(
        mb_release_id=mb_release_id,
        snapshot_fingerprint=snapshot_fingerprint(files),
        source_path=source_path,
        measurement=audio_measurement,
        measured_at=measured_at or datetime.now(UTC),
        files=files,
        codec=files[0].codec,
        container=files[0].container,
        storage_format=audio_measurement.format,
        target_format=target_format,
        target_is_cbr=target_is_cbr,
        lineage_version=CURRENT_EVIDENCE_LINEAGE_VERSION,
        v0_metric=(
            neutral_v0_metric_from_probe(import_result.v0_probe)
        ),
        verified_lossless_proof=proof,
        cd_rip_verification=cd_rip_verification,
        audio_validation=(
            measurement.audio_validation
            if measurement is not None
            else None
        ) or legacy_unrecorded_audio_validation_report(),
        audio_corrupt=audio_corrupt,
        audio_error=audio_error,
        folder_layout=folder_layout,
        audio_file_count=audio_file_count,
        filetype_band=filetype_band,
        matched_bad_audio_hash_id=matched_bad_hash_id,
        matched_bad_audio_hash_path=matched_bad_hash_path,
        aac_lattice=aac_lattice,
    )
    errors = evidence.storage_validation_errors()
    if errors:
        return EvidenceBuildResult(None, "incomplete", "; ".join(errors))
    return EvidenceBuildResult(evidence, "ready")


def evidence_from_measurement(
    *,
    mb_release_id: str,
    source_path: str,
    measurement: PreimportMeasurement,
    measured_at: datetime | None = None,
    files: list[AlbumQualityEvidenceFile] | None = None,
) -> EvidenceBuildResult:
    """Build candidate evidence purely from a ``PreimportMeasurement``.

    Used by the preview worker when the harness cannot or should not run
    (audio_corrupt, bad_audio_hash, nested_layout, empty_fileset). The
    measurement carries every U1 fact the importer's
    ``full_pipeline_decision_from_evidence`` (U11) needs to reject:
    ``audio_corrupt``, ``matched_bad_audio_hash_*``, ``folder_layout``,
    ``audio_file_count``, and the spectral measurements.

    The synthesized ``AudioQualityMeasurement`` carries whatever real quality
    facts the measurement actually observed (a real per-file extension for
    ``format``, a caller-supplied bitrate hint) and leaves the rest ``None``
    (issue #1355 item 2) — it never invents a format or a bitrate the
    harness never ran to measure. ``full_pipeline_decision_from_evidence``
    rejects on one of the four U1 facts before it ever reads
    ``measurement``, and its readiness gate
    (``AlbumQualityEvidence.policy_incomplete_reasons``) no longer demands a
    quality measurement on a row that already carries one of those facts —
    see ``lib.quality.pipeline._require_evidence_ready`` and
    ``_load_candidate_evidence_for_source`` below.

    When ``audio_file_count=0`` and ``files`` is empty, returns ``empty_fileset``
    evidence — ``AlbumQualityEvidence.storage_validation_errors`` accepts this
    case (the explicit empty-inventory signal). There is no file to read a
    format off, so ``format`` is honestly ``None`` for that case.
    """

    if files is None:
        try:
            files = snapshot_audio_files(source_path)
        except OSError as exc:
            return EvidenceBuildResult(None, "failed", str(exc))
    files = _apply_measurement_facts_to_files(files, measurement)
    audio_file_count = (
        measurement.audio_file_count
        if measurement.audio_file_count else len(files)
    )
    # Synthesize a minimal AudioQualityMeasurement from only what was
    # actually observed. The importer rejects on the U1 facts (audio_corrupt,
    # nested, etc.) before ever reading these, and the readiness gate no
    # longer demands a format or bitrate on a row that carries one of those
    # facts (issue #1355 item 2) — so there is nothing left to invent.
    # ``filetype_band`` reflects a real per-file extension whenever files
    # exist; it is only ever empty for ``empty_fileset``, where there is no
    # file to read a format from.
    filetype_band = measurement.filetype_band or derive_filetype_band(files)
    format_label = _filetype_band_to_format(filetype_band)
    min_bitrate_kbps = measurement.min_bitrate_kbps
    download_spectral = measurement.download_spectral
    audio_measurement = AudioQualityMeasurement(
        min_bitrate_kbps=min_bitrate_kbps,
        avg_bitrate_kbps=min_bitrate_kbps,
        median_bitrate_kbps=min_bitrate_kbps,
        format=format_label,
        is_cbr=measurement.is_vbr is False,
        spectral_grade=(
            download_spectral.grade if download_spectral is not None else None
        ),
        spectral_bitrate_kbps=(
            download_spectral.bitrate_kbps if download_spectral is not None else None
        ),
        spectral_subject=(
            EVIDENCE_SUBJECT_SOURCE
            if download_spectral is not None and download_spectral.grade is not None
            else None
        ),
        spectral_provenance=(
            EVIDENCE_PROVENANCE_MEASURED
            if download_spectral is not None and download_spectral.grade is not None
            else None
        ),
        cliff_hz=(
            download_spectral.cliff_hz if download_spectral is not None else None
        ),
        codec_family=(
            download_spectral.codec_family if download_spectral is not None else None
        ),
        ultrasonic_deficit_db=(
            download_spectral.ultrasonic_deficit_db
            if download_spectral is not None else None
        ),
        spectral_measurement_version=(
            download_spectral.spectral_measurement_version
            if download_spectral is not None else None
        ),
    )
    codec = files[0].codec if files else None
    container = files[0].container if files else None
    evidence = AlbumQualityEvidence(
        mb_release_id=mb_release_id,
        snapshot_fingerprint=snapshot_fingerprint(files),
        source_path=source_path,
        measurement=audio_measurement,
        measured_at=measured_at or datetime.now(UTC),
        files=files,
        codec=codec,
        container=container,
        storage_format=audio_measurement.format,
        # This path exists only for facts rejected before target policy is
        # consulted. It has no projected files, so both target fields stay
        # absent instead of fabricating a bitrate mode.
        target_format=None,
        target_is_cbr=None,
        lineage_version=CURRENT_EVIDENCE_LINEAGE_VERSION,
        v0_metric=None,
        verified_lossless_proof=(
            measurement.cd_rip_verification.verified_lossless_proof()
            if measurement.cd_rip_verification is not None else None
        ),
        cd_rip_verification=measurement.cd_rip_verification,
        audio_validation=measurement.audio_validation,
        audio_corrupt=measurement.audio_corrupt,
        audio_error=measurement.audio_error,
        folder_layout=measurement.folder_layout,
        audio_file_count=audio_file_count,
        filetype_band=filetype_band,
        matched_bad_audio_hash_id=measurement.matched_bad_hash_id,
        matched_bad_audio_hash_path=measurement.matched_bad_track_path,
        aac_lattice=measurement.aac_lattice,
    )
    errors = evidence.storage_validation_errors()
    if errors:
        return EvidenceBuildResult(None, "incomplete", "; ".join(errors))
    return EvidenceBuildResult(evidence, "ready")


def evidence_from_album_info(
    *,
    mb_release_id: str,
    album_info: Any,
    verified_lossless_proof: VerifiedLosslessProof | None = None,
    cd_rip_verification: CdRipBitVerification | None = None,
    measured_at: datetime | None = None,
) -> EvidenceBuildResult:
    """Build current evidence only from Beets facts and explicit proof."""

    album_path = getattr(album_info, "album_path", "")
    try:
        files = snapshot_audio_files(str(album_path))
    except OSError as exc:
        return EvidenceBuildResult(None, "failed", str(exc))
    if not files:
        return EvidenceBuildResult(None, "empty_fileset", "no audio files found")
    carried_cd_rip = cd_rip_verification
    if (
        carried_cd_rip is not None
        and carried_cd_rip.provenance == EVIDENCE_PROVENANCE_MEASURED
    ):
        carried_cd_rip = msgspec.structs.replace(
            carried_cd_rip,
            provenance=EVIDENCE_PROVENANCE_CARRIED,
        )
    proof = (
        carried_cd_rip.verified_lossless_proof()
        if carried_cd_rip is not None
        else verified_lossless_proof
    )
    if proof is not None and proof.provenance == EVIDENCE_PROVENANCE_MEASURED:
        proof = msgspec.structs.replace(
            proof,
            provenance=EVIDENCE_PROVENANCE_CARRIED,
        )
    measurement = AudioQualityMeasurement(
        min_bitrate_kbps=getattr(album_info, "min_bitrate_kbps", None),
        avg_bitrate_kbps=getattr(album_info, "avg_bitrate_kbps", None),
        median_bitrate_kbps=getattr(album_info, "median_bitrate_kbps", None),
        format=getattr(album_info, "format", None) or None,
        is_cbr=bool(getattr(album_info, "is_cbr", False)),
    )
    evidence = AlbumQualityEvidence(
        mb_release_id=mb_release_id,
        snapshot_fingerprint=snapshot_fingerprint(files),
        source_path=str(album_path) or "",
        measurement=measurement,
        measured_at=measured_at or datetime.now(UTC),
        files=files,
        codec=files[0].codec,
        container=files[0].container,
        storage_format=measurement.format,
        lineage_version=CURRENT_EVIDENCE_LINEAGE_VERSION,
        v0_metric=None,
        verified_lossless_proof=proof,
        cd_rip_verification=carried_cd_rip,
        audio_corrupt=any(not file.decode_ok for file in files),
        folder_layout=derive_folder_layout(files),
        audio_file_count=len(files),
        filetype_band=derive_filetype_band(files),
    )
    errors = evidence.storage_validation_errors()
    if errors:
        return EvidenceBuildResult(None, "incomplete", "; ".join(errors))
    return EvidenceBuildResult(evidence, "ready")


def _candidate_spectral_persistence_shape(
    evidence: AlbumQualityEvidence,
    attempt: SpectralAnalysisDetail | None,
) -> tuple[
    SpectralWriteIntent,
    CandidateSpectralAttemptOutcome,
    int | None,
]:
    """Describe this attempt's spectral write without reading cache state."""
    if attempt is not None and attempt.attempted:
        if attempt.grade in _POLICY_USABLE_SPECTRAL_GRADES:
            outcome: CandidateSpectralAttemptOutcome = "measured"
        elif attempt.grade == "error" or attempt.error is not None:
            outcome = "failed"
        elif attempt.grade is not None:
            # An unknown non-error grade is not normalized into a successful
            # policy tuple. Leave it measured so the central receipt validator
            # rejects the producer result closed instead of erasing bad data.
            outcome = "measured"
        else:
            outcome = "empty"
        return (
            "replace",
            outcome,
            (
                attempt.spectral_measurement_version
                if outcome == "measured"
                else None
            ),
        )

    measurement = evidence.measurement
    if (
        measurement.spectral_grade is not None
        or measurement.spectral_bitrate_kbps is not None
    ):
        return (
            "replace",
            "measured",
            measurement.spectral_measurement_version,
        )
    return "merge", "not_attempted", None


def _candidate_evidence_with_exact_attempt_spectral(
    evidence: AlbumQualityEvidence,
    attempt: SpectralAnalysisDetail | None,
    outcome: CandidateSpectralAttemptOutcome,
) -> AlbumQualityEvidence:
    """Make the persisted tuple describe this attempt, including failure."""
    if attempt is None or not attempt.attempted:
        return evidence
    measured = outcome == "measured"
    grade = attempt.grade if measured else None
    return msgspec.structs.replace(
        evidence,
        measurement=msgspec.structs.replace(
            evidence.measurement,
            spectral_grade=grade,
            spectral_bitrate_kbps=(attempt.bitrate_kbps if measured else None),
            spectral_subject=(EVIDENCE_SUBJECT_SOURCE if grade is not None else None),
            spectral_provenance=(
                EVIDENCE_PROVENANCE_MEASURED if grade is not None else None
            ),
            cliff_hz=(attempt.cliff_hz if measured else None),
            codec_family=(attempt.codec_family if measured else None),
            ultrasonic_deficit_db=(
                attempt.ultrasonic_deficit_db if measured else None
            ),
            spectral_measurement_version=(
                attempt.spectral_measurement_version if measured else None
            ),
        ),
    )


def _candidate_persistence_receipt(
    evidence: AlbumQualityEvidence,
    *,
    evidence_id: int,
    snapshot_fingerprint: str,
    write_intent: SpectralWriteIntent,
    spectral_outcome: CandidateSpectralAttemptOutcome,
    generation: int | None,
) -> CandidateEvidencePersistenceReceipt:
    """Build the one exact-attempt receipt shape for pre/post-write checks."""
    measured = spectral_outcome == "measured"
    return CandidateEvidencePersistenceReceipt(
        evidence_id=evidence_id,
        snapshot_fingerprint=snapshot_fingerprint,
        spectral_write_intent=write_intent,
        spectral_outcome=spectral_outcome,
        spectral_grade=(
            evidence.measurement.spectral_grade if measured else None
        ),
        spectral_bitrate_kbps=(
            evidence.measurement.spectral_bitrate_kbps if measured else None
        ),
        spectral_subject=(
            evidence.measurement.spectral_subject if measured else None
        ),
        spectral_provenance=(
            evidence.measurement.spectral_provenance if measured else None
        ),
        cliff_hz=(evidence.measurement.cliff_hz if measured else None),
        codec_family=(
            evidence.measurement.codec_family if measured else None
        ),
        ultrasonic_deficit_db=(
            evidence.measurement.ultrasonic_deficit_db if measured else None
        ),
        spectral_measurement_version=generation,
    )


def _persist_candidate_evidence_result(
    db: QualityEvidenceDB,
    result: EvidenceBuildResult,
    *,
    attempt: SpectralAnalysisDetail | None,
    download_log_id: int | None,
    import_job_id: int | None,
) -> EvidenceBuildResult:
    evidence = result.evidence
    if evidence is None:
        return result
    write_intent, spectral_outcome, generation = (
        _candidate_spectral_persistence_shape(evidence, attempt)
    )
    evidence = _candidate_evidence_with_exact_attempt_spectral(
        evidence,
        attempt,
        spectral_outcome,
    )
    provisional_receipt = _candidate_persistence_receipt(
        evidence,
        evidence_id=1,
        snapshot_fingerprint=evidence.snapshot_fingerprint,
        write_intent=write_intent,
        spectral_outcome=spectral_outcome,
        generation=generation,
    )
    receipt_error = candidate_evidence_persistence_receipt_semantic_error(
        provisional_receipt
    )
    if receipt_error is not None:
        return EvidenceBuildResult(
            evidence,
            "failed",
            f"candidate receipt semantic invalid: {receipt_error}",
        )
    db.upsert_album_quality_evidence(
        evidence,
        spectral_write_intent=write_intent,
    )
    persisted = db.find_album_quality_evidence(
        mb_release_id=evidence.mb_release_id,
        snapshot_fingerprint=evidence.snapshot_fingerprint,
    )
    if persisted is None or persisted.id is None:
        return EvidenceBuildResult(
            evidence,
            "failed",
            "candidate evidence upsert did not produce a reloadable row",
        )
    receipt = _candidate_persistence_receipt(
        evidence,
        evidence_id=persisted.id,
        snapshot_fingerprint=persisted.snapshot_fingerprint,
        write_intent=write_intent,
        spectral_outcome=spectral_outcome,
        generation=generation,
    )
    receipt_error = candidate_evidence_persistence_receipt_semantic_error(
        receipt
    )
    if receipt_error is not None:
        return EvidenceBuildResult(
            persisted,
            "failed",
            f"candidate receipt semantic invalid: {receipt_error}",
        )
    if import_job_id is not None:
        linked = db.set_import_job_candidate_evidence(
            import_job_id,
            persisted.id,
        )
        if (
            not linked
            or db.get_import_job_candidate_evidence_id(import_job_id)
            != persisted.id
        ):
            return EvidenceBuildResult(
                evidence,
                "failed",
                "candidate evidence did not link to import job",
            )
    if download_log_id is not None:
        db.set_download_log_candidate_evidence(
            download_log_id,
            persisted.id,
            direct_attribution=True,
        )
        if (
            db.get_download_log_candidate_evidence_id(download_log_id)
            != persisted.id
        ):
            return EvidenceBuildResult(
                evidence,
                "failed",
                "candidate evidence did not link to download log",
            )
    return EvidenceBuildResult(
        persisted,
        "ready",
        persistence_receipt=receipt,
    )


def persist_candidate_evidence_from_import_result(
    db: QualityEvidenceDB,
    *,
    mb_release_id: str,
    source_path: str,
    import_result: ImportResult | None,
    download_log_id: int | None = None,
    import_job_id: int | None = None,
    files: list[AlbumQualityEvidenceFile] | None = None,
    measurement: PreimportMeasurement | None = None,
) -> EvidenceBuildResult:
    """Persist content-addressed candidate evidence and write addressing FKs.

    After upsert (keyed by ``(mb_release_id, snapshot_fingerprint)``), writes
    the surviving evidence row's id back to ``import_jobs.candidate_evidence_id``
    and/or ``download_log.candidate_evidence_id`` so triage and importer can
    look up evidence via FK chain.
    """
    if download_log_id is None and import_job_id is None:
        return EvidenceBuildResult(None, "unowned", "no persisted candidate owner")
    if files is None:
        try:
            files = snapshot_audio_files(source_path)
        except OSError as exc:
            return EvidenceBuildResult(None, "failed", str(exc))
    result = evidence_from_import_result(
        mb_release_id=mb_release_id,
        source_path=source_path,
        import_result=import_result,
        files=files,
        measurement=measurement,
    )
    candidate_attempt = (
        import_result.spectral.candidate
        if import_result is not None
        else None
    )
    return _persist_candidate_evidence_result(
        db,
        result,
        attempt=candidate_attempt,
        download_log_id=download_log_id,
        import_job_id=import_job_id,
    )


def persist_candidate_evidence_from_measurement(
    db: QualityEvidenceDB,
    *,
    mb_release_id: str,
    source_path: str,
    measurement: PreimportMeasurement,
    download_log_id: int | None = None,
    import_job_id: int | None = None,
    files: list[AlbumQualityEvidenceFile] | None = None,
) -> EvidenceBuildResult:
    """Persist measurement-only candidate evidence (no ImportResult required).

    Mirrors ``persist_candidate_evidence_from_import_result`` for the preview
    code path that never invoked the harness (audio_corrupt / bad_audio_hash /
    nested_layout / empty_fileset). The importer's
    ``full_pipeline_decision_from_evidence`` (U11) reads the persisted U1
    facts and rejects via its four-fact early-exit branches upstream of the
    quality gate.
    """
    if download_log_id is None and import_job_id is None:
        return EvidenceBuildResult(None, "unowned", "no persisted candidate owner")
    if files is None:
        try:
            files = snapshot_audio_files(source_path)
        except OSError as exc:
            return EvidenceBuildResult(None, "failed", str(exc))
    result = evidence_from_measurement(
        mb_release_id=mb_release_id,
        source_path=source_path,
        measurement=measurement,
        files=files,
    )
    return _persist_candidate_evidence_result(
        db,
        result,
        attempt=measurement.spectral_audit.candidate,
        download_log_id=download_log_id,
        import_job_id=import_job_id,
    )


def propagate_candidate_evidence_to_current(
    db: QualityEvidenceDB,
    *,
    request_id: int,
    candidate_evidence: AlbumQualityEvidence,
    album_info: AlbumInfo,
    measured_at: datetime | None = None,
) -> EvidenceBuildResult:
    """Build new library-side evidence by propagating candidate measurement payload.

    Post-import propagation path. Acquisition facts carry by their explicit
    subject marker; installed facts never cross a fingerprint change.

    Field policy:

    * Always re-derived from the library snapshot: ``snapshot_fingerprint``,
      ``source_path``, ``files``, ``codec``, ``container``, ``storage_format``,
      ``folder_layout``, ``audio_file_count``, ``filetype_band``,
      ``audio_corrupt`` (from files[*].decode_ok), ``measured_at`` (now).
    * Always re-derived from ``album_info``: ``min_bitrate_kbps``,
      ``avg_bitrate_kbps``, ``median_bitrate_kbps``, ``format``, ``is_cbr``.
      Beets's per-track bitrate measurements describe the on-disk files at
      the library path — for renamed-only this is the same audio as the
      candidate's measurement (a dual-check that catches drift); for
      transcoded imports this describes the V0/Opus output.
    * Source-subject spectral and V0 facts carry with provenance ``carried``.
    * Installed-subject facts are dropped and measured again on the installed
      snapshot by the ordinary enrichment path.
    * Verified-lossless proof carries with provenance ``carried``.
    * The AAC frame lattice does NOT carry (issue #829 PR-A). It is a fact
      about the exact candidate bytes, and the library row is a different
      snapshot — usually the transcoded output, which carries the target
      codec's lattice or none at all. Carrying it would be the R19 mistake
      in a new column.
    """

    from lib.beets_db import exact_release_identity_matches

    # Validate ownership before observing the installed files, and most
    # importantly before either evidence mutation below. A candidate FK is
    # not authority for a different exact pressing.
    request_row = db.get_request(request_id)
    if request_row is None:
        return EvidenceBuildResult(
            None,
            "stale_request",
            "request disappeared before current evidence propagation",
        )
    requested_release_id = str(request_row.get("mb_release_id") or "")
    if not exact_release_identity_matches(
        requested_release_id,
        candidate_evidence.mb_release_id,
    ):
        return EvidenceBuildResult(
            None,
            "identity_mismatch",
            "candidate evidence exact release identity does not match request",
        )

    album_path = getattr(album_info, "album_path", "")
    try:
        files = snapshot_audio_files(str(album_path))
    except OSError as exc:
        return EvidenceBuildResult(None, "failed", str(exc))
    if not files:
        return EvidenceBuildResult(None, "empty_fileset", "no audio files found")

    source_codec = (candidate_evidence.codec or "").lower() or None
    library_codec_from_files = files[0].codec
    library_codec = (library_codec_from_files or "").lower() or None
    is_transcode = (
        source_codec is not None
        and library_codec is not None
        and source_codec != library_codec
    )
    candidate_measurement = candidate_evidence.measurement
    measured_source_format = (
        candidate_measurement.format or source_codec or ""
    ).strip().lower()
    output_source_format = (
        measured_source_format if is_transcode else None
    )
    reduced_format = album_info.format.strip().lower()
    album_formats = frozenset(
        value.strip().lower()
        for value in album_info.formats_on_disk
        if value.strip()
    )
    if not album_formats and reduced_format:
        album_formats = frozenset({reduced_format})
    album_format_is_authoritative = (
        len(album_formats) == 1 and reduced_format in album_formats
    )
    carry_spectral = (
        candidate_measurement.spectral_grade is not None
        and candidate_measurement.spectral_subject == EVIDENCE_SUBJECT_SOURCE
        and album_format_is_authoritative
    )
    carried_v0 = (
        msgspec.structs.replace(
            candidate_evidence.v0_metric,
            provenance=EVIDENCE_PROVENANCE_CARRIED,
        )
        if candidate_evidence.v0_metric is not None
        and candidate_evidence.v0_metric.subject == EVIDENCE_SUBJECT_SOURCE
        else None
    )
    carried_proof = (
        msgspec.structs.replace(
            candidate_evidence.verified_lossless_proof,
            provenance=EVIDENCE_PROVENANCE_CARRIED,
        )
        if candidate_evidence.verified_lossless_proof is not None
        else None
    )
    carried_cd_rip = (
        msgspec.structs.replace(
            candidate_evidence.cd_rip_verification,
            provenance=EVIDENCE_PROVENANCE_CARRIED,
        )
        if candidate_evidence.cd_rip_verification is not None
        else None
    )
    measurement = AudioQualityMeasurement(
        min_bitrate_kbps=getattr(album_info, "min_bitrate_kbps", None),
        avg_bitrate_kbps=getattr(album_info, "avg_bitrate_kbps", None),
        median_bitrate_kbps=getattr(album_info, "median_bitrate_kbps", None),
        format=getattr(album_info, "format", None) or None,
        is_cbr=bool(getattr(album_info, "is_cbr", False)),
        spectral_grade=(
            candidate_measurement.spectral_grade if carry_spectral else None
        ),
        spectral_bitrate_kbps=(
            candidate_measurement.spectral_bitrate_kbps if carry_spectral else None
        ),
        spectral_subject=(EVIDENCE_SUBJECT_SOURCE if carry_spectral else None),
        spectral_provenance=(
            EVIDENCE_PROVENANCE_CARRIED if carry_spectral else None
        ),
        # issue #829 Phase 5 PR1 — these are one atomic fact alongside
        # spectral_grade above (same measurement pass), so they carry under
        # the exact same gate.
        cliff_hz=(
            candidate_measurement.cliff_hz if carry_spectral else None
        ),
        codec_family=(
            candidate_measurement.codec_family if carry_spectral else None
        ),
        ultrasonic_deficit_db=(
            candidate_measurement.ultrasonic_deficit_db
            if carry_spectral else None
        ),
        spectral_measurement_version=(
            candidate_measurement.spectral_measurement_version
            if carry_spectral else None
        ),
        was_converted_from=output_source_format,
    )

    library_filetype_band = derive_filetype_band(files)
    library_container_from_files = files[0].container

    evidence = AlbumQualityEvidence(
        mb_release_id=requested_release_id,
        snapshot_fingerprint=snapshot_fingerprint(files),
        source_path=str(album_path) or "",
        measurement=measurement,
        measured_at=measured_at or datetime.now(UTC),
        files=files,
        codec=library_codec_from_files,
        container=library_container_from_files,
        storage_format=measurement.format,
        target_format=None,
        lineage_version=CURRENT_EVIDENCE_LINEAGE_VERSION,
        v0_metric=carried_v0,
        verified_lossless_proof=carried_proof,
        cd_rip_verification=carried_cd_rip,
        audio_corrupt=any(not file.decode_ok for file in files),
        folder_layout=derive_folder_layout(files),
        audio_file_count=len(files),
        filetype_band=library_filetype_band,
        matched_bad_audio_hash_id=None,
        matched_bad_audio_hash_path=None,
    )
    errors = evidence.storage_validation_errors()
    if errors:
        return EvidenceBuildResult(None, "incomplete", "; ".join(errors))

    db.upsert_album_quality_evidence(evidence)
    persisted = db.find_album_quality_evidence(
        mb_release_id=evidence.mb_release_id,
        snapshot_fingerprint=evidence.snapshot_fingerprint,
    )
    if persisted is not None and persisted.id is not None:
        expected_status = str(request_row["status"])
        if expected_status == "replaced" or not db.set_request_current_evidence(
            request_id,
            persisted.id,
            expected_status=expected_status,
        ):
            return EvidenceBuildResult(
                evidence,
                "stale_request",
                "request state changed before current evidence link",
            )
    return EvidenceBuildResult(evidence, "ready")


def backfill_current_evidence_from_album_info(
    db: QualityEvidenceDB,
    *,
    request_id: int,
    mb_release_id: str,
    album_info: Any,
    verified_lossless_proof: VerifiedLosslessProof | None = None,
    preserve_existing_verified_lossless_proof: bool = True,
) -> EvidenceBuildResult:
    """Build current evidence from beets, upsert, and write request FK.

    Identity is ``(mb_release_id, snapshot_fingerprint)``. Once persisted the
    surviving row id is written to ``album_requests.current_evidence_id`` so
    downstream readers can fetch via FK rather than scanning by mbid.
    """
    request_row = db.get_request(request_id)
    existing_id = db.get_request_current_evidence_id(request_id)
    existing = (
        db.load_album_quality_evidence_by_id(existing_id)
        if existing_id is not None
        else None
    )
    from lib.beets_db import exact_release_identity_matches

    if existing is not None and not exact_release_identity_matches(
        mb_release_id,
        existing.mb_release_id,
    ):
        # A poisoned/stale FK contributes no facts whatsoever, even when its
        # byte fingerprint happens to equal the requested album's snapshot.
        existing = None
    carried_cd_rip: CdRipBitVerification | None = None
    if (
        verified_lossless_proof is None
        and preserve_existing_verified_lossless_proof
        and existing is not None
        and existing.verified_lossless_proof is not None
        and existing.verified_lossless_proof.source
        and existing.verified_lossless_proof.classifier
    ):
        verified_lossless_proof = msgspec.structs.replace(
            existing.verified_lossless_proof,
            provenance=EVIDENCE_PROVENANCE_CARRIED,
        )
        if existing.cd_rip_verification is not None:
            carried_cd_rip = msgspec.structs.replace(
                existing.cd_rip_verification,
                provenance=EVIDENCE_PROVENANCE_CARRIED,
            )
    result = evidence_from_album_info(
        mb_release_id=mb_release_id,
        album_info=album_info,
        verified_lossless_proof=verified_lossless_proof,
        cd_rip_verification=carried_cd_rip,
    )
    if result.evidence is not None and existing is not None:
        existing_measurement = existing.measurement
        same_snapshot = (
            existing.snapshot_fingerprint == result.evidence.snapshot_fingerprint
        )
        carry_spectral = (
            existing_measurement.spectral_grade is not None
            and existing_measurement.spectral_subject == EVIDENCE_SUBJECT_SOURCE
        )
        measurement = result.evidence.measurement
        if same_snapshot and existing_measurement.was_converted_from is not None:
            # Conversion lineage is a current-library fact. Preserve it only
            # while rebuilding the same installed snapshot; the generic
            # evidence upsert must remain exact so a fresh candidate NULL can
            # clear legacy candidate contamination at the same address.
            measurement = msgspec.structs.replace(
                measurement,
                was_converted_from=existing_measurement.was_converted_from,
            )
        if carry_spectral:
            # issue #829 Phase 5 PR1: cliff_hz/codec_family/
            # ultrasonic_deficit_db/spectral_measurement_version are one
            # atomic fact alongside spectral_grade (same measurement pass)
            # — they must carry together or the upsert's atomic-pair guard
            # (keyed on EXCLUDED.spectral_grade IS NOT NULL) nulls them out
            # over a stored good value the instant a carrying grade lands.
            measurement = msgspec.structs.replace(
                measurement,
                spectral_grade=existing_measurement.spectral_grade,
                spectral_bitrate_kbps=existing_measurement.spectral_bitrate_kbps,
                spectral_subject=EVIDENCE_SUBJECT_SOURCE,
                spectral_provenance=EVIDENCE_PROVENANCE_CARRIED,
                cliff_hz=existing_measurement.cliff_hz,
                codec_family=existing_measurement.codec_family,
                ultrasonic_deficit_db=existing_measurement.ultrasonic_deficit_db,
                spectral_measurement_version=(
                    existing_measurement.spectral_measurement_version
                ),
            )
        elif (
            same_snapshot
            and existing_measurement.spectral_grade is not None
            and existing_measurement.spectral_subject == EVIDENCE_SUBJECT_INSTALLED
            and existing_measurement.spectral_provenance
            == EVIDENCE_PROVENANCE_MEASURED
        ):
            # Same-address repair: identical bytes, so the installed
            # measurement is still a true statement about them — preserve it
            # verbatim (installed keeps provenance 'measured' per the
            # cross-product rule; facts are invalidated by byte change, not
            # by row repair). Ambiguous/off-vocabulary facts still drop —
            # they cannot legally exist on a v4 row. The four #829 PR1
            # capture fields are the same atomic fact as spectral_grade
            # here too (see the carry_spectral branch above).
            measurement = msgspec.structs.replace(
                measurement,
                spectral_grade=existing_measurement.spectral_grade,
                spectral_bitrate_kbps=existing_measurement.spectral_bitrate_kbps,
                spectral_subject=EVIDENCE_SUBJECT_INSTALLED,
                spectral_provenance=EVIDENCE_PROVENANCE_MEASURED,
                cliff_hz=existing_measurement.cliff_hz,
                codec_family=existing_measurement.codec_family,
                ultrasonic_deficit_db=existing_measurement.ultrasonic_deficit_db,
                spectral_measurement_version=(
                    existing_measurement.spectral_measurement_version
                ),
            )
        existing_v0 = existing.v0_metric
        has_v0_values = existing_v0 is not None and any(
            value is not None
            for value in (
                existing_v0.min_bitrate_kbps,
                existing_v0.avg_bitrate_kbps,
                existing_v0.median_bitrate_kbps,
            )
        )
        carried_v0 = None
        if (
            existing_v0 is not None
            and has_v0_values
            and existing_v0.subject == EVIDENCE_SUBJECT_SOURCE
        ):
            carried_v0 = msgspec.structs.replace(
                existing_v0,
                provenance=EVIDENCE_PROVENANCE_CARRIED,
            )
        elif (
            same_snapshot
            and existing_v0 is not None
            and has_v0_values
            and existing_v0.subject == EVIDENCE_SUBJECT_INSTALLED
            and existing_v0.provenance == EVIDENCE_PROVENANCE_MEASURED
        ):
            # Same-address repair preserves the installed research anchor —
            # dropping it while `on_disk_v0_research_attempted` stays True
            # would blind the async researcher forever (the deploy-night
            # Seabear regression).
            carried_v0 = existing_v0
        result = EvidenceBuildResult(
            msgspec.structs.replace(
                result.evidence,
                measurement=measurement,
                # A same-address v4 repair keeps the historical capture time.
                measured_at=(
                    existing.measured_at
                    if same_snapshot
                    else result.evidence.measured_at
                ),
                # Source-subject acquisition facts survive every rebuild;
                # valid installed facts survive a same-address repair (the
                # bytes are unchanged); a fingerprint change drops installed
                # facts for re-measurement — and resets the research marker
                # (fresh build) so the async researcher re-fills the anchor.
                v0_metric=carried_v0,
                on_disk_v0_research_attempted=(
                    existing.on_disk_v0_research_attempted
                    if same_snapshot
                    else result.evidence.on_disk_v0_research_attempted
                ),
                current_enrichment_required=(
                    existing.current_enrichment_required
                    if same_snapshot
                    else True
                ),
            ),
            result.status,
            result.reason,
        )
    if result.evidence is not None:
        db.upsert_album_quality_evidence(result.evidence)
        persisted = db.find_album_quality_evidence(
            mb_release_id=result.evidence.mb_release_id,
            snapshot_fingerprint=result.evidence.snapshot_fingerprint,
        )
        if persisted is not None and persisted.id is not None:
            if request_row is None:
                return EvidenceBuildResult(
                    result.evidence,
                    "stale_request",
                    "request disappeared before current evidence link",
                )
            expected_status = str(request_row["status"])
            if expected_status == "replaced" or not db.set_request_current_evidence(
                request_id,
                persisted.id,
                expected_status=expected_status,
            ):
                return EvidenceBuildResult(
                    result.evidence,
                    "stale_request",
                    "request state changed before current evidence link",
                )
    return result


def _load_candidate_evidence_for_source(
    db: QualityEvidenceDB,
    *,
    source_path: str,
    download_log_id: int | None = None,
    import_job_id: int | None = None,
    admission: Literal["cache", "decision"],
    persistence_receipt: CandidateEvidencePersistenceReceipt | None = None,
) -> EvidenceBuildResult:
    """Load stored candidate evidence under one explicit admission policy.

    Walks explicit ownership only: ``import_jobs.candidate_evidence_id`` when
    ``import_job_id`` is provided, then ``download_log.candidate_evidence_id``.
    It never falls back to another job on the same request. Once a candidate
    evidence row is found, ``audio_snapshot_matches`` confirms it still
    describes the audio at ``source_path``. Cache admission remains generation
    strict. Decision admission may carry a concrete pre-import fact to the
    unified decider because no spectral comparison can affect that fact.
    """

    if download_log_id is None and import_job_id is None:
        return EvidenceBuildResult(None, "unowned", "no candidate owner")

    evidence_id: int | None = None
    if import_job_id is not None:
        evidence_id = db.get_import_job_candidate_evidence_id(import_job_id)
    if evidence_id is None and download_log_id is not None:
        evidence_id = db.get_download_log_candidate_evidence_id(download_log_id)

    if evidence_id is None:
        return EvidenceBuildResult(
            None,
            "missing",
            "no candidate evidence found via FK chain",
        )

    evidence = db.load_album_quality_evidence_by_id(evidence_id)
    if evidence is None:
        return EvidenceBuildResult(
            None,
            "missing",
            f"candidate evidence id {evidence_id} not found",
        )
    # A content-addressed row can be linked by both candidate and current
    # owners. Keep installed conversion history in storage, but never expose
    # that output-only fact as part of a candidate source measurement.
    evidence = candidate_evidence_for_policy(evidence)
    if persistence_receipt is not None:
        receipt_error = candidate_evidence_persistence_receipt_semantic_error(
            persistence_receipt
        )
        if receipt_error is not None:
            return EvidenceBuildResult(
                None,
                "incomplete",
                f"candidate receipt semantic invalid: {receipt_error}",
            )
        if (
            persistence_receipt.evidence_id != evidence.id
            or persistence_receipt.snapshot_fingerprint
                != evidence.snapshot_fingerprint
        ):
            return EvidenceBuildResult(
                None,
                "incomplete",
                "candidate persistence receipt does not match evidence row",
            )
        try:
            evidence = candidate_evidence_from_persistence_receipt(
                evidence,
                persistence_receipt,
            )
        except ValueError as exc:
            return EvidenceBuildResult(None, "incomplete", str(exc))
    if not audio_snapshot_matches(source_path, evidence.files):
        return EvidenceBuildResult(
            None,
            "stale",
            "candidate source changed since evidence capture",
        )
    measurement = evidence.measurement
    preimport_fact = candidate_preimport_reject_fact(evidence)
    # A reject-fact row is honestly unmeasured on purpose (issue #1355 item
    # 2, ``lib.quality_evidence.evidence_from_measurement``) — the decision
    # this evidence feeds rejects on that fact before it ever reads quality.
    errors = evidence.policy_incomplete_reasons(
        require_quality_measurement=preimport_fact is None,
    )
    if (
        persistence_receipt is not None
        and persistence_receipt.spectral_outcome in {"failed", "empty"}
        and preimport_fact is None
    ):
        errors.append(
            "candidate spectral attempt did not produce policy evidence"
        )
    spectral_generation_stale = (
        (
            measurement.spectral_grade is not None
            or measurement.spectral_bitrate_kbps is not None
        )
        and not spectral_measurement_generation_is_current(measurement)
    )
    # A measured CD-rip bit verification makes the PRODUCER skip the spectral
    # gate outright (``lib/measurement.py``: `if cd_rip_verification is None
    # and _needs_spectral_check(...)`), so no re-measurement of these bytes can
    # ever advance this row's spectral generation. Demanding a current one is
    # unsatisfiable by construction, and the requeue it triggers cannot change
    # its own precondition: the importer requeues to preview, preview declines
    # to re-grade, and the pair never converges (#1162 — job 60635 ran 2,463
    # passes over 39.2 h, its successor another 238, both ended from outside).
    # Admitting it is also safe rather than merely expedient: the proof is
    # strictly stronger evidence than the spectral estimate it displaces, and
    # the decider already lets it outrank a stale grade, so nothing reaches
    # policy here that the verified-lossless proof does not already dominate.
    # ``carried`` provenance is deliberately excluded: every writer that puts a
    # CD-rip fact on an installed/converted row rewrites it to ``carried``
    # (``evidence_from_album_info``), and that row's source-subject grade
    # describes the PRE-CONVERSION bytes, which must never reach candidate
    # policy. Only a proof measured from these exact bytes witnesses the
    # producer's own bypass.
    #
    # This closes the CD-rip instance ONLY. ``_needs_spectral_check`` also
    # skips every codec that is neither MP3 nor a lossless candidate
    # (AAC/Vorbis/Opus/WMA), stranding a stale grade the same way — but there
    # the grade is decision-relevant, because the dominating proof that makes
    # admission safe here exists only on lossless bytes. Bypassing it for a
    # lossy candidate would trade a livelock for a silent wrong reject, so
    # that instance needs the unusable grade cleared rather than admitted and
    # is tracked separately (#1167).
    cd_rip_bypasses_spectral = (
        evidence.cd_rip_verification is not None
        and evidence.cd_rip_verification.provenance
        == EVIDENCE_PROVENANCE_MEASURED
    )
    if spectral_generation_stale and not cd_rip_bypasses_spectral and not (
        admission == "decision"
        and preimport_fact is not None
    ):
        errors.append("spectral measurement generation is not current")
    if errors:
        return EvidenceBuildResult(None, "incomplete", "; ".join(errors))
    return EvidenceBuildResult(evidence, "ready")


def load_candidate_evidence_for_source(
    db: QualityEvidenceDB,
    *,
    source_path: str,
    download_log_id: int | None = None,
    import_job_id: int | None = None,
) -> EvidenceBuildResult:
    """Load generation-current candidate evidence for cache reuse."""
    return _load_candidate_evidence_for_source(
        db,
        source_path=source_path,
        download_log_id=download_log_id,
        import_job_id=import_job_id,
        admission="cache",
    )


def load_candidate_evidence_for_decision(
    db: QualityEvidenceDB,
    *,
    source_path: str,
    download_log_id: int | None = None,
    import_job_id: int | None = None,
    persistence_receipt: CandidateEvidencePersistenceReceipt | None = None,
) -> EvidenceBuildResult:
    """Load exact-attempt evidence that is valid input to the real decider.

    Unlike the reuse loader, this permits a persisted, snapshot-matched early
    fact through when a historical spectral tuple is stale. It never makes the
    quality decision itself, and spectral-dependent evidence remains subject
    to the same current-generation gate as cache reuse.
    """
    return _load_candidate_evidence_for_source(
        db,
        source_path=source_path,
        download_log_id=download_log_id,
        import_job_id=import_job_id,
        admission="decision",
        persistence_receipt=persistence_receipt,
    )


def load_or_backfill_current_evidence(
    db: QualityEvidenceDB,
    *,
    request_id: int,
    mb_release_id: str,
    quality_ranks: Any = None,
    preloaded_evidence: AlbumQualityEvidence | None = None,
    preloaded: bool = False,
    beets_library_db_path: str | None = None,
    beets_library_root: str = "",
    current_release: CurrentBeetsUnique | None = None,
) -> EvidenceBuildResult:
    """Resolve Beets freshly, then load or rebuild the exact current snapshot."""

    from lib.beets_db import (
        BeetsDB,
        CurrentBeetsAmbiguous,
        CurrentBeetsMissing,
        album_info_from_current,
        exact_release_identity_matches,
        release_identity_for_lookup,
    )
    from lib.quality import QualityRankConfig

    if current_release is None:
        identity = release_identity_for_lookup(mb_release_id)
        if identity is None:
            return EvidenceBuildResult(
                None,
                "failed",
                f"invalid exact release identity {mb_release_id!r}",
            )
        if beets_library_db_path is None:
            beets_handle = BeetsDB(library_root=beets_library_root)
        else:
            beets_handle = BeetsDB(
                beets_library_db_path,
                library_root=beets_library_root,
            )
        with beets_handle as beets:
            resolution = beets.resolve_current_release(identity)
        if isinstance(resolution, CurrentBeetsMissing):
            return EvidenceBuildResult(
                None,
                "empty_current",
                "exact album not in beets",
            )
        if isinstance(resolution, CurrentBeetsAmbiguous):
            return EvidenceBuildResult(
                None,
                "ambiguous_current",
                "ambiguous current Beets authority: "
                f"{resolution.reason}; album_ids={resolution.album_ids}",
            )
        current_release = resolution

    expected_identity = release_identity_for_lookup(mb_release_id)
    if expected_identity is None or current_release.identity != expected_identity:
        return EvidenceBuildResult(
            None,
            "failed",
            "current Beets resolution identity does not match evidence request",
            current_album_path=current_release.album_path,
        )

    current_album_path = current_release.album_path
    try:
        current_files = snapshot_audio_files(current_album_path)
    except OSError as exc:
        return EvidenceBuildResult(
            None,
            "failed",
            f"{type(exc).__name__}: {exc}",
            current_album_path=current_album_path,
        )
    if not current_files:
        return EvidenceBuildResult(
            None,
            "failed",
            "current Beets album has no audio files",
            current_album_path=current_album_path,
        )
    current_fingerprint = snapshot_fingerprint(current_files)

    if preloaded:
        existing = preloaded_evidence
        if existing is not None:
            linked_id = db.get_request_current_evidence_id(request_id)
            if existing.id is None or linked_id != existing.id:
                existing = None
    else:
        existing_id = db.get_request_current_evidence_id(request_id)
        existing = (
            db.load_album_quality_evidence_by_id(existing_id)
            if existing_id is not None
            else None
        )
    if existing is not None:
        errors = current_evidence_rebuild_reasons(existing)
        if (
            not errors
            and exact_release_identity_matches(
                mb_release_id,
                existing.mb_release_id,
            )
            and existing.snapshot_fingerprint == current_fingerprint
        ):
            return EvidenceBuildResult(
                existing,
                "ready",
                current_album_path=current_album_path,
            )

    cfg = quality_ranks if quality_ranks is not None else QualityRankConfig.defaults()
    album_info = album_info_from_current(current_release, cfg)
    if album_info is None:
        return EvidenceBuildResult(
            None,
            "failed",
            "unique current Beets album has no usable bitrate metadata",
            current_album_path=current_album_path,
        )

    rebuilt = backfill_current_evidence_from_album_info(
        db,
        request_id=request_id,
        mb_release_id=mb_release_id,
        album_info=album_info,
    )
    if (
        rebuilt.evidence is not None
        and rebuilt.evidence.snapshot_fingerprint != current_fingerprint
    ):
        return EvidenceBuildResult(
            None,
            "stale",
            "current Beets snapshot changed during evidence rebuild",
            current_album_path=current_album_path,
        )
    return EvidenceBuildResult(
        rebuilt.evidence,
        rebuilt.status,
        rebuilt.reason,
        current_album_path=current_album_path,
    )
