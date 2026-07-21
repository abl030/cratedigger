"""Orchestration for the verified-lossless album sidecar (issue #184).

``write_sidecar_for_request`` is the single entry point both the importer
success hook (``lib/dispatch/``) and the one-shot backfill call — no
parallel code paths. It loads the request's current (library-side) evidence,
gates on verified-lossless, resolves the on-disk album folder via beets, and
atomically writes ``cratedigger.json``.

The sidecar is derived state, so this is idempotent: re-running rebuilds the
same file from the same evidence. That is also the long-term-preservation
answer — if beets ever clobbers the file, regenerate by re-running the
backfill.
"""

from __future__ import annotations

import logging
import os
import tempfile
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Protocol

import msgspec

from lib.quality import QualityRankConfig
from lib.quality_evidence import (
    QualityEvidenceDB,
    SnapshotAudioFilesError,
    snapshot_audio_files,
    snapshot_fingerprint,
)
from lib.sidecar import SIDECAR_FILENAME, build_sidecar, should_write_sidecar

if TYPE_CHECKING:
    from lib.beets_db import CurrentBeetsResolution
    from lib.release_identity import ReleaseIdentity

logger = logging.getLogger("cratedigger")

OUTCOME_WRITTEN = "written"
OUTCOME_SKIPPED_NO_EVIDENCE = "skipped_no_evidence"
OUTCOME_SKIPPED_NOT_VERIFIED_LOSSLESS = "skipped_not_verified_lossless"
OUTCOME_SKIPPED_NO_ALBUM_PATH = "skipped_no_album_path"
OUTCOME_SKIPPED_CURRENT_AMBIGUOUS = "skipped_current_ambiguous"
OUTCOME_SKIPPED_EVIDENCE_IDENTITY = "skipped_evidence_identity_mismatch"
OUTCOME_SKIPPED_EVIDENCE_STALE = "skipped_evidence_stale"


class SidecarDB(QualityEvidenceDB, Protocol):
    """PipelineDB surface this service reads. ``PipelineDB`` and
    ``FakePipelineDB`` satisfy it structurally."""

    def get_recent_successful_uploader(self, request_id: int) -> str | None: ...


class SidecarBeets(Protocol):
    """BeetsDB surface this service reads (positional-only to ignore the
    ``cfg``/``_cfg`` param-name split between real and fake)."""

    def resolve_current_release(
        self, identity: "ReleaseIdentity", /,
    ) -> "CurrentBeetsResolution": ...


@dataclass(frozen=True)
class SidecarWriteResult:
    """Outcome of one sidecar write attempt. ``path`` is set only on success."""

    outcome: str
    path: str | None = None


def write_sidecar_for_request(
    db: SidecarDB,
    beets: SidecarBeets,
    request_id: int,
    *,
    mb_release_id: str,
    quality_ranks: QualityRankConfig | None = None,
    generated_at: datetime | None = None,
) -> SidecarWriteResult:
    """Write/refresh the verified-lossless sidecar for one imported album."""
    evidence = db.load_album_quality_evidence_by_id(
        db.get_request_current_evidence_id(request_id)
    )
    if evidence is None:
        return SidecarWriteResult(OUTCOME_SKIPPED_NO_EVIDENCE)
    if not should_write_sidecar(evidence):
        return SidecarWriteResult(OUTCOME_SKIPPED_NOT_VERIFIED_LOSSLESS)

    from lib.beets_db import (
        CurrentBeetsAmbiguous,
        CurrentBeetsMissing,
        exact_release_identity_matches,
        release_identity_for_lookup,
    )

    identity = release_identity_for_lookup(mb_release_id)
    if identity is None or not exact_release_identity_matches(
        mb_release_id,
        evidence.mb_release_id,
    ):
        return SidecarWriteResult(OUTCOME_SKIPPED_EVIDENCE_IDENTITY)
    current = beets.resolve_current_release(identity)
    if isinstance(current, CurrentBeetsMissing):
        return SidecarWriteResult(OUTCOME_SKIPPED_NO_ALBUM_PATH)
    if isinstance(current, CurrentBeetsAmbiguous):
        return SidecarWriteResult(OUTCOME_SKIPPED_CURRENT_AMBIGUOUS)
    album_path = current.album_path
    if not album_path or not os.path.isdir(album_path):
        return SidecarWriteResult(OUTCOME_SKIPPED_NO_ALBUM_PATH)

    # Self-validate against disk: the sidecar must faithfully describe the
    # bytes next to it. If the current evidence no longer matches the on-disk
    # audio — e.g. the post-import evidence refresh failed and
    # current_evidence_id still points at a prior row — skip rather than
    # publish a stale payload. snapshot_fingerprint mirrors how
    # propagate_candidate_evidence_to_current derived the row's fingerprint.
    try:
        on_disk = snapshot_audio_files(album_path)
    except SnapshotAudioFilesError:
        return SidecarWriteResult(OUTCOME_SKIPPED_NO_ALBUM_PATH)
    if snapshot_fingerprint(on_disk) != evidence.snapshot_fingerprint:
        return SidecarWriteResult(OUTCOME_SKIPPED_EVIDENCE_STALE)

    sidecar = build_sidecar(
        evidence,
        source_username=db.get_recent_successful_uploader(request_id),
        generated_at=(
            generated_at if generated_at is not None
            else datetime.now(timezone.utc)
        ),
    )
    path = os.path.join(album_path, SIDECAR_FILENAME)
    _atomic_write_bytes(path, msgspec.json.encode(sidecar))
    return SidecarWriteResult(OUTCOME_WRITTEN, path)


# World-readable: the sidecar exists to be read by other processes and peers
# (slskd reshare, the operator, other Cratedigger instances). mkstemp creates
# 0600, which would make the file useless for that purpose, so widen it to the
# conventional 0644 for a generated read-only metadata file.
_SIDECAR_FILE_MODE = 0o644


def _atomic_write_bytes(path: str, data: bytes) -> None:
    """Write ``data`` to ``path`` via a same-dir temp file + ``os.replace``."""
    directory = os.path.dirname(path)
    fd, tmp_path = tempfile.mkstemp(
        dir=directory, prefix=".cratedigger-", suffix=".tmp"
    )
    try:
        with os.fdopen(fd, "wb") as fh:
            fh.write(data)
        os.chmod(tmp_path, _SIDECAR_FILE_MODE)
        os.replace(tmp_path, path)
    except BaseException:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise
