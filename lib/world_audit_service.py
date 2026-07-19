"""Read-only cross-engine invariant audit over PipelineDB, Beets, and disk."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Sequence
from typing import Any, Protocol, runtime_checkable

import msgspec

from lib.beets_db import BeetsWorldAlbum
from lib.quality import AlbumQualityEvidence
from lib.quality_evidence import snapshot_audio_files, snapshot_fingerprint
from lib.release_identity import ReleaseIdentity
from lib.world_invariants import (
    DenylistAuthoritySnapshot,
    EvidenceDiskSnapshot,
    LibraryAlbumSnapshot,
    RequestMembershipSnapshot,
    WorldViolation,
    check_denylist_authority,
    check_evidence_disk_coherence,
    check_folder_exclusivity,
    check_library_filesystem,
    check_status_membership,
    derive_denylist_authorities,
)


AUDITED_INVARIANTS = (
    "folder_exclusivity",
    "library_filesystem",
    "status_membership",
    "evidence_disk_coherence",
    "denylist_authority",
)

TEMPORAL_INVARIANTS_NOT_AUDITABLE = (
    "replaced_row_frozen_after_supersede",
    "proof_lock_terminality_across_operation",
    "no_lossy_tier_widening_across_operation",
)


class WorldAuditCounts(msgspec.Struct, frozen=True):
    active_requests: int
    beets_albums: int
    linked_evidence: int
    denylist_rows: int
    violations: int


class WorldAuditReport(msgspec.Struct, frozen=True):
    status: str
    counts: WorldAuditCounts
    audited_invariants: tuple[str, ...]
    temporal_invariants_not_auditable: tuple[str, ...]
    violations: tuple[WorldViolation, ...]


@runtime_checkable
class WorldAuditPipelineDB(Protocol):
    def list_non_replaced_requests(self) -> list[dict[str, Any]]: ...

    def load_album_quality_evidence_by_id(
        self,
        evidence_id: int | None,
    ) -> AlbumQualityEvidence | None: ...

    def get_download_history_batch(
        self,
        request_ids: list[int],
    ) -> dict[int, list[dict[str, Any]]]: ...

    def list_denylist_rows(self) -> list[dict[str, Any]]: ...


@runtime_checkable
class WorldAuditBeetsDB(Protocol):
    def list_world_albums(self) -> list[BeetsWorldAlbum]: ...


def _release_id(row: dict[str, Any]) -> str | None:
    identity = ReleaseIdentity.from_fields(
        row.get("mb_release_id"),
        row.get("discogs_release_id"),
    )
    return identity.release_id if identity is not None else None


def _current_evidence_id(row: dict[str, Any]) -> int | None:
    raw = row.get("current_evidence_id")
    return int(raw) if isinstance(raw, int) else None


def _fingerprint(album: BeetsWorldAlbum) -> str:
    return snapshot_fingerprint(snapshot_audio_files(album.album_path))


def _sorted_violations(
    violations: Sequence[WorldViolation],
) -> tuple[WorldViolation, ...]:
    return tuple(sorted(
        violations,
        key=lambda violation: (
            violation.code,
            violation.request_id or -1,
            violation.release_id or "",
            violation.album_ids,
            violation.detail,
        ),
    ))


def audit_world(
    pipeline_db: WorldAuditPipelineDB,
    beets_db: WorldAuditBeetsDB,
) -> WorldAuditReport:
    """Evaluate every invariant that a read-only current-state scan can prove.

    Temporal invariants remain listed explicitly in the report: current rows
    cannot prove what happened across an earlier mutation, and a clean audit
    must never imply that those transition-only properties were evaluated.
    """
    raw_albums = beets_db.list_world_albums()
    violations: list[WorldViolation] = []
    albums: list[LibraryAlbumSnapshot] = []
    membership_albums: list[LibraryAlbumSnapshot] = []
    by_release_id: dict[str, list[BeetsWorldAlbum]] = defaultdict(list)

    for album in raw_albums:
        if not album.release_ids:
            violations.append(WorldViolation(
                code="beets_identity_missing",
                detail=(
                    f"beets album {album.album_id} has no exact MusicBrainz "
                    "or Discogs release identity"
                ),
                album_ids=(album.album_id,),
            ))
            canonical_id = "<missing-release-identity>"
        else:
            canonical_id = album.release_ids[0]
        snapshot = LibraryAlbumSnapshot(
            album_id=album.album_id,
            release_id=canonical_id,
            album_path=album.album_path,
            item_paths=album.item_paths,
        )
        albums.append(snapshot)
        for release_id in album.release_ids:
            by_release_id[release_id].append(album)
            membership_albums.append(LibraryAlbumSnapshot(
                album_id=album.album_id,
                release_id=release_id,
                album_path=album.album_path,
                item_paths=album.item_paths,
            ))

    requests = pipeline_db.list_non_replaced_requests()
    request_ids = [int(row["id"]) for row in requests]
    denylist_rows = pipeline_db.list_denylist_rows()
    denylist_request_ids = sorted({
        int(row["request_id"])
        for row in denylist_rows
    })
    histories = pipeline_db.get_download_history_batch(denylist_request_ids)
    memberships: list[RequestMembershipSnapshot] = []
    evidence_snapshots: list[EvidenceDiskSnapshot] = []
    fingerprint_failures: set[int] = set()
    fingerprint_cache: dict[int, str] = {}
    linked_evidence_count = 0

    for row in requests:
        request_id = int(row["id"])
        release_id = _release_id(row)
        if release_id is None:
            violations.append(WorldViolation(
                code="request_identity_missing",
                detail=f"active request {request_id} has no exact release identity",
                request_id=request_id,
            ))
            continue
        memberships.append(RequestMembershipSnapshot(
            request_id=request_id,
            release_id=release_id,
            status=str(row.get("status") or ""),
            imported_path=(
                str(row["imported_path"])
                if row.get("imported_path") is not None
                else None
            ),
        ))

        matches = by_release_id.get(release_id, [])
        if len(matches) > 1:
            violations.append(WorldViolation(
                code="evidence_audit_ambiguous",
                detail=(
                    f"request {request_id} release {release_id!r} resolves "
                    f"to multiple Beets albums {tuple(a.album_id for a in matches)!r}"
                ),
                request_id=request_id,
                release_id=release_id,
                album_ids=tuple(sorted(album.album_id for album in matches)),
            ))
            continue
        album = matches[0] if matches else None
        current_id = _current_evidence_id(row)
        linked = pipeline_db.load_album_quality_evidence_by_id(current_id)
        if current_id is not None:
            linked_evidence_count += 1

        actual_fingerprint: str | None = None
        if album is not None:
            try:
                actual_fingerprint = fingerprint_cache.get(album.album_id)
                if actual_fingerprint is None:
                    actual_fingerprint = _fingerprint(album)
                    fingerprint_cache[album.album_id] = actual_fingerprint
            except OSError as exc:
                fingerprint_failures.add(request_id)
                violations.append(WorldViolation(
                    code="album_fingerprint_unavailable",
                    detail=(
                        f"request {request_id} album {album.album_id} could "
                        f"not be snapshotted: {exc}"
                    ),
                    request_id=request_id,
                    release_id=release_id,
                    album_ids=(album.album_id,),
                ))

        evidence_snapshots.append(EvidenceDiskSnapshot(
            request_id=request_id,
            release_id=release_id,
            status=str(row.get("status") or ""),
            album_path=album.album_path if album is not None else None,
            current_evidence_id=current_id,
            evidence_id=linked.id if linked is not None else None,
            evidence_release_id=(
                linked.mb_release_id if linked is not None else None
            ),
            evidence_source_path=(linked.source_path if linked is not None else None),
            evidence_fingerprint=(
                linked.snapshot_fingerprint if linked is not None else None
            ),
            actual_fingerprint=actual_fingerprint,
        ))

    denylist_snapshots: list[DenylistAuthoritySnapshot] = []
    for row in denylist_rows:
        request_id = int(row["request_id"])
        history = histories.get(request_id, [])
        username = str(row.get("username") or "")
        denylist_snapshots.append(DenylistAuthoritySnapshot(
            request_id=request_id,
            username=username,
            authorizing_decisions=derive_denylist_authorities(
                username=username,
                reason=str(row.get("reason") or ""),
                history=history,
            ),
        ))

    violations.extend(check_folder_exclusivity(albums))
    violations.extend(check_library_filesystem(albums))
    violations.extend(check_status_membership(memberships, membership_albums))
    violations.extend(
        violation
        for violation in check_evidence_disk_coherence(evidence_snapshots)
        if not (
            violation.code == "evidence_fingerprint_mismatch"
            and violation.request_id in fingerprint_failures
        )
    )
    violations.extend(check_denylist_authority(denylist_snapshots))
    rendered = _sorted_violations(violations)
    return WorldAuditReport(
        status="clean" if not rendered else "violations",
        counts=WorldAuditCounts(
            active_requests=len(requests),
            beets_albums=len(raw_albums),
            linked_evidence=linked_evidence_count,
            denylist_rows=len(denylist_snapshots),
            violations=len(rendered),
        ),
        audited_invariants=AUDITED_INVARIANTS,
        temporal_invariants_not_auditable=TEMPORAL_INVARIANTS_NOT_AUDITABLE,
        violations=rendered,
    )


__all__ = [
    "AUDITED_INVARIANTS",
    "TEMPORAL_INVARIANTS_NOT_AUDITABLE",
    "WorldAuditBeetsDB",
    "WorldAuditCounts",
    "WorldAuditPipelineDB",
    "WorldAuditReport",
    "audit_world",
]
