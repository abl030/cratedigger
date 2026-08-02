"""Read-only cross-engine invariant audit over PipelineDB, Beets, and disk."""

from __future__ import annotations

import sqlite3
from collections.abc import Callable, Mapping, Sequence
from typing import TYPE_CHECKING, Any, Literal, Never, Protocol, runtime_checkable

import msgspec

if TYPE_CHECKING:
    from lib.pipeline_db.rows import AlbumRequestRow, DownloadLogWithEvidenceRow

from lib.beets_db import (
    BeetsWorldAlbum,
    CurrentBeetsAmbiguous,
    CurrentBeetsResolution,
    CurrentBeetsUnique,
)
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
    world_violation_bucket,
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
    bucket_a: int = 0
    bucket_b: int = 0
    bucket_c: int = 0

    @property
    def violations(self) -> int:
        """Internal aggregate for non-public generic consumers."""

        return self.bucket_a + self.bucket_b + self.bucket_c


class WorldAuditGroup(msgspec.Struct, frozen=True):
    bucket: Literal["A", "B", "C"]
    owner: str
    count: int
    members: tuple[WorldViolation, ...]


class WorldAuditGroups(msgspec.Struct, frozen=True):
    a: WorldAuditGroup
    b: WorldAuditGroup
    c: WorldAuditGroup


class WorldAuditReport(msgspec.Struct, frozen=True):
    status: str
    complete: bool
    counts: WorldAuditCounts
    audited_invariants: tuple[str, ...]
    temporal_invariants_not_auditable: tuple[str, ...]
    groups: WorldAuditGroups

    @property
    def violations(self) -> tuple[WorldViolation, ...]:
        """Internal aggregate; public JSON remains grouped by owner."""

        return (
            self.groups.a.members
            + self.groups.b.members
            + self.groups.c.members
        )


class BeetsAuthorityUnavailable(msgspec.Struct, frozen=True):
    """Closed expected failure while opening or querying Beets authority."""

    category: str


@runtime_checkable
class WorldAuditPipelineDB(Protocol):
    def list_non_replaced_requests(self) -> list[AlbumRequestRow]: ...

    def load_album_quality_evidence_by_id(
        self,
        evidence_id: int | None,
    ) -> AlbumQualityEvidence | None: ...

    def get_download_history_batch(
        self,
        request_ids: list[int],
    ) -> dict[int, list[DownloadLogWithEvidenceRow]]: ...

    def list_denylist_rows(self) -> list[dict[str, Any]]: ...


@runtime_checkable
class WorldAuditBeetsDB(Protocol):
    def list_world_albums(self) -> list[BeetsWorldAlbum]: ...

    def resolve_current_releases(
        self,
        identities: list[ReleaseIdentity],
    ) -> dict[ReleaseIdentity, CurrentBeetsResolution]: ...

    def close(self) -> None: ...


type WorldAuditBeetsFactory = Callable[[], WorldAuditBeetsDB]


class _BeetsQueryUnavailable(Exception):
    def __init__(self, category: str) -> None:
        super().__init__(category)
        self.category = category


_SQLITE_AUTHORITY_AVAILABILITY_CODES = frozenset({
    sqlite3.SQLITE_AUTH,
    sqlite3.SQLITE_BUSY,
    sqlite3.SQLITE_CANTOPEN,
    sqlite3.SQLITE_IOERR,
    sqlite3.SQLITE_LOCKED,
    sqlite3.SQLITE_PERM,
})


def _beets_authority_availability_category(exc: Exception) -> str | None:
    if isinstance(exc, FileNotFoundError):
        return "FileNotFoundError"
    if isinstance(exc, PermissionError):
        return "PermissionError"
    if not isinstance(exc, sqlite3.OperationalError):
        return None
    raw_code = getattr(exc, "sqlite_errorcode", None)
    if not isinstance(raw_code, int):
        return None
    primary_code = raw_code & 0xFF
    if primary_code not in _SQLITE_AUTHORITY_AVAILABILITY_CODES:
        return None
    return f"sqlite_{primary_code}"


def _translate_beets_query_failure(exc: Exception) -> Never:
    category = _beets_authority_availability_category(exc)
    if category is None:
        raise exc
    raise _BeetsQueryUnavailable(category) from exc


class _AvailabilityMediatedBeetsDB:
    """Translate only failures raised by the two Beets query calls."""

    def __init__(self, beets: WorldAuditBeetsDB) -> None:
        self._beets = beets

    def list_world_albums(self) -> list[BeetsWorldAlbum]:
        try:
            return self._beets.list_world_albums()
        except Exception as exc:  # noqa: BLE001 - closed classifier re-raises
            _translate_beets_query_failure(exc)

    def resolve_current_releases(
        self,
        identities: list[ReleaseIdentity],
    ) -> dict[ReleaseIdentity, CurrentBeetsResolution]:
        try:
            return self._beets.resolve_current_releases(identities)
        except Exception as exc:  # noqa: BLE001 - closed classifier re-raises
            _translate_beets_query_failure(exc)

    def close(self) -> None:
        self._beets.close()


def _current_evidence_id(row: Mapping[str, Any]) -> int | None:
    raw = row.get("current_evidence_id")
    return int(raw) if isinstance(raw, int) else None


def _fingerprint(album_path: str) -> str:
    return snapshot_fingerprint(snapshot_audio_files(album_path))


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


_GROUP_OWNERS = {
    "A": "cratedigger_integrity",
    "B": "current_holdings_projection",
    "C": "beets_library",
}


def build_world_audit_report(
    *,
    counts: WorldAuditCounts,
    violations: Sequence[WorldViolation],
    complete: bool = True,
) -> WorldAuditReport:
    """Build the one deterministic public ownership presentation."""

    grouped: dict[Literal["A", "B", "C"], list[WorldViolation]] = {
        "A": [],
        "B": [],
        "C": [],
    }
    for violation in _sorted_violations(violations):
        grouped[world_violation_bucket(violation.code)].append(violation)

    def make_group(bucket: Literal["A", "B", "C"]) -> WorldAuditGroup:
        members = tuple(grouped[bucket])
        return WorldAuditGroup(
            bucket=bucket,
            owner=_GROUP_OWNERS[bucket],
            count=len(members),
            members=members,
        )

    groups = WorldAuditGroups(
        a=make_group("A"),
        b=make_group("B"),
        c=make_group("C"),
    )
    if groups.a.count:
        status = "integrity_failed"
    elif groups.b.count or groups.c.count:
        status = "observations_only"
    else:
        status = "clean"
    return WorldAuditReport(
        status=status,
        complete=complete,
        counts=WorldAuditCounts(
            active_requests=counts.active_requests,
            beets_albums=counts.beets_albums,
            linked_evidence=counts.linked_evidence,
            denylist_rows=counts.denylist_rows,
            bucket_a=groups.a.count,
            bucket_b=groups.b.count,
            bucket_c=groups.c.count,
        ),
        audited_invariants=AUDITED_INVARIANTS,
        temporal_invariants_not_auditable=TEMPORAL_INVARIANTS_NOT_AUDITABLE,
        groups=groups,
    )


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

    requests = pipeline_db.list_non_replaced_requests()
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
    identified_requests: list[tuple[Mapping[str, Any], int, ReleaseIdentity]] = []

    for row in requests:
        request_id = int(row["id"])
        identity = ReleaseIdentity.from_strict_fields(
            row.get("mb_release_id"),
            row.get("discogs_release_id"),
        )
        if identity is None:
            violations.append(WorldViolation(
                code="request_identity_missing",
                detail=f"active request {request_id} has no exact release identity",
                request_id=request_id,
            ))
            continue
        identified_requests.append((row, request_id, identity))

    resolutions = beets_db.resolve_current_releases([
        identity for _row, _request_id, identity in identified_requests
    ])
    resolutions_by_release_id = {
        identity.release_id: resolution
        for identity, resolution in resolutions.items()
    }

    for row, request_id, identity in identified_requests:
        release_id = identity.release_id
        memberships.append(RequestMembershipSnapshot(
            request_id=request_id,
            release_id=release_id,
            status=str(row.get("status") or ""),
        ))

        resolution = resolutions[identity]
        if isinstance(resolution, CurrentBeetsAmbiguous):
            continue
        current = resolution if isinstance(resolution, CurrentBeetsUnique) else None
        current_id = _current_evidence_id(row)
        linked = pipeline_db.load_album_quality_evidence_by_id(current_id)
        if current_id is not None:
            linked_evidence_count += 1

        actual_fingerprint: str | None = None
        if current is not None:
            try:
                actual_fingerprint = fingerprint_cache.get(current.album_id)
                if actual_fingerprint is None:
                    actual_fingerprint = _fingerprint(current.album_path)
                    fingerprint_cache[current.album_id] = actual_fingerprint
            except OSError as exc:
                fingerprint_failures.add(request_id)
                violations.append(WorldViolation(
                    code="album_fingerprint_unavailable",
                    detail=(
                        f"request {request_id} album {current.album_id} could "
                        f"not be snapshotted: {exc}"
                    ),
                    request_id=request_id,
                    release_id=release_id,
                    album_ids=(current.album_id,),
                ))

        evidence_snapshots.append(EvidenceDiskSnapshot(
            request_id=request_id,
            release_id=release_id,
            status=str(row.get("status") or ""),
            album_path=current.album_path if current is not None else None,
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
    violations.extend(check_status_membership(
        memberships,
        resolutions_by_release_id,
    ))
    violations.extend(
        violation
        for violation in check_evidence_disk_coherence(evidence_snapshots)
        if not (
            violation.code == "evidence_fingerprint_mismatch"
            and violation.request_id in fingerprint_failures
        )
    )
    violations.extend(check_denylist_authority(denylist_snapshots))
    return build_world_audit_report(
        counts=WorldAuditCounts(
            active_requests=len(requests),
            beets_albums=len(raw_albums),
            linked_evidence=linked_evidence_count,
            denylist_rows=len(denylist_snapshots),
        ),
        violations=violations,
    )


def _open_beets_authority(
    beets_factory: WorldAuditBeetsFactory,
) -> WorldAuditBeetsDB | BeetsAuthorityUnavailable:
    try:
        return beets_factory()
    except Exception as exc:
        category = _beets_authority_availability_category(exc)
        if category is None:
            raise
        return BeetsAuthorityUnavailable(category=category)


def _audit_world_with_mediated_beets(
    pipeline_db: WorldAuditPipelineDB,
    beets: WorldAuditBeetsDB,
) -> WorldAuditReport | BeetsAuthorityUnavailable:
    try:
        return audit_world(pipeline_db, _AvailabilityMediatedBeetsDB(beets))
    except _BeetsQueryUnavailable as exc:
        return BeetsAuthorityUnavailable(category=exc.category)


def _unavailable_beets_report(category: str) -> WorldAuditReport:
    return build_world_audit_report(
        counts=WorldAuditCounts(0, 0, 0, 0),
        violations=(WorldViolation(
            code="current_beets_authority_unavailable",
            detail=f"current Beets authority unavailable ({category})",
        ),),
        complete=False,
    )


def audit_world_from_factory(
    pipeline_db: WorldAuditPipelineDB,
    beets_factory: WorldAuditBeetsFactory,
) -> WorldAuditReport:
    """Own Beets open/query/close and type only expected unavailability."""

    opened = _open_beets_authority(beets_factory)
    if isinstance(opened, BeetsAuthorityUnavailable):
        return _unavailable_beets_report(opened.category)
    try:
        result = _audit_world_with_mediated_beets(pipeline_db, opened)
    finally:
        opened.close()
    if isinstance(result, WorldAuditReport):
        return result
    return _unavailable_beets_report(result.category)


def audit_world_from_borrowed_factory(
    pipeline_db: WorldAuditPipelineDB,
    beets_factory: WorldAuditBeetsFactory,
) -> WorldAuditReport:
    """Mediate a server-owned Beets handle without closing its lifecycle."""

    opened = _open_beets_authority(beets_factory)
    if isinstance(opened, BeetsAuthorityUnavailable):
        return _unavailable_beets_report(opened.category)
    result = _audit_world_with_mediated_beets(pipeline_db, opened)
    if isinstance(result, WorldAuditReport):
        return result
    return _unavailable_beets_report(result.category)


__all__ = [
    "AUDITED_INVARIANTS",
    "TEMPORAL_INVARIANTS_NOT_AUDITABLE",
    "BeetsAuthorityUnavailable",
    "WorldAuditBeetsDB",
    "WorldAuditBeetsFactory",
    "WorldAuditCounts",
    "WorldAuditGroup",
    "WorldAuditGroups",
    "WorldAuditPipelineDB",
    "WorldAuditReport",
    "audit_world",
    "audit_world_from_borrowed_factory",
    "audit_world_from_factory",
    "build_world_audit_report",
]
