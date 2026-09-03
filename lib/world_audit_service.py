"""Read-only cross-engine invariant audit over PipelineDB, Beets, and disk."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from contextlib import closing
from typing import TYPE_CHECKING, Any, Literal, Never, Protocol, runtime_checkable

import msgspec

if TYPE_CHECKING:
    from lib.pipeline_db.rows import AlbumRequestRow, DownloadLogWithEvidenceRow

from lib.beets_db import (
    BeetsWorldAlbum,
    CurrentBeetsAmbiguous,
    CurrentBeetsResolution,
    CurrentBeetsUnique,
    beets_authority_availability_category,
)
from lib.quality import AlbumQualityEvidence
from lib.quality_evidence import fingerprint_album_path
from lib.release_identity import ReleaseIdentity
from lib.surface_outcomes import exit_codes_from_http
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
    check_library_root_containment,
    check_status_membership,
    derive_denylist_authorities,
    world_violation_bucket,
)

AUDITED_INVARIANTS = (
    "folder_exclusivity",
    "library_filesystem",
    "library_root_containment",
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


WorldAuditOutcome = Literal[
    "clean", "observations_only", "integrity_failed", "beets_unavailable",
]
"""The one completeness/availability + integrity outcome both
``pipeline-cli audit world`` and ``GET /api/audit/world`` branch on
(issue #1355 item 4). ``report.complete`` takes priority over
``report.status``: an incomplete scan cannot also vouch for Bucket A
integrity, so an unavailable Beets authority is always
``beets_unavailable`` regardless of what ``status`` happens to carry.
``integrity_failed`` keeps its own pre-existing HTTP 200 / CLI exit 1
handling wherever this is consumed and is deliberately NOT a member of
:data:`WORLD_AUDIT_HTTP_STATUS` / :data:`WORLD_AUDIT_EXIT_CODES`: the
shared surface-outcome registry audit (``tests/test_surface_outcomes.py``)
requires every registered map's exit code to obey the ordinary
status-derived convention, and exit 1 for an HTTP-200 outcome cannot —
mirroring the sibling retag-divergence audit's own, similarly
unregistered ``divergence_found`` -> exit 1 case
(``lib/retag_divergence_audit.py``)."""


def world_audit_outcome(report: WorldAuditReport) -> WorldAuditOutcome:
    """Derive the shared outcome both real outer adapters branch on."""

    if not report.complete:
        return "beets_unavailable"
    if report.status == "clean":
        return "clean"
    if report.status == "observations_only":
        return "observations_only"
    if report.status == "integrity_failed":
        return "integrity_failed"
    raise ValueError(f"unrecognized world audit status: {report.status!r}")


#: The conventional subset of :data:`WorldAuditOutcome` — deliberately
#: excludes ``integrity_failed`` (see its docstring above). 200 for a
#: complete report whatever it observed, 503 (transient/retryable) for an
#: incomplete one: the audit never actually ran, so 200 there would let a
#: cron or ``&&`` chain read "clean" from a report that answered nothing.
WORLD_AUDIT_HTTP_STATUS: dict[str, int] = {
    "clean": 200,
    "observations_only": 200,
    "beets_unavailable": 503,
}

#: Derived branch-for-branch from :data:`WORLD_AUDIT_HTTP_STATUS` through
#: the repository's CLI ⇄ API surface-outcome convention
#: (``lib/surface_outcomes.py``).
WORLD_AUDIT_EXIT_CODES: dict[str, int] = exit_codes_from_http(
    WORLD_AUDIT_HTTP_STATUS
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
    # Issue #1089: the exact filesystem root every current Beets album's
    # items are supposed to live under. Real ``BeetsDB``/``FakeBeetsDB``
    # already carry this (both bound to it at construction) — a Protocol
    # member, not a new ``audit_world`` parameter, so every existing
    # caller of ``audit_world``/``audit_world_from_factory`` needs zero
    # changes to keep working. Declared as a read-only property, not a
    # plain attribute: real ``BeetsDB.library_root`` is ``@property``, and
    # a plain mutable Protocol attribute is invariant — it would reject a
    # read-only property as "incompatible" under strict Pyright.
    @property
    def library_root(self) -> str: ...

    def list_world_albums(self) -> list[BeetsWorldAlbum]: ...

    def resolve_current_releases(
        self,
        identities: list[ReleaseIdentity],
    ) -> dict[ReleaseIdentity, CurrentBeetsResolution]: ...



class OwnedWorldAuditBeetsDB(WorldAuditBeetsDB, Protocol):
    def close(self) -> None: ...


type WorldAuditBeetsFactory = Callable[[], OwnedWorldAuditBeetsDB]


class _BeetsQueryUnavailable(Exception):
    def __init__(self, category: str) -> None:
        super().__init__(category)
        self.category = category


def _translate_beets_query_failure(exc: Exception) -> Never:
    category = beets_authority_availability_category(exc)
    if category is None:
        raise exc
    raise _BeetsQueryUnavailable(category) from exc


class _AvailabilityMediatedBeetsDB:
    """Translate only failures raised by the two Beets query calls."""

    def __init__(self, beets: WorldAuditBeetsDB) -> None:
        self._beets = beets

    @property
    def library_root(self) -> str:
        return self._beets.library_root

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

def _current_evidence_id(row: Mapping[str, Any]) -> int | None:
    raw = row.get("current_evidence_id")
    return int(raw) if isinstance(raw, int) else None


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
    fingerprint_cache: dict[int, str | None] = {}
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
                # #1089 NOTE-H (review round 3): fingerprint_album_path can
                # legitimately return None (a vanished/empty album), so a
                # cache keyed on "value is None means uncached" would
                # recompute that album's walk on every hit instead of
                # caching the negative result. Membership, not value,
                # decides cache presence. This is not ONLY a caching fix:
                # it also makes one edge stricter than before — an
                # evidence row whose recorded snapshot_fingerprint happens
                # to equal the empty-list digest, checked against a now-
                # empty album, used to read as coherent (both sides the
                # same digest string) and now reports
                # evidence_fingerprint_mismatch (a real string vs None),
                # which is the correct call: the album is actually gone.
                if current.album_id in fingerprint_cache:
                    actual_fingerprint = fingerprint_cache[current.album_id]
                else:
                    actual_fingerprint = fingerprint_album_path(
                        current.album_path,
                    )
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
    violations.extend(check_library_root_containment(
        albums,
        library_root=beets_db.library_root,
    ))
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
) -> OwnedWorldAuditBeetsDB | BeetsAuthorityUnavailable:
    try:
        return beets_factory()
    except Exception as exc:
        category = beets_authority_availability_category(exc)
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
    with closing(opened):
        result = _audit_world_with_mediated_beets(pipeline_db, opened)
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
    "WORLD_AUDIT_EXIT_CODES",
    "WORLD_AUDIT_HTTP_STATUS",
    "BeetsAuthorityUnavailable",
    "WorldAuditBeetsDB",
    "WorldAuditBeetsFactory",
    "WorldAuditCounts",
    "WorldAuditGroup",
    "WorldAuditGroups",
    "WorldAuditOutcome",
    "WorldAuditPipelineDB",
    "WorldAuditReport",
    "audit_world",
    "audit_world_from_borrowed_factory",
    "audit_world_from_factory",
    "build_world_audit_report",
    "world_audit_outcome",
]
