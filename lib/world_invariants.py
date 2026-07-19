"""Cross-engine world invariants shared by fuzzing and live audit (#743)."""

from __future__ import annotations

import os
from collections import defaultdict
from collections.abc import Mapping, Sequence
from typing import Any

import msgspec

from lib.quality import ImportResult, dispatch_action
from lib.quality.decisions import post_import_search_action_if_known


class LibraryAlbumSnapshot(msgspec.Struct, frozen=True):
    """The exact release identity and paths represented by one Beets album."""

    album_id: int
    release_id: str
    album_path: str
    item_paths: tuple[str, ...]


class RequestMembershipSnapshot(msgspec.Struct, frozen=True):
    """The request fields needed to compare pipeline state with Beets."""

    request_id: int
    release_id: str
    status: str
    imported_path: str | None


class EvidenceDiskSnapshot(msgspec.Struct, frozen=True):
    """One request's linked evidence beside its exact Beets snapshot."""

    request_id: int
    release_id: str
    status: str
    album_path: str | None
    current_evidence_id: int | None
    evidence_id: int | None
    evidence_release_id: str | None
    evidence_source_path: str | None
    evidence_fingerprint: str | None
    actual_fingerprint: str | None


class LifecycleTransitionSnapshot(msgspec.Struct, frozen=True):
    """Before/after facts for a world operation with temporal policy."""

    request_id: int
    operation: str
    before_status: str
    after_status: str
    before_release_id: str
    after_release_id: str
    before_override: str | None
    after_override: str | None
    before_album_fingerprint: str | None
    after_album_fingerprint: str | None
    before_verified_lossless: bool = False
    descendant_request_id: int | None = None


class DenylistAuthoritySnapshot(msgspec.Struct, frozen=True):
    """One denylist row and the decisions capable of authorizing it."""

    request_id: int
    username: str
    authorizing_decisions: tuple[str, ...] = ()


class WorldViolation(msgspec.Struct, frozen=True):
    """One deterministic invariant violation suitable for CLI/API output."""

    code: str
    detail: str
    request_id: int | None = None
    release_id: str | None = None
    album_ids: tuple[int, ...] = ()


def _normal_path(path: str) -> str:
    return os.path.normcase(os.path.abspath(os.path.normpath(path)))


def check_folder_exclusivity(
    albums: Sequence[LibraryAlbumSnapshot],
) -> tuple[WorldViolation, ...]:
    """Require one album per folder and every item directly in that folder."""

    violations: list[WorldViolation] = []
    folder_owners: dict[str, list[LibraryAlbumSnapshot]] = defaultdict(list)

    for album in albums:
        if not album.item_paths:
            violations.append(WorldViolation(
                code="album_empty",
                detail=(
                    f"beets album {album.album_id} for release "
                    f"{album.release_id!r} has no items"
                ),
                release_id=album.release_id,
                album_ids=(album.album_id,),
            ))
            continue
        folder = _normal_path(album.album_path)
        folder_owners[folder].append(album)
        for item_path in album.item_paths:
            item_folder = _normal_path(os.path.dirname(item_path))
            if item_folder != folder:
                violations.append(WorldViolation(
                    code="item_outside_album_folder",
                    detail=(
                        f"beets album {album.album_id} item {item_path!r} "
                        f"is outside album folder {album.album_path!r}"
                    ),
                    release_id=album.release_id,
                    album_ids=(album.album_id,),
                ))

    for folder, owners in sorted(folder_owners.items()):
        if len(owners) < 2:
            continue
        album_ids = tuple(sorted(album.album_id for album in owners))
        release_ids = tuple(sorted(album.release_id for album in owners))
        violations.append(WorldViolation(
            code="folder_shared",
            detail=(
                f"beets albums {album_ids!r} for releases {release_ids!r} "
                f"share folder {folder!r}"
            ),
            album_ids=album_ids,
        ))

    return tuple(violations)


def check_library_filesystem(
    albums: Sequence[LibraryAlbumSnapshot],
) -> tuple[WorldViolation, ...]:
    """Require every snapshot path to name the physical library state."""

    violations: list[WorldViolation] = []
    for album in albums:
        if not os.path.isdir(album.album_path):
            violations.append(WorldViolation(
                code="album_folder_missing",
                detail=(
                    f"beets album {album.album_id} folder "
                    f"{album.album_path!r} is absent"
                ),
                release_id=album.release_id,
                album_ids=(album.album_id,),
            ))
        for item_path in album.item_paths:
            if os.path.isfile(item_path):
                continue
            violations.append(WorldViolation(
                code="album_item_missing",
                detail=(
                    f"beets album {album.album_id} item "
                    f"{item_path!r} is absent"
                ),
                release_id=album.release_id,
                album_ids=(album.album_id,),
            ))
    return tuple(violations)


def assert_replaced_row_frozen(
    snapshot: Mapping[str, Any],
    row: Mapping[str, Any],
) -> None:
    """A superseded request is a byte-frozen historical audit record."""

    if row == snapshot:
        return
    diffs = {
        key: (snapshot.get(key), row.get(key))
        for key in set(snapshot) | set(row)
        if snapshot.get(key) != row.get(key)
    }
    request_id = snapshot.get("id", "unknown")
    raise AssertionError(
        f"replaced request {request_id} mutated after supersede: {diffs}"
    )


def check_status_membership(
    requests: Sequence[RequestMembershipSnapshot],
    albums: Sequence[LibraryAlbumSnapshot],
) -> tuple[WorldViolation, ...]:
    """Require every imported request to resolve to one exact Beets pressing."""

    by_release: dict[str, list[LibraryAlbumSnapshot]] = defaultdict(list)
    for album in albums:
        by_release[album.release_id].append(album)

    violations: list[WorldViolation] = []
    for request in requests:
        if request.status != "imported":
            continue
        matches = by_release.get(request.release_id, [])
        if not matches:
            violations.append(WorldViolation(
                code="imported_release_missing",
                detail=(
                    f"imported request {request.request_id} release "
                    f"{request.release_id!r} is absent from Beets"
                ),
                request_id=request.request_id,
                release_id=request.release_id,
            ))
            continue
        if len(matches) > 1:
            album_ids = tuple(sorted(album.album_id for album in matches))
            violations.append(WorldViolation(
                code="imported_release_duplicate",
                detail=(
                    f"imported request {request.request_id} release "
                    f"{request.release_id!r} resolves to Beets albums "
                    f"{album_ids!r}"
                ),
                request_id=request.request_id,
                release_id=request.release_id,
                album_ids=album_ids,
            ))
            continue
        if not request.imported_path:
            violations.append(WorldViolation(
                code="imported_path_missing",
                detail=f"imported request {request.request_id} has no imported_path",
                request_id=request.request_id,
                release_id=request.release_id,
                album_ids=(matches[0].album_id,),
            ))
            continue
        actual = _normal_path(matches[0].album_path)
        expected = _normal_path(request.imported_path)
        if actual != expected:
            violations.append(WorldViolation(
                code="imported_path_mismatch",
                detail=(
                    f"imported request {request.request_id} points at "
                    f"{request.imported_path!r}; Beets uses "
                    f"{matches[0].album_path!r}"
                ),
                request_id=request.request_id,
                release_id=request.release_id,
                album_ids=(matches[0].album_id,),
            ))

    return tuple(violations)


def check_evidence_disk_coherence(
    snapshots: Sequence[EvidenceDiskSnapshot],
) -> tuple[WorldViolation, ...]:
    """Require each active installed request to link its exact disk evidence."""

    violations: list[WorldViolation] = []
    for snapshot in snapshots:
        if snapshot.status == "replaced":
            continue
        if snapshot.album_path is None:
            if snapshot.current_evidence_id is not None:
                violations.append(WorldViolation(
                    code="evidence_link_without_album",
                    detail=(
                        f"request {snapshot.request_id} links evidence "
                        f"{snapshot.current_evidence_id} without an exact "
                        "Beets album"
                    ),
                    request_id=snapshot.request_id,
                    release_id=snapshot.release_id,
                ))
            continue
        if snapshot.current_evidence_id is None:
            violations.append(WorldViolation(
                code="current_evidence_missing",
                detail=(
                    f"request {snapshot.request_id} has an exact Beets album "
                    "but no current_evidence_id"
                ),
                request_id=snapshot.request_id,
                release_id=snapshot.release_id,
            ))
            continue
        if snapshot.evidence_id != snapshot.current_evidence_id:
            violations.append(WorldViolation(
                code="current_evidence_dangling",
                detail=(
                    f"request {snapshot.request_id} links evidence "
                    f"{snapshot.current_evidence_id}, resolved row is "
                    f"{snapshot.evidence_id}"
                ),
                request_id=snapshot.request_id,
                release_id=snapshot.release_id,
            ))
            continue
        if snapshot.evidence_release_id != snapshot.release_id:
            violations.append(WorldViolation(
                code="evidence_release_mismatch",
                detail=(
                    f"request {snapshot.request_id} release "
                    f"{snapshot.release_id!r} links evidence for "
                    f"{snapshot.evidence_release_id!r}"
                ),
                request_id=snapshot.request_id,
                release_id=snapshot.release_id,
            ))
        if (
            snapshot.evidence_source_path is None
            or _normal_path(snapshot.evidence_source_path)
            != _normal_path(snapshot.album_path)
        ):
            violations.append(WorldViolation(
                code="evidence_path_mismatch",
                detail=(
                    f"request {snapshot.request_id} evidence path "
                    f"{snapshot.evidence_source_path!r} does not match "
                    f"Beets path {snapshot.album_path!r}"
                ),
                request_id=snapshot.request_id,
                release_id=snapshot.release_id,
            ))
        if snapshot.evidence_fingerprint != snapshot.actual_fingerprint:
            violations.append(WorldViolation(
                code="evidence_fingerprint_mismatch",
                detail=(
                    f"request {snapshot.request_id} evidence fingerprint "
                    f"{snapshot.evidence_fingerprint!r} does not match disk "
                    f"{snapshot.actual_fingerprint!r}"
                ),
                request_id=snapshot.request_id,
                release_id=snapshot.release_id,
            ))
    return tuple(violations)


def check_proof_lock_terminality(
    transitions: Sequence[LifecycleTransitionSnapshot],
) -> tuple[WorldViolation, ...]:
    """Automated import attempts must not disturb a proof-bearing install."""

    protected_operations = {"upgrade_import", "force_import"}
    violations: list[WorldViolation] = []
    for transition in transitions:
        if (
            not transition.before_verified_lossless
            or transition.operation not in protected_operations
        ):
            continue
        unchanged = (
            transition.before_status == "imported"
            and transition.after_status == "imported"
            and transition.after_release_id == transition.before_release_id
            and transition.after_album_fingerprint
            == transition.before_album_fingerprint
            and transition.descendant_request_id is None
        )
        if unchanged:
            continue
        violations.append(WorldViolation(
            code="proof_lock_broken",
            detail=(
                f"{transition.operation} disturbed proof-bearing request "
                f"{transition.request_id}"
            ),
            request_id=transition.request_id,
            release_id=transition.before_release_id,
        ))
    return tuple(violations)


def check_no_lossy_tier_widening(
    transitions: Sequence[LifecycleTransitionSnapshot],
) -> tuple[WorldViolation, ...]:
    """Lossless-only rows that remain searchable must stay lossless-only."""

    violations: list[WorldViolation] = []
    for transition in transitions:
        if (
            transition.before_override != "lossless"
            or transition.after_status not in {"wanted", "unsearchable"}
            or transition.after_override == "lossless"
        ):
            continue
        violations.append(WorldViolation(
            code="lossy_tier_widened",
            detail=(
                f"{transition.operation} widened request "
                f"{transition.request_id} from lossless-only to "
                f"{transition.after_override!r}"
            ),
            request_id=transition.request_id,
            release_id=transition.after_release_id,
        ))
    return tuple(violations)


def check_denylist_authority(
    rows: Sequence[DenylistAuthoritySnapshot],
) -> tuple[WorldViolation, ...]:
    """Every source exclusion must trace to a decision that owns exclusion."""

    return tuple(
        WorldViolation(
            code="denylist_without_authority",
            detail=(
                f"request {row.request_id} user {row.username!r} has no "
                "authorizing decision"
            ),
            request_id=row.request_id,
        )
        for row in rows
        if not row.authorizing_decisions
    )


def derive_denylist_authorities(
    *,
    username: str,
    reason: str,
    history: Sequence[Mapping[str, Any]],
) -> tuple[str, ...]:
    """Find persisted decisions that authorize one source-denylist row."""
    decisions: set[str] = set()
    if any(
        entry.get("outcome") == "curator_ban"
        and entry.get("soulseek_username") == username
        for entry in history
    ):
        decisions.add("curator_ban")

    if reason.startswith("quality gate:"):
        decision = (
            "requeue_lossless"
            if "lossless-only" in reason
            else "requeue_upgrade"
        )
        action = post_import_search_action_if_known(decision)
        if action is not None and action.denylist:
            decisions.add(decision)

    for entry in history:
        if entry.get("soulseek_username") != username:
            continue
        raw_result = entry.get("import_result")
        if isinstance(raw_result, str):
            encoded_result = raw_result
        elif isinstance(raw_result, Mapping):
            encoded_result = msgspec.json.encode(dict(raw_result)).decode()
        else:
            continue
        try:
            decision = ImportResult.from_json(encoded_result).decision
        except (ValueError, TypeError, msgspec.DecodeError):
            continue
        if decision is None:
            continue
        search_action = post_import_search_action_if_known(decision)
        if (
            (search_action is not None and search_action.denylist)
            or dispatch_action(decision).denylist
        ):
            decisions.add(decision)
    return tuple(sorted(decisions))


__all__ = [
    "LibraryAlbumSnapshot",
    "DenylistAuthoritySnapshot",
    "EvidenceDiskSnapshot",
    "LifecycleTransitionSnapshot",
    "RequestMembershipSnapshot",
    "WorldViolation",
    "assert_replaced_row_frozen",
    "check_denylist_authority",
    "check_evidence_disk_coherence",
    "check_folder_exclusivity",
    "check_library_filesystem",
    "check_no_lossy_tier_widening",
    "check_proof_lock_terminality",
    "check_status_membership",
    "derive_denylist_authorities",
]
