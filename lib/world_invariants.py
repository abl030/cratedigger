"""Cross-engine world invariants shared by fuzzing and live audit (#743)."""

from __future__ import annotations

import os
import re
from collections import defaultdict
from collections.abc import Mapping, Sequence
from typing import Any

import msgspec

from lib.beets_db import (
    CurrentBeetsAmbiguous,
    CurrentBeetsMissing,
    CurrentBeetsResolution,
)
from lib.quality import ImportResult
from lib.quality.decisions import post_import_search_action_if_known
from lib.quality.dispatch_actions import decision_denylists
from lib.validation_envelope import decode_validation_envelope


class LibraryAlbumSnapshot(msgspec.Struct, frozen=True):
    """The exact release identity and paths represented by one Beets album."""

    album_id: int
    release_id: str
    album_path: str
    item_paths: tuple[str, ...]


class RequestMembershipSnapshot(msgspec.Struct, frozen=True):
    """The request fields needed to evaluate current Beets membership."""

    request_id: int
    release_id: str
    status: str


class EvidenceDiskSnapshot(msgspec.Struct, frozen=True):
    """One request's linked evidence beside its exact Beets snapshot.

    ``evidence_source_path`` is capture-time history. Current coherence uses
    the content fingerprint resolved from fresh Beets authority, never path
    equality with that historical snapshot.
    """

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
    resolutions: Mapping[str, CurrentBeetsResolution],
) -> tuple[WorldViolation, ...]:
    """Require shared typed authority for every installed exact pressing."""

    violations: list[WorldViolation] = []
    for request in requests:
        resolution = resolutions.get(request.release_id)
        if resolution is None:
            violations.append(WorldViolation(
                code="current_beets_authority_unavailable",
                detail=(
                    f"request {request.request_id} release "
                    f"{request.release_id!r} was not resolved"
                ),
                request_id=request.request_id,
                release_id=request.release_id,
            ))
            continue
        if isinstance(resolution, CurrentBeetsAmbiguous):
            violations.append(WorldViolation(
                code="current_beets_ambiguous",
                detail=(
                    f"request {request.request_id} release "
                    f"{request.release_id!r} has ambiguous current Beets "
                    f"authority ({resolution.reason}) across albums "
                    f"{resolution.album_ids!r}"
                ),
                request_id=request.request_id,
                release_id=request.release_id,
                album_ids=resolution.album_ids,
            ))
            continue
        if (
            request.status == "imported"
            and isinstance(resolution, CurrentBeetsMissing)
        ):
            violations.append(WorldViolation(
                code="current_beets_missing",
                detail=(
                    f"imported request {request.request_id} release "
                    f"{request.release_id!r} is missing from current Beets"
                ),
                request_id=request.request_id,
                release_id=request.release_id,
            ))

    return tuple(violations)


def check_evidence_disk_coherence(
    snapshots: Sequence[EvidenceDiskSnapshot],
) -> tuple[WorldViolation, ...]:
    """Require exact linked bytes plus nonblank historical capture metadata."""

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
        if not (snapshot.evidence_source_path or "").strip():
            violations.append(WorldViolation(
                code="evidence_capture_path_missing",
                detail=(
                    f"request {snapshot.request_id} linked evidence has no "
                    "capture-time source path"
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


_DENYLIST_REASON_DECISIONS: dict[str, str] = {
    "quality downgrade prevented": "downgrade",
    "lossless source locked": "lossless_source_locked",
    "audio decode failures": "audio_corrupt",
    "matched curated bad audio hash": "bad_audio_hash",
    "spectral analysis rejected the source": "spectral_reject",
    "mixed lossless+lossy source": "mixed_source",
    "duplicate remove guard failed": "duplicate_remove_guard_failed",
    "provisional lossless source imported": "provisional_lossless_upgrade",
}
_IMPORT_PREVIEW_REASON_PREFIX = "import preview rejected: "
_REJECTED_REASON_PREFIX = "rejected: "
_LEGACY_SPECTRAL_REASON = re.compile(
    r"spectral: \d+kbps <= existing \d+kbps\Z"
)
_LEGACY_TRANSCODE_REASON = re.compile(r"transcode: \d+kbps\Z")


def _decision_denylists(decision: str) -> bool:
    """Issue #813: delegates to the one production policy lookup shared with
    ``lib.dispatch.post_import`` and the quality simulator/evidence-pipeline
    display — no second reimplementation."""
    return decision_denylists(decision)


def _denylist_reason_authorities(reason: str) -> tuple[str, ...]:
    """Decode stable decision-bearing reasons across producer generations."""

    if reason == "suspect lossless source not an upgrade":
        return ("suspect_lossless_reject",)

    decision = _DENYLIST_REASON_DECISIONS.get(reason)
    if decision is not None and _decision_denylists(decision):
        return (decision,)

    if reason.startswith(_IMPORT_PREVIEW_REASON_PREFIX):
        decision = reason.removeprefix(_IMPORT_PREVIEW_REASON_PREFIX)
        if _decision_denylists(decision):
            return (decision,)

    if reason.startswith(_REJECTED_REASON_PREFIX):
        decision = reason.removeprefix(_REJECTED_REASON_PREFIX)
        if _decision_denylists(decision):
            return (decision,)

    if _LEGACY_SPECTRAL_REASON.fullmatch(reason):
        return ("spectral_reject",)
    if reason == "transcode detected" or _LEGACY_TRANSCODE_REASON.fullmatch(reason):
        return ("legacy_transcode",)
    return ()


def derive_denylist_authorities(
    *,
    username: str,
    reason: str,
    history: Sequence[Mapping[str, Any]],
) -> tuple[str, ...]:
    """Find persisted decisions that authorize one source-denylist row."""
    decisions = set(_denylist_reason_authorities(reason))
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
        exact_peer = entry.get("soulseek_username") == username
        try:
            validation = decode_validation_envelope(
                entry.get("validation_result")
            )
        except (
            ValueError,
            TypeError,
            msgspec.ValidationError,
        ):
            validation = None
        if (
            entry.get("outcome") == "rejected"
            and validation is not None
            and (
                (exact_peer and validation.valid is False)
                or (
                    reason == "beets validation rejected"
                    and validation.valid is not True
                )
            )
        ):
            decisions.add("validation_reject")

        # One import/validation decision owns every peer that contributed to
        # a multi-peer folder, while download_log retains only its primary
        # username. The canonical Beets-reject reason is the binding from each
        # secondary source_denylist row back to that request-level decision.
        if not exact_peer and reason != "beets validation rejected":
            continue
        raw_result = entry.get("import_result")
        if isinstance(raw_result, str):
            encoded_result = raw_result
        elif isinstance(raw_result, Mapping):
            # ``entry``'s value type is ``Any``, so the bare isinstance
            # narrow above leaves pyright with a partially unknown
            # ``Mapping[Unknown, Unknown]`` even though every key is a
            # JSON object key (same quirk documented on
            # ``lib.youtube_album_service._json_dict``). ``msgspec.convert``
            # hands back a fully known ``dict[str, object]`` with the
            # identical content for encoding.
            result_dict: dict[str, object] = msgspec.convert(
                raw_result, type=dict[str, object],
            )
            encoded_result = msgspec.json.encode(result_dict).decode()
        else:
            continue
        try:
            decision = ImportResult.from_json(encoded_result).decision
        except (ValueError, TypeError, msgspec.DecodeError):
            continue
        if decision is None:
            continue
        if _decision_denylists(decision):
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
