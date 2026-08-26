"""Best-effort media-server reconciliation after exact library deletion."""

from __future__ import annotations

import logging
from collections.abc import Callable
from pathlib import Path
from typing import Literal

import msgspec

from lib.config import CratediggerConfig
from lib.util import (
    JellyfinAlbumRef,
    PlexAlbumRef,
    jellyfin_find_album_by_path,
    plex_find_album_by_path,
    request_plex_scan,
)

log = logging.getLogger("cratedigger")


class DeleteNotification(msgspec.Struct, frozen=True):
    provider: Literal["plex", "jellyfin"]
    status: Literal["submitted", "skipped", "warning"]
    detail: str
    target: str = ""


PlexFindFn = Callable[[CratediggerConfig, str], PlexAlbumRef | None]
PlexScanFn = Callable[[CratediggerConfig, str], tuple[int, str] | None]
JellyfinFindFn = Callable[[CratediggerConfig, str], JellyfinAlbumRef | None]


def _nearest_existing_ancestor(path: str, root: str) -> str | None:
    if not root:
        return None
    root_path = Path(root).resolve(strict=False)
    candidate = Path(path)
    if not candidate.is_absolute():
        candidate = root_path / candidate
    candidate = candidate.resolve(strict=False)
    try:
        candidate.relative_to(root_path)
    except ValueError:
        return None
    while candidate != root_path and not candidate.exists():
        candidate = candidate.parent
    return str(candidate) if candidate.exists() else None


def _is_configured_root(path: str, root: str) -> bool:
    """Whether ``path`` (an already-resolved ancestor) IS the configured
    library root itself, rather than a narrower folder beneath it."""
    return Path(path).resolve(strict=False) == Path(root).resolve(strict=False)


def notify_library_delete(
    cfg: CratediggerConfig,
    former_album_path: str,
    *,
    allow_escalation: bool = True,
    plex_find_fn: PlexFindFn = plex_find_album_by_path,
    plex_scan_fn: PlexScanFn = request_plex_scan,
    jellyfin_find_fn: JellyfinFindFn = jellyfin_find_album_by_path,
) -> tuple[DeleteNotification, ...]:
    """Tell Plex/Jellyfin after an album's directory is gone.

    Two callers share this boundary: the operator-authorized destructive
    delete (Bad Rip / Replace / library-delete, after the destructive
    advisory locks are released) and the routine post-import reconciliation
    of a path-changing upgrade's vanished old path (issue #1203 item 2).
    ``allow_escalation`` (default ``True``, the destructive caller) governs
    ONLY the Plex leg: whether the nearest-existing-ancestor scan may
    escalate to the configured library root itself when no narrower
    ancestor survives. Refusing that escalation still records a
    ``skipped`` ``DeleteNotification`` naming why — it never silently
    no-ops.

    The Jellyfin leg is detect-and-report for EVERY caller and calls no
    refresh endpoint at all (issue #1221 item 1). A source-level read of
    Jellyfin 10.11
    (``MediaBrowser.Controller/Entities/Folder.cs::ValidateChildrenInternal2``)
    found that deletion of a vanished item is computed as the PARENT
    folder's own disk-vs-DB child-set diff — an item is never deleted by
    refreshing itself, so a targeted refresh is structurally incapable of
    reaping the album this function is called about. Worse: the album's
    directory is already gone, so enumerating it during that refresh raises
    ``DirectoryNotFoundException``; the same method's ``IOException``
    handler logs and swallows it rather than returning, so the refresh
    proceeds with an EMPTY observed disk and deletes every one of the
    album's child ``Audio`` rows (files untouched, their Jellyfin
    metadata/user-data is not). Jellyfin's own machinery reaps the vanished
    item without our help: its scheduled library scan's parent validation,
    and often sooner — measured during issue #1221 verification, a
    post-import ``/Library/Media/Updated`` report on the new sibling path
    reaped the old item within hours. So this function only finds the item
    by its former path and reports what it found.
    """
    outcomes: list[DeleteNotification] = []

    if cfg.plex_url and cfg.resolved_plex_token():
        plex_root = cfg.beets_directory
        if not plex_root and cfg.plex_path_map:
            plex_root = cfg.plex_path_map.split(":", 1)[0]
        ancestor = _nearest_existing_ancestor(
            former_album_path, plex_root)
        if ancestor is None:
            outcomes.append(DeleteNotification(
                "plex", "warning",
                "former album path is outside the configured Beets root"))
        elif not allow_escalation and _is_configured_root(ancestor, plex_root):
            outcomes.append(DeleteNotification(
                "plex", "skipped",
                "refused to escalate to a library-root scan (no narrower "
                "existing ancestor survives)", ancestor))
        else:
            ref = None
            find_warning = ""
            try:
                ref = plex_find_fn(cfg, former_album_path)
            except Exception as exc:  # noqa: BLE001 -- refresh can still run
                find_warning = f"; identity lookup failed: {type(exc).__name__}: {exc}"
                log.warning("PLEX DELETE: identity lookup failed: %s", exc)
            try:
                submitted = plex_scan_fn(cfg, ancestor)
                if submitted is None:
                    outcomes.append(DeleteNotification(
                        "plex", "skipped", "Plex is not fully configured"))
                else:
                    status, sent_path = submitted
                    identity = f" ratingKey={ref.rating_key}" if ref else ""
                    outcomes.append(DeleteNotification(
                        "plex", "warning" if find_warning else "submitted",
                        f"HTTP {status}; submission is not scan proof{identity}{find_warning}",
                        sent_path))
            except Exception as exc:  # noqa: BLE001 -- best effort
                log.warning("PLEX DELETE: refresh failed: %s", exc)
                outcomes.append(DeleteNotification(
                    "plex", "warning", f"{type(exc).__name__}: {exc}", ancestor))
    else:
        outcomes.append(DeleteNotification(
            "plex", "skipped", "Plex is not configured"))

    if cfg.jellyfin_url and cfg.resolved_jellyfin_token():
        try:
            ref = jellyfin_find_fn(cfg, former_album_path)
        except Exception as exc:  # noqa: BLE001 -- visible warning
            log.warning("JELLYFIN DELETE: identity lookup failed: %s", exc)
            outcomes.append(DeleteNotification(
                "jellyfin", "warning",
                f"identity lookup failed: {type(exc).__name__}: {exc} — "
                "cannot determine whether a Jellyfin item remains at the "
                "former path"))
        else:
            if ref is not None:
                outcomes.append(DeleteNotification(
                    "jellyfin", "warning",
                    f"exact album item {ref.item_id} found at former path "
                    f"{former_album_path!r} but NOT refreshed — Jellyfin "
                    "cannot reap an item whose directory vanished via a "
                    "targeted refresh, and attempting one would delete its "
                    "child rows instead; Jellyfin's own next library "
                    "validation reaps it",
                    ref.item_id))
            else:
                outcomes.append(DeleteNotification(
                    "jellyfin", "skipped",
                    "no Jellyfin item found by former path"))
    else:
        outcomes.append(DeleteNotification(
            "jellyfin", "skipped", "Jellyfin is not configured"))

    return tuple(outcomes)
