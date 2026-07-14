"""Best-effort media-server reconciliation after exact library deletion."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Callable, Literal

import msgspec

from lib.config import CratediggerConfig
from lib.util import (
    JellyfinAlbumRef,
    PlexAlbumRef,
    jellyfin_find_album_by_path,
    plex_find_album_by_path,
    request_jellyfin_refresh,
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
JellyfinRefreshFn = Callable[
    [CratediggerConfig, str | None], tuple[int, str] | None,
]


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


def notify_library_delete(
    cfg: CratediggerConfig,
    former_album_path: str,
    *,
    plex_find_fn: PlexFindFn = plex_find_album_by_path,
    plex_scan_fn: PlexScanFn = request_plex_scan,
    jellyfin_find_fn: JellyfinFindFn = jellyfin_find_album_by_path,
    jellyfin_refresh_fn: JellyfinRefreshFn = request_jellyfin_refresh,
) -> tuple[DeleteNotification, ...]:
    """Tell Plex/Jellyfin after the destructive advisory locks are released."""
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
        ref = None
        find_warning = ""
        try:
            ref = jellyfin_find_fn(cfg, former_album_path)
        except Exception as exc:  # noqa: BLE001 -- refresh can still run
            find_warning = f"; identity lookup failed: {type(exc).__name__}: {exc}"
            log.warning("JELLYFIN DELETE: identity lookup failed: %s", exc)
        try:
            item_id = ref.item_id if ref else cfg.jellyfin_library_id
            submitted = jellyfin_refresh_fn(cfg, item_id)
            if submitted is None:
                outcomes.append(DeleteNotification(
                    "jellyfin", "skipped", "Jellyfin is not fully configured"))
            else:
                status, target = submitted
                observed_absent = False
                post_warning = ""
                if ref is not None:
                    try:
                        observed_absent = (
                            jellyfin_find_fn(cfg, former_album_path) is None
                        )
                    except Exception as exc:  # noqa: BLE001 -- visible warning
                        post_warning = (
                            "; post-refresh observation failed: "
                            f"{type(exc).__name__}: {exc}"
                        )
                if ref is None:
                    detail = (
                        f"HTTP {status}; album item was not observable by former "
                        "path, so refresh submission is not reconciliation proof"
                    ) + find_warning
                    outcome_status: Literal["submitted", "warning"] = "warning"
                elif observed_absent:
                    detail = (
                        f"HTTP {status}; exact album item {ref.item_id} is now "
                        "absent by former path"
                    )
                    outcome_status = "submitted"
                else:
                    detail = (
                        f"HTTP {status}; exact album item {ref.item_id} remains "
                        "observable after refresh submission"
                    ) + post_warning
                    outcome_status = "warning"
                outcomes.append(DeleteNotification(
                    "jellyfin", outcome_status, detail, target))
        except Exception as exc:  # noqa: BLE001 -- best effort
            log.warning("JELLYFIN DELETE: refresh failed: %s", exc)
            outcomes.append(DeleteNotification(
                "jellyfin", "warning", f"{type(exc).__name__}: {exc}"))
    else:
        outcomes.append(DeleteNotification(
            "jellyfin", "skipped", "Jellyfin is not configured"))

    return tuple(outcomes)
