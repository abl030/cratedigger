"""Capture-then-reconcile orchestration for Jellyfin 'Recently Added' pins.

Background (migration 046, issue #574 — the Jellyfin sibling of the Plex loop
in ``lib/plex_pin_service.py``): an upgrade re-import replaces an album's
on-disk files; the Jellyfin rescan deletes the album's Audio items and
recreates them with ``DateCreated`` stamped from file ctime (= import time),
and sometimes recreates the MusicAlbum item too. Jellyfin's 'Recently
Added'/Latest row orders albums by their children's ``DateCreated``, so the
upgrade wrongly surfaces at the top. This module preserves the original date:

  capture   (importer, BEFORE the Jellyfin refresh): read the maximum
            ``DateCreated`` across the album's Audio children (the value that
            actually orders Jellyfin's music Latest rows), clamp it to Plex's
            older preserved ``addedAt`` when available, and snapshot the item
            ids. A genuinely-new album isn't in Jellyfin yet, so nothing is
            captured — the table self-selects upgrades.
  reconcile (5-min cratedigger cycle): for each pending pin past the settle
            window, re-find the album and check whether the rescan has LANDED
            — an item id differs from the snapshot OR an existing Audio item's
            date is newer than the captured maximum. The date branch is
            load-bearing: Jellyfin 10.11 restamps same-path items from file
            ctime when their mtime changes without changing their ids. Clamp
            only newer dates back to the captured maximum. Until Jellyfin
            exposes either signal the pin stays pending, then expires at TTL.

The Jellyfin client (find/children/set) lives in ``lib/util.py``; all three
functions take kwarg-DI seams so tests drive them without touching the
network.

Deliberate sibling duplication: this module and ``lib/plex_pin_service.py``
share a capture-then-reconcile outline, but not a useful common state machine.
Jellyfin stores ISO strings, waits on a landed detector plus TTL, and writes
the album and its children; Plex stores epoch integers, locks one album field,
and closes after a grace window. A shared core would mainly turn those real
differences into strategy plumbing. Before adding a third backend, compare its
actual capture/reconcile lifecycle with both siblings; extract a common engine
only if it materially simplifies the behavior, otherwise keep the backend
contract explicit in its own module.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any, Callable, Protocol

from lib.pipeline_db.pin_status import JellyfinTerminalPinStatus
from lib.util import (
    JellyfinAlbumRef,
    JellyfinItemRef,
    jellyfin_find_album_by_path,
    jellyfin_get_album_children,
    jellyfin_set_date_created,
)

if TYPE_CHECKING:
    from lib.config import CratediggerConfig
    from lib.pipeline_db import PipelineDB

logger = logging.getLogger("cratedigger")

FindFn = Callable[["CratediggerConfig", str], "JellyfinAlbumRef | None"]
ChildrenFn = Callable[["CratediggerConfig", str], "list[JellyfinItemRef]"]
SetFn = Callable[["CratediggerConfig", str, str], bool]


class _PinDBProto(Protocol):
    """Narrow DB surface this service uses — keeps it FakePipelineDB-friendly
    (same Protocol pattern as lib/plex_pin_service.py)."""

    def add_jellyfin_date_created_pin(
        self, *, imported_path: str, original_date_created: str,
        album_item_id: str, children_item_ids: list[str],
        request_id: int | None) -> int: ...

    def get_pending_jellyfin_date_created_pins(
        self, *, captured_before: datetime,
        limit: int = 100) -> list[dict[str, Any]]: ...

    def mark_jellyfin_date_created_pin(
        self, pin_id: int, *, status: JellyfinTerminalPinStatus,
        reconciled_at: datetime) -> None: ...


# A pin is only looked at this long after capture — not a completion
# guarantee (the landed-detector is), just enough to skip the cycles where
# the rescan certainly hasn't started.
DEFAULT_GRACE_SECONDS = 180

# A pin whose album update never becomes observable (ids unchanged and no
# Audio date newer than the captured maximum) is closed after this long.
# Jellyfin's nightly scheduled scan bounds eventual observation at ~24h; 48h
# covers it with margin. Expiry is benign for Recently Added because no child
# crossed the captured album maximum.
DEFAULT_TTL_HOURS = 48


@dataclass(frozen=True)
class CaptureResult:
    """Outcome of a capture attempt. ``outcome`` is one of:
    ``captured`` (pin written), ``no_album`` (genuinely-new, nothing to pin),
    ``disabled`` (Jellyfin not configured / no path), ``error`` (best-effort
    fail)."""
    outcome: str
    pin_id: int | None = None
    original_date_created: str | None = None


@dataclass(frozen=True)
class ReconcileResult:
    pinned: int = 0
    already_correct: int = 0
    waiting: int = 0
    skipped: int = 0
    expired: int = 0
    errors: int = 0

    def to_log_line(self) -> str:
        return (
            f"JELLYFIN PIN reconcile: pinned={self.pinned} "
            f"already_correct={self.already_correct} "
            f"waiting={self.waiting} skipped={self.skipped} "
            f"expired={self.expired} errors={self.errors}"
        )


def _jellyfin_pin_enabled(cfg: "CratediggerConfig") -> bool:
    return bool(cfg.jellyfin_url and cfg.resolved_jellyfin_token())


def capture_jellyfin_date_created_pin(
    cfg: "CratediggerConfig",
    db: "PipelineDB | _PinDBProto",
    imported_path: str | None,
    request_id: int | None,
    *,
    historical_added_at: int | None = None,
    find_fn: FindFn = jellyfin_find_album_by_path,
    children_fn: ChildrenFn = jellyfin_get_album_children,
) -> CaptureResult:
    """Stash the album's historical Audio date and item-id snapshot.

    MUST run before the Jellyfin media update so the children still carry the
    pre-upgrade dates that determine the album's current Latest position.
    ``historical_added_at`` is Plex's preserved original epoch value. It can
    only move the Jellyfin baseline backwards, repairing a prior Jellyfin
    rebuild or refresh that had already polluted the captured child dates.
    Best-effort: never raises.
    """
    if not _jellyfin_pin_enabled(cfg) or not imported_path:
        return CaptureResult("disabled")
    try:
        ref = find_fn(cfg, imported_path)
    except Exception:
        logger.warning(
            "JELLYFIN PIN: capture lookup failed for %r (request %s) — non-fatal",
            imported_path, request_id, exc_info=True)
        return CaptureResult("error")
    if ref is None:
        return CaptureResult("no_album")
    try:
        children = children_fn(cfg, ref.item_id)
        original_date_created = max(
            (child.date_created for child in children if child.date_created),
            default=ref.date_created,
        )
        if historical_added_at is not None:
            historical = datetime.fromtimestamp(
                historical_added_at, tz=timezone.utc
            )
            captured = datetime.fromisoformat(
                original_date_created.replace("Z", "+00:00")
            )
            if historical < captured:
                original_date_created = (
                    historical.isoformat(timespec="seconds")
                    .replace("+00:00", "Z")
                )
        pin_id = db.add_jellyfin_date_created_pin(
            imported_path=imported_path,
            original_date_created=original_date_created,
            album_item_id=ref.item_id,
            children_item_ids=[c.item_id for c in children],
            request_id=request_id,
        )
    except Exception:
        logger.warning(
            "JELLYFIN PIN: capture persist failed for %r — non-fatal",
            imported_path, exc_info=True)
        return CaptureResult("error")
    logger.info(
        "JELLYFIN PIN: captured DateCreated=%s for %r (%s — %s; pin %d, request %s)",
        original_date_created, imported_path, ref.artist, ref.name, pin_id,
        request_id)
    return CaptureResult("captured", pin_id, original_date_created)


def reconcile_jellyfin_date_created_pins(
    cfg: "CratediggerConfig",
    db: "PipelineDB | _PinDBProto",
    *,
    now: datetime,
    grace_seconds: int = DEFAULT_GRACE_SECONDS,
    ttl_hours: int = DEFAULT_TTL_HOURS,
    limit: int = 100,
    find_fn: FindFn = jellyfin_find_album_by_path,
    children_fn: ChildrenFn = jellyfin_get_album_children,
    set_fn: SetFn = jellyfin_set_date_created,
) -> ReconcileResult:
    """Clamp post-upgrade dates after the album update becomes observable.

    Item-id drift and Audio dates newer than the captured maximum are the two
    landing signals. Best-effort: per-pin failures are logged and counted.
    """
    if not _jellyfin_pin_enabled(cfg):
        return ReconcileResult()
    cutoff = now - timedelta(seconds=grace_seconds)
    try:
        pins = db.get_pending_jellyfin_date_created_pins(
            captured_before=cutoff, limit=limit)
    except Exception:
        logger.warning("JELLYFIN PIN: reconcile fetch failed — non-fatal",
                       exc_info=True)
        return ReconcileResult()

    pinned = already = waiting = skipped = expired = errors = 0
    for pin in pins:
        pin_id = pin["id"]
        path = pin["imported_path"]
        original = str(pin["original_date_created"])
        snapshot_album_id = str(pin["album_item_id"])
        snapshot_children = set(pin["children_item_ids"])
        try:
            ref = find_fn(cfg, path)
            if ref is None:
                # Album no longer locatable (removed/renamed since capture).
                db.mark_jellyfin_date_created_pin(
                    pin_id, status="skipped", reconciled_at=now)
                skipped += 1
                continue
            children = children_fn(cfg, ref.item_id)
            ids_changed = (
                ref.item_id != snapshot_album_id
                or {c.item_id for c in children} != snapshot_children
            )
            date_bumped = any(
                child.date_created > original for child in children
            )
            landed = ids_changed or date_bumped
            # A landed rescan showing ZERO audio children against a non-empty
            # snapshot is a mid-scan window (old items deleted, new ones not
            # yet inserted) — the new items don't exist to write to yet.
            settled = landed and (bool(children) or not snapshot_children)
            if not settled:
                # The rescan hasn't visibly (fully) happened yet. Writing now
                # would pin the OLD items (about to be deleted) and close the
                # pin with nothing left to fix the new ones — so wait, up to
                # TTL.
                captured_at = pin["captured_at"]
                if captured_at < now - timedelta(hours=ttl_hours):
                    db.mark_jellyfin_date_created_pin(
                        pin_id, status="expired", reconciled_at=now)
                    expired += 1
                else:
                    waiting += 1
                continue
            targets = [(ref.item_id, ref.date_created)]
            targets += [(c.item_id, c.date_created) for c in children]
            writes = failures = 0
            for item_id, current in targets:
                if current <= original:
                    continue
                if set_fn(cfg, item_id, original):
                    writes += 1
                else:
                    failures += 1
            if failures:
                # Leave pending for the next cycle to retry (writes are
                # idempotent; already-restored items compare equal next time).
                errors += 1
                continue
            db.mark_jellyfin_date_created_pin(
                pin_id, status="done", reconciled_at=now)
            if writes:
                logger.info(
                    "JELLYFIN PIN: restored DateCreated=%s on %r "
                    "(pin %d, %d items)", original, path, pin_id, writes)
                pinned += 1
            else:
                already += 1
        except Exception:
            logger.warning(
                "JELLYFIN PIN: reconcile failed for pin %s (%r) — non-fatal",
                pin_id, path, exc_info=True)
            errors += 1
    return ReconcileResult(pinned, already, waiting, skipped, expired, errors)
