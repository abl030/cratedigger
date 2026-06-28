"""Capture-then-reconcile orchestration for Plex 'Recently Added' pins.

Background (migration 040): an upgrade re-import replaces an album's on-disk
files and the post-import Plex partial scan re-stamps its ``addedAt`` to now,
wrongly surfacing it at the top of 'Recently Added'. This module preserves the
original date:

  capture   (importer, BEFORE the Plex refresh): read the album's current
            ``addedAt`` and stash it as a pending pin. A genuinely-new album
            isn't in Plex yet, so nothing is captured — the table self-selects
            upgrades.
  reconcile (5-min cratedigger cycle): for each pending pin past the settle
            window, re-find the album and, if Plex bumped its ``addedAt``,
            write the original value back (locked).

The Plex client (find/set) lives in ``lib/util.py``; both functions take
kwarg-DI seams so tests drive them without touching the network.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any, Callable, Protocol

from lib.util import (
    PlexAlbumRef,
    plex_find_album_by_path,
    plex_set_added_at,
)

if TYPE_CHECKING:
    from lib.config import CratediggerConfig
    from lib.pipeline_db import PipelineDB

logger = logging.getLogger("cratedigger")

FindFn = Callable[["CratediggerConfig", str], "PlexAlbumRef | None"]
SetFn = Callable[["CratediggerConfig", str, int], bool]


class _PinDBProto(Protocol):
    """Narrow DB surface this service uses — keeps it FakePipelineDB-friendly
    (same Protocol pattern as lib/startup_reconciliation.py)."""

    def add_plex_added_at_pin(
        self, *, imported_path: str, original_added_at: int,
        rating_key: str | None, request_id: int | None) -> int: ...

    def get_pending_plex_added_at_pins(
        self, *, captured_before: datetime,
        limit: int = 100) -> list[dict[str, Any]]: ...

    def mark_plex_added_at_pin(
        self, pin_id: int, *, status: str,
        reconciled_at: datetime) -> None: ...

# A pin is only reconciled once this long after capture, giving the Plex
# partial scan fired alongside it time to complete and re-stamp the album.
DEFAULT_GRACE_SECONDS = 180


@dataclass(frozen=True)
class CaptureResult:
    """Outcome of a capture attempt. ``outcome`` is one of:
    ``captured`` (pin written), ``no_album`` (genuinely-new, nothing to pin),
    ``disabled`` (Plex not configured / no path), ``error`` (best-effort fail)."""
    outcome: str
    pin_id: int | None = None
    original_added_at: int | None = None


@dataclass(frozen=True)
class ReconcileResult:
    pinned: int = 0
    already_correct: int = 0
    skipped: int = 0
    errors: int = 0

    def to_log_line(self) -> str:
        return (
            f"PLEX PIN reconcile: pinned={self.pinned} "
            f"already_correct={self.already_correct} "
            f"skipped={self.skipped} errors={self.errors}"
        )


def capture_plex_added_at_pin(
    cfg: "CratediggerConfig",
    db: "PipelineDB | _PinDBProto",
    imported_path: str | None,
    request_id: int | None,
    *,
    find_fn: FindFn = plex_find_album_by_path,
) -> CaptureResult:
    """Read the album currently at ``imported_path`` and stash its ``addedAt``
    as a pending pin. MUST be called BEFORE the Plex refresh fires, so the old
    item still carries its pre-upgrade date. Best-effort: never raises."""
    if not cfg.plex_url or not imported_path:
        return CaptureResult("disabled")
    try:
        ref = find_fn(cfg, imported_path)
    except Exception:
        logger.warning(
            "PLEX PIN: capture lookup failed for %r (request %s) — non-fatal",
            imported_path, request_id, exc_info=True)
        return CaptureResult("error")
    if ref is None:
        return CaptureResult("no_album")
    try:
        pin_id = db.add_plex_added_at_pin(
            imported_path=imported_path,
            original_added_at=ref.added_at,
            rating_key=ref.rating_key,
            request_id=request_id,
        )
    except Exception:
        logger.warning(
            "PLEX PIN: capture persist failed for %r — non-fatal",
            imported_path, exc_info=True)
        return CaptureResult("error")
    logger.info(
        "PLEX PIN: captured addedAt=%d for %r (pin %d, request %s)",
        ref.added_at, imported_path, pin_id, request_id)
    return CaptureResult("captured", pin_id, ref.added_at)


def reconcile_plex_added_at_pins(
    cfg: "CratediggerConfig",
    db: "PipelineDB | _PinDBProto",
    *,
    now: datetime,
    grace_seconds: int = DEFAULT_GRACE_SECONDS,
    limit: int = 100,
    find_fn: FindFn = plex_find_album_by_path,
    set_fn: SetFn = plex_set_added_at,
) -> ReconcileResult:
    """Process pending pins past the settle window: re-find each album and, if
    Plex bumped its ``addedAt``, write the original back (locked). Best-effort —
    per-pin failures are logged and counted, never raised."""
    if not cfg.plex_url:
        return ReconcileResult()
    cutoff = now - timedelta(seconds=grace_seconds)
    try:
        pins = db.get_pending_plex_added_at_pins(captured_before=cutoff, limit=limit)
    except Exception:
        logger.warning("PLEX PIN: reconcile fetch failed — non-fatal", exc_info=True)
        return ReconcileResult()

    pinned = already = skipped = errors = 0
    for pin in pins:
        pin_id = pin["id"]
        path = pin["imported_path"]
        original = int(pin["original_added_at"])
        try:
            ref = find_fn(cfg, path)
            if ref is None:
                # Album no longer locatable (rescan removed/renamed it).
                db.mark_plex_added_at_pin(pin_id, status="skipped", reconciled_at=now)
                skipped += 1
                continue
            if int(ref.added_at) == original:
                # Date already matches — but LOCK it anyway so a Plex rescan
                # that hasn't run yet (180s grace is a proxy, not a guarantee
                # that the scan completed) can't bump it later with no pin left
                # to fix it. Locking an already-correct value is harmless and
                # strictly safer. PUT failure leaves the pin pending for retry.
                if set_fn(cfg, ref.rating_key, original):
                    db.mark_plex_added_at_pin(pin_id, status="done", reconciled_at=now)
                    already += 1
                else:
                    errors += 1
                continue
            if set_fn(cfg, ref.rating_key, original):
                db.mark_plex_added_at_pin(pin_id, status="done", reconciled_at=now)
                logger.info(
                    "PLEX PIN: restored addedAt=%d on %r (pin %d, was %d)",
                    original, path, pin_id, ref.added_at)
                pinned += 1
            else:
                # PUT failed — leave pending for the next cycle to retry.
                errors += 1
        except Exception:
            logger.warning(
                "PLEX PIN: reconcile failed for pin %s (%r) — non-fatal",
                pin_id, path, exc_info=True)
            errors += 1
    return ReconcileResult(pinned, already, skipped, errors)
