"""Small orchestrator for a fully completed download.

Filesystem materialization lives in :mod:`lib.download_materialization`, exact-
release validation and dispatch in :mod:`lib.download_validation`, rejection
writes in :mod:`lib.download_rejection`, and poll state in :mod:`lib.download`.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass
from typing import Protocol, TYPE_CHECKING

from lib import download_materialization
from lib import download_validation
from lib.dispatch import DispatchCoreFn, DispatchOutcome
from lib.grab_list import GrabListEntry
from lib.processing_paths import canonical_folder_for_row, processing_albums_dir
from lib.staged_album import StagedAlbum

if TYPE_CHECKING:
    from lib.context import CratediggerContext
    from lib.terminal_outcomes import PendingImportTerminalOutcome

logger = logging.getLogger("cratedigger")


@dataclass(frozen=True)
class Completed:
    """Local non-dispatch processing succeeded."""

    terminal_outcome: PendingImportTerminalOutcome | None = None


@dataclass(frozen=True)
class CompletionFailed:
    """A non-dispatch local failure occurred."""

    reason: str
    terminal_outcome: PendingImportTerminalOutcome | None = None


@dataclass(frozen=True)
class CompletionDispatched:
    """Validation/dispatch already owned the request transition."""

    outcome: DispatchOutcome


@dataclass(frozen=True)
class CompletionDeferred:
    """Request state is intentionally untouched for retry or recovery."""

    detail: str


CompletionResult = Completed | CompletionFailed | CompletionDispatched | CompletionDeferred
"""Return type of ``process_completed_album`` / ``_run_completed_processing``."""


class ProcessAlbumFn(Protocol):
    """Exact injectable shape of :func:`process_completed_album`."""

    def __call__(
        self,
        album_data: GrabListEntry,
        ctx: CratediggerContext,
        *,
        import_job_id: int,
        validate_fn: download_validation.ValidateFn | None = None,
        handle_valid_fn: download_validation.HandleValidFn | None = None,
        dispatch_fn: DispatchCoreFn | None = None,
    ) -> CompletionResult: ...


def process_completed_album(
    album_data: GrabListEntry,
    ctx: CratediggerContext,
    *,
    import_job_id: int,
    validate_fn: download_validation.ValidateFn | None = None,
    handle_valid_fn: download_validation.HandleValidFn | None = None,
    dispatch_fn: DispatchCoreFn | None = None,
    materialize_before_file_copy: Callable[[], None] | None = None,
    materialize_fn: Callable[..., download_materialization.MaterializeResult] | None = None,
) -> CompletionResult:
    """Materialize, validate, and dispatch one fully downloaded album."""
    staged_album = StagedAlbum.from_entry(
        album_data,
        default_path=canonical_folder_for_row(
            album_data,
            processing_albums_dir(ctx.cfg.processing_dir),
        ),
    )
    materialize = materialize_fn or download_materialization._materialize_processing_dir
    materialized = materialize(
        album_data,
        staged_album,
        ctx,
        before_file_copy=materialize_before_file_copy,
    )
    if isinstance(materialized, download_materialization.MaterializeFailed):
        return CompletionFailed(reason=materialized.reason)
    if isinstance(materialized, download_materialization.MaterializeGuarded):
        return CompletionDeferred(detail=materialized.detail)
    assert isinstance(materialized, download_materialization.Materialized)

    logger.info(
        "Processing completed download: %s - %s",
        album_data.artist,
        album_data.title,
    )
    if ctx.cfg.beets_validation_enabled and album_data.mb_release_id:
        resolved_validate = (
            validate_fn
            if validate_fn is not None
            else download_validation._process_beets_validation
        )
        outcome = resolved_validate(
            album_data,
            staged_album,
            ctx,
            import_job_id=import_job_id,
            handle_valid_fn=handle_valid_fn,
            dispatch_fn=dispatch_fn,
        )
        if outcome is not None:
            if outcome.deferred:
                return CompletionDeferred(detail=outcome.message)
            return CompletionDispatched(outcome=outcome)
    return Completed()


_process_completed_album_conformance: ProcessAlbumFn = process_completed_album
