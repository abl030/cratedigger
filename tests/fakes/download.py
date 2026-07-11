"""Typed test double for completed-download processing."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from lib import download_validation
from lib.dispatch import DispatchCoreFn
from lib.download_processing import (
    Completed,
    CompletionResult,
    ProcessAlbumFn,
)
from lib.grab_list import GrabListEntry

if TYPE_CHECKING:
    from lib.context import CratediggerContext


@dataclass(frozen=True)
class ProcessAlbumCall:
    album_data: GrabListEntry
    ctx: CratediggerContext
    import_job_id: int
    validate_fn: download_validation.ValidateFn | None
    handle_valid_fn: download_validation.HandleValidFn | None
    dispatch_fn: DispatchCoreFn | None


@dataclass
class RecordingProcessAlbum:
    """Record exact completion calls while returning a real tagged result."""

    outcome: CompletionResult = field(default_factory=Completed)
    calls: list[ProcessAlbumCall] = field(default_factory=list)

    def __call__(
        self,
        album_data: GrabListEntry,
        ctx: CratediggerContext,
        *,
        import_job_id: int,
        validate_fn: download_validation.ValidateFn | None = None,
        handle_valid_fn: download_validation.HandleValidFn | None = None,
        dispatch_fn: DispatchCoreFn | None = None,
    ) -> CompletionResult:
        self.calls.append(ProcessAlbumCall(
            album_data=album_data,
            ctx=ctx,
            import_job_id=import_job_id,
            validate_fn=validate_fn,
            handle_valid_fn=handle_valid_fn,
            dispatch_fn=dispatch_fn,
        ))
        return self.outcome


_recorder_conformance: ProcessAlbumFn = RecordingProcessAlbum()
