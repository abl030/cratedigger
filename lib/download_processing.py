"""Completed-download processing — staging, materialization, validation.

Split out of lib/download.py (issue #146 phase 3). Owns everything
between "the album finished downloading" and "the importer queue takes
over": materializing files from their event-stamped local paths,
beets validation, staging moves, auto-import dispatch, rejection
handoff, and the abandoned-auto-import recovery machinery. Focused reject
writers and Wrong Matches convergence live in ``lib.download_rejection``. The poll
state machine lives in lib/download.py; slskd transfer helpers in
lib/slskd_transfers.py.
"""

from __future__ import annotations

import logging
import os
import re
import shutil
from dataclasses import dataclass
from typing import Any, Callable, TYPE_CHECKING

from lib.download_recovery import ProcessingPathLocation, classify_processing_path
from lib.grab_list import DownloadFile, GrabListEntry
from lib.dispatch import (DispatchCoreFn, DispatchOutcome, QualityGateFn,
                          _build_download_info,
                          _check_quality_gate_core,
                          _requeue_import_job_to_preview,
                          dispatch_import_core)
from lib.download_rejection import (
    _handle_rejected_result,
    _reject_request_auto_import,
)
from lib.import_evidence import (
    CandidateEvidenceActionResult,
    ensure_candidate_evidence_for_action,
)
from lib.import_manifest import (
    audio_relative_paths,
    check_audio_manifest,
    manifest_trace_summary,
    tracked_audio_paths_for_downloads,
)
from lib.processing_paths import (
    attempt_fingerprint,
    canonical_folder_for_row,
    normalize_processing_path,
    path_is_within_root,
    source_dirs_for_album,
    stage_to_ai_path,
    stage_to_ai_root,
)
from lib.quality import (ActiveDownloadState, ValidationResult,
                         compute_effective_override_bitrate)
from lib.staged_album import StagedAlbum
from lib.util import (
    move_abandoned_auto_import,
    log_validation_result,
)

if TYPE_CHECKING:
    from lib.context import CratediggerContext
    from lib.download import DownloadDB

logger = logging.getLogger("cratedigger")


ABANDONED_AUTO_IMPORT_SCENARIO = "abandoned_auto_import"
_ABANDON_PATH_PRESENT = "present"
_ABANDON_PATH_ABSENT = "absent"
_ABANDON_PATH_UNKNOWN = "unknown"


# === Tagged results for the completion-processing ownership protocol (#474) ===
#
# ``_materialize_processing_dir`` and ``process_completed_album`` used to
# return an anonymous ``bool | None`` / ``bool | DispatchOutcome | None``
# union where ``None`` meant "leave the row untouched" — a convention
# documented only in ~30-line comment blocks at each call site. These
# frozen dataclasses name each outcome so pyright can exhaustiveness-check
# every consumer (``match``/``isinstance`` + ``typing.assert_never``)
# instead of relying on identity comparisons against ``True``/``False``/
# ``None``. Never persisted — plain ``@dataclass``, not ``msgspec.Struct``
# (see CLAUDE.md "Wire-boundary types").


@dataclass(frozen=True)
class Materialized:
    """``_materialize_processing_dir`` succeeded: the album's tracked files
    are present at ``staged_album.current_path`` (materialized this call,
    or resumed from a prior crashed attempt). Historical bare ``True``."""


@dataclass(frozen=True)
class MaterializeFailed:
    """A local-only materialize failure (missing event stamp, a file-move
    error, a vanished staged directory/file, a failed ``mkdir``). The
    caller retries within the materialize grace window, then self-heals
    the request back to ``wanted``. Historical bare ``False``.

    ``reason`` is a short, machine-stable diagnostic code — consumers
    must branch on the type tag, never on this string.
    """

    reason: str


@dataclass(frozen=True)
class MaterializeGuarded:
    """Ownership/resume ambiguity: leave the row untouched (an active
    release lock, unverifiable subprocess-start evidence, a post-move
    resume block). Historical bare ``None``.

    ``detail`` is diagnostic only — consumers must branch on the type
    tag, never on this string.
    """

    detail: str


MaterializeResult = Materialized | MaterializeFailed | MaterializeGuarded
"""Return type of ``_materialize_processing_dir`` and
``_evaluate_staged_path_readiness`` (issue #509 — the shared staged-path
resume decision the former's non-canonical branch delegates to, and
``lib.download._processing_path_ready_for_importer`` also consumes)."""


@dataclass(frozen=True)
class Completed:
    """``process_completed_album`` succeeded without producing a dispatch
    summary — no validation configured, or the redownload path already
    called ``mark_done`` directly. Caller finalizes to ``imported`` only
    if the request row is still ``downloading``. Historical bare ``True``.
    """


@dataclass(frozen=True)
class CompletionFailed:
    """A non-dispatch local failure (materialization failed). Caller
    resets to ``wanted`` only if the request row is still ``downloading``.
    Historical bare ``False``.
    """

    reason: str


@dataclass(frozen=True)
class CompletionDispatched:
    """The validation/dispatch path already owned the request transition.

    ``outcome`` is an import summary for the queue owner ONLY — it must
    NEVER drive a fallback status transition. Historical raw
    ``DispatchOutcome`` return value.
    """

    outcome: DispatchOutcome


@dataclass(frozen=True)
class CompletionDeferred:
    """The path intentionally left request state untouched: release-lock
    contention, a guarded post-move staged path, or an ownership-less
    reject needing manual recovery. Caller must NOT touch status.
    Historical bare ``None``.
    """

    detail: str


CompletionResult = Completed | CompletionFailed | CompletionDispatched | CompletionDeferred
"""Return type of ``process_completed_album`` / ``_run_completed_processing``."""


# === slskd file locations ===
#
# The authoritative local path of every completed download comes from
# slskd's DownloadFileComplete event, stamped onto
# ``active_download_state.files[].local_path`` by
# ``lib.slskd_events.ingest_download_file_events`` at the top of each
# poll cycle (issue #146). There is no on-disk path inference: a
# completed file without a stamp is a hard failure.

_REQUEST_SCOPED_STAGE_SUFFIX = re.compile(r" \[request-\d+\]$")


def _is_request_scoped_auto_import_path(
    *,
    current_path: str,
    staging_dir: str,
) -> bool:
    """Return True when ``current_path`` is under auto-import request staging."""
    normalized_path = normalize_processing_path(current_path)
    if not _REQUEST_SCOPED_STAGE_SUFFIX.search(os.path.basename(normalized_path)):
        return False
    return path_is_within_root(
        normalized_path,
        stage_to_ai_root(staging_dir=staging_dir, auto_import=True),
    )


def _attempt_fingerprint_for(files: list[DownloadFile]) -> str:
    """Fingerprint this attempt's exact (username, filename) file set.

    Every canonical-folder computation for the same album must derive
    from this SAME persisted file set — at materialize, at resume
    classification, and at recovery — or a mismatch classifies the
    folder as ``external`` and strands it (issue #550 phase 2).
    """
    return attempt_fingerprint([(f.username, f.filename) for f in files])


# === Download completion processing ===
def _log_post_move_resume_blocked(
    album_data: GrabListEntry,
    *,
    current_path: str,
    detail: str,
) -> None:
    logger.error(
        "POST-MOVE RESUME BLOCKED: request_id=%s %s - %s "
        "current_path=%s %s See docs/advisory-locks.md.",
        album_data.db_request_id,
        album_data.artist,
        album_data.title,
        current_path,
        detail,
    )


def _request_import_subprocess_started(
    db: DownloadDB | None,
    request_id: int | None,
) -> bool | None:
    """Return subprocess-start evidence, or None when ownership is unknown."""
    if request_id is None or db is None:
        return None
    try:
        row = db.get_request(request_id)
    except Exception:
        logger.debug(
            "Failed to read active_download_state for resume guard",
            exc_info=True,
        )
        return None
    if not row:
        return None
    raw_state = row.get("active_download_state")
    if not raw_state:
        return False
    try:
        state = ActiveDownloadState.from_raw(raw_state)
    except Exception:
        logger.debug(
            "Failed to parse active_download_state for resume guard",
            exc_info=True,
        )
        return None
    return state.import_subprocess_started_at is not None


def _import_subprocess_already_started(
    db: DownloadDB | None,
    request_id: int | None,
) -> bool:
    """Did a previous attempt actually launch ``import_one.py`` for this row?

    The auto-import resume guard blocks retries when files live at the
    request-scoped staged path because a prior subprocess may have
    started writing to beets. That guard is correct only when a
    subprocess actually started — files-at-staged is a necessary but not
    sufficient signal. The 2026-05-04 wedge accumulated 5788 failed
    importer jobs because the guard fired even when the subprocess had
    never launched (crash window between staged-move and subprocess
    spawn). See ``docs/advisory-locks.md``.

    With the ``import_subprocess_started_at`` flag set in
    ``ActiveDownloadState`` immediately before ``run_import_one(...)``,
    this helper returns ``True`` only when the flag is set: the
    necessary AND sufficient evidence that the subprocess could have
    written to beets.

    Returns ``True`` (block) if state is unreachable — fail safe; the
    operator can still recover manually. Returns ``False`` (permit
    retry) only on positive evidence the subprocess never launched.
    """
    return _request_import_subprocess_started(db, request_id) is not False


def _probe_abandon_path_liveness(path: str) -> str:
    """Return whether a staged path is definitely present, absent, or unknown."""
    try:
        os.stat(path)
    except FileNotFoundError:
        return _ABANDON_PATH_ABSENT
    except OSError:
        logger.exception(
            "ABANDON AUTO-IMPORT BLOCKED: could not stat current_path=%s",
            path,
        )
        return _ABANDON_PATH_UNKNOWN
    return _ABANDON_PATH_PRESENT


def _restore_abandoned_auto_import(
    *,
    failed_path: str | None,
    current_path: str,
) -> None:
    if failed_path is None:
        return
    try:
        if os.path.exists(failed_path) and not os.path.exists(current_path):
            os.makedirs(os.path.dirname(current_path), exist_ok=True)
            shutil.move(failed_path, current_path)
    except Exception:
        logger.exception(
            "ABANDON AUTO-IMPORT ROLLBACK FAILED: failed_path=%s current_path=%s",
            failed_path,
            current_path,
        )


def _commit_abandoned_auto_import(
    db: DownloadDB,
    *,
    request_id: int,
    current_path: str,
    dl_info: Any,
    detail: str,
    validation_result: str | None,
) -> bool:
    log_id = db.abandon_auto_import_request(
        request_id=request_id,
        current_path=current_path,
        soulseek_username=dl_info.username,
        filetype=dl_info.filetype,
        beets_detail=detail,
        outcome="failed",
        staged_path=current_path,
        error_message=detail,
        validation_result=validation_result,
    )
    return log_id is not None


def _abandon_interrupted_auto_import(
    album_data: GrabListEntry,
    *,
    request_id: int,
    current_path: str,
    db: DownloadDB,
    detail: str,
) -> bool:
    """Quarantine an interrupted auto-import attempt and redownload later."""
    path_state = _probe_abandon_path_liveness(current_path)
    if path_state == _ABANDON_PATH_UNKNOWN:
        return False

    failed_path: str | None = None
    if path_state == _ABANDON_PATH_PRESENT:
        try:
            failed_path = move_abandoned_auto_import(current_path)
        except Exception:
            logger.exception(
                "ABANDON AUTO-IMPORT FAILED: request_id=%s current_path=%s",
                request_id,
                current_path,
            )
            return False

    dl_info = _build_download_info(album_data)
    validation_result: str | None = None
    if failed_path is not None:
        validation_result = ValidationResult(
            valid=False,
            scenario=ABANDONED_AUTO_IMPORT_SCENARIO,
            detail=detail,
            path=current_path,
            soulseek_username=dl_info.username,
            download_folder=current_path,
            failed_path=failed_path,
        ).to_json()

    logger.warning(
        "ABANDON AUTO-IMPORT: request_id=%s %s - %s current_path=%s "
        "failed_path=%s detail=%s",
        request_id,
        album_data.artist,
        album_data.title,
        current_path,
        failed_path,
        detail,
    )
    try:
        committed = _commit_abandoned_auto_import(
            db,
            request_id=request_id,
            current_path=current_path,
            dl_info=dl_info,
            detail=detail,
            validation_result=validation_result,
        )
    except Exception:
        _restore_abandoned_auto_import(
            failed_path=failed_path,
            current_path=current_path,
        )
        logger.exception(
            "ABANDON AUTO-IMPORT DB COMMIT FAILED: request_id=%s current_path=%s",
            request_id,
            current_path,
        )
        return False
    if not committed:
        _restore_abandoned_auto_import(
            failed_path=failed_path,
            current_path=current_path,
        )
        logger.warning(
            "ABANDON AUTO-IMPORT SKIPPED: request_id=%s current_path=%s "
            "row ownership changed before commit",
            request_id,
            current_path,
        )
        return False
    return True


def _abandon_request_scoped_auto_import(
    album_data: GrabListEntry,
    *,
    request_id: int | None,
    current_path: str,
    current_path_kind: str,
    db: DownloadDB | None,
    detail: str,
) -> bool:
    if (
        request_id is None
        or db is None
        or current_path_kind != "request_scoped_auto_import_staged"
    ):
        return False
    if not album_data.mb_release_id:
        _log_post_move_resume_blocked(
            album_data,
            current_path=current_path,
            detail=(
                "already lives at the request-scoped auto-import staged "
                "path but has no release id for the liveness lock; "
                "manual recovery is required."
            ),
        )
        return False
    try:
        from lib.pipeline_db import (
            ADVISORY_LOCK_NAMESPACE_RELEASE,
            release_id_to_lock_key,
        )

        with db.advisory_lock(
            ADVISORY_LOCK_NAMESPACE_RELEASE,
            release_id_to_lock_key(album_data.mb_release_id),
        ) as acquired:
            if not acquired:
                _log_post_move_resume_blocked(
                    album_data,
                    current_path=current_path,
                    detail=(
                        "already lives at the request-scoped auto-import "
                        "staged path, but the release import lock is held; "
                        "leaving it for the active importer."
                    ),
                )
                return False
            return _abandon_interrupted_auto_import(
                album_data,
                request_id=request_id,
                current_path=current_path,
                db=db,
                detail=detail,
            )
    except Exception:
        logger.exception(
            "ABANDON AUTO-IMPORT LOCK CHECK FAILED: request_id=%s current_path=%s",
            request_id,
            current_path,
        )
        return False


def _evaluate_staged_path_readiness(
    album_data: GrabListEntry,
    staged_album: StagedAlbum,
    current_path_location: ProcessingPathLocation,
    db: DownloadDB | None,
) -> MaterializeResult:
    """Decide whether a NON-canonical staged path is safe to resume.

    The ONE "is this staged /Incoming path safe to resume into the
    importer" decision (issue #509). Before this, the same checks
    (missing-dir handling, the ``blocks_post_move_retry``/
    ``blocks_auto_import_dispatch`` guards, the abandon-and-reset call)
    were expressed twice: here, and again in
    ``lib.download._processing_path_ready_for_importer``. The two copies
    had drifted — the poller's copy was missing the
    ``blocks_auto_import_dispatch`` guard entirely, and computed
    subprocess-start evidence as a plain in-memory bool
    (``state.import_subprocess_started_at is not None``) instead of this
    module's fail-safe tri-state DB read
    (``_request_import_subprocess_started`` — ``None`` when ownership is
    unverifiable, which callers must treat the same as "started").

    Both callers now go through this one function; only their REACTION
    to the tag still differs, because it's a genuinely different,
    caller-owned policy rather than a duplicated decision:
    ``_enqueue_completed_processing`` applies the grace-windowed
    retry/reset policy in ``materialize_failure_action``, while the
    poller's own pre-enqueue gate (``_processing_path_ready_for_importer``)
    resets immediately on ``MaterializeFailed`` — its own long-standing,
    tested "fail closed before even trying to enqueue" behavior.

    Callers must already have excluded ``current_path_location.kind ==
    "canonical"`` — that branch performs the real event-stamp/move work
    in ``_materialize_processing_dir`` and has no equivalent here.
    """
    request_id = album_data.db_request_id
    subprocess_started = _request_import_subprocess_started(db, request_id)

    if current_path_location.kind == "request_scoped_auto_import_staged":
        if subprocess_started is True:
            handled = _abandon_request_scoped_auto_import(
                album_data,
                request_id=request_id,
                current_path=staged_album.current_path,
                current_path_kind=current_path_location.kind,
                db=db,
                detail=(
                    "Abandoned interrupted auto-import; queued for "
                    "redownload"
                ),
            )
            if handled:
                return MaterializeFailed(
                    reason="abandoned_interrupted_auto_import")
            return MaterializeGuarded(
                detail="abandon_blocked_release_lock_or_probe_unknown")
        if subprocess_started is None:
            _log_post_move_resume_blocked(
                album_data,
                current_path=staged_album.current_path,
                detail=(
                    "already lives at the request-scoped auto-import "
                    "staged path but import ownership could not be "
                    "verified; manual recovery is required."
                ),
            )
            return MaterializeGuarded(
                detail="ownership_unverifiable_request_scoped_staged")

    if not os.path.isdir(staged_album.current_path):
        if (
            current_path_location.blocks_post_move_retry
            and subprocess_started is not False
        ):
            _log_post_move_resume_blocked(
                album_data,
                current_path=staged_album.current_path,
                detail=(
                    "already lives at the request-scoped auto-import "
                    "staged path but the directory is missing. "
                    "Automatic retry is disabled because beets may "
                    "already have consumed the staged folder; manual "
                    "recovery is required."
                ),
            )
            return MaterializeGuarded(
                detail="post_move_dir_missing_resume_blocked")
        logger.error(f"Current staged path missing: {staged_album.current_path}")
        return MaterializeFailed(reason="staged_path_missing")

    staged_album.bind_import_paths(album_data.files)
    missing_paths: list[str] = []
    for file in album_data.files:
        import_path = file.import_path
        assert import_path is not None
        if not os.path.isfile(import_path):
            missing_paths.append(import_path)
    if missing_paths:
        if (
            current_path_location.blocks_post_move_retry
            and subprocess_started is not False
        ):
            _log_post_move_resume_blocked(
                album_data,
                current_path=staged_album.current_path,
                detail=(
                    "already lives at the request-scoped auto-import "
                    f"staged path but tracked files are missing ({', '.join(missing_paths)}). "
                    "Automatic retry is disabled because import may "
                    "already have started; manual recovery is required."
                ),
            )
            return MaterializeGuarded(
                detail="post_move_files_missing_resume_blocked")
        logger.error(
            "Current staged path is missing tracked files: %s",
            ", ".join(missing_paths),
        )
        return MaterializeFailed(reason="staged_path_missing_tracked_files")

    if (
        current_path_location.blocks_auto_import_dispatch
        and subprocess_started is not False
    ):
        detail = (
            "already lives at the request-scoped auto-import staged "
            "path. Automatic retry is disabled to avoid duplicate "
            "import; manual recovery is required."
        )
        if current_path_location.kind == "legacy_shared_staged":
            detail = (
                "already lives at the legacy shared staged path. "
                "Automatic retry is disabled because the path is "
                "ambiguous across editions; manual recovery is required."
            )
        _log_post_move_resume_blocked(
            album_data,
            current_path=staged_album.current_path,
            detail=detail,
        )
        return MaterializeGuarded(detail="auto_import_dispatch_blocked_post_move")

    album_data.import_folder = staged_album.current_path
    return Materialized()


def _materialize_processing_dir(
    album_data: GrabListEntry,
    staged_album: StagedAlbum,
    ctx: CratediggerContext,
    *,
    persist_current_path: bool = True,
) -> MaterializeResult:
    """Ensure ``staged_album.current_path`` holds the album's local files."""
    canonical_path = canonical_folder_for_row(
        album_data, ctx.cfg.slskd_download_dir)
    logger.info(
        "MANIFEST-TRACE materialize request=%s %s canonical_exists=%s "
        "canonical_existing_audio=%s current_path=%s canonical=%r",
        album_data.db_request_id,
        manifest_trace_summary(album_data.files),
        os.path.isdir(canonical_path),
        len(audio_relative_paths(canonical_path)),
        staged_album.current_path,
        canonical_path,
    )
    db = (ctx.pipeline_db_source._get_db()
          if ctx.pipeline_db_source is not None else None)
    request_id = album_data.db_request_id
    if request_id is None and _is_request_scoped_auto_import_path(
        current_path=staged_album.current_path,
        staging_dir=ctx.cfg.beets_staging_dir,
    ):
        _log_post_move_resume_blocked(
            album_data,
            current_path=staged_album.current_path,
            detail=(
                "already lives at the request-scoped auto-import staged "
                "path but is missing db_request_id. Automatic retry is "
                "disabled because import ownership can no longer be "
                "verified; manual recovery is required."
            ),
        )
        return MaterializeGuarded(detail="missing_db_request_id_for_request_scoped_staged")
    current_path_location = classify_processing_path(
        current_path=staged_album.current_path,
        artist=album_data.artist,
        title=album_data.title,
        year=album_data.year,
        request_id=request_id or 0,
        staging_dir=ctx.cfg.beets_staging_dir,
        slskd_download_dir=ctx.cfg.slskd_download_dir,
        attempt_fingerprint=_attempt_fingerprint_for(album_data.files),
    )

    if current_path_location.kind != "canonical":
        return _evaluate_staged_path_readiness(
            album_data, staged_album, current_path_location, db,
        )

    # Pre-flight: every file must carry a stamped, on-disk local_path from
    # slskd's DownloadFileComplete event — or already-moved evidence at the
    # destination. Checked for the whole album BEFORE any move so a
    # missing stamp never causes move-then-rollback churn. A stamp can be
    # legitimately absent for one cycle (completion-vs-event-write race);
    # the poller retries within ``PROCESSING_MATERIALIZE_GRACE_S`` and
    # self-heals to re-download past it.
    missing_stamps: list[str] = []
    for file in album_data.files:
        dst_file = staged_album.import_path_for(file)
        file.import_path = dst_file
        src = file.local_path
        if src is not None and os.path.exists(src):
            continue
        if os.path.exists(dst_file):
            continue  # Already moved by a prior crashed attempt.
        if src is None:
            missing_stamps.append(f"{file.filename} (not_stamped)")
        else:
            missing_stamps.append(f"{file.filename} (stale_stamp: {src})")
    if missing_stamps:
        logger.error(
            "EVENT-PATH MISSING: request_id=%s %s - %s has no authoritative "
            "local path for %d file(s): %s. The DownloadFileComplete event "
            "was never ingested (pre-bootstrap completion or cursor gap) or "
            "the stamped file vanished from disk.",
            album_data.db_request_id,
            album_data.artist,
            album_data.title,
            len(missing_stamps),
            "; ".join(missing_stamps),
        )
        return MaterializeFailed(reason="event_path_missing")

    rm_dirs: list[str] = []
    moved_files_history: list[tuple[str, str]] = []
    if os.path.exists(canonical_path):
        logger.info(f"Staging folder {canonical_path} already exists — "
                    f"resuming or reusing prior attempt")
    else:
        try:
            os.makedirs(canonical_path, exist_ok=True)
        except OSError:
            # ENAMETOOLONG, EACCES, ENOSPC, etc. Letting this propagate
            # would abort the entire poll loop and starve every later row.
            logger.exception(
                "Failed to create canonical staging dir %s for request %s — "
                "leaving for next poll cycle",
                canonical_path,
                album_data.db_request_id,
            )
            return MaterializeFailed(reason="staging_dir_create_failed")

    for file in album_data.files:
        dst_file = file.import_path
        assert dst_file is not None
        src_file = file.local_path
        if src_file is None or not os.path.exists(src_file):
            # Pre-flight proved the destination copy exists.
            logger.info(f"Already-moved file detected: {dst_file} (src gone, skipping)")
            continue
        src_folder = os.path.dirname(src_file)
        if src_folder not in rm_dirs:
            rm_dirs.append(src_folder)
        try:
            # Destination keeps the clean remote basename (via
            # ``import_path_for``) even when slskd appended a ``_<ticks>``
            # collision suffix to the source.
            shutil.move(src_file, dst_file)
            moved_files_history.append((src_file, dst_file))
        except Exception:
            logger.exception(f"Failed to move: {file.filename} to temp location for import. Rolling back...")
            for src, dst in reversed(moved_files_history):
                try:
                    shutil.move(dst, src)
                except Exception:
                    logger.exception(f"Critical failure during rollback: could not move {dst} back to {src}")
            try:
                os.rmdir(canonical_path)
            except OSError:
                logger.warning(f"Could not remove temp import directory {canonical_path}")
            return MaterializeFailed(reason="file_move_failed")

    for rm_dir in rm_dirs:
        if os.path.abspath(rm_dir) == os.path.abspath(canonical_path):
            continue
        try:
            os.rmdir(rm_dir)
        except OSError:
            logger.warning(f"Skipping removal of {rm_dir} because it's not empty.")

    album_data.import_folder = staged_album.current_path
    if persist_current_path:
        staged_album.persist_current_path(db)
    return Materialized()


def _check_staged_audio_manifest(
    album_data: GrabListEntry,
    staged_album: StagedAlbum,
) -> tuple[bool, str]:
    check = check_audio_manifest(
        staged_album.current_path,
        tracked_audio_paths_for_downloads(album_data.files),
    )
    if check.ok:
        return True, ""
    detail = (
        "Staged import folder does not match the selected audio manifest: "
        f"{check.detail()}"
    )
    logger.error(
        "IMPORT MANIFEST REJECTED: request_id=%s path=%s %s",
        album_data.db_request_id,
        staged_album.current_path,
        detail,
    )
    return False, detail


def process_completed_album(
    album_data: GrabListEntry,
    failed_grab: list[Any],
    ctx: CratediggerContext,
    *,
    import_job_id: int,
    validate_fn: "Callable[..., DispatchOutcome | None] | None" = None,
    handle_valid_fn: "Callable[..., DispatchOutcome | None] | None" = None,
    dispatch_fn: DispatchCoreFn | None = None,
) -> CompletionResult:
    """Process a fully-downloaded album: move files, tag, validate, stage/import.

    Returns the local processing result (see ``CompletionResult`` variants):
    - ``Completed`` — local non-dispatch processing succeeded. Outer caller
      may finalize to ``imported`` only if the request row is still
      ``downloading``. Historical bare ``True``.
    - ``CompletionFailed`` — local non-dispatch processing failed. Outer
      caller resets to ``wanted`` only if the request row is still
      ``downloading``. Historical bare ``False``.
    - ``CompletionDispatched`` — the validation / dispatch path already owned
      the request transition; ``.outcome`` is an import summary for the
      queue owner only. Historical raw ``DispatchOutcome``.
    - ``CompletionDeferred`` — the validation / dispatch path intentionally
      left state untouched for retry / manual recovery. Outer caller must
      NOT touch status. Historical bare ``None``.
    """
    staged_album = StagedAlbum.from_entry(
        album_data,
        default_path=canonical_folder_for_row(
            album_data, ctx.cfg.slskd_download_dir),
    )
    materialized = _materialize_processing_dir(album_data, staged_album, ctx)
    if isinstance(materialized, MaterializeFailed):
        return CompletionFailed(reason=materialized.reason)
    if isinstance(materialized, MaterializeGuarded):
        return CompletionDeferred(detail=materialized.detail)
    assert isinstance(materialized, Materialized)

    logger.info(f"Processing completed download: {album_data.artist} - {album_data.title}")
    if ctx.cfg.beets_validation_enabled and album_data.mb_release_id:
        _validate = validate_fn if validate_fn is not None else _process_beets_validation
        outcome = _validate(
            album_data,
            staged_album,
            ctx,
            import_job_id=import_job_id,
            handle_valid_fn=handle_valid_fn,
            dispatch_fn=dispatch_fn,
        )
        if outcome is not None:
            if outcome.deferred:
                # Release-lock contention. Propagate ``CompletionDeferred``
                # so ``_run_completed_processing`` leaves the request's
                # status, active_download_state, and staged files
                # untouched for the next cycle to retry.
                return CompletionDeferred(detail=outcome.message)
            # DispatchOutcome is an import summary only. Wrap it so the
            # importer queue can record the real terminal job outcome, but do
            # not let it drive fallback request-status transitions below.
            return CompletionDispatched(outcome=outcome)
    return Completed()


def _process_beets_validation(
    album_data: GrabListEntry,
    staged_album: StagedAlbum,
    ctx: CratediggerContext,
    *,
    import_job_id: int,
    handle_valid_fn: "Callable[..., DispatchOutcome | None] | None" = None,
    dispatch_fn: DispatchCoreFn | None = None,
) -> "DispatchOutcome | None":
    """Beets validation sub-path of process_completed_album.

    After beets validation passes, ``ensure_candidate_evidence_for_action``
    confirms the preview worker has persisted candidate evidence keyed to
    this import_job. Missing evidence requeues the job to preview rather
    than measuring inline — the importer never measures, the preview
    worker owns evidence production, and the full pipeline decider
    (``full_pipeline_decision_from_evidence``) runs downstream of evidence.

    Returns the dispatch outcome when the auto-import path fires,
    ``None`` when beets validation rejects (``_handle_rejected_result``
    already handles the state transition) or when the non-auto
    redownload path takes over in ``_handle_valid_result``. Guarded
    ownership-less rejects also return a deferred outcome so callers
    keep the row untouched for manual recovery.
    """
    from lib.beets import beets_validate as _bv
    current_path = staged_album.current_path
    manifest_ok, manifest_detail = _check_staged_audio_manifest(
        album_data,
        staged_album,
    )
    logger.info(
        "MANIFEST-TRACE check request=%s ok=%s %s actual_audio=%s path=%s",
        album_data.db_request_id,
        manifest_ok,
        manifest_trace_summary(album_data.files),
        len(audio_relative_paths(current_path)),
        current_path,
    )
    if not manifest_ok:
        return _reject_request_auto_import(
            album_data,
            ValidationResult(
                valid=False,
                scenario="untracked_audio",
                detail=manifest_detail,
                error=manifest_detail,
                path=current_path,
            ),
            staged_album,
            ctx,
            detail=manifest_detail,
            scenario="untracked_audio",
            error=manifest_detail,
        )
    bv_result = _bv(ctx.cfg.beets_harness_path, current_path,
                    album_data.mb_release_id, ctx.cfg.beets_distance_threshold)
    usernames_pre = set(f.username for f in album_data.files if f.username)
    bv_result.soulseek_username = ", ".join(sorted(usernames_pre)) if usernames_pre else None
    bv_result.download_folder = current_path
    bv_result.source_dirs = source_dirs_for_album(album_data)
    if bv_result.valid:
        db = ctx.pipeline_db_source._get_db()
        candidate_result = ensure_candidate_evidence_for_action(
            db,
            source_path=current_path,
            import_job_id=import_job_id,
        )
        if not candidate_result.available:
            reason = (
                candidate_result.provenance.fallback_reason
                or candidate_result.provenance.candidate_status
                or "missing"
            )
            # Preview owns candidate-evidence production; the importer
            # never measures. Requeue rather than fail; the dispatch-side
            # requeue keeps the advisory-lock atomicity intact.
            return _requeue_import_job_to_preview(
                db,
                import_job_id=import_job_id,
                reason=reason,
            )
        _handle_valid = (
            handle_valid_fn if handle_valid_fn is not None else _handle_valid_result
        )
        return _handle_valid(
            album_data,
            bv_result,
            staged_album,
            ctx,
            import_job_id=import_job_id,
            prevalidated_candidate_result=candidate_result,
            dispatch_fn=dispatch_fn,
        )
    return _handle_rejected_result(
        album_data,
        bv_result,
        staged_album,
        ctx,
        import_job_id=import_job_id,
    )


def _handle_valid_result(
    album_data: GrabListEntry,
    bv_result: ValidationResult,
    staged_album: StagedAlbum,
    ctx: CratediggerContext,
    *,
    import_job_id: int | None = None,
    prevalidated_candidate_result: CandidateEvidenceActionResult | None = None,
    quality_gate_fn: QualityGateFn | None = None,
    dispatch_fn: DispatchCoreFn | None = None,
) -> "DispatchOutcome | None":
    """Handle a valid beets validation result: stage and optionally auto-import.

    Returns the ``DispatchOutcome`` summary from ``dispatch_import_core``
    when the auto-import path fires (source='request', distance within
    threshold), or ``None`` for the redownload path that just stages
    and marks done. ``process_completed_album()`` propagates the summary
    upward for the importer queue, but request-state changes remain owned
    by the dispatch/finalization seam itself.

    This function acquires the RELEASE advisory lock outer for the
    auto-import path *before* ``StagedAlbum.move_to`` runs, so
    contention is a true no-op: files stay at their current local
    processing path, ``active_download_state.current_path`` stays
    unchanged, and the next cycle can idempotently re-enter without
    any extra filesystem churn. Redownload paths don't take the lock
    — they just move into staging and mark done, so no cross-process
    race applies.

    See ``docs/advisory-locks.md`` for namespaces, keys, ordering,
    and contention behaviour (including the staged-move rationale for
    acquiring at this level rather than inside
    ``dispatch_import_core``).
    """
    from contextlib import nullcontext
    from lib.pipeline_db import (ADVISORY_LOCK_NAMESPACE_RELEASE,
                                 release_id_to_lock_key)

    source_type = album_data.db_source or "redownload"
    request_id = album_data.db_request_id
    dist = bv_result.distance if bv_result.distance is not None else 1.0
    wants_auto_import = (
        source_type == "request"
        and dist <= ctx.cfg.beets_distance_threshold)

    if wants_auto_import and request_id is None:
        return _reject_request_auto_import(
            album_data,
            bv_result,
            staged_album,
            ctx,
            detail=(
                "Request auto-import is missing db_request_id; automatic "
                "resume/import is disabled."
            ),
            scenario="request_missing_request_id",
            error="missing_request_id",
        )

    current_path_location = classify_processing_path(
        current_path=staged_album.current_path,
        artist=album_data.artist,
        title=album_data.title,
        year=album_data.year,
        request_id=request_id or 0,
        staging_dir=ctx.cfg.beets_staging_dir,
        slskd_download_dir=ctx.cfg.slskd_download_dir,
        attempt_fingerprint=_attempt_fingerprint_for(album_data.files),
    )

    if wants_auto_import and not album_data.mb_release_id:
        return _reject_request_auto_import(
            album_data,
            bv_result,
            staged_album,
            ctx,
            detail="Request auto-import requires a MusicBrainz release ID",
            scenario="request_missing_mbid",
            error="missing_mbid",
        )

    will_auto_import = wants_auto_import
    pdb = None

    if (
        will_auto_import
        and current_path_location.blocks_auto_import_dispatch
        and _import_subprocess_already_started(
            ctx.pipeline_db_source._get_db()
            if ctx.pipeline_db_source is not None
            else None,
            request_id,
        )
    ):
        _log_post_move_resume_blocked(
            album_data,
            current_path=staged_album.current_path,
            detail=(
                f"already lives at the {current_path_location.display_name}. "
                "Automatic retry is disabled to avoid duplicate import; "
                "manual recovery is required."
            ),
        )
        return DispatchOutcome(
            success=False,
            message=(
                "Auto-import may already have started for this staged "
                f"album ({album_data.mb_release_id})"
            ),
            deferred=True,
        )

    if will_auto_import and album_data.mb_release_id:
        pdb = ctx.pipeline_db_source._get_db()
        lock_ctx = pdb.advisory_lock(
            ADVISORY_LOCK_NAMESPACE_RELEASE,
            release_id_to_lock_key(album_data.mb_release_id))
    else:
        lock_ctx = nullcontext(True)

    with lock_ctx as got_release_lock:
        if not got_release_lock:
            logger.warning(
                f"AUTO-IMPORT DEFERRED: {album_data.artist} - "
                f"{album_data.title} — release lock held by another "
                f"process (mbid={album_data.mb_release_id}); skipping "
                "staged move and dispatch. Files stay at "
                f"{staged_album.current_path} so the next cycle can "
                "idempotently resume from process_completed_album.")
            return DispatchOutcome(
                success=False,
                message=("Another import is already in progress for "
                         f"this release ({album_data.mb_release_id})"),
                deferred=True,
            )

        db = (ctx.pipeline_db_source._get_db()
              if ctx.pipeline_db_source is not None else None)
        dest = staged_album.move_to(
            stage_to_ai_path(
                artist=album_data.artist,
                title=album_data.title,
                staging_dir=ctx.cfg.beets_staging_dir,
                request_id=request_id,
                auto_import=will_auto_import,
            ),
            db=db,
        )
        album_data.import_folder = dest
        log_validation_result(album_data, bv_result, ctx.cfg, dest_path=dest)
        logger.info(f"STAGED: {album_data.artist} - {album_data.title} "
                    f"(scenario={bv_result.scenario}, "
                    f"distance={bv_result.distance:.4f}) → {dest}")

        dl_info = _build_download_info(album_data)
        dl_info.validation_result = bv_result.to_json()
        if album_data.download_spectral is not None:
            dl_info.download_spectral = album_data.download_spectral
            dl_info.current_spectral = album_data.current_spectral
            dl_info.existing_min_bitrate = album_data.current_min_bitrate
            dl_info.slskd_filetype = dl_info.filetype
            dl_info.actual_filetype = dl_info.filetype
        if will_auto_import:
            assert request_id is not None, "pipeline request must have db_request_id"
            assert pdb is not None, "auto-import path must hold a pipeline DB handle"
            override_min_bitrate: int | None = None
            try:
                req = pdb.get_request(request_id)
                if req:
                    override_min_bitrate = compute_effective_override_bitrate(
                        req.get("min_bitrate"),
                        req.get("current_spectral_bitrate"),
                        req.get("current_spectral_grade"),
                    )
            except Exception:
                logger.debug("DB lookup failed for override-min-bitrate")

            resolved_quality_gate_fn = (
                quality_gate_fn
                if quality_gate_fn is not None
                else _check_quality_gate_core
            )
            if dispatch_fn is not None:
                # Test-only dependency injection seam. Its exact protocol is
                # ``DispatchCoreFn``; production always takes the direct,
                # pyright-checked call below.
                return dispatch_fn(
                    path=dest,
                    mb_release_id=album_data.mb_release_id or "",
                    request_id=request_id,
                    label=f"{album_data.artist} - {album_data.title}",
                    force=False,
                    override_min_bitrate=override_min_bitrate,
                    target_format=album_data.db_target_format,
                    verified_lossless_target=ctx.cfg.verified_lossless_target,
                    beets_harness_path=ctx.cfg.beets_harness_path,
                    db=pdb,
                    dl_info=dl_info,
                    distance=bv_result.distance,
                    scenario=bv_result.scenario or "auto_import",
                    files=album_data.files,
                    cfg=ctx.cfg,
                    outcome_label="success",
                    requeue_on_failure=True,
                    cooled_down_users=ctx.cooled_down_users,
                    source_dirs=source_dirs_for_album(album_data),
                    candidate_import_job_id=import_job_id,
                    candidate_download_log_id=None,
                    prevalidated_candidate_result=prevalidated_candidate_result,
                    quality_gate_fn=resolved_quality_gate_fn,
                )
            return dispatch_import_core(
                path=dest,
                mb_release_id=album_data.mb_release_id or "",
                request_id=request_id,
                label=f"{album_data.artist} - {album_data.title}",
                force=False,
                override_min_bitrate=override_min_bitrate,
                target_format=album_data.db_target_format,
                verified_lossless_target=ctx.cfg.verified_lossless_target,
                beets_harness_path=ctx.cfg.beets_harness_path,
                db=pdb,
                dl_info=dl_info,
                distance=bv_result.distance,
                scenario=bv_result.scenario or "auto_import",
                files=album_data.files,
                cfg=ctx.cfg,
                outcome_label="success",
                requeue_on_failure=True,
                cooled_down_users=ctx.cooled_down_users,
                source_dirs=source_dirs_for_album(album_data),
                candidate_import_job_id=import_job_id,
                candidate_download_log_id=None,
                prevalidated_candidate_result=prevalidated_candidate_result,
                quality_gate_fn=resolved_quality_gate_fn,
            )
        ctx.pipeline_db_source.mark_done(
            album_data, bv_result, dest_path=dest, download_info=dl_info)
        return None
