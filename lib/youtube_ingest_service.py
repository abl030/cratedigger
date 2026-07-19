"""YouTube rescue ingest service layer.

This is the source of truth for the YouTube-rescue ingest path. The CLI
(``pipeline-cli youtube-rescue``, U4) and HTTP API
(``POST /api/pipeline/<id>/youtube-rescue``, U5) are thin adapters that
wrap ``YoutubeIngestService.submit`` and map its outcome to exit-code /
status-code via the ``OUTCOME_EXIT_CODE`` / ``OUTCOME_HTTP_STATUS`` dicts
exported below. The drainer worker (U6) wraps
``YoutubeIngestService.run_job``.

Two methods:

* ``submit(request_id, browse_id) -> SubmitResult`` — pure validation +
  one DB insert via ``insert_youtube_running``. No subprocess / network IO
  inside the validation gates. Used by CLI + API.

* ``run_job(download_log_id) -> RunResult`` — per-job runtime. Invokes
  yt-dlp via the injected ``ytdlp_runner_fn``, enforces R10 track-count
  gate before staging, stages files, enqueues a ``youtube_import`` job,
  writes terminal ``download_log`` state via ``update_youtube_terminal``.
  Used by the U6 worker.

Wire boundary discipline: ``YoutubeIngestMetadata`` is a
``msgspec.Struct, kw_only=True`` written into ``download_log.youtube_metadata``
JSONB. Round-trip through ``msgspec.to_builtins`` / ``msgspec.convert``;
do NOT use ``dataclasses.asdict`` (it doesn't recurse into Structs).

Per the kwarg-DI seam pattern (``.claude/rules/code-quality.md``), every
external collaborator (MB track-count lookup, yt-dlp invocation, stage-dir
move, clock) is a kwarg with a production default — tests inject fakes
deterministically.
"""

from __future__ import annotations

import logging
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Literal, Optional, Protocol

import msgspec

from lib.import_queue import (
    IMPORT_JOB_YOUTUBE,
    youtube_import_dedupe_key,
    youtube_import_payload,
)
from lib.release_identity import detect_release_source, normalize_release_id
from lib import pipeline_db as _pipeline_db_mod  # noqa: F401 — module-import so
# ``except _pipeline_db_mod.YoutubeInFlightError`` resolves the class at catch
# time. A symbol import (``from lib.pipeline_db import YoutubeInFlightError``)
# would bind once at module load; tests/test_pipeline_db.py does
# ``importlib.reload(pipeline_db)`` and the symbol then points at the
# pre-reload class while the fake raises the post-reload class. Module-level
# attribute lookup survives the reload because the module object is the same.

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Outcome vocabulary — shared with CLI (U4) and HTTP route (U5).
# ---------------------------------------------------------------------------

SubmitOutcome = Literal[
    "accepted",
    "request_not_found",
    "wrong_state",
    "in_flight",
    "no_resolver_mapping",
    "track_count_precheck_failed",
    "transient",
]
"""Submission outcomes. Returned by ``YoutubeIngestService.submit``."""

RunOutcome = Literal[
    "youtube_success",
    "youtube_failed",
]
"""Per-job runtime outcomes. Returned by ``YoutubeIngestService.run_job``.

Mirrors the terminal ``download_log.outcome`` values written by
``update_youtube_terminal``."""


OUTCOME_HTTP_STATUS: dict[str, int] = {
    "accepted": 200,
    "request_not_found": 404,
    "wrong_state": 409,
    "in_flight": 409,
    "no_resolver_mapping": 422,
    "track_count_precheck_failed": 422,
    "transient": 503,
}
"""Service ``SubmitOutcome`` → HTTP status. U5 imports this directly.

The outcome set is pinned by
``TestOutcomeMapsAreComplete::test_outcome_set_is_stable`` which asserts
``set(OUTCOME_HTTP_STATUS) == set(OUTCOME_EXIT_CODE)``."""


OUTCOME_EXIT_CODE: dict[str, int] = {
    "accepted": 0,
    "request_not_found": 2,
    "wrong_state": 4,
    "in_flight": 4,
    "no_resolver_mapping": 3,
    "track_count_precheck_failed": 3,
    "transient": 5,
}
"""Service ``SubmitOutcome`` → CLI exit code. U4 imports this directly."""


# Request statuses that may be advanced into a YT rescue submission.
# ``unsearchable`` stops Soulseek search, not operator-requested imports.
YOUTUBE_IMPORT_ALLOWED_REQUEST_STATUSES: frozenset[str] = frozenset(
    {"wanted", "unsearchable"}
)


# ---------------------------------------------------------------------------
# Wire-boundary structs (cross JSONB into download_log.youtube_metadata).
# ---------------------------------------------------------------------------


class YoutubeIngestMetadata(msgspec.Struct, kw_only=True):
    """The JSONB shape persisted at ``download_log.youtube_metadata``.

    Two-phase layered: at submission time the writer
    (``insert_youtube_running``) populates ``yt_url`` / ``browse_id`` /
    ``audio_playlist_id`` / ``expected_track_count``. At terminal-state
    write time the worker layers ``reason`` / ``stderr_excerpt`` /
    ``observed_track_count`` / ``per_track_video_ids`` /
    ``resolver_mapping_id`` on top via the ``||`` JSONB merge operator
    in ``update_youtube_terminal``.

    All terminal-state-only fields default to ``None`` so the submission-
    time blob round-trips through ``msgspec.convert`` cleanly.
    """

    # Submission-time required fields.
    yt_url: str
    browse_id: str

    # Submission-time optional fields. ``audio_playlist_id`` is the
    # ``yt_audio_playlist_id`` from the resolver row — NULLable per
    # migration 034 because some YT albums lack a playlist handle.
    # ``expected_track_count`` is the precheck-validated count carried
    # through so the worker can enforce R10 without re-reading the
    # resolver row.
    audio_playlist_id: Optional[str] = None
    expected_track_count: Optional[int] = None
    resolver_mapping_id: Optional[int] = None
    per_track_video_ids: Optional[list[str]] = None

    # Terminal-state fields. Populated by the worker on
    # ``youtube_success`` or ``youtube_failed`` via the JSONB merge.
    reason: Optional[str] = None
    stderr_excerpt: Optional[str] = None
    observed_track_count: Optional[int] = None
    worker_claimed_at: Optional[str] = None
    worker_id: Optional[str] = None
    cleanup_error: Optional[str] = None


class YoutubeImportPayload(msgspec.Struct, kw_only=True):
    """Typed view of ``import_jobs.payload`` for ``youtube_import`` rows.

    The dispatcher (U9) reads this off ``ImportJob.payload``. The fields
    mirror the keys produced by ``lib.import_queue.youtube_import_payload``.
    """

    staged_path: str
    request_id: int
    browse_id: str
    # Added after the original queue payload shipped. Optional so legacy
    # queued rows still decode; new rows always carry it.
    download_log_id: Optional[int] = None


# ---------------------------------------------------------------------------
# Result types.
# ---------------------------------------------------------------------------


class SubmitResult(msgspec.Struct, kw_only=True):
    """Outcome of one ``YoutubeIngestService.submit`` call.

    ``download_log_id`` is populated on ``accepted`` (the new row's id)
    and on ``in_flight`` (the existing in-flight row's id, so callers
    can render "you already have a rescue running, check id=N").
    """

    outcome: SubmitOutcome
    download_log_id: Optional[int] = None
    import_job_id: Optional[int] = None
    blocking_resource: Optional[str] = None
    detail: Optional[str] = None


class RunResult(msgspec.Struct, kw_only=True):
    """Outcome of one ``YoutubeIngestService.run_job`` call.

    ``reason`` carries the classified failure reason on ``youtube_failed``
    (one of the R20 taxonomy values, or ``track_count_mismatch``,
    ``worker_unhandled_exception``, ``worker_interrupted``); ``None`` on success.
    """

    outcome: RunOutcome
    reason: Optional[str] = None


# ---------------------------------------------------------------------------
# yt-dlp runner contract (the U6 worker implements this).
# ---------------------------------------------------------------------------


class YtdlpRunResult(msgspec.Struct, kw_only=True):
    """Typed return of the yt-dlp subprocess invocation.

    ``exit_code`` is the subprocess returncode; ``stderr_excerpt`` is a
    bounded (e.g. 4 KiB) capture after the ``errors='replace'`` decode
    discipline (KTD8). ``staged_files`` is the list of audio files
    yt-dlp wrote to the temp directory — the service applies the R10
    track-count gate to this list before any files move into the staging
    root.
    """

    exit_code: int
    stderr_excerpt: Optional[str]
    staged_files: list[Path]
    work_dir: Optional[Path] = None


# ---------------------------------------------------------------------------
# Pure failure classification (R20).
# ---------------------------------------------------------------------------


# Substring → classified reason. Ordered most-specific-first; the first
# match wins. The ``unknown`` bucket is the fallback for unrecognised
# yt-dlp stderr — it still captures the verbatim stderr in metadata so
# operators can refine the taxonomy after the fact.
_YOUTUBE_FAILURE_TAXONOMY: tuple[tuple[str, str], ...] = (
    ("HTTP Error 404", "youtube_404"),
    ("Video unavailable", "youtube_video_removed"),
    ("This video has been removed", "youtube_video_removed"),
    ("Sign in to confirm your age", "youtube_age_gated"),
    ("age-restricted", "youtube_age_gated"),
    ("Video is age restricted", "youtube_age_gated"),
    ("not available in your country", "youtube_region_locked"),
    ("blocked it in your country", "youtube_region_locked"),
    ("region", "youtube_region_locked"),
    ("Unable to extract", "youtube_unknown"),
    ("Connection reset", "youtube_transient_network"),
    ("Connection timed out", "youtube_transient_network"),
    ("HTTP Error 429", "youtube_transient_network"),
    ("HTTP Error 5", "youtube_transient_network"),
    ("Read timed out", "youtube_transient_network"),
    ("Temporary failure", "youtube_transient_network"),
)


def classify_youtube_failure(stderr_excerpt: Optional[str]) -> str:
    """Map a yt-dlp stderr excerpt to one of the R20 reason codes.

    Pure function — no IO, deterministic. Returns ``youtube_unknown``
    for unrecognised input (including ``None`` / empty stderr); the
    caller still persists the verbatim stderr alongside the reason so
    operators can refine the taxonomy after the fact.
    """
    if not stderr_excerpt:
        return "youtube_unknown"
    haystack = stderr_excerpt
    for needle, reason in _YOUTUBE_FAILURE_TAXONOMY:
        if needle in haystack:
            return reason
    return "youtube_unknown"


# ---------------------------------------------------------------------------
# Type aliases for kwarg-DI seams.
# ---------------------------------------------------------------------------


YtdlpRunnerFn = Callable[..., YtdlpRunResult]
"""yt-dlp invocation. Signature accepts kwargs ``url``, ``output_dir``,
``expected_track_count`` (the worker may pass more). Returns
:class:`YtdlpRunResult`."""


MbTrackCountFn = Callable[[str], Optional[int]]
"""``mb_track_count_fn(mbid) -> total tracks for that MB release | None``.

Used by R7 (submission-side precheck) and R10 (worker-side gate). The
production wiring is deferred to U7/U9 (see ``_default_mb_track_count``
sentinel); tests inject a fake that returns canned values."""


StageDirFn = Callable[[Path, Path], None]
"""``stage_dir_fn(src_dir, dest_dir) -> None``. Moves a directory of
audio files from yt-dlp's temp output to
``/Incoming/auto-import/<artist>-<album>/``. Production default is
:func:`_default_stage_dir`."""


ReleaseGroupResolverFn = Callable[[dict[str, Any]], Optional[tuple[str, str]]]
"""``release_group_resolver_fn(request_row) -> (source, rg_id) | None``.

Used by ``submit`` to find the ``(release_group_identifier, source)``
key for ``pdb.get_youtube_album_mapping(...)`` for MB requests. Discogs
requests bridge through cached exact-distance resolver rows via
``find_youtube_album_mapping_for_release`` instead of this port.

Default implementation (:func:`_default_release_group_resolver`) reads
``mb_release_group_id`` directly off the request row when present."""


ClockFn = Callable[[], datetime]
"""``clock_fn() -> datetime``. Wall-clock seam for tests."""


class _TrackCountPrecheckFailure(Exception):
    """Submission-side deterministic precheck failure."""


class _TransientPrecheckFailure(Exception):
    """Submission-side transient dependency failure."""


class _PipelineDB(Protocol):
    """Structural surface of ``PipelineDB`` consumed by this service.

    Mirrors the pattern in ``lib.triage_service._PipelineDB`` — keeps the
    service body decoupled from the production class so tests can drop
    in ``FakePipelineDB`` without monkey-patching.
    """

    def get_request(self, request_id: int) -> Optional[dict[str, Any]]: ...

    def get_youtube_album_mapping(
        self, release_group_identifier: str, source: str,
    ) -> Optional[list[dict[str, Any]]]: ...

    def find_youtube_album_mapping_for_release(
        self,
        *,
        source: str,
        release_id: str,
        browse_id: str,
    ) -> Optional[dict[str, Any]]: ...

    def get_tracks(self, request_id: int) -> list[dict[str, Any]]: ...

    def insert_youtube_running(
        self,
        *,
        request_id: int,
        browse_id: str,
        audio_playlist_id: Optional[str],
        yt_url: str,
        expected_track_count: int,
        resolver_mapping_id: Optional[int] = None,
        per_track_video_ids: Optional[list[str]] = None,
    ) -> int: ...

    def update_youtube_terminal(
        self,
        download_log_id: int,
        outcome: str,
        metadata_dict: dict[str, Any],
    ) -> None: ...

    def get_download_log_entry(
        self, log_id: int,
    ) -> Optional[dict[str, Any]]: ...

    def enqueue_import_job(
        self,
        job_type: str,
        *,
        request_id: Optional[int] = None,
        dedupe_key: Optional[str] = None,
        payload: Optional[dict[str, Any]] = None,
        message: Optional[str] = None,
    ) -> Any: ...

    def enqueue_youtube_import_and_mark_success(
        self,
        *,
        download_log_id: int,
        request_id: int,
        dedupe_key: str,
        payload: dict[str, Any],
        message: str,
        terminal_metadata: dict[str, Any],
    ) -> Any: ...

    def find_active_youtube_import_job(
        self,
        *,
        request_id: int,
        browse_id: str,
    ) -> Any | None: ...


# ---------------------------------------------------------------------------
# Default kwarg-DI implementations.
# ---------------------------------------------------------------------------


def _default_ytdlp_runner(**_kwargs: Any) -> YtdlpRunResult:
    """Sentinel default — production wiring is the U6 worker.

    Raising here means a caller forgot to inject ``ytdlp_runner_fn``.
    Documented behaviour, not an oversight: the service is the source of
    truth for ``run_job``'s logic, but the actual subprocess invocation
    lives in the worker (it owns the timeout, the argv, the
    ``text=True, errors='replace'`` discipline, the temp-dir lifecycle).
    The worker provides ``ytdlp_runner_fn`` when constructing the
    service.
    """
    raise NotImplementedError(
        "ytdlp_runner_fn must be injected — the U6 worker provides the "
        "production implementation; tests inject a fake")


def _default_mb_track_count(_mbid: str) -> Optional[int]:
    """Sentinel default — production wiring lives in
    :func:`default_mb_track_count_from_mirror` below.

    Kept as a NotImplementedError sentinel so callers who construct a
    bare ``YoutubeIngestService(pdb)`` without an MB-wiring fail fast
    at the first precheck/gate rather than silently passing a
    ``None`` MB count through. The production callers (CLI in
    ``scripts/pipeline_cli/youtube.py``, HTTP route in
    ``web/routes/youtube.py``) go through
    :func:`default_youtube_ingest_service_factory` which wires
    the live MB-mirror flavour. Tests inject a fake that returns canned
    values.
    """
    raise NotImplementedError(
        "mb_track_count_fn must be injected — production wiring lives "
        "in lib.youtube_ingest_service.default_mb_track_count_from_mirror; "
        "use default_youtube_ingest_service_factory(pdb) in production "
        "callers, or inject a fake in tests")


def default_mb_track_count_from_mirror(mbid: str) -> Optional[int]:
    """Production ``mb_track_count_fn`` — counts tracks via the MB mirror.

    Thin wrapper around ``web.mb.get_release`` that counts entries in the
    slimmed ``tracks`` array. Returns ``None`` if the MB mirror responds
    without a usable track list — the service then surfaces
    ``track_count_precheck_failed`` and the operator escalates.

    Shared by both production callers (CLI in
    ``scripts/pipeline_cli/youtube.py::cmd_youtube_rescue`` and HTTP route
    in ``web/routes/youtube.py::post_pipeline_youtube_rescue``) per CLI ⇄
    API symmetry — duplicating the helper across the two wrappers would
    let them drift in subtle ways (different timeout, different cache
    behaviour). Tests inject a fake instead of calling this helper.
    """
    from web import mb as mb_api

    release = mb_api.get_release(mbid, fresh=False)
    if not isinstance(release, dict):
        return None
    tracks = release.get("tracks")
    if not isinstance(tracks, list):
        return None
    return len(tracks)


def default_youtube_ingest_service_factory(pdb: _PipelineDB) -> "YoutubeIngestService":
    """Construct a production ``YoutubeIngestService`` for CLI / API.

    Wires the live MB-mirror ``mb_track_count_fn`` (other ports retain
    their library-side production defaults). Both
    ``scripts/pipeline_cli/youtube.py::cmd_youtube_rescue`` and
    ``web/routes/youtube.py::post_pipeline_youtube_rescue`` call this
    so the two surfaces share one wiring per CLI ⇄ API symmetry. Tests
    inject a service via ``service_factory=`` (CLI) or patch the
    service method directly (HTTP contract tests).
    """
    return YoutubeIngestService(
        pdb, mb_track_count_fn=default_mb_track_count_from_mirror)


def _default_stage_dir(src: Path, dest: Path) -> None:
    """Default ``stage_dir_fn``: ``shutil.move``.

    Production behaviour: move the directory tree from yt-dlp's temp
    output to the configured auto-import staging child. ``shutil.move``
    handles cross-filesystem boundaries (copy-then-delete fallback). The
    parent directory is created if missing.
    """
    dest.parent.mkdir(parents=True, exist_ok=True)
    if dest.exists():
        if dest.is_dir():
            shutil.rmtree(dest)
        else:
            dest.unlink()
    shutil.move(str(src), str(dest))


def _default_release_group_resolver(
    request_row: dict[str, Any],
) -> Optional[tuple[str, str]]:
    """Default release-group resolver: trust ``mb_release_group_id`` only.

    Returns ``("mb", rg_id)`` when the request row carries a populated
    ``mb_release_group_id``. Discogs-only requests are handled before
    this resolver through the cached resolver-distance bridge.
    """
    mb_rg = request_row.get("mb_release_group_id")
    if isinstance(mb_rg, str) and mb_rg.strip():
        return ("mb", mb_rg.strip())
    return None


def _default_clock() -> datetime:
    return datetime.now(timezone.utc)


# ---------------------------------------------------------------------------
# Service.
# ---------------------------------------------------------------------------


class YoutubeIngestService:
    """YouTube rescue ingest service. CLI/API/worker wrap this class.

    Construct one per process (tests or production). The service holds
    only the injected ports; ``submit`` and ``run_job`` are stateless
    beyond their reads/writes to ``pdb``.
    """

    def __init__(
        self,
        pdb: _PipelineDB,
        *,
        ytdlp_runner_fn: YtdlpRunnerFn = _default_ytdlp_runner,
        mb_track_count_fn: MbTrackCountFn = _default_mb_track_count,
        stage_dir_fn: StageDirFn = _default_stage_dir,
        release_group_resolver_fn: ReleaseGroupResolverFn = (
            _default_release_group_resolver),
        staging_root: Path = Path("/mnt/virtio/Music/Incoming/auto-import"),
        clock_fn: ClockFn = _default_clock,
    ) -> None:
        self.pdb = pdb
        self.ytdlp_runner_fn = ytdlp_runner_fn
        self.mb_track_count_fn = mb_track_count_fn
        self.stage_dir_fn = stage_dir_fn
        self.release_group_resolver_fn = release_group_resolver_fn
        self.staging_root = staging_root
        self.clock_fn = clock_fn

    # ----- submit (CLI + API entry point) --------------------------------

    def submit(self, request_id: int, browse_id: str) -> SubmitResult:
        """Validate + persist a YT rescue submission.

        No subprocess / network IO: every short-circuit reads from the
        DB only. The order matches the plan's outcome table —
        request-existence → status → resolver mapping → precheck →
        idempotent insert. Each outcome is terminal (no fall-through).
        """
        # 1. Request existence.
        try:
            request_row = self.pdb.get_request(int(request_id))
        except Exception as exc:  # noqa: BLE001 — surface DB hiccup as transient
            log.warning(
                "youtube_ingest_service: get_request(%s) raised %s",
                request_id, exc)
            return SubmitResult(
                outcome="transient",
                detail=f"DB error reading request {request_id}: {exc}",
            )
        if request_row is None:
            return SubmitResult(
                outcome="request_not_found",
                detail=f"no album_requests row for id={request_id}",
            )

        # 2. Request status gate (R3).
        status = str(request_row.get("status") or "")
        if status not in YOUTUBE_IMPORT_ALLOWED_REQUEST_STATUSES:
            return SubmitResult(
                outcome="wrong_state",
                detail=(
                    f"request {request_id} is in status {status!r}; "
                    f"submit requires one of "
                    f"{sorted(YOUTUBE_IMPORT_ALLOWED_REQUEST_STATUSES)!r}"
                ),
            )

        # 3. Import-job idempotency. Once yt-dlp has succeeded, the
        # download_log row is terminal but the importer handoff may still be
        # queued/running. Treat that as in-flight for the same request/browse
        # id so repeated clicks don't stage duplicate albums.
        try:
            active_import = self.pdb.find_active_youtube_import_job(
                request_id=int(request_id),
                browse_id=str(browse_id),
            )
        except Exception as exc:  # noqa: BLE001 — surface DB hiccup as transient
            log.warning(
                "youtube_ingest_service: find_active_youtube_import_job(%s, %s) "
                "raised %s", request_id, browse_id, exc)
            return SubmitResult(
                outcome="transient",
                detail=(
                    f"DB error checking active youtube_import for "
                    f"request {request_id}: {exc}"
                ),
            )
        if active_import is not None:
            import_job_id = getattr(active_import, "id", None)
            blocking_download_log_id = _download_log_id_from_import_job(
                active_import)
            return SubmitResult(
                outcome="in_flight",
                download_log_id=blocking_download_log_id,
                import_job_id=(
                    int(import_job_id) if isinstance(import_job_id, int)
                    else None
                ),
                blocking_resource="youtube_import",
                detail=(
                    f"active youtube_import job id={import_job_id} already "
                    f"exists for request {request_id}"
                ),
            )

        # 4. Resolver-mapping lookup (R6). MB requests keep the original
        # release-group key path. Discogs-only requests bridge through cached
        # resolver rows whose exact distance entry targets the request's
        # Discogs release id; this works for both master-widened and orphan
        # leaf mappings without adding schema columns.
        mapping_result = self._lookup_submit_mapping(
            request_id=int(request_id),
            request_row=request_row,
            browse_id=str(browse_id),
        )
        if isinstance(mapping_result, SubmitResult):
            return mapping_result
        source, match, target_release_id = mapping_result

        # 5. Track-count precheck (R7).
        try:
            expected_track_count = self._expected_track_count_for_submit(
                request_id=int(request_id),
                request_row=request_row,
                source=source,
                mapping_row=match,
                target_release_id=target_release_id,
            )
        except _TransientPrecheckFailure as exc:
            return SubmitResult(outcome="transient", detail=str(exc))
        except _TrackCountPrecheckFailure as exc:
            return SubmitResult(
                outcome="track_count_precheck_failed",
                detail=str(exc),
            )

        # 6. Idempotent insert + happy-path return.
        yt_url = str(match.get("yt_url") or "")
        audio_playlist_id_raw = match.get("yt_audio_playlist_id")
        audio_playlist_id = (
            str(audio_playlist_id_raw)
            if isinstance(audio_playlist_id_raw, str)
            and audio_playlist_id_raw.strip()
            else None
        )
        try:
            new_id = self.pdb.insert_youtube_running(
                request_id=int(request_id),
                browse_id=browse_id,
                audio_playlist_id=audio_playlist_id,
                yt_url=yt_url,
                expected_track_count=int(expected_track_count),
                resolver_mapping_id=_mapping_row_id(match),
                per_track_video_ids=_per_track_video_ids(match),
            )
        except _pipeline_db_mod.YoutubeInFlightError as exc:
            existing_id = exc.existing_download_log_id
            return SubmitResult(
                outcome="in_flight",
                download_log_id=existing_id,
                blocking_resource="youtube_running",
                detail=(
                    f"existing download_log_id={existing_id} is in "
                    f"youtube_running state for request {request_id}"
                ),
            )
        except Exception as exc:  # noqa: BLE001 — surface DB hiccup as transient
            log.warning(
                "youtube_ingest_service: insert_youtube_running(req=%s) "
                "raised %s", request_id, exc)
            return SubmitResult(
                outcome="transient",
                detail=(
                    f"DB error inserting youtube_running row for "
                    f"request {request_id}: {exc}"
                ),
            )
        return SubmitResult(
            outcome="accepted",
            download_log_id=new_id,
            detail=None,
        )

    # ----- run_job (worker entry point) ----------------------------------

    def run_job(self, download_log_id: int) -> RunResult:
        """Process one ``youtube_running`` row to terminal state.

        Reads the row, invokes yt-dlp, enforces R10 track-count gate
        BEFORE staging, stages the directory, enqueues an importer job,
        writes the terminal ``download_log`` state. All exceptions
        beyond the documented ``YtdlpRunResult`` branches are caught and
        surfaced as ``youtube_failed`` with reason
        ``worker_unhandled_exception``.
        """
        row = self.pdb.get_download_log_entry(int(download_log_id))
        if row is None:
            return self._terminal_failed(
                download_log_id,
                reason="missing_download_log_row",
                stderr_excerpt=None,
                observed_track_count=None,
                detail=(
                    f"download_log_id={download_log_id} not found at "
                    f"run_job time; orphan?"
                ),
            )

        metadata_raw = row.get("youtube_metadata")
        try:
            metadata = msgspec.convert(
                metadata_raw or {}, type=YoutubeIngestMetadata)
        except msgspec.ValidationError as exc:
            return self._terminal_failed(
                download_log_id,
                reason="malformed_metadata",
                stderr_excerpt=str(exc),
                observed_track_count=None,
                detail=(
                    f"youtube_metadata blob on download_log_id="
                    f"{download_log_id} failed msgspec.convert: {exc}"
                ),
            )

        request_id_raw = row.get("request_id")
        if not isinstance(request_id_raw, int):
            return self._terminal_failed(
                download_log_id,
                reason="malformed_metadata",
                stderr_excerpt=None,
                observed_track_count=None,
                detail=(
                    f"download_log_id={download_log_id} has no integer "
                    f"request_id; cannot proceed"
                ),
            )
        request_id = int(request_id_raw)

        # Look up the request row for staging-target derivation + R10
        # source-aware count gate. MB requests keep the live MB mirror
        # counter-check; Discogs-only rows use the submission-time expected
        # count that came from stored tracks or the exact resolver distance.
        request_row = self.pdb.get_request(request_id)
        if request_row is None:
            return self._terminal_failed(
                download_log_id,
                reason="missing_request_row",
                stderr_excerpt=None,
                observed_track_count=None,
                detail=(
                    f"download_log_id={download_log_id} points at "
                    f"request_id={request_id} which no longer exists"
                ),
            )
        request_status = str(request_row.get("status") or "")
        if request_status not in YOUTUBE_IMPORT_ALLOWED_REQUEST_STATUSES:
            return self._terminal_failed(
                download_log_id,
                reason="request_no_longer_rescuable",
                stderr_excerpt=None,
                observed_track_count=None,
                detail=(
                    f"request_id={request_id} is now status "
                    f"{request_status!r}; rescue requires one of "
                    f"{sorted(YOUTUBE_IMPORT_ALLOWED_REQUEST_STATUSES)!r}"
                ),
            )

        request_mbid = self._request_mbid(request_row)
        discogs_release_id = self._request_discogs_release_id(request_row)
        if not request_mbid and not discogs_release_id:
            return self._terminal_failed(
                download_log_id,
                reason="missing_request_release_id",
                stderr_excerpt=None,
                observed_track_count=None,
                detail=(
                    f"request {request_id} has neither MB release id nor "
                    f"Discogs release id; R10 gate cannot fire"
                ),
            )

        # ----- yt-dlp invocation -----
        try:
            run = self.ytdlp_runner_fn(
                url=metadata.yt_url,
                output_dir=None,
                expected_track_count=metadata.expected_track_count,
                request_id=request_id,
                browse_id=metadata.browse_id,
            )
        except Exception as exc:  # noqa: BLE001 — worker contract
            return self._terminal_failed(
                download_log_id,
                reason="worker_unhandled_exception",
                stderr_excerpt=_safe_excerpt(str(exc)),
                observed_track_count=None,
                detail=(
                    f"ytdlp_runner_fn raised {type(exc).__name__}: {exc}"
                ),
            )

        if run.exit_code != 0 or not run.staged_files:
            reason = classify_youtube_failure(run.stderr_excerpt)
            cleanup_error = self._cleanup_ytdlp_run(run)
            return self._terminal_failed(
                download_log_id,
                reason=reason,
                stderr_excerpt=run.stderr_excerpt,
                observed_track_count=(
                    len(run.staged_files)
                    if run.staged_files is not None
                    else None
                ),
                detail=(
                    f"yt-dlp exit_code={run.exit_code} "
                    f"staged_files={len(run.staged_files)}"
                ),
                extra_metadata=_cleanup_metadata(cleanup_error),
            )

        # ----- R10: track-count gate BEFORE any staging move -----
        if request_mbid:
            try:
                expected_count = self.mb_track_count_fn(request_mbid)
            except Exception as exc:  # noqa: BLE001 — MB mirror hiccup
                cleanup_error = self._cleanup_ytdlp_run(run)
                return self._terminal_failed(
                    download_log_id,
                    reason="mb_mirror_unavailable",
                    stderr_excerpt=_safe_excerpt(str(exc)),
                    observed_track_count=len(run.staged_files),
                    detail=(
                        f"mb_track_count_fn({request_mbid}) raised {exc}"
                    ),
                    extra_metadata=_cleanup_metadata(cleanup_error),
                )
            if expected_count is None:
                cleanup_error = self._cleanup_ytdlp_run(run)
                return self._terminal_failed(
                    download_log_id,
                    reason="mb_mirror_unavailable",
                    stderr_excerpt=None,
                    observed_track_count=len(run.staged_files),
                    detail=(
                        f"MB mirror returned no track count for "
                        f"{request_mbid!r}"
                    ),
                    extra_metadata=_cleanup_metadata(cleanup_error),
                )
        else:
            expected_count = _positive_int(metadata.expected_track_count)
            if expected_count is None:
                cleanup_error = self._cleanup_ytdlp_run(run)
                return self._terminal_failed(
                    download_log_id,
                    reason="missing_expected_track_count",
                    stderr_excerpt=None,
                    observed_track_count=len(run.staged_files),
                    detail=(
                        f"Discogs request {request_id} has no positive "
                        f"submission-time expected_track_count"
                    ),
                    extra_metadata=_cleanup_metadata(cleanup_error),
                )
        if len(run.staged_files) != int(expected_count):
            cleanup_error = self._cleanup_ytdlp_run(run)
            return self._terminal_failed(
                download_log_id,
                reason="track_count_mismatch",
                stderr_excerpt=None,
                observed_track_count=len(run.staged_files),
                detail=(
                    f"observed_track_count={len(run.staged_files)} != "
                    f"expected_track_count={int(expected_count)}"
                ),
                extra_metadata=_cleanup_metadata(cleanup_error),
            )

        # ----- stage + enqueue -----
        latest_request_row = self.pdb.get_request(request_id)
        if latest_request_row is None:
            cleanup_error = self._cleanup_ytdlp_run(run)
            return self._terminal_failed(
                download_log_id,
                reason="missing_request_row",
                stderr_excerpt=None,
                observed_track_count=len(run.staged_files),
                detail=(
                    f"request_id={request_id} disappeared before staging"
                ),
                extra_metadata=_cleanup_metadata(cleanup_error),
            )
        latest_status = str(latest_request_row.get("status") or "")
        if latest_status not in YOUTUBE_IMPORT_ALLOWED_REQUEST_STATUSES:
            cleanup_error = self._cleanup_ytdlp_run(run)
            return self._terminal_failed(
                download_log_id,
                reason="request_no_longer_rescuable",
                stderr_excerpt=None,
                observed_track_count=len(run.staged_files),
                detail=(
                    f"request_id={request_id} became status "
                    f"{latest_status!r} before staging"
                ),
                extra_metadata=_cleanup_metadata(cleanup_error),
            )

        staging_target = self._derive_staging_target(
            latest_request_row,
            metadata,
            download_log_id=int(download_log_id),
        )
        try:
            src_dir = run.staged_files[0].parent
            self.stage_dir_fn(src_dir, staging_target)
        except Exception as exc:  # noqa: BLE001 — disk error
            cleanup_error = self._cleanup_ytdlp_run(run)
            return self._terminal_failed(
                download_log_id,
                reason="staging_io_error",
                stderr_excerpt=_safe_excerpt(str(exc)),
                observed_track_count=len(run.staged_files),
                detail=(
                    f"stage_dir_fn raised {type(exc).__name__}: {exc}"
                ),
                extra_metadata=_cleanup_metadata(cleanup_error),
            )

        try:
            payload = youtube_import_payload(
                staged_path=str(staging_target),
                request_id=request_id,
                browse_id=metadata.browse_id,
                download_log_id=int(download_log_id),
            )
            terminal_metadata: dict[str, Any] = {
                "observed_track_count": len(run.staged_files),
            }
            cleanup_error = self._cleanup_ytdlp_run(run)
            if cleanup_error is not None:
                terminal_metadata["cleanup_error"] = cleanup_error
            self.pdb.enqueue_youtube_import_and_mark_success(
                download_log_id=int(download_log_id),
                request_id=request_id,
                dedupe_key=youtube_import_dedupe_key(int(download_log_id)),
                payload=payload,
                message=(
                    f"youtube rescue staged for request {request_id} via "
                    f"download_log {download_log_id}"
                ),
                terminal_metadata=terminal_metadata,
            )
        except Exception as exc:  # noqa: BLE001 — DB hiccup
            cleanup_error = self._cleanup_paths(
                [staging_target, *self._ytdlp_cleanup_paths(run)])
            return self._terminal_failed(
                download_log_id,
                reason="import_enqueue_failed",
                stderr_excerpt=_safe_excerpt(str(exc)),
                observed_track_count=len(run.staged_files),
                detail=(
                    f"enqueue_import_job raised {type(exc).__name__}: {exc}"
                ),
                extra_metadata=_cleanup_metadata(cleanup_error),
            )

        return RunResult(outcome="youtube_success", reason=None)

    # ----- helpers -------------------------------------------------------

    def _terminal_failed(
        self,
        download_log_id: int,
        *,
        reason: str,
        stderr_excerpt: Optional[str],
        observed_track_count: Optional[int],
        detail: Optional[str] = None,
        extra_metadata: Optional[dict[str, Any]] = None,
    ) -> RunResult:
        """Write the terminal ``youtube_failed`` row and return RunResult.

        Centralises the terminal-write so every failure path persists
        the same shape of audit metadata (reason, stderr_excerpt,
        observed_track_count). ``detail`` is logged but not persisted —
        the JSONB schema is fixed by the YoutubeIngestMetadata Struct.
        """
        if detail is not None:
            log.warning(
                "youtube_ingest_service: run_job(%s) -> failed (%s): %s",
                download_log_id, reason, detail)
        terminal_metadata: dict[str, Any] = {"reason": reason}
        if stderr_excerpt is not None:
            terminal_metadata["stderr_excerpt"] = stderr_excerpt
        if observed_track_count is not None:
            terminal_metadata["observed_track_count"] = int(
                observed_track_count)
        if extra_metadata:
            terminal_metadata.update(extra_metadata)
        try:
            self.pdb.update_youtube_terminal(
                int(download_log_id),
                "youtube_failed",
                terminal_metadata,
            )
        except Exception as exc:  # noqa: BLE001 — terminal write
            log.error(
                "youtube_ingest_service: update_youtube_terminal failed "
                "for download_log_id=%s: %s", download_log_id, exc)
            raise
        return RunResult(outcome="youtube_failed", reason=reason)

    def _lookup_submit_mapping(
        self,
        *,
        request_id: int,
        request_row: dict[str, Any],
        browse_id: str,
    ) -> SubmitResult | tuple[str, dict[str, Any], str]:
        request_mbid = self._request_mbid(request_row)
        discogs_id = self._request_discogs_release_id(request_row)
        if request_mbid is None and discogs_id is not None:
            try:
                row = self.pdb.find_youtube_album_mapping_for_release(
                    source="discogs",
                    release_id=discogs_id,
                    browse_id=browse_id,
                )
            except Exception as exc:  # noqa: BLE001 — transient DB error
                log.warning(
                    "youtube_ingest_service: "
                    "find_youtube_album_mapping_for_release(%s, %s) "
                    "raised %s", discogs_id, browse_id, exc)
                return SubmitResult(
                    outcome="transient",
                    detail=(
                        f"DB error reading Discogs resolver mapping for "
                        f"release_id={discogs_id!r}: {exc}"
                    ),
                )
            if row is None:
                return SubmitResult(
                    outcome="no_resolver_mapping",
                    detail=(
                        f"no Discogs resolver mapping for release_id="
                        f"{discogs_id!r} browse_id={browse_id!r}; run the "
                        f"YouTube album resolver first"
                    ),
                )
            return ("discogs", row, discogs_id)

        resolver_key = self.release_group_resolver_fn(request_row)
        if resolver_key is None:
            return SubmitResult(
                outcome="no_resolver_mapping",
                detail=(
                    f"request {request_id} has no resolvable release-group "
                    f"identifier; populate mb_release_group_id and retry"
                ),
            )
        source, rg_id = resolver_key
        try:
            mapping_rows = self.pdb.get_youtube_album_mapping(rg_id, source)
        except Exception as exc:  # noqa: BLE001 — transient DB error
            log.warning(
                "youtube_ingest_service: get_youtube_album_mapping(%s, %s) "
                "raised %s", rg_id, source, exc)
            return SubmitResult(
                outcome="transient",
                detail=(
                    f"DB error reading resolver mapping for "
                    f"({rg_id!r}, {source!r}): {exc}"
                ),
            )
        if mapping_rows is None or not mapping_rows:
            return SubmitResult(
                outcome="no_resolver_mapping",
                detail=(
                    f"no resolver mapping for release_group={rg_id!r} "
                    f"source={source!r}; run the YouTube album resolver first"
                ),
            )
        for row in mapping_rows:
            if str(row.get("yt_browse_id") or "") == browse_id:
                target_release_id = request_mbid or ""
                return (source, row, target_release_id)
        return SubmitResult(
            outcome="no_resolver_mapping",
            detail=(
                f"browse_id {browse_id!r} not in resolver mapping for "
                f"release_group={rg_id!r} source={source!r}"
            ),
        )

    def _expected_track_count_for_submit(
        self,
        *,
        request_id: int,
        request_row: dict[str, Any],
        source: str,
        mapping_row: dict[str, Any],
        target_release_id: str,
    ) -> int:
        if source == "discogs":
            distance_count = self._distance_total_tracks(
                mapping_row, target_release_id)
            try:
                stored_count = self._stored_track_count(request_id)
            except Exception as exc:  # noqa: BLE001 — DB/read hiccup
                raise _TransientPrecheckFailure(
                    f"DB error reading stored tracklist for request "
                    f"{request_id}: {exc}"
                ) from exc
            if stored_count is None and distance_count is None:
                raise _TrackCountPrecheckFailure(
                    f"Discogs request {request_id} has no stored tracklist "
                    f"and resolver mapping browse_id="
                    f"{mapping_row.get('yt_browse_id')!r} has no exact "
                    f"total track count for release_id={target_release_id!r}"
                )
            if (
                stored_count is not None
                and distance_count is not None
                and int(stored_count) != int(distance_count)
            ):
                raise _TrackCountPrecheckFailure(
                    f"stored Discogs track count={int(stored_count)} != "
                    f"resolver total tracks={int(distance_count)} for "
                    f"release_id={target_release_id!r}; refresh resolver or "
                    f"repair album_tracks before retrying"
                )
            expected_count = (
                stored_count if stored_count is not None else distance_count
            )
            assert expected_count is not None
            return int(expected_count)

        request_mbid = self._request_mbid(request_row)
        if not request_mbid:
            raise _TrackCountPrecheckFailure(
                f"request {request_id} has no MB release id; "
                f"track-count precheck cannot run"
            )
        expected_track_count = self._distance_total_tracks(
            mapping_row, request_mbid)
        if expected_track_count is None:
            raise _TrackCountPrecheckFailure(
                f"resolver mapping for browse_id="
                f"{mapping_row.get('yt_browse_id')!r} has no distance entry "
                f"for mbid={request_mbid!r}; refresh the resolver and retry"
            )
        try:
            current_mb_count = self.mb_track_count_fn(request_mbid)
        except Exception as exc:  # noqa: BLE001 — MB mirror hiccup
            log.warning(
                "youtube_ingest_service: mb_track_count_fn(%s) raised %s",
                request_mbid, exc)
            raise _TransientPrecheckFailure(
                f"MB mirror error checking track count for "
                f"{request_mbid!r}: {exc}"
            ) from exc
        if current_mb_count is None:
            raise _TrackCountPrecheckFailure(
                f"MB mirror returned no track count for "
                f"{request_mbid!r}; cannot R7-precheck"
            )
        if int(current_mb_count) != int(expected_track_count):
            raise _TrackCountPrecheckFailure(
                f"resolver cache total_mb_tracks="
                f"{int(expected_track_count)} != "
                f"current MB tracks={int(current_mb_count)} for "
                f"mbid={request_mbid!r}; resolver state is stale, "
                f"refresh first"
            )
        return int(current_mb_count)

    def _stored_track_count(self, request_id: int) -> Optional[int]:
        tracks = self.pdb.get_tracks(int(request_id))
        if not tracks:
            return None
        return len(tracks)

    @staticmethod
    def _request_mbid(request_row: dict[str, Any]) -> Optional[str]:
        """Return the request's MB release id, or ``None`` for Discogs-only.

        ``mb_release_id`` may contain a numeric Discogs id for legacy
        pipeline compatibility. Only UUID-shaped MusicBrainz ids anchor
        the MB precheck / gate.
        """
        value = normalize_release_id(request_row.get("mb_release_id"))
        if detect_release_source(value) == "musicbrainz":
            return value
        return None

    @staticmethod
    def _request_discogs_release_id(
        request_row: dict[str, Any],
    ) -> Optional[str]:
        for key in ("discogs_release_id", "mb_release_id"):
            value = normalize_release_id(request_row.get(key))
            if detect_release_source(value) == "discogs":
                return value
        return None

    @staticmethod
    def _distance_entry(
        mapping_row: dict[str, Any],
        target_release_id: str,
    ) -> Optional[dict[str, Any]]:
        """Look up the resolver distance entry for one release id.

        Walks the row's ``distances`` array, returning the first entry
        whose historical ``mbid`` key matches ``target_release_id``. The
        key name is retained by the resolver for both MB and Discogs rows.
        """
        distances = mapping_row.get("distances") or []
        for entry in distances:
            if not isinstance(entry, dict):
                continue
            if str(entry.get("mbid") or "") == target_release_id:
                return entry
        return None

    @classmethod
    def _distance_total_tracks(
        cls,
        mapping_row: dict[str, Any],
        target_release_id: str,
    ) -> Optional[int]:
        """Look up the resolver's cached total tracks for one release id."""
        entry = cls._distance_entry(mapping_row, target_release_id)
        if entry is None:
            return None
        tmb = entry.get("total_mb_tracks")
        if isinstance(tmb, int):
            return tmb
        try:
            return int(tmb) if tmb is not None else None
        except (TypeError, ValueError):
            return None

    def _cleanup_ytdlp_run(self, run: YtdlpRunResult) -> Optional[str]:
        """Best-effort delete the scratch paths used by one yt-dlp run."""
        return self._cleanup_paths(self._ytdlp_cleanup_paths(run))

    @staticmethod
    def _ytdlp_cleanup_paths(run: YtdlpRunResult) -> list[Path]:
        """Return scratch roots to delete for a yt-dlp result.

        The worker now reports ``work_dir`` for real invocations. Tests
        and older fakes may omit it, so fall back to each staged file's
        parent directory.
        """
        if run.work_dir is not None:
            return [Path(run.work_dir)]
        paths: list[Path] = []
        seen: set[str] = set()
        for file_path in run.staged_files or []:
            parent = Path(file_path).parent
            key = str(parent)
            if key not in seen:
                seen.add(key)
                paths.append(parent)
        return paths

    @staticmethod
    def _cleanup_paths(paths: list[Path]) -> Optional[str]:
        errors: list[str] = []
        seen: set[str] = set()
        for raw_path in paths:
            path = Path(raw_path)
            key = str(path)
            if key in seen:
                continue
            seen.add(key)
            try:
                if not path.exists():
                    continue
                if path.is_dir():
                    shutil.rmtree(path)
                else:
                    path.unlink()
            except Exception as exc:  # noqa: BLE001 — cleanup is best-effort
                errors.append(f"{path}: {type(exc).__name__}: {exc}")
        return "; ".join(errors) if errors else None

    def _derive_staging_target(
        self,
        request_row: dict[str, Any],
        metadata: YoutubeIngestMetadata,
        *,
        download_log_id: int,
    ) -> Path:
        """Derive a request-scoped YouTube staging target from the row.

        The request id and download_log id are both included so two
        rescues for the same YT album never share a staged source path.
        """
        artist = _slug(request_row.get("artist_name"))
        album = _slug(request_row.get("album_title"))
        suffix = _slug(metadata.browse_id)
        request_id = int(request_row.get("id") or 0)
        return (
            self.staging_root
            / f"{artist}-{album}-{suffix}-request-{request_id}-log-{download_log_id}"
        )


# ---------------------------------------------------------------------------
# Pure helpers (used by service + tests).
# ---------------------------------------------------------------------------


def _slug(value: Any) -> str:
    """Filesystem-friendly slug for one path component."""
    text = str(value or "").strip()
    if not text:
        return "unknown"
    cleaned: list[str] = []
    for ch in text:
        if ch.isalnum() or ch in ("-", "_"):
            cleaned.append(ch)
        elif ch.isspace() or ch in (".", "(", ")", ",", "&", "'"):
            cleaned.append("_")
    out = "".join(cleaned).strip("_-") or "unknown"
    return out[:80]


def _safe_excerpt(text: Optional[str], limit: int = 4096) -> str:
    """Cap a stderr / exception string before persistence.

    Bounded so a runaway error message cannot bloat the JSONB column.
    The U6 worker also caps stderr after the ``errors='replace'`` decode
    — this helper is the second line of defence inside the service.
    """
    if text is None:
        return ""
    if len(text) <= limit:
        return text
    return text[:limit] + "…[truncated]"


def _positive_int(value: Any) -> Optional[int]:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed > 0 else None


def _mapping_row_id(mapping_row: dict[str, Any]) -> Optional[int]:
    raw = mapping_row.get("id")
    if isinstance(raw, int):
        return raw
    try:
        return int(raw) if raw is not None else None
    except (TypeError, ValueError):
        return None


def _per_track_video_ids(mapping_row: dict[str, Any]) -> Optional[list[str]]:
    ids: list[str] = []
    for track in mapping_row.get("yt_tracks") or []:
        if not isinstance(track, dict):
            continue
        raw = track.get("video_id")
        if raw is None:
            raw = track.get("videoId")
        if isinstance(raw, str) and raw.strip():
            ids.append(raw.strip())
    return ids or None


def _download_log_id_from_import_job(job: Any) -> Optional[int]:
    payload = getattr(job, "payload", None)
    if isinstance(payload, dict):
        raw = payload.get("download_log_id")
        if isinstance(raw, int):
            return raw
        if raw is not None:
            try:
                return int(raw)
            except (TypeError, ValueError):
                pass
    dedupe_key = getattr(job, "dedupe_key", None)
    if not isinstance(dedupe_key, str):
        return None
    prefix = f"{IMPORT_JOB_YOUTUBE}:download_log:"
    if not dedupe_key.startswith(prefix):
        return None
    try:
        return int(dedupe_key[len(prefix):])
    except ValueError:
        return None


def _cleanup_metadata(cleanup_error: Optional[str]) -> Optional[dict[str, Any]]:
    if not cleanup_error:
        return None
    return {"cleanup_error": cleanup_error}
