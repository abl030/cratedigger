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
from lib.pipeline_db import YoutubeInFlightError
from lib.release_identity import detect_release_source

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


# Request statuses that may be advanced into a YT rescue submission. Per
# R3: ``wanted`` and ``manual`` only. ``downloading`` would race the slskd
# pipeline; ``imported`` / ``replaced`` are terminal.
_VALID_SUBMIT_STATUSES: frozenset[str] = frozenset({"wanted", "manual"})


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


class YoutubeImportPayload(msgspec.Struct, kw_only=True):
    """Typed view of ``import_jobs.payload`` for ``youtube_import`` rows.

    The dispatcher (U9) reads this off ``ImportJob.payload``. The fields
    mirror the keys produced by ``lib.import_queue.youtube_import_payload``.
    """

    staged_path: str
    request_id: int
    browse_id: str


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
    detail: Optional[str] = None


class RunResult(msgspec.Struct, kw_only=True):
    """Outcome of one ``YoutubeIngestService.run_job`` call.

    ``reason`` carries the classified failure reason on ``youtube_failed``
    (one of the R20 taxonomy values, or ``track_count_mismatch``,
    ``worker_unhandled_exception``, ``worker_died``); ``None`` on success.
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
key for ``pdb.get_youtube_album_mapping(...)``. For MB requests this
typically returns ``("mb", request_row["mb_release_group_id"])``; for
Discogs it'd resolve the master id via a Discogs mirror lookup.

Default implementation (:func:`_default_release_group_resolver`) reads
``mb_release_group_id`` directly off the request row when present. When
that field is NULL or the request is Discogs-only, the default returns
``None`` and the service surfaces ``no_resolver_mapping`` — the
operator-side fix is to populate ``mb_release_group_id`` via the
existing resolver paths, or to pass a richer resolver_fn into the
service at construction time."""


ClockFn = Callable[[], datetime]
"""``clock_fn() -> datetime``. Wall-clock seam for tests."""


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

    def insert_youtube_running(
        self,
        *,
        request_id: int,
        browse_id: str,
        audio_playlist_id: Optional[str],
        yt_url: str,
        expected_track_count: int,
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
    ``scripts/pipeline_cli.py``, HTTP route in ``web/routes/youtube.py``)
    go through :func:`default_youtube_ingest_service_factory` which wires
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
    ``scripts/pipeline_cli.py::cmd_youtube_rescue`` and HTTP route in
    ``web/routes/youtube.py::post_pipeline_youtube_rescue``) per CLI ⇄
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


def default_youtube_ingest_service_factory(pdb: Any) -> "YoutubeIngestService":
    """Construct a production ``YoutubeIngestService`` for CLI / API.

    Wires the live MB-mirror ``mb_track_count_fn`` (other ports retain
    their library-side production defaults). Both
    ``scripts/pipeline_cli.py::cmd_youtube_rescue`` and
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
    output to ``/Incoming/auto-import/<artist>-<album>/``. ``shutil.move``
    handles cross-filesystem boundaries (copy-then-delete fallback). The
    parent directory is created if missing.
    """
    dest.parent.mkdir(parents=True, exist_ok=True)
    shutil.move(str(src), str(dest))


def _default_release_group_resolver(
    request_row: dict[str, Any],
) -> Optional[tuple[str, str]]:
    """Default release-group resolver: trust ``mb_release_group_id`` only.

    Returns ``("mb", rg_id)`` when the request row carries a populated
    ``mb_release_group_id``. Returns ``None`` for Discogs-only requests
    (the production caller threads a Discogs-aware resolver via the
    kwarg-DI port).
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
        if status not in _VALID_SUBMIT_STATUSES:
            return SubmitResult(
                outcome="wrong_state",
                detail=(
                    f"request {request_id} is in status {status!r}; "
                    f"submit requires one of "
                    f"{sorted(_VALID_SUBMIT_STATUSES)!r}"
                ),
            )

        # 3. Resolver-mapping lookup (R6).
        resolver_key = self.release_group_resolver_fn(request_row)
        if resolver_key is None:
            return SubmitResult(
                outcome="no_resolver_mapping",
                detail=(
                    f"request {request_id} has no resolvable release-group "
                    f"identifier; populate mb_release_group_id (or thread "
                    f"a Discogs-aware release_group_resolver_fn) and retry"
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
        match: Optional[dict[str, Any]] = None
        for row in mapping_rows:
            if str(row.get("yt_browse_id") or "") == browse_id:
                match = row
                break
        if match is None:
            return SubmitResult(
                outcome="no_resolver_mapping",
                detail=(
                    f"browse_id {browse_id!r} not in resolver mapping for "
                    f"release_group={rg_id!r} source={source!r}"
                ),
            )

        # 4. Track-count precheck (R7).
        request_mbid = self._request_mbid(request_row)
        if not request_mbid:
            # Discogs-only requests don't have an MBID to precheck
            # against — the precheck is MB-anchored. Surface as
            # no_resolver_mapping so the operator routes the request
            # through an MB-aware path. This is the same outcome the
            # R10 worker gate hits when no MB count is available.
            return SubmitResult(
                outcome="no_resolver_mapping",
                detail=(
                    f"request {request_id} has no MB release id; "
                    f"track-count precheck cannot run"
                ),
            )
        expected_track_count = self._distance_total_mb_tracks(
            match, request_mbid)
        if expected_track_count is None:
            return SubmitResult(
                outcome="track_count_precheck_failed",
                detail=(
                    f"resolver mapping for browse_id={browse_id!r} has no "
                    f"distance entry for mbid={request_mbid!r}; refresh "
                    f"the resolver and retry"
                ),
            )
        try:
            current_mb_count = self.mb_track_count_fn(request_mbid)
        except Exception as exc:  # noqa: BLE001 — MB mirror hiccup
            log.warning(
                "youtube_ingest_service: mb_track_count_fn(%s) raised %s",
                request_mbid, exc)
            return SubmitResult(
                outcome="transient",
                detail=(
                    f"MB mirror error checking track count for "
                    f"{request_mbid!r}: {exc}"
                ),
            )
        if current_mb_count is None:
            return SubmitResult(
                outcome="track_count_precheck_failed",
                detail=(
                    f"MB mirror returned no track count for "
                    f"{request_mbid!r}; cannot R7-precheck"
                ),
            )
        if int(current_mb_count) != int(expected_track_count):
            return SubmitResult(
                outcome="track_count_precheck_failed",
                detail=(
                    f"resolver cache total_mb_tracks="
                    f"{int(expected_track_count)} != "
                    f"current MB tracks={int(current_mb_count)} for "
                    f"mbid={request_mbid!r}; resolver state is stale, "
                    f"refresh first"
                ),
            )

        # 5. Idempotent insert + happy-path return.
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
                expected_track_count=int(current_mb_count),
            )
        except YoutubeInFlightError as exc:
            existing_id = exc.existing_download_log_id
            return SubmitResult(
                outcome="in_flight",
                download_log_id=existing_id,
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
        # MB-side counter-check. (R10 uses ``mb_track_count_fn``, not
        # the resolver's cached count — see KTD5.)
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

        request_mbid = self._request_mbid(request_row)
        if not request_mbid:
            return self._terminal_failed(
                download_log_id,
                reason="missing_request_mbid",
                stderr_excerpt=None,
                observed_track_count=None,
                detail=(
                    f"request {request_id} has no MB release id; R10 "
                    f"gate cannot fire"
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
            )

        # ----- R10: track-count gate BEFORE any staging move -----
        try:
            expected_count = self.mb_track_count_fn(request_mbid)
        except Exception as exc:  # noqa: BLE001 — MB mirror hiccup
            return self._terminal_failed(
                download_log_id,
                reason="mb_mirror_unavailable",
                stderr_excerpt=_safe_excerpt(str(exc)),
                observed_track_count=len(run.staged_files),
                detail=(
                    f"mb_track_count_fn({request_mbid}) raised {exc}"
                ),
            )
        if expected_count is None:
            return self._terminal_failed(
                download_log_id,
                reason="mb_mirror_unavailable",
                stderr_excerpt=None,
                observed_track_count=len(run.staged_files),
                detail=(
                    f"MB mirror returned no track count for {request_mbid!r}"
                ),
            )
        if len(run.staged_files) != int(expected_count):
            return self._terminal_failed(
                download_log_id,
                reason="track_count_mismatch",
                stderr_excerpt=None,
                observed_track_count=len(run.staged_files),
                detail=(
                    f"observed_track_count={len(run.staged_files)} != "
                    f"expected_track_count={int(expected_count)}"
                ),
            )

        # ----- stage + enqueue -----
        staging_target = self._derive_staging_target(request_row, metadata)
        try:
            src_dir = run.staged_files[0].parent
            self.stage_dir_fn(src_dir, staging_target)
        except Exception as exc:  # noqa: BLE001 — disk error
            return self._terminal_failed(
                download_log_id,
                reason="staging_io_error",
                stderr_excerpt=_safe_excerpt(str(exc)),
                observed_track_count=len(run.staged_files),
                detail=(
                    f"stage_dir_fn raised {type(exc).__name__}: {exc}"
                ),
            )

        try:
            payload = youtube_import_payload(
                staged_path=str(staging_target),
                request_id=request_id,
                browse_id=metadata.browse_id,
            )
            self.pdb.enqueue_import_job(
                IMPORT_JOB_YOUTUBE,
                request_id=request_id,
                dedupe_key=youtube_import_dedupe_key(int(download_log_id)),
                payload=payload,
                message=(
                    f"youtube rescue staged for request {request_id} via "
                    f"download_log {download_log_id}"
                ),
            )
        except Exception as exc:  # noqa: BLE001 — DB hiccup
            return self._terminal_failed(
                download_log_id,
                reason="import_enqueue_failed",
                stderr_excerpt=_safe_excerpt(str(exc)),
                observed_track_count=len(run.staged_files),
                detail=(
                    f"enqueue_import_job raised {type(exc).__name__}: {exc}"
                ),
            )

        terminal_metadata: dict[str, Any] = {
            "observed_track_count": len(run.staged_files),
        }
        self.pdb.update_youtube_terminal(
            int(download_log_id),
            "youtube_success",
            terminal_metadata,
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
        return RunResult(outcome="youtube_failed", reason=reason)

    @staticmethod
    def _request_mbid(request_row: dict[str, Any]) -> Optional[str]:
        """Return the request's MB release id, or ``None`` for Discogs-only.

        ``mb_release_id`` is the authoritative field; if NULL, the
        request was added via the Discogs path and has no MBID to anchor
        the precheck / gate.
        """
        raw = request_row.get("mb_release_id")
        if isinstance(raw, str) and raw.strip():
            return raw.strip()
        return None

    @staticmethod
    def _distance_total_mb_tracks(
        mapping_row: dict[str, Any],
        target_mbid: str,
    ) -> Optional[int]:
        """Look up the resolver's cached ``total_mb_tracks`` for one MBID.

        Walks the row's ``distances`` array, returning the first entry
        whose ``mbid`` matches ``target_mbid``. ``None`` if no entry
        matches — the caller surfaces ``track_count_precheck_failed``.
        """
        distances = mapping_row.get("distances") or []
        for entry in distances:
            if not isinstance(entry, dict):
                continue
            if str(entry.get("mbid") or "") != target_mbid:
                continue
            tmb = entry.get("total_mb_tracks")
            if isinstance(tmb, int):
                return tmb
            try:
                return int(tmb) if tmb is not None else None
            except (TypeError, ValueError):
                return None
        return None

    def _derive_staging_target(
        self,
        request_row: dict[str, Any],
        metadata: YoutubeIngestMetadata,
    ) -> Path:
        """Derive ``/Incoming/auto-import/<artist>-<album>/`` from the row.

        The naming is deterministic so a re-run of the same submission
        targets the same directory (and any orphaned partial download is
        replaced by the new content — see ``stage_dir_fn`` which uses
        ``shutil.move``). The ``browse_id`` suffix disambiguates two
        distinct YT pressings being staged for the same artist+album.
        """
        artist = _slug(request_row.get("artist_name"))
        album = _slug(request_row.get("album_title"))
        suffix = _slug(metadata.browse_id)
        return self.staging_root / f"{artist}-{album}-{suffix}"


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


# Source-detection convenience for callers that need to know which mirror
# to query when building a custom ``release_group_resolver_fn``. Kept on
# the module surface so wrappers don't reach into ``lib.release_identity``.
def detect_request_source(request_row: dict[str, Any]) -> Optional[str]:
    """Return ``'mb'`` / ``'discogs'`` / ``None`` for one request row.

    Mirrors the discriminator string used by ``youtube_album_mappings``
    (``'mb'`` / ``'discogs'``). ``None`` when the row has neither a
    valid MB release id nor a Discogs release id.
    """
    mb = request_row.get("mb_release_id")
    if isinstance(mb, str) and detect_release_source(mb) == "musicbrainz":
        return "mb"
    discogs = request_row.get("discogs_release_id")
    if isinstance(discogs, (str, int)):
        if detect_release_source(discogs) == "discogs":
            return "discogs"
    return None
