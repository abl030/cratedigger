#!/usr/bin/env python3
"""Long-running drainer for YouTube-rescue ingest jobs.

Mirrors ``scripts/importer.py`` structurally: acquires a process-wide
advisory lock, sweeps claimed ``youtube_running`` rows left by a previous
worker (R22), then loops claiming the next unclaimed row with
``PipelineDB.claim_next_youtube_pending`` and dispatching it to
``YoutubeIngestService.run_job``.

Per CLAUDE.md (single-operator, no backwards-compat) the worker is the
production owner of the yt-dlp subprocess invocation — argv shape,
``text=True, errors='replace'`` discipline (KTD8), timeout, temp-dir
lifecycle, stderr cap (4 KiB). The service layer accepts a kwarg-DI
``ytdlp_runner_fn`` so tests inject a fake; this module provides the
production implementation via :func:`_run_ytdlp`.

The worker does NOT claim importer jobs. After staging the audio into the
configured ``auto-import/<artist>-<album>/`` staging child, the service enqueues a
``youtube_import`` row in ``import_jobs``; the existing
``cratedigger-importer`` worker (its own systemd unit) drains it via
``execute_youtube_import_job`` (wired in U9).

Exit codes:
  * 0 — clean shutdown (SIGTERM, ``--once`` completed, or advisory-lock
        contention with another worker holding it).
  * 1 — startup failure (DB unreachable, etc.).
"""

from __future__ import annotations

import argparse
import logging
import os
import selectors
import shutil
import socket
import subprocess
import sys
import tempfile
import time
import traceback
from pathlib import Path
from typing import Any, Callable, Iterable

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from lib.pipeline_db import (  # noqa: E402
    ADVISORY_LOCK_NAMESPACE_YOUTUBE_INGEST,
    DEFAULT_DSN,
    PipelineDB,
)
from lib.processing_paths import stage_to_ai_root  # noqa: E402
from lib.youtube_ingest_service import (  # noqa: E402
    YoutubeIngestService,
    YtdlpRunResult,
    _slug,
    default_mb_track_count_from_mirror,
)

logger = logging.getLogger("cratedigger-youtube-ingest")

# Bounded stderr capture — KTD8 size-bound. The JSONB column should not
# carry runaway error messages regardless of how verbose yt-dlp gets on
# stuck-pattern failures (429 storms, region-locked playlists, etc.).
STDERR_EXCERPT_BYTES_LIMIT = 4096

# Per-job yt-dlp timeout. Empirically a 10-track YT Music album fetches
# in 30-90s; 600s leaves plenty of slack for large playlists and slow
# links. Cratedigger does not run multi-hour ingest jobs.
DEFAULT_YTDLP_TIMEOUT_SEC = 600

# Default per-process scratch directory. The operator overrides via
# ``--temp-dir`` (U7 will wire ``${cfg.stateDir}/youtube-ingest-temp``).
DEFAULT_TEMP_DIR = Path("/var/lib/cratedigger/youtube-ingest-temp")

# Audio extensions yt-dlp's ``bestaudio`` heuristic may produce. YouTube
# Music typically returns Opus; the broader list accommodates future
# format rotations and any operator-side ``-x --audio-format`` extension.
_AUDIO_EXTENSIONS: frozenset[str] = frozenset({
    ".opus", ".m4a", ".webm", ".mp4", ".mp3", ".flac", ".ogg", ".aac",
})


# ---------------------------------------------------------------------------
# yt-dlp subprocess invocation (production ytdlp_runner_fn).
# ---------------------------------------------------------------------------


def _cap_stderr_excerpt(text: str | None) -> str | None:
    """Return ``text`` truncated to the last 4 KiB.

    The tail is more useful than the head: yt-dlp typically prints
    setup/probe lines first and the actual error code last. Returns
    ``None`` for ``None`` / empty input so the service's
    ``classify_youtube_failure(None)`` short-circuit still works.

    KTD8 size-bound: this function is called AFTER the
    ``errors='replace'`` decode inside :func:`_run_ytdlp`, so the input
    is guaranteed-decoded text. Truncation is by character count, not
    byte count — Python's ``len`` on a str counts characters, which is
    what we want for JSONB.
    """
    if text is None or text == "":
        return None
    if len(text) <= STDERR_EXCERPT_BYTES_LIMIT:
        return text
    return text[-STDERR_EXCERPT_BYTES_LIMIT:]


def _append_stderr_tail(tail: bytearray, chunk: bytes) -> None:
    """Append ``chunk`` while retaining only a bounded stderr byte tail."""
    if not chunk:
        return
    tail.extend(chunk)
    max_bytes = STDERR_EXCERPT_BYTES_LIMIT * 4
    if len(tail) > max_bytes:
        del tail[:-max_bytes]


def _run_ytdlp_streaming(
    argv: list[str],
    *,
    timeout_sec: int,
) -> tuple[int, str | None]:
    """Run yt-dlp without ever buffering unbounded stdout/stderr in memory."""
    proc = subprocess.Popen(
        argv,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
    )
    stderr_tail = bytearray()
    stderr = proc.stderr
    if stderr is None:
        returncode = proc.wait(timeout=timeout_sec)
        return returncode, None

    selector = selectors.DefaultSelector()
    selector.register(stderr.fileno(), selectors.EVENT_READ)
    deadline = time.monotonic() + timeout_sec
    timed_out = False
    try:
        while selector.get_map():
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                timed_out = True
                proc.kill()
                break
            events = selector.select(min(remaining, 0.25))
            if not events and proc.poll() is not None:
                break
            for key, _events in events:
                chunk = os.read(key.fd, 8192)
                if chunk:
                    _append_stderr_tail(stderr_tail, chunk)
                else:
                    selector.unregister(key.fileobj)
        returncode = proc.wait()
    finally:
        selector.close()
        if not stderr.closed:
            stderr.close()

    decoded_tail = stderr_tail.decode("utf-8", errors="replace")
    if timed_out:
        return -1, _cap_stderr_excerpt(
            f"ERROR: yt-dlp Read timed out after {timeout_sec}s\n"
            f"{decoded_tail}"
        )
    return returncode, _cap_stderr_excerpt(decoded_tail)


def _collect_audio_files(directory: Path) -> list[Path]:
    """Walk ``directory`` and return every file with an audio extension.

    Recursive so yt-dlp's playlist-folder output (``%(playlist)s/...``)
    is captured. Files are returned sorted by path so test assertions
    are deterministic.
    """
    out: list[Path] = []
    if not directory.is_dir():
        return out
    for root, _dirs, files in os.walk(directory):
        for name in files:
            path = Path(root) / name
            if path.suffix.lower() in _AUDIO_EXTENSIONS:
                out.append(path)
    out.sort()
    return out


def _resolve_ytdlp_binary() -> str:
    """Locate the ``yt-dlp`` binary, preferring ``$PATH``.

    Returns the path or raises ``RuntimeError`` if it isn't on PATH.
    U7 will package ``pkgs.yt-dlp`` into the worker's systemd ``PATH``;
    until then this raises so tests / dry runs fail loudly rather than
    silently calling a missing binary.
    """
    found = shutil.which("yt-dlp")
    if found is None:
        raise RuntimeError(
            "yt-dlp not found on PATH — U7 packages it into the worker's "
            "systemd unit; for local runs, install yt-dlp into the dev "
            "shell or pass it via PATH explicitly")
    return found


def _build_ytdlp_argv(
    *,
    ytdlp_bin: str,
    url: str,
    output_template: str,
) -> list[str]:
    """Construct the yt-dlp argv list.

    Defense-in-depth: the ``--`` separator before the URL positional is
    REQUIRED. A future resolver-row drift producing a ``yt_url`` that
    starts with ``-`` would otherwise be parsed as a flag. The U6 test
    suite pins this with a regression test
    (``test_argv_separator_before_url``).

    The ``--no-ignore-errors`` flag means partial failures are not
    silently absorbed — any track error fails the whole invocation,
    which the service then routes to a ``youtube_failed`` outcome.
    The track-count gate (R10) catches partial successes as well, but
    failing loud at the subprocess boundary is the cleaner signal.
    """
    return [
        ytdlp_bin,
        "--ignore-config",
        "--no-ignore-errors",
        "-f", "bestaudio",
        "--output", output_template,
        "--",
        url,
    ]


def _run_ytdlp(
    *,
    url: str,
    output_dir: Path | None = None,
    expected_track_count: int | None = None,
    timeout_sec: int = DEFAULT_YTDLP_TIMEOUT_SEC,
    temp_root: Path = DEFAULT_TEMP_DIR,
    request_id: int | None = None,
    browse_id: str | None = None,
    subprocess_run: Callable[..., subprocess.CompletedProcess[str]] | None = None,
    ytdlp_bin_resolver: Callable[[], str] = _resolve_ytdlp_binary,
) -> YtdlpRunResult:
    """Invoke yt-dlp on ``url`` and return the typed result.

    KTD8: ``text=True`` paired with ``errors='replace'`` is non-negotiable.
    Without ``errors='replace'`` the decode happens inside
    ``Popen._communicate`` and a ``try/except subprocess.TimeoutExpired``
    around the call does NOT catch ``UnicodeDecodeError`` — the docs/solutions
    doc has the full failure-mode write-up.

    ``output_dir`` is optional — when ``None`` the runner creates its own
    temp directory under ``temp_root`` (which defaults to
    ``DEFAULT_TEMP_DIR`` so production callers get the right host path).
    The directory is NOT cleaned up here; ``YoutubeIngestService.run_job``
    stages from it and then best-effort deletes the scratch path on both
    success and failure.

    The ``subprocess_run`` and ``ytdlp_bin_resolver`` kwargs are kwarg-DI
    seams that let tests inject a fake binary path + fake ``subprocess.run``
    without monkey-patching the module. Production uses
    ``_run_ytdlp_streaming`` so noisy yt-dlp output cannot accumulate
    unbounded stdout/stderr in memory before truncation.
    """
    if expected_track_count is None or expected_track_count <= 0:
        # Defensive: the service always passes a positive int from the
        # submission-time metadata. A None here would be a contract
        # violation, not a yt-dlp failure.
        raise ValueError(
            f"_run_ytdlp: expected_track_count must be a positive int; "
            f"got {expected_track_count!r}")

    ytdlp_bin = ytdlp_bin_resolver()
    work_dir = output_dir
    if work_dir is None:
        temp_root.mkdir(parents=True, exist_ok=True)
        prefix = f"ytdlp-req{request_id or 'x'}-{browse_id or 'x'}-"
        # Use mkdtemp so each job has its own isolated scratch path; the
        # service stages from this directory and then deletes it.
        work_dir = Path(tempfile.mkdtemp(prefix=prefix, dir=str(temp_root)))

    # yt-dlp output template: one file per track, named with the track
    # number + title. Lives directly under ``work_dir`` (no nested
    # playlist folder) so ``_collect_audio_files`` finds them via a
    # single-level walk.
    output_template = str(work_dir / "%(playlist_index)02d-%(title)s.%(ext)s")
    argv = _build_ytdlp_argv(
        ytdlp_bin=ytdlp_bin,
        url=url,
        output_template=output_template,
    )

    logger.info(
        "yt-dlp run: argv=%r work_dir=%s expected_track_count=%d",
        argv, work_dir, expected_track_count)

    if subprocess_run is None:
        returncode, excerpt = _run_ytdlp_streaming(
            argv,
            timeout_sec=timeout_sec,
        )
    else:
        try:
            proc = subprocess_run(
                argv,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE,
                text=True,
                errors="replace",
                timeout=timeout_sec,
                check=False,
            )
        except subprocess.TimeoutExpired as exc:
            # Surface as a yt-dlp failure rather than letting it propagate;
            # the service classifies stderr_excerpt='timeout' as
            # youtube_unknown which is acceptable, but we prepend a clearer
            # marker so operators recognise it in triage.
            stderr_decoded = ""
            if exc.stderr is not None:
                if isinstance(exc.stderr, bytes):
                    stderr_decoded = exc.stderr.decode("utf-8", errors="replace")
                else:
                    stderr_decoded = str(exc.stderr)
            excerpt = _cap_stderr_excerpt(
                f"ERROR: yt-dlp Read timed out after {timeout_sec}s\n"
                f"{stderr_decoded}")
            return YtdlpRunResult(
                exit_code=-1,
                stderr_excerpt=excerpt,
                staged_files=_collect_audio_files(work_dir),
                work_dir=work_dir,
            )
        returncode = int(proc.returncode)
        excerpt = _cap_stderr_excerpt(proc.stderr)
    staged_files = _collect_audio_files(work_dir)
    return YtdlpRunResult(
        exit_code=int(returncode),
        stderr_excerpt=excerpt,
        staged_files=staged_files,
        work_dir=work_dir,
    )


# ---------------------------------------------------------------------------
# Service construction.
# ---------------------------------------------------------------------------


def build_service(
    pdb: Any,
    *,
    temp_dir: Path,
    staging_dir: Path | None = None,
    ytdlp_runner_fn: Callable[..., YtdlpRunResult] | None = None,
) -> YoutubeIngestService:
    """Construct the production ``YoutubeIngestService`` for the worker.

    Sibling of ``default_youtube_ingest_service_factory`` but additionally
    wires the live ``ytdlp_runner_fn``. The CLI / API surfaces never
    invoke ``run_job`` so they have no need for the runner — this is the
    worker's wiring surface.

    Tests pass ``ytdlp_runner_fn=`` directly to control the fake; the
    production default closes over ``temp_dir`` so each worker process
    has its own scratch root.
    """
    if ytdlp_runner_fn is None:
        def _bound_runner(**kwargs: Any) -> YtdlpRunResult:
            return _run_ytdlp(temp_root=temp_dir, **kwargs)
        ytdlp_runner_fn = _bound_runner
    staging_root = (
        Path(stage_to_ai_root(staging_dir=str(staging_dir), auto_import=True))
        if staging_dir is not None
        else Path("/mnt/virtio/Music/Incoming/auto-import")
    )
    return YoutubeIngestService(
        pdb,
        ytdlp_runner_fn=ytdlp_runner_fn,
        mb_track_count_fn=default_mb_track_count_from_mirror,
        staging_root=staging_root,
    )


# ---------------------------------------------------------------------------
# Startup orphan sweep (R22).
# ---------------------------------------------------------------------------


def _cleanup_orphan_paths(
    pdb: Any,
    log_id: int,
    *,
    temp_dir: Path | None,
    staging_root: Path | None,
) -> str | None:
    paths: list[Path] = []
    row = pdb.get_download_log_entry(int(log_id))
    if row is None:
        return None
    request_id = row.get("request_id")
    metadata = row.get("youtube_metadata") or {}
    browse_id = metadata.get("browse_id")
    if temp_dir is not None and isinstance(request_id, int) and isinstance(browse_id, str):
        prefix = f"ytdlp-req{request_id}-{browse_id}-"
        try:
            paths.extend(
                child for child in temp_dir.iterdir()
                if child.name.startswith(prefix)
            )
        except OSError as exc:
            return f"{temp_dir}: {type(exc).__name__}: {exc}"
    if staging_root is not None and isinstance(request_id, int) and isinstance(browse_id, str):
        request_row = pdb.get_request(request_id)
        if request_row is not None:
            paths.append(
                staging_root
                / (
                    f"{_slug(request_row.get('artist_name'))}-"
                    f"{_slug(request_row.get('album_title'))}-"
                    f"{_slug(browse_id)}-request-{request_id}-log-{log_id}"
                )
            )

    errors: list[str] = []
    seen: set[str] = set()
    for path in paths:
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
        except Exception as exc:  # noqa: BLE001 - cleanup is best effort
            errors.append(f"{path}: {type(exc).__name__}: {exc}")
    return "; ".join(errors) if errors else None


def sweep_orphan_running_rows(
    pdb: Any,
    *,
    temp_dir: Path | None = None,
    staging_root: Path | None = None,
) -> list[int]:
    """Mark claimed in-flight ``youtube_running`` rows as failed.

    Accepted-but-unclaimed rows remain drainable across worker downtime.
    A claimed row means a previous worker process exited (cleanly or
    otherwise) after taking ownership and before writing terminal state.
    We surface those as failures with reason ``worker_interrupted`` so the
    operator sees them in the audit trail and can resubmit if desired.

    Returns the list of swept ids for logging / metrics.
    """
    orphan_ids = pdb.find_orphan_youtube_running()
    for log_id in orphan_ids:
        try:
            cleanup_error = _cleanup_orphan_paths(
                pdb,
                int(log_id),
                temp_dir=temp_dir,
                staging_root=staging_root,
            )
            metadata = {"reason": "worker_interrupted"}
            if cleanup_error is not None:
                metadata["cleanup_error"] = cleanup_error
            pdb.update_youtube_terminal(
                int(log_id),
                "youtube_failed",
                metadata,
            )
        except Exception:
            logger.exception(
                "Failed to sweep orphan youtube_running id=%s; "
                "will retry next startup", log_id)
    return list(orphan_ids)


# ---------------------------------------------------------------------------
# One drain iteration.
# ---------------------------------------------------------------------------


def drain_one(
    pdb: Any,
    service: YoutubeIngestService,
    *,
    worker_id: str | None = None,
) -> int | None:
    """Process one pending ``youtube_running`` row.

    Returns the download_log_id processed, or ``None`` when the queue
    was empty. Unhandled exceptions inside :meth:`run_job` are caught
    and written as ``youtube_failed`` with reason
    ``worker_unhandled_exception`` so a single bad job cannot kill the
    worker.
    """
    pending = pdb.claim_next_youtube_pending(worker_id=worker_id, limit=1)
    if not pending:
        return None
    row = pending[0]
    log_id = int(row["id"])
    try:
        result = service.run_job(log_id)
        logger.info(
            "youtube_ingest_worker: drained download_log_id=%s outcome=%s "
            "reason=%s",
            log_id, result.outcome, result.reason)
    except Exception:
        # The service itself catches contract-shaped exceptions inside
        # run_job and writes a terminal row. This block exists for the
        # truly unexpected — a crash after the claim or inside service
        # construction. Persist a terminal row so the
        # job doesn't bounce back as an orphan next startup.
        tb = traceback.format_exc()
        logger.exception(
            "youtube_ingest_worker: unhandled exception on download_log_id=%s",
            log_id)
        try:
            pdb.update_youtube_terminal(
                log_id,
                "youtube_failed",
                {
                    "reason": "worker_unhandled_exception",
                    "stderr_excerpt": tb[-STDERR_EXCERPT_BYTES_LIMIT:],
                },
            )
        except Exception:
            logger.exception(
                "youtube_ingest_worker: terminal write failed for "
                "download_log_id=%s; row will be re-swept on next restart",
                log_id)
    return log_id


# ---------------------------------------------------------------------------
# Main loop.
# ---------------------------------------------------------------------------


def run_loop(
    pdb: Any,
    *,
    service: YoutubeIngestService,
    poll_interval: float,
    worker_id: str | None = None,
    once: bool = False,
    sleep_fn: Callable[[float], None] = time.sleep,
    iterations: int | None = None,
) -> int:
    """Drain the queue forever, sleeping when idle.

    ``once=True`` returns after the first iteration regardless of whether
    a row was drained. ``iterations`` (test-only) caps the loop at N
    iterations. ``sleep_fn`` is a kwarg-DI seam so tests can swap
    ``time.sleep`` without monkey-patching.
    """
    seen = 0
    while True:
        processed = drain_one(pdb, service, worker_id=worker_id)
        seen += 1
        if once:
            return 0
        if iterations is not None and seen >= iterations:
            return 0
        if processed is None:
            sleep_fn(poll_interval)


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Drain the YouTube-rescue ingest queue",
    )
    parser.add_argument("--dsn", default=DEFAULT_DSN)
    parser.add_argument("--poll-interval", type=float, default=5.0)
    parser.add_argument(
        "--temp-dir",
        type=Path,
        default=DEFAULT_TEMP_DIR,
        help=(
            "Per-process scratch directory yt-dlp downloads into before "
            "files are moved to the configured auto-import staging root."),
    )
    parser.add_argument(
        "--staging-dir",
        type=Path,
        default=Path("/mnt/virtio/Music/Incoming"),
        help=(
            "Shared beets staging root; YT rescues publish under its "
            "auto-import/ child."),
    )
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--worker-id", default=None)
    args = parser.parse_args(list(argv) if argv is not None else None)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )
    worker_id = args.worker_id or f"{socket.gethostname()}:{os.getpid()}"

    args.temp_dir.mkdir(parents=True, exist_ok=True)

    db = PipelineDB(args.dsn)
    try:
        with db.advisory_lock(
            ADVISORY_LOCK_NAMESPACE_YOUTUBE_INGEST, 1,
        ) as acquired:
            if not acquired:
                logger.error(
                    "Another cratedigger-youtube-ingest worker is already "
                    "running (worker_id=%s); exiting", worker_id)
                # Exit 0 because the contention is the expected behaviour
                # for a duplicate-start, not a crash. systemd's
                # Restart=on-failure won't fire.
                return 0
            logger.info(
                "youtube_ingest_worker started: worker_id=%s temp_dir=%s "
                "poll_interval=%ss", worker_id, args.temp_dir,
                args.poll_interval)

            staging_root = Path(stage_to_ai_root(
                staging_dir=str(args.staging_dir),
                auto_import=True,
            ))
            swept = sweep_orphan_running_rows(
                db,
                temp_dir=args.temp_dir,
                staging_root=staging_root,
            )
            if swept:
                logger.warning(
                    "Swept %d abandoned youtube_running row(s): %s",
                    len(swept), swept)

            service = build_service(
                db, temp_dir=args.temp_dir, staging_dir=args.staging_dir)
            try:
                return run_loop(
                    db,
                    service=service,
                    poll_interval=args.poll_interval,
                    worker_id=worker_id,
                    once=args.once,
                )
            except KeyboardInterrupt:
                # Graceful shutdown on SIGINT (default Python behaviour)
                # and SIGTERM (Python translates to KeyboardInterrupt
                # when no handler is installed — but only in 3.12+; we
                # don't rely on it). Either way, the with-statement
                # unwinds and the advisory lock releases as a session
                # property when the connection closes below.
                logger.info(
                    "youtube_ingest_worker: SIGINT received; shutting "
                    "down cleanly")
                return 0
    finally:
        db.close()


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
