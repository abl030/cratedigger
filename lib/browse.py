"""Directory browsing and file filtering helpers for Cratedigger."""

from __future__ import annotations

import logging
import threading
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    from lib.config import CratediggerConfig
    from lib.context import CratediggerContext
    from cratedigger import SlskdDirectory


logger = logging.getLogger("cratedigger")


def _routine_browse_http_status(error: Exception) -> int | None:
    if error.__class__.__name__ != "HTTPError":
        return None
    response = getattr(error, "response", None)
    if response is None:
        return None
    status = getattr(response, "status_code", None)
    if not isinstance(status, int):
        return None
    if status == 404 or 500 <= status <= 599:
        return status
    return None


@dataclass(frozen=True)
class BrowseManyResult:
    directories: dict[tuple[str, str], Any] = field(default_factory=dict)
    negative_skips: set[tuple[str, str]] = field(default_factory=set)
    browse_attempts: int = 0


@dataclass(frozen=True)
class BrowseOneResult:
    file_dir: str
    directory: Any | None = None
    cache_negative: bool = False


def _peer_cache_for(ctx: CratediggerContext) -> Any | None:
    return getattr(ctx, "peer_cache", None)


def _drain_peer_cache_stats(ctx: CratediggerContext) -> None:
    cache = _peer_cache_for(ctx)
    if cache is None:
        return
    from lib.peer_cache import drain_stats_into_context
    drain_stats_into_context(ctx, cache)


class BrowseCoordinator:
    """Shared browse/cache boundary with global capacity and single-flight."""

    def __init__(self, ctx: CratediggerContext, max_workers: int) -> None:
        self._ctx = ctx
        self._max_workers = max(1, int(max_workers))
        self._executor = ThreadPoolExecutor(
            max_workers=self._max_workers,
            thread_name_prefix="browse",
        )
        self._lock = threading.RLock()
        self._inflight: dict[tuple[str, str], Future[BrowseOneResult]] = {}

    @property
    def max_workers(self) -> int:
        return self._max_workers

    def cache_directory(
        self,
        username: str,
        file_dir: str,
        directory: Any,
    ) -> None:
        with self._lock:
            _cache_browsed_directory(self._ctx, username, file_dir, directory)

    def ensure_user(self, username: str) -> None:
        with self._lock:
            _ensure_cache_user(self._ctx, username)

    def browse_many(
        self,
        work_items: list[tuple[str, str]],
        slskd_client: Any,
    ) -> BrowseManyResult:
        """Browse uncached directories with process-wide capacity."""
        if not work_items:
            return BrowseManyResult()

        futures: dict[tuple[str, str], Future[BrowseOneResult]] = {}
        negative_skips: set[tuple[str, str]] = set()
        browse_attempts = 0
        for user, file_dir in work_items:
            key = (user, file_dir)
            with self._lock:
                self._ctx.folder_cache.setdefault(user, {})
                cached = self._ctx.folder_cache[user].get(file_dir)
                if cached is not None:
                    future: Future[BrowseOneResult] = Future()
                    future.set_result(BrowseOneResult(file_dir, cached))
                    futures[key] = future
                    continue
                existing_future = self._inflight.get(key)
                if existing_future is not None:
                    futures[key] = existing_future
                    continue

            peer_cache = _peer_cache_for(self._ctx)
            if peer_cache is not None:
                redis_cached = peer_cache.get_directory(user, file_dir)
                _drain_peer_cache_stats(self._ctx)
                if redis_cached is not None:
                    with self._lock:
                        existing_future = self._inflight.get(key)
                        if existing_future is None:
                            _store_hot_directory(
                                self._ctx, user, file_dir, redis_cached,
                            )
                        else:
                            futures[key] = existing_future
                            continue
                    future = Future()
                    future.set_result(BrowseOneResult(file_dir, redis_cached))
                    futures[key] = future
                    continue
                if peer_cache.has_negative(user, file_dir):
                    _drain_peer_cache_stats(self._ctx)
                    with self._lock:
                        cached = self._ctx.folder_cache[user].get(file_dir)
                        if cached is not None:
                            future = Future()
                            future.set_result(BrowseOneResult(file_dir, cached))
                            futures[key] = future
                            continue
                        existing_future = self._inflight.get(key)
                        if existing_future is not None:
                            futures[key] = existing_future
                            continue
                    negative_skips.add(key)
                    continue
                _drain_peer_cache_stats(self._ctx)

            with self._lock:
                cached = self._ctx.folder_cache[user].get(file_dir)
                if cached is not None:
                    future = Future()
                    future.set_result(BrowseOneResult(file_dir, cached))
                    futures[key] = future
                    continue
                existing_future = self._inflight.get(key)
                if existing_future is None:
                    browse_future = self._executor.submit(
                        self._browse_and_cache, user, file_dir, slskd_client,
                    )
                    self._inflight[key] = browse_future
                    self._ctx.peer_dir_observations.add(key)
                    browse_attempts += 1
                else:
                    browse_future = existing_future
                futures[key] = browse_future

        results: dict[tuple[str, str], Any] = {}
        for key, future in futures.items():
            result = future.result()
            if result.directory is not None:
                results[key] = result.directory
            elif result.cache_negative:
                negative_skips.add(key)
        return BrowseManyResult(
            directories=results,
            negative_skips=negative_skips,
            browse_attempts=browse_attempts,
        )

    def shutdown(self, *, wait: bool = True, cancel_futures: bool = False) -> None:
        self._executor.shutdown(wait=wait, cancel_futures=cancel_futures)

    def _browse_and_cache(
        self,
        username: str,
        file_dir: str,
        slskd_client: Any,
    ) -> BrowseOneResult:
        result = _browse_one(username, file_dir, slskd_client)
        with self._lock:
            if result.directory is not None:
                _store_hot_directory(self._ctx, username, file_dir, result.directory)
            self._inflight.pop((username, file_dir), None)
        if result.directory is not None:
            _write_peer_cache_directory(self._ctx, username, file_dir, result.directory)
        elif result.cache_negative:
            _write_peer_cache_negative(self._ctx, username, file_dir)
        return result


def get_browse_coordinator(
    ctx: CratediggerContext,
    max_workers: int,
) -> BrowseCoordinator:
    requested_workers = max(1, int(max_workers))
    lock = getattr(ctx, "browse_coordinator_lock", None)
    if lock is None:
        lock = threading.Lock()
        ctx.browse_coordinator_lock = lock
    with lock:
        coordinator = getattr(ctx, "browse_coordinator", None)
        if coordinator is None:
            coordinator = BrowseCoordinator(ctx, requested_workers)
            ctx.browse_coordinator = coordinator
        elif coordinator.max_workers != requested_workers:
            raise ValueError(
                "BrowseCoordinator already initialised with "
                f"max_workers={coordinator.max_workers}; requested {max_workers}"
            )
        return coordinator


def shutdown_browse_coordinator(
    ctx: CratediggerContext,
    *,
    wait: bool = True,
    cancel_futures: bool = False,
) -> None:
    coordinator = getattr(ctx, "browse_coordinator", None)
    if isinstance(coordinator, BrowseCoordinator):
        coordinator.shutdown(wait=wait, cancel_futures=cancel_futures)
        ctx.browse_coordinator = None


def _cache_browsed_directory(
    ctx: CratediggerContext,
    username: str,
    file_dir: str,
    directory: Any,
    *,
    persist: bool = True,
) -> None:
    _store_hot_directory(ctx, username, file_dir, directory)
    if persist:
        _write_peer_cache_directory(ctx, username, file_dir, directory)


def _store_hot_directory(
    ctx: CratediggerContext,
    username: str,
    file_dir: str,
    directory: Any,
) -> None:
    _ensure_cache_user(ctx, username)
    ctx.folder_cache[username][file_dir] = directory
    negative_skips = getattr(ctx, "peer_cache_negative_skips", None)
    if negative_skips is not None:
        negative_skips.discard((username, file_dir))


def _write_peer_cache_directory(
    ctx: CratediggerContext,
    username: str,
    file_dir: str,
    directory: Any,
) -> None:
    peer_cache = _peer_cache_for(ctx)
    if peer_cache is not None:
        peer_cache.set_directory(username, file_dir, directory)
        _drain_peer_cache_stats(ctx)


def _write_peer_cache_negative(
    ctx: CratediggerContext,
    username: str,
    file_dir: str,
) -> None:
    peer_cache = _peer_cache_for(ctx)
    if peer_cache is not None:
        peer_cache.set_negative(username, file_dir)
        _drain_peer_cache_stats(ctx)


def _ensure_cache_user(ctx: CratediggerContext, username: str) -> None:
    ctx.folder_cache.setdefault(username, {})


def ensure_cache_user(ctx: CratediggerContext, username: str) -> None:
    """Ensure shared browse cache buckets exist without overwriting entries."""
    coordinator = getattr(ctx, "browse_coordinator", None)
    if isinstance(coordinator, BrowseCoordinator):
        coordinator.ensure_user(username)
    else:
        _ensure_cache_user(ctx, username)


def cache_browsed_directory(
    ctx: CratediggerContext,
    username: str,
    file_dir: str,
    directory: Any,
) -> None:
    """Store a browsed directory through the shared browse boundary."""
    coordinator = getattr(ctx, "browse_coordinator", None)
    if isinstance(coordinator, BrowseCoordinator):
        coordinator.cache_directory(username, file_dir, directory)
    else:
        _cache_browsed_directory(ctx, username, file_dir, directory)


def download_filter(
    allowed_filetype: str,
    directory: SlskdDirectory,
    download_cfg: CratediggerConfig,
) -> SlskdDirectory:
    """Return a filtered directory listing without mutating the input."""
    logging.debug("download_filtering")
    if download_cfg.download_filtering:
        from lib.quality import audio_file_matches

        whitelist: set[str] = set()
        if download_cfg.use_extension_whitelist:
            whitelist = {
                ext.lower().lstrip(".")
                for ext in download_cfg.extensions_whitelist
            }
        kept = []
        for file in directory["files"]:
            filename = file["filename"]
            ext = filename.rsplit(".", 1)[-1].lower() if "." in filename else ""
            if audio_file_matches(file, allowed_filetype) or ext in whitelist:
                logger.debug(f"Added file to queue: {filename}")
                kept.append(file)
            else:
                logger.debug(f"Unwanted file: {filename}")
        return {**directory, "files": kept}
    return directory


_PENALTY_KEYWORDS = (
    "archive", "best of", "greatest hits", "magazine", "compilation",
    "singles", "soundtrack", "various", "bootleg", "discography",
)


def rank_candidate_dirs(
    file_dirs: list[str], album_title: str, artist_name: str
) -> list[str]:
    """Sort candidate directories by likelihood of being the correct album."""
    title_lower = album_title.lower()
    artist_lower = artist_name.lower()

    def _score(d: str) -> int:
        d_lower = d.lower()
        score = 0
        if title_lower in d_lower:
            score += 2
        if artist_lower in d_lower:
            score += 1
        for kw in _PENALTY_KEYWORDS:
            if kw in d_lower:
                score -= 3
                break
        return score

    return sorted(file_dirs, key=_score, reverse=True)


def _browse_one(
    username: str,
    file_dir: str,
    slskd_client: Any,
) -> BrowseOneResult:
    """Browse a single directory from slskd."""
    try:
        directories = slskd_client.users.directory(
            username=username,
            directory=file_dir,
        )
    except Exception as e:
        status = _routine_browse_http_status(e)
        if status is not None:
            logger.info(
                f"browse skip {username}/{file_dir}: slskd returned HTTP {status}"
            )
        else:
            logger.exception(f'Error getting directory from user: "{username}"')
        return BrowseOneResult(file_dir)
    if not directories:
        return BrowseOneResult(file_dir, cache_negative=True)
    try:
        directory = directories[0]
    except Exception:
        logger.exception(f'Invalid directory response from user: "{username}"')
        return BrowseOneResult(file_dir)
    return BrowseOneResult(file_dir, directory)


def _browse_directories(
    dirs_to_browse: list[str],
    username: str,
    slskd_client: Any,
    max_workers: int = 4,
) -> dict[str, Any]:
    """Browse multiple directories in parallel."""
    if not dirs_to_browse:
        return {}

    if len(dirs_to_browse) == 1:
        result = _browse_one(username, dirs_to_browse[0], slskd_client)
        if result.directory is None:
            return {}
        return {result.file_dir: result.directory}

    results: dict[str, Any] = {}
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {
            pool.submit(_browse_one, username, d, slskd_client): d
            for d in dirs_to_browse
        }
        for future in as_completed(futures):
            result = future.result()
            if result.directory is not None:
                results[result.file_dir] = result.directory

    return results


def _fanout_browse_users(
    work_items: list[tuple[str, str]],
    slskd_client: Any,
    ctx: CratediggerContext,
    max_workers: int,
) -> BrowseManyResult:
    """Bounded parallel browse fan-out across (user, dir) pairs.

    Routes each `(username, file_dir)` browse through the shared
    BrowseCoordinator so concurrent find_download jobs share one global
    browse capacity and duplicate cold directory misses are single-flighted.

    No client-side wave deadline — slskd's per-peer TCP read timeout
    (~30–60 s) is the only authority on when a hung peer is abandoned.
    The previous wave deadline + cycle budget were short-circuiting before
    real peers had a chance to respond and were starving the pipeline (see
    2026-05-02 regression).

    Thread-safety: BrowseCoordinator owns writes to `ctx.folder_cache`;
    callers observe "all dirs failed for this user" via an empty pre-created
    inner dict.
    """
    if not work_items:
        return BrowseManyResult()

    return get_browse_coordinator(ctx, max_workers).browse_many(work_items, slskd_client)


def _browse_directories_for_ctx(
    dirs_to_browse: list[str],
    username: str,
    ctx: CratediggerContext,
    max_workers: int,
) -> dict[str, Any]:
    """Browse one user's directories through the shared context boundary."""
    work = [(username, d) for d in dirs_to_browse]
    browsed = get_browse_coordinator(ctx, max_workers).browse_many(work, ctx.slskd).directories
    return {
        file_dir: directory
        for (_username, file_dir), directory in browsed.items()
    }


def _browse_directories_for_ctx_result(
    dirs_to_browse: list[str],
    username: str,
    ctx: CratediggerContext,
    max_workers: int,
) -> BrowseManyResult:
    """Browse one user's directories and preserve negative-skip metadata."""
    work = [(username, d) for d in dirs_to_browse]
    return get_browse_coordinator(ctx, max_workers).browse_many(work, ctx.slskd)
