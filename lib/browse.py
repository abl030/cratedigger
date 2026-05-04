"""Directory browsing and file filtering helpers for Cratedigger."""

from __future__ import annotations

import logging
import time
import threading
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    from lib.config import CratediggerConfig
    from lib.context import CratediggerContext
    from cratedigger import SlskdDirectory


logger = logging.getLogger("cratedigger")


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
        self._inflight: dict[tuple[str, str], Future[Any | None]] = {}

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
    ) -> dict[tuple[str, str], Any]:
        """Browse uncached directories with process-wide capacity."""
        if not work_items:
            return {}

        futures: dict[tuple[str, str], Future[Any | None]] = {}
        with self._lock:
            for user, file_dir in work_items:
                self._ctx.folder_cache.setdefault(user, {})
                self._ctx._folder_cache_ts.setdefault(user, {})
                cached = self._ctx.folder_cache[user].get(file_dir)
                if cached is not None:
                    future: Future[Any | None] = Future()
                    future.set_result(cached)
                    futures[(user, file_dir)] = future
                    continue
                key = (user, file_dir)
                existing_future = self._inflight.get(key)
                if existing_future is None:
                    browse_future = self._executor.submit(
                        self._browse_and_cache, user, file_dir, slskd_client,
                    )
                    self._inflight[key] = browse_future
                else:
                    browse_future = existing_future
                futures[key] = browse_future

        results: dict[tuple[str, str], Any] = {}
        for key, future in futures.items():
            result = future.result()
            if result is not None:
                results[key] = result
        return results

    def shutdown(self, *, wait: bool = True, cancel_futures: bool = False) -> None:
        self._executor.shutdown(wait=wait, cancel_futures=cancel_futures)

    def _browse_and_cache(
        self,
        username: str,
        file_dir: str,
        slskd_client: Any,
    ) -> Any | None:
        _file_dir, result = _browse_one(username, file_dir, slskd_client)
        with self._lock:
            if result is not None:
                _cache_browsed_directory(self._ctx, username, file_dir, result)
            self._inflight.pop((username, file_dir), None)
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
) -> None:
    _ensure_cache_user(ctx, username)
    ctx.folder_cache[username][file_dir] = directory
    ctx._folder_cache_ts[username][file_dir] = time.time()


def _ensure_cache_user(ctx: CratediggerContext, username: str) -> None:
    ctx.folder_cache.setdefault(username, {})
    ctx._folder_cache_ts.setdefault(username, {})


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
        from lib.quality import parse_filetype_config, AUDIO_EXTENSIONS as _all_audio

        spec = parse_filetype_config(allowed_filetype)
        whitelist = []
        if download_cfg.use_extension_whitelist:
            whitelist = list(download_cfg.extensions_whitelist)
        if spec.extension == "*":
            whitelist.extend(_all_audio)
        else:
            whitelist.append(spec.extension)
        unwanted = []
        logger.debug(f"Accepted extensions: {whitelist}")
        for file in directory["files"]:
            for extension in whitelist:
                if file["filename"].split(".")[-1].lower() == extension.lower():
                    break
            else:
                unwanted.append(file["filename"])
                logger.debug(f"Unwanted file: {file['filename']}")
        if len(unwanted) > 0:
            temp = []
            logger.debug(f"Unwanted Files: {unwanted}")
            for file in directory["files"]:
                if file["filename"] not in unwanted:
                    logger.debug(f"Added file to queue: {file['filename']}")
                    temp.append(file)
            for files in temp:
                logger.debug(f"File in final list: {files['filename']}")
            return {**directory, "files": temp}
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
) -> tuple[str, Any | None]:
    """Browse a single directory from slskd."""
    try:
        directory = slskd_client.users.directory(
            username=username,
            directory=file_dir,
        )[0]
        return file_dir, directory
    except Exception:
        logger.exception(f'Error getting directory from user: "{username}"')
        return file_dir, None


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
        file_dir, result = _browse_one(username, dirs_to_browse[0], slskd_client)
        return {file_dir: result} if result is not None else {}

    results: dict[str, Any] = {}
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {
            pool.submit(_browse_one, username, d, slskd_client): d
            for d in dirs_to_browse
        }
        for future in as_completed(futures):
            file_dir, result = future.result()
            if result is not None:
                results[file_dir] = result

    return results


def _fanout_browse_users(
    work_items: list[tuple[str, str]],
    slskd_client: Any,
    ctx: CratediggerContext,
    max_workers: int,
) -> None:
    """Bounded parallel browse fan-out across (user, dir) pairs.

    Routes each `(username, file_dir)` browse through the shared
    BrowseCoordinator so concurrent find_download jobs share one global
    browse capacity and duplicate cold directory misses are single-flighted.

    No client-side wave deadline — slskd's per-peer TCP read timeout
    (~30–60 s) is the only authority on when a hung peer is abandoned.
    The previous wave deadline + cycle budget were short-circuiting before
    real peers had a chance to respond and were starving the pipeline (see
    2026-05-02 regression).

    Thread-safety: BrowseCoordinator owns writes to `ctx.folder_cache` and
    `ctx._folder_cache_ts`; callers observe "all dirs failed for this user"
    via an empty pre-created inner dict.
    """
    if not work_items:
        return

    get_browse_coordinator(ctx, max_workers).browse_many(work_items, slskd_client)


def _browse_directories_for_ctx(
    dirs_to_browse: list[str],
    username: str,
    ctx: CratediggerContext,
    max_workers: int,
) -> dict[str, Any]:
    """Browse one user's directories through the shared context boundary."""
    work = [(username, d) for d in dirs_to_browse]
    browsed = get_browse_coordinator(ctx, max_workers).browse_many(work, ctx.slskd)
    return {
        file_dir: directory
        for (_username, file_dir), directory in browsed.items()
    }
