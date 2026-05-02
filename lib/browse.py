"""Directory browsing and file filtering helpers for Cratedigger."""

from __future__ import annotations

import concurrent.futures.thread as _futures_thread
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from concurrent.futures import TimeoutError as FuturesTimeoutError
from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    from lib.config import CratediggerConfig
    from lib.context import CratediggerContext
    from cratedigger import SlskdDirectory


logger = logging.getLogger("cratedigger")


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
    deadline_s: float,
) -> set[str]:
    """Bounded parallel browse fan-out across (user, dir) pairs.

    Submits each `(username, file_dir)` browse to a single bounded
    ThreadPoolExecutor and writes successful results into
    `ctx.folder_cache[user][dir]` and `ctx._folder_cache_ts[user][dir]`.

    The wave deadline is enforced via `as_completed(timeout=...)` plus manual
    executor lifetime + `shutdown(wait=False, cancel_futures=True)` so a
    single hung peer cannot stretch the wave to its TCP timeout.

    Thread-safety: workers only return tuples — the calling thread is the
    sole writer to `ctx.folder_cache`. The Step 1 pre-create is therefore
    not a race fix; it makes "all dirs failed for this user" observable to
    callers (the inner dict exists but is empty). Without it, the regression
    test couldn't distinguish "tried, every browse raised" from "user was
    never in the work plan."

    Orphan-thread cost: futures whose results are abandoned past the wave
    deadline keep running until their own TCP timeout (~30–60s). Plain
    `ThreadPoolExecutor` workers are non-daemon and tracked in
    `concurrent.futures.thread._threads_queues`; on interpreter exit, an
    atexit handler joins every entry — even after `shutdown(wait=False,
    cancel_futures=True)`. For a 5-min oneshot, that lets orphan TCP timeouts
    bleed past the cycle boundary and delay the next systemd-timer fire. We
    detach the pool's workers from the registry so atexit doesn't wait on
    them; they still finish their network calls naturally, we just don't
    block the process exit on them.

    Returns the set of usernames whose futures had not completed by the
    deadline (any one outstanding `(user, dir)` future puts that user in
    the set; the caller treats them as broken-for-this-cycle).
    """
    if not work_items:
        return set()

    # Step 1: pre-create user buckets so callers can observe "every dir
    # failed for this user" via an empty inner dict.
    for user, _file_dir in work_items:
        ctx.folder_cache.setdefault(user, {})
        ctx._folder_cache_ts.setdefault(user, {})

    timed_out_users: set[str] = set()
    pool = ThreadPoolExecutor(max_workers=max_workers)
    try:
        futures = {
            pool.submit(_browse_one, user, file_dir, slskd_client): (user, file_dir)
            for (user, file_dir) in work_items
        }
        try:
            for fut in as_completed(futures, timeout=deadline_s):
                user, file_dir = futures[fut]
                _file_dir, result = fut.result()
                if result is not None:
                    ctx.folder_cache[user][file_dir] = result
                    ctx._folder_cache_ts[user][file_dir] = time.time()
        except FuturesTimeoutError:
            for fut, (user, _file_dir) in futures.items():
                if not fut.done():
                    timed_out_users.add(user)
    finally:
        # cancel_futures=True (Python 3.9+) cancels queued, not-yet-started
        # tasks. Running tasks keep running but their results are abandoned.
        # This is what makes the wave deadline actually bound wall-clock.
        pool.shutdown(wait=False, cancel_futures=True)
        _detach_workers_from_atexit(pool)

    return timed_out_users


def _detach_workers_from_atexit(pool: ThreadPoolExecutor) -> None:
    """Remove `pool`'s workers from concurrent.futures' atexit registry.

    `ThreadPoolExecutor` registers each worker thread in the module-level
    `_threads_queues` dict; an atexit handler joins every entry on
    interpreter shutdown — even after `shutdown(wait=False, cancel_futures=
    True)`. For an orphaned worker stuck on a 30–60 s slskd TCP timeout,
    that means the cratedigger oneshot cannot exit until the timeout
    elapses, defeating the wave deadline at the *process* boundary.

    Popping the workers from `_threads_queues` detaches them from the join
    — the threads still finish their network calls naturally and any
    pending I/O completes; we just don't make the process wait for them.

    Uses a private CPython attribute. Wrapped in try/except so a future
    Python release that renames or removes the registry degrades to the
    pre-existing behavior (slow-but-correct exit) rather than crashing.
    """
    try:
        # `pool._threads` is a set of Thread objects; remove each from the
        # global registry. Iterate a copy to keep the dict mutation safe.
        registry: dict[Any, Any] = _futures_thread._threads_queues  # type: ignore[assignment]
        for thread in list(getattr(pool, "_threads", []) or []):
            registry.pop(thread, None)
    except Exception:
        logger.debug("Could not detach fan-out workers from atexit registry", exc_info=True)
