#!/usr/bin/env python
from __future__ import annotations

import argparse
import os
import sys
import time
import difflib
import configparser
import logging
from typing import Any, Sequence, TYPE_CHECKING, TypedDict
import copy
import slskd_api

if TYPE_CHECKING:
    from album_source import DatabaseSource
    from lib.config import SoularrConfig
    from lib.context import SoularrContext


class TrackRecord(TypedDict):
    """Track dict from pipeline DB — shape used by matching functions."""
    albumId: int
    title: str
    mediumNumber: int


class _SlskdFileRequired(TypedDict):
    filename: str

class SlskdFile(_SlskdFileRequired, total=False):
    """File dict from slskd directory browse. Only filename is required."""
    size: int
    bitRate: int
    sampleRate: int
    bitDepth: int
    isVariableBitRate: bool


class SlskdDirectory(TypedDict):
    """Directory dict from slskd users.directory() API."""
    directory: str
    files: list[SlskdFile]


class EnvInterpolation(configparser.ExtendedInterpolation):
    """
    Interpolation which expands environment variables in values.
    Borrowed from https://stackoverflow.com/a/68068943
    """

    def before_read(self, parser, section, option, value):
        value = super().before_read(parser, section, option, value)
        return os.path.expandvars(value)


# Allows backwards compatibility for users updating an older version of Soularr
# without using the new [Logging] section in the config.ini file.
DEFAULT_LOGGING_CONF = {
    "level": "INFO",
    "format": "[%(levelname)s|%(module)s|L%(lineno)d] %(asctime)s: %(message)s",
    "datefmt": "%Y-%m-%dT%H:%M:%S%z",
}

# === Typed Config (populated in main() via SoularrConfig.from_ini()) ===
cfg: SoularrConfig = None  # type: ignore[assignment]  # Set in main()

# === API Clients & Logging ===
slskd: slskd_api.SlskdClient = None  # type: ignore[assignment]  # Set in main()
config = None
logger = logging.getLogger("soularr")

# === API client instances (set in main()) ===
pipeline_db_source: "DatabaseSource" = None  # type: ignore[assignment]  # Set in main()

# === Runtime context (populated in main()) ===
# Module-level reference for thin wrappers that can't receive ctx as a parameter.
# All matching/search functions receive ctx explicitly.
_module_ctx: Any = None  # SoularrContext — set in main()


def album_match(expected_tracks: Sequence[TrackRecord], slskd_tracks: Sequence[SlskdFile], username: str, filetype: str, ctx: SoularrContext) -> bool:
    counted = []
    total_match = 0.0

    album_info = get_album_by_id(expected_tracks[0]["albumId"], ctx)
    album_name = album_info.title
    artist_name = album_info.artist_name

    from lib.quality import parse_filetype_config
    spec = parse_filetype_config(filetype)
    is_catch_all = spec.extension == "*"
    for expected_track in expected_tracks:
        best_match = 0.0
        expected_filename = expected_track["title"]  # fallback for empty slskd_tracks

        for slskd_track in slskd_tracks:
            # For catch-all, use the actual extension from the slskd track
            if is_catch_all:
                slskd_ext = slskd_track["filename"].rsplit(".", 1)[-1].lower() if "." in slskd_track["filename"] else ""
                expected_filename = expected_track["title"] + "." + slskd_ext
            else:
                expected_filename = expected_track["title"] + "." + spec.extension
            slskd_filename = slskd_track["filename"]

            # Try to match the ratio with the exact filenames
            ratio = difflib.SequenceMatcher(None, expected_filename, slskd_filename).ratio()

            # If ratio is a bad match try and split off (with " " as the separator) the garbage at the start of the slskd_filename and try again
            ratio = check_ratio(" ", ratio, expected_filename, slskd_filename)
            # Same but with "_" as the separator
            ratio = check_ratio("_", ratio, expected_filename, slskd_filename)

            # Same checks but preappend album name.
            ratio = check_ratio("", ratio, album_name + " " + expected_filename, slskd_filename)
            ratio = check_ratio(" ", ratio, album_name + " " + expected_filename, slskd_filename)
            ratio = check_ratio("_", ratio, album_name + " " + expected_filename, slskd_filename)

            if ratio > best_match:
                best_match = ratio

        if best_match > cfg.minimum_match_ratio:
            counted.append(expected_filename)
            total_match += best_match

    if len(counted) == len(expected_tracks) and username not in cfg.ignored_users:
        logger.info(f"Found match from user: {username} for {len(counted)} tracks! Track attributes: {filetype}")
        logger.info(f"Average sequence match ratio: {total_match / len(counted)}")
        logger.info("SUCCESSFUL MATCH")
        logger.info("-------------------")
        return True

    return False


def check_ratio(separator, ratio, expected_filename, slskd_filename):
    if ratio < cfg.minimum_match_ratio:
        if separator != "":
            expected_filename_word_count = len(expected_filename.split()) * -1
            truncated_slskd_filename = " ".join(slskd_filename.split(separator)[expected_filename_word_count:])
            ratio = difflib.SequenceMatcher(None, expected_filename, truncated_slskd_filename).ratio()
        else:
            ratio = difflib.SequenceMatcher(None, expected_filename, slskd_filename).ratio()

        return ratio
    return ratio


def album_track_num(directory: SlskdDirectory) -> dict[str, Any]:
    from lib.quality import AUDIO_EXTENSIONS as _all_audio_exts
    files = directory["files"]
    specs = cfg.allowed_specs
    # Check if any spec is catch-all ("*")
    has_catch_all = any(s.extension == "*" for s in specs)
    allowed_exts = list(_all_audio_exts) if has_catch_all else [s.extension for s in specs]
    count = 0
    index = -1
    filetype = ""
    for file in files:
        ext = file["filename"].split(".")[-1].lower()
        if ext in allowed_exts:
            if has_catch_all:
                # Catch-all: count all audio files, track majority extension
                if index == -1:
                    filetype = ext
                count += 1
            else:
                new_index = allowed_exts.index(ext)
                if index == -1:
                    index = new_index
                    filetype = allowed_exts[index]
                elif new_index != index:
                    filetype = ""
                    break
                count += 1

    return_data = {"count": count, "filetype": filetype}
    return return_data


def release_trackcount_mode(releases):
    track_count = {}

    for release in releases:
        trackcount = release.track_count
        if trackcount in track_count:
            track_count[trackcount] += 1
        else:
            track_count[trackcount] = 1

    most_common_trackcount = None
    max_count = 0

    for trackcount, count in track_count.items():
        if count > max_count:
            max_count = count
            most_common_trackcount = trackcount

    return most_common_trackcount


def choose_release(artist_name, releases):
    most_common_trackcount = release_trackcount_mode(releases)

    # Prefer the release marked as monitored — this is the one the
    # user explicitly selected in the UI and represents the edition they want.
    for release in releases:
        if not release.monitored:
            continue
        country = release.country[0] if release.country else None
        if release.format[1] == "x" and cfg.allow_multi_disc:
            format_accepted = release.format.split("x", 1)[1] in cfg.accepted_formats
        else:
            format_accepted = release.format in cfg.accepted_formats
        if format_accepted:
            logger.info(
                f"Selected monitored release for {artist_name}: {release.status}, "
                f"{country}, {release.format}, Mediums: {release.medium_count}, "
                f"Tracks: {release.track_count}, ID: {release.id}"
            )
            return release

    for release in releases:
        country = release.country[0] if release.country else None

        if release.format[1] == "x" and cfg.allow_multi_disc:
            format_accepted = release.format.split("x", 1)[1] in cfg.accepted_formats
        else:
            format_accepted = release.format in cfg.accepted_formats

        if cfg.use_most_common_tracknum:
            if release.track_count == most_common_trackcount:
                track_count_bool = True
            else:
                track_count_bool = False
        else:
            track_count_bool = True

        if (cfg.skip_region_check or country in cfg.accepted_countries) and format_accepted and release.status == "Official" and track_count_bool:
            logger.info(
                ", ".join(
                    [
                        f"Selected release for {artist_name}: {release.status}",
                        str(country),
                        release.format,
                        f"Mediums: {release.medium_count}",
                        f"Tracks: {release.track_count}",
                        f"ID: {release.id}",
                    ]
                )
            )

            return release

    if cfg.use_most_common_tracknum:
        for release in releases:
            if release.track_count == most_common_trackcount:
                return release
        else:
            default_release = releases[0]

    else:
        default_release = releases[0]

    return default_release




def download_filter(allowed_filetype: str, directory: SlskdDirectory) -> SlskdDirectory:
    """
    Filters the directory listing from SLSKD using the filetype whitelist.
    If not using the whitelist it will only return the audio files of the allowed filetype.
    This is to prevent downloading m3u,cue,txt,jpg,etc. files that are sometimes stored in
    the same folders as the music files.
    """
    logging.debug("download_filtering")
    if cfg.download_filtering:
        from lib.quality import parse_filetype_config, AUDIO_EXTENSIONS as _all_audio
        spec = parse_filetype_config(allowed_filetype)
        whitelist = []  # Init an empty list to take just the allowed_filetype
        if cfg.use_extension_whitelist:
            whitelist = list(cfg.extensions_whitelist)
        if spec.extension == "*":
            whitelist.extend(_all_audio)
        else:
            whitelist.append(spec.extension)
        unwanted = []
        logger.debug(f"Accepted extensions: {whitelist}")
        for file in directory["files"]:
            for extension in whitelist:
                if file["filename"].split(".")[-1].lower() == extension.lower():
                    break  # Jump out and don't add wanted files to the unwanted list
            else:
                unwanted.append(file["filename"])  # Add to list of files to remove from the wanted list
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
            return {**directory, "files": temp}  # New dict — never mutate the input
    return directory



_PENALTY_KEYWORDS = (
    "archive", "best of", "greatest hits", "magazine", "compilation",
    "singles", "soundtrack", "various", "bootleg", "discography",
)


def rank_candidate_dirs(
    file_dirs: list[str], album_title: str, artist_name: str
) -> list[str]:
    """Sort candidate directories by likelihood of being the correct album.

    Promotes paths containing the album or artist name; demotes paths with
    penalty keywords (compilations, archives, etc.). Stable sort — equal
    scores preserve original order.
    """
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
                break  # one penalty is enough
        return score

    return sorted(file_dirs, key=_score, reverse=True)


def _browse_one(username: str, file_dir: str) -> tuple[str, Any | None]:
    """Browse a single directory from slskd. Returns (file_dir, result_or_None)."""
    try:
        directory = slskd.users.directory(username=username, directory=file_dir)[0]
        return file_dir, directory
    except Exception:
        logger.exception(f'Error getting directory from user: "{username}"')
        return file_dir, None


def _browse_directories(
    dirs_to_browse: list[str], username: str, max_workers: int = 4
) -> dict[str, Any]:
    """Browse multiple directories in parallel. Returns {file_dir: directory}.

    Failed browses are omitted from the result dict.
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed

    if not dirs_to_browse:
        return {}

    # Single dir — don't bother with thread pool
    if len(dirs_to_browse) == 1:
        file_dir, result = _browse_one(username, dirs_to_browse[0])
        return {file_dir: result} if result is not None else {}

    results: dict[str, Any] = {}
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {
            pool.submit(_browse_one, username, d): d
            for d in dirs_to_browse
        }
        for future in as_completed(futures):
            file_dir, result = future.result()
            if result is not None:
                results[file_dir] = result

    return results


def check_for_match(tracks: Sequence[TrackRecord], allowed_filetype: str, file_dirs: list[str], username: str, ctx: SoularrContext) -> tuple[bool, Any, str]:
    """
    Does the actual match checking on a single disk/album.

    Phase 1: Filter dirs (negative cache, pre-filter by audio count).
    Phase 2: Browse uncached dirs in parallel.
    Phase 3: Match serially (early exit on first match).
    """
    logger.debug(f"Current broken users {ctx.broken_user}")
    if username in ctx.broken_user:
        return False, {}, ""
    track_num = len(tracks)
    album_info = get_album_by_id(tracks[0]["albumId"], ctx)
    ranked_dirs = rank_candidate_dirs(file_dirs, album_info.title, album_info.artist_name)

    # Phase 1: Filter — determine which dirs need browsing
    dirs_to_try: list[str] = []
    for file_dir in ranked_dirs:
        neg_key = (username, file_dir, track_num, allowed_filetype)
        if neg_key in ctx.negative_matches:
            logger.debug(f"Negative cache hit: {username} {file_dir} ({track_num} tracks, {allowed_filetype})")
            continue

        user_counts = ctx.search_dir_audio_count.get(username)
        if user_counts and file_dir in user_counts:
            search_count = user_counts[file_dir]
            if abs(search_count - track_num) > 2:
                logger.debug(
                    f"Pre-filter skip: {username} {file_dir} has {search_count} "
                    f"audio files, need {track_num} tracks"
                )
                ctx.negative_matches.add(neg_key)
                continue

        dirs_to_try.append(file_dir)

    if not dirs_to_try:
        return False, {}, ""

    # Phase 2: Browse uncached dirs in parallel
    if username not in ctx.folder_cache:
        ctx.folder_cache[username] = {}

    uncached = [d for d in dirs_to_try if d not in ctx.folder_cache[username]]
    if uncached:
        logger.info(
            f"Browsing {len(uncached)} dirs from {username} "
            f"(parallelism={cfg.browse_parallelism})"
        )
        browsed = _browse_directories(uncached, username, cfg.browse_parallelism)
        for d, result in browsed.items():
            ctx.folder_cache[username][d] = result

        # If ALL browses failed, mark user as broken
        if not browsed and len(uncached) == len(dirs_to_try):
            ctx.broken_user.append(username)
            logger.debug(f"All browses failed for {username}, marked as broken")
            return False, {}, ""

    # Phase 3: Match serially — early exit on first match
    for file_dir in dirs_to_try:
        if file_dir not in ctx.folder_cache[username]:
            # Browse failed for this dir
            continue

        directory = ctx.folder_cache[username][file_dir]
        tracks_info = album_track_num(directory)
        neg_key = (username, file_dir, track_num, allowed_filetype)

        if tracks_info["count"] == track_num and tracks_info["filetype"] != "":
            if album_match(tracks, directory["files"], username, allowed_filetype, ctx):
                if _track_titles_cross_check(tracks, directory["files"]):
                    return True, copy.deepcopy(directory), file_dir
                else:
                    logger.warning(
                        f"Track title cross-check FAILED for user {username}, "
                        f"dir {file_dir} — skipping (wrong pressing?)"
                    )
        ctx.negative_matches.add(neg_key)
    return False, {}, ""


def is_blacklisted(title: str) -> bool:
    for word in cfg.title_blacklist:
        if word and word.lower() in title.lower():
            logger.info(f"Skipping {title} due to blacklisted word: {word}")
            return True
    return False


def filter_list(albums):
    """
    Helper to do all the various filtering in one go and in one place. Same net effect as the previous multi-stage approach
    Just neater and easier to work on.
    """
    list_to_download = []
    for album in albums:
        if is_blacklisted(album.title):
            logger.info(f"Skipping blacklisted album: {album.artist_name} - {album.title} (ID: {album.id}")
            continue
        else:
            list_to_download.append(album)

    if len(list_to_download) > 0:
        return list_to_download
    else:
        return None


def _build_search_cache(
    search_results: list[Any],
    filter_specs: list[tuple[str, Any]],
) -> tuple[dict[str, dict[str, list[str]]], dict[str, int], dict[str, dict[str, int]]]:
    """Build cache dicts from raw slskd search results.

    Returns (cache_entries, upload_speeds, dir_audio_counts).
    Pure — no I/O, no ctx writes.
    """
    from lib.quality import file_identity, filetype_matches

    cache_entries: dict[str, dict[str, list[str]]] = {}
    upload_speeds: dict[str, int] = {}
    dir_audio_counts: dict[str, dict[str, int]] = {}

    for result in search_results:
        username = result["username"]
        if username not in cache_entries:
            cache_entries[username] = {}
        if username not in dir_audio_counts:
            dir_audio_counts[username] = {}
        user_dir_counts = dir_audio_counts[username]
        speed = result.get("uploadSpeed", 0)
        if speed and (username not in upload_speeds or speed > upload_speeds[username]):
            upload_speeds[username] = speed
        for file in result["files"]:
            file_dir = file["filename"].rsplit("\\", 1)[0]
            identity = file_identity(file)
            matched = False
            for allowed_filetype, spec in filter_specs:
                if filetype_matches(identity, spec):
                    matched = True
                    if allowed_filetype not in cache_entries[username]:
                        cache_entries[username][allowed_filetype] = []
                    if file_dir not in cache_entries[username][allowed_filetype]:
                        cache_entries[username][allowed_filetype].append(file_dir)
            if matched:
                user_dir_counts[file_dir] = user_dir_counts.get(file_dir, 0) + 1

    return cache_entries, upload_speeds, dir_audio_counts


def search_for_album(album, ctx):
    from lib.search import build_query

    album_title = album.title
    artist_name = album.artist_name
    album_id = album.id
    query = build_query(artist_name, album_title, prepend_artist=cfg.album_prepend_artist)

    if not query:
        logger.warning(f"Cannot build search query for '{artist_name} - {album_title}'")
        return False

    logger.info(f"Searching for album: {query} "
                f"(from '{artist_name} - {album_title}')")
    try:
        search = slskd.searches.search_text(
            searchText=query,
            searchTimeout=cfg.search_timeout,
            filterResponses=True,
            maximumPeerQueueLength=cfg.maximum_peer_queue,
            minimumPeerUploadSpeed=cfg.minimum_peer_upload_speed,
        )
    except Exception:
        logger.exception(f"Failed to perform search via SLSKD: {query}")
        return False

    # Wait for slskd to process the search. Searches go through:
    #   Queued -> InProgress -> Completed, (TimedOut|ResponseLimitReached|Errored)
    # We must wait while state is Queued OR InProgress.
    # slskd's searchTimeout is "time since last response", not absolute.
    # Our poll timeout must be longer — let slskd complete on its own.
    time.sleep(5)
    slskd_timeout_s = cfg.search_timeout / 1000 if cfg.search_timeout > 1000 else cfg.search_timeout
    poll_timeout_s = slskd_timeout_s * 2 + 15
    start_time = time.time()
    while True:
        state = slskd.searches.state(search["id"], False)["state"]
        if "Completed" in state or ("InProgress" not in state and "Queued" not in state):
            break
        time.sleep(1)
        if (time.time() - start_time) > poll_timeout_s:
            logger.error("Failed to perform search via SLSKD due to timeout on search results.")
            return False

    search_results = slskd.searches.search_responses(search["id"])
    logger.info(f"Search returned {len(search_results)} results")
    if cfg.delete_searches:
        slskd.searches.delete(search["id"])

    if not len(search_results) > 0:
        return False

    from lib.search import SearchResult
    filter_specs = list(zip(cfg.allowed_filetypes, cfg.allowed_specs))
    cache_entries, upload_speeds, dir_audio_counts = _build_search_cache(
        search_results, filter_specs
    )
    for username in cache_entries:
        logger.info(f"Caching and truncating results for user: {username}")

    # Reuse the same merge path as the parallel pipeline
    _merge_search_result(SearchResult(
        album_id=album_id, success=True,
        cache_entries=cache_entries,
        upload_speeds=upload_speeds,
        dir_audio_counts=dir_audio_counts,
    ), ctx)
    return True


def _submit_search(album, search_cfg, slskd_client):
    """Submit a search to slskd and return the search ID (no waiting).

    slskd has a SemaphoreSlim(1,1) on POST /searches — only one submission
    at a time. The semaphore releases after the search is queued (~100ms),
    so we submit sequentially but wait for results in parallel.

    Returns (search_id, query, album_id) or None on failure.
    """
    from lib.search import build_query
    import requests

    album_title = album.title
    artist_name = album.artist_name
    album_id = album.id
    query = build_query(artist_name, album_title, prepend_artist=search_cfg.album_prepend_artist)

    if not query:
        logger.warning(f"Cannot build search query for '{artist_name} - {album_title}'")
        return None

    logger.info(f"Submitting search: {query} "
                f"(from '{artist_name} - {album_title}')")

    # Retry on 429 (rate limit) or 409 (semaphore busy) with backoff.
    # slskd has SemaphoreSlim(1,1) — 409 means another search is still being submitted.
    for attempt in range(6):
        try:
            search = slskd_client.searches.search_text(
                searchText=query,
                searchTimeout=search_cfg.search_timeout,
                filterResponses=True,
                maximumPeerQueueLength=search_cfg.maximum_peer_queue,
                minimumPeerUploadSpeed=search_cfg.minimum_peer_upload_speed,
            )
            return (search["id"], query, album_id)
        except requests.exceptions.HTTPError as e:
            status = e.response.status_code if e.response is not None else 0
            if status in (429, 409) and attempt < 5:
                wait = min(2 ** attempt, 8)  # 1, 2, 4, 8, 8s
                logger.warning(f"{status} on search submit for {query}, "
                               f"retrying in {wait}s (attempt {attempt + 1}/6)")
                time.sleep(wait)
            else:
                logger.exception(f"Failed to submit search via SLSKD: {query}")
                return None
        except Exception:
            logger.exception(f"Failed to submit search via SLSKD: {query}")
            return None
    return None


def _collect_search_results(search_id, query, album_id, search_cfg, slskd_client):
    """Wait for a submitted search to complete and collect results.

    This is the part that can run in parallel — it's just polling + reading.
    """
    from lib.search import SearchResult

    t0 = time.time()

    # Wait for search to complete. slskd search states:
    #   Queued -> InProgress -> Completed, (TimedOut|ResponseLimitReached|Errored)
    # We must wait while state is Queued OR InProgress.
    # NOTE: slskd's searchTimeout is "time since last response", not absolute.
    # A 30s timeout means slskd waits 30s after the last peer responds. Our
    # poll timeout must be longer — slskd will complete the search on its own.
    time.sleep(5)
    slskd_timeout_s = search_cfg.search_timeout / 1000 if search_cfg.search_timeout > 1000 else search_cfg.search_timeout
    timeout_s = slskd_timeout_s + slskd_timeout_s + 15  # worst case: responses arrive at T=timeout, then wait another timeout
    start_time = time.time()
    while True:
        try:
            state_resp = slskd_client.searches.state(search_id, False)
            state = state_resp["state"]
            if "Completed" in state or ("InProgress" not in state and "Queued" not in state):
                break
        except Exception:
            logger.warning(f"Failed to poll search state for {query}")
            break
        time.sleep(1)
        if (time.time() - start_time) > timeout_s:
            logger.error(f"Search timed out for {query}")
            return SearchResult(album_id=album_id, success=False, query=query,
                                elapsed_s=time.time() - t0)

    search_results = slskd_client.searches.search_responses(search_id)
    elapsed = time.time() - t0
    logger.info(f"Search returned {len(search_results)} results in {elapsed:.1f}s for: {query}")
    if search_cfg.delete_searches:
        slskd_client.searches.delete(search_id)

    if not len(search_results) > 0:
        return SearchResult(album_id=album_id, success=False, query=query,
                            result_count=0, elapsed_s=elapsed)

    filter_specs = list(zip(search_cfg.allowed_filetypes, search_cfg.allowed_specs))
    cache_entries, upload_speeds, dir_audio_counts = _build_search_cache(
        search_results, filter_specs
    )

    return SearchResult(
        album_id=album_id,
        success=True,
        cache_entries=cache_entries,
        upload_speeds=upload_speeds,
        dir_audio_counts=dir_audio_counts,
        query=query,
        result_count=len(search_results),
        elapsed_s=elapsed,
    )


def _execute_search(album, search_cfg, slskd_client):
    """Submit + wait for one search. Used by sequential path."""
    result = _submit_search(album, search_cfg, slskd_client)
    if result is None:
        from lib.search import SearchResult
        return SearchResult(album_id=album.id, success=False,
                            query=album.title)
    search_id, query, album_id = result
    return _collect_search_results(search_id, query, album_id,
                                   search_cfg, slskd_client)


def _merge_search_result(result, ctx):
    """Merge a SearchResult into ctx caches.

    Called only from the main thread — no locking needed.
    """
    album_id = result.album_id
    if album_id not in ctx.search_cache:
        ctx.search_cache[album_id] = {}

    for username, filetypes in result.cache_entries.items():
        if username not in ctx.search_cache[album_id]:
            ctx.search_cache[album_id][username] = {}
        for filetype, dirs in filetypes.items():
            if filetype not in ctx.search_cache[album_id][username]:
                ctx.search_cache[album_id][username][filetype] = []
            for d in dirs:
                if d not in ctx.search_cache[album_id][username][filetype]:
                    ctx.search_cache[album_id][username][filetype].append(d)

    for username, speed in result.upload_speeds.items():
        if username not in ctx.user_upload_speed or speed > ctx.user_upload_speed[username]:
            ctx.user_upload_speed[username] = speed

    for username, dir_counts in result.dir_audio_counts.items():
        if username not in ctx.search_dir_audio_count:
            ctx.search_dir_audio_count[username] = {}
        for d, count in dir_counts.items():
            existing = ctx.search_dir_audio_count[username].get(d, 0)
            ctx.search_dir_audio_count[username][d] = max(existing, count)


def _get_denied_users(album_id):
    """Get denied users from pipeline DB source_denylist."""
    denied = set()
    request_id = abs(album_id)
    try:
        db = pipeline_db_source._get_db()
        denied.update(e["username"] for e in db.get_denylisted_users(request_id))
    except Exception:
        pass
    return denied


def _get_user_dirs(results_for_user: dict[str, list[str]], allowed_filetype: str) -> list[str] | None:
    """Get candidate directories for a user, handling catch-all merging.

    Returns None if the user has no dirs for the requested filetype.
    """
    if allowed_filetype == "*":
        file_dirs: list[str] = []
        for ft_dirs in results_for_user.values():
            file_dirs.extend(d for d in ft_dirs if d not in file_dirs)
        return file_dirs or None
    if allowed_filetype not in results_for_user:
        return None
    return results_for_user[allowed_filetype]


def try_enqueue(all_tracks, results, allowed_filetype, ctx):
    """
    Single album match and enqueue.
    Iterates over all users and enqueues a found match
    """
    album_id = all_tracks[0]["albumId"]
    denied_users = _get_denied_users(album_id)
    # Sort users by upload speed (fastest first) for quicker downloads
    sorted_users = sorted(results.keys(), key=lambda u: ctx.user_upload_speed.get(u, 0), reverse=True)
    for username in sorted_users:
        if username in denied_users:
            logger.info(f"Skipping user '{username}' for album ID {album_id}: denylisted (previously provided mislabeled quality)")
            continue
        file_dirs = _get_user_dirs(results[username], allowed_filetype)
        if file_dirs is None:
            continue
        logger.debug(f"Parsing result from user: {username}")
        found, directory, file_dir = check_for_match(all_tracks, allowed_filetype, file_dirs, username, ctx)
        if found:
            directory = download_filter(allowed_filetype, directory)
            for i in range(0, len(directory["files"])):
                directory["files"][i]["filename"] = file_dir + "\\" + directory["files"][i]["filename"]
            try:
                downloads = slskd_do_enqueue(username=username, files=directory["files"], file_dir=file_dir)
                if downloads is not None:
                    return True, downloads
                else:
                    album = get_album_by_id(all_tracks[0]["albumId"], ctx)
                    album_name = album.title
                    artist_name = album.artist_name
                    logger.info(f"Failed to enqueue download to slskd for {artist_name} - {album_name} from {username}")
            except Exception as e:
                album = get_album_by_id(all_tracks[0]["albumId"], ctx)
                album_name = album.title
                artist_name = album.artist_name

                logger.warning(f"Exception enqueueing tracks: {e}")
                logger.info(f"Exception enqueueing download to slskd for {artist_name} - {album_name} from {username}")
    album = get_album_by_id(all_tracks[0]["albumId"], ctx)
    album_name = album.title
    artist_name = album.artist_name
    logger.info(f"Failed to enqueue {artist_name} - {album_name}")
    return False, None


def try_multi_enqueue(release, all_tracks, results, allowed_filetype, ctx):
    """
    This is the multi-disk/media path for locating and enqueueing an album
    It does a flat search first. Then it does a split search.
    Otherwise it's basically the same as the single album search.
    """
    split_release = []
    for media in release.media:
        disk = {}
        disk["source"] = None
        disk["tracks"] = []
        disk["disk_no"] = media.medium_number
        disk["disk_count"] = len(release.media)
        for track in all_tracks:
            if track["mediumNumber"] == media.medium_number:
                disk["tracks"].append(track)
        split_release.append(disk)
    total = len(split_release)
    count_found = 0
    album_id = all_tracks[0]["albumId"]
    denied_users = _get_denied_users(album_id)
    for disk in split_release:
        ctx.negative_matches.clear()  # each disc has different expected titles
        for username in results:
            if username in denied_users:
                logger.info(f"Skipping user '{username}' for album ID {album_id} (multi-disc): denylisted (previously provided mislabeled quality)")
                continue
            file_dirs = _get_user_dirs(results[username], allowed_filetype)
            if file_dirs is None:
                continue
            found, directory, file_dir = check_for_match(disk["tracks"], allowed_filetype, file_dirs, username, ctx)
            if found:
                directory = download_filter(allowed_filetype, directory)
                disk["source"] = (username, directory, file_dir)
                count_found += 1
                break
        else:
            return (
                False,
                None,
            )  # Only runs if we complete the loop without finding a source for the current disk regardless of how many other disks we located. All or nothing.
    if count_found == total:
        all_downloads = []
        enqueued = 0
        for disk in split_release:
            username, directory, file_dir = disk["source"]
            for i in range(0, len(directory["files"])):
                directory["files"][i]["filename"] = file_dir + "\\" + directory["files"][i]["filename"]
            try:
                downloads = slskd_do_enqueue(username=username, files=directory["files"], file_dir=file_dir)
                if downloads is not None:
                    for file in downloads:
                        file.disk_no = disk["disk_no"]
                        file.disk_count = disk["disk_count"]
                    all_downloads.extend(downloads)
                    enqueued += 1
                else:
                    album = get_album_by_id(all_tracks[0]["albumId"], ctx)
                    album_name = album.title
                    artist_name = album.artist_name
                    logger.info(f"Failed to enqueue download to slskd for {artist_name} - {album_name} from {username}")
                    if len(all_downloads) > 0:
                        cancel_and_delete(all_downloads)
                        return False, None
            except Exception:
                album = get_album_by_id(all_tracks[0]["albumId"], ctx)
                album_name = album.title
                artist_name = album.artist_name

                logger.exception("Exception enqueueing tracks")
                logger.info(f"Exception enqueueing download to slskd for {artist_name} - {album_name} from {username}")
                if len(all_downloads) > 0:
                    cancel_and_delete(all_downloads)
                    return False, None
        if enqueued == total:
            return True, all_downloads
        else:
            # Delete all other downloads
            if len(all_downloads) > 0:
                cancel_and_delete(all_downloads)
            return False, None

    else:
        return False, None


def get_album_tracks(album, release_id=None):
    """Get tracks for an album from pipeline DB."""
    return pipeline_db_source.get_tracks(album)


def get_album_by_id(album_id, ctx):
    """Get album data by ID from context cache."""
    if album_id in ctx.current_album_cache:
        return ctx.current_album_cache[album_id]
    raise KeyError(f"Album {album_id} not found in cache")


def _try_filetype(album, results, allowed_filetype, grab_list, ctx) -> bool:
    """Try to match and enqueue an album at a specific filetype quality.

    Iterates releases (monitored first), tries single-album then multi-disc.
    Returns True and populates grab_list on success.
    """
    album_id = album.id
    artist_name = album.artist_name
    releases = list(album.releases)
    has_monitored = any(r.monitored for r in releases)

    for _ in range(len(releases)):
        if not releases:
            break
        release = choose_release(artist_name, releases)
        releases.remove(release)
        all_tracks = get_album_tracks(album, release_id=release.id)
        if not all_tracks:
            logger.warning(f"No tracks for {artist_name} - {album.title} (release {release.id}) — skipping")
            continue

        found, downloads = try_enqueue(all_tracks, results, allowed_filetype, ctx)
        if not found and len(release.media) > 1:
            found, downloads = try_multi_enqueue(release, all_tracks, results, allowed_filetype, ctx)

        if found:
            assert downloads is not None
            grab_list[album_id] = GrabListEntry(
                album_id=album_id,
                files=downloads,
                filetype=allowed_filetype,
                title=album.title,
                artist=artist_name,
                year=album.release_date[0:4],
                mb_release_id=release.foreign_release_id,
                db_request_id=album.db_request_id,
                db_source=album.db_source,
                db_quality_override=album.db_quality_override,
            )
            return True

        if has_monitored and release.monitored:
            logger.info(
                f"Monitored release ({release.track_count} tracks) not found on "
                f"Soulseek for {artist_name} - {album.title} at quality "
                f"{allowed_filetype}, skipping non-monitored releases"
            )
            break
        if has_monitored and not release.monitored:
            break

    return False


def find_download(album, grab_list, ctx):
    """
    This does the main loop over search results and user directories
    It has two paths it can take. One is the "single album" path
    The other is the multi-media path.
    """
    album_id = album.id
    artist_name = album.artist_name
    results = ctx.search_cache[album_id]

    # Clear negative match cache per-album — same dir could match a different album
    ctx.negative_matches.clear()

    # Cache album so get_album_by_id() works during matching
    ctx.current_album_cache[album_id] = album

    from lib.quality import derive_intent, search_filetypes, intent_allows_catch_all

    intent = derive_intent(album.db_quality_override)
    filetypes_to_try = search_filetypes(intent, list(cfg.allowed_filetypes))

    if album.db_quality_override:
        logger.info(
            f"Quality override for {artist_name} - {album.title}: "
            f"intent={intent.value}, searching {filetypes_to_try}"
        )

    for allowed_filetype in filetypes_to_try:
        logger.info(f"Checking for Quality: {allowed_filetype}")
        if _try_filetype(album, results, allowed_filetype, grab_list, ctx):
            return True

    # Catch-all fallback: only best_effort allows falling back to any format.
    if intent_allows_catch_all(intent) and "*" not in [ft.strip() for ft in (cfg.allowed_filetypes or ())]:
        logger.info(
            f"No match at preferred quality for {artist_name} - {album.title}, "
            f"trying catch-all (any audio format)"
        )
        if _try_filetype(album, results, "*", grab_list, ctx):
            return True

    return False


def search_and_queue(albums, ctx):
    if cfg.parallel_searches > 1 and len(albums) > 1:
        return _search_and_queue_parallel(albums, ctx)
    grab_list = {}
    failed_grab = []
    failed_search = []
    total = len(albums)
    for i, album in enumerate(albums, 1):
        logger.info(f"Album {i}/{total}: {album.artist_name} - {album.title}")
        if search_for_album(album, ctx):
            if not find_download(album, grab_list, ctx):
                failed_grab.append(album)

        else:
            failed_search.append(album)
    return grab_list, failed_search, failed_grab


def _search_and_queue_parallel(albums, ctx):
    """Pipeline searches with result processing: always 2 searches in flight.

    slskd constraints (from source code):
    - SemaphoreSlim(1,1) on POST /searches: one submission at a time
    - maximumConcurrentSearches=2 in Soulseek.NET: only 2 active on network

    We keep 2 searches running on the network at all times. When one completes,
    we process its results (browse dirs, match tracks, enqueue) AND submit the
    next search — so network wait and result processing overlap.

    Timeline:
      search_1 ─────────> process_1    search_5 ──────> process_5 ...
      search_2 ─────────> process_2    search_6 ──────> ...
        search_3 ─────────> process_3
        search_4 ─────────> process_4
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed

    MAX_INFLIGHT = 2  # matches slskd maximumConcurrentSearches

    grab_list: dict[Any, Any] = {}
    failed_grab: list[Any] = []
    failed_search: list[Any] = []
    total = len(albums)
    album_queue = list(albums)  # mutable copy we pop from

    logger.info(f"Pipelined search: {total} albums, {MAX_INFLIGHT} in flight")
    wall_start = time.time()

    def _submit_next() -> tuple[Any, Any] | None:
        """Submit the next album from the queue. Returns (future, album) or None."""
        while album_queue:
            album = album_queue.pop(0)
            result = _submit_search(album, cfg, slskd)
            if result is None:
                failed_search.append(album)
                continue
            search_id, query, album_id = result
            future = pool.submit(
                _collect_search_results, search_id, query, album_id, cfg, slskd
            )
            return (future, album)
        return None

    with ThreadPoolExecutor(max_workers=MAX_INFLIGHT) as pool:
        # Seed the pipeline with initial searches
        inflight: dict[Any, Any] = {}
        for _ in range(min(MAX_INFLIGHT, len(album_queue))):
            submitted = _submit_next()
            if submitted:
                future, album = submitted
                inflight[future] = album

        # Process completions and refill the pipeline
        while inflight:
            for future in as_completed(inflight):
                album = inflight.pop(future)
                try:
                    result = future.result()
                except Exception:
                    logger.exception(f"Search collection crashed for {album.title}")
                    failed_search.append(album)
                else:
                    done_count = len(grab_list) + len(failed_grab) + len(failed_search)
                    logger.info(
                        f"Search {done_count + 1}/{total} done: {result.query} "
                        f"({result.result_count} results, {result.elapsed_s:.1f}s)"
                    )
                    if result.success:
                        _merge_search_result(result, ctx)
                        if not find_download(album, grab_list, ctx):
                            failed_grab.append(album)
                    else:
                        failed_search.append(album)

                # Refill: submit next search to keep pipeline full
                submitted = _submit_next()
                if submitted:
                    new_future, new_album = submitted
                    inflight[new_future] = new_album

                # Break out of the as_completed loop to re-enter with updated dict
                break

    wall_elapsed = time.time() - wall_start
    logger.info(f"Pipelined search complete: {total} albums in {wall_elapsed:.1f}s "
                f"(found={len(grab_list)}, no_match={len(failed_grab)}, "
                f"no_results={len(failed_search)})")

    return grab_list, failed_search, failed_grab


from lib.grab_list import GrabListEntry

from lib.download import (cancel_and_delete as _cancel_and_delete_impl,
                          slskd_do_enqueue as _slskd_do_enqueue_impl,
                          grab_most_wanted as _grab_most_wanted_impl)


def _make_ctx():
    """Return the module-level SoularrContext (created in main())."""
    return _module_ctx


def cancel_and_delete(files):
    _cancel_and_delete_impl(files, _make_ctx())


def slskd_do_enqueue(username, files, file_dir):
    return _slskd_do_enqueue_impl(username, files, file_dir, _make_ctx())


def grab_most_wanted(albums):
    return _grab_most_wanted_impl(albums, lambda albs: search_and_queue(albs, _module_ctx), _module_ctx)


from lib.util import (_track_titles_cross_check,
                      is_docker, setup_logging)


def main():
    global \
        cfg, \
        slskd, \
        config, \
        pipeline_db_source, \
        _module_ctx

    # Let's allow some overrides to be passed to the script
    parser = argparse.ArgumentParser(description="""Soularr downloads wanted albums from Soulseek via slskd""")

    default_data_directory = os.getcwd()

    if is_docker():
        default_data_directory = "/data"

    parser.add_argument(
        "-c",
        "--config-dir",
        default=default_data_directory,
        const=default_data_directory,
        nargs="?",
        type=str,
        help="Config directory (default: %(default)s)",
    )

    parser.add_argument(
        "-v",
        "--var-dir",
        default=default_data_directory,
        const=default_data_directory,
        nargs="?",
        type=str,
        help="Var directory (default: %(default)s)",
    )

    parser.add_argument(
        "--no-lock-file",
        action="store_false",
        dest="lock_file",
        default=True,
        help="Disable lock file creation",
    )

    args = parser.parse_args()

    lock_file_path = os.path.join(args.var_dir, ".soularr.lock")
    config_file_path = os.path.join(args.config_dir, "config.ini")

    if not is_docker() and os.path.exists(lock_file_path) and args.lock_file:
        logger.info(f"Soularr instance is already running.")
        sys.exit(1)

    try:
        if not is_docker() and args.lock_file:
            with open(lock_file_path, "w") as lock_file:
                lock_file.write("locked")

        # Disable interpolation to make storing logging formats in the config file much easier
        config = configparser.ConfigParser(interpolation=EnvInterpolation())

        if os.path.exists(config_file_path):
            config.read(config_file_path)
        else:
            if is_docker():
                logger.error(
                    'Config file does not exist! Please mount "/data" and place your "config.ini" file there. Alternatively, pass `--config-dir /directory/of/your/liking` as post arguments to store the config somewhere else.'
                )
                logger.error("See: https://github.com/mrusse/soularr/blob/main/config.ini for an example config file.")
            else:
                logger.error(
                    "Config file does not exist! Please place it in the working directory. Alternatively, pass `--config-dir /directory/of/your/liking` as post arguments to store the config somewhere else."
                )
                logger.error("See: https://github.com/mrusse/soularr/blob/main/config.ini for an example config file.")
            if os.path.exists(lock_file_path) and not is_docker():
                os.remove(lock_file_path)
            sys.exit(0)

        # --- Parse config into typed dataclass ---
        from lib.config import SoularrConfig
        cfg = SoularrConfig.from_ini(config, config_dir=args.config_dir, var_dir=args.var_dir)

        setup_logging(config)

        if cfg.beets_validation_enabled:
            logger.info(f"Beets validation ENABLED: harness={cfg.beets_harness_path}, "
                        f"threshold={cfg.beets_distance_threshold}, staging={cfg.beets_staging_dir}")

        from album_source import DatabaseSource
        pipeline_db_source = DatabaseSource(cfg.pipeline_db_dsn)
        logger.info(f"Pipeline DB: {cfg.pipeline_db_dsn}")

        if cfg.meelo_url:
            logger.info(f"Meelo post-import scan ENABLED: {cfg.meelo_url}")

        slskd = slskd_api.SlskdClient(host=cfg.slskd_host_url, api_key=cfg.slskd_api_key, url_base=cfg.slskd_url_base)

        # Build context with fresh caches for this cycle
        from lib.context import SoularrContext
        _module_ctx = SoularrContext(cfg=cfg, slskd=slskd, pipeline_db_source=pipeline_db_source)

        # --- Phase 1: Poll active downloads from previous runs ---
        from lib.download import poll_active_downloads as _poll_impl
        logger.info("Polling active downloads...")
        try:
            _poll_impl(_module_ctx)
        except Exception:
            logger.exception("Error polling active downloads — continuing to search phase")

        # --- Phase 2: Search and enqueue new downloads ---
        logger.info("Getting wanted records from pipeline DB...")
        wanted_records = pipeline_db_source.get_wanted(limit=cfg.page_size)
        logger.info(f"Pipeline DB: {len(wanted_records)} wanted record(s)")

        if len(wanted_records) > 0:
            try:
                filtered = filter_list(wanted_records)
                if filtered is not None:
                    failed = grab_most_wanted(filtered)
                else:
                    failed = 0
                    logger.info("No releases wanted that aren't on the deny list and/or blacklisted")
            except Exception:
                logger.exception("Fatal error! Exiting...")

                if os.path.exists(lock_file_path) and not is_docker():
                    os.remove(lock_file_path)
                sys.exit(0)
            if failed == 0:
                logger.info("Soularr finished. Exiting...")
            else:
                logger.info(f"{failed}: releases failed to find a match in the search results and are still wanted.")
        else:
            logger.info("No releases wanted. Exiting...")

        # Clean up completed transfer UI entries
        slskd.transfers.remove_completed_downloads()

    finally:
        # Bust web UI cache so freshly imported albums appear immediately
        try:
            import urllib.request
            req = urllib.request.Request(
                "http://localhost:8085/api/cache/invalidate",
                data=b'{"groups": ["pipeline", "library"]}',
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            urllib.request.urlopen(req, timeout=2)
        except Exception:
            pass  # web UI may be down — that's fine

        # Clean up pipeline DB connection
        if pipeline_db_source is not None:
            try:
                pipeline_db_source.close()
            except Exception:
                pass
        # Remove the lock file after activity is done
        if os.path.exists(lock_file_path) and not is_docker():
            os.remove(lock_file_path)


if __name__ == "__main__":
    main()
