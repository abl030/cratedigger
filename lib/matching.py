"""Matching helpers extracted from cratedigger.py."""

from __future__ import annotations

import difflib
import logging
import time
from typing import Any, Sequence, TYPE_CHECKING

from lib.browse import _browse_directories, rank_candidate_dirs
from lib.util import _track_titles_cross_check

if TYPE_CHECKING:
    from lib.config import CratediggerConfig
    from lib.context import CratediggerContext
    from cratedigger import SlskdDirectory, SlskdFile, TrackRecord


logger = logging.getLogger("cratedigger")


def get_album_by_id(album_id: int, ctx: CratediggerContext) -> Any:
    """Get album data by ID from the context cache."""
    if album_id in ctx.current_album_cache:
        return ctx.current_album_cache[album_id]
    raise KeyError(f"Album {album_id} not found in cache")


def album_match(
    expected_tracks: Sequence[TrackRecord],
    slskd_tracks: Sequence[SlskdFile],
    username: str,
    filetype: str,
    ctx: CratediggerContext,
) -> bool:
    """Check whether the browsed directory matches the expected album."""
    match_cfg = ctx.cfg
    counted = []
    total_match = 0.0

    album_info = get_album_by_id(expected_tracks[0]["albumId"], ctx)
    album_name = album_info.title

    from lib.quality import parse_filetype_config

    spec = parse_filetype_config(filetype)
    is_catch_all = spec.extension == "*"
    for expected_track in expected_tracks:
        best_match = 0.0
        expected_filename = expected_track["title"]

        for slskd_track in slskd_tracks:
            if is_catch_all:
                slskd_ext = (
                    slskd_track["filename"].rsplit(".", 1)[-1].lower()
                    if "." in slskd_track["filename"]
                    else ""
                )
                expected_filename = expected_track["title"] + "." + slskd_ext
            else:
                expected_filename = expected_track["title"] + "." + spec.extension
            slskd_filename = slskd_track["filename"]

            ratio = difflib.SequenceMatcher(
                None, expected_filename, slskd_filename
            ).ratio()
            ratio = check_ratio(
                " ", ratio, expected_filename, slskd_filename,
                match_cfg.minimum_match_ratio,
            )
            ratio = check_ratio(
                "_", ratio, expected_filename, slskd_filename,
                match_cfg.minimum_match_ratio,
            )
            ratio = check_ratio(
                "", ratio, album_name + " " + expected_filename,
                slskd_filename, match_cfg.minimum_match_ratio,
            )
            ratio = check_ratio(
                " ", ratio, album_name + " " + expected_filename,
                slskd_filename, match_cfg.minimum_match_ratio,
            )
            ratio = check_ratio(
                "_", ratio, album_name + " " + expected_filename,
                slskd_filename, match_cfg.minimum_match_ratio,
            )

            if ratio > best_match:
                best_match = ratio

        if best_match > match_cfg.minimum_match_ratio:
            counted.append(expected_filename)
            total_match += best_match

    if len(counted) == len(expected_tracks) and username not in match_cfg.ignored_users:
        logger.info(
            f"Found match from user: {username} for {len(counted)} tracks! "
            f"Track attributes: {filetype}"
        )
        logger.info(f"Average sequence match ratio: {total_match / len(counted)}")
        logger.info("SUCCESSFUL MATCH")
        logger.info("-------------------")
        return True

    return False


def check_ratio(
    separator: str,
    ratio: float,
    expected_filename: str,
    slskd_filename: str,
    minimum_match_ratio: float,
) -> float:
    """Retry a weak filename match with trimmed prefixes."""
    if ratio < minimum_match_ratio:
        if separator != "":
            expected_filename_word_count = len(expected_filename.split()) * -1
            truncated_slskd_filename = " ".join(
                slskd_filename.split(separator)[expected_filename_word_count:]
            )
            ratio = difflib.SequenceMatcher(
                None, expected_filename, truncated_slskd_filename
            ).ratio()
        else:
            ratio = difflib.SequenceMatcher(
                None, expected_filename, slskd_filename
            ).ratio()

    return ratio


def album_track_num(
    directory: SlskdDirectory,
    match_cfg: CratediggerConfig,
) -> dict[str, Any]:
    """Count matching audio tracks and infer a consistent filetype."""
    from lib.quality import AUDIO_EXTENSIONS as _all_audio_exts

    files = directory["files"]
    specs = match_cfg.allowed_specs
    has_catch_all = any(s.extension == "*" for s in specs)
    allowed_exts = (
        list(_all_audio_exts)
        if has_catch_all
        else [s.extension for s in specs]
    )
    count = 0
    index = -1
    filetype = ""
    for file in files:
        ext = file["filename"].split(".")[-1].lower()
        if ext in allowed_exts:
            if has_catch_all:
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

    return {"count": count, "filetype": filetype}


def check_for_match(
    tracks: Sequence[TrackRecord],
    allowed_filetype: str,
    file_dirs: list[str],
    username: str,
    ctx: CratediggerContext,
) -> tuple[bool, Any, str]:
    """Check candidate directories for an album match."""
    logger.debug(f"Current broken users {ctx.broken_user}")
    if username in ctx.broken_user:
        return False, {}, ""
    track_num = len(tracks)
    album_info = get_album_by_id(tracks[0]["albumId"], ctx)
    ranked_dirs = rank_candidate_dirs(file_dirs, album_info.title, album_info.artist_name)

    dirs_to_try: list[str] = []
    for file_dir in ranked_dirs:
        neg_key = (username, file_dir, track_num, allowed_filetype)
        if neg_key in ctx.negative_matches:
            logger.debug(
                f"Negative cache hit: {username} {file_dir} "
                f"({track_num} tracks, {allowed_filetype})"
            )
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

    if username not in ctx.folder_cache:
        ctx.folder_cache[username] = {}

    uncached = [d for d in dirs_to_try if d not in ctx.folder_cache[username]]
    if uncached:
        logger.info(
            f"Browsing {len(uncached)} dirs from {username} "
            f"(parallelism={ctx.cfg.browse_parallelism})"
        )
        browsed = _browse_directories(
            uncached,
            username,
            ctx.slskd,
            ctx.cfg.browse_parallelism,
        )
        for d, result in browsed.items():
            ctx.folder_cache[username][d] = result
            ctx._folder_cache_ts.setdefault(username, {})[d] = time.time()

        if not browsed and len(uncached) == len(dirs_to_try):
            ctx.broken_user.append(username)
            logger.debug(f"All browses failed for {username}, marked as broken")
            return False, {}, ""

    for file_dir in dirs_to_try:
        if file_dir not in ctx.folder_cache[username]:
            continue

        directory = ctx.folder_cache[username][file_dir]
        tracks_info = album_track_num(directory, ctx.cfg)
        neg_key = (username, file_dir, track_num, allowed_filetype)

        if tracks_info["count"] == track_num and tracks_info["filetype"] != "":
            if album_match(tracks, directory["files"], username, allowed_filetype, ctx):
                if _track_titles_cross_check(tracks, directory["files"]):
                    return True, directory, file_dir
                logger.warning(
                    f"Track title cross-check FAILED for user {username}, "
                    f"dir {file_dir} — skipping (wrong pressing?)"
                )
        ctx.negative_matches.add(neg_key)
    return False, {}, ""
