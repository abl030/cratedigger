#!/usr/bin/env python
from __future__ import annotations

import argparse
import os
import sys
import time
import difflib
import configparser
import logging
from typing import Any, TYPE_CHECKING
import copy
import slskd_api

if TYPE_CHECKING:
    from album_source import DatabaseSource
    from lib.config import SoularrConfig


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

# === Runtime State & Caches ===
search_cache = {}
folder_cache = {}
user_upload_speed = {}  # username → upload speed in bytes/sec (from search results)
broken_user = []


def album_match(expected_tracks, slskd_tracks, username, filetype):
    counted = []
    total_match = 0.0

    album_info = get_album_by_id(expected_tracks[0]["albumId"])
    album_name = album_info["title"]
    artist_name = album_info["artist"]["artistName"]

    for expected_track in expected_tracks:
        expected_filename = expected_track["title"] + "." + filetype.split(" ")[0]
        best_match = 0.0

        for slskd_track in slskd_tracks:
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


def album_track_num(directory):
    files = directory["files"]
    allowed_filetypes_no_attributes = [item.split(" ")[0] for item in cfg.allowed_filetypes]
    count = 0
    index = -1
    filetype = ""
    for file in files:
        if file["filename"].split(".")[-1] in allowed_filetypes_no_attributes:
            new_index = allowed_filetypes_no_attributes.index(file["filename"].split(".")[-1])

            if index == -1:
                index = new_index
                filetype = allowed_filetypes_no_attributes[index]
            elif new_index != index:
                filetype = ""
                break

            count += 1

    return_data = {"count": count, "filetype": filetype}
    return return_data


def release_trackcount_mode(releases):
    track_count = {}

    for release in releases:
        trackcount = release["trackCount"]
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
        if not release.get("monitored", False):
            continue
        country = release["country"][0] if release["country"] else None
        if release["format"][1] == "x" and cfg.allow_multi_disc:
            format_accepted = release["format"].split("x", 1)[1] in cfg.accepted_formats
        else:
            format_accepted = release["format"] in cfg.accepted_formats
        if format_accepted:
            logger.info(
                f"Selected monitored release for {artist_name}: {release['status']}, "
                f"{country}, {release['format']}, Mediums: {release['mediumCount']}, "
                f"Tracks: {release['trackCount']}, ID: {release['id']}"
            )
            return release

    for release in releases:
        country = release["country"][0] if release["country"] else None

        if release["format"][1] == "x" and cfg.allow_multi_disc:
            format_accepted = release["format"].split("x", 1)[1] in cfg.accepted_formats
        else:
            format_accepted = release["format"] in cfg.accepted_formats

        if cfg.use_most_common_tracknum:
            if release["trackCount"] == most_common_trackcount:
                track_count_bool = True
            else:
                track_count_bool = False
        else:
            track_count_bool = True

        if (cfg.skip_region_check or country in cfg.accepted_countries) and format_accepted and release["status"] == "Official" and track_count_bool:
            logger.info(
                ", ".join(
                    [
                        f"Selected release for {artist_name}: {release['status']}",
                        str(country),
                        release["format"],
                        f"Mediums: {release['mediumCount']}",
                        f"Tracks: {release['trackCount']}",
                        f"ID: {release['id']}",
                    ]
                )
            )

            return release

    if cfg.use_most_common_tracknum:
        for release in releases:
            if release["trackCount"] == most_common_trackcount:
                return release
        else:
            default_release = releases[0]

    else:
        default_release = releases[0]

    return default_release


def verify_filetype(file, allowed_filetype):
    current_filetype = file["filename"].split(".")[-1]
    bitdepth = None
    samplerate = None
    bitrate = None

    if "bitRate" in file:
        bitrate = file["bitRate"]
    if "sampleRate" in file:
        samplerate = file["sampleRate"]
    if "bitDepth" in file:
        bitdepth = file["bitDepth"]

    # Check if the types match up for the current files type and the current type from the config
    if current_filetype == allowed_filetype.split(" ")[0]:
        # Check if the current type from the config specifies other attributes than the filetype (bitrate etc)
        if " " in allowed_filetype:
            selected_attributes = allowed_filetype.split(" ")[1]
            # If it is a bitdepth/samplerate pair instead of a simple bitrate
            if "/" in selected_attributes:
                selected_bitdepth = selected_attributes.split("/")[0]
                try:
                    selected_samplerate = str(int(float(selected_attributes.split("/")[1]) * 1000))
                except (ValueError, IndexError):
                    logger.warning("Invalid samplerate in selected_attributes")
                    return False

                if bitdepth and samplerate:
                    if str(bitdepth) == str(selected_bitdepth) and str(samplerate) == str(selected_samplerate):
                        return True
                else:
                    return False
            # If it is a VBR quality preset (e.g. "mp3 v0", "mp3 v2")
            elif selected_attributes.lower() in ("v0", "v2"):
                if bitrate:
                    cbr_values = {128, 160, 192, 224, 256, 320}
                    is_vbr = bitrate not in cbr_values
                    # Prefer isVariableBitRate flag from slskd if available
                    if "isVariableBitRate" in file:
                        is_vbr = file["isVariableBitRate"]
                    if not is_vbr:
                        return False
                    if selected_attributes.lower() == "v0":
                        return 220 <= bitrate <= 280
                    else:  # v2
                        return 170 <= bitrate <= 220
                return False
            # If it is a minimum bitrate (e.g. "aac 256+", "ogg 256+", "opus 192+")
            elif selected_attributes.endswith("+"):
                try:
                    min_bitrate = int(selected_attributes[:-1])
                except ValueError:
                    logger.warning(f"Invalid minimum bitrate in allowed_filetype: {allowed_filetype}")
                    return False
                if bitrate:
                    return bitrate >= min_bitrate
                return False
            # If it is an exact bitrate
            else:
                selected_bitrate = selected_attributes
                if bitrate:
                    if str(bitrate) == str(selected_bitrate):
                        return True
                return False
        # If no bitrate or other info then it is a match so return true
        else:
            return True
    else:
        return False


def download_filter(allowed_filetype, directory: Any):
    """
    Filters the directory listing from SLSKD using the filetype whitelist.
    If not using the whitelist it will only return the audio files of the allowed filetype.
    This is to prevent downloading m3u,cue,txt,jpg,etc. files that are sometimes stored in
    the same folders as the music files.
    """
    logging.debug("download_filtering")
    if cfg.download_filtering:
        whitelist = []  # Init an empty list to take just the allowed_filetype
        if cfg.use_extension_whitelist:
            whitelist = list(cfg.extensions_whitelist)
        whitelist.append(allowed_filetype.split(" ")[0])
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
                    temp.append(file)  # Build the new list of files
            directory["files"] = temp
            for files in temp:
                logger.debug(f"File in final list: {files['filename']}")
            return directory  # Return the modified list
    return directory  # If we didn't find unwanted files or we aren't filtering just return the original list


def check_for_match(tracks, allowed_filetype, file_dirs, username):
    """
    Does the actual match checking on a single disk/album.
    """
    logger.debug(f"Current broken users {broken_user}")
    if username in broken_user:
        return False, {}, ""
    for file_dir in file_dirs:
        if username not in folder_cache:
            logger.debug(f"Add user to cache: {username}")
            folder_cache[username] = {}

        if file_dir not in folder_cache[username]:
            logger.info(f"User: {username} Folder: {file_dir} not in cache. Fetching from SLSKD")
            version = slskd.application.version()
            version_check = slskd_version_check(version)

            if not version_check:
                logger.info(f"Error checking slskd version number: {version}. Version check > 0.22.2: {version_check}. This would most likely be fixed by updating your slskd.")

            try:
                directory: Any
                if version_check:
                    directory = slskd.users.directory(username=username, directory=file_dir)[0]
                else:
                    directory = slskd.users.directory(username=username, directory=file_dir)
            except Exception:
                logger.exception(f'Error getting directory from user: "{username}"')
                broken_user.append(username)
                logger.debug(f"Updated broken users {broken_user}")
                return False, {}, ""
            folder_cache[username][file_dir] = copy.deepcopy(directory)
        else:
            logger.info(f"User: {username} Folder: {file_dir} in cache. Using cached value")
            directory = copy.deepcopy(folder_cache[username][file_dir])

        track_num = len(tracks)
        tracks_info = album_track_num(directory)

        if tracks_info["count"] == track_num and tracks_info["filetype"] != "":
            if album_match(tracks, directory["files"], username, allowed_filetype):
                if _track_titles_cross_check(tracks, directory["files"]):
                    return True, directory, file_dir
                else:
                    logger.warning(
                        f"Track title cross-check FAILED for user {username}, "
                        f"dir {file_dir} — skipping (wrong pressing?)"
                    )
                    continue
            else:
                continue
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
        if is_blacklisted(album["title"]):
            logger.info(f"Skipping blacklisted album: {album['artist']['artistName']} - {album['title']} (ID: {album['id']}")
            continue
        else:
            list_to_download.append(album)

    if len(list_to_download) > 0:
        return list_to_download
    else:
        return None


def search_for_album(album):
    from lib.search import build_query

    album_title = album["title"]
    artist_name = album["artist"]["artistName"]
    album_id = album["id"]
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

    # Add timeout here to increase reliability with Slskd. Sometimes it doesn't update search status fast enough. More of an issue with lots of historical searches in slskd
    time.sleep(5)
    start_time = time.time()
    while True:
        if slskd.searches.state(search["id"], False)["state"] != "InProgress":  # Added False here as we don't want the search results here. Just the state.
            break
        time.sleep(1)
        if (time.time() - start_time) > cfg.search_timeout:
            logger.error("Failed to perform search via SLSKD due to timeout on search results.")
            return False

    search_results = slskd.searches.search_responses(search["id"])  # We use this API call twice. Let's just cache it locally.
    logger.info(f"Search returned {len(search_results)} results")
    if cfg.delete_searches:
        slskd.searches.delete(search["id"])

    if not len(search_results) > 0:
        return False

    if album_id not in search_cache:
        search_cache[album_id] = {}  # This is so we can check for matches we missed or if a user goes offline during our download

    for result in search_results:  # Switching to cached version. One less API call
        username = result["username"]
        if username not in search_cache[album_id]:
            # If we don't currently have a cache for a user set one up
            search_cache[album_id][username] = {}
        # Cache upload speed for sorting — prefer faster peers
        speed = result.get("uploadSpeed", 0)
        if speed and (username not in user_upload_speed or speed > user_upload_speed[username]):
            user_upload_speed[username] = speed
        logger.info(f"Caching and truncating results for user: {username}")
        init_files = result["files"]  # init_files short for initial files. Before truncating
        # Search the returned files and only cache files that are of the allowed_filetypes
        for file in init_files:
            file_dir = file["filename"].rsplit("\\", 1)[0]  # split dir/filenames on \
            for allowed_filetype in cfg.allowed_filetypes:
                if verify_filetype(file, allowed_filetype):  # Check the filename for an allowed type
                    if allowed_filetype not in search_cache[album_id][username]:
                        search_cache[album_id][username][allowed_filetype] = []  # Init the cache for this allowed filetype
                    if file_dir not in search_cache[album_id][username][allowed_filetype]:
                        search_cache[album_id][username][allowed_filetype].append(file_dir)
    return True


def _execute_search(album, search_cfg, slskd_client):
    """Thread-safe search: returns SearchResult instead of writing to globals.

    Takes explicit cfg/slskd args (no module global access) so it's safe
    to call from ThreadPoolExecutor.
    """
    from lib.search import build_query, SearchResult

    album_title = album["title"]
    artist_name = album["artist"]["artistName"]
    album_id = album["id"]
    query = build_query(artist_name, album_title, prepend_artist=search_cfg.album_prepend_artist)

    if not query:
        logger.warning(f"Cannot build search query for '{artist_name} - {album_title}'")
        return SearchResult(album_id=album_id, success=False, query=query or "")

    logger.info(f"Searching for album: {query} "
                f"(from '{artist_name} - {album_title}')")
    t0 = time.time()
    try:
        search = slskd_client.searches.search_text(
            searchText=query,
            searchTimeout=search_cfg.search_timeout,
            filterResponses=True,
            maximumPeerQueueLength=search_cfg.maximum_peer_queue,
            minimumPeerUploadSpeed=search_cfg.minimum_peer_upload_speed,
        )
    except Exception:
        logger.exception(f"Failed to perform search via SLSKD: {query}")
        return SearchResult(album_id=album_id, success=False, query=query,
                            elapsed_s=time.time() - t0)

    time.sleep(5)
    start_time = time.time()
    while True:
        if slskd_client.searches.state(search["id"], False)["state"] != "InProgress":
            break
        time.sleep(1)
        if (time.time() - start_time) > search_cfg.search_timeout:
            logger.error("Failed to perform search via SLSKD due to timeout on search results.")
            return SearchResult(album_id=album_id, success=False, query=query,
                                elapsed_s=time.time() - t0)

    search_results = slskd_client.searches.search_responses(search["id"])
    elapsed = time.time() - t0
    logger.info(f"Search returned {len(search_results)} results in {elapsed:.1f}s")
    if search_cfg.delete_searches:
        slskd_client.searches.delete(search["id"])

    if not len(search_results) > 0:
        return SearchResult(album_id=album_id, success=False, query=query,
                            result_count=0, elapsed_s=elapsed)

    # Build cache entries and upload speeds without touching globals
    cache_entries: dict[str, dict[str, list[str]]] = {}
    upload_speeds: dict[str, int] = {}

    for result in search_results:
        username = result["username"]
        if username not in cache_entries:
            cache_entries[username] = {}
        speed = result.get("uploadSpeed", 0)
        if speed and (username not in upload_speeds or speed > upload_speeds[username]):
            upload_speeds[username] = speed
        for file in result["files"]:
            file_dir = file["filename"].rsplit("\\", 1)[0]
            for allowed_filetype in search_cfg.allowed_filetypes:
                if verify_filetype(file, allowed_filetype):
                    if allowed_filetype not in cache_entries[username]:
                        cache_entries[username][allowed_filetype] = []
                    if file_dir not in cache_entries[username][allowed_filetype]:
                        cache_entries[username][allowed_filetype].append(file_dir)

    return SearchResult(
        album_id=album_id,
        success=True,
        cache_entries=cache_entries,
        upload_speeds=upload_speeds,
        query=query,
        result_count=len(search_results),
        elapsed_s=elapsed,
    )


def _merge_search_result(result):
    """Merge a SearchResult into module-level search_cache and user_upload_speed.

    Called only from the main thread — no locking needed.
    """
    album_id = result.album_id
    if album_id not in search_cache:
        search_cache[album_id] = {}

    for username, filetypes in result.cache_entries.items():
        if username not in search_cache[album_id]:
            search_cache[album_id][username] = {}
        for filetype, dirs in filetypes.items():
            if filetype not in search_cache[album_id][username]:
                search_cache[album_id][username][filetype] = []
            for d in dirs:
                if d not in search_cache[album_id][username][filetype]:
                    search_cache[album_id][username][filetype].append(d)

    for username, speed in result.upload_speeds.items():
        if username not in user_upload_speed or speed > user_upload_speed[username]:
            user_upload_speed[username] = speed


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


def try_enqueue(all_tracks, results, allowed_filetype):
    """
    Single album match and enqueue.
    Iterates over all users and enqueues a found match
    """
    album_id = all_tracks[0]["albumId"]
    denied_users = _get_denied_users(album_id)
    # Sort users by upload speed (fastest first) for quicker downloads
    sorted_users = sorted(results.keys(), key=lambda u: user_upload_speed.get(u, 0), reverse=True)
    for username in sorted_users:
        if username in denied_users:
            logger.info(f"Skipping user '{username}' for album ID {album_id}: denylisted (previously provided mislabeled quality)")
            continue
        if allowed_filetype not in results[username]:
            continue
        logger.debug(f"Parsing result from user: {username}")
        file_dirs = results[username][allowed_filetype]
        found, directory, file_dir = check_for_match(all_tracks, allowed_filetype, file_dirs, username)
        if found:
            directory = download_filter(allowed_filetype, directory)
            for i in range(0, len(directory["files"])):
                directory["files"][i]["filename"] = file_dir + "\\" + directory["files"][i]["filename"]
            try:
                downloads = slskd_do_enqueue(username=username, files=directory["files"], file_dir=file_dir)
                if downloads is not None:
                    return True, downloads
                else:
                    album = get_album_by_id(all_tracks[0]["albumId"])
                    album_name = album["title"]
                    artist_name = album["artist"]["artistName"]
                    logger.info(f"Failed to enqueue download to slskd for {artist_name} - {album_name} from {username}")
            except Exception as e:
                album = get_album_by_id(all_tracks[0]["albumId"])
                album_name = album["title"]
                artist_name = album["artist"]["artistName"]

                logger.warning(f"Exception enqueueing tracks: {e}")
                logger.info(f"Exception enqueueing download to slskd for {artist_name} - {album_name} from {username}")
    album = get_album_by_id(all_tracks[0]["albumId"])
    album_name = album["title"]
    artist_name = album["artist"]["artistName"]
    logger.info(f"Failed to enqueue {artist_name} - {album_name}")
    return False, None


def try_multi_enqueue(release, all_tracks, results, allowed_filetype):
    """
    This is the multi-disk/media path for locating and enqueueing an album
    It does a flat search first. Then it does a split search.
    Otherwise it's basically the same as the single album search.
    """
    split_release = []
    tmp_results = copy.deepcopy(results)
    for media in release["media"]:
        disk = {}
        disk["source"] = None
        disk["tracks"] = []
        disk["disk_no"] = media["mediumNumber"]
        disk["disk_count"] = len(release["media"])
        for track in all_tracks:
            if track["mediumNumber"] == media["mediumNumber"]:
                disk["tracks"].append(track)
        split_release.append(disk)
    total = len(split_release)
    count_found = 0
    album_id = all_tracks[0]["albumId"]
    denied_users = _get_denied_users(album_id)
    for disk in split_release:
        for username in tmp_results:
            if username in denied_users:
                logger.info(f"Skipping user '{username}' for album ID {album_id} (multi-disc): denylisted (previously provided mislabeled quality)")
                continue
            if allowed_filetype not in tmp_results[username]:
                continue
            file_dirs = results[username][allowed_filetype]
            found, directory, file_dir = check_for_match(disk["tracks"], allowed_filetype, file_dirs, username)
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
                    album = get_album_by_id(all_tracks[0]["albumId"])
                    album_name = album["title"]
                    artist_name = album["artist"]["artistName"]
                    logger.info(f"Failed to enqueue download to slskd for {artist_name} - {album_name} from {username}")
                    # Delete ALL other downloads in all_downloads list
                    if len(all_downloads) > 0:
                        cancel_and_delete(all_downloads)
                        return False, None
            except Exception:
                album = get_album_by_id(all_tracks[0]["albumId"])
                album_name = album["title"]
                artist_name = album["artist"]["artistName"]

                logger.exception("Exception enqueueing tracks")
                logger.info(f"Exception enqueueing download to slskd for {artist_name} - {album_name} from {username}")
                # Delete all other downloads in all_downloads list
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


# Cache for current album being processed
_current_album_cache = {}


def get_album_by_id(album_id):
    """Get album data by ID from cache."""
    if album_id in _current_album_cache:
        return _current_album_cache[album_id]
    raise KeyError(f"Album {album_id} not found in cache")


def find_download(album, grab_list):
    """
    This does the main loop over search results and user directories
    It has two paths it can take. One is the "single album" path
    The other is the multi-media path.
    """
    album_id = album["id"]
    artist_name = album["artist"]["artistName"]
    artist_id = album["artistId"]
    results = search_cache[album_id]

    # Cache album so get_album_by_id() works during matching
    _current_album_cache[album_id] = album

    filetypes_to_try = cfg.allowed_filetypes

    # Per-album quality override from pipeline DB (e.g. upgrade requests)
    quality_override = album.get("_db_quality_override")
    if quality_override:
        filetypes_to_try = [ft.strip() for ft in quality_override.split(",")]
        logger.info(
            f"Quality override for {artist_name} - {album['title']}: "
            f"searching only {filetypes_to_try}"
        )

    for allowed_filetype in filetypes_to_try:
        logger.info(f"Checking for Quality: {allowed_filetype}")
        releases = list(album.get("releases", []))

        # Check if any release is explicitly monitored by the user.
        has_monitored = any(r.get("monitored", False) for r in releases)

        num_releases = len(releases)
        for _ in range(0, num_releases):
            if len(releases) == 0:
                break
            release = choose_release(artist_name, releases)
            releases.remove(release)
            release_id = release["id"]
            all_tracks = get_album_tracks(album, release_id=release_id)
            if not all_tracks:
                logger.warning(f"No tracks for {artist_name} - {album['title']} (release {release_id}) — skipping")
                continue
            found, downloads = try_enqueue(all_tracks, results, allowed_filetype)

            if found:
                assert downloads is not None
                grab_list[album_id] = GrabListEntry(
                    album_id=album_id,
                    files=downloads,
                    filetype=allowed_filetype,
                    title=album["title"],
                    artist=artist_name,
                    year=album["releaseDate"][0:4],
                    mb_release_id=release.get("foreignReleaseId", ""),

                    db_request_id=album.get("_db_request_id"),
                    db_source=album.get("_db_source"),
                    db_quality_override=album.get("_db_quality_override"),
                )
                return True
            elif len(release["media"]) > 1:
                found, downloads = try_multi_enqueue(release, all_tracks, results, allowed_filetype)
                if found:
                    assert downloads is not None
                    grab_list[album_id] = GrabListEntry(
                        album_id=album_id,
                        files=downloads,
                        filetype=allowed_filetype,
                        title=album["title"],
                        artist=artist_name,
                        year=album["releaseDate"][0:4],
                        mb_release_id=release.get("foreignReleaseId", ""),
                        db_request_id=album.get("_db_request_id"),
                        db_source=album.get("_db_source"),
                        db_quality_override=album.get("_db_quality_override"),
                    )
                    return True

            # If a monitored release was tried and didn't match, stop here.
            if has_monitored and not release.get("monitored", False):
                # We already tried the monitored release (choose_release
                # returns it first) and we're now on a non-monitored one.
                # This shouldn't happen because we break below, but guard
                # against it defensively.
                break
            if has_monitored and release.get("monitored", False):
                # The monitored release was tried and didn't match.
                # Skip remaining releases for this quality tier.
                logger.info(
                    f"Monitored release ({release['trackCount']} tracks) not found on "
                    f"Soulseek for {artist_name} - {album['title']} at quality "
                    f"{allowed_filetype}, skipping non-monitored releases"
                )
                break
    return False


def search_and_queue(albums):
    if cfg.parallel_searches > 1 and len(albums) > 1:
        return _search_and_queue_parallel(albums)
    grab_list = {}
    failed_grab = []
    failed_search = []
    total = len(albums)
    for i, album in enumerate(albums, 1):
        logger.info(f"Album {i}/{total}: {album['artist']['artistName']} - {album['title']}")
        if search_for_album(album):
            if not find_download(album, grab_list):
                failed_grab.append(album)

        else:
            failed_search.append(album)
    return grab_list, failed_search, failed_grab


def _search_and_queue_parallel(albums):
    """Fire all searches concurrently, process results as they arrive."""
    from concurrent.futures import ThreadPoolExecutor, as_completed

    grab_list: dict[Any, Any] = {}
    failed_grab: list[Any] = []
    failed_search: list[Any] = []
    total = len(albums)
    workers = min(cfg.parallel_searches, total)

    logger.info(f"Parallel search: {total} albums, concurrency={workers}")
    wall_start = time.time()

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {
            pool.submit(_execute_search, album, cfg, slskd): album
            for album in albums
        }
        for i, future in enumerate(as_completed(futures), 1):
            album = futures[future]
            try:
                result = future.result()
            except Exception:
                logger.exception(f"Search thread crashed for {album['title']}")
                failed_search.append(album)
                continue

            logger.info(
                f"Search {i}/{total} done: {result.query} "
                f"({result.result_count} results, {result.elapsed_s:.1f}s)"
            )
            if result.success:
                _merge_search_result(result)
                if not find_download(album, grab_list):
                    failed_grab.append(album)
            else:
                failed_search.append(album)

    wall_elapsed = time.time() - wall_start
    logger.info(f"Parallel search complete: {total} albums in {wall_elapsed:.1f}s "
                f"(found={len(grab_list)}, no_match={len(failed_grab)}, "
                f"no_results={len(failed_search)})")

    return grab_list, failed_search, failed_grab


from lib.grab_list import GrabListEntry

from lib.download import (cancel_and_delete as _cancel_and_delete_impl,
                          slskd_do_enqueue as _slskd_do_enqueue_impl,
                          grab_most_wanted as _grab_most_wanted_impl)


def _make_ctx():
    """Build a SoularrContext from module globals (bridge during migration)."""
    from lib.context import SoularrContext
    return SoularrContext(cfg=cfg, slskd=slskd, pipeline_db_source=pipeline_db_source)


def cancel_and_delete(files):
    _cancel_and_delete_impl(files, _make_ctx())


def slskd_do_enqueue(username, files, file_dir):
    return _slskd_do_enqueue_impl(username, files, file_dir, _make_ctx())


def grab_most_wanted(albums):
    return _grab_most_wanted_impl(albums, search_and_queue, _make_ctx())


from lib.util import (_track_titles_cross_check, slskd_version_check,
                      is_docker, setup_logging)


def main():
    global \
        cfg, \
        slskd, \
        config, \
        pipeline_db_source, \
        search_cache, \
        folder_cache, \
        user_upload_speed, \
        broken_user

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

        # Init directory cache. The wide search returns all the data we need. This prevents us from hammering the users on the Soulseek network
        search_cache = {}
        folder_cache = {}
        user_upload_speed = {}
        broken_user = []

        slskd = slskd_api.SlskdClient(host=cfg.slskd_host_url, api_key=cfg.slskd_api_key, url_base=cfg.slskd_url_base)
        wanted_records = []

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
                slskd.transfers.remove_completed_downloads()
            else:
                logger.info(f"{failed}: releases failed to find a match in the search results and are still wanted.")
                slskd.transfers.remove_completed_downloads()
        else:
            logger.info("No releases wanted. Exiting...")

    finally:
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
