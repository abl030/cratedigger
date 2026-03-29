#!/usr/bin/env python
from __future__ import annotations

import argparse
import re
import unicodedata
import os
import sys
import time
import subprocess as sp
import shutil
import difflib
import configparser
import logging
import json
import urllib.request
import urllib.error
from datetime import datetime
from typing import TYPE_CHECKING
import copy
import music_tag
import slskd_api
from slskd_api.apis import users

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


def sanitize_folder_name(folder_name):
    valid_characters = re.sub(r'[<>:."/\\|?*]', "", folder_name)
    return valid_characters.strip()


def cancel_and_delete(files):
    for file in files:
        try:
            slskd.transfers.cancel_download(username=file.username, id=file.id)
        except Exception:
            logger.warning(f"Failed to cancel download {file.filename} for {file.username}", exc_info=True)
        delete_dir = file.file_dir.split("\\")[-1]
        os.chdir(cfg.slskd_download_dir)

        if os.path.exists(delete_dir):
            shutil.rmtree(delete_dir)


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


def download_filter(allowed_filetype, directory):
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


def slskd_do_enqueue(username, files, file_dir):
    """
    Takes a list of files to download and returns a list of files that were successfully added to the download queue
    It also adds to each file the details needed to track that specific file.
    """
    downloads = []
    try:
        enqueue = slskd.transfers.enqueue(username=username, files=files)
    except Exception:
        logger.debug("Enqueue failed", exc_info=True)
        return None
    if enqueue:
        time.sleep(5)
        try:
            download_list = slskd.transfers.get_downloads(username=username)
        except Exception:
            logger.warning(f"Failed to get download status for {username} after enqueue", exc_info=True)
            return None
        for file in files:
            for directory in download_list["directories"]:
                if directory["directory"] == file_dir:
                    for slskd_file in directory["files"]:
                        if file["filename"] == slskd_file["filename"]:
                            downloads.append(DownloadFile(
                                filename=file["filename"],
                                id=slskd_file["id"],
                                file_dir=file_dir,
                                username=username,
                                size=file["size"],
                            ))
        return downloads
    else:
        return None


def slskd_download_status(downloads):
    """
    Takes a list of files and gets the status of each file and packs it into the file object.
    """
    ok = True
    for file in downloads:
        try:
            status = slskd.transfers.get_download(file.username, file.id)
            file.status = status
        except Exception:
            logger.exception(f"Error getting download status of {file.filename}")
            file.status = None
            ok = False
    return ok


def downloads_all_done(downloads):
    """
    Checks the status of all the files in an album and returns a flag if all done as well
    as returning a list of files with errors to check and how many files are in "Queued, Remotely"
    """
    all_done = True
    error_list = []
    remote_queue = 0
    for file in downloads:
        if file.status is not None:
            if not file.status["state"] == "Completed, Succeeded":
                all_done = False
            if file.status["state"] in [
                "Completed, Cancelled",
                "Completed, TimedOut",
                "Completed, Errored",
                "Completed, Rejected",
                "Completed, Aborted",
            ]:
                error_list.append(file)
            if file.status["state"] == "Queued, Remotely":
                remote_queue += 1
    if not len(error_list) > 0:
        error_list = None
    return all_done, error_list, remote_queue


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


from lib.grab_list import GrabListEntry, DownloadFile
from lib.quality import quality_gate_decision, QUALITY_UPGRADE_TIERS, QUALITY_MIN_BITRATE_KBPS


def _check_quality_gate(album_data, request_id):
    """Post-import quality gate: if min track bitrate is below V0, queue for upgrade."""
    mb_id = album_data.mb_release_id
    if not mb_id or not cfg.pipeline_db_enabled or pipeline_db_source is None:
        return
    try:
        import sqlite3 as _sqlite3
        beets_db = os.environ.get("BEETS_DB", "/mnt/virtio/Music/beets-library.db")
        if not os.path.exists(beets_db):
            return
        conn = _sqlite3.connect(f"file:{beets_db}?mode=ro", uri=True)
        album_row = conn.execute(
            "SELECT id FROM albums WHERE mb_albumid = ?", (mb_id,)
        ).fetchone()
        if not album_row:
            conn.close()
            return
        br_row = conn.execute(
            "SELECT MIN(bitrate) FROM items WHERE album_id = ?", (album_row[0],)
        ).fetchone()
        if not br_row or not br_row[0]:
            conn.close()
            return
        min_br_kbps = int(br_row[0] / 1000)

        # Gather pipeline DB state
        spectral_br = None
        req = None
        if request_id:
            try:
                req = pipeline_db_source._get_db().get_request(request_id)
                spectral_br = req.get("spectral_bitrate") if req else None
                if spectral_br and spectral_br < min_br_kbps:
                    logger.info(f"QUALITY GATE: using spectral_bitrate={spectral_br}kbps "
                                f"(lower than beets min_bitrate={min_br_kbps}kbps)")
            except Exception:
                pass
        verified_lossless = req.get("verified_lossless") if req else False

        # CBR detection
        is_cbr = False
        try:
            distinct_br = conn.execute(
                "SELECT COUNT(DISTINCT bitrate) FROM items WHERE album_id = ?",
                (album_row[0],)
            ).fetchone()
            is_cbr = distinct_br and distinct_br[0] == 1
        except Exception:
            pass
        conn.close()

        # --- Pure decision ---
        decision = quality_gate_decision(min_br_kbps, is_cbr, verified_lossless, spectral_br)

        # --- Act on decision ---
        label = f"{album_data.artist} - {album_data.title}"
        spectral_note = f" (spectral={spectral_br}kbps)" if spectral_br else ""

        if decision == "requeue_upgrade":
            if verified_lossless:
                logger.info(
                    f"QUALITY GATE: {label} gate_bitrate < {QUALITY_MIN_BITRATE_KBPS}kbps "
                    f"but verified_lossless=True — accepting")
                # verified_lossless override means quality_gate_decision already
                # forced accept; we only get here if spectral overrode that too,
                # which shouldn't happen. Defensive fallthrough.
            db = pipeline_db_source._get_db()
            db.reset_to_wanted(request_id,
                               quality_override=QUALITY_UPGRADE_TIERS,
                               min_bitrate=min_br_kbps)
            usernames = set(f.username for f in album_data.files
                           if f.username)
            gate_br = spectral_br if (spectral_br and spectral_br < min_br_kbps) else min_br_kbps
            if spectral_br and spectral_br < min_br_kbps:
                reason = (f"quality gate: spectral {spectral_br}kbps "
                          f"(beets {min_br_kbps}kbps) < {QUALITY_MIN_BITRATE_KBPS}kbps")
            else:
                reason = f"quality gate: {min_br_kbps}kbps < {QUALITY_MIN_BITRATE_KBPS}kbps"
            for username in usernames:
                db.add_denylist(request_id, username, reason)
            logger.info(
                f"QUALITY GATE: {label} "
                f"gate_bitrate={gate_br}kbps{spectral_note} < {QUALITY_MIN_BITRATE_KBPS}kbps, "
                f"queued for upgrade, denylisted {usernames} "
                f"(searching {QUALITY_UPGRADE_TIERS})")
        elif decision == "requeue_flac":
            db = pipeline_db_source._get_db()
            db.reset_to_wanted(request_id,
                               quality_override="flac",
                               min_bitrate=min_br_kbps)
            logger.info(
                f"QUALITY GATE: {label} "
                f"min_bitrate={min_br_kbps}kbps CBR, not verified lossless — "
                f"searching for FLAC to verify")
        else:  # accept
            db = pipeline_db_source._get_db()
            update_fields = {"min_bitrate": min_br_kbps}
            if spectral_br:
                update_fields["spectral_bitrate"] = spectral_br
            db.update_status(request_id, "imported", **update_fields)
            if verified_lossless:
                logger.info(f"QUALITY GATE: {label} min_bitrate={min_br_kbps}kbps — quality OK")
            else:
                logger.info(f"QUALITY GATE: {label} min_bitrate={min_br_kbps}kbps VBR — quality OK")
    except Exception:
        logger.exception("QUALITY GATE: failed to check quality")


def _build_download_info(album_data):
    """Extract audio quality metadata from album files for download logging."""
    files = album_data.files
    if not files:
        return {}
    usernames = set(f.username for f in files if f.username)
    filetypes = set(f.filename.split(".")[-1].lower() for f in files if "." in f.filename)
    bitrates = [f.bitRate for f in files if f.bitRate is not None]
    sample_rates = [f.sampleRate for f in files if f.sampleRate is not None]
    bit_depths = [f.bitDepth for f in files if f.bitDepth is not None]
    vbr_flags = [f.isVariableBitRate for f in files if f.isVariableBitRate is not None]

    info = {}
    if usernames:
        info["username"] = ", ".join(sorted(usernames))
    if filetypes:
        info["filetype"] = ", ".join(sorted(filetypes))
    if bitrates:
        info["bitrate"] = max(bitrates)  # representative max
    if sample_rates:
        info["sample_rate"] = max(sample_rates)
    if bit_depths:
        info["bit_depth"] = max(bit_depths)
    if vbr_flags:
        info["is_vbr"] = any(vbr_flags)
    return info


def process_completed_album(album_data: GrabListEntry, failed_grab):
    os.chdir(cfg.slskd_download_dir)
    import_folder_name = sanitize_folder_name(album_data.artist + " - " + album_data.title + " (" + album_data.year + ")")
    import_folder_fullpath = os.path.join(cfg.slskd_download_dir, import_folder_name)
    rm_dirs = []
    moved_files_history = []
    if not os.path.exists(import_folder_fullpath):
        os.mkdir(import_folder_fullpath)
    for file in album_data.files:
        file_folder = file.file_dir.split("\\")[-1]
        filename = file.filename.split("\\")[-1]
        src_folder = os.path.join(cfg.slskd_download_dir, file_folder)
        if src_folder not in rm_dirs:
            rm_dirs.append(src_folder)  # Multi disk albums are sometimes in multiple folders. eg. CD01 CD02. So we need to clean up both
        src_file = os.path.join(src_folder, filename)
        if file.disk_no is not None and file.disk_count is not None and file.disk_count > 1:
            filename = f"Disk {file.disk_no} - {filename}"
        dst_file = os.path.join(import_folder_fullpath, filename)
        file.import_path = dst_file
        try:
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
                os.rmdir(import_folder_fullpath)
            except OSError:
                logger.warning(f"Could not remove temp import directory {import_folder_fullpath}")
            return
    else:  # Only runs if all files are successfully moved
        for rm_dir in rm_dirs:
            if not rm_dir == import_folder_fullpath:
                try:
                    os.rmdir(rm_dir)
                except OSError:
                    logger.warning(f"Skipping removal of {rm_dir} because it's not empty.")
        logger.info(f"Processing completed download: {album_data.artist} - {album_data.title}")
        for file in album_data.files:
            try:  # This sometimes fails. No idea why. Nor do we care. We try and that's what matters
                song = music_tag.load_file(file.import_path)
                assert song is not None
                if file.disk_no is not None:
                    song["discnumber"] = file.disk_no
                    song["totaldiscs"] = file.disk_count

                song["albumartist"] = album_data.artist
                song["album"] = album_data.title
                song.save()
            except Exception:
                logger.exception(f"Error writing tags for: {file.import_path}")
        if cfg.beets_validation_enabled and album_data.mb_release_id:
            # === Beets validation path ===
            bv_result = beets_validate(import_folder_fullpath,
                                       album_data.mb_release_id,
                                       cfg.beets_distance_threshold)

            if bv_result["valid"]:
                # Repair MP3 headers before audio validation
                repair_mp3_headers(import_folder_fullpath)
                # Audio integrity check before staging
                audio_result = validate_audio(import_folder_fullpath, cfg.audio_check_mode)
                if not audio_result["valid"]:
                    bv_result = {
                        "valid": False,
                        "distance": bv_result.get("distance"),
                        "mbid_found": bv_result.get("mbid_found"),
                        "scenario": "audio_corrupt",
                        "detail": audio_result["error"],
                        "error": None,
                    }

            # Spectral check for non-VBR MP3 downloads (CBR 320 has no other quality signal)
            if bv_result["valid"]:
                dl_info_pre = _build_download_info(album_data)
                filetype_str = dl_info_pre.get("filetype", "").lower()
                is_vbr = dl_info_pre.get("is_vbr", False)
                is_mp3 = "mp3" in filetype_str and "flac" not in filetype_str
                if is_mp3 and not is_vbr:
                    try:
                        lib_dir = os.path.join(os.path.dirname(__file__), "lib")
                        if lib_dir not in sys.path:
                            sys.path.insert(0, lib_dir)
                        from spectral_check import analyze_album as spectral_analyze
                        spectral_result = spectral_analyze(import_folder_fullpath, trim_seconds=30)
                        logger.info(f"SPECTRAL: {album_data.artist} - {album_data.title} "
                                    f"grade={spectral_result.grade}, "
                                    f"estimated_bitrate={spectral_result.estimated_bitrate_kbps}kbps, "
                                    f"suspect={spectral_result.suspect_pct:.0f}%")
                        # Store in album_data for downstream dl_info
                        album_data.spectral_grade = spectral_result.grade
                        album_data.spectral_bitrate = spectral_result.estimated_bitrate_kbps
                        # Also check existing beets files for comparison
                        mb_id = album_data.mb_release_id
                        if mb_id:
                            try:
                                import sqlite3 as _sqlite3
                                beets_db = os.environ.get("BEETS_DB", "/mnt/virtio/Music/beets-library.db")
                                if os.path.exists(beets_db):
                                    conn = _sqlite3.connect(f"file:{beets_db}?mode=ro", uri=True)
                                    # Get existing album path + min bitrate
                                    row = conn.execute(
                                        "SELECT a.id, "
                                        "  (SELECT path FROM items WHERE album_id = a.id LIMIT 1), "
                                        "  (SELECT CAST(MIN(bitrate)/1000 AS INTEGER) FROM items WHERE album_id = a.id) "
                                        "FROM albums a WHERE a.mb_albumid = ?", (mb_id,)
                                    ).fetchone()
                                    conn.close()
                                    if row and row[1]:
                                        existing_path = os.path.dirname(
                                            row[1].decode() if isinstance(row[1], bytes) else row[1])
                                        album_data.existing_min_bitrate = row[2]
                                        if os.path.isdir(existing_path):
                                            existing_spectral = spectral_analyze(existing_path, trim_seconds=30)
                                            album_data.existing_spectral_bitrate = existing_spectral.estimated_bitrate_kbps
                                            logger.info(f"SPECTRAL: existing on disk: grade={existing_spectral.grade}, "
                                                        f"estimated_bitrate={existing_spectral.estimated_bitrate_kbps}kbps, "
                                                        f"beets_min={row[2]}kbps")
                            except Exception:
                                logger.exception("SPECTRAL: failed to check existing files")
                        # Decision: compare spectral bitrates
                        new_quality = spectral_result.estimated_bitrate_kbps
                        existing_quality = album_data.existing_spectral_bitrate or 0
                        request_id = album_data.db_request_id

                        if spectral_result.grade in ("suspect", "likely_transcode"):
                            if new_quality and existing_quality and new_quality <= existing_quality:
                                # Not an upgrade — reject and denylist
                                logger.warning(
                                    f"SPECTRAL REJECT: {album_data.artist} - {album_data.title} "
                                    f"new spectral {new_quality}kbps <= existing {existing_quality}kbps")
                                usernames = set(f.username for f in album_data.files
                                                if f.username)
                                if request_id and pipeline_db_source:
                                    db = pipeline_db_source._get_db()
                                    for username in usernames:
                                        db.add_denylist(request_id, username,
                                                        f"spectral: {new_quality}kbps <= existing {existing_quality}kbps")
                                    # Log the rejected download
                                    dl_info_rej = _build_download_info(album_data)
                                    dl_info_rej["spectral_grade"] = spectral_result.grade
                                    dl_info_rej["spectral_bitrate"] = new_quality
                                    dl_info_rej["existing_spectral_bitrate"] = existing_quality
                                    dl_info_rej["slskd_filetype"] = dl_info_rej.get("filetype")
                                    dl_info_rej["actual_filetype"] = dl_info_rej.get("filetype")
                                    pipeline_db_source.mark_failed(
                                        album_data,
                                        {"distance": bv_result.get("distance"), "scenario": "spectral_reject",
                                         "detail": f"spectral {new_quality}kbps <= existing {existing_quality}kbps",
                                         "error": None},
                                        usernames=usernames, download_info=dl_info_rej)
                                    logger.info(f"  Denylisted {usernames} for request {request_id}")
                                move_failed_import(import_folder_fullpath)
                                bv_result["valid"] = False
                            elif new_quality and (not existing_quality or new_quality > existing_quality):
                                # Suspect but better than what we have — import as upgrade
                                logger.info(
                                    f"SPECTRAL UPGRADE: {album_data.artist} - {album_data.title} "
                                    f"suspect at {new_quality}kbps but > existing {existing_quality}kbps, importing")
                            elif not existing_quality:
                                # Nothing on disk yet — something is better than nothing
                                logger.info(
                                    f"SPECTRAL: {album_data.artist} - {album_data.title} "
                                    f"suspect at {new_quality}kbps but no existing album, importing")
                    except Exception:
                        logger.exception(f"SPECTRAL: failed for {album_data.artist} - {album_data.title}")

            if bv_result["valid"]:
                dest = stage_to_ai(album_data, import_folder_fullpath, cfg.beets_staging_dir)
                log_validation_result(album_data, bv_result, dest)
                logger.info(f"STAGED: {album_data.artist} - {album_data.title} "
                            f"(scenario={bv_result.get('scenario')}, "
                            f"distance={bv_result['distance']:.4f}) → {dest}")

                dl_info = _build_download_info(album_data)
                # Inject spectral analysis results from pre-staging check
                if album_data.spectral_grade:
                    dl_info["spectral_grade"] = album_data.spectral_grade
                    dl_info["spectral_bitrate"] = album_data.spectral_bitrate
                    dl_info["existing_spectral_bitrate"] = album_data.existing_spectral_bitrate
                    dl_info["existing_min_bitrate"] = album_data.existing_min_bitrate
                    dl_info["slskd_filetype"] = dl_info.get("filetype")
                    dl_info["actual_filetype"] = dl_info.get("filetype")
                source_type = album_data.db_source or "redownload"
                request_id = album_data.db_request_id
                if source_type == "request" and bv_result.get("distance", 1.0) <= cfg.beets_distance_threshold:
                    # Auto-import via import_one.py (handles convert + import + DB updates)
                    import_script = os.path.join(
                        os.path.dirname(cfg.beets_harness_path), "import_one.py")
                    mb_id = album_data.mb_release_id or ""
                    logger.info(f"AUTO-IMPORT: {album_data.artist} - {album_data.title} "
                                f"(source=request, dist={bv_result['distance']:.4f})")
                    try:
                        cmd = [sys.executable, import_script, dest, mb_id]
                        if request_id:
                            cmd.extend(["--request-id", str(request_id)])
                            # Pass pipeline DB min_bitrate to override beets
                            # comparison when existing files are known garbage
                            try:
                                req = pipeline_db_source._get_db().get_request(request_id)
                                db_min_br = req.get("min_bitrate") if req else None
                                if db_min_br is not None:
                                    cmd.extend(["--override-min-bitrate", str(db_min_br)])
                            except Exception:
                                pass
                        import_env = {**os.environ, "HOME": "/home/abl030"}
                        result = sp.run(cmd, capture_output=True, text=True,
                                        timeout=1800, env=import_env)
                        if result.returncode == 0:
                            logger.info(f"AUTO-IMPORT OK: {album_data.artist} - {album_data.title}")
                            # Log beets internal logging (stderr)
                            for line in (result.stderr or "").strip().split("\n"):
                                if line.strip():
                                    logger.info(f"  [beets] {line}")
                            prev_min_br = None
                            new_min_br = None
                            for line in result.stdout.strip().split("\n"):
                                logger.info(f"  {line}")
                                # Detect FLAC->V0 conversion from import_one output
                                if line.strip().startswith("Converted ") and ", failed " in line:
                                    try:
                                        parts = line.strip().split()
                                        conv_count = int(parts[1].rstrip(","))
                                        if conv_count > 0:
                                            dl_info["was_converted"] = True
                                            dl_info["original_filetype"] = "flac"
                                            dl_info["filetype"] = "mp3"
                                            dl_info["is_vbr"] = True
                                    except (ValueError, IndexError):
                                        pass
                                # Capture actual post-conversion bitrate
                                if line.strip().startswith("min_bitrate="):
                                    try:
                                        actual_br = int(line.strip().split("=")[1])
                                        dl_info["bitrate"] = actual_br * 1000
                                    except (ValueError, IndexError):
                                        pass
                                # Capture upgrade delta from import_one
                                if line.strip().startswith("prev_min_bitrate="):
                                    try:
                                        prev_min_br = int(line.strip().split("=")[1])
                                    except (ValueError, IndexError):
                                        pass
                                if line.strip().startswith("new_min_bitrate="):
                                    try:
                                        new_min_br = int(line.strip().split("=")[1])
                                    except (ValueError, IndexError):
                                        pass
                                # Capture spectral analysis results
                                if line.strip().startswith("spectral_grade="):
                                    dl_info["spectral_grade"] = line.strip().split("=")[1]
                                if line.strip().startswith("spectral_bitrate="):
                                    try:
                                        dl_info["spectral_bitrate"] = int(line.strip().split("=")[1])
                                    except (ValueError, IndexError):
                                        pass
                                if line.strip().startswith("existing_spectral_bitrate="):
                                    try:
                                        dl_info["existing_spectral_bitrate"] = int(line.strip().split("=")[1])
                                    except (ValueError, IndexError):
                                        pass
                            # Set slskd-reported quality (before conversion changed it)
                            if dl_info.get("was_converted"):
                                dl_info["slskd_filetype"] = dl_info.get("original_filetype", "flac")
                                dl_info["actual_filetype"] = dl_info.get("filetype", "mp3")
                            else:
                                dl_info["slskd_filetype"] = dl_info.get("filetype")
                                dl_info["actual_filetype"] = dl_info.get("filetype")
                            # Ensure DB status is set even if import_one's DB update failed
                            pipeline_db_source.mark_done(album_data, bv_result, dest_path=dest, download_info=dl_info)
                            # Update upgrade delta on the pipeline request
                            if request_id and (prev_min_br is not None or new_min_br is not None):
                                try:
                                    db = pipeline_db_source._get_db()
                                    db.update_status(request_id, "imported",
                                                     prev_min_bitrate=prev_min_br or db.get_request(request_id).get("min_bitrate"),
                                                     min_bitrate=new_min_br)
                                except Exception:
                                    logger.exception("Failed to update upgrade delta")
                            # Quality gate: if below V0, queue for upgrade
                            _check_quality_gate(album_data, request_id)
                            trigger_meelo_scan()
                            # Clean up staged directory -- beets already moved files
                            # to /Beets, or pre-flight found a dupe (files unneeded)
                            if os.path.isdir(dest):
                                shutil.rmtree(dest)
                                logger.info(f"  Cleaned up staged dir: {dest}")
                                parent = os.path.dirname(dest)
                                if os.path.isdir(parent) and not os.listdir(parent):
                                    os.rmdir(parent)
                                    logger.info(f"  Cleaned up empty artist dir: {parent}")
                        elif result.returncode == 5:
                            # Quality downgrade -- not imported
                            logger.warning(
                                f"QUALITY DOWNGRADE PREVENTED: {album_data.artist} - {album_data.title}")
                            for line in result.stderr.strip().split("\n"):
                                logger.warning(f"  {line}")
                            usernames = set(f.username for f in album_data.files
                                            if f.username)
                            db = pipeline_db_source._get_db()
                            for username in usernames:
                                db.add_denylist(request_id, username, "quality downgrade prevented")
                            logger.info(f"  Denylisted {usernames} for request {request_id}")
                            if os.path.isdir(dest):
                                shutil.rmtree(dest)
                        elif result.returncode == 6:
                            # Transcode detected. May or may not have imported:
                            # - "[OK] Transcode imported" = imported as upgrade, keep searching
                            # - "[QUALITY DOWNGRADE]" = not imported, keep searching
                            stdout_text = result.stdout or ""
                            imported_transcode = "[OK] Transcode imported" in stdout_text
                            actual_br = None
                            for line in stdout_text.strip().split("\n"):
                                logger.info(f"  {line}")
                                if line.strip().startswith("min_bitrate="):
                                    try:
                                        actual_br = int(line.strip().split("=")[1])
                                    except (ValueError, IndexError):
                                        pass
                                # Capture conversion info for download log
                                if line.strip().startswith("Converted ") and ", failed " in line:
                                    try:
                                        parts = line.strip().split()
                                        conv_count = int(parts[1].rstrip(","))
                                        if conv_count > 0:
                                            dl_info["was_converted"] = True
                                            dl_info["original_filetype"] = "flac"
                                            dl_info["filetype"] = "mp3"
                                            dl_info["is_vbr"] = True
                                    except (ValueError, IndexError):
                                        pass
                            if actual_br:
                                dl_info["bitrate"] = actual_br * 1000
                            for line in (result.stderr or "").strip().split("\n"):
                                if line.strip():
                                    logger.warning(f"  {line}")
                            if imported_transcode:
                                logger.info(
                                    f"TRANSCODE UPGRADE: {album_data.artist} - {album_data.title} "
                                    f"imported at {actual_br}kbps, denylisting + continuing search")
                                pipeline_db_source.mark_done(album_data, bv_result, dest_path=dest, download_info=dl_info)
                                trigger_meelo_scan()
                            else:
                                logger.warning(
                                    f"TRANSCODE REJECTED: {album_data.artist} - {album_data.title} "
                                    f"at {actual_br}kbps -- not an upgrade")
                            # Denylist source user and reset to wanted for further searching
                            usernames = set(f.username for f in album_data.files
                                            if f.username)
                            db = pipeline_db_source._get_db()
                            reason = f"transcode: {actual_br}kbps" if actual_br else "transcode detected"
                            for username in usernames:
                                db.add_denylist(request_id, username, reason)
                            logger.info(f"  Denylisted {usernames} for request {request_id}")
                            # Reset to wanted so we keep searching for better quality
                            # Only update min_bitrate if we actually imported -- otherwise
                            # keep the existing on-disk value
                            db.reset_to_wanted(request_id,
                                               quality_override=QUALITY_UPGRADE_TIERS,
                                               min_bitrate=actual_br if imported_transcode else None)
                            # Clean up staged dir (beets already moved files if imported)
                            if os.path.isdir(dest):
                                shutil.rmtree(dest)
                                logger.info(f"  Cleaned up staged dir: {dest}")
                                parent = os.path.dirname(dest)
                                if os.path.isdir(parent) and not os.listdir(parent):
                                    os.rmdir(parent)
                                    logger.info(f"  Cleaned up empty artist dir: {parent}")
                        else:
                            logger.error(f"AUTO-IMPORT FAILED (rc={result.returncode}): "
                                         f"{album_data.artist} - {album_data.title}")
                            for line in result.stderr.strip().split("\n"):
                                logger.error(f"  {line}")
                            for line in result.stdout.strip().split("\n"):
                                logger.error(f"  {line}")
                            # import_one.py already set DB to review_needed
                    except sp.TimeoutExpired:
                        logger.error(f"AUTO-IMPORT TIMEOUT: {album_data.artist} - {album_data.title}")
                    except Exception:
                        logger.exception(f"AUTO-IMPORT ERROR: {album_data.artist} - {album_data.title}")
                else:
                    # Redownload or high distance: stage only, user reviews manually
                    pipeline_db_source.mark_done(album_data, bv_result, dest_path=dest, download_info=dl_info)
            else:
                move_failed_import(import_folder_fullpath)
                log_validation_result(album_data, bv_result)
                usernames = set(f.username for f in album_data.files)
                dl_info = _build_download_info(album_data)
                pipeline_db_source.mark_failed(album_data, bv_result, usernames=usernames, download_info=dl_info)
                logger.warning(f"REJECTED: {album_data.artist} - {album_data.title} "
                              f"(scenario={bv_result.get('scenario')}, "
                              f"distance={bv_result.get('distance')}, "
                              f"detail={bv_result.get('detail')}) "
                              f"| denylisted users: {', '.join(usernames)}")


def monitor_downloads(grab_list, failed_grab):
    def delete_album(reason):
        entry = grab_list[album_id]
        cancel_and_delete(entry.files)
        usernames = set(f.username for f in entry.files if f.username)
        total = len(entry.files)
        completed = sum(1 for f in entry.files if f.status and f.status.get("state") == "Completed, Succeeded")
        elapsed = time.time() - (entry.count_start or time.time())
        elapsed_min = elapsed / 60
        logger.info(f"{reason} Album: {entry.title} Artist: {entry.artist} "
                     f"({completed}/{total} files done, {elapsed_min:.1f}min elapsed, "
                     f"stalled_timeout={cfg.stalled_timeout}s, cfg.remote_queue_timeout={cfg.remote_queue_timeout}s)")
        del grab_list[album_id]

    while True:
        total_albums = len(grab_list)
        # Deal with the problems.
        #    "Completed, Cancelled", Abort album as failed
        #    "Completed, TimedOut",  Abort album as failed
        #    "Completed, Errored",   Abort album as failed
        #    "Completed, Aborted",   Abort album as failed
        #    "Completed, Rejected",  Retry. Some users have a max grab count. We need to check if ALL files are Rejected first.
        # We're going to need to drop items out of the list. So we might have to resort to enumerating the keys so we don't hit issues.
        done_count = 0
        for album_id in list(grab_list.keys()):
            entry = grab_list[album_id]
            if slskd_download_status(entry.files):
                album_done, problems, queued = downloads_all_done(entry.files)  # Lets check to see what status the files have
                if entry.count_start is None:
                    entry.count_start = time.time()
                if (time.time() - entry.count_start) >= cfg.stalled_timeout:  # Album is taking too long. Bail out regardless
                    delete_album("Timeout waiting for download of")
                    continue
                if queued == len(entry.files):  # Shorter time out for whole albums in "Queued, Remotely"
                    if (time.time() - entry.count_start) >= cfg.remote_queue_timeout:
                        delete_album("Timeout waiting for download of")
                        continue
                # Also timeout when most files are done but some are stuck queued remotely
                if queued > 0 and done_count > 0:
                    completed = sum(1 for f in entry.files
                                    if f.status and f.status["state"] == "Completed, Succeeded")
                    if completed + queued == len(entry.files):
                        # All files are either done or stuck — apply remote_queue_timeout
                        if (time.time() - entry.count_start) >= cfg.remote_queue_timeout:
                            delete_album("Timeout waiting for stuck remote queue file in")
                            continue
                done_count += album_done
                if problems is not None:
                    logger.debug("We got problems!")
                    for file in problems:
                        logger.debug(f"Checking {file.filename}")
                        match file.status["state"]:
                            case (
                                "Completed, Cancelled" | "Completed, TimedOut" | "Completed, Errored" | "Completed, Aborted"
                            ):  # Normal errors. We'll retry a few times as sometumes the error is transient
                                abort = False
                                if len(problems) == len(entry.files):
                                    delete_album("Failed grab of")
                                    break
                                for download_file in entry.files:
                                    if file.filename == download_file.filename:
                                        if download_file.retry is None:
                                            download_file.retry = 0
                                        download_file.retry += 1
                                        if download_file.retry < 5:
                                            retry = download_file.retry
                                            size = file.size
                                            data_dict = [{"filename": file.filename, "size": size}]
                                            logger.info(f"Download error. Requeue file: {file.filename}")
                                            requeue = slskd_do_enqueue(
                                                file.username,
                                                data_dict,
                                                file.file_dir,
                                            )
                                            if requeue is not None:
                                                download_file.id = requeue[0].id
                                                download_file.retry = retry
                                                time.sleep(1)
                                                _ = slskd_download_status(entry.files)  # Refresh the status of the files to prevent issues.
                                            else:
                                                delete_album("Failed grab of")
                                                abort = True  # Move to the next album so we don't block or overload a remote user
                                                break
                                        else:
                                            # Delete from album list add to failures
                                            delete_album("Failed grab of")
                                            abort = True  # As above.
                                            break
                                if abort:
                                    break
                            case "Completed, Rejected":
                                # Do a measured retry. This is often a soft failure due to grab limits. Check if any files worked then go from there.
                                # This needs a recode. But it works for now.
                                # In the recode we need to test to see if we are getting multiple albums from the same user and temper our retries based on
                                # those other album(s) completing.
                                # If we aren't in that condition we need to fall back to per file retry counts as files will also be rejected if the file is
                                # too long or too short based on the share record. This can happen when people re-tag media but don't rescan media.
                                # Also I've seen cases of single files out of a set being in the "not shared" category.
                                if len(problems) == len(entry.files):
                                    delete_album("Failed grab of")  # They are all rejected. Usually this happens because of misconfigurations. Files appear in search but aren't shared.
                                    break
                                else:
                                    if entry.rejected_retries is None:
                                        entry.rejected_retries = 0
                                    working_count = len(entry.files) - len(problems)
                                    for gfile in entry.files:
                                        if gfile.status["state"] in [
                                            "Completed, Succeeded",
                                            "Queued, Remotely",
                                            "Queued, Locally",
                                        ]:
                                            working_count -= 1
                                    if working_count == 0:
                                        if entry.rejected_retries < int(len(entry.files) * 1.2):  # Little bit of wiggle room here
                                            abort = False
                                            for gfile in entry.files:
                                                if gfile.filename == file.filename:
                                                    size = file.size
                                                    data_dict = [
                                                        {
                                                            "filename": file.filename,
                                                            "size": size,
                                                        }
                                                    ]
                                                    logger.info(f"Download error. Requeue file: {file.filename}")
                                                    requeue = slskd_do_enqueue(
                                                        file.username,
                                                        data_dict,
                                                        file.file_dir,
                                                    )
                                                    if requeue is not None:
                                                        gfile.id = requeue[0].id
                                                        entry.rejected_retries += 1
                                                        _ = slskd_download_status(entry.files)
                                                        abort = True
                                                        break
                                                    else:
                                                        cancel_and_delete(entry.files)
                                                        logger.info(f"Failed grab of Album: {entry.title} Artist: {entry.artist}")
                                                        del grab_list[album_id]
                                                        abort = True
                                                        break
                                            if abort:
                                                break
                                        else:
                                            delete_album("Failed grab of")
                                            break
                            case _:
                                logger.error(
                                    "Not sure how I got here. This shouldn't be possible for problem files!"
                                )  # This really should be impossible to reach. But is required to round out the case statement.
                else:
                    if album_done:
                        logger.info(f"Completed download of Album: {entry.title} Artist: {entry.artist}")
                        process_completed_album(entry, failed_grab)
                        del grab_list[album_id]

            else:
                if entry.error_count is None:
                    entry.error_count = 0
                entry.error_count += 1
            # I dunno. slskd might be broken? Or the user deleted things? I've never seen this so I have no idea what we should do here. It most likely would mean SLSKD is down.
            # So we probably want to abort everything because cleanup would be impossible.

        if len(grab_list) < 1:  # We remove items from the grab list once they are downloaded or aborted. So when there are no grabs left, we are done!
            break

        time.sleep(5)  # Wait for things to progress and start the checks again.


def grab_most_wanted(albums):
    """
    This is the "main loop" that calls all the functions to do all the work.
    Basic flow per item is as follows:
    Perform coarse search
    Check search results for a match
    enqueue download
    After that has happened for all the downloads it then shifts to monitoring the downloads:
    Monitor download and perform retries and/or requeues.
    When all completed, process the downloaded album
    """

    grab_list, failed_search, failed_grab = search_and_queue(albums)

    total_albums = len(grab_list)
    logger.info(f"Total Downloads added: {total_albums}")
    for album_id in grab_list:
        logger.info(f"Album: {grab_list[album_id].title} Artist: {grab_list[album_id].artist}")
    logger.info(f"Failed to grab: {len(failed_grab)}")
    for album in failed_grab:
        logger.info(f"Album: {album['title']} Artist: {album['artist']['artistName']}")

    logger.info("-------------------")
    logger.info(f"Waiting for downloads... monitor at: {''.join([cfg.slskd_host_url, cfg.slskd_url_base, 'downloads'])}")

    monitor_downloads(grab_list, failed_grab)

    count = len(failed_search) + len(failed_grab)
    for album in failed_search:
        album_title = album["title"]
        artist_name = album["artist"]["artistName"]
        logger.info(f"Search failed for Album: {album_title} - Artist: {artist_name}")
    for album in failed_grab:
        album_title = album["title"]
        artist_name = album["artist"]["artistName"]
        logger.info(f"Download failed for Album: {album_title} - Artist: {artist_name}")

    return count


def move_failed_import(src_path):
    failed_imports_dir = "failed_imports"

    if not os.path.exists(failed_imports_dir):
        os.makedirs(failed_imports_dir)

    folder_name = os.path.basename(src_path)
    target_path = os.path.join(failed_imports_dir, folder_name)

    counter = 1
    while os.path.exists(target_path):
        target_path = os.path.join(failed_imports_dir, f"{folder_name}_{counter}")
        counter += 1

    if os.path.exists(folder_name):
        shutil.move(folder_name, target_path)
        logger.info(f"Failed import moved to: {target_path}")


def _normalize_title(s):
    """Normalize a title for comparison: lowercase, strip punctuation, collapse whitespace."""
    s = unicodedata.normalize("NFKD", s)
    s = s.lower().strip()
    s = re.sub(r"[''`]", "'", s)
    s = re.sub(r"[^\w\s'&]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _extract_title_from_filename(filename):
    """Extract a track title from a Soulseek filename.

    Strips: extension, leading track numbers, artist prefixes.
    Returns normalized title via _normalize_title().

    Examples:
        "01 - Enter Sandman.mp3" → "enter sandman"
        "01_Enter_Sandman.flac" → "enter sandman"
        "Metallica - 01 - Enter Sandman.mp3" → "enter sandman"
        "Enter Sandman.mp3" → "enter sandman"
    """
    # Strip extension
    name = re.sub(r'\.[a-zA-Z0-9]{2,4}$', '', filename)
    # Replace underscores with spaces
    name = name.replace('_', ' ')
    # Strip leading "Artist - " prefix (before track number)
    # Pattern: "Something - 01 - Title" → "01 - Title"
    name = re.sub(r'^.+?\s*-\s*(?=\d{1,2}\s*[-.\s])', '', name)
    # Strip leading track number patterns: "01 - ", "01. ", "01 ", "1 - ", etc.
    name = re.sub(r'^\d{1,3}\s*[-._)\s]+\s*', '', name)
    # Strip leading "Artist - " if still present (e.g., "Artist - Title")
    # Only strip if there's content after the dash
    if ' - ' in name:
        parts = name.split(' - ', 1)
        # Heuristic: if the part before dash is short-ish, it might be artist
        # But if after dash is empty, keep the whole thing
        if len(parts) == 2 and parts[1].strip():
            name = parts[1]
    return _normalize_title(name)


def _track_titles_cross_check(expected_tracks, slskd_files):
    """Cross-check that Soulseek filenames match expected track titles.

    This catches wrong pressings with different tracklists (e.g., Weezer Blue vs Green).
    Runs AFTER album_match() passes (track count + fuzzy filename already verified).

    Returns True if enough titles match, False if too many are missing.
    Tolerance: up to 1/5 tracks can mismatch (same as _tracks_are_trivial_match).
    """
    if not expected_tracks or not slskd_files:
        return True  # Nothing to check

    # Extract and normalize expected titles
    expected = [_normalize_title(t.get("title", "")) for t in expected_tracks]

    # Extract and normalize titles from Soulseek filenames
    slskd_titles = [_extract_title_from_filename(f.get("filename", "")) for f in slskd_files]

    # For each expected title, find the best fuzzy match among Soulseek titles
    mismatches = 0
    for exp_title in expected:
        if not exp_title:
            continue
        best_ratio = 0.0
        for slskd_title in slskd_titles:
            if not slskd_title:
                continue
            # Check substring containment first (fast path)
            if exp_title in slskd_title or slskd_title in exp_title:
                best_ratio = 1.0
                break
            ratio = difflib.SequenceMatcher(None, exp_title, slskd_title).ratio()
            if ratio > best_ratio:
                best_ratio = ratio
        if best_ratio < 0.5:  # Lenient threshold — just catching obviously wrong tracks
            mismatches += 1

    max_allowed = max(1, len(expected) // 5)
    if mismatches > max_allowed:
        logger.info(f"CROSS-CHECK: {mismatches}/{len(expected)} tracks failed title match "
                    f"(max allowed: {max_allowed})")
        return False
    return True



def repair_mp3_headers(folder_path):
    """Run mp3val -f on all MP3 files to fix header issues before audio validation."""
    for f in os.listdir(folder_path):
        if not f.lower().endswith(".mp3"):
            continue
        filepath = os.path.join(folder_path, f)
        try:
            result = sp.run(["mp3val", "-f", filepath],
                            capture_output=True, text=True, timeout=60)
            if "FIXED" in result.stdout:
                logger.info(f"MP3VAL: fixed {f}")
        except FileNotFoundError:
            logger.warning("MP3VAL: mp3val not found on PATH — skipping header repair")
            return
        except sp.TimeoutExpired:
            logger.warning(f"MP3VAL: timeout on {f}")
        except Exception:
            logger.exception(f"MP3VAL: error on {f}")


def validate_audio(folder_path, mode="normal"):
    """Check audio integrity of downloaded files via ffmpeg full decode.

    mode: "strict" = any error rejects, "normal" = reject if >10% fail, "off" = skip.
    Returns: {"valid": bool, "error": str|None, "failed_files": list}
    """
    if mode == "off":
        return {"valid": True, "error": None, "failed_files": []}

    audio_exts = {"mp3", "flac", "m4a", "ogg", "opus", "wma", "aac", "alac", "wav"}
    files = []
    for f in os.listdir(folder_path):
        ext = f.rsplit(".", 1)[-1].lower() if "." in f else ""
        if ext in audio_exts:
            files.append(os.path.join(folder_path, f))

    if not files:
        return {"valid": True, "error": None, "failed_files": []}

    failed = []
    for filepath in files:
        try:
            result = sp.run(
                ["ffmpeg", "-v", "error", "-i", filepath, "-f", "null", "-"],
                capture_output=True, text=True, timeout=300
            )
            if result.returncode != 0 or result.stderr.strip():
                stderr = result.stderr.strip()
                # FLAC missing MD5: re-encode in place to fix, then re-test
                if filepath.lower().endswith(".flac") and "cannot check MD5 signature" in stderr:
                    logger.info(f"AUDIO_CHECK: fixing unset MD5: {os.path.basename(filepath)}")
                    fix = sp.run(
                        ["flac", "-f", "--verify", filepath],
                        capture_output=True, text=True, timeout=300,
                    )
                    if fix.returncode == 0:
                        retest = sp.run(
                            ["ffmpeg", "-v", "error", "-i", filepath, "-f", "null", "-"],
                            capture_output=True, text=True, timeout=300,
                        )
                        if retest.returncode == 0 and not retest.stderr.strip():
                            continue  # fixed and clean
                        stderr = retest.stderr.strip()
                    else:
                        stderr = f"MD5 fix failed: {fix.stderr.strip()[:150]}"
                err = stderr[:200]
                failed.append((os.path.basename(filepath), err))
        except sp.TimeoutExpired:
            failed.append((os.path.basename(filepath), "ffmpeg timeout"))
        except FileNotFoundError:
            logger.error("AUDIO_CHECK: ffmpeg not found on PATH — skipping audio validation")
            return {"valid": True, "error": None, "failed_files": []}

    if not failed:
        logger.info(f"AUDIO_CHECK: all {len(files)} files passed ({mode} mode)")
        return {"valid": True, "error": None, "failed_files": []}

    fail_pct = len(failed) / len(files)
    detail = "; ".join(f"{name}: {err}" for name, err in failed[:5])
    error_msg = f"{len(failed)}/{len(files)} files failed: {detail}"
    logger.warning(f"AUDIO_CHECK: {error_msg}")

    if mode == "strict":
        reject = True
    else:  # normal
        reject = fail_pct > 0.10 or any(len(err) > 500 for _, err in failed)

    if reject:
        logger.warning(f"AUDIO_CHECK: → REJECT ({mode} mode, {fail_pct:.0%} failed)")
        return {"valid": False, "error": error_msg, "failed_files": failed}
    else:
        logger.info(f"AUDIO_CHECK: → PASS ({mode} mode, {fail_pct:.0%} failed, below threshold)")
        return {"valid": True, "error": None, "failed_files": failed}


def beets_validate(album_path, mb_release_id, distance_threshold=0.15):
    """Thin wrapper — delegates to lib.beets.beets_validate()."""
    from lib.beets import beets_validate as _bv
    return _bv(cfg.beets_harness_path, album_path, mb_release_id, distance_threshold)


def stage_to_ai(album_data, source_path, staging_dir):
    """Move validated files from slskd download area to /AI/{Artist}/{Album}/."""
    artist_dir = sanitize_folder_name(album_data.artist)
    album_dir = sanitize_folder_name(album_data.title)
    dest = os.path.join(staging_dir, artist_dir, album_dir)
    os.makedirs(dest, exist_ok=True)

    for f in os.listdir(source_path):
        src = os.path.join(source_path, f)
        dst = os.path.join(dest, f)
        shutil.move(src, dst)

    shutil.rmtree(source_path, ignore_errors=True)
    return dest


def trigger_meelo_scan():
    """Trigger a Meelo library scan after import. Best-effort — failures don't block."""
    if not cfg.meelo_url:
        return
    try:
        # Get JWT
        login_data = json.dumps({"username": cfg.meelo_username, "password": cfg.meelo_password}).encode()
        login_req = urllib.request.Request(
            f"{cfg.meelo_url}/api/auth/login",
            data=login_data,
            headers={"Content-Type": "application/json"},
        )
        with urllib.request.urlopen(login_req, timeout=10) as resp:
            jwt = json.loads(resp.read())["access_token"]

        # Trigger scan of beets library only
        scan_req = urllib.request.Request(
            f"{cfg.meelo_url}/scanner/scan?library=beets",
            method="POST",
            headers={"Authorization": f"Bearer {jwt}"},
        )
        with urllib.request.urlopen(scan_req, timeout=10) as resp:
            resp.read()
        logger.info("MEELO: triggered beets library scan")
    except Exception as e:
        logger.warning(f"MEELO: scan trigger failed: {e}")


def log_validation_result(album_data, result, dest_path=None):
    """Append beets validation result to tracking JSONL."""
    entry = {
        "timestamp": datetime.now().isoformat(),
        "artist": album_data.artist,
        "album": album_data.title,
        "mb_release_id": album_data.mb_release_id,
        "album_id": album_data.album_id,
        "status": "staged" if result["valid"] else "rejected",
        "scenario": result.get("scenario", ""),
        "distance": result.get("distance"),
        "detail": result.get("detail", ""),
        "dest_path": dest_path,
        "error": result.get("error"),
    }
    try:
        with open(cfg.beets_tracking_file, "a") as f:
            f.write(json.dumps(entry) + "\n")
    except Exception:
        logger.exception(f"Failed to write beets tracking entry")


def is_docker():
    return os.getenv("IN_DOCKER") is not None


def slskd_version_check(version, target="0.22.2"):
    version_tuple = tuple(map(int, version.split(".")[:3]))
    target_tuple = tuple(map(int, target.split(".")[:3]))
    return version_tuple > target_tuple


def setup_logging(config):
    if "Logging" in config:
        log_config = config["Logging"]
    else:
        log_config = DEFAULT_LOGGING_CONF
    logging.basicConfig(**log_config)  # type: ignore


def load_search_denylist(file_path):
    if not os.path.exists(file_path):
        return {}

    try:
        with open(file_path, "r") as file:
            return json.load(file)
    except (json.JSONDecodeError, IOError) as ex:
        logger.warning(f"Error loading search denylist: {ex}. Starting with empty denylist.")
        return {}


def save_search_denylist(file_path, denylist):
    try:
        with open(file_path, "w") as file:
            json.dump(denylist, file, indent=2)
    except IOError as ex:
        logger.error(f"Error saving search denylist: {ex}")


def is_search_denylisted(denylist, album_id, max_failures):
    album_key = str(album_id)
    if album_key in denylist:
        return denylist[album_key]["failures"] >= max_failures
    return False


def update_search_denylist(denylist, album_id, success):
    album_key = str(album_id)
    current_datetime = datetime.now()
    current_datetime_str = current_datetime.strftime("%Y-%m-%dT%H:%M:%S")

    if success:
        if album_key in denylist:
            logger.info("Removing album from denylist: %s", denylist[album_key]["album_id"])
            del denylist[album_key]
    else:
        logger.info("Adding album to denylist: " + album_key)
        if album_key in denylist:
            denylist[album_key]["failures"] += 1
            denylist[album_key]["last_attempt"] = current_datetime_str
        else:
            denylist[album_key] = {
                "failures": 1,
                "last_attempt": current_datetime_str,
                "album_id": album_id,
            }


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
