#!/usr/bin/env python

import argparse
import math
import re
import unicodedata
import os
import sys
import time
import subprocess as sp
import shutil
import difflib
import operator
import configparser
import logging
import json
import urllib.request
import urllib.error
from datetime import datetime
import copy
import requests
import music_tag
import slskd_api
from pyarr import LidarrAPI
from slskd_api.apis import users


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

# === API Clients & Logging ===
lidarr = None
slskd = None
config = None
logger = logging.getLogger("soularr")

# === Configuration Constants ===
slskd_api_key = None
lidarr_api_key = None
lidarr_download_dir = None
lidarr_disable_sync = None
slskd_download_dir = None
lidarr_host_url = None
slskd_host_url = None
stalled_timeout = None
remote_queue_timeout = None
delete_searches = None
slskd_url_base = None
ignored_users = []
search_type = None
search_source = None
download_filtering = None
use_extension_whitelist = None
extensions_whitelist = []
search_sources = []
minimum_match_ratio = None
page_size = None
remove_wanted_on_failure = None
enable_search_denylist = None
max_search_failures = None
use_most_common_tracknum = None
allow_multi_disc = None
accepted_countries = []
skip_region_check = None
accepted_formats = []
allowed_filetypes = []
lock_file_path = None
config_file_path = None
failure_file_path = None
current_page_file_path = None
denylist_file_path = None
cutoff_denylist_file_path = None
search_blacklist = []

# === Beets Validation Config ===
beets_validation_enabled = False
beets_harness_path = ""
beets_distance_threshold = 0.15
beets_staging_dir = "/mnt/virtio/Music/AI"
audio_check_mode = "normal"  # strict | normal | off
beets_tracking_file = "/mnt/virtio/Music/Re-download/beets-validated.jsonl"

# === Pipeline DB Config ===
pipeline_db_enabled = False
pipeline_db_dsn = "postgresql://soularr@localhost/soularr"
pipeline_db_source = None  # DatabaseSource instance when enabled

# === Meelo Config ===
meelo_url = None  # e.g. "http://192.168.1.29:5001" — set via config to enable post-import scan
meelo_username = None
meelo_password = None

# === Runtime State & Caches ===
search_cache = {}
folder_cache = {}
user_upload_speed = {}  # username → upload speed in bytes/sec (from search results)
broken_user = []
cutoff_denylist = {}  # album_id → set of usernames that provided bad quality
download_fail_counts = {}  # "album_id:username" → int, denylist after 3 failures


def album_match(lidarr_tracks, slskd_tracks, username, filetype):
    counted = []
    total_match = 0.0

    lidarr_album = get_album_by_id(lidarr_tracks[0]["albumId"])
    lidarr_album_name = lidarr_album["title"]
    lidarr_artist_name = lidarr_album["artist"]["artistName"]

    for lidarr_track in lidarr_tracks:
        lidarr_filename = lidarr_track["title"] + "." + filetype.split(" ")[0]
        best_match = 0.0

        for slskd_track in slskd_tracks:
            slskd_filename = slskd_track["filename"]

            # Try to match the ratio with the exact filenames
            ratio = difflib.SequenceMatcher(None, lidarr_filename, slskd_filename).ratio()

            # If ratio is a bad match try and split off (with " " as the separator) the garbage at the start of the slskd_filename and try again
            ratio = check_ratio(" ", ratio, lidarr_filename, slskd_filename)
            # Same but with "_" as the separator
            ratio = check_ratio("_", ratio, lidarr_filename, slskd_filename)

            # Same checks but preappend album name.
            ratio = check_ratio("", ratio, lidarr_album_name + " " + lidarr_filename, slskd_filename)
            ratio = check_ratio(" ", ratio, lidarr_album_name + " " + lidarr_filename, slskd_filename)
            ratio = check_ratio("_", ratio, lidarr_album_name + " " + lidarr_filename, slskd_filename)

            if ratio > best_match:
                best_match = ratio

        if best_match > minimum_match_ratio:
            counted.append(lidarr_filename)
            total_match += best_match

    if len(counted) == len(lidarr_tracks) and username not in ignored_users:
        logger.info(f"Found match from user: {username} for {len(counted)} tracks! Track attributes: {filetype}")
        logger.info(f"Average sequence match ratio: {total_match / len(counted)}")
        logger.info("SUCCESSFUL MATCH")
        logger.info("-------------------")
        return True

    return False


def check_ratio(separator, ratio, lidarr_filename, slskd_filename):
    if ratio < minimum_match_ratio:
        if separator != "":
            lidarr_filename_word_count = len(lidarr_filename.split()) * -1
            truncated_slskd_filename = " ".join(slskd_filename.split(separator)[lidarr_filename_word_count:])
            ratio = difflib.SequenceMatcher(None, lidarr_filename, truncated_slskd_filename).ratio()
        else:
            ratio = difflib.SequenceMatcher(None, lidarr_filename, slskd_filename).ratio()

        return ratio
    return ratio


def album_track_num(directory):
    files = directory["files"]
    allowed_filetypes_no_attributes = [item.split(" ")[0] for item in allowed_filetypes]
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
            slskd.transfers.cancel_download(username=file["username"], id=file["id"])
        except Exception:
            logger.warning(f"Failed to cancel download {file['filename']} for {file['username']}", exc_info=True)
        delete_dir = file["file_dir"].split("\\")[-1]
        os.chdir(slskd_download_dir)

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

    # Prefer the release Lidarr has marked as monitored — this is the one the
    # user explicitly selected in the UI and represents the edition they want.
    for release in releases:
        if not release.get("monitored", False):
            continue
        country = release["country"][0] if release["country"] else None
        if release["format"][1] == "x" and allow_multi_disc:
            format_accepted = release["format"].split("x", 1)[1] in accepted_formats
        else:
            format_accepted = release["format"] in accepted_formats
        if format_accepted:
            logger.info(
                f"Selected monitored release for {artist_name}: {release['status']}, "
                f"{country}, {release['format']}, Mediums: {release['mediumCount']}, "
                f"Tracks: {release['trackCount']}, ID: {release['id']}"
            )
            return release

    for release in releases:
        country = release["country"][0] if release["country"] else None

        if release["format"][1] == "x" and allow_multi_disc:
            format_accepted = release["format"].split("x", 1)[1] in accepted_formats
        else:
            format_accepted = release["format"] in accepted_formats

        if use_most_common_tracknum:
            if release["trackCount"] == most_common_trackcount:
                track_count_bool = True
            else:
                track_count_bool = False
        else:
            track_count_bool = True

        if (skip_region_check or country in accepted_countries) and format_accepted and release["status"] == "Official" and track_count_bool:
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

    if use_most_common_tracknum:
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
    if download_filtering:
        whitelist = []  # Init an empty list to take just the allowed_filetype
        if use_extension_whitelist:
            whitelist = copy.deepcopy(extensions_whitelist)  # Copy the whitelist to allow us to append the allowed_filetype
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
    blacklist = config.get("Search Settings", "title_blacklist", fallback="").lower().split(",")
    for word in blacklist:
        if word != "" and word in title.lower():
            logger.info(f"Skipping {title} due to blacklisted word: {word}")
            return True
    return False


def filter_list(albums):
    """
    Helper to do all the various filtering in one go and in one place. Same net effect as the previous multi-stage approach
    Just neater and easier to work on.
    """
    if enable_search_denylist:
        temp_list = []
        denylist = load_search_denylist(denylist_file_path)
        for album in albums:
            if not is_search_denylisted(denylist, album["id"], max_search_failures):
                temp_list.append(album)
            else:
                logger.info(f"Skipping denylisted album: {album['artist']['artistName']} - {album['title']} (ID: {album['id']})")
    else:
        temp_list = copy.deepcopy(albums)

    list_to_download = []
    for album in temp_list:
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
    album_title = album["title"]
    artist_name = album["artist"]["artistName"]
    album_id = album["id"]
    if len(album_title) == 1:
        query = artist_name + " " + album_title
    elif len(artist_name) <= 3:
        # Short artist names (e.g. "AFI", "REM") poison Soulseek searches —
        # the short token gets dropped or mismatched, causing 0 results.
        # Skip prepending and search by album title alone.
        query = album_title
    else:
        query = artist_name + " " + album_title if config.getboolean("Search Settings", "album_prepend_artist", fallback=False) else album_title

    original_query = query
    for word in search_blacklist:
        if word:
            # Case-insensitive replacement
            pattern = re.compile(re.escape(word), re.IGNORECASE)
            query = pattern.sub("", query)

    # Clean up double spaces
    query = " ".join(query.split())

    if query != original_query:
        logger.info(f"Filtered search query: '{original_query}' -> '{query}'")

    # Strip special characters that poison Soulseek searches.
    # Apostrophes (e.g. "Pink's"), underscores (e.g. "Euro_EP"),
    # and other punctuation cause 0 results or wrong matches.
    clean = re.sub(r"['\"\(\)\[\]\{\}!@#$%^&*_]", " ", query)
    clean = " ".join(clean.split())  # collapse any double spaces
    if clean != query:
        logger.info(f"Stripped special characters from search: '{query}' -> '{clean}'")
        query = clean

    # Soulseek silently drops search tokens with <= 2 characters.
    # Strip them proactively to avoid wasting a token slot.
    # e.g. "A Tribe Called Quest" → "Tribe Called Quest"
    # NOTE: Do NOT strip 3-char tokens (e.g. "New", "All", "Raw") —
    # they are meaningful and Soulseek matches them fine.
    tokens = query.split()
    long_tokens = [t for t in tokens if len(t) > 2]
    if long_tokens:
        stripped = " ".join(long_tokens)
        if stripped != query:
            logger.info(
                f"Stripped short tokens from search: '{query}' -> '{stripped}'"
            )
            query = stripped
        tokens = query.split()

    # Soulseek's distributed search returns 0 results when queries have
    # too many tokens (typically > 4). Common words like "Post" appear in
    # millions of paths; intersecting too many token result sets causes
    # server-side timeouts. Cap at 4 tokens, dropping the shortest
    # (most common/ambiguous) ones first.
    # e.g. "Animal Collective Merriweather Post Pavilion"
    #    → "Animal Collective Merriweather Pavilion" (drops "Post")
    MAX_SEARCH_TOKENS = 4
    if len(tokens) > MAX_SEARCH_TOKENS:
        # Sort by length descending, keep the longest (most distinctive) tokens
        kept = sorted(tokens, key=len, reverse=True)[:MAX_SEARCH_TOKENS]
        # Restore original word order
        capped = [t for t in tokens if t in kept]
        # Handle duplicates: only keep as many as we selected
        seen = {}
        ordered = []
        for t in capped:
            count = seen.get(t, 0)
            if count < kept.count(t):
                ordered.append(t)
                seen[t] = count + 1
        capped_query = " ".join(ordered[:MAX_SEARCH_TOKENS])
        logger.info(
            f"Capped search tokens ({len(tokens)} -> {MAX_SEARCH_TOKENS}): "
            f"'{query}' -> '{capped_query}'"
        )
        query = capped_query
        tokens = query.split()

    # When ALL tokens are short (e.g. "AFI AFI", "REM Up"), the search
    # returns 0 results. Fall back to searching for the longest track
    # title from the album — the existing check_for_match logic will
    # still validate that the returned directories contain the full album.
    if tokens and all(len(t) <= 3 for t in tokens):
        try:
            releases = album.get("releases") or lidarr.get_album(album_id)["releases"]
            release = choose_release(artist_name, releases)
            tracks = get_album_tracks(album, release_id=release["id"])
            if tracks:
                best_track = max(tracks, key=lambda t: len(t["title"]))
                if len(best_track["title"]) > 3:
                    logger.info(
                        f"Short-name fallback for '{query}': all tokens <= 3 chars, "
                        f"searching for track '{best_track['title']}' instead"
                    )
                    query = best_track["title"]
                else:
                    logger.warning(
                        f"Cannot search for '{artist_name} - {album_title}': "
                        f"all query tokens and track names are <= 3 chars"
                    )
                    return False
        except Exception:
            logger.exception(
                f"Short-name fallback failed for '{artist_name} - {album_title}'"
            )
            return False

    logger.info(f"Searching for album: {query}")
    try:
        search = slskd.searches.search_text(
            searchText=query,
            searchTimeout=config.getint("Search Settings", "search_timeout", fallback=5000),
            filterResponses=True,
            maximumPeerQueueLength=config.getint("Search Settings", "maximum_peer_queue", fallback=50),
            minimumPeerUploadSpeed=config.getint("Search Settings", "minimum_peer_upload_speed", fallback=0),
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
        if (time.time() - start_time) > config.getint("Search Settings", "search_timeout", fallback=5000):
            logger.error("Failed to perform search via SLSKD due to timeout on search results.")
            return False

    search_results = slskd.searches.search_responses(search["id"])  # We use this API call twice. Let's just cache it locally.
    logger.info(f"Search returned {len(search_results)} results")
    if delete_searches:
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
            for allowed_filetype in allowed_filetypes:
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
                            file_details = {}
                            file_details["filename"] = file["filename"]
                            file_details["id"] = slskd_file["id"]
                            file_details["file_dir"] = file_dir
                            file_details["username"] = username
                            file_details["size"] = file["size"]
                            downloads.append(file_details)
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
            status = slskd.transfers.get_download(file["username"], file["id"])
            file["status"] = status
        except Exception:
            logger.exception(f"Error getting download status of {file['filename']}")
            file["status"] = None
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
        if file["status"] is not None:
            if not file["status"]["state"] == "Completed, Succeeded":
                all_done = False
            if file["status"]["state"] in [
                "Completed, Cancelled",
                "Completed, TimedOut",
                "Completed, Errored",
                "Completed, Rejected",
                "Completed, Aborted",
            ]:
                error_list.append(file)
            if file["status"]["state"] == "Queued, Remotely":
                remote_queue += 1
    if not len(error_list) > 0:
        error_list = None
    return all_done, error_list, remote_queue


def _get_denied_users(album_id):
    """Merge file-based cutoff denylist with pipeline DB source_denylist."""
    denied = set(cutoff_denylist.get(album_id, set()))
    # In DB mode, album_id is negative; request_id is the positive counterpart
    if pipeline_db_enabled and pipeline_db_source is not None and album_id < 0:
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
                        file["disk_no"] = disk["disk_no"]
                        file["disk_count"] = disk["disk_count"]
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


def get_existing_quality_tier(album_id):
    """
    For cutoff_unmet albums, determine which allowed_filetypes index matches
    the existing files' quality.  Only filetypes at a LOWER index (higher
    priority) would be a genuine upgrade.

    Returns (index, quality_name) where index is into allowed_filetypes, or
    len(allowed_filetypes) when quality can't be determined (safe fallback —
    all filetypes are tried).  quality_name is the raw Lidarr quality string.

    Lidarr quality names look like "MP3-320", "MP3-192", "FLAC", "FLAC 24bit".
    We map them to allowed_filetypes format: "mp3 320", "mp3 192", "flac", etc.
    """
    fallback = (len(allowed_filetypes), "Unknown")
    try:
        response = requests.get(
            f"{lidarr_host_url}/api/v1/trackfile",
            params={"albumId": album_id},
            headers={"X-Api-Key": lidarr_api_key},
            timeout=10,
        )
        response.raise_for_status()
        track_files = response.json()

        if not track_files:
            return fallback

        # Use the quality reported by Lidarr for the first track file
        quality_name = (
            track_files[0]
            .get("quality", {})
            .get("quality", {})
            .get("name", "")
        )
        if not quality_name:
            return fallback

        # "MP3-320" -> "mp3 320", "FLAC" -> "flac", "FLAC 24bit" -> "flac 24bit"
        # "ALAC" -> "m4a" (ALAC is Apple Lossless in an m4a container)
        # "MP3 VBR V0" -> "mp3 v0", "MP3 VBR V2" -> "mp3 v2"
        mapped = quality_name.lower().replace("-", " ")
        if mapped.startswith("alac"):
            mapped = "m4a" + mapped[4:]  # "alac" -> "m4a", "alac 16/44.1" -> "m4a 16/44.1"
        elif mapped.startswith("mp3 vbr "):
            mapped = "mp3 " + mapped[8:]  # "mp3 vbr v0" -> "mp3 v0"

        # Exact match first (e.g. "mp3 320" in allowed_filetypes)
        matched_index = None
        for i, ft in enumerate(allowed_filetypes):
            if ft.strip().lower() == mapped:
                matched_index = i
                break

        # Bare-format fallback (e.g. "mp3 192" not in list → match bare "mp3")
        if matched_index is None:
            bare_format = mapped.split()[0] if " " in mapped else mapped
            for i, ft in enumerate(allowed_filetypes):
                if ft.strip().lower() == bare_format:
                    matched_index = i
                    break

        if matched_index is None:
            return fallback

        # When Lidarr reports a bare format (e.g. "FLAC" with no bitrate/depth),
        # it can't distinguish sub-variants (flac 24/192 vs flac 16/44.1).
        # Downloading any FLAC variant would import as "FLAC" again → infinite
        # loop.  Prevent this by returning the index of the FIRST entry with the
        # same base format, so all same-format variants are excluded.
        if " " not in mapped:
            for i in range(matched_index):
                if allowed_filetypes[i].strip().lower().split()[0] == mapped:
                    logger.info(
                        f"Bare format '{quality_name}' detected — excluding "
                        f"all '{mapped}' variants (indices {i}-{matched_index})"
                    )
                    return (i, quality_name)
        return (matched_index, quality_name)
    except Exception:
        logger.debug(
            f"Could not determine existing quality for album {album_id}",
            exc_info=True,
        )
        return fallback


def get_album_tracks(album, release_id=None):
    """Get tracks for an album — from pipeline DB or Lidarr API.

    DB records have negative IDs. For those, fetch tracks from DatabaseSource.
    For Lidarr records (positive IDs), use lidarr.get_tracks() as before.
    """
    if pipeline_db_enabled and pipeline_db_source is not None and album.get("_db_request_id"):
        return pipeline_db_source.get_tracks(album)
    return lidarr.get_tracks(
        artistId=album["artistId"],
        albumId=album["id"],
        albumReleaseId=release_id,
    )


# Cache for current album being processed — avoids lidarr.get_album() for DB records
_current_album_cache = {}


def get_album_by_id(album_id):
    """Get album data by ID — from cache (DB mode) or Lidarr API."""
    if album_id in _current_album_cache:
        return _current_album_cache[album_id]
    return lidarr.get_album(album_id)


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

    # Cache album for DB mode so try_enqueue doesn't need lidarr.get_album()
    _current_album_cache[album_id] = album

    # For cutoff_unmet albums, only try filetypes that are strictly better
    # than what Lidarr already has.  This prevents re-downloading the same
    # quality in an endless loop (e.g. mp3-192 replacing mp3-192).
    is_cutoff = album.get("_is_cutoff", False)
    filetypes_to_try = allowed_filetypes
    if is_cutoff:
        existing_tier, lidarr_quality = get_existing_quality_tier(album_id)
        if existing_tier == 0:
            logger.info(
                f"Already at highest quality tier, skipping cutoff upgrade: "
                f"{artist_name} - {album['title']} (Lidarr quality: {lidarr_quality})"
            )
            return False
        if existing_tier < len(allowed_filetypes):
            filetypes_to_try = allowed_filetypes[:existing_tier]
            logger.info(
                f"Cutoff upgrade for {artist_name} - {album['title']}: "
                f"Lidarr quality '{lidarr_quality}' → tier '{allowed_filetypes[existing_tier]}', "
                f"only trying {len(filetypes_to_try)} higher-priority formats"
            )

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
        # DB records carry releases inline (copy to avoid mutation);
        # Lidarr records fetch fresh from API each iteration
        if album.get("_db_request_id"):
            releases = list(album.get("releases", []))
        else:
            releases = lidarr.get_album(album_id)["releases"]

        # Check if any release is explicitly monitored by the user.
        # When a monitored release exists, we ONLY try that release and skip
        # all others. Rationale: Lidarr imports against the monitored release,
        # so downloading a non-monitored release (different track count/edition)
        # will always fail at import time. This avoids wasted downloads and
        # bandwidth — e.g. downloading an 11-track edition when Lidarr expects
        # a 20-track deluxe. If the monitored release isn't available on
        # Soulseek, the album simply fails and can be retried later or the
        # user can manually select a different release in Lidarr.
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
                grab_list[album_id] = {}
                grab_list[album_id]["files"] = downloads
                grab_list[album_id]["filetype"] = allowed_filetype
                grab_list[album_id]["title"] = album["title"]
                grab_list[album_id]["artist"] = artist_name
                grab_list[album_id]["year"] = album["releaseDate"][0:4]
                grab_list[album_id]["mb_release_id"] = release.get("foreignReleaseId", "")
                if is_cutoff:
                    grab_list[album_id]["_is_cutoff"] = True
                    grab_list[album_id]["_pre_tier"] = existing_tier
                # Propagate DB metadata for pipeline_db_source.mark_done/mark_failed
                if album.get("_db_request_id"):
                    grab_list[album_id]["_db_request_id"] = album["_db_request_id"]
                    grab_list[album_id]["_db_source"] = album.get("_db_source")
                    grab_list[album_id]["_db_quality_override"] = album.get("_db_quality_override")
                return True
            elif len(release["media"]) > 1:
                found, downloads = try_multi_enqueue(release, all_tracks, results, allowed_filetype)
                if found:
                    grab_list[album_id] = {}
                    grab_list[album_id]["files"] = downloads
                    grab_list[album_id]["filetype"] = allowed_filetype
                    grab_list[album_id]["title"] = album["title"]
                    grab_list[album_id]["artist"] = artist_name
                    grab_list[album_id]["year"] = album["releaseDate"][0:4]
                    grab_list[album_id]["mb_release_id"] = release.get("foreignReleaseId", "")
                    if is_cutoff:
                        grab_list[album_id]["_is_cutoff"] = True
                        grab_list[album_id]["_pre_tier"] = existing_tier
                    if album.get("_db_request_id"):
                        grab_list[album_id]["_db_request_id"] = album["_db_request_id"]
                        grab_list[album_id]["_db_source"] = album.get("_db_source")
                        grab_list[album_id]["_db_quality_override"] = album.get("_db_quality_override")
                    return True

            # If a monitored release was tried and didn't match, stop here.
            # Don't fall through to other releases — Lidarr will reject them
            # at import because they won't match the monitored edition.
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


QUALITY_UPGRADE_TIERS = "flac,mp3 v0,mp3 320"
QUALITY_MIN_BITRATE_KBPS = 210  # V0 floor — below this triggers upgrade


def _check_quality_gate(album_data, request_id):
    """Post-import quality gate: if min track bitrate is below V0, queue for upgrade."""
    mb_id = album_data.get("mb_release_id")
    if not mb_id or not pipeline_db_enabled or pipeline_db_source is None:
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
        conn.close()
        if not br_row or not br_row[0]:
            return
        min_br_kbps = int(br_row[0] / 1000)
        # Use spectral_bitrate for quality gate if available (catches fake 320s)
        gate_br = min_br_kbps
        spectral_br = None
        if request_id:
            try:
                req = pipeline_db_source._get_db().get_request(request_id)
                spectral_br = req.get("spectral_bitrate") if req else None
                if spectral_br and spectral_br < gate_br:
                    gate_br = spectral_br
                    logger.info(f"QUALITY GATE: using spectral_bitrate={spectral_br}kbps "
                                f"(lower than beets min_bitrate={min_br_kbps}kbps)")
            except Exception:
                pass
        if gate_br < QUALITY_MIN_BITRATE_KBPS:
            db = pipeline_db_source._get_db()
            db.reset_to_wanted(request_id,
                               quality_override=QUALITY_UPGRADE_TIERS,
                               min_bitrate=min_br_kbps)
            # Denylist the source user(s) so we don't re-download same quality
            usernames = set(f.get("username") for f in album_data.get("files", [])
                           if f.get("username"))
            for username in usernames:
                db.add_denylist(request_id, username,
                                f"quality gate: {min_br_kbps}kbps < {QUALITY_MIN_BITRATE_KBPS}kbps")
            spectral_note = f" (spectral={spectral_br}kbps)" if spectral_br else ""
            logger.info(
                f"QUALITY GATE: {album_data['artist']} - {album_data['title']} "
                f"gate_bitrate={gate_br}kbps{spectral_note} < {QUALITY_MIN_BITRATE_KBPS}kbps, "
                f"queued for upgrade, denylisted {usernames} "
                f"(searching {QUALITY_UPGRADE_TIERS})")
        else:
            # Update min_bitrate to current on-disk value (quality gate passed)
            db = pipeline_db_source._get_db()
            update_fields = {"min_bitrate": min_br_kbps}
            if spectral_br:
                update_fields["spectral_bitrate"] = spectral_br
            db.update_status(request_id, "imported", **update_fields)
            logger.info(
                f"QUALITY GATE: {album_data['artist']} - {album_data['title']} "
                f"min_bitrate={min_br_kbps}kbps — quality OK")
    except Exception:
        logger.exception("QUALITY GATE: failed to check quality")


def _build_download_info(album_data):
    """Extract audio quality metadata from album files for download logging."""
    files = album_data.get("files", [])
    if not files:
        return {}
    usernames = set(f.get("username") for f in files if f.get("username"))
    filetypes = set(f["filename"].split(".")[-1].lower() for f in files if "." in f["filename"])
    bitrates = [f["bitRate"] for f in files if "bitRate" in f]
    sample_rates = [f["sampleRate"] for f in files if "sampleRate" in f]
    bit_depths = [f["bitDepth"] for f in files if "bitDepth" in f]
    vbr_flags = [f["isVariableBitRate"] for f in files if "isVariableBitRate" in f]

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


def process_completed_album(album_data, failed_grab):
    os.chdir(slskd_download_dir)
    import_folder_name = sanitize_folder_name(album_data["artist"] + " - " + album_data["title"] + " (" + album_data["year"] + ")")
    import_folder_fullpath = os.path.join(slskd_download_dir, import_folder_name)
    lidarr_import_fullpath = os.path.join(lidarr_download_dir, import_folder_name)
    album_data["import_folder"] = lidarr_import_fullpath
    rm_dirs = []
    moved_files_history = []
    if not os.path.exists(import_folder_fullpath):
        os.mkdir(import_folder_fullpath)
    for file in album_data["files"]:
        file_folder = file["file_dir"].split("\\")[-1]
        filename = file["filename"].split("\\")[-1]
        src_folder = os.path.join(slskd_download_dir, file_folder)
        if src_folder not in rm_dirs:
            rm_dirs.append(src_folder)  # Multi disk albums are sometimes in multiple folders. eg. CD01 CD02. So we need to clean up both
        src_file = os.path.join(src_folder, filename)
        if "disk_no" in file and "disk_count" in file and file["disk_count"] > 1:
            filename = f"Disk {file['disk_no']} - {filename}"
        dst_file = os.path.join(import_folder_fullpath, filename)
        file["import_path"] = dst_file
        try:
            shutil.move(src_file, dst_file)
            moved_files_history.append((src_file, dst_file))
        except Exception:
            logger.exception(f"Failed to move: {file['filename']} to temp location for import into Lidarr. Rolling back...")
            for src, dst in reversed(moved_files_history):
                try:
                    shutil.move(dst, src)
                except Exception:
                    logger.exception(f"Critical failure during rollback: could not move {dst} back to {src}")
            try:
                os.rmdir(import_folder_fullpath)
            except OSError:
                logger.warning(f"Could not remove temp import directory {import_folder_fullpath}")
            if album_data["album_id"] > 0:
                failed_grab.append(lidarr.get_album(album_data["album_id"]))
            return
    else:  # Only runs if all files are successfully moved
        for rm_dir in rm_dirs:
            if not rm_dir == import_folder_fullpath:
                try:
                    os.rmdir(rm_dir)
                except OSError:
                    logger.warning(f"Skipping removal of {rm_dir} because it's not empty.")
        logger.info(f"Attempting Lidarr import of {album_data['artist']} - {album_data['title']}")
        for file in album_data["files"]:
            try:  # This sometimes fails. No idea why. Nor do we care. We try and that's what matters
                song = music_tag.load_file(file["import_path"])
                if "disk_no" in file:
                    song["discnumber"] = file["disk_no"]
                    song["totaldiscs"] = file["disk_count"]

                song["albumartist"] = album_data["artist"]
                song["album"] = album_data["title"]
                song.save()
            except Exception:
                logger.exception(f"Error writing tags for: {file['import_path']}")
        if beets_validation_enabled and album_data.get("mb_release_id"):
            # === Beets validation path ===
            bv_result = beets_validate(import_folder_fullpath,
                                       album_data["mb_release_id"],
                                       beets_distance_threshold)

            is_db_mode = pipeline_db_enabled and pipeline_db_source is not None

            if bv_result["valid"]:
                # Repair MP3 headers before audio validation
                repair_mp3_headers(import_folder_fullpath)
                # Audio integrity check before staging
                audio_result = validate_audio(import_folder_fullpath, audio_check_mode)
                if not audio_result["valid"]:
                    bv_result = {
                        "valid": False,
                        "distance": bv_result.get("distance"),
                        "mbid_found": bv_result.get("mbid_found"),
                        "scenario": "audio_corrupt",
                        "detail": audio_result["error"],
                        "error": None,
                    }

            if bv_result["valid"]:
                dest = stage_to_ai(album_data, import_folder_fullpath, beets_staging_dir)
                log_validation_result(album_data, bv_result, dest)
                if not is_db_mode:
                    unmonitor_album(album_data["album_id"])
                logger.info(f"STAGED: {album_data['artist']} - {album_data['title']} "
                            f"(scenario={bv_result.get('scenario')}, "
                            f"distance={bv_result['distance']:.4f}) → {dest}")

                # Pipeline DB: two-track post-download pipeline
                if is_db_mode:
                    dl_info = _build_download_info(album_data)
                    source_type = album_data.get("_db_source", "redownload")
                    request_id = album_data.get("_db_request_id")
                    if source_type == "request" and bv_result.get("distance", 1.0) <= beets_distance_threshold:
                        # Auto-import via import_one.py (handles convert + import + DB updates)
                        import_script = os.path.join(
                            os.path.dirname(beets_harness_path), "import_one.py")
                        mb_id = album_data.get("mb_release_id", "")
                        logger.info(f"AUTO-IMPORT: {album_data['artist']} - {album_data['title']} "
                                    f"(source=request, dist={bv_result['distance']:.4f})")
                        try:
                            cmd = [sys.executable, import_script, dest, mb_id]
                            if request_id:
                                cmd.extend(["--request-id", str(request_id)])
                            import_env = {**os.environ, "HOME": "/home/abl030"}
                            result = sp.run(cmd, capture_output=True, text=True,
                                            timeout=1800, env=import_env)
                            if result.returncode == 0:
                                logger.info(f"AUTO-IMPORT OK: {album_data['artist']} - {album_data['title']}")
                                # Log beets internal logging (stderr)
                                for line in (result.stderr or "").strip().split("\n"):
                                    if line.strip():
                                        logger.info(f"  [beets] {line}")
                                prev_min_br = None
                                new_min_br = None
                                for line in result.stdout.strip().split("\n"):
                                    logger.info(f"  {line}")
                                    # Detect FLAC→V0 conversion from import_one output
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
                                # Clean up staged directory — beets already moved files
                                # to /Beets, or pre-flight found a dupe (files unneeded)
                                if os.path.isdir(dest):
                                    shutil.rmtree(dest)
                                    logger.info(f"  Cleaned up staged dir: {dest}")
                                    parent = os.path.dirname(dest)
                                    if os.path.isdir(parent) and not os.listdir(parent):
                                        os.rmdir(parent)
                                        logger.info(f"  Cleaned up empty artist dir: {parent}")
                            elif result.returncode == 5:
                                # Quality downgrade — not imported
                                logger.warning(
                                    f"QUALITY DOWNGRADE PREVENTED: {album_data['artist']} - {album_data['title']}")
                                for line in result.stderr.strip().split("\n"):
                                    logger.warning(f"  {line}")
                                usernames = set(f.get("username") for f in album_data.get("files", [])
                                                if f.get("username"))
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
                                        f"TRANSCODE UPGRADE: {album_data['artist']} - {album_data['title']} "
                                        f"imported at {actual_br}kbps, denylisting + continuing search")
                                    pipeline_db_source.mark_done(album_data, bv_result, dest_path=dest, download_info=dl_info)
                                    trigger_meelo_scan()
                                else:
                                    logger.warning(
                                        f"TRANSCODE REJECTED: {album_data['artist']} - {album_data['title']} "
                                        f"at {actual_br}kbps — not an upgrade")
                                # Denylist source user and reset to wanted for further searching
                                usernames = set(f.get("username") for f in album_data.get("files", [])
                                                if f.get("username"))
                                db = pipeline_db_source._get_db()
                                reason = f"transcode: {actual_br}kbps" if actual_br else "transcode detected"
                                for username in usernames:
                                    db.add_denylist(request_id, username, reason)
                                logger.info(f"  Denylisted {usernames} for request {request_id}")
                                # Reset to wanted so we keep searching for better quality
                                # Only update min_bitrate if we actually imported — otherwise
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
                                             f"{album_data['artist']} - {album_data['title']}")
                                for line in result.stderr.strip().split("\n"):
                                    logger.error(f"  {line}")
                                for line in result.stdout.strip().split("\n"):
                                    logger.error(f"  {line}")
                                # import_one.py already set DB to review_needed
                        except sp.TimeoutExpired:
                            logger.error(f"AUTO-IMPORT TIMEOUT: {album_data['artist']} - {album_data['title']}")
                        except Exception:
                            logger.exception(f"AUTO-IMPORT ERROR: {album_data['artist']} - {album_data['title']}")
                    else:
                        # Redownload or high distance: stage only, user reviews manually
                        pipeline_db_source.mark_done(album_data, bv_result, dest_path=dest, download_info=dl_info)
            else:
                move_failed_import(import_folder_fullpath)
                log_validation_result(album_data, bv_result)
                usernames = set(f["username"] for f in album_data.get("files", []))
                # Pipeline DB: mark failed + denylist users
                if is_db_mode:
                    dl_info = _build_download_info(album_data)
                    pipeline_db_source.mark_failed(album_data, bv_result, usernames=usernames, download_info=dl_info)
                else:
                    failed_grab.append(lidarr.get_album(album_data["album_id"]))
                # Denylist the source user(s) so we try a different source next run
                aid = album_data["album_id"]
                if aid not in cutoff_denylist:
                    cutoff_denylist[aid] = set()
                cutoff_denylist[aid].update(usernames)
                save_cutoff_denylist(cutoff_denylist_file_path, cutoff_denylist)
                logger.warning(f"REJECTED: {album_data['artist']} - {album_data['title']} "
                              f"(scenario={bv_result.get('scenario')}, "
                              f"distance={bv_result.get('distance')}, "
                              f"detail={bv_result.get('detail')}) "
                              f"| denylisted users: {', '.join(usernames)}")
        else:
            # === Lidarr DownloadedAlbumsScan path (original) ===
            command = lidarr.post_command(
                name="DownloadedAlbumsScan",
                path=album_data["import_folder"],
            )
            logger.info(f"Starting Lidarr import for: {album_data['title']} ID: {command['id']}")

            while True:
                current_task = lidarr.get_command(command["id"])
                if current_task["status"] == "completed" or current_task["status"] == "failed":
                    break
                time.sleep(2)

            try:
                logger.info(f"{current_task['commandName']} {current_task['message']} from: {current_task['body']['path']}")

                if "Failed" in current_task["message"]:
                    move_failed_import(current_task["body"]["path"])
                    failed_grab.append(lidarr.get_album(album_data["album_id"]))
                elif album_data.get("_is_cutoff"):
                    post_tier, post_quality = get_existing_quality_tier(album_data["album_id"])
                    pre_tier = album_data.get("_pre_tier", len(allowed_filetypes))
                    if post_tier >= pre_tier:
                        usernames = set(f["username"] for f in album_data.get("files", []))
                        aid = album_data["album_id"]
                        logger.warning(
                            f"Cutoff upgrade failed for {album_data['artist']} - {album_data['title']}: "
                            f"quality still '{post_quality}' after import (tier {post_tier} >= {pre_tier}). "
                            f"Source users likely had mislabeled files: {', '.join(usernames)}. "
                            f"Denylisting user/album pairs permanently."
                        )
                        if aid not in cutoff_denylist:
                            cutoff_denylist[aid] = set()
                        cutoff_denylist[aid].update(usernames)
                        save_cutoff_denylist(cutoff_denylist_file_path, cutoff_denylist)
                    else:
                        logger.info(
                            f"Cutoff upgrade verified for {album_data['artist']} - {album_data['title']}: "
                            f"quality improved to '{post_quality}' (tier {pre_tier} → {post_tier})"
                        )
            except Exception:
                logger.exception("Error printing lidarr task message")
                logger.error(current_task)


def monitor_downloads(grab_list, failed_grab):
    def delete_album(reason):
        cancel_and_delete(grab_list[album_id]["files"])
        usernames = set(f.get("username") for f in grab_list[album_id].get("files", []) if f.get("username"))
        files = grab_list[album_id]["files"]
        total = len(files)
        completed = sum(1 for f in files if f.get("status") and f["status"].get("state") == "Completed, Succeeded")
        elapsed = time.time() - grab_list[album_id].get("count_start", time.time())
        elapsed_min = elapsed / 60
        logger.info(f"{reason} Album: {grab_list[album_id]['title']} Artist: {grab_list[album_id]['artist']} "
                     f"({completed}/{total} files done, {elapsed_min:.1f}min elapsed, "
                     f"stalled_timeout={stalled_timeout}s, remote_queue_timeout={remote_queue_timeout}s)")
        for username in usernames:
            key = f"{album_id}:{username}"
            download_fail_counts[key] = download_fail_counts.get(key, 0) + 1
            save_download_fail_counts(cutoff_denylist_file_path, download_fail_counts)
            if download_fail_counts[key] >= 3:
                if album_id not in cutoff_denylist:
                    cutoff_denylist[album_id] = set()
                cutoff_denylist[album_id].add(username)
                save_cutoff_denylist(cutoff_denylist_file_path, cutoff_denylist)
                logger.info(f"Denylisted user '{username}' for album {album_id} after {download_fail_counts[key]} failures")
            else:
                logger.info(f"Download failure {download_fail_counts[key]}/3 for user '{username}' on album {album_id}")
        del grab_list[album_id]
        if album_id > 0:  # Lidarr IDs are positive; DB IDs are negative
            failed_grab.append(lidarr.get_album(album_id))

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
            if slskd_download_status(grab_list[album_id]["files"]):
                album_done, problems, queued = downloads_all_done(grab_list[album_id]["files"])  # Lets check to see what status the files have
                if "count_start" not in grab_list[album_id]:
                    grab_list[album_id]["count_start"] = time.time()
                if (time.time() - grab_list[album_id]["count_start"]) >= stalled_timeout:  # Album is taking too long. Bail out regardless
                    delete_album("Timeout waiting for download of")
                    continue
                if queued == len(grab_list[album_id]["files"]):  # Shorter time out for whole albums in "Queued, Remotely"
                    if (time.time() - grab_list[album_id]["count_start"]) >= remote_queue_timeout:
                        delete_album("Timeout waiting for download of")
                        continue
                # Also timeout when most files are done but some are stuck queued remotely
                if queued > 0 and done_count > 0:
                    completed = sum(1 for f in grab_list[album_id]["files"]
                                    if f.get("status") and f["status"]["state"] == "Completed, Succeeded")
                    if completed + queued == len(grab_list[album_id]["files"]):
                        # All files are either done or stuck — apply remote_queue_timeout
                        if (time.time() - grab_list[album_id]["count_start"]) >= remote_queue_timeout:
                            delete_album("Timeout waiting for stuck remote queue file in")
                            continue
                done_count += album_done
                if problems is not None:
                    logger.debug("We got problems!")
                    for file in problems:
                        logger.debug(f"Checking {file['filename']}")
                        match file["status"]["state"]:
                            case (
                                "Completed, Cancelled" | "Completed, TimedOut" | "Completed, Errored" | "Completed, Aborted"
                            ):  # Normal errors. We'll retry a few times as sometumes the error is transient
                                abort = False
                                if len(problems) == len(grab_list[album_id]["files"]):
                                    delete_album("Failed grab of")
                                    break
                                for download_file in grab_list[album_id]["files"]:
                                    if file["filename"] == download_file["filename"]:
                                        if "retry" not in download_file:
                                            download_file["retry"] = 0
                                        download_file["retry"] += 1
                                        if download_file["retry"] < 5:
                                            retry = download_file["retry"]
                                            size = file["size"]
                                            data_dict = [{"filename": file["filename"], "size": size}]
                                            logger.info(f"Download error. Requeue file: {file['filename']}")
                                            requeue = slskd_do_enqueue(
                                                file["username"],
                                                data_dict,
                                                file["file_dir"],
                                            )
                                            if requeue is not None:
                                                download_file["id"] = requeue[0]["id"]
                                                download_file["retry"] = retry
                                                time.sleep(1)
                                                _ = slskd_download_status(grab_list[album_id]["files"])  # Refresh the status of the files to prevent issues.
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
                                if len(problems) == len(grab_list[album_id]["files"]):
                                    delete_album("Failed grab of")  # They are all rejected. Usually this happens because of misconfigurations. Files appear in search but aren't shared.
                                    break
                                else:
                                    if "rejected_retries" not in grab_list[album_id]:
                                        grab_list[album_id]["rejected_retries"] = 0
                                    working_count = len(grab_list[album_id]["files"]) - len(problems)
                                    for gfile in grab_list[album_id]["files"]:
                                        if gfile["status"]["state"] in [
                                            "Completed, Succeeded",
                                            "Queued, Remotely",
                                            "Queued, Locally",
                                        ]:
                                            working_count -= 1
                                    if working_count == 0:
                                        if grab_list[album_id]["rejected_retries"] < int(len(grab_list[album_id]["files"]) * 1.2):  # Little bit of wiggle room here
                                            abort = False
                                            for gfile in grab_list[album_id]["files"]:
                                                if gfile["filename"] == file["filename"]:
                                                    size = file["size"]
                                                    data_dict = [
                                                        {
                                                            "filename": file["filename"],
                                                            "size": size,
                                                        }
                                                    ]
                                                    logger.info(f"Download error. Requeue file: {file['filename']}")
                                                    requeue = slskd_do_enqueue(
                                                        file["username"],
                                                        data_dict,
                                                        file["file_dir"],
                                                    )
                                                    if requeue is not None:
                                                        gfile["id"] = requeue[0]["id"]
                                                        grab_list[album_id]["rejected_retries"] += 1
                                                        _ = slskd_download_status(grab_list[album_id]["files"])
                                                        abort = True
                                                        break
                                                    else:
                                                        cancel_and_delete(grab_list[album_id]["files"])
                                                        logger.info(f"Failed grab of Album: {grab_list[album_id]['title']} Artist: {grab_list[album_id]['artist']}")
                                                        del grab_list[album_id]
                                                        if album_id > 0:
                                                            failed_grab.append(lidarr.get_album(album_id))  # Not sure if returns an array or not
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
                        album_data = grab_list[album_id]
                        album_data["album_id"] = album_id
                        logger.info(f"Completed download of Album: {album_data['title']} Artist: {album_data['artist']}")
                        process_completed_album(album_data, failed_grab)
                        del grab_list[album_id]

            else:
                if "error_count" not in grab_list[album_id]:
                    grab_list[album_id]["error_count"] = 0
                grab_list[album_id]["error_count"] += 1
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
    When all completed, call lidarr to import
    """

    grab_list, failed_search, failed_grab = search_and_queue(albums)

    total_albums = len(grab_list)
    logger.info(f"Total Downloads added: {total_albums}")
    for album_id in grab_list:
        logger.info(f"Album: {grab_list[album_id]['title']} Artist: {grab_list[album_id]['artist']}")
    logger.info(f"Failed to grab: {len(failed_grab)}")
    for album in failed_grab:
        logger.info(f"Album: {album['title']} Artist: {album['artist']['artistName']}")

    logger.info("-------------------")
    logger.info(f"Waiting for downloads... monitor at: {''.join([slskd_host_url, slskd_url_base, 'downloads'])}")

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
    # if enable_search_denylist:
    #    save_search_denylist(denylist_file_path, search_denylist)


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
    """Dry-run beets import with specific MBID. Returns validation result.

    Uses the same decision matrix as batch_import.py to classify matches.
    Returns: {"valid": bool, "distance": float|None, "mbid_found": bool,
              "scenario": str, "detail": str, "error": str|None}
    """
    cmd = [beets_harness_path, "--pretend", "--noincremental",
           "--search-id", mb_release_id, album_path]
    result = {"valid": False, "distance": None, "mbid_found": False, "error": None}

    logger.info(f"BEETS_VALIDATE: path={album_path}, target_mbid={mb_release_id}, "
                f"threshold={distance_threshold}")
    logger.info(f"BEETS_VALIDATE: cmd={' '.join(cmd)}")

    try:
        proc = sp.Popen(cmd, stdin=sp.PIPE, stdout=sp.PIPE, stderr=sp.PIPE, text=True)
    except Exception as e:
        result["error"] = f"Failed to start harness: {e}"
        logger.error(f"BEETS_VALIDATE: {result['error']}")
        return result

    got_choose_match = False
    try:
        for line in proc.stdout:
            line = line.strip()
            if not line:
                continue
            try:
                msg = json.loads(line)
            except json.JSONDecodeError:
                logger.debug(f"BEETS_VALIDATE: non-JSON line: {line[:200]}")
                continue

            msg_type = msg.get("type", "")
            logger.info(f"BEETS_VALIDATE: msg type={msg_type}")

            if msg_type == "choose_match":
                got_choose_match = True
                candidates = msg.get("candidates", [])
                logger.info(f"BEETS_VALIDATE: {len(candidates)} candidates, "
                            f"looking for mbid={mb_release_id}")
                for i, cand in enumerate(candidates):
                    cand_mbid = cand.get("album_id", "")
                    cand_dist = cand.get("distance", "?")
                    cand_album = cand.get("album", "?")
                    logger.info(f"BEETS_VALIDATE:   candidate[{i}]: "
                                f"mbid={cand_mbid}, dist={cand_dist}, album={cand_album}")
                # Check if target MBID was found and distance is acceptable
                for cand in candidates:
                    if cand.get("album_id") == mb_release_id:
                        result["mbid_found"] = True
                        result["distance"] = cand["distance"]
                        extra_tracks = cand.get("extra_tracks", 0)
                        if extra_tracks > 0:
                            result["scenario"] = "extra_tracks"
                            result["detail"] = f"MB has {extra_tracks} more tracks than local files"
                        elif cand["distance"] <= distance_threshold:
                            result["valid"] = True
                            result["scenario"] = "strong_match"
                            result["detail"] = f"distance={cand['distance']}"
                        else:
                            result["scenario"] = "high_distance"
                            result["detail"] = f"distance={cand['distance']}"
                        break
                if not result["mbid_found"]:
                    result["scenario"] = "mbid_not_found"
                    result["detail"] = f"Target MBID {mb_release_id} not in candidates"
                logger.info(f"BEETS_VALIDATE: valid={result['valid']}, "
                            f"scenario={result['scenario']}, detail={result['detail']}")
                # Always skip (dry-run)
                proc.stdin.write('{"action":"skip"}\n')
                proc.stdin.flush()

            elif msg_type in ("choose_item", "resolve_duplicate", "should_resume"):
                proc.stdin.write('{"action":"skip"}\n')
                proc.stdin.flush()

            elif msg_type == "session_end":
                break
    except Exception as e:
        result["error"] = str(e)
        logger.error(f"BEETS_VALIDATE: exception: {e}")
    finally:
        stderr_out = ""
        try:
            stderr_out = proc.stderr.read()
        except Exception:
            pass
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except sp.TimeoutExpired:
            proc.kill()

    if stderr_out:
        logger.warning(f"BEETS_VALIDATE: stderr: {stderr_out[:500]}")
    if not got_choose_match:
        logger.warning(f"BEETS_VALIDATE: harness never sent choose_match!")

    logger.info(f"BEETS_VALIDATE: result={result}")
    return result


def stage_to_ai(album_data, source_path, staging_dir):
    """Move validated files from slskd download area to /AI/{Artist}/{Album}/."""
    artist_dir = sanitize_folder_name(album_data["artist"])
    album_dir = sanitize_folder_name(album_data["title"])
    dest = os.path.join(staging_dir, artist_dir, album_dir)
    os.makedirs(dest, exist_ok=True)

    for f in os.listdir(source_path):
        src = os.path.join(source_path, f)
        dst = os.path.join(dest, f)
        shutil.move(src, dst)

    shutil.rmtree(source_path, ignore_errors=True)
    return dest


def unmonitor_album(album_id):
    """Unmonitor album in Lidarr so Soularr doesn't re-download it."""
    try:
        url = f"{lidarr_host_url}/api/v1/album/monitor"
        payload = {"albumIds": [album_id], "monitored": False}
        logger.info(f"UNMONITOR: PUT {url} albumId={album_id}")
        resp = requests.put(
            url,
            headers={"X-Api-Key": lidarr_api_key},
            json=payload,
        )
        logger.info(f"UNMONITOR: response status={resp.status_code}")
        resp.raise_for_status()
        logger.info(f"UNMONITOR: album {album_id} unmonitored successfully")
    except Exception:
        logger.exception(f"Failed to unmonitor album {album_id}")


def trigger_meelo_scan():
    """Trigger a Meelo library scan after import. Best-effort — failures don't block."""
    if not meelo_url:
        return
    try:
        # Get JWT
        login_data = json.dumps({"username": meelo_username, "password": meelo_password}).encode()
        login_req = urllib.request.Request(
            f"{meelo_url}/api/auth/login",
            data=login_data,
            headers={"Content-Type": "application/json"},
        )
        with urllib.request.urlopen(login_req, timeout=10) as resp:
            jwt = json.loads(resp.read())["access_token"]

        # Trigger scan of beets library only
        scan_req = urllib.request.Request(
            f"{meelo_url}/scanner/scan?library=beets",
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
        "artist": album_data.get("artist", ""),
        "album": album_data.get("title", ""),
        "mb_release_id": album_data.get("mb_release_id", ""),
        "lidarr_album_id": album_data.get("album_id"),
        "status": "staged" if result["valid"] else "rejected",
        "scenario": result.get("scenario", ""),
        "distance": result.get("distance"),
        "detail": result.get("detail", ""),
        "dest_path": dest_path,
        "error": result.get("error"),
    }
    try:
        with open(beets_tracking_file, "a") as f:
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


def get_current_page(path: str, default_page=1) -> int:
    if os.path.exists(path):
        with open(path, "r") as file:
            page_string = file.read().strip()

            if page_string:
                return int(page_string)
            else:
                with open(path, "w") as file:
                    file.write(str(default_page))
                return default_page
    else:
        with open(path, "w") as file:
            file.write(str(default_page))
        return default_page


def update_current_page(path: str, page: str) -> None:
    with open(path, "w") as file:
        file.write(page)


def get_records(missing: bool) -> list:
    try:
        wanted = lidarr.get_wanted(
            page_size=page_size,
            sort_dir="ascending",
            sort_key="albums.title",
            missing=missing,
        )
    except ConnectionError as ex:
        logger.error(f"An error occurred when attempting to get records: {ex}")
        return []

    total_wanted = wanted["totalRecords"]

    wanted_records = []
    if search_type == "all":
        page = 1
        while len(wanted_records) < total_wanted:
            try:
                wanted = lidarr.get_wanted(
                    page=page,
                    page_size=page_size,
                    sort_dir="ascending",
                    sort_key="albums.title",
                    missing=missing,
                )
            except ConnectionError as ex:
                logger.error(f"Failed to grab record: {ex}")
            wanted_records.extend(wanted["records"])
            page += 1

    elif search_type == "incrementing_page":
        source_suffix = "missing" if missing else "cutoff"
        page_file = current_page_file_path.replace(".current_page.txt", f".current_page_{source_suffix}.txt")
        page = get_current_page(page_file)
        try:
            wanted_records = lidarr.get_wanted(
                page=page,
                page_size=page_size,
                sort_dir="ascending",
                sort_key="albums.title",
                missing=missing,
            )["records"]
        except ConnectionError as ex:
            logger.error(f"Failed to grab record: {ex}")
        page = 1 if page >= math.ceil(total_wanted / page_size) else page + 1
        update_current_page(page_file, str(page))

    elif search_type == "first_page":
        wanted_records = wanted["records"]

    else:
        if os.path.exists(lock_file_path) and not is_docker():
            os.remove(lock_file_path)

        raise ValueError(f"[Search Settings] - {search_type = } is not valid")

    try:
        queued_records = lidarr.get_queue(sort_dir="ascending", sort_key="albums.title")
        total_queued = queued_records["totalRecords"]
        current_queue = queued_records["records"]

        if queued_records["pageSize"] < total_queued:
            page = 2
            while len(current_queue) < total_queued:
                try:
                    next_page = lidarr.get_queue(page=page, sort_key="albums.title", sort_dir="ascending")
                except ConnectionError as ex:
                    logger.error(f"Failed to get queue details: {ex}")
                    break
                current_queue.extend(next_page["records"])
                page += 1

        queued_album_ids = []

        for record in current_queue:
            if "albumId" in record:
                queued_album_ids.append(record["albumId"])
            else:
                logger.warning(f"Dropping entry due to missing key in keylist: [{record.keys()}]")

        wanted_records_not_queued = []
        for record in wanted_records:
            for release in record["releases"]:
                if release["albumId"] in queued_album_ids:
                    logging.info(f"Skipping record '{record['title']}' because it's already in download queue")
                    break
            else:  # This only runs if the loop is broken out of. Saves on all the boolean found= stuff
                wanted_records_not_queued.append(record)
        if len(wanted_records_not_queued) > 0:
            wanted_records = wanted_records_not_queued
        else:
            logging.info("No records wanted that arent already queued")
            wanted_records = []
    except ConnectionError as ex:
        logger.error(f"Failed to get queue details so not filtering based on queue: {ex}")

    return wanted_records


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


def load_cutoff_denylist(file_path):
    """Load persistent cutoff denylist from JSON. Format: {album_id_str: [username, ...]}"""
    if not os.path.exists(file_path):
        return {}
    try:
        with open(file_path, "r") as f:
            raw = json.load(f)
            # Convert lists back to sets
            return {int(k): set(v) for k, v in raw.items()}
    except (json.JSONDecodeError, IOError) as ex:
        logger.warning(f"Error loading cutoff denylist: {ex}. Starting with empty denylist.")
        return {}


def load_download_fail_counts(file_path):
    """Load persistent download failure counts. Format: {"album_id:username": count}"""
    p = file_path.replace("cutoff_denylist", "download_fail_counts")
    if not os.path.exists(p):
        return {}
    try:
        with open(p, "r") as f:
            return json.load(f)
    except (json.JSONDecodeError, IOError):
        return {}


def save_download_fail_counts(file_path, counts):
    p = file_path.replace("cutoff_denylist", "download_fail_counts")
    try:
        with open(p, "w") as f:
            json.dump(counts, f, indent=2)
    except IOError as ex:
        logger.error(f"Error saving download fail counts: {ex}")


def save_cutoff_denylist(file_path, denylist):
    """Save cutoff denylist to JSON. Converts sets to lists for serialization."""
    try:
        with open(file_path, "w") as f:
            json.dump({str(k): list(v) for k, v in denylist.items()}, f, indent=2)
    except IOError as ex:
        logger.error(f"Error saving cutoff denylist: {ex}")


def main():
    global \
        slskd_api_key, \
        lidarr_api_key, \
        lidarr_download_dir, \
        lidarr_disable_sync, \
        slskd_download_dir, \
        lidarr_host_url, \
        slskd_host_url, \
        stalled_timeout, \
        remote_queue_timeout, \
        delete_searches, \
        slskd_url_base, \
        ignored_users, \
        search_type, \
        search_source, \
        download_filtering, \
        use_extension_whitelist, \
        extensions_whitelist, \
        search_sources, \
        minimum_match_ratio, \
        page_size, \
        remove_wanted_on_failure, \
        enable_search_denylist, \
        max_search_failures, \
        use_most_common_tracknum, \
        allow_multi_disc, \
        accepted_countries, \
        skip_region_check, \
        accepted_formats, \
        allowed_filetypes, \
        lock_file_path, \
        config_file_path, \
        failure_file_path, \
        current_page_file_path, \
        denylist_file_path, \
        cutoff_denylist_file_path, \
        search_blacklist, \
        lidarr, \
        slskd, \
        config, \
        logger, \
        search_cache, \
        folder_cache, \
        user_upload_speed, \
        broken_user, \
        cutoff_denylist, \
        download_fail_counts, \
        beets_validation_enabled, \
        beets_harness_path, \
        beets_distance_threshold, \
        beets_staging_dir, \
        audio_check_mode, \
        beets_tracking_file, \
        pipeline_db_enabled, \
        pipeline_db_dsn, \
        pipeline_db_source, \
        meelo_url, \
        meelo_username, \
        meelo_password

    # Let's allow some overrides to be passed to the script
    parser = argparse.ArgumentParser(description="""Soularr reads all of your "wanted" albums/artists from Lidarr and downloads them using Slskd""")

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
    failure_file_path = os.path.join(args.var_dir, "failure_list.txt")
    current_page_file_path = os.path.join(args.var_dir, ".current_page.txt")
    denylist_file_path = os.path.join(args.var_dir, "search_denylist.json")
    cutoff_denylist_file_path = os.path.join(args.var_dir, "cutoff_denylist.json")

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

        slskd_api_key = config["Slskd"]["api_key"]
        lidarr_api_key = config["Lidarr"]["api_key"]

        lidarr_download_dir = config["Lidarr"]["download_dir"]
        lidarr_disable_sync = config.getboolean("Lidarr", "disable_sync", fallback=False)

        slskd_download_dir = config["Slskd"]["download_dir"]

        lidarr_host_url = config["Lidarr"]["host_url"]
        slskd_host_url = config["Slskd"]["host_url"]

        stalled_timeout = config.getint("Slskd", "stalled_timeout", fallback=3600)
        remote_queue_timeout = config.getint("Slskd", "remote_queue_timeout", fallback=300)

        delete_searches = config.getboolean("Slskd", "delete_searches", fallback=True)

        slskd_url_base = config.get("Slskd", "url_base", fallback="/")

        ignored_users = config.get("Search Settings", "ignored_users", fallback="").split(",")
        search_blacklist = config.get("Search Settings", "search_blacklist", fallback="").split(",")
        search_blacklist = [word.strip() for word in search_blacklist if word.strip()]
        search_type = config.get("Search Settings", "search_type", fallback="first_page").lower().strip()
        search_source = config.get("Search Settings", "search_source", fallback="missing").lower().strip()

        download_filtering = config.getboolean("Download Settings", "download_filtering", fallback=False)
        use_extension_whitelist = config.getboolean("Download Settings", "use_extension_whitelist", fallback=False)
        extensions_whitelist = config.get("Download Settings", "extensions_whitelist", fallback="txt,nfo,jpg").split(",")

        search_sources = [search_source]
        if search_sources[0] == "all":
            search_sources = ["missing", "cutoff_unmet"]

        minimum_match_ratio = config.getfloat("Search Settings", "minimum_filename_match_ratio", fallback=0.5)
        page_size = config.getint("Search Settings", "number_of_albums_to_grab", fallback=10)
        remove_wanted_on_failure = config.getboolean("Search Settings", "remove_wanted_on_failure", fallback=True)
        enable_search_denylist = config.getboolean("Search Settings", "enable_search_denylist", fallback=False)
        max_search_failures = config.getint("Search Settings", "max_search_failures", fallback=3)

        use_most_common_tracknum = config.getboolean("Release Settings", "use_most_common_tracknum", fallback=True)
        allow_multi_disc = config.getboolean("Release Settings", "allow_multi_disc", fallback=True)

        default_accepted_countries = "Europe,Japan,United Kingdom,United States,[Worldwide],Australia,Canada"
        default_accepted_formats = "CD,Digital Media,Vinyl"
        accepted_countries = config.get("Release Settings", "accepted_countries", fallback=default_accepted_countries).split(",")
        skip_region_check = config.getboolean("Release Settings", "skip_region_check", fallback=False)
        accepted_formats = config.get("Release Settings", "accepted_formats", fallback=default_accepted_formats).split(",")

        raw_filetypes = config.get("Search Settings", "allowed_filetypes", fallback="flac,mp3")

        if "," in raw_filetypes:
            allowed_filetypes = raw_filetypes.split(",")
        else:
            allowed_filetypes = [raw_filetypes]

        setup_logging(config)

        # Beets validation config
        beets_validation_enabled = config.getboolean("Beets Validation", "enabled", fallback=False)
        beets_harness_path = config.get("Beets Validation", "harness_path",
                                        fallback="/mnt/virtio/Music/harness/run_beets_harness.sh")
        beets_distance_threshold = config.getfloat("Beets Validation", "distance_threshold", fallback=0.15)
        beets_staging_dir = config.get("Beets Validation", "staging_dir", fallback="/mnt/virtio/Music/AI")
        audio_check_mode = config.get("Beets Validation", "audio_check", fallback="normal")
        beets_tracking_file = config.get("Beets Validation", "tracking_file",
                                         fallback="/mnt/virtio/Music/Re-download/beets-validated.jsonl")
        if beets_validation_enabled:
            logger.info(f"Beets validation ENABLED: harness={beets_harness_path}, "
                        f"threshold={beets_distance_threshold}, staging={beets_staging_dir}")

        # Pipeline DB config
        pipeline_db_enabled = config.getboolean("Pipeline DB", "enabled", fallback=False)
        pipeline_db_dsn = config.get("Pipeline DB", "dsn",
                                     fallback="postgresql://soularr@localhost/soularr")
        if pipeline_db_enabled:
            from album_source import DatabaseSource
            pipeline_db_source = DatabaseSource(pipeline_db_dsn)
            logger.info(f"Pipeline DB ENABLED: {pipeline_db_dsn}")

        # Meelo config — trigger library scan after auto-import
        meelo_url = config.get("Meelo", "url", fallback=None)
        meelo_username = config.get("Meelo", "username", fallback=None)
        meelo_password = config.get("Meelo", "password", fallback=None)
        if meelo_url:
            logger.info(f"Meelo post-import scan ENABLED: {meelo_url}")

        # Init directory cache. The wide search returns all the data we need. This prevents us from hammering the users on the Soulseek network
        search_cache = {}
        folder_cache = {}
        user_upload_speed = {}
        broken_user = []
        cutoff_denylist = load_cutoff_denylist(cutoff_denylist_file_path)
        if cutoff_denylist:
            logger.info(f"Loaded cutoff denylist with {len(cutoff_denylist)} album(s) from {cutoff_denylist_file_path}")
        download_fail_counts = load_download_fail_counts(cutoff_denylist_file_path)

        slskd = slskd_api.SlskdClient(host=slskd_host_url, api_key=slskd_api_key, url_base=slskd_url_base)
        lidarr = LidarrAPI(lidarr_host_url, lidarr_api_key)
        wanted_records = []

        if pipeline_db_enabled and pipeline_db_source is not None:
            # === Pipeline DB mode: get wanted albums from DB ===
            logger.info("Getting wanted records from pipeline DB...")
            wanted_records = pipeline_db_source.get_wanted(limit=page_size)
            logger.info(f"Pipeline DB: {len(wanted_records)} wanted record(s)")
        else:
            # === Lidarr mode (original): get wanted from Lidarr API ===
            try:
                for source in search_sources:
                    logging.debug(f"Getting records from {source}")
                    missing = source == "missing"
                    records = get_records(missing)
                    for record in records:
                        record["_is_cutoff"] = not missing
                    wanted_records.extend(records)
            except ValueError as ex:
                logger.error(f"An error occurred: {ex}")
                logger.error("Exiting...")
                sys.exit(0)

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
                if remove_wanted_on_failure:
                    logger.info(f'{failed}: releases failed to find a match in the search results. View "failure_list.txt" for list of failed albums.')
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
