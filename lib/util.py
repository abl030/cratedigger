"""Utility functions for the Soularr pipeline.

Pure utilities with no dependency on module-level globals.
Functions that need config receive it as a parameter.
"""

from __future__ import annotations

import json
import logging
import os
import re
import shutil
import subprocess as sp
import unicodedata
import difflib
from datetime import datetime
from typing import Any

logger = logging.getLogger("soularr")


# === Filesystem utilities ===

def sanitize_folder_name(folder_name: str) -> str:
    valid_characters = re.sub(r'[<>:."/\\|?*]', "", folder_name)
    return valid_characters.strip()


def move_failed_import(src_path: str) -> str | None:
    """Move a failed import to the failed_imports directory. Returns the absolute destination path."""
    failed_imports_dir = os.path.join(os.getcwd(), "failed_imports")

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
        return target_path
    return None


def stage_to_ai(album_data: Any, source_path: str, staging_dir: str) -> str:
    """Move validated files from slskd download area to staging/{Artist}/{Album}/."""
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


# === Audio validation ===

def repair_mp3_headers(folder_path: str) -> None:
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


_AUDIO_EXTS = {"mp3", "flac", "m4a", "ogg", "opus", "wma", "aac", "alac", "wav"}


def cleanup_disambiguation_orphans(imported_path: str) -> list[str]:
    """Remove sibling directories that contain no audio files.

    After beets disambiguates an album path (e.g. renames '2009 - Blood Bank'
    to '2009 - Blood Bank [2009]'), the original directory may be left behind
    containing only non-audio clutter (cover.jpg, Thumbs.DB). This function
    scans the parent (artist) directory and removes any sibling dirs that
    have zero audio files.

    Returns the list of removed directory paths.
    """
    artist_dir = os.path.dirname(imported_path)
    if not os.path.isdir(artist_dir):
        return []
    removed: list[str] = []
    for entry in os.listdir(artist_dir):
        sibling = os.path.join(artist_dir, entry)
        if sibling == imported_path or not os.path.isdir(sibling):
            continue
        has_audio = any(
            f.rsplit(".", 1)[-1].lower() in _AUDIO_EXTS
            for f in os.listdir(sibling)
            if os.path.isfile(os.path.join(sibling, f)) and "." in f
        )
        if not has_audio:
            shutil.rmtree(sibling)
            logger.info(f"Removed disambiguation orphan: {sibling}")
            removed.append(sibling)
    return removed


def validate_audio(folder_path: str, mode: str = "normal") -> dict[str, Any]:
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


# === Track title matching ===

def _normalize_title(s: str) -> str:
    """Normalize a title for comparison: lowercase, strip punctuation, collapse whitespace."""
    s = unicodedata.normalize("NFKD", s)
    s = s.lower().strip()
    s = re.sub(r"[''`]", "'", s)
    s = re.sub(r"[^\w\s'&]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _extract_title_from_filename(filename: str) -> str:
    """Extract a track title from a Soulseek filename.

    Strips: extension, leading track numbers, artist prefixes.
    Returns normalized title via _normalize_title().
    """
    # Strip extension
    name = re.sub(r'\.[a-zA-Z0-9]{2,4}$', '', filename)
    # Replace underscores with spaces
    name = name.replace('_', ' ')
    # Strip leading "Artist - " prefix (before track number)
    name = re.sub(r'^.+?\s*-\s*(?=\d{1,2}\s*[-.\s])', '', name)
    # Strip leading track number patterns
    name = re.sub(r'^\d{1,3}\s*[-._)\s]+\s*', '', name)
    # Strip leading "Artist - " if still present
    if ' - ' in name:
        parts = name.split(' - ', 1)
        if len(parts) == 2 and parts[1].strip():
            name = parts[1]
    return _normalize_title(name)


def _track_titles_cross_check(expected_tracks: list, slskd_files: list) -> bool:
    """Cross-check that Soulseek filenames match expected track titles.

    Returns True if enough titles match, False if too many are missing.
    Tolerance: up to 1/5 tracks can mismatch.
    """
    if not expected_tracks or not slskd_files:
        return True

    expected = [_normalize_title(t.get("title", "")) for t in expected_tracks]
    slskd_titles = [_extract_title_from_filename(f.get("filename", "")) for f in slskd_files]

    mismatches = 0
    for exp_title in expected:
        if not exp_title:
            continue
        best_ratio = 0.0
        for slskd_title in slskd_titles:
            if not slskd_title:
                continue
            if exp_title in slskd_title or slskd_title in exp_title:
                best_ratio = 1.0
                break
            ratio = difflib.SequenceMatcher(None, exp_title, slskd_title).ratio()
            if ratio > best_ratio:
                best_ratio = ratio
        if best_ratio < 0.5:
            mismatches += 1

    max_allowed = max(1, len(expected) // 5)
    if mismatches > max_allowed:
        logger.info(f"CROSS-CHECK: {mismatches}/{len(expected)} tracks failed title match "
                    f"(max allowed: {max_allowed})")
        return False
    return True


# === Beets validation wrapper ===

def beets_validate(harness_path: str, album_path: str, mb_release_id: str,
                   distance_threshold: float = 0.15) -> Any:
    """Thin wrapper — delegates to lib.beets.beets_validate()."""
    from lib.beets import beets_validate as _bv
    return _bv(harness_path, album_path, mb_release_id, distance_threshold)


# === Meelo integration ===

import urllib.request


def _meelo_jwt_login(url: str, username: str, password: str) -> str:
    """Authenticate with Meelo and return a JWT token."""
    login_data = json.dumps({"username": username, "password": password}).encode()
    login_req = urllib.request.Request(
        f"{url}/api/auth/login",
        data=login_data,
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(login_req, timeout=10) as resp:
        return json.loads(resp.read())["access_token"]


def _meelo_scanner_post(url: str, jwt: str, path: str) -> None:
    """POST to a Meelo scanner endpoint with JWT auth."""
    req = urllib.request.Request(
        f"{url}{path}",
        method="POST",
        headers={"Authorization": f"Bearer {jwt}"},
    )
    with urllib.request.urlopen(req, timeout=10) as resp:
        resp.read()


def trigger_meelo_scan(cfg: Any) -> None:
    """Trigger a Meelo library scan after import. Best-effort — failures don't block."""
    if not cfg.meelo_url:
        return
    try:
        jwt = _meelo_jwt_login(cfg.meelo_url, cfg.meelo_username, cfg.meelo_password)
        _meelo_scanner_post(cfg.meelo_url, jwt, "/scanner/scan?library=beets")
        logger.info("MEELO: triggered beets library scan")
    except Exception as e:
        logger.warning(f"MEELO: scan trigger failed: {e}")


def trigger_meelo_clean(cfg: Any) -> None:
    """Trigger a Meelo library clean to remove orphaned entries. Best-effort."""
    if not cfg.meelo_url:
        return
    try:
        jwt = _meelo_jwt_login(cfg.meelo_url, cfg.meelo_username, cfg.meelo_password)
        _meelo_scanner_post(cfg.meelo_url, jwt, "/scanner/clean?library=beets")
        logger.info("MEELO: triggered beets library clean")
    except Exception as e:
        logger.warning(f"MEELO: clean trigger failed: {e}")


# === Plex integration ===


def trigger_plex_scan(cfg: Any, imported_path: str | None = None) -> None:
    """Trigger a Plex library scan after import. Best-effort — failures don't block.

    If imported_path is provided, does a targeted partial scan of just that folder.
    Otherwise triggers a full library section refresh.
    """
    if not cfg.plex_url or not cfg.plex_token:
        logger.debug("PLEX: skipped scan (no url or token configured)")
        return
    try:
        section = cfg.plex_library_section_id or "1"
        url = f"{cfg.plex_url}/library/sections/{section}/refresh?X-Plex-Token={cfg.plex_token}"
        if imported_path:
            from urllib.parse import quote
            url += f"&path={quote(imported_path, safe='')}"
        # Log the URL without the token for debugging
        safe_url = url.split("X-Plex-Token=")[0] + "X-Plex-Token=<redacted>"
        if "&path=" in url:
            safe_url += "&path=" + url.split("&path=")[1]
        logger.debug(f"PLEX: GET {safe_url}")
        req = urllib.request.Request(url)
        with urllib.request.urlopen(req, timeout=10) as resp:
            status = resp.status
        if imported_path:
            logger.info(f"PLEX: triggered partial scan for {imported_path} (HTTP {status})")
        else:
            logger.info(f"PLEX: triggered full library scan (HTTP {status})")
    except Exception as e:
        logger.warning(f"PLEX: scan trigger failed: {e}")


# === Validation logging ===

def log_validation_result(album_data: Any, result: Any, cfg: Any,
                          dest_path: str | None = None) -> None:
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
        logger.exception("Failed to write beets tracking entry")


# === Misc utilities ===

def is_docker() -> bool:
    return os.getenv("IN_DOCKER") is not None


def slskd_version_check(version: str, target: str = "0.22.2") -> bool:
    version_tuple = tuple(map(int, version.split(".")[:3]))
    target_tuple = tuple(map(int, target.split(".")[:3]))
    return version_tuple > target_tuple


def setup_logging(config: Any) -> None:
    DEFAULT_LOGGING_CONF = {
        "level": "INFO",
        "format": "[%(levelname)s|%(module)s|L%(lineno)d] %(asctime)s: %(message)s",
        "datefmt": "%Y-%m-%dT%H:%M:%S%z",
    }
    if "Logging" in config:
        log_config = config["Logging"]
    else:
        log_config = DEFAULT_LOGGING_CONF
    logging.basicConfig(**log_config)  # type: ignore


# === Search denylist ===

def load_search_denylist(file_path: str) -> dict:
    if not os.path.exists(file_path):
        return {}
    try:
        with open(file_path, "r") as file:
            return json.load(file)
    except (json.JSONDecodeError, IOError) as ex:
        logger.warning(f"Error loading search denylist: {ex}. Starting with empty denylist.")
        return {}


def save_search_denylist(file_path: str, denylist: dict) -> None:
    try:
        with open(file_path, "w") as file:
            json.dump(denylist, file, indent=2)
    except IOError as ex:
        logger.error(f"Error saving search denylist: {ex}")


def is_search_denylisted(denylist: dict, album_id: int, max_failures: int) -> bool:
    album_key = str(album_id)
    if album_key in denylist:
        return denylist[album_key]["failures"] >= max_failures
    return False


def update_search_denylist(denylist: dict, album_id: int, success: bool) -> None:
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
