"""Utility functions for the Cratedigger pipeline.

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
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable, Iterator, Sequence, TYPE_CHECKING

if TYPE_CHECKING:
    from lib.config import CratediggerConfig
    from lib.grab_list import GrabListEntry
    from lib.quality import ValidationResult

logger = logging.getLogger("cratedigger")


@dataclass
class AudioValidationResult:
    """Result from validate_audio() — audio integrity check."""
    valid: bool = True
    error: str | None = None
    failed_files: list[tuple[str, str]] = field(default_factory=list)


def parse_mb_first_release_year(data: dict[str, Any]) -> int | None:
    """Parse the 4-digit year from an MB release-group ``first-release-date``.

    Used by ``web/mb.py::get_release_group_year``; the resolver service
    goes through that same web client. Returns ``None`` for
    missing/short/non-numeric prefixes.
    """
    date = data.get("first-release-date", "")
    if not isinstance(date, str) or len(date) < 4:
        return None
    try:
        return int(date[:4])
    except ValueError:
        return None


# === Subprocess env for beets ===


def beet_bin() -> str:
    """Locate the ``beet`` executable, preferring PATH.

    Single source of truth for "where does ``beet`` live" across every
    subprocess callsite (release_cleanup remove, force-import, diagnostics,
    etc). Before this helper, ``release_cleanup``
    used the literal ``"beet"`` and relied on parent PATH resolution
    while ``harness/import_one.py`` had its own ``shutil.which("beet")
    or <hardcoded path>`` fallback — Codex (PR #131 round 1 P3) flagged
    the inconsistency after the pre-flight removal path started
    routing through ``release_cleanup`` from the harness, where the
    systemd-narrowed PATH (coreutils/findutils/grep/sed only) does not
    include ``/etc/profiles/per-user/abl030/bin``.

    Resolved at CALL time (not import time) so tests that patch
    ``shutil.which`` see the patched value.
    """
    return (shutil.which("beet")
            or "/etc/profiles/per-user/abl030/bin/beet")


def beets_subprocess_env() -> dict[str, str]:
    """Env for subprocesses that invoke beets (directly or via the harness
    and import_one.py). Single source of truth for the HOME override.

    Beets resolves its config from `$HOME/.config/beets/config.yaml`. The
    cratedigger systemd service runs as root with HOME=/root; the Nix Home
    Manager beets config (including the Discogs plugin token and the patched
    base URL for the local Discogs mirror) lives at /home/abl030/.config/.
    Without the override, the Discogs plugin silently returns 0 candidates
    for every --search-id <numeric_id> → scenario=mbid_not_found on every
    Discogs validation. This hit every Blueline Medic - Apology Wars
    attempt (download_log 3604–3616) post-PR #100.

    Every subprocess that runs beets must use this env:
      - lib/beets.py::beets_validate (harness for validation)
      - lib/dispatch/ (launches import_one.py)
      - harness/import_one.py (launches the harness + `beet move`)
      - web/routes/pipeline.py (ban-source `beet remove`)

    os.environ is snapshotted at CALL time, not import time, so tests that
    patch the environment see the patched values.
    """
    return {**os.environ, "HOME": "/home/abl030"}


# === Filesystem utilities ===

_BAD_FILE_SCENARIOS = frozenset({"audio_corrupt", "spectral_reject"})
FAILED_IMPORT_SEARCH_DIRS = ("/mnt/virtio/music/slskd",)
ABANDONED_AUTO_IMPORT_PREFIX = "abandoned_auto_import"


def _move_to_failed_imports(
    src_path: str,
    *,
    scenario: str | None = None,
    folder_name: str | None = None,
) -> str | None:
    src_path = os.path.abspath(src_path)
    if not os.path.exists(src_path):
        return None

    parent_dir = os.path.dirname(src_path)
    failed_imports_dir = os.path.join(parent_dir, "failed_imports")
    if scenario in _BAD_FILE_SCENARIOS:
        failed_imports_dir = os.path.join(failed_imports_dir, "bad_files")
    os.makedirs(failed_imports_dir, exist_ok=True)

    target_folder_name = folder_name or os.path.basename(src_path)
    target_path = os.path.join(failed_imports_dir, target_folder_name)

    counter = 1
    while os.path.exists(target_path):
        target_path = os.path.join(
            failed_imports_dir,
            f"{target_folder_name}_{counter}",
        )
        counter += 1

    shutil.move(src_path, target_path)
    logger.info(f"Failed import moved to: {target_path}")
    return target_path


def move_abandoned_auto_import(src_path: str) -> str | None:
    """Move an interrupted auto-import to a diagnosable failed_imports folder."""
    folder_name = os.path.basename(os.path.abspath(src_path))
    return _move_to_failed_imports(
        src_path,
        folder_name=f"{ABANDONED_AUTO_IMPORT_PREFIX} - {folder_name}",
    )


def resolve_failed_path(
    failed_path: str,
    search_dirs: Sequence[str] | None = None,
) -> str | None:
    """Resolve a failed-path entry to an existing absolute directory.

    Older download_log rows stored paths relative to the slskd download root
    (for example ``failed_imports/Foo - Bar``). Newer rows store absolute
    paths. This helper accepts either representation and returns an absolute
    path when the directory still exists.
    """
    if not failed_path:
        return None

    if os.path.isdir(failed_path):
        return os.path.abspath(failed_path)

    for base in search_dirs or FAILED_IMPORT_SEARCH_DIRS:
        candidate = os.path.join(base, failed_path)
        if os.path.isdir(candidate):
            return os.path.abspath(candidate)

    return None

# === Audio validation ===

def repair_mp3_headers(folder_path: str) -> None:
    """Run mp3val -f on all MP3 files to fix header issues before audio validation.

    Walks subdirectories so nested multi-disc layouts are repaired too —
    must match validate_audio's traversal or fixable header issues inside
    subdirectories reach ffmpeg unrepaired and falsely reject the import.
    """
    for root, _dirs, files in os.walk(folder_path):
        for f in files:
            if not f.lower().endswith(".mp3"):
                continue
            filepath = os.path.join(root, f)
            try:
                result = sp.run(["mp3val", "-f", "-nb", filepath],
                                capture_output=True, text=True,
                                errors="replace", timeout=60)
                if "FIXED" in result.stdout:
                    logger.info(f"MP3VAL: fixed {f}")
            except FileNotFoundError:
                logger.warning("MP3VAL: mp3val not found on PATH — skipping header repair")
                return
            except sp.TimeoutExpired:
                logger.warning(f"MP3VAL: timeout on {f}")
            except Exception:
                logger.exception(f"MP3VAL: error on {f}")


from lib.quality import AUDIO_EXTENSIONS as _AUDIO_EXTS


def cleanup_disambiguation_orphans(
    imported_path: str, beets_directory: str = ""
) -> list[str]:
    """Remove sibling directories that contain no audio files.

    After beets disambiguates an album path (e.g. renames '2009 - Blood Bank'
    to '2009 - Blood Bank [2009]'), the original directory may be left behind
    containing only non-audio clutter (cover.jpg, Thumbs.DB). This function
    scans the parent (artist) directory and removes any sibling dirs that
    have zero audio files.

    ``imported_path`` is absolutized against ``beets_directory`` if it's
    relative (``BeetsDB.get_album_info()`` returns paths relative to the
    beets library root). When ``beets_directory`` is empty and the path
    is relative, warn-and-skip rather than silently no-op.
    Returns the list of removed directory paths.
    """
    if not os.path.isabs(imported_path):
        if beets_directory:
            imported_path = os.path.join(beets_directory, imported_path)
        else:
            # Symmetric defense with trigger_plex_scan: silent no-ops on a
            # path-translation gap is exactly how PR #236 lurked for 5 weeks.
            # See docs/solutions/runtime-errors/plex-partial-scan-silent-200.md
            logger.warning(
                f"cleanup_disambiguation_orphans: imported_path {imported_path!r} "
                "is relative and beets_directory is unset; cannot resolve "
                "siblings. Skipping (no orphans removed).")
            return []
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


def validate_audio(folder_path: str, mode: str = "normal") -> AudioValidationResult:
    """Check audio integrity of downloaded files via ffmpeg full decode.

    mode: "off" = skip, anything else = reject if any file fails.

    Walks subdirectories so multi-disc layouts (``Album/CD1/*.mp3``) are
    validated too. The auto-import path always passes a flattened folder
    so recursion is a no-op there; force/manual-import paths can point at
    user folders with nested discs.
    """
    if mode == "off":
        return AudioValidationResult()

    files = []
    for root, _dirs, names in os.walk(folder_path):
        for f in names:
            ext = f.rsplit(".", 1)[-1].lower() if "." in f else ""
            if ext in _AUDIO_EXTS:
                files.append(os.path.join(root, f))

    if not files:
        return AudioValidationResult()

    failed = []
    for filepath in files:
        # Use the path relative to folder_path so nested layouts (CD1/01.mp3,
        # CD2/01.mp3) remain distinguishable in failed_files and the error
        # message — os.path.basename would collapse same-named tracks across
        # discs into the same entry.
        display = os.path.relpath(filepath, folder_path)
        try:
            result = sp.run(
                ["ffmpeg", "-v", "error", "-i", filepath,
                 "-map", "0:a", "-f", "null", "-"],
                capture_output=True, text=True, errors="replace", timeout=300
            )
            ffmpeg_returncode = result.returncode
            stderr = result.stderr.strip()
            if filepath.lower().endswith(".flac") and "cannot check MD5 signature" in stderr:
                logger.info(f"AUDIO_CHECK: fixing unset MD5: {display}")
                fix = sp.run(
                    ["flac", "-f", "--verify", filepath],
                    capture_output=True, text=True, errors="replace",
                    timeout=300,
                )
                if fix.returncode == 0:
                    retest = sp.run(
                        ["ffmpeg", "-v", "error", "-i", filepath,
                         "-map", "0:a", "-f", "null", "-"],
                        capture_output=True, text=True, errors="replace",
                        timeout=300,
                    )
                    ffmpeg_returncode = retest.returncode
                    stderr = retest.stderr.strip()
                    if retest.returncode == 0:
                        if stderr:
                            logger.info(
                                "AUDIO_CHECK: ignoring informational stderr after MD5 fix: %s",
                                display,
                            )
                        continue  # fixed and clean enough for audio validation
                else:
                    stderr = f"MD5 fix failed: {fix.stderr.strip()[:150]}"
                    failed.append((display, stderr))
                    continue

            # #251: ffmpeg's exit code is the authoritative signal for decode
            # failure. Stderr at rc=0 is informational only (metadata-only
            # demuxer warnings: BOM, mjpeg APP fields, mp3float backstep,
            # attached-picture mimetype) and must not flag audio_corrupt.
            # The previous `_IGNORABLE_AUDIO_VALIDATION_STDERR` allowlist was
            # retired with this change: every documented "ignorable" stderr
            # pattern had rc=0 in practice (see d7fdff7 commit history), so
            # trusting rc alone subsumes the filter without loss of coverage.
            if ffmpeg_returncode != 0:
                err = stderr[:200]
                failed.append((display, err))
        except sp.TimeoutExpired:
            failed.append((display, "ffmpeg timeout"))
        except FileNotFoundError:
            logger.error("AUDIO_CHECK: ffmpeg not found on PATH — skipping audio validation")
            return AudioValidationResult()

    if not failed:
        logger.info(f"AUDIO_CHECK: all {len(files)} files passed ({mode} mode)")
        return AudioValidationResult()

    detail = "; ".join(f"{name}: {err}" for name, err in failed[:5])
    error_msg = f"{len(failed)}/{len(files)} files failed: {detail}"
    logger.warning(f"AUDIO_CHECK: {error_msg} → REJECT ({mode} mode)")
    return AudioValidationResult(valid=False, error=error_msg, failed_files=failed)


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


def _track_titles_cross_check(expected_tracks: Sequence[Any], slskd_files: Sequence[Any]) -> bool:
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


def trigger_meelo_scan(cfg: CratediggerConfig) -> None:
    """Trigger a Meelo library scan after import. Best-effort — failures don't block."""
    if not cfg.meelo_url:
        return
    try:
        username = cfg.resolved_meelo_username()
        password = cfg.resolved_meelo_password()
        if not username or not password:
            return
        jwt = _meelo_jwt_login(cfg.meelo_url, username, password)
        _meelo_scanner_post(cfg.meelo_url, jwt, "/scanner/scan?library=beets")
        logger.info("MEELO: triggered beets library scan")
    except Exception as e:
        logger.warning(f"MEELO: scan trigger failed: {e}")


def trigger_meelo_clean(cfg: CratediggerConfig) -> None:
    """Trigger a Meelo library clean to remove orphaned entries. Best-effort."""
    if not cfg.meelo_url:
        return
    try:
        username = cfg.resolved_meelo_username()
        password = cfg.resolved_meelo_password()
        if not username or not password:
            return
        jwt = _meelo_jwt_login(cfg.meelo_url, username, password)
        _meelo_scanner_post(cfg.meelo_url, jwt, "/scanner/clean?library=beets")
        logger.info("MEELO: triggered beets library clean")
    except Exception as e:
        logger.warning(f"MEELO: clean trigger failed: {e}")


# === Plex integration ===


def trigger_plex_scan(cfg: CratediggerConfig, imported_path: str | None = None) -> None:
    """Trigger a Plex library scan after import. Best-effort — failures don't block.

    If imported_path is provided, does a targeted partial scan of just that folder.
    Otherwise triggers a full library section refresh.
    """
    if not cfg.plex_url:
        logger.debug("PLEX: skipped scan (no url configured)")
        return
    try:
        token = cfg.resolved_plex_token()
        if not token:
            logger.debug("PLEX: skipped scan (no token configured)")
            return
        section = cfg.plex_library_section_id or "1"
        url = f"{cfg.plex_url}/library/sections/{section}/refresh?X-Plex-Token={token}"
        scan_path: str | None = None
        if imported_path:
            from urllib.parse import quote
            scan_path = imported_path
            # Step 1: absolutize relative paths via beets_directory if set.
            # Beets stores paths relative to its library root, so this is the
            # general-purpose mechanism for any deployment to absolutize.
            if not os.path.isabs(scan_path) and cfg.beets_directory:
                scan_path = os.path.join(cfg.beets_directory, scan_path)
            # Step 2: translate host path → container path via path_map
            # (only needed when Plex runs in a container with a different mount).
            if cfg.plex_path_map:
                local_prefix, container_prefix = cfg.plex_path_map.split(":", 1)
                if scan_path.startswith(local_prefix):
                    scan_path = container_prefix + scan_path[len(local_prefix):]
                elif not os.path.isabs(scan_path):
                    # path_map is set but beets_directory wasn't (or didn't
                    # apply). Fall back to anchoring relative paths under the
                    # container_prefix — this is the original PR #236 fix.
                    scan_path = container_prefix.rstrip("/") + "/" + scan_path
                else:
                    logger.warning(
                        f"PLEX: imported_path {scan_path!r} is absolute but "
                        f"outside path_map local_prefix {local_prefix!r}; "
                        "Plex may silently ignore the partial scan")
            # Step 3: final guard — Plex won't resolve a relative path.
            if not os.path.isabs(scan_path):
                logger.warning(
                    f"PLEX: imported_path {scan_path!r} is relative and no "
                    "beets_directory or plex_path_map is configured to "
                    "absolutize it; Plex may silently ignore the partial scan")
            url += f"&path={quote(scan_path, safe='')}"
        # Log the URL without the token for debugging
        safe_url = url.split("X-Plex-Token=")[0] + "X-Plex-Token=<redacted>"
        if "&path=" in url:
            safe_url += "&path=" + url.split("&path=")[1]
        logger.debug(f"PLEX: GET {safe_url}")
        req = urllib.request.Request(url)
        with urllib.request.urlopen(req, timeout=10) as resp:
            status = resp.status
        if scan_path is not None:
            logger.info(f"PLEX: triggered partial scan for {scan_path} (HTTP {status})")
        else:
            logger.info(f"PLEX: triggered full library scan (HTTP {status})")
    except Exception as e:
        logger.warning(f"PLEX: scan trigger failed: {e}")


# === Plex addedAt pin (read + edit) ===
#
# Read/edit half of the "Recently Added" pin feature (migration 040). The
# capture/reconcile orchestration lives in lib/plex_pin_service.py; these
# functions are the thin, testable Plex client it drives.


@dataclass(frozen=True)
class PlexAlbumRef:
    """A located Plex album: its rating key and current ``addedAt`` (epoch
    seconds), plus title/artist for logging."""
    rating_key: str
    added_at: int
    title: str = ""
    artist: str = ""


FetchXml = Callable[..., ET.Element]
PutFn = Callable[..., int]


def _plex_container_path(cfg: "CratediggerConfig", imported_path: str) -> str | None:
    """Translate a beets ``imported_path`` to the absolute path Plex stores in
    ``Media.Part.file`` — the join key for locating an album in Plex.

    Mirrors the absolutize + path_map transform in ``trigger_plex_scan``. Kept
    as a separate function deliberately: ``trigger_plex_scan`` has a documented
    five-week silent-failure history (see
    ``docs/solutions/runtime-errors/plex-partial-scan-silent-200.md``) and is
    not refactored here to avoid regressing it. Returns ``None`` when the
    result is not absolute (Plex can't match a relative path)."""
    if not imported_path:
        return None
    out = imported_path
    if not os.path.isabs(out) and cfg.beets_directory:
        out = os.path.join(cfg.beets_directory, out)
    if cfg.plex_path_map:
        local_prefix, container_prefix = cfg.plex_path_map.split(":", 1)
        if out.startswith(local_prefix):
            out = container_prefix + out[len(local_prefix):]
        elif not os.path.isabs(out):
            out = container_prefix.rstrip("/") + "/" + out
    return out if os.path.isabs(out) else None


def _plex_urlopen(req: "urllib.request.Request", timeout: int = 15) -> Any:
    """urlopen with a verify-then-unverified SSL fallback. The homelab Plex
    reverse proxy can present a cert that fails default verification from the
    cratedigger host; the pin must still read/write, so fall back to an
    unverified context (LAN-local, best-effort) rather than silently failing."""
    import ssl
    try:
        return urllib.request.urlopen(req, timeout=timeout)
    except ssl.SSLError:
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        return urllib.request.urlopen(req, timeout=timeout, context=ctx)


def _plex_fetch_xml(cfg: "CratediggerConfig", path: str, **params: str) -> ET.Element:
    """Thin urllib GET → parsed Plex XML. Network leaf seam."""
    from urllib.parse import urlencode
    params = dict(params)
    params["X-Plex-Token"] = cfg.resolved_plex_token() or ""
    url = f"{cfg.plex_url}{path}?{urlencode(params)}"
    req = urllib.request.Request(url, headers={"Accept": "application/xml"})
    with _plex_urlopen(req) as resp:
        return ET.fromstring(resp.read())


def _plex_put(cfg: "CratediggerConfig", path: str, **params: str) -> int:
    """Thin urllib PUT → HTTP status. Network leaf seam."""
    from urllib.parse import urlencode
    params = dict(params)
    params["X-Plex-Token"] = cfg.resolved_plex_token() or ""
    url = f"{cfg.plex_url}{path}?{urlencode(params)}"
    req = urllib.request.Request(url, method="PUT")
    with _plex_urlopen(req) as resp:
        return resp.status


def _parse_artist_album(imported_path: str) -> tuple[str, str]:
    """Best-effort ``(artist, album_title)`` from a beets ``imported_path``.

    The album folder is always the LAST segment (``YYYY - Album``) and the
    artist the second-to-last — this holds whether the importer hands us a
    relative ``Artist/YYYY - Album`` or an absolute
    ``/mnt/.../Beets/Artist/YYYY - Album`` (``ir.postflight.imported_path`` is
    the latter in production). Used only to narrow the Plex search; the
    authoritative match is the on-disk path verification afterward, so a
    slightly-off parse degrades to "search returns the wrong set, path check
    rejects it", never a false positive."""
    parts = imported_path.strip("/").split("/")
    artist = parts[-2] if len(parts) >= 2 else ""
    folder = parts[-1] if parts else ""
    m = re.match(r"^\d{4} - (.+)$", folder)
    album = m.group(1) if m else folder
    return artist, album


def plex_find_album_by_path(
    cfg: "CratediggerConfig",
    imported_path: str,
    *,
    fetch_xml: FetchXml | None = None,
) -> PlexAlbumRef | None:
    """Locate the Plex album whose track files live under ``imported_path``.

    Narrows candidates by album-title then artist search, and confirms each by
    checking its ``Media.Part.file`` paths start with the translated container
    folder — the authoritative join (resilient to the extension change an
    upgrade causes, since it matches the folder prefix, not the filename).
    Returns ``None`` when Plex has no album there (e.g. a genuinely-new album
    not yet scanned). Transport/parse failures raise to the caller, which
    treats Plex work as non-fatal."""
    if not cfg.plex_url:
        return None
    container = _plex_container_path(cfg, imported_path)
    if not container:
        return None
    fetch: FetchXml = fetch_xml or (lambda path, **p: _plex_fetch_xml(cfg, path, **p))
    section = cfg.plex_library_section_id or "1"
    artist, album = _parse_artist_album(imported_path)
    prefix = container.rstrip("/") + "/"

    def _candidates() -> Iterator[ET.Element]:
        if album:
            root = fetch(f"/library/sections/{section}/search", type="9", query=album)
            for d in root.findall(".//Directory"):
                if d.get("type") == "album":
                    yield d
        if artist:
            aroot = fetch(f"/library/sections/{section}/search", type="8", query=artist)
            for ad in aroot.findall(".//Directory"):
                if ad.get("type") != "artist":
                    continue
                ark = ad.get("ratingKey")
                if not ark:
                    continue
                albroot = fetch(f"/library/metadata/{ark}/children")
                for d in albroot.findall(".//Directory"):
                    if d.get("type") == "album":
                        yield d

    seen: set[str] = set()
    for d in _candidates():
        rk = d.get("ratingKey")
        if not rk or rk in seen:
            continue
        seen.add(rk)
        children = fetch(f"/library/metadata/{rk}/children")
        files = [p.get("file", "") or "" for p in children.findall(".//Part")]
        if any(f.startswith(prefix) for f in files):
            added = d.get("addedAt")
            try:
                added_int = int(added) if added is not None else 0
            except (TypeError, ValueError):
                added_int = 0
            return PlexAlbumRef(
                rating_key=rk,
                added_at=added_int,
                title=d.get("title", "") or "",
                artist=d.get("parentTitle", "") or "",
            )
    return None


def plex_set_added_at(
    cfg: "CratediggerConfig",
    rating_key: str,
    added_at: int,
    *,
    put_fn: PutFn | None = None,
) -> bool:
    """Pin an album's ``addedAt`` to ``added_at`` (epoch seconds) and lock the
    field so future Plex metadata refreshes don't clobber it. Returns ``True``
    on HTTP 200. The ``addedAt.locked=1`` is load-bearing — without it the next
    scan re-stamps the date (the cause of the "PUT didn't stick" reports)."""
    if not cfg.plex_url:
        return False
    section = cfg.plex_library_section_id or "1"
    put: PutFn = put_fn or (lambda path, **p: _plex_put(cfg, path, **p))
    status = put(
        f"/library/sections/{section}/all",
        type="9",
        id=str(rating_key),
        **{"addedAt.value": str(int(added_at)), "addedAt.locked": "1"},
    )
    return status == 200


# === Jellyfin integration ===


def trigger_jellyfin_scan(cfg: CratediggerConfig) -> None:
    """Trigger a Jellyfin library scan after import. Best-effort — failures don't block.

    If jellyfin_library_id is set, refreshes just that library item.
    Otherwise triggers a full library refresh.
    """
    if not cfg.jellyfin_url:
        logger.debug("JELLYFIN: skipped scan (no url configured)")
        return
    try:
        token = cfg.resolved_jellyfin_token()
        if not token:
            logger.debug("JELLYFIN: skipped scan (no token configured)")
            return
        if cfg.jellyfin_library_id:
            url = f"{cfg.jellyfin_url}/Items/{cfg.jellyfin_library_id}/Refresh"
        else:
            url = f"{cfg.jellyfin_url}/Library/Refresh"
        req = urllib.request.Request(
            url,
            method="POST",
            headers={"X-Emby-Token": token},
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            resp.read()
        if cfg.jellyfin_library_id:
            logger.info(f"JELLYFIN: triggered library refresh for item {cfg.jellyfin_library_id}")
        else:
            logger.info("JELLYFIN: triggered full library refresh")
    except Exception as e:
        logger.warning(f"JELLYFIN: scan trigger failed: {e}")


# === Validation logging ===

def log_validation_result(album_data: GrabListEntry, result: ValidationResult,
                          cfg: CratediggerConfig,
                          dest_path: str | None = None) -> None:
    """Append beets validation result to tracking JSONL."""
    entry = {
        "timestamp": datetime.now().isoformat(),
        "artist": album_data.artist,
        "album": album_data.title,
        "mb_release_id": album_data.mb_release_id,
        "album_id": album_data.album_id,
        "status": "staged" if result.valid else "rejected",
        "scenario": result.scenario or "",
        "distance": result.distance,
        "detail": result.detail or "",
        "dest_path": dest_path,
        "error": result.error,
    }
    try:
        with open(cfg.beets_tracking_file, "a") as f:
            f.write(json.dumps(entry) + "\n")
    except Exception:
        logger.exception("Failed to write beets tracking entry")


# === Misc utilities ===


def setup_logging(config: Any) -> None:
    _DEFAULT = {
        "level": "INFO",
        "format": "[%(levelname)s|%(module)s|L%(lineno)d] %(asctime)s: %(message)s",
        "datefmt": "%Y-%m-%dT%H:%M:%S%z",
    }
    log_config = config["Logging"] if "Logging" in config else _DEFAULT
    logging.basicConfig(**log_config)  # type: ignore


