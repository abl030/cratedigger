"""Utility functions for the Cratedigger pipeline.

Pure utilities with no dependency on module-level globals.
Functions that need config receive it as a parameter.
"""

from __future__ import annotations

import configparser
import json
import logging
import os
import re
import shutil
import stat
import subprocess as sp
import time
import unicodedata
import difflib
from dataclasses import dataclass
from datetime import datetime
from functools import lru_cache
from typing import Any, Callable, Iterator, Protocol, Sequence, TYPE_CHECKING
from xml.etree.ElementTree import Element

from defusedxml import ElementTree as ET

from lib.json_narrow import (
    is_object_list as _is_object_list,
    is_str_object_dict as _is_str_object_dict,
)
from lib.quality.audio_validation import (
    AUDIO_VALIDATION_DIAGNOSTIC_LIMIT,
    AUDIO_VALIDATION_POLICY_ID,
    AUDIO_VALIDATION_TOOL,
    AudioToolDiagnostic,
    AudioToolDiagnosticCategory,
    AudioValidationOutcome,
    AudioValidationReport,
    AudioValidationResult,
    bounded_audio_tool_diagnostic,
    validate_audio_validation_report,
)

if TYPE_CHECKING:
    from lib.config import CratediggerConfig
    from lib.grab_list import GrabListEntry
    from lib.quality import ValidationResult

logger = logging.getLogger("cratedigger")


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


def beets_subprocess_env(
    *,
    beets_config_dir: str | None = None,
    beets_python: str | None = None,
    beets_library_db_path: str | None = None,
    beets_library_root: str | None = None,
) -> dict[str, str]:
    """Env for subprocesses that invoke beets (directly or via the harness
    and import_one.py). Single source of truth for how a beets subprocess
    finds its config and interpreter.

    Sets ``BEETSDIR`` (beets' native config-dir override) at the
    module-rendered config dir from ``[Beets] config_dir`` in the runtime
    config.ini, and ``CRATEDIGGER_BEETS_PYTHON`` (the pinned interpreter the
    harness wrapper execs) from ``[Beets] python``. Pre-set environment
    values act as the dev/test fallback when the runtime config doesn't
    carry the keys. The Home-Manager-era ``HOME=/home/<user>`` impersonation
    is gone (tier-2 plan R6): an unset config dir raises an actionable
    error instead of silently letting beets fall back to the invoking
    user's ~/.config/beets — the misconfig class behind the 2026-06-29
    breakage and the Blueline Medic 0-candidates incident.

    Every subprocess that runs beets must use this env:
      - lib/beets.py::beets_validate (harness for validation)
      - lib/dispatch/ (launches import_one.py)
      - harness/import_one.py (launches the harness)

    os.environ and the runtime config are read at CALL time, not import
    time, so tests that patch either see the patched values.
    """
    env = {**os.environ}
    if beets_config_dir is not None:
        from lib.beets_db import validate_beets_storage_pair

        validate_beets_storage_pair(
            db_path=beets_library_db_path,
            library_root=beets_library_root,
        )
        beetsdir = beets_config_dir or env.get("BEETSDIR", "")
        if not beetsdir:
            raise RuntimeError("beets config dir is not set")
        assert beets_library_db_path is not None
        env["BEETSDIR"] = beetsdir
        env["BEETS_DB"] = beets_library_db_path
        if beets_python:
            env["CRATEDIGGER_BEETS_PYTHON"] = beets_python
        return env

    from lib.config import read_runtime_config
    cfg = read_runtime_config()
    beetsdir = cfg.beets_config_dir or env.get("BEETSDIR", "")
    if not beetsdir:
        raise RuntimeError(
            "beets config dir is not set: set [Beets] config_dir in "
            "config.ini (services.cratedigger renders it at "
            "<stateDir>/beets) or export BEETSDIR. Refusing to launch a "
            "beets subprocess that would silently fall back to the "
            "invoking user's ~/.config/beets."
        )
    env["BEETSDIR"] = beetsdir
    beets_python = cfg.beets_python or env.get("CRATEDIGGER_BEETS_PYTHON", "")
    if beets_python:
        env["CRATEDIGGER_BEETS_PYTHON"] = beets_python
    return env


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


_AUDIO_VALIDATION_TIMEOUT_SECONDS = 300


@dataclass(frozen=True)
class _AudioSourceSnapshot:
    device: int
    inode: int
    size: int
    mtime_ns: int


class _AudioSourceChangedError(OSError):
    pass


class _AudioReadProbe(Protocol):
    def __call__(
        self,
        filepath: str,
        *,
        complete: bool = False,
        expected: _AudioSourceSnapshot | None = None,
    ) -> _AudioSourceSnapshot: ...


def build_audio_validation_argv(filepath: str) -> list[str]:
    """Build the fixed audio-only full-decode policy from issue #835."""
    return [
        "ffmpeg",
        "-hide_banner",
        "-nostdin",
        "-v",
        "error",
        "-max_error_rate",
        "0",
        "-abort_on",
        "empty_output_stream",
        "-err_detect:a",
        "crccheck+bitstream+buffer+explode",
        "-vn",
        "-sn",
        "-dn",
        "-i",
        filepath,
        "-map",
        "0:a",
        "-map_metadata",
        "-1",
        "-map_chapters",
        "-1",
        "-f",
        "null",
        "-",
    ]


def _source_snapshot(filepath: str) -> _AudioSourceSnapshot:
    file_stat = os.stat(filepath)
    if not stat.S_ISREG(file_stat.st_mode):
        raise OSError("audio validation source is not a regular file")
    return _AudioSourceSnapshot(
        device=file_stat.st_dev,
        inode=file_stat.st_ino,
        size=file_stat.st_size,
        mtime_ns=file_stat.st_mtime_ns,
    )


def _probe_file_readable(
    filepath: str,
    *,
    complete: bool = False,
    expected: _AudioSourceSnapshot | None = None,
) -> _AudioSourceSnapshot:
    """Read enough to prove access, or the whole file after ambiguous failure."""
    before = _source_snapshot(filepath)
    if expected is not None and before != expected:
        raise _AudioSourceChangedError(
            "source changed during audio validation",
        )
    with open(filepath, "rb") as stream:
        if complete:
            while stream.read(1024 * 1024):
                pass
        else:
            stream.read(1)
    after = _source_snapshot(filepath)
    if after != before:
        raise _AudioSourceChangedError(
            "source changed during audio validation",
        )
    return before


def _audio_files_in_folder(folder_path: str) -> list[str]:
    walk_errors: list[OSError] = []
    files: list[str] = []
    for root, _dirs, names in os.walk(
        folder_path,
        onerror=walk_errors.append,
    ):
        for name in names:
            ext = name.rsplit(".", 1)[-1].lower() if "." in name else ""
            if ext in _AUDIO_EXTS:
                files.append(os.path.join(root, name))
    if walk_errors:
        raise walk_errors[0]
    files.sort()
    return files


@lru_cache(maxsize=1)
def _ffmpeg_version() -> str:
    """Return one compact version identity per process."""
    try:
        output = sp.check_output(
            # This is a version identity probe, not a media command; the
            # exact executable literal remains reserved for audited command
            # lists that map audio explicitly.
            [AUDIO_VALIDATION_TOOL, "-version"],
            stderr=sp.STDOUT,
            timeout=5,
        )
    except (OSError, sp.SubprocessError):
        return ""
    first_line = output.decode("utf-8", "replace").splitlines()
    return first_line[0].strip() if first_line else ""


def _audio_validation_result(
    *,
    outcome: AudioValidationOutcome,
    files_checked: int,
    files_failed: int,
    diagnostics: list[AudioToolDiagnostic],
    omitted_diagnostics: int = 0,
    tool_version: str | None = None,
    failed_paths: tuple[str, ...] = (),
    elapsed_seconds: float | None = None,
) -> AudioValidationResult:
    report = AudioValidationReport(
        policy_id=AUDIO_VALIDATION_POLICY_ID,
        tool=AUDIO_VALIDATION_TOOL,
        tool_version=(
            _ffmpeg_version() if tool_version is None else tool_version
        ),
        outcome=outcome,
        files_checked=files_checked,
        files_failed=files_failed,
        diagnostics=diagnostics,
        omitted_diagnostics=omitted_diagnostics,
    )
    validate_audio_validation_report(report)
    first = diagnostics[0] if diagnostics else None
    summary = (
        f"AUDIO_CHECK outcome={outcome} files={files_checked} "
        f"failed={files_failed} policy={AUDIO_VALIDATION_POLICY_ID}"
    )
    if elapsed_seconds is not None:
        summary += f" elapsed_ms={round(elapsed_seconds * 1000)}"
    if first is not None:
        summary += (
            f" first={first.relative_path or '.'}"
            f" category={first.category}"
        )
    if outcome == "audio_corrupt":
        logger.warning(summary)
    elif outcome == "measurement_failed":
        logger.error(summary)
    else:
        logger.info(summary)
    return AudioValidationResult(report, failed_paths=failed_paths)


def _world_failure_result(
    *,
    relative_path: str,
    category: AudioToolDiagnosticCategory,
    detail: bytes | str | None,
    files_checked: int,
    return_code: int | None = None,
    tool_version: str | None = None,
) -> AudioValidationResult:
    return _audio_validation_result(
        outcome="measurement_failed",
        files_checked=files_checked,
        files_failed=0,
        diagnostics=[
            bounded_audio_tool_diagnostic(
                relative_path=relative_path,
                category=category,
                return_code=return_code,
                stderr=detail,
            ),
        ],
        tool_version=tool_version,
    )


def validate_audio(
    folder_path: str,
    mode: str = "normal",
    *,
    read_probe: _AudioReadProbe = _probe_file_readable,
) -> AudioValidationResult:
    """Perform a read-only, audio-only strict FFmpeg full decode.

    Exit-zero stderr is discarded without inspection. A positive FFmpeg exit
    on a fully readable, stable source is bad audio. Failures to perform the
    measurement (filesystem access, process startup, signal termination, or a
    changing source) are reported separately and must not blame the peer.

    Walks subdirectories so multi-disc layouts (``Album/CD1/*.mp3``) are
    validated too. The auto-import path always passes a flattened folder
    so recursion is a no-op there; force-import paths can point at
    user folders with nested discs.
    """
    started_at = time.monotonic()
    if mode == "off":
        return _audio_validation_result(
            outcome="skipped",
            files_checked=0,
            files_failed=0,
            diagnostics=[],
            tool_version="",
            elapsed_seconds=time.monotonic() - started_at,
        )

    if not os.path.isdir(folder_path):
        return _world_failure_result(
            relative_path=".",
            category="read_error",
            detail="audio validation folder is missing or not a directory",
            files_checked=0,
            tool_version="",
        )

    try:
        files = _audio_files_in_folder(folder_path)
    except OSError as exc:
        return _world_failure_result(
            relative_path=".",
            category="read_error",
            detail=str(exc),
            files_checked=0,
            tool_version="",
        )

    if not files:
        return _audio_validation_result(
            outcome="passed",
            files_checked=0,
            files_failed=0,
            diagnostics=[],
            elapsed_seconds=time.monotonic() - started_at,
        )

    diagnostics: list[AudioToolDiagnostic] = []
    omitted_diagnostics = 0
    files_checked = 0
    files_failed = 0
    failed_paths: list[str] = []
    snapshots: dict[str, _AudioSourceSnapshot] = {}
    for filepath in files:
        display = os.path.relpath(filepath, folder_path)
        try:
            snapshot = read_probe(filepath)
        except _AudioSourceChangedError as exc:
            return _world_failure_result(
                relative_path=display,
                category="source_changed",
                detail=str(exc),
                files_checked=files_checked,
                tool_version="",
            )
        except OSError as exc:
            return _world_failure_result(
                relative_path=display,
                category="read_error",
                detail=str(exc),
                files_checked=files_checked,
                tool_version="",
            )
        snapshots[filepath] = snapshot

        try:
            result = sp.run(
                build_audio_validation_argv(filepath),
                capture_output=True,
                timeout=_AUDIO_VALIDATION_TIMEOUT_SECONDS,
            )
        except sp.TimeoutExpired as exc:
            try:
                read_probe(
                    filepath,
                    complete=True,
                    expected=snapshot,
                )
            except _AudioSourceChangedError as read_exc:
                return _world_failure_result(
                    relative_path=display,
                    category="source_changed",
                    detail=str(read_exc),
                    files_checked=files_checked + 1,
                    tool_version="",
                )
            except OSError as read_exc:
                return _world_failure_result(
                    relative_path=display,
                    category="read_error",
                    detail=str(read_exc),
                    files_checked=files_checked + 1,
                    tool_version="",
                )
            files_checked += 1
            files_failed += 1
            failed_paths.append(display)
            diagnostic = bounded_audio_tool_diagnostic(
                relative_path=display,
                category="decode_timeout",
                stderr=exc.stderr,
            )
        except OSError as exc:
            return _world_failure_result(
                relative_path=display,
                category="process_unavailable",
                detail=str(exc),
                files_checked=files_checked,
                tool_version="unavailable",
            )
        else:
            files_checked += 1
            returncode = result.returncode
            if returncode < 0:
                return _world_failure_result(
                    relative_path=display,
                    category="process_interrupted",
                    detail=f"ffmpeg terminated by signal {-returncode}",
                    files_checked=files_checked,
                    return_code=returncode,
                )
            if returncode == 0:
                try:
                    current = _source_snapshot(filepath)
                except OSError as exc:
                    return _world_failure_result(
                        relative_path=display,
                        category="read_error",
                        detail=str(exc),
                        files_checked=files_checked,
                    )
                if current != snapshot:
                    return _world_failure_result(
                        relative_path=display,
                        category="source_changed",
                        detail="source changed during audio validation",
                        files_checked=files_checked,
                    )
                # Exit-zero stderr is deliberately never read, normalized,
                # hashed, classified, logged, or persisted.
                continue

            try:
                read_probe(
                    filepath,
                    complete=True,
                    expected=snapshot,
                )
            except _AudioSourceChangedError as exc:
                return _world_failure_result(
                    relative_path=display,
                    category="source_changed",
                    detail=str(exc),
                    files_checked=files_checked,
                )
            except OSError as exc:
                return _world_failure_result(
                    relative_path=display,
                    category="read_error",
                    detail=str(exc),
                    files_checked=files_checked,
                )
            files_failed += 1
            failed_paths.append(display)
            diagnostic = bounded_audio_tool_diagnostic(
                relative_path=display,
                category=(
                    "decode_error"
                    if returncode == 69
                    else "ffmpeg_failed_unclassified"
                ),
                return_code=returncode,
                stderr=result.stderr,
            )

        if len(diagnostics) < AUDIO_VALIDATION_DIAGNOSTIC_LIMIT:
            diagnostics.append(diagnostic)
        else:
            omitted_diagnostics += 1

    try:
        final_files = _audio_files_in_folder(folder_path)
    except OSError as exc:
        return _world_failure_result(
            relative_path=".",
            category="read_error",
            detail=str(exc),
            files_checked=files_checked,
        )
    if final_files != files:
        return _world_failure_result(
            relative_path=".",
            category="source_changed",
            detail="audio file set changed during validation",
            files_checked=files_checked,
        )
    for filepath, expected in snapshots.items():
        try:
            current = _source_snapshot(filepath)
        except OSError as exc:
            return _world_failure_result(
                relative_path=os.path.relpath(filepath, folder_path),
                category="read_error",
                detail=str(exc),
                files_checked=files_checked,
            )
        if current != expected:
            return _world_failure_result(
                relative_path=os.path.relpath(filepath, folder_path),
                category="source_changed",
                detail="source changed during audio validation",
                files_checked=files_checked,
            )

    return _audio_validation_result(
        outcome=("audio_corrupt" if files_failed else "passed"),
        files_checked=files_checked,
        files_failed=files_failed,
        diagnostics=diagnostics,
        omitted_diagnostics=omitted_diagnostics,
        failed_paths=tuple(failed_paths),
        elapsed_seconds=time.monotonic() - started_at,
    )


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


# === Media server integrations ===

import urllib.error
import urllib.request


# === Plex integration ===


def request_plex_scan(
    cfg: CratediggerConfig,
    imported_path: str | None = None,
) -> tuple[int, str] | None:
    """Submit one Plex refresh and return its status and actual sent path.

    This proves only that Plex accepted the request. Plex returns HTTP 200 for
    invalid paths too, so callers must not treat the status as scan evidence.
    Missing configuration returns ``None``; transport failures raise.
    """
    if not cfg.plex_url:
        return None
    token = cfg.resolved_plex_token()
    if not token:
        return None
    section = cfg.plex_library_section_id or "1"
    url = f"{cfg.plex_url}/library/sections/{section}/refresh?X-Plex-Token={token}"
    scan_path: str | None = None
    if imported_path:
        from urllib.parse import quote
        scan_path = imported_path
        if not os.path.isabs(scan_path) and cfg.beets_directory:
            scan_path = os.path.join(cfg.beets_directory, scan_path)
        if cfg.plex_path_map:
            local_prefix, container_prefix = cfg.plex_path_map.split(":", 1)
            if scan_path.startswith(local_prefix):
                scan_path = container_prefix + scan_path[len(local_prefix):]
            elif not os.path.isabs(scan_path):
                scan_path = container_prefix.rstrip("/") + "/" + scan_path
            else:
                logger.warning(
                    f"PLEX: imported_path {scan_path!r} is absolute but "
                    f"outside path_map local_prefix {local_prefix!r}; "
                    "Plex may silently ignore the partial scan")
        if not os.path.isabs(scan_path):
            logger.warning(
                f"PLEX: imported_path {scan_path!r} is relative and no "
                "beets_directory or plex_path_map is configured to "
                "absolutize it; Plex may silently ignore the partial scan")
        url += f"&path={quote(scan_path, safe='')}"
    safe_url = url.split("X-Plex-Token=")[0] + "X-Plex-Token=<redacted>"
    if "&path=" in url:
        safe_url += "&path=" + url.split("&path=")[1]
    logger.debug(f"PLEX: GET {safe_url}")
    req = urllib.request.Request(url)
    with urllib.request.urlopen(req, timeout=10) as resp:
        status = resp.status
    return status, scan_path or ""


def trigger_plex_scan(cfg: CratediggerConfig, imported_path: str | None = None) -> None:
    """Trigger a Plex library scan after import. Best-effort — failures don't block.

    If imported_path is provided, does a targeted partial scan of just that folder.
    Otherwise triggers a full library section refresh.
    """
    try:
        submitted = request_plex_scan(cfg, imported_path)
        if submitted is None:
            logger.debug("PLEX: skipped scan (url/token not configured)")
            return
        status, scan_path = submitted
        if scan_path:
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


FetchXml = Callable[..., Element]
PutFn = Callable[..., int]


def _notifier_container_path(
    imported_path: str,
    *,
    beets_directory: str | None,
    path_map: str | None,
) -> str | None:
    """Translate a beets ``imported_path`` to the absolute path a media
    server sees on its side of a ``local_prefix:container_prefix`` remap —
    the join key for locating an album in Plex/Jellyfin.

    Mirrors the absolutize + path-map transform used by
    ``request_plex_scan``. It remains a pure helper so lookup callers can
    derive the media-server path without submitting a scan request. Returns
    ``None`` when the result is not absolute (a media server cannot match a
    relative path)."""
    if not imported_path:
        return None
    out = imported_path
    if not os.path.isabs(out) and beets_directory:
        out = os.path.join(beets_directory, out)
    if path_map:
        local_prefix, container_prefix = path_map.split(":", 1)
        if out.startswith(local_prefix):
            out = container_prefix + out[len(local_prefix):]
        elif not os.path.isabs(out):
            out = container_prefix.rstrip("/") + "/" + out
    return out if os.path.isabs(out) else None


def _plex_container_path(cfg: "CratediggerConfig", imported_path: str) -> str | None:
    """The path Plex stores in ``Media.Part.file`` for a beets album folder."""
    return _notifier_container_path(
        imported_path, beets_directory=cfg.beets_directory,
        path_map=cfg.plex_path_map)


def _plex_fetch_xml(cfg: "CratediggerConfig", path: str, **params: str) -> Element:
    """Thin urllib GET → parsed Plex XML. Network leaf seam."""
    from urllib.parse import urlencode
    params = dict(params)
    params["X-Plex-Token"] = cfg.resolved_plex_token() or ""
    url = f"{cfg.plex_url}{path}?{urlencode(params)}"
    req = urllib.request.Request(url, headers={"Accept": "application/xml"})
    with urllib.request.urlopen(req, timeout=15) as resp:
        return ET.fromstring(resp.read(), forbid_dtd=True)


def _plex_put(cfg: "CratediggerConfig", path: str, **params: str) -> int:
    """Thin urllib PUT → HTTP status. Network leaf seam."""
    from urllib.parse import urlencode
    params = dict(params)
    params["X-Plex-Token"] = cfg.resolved_plex_token() or ""
    url = f"{cfg.plex_url}{path}?{urlencode(params)}"
    req = urllib.request.Request(url, method="PUT")
    with urllib.request.urlopen(req, timeout=15) as resp:
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

    def _default_fetch(path: str, **p: str) -> Element:
        return _plex_fetch_xml(cfg, path, **p)

    fetch: FetchXml = fetch_xml or _default_fetch
    section = cfg.plex_library_section_id or "1"
    artist, album = _parse_artist_album(imported_path)
    prefix = container.rstrip("/") + "/"

    def _candidates() -> Iterator[Element]:
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

    def _default_put(path: str, **p: str) -> int:
        return _plex_put(cfg, path, **p)

    put: PutFn = put_fn or _default_put
    status = put(
        f"/library/sections/{section}/all",
        type="9",
        id=str(rating_key),
        **{"addedAt.value": str(int(added_at)), "addedAt.locked": "1"},
    )
    return status == 200


# === Jellyfin integration ===


def trigger_jellyfin_scan(
    cfg: CratediggerConfig,
    imported_path: str | None,
) -> None:
    """Report one changed album directory to Jellyfin, best-effort.

    Jellyfin's filesystem-change endpoint resolves an existing album exactly,
    or walks to its nearest indexed ancestor to discover a genuinely new
    album. It then reconciles that affected item/ancestor with its normal
    metadata and image defaults. A missing/unmappable path is deliberately a
    no-op: post-import notification must never degrade into a collection
    refresh.
    """
    if not cfg.jellyfin_url:
        logger.debug("JELLYFIN: skipped media update (no url configured)")
        return
    try:
        token = cfg.resolved_jellyfin_token()
        if not token:
            logger.debug("JELLYFIN: skipped media update (no token configured)")
            return
        if not imported_path:
            logger.warning("JELLYFIN: skipped media update (no album path)")
            return
        container_path = _jellyfin_container_path(cfg, imported_path)
        if not container_path:
            logger.warning(
                "JELLYFIN: skipped media update for unmappable album path %r",
                imported_path,
            )
            return
        if cfg.jellyfin_path_map:
            _local_prefix, container_prefix = cfg.jellyfin_path_map.split(":", 1)
            if os.path.commonpath((container_path, container_prefix)) != os.path.normpath(
                container_prefix
            ):
                logger.warning(
                    "JELLYFIN: skipped media update outside configured library: %r",
                    container_path,
                )
                return
        payload = {
            "Updates": [{
                "Path": container_path,
                "UpdateType": "Modified",
            }],
        }
        req = urllib.request.Request(
            f"{cfg.jellyfin_url}/Library/Media/Updated",
            data=json.dumps(payload).encode("utf-8"),
            method="POST",
            headers={
                "X-Emby-Token": token,
                "Content-Type": "application/json",
            },
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            resp.read()
        logger.info("JELLYFIN: reported changed album %s", container_path)
    except Exception as e:
        logger.warning(f"JELLYFIN: media update failed: {e}")


def request_jellyfin_refresh(
    cfg: CratediggerConfig,
    item_id: str | None,
) -> tuple[int, str] | None:
    """Submit a targeted Jellyfin refresh, with full refresh on target 404."""
    if not cfg.jellyfin_url:
        return None
    token = cfg.resolved_jellyfin_token()
    if not token:
        return None

    def _post(path: str) -> int:
        req = urllib.request.Request(
            f"{cfg.jellyfin_url}{path}",
            method="POST",
            headers={"X-Emby-Token": token},
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            resp.read()
            return resp.status

    target = f"/Items/{item_id}/Refresh" if item_id else "/Library/Refresh"
    try:
        return _post(target), target
    except urllib.error.HTTPError as exc:
        if exc.code != 404 or target == "/Library/Refresh":
            raise
        fallback = "/Library/Refresh"
        return _post(fallback), fallback


# === Jellyfin DateCreated pin (read + edit) ===
#
# Read/edit half of the Jellyfin "Recently Added" pin feature (migration 046,
# issue #574). The capture/reconcile orchestration lives in
# lib/jellyfin_pin_service.py; these functions are the thin, testable Jellyfin
# client it drives. Verified against Jellyfin 10.11 (2026-07-10): item update
# is POST /Items/{id} with the FULL dto from GET /Items/{id}?userId=… — a
# partial body wipes the omitted metadata fields, so the setter always
# round-trips the fetched dto with only DateCreated changed.


@dataclass(frozen=True)
class JellyfinAlbumRef:
    """A located Jellyfin MusicAlbum: its item id and current ``DateCreated``
    (ISO-8601 string, stored verbatim), plus name/artist for logging."""
    item_id: str
    date_created: str
    name: str = ""
    artist: str = ""


@dataclass(frozen=True)
class JellyfinItemRef:
    """An album child (Audio item): item id and current ``DateCreated``."""
    item_id: str
    date_created: str


JsonGetFn = Callable[..., Any]
JsonPostFn = Callable[[str, Any], int]


def _jellyfin_container_path(cfg: "CratediggerConfig", imported_path: str) -> str | None:
    """The path Jellyfin stores as a MusicAlbum's ``Path`` for a beets album
    folder (Jellyfin sees the library through its own mount, e.g.
    ``/mnt/fuse/Media/Music/Beets/...`` for ``/mnt/virtio/Music/Beets/...``)."""
    return _notifier_container_path(
        imported_path, beets_directory=cfg.beets_directory,
        path_map=cfg.jellyfin_path_map)


def _jellyfin_get_json(cfg: "CratediggerConfig", path: str, **params: str) -> Any:
    """Thin urllib GET → decoded Jellyfin JSON. Network leaf seam."""
    from urllib.parse import urlencode
    url = f"{cfg.jellyfin_url}{path}"
    if params:
        url += f"?{urlencode(params)}"
    req = urllib.request.Request(url, headers={
        "X-Emby-Token": cfg.resolved_jellyfin_token() or "",
        "Accept": "application/json",
    })
    with urllib.request.urlopen(req, timeout=15) as resp:
        return json.loads(resp.read())


def _jellyfin_post_json(cfg: "CratediggerConfig", path: str, payload: Any) -> int:
    """Thin urllib POST of a JSON body → HTTP status. Network leaf seam."""
    req = urllib.request.Request(
        f"{cfg.jellyfin_url}{path}",
        data=json.dumps(payload).encode("utf-8"),
        method="POST",
        headers={
            "X-Emby-Token": cfg.resolved_jellyfin_token() or "",
            "Content-Type": "application/json",
        })
    with urllib.request.urlopen(req, timeout=15) as resp:
        return resp.status


def jellyfin_find_album_by_path(
    cfg: "CratediggerConfig",
    imported_path: str,
    *,
    get_json: JsonGetFn | None = None,
) -> JellyfinAlbumRef | None:
    """Locate the Jellyfin MusicAlbum whose ``Path`` is ``imported_path``'s
    album folder (translated through ``jellyfin_path_map``).

    Narrows candidates by album-title search then artist search (Jellyfin has
    no path-filter API — an unfiltered recursive /Items sweep is too slow to
    be a fallback), and confirms each candidate by exact ``Path`` equality —
    the authoritative join, resilient to the extension change an upgrade
    causes. Returns ``None`` when Jellyfin has no album there (e.g. a
    genuinely-new album not yet scanned). Transport/parse failures raise to
    the caller, which treats Jellyfin work as non-fatal."""
    if not cfg.jellyfin_url:
        return None
    container = _jellyfin_container_path(cfg, imported_path)
    if not container:
        return None
    def _default_get(path: str, **p: str) -> object:
        return _jellyfin_get_json(cfg, path, **p)

    get: JsonGetFn = get_json or _default_get
    artist, album = _parse_artist_album(imported_path)
    target = container.rstrip("/")

    def _candidates() -> Iterator[dict[str, Any]]:
        if album:
            doc = get("/Items", recursive="true",
                      includeItemTypes="MusicAlbum", searchTerm=album,
                      fields="Path,DateCreated", limit="50")
            yield from doc.get("Items", [])
        if artist:
            adoc = get("/Items", recursive="true",
                       includeItemTypes="MusicArtist", searchTerm=artist,
                       limit="10")
            for a in adoc.get("Items", []):
                aid = a.get("Id")
                if not aid:
                    continue
                doc = get("/Items", recursive="true",
                          includeItemTypes="MusicAlbum", albumArtistIds=aid,
                          fields="Path,DateCreated", limit="200")
                yield from doc.get("Items", [])

    seen: set[str] = set()
    for it in _candidates():
        iid = it.get("Id")
        if not iid or iid in seen:
            continue
        seen.add(iid)
        if (it.get("Path") or "").rstrip("/") != target:
            continue
        return JellyfinAlbumRef(
            item_id=iid,
            date_created=it.get("DateCreated") or "",
            name=it.get("Name") or "",
            artist=it.get("AlbumArtist") or "",
        )
    return None


def jellyfin_get_album_children(
    cfg: "CratediggerConfig",
    album_item_id: str,
    *,
    get_json: JsonGetFn | None = None,
) -> list[JellyfinItemRef]:
    """The Audio items under a Jellyfin album — the rows whose ``DateCreated``
    actually drives the 'Recently Added'/Latest ordering. Transport failures
    raise to the caller."""
    def _default_get(path: str, **p: str) -> object:
        return _jellyfin_get_json(cfg, path, **p)

    get: JsonGetFn = get_json or _default_get
    doc = get("/Items", parentId=album_item_id, includeItemTypes="Audio",
              fields="DateCreated", limit="2000")
    out: list[JellyfinItemRef] = []
    for it in doc.get("Items", []):
        iid = it.get("Id")
        if not iid:
            continue
        out.append(JellyfinItemRef(
            item_id=iid, date_created=it.get("DateCreated") or ""))
    return out


def jellyfin_set_date_created(
    cfg: "CratediggerConfig",
    item_id: str,
    date_created: str,
    *,
    get_json: JsonGetFn | None = None,
    post_json: JsonPostFn | None = None,
) -> bool:
    """Set an item's ``DateCreated`` to ``date_created`` (ISO-8601 string).

    Fetches the item's full dto (Jellyfin's update endpoint REPLACES the item
    metadata — omitted fields are wiped, so a partial body is data loss) and
    posts it back with only DateCreated changed. Jellyfin only stamps
    ``DateCreated`` during item creation or changed-file ingestion, so a
    post-update restoration survives ordinary scans without a Plex-style
    field lock. Returns ``True`` on HTTP 200/204.

    The single-item GET needs a userId in Jellyfin 10.11; any user works for
    reading the dto, so the first user on the server is used."""
    if not cfg.jellyfin_url:
        return False
    def _default_get(path: str, **p: str) -> object:
        return _jellyfin_get_json(cfg, path, **p)

    get: JsonGetFn = get_json or _default_get
    post: JsonPostFn = post_json or (
        lambda path, payload: _jellyfin_post_json(cfg, path, payload))
    users = get("/Users")
    if not _is_object_list(users) or not users:
        logger.warning("JELLYFIN PIN: /Users returned no users; cannot edit items")
        return False
    first_user = users[0]
    user_id = first_user.get("Id") if _is_str_object_dict(first_user) else None
    dto = get(f"/Items/{item_id}", userId=str(user_id))
    if not _is_str_object_dict(dto) or not dto.get("Id"):
        logger.warning("JELLYFIN PIN: item %s dto fetch returned no item", item_id)
        return False
    dto["DateCreated"] = date_created
    status = post(f"/Items/{item_id}", dto)
    return status in (200, 204)


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


def setup_logging(config: configparser.RawConfigParser) -> None:
    section: configparser.SectionProxy | dict[str, str] = (
        config["Logging"] if "Logging" in config else {}
    )
    logging.basicConfig(
        level=section.get("level", "INFO"),
        format=section.get(
            "format",
            "[%(levelname)s|%(module)s|L%(lineno)d] %(asctime)s: %(message)s",
        ),
        datefmt=section.get("datefmt", "%Y-%m-%dT%H:%M:%S%z"),
    )
