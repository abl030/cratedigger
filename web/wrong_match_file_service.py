"""Filesystem-backed explorer helpers for Wrong Matches candidates."""

from __future__ import annotations

import json
import mimetypes
import os
from urllib.parse import quote
from typing import Any, Mapping

from lib.processing_paths import normalize_source_dirs, path_is_within_root
from lib.quality import AUDIO_EXTENSIONS_DOTTED
from lib.util import resolve_failed_path

_PLAYABLE_AUDIO_EXTENSIONS: frozenset[str] = frozenset({
    ".aac",
    ".flac",
    ".m4a",
    ".mp3",
    ".ogg",
    ".opus",
    ".wav",
})

_AUDIO_MIME_TYPES: dict[str, str] = {
    ".aac": "audio/aac",
    ".flac": "audio/flac",
    ".m4a": "audio/mp4",
    ".mp3": "audio/mpeg",
    ".ogg": "audio/ogg",
    ".opus": "audio/ogg",
    ".wav": "audio/wav",
    ".wma": "audio/x-ms-wma",
}


def _validation_result_dict(raw: Any) -> dict[str, Any]:
    if isinstance(raw, dict):
        return raw
    if not raw:
        return {}
    return json.loads(str(raw))


def source_dirs_from_validation_result(validation_result: Mapping[str, Any]) -> list[str]:
    raw = validation_result.get("source_dirs")
    if not isinstance(raw, list):
        return []
    return normalize_source_dirs(raw)


def _resolved_wrong_match_root(entry: Mapping[str, Any]) -> tuple[dict[str, Any], str]:
    validation_result = _validation_result_dict(entry.get("validation_result"))
    failed_path_raw = validation_result.get("failed_path")
    failed_path = failed_path_raw if isinstance(failed_path_raw, str) else ""
    resolved_path = resolve_failed_path(failed_path)
    if resolved_path is None:
        raise FileNotFoundError(f"Wrong-match files not found: {failed_path or '<missing>'}")
    return validation_result, resolved_path


def _audio_mime_type(path: str) -> str:
    ext = os.path.splitext(path)[1].lower()
    if ext in _AUDIO_MIME_TYPES:
        return _AUDIO_MIME_TYPES[ext]
    guessed, _encoding = mimetypes.guess_type(path)
    if guessed:
        return guessed
    return "application/octet-stream"


def _safe_tag_values(raw: object) -> list[str]:
    values: list[str] = []
    candidates = raw if isinstance(raw, (list, tuple)) else [raw]
    for candidate in candidates:
        if isinstance(candidate, bytes):
            continue
        text = str(candidate).strip()
        if not text:
            continue
        if len(text) > 240:
            text = f"{text[:237]}..."
        if text not in values:
            values.append(text)
    return values


def _inspect_audio_file(path: str) -> tuple[dict[str, list[str]], float | None, int | None]:
    try:
        from mutagen import File as mutagen_file  # type: ignore[import-untyped]
    except ImportError:
        return {}, None, None

    try:
        audio = mutagen_file(path, easy=True)
    except Exception:
        return {}, None, None
    if audio is None:
        return {}, None, None

    tags: dict[str, list[str]] = {}
    raw_tags = getattr(audio, "tags", None)
    if hasattr(raw_tags, "items"):
        for key, value in raw_tags.items():
            safe_values = _safe_tag_values(value)
            if safe_values:
                tags[str(key)] = safe_values

    info = getattr(audio, "info", None)
    length_raw = getattr(info, "length", None)
    bitrate_raw = getattr(info, "bitrate", None)
    length = float(length_raw) if isinstance(length_raw, (int, float)) else None
    bitrate_bps = int(bitrate_raw) if isinstance(bitrate_raw, (int, float)) else None
    return tags, length, bitrate_bps


def build_wrong_match_explorer(
    *,
    download_log_id: int,
    entry: Mapping[str, Any],
) -> dict[str, object]:
    validation_result, root = _resolved_wrong_match_root(entry)
    files: list[dict[str, object]] = []
    other_file_count = 0

    for dirpath, dirnames, filenames in os.walk(root):
        dirnames.sort()
        filenames.sort()
        for filename in filenames:
            abs_path = os.path.join(dirpath, filename)
            ext = os.path.splitext(filename)[1].lower()
            if ext not in AUDIO_EXTENSIONS_DOTTED:
                other_file_count += 1
                continue

            rel_path = os.path.relpath(abs_path, root).replace(os.sep, "/")
            tags, duration_seconds, bitrate_bps = _inspect_audio_file(abs_path)
            try:
                size_bytes = os.path.getsize(abs_path)
            except OSError:
                size_bytes = None
            playable = ext in _PLAYABLE_AUDIO_EXTENSIONS
            files.append({
                "relative_path": rel_path,
                "filename": filename,
                "directory": os.path.dirname(rel_path),
                "format": ext[1:].upper(),
                "mime_type": _audio_mime_type(abs_path),
                "playable": playable,
                "duration_seconds": duration_seconds,
                "bitrate_kbps": (
                    int(round(bitrate_bps / 1000))
                    if isinstance(bitrate_bps, int) and bitrate_bps > 0
                    else None
                ),
                "size_bytes": size_bytes,
                "tags": tags,
                "stream_url": (
                    "/api/wrong-matches/audio"
                    f"?download_log_id={int(download_log_id)}"
                    f"&path={quote(rel_path)}"
                    if playable else None
                ),
            })

    return {
        "status": "ok",
        "download_log_id": int(download_log_id),
        "failed_path": root,
        "folder_name": os.path.basename(root),
        "source_dirs": source_dirs_from_validation_result(validation_result),
        "audio_file_count": len(files),
        "other_file_count": other_file_count,
        "files": files,
    }


def resolve_wrong_match_stream_file(
    *,
    entry: Mapping[str, Any],
    relative_path: str,
) -> tuple[str, str]:
    _validation_result, root = _resolved_wrong_match_root(entry)
    cleaned_relative_path = str(relative_path or "").replace("\\", os.sep).strip()
    if not cleaned_relative_path:
        raise ValueError("Missing path")

    abs_path = os.path.abspath(os.path.join(root, cleaned_relative_path))
    if not path_is_within_root(abs_path, root):
        raise ValueError("Path escapes wrong-match root")
    if not os.path.isfile(abs_path):
        raise FileNotFoundError(f"Wrong-match file not found: {cleaned_relative_path}")

    ext = os.path.splitext(abs_path)[1].lower()
    if ext not in AUDIO_EXTENSIONS_DOTTED:
        raise ValueError("Requested file is not an audio file")

    return abs_path, _audio_mime_type(abs_path)
