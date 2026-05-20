"""Filesystem-backed explorer helpers for Wrong Matches candidates."""

from __future__ import annotations

import json
import mimetypes
import os
import re
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

_SKIPPED_TAG_KEYS: frozenset[str] = frozenset({
    "covr",
    "metadata_block_picture",
})

_SKIPPED_TAG_PREFIXES: tuple[str, ...] = (
    "apic",
)

_RAW_ID3_TAG_ALIASES: dict[str, str] = {
    "talb": "album",
    "tcon": "genre",
    "tdrc": "date",
    "tit2": "title",
    "tpe1": "artist",
    "tpe2": "albumartist",
    "tpos": "discnumber",
    "trck": "tracknumber",
    "tso2": "albumartistsort",
    "tsop": "artistsort",
}

_TXXX_TAG_ALIASES: dict[str, str] = {
    "asin": "asin",
    "barcode": "barcode",
    "catalog number": "catalognumber",
    "catalognumber": "catalognumber",
    "encoded by": "encodedby",
    "musicbrainz album artist id": "musicbrainz_albumartistid",
    "musicbrainz album id": "musicbrainz_albumid",
    "musicbrainz album release country": "musicbrainz_albumreleasecountry",
    "musicbrainz artist id": "musicbrainz_artistid",
    "musicbrainz release group id": "musicbrainz_releasegroupid",
    "musicbrainz release track id": "musicbrainz_releasetrackid",
    "musicbrainz track id": "musicbrainz_trackid",
    "musicbrainz work id": "musicbrainz_workid",
}


def _target_candidate(validation_result: Mapping[str, Any]) -> dict[str, Any] | None:
    raw_candidates = validation_result.get("candidates")
    if not isinstance(raw_candidates, list):
        return None
    candidates = [
        candidate for candidate in raw_candidates
        if isinstance(candidate, dict)
    ]
    target = next(
        (candidate for candidate in candidates if candidate.get("is_target")),
        None,
    )
    return target if target is not None else (candidates[0] if candidates else None)


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
    text_values = getattr(raw, "text", None)
    candidates = text_values if isinstance(text_values, (list, tuple)) else (
        raw if isinstance(raw, (list, tuple)) else [raw]
    )
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


def _normalized_tag_key(raw_key: object) -> str | None:
    key = str(raw_key or "").strip()
    if not key:
        return None

    lowered = key.lower()
    if lowered in _SKIPPED_TAG_KEYS:
        return None
    if any(lowered.startswith(prefix) for prefix in _SKIPPED_TAG_PREFIXES):
        return None
    if lowered in _RAW_ID3_TAG_ALIASES:
        return _RAW_ID3_TAG_ALIASES[lowered]
    if lowered.startswith("txxx:"):
        descriptor = lowered.split(":", 1)[1].strip()
        if descriptor in _TXXX_TAG_ALIASES:
            return _TXXX_TAG_ALIASES[descriptor]
        normalized = re.sub(r"[^a-z0-9]+", "_", descriptor).strip("_")
        return normalized or None
    return lowered


def _normalized_file_basename(path: object) -> str:
    text = str(path or "").strip()
    if not text:
        return ""
    return os.path.basename(text).casefold()


def _normalized_title(text: object) -> str:
    return str(text or "").strip().casefold()


def _parse_position(value: object) -> int | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        parsed = int(value)
        return parsed if parsed > 0 else None
    text = str(value).strip()
    if not text:
        return None
    head = text.split("/", 1)[0].strip()
    if not head.isdigit():
        return None
    parsed = int(head)
    return parsed if parsed > 0 else None


def _file_identity(file_data: Mapping[str, Any]) -> tuple[str, str, int | None, int | None]:
    tags = file_data.get("tags")
    tag_map = tags if isinstance(tags, dict) else {}
    title_values = tag_map.get("title")
    track_values = tag_map.get("tracknumber")
    disc_values = tag_map.get("discnumber")
    basename = _normalized_file_basename(file_data.get("relative_path") or file_data.get("filename"))
    title = _normalized_title(title_values[0] if isinstance(title_values, list) and title_values else "")
    track = _parse_position(track_values[0] if isinstance(track_values, list) and track_values else None)
    disc = _parse_position(disc_values[0] if isinstance(disc_values, list) and disc_values else None)
    return basename, title, track, disc


def _mapping_identity(mapping_row: Mapping[str, Any]) -> tuple[str, str, int | None, int | None]:
    item = mapping_row.get("item")
    item_map = item if isinstance(item, dict) else {}
    basename = _normalized_file_basename(item_map.get("path"))
    title = _normalized_title(item_map.get("title"))
    track = _parse_position(item_map.get("track"))
    disc = _parse_position(item_map.get("disc"))
    return basename, title, track, disc


def _mapping_sort_key(mapping_row: Mapping[str, Any], fallback_index: int) -> tuple[int, int, int]:
    track = mapping_row.get("track")
    track_map = track if isinstance(track, dict) else {}
    medium = _parse_position(track_map.get("medium")) or 1
    medium_index = _parse_position(track_map.get("medium_index"))
    index = _parse_position(track_map.get("index"))
    primary = medium_index if medium_index is not None else (index if index is not None else fallback_index + 1)
    return medium, primary, fallback_index


def _reorder_files_by_match(
    files: list[dict[str, object]],
    validation_result: Mapping[str, Any],
) -> tuple[list[dict[str, object]], str]:
    target = _target_candidate(validation_result)
    if not target:
        return files, "folder"

    raw_mapping = target.get("mapping")
    if not isinstance(raw_mapping, list) or not raw_mapping:
        return files, "folder"

    file_entries = [
        {
            "original_index": index,
            "file": file_data,
            "identity": _file_identity(file_data),
        }
        for index, file_data in enumerate(files)
    ]
    unmatched_indexes = set(range(len(file_entries)))
    matched_positions: dict[int, int] = {}

    mapping_rows = [
        (fallback_index, mapping_row)
        for fallback_index, mapping_row in enumerate(raw_mapping)
        if isinstance(mapping_row, dict)
    ]
    mapping_rows.sort(key=lambda row: _mapping_sort_key(row[1], row[0]))

    for match_position, (_fallback_index, mapping_row) in enumerate(mapping_rows, start=1):
        basename, title, track, disc = _mapping_identity(mapping_row)
        candidates = sorted(unmatched_indexes)

        exact_basename = [
            idx for idx in candidates
            if basename and file_entries[idx]["identity"][0] == basename
        ]
        if len(exact_basename) == 1:
            chosen = exact_basename[0]
        else:
            title_track_disc = [
                idx for idx in candidates
                if title and file_entries[idx]["identity"][1] == title
                and file_entries[idx]["identity"][2] == track
                and file_entries[idx]["identity"][3] == disc
            ]
            if len(title_track_disc) == 1:
                chosen = title_track_disc[0]
            else:
                title_track = [
                    idx for idx in candidates
                    if title and file_entries[idx]["identity"][1] == title
                    and file_entries[idx]["identity"][2] == track
                ]
                if len(title_track) == 1:
                    chosen = title_track[0]
                else:
                    exact_title = [
                        idx for idx in candidates
                        if title and file_entries[idx]["identity"][1] == title
                    ]
                    chosen = exact_title[0] if len(exact_title) == 1 else None

        if chosen is None:
            continue
        matched_positions[chosen] = match_position
        unmatched_indexes.discard(chosen)

    if len(matched_positions) != len(files):
        return files, "folder"

    reordered_entries = sorted(
        file_entries,
        key=lambda entry: matched_positions.get(entry["original_index"], 10**9),
    )
    reordered: list[dict[str, object]] = []
    for entry in reordered_entries:
        file_data = entry["file"]
        file_data["matched_order"] = matched_positions.get(entry["original_index"])
        reordered.append(file_data)
    return reordered, "matched"


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
    if raw_tags is not None and hasattr(raw_tags, "items"):
        for raw_key, value in raw_tags.items():
            key = _normalized_tag_key(raw_key)
            if key is None:
                continue
            safe_values = _safe_tag_values(value)
            if safe_values:
                existing = tags.setdefault(key, [])
                for safe_value in safe_values:
                    if safe_value not in existing:
                        existing.append(safe_value)

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

    files, ordered_by = _reorder_files_by_match(files, validation_result)

    return {
        "status": "ok",
        "download_log_id": int(download_log_id),
        "failed_path": root,
        "folder_name": os.path.basename(root),
        "source_dirs": source_dirs_from_validation_result(validation_result),
        "audio_file_count": len(files),
        "other_file_count": other_file_count,
        "ordered_by": ordered_by,
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
