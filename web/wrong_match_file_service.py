"""Filesystem-backed explorer helpers for Wrong Matches candidates."""

from __future__ import annotations

import mimetypes
import os
import re
import stat
from urllib.parse import quote
from dataclasses import dataclass
from typing import Any, Mapping, Sequence, TypeGuard

from lib.json_narrow import (
    is_object_list as _is_object_list,
    is_str_object_dict as _is_str_object_dict,
)
from lib.config import read_runtime_config
from lib.fs_authority import (
    FilesystemAuthorityError,
    OpenedRegularFile,
    open_directory_path,
    open_regular_relative,
)
from lib.processing_paths import (
    normalize_source_dirs,
    path_is_within_root,
    processing_albums_dir,
)
from lib.quality import AUDIO_EXTENSIONS_DOTTED
from lib.validation_envelope import (
    ValidationResultEnvelope,
    decode_validation_envelope,
)


def _is_object_sequence(value: object) -> TypeGuard[Sequence[object]]:
    """Narrow a decoded-JSON value to a list/tuple, precisely typed.

    Same rationale as :func:`_is_str_object_dict` — bare
    ``isinstance(value, (list, tuple))`` erases the element type to
    ``Unknown``; the ``TypeGuard`` declares ``Sequence[object]`` instead
    with no change to the runtime check.
    """
    return isinstance(value, (list, tuple))


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

_EXPLORER_MAX_DEPTH = 32
_EXPLORER_MAX_FILES = 5000
_EXPLORER_MAX_BYTES = 100 * 1024**3


def target_candidate(
    validation_result: ValidationResultEnvelope,
) -> dict[str, Any] | None:
    """Return the target candidate (or first) from a decoded envelope."""
    candidates = validation_result.candidates
    target = next(
        (candidate for candidate in candidates if candidate.get("is_target")),
        None,
    )
    return target if target is not None else (candidates[0] if candidates else None)


def source_dirs_from_validation_result(
    validation_result: ValidationResultEnvelope,
) -> list[str]:
    return normalize_source_dirs(validation_result.source_dirs)


def _resolved_wrong_match_root(
    entry: Mapping[str, Any],
) -> tuple[ValidationResultEnvelope, str]:
    validation_result = decode_validation_envelope(entry.get("validation_result"))
    failed_path = validation_result.failed_path or ""
    cfg = read_runtime_config()
    roots = (
        os.path.join(cfg.slskd_download_dir, "failed_imports"),
        os.path.join(cfg.slskd_download_dir, "wrong_matches"),
        os.path.join(cfg.beets_staging_dir, "failed_imports"),
        os.path.join(processing_albums_dir(cfg.processing_dir), "failed_imports"),
        os.path.join(processing_albums_dir(cfg.processing_dir), "wrong_matches"),
    )
    candidates = [failed_path] if os.path.isabs(failed_path) else [
        os.path.join(root, failed_path) for root in roots
    ]
    for candidate in candidates:
        if not any(path_is_within_root(candidate, root) for root in roots):
            continue
        try:
            with open_directory_path(candidate):
                pass
        except FilesystemAuthorityError:
            continue
        return validation_result, os.path.abspath(candidate)
    raise FileNotFoundError(f"Wrong-match files not found or unauthorized: {failed_path or '<missing>'}")


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
    candidates: Sequence[object] = (
        text_values if _is_object_sequence(text_values)
        else raw if _is_object_sequence(raw)
        else [raw]
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
    tag_map: dict[str, object] = tags if _is_str_object_dict(tags) else {}
    title_values = tag_map.get("title")
    track_values = tag_map.get("tracknumber")
    disc_values = tag_map.get("discnumber")
    basename = _normalized_file_basename(file_data.get("relative_path") or file_data.get("filename"))
    title_first: object = (
        title_values[0]
        if _is_object_list(title_values) and title_values else "")
    track_first: object = (
        track_values[0]
        if _is_object_list(track_values) and track_values else None)
    disc_first: object = (
        disc_values[0]
        if _is_object_list(disc_values) and disc_values else None)
    title = _normalized_title(title_first)
    track = _parse_position(track_first)
    disc = _parse_position(disc_first)
    return basename, title, track, disc


def _mapping_identity(mapping_row: Mapping[str, Any]) -> tuple[str, str, int | None, int | None]:
    item = mapping_row.get("item")
    item_map: dict[str, object] = item if _is_str_object_dict(item) else {}
    basename = _normalized_file_basename(item_map.get("path"))
    title = _normalized_title(item_map.get("title"))
    track = _parse_position(item_map.get("track"))
    disc = _parse_position(item_map.get("disc"))
    return basename, title, track, disc


def _mapping_sort_key(mapping_row: Mapping[str, Any], fallback_index: int) -> tuple[int, int, int]:
    track = mapping_row.get("track")
    track_map: dict[str, object] = track if _is_str_object_dict(track) else {}
    medium = _parse_position(track_map.get("medium")) or 1
    medium_index = _parse_position(track_map.get("medium_index"))
    index = _parse_position(track_map.get("index"))
    primary = medium_index if medium_index is not None else (index if index is not None else fallback_index + 1)
    return medium, primary, fallback_index


@dataclass
class _FileEntry:
    original_index: int
    file: dict[str, object]
    identity: tuple[str, str, int | None, int | None]


def _reorder_files_by_match(
    files: list[dict[str, object]],
    validation_result: ValidationResultEnvelope,
) -> tuple[list[dict[str, object]], str]:
    target = target_candidate(validation_result)
    if not target:
        return files, "folder"

    raw_mapping = target.get("mapping")
    if not _is_object_list(raw_mapping) or not raw_mapping:
        return files, "folder"

    file_entries = [
        _FileEntry(
            original_index=index,
            file=file_data,
            identity=_file_identity(file_data),
        )
        for index, file_data in enumerate(files)
    ]
    unmatched_indexes = set(range(len(file_entries)))
    matched_positions: dict[int, int] = {}

    mapping_rows: list[tuple[int, dict[str, object]]] = [
        (fallback_index, mapping_row)
        for fallback_index, mapping_row in enumerate(raw_mapping)
        if _is_str_object_dict(mapping_row)
    ]
    mapping_rows.sort(key=lambda row: _mapping_sort_key(row[1], row[0]))

    for match_position, (_fallback_index, mapping_row) in enumerate(mapping_rows, start=1):
        basename, title, track, disc = _mapping_identity(mapping_row)
        candidates = sorted(unmatched_indexes)

        exact_basename = [
            idx for idx in candidates
            if basename and file_entries[idx].identity[0] == basename
        ]
        if len(exact_basename) == 1:
            chosen = exact_basename[0]
        else:
            title_track_disc = [
                idx for idx in candidates
                if title and file_entries[idx].identity[1] == title
                and file_entries[idx].identity[2] == track
                and file_entries[idx].identity[3] == disc
            ]
            if len(title_track_disc) == 1:
                chosen = title_track_disc[0]
            else:
                title_track = [
                    idx for idx in candidates
                    if title and file_entries[idx].identity[1] == title
                    and file_entries[idx].identity[2] == track
                ]
                if len(title_track) == 1:
                    chosen = title_track[0]
                else:
                    exact_title = [
                        idx for idx in candidates
                        if title and file_entries[idx].identity[1] == title
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
        key=lambda entry: matched_positions.get(entry.original_index, 10**9),
    )
    reordered: list[dict[str, object]] = []
    for entry in reordered_entries:
        file_data = entry.file
        file_data["matched_order"] = matched_positions.get(entry.original_index)
        reordered.append(file_data)
    return reordered, "matched"


def _inspect_audio_file(handle: int) -> tuple[dict[str, list[str]], float | None, int | None]:
    try:
        # getattr (not `from mutagen import File`) keeps this Any-typed:
        # mutagen's File() factory has an untyped `filething` parameter and
        # a partially-unknown overloaded return (many mutagen format
        # classes) — third-party, not ours to annotate. Same pattern as
        # harness/import_one.py::_probe_source_channels.
        import mutagen
        _mutagen_file = getattr(mutagen, "File")
    except ImportError:
        return {}, None, None

    try:
        with os.fdopen(os.dup(handle), "rb") as source:
            audio = _mutagen_file(source, easy=True)
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
    scanned_file_count = 0
    scanned_bytes = 0
    truncated_reason: str | None = None
    with open_directory_path(root) as root_fd:
        stack: list[tuple[int, str, int]] = [(os.dup(root_fd), "", 0)]
        try:
            while stack and truncated_reason is None:
                directory_fd, relative_dir, depth = stack.pop()
                try:
                    entries = sorted(list(os.scandir(directory_fd)), key=lambda entry: entry.name)
                    for directory_entry in entries:
                        if directory_entry.is_symlink():
                            continue
                        relative = f"{relative_dir}/{directory_entry.name}".strip("/")
                        try:
                            info = directory_entry.stat(follow_symlinks=False)
                        except OSError:
                            continue
                        if stat.S_ISDIR(info.st_mode):
                            if depth >= _EXPLORER_MAX_DEPTH:
                                truncated_reason = "depth_limit"
                                break
                            try:
                                child_fd = os.open(
                                    directory_entry.name,
                                    os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC | os.O_NOFOLLOW,
                                    dir_fd=directory_fd,
                                )
                            except OSError:
                                continue
                            stack.append((child_fd, relative, depth + 1))
                            continue
                        if not stat.S_ISREG(info.st_mode):
                            continue
                        if scanned_file_count >= _EXPLORER_MAX_FILES:
                            truncated_reason = "file_limit"
                            break
                        if scanned_bytes + info.st_size > _EXPLORER_MAX_BYTES:
                            truncated_reason = "byte_limit"
                            break
                        scanned_file_count += 1
                        scanned_bytes += info.st_size
                        ext = os.path.splitext(directory_entry.name)[1].lower()
                        if ext not in AUDIO_EXTENSIONS_DOTTED:
                            other_file_count += 1
                            continue
                        try:
                            opened = open_regular_relative(directory_fd, directory_entry.name)
                        except FilesystemAuthorityError:
                            continue
                        try:
                            tags, duration_seconds, bitrate_bps = _inspect_audio_file(opened.fd)
                        finally:
                            opened.close()
                        playable = ext in _PLAYABLE_AUDIO_EXTENSIONS
                        files.append({
                            "relative_path": relative,
                            "filename": directory_entry.name,
                            "directory": os.path.dirname(relative),
                            "format": ext[1:].upper(),
                            "mime_type": _audio_mime_type(directory_entry.name),
                            "playable": playable,
                            "duration_seconds": duration_seconds,
                            "bitrate_kbps": int(round(bitrate_bps / 1000)) if isinstance(bitrate_bps, int) and bitrate_bps > 0 else None,
                            "size_bytes": info.st_size,
                            "tags": tags,
                            "stream_url": "/api/wrong-matches/audio" f"?download_log_id={int(download_log_id)}" f"&path={quote(relative)}" if playable else None,
                        })
                finally:
                    os.close(directory_fd)
        finally:
            for pending_fd, _relative, _depth in stack:
                os.close(pending_fd)

    files, ordered_by = _reorder_files_by_match(files, validation_result)

    return {
        "status": "ok",
        "download_log_id": int(download_log_id),
        "failed_path": root,
        "folder_name": os.path.basename(root),
        "source_dirs": source_dirs_from_validation_result(validation_result),
        "audio_file_count": len(files),
        "other_file_count": other_file_count,
        "partial": truncated_reason is not None,
        "truncated_reason": truncated_reason,
        "scanned_file_count": scanned_file_count,
        "scanned_bytes": scanned_bytes,
        "ordered_by": ordered_by,
        "files": files,
    }


def resolve_wrong_match_stream_file(
    *,
    entry: Mapping[str, Any],
    relative_path: str,
) -> tuple[OpenedRegularFile, str]:
    _validation_result, root = _resolved_wrong_match_root(entry)
    cleaned_relative_path = str(relative_path or "").replace("\\", os.sep).strip()
    if not cleaned_relative_path:
        raise ValueError("Missing path")

    ext = os.path.splitext(cleaned_relative_path)[1].lower()
    if ext not in AUDIO_EXTENSIONS_DOTTED:
        raise ValueError("Requested file is not an audio file")
    try:
        with open_directory_path(root) as root_fd:
            opened = open_regular_relative(root_fd, cleaned_relative_path)
    except FilesystemAuthorityError as exc:
        raise FileNotFoundError(f"Wrong-match file not found: {cleaned_relative_path}") from exc
    return opened, _audio_mime_type(cleaned_relative_path)
