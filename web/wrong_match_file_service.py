"""Filesystem-backed explorer helpers for Wrong Matches candidates."""

from __future__ import annotations

import mimetypes
import os
import re
import stat
from collections.abc import Generator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, TypeGuard
from urllib.parse import quote

from lib.config import read_runtime_config
from lib.fs_authority import (
    FilesystemAuthorityError,
    HeldDirectory,
    OpenedRegularFile,
    errno_proves_absence,
    is_containment_refusal,
    open_configured_quarantine_directory,
    open_regular_relative,
    refusal_is_indeterminate,
    unreadable_reason_text,
)
from lib.json_narrow import (
    is_object_list as _is_object_list,
)
from lib.json_narrow import (
    is_str_object_dict as _is_str_object_dict,
)
from lib.media_readiness import MediaReadinessError, media_facts_for_open_file
from lib.processing_paths import (
    normalize_source_dirs,
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
_EXPLORER_MAX_ENTRIES = 5000
_EXPLORER_MAX_FILES = 5000
_EXPLORER_MAX_BYTES = 100 * 1024**3


@dataclass(frozen=True)
class WrongMatchExplorerLimits:
    """Bounded traversal policy for one Wrong Matches explorer response."""

    max_depth: int = _EXPLORER_MAX_DEPTH
    max_entries: int = _EXPLORER_MAX_ENTRIES
    max_files: int = _EXPLORER_MAX_FILES
    max_bytes: int = _EXPLORER_MAX_BYTES


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


class WrongMatchSourceUnavailable(OSError):
    """The storage refused to answer for a Wrong Matches source.

    Its own type so the routes answer 503 instead of 404: EACCES on the
    private tree — or the ESTALE/EIO this deployment's nested virtiofs
    really produces — is not evidence that the files are not found
    (issue #1063).
    """


@contextmanager
def _opened_wrong_match_root(
    entry: Mapping[str, object],
    *,
    cfg: object | None = None,
) -> Generator[tuple[ValidationResultEnvelope, HeldDirectory]]:
    """Yield a validation envelope and its held authoritative directory."""
    validation_result = decode_validation_envelope(entry.get("validation_result"))
    failed_path = validation_result.failed_path or ""
    runtime_config = cfg or read_runtime_config()
    try:
        with open_configured_quarantine_directory(failed_path, runtime_config) as root:
            yield validation_result, root
    except FilesystemAuthorityError as exc:
        if refusal_is_indeterminate(exc.code) is True:
            raise WrongMatchSourceUnavailable(
                "Wrong-match files could not be read: "
                f"{failed_path or '<missing>'} ({exc})",
            ) from exc
        raise FileNotFoundError(
            f"Wrong-match files not found or unauthorized: {failed_path or '<missing>'}",
        ) from exc


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


def _inspect_audio_file(
    handle: int,
    *,
    container: str,
) -> tuple[dict[str, list[str]], float | None, int | None]:
    # Stream facts do not depend on tag parsing.  Preserve them when an
    # unusual-but-decode-valid file has tags Mutagen cannot open.
    try:
        facts = media_facts_for_open_file(handle, container=container)
        length = facts.duration_seconds
        bitrate_bps = (
            facts.average_bitrate_kbps * 1000
            if facts.average_bitrate_kbps is not None else None
        )
    except MediaReadinessError:
        length = None
        bitrate_bps = None

    try:
        # getattr (not `from mutagen import File`) keeps this Any-typed:
        # mutagen's File() factory has an untyped `filething` parameter and
        # a partially-unknown overloaded return (many mutagen format
        # classes) — third-party, not ours to annotate. Same pattern as
        # harness/import_one.py::_probe_source_channels.
        import mutagen
        _mutagen_file = getattr(mutagen, "File")  # noqa: B009 - dynamic untyped factory
    except ImportError:
        return {}, length, bitrate_bps

    try:
        with os.fdopen(os.dup(handle), "rb") as source:
            audio = _mutagen_file(source, easy=True)
    except Exception:  # noqa: BLE001 - boundary converts or isolates collaborator failures
        return {}, length, bitrate_bps
    if audio is None:
        return {}, length, bitrate_bps

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

    return tags, length, bitrate_bps


def build_wrong_match_explorer(
    *,
    download_log_id: int,
    entry: Mapping[str, Any],
    cfg: object | None = None,
    limits: WrongMatchExplorerLimits | None = None,
) -> dict[str, object]:
    effective_limits = limits or WrongMatchExplorerLimits()
    files: list[dict[str, object]] = []
    other_file_count = 0
    scanned_file_count = 0
    scanned_bytes = 0
    entries_seen = 0
    truncated_reason: str | None = None
    unreadable_entry_count = 0
    unreadable_reason: str | None = None
    # Structured discriminator alongside ``unreadable_reason`` — the same
    # "what do we call it" / "should the JS branch differ" split
    # ``BeetsDistanceResult.partial_read_is_containment`` makes on the
    # distance side (issue #1086). ``True`` for the first recorded
    # refusal that was a containment DECISION (a symlink, socket, FIFO or
    # device node), ``False`` for an ordinary world failure, ``None``
    # when nothing was refused. The frontend needs this to stop offering
    # a Retry that re-fetching can never satisfy for a containment
    # refusal, and to stop leading with wording that implies the world
    # might just clear up.
    unreadable_is_containment: bool | None = None
    with _opened_wrong_match_root(entry, cfg=cfg) as (validation_result, root):
        root_fd = root.fd
        def scan_directory(directory_fd: int, relative_dir: str, depth: int) -> None:
            """Walk depth-first so a broad tree cannot exhaust descriptors."""
            nonlocal entries_seen, other_file_count, scanned_bytes
            nonlocal scanned_file_count, truncated_reason
            nonlocal unreadable_entry_count, unreadable_reason
            nonlocal unreadable_is_containment
            try:
                names: list[str] = []
                with os.scandir(directory_fd) as entries:
                    for directory_entry in entries:
                        entries_seen += 1
                        if entries_seen > effective_limits.max_entries:
                            truncated_reason = "entry_limit"
                            break
                        names.append(directory_entry.name)
                if truncated_reason is not None:
                    return
                names.sort()
                for name in names:
                    if truncated_reason is not None:
                        return
                    relative = f"{relative_dir}/{name}".strip("/")
                    try:
                        child_fd = os.open(
                            name,
                            os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC | os.O_NOFOLLOW,
                            dir_fd=directory_fd,
                        )
                    except OSError:
                        child_fd = -1
                    if child_fd >= 0:
                        try:
                            if not stat.S_ISDIR(os.fstat(child_fd).st_mode):
                                continue
                            if depth >= effective_limits.max_depth:
                                truncated_reason = "depth_limit"
                                return
                            next_fd = child_fd
                            child_fd = -1
                            scan_directory(next_fd, relative, depth + 1)
                        finally:
                            if child_fd >= 0:
                                os.close(child_fd)
                        continue
                    try:
                        opened = open_regular_relative(directory_fd, name)
                    except FilesystemAuthorityError as exc:
                        # An entry we were REFUSED is not an entry that is
                        # not there. Both swallow sites funnel here: an
                        # unreadable subdirectory fails its O_DIRECTORY
                        # probe above and lands on this open too. Dropping
                        # these silently let an intact album render as a
                        # confident empty folder — the panel the operator
                        # reads before deciding to delete (issue #1063).
                        #
                        # The gate is ``errno_proves_absence``, NOT
                        # ``refusal_is_indeterminate(...) is True``. The
                        # latter answers "is this retryable", a different
                        # question from "did we learn this entry isn't
                        # there" — and answers ``False`` for a symlink loop,
                        # a socket, and a driverless device node, none of
                        # which prove anything is absent. Keying off it
                        # silently dropped ``ELOOP``/``ENXIO``/``ENODEV``
                        # here while the beets-distance path (which asks
                        # ``errno_proves_absence``) reported the same folder
                        # as incomplete (issue #1086).
                        if not errno_proves_absence(exc.code):
                            unreadable_entry_count += 1
                            if unreadable_reason is None:
                                # Honest per-code copy: a symlink/special-file
                                # refusal is a containment decision, not a
                                # world failure, and must not be worded like
                                # one (issue #1086 part 2).
                                unreadable_reason = (
                                    f"{relative}: "
                                    f"{unreadable_reason_text(exc.code, errno_symbol=exc.errno_symbol)}"
                                )
                                # Structured, not string-sniffed (same rule
                                # as ``BeetsDistanceResult.partial_read_is_containment``):
                                # the frontend must branch on THIS, never on
                                # ``unreadable_reason`` text.
                                unreadable_is_containment = is_containment_refusal(exc.code)
                        continue
                    try:
                        info = opened.stat_result
                        if scanned_file_count >= effective_limits.max_files:
                            truncated_reason = "file_limit"
                            return
                        if scanned_bytes + info.st_size > effective_limits.max_bytes:
                            truncated_reason = "byte_limit"
                            return
                        scanned_file_count += 1
                        scanned_bytes += info.st_size
                        ext = os.path.splitext(name)[1].lower()
                        if ext not in AUDIO_EXTENSIONS_DOTTED:
                            other_file_count += 1
                            continue
                        tags, duration_seconds, bitrate_bps = _inspect_audio_file(
                            opened.fd, container=ext[1:],
                        )
                    finally:
                        opened.close()
                    playable = ext in _PLAYABLE_AUDIO_EXTENSIONS
                    files.append({
                        "relative_path": relative,
                        "filename": name,
                        "directory": os.path.dirname(relative),
                        "format": ext[1:].upper(),
                        "mime_type": _audio_mime_type(name),
                        "playable": playable,
                        "duration_seconds": duration_seconds,
                        "bitrate_kbps": round(bitrate_bps / 1000) if isinstance(bitrate_bps, int) and bitrate_bps > 0 else None,
                        "size_bytes": info.st_size,
                        "tags": tags,
                        "stream_url": "/api/wrong-matches/audio" f"?download_log_id={int(download_log_id)}" f"&path={quote(relative)}" if playable else None,
                    })
            finally:
                os.close(directory_fd)

        scan_directory(os.dup(root_fd), "", 0)

    files, ordered_by = _reorder_files_by_match(files, validation_result)

    nothing_readable = not files and other_file_count == 0
    return {
        # An empty listing is only "ok" when we were allowed to look at
        # everything. With refusals recorded and nothing readable, this is
        # an unavailable folder, never a confidently empty one (#1063).
        "status": (
            "unavailable"
            if nothing_readable and unreadable_entry_count
            else "ok"
        ),
        "download_log_id": int(download_log_id),
        "failed_path": root.display_path,
        "folder_name": os.path.basename(root.display_path),
        "source_dirs": source_dirs_from_validation_result(validation_result),
        "audio_file_count": len(files),
        "other_file_count": other_file_count,
        # ``partial`` means "this listing is incomplete", for either
        # reason; the two reasons stay in separate, distinguishable
        # fields — ``truncated_reason`` remains LIMITS only.
        "partial": truncated_reason is not None or unreadable_entry_count > 0,
        "truncated_reason": truncated_reason,
        "unreadable_entry_count": unreadable_entry_count,
        "unreadable_reason": unreadable_reason,
        # Structured discriminator, same rule as
        # ``BeetsDistanceResult.partial_read_is_containment``: ``True``
        # for a containment refusal (symlink/socket/FIFO/device node),
        # ``False`` for an ordinary world failure, ``None`` when nothing
        # was refused (issue #1086).
        "unreadable_is_containment": unreadable_is_containment,
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
    cleaned_relative_path = str(relative_path or "").replace("\\", os.sep).strip()
    if not cleaned_relative_path:
        raise ValueError("Missing path")

    ext = os.path.splitext(cleaned_relative_path)[1].lower()
    if ext not in AUDIO_EXTENSIONS_DOTTED:
        raise ValueError("Requested file is not an audio file")
    # Classify the per-FILE refusal HERE. ``_opened_wrong_match_root`` is a
    # context manager, so a refusal raised inside its ``with`` is thrown
    # back into the generator and converted there — attributing a single
    # unreadable file to the whole folder. Neither replacement type is a
    # ``FilesystemAuthorityError``, so both pass through untouched.
    with _opened_wrong_match_root(entry) as (_validation_result, root):
        try:
            opened = open_regular_relative(root.fd, cleaned_relative_path)
        except FilesystemAuthorityError as exc:
            if refusal_is_indeterminate(exc.code) is True:
                raise WrongMatchSourceUnavailable(
                    f"Wrong-match file could not be read: "
                    f"{cleaned_relative_path} ({exc})",
                ) from exc
            raise FileNotFoundError(
                f"Wrong-match file not found: {cleaned_relative_path}",
            ) from exc
    return opened, _audio_mime_type(cleaned_relative_path)
