"""Measured track lengths for the library completeness census (issue #1472).

Finds installed copies whose files contradict their release's listed track
lengths and names the shape of the disagreement with
:func:`lib.track_length_shape.classify_track_length_shape`. It only reports.
Every action on a finding stays the operator's: Mark incomplete, Bad Rip,
Replace or Mark complete.

Two observations are compared and kept apart:

* the release's listed length, from the census's own raw source manifest
  (``SourceComponent.declared_seconds``), and
* the installed file's length, MEASURED from the audio file's header.

Beets' ``items.length`` is never read: it holds the release's declared
length, not the file's (on 2026-11-27 Kurt Vile's track 2 showed 4:49 in
Beets while the file ran 336.3s). ``download_log`` is never read either:
it describes download attempts, not what is installed now.

Installed items are paired with release tracks by release-track identity,
the same keys the census uses (``mb_releasetrackid`` for MusicBrainz, the
synthetic Discogs position id in ``mb_trackid``), never by position.

Reading 95k files over virtiofs is the costly part, and an earlier IO
stress test on that mount kernel-panicked doc1. :class:`TrackLengthMeasurer`
is therefore gentle: one file at a time on one thread, a minimum interval
between reads, a time budget per run, and a cache keyed by a fingerprint of
each album's file paths, sizes and modification times so an unchanged album
is never read twice. Albums the budget does not reach wait for the next run
and are counted as pending.
"""
from __future__ import annotations

import hashlib
import importlib
import json
import logging
import os
import time
from collections import Counter
from collections.abc import Callable, Mapping, Sequence
from typing import Final

import msgspec

from lib.library_completeness import (
    CatalogItem,
    CompletenessFinding,
    LibraryAlbum,
    SourceComponent,
    SourceManifest,
    TrackLengthResult,
)
from lib.media_readiness import mediafile_length_seconds
from lib.sidecar_service import _atomic_write_bytes
from lib.track_length_shape import (
    LengthPair,
    TrackLengthShape,
    classify_track_length_shape,
)

logger = logging.getLogger(__name__)

TRACK_LENGTH_CACHE_FILENAME: Final = "library-track-lengths.json"

#: Defaults for one census run. 20 header reads a second is a trickle next
#: to the stress test that panicked doc1; 20 minutes covers about 24,000
#: files, so the first full pass of the library takes about four daily runs
#: and later runs only read albums whose files changed.
DEFAULT_LENGTH_BUDGET_SECONDS: Final[float] = 1200.0
DEFAULT_LENGTH_READS_PER_SECOND: Final[float] = 20.0


class TrackLengthReadError(RuntimeError):
    """An installed file's length could not be read."""


def read_track_length_seconds(path: str) -> float:
    """Header-only length of one installed file, the way Beets reads it.

    Expected unreadable/corrupt-file failures become
    :class:`TrackLengthReadError`; anything else stays loud.
    """
    error_type: type[BaseException] = getattr(  # noqa: B009 - untyped module
        importlib.import_module("mediafile.exceptions"), "MediaFileError",
    )
    try:
        return mediafile_length_seconds(path)
    except (OSError, ValueError, TypeError) as exc:
        raise TrackLengthReadError(str(exc)) from exc
    except Exception as exc:
        if isinstance(exc, error_type):
            raise TrackLengthReadError(str(exc)) from exc
        raise


# --- pairing ----------------------------------------------------------------


def _pair_items(
    manifest: SourceManifest, items: Sequence[CatalogItem],
) -> dict[str, CatalogItem]:
    """Release-track key -> the ONE installed item carrying it.

    A key two items claim, or an item naming no current component, is left
    out: nothing is paired by guesswork.
    """
    by_key = {component.key: component for component in manifest.components}

    def component_key(item: CatalogItem) -> str | None:
        if item.source_key in by_key:
            return item.source_key
        if manifest.source == "musicbrainz" and item.recording_id:
            matches = [
                component.key for component in manifest.components
                if component.recording_id == item.recording_id
            ]
            if len(matches) == 1:
                return matches[0]
        return None

    keyed = [(component_key(item), item) for item in items]
    counts = Counter(key for key, _ in keyed if key)
    return {
        key: item for key, item in keyed
        if key and counts[key] == 1 and by_key[key].kind == "audio"
    }


def _placements(
    manifest: SourceManifest, audio: Sequence[SourceComponent],
    paired: Mapping[str, CatalogItem],
) -> dict[str, tuple[int | None, int | None, int | None]]:
    """``key -> (medium, medium_index, medium_track_count)``.

    MusicBrainz states all three. Discogs positions do not say which vinyl
    side belongs to which disc, so the disc is the one Beets stored on the
    installed item and the position is the track's order on that disc. That
    order is only trusted when every audio track is installed; otherwise a
    missing track would shift every later number, and the tracks are left
    unplaced (the sentence then names them by title).
    """
    if manifest.source == "musicbrainz":
        return {
            component.key: (
                component.medium, component.medium_index,
                component.medium_track_count,
            )
            for component in audio
        }
    if any(component.key not in paired for component in audio):
        return {component.key: (paired[component.key].disc, None, None)
                for component in audio if component.key in paired}
    per_disc: dict[int | None, list[str]] = {}
    for component in audio:
        per_disc.setdefault(paired[component.key].disc, []).append(component.key)
    placements: dict[str, tuple[int | None, int | None, int | None]] = {}
    for disc, keys in per_disc.items():
        for index, key in enumerate(keys, 1):
            placements[key] = (disc, index, len(keys))
    return placements


def length_pairs(
    album: LibraryAlbum, manifest: SourceManifest,
    measured: Mapping[str, float | None],
) -> list[LengthPair]:
    """One pair per installed release track, measured file vs listed length.

    A file that could not be measured, or a track the release lists no
    length for, still yields a pair with ``0.0`` on that side; the shape
    classifier treats it as unusable.
    """
    audio = [component for component in manifest.components if component.kind == "audio"]
    paired = _pair_items(manifest, album.catalog_items)
    placements = _placements(manifest, audio, paired)
    pairs: list[LengthPair] = []
    for component in audio:
        item = paired.get(component.key)
        if item is None:
            continue
        medium, medium_index, medium_track_count = placements[component.key]
        pairs.append(LengthPair(
            medium=medium,
            medium_index=medium_index,
            medium_track_count=medium_track_count,
            file_seconds=measured.get(item.path) or 0.0,
            listed_seconds=component.declared_seconds or 0.0,
            title=component.title,
            file_name=os.path.basename(item.path),
            component_count=max(1, len(component.sub_component_keys)),
        ))
    return pairs


def _slot_ref(medium: int | None, medium_index: int | None, title: str,
              *, multi_disc: bool) -> str:
    if medium_index is None:
        return f'"{title}"' if title else "one track"
    if multi_disc and medium is not None:
        return f"disc {medium} track {medium_index}"
    return f"track {medium_index}"


def finding_for_shape(shape: TrackLengthShape, *, multi_disc: bool) -> CompletenessFinding:
    """The census finding for one named shape.

    ``detail`` names only the shape and the tracks, never seconds, so it
    stays the same from run to run and an operator's dismissal sticks while
    the same tracks show the same shape. The sentence carries the numbers.
    """
    refs = ", ".join(
        _slot_ref(slot.medium, slot.medium_index, slot.title, multi_disc=multi_disc)
        for slot in shape.slots
    )
    return CompletenessFinding(
        kind="track_length",
        detail=f"{shape.label} ({refs})" if refs else shape.label,
        sentence=shape.sentence,
        probably_harmless=shape.probably_harmless,
    )


def track_length_result(
    album: LibraryAlbum, manifest: SourceManifest,
    measured: Mapping[str, float | None],
) -> TrackLengthResult:
    """Compare one measured album with its release."""
    pairs = length_pairs(album, manifest, measured)
    listed = any(
        pair.listed_seconds > 0 and pair.component_count <= 1 for pair in pairs
    )
    if not listed:
        return TrackLengthResult(status="measured", listed=False)
    shape = classify_track_length_shape(pairs)
    if shape is None:
        return TrackLengthResult(status="measured")
    multi_disc = len({pair.medium for pair in pairs if pair.medium is not None}) > 1
    return TrackLengthResult(
        status="measured", finding=finding_for_shape(shape, multi_disc=multi_disc),
    )


# --- gentle, cached measurement ---------------------------------------------


class CachedAlbumLengths(msgspec.Struct, frozen=True):
    """Lengths of one album's files, aligned with its sorted paths."""

    fingerprint: str
    lengths: tuple[float | None, ...]


def _no_albums() -> dict[str, CachedAlbumLengths]:
    return {}


class TrackLengthCache(msgspec.Struct, frozen=True):
    albums: dict[str, CachedAlbumLengths] = msgspec.field(default_factory=_no_albums)


def track_length_cache_path(var_dir: str) -> str:
    return os.path.join(var_dir, TRACK_LENGTH_CACHE_FILENAME)


def read_track_length_cache(path: str) -> TrackLengthCache:
    """The stored cache; a missing or unreadable one starts empty.

    An unreadable cache costs re-reading files, never a wrong answer, so it
    is logged and replaced rather than failing the census.
    """
    try:
        with open(path, "rb") as fh:
            data = fh.read()
        return msgspec.convert(json.loads(data.decode("ascii")), type=TrackLengthCache)
    except FileNotFoundError:
        return TrackLengthCache()
    except (OSError, UnicodeDecodeError, ValueError, msgspec.ValidationError):
        logger.exception("track-length cache at %s is unreadable; starting empty", path)
        return TrackLengthCache()


def write_track_length_cache(path: str, cache: TrackLengthCache) -> None:
    # ASCII JSON keeps any surrogate-escaped Beets path intact.
    _atomic_write_bytes(path, json.dumps(msgspec.to_builtins(cache)).encode("ascii"))


def album_fingerprint(
    paths: Sequence[str], *, stat: Callable[[str], os.stat_result] = os.stat,
) -> str:
    """Changes when any file is added, removed, resized or rewritten."""
    digest = hashlib.sha256()
    for path in paths:
        try:
            info = stat(path)
            line = f"{path}\t{info.st_size}\t{info.st_mtime_ns}\n"
        except OSError:
            line = f"{path}\t!\n"
        digest.update(line.encode("utf-8", "surrogateescape"))
    return digest.hexdigest()


class TrackLengthMeasurer:
    """Measures each album's files at most once while they stay unchanged.

    ``lengths_for`` returns ``None`` when the album is not cached and this
    run's time budget is spent; the album is measured on a later run. The
    budget clock starts at the first real read, so cached albums cost
    nothing. An album is finished once started, so a run overshoots by at
    most one album. An album with a file that could not be read is used for
    this run but not cached, so a passing IO error is retried next run.
    """

    def __init__(
        self, cache: TrackLengthCache, *,
        budget_seconds: float = DEFAULT_LENGTH_BUDGET_SECONDS,
        reads_per_second: float = DEFAULT_LENGTH_READS_PER_SECOND,
        read_length: Callable[[str], float] = read_track_length_seconds,
        stat: Callable[[str], os.stat_result] = os.stat,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        if reads_per_second <= 0:
            raise ValueError("reads_per_second must be positive")
        if budget_seconds < 0:
            raise ValueError("budget_seconds must not be negative")
        self._previous = cache.albums
        self._kept: dict[str, CachedAlbumLengths] = {}
        self._budget = budget_seconds
        self._interval = 1.0 / reads_per_second
        self._read_length = read_length
        self._stat = stat
        self._clock = clock
        self._sleep = sleep
        self._deadline: float | None = None
        self._last_read: float | None = None
        self.files_read = 0

    def _budget_spent(self) -> bool:
        if self._deadline is None:
            if self._budget <= 0:
                return True
            self._deadline = self._clock() + self._budget
            return False
        return self._clock() >= self._deadline

    def _read(self, path: str) -> float | None:
        if self._last_read is not None:
            wait = self._last_read + self._interval - self._clock()
            if wait > 0:
                self._sleep(wait)
        try:
            seconds = self._read_length(path)
        except TrackLengthReadError as exc:
            logger.info("track length unreadable for %s: %s", path, exc)
            return None
        finally:
            self._last_read = self._clock()
            self.files_read += 1
        return seconds if seconds > 0 else None

    def keep(self, album_id: int) -> None:
        """Carry an album's cached entry forward without measuring it.

        For albums the census saw but could not compare this run (its
        source was unreachable, say): their measurements stay valid.
        """
        entry = self._previous.get(str(album_id))
        if entry is not None:
            self._kept[str(album_id)] = entry

    def lengths_for(
        self, album_id: int, paths: Sequence[str],
    ) -> dict[str, float | None] | None:
        ordered = sorted(set(paths))
        fingerprint = album_fingerprint(ordered, stat=self._stat)
        key = str(album_id)
        cached = self._previous.get(key)
        if (cached is not None and cached.fingerprint == fingerprint
                and len(cached.lengths) == len(ordered)):
            self._kept[key] = cached
            return dict(zip(ordered, cached.lengths, strict=True))
        if self._budget_spent():
            return None
        lengths = tuple(self._read(path) for path in ordered)
        if all(length is not None for length in lengths):
            self._kept[key] = CachedAlbumLengths(
                fingerprint=fingerprint,
                lengths=tuple(
                    round(length, 2) if length is not None else None
                    for length in lengths
                ),
            )
        return dict(zip(ordered, lengths, strict=True))

    def updated_cache(self) -> TrackLengthCache:
        """Entries for the albums this run saw; removed albums drop out."""
        return TrackLengthCache(albums=dict(self._kept))


class TrackLengthCheck:
    """The census's per-album length step (``scan_library_completeness``)."""

    def __init__(self, measurer: TrackLengthMeasurer) -> None:
        self.measurer = measurer

    def __call__(
        self, album: LibraryAlbum, manifest: SourceManifest | None,
    ) -> TrackLengthResult | None:
        if (manifest is None or album.identity is None
                or album.identity.release_id != manifest.release_id
                or album.identity.source != manifest.source
                or not album.catalog_items):
            self.measurer.keep(album.album_id)
            return None
        measured = self.measurer.lengths_for(
            album.album_id, [item.path for item in album.catalog_items],
        )
        if measured is None:
            return TrackLengthResult(status="pending")
        return track_length_result(album, manifest, measured)
