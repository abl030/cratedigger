"""Read-only exact-source / Beets / filesystem completeness census (#1149).

This deliberately answers only whether an installed exact pressing is
complete.  It does not repair files, mutate Beets, or use pipeline request
tracks as a source of truth.  Source components, Beets item paths, and the
audio files in the album directory remain three independent observations.
"""
from __future__ import annotations

import importlib
import json
import os
import stat
import urllib.error
from collections.abc import Callable, Iterable, Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import Literal, Protocol

import msgspec

from lib.beets_db import beets_authority_availability_category
from lib.composite_audio_gap import (
    CompositeAudioReadError,
    detect_composite_silence_gap,
)
from lib.mb_canonical import CanonicalReleaseRedirected, TaggedCanonicalReleaseFn
from lib.quality import AUDIO_EXTENSIONS_DOTTED
from lib.release_identity import ReleaseIdentity

SourceKind = Literal["audio", "non_audio"]
CompletenessStatus = Literal["complete", "incomplete", "unknown", "beets_unavailable"]


class SourceManifestError(ValueError):
    """Expected raw-source shape/identity failure; callers publish unknown."""


class AudioTagReadError(RuntimeError):
    """Expected corrupt/unreadable audio-tag boundary failure."""


class SourceComponent(msgspec.Struct, frozen=True):
    """One source-declared release component; ``key`` is source-native.

    ``sub_component_titles``/``sub_component_keys`` (aligned, same order)
    are populated (len >= 2) only when this component is a coalesced
    Discogs group -- consecutive flat indexed sub-positions (e.g.
    ``16.1``/``16.2``) or a subindexed ``sub_tracks`` header that Beets
    catalogues as ONE physical track (issue #1237). Empty for every
    ordinary component, including MusicBrainz's. ``sub_component_keys``
    lets ``classify_album`` recognise when Beets' #1183 flat retry
    installed each sub-position as its OWN separate catalogued item
    (issue #1237 review C1) -- that is a complete import, not missing.
    """
    key: str
    title: str
    kind: SourceKind
    recording_id: str | None = None
    sub_component_titles: tuple[str, ...] = ()
    sub_component_keys: tuple[str, ...] = ()


class SourceManifest(msgspec.Struct, frozen=True):
    source: Literal["musicbrainz", "discogs"]
    release_id: str
    components: tuple[SourceComponent, ...]


class CompletenessFinding(msgspec.Struct, frozen=True):
    kind: Literal["missing_source_audio", "catalog_drift", "unknown"]
    detail: str


class CompletenessAlbum(msgspec.Struct, frozen=True):
    album_id: int
    artist: str
    title: str
    release_id: str
    findings: tuple[CompletenessFinding, ...]
    source_audio_components: int
    physical_audio_files: int
    catalog_items: int


class CompletenessCounts(msgspec.Struct, frozen=True):
    albums_scanned: int
    audio_complete: int
    missing_source_audio: int
    catalog_drift: int
    unknown: int


class CompletenessReport(msgspec.Struct, frozen=True):
    status: CompletenessStatus
    counts: CompletenessCounts
    albums: tuple[CompletenessAlbum, ...]
    unavailable_detail: str | None = None


class CatalogItem(msgspec.Struct, frozen=True):
    path: str
    source_key: str
    recording_id: str
    title: str = ""
    track: int | None = None


@dataclass(frozen=True)
class LibraryAlbum:
    album_id: int
    artist: str
    title: str
    identity: ReleaseIdentity | None
    directory: str
    catalog_items: tuple[CatalogItem, ...]
    refused_paths: tuple[str, ...] = ()


class CompletenessBeets(Protocol):
    def list_library_completeness_albums(self) -> list[LibraryAlbum]: ...


def _raw_list(value: object, detail: str) -> list[object]:
    if not isinstance(value, list):
        raise SourceManifestError(detail)
    return msgspec.convert(value, type=list[object])


def _raw_mapping(value: object, detail: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise SourceManifestError(detail)
    try:
        return msgspec.convert(value, type=dict[str, object])
    except msgspec.ValidationError as exc:
        raise SourceManifestError(detail) from exc


def musicbrainz_manifest(
    release_id: str, raw: Mapping[str, object], *,
    resolve_redirect: TaggedCanonicalReleaseFn | None = None,
) -> SourceManifest:
    """Normalize raw MB media without losing release-track/recording identity.

    A release track id is the primary source key.  ``recording.video`` is
    source authority for excluding a non-audio component from completeness.
    """
    raw_release_id = raw.get("id")
    if not isinstance(raw_release_id, str):
        raise SourceManifestError("MusicBrainz raw release identity is unavailable or mismatched")
    redirect = (
        resolve_redirect(release_id)
        if raw_release_id != release_id and resolve_redirect else None
    )
    if raw_release_id != release_id and (
        not isinstance(redirect, CanonicalReleaseRedirected)
        or redirect.survivor != raw_release_id
    ):
        # ``web.mb`` follows a 301 before returning its raw body. A body ID
        # alone is not proof that it represents this installed release: the
        # canonical resolver separately observes the transport redirect and
        # names its survivor. Discogs has no equivalent identity pathway.
        raise SourceManifestError("MusicBrainz raw release identity is unavailable or mismatched")
    components: list[SourceComponent] = []
    media = _raw_list(raw.get("media"), "MusicBrainz raw release has no media list")
    for raw_medium in media:
        medium = _raw_mapping(raw_medium, "MusicBrainz medium is not an object")
        tracks = _raw_list(medium.get("tracks"), "MusicBrainz medium has no tracks list")
        # MusicBrainz treats a pregap as a real release-track component.
        entries: list[object] = []
        pregap = medium.get("pregap")
        if pregap is not None:
            entries.append(pregap)
        entries.extend(tracks)
        for raw_track in entries:
            track = _raw_mapping(raw_track, "MusicBrainz track is not an object")
            key = track.get("id")
            raw_recording = track.get("recording")
            if not isinstance(key, str) or not key:
                raise SourceManifestError("MusicBrainz track lacks release-track identity")
            recording = _raw_mapping(raw_recording, "MusicBrainz track lacks release-track identity")
            recording_id = recording.get("id")
            if not isinstance(recording_id, str) or not recording_id:
                raise SourceManifestError("MusicBrainz track lacks recording identity")
            video = recording.get("video")
            if not isinstance(video, bool):
                raise SourceManifestError("MusicBrainz recording lacks video boolean")
            name = track.get("title")
            title = name if isinstance(name, str) else ""
            components.append(SourceComponent(
                key=key, title=title, kind="non_audio" if video else "audio",
                recording_id=recording_id,
            ))
    return _valid_manifest("musicbrainz", release_id, components)


def _discogs_get_track_index(position: str) -> tuple[str | None, str | None, str | None]:
    """Beets' own Discogs position parser, resolved dynamically.

    Calls ``beetsplug.discogs.DiscogsPlugin.get_track_index`` -- the exact
    static parser ``_subtrack_position`` itself delegates to -- via
    ``importlib``/``getattr`` rather than a static import. This is the
    same seam ``harness/beets_compat.py`` already uses to reach into this
    package: it avoids requiring third-party type stubs for
    ``beetsplug.discogs`` under production-strict Pyright, and fails
    closed (``SourceManifestError``) rather than silently trusting an
    unexpected return shape.
    """
    module = importlib.import_module("beetsplug.discogs")
    plugin_class = getattr(module, "DiscogsPlugin", None)
    get_track_index = getattr(plugin_class, "get_track_index", None)
    if not callable(get_track_index):
        raise SourceManifestError("Beets Discogs plugin lacks callable get_track_index")
    raw: object = get_track_index(position)
    try:
        return msgspec.convert(raw, type=tuple[str | None, str | None, str | None])
    except msgspec.ValidationError as exc:
        raise SourceManifestError(
            "Beets Discogs get_track_index returned an unsupported shape"
        ) from exc


def _discogs_subtrack_group_key(position: str) -> str | None:
    """Physical-track key for a literal Discogs position, or ``None``.

    Mirrors ``beetsplug.discogs.DiscogsPlugin._subtrack_position`` exactly
    by calling its own ``get_track_index`` static parser -- no local regex
    copy to drift from the real Beets Discogs plugin. ``None`` means the
    position carries no subtrack index (e.g. vinyl sides ``A1``/``B2``);
    non-``None`` is the ``(medium, medium_index)`` prefix consecutive
    entries must share to be the SAME physical track (e.g. ``16.1`` and
    ``16.2`` both key to ``"16"``).
    """
    medium, index, subindex = _discogs_get_track_index(position)
    if not subindex:
        return None
    return f"{medium or ''}{index or ''}"


def _try_coalesce_nested_index(
    release_id: str, header: Mapping[str, object], sub_list: list[object],
) -> SourceComponent | None:
    """Mirror ``DiscogsPlugin._coalesce_index_track``'s subindexed branch.

    Issue #1237 review C6: verified empirically against the real plugin
    (``object.__new__(DiscogsPlugin)._coalesce_tracks(...)``) -- when the
    FIRST nested ``sub_tracks`` child carries a subtrack index, Beets
    catalogues the WHOLE header as ONE physical track keyed at that
    child's STRIPPED ``medium+medium_index`` position (NOT the child's own
    literal position -- e.g. first child ``"A2.1"`` yields key ``"A2"``,
    even though the flat-sibling case would key it ``"A2.1"``), titled by
    the INDEX's OWN title, never the children's. Even a single subindexed
    child triggers this (empirically confirmed), so there is no size
    threshold here. Returns ``None`` when the first child has no
    subtrack index, so the caller falls back to Beets' OTHER real branch:
    literal per-child expansion (this module's pre-existing, unchanged
    ``sub_tracks`` flattening).

    Issue #1237 review D5/E4: observed at review time, not a structural
    guarantee this repository can verify on its own (the mirror is an
    external service, not vendored code) -- a manual sample of all 410
    live library releases (3,965 track entries) plus 300 further sampled
    mirror releases found the deployed Discogs mirror never emitting
    ``sub_tracks``; every sampled entry returned a fixed ``{artists,
    duration, position, title}`` schema instead. At the time of that
    sample this branch had no live producer; it corrects a real
    divergence from the real Beets plugin in a code path that already
    existed before this issue (the pre-existing literal-per-child
    flattening below), independent of whether the mirror's shape has
    since changed.
    """
    if not sub_list:
        return None
    first = _raw_mapping(sub_list[0], "Discogs track is not an object")
    first_position = first.get("position")
    if not isinstance(first_position, str) or not first_position:
        return None
    medium, index, subindex = _discogs_get_track_index(first_position)
    if not subindex:
        return None
    child_keys: list[str] = []
    child_titles: list[str] = []
    for raw_child in sub_list:
        child = _raw_mapping(raw_child, "Discogs track is not an object")
        position = child.get("position")
        if not isinstance(position, str) or not position:
            raise SourceManifestError("Discogs track lacks literal position")
        title = child.get("title")
        child_keys.append(f"{release_id}-{position}")
        child_titles.append(title if isinstance(title, str) else "")
    header_title = header.get("title")
    stripped_position = f"{medium or ''}{index or ''}"
    return SourceComponent(
        key=f"{release_id}-{stripped_position}",
        title=header_title if isinstance(header_title, str) else "",
        kind="audio",
        sub_component_titles=tuple(child_titles) if len(child_titles) > 1 else (),
        sub_component_keys=tuple(child_keys) if len(child_keys) > 1 else (),
    )


def discogs_manifest(release_id: str, raw: Mapping[str, object]) -> SourceManifest:
    """Normalize raw Discogs tracks, reproducing Beets' own coalescing.

    Beets (2.13.1, deployed) groups CONSECUTIVE flat indexed sub-positions
    sharing the same ``_discogs_subtrack_group_key`` into one physical
    component keyed by the FIRST sub-position, titled by joining every
    sub-component's title with ``" / "`` (``DiscogsPlugin._merge_subtracks``).
    A2/B-side positions (``A1``, ``B2``) never carry a subtrack index and
    so never group; ``1A``/``4A``/``1B`` each form their own singleton
    group because consecutive keys differ (issue #1237).

    A ``sub_tracks``-nested header (Discogs' distinct index/heading
    container -- measured absent from the deployed mirror as of issue
    #1237 review D5; see ``_try_coalesce_nested_index``) reproduces
    Beets' OWN nested branch instead
    (``_try_coalesce_nested_index`` / ``_coalesce_index_track``, issue
    #1237 review C6) -- subindexed children merge into one track keyed at
    the stripped first-child position with the header's own title;
    non-subindexed children flatten literally, unchanged. Either way, a
    nested header breaks any pending TOP-LEVEL group, like a real Beets
    non-"track" entry breaks ``groupby``'s adjacency.
    """
    raw_release_id = raw.get("id")
    if (isinstance(raw_release_id, bool)
            or not isinstance(raw_release_id, (int, str))
            or str(raw_release_id) != release_id):
        raise SourceManifestError("Discogs raw release identity is unavailable or mismatched")
    tracks = _raw_list(raw.get("tracks"), "Discogs raw release has no tracks list")
    components: list[SourceComponent] = []
    pending: list[tuple[str, str]] = []
    pending_key: str | None = None

    def flush_pending() -> None:
        nonlocal pending, pending_key
        if not pending:
            return
        first_position, _ = pending[0]
        titles = tuple(title for _, title in pending)
        keys = tuple(f"{release_id}-{position}" for position, _ in pending)
        components.append(SourceComponent(
            key=f"{release_id}-{first_position}",
            title=" / ".join(titles),
            kind="audio",
            sub_component_titles=titles if len(pending) > 1 else (),
            sub_component_keys=keys if len(pending) > 1 else (),
        ))
        pending = []
        pending_key = None

    def visit(entries: Iterable[object], *, groupable: bool) -> None:
        nonlocal pending, pending_key
        for raw_entry in entries:
            entry = _raw_mapping(raw_entry, "Discogs track is not an object")
            subtracks = entry.get("sub_tracks")
            if subtracks is not None:
                if groupable:
                    flush_pending()
                sub_list = _raw_list(subtracks, "Discogs sub_tracks is not a list")
                merged = _try_coalesce_nested_index(release_id, entry, sub_list)
                if merged is not None:
                    components.append(merged)
                else:
                    # Beets' non-subindexed branch: independent physical
                    # tracks grouped under a heading -- preserve each
                    # child literal position (unchanged pre-#1237
                    # behaviour, not grouped by the top-level pass).
                    visit(sub_list, groupable=False)
                continue
            position = entry.get("position")
            duration = entry.get("duration")
            # The deployed mirror flattens Discogs' index/side headings into
            # ordinary rows. A literal empty position AND empty duration is
            # that non-playable header shape; an absent/nonempty duration is
            # ambiguous and must not be silently discarded.
            if position == "" and duration == "":
                if groupable:
                    flush_pending()
                continue
            if not isinstance(position, str) or not position:
                raise SourceManifestError("Discogs track lacks literal position")
            title = entry.get("title")
            title_str = title if isinstance(title, str) else ""
            if not groupable:
                components.append(SourceComponent(
                    key=f"{release_id}-{position}", title=title_str, kind="audio",
                ))
                continue
            group_key = _discogs_subtrack_group_key(position)
            if group_key is not None and group_key == pending_key:
                pending.append((position, title_str))
                continue
            flush_pending()
            if group_key is not None:
                pending = [(position, title_str)]
                pending_key = group_key
            else:
                components.append(SourceComponent(
                    key=f"{release_id}-{position}", title=title_str, kind="audio",
                ))
    visit(tracks, groupable=True)
    flush_pending()
    return _valid_manifest("discogs", release_id, components)


def _valid_manifest(
    source: Literal["musicbrainz", "discogs"], release_id: str,
    components: list[SourceComponent],
) -> SourceManifest:
    if not release_id or any(not component.key for component in components):
        raise SourceManifestError("source manifest has blank identity")
    keys = [component.key for component in components]
    if len(keys) != len(set(keys)):
        raise SourceManifestError("source manifest has duplicate identity")
    if not components:
        raise SourceManifestError("source manifest has no playable components")
    return SourceManifest(source=source, release_id=release_id, components=tuple(components))


def read_audio_tag_identities(
    path: str, *,
    media_file_factory: Callable[[str], object] | None = None,
    mediafile_error_type: type[BaseException] | None = None,
) -> tuple[str, str]:
    """Read the exact release-track and recording tags of one audio file."""
    # mediafile has no type stubs. Resolve the concrete error class from its
    # exceptions module—not the package root—while confining the untyped
    # boundary to these two accesses.
    factory: Callable[[str], object]
    error_type: type[BaseException]
    if media_file_factory is None:
        factory = getattr(  # noqa: B009 - untyped third-party module
            importlib.import_module("mediafile"), "MediaFile",
        )
    else:
        factory = media_file_factory
    if mediafile_error_type is None:
        error_type = getattr(  # noqa: B009 - untyped third-party module
            importlib.import_module("mediafile.exceptions"), "MediaFileError",
        )
    else:
        error_type = mediafile_error_type
    try:
        media = factory(path)
        release_track = getattr(media, "mb_releasetrackid", "") or ""
        recording = getattr(media, "mb_trackid", "") or ""
        if not isinstance(release_track, str) or not isinstance(recording, str):
            raise TypeError("audio tag identity is not text")
        return release_track, recording
    except (OSError, ValueError, TypeError) as exc:
        # mediafile's corrupt/unsupported-file branch is not an OSError.
        # Keep all other programmer/import defects loud.
        raise AudioTagReadError(str(exc)) from exc
    except Exception as exc:
        if isinstance(exc, error_type):
            raise AudioTagReadError(str(exc)) from exc
        raise


def enumerate_audio_files(directory: str) -> tuple[str, ...]:
    """Exact recursive physical audio inventory; no inferred track arithmetic."""
    if not os.path.isdir(directory):
        raise FileNotFoundError(f"album directory is unavailable: {directory}")

    def raise_walk_error(error: OSError) -> None:
        raise error

    paths: list[str] = []
    for root, _dirs, names in os.walk(directory, onerror=raise_walk_error):
        for name in names:
            path = os.path.join(root, name)
            if os.path.splitext(name)[1].lower() in AUDIO_EXTENSIONS_DOTTED:
                if not stat.S_ISREG(os.lstat(path).st_mode):
                    raise OSError(f"audio inventory entry is not a regular file: {path}")
                paths.append(path)
    return tuple(sorted(paths))


def classify_album(
    album: LibraryAlbum, manifest: SourceManifest,
    *, enumerate_files: Callable[[str], tuple[str, ...]] = enumerate_audio_files,
    tag_reader: Callable[[str], tuple[str, str]] = read_audio_tag_identities,
    detect_composite_gap: Callable[[str], bool] = detect_composite_silence_gap,
) -> CompletenessAlbum:
    """Classify one album with independent source/catalog/filesystem evidence.

    The finding list is intentionally nonexclusive.  Any malformed source,
    containment refusal, directory error, or unreadable noncatalogued audio
    turns the answer into ``unknown`` rather than guessing missingness.

    ``detect_composite_gap`` is issue #1237's physical instrument: for a
    coalesced Discogs component (``sub_component_titles`` len >= 2) whose
    installed item IS present by identity, it decides from the audio
    itself whether the file plausibly covers the whole declared program.
    Only ever called for such a component -- an ordinary or genuinely
    absent component never reaches it.
    """
    findings: list[CompletenessFinding] = []
    if album.identity is None or album.identity.release_id != manifest.release_id:
        return _unknown(album, manifest, "exact release identity unavailable or mismatched")
    if album.identity.source != manifest.source:
        return _unknown(album, manifest, "source pathway mismatched")
    source_keys = [component.key for component in manifest.components]
    if not source_keys or any(not key for key in source_keys) or len(source_keys) != len(set(source_keys)):
        return _unknown(album, manifest, "source manifest has unsafe component identities")
    if album.refused_paths:
        return _unknown(album, manifest, "catalog path refused by library containment")
    try:
        physical = enumerate_files(album.directory)
    except OSError as exc:
        return _unknown(album, manifest, f"physical inventory unreadable: {exc}")
    catalog_paths = {item.path for item in album.catalog_items}
    physical_paths = set(physical)
    if catalog_paths != physical_paths:
        findings.append(CompletenessFinding(
            kind="catalog_drift",
            detail=(f"uncatalogued={len(physical_paths - catalog_paths)} "
                    f"catalogued_missing={len(catalog_paths - physical_paths)}"),
        ))

    audio = [component for component in manifest.components if component.kind == "audio"]
    audio_keys = {component.key for component in audio}
    components_by_key = {component.key: component for component in manifest.components}

    def current_component_key(source_key: str, recording_id: str) -> str | None:
        """Return the one current source component this identity safely names."""
        if source_key in components_by_key:
            return source_key
        if recording_id:
            matches = [component.key for component in manifest.components
                       if component.recording_id == recording_id]
            if len(matches) == 1:
                return matches[0]
        return None

    existing_catalog = [item for item in album.catalog_items if item.path in physical_paths]
    known_component_keys: set[str] = set()
    # Tracks, for a component matched by exact identity, the ONE physical
    # path that satisfied it -- issue #1237's grouped-composite physical
    # check below reads the installed audio from here, never a guess.
    component_key_paths: dict[str, str] = {}
    unmatched_mb_witnesses = 0
    for item in existing_catalog:
        key = current_component_key(item.source_key, item.recording_id)
        if key:
            known_component_keys.add(key)
            component_key_paths[key] = item.path
        elif manifest.source == "musicbrainz":
            # A physically present Beets item with historical MB entities can
            # still be the current missing source component. It is not proof
            # of absence unless the complete program fallback vouches for it.
            unmatched_mb_witnesses += 1

    unknown_extra = False
    # Issue #1237 review E3/G3/G6: a physically-present, correctly-tagged
    # uncatalogued file can name a SUB-position of a coalesced Discogs
    # group (e.g. "1-16.2") that is NOT a top-level ``components_by_key``
    # entry -- true of every sub-position except the group's own FIRST
    # one (which IS the group's top-level key, so ``matching_keys`` below
    # already recognises it). Recorded as a set of identities -- the
    # matching physical path is never consumed here, only membership --
    # so the grouped-composite subtraction further down can treat a
    # REMAINING sub-position as satisfied too, symmetrically with the
    # FIRST position's own ``component_key_paths`` route
    # (``test_uncatalogued_present_first_position_still_reaches_
    # decode``). Deliberately does not change ``known_component_keys`` or
    # ``unknown_extra`` semantics above.
    uncatalogued_sub_identities: set[str] = set()
    for path in physical_paths - catalog_paths:
        try:
            release_track, recording = tag_reader(path)
        except AudioTagReadError as exc:
            unknown_extra = True
            findings.append(CompletenessFinding("unknown", f"unreadable uncatalogued audio: {exc}"))
            continue
        if manifest.source == "discogs":
            matching_keys = {
                identity for identity in (release_track, recording)
                if identity in components_by_key
            }
            if len(matching_keys) == 1:
                known_component_keys.update(matching_keys)
                (matched_key,) = matching_keys
                component_key_paths[matched_key] = path
            elif len(matching_keys) > 1:
                unknown_extra = True
                findings.append(CompletenessFinding(
                    "unknown", "uncatalogued audio has conflicting exact Discogs identities",
                ))
            elif not release_track and not recording:
                unknown_extra = True
                findings.append(CompletenessFinding(
                    "unknown", "uncatalogued audio lacks exact source identity",
                ))
            for identity in (release_track, recording):
                if identity:
                    uncatalogued_sub_identities.add(identity)
        elif manifest.source == "musicbrainz":
            key = current_component_key(release_track, recording)
            if key:
                known_component_keys.add(key)
            elif release_track or recording:
                # Current MB track/recording entities can churn. A readable
                # stale extra is therefore an uncertainty witness, not an
                # unrelated file that permits a definite missing verdict.
                unmatched_mb_witnesses += 1
            else:
                unknown_extra = True
                findings.append(CompletenessFinding(
                    "unknown", "uncatalogued audio lacks exact source identity",
                ))

    # Issue #1237, design item 5: for a coalesced Discogs group (>= 2
    # declared sub-components) whose installed item IS present by identity,
    # identity alone cannot say whether the file covers the WHOLE program --
    # decide from the audio. Never runs for an ordinary or absent component,
    # never overrides identity, and never touches ``known_component_keys``:
    # it only ever ADDS evidence alongside the identity-driven verdict below.
    if manifest.source == "discogs":
        existing_source_keys = {item.source_key for item in existing_catalog}
        for component in audio:
            if len(component.sub_component_titles) < 2:
                continue
            if component.key not in known_component_keys:
                continue
            # Issue #1237 review C9: a Discogs "(silence)" sub-position is
            # a literal filler/gap marker, not real audio -- never expected
            # to have its own installed item, never named as missing.
            real_subcomponents = [
                (key, title) for key, title in zip(
                    component.sub_component_keys, component.sub_component_titles,
                    strict=True,
                )
                if title.strip().casefold() != "(silence)"
            ]
            # Issue #1237 review C1 (live regression): subtract sub-positions
            # Beets' own #1183 flat retry already installed as SEPARATE
            # catalogued items -- one per literal sub-position (e.g.
            # 2823685-A2.1 AND 2823685-A2.2 both present) is a COMPLETE
            # import, not missing, and needs no audio decode at all.
            # Issue #1237 review E3: the same subtraction applies when a
            # sub-position is satisfied by an uncatalogued-but-correctly-
            # identified physical file, not only a catalogued one -- the
            # group's FIRST position could already reach ``component_key_
            # paths`` this way; the REMAINING positions must too, or a
            # genuinely complete composite (one part catalogued, the other
            # merely uncatalogued) is falsely accused.
            remaining = [
                (key, title) for key, title in real_subcomponents
                if key != component.key
                and key not in existing_source_keys
                and key not in uncatalogued_sub_identities
            ]
            if not remaining:
                continue
            path = component_key_paths.get(component.key)
            if path is None:
                continue
            # C9/D7/G1: all THREE messages built from ``real_label``/
            # ``real_titles`` below -- the composite-unreadable ``unknown``,
            # the unknown_extra-suppressed ``unknown``, and
            # ``missing_source_audio`` -- name only the REAL sub-components;
            # a silence marker was never real audio, and the label must be
            # consistent across every branch (previously only the
            # missing_source_audio branch filtered it, so an "unknown"
            # finding for the same group still spelled out "(silence)").
            # Only the missing_source_audio message also COUNTS them
            # (``len(real_titles)``); the other two name but do not count.
            real_titles = tuple(title for _, title in real_subcomponents)
            real_label = " / ".join(real_titles) or component.key
            try:
                gap_present = detect_composite_gap(path)
            except CompositeAudioReadError as exc:
                findings.append(CompletenessFinding(
                    "unknown", f"{real_label}: composite audio unreadable: {exc}",
                ))
                continue
            if gap_present:
                continue
            # Issue #1237 review D1/E1: mirror the identity-driven verdict's
            # own ``not unknown_extra`` behaviour below, not only its
            # CONDITION. In this subsystem a gap PROVES completeness, so
            # ``not gap_present`` here is a definite ABSENCE of a gap, not a
            # "definite gap" (D1's own docstring had that inverted) -- and
            # while an unreadable/unidentified uncatalogued extra file's
            # identity is unresolved, that absence must not become a
            # SILENT drop of the composite's own evidence (D1 shipped that
            # gap: unknown_extra suppressed the accusation but emitted
            # nothing naming the composite at all). Emit the SAME kind of
            # explanatory ``unknown`` the identity-driven verdict emits for
            # its own ``missing and unknown_extra`` case, naming this
            # composite specifically.
            if unknown_extra:
                findings.append(CompletenessFinding(
                    "unknown",
                    f"{real_label}: uncatalogued extra audio could satisfy "
                    "the missing part -- installed composite shows no "
                    "internal silence gap",
                ))
            else:
                findings.append(CompletenessFinding(
                    "missing_source_audio",
                    f"{real_label}: installed composite is one continuous "
                    "audio segment; no internal silence gap found across "
                    f"{len(real_titles)} declared components "
                    f"({' / '.join(real_titles)})",
                ))

    # A catalogued missing path is already catalog drift.  It cannot satisfy
    # source audio, but is not unreadable physical evidence either.
    missing: set[str] = audio_keys - known_component_keys
    # Historical MB track entities can be replaced upstream while the exact
    # release/program is unchanged (the Moana Deluxe live control). IDs are
    # still preferred, but a *whole* one-to-one program may vouch safely when
    # Beets' global track ordinals and nonblank titles agree in source order.
    # A partial or titleless sequence remains unknown rather than positional
    # invention.
    if (manifest.source == "musicbrainz" and missing
            and _safe_program_fallback(audio, existing_catalog, physical_paths)):
        known_component_keys.update(audio_keys)
        missing = set()
    if missing and not unknown_extra and not unmatched_mb_witnesses:
        labels = [component.title or component.key for component in audio if component.key in missing]
        findings.append(CompletenessFinding("missing_source_audio", ", ".join(labels)))
    elif missing:
        if unknown_extra:
            detail = "unreadable extra audio could satisfy missing source component"
        else:
            detail = ("current MusicBrainz identities do not match "
                      f"{unmatched_mb_witnesses} installed audio component(s)")
        findings.append(CompletenessFinding("unknown", detail))
    return CompletenessAlbum(
        album_id=album.album_id, artist=album.artist, title=album.title,
        release_id=manifest.release_id, findings=tuple(findings),
        source_audio_components=len(audio), physical_audio_files=len(physical),
        catalog_items=len(album.catalog_items),
    )


def _safe_program_fallback(
    source_audio: Sequence[SourceComponent], catalog_items: Sequence[CatalogItem],
    physical_paths: set[str],
) -> bool:
    """Whether IDs churned but the complete exact-release program agrees."""
    if len(source_audio) != len(catalog_items) or len(catalog_items) != len(physical_paths):
        return False
    ordered = sorted(catalog_items, key=lambda item: item.track if item.track is not None else -1)
    if any(item.track is None or not item.title or not component.title
           for item, component in zip(ordered, source_audio, strict=True)):
        return False
    corroborated = [
        item.track == index and _titles_corrobate(item.title, component.title)
        for index, (item, component) in enumerate(zip(ordered, source_audio, strict=True), 1)
    ]
    if all(corroborated):
        return True
    # The live 59-track Moana Deluxe release has one current-source display
    # title variant while every global coordinate and other title agrees. A
    # single mismatch in a long program is corroboration, not fuzzy matching:
    # short programs and two mismatches remain unknown.
    return len(corroborated) >= 50 and sum(corroborated) >= len(corroborated) - 1


def _titles_corrobate(left: str, right: str) -> bool:
    """Conservative title corroboration tolerant of source suffix churn."""
    def words(value: str) -> tuple[str, ...]:
        return tuple(
            token for part in value.casefold().split()
            if (token := "".join(ch for ch in part if ch.isalnum()))
        )
    a, b = words(left), words(right)
    shortest = min(len(a), len(b))
    return shortest >= 1 and a[:shortest] == b[:shortest]


def _unknown(album: LibraryAlbum, manifest: SourceManifest, detail: str) -> CompletenessAlbum:
    return CompletenessAlbum(album.album_id, album.artist, album.title, manifest.release_id,
                             (CompletenessFinding("unknown", detail),), 0, 0,
                             len(album.catalog_items))


def scan_library_completeness(
    beets: CompletenessBeets,
    *, fetch_musicbrainz_raw: Callable[[str], dict[str, object]],
    fetch_discogs_raw: Callable[[str], dict[str, object]],
    enumerate_files: Callable[[str], tuple[str, ...]] = enumerate_audio_files,
    tag_reader: Callable[[str], tuple[str, str]] = read_audio_tag_identities,
    detect_composite_gap: Callable[[str], bool] = detect_composite_silence_gap,
    resolve_musicbrainz_redirect: TaggedCanonicalReleaseFn | None = None,
    max_workers: int = 4,
) -> CompletenessReport:
    """Run the full, read-only census. Per-album uncertainty is published."""
    try:
        albums = beets.list_library_completeness_albums()
    except Exception as exc:
        category = beets_authority_availability_category(exc)
        if category is None:
            raise
        return CompletenessReport("beets_unavailable", CompletenessCounts(0, 0, 0, 0, 0), (), category)
    if max_workers < 1:
        raise ValueError("max_workers must be positive")

    def classify_one(album: LibraryAlbum) -> CompletenessAlbum:
        if album.identity is None:
            return _unknown(album, SourceManifest("musicbrainz", "", ()), "unclassifiable Beets release identity")
        try:
            raw = (fetch_musicbrainz_raw(album.identity.release_id)
                   if album.identity.source == "musicbrainz"
                   else fetch_discogs_raw(album.identity.release_id))
            manifest = (musicbrainz_manifest(
                            album.identity.release_id, raw,
                            resolve_redirect=resolve_musicbrainz_redirect,
                        )
                        if album.identity.source == "musicbrainz"
                        else discogs_manifest(album.identity.release_id, raw))
            return classify_album(
                album, manifest, enumerate_files=enumerate_files, tag_reader=tag_reader,
                detect_composite_gap=detect_composite_gap,
            )
        except (urllib.error.HTTPError, urllib.error.URLError, OSError,
                json.JSONDecodeError, UnicodeDecodeError, SourceManifestError,
                msgspec.ValidationError) as exc:
            return _unknown(album, SourceManifest(album.identity.source, album.identity.release_id, ()), f"source unreadable: {exc}")

    # ``map`` yields input order despite concurrent fetching. Web clients
    # enforce their mirror-specific semaphores; this outer bound prevents the
    # census itself from making unbounded filesystem/network work.
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        results = list(executor.map(classify_one, albums))
    listed: list[CompletenessAlbum] = []
    complete = missing = drift = unknown = 0
    for result in results:
        kinds = {finding.kind for finding in result.findings}
        missing += "missing_source_audio" in kinds
        drift += "catalog_drift" in kinds
        unknown += "unknown" in kinds
        if not {"missing_source_audio", "unknown"} & kinds:
            complete += 1
        if result.findings:
            listed.append(result)
    status: CompletenessStatus = "unknown" if unknown else "incomplete" if missing else "complete"
    return CompletenessReport(
        status,
        CompletenessCounts(len(albums), complete, missing, drift, unknown),
        tuple(listed),
    )
