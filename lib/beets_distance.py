"""Standalone beets-distance computation for the Replace picker.

Given a ``download_log_id`` and a candidate MBID, return the beets match
distance between the audio files at the download_log's failed_path and
the MBID's MusicBrainz release. This is the same distance metric the
beets importer uses, computed without booting the harness, the importer
plugin chain, or any MB roundtrip beyond the one cached fetch.

Guardrail (R1): the candidate MBID **must** belong to the same release
group as the download_log's request. We refuse otherwise. The replace
picker only ever needs to compare alternative pressings of the same
album, and an MBID from an unrelated release group is almost certainly
an operator slip — failing fast keeps the API hard to misuse.

Speed: the dominant cost is reading tags off N audio files via beets'
``Item.from_path``. We cache a JSON-serialisable fingerprint of each
file's tag fields keyed by ``(absolute_path, mtime, size)``; subsequent
calls reconstruct lightweight ``Item`` instances from the cache and
never touch the filesystem again. MB releases are fetched through the
existing web/mb.py memoiser (24h TTL). The distance compute itself is
microseconds once both sides are in memory.

The module surface is a pure service function plus its typed result:

    compute_beets_distance(
        download_log_id: int,
        mbid: str,
        *,
        pdb: PipelineDB,
        mb_get_release: Callable[[str], dict],
        cache: BeetsDistanceCache | None = None,
    ) -> BeetsDistanceResult

Callers (web route + pipeline-cli) wrap the result into HTTP status
codes / shell exit codes the same way ``mbid_replace_service`` and
``search_plan_service`` do — see the CLI ⇄ API symmetry rule in
``.claude/rules/code-quality.md``.
"""

from __future__ import annotations

import logging
import os
import time
from collections.abc import Callable, Mapping, Sequence
from typing import Protocol

import msgspec

# Eager beets imports — deliberate. The module is heavy, but importing
# at load time pins the upstream ``beets`` package before anything can
# pollute sys.path (the first import wins; later sys.path mutations
# can't rebind a name already in sys.modules). Historically this
# guarded against tests/web/_harness.py putting ``lib/`` on sys.path so
# ``lib/beets.py`` shadowed the real package — that insert is gone, and
# tests/test_no_dual_load.py + TestSysPathAudit ban the ambiguity
# outright.
from beets import library as _beets_library

# beets 2.x's autotag/__init__ re-exports the `distance` *function* (from
# .distance import distance), shadowing the submodule of the same name — so
# `from beets.autotag import distance` binds the function, not the module.
# Call it directly; `_beets_distance_mod.distance(...)` would AttributeError.
from beets.autotag import distance as _beets_distance_fn
from beets.autotag import hooks as _beets_hooks
from beets.autotag import match as _beets_match_mod

from lib.validation_envelope import decode_validation_envelope

log = logging.getLogger(__name__)

# Keep the upstream constructor behind one local seam so tests can replace the
# tag reader without loading real media files.
_item_from_path_fn = _beets_library.Item.from_path


def _item_from_path(path: str) -> _beets_library.Item:
    return _item_from_path_fn(path)


def _item_field(
    item: _beets_library.Item, key: str, default: object = None,
) -> int | float | str | None:
    return item.get(key, default)


class BeetsDistanceResult(msgspec.Struct, kw_only=True):
    """Typed result of a single distance computation.

    Crosses the wire (HTTP response + CLI JSON output) so this is a
    ``msgspec.Struct`` per the wire-boundary rule. Optional fields stay
    ``None`` when the outcome is not ``ok``; ``error_message`` carries
    the human-readable reason for any non-``ok`` outcome.
    """

    outcome: str
    distance: float | None = None
    matched_tracks: int | None = None
    total_local_tracks: int | None = None
    total_mb_tracks: int | None = None
    extra_local_tracks: int | None = None
    extra_mb_tracks: int | None = None
    # Per-component distance breakdown — same names beets emits, useful
    # when an operator wants to know "why is this 0.42?".
    components: dict[str, float] | None = None
    # RG IDs for guardrail traceability. Always populated when we got
    # far enough to look them up; ``None`` otherwise.
    request_release_group_id: str | None = None
    candidate_release_group_id: str | None = None
    candidate_mbid: str | None = None
    download_log_id: int | None = None
    request_id: int | None = None
    folder_path: str | None = None
    error_message: str | None = None
    # Latency observability — useful in tests + the eventual UI.
    duration_ms: int | None = None


# === Cache protocol =====================================================


class BeetsDistanceCache(Protocol):
    """Minimal key-value cache protocol the service depends on.

    Implementations adapt Redis or any in-process dict. Production wires
    this to the Redis client adapter in ``web/routes/pipeline.py``; tests
    pass an in-process dict-backed implementation or ``None`` for "no
    caching".
    """

    def get(self, key: str) -> bytes | None: ...
    def set(self, key: str, value: bytes, ttl_seconds: int) -> None: ...


# === Pipeline-DB protocol (#409 per-consumer pattern) ===================


class BeetsDistancePipelineDB(Protocol):
    """The slice of ``PipelineDB`` ``compute_beets_distance`` consumes.

    Only the two reads the Replace-picker path touches — narrower than
    the full ``PipelineDB`` so test stand-ins (``tests/test_beets_distance.py``
    ``_StubPDB``/``_PDBExploder``) satisfy it structurally without
    inheriting from or duck-typing the whole class. Return types are
    ``Mapping[str, object]`` (not the full ``DownloadLogWithEvidenceRow``/
    ``AlbumRequestRow`` TypedDicts) because this function only ever reads
    a handful of keys via ``.get(...)`` — the real ``PipelineDB`` methods'
    richer TypedDict returns are structurally compatible mappings.
    """

    def get_download_log_entry(
        self, log_id: int,
    ) -> Mapping[str, object] | None: ...

    def get_request(self, request_id: int) -> Mapping[str, object] | None: ...


# Reasonable defaults — folder reads are dominated by tag IO so a long
# TTL is fine; files are keyed by ``(path, mtime, size)`` so any real
# change invalidates the cache automatically.
_FOLDER_CACHE_TTL_S = 7 * 24 * 60 * 60


# === Audio-file fingerprint (JSON-safe) =================================


class _AudioFileFingerprint(msgspec.Struct):
    """Subset of beets ``Item`` fields needed to compute distance.

    Kept narrow on purpose — broadening this without intent leads to
    cache size creep and slow first-reads. Add fields only when distance
    actually reads them.
    """

    path: str
    mtime: float
    size: int
    title: str
    artist: str
    album: str
    albumartist: str
    track: int
    tracktotal: int
    disc: int
    disctotal: int
    length: float
    format: str
    media: str


class SyntheticItem(msgspec.Struct, kw_only=True):
    """Caller-provided item shape for the ``items_override`` path.

    Mirrors the subset of ``_AudioFileFingerprint`` that beets'
    ``distance()`` actually reads, minus the on-disk fields
    (``path``, ``mtime``, ``size``, ``format``, ``media``). This is the
    wire-boundary type the YT Music album resolver constructs from
    upstream JSON before handing the matrix to ``compute_beets_distance``.

    ``length`` is in seconds (float) to match beets' internal
    representation and ``_AudioFileFingerprint.length``.
    """

    title: str
    artist: str
    album: str
    albumartist: str
    track: int
    tracktotal: int
    disc: int
    disctotal: int
    length: float


def _audio_files_under(folder: str) -> list[str]:
    """Return absolute paths to audio files under ``folder``.

    Sorts for deterministic ordering — beets distance is order-
    insensitive but stable ordering makes the cache key stable, the
    test assertions stable, and the per-track distance breakdown
    consistent across runs.
    """
    from lib.measurement import AUDIO_EXTS

    out: list[str] = []
    for root, _dirs, files in os.walk(folder):
        for f in files:
            ext = f.rsplit(".", 1)[-1].lower() if "." in f else ""
            if ext in AUDIO_EXTS:
                out.append(os.path.join(root, f))
    out.sort()
    return out


def _fingerprint_file(path: str) -> _AudioFileFingerprint | None:
    """Read tags via beets ``Item.from_path`` and project to fingerprint.

    Returns ``None`` if the file can't be read — caller skips it. We
    deliberately don't raise: a single corrupt sidecar file shouldn't
    fail the whole picker query.
    """
    try:
        item = _item_from_path(path)
    except Exception as exc:  # noqa: BLE001 — beets raises a mediafile mess
        log.warning("beets_distance: tag read failed for %s: %s", path, exc)
        return None

    try:
        st = os.stat(path)
    except OSError:
        return None

    # Beets Item exposes tag fields as attributes (LightFlavoredDict).
    return _AudioFileFingerprint(
        path=path,
        mtime=st.st_mtime,
        size=st.st_size,
        title=str(_item_field(item, "title", "") or ""),
        artist=str(_item_field(item, "artist", "") or ""),
        album=str(_item_field(item, "album", "") or ""),
        albumartist=str(_item_field(item, "albumartist", "") or ""),
        track=int(_item_field(item, "track", 0) or 0),
        tracktotal=int(_item_field(item, "tracktotal", 0) or 0),
        disc=int(_item_field(item, "disc", 0) or 0),
        disctotal=int(_item_field(item, "disctotal", 0) or 0),
        length=float(_item_field(item, "length", 0.0) or 0.0),
        format=str(_item_field(item, "format", "") or ""),
        media=str(_item_field(item, "media", "") or ""),
    )


def _read_folder_fingerprints(
    folder: str,
    cache: BeetsDistanceCache | None,
) -> list[_AudioFileFingerprint]:
    """Read fingerprints for every audio file under ``folder``.

    Per-file cache keyed by ``(path, mtime, size)``: if any of those
    change the cache misses and the file is re-read. We don't cache the
    folder-level list because the same folder can gain or lose files
    without any individual file's stats changing — the os.walk is cheap
    relative to tag reads.
    """
    fps: list[_AudioFileFingerprint] = []
    for path in _audio_files_under(folder):
        try:
            st = os.stat(path)
        except OSError:
            continue
        cached: _AudioFileFingerprint | None = None
        if cache is not None:
            blob = cache.get(_file_cache_key(path, st.st_mtime, st.st_size))
            if blob:
                try:
                    cached = msgspec.json.decode(blob, type=_AudioFileFingerprint)
                except msgspec.ValidationError:
                    cached = None
        if cached is not None and cached.mtime == st.st_mtime and cached.size == st.st_size:
            fps.append(cached)
            continue
        fp = _fingerprint_file(path)
        if fp is None:
            continue
        fps.append(fp)
        if cache is not None:
            cache.set(
                _file_cache_key(path, st.st_mtime, st.st_size),
                msgspec.json.encode(fp),
                _FOLDER_CACHE_TTL_S,
            )
    return fps


def _file_cache_key(path: str, mtime: float, size: int) -> str:
    # mtime as int-seconds is enough resolution and keeps the key short.
    return f"beets-distance:fp:{path}:{int(mtime)}:{size}"


def _mb_release_track_dicts(mb_release: dict[str, object]) -> list[dict[str, object]]:
    """Narrow ``mb_get_release()``'s ``tracks`` field to a typed list.

    Graceful narrowing over the external MB/Discogs JSON boundary (both
    sources feed this same slim shape — CLAUDE.md's "no adapter code
    between MB and Discogs" invariant): ``msgspec.convert`` validates the
    list-of-objects SHAPE only (the target ``dict[str, object]`` accepts
    any per-track field values), so this behaves the same as the untyped
    ``.get(..., [])`` it replaces for any real response — it only raises
    if ``tracks`` is present but isn't structurally a list of
    string-keyed objects, a failure ``compute_beets_distance``'s step-8
    ``try/except`` already turns into a ``distance_failed`` outcome.
    """
    tracks = mb_release.get("tracks")
    if not isinstance(tracks, list):
        return []
    return msgspec.convert(tracks, type=list[dict[str, object]])


def _mb_release_tracks(mb_release: dict[str, object]) -> list[object]:
    """Narrow ``mb_get_release()``'s ``tracks`` field for a bare length check."""
    return list(_mb_release_track_dicts(mb_release))


def _mb_str_field(mb_release: dict[str, object], key: str) -> str | None:
    value = mb_release.get(key)
    return value if isinstance(value, str) else None


def _mb_float_field(mb_release: dict[str, object], key: str) -> float | None:
    value = mb_release.get(key)
    return float(value) if isinstance(value, (int, float)) else None


def _mb_int_field(mb_release: dict[str, object], key: str) -> int | None:
    value = mb_release.get(key)
    return value if isinstance(value, int) else None


# === MB → AlbumInfo / TrackInfo conversion ==============================


def _build_album_info(
    mb_release: dict[str, object], mbid: str,
) -> _beets_hooks.AlbumInfo:
    """Construct a beets ``AlbumInfo`` from the local MB mirror's JSON.

    Only fields beets ``distance()`` actually reads are populated.
    Anything not in the MB payload stays ``None`` and beets' distance
    helpers skip the missing comparison (no penalty for unknowable
    fields — appropriate for picker-time triage).
    """
    tracks: list[_beets_hooks.TrackInfo] = []
    artist_name = _mb_str_field(mb_release, "artist_name") or ""
    artist_id = _mb_str_field(mb_release, "artist_id")
    for t in _mb_release_track_dicts(mb_release):
        # MB pre-gap tracks ride as track_number=0; beets'
        # ``assign_items`` is fine with that.
        track_number = _mb_int_field(t, "track_number")
        tracks.append(_beets_hooks.TrackInfo(
            title=_mb_str_field(t, "title") or "",
            track_id=None,
            artist=artist_name,
            artist_id=artist_id,
            length=_mb_float_field(t, "length_seconds"),
            index=track_number or None,
            medium=_mb_int_field(t, "disc_number") or 1,
            medium_index=track_number or None,
        ))

    year = _mb_int_field(mb_release, "year")
    rg_id = _mb_str_field(mb_release, "release_group_id")
    return _beets_hooks.AlbumInfo(
        tracks=tracks,
        album=_mb_str_field(mb_release, "title") or "",
        album_id=mbid,
        artist=artist_name,
        artist_id=artist_id,
        releasegroup_id=rg_id,
        year=year,
        country=_mb_str_field(mb_release, "country"),
        albumstatus=_mb_str_field(mb_release, "status"),
        va=False,
    )


def _build_items(
    fingerprints: Sequence[_AudioFileFingerprint],
) -> list[_beets_library.Item]:
    """Construct in-memory beets ``Item`` instances from fingerprints.

    Reconstructing rather than re-reading is the whole point of the
    fingerprint cache — beets ``library.Item`` is a dict-like, so we
    just splat the fields in and skip the MediaFile roundtrip.
    """
    items: list[_beets_library.Item] = []
    for fp in fingerprints:
        item = _beets_library.Item(
            path=fp.path.encode("utf-8"),
            title=fp.title,
            artist=fp.artist,
            album=fp.album,
            albumartist=fp.albumartist,
            track=fp.track,
            tracktotal=fp.tracktotal,
            disc=fp.disc,
            disctotal=fp.disctotal,
            length=fp.length,
            format=fp.format,
            media=fp.media,
        )
        items.append(item)
    return items


def _build_items_from_synthetic(
    items: Sequence[SyntheticItem],
) -> list[_beets_library.Item]:
    """Construct in-memory beets ``Item`` instances from synthetic items.

    Mirrors ``_build_items`` for the ``items_override`` path. ``path`` is
    a synthetic placeholder per-index (beets ``Item.path`` must be bytes
    but the distance helpers never read it back) and ``format`` / ``media``
    are empty strings — beets' distance helpers skip missing-field
    comparisons rather than penalising them.
    """
    out: list[_beets_library.Item] = []
    for i, si in enumerate(items):
        item = _beets_library.Item(
            path=f"synthetic://{i}".encode(),
            title=si.title,
            artist=si.artist,
            album=si.album,
            albumartist=si.albumartist,
            track=si.track,
            tracktotal=si.tracktotal,
            disc=si.disc,
            disctotal=si.disctotal,
            length=si.length,
            format="",
            media="",
        )
        out.append(item)
    return out


# === Service entrypoint =================================================


def compute_beets_distance(
    download_log_id: int | None = None,
    mbid: str | None = None,
    *,
    items_override: list[SyntheticItem] | None = None,
    mb_release_group_id: str | None = None,
    pdb: BeetsDistancePipelineDB,
    mb_get_release: Callable[[str], dict[str, object] | None],
    cache: BeetsDistanceCache | None = None,
    resolve_failed_path: Callable[[str], str | None] | None = None,
) -> BeetsDistanceResult:
    """Compute beets match distance for one MBID.

    Service-layer entrypoint. Pure of HTTP/CLI concerns; callers map the
    typed result onto status codes / exit codes (CLI ⇄ API symmetry).

    Two input modes (exactly one of them must be supplied):

    1. **Replace-picker mode** — pass ``download_log_id``. The function
       loads the download_log + request rows, resolves the on-disk folder,
       reads tag fingerprints, and scores those against the candidate
       MBID's MB release. The cross-RG guardrail compares the candidate's
       RG to the request's RG.

    2. **Override mode** — pass ``items_override`` (and optionally
       ``mb_release_group_id``). Caller provides ``SyntheticItem``s
       directly; no DB or filesystem IO occurs. When
       ``mb_release_group_id`` is provided, the cross-RG guardrail
       compares the candidate's RG to that caller-supplied RG; when it's
       ``None`` the guardrail is skipped (standalone scoring contract).

    Guardrails fire in this order, before any heavy work:
      0. Exactly one of (download_log_id, items_override) must be set,
         and items_override (when set) must be non-empty;
      1. (Replace mode) download_log row must exist;
      2. (Replace mode) request row for that log must exist;
      3. MB release for ``mbid`` must be fetchable;
      4. (only when the caller's RG is set) MB release must belong to a
         release group — a no-RG candidate can't satisfy step 5 anyway,
         so step 4 is the early-exit. When the caller has no RG to
         compare against (Replace-mode legacy row, or Override-mode
         orphan scoring), step 4 is skipped and the no-RG candidate
         flows through;
      5. that release group MUST equal the caller-known RG (request's
         in Replace mode, ``mb_release_group_id`` in Override mode).
         Skipped when the caller's RG is None.

    Only after all five does the function touch the filesystem (and only
    in Replace mode).
    """
    started = time.monotonic()

    # 0. Exactly-one-of guardrail. Both supplied or neither → caller bug.
    if (download_log_id is not None) == (items_override is not None):
        return _result(
            "invalid_input",
            error=(
                "exactly one of download_log_id or items_override must be "
                "supplied (got both)" if download_log_id is not None
                else "exactly one of download_log_id or items_override must be "
                "supplied (got neither)"
            ),
            candidate_mbid=mbid,
            started=started,
        )

    # 0a. mbid is structurally required for any downstream MB lookup.
    if mbid is None:
        return _result(
            "invalid_input",
            error="mbid is required",
            started=started,
        )

    # 0b. Empty items_override is a distinct caller error from on-disk no_audio.
    if items_override is not None and len(items_override) == 0:
        return _result(
            "empty_items_override",
            error="items_override is empty (caller provided no items to score)",
            candidate_mbid=mbid,
            started=started,
        )

    # Branch: Replace-picker path loads DB rows; Override path skips them.
    log_row: Mapping[str, object] | None = None
    request_id: int | None = None
    request_rg: str | None = None

    if download_log_id is not None:
        # 1. Load the download_log entry.
        log_row = pdb.get_download_log_entry(download_log_id)
        if not log_row:
            return _result(
                "download_log_not_found",
                error=f"download_log #{download_log_id} not found",
                download_log_id=download_log_id,
                candidate_mbid=mbid,
                started=started,
            )

        # 2. Load the request for its release group.
        request_id_raw = log_row.get("request_id")
        if not isinstance(request_id_raw, int):
            return _result(
                "request_not_found",
                error="download_log row has no request_id",
                download_log_id=download_log_id,
                candidate_mbid=mbid,
                started=started,
            )
        request_id = request_id_raw
        req = pdb.get_request(request_id)
        if not req:
            return _result(
                "request_not_found",
                error=f"request #{request_id} not found",
                download_log_id=download_log_id,
                request_id=request_id,
                candidate_mbid=mbid,
                started=started,
            )
        request_rg_raw = req.get("mb_release_group_id")
        request_rg = request_rg_raw if isinstance(request_rg_raw, str) else None
    else:
        # Override mode: caller supplies the RG directly (or None to opt
        # out of the cross-RG guardrail). No DB consult.
        request_rg = mb_release_group_id

    # 3. Fetch MB release for the candidate mbid.
    try:
        mb_release = mb_get_release(mbid)
    except Exception as exc:  # noqa: BLE001 — upstream errors vary
        return _result(
            "mb_lookup_failed",
            error=f"MB lookup for {mbid} failed: {exc}",
            download_log_id=download_log_id,
            request_id=request_id,
            request_release_group_id=request_rg,
            candidate_mbid=mbid,
            started=started,
        )
    if not mb_release:
        return _result(
            "mb_lookup_failed",
            error=f"MB lookup for {mbid} returned empty",
            download_log_id=download_log_id,
            request_id=request_id,
            request_release_group_id=request_rg,
            candidate_mbid=mbid,
            started=started,
        )
    candidate_rg_raw = mb_release.get("release_group_id")
    candidate_rg = candidate_rg_raw if isinstance(candidate_rg_raw, str) else None

    # 4. MB release must have a release group — but only when the caller
    # has an RG to compare against. With no caller RG, step 5 is skipped
    # too, so the no-RG candidate is the caller's responsibility (used
    # by the YouTube resolver to score orphan releases — Discogs releases
    # with no master, or legacy MB releases without a release-group).
    if request_rg and not candidate_rg:
        return _result(
            "mb_no_release_group",
            error=f"MB release {mbid} has no release_group_id",
            download_log_id=download_log_id,
            request_id=request_id,
            request_release_group_id=request_rg,
            candidate_mbid=mbid,
            started=started,
        )

    # 5. Guardrail — refuse cross-RG distance queries.
    # Replace mode: ``request_rg`` is the request row's RG.
    # Override mode: ``request_rg`` is the caller-supplied
    # ``mb_release_group_id`` (None opts out).
    if request_rg and request_rg != candidate_rg:
        diff_desc = (
            f"request #{request_id}'s release group {request_rg}"
            if request_id is not None
            else f"caller-supplied release group {request_rg}"
        )
        return _result(
            "wrong_release_group",
            error=(
                f"MBID {mbid} is in release group {candidate_rg}, "
                f"which differs from {diff_desc}"
            ),
            download_log_id=download_log_id,
            request_id=request_id,
            request_release_group_id=request_rg,
            candidate_release_group_id=candidate_rg,
            candidate_mbid=mbid,
            started=started,
        )

    # 6. Build items — either from disk (Replace mode) or from synthetic.
    resolved: str | None = None
    fingerprint_count: int | None = None
    if items_override is not None:
        items = _build_items_from_synthetic(items_override)
    else:
        # Resolve the on-disk path for the rejected download.
        assert log_row is not None  # narrowed above; Replace-mode invariant
        failed_path = decode_validation_envelope(
            log_row.get("validation_result")).failed_path or ""
        resolver = resolve_failed_path
        if resolver is None:
            from lib.util import resolve_failed_path as default_resolver
            resolver = default_resolver
        resolved = resolver(str(failed_path)) if failed_path else None
        if not resolved:
            return _result(
                "folder_missing",
                error=(
                    f"download_log #{download_log_id} failed_path "
                    f"{failed_path!r} does not exist on disk"
                ),
                download_log_id=download_log_id,
                request_id=request_id,
                request_release_group_id=request_rg,
                candidate_release_group_id=candidate_rg,
                candidate_mbid=mbid,
                started=started,
            )

        # 7. Read (or cache-hit) audio fingerprints.
        fingerprints = _read_folder_fingerprints(resolved, cache)
        if not fingerprints:
            return _result(
                "no_audio",
                error=f"no readable audio files under {resolved}",
                download_log_id=download_log_id,
                request_id=request_id,
                request_release_group_id=request_rg,
                candidate_release_group_id=candidate_rg,
                candidate_mbid=mbid,
                folder_path=resolved,
                started=started,
            )
        fingerprint_count = len(fingerprints)
        items = _build_items(fingerprints)

    # 8. Hand to beets for the actual distance compute.
    try:
        album_info = _build_album_info(mb_release, mbid)
        mapping, extra_items, extra_tracks = _beets_match_mod.assign_items(
            items, album_info.tracks)
        dist = _beets_distance_fn(items, album_info, mapping)
    except Exception as exc:  # noqa: BLE001 — beets bugs shouldn't 500 us
        return _result(
            "distance_failed",
            error=f"beets distance failed: {exc}",
            download_log_id=download_log_id,
            request_id=request_id,
            request_release_group_id=request_rg,
            candidate_release_group_id=candidate_rg,
            candidate_mbid=mbid,
            folder_path=resolved,
            total_local_tracks=(
                fingerprint_count if fingerprint_count is not None
                else len(items)
            ),
            total_mb_tracks=len(_mb_release_tracks(mb_release)),
            started=started,
        )

    # 9. Extract per-component scores so the caller can show "why 0.42?"
    components: dict[str, float] = {}
    try:
        for k, v in dist.items():
            try:
                components[str(k)] = float(v)
            except (TypeError, ValueError):
                continue
    except Exception:  # noqa: BLE001 — Distance API varies across beets versions
        components = {}

    return _result(
        "ok",
        distance=float(dist.distance),
        matched_tracks=len(mapping),
        total_local_tracks=len(items),
        total_mb_tracks=len(album_info.tracks),
        extra_local_tracks=len(extra_items),
        extra_mb_tracks=len(extra_tracks),
        components=components,
        download_log_id=download_log_id,
        request_id=request_id,
        request_release_group_id=request_rg,
        candidate_release_group_id=candidate_rg,
        candidate_mbid=mbid,
        folder_path=resolved,
        started=started,
    )


def _result(
    outcome: str,
    *,
    distance: float | None = None,
    matched_tracks: int | None = None,
    total_local_tracks: int | None = None,
    total_mb_tracks: int | None = None,
    extra_local_tracks: int | None = None,
    extra_mb_tracks: int | None = None,
    components: dict[str, float] | None = None,
    request_release_group_id: str | None = None,
    candidate_release_group_id: str | None = None,
    candidate_mbid: str | None = None,
    download_log_id: int | None = None,
    request_id: int | None = None,
    folder_path: str | None = None,
    error: str | None = None,
    started: float,
) -> BeetsDistanceResult:
    return BeetsDistanceResult(
        outcome=outcome,
        distance=distance,
        matched_tracks=matched_tracks,
        total_local_tracks=total_local_tracks,
        total_mb_tracks=total_mb_tracks,
        extra_local_tracks=extra_local_tracks,
        extra_mb_tracks=extra_mb_tracks,
        components=components,
        request_release_group_id=request_release_group_id,
        candidate_release_group_id=candidate_release_group_id,
        candidate_mbid=candidate_mbid,
        download_log_id=download_log_id,
        request_id=request_id,
        folder_path=folder_path,
        error_message=error,
        duration_ms=int((time.monotonic() - started) * 1000),
    )
