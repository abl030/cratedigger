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
from typing import Callable, Optional, Protocol, Sequence

import msgspec


log = logging.getLogger(__name__)


# === Outcomes ===========================================================

# Public outcome string set. Tests pin against these — adding a new
# outcome is a contract change and must be reflected at the route /
# CLI / frontend boundaries.
OUTCOMES = (
    "ok",
    "download_log_not_found",
    "request_not_found",
    "folder_missing",
    "no_audio",
    "mb_lookup_failed",
    "mb_no_release_group",
    "wrong_release_group",
    "distance_failed",
)


class BeetsDistanceResult(msgspec.Struct, kw_only=True):
    """Typed result of a single distance computation.

    Crosses the wire (HTTP response + CLI JSON output) so this is a
    ``msgspec.Struct`` per the wire-boundary rule. Optional fields stay
    ``None`` when the outcome is not ``ok``; ``error_message`` carries
    the human-readable reason for any non-``ok`` outcome.
    """

    outcome: str
    distance: Optional[float] = None
    matched_tracks: Optional[int] = None
    total_local_tracks: Optional[int] = None
    total_mb_tracks: Optional[int] = None
    extra_local_tracks: Optional[int] = None
    extra_mb_tracks: Optional[int] = None
    # Per-component distance breakdown — same names beets emits, useful
    # when an operator wants to know "why is this 0.42?".
    components: Optional[dict[str, float]] = None
    # RG IDs for guardrail traceability. Always populated when we got
    # far enough to look them up; ``None`` otherwise.
    request_release_group_id: Optional[str] = None
    candidate_release_group_id: Optional[str] = None
    candidate_mbid: Optional[str] = None
    download_log_id: Optional[int] = None
    request_id: Optional[int] = None
    folder_path: Optional[str] = None
    error_message: Optional[str] = None
    # Latency observability — useful in tests + the eventual UI.
    duration_ms: Optional[int] = None


# === Cache protocol =====================================================


class BeetsDistanceCache(Protocol):
    """Minimal key-value cache protocol the service depends on.

    Implementations adapt Redis or any in-process dict. Production
    wires this to the existing Redis client in ``web/_cache.py``; tests
    pass a ``DictCache`` (see below) or ``None`` for "no caching".
    """

    def get(self, key: str) -> Optional[bytes]: ...
    def set(self, key: str, value: bytes, ttl_seconds: int) -> None: ...


class DictCache:
    """In-memory ``BeetsDistanceCache`` for tests + single-process runs."""

    def __init__(self) -> None:
        self._store: dict[str, bytes] = {}

    def get(self, key: str) -> Optional[bytes]:
        return self._store.get(key)

    def set(self, key: str, value: bytes, ttl_seconds: int) -> None:
        self._store[key] = value


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


def _fingerprint_file(path: str) -> Optional[_AudioFileFingerprint]:
    """Read tags via beets ``Item.from_path`` and project to fingerprint.

    Returns ``None`` if the file can't be read — caller skips it. We
    deliberately don't raise: a single corrupt sidecar file shouldn't
    fail the whole picker query.
    """
    from beets import library

    try:
        item = library.Item.from_path(path)
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
        title=str(item.get("title", "") or ""),
        artist=str(item.get("artist", "") or ""),
        album=str(item.get("album", "") or ""),
        albumartist=str(item.get("albumartist", "") or ""),
        track=int(item.get("track", 0) or 0),
        tracktotal=int(item.get("tracktotal", 0) or 0),
        disc=int(item.get("disc", 0) or 0),
        disctotal=int(item.get("disctotal", 0) or 0),
        length=float(item.get("length", 0.0) or 0.0),
        format=str(item.get("format", "") or ""),
        media=str(item.get("media", "") or ""),
    )


def _read_folder_fingerprints(
    folder: str,
    cache: Optional[BeetsDistanceCache],
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
        cached: Optional[_AudioFileFingerprint] = None
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


# === MB → AlbumInfo / TrackInfo conversion ==============================


def _build_album_info(mb_release: dict, mbid: str):
    """Construct a beets ``AlbumInfo`` from the local MB mirror's JSON.

    Only fields beets ``distance()`` actually reads are populated.
    Anything not in the MB payload stays ``None`` and beets' distance
    helpers skip the missing comparison (no penalty for unknowable
    fields — appropriate for picker-time triage).
    """
    from beets.autotag import hooks

    tracks = []
    artist_name = mb_release.get("artist_name") or ""
    artist_id = mb_release.get("artist_id")
    for t in mb_release.get("tracks") or []:
        # MB pre-gap tracks ride as track_number=0; beets'
        # ``assign_items`` is fine with that.
        tracks.append(hooks.TrackInfo(
            title=t.get("title") or "",
            track_id=None,
            artist=artist_name,
            artist_id=artist_id,
            length=t.get("length_seconds"),
            index=t.get("track_number") or None,
            medium=t.get("disc_number") or 1,
            medium_index=t.get("track_number") or None,
        ))

    year = mb_release.get("year")
    rg_id = mb_release.get("release_group_id")
    return hooks.AlbumInfo(
        tracks=tracks,
        album=mb_release.get("title") or "",
        album_id=mbid,
        artist=artist_name,
        artist_id=artist_id,
        releasegroup_id=rg_id,
        year=year if isinstance(year, int) else None,
        country=mb_release.get("country") or None,
        albumstatus=mb_release.get("status") or None,
        va=False,
    )


def _build_items(fingerprints: Sequence[_AudioFileFingerprint]):
    """Construct in-memory beets ``Item`` instances from fingerprints.

    Reconstructing rather than re-reading is the whole point of the
    fingerprint cache — beets ``library.Item`` is a dict-like, so we
    just splat the fields in and skip the MediaFile roundtrip.
    """
    from beets import library

    items = []
    for fp in fingerprints:
        item = library.Item(
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


# === Service entrypoint =================================================


def compute_beets_distance(
    download_log_id: int,
    mbid: str,
    *,
    pdb,  # lib.pipeline_db.PipelineDB — duck-typed for test fakes
    mb_get_release: Callable[[str], Optional[dict]],
    cache: Optional[BeetsDistanceCache] = None,
    resolve_failed_path: Optional[Callable[[str], Optional[str]]] = None,
) -> BeetsDistanceResult:
    """Compute beets match distance for one ``(download_log_id, mbid)``.

    Service-layer entrypoint. Pure of HTTP/CLI concerns; callers map the
    typed result onto status codes / exit codes (CLI ⇄ API symmetry).

    Guardrails before any heavy work:
      1. download_log row must exist;
      2. request row for that log must exist;
      3. MB release for ``mbid`` must be fetchable;
      4. MB release must belong to a release group;
      5. that release group MUST equal the request's release group.

    Only after all five does the function touch the filesystem.
    """
    started = time.monotonic()

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
    request_id = log_row.get("request_id")
    if not isinstance(request_id, int):
        return _result(
            "request_not_found",
            error="download_log row has no request_id",
            download_log_id=download_log_id,
            candidate_mbid=mbid,
            started=started,
        )
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
    request_rg = req.get("mb_release_group_id")

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
    candidate_rg = mb_release.get("release_group_id")

    # 4. MB release must have a release group (legacy / non-MB rows fail here).
    if not candidate_rg:
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
    if request_rg and request_rg != candidate_rg:
        return _result(
            "wrong_release_group",
            error=(
                f"MBID {mbid} is in release group {candidate_rg}, "
                f"which differs from request #{request_id}'s release "
                f"group {request_rg}"
            ),
            download_log_id=download_log_id,
            request_id=request_id,
            request_release_group_id=request_rg,
            candidate_release_group_id=candidate_rg,
            candidate_mbid=mbid,
            started=started,
        )

    # 6. Resolve the on-disk path for the rejected download.
    vr = log_row.get("validation_result") or {}
    failed_path = (vr.get("failed_path") if isinstance(vr, dict) else None) or ""
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

    # 8. Hand to beets for the actual distance compute.
    try:
        from beets.autotag import distance as distance_mod
        from beets.autotag import match as match_mod

        album_info = _build_album_info(mb_release, mbid)
        items = _build_items(fingerprints)
        mapping, extra_items, extra_tracks = match_mod.assign_items(items, album_info.tracks)
        dist = distance_mod.distance(items, album_info, mapping)
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
            total_local_tracks=len(fingerprints),
            total_mb_tracks=len(mb_release.get("tracks") or []),
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
    distance: Optional[float] = None,
    matched_tracks: Optional[int] = None,
    total_local_tracks: Optional[int] = None,
    total_mb_tracks: Optional[int] = None,
    extra_local_tracks: Optional[int] = None,
    extra_mb_tracks: Optional[int] = None,
    components: Optional[dict[str, float]] = None,
    request_release_group_id: Optional[str] = None,
    candidate_release_group_id: Optional[str] = None,
    candidate_mbid: Optional[str] = None,
    download_log_id: Optional[int] = None,
    request_id: Optional[int] = None,
    folder_path: Optional[str] = None,
    error: Optional[str] = None,
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
