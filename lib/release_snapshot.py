"""Release-snapshot construction for the search-plan service.

This module is the single source of `ReleaseSnapshot` construction for both
add-time generation (CLI/web) and startup/regeneration. Keeping it pure means
the search-plan service can mock the resolver boundary in tests without
mocking what release metadata "looks like".

Two construction paths:

* `snapshot_from_request_row(row, tracks)` — build from already-persisted
  `album_requests` + `request_tracks` rows. Used by startup reconciliation
  (U4) and explicit regeneration (U8).
* `snapshot_from_add_payload(...)` — build from in-memory metadata + tracks
  resolved at add time. Used by CLI `add` and web `/api/pipeline/add`.

A small `TrackResolver` protocol is exposed so the service can lazily
populate missing tracks via MB / Discogs APIs without coupling this module
to those clients. Production wires the resolver at the service layer.

The output is `lib.search.ReleaseSnapshot`. We re-export it so callers do
not have to import from `lib.search` for a frozen value type.
"""

from __future__ import annotations

from typing import Any, Protocol

from lib.search import ReleaseSnapshot

__all__ = [
    "ReleaseSnapshot",
    "TrackResolver",
    "ResolverFailure",
    "ResolverMetadataIncomplete",
    "snapshot_from_request_row",
    "snapshot_from_add_payload",
    "year_from_value",
]


class ResolverFailure(Exception):
    """Resolver could not reach the upstream metadata source.

    Mapped by the service layer to a transient failed plan
    (`failure_class='resolver_unavailable'` or `'dependency_failure'`).
    """


class ResolverMetadataIncomplete(Exception):
    """Resolver succeeded but the release has no usable tracks/metadata.

    Mapped by the service layer to a deterministic failed plan
    (`failure_class='metadata_incomplete'`). Distinct from
    `ResolverFailure` so transient outages stay retryable.
    """


class TrackResolver(Protocol):
    """Optional adapter the service uses to fetch missing tracks.

    Implementations should:

      * return a list of `{title, track_number, disc_number, ...}` dicts
        when tracks are found upstream (matches `PipelineDB.set_tracks` and
        `PipelineDB.get_tracks` shape).
      * raise `ResolverFailure` on connection / HTTP errors so the service
        records a transient plan failure.
      * return `[]` (or raise `ResolverMetadataIncomplete`) when the
        release is unambiguously empty / not found.
    """

    def resolve_tracks(
        self,
        *,
        release_id: str,
        request_id: int,
    ) -> list[dict[str, Any]]:
        ...


def year_from_value(value: object) -> str | None:
    """Normalise a year column / payload field to the generator-input shape.

    The pure generator's `_year_is_known()` accepts a 4-digit-prefixed
    string. Production stores years as integers in `album_requests.year`
    but receives strings in some web payloads, and a None/0 sentinel from
    cratedigger's earlier code paths. Coerce all of these here so callers
    don't each rebuild this logic.
    """
    if value is None:
        return None
    if isinstance(value, int):
        if value <= 0:
            return None
        return f"{value:04d}"
    if isinstance(value, str):
        s = value.strip()
        if not s:
            return None
        # Accept "2014", "2014-05-06", "0000-..." etc. The generator
        # truncates to the first 4 chars and validates them.
        return s
    return None


def _track_titles_from_tracks(tracks: list[dict[str, Any]]) -> tuple[str, ...]:
    """Extract ordered track titles from a tracks list.

    Sort by (disc_number, track_number) so the snapshot ordering matches
    pressing order — the generator's tiebreaker depends on it.
    """
    if not tracks:
        return ()
    sortable: list[tuple[int, int, str]] = []
    for t in tracks:
        title = t.get("title") or ""
        if not isinstance(title, str):
            continue
        title = title.strip()
        if not title:
            continue
        disc = t.get("disc_number") or t.get("mediumNumber") or 1
        try:
            disc_i = int(disc)
        except (TypeError, ValueError):
            disc_i = 1
        track_no = t.get("track_number") or t.get("trackNumber") or 0
        try:
            track_i = int(track_no)
        except (TypeError, ValueError):
            track_i = 0
        sortable.append((disc_i, track_i, title))
    sortable.sort(key=lambda x: (x[0], x[1]))
    return tuple(s[2] for s in sortable)


def _release_group_year_from_value(value: object) -> int | None:
    """Normalise the ``release_group_year`` column / payload to ``int | None``.

    The DB column is INTEGER NULL; CLI / web add paths pass a Python
    int or None. Strings are accepted defensively (some test fixtures
    use strings) and parsed as a 4-digit year. Non-positive ints / bad
    strings collapse to None so the generator's ``unwild_rg_year``
    skip path fires.
    """
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value if value > 0 else None
    if isinstance(value, str):
        s = value.strip()
        if not s:
            return None
        try:
            n = int(s[:4])
        except ValueError:
            return None
        return n if n > 0 else None
    return None


def snapshot_from_request_row(
    row: dict[str, Any],
    tracks: list[dict[str, Any]],
    *,
    prepend_artist: bool = False,
) -> ReleaseSnapshot:
    """Build a `ReleaseSnapshot` from a persisted `album_requests` row.

    Used by startup reconciliation and explicit regeneration. Both
    operate against persisted state — the service layer is responsible
    for calling a `TrackResolver` first if `tracks` is empty and the
    request's release_id can be resolved.
    """
    artist = row.get("artist_name") or ""
    title = row.get("album_title") or ""
    if not isinstance(artist, str):
        artist = str(artist)
    if not isinstance(title, str):
        title = str(title)
    year = year_from_value(row.get("year"))
    redownload = bool(row.get("source") == "redownload")
    rg_year = _release_group_year_from_value(row.get("release_group_year"))
    return ReleaseSnapshot(
        artist_name=artist,
        title=title,
        year=year,
        track_titles=_track_titles_from_tracks(tracks),
        redownload=redownload,
        prepend_artist=prepend_artist,
        release_group_year=rg_year,
    )


def snapshot_from_add_payload(
    *,
    artist_name: str,
    album_title: str,
    year: object,
    tracks: list[dict[str, Any]],
    source: str,
    prepend_artist: bool = False,
    release_group_year: object = None,
) -> ReleaseSnapshot:
    """Build a `ReleaseSnapshot` from add-time metadata.

    Both CLI `cmd_add` and web `/api/pipeline/add` already resolve a
    release dict + tracks list before calling `set_tracks`. Pass those
    same values through here so add-time generation sees identical
    inputs to a startup-time regeneration of the same release.

    ``release_group_year`` is the first-release year of the MB release
    group (U5 R9). When known AND different from ``year``, the
    generator emits an extra ``unwild_rg_year`` slot so reissues find
    their original-pressing peers on Soulseek.
    """
    return ReleaseSnapshot(
        artist_name=artist_name or "",
        title=album_title or "",
        year=year_from_value(year),
        track_titles=_track_titles_from_tracks(tracks),
        redownload=(source == "redownload"),
        prepend_artist=prepend_artist,
        release_group_year=_release_group_year_from_value(release_group_year),
    )
