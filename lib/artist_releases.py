"""Pure logic for artist release disambiguation — recording uniqueness analysis."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class TrackInfo:
    """A track on a release, annotated with uniqueness."""

    recording_id: str
    title: str
    position: int
    disc: int
    length_seconds: float | None
    unique: bool  # True if this recording only appears on one release
    also_on: list[str]  # titles of other releases containing this recording


@dataclass(frozen=True)
class ReleaseInfo:
    """A release annotated with unique track info and library/pipeline status."""

    release_id: str
    title: str
    date: str
    format: str
    track_count: int
    release_group_id: str
    release_group_title: str
    release_group_type: str
    tracks: list[TrackInfo]
    unique_track_count: int
    library_status: str | None = None  # None or "in_library"
    pipeline_status: str | None = None  # None, "wanted", "imported", etc.


@dataclass(frozen=True)
class ArtistDisambiguation:
    """Full disambiguation result for an artist."""

    artist_id: str
    artist_name: str
    releases: list[ReleaseInfo] = field(default_factory=list)


def filter_non_live(releases: list[dict]) -> list[dict]:
    """Drop releases whose release-group has 'Live' in secondary-types."""
    result: list[dict] = []
    for r in releases:
        rg = r.get("release-group", {})
        secondary = rg.get("secondary-types", [])
        if "Live" not in secondary:
            result.append(r)
    return result


def build_recording_map(releases: list[dict]) -> dict[str, set[str]]:
    """Build recording_id → set of release_ids map."""
    rec_map: dict[str, set[str]] = {}
    for r in releases:
        release_id: str = r["id"]
        for medium in r.get("media", []):
            for track in medium.get("tracks", []):
                rec_id = track.get("recording", {}).get("id")
                if rec_id:
                    if rec_id not in rec_map:
                        rec_map[rec_id] = set()
                    rec_map[rec_id].add(release_id)
    return rec_map


def mark_unique_tracks(
    releases: list[dict], recording_map: dict[str, set[str]]
) -> list[ReleaseInfo]:
    """Annotate each track with uniqueness, build ReleaseInfo list."""
    # Build release_id → title map for also_on labels
    release_titles: dict[str, str] = {}
    for r in releases:
        release_titles[r["id"]] = r.get("title", "")

    result: list[ReleaseInfo] = []
    for r in releases:
        rg = r.get("release-group", {})
        own_id = r["id"]
        tracks: list[TrackInfo] = []
        formats: list[str] = []

        for medium in r.get("media", []):
            disc = medium.get("position", 1)
            fmt = medium.get("format") or "?"
            if fmt not in formats:
                formats.append(fmt)
            for track in medium.get("tracks", []):
                rec_id = track.get("recording", {}).get("id", "")
                length_ms = track.get("length")
                length_seconds = round(length_ms / 1000, 1) if length_ms else None
                rel_ids = recording_map.get(rec_id, set())
                is_unique = len(rel_ids) == 1
                other_ids = sorted(rel_ids - {own_id})
                also_on = [release_titles.get(rid, rid) for rid in other_ids]
                tracks.append(
                    TrackInfo(
                        recording_id=rec_id,
                        title=track.get("title", ""),
                        position=track.get("position", 0),
                        disc=disc,
                        length_seconds=length_seconds,
                        unique=is_unique,
                        also_on=also_on,
                    )
                )

        unique_count = sum(1 for t in tracks if t.unique)
        result.append(
            ReleaseInfo(
                release_id=r["id"],
                title=r.get("title", ""),
                date=r.get("date", ""),
                format=", ".join(formats),
                track_count=len(tracks),
                release_group_id=rg.get("id", ""),
                release_group_title=rg.get("title", ""),
                release_group_type=rg.get("primary-type", ""),
                tracks=tracks,
                unique_track_count=unique_count,
            )
        )
    return result
