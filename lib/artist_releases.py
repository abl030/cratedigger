"""Pure logic for artist release disambiguation — tier-based coverage analysis.

Determines which release groups by an artist contain recordings not
available on any higher-tier release. Tiers: Album > EP > Single.
Multiple pressings of the same release group are unioned.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field


# Tier ordering: lower number = higher priority (Album beats EP beats Single).
_TIER: dict[str, int] = {"Album": 1, "EP": 2, "Single": 3, "Other": 1}


def _get_tier(primary_type: str) -> int:
    return _TIER.get(primary_type, 1)


@dataclass(frozen=True)
class TrackInfo:
    """A track on a release group, annotated with coverage."""

    recording_id: str
    title: str
    unique: bool  # True if no higher-or-equal-tier RG also has this recording
    also_on: list[str]  # titles of RGs that cover this recording


@dataclass(frozen=True)
class PressingInfo:
    """A single pressing (release) within a release group."""

    release_id: str
    title: str
    date: str
    format: str
    track_count: int
    country: str


@dataclass(frozen=True)
class ReleaseGroupInfo:
    """A release group with coverage analysis."""

    release_group_id: str
    title: str
    primary_type: str
    first_date: str
    release_ids: list[str]  # all pressing IDs
    pressings: list[PressingInfo]
    tracks: list[TrackInfo]
    track_count: int
    unique_track_count: int
    covered_by: str | None  # title of the RG that covers ALL tracks, or None
    library_status: str | None = None
    pipeline_status: str | None = None
    pipeline_id: int | None = None


@dataclass(frozen=True)
class ArtistDisambiguation:
    """Full disambiguation result for an artist."""

    artist_id: str
    artist_name: str
    release_groups: list[ReleaseGroupInfo] = field(default_factory=list)


_REMASTER_RE = re.compile(
    r"\s*\(.*(?:remaster|deluxe|bonus|anniversary).*\)\s*$",
    re.IGNORECASE,
)


def _normalize_title_for_rg_dedup(title: str) -> str:
    """Normalize a track title for within-RG deduplication.

    Strips remaster/deluxe parentheticals and lowercases.
    Only used within the same release group — NOT across RGs.
    """
    return _REMASTER_RE.sub("", title).strip().lower()


def filter_non_live(releases: list[dict]) -> list[dict]:
    """Drop releases whose release-group has 'Live' in secondary-types."""
    result: list[dict] = []
    for r in releases:
        rg = r.get("release-group", {})
        secondary = rg.get("secondary-types", [])
        if "Live" not in secondary:
            result.append(r)
    return result


def analyse_artist_releases(releases: list[dict]) -> list[ReleaseGroupInfo]:
    """Analyse releases and return coverage info per release group.

    Algorithm:
    1. Collapse releases into release groups, unioning recordings.
    2. For each RG, determine which recordings are "covered" by a
       higher-tier (or larger same-tier) RG.
    3. A RG is "fully covered" if ALL its recordings are covered by
       a single other RG.
    """
    if not releases:
        return []

    # Step 1: Collapse into release groups
    rg_data: dict[str, _RGData] = {}
    for r in releases:
        rg = r.get("release-group", {})
        rg_id = rg.get("id", "")
        if not rg_id:
            continue

        if rg_id not in rg_data:
            rg_data[rg_id] = _RGData(
                rg_id=rg_id,
                title=rg.get("title", ""),
                primary_type=rg.get("primary-type", "Other"),
                tier=_get_tier(rg.get("primary-type", "Other")),
                first_date=r.get("date", ""),
                release_ids=[],
                pressings=[],
                recordings=set(),
                track_list=[],
                seen_titles=set(),
            )

        data = rg_data[rg_id]
        data.release_ids.append(r["id"])

        # Collect pressing info
        media = r.get("media", [])
        track_count = sum(len(m.get("tracks", [])) for m in media)
        formats = [m.get("format") or "?" for m in media]
        data.pressings.append(PressingInfo(
            release_id=r["id"],
            title=r.get("title", ""),
            date=r.get("date", ""),
            format=", ".join(formats) if formats else "?",
            track_count=track_count,
            country=r.get("country", ""),
        ))
        if data.first_date and r.get("date", "") and r["date"] < data.first_date:
            data.first_date = r["date"]
        elif not data.first_date:
            data.first_date = r.get("date", "")

        # Union recordings; deduplicate track list by normalized title within RG
        for medium in r.get("media", []):
            for track in medium.get("tracks", []):
                rec_id = track.get("recording", {}).get("id")
                if rec_id:
                    data.recordings.add(rec_id)
                    title = track.get("title", "")
                    norm = _normalize_title_for_rg_dedup(title)
                    if norm not in data.seen_titles:
                        data.seen_titles.add(norm)
                        data.track_list.append((rec_id, title))

    # Step 2: For each recording, find which RGs contain it
    rec_to_rgs: dict[str, set[str]] = {}
    for rg_id, data in rg_data.items():
        for rec_id in data.recordings:
            if rec_id not in rec_to_rgs:
                rec_to_rgs[rec_id] = set()
            rec_to_rgs[rec_id].add(rg_id)

    # Step 3: For each RG, determine coverage
    result: list[ReleaseGroupInfo] = []
    for rg_id, data in rg_data.items():
        # For each recording, is it covered by a better RG?
        track_infos: list[TrackInfo] = []
        uncovered_count = 0

        for rec_id, title in data.track_list:
            other_rg_ids = rec_to_rgs.get(rec_id, set()) - {rg_id}
            # A recording is covered if ANY other RG with higher tier
            # (or same tier + more recordings) also has it
            covering_rgs: list[str] = []
            for other_id in sorted(other_rg_ids):
                other = rg_data[other_id]
                if _covers(other, data):
                    covering_rgs.append(other.title)

            is_unique = len(covering_rgs) == 0
            if is_unique:
                uncovered_count += 1

            track_infos.append(TrackInfo(
                recording_id=rec_id,
                title=title,
                unique=is_unique,
                also_on=covering_rgs,
            ))

        # Is the whole RG covered by a single other RG?
        covered_by = _find_single_cover(rg_id, data, rg_data)

        result.append(ReleaseGroupInfo(
            release_group_id=rg_id,
            title=data.title,
            primary_type=data.primary_type,
            first_date=data.first_date,
            release_ids=data.release_ids,
            pressings=data.pressings,
            tracks=track_infos,
            track_count=len(track_infos),
            unique_track_count=uncovered_count,
            covered_by=covered_by,
        ))

    return result


def _covers(candidate: _RGData, target: _RGData) -> bool:
    """Does candidate RG cover target RG's recordings?

    A candidate covers a target if:
    - It's a strictly higher tier, OR
    - It's the same tier with more recordings, OR
    - It's the same tier with the same count but released earlier.
    """
    if candidate.tier < target.tier:
        return True
    if candidate.tier == target.tier:
        if len(candidate.recordings) > len(target.recordings):
            return True
        if (len(candidate.recordings) == len(target.recordings)
                and candidate.first_date < target.first_date
                and candidate.first_date):
            return True
    return False


def _find_single_cover(
    rg_id: str, data: _RGData, all_rgs: dict[str, _RGData]
) -> str | None:
    """Find a single RG that covers ALL of this RG's recordings, if any."""
    for other_id, other in all_rgs.items():
        if other_id == rg_id:
            continue
        if not _covers(other, data):
            continue
        # Does this other RG contain ALL our recordings?
        if data.recordings <= other.recordings:
            return other.title
    return None


@dataclass
class _RGData:
    """Internal mutable state for building release group info."""

    rg_id: str
    title: str
    primary_type: str
    tier: int
    first_date: str
    release_ids: list[str]
    pressings: list[PressingInfo]
    recordings: set[str]
    track_list: list[tuple[str, str]]  # (recording_id, title)
    seen_titles: set[str]  # normalized titles for within-RG dedup
