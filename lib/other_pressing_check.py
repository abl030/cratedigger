"""Does this copy match another pressing? (issue #1445)

An ON-DEMAND, read-only check the operator runs for one download_log row.
It loads the selected candidate's mapping (the file length mapped to each
release track) and the target release id, asks the MusicBrainz mirror for
the target release and every other release in its release group, and
answers in plain sentences, claiming no more than the lengths show:

- **This release's own listing first.** When every file is within 2s of
  the target's listed length, the copy fits its own release; any other
  pressing the files also fit is reported as indistinguishable by length,
  never as a finding.
- **Other pressings.** Only when the files do NOT fit the target's listing
  is a sibling whose listing they do fit reported as a match, with the
  positions where its titles differ.
- **Disc IDs.** A disc ID carries a physical table of contents. The
  target's LISTED lengths are compared with each attached disc, and so are
  the FILES, separately: the listing may match a disc or not, and the copy
  may match the disc the listing matches, a different attached disc, or
  none. No disc ID means only that there is nothing to check against; a
  digital or vinyl medium can never have one.

Nothing here writes anything, feeds a pipeline decision, or runs at render
time. One code path serves ``GET /api/other-pressings/<id>`` and
``pipeline-cli other-pressings``.
"""

from __future__ import annotations

import json
import math
import time
import unicodedata
import urllib.error
from collections.abc import Callable, Mapping, Sequence
from enum import StrEnum
from http.client import HTTPException
from typing import Final, Protocol

import msgspec

from lib import mb_api
from lib.api_bases import PUBLIC_MB_ORIGIN
from lib.json_narrow import is_str_object_dict, json_list
from lib.mb_api import (
    MBDisc,
    MBDiscMedium,
    MBDiscRelease,
    MBDiscReleasePage,
    MBDiscTrack,
)
from lib.release_identity import detect_release_source, normalize_release_id
from lib.track_length_shape import SAME_AUDIO_TOLERANCE_SECONDS
from lib.validation_envelope import decode_validation_envelope

#: Releases read from one release group at most. Big groups (reissue-heavy
#: classics) run to hundreds; the result says when the cap was hit.
RELEASE_GROUP_CAP: Final[int] = 150
#: MusicBrainz returns at most 25 releases per page with recordings included.
RELEASE_GROUP_PAGE_SIZE: Final[int] = 25
#: The whole check gives up after this long, so a slow mirror cannot hang
#: the request.
TOTAL_BUDGET_SECONDS: Final[float] = 30.0
#: One wait never exceeds this. A request may wait for a mirror slot, make
#: one attempt and retry a dropped connection once, so each gets at most a
#: third of what is left of the budget.
PER_REQUEST_TIMEOUT_SECONDS: Final[float] = 10.0
#: CD frames per second in a disc table of contents.
_SECTORS_PER_SECOND: Final[float] = 75.0
#: Above the 2s tolerance and up to this, a track is "a few seconds off";
#: beyond it, it does not match.
_FEW_SECONDS: Final[float] = 5.0

#: Everything a mirror read can raise: connection and timeout failures
#: (``OSError``, which covers ``URLError`` and a busy mirror slot), a broken
#: HTTP exchange, and an undecodable or wrongly shaped body (``ValueError``,
#: which covers ``json.JSONDecodeError`` and ``msgspec.ValidationError``).
_MIRROR_ERRORS = (OSError, HTTPException, ValueError)


class PressingMirror(Protocol):
    """The two MusicBrainz reads the check needs."""

    def release(self, release_id: str, *, timeout: float) -> MBDiscRelease: ...

    def release_group_page(
        self, release_group_id: str, *, offset: int, limit: int,
        timeout: float,
    ) -> MBDiscReleasePage: ...


class MusicBrainzPressingMirror:
    """The configured mirror, through the project's one MusicBrainz client."""

    def release(self, release_id: str, *, timeout: float) -> MBDiscRelease:
        return mb_api.get_release_with_discids(release_id, timeout=timeout)

    def release_group_page(
        self, release_group_id: str, *, offset: int, limit: int,
        timeout: float,
    ) -> MBDiscReleasePage:
        return mb_api.browse_release_group_with_discids(
            release_group_id, offset=offset, limit=limit, timeout=timeout)


class DownloadLogSource(Protocol):
    def get_download_log_entry(
        self, log_id: int,
    ) -> Mapping[str, object] | None: ...


class PressingCheckOutcome(StrEnum):
    CHECKED = "checked"
    DOWNLOAD_LOG_NOT_FOUND = "download_log_not_found"
    NO_MATCHED_RELEASE = "no_matched_release"
    DISCOGS_RELEASE = "discogs_release"
    RELEASE_NOT_FOUND = "release_not_found"
    MIRROR_UNAVAILABLE = "mirror_unavailable"


class MediumDiscStatus(StrEnum):
    #: The medium's format cannot carry a disc ID (digital, vinyl, DVD...).
    NO_DISC_FORMAT = "no_disc_format"
    #: It could, but none is attached.
    NO_DISC_ID = "no_disc_id"
    #: One is attached but its table of contents does not fit the tracks.
    DISC_ID_UNUSABLE = "disc_id_unusable"
    #: At least one attached table of contents was compared.
    CHECKED = "checked"


class FilesAgainstDisc(StrEnum):
    #: The files fit a disc the listing also fits.
    LISTING_DISC = "listing_disc"
    #: The files fit an attached disc the listing does not fit.
    OTHER_DISC = "other_disc"
    #: The files fit no attached disc.
    NO_DISC = "no_disc"


class TitleDifference(msgspec.Struct, frozen=True, kw_only=True):
    """One position where the other pressing names the track differently.

    ``medium``/``position`` are the TARGET release's position at that
    point in the running order."""

    medium: int
    position: int
    pressing_title: str
    release_title: str


class MatchingPressing(msgspec.Struct, frozen=True, kw_only=True):
    release_id: str
    url: str
    title: str
    country: str | None
    date: str | None
    formats: list[str]
    disc_id_count: int
    #: Largest gap between a file and this pressing's listed length.
    worst_offset_seconds: float
    title_differences: list[TitleDifference]
    #: "UK CD, 2004": how the sentences name this pressing.
    description: str


class DiscTrack(msgspec.Struct, frozen=True, kw_only=True):
    """One track of one attached disc, against the listing and the file."""

    position: int
    title: str
    disc_seconds: float
    listed_seconds: float | None
    file_seconds: float | None
    #: ``None`` when the release lists no length for this track.
    listing_agrees: bool | None
    #: ``None`` when no file is mapped to this track.
    file_agrees: bool | None


class DiscCheck(msgspec.Struct, frozen=True, kw_only=True):
    disc_id: str
    #: Every track has a listed length within 2s of this disc.
    listing_agrees: bool
    #: Every mapped file is within 2s of this disc; ``None`` with no files.
    files_agree: bool | None
    tracks: list[DiscTrack]


class MediumLengths(msgspec.Struct, frozen=True, kw_only=True):
    medium: int
    format: str | None
    status: MediumDiscStatus
    #: Every usable attached disc ID, compared.
    discs: list[DiscCheck]
    #: The disc the listing fits, if any.
    listing_disc_id: str | None
    #: The disc the files fit, if any.
    files_disc_id: str | None
    #: ``None`` when no disc was usable or no file is mapped to this medium.
    files_against_disc: FilesAgainstDisc | None


class TargetLengths(msgspec.Struct, frozen=True, kw_only=True):
    has_disc_id: bool
    media: list[MediumLengths]


class PressingCheck(msgspec.Struct, frozen=True, kw_only=True):
    outcome: PressingCheckOutcome
    download_log_id: int
    sentences: list[str]
    target_release_id: str | None = None
    target_url: str | None = None
    release_group_id: str | None = None
    #: False when the files do not line up one-to-one with the release.
    compared_with_pressings: bool = False
    #: Every file within 2s of the target's own listed length. ``None`` when
    #: not compared or the target lists no length for some track.
    fits_own_listing: bool | None = None
    #: Other pressings the files fit where they do NOT fit the target.
    matching_pressings: list[MatchingPressing] = msgspec.field(
        default_factory=list[MatchingPressing])
    #: Other pressings the files fit as well as the target's own listing:
    #: track lengths cannot tell them apart.
    same_listing_pressings: list[MatchingPressing] = msgspec.field(
        default_factory=list[MatchingPressing])
    target_lengths: TargetLengths | None = None
    #: Releases in the group as MusicBrainz counts them.
    releases_in_group: int = 0
    #: Other distinct releases actually read (the target itself excluded).
    releases_checked: int = 0
    capped: bool = False
    #: Short technical reason on a mirror failure, for logs and --json.
    detail: str | None = None


class _OutOfTime(Exception):
    pass


# --- reading the stored attempt -------------------------------------------

class _Attempt(msgspec.Struct, frozen=True, kw_only=True):
    release_id: str
    #: (medium, position) -> file seconds, for every mapped release track.
    files: dict[tuple[int, int], float]
    #: The files line up one-to-one with mapped tracks and nothing extra.
    clean: bool


def _as_int(value: object) -> int | None:
    if isinstance(value, bool) or not isinstance(value, int):
        return None
    return value


def _as_seconds(value: object) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    seconds = float(value)
    return seconds if math.isfinite(seconds) and seconds > 0 else None


def _selected_attempt(row: Mapping[str, object]) -> _Attempt | None:
    try:
        envelope = decode_validation_envelope(row.get("validation_result"))
    except (msgspec.ValidationError, json.JSONDecodeError):
        return None
    for candidate in envelope.candidates:
        if candidate.get("is_target") is not True:
            continue
        release_id = normalize_release_id(candidate.get("album_id"))
        if not release_id:
            return None
        files: dict[tuple[int, int], float] = {}
        clean = (
            not json_list(candidate.get("extra_items"))
            and not json_list(candidate.get("extra_tracks"))
        )
        for pair in json_list(candidate.get("mapping")):
            if not is_str_object_dict(pair):
                clean = False
                continue
            item, track = pair.get("item"), pair.get("track")
            if not is_str_object_dict(item) or not is_str_object_dict(track):
                clean = False
                continue
            medium = _as_int(track.get("medium"))
            position = _as_int(track.get("medium_index"))
            seconds = _as_seconds(item.get("length"))
            if medium is None or position is None or seconds is None:
                clean = False
                continue
            if (medium, position) in files:
                clean = False
            files[(medium, position)] = seconds
        return _Attempt(release_id=release_id, files=files, clean=clean)
    return None


# --- comparing --------------------------------------------------------------

def _listed_seconds(track: MBDiscTrack) -> float | None:
    """The track's listed length, falling back to its recording's length
    exactly as Beets does when the track carries none."""
    milliseconds = track.length
    if (milliseconds is None or milliseconds <= 0) and track.recording:
        milliseconds = track.recording.length
    if milliseconds is None or milliseconds <= 0:
        return None
    return milliseconds / 1000.0


def _running_order(release: MBDiscRelease) -> list[tuple[int, int, MBDiscTrack]]:
    order: list[tuple[int, int, MBDiscTrack]] = []
    for medium in sorted(release.media, key=lambda m: m.position):
        for index, track in enumerate(medium.tracks, 1):
            order.append((medium.position, track.position or index, track))
    return order


def _normalize_title(title: str) -> str:
    """Letters, digits and their combining marks, case-folded: 'Drip, Drip,
    Drip' and 'Drip Drip Drip', or '...and' and '… And', are the same
    title, while 'कि' and 'का' (which differ only in a vowel mark) are not.
    A title with no letters at all ('?', '!') is compared as written."""
    folded = unicodedata.normalize("NFKC", title).casefold()
    kept = "".join(
        char for char in folded
        if char.isalnum() or unicodedata.category(char).startswith("M"))
    return kept or folded.strip()


_COUNTRY_NAMES: Final[dict[str, str]] = {
    "GB": "UK", "US": "US", "XE": "Europe", "XW": "Worldwide",
    "JP": "Japan", "DE": "Germany", "FR": "France", "CA": "Canada",
    "AU": "Australia", "NL": "Netherlands", "IT": "Italy", "ES": "Spain",
    "SE": "Sweden", "NZ": "New Zealand", "BR": "Brazil", "KR": "Korea",
    "MX": "Mexico", "RU": "Russia", "AR": "Argentina", "BE": "Belgium",
    "DK": "Denmark", "NO": "Norway", "FI": "Finland", "AT": "Austria",
    "CH": "Switzerland", "IE": "Ireland", "PL": "Poland", "TW": "Taiwan",
    "HK": "Hong Kong", "CN": "China", "ZA": "South Africa", "PT": "Portugal",
    "GR": "Greece", "TR": "Turkey", "IN": "India", "IL": "Israel",
}


_FORMAT_WORDS: Final[dict[str, str]] = {"Digital Media": "digital release"}


def _formats(release: MBDiscRelease) -> list[str]:
    return [medium.format or "unknown format" for medium in release.media]


def _format_word(fmt: str) -> str:
    return _FORMAT_WORDS.get(fmt, fmt)


def _describe(release: MBDiscRelease) -> str:
    """'UK CD, 2004', '2×CD', 'Europe 12" Vinyl + CD'."""
    formats = [_format_word(fmt) for fmt in _formats(release)]
    if formats and all(fmt == formats[0] for fmt in formats):
        medium = formats[0] if len(formats) == 1 else f"{len(formats)}×{formats[0]}"
    else:
        medium = " + ".join(formats)
    country = _COUNTRY_NAMES.get(release.country or "", release.country or "")
    head = " ".join(part for part in (country, medium) if part)
    year = (release.date or "")[:4]
    if head and year:
        return f"{head}, {year}"
    return head or year or "an undated pressing"


def _url(release_id: str) -> str:
    return f"{PUBLIC_MB_ORIGIN}/release/{release_id}"


def _fit(
    file_lengths: Sequence[float], listed: Sequence[float | None],
) -> float | None:
    """The largest file-to-listing gap when every track has a listing and
    every gap is within tolerance, else ``None``."""
    if len(listed) != len(file_lengths):
        return None
    worst = 0.0
    for file_seconds, listed_seconds in zip(file_lengths, listed, strict=True):
        if listed_seconds is None:
            return None
        gap = abs(file_seconds - listed_seconds)
        if gap > SAME_AUDIO_TOLERANCE_SECONDS:
            return None
        worst = max(worst, gap)
    return worst


def _match(
    target_order: Sequence[tuple[int, int, MBDiscTrack]],
    file_lengths: Sequence[float],
    sibling: MBDiscRelease,
) -> MatchingPressing | None:
    sibling_order = _running_order(sibling)
    worst = _fit(
        file_lengths,
        [_listed_seconds(track) for _, _, track in sibling_order])
    if worst is None:
        return None
    differences = [
        TitleDifference(
            medium=medium, position=position,
            pressing_title=sibling_track.title,
            release_title=target_track.title,
        )
        for (medium, position, target_track), (_, _, sibling_track)
        in zip(target_order, sibling_order, strict=True)
        if _normalize_title(target_track.title)
        != _normalize_title(sibling_track.title)
    ]
    return MatchingPressing(
        release_id=sibling.id, url=_url(sibling.id), title=sibling.title,
        country=sibling.country, date=sibling.date,
        formats=_formats(sibling),
        disc_id_count=sum(len(medium.discs) for medium in sibling.media),
        worst_offset_seconds=round(worst, 1),
        title_differences=differences,
        description=_describe(sibling),
    )


def _disc_lengths(disc: MBDisc, track_count: int) -> list[float] | None:
    """Per-track seconds from a table of contents, or ``None`` when it
    does not describe exactly this medium's tracks (a data track on an
    enhanced CD, a truncated entry)."""
    offsets = disc.offsets
    if (
        track_count == 0
        or disc.offset_count != track_count
        or len(offsets) != track_count
    ):
        return None
    ends = [*offsets[1:], disc.sectors]
    lengths = [
        round((end - start) / _SECTORS_PER_SECOND, 1)
        for start, end in zip(offsets, ends, strict=True)
    ]
    if any(length <= 0 for length in lengths):
        return None
    return lengths


def _can_carry_disc_id(fmt: str | None) -> bool:
    """MusicBrainz disc IDs exist only for CD media (CD, HDCD, Enhanced CD,
    SHM-CD, hybrid SACD...). An unknown format might be one."""
    return fmt is None or "CD" in fmt


def _within(a: float | None, b: float) -> bool | None:
    return None if a is None else abs(a - b) <= SAME_AUDIO_TOLERANCE_SECONDS


def _disc_check(
    medium: MBDiscMedium, disc: MBDisc, lengths: Sequence[float],
    files: Mapping[tuple[int, int], float],
) -> DiscCheck:
    rows: list[DiscTrack] = []
    for index, (track, disc_seconds) in enumerate(
        zip(medium.tracks, lengths, strict=True), 1,
    ):
        position = track.position or index
        listed = _listed_seconds(track)
        file_seconds = files.get((medium.position, position))
        rows.append(DiscTrack(
            position=position, title=track.title, disc_seconds=disc_seconds,
            listed_seconds=listed, file_seconds=file_seconds,
            listing_agrees=_within(listed, disc_seconds),
            file_agrees=_within(file_seconds, disc_seconds),
        ))
    compared = [row.file_agrees for row in rows if row.file_agrees is not None]
    return DiscCheck(
        disc_id=disc.id,
        listing_agrees=all(row.listing_agrees is True for row in rows),
        files_agree=all(compared) if compared else None,
        tracks=rows,
    )


def _medium_lengths(
    medium: MBDiscMedium, files: Mapping[tuple[int, int], float],
) -> MediumLengths:
    checks = [
        _disc_check(medium, disc, lengths, files)
        for disc in medium.discs
        if (lengths := _disc_lengths(disc, len(medium.tracks))) is not None
    ]
    if not checks:
        status = (
            MediumDiscStatus.DISC_ID_UNUSABLE if medium.discs
            else MediumDiscStatus.NO_DISC_ID if _can_carry_disc_id(medium.format)
            else MediumDiscStatus.NO_DISC_FORMAT
        )
        return MediumLengths(
            medium=medium.position, format=medium.format, status=status,
            discs=[], listing_disc_id=None, files_disc_id=None,
            files_against_disc=None)
    listing_discs = [check for check in checks if check.listing_agrees]
    both = [check for check in listing_discs if check.files_agree]
    files_only = [check for check in checks if check.files_agree]
    if all(check.files_agree is None for check in checks):
        against: FilesAgainstDisc | None = None
        files_disc: DiscCheck | None = None
    elif both:
        against, files_disc = FilesAgainstDisc.LISTING_DISC, both[0]
    elif files_only:
        against, files_disc = FilesAgainstDisc.OTHER_DISC, files_only[0]
    else:
        against, files_disc = FilesAgainstDisc.NO_DISC, None
    listing_disc = both[0] if both else (listing_discs[0] if listing_discs else None)
    return MediumLengths(
        medium=medium.position, format=medium.format,
        status=MediumDiscStatus.CHECKED, discs=checks,
        listing_disc_id=listing_disc.disc_id if listing_disc else None,
        files_disc_id=files_disc.disc_id if files_disc else None,
        files_against_disc=against,
    )


def _target_lengths(
    target: MBDiscRelease, files: Mapping[tuple[int, int], float],
) -> TargetLengths:
    return TargetLengths(
        has_disc_id=any(medium.discs for medium in target.media),
        media=[
            _medium_lengths(medium, files)
            for medium in sorted(target.media, key=lambda m: m.position)
        ],
    )


# --- sentences --------------------------------------------------------------

def _join(parts: Sequence[str], *, separator: str = ", ") -> str:
    if len(parts) == 1:
        return parts[0]
    return f"{separator.join(parts[:-1])} and {parts[-1]}"


def _quoted(titles: Sequence[str]) -> str:
    return _join([f"'{title}'" for title in titles])


#: Title differences spelled out before the sentence just counts them.
_SHOWN_DIFFERENCES: Final[int] = 3
#: Pressings named in a list before "and N more".
_SHOWN_PRESSINGS: Final[int] = 4
_COUNT_WORDS: Final[dict[int, str]] = {2: "Both", 3: "All three", 4: "All four"}
_HEDGE = (
    "This may be the same audio described differently (for example where a "
    "hidden track is counted), not necessarily a different disc."
)


def _numbers(positions: Sequence[int]) -> str:
    noun = "tracks" if len(positions) > 1 else "track"
    return f"{noun} {_join([str(p) for p in positions])}"


def _positions(keys: Sequence[tuple[int, int]], *, multi_disc: bool) -> str:
    """'tracks 2 and 5', or per disc on a multi-disc release."""
    if not multi_disc:
        return _numbers([position for _, position in keys])
    parts: list[str] = []
    for medium in sorted({medium for medium, _ in keys}):
        numbers = [position for m, position in keys if m == medium]
        parts.append(f"{_numbers(numbers)} on disc {medium}")
    return _join(parts)


def _pressing_list(descriptions: Sequence[str]) -> str:
    """Repeated descriptions collapse ('US CD, 1997 (2 releases)'); items
    are separated by semicolons because a description holds a comma."""
    counted: dict[str, int] = {}
    for description in descriptions:
        counted[description] = counted.get(description, 0) + 1
    items = [
        description if count == 1 else f"{description} ({count} releases)"
        for description, count in counted.items()
    ]
    if len(items) > _SHOWN_PRESSINGS:
        rest = len(items) - _SHOWN_PRESSINGS
        return (
            f"{'; '.join(items[:_SHOWN_PRESSINGS])} and {rest} more")
    return _join(items, separator="; ")


def _differences_clause(differences: Sequence[TitleDifference]) -> str:
    """'have 'X' and 'Y' where this release lists 'A' and 'B'' (no subject,
    plural verb; the caller conjugates)."""
    if len(differences) > _SHOWN_DIFFERENCES:
        first = differences[0]
        return (
            f"name {len(differences)} tracks differently from this release, "
            f"starting with '{first.pressing_title}' where this release "
            f"lists '{first.release_title}'"
        )
    return (
        f"have {_quoted([d.pressing_title for d in differences])} where this "
        f"release lists {_quoted([d.release_title for d in differences])}"
    )


def _singular(clause: str) -> str:
    if clause.startswith("have "):
        return "has " + clause[len("have "):]
    if clause.startswith("name "):
        return "names " + clause[len("name "):]
    return clause


def _same_listing_sentence(same: Sequence[MatchingPressing]) -> str:
    if not same:
        return "No other pressing in this release group has the same listing."
    if len(same) == 1:
        return (
            "1 other pressing has the same listing, so track lengths cannot "
            "tell them apart.")
    return (
        f"{len(same)} other pressings have the same listing, so track "
        "lengths cannot tell them apart.")


def _match_sentences(
    matches: Sequence[MatchingPressing], track_count: int,
) -> list[str]:
    if not matches:
        return ["No other pressing in this release group matches."]
    if len(matches) == 1:
        lead = (
            f"This copy matches another pressing: {matches[0].description}. "
            f"All {track_count} tracks line up within two seconds."
        )
    else:
        lead = (
            f"This copy matches {len(matches)} other pressings: "
            f"{_pressing_list([m.description for m in matches])}. All "
            f"{track_count} tracks line up within two seconds on each."
        )
    sentences = [lead]
    groups: dict[tuple[TitleDifference, ...], list[MatchingPressing]] = {}
    for match in matches:
        groups.setdefault(tuple(match.title_differences), []).append(match)
    same_titles = () in groups
    if len(groups) == 1:
        (differences,) = groups
        plural = len(matches) > 1
        if not differences:
            possessive = "Their" if plural else "Its"
            sentences.append(
                f"{possessive} titles are the same as this release's, so "
                f"the listings differ only in length. {_HEDGE}")
            return sentences
        clause = _differences_clause(differences)
        if plural:
            subject = _COUNT_WORDS.get(len(matches), f"All {len(matches)}")
            sentences.append(f"{subject} {clause}.")
        else:
            sentences.append(f"That pressing {_singular(clause)}.")
        return sentences
    for differences, members in groups.items():
        subject = "The " + _pressing_list([m.description for m in members])
        plural = len(members) > 1
        if not differences:
            verb = "have" if plural else "has"
            sentences.append(
                f"{subject} {verb} the same titles as this release, so those "
                "listings differ only in length.")
            continue
        clause = _differences_clause(differences)
        sentences.append(
            f"{subject} {clause if plural else _singular(clause)}.")
    if same_titles:
        sentences.append(_HEDGE)
    return sentences


_FORMAT_PHRASES: Final[dict[str, str]] = {
    "Digital Media": "a digital release",
    "Vinyl": "a vinyl record", '12" Vinyl': "a vinyl record",
    '7" Vinyl': "a vinyl record", '10" Vinyl': "a vinyl record",
    "Cassette": "a cassette",
}


def _format_phrase(fmt: str | None) -> str:
    if fmt is None:
        return "a release of unknown format"
    return _FORMAT_PHRASES.get(fmt, f"a {fmt}")


def _disc_reference(medium: MediumLengths) -> tuple[str, DiscCheck]:
    """The disc the files are measured against when they fit none, and how
    to name it: the listing's disc, the only disc, or the closest one."""
    if medium.listing_disc_id is not None:
        disc = next(d for d in medium.discs if d.disc_id == medium.listing_disc_id)
        return "that disc", disc

    def worst(check: DiscCheck) -> float:
        return max(
            (abs(row.file_seconds - row.disc_seconds) for row in check.tracks
             if row.file_seconds is not None),
            default=0.0)

    closest = min(medium.discs, key=worst)
    if len(medium.discs) == 1:
        return "the attached disc", closest
    return f"the closest of the {len(medium.discs)} attached discs", closest


def _files_sentence(medium: MediumLengths) -> str | None:
    against = medium.files_against_disc
    count = len(medium.discs)
    if against is None:
        return None
    if against is FilesAgainstDisc.LISTING_DISC:
        return "This copy matches that disc on every track."
    if against is FilesAgainstDisc.OTHER_DISC:
        if medium.listing_disc_id is not None:
            return "This copy matches a different attached disc than the listing does."
        if count == 1:
            return "This copy matches the attached disc on every track."
        return f"This copy matches one of the {count} attached discs on every track."
    name, disc = _disc_reference(medium)
    off = [
        row for row in disc.tracks
        if row.file_agrees is False and row.file_seconds is not None]
    far = [r.position for r in off
           if abs((r.file_seconds or 0.0) - r.disc_seconds) > _FEW_SECONDS]
    near = [r.position for r in off
            if abs((r.file_seconds or 0.0) - r.disc_seconds) <= _FEW_SECONDS]
    if far and near:
        return (
            f"This copy does not match {name} on {_numbers(far)}, and is a "
            f"few seconds off on {_numbers(near)}.")
    if far:
        return f"This copy does not match {name} on {_numbers(far)}."
    return f"This copy is a few seconds off {name} on {_numbers(near)}."


def _listing_sentence(medium: MediumLengths, *, subject: str | None) -> str:
    """``subject`` is ``None`` for a one-disc release, else 'Disc 2'."""
    count = len(medium.discs)
    whose = "the listed lengths" if subject is None else f"{subject}'s listed lengths"
    if medium.listing_disc_id is not None:
        where = "this release" if subject is None else "it"
        if count == 1:
            return f"{whose.capitalize()} match the disc ID attached to {where}."
        return (
            f"{whose.capitalize()} match one of the {count} disc IDs attached "
            f"to {where}.")
    attached = (
        ("A disc ID is attached" if count == 1
         else f"{count} disc IDs are attached")
        if subject is None else
        (f"{subject} has a disc ID attached" if count == 1
         else f"{subject} has {count} disc IDs attached")
    )
    listed = "the listed lengths" if subject is None else "its listed lengths"
    if count > 1:
        return (
            f"{attached}, but {listed} do not match any of them, so the "
            "listing may be wrong.")
    rows = medium.discs[0].tracks
    off = [row.position for row in rows if row.listing_agrees is False]
    missing = [row.position for row in rows if row.listing_agrees is None]
    if off and missing:
        return (
            f"{attached}, but {listed} do not match it on {_numbers(off)} and "
            f"give no length for {_numbers(missing)}, so the listing may be "
            "wrong.")
    if missing:
        return (
            f"{attached}, but the listing gives no length for "
            f"{_numbers(missing)}, so it cannot be checked in full against "
            "the disc.")
    return (
        f"{attached}, but {listed} do not match it on {_numbers(off)}, so the "
        "listing may be wrong.")


def _medium_sentences(medium: MediumLengths, *, subject: str | None) -> list[str]:
    if medium.status is MediumDiscStatus.NO_DISC_FORMAT:
        if subject is None:
            return [
                (f"This is {_format_phrase(medium.format)}, so there is no disc "
                "to check the listed lengths against.")]
        return [
            (f"{subject} is {_format_phrase(medium.format)}, so there is no "
            "disc to check its listed lengths against.")]
    if medium.status is MediumDiscStatus.NO_DISC_ID:
        where = "this release" if subject is None else subject.lower()
        listed = "the listed lengths" if subject is None else "its listed lengths"
        return [
            (f"No disc ID is attached to {where}, so there is no disc to check "
            f"{listed} against.")]
    if medium.status is MediumDiscStatus.DISC_ID_UNUSABLE:
        if subject is None:
            return [
                ("A disc ID is attached, but its track layout does not fit "
                "this release's tracks, so it cannot be used to check the "
                "listed lengths.")]
        return [
            (f"{subject} has a disc ID attached, but its track layout does not "
            "fit that disc's tracks, so it cannot be used to check the listed "
            "lengths.")]
    sentences = [_listing_sentence(medium, subject=subject)]
    files = _files_sentence(medium)
    if files is not None:
        sentences.append(files)
    return sentences


def _lengths_sentences(lengths: TargetLengths) -> list[str]:
    if len(lengths.media) == 1:
        return _medium_sentences(lengths.media[0], subject=None)
    sentences: list[str] = []
    for medium in lengths.media:
        sentences.extend(
            _medium_sentences(medium, subject=f"Disc {medium.medium}"))
    return sentences


def _cap_sentence(read: int, in_group: int) -> str:
    return (
        f"Only the first {read} of {in_group} releases in this release "
        "group were checked."
    )


# --- the check ----------------------------------------------------------------

_SENTENCE_NO_ROW = "This attempt could not be found."
_SENTENCE_NO_RELEASE = "This attempt has no matched release to compare against."
_SENTENCE_DISCOGS = (
    "Checking other pressings is not available for Discogs releases.")
_SENTENCE_GONE = (
    "MusicBrainz no longer has this release, so other pressings could not "
    "be checked.")
_SENTENCE_UNREACHABLE = (
    "Could not reach MusicBrainz, so other pressings were not checked. Try "
    "again later.")
_SENTENCE_NOT_ALIGNED = (
    "Not every file lines up with a track on this release, so other "
    "pressings could not be compared.")
_SENTENCE_FITS_OWN = "This copy fits this release's own listing on every track."


def _own_listing_gaps(
    target_order: Sequence[tuple[int, int, MBDiscTrack]],
    file_lengths: Sequence[float],
) -> tuple[list[tuple[int, int]], list[tuple[int, int]]]:
    """(tracks whose file is more than 2s off the listing, tracks the
    release lists no length for)."""
    off: list[tuple[int, int]] = []
    unlisted: list[tuple[int, int]] = []
    for (medium, position, track), file_seconds in zip(
        target_order, file_lengths, strict=True,
    ):
        listed = _listed_seconds(track)
        if listed is None:
            unlisted.append((medium, position))
        elif abs(file_seconds - listed) > SAME_AUDIO_TOLERANCE_SECONDS:
            off.append((medium, position))
    return off, unlisted


def _own_listing_sentences(
    off: Sequence[tuple[int, int]], unlisted: Sequence[tuple[int, int]],
    *, multi_disc: bool,
) -> list[str]:
    if not off and not unlisted:
        return [_SENTENCE_FITS_OWN]
    sentences: list[str] = []
    if off:
        sentences.append(
            "This copy does not fit this release's own listing on "
            f"{_positions(off, multi_disc=multi_disc)}.")
    if unlisted:
        sentences.append(
            "This release lists no length for "
            f"{_positions(unlisted, multi_disc=multi_disc)}, so this copy "
            "cannot be checked against it in full.")
    return sentences


def check_other_pressings(
    download_log_id: int,
    *,
    pdb: DownloadLogSource,
    mirror: PressingMirror | None = None,
    clock: Callable[[], float] = time.monotonic,
) -> PressingCheck:
    """Run the check for one download_log row. Never raises for a mirror
    failure: every outcome is a typed result with sentences."""
    row = pdb.get_download_log_entry(download_log_id)
    if row is None:
        return PressingCheck(
            outcome=PressingCheckOutcome.DOWNLOAD_LOG_NOT_FOUND,
            download_log_id=download_log_id, sentences=[_SENTENCE_NO_ROW])
    attempt = _selected_attempt(row)
    if attempt is None:
        return PressingCheck(
            outcome=PressingCheckOutcome.NO_MATCHED_RELEASE,
            download_log_id=download_log_id,
            sentences=[_SENTENCE_NO_RELEASE])
    source_kind = detect_release_source(attempt.release_id)
    if source_kind == "discogs":
        return PressingCheck(
            outcome=PressingCheckOutcome.DISCOGS_RELEASE,
            download_log_id=download_log_id,
            target_release_id=attempt.release_id,
            sentences=[_SENTENCE_DISCOGS])
    if source_kind != "musicbrainz":
        return PressingCheck(
            outcome=PressingCheckOutcome.NO_MATCHED_RELEASE,
            download_log_id=download_log_id,
            sentences=[_SENTENCE_NO_RELEASE])

    source = mirror if mirror is not None else MusicBrainzPressingMirror()
    deadline = clock() + TOTAL_BUDGET_SECONDS

    def timeout() -> float:
        remaining = deadline - clock()
        if remaining <= 0:
            raise _OutOfTime
        return min(PER_REQUEST_TIMEOUT_SECONDS, remaining / 3)

    base = PressingCheck(
        outcome=PressingCheckOutcome.CHECKED,
        download_log_id=download_log_id,
        target_release_id=attempt.release_id,
        target_url=_url(attempt.release_id), sentences=[],
    )
    try:
        target = source.release(attempt.release_id, timeout=timeout())
    except urllib.error.HTTPError as error:
        # An HTTPError owns the response body; close it so nothing leaks.
        error.close()
        if error.code == 404:
            return msgspec.structs.replace(
                base, outcome=PressingCheckOutcome.RELEASE_NOT_FOUND,
                sentences=[_SENTENCE_GONE])
        return _unavailable(base, error)
    except (_OutOfTime, *_MIRROR_ERRORS) as error:
        return _unavailable(base, error)
    if not target.id or not target.media:
        return _unavailable(base, ValueError("empty release in the reply"))

    group_id = target.release_group.id if target.release_group else ""
    not_siblings = {attempt.release_id, normalize_release_id(target.id)}
    siblings: list[MBDiscRelease] = []
    seen: set[str] = set()
    in_group = 0
    read = 0
    if group_id:
        try:
            offset = 0
            while offset < RELEASE_GROUP_CAP:
                page = source.release_group_page(
                    group_id, offset=offset,
                    limit=RELEASE_GROUP_PAGE_SIZE, timeout=timeout())
                in_group = page.release_count
                if not page.releases:
                    break
                read += len(page.releases)
                for release in page.releases:
                    # Browse can list one release on two pages.
                    key = normalize_release_id(release.id)
                    if not key or key in seen:
                        continue
                    seen.add(key)
                    if key not in not_siblings:
                        siblings.append(release)
                offset += len(page.releases)
                if offset >= in_group:
                    break
        except (_OutOfTime, *_MIRROR_ERRORS) as error:
            return _unavailable(
                msgspec.structs.replace(base, release_group_id=group_id),
                error)

    target_order = _running_order(target)
    multi_disc = len(target.media) > 1
    lengths = _target_lengths(target, attempt.files)
    aligned = attempt.clean and len(attempt.files) == len(target_order) and all(
        (medium, position) in attempt.files
        for medium, position, _ in target_order)
    matches: list[MatchingPressing] = []
    same_listing: list[MatchingPressing] = []
    fits_own: bool | None = None
    sentences: list[str] = []
    if aligned:
        file_lengths = [
            attempt.files[(medium, position)]
            for medium, position, _ in target_order]
        fitting = [
            match for sibling in siblings
            if (match := _match(target_order, file_lengths, sibling))
            is not None
        ]
        off, unlisted = _own_listing_gaps(target_order, file_lengths)
        fits_own = True if not off and not unlisted else (
            False if off else None)
        sentences.extend(
            _own_listing_sentences(off, unlisted, multi_disc=multi_disc))
        if fits_own:
            # The copy fits its own release: a sibling the files also fit
            # is indistinguishable by length, never a finding.
            same_listing = fitting
            sentences.append(_same_listing_sentence(same_listing))
        else:
            matches = fitting
            sentences.extend(_match_sentences(matches, len(file_lengths)))
    else:
        sentences.append(_SENTENCE_NOT_ALIGNED)
    sentences.extend(_lengths_sentences(lengths))
    capped = in_group > read
    if capped:
        sentences.append(_cap_sentence(read, in_group))
    return msgspec.structs.replace(
        base, release_group_id=group_id or None,
        compared_with_pressings=aligned, fits_own_listing=fits_own,
        matching_pressings=matches, same_listing_pressings=same_listing,
        target_lengths=lengths, releases_in_group=in_group,
        releases_checked=len(siblings), capped=capped, sentences=sentences,
    )


def _unavailable(base: PressingCheck, error: BaseException) -> PressingCheck:
    detail = (
        "MusicBrainz took too long" if isinstance(error, _OutOfTime)
        else f"{type(error).__name__}: {error}"[:200]
    )
    return msgspec.structs.replace(
        base, outcome=PressingCheckOutcome.MIRROR_UNAVAILABLE,
        sentences=[_SENTENCE_UNREACHABLE], detail=detail)
