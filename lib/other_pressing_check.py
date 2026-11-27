"""Does this copy match another pressing? (issue #1445)

An ON-DEMAND, read-only check the operator runs for one download_log row.
It loads the selected candidate's mapping (the file length mapped to each
release track) and the target release id, asks the MusicBrainz mirror for
the target release and every other release in its release group, and
answers two questions in plain sentences:

- **Matching pressings.** Which sibling releases list a length for every
  track that every file matches within 2s, and where their track titles
  differ from the target's. A match proves the lengths fit that pressing's
  listing, nothing more: when the titles are the same on both, the two
  listings may describe the same audio differently.
- **How far to trust the listed lengths.** A disc ID on the target carries
  the physical table of contents, so per-track lengths come from a real
  disc and the files can be checked against them. Without one, the listed
  lengths were entered by hand.

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
from lib.release_identity import detect_release_source
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
#: One HTTP attempt never waits longer than this. The client retries a
#: dropped connection once, so each request gets at most half of what is
#: left of the budget.
PER_REQUEST_TIMEOUT_SECONDS: Final[float] = 10.0
#: CD frames per second in a disc table of contents.
_SECTORS_PER_SECOND: Final[float] = 75.0

#: Everything a mirror read can raise: connection and timeout failures
#: (``OSError``, which covers ``URLError``), a broken HTTP exchange, and an
#: undecodable or wrongly shaped body (``ValueError``, which covers
#: ``json.JSONDecodeError`` and ``msgspec.ValidationError``).
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
    """One target track measured off a disc table of contents."""

    medium: int
    position: int
    title: str
    disc_seconds: float
    #: ``None`` when no file is mapped to this track.
    file_seconds: float | None
    #: ``None`` when there is no file to compare.
    agrees: bool | None


class TargetLengths(msgspec.Struct, frozen=True, kw_only=True):
    has_disc_id: bool
    #: Disc IDs whose table of contents was used, one per covered medium.
    disc_ids: list[str]
    media_with_disc: list[int]
    media_without_disc: list[int]
    tracks: list[DiscTrack]
    #: ``None`` when no disc table of contents could be compared.
    files_agree: bool | None


class PressingCheck(msgspec.Struct, frozen=True, kw_only=True):
    outcome: PressingCheckOutcome
    download_log_id: int
    sentences: list[str]
    target_release_id: str | None = None
    target_url: str | None = None
    release_group_id: str | None = None
    #: False when the files do not line up one-to-one with the release.
    compared_with_pressings: bool = False
    matching_pressings: list[MatchingPressing] = msgspec.field(
        default_factory=list[MatchingPressing])
    target_lengths: TargetLengths | None = None
    #: Releases in the group as MusicBrainz counts them.
    releases_in_group: int = 0
    #: Other releases actually read (the target itself excluded).
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
        release_id = candidate.get("album_id")
        if not isinstance(release_id, str) or not release_id:
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

def _listed_seconds(milliseconds: int | None) -> float | None:
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
    folded = unicodedata.normalize("NFKC", title).casefold()
    for curly, plain in (("’", "'"), ("‘", "'"), ("“", '"'),
                         ("”", '"'), ("‐", "-")):
        folded = folded.replace(curly, plain)
    return " ".join(folded.split())


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


def _match(
    target_order: Sequence[tuple[int, int, MBDiscTrack]],
    file_lengths: Sequence[float],
    sibling: MBDiscRelease,
) -> MatchingPressing | None:
    sibling_order = _running_order(sibling)
    if len(sibling_order) != len(file_lengths):
        return None
    listed = [_listed_seconds(track.length) for _, _, track in sibling_order]
    worst = 0.0
    for file_seconds, sibling_seconds in zip(file_lengths, listed, strict=True):
        if sibling_seconds is None:
            return None
        gap = abs(file_seconds - sibling_seconds)
        if gap > SAME_AUDIO_TOLERANCE_SECONDS:
            return None
        worst = max(worst, gap)
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


def _target_lengths(
    target: MBDiscRelease, files: Mapping[tuple[int, int], float],
) -> TargetLengths:
    disc_ids: list[str] = []
    with_disc: list[int] = []
    without_disc: list[int] = []
    rows: list[DiscTrack] = []
    for medium in sorted(target.media, key=lambda m: m.position):
        chosen = _best_disc(medium, files)
        if chosen is None:
            without_disc.append(medium.position)
            continue
        disc, lengths = chosen
        disc_ids.append(disc.id)
        with_disc.append(medium.position)
        for index, (track, disc_seconds) in enumerate(
            zip(medium.tracks, lengths, strict=True), 1,
        ):
            position = track.position or index
            file_seconds = files.get((medium.position, position))
            rows.append(DiscTrack(
                medium=medium.position, position=position, title=track.title,
                disc_seconds=disc_seconds, file_seconds=file_seconds,
                agrees=(
                    None if file_seconds is None
                    else abs(file_seconds - disc_seconds)
                    <= SAME_AUDIO_TOLERANCE_SECONDS
                ),
            ))
    compared = [row.agrees for row in rows if row.agrees is not None]
    return TargetLengths(
        has_disc_id=any(medium.discs for medium in target.media),
        disc_ids=disc_ids, media_with_disc=with_disc,
        media_without_disc=without_disc, tracks=rows,
        files_agree=all(compared) if compared else None,
    )


def _best_disc(
    medium: MBDiscMedium, files: Mapping[tuple[int, int], float],
) -> tuple[MBDisc, list[float]] | None:
    """The disc ID on this medium whose table of contents is closest to
    the files (a medium can carry several pressings' disc IDs)."""
    best: tuple[float, MBDisc, list[float]] | None = None
    for disc in medium.discs:
        lengths = _disc_lengths(disc, len(medium.tracks))
        if lengths is None:
            continue
        gaps = [
            abs(files[(medium.position, track.position or index)] - seconds)
            for index, (track, seconds) in enumerate(
                zip(medium.tracks, lengths, strict=True), 1)
            if (medium.position, track.position or index) in files
        ]
        score = max(gaps, default=0.0)
        if best is None or score < best[0]:
            best = (score, disc, lengths)
    return None if best is None else (best[1], best[2])


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
#: Above this, a file is not the disc's track at all; at or below it (and
#: above the 2s tolerance) the difference is the size gap handling makes.
_FEW_SECONDS: Final[float] = 10.0
_COUNT_WORDS: Final[dict[int, str]] = {2: "Both", 3: "All three", 4: "All four"}
_HEDGE = (
    "This may be the same audio described differently (for example where a "
    "hidden track is counted), not necessarily a different disc."
)


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


def _pressing_sentences(
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


def _track_list(rows: Sequence[DiscTrack], *, multi_disc: bool) -> str:
    if not multi_disc:
        numbers = [str(row.position) for row in rows]
        noun = "tracks" if len(numbers) > 1 else "track"
        return f"{noun} {_join(numbers)}"
    parts: list[str] = []
    for medium in sorted({row.medium for row in rows}):
        numbers = [str(row.position) for row in rows if row.medium == medium]
        noun = "tracks" if len(numbers) > 1 else "track"
        parts.append(f"{noun} {_join(numbers)} on disc {medium}")
    return _join(parts)


def _lengths_sentence(lengths: TargetLengths, *, multi_disc: bool) -> str:
    if not lengths.media_with_disc:
        if lengths.has_disc_id:
            return (
                "This release has a disc ID, but its track layout does not "
                "fit the listing, so the listed lengths cannot be checked "
                "against it. They may have been typed in by hand."
            )
        return "The listed lengths were typed in by hand and may be wrong."
    if lengths.media_without_disc:
        discs = _join([str(m) for m in lengths.media_with_disc])
        noun = "discs" if len(lengths.media_with_disc) > 1 else "disc"
        source = (
            f"The listed lengths for {noun} {discs} come from a real disc, "
            "so they can be trusted; the others were typed in by hand and "
            "may be wrong."
        )
    else:
        source = (
            "The listed lengths come from a real disc, so they can be "
            "trusted."
        )
    if lengths.files_agree is None:
        return source
    if lengths.files_agree:
        return f"{source} This copy matches that disc on every track."
    off = [row for row in lengths.tracks if row.agrees is False]
    far = [row for row in off if _disc_gap(row) > _FEW_SECONDS]
    near = [row for row in off if _disc_gap(row) <= _FEW_SECONDS]
    gaps_note = (
        ", which can come from how the gaps between tracks were ripped")
    if far and near:
        return (
            f"{source} This copy does not match that disc on "
            f"{_track_list(far, multi_disc=multi_disc)}, and is a few seconds "
            f"off on {_track_list(near, multi_disc=multi_disc)}."
        )
    if far:
        return (
            f"{source} This copy does not match that disc on "
            f"{_track_list(far, multi_disc=multi_disc)}."
        )
    return (
        f"{source} This copy is a few seconds off that disc on "
        f"{_track_list(near, multi_disc=multi_disc)}{gaps_note}."
    )


def _disc_gap(row: DiscTrack) -> float:
    if row.file_seconds is None:
        return 0.0
    return abs(row.file_seconds - row.disc_seconds)


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
        return min(PER_REQUEST_TIMEOUT_SECONDS, remaining / 2)

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

    group_id = target.release_group.id if target.release_group else ""
    siblings: list[MBDiscRelease] = []
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
                siblings.extend(
                    release for release in page.releases
                    if release.id != target.id)
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
    sentences: list[str] = []
    if aligned:
        file_lengths = [
            attempt.files[(medium, position)]
            for medium, position, _ in target_order]
        matches = [
            match for sibling in siblings
            if (match := _match(target_order, file_lengths, sibling))
            is not None
        ]
        sentences.extend(_pressing_sentences(matches, len(file_lengths)))
    else:
        sentences.append(_SENTENCE_NOT_ALIGNED)
    sentences.append(_lengths_sentence(lengths, multi_disc=multi_disc))
    capped = in_group > read
    if capped:
        sentences.append(_cap_sentence(read, in_group))
    return msgspec.structs.replace(
        base, release_group_id=group_id or None,
        compared_with_pressings=aligned, matching_pressings=matches,
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

