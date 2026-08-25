"""Discogs track-position parsing and tracklist normalization (issue #1261).

The single canonical parser for Discogs ``position`` strings and the
manifest normalization built on it. Every ``album_tracks`` persist path
for Discogs tracklists flows through here — ``web/discogs.py::get_release``
feeds the add/Replace/CLI persist callers (and, with the same normalized
shape, the read-only browse surfaces), while
``album_source.py::_populate_tracks_discogs`` (the search worker's
empty-manifest fallback) persists directly — and all of them must
produce the same rip-shaped rows, or the matcher count gate compares
candidates against a manifest no rip can equal.

Discogs encodes hidden-track runs as sub-positions of one physical track
(``10.1 Song / 10.2 (silence) / 10.3 Untitled``); a rip of the disc has
ONE file at position 10. ``normalize_release_tracks`` collapses each
sub-position group into a single row. Two deliberate divergences from
the Beets Discogs plugin: merged rows take the first real sub-title
alone (Beets joins every sub-title with ``" / "``) because that is what
rips name the file after, the better input for the matcher's
filename-similarity check and the search plan's track queries; and
``1A``/``1B`` number-then-letter positions read as side-suffix vinyl
(track 1 of sides A/B — one manifest row per entry, preserving the
count a rip has) where Beets reads the letter as a subtrack index and
may coalesce entries. Manifests here feed the count gate, so
count-preserving wins.
"""

import re
from collections.abc import Mapping, Sequence

SUB_POSITION_RE = re.compile(r"^(.+)\.(\d+)$")
# Discogs hidden-track placeholder titles: "(silence)", "[silence]",
# bare "silence". These never name an audio file in a rip.
SILENCE_TITLE_RE = re.compile(r"^[\[(]?\s*silence\s*[\])]?$", re.IGNORECASE)


def parse_duration(duration_str: str) -> float | None:
    """Parse a Discogs duration string (e.g. '4:44') to seconds."""
    if not duration_str:
        return None
    parts = duration_str.split(":")
    try:
        if len(parts) == 2:
            return int(parts[0]) * 60 + int(parts[1])
        if len(parts) == 3:
            return int(parts[0]) * 3600 + int(parts[1]) * 60 + int(parts[2])
    except ValueError:
        return None
    return None


def split_sub_position(position: str) -> tuple[str, int | None]:
    """Split sub-position notation: '10.2' → ('10', 2); '7' → ('7', None)."""
    m = SUB_POSITION_RE.match(position)
    if m:
        return m.group(1), int(m.group(2))
    return position, None


def parse_position(position: str) -> tuple[int, int]:
    """Parse a Discogs track position like '1', 'A1', '1-3' into (disc, track).

    Simple numeric: disc=1, track=N
    Letter prefix (vinyl): disc=ord(letter)-ord('A')+1, track from digits
    Bare side letter ('A'): disc from the letter, track 1
    Number-then-letter (vinyl, '1A'/'1B'): disc from the letter, track
    from the digits
    Disc-track (CD): split on '-'
    Sub-position ('10.2', 'A2.1'): parsed by the base ('10', 'A2');
    trailing-dot positions ('1.') parse by their digits
    Anything else: the (1, 0) unparseable sentinel
    """
    if not position:
        return 1, 0
    base, _sub = split_sub_position(position)
    base = base.rstrip(".")
    m = re.match(r"^(\d+)-(\d+)$", base)
    if m:
        return int(m.group(1)), int(m.group(2))
    m = re.match(r"^([A-Za-z])(\d*)$", base)
    if m:
        disc = ord(m.group(1).upper()) - ord("A") + 1
        return disc, int(m.group(2)) if m.group(2) else 1
    m = re.match(r"^(\d+)([A-Za-z])$", base)
    if m:
        disc = ord(m.group(2).upper()) - ord("A") + 1
        return disc, int(m.group(1))
    m = re.match(r"^(\d+)$", base)
    if m:
        return 1, int(m.group(1))
    return 1, 0


def _is_placeholder_title(title: str) -> bool:
    """Blank titles and '(silence)'-style markers never name a rip file."""
    stripped = title.strip()
    return not stripped or SILENCE_TITLE_RE.match(stripped) is not None


class _PositionGroup:
    """Accumulator for one physical position's sub-entry run.

    Grouping keys on the literal base string, never the parsed
    ``(disc, track)``: unparseable bases all parse to the (1, 0)
    sentinel, and keying on that would silently merge distinct physical
    tracks ('CD1.1' with 'CD2.1') into one manifest row.

    A flat 'parent' index row that joins the group (position equal to
    the sub base) is authoritative wherever it sits in the tracklist:
    its title outranks the sub-entries' (mirroring Beets'
    ``_coalesce_index_track``, which titles the whole physical track by
    the INDEX's OWN title), and its duration — when it carries one — IS
    the physical track's total, so it replaces the children's sum
    rather than adding to it.
    """

    def __init__(self, disc: int, track: int) -> None:
        self.disc = disc
        self.track = track
        self.titles: list[str] = []
        self.durations: list[float | None] = []
        self.parent_title: str | None = None
        self.parent_duration: float | None = None

    def add(self, title: str, duration: float | None) -> None:
        self.titles.append(title)
        self.durations.append(duration)

    def add_parent(self, title: str, duration: float | None) -> None:
        if self.parent_title is None:
            self.parent_title = title
            self.parent_duration = duration
            return
        # A second flat row on the same base is upstream data debris;
        # treat it as an ordinary member.
        self.add(title, duration)

    def row(self) -> dict[str, object]:
        candidates = list(self.titles)
        if self.parent_title is not None:
            candidates.insert(0, self.parent_title)
        real = [t for t in candidates if not _is_placeholder_title(t)]
        title = real[0] if real else (candidates[0] if candidates else "")
        if self.parent_duration is not None:
            length: float | None = self.parent_duration
        else:
            known = [d for d in self.durations if d is not None]
            length = sum(known) if known else None
        return {
            "disc_number": self.disc,
            "track_number": self.track,
            "title": title,
            "length_seconds": length,
        }


def normalize_release_tracks(
    raw_tracks: Sequence[Mapping[str, object]],
) -> list[dict[str, object]]:
    """Collapse flat sub-position notation into rip-shaped manifest rows.

    One row per physical position, in first-appearance order: flat
    positions pass through unchanged; a sub-position group merges into a
    single row titled by its first non-placeholder sub-entry with the
    known durations summed. A flat row whose position equals a
    sub-position base (a 'parent' index row like '10' alongside
    '10.1'/'10.2') joins that group as its authoritative parent (see
    ``_PositionGroup``) rather than duplicating its ``(disc, track)``.

    Heading rows — a literal empty position AND empty duration on a
    release that positions its other tracks — are Discogs' flattened
    side/disc/bonus section labels, not tracks: no rip has a file for
    them, so they are dropped. The predicate matches the measured mirror
    rule in ``lib/library_completeness.py`` (empty position alone, with
    a duration present, is ambiguous and is kept); a release that
    positions NOTHING has no heading signal and keeps every row, and a
    row carrying nested ``sub_tracks`` is an index parent, never a
    heading (the deployed mirror has not been observed emitting nested
    ``sub_tracks`` — that carve-out is fail-closed legislation, as in
    the sibling census module). Positions whose base parses to no known
    grammar share the ``(1, 0)`` sentinel; their rows survive with
    correct COUNT, but their relative order under ``album_tracks``'s
    ``(disc_number, track_number)`` read ordering is not defined. Raw
    consumers keep literal positions via
    ``web/discogs.py::get_release_raw``.
    """
    parsed: list[tuple[str, str, int | None, str, str, bool]] = []
    for track in raw_tracks:
        position = str(track.get("position", "") or "")
        title = str(track.get("title", "") or "")
        duration = str(track.get("duration", "") or "")
        has_children = bool(track.get("sub_tracks"))
        base, sub = split_sub_position(position)
        parsed.append((position, base, sub, title, duration, has_children))

    sub_bases = {base for _, base, sub, _, _, _ in parsed if sub is not None}
    any_positioned = any(position for position, _, _, _, _, _ in parsed)

    ordered: list[dict[str, object] | _PositionGroup] = []
    groups: dict[str, _PositionGroup] = {}
    for position, base, sub, title, duration, has_children in parsed:
        length = parse_duration(duration)
        if not position and not duration and any_positioned and not has_children:
            continue
        member_base = base if sub is not None else position
        if member_base in sub_bases:
            group = groups.get(member_base)
            if group is None:
                disc, track_num = parse_position(member_base)
                group = _PositionGroup(disc, track_num)
                groups[member_base] = group
                ordered.append(group)
            if sub is None:
                group.add_parent(title, length)
            else:
                group.add(title, length)
            continue
        disc, track_num = parse_position(position)
        ordered.append({
            "disc_number": disc,
            "track_number": track_num,
            "title": title,
            "length_seconds": length,
        })
    return [
        item.row() if isinstance(item, _PositionGroup) else item
        for item in ordered
    ]
