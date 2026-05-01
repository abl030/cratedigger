"""Search query builder for Soulseek.

Builds search queries from artist + album title, applying transforms
to work around Soulseek's server-side search filtering.

Key insight: Soulseek bans certain artist names server-side (Beatles,
AFI, Kanye, etc.). Searches containing banned terms return 0 results.
Replacing the first character with * bypasses the filter:
  "Beatles" → "*eatles" (17786 results vs 0).

The wildcarded form is the default. It bypasses server-side bans but
is silently dropped by ~95% of peer clients on the network — many
older Soulseek/Nicotine+/museek+ clients don't index wildcarded
terms (live A/B for "the wiggles 1991": 241 hits un-wildcarded vs
14 hits wildcarded). The escalation ladder therefore retries the
un-wildcarded form once base/year cycles fail, before falling back
to per-track queries.

Pure functions — no I/O, no external dependencies.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Literal, TYPE_CHECKING

if TYPE_CHECKING:
    from lib.quality import CandidateScore


@dataclass
class SearchResult:
    """Thread-safe container for one album's search results.

    Returned by _execute_search() instead of writing to module globals.
    The main thread merges these into search_cache/user_upload_speed.
    """
    album_id: int
    success: bool
    # username -> filetype -> [dirs] (same shape as search_cache[album_id])
    cache_entries: dict[str, dict[str, list[str]]] = field(default_factory=dict)
    # username -> upload speed
    upload_speeds: dict[str, int] = field(default_factory=dict)
    # username -> dir -> audio file count (for pre-filtering before browse)
    dir_audio_counts: dict[str, dict[str, int]] = field(default_factory=dict)
    query: str = ""
    result_count: int | None = None
    elapsed_s: float = 0.0
    # found, no_match, no_results, timeout, error, empty_query, exhausted
    outcome: str = ""
    # U5 forensic capture (search-escalation-and-forensics):
    # - candidates: per-(user, dir, filetype) match scores collected from
    #   `find_download` → `_apply_find_download_result`. Persisted to
    #   `search_log.candidates` JSONB after top-20 truncation.
    # - variant_tag: "default" | "v1_year" | "v4_tracks_<idx>" | "exhausted",
    #   produced by `select_variant`. Persisted to `search_log.variant`.
    # - final_state: slskd's terminal state string ("Completed",
    #   "TimedOut", "ResponseLimitReached", "Errored"...). Persisted to
    #   `search_log.final_state`.
    candidates: tuple[CandidateScore, ...] = ()
    variant_tag: str | None = None
    final_state: str | None = None

# Soulseek's distributed search times out with too many tokens.
# 4 is the safe maximum.
MAX_SEARCH_TOKENS = 4


def strip_special_chars(text):
    """Remove punctuation that poisons Soulseek searches.

    Keeps only alphanumeric characters, hyphens, and whitespace.
    Everything else (commas, periods, colons, brackets, etc.)
    is replaced with a space.
    """
    clean = re.sub(r"[^\w\s-]", " ", text)
    # Also strip underscores (matched by \w but unwanted)
    clean = clean.replace("_", " ")
    return " ".join(clean.split())


def strip_short_tokens(tokens):
    """Remove tokens with <= 2 characters.

    Soulseek silently drops these server-side, so they waste a
    token slot without contributing to the search.
    e.g. "A Tribe Called Quest" → "Tribe Called Quest"
    """
    long = [t for t in tokens if len(t) > 2]
    return long if long else tokens  # keep originals if ALL are short


def wildcard_artist_tokens(artist_tokens):
    """Replace the first character of each artist token with *.

    Bypasses Soulseek's server-side artist name bans.
    e.g. ["Mountain", "Goats"] → ["*ountain", "*oats"]

    Tokens that are already too short to wildcard (<=1 char) are dropped.
    """
    result = []
    for t in artist_tokens:
        if len(t) > 1:
            result.append("*" + t[1:])
        # Drop single-char tokens — they'd become just "*" which matches everything
    return result


def cap_tokens(tokens, max_tokens=MAX_SEARCH_TOKENS):
    """Keep the most distinctive tokens, cap at max count.

    Drops the shortest (most common/ambiguous) tokens first,
    preserving original word order.
    """
    if len(tokens) <= max_tokens:
        return tokens

    # Sort by length descending, keep the longest
    kept = sorted(tokens, key=len, reverse=True)[:max_tokens]

    # Restore original order, handling duplicates
    seen = {}
    ordered = []
    for t in tokens:
        count = seen.get(t, 0)
        if count < kept.count(t):
            ordered.append(t)
            seen[t] = count + 1
        if len(ordered) >= max_tokens:
            break

    return ordered


def build_query(
    artist, title, prepend_artist=True, max_tokens=MAX_SEARCH_TOKENS,
    wildcard_artist=True,
):
    """Build a Soulseek search query from artist + album title.

    Returns the final query string.

    Pipeline:
      1. Clean punctuation from both artist and title
      2. Tokenize separately
      3. Strip short tokens (<=2 chars)
      4. Wildcard artist tokens (bypass bans) — when wildcard_artist=True
      5. Combine and cap total token count

    With ``wildcard_artist=False`` artist tokens are still prepended (when
    ``prepend_artist=True``) but kept literal. Used by the un-wildcarded
    escalation tier where peers that drop wildcarded queries are the goal.
    """
    # Clean punctuation
    clean_artist = strip_special_chars(artist)
    clean_title = strip_special_chars(title)

    # Tokenize
    artist_tokens = clean_artist.split()
    title_tokens = clean_title.split()

    # Strip short tokens from each
    artist_tokens = strip_short_tokens(artist_tokens)
    title_tokens = strip_short_tokens(title_tokens)

    # Drop title tokens that duplicate artist tokens (case-insensitive).
    # e.g. "The Castiles - The Castiles Live" → artist has "Castiles",
    # title has "Castiles" + "Live" → drop duplicate, keep "Live".
    # This avoids wasting token slots and leaking un-wildcarded artist names.
    artist_lower = {t.lower() for t in artist_tokens}
    title_tokens = [t for t in title_tokens if t.lower() not in artist_lower]

    # Drop artist entirely if it's "Various Artists" — adds nothing to search,
    # and the wildcarded version (*arious *rtists) actively poisons results.
    if clean_artist.lower() in ("various artists", "various"):
        artist_tokens = []

    if wildcard_artist:
        artist_tokens = wildcard_artist_tokens(artist_tokens)

    if prepend_artist and artist_tokens:
        all_tokens = artist_tokens + title_tokens
    else:
        all_tokens = title_tokens

    if not all_tokens:
        return None

    # Cap total tokens
    all_tokens = cap_tokens(all_tokens, max_tokens)

    return " ".join(all_tokens)


# ---------------------------------------------------------------------------
# Variant generator
#
# Pure function: given the current cycle counter, escalation threshold, both
# base queries (wildcarded + un-wildcarded), year, and track titles, return
# which query to issue next. Single source of truth for the search-cycle
# ladder. No I/O.
#
# Ladder:
#   cycle < threshold        → kind="default",     query=base_query (wildcarded)
#   cycle == threshold       → kind="unwild",      query=base_query_unwild
#   cycle == threshold + 1   → kind="unwild_year", query="<unwild> <yyyy>" (if year known)
#   cycle == threshold + N   → kind="track",       query=<one track title>, idx N
#   queue drained            → kind="exhausted",   query=None (search loop short-circuits)
#
# Year is treated as unknown when None or starts with "0000" (the AlbumRecord
# fallback string when MusicBrainz has no year). When unknown, the
# unwild_year tier is skipped and the per-track tier starts one cycle earlier.
#
# Single-track albums skip the per-track tier entirely (lone-track-title
# queries match unrelated albums too easily for the 0.15 distance gate).
# ---------------------------------------------------------------------------


@dataclass
class SearchVariant:
    """One cycle's variant decision.

    Internal type — never crosses JSON. The `tag` field is the persisted
    label written to `search_log.variant`. `kind` drives loop behaviour:
    "exhausted" tells the search loop to short-circuit before hitting slskd.
    """
    kind: Literal["default", "unwild", "unwild_year", "track", "exhausted"]
    query: str | None  # None for kind="exhausted"
    tag: str           # "default" | "unwild" | "unwild_year" | "track_<idx>" | "exhausted"
    slice_index: int | None  # track tier only, for diagnostics


def _per_track_queries(track_titles: list[str]) -> list[str]:
    """Build the per-track query list.

    Each track title is independently cleaned and tokenised by the same
    pipeline that ``build_query`` uses for album titles: strip punctuation,
    drop short tokens (<=2 chars, with the standard fallback when *all*
    tokens are short), then cap to ``MAX_SEARCH_TOKENS``. The result is a
    list of ready-to-issue Soulseek query strings, one per track, in
    source-tracklist order.

    Cleaning rules:
      - Empty queries (titles that clean to nothing alpha) are skipped.
      - Identical tokenised queries are deduplicated case-insensitively
        so duplicate tracklist entries (e.g. two ``Archie's Theme`` tracks
        on the Wiggles 1991 album) don't burn two cycles on the same
        query.

    No artist context is added. The album-match step (sub-count gate +
    filename ratio + cross-check) handles disambiguation after slskd
    returns peers, so the per-track query optimises for recall.
    """
    seen_lower: set[str] = set()
    queries: list[str] = []
    for title in track_titles:
        cleaned = strip_special_chars(title)
        tokens = cleaned.split()
        tokens = strip_short_tokens(tokens)
        if not tokens:
            continue
        tokens = cap_tokens(tokens)
        query = " ".join(tokens)
        if not query:
            continue
        key = query.lower()
        if key in seen_lower:
            continue
        seen_lower.add(key)
        queries.append(query)
    return queries


def _year_is_known(year: str | None) -> bool:
    """Year is known iff the first 4 chars are all digits and not "0000".

    MB sometimes returns a fallback "0000" year, an empty string, whitespace,
    or non-numeric placeholders ("unknown"). Treat all of those as unknown
    so the unwild_year cycle does not append a meaningless year token to the
    slskd query (degrading match recall). Also rejects shorter-than-4-char
    numeric prefixes ("199") because they are clearly malformed.
    """
    return (
        year is not None
        and len(year) >= 4
        and year[:4].isdigit()
        and year[:4] != "0000"
    )


def select_variant(
    search_attempts: int,
    threshold: int,
    base_query: str,
    base_query_unwild: str,
    year: str | None,
    track_titles: list[str],
) -> SearchVariant:
    """Select the variant for this search cycle.

    Deterministic for a given input — testable via subTest tables.

    `search_attempts` is how many search cycles this album has already
    consumed (0 on first attempt). `threshold` is the count at which
    escalation begins. Below the threshold the wildcarded default query
    repeats; at and above, the ladder advances by one step per cycle:

      threshold     → unwild      (un-wildcarded base)
      threshold+1   → unwild_year (un-wildcarded base + year, if known)
      threshold+N   → track_<i>   (one bare track title per cycle)
      drained       → exhausted
    """
    if search_attempts < threshold:
        return SearchVariant(
            kind="default",
            query=base_query,
            tag="default",
            slice_index=None,
        )

    esc_idx = search_attempts - threshold

    if esc_idx == 0:
        return SearchVariant(
            kind="unwild",
            query=base_query_unwild,
            tag="unwild",
            slice_index=None,
        )

    year_known = _year_is_known(year)

    if esc_idx == 1 and year_known:
        # year_known guarantees year is not None; year[:4] yields the 4-char
        # year prefix (e.g. "1991" from "1991" or "1991-08-01").
        assert year is not None  # for type checker
        return SearchVariant(
            kind="unwild_year",
            query=f"{base_query_unwild} {year[:4]}",
            tag="unwild_year",
            slice_index=None,
        )

    # Skip the per-track tier entirely for single-track albums. A lone track
    # title produces slskd matches that pass the 0.15 distance gate too
    # easily — the query collapses to "single song" tokens and unrelated
    # albums on Soulseek that happen to share that song name slip through.
    if len(track_titles) <= 1:
        return SearchVariant(
            kind="exhausted",
            query=None,
            tag="exhausted",
            slice_index=None,
        )

    track_start_offset = 2 if year_known else 1
    track_idx = esc_idx - track_start_offset

    queries = _per_track_queries(track_titles)
    if track_idx < 0 or track_idx >= len(queries):
        return SearchVariant(
            kind="exhausted",
            query=None,
            tag="exhausted",
            slice_index=None,
        )

    return SearchVariant(
        kind="track",
        query=queries[track_idx],
        tag=f"track_{track_idx}",
        slice_index=track_idx,
    )
