"""Search query builder for Soulseek.

Builds search queries from artist + album title, applying transforms
to work around Soulseek's server-side search filtering.

Key insight: Soulseek bans certain artist names server-side (Beatles,
AFI, Kanye, etc.). Searches containing banned terms return 0 results.
Replacing the first character with * bypasses the filter:
  "Beatles" → "*eatles" (17786 results vs 0).

We wildcard ALL artist tokens unconditionally — there's no downside
(*ountain matches Mountain) and it avoids needing to maintain a
banned word list.

Pure functions — no I/O, no external dependencies.
"""

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
    candidates: tuple["CandidateScore", ...] = ()
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


def build_query(artist, title, prepend_artist=True, max_tokens=MAX_SEARCH_TOKENS):
    """Build a Soulseek search query from artist + album title.

    Returns the final query string.

    Pipeline:
      1. Clean punctuation from both artist and title
      2. Tokenize separately
      3. Strip short tokens (<=2 chars)
      4. Wildcard artist tokens (bypass bans)
      5. Combine and cap total token count

    Artist tokens are always prepended and wildcarded.
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

    # Wildcard artist tokens
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
# Variant generator (U4 of search-escalation-and-forensics)
#
# Pure function: given the current cycle counter, escalation threshold, base
# query, year, and track titles, return which query to issue next. Single
# source of truth for the search-cycle ladder. No I/O.
#
# Ladder:
#   cycle < threshold        → kind="default",   query=base_query
#   cycle == threshold       → kind="v1_year",   query="<base> <yyyy>" (if year known)
#   cycle == threshold + N   → kind="v4_tracks", query=N×3 distinctive tokens
#   pool exhausted           → kind="exhausted", query=None (search loop short-circuits)
#
# Year is treated as unknown when None or starts with "0000" (the AlbumRecord
# fallback string when MusicBrainz has no year). When unknown, V1 is skipped
# and V4 starts at the threshold cycle.
# ---------------------------------------------------------------------------


@dataclass
class SearchVariant:
    """One cycle's variant decision.

    Internal type — never crosses JSON. The `tag` field is the persisted
    label written to `search_log.variant`. `kind` drives loop behaviour:
    "exhausted" tells the search loop to short-circuit before hitting slskd.
    """
    kind: Literal["default", "v1_year", "v4_tracks", "exhausted"]
    query: str | None  # None for kind="exhausted"
    tag: str           # "default" | "v1_year" | "v4_tracks_<idx>" | "exhausted"
    slice_index: int | None  # V4 only, for diagnostics


def _distinctive_token_pool(track_titles: list[str]) -> list[str]:
    """Build the V4 token pool from track titles.

    Rules:
      - Strip punctuation via `strip_special_chars` (already used by the query
        builder so the pool matches what slskd will tolerate).
      - Drop tokens of length <= 2 (mirrors `strip_short_tokens` behaviour).
      - Dedupe case-insensitively, preserving first-seen casing.
      - Sort by length descending; alphabetical-lowercase secondary order
        for determinism on length ties.
    """
    seen_lower: set[str] = set()
    distinct: list[str] = []
    for title in track_titles:
        cleaned = strip_special_chars(title)
        for token in cleaned.split():
            if len(token) <= 2:
                continue
            lower = token.lower()
            if lower in seen_lower:
                continue
            seen_lower.add(lower)
            distinct.append(token)

    # Sort by (-length, lowercase) for deterministic ordering on length ties.
    distinct.sort(key=lambda t: (-len(t), t.lower()))
    return distinct


def _year_is_known(year: str | None) -> bool:
    """Year is unknown when None or starts with the MB-fallback "0000"."""
    if year is None:
        return False
    if year.startswith("0000"):
        return False
    return True


def select_variant(
    search_attempts: int,
    threshold: int,
    base_query: str,
    year: str | None,
    track_titles: list[str],
) -> SearchVariant:
    """Select the variant for this search cycle.

    Deterministic for a given input — testable via subTest tables.

    `search_attempts` is how many search cycles this album has already
    consumed (0 on first attempt). `threshold` is the count at which
    escalation begins. Below the threshold the default query repeats; at
    and above, the ladder advances by one step per cycle.
    """
    if search_attempts < threshold:
        return SearchVariant(
            kind="default",
            query=base_query,
            tag="default",
            slice_index=None,
        )

    year_known = _year_is_known(year)
    esc_idx = search_attempts - threshold

    if esc_idx == 0 and year_known:
        # year_known guarantees year is not None; year[:4] yields the 4-char
        # year prefix (e.g. "1991" from "1991" or "1991-08-01").
        assert year is not None  # for type checker
        return SearchVariant(
            kind="v1_year",
            query=f"{base_query} {year[:4]}",
            tag="v1_year",
            slice_index=None,
        )

    # V1 either ran (esc_idx 0 with year) or was skipped (year unknown).
    v4_start = 1 if year_known else 0
    v4_idx = esc_idx - v4_start

    pool = _distinctive_token_pool(track_titles)
    slice_start = v4_idx * 3
    if slice_start >= len(pool):
        return SearchVariant(
            kind="exhausted",
            query=None,
            tag="exhausted",
            slice_index=None,
        )

    slice_tokens = pool[slice_start : slice_start + 3]
    return SearchVariant(
        kind="v4_tracks",
        query=" ".join(slice_tokens),
        tag=f"v4_tracks_{v4_idx}",
        slice_index=v4_idx,
    )
