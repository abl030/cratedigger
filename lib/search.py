"""Search query builder for Soulseek.

Builds search queries from artist + album title, applying transforms
to work around Soulseek's server-side search filtering.

Key insight: Soulseek bans certain artist names server-side (Beatles,
AFI, Kanye, etc.). Searches containing banned terms return 0 results.
Replacing the first character with * bypasses the filter:
  "Beatles" → "*eatles" (17786 results vs 0).

The wildcarded form is the default. It bypasses server-side bans but
many older Soulseek/Nicotine+/museek+ clients don't index wildcarded
terms, so the escalation ladder retries the un-wildcarded form once
base/year cycles fail, before falling back to per-track queries.

Pure functions — no I/O, no external dependencies.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Literal, TYPE_CHECKING

if TYPE_CHECKING:
    from lib.quality import CandidateScore


# ---------------------------------------------------------------------------
# Persisted search-plan generator id.
#
# This constant is the ONLY automatic invalidation key for persisted plans
# (per docs/plans/2026-05-08-001-feat-persisted-search-plans-plan.md).
# Any change that affects generator output — token rules, strategy ladder,
# repeat-group identity, dedupe behavior, provenance shape — MUST bump this
# string. U3 (service) and U4 (reconciliation) read this to decide whether
# a stored plan is current.
#
# Format is free-form, but keep it short and stable. Use a date+seq tag.
# ---------------------------------------------------------------------------
SEARCH_PLAN_GENERATOR_ID = "search-plan/2026-05-08-1"


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
    # Per-search browse/match cost copied from FindDownloadMetrics. These are
    # persisted on search_log so the dashboard can identify the exact query
    # tokens that produce the largest peer/dir fan-out.
    browse_time_s: float = 0.0
    match_time_s: float = 0.0
    peers_browsed: int = 0
    peers_browsed_lazy: int = 0
    fanout_waves: int = 0
    # True when `_collect_search_results` cancelled the search via slskd's
    # PUT /api/v0/searches/{id} after 90s of no new peer responses (issue
    # #212). Diagnostic only — outcome classification still reflects what
    # was harvested, not the watchdog.
    watchdog_fired: bool = False

# Soulseek's distributed search times out with too many tokens.
# 4 is the safe maximum.
MAX_SEARCH_TOKENS = 4
# Empirical from search_log peer-dir fanout: keep this list narrow and add
# only words that repeatedly dominate expensive broad searches.
LOW_ENTROPY_QUERY_TOKENS = {"the", "you", "from", "and"}

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


def _normalize_query_tokens(
    tokens: list[str],
    *,
    preserve_all_low_entropy: bool = False,
) -> list[str]:
    """Drop low-entropy tokens and case-insensitive repeats."""
    normalized: list[str] = []
    seen: set[str] = set()
    for token in tokens:
        key = token.lower()
        if key in LOW_ENTROPY_QUERY_TOKENS:
            continue
        if key in seen:
            continue
        seen.add(key)
        normalized.append(token)

    if normalized or not preserve_all_low_entropy:
        return normalized

    # Artist names that are only low-entropy words ("The The", "You You")
    # are still real artist identity. Preserve one case-insensitive copy
    # rather than erasing the artist side of the query entirely.
    fallback: list[str] = []
    seen.clear()
    for token in tokens:
        key = token.lower()
        if key in seen:
            continue
        seen.add(key)
        fallback.append(token)
    return fallback


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


def _longest_artist_token(
    artist_name: str,
    excluded_tokens: set[str] | None = None,
) -> str | None:
    """Return the longest useful literal artist token for track queries.

    Ties keep source order, so "Dallas Crane" contributes "Dallas". Short
    artist tokens are ignored because Soulseek drops <=2-char tokens anyway.
    """
    cleaned = strip_special_chars(artist_name)
    if cleaned.lower() in ("various artists", "various"):
        return None

    excluded = excluded_tokens or set()
    tokens = _normalize_query_tokens(
        [t for t in cleaned.split() if len(t) > 2],
        preserve_all_low_entropy=True,
    )
    candidates = [t for t in tokens if t.lower() not in excluded]
    if candidates:
        return max(candidates, key=len)
    # If every useful artist token is the current title token, do not append
    # it back and produce duplicates like "Fall Fall".
    if tokens and not excluded:
        return max(tokens, key=len)
    return None


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

    # Drop very low-entropy search terms ("the", "you") and repeated tokens
    # before wildcarding/capping so they do not consume Soulseek query slots.
    artist_tokens = _normalize_query_tokens(
        artist_tokens,
        preserve_all_low_entropy=True,
    )
    title_tokens = _normalize_query_tokens(title_tokens)

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
# base queries (wildcarded + un-wildcarded), year, track titles, and artist
# name, return which query to issue next. Single source of truth for the
# search-cycle ladder. No I/O.
#
# Ladder:
#   cycle < threshold        → kind="default",     query=base_query (wildcarded)
#   cycle == threshold       → kind="unwild",      query=base_query_unwild
#   cycle == threshold + 1   → kind="unwild_year", query="<unwild> <yyyy>" (if year known)
#   cycle == threshold + N   → kind="track",       query=<one track query>, idx N
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


def _per_track_queries(
    track_titles: list[str],
    artist_name: str = "",
) -> list[str]:
    """Build the per-track query list.

    Each track title is independently cleaned and tokenised by the same
    pipeline that ``build_query`` uses for album titles: strip punctuation,
    drop short tokens (<=2 chars, with the standard fallback when *all*
    tokens are short), then cap to ``MAX_SEARCH_TOKENS``. The result is a
    list of ready-to-issue Soulseek query strings, one per track, in
    source-tracklist order. Single-token track queries get the longest
    distinct cleaned artist token appended for extra entropy, e.g.
    "Sweet Dallas", or are skipped when no such artist token exists.

    Cleaning rules:
      - Empty queries (titles that clean to nothing alpha) are skipped.
      - One-token queries are enriched with the longest distinct artist token
        when available; otherwise they are skipped as too broad.
      - Identical tokenised queries are deduplicated case-insensitively
        so duplicate tracklist entries (e.g. two ``Archie's Theme`` tracks
        on the Wiggles 1991 album) don't burn two cycles on the same
        query.

    No wildcards are added. The album-match step (sub-count gate + filename
    ratio + cross-check) handles disambiguation after slskd returns peers,
    so the per-track query optimises for recall while avoiding ultra-broad
    bare one-word searches.
    """
    seen_lower: set[str] = set()
    queries: list[str] = []
    for title in track_titles:
        cleaned = strip_special_chars(title)
        tokens = cleaned.split()
        tokens = strip_short_tokens(tokens)
        tokens = _normalize_query_tokens(tokens)
        if not tokens:
            continue
        tokens = cap_tokens(tokens)
        if len(tokens) == 1:
            artist_token = _longest_artist_token(
                artist_name,
                excluded_tokens={tokens[0].lower()},
            )
            if artist_token:
                tokens = tokens + [artist_token]
            else:
                continue
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
    artist_name: str = "",
) -> SearchVariant:
    """Select the variant for this search cycle.

    Deterministic for a given input — testable via subTest tables.

    `search_attempts` is how many search cycles this album has already
    consumed (0 on first attempt). `threshold` is the count at which
    escalation begins. Below the threshold the wildcarded default query
    repeats; at and above, the ladder advances by one step per cycle:

      threshold     → unwild      (un-wildcarded base)
      threshold+1   → unwild_year (un-wildcarded base + year, if known)
      threshold+N   → track_<i>   (one track query per cycle)
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

    queries = _per_track_queries(track_titles, artist_name=artist_name)
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


# ---------------------------------------------------------------------------
# Pure search-plan generator (U2 — persisted search plans plan).
#
# Replaces cycle-index variant selection with a deterministic, materialized
# plan list. Same album-level behavior as `select_variant()`, plus:
#   - bounded provenance (omitted candidates with reasons, dedupe losers)
#   - canonical query keys for usefulness aggregation
#   - repeat-group identity for the intentional repeated-default slots
#   - explicit deterministic generation-failure result (not an empty plan)
#
# `select_variant()` and `build_query()` remain the runtime executor's
# entry point until U5 cuts over. This generator is additive.
# ---------------------------------------------------------------------------


# Strategy labels used on plan items. Aligned with current `select_variant()`
# tags so search-log forensics stay readable across the cutover.
_STRATEGY_DEFAULT = "default"
_STRATEGY_UNWILD = "unwild"
_STRATEGY_UNWILD_YEAR = "unwild_year"


# Maximum number of per-track slots in a generated plan. Mirrors the
# "Up to 3 track slots" requirement from §U2.
MAX_TRACK_SLOTS_PER_PLAN = 3


# Plan-status values. Kept as string literals so they round-trip cleanly
# through JSONB at the persistence layer (U1/U3).
PLAN_STATUS_SUCCESS = "success"
PLAN_STATUS_GENERATION_FAILED = "generation_failed"


@dataclass(frozen=True)
class ReleaseSnapshot:
    """Pure input value to `generate_search_plan`.

    A snapshot of release metadata at generation time. Resolver/DB I/O happens
    in U3 — the generator only reads this value. `redownload` is exposed as a
    snapshot field so callers can record it; it does not affect generator
    behavior in this unit (album-level slot ladder is the same for both
    request and redownload sources), but it is part of the snapshot contract
    and must be preserved into provenance for debuggability.

    `prepend_artist` is a generation-affecting config knob — moving it onto
    the snapshot keeps the function pure (no implicit module/global config
    reads).
    """

    artist_name: str
    title: str
    year: str | None
    track_titles: tuple[str, ...]
    redownload: bool = False
    prepend_artist: bool = False


@dataclass(frozen=True)
class SearchPlanConfig:
    """Generation-affecting config for `generate_search_plan`.

    Bumping any field's effective semantics requires bumping
    `SEARCH_PLAN_GENERATOR_ID`.
    """

    escalation_threshold: int = 5
    max_track_slots: int = MAX_TRACK_SLOTS_PER_PLAN


@dataclass(frozen=True)
class SearchPlanItem:
    """One ordered plan slot.

    Pure-Python value type. Persistence (U1) will translate this into a row
    on `search_plan_items`; the executor (U5) will read these in order.

    `strategy` is one of `default` | `unwild` | `unwild_year` | `track_<idx>`.
    `canonical_query_key` is the lowercased / whitespace-collapsed query and
    is the aggregation key for usefulness stats. `repeat_group` marks slots
    that share intentional repeat identity — repeated-default slots all share
    the same repeat_group, while non-default slots have their strategy as
    their repeat_group.
    """

    ordinal: int
    strategy: str
    query: str
    canonical_query_key: str
    repeat_group: str
    # Per-item provenance (e.g. source_track_index for track slots, repeat
    # ordinal within the repeat group for default slots). Bounded — small
    # JSON-serialisable dict.
    provenance: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class SearchPlan:
    """Materialized plan output of `generate_search_plan`.

    On success, `items` is a non-empty ordered list. On deterministic
    generation failure (no runnable artist/title/track query), `status` is
    `generation_failed` and `items` is empty; `failure_reason` is set and
    `provenance` carries the input snapshot signature.
    """

    generator_id: str
    status: str  # PLAN_STATUS_SUCCESS | PLAN_STATUS_GENERATION_FAILED
    items: tuple[SearchPlanItem, ...]
    # Plan-level provenance:
    #   - `omitted_candidates`: list of {strategy, reason, ...} entries for
    #     candidates that were generated but not emitted as runnable items
    #     (e.g. unwild_year skipped because year unknown, track tier skipped
    #     because single-track album, track candidates beyond max_track_slots).
    #   - `dedupe_losers`: list of {winner_strategy, loser_strategy,
    #     canonical_query_key, would_have_been_ordinal} for cross-strategy
    #     canonical-key collisions resolved in favor of the earlier slot.
    #   - `dropped_low_entropy_tokens`: case-insensitive set of low-entropy
    #     tokens that were dropped from any candidate, recorded once at the
    #     plan level so callers can audit `the/you/from/and` removal without
    #     digging into per-item provenance.
    #   - `snapshot_signature`: small subset of input (artist, title, year,
    #     track_count, redownload) so failed plans remain debuggable.
    provenance: dict[str, Any] = field(default_factory=dict)
    failure_reason: str | None = None


# Candidates the generator considered before deciding which became runnable
# items. Internal scratch type; never serialised.
@dataclass
class _Candidate:
    strategy: str
    repeat_group: str
    query: str | None  # None when generation produced no runnable query
    omit_reason: str | None  # None when query is runnable
    extra_provenance: dict[str, Any] = field(default_factory=dict)


def _canonical_query_key(query: str) -> str:
    """Canonical key for usefulness aggregation and dedupe.

    Lowercase + whitespace-collapsed. Keep this deliberately simple — any
    change to canonicalisation must bump `SEARCH_PLAN_GENERATOR_ID`.
    """
    return " ".join(query.lower().split())


def _has_dropped_low_entropy(*token_lists: list[str]) -> set[str]:
    """Return the lowercased low-entropy tokens that appear in any source list.

    Used purely for plan-level provenance: we want to record that the
    generator dropped (e.g.) `the` from a candidate, even though the
    drop happens inside `_normalize_query_tokens`. The presence check is
    case-insensitive.
    """
    dropped: set[str] = set()
    for tokens in token_lists:
        for tok in tokens:
            key = tok.lower()
            if key in LOW_ENTROPY_QUERY_TOKENS:
                dropped.add(key)
    return dropped


def _per_track_candidates(
    track_titles: list[str],
    artist_name: str,
) -> list[tuple[int, str]]:
    """Return runnable per-track queries paired with source-track index.

    Mirrors `_per_track_queries()` semantics — same cleaning, same enrichment,
    same low-entropy drops, same case-insensitive dedupe — but preserves the
    source-track index so the generator can rank stably by token count then
    char count then original track order.
    """
    seen_lower: set[str] = set()
    out: list[tuple[int, str]] = []
    for src_idx, title in enumerate(track_titles):
        cleaned = strip_special_chars(title)
        tokens = cleaned.split()
        tokens = strip_short_tokens(tokens)
        tokens = _normalize_query_tokens(tokens)
        if not tokens:
            continue
        tokens = cap_tokens(tokens)
        if len(tokens) == 1:
            artist_token = _longest_artist_token(
                artist_name,
                excluded_tokens={tokens[0].lower()},
            )
            if artist_token:
                tokens = tokens + [artist_token]
            else:
                continue
        query = " ".join(tokens)
        if not query:
            continue
        key = query.lower()
        if key in seen_lower:
            continue
        seen_lower.add(key)
        out.append((src_idx, query))
    return out


def generate_search_plan(
    snapshot: ReleaseSnapshot,
    config: SearchPlanConfig,
) -> SearchPlan:
    """Generate the deterministic search plan for one release.

    Pure function: same input → same output. No I/O. The result is the
    materialized executor schedule for the release, in slot order.

    Slot ordering:
      1. `escalation_threshold` repeated `default` slots (intentional repeats,
         shared `repeat_group`, distinct ordinals).
      2. One `unwild` slot.
      3. One `unwild_year` slot ONLY when year is known
         (per `_year_is_known()`).
      4. Up to `max_track_slots` track slots (default 3) drawn from the
         per-track candidates ranked by:
           a. useful-token count desc
           b. character count desc
           c. source-track index asc
         Multi-track albums only — single-track and zero-track inputs skip
         the track tier entirely.

    Cross-strategy dedupe: when a non-default candidate produces the same
    canonical query as an earlier slot, the earlier one wins and the loser
    is recorded in plan-level provenance with the ordinal it would have had.
    Repeated-default slots are NOT deduped against each other — they share
    intentional repeat-group identity.

    Generation failure: when artist/title is unrunnable AND no per-track
    candidate is runnable, returns `PLAN_STATUS_GENERATION_FAILED` with
    populated provenance, NOT an empty success plan.
    """
    artist = snapshot.artist_name
    title = snapshot.title
    year = snapshot.year
    track_titles = list(snapshot.track_titles)
    prepend_artist = snapshot.prepend_artist

    # --- Build the candidate ladder up front ----------------------------
    # We construct candidates for every potential slot, runnable or not, so
    # provenance can carry the omission reason. Collapse to runnable items
    # at the end.

    # Track which low-entropy tokens were ever present in raw inputs, so the
    # plan can record the drop set even though the drop happens inside
    # _normalize_query_tokens. We tokenise on whitespace post-strip-special
    # to mirror the helper pipeline.
    all_input_tokens: list[list[str]] = []
    all_input_tokens.append(strip_special_chars(artist).split())
    all_input_tokens.append(strip_special_chars(title).split())
    for t in track_titles:
        all_input_tokens.append(strip_special_chars(t).split())
    dropped_low_entropy = _has_dropped_low_entropy(*all_input_tokens)

    base_query = build_query(artist, title, prepend_artist=prepend_artist)
    base_query_unwild = build_query(
        artist, title, prepend_artist=prepend_artist, wildcard_artist=False,
    )

    candidates: list[_Candidate] = []

    # 1. Repeated default slots
    threshold = max(0, config.escalation_threshold)
    for repeat_idx in range(threshold):
        if base_query:
            candidates.append(_Candidate(
                strategy=_STRATEGY_DEFAULT,
                repeat_group=_STRATEGY_DEFAULT,
                query=base_query,
                omit_reason=None,
                extra_provenance={"repeat_index": repeat_idx},
            ))
        else:
            candidates.append(_Candidate(
                strategy=_STRATEGY_DEFAULT,
                repeat_group=_STRATEGY_DEFAULT,
                query=None,
                omit_reason="empty_base_query",
                extra_provenance={"repeat_index": repeat_idx},
            ))

    # 2. Unwild slot
    if base_query_unwild:
        candidates.append(_Candidate(
            strategy=_STRATEGY_UNWILD,
            repeat_group=_STRATEGY_UNWILD,
            query=base_query_unwild,
            omit_reason=None,
        ))
    else:
        candidates.append(_Candidate(
            strategy=_STRATEGY_UNWILD,
            repeat_group=_STRATEGY_UNWILD,
            query=None,
            omit_reason="empty_unwild_query",
        ))

    # 3. Unwild_year slot — only when year is known
    year_known = _year_is_known(year)
    if year_known and base_query_unwild:
        # year_known guarantees year is not None and 4-digit-prefixed.
        assert year is not None  # type checker
        candidates.append(_Candidate(
            strategy=_STRATEGY_UNWILD_YEAR,
            repeat_group=_STRATEGY_UNWILD_YEAR,
            query=f"{base_query_unwild} {year[:4]}",
            omit_reason=None,
            extra_provenance={"year": year[:4]},
        ))
    else:
        reason = "year_unknown" if not year_known else "empty_unwild_query"
        candidates.append(_Candidate(
            strategy=_STRATEGY_UNWILD_YEAR,
            repeat_group=_STRATEGY_UNWILD_YEAR,
            query=None,
            omit_reason=reason,
        ))

    # 4. Track candidates — multi-track albums only
    per_track_pairs = _per_track_candidates(track_titles, artist)
    track_omissions: list[dict[str, Any]] = []

    if len(track_titles) <= 1:
        # Don't emit any track slots; record the structural skip once.
        if track_titles:
            track_omissions.append({
                "strategy": "track_*",
                "reason": "single_track_album",
            })
    elif not per_track_pairs:
        track_omissions.append({
            "strategy": "track_*",
            "reason": "no_runnable_track_queries",
        })
    else:
        # Rank by token count desc, char count desc, source index asc.
        # Source index is the natural tiebreaker for stable ordering.
        ranked = sorted(
            per_track_pairs,
            key=lambda pair: (
                -len(pair[1].split()),
                -len(pair[1]),
                pair[0],
            ),
        )
        chosen = ranked[: config.max_track_slots]
        omitted = ranked[config.max_track_slots:]
        # Record omitted track candidates for plan-level provenance.
        for src_idx, q in omitted:
            track_omissions.append({
                "strategy": "track_excess",
                "reason": "exceeded_max_track_slots",
                "source_track_index": src_idx,
                "query": q,
                "canonical_query_key": _canonical_query_key(q),
            })
        # The track slot index is the *plan-side* ordinal among track slots
        # (0..max_track_slots-1), not the source-track-index. This matches
        # the existing `track_<idx>` strategy label semantics where idx is
        # an executor cycle position.
        for plan_track_idx, (src_idx, q) in enumerate(chosen):
            candidates.append(_Candidate(
                strategy=f"track_{plan_track_idx}",
                repeat_group=f"track_{plan_track_idx}",
                query=q,
                omit_reason=None,
                extra_provenance={
                    "source_track_index": src_idx,
                    "track_slot_index": plan_track_idx,
                },
            ))

    # --- Resolve candidates into runnable items + provenance ------------
    runnable: list[SearchPlanItem] = []
    omitted_candidates: list[dict[str, Any]] = []
    dedupe_losers: list[dict[str, Any]] = []
    # Map canonical_query_key -> (winner_strategy, winner_ordinal). Default
    # slots are tracked by their shared repeat_group so multiple default
    # slots are NOT considered duplicates of each other.
    seen_keys: dict[str, tuple[str, int]] = {}

    next_ordinal = 0
    for cand in candidates:
        if cand.query is None:
            omitted_candidates.append({
                "strategy": cand.strategy,
                "reason": cand.omit_reason or "unknown",
                **cand.extra_provenance,
            })
            continue
        key = _canonical_query_key(cand.query)
        is_default_repeat = cand.repeat_group == _STRATEGY_DEFAULT
        prior = seen_keys.get(key)
        if prior is not None and not is_default_repeat:
            # Cross-strategy duplicate. Keep earlier slot, record loser.
            winner_strategy, _winner_ordinal = prior
            dedupe_losers.append({
                "winner_strategy": winner_strategy,
                "loser_strategy": cand.strategy,
                "canonical_query_key": key,
                "would_have_been_ordinal": next_ordinal,
            })
            continue

        item = SearchPlanItem(
            ordinal=next_ordinal,
            strategy=cand.strategy,
            query=cand.query,
            canonical_query_key=key,
            repeat_group=cand.repeat_group,
            provenance=dict(cand.extra_provenance),
        )
        runnable.append(item)
        # Only record default key once — repeated defaults share identity but
        # do NOT block other strategies that produce the same key (rare but
        # possible if title equals artist token).
        if not is_default_repeat or key not in seen_keys:
            seen_keys[key] = (cand.strategy, next_ordinal)
        next_ordinal += 1

    omitted_candidates.extend(track_omissions)

    snapshot_signature = {
        "artist_name": artist,
        "title": title,
        "year": year,
        "track_count": len(track_titles),
        "redownload": snapshot.redownload,
    }

    base_provenance: dict[str, Any] = {
        "omitted_candidates": omitted_candidates,
        "dedupe_losers": dedupe_losers,
        "dropped_low_entropy_tokens": sorted(dropped_low_entropy),
        "snapshot_signature": snapshot_signature,
    }

    if not runnable:
        # Deterministic generation failure: no runnable artist/title/track
        # query. This is sticky for the current generator id (U3).
        return SearchPlan(
            generator_id=SEARCH_PLAN_GENERATOR_ID,
            status=PLAN_STATUS_GENERATION_FAILED,
            items=(),
            provenance=base_provenance,
            failure_reason="no_runnable_query",
        )

    return SearchPlan(
        generator_id=SEARCH_PLAN_GENERATOR_ID,
        status=PLAN_STATUS_SUCCESS,
        items=tuple(runnable),
        provenance=base_provenance,
        failure_reason=None,
    )
