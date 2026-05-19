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
# Persisted search-plan generator id — the ONLY automatic invalidation key
# for stored plans. Any change that affects generator output (token rules,
# strategy ladder, repeat-group identity, dedupe behavior, provenance shape)
# MUST bump this string. The plan-generation service and startup
# reconciliation read it to decide whether a stored plan is current.
SEARCH_PLAN_GENERATOR_ID = "search-plan/2026-05-19-1"


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
    # Forensic capture persisted to search_log: candidates JSONB (top-20
    # match scores from find_download), variant tag, slskd terminal state.
    candidates: tuple[CandidateScore, ...] = ()
    # U2 of search-plan-entropy: aggregate count of dirs the asymmetric
    # pre-filter rejected before browse during this search's
    # find_download walk. Persisted on search_log.pre_filter_skip_count
    # for per-search aggregation.
    pre_filter_skip_count: int = 0
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
    # Plan-item snapshot the executor ran, plus the cycle-count taken at
    # selection time. The cycle snapshot is the stale-completion guard: at
    # log time the DB compares it to the request's current plan_cycle_count
    # to detect completions after mid-flight regeneration. None for
    # legacy / no-plan code paths.
    plan_execution: "PlanExecutionContext | None" = None


@dataclass(frozen=True)
class PlanExecutionContext:
    """Snapshot of the active plan-item the executor selected for a search.

    Frozen and immutable — captured once in the owner thread when the
    executor reads the request's active plan, then carried through the
    in-flight ``SearchResult`` and downstream ownership claims so the DB
    can validate that the executing plan/ordinal/cycle still match the
    request's active state at log/claim time.
    """

    plan_id: int
    plan_item_id: int
    plan_ordinal: int
    plan_strategy: str
    plan_canonical_query_key: str | None
    plan_repeat_group: str | None
    plan_generator_id: str
    plan_item_count: int
    cycle_count_snapshot: int


def is_plan_execution_current(
    request_row: dict[str, Any] | None,
    plan_execution: "PlanExecutionContext | None",
) -> bool:
    """Return True iff the request's active plan still matches the executed plan.

    Pure — facts in, decision out. Used as a stale-completion guard before
    any active-state mutation driven by an in-flight search (download
    ownership claims, status transitions, request-level cursor writes that
    are NOT routed through ``record_consumed_search_attempt``).

    A plan-execution-less request_row is never current with no plan_execution
    (returns False). When ``plan_execution`` is None we treat the call as
    "non-plan-aware" and return True so legacy paths still work; the only
    real-world callers of this guard are search-execution-driven mutations
    that always have plan context.
    """
    if plan_execution is None:
        return True
    if request_row is None:
        return False
    active_plan_id = request_row.get("active_plan_id")
    if active_plan_id != plan_execution.plan_id:
        return False
    next_ordinal = request_row.get("next_plan_ordinal")
    if next_ordinal is None:
        return False
    if int(next_ordinal) != plan_execution.plan_ordinal:
        return False
    cycle_count = request_row.get("plan_cycle_count")
    if cycle_count is None:
        return False
    if int(cycle_count) != plan_execution.cycle_count_snapshot:
        return False
    return True


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
# Pure search-plan generator.
#
# Replaces cycle-index variant selection with a deterministic, materialized
# plan list. Same album-level behavior as `select_variant()`, plus:
#   - bounded provenance (omitted candidates with reasons, dedupe losers)
#   - canonical query keys for usefulness aggregation
#   - repeat-group identity for the intentional repeated-default slots
#   - explicit deterministic generation-failure result (not an empty plan)
#
# `select_variant` / `build_query` remain in this module for non-executor
# callers (CLI smoke tools, generator parity tests). Do NOT reintroduce them
# on the search execution path — `cratedigger._select_active_plan_item_for_album`
# is authoritative.
# ---------------------------------------------------------------------------


# Strategy labels used on plan items. Aligned with `select_variant()` tags
# where they overlap so search-log forensics stay readable across the
# generator-output transitions. U5 of search-plan-entropy added:
#   * literal           — un-wildcarded artist+title, no short-token drop
#   * literal_flac      — literal + " FLAC" format hint
#   * literal_lossless  — literal + " lossless" format hint
#   * unwild_rg_year    — un-wildcarded + release-group year (reissue path)
#   * track_<idx>_artist — artist prepended to per-track fallback query
#   * selftitled_*      — dedicated mix for self-titled releases
_STRATEGY_DEFAULT = "default"
_STRATEGY_LITERAL = "literal"
_STRATEGY_LITERAL_FLAC = "literal_flac"
_STRATEGY_LITERAL_LOSSLESS = "literal_lossless"
_STRATEGY_UNWILD = "unwild"
_STRATEGY_UNWILD_YEAR = "unwild_year"
_STRATEGY_UNWILD_RG_YEAR = "unwild_rg_year"
_STRATEGY_SELFTITLED_ARTIST_TRACK_PREFIX = "selftitled_artist_track_"
_STRATEGY_SELFTITLED_ARTIST_YEAR = "selftitled_artist_year"


MAX_TRACK_SLOTS_PER_PLAN = 3
# Format-hint tokens appended to the un-wildcarded literal query to bait
# peers who file their lossless rips with the format tag in the directory
# name. Tokens stay literal — no wildcarding, no short-token drop, no
# low-entropy normalization. Soulseek's distributed search caps at 4
# tokens, so we pre-cap the body to MAX_SEARCH_TOKENS-1 to make room.
_FORMAT_HINT_FLAC = "FLAC"
_FORMAT_HINT_LOSSLESS = "lossless"


# Plan-status values. String literals so they round-trip cleanly through
# JSONB at the persistence layer.
PLAN_STATUS_SUCCESS = "success"
PLAN_STATUS_GENERATION_FAILED = "generation_failed"

# Generator-output failure class. Mirrored in lib/search_plan_service.py
# alongside the service-layer failure classes (resolver_unavailable etc.).
FAILURE_CLASS_NO_RUNNABLE_QUERY = "no_runnable_query"


@dataclass(frozen=True)
class ReleaseSnapshot:
    """Pure input value to `generate_search_plan`.

    A snapshot of release metadata at generation time. The generator only
    reads this value — resolver/DB I/O happens in the service layer.
    `redownload` is preserved into provenance for debuggability but does
    not affect the generated ladder.

    `prepend_artist` is a generation-affecting config knob carried on the
    snapshot so the function stays pure (no implicit module/global config
    reads).
    """

    artist_name: str
    title: str
    year: str | None
    track_titles: tuple[str, ...]
    redownload: bool = False
    prepend_artist: bool = False
    # U5 of search-plan-entropy: release-group year (first release year of
    # the MB release group). When known AND different from `year`, the
    # generator emits an extra `unwild_rg_year` slot so reissues find their
    # original-pressing peers on Soulseek. NULL means no extra slot.
    release_group_year: int | None = None


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
    *,
    prepend_artist: bool = True,
) -> list[tuple[int, str]]:
    """Return runnable per-track queries paired with source-track index.

    Cleaning + low-entropy normalization + case-insensitive dedupe as
    before. U5 of search-plan-entropy (R8) prepends artist tokens to the
    track query so a bare track title can't collapse to "Distinct Word"
    that matches a million unrelated peers. The artist tokens are
    *wildcarded* via ``wildcard_artist_tokens`` to bypass server-side bans
    consistently with the default/literal strategies.

    Single-token track titles still fall back to ``_longest_artist_token``
    enrichment when ``prepend_artist=False`` (legacy call path) or when
    the artist name has no wildcardable tokens (``Various Artists`` →
    empty after the wildcard filter; the legacy fallback supplies a
    distinctive artist token).

    No short-token drop anywhere in this pipeline — distinctiveness is
    enforced by ``cap_tokens`` (longest-first) and by the prepended
    artist providing entropy. Numeric / one-character tokens like "A"
    and "22" survive into queries.
    """
    seen_lower: set[str] = set()
    out: list[tuple[int, str]] = []

    # Prepare prepended artist tokens once. Wildcarded for ban bypass,
    # mirroring how the default/literal album-level slots treat artist
    # tokens. Empty when prepend_artist=False or artist has no
    # wildcardable content.
    artist_prepend_tokens: list[str] = []
    if prepend_artist:
        clean_artist = strip_special_chars(artist_name)
        if clean_artist.lower() not in ("various artists", "various"):
            raw_artist_tokens = clean_artist.split()
            normalized = _normalize_query_tokens(
                raw_artist_tokens,
                preserve_all_low_entropy=True,
            )
            artist_prepend_tokens = wildcard_artist_tokens(normalized)

    for src_idx, title in enumerate(track_titles):
        cleaned = strip_special_chars(title)
        tokens = cleaned.split()
        # NO short-token drop here (U5 R5): titles like "Get Up" or
        # numeric tracks ("22") were being stripped, collapsing to
        # nothing or to broken fragments.
        tokens = _normalize_query_tokens(tokens)
        if not tokens:
            continue

        if artist_prepend_tokens:
            # Drop title tokens that duplicate (case-insensitive) any
            # artist token already in the prefix — avoids "Wiggles Wiggles
            # Song" double-up.
            artist_keys = {t.lower().lstrip("*") for t in artist_prepend_tokens}
            tokens = [
                t for t in tokens
                if t.lower() not in artist_keys
            ]
            if not tokens:
                # All title tokens were already in the artist prefix.
                # Skip — query collapses to bare artist name.
                continue
            combined = artist_prepend_tokens + tokens
            combined = cap_tokens(combined)
            query = " ".join(combined)
        else:
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


def _normalize_token_set(text: str) -> set[str]:
    """Lowercase + dedupe tokens from a cleaned string. Empty on empty."""
    cleaned = strip_special_chars(text)
    return {t.lower() for t in cleaned.split() if t}


def _is_self_titled(artist_name: str, title: str) -> bool:
    """Self-titled detection (U5 R10).

    True iff the case-insensitive deduped token sets of artist and title
    are equal and non-empty. Catches:

      * Exact match: ``Willow / Willow`` → both {willow}.
      * Repetition: ``Mountains / Mountains Mountains Mountains`` →
        both {mountains} after dedupe.

    Does NOT match ``Willow / Willow Tree`` (title carries extra tokens
    that disambiguate it from the self-titled release).
    """
    a = _normalize_token_set(artist_name)
    t = _normalize_token_set(title)
    return bool(a) and a == t


def _build_default_query(
    artist: str,
    title: str,
    *,
    prepend_artist: bool,
) -> str | None:
    """Build the default slot query: wildcarded artist + title, no short drop.

    U5 of search-plan-entropy:
      * No `strip_short_tokens` call — short tokens survive into the
        query (capped only by token count, longest-first).
      * Low-entropy normalization still applies ("the/you/from/and"
        dropped).
      * Artist tokens wildcarded for ban bypass.
      * Title tokens that duplicate the wildcarded artist tokens
        (case-insensitive on the un-wildcarded form) are dropped.
    """
    clean_artist = strip_special_chars(artist)
    clean_title = strip_special_chars(title)

    artist_tokens = clean_artist.split()
    title_tokens = clean_title.split()

    artist_tokens = _normalize_query_tokens(
        artist_tokens, preserve_all_low_entropy=True,
    )
    title_tokens = _normalize_query_tokens(title_tokens)

    artist_lower = {t.lower() for t in artist_tokens}
    title_tokens = [t for t in title_tokens if t.lower() not in artist_lower]

    if clean_artist.lower() in ("various artists", "various"):
        artist_tokens = []

    artist_tokens = wildcard_artist_tokens(artist_tokens)

    if prepend_artist and artist_tokens:
        all_tokens = artist_tokens + title_tokens
    else:
        all_tokens = title_tokens

    if not all_tokens:
        return None

    all_tokens = cap_tokens(all_tokens, MAX_SEARCH_TOKENS)
    return " ".join(all_tokens)


def _build_literal_query(
    artist: str,
    title: str,
    *,
    prepend_artist: bool,
    max_tokens: int = MAX_SEARCH_TOKENS,
) -> str | None:
    """Build the literal slot query: artist + title, no wildcard, no short drop.

    Used directly for the ``literal`` slot and as the body for the
    format-hint and year-anchored slots (``literal_flac``,
    ``literal_lossless``, ``unwild_year``, ``unwild_rg_year``).

    Pipeline:
      * Strip special chars.
      * Low-entropy normalization on both sides (``the/you/from/and``).
      * Drop title tokens that duplicate artist tokens (case-insensitive).
      * Drop artist for Various Artists.
      * Cap to `max_tokens` longest-first.
    """
    clean_artist = strip_special_chars(artist)
    clean_title = strip_special_chars(title)

    artist_tokens = clean_artist.split()
    title_tokens = clean_title.split()

    artist_tokens = _normalize_query_tokens(
        artist_tokens, preserve_all_low_entropy=True,
    )
    title_tokens = _normalize_query_tokens(title_tokens)

    artist_lower = {t.lower() for t in artist_tokens}
    title_tokens = [t for t in title_tokens if t.lower() not in artist_lower]

    if clean_artist.lower() in ("various artists", "various"):
        artist_tokens = []

    if prepend_artist and artist_tokens:
        all_tokens = artist_tokens + title_tokens
    else:
        all_tokens = title_tokens

    if not all_tokens:
        return None

    all_tokens = cap_tokens(all_tokens, max_tokens)
    return " ".join(all_tokens)


def _literal_artist_tokens(artist: str) -> list[str]:
    """Literal (un-wildcarded) artist tokens used by selftitled mix."""
    clean_artist = strip_special_chars(artist)
    if clean_artist.lower() in ("various artists", "various"):
        return []
    tokens = clean_artist.split()
    return _normalize_query_tokens(tokens, preserve_all_low_entropy=True)


def _selftitled_track_query(
    artist: str,
    track_title: str,
    *,
    max_tokens: int = MAX_SEARCH_TOKENS,
) -> str | None:
    """Build a ``<artist> <track-title>`` query for the selftitled mix.

    Artist tokens are kept LITERAL (no wildcard) — the selftitled mix
    leans on the track-title's distinctiveness rather than on bypassing
    server bans, and a literal artist + literal track title is the
    highest-recall shape for niche self-titled releases.
    """
    artist_tokens = _literal_artist_tokens(artist)
    cleaned = strip_special_chars(track_title)
    title_tokens = _normalize_query_tokens(cleaned.split())
    if not title_tokens:
        return None
    # Drop title tokens that duplicate artist tokens
    artist_keys = {t.lower() for t in artist_tokens}
    title_tokens = [t for t in title_tokens if t.lower() not in artist_keys]
    if not title_tokens:
        return None
    if artist_tokens:
        combined = artist_tokens + title_tokens
    else:
        combined = title_tokens
    combined = cap_tokens(combined, max_tokens)
    if not combined:
        return None
    return " ".join(combined)


def _selftitled_artist_year_query(
    artist: str,
    year: str | None,
) -> str | None:
    """Build ``<artist> <year>`` for the selftitled mix. None if no year."""
    if not _year_is_known(year):
        return None
    assert year is not None
    artist_tokens = _literal_artist_tokens(artist)
    if not artist_tokens:
        return None
    combined = artist_tokens + [year[:4]]
    combined = cap_tokens(combined, MAX_SEARCH_TOKENS)
    if not combined:
        return None
    return " ".join(combined)


def _append_format_hint(literal_body: str | None, hint: str) -> str | None:
    """Append a literal format hint token to a pre-built literal body.

    Body is expected to already be capped at MAX_SEARCH_TOKENS - 1
    (caller's responsibility) so the hint is preserved. Returns None
    when the body is None or empty.
    """
    if not literal_body:
        return None
    return f"{literal_body} {hint}"


def _generate_normal_plan(
    snapshot: ReleaseSnapshot,
    config: SearchPlanConfig,
) -> tuple[list["_Candidate"], list[dict[str, Any]]]:
    """Build the candidate ladder for a non-self-titled request.

    Returns ``(candidates, track_omissions)``. Caller folds them into
    runnable items via the shared dedupe pass in ``generate_search_plan``.
    """
    artist = snapshot.artist_name
    title = snapshot.title
    year = snapshot.year
    rg_year = snapshot.release_group_year
    track_titles = list(snapshot.track_titles)
    prepend_artist = snapshot.prepend_artist

    candidates: list[_Candidate] = []

    # 1. Default slot — wildcarded artist + title, no short-token drop.
    default_query = _build_default_query(
        artist, title, prepend_artist=prepend_artist,
    )
    if default_query:
        candidates.append(_Candidate(
            strategy=_STRATEGY_DEFAULT,
            repeat_group=_STRATEGY_DEFAULT,
            query=default_query,
            omit_reason=None,
        ))
    else:
        candidates.append(_Candidate(
            strategy=_STRATEGY_DEFAULT,
            repeat_group=_STRATEGY_DEFAULT,
            query=None,
            omit_reason="empty_default_query",
        ))

    # 2. Literal slot — un-wildcarded, no short-token drop, no year.
    literal_query = _build_literal_query(
        artist, title, prepend_artist=prepend_artist,
    )
    # Body capped at MAX_SEARCH_TOKENS - 1 so format-hint slots can
    # append their hint token without losing it to the slskd 4-token cap.
    literal_body_for_hint = _build_literal_query(
        artist, title,
        prepend_artist=prepend_artist,
        max_tokens=MAX_SEARCH_TOKENS - 1,
    )

    if literal_query:
        candidates.append(_Candidate(
            strategy=_STRATEGY_LITERAL,
            repeat_group=_STRATEGY_LITERAL,
            query=literal_query,
            omit_reason=None,
        ))
    else:
        candidates.append(_Candidate(
            strategy=_STRATEGY_LITERAL,
            repeat_group=_STRATEGY_LITERAL,
            query=None,
            omit_reason="empty_literal_query",
        ))

    # 3 + 4. Format-hint slots — unconditional (R7).
    for hint_strategy, hint_token in (
        (_STRATEGY_LITERAL_FLAC, _FORMAT_HINT_FLAC),
        (_STRATEGY_LITERAL_LOSSLESS, _FORMAT_HINT_LOSSLESS),
    ):
        hint_query = _append_format_hint(literal_body_for_hint, hint_token)
        if hint_query:
            candidates.append(_Candidate(
                strategy=hint_strategy,
                repeat_group=hint_strategy,
                query=hint_query,
                omit_reason=None,
                extra_provenance={"format_hint": hint_token},
            ))
        else:
            candidates.append(_Candidate(
                strategy=hint_strategy,
                repeat_group=hint_strategy,
                query=None,
                omit_reason="empty_literal_query",
                extra_provenance={"format_hint": hint_token},
            ))

    # 5. unwild_year slot — when release `year` is known.
    year_known = _year_is_known(year)
    if year_known and literal_query:
        assert year is not None
        candidates.append(_Candidate(
            strategy=_STRATEGY_UNWILD_YEAR,
            repeat_group=_STRATEGY_UNWILD_YEAR,
            query=f"{literal_query} {year[:4]}",
            omit_reason=None,
            extra_provenance={"year": year[:4]},
        ))
    else:
        reason = "year_unknown" if not year_known else "empty_literal_query"
        candidates.append(_Candidate(
            strategy=_STRATEGY_UNWILD_YEAR,
            repeat_group=_STRATEGY_UNWILD_YEAR,
            query=None,
            omit_reason=reason,
        ))

    # 6. unwild_rg_year slot — only when rg_year is known AND differs
    #    from release `year`. When they match we omit (avoid duplicate
    #    of the unwild_year slot).
    if (
        year_known
        and rg_year is not None
        and int(rg_year) > 0
        and str(rg_year) != (year[:4] if year else "")
        and literal_query
    ):
        rg_year_str = f"{int(rg_year):04d}"
        candidates.append(_Candidate(
            strategy=_STRATEGY_UNWILD_RG_YEAR,
            repeat_group=_STRATEGY_UNWILD_RG_YEAR,
            query=f"{literal_query} {rg_year_str}",
            omit_reason=None,
            extra_provenance={"release_group_year": int(rg_year)},
        ))
    elif rg_year is None:
        candidates.append(_Candidate(
            strategy=_STRATEGY_UNWILD_RG_YEAR,
            repeat_group=_STRATEGY_UNWILD_RG_YEAR,
            query=None,
            omit_reason="release_group_year_unknown",
        ))
    elif not year_known:
        candidates.append(_Candidate(
            strategy=_STRATEGY_UNWILD_RG_YEAR,
            repeat_group=_STRATEGY_UNWILD_RG_YEAR,
            query=None,
            omit_reason="year_unknown",
            extra_provenance={"release_group_year": int(rg_year)},
        ))
    elif str(rg_year) == (year[:4] if year else ""):
        candidates.append(_Candidate(
            strategy=_STRATEGY_UNWILD_RG_YEAR,
            repeat_group=_STRATEGY_UNWILD_RG_YEAR,
            query=None,
            omit_reason="release_group_year_matches_year",
            extra_provenance={"release_group_year": int(rg_year)},
        ))
    else:
        candidates.append(_Candidate(
            strategy=_STRATEGY_UNWILD_RG_YEAR,
            repeat_group=_STRATEGY_UNWILD_RG_YEAR,
            query=None,
            omit_reason="empty_literal_query",
            extra_provenance={"release_group_year": int(rg_year)},
        ))

    # 7. Per-track slots — artist-prepended (R8). Multi-track only.
    per_track_pairs = _per_track_candidates(
        track_titles, artist, prepend_artist=True,
    )
    track_omissions: list[dict[str, Any]] = []
    track_candidates = _build_track_candidates(
        track_titles, per_track_pairs, config.max_track_slots,
        track_omissions, slot_label_suffix="_artist",
    )
    candidates.extend(track_candidates)

    return candidates, track_omissions


def _generate_selftitled_plan(
    snapshot: ReleaseSnapshot,
    config: SearchPlanConfig,
) -> tuple[list["_Candidate"], list[dict[str, Any]]]:
    """Build the dedicated candidate ladder for self-titled releases (R11).

    The default/literal slots are skipped — they collapse to bare artist
    name and saturate slskd. Instead the mix substitutes:

      1. ``selftitled_artist_track_0`` — artist + first track (literal).
      2. ``selftitled_artist_track_0_flac`` — same, with FLAC suffix.
      3. ``selftitled_artist_year`` — artist + release year (literal).
      4-N. ``track_<idx>_artist`` — artist-prepended per-track slots
            (wildcarded artist) up to ``max_track_slots`` ranked tracks.
    """
    artist = snapshot.artist_name
    year = snapshot.year
    track_titles = list(snapshot.track_titles)

    candidates: list[_Candidate] = []

    # Pick first track that yields a runnable selftitled query.
    first_track_idx: int | None = None
    first_track_query: str | None = None
    first_track_query_body: str | None = None
    for src_idx, title in enumerate(track_titles):
        body = _selftitled_track_query(
            artist, title, max_tokens=MAX_SEARCH_TOKENS - 1,
        )
        full = _selftitled_track_query(artist, title)
        if full:
            first_track_idx = src_idx
            first_track_query = full
            first_track_query_body = body
            break

    # 1. selftitled_artist_track_0
    strat1 = f"{_STRATEGY_SELFTITLED_ARTIST_TRACK_PREFIX}0"
    if first_track_query is not None and first_track_idx is not None:
        candidates.append(_Candidate(
            strategy=strat1,
            repeat_group=strat1,
            query=first_track_query,
            omit_reason=None,
            extra_provenance={"source_track_index": first_track_idx},
        ))
    else:
        candidates.append(_Candidate(
            strategy=strat1,
            repeat_group=strat1,
            query=None,
            omit_reason=(
                "no_runnable_track_queries" if track_titles
                else "no_tracks"
            ),
        ))

    # 2. selftitled_artist_track_0_flac
    strat1_flac = f"{strat1}_flac"
    if first_track_query_body is not None and first_track_idx is not None:
        candidates.append(_Candidate(
            strategy=strat1_flac,
            repeat_group=strat1_flac,
            query=f"{first_track_query_body} {_FORMAT_HINT_FLAC}",
            omit_reason=None,
            extra_provenance={
                "source_track_index": first_track_idx,
                "format_hint": _FORMAT_HINT_FLAC,
            },
        ))
    else:
        candidates.append(_Candidate(
            strategy=strat1_flac,
            repeat_group=strat1_flac,
            query=None,
            omit_reason=(
                "no_runnable_track_queries" if track_titles
                else "no_tracks"
            ),
            extra_provenance={"format_hint": _FORMAT_HINT_FLAC},
        ))

    # 3. selftitled_artist_year
    ay_query = _selftitled_artist_year_query(artist, year)
    if ay_query:
        assert year is not None
        candidates.append(_Candidate(
            strategy=_STRATEGY_SELFTITLED_ARTIST_YEAR,
            repeat_group=_STRATEGY_SELFTITLED_ARTIST_YEAR,
            query=ay_query,
            omit_reason=None,
            extra_provenance={"year": year[:4]},
        ))
    else:
        reason = "year_unknown" if not _year_is_known(year) else "empty_artist"
        candidates.append(_Candidate(
            strategy=_STRATEGY_SELFTITLED_ARTIST_YEAR,
            repeat_group=_STRATEGY_SELFTITLED_ARTIST_YEAR,
            query=None,
            omit_reason=reason,
        ))

    # 4. track_<idx>_artist slots — same shape as the normal mix.
    per_track_pairs = _per_track_candidates(
        track_titles, artist, prepend_artist=True,
    )
    track_omissions: list[dict[str, Any]] = []
    track_candidates = _build_track_candidates(
        track_titles, per_track_pairs, config.max_track_slots,
        track_omissions, slot_label_suffix="_artist",
    )
    candidates.extend(track_candidates)

    return candidates, track_omissions


def _build_track_candidates(
    track_titles: list[str],
    per_track_pairs: list[tuple[int, str]],
    max_track_slots: int,
    track_omissions: list[dict[str, Any]],
    *,
    slot_label_suffix: str,
) -> list["_Candidate"]:
    """Rank per-track pairs and emit up to ``max_track_slots`` candidates.

    Ranking: useful-token count desc, char count desc, source-track
    index asc. Mirrors the legacy ordering so search-log forensics
    stay comparable across the U5 transition.

    ``slot_label_suffix`` is appended to the ``track_<idx>`` strategy
    label (e.g. ``"_artist"`` → ``track_0_artist``) so the new
    artist-prepended slots are distinguishable from the legacy
    track-only labels in search-log forensics.
    """
    if len(track_titles) <= 1:
        if track_titles:
            track_omissions.append({
                "strategy": "track_*",
                "reason": "single_track_album",
            })
        return []
    if not per_track_pairs:
        track_omissions.append({
            "strategy": "track_*",
            "reason": "no_runnable_track_queries",
        })
        return []

    ranked = sorted(
        per_track_pairs,
        key=lambda pair: (
            -len(pair[1].split()),
            -len(pair[1]),
            pair[0],
        ),
    )
    chosen = ranked[:max_track_slots]
    omitted = ranked[max_track_slots:]
    for src_idx, q in omitted:
        track_omissions.append({
            "strategy": "track_excess",
            "reason": "exceeded_max_track_slots",
            "source_track_index": src_idx,
            "query": q,
            "canonical_query_key": _canonical_query_key(q),
        })

    out: list[_Candidate] = []
    for plan_track_idx, (src_idx, q) in enumerate(chosen):
        label = f"track_{plan_track_idx}{slot_label_suffix}"
        out.append(_Candidate(
            strategy=label,
            repeat_group=label,
            query=q,
            omit_reason=None,
            extra_provenance={
                "source_track_index": src_idx,
                "track_slot_index": plan_track_idx,
            },
        ))
    return out


def generate_search_plan(
    snapshot: ReleaseSnapshot,
    config: SearchPlanConfig,
) -> SearchPlan:
    """Generate the deterministic search plan for one release.

    Pure function: same input → same output. No I/O. The result is the
    materialized executor schedule for the release, in slot order.

    U5 of search-plan-entropy (2026-05-19) restructured the slot mix.
    Normal request (year known, release_group_year populated and
    differs from year, multi-track):

      1. default              wildcarded artist+title, no short-drop
      2. literal              un-wildcarded artist+title, no year
      3. literal_flac         literal + " FLAC"
      4. literal_lossless     literal + " lossless"
      5. unwild_year          literal + " <year>"
      6. unwild_rg_year       literal + " <rg_year>" (conditional)
      7-9. track_<idx>_artist artist-prepended per-track fallback

    Self-titled requests (R10/R11) skip the album-level default/literal
    slots — they collapse to bare artist name and waste slskd cycles
    — and emit a dedicated mix:

      1. selftitled_artist_track_0       artist + first track (literal)
      2. selftitled_artist_track_0_flac  + " FLAC"
      3. selftitled_artist_year          artist + year (literal)
      4-N. track_<idx>_artist            artist-prepended per-track

    The ``escalation_threshold`` config field is preserved for backwards
    compatibility with persisted ``SearchPlanConfig`` values but no
    longer controls slot count — U5 removed the five-slot default
    repetition (R4).

    Cross-strategy dedupe: when a candidate produces the same canonical
    query as an earlier emitted slot, the earlier one wins and the
    loser is recorded in plan-level provenance with the ordinal it
    would have had.

    Generation failure: when no candidate is runnable, returns
    ``PLAN_STATUS_GENERATION_FAILED`` with populated provenance, NOT
    an empty success plan.
    """
    artist = snapshot.artist_name
    title = snapshot.title
    year = snapshot.year
    track_titles = list(snapshot.track_titles)
    rg_year = snapshot.release_group_year

    # Self-titled detection (R10). When True we route to a dedicated
    # ladder; otherwise the normal mix.
    selftitled = _is_self_titled(artist, title)

    # Track low-entropy drops across raw inputs for plan provenance.
    # The drop itself happens inside _normalize_query_tokens, but
    # recording it once at the plan level keeps the provenance
    # contract stable for operators.
    all_input_tokens: list[list[str]] = []
    all_input_tokens.append(strip_special_chars(artist).split())
    all_input_tokens.append(strip_special_chars(title).split())
    for t in track_titles:
        all_input_tokens.append(strip_special_chars(t).split())
    dropped_low_entropy = _has_dropped_low_entropy(*all_input_tokens)

    if selftitled:
        candidates, track_omissions = _generate_selftitled_plan(
            snapshot, config,
        )
    else:
        candidates, track_omissions = _generate_normal_plan(
            snapshot, config,
        )

    # --- Resolve candidates into runnable items + provenance ------------
    runnable: list[SearchPlanItem] = []
    omitted_candidates: list[dict[str, Any]] = []
    dedupe_losers: list[dict[str, Any]] = []
    # Map canonical_query_key -> (winner_strategy, winner_ordinal).
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
        prior = seen_keys.get(key)
        if prior is not None:
            # Cross-strategy duplicate. Keep earlier slot, record loser.
            winner_strategy, _ = prior
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
        seen_keys[key] = (cand.strategy, next_ordinal)
        next_ordinal += 1

    omitted_candidates.extend(track_omissions)

    snapshot_signature: dict[str, Any] = {
        "artist_name": artist,
        "title": title,
        "year": year,
        "track_count": len(track_titles),
        "redownload": snapshot.redownload,
    }
    if rg_year is not None:
        snapshot_signature["release_group_year"] = int(rg_year)

    base_provenance: dict[str, Any] = {
        "omitted_candidates": omitted_candidates,
        "dedupe_losers": dedupe_losers,
        "dropped_low_entropy_tokens": sorted(dropped_low_entropy),
        "snapshot_signature": snapshot_signature,
    }
    if selftitled:
        base_provenance["selftitled"] = True
    if rg_year is not None:
        base_provenance["release_group_year"] = int(rg_year)

    if not runnable:
        return SearchPlan(
            generator_id=SEARCH_PLAN_GENERATOR_ID,
            status=PLAN_STATUS_GENERATION_FAILED,
            items=(),
            provenance=base_provenance,
            failure_reason=FAILURE_CLASS_NO_RUNNABLE_QUERY,
        )

    return SearchPlan(
        generator_id=SEARCH_PLAN_GENERATOR_ID,
        status=PLAN_STATUS_SUCCESS,
        items=tuple(runnable),
        provenance=base_provenance,
        failure_reason=None,
    )
