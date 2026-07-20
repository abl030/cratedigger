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
from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    from lib.quality import CandidateScore


# ---------------------------------------------------------------------------
# Persisted search-plan generator id — the ONLY automatic invalidation key
# for stored plans. Any change that affects generator output (token rules,
# strategy ladder, repeat-group identity, dedupe behavior, provenance shape)
# MUST bump this string. The plan-generation service and startup
# reconciliation read it to decide whether a stored plan is current.
SEARCH_PLAN_GENERATOR_ID = "search-plan/2026-05-25-1"


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
    # Aggregate count of dirs the asymmetric pre-filter rejected before
    # browse during this search; persisted on
    # ``search_log.pre_filter_skip_count`` for per-search aggregation.
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
    # U11 R23: uncapped result count from slskd's terminal state
    # response. ``responseCount`` is what slskd's writer tracked;
    # ``result_count`` is what our harvest call returned. The two
    # diverge when slskd hit ``responseLimit`` / ``fileLimit`` and
    # truncated the response array. ``None`` when the slskd state
    # lookup failed before a terminal state was observed (pre-attempt /
    # error paths).
    result_count_uncapped: int | None = None
    # NOTE: ``rejection_reason`` (U11 R22) and ``matcher_score_top1``
    # (U11 R26) are NOT carried on SearchResult. The log site in
    # ``cratedigger.py::_log_search_result`` reconstructs both from
    # ``result.candidates`` via the pure helpers
    # ``lib.matching.classify_rejection_from_log_inputs`` and
    # ``lib.matching.matcher_score_top1_for`` — the single source of
    # truth for both scalars. Adding them back to SearchResult would
    # be dead duplication.


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


# Soulseek's distributed search times out with too many tokens.
# 4 is the safe maximum.
MAX_SEARCH_TOKENS = 4
# Empirical from search_log peer-dir fanout: keep this list narrow and add
# only words that repeatedly dominate expensive broad searches.
#
# Single canonical stopword set for the whole pipeline. Callers MUST go
# through `strip_stopwords()` rather than reading this constant directly —
# `tests/test_stopwords_audit.py` walks `lib/` to enforce that and also
# fails the suite if anyone re-introduces an inline `{"the", "and", ...}`
# stopword literal anywhere in `lib/`.
STOPWORDS: frozenset[str] = frozenset({"the", "you", "from", "and"})


def strip_stopwords(tokens: list[str]) -> list[str]:
    """Drop case-insensitive stopword matches from `tokens`.

    Order-preserving. Does not dedupe — `_normalize_query_tokens` composes
    this with case-insensitive dedupe and the all-stopword fallback.

    This is the only public reader of `STOPWORDS`. Callers outside this
    module MUST go through this helper (enforced by
    `tests/test_stopwords_audit.py`) so the set's contents can change in
    exactly one place.
    """
    return [t for t in tokens if t.lower() not in STOPWORDS]


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


def _normalize_query_tokens(
    tokens: list[str],
    *,
    preserve_all_low_entropy: bool = False,
) -> list[str]:
    """Drop stopwords and case-insensitive repeats."""
    normalized: list[str] = []
    seen: set[str] = set()
    for token in strip_stopwords(tokens):
        key = token.lower()
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

    R6 rationale (kept unchanged by PR2): every token in the artist string
    gets wildcarded, not just the first. Soulseek's server-side ban list is
    keyed on exact strings; "Mountain Goats" might be banned while
    "*ountain *oats" is not. Wildcarding every token is what bypasses bans
    in the median case. Single-token wildcarding (just the first token)
    would leave second-token bans unaddressed. The all-tokens behaviour is
    deliberate and stays unchanged through PR2 — see the brainstorm
    ``docs/brainstorms/2026-05-25-search-plan-iteration-2-requirements.md``
    for the full discussion and the trade-off against precision loss.
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


# ---------------------------------------------------------------------------
# Pure search-plan generator.
#
# Deterministic, materialized plan list with:
#   - bounded provenance (omitted candidates with reasons, dedupe losers)
#   - canonical query keys for usefulness aggregation
#   - repeat-group identity for the intentional repeated-default slots
#   - explicit deterministic generation-failure result (not an empty plan)
#
# Sole search-execution entry point — `cratedigger._select_active_plan_item_
# for_album` consumes the plan items materialised here. (An earlier cycle-
# index `select_variant` / `build_query` pair lived in this module; both
# were retired in the dead-code audit that opened #352 once nothing on the
# search execution path called them anymore.)
# ---------------------------------------------------------------------------


# Strategy labels used on plan items. Aligned with `select_variant()` tags
# where they overlap so search-log forensics stay readable across the
# generator-output transitions. U5 of search-plan-entropy added:
#   * literal           — un-wildcarded artist+title, no short-token drop
#   * literal_flac      — literal + " FLAC" format hint
#   * unwild_rg_year    — un-wildcarded + release-group year (reissue path)
#   * track_<idx>_artist — artist prepended to per-track fallback query
#   * selftitled_*      — dedicated mix for self-titled releases
# PR2 U8 retired ``literal_lossless`` (R1) — 5.5 days post-deploy data
# showed 2,093 runs / 1 successful match. The network doesn't tag for
# "lossless" the way it does for "FLAC", so the slot was spending 13% of
# search volume for a 0.05% match rate.
_STRATEGY_DEFAULT = "default"
_STRATEGY_LITERAL = "literal"
_STRATEGY_LITERAL_FLAC = "literal_flac"
_STRATEGY_UNWILD_YEAR = "unwild_year"
_STRATEGY_UNWILD_RG_YEAR = "unwild_rg_year"
# PR2 U8 — new slots for the iteration-2 generator mix.
_STRATEGY_CATALOG_NUMBER = "catalog_number"
# VA mix per-track-artist slot prefix (va_track_artist_0, _1, _2).
_STRATEGY_VA_TRACK_ARTIST_PREFIX = "va_track_artist_"
_STRATEGY_COMPILATION_SERIES = "compilation_series"
_STRATEGY_SELFTITLED_ARTIST_TRACK_PREFIX = "selftitled_artist_track_"
_STRATEGY_SELFTITLED_ARTIST_TRACK_0 = (
    f"{_STRATEGY_SELFTITLED_ARTIST_TRACK_PREFIX}0"
)
_STRATEGY_SELFTITLED_ARTIST_TRACK_0_FLAC = (
    f"{_STRATEGY_SELFTITLED_ARTIST_TRACK_0}_flac"
)
_STRATEGY_SELFTITLED_ARTIST_YEAR = "selftitled_artist_year"


# U11 R27: operator-readable template shape per plan strategy. Logged
# on every search_log row so triage queries can group by query shape
# without re-parsing the rendered text. Keys are the same strategy
# labels emitted on ``SearchPlanItem.strategy``; the per-track strategy
# families (``track_<idx>_artist`` and ``va_track_artist_<idx>``) collapse
# their numeric suffix to ``track_N`` in the template because the slot
# index is plan-position metadata, not part of the shape.
_QUERY_TEMPLATE_BY_STRATEGY: dict[str, str] = {
    _STRATEGY_DEFAULT: "{artist} {title}",
    _STRATEGY_LITERAL: "{artist} {title}",
    _STRATEGY_LITERAL_FLAC: "{artist} {title} FLAC",
    _STRATEGY_UNWILD_YEAR: "{artist} {title} {year}",
    _STRATEGY_UNWILD_RG_YEAR: "{artist} {title} {rg_year}",
    _STRATEGY_CATALOG_NUMBER: "{artist} {catno}",
    _STRATEGY_COMPILATION_SERIES: "{title}",
    _STRATEGY_SELFTITLED_ARTIST_TRACK_0: "{artist} {track_0}",
    _STRATEGY_SELFTITLED_ARTIST_TRACK_0_FLAC: "{artist} {track_0} FLAC",
    _STRATEGY_SELFTITLED_ARTIST_YEAR: "{artist} {year}",
}


def query_template_for_strategy(strategy: str | None) -> str | None:
    """Return the operator-readable template shape for ``strategy``.

    Pure mapping — no I/O. Used by the search-log writer (U11 R27) to
    persist a per-row template label so the operator dashboard can
    group searches by shape (e.g. how often does the ``{artist} {title}
    FLAC`` template find a match vs. ``{artist} {catno}``).

    The numeric suffix on track-slot families (``track_<idx>_artist``
    and ``va_track_artist_<idx>``) is collapsed to the family template
    (``{artist} {track_N}`` / ``{track_artist} {track_N}``) because the
    slot index is plan-position metadata, not part of the shape — the
    operator wants to know "how do per-track queries perform" as one
    bucket.

    Unknown labels are echoed back rather than collapsed to ``None`` so
    new strategies surface in triage even before this mapping catches
    up. ``None`` / ``""`` input returns ``None`` so callers can pass the
    raw column value without pre-checking.
    """
    if not strategy:
        return None
    template = _QUERY_TEMPLATE_BY_STRATEGY.get(strategy)
    if template is not None:
        return template
    if strategy.startswith("track_") and strategy.endswith("_artist"):
        return "{artist} {track_N}"
    if strategy.startswith(_STRATEGY_VA_TRACK_ARTIST_PREFIX):
        return "{track_artist} {track_N}"
    return strategy


# PR2 U8 R3 — bumped from 3 to 4 so the non-VA mix emits
# ``track_0_artist`` through ``track_3_artist``. Distinctiveness ranking
# (U7) picks the top-4 distinctive tracks.
MAX_TRACK_SLOTS_PER_PLAN = 4
# PR2 U8 R13 — VA per-track-artist slots use a smaller cap (the per-track
# artist query is the natural shape for VA where every track has a
# different artist; cap at 3 to keep the plan bounded).
MAX_VA_TRACK_ARTIST_SLOTS = 3
# PR2 U8 R2 — catalog_number slot length cutoff. Numeric-only or very
# short values (e.g. "100") match too broadly to be useful; require at
# least 4 chars so values like "STRMRT-001" produce a high-precision
# search and "100" does not. Working baseline from the plan's deferred
# question.
CATALOG_NUMBER_MIN_LENGTH = 4
# Compilation-series detector regex (R13). Matches "Vol 1", "Vol. 1",
# "Volume 100", "#100" anywhere in the title (case-insensitive). When
# the album title contains a volume marker, the VA mix emits a
# ``compilation_series`` slot so anthology series like "Now That's What
# I Call Music #100" get a clean handle separate from generic title
# queries.
_COMPILATION_SERIES_RE = re.compile(
    r"(?:vol(?:ume)?\.?\s*\d+|#\s*\d+)",
    re.IGNORECASE,
)
# Format-hint tokens appended to the un-wildcarded literal query to bait
# peers who file their lossless rips with the format tag in the directory
# name. Tokens stay literal — no wildcarding, no short-token drop, no
# low-entropy normalization. Soulseek's distributed search caps at 4
# tokens, so we pre-cap the body to MAX_SEARCH_TOKENS-1 to make room.
_FORMAT_HINT_FLAC = "FLAC"


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

    ``is_va_compilation`` and ``catalog_number`` are PR2 U8 additions
    (R2/R13). They drive the VA branch in ``generate_search_plan`` and
    the ``catalog_number`` slot. ``track_artists`` is the per-track
    artist credit list resolved by U2's field resolver; non-empty only
    for VA-detected requests where ``resolve_track_artists`` populated
    rows in ``album_tracks.track_artist``. Defaults make these fields
    backwards-compatible for in-tree tests that construct snapshots
    directly.
    """

    artist_name: str
    title: str
    year: str | None
    track_titles: tuple[str, ...]
    redownload: bool = False
    prepend_artist: bool = False
    # First-release year of the MB release group. When known and
    # different from ``year``, the generator emits an extra
    # ``unwild_rg_year`` slot so reissues find original-pressing peers
    # on Soulseek. NULL means no extra slot.
    release_group_year: int | None = None
    # PR2 U8 (R13): true when the request was flagged as a Various
    # Artists compilation at enqueue (or by U3 backfill). Routes the
    # generator to ``_generate_va_plan`` which drops the artist-driven
    # slots (default/literal/literal_flac collapse for VA — no
    # discriminating artist axis) and substitutes per-track-artist
    # queries.
    is_va_compilation: bool = False
    # PR2 U8 (R2): label catalog number resolved by U2's resolver.
    # Drives the ``catalog_number`` slot when present and >= 4 chars.
    catalog_number: str | None = None
    # PR2 U8 (R13): per-track artist credits in the same order as
    # ``track_titles``. ``None`` entries mean the resolver couldn't
    # determine that track's artist (recorded as
    # ``unresolved_field_missing_upstream`` by the resolver service).
    # Empty tuple is the non-VA default. The VA plan generator uses the
    # picker (U7) on this list to choose distinctive (artist, title)
    # pairs for the per-track-artist slots.
    track_artists: tuple[str | None, ...] = ()


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
    """Return the lowercased stopword tokens that appear in any source list.

    Used purely for plan-level provenance: we want to record that the
    generator dropped (e.g.) `the` from a candidate, even though the
    drop happens inside `_normalize_query_tokens`. The presence check is
    case-insensitive.

    Reads ``STOPWORDS`` directly — this module is the canonical owner of
    the constant, so the AST audit's external-readers rule does not
    apply. Single-pass membership check (review #13): the earlier
    two-pass via ``strip_stopwords`` existed only to keep ``STOPWORDS``
    a private read for external modules.
    """
    dropped: set[str] = set()
    for tokens in token_lists:
        for tok in tokens:
            key = tok.lower()
            if key in STOPWORDS:
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


def _build_query(
    artist: str,
    title: str,
    *,
    prepend_artist: bool,
    wildcard: bool,
    max_tokens: int = MAX_SEARCH_TOKENS,
) -> str | None:
    """Build a slot query from artist + title.

    Shared pipeline for the ``default`` slot (wildcard=True), the
    ``literal`` slot, and the bodies of format-hint and year-anchored
    slots (``literal_flac``, ``unwild_year``, ``unwild_rg_year``)
    (wildcard=False):

      * Strip special chars.
      * Low-entropy normalization ("the/you/from/and" dropped) on both
        sides.
      * Drop title tokens that duplicate artist tokens
        (case-insensitive on the un-wildcarded form).
      * ``Various Artists`` → no artist tokens.
      * When ``wildcard=True``, artist tokens are wildcarded for ban
        bypass.
      * Cap longest-first to ``max_tokens``.
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

    if wildcard:
        artist_tokens = wildcard_artist_tokens(artist_tokens)

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
    track_titles = list(snapshot.track_titles)
    prepend_artist = snapshot.prepend_artist

    candidates: list[_Candidate] = []

    # 1. Default slot — wildcarded artist + title, no short-token drop.
    default_query = _build_query(
        artist, title, prepend_artist=prepend_artist, wildcard=True,
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
    literal_query = _build_query(
        artist, title, prepend_artist=prepend_artist, wildcard=False,
    )
    # Body capped at MAX_SEARCH_TOKENS - 1 so format-hint slots can
    # append their hint token without losing it to the slskd 4-token cap.
    literal_body_for_hint = _build_query(
        artist, title,
        prepend_artist=prepend_artist,
        wildcard=False,
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

    # 3. literal_flac slot — unconditional (R7). PR2 U8 (R1) retired
    #    ``literal_lossless`` here: post-deploy data showed it produced
    #    1 successful match across 2,093 runs (13% of search volume).
    flac_query = _append_format_hint(literal_body_for_hint, _FORMAT_HINT_FLAC)
    if flac_query:
        candidates.append(_Candidate(
            strategy=_STRATEGY_LITERAL_FLAC,
            repeat_group=_STRATEGY_LITERAL_FLAC,
            query=flac_query,
            omit_reason=None,
            extra_provenance={"format_hint": _FORMAT_HINT_FLAC},
        ))
    else:
        candidates.append(_Candidate(
            strategy=_STRATEGY_LITERAL_FLAC,
            repeat_group=_STRATEGY_LITERAL_FLAC,
            query=None,
            omit_reason="empty_literal_query",
            extra_provenance={"format_hint": _FORMAT_HINT_FLAC},
        ))

    # 5. unwild_year slot — when release `year` is known.
    candidates.append(
        _unwild_year_candidate(snapshot, prepend_artist=prepend_artist),
    )

    # 6. unwild_rg_year slot — emit when rg_year is known, year is
    #    known, the two differ, and the literal body exists. Otherwise
    #    emit one omission with a precise reason. Shared with VA via
    #    the prepend_artist kwarg.
    candidates.append(
        _unwild_rg_year_candidate(snapshot, prepend_artist=prepend_artist),
    )

    # 7. catalog_number slot (PR2 U8 R2) — emit when the resolved
    #    catalog number is non-empty and meets the length cutoff.
    #    Catalog numbers like "STRMRT-001" produce high-precision
    #    searches; very short / numeric-only values match too broadly.
    candidates.append(
        _catalog_number_candidate(snapshot, prepend_artist=prepend_artist),
    )

    # 8. Per-track slots — artist-prepended (R8). Multi-track only.
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


def _catalog_number_candidate(
    snapshot: ReleaseSnapshot,
    *,
    prepend_artist: bool,
) -> _Candidate:
    """Build the ``catalog_number`` slot candidate (PR2 U8 R2).

    Shared by the non-VA and VA branches — both prefer high-precision
    catalog-number queries when one is available. The slot is omitted
    with an explicit reason in three cases:

      * ``catalog_number_unknown`` — the resolver returned NULL.
      * ``catalog_number_too_short`` — value present but below
        ``CATALOG_NUMBER_MIN_LENGTH`` (e.g. "100").
      * ``empty_catalog_number_query`` — body collapsed to nothing
        after normalisation (defensive — would mean the artist is
        also empty and the catno is whitespace-only).
    """
    raw = snapshot.catalog_number or ""
    catno = raw.strip()
    if not catno:
        return _Candidate(
            strategy=_STRATEGY_CATALOG_NUMBER,
            repeat_group=_STRATEGY_CATALOG_NUMBER,
            query=None,
            omit_reason="catalog_number_unknown",
        )
    if len(catno) < CATALOG_NUMBER_MIN_LENGTH:
        return _Candidate(
            strategy=_STRATEGY_CATALOG_NUMBER,
            repeat_group=_STRATEGY_CATALOG_NUMBER,
            query=None,
            omit_reason="catalog_number_too_short",
            extra_provenance={"catalog_number": catno},
        )
    query = _build_query(
        snapshot.artist_name, catno,
        prepend_artist=prepend_artist, wildcard=False,
    )
    if not query:
        return _Candidate(
            strategy=_STRATEGY_CATALOG_NUMBER,
            repeat_group=_STRATEGY_CATALOG_NUMBER,
            query=None,
            omit_reason="empty_catalog_number_query",
            extra_provenance={"catalog_number": catno},
        )
    return _Candidate(
        strategy=_STRATEGY_CATALOG_NUMBER,
        repeat_group=_STRATEGY_CATALOG_NUMBER,
        query=query,
        omit_reason=None,
        extra_provenance={"catalog_number": catno},
    )


def _unwild_year_candidate(
    snapshot: ReleaseSnapshot,
    *,
    prepend_artist: bool = False,
) -> _Candidate:
    """Build the ``unwild_year`` slot — shared between non-VA and VA.

    Year discriminates even when the artist axis is degenerate (every
    track is by a different artist). VA mix wants the year slot, but
    can't rely on the album-level literal body — for VA, the title is
    often a generic word ("Compilation") that collapses post-dedupe.

    ``prepend_artist`` defaults to False (VA-safe). Non-VA callers pass
    ``snapshot.prepend_artist`` to get the artist+title body. When
    ``title``/body is empty or year is unknown, returns the omission
    shape with a precise reason.
    """
    year = snapshot.year
    if not _year_is_known(year):
        return _Candidate(
            strategy=_STRATEGY_UNWILD_YEAR,
            repeat_group=_STRATEGY_UNWILD_YEAR,
            query=None,
            omit_reason="year_unknown",
        )
    assert year is not None
    body = _build_query(
        snapshot.artist_name, snapshot.title,
        prepend_artist=prepend_artist, wildcard=False,
    )
    if not body:
        return _Candidate(
            strategy=_STRATEGY_UNWILD_YEAR,
            repeat_group=_STRATEGY_UNWILD_YEAR,
            query=None,
            omit_reason="empty_literal_query",
            extra_provenance={"year": year[:4]},
        )
    return _Candidate(
        strategy=_STRATEGY_UNWILD_YEAR,
        repeat_group=_STRATEGY_UNWILD_YEAR,
        query=f"{body} {year[:4]}",
        omit_reason=None,
        extra_provenance={"year": year[:4]},
    )


def _unwild_rg_year_candidate(
    snapshot: ReleaseSnapshot,
    *,
    prepend_artist: bool = False,
) -> _Candidate:
    """Build the ``unwild_rg_year`` slot — shared between non-VA and VA.

    Emit a query only when rg_year is known, year is known, the two
    differ, and the body is non-empty. ``prepend_artist`` defaults to
    False (VA-safe); non-VA callers pass ``snapshot.prepend_artist``.

    The ``int(rg_year) <= 0`` branch is defensive only —
    ``_release_group_year_from_value`` (lib/release_snapshot.py) already
    normalises non-positive ints to None, so the path is unreachable in
    production. Kept for the same reason both inline copies kept it.
    """
    rg_year = snapshot.release_group_year
    year = snapshot.year
    year_known = _year_is_known(year)
    rg_provenance: dict[str, Any] = (
        {"release_group_year": int(rg_year)}
        if rg_year is not None else {}
    )
    if rg_year is None:
        return _Candidate(
            strategy=_STRATEGY_UNWILD_RG_YEAR,
            repeat_group=_STRATEGY_UNWILD_RG_YEAR,
            query=None,
            omit_reason="release_group_year_unknown",
        )
    if not year_known:
        return _Candidate(
            strategy=_STRATEGY_UNWILD_RG_YEAR,
            repeat_group=_STRATEGY_UNWILD_RG_YEAR,
            query=None,
            omit_reason="year_unknown",
            extra_provenance=rg_provenance,
        )
    if str(rg_year) == (year[:4] if year else ""):
        return _Candidate(
            strategy=_STRATEGY_UNWILD_RG_YEAR,
            repeat_group=_STRATEGY_UNWILD_RG_YEAR,
            query=None,
            omit_reason="release_group_year_matches_year",
            extra_provenance=rg_provenance,
        )
    if int(rg_year) <= 0:
        return _Candidate(
            strategy=_STRATEGY_UNWILD_RG_YEAR,
            repeat_group=_STRATEGY_UNWILD_RG_YEAR,
            query=None,
            omit_reason="release_group_year_unknown",
            extra_provenance=rg_provenance,
        )
    body = _build_query(
        snapshot.artist_name, snapshot.title,
        prepend_artist=prepend_artist, wildcard=False,
    )
    if not body:
        return _Candidate(
            strategy=_STRATEGY_UNWILD_RG_YEAR,
            repeat_group=_STRATEGY_UNWILD_RG_YEAR,
            query=None,
            omit_reason="empty_literal_query",
            extra_provenance=rg_provenance,
        )
    return _Candidate(
        strategy=_STRATEGY_UNWILD_RG_YEAR,
        repeat_group=_STRATEGY_UNWILD_RG_YEAR,
        query=f"{body} {int(rg_year):04d}",
        omit_reason=None,
        extra_provenance=rg_provenance,
    )


def _generate_va_plan(
    snapshot: ReleaseSnapshot,
    config: SearchPlanConfig,
) -> tuple[list["_Candidate"], list[dict[str, Any]]]:
    """Build the candidate ladder for a Various Artists compilation (R13).

    The VA mix replaces the artist+title-driven slots (default / literal /
    literal_flac) with per-track-artist + per-track-title queries. For
    real VA, those tracks have different artists, so the per-track query
    is more discriminating than the album-level "Various Artists
    {Title}" shape — which collapses to bare title after the resolver
    strips the artist (see ``_per_track_candidates``).

    Plus a ``compilation_series`` slot when the album title matches
    ``Vol \\d+ | Volume \\d+ | #\\d+`` so anthology series like "Now
    That's What I Call Music #100" get a clean handle.

    Keeps ``unwild_year``, ``unwild_rg_year``, and ``catalog_number``
    slots from the normal path because year + catno discriminate even
    when the artist axis is degenerate.

    Degradation case: when ``is_va_compilation=True`` but every entry
    in ``track_artists`` is NULL or empty (the resolver couldn't
    determine track-level artists — e.g. Discogs payload lacked
    per-track ``artists`` entries), the VA branch emits whatever
    year/catno slots are available and tags an omission
    ``{"strategy": "va_track_artist_*", "reason":
    "no_track_artists_resolved"}``. The plan stays useful instead of
    crashing.
    """
    candidates: list[_Candidate] = []
    track_omissions: list[dict[str, Any]] = []

    # 1. Per-track-artist slots — the heart of the VA mix.
    va_track_candidates = _build_va_track_artist_candidates(
        snapshot, MAX_VA_TRACK_ARTIST_SLOTS, track_omissions,
    )
    candidates.extend(va_track_candidates)

    # 2. compilation_series slot — when the title carries a volume
    #    marker. Operator-readable handle for anthology series.
    candidates.append(_compilation_series_candidate(snapshot))

    # 3. unwild_year (title + year, no artist).
    candidates.append(_unwild_year_candidate(snapshot))

    # 4. unwild_rg_year (title + rg_year, no artist).
    candidates.append(_unwild_rg_year_candidate(snapshot))

    # 5. catalog_number — shared with the non-VA branch.
    #    prepend_artist=False here: "Various Artists" gets dropped by
    #    ``_build_query`` anyway, but being explicit makes the intent
    #    clear (the catno alone IS the discriminator).
    candidates.append(
        _catalog_number_candidate(snapshot, prepend_artist=False),
    )

    return candidates, track_omissions


def _build_va_track_artist_candidates(
    snapshot: ReleaseSnapshot,
    max_slots: int,
    track_omissions: list[dict[str, Any]],
) -> list["_Candidate"]:
    """Build per-track-artist candidate slots for the VA branch (R13).

    Pairs each ``track_title`` with its corresponding ``track_artist``
    entry. Drops pairs where the track artist is NULL or empty (the
    resolver couldn't determine it). Ranks the remaining pairs by
    distinctiveness of the track title (U7) and emits up to
    ``max_slots`` ``<track_artist> <track_title>`` queries.

    When zero pairs have a resolved track artist, records a single
    ``no_track_artists_resolved`` omission so the operator triage
    surface can see the VA mix degraded.
    """
    titles = list(snapshot.track_titles)
    artists = list(snapshot.track_artists)
    # Pair each title with its resolved artist (or None when the
    # resolver came up empty for that track). When ``track_artists``
    # is shorter than ``track_titles`` (legacy snapshots, partial
    # resolution edge), missing positions default to None.
    paired: list[tuple[int, str, str]] = []
    for idx, title in enumerate(titles):
        ta = artists[idx] if idx < len(artists) else None
        if ta is None:
            continue
        ta_clean = ta.strip()
        if not ta_clean:
            continue
        if not title:
            continue
        paired.append((idx, ta_clean, title))

    if not paired:
        track_omissions.append({
            "strategy": "va_track_artist_*",
            "reason": "no_track_artists_resolved",
        })
        return []

    # Rank by distinctiveness of the raw title (consistent with the
    # non-VA per-track ranker — distinctiveness is the title's
    # property; the artist just goes along).
    ranked = sorted(
        paired,
        key=lambda p: (-score_track_distinctiveness(p[2]), p[0]),
    )
    chosen = ranked[:max_slots]
    omitted = ranked[max_slots:]
    for src_idx, _ta, title in omitted:
        track_omissions.append({
            "strategy": "va_track_artist_excess",
            "reason": "exceeded_max_va_track_artist_slots",
            "source_track_index": src_idx,
            "title": title,
        })

    out: list[_Candidate] = []
    for slot_idx, (src_idx, ta, title) in enumerate(chosen):
        # Build the per-track query: literal track artist + literal
        # track title, capped at MAX_SEARCH_TOKENS. The track artist
        # tokens are NOT wildcarded — VA per-track artists are
        # typically individually-named, so the discriminating value
        # IS the literal artist name. Reuses ``_build_query`` for
        # uniform normalisation (stopword drop, dedupe, cap).
        query = _build_query(
            ta, title, prepend_artist=True, wildcard=False,
        )
        label = f"{_STRATEGY_VA_TRACK_ARTIST_PREFIX}{slot_idx}"
        if not query:
            track_omissions.append({
                "strategy": label,
                "reason": "empty_va_track_artist_query",
                "source_track_index": src_idx,
                "track_artist": ta,
                "title": title,
            })
            continue
        out.append(_Candidate(
            strategy=label,
            repeat_group=label,
            query=query,
            omit_reason=None,
            extra_provenance={
                "source_track_index": src_idx,
                "track_slot_index": slot_idx,
                "track_artist": ta,
            },
        ))
    return out


def _compilation_series_candidate(snapshot: ReleaseSnapshot) -> _Candidate:
    """Build the VA ``compilation_series`` slot.

    Triggers when the album title matches the volume-marker regex
    (``Vol 1``, ``Vol. 1``, ``Volume 100``, ``#100``). Emits
    ``<title>`` capped to MAX_SEARCH_TOKENS so the volume marker is
    preserved; the marker is what makes the search discriminating for
    anthology series.
    """
    title = snapshot.title or ""
    if not _COMPILATION_SERIES_RE.search(title):
        return _Candidate(
            strategy=_STRATEGY_COMPILATION_SERIES,
            repeat_group=_STRATEGY_COMPILATION_SERIES,
            query=None,
            omit_reason="no_volume_marker",
        )
    # Use a literal title-only query — the volume marker is in the
    # title and the artist (Various Artists) collapses anyway.
    body = _build_query(
        snapshot.artist_name, title,
        prepend_artist=False, wildcard=False,
    )
    if not body:
        return _Candidate(
            strategy=_STRATEGY_COMPILATION_SERIES,
            repeat_group=_STRATEGY_COMPILATION_SERIES,
            query=None,
            omit_reason="empty_compilation_series_query",
        )
    return _Candidate(
        strategy=_STRATEGY_COMPILATION_SERIES,
        repeat_group=_STRATEGY_COMPILATION_SERIES,
        query=body,
        omit_reason=None,
        extra_provenance={"compilation_series_match": True},
    )


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
    if first_track_query is not None and first_track_idx is not None:
        candidates.append(_Candidate(
            strategy=_STRATEGY_SELFTITLED_ARTIST_TRACK_0,
            repeat_group=_STRATEGY_SELFTITLED_ARTIST_TRACK_0,
            query=first_track_query,
            omit_reason=None,
            extra_provenance={"source_track_index": first_track_idx},
        ))
    else:
        candidates.append(_Candidate(
            strategy=_STRATEGY_SELFTITLED_ARTIST_TRACK_0,
            repeat_group=_STRATEGY_SELFTITLED_ARTIST_TRACK_0,
            query=None,
            omit_reason=(
                "no_runnable_track_queries" if track_titles
                else "no_tracks"
            ),
        ))

    # 2. selftitled_artist_track_0_flac
    if first_track_query_body is not None and first_track_idx is not None:
        candidates.append(_Candidate(
            strategy=_STRATEGY_SELFTITLED_ARTIST_TRACK_0_FLAC,
            repeat_group=_STRATEGY_SELFTITLED_ARTIST_TRACK_0_FLAC,
            query=f"{first_track_query_body} {_FORMAT_HINT_FLAC}",
            omit_reason=None,
            extra_provenance={
                "source_track_index": first_track_idx,
                "format_hint": _FORMAT_HINT_FLAC,
            },
        ))
    else:
        candidates.append(_Candidate(
            strategy=_STRATEGY_SELFTITLED_ARTIST_TRACK_0_FLAC,
            repeat_group=_STRATEGY_SELFTITLED_ARTIST_TRACK_0_FLAC,
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


# Title fragments / patterns that signal a generic / non-distinctive track
# title. Used by `score_track_distinctiveness` to penalise tracks that
# would make poor fallback queries (every soundtrack has a "Motion Picture
# Soundtrack" or "Theme"; every album-without-track-titles has "Untitled").
# Set is intentionally small — distinctiveness scoring is deliberately
# dumb (no IDF, no corpus statistics, no ML). Bump
# `SEARCH_PLAN_GENERATOR_ID` whenever this set changes — track ordering
# is generator output.
GENERIC_TITLE_TOKENS: frozenset[str] = frozenset({
    "intro", "outro", "interlude", "untitled", "overture", "theme",
    "motion", "picture", "soundtrack", "prelude", "reprise",
})
# "Track N" is a separate pattern (regex), handled by
# `score_track_distinctiveness` directly.
_GENERIC_TRACK_NUMBER_RE = re.compile(r"^track\s*\d+$", re.IGNORECASE)


def score_track_distinctiveness(title: str) -> float:
    """Score how distinctive ``title`` is as a fallback-query token source.

    Higher = more distinctive (longer non-generic tokens score best).
    Pure function — no DB, no corpus stats. Tuned empirically against
    Kid A's track list (canonical bad case: "Motion Picture Soundtrack"
    must score lower than "Everything in Its Right Place").

    Formula: ``len(longest_non_generic_token) * num_non_generic_tokens``.
    Tokens in ``GENERIC_TITLE_TOKENS`` AND tokens that match the
    ``Track \\d+`` regex are excluded from both factors. Stopwords (the,
    you, from, and) are NOT specially demoted — they're not distinctive
    themselves but they don't poison a title's score either.

    Returns ``0.0`` for empty titles, all-generic titles, and "Track 7"
    style placeholders. Score is ``float`` (not ``int``) so future
    fractional tiebreaks don't require re-typing the signature.
    """
    if not title:
        return 0.0
    if _GENERIC_TRACK_NUMBER_RE.match(title.strip()):
        return 0.0
    tokens = title.split()
    non_generic = [
        t for t in tokens
        if t.lower() not in GENERIC_TITLE_TOKENS
    ]
    if not non_generic:
        return 0.0
    longest = max(len(t) for t in non_generic)
    return float(longest * len(non_generic))


def _build_track_candidates(
    track_titles: list[str],
    per_track_pairs: list[tuple[int, str]],
    max_track_slots: int,
    track_omissions: list[dict[str, Any]],
    *,
    slot_label_suffix: str,
) -> list["_Candidate"]:
    """Rank per-track pairs and emit up to ``max_track_slots`` candidates.

    Ranking is driven by ``score_track_distinctiveness`` on the RAW
    source-track title (looked up via the pair's source index). Raw
    titles, not the rendered queries, so a title like "Motion Picture
    Soundtrack" is penalised even when the rendered query
    ("*adiohead Motion Picture Soundtrack") happens to be long.
    Tiebreaks fall back to rendered char-count desc and source-track
    index asc so the order stays deterministic.

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
            -score_track_distinctiveness(track_titles[pair[0]]),
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

    PR2 U8 (2026-05-25) restructured the slot mix again. Normal
    request (year known, release_group_year populated and differs from
    year, multi-track, non-VA):

      1. default              wildcarded artist+title, no short-drop
      2. literal              un-wildcarded artist+title, no year
      3. literal_flac         literal + " FLAC"
         (``literal_lossless`` retired — 1 match in 2,093 runs)
      4. unwild_year          literal + " <year>"
      5. unwild_rg_year       literal + " <rg_year>" (conditional)
      6. catalog_number       artist + " <catno>" (conditional, R2)
      7-10. track_<idx>_artist artist-prepended per-track fallback
         (max 4 — R3 bumped from 3)

    VA-detected requests (``is_va_compilation=True``, R13) skip the
    artist-driven slots and substitute per-track-artist queries:

      1-3. va_track_artist_<idx> <track_artist> <track_title>
      4. compilation_series   title (conditional — volume marker)
      5. unwild_year          title + " <year>" (conditional)
      6. unwild_rg_year       title + " <rg_year>" (conditional)
      7. catalog_number       title + " <catno>" (conditional)

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

    # PR2 U8 (R13): VA detection runs before self-titled because a
    # request flagged as VA shouldn't get the self-titled mix even if
    # the artist == title (e.g. an unlikely "Various Artists / Various
    # Artists" payload). VA is the strongest dispatch signal.
    if snapshot.is_va_compilation:
        candidates, track_omissions = _generate_va_plan(
            snapshot, config,
        )
    elif selftitled:
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
    if snapshot.is_va_compilation:
        base_provenance["is_va_compilation"] = True
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
