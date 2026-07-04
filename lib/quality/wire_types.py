"""Harness/DB wire-boundary Structs: harness items, candidate summaries, validation results.

Extracted verbatim from the monolithic ``lib/quality.py`` (issue #477).
Pure move: every definition is AST-identical to the original.
"""

from typing import Optional, Sequence
import msgspec


# ---------------------------------------------------------------------------
# Harness wire-boundary types — msgspec.Struct with strict `str` validation.
#
# Beets' Discogs plugin returns integer album_id / track_id values; beets'
# MusicBrainz plugin returns UUID strings. Every downstream consumer in the
# pipeline compares these against DB-stored TEXT release IDs with `==`, so a
# mixed-type wire format silently fails (that was the "mbid_not_found" bug
# for every Discogs validation — fixed in PR #98, guarded here).
#
# The harness normalises IDs to str via `_id_str` in beets_harness.py before
# emitting. These Structs declare `str` and msgspec validates at decode time
# — an int on the wire raises `msgspec.ValidationError` inside
# `lib/beets.py::beets_validate`, which surfaces it as `result.error`. Loud
# failure instead of a silent miss.
#
# Why msgspec.Struct (not @dataclass):
#   1. msgspec.json.decode(blob, type=Foo) validates types at the boundary.
#      @dataclass has no runtime schema enforcement; you'd have to hand-roll
#      a from_dict that coerces every field, and that coercion becomes
#      defensive cruft downstream.
#   2. Wire-shape changes are detected by tests, not by production bugs.
#   3. Near-zero overhead.
#
# Every wire-boundary type in this module is a ``msgspec.Struct`` — the
# harness ones below PLUS ``ImportResult`` / ``PostflightInfo`` /
# ``ConversionInfo`` / ``SpectralDetail`` / ``AudioQualityMeasurement`` /
# ``MovedSibling`` / ``ValidationResult`` further down, which all round-trip
# through ``download_log`` JSONB and/or subprocess stdout (issue #141
# unified on one encoder: ``msgspec.json.encode``). Types constructed
# entirely from in-process Python (e.g. ``QualityRankConfig``,
# ``CratediggerConfig``) stay as ``@dataclass`` — their inputs are already
# typed, not a wire protocol.
# ---------------------------------------------------------------------------


class HarnessItem(msgspec.Struct):
    """Local file as seen by the beets harness during matching."""
    path: str = ""
    title: str = ""
    artist: str = ""
    album: str = ""
    track: int = 0
    disc: int = 0
    length: float = 0.0
    bitrate: Optional[int] = None
    format: str = ""
    mb_trackid: str = ""
    data_source: str = ""


class HarnessTrackInfo(msgspec.Struct):
    """MusicBrainz / Discogs track info as seen by the beets harness.

    `track_id` and `release_track_id` are declared `str`; msgspec raises
    ValidationError if beets leaks an int through (regression guard for
    the PR #98 bug).
    """
    title: str = ""
    artist: str = ""
    index: Optional[int] = None
    medium: Optional[int] = None
    medium_index: Optional[int] = None
    medium_total: Optional[int] = None
    length: float = 0.0
    track_id: str = ""
    release_track_id: str = ""
    track_alt: Optional[str] = None
    disctitle: Optional[str] = None
    data_source: str = ""


class TrackMapping(msgspec.Struct):
    """Which local item matched which MB/Discogs track."""
    item: HarnessItem = msgspec.field(default_factory=HarnessItem)
    track: HarnessTrackInfo = msgspec.field(default_factory=HarnessTrackInfo)


class CandidateSummary(msgspec.Struct, rename={"mbid": "album_id"}):
    """Full beets candidate match data for audit logging.

    Stores everything the harness sends — every field from AlbumInfo,
    the distance breakdown, track mapping, and extra items/tracks with
    full detail.

    Wire ↔ attribute mapping: the harness emits the JSON key `album_id`
    (beets' own field name); this Struct exposes it as `.mbid` for
    continuity with existing Python callers. msgspec handles both the
    rename and the strict `str` validation.

    JSONB format note: rows written by `ValidationResult.to_json()`
    AFTER commit 48914ca (PR #100) use the key `album_id`; earlier rows
    use `mbid`. No production code round-trips old rows back through
    `ValidationResult.from_dict` (web routes parse the raw dict), so
    this is a forward-only format change. If you ever need to decode
    pre-48914ca rows via msgspec, either pre-rename the key or add
    `"mbid"` as a secondary key on the Struct.
    """
    # Core identity
    mbid: str = ""
    artist: str = ""
    album: str = ""
    distance: float = 0.0
    distance_breakdown: dict[str, float] = {}
    is_target: bool = False
    # AlbumInfo metadata
    albumdisambig: str = ""
    year: Optional[int] = None
    original_year: Optional[int] = None
    country: Optional[str] = None
    label: Optional[str] = None
    catalognum: Optional[str] = None
    media: Optional[str] = None
    mediums: Optional[int] = None
    albumtype: Optional[str] = None
    albumtypes: list[str] = []
    albumstatus: Optional[str] = None
    releasegroup_id: str = ""
    release_group_title: str = ""
    va: bool = False
    language: Optional[str] = None
    script: Optional[str] = None
    data_source: str = ""
    barcode: str = ""
    asin: str = ""
    # Tracks and mapping
    track_count: int = 0
    tracks: list[HarnessTrackInfo] = []
    mapping: list[TrackMapping] = []
    extra_items: list[HarnessItem] = []
    extra_tracks: list[HarnessTrackInfo] = []


class ChooseMatchMessage(msgspec.Struct):
    """Full schema of the harness `choose_match` JSON message. Decoded in
    one shot at the wire boundary (`lib/beets.py::beets_validate`) via
    `msgspec.convert(msg, type=ChooseMatchMessage)` — any type drift in
    any nested field raises `msgspec.ValidationError` immediately.
    """
    task_id: int = 0
    path: str = ""
    cur_artist: str = ""
    cur_album: str = ""
    item_count: int = 0
    items: list[HarnessItem] = []
    recommendation: str = "none"
    candidate_count: int = 0
    candidates: list[CandidateSummary] = []


class ValidationResult(msgspec.Struct):
    """Structured result from beets validation + audio integrity check.

    Accumulated through the validation pipeline:
    1. beets_validate() populates candidates, distance, scenario
    2. Audio integrity check may set scenario=audio_corrupt + corrupt_files
    3. cratedigger.py populates source info (username, folder, failed_path, denylisted)

    Stored in download_log.validation_result (JSONB) for complete auditability.
    Wire-boundary type per ``.claude/rules/code-quality.md``: encode via
    ``msgspec.json.encode``, decode via ``msgspec.convert`` — symmetric.
    """
    valid: bool = False
    distance: Optional[float] = None
    scenario: Optional[str] = None
    detail: Optional[str] = None
    mbid_found: bool = False
    target_mbid: Optional[str] = None
    candidate_count: int = 0
    candidates: list[CandidateSummary] = []
    # Local file info (from harness choose_match items)
    items: list[dict] = []
    local_track_count: Optional[int] = None
    recommendation: Optional[str] = None        # beets confidence: "strong", "medium", "none"
    path: Optional[str] = None                  # album path being validated
    # Source info (populated by cratedigger.py)
    soulseek_username: Optional[str] = None
    download_folder: Optional[str] = None
    failed_path: Optional[str] = None
    source_dirs: list[str] = []
    denylisted_users: list[str] = []
    # Audio integrity
    corrupt_files: list[str] = []
    error: Optional[str] = None
    # Bad-audio-hash gate (pre-import defense, plan 2026-04-29-005 / U5).
    # Populated when ``scenario == "bad_audio_hash"``: the matched
    # ``bad_audio_hashes.id`` and the candidate track that hashed to it.
    matched_bad_hash_id: Optional[int] = None
    matched_bad_track_path: Optional[str] = None

    def to_json(self) -> str:
        """Serialize to JSON string via msgspec.json.encode."""
        return msgspec.json.encode(self).decode()

    @classmethod
    def from_dict(cls, d: dict) -> "ValidationResult":
        """Construct from a dict — strict-typed decode at the boundary.

        Every nested ``CandidateSummary`` / ``TrackMapping`` / ``HarnessItem``
        / ``HarnessTrackInfo`` is validated against its declared types.
        """
        return msgspec.convert(d, type=cls)

    @classmethod
    def from_json(cls, s: str) -> "ValidationResult":
        """Deserialize from JSON string."""
        return msgspec.json.decode(s.encode(), type=cls)


class CandidateScore(msgspec.Struct):
    """Forensic record of one (user, dir, filetype) candidate's match score.

    Wire-boundary type — written into ``search_log.candidates`` JSONB by
    ``PipelineDB.log_search`` and decoded by U7 readers (CLI + web UI).
    Encode via ``msgspec.json.encode``; decode via
    ``msgspec.convert(blob, type=list[CandidateScore])`` — symmetric strict
    validation at both boundaries per ``.claude/rules/code-quality.md`` §
    Wire-boundary types.

    Construct via keyword arguments only. ``check_for_match`` builds the
    full-score variant when ``album_match`` runs; the count-gate-failure
    variant is the cheap zero-score record (``matched_tracks=0``,
    ``avg_ratio=0.0``, ``missing_titles=[]``) so the forensic blob still
    captures peers that had a sub-count audio file count.

    ``pre_filter_skip`` (U2 of search-plan-entropy): True for the
    sampled flagged rows ``check_for_match`` emits when the asymmetric
    pre-filter (``search_count > 2 * track_num``) rejected the dir
    before any browse. Sample rows carry ``matched_tracks=0``,
    ``avg_ratio=0.0``, ``missing_titles=[]`` and the cached
    ``file_count`` from the search response so operators can see
    which peers are noisy. Defaults to ``False`` so historic blobs
    decode unchanged (msgspec strict decode tolerates missing fields
    only when a default is declared — symmetric encode keeps the field
    on every row, including ``False`` on scored / sub-count rows).
    """
    username: str
    dir: str
    filetype: str
    matched_tracks: int
    total_tracks: int
    avg_ratio: float
    missing_titles: list[str]
    file_count: int
    pre_filter_skip: bool = False


def top_candidates(
    candidates: Sequence[CandidateScore], limit: int = 20,
) -> list[CandidateScore]:
    """Return the top-N candidates sorted by (matched_tracks, avg_ratio) DESC.

    Pure helper — no DB, no I/O. Single source of truth for the candidate
    ranking used by:

    - ``cratedigger._log_search_result`` (top-20 written to
      ``search_log.candidates`` JSONB)
    - ``web/routes/pipeline.py:_build_last_search_payload`` (top-3 surfaced
      on ``/api/pipeline/<id>``)
    - ``scripts/pipeline_cli.py:_render_search_forensics_summary`` (top-3 in
      ``pipeline-cli show <id>``)

    Sorting by matched_tracks first surfaces the closest peers; avg_ratio is
    the secondary tiebreak so a 24/26 dir with high ratio beats a 24/26 dir
    with low ratio.

    U2 of search-plan-entropy: ``pre_filter_skip`` flagged rows
    (``matched_tracks=0``, ``avg_ratio=0.0``) sink to the bottom of
    this ordering naturally. Callers that want a guaranteed mix of
    scored + skip-sample rows should split the input first
    (see ``top_candidates_with_skip_split``).
    """
    return sorted(
        candidates,
        key=lambda c: (c.matched_tracks, c.avg_ratio),
        reverse=True,
    )[:limit]


def top_candidates_with_skip_split(
    candidates: Sequence[CandidateScore],
    *,
    scored_limit: int = 15,
    skip_limit: int = 5,
) -> list[CandidateScore]:
    """Return scored top-N + sampled pre-filter-skip rows.

    Splits ``candidates`` into scored vs pre-filter-skip; ranks scored
    via ``top_candidates``, preserves visit order for the skip sample.
    Default 15 + 5 keeps the JSONB blob the same size as the pre-split
    cap of 20.
    """
    scored = [c for c in candidates if not c.pre_filter_skip]
    skipped = [c for c in candidates if c.pre_filter_skip]
    return (
        top_candidates(scored, limit=scored_limit)
        + list(skipped[:skip_limit])
    )
