"""Harness/DB wire-boundary Structs: harness items, candidate summaries, validation results.

Extracted verbatim from the monolithic ``lib/quality.py`` (issue #477).
Pure move: every definition is AST-identical to the original.
"""

from collections.abc import Sequence
from typing import Self

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
    """Local file as seen by the beets harness during matching.

    ``path`` is required (#1278 item 8): ``candidate_audio_coverage``
    keys every coverage decision on it, and every constructor — including
    ``harness/import_one.py``'s path-only filesystem fallback — supplies
    it. The rest stays defaulted (``length`` is consulted only inside the
    coalesced-composite check, like ``HarnessTrackInfo.length`` below);
    the wire key set is audited in
    ``tests/test_harness_wire_contract_audit.py``.
    """
    path: str
    title: str = ""
    artist: str = ""
    album: str = ""
    track: int = 0
    disc: int = 0
    length: float = 0.0
    bitrate: int | None = None
    format: str = ""
    mb_trackid: str = ""
    data_source: str = ""


class HarnessTrackInfo(msgspec.Struct):
    """MusicBrainz / Discogs track info as seen by the beets harness.

    `track_id` and `release_track_id` are declared `str`; msgspec raises
    ValidationError if beets leaks an int through (regression guard for
    the PR #98 bug).

    Every field stays defaulted (#1278 item 8). The two Discogs fields
    below carry documented semantic defaults. ``length`` IS a decision
    input, but only for coalesced Discogs components — where
    ``discogs_indexed_component_count`` (default 1, the no-coalescing
    semantics) gates the check — so its 0.0 default is inert outside that
    path, and a dropped key is caught by the key-set audit in
    ``tests/test_harness_wire_contract_audit.py`` rather than at decode.
    """
    title: str = ""
    artist: str = ""
    index: int | None = None
    medium: int | None = None
    medium_index: int | None = None
    medium_total: int | None = None
    length: float = 0.0
    track_id: str = ""
    release_track_id: str = ""
    track_alt: str | None = None
    disctitle: str | None = None
    data_source: str = ""
    # Cratedigger's Discogs compatibility layer sets this only when Beets
    # coalesces multiple flat indexed entries into one physical-track slot.
    # It lets the import boundary distinguish complete composite audio from
    # a file that contains only the first indexed component (#1183).
    discogs_indexed_component_count: int = 1
    # False when any indexed component lacks a positive parseable duration.
    # A coalesced physical file cannot prove complete-program coverage then.
    discogs_indexed_duration_complete: bool = True


class TrackMapping(msgspec.Struct):
    """Which local item matched which MB/Discogs track.

    Both halves are required: the harness emits every mapping entry with
    both keys, and a defaulted empty item/track pair is meaningless — a
    dropped key must raise at the decode boundary (#1278 item 8).
    """
    item: HarnessItem
    track: HarnessTrackInfo


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
    this is a forward-only format change. Such a row now fails loud at
    decode (`album_id` is required, below); if you ever need to decode
    pre-48914ca rows via msgspec, either pre-rename the key or add
    `"mbid"` as a secondary key on the Struct.

    Required/optional split (#1278 item 8): the fields a production
    decision path consumes are required, so a dropped/renamed wire key
    raises ``msgspec.ValidationError`` at decode instead of silently
    filling a default — `mbid` is release identity (the PR #98 incident
    class), `distance` the accept gate, `data_source` the Discogs
    second-pass decision in ``lib/beets.py``, `extra_tracks` the validity
    scenario in ``apply_candidate_scenario``, and `mapping`/`extra_items`
    (with `extra_tracks` again) the ``candidate_audio_coverage`` inputs.
    `tracks` and the rest of the metadata are audit-only — no production
    decision reads them — and keep defaults.
    ``tests/test_harness_wire_contract_audit.py`` pins both the split and
    the key-set equality with the harness serializers.
    """
    # Decision-consumed fields — required on the wire.
    mbid: str
    distance: float
    data_source: str
    mapping: list[TrackMapping]
    extra_items: list[HarnessItem]
    extra_tracks: list[HarnessTrackInfo]
    # Audit metadata — defaulted.
    tracks: list[HarnessTrackInfo] = []
    artist: str = ""
    album: str = ""
    distance_breakdown: dict[str, float] = {}
    # Lib-side annotation stamped after decode — never on the HARNESS
    # wire (the audit carves it out there), but it IS persisted in
    # ValidationResult JSONB and read back by web/classify.py and
    # web/wrong_match_file_service.py. Do not drop or omit-default it.
    is_target: bool = False
    albumdisambig: str = ""
    year: int | None = None
    original_year: int | None = None
    country: str | None = None
    label: str | None = None
    catalognum: str | None = None
    media: str | None = None
    mediums: int | None = None
    albumtype: str | None = None
    albumtypes: list[str] = []
    albumstatus: str | None = None
    releasegroup_id: str = ""
    release_group_title: str = ""
    va: bool = False
    language: str | None = None
    script: str | None = None
    barcode: str = ""
    asin: str = ""
    track_count: int = 0


class ChooseMatchMessage(msgspec.Struct):
    """Full schema of the harness `choose_match` JSON message. Decoded in
    one shot at the wire boundary (`lib/beets.py::beets_validate` and
    `harness/import_one.py`) via
    `msgspec.convert(msg, type=ChooseMatchMessage)` — any type drift in
    any nested field raises `msgspec.ValidationError` immediately.

    Every field is required (#1278 item 8): the message is decode-only in
    production and the harness emits every key unconditionally, so a
    missing key is drift, not a partial message — `candidates` or `items`
    silently reading as empty would be the worst shape of it.
    """
    task_id: int
    path: str
    cur_artist: str
    cur_album: str
    item_count: int
    items: list[HarnessItem]
    recommendation: str
    candidate_count: int
    candidates: list[CandidateSummary]


class HarnessSessionEvidence(msgspec.Struct):
    """What the harness session itself did, when it offered no match.

    Written by ``lib/beets.py::beets_validate`` on exactly the runs that end
    without a processed ``choose_match`` message — the rejection that used
    to persist nothing at all beyond one WARNING in the journal (issue
    #888). It records OBSERVATIONS only, never an inferred cause: which
    message types the harness sent, whether it announced a ``session_end``,
    and the tail of its stderr.

    Those three together separate the causes the observation is consistent
    with. A harness that died before starting sends nothing and leaves a
    traceback; one that ran and found no importable audio announces
    ``session_end`` with a clean stderr. Neither is asserted here — the
    evidence is recorded and the operator reads it.

    Wire-boundary type: persisted inside ``download_log.validation_result``
    JSONB via ``ValidationResult.to_json()``.
    """
    # Ordered, deduplicated ``type`` values seen on the harness's stdout.
    # Empty means the harness never emitted a single JSON message.
    message_types: list[str] = []
    # Whether the harness announced the end of its import session.
    session_end_seen: bool = False
    # Bounded TAIL of the harness's stderr. The tail is the diagnostic half
    # of a Python traceback — the exception line is at the bottom, which is
    # the same reason ``beets_validate`` logs stderr in full rather than
    # head-slicing it (the 2026-05-04 Psilodump crash).
    stderr_tail: str | None = None


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
    distance: float | None = None
    scenario: str | None = None
    detail: str | None = None
    mbid_found: bool = False
    target_mbid: str | None = None
    candidate_count: int = 0
    candidates: list[CandidateSummary] = []
    # Local file info (from harness choose_match items) — JSON-plain
    # projection (``msgspec.to_builtins``) of ``HarnessItem``, not the
    # Struct itself.
    items: list[dict[str, object]] = []
    local_track_count: int | None = None
    recommendation: str | None = None        # beets confidence: "strong", "medium", "none"
    path: str | None = None                  # album path being validated
    # Source info (populated by cratedigger.py)
    soulseek_username: str | None = None
    download_folder: str | None = None
    failed_path: str | None = None
    source_dirs: list[str] = []
    denylisted_users: list[str] = []
    # Audio integrity
    corrupt_files: list[str] = []
    error: str | None = None
    # Populated by ``beets_validate`` exactly when it returns without having
    # processed a ``choose_match`` — i.e. alongside ``scenario ==
    # "no_choose_match"``. ``None`` on every other result (issue #888).
    harness_session: HarnessSessionEvidence | None = None
    # Bad-audio-hash gate (pre-import defense, plan 2026-04-29-005 / U5).
    # Populated when ``scenario == "bad_audio_hash"``: the matched
    # ``bad_audio_hashes.id`` and the candidate track that hashed to it.
    matched_bad_hash_id: int | None = None
    matched_bad_track_path: str | None = None
    # Composite-duration observation (issue #1237): populated whenever
    # ``candidate_audio_coverage`` finds a coalesced Discogs indexed
    # component with unresolved duration evidence -- either the declared
    # sub-durations disagree with the local file's length, OR that
    # duration evidence is simply unknown/unprovable (an unparseable or
    # missing declared duration, or an unreadable local length). Never
    # fails validation on its own (see ``lib.beets_candidate_coverage.
    # CandidateAudioCoverage.complete``) -- this is evidence only,
    # persisted so it stays visible through existing ``validation_result``
    # audit surfaces even on a ``strong_match``.
    incomplete_composite_paths: list[str] = []

    def to_json(self) -> str:
        """Serialize to JSON string via msgspec.json.encode."""
        return msgspec.json.encode(self).decode()

    @classmethod
    def from_dict(cls, d: dict[str, object]) -> Self:
        """Construct from a dict — strict-typed decode at the boundary.

        Every nested ``CandidateSummary`` / ``TrackMapping`` / ``HarnessItem``
        / ``HarnessTrackInfo`` is validated against its declared types.
        """
        return msgspec.convert(d, type=cls)

    @classmethod
    def from_json(cls, s: str) -> Self:
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
