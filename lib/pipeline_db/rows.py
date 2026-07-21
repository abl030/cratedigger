"""Read-projection row types for ``PipelineDB`` (issue #765 phase 6).

The write side has been Struct-typed since #546 W3 (``PersistedYoutubeRow``
et al. — flat ``msgspec.Struct`` payloads whose field names ARE column
names, guarded by ``tests/test_pipeline_db_column_contract.py``). This
module is the READ-side twin: a ``TypedDict`` per ``SELECT *`` row shape.

TypedDict — not Struct — on the read side deliberately: rows stay plain
dicts at runtime, so the entire ``row["field"]`` consumer surface keeps
working unchanged while pyright gains per-key types and key-literal
checking. There is no runtime validation here (TypedDict is erased); the
schema-parity audit in ``tests/test_pipeline_db_column_contract.py``
asserts key-for-key EQUALITY against ``information_schema.columns`` on
the ephemeral migrations-applied PG, so a new migration column fails the
suite until the row type (and the ``make_request_row`` builder) learn it
in the same PR.

Value-type mapping from ``information_schema``: integer/bigint → int,
text → str, real → float, boolean → bool, timestamptz → datetime,
jsonb → ``dict[str, object]``. Nullable columns carry ``| None``.

``download_log`` joined readers (issue #784 continuation of #765 phase 6):
every existing reader in ``lib/pipeline_db/download_log.py`` joins
``dl.*`` against ``album_quality_evidence`` / ``album_requests`` / a
self-join for ``source_download_log_id`` provenance, so none of them
project exactly this table's columns. Each joined shape gets its own row
type that INHERITS ``DownloadLogRow`` and declares the join extras:

- ``DownloadLogWithEvidenceRow`` — ``get_download_log_entry``,
  ``get_download_history``, ``get_download_history_batch``,
  ``get_latest_download_summaries``: ``dl.*`` LEFT JOINed against
  ``album_quality_evidence`` (the per-candidate evidence), post-overlay.
  ``original_beets_distance`` (the self-joined origin row's
  ``beets_distance``) plus the four ``source_*`` fields — these are
  NOT real ``download_log`` columns; the overlay is their sole producer
  (folding ``album_quality_evidence.format``/bitrates in when the
  evidence is lineage-v3/v4 "source-semantic"), so they are always
  present but nullable rather than conditionally absent.
- ``DownloadLogWithRequestRow`` — ``get_log``'s three query variants
  (default/imported/rejected): everything ``DownloadLogWithEvidenceRow``
  has, PLUS the surviving ``_current_evidence_*`` facts (the request's
  CURRENT evidence — a second, separate evidence join from the
  per-candidate one above) and the joined ``album_requests`` facts.
- ``DownloadLogWithOriginRow`` — ``get_linked_import_logs`` ONLY: ``dl.*``
  self-joined for ``original_beets_distance`` with NO evidence join at
  all (so the overlay never runs and the ``source_*`` fields never
  appear) — a narrower shape than ``DownloadLogWithEvidenceRow``, not an
  interchangeable one.

All three row types are the shape AFTER
``_DownloadLogMixin._overlay_evidence_onto_download_log_row`` runs, where
applicable — that step POPS the transient ``_evidence_*``/
``_evidence_lineage_version`` keys off the raw joined row once it folds
their values into the legacy ``dl.*`` columns (and the four synthetic
``source_*`` keys), so those transient keys never reach the adapter or
the row type. **Critical constraint (found in review): ``msgspec.convert``
targeting a ``TypedDict`` silently DROPS any key not declared on the
type** — it does not raise. That means the bare ``download_log_row()``
adapter must never be pointed at one of these joined rows: it would
silently discard every join extra (``album_title``,
``original_beets_distance``, ...) instead of erroring, which is exactly
the failure mode a "typed projection" exists to prevent. Each joined
projection therefore gets its own adapter (``download_log_with_evidence_row``,
``download_log_with_request_row``, ``download_log_with_origin_row``) that
converts to the row type declaring those extras, so the parity/subset
contract stays meaningful.

``get_wrong_matches`` does not project ``dl.*`` at all — it SELECTs an
explicit, aliased, COALESCEd column list (candidate-evidence facts merged
with legacy ``dl.*`` facts in SQL, plus ``album_requests`` facts). Its row
type, ``WrongMatchCandidateRow``, is a standalone ``TypedDict`` — it does
NOT inherit ``DownloadLogRow`` because it does not carry that table's full
column set (e.g. no ``created_at``, no ``outcome``).
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
from typing import TypedDict

import msgspec


class AlbumRequestRow(TypedDict):
    """One ``SELECT * FROM album_requests`` row (52 columns as of schema 061)."""

    id: int
    mb_release_id: str | None
    mb_release_group_id: str | None
    mb_artist_id: str | None
    discogs_release_id: str | None
    artist_name: str
    album_title: str
    year: int | None
    country: str | None
    format: str | None
    source: str
    source_path: str | None
    reasoning: str | None
    status: str
    search_attempts: int
    download_attempts: int
    validation_attempts: int
    last_attempt_at: datetime | None
    next_retry_after: datetime | None
    beets_distance: float | None
    beets_scenario: str | None
    created_at: datetime
    updated_at: datetime
    min_bitrate: int | None
    prev_min_bitrate: int | None
    last_download_spectral_bitrate: int | None
    last_download_spectral_grade: str | None
    verified_lossless: bool | None
    current_spectral_grade: str | None
    current_spectral_bitrate: int | None
    active_download_state: dict[str, object] | None
    final_format: str | None
    search_filetype_override: str | None
    target_format: str | None
    current_lossless_source_v0_probe_min_bitrate: int | None
    current_lossless_source_v0_probe_avg_bitrate: int | None
    current_lossless_source_v0_probe_median_bitrate: int | None
    active_plan_id: int | None
    next_plan_ordinal: int
    plan_cycle_count: int
    current_evidence_id: int | None
    replaces_request_id: int | None
    release_group_year: int | None
    failure_class: str | None
    is_va_compilation: bool
    unfindable_category: str | None
    unfindable_categorised_at: datetime | None
    last_artist_probe_at: datetime | None
    last_artist_probe_match_count: int | None
    rescued_at: datetime | None
    prior_unfindable_category: str | None
    catalog_number: str | None


def album_request_row(raw: Mapping[str, object]) -> AlbumRequestRow:
    """Detach a psycopg2 ``RealDictRow`` into the validated row projection.

    The one adapter between the untyped cursor and the TypedDict — the
    read-side analogue of decoding at exactly one site. ``msgspec.convert``
    validates every declared key/type at runtime, so column-type drift
    raises ``msgspec.ValidationError`` here instead of surfacing as a
    confusing failure deep in a consumer.
    """
    return msgspec.convert(dict(raw), type=AlbumRequestRow)


class DownloadLogRow(TypedDict):
    """One ``SELECT * FROM download_log`` row (42 columns as of schema 054).

    ``download_log`` doubles as the slskd audit trail AND the YouTube
    rescue queue (``source`` discriminates, migration 037), so only
    ``id``, ``request_id``, ``created_at``, and ``source`` are NOT NULL —
    every other column is legitimately absent on some row shape (a bare
    slskd reject never populates ``youtube_metadata``; a YouTube queue
    row never populates the spectral/V0 columns).
    """

    id: int
    request_id: int
    soulseek_username: str | None
    filetype: str | None
    download_path: str | None
    beets_distance: float | None
    beets_scenario: str | None
    beets_detail: str | None
    valid: bool | None
    outcome: str | None
    staged_path: str | None
    error_message: str | None
    bitrate: int | None
    sample_rate: int | None
    bit_depth: int | None
    is_vbr: bool | None
    was_converted: bool | None
    original_filetype: str | None
    created_at: datetime
    slskd_filetype: str | None
    actual_filetype: str | None
    actual_min_bitrate: int | None
    spectral_grade: str | None
    spectral_bitrate: int | None
    existing_min_bitrate: int | None
    existing_spectral_bitrate: int | None
    import_result: dict[str, object] | None
    validation_result: dict[str, object] | None
    final_format: str | None
    v0_probe_kind: str | None
    v0_probe_min_bitrate: int | None
    v0_probe_avg_bitrate: int | None
    v0_probe_median_bitrate: int | None
    existing_v0_probe_kind: str | None
    existing_v0_probe_min_bitrate: int | None
    existing_v0_probe_avg_bitrate: int | None
    existing_v0_probe_median_bitrate: int | None
    candidate_evidence_id: int | None
    source: str
    youtube_metadata: dict[str, object] | None
    # Migration 043 (issue #564 C7) — despite the ``jsonb`` column type,
    # the runtime value ``lib/download.py`` writes here is a JSON ARRAY of
    # per-file failure-detail objects (``msgspec.to_builtins`` over
    # ``list[FileFailureDetail]``), not a JSON object. Verified empirically
    # against the sole writer before pinning this type — the one column on
    # this table where the generic jsonb → ``dict[str, object]`` mapping
    # does not hold.
    transfer_detail: list[dict[str, object]] | None
    # Migration 052 — exact validation/download row that produced a later
    # force-import or historical manual-import audit row (self-FK).
    source_download_log_id: int | None


def download_log_row(raw: Mapping[str, object]) -> DownloadLogRow:
    """Detach a psycopg2 ``RealDictRow`` into the validated row projection.

    The read-side twin of ``album_request_row`` for a pure (unjoined)
    ``download_log`` row. No production reader issues a bare
    ``SELECT * FROM download_log`` today — see the module docstring's
    ``download_log`` note — so this adapter is exercised by the
    column-contract guard now and by the next pure reader that needs it.
    """
    return msgspec.convert(dict(raw), type=DownloadLogRow)


class DownloadLogWithEvidenceRow(DownloadLogRow):
    """``get_download_log_entry``, ``get_download_history``,
    ``get_download_history_batch``, ``get_latest_download_summaries``:
    ``dl.*`` LEFT JOINed against the per-candidate
    ``album_quality_evidence`` row, post-overlay. ``source_format`` /
    ``source_min_bitrate`` / ``source_avg_bitrate`` / ``source_median_bitrate``
    are NOT real ``download_log`` columns — the evidence overlay is their
    sole producer, kept always-present-but-nullable by
    ``_overlay_evidence_onto_download_log_row`` rather than conditionally
    absent (see the module docstring)."""

    original_beets_distance: float | None
    source_format: str | None
    source_min_bitrate: int | None
    source_avg_bitrate: int | None
    source_median_bitrate: int | None


def download_log_with_evidence_row(
    raw: Mapping[str, object],
) -> DownloadLogWithEvidenceRow:
    """Convert a post-overlay evidence-joined row (no ``album_requests``
    join) into its typed projection.

    Callers run ``_overlay_evidence_onto_download_log_row`` on the raw
    joined row FIRST (folding candidate-evidence facts into the legacy
    ``dl.*`` columns / the synthetic ``source_*`` keys, and popping the
    transient ``_evidence_*`` keys) — this adapter only validates/detaches
    the result, exactly like ``album_request_row`` / ``download_log_row``.
    """
    return msgspec.convert(dict(raw), type=DownloadLogWithEvidenceRow)


class DownloadLogWithRequestRow(DownloadLogWithEvidenceRow):
    """``get_log``'s three query variants: everything
    ``DownloadLogWithEvidenceRow`` has, PLUS the request's CURRENT
    evidence facts (a second, separate evidence join from the
    per-candidate one) and the joined ``album_requests`` facts."""

    _current_evidence_id: int | None
    _current_evidence_is_pre_attempt: bool | None
    _current_evidence_format: str | None
    _current_evidence_min_bitrate: int | None
    _current_evidence_avg_bitrate: int | None
    _current_evidence_median_bitrate: int | None
    _current_evidence_spectral_grade: str | None
    _current_evidence_spectral_bitrate: int | None
    _current_evidence_v0_probe_kind: str | None
    _current_evidence_v0_probe_min_bitrate: int | None
    _current_evidence_v0_probe_avg_bitrate: int | None
    _current_evidence_v0_probe_median_bitrate: int | None
    album_title: str
    artist_name: str
    mb_release_id: str | None
    year: int | None
    country: str | None
    request_status: str
    request_min_bitrate: int | None
    prev_min_bitrate: int | None
    search_filetype_override: str | None
    request_source: str


def download_log_with_request_row(
    raw: Mapping[str, object],
) -> DownloadLogWithRequestRow:
    """Convert a post-overlay ``get_log`` row into its typed projection.

    Same contract as ``download_log_with_evidence_row`` — the caller runs
    the overlay first; this adapter only validates/detaches the result.
    """
    return msgspec.convert(dict(raw), type=DownloadLogWithRequestRow)


class DownloadLogWithOriginRow(DownloadLogRow):
    """``get_linked_import_logs`` ONLY: ``dl.*`` self-joined for
    ``original_beets_distance``, with NO evidence join — the overlay
    never runs for this reader, so the ``source_*`` fields
    ``DownloadLogWithEvidenceRow`` carries never appear here. A narrower,
    NOT interchangeable shape (see the module docstring)."""

    original_beets_distance: float | None


def download_log_with_origin_row(
    raw: Mapping[str, object],
) -> DownloadLogWithOriginRow:
    """Convert ``get_linked_import_logs``'s raw ``dl.* + original_beets_distance``
    row — no evidence join, so no overlay step runs before this adapter."""
    return msgspec.convert(dict(raw), type=DownloadLogWithOriginRow)


class WrongMatchCandidateRow(TypedDict):
    """One ``get_wrong_matches`` row: a standalone projection, NOT a
    ``dl.*`` superset — the query selects an explicit, aliased,
    COALESCEd column list (candidate evidence merged with legacy
    ``dl.*`` facts in SQL) plus joined ``album_requests`` facts. See the
    module docstring for why this does not inherit ``DownloadLogRow``.
    """

    download_log_id: int
    request_id: int
    artist_name: str
    album_title: str
    mb_release_id: str | None
    mb_release_group_id: str | None
    soulseek_username: str | None
    validation_result: dict[str, object] | None
    spectral_grade: str | None
    spectral_bitrate: int | None
    v0_probe_kind: str | None
    v0_probe_avg_bitrate: int | None
    evidence_source_codec: str | None
    evidence_source_container: str | None
    evidence_storage_format: str | None
    evidence_target_format: str | None
    evidence_target_is_cbr: bool | None
    evidence_lineage_version: int | None
    evidence_min_bitrate: int | None
    evidence_avg_bitrate: int | None
    evidence_verified_lossless: bool | None
    request_status: str
    request_min_bitrate: int | None
    request_verified_lossless: bool | None
    request_current_spectral_grade: str | None
    request_current_spectral_bitrate: int | None


def wrong_match_candidate_row(raw: Mapping[str, object]) -> WrongMatchCandidateRow:
    """Detach a psycopg2 ``RealDictRow`` into the validated row projection
    for one ``get_wrong_matches`` candidate."""
    return msgspec.convert(dict(raw), type=WrongMatchCandidateRow)
