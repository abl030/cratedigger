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

``download_log`` (issue #765 phase 6 continuation): every existing reader
in ``lib/pipeline_db/download_log.py`` joins ``dl.*`` against
``album_quality_evidence`` / ``album_requests`` / a self-join for
``source_download_log_id`` provenance, so none of them project exactly
this table's columns — retyping those joined projections to a row type
plus their extra fields is deferred to a later pass; ``DownloadLogRow`` /
``download_log_row()`` exist now for the column-contract guard and the
next pure reader that needs them.
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
from typing import TypedDict

import msgspec


class AlbumRequestRow(TypedDict):
    """One ``SELECT * FROM album_requests`` row (53 columns as of schema 060)."""

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
    imported_path: str | None
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
