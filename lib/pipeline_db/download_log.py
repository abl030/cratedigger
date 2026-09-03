"""download_log audit rows and wrong-match bookkeeping."""
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any, Literal, TypedDict, get_args

import msgspec
import psycopg2
import psycopg2.extras

from lib.beets_db import exact_release_identity_matches
from lib.convergence_service import normalize_contributor_usernames
from lib.pipeline_db._shared import (
    CANDIDATE_EVIDENCE_PREFIX,
    CURRENT_EVIDENCE_PREFIX,
    accusation_evidence_columns,
)
from lib.pipeline_db.rows import (
    DownloadLogWithEvidenceRow,
    DownloadLogWithOriginRow,
    DownloadLogWithRequestRow,
    WrongMatchCandidateRow,
    download_log_with_evidence_row,
    download_log_with_origin_row,
    download_log_with_request_row,
    wrong_match_candidate_row,
)
from lib.quality import evidence_is_source_semantic

# Canonical ``download_log.outcome`` taxonomy — the Python mirror of the
# ``download_log_outcome_check`` CHECK constraint (latest definition:
# migrations/054). Two sync points only: this Literal and the migration
# SQL; ``tests/test_migrator.py`` pins them together. Writers get pyright
# enforcement at the call site instead of a CheckViolation in production
# (the failure mode that shipped twice on 2026-07-02: 'error' from the
# #146 grace escape and the latent 'user_offline' from lib/enqueue.py).
DownloadLogOutcome = Literal[
    "success", "rejected", "failed", "timeout",
    "force_import", "manual_import", "curator_ban",
    "measurement_failed", "user_offline", "have_analysis_error",
    "youtube_running", "youtube_success", "youtube_failed",
    "local_import",
]
DOWNLOAD_LOG_OUTCOMES: frozenset[str] = frozenset(get_args(DownloadLogOutcome))

from lib.pipeline_db._core import _PipelineDBBase
from lib.validation_envelope import (
    FAILED_PATH_KEY,
    VALIDATION_PROJECTION_UNSET,
    WRONG_MATCH_TRIAGE_KEY,
    ValidationProjectionUnset,
    WrongMatchTriageAudit,
    derive_validation_log_columns,
)


@dataclass(frozen=True)
class DownloadLogCounts:
    """Aggregate counts behind the Recents tab's filter chips +
    found-search enqueue rates (#445 item 2)."""

    total: int
    imported: int
    matches_24h: int
    matches_6h: int


class LatestDownloadSummary(TypedDict):
    """One ``get_latest_download_summaries`` entry: the newest row for a
    request plus its total history count (#426 — the pipeline queue only
    renders the latest verdict and a count, never the full history)."""

    latest: DownloadLogWithEvidenceRow
    count: int


#: The candidate-evidence aliases every ``download_log`` reader joins.
#:
#: Five queries carried verbatim copies of this block. Issue #829 Phase 5
#: PR4 had to add the proof-gate columns to all of them, and a block that
#: must be edited five identically-or-else times is the duplication
#: ``scope.md`` says to fix rather than extend.
#:
#: The first eleven aliases are folded into their legacy ``download_log``
#: columns by ``overlay_evidence_onto_download_log_row``. The PR4 aliases
#: below them have no legacy counterpart, so they reach the renderer under
#: these names and are declared on ``DownloadLogWithEvidenceRow``.
#: ``_evidence_verified_lossless_classifier`` is the one of those the overlay
#: still adjudicates: it keeps its own alias but is NULLed on a
#: non-source-semantic lineage, because a cross-walked
#: ``candidate_evidence_id`` (migration 021 §6b) names a sibling attempt's
#: snapshot and a proof minted from other bytes is not this attempt's proof.
#: ``_evidence_format`` is deliberately a SECOND alias over the same
#: column as ``_evidence_source_format``: the overlay folds that one into
#: the legacy ``source_format`` key ONLY for lineage 3/4 rows, so 26,503 of
#: 30,467 live rows hand the renderer ``None`` there. (Issue #1355 item 2:
#: the decider's ``measurement.format`` CAN itself be NULL now, on an
#: early-reject candidate with no files to read a real extension from —
#: ``_evidence_format``'s own value is then also NULL, so this alias split
#: still can't launder one into the other.) Reusing it would let the render
#: adapter resolve a different codec from the same evidence row.
#: ``_evidence_was_converted_from`` is deliberately NULL: canonical evidence
#: may be co-referenced as current, but this projection is always candidate
#: source semantics. Recents must agree with action-time and CLI projection.
#: ``_evidence_container_extensions`` is the snapshot's own distinct file
#: extensions — the ultrasonic proof leg's decode-path input. Without it
#: that leg would withhold on the render path while the decider
#: adjudicated, and the panel would state a verdict production never
#: reached (Phase 5 plan §1.5c: the same bits measure 3.09 dB apart across
#: decode paths).
#: The nine measurement columns the audit-only accusation derivation reads
#: are spelled inline here rather than generated from
#: ``accusation_evidence_columns``: this literal is what lets
#: ``tests/test_replaced_write_audit.py`` resolve the four queries that
#: embed it as STATIC SQL, and an interpolated call would demote all four
#: into the reviewed-dynamic registry. ``tests/test_pipeline_db_column_
#: contract.py`` pins this block against the generator instead, so the
#: two cannot drift into projecting different subsets.
_CANDIDATE_EVIDENCE_COLUMNS = """
    e.mb_release_id AS _evidence_mb_release_id,
    e.format AS _evidence_source_format,
    e.min_bitrate_kbps AS _evidence_source_min_bitrate,
    e.avg_bitrate_kbps AS _evidence_source_avg_bitrate,
    e.median_bitrate_kbps AS _evidence_source_median_bitrate,
    e.lineage_version AS _evidence_lineage_version,
    e.spectral_grade AS _evidence_spectral_grade,
    e.spectral_bitrate_kbps AS _evidence_spectral_bitrate,
    e.v0_subject AS _evidence_v0_probe_kind,
    e.v0_min_bitrate_kbps AS _evidence_v0_probe_min_bitrate,
    e.v0_avg_bitrate_kbps AS _evidence_v0_probe_avg_bitrate,
    e.v0_median_bitrate_kbps AS _evidence_v0_probe_median_bitrate,
    e.format AS _evidence_format,
    e.codec_family AS _evidence_codec_family,
    e.cliff_hz AS _evidence_cliff_hz,
    e.storage_format AS _evidence_storage_format,
    e.filetype_band AS _evidence_filetype_band,
    e.spectral_subject AS _evidence_spectral_subject,
    NULL::text AS _evidence_was_converted_from,
    e.ultrasonic_deficit_db AS _evidence_ultrasonic_deficit_db,
    e.spectral_measurement_version AS _evidence_spectral_measurement_version,
    e.aac_lattice_modal_count AS _evidence_aac_lattice_modal_count,
    e.aac_lattice_scored_tracks AS _evidence_aac_lattice_scored_tracks,
    e.aac_lattice_max_z AS _evidence_aac_lattice_max_z,
    e.verified_lossless_classifier AS _evidence_verified_lossless_classifier,
    e.cd_rip_verification AS _evidence_cd_rip_verification,
    (SELECT array_agg(DISTINCT f.extension)
       FROM album_quality_evidence_files f
      WHERE f.evidence_id = e.id) AS _evidence_container_extensions,
"""


#: ``get_log``'s three variants differed ONLY in their outcome filter and
#: were three verbatim copies of a 40-line SELECT. Issue #829 Phase 5 PR4
#: needed one more evidence join on every variant, and a change that has to
#: be made three identically-or-else times is the duplication ``scope.md``
#: says to fix rather than extend. The ``{where}`` slot is the only
#: difference; it is filled from the closed literal map below, never from
#: caller input.
_LOG_QUERY_TEMPLATE = """
    SELECT dl.*,
           {candidate_evidence_columns}
           ar.mb_release_id AS _request_mb_release_id,
           current_evidence.id AS _current_evidence_id,
           current_evidence.mb_release_id
               AS _current_evidence_mb_release_id,
           (current_evidence.measured_at <= dl.created_at)
               AS _current_evidence_is_pre_attempt,
           current_evidence.format AS _current_evidence_format,
           current_evidence.min_bitrate_kbps AS _current_evidence_min_bitrate,
           current_evidence.avg_bitrate_kbps AS _current_evidence_avg_bitrate,
           current_evidence.median_bitrate_kbps AS _current_evidence_median_bitrate,
           current_evidence.spectral_grade AS _current_evidence_spectral_grade,
           current_evidence.spectral_bitrate_kbps AS _current_evidence_spectral_bitrate,
           current_evidence.v0_subject AS _current_evidence_v0_probe_kind,
           current_evidence.v0_min_bitrate_kbps AS _current_evidence_v0_probe_min_bitrate,
           current_evidence.v0_avg_bitrate_kbps AS _current_evidence_v0_probe_avg_bitrate,
           current_evidence.v0_median_bitrate_kbps AS _current_evidence_v0_probe_median_bitrate,
           current_evidence.codec_family AS _current_evidence_codec_family,
           current_evidence.cliff_hz AS _current_evidence_cliff_hz,
           current_evidence.storage_format AS _current_evidence_storage_format,
           current_evidence.filetype_band AS _current_evidence_filetype_band,
           current_evidence.spectral_subject AS _current_evidence_spectral_subject,
           current_evidence.was_converted_from
               AS _current_evidence_was_converted_from,
           origin.beets_distance AS original_beets_distance,
           ar.album_title, ar.artist_name, ar.mb_release_id,
           ar.year, ar.country, ar.status AS request_status,
           ar.min_bitrate AS request_min_bitrate,
           ar.prev_min_bitrate, ar.search_filetype_override,
           ar.source AS request_source
    FROM download_log dl
    LEFT JOIN album_quality_evidence e
        ON e.id = dl.candidate_evidence_id
    LEFT JOIN download_log origin
        ON origin.id = dl.source_download_log_id
    JOIN album_requests ar ON dl.request_id = ar.id
    LEFT JOIN album_quality_evidence current_evidence
        ON current_evidence.id = ar.current_evidence_id
    {where}
    ORDER BY dl.created_at DESC LIMIT %s
"""

#: The ``download_log.outcome`` values ``get_log``'s two Recents filters
#: admit. One spelling: the SQL fragments below are rendered from these
#: tuples, and ``tests/fakes/pipeline_db/download_log.py`` imports the same tuples rather
#: than hand-copying the vocabulary into a Python set.
LOG_FILTER_IMPORTED_OUTCOMES: tuple[DownloadLogOutcome, ...] = (
    "success", "force_import", "local_import",
)
LOG_FILTER_REJECTED_OUTCOMES: tuple[DownloadLogOutcome, ...] = (
    "rejected", "failed", "timeout", "measurement_failed",
)


def _sql_outcome_list(outcomes: tuple[DownloadLogOutcome, ...]) -> str:
    """Render a closed outcome vocabulary as a SQL string list.

    Input is always a module-owned tuple of ``DownloadLogOutcome`` members
    — never caller data — and every member is re-checked against the
    canonical taxonomy so a typo cannot reach the database as a silently
    unmatched literal.
    """
    unknown = sorted(set(outcomes) - DOWNLOAD_LOG_OUTCOMES)
    if unknown:
        raise ValueError(
            "not download_log.outcome values: " + ", ".join(unknown)
        )
    return ", ".join(f"'{outcome}'" for outcome in outcomes)


#: The ``download_log.outcome`` values that mark a row as the MUTATING
#: successor of an earlier audit row — what ``get_linked_import_logs``
#: fetches for a kept wrong match. Broader than the Recents "imported"
#: filter above: it also admits the historical ``manual_import`` lane,
#: whose rows still link back. The vocabulary is exported so the in-memory
#: twin filters on the same set; the SQL below keeps its literal, and
#: ``TestSharedOutcomeVocabularies`` binds the two by round-tripping every
#: canonical outcome through real PostgreSQL.
LINKED_IMPORT_OUTCOMES: tuple[DownloadLogOutcome, ...] = (
    "success", "force_import", "manual_import", "local_import",
)

#: The only outcome filters ``get_log`` accepts, as literal SQL fragments.
_LOG_OUTCOME_FILTERS: dict[str, str] = {
    "imported": (
        "WHERE dl.outcome IN "
        f"({_sql_outcome_list(LOG_FILTER_IMPORTED_OUTCOMES)})"
    ),
    "rejected": (
        "WHERE dl.outcome IN "
        f"({_sql_outcome_list(LOG_FILTER_REJECTED_OUTCOMES)})"
    ),
}


# Evidence-overlay extension applied to every download_log read seam
# (single entry / per-request history / batch). The legacy denorm
# spectral / V0 columns on download_log are NULL whenever the
# candidate was rejected before the dispatch path could backfill
# them — every wrong-match reject hits this. The canonical
# measurement lives on album_quality_evidence, addressed via
# download_log.candidate_evidence_id. We LEFT JOIN it here and let
# the Python overlay step COALESCE evidence over the denorm columns
# before handing the row dict to downstream consumers (LogEntry,
# build_download_history_row, the wrong-match route, ...). Doing it
# at the read seam means there's exactly one place to maintain the
# mapping, and downstream code keeps using the existing field names.
#
# ``dl.*`` automatically projects ``source`` and ``youtube_metadata``
# (migration 037) onto every consumer; no additional column list
# change is needed here.
# Evidence stores lineage as ``lossless_source`` / ``native_lossy_research``;
# download_log.v0_probe_kind stores the wire-shaped kind
# ``lossless_source_v0`` / ``native_lossy_research_v0`` (constrained by
# migration 007). When we overlay evidence lineage into the kind slot, we
# have to translate, or the renderer (history.js::formatV0Probe) won't
# recognize the value and will fall through to the raw-kind branch.
_EVIDENCE_LINEAGE_TO_PROBE_KIND: dict[str, str] = {
    "source":    "lossless_source_v0",
    "installed": "native_lossy_research_v0",
    "lossless_source": "lossless_source_v0",
    "native_lossy_research": "native_lossy_research_v0",
    "on_disk_research": "on_disk_research_v0",
}

#: The aliases the overlay consumes as its own gate inputs and pops
#: before any renderer sees the row. Named here so the SELECT-block
#: partition in ``tests/test_pipeline_db_column_contract.py`` derives
#: them from production rather than restating them.
_EVIDENCE_LINEAGE_ALIAS = "_evidence_lineage_version"
_EVIDENCE_IDENTITY_ALIAS = "_evidence_mb_release_id"
_REQUEST_IDENTITY_ALIAS = "_request_mb_release_id"
_CURRENT_EVIDENCE_IDENTITY_ALIAS = "_current_evidence_mb_release_id"
_EVIDENCE_GATE_INPUT_ALIASES: tuple[str, ...] = (
    _EVIDENCE_LINEAGE_ALIAS,
    _EVIDENCE_IDENTITY_ALIAS,
)

#: ``(legacy download_log key, candidate-evidence alias, gated)`` — the
#: aliases this overlay CONSUMES, folding each into the legacy column
#: downstream code already reads. Hoisted out of the function body so
#: ``tests/test_pipeline_db_column_contract.py::TestRenderAliasMap`` can
#: partition the SELECT block by what actually happens to each alias
#: instead of restating the list.
_EVIDENCE_OVERLAY_FOLD: tuple[tuple[str, str, bool], ...] = (
    ("source_format",          "_evidence_source_format", True),
    ("source_min_bitrate",     "_evidence_source_min_bitrate", True),
    ("source_avg_bitrate",     "_evidence_source_avg_bitrate", True),
    ("source_median_bitrate",  "_evidence_source_median_bitrate", True),
    ("spectral_grade",       "_evidence_spectral_grade", False),
    ("spectral_bitrate",     "_evidence_spectral_bitrate", False),
    ("v0_probe_kind",        "_evidence_v0_probe_kind", False),
    ("v0_probe_min_bitrate", "_evidence_v0_probe_min_bitrate", False),
    ("v0_probe_avg_bitrate", "_evidence_v0_probe_avg_bitrate", False),
    ("v0_probe_median_bitrate", "_evidence_v0_probe_median_bitrate", False),
)

#: The aliases with no legacy column to fold into, so they are gated IN
#: PLACE by exact release identity and ``evidence_is_source_semantic``.
#: These are the two attribution predicates shared with
#: ``pipeline-cli quality`` so the surfaces cannot state different proofs
#: for the same album.
#:
#: ``candidate_evidence_id`` does not always name evidence measured from
#: THIS attempt's bytes: migration 021 §6b cross-walked pre-content-
#: addressing rows onto whichever content-addressed row their release
#: already had, so a legacy-lineage attempt can point at a sibling
#: attempt's snapshot. That is exactly why the ``source_*`` fold is gated
#: on lineage 3/4, and the minted proof is the same kind of fact — a
#: conclusion about the measured source bytes. Ungated it lent a proof to
#: 5,014 live rows and put "verified lossless" in 281 live verdicts, 250
#: of which point at an evidence row another attempt also claims and 109
#: of which converted nothing at all — a plain MP3 import rendering
#: "MP3 320, verified lossless" off its FLAC sibling's snapshot.
_EVIDENCE_ATTRIBUTABLE_PROOF_ALIASES: tuple[str, ...] = (
    "_evidence_verified_lossless_classifier",
    "_evidence_cd_rip_verification",
)


def overlay_evidence_onto_download_log_row(
    row: dict[str, object],
) -> dict[str, object]:
    """Fold the candidate-evidence join onto one raw ``download_log`` row.

    The single owner of the read-seam evidence overlay. ``row`` is the
    pre-overlay shape the SQL SELECT produces: the ``download_log``
    columns plus the ``_CANDIDATE_EVIDENCE_COLUMNS`` aliases, the
    ``_request_mb_release_id`` gate input, and (for ``get_log``) the
    ``_current_evidence_*`` aliases. Mutates and returns ``row``.

    ``tests/fakes/pipeline_db/download_log.py`` builds that same raw shape and calls
    this function, so the fake cannot be more permissive than production
    about the GATING this function owns — which evidence facts survive
    the identity and lineage predicates (issue #1278 item 7). It is not a
    claim about the whole row shape: production runs one more step the
    fake does not, converting to a typed row in ``lib/pipeline_db/rows.py``
    which silently drops any key the TypedDict does not declare, where the
    fake casts the overlaid dict as-is. The two key sets were measured
    equal when this was written, but only the gating above is shared
    machinery holding them that way.
    """
    request_identity = row.pop(
        _REQUEST_IDENTITY_ALIAS, row.get("mb_release_id")
    )
    evidence_identity = row.pop(_EVIDENCE_IDENTITY_ALIAS, None)
    evidence_attributable = exact_release_identity_matches(
        request_identity, evidence_identity
    )
    # Migration 050 deliberately marks historical evidence as lineage v1:
    # its measurement format/bitrates may be a projected target rather
    # than facts about the downloaded source. Only v3 proves those fields
    # source-semantic. Spectral and V0 facts were never target projections,
    # so they remain safe to recover from either lineage.
    evidence_lineage = row.pop(_EVIDENCE_LINEAGE_ALIAS, None)
    source_semantic = (
        evidence_attributable
        and evidence_is_source_semantic(evidence_lineage)
    )
    for alias in _EVIDENCE_ATTRIBUTABLE_PROOF_ALIASES:
        # Same stable-shape contract as the ``source_*`` keys: always
        # present, NULL when this row's evidence cannot speak for this
        # attempt's source bytes.
        row.setdefault(alias, None)
        if not source_semantic:
            row[alias] = None
    for legacy, overlay, requires_source_semantic in _EVIDENCE_OVERLAY_FOLD:
        evidence_value = row.pop(overlay, None)
        if not evidence_attributable:
            if requires_source_semantic:
                row.setdefault(legacy, None)
            continue
        if requires_source_semantic:
            # The four ``source_*`` legacy keys are NOT real
            # ``download_log`` columns — this overlay is their SOLE
            # producer (issue #784's DownloadLogWithEvidenceRow), so
            # they must always end up present (nullable) for a
            # stable row shape, never silently absent depending on
            # whether candidate evidence happened to exist.
            row.setdefault(legacy, None)
            if not source_semantic:
                continue
        if row.get(legacy) is None and evidence_value is not None:
            if legacy == "v0_probe_kind" and isinstance(evidence_value, str):
                evidence_value = _EVIDENCE_LINEAGE_TO_PROBE_KIND.get(
                    evidence_value, evidence_value
                )
            row[legacy] = evidence_value

    # Candidate aliases without legacy columns flow directly into the
    # renderer. A foreign exact pressing must therefore null every one,
    # not only proof fields: classifier, spectral, bitrate, codec, and
    # container facts all describe the sibling evidence row's bytes.
    if not evidence_attributable:
        for key in row:
            if key.startswith("_evidence_"):
                row[key] = None

    current_identity = row.pop(_CURRENT_EVIDENCE_IDENTITY_ALIAS, None)
    current_attributable = exact_release_identity_matches(
        request_identity, current_identity
    )
    if not current_attributable:
        for key in row:
            if key.startswith("_current_evidence_"):
                row[key] = None
    return row


class _DownloadLogMixin(_PipelineDBBase):
    """download_log audit rows and wrong-match bookkeeping."""

    def get_retained_failure_paths(self) -> set[str]:
        """Return source paths whose persisted failure audit requires retention.

        ``measurement_failed`` means Cratedigger could not establish an audio
        fact because the surrounding world failed (for example permissions,
        a vanished path, or a tool/process failure).  The source is therefore
        audit evidence, not an ordinary abandoned download, and the disk
        reaper must never remove it.

        A post-commit corrupt-audio quarantine audit is also retained. No
        current writer produces this key (issue #1077, D3: bad rips are now
        ban + delete, never quarantined), but historical rows from before
        that fix still carry it, and a retained folder must stay protected
        for as long as its audit row exists. There is intentionally no age
        or request-status filter: the persisted terminal audit row is the
        retention authority for as long as it exists.
        """
        cur = self._execute(
            """
            SELECT DISTINCT retained_path
            FROM (
                SELECT staged_path AS retained_path
                FROM download_log
                WHERE outcome = 'measurement_failed'
                UNION ALL
                SELECT validation_result->>'failed_path' AS retained_path
                FROM download_log
                WHERE validation_result ? 'post_commit_quarantine'
            ) retained
            WHERE retained_path IS NOT NULL
              AND BTRIM(retained_path) <> ''
            """,
        )
        return {str(row["retained_path"]) for row in cur.fetchall()}

    def get_download_log_counts(self) -> DownloadLogCounts:
        """One-query aggregate: download_log totals plus found-search
        counts in the 24h/6h windows. The CROSS JOIN of two single-row
        aggregates always returns exactly one row."""
        cur = self._execute("""
            WITH download_counts AS (
                SELECT
                    COUNT(*) AS total,
                    COUNT(*) FILTER (
                        WHERE outcome IN ('success', 'force_import', 'local_import')
                    ) AS imported
                FROM download_log
            ),
            match_counts AS (
                SELECT
                    COUNT(*) FILTER (
                        WHERE outcome = 'found'
                          AND created_at >= NOW() - INTERVAL '24 hours'
                    )::int AS matches_24h,
                    COUNT(*) FILTER (
                        WHERE outcome = 'found'
                          AND created_at >= NOW() - INTERVAL '6 hours'
                    )::int AS matches_6h
                FROM search_log
            )
            SELECT
                download_counts.total,
                download_counts.imported,
                match_counts.matches_24h,
                match_counts.matches_6h
            FROM download_counts
            CROSS JOIN match_counts
        """)
        row = cur.fetchone()
        assert row is not None, "CROSS JOIN of aggregates always yields a row"
        return DownloadLogCounts(
            total=row["total"],
            imported=row["imported"],
            matches_24h=row["matches_24h"],
            matches_6h=row["matches_6h"],
        )


    def get_log(self, limit: int = 50,
                outcome_filter: str | None = None,
                ) -> list[DownloadLogWithRequestRow]:
        """Get recent download_log entries joined with album_requests.

        Args:
            limit: max entries to return
            outcome_filter: "imported" (success + force_import),
                           "rejected" (rejected + failed + timeout +
                           measurement_failed),
                           or None for all
        """
        query = _LOG_QUERY_TEMPLATE.format(
            candidate_evidence_columns=_CANDIDATE_EVIDENCE_COLUMNS,
            where=_LOG_OUTCOME_FILTERS.get(outcome_filter or "", ""),
        )
        cur = self._execute(query, (limit,))
        return [
            download_log_with_request_row(
                overlay_evidence_onto_download_log_row(dict(r))
            )
            for r in cur.fetchall()
        ]


    def get_linked_import_logs(
        self,
        source_log_ids: list[int],
    ) -> list[DownloadLogWithOriginRow]:
        """Fetch mutating successors for an explicit set of audit rows.

        Recents filters select the rows which are displayed, but a kept
        wrong-match row still needs its mutating successor. Active force-import
        and historical manual-import rows explicitly link back to it. Fetch
        those companions independently so filters cannot change the evidence.
        """
        if not source_log_ids:
            return []
        cur = self._execute(
            """
            SELECT dl.*,
                   origin.beets_distance AS original_beets_distance
            FROM download_log dl
            LEFT JOIN download_log origin
                ON origin.id = dl.source_download_log_id
            WHERE dl.source_download_log_id = ANY(%s)
              AND dl.outcome IN ('success', 'force_import', 'manual_import', 'local_import')
            ORDER BY dl.id DESC
            """,
            ([int(log_id) for log_id in source_log_ids],),
        )
        return [download_log_with_origin_row(row) for row in cur.fetchall()]


    # --- Download logging ---

    def log_download(self, request_id: int,
                     soulseek_username: str | None = None,
                     contributor_usernames: Sequence[str] | None = None,
                     filetype: str | None = None,
                     download_path: str | None = None,
                     beets_distance: float | None | ValidationProjectionUnset = (
                         VALIDATION_PROJECTION_UNSET),
                     beets_scenario: str | None | ValidationProjectionUnset = (
                         VALIDATION_PROJECTION_UNSET),
                     beets_detail: str | None = None,
                     valid: bool | None = None,
                     outcome: DownloadLogOutcome | None = None,
                     staged_path: str | None = None,
                     error_message: str | None = None,
                     bitrate: int | None = None,
                     sample_rate: int | None = None,
                     bit_depth: int | None = None,
                     is_vbr: bool | None = None,
                     was_converted: bool | None = None,
                     original_filetype: str | None = None,
                     # Spectral quality verification fields
                     slskd_filetype: str | None = None,
                     actual_filetype: str | None = None,
                     actual_min_bitrate: int | None = None,
                     spectral_grade: str | None = None,
                     spectral_bitrate: int | None = None,
                     existing_min_bitrate: int | None = None,
                     existing_spectral_bitrate: int | None = None,
                     # Full import_one.py result (JSON string)
                     import_result: Any = None,
                     # Full validation result (JSON string)
                     validation_result: Any = None,
                     # Final format on disk
                     final_format: str | None = None,
                     v0_probe_kind: str | None = None,
                     v0_probe_min_bitrate: int | None = None,
                     v0_probe_avg_bitrate: int | None = None,
                     v0_probe_median_bitrate: int | None = None,
                     existing_v0_probe_kind: str | None = None,
                     existing_v0_probe_min_bitrate: int | None = None,
                     existing_v0_probe_avg_bitrate: int | None = None,
                     existing_v0_probe_median_bitrate: int | None = None,
                     # Per-file failure detail audit blob (issue #564 C7,
                     # migration 043) — a list of FileFailureDetail
                     # dicts (via msgspec.to_builtins), or None.
                     transfer_detail: Any = None,
                     source_download_log_id: int | None = None,
                     # Migration 037 discriminator (``'slskd'`` / ``'youtube'``
                     # / ``'local'`` as of migration 080). Defaults to
                     # ``'slskd'`` so every existing DIRECT caller of this
                     # method is unaffected. Three writers reach the
                     # operator-visible TERMINAL ``download_log`` row:
                     #   - job-BACKED outcomes (accept or reject) never call
                     #     this method at all — they go through
                     #     ``_insert_terminal_download_audit``
                     #     (lib/pipeline_db/terminal_outcomes.py), which
                     #     derives ``source`` from the linked
                     #     ``import_jobs.job_type`` in its own SQL CASE
                     #     (widened for ``'local_import'`` -> ``'local'``
                     #     alongside this migration, issue #1176 PR1
                     #     round 2);
                     #   - job-less REJECTIONS (``_finalize_request_and_
                     #     log_rejection`` in ``lib/dispatch/
                     #     outcome_actions.py``, reached when
                     #     ``import_job_id`` is absent) commit through
                     #     ``PipelineDB.persist_request_rejection_outcome``
                     #     (issue #1355 item 3), whose own private
                     #     ``_insert_nonjob_download_audit`` is the writer —
                     #     NOT this method — so the audit row lands in the
                     #     same transaction as the request transition and
                     #     any denylist/cooldown writes;
                     #   - job-less SUCCESS (``_do_mark_done``'s job-less
                     #     branch) calls THIS method directly, so THIS
                     #     parameter genuinely is the writer for that row —
                     #     currently always the ``'slskd'`` default, since
                     #     that caller never passes ``source=`` either.
                     # ``insert_youtube_running`` is a fourth, wholly
                     # separate INSERT — the queue-only
                     # ``'youtube_running'`` writer — unrelated to any lane
                     # above. Several other call sites in this repo call
                     # this method directly for a non-terminal write (an
                     # in-progress timeout retry, a manual/local import
                     # audit, ...); this comment is scoped to the terminal-
                     # outcome writers only, not every caller.
                     source: str = "slskd",
                     ) -> int:
        beets_distance_value, beets_scenario_value = derive_validation_log_columns(
            validation_result,
            beets_distance=beets_distance,
            beets_scenario=beets_scenario,
        )
        normalized_contributors = list(normalize_contributor_usernames(
            contributor_usernames or (),
        )) or None
        cur = self._execute("""
            INSERT INTO download_log (
                request_id, soulseek_username, candidate_contributor_usernames,
                filetype, download_path,
                beets_distance, beets_scenario, beets_detail, valid,
                outcome, staged_path, error_message,
                bitrate, sample_rate, bit_depth, is_vbr,
                was_converted, original_filetype,
                slskd_filetype,
                actual_filetype, actual_min_bitrate,
                spectral_grade, spectral_bitrate,
                existing_min_bitrate, existing_spectral_bitrate,
                import_result, validation_result, final_format,
                v0_probe_kind, v0_probe_min_bitrate,
                v0_probe_avg_bitrate, v0_probe_median_bitrate,
                existing_v0_probe_kind, existing_v0_probe_min_bitrate,
                existing_v0_probe_avg_bitrate, existing_v0_probe_median_bitrate,
                transfer_detail, source_download_log_id, source
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                      %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                      %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
        """, (
            request_id, soulseek_username, normalized_contributors,
            filetype, download_path,
            beets_distance_value, beets_scenario_value, beets_detail, valid,
            outcome, staged_path, error_message,
            bitrate, sample_rate, bit_depth, is_vbr,
            was_converted, original_filetype,
            slskd_filetype,
            actual_filetype, actual_min_bitrate,
            spectral_grade, spectral_bitrate,
            existing_min_bitrate, existing_spectral_bitrate,
            import_result, validation_result, final_format,
            v0_probe_kind, v0_probe_min_bitrate,
            v0_probe_avg_bitrate, v0_probe_median_bitrate,
            existing_v0_probe_kind, existing_v0_probe_min_bitrate,
            existing_v0_probe_avg_bitrate, existing_v0_probe_median_bitrate,
            psycopg2.extras.Json(transfer_detail)
            if transfer_detail is not None else None,
            source_download_log_id,
            source,
        ))
        row = cur.fetchone()
        self.conn.commit()
        assert row is not None, "INSERT RETURNING should always return a row"
        return int(row["id"])


    def get_download_log_entry(
        self, log_id: int,
    ) -> DownloadLogWithEvidenceRow | None:
        """Get a single download_log entry by its ID."""
        cur = self._execute(
            f"""
            SELECT dl.*,
                   {_CANDIDATE_EVIDENCE_COLUMNS}
                   ar.mb_release_id AS _request_mb_release_id,
                   origin.beets_distance AS original_beets_distance
            FROM download_log dl
            JOIN album_requests ar ON ar.id = dl.request_id
            LEFT JOIN album_quality_evidence e
                ON e.id = dl.candidate_evidence_id
            LEFT JOIN download_log origin
                ON origin.id = dl.source_download_log_id
            WHERE dl.id = %s
            """,
            (log_id,),
        )
        row = cur.fetchone()
        return download_log_with_evidence_row(
            overlay_evidence_onto_download_log_row(dict(row))
        ) if row else None


    def get_download_history(
        self, request_id: int,
    ) -> list[DownloadLogWithEvidenceRow]:
        cur = self._execute(
            f"""
            SELECT dl.*,
                   {_CANDIDATE_EVIDENCE_COLUMNS}
                   ar.mb_release_id AS _request_mb_release_id,
                   origin.beets_distance AS original_beets_distance
            FROM download_log dl
            JOIN album_requests ar ON ar.id = dl.request_id
            LEFT JOIN album_quality_evidence e
                ON e.id = dl.candidate_evidence_id
            LEFT JOIN download_log origin
                ON origin.id = dl.source_download_log_id
            WHERE dl.request_id = %s
            ORDER BY dl.id DESC
            """,
            (request_id,),
        )
        return [
            download_log_with_evidence_row(
                overlay_evidence_onto_download_log_row(dict(r))
            )
            for r in cur.fetchall()
        ]


    def get_download_history_batch(
        self, request_ids: list[int],
    ) -> dict[int, list[DownloadLogWithEvidenceRow]]:
        """Batch fetch download history for multiple request IDs.

        Returns dict of request_id → list of history rows (most recent first).
        """
        if not request_ids:
            return {}
        cur = self._execute(
            f"""
            SELECT dl.*,
                   {_CANDIDATE_EVIDENCE_COLUMNS}
                   ar.mb_release_id AS _request_mb_release_id,
                   origin.beets_distance AS original_beets_distance
            FROM download_log dl
            JOIN album_requests ar ON ar.id = dl.request_id
            LEFT JOIN album_quality_evidence e
                ON e.id = dl.candidate_evidence_id
            LEFT JOIN download_log origin
                ON origin.id = dl.source_download_log_id
            WHERE dl.request_id = ANY(%s)
            ORDER BY dl.id DESC
            """,
            ([int(request_id) for request_id in request_ids],),
        )
        result: dict[int, list[DownloadLogWithEvidenceRow]] = {}
        for row in cur.fetchall():
            r = download_log_with_evidence_row(
                overlay_evidence_onto_download_log_row(dict(row))
            )
            rid = r["request_id"]
            if rid not in result:
                result[rid] = []
            result[rid].append(r)
        return result


    def get_latest_download_summaries(
        self, request_ids: list[int],
    ) -> dict[int, LatestDownloadSummary]:
        """Batch fetch only the NEWEST download_log row + history count
        per request: ``{request_id: {"latest": row, "count": n}}``.

        #426: the pipeline queue only renders the latest verdict and a
        count, but ``get_download_history_batch`` dragged every
        historical row (with fat JSONB) through Postgres and Python to
        get them. ``DISTINCT ON`` returns one detoasted row per request;
        the count aggregate never touches the JSONB columns.
        """
        if not request_ids:
            return {}
        ids = [int(r) for r in request_ids]
        latest_cur = self._execute(
            f"""
            SELECT * FROM (
                SELECT DISTINCT ON (dl.request_id)
                       dl.*,
                       {_CANDIDATE_EVIDENCE_COLUMNS}
                       ar.mb_release_id AS _request_mb_release_id,
                       origin.beets_distance AS original_beets_distance
                FROM download_log dl
                JOIN album_requests ar ON ar.id = dl.request_id
                LEFT JOIN album_quality_evidence e
                    ON e.id = dl.candidate_evidence_id
                LEFT JOIN download_log origin
                    ON origin.id = dl.source_download_log_id
                WHERE dl.request_id = ANY(%s)
                ORDER BY dl.request_id, dl.id DESC
            ) latest
            """,
            (ids,),
        )
        result: dict[int, LatestDownloadSummary] = {}
        for row in latest_cur.fetchall():
            r = download_log_with_evidence_row(
                overlay_evidence_onto_download_log_row(dict(row))
            )
            result[int(r["request_id"])] = {"latest": r, "count": 0}

        count_cur = self._execute(
            "SELECT request_id, COUNT(*)::int AS n FROM download_log"
            " WHERE request_id = ANY(%s) GROUP BY request_id",
            (ids,),
        )
        for row in count_cur.fetchall():
            rid = int(row["request_id"])
            if rid in result:
                result[rid]["count"] = int(row["n"])
        return result


    # -- Wrong matches ---------------------------------------------------------

    def get_wrong_matches(self) -> list[WrongMatchCandidateRow]:
        """Return every rejected wrong-match candidate still on disk.

        Issue #113: one row per actionable folder, not one per request.
        ``download_log`` accumulates multiple rejected rows for the same
        ``failed_path`` whenever a folder is retried (force-import logs
        the same ``failed_path`` on every retry), so we collapse to the newest
        row per ``(request_id, failed_path)`` pair — each surviving row
        represents a distinct on-disk directory the user can act on.

        Only candidate/pressing-match rejections survive. Folder/audio facts
        and spectral-quality rejects have their own handling and stay out of
        the manual-review queue; their taxonomy lives in
        ``lib.wrong_match_policy``.
        """
        from lib.wrong_matches import wrong_match_row_is_visible

        # Pull the per-candidate quality measurement straight from the
        # canonical evidence row (FK on download_log.candidate_evidence_id).
        # The legacy denorm columns on download_log are NULL for every
        # wrong-match reject — they only get populated for the request's
        # current-state row. COALESCE keeps the older audit history working
        # if any pre-evidence rows are still around.
        #
        # Both evidence joins also project the audit-only accusation
        # columns (issue #829 Phase 5 PR4): the per-entry chip renders the
        # candidate's grade and the group badge renders the installed
        # copy's, so each needs the codec facts for ITS OWN measurement.
        cur = self._execute(f"""
            SELECT DISTINCT ON (dl.request_id, dl.validation_result->>'{FAILED_PATH_KEY}')
                dl.id AS download_log_id,
                dl.request_id,
                ar.artist_name,
                ar.album_title,
                ar.mb_release_id,
                ar.mb_release_group_id,
                dl.soulseek_username,
                dl.validation_result,
                COALESCE(e.spectral_grade, dl.spectral_grade) AS spectral_grade,
                COALESCE(e.spectral_bitrate_kbps, dl.spectral_bitrate) AS spectral_bitrate,
                COALESCE(
                    CASE e.v0_subject
                        WHEN 'source' THEN 'lossless_source_v0'
                        WHEN 'installed' THEN 'native_lossy_research_v0'
                    END,
                    dl.v0_probe_kind
                ) AS v0_probe_kind,
                COALESCE(e.v0_avg_bitrate_kbps, dl.v0_probe_avg_bitrate) AS v0_probe_avg_bitrate,
                e.codec AS evidence_source_codec,
                e.container AS evidence_source_container,
                e.storage_format AS evidence_storage_format,
                e.target_format AS evidence_target_format,
                e.target_is_cbr AS evidence_target_is_cbr,
                e.lineage_version AS evidence_lineage_version,
                e.min_bitrate_kbps AS evidence_min_bitrate,
                e.avg_bitrate_kbps AS evidence_avg_bitrate,
                e.verified_lossless AS evidence_verified_lossless,
                dl.import_result->>'decision' AS terminal_import_decision,
                ar.status AS request_status,
                ar.min_bitrate AS request_min_bitrate,
                ar.verified_lossless AS request_verified_lossless,
                ar.current_spectral_grade AS request_current_spectral_grade,
                ar.current_spectral_bitrate AS request_current_spectral_bitrate,
{accusation_evidence_columns('e', CANDIDATE_EVIDENCE_PREFIX)}{accusation_evidence_columns('current_evidence', CURRENT_EVIDENCE_PREFIX).rstrip().rstrip(',')}
            FROM download_log dl
            JOIN album_requests ar ON dl.request_id = ar.id
            LEFT JOIN album_quality_evidence e
                ON e.id = dl.candidate_evidence_id
            LEFT JOIN album_quality_evidence current_evidence
                ON current_evidence.id = ar.current_evidence_id
            WHERE dl.outcome = 'rejected'
              AND dl.validation_result->>'{FAILED_PATH_KEY}' IS NOT NULL
            ORDER BY dl.request_id, dl.validation_result->>'{FAILED_PATH_KEY}', dl.id DESC
        """)
        rows = [
            row for raw in cur.fetchall()
            if wrong_match_row_is_visible(
                row := wrong_match_candidate_row(raw), include_replaced=True,
            )
        ]
        # DISTINCT ON sorts by path within a request; re-sort so the route
        # layer sees newest-first within each request, matching the frontend
        # expectation that the most-recent candidate appears first.
        rows.sort(key=lambda r: (r["request_id"], -int(r["download_log_id"])))
        return rows


    def clear_wrong_match_path(self, log_id: int) -> bool:
        """Null out failed_path in validation_result for a download_log entry.

        Returns True if the entry was found and updated.
        """
        cur = self._execute(f"""
            UPDATE download_log
            SET validation_result = validation_result - '{FAILED_PATH_KEY}'
            WHERE id = %s AND validation_result->>'{FAILED_PATH_KEY}' IS NOT NULL
        """, (log_id,))
        return cur.rowcount > 0


    def clear_wrong_match_paths(
        self,
        request_id: int,
        failed_paths: list[str] | tuple[str, ...] | set[str],
    ) -> int:
        """Null out failed_path for rejected rows matching request/path pairs."""
        paths = [str(path) for path in dict.fromkeys(failed_paths) if path]
        if not paths:
            return 0
        placeholders = ", ".join(["%s"] * len(paths))
        cur = self._execute(f"""
            UPDATE download_log
            SET validation_result = validation_result - '{FAILED_PATH_KEY}'
            WHERE request_id = %s
              AND outcome = 'rejected'
              AND validation_result->>'{FAILED_PATH_KEY}' IN ({placeholders})
        """, (request_id, *paths))
        self.conn.commit()
        return cur.rowcount


    def record_wrong_match_triage(
        self,
        log_id: int,
        triage_result: WrongMatchTriageAudit,
    ) -> bool:
        """Persist cleanup audit details on a download_log row."""
        cur = self._execute(f"""
            UPDATE download_log
            SET validation_result = jsonb_set(
                CASE
                    WHEN jsonb_typeof(validation_result) = 'object'
                    THEN validation_result
                    ELSE '{{}}'::jsonb
                END,
                '{{{WRONG_MATCH_TRIAGE_KEY}}}',
                %s::jsonb,
                true
            )
            WHERE id = %s
        """, (msgspec.json.encode(triage_result).decode(), log_id))
        self.conn.commit()
        return cur.rowcount > 0


    def get_recent_successful_uploader(
        self,
        request_id: int,
    ) -> str | None:
        """Return the most recent successful uploader for this request.

        Used by the ban-source route to resolve `reported_username`
        server-side. Considers both `success` and `force_import` outcomes.
        """
        cur = self._execute("""
            SELECT soulseek_username
            FROM download_log
            WHERE request_id = %s
              AND outcome IN ('success', 'force_import', 'local_import')
              AND soulseek_username IS NOT NULL
            ORDER BY id DESC
            LIMIT 1
        """, (request_id,))
        row = cur.fetchone()
        return row["soulseek_username"] if row else None
