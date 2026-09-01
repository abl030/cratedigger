"""FakePipelineDB download_log cluster — mirrors ``lib/pipeline_db/download_log.py``.

``download_log`` rows and their evidence overlay.
"""
from __future__ import annotations

import copy
import json
from collections.abc import (
    Sequence,
)
from datetime import timedelta
from typing import (
    TYPE_CHECKING,
    Any,
    cast,
)

import msgspec

if TYPE_CHECKING:
    from lib.pipeline_db import (
        DownloadLogWithEvidenceRow,
        DownloadLogWithOriginRow,
        DownloadLogWithRequestRow,
        LatestDownloadSummary,
        WrongMatchCandidateRow,
    )
from lib.pipeline_db import (
    DOWNLOAD_LOG_OUTCOMES,
    DownloadLogCounts,
)
from lib.pipeline_db._shared import (
    CANDIDATE_EVIDENCE_PREFIX,
    CURRENT_EVIDENCE_PREFIX,
)
from lib.pipeline_db.download_log import (
    LINKED_IMPORT_OUTCOMES,
    LOG_FILTER_IMPORTED_OUTCOMES,
    LOG_FILTER_REJECTED_OUTCOMES,
    overlay_evidence_onto_download_log_row,
)
from lib.quality import (
    EVIDENCE_SUBJECT_SOURCE,
    V0_PROBE_LOSSLESS_SOURCE,
    V0_PROBE_NATIVE_LOSSY_RESEARCH,
    AlbumQualityEvidence,
)
from lib.validation_envelope import (
    VALIDATION_PROJECTION_UNSET,
    ValidationProjectionUnset,
    WrongMatchTriageAudit,
    derive_validation_log_columns,
)
from tests.fakes._shared import _utcnow
from tests.fakes.pipeline_db._base import _FakePipelineDBBase
from tests.fakes.pipeline_db._shared import _jsonb_column
from tests.fakes.rows import (
    DownloadLogRow,
)


class _FakeDownloadLogMixin(_FakePipelineDBBase):
    """``download_log`` rows and their evidence overlay."""


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
                     outcome: str | None = None,
                     staged_path: str | None = None,
                     error_message: str | None = None,
                     bitrate: int | None = None,
                     sample_rate: int | None = None,
                     bit_depth: int | None = None,
                     is_vbr: bool | None = None,
                     was_converted: bool | None = None,
                     original_filetype: str | None = None,
                     slskd_filetype: str | None = None,
                     actual_filetype: str | None = None,
                     actual_min_bitrate: int | None = None,
                     spectral_grade: str | None = None,
                     spectral_bitrate: int | None = None,
                     existing_min_bitrate: int | None = None,
                     existing_spectral_bitrate: int | None = None,
                     import_result: Any = None,
                     validation_result: Any = None,
                     final_format: str | None = None,
                     v0_probe_kind: str | None = None,
                     v0_probe_min_bitrate: int | None = None,
                     v0_probe_avg_bitrate: int | None = None,
                     v0_probe_median_bitrate: int | None = None,
                     existing_v0_probe_kind: str | None = None,
                     existing_v0_probe_min_bitrate: int | None = None,
                     existing_v0_probe_avg_bitrate: int | None = None,
                     existing_v0_probe_median_bitrate: int | None = None,
                     transfer_detail: Any = None,
                     source_download_log_id: int | None = None,
                     source: str = "slskd",
                     **extra: Any) -> int:
        """Record a download_log row.

        Every parameter name matches ``PipelineDB.log_download`` exactly
        — the contract test in ``test_fakes.py`` enforces this. Only
        the 12 "first-class" fields land on ``DownloadLogRow``; the
        remaining named fields plus any test-only ``**extra`` merge into
        ``.extra`` so ``assert_log`` can still introspect them.
        """
        if request_id is None:
            # Mirror production: download_log.request_id is NOT NULL
            # (test-fidelity Rule B — the fake must not be more
            # permissive than the real INSERT).
            import psycopg2.errors

            raise psycopg2.errors.NotNullViolation(
                'null value in column "request_id" of relation '
                '"download_log" violates not-null constraint'
            )
        if outcome is not None and outcome not in DOWNLOAD_LOG_OUTCOMES:
            # Mirror download_log_outcome_check (migration 037) — a fake
            # that accepts any string shipped an outcome production
            # rejects (#146 phase-3 grace escape, 2026-07-02).
            import psycopg2.errors

            raise psycopg2.errors.CheckViolation(
                'new row for relation "download_log" violates check '
                f'constraint "download_log_outcome_check" (outcome={outcome!r})'
            )
        beets_distance_value, beets_scenario_value = derive_validation_log_columns(
            validation_result,
            beets_distance=beets_distance,
            beets_scenario=beets_scenario,
        )
        from lib.convergence_service import normalize_contributor_usernames

        new_log_id = self._mint_download_log_id()
        normalized_contributors = list(normalize_contributor_usernames(
            contributor_usernames or (),
        )) or None
        auxiliary: dict[str, Any] = {
            "download_path": download_path,
            "valid": valid,
            "bitrate": bitrate,
            "sample_rate": sample_rate,
            "bit_depth": bit_depth,
            "is_vbr": is_vbr,
            "was_converted": was_converted,
            "original_filetype": original_filetype,
            "slskd_filetype": slskd_filetype,
            "actual_filetype": actual_filetype,
            "actual_min_bitrate": actual_min_bitrate,
            "spectral_grade": spectral_grade,
            "spectral_bitrate": spectral_bitrate,
            "existing_min_bitrate": existing_min_bitrate,
            "existing_spectral_bitrate": existing_spectral_bitrate,
            "final_format": final_format,
            "v0_probe_kind": v0_probe_kind,
            "v0_probe_min_bitrate": v0_probe_min_bitrate,
            "v0_probe_avg_bitrate": v0_probe_avg_bitrate,
            "v0_probe_median_bitrate": v0_probe_median_bitrate,
            "existing_v0_probe_kind": existing_v0_probe_kind,
            "existing_v0_probe_min_bitrate": existing_v0_probe_min_bitrate,
            "existing_v0_probe_avg_bitrate": existing_v0_probe_avg_bitrate,
            "existing_v0_probe_median_bitrate": existing_v0_probe_median_bitrate,
        }
        auxiliary.update(extra)
        self.download_logs.append(DownloadLogRow(
            request_id=request_id,
            outcome=outcome,
            soulseek_username=soulseek_username,
            filetype=filetype,
            beets_distance=beets_distance_value,
            beets_scenario=beets_scenario_value,
            beets_detail=beets_detail,
            staged_path=staged_path,
            error_message=error_message,
            validation_result=validation_result,
            import_result=import_result,
            transfer_detail=transfer_detail,
            id=new_log_id,
            candidate_contributor_usernames=normalized_contributors,
            source_download_log_id=source_download_log_id,
            source=source,
            extra=auxiliary,
        ))
        return new_log_id

    def get_recent_successful_uploader(
        self,
        request_id: int,
    ) -> str | None:
        """Most recent successful uploader for this request, or None."""
        for entry in reversed(self.download_logs):
            if entry.request_id != request_id:
                continue
            if entry.outcome not in ("success", "force_import", "local_import"):
                continue
            if entry.soulseek_username is None:
                continue
            return entry.soulseek_username
        return None

    def get_retained_failure_paths(self) -> set[str]:
        retained: set[str] = set()
        for row in self.download_logs:
            if (
                row.outcome == "measurement_failed"
                and row.staged_path is not None
                and row.staged_path.strip()
            ):
                retained.add(row.staged_path)
            raw = row.validation_result
            if isinstance(raw, (str, bytes)):
                try:
                    raw = msgspec.json.decode(raw)
                except msgspec.DecodeError:
                    raw = None
            if (
                isinstance(raw, dict)
                and "post_commit_quarantine" in raw
                and isinstance(raw.get("failed_path"), str)
                and raw["failed_path"].strip()
            ):
                retained.add(raw["failed_path"])
        return retained

    def get_download_log_counts(self) -> DownloadLogCounts:
        """Mirror of ``PipelineDB.get_download_log_counts`` — computed
        from the fake's real ``download_logs``/``search_logs`` state,
        never queued (#445 item 2). Parity with the production SQL is
        pinned by ``tests/test_pipeline_db.py::TestGetDownloadLogCounts``.
        """
        now = _utcnow()
        total = len(self.download_logs)
        imported = sum(
            1 for e in self.download_logs
            if e.outcome in ("success", "force_import", "local_import"))
        found_24h = sum(
            1 for e in self.search_logs
            if e.outcome == "found"
            and self._as_utc(e.created_at) >= now - timedelta(hours=24))
        found_6h = sum(
            1 for e in self.search_logs
            if e.outcome == "found"
            and self._as_utc(e.created_at) >= now - timedelta(hours=6))
        return DownloadLogCounts(
            total=total, imported=imported,
            matches_24h=found_24h, matches_6h=found_6h)

    def get_log(self, limit: int = 50,
                outcome_filter: str | None = None,
                ) -> list[DownloadLogWithRequestRow]:
        imported = frozenset(LOG_FILTER_IMPORTED_OUTCOMES)
        rejected = frozenset(LOG_FILTER_REJECTED_OUTCOMES)
        rows: list[dict[str, object]] = []
        # Newest-first to match the real ORDER BY dl.created_at DESC.
        for entry in reversed(self.download_logs):
            if outcome_filter == "imported" and entry.outcome not in imported:
                continue
            if outcome_filter == "rejected" and entry.outcome not in rejected:
                continue
            req = self._requests.get(entry.request_id, {})
            # Real SQL is ``SELECT dl.*, ar.album_title, …`` — every
            # download_log column must appear, including the auxiliary
            # fields ``log_download`` parks in ``entry.extra``
            # (bitrate, actual_filetype, spectral_grade, final_format,
            # etc.). Dropping them here would silently mis-classify rows
            # in callers that feed ``get_log`` into LogEntry.from_row.
            joined: dict[str, object] = self._download_log_raw_evidence_row(
                entry)
            joined.update({
                # Joined request columns.
                "album_title": req.get("album_title"),
                "artist_name": req.get("artist_name"),
                "mb_release_id": req.get("mb_release_id"),
                "year": req.get("year"),
                "country": req.get("country"),
                "request_status": req.get("status"),
                "request_min_bitrate": req.get("min_bitrate"),
                "prev_min_bitrate": req.get("prev_min_bitrate"),
                "search_filetype_override": req.get(
                    "search_filetype_override"),
                "request_source": req.get("source"),
            })
            # ``get_log`` is the one reader whose SQL also LEFT JOINs the
            # request's CURRENT evidence. Project those aliases RAW — the
            # identity gate below them belongs to the shared overlay, which
            # NULLs the whole ``_current_evidence_*`` family when
            # ``_current_evidence_mb_release_id`` names another pressing.
            current_evidence_id = req.get("current_evidence_id")
            current_evidence = (
                self._evidence_by_id.get(int(current_evidence_id))
                if current_evidence_id is not None else None
            )
            current_measurement = (
                current_evidence.measurement
                if current_evidence is not None else None
            )
            current_v0 = (
                current_evidence.v0_metric
                if current_evidence is not None else None
            )
            joined.update({
                "_current_evidence_id": (
                    current_evidence.id if current_evidence is not None else None
                ),
                "_current_evidence_mb_release_id": (
                    current_evidence.mb_release_id
                    if current_evidence is not None else None
                ),
                "_current_evidence_is_pre_attempt": (
                    current_evidence.measured_at <= entry.created_at
                    if current_evidence is not None else None
                ),
                "_current_evidence_min_bitrate": (
                    current_measurement.min_bitrate_kbps
                    if current_measurement is not None else None
                ),
                "_current_evidence_avg_bitrate": (
                    current_measurement.avg_bitrate_kbps
                    if current_measurement is not None else None
                ),
                "_current_evidence_median_bitrate": (
                    current_measurement.median_bitrate_kbps
                    if current_measurement is not None else None
                ),
                # issue #829 Phase 5 PR4 — the installed album's own
                # codec-resolution facts, seeded from the evidence row so
                # the HAVE grade chip's audit-only flag is derived, never
                # invented. Same nine aliases the production fragment
                # projects, from the same helper the other two joins use.
                **self._accusation_alias_projection(
                    current_evidence, CURRENT_EVIDENCE_PREFIX),
                "_current_evidence_v0_probe_kind": (
                    current_v0.subject if current_v0 is not None else None
                ),
                "_current_evidence_v0_probe_min_bitrate": (
                    current_v0.min_bitrate_kbps if current_v0 is not None else None
                ),
                "_current_evidence_v0_probe_avg_bitrate": (
                    current_v0.avg_bitrate_kbps if current_v0 is not None else None
                ),
                "_current_evidence_v0_probe_median_bitrate": (
                    current_v0.median_bitrate_kbps if current_v0 is not None else None
                ),
            })
            rows.append(overlay_evidence_onto_download_log_row(joined))
            if len(rows) >= limit:
                break
        return cast("list[DownloadLogWithRequestRow]", rows)

    def get_linked_import_logs(
        self,
        source_log_ids: list[int],
    ) -> list[DownloadLogWithOriginRow]:
        wanted = {int(log_id) for log_id in source_log_ids}
        return cast("list[DownloadLogWithOriginRow]", [
            self._download_log_base_dict(entry)
            for entry in reversed(self.download_logs)
            if entry.source_download_log_id in wanted
            and entry.outcome in LINKED_IMPORT_OUTCOMES
        ])

    def get_download_log_entry(self,
                               log_id: int) -> DownloadLogWithEvidenceRow | None:
        for entry in self.download_logs:
            if entry.id == log_id:
                return cast(
                    "DownloadLogWithEvidenceRow",
                    self._download_log_evidence_dict(entry),
                )
        return None

    def get_download_history(self,
                             request_id: int) -> list[DownloadLogWithEvidenceRow]:
        return cast("list[DownloadLogWithEvidenceRow]", [
            self._download_log_evidence_dict(e)
            for e in reversed(self.download_logs)
            if e.request_id == request_id
        ])

    def get_download_history_batch(
        self, request_ids: list[int],
    ) -> dict[int, list[DownloadLogWithEvidenceRow]]:
        wanted = set(request_ids)
        result: dict[int, list[dict[str, Any]]] = {}
        for entry in reversed(self.download_logs):
            if entry.request_id not in wanted:
                continue
            result.setdefault(entry.request_id, []).append(
                self._download_log_evidence_dict(entry))
        return cast("dict[int, list[DownloadLogWithEvidenceRow]]", result)

    def get_latest_download_summaries(
        self, request_ids: list[int],
    ) -> dict[int, LatestDownloadSummary]:
        """Mirror ``PipelineDB.get_latest_download_summaries``: newest
        row + history count per request (#426)."""
        return cast("dict[int, LatestDownloadSummary]", {
            rid: {"latest": history[0], "count": len(history)}
            for rid, history in
            self.get_download_history_batch(request_ids).items()
        })

    def _download_log_base_dict(self,
                                entry: DownloadLogRow) -> dict[str, Any]:
        """The ``SELECT dl.*`` projection plus the origin join.

        The shape ``PipelineDB.get_linked_import_logs`` returns: every
        ``download_log`` column (the auxiliary ones ``log_download`` parks
        in ``entry.extra`` included) and ``original_beets_distance``. That
        query has NO ``album_quality_evidence`` join, so nothing here may
        recover an evidence-derived measurement (issue #1278 item 7 — the
        fake used to fold evidence in here and hand linked-import rows
        spectral/V0 facts production never returns).
        """
        row: dict[str, Any] = {
            "id": entry.id,
            "request_id": entry.request_id,
            "outcome": entry.outcome,
            "soulseek_username": entry.soulseek_username,
            "filetype": entry.filetype,
            "beets_distance": entry.beets_distance,
            "beets_scenario": entry.beets_scenario,
            "beets_detail": entry.beets_detail,
            "staged_path": entry.staged_path,
            "error_message": entry.error_message,
            "validation_result": _jsonb_column(entry.validation_result),
            "import_result": _jsonb_column(entry.import_result),
            # Migration 043 — per-file failure detail audit blob (issue
            # #564 C7).
            "transfer_detail": entry.transfer_detail,
            "created_at": entry.created_at,
            "candidate_evidence_id": entry.candidate_evidence_id,
            "candidate_evidence_direct": entry.candidate_evidence_direct,
            "candidate_contributor_usernames": (
                list(entry.candidate_contributor_usernames)
                if entry.candidate_contributor_usernames is not None
                else None
            ),
            "source_download_log_id": entry.source_download_log_id,
            "original_beets_distance": next(
                (
                    origin.beets_distance
                    for origin in self.download_logs
                    if origin.id == entry.source_download_log_id
                ),
                None,
            ),
            # Migration 037 — source discriminator + YT JSONB. Mirrors
            # the production read seam (every consumer sees these two
            # columns whether or not the row originated from YT).
            "source": entry.source,
            "youtube_metadata": copy.deepcopy(entry.youtube_metadata)
            if entry.youtube_metadata is not None else None,
        }
        row.update(entry.extra)
        return row

    def _candidate_evidence_alias_projection(
        self,
        evidence: AlbumQualityEvidence | None,
    ) -> dict[str, object]:
        """Mirror ``_CANDIDATE_EVIDENCE_COLUMNS`` for one candidate join.

        Raw, ungated column values — the LEFT JOIN alone, before
        ``overlay_evidence_onto_download_log_row`` adjudicates identity
        and lineage. An unmatched join is all-NULL.

        Nine of the aliases are the accusation block that
        ``accusation_evidence_columns`` generates on the production side,
        so they come from ``_accusation_alias_projection`` — the same
        helper ``get_log`` already uses for the current-evidence prefix,
        and the owner of the candidate-prefix
        ``was_converted_from`` -> NULL carve-out. Spelling them a second
        time here would leave a tenth alias free to drift between the two
        copies.
        """
        measurement = evidence.measurement if evidence is not None else None
        v0 = evidence.v0_metric if evidence is not None else None
        lattice = evidence.aac_lattice if evidence is not None else None
        proof = (
            evidence.verified_lossless_proof if evidence is not None else None
        )
        return {
            **self._accusation_alias_projection(
                evidence, CANDIDATE_EVIDENCE_PREFIX),
            "_evidence_mb_release_id": (
                evidence.mb_release_id if evidence is not None else None),
            "_evidence_source_format": (
                measurement.format if measurement is not None else None),
            "_evidence_source_min_bitrate": (
                measurement.min_bitrate_kbps
                if measurement is not None else None),
            "_evidence_source_avg_bitrate": (
                measurement.avg_bitrate_kbps
                if measurement is not None else None),
            "_evidence_source_median_bitrate": (
                measurement.median_bitrate_kbps
                if measurement is not None else None),
            "_evidence_lineage_version": (
                evidence.lineage_version if evidence is not None else None),
            # ``e.v0_subject`` raw — the overlay owns the subject →
            # wire-kind translation (a five-key map applied with ``.get``,
            # so an unmapped subject passes through rather than raising).
            "_evidence_v0_probe_kind": (
                v0.subject if v0 is not None else None),
            "_evidence_v0_probe_min_bitrate": (
                v0.min_bitrate_kbps if v0 is not None else None),
            "_evidence_v0_probe_avg_bitrate": (
                v0.avg_bitrate_kbps if v0 is not None else None),
            "_evidence_v0_probe_median_bitrate": (
                v0.median_bitrate_kbps if v0 is not None else None),
            "_evidence_ultrasonic_deficit_db": (
                measurement.ultrasonic_deficit_db
                if measurement is not None else None),
            "_evidence_spectral_measurement_version": (
                measurement.spectral_measurement_version
                if measurement is not None else None),
            "_evidence_aac_lattice_modal_count": (
                lattice.modal_count if lattice is not None else None),
            "_evidence_aac_lattice_scored_tracks": (
                lattice.scored_tracks if lattice is not None else None),
            "_evidence_aac_lattice_max_z": (
                lattice.max_z if lattice is not None else None),
            # Both proof aliases are raw here; the overlay NULLs them on a
            # non-source-semantic lineage. The fake used to lineage-gate
            # cd_rip_verification by hand and leave the classifier ungated
            # — the exact asymmetry issue #1278 item 7 removed.
            "_evidence_verified_lossless_classifier": (
                proof.classifier if proof is not None else None),
            "_evidence_cd_rip_verification": (
                msgspec.to_builtins(evidence.cd_rip_verification)
                if evidence is not None
                and evidence.cd_rip_verification is not None
                else None),
            "_evidence_container_extensions": (
                sorted({file.extension for file in evidence.files})
                if evidence is not None and evidence.files
                else None),
        }

    def _download_log_raw_evidence_row(
        self, entry: DownloadLogRow,
    ) -> dict[str, object]:
        """The PRE-overlay row an evidence-joined reader's SQL produces.

        ``dl.*`` + the candidate-evidence aliases + the request identity
        gate input. The LEFT JOIN is keyed on ``candidate_evidence_id``
        alone — no identity filter here, because adjudicating identity is
        the overlay's job, not the join's.
        """
        row = self._download_log_base_dict(entry)
        evidence = (
            self._evidence_by_id.get(entry.candidate_evidence_id)
            if entry.candidate_evidence_id is not None
            else None
        )
        row.update(self._candidate_evidence_alias_projection(evidence))
        request = self._requests.get(entry.request_id)
        row["_request_mb_release_id"] = (
            request.get("mb_release_id") if request is not None else None
        )
        return row

    def _download_log_evidence_dict(self, entry: DownloadLogRow) -> dict[str, Any]:
        """One evidence-joined ``download_log`` row, overlaid by production.

        Delegates the whole identity/lineage adjudication — including the
        guaranteed-present ``source_*`` keys of issue #784's
        ``DownloadLogWithEvidenceRow`` — to
        ``overlay_evidence_onto_download_log_row``, so the fake cannot be
        more permissive than production about which evidence facts a
        reader recovers.

        Used by every reader that joins ``album_quality_evidence``
        (``get_download_log_entry``, ``get_download_history``,
        ``get_download_history_batch``, ``get_latest_download_summaries``;
        ``get_log`` builds its own raw row so the current-evidence aliases
        reach the same single overlay call). ``get_linked_import_logs``
        has no evidence join in production and must call the bare
        ``_download_log_base_dict`` instead.
        """
        return overlay_evidence_onto_download_log_row(
            self._download_log_raw_evidence_row(entry))

    def get_wrong_matches(self) -> list[WrongMatchCandidateRow]:
        """Rejected downloads whose ``validation_result.failed_path`` is set.

        Mirrors the real ``DISTINCT ON (request_id, failed_path)`` —
        collapse to newest per ``(request_id, failed_path)``, then sort
        newest-first within each request.
        """
        from lib.wrong_matches import wrong_match_row_is_visible

        collapsed: dict[tuple[int, str], DownloadLogRow] = {}
        for entry in self.download_logs:
            if entry.outcome != "rejected":
                continue
            vr = self._validation_result_dict(entry.validation_result)
            failed_path = vr.get("failed_path") if vr else None
            if not failed_path:
                continue
            key = (entry.request_id, str(failed_path))
            prev = collapsed.get(key)
            if prev is None or entry.id > prev.id:
                collapsed[key] = entry
        rows: list[dict[str, object]] = []
        for entry in collapsed.values():
            req = self._requests.get(entry.request_id, {})
            # Mirror the real LEFT JOIN to album_quality_evidence: prefer
            # evidence-derived measurements over the legacy denorm columns.
            ev = self._evidence_by_id.get(entry.candidate_evidence_id) \
                if entry.candidate_evidence_id is not None else None
            import_payload = _jsonb_column(entry.import_result)
            ev_measurement = ev.measurement if ev is not None else None
            ev_v0 = ev.v0_metric if ev is not None else None
            spectral_grade = (
                ev_measurement.spectral_grade if ev_measurement is not None
                else None
            ) or entry.extra.get("spectral_grade")
            spectral_bitrate = (
                ev_measurement.spectral_bitrate_kbps
                if ev_measurement is not None else None
            ) or entry.extra.get("spectral_bitrate")
            v0_probe_kind = (
                V0_PROBE_LOSSLESS_SOURCE
                if (
                    ev_v0 is not None
                    and ev_v0.subject == EVIDENCE_SUBJECT_SOURCE
                )
                else V0_PROBE_NATIVE_LOSSY_RESEARCH
                if ev_v0 is not None
                else None
            ) or entry.extra.get("v0_probe_kind")
            v0_probe_avg_bitrate = (
                ev_v0.avg_bitrate_kbps if ev_v0 is not None else None
            ) or entry.extra.get("v0_probe_avg_bitrate")
            row: dict[str, object] = {
                "download_log_id": entry.id,
                "request_id": entry.request_id,
                "artist_name": req.get("artist_name"),
                "album_title": req.get("album_title"),
                "mb_release_id": req.get("mb_release_id"),
                "mb_release_group_id": req.get("mb_release_group_id"),
                "soulseek_username": entry.soulseek_username,
                "validation_result": _jsonb_column(entry.validation_result),
                "spectral_grade": spectral_grade,
                "spectral_bitrate": spectral_bitrate,
                "v0_probe_kind": v0_probe_kind,
                "v0_probe_avg_bitrate": v0_probe_avg_bitrate,
                "evidence_source_codec": (
                    ev.codec if ev is not None else None
                ),
                "evidence_source_container": (
                    ev.container if ev is not None else None
                ),
                "evidence_storage_format": (
                    ev.storage_format if ev is not None else None
                ),
                "evidence_target_format": (
                    ev.target_format if ev is not None else None
                ),
                "evidence_target_is_cbr": (
                    ev.target_is_cbr if ev is not None else None
                ),
                "evidence_lineage_version": (
                    ev.lineage_version if ev is not None else None
                ),
                "evidence_min_bitrate": (
                    ev_measurement.min_bitrate_kbps
                    if ev_measurement is not None else None
                ),
                "evidence_avg_bitrate": (
                    ev_measurement.avg_bitrate_kbps
                    if ev_measurement is not None else None
                ),
                # LEFT JOIN semantics: with no evidence row joined the
                # column is SQL NULL, not FALSE. A bare boolean here told
                # every fake-backed reader "this candidate is provably not
                # verified lossless" where production says "unknown".
                "evidence_verified_lossless": (
                    ev.verified_lossless_proof is not None
                    if ev is not None else None
                ),
                # ``dl.import_result->>'decision'`` reads the JSONB
                # column, so the string form a production writer stored is
                # a dict by the time the operator query touches it.
                "terminal_import_decision": (
                    import_payload.get("decision")
                    if isinstance(import_payload, dict)
                    and isinstance(import_payload.get("decision"), str)
                    else None
                ),
                "request_status": req.get("status"),
                "request_min_bitrate": req.get("min_bitrate"),
                "request_verified_lossless": req.get("verified_lossless"),
                "request_current_spectral_grade": req.get(
                    "current_spectral_grade"),
                "request_current_spectral_bitrate": req.get(
                    "current_spectral_bitrate"),
                **self._accusation_alias_projection(
                    ev, CANDIDATE_EVIDENCE_PREFIX),
                **self._accusation_alias_projection(
                    self._current_evidence_for_request(req),
                    CURRENT_EVIDENCE_PREFIX),
            }
            if wrong_match_row_is_visible(row, include_replaced=True):
                rows.append(row)
        rows.sort(key=lambda r: (
            r["request_id"], -int(r["download_log_id"])))  # type: ignore[arg-type, operator]
        return cast("list[WrongMatchCandidateRow]", rows)

    def clear_wrong_match_path(self, log_id: int) -> bool:
        """Strip ``failed_path`` from a download_log row's validation_result.

        Returns True when the entry was found and carried a failed_path.
        """
        for entry in self.download_logs:
            if entry.id != log_id:
                continue
            vr = self._validation_result_dict(entry.validation_result)
            if not vr or "failed_path" not in vr:
                return False
            new_vr = {k: v for k, v in vr.items() if k != "failed_path"}
            if isinstance(entry.validation_result, str):
                entry.validation_result = json.dumps(new_vr)
            else:
                entry.validation_result = new_vr
            return True
        return False

    def clear_wrong_match_paths(
        self,
        request_id: int,
        failed_paths: list[str] | tuple[str, ...] | set[str],
    ) -> int:
        """Strip ``failed_path`` from rejected rows for request/path pairs."""
        paths = {str(path) for path in failed_paths if path}
        if not paths:
            return 0
        cleared = 0
        for entry in self.download_logs:
            if entry.request_id != request_id or entry.outcome != "rejected":
                continue
            vr = self._validation_result_dict(entry.validation_result)
            if not vr or vr.get("failed_path") not in paths:
                continue
            new_vr = {k: v for k, v in vr.items() if k != "failed_path"}
            if isinstance(entry.validation_result, str):
                entry.validation_result = json.dumps(new_vr)
            else:
                entry.validation_result = new_vr
            cleared += 1
        return cleared

    def record_wrong_match_triage(
        self,
        log_id: int,
        triage_result: WrongMatchTriageAudit,
    ) -> bool:
        for entry in self.download_logs:
            if entry.id != log_id:
                continue
            vr = self._validation_result_dict(entry.validation_result) or {}
            new_vr = dict(vr)
            # Mirror the real writer: msgspec encode honours omit_defaults.
            new_vr["wrong_match_triage"] = msgspec.json.decode(
                msgspec.json.encode(triage_result))
            if isinstance(entry.validation_result, str):
                entry.validation_result = json.dumps(new_vr)
            else:
                entry.validation_result = new_vr
            return True
        return False

    @staticmethod
    def _validation_result_dict(vr: Any) -> dict[str, Any] | None:
        """Read a stored ``validation_result`` for FILTERING, not projection.

        Two decoders touch this one column, deliberately and with
        different jobs (issue #1278 item 7, reader F4). This one answers
        "does this row have a failed_path / a triage blob?" and degrades a
        non-object to ``None`` so a predicate can simply say no.
        ``_jsonb_column`` answers "what does a SELECT hand the caller?"
        and raises instead, because PostgreSQL rejects malformed JSON at
        INSERT and a JSONB column therefore cannot hold any.

        The two can never disagree on a value production could actually
        store: for a stored JSON OBJECT both return the same dict. They
        differ only on inputs a real column cannot hold — a non-JSON
        string, or valid JSON that is not an object — where filtering
        wants a quiet "no" and projection wants the fake to stop being
        more permissive than the database.
        """
        if isinstance(vr, dict):
            return vr
        if isinstance(vr, str):
            try:
                parsed = json.loads(vr)
            except (json.JSONDecodeError, ValueError):
                return None
            return parsed if isinstance(parsed, dict) else None
        return None

