"""Typed download-log presentation helpers shared by detail and pipeline views."""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass

import msgspec

from lib.json_narrow import json_dict
from lib.pipeline_db._shared import CURRENT_EVIDENCE_PREFIX
from web.classify import (
    AccusationFlags,
    ClassifiedEntry,
    LogEntry,
    classify_log_entry,
    evidence_column_accusation_flags,
    proof_gate_projection,
)


@dataclass(frozen=True)
class ClassifiedDownloadLogRow:
    """One raw download_log row plus its shared UI classification."""

    entry: LogEntry
    classified: ClassifiedEntry


class DownloadHistoryViewRow(msgspec.Struct, frozen=True):
    """Frontend contract shared by detail-view download-history panels."""

    id: int
    request_id: int
    outcome: str
    badge: str
    badge_class: str
    border_color: str
    created_at: str | None
    beets_scenario: str | None
    beets_distance: float | None
    # Apply-time beets distance persisted by #863 in import_result JSONB
    # (None on rows predating it) — the card's Distance row shows it next
    # to the validate-time number (issue #865).
    apply_beets_distance: float | None
    source_download_log_id: int | None
    original_beets_distance: float | None
    beets_detail: str | None
    soulseek_username: str | None
    error_message: str | None
    download_path: str | None
    staged_path: str | None
    import_result: str | dict[str, object] | None
    validation_result: str | dict[str, object] | None
    filetype: str | None
    bitrate: int | None
    was_converted: bool | None
    original_filetype: str | None
    actual_filetype: str | None
    actual_min_bitrate: int | None
    slskd_filetype: str | None
    downloaded_label: str
    verdict: str
    summary: str
    # Bounded, deduplicated raw per-transfer text behind the verdict, plus
    # the server-owned label for it (issue #868). The full
    # ``transfer_detail`` array stays log-only by contract; this is the one
    # operator-visible projection of the peer's own words.
    transfer_message: str | None
    transfer_message_label: str | None
    beets_detail_label: str | None
    failure_category: str | None
    analysis_error: str | None
    installed_path: str | None
    candidate_reference: str | None
    # Persisted QualityComparisonBasis as a plain dict (null on rows
    # predating the field) — the detail grid's "Compared" row.
    comparison_basis: dict[str, object] | None
    disambiguation_failure: str | None
    disambiguation_detail: str | None
    bad_extensions: list[str]
    wrong_match_triage_action: str | None
    wrong_match_triage_summary: str | None
    wrong_match_triage_reason: str | None
    wrong_match_triage_preview_verdict: str | None
    wrong_match_triage_preview_decision: str | None
    wrong_match_triage_stage_chain: list[str]
    wrong_match_triage_detail: str | None
    spectral_grade: str | None
    spectral_bitrate: int | None
    existing_min_bitrate: int | None
    existing_avg_bitrate: int | None
    existing_median_bitrate: int | None
    existing_spectral_bitrate: int | None
    existing_spectral_grade: str | None
    spectral_attempted: bool | None
    spectral_error: str | None
    existing_spectral_attempted: bool | None
    existing_spectral_error: str | None
    existing_format: str | None
    source_format: str | None
    source_min_bitrate: int | None
    source_avg_bitrate: int | None
    source_median_bitrate: int | None
    target_contract_format: str | None
    legacy_projection_version: int | None
    materialized_format: str | None
    materialized_min_bitrate: int | None
    materialized_avg_bitrate: int | None
    materialized_median_bitrate: int | None
    final_format: str | None
    v0_probe_kind: str | None
    v0_probe_min_bitrate: int | None
    v0_probe_avg_bitrate: int | None
    v0_probe_median_bitrate: int | None
    existing_v0_probe_kind: str | None
    existing_v0_probe_min_bitrate: int | None
    existing_v0_probe_avg_bitrate: int | None
    existing_v0_probe_median_bitrate: int | None
    album_title: str
    artist_name: str
    mb_release_id: str | None
    request_status: str | None
    request_min_bitrate: int | None
    search_filetype_override: str | None
    source: str | None
    # issue #829 Phase 5 PR4 — the proof-gate verdict and the model that
    # minted any verified-lossless proof. Defaults keep historical callers
    # (and every row with no candidate-evidence join) building cleanly.
    verdict_tier: int | None = None
    verdict_tier_statement: str | None = None
    verdict_fired_legs: list[str] = msgspec.field(default_factory=list[str])
    spectral_accusation_admissible: bool | None = None
    spectral_accusation_withheld: str | None = None
    existing_spectral_accusation_admissible: bool | None = None
    existing_spectral_accusation_withheld: str | None = None
    verified_lossless_classifier: str | None = None
    verified_lossless_generation: str | None = None
    cd_rip_verification: dict[str, object] | None = None
    stage2_if_stage1_deferred: str | None = None
    stage2_if_stage1_deferred_verdict: str | None = None
    request_source: str | None = None
    youtube_metadata: dict[str, object] | None = None

    def to_dict(self) -> dict[str, object]:
        return msgspec.to_builtins(self)


def build_download_history_rows(
    rows: Sequence[Mapping[str, object]],
) -> list[DownloadHistoryViewRow]:
    """Classify raw download_log rows into the shared detail-view contract."""
    return [build_download_history_row(row) for row in rows]


def last_download_accusation_flags(
    history_items: Sequence[Mapping[str, object]],
    last_download_spectral_grade: object,
) -> AccusationFlags:
    """The audit-only pair for ``last_download_spectral_grade``.

    That column is a denormalised copy of ONE attempt's grade, and a flag
    must describe the measurement that produced the grade rendered beside
    it — the same rule ``_project_current_library_have`` follows for the
    HAVE side. So this returns the flags of the newest attempt whose own
    grade still equals the denorm, and an empty pair when no attempt does
    (a later attempt overwrote it, or the producing row predates the
    retained history). An empty pair leaves the surface on its historical
    accusing render, which is the fail-accusing direction.

    ``history_items`` are ``DownloadHistoryViewRow`` dicts in
    ``get_download_history`` order — newest first.
    """
    if not isinstance(last_download_spectral_grade, str) or (
        not last_download_spectral_grade
    ):
        return AccusationFlags()
    for item in history_items:
        if item.get("spectral_grade") != last_download_spectral_grade:
            continue
        admissible = item.get("spectral_accusation_admissible")
        withheld = item.get("spectral_accusation_withheld")
        return AccusationFlags(
            admissible=admissible if isinstance(admissible, bool) else None,
            withheld=withheld if isinstance(withheld, str) else None,
        )
    return AccusationFlags()


def classify_download_log_row(
    row: Mapping[str, object],
) -> ClassifiedDownloadLogRow:
    """Build the shared typed classification for one raw download_log row.

    The proof-gate verdict (issue #829 Phase 5 PR4) is projected here
    rather than inside ``classify_log_entry`` because it is derived from
    the candidate-evidence JOIN aliases, which live on the raw row and are
    deliberately not folded into ``LogEntry``'s legacy columns. Doing it at
    this one seam means every consumer of a classified row — the Recents
    page and the detail-view history panel alike — carries the same
    verdict.
    """
    entry = LogEntry.from_row(dict(row))
    return ClassifiedDownloadLogRow(
        entry=entry,
        classified=msgspec.structs.replace(
            classify_log_entry(entry),
            **msgspec.structs.asdict(proof_gate_projection(row)),
        ),
    )


def _classify_pipeline_log_item(
    row: Mapping[str, object],
) -> dict[str, object]:
    classified_row = classify_download_log_row(row)
    return {
        **classified_row.entry.to_json_dict(),
        **msgspec.to_builtins(classified_row.classified),
    }


def _project_current_library_have(
    item: dict[str, object],
    row: Mapping[str, object],
) -> None:
    """Select one complete, provably pre-attempt HAVE snapshot.

    HAVE is historical: what was on disk before this attempt. Successful and
    explicit import rows may have updated the request's current evidence, so
    they never receive an overlay. A deleted-triage or failed row may carry a
    partial embedded snapshot (for example Qigong's V0 probe with no codec or
    bitrate); when canonical evidence predates the attempt, replace that
    partial snapshot wholesale instead of mixing fields. Live Beets data has
    no historical timestamp and is never evidence for HAVE.
    """
    attempt_measurement_fields = (
        "existing_format",
        "existing_min_bitrate",
        "existing_avg_bitrate",
        "existing_median_bitrate",
        "existing_spectral_grade",
        "existing_spectral_bitrate",
        "existing_spectral_error",
        "existing_v0_probe_kind",
        "existing_v0_probe_min_bitrate",
        "existing_v0_probe_avg_bitrate",
        "existing_v0_probe_median_bitrate",
        "comparison_basis",
    )
    if item.get("outcome") in ("success", "force_import", "manual_import"):
        return

    attempt_has_any = (
        item.get("existing_spectral_attempted") is True
        or any(
            item.get(field) is not None
            for field in attempt_measurement_fields
        )
    )
    attempt_has_complete_core = (
        item.get("existing_format") is not None
        and item.get("existing_min_bitrate") is not None
        and item.get("existing_avg_bitrate") is not None
    )
    partial_snapshot_can_be_replaced = (
        item.get("comparison_basis") is None
        and (
            item.get("outcome") in (
                "failed", "timeout", "measurement_failed",
            )
            or item.get("wrong_match_triage_action") in (
                "deleted_reject",
                "deleted_verified_lossless_parent",
            )
        )
    )
    if attempt_has_any and (
        attempt_has_complete_core or not partial_snapshot_can_be_replaced
    ):
        return

    # Re-derived, never carried: the current-evidence grade below comes
    # from a different measurement than the attempt's own HAVE snapshot,
    # so the audit-only flags beside it have to describe THAT measurement
    # (issue #829 Phase 5 PR4).
    current_flags = evidence_column_accusation_flags(
        row, prefix=CURRENT_EVIDENCE_PREFIX)
    current_projection = {
        "existing_format": row.get("_current_evidence_format"),
        "existing_min_bitrate": row.get("_current_evidence_min_bitrate"),
        "existing_avg_bitrate": row.get("_current_evidence_avg_bitrate"),
        "existing_median_bitrate": row.get(
            "_current_evidence_median_bitrate"
        ),
        "existing_spectral_grade": row.get(
            "_current_evidence_spectral_grade"
        ),
        "existing_spectral_bitrate": row.get(
            "_current_evidence_spectral_bitrate"
        ),
        "existing_spectral_accusation_admissible": current_flags.admissible,
        "existing_spectral_accusation_withheld": current_flags.withheld,
        "existing_v0_probe_kind": row.get(
            "_current_evidence_v0_probe_kind"
        ),
        "existing_v0_probe_min_bitrate": row.get(
            "_current_evidence_v0_probe_min_bitrate"
        ),
        "existing_v0_probe_avg_bitrate": row.get(
            "_current_evidence_v0_probe_avg_bitrate"
        ),
        "existing_v0_probe_median_bitrate": row.get(
            "_current_evidence_v0_probe_median_bitrate"
        ),
    }
    current_has_complete_core = (
        current_projection["existing_format"] is not None
        and current_projection["existing_min_bitrate"] is not None
        and current_projection["existing_avg_bitrate"] is not None
    )
    if (
        row.get("_current_evidence_id") is not None
        and row.get("_current_evidence_is_pre_attempt") is True
        and (not attempt_has_any or current_has_complete_core)
    ):
        if attempt_has_any:
            item["existing_spectral_attempted"] = False
            item["existing_spectral_error"] = None
        item.update(current_projection)


_LINKED_IMPORT_EVIDENCE_FIELDS = (
    "existing_format",
    "existing_min_bitrate",
    "existing_avg_bitrate",
    "existing_median_bitrate",
    "existing_spectral_grade",
    "existing_spectral_bitrate",
    "existing_spectral_attempted",
    "existing_spectral_error",
    "existing_spectral_accusation_admissible",
    "existing_v0_probe_kind",
    "existing_v0_probe_min_bitrate",
    "existing_v0_probe_avg_bitrate",
    "existing_v0_probe_median_bitrate",
    "materialized_format",
    "materialized_min_bitrate",
    "materialized_avg_bitrate",
    "materialized_median_bitrate",
    "target_contract_format",
)


def _project_linked_import_evidence(
    items: list[dict[str, object]],
    linked_successors: Sequence[Mapping[str, object]] = (),
) -> None:
    """Attach a successor import's measurements to its source audit row.

    Active force-import rows and historical manual-import rows explicitly
    point back through ``source_download_log_id``. That is the authoritative
    bridge from a kept wrong-match card to the conversion which later
    materialized those bytes; do not infer the relationship from matching
    albums or measurements. The newest qualifying download-log id has
    precedence regardless of query order or whether the successor is also on
    the current page.
    """
    by_id = {
        item.get("id"): item
        for item in items
        if isinstance(item.get("id"), int)
    }
    successors_by_id: dict[int, dict[str, object]] = {}
    for successor in (*items, *linked_successors):
        if successor.get("outcome") not in (
            "success", "force_import", "manual_import"
        ):
            continue
        source_id = successor.get("source_download_log_id")
        origin = by_id.get(source_id)
        if origin is None or successor.get("materialized_format") is None:
            continue
        successor_id = successor.get("id")
        if not isinstance(successor_id, int) or isinstance(successor_id, bool):
            raise TypeError(
                "linked import successor has no integer download-log id"
            )
        payload = {
            "source_download_log_id": source_id,
            **{
                field: successor.get(field)
                for field in _LINKED_IMPORT_EVIDENCE_FIELDS
            },
        }
        previous = successors_by_id.get(successor_id)
        if previous is not None and previous != payload:
            raise ValueError(
                "linked import successor id "
                f"{successor_id} has conflicting projection data"
            )
        successors_by_id[successor_id] = payload

    for successor_id in sorted(successors_by_id, reverse=True):
        successor = successors_by_id[successor_id]
        origin = by_id[successor["source_download_log_id"]]
        for field in _LINKED_IMPORT_EVIDENCE_FIELDS:
            if origin.get(field) is None:
                origin[field] = successor.get(field)


def build_recents_download_log_rows(
    rows: Sequence[Mapping[str, object]],
    *,
    linked_successor_rows: Sequence[Mapping[str, object]] = (),
) -> list[dict[str, object]]:
    """Render a Recents page through every persisted-evidence projection."""
    items = [_classify_pipeline_log_item(row) for row in rows]
    for item, row in zip(items, rows, strict=True):
        _project_current_library_have(item, row)
    linked_items = [
        _classify_pipeline_log_item(row)
        for row in linked_successor_rows
    ]
    _project_linked_import_evidence(items, linked_items)
    return items


def build_download_history_row(
    row: Mapping[str, object],
) -> DownloadHistoryViewRow:
    """Build one detail-view history row from a raw download_log row."""
    classified_row = classify_download_log_row(row)
    entry = classified_row.entry
    classified = classified_row.classified
    merged: dict[str, object] = {
        **entry.to_json_dict(),
        **msgspec.to_builtins(classified),
    }
    merged["apply_beets_distance"] = _apply_beets_distance(
        merged.get("import_result"))
    return msgspec.convert(
        merged,
        type=DownloadHistoryViewRow,
        strict=True,
    )


def _apply_beets_distance(import_result: object) -> float | None:
    """Read #863's persisted apply-time distance off the row's JSONB."""
    if isinstance(import_result, str):
        try:
            import_result = json.loads(import_result)
        except ValueError:
            return None
    value = json_dict(import_result).get("apply_beets_distance")
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    return float(value)
