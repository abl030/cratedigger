"""Typed msgspec view over ``download_log.validation_result`` JSONB (#410).

The blob is written by ``ValidationResult.to_json()`` (lib/quality/wire_types.py) on
rejection rows, by the curator-ban route with its own audit shape, and has
``wrong_match_triage`` grafted on later via ``jsonb_set``
(``PipelineDB.record_wrong_match_triage``). This module is the single envelope
contract: readers decode through ``decode_validation_envelope`` and download-log
writers project the query columns through ``derive_validation_log_columns`` — no
per-module dict-poking or duplicated distance/scenario inputs.

Strictness: declared keys are validated (wrong-typed JSONB raises
``msgspec.ValidationError`` — the wire-boundary rule in
``.claude/rules/code-quality.md``); unknown keys are ignored, which is what
keeps the curator-ban shape and legacy triage-writer keys decodable. The
full live table was probed clean against these types on 2026-06-11.

SQL writers (``get_wrong_matches``, ``clear_wrong_match_path(s)``,
``record_wrong_match_triage``) interpolate the ``*_KEY`` constants below so
the key names in SQL come from this contract.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, cast

import msgspec


class WrongMatchTriageAudit(msgspec.Struct, frozen=True, omit_defaults=True):
    """Cleanup/triage audit persisted under ``wrong_match_triage``.

    Written by ``lib/wrong_match_cleanup_service.py`` (the only producer);
    read by ``web/classify.py`` for the recents triage verdict line.
    ``omit_defaults=True`` keeps unset fields out of the JSONB, matching the
    historical conditional dict building.
    """

    action: str | None = None
    outcome: str | None = None
    success: bool = False
    reason: str | None = None
    preview_verdict: str | None = None
    preview_decision: str | None = None
    cleanup_eligible: bool = False
    source_path: str | None = None
    stage_chain: list[str] = []
    cleared_rows: int = 0
    deleted_path: str | None = None
    path_missing: bool = False
    error: str | None = None


class ValidationResultEnvelope(msgspec.Struct, frozen=True):
    """Read view declaring the blob keys consumers branch on.

    Field names ARE the JSONB key contract; every field except
    ``wrong_match_triage`` must exist on ``ValidationResult`` (drift guard
    in ``tests/test_validation_envelope.py``). ``candidates`` and ``items``
    stay ``list[dict]``: pre-#100 rows use the ``mbid`` wire key where
    ``CandidateSummary`` now expects ``album_id`` (see the format note on
    ``CandidateSummary`` in lib/quality/wire_types.py), so strict nested decoding
    would reject valid historical audit rows.
    """

    valid: bool = False
    scenario: str | None = None
    detail: str | None = None
    distance: float | None = None
    failed_path: str | None = None
    soulseek_username: str | None = None
    source_dirs: list[str] = []
    items: list[dict] = []
    candidates: list[dict] = []
    wrong_match_triage: WrongMatchTriageAudit | None = None


FAILED_PATH_KEY = "failed_path"
DISTANCE_KEY = "distance"
SCENARIO_KEY = "scenario"
WRONG_MATCH_TRIAGE_KEY = "wrong_match_triage"


@dataclass(frozen=True)
class ValidationProjectionUnset:
    """Sentinel distinguishing an omitted denormalized field from NULL."""


VALIDATION_PROJECTION_UNSET = ValidationProjectionUnset()


def derive_validation_log_columns(
    raw: Any,
    *,
    beets_distance: float | None | ValidationProjectionUnset = (
        VALIDATION_PROJECTION_UNSET
    ),
    beets_scenario: str | None | ValidationProjectionUnset = (
        VALIDATION_PROJECTION_UNSET
    ),
) -> tuple[float | None, str | None]:
    """Project ValidationResult envelope fields onto query columns.

    A key present in ``validation_result`` is the sole input for its
    denormalized column, including an explicit JSON ``null``. Callers may
    supply top-level metadata only when the payload omits that key (for
    example ``MeasurementFailure`` has neither distance nor scenario).
    Rejecting both inputs avoids a compatibility precedence rule that could
    hide writer drift.
    """
    envelope = decode_validation_envelope(raw)
    if raw is None or raw == "" or raw == b"":
        raw_object: dict[str, Any] = {}
    else:
        parsed = json.loads(raw) if isinstance(raw, (str, bytes)) else raw
        # ``decode_validation_envelope`` above has already proved this is a
        # JSON object and raised its established msgspec.ValidationError for
        # every non-object shape. The cast only records that fact for pyright.
        raw_object = cast(dict[str, Any], parsed)

    if DISTANCE_KEY in raw_object:
        if not isinstance(beets_distance, ValidationProjectionUnset):
            raise ValueError(
                "beets_distance must be omitted when validation_result "
                "contains distance"
            )
        projected_distance = envelope.distance
    else:
        projected_distance = (
            None
            if isinstance(beets_distance, ValidationProjectionUnset)
            else beets_distance
        )

    if SCENARIO_KEY in raw_object:
        if not isinstance(beets_scenario, ValidationProjectionUnset):
            raise ValueError(
                "beets_scenario must be omitted when validation_result "
                "contains scenario"
            )
        projected_scenario = envelope.scenario
    else:
        projected_scenario = (
            None
            if isinstance(beets_scenario, ValidationProjectionUnset)
            else beets_scenario
        )
    return projected_distance, projected_scenario


def decode_validation_envelope(raw: Any) -> ValidationResultEnvelope:
    """The ONE decode site for ``download_log.validation_result``.

    Accepts the raw column value: ``None`` (no validation ran), a dict
    (psycopg2 JSONB), or a JSON string (fakes / pre-decode callers).
    """
    if raw is None or raw == "" or raw == b"":
        return ValidationResultEnvelope()
    if isinstance(raw, (str, bytes)):
        raw = json.loads(raw)
    return msgspec.convert(raw, type=ValidationResultEnvelope)
