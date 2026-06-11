"""Typed msgspec view over ``download_log.validation_result`` JSONB (#410).

The blob is written by ``ValidationResult.to_json()`` (lib/quality.py) on
rejection rows, by the curator-ban route with its own audit shape, and has
``wrong_match_triage`` grafted on later via ``jsonb_set``
(``PipelineDB.record_wrong_match_triage``). This module is the single read
contract: every consumer that branches on a key in the blob decodes through
``decode_validation_envelope`` and uses the typed fields — no per-module
dict-poking.

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
from typing import Any

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
    ``CandidateSummary`` in lib/quality.py), so strict nested decoding
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
SCENARIO_KEY = "scenario"
WRONG_MATCH_TRIAGE_KEY = "wrong_match_triage"


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
