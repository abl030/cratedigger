"""Helpers shared by more than one FakePipelineDB cluster module."""
from __future__ import annotations

import json


def _jsonb_column(value: object) -> object:
    """Project a stored JSONB value the way a real ``SELECT`` returns it.

    ``download_log.validation_result`` / ``.import_result`` are JSONB
    columns and every production writer hands them a JSON STRING
    (``ValidationResult.to_json()``, ``msgspec.json.encode(...).decode()``,
    ``json.dumps(...)``) or ``None`` — psycopg2 has no adapter that would
    let a bare dict through. What comes back out is parsed JSON, for every
    reader. The fake stores what it was handed — that raw form is what
    ``db.download_logs[i]`` exposes and what the JSONB-boundary tests
    decode — so the parse belongs on the row PROJECTION, which is exactly
    where psycopg2 does it (issue #1278 item 7: a string-seeded fake used
    to hand ``get_wrong_matches`` callers a ``str`` where production hands
    a ``dict``). Values already stored as a mapping — the shape many
    fake-backed tests seed directly — pass through unchanged, which is
    what production's reader would have returned for them too.

    A non-JSON string is not a shape production can hold: PostgreSQL
    rejects it at INSERT. Surfacing the decode error here is the closest
    the fake can get to that refusal.
    """
    if isinstance(value, str):
        return json.loads(value)
    return value


def _reject_nonstandard_json_constant(value: str) -> None:
    raise ValueError(f"nonstandard JSON constant: {value}")
