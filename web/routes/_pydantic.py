"""Shared Pydantic adapter for HTTP request-body validation.

The single entry point is :func:`parse_body`, which validates a request
body against a Pydantic ``BaseModel`` and routes ``ValidationError`` to
the standard ``{"error": ..., "errors": [...]}`` HTTP 400 response.

See ``.claude/rules/code-quality.md`` § "HTTP request bodies" for the
boundary policy. This adapter is the only place ``ValidationError`` is
handled; routes never catch it inline.
"""

from __future__ import annotations

from typing import Any, Type, TypeVar

from pydantic import BaseModel, ValidationError


M = TypeVar("M", bound=BaseModel)


def parse_body(handler: Any, body: Any, model: Type[M]) -> M | None:
    """Validate ``body`` against ``model``; on failure, send HTTP 400 and return None.

    Returns the parsed model instance on success, or ``None`` after
    sending the error response. Callers check ``if payload is None:
    return`` and proceed against the typed payload otherwise.

    The 400 status matches the existing route convention (hand-rolled
    body parsers raised 400 on missing fields); using 400 preserves
    every existing contract test that asserted ``status == 400`` for
    bad input. ``ValidationError.errors(include_url=False)`` produces a
    list of ``{"loc": [...], "msg": "...", "type": "..."}`` entries
    that the frontend can render directly.
    """
    if not isinstance(body, dict):
        handler._json(
            {"error": "request body must be a JSON object", "errors": []},
            status=400,
        )
        return None
    try:
        return model.model_validate(body)
    except ValidationError as exc:
        # ``include_context=False`` strips the ``ctx`` dict — Pydantic
        # populates it with the raw ValueError / TypeError object for
        # ``@model_validator`` errors, which the stdlib ``json`` encoder
        # cannot serialise. ``include_input=False`` keeps the response
        # compact (the frontend only needs ``loc`` + ``msg`` + ``type``).
        handler._json(
            {
                "error": "validation failed",
                "errors": exc.errors(
                    include_url=False,
                    include_context=False,
                    include_input=False,
                ),
            },
            status=400,
        )
        return None
