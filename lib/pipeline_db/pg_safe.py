"""Every value bound to a PostgreSQL parameter is made storable here, once.

PostgreSQL cannot hold some values Python strings and floats can (measured
2026-10-15 against real PG, every encoder this codebase uses):

- U+0000 — rejected by text (psycopg2 ``ValueError``) and JSONB
  (``UntranslatableCharacter``).
- Lone surrogates (U+D800–U+DFFF, what ``surrogateescape`` decodes a
  non-UTF-8 filename into) — rejected by text (``UnicodeEncodeError``) and
  JSONB; msgspec refuses to encode them at all.
- NaN / ±Infinity — not JSON; stdlib ``json.dumps`` emits them anyway.

External text carries all of these: ID3 tags (a NUL-terminated
``mb_trackid`` looped request 6343's import forever, 2026-10-12), Soulseek
filenames and peer messages, subprocess stderr. Any one of them in an audit
row turned a recoverable outcome into a write failure.

The fix lives at psycopg2's adaptation layer, not at call sites: every
``str`` parameter (scalars, ``text[]`` elements, ``execute_values`` rows,
``sql.Literal``) and every ``psycopg2.extras.Json`` is adapted through this
module, so no writer — present or future, pooled or raw cursor — can bind an
unstorable value. JSONB payloads must therefore be bound as
``psycopg2.extras.Json(obj)`` — or, for a document a caller hands over
already encoded, ``pg_jsonb_text(text)`` — never as a raw JSON string, whose
escapes this layer cannot see.

Cleaning is deliberately minimal and visible: trailing NULs (ID3 padding)
are dropped, an interior NUL or a lone surrogate becomes U+FFFD so the
audit still shows something unrepresentable was there, and a non-finite
float becomes JSON ``null``.
"""

from __future__ import annotations

import json
import math
import re
from typing import TypeGuard

import msgspec
import psycopg2.extras
from psycopg2.extensions import QuotedString, register_adapter

_UNSTORABLE = re.compile("[\x00\ud800-\udfff]")


def pg_safe_text(value: str) -> str:
    """Return ``value`` with every character PostgreSQL rejects replaced."""
    if _UNSTORABLE.search(value) is None:
        return value
    return _UNSTORABLE.sub("�", value.rstrip("\x00"))


def pg_safe_json(value: object) -> object:
    """Builtin JSON value with every unstorable string and float cleaned."""
    if isinstance(value, str):
        return pg_safe_text(value)
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if _is_object_dict(value):
        return {
            pg_safe_json(key): pg_safe_json(item) for key, item in value.items()
        }
    if _is_object_list(value):
        return [pg_safe_json(item) for item in value]
    return value


def pg_json_dumps(value: object) -> str:
    """Encode ``value`` (any msgspec-encodable object) as storable JSON."""
    return msgspec.json.encode(
        pg_safe_json(msgspec.to_builtins(value)),
    ).decode()


def pg_jsonb_text(text: str | None) -> psycopg2.extras.Json | str | None:
    """Bind an already-encoded JSON document to a JSONB parameter.

    The adapter cannot clean escapes inside JSON text, so the document is
    decoded here and bound as an object. ``json.loads`` accepts every escape
    and ``NaN`` token the encoders emit, so the ``Json`` adapter then sees
    the real strings and floats. Text that is not JSON at all is bound
    unchanged, so PostgreSQL rejects it with ``InvalidTextRepresentation``
    exactly as it did before this layer existed.
    """
    if text is None:
        return None
    try:
        decoded: object = json.loads(text)
    except ValueError:
        return text
    return psycopg2.extras.Json(decoded)


def _is_object_dict(value: object) -> TypeGuard[dict[object, object]]:
    return isinstance(value, dict)


def _is_object_list(
    value: object,
) -> TypeGuard[list[object] | tuple[object, ...]]:
    # ``msgspec.to_builtins`` keeps tuples as tuples.
    return isinstance(value, (list, tuple))


def _adapt_str(value: str) -> QuotedString:
    return QuotedString(pg_safe_text(value))


def _adapt_json(value: psycopg2.extras.Json) -> QuotedString:
    return QuotedString(pg_json_dumps(value.adapted))


def install_pg_safe_adapters() -> None:
    """Route every ``str`` and ``Json`` parameter through this module.

    psycopg2's adapter registry is process-global and consulted before an
    object's own ``__conform__``, so this covers every connection in the
    process. Idempotent: re-registering replaces the same entries.
    """
    register_adapter(str, _adapt_str)
    register_adapter(psycopg2.extras.Json, _adapt_json)
