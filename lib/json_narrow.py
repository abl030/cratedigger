"""Narrowing helpers for already-decoded JSON values under strict pyright.

Cratedigger reads a lot of untyped JSON: ytmusicapi / MusicBrainz / Discogs
responses, ffprobe stdout, and JSONB rows round-tripped through
``msgspec.to_builtins``. Under ``typeCheckingMode: strict`` a bare
``isinstance(value, dict)`` narrows the name to a generic-erased
``dict[Unknown, Unknown]`` — pyright never recovers a generic's type argument
from a runtime class check — which then taints every downstream ``.get()`` /
``.items()`` / subscript as "partially unknown". These helpers are the single
sanctioned way to cross that seam without a (banned) ``cast`` / ``# type:
ignore``. Before #809 the same handful of one-liners were copy-pasted, with
near-identical multi-paragraph docstrings, across ~10 modules; this is their
one home.

Three tools, three deliberately different jobs — pick by what you want pyright
to do to the *caller's* variable:

* ``json_dict`` / ``json_list`` — graceful narrow-and-degrade. Guards with
  ``isinstance`` then routes the already-plain value through ``msgspec.convert``
  so callers get a fully-known ``dict[str, object]`` / ``list[object]``. A
  non-matching value degrades to ``{}`` / ``[]`` — never an assertion — because
  the inputs are external/untrusted JSON where a malformed field should read as
  absent, not crash the caller. Elements are not copied or coerced
  (``msgspec.convert`` at an ``object`` value type returns each element
  unchanged, including already-constructed ``msgspec.Struct`` instances).

* ``is_dict_like`` / ``is_list_like`` / ``is_container_like`` — plain ``bool``
  checks that DELIBERATELY DO NOT narrow the caller. A bare inline
  ``isinstance`` would flow-narrow an ``Any`` / ``object`` local to
  ``dict[Unknown, Unknown]`` for the rest of the scope; routing through a
  non-``TypeGuard`` function does the identical runtime check while leaving the
  caller's variable at its original type — exactly what you want when the next
  step is a graceful ``.get(key, default)`` on a still-``Any`` envelope, or a
  hand-off to ``json_dict`` (which re-guards internally).

* ``is_str_object_dict`` / ``is_object_list`` — ``TypeGuard`` checks that DO
  narrow the caller, to ``dict[str, object]`` / ``list[object]``. Use when the
  branch body needs the value already typed (``.items()``, subscripting) rather
  than re-narrowing through ``json_dict``.

**convert is not identity — do not reach for raw ``msgspec.convert`` here.**
When the only goal is to give pyright a concrete type for a value that is
*already decoded* (a ``to_builtins`` result, a JSONB row, a plain dict local),
prefer one of these helpers or annotate the assignment target
(``x: dict[str, object] = msgspec.to_builtins(...)`` — ``to_builtins -> Any``
makes that strict-clean and does nothing at runtime). A raw
``msgspec.convert(value, dict[str, object])`` in the middle of a data path is
NOT a no-op: it re-validates and reconstructs, so it can reshape the value or
raise ``ValidationError`` depending on the input (the bug behind #804 — a
re-converted terminal-outcome payload diverged from the intended pass-through
and stranded rescue imports at ``wanted`` instead of ``imported``; on a
``msgspec.Struct`` the same call raises outright). ``json_dict`` is safe from
that class of bug by construction: its ``isinstance(value, dict)`` guard
degrades any non-dict — a Struct included — to ``{}`` instead of re-converting
it. Reserve ``msgspec.convert`` for a real wire-boundary DECODE where
re-validation is actually wanted.
"""

from __future__ import annotations

from typing import Any, TypeGuard

import msgspec


def json_dict(value: Any) -> dict[str, object]:
    """Graceful-narrow an already-decoded JSON value to a string-keyed dict.

    Non-dict input (including a ``msgspec.Struct``) degrades to ``{}`` — see the
    module docstring for why this is graceful and why it is safe from the
    convert-is-not-identity reshape. The ``Any`` parameter absorbs the
    isinstance-narrowing taint from callers that pass an already-narrowed local;
    ``object`` would re-flag it under strict mode.
    """
    if not isinstance(value, dict):
        return {}
    return msgspec.convert(value, type=dict[str, object])


def json_list(value: Any) -> list[object]:
    """Graceful-narrow an already-decoded JSON value to a list.

    Non-list input degrades to ``[]``; see ``json_dict`` and the module
    docstring for the ``Any`` parameter and convert-is-not-identity rationale.
    """
    if not isinstance(value, list):
        return []
    return msgspec.convert(value, type=list[object])


def is_dict_like(value: object) -> bool:
    """Plain ``isinstance(value, dict)`` that does NOT narrow the caller.

    Prefer this over ``is_str_object_dict`` when the caller's variable must stay
    ``Any`` / ``object`` for a subsequent graceful ``.get()`` or ``json_dict``
    hand-off. See the module docstring.
    """
    return isinstance(value, dict)


def is_list_like(value: object) -> bool:
    """Plain ``isinstance(value, list)`` that does NOT narrow — see ``is_dict_like``."""
    return isinstance(value, list)


def is_container_like(value: object) -> bool:
    """Plain ``isinstance(value, (dict, list, tuple))`` — non-narrowing; see ``is_dict_like``."""
    return isinstance(value, (dict, list, tuple))


def is_str_object_dict(value: object) -> TypeGuard[dict[str, object]]:
    """``TypeGuard`` that narrows the caller's value to ``dict[str, object]``.

    Use when the branch body needs the value already typed (``.items()``,
    subscripting); use ``is_dict_like`` when you specifically do NOT want the
    caller narrowed. See the module docstring.
    """
    return isinstance(value, dict)


def is_object_list(value: object) -> TypeGuard[list[object]]:
    """``TypeGuard`` that narrows the caller's value to ``list[object]`` — see ``is_str_object_dict``."""
    return isinstance(value, list)
