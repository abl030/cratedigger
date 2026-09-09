"""Narrow one mirror ``get_release()`` payload field at a time.

``lib.mb_api.get_release()`` and ``lib.discogs_api.get_release()`` both return
``dict[str, object]`` in ONE shape (``id``, ``title``, ``artist_name``,
``artist_id``, ``release_group_id``, ``year``, ``country``, ``tracks``, ...)
— the same columns in the same shape, no adapter between the pathways. The
Structs that validated the wire live in those two modules; downstream
consumers receive the already-decoded dict and read a handful of scalar
fields plus the per-track list. These helpers are that read: a graceful
narrow of an already-decoded value (``lib.json_narrow`` doctrine — never a
re-``convert``), where a field of the wrong shape reads as absent rather
than raising or being coerced.

Shared by the add flow (``web/routes/pipeline_mutations.py``) and the
Replace service (``lib/mbid_replace_service.py``), which persist the same
fields onto ``album_requests``.
"""

from __future__ import annotations

from lib.json_narrow import is_object_list, is_str_object_dict


def release_tracks(release: dict[str, object]) -> list[dict[str, object]]:
    """Narrow a payload's ``tracks`` field to the per-track dict list.

    Both mirrors carry a list of per-track dicts under ``"tracks"`` (or omit
    the key). A malformed member makes the whole field read as absent
    (``[]``) rather than passing a half-typed list downstream.
    """
    tracks = release.get("tracks")
    if not is_object_list(tracks):
        return []
    result: list[dict[str, object]] = []
    for track in tracks:
        if not is_str_object_dict(track):
            return []
        result.append(track)
    return result


def release_str(release: dict[str, object], key: str, default: str = "") -> str:
    """Narrow one scalar field to ``str``; missing or non-str reads as ``default``."""
    value = release.get(key, default)
    return value if isinstance(value, str) else default


def release_str_or_none(release: dict[str, object], key: str) -> str | None:
    """Narrow one scalar field to ``str | None``; non-str reads as ``None``."""
    value = release.get(key)
    return value if isinstance(value, str) else None


def release_int_or_none(release: dict[str, object], key: str) -> int | None:
    """Narrow one scalar field to ``int | None``; non-int reads as ``None``."""
    value = release.get(key)
    return value if isinstance(value, int) else None
