"""Resolve MusicBrainz canonical release ids at the point of use (#1049).

Cratedigger acquires; Beets manages; MusicBrainz owns release identity above
both. When MusicBrainz editors merge two release entries, the loser's MBID
becomes a permanent ``301`` to the survivor, and ``mbsync`` retags the local
files onto the survivor. Cratedigger's stored acquisition id is frozen
history — "I went and got release X, here is the proof" — and never moves.

This module answers the one question that lets a stale join or match retry
against the survivor: *what does MusicBrainz call this release now?* It
stores nothing. There is no alias table, no column, no backfill, no sweep:
the ``301`` is authoritative and always current, so it is asked at the point
of use and thrown away.

**Fail-open by contract.** Every failure returns ``None``, and the caller
keeps exactly today's behaviour. In particular a ``4xx`` is NEVER read as
"this release was deleted": the mirror's WS/2 app layer has served poisoned
404s that its own PostgreSQL contradicted, and a bogus UUID answers ``400``
rather than ``404`` at all. Concluding deletion from this module's silence
is a bug.

**Inert until configured.** ``MB_WS2_BASE`` starts unset, so a process that
never wires it degrades to the literal stored id rather than silently
reaching out to public MusicBrainz. Production entry points configure it via
``web.api_bases.configure_api_bases_from_runtime_config()``.
"""

from __future__ import annotations

import json
import logging
import urllib.parse
import urllib.request
from collections.abc import Callable

from lib.json_narrow import json_dict
from lib.release_identity import detect_release_source, normalize_release_id

logger = logging.getLogger("cratedigger")

#: A canonical-release resolver: stored release id -> survivor id, or None.
type CanonicalReleaseFn = Callable[[str], str | None]

#: A decoded-JSON fetch of one URL. The external HTTP edge, and the only
#: seam tests replace.
type CanonicalFetchFn = Callable[[str], object]

#: WS/2 base (``scheme://host[:port]/ws/2``) for canonical resolution.
#: Unset means inert — see the module docstring. Read it through
#: :func:`configured_canonical_base`; it is process-startup state, not a
#: constant.
_mb_ws2_base: str | None = None

_TIMEOUT_SECONDS = 15
_USER_AGENT = "cratedigger-canonical/1.0"


def configure_canonical_base(ws2_base: str | None) -> None:
    """Point canonical resolution at one MusicBrainz WS/2 base."""
    global _mb_ws2_base
    _mb_ws2_base = (ws2_base or "").rstrip("/") or None


def configured_canonical_base() -> str | None:
    """The wired WS/2 base, or None while this process is inert."""
    return _mb_ws2_base


def _fetch_json(url: str) -> object:
    """Fetch and decode one MB URL, following the merge redirect.

    ``urllib`` follows the ``301`` transparently, so the response body is
    already the survivor's document and its top-level ``id`` is the
    canonical MBID. Verified live against the mirror on 2026-08-06.
    """
    request = urllib.request.Request(url)
    request.add_header("User-Agent", _USER_AGENT)
    request.add_header("Connection", "close")
    with urllib.request.urlopen(request, timeout=_TIMEOUT_SECONDS) as response:
        return json.loads(response.read())


def canonical_release_id(
    release_id: str,
    *,
    ws2_base: str | None = None,
    fetch: CanonicalFetchFn | None = None,
) -> str | None:
    """The survivor MusicBrainz redirects ``release_id`` to, or ``None``.

    ``None`` means "no different canonical is known" and is returned for
    every non-answer alike: a non-MusicBrainz identity, an unconfigured
    base, any transport or protocol failure, an unusable response shape,
    and the ordinary case of an id MusicBrainz still considers current.
    Callers treat all of them as "keep using the stored id".

    Never raises.
    """
    requested = normalize_release_id(release_id)
    # MB-only by nature: Discogs release ids have no redirect concept, so
    # this is not an adapter between the two sources.
    if detect_release_source(requested) != "musicbrainz":
        return None

    base = (ws2_base if ws2_base is not None else _mb_ws2_base) or ""
    base = base.rstrip("/")
    if not base:
        return None

    url = (
        f"{base}/release/"
        f"{urllib.parse.quote(requested, safe='')}?fmt=json"
    )
    try:
        payload = (fetch or _fetch_json)(url)
    except Exception as exc:  # noqa: BLE001 - fail-open boundary, never raises
        # Deliberately including HTTPError 4xx: a 404 here is not evidence
        # that the release is gone, only that this lookup did not answer.
        logger.debug(
            "canonical release lookup for %s did not answer: %s: %s",
            requested, type(exc).__name__, exc,
        )
        return None

    # Narrowing an already-decoded value: the shared graceful helper, never
    # a re-``convert`` (`.claude/rules/code-quality.md`).
    canonical = normalize_release_id(json_dict(payload).get("id"))
    if detect_release_source(canonical) != "musicbrainz":
        return None
    if canonical == requested:
        return None
    logger.info(
        "MusicBrainz canonicalizes release %s to %s", requested, canonical,
    )
    return canonical


def production_canonical_release_fn() -> CanonicalReleaseFn:
    """The configured resolver, bound late so startup order cannot matter."""

    def resolve(release_id: str) -> str | None:
        return canonical_release_id(release_id)

    return resolve


__all__ = [
    "CanonicalFetchFn",
    "CanonicalReleaseFn",
    "canonical_release_id",
    "configure_canonical_base",
    "configured_canonical_base",
    "production_canonical_release_fn",
]
