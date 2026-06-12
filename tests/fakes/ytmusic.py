"""Stateful fake for the ytmusicapi client."""

from __future__ import annotations

import copy
from typing import Any


class FakeYTMusic:
    """In-memory fake for the slice of ``ytmusicapi.YTMusic`` the YouTube
    album resolver uses (``search`` + ``get_album``).

    Mirrors ``FakeSlskdAPI`` in shape: pre-seed canned per-query results,
    queue one-shot exceptions to simulate upstream failures, and record
    every call so service tests can assert N+1 fan-out shape.

    Exceptions to inject mirror the real ``ytmusicapi`` boundary:
    ``YTMusicServerError`` (4xx/5xx upstream), ``YTMusicUserError``
    (malformed query), ``requests.Timeout`` / ``requests.ConnectionError``
    (transport layer), and ``KeyError`` (parser drift / library version
    skew).
    """

    def __init__(self) -> None:
        self._search_results: dict[str, list[dict[str, Any]]] = {}
        self._album_results: dict[str, dict[str, Any]] = {}
        self._search_errors: dict[str, Exception] = {}
        self._album_errors: dict[str, Exception] = {}
        self.search_calls: list[dict[str, Any]] = []
        self.get_album_calls: list[dict[str, Any]] = []

    def set_search(
        self,
        query: str,
        results: list[dict[str, Any]],
    ) -> None:
        """Configure ``search(query, ...)`` to return ``results``."""
        self._search_results[query] = copy.deepcopy(results)

    def set_album(
        self,
        browseId: str,
        response: dict[str, Any],
    ) -> None:
        """Configure ``get_album(browseId)`` to return ``response``."""
        self._album_results[browseId] = copy.deepcopy(response)

    def set_search_error(
        self,
        query: str,
        error: Exception,
    ) -> None:
        """Queue a single exception to raise on the next ``search(query, ...)``
        call. One-shot: subsequent matching calls fall through to the canned
        result (or the empty default)."""
        self._search_errors[query] = error

    def set_album_error(
        self,
        browseId: str,
        error: Exception,
    ) -> None:
        """Queue a single exception to raise on the next
        ``get_album(browseId)``. One-shot: subsequent matching calls fall
        through to the canned result (or the default unconfigured-raise)."""
        self._album_errors[browseId] = error

    def search(
        self,
        query: str,
        filter: str | None = None,
        scope: str | None = None,
        limit: int = 20,
        ignore_spelling: bool = False,
    ) -> list[dict[str, Any]]:
        """Mirror ``ytmusicapi.YTMusic.search``. Returns the canned list for
        ``query`` if configured, else an empty list (the real library returns
        ``[]`` for "no hits" — not an error)."""
        self.search_calls.append({
            "query": query,
            "filter": filter,
            "scope": scope,
            "limit": limit,
            "ignore_spelling": ignore_spelling,
        })
        queued_error = self._search_errors.pop(query, None)
        if queued_error is not None:
            raise queued_error
        return copy.deepcopy(self._search_results.get(query, []))

    def get_album(self, browseId: str) -> dict[str, Any]:
        """Mirror ``ytmusicapi.YTMusic.get_album``. Returns the canned dict
        for ``browseId`` if configured. Unconfigured browseIds raise
        ``YTMusicServerError`` — matching the real library's behavior for
        a non-existent album."""
        self.get_album_calls.append({"browseId": browseId})
        queued_error = self._album_errors.pop(browseId, None)
        if queued_error is not None:
            raise queued_error
        if browseId not in self._album_results:
            # Lazy import — ytmusicapi is a heavy import; only pull it in
            # when the fake actually needs to raise the real exception type
            # so tests calling other methods don't pay for it.
            from ytmusicapi.exceptions import YTMusicServerError
            raise YTMusicServerError(
                f"Server returned HTTP 400: no album for browseId={browseId!r}"
            )
        return copy.deepcopy(self._album_results[browseId])

    @staticmethod
    def make_album_fixture(
        audio_playlist_id: str | None,
        title: str,
        artists: list[dict[str, Any]],
        year: str | None,
        tracks: list[dict[str, Any]],
        other_versions: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        """Synthesize a ``get_album()``-shaped dict for use in tests.

        Mirrors the canonical ``ytmusicapi.YTMusic.get_album`` return shape
        per the library's documented schema. The real response does not
        carry the album's own ``browseId`` — callers already know it
        because they passed it to ``get_album()`` — so the fixture omits
        it too. Pair this with ``set_album(browse_id, fixture)`` so the
        fake knows which browseId to return it for. Tests that only care
        about a subset of fields can still call ``set_album`` with
        whatever subset they need — this helper is for the happy-path
        / integration-slice cases where the service's parsing code reads
        many of these fields.

        Track-level keys: ``videoId``, ``title``, ``artists``, ``album``,
        ``duration``, ``duration_seconds``, ``trackNumber``, ``isAvailable``,
        ``isExplicit``, ``likeStatus``, ``thumbnails``, ``feedbackTokens``,
        ``creditsBrowseId``.

        Other-version keys: ``browseId``, ``title``, ``artists``, ``year``,
        ``thumbnails``, ``isExplicit``.
        """
        total_seconds = sum(
            int(t.get("duration_seconds", 0) or 0) for t in tracks
        )
        # Format total duration as M:SS or H:MM:SS (best-effort, matches
        # YTMusic's typical "30:14" or "1:02:14" formats).
        if total_seconds >= 3600:
            hours = total_seconds // 3600
            minutes = (total_seconds % 3600) // 60
            seconds = total_seconds % 60
            duration_str = f"{hours}:{minutes:02d}:{seconds:02d}"
        else:
            minutes = total_seconds // 60
            seconds = total_seconds % 60
            duration_str = f"{minutes}:{seconds:02d}"
        return {
            "title": title,
            "type": "Album",
            "thumbnails": [],
            "description": None,
            "artists": copy.deepcopy(artists),
            "year": year,
            "trackCount": len(tracks),
            "duration": duration_str,
            "duration_seconds": total_seconds,
            "audioPlaylistId": audio_playlist_id,
            "tracks": copy.deepcopy(tracks),
            "other_versions": copy.deepcopy(other_versions or []),
        }


# ---------------------------------------------------------------------------
# Mirror-adapter lookup fakes (test-fidelity Rule B).
#
# ``web/mb.py::get_release`` and ``web/discogs.py::get_release`` RAISE
# ``urllib.error.HTTPError(404)`` when the id is absent — they NEVER return
# ``None``. A test that fakes a miss with ``lambda mbid: None`` simulates a
# code path production never produces. This was the round-1 P0 of the
# YT-resolver PR: ``_resolve_mb_group`` expected ``None`` on 404, the real
# adapter raised, and every test used ``lambda: None`` so the production
# crash never surfaced. These fakes mirror the real exception contract:
# seed known releases; any un-seeded id raises ``HTTPError(404)`` by
# default. See ``.claude/rules/test-fidelity.md`` § "Rule B".
# ---------------------------------------------------------------------------


