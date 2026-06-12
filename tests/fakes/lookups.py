"""MB / Discogs mirror lookup fakes with real exception contracts."""

from __future__ import annotations

import copy
import email.message
import urllib.error
from typing import Any, Callable, Optional


def http_error(
    code: int,
    *,
    url: str = "http://mirror.test/x",
    msg: str | None = None,
) -> urllib.error.HTTPError:
    """Construct a real ``urllib.error.HTTPError`` for ``code``.

    The canonical way to make a fake adapter raise exactly what the live
    MB / Discogs mirror raises on a 4xx/5xx. ``HTTPError`` is a subclass
    of ``urllib.error.URLError``, so callers that catch either type see it.
    """
    return urllib.error.HTTPError(
        url, code, msg or f"HTTP {code}", email.message.Message(), None,
    )


class FakeMBLookup:
    """Callable stand-in for ``web.mb.get_release`` with the REAL exception
    contract.

    Production ``web.mb.get_release(mbid)`` raises
    ``urllib.error.HTTPError(code=404)`` for an absent MBID and returns a
    slim release dict for a present one — it NEVER returns ``None``. Faking
    a miss with ``lambda mbid: None`` is the test-fidelity Rule B
    anti-pattern: it exercises a ``None`` branch production cannot reach
    while hiding the exception branch production actually takes.

    Usage::

        mb = FakeMBLookup()
        mb.set_release("rel-1", {"id": "rel-1", "title": "X", ...})
        resolve(..., mb_get_release=mb)   # hit -> dict
        # any other mbid -> raises HTTPError(404), exactly like production

    Pass ``raises_on_404=False`` only to model an adapter variant that
    genuinely returns ``None`` on miss (rare — document why at the call
    site). Accepts the ``fresh=`` kwarg the production callable takes.
    """

    def __init__(
        self,
        releases: dict[str, dict[str, Any]] | None = None,
        *,
        raises_on_404: bool = True,
    ) -> None:
        self._releases: dict[str, dict[str, Any]] = dict(releases or {})
        self._raises_on_404 = raises_on_404
        self.calls: list[str] = []

    def set_release(
        self, identifier: str, payload: dict[str, Any],
    ) -> "FakeMBLookup":
        """Seed a hit for ``identifier``. Returns self for chaining."""
        self._releases[identifier] = copy.deepcopy(payload)
        return self

    def __call__(
        self, identifier: str, *, fresh: bool = False,
    ) -> Optional[dict[str, Any]]:
        self.calls.append(identifier)
        if identifier in self._releases:
            return copy.deepcopy(self._releases[identifier])
        if self._raises_on_404:
            raise http_error(404, url=f"http://mb.test/release/{identifier}")
        return None


class FakeDiscogsLookup:
    """Callable stand-in for ``web.discogs.get_release`` with the REAL
    exception contract — the Discogs analogue of :class:`FakeMBLookup`.

    Production ``web.discogs.get_release(release_id)`` raises
    ``urllib.error.HTTPError(404)`` for an absent id (the mirror's ``_get``
    propagates ``urlopen``'s ``HTTPError`` directly — no retry) and never
    returns ``None``.
    """

    def __init__(
        self,
        releases: dict[str, dict[str, Any]] | None = None,
        *,
        raises_on_404: bool = True,
    ) -> None:
        self._releases: dict[str, dict[str, Any]] = dict(releases or {})
        self._raises_on_404 = raises_on_404
        self.calls: list[str] = []

    def set_release(
        self, identifier: str, payload: dict[str, Any],
    ) -> "FakeDiscogsLookup":
        """Seed a hit for ``identifier``. Returns self for chaining."""
        self._releases[identifier] = copy.deepcopy(payload)
        return self

    def __call__(
        self, identifier: str, *, fresh: bool = False,
    ) -> Optional[dict[str, Any]]:
        self.calls.append(identifier)
        if identifier in self._releases:
            return copy.deepcopy(self._releases[identifier])
        if self._raises_on_404:
            raise http_error(
                404, url=f"http://discogs.test/releases/{identifier}")
        return None


