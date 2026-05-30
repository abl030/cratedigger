"""Adapter contract tests — Layer 3 of test-fidelity hardening (#382).

Documents and locks **what the real MB / Discogs mirror adapters raise**.
Rule B (``.claude/rules/test-fidelity.md``) forbids fakes that are more
permissive than production — but a fake can only mirror a contract that is
itself pinned. These tests are that pin: they exercise the *real* adapter
code in ``web/mb.py`` and ``web/discogs.py`` with the HTTP transport stubbed
at the ``urllib.request.urlopen`` leaf seam, so they run fully offline and
deterministically (no live mirror, no skip-gating — see CLAUDE.md
§ "Skipped tests are an anti-pattern").

The canonical fakes that consumers should use —
``tests/fakes.py::FakeMBLookup`` / ``FakeDiscogsLookup`` — are asserted here
to raise the SAME exception type the real adapter raises, closing the loop:
if the production contract ever drifts (adapter starts returning ``None``
on 404, say), the contract test fails and the fake is updated in lockstep.

Round-1 P0 recap: ``_resolve_mb_group`` expected ``None`` on 404; the real
``web.mb.get_release`` raises ``urllib.error.HTTPError``. Every resolver
test used ``lambda m: None`` so the production crash never surfaced. The
404-raises contract below is the one that bug violated.
"""
from __future__ import annotations

import email.message
import json
import socket
import unittest
import urllib.error
from unittest.mock import patch

import web.cache as _cache
import web.discogs as discogs
import web.mb as mb
from tests.fakes import FakeDiscogsLookup, FakeMBLookup, http_error


class _FakeResp:
    """Minimal stand-in for the object ``urllib.request.urlopen`` yields:
    a context manager whose ``.read()`` returns the response body bytes."""

    def __init__(self, payload: object) -> None:
        self._body = json.dumps(payload).encode("utf-8")

    def read(self) -> bytes:
        return self._body

    def __enter__(self) -> "_FakeResp":
        return self

    def __exit__(self, *exc: object) -> bool:
        return False


def _raise_http(code: int):
    """An ``urlopen`` replacement that always raises ``HTTPError(code)``.

    Always-raise (not one-shot) because ``web.mb._get`` retries once on a
    ``URLError`` — and ``HTTPError`` is a ``URLError`` subclass — so the
    transport is hit twice before the error escapes. The contract is that
    it still escapes; a one-shot stub would mask the retry."""

    def _urlopen(*_a: object, **_k: object):
        raise urllib.error.HTTPError(
            "http://mirror.test/x", code, f"HTTP {code}",
            email.message.Message(), None,
        )

    return _urlopen


def _raise_timeout():
    """An ``urlopen`` replacement that always times out (the mirror's
    documented transient-failure mode)."""

    def _urlopen(*_a: object, **_k: object):
        raise urllib.error.URLError(socket.timeout("timed out"))

    return _urlopen


def _return(payload: object):
    """An ``urlopen`` replacement that returns ``payload`` as JSON body."""

    def _urlopen(*_a: object, **_k: object) -> _FakeResp:
        return _FakeResp(payload)

    return _urlopen


class _MirrorContractCase(unittest.TestCase):
    """Base case: neutralise the Redis cache so ``memoize_meta`` always runs
    the wrapped ``_fetch`` and never reaches a real Redis. ``meta_get`` /
    ``meta_set`` are the thin Redis leaf-seam wrappers (allowed to patch)."""

    def setUp(self) -> None:
        get_p = patch.object(_cache, "meta_get", lambda *_a, **_k: None)
        set_p = patch.object(_cache, "meta_set", lambda *_a, **_k: None)
        get_p.start()
        set_p.start()
        self.addCleanup(get_p.stop)
        self.addCleanup(set_p.stop)


class TestMBAdapterContract(_MirrorContractCase):
    def test_get_release_raises_HTTPError_on_404(self) -> None:
        with patch("urllib.request.urlopen", _raise_http(404)):
            with self.assertRaises(urllib.error.HTTPError) as ctx:
                mb.get_release("00000000-0000-0000-0000-000000000000")
        self.assertEqual(ctx.exception.code, 404)

    def test_get_release_raises_HTTPError_on_5xx(self) -> None:
        with patch("urllib.request.urlopen", _raise_http(503)):
            with self.assertRaises(urllib.error.HTTPError) as ctx:
                mb.get_release("00000000-0000-0000-0000-000000000000")
        self.assertEqual(ctx.exception.code, 503)

    def test_get_release_group_releases_raises_HTTPError_on_404(self) -> None:
        with patch("urllib.request.urlopen", _raise_http(404)):
            with self.assertRaises(urllib.error.HTTPError):
                mb.get_release_group_releases(
                    "00000000-0000-0000-0000-000000000000")

    def test_transport_timeout_propagates(self) -> None:
        """Timeouts surface as a URLError subclass — the field resolver
        classifies these as transient, distinct from a 404."""
        with patch("urllib.request.urlopen", _raise_timeout()):
            with self.assertRaises(urllib.error.URLError):
                mb.get_release("00000000-0000-0000-0000-000000000000")


class TestMBReleaseGroupYearDualContract(_MirrorContractCase):
    """The year lookup has a TWO-pronged contract the field resolver
    depends on: 404 *raises* (``unresolved_404``, sticky) while
    "record exists but no parseable year" *returns None*
    (``unresolved_field_missing_upstream``). Conflating them — e.g. faking
    a 404 with ``lambda: None`` — routes a missing MBID to the wrong
    triage bucket. Both prongs are pinned here."""

    def test_404_raises(self) -> None:
        with patch("urllib.request.urlopen", _raise_http(404)):
            with self.assertRaises(urllib.error.HTTPError):
                mb.get_release_group_year(
                    "00000000-0000-0000-0000-000000000000")

    def test_exists_but_no_year_returns_none(self) -> None:
        # A valid release-group record carrying no first-release-date.
        with patch("urllib.request.urlopen",
                   _return({"id": "rg-x", "title": "Untitled"})):
            self.assertIsNone(
                mb.get_release_group_year("rg-x"))


class TestDiscogsAdapterContract(_MirrorContractCase):
    def test_get_release_raises_HTTPError_on_404(self) -> None:
        with patch("urllib.request.urlopen", _raise_http(404)):
            with self.assertRaises(urllib.error.HTTPError) as ctx:
                discogs.get_release(99999999)
        self.assertEqual(ctx.exception.code, 404)

    def test_get_master_releases_raises_HTTPError_on_404(self) -> None:
        with patch("urllib.request.urlopen", _raise_http(404)):
            with self.assertRaises(urllib.error.HTTPError):
                discogs.get_master_releases(99999999)


class TestFakesMirrorTheRealContract(unittest.TestCase):
    """The fakes in ``tests/fakes.py`` MUST raise the same exception type
    the real adapters raise (asserted above), or Rule B's whole premise —
    "the fake mirrors production" — is hollow."""

    def test_fake_mb_lookup_raises_HTTPError_on_unseeded_id(self) -> None:
        mb_fake = FakeMBLookup()
        with self.assertRaises(urllib.error.HTTPError) as ctx:
            mb_fake("never-seeded")
        self.assertEqual(ctx.exception.code, 404)

    def test_fake_mb_lookup_returns_seeded_payload(self) -> None:
        mb_fake = FakeMBLookup()
        mb_fake.set_release("rel-1", {"id": "rel-1", "title": "X"})
        self.assertEqual(mb_fake("rel-1"), {"id": "rel-1", "title": "X"})
        self.assertEqual(mb_fake.calls, ["rel-1"])

    def test_fake_discogs_lookup_raises_HTTPError_on_unseeded_id(self) -> None:
        dc_fake = FakeDiscogsLookup()
        with self.assertRaises(urllib.error.HTTPError) as ctx:
            dc_fake("404404")
        self.assertEqual(ctx.exception.code, 404)

    def test_fake_opt_out_returns_none_when_configured(self) -> None:
        """The escape hatch for the rare adapter variant that genuinely
        returns None on miss must still be available — documented at the
        call site per the fake's docstring."""
        mb_fake = FakeMBLookup(raises_on_404=False)
        self.assertIsNone(mb_fake("missing"))

    def test_http_error_factory_is_a_urlerror_subclass(self) -> None:
        # HTTPError IS-A URLError — code catching either sees a 404.
        err = http_error(404)
        self.assertIsInstance(err, urllib.error.HTTPError)
        self.assertIsInstance(err, urllib.error.URLError)
        self.assertEqual(err.code, 404)


if __name__ == "__main__":
    unittest.main()
