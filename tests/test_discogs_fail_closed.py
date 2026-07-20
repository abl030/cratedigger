"""Fail-closed contract for every public cached Discogs adapter."""

from __future__ import annotations

import ast
import inspect
import json
import unittest
import urllib.parse
import urllib.request
from collections.abc import Callable
from typing import Any
from unittest.mock import patch

from tests.test_web_cache import FakeRedis
from web import cache, discogs


PUBLIC_CACHED_DISCOGS_ADAPTERS = (
    "search_releases",
    "search_artists",
    "get_artist_releases",
    "get_master_releases",
    "get_release",
    "get_artist_name",
    "search_labels",
    "get_label",
    "get_label_releases",
)


class _JsonResponse:
    def __init__(self, payload: dict[str, Any]) -> None:
        self._body = json.dumps(payload).encode()

    def read(self) -> bytes:
        return self._body

    def __enter__(self) -> _JsonResponse:
        return self

    def __exit__(self, *args: object) -> None:
        return None


class LeafDiscogsMirror:
    """External HTTP-leaf fake; every adapter and cache call remains real."""

    def __init__(self) -> None:
        self.urls: list[str] = []

    def urlopen(
        self,
        request: urllib.request.Request,
        *args: object,
        **kwargs: object,
    ) -> _JsonResponse:
        del args, kwargs
        url = request.full_url
        self.urls.append(url)
        parsed = urllib.parse.urlparse(url)
        path = parsed.path

        if path == "/api/search":
            payload = {"results": []}
        elif path == "/api/artists":
            payload = {"results": []}
        elif path.endswith("/masters/all") or path.endswith("/appearances"):
            payload = {"results": [], "total": 0, "page": 1, "per_page": 100}
        elif path.startswith("/api/masters/"):
            payload = {"releases": []}
        elif path.startswith("/api/releases/"):
            release_id = int(path.rsplit("/", 1)[1])
            payload = {
                "id": release_id,
                "title": "Synthetic Release",
                "country": "",
                "released": "",
                "master_id": None,
                "artists": [],
                "labels": [],
                "formats": [],
                "tracks": [],
            }
        elif path.startswith("/api/artists/"):
            payload = {"name": "Synthetic Artist"}
        elif path == "/api/labels":
            payload = {
                "results": [], "total": 0, "page": 1, "per_page": 25,
            }
        elif path.endswith("/releases") and path.startswith("/api/labels/"):
            payload = {
                "results": [],
                "pagination": {"page": 1, "per_page": 100, "pages": 0, "items": 0},
                "include_sublabels": True,
            }
        elif path.startswith("/api/labels/"):
            label_id = int(path.rsplit("/", 1)[1])
            payload = {
                "id": label_id,
                "name": "Synthetic Label",
                "profile": None,
                "contactinfo": None,
                "data_quality": None,
                "parent_label_id": None,
                "parent_label_name": None,
                "total_releases": 0,
                "sub_labels": [],
            }
        else:
            raise AssertionError(f"unexpected Discogs leaf URL: {url}")
        return _JsonResponse(payload)


def call_public_cached_adapter(
    surface: str, *, query: str, entity_id: int,
) -> object:
    """Dispatch through the real public adapter selected by the matrix."""
    if surface == "search_releases":
        return discogs.search_releases(query)
    if surface == "search_artists":
        return discogs.search_artists(query)
    if surface == "get_artist_releases":
        return discogs.get_artist_releases(entity_id)
    if surface == "get_master_releases":
        return discogs.get_master_releases(entity_id)
    if surface == "get_release":
        return discogs.get_release(entity_id)
    if surface == "get_artist_name":
        return discogs.get_artist_name(entity_id)
    if surface == "search_labels":
        return discogs.search_labels(query)
    if surface == "get_label":
        return discogs.get_label(entity_id)
    if surface == "get_label_releases":
        return discogs.get_label_releases(entity_id)
    raise AssertionError(f"uncovered Discogs adapter: {surface}")


def assert_missing_discogs_blocks(call_adapter: Callable[[], object]) -> None:
    """Reject a cached value unless configuration is checked first."""
    try:
        call_adapter()
    except discogs.DiscogsMirrorNotConfigured:
        return
    raise AssertionError("warm Discogs metadata cache bypassed missing configuration")


class TestPublicCachedDiscogsAdaptersFailClosed(unittest.TestCase):
    def setUp(self) -> None:
        self.saved_base = discogs.DISCOGS_API_BASE
        self.saved_redis = cache._redis

    def tearDown(self) -> None:
        discogs.DISCOGS_API_BASE = self.saved_base
        cache._redis = self.saved_redis

    def test_matrix_covers_every_public_adapter_that_dispatches_metadata_cache(
        self,
    ) -> None:
        tree = ast.parse(inspect.getsource(discogs))
        discovered = {
            node.name
            for node in tree.body
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
            and node.name.startswith(("search_", "get_"))
            and any(
                isinstance(descendant, ast.Call)
                and isinstance(descendant.func, ast.Attribute)
                and descendant.func.attr == "memoize_meta"
                for descendant in ast.walk(node)
            )
        }
        self.assertEqual(discovered, set(PUBLIC_CACHED_DISCOGS_ADAPTERS))

    def test_every_adapter_rejects_before_returning_its_warm_cache_entry(
        self,
    ) -> None:
        for surface in PUBLIC_CACHED_DISCOGS_ADAPTERS:
            with self.subTest(surface=surface):
                mirror = LeafDiscogsMirror()
                cache._redis = FakeRedis()
                discogs.DISCOGS_API_BASE = "https://discogs-mirror.test"
                with patch(
                    "web.discogs.urllib.request.urlopen",
                    side_effect=mirror.urlopen,
                ):
                    call_public_cached_adapter(
                        surface, query="Deloris", entity_id=681,
                    )
                self.assertTrue(mirror.urls, surface)
                self.assertGreater(cache._redis.dbsize(), 0, surface)

                discogs.DISCOGS_API_BASE = None

                def call_warm_adapter() -> object:
                    return call_public_cached_adapter(
                        surface, query="Deloris", entity_id=681,
                    )

                assert_missing_discogs_blocks(call_warm_adapter)

    def test_checker_rejects_a_silent_cached_return(self) -> None:
        def return_cached_value() -> object:
            return {"cached": True}

        with self.assertRaises(AssertionError):
            assert_missing_discogs_blocks(return_cached_value)


if __name__ == "__main__":
    unittest.main()
