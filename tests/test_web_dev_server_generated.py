"""Generated live-db metadata-mirror wiring contract."""

from __future__ import annotations

import ast
import inspect
import os
import threading
import unittest
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, wait
from typing import Any

import psycopg2
from hypothesis import assume, example, given, settings
from hypothesis import strategies as st

import lib.api_bases
import lib.discogs_api
import lib.mb_api
import tests._hypothesis_profiles
import tests.conftest  # noqa: F401 - bootstraps TEST_DB_DSN
import web.routes.browse
import web.server
from lib import redis_cache as cache
from lib.api_bases import PUBLIC_MB_ORIGIN
from lib.mb_canonical import configure_canonical_base, configured_canonical_base
from scripts.web_dev_server import (
    DevConfig,
    DevHandler,
    DevHTTPServer,
    configure_live_db_metadata,
)
from tests.fakes import FakeBeetsDB
from tests.helpers import make_web_runtime
from tests.test_redis_cache import FakeRedis
from tests.test_web_dev_server import (
    StaticParityCase,
    _get_http_outcome,
    _wait_for_blocked_backend,
    assert_live_db_parallel_outcomes,
)
from web.routes.browse import get_artist_compare
from web.runtime import install_runtime, runtime
from web.static_assets import normalized_request_path

TEST_DSN = os.environ["TEST_DB_DSN"]


def assert_metadata_wiring(config: DevConfig) -> None:
    """Configured origins must be exact and missing values must not stay stale.

    #1089 NOTE-3 (review round 2): ``configure_live_db_metadata`` mutates a
    THIRD process-global — ``lib.mb_canonical``'s configured base, wired
    for the merge-rekey button (#1089 NOTE-10) — alongside the two mirror
    origins this checker already asserted; it must agree with the SAME
    ``mb_api`` origin, or a forgotten wiring degrades the button to
    ``mirror_unavailable`` forever while looking configured everywhere
    else.
    """
    expected_mb = config.mb_api or urllib.parse.urljoin(
        f"{PUBLIC_MB_ORIGIN.rstrip('/')}/", "ws/2",
    )
    assert lib.mb_api.MB_API_BASE == expected_mb
    assert lib.discogs_api.DISCOGS_API_BASE == config.discogs_api
    assert configured_canonical_base() == expected_mb


def assert_missing_discogs_blocks(call_route: Callable[[], None]) -> None:
    """A missing mirror must reject before any warm-cache route result."""
    try:
        call_route()
    except lib.discogs_api.DiscogsMirrorNotConfigured:
        return
    raise AssertionError("warm metadata cache bypassed missing Discogs config")


_DISCOGS_ROUTE_CACHE_USERS = {
    "get_artist_compare",
    "get_browse_resolve",
}


def assert_discogs_route_cache_inventory(source: str) -> None:
    """Every route-level cache with a transitive Discogs call guards first."""
    tree = ast.parse(source)
    functions = {
        node.name: node
        for node in tree.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
    }

    def calls_discogs(
        node: ast.FunctionDef | ast.AsyncFunctionDef,
        seen: set[str],
    ) -> bool:
        if node.name in seen:
            return False
        seen.add(node.name)
        for descendant in ast.walk(node):
            if not isinstance(descendant, ast.Call):
                continue
            fn = descendant.func
            if (
                isinstance(fn, ast.Attribute)
                and isinstance(fn.value, ast.Name)
                and fn.value.id == "discogs_api"
                and fn.attr != "require_mirror_configured"
            ):
                return True
            if (
                isinstance(fn, ast.Name)
                and fn.id in functions
                and calls_discogs(functions[fn.id], seen)
            ):
                return True
        return False

    cached_routes: dict[str, ast.FunctionDef | ast.AsyncFunctionDef] = {}
    for name, node in functions.items():
        has_route_cache = any(
            isinstance(descendant, ast.Call)
            and isinstance(descendant.func, ast.Attribute)
            and descendant.func.attr == "memoize_meta"
            for descendant in ast.walk(node)
        )
        if has_route_cache and calls_discogs(node, set()):
            cached_routes[name] = node

    if set(cached_routes) != _DISCOGS_ROUTE_CACHE_USERS:
        raise AssertionError(
            "Discogs-dependent route cache inventory drifted: "
            f"{sorted(cached_routes)} != {sorted(_DISCOGS_ROUTE_CACHE_USERS)}"
        )

    for name, node in cached_routes.items():
        guard_lines = [
            descendant.lineno
            for descendant in ast.walk(node)
            if isinstance(descendant, ast.Call)
            and isinstance(descendant.func, ast.Attribute)
            and isinstance(descendant.func.value, ast.Name)
            and descendant.func.value.id == "discogs_api"
            and descendant.func.attr == "require_mirror_configured"
        ]
        cache_lines = [
            descendant.lineno
            for descendant in ast.walk(node)
            if isinstance(descendant, ast.Call)
            and isinstance(descendant.func, ast.Attribute)
            and descendant.func.attr == "memoize_meta"
        ]
        if not guard_lines or min(guard_lines) >= min(cache_lines):
            raise AssertionError(
                f"{name} must validate Discogs before route-cache dispatch"
            )


class _RouteHandler:
    def __init__(self) -> None:
        self.payload: dict[str, Any] | None = None

    def _json(self, payload: dict[str, Any], status: int = 200) -> None:
        if status != 200:
            raise AssertionError(f"unexpected route status {status}")
        self.payload = payload

    def _error(self, message: str, status: int = 400) -> None:
        raise AssertionError(f"unexpected route error {status}: {message}")


class _QuietDevHandler(DevHandler):
    def log_message(self, format: str, *args: Any) -> None:
        del format, args


_HOST = st.text(
    alphabet=st.characters(min_codepoint=ord("a"), max_codepoint=ord("z")),
    min_size=1,
    max_size=12,
)
_ORIGIN = st.builds(
    lambda scheme, host, port: f"{scheme}://{host}.test:{port}",
    st.sampled_from(("http", "https")),
    _HOST,
    st.integers(min_value=1, max_value=65535),
)


def _config(*, mb_api: str | None, discogs_api: str | None) -> DevConfig:
    return DevConfig(
        data="live-db",
        scenario="generated",
        prod_base_url="https://music.ablz.au",
        dsn="postgresql://unused-by-metadata-wiring",
        beets_db=None,
        mb_api=mb_api,
        discogs_api=discogs_api,
        redis_host=None,
        redis_port=6379,
    )


class TestLiveDbMetadataWiringGenerated(unittest.TestCase):
    def setUp(self) -> None:
        self.saved = (
            lib.mb_api.MB_API_BASE,
            lib.discogs_api.DISCOGS_API_BASE,
        )
        # #1089 NOTE-3 (review round 2): the third process-global
        # configure_live_db_metadata now mutates.
        self.saved_canonical_base = configured_canonical_base()
        self.saved_redis = cache._redis

    def tearDown(self) -> None:
        lib.mb_api.MB_API_BASE, lib.discogs_api.DISCOGS_API_BASE = self.saved
        configure_canonical_base(self.saved_canonical_base)
        cache._redis = self.saved_redis

    @given(
        stale_mb=_ORIGIN,
        stale_discogs=_ORIGIN,
        mb_origin=st.one_of(st.none(), _ORIGIN),
        discogs_origin=st.one_of(st.none(), _ORIGIN),
    )
    def test_each_configuration_exactly_replaces_prior_process_state(
        self,
        stale_mb: str,
        stale_discogs: str,
        mb_origin: str | None,
        discogs_origin: str | None,
    ) -> None:
        configure_live_db_metadata(_config(
            mb_api=f"{stale_mb}/old-ws",
            discogs_api=f"{stale_discogs}/old-api",
        ))
        config = _config(
            mb_api=f"{mb_origin}/ws/2" if mb_origin else None,
            discogs_api=discogs_origin,
        )
        configure_live_db_metadata(config)
        assert_metadata_wiring(config)

    @given(
        mbid=_HOST,
        discogs_id=st.integers(min_value=1, max_value=2_000_000_000),
        artist_name=_HOST,
    )
    def test_missing_discogs_rejects_before_arbitrary_warm_compare_cache(
        self, mbid: str, discogs_id: int, artist_name: str,
    ) -> None:
        config = _config(mb_api=None, discogs_api=None)
        configure_live_db_metadata(config)
        cache._redis = FakeRedis()
        cache.meta_set(
            f"artist:compare:v8:{mbid}:{discogs_id}",
            {
                "both": [],
                "mb_unpaired": [],
                "discogs_unpaired": [],
                "discogs_ungrouped_releases": [],
            },
        )
        cache.meta_set(f"mb:artist:{mbid}:name", artist_name)
        cache.meta_set(f"discogs:artist:{discogs_id}:name", artist_name)
        handler = _RouteHandler()
        params = {
            "name": [artist_name],
            "mbid": [mbid],
            "discogs_id": [str(discogs_id)],
        }

        assert_missing_discogs_blocks(
            lambda: get_artist_compare(handler, params),  # type: ignore[arg-type]
        )
        self.assertIsNone(handler.payload)


class TestBrowseResolveWarmCacheGenerated(unittest.TestCase):
    def setUp(self) -> None:
        self.saved_base = lib.discogs_api.DISCOGS_API_BASE
        self.saved_mb_base = lib.mb_api.MB_API_BASE
        # #1089 NOTE-3 (review round 2): the third process-global
        # configure_live_db_metadata now mutates.
        self.saved_canonical_base = configured_canonical_base()
        self.saved_redis = cache._redis
        configure_live_db_metadata(_config(mb_api=None, discogs_api=None))
        cache._redis = FakeRedis()
        self.server = DevHTTPServer(
            ("127.0.0.1", 0),
            _QuietDevHandler,
            _config(mb_api=None, discogs_api=None),
        )
        self.thread = threading.Thread(
            target=self.server.serve_forever,
            daemon=True,
        )
        self.thread.start()
        self.base = f"http://127.0.0.1:{self.server.server_port}"

    def tearDown(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2)
        lib.discogs_api.DISCOGS_API_BASE = self.saved_base
        lib.mb_api.MB_API_BASE = self.saved_mb_base
        configure_canonical_base(self.saved_canonical_base)
        cache._redis = self.saved_redis

    @given(
        discogs_id=st.integers(min_value=1, max_value=2_000_000_000),
        kind=st.sampled_from(("release", "master", "unknown")),
        marker=_HOST,
    )
    def test_missing_discogs_returns_503_before_arbitrary_warm_resolver_cache(
        self, discogs_id: int, kind: str, marker: str,
    ) -> None:
        cache._redis = FakeRedis()
        cache_key = f"browse-resolve:v2:discogs:{kind}:{discogs_id}"
        cache.meta_set(cache_key, {
            "source": "discogs",
            "kind": "master" if kind == "master" else "release",
            "artist_id": marker,
            "artist_name": marker,
            "is_va": False,
            "target_identity_kind": "work" if kind == "master" else "release",
            "expand_id": str(discogs_id),
            "leaf_id": None if kind == "master" else str(discogs_id),
        })
        self.assertIn(f"meta:{cache_key}", cache._redis._store)

        url = (
            f"{self.base}/api/browse/resolve?source=discogs&"
            f"id={discogs_id}&kind={kind}"
        )
        # Keep this bounded while allowing scheduling headroom for the server
        # thread on a busy development host.
        with self.assertRaises(urllib.error.HTTPError) as raised:
            urllib.request.urlopen(url, timeout=10)
        self.assertEqual(raised.exception.code, 503)
        raised.exception.close()


class TestLiveDbConcurrentReadsGenerated(unittest.TestCase):
    """Generated real-HTTP patrol for the singleton read-only DB session."""

    def setUp(self) -> None:
        # One installed WebRuntime replaces the six module globals this
        # used to save and restore (#1313). Registered BEFORE the
        # enterContext below so LIFO cleanup uninstalls the derived
        # runtime first and this pops the live-db one underneath it —
        # the reverse order leaves the live-db runtime installed with an
        # already-closed handle for whatever runs next.
        from scripts.web_dev_server import reset_live_db_runtime

        self.addCleanup(reset_live_db_runtime)
        config = DevConfig(
            data="live-db",
            scenario="generated",
            prod_base_url="https://music.ablz.au",
            dsn=TEST_DSN,
            beets_db=None,
            mb_api=None,
            discogs_api=None,
            redis_host=None,
            redis_port=6379,
        )
        from scripts.web_dev_server import create_server

        self.server = create_server("127.0.0.1", 0, config)
        self.enterContext(install_runtime(
            make_web_runtime(runtime(), beets=FakeBeetsDB()),
        ))
        self.thread = threading.Thread(
            target=self.server.serve_forever, daemon=True,
        )
        self.thread.start()
        self.base = f"http://127.0.0.1:{self.server.server_port}"

    def tearDown(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2)

    @settings(max_examples=30)
    @example(request_count=6)
    @given(request_count=st.integers(min_value=2, max_value=8))
    def test_repeated_parallel_library_reads_all_complete(
        self, request_count: int,
    ) -> None:
        shared = runtime().shared_db
        self.assertIsNotNone(shared)
        assert shared is not None
        backend_pid = shared.conn.get_backend_pid()
        path = "/api/library/artist?name=Generated&mbid="

        blocker = psycopg2.connect(TEST_DSN)
        blocker.autocommit = False
        try:
            with blocker.cursor() as cursor:
                cursor.execute(
                    "LOCK TABLE album_requests IN ACCESS EXCLUSIVE MODE"
                )
            with ThreadPoolExecutor(max_workers=request_count) as executor:
                first = executor.submit(_get_http_outcome, self.base, path)
                _wait_for_blocked_backend(TEST_DSN, backend_pid)
                rest = [
                    executor.submit(_get_http_outcome, self.base, path)
                    for _ in range(request_count - 1)
                ]
                wait(rest, timeout=0.05)
                blocker.commit()
                outcomes = [first.result(timeout=10)] + [
                    future.result(timeout=10) for future in rest
                ]
        finally:
            blocker.rollback()
            blocker.close()

        assert_live_db_parallel_outcomes(outcomes)


class TestLiveDbConcurrentReadsCheckerKnownBad(unittest.TestCase):
    def test_checker_rejects_the_historical_psycopg_failures(self) -> None:
        with self.assertRaisesRegex(
            AssertionError, "asynchronous query is underway",
        ):
            assert_live_db_parallel_outcomes([
                (
                    "/api/artist/example",
                    500,
                    {"error": (
                        "execute cannot be used while an asynchronous query "
                        "is underway"
                    )},
                ),
                (
                    "/api/artist/compare",
                    500,
                    {"error": "cursor already closed"},
                ),
            ])


class TestDiscogsRouteCacheInventory(unittest.TestCase):
    def test_exact_discogs_dependent_route_cache_inventory_is_guarded(self) -> None:
        assert_discogs_route_cache_inventory(inspect.getsource(web.routes.browse))

    def test_checker_rejects_resolver_guard_removed(self) -> None:
        source = inspect.getsource(web.routes.browse)
        guard = (
            '    if source == "discogs":\n'
            "        discogs_api.require_mirror_configured()\n"
        )
        mutant = source.replace(guard, "", 1)
        self.assertNotEqual(mutant, source)
        with self.assertRaises(AssertionError):
            assert_discogs_route_cache_inventory(mutant)


class TestMetadataWiringCheckerKnownBad(unittest.TestCase):
    def setUp(self) -> None:
        self.saved = (
            lib.mb_api.MB_API_BASE,
            lib.discogs_api.DISCOGS_API_BASE,
        )
        # #1089 NOTE-3 (review round 2): the third process-global
        # configure_live_db_metadata now mutates —
        # test_missing_mb_uses_the_canonical_public_ws2_declaration below
        # calls it directly.
        self.saved_canonical_base = configured_canonical_base()

    def tearDown(self) -> None:
        lib.mb_api.MB_API_BASE, lib.discogs_api.DISCOGS_API_BASE = self.saved
        configure_canonical_base(self.saved_canonical_base)

    def test_checker_rejects_swapped_origins(self) -> None:
        config = _config(
            mb_api="https://mb.test/ws/2",
            discogs_api="https://discogs.test",
        )
        lib.mb_api.MB_API_BASE = config.discogs_api or ""
        lib.discogs_api.DISCOGS_API_BASE = config.mb_api
        with self.assertRaises(AssertionError):
            assert_metadata_wiring(config)

    def test_checker_rejects_stale_discogs_when_configuration_is_missing(
        self,
    ) -> None:
        config = _config(mb_api=None, discogs_api=None)
        lib.mb_api.MB_API_BASE = urllib.parse.urljoin(
            f"{PUBLIC_MB_ORIGIN.rstrip('/')}/", "ws/2",
        )
        lib.discogs_api.DISCOGS_API_BASE = "https://stale-discogs.test"
        with self.assertRaises(AssertionError):
            assert_metadata_wiring(config)

    def test_missing_mb_uses_the_canonical_public_ws2_declaration(self) -> None:
        config = _config(mb_api=None, discogs_api=None)
        sentinel = "https://canonical-mb.test/custom-ws2"
        saved_public_base = lib.api_bases.PUBLIC_MB_WS2_BASE
        try:
            lib.api_bases.PUBLIC_MB_WS2_BASE = sentinel
            configure_live_db_metadata(config)
        finally:
            lib.api_bases.PUBLIC_MB_WS2_BASE = saved_public_base
        self.assertEqual(lib.mb_api.MB_API_BASE, sentinel)

    def test_warm_cache_guard_checker_rejects_a_silent_route(self) -> None:
        with self.assertRaises(AssertionError):
            assert_missing_discogs_blocks(lambda: None)


if __name__ == "__main__":
    unittest.main()


# --- Static-surface parity, generated (#1390 residual 8, review F2) ---
#
# `web/static_assets.py` is a shared library function, and
# `.claude/rules/code-quality.md` is explicit that agreement proven at a
# shared library function is not agreement at the adapter. The adapters
# here are `web.server.Handler.do_GET` and
# `scripts.web_dev_server.DevHandler.do_GET`, and the first draft of that
# module proved the point: it shared the RULE and not the normalization
# production applies before asking it, so `/js/main.js/` and an
# absolute-form request target both served on production and 404d on the
# dev server. The deterministic table lives in
# `tests/test_web_dev_server.py::WebDevServerProductionParityTest`; this
# patrols the space around it.

#: Segments the strategy composes request targets from. Chosen so the
#: interesting shapes are reachable rather than plausible: both halves of
#: each rule (`js`, `main.js`, the icon names), each half alone, the near
#: misses (`jsconfig.json`, `globals.d.ts`, `server.py`), the traversal
#: forms, and the separators that normalization is about.
_TARGET_SEGMENTS = (
    "js", "JS", "main.js", "util.js", "nope.js", "jsconfig.json",
    "globals.d.ts", "server.py", "classify.py", "routes", "assets",
    "favicon.ico", "favicon-16x16.png", "apple-touch-icon.png",
    "index.html", "..", "%2e%2e", ".js", "", "x",
)


#: Whole targets worth drawing directly. Free composition from segments
#: alone leaves the interesting cells unreachable in practice: measured at
#: 60 examples, it never once composed a servable path AND a trailing
#: slash, so the property passed against a dev server with the
#: normalization removed. That is the entropy-budget miss
#: `.claude/rules/code-quality.md` describes, and this is the widening it
#: prescribes rather than leaving the pin to carry the whole guard.
_BASE_TARGETS = (
    "/", "/index.html", "/js/main.js", "/js/util.js", "/js/nope.js",
    "/js/jsconfig.json", "/js/globals.d.ts", "/js/../server.py",
    "/js/../main.js", "/js/%2e%2e/main.js", "/js", "/JS/main.js",
    "/favicon.ico", "/favicon-16x16.png", "/favicon-32x32.png",
    "/apple-touch-icon.png", "/assets/favicon.ico", "/server.py",
    "/classify.py", "/routes/pipeline.py", "/not-a-route",
)


@st.composite
def _static_request_targets(draw: st.DrawFn) -> str:
    """A request target neither server routes as an API call."""
    base = draw(st.one_of(
        st.sampled_from(_BASE_TARGETS),
        st.builds(
            lambda segments: "/" + "/".join(segments),
            st.lists(st.sampled_from(_TARGET_SEGMENTS), max_size=4),
        ),
    ))
    return base + draw(st.sampled_from(("", "/", "//", "///")))


class TestStaticSurfaceParity(StaticParityCase):
    """Neither server may answer a static target the other refuses."""

    @settings(max_examples=60, deadline=None)
    @given(target=_static_request_targets())
    @example(target="/js/main.js/")
    @example(target="/js/main.js")
    @example(target="/favicon.ico/")
    @example(target="/index.html")
    @example(target="/js/../server.py")
    @example(target="/")
    def test_both_servers_answer_the_same(self, target: str) -> None:
        # `/api/` is the dev server's own fixture/proxy surface and has
        # nothing to do with the static rule; production routes it through
        # a registry this test does not seed.
        assume(not target.startswith("/api"))
        assume(not target.startswith("/__dev"))
        assume(normalized_request_path(target)
               not in web.server.Handler._FUNC_GET_ROUTES)
        production, dev = self.both_statuses(target)
        self.assertEqual(
            production, dev, f"{target}: production {production}, dev {dev}")

    def test_the_strategy_reaches_both_answers(self) -> None:
        # A strategy that only ever drew 404-on-both targets would make the
        # property above unfalsifiable. Prove both answers are producible
        # by naming one of each that the strategy can compose.
        self.assertEqual(self.both_statuses("/js/main.js"), (200, 200))
        self.assertEqual(self.both_statuses("/js/server.py"), (404, 404))
