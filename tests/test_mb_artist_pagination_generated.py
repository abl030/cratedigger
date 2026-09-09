"""Real-HTTP generated contracts for cold MusicBrainz artist pagination.

The server intentionally models the nested-recordings browse quirk observed
for Taylor Swift and The Beatles: a short ``limit=100`` page followed by the
old ``offset += len(page)``/``limit=100`` request overlaps an earlier ID.
Tests never replace ``lib.mb_api._get`` or its paginator; production urllib code
talks to this local threaded HTTP server.
"""
from __future__ import annotations

import json
import threading
import time
import unittest
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Callable
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from unittest.mock import MagicMock, patch

from hypothesis import given, settings
from hypothesis import strategies as st

import tests._hypothesis_profiles  # noqa: F401
from lib import mb_api as mb
from lib.api_bases import PUBLIC_MB_WS2_BASE

ARTIST_ID = "00000000-0000-0000-0000-000000000917"

# The two client-side fan-out widths this module pins, spelled here rather
# than read from production: a mutant that narrows either one must move the
# assertion, not the target it is measured against.
#   * four concurrent requests per custom mirror -- web/mb.py::_MB_MIRROR_CONCURRENCY,
#     also pinned directly by test_public_musicbrainz_stays_serial_while_custom_mirror_uses_four_slots
#   * three independent browse families per catalogue fetch -- the
#     ``max_workers=3`` fan-out in web/mb.py::get_artist_release_groups
_MIRROR_SLOTS = 4
_BROWSE_FAMILIES = 3


@dataclass(frozen=True)
class _FanoutGates:
    """Fan-out facts the mirror ENFORCES, so no assertion has to observe one.

    ``concurrent_request_waves`` is a ladder of arrival widths: the mirror
    holds each arriving handler before it answers until that many requests
    are in flight at once, then opens that rung for good and moves to the
    next.  The client cannot reach a rung it does not fan out far enough to
    fill, so a narrowed client fails loudly instead of quietly passing.

    ``overlapping_handlers`` is applied after the response is written: a
    handler stays alive until that many handler threads overlap.  The client
    has already read its body and dropped its lease by then, which is the
    whole point -- server thread lifetime is not the client's mirror lease.
    """

    concurrent_request_waves: tuple[int, ...]
    overlapping_handlers: int
    # Bounded well under production's own 15s urlopen timeout so a client
    # that stops fanning out records a named failure instead of colliding
    # with a socket timeout and a production retry.
    timeout_seconds: float = 10.0


class _FanoutLatch:
    """Hold mirror handlers until the gated fan-out facts are true."""

    def __init__(
        self, gates: _FanoutGates, lock: threading.Lock, alive: Callable[[], int],
    ) -> None:
        self.gates = gates
        self.failures: list[str] = []
        self._alive = alive
        self._condition = threading.Condition(lock)
        self._rung = 0
        self._waiting = 0
        self._overlap_open = False
        self._abandoned = False

    def wake_all(self) -> None:
        """Re-check every held handler.  Call with the shared lock held."""
        self._condition.notify_all()

    def hold_before_write(self) -> None:
        with self._condition:
            rung = self._rung
            if self._abandoned or rung >= len(self.gates.concurrent_request_waves):
                return
            wanted = self.gates.concurrent_request_waves[rung]
            self._waiting += 1
            if self._waiting >= wanted:
                self._open_rung(rung)
                return
            deadline = time.monotonic() + self.gates.timeout_seconds
            while self._rung == rung and not self._abandoned:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    self.failures.append(
                        f"client never had {wanted} requests in flight at once "
                        f"(wave {rung}; {self._waiting} arrived)",
                    )
                    self._open_rung(rung)
                    return
                self._condition.wait(remaining)

    def hold_after_write(self) -> None:
        with self._condition:
            wanted = self.gates.overlapping_handlers
            deadline = time.monotonic() + self.gates.timeout_seconds
            while not self._overlap_open and not self._abandoned:
                if self._alive() >= wanted:
                    self._overlap_open = True
                    self._condition.notify_all()
                    return
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    self.failures.append(
                        f"only {self._alive()} handler threads ever overlapped; "
                        f"wanted {wanted}",
                    )
                    self._overlap_open = True
                    self._condition.notify_all()
                    return
                self._condition.wait(remaining)

    def abandon(self) -> None:
        """Release every held handler at mirror teardown.

        A client that raised early leaves handlers held; releasing them keeps
        teardown bounded.  Anything still shut records its own failure, so an
        abandoned run can never read as a satisfied gate.
        """
        with self._condition:
            self._abandoned = True
            if self._rung < len(self.gates.concurrent_request_waves):
                self.failures.append(
                    "mirror shut down with the arrival ladder still closed at wave "
                    f"{self._rung}",
                )
            if not self._overlap_open:
                self.failures.append(
                    "mirror shut down before "
                    f"{self.gates.overlapping_handlers} handler threads overlapped",
                )
            self._condition.notify_all()

    def _open_rung(self, rung: int) -> None:
        self._rung = rung + 1
        self._waiting = 0
        self._condition.notify_all()


class _NestedRecordingWorld:
    def __init__(
        self, total: int, short_page: int = 59, *, delay: float = 0,
        after_write_delay: float = 0,
        catalogue_short_page: int | None = None, fail_release_groups: bool = False,
        blank_catalogue_id: bool = False, catalogue_total: int | None = None,
        fanout_gates: _FanoutGates | None = None,
    ) -> None:
        self.ids = [f"release-{index:05d}" for index in range(total)]
        self.short_page = short_page
        self.catalogue_short_page = catalogue_short_page
        self.fail_release_groups = fail_release_groups
        self.blank_catalogue_id = blank_catalogue_id
        self.catalogue_total = catalogue_total
        self.delay = delay
        self.after_write_delay = after_write_delay
        self.active_server_handlers = 0
        self.max_active_server_handlers = 0
        self._lock = threading.Lock()
        self.latch = (
            None if fanout_gates is None
            else _FanoutLatch(
                fanout_gates, self._lock, lambda: self.active_server_handlers,
            )
        )

    def begin_handler(self) -> None:
        with self._lock:
            self.active_server_handlers += 1
            self.max_active_server_handlers = max(
                self.max_active_server_handlers, self.active_server_handlers,
            )
            if self.latch is not None:
                self.latch.wake_all()

    def end_handler(self) -> None:
        with self._lock:
            self.active_server_handlers -= 1
            if self.latch is not None:
                self.latch.wake_all()

    def response(self, path: str, query: dict[str, list[str]]) -> dict[str, object]:
        offset = int(query.get("offset", ["0"])[0])
        limit = int(query.get("limit", ["100"])[0])
        if path.endswith("/release-group"):
            if self.fail_release_groups:
                return {"release-group-count": -1, "release-groups": []}
            size = min(limit, self.catalogue_short_page or limit)
            groups = [
                {
                    "id": "" if self.blank_catalogue_id and index == 0 else f"rg-{index:05d}",
                    "title": f"Group {index}",
                    "primary-type": "Album", "first-release-date": "2000",
                    "artist-credit": [{"name": "Artist", "artist": {
                        "id": ARTIST_ID, "name": "Artist",
                    }}],
                }
                for index in range(offset, min(offset + size, len(self.ids)))
            ]
            return {
                "release-group-count": self.catalogue_total if self.catalogue_total is not None else len(self.ids),
                "release-groups": groups,
            }

        if query.get("track_artist"):
            size = min(limit, self.catalogue_short_page or limit)
            releases = [
                {
                    "id": f"track-{index:05d}", "status": "Official",
                    "release-group": {
                        "id": f"track-rg-{index:05d}", "title": f"Track {index}",
                        "primary-type": "Album", "first-release-date": "2000",
                        "artist-credit": [{"name": "Various", "artist": {
                            "id": "various", "name": "Various",
                        }}],
                    },
                }
                for index in range(offset, min(offset + size, len(self.ids)))
            ]
            return {"release-count": len(self.ids), "releases": releases}

        if path.endswith("/release") and "inc" not in query:
            return {
                "release-count": len(self.ids),
                "releases": [{"id": release_id} for release_id in self.ids[offset:offset + limit]],
            }

        if path.endswith("/release") and query.get("inc") == ["release-groups"]:
            size = min(limit, self.catalogue_short_page or limit)
            return {
                "release-count": len(self.ids),
                "releases": [
                    {"id": release_id, "status": "Official", "release-group": {"id": "rg"}}
                    for release_id in self.ids[offset:offset + size]
                ],
            }

        if path.endswith("/release"):
            # The known-bad shape: after a short nested page, preserving a
            # 100-row limit starts one row early.  Reducing the limit to the
            # exact segment remainder avoids that specific overlap.
            start = offset - 1 if limit == 100 and offset > 0 else offset
            size = min(self.short_page, limit)
            return {
                "release-count": len(self.ids),
                "releases": [
                    {"id": release_id, "media": [], "release-group": {"id": "rg"}}
                    for release_id in self.ids[start:start + size]
                ],
            }

        if "/release/" in path:
            release_id = path.rsplit("/", 1)[-1]
            return {"id": release_id, "media": [], "release-group": {"id": "rg"}}
        raise AssertionError(f"unexpected request {path}?{query}")


class _Mirror:
    def __init__(self, world: _NestedRecordingWorld) -> None:
        self.world = world
        mirror = self

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self) -> None:
                world = mirror.world
                world.begin_handler()
                try:
                    if world.delay:
                        time.sleep(world.delay)
                    parsed = urllib.parse.urlsplit(self.path)
                    body = json.dumps(world.response(
                        parsed.path, urllib.parse.parse_qs(parsed.query),
                    )).encode()
                    if world.latch is not None:
                        world.latch.hold_before_write()
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json")
                    self.send_header("Content-Length", str(len(body)))
                    self.end_headers()
                    # ``wbufsize = 0``: the body reaches the client here, so the
                    # client drops its mirror lease while this thread is held.
                    self.wfile.write(body)
                    if world.latch is not None:
                        world.latch.hold_after_write()
                    if world.after_write_delay:
                        time.sleep(world.after_write_delay)
                finally:
                    world.end_handler()

            def log_message(self, format: str, *_args: object) -> None:
                pass

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever)

    def __enter__(self) -> str:
        self.thread.start()
        address = self.server.server_address
        host = str(address[0])
        port = int(address[1])
        return f"http://{host}:{port}/ws/2"

    def __exit__(self, *_exc: object) -> None:
        # ``server_close`` joins handler threads, so release anything still
        # held before teardown; a gate that never opened records its own
        # failure inside ``abandon``.
        if self.world.latch is not None:
            self.world.latch.abandon()
        self.server.shutdown()
        self.thread.join()
        self.server.server_close()


class _ClientSlotSemaphore(threading.BoundedSemaphore):
    """Observe the exact client-side lease guarded by production ``_get``."""

    def __init__(self, slots: int) -> None:
        super().__init__(slots)
        self.active = 0
        self.max_active = 0
        self._probe_lock = threading.Lock()

    def __enter__(
        self, blocking: bool = True, timeout: float | None = None,
    ) -> bool:
        acquired = super().acquire(blocking, timeout)
        if not acquired:
            return False
        with self._probe_lock:
            self.active += 1
            self.max_active = max(self.max_active, self.active)
        return True

    def __exit__(self, *_exc: object) -> None:
        with self._probe_lock:
            self.active -= 1
        super().release()


class _ClientSlotProbe:
    """Observe the semaphore built by production for one mirror origin."""

    def __init__(self, api_base: str) -> None:
        self.api_base = api_base
        self.origin = mb._mirror_origin(api_base)
        self.semaphore: _ClientSlotSemaphore | None = None
        self._previous: threading.BoundedSemaphore | None = None

    def __enter__(self) -> _ClientSlotSemaphore:
        with mb._mb_mirror_semaphores_lock:
            self._previous = mb._mb_mirror_semaphores.pop(self.origin, None)
        try:
            # Intercept only construction: production still chooses the capacity,
            # origin key, registry entry, and semaphore used by every request.
            with patch.object(mb.threading, "BoundedSemaphore", _ClientSlotSemaphore):
                semaphore = mb._mirror_semaphore(self.api_base)
        except BaseException:
            with mb._mb_mirror_semaphores_lock:
                if self._previous is not None:
                    mb._mb_mirror_semaphores[self.origin] = self._previous
            raise
        if not isinstance(semaphore, _ClientSlotSemaphore):
            with mb._mb_mirror_semaphores_lock:
                mb._mb_mirror_semaphores.pop(self.origin, None)
                if self._previous is not None:
                    mb._mb_mirror_semaphores[self.origin] = self._previous
            raise TypeError("production did not construct the observable semaphore")
        self.semaphore = semaphore
        return semaphore

    def __exit__(self, *_exc: object) -> None:
        with mb._mb_mirror_semaphores_lock:
            current = mb._mb_mirror_semaphores.pop(self.origin, None)
            if self._previous is not None:
                mb._mb_mirror_semaphores[self.origin] = self._previous
        if current is not self.semaphore:
            raise AssertionError("production replaced the observed mirror semaphore")


def _without_metadata_cache(
    _key: str, fetch: Callable[[], object], *_args: object, **_kwargs: object,
) -> object:
    """Cold-cache seam: call production fetches without Redis/process results."""
    return fetch()


def assert_complete_order(expected: list[str], actual: list[str]) -> None:
    if actual != expected:
        raise AssertionError(f"identity/order drift: {len(actual)} != {len(expected)}")


def assert_request_cap(max_active: int, cap: int = 4) -> None:
    if max_active > cap:
        raise AssertionError(f"mirror cap exceeded: {max_active} > {cap}")


def assert_catalogue_identity_count(expected: int, actual: int) -> None:
    if actual != expected:
        raise AssertionError(f"catalogue identity loss: {actual} != {expected}")


def _old_offset_len_shape(world: _NestedRecordingWorld) -> list[str]:
    """The former fixed-limit walk, retained only as a known-bad test input."""
    offset = 0
    old_ids: list[str] = []
    while offset < len(world.ids):
        start = offset - 1 if offset > 0 else offset
        page = world.ids[start:start + world.short_page]
        old_ids.extend(page)
        offset += len(page)
    return old_ids


class TestArtistRecordingPaginationPins(unittest.TestCase):
    def _fetch(self, total: int, short_page: int = 59) -> tuple[list[str], _NestedRecordingWorld]:
        world = _NestedRecordingWorld(total, short_page)
        with _Mirror(world) as api_base, patch.object(mb, "MB_API_BASE", api_base), patch(
            "lib.mb_api._cache.memoize_meta", side_effect=_without_metadata_cache,
        ):
            releases = mb.get_artist_releases_with_recordings(ARTIST_ID)
        return [release.get("id", "") for release in releases], world

    def test_taylor_and_beatles_short_overlap_shapes_conserve_every_id(self) -> None:
        for artist, total in (("Taylor Swift", 2436), ("The Beatles", 3269)):
            with self.subTest(artist=artist):
                actual, world = self._fetch(total)
                assert_complete_order(world.ids, actual)
                self.assertEqual(len(actual), len(set(actual)))

    def test_catalogue_fanout_is_globally_bounded_and_stably_normalized(self) -> None:
        # The mirror ENFORCES this overlap instead of the test observing one.
        # A 250-row catalogue is fetched as three independent browse families
        # (three first pages), then six segment pages the client may only run
        # four at a time.  So the arrival ladder is (3, 4): the first rung can
        # only be filled by all three families being in flight together, and
        # the second only by the client leasing every mirror slot at once.
        # Handlers are then held past their own response until more of them
        # are alive than the client is ever allowed to lease -- server thread
        # lifetime is not the client semaphore lease.  A client that stops
        # fanning out cannot fill a rung, and records a named failure rather
        # than hanging or reading whatever the host's idle cores allowed.
        gates = _FanoutGates(
            concurrent_request_waves=(_BROWSE_FAMILIES, _MIRROR_SLOTS),
            overlapping_handlers=_MIRROR_SLOTS + 1,
        )
        world = _NestedRecordingWorld(250, fanout_gates=gates)
        with _Mirror(world) as api_base, _ClientSlotProbe(api_base) as probe, patch.object(
            mb, "MB_API_BASE", api_base,
        ), patch(
            "lib.mb_api._cache.memoize_meta", side_effect=_without_metadata_cache,
        ):
            rows = mb.get_artist_release_groups(ARTIST_ID)
        assert world.latch is not None
        self.assertEqual(world.latch.failures, [])
        assert_request_cap(probe.max_active)
        self.assertEqual(probe.max_active, _MIRROR_SLOTS)
        self.assertGreater(world.max_active_server_handlers, _MIRROR_SLOTS)
        self.assertEqual(
            [(row.first_release_date, row.id) for row in rows],
            sorted((row.first_release_date, row.id) for row in rows),
        )

    def test_fanout_latch_names_the_gate_a_narrowed_client_cannot_fill(self) -> None:
        # Known-bad self-test, one world per clause of the latch's own
        # reporting.  Both worlds are single-threaded: nothing else can fill
        # the gate, which is exactly the shape a client that stopped fanning
        # out presents.
        arrivals = _FanoutLatch(
            _FanoutGates(
                concurrent_request_waves=(2,), overlapping_handlers=1,
                timeout_seconds=0.01,
            ),
            threading.Lock(), lambda: 1,
        )
        arrivals.hold_before_write()
        self.assertEqual(
            arrivals.failures,
            ["client never had 2 requests in flight at once (wave 0; 1 arrived)"],
        )
        overlap = _FanoutLatch(
            _FanoutGates(
                concurrent_request_waves=(), overlapping_handlers=2,
                timeout_seconds=0.01,
            ),
            threading.Lock(), lambda: 1,
        )
        overlap.hold_after_write()
        self.assertEqual(
            overlap.failures,
            ["only 1 handler threads ever overlapped; wanted 2"],
        )
        # Q3: a gate its world does reach stays quiet, and says so at teardown.
        satisfied = _FanoutLatch(
            _FanoutGates(
                concurrent_request_waves=(1,), overlapping_handlers=1,
                timeout_seconds=0.01,
            ),
            threading.Lock(), lambda: 1,
        )
        satisfied.hold_before_write()
        satisfied.hold_after_write()
        satisfied.abandon()
        self.assertEqual(satisfied.failures, [])

    def test_fanout_latch_reports_a_teardown_that_beat_its_own_gates(self) -> None:
        # A client that raised early leaves the gates shut; teardown must
        # release the handlers AND refuse to read as a satisfied run.
        latch = _FanoutLatch(
            _FanoutGates(concurrent_request_waves=(2,), overlapping_handlers=2),
            threading.Lock(), lambda: 0,
        )
        latch.abandon()
        self.assertEqual(latch.failures, [
            "mirror shut down with the arrival ladder still closed at wave 0",
            "mirror shut down before 2 handler threads overlapped",
        ])
        # Abandoned gates never block again: teardown stays bounded.
        latch.hold_before_write()
        latch.hold_after_write()
        self.assertEqual(len(latch.failures), 2)

    def test_short_catalogue_pages_fill_each_counted_segment(self) -> None:
        world = _NestedRecordingWorld(150, catalogue_short_page=80)
        with _Mirror(world) as api_base, patch.object(mb, "MB_API_BASE", api_base), patch(
            "lib.mb_api._cache.memoize_meta", side_effect=_without_metadata_cache,
        ):
            rows = mb.get_artist_release_groups(ARTIST_ID)
        # Direct work and track appearances deliberately occupy separate IDs.
        assert_catalogue_identity_count(300, len(rows))

    def test_counted_catalogue_rejects_blank_identity_and_zero_total_with_rows(self) -> None:
        for world in (
            _NestedRecordingWorld(2, blank_catalogue_id=True),
            _NestedRecordingWorld(1, catalogue_total=0),
        ):
            with self.subTest(world=world.__dict__), _Mirror(world) as api_base, patch.object(
                mb, "MB_API_BASE", api_base,
            ), patch(
                "lib.mb_api._cache.memoize_meta", side_effect=_without_metadata_cache,
            ), self.assertRaises(mb.MusicBrainzArtistCatalogueIncomplete):
                mb.get_artist_release_groups(ARTIST_ID)

    def test_identity_checker_rejects_the_observed_old_overlap(self) -> None:
        world = _NestedRecordingWorld(2436)
        with self.assertRaisesRegex(AssertionError, "identity/order drift"):
            assert_complete_order(world.ids, _old_offset_len_shape(world))
        with self.assertRaises(mb.MusicBrainzArtistCatalogueIncomplete):
            mb.assert_exact_release_id_order(
                world.ids[:2],
                [
                    mb._MBReleaseFullStruct(id=world.ids[0]),
                    mb._MBReleaseFullStruct(id=world.ids[0]),
                ],
            )
        with self.assertRaisesRegex(AssertionError, "mirror cap exceeded"):
            assert_request_cap(5)
        with self.assertRaisesRegex(AssertionError, "catalogue identity loss"):
            assert_catalogue_identity_count(150, 130)

    def test_public_musicbrainz_stays_serial_while_custom_mirror_uses_four_slots(self) -> None:
        self.assertEqual(mb._mirror_concurrency(PUBLIC_MB_WS2_BASE), 1)
        self.assertEqual(mb._mirror_concurrency("http://127.0.0.1:5200/ws/2"), 4)

    def test_public_retry_is_paced_like_a_new_request(self) -> None:
        now = [0.0]
        starts: list[float] = []

        def monotonic() -> float:
            return now[0]

        def sleep(seconds: float) -> None:
            now[0] += seconds

        success = MagicMock()
        success.read.return_value = b"{}"
        success.__enter__ = lambda value: value
        success.__exit__ = MagicMock(return_value=False)

        pacing_url = f"{PUBLIC_MB_WS2_BASE}/artist/pacing-artist?fmt=json"

        def urlopen(request: object, **_kwargs: object) -> object:
            if not isinstance(request, urllib.request.Request) or request.full_url != pacing_url:
                return success
            starts.append(now[0])
            if len(starts) == 1:
                raise urllib.error.URLError("retry me")
            return success

        with mb._mb_mirror_semaphores_lock:
            previous_schedule = dict(mb._mb_public_next_request_at)
            mb._mb_public_next_request_at.clear()
        try:
            with patch.object(mb, "_monotonic", monotonic), patch.object(
                mb, "_sleep", sleep,
            ), patch("lib.mb_api.urllib.request.urlopen", side_effect=urlopen):
                mb._get(pacing_url)
        finally:
            with mb._mb_mirror_semaphores_lock:
                mb._mb_public_next_request_at.clear()
                mb._mb_public_next_request_at.update(previous_schedule)
        self.assertGreaterEqual(starts[1] - starts[0], 1.0)

    def test_catalogue_failure_returns_without_waiting_for_slow_siblings(self) -> None:
        # A one-page world leaves only the first direct/appearance requests in
        # flight. Keep the MB base patched until its local server has closed,
        # and block public URLs afterwards: failure-fast must not let a worker
        # resume against the restored production default.
        world = _NestedRecordingWorld(1, delay=0.1, fail_release_groups=True)
        public_urls: list[str] = []
        real_urlopen = urllib.request.urlopen

        def local_only(
            request: urllib.request.Request, *, timeout: float | None = None,
        ) -> object:
            url = request.full_url
            if url.startswith(PUBLIC_MB_WS2_BASE):
                public_urls.append(url)
                raise AssertionError(f"background worker escaped to public MB: {url}")
            return real_urlopen(request, timeout=timeout)

        with patch("lib.mb_api.urllib.request.urlopen", side_effect=local_only), patch.object(
            mb, "MB_API_BASE", "http://invalid-before-mirror/ws/2",
        ), patch("lib.mb_api._cache.memoize_meta", side_effect=_without_metadata_cache):
            with _Mirror(world) as api_base, patch.object(mb, "MB_API_BASE", api_base):
                started = time.monotonic()
                with self.assertRaises(mb.MusicBrainzArtistCatalogueIncomplete):
                    mb.get_artist_release_groups(ARTIST_ID)
                self.assertLess(time.monotonic() - started, 0.5)
            time.sleep(0.2)
        self.assertEqual(public_urls, [])


class TestArtistRecordingPaginationGenerated(unittest.TestCase):
    # Each example drives real urllib requests through a real threaded HTTP
    # server.  Thirty deterministic worlds cover page/segment boundaries in
    # the normal suite; the fuzz profile remains able to raise this budget.
    @settings(max_examples=30)
    @given(total=st.integers(min_value=1, max_value=260),
           short_page=st.integers(min_value=1, max_value=99))
    def test_nested_pages_preserve_the_canonical_direct_release_universe(
        self, total: int, short_page: int,
    ) -> None:
        world = _NestedRecordingWorld(total, short_page, after_write_delay=0.005)
        with _Mirror(world) as api_base, _ClientSlotProbe(api_base) as probe, patch.object(
            mb, "MB_API_BASE", api_base,
        ), patch(
            "lib.mb_api._cache.memoize_meta", side_effect=_without_metadata_cache,
        ):
            actual = [
                release.get("id", "")
                for release in mb.get_artist_releases_with_recordings(ARTIST_ID)
            ]
        assert_complete_order(world.ids, actual)
        assert_request_cap(probe.max_active)

    @settings(max_examples=8)
    @given(total=st.integers(min_value=1, max_value=180),
           short_page=st.integers(min_value=20, max_value=99))
    def test_short_catalogue_pages_conserve_every_direct_and_track_identity(
        self, total: int, short_page: int,
    ) -> None:
        world = _NestedRecordingWorld(total, catalogue_short_page=short_page)
        with _Mirror(world) as api_base, patch.object(mb, "MB_API_BASE", api_base), patch(
            "lib.mb_api._cache.memoize_meta", side_effect=_without_metadata_cache,
        ):
            rows = mb.get_artist_release_groups(ARTIST_ID)
        assert_catalogue_identity_count(total * 2, len(rows))

    @settings(max_examples=12)
    @given(total=st.integers(min_value=1, max_value=100))
    def test_counted_catalogue_generated_blank_identity_is_rejected(self, total: int) -> None:
        # This invariant is already decided by the first response.  Keep this
        # failure world single-page so a sibling cannot build page-two URLs
        # after the temporary local-MB base is restored; multipage coverage is
        # exercised by the conservation properties above.
        world = _NestedRecordingWorld(total, blank_catalogue_id=True)
        public_urls: list[str] = []
        real_urlopen = urllib.request.urlopen

        def local_only(
            request: urllib.request.Request, *, timeout: float | None = None,
        ) -> object:
            url = request.full_url
            if url.startswith(PUBLIC_MB_WS2_BASE):
                public_urls.append(url)
                raise AssertionError(f"background worker escaped to public MB: {url}")
            return real_urlopen(request, timeout=timeout)

        with patch("lib.mb_api.urllib.request.urlopen", side_effect=local_only), patch.object(
            mb, "MB_API_BASE", "http://invalid-before-mirror/ws/2",
        ), patch("lib.mb_api._cache.memoize_meta", side_effect=_without_metadata_cache):
            with _Mirror(world) as api_base, patch.object(
                mb, "MB_API_BASE", api_base,
            ), self.assertRaises(mb.MusicBrainzArtistCatalogueIncomplete):
                mb.get_artist_release_groups(ARTIST_ID)
            # Leave the guard active long enough for a wrongly retained
            # sibling to reveal a restored-base request.
            time.sleep(0.05)
        self.assertEqual(public_urls, [])


if __name__ == "__main__":
    unittest.main()
