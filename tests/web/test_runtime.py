"""Contract for the web process's one typed runtime value (#1313).

Two families live here:

* The installation lifecycle — ``runtime()`` fails closed, and
  ``install_runtime`` restores rather than clears, so nesting is honest.
  These are new with :class:`WebRuntime`; nothing guarded them while the
  same state was thirteen module globals.
* The handle-resolution contract moved from ``web/server.py``'s module
  functions: per-thread pipeline/Beets handles under a DSN, the injected
  shared handle without one, and the teardown that never closes what the
  caller owns. Previously pinned against ``srv._db()`` /
  ``srv._beets_db()`` in ``tests/web/test_server_threading.py``.
"""

import dataclasses
import os
import sys
import tempfile
import threading
import unittest

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import conftest  # noqa: F401 — sets TEST_DB_DSN for the per-thread tests

from lib.pipeline_db import PipelineDB
from tests.fakes import FakeBeetsDB, FakePipelineDB
from tests.helpers import make_web_runtime
from web.runtime import WebRuntime, install_runtime, runtime

TEST_DSN = os.environ.get("TEST_DB_DSN")


class TestRuntimeInstallation(unittest.TestCase):
    """``runtime()`` is late-bound, fail-closed, and properly nested."""

    def test_runtime_fails_closed_when_none_is_installed(self) -> None:
        # No install_runtime block is active here, and the module-level
        # slot starts empty — a route reached before boot wired one up is
        # a wiring bug, not a request to invent an empty runtime.
        with self.assertRaisesRegex(RuntimeError, "no WebRuntime installed"):
            runtime()

    def test_install_runtime_exposes_exactly_the_installed_value(self) -> None:
        wanted = WebRuntime(canonical_origin="https://music.ablz.au")
        with install_runtime(wanted) as yielded:
            self.assertIs(yielded, wanted)
            self.assertIs(runtime(), wanted)

    def test_install_runtime_restores_the_previous_value(self) -> None:
        outer = WebRuntime(canonical_origin="https://outer.example")
        inner = WebRuntime(canonical_origin="https://inner.example")
        with install_runtime(outer):
            with install_runtime(inner):
                self.assertIs(runtime(), inner)
            # Restored, not cleared: a nested override must not strand
            # the enclosing runtime.
            self.assertIs(runtime(), outer)
        with self.assertRaises(RuntimeError):
            runtime()

    def test_install_runtime_restores_after_an_exception(self) -> None:
        outer = WebRuntime(canonical_origin="https://outer.example")
        with install_runtime(outer):
            with (
                self.assertRaises(ValueError),
                install_runtime(WebRuntime()),
            ):
                raise ValueError("boom")
            self.assertIs(runtime(), outer)

    def test_runtime_is_visible_from_a_freshly_started_thread(self) -> None:
        """The slot is a module global, never a ContextVar.

        ``ThreadingHTTPServer`` starts each worker with a fresh empty
        context, so a ContextVar set at boot would be invisible to every
        request thread — this is the pin that would fail on that swap.
        """
        installed = WebRuntime(canonical_origin="https://music.ablz.au")
        seen: list[WebRuntime | Exception] = []

        def observe() -> None:
            try:
                seen.append(runtime())
            except Exception as exc:  # noqa: BLE001 - recorded, then asserted
                seen.append(exc)

        with install_runtime(installed):
            worker = threading.Thread(target=observe)
            worker.start()
            worker.join(timeout=10)
        self.assertEqual(seen, [installed])


class TestRuntimeIsFrozen(unittest.TestCase):
    """A route cannot rebind a collaborator; a derived runtime is clean."""

    def test_fields_cannot_be_rebound(self) -> None:
        rt = WebRuntime(beets_library_root="/mnt/virtio/Music/Beets")
        with self.assertRaises(dataclasses.FrozenInstanceError):
            # Written exactly as a future author would write it by
            # accident. Pyright already rejects it statically, which is
            # the first line of defence; the scoped ignore is what lets
            # this assert the runtime half — that `frozen=True` is still
            # on the dataclass, not merely that Pyright dislikes the line.
            rt.beets_library_root = (  # pyright: ignore[reportAttributeAccessIssue]
                "/somewhere/else"
            )

    def test_replace_mints_a_fresh_per_thread_handle_store(self) -> None:
        """A derived runtime never serves handles another runtime opened.

        ``_threads`` is ``init=False``, so ``replace`` re-runs its
        default_factory instead of carrying the original's thread-local
        across. Without that, a test varying one field would inherit the
        base runtime's live psycopg2/sqlite handles.
        """
        base = WebRuntime(db_dsn="postgresql://example")
        sentinel = FakePipelineDB()
        base._threads.db = sentinel

        derived = dataclasses.replace(base, db_dsn="postgresql://other")

        self.assertIsNot(derived._threads, base._threads)
        self.assertIsNone(getattr(derived._threads, "db", None))
        # The base keeps its own handle — replace() is not a move.
        self.assertIs(base._threads.db, sentinel)


class TestPipelineHandleResolution(unittest.TestCase):
    """DSN set ⟺ per-thread handles; DSN unset ⟺ the injected handle.

    The either/or is the seam ``scripts/web_dev_server.py`` depends on:
    it deliberately leaves the DSN unset so every request routes through
    its one read-only session instead of opening fresh per-thread
    connections that would skip the read-only flag.
    """

    def test_no_dsn_and_no_injected_handle_fails_closed(self) -> None:
        with self.assertRaisesRegex(RuntimeError, "Pipeline DB not connected"):
            WebRuntime().db()

    def test_injected_handle_is_shared_across_threads(self) -> None:
        sentinel = FakePipelineDB()
        rt = make_web_runtime(db=sentinel)
        seen: list[object] = []

        worker = threading.Thread(target=lambda: seen.append(rt.db()))
        worker.start()
        worker.join(timeout=10)

        self.assertEqual(seen, [sentinel])
        self.assertIs(rt.db(), sentinel)

    def test_db_available_and_or_none_agree_with_db(self) -> None:
        empty = WebRuntime()
        self.assertFalse(empty.db_available())
        self.assertIsNone(empty.db_or_none())

        injected = make_web_runtime(db=FakePipelineDB())
        self.assertTrue(injected.db_available())
        self.assertIs(injected.db_or_none(), injected.shared_db)

        with_dsn = WebRuntime(db_dsn="postgresql://example")
        self.assertTrue(with_dsn.db_available())

    def test_each_thread_gets_its_own_connection(self) -> None:
        rt = WebRuntime(db_dsn=TEST_DSN)
        handles: dict[str, object] = {}

        def grab(label: str) -> None:
            handle = rt.db()
            # Same thread, same handle (cached in the thread-local).
            assert rt.db() is handle
            handles[label] = handle
            handle.close()

        first = threading.Thread(target=grab, args=("first",))
        second = threading.Thread(target=grab, args=("second",))
        first.start(); second.start()
        first.join(timeout=10); second.join(timeout=10)

        self.assertIn("first", handles)
        self.assertIn("second", handles)
        self.assertIsNot(handles["first"], handles["second"])

    def test_background_session_owns_the_connection_it_opens(self) -> None:
        """Background work outliving a request never borrows its handle.

        Under a DSN the session opens its own connection and closes it
        at the end of the block, so the sweep's handle lifetime belongs
        to the runtime rather than to whoever entered the block.
        """
        rt = WebRuntime(db_dsn=TEST_DSN)
        self.addCleanup(rt.close_thread_handles)
        request_handle = rt.db()

        with rt.open_background_db() as background:
            self.assertIsNot(background, request_handle)
            self.assertFalse(background.conn.closed)

        self.assertTrue(background.conn.closed)
        # The request thread's own handle is untouched by the sweep's.
        self.assertFalse(request_handle.conn.closed)

    def test_background_session_closes_its_connection_after_a_raise(
        self,
    ) -> None:
        rt = WebRuntime(db_dsn=TEST_DSN)
        self.addCleanup(rt.close_thread_handles)
        opened: list[PipelineDB] = []

        with (
            self.assertRaises(ValueError),
            rt.open_background_db() as background,
        ):
            opened.append(background)
            raise ValueError("the sweep blew up")

        self.assertTrue(opened[0].conn.closed)

    def test_background_session_never_closes_an_injected_handle(self) -> None:
        """The dev server and the harness own the handle they inject.

        The bulk-triage sweep used to close whatever the runtime handed
        it, which under a DSN-less runtime is the one shared connection
        those two callers keep for every request — the same handle
        ``close_thread_handles`` already refuses to touch.
        """
        sentinel = FakePipelineDB()
        rt = make_web_runtime(db=sentinel)

        with rt.open_background_db() as background:
            self.assertIs(background, sentinel)

        self.assertFalse(sentinel.closed)
        self.assertEqual(sentinel.close_calls, 0)

    def test_background_session_fails_closed_with_nothing_configured(
        self,
    ) -> None:
        with (
            self.assertRaisesRegex(RuntimeError, "Pipeline DB not connected"),
            WebRuntime().open_background_db(),
        ):
            pass

    def test_drop_thread_db_drops_only_this_threads_handle(self) -> None:
        rt = WebRuntime(db_dsn=TEST_DSN)
        first = rt.db()
        rt.drop_thread_db()
        second = rt.db()
        try:
            self.assertIsNot(first, second)
        finally:
            rt.close_thread_handles()

    def test_drop_thread_db_is_inert_without_a_dsn(self) -> None:
        """The injected handle belongs to the dev server / harness."""
        sentinel = FakePipelineDB()
        rt = make_web_runtime(db=sentinel)
        rt.drop_thread_db()
        self.assertIs(rt.db(), sentinel)

    def test_close_thread_handles_closes_this_threads_handle(self) -> None:
        """#435: teardown releases psycopg2 rather than waiting on GC."""
        rt = WebRuntime(db_dsn=TEST_DSN)
        handle = rt.db()
        self.assertFalse(handle.conn.closed)

        rt.close_thread_handles()

        self.assertTrue(handle.conn.closed)
        self.assertIsNone(getattr(rt._threads, "db", None))

    def test_close_thread_handles_leaves_the_injected_handle_alone(
        self,
    ) -> None:
        sentinel = FakePipelineDB()
        rt = make_web_runtime(db=sentinel)
        rt.close_thread_handles()
        self.assertIs(rt.shared_db, sentinel)


class TestBeetsHandleResolution(unittest.TestCase):
    """The injected handle wins; otherwise the admitted pair opens one."""

    def test_no_injected_handle_and_no_path_is_not_configured(self) -> None:
        self.assertIsNone(WebRuntime().beets_db())

    def test_injected_handle_wins_over_the_admitted_pair(self) -> None:
        sentinel = FakeBeetsDB()
        with tempfile.NamedTemporaryFile() as library:
            rt = WebRuntime(
                beets_db_path=library.name,
                beets_library_root="/mnt/virtio/Music/Beets",
            )
            rt = make_web_runtime(rt, beets=sentinel)
            self.assertIs(rt.beets_db(), sentinel)

    def test_admitted_pair_opens_a_handle_carrying_the_library_root(
        self,
    ) -> None:
        with tempfile.NamedTemporaryFile() as library:
            rt = WebRuntime(
                beets_db_path=library.name,
                beets_library_root="/mnt/virtio/Music/Beets",
            )
            self.addCleanup(rt.close_thread_handles)

            handle = rt.beets_db()

            self.assertIsNotNone(handle)
            assert handle is not None
            self.assertEqual(handle.library_db_path, library.name)
            self.assertEqual(handle.library_root, "/mnt/virtio/Music/Beets")

    def test_a_missing_library_file_degrades_instead_of_raising(self) -> None:
        rt = WebRuntime(
            beets_db_path="/nonexistent/beets-library.db",
            beets_library_root="/nonexistent",
        )
        self.assertIsNone(rt.beets_db())

    def test_each_thread_opens_its_own_sqlite_handle(self) -> None:
        """sqlite3 connections are bound to their opening thread."""
        with tempfile.NamedTemporaryFile() as library:
            rt = WebRuntime(
                beets_db_path=library.name,
                beets_library_root="/mnt/virtio/Music/Beets",
            )
            handles: dict[str, object] = {}

            def grab(label: str) -> None:
                handle = rt.beets_db()
                assert rt.beets_db() is handle
                handles[label] = handle
                rt.close_thread_handles()

            first = threading.Thread(target=grab, args=("first",))
            second = threading.Thread(target=grab, args=("second",))
            first.start(); second.start()
            first.join(timeout=10); second.join(timeout=10)

            self.assertIsNot(handles["first"], handles["second"])

    def test_close_thread_handles_leaves_the_injected_beets_alone(
        self,
    ) -> None:
        sentinel = FakeBeetsDB()
        rt = make_web_runtime(beets=sentinel)
        rt.close_thread_handles()
        self.assertIs(rt.shared_beets, sentinel)


if __name__ == "__main__":
    unittest.main()
