"""The web process's one typed runtime value.

``web/server.py`` used to *be* the runtime: thirteen mutable module
globals that every route module reached by string-addressed attribute
lookup through a deferred-import shim, and that every test rebound by
name. Nothing about that arrangement was checkable — a renamed global
broke at request time, and the only way to describe correct construction
was prose.

This module holds the value instead. :class:`WebRuntime` is frozen: it
carries the process's admitted configuration, the optional injected
handles the development server and the test harness supply, and the
per-thread pipeline/Beets handles production opens on first use. Routes
read it through :func:`runtime`; boot and tests install one through
:func:`install_runtime`.

The import direction is what lets ``web/routes/_server_access.py`` go
away. ``web/server.py`` imports every route module to build
``ALL_ROUTES``, so a route module could never import ``web.server`` at
module scope. This module imports only ``lib.*`` and ``web.overlay``
(itself a leaf), so routes import it directly and the cycle is gone
rather than deferred.
"""

from __future__ import annotations

import logging
import threading
from collections.abc import Generator
from contextlib import contextmanager
from dataclasses import dataclass, field

from lib.beets_db import BeetsDB, open_beets_db
from lib.beets_delete import BeetsDeleteFn
from lib.convergence_service import ConvergenceSignal
from lib.destructive_release_service import DeleteNotifyFn
from lib.pipeline_db import PipelineDB
from lib.pipeline_db.rows import ArtistRequestRow
from web import overlay as _overlay

# The same logger name ``web/server.py`` uses: these messages are part of
# one web process's operator-facing record, not a second stream.
log = logging.getLogger("cratedigger-web")


@dataclass(frozen=True)
class WebRuntime:
    """One web process's collaborators and admitted configuration.

    Every field is optional because two callers legitimately supply
    different subsets: production sets ``db_dsn`` and the admitted Beets
    pair and opens per-thread handles from them, while the development
    server and the test harness inject ``shared_db`` / ``shared_beets``
    and leave the DSN unset. The two are distinguished by value, not by
    prose — :meth:`db` and :meth:`beets_db` read both shapes.

    Frozen so a route cannot rebind a collaborator mid-request. Tests
    vary one field with :func:`dataclasses.replace`, which mints a fresh
    per-thread handle store (``_threads`` is ``init=False``), so a
    derived runtime never serves handles another runtime opened.
    """

    #: Exact public origin browser mutations must claim. ``None`` rejects
    #: every non-liveness request — the fail-closed pre-startup state.
    canonical_origin: str | None = None
    #: Render and log the explicit insecure-authentication warning.
    insecure_mode: bool = False
    #: Production pipeline DSN. Set means "open one handle per thread".
    db_dsn: str | None = None
    #: Injected shared pipeline handle (dev server, test harness). The
    #: caller owns its thread-safety; :meth:`close_thread_handles` never
    #: touches it.
    shared_db: PipelineDB | None = None
    #: The admitted Beets library database, installed at startup.
    beets_db_path: str | None = None
    #: The admitted Beets library directory, installed alongside the pair.
    beets_library_root: str = ""
    #: Injected shared Beets handle (dev server, test harness); wins over
    #: the admitted pair and is likewise never closed here.
    shared_beets: BeetsDB | None = None
    #: Resolved daily retag-divergence census snapshot file. ``None``
    #: reads exactly like a missing snapshot.
    retag_census_snapshot_path: str | None = None
    #: Resolved daily library-completeness snapshot file.
    library_completeness_snapshot_path: str | None = None
    #: Test/dev seams for the pinned destructive operation. Production
    #: leaves both unset and the service selects its real implementations.
    beets_delete_fn: BeetsDeleteFn | None = None
    delete_notify_fn: DeleteNotifyFn | None = None

    #: Per-thread handles. Threads are mostly long-lived: the handler
    #: speaks HTTP/1.1 keep-alive, so a browser's persistent connections
    #: each pin one worker thread (and its handles) across many requests.
    #: One-shot clients (curl, the importer's notify hooks) cost one
    #: connect/teardown each — fine at single-operator scale. A
    #: ``PipelineDB`` is not safe to share across threads (``_ensure_conn``
    #: replaces ``self.conn`` in place, under whichever caller happens to
    #: be mid-statement, and one handle is one session, so both threads
    #: would sit inside the same session-level advisory locks), and a
    #: sqlite3 handle is bound to its opening thread outright
    #: (``check_same_thread``). That is why this exists at all. Note
    #: psycopg2 itself reports ``threadsafety == 2``: sharing a raw
    #: connection is allowed and it is the wrapper's semantics, not
    #: libpq's, that make it wrong here.
    _threads: threading.local = field(
        init=False,
        default_factory=threading.local,
        compare=False,
        repr=False,
    )

    # ── Pipeline DB ──────────────────────────────────────────────────

    def db(self) -> PipelineDB:
        """This thread's pipeline DB, opening it on first use."""
        if not self.db_dsn:
            if self.shared_db is None:
                raise RuntimeError("Pipeline DB not connected")
            return self.shared_db
        handle = getattr(self._threads, "db", None)
        if handle is None:
            handle = PipelineDB(self.db_dsn)
            self._threads.db = handle
        return handle

    @contextmanager
    def open_background_db(self) -> Generator[PipelineDB]:
        """A scoped pipeline handle for work that outlives the request.

        The bulk-triage sweep runs on its own thread for minutes after
        its POST has answered 202, so it must not borrow the request
        thread's handle. One handle is one PostgreSQL session: the two
        threads would share session-level advisory locks and whatever
        transaction either one opened, and
        :meth:`close_thread_handles` would release the connection out
        from under the sweep the moment the request's own connection
        ended.

        Under a DSN this opens a connection nothing else holds and
        closes it on the way out. A failing close is swallowed rather
        than raised at the caller, whose own work already succeeded or
        failed on its own terms — the same best-effort teardown
        :meth:`close_thread_handles` performs, though that one swallows
        silently and this one logs.

        With no DSN it yields the injected shared handle and leaves it
        OPEN. The dev server and the test harness own that one, so the
        rule teardown already follows applies here too — the runtime
        closes only what the runtime opened. The separation above is
        genuinely absent in that case, one session serving both threads,
        which is accepted for those two callers and never reached in
        production, where the DSN is always set.

        A context manager rather than a method returning a handle, so
        ownership is structural instead of conventional: nothing can
        pass this where :meth:`db` belongs, or the reverse, without
        Pyright saying so.
        """
        if not self.db_dsn:
            yield self.db()
            return
        handle = PipelineDB(self.db_dsn)
        try:
            yield handle
        finally:
            try:
                handle.close()
            except Exception:
                # Best-effort, like close_thread_handles: the caller's
                # own work already succeeded or failed on its own terms.
                log.exception("Background pipeline DB handle failed to close")

    def db_available(self) -> bool:
        """True when :meth:`db` can return a handle."""
        return bool(self.db_dsn) or self.shared_db is not None

    def db_or_none(self) -> PipelineDB | None:
        """This thread's pipeline DB, or None when no DB is configured."""
        return self.db() if self.db_available() else None

    def drop_thread_db(self) -> None:
        """Drop this thread's pipeline-DB handle so the next :meth:`db`
        call opens a fresh connection.

        Only request-handler threads call this (from the do_GET/do_POST
        catch-alls), so the thread-local is the right scope; other
        threads' healthy connections are left alone. ``PipelineDB`` also
        self-heals via ``_ensure_conn``, so this is belt-and-braces for
        errors that escape it.
        """
        if not self.db_dsn:
            return
        handle = getattr(self._threads, "db", None)
        if handle is not None:
            try:
                handle.conn.close()
            except Exception:  # noqa: BLE001, S110 - best-effort boundary must not mask primary work
                pass
            self._threads.db = None
            log.info(
                "Dropped this thread's pipeline DB handle; "
                "next request reconnects",
            )

    # ── Beets ────────────────────────────────────────────────────────

    def beets_db(self) -> BeetsDB | None:
        """This thread's BeetsDB, or None if not configured.

        sqlite3 connections are bound to their opening thread
        (``check_same_thread``), so each worker opens its own read-only
        handle on first use. An injected ``shared_beets`` wins.
        """
        if self.shared_beets is not None:
            return self.shared_beets
        # Startup installs the exact admitted pair. An absent pair is
        # deliberate dependency injection (tests/dev) and must never
        # trigger a second runtime config read after the one startup
        # admission.
        if self.beets_db_path is None:
            return None
        handle = getattr(self._threads, "beets", None)
        if handle is None:
            try:
                handle = open_beets_db(
                    db_path=self.beets_db_path,
                    library_root=self.beets_library_root,
                )
            except FileNotFoundError:
                return None
            self._threads.beets = handle
        return handle

    def close_thread_handles(self) -> None:
        """Close and drop this thread's DB handles.

        Called from ``Handler.finish()`` — under either threaded HTTP
        server one thread serves one connection, so connection-close IS
        thread-death and this releases the psycopg2/sqlite handles
        deterministically instead of waiting on GC (#435). Injected
        shared handles (tests, dev server) are never touched.
        """
        handle = getattr(self._threads, "db", None)
        if handle is not None:
            try:
                handle.close()
            except Exception:  # noqa: BLE001, S110 - best-effort boundary must not mask primary work
                pass
            self._threads.db = None
        beets_handle = getattr(self._threads, "beets", None)
        if beets_handle is not None:
            try:
                beets_handle.close()
            except Exception:  # noqa: BLE001, S110 - best-effort boundary must not mask primary work
                pass
            self._threads.beets = None

    # ── Overlay adapters ─────────────────────────────────────────────
    #
    # The overlay/domain logic lives in web/overlay.py with explicit DB
    # parameters (#432). Binding this runtime's handles into those
    # parameters is exactly the composition this value exists to own, so
    # the adapters are methods here rather than re-bound module globals.

    def check_beets_library(self, mbids: list[str] | list[object]) -> set[str]:
        return _overlay.check_beets_library(self.beets_db(), mbids)

    def check_beets_library_detail(
        self,
        mbids: list[str] | list[object],
    ) -> dict[str, dict[str, object]]:
        return _overlay.check_beets_library_detail(self.beets_db(), mbids)

    def get_library_artist(
        self,
        artist_name: str,
        mb_artist_id: str = "",
    ) -> list[dict[str, object]]:
        return _overlay.get_library_artist(
            self.beets_db(), artist_name, mb_artist_id,
        )

    def get_library_releases(
        self,
        release_ids: list[str],
    ) -> list[dict[str, object]]:
        return _overlay.get_library_releases(self.beets_db(), release_ids)

    def check_pipeline(
        self,
        mbids: list[str] | list[object],
    ) -> dict[str, dict[str, object]]:
        return _overlay.check_pipeline(self.db_or_none(), mbids)

    def get_convergence_signals(
        self,
        request_ids: list[int],
    ) -> dict[int, ConvergenceSignal]:
        """Batch the observational signal for browse release overlays."""
        handle = self.db_or_none()
        if handle is None or not request_ids:
            return {}
        return handle.get_convergence_signals(request_ids)

    def list_artist_requests(
        self,
        artist_name: str,
        mb_artist_id: str = "",
    ) -> list[ArtistRequestRow]:
        """One artist's request rows, for the rg-row badge overlay (#575)."""
        handle = self.db_or_none()
        if handle is None or not artist_name:
            return []
        return handle.list_requests_by_artist(artist_name, mb_artist_id)


_installed: WebRuntime | None = None


def runtime() -> WebRuntime:
    """The installed runtime.

    Fails closed rather than manufacturing a default: a route reached
    before boot installed one is a wiring bug, and an empty runtime
    would answer it with a plausible-looking 500 instead.
    """
    if _installed is None:
        raise RuntimeError("no WebRuntime installed")
    return _installed


@contextmanager
def install_runtime(new: WebRuntime) -> Generator[WebRuntime]:
    """Install ``new`` for the duration of the block, then restore.

    One way in, for boot and tests alike. Restoring the previous value
    rather than clearing it keeps nesting honest, so a test that varies
    one field inside another test's runtime cannot strand its override.

    The slot is a plain module global, deliberately not a
    :class:`~contextvars.ContextVar`: ``ThreadingHTTPServer`` starts each
    worker with a fresh empty context, so a context variable set at boot
    would be invisible to every request thread.

    The save/restore is not itself thread-safe — two threads installing
    concurrently would interleave their saved values and corrupt the
    chain. Inert as used: production installs exactly once, on the main
    thread, before the listener exists, and tests install from the thread
    running the test. Request threads only ever *read* the slot.
    """
    global _installed

    previous = _installed
    _installed = new
    try:
        yield new
    finally:
        _installed = previous
