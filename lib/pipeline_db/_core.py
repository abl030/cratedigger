"""PipelineDB core primitives: connection, _execute, advisory_lock, _atomic."""
from contextlib import contextmanager
from typing import Any, Iterator
import psycopg2
import psycopg2.extras

from lib.pipeline_db._shared import (
    DEFAULT_DSN,
    logger,
)



class _PipelineDBBase:
    """Typed shared-primitive contract every PipelineDB cluster mixin relies
    on. The real implementations live in :class:`_CoreMixin`; these stubs let
    each cluster mixin's ``self.conn`` / ``self._execute(...)`` /
    ``self._atomic()`` type-check without importing the composed class (which
    would be a circular import). At runtime the concrete ``PipelineDB`` MRO
    resolves every call to the real ``_CoreMixin`` / sibling-mixin method, so
    these bodies never execute.
    """

    dsn: str
    conn: Any

    def _ensure_conn(self) -> None: ...
    def _execute(self, sql: str, params: Any = ()) -> Any: ...
    def _atomic(self) -> Any: ...
    def advisory_lock(self, namespace: int, key: int) -> Any: ...
    # Sole cross-cluster call: the dashboard metrics aggregator reaches into
    # the search-plan cluster for readiness. Declared here so _DashboardMixin
    # type-checks; resolved to _SearchPlanMixin.get_search_plan_readiness at
    # runtime via the composed MRO.
    def get_search_plan_readiness(self, *args: Any, **kwargs: Any) -> Any: ...


class _CoreMixin(_PipelineDBBase):
    """Connection lifecycle + the shared transaction / advisory-lock
    primitives every other cluster mixin builds on."""
    def __init__(self, dsn=None):
        self.dsn = dsn or DEFAULT_DSN
        self.conn = self._connect()


    def _connect(self):
        conn = psycopg2.connect(
            self.dsn,
            connect_timeout=10,
            options="-c statement_timeout=30000"
                    " -c tcp_keepalives_idle=60"
                    " -c tcp_keepalives_interval=10"
                    " -c tcp_keepalives_count=5",
        )
        conn.autocommit = True
        return conn


    def _ensure_conn(self):
        """Reconnect if the connection is dead."""
        if self.conn.closed:
            self.conn = self._connect()


    def close(self):
        self.conn.close()


    def _execute(self, sql, params=()):
        self._ensure_conn()
        try:
            cur = self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            if params:
                cur.execute(sql, params)
            else:
                cur.execute(sql)
            return cur
        except (psycopg2.OperationalError, psycopg2.InterfaceError):
            # If libpq has just discovered the socket is dead (server-side
            # close while the connection sat idle between statements), the
            # error leaves ``conn.closed != 0``. Reconnect once and retry
            # the statement; autocommit semantics mean no in-flight
            # transaction state is being silently dropped. Statement-level
            # OperationalErrors (e.g. statement_timeout) keep the
            # connection open — re-raise those so the caller sees them.
            if not self.conn.closed:
                raise
            # The reconnect below returns a fresh ``autocommit=True``
            # connection (see ``_connect``). That heal is only safe OUTSIDE a
            # transaction. If we are mid-transaction (``autocommit=False`` —
            # i.e. called inside ``with self._atomic():``), silently swapping
            # to a fresh connection would drop the in-flight transaction's
            # partial writes onto a connection that doesn't know it is
            # supposed to be in a transaction. Re-raise so ``_atomic`` sees
            # the error and rolls back. (Latent today — no ``_atomic`` body
            # calls ``_execute`` — but nothing else enforces the invariant.)
            if not self.conn.autocommit:
                raise
            self.conn = self._connect()
            cur = self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            if params:
                cur.execute(sql, params)
            else:
                cur.execute(sql)
            return cur


    @contextmanager
    def advisory_lock(self, namespace: int, key: int) -> Iterator[bool]:
        """Try to acquire a session-level PostgreSQL advisory lock. Non-blocking.

        Yields ``True`` if acquired, ``False`` if another session already
        holds it. Always releases on ``__exit__`` when acquired.

        Used to serialise operations that must not run concurrently on the
        same ``(namespace, key)`` pair across different DB sessions — e.g.
        two ``pipeline-cli force-import`` invocations racing on the same
        ``request_id`` (issue #92). Advisory locks are reentrant within a
        single session, so this only protects against inter-session races.
        The web server's request thread and its background bulk-triage
        sweep thread (``web/triage_runner.py``) each hold their OWN
        connection/session — cross-thread serialisation in that process
        depends on never sharing a ``PipelineDB`` connection between
        threads (``web/server.py::_new_db``).

        See ``docs/advisory-locks.md`` for namespaces, keys, ordering,
        and call-site index.
        """
        self._ensure_conn()
        with self.conn.cursor() as cur:
            cur.execute("SELECT pg_try_advisory_lock(%s, %s)", (namespace, key))
            row = cur.fetchone()
        acquired = bool(row and row[0])
        try:
            yield acquired
        finally:
            if acquired:
                # Swallow unlock errors so they cannot mask the original
                # exception from the ``with`` body. PostgreSQL releases
                # session-level advisory locks on connection death anyway,
                # so a transient cursor/connection failure here cannot
                # leak the lock beyond the session.
                try:
                    with self.conn.cursor() as cur:
                        cur.execute(
                            "SELECT pg_advisory_unlock(%s, %s)",
                            (namespace, key),
                        )
                        cur.fetchone()
                except Exception:  # noqa: BLE001
                    logger.debug(
                        "advisory_unlock(%s, %s) failed; lock will be "
                        "released at session end",
                        namespace, key,
                    )


    @contextmanager
    def _atomic(self) -> Iterator[Any]:
        """Run a multi-row write in one explicit transaction.

        ``PipelineDB`` runs ``autocommit=True`` — one statement per implicit
        transaction (see ``_connect``). The handful of methods that must
        write several rows atomically (Replace / supersede, rescue-import,
        search-plan create / supersede / cursor-advance, the consumed-attempt
        log+advance, the YouTube enqueue / mapping upsert) temporarily flip to
        ``autocommit=False`` for the duration. This context manager is the one
        place that flip lives — it replaces ten hand-rolled copies of the same
        ``old_autocommit = … ; try/except rollback/raise ; finally restore``
        boilerplate, each of which risked forgetting the ``finally`` restore.

        Contract: the **caller commits explicitly** inside the block (every
        site already does, exactly once on its success path). On any exception
        the transaction is rolled back and the ORIGINAL exception is re-raised;
        the prior autocommit mode is restored on the way out. Because the body
        commits (success) or this rolls back (failure) before the ``finally``,
        autocommit is only ever restored with no transaction in flight —
        matching the original per-method ordering. A caller that needs to
        abort with no writes may ``rollback()`` and return early inside the
        block (``abandon_auto_import_request`` does this); that path is
        preserved unchanged.

        Dead-connection error paths (issue #395): if the connection died
        mid-transaction (or the caller's ``commit()`` raised), both the
        ``rollback()`` and the autocommit-restore raise a *secondary*
        ``InterfaceError``. Both are wrapped so the secondary error can never
        mask the original — the same shape as ``advisory_lock``'s unlock
        guard. A dead connection that fails to restore autocommit is harmless:
        the next ``_ensure_conn`` reconnects with a fresh ``autocommit=True``
        connection.

        Yields the live connection for convenience; callers continue to use
        ``self.conn`` directly.
        """
        self._ensure_conn()
        old_autocommit = self.conn.autocommit
        self.conn.autocommit = False  # explicit transaction for this block
        try:
            yield self.conn
        except Exception:
            # The connection may have died mid-transaction (or the caller's
            # ``commit()`` itself raised). ``rollback()`` on a dead connection
            # raises a *secondary* ``InterfaceError`` that would replace the
            # original error the caller must see. Swallow it — same shape as
            # ``advisory_lock``'s unlock guard.
            try:
                self.conn.rollback()  # discard partial writes
            except Exception:  # noqa: BLE001 — never mask the original error
                logger.debug(
                    "rollback failed during _atomic (connection likely "
                    "dead); original error propagates")
            raise  # re-raise the ORIGINAL exception to the caller
        finally:
            # Restoring autocommit on a dead connection ALSO raises
            # ``InterfaceError``, and from a ``finally`` block that would
            # re-mask the original error. Guard it the same way; the next
            # ``_ensure_conn`` reconnects with a fresh ``autocommit=True``
            # connection regardless of whether this restore landed.
            try:
                self.conn.autocommit = old_autocommit  # restore one-stmt mode
            except Exception:  # noqa: BLE001 — never mask the original error
                logger.debug(
                    "autocommit restore failed during _atomic (connection "
                    "likely dead); a fresh connection will be established on "
                    "next use")
