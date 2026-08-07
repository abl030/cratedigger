"""PipelineDB core primitives: connection, _execute, advisory_lock, _atomic."""
import select
import threading
import time
from collections.abc import Generator, Mapping, Sequence
from contextlib import AbstractContextManager, contextmanager
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Protocol

import psycopg2
import psycopg2.extras
from psycopg2.extensions import (
    POLL_OK,
    POLL_READ,
    POLL_WRITE,
    set_wait_callback,
)

from lib.import_execution import (
    CancellationToken,
    OwnerSessionIdentity,
    OwnerSessionProbe,
    OwnerSessionWatchdog,
)
from lib.pipeline_db._shared import (
    DEFAULT_DSN,
    logger,
)

if TYPE_CHECKING:
    from lib.pipeline_db.cleanup_journal import (
        ProcessingCleanupJournalRow,
        _CleanupCursor,
        _LockedCleanupScope,
    )


class ReadOnlyQueryCursor(Protocol):
    """Cursor surface exposed by the raw read-only query scope."""

    @property
    def description(self) -> Sequence[Sequence[object]] | None: ...

    def execute(self, sql: str) -> None: ...
    def fetchall(self) -> list[Mapping[str, object]]: ...
    def close(self) -> None: ...


class _PollingConnection(Protocol):
    """libpq surface used by the process-wide bounded wait callback."""

    OperationalError: type[Exception]

    def poll(self) -> int: ...
    def fileno(self) -> int: ...
    def cancel(self) -> None: ...


class OwnerSessionLost(RuntimeError):
    """The exact PostgreSQL session holding processor authority was lost."""


class AdvisoryLockSessionLost(RuntimeError):
    """The session which held an advisory-lock scope was lost.

    PostgreSQL releases session-scoped advisory locks when that backend dies.
    Reconnecting and replaying the next autocommit statement would therefore
    continue a supposedly locked protocol without its authority.
    """


@dataclass(frozen=True)
class _PinnedOwnerSession:
    connection: object
    identity: OwnerSessionIdentity
    token: CancellationToken
    io_lock: AbstractContextManager[object]


class _OwnerSessionProbeDeadline(TimeoutError):
    """A libpq wait exceeded the current thread's owner-probe deadline."""


_wait_state = threading.local()


def _pipeline_wait_callback(connection: _PollingConnection) -> None:
    """Drive synchronous libpq with an optional thread-local deadline."""
    while True:
        try:
            state = connection.poll()
            if state == POLL_OK:
                return
            deadline = getattr(_wait_state, "deadline", None)
            timeout = None
            if deadline is not None:
                timeout = deadline - time.monotonic()
                if timeout <= 0:
                    raise _OwnerSessionProbeDeadline(
                        "owner-session probe deadline exceeded"
                    )
            if state == POLL_READ:
                ready = select.select(
                    [connection.fileno()],
                    [],
                    [],
                    timeout,
                )
            elif state == POLL_WRITE:
                ready = select.select(
                    [],
                    [connection.fileno()],
                    [],
                    timeout,
                )
            else:
                raise connection.OperationalError(
                    f"bad libpq poll state: {state}"
                )
            if not any(ready):
                raise _OwnerSessionProbeDeadline(
                    "owner-session probe deadline exceeded"
                )
        except KeyboardInterrupt:
            connection.cancel()
            continue


@contextmanager
def _bounded_probe_wait(timeout_seconds: float) -> Generator[None]:
    previous = getattr(_wait_state, "deadline", None)
    _wait_state.deadline = time.monotonic() + timeout_seconds
    try:
        yield
    finally:
        _wait_state.deadline = previous



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
    def read_only_query_cursor(self) -> AbstractContextManager[ReadOnlyQueryCursor]: ...
    def _atomic(self) -> Any: ...
    def advisory_lock(self, namespace: int, key: int) -> Any: ...
    def _pin_owner_session(
        self,
        token: CancellationToken,
    ) -> AbstractContextManager[OwnerSessionIdentity]: ...
    def _probe_owner_session(
        self,
        identity: OwnerSessionIdentity,
    ) -> OwnerSessionProbe: ...
    def _lock_processing_cleanup_scope(
        self,
        cur: "_CleanupCursor",
        *,
        request_id: int,
    ) -> "_LockedCleanupScope": ...
    @staticmethod
    def _require_exact_processing_owner(
        scope: "_LockedCleanupScope",
        *,
        request_id: int,
        job_id: int,
    ) -> None: ...
    def _get_processing_cleanup_journal_locked(
        self,
        *,
        request_id: int,
        job_id: int,
        scope: "_LockedCleanupScope",
    ) -> "ProcessingCleanupJournalRow | None": ...
    # Cross-cluster calls: declared here so the calling mixin type-checks;
    # resolved to the owning sibling mixin at runtime via the composed MRO.
    # - dashboard metrics aggregator -> search-plan cluster readiness:
    def get_search_plan_readiness(self, *args: Any, **kwargs: Any) -> Any: ...
    # - terminal-outcome cooldown -> the one streak evaluator in _MiscMixin
    #   (decision 20 follow-up: pending and direct paths share it):
    def _cooldown_streak_verdict(
        self, *args: Any, **kwargs: Any
    ) -> Any: ...


class _CoreMixin(_PipelineDBBase):
    """Connection lifecycle + the shared transaction / advisory-lock
    primitives every other cluster mixin builds on."""
    def __init__(self, dsn: str | None = None) -> None:
        self.dsn = dsn or DEFAULT_DSN
        self.conn = self._connect()
        self._owner_session_pin: _PinnedOwnerSession | None = None
        self._advisory_lock_connection: object | None = None


    def _connect(self):
        # One process-global callback preserves ordinary blocking behavior,
        # while owner watchdog threads opt into a thread-local hard deadline.
        set_wait_callback(_pipeline_wait_callback)
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
        pin = self._owner_session_pin
        if pin is not None:
            if pin.token.cancelled:
                raise OwnerSessionLost("pinned owner execution is cancelled")
            if self.conn is not pin.connection:
                pin.token.cancel("owner_session_connection_replaced")
                raise OwnerSessionLost(
                    "pinned owner-session connection was replaced"
                )
            if self.conn.closed:
                pin.token.cancel("owner_session_closed")
                raise OwnerSessionLost("pinned owner session is closed")
            return
        lock_connection = getattr(self, "_advisory_lock_connection", None)
        if lock_connection is not None:
            if self.conn is not lock_connection:
                raise AdvisoryLockSessionLost(
                    "advisory-lock session connection was replaced"
                )
            if self.conn.closed:
                raise AdvisoryLockSessionLost("advisory-lock session is closed")
            return
        if self.conn.closed:
            self.conn = self._connect()


    def close(self) -> None:
        if self._owner_session_pin is not None:
            self._owner_session_pin.token.cancel("owner_session_closed")
        self.conn.close()


    @contextmanager
    def _owner_session_io(self) -> Generator[None]:
        pin = self._owner_session_pin
        if pin is None:
            yield
            return
        with pin.io_lock:
            yield


    def _execute(
        self,
        sql: str,
        params: Sequence[object] | Mapping[str, object] = (),
    ):
        pin = self._owner_session_pin
        if pin is None:
            return self._execute_locked(sql, params)
        with pin.io_lock:
            return self._execute_locked(sql, params)


    def _execute_locked(
        self,
        sql: str,
        params: Sequence[object] | Mapping[str, object] = (),
    ):
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
            if self._owner_session_pin is not None:
                self._owner_session_pin.token.cancel(
                    "owner_session_connection_lost"
                )
                raise OwnerSessionLost(
                    "pinned owner session was lost; statement was not replayed"
                )
            if getattr(self, "_advisory_lock_connection", None) is not None:
                raise AdvisoryLockSessionLost(
                    "advisory-lock session was lost; statement was not replayed"
                )
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
    def _advisory_lock_session(self) -> Generator[None]:
        """Keep a short advisory-lock protocol on exactly one PG session.

        This intentionally does not add a transaction or watchdog.  It only
        fences the ordinary autocommit reconnect/replay convenience while one
        or more session advisory locks are held.  Nested locks share the
        outer connection; owner-pinned execution already provides the
        stronger equivalent guarantee.
        """
        if self._owner_session_pin is not None:
            yield
            return
        existing = getattr(self, "_advisory_lock_connection", None)
        if existing is not None:
            self._ensure_conn()
            yield
            return

        self._ensure_conn()
        self._advisory_lock_connection = self.conn
        try:
            yield
        finally:
            self._advisory_lock_connection = None


    @contextmanager
    def _pin_owner_session(
        self,
        token: CancellationToken,
    ) -> Generator[OwnerSessionIdentity]:
        """Pin one live session and disable reconnect/replay for its scope."""
        if self._owner_session_pin is not None:
            raise RuntimeError("owner session is already pinned")
        token.raise_if_cancelled()
        self._ensure_conn()
        connection = self.conn
        if not connection.autocommit:
            raise RuntimeError(
                "owner session must be pinned before entering a transaction"
            )
        try:
            with _bounded_probe_wait(0.75), connection.cursor() as cur:
                cur.execute("SELECT pg_backend_pid()")
                row = cur.fetchone()
        except _OwnerSessionProbeDeadline as exc:
            token.cancel("owner_session_pin_deadline")
            connection.close()
            raise OwnerSessionLost(
                "owner session pin exceeded its deadline"
            ) from exc
        except (psycopg2.OperationalError, psycopg2.InterfaceError) as exc:
            token.cancel("owner_session_pin_failed")
            raise OwnerSessionLost("could not pin owner session") from exc
        if row is None:
            token.cancel("owner_session_pin_failed")
            raise OwnerSessionLost("owner session returned no backend identity")
        identity = OwnerSessionIdentity(
            connection_object_id=id(connection),
            backend_pid=int(row[0]),
        )
        pin = _PinnedOwnerSession(
            connection,
            identity,
            token,
            threading.RLock(),
        )
        self._owner_session_pin = pin
        watchdog = OwnerSessionWatchdog(
            token,
            lambda deadline: self._probe_owner_session(
                identity,
                deadline_seconds=deadline,
            ).live,
            probe_interval=0.75,
        )
        try:
            watchdog.start()
        except Exception as exc:
            token.cancel("owner_session_watchdog_start_failed")
            if self._owner_session_pin is pin:
                self._owner_session_pin = None
            connection.close()
            raise OwnerSessionLost(
                "owner-session watchdog could not start"
            ) from exc
        try:
            yield identity
        finally:
            try:
                watchdog.stop()
            finally:
                if self._owner_session_pin is pin:
                    self._owner_session_pin = None


    def _probe_owner_session(
        self,
        identity: OwnerSessionIdentity,
        *,
        deadline_seconds: float = 0.75,
    ) -> OwnerSessionProbe:
        """Probe the exact pinned backend without reconnecting or replaying."""
        if not 0 < deadline_seconds <= 1.0:
            raise ValueError(
                "owner-session probe deadline must be in (0, 1.0]"
            )
        pin = self._owner_session_pin
        if pin is None:
            return OwnerSessionProbe(
                False, "scope_not_pinned", identity, None
            )
        with pin.io_lock:
            if pin.token.cancelled:
                return OwnerSessionProbe(
                    False, "execution_cancelled", identity, None
                )
            if pin.identity != identity:
                return OwnerSessionProbe(
                    False, "identity_mismatch", identity, None
                )
            if self.conn is not pin.connection:
                pin.token.cancel("owner_session_connection_replaced")
                return OwnerSessionProbe(
                    False, "connection_replaced", identity, None
                )
            if self.conn.closed:
                pin.token.cancel("owner_session_closed")
                return OwnerSessionProbe(
                    False, "connection_closed", identity, None
                )
            milliseconds = max(1, int(deadline_seconds * 1000))
            sql = (
                f"SET LOCAL statement_timeout = {milliseconds}; "
                "SELECT pg_backend_pid()"
            )
            try:
                with _bounded_probe_wait(
                    deadline_seconds,
                ), self.conn.cursor() as cur:
                    cur.execute(sql)
                    row = cur.fetchone()
            except _OwnerSessionProbeDeadline:
                pin.token.cancel("owner_session_probe_deadline")
                # The query may still be executing server-side. Closing the
                # exact session is the only safe way to abandon it without a
                # probe thread retaining access or a later statement replay.
                self.conn.close()
                return OwnerSessionProbe(
                    False, "probe_deadline", identity, None
                )
            except psycopg2.Error as exc:
                pin.token.cancel(
                    f"owner_session_probe_failed:{type(exc).__name__}"
                )
                return OwnerSessionProbe(
                    False, "probe_failed", identity, None
                )
            if row is None:
                pin.token.cancel("owner_session_probe_empty")
                return OwnerSessionProbe(
                    False, "probe_empty", identity, None
                )
            observed_backend_pid = int(row[0])
            if observed_backend_pid != identity.backend_pid:
                pin.token.cancel("owner_session_backend_changed")
                return OwnerSessionProbe(
                    False,
                    "backend_changed",
                    identity,
                    observed_backend_pid,
                )
            return OwnerSessionProbe(
                True, "exact_backend", identity, observed_backend_pid
            )


    @contextmanager
    def read_only_query_cursor(self) -> Generator[ReadOnlyQueryCursor]:
        """Yield a cursor in one non-retrying read-only transaction.

        Raw operator diagnostics need a transaction-level safety boundary.
        Capture the live connection once, then execute both ``BEGIN ... READ
        ONLY`` and caller SQL through that same connection.  In particular,
        do not use :meth:`_execute` after ``BEGIN``: its healthy autocommit
        reconnect/retry policy is correct for ordinary one-statement work but
        could replay caller SQL on a fresh writable session after connection
        death.

        Cleanup is best-effort.  A dead connection cannot retain a
        transaction, and its rollback failure must never hide the diagnostic
        error which caused the scope to exit.
        """
        self._ensure_conn()
        conn = self.conn
        cur: ReadOnlyQueryCursor = conn.cursor(
            cursor_factory=psycopg2.extras.RealDictCursor,
        )
        try:
            cur.execute("BEGIN TRANSACTION READ ONLY")
            # The raw-SQL lexer intentionally follows PostgreSQL's standard
            # string rules. Pin the transaction-local server setting so a
            # caller's session configuration cannot make its interpretation
            # drift from the lexical safety boundary.
            cur.execute("SET LOCAL standard_conforming_strings = on")
            yield cur
        finally:
            try:
                conn.rollback()
            except (psycopg2.OperationalError, psycopg2.InterfaceError):
                logger.debug(
                    "read-only query rollback failed; connection is already dead")
            try:
                cur.close()
            except (psycopg2.OperationalError, psycopg2.InterfaceError):
                logger.debug(
                    "read-only query cursor close failed; connection is already dead")


    @contextmanager
    def advisory_lock(self, namespace: int, key: int) -> Generator[bool]:
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
        with self._advisory_lock_session():
            try:
                with self._owner_session_io():
                    self._ensure_conn()
                    with self.conn.cursor() as cur:
                        cur.execute(
                            "SELECT pg_try_advisory_lock(%s, %s)",
                            (namespace, key),
                        )
                        row = cur.fetchone()
            except (psycopg2.OperationalError, psycopg2.InterfaceError) as exc:
                raise AdvisoryLockSessionLost(
                    "advisory-lock session was lost before acquisition"
                ) from exc
            acquired = bool(row and row[0])
            try:
                yield acquired
            finally:
                # Swallow unlock errors so they cannot mask the original
                # exception from the ``with`` body. PostgreSQL releases
                # session-level advisory locks on connection death anyway,
                # so a transient cursor/connection failure here cannot
                # leak the lock beyond the session.
                if acquired:
                    try:
                        with self._owner_session_io(), self.conn.cursor() as cur:
                            cur.execute(
                                "SELECT pg_advisory_unlock(%s, %s)",
                                (namespace, key),
                            )
                            cur.fetchone()
                    except Exception:  # noqa: BLE001 - unlock cannot mask body
                        if self._owner_session_pin is not None:
                            self._owner_session_pin.token.cancel(
                                "owner_session_unlock_failed"
                            )
                        logger.debug(
                            "advisory_unlock(%s, %s) failed; lock will be "
                            "released at session end",
                            namespace, key,
                        )


    @contextmanager
    def _atomic(self) -> Generator[Any]:
        """Serialize a pinned transaction against its same-session probe."""
        with self._owner_session_io(), self._atomic_locked() as connection:
            yield connection


    @contextmanager
    def _atomic_locked(self) -> Generator[object]:
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
        site already does, exactly once on its success path). On any ordinary
        exception the transaction is rolled back and the ORIGINAL exception is
        re-raised; the prior autocommit mode is restored on the way out. A raw
        database error on a dead advisory-lock session instead becomes
        ``AdvisoryLockSessionLost``; it is never retried on a replacement
        connection. Because the body
        commits (success) or this rolls back (failure) before the ``finally``,
        autocommit is only ever restored with no transaction in flight —
        matching the original per-method ordering. A caller that needs to
        abort with no writes may ``rollback()`` and return early inside the
        block; that path is preserved unchanged.

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
        try:
            self.conn.autocommit = False  # explicit transaction for this block
        except (psycopg2.OperationalError, psycopg2.InterfaceError) as exc:
            if self._advisory_lock_session_lost():
                raise AdvisoryLockSessionLost(
                    "advisory-lock transaction session was lost"
                ) from exc
            raise
        body_error: Exception | None = None
        try:
            yield self.conn
        except Exception as exc:
            body_error = exc
            pin = self._owner_session_pin
            if pin is not None and (
                self.conn is not pin.connection or self.conn.closed
            ):
                pin.token.cancel("owner_session_transaction_lost")
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
            if (
                isinstance(exc, (psycopg2.OperationalError, psycopg2.InterfaceError))
                and self._advisory_lock_session_lost()
            ):
                raise AdvisoryLockSessionLost(
                    "advisory-lock transaction session was lost; "
                    "statement was not replayed"
                ) from exc
            raise  # re-raise the ORIGINAL exception to the caller
        finally:
            # Restoring autocommit on a dead connection ALSO raises
            # ``InterfaceError``, and from a ``finally`` block that would
            # re-mask the original error. Guard it the same way; the next
            # ``_ensure_conn`` reconnects with a fresh ``autocommit=True``
            # connection regardless of whether this restore landed.
            try:
                self.conn.autocommit = old_autocommit  # restore one-stmt mode
            except Exception as exc:
                if self._owner_session_pin is not None:
                    self._owner_session_pin.token.cancel(
                        "owner_session_transaction_lost"
                    )
                if body_error is None and self._advisory_lock_session_lost():
                    raise AdvisoryLockSessionLost(
                        "advisory-lock transaction session was lost; "
                        "autocommit was not restored"
                    ) from exc
                logger.debug(
                    "autocommit restore failed during _atomic (connection "
                    "likely dead); a fresh connection will be established on "
                    "next use")

    def _advisory_lock_session_lost(self) -> bool:
        connection = getattr(self, "_advisory_lock_connection", None)
        return connection is not None and (
            self.conn is not connection or bool(self.conn.closed)
        )
