"""FakePipelineDB _core cluster — mirrors ``lib/pipeline_db/_core.py``.

Connection lifecycle, raw-execute recording, and advisory locks.
"""
from __future__ import annotations

from collections.abc import (
    Callable,
    Generator,
    Iterator,
    Mapping,
    Sequence,
)
from contextlib import contextmanager
from typing import (
    Any,
)

from lib.import_execution import (
    CancellationToken,
    OwnerSessionIdentity,
    OwnerSessionProbe,
)
from lib.pipeline_db._core import OwnerSessionLost, ReadOnlyQueryCursor
from tests.fakes.pipeline_db._base import _FakePipelineDBBase


class _FakeReadOnlyQueryCursor:
    """Typed cursor adapter for FakePipelineDB's raw-query context."""

    def __init__(self, db: _FakeCoreMixin) -> None:
        self._db = db
        self._query_cursor: ReadOnlyQueryCursor | None = None

    @property
    def description(self) -> Sequence[Sequence[object]] | None:
        return self._cursor().description

    def execute(self, sql: str) -> None:
        self._query_cursor = self._db._execute(sql)

    def fetchall(self) -> list[Mapping[str, object]]:
        return self._cursor().fetchall()

    def close(self) -> None:
        return None

    def _cursor(self) -> ReadOnlyQueryCursor:
        if self._query_cursor is None:
            raise AssertionError("read-only query cursor has not executed SQL")
        return self._query_cursor


class _FakeCoreMixin(_FakePipelineDBBase):
    """Connection lifecycle, raw-execute recording, and advisory locks."""

    def queue_execute_results(self, *results: Any) -> None:
        """Register a deterministic cursor sequence for ``_execute`` calls.

        Each subsequent ``_execute(sql, params)`` call pops the next entry:
        - If the entry is an ``Exception`` instance/subclass, it is raised
          (so tests can simulate ``psycopg2.ProgrammingError`` etc.).
        - Otherwise the entry is returned as the cursor.

        Replaces ``MagicMock(); db._execute.side_effect = [c1, c2, c3]``.
        Inspect call args via ``db.execute_calls``.
        """
        self._execute_queue = list(results)

    def _execute(self, sql: str, params: tuple[Any, ...] = ()) -> Any:
        """Stand-in for ``PipelineDB._execute``.

        Records the call and returns the next entry from
        ``queue_execute_results``. If the queue is empty, returns
        ``self._execute_default`` (an empty :class:`FakeCursor` —
        mirrors production's "query ran, zero rows" contract)."""
        self.execute_calls.append((sql, params))
        if not self._execute_queue:
            return self._execute_default
        entry = self._execute_queue.pop(0)
        if isinstance(entry, Exception):
            raise entry
        return entry

    @contextmanager
    def read_only_query_cursor(self) -> Generator[ReadOnlyQueryCursor]:
        """Fake the pinned read-only query scope used by ``pipeline-cli``.

        The production guarantee (no reconnect/retry after BEGIN) belongs to
        its real-connection regression test. This fake preserves the visible
        begin/setup/query/rollback sequence for command-output tests.
        """
        self._execute("BEGIN TRANSACTION READ ONLY")
        self._execute("SET LOCAL standard_conforming_strings = on")
        try:
            yield _FakeReadOnlyQueryCursor(self)
        finally:
            try:
                self._execute("ROLLBACK")
            except Exception as exc:
                # Match the production context manager: a dead connection
                # cannot retain the transaction, and cleanup must not replace
                # successful query output (or the caller's original error).
                import psycopg2

                if not isinstance(
                    exc, (psycopg2.OperationalError, psycopg2.InterfaceError),
                ):
                    raise

    def set_cooldown_result(self, result: bool | Callable[[str], bool]) -> None:
        """Configure what check_and_apply_cooldown returns.

        Pass a bool for a fixed result, or a callable(username) -> bool
        for per-user conditional results.
        """
        self._cooldown_result = result

    def set_update_download_state_error(
        self, request_id: int, error: Exception,
    ) -> None:
        """Make the witnessed download-state writer raise for one request.

        Mirrors a production psycopg2 error at the
        UPDATE: the call is recorded but the row is never mutated. Same
        targeted-seam style as ``set_cooldown_result`` /
        ``FakeSlskdUsers.set_directory_error``. Persistent for the
        fake's lifetime (a one-shot harvest only calls once).
        """
        self._update_download_state_errors[request_id] = error

    def set_advisory_lock_result(
        self, result: bool | Callable[[int, int], bool],
    ) -> None:
        """Configure what advisory_lock yields.

        Pass a bool for a fixed result across every (namespace, key), or
        a callable (namespace, key) -> bool for per-lock answers. The
        callable form is needed for issue #133 where one test scenario
        holds the request-lock but releases the release-lock (or vice
        versa) to model the cross-process race between the auto cycle
        and web force-import on the same MBID.
        """
        self._advisory_lock_result = result

    @contextmanager
    def advisory_lock(self, namespace: int, key: int) -> Iterator[bool]:
        """In-memory stand-in for ``PipelineDB.advisory_lock``.

        Records every ``(namespace, key)`` invocation and yields the
        value set via ``set_advisory_lock_result`` (default ``True``).
        Tests that want to simulate contention flip the flag to ``False``
        before calling the code under test.
        """
        self.advisory_lock_calls.append((namespace, key))
        acquired = (
            self._advisory_lock_result(namespace, key)
            if callable(self._advisory_lock_result)
            else self._advisory_lock_result)
        yield acquired

    @contextmanager
    def _pin_owner_session(
        self,
        token: CancellationToken,
    ) -> Iterator[OwnerSessionIdentity]:
        if self._owner_session_pin is not None:
            raise RuntimeError("owner session is already pinned")
        token.raise_if_cancelled()
        if self.closed:
            token.cancel("owner_session_pin_failed")
            raise OwnerSessionLost("could not pin owner session")
        identity = OwnerSessionIdentity(
            connection_object_id=id(self),
            backend_pid=1,
        )
        pin = (identity, token)
        self._owner_session_pin = pin
        try:
            yield identity
        finally:
            if self._owner_session_pin is pin:
                self._owner_session_pin = None

    def _probe_owner_session(
        self,
        identity: OwnerSessionIdentity,
        *,
        deadline_seconds: float = 0.75,
    ) -> OwnerSessionProbe:
        if not 0 < deadline_seconds <= 1.0:
            raise ValueError(
                "owner-session probe deadline must be in (0, 1.0]"
            )
        pin = self._owner_session_pin
        if pin is None:
            return OwnerSessionProbe(
                False,
                "scope_not_pinned",
                identity,
                None,
            )
        expected, token = pin
        if token.cancelled:
            return OwnerSessionProbe(
                False,
                "execution_cancelled",
                identity,
                None,
            )
        if expected != identity:
            return OwnerSessionProbe(
                False,
                "identity_mismatch",
                identity,
                None,
            )
        if self.closed:
            token.cancel("owner_session_closed")
            return OwnerSessionProbe(
                False,
                "connection_closed",
                identity,
                None,
            )
        return OwnerSessionProbe(
            True,
            "exact_backend",
            identity,
            identity.backend_pid,
        )

    def close(self) -> None:
        """Record that the fake connection was closed. No-op otherwise."""
        self.closed = True
        self.close_calls += 1

