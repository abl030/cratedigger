"""FakeCursor — deterministic cursor for raw-SQL CLI paths."""

from __future__ import annotations

from typing import Any


class FakeCursor:
    """Minimal DB-API cursor stand-in for raw-SQL seams.

    Pair with :meth:`FakePipelineDB.queue_execute_results` when the
    code under test goes through ``PipelineDB._execute`` directly
    (e.g. ``web.overlay.check_pipeline``) — the fake cannot interpret
    SQL, so the test supplies the rows the query would return.

    Mirrors real psycopg2 consumption semantics (test-fidelity Rule B):
    ``fetchone`` advances and returns ``None`` when exhausted;
    ``fetchall`` drains and returns only the remainder. A
    ``while cur.fetchone():`` consumer must terminate.
    """

    def __init__(self, rows: list[dict[str, Any]] | None = None) -> None:
        self._rows = list(rows) if rows else []

    def fetchall(self) -> list[dict[str, Any]]:
        rows, self._rows = self._rows, []
        return rows

    def fetchone(self) -> dict[str, Any] | None:
        return self._rows.pop(0) if self._rows else None


