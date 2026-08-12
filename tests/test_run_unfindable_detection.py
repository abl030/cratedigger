"""Deterministic tests for the unfindable-detection oneshot's run-health
exit code (issue #1090) and run-metrics telemetry (issue #1112).

``main()`` wires a real ``PipelineDB``/``SlskdClient`` from runtime
config — not worth mocking end-to-end for a process exit-code contract.
``_process_batch`` is the extracted, directly testable seam: given an
already-constructed ``UnfindableDetectionService`` and its ``db``, it
drives one due-batch pass, logs per-row outcomes, records one
``unfindable_run_metrics`` row (#1112), and returns the process exit
code. This module drives the REAL production service against
``FakePipelineDB`` + ``FakeSlskdAPI`` and pins the two exit-code
branches: a fully classified run (0) vs. an incomplete run where the
circuit breaker tripped (``EXIT_INCOMPLETE_RUN``).
"""

from __future__ import annotations

import unittest

from lib.search_exec import SearchSubmitError
from lib.unfindable_detection_service import (
    ArtistProbeResult,
    UnfindableDetectionService,
)
from scripts.run_unfindable_detection import (
    EXIT_CONFIG_ABORT,
    EXIT_INCOMPLETE_RUN,
    _process_batch,
)
from tests.fakes import FakePipelineDB, FakeSlskdAPI


def _seed_wanted_request(db: FakePipelineDB, *, artist_name: str) -> int:
    rid = db.add_request(
        artist_name=artist_name,
        album_title=f"{artist_name} Album",
        source="request",
        mb_release_id=f"mb-{artist_name.replace(' ', '_')}-1",
    )
    db.set_tracks(rid, [
        {"disc_number": 1, "track_number": 1, "title": "T1"},
        {"disc_number": 1, "track_number": 2, "title": "T2"},
    ])
    return rid


class TestProcessBatchExitCode(unittest.TestCase):
    """Issue #1090 pin (d): a fully classified run returns 0; an
    incomplete run (circuit breaker tripped) returns a distinct
    non-zero code."""

    def setUp(self) -> None:
        self.db = FakePipelineDB()
        self.slskd = FakeSlskdAPI()

    def test_fully_classified_run_returns_zero(self) -> None:
        _seed_wanted_request(self.db, artist_name="Fine Artist")

        def _probe(
            _client: object, *, artist_name: str, **_kw: object,
        ) -> ArtistProbeResult:
            return ArtistProbeResult(match_count=50, artist_observed=True)

        svc = UnfindableDetectionService(
            self.db, self.slskd, probe_runner=_probe)
        exit_code = _process_batch(
            svc, self.db, limit=10,
            cohort_total=1, due_backlog_at_start=1,
        )
        self.assertEqual(exit_code, 0)

    def test_breaker_tripped_run_returns_distinct_exit_code(self) -> None:
        for i in range(5):
            _seed_wanted_request(self.db, artist_name=f"Artist{i}")

        def _always_submit_failure(
            _client: object, *, artist_name: str, **_kw: object,
        ) -> ArtistProbeResult:
            raise SearchSubmitError(
                "simulated sustained slskd outage", retry_exhausted=True)

        svc = UnfindableDetectionService(
            self.db, self.slskd, probe_runner=_always_submit_failure,
        )
        exit_code = _process_batch(
            svc, self.db, limit=10,
            cohort_total=5, due_backlog_at_start=5,
        )
        self.assertEqual(exit_code, EXIT_INCOMPLETE_RUN)
        self.assertNotEqual(exit_code, 0)

    def test_exit_codes_are_distinct_from_config_schema_abort(self) -> None:
        """``EXIT_INCOMPLETE_RUN`` must not collide with
        ``EXIT_CONFIG_ABORT`` (returned directly by ``main()`` before any
        work runs) -- two distinguishable failure classes need two
        distinguishable, producer-derived codes (issue #1090 NIT-8)."""
        self.assertNotEqual(EXIT_INCOMPLETE_RUN, EXIT_CONFIG_ABORT)
        self.assertNotEqual(EXIT_INCOMPLETE_RUN, 0)
        self.assertNotEqual(EXIT_CONFIG_ABORT, 0)


class TestProcessBatchRunMetrics(unittest.TestCase):
    """Issue #1112: every ``_process_batch`` call writes exactly one
    ``unfindable_run_metrics`` row -- for both a fully classified run
    and a breaker-tripped (incomplete) one."""

    def setUp(self) -> None:
        self.db = FakePipelineDB()
        self.slskd = FakeSlskdAPI()

    def test_fully_classified_run_records_one_metrics_row_with_outcomes(
        self,
    ) -> None:
        _seed_wanted_request(self.db, artist_name="Fine Artist")
        _seed_wanted_request(self.db, artist_name="Solo Artist")

        def _probe(
            _client: object, *, artist_name: str, **_kw: object,
        ) -> ArtistProbeResult:
            return ArtistProbeResult(match_count=50, artist_observed=True)

        svc = UnfindableDetectionService(
            self.db, self.slskd, probe_runner=_probe)
        exit_code = _process_batch(
            svc, self.db, limit=10,
            cohort_total=2, due_backlog_at_start=2,
        )

        self.assertEqual(exit_code, 0)
        self.assertEqual(len(self.db.unfindable_run_metrics), 1)
        row = self.db.unfindable_run_metrics[0]
        self.assertEqual(row["cohort_total"], 2)
        self.assertEqual(row["due_backlog_at_start"], 2)
        self.assertEqual(row["batch_limit"], 10)
        self.assertEqual(row["probes_attempted"], 2)
        self.assertFalse(row["breaker_tripped"])
        self.assertGreaterEqual(row["duration_seconds"], 0.0)
        # Both requests matched — neither categorised nor downgraded.
        self.assertEqual(row["no_change_count"], 2)
        self.assertEqual(row["categorised_count"], 0)
        self.assertEqual(row["probe_failed_count"], 0)

    def test_breaker_tripped_run_still_records_a_metrics_row(self) -> None:
        """A partial/failed run is exactly what the operator needs to
        see on the dashboard — the metrics write is unconditional."""
        for i in range(5):
            _seed_wanted_request(self.db, artist_name=f"Artist{i}")

        def _always_submit_failure(
            _client: object, *, artist_name: str, **_kw: object,
        ) -> ArtistProbeResult:
            raise SearchSubmitError(
                "simulated sustained slskd outage", retry_exhausted=True)

        svc = UnfindableDetectionService(
            self.db, self.slskd, probe_runner=_always_submit_failure,
        )
        exit_code = _process_batch(
            svc, self.db, limit=10,
            cohort_total=5, due_backlog_at_start=5,
        )

        self.assertEqual(exit_code, EXIT_INCOMPLETE_RUN)
        self.assertEqual(len(self.db.unfindable_run_metrics), 1)
        row = self.db.unfindable_run_metrics[0]
        self.assertTrue(row["breaker_tripped"])
        # Circuit breaker trips after 3 consecutive submit failures —
        # fewer than all 5 candidates were attempted.
        self.assertEqual(row["probes_attempted"], 3)
        self.assertEqual(row["probe_failed_count"], 3)
        self.assertEqual(row["cohort_total"], 5)
        self.assertEqual(row["due_backlog_at_start"], 5)


if __name__ == "__main__":
    unittest.main()
