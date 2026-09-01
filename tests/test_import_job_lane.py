"""Deterministic pins for the two ``import_jobs`` claim lanes."""

from __future__ import annotations

import os
import sys
import unittest

sys.path.append(os.path.dirname(__file__))
import conftest  # noqa: F401  (boots and migrates ephemeral PostgreSQL)

from lib.import_job_lane import IMPORT_LANE, JOB_LANES, PREVIEW_LANE, JobLane
from lib.import_queue import (
    IMPORT_JOB_PREVIEW_EVIDENCE_READY,
    IMPORT_JOB_PREVIEW_WAITING,
)
from lib.pipeline_db import PipelineDB
from lib.pipeline_db.import_jobs import (
    _CANDIDATE_JOB_TYPE_ROUTING,
    _claim_assignments_sql,
)

TEST_DSN = os.environ["TEST_DB_DSN"]


class TestJobLaneValues(unittest.TestCase):
    """The lane values themselves — what a claim in each lane means."""

    def test_a_row_travels_the_preview_lane_then_the_import_lane(self) -> None:
        self.assertEqual(JOB_LANES, (PREVIEW_LANE, IMPORT_LANE))

    def test_each_lane_enters_at_the_status_the_other_lane_leaves(self) -> None:
        # The preview lane takes a fresh row and the import lane takes the
        # row the preview lane finished with; nothing else may enter either.
        self.assertEqual(
            PREVIEW_LANE.entry_preview_status,
            IMPORT_JOB_PREVIEW_WAITING,
        )
        self.assertEqual(
            IMPORT_LANE.entry_preview_status,
            IMPORT_JOB_PREVIEW_EVIDENCE_READY,
        )
        self.assertNotEqual(
            PREVIEW_LANE.entry_preview_status,
            IMPORT_LANE.entry_preview_status,
        )

    def test_the_two_lanes_stamp_disjoint_columns(self) -> None:
        """No column a claim writes may belong to both lanes.

        This is what makes a preview claim invisible to the import lane's
        own state and vice versa: they share the row, never the columns.
        """
        self.assertEqual(
            set(IMPORT_LANE.stamped_columns)
            & set(PREVIEW_LANE.stamped_columns),
            set(),
        )

    def test_only_the_preview_lane_clears_the_previous_attempt(self) -> None:
        self.assertEqual(IMPORT_LANE.cleared_columns, ())
        self.assertEqual(
            PREVIEW_LANE.cleared_columns,
            ("preview_message", "preview_error"),
        )

    def test_stamped_columns_are_the_lane_columns_and_nothing_else(
        self,
    ) -> None:
        self.assertEqual(
            IMPORT_LANE.stamped_columns,
            (
                "status",
                "attempts",
                "worker_id",
                "started_at",
                "heartbeat_at",
            ),
        )
        self.assertEqual(
            PREVIEW_LANE.stamped_columns,
            (
                "preview_status",
                "preview_attempts",
                "preview_worker_id",
                "preview_started_at",
                "preview_heartbeat_at",
                "preview_message",
                "preview_error",
            ),
        )


class TestLaneColumnsAreRealColumns(unittest.TestCase):
    """Fail closed if a lane names a column ``import_jobs`` does not have.

    The claim ``SET`` fragment is rendered from these names, so a typo here
    is a runtime ``UndefinedColumn`` on the live claim path rather than a
    type error.
    """

    def setUp(self) -> None:
        self.db = PipelineDB(TEST_DSN)

    def tearDown(self) -> None:
        self.db.close()

    def _import_jobs_columns(self) -> set[str]:
        cur = self.db._execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'import_jobs'
            """,
        )
        return {str(row["column_name"]) for row in cur.fetchall()}

    def test_every_lane_column_exists_on_import_jobs(self) -> None:
        columns = self._import_jobs_columns()
        self.assertIn("id", columns)  # the query found the real table
        for lane in JOB_LANES:
            for column in lane.stamped_columns:
                with self.subTest(lane=lane.name, column=column):
                    self.assertIn(column, columns)


class TestClaimAssignmentsSql(unittest.TestCase):
    """The rendered claim ``SET`` fragment, per lane."""

    def test_import_lane_renders_the_unprefixed_claim(self) -> None:
        self.assertEqual(
            _claim_assignments_sql(IMPORT_LANE),
            "status = 'running',\n"
            "                    attempts = attempts + 1,\n"
            "                    worker_id = %s,\n"
            "                    started_at = COALESCE(started_at, NOW()),\n"
            "                    heartbeat_at = NOW()",
        )

    def test_preview_lane_renders_the_prefixed_claim_and_clears(self) -> None:
        self.assertEqual(
            _claim_assignments_sql(PREVIEW_LANE),
            "preview_status = 'running',\n"
            "                    preview_attempts = preview_attempts + 1,\n"
            "                    preview_worker_id = %s,\n"
            "                    preview_started_at = "
            "COALESCE(preview_started_at, NOW()),\n"
            "                    preview_heartbeat_at = NOW(),\n"
            "                    preview_message = NULL,\n"
            "                    preview_error = NULL",
        )

    def test_exactly_one_placeholder_regardless_of_lane(self) -> None:
        """The worker id is the only bound value in the fragment.

        Every caller appends its own placeholders after this fragment, so a
        lane that rendered a second ``%s`` would silently shift every one of
        them.
        """
        for lane in JOB_LANES:
            with self.subTest(lane=lane.name):
                self.assertEqual(_claim_assignments_sql(lane).count("%s"), 1)

    def test_a_lane_with_no_cleared_columns_renders_no_trailing_comma(
        self,
    ) -> None:
        rendered = _claim_assignments_sql(
            JobLane(
                name="probe",
                entry_preview_status="waiting",
                status_column="status",
                attempts_column="attempts",
                worker_id_column="worker_id",
                started_at_column="started_at",
                heartbeat_at_column="heartbeat_at",
                cleared_columns=(),
            ),
        )
        self.assertFalse(rendered.rstrip().endswith(","))


class TestSharedCandidateRouting(unittest.TestCase):
    """The one positive ``job_type`` routing table both scans select through."""

    def test_every_job_type_is_named_positively(self) -> None:
        """#1176 PR3: no ``NOT IN`` bucket may decide candidate visibility."""
        self.assertNotIn("NOT IN ('automation_import'", _CANDIDATE_JOB_TYPE_ROUTING)
        for job_type in (
            "youtube_import",
            "automation_import",
            "force_import",
            "local_import",
        ):
            with self.subTest(job_type=job_type):
                self.assertIn(f"'{job_type}'", _CANDIDATE_JOB_TYPE_ROUTING)

    def test_it_binds_exactly_the_execution_lease(self) -> None:
        self.assertEqual(_CANDIDATE_JOB_TYPE_ROUTING.count("%s"), 1)


if __name__ == "__main__":
    unittest.main()
