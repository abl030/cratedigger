"""Generated exact-owner/revision cleanup-journal persistence patrol."""

from __future__ import annotations

import os
import re
import sys
import unittest
from dataclasses import dataclass
from typing import Literal

from hypothesis import example, given
from hypothesis import strategies as st

import tests._hypothesis_profiles  # noqa: F401  (loads the active profile)

sys.path.append(os.path.dirname(__file__))
import conftest  # noqa: F401  (boots and migrates ephemeral PostgreSQL)

from lib.pipeline_db import (
    CleanupJournalConflict,
    CleanupJournalIntent,
    PipelineDB,
)

TEST_DSN = os.environ["TEST_DB_DSN"]
GuardMutation = Literal["none", "owner_blind", "revision_blind"]

# Per-clause proof (#1094). Every clause of ``assert_owner_revision_guard``
# is named here so a self-test can anchor on that clause's own message rather
# than a substring a sibling could also satisfy.
CLAUSE_FOREIGN_MUTATION = (
    "cleanup journal mutated without exact owner and revision"
)
CLAUSE_EXACT_REFUSED = (
    "exact owner and revision did not admit its checkpoint"
)


def _exact_clause(message: str) -> str:
    """Anchor a clause message so no sibling clause can satisfy the regex."""
    return "^" + re.escape(message) + "$"


@dataclass(frozen=True)
class GuardWorld:
    mutation: GuardMutation
    expected_revision_delta: int
    progress_key: str


def assert_owner_revision_guard(
    world: GuardWorld,
    *,
    mutation_committed: bool,
) -> None:
    """Neither a wrong owner nor a stale revision may mutate the journal."""
    guard_is_wrong = (
        world.mutation != "none"
        or world.expected_revision_delta != 0
    )
    if guard_is_wrong and mutation_committed:
        raise AssertionError(CLAUSE_FOREIGN_MUTATION)
    if not guard_is_wrong and not mutation_committed:
        raise AssertionError(CLAUSE_EXACT_REFUSED)


class TestCleanupJournalGenerated(unittest.TestCase):
    def setUp(self) -> None:
        self.db = PipelineDB(TEST_DSN)

    def tearDown(self) -> None:
        self.db.close()

    def _reset(self) -> None:
        self.db._execute(
            "TRUNCATE processing_cleanup_journal, import_jobs, "
            "album_requests CASCADE"
        )

    def _owner(self, suffix: str) -> tuple[int, int]:
        with self.db._atomic():
            request = self.db._execute(
                """
                INSERT INTO album_requests (
                    mb_release_id, artist_name, album_title, source
                )
                VALUES (%s, 'Artist', 'Album', 'request')
                RETURNING id
                """,
                (suffix,),
            ).fetchone()
            request_id = int(request["id"])
            job = self.db._execute(
                """
                INSERT INTO import_jobs (
                    job_type, status, request_id, payload, preview_status
                )
                VALUES (
                    'automation_import', 'queued', %s, '{}'::jsonb, 'waiting'
                )
                RETURNING id
                """,
                (request_id,),
            ).fetchone()
            job_id = int(job["id"])
            self.db._execute(
                """
                UPDATE album_requests
                SET status = 'processing',
                    active_automation_import_job_id = %s
                WHERE id = %s
                """,
                (job_id, request_id),
            )
            self.db.conn.commit()
        return request_id, job_id

    @example(
        mutation="revision_blind",
        expected_revision_delta=-1,
        progress_key="published",
    )
    # Pins the ONLY world that can fire ``CLAUSE_EXACT_REFUSED``: the exact
    # owner at the exact revision. Measured at 6 / 150 in the gating `suite`
    # tier before this pin, so any future edit to the property body could
    # reshuffle it to zero and retire the clause silently (#1094 Q4).
    @example(
        mutation="none",
        expected_revision_delta=0,
        progress_key="published",
    )
    @given(
        mutation=st.sampled_from(
            ("none", "owner_blind", "revision_blind")
        ),
        expected_revision_delta=st.integers(min_value=-3, max_value=3),
        progress_key=st.text(
            alphabet="abcdefghijklmnopqrstuvwxyz_",
            min_size=1,
            max_size=16,
        ),
    )
    def test_real_pg_requires_exact_owner_and_revision(
        self,
        mutation: GuardMutation,
        expected_revision_delta: int,
        progress_key: str,
    ) -> None:
        self._reset()
        request_id, job_id = self._owner("generated-owner")
        other_request_id, other_job_id = self._owner("generated-other")
        intent = CleanupJournalIntent(
            action="move_tree",
            source_path="/processing/source",
            source_manifest=({"path": "01.flac", "size": 12},),
            source_manifest_hash="source-hash",
        )
        row = self.db.create_processing_cleanup_journal(
            request_id=request_id,
            job_id=job_id,
            intent=intent,
        )

        target_request_id = request_id
        target_job_id = job_id
        if mutation == "owner_blind":
            target_request_id = other_request_id
            target_job_id = other_job_id
        actual_revision_delta = expected_revision_delta
        if mutation == "revision_blind" and actual_revision_delta == 0:
            actual_revision_delta = 1
        expected_revision = row["revision"] + actual_revision_delta
        mutation_committed = False
        try:
            self.db.checkpoint_processing_cleanup_journal(
                request_id=target_request_id,
                job_id=target_job_id,
                expected_revision=expected_revision,
                step_progress={progress_key: True},
            )
            mutation_committed = True
        except (CleanupJournalConflict, ValueError):
            pass

        world = GuardWorld(
            mutation=mutation,
            expected_revision_delta=expected_revision_delta,
            progress_key=progress_key,
        )
        assert_owner_revision_guard(
            world,
            mutation_committed=mutation_committed,
        )
        persisted = self.db.get_processing_cleanup_journal(
            request_id=request_id,
            job_id=job_id,
        )
        self.assertIsNotNone(persisted)
        assert persisted is not None
        if mutation_committed:
            self.assertEqual(
                persisted["step_progress"],
                {progress_key: True},
            )
        else:
            self.assertEqual(persisted["step_progress"], {})

    def test_every_guard_clause_has_a_named_world(self) -> None:
        """One world per clause, anchored on that clause's own message.

        Both historical known-bad self-tests asserted the substring
        ``"without exact owner and revision"``, which only the
        foreign-mutation clause can emit — so the exact-refusal clause had no
        proof at all. Each row below is the minimal world that makes exactly
        one clause's condition true.
        """
        cases: tuple[tuple[str, GuardWorld, bool, str], ...] = (
            (
                "owner_blind committed",
                GuardWorld("owner_blind", 0, "published"),
                True,
                CLAUSE_FOREIGN_MUTATION,
            ),
            (
                "revision_blind committed",
                GuardWorld("revision_blind", -1, "published"),
                True,
                CLAUSE_FOREIGN_MUTATION,
            ),
            (
                "stale revision committed under the exact owner",
                GuardWorld("none", 1, "published"),
                True,
                CLAUSE_FOREIGN_MUTATION,
            ),
            (
                "exact owner and revision refused",
                GuardWorld("none", 0, "published"),
                False,
                CLAUSE_EXACT_REFUSED,
            ),
        )
        for description, world, committed, message in cases:
            with (
                self.subTest(description),
                self.assertRaisesRegex(
                    AssertionError,
                    _exact_clause(message),
                ),
            ):
                assert_owner_revision_guard(
                    world,
                    mutation_committed=committed,
                )

    def test_guard_admits_the_only_two_correct_worlds(self) -> None:
        """Must-still-work: neither clause fires on a correctly guarded world."""
        assert_owner_revision_guard(
            GuardWorld("none", 0, "published"),
            mutation_committed=True,
        )
        assert_owner_revision_guard(
            GuardWorld("owner_blind", 0, "published"),
            mutation_committed=False,
        )


if __name__ == "__main__":
    unittest.main()
