"""The importer answers "which import job kind is this" exactly once.

``scripts/importer.py`` used to re-derive every job-type-dependent fact at
its own point of use: four ``if job.job_type == ...`` arms in
``execute_import_job``, two authority booleans in ``process_claimed_job``,
two ``!= IMPORT_JOB_FORCE`` wrong-match gates, and a four-way claim-route
chain in ``run_once``. Issue #1278 replaced those with one per-kind adapter
registry, so this module pins what each kind's adapter says — the executor,
the claim route, the authority class, the action-copy lane, and the two
wrong-match roles — plus the fail-closed shape an unregistered job type
takes.

A registry that selects behaviour per kind is exactly where an
argument-inversion mutant hides (the #1110/#1241 lesson), so every field is
asserted per kind rather than "some kind has it".
"""

from __future__ import annotations

import unittest
from datetime import UTC, datetime
from typing import ClassVar

from lib.import_preview import ACTION_COPY_PREFIX_BY_JOB_TYPE
from lib.import_queue import (
    IMPORT_JOB_AUTOMATION,
    IMPORT_JOB_FORCE,
    IMPORT_JOB_LOCAL,
    IMPORT_JOB_TYPES,
    IMPORT_JOB_YOUTUBE,
    AutomationImportPayload,
    ForceImportPayload,
    ImportJob,
    ImportJobPayload,
    LocalImportPayload,
    YoutubeImportPayload,
)
from scripts import importer

_PAYLOADS: dict[str, ImportJobPayload] = {
    IMPORT_JOB_FORCE: ForceImportPayload(
        download_log_id=7,
        failed_path="/processing/albums/wrong_matches/Album",
        source_username="peer",
        source_dirs=["peer\\Album", ""],
    ),
    IMPORT_JOB_LOCAL: LocalImportPayload(
        source_path="/operator/real/Album", request_id=41,
    ),
    IMPORT_JOB_AUTOMATION: AutomationImportPayload(),
    IMPORT_JOB_YOUTUBE: YoutubeImportPayload(
        staged_path="/Incoming/auto-import/Album",
        request_id=41,
        browse_id="MPREb_x",
        download_log_id=7,
    ),
}


def _job(job_type: str, *, payload: ImportJobPayload | None = None) -> ImportJob:
    """One ``ImportJob`` of the given type, built without ``from_row``.

    Direct construction (the ``TestCleanupTerminalForceActionFailsClosed``
    precedent) is what lets an unregistered ``job_type`` reach production
    at all: ``ImportJob.from_row`` runs ``validate_job_type`` first.
    """
    now = datetime(2026, 8, 31, tzinfo=UTC)
    return ImportJob(
        id=4101,
        job_type=job_type,
        status="running",
        request_id=41,
        dedupe_key=None,
        payload=payload if payload is not None else _PAYLOADS[job_type],
        result=None,
        message=None,
        error=None,
        attempts=0,
        worker_id="w",
        created_at=now,
        updated_at=now,
        started_at=now,
        heartbeat_at=now,
        completed_at=None,
    )


class TestImportJobKindRegistry(unittest.TestCase):
    """Every kind's adapter names its own executor, route and policy."""

    def test_the_registry_covers_exactly_the_validated_job_types(self) -> None:
        # Derived from the real enum, never hand-listed: a fifth job type
        # added to IMPORT_JOB_TYPES without an adapter would otherwise fall
        # through to the unrouted kind unnoticed.
        self.assertEqual(set(importer._IMPORT_JOB_KINDS), set(IMPORT_JOB_TYPES))

    EXECUTORS: ClassVar[list[tuple[str, object]]] = [
        (IMPORT_JOB_FORCE, importer._execute_force_kind),
        (IMPORT_JOB_LOCAL, importer._execute_local_kind),
        (IMPORT_JOB_AUTOMATION, importer._execute_automation_kind),
        (IMPORT_JOB_YOUTUBE, importer._execute_youtube_kind),
    ]

    def test_each_kind_names_its_own_executor(self) -> None:
        for job_type, execute in self.EXECUTORS:
            with self.subTest(job_type=job_type):
                self.assertIs(
                    importer._kind_for(job_type).execute_fn, execute,
                )

    CLAIM_ROUTES: ClassVar[list[tuple[str, object]]] = [
        (IMPORT_JOB_FORCE, importer._claim_route_force_import),
        (IMPORT_JOB_LOCAL, importer._claim_route_local_import),
        (IMPORT_JOB_AUTOMATION, importer._claim_route_automation),
        (IMPORT_JOB_YOUTUBE, importer._claim_route_plain),
    ]

    def test_each_kind_names_its_own_claim_route(self) -> None:
        for job_type, route in self.CLAIM_ROUTES:
            with self.subTest(job_type=job_type):
                self.assertIs(importer._kind_for(job_type).claim_route, route)

    AUTHORITY: ClassVar[list[tuple[str, str]]] = [
        (IMPORT_JOB_FORCE, importer._AUTHORITY_PINNED_PAIR),
        (IMPORT_JOB_LOCAL, importer._AUTHORITY_PINNED_PAIR),
        (IMPORT_JOB_AUTOMATION, importer._AUTHORITY_AUTOMATION),
        (IMPORT_JOB_YOUTUBE, importer._AUTHORITY_PLAIN),
    ]

    def test_each_kind_declares_its_own_claim_authority_class(self) -> None:
        for job_type, authority in self.AUTHORITY:
            with self.subTest(job_type=job_type):
                self.assertEqual(
                    importer._kind_for(job_type).authority, authority,
                )

    # (job_type, runs the committed wrong-match rejection convergence,
    #  owns an originating Wrong Matches source row)
    WRONG_MATCH_ROLES: ClassVar[list[tuple[str, bool, bool]]] = [
        (IMPORT_JOB_FORCE, False, True),
        (IMPORT_JOB_LOCAL, True, False),
        (IMPORT_JOB_AUTOMATION, True, False),
        (IMPORT_JOB_YOUTUBE, True, False),
    ]

    def test_each_kind_declares_both_wrong_match_roles(self) -> None:
        for job_type, runs_cleanup, owns_source in self.WRONG_MATCH_ROLES:
            with self.subTest(job_type=job_type):
                kind = importer._kind_for(job_type)
                self.assertEqual(
                    kind.runs_committed_wrong_match_cleanup, runs_cleanup,
                )
                self.assertEqual(kind.owns_wrong_match_source, owns_source)

    def test_only_the_force_lane_consumes_a_wrong_match_source(self) -> None:
        """The role flag decides a real production return, not just a table."""
        force_payload = importer._force_job_wrong_match_payload(
            _job(IMPORT_JOB_FORCE),
        )
        self.assertEqual(
            force_payload,
            (7, "/processing/albums/wrong_matches/Album"),
        )
        for job_type in (
            IMPORT_JOB_LOCAL, IMPORT_JOB_AUTOMATION, IMPORT_JOB_YOUTUBE,
        ):
            with self.subTest(job_type=job_type):
                self.assertIsNone(
                    importer._force_job_wrong_match_payload(_job(job_type)),
                )


class TestActionCopyLanes(unittest.TestCase):
    """The two lanes that retain a job-scoped private action copy."""

    # (job_type, lane constant, lane label, attempt scenario,
    #  distance-threshold resolver)
    LANES: ClassVar[list[tuple[str, object, str, str, object]]] = [
        (
            IMPORT_JOB_FORCE,
            importer._FORCE_ACTION_COPY_LANE,
            "force",
            "force_import",
            importer._force_import_distance_threshold,
        ),
        (
            IMPORT_JOB_LOCAL,
            importer._LOCAL_IMPORT_ACTION_COPY_LANE,
            "local-import",
            "local_import",
            importer._configured_distance_threshold,
        ),
    ]

    def test_each_action_copy_kind_names_its_own_lane(self) -> None:
        for job_type, lane, label, scenario, threshold_fn in self.LANES:
            with self.subTest(job_type=job_type):
                kind_lane = importer._kind_for(job_type).action_copy
                self.assertIs(kind_lane, lane)
                assert kind_lane is not None
                self.assertEqual(kind_lane.job_type, job_type)
                self.assertEqual(kind_lane.lane_label, label)
                self.assertEqual(kind_lane.scenario, scenario)
                self.assertIs(kind_lane.distance_threshold_fn, threshold_fn)

    def test_the_lanes_are_exactly_the_prefix_tables_own_job_types(self) -> None:
        # The prefix table in lib/import_preview.py stays the single source
        # for job_type -> action-copy prefix; this proves the kind registry
        # agrees with it in both directions rather than restating it.
        with_lane = {
            job_type
            for job_type, kind in importer._IMPORT_JOB_KINDS.items()
            if kind.action_copy is not None
        }
        self.assertEqual(with_lane, set(ACTION_COPY_PREFIX_BY_JOB_TYPE))

    def test_each_lane_resolves_its_prefix_from_that_single_source(self) -> None:
        for job_type, lane, _label, _scenario, _fn in self.LANES:
            with self.subTest(job_type=job_type):
                assert isinstance(lane, importer._ActionCopyLane)
                self.assertEqual(
                    lane.action_copy_prefix(),
                    ACTION_COPY_PREFIX_BY_JOB_TYPE[job_type],
                )

    def test_a_kind_with_no_action_copy_lane_declares_none(self) -> None:
        for job_type in (IMPORT_JOB_AUTOMATION, IMPORT_JOB_YOUTUBE):
            with self.subTest(job_type=job_type):
                self.assertIsNone(importer._kind_for(job_type).action_copy)

    def test_each_lane_extracts_its_own_dispatch_source(self) -> None:
        force = importer._FORCE_ACTION_COPY_LANE.source_fn(
            _PAYLOADS[IMPORT_JOB_FORCE],
        )
        self.assertEqual(
            force,
            importer._ActionCopySource(
                source_reference_path="/processing/albums/wrong_matches/Album",
                source_username="peer",
                # Blank entries are dropped; an empty list becomes None.
                source_dirs=["peer\\Album"],
                download_log_id=7,
            ),
        )
        # CLAUDE.md decision 2: a local import never exposes the operator's
        # own folder, and has no Soulseek peer behind it.
        self.assertEqual(
            importer._LOCAL_IMPORT_ACTION_COPY_LANE.source_fn(
                _PAYLOADS[IMPORT_JOB_LOCAL],
            ),
            importer._ActionCopySource(
                source_reference_path=None,
                source_username=None,
                source_dirs=None,
                download_log_id=None,
            ),
        )

    def test_each_lane_rejects_the_other_lanes_payload(self) -> None:
        cases = [
            (
                importer._FORCE_ACTION_COPY_LANE,
                _PAYLOADS[IMPORT_JOB_LOCAL],
                "force_import payload type mismatch",
            ),
            (
                importer._LOCAL_IMPORT_ACTION_COPY_LANE,
                _PAYLOADS[IMPORT_JOB_FORCE],
                "local_import payload type mismatch",
            ),
        ]
        for lane, payload, message in cases:
            with (
                self.subTest(lane=lane.lane_label),
                self.assertRaisesRegex(AssertionError, message),
            ):
                lane.source_fn(payload)

    def test_force_import_never_overrides_the_configured_distance(self) -> None:
        cfg = importer.CratediggerConfig(beets_distance_threshold=0.31)
        self.assertIsNone(importer._force_import_distance_threshold(cfg))
        self.assertEqual(importer._configured_distance_threshold(cfg), 0.31)


class TestUnroutedJobKind(unittest.TestCase):
    """A job type ``validate_job_type`` cannot produce still fails closed."""

    def test_an_unregistered_type_resolves_to_the_unrouted_kind(self) -> None:
        self.assertIs(
            importer._kind_for("manual_import"), importer._UNROUTED_JOB_KIND,
        )

    def test_the_unrouted_kind_keeps_the_pre_registry_fall_through(self) -> None:
        kind = importer._UNROUTED_JOB_KIND
        # The old code's `else` arms, field by field: the plain claim path
        # with no pinned session, `execute_import_job`'s unsupported-type
        # outcome, no retained action copy, and the `!= IMPORT_JOB_FORCE`
        # wrong-match gate reading True.
        self.assertIs(kind.claim_route, importer._claim_route_plain)
        self.assertIs(kind.execute_fn, importer._execute_unsupported_kind)
        self.assertEqual(kind.authority, importer._AUTHORITY_PLAIN)
        self.assertIsNone(kind.action_copy)
        self.assertTrue(kind.runs_committed_wrong_match_cleanup)
        self.assertFalse(kind.owns_wrong_match_source)

    def test_an_unregistered_type_never_consumes_a_wrong_match_source(
        self,
    ) -> None:
        job = _job("manual_import", payload=_PAYLOADS[IMPORT_JOB_FORCE])
        self.assertIsNone(importer._force_job_wrong_match_payload(job))


if __name__ == "__main__":
    unittest.main()
