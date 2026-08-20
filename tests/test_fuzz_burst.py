"""Contracts for the generated fuzz-burst runner."""

from __future__ import annotations

import contextlib
import io
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from scripts.run_fuzz_tests import (
    DEPTH_REPORT_LIMIT,
    EPHEMERAL_POSTGRES_TARGET_LIMIT,
    MAX_FUZZ_JOBS,
    FuzzModuleManifest,
    FuzzPropertyManifest,
    FuzzTarget,
    PropertyDepth,
    aggregate_property_depth,
    assert_exact_fuzz_coverage,
    assert_fuzz_admission,
    build_fuzz_targets,
    discard_rate,
    discover_fuzz_manifests,
    format_depth_report,
    is_structurally_shallow,
    main,
    property_profile_max_examples,
    recommended_fuzz_jobs,
    recommended_postgres_jobs,
    recommended_property_shards,
    run_fuzz_targets,
    select_fuzz_admissions,
)
from scripts.run_python_tests import (
    STRATEGY_SPACE_EXHAUSTED,
    TEST_RAM_ROOT_EXHAUSTED_EXIT_CODE,
    HypothesisPropertyStats,
)
from scripts.run_test_suite import TEST_RAM_ROOT_EXHAUSTED, RamRootExhaustedError
from tests._source_pins import pinned_source

REPO_ROOT = Path(__file__).resolve().parent.parent
RUNNER = REPO_ROOT / "scripts" / "run_fuzz_tests.py"
WRAPPER = REPO_ROOT / "scripts" / "fuzz_burst.sh"


class TestFuzzTargetPlanning(unittest.TestCase):
    @staticmethod
    def property(
        test_id: str,
        *,
        max_examples: int = 500,
        uses_default_settings: bool = True,
    ) -> FuzzPropertyManifest:
        return FuzzPropertyManifest(
            test_id=test_id,
            max_examples=max_examples,
            uses_default_settings=uses_default_settings,
        )

    def test_multiple_properties_split_while_pins_stay_in_one_batch(self) -> None:
        property_one = (
            "tests.test_example_generated.TestWorld.test_property_one"
        )
        property_two = (
            "tests.test_example_generated.TestWorld.test_property_two"
        )
        manifest = FuzzModuleManifest(
            module_name="tests.test_example_generated",
            test_ids=(
                property_one,
                property_two,
                "tests.test_example_generated.TestWorld.test_pin",
            ),
            hypothesis_tests=(
                self.property(property_one),
                self.property(property_two),
            ),
        )

        targets = build_fuzz_targets((manifest,))

        assert_exact_fuzz_coverage((manifest,), targets)
        self.assertEqual(len(targets), 3)
        self.assertEqual(
            tuple(target.load_names for target in targets),
            (
                (manifest.module_name,),
                (manifest.module_name,),
                (manifest.module_name,),
            ),
        )
        self.assertEqual(
            tuple(target.expected_test_ids for target in targets),
            (
                (property_one,),
                (property_two,),
                ("tests.test_example_generated.TestWorld.test_pin",),
            ),
        )

    def test_audited_method_hotspot_splits_fixed_tests_into_twelve_batches(
        self,
    ) -> None:
        module_name = "tests.test_beets_destructive_configs_generated"
        property_ids = tuple(
            f"{module_name}.TestWorld.test_property_{index}"
            for index in range(2)
        )
        pin_ids = tuple(
            f"{module_name}.TestMatrix.test_cell_{index:02d}"
            for index in range(60)
        )
        manifest = FuzzModuleManifest(
            module_name=module_name,
            test_ids=property_ids + pin_ids,
            hypothesis_tests=tuple(
                self.property(property_id) for property_id in property_ids
            ),
        )

        targets = build_fuzz_targets((manifest,))

        assert_exact_fuzz_coverage((manifest,), targets)
        pin_targets = tuple(
            target
            for target in targets
            if set(target.expected_test_ids).intersection(pin_ids)
        )
        self.assertEqual(len(pin_targets), 12)
        self.assertEqual(
            {len(target.expected_test_ids) for target in pin_targets},
            {5},
        )
        self.assertTrue(
            all(target.load_names == target.expected_test_ids for target in pin_targets)
        )

    def test_single_property_module_keeps_one_process(self) -> None:
        property_id = "tests.test_example_generated.TestWorld.test_property"
        manifest = FuzzModuleManifest(
            module_name="tests.test_example_generated",
            test_ids=(
                property_id,
                "tests.test_example_generated.TestWorld.test_pin",
            ),
            hypothesis_tests=(
                self.property(property_id),
            ),
        )

        targets = build_fuzz_targets((manifest,))

        assert_exact_fuzz_coverage((manifest,), targets)
        self.assertEqual(len(targets), 1)
        self.assertEqual(targets[0].load_names, (manifest.module_name,))

    def test_default_budget_is_split_exactly_across_entropy_shards(self) -> None:
        named_property = (
            "tests.test_request_lifecycle_generated."
            "TestWorld.test_named_property"
        )
        dynamic_property = (
            "hypothesis.stateful.RequestLifecycleMachine.TestCase.runTest"
        )
        manifest = FuzzModuleManifest(
            module_name="tests.test_request_lifecycle_generated",
            test_ids=(
                named_property,
                dynamic_property,
                "tests.test_request_lifecycle_generated.TestWorld.test_pin",
            ),
            hypothesis_tests=(
                self.property(named_property, max_examples=20_003),
                self.property(dynamic_property, max_examples=20_003),
            ),
        )

        targets = build_fuzz_targets((manifest,), property_shards=4)

        assert_exact_fuzz_coverage((manifest,), targets)
        self.assertEqual(len(targets), 9)
        for property_id in (named_property, dynamic_property):
            shards = [
                target
                for target in targets
                if target.expected_test_ids == (property_id,)
            ]
            self.assertEqual(len(shards), 4)
            self.assertEqual(
            sum(target.max_examples_override or 0 for target in shards),
                20_003,
            )
            self.assertEqual(
                {target.shard_index for target in shards},
                {0, 1, 2, 3},
            )
            self.assertTrue(
                all(
                    target.load_names == (manifest.module_name,)
                    for target in shards
                )
            )

    def test_explicit_property_budget_is_split_without_multiplication(self) -> None:
        property_id = "tests.test_example_generated.TestWorld.test_property"
        manifest = FuzzModuleManifest(
            module_name="tests.test_example_generated",
            test_ids=(property_id,),
            hypothesis_tests=(
                self.property(
                    property_id,
                    max_examples=30,
                    uses_default_settings=False,
                ),
            ),
        )

        targets = build_fuzz_targets((manifest,), property_shards=8)

        assert_exact_fuzz_coverage((manifest,), targets)
        self.assertEqual(len(targets), 8)
        self.assertEqual(
            sum(target.max_examples_override or 0 for target in targets),
            30,
        )
        self.assertEqual(
            {target.shard_index for target in targets},
            set(range(8)),
        )

    def test_derandomized_explicit_property_remains_one_target(self) -> None:
        property_id = "tests.test_example_generated.TestWorld.test_property"
        manifest = FuzzModuleManifest(
            module_name="tests.test_example_generated",
            test_ids=(property_id,),
            hypothesis_tests=(
                FuzzPropertyManifest(
                    test_id=property_id,
                    max_examples=30,
                    uses_default_settings=False,
                    entropy_shardable=False,
                ),
            ),
        )

        targets = build_fuzz_targets((manifest,), property_shards=8)

        assert_exact_fuzz_coverage((manifest,), targets)
        self.assertEqual(len(targets), 1)
        self.assertIsNone(targets[0].max_examples_override)

    def test_real_beets_property_remains_one_frontloaded_target(self) -> None:
        module_name = "tests.test_beets_destructive_configs_generated"
        property_id = f"{module_name}.TestWorld.test_property"
        manifest = FuzzModuleManifest(
            module_name=module_name,
            test_ids=(property_id,),
            hypothesis_tests=(
                FuzzPropertyManifest(
                    test_id=property_id,
                    max_examples=96,
                    uses_default_settings=False,
                ),
            ),
        )

        targets = build_fuzz_targets((manifest,), property_shards=8)

        assert_exact_fuzz_coverage((manifest,), targets)
        self.assertEqual(len(targets), 1)
        self.assertEqual(targets[0].module_name, module_name)
        self.assertIsNone(targets[0].max_examples_override)

    def test_property_dense_modules_are_frontloaded(self) -> None:
        light = FuzzModuleManifest(
            module_name="tests.test_light_generated",
            test_ids=(
                "tests.test_light_generated.TestWorld.test_property_one",
                "tests.test_light_generated.TestWorld.test_property_two",
            ),
            hypothesis_tests=(
                self.property(
                    "tests.test_light_generated.TestWorld.test_property_one"
                ),
                self.property(
                    "tests.test_light_generated.TestWorld.test_property_two"
                ),
            ),
        )
        dense = FuzzModuleManifest(
            module_name="tests.test_dense_generated",
            test_ids=tuple(
                f"tests.test_dense_generated.TestWorld.test_property_{index}"
                for index in range(4)
            ),
            hypothesis_tests=tuple(
                self.property(
                    f"tests.test_dense_generated.TestWorld.test_property_{index}"
                )
                for index in range(4)
            ),
        )

        targets = build_fuzz_targets((light, dense))

        assert_exact_fuzz_coverage((light, dense), targets)
        self.assertTrue(
            all(
                target.module_name == dense.module_name
                for target in targets[:4]
            )
        )

    def test_audited_hotspot_precedes_an_ordinary_property_dense_module(
        self,
    ) -> None:
        hotspot_name = "tests.test_beets_destructive_configs_generated"
        hotspot_property = f"{hotspot_name}.TestWorld.test_property"
        hotspot = FuzzModuleManifest(
            module_name=hotspot_name,
            test_ids=(
                hotspot_property,
                f"{hotspot_name}.TestWorld.test_pin",
            ),
            hypothesis_tests=(self.property(hotspot_property),),
        )
        ordinary_name = "tests.test_dense_generated"
        ordinary_ids = tuple(
            f"{ordinary_name}.TestWorld.test_property_{index}"
            for index in range(8)
        )
        ordinary = FuzzModuleManifest(
            module_name=ordinary_name,
            test_ids=ordinary_ids,
            hypothesis_tests=tuple(
                self.property(test_id) for test_id in ordinary_ids
            ),
        )

        targets = build_fuzz_targets((ordinary, hotspot))

        self.assertEqual(targets[0].module_name, hotspot_name)
        self.assertTrue(
            all(
                target.module_name == hotspot_name
                for target in targets
                if target.module_name == hotspot_name
            )
        )

    def test_postgres_resource_flag_reaches_every_module_target(self) -> None:
        property_id = "tests.test_pg_generated.TestWorld.test_property"
        manifest = FuzzModuleManifest(
            module_name="tests.test_pg_generated",
            test_ids=(
                property_id,
                "tests.test_pg_generated.TestWorld.test_pin",
            ),
            hypothesis_tests=(self.property(property_id),),
            uses_ephemeral_postgres=True,
        )

        targets = build_fuzz_targets((manifest,), property_shards=2)

        self.assertTrue(targets)
        self.assertTrue(
            all(target.uses_ephemeral_postgres for target in targets)
        )

    def test_postgres_limit_bypasses_blocked_pg_targets_for_ordinary_work(
        self,
    ) -> None:
        active = (
            FuzzTarget(
                label="active-pg",
                module_name="active-pg",
                load_names=("active-pg",),
                expected_test_ids=("active-pg.test",),
                uses_ephemeral_postgres=True,
            ),
        )
        pending = (
            FuzzTarget(
                label="pending-pg-one",
                module_name="pending-pg-one",
                load_names=("pending-pg-one",),
                expected_test_ids=("pending-pg-one.test",),
                uses_ephemeral_postgres=True,
            ),
            FuzzTarget(
                label="pending-pg-two",
                module_name="pending-pg-two",
                load_names=("pending-pg-two",),
                expected_test_ids=("pending-pg-two.test",),
                uses_ephemeral_postgres=True,
            ),
            FuzzTarget(
                label="ordinary",
                module_name="ordinary",
                load_names=("ordinary",),
                expected_test_ids=("ordinary.test",),
            ),
        )

        admitted = select_fuzz_admissions(
            pending,
            active,
            worker_count=3,
            postgres_worker_count=1,
        )

        assert_fuzz_admission(
            pending,
            active,
            admitted,
            worker_count=3,
            postgres_worker_count=1,
        )
        self.assertEqual(admitted, (2,))
        self.assertEqual(EPHEMERAL_POSTGRES_TARGET_LIMIT, 24)

    def test_idle_postgres_lane_is_filled_before_ordinary_capacity(self) -> None:
        pending = tuple(
            FuzzTarget(
                label=f"ordinary-{index}",
                module_name=f"ordinary-{index}",
                load_names=(f"ordinary-{index}",),
                expected_test_ids=(f"ordinary-{index}.test",),
            )
            for index in range(3)
        ) + (
            FuzzTarget(
                label="postgres",
                module_name="postgres",
                load_names=("postgres",),
                expected_test_ids=("postgres.test",),
                uses_ephemeral_postgres=True,
            ),
        )

        admitted = select_fuzz_admissions(
            pending,
            (),
            worker_count=3,
            postgres_worker_count=1,
        )

        self.assertEqual(len(admitted), 3)
        self.assertIn(3, admitted)

    def test_admission_checker_rejects_an_idle_postgres_lane(self) -> None:
        pending = tuple(
            FuzzTarget(
                label=f"ordinary-{index}",
                module_name=f"ordinary-{index}",
                load_names=(f"ordinary-{index}",),
                expected_test_ids=(f"ordinary-{index}.test",),
            )
            for index in range(2)
        ) + (
            FuzzTarget(
                label="postgres",
                module_name="postgres",
                load_names=("postgres",),
                expected_test_ids=("postgres.test",),
                uses_ephemeral_postgres=True,
            ),
        )

        with self.assertRaisesRegex(AssertionError, "PostgreSQL lane idle"):
            assert_fuzz_admission(
                pending,
                (),
                (0, 1),
                worker_count=2,
                postgres_worker_count=1,
            )

    def test_30_core_500_example_profile_uses_two_entropy_shards(self) -> None:
        self.assertEqual(recommended_property_shards(30, 500), 2)

    def test_30_core_overnight_profile_keeps_eight_entropy_shards(self) -> None:
        self.assertEqual(recommended_property_shards(30, 20_000), 8)

    def test_30_core_host_defaults_to_60_by_24_concurrency(self) -> None:
        workers = recommended_fuzz_jobs(30)

        self.assertEqual(workers, 60)
        self.assertEqual(recommended_postgres_jobs(30, workers), 24)

    def test_worker_formula_is_capped_regardless_of_host_size(self) -> None:
        """Issue #1214 "Contributing gaps" item 1 (independent review F5(b):
        NOT #1156 item 1, a different, unrelated finding): recommended_
        fuzz_jobs had no ceiling; MAX_FUZZ_JOBS bounds it on an arbitrarily
        large host without moving today's measured 30-core number (60,
        well under the 64 ceiling)."""
        self.assertEqual(recommended_fuzz_jobs(40), MAX_FUZZ_JOBS)
        self.assertLess(recommended_fuzz_jobs(40), 40 * 2)
        self.assertEqual(recommended_fuzz_jobs(1000), MAX_FUZZ_JOBS)

    def test_discovered_profile_budget_ignores_explicit_properties(self) -> None:
        manifest = FuzzModuleManifest(
            module_name="tests.test_generated_world",
            test_ids=("default", "explicit"),
            hypothesis_tests=(
                FuzzPropertyManifest(
                    test_id="default",
                    max_examples=500,
                    uses_default_settings=True,
                ),
                FuzzPropertyManifest(
                    test_id="explicit",
                    max_examples=20_000,
                    uses_default_settings=False,
                ),
            ),
        )

        self.assertEqual(property_profile_max_examples((manifest,)), 500)

    def test_discovered_profile_budget_must_be_consistent(self) -> None:
        manifest = FuzzModuleManifest(
            module_name="tests.test_generated_world",
            test_ids=("first", "second"),
            hypothesis_tests=(
                FuzzPropertyManifest(
                    test_id="first",
                    max_examples=500,
                    uses_default_settings=True,
                ),
                FuzzPropertyManifest(
                    test_id="second",
                    max_examples=501,
                    uses_default_settings=True,
                ),
            ),
        )

        with self.assertRaisesRegex(ValueError, "inconsistent default"):
            property_profile_max_examples((manifest,))

    def test_generated_state_machine_inherits_the_profile_budget(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            manifest = discover_fuzz_manifests(
                ("tests.test_request_lifecycle_generated",),
                worker_count=1,
                environment={
                    **os.environ,
                    "CRATEDIGGER_HYPOTHESIS_PROFILE": "suite",
                    "PYTHONPATH": str(REPO_ROOT),
                },
                work_directory=Path(tempdir),
            )[0]

        state_machine = next(
            item
            for item in manifest.hypothesis_tests
            if item.test_id.startswith("hypothesis.stateful.")
        )
        self.assertTrue(state_machine.uses_default_settings)
        self.assertEqual(state_machine.max_examples, 150)

    def test_real_pg_module_discovery_records_the_resource(self) -> None:
        environment = {
            **os.environ,
            "CRATEDIGGER_HYPOTHESIS_PROFILE": "suite",
            "PYTHONPATH": str(REPO_ROOT),
        }
        environment.pop("TEST_DB_DSN", None)
        environment.pop("CRATEDIGGER_TEST_SCHEMA_READY", None)
        with tempfile.TemporaryDirectory() as tempdir:
            manifest = discover_fuzz_manifests(
                ("tests.test_cleanup_journal_generated",),
                worker_count=1,
                environment=environment,
                work_directory=Path(tempdir),
            )[0]

        self.assertTrue(manifest.uses_ephemeral_postgres)
        self.assertNotIn("TEST_DB_DSN", environment)


class TestFuzzTargetsMidRunHeadroom(unittest.TestCase):
    """``run_fuzz_targets``'s own admission-loop headroom check (issue
    #1156 item 3) -- distinct from the preflight check ``main`` also runs
    (covered end-to-end in ``TestFuzzRunnerProcess`` below). Both call
    sites use the identical real ``_check_suite_headroom`` measurement
    against the same shared tmpfs, so a static real-disk threshold cannot
    tell them apart; a controlled ``check_headroom`` fake proves the loop
    consults it a SECOND time, mid-run, not just once at the top.
    """

    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tempdir.cleanup)
        self.root = Path(self.tempdir.name)
        package = self.root / "midrun_fixture"
        package.mkdir()
        (package / "__init__.py").write_text("", encoding="utf-8")
        (package / "test_pins.py").write_text(
            "import unittest\n\n"
            "class PinWorld(unittest.TestCase):\n"
            "    def test_a(self):\n"
            "        self.assertTrue(True)\n\n"
            "    def test_b(self):\n"
            "        self.assertTrue(True)\n\n"
            "    def test_c(self):\n"
            "        self.assertTrue(True)\n",
            encoding="utf-8",
        )
        self.module = "midrun_fixture.test_pins"
        self.log_directory = self.root / "logs"
        self.log_directory.mkdir()
        self.environment = {
            **os.environ,
            "PYTHONPATH": os.pathsep.join(
                (str(self.root), str(REPO_ROOT), os.environ.get("PYTHONPATH", ""))
            ),
        }

    def _target(self, letter: str) -> FuzzTarget:
        return FuzzTarget(
            label=letter,
            module_name=self.module,
            load_names=(self.module,),
            expected_test_ids=(f"{self.module}.PinWorld.test_{letter}",),
        )

    def test_trip_aborts_admission_without_starting_the_rest(self) -> None:
        calls = {"count": 0}

        def flaky_check_headroom() -> None:
            calls["count"] += 1
            if calls["count"] >= 2:
                raise RamRootExhaustedError(
                    f"{TEST_RAM_ROOT_EXHAUSTED}: synthetic mid-run trip"
                )

        targets = (self._target("a"), self._target("b"), self._target("c"))

        batch = run_fuzz_targets(
            targets,
            worker_count=1,
            postgres_worker_count=1,
            environment=self.environment,
            log_directory=self.log_directory,
            check_headroom=flaky_check_headroom,
        )

        self.assertTrue(batch.headroom_exhausted)
        self.assertEqual(batch.infrastructure_failures, ())
        self.assertEqual(len(batch.results), 1)
        self.assertTrue(batch.results[0].successful)
        self.assertEqual(len(batch.not_started), 2)
        self.assertGreaterEqual(calls["count"], 2)

    def test_never_tripping_runs_every_target_normally(self) -> None:
        targets = (self._target("a"), self._target("b"), self._target("c"))

        batch = run_fuzz_targets(
            targets,
            worker_count=2,
            postgres_worker_count=2,
            environment=self.environment,
            log_directory=self.log_directory,
            check_headroom=lambda: None,
        )

        self.assertFalse(batch.headroom_exhausted)
        self.assertEqual(batch.infrastructure_failures, ())
        self.assertEqual(len(batch.results), 3)
        self.assertTrue(all(result.successful for result in batch.results))
        self.assertEqual(batch.not_started, ())

    def test_drain_phase_never_calls_check_headroom_once_pending_is_empty(
        self,
    ) -> None:
        """Independent review F1 (BLOCKING): the mid-run check used to sit
        inside ``if not infrastructure_aborted:`` with no ``pending`` gate,
        so it kept firing on every drain iteration after the LAST target
        was already admitted -- when there is nothing left to admit. A
        free-space dip at that exact moment reported a fully green burst
        ("2 completed of 2, 0 not started") as an infrastructure failure.
        worker_count >= len(targets) admits everything in the first
        iteration; the poison pill below (a RamRootExhaustedError on every
        call after the first) proves check_headroom is never consulted
        again during the drain that follows.
        """
        calls = {"count": 0}

        def check_headroom() -> None:
            calls["count"] += 1
            if calls["count"] > 1:
                raise RamRootExhaustedError(
                    f"{TEST_RAM_ROOT_EXHAUSTED}: must never fire once "
                    "pending is empty"
                )

        targets = (self._target("a"), self._target("b"), self._target("c"))

        batch = run_fuzz_targets(
            targets,
            worker_count=3,
            postgres_worker_count=3,
            environment=self.environment,
            log_directory=self.log_directory,
            check_headroom=check_headroom,
        )

        self.assertFalse(batch.headroom_exhausted)
        self.assertEqual(batch.infrastructure_failures, ())
        self.assertEqual(len(batch.results), 3)
        self.assertTrue(all(result.successful for result in batch.results))
        self.assertEqual(batch.not_started, ())


class TestFuzzMainMidRunHeadroom(unittest.TestCase):
    """``main``'s own POST-run reporting of a mid-run trip (issue #1156
    item 3) -- distinct from TestFuzzTargetsMidRunHeadroom above, which
    only proves ``run_fuzz_targets``'s admission loop stops correctly.
    This drives the real CLI entry point in-process (the ``check_headroom``
    DI seam mirrors ``run_world_model_burst.py::main``'s own) so the
    unified failed_results/infrastructure_failures/headroom_exhausted
    reporting block -- the code that used to silently drop other targets'
    detail when it early-returned before printing them -- is exercised
    end-to-end, not just unit-tested in isolation.
    """

    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tempdir.cleanup)
        self.root = Path(self.tempdir.name)
        package = self.root / "main_midrun_fixture"
        package.mkdir()
        (package / "__init__.py").write_text("", encoding="utf-8")
        (package / "test_alpha.py").write_text(
            "import unittest\n\n"
            "class Alpha(unittest.TestCase):\n"
            "    def test_pin(self):\n"
            "        self.assertTrue(True)\n",
            encoding="utf-8",
        )
        (package / "test_beta.py").write_text(
            "import unittest\n\n"
            "class Beta(unittest.TestCase):\n"
            "    def test_pin(self):\n"
            "        self.assertTrue(True)\n",
            encoding="utf-8",
        )
        self.module_a = "main_midrun_fixture.test_alpha"
        self.module_b = "main_midrun_fixture.test_beta"
        self.database = self.root / "database"
        self.database.mkdir()
        original_pythonpath = os.environ.get("PYTHONPATH")
        os.environ["PYTHONPATH"] = os.pathsep.join(
            (str(self.root), original_pythonpath or "")
        )
        original_hsd = os.environ.pop("HYPOTHESIS_STORAGE_DIRECTORY", None)
        os.environ["HYPOTHESIS_STORAGE_DIRECTORY"] = str(self.database)
        original_output_dir = os.environ.pop("CRATEDIGGER_FUZZ_OUTPUT_DIR", None)

        def _restore() -> None:
            if original_pythonpath is None:
                os.environ.pop("PYTHONPATH", None)
            else:
                os.environ["PYTHONPATH"] = original_pythonpath
            if original_hsd is None:
                os.environ.pop("HYPOTHESIS_STORAGE_DIRECTORY", None)
            else:
                os.environ["HYPOTHESIS_STORAGE_DIRECTORY"] = original_hsd
            if original_output_dir is not None:
                os.environ["CRATEDIGGER_FUZZ_OUTPUT_DIR"] = original_output_dir

        self.addCleanup(_restore)

    def test_main_reports_a_mid_run_trip_through_the_ordinary_failure_path(
        self,
    ) -> None:
        """Independent review B4 (HIGH): the fake used to raise
        ``f"{TEST_RAM_ROOT_EXHAUSTED}: synthetic mid-run trip"`` -- an
        identity string ALREADY embedded in the exception's own message --
        so a loose ``assertIn`` matched ``run_fuzz_targets``'s OWN separate
        admission-loop print (`scripts/run_fuzz_tests.py` line ~959,
        ``print(f"RAM ROOT EXHAUSTED mid-run: {exc}")``) regardless of
        whether ``main()``'s OWN reporting block (lines ~1473-1499) ever
        ran at all. The fake below deliberately carries NEITHER
        ``TEST_RAM_ROOT_EXHAUSTED`` NOR the word "mid-run", so the
        assertions can only pass via strings ``main()`` itself composes."""
        calls = {"count": 0}

        def flaky_check_headroom() -> None:
            calls["count"] += 1
            if calls["count"] >= 3:
                raise RamRootExhaustedError("xyzzy-unrelated-probe-marker")

        stdout = io.StringIO()
        with contextlib.redirect_stdout(stdout):
            status = main(
                (self.module_a, self.module_b, "--jobs", "1", "--profile", "suite"),
                check_headroom=flaky_check_headroom,
            )
        output = stdout.getvalue()

        self.assertEqual(status, TEST_RAM_ROOT_EXHAUSTED_EXIT_CODE, output)
        # main()'s own header line (scripts/run_fuzz_tests.py:1474) -- no
        # other print site in the module emits this exact banner.
        self.assertIn(f"--- {TEST_RAM_ROOT_EXHAUSTED} (mid-run) ---", output)
        # main()'s own unique explanatory sentence (line ~1476-1478) --
        # not duplicated by run_fuzz_targets's admission-loop print.
        self.assertIn(
            "already-running targets were drained and no new target "
            "was started",
            output,
        )
        # main()'s own terminal summary line (line ~1494-1499).
        self.assertIn("property verdict invalid", output)
        self.assertGreaterEqual(calls["count"], 3)

    def test_unavailable_runtime_dir_aborts_cleanly_not_as_a_raw_traceback(
        self,
    ) -> None:
        """Independent review F3 (MEDIUM): private_runtime_dir() used to be
        called ONE LINE ABOVE the try/except that exists to catch its own
        RuntimeError, so it escaped as an unhandled traceback instead of
        the intended "infrastructure precondition failed" exit 2. Drives
        the REAL production private_runtime_dir() (check_headroom=None,
        the default -- no injected fake), pointed at a real, non-tmpfs
        directory via XDG_RUNTIME_DIR, matching test-fidelity Rule B: the
        fake in test_preflight_non_ram_root_error_returns_two only proves
        the except clause's own handling, never the real raise site.
        """
        # dir="/var/tmp" deliberately: TMPDIR is itself tmpfs inside this
        # nix-shell (scripts/test_tmpfs.sh's own shell-entry scratch), so a
        # bare tempfile.TemporaryDirectory() would land ON tmpfs and never
        # trip the check this test exists to prove. /var/tmp is real
        # disk-backed storage (ext4 on doc1 and in this worktree),
        # guaranteed non-tmpfs -- AND, unlike an earlier version of this
        # test that used dir=REPO_ROOT, it is NOT inside the git-tracked
        # checkout (independent review B9): a hard kill mid-test (the
        # TemporaryDirectory context manager's own cleanup never runs on
        # SIGKILL) used to risk leaving a stray directory inside the repo
        # worktree; /var/tmp is real disk but outside anything git tracks.
        with tempfile.TemporaryDirectory(dir="/var/tmp") as fake_runtime_dir:
            original = os.environ.get("XDG_RUNTIME_DIR")
            os.environ["XDG_RUNTIME_DIR"] = fake_runtime_dir
            try:
                stdout = io.StringIO()
                stderr = io.StringIO()
                with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(
                    stderr
                ):
                    status = main((self.module_a, "--jobs", "1", "--profile", "suite"))
            finally:
                if original is None:
                    os.environ.pop("XDG_RUNTIME_DIR", None)
                else:
                    os.environ["XDG_RUNTIME_DIR"] = original
            output = stdout.getvalue() + stderr.getvalue()

        self.assertEqual(status, 2, output)
        self.assertIn("infrastructure precondition failed", output)
        self.assertIn("not tmpfs", output)


class TestFuzzProfileBudget(unittest.TestCase):
    def _profile_budget(self, override: str | None) -> int:
        env = os.environ.copy()
        if override is None:
            env.pop("CRATEDIGGER_FUZZ_MAX_EXAMPLES", None)
        else:
            env["CRATEDIGGER_FUZZ_MAX_EXAMPLES"] = override
        completed = subprocess.run(
            [
                sys.executable,
                "-c",
                (
                    "import tests._hypothesis_profiles; "
                    "from hypothesis import settings; "
                    "print(settings.get_profile('fuzz').max_examples)"
                ),
            ],
            cwd=REPO_ROOT,
            env=env,
            capture_output=True,
            text=True,
            check=False,
        )

        self.assertEqual(completed.returncode, 0, completed.stderr)
        return int(completed.stdout)

    def test_default_fuzz_budget_is_500_examples(self) -> None:
        self.assertEqual(self._profile_budget(None), 500)

    def test_fuzz_budget_honors_the_overnight_override(self) -> None:
        self.assertEqual(self._profile_budget("20000"), 20_000)


def _shard(
    test_id: str,
    *,
    budget: int,
    valid: int,
    invalid: int = 0,
    overrun: int = 0,
    interesting: int = 0,
    stopped_because: str = STRATEGY_SPACE_EXHAUSTED,
) -> HypothesisPropertyStats:
    """One child's statistics, in the shape the real child encodes."""
    return HypothesisPropertyStats(
        test_id=test_id,
        max_examples=budget,
        valid=valid,
        invalid=invalid,
        overrun=overrun,
        interesting=interesting,
        stopped_because=stopped_because,
    )


class TestBurstDepthReport(unittest.TestCase):
    """Per-property depth disclosure (#888 item 1)."""

    PROPERTY = "tests.test_example_generated.TestWorld.test_property"

    def test_entropy_shards_are_aggregated_before_depth_is_judged(self) -> None:
        """One property split eight ways is one row, judged once.

        Folding does not change WHICH properties are flagged — it de-duplicates
        eight identical rows out of the ranked list and reports the property's
        real total budget against one distinct-world bound.
        """
        records = tuple(
            _shard(self.PROPERTY, budget=2_500, valid=4) for _ in range(8)
        )

        depths = aggregate_property_depth(records)

        self.assertEqual(
            depths,
            (
                PropertyDepth(
                    test_id=self.PROPERTY,
                    budget=20_000,
                    shard_budget_bound=2_500,
                    shards=8,
                    exhausted_shards=8,
                    distinct_world_bound=4,
                    valid=32,
                    invalid=0,
                    overrun=0,
                    interesting=0,
                ),
            ),
        )
        self.assertTrue(is_structurally_shallow(depths[0]))
        self.assertEqual(
            [line for line in format_depth_report(depths) if "SHALLOW" in line],
            [
                ("SHALLOW 4 worlds vs 2500 examples per shard "
                f"(8 shards, 20000 total) {self.PROPERTY}")
            ],
        )

    def test_a_single_shard_line_names_one_shard(self) -> None:
        depths = aggregate_property_depth(
            (_shard(self.PROPERTY, budget=50, valid=4),)
        )

        self.assertEqual(
            [line for line in format_depth_report(depths) if "SHALLOW" in line],
            [
                ("SHALLOW 4 worlds vs 50 examples per shard "
                f"(1 shard, 50 total) {self.PROPERTY}")
            ],
        )

    def test_a_property_that_spends_its_whole_budget_is_not_shallow(self) -> None:
        depth = aggregate_property_depth(
            (
                _shard(
                    self.PROPERTY,
                    budget=150,
                    valid=150,
                    stopped_because="settings.max_examples=150",
                ),
            )
        )[0]

        self.assertFalse(is_structurally_shallow(depth))

    def test_an_exactly_exhausted_budget_is_not_shallow(self) -> None:
        """Exhausting a space that is exactly the budget wastes nothing."""
        depth = aggregate_property_depth(
            (_shard(self.PROPERTY, budget=4, valid=4),)
        )[0]

        self.assertFalse(is_structurally_shallow(depth))

    def test_one_unexhausted_shard_withholds_the_shallow_verdict(self) -> None:
        depths = aggregate_property_depth(
            (
                _shard(self.PROPERTY, budget=2_500, valid=4),
                _shard(
                    self.PROPERTY,
                    budget=2_500,
                    valid=2_500,
                    stopped_because="settings.max_examples=2500",
                ),
            )
        )

        self.assertEqual(depths[0].exhausted_shards, 1)
        self.assertEqual(depths[0].shards, 2)
        self.assertFalse(is_structurally_shallow(depths[0]))

    def test_discard_rate_measures_assume_cost_against_spent_examples(self) -> None:
        depth = aggregate_property_depth(
            (
                _shard(
                    self.PROPERTY,
                    budget=150,
                    valid=150,
                    invalid=150,
                    stopped_because="settings.max_examples=150",
                ),
            )
        )[0]

        self.assertEqual(discard_rate(depth), 0.5)
        self.assertEqual(
            [line for line in format_depth_report((depth,)) if "DISCARD" in line],
            [
                (f"DISCARD 50% of 300 examples (150 discarded, 150 worlds) "
                f"{self.PROPERTY}")
            ],
        )

    def test_a_property_that_ran_nothing_has_no_discard_rate(self) -> None:
        depth = aggregate_property_depth(
            (_shard(self.PROPERTY, budget=150, valid=0, stopped_because=""),)
        )[0]

        self.assertEqual(discard_rate(depth), 0.0)

    def test_shallow_properties_rank_by_world_count_and_truncate(self) -> None:
        records = tuple(
            _shard(f"{self.PROPERTY}_{index:02d}", budget=20_000, valid=index)
            for index in range(DEPTH_REPORT_LIMIT + 5)
        )

        lines = format_depth_report(aggregate_property_depth(records))

        self.assertEqual(
            lines[0],
            f"DEPTH {DEPTH_REPORT_LIMIT + 5} properties measured, "
            f"{DEPTH_REPORT_LIMIT + 5} shallow (space exhausted below budget), "
            "0 discarding at least 10% of their examples",
        )
        self.assertEqual(
            [line.split()[1] for line in lines[1 : DEPTH_REPORT_LIMIT + 1]],
            [str(index) for index in range(DEPTH_REPORT_LIMIT)],
        )
        self.assertEqual(lines[-1], "SHALLOW ... 5 more shallow")

    def test_discarding_properties_rank_by_rate_and_truncate(self) -> None:
        records = tuple(
            _shard(
                f"{self.PROPERTY}_{index:02d}",
                budget=150,
                valid=150,
                invalid=20 + index,
                stopped_because="settings.max_examples=150",
            )
            for index in range(DEPTH_REPORT_LIMIT + 3)
        )

        lines = format_depth_report(aggregate_property_depth(records))
        discard_lines = [
            line for line in lines if line.startswith("DISCARD ") and "..." not in line
        ]

        self.assertEqual(
            lines[0],
            f"DEPTH {DEPTH_REPORT_LIMIT + 3} properties measured, "
            "0 shallow (space exhausted below budget), "
            f"{DEPTH_REPORT_LIMIT + 3} discarding at least 10% of their examples",
        )
        self.assertEqual(
            [line.split()[-1][-2:] for line in discard_lines],
            [
                f"{index:02d}"
                for index in range(
                    DEPTH_REPORT_LIMIT + 2,
                    DEPTH_REPORT_LIMIT + 2 - DEPTH_REPORT_LIMIT,
                    -1,
                )
            ],
        )
        self.assertEqual(lines[-1], "DISCARD ... 3 more discarding")

    def test_a_burst_without_properties_reports_nothing(self) -> None:
        self.assertEqual(format_depth_report(aggregate_property_depth(())), ())


class TestFuzzRunnerProcess(unittest.TestCase):
    def setUp(self) -> None:
        # Independent review B1 (BLOCKING): main()'s production default
        # (check_headroom=None) now calls the REAL private_runtime_dir()/
        # _check_suite_headroom() with a flat 1 GiB floor. Every subprocess
        # test in this class that does not deliberately test headroom
        # inherited a live ambient-free-space precondition, coupling them
        # to whatever the shared tmpfs root happens to have free at test
        # time -- reproduced live (4/460 targets failed on doc1 with
        # 1002692608 bytes free, needs 1073741824). Pin the floor to "0"
        # (always satisfied, real code path, no DI) for the whole class;
        # the one test that is actually ABOUT headroom
        # (test_preflight_headroom_exhaustion_aborts_before_any_discovery)
        # overrides it back to an impossible value in its OWN env dict,
        # which wins over this class-level default.
        self._original_ram_min_bytes = os.environ.get(
            "CRATEDIGGER_TEST_RAM_MIN_BYTES"
        )
        os.environ["CRATEDIGGER_TEST_RAM_MIN_BYTES"] = "0"
        self.addCleanup(self._restore_ram_min_bytes)
        self.tempdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tempdir.cleanup)
        self.root = Path(self.tempdir.name)
        package = self.root / "fuzz_fixture"
        package.mkdir()
        (package / "__init__.py").write_text("", encoding="utf-8")
        (package / "test_example_generated.py").write_text(
            "import os\n"
            "import unittest\n"
            "from pathlib import Path\n"
            "from hypothesis import given, settings\n"
            "from hypothesis import strategies as st\n"
            "import tests._hypothesis_profiles\n\n"
            "class ExampleWorld(unittest.TestCase):\n"
            "    @settings(max_examples=3, deadline=None)\n"
            "    @given(value=st.integers())\n"
            "    def test_property_one(self, value):\n"
            "        count_dir = Path(os.environ['FUZZ_COUNT_DIR'])\n"
            "        with (count_dir / 'property-one-count').open('ab') as stream:\n"
            "            stream.write(b'x')\n"
            "        self.assertEqual(value, value)\n\n"
            "    @settings(max_examples=3, deadline=None)\n"
            "    @given(value=st.text(max_size=3))\n"
            "    def test_property_two(self, value):\n"
            "        count_dir = Path(os.environ['FUZZ_COUNT_DIR'])\n"
            "        with (count_dir / 'property-two-count').open('ab') as stream:\n"
            "            stream.write(b'x')\n"
            "        self.assertEqual(value, value)\n\n"
            "    def test_pin(self):\n"
            "        database = Path(os.environ['HYPOTHESIS_STORAGE_DIRECTORY'])\n"
            "        database.mkdir(parents=True, exist_ok=True)\n"
            "        (database / 'active-marker').write_text('active')\n"
            "        print(f'active-database={database}')\n"
            "        if os.environ.get('FUZZ_FIXTURE_FAIL') == '1':\n"
            "            self.fail('fuzz-log-marker')\n",
            encoding="utf-8",
        )
        (package / "test_external_generated.py").write_text(
            "import unittest\n"
            "from hypothesis import given\n"
            "from hypothesis import strategies as st\n"
            "import tests._hypothesis_profiles\n\n"
            "class ExternalWorld(unittest.TestCase):\n"
            "    def id(self):\n"
            "        return 'hypothesis.stateful.ExternalWorld.TestCase.runTest'\n\n"
            "    @given(value=st.integers())\n"
            "    def test_property(self, value):\n"
            "        self.assertEqual(value, value)\n",
            encoding="utf-8",
        )
        (package / "test_mixed_generated.py").write_text(
            "import unittest\n"
            "from hypothesis import given, settings\n"
            "from hypothesis import strategies as st\n"
            "from hypothesis.stateful import RuleBasedStateMachine, rule\n"
            "import tests._hypothesis_profiles\n\n"
            "class MixedWorld(unittest.TestCase):\n"
            "    @settings(max_examples=50, database=None)\n"
            "    @given(value=st.booleans())\n"
            "    def test_property(self, value):\n"
            "        self.assertIsInstance(value, bool)\n\n"
            "    def test_pin(self):\n"
            "        self.assertTrue(True)\n\n"
            "    def test_pin_running_an_inner_property(self):\n"
            "        @settings(max_examples=50, database=None)\n"
            "        @given(first=st.booleans(), second=st.booleans())\n"
            "        def planted(first, second):\n"
            "            raise AssertionError('planted')\n"
            "        with self.assertRaises(AssertionError):\n"
            "            planted()\n\n"
            "@settings(max_examples=5, stateful_step_count=3,\n"
            "          database=None, deadline=None)\n"
            "class Machine(RuleBasedStateMachine):\n"
            "    @rule(value=st.integers())\n"
            "    def step(self, value):\n"
            "        assert isinstance(value, int)\n\n"
            "TestMachine = Machine.TestCase\n",
            encoding="utf-8",
        )
        (package / "test_shallow_generated.py").write_text(
            "import unittest\n"
            "from hypothesis import given, settings\n"
            "from hypothesis import strategies as st\n"
            "import tests._hypothesis_profiles\n\n"
            "class ShallowWorld(unittest.TestCase):\n"
            "    @settings(max_examples=50, database=None)\n"
            "    @given(first=st.booleans(), second=st.booleans())\n"
            "    def test_property(self, first, second):\n"
            "        self.assertIsInstance(first, bool)\n"
            "        self.assertIsInstance(second, bool)\n",
            encoding="utf-8",
        )
        (package / "test_capacity_generated.py").write_text(
            "import errno\n"
            "import os\n"
            "import time\n"
            "import unittest\n"
            "from pathlib import Path\n"
            "from types import SimpleNamespace\n"
            "from unittest.mock import patch\n"
            "from hypothesis import given, settings\n"
            "from hypothesis import strategies as st\n"
            "from psycopg2 import OperationalError\n"
            "from psycopg2.errors import DiskFull\n"
            "import tests._hypothesis_profiles\n\n"
            "class CapacityWorld(unittest.TestCase):\n"
            "    @settings(max_examples=1, database=None, deadline=None)\n"
            "    @given(value=st.none())\n"
            "    def test_a_capacity_failure(self, value):\n"
            "        self.assertIsNone(value)\n"
            "        database = Path(os.environ['HYPOTHESIS_STORAGE_DIRECTORY'])\n"
            "        (database / 'invalid-infrastructure-marker').write_text('x')\n"
            "        if os.environ['FUZZ_INFRA_KIND'] == 'postgres':\n"
            "            raise DiskFull('could not extend file: No space left')\n"
            "        if os.environ['FUZZ_INFRA_KIND'] == 'database':\n"
            "            raise OperationalError('ephemeral database stopped')\n"
            "        if os.environ['FUZZ_INFRA_KIND'] == 'swallowed':\n"
            "            patch('shutil.disk_usage', "
            "return_value=SimpleNamespace(free=1)).start()\n"
            "            self.fail('preview swallowed ENOSPC')\n"
            "        if os.environ['FUZZ_INFRA_KIND'] == 'subtest':\n"
            "            with self.subTest(stage='snapshot'):\n"
            "                raise OSError(errno.ENOSPC, "
            "'No space left on device')\n"
            "            return\n"
            "        raise OSError(errno.ENOSPC, 'No space left on device')\n\n"
            "    @settings(max_examples=1, database=None, deadline=None)\n"
            "    @given(value=st.none())\n"
            "    def test_b_contaminated_failure(self, value):\n"
            "        self.assertIsNone(value)\n"
            "        time.sleep(1)\n"
            "        self.fail('must not become a property report')\n\n"
            "    @settings(max_examples=1, database=None, deadline=None)\n"
            "    @given(value=st.none())\n"
            "    def test_z_pending_target(self, value):\n"
            "        self.assertIsNone(value)\n"
            "        print('pending-target-ran')\n",
            encoding="utf-8",
        )
        self.module = "fuzz_fixture.test_example_generated"
        self.output_dir = self.root / "failures"
        self.database = self.root / "persistent-database"
        self.database.mkdir()
        (self.database / "seed-marker").write_text("seed", encoding="utf-8")

    def _restore_ram_min_bytes(self) -> None:
        if self._original_ram_min_bytes is None:
            os.environ.pop("CRATEDIGGER_TEST_RAM_MIN_BYTES", None)
        else:
            os.environ["CRATEDIGGER_TEST_RAM_MIN_BYTES"] = self._original_ram_min_bytes

    def run_burst(self, *, failing: bool) -> subprocess.CompletedProcess[str]:
        env = {
            **os.environ,
            "PYTHONPATH": os.pathsep.join(
                (str(self.root), str(REPO_ROOT), os.environ.get("PYTHONPATH", ""))
            ),
            "HYPOTHESIS_STORAGE_DIRECTORY": str(self.database),
            "CRATEDIGGER_FUZZ_OUTPUT_DIR": str(self.output_dir),
            "FUZZ_FIXTURE_FAIL": "1" if failing else "0",
            "FUZZ_COUNT_DIR": str(self.root),
        }
        return subprocess.run(
            [
                sys.executable,
                str(RUNNER),
                "--jobs",
                "2",
                "--profile",
                "suite",
                self.module,
            ],
            cwd=REPO_ROOT,
            env=env,
            capture_output=True,
            text=True,
            check=False,
        )

    def test_green_run_discards_active_logs_and_database_writes(self) -> None:
        completed = self.run_burst(failing=False)

        self.assertEqual(completed.returncode, 0, completed.stdout + completed.stderr)
        self.assertIn("3 targets", completed.stdout)
        self.assertIn("ALL GREEN", completed.stdout)
        self.assertEqual(
            sorted(path.name for path in self.database.iterdir()),
            ["seed-marker"],
        )
        self.assertFalse(self.output_dir.exists())

    def test_failure_persists_logs_and_replay_database(self) -> None:
        completed = self.run_burst(failing=True)

        self.assertNotEqual(completed.returncode, 0)
        run_directories = list(self.output_dir.glob("run.*"))
        self.assertEqual(len(run_directories), 1)
        combined_logs = "\n".join(
            path.read_text(encoding="utf-8")
            for path in run_directories[0].rglob("*.log")
        )
        self.assertIn("fuzz-log-marker", combined_logs)
        self.assertIn(
            f"active-database={tempfile.gettempdir()}/",
            combined_logs,
        )
        self.assertTrue(
            {"active-marker", "seed-marker"}.issubset(
                path.name for path in self.database.iterdir()
            )
        )
        self.assertIn(str(run_directories[0]), completed.stdout)

    def test_capacity_errors_abort_without_becoming_property_failures(
        self,
    ) -> None:
        for kind in (
            "filesystem",
            "postgres",
            "database",
            "swallowed",
            "subtest",
        ):
            with self.subTest(kind=kind):
                env = {
                    **os.environ,
                    "PYTHONPATH": os.pathsep.join(
                        (
                            str(self.root),
                            str(REPO_ROOT),
                            os.environ.get("PYTHONPATH", ""),
                        )
                    ),
                    "HYPOTHESIS_STORAGE_DIRECTORY": str(self.database),
                    "FUZZ_INFRA_KIND": kind,
                }
                completed = subprocess.run(
                    [
                        sys.executable,
                        str(RUNNER),
                        "--jobs",
                        "2",
                        "--profile",
                        "suite",
                        "fuzz_fixture.test_capacity_generated",
                    ],
                    cwd=REPO_ROOT,
                    env=env,
                    capture_output=True,
                    text=True,
                    check=False,
                )

                self.assertEqual(completed.returncode, 1)
                self.assertIn("INFRASTRUCTURE ABORT", completed.stdout)
                self.assertIn("property verdict invalid", completed.stdout)
                self.assertIn("2 completed of 3", completed.stdout)
                self.assertIn("1 not started", completed.stdout)
                self.assertIn(
                    "1 unittest-failed target withheld from property reporting",
                    completed.stdout,
                )
                self.assertIn("--- INFRASTRUCTURE FAIL", completed.stdout)
                self.assertNotIn("--- FAIL ", completed.stdout)
                self.assertNotIn("DEPTH ", completed.stdout)
                self.assertNotIn("pending-target-ran", completed.stdout)
                self.assertFalse(
                    (self.database / "invalid-infrastructure-marker").exists()
                )

    def test_external_property_id_runs_through_filtered_module_load(self) -> None:
        env = {
            **os.environ,
            "PYTHONPATH": os.pathsep.join(
                (str(self.root), str(REPO_ROOT), os.environ.get("PYTHONPATH", ""))
            ),
            "HYPOTHESIS_STORAGE_DIRECTORY": str(self.database),
            "CRATEDIGGER_FUZZ_MAX_EXAMPLES": "8",
        }

        completed = subprocess.run(
            [
                sys.executable,
                str(RUNNER),
                "--jobs",
                "4",
                "--profile",
                "fuzz",
                "--property-shards",
                "4",
                "fuzz_fixture.test_external_generated",
            ],
            cwd=REPO_ROOT,
            env=env,
            capture_output=True,
            text=True,
            check=False,
        )

        self.assertEqual(completed.returncode, 0, completed.stdout + completed.stderr)
        self.assertIn("4 targets", completed.stdout)
        self.assertIn("1 tests", completed.stdout)
        self.assertIn("ALL GREEN", completed.stdout)

    def test_explicit_property_shards_run_their_divided_budget(self) -> None:
        env = {
            **os.environ,
            "PYTHONPATH": os.pathsep.join(
                (str(self.root), str(REPO_ROOT), os.environ.get("PYTHONPATH", ""))
            ),
            "HYPOTHESIS_STORAGE_DIRECTORY": str(self.database),
            "CRATEDIGGER_FUZZ_MAX_EXAMPLES": "100",
            "FUZZ_COUNT_DIR": str(self.root),
        }

        completed = subprocess.run(
            [
                sys.executable,
                str(RUNNER),
                "--jobs",
                "3",
                "--profile",
                "fuzz",
                "--property-shards",
                "3",
                self.module,
            ],
            cwd=REPO_ROOT,
            env=env,
            capture_output=True,
            text=True,
            check=False,
        )

        self.assertEqual(completed.returncode, 0, completed.stdout + completed.stderr)
        self.assertIn("7 targets (6 property targets", completed.stdout)
        self.assertIn("DEPTH 2 properties measured", completed.stdout)
        self.assertIn("ALL GREEN", completed.stdout)
        self.assertEqual((self.root / "property-one-count").read_bytes(), b"xxx")
        self.assertEqual((self.root / "property-two-count").read_bytes(), b"xxx")

    def test_burst_discloses_a_structurally_shallow_property(self) -> None:
        """End-to-end: real runner, real child, real Hypothesis statistics."""
        env = {
            **os.environ,
            "PYTHONPATH": os.pathsep.join(
                (str(self.root), str(REPO_ROOT), os.environ.get("PYTHONPATH", ""))
            ),
            "HYPOTHESIS_STORAGE_DIRECTORY": str(self.database),
        }

        completed = subprocess.run(
            [
                sys.executable,
                str(RUNNER),
                "--jobs",
                "1",
                "--profile",
                "suite",
                "fuzz_fixture.test_shallow_generated",
            ],
            cwd=REPO_ROOT,
            env=env,
            capture_output=True,
            text=True,
            check=False,
        )

        self.assertEqual(completed.returncode, 0, completed.stdout + completed.stderr)
        self.assertIn(
            "DEPTH 1 properties measured, 1 shallow "
            "(space exhausted below budget)",
            completed.stdout,
        )
        self.assertIn(
            "SHALLOW 4 worlds vs 50 examples per shard (1 shard, 50 total) "
            "fuzz_fixture.test_shallow_generated.ShallowWorld.test_property",
            completed.stdout,
        )

    def test_reported_property_count_equals_the_discovered_property_count(
        self,
    ) -> None:
        """The denominator is derived, never hand-counted.

        The fixture mixes a property, a plain pin, a pin whose body declares
        and calls its own ``@given`` (the known-bad self-test shape, which
        emits statistics under the enclosing test's id), and a stateful
        machine (whose id comes from ``hypothesis.stateful``, not the module).
        Only the two real properties may be counted.
        """
        module = "fuzz_fixture.test_mixed_generated"
        env = {
            **os.environ,
            "PYTHONPATH": os.pathsep.join(
                (str(self.root), str(REPO_ROOT), os.environ.get("PYTHONPATH", ""))
            ),
            "HYPOTHESIS_STORAGE_DIRECTORY": str(self.database),
            "CRATEDIGGER_HYPOTHESIS_PROFILE": "suite",
        }
        with tempfile.TemporaryDirectory() as discovery:
            manifest = discover_fuzz_manifests(
                (module,),
                worker_count=1,
                environment=env,
                work_directory=Path(discovery),
            )[0]

        completed = subprocess.run(
            [
                sys.executable,
                str(RUNNER),
                "--jobs",
                "2",
                "--profile",
                "suite",
                module,
            ],
            cwd=REPO_ROOT,
            env=env,
            capture_output=True,
            text=True,
            check=False,
        )

        self.assertEqual(completed.returncode, 0, completed.stdout + completed.stderr)
        discovered = tuple(item.test_id for item in manifest.hypothesis_tests)
        self.assertEqual(
            sorted(discovered),
            [
                "fuzz_fixture.test_mixed_generated.MixedWorld.test_property",
                "hypothesis.stateful.Machine.TestCase.runTest",
            ],
        )
        self.assertIn(
            f"DEPTH {len(discovered)} properties measured,",
            completed.stdout,
        )
        self.assertNotIn("test_pin_running_an_inner_property", completed.stdout)

    def test_deterministic_profile_rejects_entropy_sharding(self) -> None:
        completed = subprocess.run(
            [
                sys.executable,
                str(RUNNER),
                "--profile",
                "suite",
                "--property-shards",
                "2",
                self.module,
            ],
            cwd=REPO_ROOT,
            capture_output=True,
            text=True,
            check=False,
        )

        self.assertEqual(completed.returncode, 2)
        self.assertIn(
            "supported only by the fuzz profile",
            completed.stderr,
        )

    def test_preflight_headroom_exhaustion_aborts_before_any_discovery(
        self,
    ) -> None:
        """Issue #1156 item 3: the fail-fast, before-any-work precondition.

        CRATEDIGGER_TEST_RAM_MIN_BYTES set to a value no real host's free
        bytes can exceed is the same deterministic trick
        TestRunTargetsWorkerExceptionWiring already documents for the
        suite's own classifier -- real headroom_floor_bytes/
        _check_suite_headroom, genuinely tripped, no fake disk.
        """
        env = {
            **os.environ,
            "PYTHONPATH": os.pathsep.join(
                (str(self.root), str(REPO_ROOT), os.environ.get("PYTHONPATH", ""))
            ),
            "HYPOTHESIS_STORAGE_DIRECTORY": str(self.database),
            "CRATEDIGGER_FUZZ_OUTPUT_DIR": str(self.output_dir),
            "CRATEDIGGER_TEST_RAM_MIN_BYTES": str(10**18),
        }

        completed = subprocess.run(
            [
                sys.executable,
                str(RUNNER),
                "--jobs",
                "1",
                "--profile",
                "suite",
                self.module,
            ],
            cwd=REPO_ROOT,
            env=env,
            capture_output=True,
            text=True,
            check=False,
        )

        self.assertEqual(
            completed.returncode,
            TEST_RAM_ROOT_EXHAUSTED_EXIT_CODE,
            completed.stdout + completed.stderr,
        )
        self.assertIn(
            TEST_RAM_ROOT_EXHAUSTED, completed.stdout + completed.stderr
        )
        self.assertFalse(self.output_dir.exists())
        self.assertNotIn("targets (", completed.stdout)

    def test_wrapper_delegates_to_the_exact_coverage_runner(self) -> None:
        source = pinned_source(WRAPPER)

        self.assertIn("python3 scripts/run_fuzz_tests.py", source)


if __name__ == "__main__":
    unittest.main()
