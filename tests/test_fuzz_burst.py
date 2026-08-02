"""Contracts for the generated fuzz-burst runner."""

from __future__ import annotations

import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from scripts.run_fuzz_tests import (
    DEPTH_REPORT_LIMIT,
    EPHEMERAL_POSTGRES_TARGET_LIMIT,
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
    recommended_property_shards,
    select_fuzz_admissions,
)
from scripts.run_python_tests import (
    STRATEGY_SPACE_EXHAUSTED,
    HypothesisPropertyStats,
)

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
                sum(target.profile_max_examples or 0 for target in shards),
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

    def test_explicit_property_budget_is_not_multiplied(self) -> None:
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
        self.assertEqual(len(targets), 1)
        self.assertIsNone(targets[0].profile_max_examples)

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
        self.assertEqual(EPHEMERAL_POSTGRES_TARGET_LIMIT, 2)

    def test_30_core_host_uses_eight_entropy_shards(self) -> None:
        self.assertEqual(recommended_property_shards(30), 8)

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
            "        self.assertEqual(value, value)\n\n"
            "    @settings(max_examples=3, deadline=None)\n"
            "    @given(value=st.text(max_size=3))\n"
            "    def test_property_two(self, value):\n"
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

    def run_burst(self, *, failing: bool) -> subprocess.CompletedProcess[str]:
        env = {
            **os.environ,
            "PYTHONPATH": os.pathsep.join(
                (str(self.root), str(REPO_ROOT), os.environ.get("PYTHONPATH", ""))
            ),
            "HYPOTHESIS_STORAGE_DIRECTORY": str(self.database),
            "CRATEDIGGER_FUZZ_OUTPUT_DIR": str(self.output_dir),
            "FUZZ_FIXTURE_FAIL": "1" if failing else "0",
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

    def test_wrapper_delegates_to_the_exact_coverage_runner(self) -> None:
        source = WRAPPER.read_text(encoding="utf-8")

        self.assertIn("python3 scripts/run_fuzz_tests.py", source)


if __name__ == "__main__":
    unittest.main()
