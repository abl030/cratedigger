"""Contracts for the generated fuzz-burst runner."""

from __future__ import annotations

import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from scripts.run_fuzz_tests import (
    FuzzModuleManifest,
    FuzzPropertyManifest,
    assert_exact_fuzz_coverage,
    build_fuzz_targets,
    discover_fuzz_manifests,
    recommended_property_shards,
)


REPO_ROOT = Path(__file__).resolve().parent.parent
RUNNER = REPO_ROOT / "scripts" / "run_fuzz_tests.py"
WRAPPER = REPO_ROOT / "scripts" / "fuzz_burst.sh"


class TestFuzzTargetPlanning(unittest.TestCase):
    @staticmethod
    def property(
        test_id: str,
        *,
        max_examples: int = 20_000,
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
