"""Focused contracts for the multicore world-model burst coordinator."""

from __future__ import annotations

import hashlib
import os
import sys
import tempfile
import unittest
from pathlib import Path
from typing import ClassVar

import msgspec
from hypothesis import given, settings
from hypothesis import strategies as st
from hypothesis.database import DirectoryBasedExampleDatabase
from hypothesis.stateful import RuleBasedStateMachine, rule

import tests._hypothesis_profiles  # noqa: F401 - required profile side effect
from scripts.run_world_model_burst import (
    ChildReceipt,
    ReplayReceipt,
    ReplayTarget,
    WorldManifest,
    WorldTarget,
    _hypothesis_database_key_paths,
    _seed_exact_test,
    _seed_private_database,
    build_targets,
    child_environment,
    classify_receipt,
    classify_unittest_outcome,
    effective_jobs,
    main,
    prepare_canonical_database,
    replace_canonical_database,
    target_seed,
)

GENERATED = tuple(
    f"tests.world_model.state_machine.Generated{i}.test_world" for i in range(5)
)
STATEFUL = "hypothesis.stateful.LifecycleWorldMachine.TestCase.runTest"
PINS = (
    "tests.world_model.state_machine.TestPins.test_a",
    "tests.world_model.state_machine.TestPins.test_b",
)


def _database_keys(
    generated_ids: tuple[str, ...],
) -> tuple[tuple[str, tuple[str, ...]], ...]:
    return tuple(
        (
            test_id,
            (
                hashlib.sha384(test_id.encode()).hexdigest()[:16],
                hashlib.sha384(f"{test_id}.secondary".encode()).hexdigest()[:16],
                hashlib.sha384(f"{test_id}.pareto".encode()).hexdigest()[:16],
            ),
        )
        for test_id in generated_ids
    )


class _SeedProbeCase(unittest.TestCase):
    """Receives a generated method dynamically so discovery ignores this probe."""


class _SeedMachine(RuleBasedStateMachine):
    draws: ClassVar[list[int]] = []

    @rule(value=st.integers())
    def record(self, value: int) -> None:
        type(self).draws.append(value)


class TestWorldModelScheduling(unittest.TestCase):
    def test_run_receipt_records_elapsed_and_aborted_admission(self) -> None:
        not_started = ReplayTarget(
            label="generated::shard-02-of-02",
            logical_id=GENERATED[0],
            test_ids=(GENERATED[0],),
            shard_index=1,
            shard_count=2,
            examples=12,
            steps=100,
            seed=42,
            outcome="not_started",
            elapsed_seconds=0.0,
        )
        receipt = ReplayReceipt(
            root_seed=91,
            engine="in-process",
            examples=25,
            steps=100,
            jobs=30,
            elapsed_seconds=3.5,
            admission_aborted=True,
            targets=(),
            not_started=(not_started,),
        )

        wire = msgspec.json.decode(msgspec.json.encode(receipt))
        self.assertEqual(wire["elapsed_seconds"], 3.5)
        self.assertTrue(wire["admission_aborted"])
        self.assertEqual(wire["not_started"][0]["outcome"], "not_started")

    def test_pins_run_once_and_generated_budgets_sum_exactly(self) -> None:
        load_names = PINS + tuple(
            f"tests.world_model.state_machine.GeneratedAlias{i}.test_world"
            for i in range(5)
        )
        manifest = WorldManifest(
            test_ids=PINS + GENERATED,
            generated_ids=GENERATED,
            load_names=load_names,
            database_key_paths=_database_keys(GENERATED),
        )

        targets = build_targets(manifest, examples=25, jobs=30, root_seed=91)

        pin_targets = [target for target in targets if not target.generated]
        self.assertEqual(len(pin_targets), 1)
        self.assertEqual(pin_targets[0].test_ids, PINS)
        self.assertEqual(pin_targets[0].load_names, PINS)
        for generated_index, test_id in enumerate(GENERATED):
            shards = [target for target in targets if target.logical_id == test_id]
            self.assertEqual(
                {target.load_names for target in shards},
                {(load_names[len(PINS) + generated_index],)},
            )
            self.assertEqual(sum(target.examples for target in shards), 25)
            self.assertEqual({target.steps for target in shards}, {None})
            self.assertEqual(
                {target.shard_index for target in shards},
                set(range(len(shards))),
            )
            replay_owners = [target for target in shards if target.replay_corpus]
            self.assertEqual(len(replay_owners), 1)
            self.assertEqual(replay_owners[0].shard_index, 0)
            self.assertEqual(
                {target.database_key_paths for target in shards},
                {_database_keys(GENERATED)[generated_index][1]},
            )

    def test_every_state_machine_shard_keeps_the_full_step_budget(self) -> None:
        manifest = WorldManifest(
            test_ids=GENERATED,
            generated_ids=GENERATED,
            database_key_paths=_database_keys(GENERATED),
        )

        targets = build_targets(
            manifest,
            examples=25,
            jobs=30,
            root_seed=91,
            steps=100,
        )

        self.assertEqual({target.steps for target in targets}, {100})
        for test_id in GENERATED:
            self.assertEqual(
                sum(target.examples for target in targets if target.logical_id == test_id),
                25,
            )

    def test_dominant_state_machine_can_fill_all_example_slots(self) -> None:
        generated = GENERATED[:4] + (STATEFUL,)
        manifest = WorldManifest(
            test_ids=generated,
            generated_ids=generated,
            database_key_paths=_database_keys(generated),
        )

        targets = build_targets(
            manifest,
            examples=25,
            jobs=30,
            root_seed=91,
            steps=100,
        )

        stateful = [target for target in targets if target.logical_id == STATEFUL]
        self.assertEqual(len(stateful), 25)
        self.assertEqual({target.examples for target in stateful}, {1})
        for test_id in GENERATED[:4]:
            self.assertEqual(
                len([target for target in targets if target.logical_id == test_id]),
                6,
            )
        # Round-robin admission prevents any one logical property monopolising
        # the initial worker wave.
        self.assertEqual(
            {target.logical_id for target in targets[:5]},
            set(generated),
        )

    def test_discovery_fails_closed_unless_exactly_five_generated_ids(self) -> None:
        manifest = WorldManifest(
            test_ids=GENERATED[:4],
            generated_ids=GENERATED[:4],
            database_key_paths=_database_keys(GENERATED[:4]),
        )
        with self.assertRaisesRegex(ValueError, "exactly 5 generated tests"):
            build_targets(manifest, examples=25, jobs=30, root_seed=91)

    def test_production_census_fails_closed_unless_exactly_nineteen_pins(self) -> None:
        manifest = WorldManifest(
            test_ids=PINS + GENERATED,
            generated_ids=GENERATED,
            database_key_paths=_database_keys(GENERATED),
        )

        with self.assertRaisesRegex(ValueError, "exactly 19 deterministic pins"):
            build_targets(
                manifest,
                examples=25,
                jobs=30,
                root_seed=91,
                expected_pins=19,
            )

    def test_target_seed_is_stable_across_worker_pid_and_schedule_order(self) -> None:
        first = target_seed(123456, GENERATED[2], 3, 6)
        reordered = target_seed(123456, GENERATED[2], 3, 6)
        sibling = target_seed(123456, GENERATED[2], 4, 6)
        different_schedule = target_seed(123456, GENERATED[2], 3, 7)

        self.assertEqual(first, reordered)
        self.assertNotEqual(first, sibling)
        self.assertNotEqual(first, different_schedule)
        self.assertGreaterEqual(first, 0)

    def test_default_capacity_uses_thirty_cores_but_mirror_is_capped_at_two(self) -> None:
        self.assertEqual(effective_jobs(None, "in-process", host_cpus=64), 30)
        self.assertEqual(effective_jobs(None, "mirror-harness", host_cpus=64), 2)
        self.assertEqual(effective_jobs(50, "in-process", host_cpus=64), 30)
        self.assertEqual(effective_jobs(10, "mirror-harness", host_cpus=64), 2)


class TestWorldModelChildBoundary(unittest.TestCase):
    def _run_given_probe(
        self,
        *,
        seed: int,
        database_path: Path,
        fail_on_zero: bool,
    ) -> tuple[list[int], unittest.TestResult, tuple[str, str, str]]:
        draws: list[int] = []

        def property_test(_case: unittest.TestCase, value: int) -> None:
            draws.append(value)
            if fail_on_zero and value == 0:
                raise AssertionError("seed probe")

        method = settings(
            max_examples=8,
            deadline=None,
            database=DirectoryBasedExampleDatabase(database_path),
        )(given(st.integers())(property_test))
        method_name = "test_generated_seed_probe"
        setattr(_SeedProbeCase, method_name, method)
        try:
            database_key_paths = _hypothesis_database_key_paths(
                _SeedProbeCase,
                method,
            )
            load_name = f"{__name__}._SeedProbeCase.{method_name}"
            _seed_exact_test(load_name, seed)
            suite = unittest.defaultTestLoader.loadTestsFromName(load_name)
            result = unittest.TestResult()
            suite.run(result)
            return draws, result, database_key_paths
        finally:
            delattr(_SeedProbeCase, method_name)

    def _run_stateful_probe(
        self,
        *,
        seed: int,
        database_path: Path,
    ) -> tuple[int, ...]:
        case_type = _SeedMachine.TestCase
        original_run_test = case_type.runTest
        case_type.settings = settings(
            max_examples=3,
            stateful_step_count=4,
            deadline=None,
            database=DirectoryBasedExampleDatabase(database_path),
        )
        alias = "_DynamicSeedMachineCase"
        module = sys.modules[__name__]
        setattr(module, alias, case_type)
        _SeedMachine.draws = []
        try:
            load_name = f"{__name__}.{alias}.runTest"
            _seed_exact_test(load_name, seed)
            suite = unittest.defaultTestLoader.loadTestsFromName(load_name)
            result = unittest.TestResult()
            suite.run(result)
            self.assertTrue(result.wasSuccessful(), result.errors)
            return tuple(_SeedMachine.draws)
        finally:
            case_type.runTest = original_run_test
            delattr(module, alias)

    def test_real_given_seed_is_repeatable_without_disabling_replay_database(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            first, first_result, _first_keys = self._run_given_probe(
                seed=90210,
                database_path=root / "first",
                fail_on_zero=False,
            )
            second, second_result, _second_keys = self._run_given_probe(
                seed=90210,
                database_path=root / "second",
                fail_on_zero=False,
            )
            _failed, failed_result, failure_keys = self._run_given_probe(
                seed=90210,
                database_path=root / "failure",
                fail_on_zero=True,
            )

            self.assertTrue(first_result.wasSuccessful())
            self.assertTrue(second_result.wasSuccessful())
            self.assertEqual(first, second)
            self.assertFalse(failed_result.wasSuccessful())
            self.assertTrue(
                any((root / "failure" / key).is_dir() for key in failure_keys),
                "a real seeded Hypothesis failure must persist replay data",
            )

    def test_real_state_machine_seed_recreates_draws(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            first = self._run_stateful_probe(
                seed=314159,
                database_path=root / "first",
            )
            second = self._run_stateful_probe(
                seed=314159,
                database_path=root / "second",
            )

            self.assertTrue(first)
            self.assertEqual(first, second)

    def test_each_target_gets_private_hypothesis_and_beets_scratch(self) -> None:
        target = WorldTarget(
            label="generated-1",
            logical_id=GENERATED[0],
            test_ids=(GENERATED[0],),
            generated=True,
            shard_index=0,
            shard_count=2,
            examples=13,
            steps=100,
            seed=777,
        )
        with tempfile.TemporaryDirectory() as temporary:
            env = child_environment(
                os.environ,
                target=target,
                dsn="postgresql:///world_1",
                target_root=Path(temporary),
                engine="in-process",
                mirror_url="",
            )

            self.assertEqual(env["TEST_DB_DSN"], "postgresql:///world_1")
            self.assertEqual(env["CRATEDIGGER_TEST_SCHEMA_READY"], "1")
            self.assertEqual(env["CRATEDIGGER_WORLD_EXAMPLES"], "13")
            self.assertEqual(env["CRATEDIGGER_WORLD_STEPS"], "100")
            self.assertEqual(env["CRATEDIGGER_WORLD_SEED"], "777")
            self.assertTrue(env["CRATEDIGGER_WORLD_DATABASE"].startswith(temporary))
            self.assertTrue(env["BEETSDIR"].startswith(temporary))

    def test_receipt_rejects_unexpected_ids_and_malformed_wire(self) -> None:
        target = WorldTarget(
            label="generated-1",
            logical_id=GENERATED[0],
            test_ids=(GENERATED[0],),
            generated=True,
            shard_index=0,
            shard_count=1,
            examples=25,
            steps=100,
            seed=777,
        )
        wrong = ChildReceipt(
            test_ids=(GENERATED[1],),
            outcome="passed",
            tests_run=1,
            seed=777,
            output="",
        )

        with self.assertRaisesRegex(RuntimeError, "unexpected test IDs"):
            classify_receipt(target, msgspec.json.encode(wrong))
        with self.assertRaisesRegex(RuntimeError, "malformed child receipt"):
            classify_receipt(target, b"not-json")

        wrong_count = ChildReceipt(
            test_ids=target.test_ids,
            outcome="passed",
            tests_run=0,
            seed=target.seed,
            output="",
        )
        with self.assertRaisesRegex(RuntimeError, "test count changed"):
            classify_receipt(target, msgspec.json.encode(wrong_count))

    def test_receipt_distinguishes_property_failure_from_infrastructure(self) -> None:
        target = WorldTarget(
            label="generated-1",
            logical_id=GENERATED[0],
            test_ids=(GENERATED[0],),
            generated=True,
            shard_index=0,
            shard_count=1,
            examples=25,
            steps=100,
            seed=777,
        )
        failed = ChildReceipt(
            test_ids=target.test_ids,
            outcome="property_failure",
            tests_run=1,
            seed=777,
            output="counterexample",
        )
        infrastructure = ChildReceipt(
            test_ids=target.test_ids,
            outcome="infrastructure_error",
            tests_run=0,
            seed=777,
            output="database unavailable",
        )

        self.assertEqual(classify_receipt(target, msgspec.json.encode(failed)), "property_failure")
        self.assertEqual(
            classify_receipt(target, msgspec.json.encode(infrastructure)),
            "infrastructure_error",
        )

    def test_unittest_infrastructure_wins_over_property_failure(self) -> None:
        self.assertEqual(
            classify_unittest_outcome(
                generated=True,
                successful=False,
                infrastructure_errors=("database unavailable",),
            ),
            "infrastructure_error",
        )
        self.assertEqual(
            classify_unittest_outcome(
                generated=True,
                successful=False,
                infrastructure_errors=(),
            ),
            "property_failure",
        )

    def test_skips_are_infrastructure_not_green_coverage(self) -> None:
        self.assertEqual(
            classify_unittest_outcome(
                generated=True,
                successful=True,
                infrastructure_errors=(),
                skipped=("resource unavailable",),
            ),
            "infrastructure_error",
        )


class TestWorldModelReplayDatabase(unittest.TestCase):
    def test_only_the_replay_owner_receives_its_canonical_key_directories(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            canonical = root / "canonical"
            canonical.mkdir()
            owned = "0123456789abcdef"
            foreign = "fedcba9876543210"
            (canonical / owned).mkdir()
            (canonical / owned / "replay").write_text("owned", encoding="utf-8")
            (canonical / foreign).mkdir()
            (canonical / foreign / "replay").write_text("foreign", encoding="utf-8")
            replay_owner = root / "owner"
            entropy_shard = root / "entropy"

            _seed_private_database(canonical, replay_owner, (owned,))
            _seed_private_database(canonical, entropy_shard, ())

            self.assertEqual(
                (replay_owner / owned / "replay").read_text(),
                "owned",
            )
            self.assertFalse((replay_owner / foreign).exists())
            self.assertFalse((entropy_shard / owned).exists())

    def test_green_union_replaces_only_owned_hypothesis_keys(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            canonical = root / "canonical"
            prepare_canonical_database(canonical)
            owned = "0123456789abcdef"
            foreign = "unrelated-data"
            (canonical / owned).mkdir()
            (canonical / owned / "stale").write_text("old", encoding="utf-8")
            (canonical / foreign).write_text("keep", encoding="utf-8")
            first = root / "first"
            second = root / "second"
            (first / owned).mkdir(parents=True)
            (second / owned).mkdir(parents=True)
            (first / owned / "first").write_text("one", encoding="utf-8")
            (second / owned / "second").write_text("two", encoding="utf-8")

            replace_canonical_database(canonical, (first, second), (owned,))

            self.assertFalse((canonical / owned / "stale").exists())
            self.assertEqual(
                {path.name for path in (canonical / owned).iterdir()},
                {"first", "second"},
            )
            self.assertEqual((canonical / foreign).read_text(), "keep")
            self.assertFalse(any(root.glob(".canonical.staging.*")))
            self.assertFalse(any(root.glob(".canonical.union.*")))
            self.assertFalse(any(root.glob(".canonical.backup.*")))

    def test_later_key_swap_failure_rolls_back_every_owned_key(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            canonical = root / "canonical"
            prepare_canonical_database(canonical)
            keys = ("0123456789abcdef", "fedcba9876543210")
            private = root / "private"
            for key in keys:
                (canonical / key).mkdir()
                (canonical / key / "example").write_text(
                    f"old-{key}", encoding="utf-8"
                )
                (private / key).mkdir(parents=True)
                (private / key / "example").write_text(
                    f"new-{key}", encoding="utf-8"
                )
            replacements = 0

            def fail_generation_commit(source: Path, destination: Path) -> None:
                nonlocal replacements
                replacements += 1
                if replacements == 2:
                    raise OSError("injected generation-commit failure")
                os.replace(source, destination)

            with self.assertRaisesRegex(OSError, "generation-commit"):
                replace_canonical_database(
                    canonical,
                    (private,),
                    keys,
                    replace=fail_generation_commit,
                )

            for key in keys:
                self.assertEqual(
                    (canonical / key / "example").read_text(),
                    f"old-{key}",
                )
            self.assertFalse(any(root.glob(".canonical.staging.*")))
            self.assertFalse(any(root.glob(".canonical.union.*")))
            self.assertFalse(any(root.glob(".canonical.backup.*")))

    def test_early_coordinator_abort_writes_terminal_replay_receipt(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            canonical = root / "unowned"
            canonical.mkdir()
            (canonical / "important").write_text("keep", encoding="utf-8")
            output = root / "output"

            status = main((
                "--database", str(canonical),
                "--output-dir", str(output),
                "--examples", "1",
                "--steps", "1",
                "--jobs", "1",
                "--seed", "7",
            ))

            self.assertEqual(status, 1)
            run_directories = tuple(output.glob("run.*"))
            self.assertEqual(len(run_directories), 1)
            receipt = msgspec.json.decode(
                (run_directories[0] / "replay.json").read_bytes(),
                type=ReplayReceipt,
            )
            self.assertTrue(receipt.admission_aborted)
            self.assertIn("ownership marker", receipt.coordinator_error or "")
            self.assertEqual(receipt.targets, ())
            self.assertEqual(receipt.not_started, ())
            self.assertEqual((canonical / "important").read_text(), "keep")

    def test_corpus_commit_abort_preserves_completed_target_receipts(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            canonical = root / "canonical"
            output = root / "output"

            def fail_commit(
                _canonical: Path,
                _private_databases: tuple[Path, ...],
                _key_paths: tuple[str, ...],
            ) -> None:
                raise OSError("injected corpus commit failure")

            status = main(
                (
                    "--database", str(canonical),
                    "--output-dir", str(output),
                    "--examples", "1",
                    "--steps", "1",
                    "--jobs", "1",
                    "--seed", "8",
                ),
                replace_canonical=fail_commit,
            )

            self.assertEqual(status, 1)
            run_directory, = output.glob("run.*")
            receipt = msgspec.json.decode(
                (run_directory / "replay.json").read_bytes(),
                type=ReplayReceipt,
            )
            self.assertTrue(receipt.admission_aborted)
            self.assertIn("corpus commit failure", receipt.coordinator_error or "")
            self.assertEqual(len(receipt.targets), 6)
            self.assertEqual({target.outcome for target in receipt.targets}, {"passed"})
            self.assertEqual(receipt.not_started, ())

    def test_nonempty_unowned_database_path_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            candidate = Path(temporary) / "not-a-world-model-database"
            candidate.mkdir()
            (candidate / "important").write_text("keep", encoding="utf-8")

            with self.assertRaisesRegex(ValueError, "ownership marker"):
                prepare_canonical_database(candidate)

            self.assertEqual((candidate / "important").read_text(), "keep")

    def test_legacy_hypothesis_key_directories_gain_marker_without_data_loss(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            candidate = Path(temporary) / "legacy-world-model"
            key = candidate / "0123456789abcdef"
            key.mkdir(parents=True)
            (key / "example").write_bytes(b"counterexample")

            prepare_canonical_database(candidate)

            self.assertEqual((key / "example").read_bytes(), b"counterexample")
            self.assertTrue(
                (candidate / ".cratedigger-world-model-database").is_file()
            )


if __name__ == "__main__":
    unittest.main()
