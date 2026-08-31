"""Focused contracts for the multicore world-model burst coordinator."""

from __future__ import annotations

import contextlib
import hashlib
import io
import os
import subprocess
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
from scripts.run_python_tests import TEST_RAM_ROOT_EXHAUSTED_EXIT_CODE
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
from scripts.test_substrate import TEST_RAM_ROOT_EXHAUSTED, RamRootExhaustedError

REPO_ROOT = Path(__file__).resolve().parents[1]
WORLD_MODEL_BURST = REPO_ROOT / "scripts" / "run_world_model_burst.py"
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

            # Independent review B1 (BLOCKING): check_headroom=None (the
            # production default) drives the REAL private_runtime_dir()/
            # check_suite_headroom(), coupling this test -- which is NOT
            # about headroom -- to whatever the shared tmpfs root happens
            # to have free at test time. Pin it to an always-satisfied
            # no-op; the tests that ARE about headroom inject their own.
            status = main(
                (
                    "--database", str(canonical),
                    "--output-dir", str(output),
                    "--examples", "1",
                    "--steps", "1",
                    "--jobs", "1",
                    "--seed", "7",
                ),
                check_headroom=lambda: None,
            )

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

            # Independent review B1 (BLOCKING): see the sibling test above
            # -- this one is not about headroom either.
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
                check_headroom=lambda: None,
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

    def test_preflight_headroom_exhaustion_returns_before_any_work(self) -> None:
        """Issue #1156 item 3: the fail-fast, before-any-work precondition.

        Distinct from the mid-run trip below: this fake always raises, so
        it must trip on `main`'s very first `check_headroom()` call, before
        `canonical_database`/`output_root` are even created.
        """

        def always_fail() -> None:
            # Independent review F9: the message deliberately does NOT
            # embed TEST_RAM_ROOT_EXHAUSTED -- the identity in the assembled
            # coordinator_error must come from main's own handler labeling
            # (f"{TEST_RAM_ROOT_EXHAUSTED}: {error}"), never leak through
            # from this fake's own text (agree-by-construction).
            raise RamRootExhaustedError("synthetic preflight trip")

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            canonical = root / "canonical"
            output = root / "output"

            status = main(
                (
                    "--database", str(canonical),
                    "--output-dir", str(output),
                    "--examples", "1",
                    "--steps", "1",
                    "--jobs", "1",
                    "--seed", "10",
                ),
                check_headroom=always_fail,
            )

            self.assertEqual(status, TEST_RAM_ROOT_EXHAUSTED_EXIT_CODE)
            self.assertFalse(output.exists())
            self.assertFalse(canonical.exists())

    def test_preflight_non_ram_root_error_returns_two(self) -> None:
        def boom() -> None:
            raise RuntimeError("synthetic infrastructure boom")

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)

            status = main(
                (
                    "--database", str(root / "canonical"),
                    "--output-dir", str(root / "output"),
                    "--examples", "1",
                    "--steps", "1",
                    "--jobs", "1",
                    "--seed", "11",
                ),
                check_headroom=boom,
            )

            self.assertEqual(status, 2)

    def test_unavailable_runtime_dir_aborts_cleanly_not_as_a_raw_traceback(
        self,
    ) -> None:
        """Independent review F3 (MEDIUM): private_runtime_dir() used to be
        called ONE LINE ABOVE the try/except that exists to catch its own
        RuntimeError, so it escaped as an unhandled traceback instead of
        the intended exit 2. Drives the REAL production private_runtime_dir()
        (check_headroom=None, the default -- no injected fake), pointed at
        a real, non-tmpfs directory via XDG_RUNTIME_DIR: test-fidelity
        Rule B, since test_preflight_non_ram_root_error_returns_two's fake
        only proves the except clause's own handling, never the real raise
        site. dir="/var/tmp" deliberately (independent review B9): TMPDIR
        is itself tmpfs inside this nix-shell, so a bare
        tempfile.TemporaryDirectory() would land ON tmpfs and never trip
        the check this test exists to prove; /var/tmp is real disk-backed
        storage that is also outside the git-tracked worktree, so a hard
        kill mid-test cannot leave stray debris in the repo checkout.
        """
        # Independent review B8: this used to assert only the exit code,
        # with no stdout/stderr capture at all -- so it could not tell "the
        # intended infrastructure-precondition abort ran" apart from "some
        # other unrelated path also happens to return 2" (e.g.
        # test_preflight_non_ram_root_error_returns_two's own generic
        # RuntimeError branch). Captured and asserted the same way the
        # fuzz twin (tests/test_fuzz_burst.py::TestFuzzMainMidRunHeadroom::
        # test_unavailable_runtime_dir_aborts_cleanly_not_as_a_raw_traceback)
        # already does.
        # dir="/var/tmp" deliberately (independent review B9, mirroring
        # the fuzz twin's own fix): real disk-backed (non-tmpfs, so this
        # actually trips the check under test) but NOT inside the
        # git-tracked worktree, so a hard kill mid-test -- which skips
        # TemporaryDirectory's own context-manager cleanup -- cannot leave
        # stray debris in the repo checkout the way an earlier
        # dir=repo_root version could.
        with tempfile.TemporaryDirectory(dir="/var/tmp") as fake_runtime_dir:
            original = os.environ.get("XDG_RUNTIME_DIR")
            os.environ["XDG_RUNTIME_DIR"] = fake_runtime_dir
            try:
                with tempfile.TemporaryDirectory() as temporary:
                    root = Path(temporary)
                    stdout = io.StringIO()
                    stderr = io.StringIO()
                    with contextlib.redirect_stdout(
                        stdout
                    ), contextlib.redirect_stderr(stderr):
                        status = main(
                            (
                                "--database", str(root / "canonical"),
                                "--output-dir", str(root / "output"),
                                "--examples", "1",
                                "--steps", "1",
                                "--jobs", "1",
                                "--seed", "13",
                            ),
                        )
                    output = stdout.getvalue() + stderr.getvalue()
            finally:
                if original is None:
                    os.environ.pop("XDG_RUNTIME_DIR", None)
                else:
                    os.environ["XDG_RUNTIME_DIR"] = original

        self.assertEqual(status, 2, output)
        self.assertIn("infrastructure precondition failed", output)
        self.assertIn("not tmpfs", output)

    def test_mid_run_headroom_exhaustion_aborts_and_labels_the_receipt(
        self,
    ) -> None:
        """The admission loop's OWN check, not the preflight one: the fake
        passes on call 1 (preflight) and trips on call 2 (the first
        admission-loop iteration), proving the loop really consults it a
        second time rather than only once at the top."""
        calls = {"count": 0}

        def flaky_check_headroom() -> None:
            calls["count"] += 1
            if calls["count"] >= 2:
                # Independent review F9: the message deliberately does NOT
                # embed TEST_RAM_ROOT_EXHAUSTED -- assertIn(TEST_RAM_ROOT_
                # EXHAUSTED, receipt.coordinator_error) below must be
                # satisfied by main's own handler labeling
                # (f"{TEST_RAM_ROOT_EXHAUSTED}: {error}"), not by this
                # fake's own message text (agree-by-construction: with the
                # identity embedded here, the assertion passed even when
                # the handler's own f-string prefix was mutated away).
                raise RamRootExhaustedError("synthetic mid-run trip")

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            canonical = root / "canonical"
            output = root / "output"

            status = main(
                (
                    "--database", str(canonical),
                    "--output-dir", str(output),
                    "--examples", "1",
                    "--steps", "1",
                    "--jobs", "1",
                    "--seed", "12",
                ),
                check_headroom=flaky_check_headroom,
            )

            self.assertEqual(status, TEST_RAM_ROOT_EXHAUSTED_EXIT_CODE)
            run_directory, = output.glob("run.*")
            receipt = msgspec.json.decode(
                (run_directory / "replay.json").read_bytes(),
                type=ReplayReceipt,
            )
            self.assertTrue(receipt.admission_aborted)
            self.assertIn(TEST_RAM_ROOT_EXHAUSTED, receipt.coordinator_error or "")
            self.assertEqual(receipt.targets, ())
            self.assertGreaterEqual(calls["count"], 2)

    def test_drain_phase_never_calls_check_headroom_once_pending_is_empty(
        self,
    ) -> None:
        """Independent review B6: the fuzz burst's twin regression pin
        (tests/test_fuzz_burst.py::TestFuzzTargetsMidRunHeadroom::
        test_drain_phase_never_calls_check_headroom_once_pending_is_empty)
        has no world-model-side equivalent, even though
        scripts/run_world_model_burst.py's own admission loop carries the
        SAME `if pending and not admission_aborted: check_headroom()` gate
        (line ~1138). `--jobs 10` exceeds this manifest's total target
        count (5 generated + 1 pins = 6 for --engine in-process), so every
        target is admitted in the FIRST admission-loop iteration -- call
        #1 is the preflight check, call #2 is that single admission
        check. Every later outer-loop iteration only drains `active`
        (`pending` is empty), so a correctly gated loop never calls
        check_headroom again. The poison pill traps starting at call #3:
        correct code lets the whole burst complete successfully; a mutant
        that drops the `pending` condition (checking on EVERY outer-loop
        iteration regardless) would trip it as each of the six targets
        completes and the loop re-polls."""
        calls = {"count": 0}

        def check_headroom() -> None:
            calls["count"] += 1
            if calls["count"] > 2:
                raise RamRootExhaustedError(
                    "must never fire once pending is empty"
                )

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            canonical = root / "canonical"
            output = root / "output"

            status = main(
                (
                    "--database", str(canonical),
                    "--output-dir", str(output),
                    "--examples", "1",
                    "--steps", "1",
                    "--jobs", "10",
                    "--seed", "14",
                ),
                check_headroom=check_headroom,
            )

        self.assertEqual(status, 0)
        self.assertEqual(calls["count"], 2)

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


class TestWorldModelRealHeadroomWiring(unittest.TestCase):
    """`main`'s own production `check_headroom is None` branch, driven for real.

    Every other headroom test in this module injects a fake through the
    `check_headroom` DI seam, so the real closure that seam defaults to --
    `private_runtime_dir()` plus `headroom_floor_bytes(1)` closed over
    `check_suite_headroom` -- was unconstrained: neutering it to
    `lambda: None` left every one of them green (independent review's
    surviving mutant M6b).

    Twin of `tests.test_fuzz_burst`'s
    `test_preflight_headroom_exhaustion_aborts_before_any_discovery`, and
    the same deterministic trick: `CRATEDIGGER_TEST_RAM_MIN_BYTES` past any
    real host's free bytes, so the genuine measurement trips. No fake disk,
    no injected seam, and a real subprocess so the CLI's own
    `check_headroom=None` default is what runs.
    """

    def test_the_real_preflight_closure_aborts_before_any_output(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            output = root / "output"

            completed = subprocess.run(
                [
                    sys.executable,
                    str(WORLD_MODEL_BURST),
                    "--database", str(root / "canonical"),
                    "--output-dir", str(output),
                    "--examples", "1",
                    "--steps", "1",
                    "--jobs", "1",
                    "--seed", "11",
                ],
                cwd=REPO_ROOT,
                env={
                    **os.environ,
                    "CRATEDIGGER_TEST_RAM_MIN_BYTES": str(10**18),
                },
                capture_output=True,
                text=True,
                check=False,
            )

            self.assertEqual(
                completed.returncode,
                TEST_RAM_ROOT_EXHAUSTED_EXIT_CODE,
                completed.stdout + completed.stderr,
            )
            self.assertIn(TEST_RAM_ROOT_EXHAUSTED, completed.stderr)
            self.assertFalse(output.exists())


if __name__ == "__main__":
    unittest.main()
