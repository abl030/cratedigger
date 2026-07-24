"""Generated exact-coverage patrol for fuzz property sharding."""

from __future__ import annotations

import unittest
from dataclasses import replace
import os
from pathlib import Path
import tempfile

from hypothesis import given
from hypothesis import strategies as st

import tests._hypothesis_profiles  # noqa: F401 - registers active profile
from scripts.run_fuzz_tests import (
    FuzzModuleManifest,
    FuzzPropertyManifest,
    assert_exact_fuzz_coverage,
    build_fuzz_targets,
    discover_fuzz_manifests,
)


class TestGeneratedFuzzTargetPlanning(unittest.TestCase):
    @given(
        property_count=st.integers(min_value=0, max_value=40),
        pin_count=st.integers(min_value=0, max_value=40),
        property_shards=st.integers(min_value=1, max_value=8),
        max_examples=st.integers(min_value=8, max_value=20_003),
    )
    def test_every_discovered_test_receives_its_exact_budget(
        self,
        property_count: int,
        pin_count: int,
        property_shards: int,
        max_examples: int,
    ) -> None:
        module_name = "tests.test_generated_world"
        property_ids = tuple(
            f"{module_name}.TestWorld.test_property_{index}"
            for index in range(property_count)
        )
        pin_ids = tuple(
            f"{module_name}.TestWorld.test_pin_{index}"
            for index in range(pin_count)
        )
        manifest = FuzzModuleManifest(
            module_name=module_name,
            test_ids=property_ids + pin_ids,
            hypothesis_tests=tuple(
                FuzzPropertyManifest(
                    test_id=test_id,
                    max_examples=max_examples,
                    uses_default_settings=True,
                )
                for test_id in property_ids
            ),
        )

        targets = build_fuzz_targets(
            (manifest,),
            property_shards=property_shards,
        )

        assert_exact_fuzz_coverage((manifest,), targets)
        for property_id in property_ids:
            shards = [
                target
                for target in targets
                if property_id in target.expected_test_ids
            ]
            self.assertEqual(len(shards), property_shards)
            if property_shards == 1:
                self.assertIsNone(shards[0].profile_max_examples)
            else:
                self.assertEqual(
                    sum(
                        target.profile_max_examples or 0
                        for target in shards
                    ),
                    max_examples,
                )
        for pin_id in pin_ids:
            self.assertEqual(
                sum(pin_id in target.expected_test_ids for target in targets),
                1,
            )


class TestFuzzCoverageCheckerKnownBad(unittest.TestCase):
    def test_checker_rejects_an_omitted_property(self) -> None:
        manifest = FuzzModuleManifest(
            module_name="tests.test_generated_world",
            test_ids=(
                "tests.test_generated_world.TestWorld.test_property_one",
                "tests.test_generated_world.TestWorld.test_property_two",
            ),
            hypothesis_tests=(
                FuzzPropertyManifest(
                    test_id=(
                        "tests.test_generated_world."
                        "TestWorld.test_property_one"
                    ),
                    max_examples=20_000,
                    uses_default_settings=True,
                ),
                FuzzPropertyManifest(
                    test_id=(
                        "tests.test_generated_world."
                        "TestWorld.test_property_two"
                    ),
                    max_examples=20_000,
                    uses_default_settings=True,
                ),
            ),
        )
        targets = build_fuzz_targets((manifest,))

        with self.assertRaisesRegex(ValueError, "missing fuzz test"):
            assert_exact_fuzz_coverage((manifest,), targets[:1])

    def test_checker_rejects_a_changed_entropy_budget(self) -> None:
        property_id = (
            "tests.test_generated_world.TestWorld.test_property_one"
        )
        manifest = FuzzModuleManifest(
            module_name="tests.test_generated_world",
            test_ids=(property_id,),
            hypothesis_tests=(
                FuzzPropertyManifest(
                    test_id=property_id,
                    max_examples=20_000,
                    uses_default_settings=True,
                ),
            ),
        )
        targets = list(build_fuzz_targets((manifest,), property_shards=4))
        assert targets[0].profile_max_examples is not None
        targets[0] = replace(
            targets[0],
            profile_max_examples=targets[0].profile_max_examples + 1,
        )

        with self.assertRaisesRegex(ValueError, "changed fuzz property budget"):
            assert_exact_fuzz_coverage((manifest,), targets)


class TestFuzzDiscoverySettingsContract(unittest.TestCase):
    def test_discovery_rejects_property_with_default_deadline(self) -> None:
        """A module that omits profile registration must fail before sharding."""
        with tempfile.TemporaryDirectory() as directory:
            fixture_root = Path(directory)
            fixture = fixture_root / "unprofiled_fuzz_fixture.py"
            fixture.write_text(
                "from hypothesis import given, strategies as st\n"
                "import unittest\n\n"
                "class TestUnprofiled(unittest.TestCase):\n"
                "    @given(st.integers())\n"
                "    def test_property(self, value):\n"
                "        self.assertIsInstance(value, int)\n",
                encoding="utf-8",
            )
            environment = dict(os.environ)
            old_pythonpath = environment.get("PYTHONPATH", "")
            environment["PYTHONPATH"] = os.pathsep.join(
                part for part in (str(fixture_root), old_pythonpath) if part
            )
            with self.assertRaisesRegex(RuntimeError, "non-None deadline"):
                discover_fuzz_manifests(
                    ("unprofiled_fuzz_fixture",),
                    worker_count=1,
                    environment=environment,
                    work_directory=fixture_root,
                )


if __name__ == "__main__":
    unittest.main()
