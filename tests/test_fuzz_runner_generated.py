"""Generated exact-coverage patrol for fuzz property sharding."""

from __future__ import annotations

import unittest
from collections.abc import Sequence
from dataclasses import replace
import os
from pathlib import Path
import tempfile

from hypothesis import example, given
from hypothesis import strategies as st

import tests._hypothesis_profiles  # noqa: F401 - registers active profile
from scripts.run_fuzz_tests import (
    DEPTH_REPORT_LIMIT,
    DISCARD_RATE_THRESHOLD,
    FuzzModuleManifest,
    FuzzPropertyManifest,
    PropertyDepth,
    aggregate_property_depth,
    assert_exact_fuzz_coverage,
    build_fuzz_targets,
    discard_rate,
    discover_fuzz_manifests,
    format_depth_report,
    is_structurally_shallow,
)
from scripts.run_python_tests import (
    STRATEGY_SPACE_EXHAUSTED,
    HypothesisPropertyStats,
)


#: Reasons Hypothesis reports, exhaustion plus the ordinary budget endings.
STOPPED_BECAUSE = (
    STRATEGY_SPACE_EXHAUSTED,
    "settings.max_examples=2500",
    "settings.max_examples=2500, but < 1% of examples satisfied assumptions",
    "",
)


def property_stats(id_count: int, max_size: int) -> st.SearchStrategy[
    tuple[HypothesisPropertyStats, ...]
]:
    """Child statistics over a small ID pool, so shards of one property collide."""
    return st.lists(
        st.builds(
            HypothesisPropertyStats,
            test_id=st.sampled_from(
                [
                    f"tests.test_world_{index:02d}_generated.World.test_property"
                    for index in range(id_count)
                ]
            ),
            max_examples=st.integers(min_value=1, max_value=20_000),
            valid=st.integers(min_value=0, max_value=20_000),
            invalid=st.integers(min_value=0, max_value=20_000),
            overrun=st.integers(min_value=0, max_value=500),
            interesting=st.integers(min_value=0, max_value=10),
            stopped_because=st.sampled_from(STOPPED_BECAUSE),
        ),
        min_size=1,
        max_size=max_size,
    ).map(tuple)


def assert_shard_aggregation(
    records: Sequence[HypothesisPropertyStats],
    depths: Sequence[PropertyDepth],
) -> None:
    """One row per property, conserving every shard's budget and cases."""
    shards_by_id: dict[str, list[HypothesisPropertyStats]] = {}
    for record in records:
        shards_by_id.setdefault(record.test_id, []).append(record)
    if sorted(depth.test_id for depth in depths) != sorted(shards_by_id):
        raise AssertionError(
            f"depth rows {[depth.test_id for depth in depths]} do not cover "
            f"exactly {sorted(shards_by_id)}"
        )
    for depth in depths:
        shards = shards_by_id[depth.test_id]
        expected = PropertyDepth(
            test_id=depth.test_id,
            budget=sum(shard.max_examples for shard in shards),
            shard_budget_bound=max(shard.max_examples for shard in shards),
            shards=len(shards),
            exhausted_shards=sum(
                shard.stopped_because == STRATEGY_SPACE_EXHAUSTED
                for shard in shards
            ),
            distinct_world_bound=max(shard.valid for shard in shards),
            valid=sum(shard.valid for shard in shards),
            invalid=sum(shard.invalid for shard in shards),
            overrun=sum(shard.overrun for shard in shards),
            interesting=sum(shard.interesting for shard in shards),
        )
        if depth != expected:
            raise AssertionError(f"{depth} does not aggregate its shards: {expected}")


def assert_shallow_verdict_is_conservative(
    depth: PropertyDepth,
    shallow: bool,
) -> None:
    """A shallow verdict claims wasted budget, and nothing else."""
    if not shallow:
        return
    if depth.interesting:
        raise AssertionError(
            f"{depth.test_id} stopped early because it found "
            f"{depth.interesting} interesting cases, not because it ran out "
            "of worlds"
        )
    if depth.exhausted_shards != depth.shards:
        raise AssertionError(
            f"{depth.test_id} exhausted only {depth.exhausted_shards} of "
            f"{depth.shards} shards"
        )
    if depth.distinct_world_bound >= depth.budget:
        raise AssertionError(
            f"{depth.test_id} reached {depth.distinct_world_bound} worlds "
            f"against a {depth.budget}-example budget: nothing was wasted"
        )


def _named_by_section(lines: Sequence[str], prefix: str) -> list[str]:
    return [
        line.split()[-1]
        for line in lines
        if line.startswith(f"{prefix} ") and not line.startswith(f"{prefix} ...")
    ]


def _assert_section_truncation(
    lines: Sequence[str],
    prefix: str,
    total: int,
    suffix: str,
) -> None:
    expected = (
        [f"{prefix} ... {total - DEPTH_REPORT_LIMIT} more {suffix}"]
        if total > DEPTH_REPORT_LIMIT
        else []
    )
    actual = [line for line in lines if line.startswith(f"{prefix} ...")]
    if actual != expected:
        raise AssertionError(
            f"{prefix} truncation line is {actual}, expected {expected} "
            f"for {total} properties"
        )


def assert_report_names_exactly_the_shallow_properties(
    depths: Sequence[PropertyDepth],
    lines: Sequence[str],
) -> None:
    """Every ranked SHALLOW line names a shallow property, smallest first."""
    shallow = {depth.test_id for depth in depths if is_structurally_shallow(depth)}
    named = _named_by_section(lines, "SHALLOW")
    if len(named) != len(set(named)):
        raise AssertionError(f"a property is reported twice: {named}")
    unexpected = sorted(set(named) - shallow)
    if unexpected:
        raise AssertionError(f"report names non-shallow properties: {unexpected}")
    if len(named) != min(len(shallow), DEPTH_REPORT_LIMIT):
        raise AssertionError(
            f"report names {len(named)} of {len(shallow)} shallow properties"
        )
    bounds = [
        depth.distinct_world_bound
        for name in named
        for depth in depths
        if depth.test_id == name
    ]
    if bounds != sorted(bounds):
        raise AssertionError(f"shallow ranking is not ascending: {bounds}")
    _assert_section_truncation(lines, "SHALLOW", len(shallow), "shallow")


def assert_report_names_exactly_the_discarding_properties(
    depths: Sequence[PropertyDepth],
    lines: Sequence[str],
) -> None:
    """Every ranked DISCARD line names a discarding property, worst first."""
    discarding = {
        depth.test_id
        for depth in depths
        if discard_rate(depth) >= DISCARD_RATE_THRESHOLD
    }
    named = _named_by_section(lines, "DISCARD")
    if len(named) != len(set(named)):
        raise AssertionError(f"a property is reported twice: {named}")
    unexpected = sorted(set(named) - discarding)
    if unexpected:
        raise AssertionError(
            f"report names properties below the discard threshold: {unexpected}"
        )
    if len(named) != min(len(discarding), DEPTH_REPORT_LIMIT):
        raise AssertionError(
            f"report names {len(named)} of {len(discarding)} discarding properties"
        )
    rates = [
        discard_rate(depth)
        for name in named
        for depth in depths
        if depth.test_id == name
    ]
    if rates != sorted(rates, reverse=True):
        raise AssertionError(f"discard ranking is not descending: {rates}")
    _assert_section_truncation(lines, "DISCARD", len(discarding), "discarding")


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


class TestGeneratedBurstDepthReport(unittest.TestCase):
    @given(records=property_stats(id_count=3, max_size=16))
    def test_depth_aggregates_shards_and_never_overclaims_shallowness(
        self,
        records: tuple[HypothesisPropertyStats, ...],
    ) -> None:
        depths = aggregate_property_depth(records)

        assert_shard_aggregation(records, depths)
        for depth in depths:
            assert_shallow_verdict_is_conservative(
                depth,
                is_structurally_shallow(depth),
            )

    @example(
        records=tuple(
            HypothesisPropertyStats(
                test_id=(
                    f"tests.test_world_{index:02d}_generated.World.test_property"
                ),
                max_examples=20_000,
                valid=100,
                invalid=10 + index,
                overrun=0,
                interesting=0,
                stopped_because="settings.max_examples=20000",
            )
            for index in range(25)
        ),
    )
    @given(records=property_stats(id_count=40, max_size=60))
    def test_report_ranks_exactly_the_shallow_and_discarding_properties(
        self,
        records: tuple[HypothesisPropertyStats, ...],
    ) -> None:
        depths = aggregate_property_depth(records)
        lines = format_depth_report(depths)

        assert_report_names_exactly_the_shallow_properties(depths, lines)
        assert_report_names_exactly_the_discarding_properties(depths, lines)


class TestDepthCheckersTripOnViolations(unittest.TestCase):
    """Known-bad self-tests: each depth checker must reject a planted world."""

    RECORDS = (
        HypothesisPropertyStats(
            test_id="tests.test_world_00_generated.World.test_property",
            max_examples=2_500,
            valid=4,
            invalid=0,
            overrun=0,
            interesting=0,
            stopped_because=STRATEGY_SPACE_EXHAUSTED,
        ),
    ) * 2

    def _depth(self, **overrides: int) -> PropertyDepth:
        base = aggregate_property_depth(self.RECORDS)[0]
        return replace(base, **overrides)

    def test_aggregation_checker_rejects_one_row_per_shard(self) -> None:
        per_shard = tuple(
            aggregate_property_depth((record,))[0] for record in self.RECORDS
        )

        with self.assertRaisesRegex(AssertionError, "do not cover exactly"):
            assert_shard_aggregation(self.RECORDS, per_shard)

    def test_aggregation_checker_rejects_a_dropped_shard_budget(self) -> None:
        with self.assertRaisesRegex(AssertionError, "does not aggregate its shards"):
            assert_shard_aggregation(self.RECORDS, (self._depth(budget=2_500),))

    def test_shallow_checker_rejects_a_verdict_over_an_interesting_run(self) -> None:
        with self.assertRaisesRegex(AssertionError, "interesting cases"):
            assert_shallow_verdict_is_conservative(self._depth(interesting=2), True)

    def test_shallow_checker_rejects_a_verdict_over_partial_exhaustion(self) -> None:
        with self.assertRaisesRegex(AssertionError, "exhausted only"):
            assert_shallow_verdict_is_conservative(
                self._depth(exhausted_shards=1),
                True,
            )

    def test_shallow_checker_rejects_a_verdict_over_a_spent_budget(self) -> None:
        with self.assertRaisesRegex(AssertionError, "nothing was wasted"):
            assert_shallow_verdict_is_conservative(
                self._depth(distinct_world_bound=5_000),
                True,
            )

    def test_shallow_checker_accepts_the_real_shallow_world(self) -> None:
        """Must-still-work: the four-world property is genuinely shallow."""
        depth = self._depth()

        self.assertTrue(is_structurally_shallow(depth))
        assert_shallow_verdict_is_conservative(depth, True)

    def test_report_checker_rejects_a_line_for_a_deep_property(self) -> None:
        deep = self._depth(interesting=2)

        with self.assertRaisesRegex(AssertionError, "non-shallow properties"):
            assert_report_names_exactly_the_shallow_properties(
                (deep,),
                (f"SHALLOW 4 worlds of 5000 examples (2 shards) {deep.test_id}",),
            )

    def test_report_checker_rejects_an_omitted_shallow_property(self) -> None:
        with self.assertRaisesRegex(AssertionError, "names 0 of 1"):
            assert_report_names_exactly_the_shallow_properties(
                (self._depth(),),
                ("DEPTH 1 properties measured",),
            )

    def test_report_checker_rejects_a_missing_shallow_truncation_line(self) -> None:
        depths = tuple(
            replace(self._depth(), test_id=f"module.World.test_{index:02d}")
            for index in range(DEPTH_REPORT_LIMIT + 1)
        )
        lines = format_depth_report(depths)

        with self.assertRaisesRegex(AssertionError, "truncation line"):
            assert_report_names_exactly_the_shallow_properties(
                depths,
                [line for line in lines if not line.startswith("SHALLOW ...")],
            )

    def _discarding(self, invalid: int, index: int) -> PropertyDepth:
        return replace(
            self._depth(),
            test_id=f"module.World.test_{index:02d}",
            valid=100,
            invalid=invalid,
            exhausted_shards=0,
        )

    def test_discard_checker_rejects_a_reversed_ranking(self) -> None:
        depths = (self._discarding(20, 0), self._discarding(60, 1))
        ascending = tuple(
            sorted(depths, key=lambda depth: discard_rate(depth))
        )

        assert_report_names_exactly_the_discarding_properties(
            depths,
            format_depth_report(depths),
        )
        with self.assertRaisesRegex(AssertionError, "not descending"):
            assert_report_names_exactly_the_discarding_properties(
                depths,
                tuple(
                    f"DISCARD x of y examples (a discarded, b worlds) "
                    f"{depth.test_id}"
                    for depth in ascending
                ),
            )

    def test_discard_checker_rejects_a_line_below_the_threshold(self) -> None:
        quiet = self._discarding(1, 0)

        self.assertLess(discard_rate(quiet), DISCARD_RATE_THRESHOLD)
        with self.assertRaisesRegex(AssertionError, "below the discard threshold"):
            assert_report_names_exactly_the_discarding_properties(
                (quiet,),
                (f"DISCARD 1% of 101 examples (1 discarded, 100 worlds) "
                 f"{quiet.test_id}",),
            )

    def test_discard_checker_rejects_an_omitted_discarding_property(self) -> None:
        with self.assertRaisesRegex(AssertionError, "names 0 of 1"):
            assert_report_names_exactly_the_discarding_properties(
                (self._discarding(60, 0),),
                ("DEPTH 1 properties measured",),
            )

    def test_discard_checker_accepts_the_real_report(self) -> None:
        """Must-still-work: the production report satisfies its own checker."""
        depths = tuple(
            self._discarding(20 + index, index)
            for index in range(DEPTH_REPORT_LIMIT + 2)
        )

        assert_report_names_exactly_the_discarding_properties(
            depths,
            format_depth_report(depths),
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
