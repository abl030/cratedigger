"""Regression pin for issue #1214: a BeetsContractWorld's tmpfs lifetime
must be bound to the Hypothesis EXAMPLE that created it, not to the
enclosing test METHOD.

Test machinery -- deterministic only, per code-quality.md ("Never
property-test the test machinery ... A regression in test infrastructure
gets an exact pin and an end-to-end deterministic contract, not a
pin/property pair"). This file therefore drives a small, self-contained
@given body through the real Hypothesis machinery from inside one plain
unittest test method, and asserts on the result afterward -- it is not
itself a test_*_generated.py discovered fuzz target.

Bug recap: every production site used to construct the fixture as
    world = BeetsContractWorld()
    self.addCleanup(world.close)
inside an @given-decorated method. addCleanup fires once per test METHOD,
but Hypothesis re-executes the method body once per EXAMPLE, so every
example before the last leaked a live world (two real tmpfs trees) until
the method finally returned. All 45 leaking call sites found across
tests/ (40 in the original two beets_config_contract files, plus 5 more
found widening the same check to other resources/files) now use
`with BeetsContractWorld() as world:` (or an equivalent locally-scoped
context manager for their own resource), and BeetsContractWorld gained
__enter__/__exit__ (tests/fakes/beets_contract.py) so __exit__ closes the
world -- and releases its tmpfs trees -- at the end of the `with` block,
i.e. at the end of the EXAMPLE, not the method.
"""

from __future__ import annotations

import unittest
from pathlib import Path

from hypothesis import given, settings
from hypothesis import strategies as st

from tests.fakes.beets_contract import BeetsContractWorld

#: >= 20 per the task brief; the real "resident worlds" production number
#: this issue measured used CRATEDIGGER_FUZZ_MAX_EXAMPLES=2500 -- this pin
#: only needs enough examples to prove the shape (bounded vs. growing), not
#: to reproduce the exact production magnitude.
_EXAMPLE_COUNT = 25


class TestBeetsContractWorldLifetimeBoundToExample(unittest.TestCase):
    """A world's tmpfs trees never outlive the example that created them."""

    def test_resident_world_count_stays_bounded_across_examples(self) -> None:
        created: list[tuple[Path, Path]] = []
        alive_at_each_example: list[int] = []

        @settings(max_examples=_EXAMPLE_COUNT, deadline=None)
        @given(st.integers(min_value=0, max_value=10_000))
        def drive_examples(seed: int) -> None:
            # A plain st.sampled_from(ROLES) strategy has only 4 distinct
            # outputs; Hypothesis special-cases that shape and can stop
            # early once it believes the (tiny) space is exhausted, well
            # short of _EXAMPLE_COUNT calls -- exactly the wrong behaviour
            # for a pin whose whole point is running many sequential
            # examples. st.integers over a wide range has no such
            # exhaustion shortcut, so every one of _EXAMPLE_COUNT examples
            # actually calls this function. seed is a real drawn input
            # (referenced below), not decorative -- it selects role the
            # exact way every one of the fixed production sites varies it.
            role = ("importer", "preview", "web", "main")[seed % 4]
            with BeetsContractWorld(role=role) as world:
                created.append((world.root, world.authority_root))
                world.cfg()
                alive_at_each_example.append(
                    sum(
                        1
                        for tmp_root, authority_root in created
                        if tmp_root.exists() or authority_root.exists()
                    )
                )

        drive_examples()

        self.assertEqual(
            len(created), _EXAMPLE_COUNT,
            "Hypothesis did not actually run every example",
        )
        # The whole point of the fix: at the moment each example's world is
        # constructed, every EARLIER example's world is already closed, so
        # exactly one world -- never more -- is ever concurrently resident.
        # Under the pre-#1214 addCleanup-per-method shape this would instead
        # count 1, 2, 3, ... _EXAMPLE_COUNT (linear growth with the example
        # index), never staying at 1.
        self.assertEqual(
            alive_at_each_example,
            [1] * _EXAMPLE_COUNT,
            "resident BeetsContractWorld count grew with the example count "
            "instead of staying bounded at 1 -- a world outlived its "
            "example (issue #1214 regression)",
        )
        # And after the whole run, nothing is left resident at all.
        self.assertTrue(
            all(
                not tmp_root.exists() and not authority_root.exists()
                for tmp_root, authority_root in created
            ),
            "a world's tmpfs trees were still on disk after its own "
            "with-block exited",
        )


if __name__ == "__main__":
    unittest.main()
