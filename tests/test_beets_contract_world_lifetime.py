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
the method finally returned. 45 leaking call sites were found across
tests/: 42 used this direct shape (31 in
tests/test_beets_config_contract_generated.py, 9 in
tests/test_beets_config_contract_regressions_generated.py, 2 in
tests/test_beets_config_startup_generated.py) and now use
`with BeetsContractWorld(...) as world:`; 3 more were found widening the
same check to a helper-mediated shape (a resource built in a plain helper
METHOD that a @given body merely calls by name --
tests/test_import_queue.py, tests/test_mbid_replace_service.py,
tests/test_automation_recovery_debris_generated.py) and now use an
equivalent locally-scoped context manager for their own resource.
BeetsContractWorld itself gained __enter__/__exit__
(tests/fakes/beets_contract.py) so __exit__ closes the world -- and
releases its tmpfs trees -- at the end of the `with` block, i.e. at the end
of the EXAMPLE, not the method.

Why this pin holds a live reference to every world it creates (issue #1214
review finding F2): BeetsContractWorld's authority tree is chown'd to a
subordinate uid while sealed (`_seal`), so an unremovable-until-unseal
directory plus CPython's refcount-triggered `tempfile.TemporaryDirectory`
finalizer can, by ACCIDENT, clean up both tmp trees the moment a world
becomes unreachable -- regardless of whether `close()` (and therefore
`__exit__`) ever ran, or ran only partially. A pin that lets each `world`
go out of scope at the end of its own Hypothesis call therefore cannot
tell "the fixture explicitly released its resources" from "CPython
happened to garbage-collect them anyway": `__exit__ -> self.unseal();
return` (no `close()` at all) and `close()`'s `finally` cleaning only one
of the two tmp trees both still pass, because unsealing plus the world
becoming unreachable is sufficient for the finalizers to do the fixture's
job for it. Keeping every `world` strongly referenced in `created` for the
entire test (never letting one go out of scope) removes that confound
categorically: with a live reference held throughout, the ONLY way a
world's tmp trees can disappear from disk is `BeetsContractWorld.close()`
actually running to completion and calling `.cleanup()` on both
`TemporaryDirectory` objects itself.
"""

from __future__ import annotations

import unittest

from hypothesis import given, settings
from hypothesis import strategies as st

import tests._hypothesis_profiles  # noqa: F401
from tests.fakes.beets_contract import BeetsContractWorld

#: >= 20 per the task brief; the real "resident worlds" production number
#: this issue measured used CRATEDIGGER_FUZZ_MAX_EXAMPLES=2500 -- this pin
#: only needs enough examples to prove the shape (bounded vs. growing), not
#: to reproduce the exact production magnitude.
_EXAMPLE_COUNT = 25


class TestBeetsContractWorldLifetimeBoundToExample(unittest.TestCase):
    """A world's tmpfs trees never outlive the example that created them."""

    def test_resident_world_count_stays_bounded_across_examples(self) -> None:
        # Every world is kept alive here for the whole test (see module
        # docstring) so CPython's own refcount-triggered cleanup can never
        # be mistaken for BeetsContractWorld.close() actually running.
        created: list[BeetsContractWorld] = []
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
            # (referenced below), not decorative -- role is illustrative
            # variation only, not a reproduction of any production site's
            # own strategy: of the 42 real `with BeetsContractWorld(...)`
            # call sites this issue fixed, 34 pass no role at all, 1 passes
            # a fixed literal ("importer"), and 7 vary it via a
            # Hypothesis-drawn `st.sampled_from(...)` role strategy -- none
            # use a modulo-4 index. seed % 4 here is just a cheap way to
            # touch all four roles across the run.
            role = ("importer", "preview", "web", "main")[seed % 4]
            with BeetsContractWorld(role=role) as world:
                created.append(world)
                world.cfg()
                alive_at_each_example.append(
                    sum(
                        1
                        for w in created
                        if w.root.exists() or w.authority_root.exists()
                    )
                )

        drive_examples()

        self.assertEqual(
            len(created), _EXAMPLE_COUNT,
            "Hypothesis did not actually run every example",
        )
        # The whole point of the fix: at the moment each example's world is
        # constructed, every EARLIER example's world has already had
        # close() run against it (via __exit__), so exactly one world's
        # tmp trees -- never more -- are ever concurrently resident on
        # disk. Under the pre-#1214 addCleanup-per-method shape this would
        # instead count 1, 2, 3, ... _EXAMPLE_COUNT (linear growth with the
        # example index), never staying at 1.
        self.assertEqual(
            alive_at_each_example,
            [1] * _EXAMPLE_COUNT,
            "resident BeetsContractWorld tmp-tree count grew with the "
            "example count instead of staying bounded at 1 -- a world's "
            "close() did not run (or did not fully run) before the next "
            "example's world was constructed (issue #1214 regression)",
        )
        # And after the whole run, nothing is left resident at all --
        # despite every world in `created` still being strongly referenced
        # right here, so this can only be explicit close() having run for
        # every one of them, never incidental garbage collection.
        self.assertTrue(
            all(
                not w.root.exists() and not w.authority_root.exists()
                for w in created
            ),
            "a world's tmpfs trees were still on disk after its own "
            "with-block exited, even though the world object itself was "
            "kept alive throughout -- close() did not fully release both "
            "trees",
        )


if __name__ == "__main__":
    unittest.main()
