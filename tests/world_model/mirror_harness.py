"""Operator-only real-harness world profile backed by a MusicBrainz mirror.

This module is intentionally outside unittest discovery. Invoke it through
``scripts/world_model_burst.sh --engine mirror-harness --mirror-url ORIGIN``.
The release fixture is public MusicBrainz catalogue data selected independently
of the production collection; every database, file, and Beets path is scratch.
"""

from __future__ import annotations

import os
import sys
import unittest

from hypothesis import HealthCheck, settings
from hypothesis.database import DirectoryBasedExampleDatabase
from hypothesis.stateful import RuleBasedStateMachine, invariant, precondition, rule

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
import conftest  # noqa: E402, F401

from tests.beets_world import BeetsWorldRelease  # noqa: E402
from tests.world_model.census_seeds import (  # noqa: E402
    STATEFUL_WORLD_CENSUS_SEEDS,
)
from tests.world_model.support import LifecycleWorld, repository_root  # noqa: E402


TEST_DSN = os.environ.get("TEST_DB_DSN")
MIRROR_URL = os.environ.get("CRATEDIGGER_WORLD_MIRROR_URL", "")
if not TEST_DSN:
    raise RuntimeError("mirror world requires ephemeral PostgreSQL in nix-shell")
if not MIRROR_URL:
    raise RuntimeError("mirror world requires CRATEDIGGER_WORLD_MIRROR_URL")


# Public, one-track official release; metadata was read from the MB mirror on
# 2026-07-19. It is not sampled from album_requests or the Beets collection.
PUBLIC_MIRROR_RELEASE = BeetsWorldRelease(
    release_id="d399d08e-c5a1-4ab4-b1f3-a6d5e732b921",
    artist="Cut Capers",
    album="Test",
    year=2019,
    codec="mp3",
    track_count=1,
    track_titles=("I Know",),
)
PRISTINE_CENSUS_SEED = next(
    seed
    for seed in STATEFUL_WORLD_CENSUS_SEEDS
    if seed.name == "wanted_mb_pristine"
)


def _open_world() -> LifecycleWorld:
    assert TEST_DSN is not None
    return LifecycleWorld(
        TEST_DSN,
        repository_root(),
        import_engine="mirror-harness",
        mirror_url=MIRROR_URL,
    )


class TestPinnedMirrorHarnessWorld(unittest.TestCase):
    def test_public_exact_release_crosses_real_harness_subprocess(self) -> None:
        with _open_world() as world:
            request_id = world.seed_census_release(
                PUBLIC_MIRROR_RELEASE,
                PRISTINE_CENSUS_SEED,
            )

            self.assertTrue(world.import_request(request_id, codec="mp3"))
            world.assert_invariants()
            self.assertEqual(
                {album.release_id for album in world.beets.snapshots()},
                {PUBLIC_MIRROR_RELEASE.release_id},
            )


class MirrorHarnessWorldMachine(RuleBasedStateMachine):
    """Hammer real service lifecycles after a guaranteed subprocess import."""

    def __init__(self) -> None:
        super().__init__()
        self.world = _open_world()
        self.request_id = self.world.seed_census_release(
            PUBLIC_MIRROR_RELEASE,
            PRISTINE_CENSUS_SEED,
        )
        if not self.world.import_request(self.request_id, codec="mp3"):
            raise AssertionError("initial mirror-harness import was rejected")

    def teardown(self) -> None:
        self.world.close()

    @precondition(lambda self: bool(self.world.request_ids_with_status("wanted")))
    @rule()
    def reimport_exact_release(self) -> None:
        self.world.import_request(self.request_id, codec="mp3")

    @precondition(lambda self: bool(self.world.request_ids_with_status("wanted")))
    @rule()
    def force_import_exact_release(self) -> None:
        self.world.force_import_request(self.request_id, codec="mp3")

    @precondition(lambda self: bool(self.world.request_ids_with_album()))
    @rule()
    def ban_then_make_searchable(self) -> None:
        self.world.ban_request_source(self.request_id)

    @rule()
    def delete_wrong_match(self) -> None:
        self.world.delete_wrong_match(self.request_id)

    @invariant()
    def cross_engine_invariants_hold(self) -> None:
        self.world.assert_invariants()


TestGeneratedMirrorHarnessWorld = MirrorHarnessWorldMachine.TestCase
_RANDOMIZED = os.environ.get("CRATEDIGGER_WORLD_RANDOMIZED") == "1"
_DATABASE = (
    DirectoryBasedExampleDatabase(
        os.environ.get(
            "CRATEDIGGER_WORLD_DATABASE",
            ".hypothesis/world-model-mirror",
        )
    )
    if _RANDOMIZED
    else None
)
TestGeneratedMirrorHarnessWorld.settings = settings(
    max_examples=int(os.environ.get("CRATEDIGGER_WORLD_EXAMPLES", "2")),
    stateful_step_count=int(os.environ.get("CRATEDIGGER_WORLD_STEPS", "4")),
    deadline=None,
    derandomize=not _RANDOMIZED,
    database=_DATABASE,
    print_blob=_RANDOMIZED,
    suppress_health_check=(HealthCheck.too_slow,),
)


if __name__ == "__main__":
    unittest.main()
