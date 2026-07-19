"""Heavy real-PostgreSQL/real-Beets add/import/upgrade world model (#743).

This module is intentionally outside unittest discovery. Run it directly:

    nix-shell --run "python3 -m unittest tests.world_model.state_machine -v"

Its default generated budget is deliberately small while issue #743 measures
the real runtime. ``CRATEDIGGER_WORLD_EXAMPLES`` and
``CRATEDIGGER_WORLD_STEPS`` can increase that budget without changing code.
"""

from __future__ import annotations

import os
import sys
import unittest

from hypothesis import HealthCheck, settings
from hypothesis import strategies as st
from hypothesis.stateful import (
    RuleBasedStateMachine,
    invariant,
    precondition,
    rule,
)

# Start a throwaway PostgreSQL and apply the real migration stack before the
# world imports TEST_DB_DSN. This never connects to production.
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
import conftest  # noqa: E402, F401

from tests.beets_world import BeetsWorldRelease  # noqa: E402
from tests.world_model.support import LifecycleWorld, repository_root  # noqa: E402


TEST_DSN = os.environ.get("TEST_DB_DSN")
if not TEST_DSN:
    raise RuntimeError(
        "world model requires ephemeral PostgreSQL; run it inside nix-shell"
    )


def _mb_release_id(counter: int) -> str:
    return f"00000000-0000-4000-8000-{counter:012x}"


def _discogs_release_id(counter: int) -> str:
    return str(7_000_000 + counter)


class TestPinnedLifecycleWorld(unittest.TestCase):
    """Concrete regression pin for collision plus same-pressing upgrade."""

    def test_two_pressings_then_upgrade_preserve_exact_membership(self) -> None:
        assert TEST_DSN is not None
        with LifecycleWorld(TEST_DSN, repository_root()) as world:
            first_id = world.add_release(BeetsWorldRelease(
                release_id="10000000-0000-4000-8000-000000000001",
                artist="Passenger",
                album="Collision Course",
                year=2008,
                label="Archive One",
                catalognum="A-1",
            ))
            second_id = world.add_release(BeetsWorldRelease(
                release_id="7000002",
                artist="Passenger",
                album="Collision Course",
                year=2008,
                codec="mp3",
                label="Archive Two",
                catalognum="B-2",
            ))

            world.import_request(first_id)
            world.import_request(second_id)
            world.import_request(first_id)

            world.assert_invariants()
            albums = world.beets.snapshots()
            self.assertEqual(len(albums), 2)
            self.assertEqual(
                {album.release_id for album in albums},
                {
                    "10000000-0000-4000-8000-000000000001",
                    "7000002",
                },
            )
            self.assertEqual(
                len({album.album_path for album in albums}),
                2,
            )


class AddImportUpgradeMachine(RuleBasedStateMachine):
    """Generate multi-pressing worlds and check after every real mutation."""

    def __init__(self) -> None:
        super().__init__()
        assert TEST_DSN is not None
        self.world = LifecycleWorld(TEST_DSN, repository_root())
        self._release_counter = 0

    def teardown(self) -> None:
        self.world.close()

    @rule(
        identity_source=st.sampled_from(("musicbrainz", "discogs")),
        artist_index=st.integers(min_value=0, max_value=2),
        album_index=st.integers(min_value=0, max_value=2),
        year=st.integers(min_value=1960, max_value=2026),
        label_index=st.integers(min_value=0, max_value=2),
        codec=st.sampled_from(("flac", "mp3")),
    )
    def add_request(
        self,
        identity_source: str,
        artist_index: int,
        album_index: int,
        year: int,
        label_index: int,
        codec: str,
    ) -> None:
        self._release_counter += 1
        if identity_source == "discogs":
            release_id = _discogs_release_id(self._release_counter)
        else:
            release_id = _mb_release_id(self._release_counter)
        self.world.add_release(BeetsWorldRelease(
            release_id=release_id,
            artist=f"Archive Artist {artist_index}",
            album=f"Recovered Album {album_index}",
            year=year,
            codec=codec,
            label=f"Label {label_index}",
            catalognum=f"CAT-{self._release_counter % 4}",
        ))

    @precondition(lambda self: bool(self.world.request_ids_with_status("wanted")))
    @rule(data=st.data())
    def import_wanted(self, data: st.DataObject) -> None:
        request_id = data.draw(st.sampled_from(
            self.world.request_ids_with_status("wanted")
        ))
        self.world.import_request(request_id)

    @precondition(lambda self: bool(self.world.request_ids_with_status("imported")))
    @rule(data=st.data())
    def upgrade_imported(self, data: st.DataObject) -> None:
        request_id = data.draw(st.sampled_from(
            self.world.request_ids_with_status("imported")
        ))
        self.world.import_request(request_id)

    @invariant()
    def cross_engine_invariants_hold(self) -> None:
        self.world.assert_invariants()


TestGeneratedLifecycleWorld = AddImportUpgradeMachine.TestCase
TestGeneratedLifecycleWorld.settings = settings(
    max_examples=int(os.environ.get("CRATEDIGGER_WORLD_EXAMPLES", "6")),
    stateful_step_count=int(os.environ.get("CRATEDIGGER_WORLD_STEPS", "8")),
    deadline=None,
    derandomize=True,
    database=None,
    suppress_health_check=(HealthCheck.too_slow,),
)


if __name__ == "__main__":
    unittest.main()
