"""Heavy real-PostgreSQL/real-Beets lifecycle world model (#743).

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
    """Concrete pins promoted from incidents and generated counterexamples."""

    def test_two_pressings_then_upgrade_preserve_exact_membership(self) -> None:
        assert TEST_DSN is not None
        with LifecycleWorld(TEST_DSN, repository_root()) as world:
            first_id = world.add_release(BeetsWorldRelease(
                release_id="10000000-0000-4000-8000-000000000001",
                artist="Passenger",
                album="Collision Course",
                year=2008,
                codec="mp3",
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
            world.import_request(first_id, codec="flac")

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

    def test_rejected_identical_retry_rebinds_current_evidence_path(self) -> None:
        """Shrunk #743 world: candidate/current share one content address."""

        assert TEST_DSN is not None
        with LifecycleWorld(TEST_DSN, repository_root()) as world:
            request_id = world.add_release(BeetsWorldRelease(
                release_id="10000000-0000-4000-8000-000000000743",
                artist="Evidence Address",
                album="Same Bytes, New Attempt",
                year=2001,
                codec="mp3",
            ))

            self.assertTrue(world.import_request(request_id, codec="mp3"))
            first_evidence_id = world.db.get_request_current_evidence_id(
                request_id
            )
            self.assertIsNotNone(first_evidence_id)
            self.assertFalse(world.import_request(request_id, codec="mp3"))
            self.assertEqual(
                world.db.get_request_current_evidence_id(request_id),
                first_evidence_id,
                "identical retry must collide on the installed content address",
            )
            world.assert_invariants()

    def test_operator_lifecycles_preserve_world_authority(self) -> None:
        assert TEST_DSN is not None
        with LifecycleWorld(TEST_DSN, repository_root()) as world:
            request_id = world.add_release(BeetsWorldRelease(
                release_id="20000000-0000-4000-8000-000000000001",
                artist="World Operator",
                album="Lifecycle Archive",
                year=1999,
                codec="mp3",
            ))
            self.assertTrue(world.force_import_request(request_id, codec="mp3"))
            world.delete_wrong_match(request_id)
            replacement_id = world.replace_request(request_id)
            self.assertTrue(world.force_import_request(
                replacement_id,
                codec="flac",
                verified_lossless=True,
            ))
            self.assertFalse(world.force_import_request(
                replacement_id,
                codec="mp3",
            ))
            world.ban_request_source(replacement_id)

            world.assert_invariants()
            self.assertEqual(
                world.request_ids_with_status("replaced"),
                [request_id],
            )
            self.assertEqual(
                world.request_ids_with_status("wanted"),
                [replacement_id],
            )
            self.assertEqual(world.beets.snapshots(), ())

    def test_discogs_replace_preserves_pathway_and_exact_identity(self) -> None:
        assert TEST_DSN is not None
        with LifecycleWorld(TEST_DSN, repository_root()) as world:
            request_id = world.add_release(BeetsWorldRelease(
                release_id="7000743",
                artist="Numeric Archive",
                album="Discogs Pressing",
                year=1988,
                codec="mp3",
            ))
            self.assertTrue(world.import_request(request_id))

            replacement_id = world.replace_request(request_id)
            world.assert_invariants()
            replacement = world.db.get_request(replacement_id)
            assert replacement is not None
            self.assertEqual(
                replacement["mb_release_id"],
                replacement["discogs_release_id"],
            )
            self.assertEqual(world.beets.snapshots(), ())

    def test_rejected_second_force_keeps_denylist_audit_authority(self) -> None:
        """Shrunk #743 world: PostgreSQL returns import_result as JSONB."""

        assert TEST_DSN is not None
        with LifecycleWorld(TEST_DSN, repository_root()) as world:
            request_id = world.add_release(BeetsWorldRelease(
                release_id="30000000-0000-4000-8000-000000000743",
                artist="Force Audit",
                album="Two Attempts",
                year=2002,
                codec="mp3",
            ))

            self.assertTrue(world.force_import_request(request_id, codec="mp3"))
            self.assertFalse(world.force_import_request(request_id, codec="mp3"))
            world.assert_invariants()


class LifecycleWorldMachine(RuleBasedStateMachine):
    """Generate operator lifecycles and check after every real mutation."""

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
        codec = data.draw(st.sampled_from(("flac", "mp3")))
        verified = codec == "flac" and data.draw(st.booleans())
        self.world.import_request(
            request_id,
            codec=codec,
            verified_lossless=verified,
        )

    @precondition(lambda self: bool(self.world.verified_lossless_request_ids()))
    @rule(data=st.data())
    def force_import_proof_locked(self, data: st.DataObject) -> None:
        request_id = data.draw(st.sampled_from(
            self.world.verified_lossless_request_ids()
        ))
        accepted = self.world.force_import_request(
            request_id,
            codec=data.draw(st.sampled_from(("flac", "mp3"))),
        )
        if accepted:
            raise AssertionError("force import crossed verified-lossless proof lock")

    @precondition(lambda self: bool(self.world.request_ids_with_status("wanted")))
    @rule(data=st.data())
    def force_import_wanted(self, data: st.DataObject) -> None:
        request_id = data.draw(st.sampled_from(
            self.world.request_ids_with_status("wanted")
        ))
        codec = data.draw(st.sampled_from(("flac", "mp3")))
        self.world.force_import_request(
            request_id,
            codec=codec,
            verified_lossless=codec == "flac" and data.draw(st.booleans()),
        )

    @precondition(lambda self: bool(self.world.request_ids_with_album()))
    @rule(data=st.data())
    def reset_search_policy(self, data: st.DataObject) -> None:
        imported = set(self.world.request_ids_with_status("imported"))
        candidates = [
            request_id
            for request_id in self.world.request_ids_with_album()
            if request_id not in imported
        ]
        if not candidates:
            return
        self.world.reset_to_wanted(data.draw(st.sampled_from(candidates)))

    @precondition(lambda self: bool(self.world.active_request_ids()))
    @rule(data=st.data())
    def replace_request(self, data: st.DataObject) -> None:
        request_id = data.draw(st.sampled_from(self.world.active_request_ids()))
        self.world.replace_request(request_id)

    @precondition(lambda self: bool(self.world.request_ids_with_album()))
    @rule(data=st.data())
    def ban_source(self, data: st.DataObject) -> None:
        request_id = data.draw(st.sampled_from(
            self.world.request_ids_with_album()
        ))
        self.world.ban_request_source(request_id)

    @precondition(lambda self: bool(self.world.active_request_ids()))
    @rule(data=st.data())
    def wrong_match_delete(self, data: st.DataObject) -> None:
        request_id = data.draw(st.sampled_from(self.world.active_request_ids()))
        self.world.delete_wrong_match(request_id)

    @invariant()
    def cross_engine_invariants_hold(self) -> None:
        self.world.assert_invariants()


TestGeneratedLifecycleWorld = LifecycleWorldMachine.TestCase
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
