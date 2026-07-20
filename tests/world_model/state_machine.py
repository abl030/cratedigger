"""Heavy real-PostgreSQL/real-Beets lifecycle world model (#743).

The normal suite invokes this module explicitly after unittest discovery:

    nix-shell --run "bash scripts/run_tests.sh"

It can also be run directly while working on the world model:

    nix-shell --run "python3 -m unittest tests.world_model.state_machine -v"

Normal-suite and direct invocation use a small deterministic budget. The
``scripts/world_model_burst.sh`` switches the same machine to randomized
generation with a replay database and a much deeper lifecycle budget.
"""

from __future__ import annotations

import os
import sys
import unittest

from beets import config as beets_config
from hypothesis import HealthCheck, example, given, settings
from hypothesis.database import DirectoryBasedExampleDatabase
from hypothesis import strategies as st
from hypothesis.stateful import (
    RuleBasedStateMachine,
    initialize,
    invariant,
    precondition,
    rule,
)

# Start a throwaway PostgreSQL and apply the real migration stack before the
# world imports TEST_DB_DSN. This never connects to production.
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
import conftest  # noqa: E402, F401

from tests.beets_world import (  # noqa: E402
    BeetsWorldRelease,
    HISTORICAL_PASSENGER_PATH_TEMPLATE,
)
from tests.world_model.support import LifecycleWorld, repository_root  # noqa: E402
from tests.world_model.census_seeds import (  # noqa: E402
    EVIDENCE_DRIFT_FACT_SEEDS,
    EVIDENCE_DRIFT_MUTATION_SEEDS,
    STATEFUL_WORLD_CENSUS_SEEDS,
    EvidenceDriftFactSeed,
    EvidenceDriftMutationSeed,
    WorldCensusSeed,
)


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

    @staticmethod
    def _add_passenger_pressings(world: LifecycleWorld) -> tuple[int, int]:
        """Recreate the exact same-key label shape from the live incident."""

        first_id = world.add_release(BeetsWorldRelease(
            release_id="dd578a59-ef6d-46fa-9f28-1e19c456dac8",
            artist="Lisa Hannigan",
            album="Passenger",
            year=2011,
            codec="mp3",
            label="ATO Records",
        ))
        second_id = world.add_release(BeetsWorldRelease(
            release_id="5e7a6000-ce08-4e7b-9773-22a26e0a2980",
            artist="Lisa Hannigan",
            album="Passenger",
            year=2011,
            codec="mp3",
            label="",
        ))
        return first_id, second_id

    def test_passenger_pressings_stay_disambiguated_through_upgrade(self) -> None:
        assert TEST_DSN is not None
        with LifecycleWorld(TEST_DSN, repository_root()) as world:
            first_id, second_id = self._add_passenger_pressings(world)

            world.import_request(first_id)
            world.import_request(second_id)
            world.import_request(first_id, codec="flac")

            world.assert_invariants()
            albums = world.beets.snapshots()
            self.assertEqual(len(albums), 2)
            self.assertEqual(
                {album.release_id for album in albums},
                {
                    "dd578a59-ef6d-46fa-9f28-1e19c456dac8",
                    "5e7a6000-ce08-4e7b-9773-22a26e0a2980",
                },
            )
            self.assertEqual(
                len({album.album_path for album in albums}),
                2,
            )

    def test_passenger_historical_template_poison_is_caught(self) -> None:
        """The real lifecycle invariant must kill the pre-fix path policy."""

        assert TEST_DSN is not None
        with LifecycleWorld(TEST_DSN, repository_root()) as world:
            beets_config["paths"]["default"].set(
                HISTORICAL_PASSENGER_PATH_TEMPLATE
            )
            violation_codes: set[str] = set()
            try:
                first_id, second_id = self._add_passenger_pressings(world)

                world.import_request(first_id)
                world.import_request(second_id)
                violation_codes = {
                    violation.code for violation in world.violations()
                }
            finally:
                beets_config["paths"]["default"].set(
                    world.beets.shipped.default_path_template
                )

            self.assertIn(
                "folder_shared",
                violation_codes,
                "the world model did not detect the historical Passenger "
                "folder collision",
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

    def test_census_seed_rebuilds_legacy_evidence_on_touch(self) -> None:
        seed = next(
            seed
            for seed in STATEFUL_WORLD_CENSUS_SEEDS
            if seed.name == "wanted_mb_full_legacy_ladder_lineage1"
        )
        assert TEST_DSN is not None
        with LifecycleWorld(TEST_DSN, repository_root()) as world:
            request_id = world.seed_census_release(BeetsWorldRelease(
                release_id="40000000-0000-4000-8000-000000000743",
                artist="Census Archive",
                album="Legacy Ladder",
                year=2004,
                codec="mp3",
            ), seed)
            before_id = world.db.get_request_current_evidence_id(request_id)
            before = world.db.load_album_quality_evidence_by_id(before_id)
            assert before is not None
            self.assertEqual(before.lineage_version, 1)

            self.assertTrue(world.import_request(request_id, codec="flac"))
            after_id = world.db.get_request_current_evidence_id(request_id)
            after = world.db.load_album_quality_evidence_by_id(after_id)
            assert after is not None
            self.assertEqual(after.lineage_version, 4)
            world.assert_invariants()

    def test_live_drift_retry_stays_closed_until_new_facts_exist(self) -> None:
        """Shrunk #743 world: installed facts die with the old fingerprint."""

        facts = next(
            seed
            for seed in EVIDENCE_DRIFT_FACT_SEEDS
            if seed.name == "installed_facts_measured"
        )
        assert TEST_DSN is not None
        with LifecycleWorld(TEST_DSN, repository_root()) as world:
            request_id = world.seed_evidence_drift_release(BeetsWorldRelease(
                release_id="50000000-0000-4000-8000-000000000743",
                artist="Drift Archive",
                album="Changed Bytes",
                year=2005,
                codec="mp3",
            ), facts)
            old_id = world.db.get_request_current_evidence_id(request_id)
            world.inject_evidence_drift(request_id, "same_name_size_drift")

            first = world.touch_current_evidence(request_id)
            second = world.touch_current_evidence(request_id)

            self.assertFalse(first.available)
            self.assertFalse(second.available)
            new_id = world.db.get_request_current_evidence_id(request_id)
            self.assertNotEqual(new_id, old_id)
            linked = world.db.load_album_quality_evidence_by_id(new_id)
            assert linked is not None
            self.assertTrue(linked.current_enrichment_required)
            self.assertEqual(linked.lineage_version, 4)
            world.assert_invariants()

    def test_drift_blocks_both_import_paths_until_enrichment(self) -> None:
        """Odd installed facts fail closed, then converge after measurement."""

        facts = next(
            seed
            for seed in EVIDENCE_DRIFT_FACT_SEEDS
            if seed.name == "installed_facts_measured"
        )
        assert TEST_DSN is not None
        actions = (
            ("ordinary", LifecycleWorld.import_request),
            ("force", LifecycleWorld.force_import_request),
        )
        for index, (name, action) in enumerate(actions, start=1):
            with self.subTest(action=name):
                with LifecycleWorld(TEST_DSN, repository_root()) as world:
                    request_id = world.seed_evidence_drift_release(
                        BeetsWorldRelease(
                            release_id=(
                                "51000000-0000-4000-8000-"
                                f"{index:012x}"
                            ),
                            artist="Drift Archive",
                            album=f"Blocked {name.title()} Import",
                            year=2005,
                            codec="mp3",
                        ),
                        facts,
                    )
                    world.inject_evidence_drift(
                        request_id,
                        "same_name_size_drift",
                    )
                    self.assertFalse(world.touch_current_evidence(
                        request_id
                    ).available)

                    self.assertFalse(action(
                        world,
                        request_id,
                        codec="flac",
                    ))
                    self.assertEqual(
                        world.latest_download_outcome(request_id),
                        "have_analysis_error",
                    )
                    self.assertEqual(
                        world.request_ids_with_status("wanted"),
                        [request_id],
                    )

                    self.assertEqual(
                        world.enrich_current_evidence(request_id),
                        "enriched",
                    )
                    self.assertTrue(action(
                        world,
                        request_id,
                        codec="flac",
                    ))
                    world.assert_invariants()

    def test_every_live_filesystem_drift_shape_relinks_exact_snapshot(self) -> None:
        facts = next(
            seed
            for seed in EVIDENCE_DRIFT_FACT_SEEDS
            if seed.name == "source_facts_carried_v0"
        )
        assert TEST_DSN is not None
        for index, mutation in enumerate(EVIDENCE_DRIFT_MUTATION_SEEDS, start=1):
            rename_variants = (
                (False, True)
                if mutation.mutation == "codec_replacement"
                else (False,)
            )
            for rename_codec_files in rename_variants:
                with self.subTest(
                    mutation=mutation.name,
                    rename_codec_files=rename_codec_files,
                ):
                    with LifecycleWorld(TEST_DSN, repository_root()) as world:
                        request_id = world.seed_evidence_drift_release(
                            BeetsWorldRelease(
                                release_id=(
                                    "60000000-0000-4000-8000-"
                                    f"{index:012x}"
                                ),
                                artist="Drift Archive",
                                album=f"Mutation {index}",
                                year=2006,
                                codec=mutation.initial_codec,
                            ),
                            facts,
                        )
                        old_id = world.db.get_request_current_evidence_id(
                            request_id
                        )
                        world.inject_evidence_drift(
                            request_id,
                            mutation.mutation,
                            rename_codec_files=rename_codec_files,
                        )

                        result = world.touch_current_evidence(request_id)

                        self.assertTrue(result.available)
                        new_id = world.db.get_request_current_evidence_id(
                            request_id
                        )
                        self.assertNotEqual(new_id, old_id)
                        linked = world.db.load_album_quality_evidence_by_id(
                            new_id
                        )
                        assert linked is not None
                        self.assertTrue(linked.current_enrichment_required)
                        self.assertEqual(linked.lineage_version, 4)
                        world.assert_invariants()

    def test_every_live_evidence_fact_shape_has_stable_retry_outcome(self) -> None:
        assert TEST_DSN is not None
        for index, facts in enumerate(EVIDENCE_DRIFT_FACT_SEEDS, start=1):
            with self.subTest(facts=facts.name):
                with LifecycleWorld(TEST_DSN, repository_root()) as world:
                    request_id = world.seed_evidence_drift_release(
                        BeetsWorldRelease(
                            release_id=(
                                "61000000-0000-4000-8000-"
                                f"{index:012x}"
                            ),
                            artist="Drift Archive",
                            album=f"Fact Shape {index}",
                            year=2006,
                            codec="mp3",
                        ),
                        facts,
                    )
                    world.inject_evidence_drift(
                        request_id,
                        "same_name_size_drift",
                    )

                    first = world.touch_current_evidence(request_id)
                    second = world.touch_current_evidence(request_id)

                    source_facts_survive = (
                        facts.spectral_subject == "source"
                        and facts.v0_subject == "source"
                    )
                    self.assertEqual(first.available, source_facts_survive)
                    self.assertEqual(second.available, source_facts_survive)
                    world.assert_invariants()

    def test_retained_import_preserves_existing_lossless_scope(self) -> None:
        seed = next(
            seed
            for seed in STATEFUL_WORLD_CENSUS_SEEDS
            if seed.name == "wanted_mb_lossless_lineage1_installed"
        )
        assert TEST_DSN is not None
        with LifecycleWorld(TEST_DSN, repository_root()) as world:
            request_id = world.seed_census_release(BeetsWorldRelease(
                release_id="62000000-0000-4000-8000-000000000743",
                artist="Scope Archive",
                album="Lossless Search",
                year=2006,
                codec="mp3",
            ), seed)

            self.assertTrue(world.force_import_request(
                request_id,
                codec="flac",
                verified_lossless=False,
            ))

            row = world.db.get_request(request_id)
            assert row is not None
            self.assertEqual(row["status"], "wanted")
            self.assertEqual(row["search_filetype_override"], "lossless")
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

    @initialize(seed=st.sampled_from(STATEFUL_WORLD_CENSUS_SEEDS))
    def initialize_from_production_census(self, seed: WorldCensusSeed) -> None:
        self._release_counter += 1
        self.world.seed_census_release(BeetsWorldRelease(
            release_id=_mb_release_id(self._release_counter),
            artist="Census Artist",
            album=f"Census Shape {seed.name}",
            year=2000,
            codec="flac" if seed.verified_lossless else "mp3",
            label="Census Label",
            catalognum=f"CENSUS-{self._release_counter}",
        ), seed)

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

    @precondition(lambda self: any(
        self.world.request_ids_for_evidence_drift(seed)
        for seed in EVIDENCE_DRIFT_MUTATION_SEEDS
    ))
    @rule(
        data=st.data(),
        mutation=st.sampled_from(EVIDENCE_DRIFT_MUTATION_SEEDS),
        rename_codec_files=st.booleans(),
    )
    def drift_then_touch_current_evidence(
        self,
        data: st.DataObject,
        mutation: EvidenceDriftMutationSeed,
        rename_codec_files: bool,
    ) -> None:
        candidates = self.world.request_ids_for_evidence_drift(mutation)
        if not candidates:
            return
        request_id = data.draw(st.sampled_from(candidates))
        self.world.inject_evidence_drift(
            request_id,
            mutation.mutation,
            rename_codec_files=(
                rename_codec_files
                and mutation.mutation == "codec_replacement"
            ),
        )
        first = self.world.touch_current_evidence(request_id)
        second = self.world.touch_current_evidence(request_id)
        linked_id = self.world.db.get_request_current_evidence_id(request_id)
        linked = self.world.db.load_album_quality_evidence_by_id(linked_id)
        if linked is None or not linked.current_enrichment_required:
            raise AssertionError(
                "changed snapshot lost its durable enrichment gate: "
                f"{mutation.name}"
            )
        measurement = linked.measurement
        missing_spectral = (
            measurement.spectral_grade is None
            and measurement.spectral_bitrate_kbps is None
        )
        missing_v0 = (
            linked.v0_metric is None
            and not linked.on_disk_v0_research_attempted
        )
        if (missing_spectral or missing_v0) and (
            first.available or second.available
        ):
            raise AssertionError(
                "changed snapshot bypassed its enrichment gate: "
                f"{mutation.name}"
            )

    @invariant()
    def cross_engine_invariants_hold(self) -> None:
        self.world.assert_invariants()


TestGeneratedLifecycleWorld = LifecycleWorldMachine.TestCase
_RANDOMIZED = os.environ.get("CRATEDIGGER_WORLD_RANDOMIZED") == "1"
_DATABASE = (
    DirectoryBasedExampleDatabase(
        os.environ.get(
            "CRATEDIGGER_WORLD_DATABASE",
            ".hypothesis/world-model",
        )
    )
    if _RANDOMIZED
    else None
)
TestGeneratedLifecycleWorld.settings = settings(
    max_examples=int(os.environ.get("CRATEDIGGER_WORLD_EXAMPLES", "6")),
    stateful_step_count=int(os.environ.get("CRATEDIGGER_WORLD_STEPS", "8")),
    deadline=None,
    derandomize=not _RANDOMIZED,
    database=_DATABASE,
    print_blob=_RANDOMIZED,
    suppress_health_check=(HealthCheck.too_slow,),
)


class TestGeneratedEvidenceDriftWorld(unittest.TestCase):
    """Cross the live mutation and evidence-fact vocabularies in real stores."""

    @given(
        mutation=st.sampled_from(EVIDENCE_DRIFT_MUTATION_SEEDS),
        facts=st.sampled_from(EVIDENCE_DRIFT_FACT_SEEDS),
        retries=st.integers(min_value=1, max_value=3),
        rename_codec_files=st.booleans(),
    )
    @example(
        mutation=EVIDENCE_DRIFT_MUTATION_SEEDS[2],
        facts=EVIDENCE_DRIFT_FACT_SEEDS[2],
        retries=2,
        rename_codec_files=False,
    )
    def test_live_drift_worlds_relink_without_retry_bypass(
        self,
        mutation: EvidenceDriftMutationSeed,
        facts: EvidenceDriftFactSeed,
        retries: int,
        rename_codec_files: bool,
    ) -> None:
        assert TEST_DSN is not None
        with LifecycleWorld(TEST_DSN, repository_root()) as world:
            request_id = world.seed_evidence_drift_release(BeetsWorldRelease(
                release_id="70000000-0000-4000-8000-000000000743",
                artist="Generated Drift",
                album="Generated Evidence",
                year=2007,
                codec=mutation.initial_codec,
            ), facts)
            old_id = world.db.get_request_current_evidence_id(request_id)
            world.inject_evidence_drift(
                request_id,
                mutation.mutation,
                rename_codec_files=(
                    rename_codec_files
                    and mutation.mutation == "codec_replacement"
                ),
            )

            results = [
                world.touch_current_evidence(request_id)
                for _ in range(retries)
            ]

            source_facts_survive = (
                facts.spectral_subject == "source"
                and facts.v0_subject == "source"
            )
            self.assertEqual(
                [result.available for result in results],
                [source_facts_survive] * retries,
            )
            new_id = world.db.get_request_current_evidence_id(request_id)
            self.assertNotEqual(new_id, old_id)
            linked = world.db.load_album_quality_evidence_by_id(new_id)
            assert linked is not None
            self.assertTrue(linked.current_enrichment_required)
            self.assertEqual(linked.lineage_version, 4)
            world.assert_invariants()


TestGeneratedEvidenceDriftWorld.test_live_drift_worlds_relink_without_retry_bypass = (
    settings(
        max_examples=int(os.environ.get("CRATEDIGGER_WORLD_EXAMPLES", "6")),
        deadline=None,
        derandomize=not _RANDOMIZED,
        database=_DATABASE,
        print_blob=_RANDOMIZED,
        suppress_health_check=(HealthCheck.too_slow,),
    )(
        TestGeneratedEvidenceDriftWorld
        .test_live_drift_worlds_relink_without_retry_bypass
    )
)


if __name__ == "__main__":
    unittest.main()
