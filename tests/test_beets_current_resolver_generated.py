#!/usr/bin/env python3
"""Real-Beets current-library resolver contracts for exact release identity."""

from __future__ import annotations

import configparser
import os
import unittest
from dataclasses import dataclass
from pathlib import Path

from hypothesis import HealthCheck, example, given, settings, strategies as st

import tests._hypothesis_profiles  # noqa: F401
from lib.beets_db import (
    BeetsDB,
    CurrentBeetsAmbiguous,
    CurrentBeetsItem,
    CurrentBeetsMissing,
    CurrentBeetsResolution,
    CurrentBeetsUnique,
    open_beets_db,
)
from lib.config import CratediggerConfig
from lib.release_identity import ReleaseIdentity
from tests.beets_world import BeetsWorld, BeetsWorldRelease


REPO = Path(__file__).resolve().parent.parent
MB_TARGET = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
MB_SIBLING = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
DISCOGS_TARGET = "12856590"
DISCOGS_SIBLING = "12856591"


@dataclass(frozen=True)
class ResolverExpectation:
    identity: ReleaseIdentity
    exact_album_count: int
    expected_album_path: str | None = None
    topology_error: str | None = None


def assert_current_resolution(
    result: CurrentBeetsResolution,
    expected: ResolverExpectation,
) -> None:
    """Executable exact-identity/cardinality/topology resolver law."""

    if result.identity != expected.identity:
        raise AssertionError("resolver substituted another release identity")
    if expected.exact_album_count == 0:
        if not isinstance(result, CurrentBeetsMissing):
            raise AssertionError("zero exact matches did not resolve missing")
        return
    if expected.exact_album_count > 1 or expected.topology_error is not None:
        if not isinstance(result, CurrentBeetsAmbiguous):
            raise AssertionError("ambiguous exact membership was collapsed")
        if expected.topology_error is not None and result.reason != expected.topology_error:
            raise AssertionError(
                f"topology reason drifted: {result.reason!r} != "
                f"{expected.topology_error!r}"
            )
        return
    if not isinstance(result, CurrentBeetsUnique):
        raise AssertionError("one usable exact album did not resolve unique")
    if not os.path.isabs(result.album_path):
        raise AssertionError("unique album path is not absolute")
    if not result.items:
        raise AssertionError("unique resolution has no item topology")
    if any(not os.path.isabs(item.path) for item in result.items):
        raise AssertionError("unique item path is not absolute")
    if {os.path.dirname(item.path) for item in result.items} != {result.album_path}:
        raise AssertionError("unique resolution spans more than one directory")
    if expected.expected_album_path is not None:
        if result.album_path != expected.expected_album_path:
            raise AssertionError("resolver returned a stale or inferred album path")


def _identity(source: str) -> ReleaseIdentity:
    if source == "mb":
        identity = ReleaseIdentity.from_id(MB_TARGET)
    else:
        identity = ReleaseIdentity.from_id(DISCOGS_TARGET)
    assert identity is not None
    return identity


def _release(source: str, *, tracks: int, suffix: str = "") -> BeetsWorldRelease:
    release_id = MB_TARGET if source == "mb" else DISCOGS_TARGET
    return BeetsWorldRelease(
        release_id=release_id,
        artist=f"Archivist {suffix}".strip(),
        album=f"Exact pressing {suffix}".strip(),
        year=2001,
        track_count=tracks,
    )


def _sibling(source: str) -> BeetsWorldRelease:
    release_id = MB_SIBLING if source == "mb" else DISCOGS_SIBLING
    return BeetsWorldRelease(
        release_id=release_id,
        artist="Archivist",
        album="Exact pressing",
        year=2001,
        track_count=1,
    )


def _runtime_config(world: BeetsWorld) -> CratediggerConfig:
    ini = configparser.RawConfigParser()
    ini["Beets"] = {
        "directory": str(world.library_root),
        "library": str(world.library_db),
    }
    return CratediggerConfig.from_ini(ini)


class TestCurrentBeetsResolverPins(unittest.TestCase):
    def test_mb_unique_uses_moved_unicode_relative_current_paths(self) -> None:
        with BeetsWorld(REPO) as world:
            initial = world.import_release(_release("mb", tracks=12))
            moved = world.relocate_release_out_of_band(
                MB_TARGET,
                world.library_root / "Beyoncé" / "曖昧 — current",
                store_relative_paths=True,
            )
            self.assertNotEqual(initial.album_path, moved.album_path)
            with open_beets_db(_runtime_config(world)) as beets:
                result = beets.resolve_current_release(_identity("mb"))
            assert_current_resolution(result, ResolverExpectation(
                identity=_identity("mb"),
                exact_album_count=1,
                expected_album_path=moved.album_path,
            ))
            self.assertIsInstance(result, CurrentBeetsUnique)
            assert isinstance(result, CurrentBeetsUnique)
            self.assertEqual(len(result.items), 12)
            self.assertEqual(result.selectors, (f"mb_albumid:{MB_TARGET}",))

    def test_modern_and_legacy_discogs_share_exact_unique_semantics(self) -> None:
        for legacy in (False, True):
            with self.subTest(layout="legacy" if legacy else "modern"):
                with BeetsWorld(REPO) as world:
                    snapshot = world.import_release(_release("discogs", tracks=2))
                    world.set_discogs_identity_layout(
                        DISCOGS_TARGET, legacy=legacy,
                    )
                    world.set_release_paths_relative(DISCOGS_TARGET)
                    with BeetsDB(
                        str(world.library_db),
                        library_root=str(world.library_root),
                    ) as beets:
                        result = beets.resolve_current_release(_identity("discogs"))
                    assert_current_resolution(result, ResolverExpectation(
                        identity=_identity("discogs"),
                        exact_album_count=1,
                        expected_album_path=snapshot.album_path,
                    ))
                    assert isinstance(result, CurrentBeetsUnique)
                    self.assertEqual(result.selectors, (
                        f"discogs_albumid:{DISCOGS_TARGET}",
                        f"mb_albumid:{DISCOGS_TARGET}",
                    ))

    def test_each_discogs_layout_preserves_zero_and_two_match_cardinality(self) -> None:
        for legacy in (False, True):
            for cardinality in (0, 2):
                with self.subTest(legacy=legacy, cardinality=cardinality):
                    with BeetsWorld(REPO) as world:
                        world.import_release(_sibling("discogs"))
                        if cardinality:
                            world.import_release(_release(
                                "discogs", tracks=1, suffix="one",
                            ))
                            if legacy:
                                world.set_discogs_identity_layout(
                                    DISCOGS_TARGET, legacy=True,
                                )
                            world.import_duplicate_release(_release(
                                "discogs", tracks=2, suffix="two",
                            ))
                        with BeetsDB(
                            str(world.library_db),
                            library_root=str(world.library_root),
                        ) as beets:
                            result = beets.resolve_current_release(
                                _identity("discogs"),
                            )
                            batch = beets.get_album_ids_by_mbids([
                                DISCOGS_TARGET,
                            ])
                        assert_current_resolution(result, ResolverExpectation(
                            identity=_identity("discogs"),
                            exact_album_count=cardinality,
                        ))
                        self.assertEqual(batch, {})

    def test_duplicate_exact_identity_is_ambiguous_and_absent_from_batches(self) -> None:
        with BeetsWorld(REPO) as world:
            world.import_release(_release("mb", tracks=1, suffix="one"))
            world.import_duplicate_release(
                _release("mb", tracks=2, suffix="two"),
            )
            with BeetsDB(
                str(world.library_db), library_root=str(world.library_root),
            ) as beets:
                result = beets.resolve_current_release(_identity("mb"))
                self.assertEqual(beets.check_mbids([MB_TARGET]), set())
                self.assertEqual(beets.get_album_ids_by_mbids([MB_TARGET]), {})
                self.assertEqual(beets.check_mbids_detail([MB_TARGET]), {})
            assert_current_resolution(result, ResolverExpectation(
                identity=_identity("mb"), exact_album_count=2,
            ))
            assert isinstance(result, CurrentBeetsAmbiguous)
            self.assertEqual(result.reason, "multiple_matches")
            self.assertEqual(len(result.album_ids), 2)

    def test_empty_and_split_topologies_are_explicitly_unusable(self) -> None:
        for topology in ("empty", "split"):
            with self.subTest(topology=topology):
                with BeetsWorld(REPO) as world:
                    tracks = 1 if topology == "empty" else 2
                    world.import_release(_release("mb", tracks=tracks))
                    if topology == "empty":
                        world.empty_release_topology(MB_TARGET)
                        reason = "empty_topology"
                    else:
                        world.split_release_topology(MB_TARGET)
                        reason = "split_topology"
                    with BeetsDB(
                        str(world.library_db),
                        library_root=str(world.library_root),
                    ) as beets:
                        result = beets.resolve_current_release(_identity("mb"))
                    assert_current_resolution(result, ResolverExpectation(
                        identity=_identity("mb"),
                        exact_album_count=1,
                        topology_error=reason,
                    ))

    def test_conflicting_discogs_columns_and_invalid_paths_are_ambiguous(self) -> None:
        cases = (
            ("conflicting_identity", "conflict"),
            ("invalid_path", "invalid_path"),
        )
        for reason, mutation in cases:
            with self.subTest(reason=reason):
                with BeetsWorld(REPO) as world:
                    world.import_release(_release("discogs", tracks=1))
                    if mutation == "conflict":
                        world.set_conflicting_discogs_identities(
                            DISCOGS_TARGET,
                            conflicting_release_id=DISCOGS_SIBLING,
                        )
                    else:
                        world.set_release_item_path(DISCOGS_TARGET, b"")
                    with BeetsDB(
                        str(world.library_db),
                        library_root=str(world.library_root),
                    ) as beets:
                        result = beets.resolve_current_release(
                            _identity("discogs"),
                        )
                assert_current_resolution(result, ResolverExpectation(
                    identity=_identity("discogs"),
                    exact_album_count=1,
                    topology_error=reason,
                ))

    def test_missing_target_never_falls_back_to_same_metadata_sibling(self) -> None:
        with BeetsWorld(REPO) as world:
            world.import_release(_sibling("mb"))
            with BeetsDB(
                str(world.library_db), library_root=str(world.library_root),
            ) as beets:
                result = beets.resolve_current_release(_identity("mb"))
            assert_current_resolution(result, ResolverExpectation(
                identity=_identity("mb"), exact_album_count=0,
            ))


class TestCurrentBeetsResolverGenerated(unittest.TestCase):
    @settings(
        max_examples=(
            72
            if os.environ.get("CRATEDIGGER_HYPOTHESIS_PROFILE") == "fuzz"
            else 18
        ),
        deadline=None,
        suppress_health_check=[HealthCheck.too_slow],
    )
    @example(
        source="discogs_legacy", cardinality=1, track_count=12,
        relative_paths=True, unicode_move=True, topology="usable",
    )
    @example(
        source="mb", cardinality=2, track_count=1,
        relative_paths=False, unicode_move=False, topology="usable",
    )
    @example(
        source="discogs_modern", cardinality=1, track_count=2,
        relative_paths=True, unicode_move=False, topology="split",
    )
    @example(
        source="discogs_conflict", cardinality=1, track_count=12,
        relative_paths=False, unicode_move=False, topology="usable",
    )
    @given(
        source=st.sampled_from((
            "mb", "discogs_modern", "discogs_legacy", "discogs_conflict",
        )),
        cardinality=st.integers(min_value=0, max_value=2),
        track_count=st.sampled_from((1, 2, 12)),
        relative_paths=st.booleans(),
        unicode_move=st.booleans(),
        topology=st.sampled_from(("usable", "empty", "split", "invalid")),
    )
    def test_real_beets_identity_cardinality_and_path_worlds(
        self,
        source: str,
        cardinality: int,
        track_count: int,
        relative_paths: bool,
        unicode_move: bool,
        topology: str,
    ) -> None:
        identity_source = "mb" if source == "mb" else "discogs"
        release_id = MB_TARGET if identity_source == "mb" else DISCOGS_TARGET
        with BeetsWorld(REPO) as world:
            world.import_release(_sibling(identity_source))
            snapshots = []
            if cardinality:
                snapshots.append(world.import_release(
                    _release(identity_source, tracks=track_count, suffix="one"),
                ))
                if source == "discogs_legacy":
                    world.set_discogs_identity_layout(release_id, legacy=True)
                if unicode_move:
                    snapshots[-1] = world.relocate_release_out_of_band(
                        release_id,
                        world.library_root / "ユニコード" / "moved album",
                        store_relative_paths=relative_paths,
                    )
                elif relative_paths:
                    world.set_release_paths_relative(release_id)
                if cardinality == 2:
                    snapshots.append(world.import_duplicate_release(
                        _release(identity_source, tracks=track_count, suffix="two"),
                    ))
                elif topology == "empty":
                    world.empty_release_topology(release_id)
                elif topology == "split" and track_count >= 2:
                    world.split_release_topology(release_id)
                elif topology == "invalid":
                    world.set_release_item_path(release_id, b"")
                if source == "discogs_conflict":
                    world.set_conflicting_discogs_identities(
                        release_id,
                        conflicting_release_id=DISCOGS_SIBLING,
                    )

            cfg = _runtime_config(world)
            with open_beets_db(cfg) as beets:
                result = beets.resolve_current_release(_identity(identity_source))
                batch_present = release_id in beets.check_mbids([release_id])
                batch_ids = beets.get_album_ids_by_mbids([release_id])

            topology_error = None
            if cardinality >= 1 and source == "discogs_conflict":
                topology_error = "conflicting_identity"
            elif cardinality == 1 and topology == "empty":
                topology_error = "empty_topology"
            elif cardinality == 1 and topology == "split" and track_count >= 2:
                topology_error = "split_topology"
            elif cardinality == 1 and topology == "invalid":
                topology_error = "invalid_path"
            expected_path = None
            if cardinality == 1 and topology_error is None:
                expected_path = snapshots[0].album_path
            assert_current_resolution(result, ResolverExpectation(
                identity=_identity(identity_source),
                exact_album_count=cardinality,
                expected_album_path=expected_path,
                topology_error=topology_error,
            ))
            should_be_unique = cardinality == 1 and topology_error is None
            self.assertEqual(batch_present, should_be_unique)
            self.assertEqual(release_id in batch_ids, should_be_unique)

    def test_checker_kills_limit_one_one_column_and_fuzzy_mutants(self) -> None:
        mb = _identity("mb")
        discogs = _identity("discogs")
        fake_item = CurrentBeetsItem(id=1, path="/library/a.flac")
        limit_one = CurrentBeetsUnique(
            identity=mb,
            album_id=1,
            album_path="/library",
            items=(fake_item,),
            selectors=(f"mb_albumid:{MB_TARGET}",),
        )
        with self.assertRaisesRegex(AssertionError, "collapsed"):
            assert_current_resolution(limit_one, ResolverExpectation(
                identity=mb, exact_album_count=2,
            ))
        one_column = CurrentBeetsMissing(identity=discogs)
        with self.assertRaisesRegex(AssertionError, "did not resolve unique"):
            assert_current_resolution(one_column, ResolverExpectation(
                identity=discogs, exact_album_count=1,
            ))
        fuzzy = CurrentBeetsUnique(
            identity=ReleaseIdentity(source="musicbrainz", release_id=MB_SIBLING),
            album_id=2,
            album_path="/library/sibling",
            items=(fake_item,),
            selectors=(f"mb_albumid:{MB_SIBLING}",),
        )
        with self.assertRaisesRegex(AssertionError, "substituted"):
            assert_current_resolution(fuzzy, ResolverExpectation(
                identity=mb, exact_album_count=0,
            ))


if __name__ == "__main__":
    unittest.main()
