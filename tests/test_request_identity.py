"""Deterministic pins for the request↔album union resolution (#1059).

Invariants under test:

I3  The join resolves the union {canonical, acquisition}, canonical first.
    Both sides resolving to DIFFERENT albums is ambiguous and fails closed —
    never a silent pick.
I1  The acquisition id is never mutated, and every non-unique resolution
    names the identity the caller asked for, never the survivor.

The four live mbsync states are pinned by name, because which side Beets
holds is the whole reason the derivation is a union: of the six merged
requests measured on 2026-08-06, two were installed under the survivor and
four still under the loser.

The composition test at the bottom drives the REAL ``BeetsDB`` over a real
temp Beets library and then feeds the result to a REAL consumer, per
``.claude/rules/code-quality.md`` § "Invariants live at the widest boundary".
A union that only agrees with itself is not evidence.
"""

from __future__ import annotations

import unittest
from collections.abc import Mapping
from pathlib import Path

from lib.beets_db import (
    CurrentBeetsAmbiguous,
    CurrentBeetsMissing,
    CurrentBeetsResolution,
    CurrentBeetsUnique,
    open_beets_db,
)
from lib.current_library_display import (
    CurrentLibraryAmbiguousDisplay,
    CurrentLibraryMissingDisplay,
    CurrentLibraryUniqueDisplay,
    current_library_display,
    resolve_request_current_library,
)
from lib.release_identity import ReleaseIdentity
from lib.request_identity import (
    acceptable_identities,
    acquisition_identity,
    canonical_identity,
    merge_union_resolutions,
    resolve_current_for_request,
    resolve_current_for_requests,
)
from tests.beets_world import BeetsWorld, BeetsWorldRelease

REPO = Path(__file__).resolve().parent.parent

# Request 316's live merge, probed against the mirror on 2026-08-06.
LOSER = "4878ee47-f8b8-45c8-832c-62de3bccfa6e"
SURVIVOR = "7aabf975-9a06-4b2e-854c-2c700380ebd5"
UNRELATED = "bce7d8c3-815b-449c-8e18-df806398986c"


def _identity(release_id: str) -> ReleaseIdentity:
    identity = ReleaseIdentity.from_id(release_id)
    assert identity is not None
    return identity


def _row(
    *,
    request_id: int = 316,
    mb_release_id: str | None = LOSER,
    canonical: str | None = None,
    discogs_release_id: str | None = None,
) -> dict[str, object]:
    return {
        "id": request_id,
        "mb_release_id": mb_release_id,
        "discogs_release_id": discogs_release_id,
        "canonical_release_id": canonical,
    }


def _unique(identity: ReleaseIdentity, album_id: int) -> CurrentBeetsUnique:
    return CurrentBeetsUnique(
        identity=identity,
        album_id=album_id,
        album_path=f"/library/album-{album_id}",
        items=(),
        selectors=(f"mb_albumid:{identity.release_id}",),
    )


class _StubResolver:
    """A batch resolver returning canned per-identity resolutions.

    Not a mock of our own logic: it stands in for the SQLite read edge so
    the fold can be pinned over states a real library reaches only after an
    ``mbsync`` run. The real resolver drives the composition test below.
    """

    def __init__(
        self, answers: Mapping[ReleaseIdentity, CurrentBeetsResolution],
        *,
        omit: set[ReleaseIdentity] | None = None,
    ) -> None:
        self.answers = answers
        self.omit = omit or set()
        self.batches: list[list[ReleaseIdentity]] = []

    def resolve_current_releases(
        self, identities: list[ReleaseIdentity],
    ) -> dict[ReleaseIdentity, CurrentBeetsResolution]:
        self.batches.append(list(identities))
        return {
            identity: self.answers.get(
                identity, CurrentBeetsMissing(identity=identity),
            )
            for identity in identities
            if identity not in self.omit
        }


class TestAcceptableIdentities(unittest.TestCase):
    """The single definition every seam reads."""

    def test_no_canonical_is_just_the_acquisition_id(self) -> None:
        self.assertEqual(
            acceptable_identities(_row()), (_identity(LOSER),))

    def test_canonical_is_preferred_and_acquisition_is_last(self) -> None:
        self.assertEqual(
            acceptable_identities(_row(canonical=SURVIVOR)),
            (_identity(SURVIVOR), _identity(LOSER)),
        )

    def test_a_row_without_an_exact_identity_is_empty(self) -> None:
        self.assertEqual(acceptable_identities(_row(mb_release_id=None)), ())
        self.assertEqual(
            acceptable_identities(_row(mb_release_id="not-a-uuid")), ())

    def test_conflicting_identity_fields_fail_closed(self) -> None:
        """A row naming two different pressings resolves to neither."""
        row = _row(mb_release_id=LOSER, discogs_release_id="12856590")
        self.assertEqual(acceptable_identities(row), ())

    def test_discogs_row_never_gains_a_canonical(self) -> None:
        """Discogs has no merge concept; the reconciler never writes one."""
        row = _row(mb_release_id=None, discogs_release_id="12856590")
        self.assertEqual(
            acceptable_identities(row), (_identity("12856590"),))

    def test_component_accessors_agree_with_the_tuple(self) -> None:
        row = _row(canonical=SURVIVOR)
        self.assertEqual(acquisition_identity(row), _identity(LOSER))
        self.assertEqual(canonical_identity(row), _identity(SURVIVOR))


class TestUnionFold(unittest.TestCase):
    """I3 — the four live mbsync states, plus the two failure shapes."""

    def setUp(self) -> None:
        self.acquisition = _identity(LOSER)
        self.canonical = _identity(SURVIVOR)

    def test_installed_under_the_loser_resolves(self) -> None:
        """Requests 1838 / 8815 / 346 / 8712 — mbsync has not run yet."""
        held = _unique(self.acquisition, 11541)
        result = merge_union_resolutions(
            self.acquisition,
            [CurrentBeetsMissing(identity=self.canonical), held],
        )
        self.assertEqual(result, held)

    def test_installed_under_the_survivor_reports_the_acquisition_id(
        self,
    ) -> None:
        """Requests 316 / 8832 — mbsync retagged, and today the join misses.

        The album resolves, but the resolution names the id the request
        asked for. A dozen consumers compare that against the stored id and
        fail the operation on a mismatch; reporting the survivor is how the
        aborted attempt broke ``quality_evidence`` in round 1.
        """
        held = _unique(self.canonical, 19345)
        result = merge_union_resolutions(
            self.acquisition,
            [held, CurrentBeetsMissing(identity=self.acquisition)],
        )
        self.assertIsInstance(result, CurrentBeetsUnique)
        assert isinstance(result, CurrentBeetsUnique)
        self.assertEqual(result.album_id, 19345)
        self.assertEqual(result.identity, self.acquisition)
        # Selectors keep naming where the album is really filed, or a
        # destructive action would target an id Beets no longer stores.
        self.assertEqual(result.selectors, (f"mb_albumid:{SURVIVOR}",))

    def test_both_sides_naming_one_album_resolves_once(self) -> None:
        held = _unique(self.canonical, 19345)
        result = merge_union_resolutions(
            self.acquisition,
            [held, _unique(self.acquisition, 19345)],
        )
        self.assertIsInstance(result, CurrentBeetsUnique)
        assert isinstance(result, CurrentBeetsUnique)
        self.assertEqual(result.album_id, 19345)

    def test_neither_side_held_is_missing_under_the_acquisition_id(
        self,
    ) -> None:
        result = merge_union_resolutions(
            self.acquisition,
            [
                CurrentBeetsMissing(identity=self.canonical),
                CurrentBeetsMissing(identity=self.acquisition),
            ],
        )
        self.assertIsInstance(result, CurrentBeetsMissing)
        # I1: a miss names what the caller asked for, never the survivor.
        # Reporting the canonical here would leak a substituted identity out
        # of a failed lookup, and downstream calls that a substitution.
        self.assertEqual(result.identity, self.acquisition)

    def test_two_different_albums_is_the_double_sided_merge(self) -> None:
        """Fails closed. Zero live instances; the stamp is its future exit."""
        result = merge_union_resolutions(
            self.acquisition,
            [_unique(self.canonical, 18672), _unique(self.acquisition, 6612)],
        )
        self.assertIsInstance(result, CurrentBeetsAmbiguous)
        assert isinstance(result, CurrentBeetsAmbiguous)
        self.assertEqual(result.reason, "merged_identity_split")
        self.assertEqual(result.album_ids, (6612, 18672))
        self.assertEqual(result.identity, self.acquisition)

    def test_an_ambiguous_side_propagates_with_its_reason(self) -> None:
        ambiguous = CurrentBeetsAmbiguous(
            identity=self.canonical,
            album_ids=(1, 2),
            reason="multiple_matches",
        )
        result = merge_union_resolutions(
            self.acquisition,
            [ambiguous, _unique(self.acquisition, 9)],
        )
        self.assertIsInstance(result, CurrentBeetsAmbiguous)
        assert isinstance(result, CurrentBeetsAmbiguous)
        self.assertEqual(result.reason, "multiple_matches")
        self.assertEqual(result.album_ids, (1, 2))
        self.assertEqual(result.identity, self.acquisition)


class TestBatching(unittest.TestCase):
    def test_the_whole_cohort_costs_one_round_trip(self) -> None:
        resolver = _StubResolver({
            _identity(SURVIVOR): _unique(_identity(SURVIVOR), 19345),
        })
        rows = [
            _row(request_id=316, canonical=SURVIVOR),
            _row(request_id=8832, mb_release_id=UNRELATED),
            _row(request_id=999, mb_release_id=None),
        ]

        resolved = resolve_current_for_requests(resolver, rows)

        self.assertEqual(len(resolver.batches), 1)
        # De-duplicated, and the identity-less row never entered the batch.
        self.assertEqual(
            resolver.batches[0],
            [_identity(SURVIVOR), _identity(LOSER), _identity(UNRELATED)],
        )
        self.assertEqual(sorted(resolved), [316, 8832])
        self.assertIsInstance(resolved[316], CurrentBeetsUnique)
        self.assertIsInstance(resolved[8832], CurrentBeetsMissing)

    def test_an_omitted_identity_is_an_authority_failure_not_absence(
        self,
    ) -> None:
        """A resolver that drops a requested identity must not be read as
        'the pressing is missing' — that is a silent downgrade of an
        authority failure into a claim about the library."""
        resolver = _StubResolver({}, omit={_identity(SURVIVOR)})
        rows = [_row(canonical=SURVIVOR)]

        self.assertEqual(resolve_current_for_requests(resolver, rows), {})
        self.assertIsNone(resolve_current_for_request(resolver, rows[0]))

    def test_single_request_helper_matches_the_batch(self) -> None:
        answers = {_identity(SURVIVOR): _unique(_identity(SURVIVOR), 19345)}
        row = _row(canonical=SURVIVOR)
        one = resolve_current_for_request(_StubResolver(answers), row)
        many = resolve_current_for_requests(_StubResolver(answers), [row])
        self.assertEqual(one, many[316])


class TestUnionThroughRealBeetsAndARealConsumer(unittest.TestCase):
    """Composition: real writer, real resolver, real consumer, one resource.

    ``resolve_request_current_library`` is the production entry point behind
    the library tab and Recents "currently have". Driving it end to end is
    what the first attempt never did — its pin and its property both stopped
    at the resolver, and four consumers broke in production review instead.
    """

    def _display(self, world: BeetsWorld, row: dict[str, object]) -> object:
        with open_beets_db(
            db_path=str(world.library_db),
            library_root=str(world.library_root),
        ) as beets:
            return current_library_display(
                resolve_request_current_library(row, beets),
            )

    # The display union is msgspec-tagged, so ``state`` is a wire field, not
    # a Python attribute — assert the real types.

    def test_survivor_installed_resolves_for_the_real_consumer(self) -> None:
        """The live 316 shape: Beets holds the survivor, the request stores
        the loser. Today this renders as 'missing'; with the canonical
        stored, the consumer sees the album that is actually on disk."""
        with BeetsWorld(REPO) as world:
            album = world.import_release(BeetsWorldRelease(
                release_id=SURVIVOR,
                artist="Merged Artist",
                album="Merged Album",
                year=1996,
            ))

            without = self._display(world, _row())
            with_canonical = self._display(world, _row(canonical=SURVIVOR))

        self.assertIsInstance(without, CurrentLibraryMissingDisplay)
        self.assertIsInstance(with_canonical, CurrentLibraryUniqueDisplay)
        assert isinstance(with_canonical, CurrentLibraryUniqueDisplay)
        self.assertEqual(with_canonical.album_id, album.album_id)
        # I1: the consumer still names the id the request asked for.
        self.assertEqual(with_canonical.release_id, LOSER)

    def test_loser_installed_still_resolves_with_a_canonical_stored(
        self,
    ) -> None:
        """The live 346 shape. A canonical-only join would break this row —
        which is why the derivation is a union and not a replacement."""
        with BeetsWorld(REPO) as world:
            album = world.import_release(BeetsWorldRelease(
                release_id=LOSER,
                artist="Merged Artist",
                album="Merged Album",
                year=1996,
            ))

            display = self._display(world, _row(canonical=SURVIVOR))

        self.assertIsInstance(display, CurrentLibraryUniqueDisplay)
        assert isinstance(display, CurrentLibraryUniqueDisplay)
        self.assertEqual(display.album_id, album.album_id)

    def test_both_sides_installed_fails_closed_for_the_real_consumer(
        self,
    ) -> None:
        """The double-sided merge, composed rather than asserted: two real
        albums, one on each identity, and the consumer must refuse to pick."""
        with BeetsWorld(REPO) as world:
            world.import_release(BeetsWorldRelease(
                release_id=LOSER, artist="Merged Artist",
                album="Merged Album", year=1996,
            ))
            world.import_release(BeetsWorldRelease(
                release_id=SURVIVOR, artist="Merged Artist",
                album="Merged Album Deluxe", year=1996,
            ))

            display = self._display(world, _row(canonical=SURVIVOR))

        self.assertIsInstance(display, CurrentLibraryAmbiguousDisplay)
        assert isinstance(display, CurrentLibraryAmbiguousDisplay)
        self.assertEqual(display.reason, "merged_identity_split")


if __name__ == "__main__":
    unittest.main()
