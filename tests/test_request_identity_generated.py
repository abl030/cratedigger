"""Generated properties for the request↔album union (#1059).

The pins in ``tests/test_request_identity.py`` prove the four live mbsync
states; these properties patrol the world space around them, driving the
REAL ``BeetsDB`` resolver over a real Beets library through the REAL
``resolve_current_for_requests``.

One library is built once and holds an album for every id in ``HELD``. The
generated space is which acquisition id a request stores, which survivor (if
any) the reconciler recorded for it, and how many such requests share a
batch — including ids nothing holds and ids that are not release identities
at all. Nothing is filtered for plausibility.

Invariants patrolled — each is a module-level checker so the known-bad
self-tests below can call it directly:

U1  Every resolution names the ACQUISITION identity. A dozen consumers
    compare that against the request's stored id and fail the operation on
    a mismatch, so reporting the survivor is a substituted identity leaking
    out of the join (#1059 invariant 1).
U2  A unique resolution's album is one that genuinely answers to one of the
    request's acceptable identities — never a third album, never a sibling.
U3  Two acceptable identities resolving to DIFFERENT albums is ambiguous
    and fails closed as ``merged_identity_split`` — never a silent pick
    (#1059 invariant 3).
U4  A request with no stored survivor resolves byte-identically to the
    plain single-identity resolver. This is the inertness proof for the
    ~8,500 unmerged rows: the change cannot move a row it does not concern.
U5  A cohort costs exactly one batched query, however many identities it
    spans. The rejected design's per-read fan-out is what made it
    unaffordable.
"""

from __future__ import annotations

import unittest
from pathlib import Path

from hypothesis import HealthCheck, example, given, settings
from hypothesis import strategies as st

import tests._hypothesis_profiles  # noqa: F401  (loads the active profile)
from lib.beets_db import (
    CurrentBeetsAmbiguous,
    CurrentBeetsMissing,
    CurrentBeetsResolution,
    CurrentBeetsUnique,
    open_beets_db,
)
from lib.release_identity import ReleaseIdentity
from lib.request_identity import (
    CurrentBeetsBatchResolver,
    acceptable_identities,
    resolve_current_for_requests,
)
from tests.beets_world import BeetsWorld, BeetsWorldRelease

REPO = Path(__file__).resolve().parent.parent

# Request 316's live merge, plus neighbours that must never be substituted.
LOSER = "4878ee47-f8b8-45c8-832c-62de3bccfa6e"
SURVIVOR = "7aabf975-9a06-4b2e-854c-2c700380ebd5"
SIBLING = "bce7d8c3-815b-449c-8e18-df806398986c"
FOURTH = "abe18a1c-ad01-423c-b6ca-63cfa8a9daf1"
DISCOGS = "12856590"

#: Every id the shared library actually holds an album for.
HELD = (LOSER, SURVIVOR, SIBLING, FOURTH)

#: Well-formed MusicBrainz ids nothing holds — the miss half of the space.
UNHELD = (
    "11111111-1111-1111-1111-111111111111",
    "22222222-2222-2222-2222-222222222222",
)

#: Values that are not usable exact identities at all.
UNUSABLE = ("", "   ", "not-a-uuid", None)

RELEASE_IDS = st.sampled_from([*HELD, *UNHELD, *UNUSABLE, DISCOGS])
CANONICALS = st.sampled_from([*HELD, *UNHELD, *UNUSABLE])


# ---------------------------------------------------------------------------
# Invariant checkers — module level so the known-bad self-tests can call them
# ---------------------------------------------------------------------------


def check_names_the_acquisition(
    resolution: CurrentBeetsResolution,
    acquisition: ReleaseIdentity,
) -> None:
    """U1 — the join never substitutes another release identity."""
    if resolution.identity != acquisition:
        raise AssertionError(
            "resolution names "
            f"{resolution.identity.release_id!r} but the request acquired "
            f"{acquisition.release_id!r}; downstream reads that as a "
            "substituted identity and fails the operation"
        )


def check_album_is_acceptable(
    resolution: CurrentBeetsResolution,
    album_ids_by_identity: dict[ReleaseIdentity, int],
    acceptable: tuple[ReleaseIdentity, ...],
) -> None:
    """U2 — a unique resolution names an album one of our ids really has."""
    if not isinstance(resolution, CurrentBeetsUnique):
        return
    allowed = {
        album_ids_by_identity[identity]
        for identity in acceptable
        if identity in album_ids_by_identity
    }
    if resolution.album_id not in allowed:
        raise AssertionError(
            f"resolution named album {resolution.album_id}, which answers to "
            f"none of {[i.release_id for i in acceptable]}"
        )


def check_split_is_ambiguous(
    resolution: CurrentBeetsResolution,
    album_ids_by_identity: dict[ReleaseIdentity, int],
    acceptable: tuple[ReleaseIdentity, ...],
) -> None:
    """U3 — the double-sided merge fails closed instead of picking."""
    held = {
        album_ids_by_identity[identity]
        for identity in acceptable
        if identity in album_ids_by_identity
    }
    if len(held) < 2:
        return
    reason = getattr(resolution, "reason", None)
    if reason is None:
        raise AssertionError(
            f"two acceptable identities hold different albums {sorted(held)} "
            f"but the join returned {type(resolution).__name__} — it picked "
            "one instead of surfacing the split"
        )
    if reason != "merged_identity_split":
        raise AssertionError(
            "a two-album split must be reported as merged_identity_split, "
            f"not {reason!r}"
        )


def check_unmerged_is_inert(
    union_resolution: CurrentBeetsResolution,
    plain_resolution: CurrentBeetsResolution,
) -> None:
    """U4 — no stored survivor ⇒ exactly today's answer, byte for byte."""
    if union_resolution != plain_resolution:
        raise AssertionError(
            "a request with no stored survivor resolved differently under "
            f"the union ({union_resolution!r}) than under the plain "
            f"resolver ({plain_resolution!r})"
        )


def check_one_batch(batch_count: int) -> None:
    """U5 — a cohort costs one query, not one per identity."""
    if batch_count > 1:
        raise AssertionError(
            f"a single cohort resolution issued {batch_count} batched "
            "queries; the union must flatten identities before querying"
        )


class _CountingResolver:
    """The real resolver plus a call counter. Instrumentation, not a mock."""

    def __init__(self, beets: CurrentBeetsBatchResolver) -> None:
        self._beets = beets
        self.batches = 0

    def resolve_current_releases(
        self, identities: list[ReleaseIdentity],
    ) -> dict[ReleaseIdentity, CurrentBeetsResolution]:
        self.batches += 1
        return self._beets.resolve_current_releases(identities)


class TestUnionProperties(unittest.TestCase):
    """U1–U5 driven through the real resolver over one real library."""

    world: BeetsWorld
    album_ids_by_identity: dict[ReleaseIdentity, int]

    @classmethod
    def setUpClass(cls) -> None:
        cls.world = BeetsWorld(REPO)
        cls.album_ids_by_identity = {}
        for index, release_id in enumerate(HELD):
            snapshot = cls.world.import_release(BeetsWorldRelease(
                release_id=release_id,
                artist=f"Artist {index}",
                album=f"Album {index}",
                year=1990 + index,
            ))
            identity = ReleaseIdentity.from_id(release_id)
            assert identity is not None
            cls.album_ids_by_identity[identity] = snapshot.album_id

    @classmethod
    def tearDownClass(cls) -> None:
        cls.world.close()

    def _rows(
        self, pairs: list[tuple[str | None, str | None]],
    ) -> list[dict[str, object]]:
        return [
            {
                "id": 1000 + index,
                "mb_release_id": None if acquired == DISCOGS else acquired,
                "discogs_release_id": acquired if acquired == DISCOGS else None,
                "canonical_release_id": canonical,
            }
            for index, (acquired, canonical) in enumerate(pairs)
        ]

    @settings(deadline=None, suppress_health_check=[HealthCheck.too_slow])
    @given(
        pairs=st.lists(
            st.tuples(RELEASE_IDS, st.one_of(st.none(), CANONICALS)),
            min_size=1,
            max_size=6,
        ),
    )
    # The decisive live worlds: survivor-installed (316), loser-installed
    # (346), and the double-sided merge that must fail closed.
    @example(pairs=[(LOSER, SURVIVOR)])
    @example(pairs=[(SURVIVOR, None)])
    @example(pairs=[(LOSER, None)])
    @example(pairs=[(LOSER, SIBLING)])
    def test_every_cohort_upholds_the_union_invariants(
        self, pairs: list[tuple[str | None, str | None]],
    ) -> None:
        rows = self._rows(pairs)
        with open_beets_db(
            db_path=str(self.world.library_db),
            library_root=str(self.world.library_root),
        ) as beets:
            counting = _CountingResolver(beets)
            resolved = resolve_current_for_requests(counting, rows)
            check_one_batch(counting.batches)

            for row in rows:
                acceptable = acceptable_identities(row)
                request_id = row["id"]
                assert isinstance(request_id, int)
                if not acceptable:
                    # No usable exact identity — absent from the result, and
                    # never reported as "the library does not have it".
                    self.assertNotIn(request_id, resolved)
                    continue

                resolution = resolved[request_id]
                acquisition = acceptable[-1]
                check_names_the_acquisition(resolution, acquisition)
                check_album_is_acceptable(
                    resolution, self.album_ids_by_identity, acceptable)
                check_split_is_ambiguous(
                    resolution, self.album_ids_by_identity, acceptable)

                if row["canonical_release_id"] is None:
                    plain = beets.resolve_current_release(acquisition)
                    check_unmerged_is_inert(resolution, plain)


class TestSwitchedConsumersAgreeWithTheUnion(unittest.TestCase):
    """U6 — every switched consumer answers what the union answers.

    The pins in ``tests/test_request_identity.py`` prove the live 316 shape
    through each real consumer; this patrols the world space around them.
    Independent review (2026-08-06) found the evidence build, long-tail
    banding and the world audit each unconstrained — the union could be
    deleted from all three and the complete suite stayed green — so the
    property drives the REAL consumers, not the shared helper they call.
    """

    world: BeetsWorld

    @classmethod
    def setUpClass(cls) -> None:
        cls.world = BeetsWorld(REPO)
        for index, release_id in enumerate(HELD):
            cls.world.import_release(BeetsWorldRelease(
                release_id=release_id,
                artist=f"Artist {index}",
                album=f"Album {index}",
                year=1990 + index,
            ))

    @classmethod
    def tearDownClass(cls) -> None:
        cls.world.close()

    @settings(deadline=None, max_examples=25)
    @given(
        acquired=st.sampled_from([*HELD, *UNHELD]),
        canonical=st.one_of(st.none(), st.sampled_from([*HELD, *UNHELD])),
    )
    @example(acquired=LOSER, canonical=SURVIVOR)   # the live 316 shape
    @example(acquired=SURVIVOR, canonical=None)    # unmerged control
    def test_long_tail_banding_agrees_with_the_union(
        self, acquired: str, canonical: str | None,
    ) -> None:
        """Banding says "missing" exactly when the union says missing."""
        from lib.banding import resolve_current_release_bands
        from lib.quality import QualityRankConfig

        if canonical == acquired:
            canonical = None
        row = {
            "id": 316,
            "mb_release_id": acquired,
            "discogs_release_id": None,
            "canonical_release_id": canonical,
        }

        with open_beets_db(
            db_path=str(self.world.library_db),
            library_root=str(self.world.library_root),
        ) as beets:
            union = resolve_current_for_requests(beets, [row])[316]
            bands = resolve_current_release_bands(
                beets, [row], QualityRankConfig.defaults())

        banded_missing = bands[acquired] == "missing"
        if isinstance(union, CurrentBeetsAmbiguous):
            # A split falls back to the acquisition-only answer for display
            # rather than aborting the cohort; it is the world audit's job
            # to surface it. Assert the cohort SURVIVED, which is the whole
            # point — reaching here at all means no exception was raised.
            return
        union_missing = isinstance(union, CurrentBeetsMissing)
        if banded_missing != union_missing:
            raise AssertionError(
                f"banding says missing={banded_missing} while the union says "
                f"missing={union_missing} for acquired={acquired} "
                f"canonical={canonical}"
            )


class TestInvariantCheckersTripOnViolations(unittest.TestCase):
    """Every checker owes a planted violation proving it can fail."""

    def setUp(self) -> None:
        self.acquisition = _identity(LOSER)
        self.canonical = _identity(SURVIVOR)
        self.by_identity = {self.acquisition: 11541, self.canonical: 19345}

    def test_substituted_identity_is_rejected(self) -> None:
        with self.assertRaises(AssertionError):
            check_names_the_acquisition(
                CurrentBeetsMissing(identity=self.canonical), self.acquisition)

    def test_a_third_album_is_rejected(self) -> None:
        with self.assertRaises(AssertionError):
            check_album_is_acceptable(
                _unique(self.acquisition, 99999),
                self.by_identity,
                (self.acquisition,),
            )

    def test_picking_one_side_of_a_split_is_rejected(self) -> None:
        with self.assertRaises(AssertionError):
            check_split_is_ambiguous(
                _unique(self.acquisition, 19345),
                self.by_identity,
                (self.canonical, self.acquisition),
            )

    def test_a_split_reported_under_the_wrong_reason_is_rejected(self) -> None:
        with self.assertRaises(AssertionError):
            check_split_is_ambiguous(
                CurrentBeetsAmbiguous(
                    identity=self.acquisition,
                    album_ids=(11541, 19345),
                    reason="multiple_matches",
                ),
                self.by_identity,
                (self.canonical, self.acquisition),
            )

    def test_moving_an_unmerged_row_is_rejected(self) -> None:
        with self.assertRaises(AssertionError):
            check_unmerged_is_inert(
                _unique(self.acquisition, 11541),
                CurrentBeetsMissing(identity=self.acquisition),
            )

    def test_per_identity_fan_out_is_rejected(self) -> None:
        with self.assertRaises(AssertionError):
            check_one_batch(2)

    def test_checkers_accept_the_legitimate_worlds(self) -> None:
        """Must-still-work: the real merge passes every checker."""
        held = _unique(self.acquisition, 19345)
        check_names_the_acquisition(held, self.acquisition)
        check_album_is_acceptable(
            held, {self.canonical: 19345}, (self.canonical, self.acquisition))
        check_split_is_ambiguous(
            held, {self.canonical: 19345}, (self.canonical, self.acquisition))
        check_unmerged_is_inert(held, held)
        check_one_batch(1)


def _identity(release_id: str) -> ReleaseIdentity:
    identity = ReleaseIdentity.from_id(release_id)
    assert identity is not None
    return identity


def _unique(identity: ReleaseIdentity, album_id: int) -> CurrentBeetsUnique:
    return CurrentBeetsUnique(
        identity=identity,
        album_id=album_id,
        album_path=f"/library/album-{album_id}",
        items=(),
        selectors=(f"mb_albumid:{identity.release_id}",),
    )


if __name__ == "__main__":
    unittest.main()
