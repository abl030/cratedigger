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
U6  Every switched consumer reports the state the union resolved, driven
    through that consumer's own outermost adapter. Five consumers were
    found unconstrained by fault injection across two review rounds; a
    property that stops at the shared helper cannot see the revert,
    because the revert lives inside the adapter.
U7  A presence-only consumer — one that can only ask "is any acceptable
    id in beets" — holds an album exactly when the union found one. Disk
    coverage cannot answer U6's three states at all, and a split is
    ambiguous to the join while still being on disk.
"""

from __future__ import annotations

import unittest
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING
from unittest.mock import MagicMock, patch

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

if TYPE_CHECKING:
    from tests.fakes import FakePipelineDB

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


def check_consumer_state_agrees(
    consumer: str,
    observed: str,
    union: CurrentBeetsResolution,
) -> None:
    """U6 — a switched consumer reports the state the union resolved.

    ``observed`` is the consumer's own verdict normalised onto the union's
    three states. A consumer that quietly reverted to the acquisition id
    alone reports ``missing`` for a survivor-held album while the union
    says ``unique`` — the live 316 shape — and this is what separates it
    from a pin on the shared helper, which such a revert walks straight
    past.
    """
    expected = (
        "unique"
        if isinstance(union, CurrentBeetsUnique)
        else "missing"
        if isinstance(union, CurrentBeetsMissing)
        else "ambiguous"
    )
    if observed != expected:
        raise AssertionError(
            f"{consumer} reports {observed!r} while the union resolved "
            f"{expected!r}; that consumer is not resolving over the union"
        )


def check_presence_agrees(
    consumer: str,
    present: bool,
    union: CurrentBeetsResolution,
) -> None:
    """U7 — a presence-only consumer holds an album iff the union found one.

    Disk coverage cannot use :func:`check_consumer_state_agrees`: it asks
    "is any acceptable id in beets", so it genuinely cannot tell a unique
    resolution from a split — and both mean the album IS on disk. Forcing
    it into the three-state checker would make the test read the union to
    decide what to expect, which proves nothing. The honest agreement is
    binary: off-disk exactly when the union is missing.
    """
    expected = not isinstance(union, CurrentBeetsMissing)
    if present != expected:
        raise AssertionError(
            f"{consumer} reports on-disk={present} while the union "
            f"resolved {type(union).__name__}, which means on-disk="
            f"{expected}; that consumer is not resolving over the union"
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
    Independent review found five consumers unconstrained in two rounds —
    the evidence build, long-tail banding and the world audit (2026-08-06),
    then automation recovery evidence and the post-import evidence refresh
    (2026-08-07). Each could have its union deleted with the complete suite
    still green. So every property here drives the REAL consumer's outermost
    adapter, never the shared helper it calls: a revert to
    ``resolve_current_release`` lives inside that adapter and a property
    stopping at ``resolve_current_for_request`` walks straight past it.
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
            # A split bands "unknown" rather than aborting the cohort or
            # asserting the quality of one arbitrarily chosen side.
            # Reaching here at all proves the cohort survived.
            if bands[acquired] != "unknown":
                raise AssertionError(
                    f"a split must band unknown, not {bands[acquired]!r} — "
                    "the worklist band is a decision surface, and #1059 "
                    "invariant 3 forbids a silent pick"
                )
            return
        union_missing = isinstance(union, CurrentBeetsMissing)
        if banded_missing != union_missing:
            raise AssertionError(
                f"banding says missing={banded_missing} while the union says "
                f"missing={union_missing} for acquired={acquired} "
                f"canonical={canonical}"
            )

    def _merged_request(
        self, db: FakePipelineDB, acquired: str, canonical: str | None,
    ) -> tuple[int, dict[str, object]]:
        """Seed one request in the generated merge state, and its row."""
        request_id = db.add_request(
            artist_name="Merged Artist",
            album_title="Merged Album",
            source="request",
            mb_release_id=acquired,
        )
        if canonical is not None:
            db.record_canonical_release_id(
                request_id,
                canonical_release_id=canonical,
                resolved_at=datetime(2026, 8, 6, tzinfo=UTC),
            )
        return request_id, {
            "id": request_id,
            "mb_release_id": acquired,
            "discogs_release_id": None,
            "canonical_release_id": canonical,
        }

    @settings(deadline=None, max_examples=25)
    @given(
        acquired=st.sampled_from([*HELD, *UNHELD]),
        canonical=st.one_of(st.none(), st.sampled_from([*HELD, *UNHELD])),
    )
    @example(acquired=LOSER, canonical=SURVIVOR)   # the live 316 shape
    @example(acquired=SURVIVOR, canonical=None)    # unmerged control
    def test_recovery_evidence_agrees_with_the_union(
        self, acquired: str, canonical: str | None,
    ) -> None:
        """``exact_library`` reports what the union resolved.

        Driven through the REAL ``get_automation_recovery_detail`` over a
        real automation owner: the union call is in a private helper, so
        this is the outermost adapter an operator's triage read actually
        goes through.
        """
        from lib.import_job_recovery_service import (
            get_automation_recovery_detail,
        )
        from tests.fakes import FakePipelineDB
        from tests.helpers import handoff_automation_owner

        if canonical == acquired:
            canonical = None
        db = FakePipelineDB()
        request_id, row = self._merged_request(db, acquired, canonical)
        job = handoff_automation_owner(db, request_id)

        with open_beets_db(
            db_path=str(self.world.library_db),
            library_root=str(self.world.library_root),
        ) as beets:
            union = resolve_current_for_requests(beets, [row])[request_id]
            result = get_automation_recovery_detail(db, beets, job.id)

        assert result.detail is not None
        check_consumer_state_agrees(
            "automation recovery evidence",
            result.detail.exact_library.status,
            union,
        )

    @settings(deadline=None, max_examples=25)
    @given(
        acquired=st.sampled_from([*HELD, *UNHELD]),
        canonical=st.one_of(st.none(), st.sampled_from([*HELD, *UNHELD])),
    )
    @example(acquired=LOSER, canonical=SURVIVOR)   # the live 316 shape
    @example(acquired=SURVIVOR, canonical=None)    # unmerged control
    def test_post_import_evidence_refresh_agrees_with_the_union(
        self, acquired: str, canonical: str | None,
    ) -> None:
        """The post-import writer finds an album exactly when the union does.

        Only the three-way resolution fork is compared. ``empty_current``
        and ``ambiguous_current`` are the refresh's own words for the two
        non-unique branches; every other status it can return is downstream
        of a resolved album (missing bitrate metadata, propagation, the
        exact-linked-row check) and so is folded into ``unique``.
        """
        from lib.dispatch import _refresh_current_evidence_after_import
        from lib.quality import QualityRankConfig
        from tests.fakes import FakePipelineDB

        if canonical == acquired:
            canonical = None
        db = FakePipelineDB()
        request_id, row = self._merged_request(db, acquired, canonical)

        with open_beets_db(
            db_path=str(self.world.library_db),
            library_root=str(self.world.library_root),
        ) as beets:
            union = resolve_current_for_requests(beets, [row])[request_id]

        result = _refresh_current_evidence_after_import(
            db,
            request_id=request_id,
            mb_release_id=acquired,
            quality_ranks=QualityRankConfig.defaults(),
            beets_library_db_path=str(self.world.library_db),
            beets_library_root=str(self.world.library_root),
        )

        observed = (
            "missing" if result.status == "empty_current"
            else "ambiguous" if result.status == "ambiguous_current"
            else "unique"
        )
        check_consumer_state_agrees(
            "the post-import evidence refresh", observed, union)

    @settings(deadline=None, max_examples=25)
    @given(
        acquired=st.sampled_from([*HELD, *UNHELD]),
        canonical=st.one_of(st.none(), st.sampled_from([*HELD, *UNHELD])),
    )
    @example(acquired=LOSER, canonical=SURVIVOR)   # the live 316 shape
    @example(acquired=SURVIVOR, canonical=None)    # unmerged control
    @example(acquired=LOSER, canonical=SIBLING)    # a split is still on disk
    def test_disk_coverage_agrees_with_the_union(
        self, acquired: str, canonical: str | None,
    ) -> None:
        """Disk coverage calls a request off-disk exactly when the union
        finds no album for it, in every world.

        Driven through the REAL ``disk_coverage`` service — the derivation
        the dashboard card, ``GET /api/disk-coverage`` and
        ``pipeline-cli disk-coverage`` all share.
        """
        from lib.disk_coverage_service import disk_coverage
        from tests.fakes import FakePipelineDB

        if canonical == acquired:
            canonical = None
        db = FakePipelineDB()
        request_id, row = self._merged_request(db, acquired, canonical)

        with open_beets_db(
            db_path=str(self.world.library_db),
            library_root=str(self.world.library_root),
        ) as beets:
            union = resolve_current_for_requests(beets, [row])[request_id]
            result = disk_coverage(db, beets, include_rows=True)

        off_disk = {off.id for off in (result.off_disk or [])}
        check_presence_agrees(
            "disk coverage", request_id not in off_disk, union)

    @settings(deadline=None, max_examples=25)
    @given(
        acquired=st.sampled_from([*HELD, *UNHELD]),
        canonical=st.one_of(st.none(), st.sampled_from([*HELD, *UNHELD])),
    )
    @example(acquired=LOSER, canonical=SURVIVOR)
    @example(acquired=SURVIVOR, canonical=None)
    def test_replace_cleanup_uses_the_union_filed_identity(
        self, acquired: str, canonical: str | None,
    ) -> None:
        """Replace delegates deletion only to the union's filed album.

        The shared BeetsWorld has one real album per held identity, so merged
        pairs can produce unique, missing, and split authority without the
        producer-impossible shape of two identities on one Beets row.  The
        child is injected only to keep that immutable property library intact;
        the public Replace service and real Beets resolver both run.
        """
        from lib.beets_delete import BeetsDeleteCompleted, BeetsDeleteRequest
        from lib.config import CratediggerConfig
        from lib.mbid_replace_service import (
            RESULT_REPLACED,
            RESULT_WRONG_STATE,
            MbidReplaceService,
        )
        from tests.fakes import FakePipelineDB

        if canonical == acquired:
            canonical = None
        db = FakePipelineDB()
        request_id, row = self._merged_request(db, acquired, canonical)
        db.request(request_id)["status"] = "imported"
        db.request(request_id)["mb_release_group_id"] = (
            "11111111-1111-1111-1111-111111111111"
        )
        calls: list[BeetsDeleteRequest] = []

        def exact_delete(request: BeetsDeleteRequest) -> BeetsDeleteCompleted:
            calls.append(request)
            return BeetsDeleteCompleted(
                album_id=request.album_id,
                album_name="Merged Album",
                artist_name="Merged Artist",
                former_album_path="/immutable/generated-world",
                deleted_tracks=1,
                deleted_artifacts=1,
                preserved_paths=(),
            )

        target = {
            "id": "99999999-9999-9999-9999-999999999999",
            "title": "Replacement",
            "artist_name": "Merged Artist",
            "artist_id": "artist-1",
            "release_group_id": "11111111-1111-1111-1111-111111111111",
            "year": 2026,
            "country": "AU",
            "tracks": [],
        }
        with open_beets_db(
            db_path=str(self.world.library_db),
            library_root=str(self.world.library_root),
        ) as beets:
            union = resolve_current_for_requests(beets, [row])[request_id]

        with (
            patch("lib.mbid_replace_service.trigger_plex_scan"),
            patch("lib.mbid_replace_service.trigger_jellyfin_scan"),
        ):
            result = MbidReplaceService(
                db,
                CratediggerConfig(),
                beets_db_factory=lambda: open_beets_db(
                    db_path=str(self.world.library_db),
                    library_root=str(self.world.library_root),
                ),
                mb_lookup=lambda _rid, *, fresh=False: target,
                search_plan_service=MagicMock(),
                beets_delete_fn=exact_delete,
                wrong_match_delete_fn=lambda _db, _request_id: MagicMock(
                    errors=0, remaining=0,
                ),
            ).replace_request_mbid(
                request_id,
                target_mb_release_id=str(target["id"]),
            )

        if isinstance(union, CurrentBeetsAmbiguous):
            self.assertEqual(result.outcome, RESULT_WRONG_STATE)
            self.assertEqual(calls, [])
        elif isinstance(union, CurrentBeetsMissing):
            self.assertEqual(result.outcome, RESULT_REPLACED)
            self.assertEqual(calls, [])
        else:
            self.assertEqual(result.outcome, RESULT_REPLACED)
            self.assertEqual([call.album_id for call in calls], [union.album_id])
            self.assertEqual(
                [call.expected_release_id for call in calls],
                [union.filed_identity.release_id],
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

    def test_a_consumer_calling_a_held_album_missing_is_rejected(self) -> None:
        """The exact shape of both surviving mutants: the union found the
        survivor's album, the consumer reverted to the acquisition id and
        told the operator nothing is installed."""
        with self.assertRaises(AssertionError):
            check_consumer_state_agrees(
                "a reverted consumer",
                "missing",
                _unique(self.acquisition, 19345),
            )

    def test_a_consumer_hiding_a_split_is_rejected(self) -> None:
        with self.assertRaises(AssertionError):
            check_consumer_state_agrees(
                "a reverted consumer",
                "unique",
                CurrentBeetsAmbiguous(
                    identity=self.acquisition,
                    album_ids=(11541, 19345),
                    reason="merged_identity_split",
                ),
            )

    def test_a_presence_consumer_missing_a_held_album_is_rejected(
        self,
    ) -> None:
        """The disk-coverage mutant: the union found the survivor's album
        and the drift card still called the request off-disk."""
        with self.assertRaises(AssertionError):
            check_presence_agrees(
                "a reverted presence consumer",
                False,
                _unique(self.acquisition, 19345),
            )

    def test_a_presence_consumer_claiming_an_absent_album_is_rejected(
        self,
    ) -> None:
        with self.assertRaises(AssertionError):
            check_presence_agrees(
                "a reverted presence consumer",
                True,
                CurrentBeetsMissing(identity=self.acquisition),
            )

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
        check_consumer_state_agrees("a switched consumer", "unique", held)
        check_consumer_state_agrees(
            "a switched consumer",
            "missing",
            CurrentBeetsMissing(identity=self.acquisition),
        )
        check_presence_agrees("a presence consumer", True, held)
        check_presence_agrees(
            "a presence consumer",
            False,
            CurrentBeetsMissing(identity=self.acquisition),
        )
        # A split is ambiguous to the join and still ON DISK to a
        # presence-only consumer — the case the three-state checker
        # cannot express.
        check_presence_agrees(
            "a presence consumer",
            True,
            CurrentBeetsAmbiguous(
                identity=self.acquisition,
                album_ids=(11541, 19345),
                reason="merged_identity_split",
            ),
        )


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
