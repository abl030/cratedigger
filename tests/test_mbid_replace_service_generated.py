"""Generated patrol for the cross-pathway Replace gate (issue #1366).

Invariants, written down first:

* An unflagged target from the other pathway is never written: the
  outcome is ``target_invalid`` / ``cross_pathway_target``, no mirror is
  consulted, and the source row is untouched.
* A flagged cross-pathway Replace supersedes at the CANONICAL of exactly
  the operator's target, in the target pathway's own row shape: an MB
  target lands as a UUID with no Discogs id and its MB release group; a
  Discogs target lands dual-written with its master (or no master — a
  masterless release is a legal cross-pathway target). A mirror 301
  canonical is the same release under its surviving id, never a sibling;
  the canonical must carry the target pathway's shape.
* Phase 0 order is a contract ("guardrails before IO"): a pre-check
  collision on the requested id refuses BEFORE any mirror lookup and names
  the holder's status; an MB target without a release group is refused
  after exactly one lookup; a canonical redirect onto an id another active
  row holds is refused after that one lookup and names the holder.
* Crossing needs nothing from the source's group: the source-pathway
  lookup is never issued, whether or not the source has a persisted group.
* The flag is inert on a same-pathway target: identical worlds produce
  identical outcomes with the flag on and off.
* Same-pathway lifecycle facts hold: the old row freezes as ``replaced``
  with its identity untouched, and the descendant points back.
* Every result echoes the source request id; every refusal carries an
  operator-facing message; a replaced cross-pathway target was looked up
  on its own mirror exactly once, for exactly the id the operator typed.
* The mirror's canonical id must be a release id on the target's pathway:
  a canonical of the other shape (including the source's own id) or of no
  shape (the Discogs Struct's ``0`` default) is refused as unresolvable
  after exactly one lookup, before the release-group check, with no write.
* The descendant carries the mirror's metadata (artist, title, artist id,
  year, country) on the right columns and the payload's tracks as its
  track rows.
* The target id's letter case is inert (issue #1382 item 3): an MB UUID
  pasted in uppercase names the same release as its canonical lowercase
  form, so the same world driven with the uppercased target produces the
  same outcome and reason, any row it writes carries the lowercase id, and
  every mirror lookup it makes is by the lowercase id — the pre-check and
  the UNIQUE net see one identity, never two.

Ids are drawn in canonical spelling (lowercase UUIDs, plain integers);
raw-spelling normalization is ``lib.release_identity``'s own contract and
is pinned there, not here.

The checker accumulates violations (every clause evaluates; ordering
cannot mask one). ``TestInvariantCheckersTripOnViolations`` proves each
clause trips on a planted world and stays quiet where production is
right; ``TestCrossPathwayReplaceGenerated`` drives the REAL service over
generated worlds through ``FakePipelineDB`` and injected mirror lookups.
"""

from __future__ import annotations

import unittest
from dataclasses import dataclass, replace
from typing import Literal
from unittest.mock import MagicMock

import msgspec
from hypothesis import assume, example, given
from hypothesis import strategies as st

import tests._hypothesis_profiles  # noqa: F401 - registers suite/fuzz
from lib.mbid_replace_service import (
    REPLACE_REASON_CROSS_PATHWAY_TARGET,
    REPLACE_REASON_TARGET_NO_RELEASE_GROUP,
    REPLACE_REASON_UNRESOLVABLE_TARGET,
    RESULT_REPLACED,
    RESULT_TARGET_COLLISION_REQUEST,
    RESULT_TARGET_INVALID,
    MbidReplaceService,
    ReplaceResult,
)
from tests.fakes import FakePipelineDB
from tests.helpers import make_request_row
from tests.test_mbid_replace_service import (
    DISCOGS_MASTER,
    OLD_DISCOGS_ID,
    OLD_MBID,
    RG_ID,
    _fake_discogs_payload,
    _fake_target_payload,
    _ServiceCase,
)

Pathway = Literal["musicbrainz", "discogs"]
Collision = Literal["none", "target", "canonical"]
CanonicalShape = Literal["target", "other", "zero"]

_PATHWAYS: tuple[Pathway, ...] = ("musicbrainz", "discogs")
_SOURCE_STATUSES = ("wanted", "downloading", "unsearchable", "imported")
_RESERVED_IDS = frozenset({OLD_MBID, OLD_DISCOGS_ID})
HOLDER_ID = 43
HOLDER_STATUS = "downloading"


def _mirror_metadata(pathway: Pathway) -> dict[str, object]:
    """What the payload builders say about the target beyond its identity,
    read from the builders themselves so a fixture change cannot drift."""
    payload = (
        _fake_target_payload() if pathway == "musicbrainz"
        else _fake_discogs_payload()
    )
    return {
        "artist_name": payload["artist_name"],
        "album_title": payload["title"],
        "mb_artist_id": payload["artist_id"],
        "year": payload["year"],
        "country": payload["country"],
    }


MIRROR_METADATA: dict[str, dict[str, object]] = {
    "musicbrainz": _mirror_metadata("musicbrainz"),
    "discogs": _mirror_metadata("discogs"),
}
MIRROR_TRACK_TITLES = [t["title"] for t in _fake_target_payload()["tracks"]]


@dataclass(frozen=True)
class World:
    """One Replace attempt: a seeded source, a target, the mirror's answer,
    who else holds what, and the opt-in."""

    source_pathway: Pathway
    source_has_group: bool
    source_status: str
    target_pathway: Pathway
    target_id: str
    target_has_group: bool
    cross_pathway: bool
    canonical_redirect: bool
    canonical_id: str
    collision: Collision
    canonical_shape: CanonicalShape = "target"

    @property
    def crosses(self) -> bool:
        return self.source_pathway != self.target_pathway

    @property
    def canonical_is_valid(self) -> bool:
        """Whether the id the mirror reports is a release id on the
        target's pathway (the shape the resolvers require)."""
        return not self.canonical_redirect or self.canonical_shape == "target"

    @property
    def target_group(self) -> str | None:
        if not self.target_has_group:
            return None
        return RG_ID if self.target_pathway == "musicbrainz" else DISCOGS_MASTER

    @property
    def resolved_id(self) -> str:
        """The id the mirror reports for the target: its canonical."""
        return self.canonical_id if self.canonical_redirect else self.target_id


@dataclass(frozen=True)
class Run:
    """What one drive of the real service observed."""

    world: World
    result: ReplaceResult
    source_before: dict
    source_after: dict
    descendant: dict | None
    descendant_tracks: tuple[str, ...]
    mb_ids: tuple[str, ...]
    discogs_ids: tuple[str, ...]

    @property
    def mb_lookups(self) -> int:
        return len(self.mb_ids)

    @property
    def discogs_lookups(self) -> int:
        return len(self.discogs_ids)

    @property
    def target_ids(self) -> tuple[str, ...]:
        return (
            self.mb_ids if self.world.target_pathway == "musicbrainz"
            else self.discogs_ids
        )


def _ids(pathway: Pathway) -> st.SearchStrategy[str]:
    if pathway == "musicbrainz":
        return st.uuids(version=4).map(str)
    return st.integers(min_value=1, max_value=10**9).map(str)


def _other(pathway: Pathway) -> Pathway:
    return "discogs" if pathway == "musicbrainz" else "musicbrainz"


@st.composite
def worlds(draw: st.DrawFn) -> World:
    source_pathway = draw(st.sampled_from(_PATHWAYS))
    target_pathway = draw(st.sampled_from(_PATHWAYS))
    target_id = draw(_ids(target_pathway))
    assume(target_id not in _RESERVED_IDS)
    collision: Collision = draw(st.sampled_from(("none", "target", "canonical")))
    # A collision on the canonical needs a canonical the pre-check can
    # hold: a valid one. Otherwise the mirror may canonicalise onto the
    # other pathway's shape — sometimes the source's own id, the exact
    # case the guard exists for — or onto the shapeless ``"0"``.
    canonical_shape: CanonicalShape = (
        "target" if collision == "canonical"
        else draw(st.sampled_from(("target", "other", "zero")))
    )
    if canonical_shape == "target":
        canonical_id = draw(_ids(target_pathway))
        assume(canonical_id not in _RESERVED_IDS and canonical_id != target_id)
    elif canonical_shape == "other":
        own = OLD_DISCOGS_ID if _other(target_pathway) == "discogs" else OLD_MBID
        canonical_id = draw(st.one_of(
            st.just(own), _ids(_other(target_pathway)),
        ))
    else:
        canonical_id = "0"
    canonical_redirect = (
        True if collision == "canonical" or canonical_shape != "target"
        else draw(st.booleans())
    )
    return World(
        source_pathway=source_pathway,
        source_has_group=draw(st.booleans()),
        source_status=draw(st.sampled_from(_SOURCE_STATUSES)),
        target_pathway=target_pathway,
        target_id=target_id,
        target_has_group=draw(st.booleans()),
        cross_pathway=draw(st.booleans()),
        canonical_redirect=canonical_redirect,
        canonical_id=canonical_id,
        collision=collision,
        canonical_shape=canonical_shape,
    )


# ---------------------------------------------------------------------------
# Checker — module-level, accumulating, so every clause can be proven.
# ---------------------------------------------------------------------------

def _refusal_common(run: Run, out: list[str], what: str) -> None:
    rid = int(run.source_before["id"])
    if run.source_after != run.source_before:
        out.append(f"{what} mutated source {rid}")


def cross_pathway_violations(run: Run) -> list[str]:
    """Every clause evaluates; a clause that does not apply says nothing."""
    world, result = run.world, run.result
    out: list[str] = []
    rid = int(run.source_before["id"])

    if result.request_id != rid:
        out.append(
            f"result.request_id does not echo source {rid}: "
            f"{result.request_id!r}"
        )
    if result.outcome != RESULT_REPLACED and not result.error_message:
        out.append(
            f"refusal {result.outcome!r} carries no error_message"
        )

    if world.crosses and not world.cross_pathway:
        if not (
            result.outcome == RESULT_TARGET_INVALID
            and result.reason == REPLACE_REASON_CROSS_PATHWAY_TARGET
        ):
            out.append(
                "unflagged cross-pathway target was not refused as "
                f"cross_pathway_target: outcome={result.outcome!r} "
                f"reason={result.reason!r}"
            )
        if run.mb_lookups or run.discogs_lookups:
            out.append(
                "unflagged cross-pathway target reached a mirror lookup: "
                f"mb={run.mb_lookups} discogs={run.discogs_lookups}"
            )
        _refusal_common(run, out, "unflagged cross-pathway target")
        return out

    if not world.crosses:
        return out

    # --- flagged cross-pathway: Phase 0 order is the contract -------------
    if world.collision == "target":
        if result.outcome != RESULT_TARGET_COLLISION_REQUEST:
            out.append(
                "pre-check collision on the requested id was not refused: "
                f"outcome={result.outcome!r} reason={result.reason!r}"
            )
        if result.current_status != HOLDER_STATUS:
            out.append(
                "pre-check collision did not name the holder's status: "
                f"{result.current_status!r}"
            )
        if run.target_ids:
            out.append(
                "pre-check collision reached the mirror: asked "
                f"{run.target_ids!r}"
            )
        _refusal_common(run, out, "pre-check collision")
        return out

    if not world.canonical_is_valid:
        if not (
            result.outcome == RESULT_TARGET_INVALID
            and result.reason == REPLACE_REASON_UNRESOLVABLE_TARGET
        ):
            out.append(
                "canonical of the wrong shape was not refused as "
                f"unresolvable_target: outcome={result.outcome!r} "
                f"reason={result.reason!r}"
            )
        if run.target_ids != (world.target_id,):
            out.append(
                "wrong-shape canonical refusal did not ask the mirror exactly "
                f"once for the operator's target {world.target_id!r}: asked "
                f"{run.target_ids!r}"
            )
        _refusal_common(run, out, "wrong-shape canonical refusal")
        return out

    if world.target_pathway == "musicbrainz" and not world.target_has_group:
        if not (
            result.outcome == RESULT_TARGET_INVALID
            and result.reason == REPLACE_REASON_TARGET_NO_RELEASE_GROUP
        ):
            out.append(
                "cross-pathway MB target without a release group was "
                f"not refused as target_no_release_group: "
                f"outcome={result.outcome!r} reason={result.reason!r}"
            )
        _refusal_common(run, out, "refused cross-pathway target")
        return out

    if world.collision == "canonical":
        if result.outcome != RESULT_TARGET_COLLISION_REQUEST:
            out.append(
                "canonical redirect onto a held id was not refused: "
                f"outcome={result.outcome!r} reason={result.reason!r}"
            )
        if result.current_status != HOLDER_STATUS:
            out.append(
                "canonical redirect collision did not name the holder's "
                f"status: {result.current_status!r}"
            )
        if run.target_ids != (world.target_id,):
            out.append(
                "canonical redirect collision did not ask the mirror exactly "
                f"once for the operator's target {world.target_id!r}: asked "
                f"{run.target_ids!r}"
            )
        _refusal_common(run, out, "canonical redirect collision")
        return out

    if result.outcome != RESULT_REPLACED:
        out.append(
            "flagged cross-pathway target was not replaced: "
            f"outcome={result.outcome!r} reason={result.reason!r}"
        )
        return out
    if world.source_pathway == "musicbrainz" and run.mb_lookups:
        out.append(
            "cross-pathway Replace consulted the MB source's group "
            f"({run.mb_lookups} MB lookups)"
        )
    if world.source_pathway == "discogs" and run.discogs_lookups:
        out.append(
            "cross-pathway Replace consulted the Discogs source's master "
            f"({run.discogs_lookups} Discogs lookups)"
        )
    if run.target_ids != (world.target_id,):
        out.append(
            "target mirror was not asked exactly once for the operator's "
            f"target {world.target_id!r}: asked {run.target_ids!r}"
        )
    descendant = run.descendant
    if descendant is None:
        out.append(f"replaced request {rid} has no descendant row")
        return out
    resolved = world.resolved_id
    if world.target_pathway == "musicbrainz":
        expected = {
            "mb_release_id": resolved,
            "discogs_release_id": None,
            "mb_release_group_id": world.target_group,
        }
    else:
        expected = {
            "mb_release_id": resolved,
            "discogs_release_id": resolved,
            "mb_release_group_id": world.target_group,
        }
    actual = {key: descendant.get(key) for key in expected}
    if actual != expected:
        out.append(
            "descendant identity is not the canonical of the operator's "
            f"target in its pathway shape: expected {expected!r}, got "
            f"{actual!r}"
        )
    if run.source_after.get("status") != "replaced":
        out.append(
            f"source {rid} did not freeze as replaced: "
            f"{run.source_after.get('status')!r}"
        )
    frozen_identity = ("mb_release_id", "discogs_release_id",
                       "mb_release_group_id")
    before_identity = {k: run.source_before.get(k) for k in frozen_identity}
    after_identity = {k: run.source_after.get(k) for k in frozen_identity}
    if before_identity != after_identity:
        out.append(
            f"source {rid} identity drifted on supersede: "
            f"{before_identity!r} -> {after_identity!r}"
        )
    if descendant.get("replaces_request_id") != rid:
        out.append(
            f"descendant of {rid} does not point back "
            f"(replaces_request_id="
            f"{descendant.get('replaces_request_id')!r})"
        )
    if descendant.get("status") != "wanted":
        out.append(
            f"descendant of {rid} was not born wanted: "
            f"{descendant.get('status')!r}"
        )
    expected_meta = MIRROR_METADATA[world.target_pathway]
    actual_meta = {key: descendant.get(key) for key in expected_meta}
    if actual_meta != expected_meta:
        out.append(
            "descendant metadata is not the mirror's: expected "
            f"{expected_meta!r}, got {actual_meta!r}"
        )
    if list(run.descendant_tracks) != MIRROR_TRACK_TITLES:
        out.append(
            "descendant tracks are not the mirror's: "
            f"{list(run.descendant_tracks)!r}"
        )
    return out


TargetCase = Literal["as_is", "upper"]


def case_inert_violations(plain: Run, upper: Run) -> list[str]:
    """An uppercased MB target must be indistinguishable from the canonical
    one everywhere the service can be observed (issue #1382 item 3)."""
    out: list[str] = []
    if plain.world.target_pathway != "musicbrainz":
        return out
    if plain.result.outcome != upper.result.outcome:
        out.append(
            "target case changed the outcome: "
            f"{plain.result.outcome!r} (lower) vs {upper.result.outcome!r} (upper)"
        )
    if plain.result.reason != upper.result.reason:
        out.append(
            "target case changed the reason: "
            f"{plain.result.reason!r} (lower) vs {upper.result.reason!r} (upper)"
        )
    if (plain.descendant is None) != (upper.descendant is None):
        out.append("target case changed whether a descendant was written")
    if upper.descendant is not None:
        written = str(upper.descendant.get("mb_release_id"))
        if written != written.lower():
            out.append(f"uppercase target wrote a non-canonical id: {written!r}")
    if upper.mb_ids != plain.mb_ids:
        out.append(
            "target case changed the mirror lookups: "
            f"{plain.mb_ids!r} (lower) vs {upper.mb_ids!r} (upper)"
        )
    return out


def flag_inert_violations(plain: Run, flagged: Run) -> list[str]:
    """Same-pathway worlds: the opt-in must not change the outcome."""
    out: list[str] = []
    if plain.world.crosses:
        return out
    if plain.result.outcome != flagged.result.outcome:
        out.append(
            "cross_pathway flag changed a same-pathway outcome: "
            f"{plain.result.outcome!r} (off) vs "
            f"{flagged.result.outcome!r} (on)"
        )
    if plain.result.reason != flagged.result.reason:
        out.append(
            "cross_pathway flag changed a same-pathway reason: "
            f"{plain.result.reason!r} (off) vs {flagged.result.reason!r} (on)"
        )
    return out


# ---------------------------------------------------------------------------
# Driver — the REAL service over a FakePipelineDB with injected lookups.
# ---------------------------------------------------------------------------

class _Driver(_ServiceCase):
    """Reuses the deterministic module's seeding and service wiring."""

    def runTest(self) -> None:  # pragma: no cover - never collected
        pass

    def _seed_holder(self, db: FakePipelineDB, world: World, held: str) -> None:
        """Another active request already holding ``held`` (the requested
        target or its canonical), in the target pathway's row shape."""
        if world.target_pathway == "musicbrainz":
            row = make_request_row(
                id=HOLDER_ID, mb_release_id=held,
                mb_release_group_id=world.target_group, status=HOLDER_STATUS,
            )
        else:
            # ``discogs_release_id`` only: the shape only the identity-aware
            # lookup resolves (KTD-6). An MB-only lookup misses it, and so
            # does the supersede's UNIQUE net on ``mb_release_id``.
            row = make_request_row(
                id=HOLDER_ID, mb_release_id=None, discogs_release_id=held,
                mb_release_group_id=world.target_group, status=HOLDER_STATUS,
            )
        db.seed_request(row)

    def drive(
        self, world: World, *, cross_pathway: bool,
        target_case: TargetCase = "as_is",
    ) -> Run:
        db = FakePipelineDB()
        if world.source_pathway == "musicbrainz":
            self._seed_old(
                db,
                status=world.source_status,
                mb_release_group_id=RG_ID if world.source_has_group else None,
            )
        else:
            self._seed_discogs(
                db,
                status=world.source_status,
                master=DISCOGS_MASTER if world.source_has_group else None,
            )
        if world.collision == "target":
            self._seed_holder(db, world, world.target_id)
        elif world.collision == "canonical":
            self._seed_holder(db, world, world.canonical_id)
        source_before = dict(db.request(42))

        mb_ids: list[str] = []
        discogs_ids: list[str] = []

        def mb_lookup(mbid, *, fresh=False):
            mb_ids.append(str(mbid))
            if str(mbid) == OLD_MBID:
                return _fake_target_payload(mbid=OLD_MBID, rg_id=RG_ID)
            return _fake_target_payload(
                mbid=world.resolved_id, rg_id=world.target_group,
            )

        def discogs_lookup(rid, *, fresh=False):
            discogs_ids.append(str(rid))
            if str(rid) == OLD_DISCOGS_ID:
                return _fake_discogs_payload(
                    release_id=OLD_DISCOGS_ID, master=DISCOGS_MASTER,
                )
            return _fake_discogs_payload(
                release_id=world.resolved_id, master=world.target_group,
            )

        with self._patch_externals_scoped():
            svc: MbidReplaceService = self._make_service(
                db,
                mb_lookup=mb_lookup,
                discogs_lookup=discogs_lookup,
                search_plan_service=MagicMock(),
            )
            result = svc.replace_request_mbid(
                42,
                target_mb_release_id=(
                    world.target_id.upper() if target_case == "upper"
                    else world.target_id
                ),
                cross_pathway=cross_pathway,
            )
        source_after = dict(db.request(42))
        descendant = db.get_request_by_replaces_request_id(42)
        tracks: tuple[str, ...] = ()
        if descendant is not None:
            tracks = tuple(
                str(t["title"]) for t in db.get_tracks(int(descendant["id"]))
            )
        return Run(
            world=replace(world, cross_pathway=cross_pathway),
            result=result,
            source_before=source_before,
            source_after=source_after,
            descendant=dict(descendant) if descendant is not None else None,
            descendant_tracks=tracks,
            mb_ids=tuple(mb_ids),
            discogs_ids=tuple(discogs_ids),
        )


_CROSS_MB_TO_DISCOGS = World(
    source_pathway="musicbrainz", source_has_group=True,
    source_status="imported", target_pathway="discogs", target_id="1002",
    target_has_group=True, cross_pathway=True,
    canonical_redirect=False, canonical_id="1003", collision="none",
)
_CROSS_DISCOGS_TO_MB = World(
    source_pathway="discogs", source_has_group=True, source_status="wanted",
    target_pathway="musicbrainz",
    target_id="bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
    target_has_group=True, cross_pathway=True,
    canonical_redirect=False,
    canonical_id="cccccccc-cccc-cccc-cccc-cccccccccccc", collision="none",
)
_CROSS_MASTERLESS = replace(_CROSS_MB_TO_DISCOGS, target_has_group=False)
_CROSS_UNFLAGGED = replace(_CROSS_MB_TO_DISCOGS, cross_pathway=False)
_CROSS_MB_NO_GROUP = replace(_CROSS_DISCOGS_TO_MB, target_has_group=False)
_CROSS_REDIRECT = replace(_CROSS_DISCOGS_TO_MB, canonical_redirect=True)
_CROSS_TARGET_COLLISION = replace(_CROSS_MB_TO_DISCOGS, collision="target")
_CROSS_CANONICAL_COLLISION = replace(
    _CROSS_DISCOGS_TO_MB, canonical_redirect=True, collision="canonical",
)
_CROSS_CANONICAL_IS_SOURCE = replace(
    _CROSS_DISCOGS_TO_MB, canonical_redirect=True, canonical_shape="other",
    canonical_id=OLD_DISCOGS_ID,
)
_CROSS_CANONICAL_ZERO = replace(
    _CROSS_MB_TO_DISCOGS, canonical_redirect=True, canonical_shape="zero",
    canonical_id="0",
)
_SAME_MB = World(
    source_pathway="musicbrainz", source_has_group=True,
    source_status="wanted", target_pathway="musicbrainz",
    target_id="bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb", target_has_group=True,
    cross_pathway=False, canonical_redirect=False,
    canonical_id="cccccccc-cccc-cccc-cccc-cccccccccccc", collision="none",
)
_PINNED_CROSS = (
    _CROSS_MB_TO_DISCOGS, _CROSS_DISCOGS_TO_MB, _CROSS_MASTERLESS,
    _CROSS_UNFLAGGED, _CROSS_MB_NO_GROUP, _CROSS_REDIRECT,
    _CROSS_TARGET_COLLISION, _CROSS_CANONICAL_COLLISION,
    _CROSS_CANONICAL_IS_SOURCE, _CROSS_CANONICAL_ZERO,
)


class TestCrossPathwayReplaceGenerated(unittest.TestCase):
    """Property + pinned worlds, all through the real service."""

    def setUp(self) -> None:
        self.driver = _Driver()

    @given(world=worlds())
    @example(world=_CROSS_MB_TO_DISCOGS)
    @example(world=_CROSS_DISCOGS_TO_MB)
    @example(world=_CROSS_MASTERLESS)
    @example(world=_CROSS_UNFLAGGED)
    @example(world=_CROSS_MB_NO_GROUP)
    @example(world=_CROSS_REDIRECT)
    @example(world=_CROSS_TARGET_COLLISION)
    @example(world=_CROSS_CANONICAL_COLLISION)
    @example(world=_CROSS_CANONICAL_IS_SOURCE)
    @example(world=_CROSS_CANONICAL_ZERO)
    @example(world=replace(_CROSS_CANONICAL_ZERO, target_has_group=False))
    @example(world=replace(_CROSS_MB_TO_DISCOGS, source_has_group=False))
    @example(world=replace(_CROSS_DISCOGS_TO_MB, source_has_group=False))
    @example(world=replace(_CROSS_MB_NO_GROUP, collision="target"))
    def test_cross_pathway_gate_and_supersede_shape(self, world: World) -> None:
        run = self.driver.drive(world, cross_pathway=world.cross_pathway)
        violations = cross_pathway_violations(run)
        self.assertEqual(violations, [], "\n".join(violations))

    @given(world=worlds())
    @example(world=_SAME_MB)
    @example(world=replace(_SAME_MB, source_has_group=False))
    @example(world=replace(_SAME_MB, target_has_group=False))
    @example(world=replace(_SAME_MB, canonical_redirect=True))
    @example(world=replace(_SAME_MB, collision="target"))
    @example(world=replace(
        _SAME_MB, canonical_redirect=True, collision="canonical"))
    @example(world=replace(
        _SAME_MB, canonical_redirect=True, canonical_shape="zero",
        canonical_id="0"))
    @example(world=World(
        source_pathway="discogs", source_has_group=True, source_status="wanted",
        target_pathway="discogs", target_id="1002", target_has_group=True,
        cross_pathway=False, canonical_redirect=False, canonical_id="1003",
        collision="none",
    ))
    def test_flag_is_inert_on_same_pathway_targets(self, world: World) -> None:
        assume(not world.crosses)
        plain = self.driver.drive(world, cross_pathway=False)
        flagged = self.driver.drive(world, cross_pathway=True)
        violations = flag_inert_violations(plain, flagged)
        self.assertEqual(violations, [], "\n".join(violations))

    @given(world=worlds())
    @example(world=_SAME_MB)
    @example(world=replace(_SAME_MB, collision="target"))
    @example(world=replace(_SAME_MB, collision="canonical"))
    @example(world=replace(_SAME_MB, canonical_redirect=True))
    @example(world=replace(_SAME_MB, target_has_group=False))
    @example(world=_CROSS_DISCOGS_TO_MB)
    @example(world=replace(_CROSS_DISCOGS_TO_MB, collision="target"))
    def test_target_case_is_inert(self, world: World) -> None:
        assume(world.target_pathway == "musicbrainz")
        plain = self.driver.drive(world, cross_pathway=world.crosses)
        upper = self.driver.drive(
            world, cross_pathway=world.crosses, target_case="upper",
        )
        violations = case_inert_violations(plain, upper)
        self.assertEqual(violations, [], "\n".join(violations))


def _with_result(run: Run, **changes: object) -> Run:
    """A copy of ``run`` whose ``ReplaceResult`` (a msgspec Struct, not a
    dataclass) carries the planted change."""
    return replace(run, result=msgspec.structs.replace(run.result, **changes))


class TestInvariantCheckersTripOnViolations(unittest.TestCase):
    """Known-bad self-tests, per clause: Q1 trips on its own message,
    Q3 stays quiet on a correct world the clause's condition resembles."""

    def setUp(self) -> None:
        self.driver = _Driver()

    def _correct(self, world: World) -> Run:
        return self.driver.drive(world, cross_pathway=world.cross_pathway)

    def test_every_pinned_world_is_quiet_on_correct_production(self) -> None:
        for world in (*_PINNED_CROSS, _SAME_MB):
            with self.subTest(world=world):
                self.assertEqual(
                    cross_pathway_violations(self._correct(world)), [],
                )

    def test_case_inert_clauses_trip(self) -> None:
        """Each clause of ``case_inert_violations`` on a planted upper run;
        quiet on the real one and on a Discogs-target world."""
        world = replace(_SAME_MB, collision="none")
        plain = self.driver.drive(world, cross_pathway=False)
        upper = self.driver.drive(world, cross_pathway=False, target_case="upper")
        self.assertEqual(case_inert_violations(plain, upper), [])
        with self.subTest(clause="outcome"):
            self.assertRegex(
                "\n".join(case_inert_violations(plain, _with_result(upper, outcome="target_invalid"))),
                r"^target case changed the outcome",
            )
        with self.subTest(clause="reason"):
            self.assertRegex(
                "\n".join(case_inert_violations(plain, _with_result(upper, reason="planted"))),
                r"target case changed the reason",
            )
        with self.subTest(clause="descendant presence"):
            self.assertRegex(
                "\n".join(case_inert_violations(plain, replace(upper, descendant=None))),
                r"target case changed whether a descendant was written",
            )
        with self.subTest(clause="non-canonical id written"):
            assert upper.descendant is not None
            planted = replace(upper, descendant={
                **upper.descendant, "mb_release_id": world.target_id.upper(),
            })
            self.assertRegex(
                "\n".join(case_inert_violations(plain, planted)),
                r"uppercase target wrote a non-canonical id",
            )
        with self.subTest(clause="mirror lookups"):
            planted = replace(upper, mb_ids=(world.target_id.upper(),))
            self.assertRegex(
                "\n".join(case_inert_violations(plain, planted)),
                r"target case changed the mirror lookups",
            )
        with self.subTest(clause="quiet on a Discogs target"):
            discogs_world = _CROSS_MB_TO_DISCOGS
            run = self.driver.drive(discogs_world, cross_pathway=True)
            self.assertEqual(
                case_inert_violations(run, _with_result(run, outcome="target_invalid")), [],
            )

    def test_echo_and_message_clauses_trip(self) -> None:
        """The two world-independent clauses, planted on a refusal and on
        a success."""
        refused = self._correct(_CROSS_UNFLAGGED)
        with self.subTest(clause="request_id echo"):
            bad = _with_result(refused, request_id=7)
            self.assertRegex(
                "\n".join(cross_pathway_violations(bad)),
                r"^result.request_id does not echo source 42: 7$",
            )
        with self.subTest(clause="refusal message"):
            bad = _with_result(refused, error_message=None)
            self.assertRegex(
                "\n".join(cross_pathway_violations(bad)),
                r"^refusal 'target_invalid' carries no error_message$",
            )
        replaced = self._correct(_CROSS_MB_TO_DISCOGS)
        with self.subTest(clause="request_id echo on success"):
            bad = _with_result(replaced, request_id=7)
            self.assertRegex(
                "\n".join(cross_pathway_violations(bad)),
                r"^result.request_id does not echo source 42: 7$",
            )
        with self.subTest(clause="success needs no message"):
            self.assertEqual(
                cross_pathway_violations(
                    _with_result(replaced, error_message=None)), [],
            )

    def test_unflagged_clauses_trip(self) -> None:
        run = self._correct(_CROSS_UNFLAGGED)
        with self.subTest(clause="refused"):
            bad = _with_result(run, outcome=RESULT_REPLACED, reason=None)
            self.assertRegex(
                "\n".join(cross_pathway_violations(bad)),
                r"^unflagged cross-pathway target was not refused",
            )
        with self.subTest(clause="no lookup"):
            bad = replace(run, discogs_ids=("1002",))
            self.assertRegex(
                "\n".join(cross_pathway_violations(bad)),
                r"unflagged cross-pathway target reached a mirror lookup",
            )
        with self.subTest(clause="no mutation"):
            bad = replace(run, source_after={
                **run.source_after, "status": "replaced"})
            self.assertRegex(
                "\n".join(cross_pathway_violations(bad)),
                r"unflagged cross-pathway target mutated source 42$",
            )

    def test_pre_check_collision_clauses_trip(self) -> None:
        run = self._correct(_CROSS_TARGET_COLLISION)
        with self.subTest(clause="refused"):
            bad = _with_result(run, outcome=RESULT_REPLACED, reason=None)
            self.assertRegex(
                "\n".join(cross_pathway_violations(bad)),
                r"^pre-check collision on the requested id was not refused",
            )
        with self.subTest(clause="names the holder"):
            bad = _with_result(run, current_status="wanted")
            self.assertRegex(
                "\n".join(cross_pathway_violations(bad)),
                r"^pre-check collision did not name the holder's status: "
                r"'wanted'$",
            )
        with self.subTest(clause="no mirror"):
            bad = replace(run, discogs_ids=("1002",))
            self.assertRegex(
                "\n".join(cross_pathway_violations(bad)),
                r"^pre-check collision reached the mirror",
            )
        with self.subTest(clause="no mutation"):
            bad = replace(run, source_after={
                **run.source_after, "status": "replaced"})
            self.assertRegex(
                "\n".join(cross_pathway_violations(bad)),
                r"pre-check collision mutated source 42$",
            )
        with self.subTest(clause="quiet when the holder is named"):
            self.assertEqual(cross_pathway_violations(run), [])

    def test_wrong_shape_canonical_clauses_trip(self) -> None:
        for world in (_CROSS_CANONICAL_IS_SOURCE, _CROSS_CANONICAL_ZERO):
            run = self._correct(world)
            with self.subTest(world=world.canonical_shape, clause="refused"):
                bad = _with_result(run, outcome=RESULT_REPLACED, reason=None)
                self.assertRegex(
                    "\n".join(cross_pathway_violations(bad)),
                    r"^canonical of the wrong shape was not refused",
                )
            with self.subTest(world=world.canonical_shape, clause="asked once"):
                bad = replace(run, mb_ids=(), discogs_ids=())
                self.assertRegex(
                    "\n".join(cross_pathway_violations(bad)),
                    r"^wrong-shape canonical refusal did not ask the mirror "
                    r"exactly once",
                )
            with self.subTest(world=world.canonical_shape, clause="no mutation"):
                bad = replace(run, source_after={
                    **run.source_after, "status": "replaced"})
                self.assertRegex(
                    "\n".join(cross_pathway_violations(bad)),
                    r"wrong-shape canonical refusal mutated source 42$",
                )
        with self.subTest(clause="quiet on a valid canonical"):
            self.assertEqual(
                cross_pathway_violations(self._correct(_CROSS_REDIRECT)), [],
            )

    def test_mb_target_without_group_clauses_trip(self) -> None:
        run = self._correct(_CROSS_MB_NO_GROUP)
        with self.subTest(clause="refused as no group"):
            bad = _with_result(run, outcome=RESULT_REPLACED, reason=None)
            self.assertRegex(
                "\n".join(cross_pathway_violations(bad)),
                r"^cross-pathway MB target without a release group was not "
                r"refused",
            )
        with self.subTest(clause="no mutation"):
            bad = replace(run, source_after={
                **run.source_after, "status": "replaced"})
            self.assertRegex(
                "\n".join(cross_pathway_violations(bad)),
                r"refused cross-pathway target mutated source 42$",
            )

    def test_canonical_collision_clauses_trip(self) -> None:
        run = self._correct(_CROSS_CANONICAL_COLLISION)
        with self.subTest(clause="refused"):
            bad = _with_result(run, outcome=RESULT_REPLACED, reason=None)
            self.assertRegex(
                "\n".join(cross_pathway_violations(bad)),
                r"^canonical redirect onto a held id was not refused",
            )
        with self.subTest(clause="names the holder"):
            bad = _with_result(run, current_status=None)
            self.assertRegex(
                "\n".join(cross_pathway_violations(bad)),
                r"^canonical redirect collision did not name the holder's "
                r"status: None$",
            )
        with self.subTest(clause="asked exactly once"):
            bad = replace(run, mb_ids=())
            self.assertRegex(
                "\n".join(cross_pathway_violations(bad)),
                r"^canonical redirect collision did not ask the mirror exactly "
                r"once",
            )
        with self.subTest(clause="no mutation"):
            bad = replace(run, source_after={
                **run.source_after, "status": "replaced"})
            self.assertRegex(
                "\n".join(cross_pathway_violations(bad)),
                r"canonical redirect collision mutated source 42$",
            )

    def test_flagged_clauses_trip(self) -> None:
        run = self._correct(_CROSS_MB_TO_DISCOGS)
        assert run.descendant is not None
        cases = (
            ("not replaced",
             _with_result(run, outcome="wrong_state", error_message="planted"),
             r"^flagged cross-pathway target was not replaced"),
            ("mb source consulted", replace(run, mb_ids=(OLD_MBID,)),
             r"consulted the MB source's group"),
            ("target asked twice",
             replace(run, discogs_ids=("1002", "1002")),
             r"^target mirror was not asked exactly once"),
            ("target asked for a sibling",
             replace(run, discogs_ids=("1003",)),
             r"^target mirror was not asked exactly once"),
            ("no descendant", replace(run, descendant=None),
             r"^replaced request 42 has no descendant row$"),
            ("descendant identity",
             replace(run, descendant={
                 **run.descendant, "discogs_release_id": None}),
             (r"^descendant identity is not the canonical of the operator's "
              r"target")),
            ("source not frozen",
             replace(run, source_after={
                 **run.source_after, "status": "wanted"}),
             r"^source 42 did not freeze as replaced"),
            ("source identity drift",
             replace(run, source_after={
                 **run.source_after, "mb_release_group_id": "other"}),
             r"^source 42 identity drifted on supersede"),
            ("back-link",
             replace(run, descendant={
                 **run.descendant, "replaces_request_id": 7}),
             r"^descendant of 42 does not point back"),
            ("born wanted",
             replace(run, descendant={
                 **run.descendant, "status": "imported"}),
             r"^descendant of 42 was not born wanted"),
            ("metadata on the wrong columns",
             replace(run, descendant={
                 **run.descendant, "artist_name": "New Pressing",
                 "album_title": "Pet Grief"}),
             r"^descendant metadata is not the mirror's"),
            ("tracks dropped",
             replace(run, descendant_tracks=()),
             r"^descendant tracks are not the mirror's"),
        )
        for label, bad, pattern in cases:
            with self.subTest(clause=label):
                joined = "\n".join(cross_pathway_violations(bad))
                self.assertRegex(joined, pattern)
        with self.subTest(clause="discogs source consulted"):
            run_d = self._correct(_CROSS_DISCOGS_TO_MB)
            bad = replace(run_d, discogs_ids=(OLD_DISCOGS_ID, OLD_DISCOGS_ID))
            self.assertRegex(
                "\n".join(cross_pathway_violations(bad)),
                r"consulted the Discogs source's master",
            )
        with self.subTest(clause="redirect lands at the canonical"):
            run_r = self._correct(_CROSS_REDIRECT)
            assert run_r.descendant is not None
            self.assertEqual(
                run_r.descendant["mb_release_id"], _CROSS_REDIRECT.canonical_id,
            )
            bad = replace(run_r, descendant={
                **run_r.descendant, "mb_release_id": _CROSS_REDIRECT.target_id})
            self.assertRegex(
                "\n".join(cross_pathway_violations(bad)),
                r"^descendant identity is not the canonical of the operator's "
                r"target",
            )

    def test_flag_inert_clauses_trip_and_stay_quiet(self) -> None:
        plain = self.driver.drive(_SAME_MB, cross_pathway=False)
        flagged = self.driver.drive(_SAME_MB, cross_pathway=True)
        self.assertEqual(flag_inert_violations(plain, flagged), [])
        with self.subTest(clause="outcome"):
            bad = _with_result(flagged, outcome="wrong_state")
            self.assertRegex(
                "\n".join(flag_inert_violations(plain, bad)),
                r"^cross_pathway flag changed a same-pathway outcome",
            )
        with self.subTest(clause="reason"):
            bad = _with_result(flagged, reason="unresolvable_target")
            self.assertRegex(
                "\n".join(flag_inert_violations(plain, bad)),
                r"cross_pathway flag changed a same-pathway reason",
            )
        with self.subTest(clause="quiet across pathways"):
            crossed = self._correct(_CROSS_MB_TO_DISCOGS)
            self.assertEqual(flag_inert_violations(crossed, crossed), [])


if __name__ == "__main__":
    unittest.main()
