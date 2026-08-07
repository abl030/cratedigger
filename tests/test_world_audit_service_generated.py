"""Generated resolver-outage contracts for the public world-audit seams."""

from __future__ import annotations

import sqlite3
import unittest
from dataclasses import dataclass, replace
from typing import Literal

from hypothesis import example, given
from hypothesis import strategies as st

import tests._hypothesis_profiles  # noqa: F401  (loads active profile)
from lib.beets_db import (
    BeetsWorldAlbum,
    CurrentBeetsMissing,
    CurrentBeetsResolution,
)
from lib.release_identity import ReleaseIdentity
from lib.world_audit_service import (
    WorldAuditCounts,
    WorldAuditReport,
    audit_world,
    audit_world_from_borrowed_factory,
    audit_world_from_factory,
    build_world_audit_report,
)
from tests.fakes import FakeBeetsDB, FakePipelineDB
from tests.helpers import make_request_row

_RELEASE_ID = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
_EXPECTED_IDENTITY = ReleaseIdentity(
    source="musicbrainz",
    release_id=_RELEASE_ID,
)
_AVAILABILITY_PRIMARY_CODES = (
    sqlite3.SQLITE_AUTH,
    sqlite3.SQLITE_BUSY,
    sqlite3.SQLITE_CANTOPEN,
    sqlite3.SQLITE_IOERR,
    sqlite3.SQLITE_LOCKED,
    sqlite3.SQLITE_PERM,
)


@dataclass(frozen=True)
class _ResolverFailureWorld:
    owned: bool
    kind: Literal["availability", "sqlite_error", "runtime", "value"]
    sqlite_code: int | None
    sqlite_exception: Literal["database", "operational"] | None

    @property
    def admitted(self) -> bool:
        return self.kind == "availability"


@dataclass(frozen=True)
class _AuditObservation:
    report: WorldAuditReport | None
    raised: Exception | None
    expected_failure: Exception
    factory_calls: int
    list_world_albums_calls: int
    resolve_current_releases_calls: tuple[tuple[ReleaseIdentity, ...], ...]
    close_calls: int


class _ResolverFailureBeets(FakeBeetsDB):
    def __init__(self, failure: Exception) -> None:
        super().__init__()
        self._failure = failure
        self.list_world_albums_calls = 0
        self.resolve_current_releases_calls: list[
            tuple[ReleaseIdentity, ...]
        ] = []

    def list_world_albums(self) -> list[BeetsWorldAlbum]:
        self.list_world_albums_calls += 1
        return super().list_world_albums()

    def resolve_current_releases(
        self,
        identities: list[ReleaseIdentity],
    ) -> dict[ReleaseIdentity, CurrentBeetsResolution]:
        self.resolve_current_releases_calls.append(tuple(identities))
        raise self._failure


@st.composite
def _resolver_failure_worlds(draw: st.DrawFn) -> _ResolverFailureWorld:
    owned = draw(st.booleans())
    kind = draw(st.sampled_from((
        "availability",
        "sqlite_error",
        "runtime",
        "value",
    )))
    if kind == "availability":
        primary = draw(st.sampled_from(_AVAILABILITY_PRIMARY_CODES))
        extension = draw(st.integers(min_value=0, max_value=33))
        sqlite_code = primary | (extension << 8)
        sqlite_exception = draw(st.sampled_from(("database", "operational")))
    elif kind == "sqlite_error":
        extension = draw(st.integers(min_value=0, max_value=33))
        sqlite_code = sqlite3.SQLITE_ERROR | (extension << 8)
        sqlite_exception = draw(st.sampled_from(("database", "operational")))
    else:
        sqlite_code = None
        sqlite_exception = None
    return _ResolverFailureWorld(
        owned=owned,
        kind=kind,
        sqlite_code=sqlite_code,
        sqlite_exception=sqlite_exception,
    )


def _failure_for(world: _ResolverFailureWorld) -> Exception:
    if world.sqlite_code is not None:
        error_type = (
            sqlite3.DatabaseError
            if world.sqlite_exception == "database"
            else sqlite3.OperationalError
        )
        failure = error_type(
            f"generated resolver sqlite failure {world.sqlite_code}"
        )
        failure.sqlite_errorcode = world.sqlite_code
        return failure
    if world.kind == "runtime":
        return RuntimeError("generated resolver runtime failure")
    return ValueError("generated resolver value failure")


def _pipeline_db_with_exact_request() -> FakePipelineDB:
    pipeline_db = FakePipelineDB()
    pipeline_db.seed_request(make_request_row(
        id=1,
        mb_release_id=_RELEASE_ID,
        discogs_release_id=None,
        status="imported",
    ))
    return pipeline_db


def _observe(world: _ResolverFailureWorld) -> _AuditObservation:
    failure = _failure_for(world)
    beets = _ResolverFailureBeets(failure)
    factory_calls = 0

    def factory() -> _ResolverFailureBeets:
        nonlocal factory_calls
        factory_calls += 1
        return beets

    report: WorldAuditReport | None = None
    raised: Exception | None = None
    try:
        if world.owned:
            report = audit_world_from_factory(
                _pipeline_db_with_exact_request(),
                factory,
            )
        else:
            report = audit_world_from_borrowed_factory(
                _pipeline_db_with_exact_request(),
                factory,
            )
    except Exception as exc:  # noqa: BLE001 - propagation is the contract
        raised = exc
    return _AuditObservation(
        report=report,
        raised=raised,
        expected_failure=failure,
        factory_calls=factory_calls,
        list_world_albums_calls=beets.list_world_albums_calls,
        resolve_current_releases_calls=tuple(
            beets.resolve_current_releases_calls
        ),
        close_calls=beets.close_calls,
    )


def assert_resolver_failure_contract(
    world: _ResolverFailureWorld,
    observation: _AuditObservation,
) -> None:
    """Check outage typing, exact resolver reachability, and handle ownership."""

    expected_close_calls = 1 if world.owned else 0
    if observation.factory_calls != 1:
        raise AssertionError(
            f"resolver audit opened {observation.factory_calls} handles"
        )
    if observation.list_world_albums_calls != 1:
        raise AssertionError(
            "resolver audit did not pass exactly once through list_world_albums"
        )
    if observation.resolve_current_releases_calls != ((_EXPECTED_IDENTITY,),):
        raise AssertionError(
            "resolver audit did not reach the exact seeded request identity: "
            f"{observation.resolve_current_releases_calls!r}"
        )
    if observation.close_calls != expected_close_calls:
        raise AssertionError(
            "world-audit handle lifecycle drifted: "
            f"owned={world.owned}, close_calls={observation.close_calls}"
        )

    if not world.admitted:
        if observation.report is not None:
            raise AssertionError("unexpected resolver failure became a report")
        if observation.raised is not observation.expected_failure:
            raise AssertionError("unexpected resolver failure did not propagate")
        return

    if observation.raised is not None:
        raise AssertionError(
            f"availability failure escaped: {observation.raised!r}"
        )
    report = observation.report
    if report is None:
        raise AssertionError("availability failure produced no audit report")
    if report.complete:
        raise AssertionError("availability failure was reported complete")
    if report.status != "observations_only":
        raise AssertionError(
            f"availability failure status drifted: {report.status!r}"
        )
    grouped_codes = (
        tuple(member.code for member in report.groups.a.members),
        tuple(member.code for member in report.groups.b.members),
        tuple(member.code for member in report.groups.c.members),
    )
    expected_grouped_codes = (
        (),
        ("current_beets_authority_unavailable",),
        (),
    )
    if grouped_codes != expected_grouped_codes:
        raise AssertionError(
            "availability failure was not the exact Bucket B observation: "
            f"{grouped_codes!r}"
        )
    if (
        report.counts.bucket_a,
        report.counts.bucket_b,
        report.counts.bucket_c,
    ) != (0, 1, 0):
        raise AssertionError(
            f"availability failure bucket counts drifted: {report.counts!r}"
        )
    assert world.sqlite_code is not None
    expected_detail = (
        "current Beets authority unavailable "
        f"(sqlite_{world.sqlite_code & 0xFF})"
    )
    if report.groups.b.members[0].detail != expected_detail:
        raise AssertionError(
            "extended SQLite result was not classified by its primary code: "
            f"{report.groups.b.members[0].detail!r}"
        )


def assert_duplicate_acquisition_membership(
    report: WorldAuditReport,
    *,
    present_request_id: int,
    missing_request_id: int,
) -> None:
    """Only the request whose own union is missing may report missing."""
    missing_members = tuple(
        member.request_id
        for member in report.groups.b.members
        if member.code == "current_beets_missing"
    )
    if missing_members != (missing_request_id,):
        raise AssertionError(
            "duplicate acquisition membership was rekeyed by release id: "
            f"present={present_request_id}, missing={missing_request_id}, "
            f"observed={missing_members}"
        )


class TestWorldAuditResolverFailureGenerated(unittest.TestCase):
    @given(world=_resolver_failure_worlds())
    @example(world=_ResolverFailureWorld(
        owned=True,
        kind="availability",
        sqlite_code=sqlite3.SQLITE_IOERR_READ,
        sqlite_exception="operational",
    ))
    @example(world=_ResolverFailureWorld(
        owned=False,
        kind="availability",
        sqlite_code=sqlite3.SQLITE_AUTH,
        sqlite_exception="database",
    ))
    @example(world=_ResolverFailureWorld(
        owned=False,
        kind="sqlite_error",
        sqlite_code=sqlite3.SQLITE_ERROR,
        sqlite_exception="operational",
    ))
    def test_resolver_failures_are_typed_without_lifecycle_drift(
        self,
        world: _ResolverFailureWorld,
    ) -> None:
        assert_resolver_failure_contract(world, _observe(world))

    def test_checker_rejects_a_complete_outage_report(self) -> None:
        world = _ResolverFailureWorld(
            owned=True,
            kind="availability",
            sqlite_code=sqlite3.SQLITE_BUSY_RECOVERY,
            sqlite_exception="operational",
        )
        observation = _observe(world)
        assert observation.report is not None
        known_bad = replace(
            observation,
            report=WorldAuditReport(
                status=observation.report.status,
                complete=True,
                counts=observation.report.counts,
                audited_invariants=observation.report.audited_invariants,
                temporal_invariants_not_auditable=(
                    observation.report.temporal_invariants_not_auditable
                ),
                groups=observation.report.groups,
            ),
        )

        with self.assertRaisesRegex(AssertionError, "reported complete"):
            assert_resolver_failure_contract(world, known_bad)

    def test_checker_rejects_operational_error_only_auth_classifier(self) -> None:
        world = _ResolverFailureWorld(
            owned=False,
            kind="availability",
            sqlite_code=sqlite3.SQLITE_AUTH,
            sqlite_exception="database",
        )
        failure = _failure_for(world)
        known_bad = _AuditObservation(
            report=None,
            raised=failure,
            expected_failure=failure,
            factory_calls=1,
            list_world_albums_calls=1,
            resolve_current_releases_calls=((_EXPECTED_IDENTITY,),),
            close_calls=0,
        )

        with self.assertRaisesRegex(AssertionError, "availability failure escaped"):
            assert_resolver_failure_contract(world, known_bad)


class TestWorldAuditDuplicateAcquisitionGenerated(unittest.TestCase):
    """The public audit keeps duplicated acquisition rows distinct by id."""

    @given(
        request_ids=st.lists(
            st.integers(min_value=1, max_value=2_000_000_000),
            min_size=2,
            max_size=2,
            unique=True,
        ),
        survivor_first=st.booleans(),
    )
    @example(request_ids=[11, 12], survivor_first=True)
    def test_duplicate_acquisition_union_answers_do_not_collide(
        self,
        request_ids: list[int],
        survivor_first: bool,
    ) -> None:
        acquisition = _RELEASE_ID
        survivor = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
        present_request_id, missing_request_id = (
            (request_ids[0], request_ids[1])
            if survivor_first else (request_ids[1], request_ids[0])
        )
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=present_request_id,
            mb_release_id=acquisition,
            canonical_release_id=survivor,
            status="imported",
        ))
        db._requests[missing_request_id] = make_request_row(
            id=missing_request_id,
            mb_release_id=acquisition,
            status="imported",
        )
        beets = FakeBeetsDB()
        beets.set_album_ids_for_release(survivor, [77])
        beets.set_world_albums([BeetsWorldAlbum(
            album_id=77,
            release_ids=(survivor,),
            album_path="",
            item_paths=(),
        )])

        report = audit_world(db, beets)

        assert_duplicate_acquisition_membership(
            report,
            present_request_id=present_request_id,
            missing_request_id=missing_request_id,
        )

    def test_checker_rejects_the_legacy_release_keyed_mutant(self) -> None:
        from lib.world_invariants import (
            RequestMembershipSnapshot,
            check_status_membership,
        )

        requests = (
            RequestMembershipSnapshot(11, _RELEASE_ID, "imported"),
            RequestMembershipSnapshot(12, _RELEASE_ID, "imported"),
        )
        legacy_by_release = {
            _RELEASE_ID: CurrentBeetsMissing(identity=_EXPECTED_IDENTITY),
        }
        release_keyed = check_status_membership(
            requests,
            {
                request.request_id: legacy_by_release[request.release_id]
                for request in requests
            },
        )
        known_bad = build_world_audit_report(
            counts=WorldAuditCounts(2, 1, 0, 0),
            violations=release_keyed,
        )

        with self.assertRaisesRegex(AssertionError, "rekeyed by release id"):
            assert_duplicate_acquisition_membership(
                known_bad,
                present_request_id=11,
                missing_request_id=12,
            )


if __name__ == "__main__":
    unittest.main()
