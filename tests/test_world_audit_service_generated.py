"""Generated contracts for the public world-audit seams: resolver-outage
handling, and (issue #1089 review n10) the real Beets/library-root
containment adapter chain."""

from __future__ import annotations

import os
import sqlite3
import tempfile
import unittest
from dataclasses import dataclass, replace
from typing import Literal
from unittest.mock import patch

from hypothesis import example, given
from hypothesis import strategies as st

import tests._hypothesis_profiles  # noqa: F401  (loads active profile)
from lib.beets_db import BeetsDB, BeetsWorldAlbum, CurrentBeetsResolution
from lib.release_identity import ReleaseIdentity
from lib.world_audit_service import (
    WORLD_AUDIT_EXIT_CODES,
    WORLD_AUDIT_HTTP_STATUS,
    WorldAuditReport,
    audit_world,
    audit_world_from_borrowed_factory,
    audit_world_from_factory,
    world_audit_outcome,
)
from lib.world_invariants import (
    LibraryAlbumSnapshot,
    check_library_root_containment,
)
from tests.fakes import FakeBeetsDB, FakePipelineDB
from tests.helpers import make_request_row
from tests.test_world_audit_service import _create_beets_db

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

    # Issue #1355 item 4: an incomplete/beets-unavailable report must be a
    # non-successful CLI exit and HTTP status, not the proxy `complete`
    # field alone — drive the exact derivation
    # `cmd_audit_world`/`get_world_audit` use and assert the DECIDED
    # values, matching what an operator or cron actually observes.
    outcome = world_audit_outcome(report)
    exit_code = (
        1 if outcome == "integrity_failed" else WORLD_AUDIT_EXIT_CODES[outcome]
    )
    if exit_code != 5:
        raise AssertionError(
            f"availability failure CLI exit code drifted: {exit_code}"
        )
    status_code = (
        200 if outcome == "integrity_failed" else WORLD_AUDIT_HTTP_STATUS[outcome]
    )
    if status_code != 503:
        raise AssertionError(
            f"availability failure HTTP status drifted: {status_code}"
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

    def test_checker_rejects_a_wrong_availability_cli_exit_code(self) -> None:
        """Issue #1355 item 4: proves the new decided-exit-code clause
        actually trips, rather than being an unfalsifiable extra line."""
        world = _ResolverFailureWorld(
            owned=True,
            kind="availability",
            sqlite_code=sqlite3.SQLITE_BUSY,
            sqlite_exception="operational",
        )
        observation = _observe(world)

        with (
            patch.dict(WORLD_AUDIT_EXIT_CODES, {"beets_unavailable": 0}),
            self.assertRaisesRegex(AssertionError, "CLI exit code drifted"),
        ):
            assert_resolver_failure_contract(world, observation)

    def test_checker_rejects_a_wrong_availability_http_status(self) -> None:
        """Issue #1355 item 4: proves the new decided-HTTP-status clause
        actually trips, rather than being an unfalsifiable extra line."""
        world = _ResolverFailureWorld(
            owned=True,
            kind="availability",
            sqlite_code=sqlite3.SQLITE_BUSY,
            sqlite_exception="operational",
        )
        observation = _observe(world)

        with (
            patch.dict(WORLD_AUDIT_HTTP_STATUS, {"beets_unavailable": 200}),
            self.assertRaisesRegex(AssertionError, "HTTP status drifted"),
        ):
            assert_resolver_failure_contract(world, observation)

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


_PATH_SEGMENT = st.text(
    alphabet=st.characters(
        whitelist_categories=("Ll", "Lu", "Nd"),
        whitelist_characters=("-", "_", " "),
    ),
    min_size=1,
    max_size=16,
).filter(lambda segment: segment.strip(" ") not in ("", ".", ".."))


@dataclass(frozen=True)
class _LibraryRootWorld:
    folder_inside: bool
    item1_inside: bool
    folder_segment: str
    item1_segment: str


@st.composite
def _library_root_worlds(draw: st.DrawFn) -> _LibraryRootWorld:
    return _LibraryRootWorld(
        folder_inside=draw(st.booleans()),
        item1_inside=draw(st.booleans()),
        folder_segment=draw(_PATH_SEGMENT),
        item1_segment=draw(_PATH_SEGMENT),
    )


class TestWorldAuditLibraryRootContainmentGenerated(unittest.TestCase):
    """Issue #1089 review n10.

    ``tests/test_world_invariants_generated.py`` already patrols the pure
    ``check_library_root_containment`` checker in isolation. Per the "agree
    by construction" rule (V4 in ``tests/test_verdict_tiers_generated.py``),
    a mutant at that pure-function adapter does not qualify the REAL
    ``audit_world`` + real ``BeetsDB`` SQLite adapter chain the production
    world-audit CLI/API actually drives — this property exercises that
    chain directly, over the same two bucket-C clauses
    (``album_folder_outside_library_root`` / ``album_item_outside_library_root``),
    and asserts it classifies each generated world IDENTICALLY to the pure
    checker fed the equivalent snapshot — the composition, not either half
    alone, is what this property proves.
    """

    @given(world=_library_root_worlds())
    @example(world=_LibraryRootWorld(
        folder_inside=False, item1_inside=False,
        folder_segment="frozen-ghost", item1_segment="frozen-ghost-2",
    ))
    @example(world=_LibraryRootWorld(
        folder_inside=True, item1_inside=False,
        folder_segment="partially-moved", item1_segment="escaped",
    ))
    @example(world=_LibraryRootWorld(
        folder_inside=True, item1_inside=True,
        folder_segment="installed", item1_segment="installed",
    ))
    def test_real_audit_agrees_with_the_pure_checker(
        self,
        world: _LibraryRootWorld,
    ) -> None:
        mb_release_id = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
        with tempfile.TemporaryDirectory() as root:
            library_root = os.path.join(root, "library")
            os.makedirs(library_root)
            outside_root = os.path.join(root, "processing", "albums")

            folder_root = library_root if world.folder_inside else outside_root
            item1_root = library_root if world.item1_inside else outside_root
            item0_path = os.path.join(
                folder_root, world.folder_segment, "01 Track.flac",
            )
            item1_path = os.path.join(
                item1_root, world.item1_segment, "02 Track.flac",
            )

            db_path = os.path.join(root, "beets.db")
            _create_beets_db(db_path)
            _insert_two_item_album(
                db_path,
                album_id=1,
                mb_release_id=mb_release_id,
                item_paths=(item0_path, item1_path),
            )

            snapshot = LibraryAlbumSnapshot(
                album_id=1,
                release_id=mb_release_id,
                album_path=os.path.dirname(item0_path),
                item_paths=(item0_path, item1_path),
            )
            expected = {
                violation.code
                for violation in check_library_root_containment(
                    (snapshot,), library_root=library_root,
                )
            }

            with BeetsDB(db_path, library_root=library_root) as beets:
                report = audit_world(FakePipelineDB(), beets)

        actual = {
            member.code
            for group in (report.groups.a, report.groups.b, report.groups.c)
            for member in group.members
            if member.code in (
                "album_folder_outside_library_root",
                "album_item_outside_library_root",
            )
        }

        self.assertEqual(actual, expected)


def _insert_two_item_album(
    db_path: str,
    *,
    album_id: int,
    mb_release_id: str,
    item_paths: tuple[str, str],
) -> None:
    """Local sibling of ``tests.test_world_audit_service._insert_album``:
    that helper inserts exactly one item per album, which cannot represent
    the partially-moved world (album folder inside the root, one item
    already escaped it) this property needs."""
    conn = sqlite3.connect(db_path)
    conn.execute(
        "INSERT INTO albums (id, mb_albumid, discogs_albumid) VALUES (?, ?, NULL)",
        (album_id, mb_release_id),
    )
    for index, item_path in enumerate(item_paths, start=1):
        conn.execute(
            "INSERT INTO items "
            "(id, album_id, path, title, track, disc, length, format, "
            "bitrate, samplerate, bitdepth) VALUES "
            "(?, ?, ?, 'Track', ?, 1, 180.0, 'MP3', 256000, 44100, 16)",
            (album_id * 100 + index, album_id, item_path, index),
        )
    conn.commit()
    conn.close()


if __name__ == "__main__":
    unittest.main()
