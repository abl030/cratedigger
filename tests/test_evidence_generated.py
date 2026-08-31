"""Generated evidence-lifecycle tests — issue #548.

Property-based port of the local fuzzer that found the V0-evidence bug
fixed in ``6cf26a4`` (require source V0 for converted current evidence):
a current-evidence row representing a lossless-source transcode must never
become action-ready without its source V0 metric.

For each generated world the test builds the real on-disk + DB state — a
staged album folder, a ``FakePipelineDB`` request row, and a converted current
evidence row with no linked V0 metric — then runs the production action loader
(``ensure_current_evidence_for_action``) and asserts:

1. the stale transcode row is never accepted as ``current_status=loaded``;
2. mutating the request-row V0 stamps cannot change the action result;
3. missing linked acquisition evidence fails closed (not available).

Profiles and promotion policy: tests/_hypothesis_profiles.py and
docs/generated-testing.md. The exact minimized cases from the original
RED run are committed in tests/test_import_evidence.py; the ``@example``
pin below keeps the original failing shape replaying here forever.

Every clause of every checker here carries a known-bad world that names
that clause's own message (issue #1094, docs/generated-testing.md
§ "Per-clause proof"). Two checkers accumulate their violations instead of
raising on the first — the two-axis carry checker and the integrity
precedence checker — because the audit found clauses whose only reachable
world already trips an earlier one, which under a raise chain left them
unfalsifiable rather than satisfied.
"""

import configparser
import os
import re
import shutil
import sys
import tempfile
import unittest
import uuid
from dataclasses import dataclass
from typing import Any, Literal, cast
from unittest.mock import patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import msgspec
from hypothesis import example, given
from hypothesis import strategies as st

import tests._hypothesis_profiles  # noqa: F401  (loads the active profile)
from lib.beets_db import (
    AlbumInfo,
    CurrentBeetsItem,
    CurrentBeetsUnique,
    release_identity_for_lookup,
)
from lib.import_evidence import ensure_current_evidence_for_action
from lib.import_preview import measure_and_persist_candidate_evidence
from lib.measurement import ExistingSpectralAuditLookup
from lib.quality import (
    AlbumQualityEvidence,
    AlbumQualityV0Metric,
    AudioQualityMeasurement,
    EvidenceProvenance,
    EvidenceSubject,
    SpectralAnalysisDetail,
    VerifiedLosslessProof,
)
from lib.quality_evidence import (
    backfill_current_evidence_from_album_info,
    snapshot_audio_files,
    snapshot_fingerprint,
)
from lib.spectral_check import SPECTRAL_MEASUREMENT_VERSION
from tests.dispatch_helpers import claim_next_import_preview_job
from tests.evidence_helpers import make_album_quality_evidence
from tests.fakes import FakeBeetsDB, FakePipelineDB
from tests.helpers import make_request_row

_CHANGED_SNAPSHOT_FACT_SHAPES: tuple[
    tuple[EvidenceSubject | None, EvidenceSubject | None], ...
] = (
    ("source", "source"),
    ("installed", "installed"),
    ("installed", None),
    (None, None),
)


def _current_release(
    release_id: str,
    root: str,
    *,
    audio_format: str = "MP3",
    bitrate_kbps: int = 250,
) -> CurrentBeetsUnique:
    identity = release_identity_for_lookup(release_id)
    assert identity is not None
    filename = next(
        name for name in os.listdir(root)
        if os.path.splitext(name)[1].lower() in {".mp3", ".opus", ".flac"}
    )
    return CurrentBeetsUnique(
        identity=identity,
        album_id=1,
        album_path=root,
        items=(CurrentBeetsItem(
            id=1,
            path=os.path.join(root, filename),
            format=audio_format,
            bitrate=bitrate_kbps * 1000,
        ),),
        selectors=(f"mb_albumid:{release_id}",),
    )


@dataclass(frozen=True)
class EvidenceLifecycleWorld:
    """One stale-converted-current-evidence world."""
    extension: str            # on-disk transcode container: "opus" | "mp3"
    was_converted_from: str   # lossless source lineage: "flac"|"alac"|"wav"
    source_v0_avg: int
    source_v0_min: int
    stale_min_bitrate: int
    stale_avg_bitrate: int

    @property
    def storage_format(self) -> str:
        return "Opus" if self.extension == "opus" else "MP3"


@st.composite
def evidence_lifecycle_worlds(draw) -> EvidenceLifecycleWorld:
    avg = draw(st.integers(min_value=1, max_value=400))
    stale_min = draw(st.integers(min_value=1, max_value=400))
    return EvidenceLifecycleWorld(
        extension=draw(st.sampled_from(("opus", "mp3"))),
        was_converted_from=draw(st.sampled_from(("flac", "alac", "wav"))),
        source_v0_avg=avg,
        source_v0_min=max(avg - draw(st.integers(min_value=0, max_value=50)), 1),
        stale_min_bitrate=stale_min,
        stale_avg_bitrate=stale_min + draw(st.integers(min_value=0, max_value=50)),
    )


def assert_lifecycle_outcome(
    *,
    current_status: str | None,
    available: bool,
    result_v0_avg: int | None,
) -> None:
    """Missing linked acquisition evidence always fails closed."""
    if current_status == "loaded":
        raise AssertionError(
            "lossless-source transcode current evidence loaded without "
            "V0 metric")
    if available or result_v0_avg is not None:
        raise AssertionError(
            "request stamps resurrected a missing linked V0 fact")


def _run_world(
    world: EvidenceLifecycleWorld,
    *,
    request_has_v0_scalar: bool,
) -> tuple[str | None, bool, int | None]:
    """Build the world's on-disk + DB state and run the action loader."""
    root = tempfile.mkdtemp(prefix="cratedigger-evidence-gen-")
    try:
        audio_path = os.path.join(root, f"01 - Track.{world.extension}")
        with open(audio_path, "wb") as handle:
            handle.write(b"generated-audio")

        request_id = 1
        mbid = "evidence-generated-mbid"
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=request_id, mb_release_id=mbid))
        if request_has_v0_scalar:
            db.update_request_fields(
                request_id,
                current_spectral_grade="likely_transcode",
                current_spectral_bitrate=128,
                current_lossless_source_v0_probe_min_bitrate=world.source_v0_min,
                current_lossless_source_v0_probe_avg_bitrate=world.source_v0_avg,
                current_lossless_source_v0_probe_median_bitrate=world.source_v0_avg,
            )

        stale_current = make_album_quality_evidence(
            mb_release_id=mbid,
            files=snapshot_audio_files(root),
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=world.stale_min_bitrate,
                avg_bitrate_kbps=world.stale_avg_bitrate,
                median_bitrate_kbps=world.stale_avg_bitrate,
                format=world.storage_format,
                is_cbr=False,
                spectral_grade=None,
                spectral_bitrate_kbps=None,
                was_converted_from=world.was_converted_from,
            ),
            v0_metric=None,
            codec=world.extension,
            container=world.extension,
            storage_format=world.storage_format,
        )
        db.upsert_album_quality_evidence(stale_current)
        persisted = db.find_album_quality_evidence(
            mb_release_id=mbid,
            snapshot_fingerprint=stale_current.snapshot_fingerprint,
        )
        assert persisted is not None and persisted.id is not None
        db.set_request_current_evidence(request_id, persisted.id)

        result = ensure_current_evidence_for_action(
            db,
            request_id=request_id,
            mb_release_id=mbid,
            current_release=_current_release(
                mbid,
                root,
                audio_format=world.storage_format,
                bitrate_kbps=world.stale_avg_bitrate,
            ),
            album_info=AlbumInfo(
                album_id=1,
                track_count=1,
                min_bitrate_kbps=world.stale_min_bitrate,
                avg_bitrate_kbps=world.stale_avg_bitrate,
                median_bitrate_kbps=world.stale_avg_bitrate,
                is_cbr=False,
                album_path=root,
                format=world.storage_format,
            ),
        )
        result_v0_avg = (
            result.evidence.v0_metric.avg_bitrate_kbps
            if result.evidence is not None and result.evidence.v0_metric is not None
            else None
        )
        return result.provenance.current_status, result.available, result_v0_avg
    finally:
        shutil.rmtree(root, ignore_errors=True)


# The exact world shape of the original RED run (seed 548 case 0 of the
# pre-Hypothesis fuzzer): an opus transcode from flac with the legacy
# request scalar present. Fix 6cf26a4; exact minimized twins live in
# tests/test_import_evidence.py.
_ORIGINAL_RED_WORLD = EvidenceLifecycleWorld(
    extension="opus",
    was_converted_from="flac",
    source_v0_avg=171,
    source_v0_min=171,
    stale_min_bitrate=108,
    stale_avg_bitrate=114,
)


class TestGeneratedEvidenceLifecycle(unittest.TestCase):
    """Action-loader invariants over generated stale-current worlds."""

    @given(world=evidence_lifecycle_worlds())
    @example(world=_ORIGINAL_RED_WORLD)
    def test_request_scalar_cannot_resurrect_source_v0(self, world):
        without_scalar = _run_world(world, request_has_v0_scalar=False)
        with_scalar = _run_world(world, request_has_v0_scalar=True)
        self.assertEqual(with_scalar, without_scalar)
        current_status, available, result_v0_avg = with_scalar
        assert_lifecycle_outcome(
            current_status=current_status,
            available=available,
            result_v0_avg=result_v0_avg,
        )

    def test_changed_snapshot_retry_waits_for_surviving_or_new_facts(
        self,
    ) -> None:
        for fact_shape in _CHANGED_SNAPSHOT_FACT_SHAPES:
            with self.subTest(fact_shape=fact_shape):
                self._assert_changed_snapshot_retry(fact_shape)

    def _assert_changed_snapshot_retry(
        self,
        fact_shape: tuple[EvidenceSubject | None, EvidenceSubject | None],
    ) -> None:
        """A retry cannot turn an unenriched #743 drift rebuild into authority."""

        spectral_subject, v0_subject = fact_shape

        root = tempfile.mkdtemp(prefix="cratedigger-evidence-drift-")
        try:
            audio_path = os.path.join(root, "01 - Track.mp3")
            with open(audio_path, "wb") as handle:
                handle.write(b"before")
            db = FakePipelineDB()
            db.seed_request(make_request_row(id=1, mb_release_id="drift-mbid"))
            evidence = make_album_quality_evidence(
                mb_release_id="drift-mbid",
                source_path=root,
                files=snapshot_audio_files(root),
                measurement=AudioQualityMeasurement(
                    min_bitrate_kbps=192,
                    avg_bitrate_kbps=200,
                    median_bitrate_kbps=198,
                    format="MP3",
                    is_cbr=False,
                    spectral_grade=(
                        "genuine" if spectral_subject is not None else None
                    ),
                    spectral_subject=spectral_subject,
                    spectral_provenance=(
                        "carried"
                        if spectral_subject == "source"
                        else "measured"
                        if spectral_subject == "installed"
                        else None
                    ),
                    spectral_measurement_version=(
                        SPECTRAL_MEASUREMENT_VERSION
                        if spectral_subject is not None
                        else None
                    ),
                ),
                v0_metric=(
                    AlbumQualityV0Metric(
                        min_bitrate_kbps=190,
                        avg_bitrate_kbps=200,
                        median_bitrate_kbps=198,
                        subject=v0_subject,
                        provenance=(
                            "carried" if v0_subject == "source" else "measured"
                        ),
                    )
                    if v0_subject is not None
                    else None
                ),
            )
            db.upsert_album_quality_evidence(evidence)
            stored = db.find_album_quality_evidence(
                mb_release_id=evidence.mb_release_id,
                snapshot_fingerprint=evidence.snapshot_fingerprint,
            )
            assert stored is not None and stored.id is not None
            db.set_request_current_evidence(1, stored.id)
            with open(audio_path, "ab") as handle:
                handle.write(b"-after")
            album_info = AlbumInfo(
                album_id=1,
                track_count=1,
                min_bitrate_kbps=192,
                avg_bitrate_kbps=200,
                median_bitrate_kbps=198,
                is_cbr=False,
                album_path=root,
                format="MP3",
            )

            first = ensure_current_evidence_for_action(
                db,
                request_id=1,
                mb_release_id="drift-mbid",
                current_release=_current_release("drift-mbid", root),
                album_info=album_info,
            )
            second = ensure_current_evidence_for_action(
                db,
                request_id=1,
                mb_release_id="drift-mbid",
                current_release=_current_release("drift-mbid", root),
                album_info=album_info,
            )

            surviving_authority = (
                spectral_subject == "source" and v0_subject == "source"
            )
            self.assertEqual(first.available, surviving_authority)
            self.assertEqual(second.available, surviving_authority)
            linked_id = db.get_request_current_evidence_id(1)
            linked = db.load_album_quality_evidence_by_id(linked_id)
            assert linked is not None
            self.assertEqual(
                linked.snapshot_fingerprint,
                snapshot_fingerprint(snapshot_audio_files(root)),
            )
        finally:
            shutil.rmtree(root, ignore_errors=True)


@dataclass(frozen=True)
class BlankPathWorld:
    """One current-evidence world varying source_path recordability."""
    source_path_kind: str      # "blank" | "whitespace" | "real"
    spectral_grade: str | None
    min_bitrate: int
    avg_bitrate: int
    # Converted-from-lossless rows interact with the lossless-source-V0
    # fail-closed branch. Request stamps never rescue the missing linked fact.
    was_converted_from: str | None = None
    request_has_v0_scalar: bool = False


@st.composite
def blank_path_worlds(draw) -> BlankPathWorld:
    min_bitrate = draw(st.integers(min_value=1, max_value=400))
    return BlankPathWorld(
        source_path_kind=draw(
            st.sampled_from(("blank", "whitespace", "real"))
        ),
        spectral_grade=draw(
            st.sampled_from((None, "genuine", "likely_transcode"))
        ),
        min_bitrate=min_bitrate,
        avg_bitrate=min_bitrate + draw(st.integers(min_value=0, max_value=100)),
        was_converted_from=draw(st.sampled_from((None, "flac"))),
        request_has_v0_scalar=draw(st.booleans()),
    )


def assert_blank_path_outcome(
    *,
    source_path_kind: str,
    requires_lossless_v0: bool,
    current_status: str | None,
    available: bool,
    result_source_path: str | None,
) -> None:
    """A blank-source_path row is never authoritative for an action.

    The invariant behind download_log 37206 (French Quarter): a row whose
    recorded path is blank can never be re-verified against disk nor
    enriched with HAVE spectral, so the loader must rebuild it — never
    hand it to the decision as ``loaded``. The one legitimate non-rebuild
    outcome is the lossless-source-V0 guard: a converted row missing the linked
    acquisition fact fails closed instead (a disk rebuild would fabricate
    provenance). Request stamps cannot change that outcome. Here it only
    shapes which blank-path outcome is legal.
    """
    if source_path_kind == "real":
        if not requires_lossless_v0 and current_status != "loaded":
            raise AssertionError(
                "complete current evidence with a real source_path must "
                f"load as authoritative (got {current_status})")
        return
    if current_status == "loaded":
        raise AssertionError(
            "blank-source_path current evidence was loaded as authoritative")
    if requires_lossless_v0:
        if available:
            raise AssertionError(
                "lossless-source row without a V0 backfill source "
                "must fail closed, not become available")
        return
    if not available:
        raise AssertionError(
            "blank-source_path row must rebuild from album_info, "
            "not fail closed")
    if not (result_source_path or "").strip():
        raise AssertionError(
            "rebuilt action evidence still carries a blank source_path")


def _run_blank_path_world(
    world: BlankPathWorld,
) -> tuple[str | None, bool, str | None]:
    """Build the world's on-disk + DB state and run the action loader."""
    root = tempfile.mkdtemp(prefix="cratedigger-blankpath-gen-")
    try:
        with open(os.path.join(root, "01 - Track.mp3"), "wb") as handle:
            handle.write(b"generated-audio")

        request_id = 1
        mbid = "blank-path-generated-mbid"
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=request_id, mb_release_id=mbid))
        if world.request_has_v0_scalar:
            db.update_request_fields(
                request_id,
                current_lossless_source_v0_probe_min_bitrate=190,
                current_lossless_source_v0_probe_avg_bitrate=200,
                current_lossless_source_v0_probe_median_bitrate=200,
            )

        source_path = {
            "blank": "",
            "whitespace": "   ",
            "real": root,
        }[world.source_path_kind]
        current = make_album_quality_evidence(
            mb_release_id=mbid,
            source_path=source_path,
            files=snapshot_audio_files(root),
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=world.min_bitrate,
                avg_bitrate_kbps=world.avg_bitrate,
                median_bitrate_kbps=world.avg_bitrate,
                format="MP3",
                is_cbr=False,
                spectral_grade=world.spectral_grade,
                spectral_bitrate_kbps=(
                    96 if world.spectral_grade == "likely_transcode" else None
                ),
                was_converted_from=world.was_converted_from,
                spectral_subject=(
                    "source"
                    if world.spectral_grade is not None
                    and (world.was_converted_from or "").lower()
                    in {"flac", "alac", "wav"}
                    else None
                ),
                spectral_provenance=(
                    "carried"
                    if world.spectral_grade is not None
                    and (world.was_converted_from or "").lower()
                    in {"flac", "alac", "wav"}
                    else None
                ),
                spectral_measurement_version=(
                    SPECTRAL_MEASUREMENT_VERSION
                    if world.spectral_grade is not None
                    else None
                ),
            ),
            v0_metric=None,
        )
        db.upsert_album_quality_evidence(current)
        persisted = db.find_album_quality_evidence(
            mb_release_id=mbid,
            snapshot_fingerprint=current.snapshot_fingerprint,
        )
        assert persisted is not None and persisted.id is not None
        db.set_request_current_evidence(request_id, persisted.id)

        result = ensure_current_evidence_for_action(
            db,
            request_id=request_id,
            mb_release_id=mbid,
            current_release=_current_release(
                mbid,
                root,
                bitrate_kbps=world.avg_bitrate,
            ),
            album_info=AlbumInfo(
                album_id=1,
                track_count=1,
                min_bitrate_kbps=world.min_bitrate,
                avg_bitrate_kbps=world.avg_bitrate,
                median_bitrate_kbps=world.avg_bitrate,
                is_cbr=False,
                album_path=root,
                format="MP3",
            ),
        )
        return (
            result.provenance.current_status,
            result.available,
            result.evidence.source_path if result.evidence is not None else None,
        )
    finally:
        shutil.rmtree(root, ignore_errors=True)


# The exact French Quarter shape (download_log 37206): a 2026-05-16
# library-backfill row with min 186 / avg 194 and no spectral, whose blank
# source_path kept every HAVE enrichment guard refusing forever.
_FRENCH_QUARTER_WORLD = BlankPathWorld(
    source_path_kind="blank",
    spectral_grade=None,
    min_bitrate=186,
    avg_bitrate=194,
)

# The fail-closed seam: a blank-path row that is ALSO converted-from-
# lossless with no linked source V0 fact must fail closed, not rebuild.
_BLANK_LOSSLESS_NO_SCALAR_WORLD = BlankPathWorld(
    source_path_kind="blank",
    spectral_grade=None,
    min_bitrate=108,
    avg_bitrate=114,
    was_converted_from="flac",
    request_has_v0_scalar=False,
)

# The must-still-work arm, pinned by the issue #1094 per-clause audit: a
# complete real-path row that is not converted-from-lossless is the only
# world in which "must load as authoritative" can fire. It ran 21 times in
# the 152-example suite tier; the pin keeps that decisive world reachable
# no matter how the strategy is later reshaped.
_REAL_PATH_COMPLETE_WORLD = BlankPathWorld(
    source_path_kind="real",
    spectral_grade=None,
    min_bitrate=186,
    avg_bitrate=194,
    was_converted_from=None,
    request_has_v0_scalar=False,
)


class TestGeneratedBlankSourcePath(unittest.TestCase):
    """Action-loader invariants over generated source_path worlds."""

    @given(world=blank_path_worlds())
    @example(world=_FRENCH_QUARTER_WORLD)
    @example(world=_BLANK_LOSSLESS_NO_SCALAR_WORLD)
    @example(world=_REAL_PATH_COMPLETE_WORLD)
    def test_blank_source_path_is_never_authoritative(self, world):
        current_status, available, result_source_path = (
            _run_blank_path_world(world)
        )
        assert_blank_path_outcome(
            source_path_kind=world.source_path_kind,
            requires_lossless_v0=world.was_converted_from is not None,
            current_status=current_status,
            available=available,
            result_source_path=result_source_path,
        )


class TestBlankPathCheckerTripsOnViolations(unittest.TestCase):
    """Per-clause known-bad worlds for the blank-path checker (issue #1094).

    Each world makes exactly one clause's condition true while every earlier
    clause passes, and names that clause's own message.
    """

    # (description, source_path_kind, requires_lossless_v0, current_status,
    #  available, result_source_path, message)
    CASES: tuple[
        tuple[str, str, bool, str, bool, str | None, str], ...
    ] = (
        (
            "real path that did not load",
            "real", False, "backfilled", True, "/library/album",
            ("complete current evidence with a real source_path must load as "
             "authoritative (got backfilled)"),
        ),
        (
            "blank path loaded as authority",
            "blank", False, "loaded", False, "/library/album",
            "blank-source_path current evidence was loaded as authoritative",
        ),
        (
            "lossless-source row became available",
            "blank", True, "backfilled", True, "/library/album",
            ("lossless-source row without a V0 backfill source must fail "
             "closed, not become available"),
        ),
        (
            "blank path failed closed instead of rebuilding",
            "blank", False, "failed", False, None,
            ("blank-source_path row must rebuild from album_info, not fail "
             "closed"),
        ),
        (
            "rebuilt row still carries a blank path",
            "whitespace", False, "backfilled", True, "   ",
            "rebuilt action evidence still carries a blank source_path",
        ),
    )

    def test_every_clause_trips_with_its_own_message(self):
        for (
            description, kind, requires_v0, status, available, path, message,
        ) in self.CASES:
            with self.subTest(description=description), self.assertRaisesRegex(
                AssertionError, re.escape(message),
            ):
                assert_blank_path_outcome(
                    source_path_kind=kind,
                    requires_lossless_v0=requires_v0,
                    current_status=status,
                    available=available,
                    result_source_path=path,
                )

    def test_complete_real_path_world_is_accepted(self):
        """The must-still-work control: no clause fires on a legal world."""
        assert_blank_path_outcome(
            source_path_kind="real", requires_lossless_v0=False,
            current_status="loaded", available=True,
            result_source_path="/library/album")
        assert_blank_path_outcome(
            source_path_kind="blank", requires_lossless_v0=False,
            current_status="backfilled", available=True,
            result_source_path="/library/album")


LosslessSpectralFailureKind = Literal[
    "absent",
    "not_attempted",
    "error",
    "grade_none",
    "grade_error",
]


def assert_lossless_spectral_failure_lifecycle(
    *,
    request_status: str,
    expected_request_status: str,
    job_status: str,
    preview_status: str | None,
    harness_calls: int,
) -> None:
    """Unusable lossless spectral evidence always fails before the harness."""

    if request_status != expected_request_status:
        raise AssertionError(
            "lossless spectral failure changed the force-import request from "
            f"{expected_request_status!r} to {request_status!r}"
        )
    if job_status != "failed" or preview_status != "measurement_failed":
        raise AssertionError(
            "lossless spectral failure did not terminate the preview job"
        )
    if harness_calls:
        raise AssertionError("harness ran without usable lossless spectral evidence")


def integrity_precedence_violations(
    *,
    job_status: str,
    preview_status: str | None,
    decision: str | None,
    harness_calls: int,
    candidate_evidence_id: int | None,
) -> list[str]:
    """Accumulate every integrity-precedence violation of one preview run.

    Accumulating rather than raising (issue #1094 per-clause audit): leaving
    the measurement-only path is the only way production can reach the
    harness, and doing so also moves the decision and the job status — so
    under a raise chain the harness and link clauses could never witness
    anything the first clause had not already caught.
    """
    violations: list[str] = []
    if job_status != "queued" or preview_status != "evidence_ready":
        violations.append("audio corruption was demoted to measurement failure")
    if decision != "audio_corrupt":
        violations.append("audio corruption did not win decision precedence")
    if harness_calls:
        violations.append("harness ran after completed audio-corrupt evidence")
    if candidate_evidence_id is None:
        violations.append("audio-corrupt candidate evidence was not linked")
    return violations


def assert_integrity_fact_precedes_spectral_failure(
    *,
    job_status: str,
    preview_status: str | None,
    decision: str | None,
    harness_calls: int,
    candidate_evidence_id: int | None,
) -> None:
    """Completed corruption evidence must not become measurement_failed."""

    violations = integrity_precedence_violations(
        job_status=job_status,
        preview_status=preview_status,
        decision=decision,
        harness_calls=harness_calls,
        candidate_evidence_id=candidate_evidence_id,
    )
    if violations:
        raise AssertionError("; ".join(violations))


def _lossless_spectral_detail(
    kind: LosslessSpectralFailureKind,
) -> SpectralAnalysisDetail | None:
    if kind == "absent":
        return None
    if kind == "not_attempted":
        return SpectralAnalysisDetail(attempted=False)
    if kind == "error":
        return SpectralAnalysisDetail(
            attempted=True,
            error="RuntimeError: generated analyzer failure",
        )
    if kind == "grade_none":
        return SpectralAnalysisDetail(attempted=True, grade=None)
    return SpectralAnalysisDetail(attempted=True, grade="error")


def _run_lossless_spectral_failure_world(
    kind: LosslessSpectralFailureKind,
    *,
    audio_corrupt: bool = False,
) -> tuple[str, str, str | None, int, str | None, int | None]:
    from lib.config import CratediggerConfig
    from lib.import_queue import (
        IMPORT_JOB_FORCE,
        force_import_dedupe_key,
        force_import_payload,
    )
    from scripts import import_preview_worker

    root = tempfile.mkdtemp(
        prefix="cratedigger-lossless-spectral-gen-",
    )
    try:
        staging_dir = os.path.join(root, "Incoming")
        source = os.path.join(
            staging_dir,
            "failed_imports",
            "album",
        )
        os.makedirs(source)
        slskd_dir = os.path.join(root, "slskd")
        os.makedirs(slskd_dir)
        processing_dir = os.path.join(root, "processing")
        os.makedirs(processing_dir, mode=0o700)
        os.makedirs(os.path.join(processing_dir, "preview"), mode=0o700)
        with open(os.path.join(source, "01.flac"), "wb") as handle:
            handle.write(b"generated-lossless-audio")

        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=71,
            status="downloading",
            mb_release_id="generated-lossless-mbid",
        ))
        log_id = db.log_download(
            71,
            outcome="rejected",
            validation_result={"failed_path": source},
        )
        db.enqueue_import_job(
            IMPORT_JOB_FORCE,
            request_id=71,
            dedupe_key=force_import_dedupe_key(log_id),
            payload=force_import_payload(
                download_log_id=log_id,
                failed_path=source,
                source_username="generated-peer",
            ),
        )
        claimed = claim_next_import_preview_job(db, worker_id="preview")
        assert claimed is not None

        harness_calls = 0

        def run_import(**_kwargs: Any):
            nonlocal harness_calls
            harness_calls += 1
            raise AssertionError("harness must not run")

        detail = _lossless_spectral_detail(kind)

        def analyzer(_path: str) -> SpectralAnalysisDetail:
            return cast(SpectralAnalysisDetail, detail)

        def preview(db_arg, _job):
            return measure_and_persist_candidate_evidence(
                db_arg,
                request_id=71,
                path=source,
                force=True,
                download_log_id=log_id,
                import_job_id=claimed.id,
                run_import_fn=run_import,
                spectral_detail_analyzer=analyzer,
                existing_spectral_resolver=(
                    lambda _release_id: ExistingSpectralAuditLookup()
                ),
                runtime_config=cfg,
            )

        ini = configparser.ConfigParser()
        ini["Beets Validation"] = {
            "harness_path": "/fake/harness/run_beets_harness.sh",
            "audio_check": "normal" if audio_corrupt else "off",
            "staging_dir": staging_dir,
        }
        ini["Slskd"] = {"download_dir": slskd_dir}
        ini["Paths"] = {"processing_dir": processing_dir}
        ini["Pipeline DB"] = {"enabled": "true"}
        cfg = CratediggerConfig.from_ini(ini)
        fake_beets = FakeBeetsDB()
        from lib.quality import (
            AudioToolDiagnostic,
            AudioValidationReport,
            skipped_audio_validation_report,
        )
        from lib.util import AudioValidationResult

        audio_result = AudioValidationResult(
            (
                AudioValidationReport(
                    outcome="audio_corrupt",
                    files_checked=1,
                    files_failed=1,
                    diagnostics=[
                        AudioToolDiagnostic(
                            relative_path="01.flac",
                            category="decode_error",
                            return_code=69,
                            stderr_excerpt=(
                                "Invalid data found when processing input"
                            ),
                        ),
                    ],
                )
                if audio_corrupt
                else skipped_audio_validation_report()
            ),
            failed_paths=("01.flac",) if audio_corrupt else (),
        )
        with patch(
            "lib.beets_db.BeetsDB",
            lambda **_kwargs: fake_beets,
        ), patch(
            "lib.measurement.validate_audio",
            return_value=audio_result,
        ):
            updated = import_preview_worker.process_claimed_preview_job(
                db,
                claimed,
                preview_fn=preview,
                runtime_config=cfg,
            )
        assert updated is not None
        preview_result = updated.preview_result or {}
        return (
            str(db.request(71)["status"]),
            updated.status,
            updated.preview_status,
            harness_calls,
            cast(str | None, preview_result.get("decision")),
            db.get_import_job_candidate_evidence_id(claimed.id),
        )
    finally:
        shutil.rmtree(root, ignore_errors=True)


class TestGeneratedLosslessSpectralFailureLifecycle(unittest.TestCase):
    def test_unusable_lossless_spectral_never_reaches_harness(self):
        kinds = (
            "absent",
            "not_attempted",
            "error",
            "grade_none",
            "grade_error",
        )
        for kind in kinds:
            with self.subTest(kind=kind):
                (
                    request_status,
                    job_status,
                    preview_status,
                    harness_calls,
                    _decision,
                    _candidate_evidence_id,
                ) = _run_lossless_spectral_failure_world(kind)
                assert_lossless_spectral_failure_lifecycle(
                    request_status=request_status,
                    expected_request_status="downloading",
                    job_status=job_status,
                    preview_status=preview_status,
                    harness_calls=harness_calls,
                )

    def test_audio_corrupt_wins_over_every_spectral_failure_shape(self):
        kinds = (
            "absent",
            "not_attempted",
            "error",
            "grade_none",
            "grade_error",
        )
        for kind in kinds:
            with self.subTest(kind=kind):
                (
                    _request_status,
                    job_status,
                    preview_status,
                    harness_calls,
                    decision,
                    candidate_evidence_id,
                ) = _run_lossless_spectral_failure_world(
                    kind,
                    audio_corrupt=True,
                )
                assert_integrity_fact_precedes_spectral_failure(
                    job_status=job_status,
                    preview_status=preview_status,
                    decision=decision,
                    harness_calls=harness_calls,
                    candidate_evidence_id=candidate_evidence_id,
                )


class TestLosslessSpectralFailureCheckerTripsOnViolations(unittest.TestCase):
    """Per-clause known-bad worlds for both preview checkers (issue #1094)."""

    # (description, request_status, expected_request_status, job_status,
    #  preview_status, harness_calls, message)
    LIFECYCLE_CASES: tuple[
        tuple[str, str, str, str, str | None, int, str], ...
    ] = (
        (
            "failed preview moved the force request",
            "imported", "downloading", "failed", "measurement_failed", 0,
            ("lossless spectral failure changed the force-import request from "
             "'downloading' to 'imported'"),
        ),
        (
            "job did not fail",
            "downloading", "downloading", "queued", "measurement_failed", 0,
            "lossless spectral failure did not terminate the preview job",
        ),
        (
            "preview did not record measurement_failed",
            "downloading", "downloading", "failed", "evidence_ready", 0,
            "lossless spectral failure did not terminate the preview job",
        ),
        (
            "harness ran anyway",
            "downloading", "downloading", "failed", "measurement_failed", 1,
            "harness ran without usable lossless spectral evidence",
        ),
    )

    # (description, job_status, preview_status, decision, harness_calls,
    #  candidate_evidence_id, message)
    INTEGRITY_CASES: tuple[
        tuple[str, str, str | None, str | None, int, int | None, str], ...
    ] = (
        (
            "corruption demoted to a failed job",
            "failed", "measurement_failed", "audio_corrupt", 0, 7,
            "audio corruption was demoted to measurement failure",
        ),
        (
            "corruption demoted to a non-ready preview status",
            "queued", "measurement_failed", "audio_corrupt", 0, 7,
            "audio corruption was demoted to measurement failure",
        ),
        (
            "another fact won decision precedence",
            "queued", "evidence_ready", "bad_audio_hash", 0, 7,
            "audio corruption did not win decision precedence",
        ),
        (
            "harness ran on completed corruption evidence",
            "queued", "evidence_ready", "audio_corrupt", 1, 7,
            "harness ran after completed audio-corrupt evidence",
        ),
        (
            "candidate evidence was not linked to the job",
            "queued", "evidence_ready", "audio_corrupt", 0, None,
            "audio-corrupt candidate evidence was not linked",
        ),
    )

    def test_every_lifecycle_clause_trips_with_its_own_message(self):
        for (
            description, request_status, expected_status, job_status,
            preview_status, harness_calls, message,
        ) in self.LIFECYCLE_CASES:
            with self.subTest(description=description), self.assertRaisesRegex(
                AssertionError, re.escape(message),
            ):
                assert_lossless_spectral_failure_lifecycle(
                    request_status=request_status,
                    expected_request_status=expected_status,
                    job_status=job_status,
                    preview_status=preview_status,
                    harness_calls=harness_calls,
                )

    def test_every_integrity_clause_trips_with_its_own_message(self):
        for (
            description, job_status, preview_status, decision, harness_calls,
            candidate_evidence_id, message,
        ) in self.INTEGRITY_CASES:
            with self.subTest(description=description), self.assertRaisesRegex(
                AssertionError, re.escape(message),
            ):
                assert_integrity_fact_precedes_spectral_failure(
                    job_status=job_status,
                    preview_status=preview_status,
                    decision=decision,
                    harness_calls=harness_calls,
                    candidate_evidence_id=candidate_evidence_id,
                )

    def test_legal_worlds_are_accepted(self):
        """The must-still-work control for both preview checkers."""
        assert_lossless_spectral_failure_lifecycle(
            request_status="downloading",
            expected_request_status="downloading",
            job_status="failed",
            preview_status="measurement_failed",
            harness_calls=0,
        )
        assert_integrity_fact_precedes_spectral_failure(
            job_status="queued",
            preview_status="evidence_ready",
            decision="audio_corrupt",
            harness_calls=0,
            candidate_evidence_id=7,
        )

class TestLifecycleCheckerTripsOnViolations(unittest.TestCase):
    """Per-clause known-bad worlds for the lifecycle checker (issue #1094)."""

    # (description, current_status, available, result_v0_avg, message)
    CASES: tuple[tuple[str, str, bool, int | None, str], ...] = (
        (
            "loaded without the linked V0 metric",
            "loaded", False, None,
            ("lossless-source transcode current evidence loaded without V0 "
             "metric"),
        ),
        (
            "available despite the missing linked fact",
            "rebuilt", True, None,
            "request stamps resurrected a missing linked V0 fact",
        ),
        (
            "request scalar resurrected as an evidence V0 average",
            "rebuilt", False, 171,
            "request stamps resurrected a missing linked V0 fact",
        ),
    )

    def test_every_clause_trips_with_its_own_message(self):
        for description, status, available, v0_avg, message in self.CASES:
            with self.subTest(description=description), self.assertRaisesRegex(
                AssertionError, re.escape(message),
            ):
                assert_lifecycle_outcome(
                    current_status=status,
                    available=available,
                    result_v0_avg=v0_avg,
                )

    def test_fail_closed_world_is_accepted(self):
        """The must-still-work control: the legal fail-closed outcome passes."""
        assert_lifecycle_outcome(
            current_status="failed", available=False, result_v0_avg=None)


def fingerprint_flip_two_axis_violations(
    *,
    original_subject: str,
    evidence: AlbumQualityEvidence,
) -> list[str]:
    """Accumulate every two-axis carry violation of one rebuilt row.

    Accumulating rather than raising (issue #1094 per-clause audit): under
    the original raise chain the two cross-product clauses had no world of
    their own. ``installed V0 fact cannot be carried`` could only arrive
    behind ``installed V0 fact crossed fingerprints`` or
    ``source V0 fact was not marked carried``, both of which raised first,
    so the clause was unfalsifiable rather than satisfied.
    """
    violations: list[str] = []
    measurement = evidence.measurement
    if original_subject == "source":
        if measurement.spectral_grade is None:
            violations.append("source spectral fact was dropped")
        elif (
            measurement.spectral_subject,
            measurement.spectral_provenance,
        ) != ("source", "carried"):
            violations.append("source spectral fact was not marked carried")
        if evidence.v0_metric is None:
            violations.append("source V0 fact was dropped")
        elif (
            evidence.v0_metric.subject,
            evidence.v0_metric.provenance,
        ) != ("source", "carried"):
            violations.append("source V0 fact was not marked carried")
    else:
        if measurement.spectral_grade is not None:
            violations.append("installed spectral fact crossed fingerprints")
        if evidence.v0_metric is not None:
            violations.append("installed V0 fact crossed fingerprints")

    if (
        measurement.spectral_subject == "installed"
        and measurement.spectral_provenance == "carried"
    ):
        violations.append("installed spectral fact cannot be carried")
    if (
        evidence.v0_metric is not None
        and evidence.v0_metric.subject == "installed"
        and evidence.v0_metric.provenance == "carried"
    ):
        violations.append("installed V0 fact cannot be carried")
    if evidence.verified_lossless_proof is None:
        violations.append("verified-lossless proof was dropped")
    elif evidence.verified_lossless_proof.provenance != "carried":
        violations.append("verified-lossless proof was not marked carried")
    return violations


def assert_fingerprint_flip_two_axis_carry(
    *,
    original_subject: str,
    evidence: AlbumQualityEvidence,
) -> None:
    """Only source facts survive a content fingerprint change.

    A changed fingerprint means the installed files are a new subject. Source
    facts remain meaningful but become carried; installed facts must be
    measured again from the new files rather than copied from the old row.
    """
    violations = fingerprint_flip_two_axis_violations(
        original_subject=original_subject,
        evidence=evidence,
    )
    if violations:
        raise AssertionError("; ".join(violations))


def _run_fingerprint_flip_world(
    subject: Literal["source", "installed"],
) -> AlbumQualityEvidence:
    root = tempfile.mkdtemp(prefix="cratedigger-two-axis-gen-")
    try:
        audio_path = os.path.join(root, "01 - Track.mp3")
        with open(audio_path, "wb") as handle:
            handle.write(b"original-audio")

        request_id = 1
        mbid = "two-axis-generated-mbid"
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=request_id, mb_release_id=mbid))
        current = make_album_quality_evidence(
            mb_release_id=mbid,
            source_path=root,
            files=snapshot_audio_files(root),
            lineage_version=3,
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=128,
                avg_bitrate_kbps=130,
                median_bitrate_kbps=129,
                format="MP3",
                spectral_grade="genuine",
                spectral_subject=subject,
                spectral_provenance="measured",
            ),
            v0_metric=AlbumQualityV0Metric(
                subject=subject,
                provenance="measured",
                avg_bitrate_kbps=245,
            ),
            verified_lossless_proof=VerifiedLosslessProof(
                provenance="measured",
                source="flac",
                classifier="spectral_verified_lossless",
            ),
        )
        db.upsert_album_quality_evidence(current)
        persisted = db.find_album_quality_evidence(
            mb_release_id=mbid,
            snapshot_fingerprint=current.snapshot_fingerprint,
        )
        assert persisted is not None and persisted.id is not None
        db.set_request_current_evidence(request_id, persisted.id)

        with open(audio_path, "ab") as handle:
            handle.write(b"-changed")
        result = backfill_current_evidence_from_album_info(
            db,
            request_id=request_id,
            mb_release_id=mbid,
            album_info=AlbumInfo(
                album_id=1,
                track_count=1,
                min_bitrate_kbps=190,
                avg_bitrate_kbps=196,
                median_bitrate_kbps=195,
                is_cbr=False,
                album_path=root,
                format="MP3",
            ),
        )
        assert result.evidence is not None
        return result.evidence
    finally:
        shutil.rmtree(root, ignore_errors=True)


class TestGeneratedTwoAxisFingerprintCarry(unittest.TestCase):
    def test_fingerprint_flip_carries_only_source_facts(self):
        subjects: tuple[EvidenceSubject, ...] = ("source", "installed")
        for subject in subjects:
            with self.subTest(subject=subject):
                evidence = _run_fingerprint_flip_world(subject)
                assert_fingerprint_flip_two_axis_carry(
                    original_subject=subject,
                    evidence=evidence,
                )


def _carried_proof(
    provenance: EvidenceProvenance = "carried",
) -> VerifiedLosslessProof:
    return VerifiedLosslessProof(
        provenance=provenance,
        source="flac",
        classifier="spectral_verified_lossless",
    )


def _two_axis_evidence(
    *,
    spectral_grade: str | None = "genuine",
    spectral_subject: EvidenceSubject | None = "source",
    spectral_provenance: EvidenceProvenance | None = "carried",
    v0_metric: AlbumQualityV0Metric | None = None,
    proof_provenance: EvidenceProvenance | None = "carried",
) -> AlbumQualityEvidence:
    """A rebuilt row with exactly the two-axis facts a known-bad world needs.

    ``proof_provenance=None`` drops the verified-lossless proof entirely.
    """
    evidence = make_album_quality_evidence(
        lineage_version=3,
        measurement=AudioQualityMeasurement(
            spectral_grade=spectral_grade,
            spectral_subject=spectral_subject,
            spectral_provenance=spectral_provenance,
        ),
        v0_metric=v0_metric,
        verified_lossless_proof=(
            _carried_proof(proof_provenance)
            if proof_provenance is not None
            else None
        ),
    )
    # The builder normalises a grade with no subject; these worlds need the
    # exact stored tuple, including shapes production's own validator refuses.
    return msgspec.structs.replace(
        evidence,
        measurement=msgspec.structs.replace(
            evidence.measurement,
            spectral_grade=spectral_grade,
            spectral_subject=spectral_subject,
            spectral_provenance=spectral_provenance,
        ),
    )


class TestTwoAxisCarryCheckerTripsOnViolations(unittest.TestCase):
    """Per-clause known-bad worlds for the two-axis checker (issue #1094).

    Two clauses — the installed/carried cross-product pair — can only ever
    arrive alongside another violation, which is why the checker accumulates
    rather than raising on the first hit. Their worlds name their own message
    out of the accumulated report.
    """

    SOURCE_V0 = AlbumQualityV0Metric(
        subject="source", provenance="carried", avg_bitrate_kbps=245,
    )

    def _cases(self) -> tuple[tuple[str, str, AlbumQualityEvidence, str], ...]:
        return (
            (
                "source spectral grade dropped",
                "source",
                _two_axis_evidence(
                    spectral_grade=None, spectral_subject=None,
                    spectral_provenance=None, v0_metric=self.SOURCE_V0,
                ),
                "source spectral fact was dropped",
            ),
            (
                "source spectral still marked measured",
                "source",
                _two_axis_evidence(
                    spectral_provenance="measured", v0_metric=self.SOURCE_V0,
                ),
                "source spectral fact was not marked carried",
            ),
            (
                "source V0 dropped",
                "source",
                _two_axis_evidence(v0_metric=None),
                "source V0 fact was dropped",
            ),
            (
                "source V0 still marked measured",
                "source",
                _two_axis_evidence(
                    v0_metric=AlbumQualityV0Metric(
                        subject="source", provenance="measured",
                        avg_bitrate_kbps=245,
                    ),
                ),
                "source V0 fact was not marked carried",
            ),
            (
                "installed spectral survived the flip",
                "installed",
                _two_axis_evidence(
                    spectral_subject="installed",
                    spectral_provenance="measured",
                ),
                "installed spectral fact crossed fingerprints",
            ),
            (
                "installed V0 survived the flip",
                "installed",
                _two_axis_evidence(
                    spectral_grade=None, spectral_subject=None,
                    spectral_provenance=None,
                    v0_metric=AlbumQualityV0Metric(
                        subject="installed", provenance="measured",
                        avg_bitrate_kbps=245,
                    ),
                ),
                "installed V0 fact crossed fingerprints",
            ),
            (
                "installed spectral markers stamped carried",
                "installed",
                _two_axis_evidence(
                    spectral_grade=None, spectral_subject="installed",
                    spectral_provenance="carried",
                ),
                "installed spectral fact cannot be carried",
            ),
            (
                "installed V0 stamped carried",
                "installed",
                _two_axis_evidence(
                    spectral_grade=None, spectral_subject=None,
                    spectral_provenance=None,
                    v0_metric=AlbumQualityV0Metric(
                        subject="installed", provenance="carried",
                        avg_bitrate_kbps=245,
                    ),
                ),
                "installed V0 fact cannot be carried",
            ),
            (
                "verified-lossless proof dropped",
                "source",
                _two_axis_evidence(
                    v0_metric=self.SOURCE_V0, proof_provenance=None,
                ),
                "verified-lossless proof was dropped",
            ),
            (
                "verified-lossless proof still marked measured",
                "source",
                _two_axis_evidence(
                    v0_metric=self.SOURCE_V0, proof_provenance="measured",
                ),
                "verified-lossless proof was not marked carried",
            ),
        )

    def test_every_clause_trips_with_its_own_message(self):
        for description, subject, evidence, message in self._cases():
            with self.subTest(description=description), self.assertRaisesRegex(
                AssertionError, re.escape(message),
            ):
                assert_fingerprint_flip_two_axis_carry(
                    original_subject=subject,
                    evidence=evidence,
                )

    def test_legal_carried_rebuild_is_accepted(self):
        """The must-still-work control: both legal rebuilt shapes pass."""
        assert_fingerprint_flip_two_axis_carry(
            original_subject="source",
            evidence=_two_axis_evidence(v0_metric=self.SOURCE_V0),
        )
        assert_fingerprint_flip_two_axis_carry(
            original_subject="installed",
            evidence=_two_axis_evidence(
                spectral_grade=None, spectral_subject=None,
                spectral_provenance=None,
            ),
        )


_POISONED_LINK_IDENTITIES = (
    (
        "11111111-1111-1111-1111-111111111111",
        "22222222-2222-2222-2222-222222222222",
    ),
    ("12345", "54321"),
    ("legacy-release-a", "legacy-release-b"),
)


@st.composite
def poisoned_link_identity_pairs(
    draw: st.DrawFn,
) -> tuple[str, str]:
    """Distinct exact identities across every stored identity shape."""
    kinds = ("uuid", "numeric", "legacy")
    requested_kind = draw(st.sampled_from(kinds))
    poisoned_kind = draw(st.sampled_from(kinds))
    requested_ordinal = draw(st.integers(min_value=1, max_value=2**63))
    poisoned_ordinal = requested_ordinal + draw(
        st.integers(min_value=1, max_value=2**31),
    )

    def render(kind: str, ordinal: int) -> str:
        if kind == "uuid":
            value = str(uuid.UUID(int=ordinal))
            if draw(st.booleans()):
                value = value.upper()
        elif kind == "numeric":
            value = ("0" * draw(st.integers(min_value=0, max_value=4))) + str(
                ordinal,
            )
        else:
            value = f"legacy-release-{ordinal}"
        whitespace = st.sampled_from(("", " ", "\t", "\n"))
        return f"{draw(whitespace)}{value}{draw(whitespace)}"

    return (
        render(requested_kind, requested_ordinal),
        render(poisoned_kind, poisoned_ordinal),
    )


def _single_fact_evidence(
    *,
    spectral_grade: str | None = None,
    spectral_bitrate_kbps: int | None = None,
    spectral_subject: EvidenceSubject | None = None,
    spectral_provenance: EvidenceProvenance | None = None,
    v0_metric: AlbumQualityV0Metric | None = None,
    verified_lossless_proof: VerifiedLosslessProof | None = None,
    on_disk_v0_research_attempted: bool = False,
) -> AlbumQualityEvidence:
    """A rebuilt row carrying exactly one linked HAVE fact, or none."""
    evidence = make_album_quality_evidence(
        measurement=AudioQualityMeasurement(
            min_bitrate_kbps=128, format="MP3",
        ),
        v0_metric=v0_metric,
        verified_lossless_proof=verified_lossless_proof,
        on_disk_v0_research_attempted=on_disk_v0_research_attempted,
    )
    return msgspec.structs.replace(
        evidence,
        measurement=msgspec.structs.replace(
            evidence.measurement,
            spectral_grade=spectral_grade,
            spectral_bitrate_kbps=spectral_bitrate_kbps,
            spectral_subject=spectral_subject,
            spectral_provenance=spectral_provenance,
        ),
    )


def poisoned_link_facts(evidence: AlbumQualityEvidence) -> list[str]:
    """Name every HAVE fact that survived a mismatched exact identity.

    Naming the facts (issue #1094) keeps the seven disjuncts individually
    provable: one shared message could only ever witness whichever fact the
    known-bad world happened to set.
    """
    measurement = evidence.measurement
    crossed = (
        ("spectral_grade", measurement.spectral_grade is not None),
        ("spectral_bitrate_kbps", measurement.spectral_bitrate_kbps is not None),
        ("spectral_subject", measurement.spectral_subject is not None),
        ("spectral_provenance", measurement.spectral_provenance is not None),
        ("v0_metric", evidence.v0_metric is not None),
        ("verified_lossless_proof", evidence.verified_lossless_proof is not None),
        ("on_disk_v0_research_attempted", evidence.on_disk_v0_research_attempted),
    )
    return [name for name, present in crossed if present]


def assert_no_poisoned_link_facts(evidence: AlbumQualityEvidence) -> None:
    facts = poisoned_link_facts(evidence)
    if facts:
        raise AssertionError(
            "poisoned linked HAVE facts crossed exact identity: "
            + ", ".join(facts))


class TestGeneratedPoisonedCurrentLink(unittest.TestCase):
    @given(identities=poisoned_link_identity_pairs())
    @example(identities=_POISONED_LINK_IDENTITIES[0])
    @example(identities=_POISONED_LINK_IDENTITIES[1])
    @example(identities=_POISONED_LINK_IDENTITIES[2])
    @example(identities=(
        " 11111111-1111-1111-1111-111111111111 ",
        "\t00054321\n",
    ))
    def test_mismatched_exact_identity_carries_no_linked_facts(self, identities):
        requested, poisoned = identities
        root = tempfile.mkdtemp(prefix="cratedigger-poisoned-link-gen-")
        try:
            with open(os.path.join(root, "01.mp3"), "wb") as handle:
                handle.write(b"same-address-bytes")
            db = FakePipelineDB()
            db.seed_request(make_request_row(
                id=1,
                mb_release_id=requested,
                status="imported",
            ))
            linked = make_album_quality_evidence(
                mb_release_id=poisoned,
                files=snapshot_audio_files(root),
                measurement=AudioQualityMeasurement(
                    min_bitrate_kbps=245,
                    format="MP3",
                    spectral_grade="genuine",
                    spectral_bitrate_kbps=228,
                    spectral_subject="source",
                    spectral_provenance="measured",
                ),
                v0_metric=AlbumQualityV0Metric(
                    subject="source",
                    provenance="measured",
                    avg_bitrate_kbps=245,
                ),
                verified_lossless_proof=VerifiedLosslessProof(
                    provenance="measured",
                    source="flac",
                    classifier="spectral_verified_lossless",
                ),
                on_disk_v0_research_attempted=True,
            )
            db.upsert_album_quality_evidence(linked)
            stored = db.find_album_quality_evidence(
                mb_release_id=poisoned,
                snapshot_fingerprint=linked.snapshot_fingerprint,
            )
            assert stored is not None and stored.id is not None
            db.set_request_current_evidence(1, stored.id)
            result = backfill_current_evidence_from_album_info(
                db,
                request_id=1,
                mb_release_id=requested,
                album_info=AlbumInfo(
                    album_id=1,
                    track_count=1,
                    min_bitrate_kbps=128,
                    is_cbr=False,
                    album_path=root,
                    format="MP3",
                ),
            )
            assert result.evidence is not None
            assert_no_poisoned_link_facts(result.evidence)
        finally:
            shutil.rmtree(root, ignore_errors=True)

    def test_every_carried_fact_trips_the_checker_by_name(self):
        """One known-bad world per fact the poisoned-link clause enumerates."""
        cases: tuple[tuple[str, AlbumQualityEvidence], ...] = (
            (
                "spectral_grade",
                _single_fact_evidence(spectral_grade="genuine"),
            ),
            (
                "spectral_bitrate_kbps",
                _single_fact_evidence(spectral_bitrate_kbps=228),
            ),
            (
                "spectral_subject",
                _single_fact_evidence(spectral_subject="source"),
            ),
            (
                "spectral_provenance",
                _single_fact_evidence(spectral_provenance="measured"),
            ),
            (
                "v0_metric",
                _single_fact_evidence(v0_metric=AlbumQualityV0Metric(
                    subject="source", provenance="measured",
                    avg_bitrate_kbps=245,
                )),
            ),
            (
                "verified_lossless_proof",
                _single_fact_evidence(
                    verified_lossless_proof=_carried_proof("measured"),
                ),
            ),
            (
                "on_disk_v0_research_attempted",
                _single_fact_evidence(on_disk_v0_research_attempted=True),
            ),
        )
        for fact, known_bad in cases:
            with self.subTest(fact=fact):
                self.assertEqual(poisoned_link_facts(known_bad), [fact])
                with self.assertRaisesRegex(
                    AssertionError,
                    re.escape(
                        "poisoned linked HAVE facts crossed exact identity: "
                        f"{fact}"),
                ):
                    assert_no_poisoned_link_facts(known_bad)

    def test_fact_free_rebuild_is_accepted(self):
        """The must-still-work control: a fact-free rebuild carries nothing."""
        assert_no_poisoned_link_facts(_single_fact_evidence())

# ---------------------------------------------------------------------------
# 2026-07-18 proof-mint incident (Passenger / request 8877) — two invariants:
#
# 1. Proof minting is total over its input space: it never raises, mints a
#    proof exactly when the attempt is verified lossless, and the minted
#    source is a normalised non-empty token.
# 2. A crashed harness result (decision="crash") NEVER becomes candidate
#    evidence, however complete its partial measurements look — the live
#    crash fired one line after source_measurement was set, so the partial
#    result persisted proof-less and the proof lock silently never engaged.
# ---------------------------------------------------------------------------


def assert_minted_proof_consistent(
    will_be: bool,
    was_converted_from: Any,
    detected_source_format: Any,
    spectral_grade: Any,
    proof: Any,
) -> None:
    """Checker: mint output obeys the proof-construction contract.

    Every clause carries its own message (issue #1094) so a known-bad world
    proves the clause it is named for rather than whichever one happens to
    evaluate first.
    """
    if not will_be:
        assert proof is None, "unverified attempt must not mint a proof"
        return
    assert proof is not None, "verified attempt must mint a proof"
    assert proof.provenance == "measured", (
        f"minted proof provenance must be measured: {proof.provenance!r}")
    assert proof.classifier == "spectral_verified_lossless", (
        f"minted proof classifier must be the base name: {proof.classifier!r}")
    assert proof.source, "minted proof source must be non-empty"
    assert proof.source == proof.source.strip().lower(), (
        f"minted proof source must be normalised: {proof.source!r}")
    assert proof.detail == spectral_grade, (
        f"minted proof detail must be the spectral grade: {proof.detail!r}")


def assert_crashed_result_never_persists(
    decision: Any,
    build_result: Any,
) -> None:
    """Checker: decision='crash' never yields buildable evidence."""
    if decision == "crash":
        assert build_result.evidence is None, (
            "a crashed ImportResult must never become candidate evidence"
        )
        assert build_result.status == "crashed_result", (
            "a crashed ImportResult must report status crashed_result: "
            f"{build_result.status!r}")


_filetype_token = st.one_of(
    st.none(),
    st.sampled_from(["flac", "FLAC", "alac", "wav", "m4a", "UNKNOWN", "  "]),
    st.text(max_size=8),
)

_grade_token = st.sampled_from(
    [None, "genuine", "marginal", "suspect", "likely_transcode", "error"]
)


class TestGeneratedProofMint(unittest.TestCase):
    @given(
        will_be=st.booleans(),
        was_converted_from=_filetype_token,
        detected=_filetype_token,
        grade=_grade_token,
    )
    @example(  # the live Passenger world that crashed on args.filetype
        will_be=True, was_converted_from="flac", detected="FLAC",
        grade="genuine",
    )
    @example(  # #1094: the only world where the source fallback decides
        will_be=True, was_converted_from=None, detected="UNKNOWN",
        grade="genuine",
    )
    @example(  # #1094: the only world where source normalisation decides
        will_be=True, was_converted_from="  FLAC ", detected=None,
        grade="marginal",
    )
    def test_mint_is_total_and_consistent(
        self, will_be, was_converted_from, detected, grade,
    ):
        from lib.quality import mint_verified_lossless_proof

        proof = mint_verified_lossless_proof(
            will_be,
            was_converted_from=was_converted_from,
            detected_source_format=detected,
            spectral_grade=grade,
        )
        assert_minted_proof_consistent(
            will_be, was_converted_from, detected, grade, proof)


class TestGeneratedCrashedResultPersistGate(unittest.TestCase):
    @given(
        decision=st.sampled_from(
            ["crash", "import", "reject", "conversion_failed", None]
        ),
        with_measurement=st.booleans(),
        error=st.one_of(st.none(), st.text(max_size=40)),
    )
    @example(  # the live 2026-07-18 shape
        decision="crash", with_measurement=True,
        error="AttributeError: 'Namespace' object has no attribute 'filetype'",
    )
    def test_crashed_results_never_build_evidence(
        self, decision, with_measurement, error,
    ):
        from lib.quality import AlbumQualityEvidenceFile as EvidenceFile
        from lib.quality import ImportResult
        from lib.quality_evidence import evidence_from_import_result

        measurement = (
            AudioQualityMeasurement(
                min_bitrate_kbps=767, avg_bitrate_kbps=851,
                median_bitrate_kbps=847, format="FLAC",
                spectral_grade="genuine", spectral_subject="source",
                spectral_provenance="measured",
            )
            if with_measurement else None
        )
        result = evidence_from_import_result(
            mb_release_id="mbid-crash-gate",
            source_path="/nonexistent/crash-gate",
            import_result=ImportResult(
                decision=decision,
                error=error,
                source_measurement=measurement,
            ),
            files=[
                EvidenceFile(
                    relative_path="01.mp3", size_bytes=47, mtime_ns=1,
                    extension="mp3", container="mp3", codec="mp3",
                )
            ],
        )
        assert_crashed_result_never_persists(decision, result)


@dataclass(frozen=True)
class EvidenceBuildResultForTest:
    """Planted stand-in for the known-bad checker self-test only."""

    evidence: Any
    status: str


def _planted_proof(
    *,
    provenance: EvidenceProvenance = "measured",
    source: str = "flac",
    classifier: str = "spectral_verified_lossless",
    detail: str | None = "genuine",
) -> VerifiedLosslessProof:
    return VerifiedLosslessProof(
        provenance=provenance, source=source,
        classifier=classifier, detail=detail,
    )


class TestProofMintCheckersTripOnViolations(unittest.TestCase):
    """Per-clause known-bad worlds for both mint checkers (issue #1094)."""

    # (description, will_be_verified_lossless, planted proof, message)
    MINT_CASES: tuple[
        tuple[str, bool, VerifiedLosslessProof | None, str], ...
    ] = (
        (
            "phantom proof on an unverified attempt",
            False, _planted_proof(),
            "unverified attempt must not mint a proof",
        ),
        (
            "no proof on a verified attempt",
            True, None,
            "verified attempt must mint a proof",
        ),
        (
            "proof minted as carried",
            True, _planted_proof(provenance="carried"),
            "minted proof provenance must be measured: 'carried'",
        ),
        (
            "proof minted with a leg classifier no leg adjudicated",
            True, _planted_proof(classifier="spectral_verified_lossless_v3"),
            ("minted proof classifier must be the base name: "
             "'spectral_verified_lossless_v3'"),
        ),
        (
            "proof minted with an empty source",
            True, _planted_proof(source=""),
            "minted proof source must be non-empty",
        ),
        (
            "proof minted with an unnormalised source",
            True, _planted_proof(source="FLAC "),
            "minted proof source must be normalised: 'FLAC '",
        ),
        (
            "proof detail is not the measured grade",
            True, _planted_proof(detail=None),
            "minted proof detail must be the spectral grade: None",
        ),
    )

    # (description, planted build result, message) — decision is always "crash"
    CRASH_CASES: tuple[
        tuple[str, EvidenceBuildResultForTest, str], ...
    ] = (
        (
            "crashed result built evidence",
            EvidenceBuildResultForTest(
                evidence=object(), status="crashed_result"),
            "a crashed ImportResult must never become candidate evidence",
        ),
        (
            "crashed result reported another status",
            EvidenceBuildResultForTest(evidence=None, status="incomplete"),
            ("a crashed ImportResult must report status crashed_result: "
             "'incomplete'"),
        ),
    )

    def test_every_mint_clause_trips_with_its_own_message(self):
        for description, will_be, proof, message in self.MINT_CASES:
            with self.subTest(description=description), self.assertRaisesRegex(
                AssertionError, re.escape(message),
            ):
                assert_minted_proof_consistent(
                    will_be, "flac", "FLAC", "genuine", proof,
                )

    def test_every_crash_clause_trips_with_its_own_message(self):
        for description, build_result, message in self.CRASH_CASES:
            with self.subTest(description=description), self.assertRaisesRegex(
                AssertionError, re.escape(message),
            ):
                assert_crashed_result_never_persists(
                    "crash", build_result,
                )

    def test_legal_mint_and_crash_worlds_are_accepted(self):
        """The must-still-work control for both mint-side checkers."""
        assert_minted_proof_consistent(
            True, "flac", "FLAC", "genuine", _planted_proof())
        assert_minted_proof_consistent(False, "flac", "FLAC", "genuine", None)
        assert_crashed_result_never_persists(
            "crash",
            EvidenceBuildResultForTest(evidence=None, status="crashed_result"),
        )
        assert_crashed_result_never_persists(
            "import", EvidenceBuildResultForTest(
                evidence=object(), status="ready"),
        )


if __name__ == "__main__":
    unittest.main()
