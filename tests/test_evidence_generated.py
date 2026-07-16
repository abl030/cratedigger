#!/usr/bin/env python3
"""Generated evidence-lifecycle tests — issue #548.

Property-based port of the local fuzzer that found the V0-evidence bug
fixed in ``6cf26a4`` (require source V0 for converted current evidence):
a current-evidence row representing a lossless-source transcode must never
become action-ready without its source V0 metric.

For each generated world the test builds the real on-disk + DB state — a
staged album folder, a ``FakePipelineDB`` request row (optionally carrying
the legacy scalar V0 fields), and a stale converted current-evidence row
with no V0 metric — then runs the production action loader
(``ensure_current_evidence_for_action``) and asserts:

1. the stale transcode row is never accepted as ``current_status=loaded``;
2. when the request carries legacy scalar V0 state, it is backfilled into
   the action evidence (available, with the scalar's avg);
3. when no backfill source exists, the loader fails closed (not available).

Profiles and promotion policy: tests/_hypothesis_profiles.py and
docs/generated-testing.md. The exact minimized cases from the original
RED run are committed in tests/test_import_evidence.py; the ``@example``
pin below keeps the original failing shape replaying here forever.
"""

import os
import shutil
import sys
import tempfile
import unittest
from dataclasses import dataclass

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import tests._hypothesis_profiles  # noqa: F401  (loads the active profile)

from hypothesis import example, given
from hypothesis import strategies as st

from lib.beets_db import AlbumInfo
from lib.import_evidence import ensure_current_evidence_for_action
from lib.quality import AudioQualityMeasurement
from lib.quality_evidence import snapshot_audio_files
from tests.fakes import FakePipelineDB
from tests.helpers import make_album_quality_evidence, make_request_row


@dataclass(frozen=True)
class EvidenceLifecycleWorld:
    """One stale-converted-current-evidence world."""
    extension: str            # on-disk transcode container: "opus" | "mp3"
    was_converted_from: str   # lossless source lineage: "flac"|"alac"|"wav"
    request_has_v0_scalar: bool
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
        request_has_v0_scalar=draw(st.booleans()),
        source_v0_avg=avg,
        source_v0_min=max(avg - draw(st.integers(min_value=0, max_value=50)), 1),
        stale_min_bitrate=stale_min,
        stale_avg_bitrate=stale_min + draw(st.integers(min_value=0, max_value=50)),
    )


def assert_lifecycle_outcome(
    *,
    request_has_v0_scalar: bool,
    source_v0_avg: int,
    current_status: str | None,
    available: bool,
    result_v0_avg: int | None,
) -> None:
    """The action-loader invariant behind fix 6cf26a4."""
    if current_status == "loaded":
        raise AssertionError(
            "lossless-source transcode current evidence loaded without "
            "V0 metric")
    if request_has_v0_scalar:
        if not available or result_v0_avg != source_v0_avg:
            raise AssertionError(
                "legacy request V0 scalar was not backfilled into action "
                f"evidence (available={available}, "
                f"v0_avg={result_v0_avg}, expected={source_v0_avg})")
    elif available:
        raise AssertionError(
            "missing essential V0 metric should fail closed when no "
            "backfill source exists")


def _run_world(world: EvidenceLifecycleWorld) -> tuple[str | None, bool, int | None]:
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
        if world.request_has_v0_scalar:
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
                verified_lossless=False,
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
            current_album_path=root,
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
    request_has_v0_scalar=True,
    source_v0_avg=171,
    source_v0_min=171,
    stale_min_bitrate=108,
    stale_avg_bitrate=114,
)


class TestGeneratedEvidenceLifecycle(unittest.TestCase):
    """Action-loader invariants over generated stale-current worlds."""

    @given(world=evidence_lifecycle_worlds())
    @example(world=_ORIGINAL_RED_WORLD)
    def test_converted_current_requires_source_v0(self, world):
        current_status, available, result_v0_avg = _run_world(world)
        assert_lifecycle_outcome(
            request_has_v0_scalar=world.request_has_v0_scalar,
            source_v0_avg=world.source_v0_avg,
            current_status=current_status,
            available=available,
            result_v0_avg=result_v0_avg,
        )


@dataclass(frozen=True)
class BlankPathWorld:
    """One current-evidence world varying source_path recordability."""
    source_path_kind: str      # "blank" | "whitespace" | "real"
    spectral_grade: str | None
    min_bitrate: int
    avg_bitrate: int
    # Converted-from-lossless rows interact with the lossless-source-V0
    # fail-closed branch: without a request V0 scalar to resurrect, the
    # action loader must fail closed rather than rebuild (a disk rebuild
    # would lose the lossless-source provenance).
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
    has_v0_backfill_source: bool,
    current_status: str | None,
    available: bool,
    result_source_path: str | None,
) -> None:
    """A blank-source_path row is never authoritative for an action.

    The invariant behind download_log 37206 (French Quarter): a row whose
    recorded path is blank can never be re-verified against disk nor
    enriched with HAVE spectral, so the loader must rebuild it — never
    hand it to the decision as ``loaded``. The one legitimate non-rebuild
    outcome is the pre-existing lossless-source-V0 guard: a row that
    requires the lossless-source V0 metric and has no request scalar to
    resurrect fails closed instead (a disk rebuild would fabricate
    provenance). That V0 lifecycle is the property above; here it only
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
    if requires_lossless_v0 and not has_v0_backfill_source:
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
            current_album_path=root,
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
# lossless with no request V0 scalar must fail closed, not rebuild.
_BLANK_LOSSLESS_NO_SCALAR_WORLD = BlankPathWorld(
    source_path_kind="blank",
    spectral_grade=None,
    min_bitrate=108,
    avg_bitrate=114,
    was_converted_from="flac",
    request_has_v0_scalar=False,
)


class TestGeneratedBlankSourcePath(unittest.TestCase):
    """Action-loader invariants over generated source_path worlds."""

    @given(world=blank_path_worlds())
    @example(world=_FRENCH_QUARTER_WORLD)
    @example(world=_BLANK_LOSSLESS_NO_SCALAR_WORLD)
    def test_blank_source_path_is_never_authoritative(self, world):
        current_status, available, result_source_path = (
            _run_blank_path_world(world)
        )
        assert_blank_path_outcome(
            source_path_kind=world.source_path_kind,
            requires_lossless_v0=(
                world.was_converted_from is not None
                or world.request_has_v0_scalar
            ),
            has_v0_backfill_source=world.request_has_v0_scalar,
            current_status=current_status,
            available=available,
            result_source_path=result_source_path,
        )


class TestBlankPathCheckerTripsOnViolations(unittest.TestCase):
    """Known-bad self-tests for the blank-path invariant checker."""

    def test_trips_on_loaded_blank_path(self):
        with self.assertRaises(AssertionError):
            assert_blank_path_outcome(
                source_path_kind="blank", requires_lossless_v0=False,
                has_v0_backfill_source=False, current_status="loaded",
                available=True, result_source_path="")

    def test_trips_on_fail_closed_instead_of_rebuild(self):
        with self.assertRaises(AssertionError):
            assert_blank_path_outcome(
                source_path_kind="blank", requires_lossless_v0=False,
                has_v0_backfill_source=False, current_status="failed",
                available=False, result_source_path=None)

    def test_trips_on_rebuilt_row_still_blank(self):
        with self.assertRaises(AssertionError):
            assert_blank_path_outcome(
                source_path_kind="whitespace", requires_lossless_v0=False,
                has_v0_backfill_source=False, current_status="backfilled",
                available=True, result_source_path="   ")

    def test_trips_on_real_path_not_loading(self):
        with self.assertRaises(AssertionError):
            assert_blank_path_outcome(
                source_path_kind="real", requires_lossless_v0=False,
                has_v0_backfill_source=False, current_status="backfilled",
                available=True, result_source_path="/library/album")

    def test_trips_on_lossless_no_scalar_becoming_available(self):
        with self.assertRaises(AssertionError):
            assert_blank_path_outcome(
                source_path_kind="blank", requires_lossless_v0=True,
                has_v0_backfill_source=False, current_status="backfilled",
                available=True, result_source_path="/library/album")


class TestLifecycleCheckerTripsOnViolations(unittest.TestCase):
    """Known-bad self-tests for the lifecycle invariant checker."""

    def test_trips_on_loaded_without_v0(self):
        with self.assertRaises(AssertionError):
            assert_lifecycle_outcome(
                request_has_v0_scalar=True, source_v0_avg=171,
                current_status="loaded", available=True, result_v0_avg=171)

    def test_trips_on_missing_backfill(self):
        with self.assertRaises(AssertionError):
            assert_lifecycle_outcome(
                request_has_v0_scalar=True, source_v0_avg=171,
                current_status="rebuilt", available=False, result_v0_avg=None)

    def test_trips_on_not_failing_closed(self):
        with self.assertRaises(AssertionError):
            assert_lifecycle_outcome(
                request_has_v0_scalar=False, source_v0_avg=171,
                current_status="rebuilt", available=True, result_v0_avg=None)


if __name__ == "__main__":
    unittest.main()
