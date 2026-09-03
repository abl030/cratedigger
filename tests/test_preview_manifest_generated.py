"""Preview-sidecar canonical-manifest-purity tests — issue #859.

PR #858 (fix for #853) let the automation preview worker operate directly
on the Cratedigger-owned canonical album under ``processing/albums/``
(``owns_path=True`` -> ``temp_root=None`` -> ``preview_path=path``).
``_write_preview_spectral_evidence_file`` then wrote
``preview-spectral-evidence.json`` INTO that canonical directory, and the
preview worker's cleanup only removed a private ``temp_root`` snapshot — so
the sidecar persisted forever on an owned album whose preview never needed
a private copy. The importer's ``_materialize_processing_dir`` (via
``_canonical_manifest_complete``) requires EXACT set equality between the
canonical directory listing and the download manifest; the leaked sidecar
broke that equality, so every rematerialize attempt returned
``MaterializeGuarded(detail="incomplete_or_unsafe_canonical")`` and the
automation job failed forever — the request never left ``downloading``.

Invariant: **a canonical processing album is an exact media manifest** — no
preview JSON, action file, or other control-plane artifact ever belongs
inside it, whatever preview action ran against it. This module ships the
required PAIR (``.claude/rules/code-quality.md`` § Red/Green TDD):

  1. A deterministic composed pin (``TestPreviewSidecarManifestPurityPin``)
     driving the REAL ``_materialize_processing_dir`` +
     ``measure_and_persist_candidate_evidence`` (the actual #859 fire
     site, with the real sidecar writer — never stubbed) against a real
     owned canonical album, then asserting manifest purity, a clean
     rematerialize, and that ``process_completed_album`` reaches its
     dispatch seam instead of deferring.
  2. A generated property (``TestPreviewManifestPurityProperty``)
     patrolling the same composed path over varied manifests (file count,
     basenames with spaces/unicode, mp3/flac mix).
  3. Known-bad self-tests proving EVERY CLAUSE of every checker trips on
     a planted violation of exactly that clause (the pre-fix shape: an
     extra file left in the canonical directory / a guarded
     rematerialize / an action file that moved or vanished), each
     asserting its own message anchored end to end — issue #1094's
     per-clause proof.

Profiles and promotion policy: tests/_hypothesis_profiles.py and
docs/generated-testing.md.
"""

import os
import re
import sys
import tempfile
import unittest
from dataclasses import dataclass
from itertools import combinations
from unittest.mock import patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from hypothesis import example, given
from hypothesis import strategies as st

import tests._hypothesis_profiles  # noqa: F401  (loads the active profile)
from lib.config import CratediggerConfig
from lib.context import CratediggerContext
from lib.dispatch import DispatchCoreFn, DispatchOutcome
from lib.dispatch.types import ImportOneRun
from lib.download_materialization import (
    Materialized,
    MaterializeGuarded,
    MaterializeResult,
    _materialize_processing_dir,
)
from lib.download_processing import (
    CompletionDeferred,
    CompletionDispatched,
    process_completed_album,
)
from lib.download_validation import HandleValidFn
from lib.grab_list import DownloadFile, GrabListEntry
from lib.import_execution import CancellationToken, ExecutionOwnerProof
from lib.import_preview import (
    ImportPreviewResult,
    measure_and_persist_candidate_evidence,
)
from lib.measurement import ExistingSpectralAuditLookup
from lib.processing_paths import canonical_folder_for_row, processing_albums_dir
from lib.quality import (
    AacLatticeCapture,
    AudioQualityMeasurement,
    ImportResult,
    SpectralAnalysisDetail,
    SpectralDetail,
)
from lib.spectral_check import SPECTRAL_MEASUREMENT_VERSION
from lib.staged_album import StagedAlbum
from tests.audio_fixtures import make_test_flac
from tests.fakes import FakeBeetsDB, FakePipelineDB
from tests.finite_domain import finite_generated_domain
from tests.helpers import make_ctx_with_fake_db, make_grab_list_entry, make_request_row

_HARNESS = "/nix/store/fake/harness/run_beets_harness.sh"

# One mandatory FLAC guarantees ``lossless_candidate=True`` (the gate that
# selects the sidecar-writing branch) regardless of which extras Hypothesis
# draws — see ``lib.measurement.has_supported_lossless_audio``.
_MANDATORY_FLAC = "01 - Track One.flac"
_EXTRA_FILENAME_POOL: tuple[str, ...] = (
    "02 - Track Two.mp3",
    "03 - Track Three (Live).flac",
    "04 - Ünïcödé Track.mp3",
    "05 - 曲.flac",
    "06 - Space Name Here.mp3",
    "07 - Été à Paris.flac",
    "08 - ☆Star☆.mp3",
)
# This is an exact finite domain, not an entropy surface. A bounded integer
# gives Hypothesis one canonical representation per subset; the matching fixed
# budget also keeps the fuzz runner from repeating the same domain in eight
# shards after every subset has already been exercised.
_EXTRA_FILENAME_WORLD_COUNT = 1 << len(_EXTRA_FILENAME_POOL)
_extra_filename_mask_strategy = st.integers(
    min_value=0,
    max_value=_EXTRA_FILENAME_WORLD_COUNT - 1,
)


def _extra_filenames_for_mask(mask: int) -> frozenset[str]:
    if not 0 <= mask < _EXTRA_FILENAME_WORLD_COUNT:
        raise ValueError(f"filename mask is outside the finite domain: {mask}")
    return frozenset(
        filename
        for index, filename in enumerate(_EXTRA_FILENAME_POOL)
        if mask & (1 << index)
    )


def assert_extra_filename_mask_domain(
    mapped: tuple[frozenset[str], ...],
) -> None:
    """Every subset of the filename pool must have one exact mask."""
    expected = {
        frozenset(selected)
        for size in range(len(_EXTRA_FILENAME_POOL) + 1)
        for selected in combinations(_EXTRA_FILENAME_POOL, size)
    }
    actual = set(mapped)
    if len(mapped) != len(expected) or actual != expected:
        raise AssertionError(
            "filename masks do not map one-to-one onto every manifest subset"
        )


def verify_extra_filename_mask_domain() -> None:
    assert_extra_filename_mask_domain(tuple(
        _extra_filenames_for_mask(mask)
        for mask in range(_EXTRA_FILENAME_WORLD_COUNT)
    ))


# ============================================================================
# Shared composed-path builders
# ============================================================================

def _stamped_files(
    basenames: frozenset[str], src_dir: str, *, username: str = "peer0",
    real_audio: bool = False,
) -> list[DownloadFile]:
    """Real on-disk, event-stamped DownloadFiles for a generated manifest."""
    files: list[DownloadFile] = []
    os.makedirs(src_dir, exist_ok=True)
    for basename in sorted(basenames):
        src_path = os.path.join(src_dir, basename)
        if real_audio:
            assert basename.endswith(".flac")
            make_test_flac(src_path, duration=1)
        else:
            with open(src_path, "wb") as handle:
                handle.write(f"fake-audio-bytes:{basename}".encode())
        file = DownloadFile(
            filename=f"{username}\\Music\\{basename}",
            id=f"{username}:{basename}",
            file_dir=f"{username}\\Music",
            username=username,
            size=os.path.getsize(src_path) if real_audio else 32,
        )
        file.local_path = src_path
        files.append(file)
    return files


def _materialize_canonical_album(
    tmp_root: str,
    *,
    request_id: int,
    mb_release_id: str,
    basenames: frozenset[str],
    beets_validation_enabled: bool = False,
    real_audio: bool = False,
) -> tuple[FakePipelineDB, CratediggerContext, GrabListEntry, StagedAlbum]:
    """Build and materialize a real Cratedigger-owned canonical album.

    Returns ``(db, ctx, album, staged_album)`` with
    ``staged_album.current_path`` set to the published canonical directory
    — exactly the state the automation import queue hands to preview.
    """
    slskd_dir = os.path.join(tmp_root, "slskd")
    processing_dir = os.path.join(tmp_root, "processing")
    staging_dir = os.path.join(tmp_root, "Incoming")
    os.makedirs(processing_dir, mode=0o700)
    os.makedirs(os.path.join(processing_dir, "albums"), mode=0o700)
    cfg = CratediggerConfig(
        slskd_download_dir=slskd_dir,
        processing_dir=processing_dir,
        beets_staging_dir=staging_dir,
        beets_harness_path=_HARNESS,
        pipeline_db_enabled=True,
        beets_validation_enabled=beets_validation_enabled,
        audio_check_mode="off",
        var_dir=tmp_root,
    )
    files = _stamped_files(
        basenames,
        os.path.join(slskd_dir, "peer0", "Music"),
        real_audio=real_audio,
    )
    album = make_grab_list_entry(
        files=files,
        artist="Issue Artist",
        title="Issue Album",
        year="2026",
        mb_release_id=mb_release_id,
        db_source="request",
        db_request_id=request_id,
    )
    db = FakePipelineDB()
    db.seed_request(make_request_row(
        id=request_id,
        status="downloading",
        mb_release_id=mb_release_id,
        artist_name="Issue Artist",
        album_title="Issue Album",
        year=2026,
        active_download_state={
            "filetype": "flac",
            "enqueued_at": "2026-07-24T00:00:00+00:00",
            "files": [],
            "current_path": "",
        },
    ))
    ctx: CratediggerContext = make_ctx_with_fake_db(db, cfg=cfg)
    staged_album = StagedAlbum.from_entry(
        album,
        default_path=canonical_folder_for_row(
            album, processing_albums_dir(cfg.processing_dir)),
    )
    result = _materialize_processing_dir(album, staged_album, ctx)
    assert isinstance(result, Materialized), (
        f"setup precondition failed: initial materialize returned {result!r}"
    )
    return db, ctx, album, staged_album


def _stub_aac_lattice(path: str) -> AacLatticeCapture:
    """Fast per-track stand-in wired through the REAL per-album recording
    loop, not a flat replacement of the whole capture.

    ``measure_album_aac_lattice`` (``lib/aac_lattice.py``) exposes
    ``analyze_fn`` as its own sanctioned kwarg-DI seam for exactly this —
    its docstring: "lets the generated fault-isolation property drive this
    real recording loop over a generated fault space without paying for
    ffmpeg per example." Using it here keeps the real ``album_audio_files``
    walk of the canonical directory, the real per-file recording loop, and
    the real ``AacLatticeCapture.from_tracks`` aggregation inside the
    composition — only ``analyze_track``'s expensive leg (subprocess ffmpeg
    decode + MDCT/FFT, tens of seconds of CPU per track) is replaced by
    ``_fixed_analysis``. That is higher fidelity than a flat empty capture:
    every manifest this module builds has >=1 audio file, and production
    always returns one scored row per file it finds — a flat
    ``AacLatticeCapture()`` is the shape production only returns for an
    album with NO audio files, which this module never builds.

    ``measure_and_persist_candidate_evidence`` defaults
    ``aac_lattice_measure_fn`` to the real ``measure_aac_lattice`` (issue
    #829 PR-A). No assertion in this module reads the resulting
    ``AlbumQualityEvidence.aac_lattice`` VALUE — but it is not entirely off
    the assertion path either: ``_write_preview_spectral_evidence_file``
    folds ``AacLatticeCapture.validation_errors()`` into its own
    ``storage_validation_errors()`` gate, so a MALFORMED capture would
    break today's ``assert len(handoffs) == 1`` below. The accurate claim
    is narrower than "capture-only": only the lattice's well-formedness is
    load-bearing here, never its content. What this module genuinely never
    exercises is the DECISION leg — ``full_pipeline_decision_from_evidence``
    reads a candidate's lattice as a promotion gate
    (``lib/quality/pipeline.py``), and production does carry a fresh
    lattice into the quality-evidence action-file payload and on to
    ``harness/import_one.py``'s ``aac_lattice_proof_leg`` — but what severs
    that here is the PRE-EXISTING ``run_import_fn`` stub below
    (``_capture_action_file_handoff``), not any preview/importer split.
    """
    from lib.aac_lattice import AacLatticeAnalysis, measure_album_aac_lattice

    def _fixed_analysis(_track_path: str) -> AacLatticeAnalysis:
        """A fast, well-formed per-track score — same shape as a real
        scored track (bounded offset, finite z/proba) — standing in for
        ``analyze_track``'s real decode+DSP."""
        return AacLatticeAnalysis(
            offset=347, z=4.72, proba=0.5, sample_rate=44100, channels=2,
        )

    return measure_album_aac_lattice(path, analyze_fn=_fixed_analysis)


def _stub_import_one_run() -> ImportOneRun:
    """A minimal, valid harness result — the harness subprocess itself is
    the sanctioned ``run_import_fn`` kwarg-DI seam (never the sidecar
    writer, which runs for real before this stub is even invoked)."""
    return ImportOneRun(
        command=("import_one",), returncode=0, stdout="", stderr="",
        import_result=ImportResult(
            decision="import",
            source_measurement=AudioQualityMeasurement(
                min_bitrate_kbps=1000, avg_bitrate_kbps=1000,
                median_bitrate_kbps=1000, format="FLAC", is_cbr=True,
            ),
            spectral=SpectralDetail(candidate=SpectralAnalysisDetail(
                attempted=True, grade="genuine", bitrate_kbps=1000,
                spectral_measurement_version=SPECTRAL_MEASUREMENT_VERSION,
            )),
        ),
    )


@dataclass(frozen=True)
class PreviewActionFileHandoff:
    """The real action-file path observed at the harness leaf seam."""

    path: str
    exists_at_handoff: bool
    resolved_parent: str


def _run_owned_preview_action(
    db: FakePipelineDB,
    ctx: CratediggerContext,
    *,
    request_id: int,
    canonical_dir: str,
    import_job_id: int = 1,
) -> tuple[ImportPreviewResult, PreviewActionFileHandoff]:
    """Drive the REAL preview fact-gathering (the #859 fire site) against a
    real owned canonical album. ``_write_preview_spectral_evidence_file``
    runs unmocked. Every kwarg-DI seam passed to
    ``measure_and_persist_candidate_evidence`` below stands in for a
    legitimate external edge, never this module's own manifest-purity
    logic: the harness subprocess (``run_import_fn``), the beets
    exact-release lookup (``existing_spectral_resolver``), and both
    spectral detectors (``spectral_detail_analyzer``,
    ``aac_lattice_measure_fn``)."""
    run = _stub_import_one_run()
    handoffs: list[PreviewActionFileHandoff] = []
    if db.get_import_job(import_job_id) is None:
        download_log_id = db.log_download(
            request_id=request_id,
            outcome="rejected",
        )
        job = db.enqueue_import_job(
            "force_import",
            request_id=request_id,
            payload={
                "download_log_id": download_log_id,
                "failed_path": canonical_dir,
            },
        )
        assert job.id == import_job_id

    def _capture_action_file_handoff(**kwargs: object) -> ImportOneRun:
        action_file = kwargs["quality_evidence_action_file"]
        assert isinstance(action_file, str)
        handoffs.append(PreviewActionFileHandoff(
            path=action_file,
            exists_at_handoff=os.path.isfile(action_file),
            resolved_parent=os.path.realpath(os.path.dirname(action_file)),
        ))
        return run

    with patch("lib.beets_db.BeetsDB", lambda **_kwargs: FakeBeetsDB()):
        result = measure_and_persist_candidate_evidence(
            db,
            request_id=request_id,
            path=canonical_dir,
            runtime_config=ctx.cfg,
            import_job_id=import_job_id,
            run_import_fn=_capture_action_file_handoff,
            existing_spectral_resolver=lambda _mbid: ExistingSpectralAuditLookup(),
            spectral_detail_analyzer=lambda _path: SpectralAnalysisDetail(
                attempted=True, grade="genuine", bitrate_kbps=1000,
                spectral_measurement_version=SPECTRAL_MEASUREMENT_VERSION,
            ),
            aac_lattice_measure_fn=_stub_aac_lattice,
        )
    assert len(handoffs) == 1, "lossless preview must hand off one action file"
    return result, handoffs[0]


def _expected_basenames(
    album: GrabListEntry, staged_album: StagedAlbum,
) -> frozenset[str]:
    return frozenset(
        os.path.basename(staged_album.import_path_for(f)) for f in album.files
    )


def _fresh_rematerialize(
    album: GrabListEntry, ctx: CratediggerContext,
) -> MaterializeResult:
    """A brand-new ``StagedAlbum`` at the same canonical default path — the
    exact construction ``process_completed_album`` performs on every
    poll-cycle retry."""
    fresh_staged = StagedAlbum.from_entry(
        album,
        default_path=canonical_folder_for_row(
            album, processing_albums_dir(ctx.cfg.processing_dir)),
    )
    return _materialize_processing_dir(album, fresh_staged, ctx)


# ============================================================================
# Invariant checkers (module-level so the known-bad self-tests can call
# them directly)
# ============================================================================

def assert_canonical_manifest_pure(
    actual_basenames: frozenset[str],
    expected_basenames: frozenset[str],
    *, label: str,
) -> None:
    """A canonical processing album must remain an exact media manifest
    after any preview action — no sidecar/action-file/control-plane
    artifact ever belongs inside it."""
    if actual_basenames != expected_basenames:
        raise AssertionError(
            f"{label}: canonical album directory diverged from its manifest "
            f"after preview (missing="
            f"{sorted(expected_basenames - actual_basenames)} extra="
            f"{sorted(actual_basenames - expected_basenames)})"
        )


def assert_preview_action_file_handoff_is_safe(
    handoff: PreviewActionFileHandoff,
    canonical_dir: str,
    *,
    label: str,
) -> None:
    """The action file must exist at handoff, outside every album tree.

    The canonical album is itself below the system temporary root in these
    tests, so checking only that the action file is below ``gettempdir()``
    would permit the relocation-only #859 regression.
    """
    expected_parent = os.path.realpath(tempfile.gettempdir())
    resolved_album = os.path.realpath(canonical_dir)
    if not handoff.exists_at_handoff:
        raise AssertionError(f"{label}: action file did not exist at handoff")
    if handoff.resolved_parent != expected_parent:
        raise AssertionError(
            f"{label}: action file parent {handoff.resolved_parent!r} is not "
            f"the system tempfile directory {expected_parent!r}"
        )
    if os.path.commonpath((os.path.realpath(handoff.path), resolved_album)) == resolved_album:
        raise AssertionError(
            f"{label}: action file {handoff.path!r} was inside canonical "
            f"album {canonical_dir!r}"
        )


def assert_rematerializes_cleanly(result: MaterializeResult, *, label: str) -> None:
    """Rematerializing an already-complete canonical album must return
    ``Materialized`` — a leaked control-plane file breaks the manifest-
    equality guard and stalls the request in ``downloading`` forever."""
    if not isinstance(result, Materialized):
        raise AssertionError(  # noqa: TRY004 - generated invariant failure
            f"{label}: rematerialize after preview must return Materialized, "
            f"got {result!r}"
        )


# ============================================================================
# 1. Deterministic composed pin
# ============================================================================

class TestPreviewSidecarManifestPurityPin(unittest.TestCase):
    """Issue #859's composed RED reproduction.

    Fails on the unmodified tree with a leaked
    ``preview-spectral-evidence.json`` inside the canonical album
    directory and a ``CompletionDeferred(detail="incomplete_or_unsafe_canonical")``
    from ``process_completed_album`` — passes once the sidecar writer
    moves outside the canonical album for good.
    """

    def test_owned_canonical_preview_keeps_manifest_pure_and_unblocks_reimport(
        self,
    ) -> None:
        request_id = 8590001
        mb_release_id = "mbid-issue-859"
        basenames = frozenset({
            "01 - Track One.flac",
            "02 - Track Two.flac",
        })
        with tempfile.TemporaryDirectory(
            prefix="cratedigger-issue-859-pin-",
        ) as tmp_root:
            db, ctx, album, staged_album = _materialize_canonical_album(
                tmp_root,
                request_id=request_id,
                mb_release_id=mb_release_id,
                basenames=basenames,
                beets_validation_enabled=True,
                real_audio=True,
            )
            canonical_dir = staged_album.current_path
            expected_basenames = _expected_basenames(album, staged_album)

            preview_result, action_handoff = _run_owned_preview_action(
                db, ctx, request_id=request_id, canonical_dir=canonical_dir,
            )
            self.assertEqual(
                preview_result.verdict, "evidence_ready",
                f"preview must reach a real verdict, got "
                f"decision={preview_result.decision!r} "
                f"detail={preview_result.detail!r}",
            )

            assert_preview_action_file_handoff_is_safe(
                action_handoff, canonical_dir, label="preview action handoff",
            )
            self.assertFalse(
                os.path.exists(action_handoff.path),
                "preview cleanup must remove the action file after handoff",
            )

            # (a) manifest purity: the canonical dir holds ONLY the manifest.
            actual_basenames = frozenset(os.listdir(canonical_dir))
            assert_canonical_manifest_pure(
                actual_basenames, expected_basenames,
                label="post-preview canonical dir",
            )

            # (b) a fresh rematerialize (the next poll cycle's retry shape)
            # must still succeed.
            remat_result = _fresh_rematerialize(album, ctx)
            assert_rematerializes_cleanly(
                remat_result, label="rematerialize after preview")

            # (c) process_completed_album must reach its dispatch seam
            # instead of deferring — the real default materialize_fn, with
            # kwarg-DI stubs standing in for beets validation/dispatch.
            validate_calls: list[str] = []

            def _stub_validate(
                album_data: GrabListEntry,
                staged_album: StagedAlbum,
                ctx: CratediggerContext,
                *,
                import_job_id: int,
                handle_valid_fn: HandleValidFn | None = None,
                dispatch_fn: DispatchCoreFn | None = None,
                cancellation_token: CancellationToken | None = None,
                owner_proof: ExecutionOwnerProof | None = None,
            ) -> DispatchOutcome:
                del (
                    album_data,
                    ctx,
                    import_job_id,
                    handle_valid_fn,
                    dispatch_fn,
                    cancellation_token,
                    owner_proof,
                )
                validate_calls.append(staged_album.current_path)
                return DispatchOutcome(success=True, message="validate reached")

            completion_result = process_completed_album(
                album, ctx, import_job_id=1, validate_fn=_stub_validate,
            )
            self.assertNotIsInstance(
                completion_result, CompletionDeferred,
                f"process_completed_album deferred: {completion_result!r}",
            )
            self.assertIsInstance(completion_result, CompletionDispatched)
            self.assertEqual(len(validate_calls), 1)


# ============================================================================
# 2. Generated property — same composed path, varied manifests
# ============================================================================

class TestPreviewManifestPurityProperty(unittest.TestCase):
    """Patrols the same composed path (``_materialize_canonical_album`` +
    ``measure_and_persist_candidate_evidence``) over the exact finite manifest
    domain: file count, basenames with spaces/unicode, mp3/flac mix."""

    @finite_generated_domain(
        cardinality=_EXTRA_FILENAME_WORLD_COUNT,
        verify=verify_extra_filename_mask_domain,
    )
    @given(mask=_extra_filename_mask_strategy)
    @example(mask=0)
    @example(mask=_EXTRA_FILENAME_WORLD_COUNT - 1)
    def test_owned_canonical_album_stays_pure_after_preview(self, mask: int):
        extra = _extra_filenames_for_mask(mask)
        basenames = frozenset({_MANDATORY_FLAC}) | frozenset(extra)
        request_id = 8590100
        mb_release_id = "mbid-issue-859-gen"
        with tempfile.TemporaryDirectory(
            prefix="cratedigger-issue-859-gen-",
        ) as tmp_root:
            db, ctx, album, staged_album = _materialize_canonical_album(
                tmp_root,
                request_id=request_id,
                mb_release_id=mb_release_id,
                basenames=basenames,
            )
            canonical_dir = staged_album.current_path
            expected_basenames = _expected_basenames(album, staged_album)

            _, action_handoff = _run_owned_preview_action(
                db, ctx, request_id=request_id, canonical_dir=canonical_dir,
            )

            assert_preview_action_file_handoff_is_safe(
                action_handoff, canonical_dir, label="generated world",
            )
            self.assertFalse(
                os.path.exists(action_handoff.path),
                "preview cleanup must remove the action file after handoff",
            )

            actual_basenames = frozenset(os.listdir(canonical_dir))
            assert_canonical_manifest_pure(
                actual_basenames, expected_basenames, label="generated world",
            )

            remat_result = _fresh_rematerialize(album, ctx)
            assert_rematerializes_cleanly(remat_result, label="generated world")


# ============================================================================
# 3. Known-bad self-tests for the invariant checkers
# ============================================================================
#
# Per-clause proof (issue #1094, docs/generated-testing.md § "Per-clause
# proof"). Every clause of every checker above gets its OWN named world —
# the minimal one that makes that clause's condition true while every
# EARLIER clause in the same function passes — and asserts that clause's
# own message anchored end to end. A bare ``assertRaises(AssertionError)``
# proves only that *something* refused, which is how a short-circuiting
# ``raise`` chain advertises clauses it never evaluates.


def _exactly(message: str) -> str:
    """Anchor one clause's complete message for ``assertRaisesRegex``."""
    return f"^{re.escape(message)}$"


class TestPreviewManifestCheckersTripOnViolations(unittest.TestCase):
    """Every clause of every checker above must trip on a planted
    violation of the invariant it claims to enforce — the pre-fix #859
    shape, one clause at a time."""

    def test_manifest_purity_clause_names_the_exact_divergence(self):
        cases = (
            (
                "leaked sidecar (the #859 shape)",
                frozenset({"01.flac", "preview-spectral-evidence.json"}),
                frozenset({"01.flac"}),
                ("known-bad: canonical album directory diverged from its "
                "manifest after preview (missing=[] "
                "extra=['preview-spectral-evidence.json'])"),
            ),
            (
                "a manifest file went missing",
                frozenset(),
                frozenset({"01.flac"}),
                ("known-bad: canonical album directory diverged from its "
                "manifest after preview (missing=['01.flac'] extra=[])"),
            ),
        )
        for label, actual, expected, message in cases:
            with self.subTest(clause=label), self.assertRaisesRegex(
                AssertionError, _exactly(message),
            ):
                assert_canonical_manifest_pure(
                    actual, expected, label="known-bad",
                )

    def test_rematerialize_clause_names_the_guarded_result(self):
        guarded = MaterializeGuarded(detail="incomplete_or_unsafe_canonical")
        with self.assertRaisesRegex(AssertionError, _exactly(
            "known-bad: rematerialize after preview must return "
            f"Materialized, got {guarded!r}",
        )):
            assert_rematerializes_cleanly(guarded, label="known-bad")

    def test_every_action_file_handoff_clause_fires_on_its_own_world(self):
        """Each clause's world lets every earlier clause pass.

        Clause 3 (``inside canonical album``) is the one clause with no
        production-shaped world: clause 2 demands the action file's
        parent be the system temp directory ITSELF, so an album directory
        anywhere below it — where every real canonical album lives —
        trips clause 2 first. It is kept as fail-closed legislation for a
        future relaxation of clause 2 into a prefix test, which is exactly
        the relocation-only #859 regression clause 2's docstring names.
        Its only legitimate caller is this world.
        """
        tmp = os.path.realpath(tempfile.gettempdir())
        album_below_tmp = os.path.join(tmp, "canonical-album")
        cases = (
            (
                "1: the action file did not exist at handoff",
                PreviewActionFileHandoff(
                    path=os.path.join(tmp, "action.json"),
                    exists_at_handoff=False,
                    resolved_parent=tmp,
                ),
                album_below_tmp,
                "known-bad: action file did not exist at handoff",
            ),
            (
                "2: relocated out of the system temp directory",
                PreviewActionFileHandoff(
                    path=os.path.join(album_below_tmp, "action.json"),
                    exists_at_handoff=True,
                    resolved_parent=album_below_tmp,
                ),
                album_below_tmp,
                (f"known-bad: action file parent {album_below_tmp!r} is not "
                f"the system tempfile directory {tmp!r}"),
            ),
            (
                "3: inside the canonical album (fail-closed legislation)",
                PreviewActionFileHandoff(
                    path=os.path.join(tmp, "action.json"),
                    exists_at_handoff=True,
                    resolved_parent=tmp,
                ),
                tmp,
                (f"known-bad: action file {os.path.join(tmp, 'action.json')!r} "
                f"was inside canonical album {tmp!r}"),
            ),
        )
        for clause, handoff, canonical_dir, message in cases:
            with self.subTest(clause=clause), self.assertRaisesRegex(
                AssertionError, _exactly(message),
            ):
                assert_preview_action_file_handoff_is_safe(
                    handoff, canonical_dir, label="known-bad",
                )

    def test_a_safe_handoff_passes_every_clause(self):
        """The must-still-work control: no clause fires on the real shape."""
        tmp = os.path.realpath(tempfile.gettempdir())
        assert_preview_action_file_handoff_is_safe(
            PreviewActionFileHandoff(
                path=os.path.join(tmp, "cratedigger-quality-evidence.json"),
                exists_at_handoff=True,
                resolved_parent=tmp,
            ),
            os.path.join(tmp, "processing", "albums", "Artist - Album (2026)"),
            label="must-still-work",
        )

    def test_filename_mask_domain_checker_trips_on_a_collapsed_world(self):
        collapsed = tuple(
            frozenset()
            for _mask in range(1 << len(_EXTRA_FILENAME_POOL))
        )
        with self.assertRaisesRegex(AssertionError, _exactly(
            "filename masks do not map one-to-one onto every manifest subset",
        )):
            assert_extra_filename_mask_domain(collapsed)

    def test_filename_mask_domain_checker_trips_on_a_short_domain(self):
        """The other half of the same clause: a domain missing worlds."""
        short = tuple(
            _extra_filenames_for_mask(mask)
            for mask in range(_EXTRA_FILENAME_WORLD_COUNT - 1)
        )
        with self.assertRaisesRegex(AssertionError, _exactly(
            "filename masks do not map one-to-one onto every manifest subset",
        )):
            assert_extra_filename_mask_domain(short)

    def test_mask_mapper_refuses_a_value_outside_the_finite_domain(self):
        """The strategy mapper's own domain guard: the certified budget and
        the mask range must not drift apart silently."""
        for mask in (-1, _EXTRA_FILENAME_WORLD_COUNT):
            with self.subTest(mask=mask), self.assertRaisesRegex(
                ValueError,
                _exactly(f"filename mask is outside the finite domain: {mask}"),
            ):
                _extra_filenames_for_mask(mask)


class TestPreviewManifestFiniteDomain(unittest.TestCase):
    def test_every_filename_subset_has_one_exact_mask(self):
        verify_extra_filename_mask_domain()


if __name__ == "__main__":
    unittest.main()
