#!/usr/bin/env python3
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
  3. Known-bad self-tests proving both checkers trip on a planted
     violation (the pre-fix shape: an extra file left in the canonical
     directory / a guarded rematerialize).

Profiles and promotion policy: tests/_hypothesis_profiles.py and
docs/generated-testing.md.
"""

import os
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import tests._hypothesis_profiles  # noqa: F401  (loads the active profile)

from hypothesis import example, given
from hypothesis import strategies as st

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
from lib.import_preview import (
    ImportPreviewResult,
    measure_and_persist_candidate_evidence,
)
from lib.measurement import ExistingSpectralAuditLookup
from lib.processing_paths import canonical_folder_for_row, processing_albums_dir
from lib.quality import (
    AudioQualityMeasurement,
    ImportResult,
    SpectralAnalysisDetail,
    SpectralDetail,
)
from lib.staged_album import StagedAlbum
from tests.fakes import FakeBeetsDB, FakePipelineDB
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
_extra_filenames_strategy = st.sets(
    st.sampled_from(_EXTRA_FILENAME_POOL), max_size=len(_EXTRA_FILENAME_POOL),
)


# ============================================================================
# Shared composed-path builders
# ============================================================================

def _stamped_files(
    basenames: "frozenset[str]", src_dir: str, *, username: str = "peer0",
) -> list[DownloadFile]:
    """Real on-disk, event-stamped DownloadFiles for a generated manifest."""
    files: list[DownloadFile] = []
    os.makedirs(src_dir, exist_ok=True)
    for basename in sorted(basenames):
        src_path = os.path.join(src_dir, basename)
        with open(src_path, "wb") as handle:
            handle.write(f"fake-audio-bytes:{basename}".encode("utf-8"))
        file = DownloadFile(
            filename=f"{username}\\Music\\{basename}",
            id=f"{username}:{basename}",
            file_dir=f"{username}\\Music",
            username=username,
            size=32,
        )
        file.local_path = src_path
        files.append(file)
    return files


def _materialize_canonical_album(
    tmp_root: str,
    *,
    request_id: int,
    mb_release_id: str,
    basenames: "frozenset[str]",
    beets_validation_enabled: bool = False,
) -> "tuple[FakePipelineDB, CratediggerContext, GrabListEntry, StagedAlbum]":
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
    )
    files = _stamped_files(basenames, os.path.join(slskd_dir, "peer0", "Music"))
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
            )),
        ),
    )


def _run_owned_preview_action(
    db: FakePipelineDB,
    ctx: CratediggerContext,
    *,
    request_id: int,
    canonical_dir: str,
    import_job_id: int = 1,
) -> ImportPreviewResult:
    """Drive the REAL preview fact-gathering (the #859 fire site) against a
    real owned canonical album. ``_write_preview_spectral_evidence_file``
    runs unmocked — only the harness subprocess and the beets exact-release
    lookup (both legitimate external-edge seams) are stubbed."""
    run = _stub_import_one_run()
    with patch("lib.beets_db.BeetsDB", lambda **_kwargs: FakeBeetsDB()):
        return measure_and_persist_candidate_evidence(
            db,
            request_id=request_id,
            path=canonical_dir,
            runtime_config=ctx.cfg,
            import_job_id=import_job_id,
            run_import_fn=lambda **_kwargs: run,
            existing_spectral_resolver=lambda _mbid: ExistingSpectralAuditLookup(),
            spectral_detail_analyzer=lambda _path: SpectralAnalysisDetail(
                attempted=True, grade="genuine", bitrate_kbps=1000,
            ),
        )


def _expected_basenames(
    album: GrabListEntry, staged_album: StagedAlbum,
) -> "frozenset[str]":
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
    actual_basenames: "frozenset[str]",
    expected_basenames: "frozenset[str]",
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


def assert_rematerializes_cleanly(result: MaterializeResult, *, label: str) -> None:
    """Rematerializing an already-complete canonical album must return
    ``Materialized`` — a leaked control-plane file breaks the manifest-
    equality guard and stalls the request in ``downloading`` forever."""
    if not isinstance(result, Materialized):
        raise AssertionError(
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
            )
            canonical_dir = staged_album.current_path
            expected_basenames = _expected_basenames(album, staged_album)

            preview_result = _run_owned_preview_action(
                db, ctx, request_id=request_id, canonical_dir=canonical_dir,
            )
            self.assertEqual(
                preview_result.verdict, "evidence_ready",
                f"preview must reach a real verdict, got "
                f"decision={preview_result.decision!r} "
                f"detail={preview_result.detail!r}",
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
            ) -> DispatchOutcome:
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
    ``measure_and_persist_candidate_evidence``) over generated manifests:
    file count, basenames with spaces/unicode, mp3/flac mix."""

    @given(extra=_extra_filenames_strategy)
    @example(extra=frozenset())
    @example(extra=frozenset(_EXTRA_FILENAME_POOL))
    def test_owned_canonical_album_stays_pure_after_preview(self, extra):
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

            _run_owned_preview_action(
                db, ctx, request_id=request_id, canonical_dir=canonical_dir,
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

class TestPreviewManifestCheckersTripOnViolations(unittest.TestCase):
    """Every checker above must trip on a planted violation of the
    invariant it claims to enforce — the pre-fix #859 shape."""

    def test_manifest_purity_checker_trips_on_leaked_sidecar(self):
        with self.assertRaises(AssertionError):
            assert_canonical_manifest_pure(
                frozenset({"01.flac", "preview-spectral-evidence.json"}),
                frozenset({"01.flac"}),
                label="known-bad",
            )

    def test_manifest_purity_checker_trips_on_missing_file(self):
        with self.assertRaises(AssertionError):
            assert_canonical_manifest_pure(
                frozenset(), frozenset({"01.flac"}), label="known-bad",
            )

    def test_rematerialize_checker_trips_on_guarded_result(self):
        with self.assertRaises(AssertionError):
            assert_rematerializes_cleanly(
                MaterializeGuarded(detail="incomplete_or_unsafe_canonical"),
                label="known-bad",
            )


if __name__ == "__main__":
    unittest.main()
