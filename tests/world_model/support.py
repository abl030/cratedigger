"""Real PostgreSQL + real Beets support for the heavyweight world model."""

from __future__ import annotations

import os
from dataclasses import replace
from pathlib import Path
from typing import Any

import msgspec

from lib.pipeline_db.rows import AlbumRequestRow
from lib.beets_db import BeetsDB
from lib.beets_delete import (
    BeetsDeleteCompleted,
    BeetsDeleteFailed,
    BeetsDeleteRequest,
)
from lib.config import CratediggerConfig
from lib.destructive_release_service import (
    BanSourceRequest,
    BanSourceReleaseMismatch,
    BanSourceSuccess,
    ban_source,
)
from lib.dispatch import (
    dispatch_import_core,
    dispatch_import_from_db,
    run_import_one,
)
from lib.dispatch.types import ImportOneRun
from lib.import_evidence import (
    ActionEvidenceProvenance,
    CandidateEvidenceActionResult,
    CurrentEvidenceActionResult,
    load_current_evidence_for_action,
)
from lib.import_preview import enrich_incomplete_current_evidence_for_request
from lib.import_queue import (
    IMPORT_JOB_AUTOMATION,
    IMPORT_JOB_FORCE,
    automation_import_dedupe_key,
    automation_import_payload,
    force_import_dedupe_key,
    force_import_payload,
)
from lib.quality import (
    EVIDENCE_PROVENANCE_MEASURED,
    EVIDENCE_SUBJECT_SOURCE,
    AudioQualityMeasurement,
    DownloadInfo,
    QualityRankConfig,
    SpectralAnalysisDetail,
    V0ProbeEvidence,
    V0_PROBE_ON_DISK_RESEARCH,
    VerifiedLosslessProof,
    resolve_user_requeue_override,
)
from lib.quality_evidence import snapshot_audio_files, snapshot_fingerprint
from lib.mbid_replace_service import MbidReplaceService, RESULT_REPLACED
from lib.release_identity import ReleaseIdentity
from lib.search_plan_service import SearchPlanService
from lib.transitions import (
    RequestTransition,
    finalize_request,
    require_transition_applied,
)
from lib.world_invariants import (
    DenylistAuthoritySnapshot,
    EvidenceDiskSnapshot,
    LifecycleTransitionSnapshot,
    LibraryAlbumSnapshot,
    RequestMembershipSnapshot,
    WorldViolation,
    assert_replaced_row_frozen,
    check_denylist_authority,
    check_evidence_disk_coherence,
    check_folder_exclusivity,
    check_library_filesystem,
    check_no_lossy_tier_widening,
    check_proof_lock_terminality,
    check_status_membership,
    derive_denylist_authorities,
)
from lib.validation_envelope import decode_validation_envelope
from lib.wrong_match_delete_service import (
    delete_wrong_match as delete_wrong_match_source,
)
from tests.beets_world import BeetsWorld, BeetsWorldRelease
from tests.helpers import (
    finalize_claimed_dispatch,
    make_active_download_state_json,
    make_album_quality_evidence,
    make_import_result,
)
from tests.test_pipeline_db import TEST_DSN, make_db
from tests.world_model.census_seeds import (
    STATEFUL_WORLD_CENSUS_SEEDS,
    EvidenceDriftFactSeed,
    EvidenceDriftMutationSeed,
    WorldCensusSeed,
)


class LifecycleWorld:
    """One disposable pipeline DB slate coupled to one Beets library."""

    def __init__(
        self,
        dsn: str,
        repo_root: str | os.PathLike[str],
        *,
        import_engine: str = "in-process",
        mirror_url: str | None = None,
    ) -> None:
        if TEST_DSN != dsn:
            raise ValueError(
                "world model must use tests.test_pipeline_db.make_db() "
                "against its ephemeral TEST_DB_DSN"
            )
        if import_engine not in {"in-process", "mirror-harness"}:
            raise ValueError(f"unsupported world import engine: {import_engine!r}")
        if import_engine == "mirror-harness" and not mirror_url:
            raise ValueError("mirror-harness world requires a mirror URL")
        self._import_engine = import_engine
        self._repo_root = Path(repo_root)
        self._beets_harness_path = str(
            self._repo_root / "harness" / "run_beets_harness.sh"
        )
        self.db = make_db()
        try:
            self.beets = BeetsWorld(
                repo_root,
                subprocess_mirror_url=(
                    mirror_url if import_engine == "mirror-harness" else None
                ),
            )
        except BaseException:
            self.db.close()
            raise
        self._release_by_request: dict[int, BeetsWorldRelease] = {}
        self._dispatch_counter = 0
        self._operator_counter = 0
        self._last_subprocess_run: ImportOneRun | None = None
        self._replaced_snapshots: dict[int, dict[str, object]] = {}
        self._transitions: list[LifecycleTransitionSnapshot] = []

    def close(self) -> None:
        try:
            self.beets.close()
        finally:
            self.db.close()

    def __enter__(self) -> LifecycleWorld:
        return self

    def __exit__(self, *_args: object) -> None:
        self.close()

    def add_release(self, release: BeetsWorldRelease) -> int:
        identity = ReleaseIdentity.from_id(release.release_id)
        if identity is None:
            raise ValueError(
                f"world releases require an MB or Discogs id: {release.release_id!r}"
            )
        request_id = int(self.db.add_request(
            artist_name=release.artist,
            album_title=release.album,
            source="request",
            year=release.year,
            mb_release_id=identity.release_id,
            mb_release_group_id=(
                "00000000-0000-4000-8000-000000000743"
                if identity.source == "musicbrainz"
                else "9743"
            ),
            discogs_release_id=(
                identity.release_id
                if identity.source == "discogs"
                else None
            ),
        ))
        self.db.set_tracks(request_id, [
            {
                "disc_number": 1,
                "track_number": track,
                "title": release.track_title(track),
            }
            for track in range(1, release.track_count + 1)
        ])
        self._release_by_request[request_id] = release
        return request_id

    def seed_census_release(
        self,
        release: BeetsWorldRelease,
        seed: WorldCensusSeed,
    ) -> int:
        """Materialize one anonymized live row shape in disposable stores."""

        if seed not in STATEFUL_WORLD_CENSUS_SEEDS:
            raise ValueError(f"census seed is not stateful-safe: {seed.name}")

        request_id = self.add_release(release)
        album: LibraryAlbumSnapshot | None = None
        if seed.has_current_evidence:
            codec = seed.codec or release.codec
            if not self.import_request(
                request_id,
                codec=codec,
                verified_lossless=seed.verified_lossless,
            ):
                raise AssertionError(
                    f"census seed import was rejected: {seed.name}"
                )
            album = self._album_for_release(release.release_id)
            if album is None:
                raise AssertionError(
                    f"census seed did not create a Beets album: {seed.name}"
                )

        row = self._require_request(request_id)
        if seed.status == "imported" and row["status"] != "imported":
            if album is None:
                raise AssertionError("imported census seed requires a Beets album")
            require_transition_applied(finalize_request(
                self.db,
                request_id,
                RequestTransition.to_imported(
                    from_status=str(row["status"]),
                    imported_path=album.album_path,
                ),
            ))
        elif seed.status == "wanted" and row["status"] != "wanted":
            self.reset_to_wanted(request_id)

        imported_path: str | None
        if seed.has_imported_path:
            imported_path = (
                album.album_path
                if album is not None
                else str(
                    self.beets.library_root
                    / f"legacy-installed-marker-{request_id}"
                )
            )
        else:
            imported_path = None
        if not self.db.update_request_fields(
            request_id,
            expected_status=seed.status,
            search_filetype_override=seed.search_override,
            imported_path=imported_path,
            final_format=seed.final_format,
            current_spectral_grade=seed.spectral_grade,
            verified_lossless=seed.verified_lossless,
        ):
            raise AssertionError(f"failed to apply census metadata: {seed.name}")

        if seed.identity_shape == "both":
            self.db._execute(
                "UPDATE album_requests SET discogs_release_id = %s WHERE id = %s",
                (str(8_000_000 + request_id), request_id),
            )
            self.db.conn.commit()

        evidence_id = self.db.get_request_current_evidence_id(request_id)
        if seed.has_current_evidence and evidence_id is None:
            raise AssertionError(f"census evidence link missing: {seed.name}")
        if evidence_id is not None:
            self.db._execute(
                """
                UPDATE album_quality_evidence
                SET lineage_version = %s,
                    codec = %s,
                    storage_format = %s,
                    format = %s,
                    spectral_grade = %s,
                    spectral_subject = %s,
                    spectral_provenance = %s,
                    v0_min_bitrate_kbps = %s,
                    v0_avg_bitrate_kbps = %s,
                    v0_median_bitrate_kbps = %s,
                    v0_subject = %s,
                    v0_provenance = %s
                WHERE id = %s
                """,
                (
                    seed.lineage_version,
                    seed.codec,
                    seed.storage_format,
                    seed.measured_format,
                    seed.spectral_grade,
                    seed.spectral_subject,
                    seed.spectral_provenance,
                    192 if seed.has_v0_metrics else None,
                    205 if seed.has_v0_metrics else None,
                    198 if seed.has_v0_metrics else None,
                    seed.v0_subject,
                    seed.v0_provenance,
                    evidence_id,
                ),
            )
            self.db.conn.commit()

        row = self._require_request(request_id)
        if row["status"] != seed.status:
            raise AssertionError(
                f"census seed status drifted: {row['status']!r} != {seed.status!r}"
            )
        return request_id

    def seed_evidence_drift_release(
        self,
        release: BeetsWorldRelease,
        facts: EvidenceDriftFactSeed,
    ) -> int:
        """Materialize one linked lineage-1 fact shape from the live cohort."""

        request_id = self.add_release(release)
        if not self.import_request(
            request_id,
            codec=release.codec,
            verified_lossless=facts.verified_lossless,
        ):
            raise AssertionError(
                f"evidence drift seed import was rejected: {facts.name}"
            )
        evidence_id = self.db.get_request_current_evidence_id(request_id)
        evidence = self.db.load_album_quality_evidence_by_id(evidence_id)
        if evidence is None or evidence.id is None:
            raise AssertionError("evidence drift seed has no linked evidence")
        proof = evidence.verified_lossless_proof
        if facts.verified_lossless and proof is None:
            raise AssertionError("verified drift seed lost its source proof")
        self.db._execute(
            """
            UPDATE album_quality_evidence
            SET lineage_version = 1,
                spectral_grade = %s,
                spectral_bitrate_kbps = NULL,
                spectral_subject = %s,
                spectral_provenance = %s,
                v0_min_bitrate_kbps = %s,
                v0_avg_bitrate_kbps = %s,
                v0_median_bitrate_kbps = %s,
                v0_subject = %s,
                v0_provenance = %s,
                verified_lossless = %s,
                verified_lossless_provenance = %s,
                verified_lossless_source = %s,
                verified_lossless_classifier = %s,
                verified_lossless_detail = %s,
                on_disk_v0_research_attempted = FALSE,
                current_enrichment_required = FALSE
            WHERE id = %s
            """,
            (
                "genuine" if facts.spectral_subject is not None else None,
                facts.spectral_subject,
                facts.spectral_provenance,
                190 if facts.v0_subject is not None else None,
                200 if facts.v0_subject is not None else None,
                198 if facts.v0_subject is not None else None,
                facts.v0_subject,
                facts.v0_provenance,
                facts.verified_lossless,
                proof.provenance if proof is not None else None,
                proof.source if proof is not None else None,
                proof.classifier if proof is not None else None,
                proof.detail if proof is not None else None,
                evidence.id,
            ),
        )
        self.db.conn.commit()
        if not self.db.update_request_fields(
            request_id,
            expected_status=str(self._require_request(request_id)["status"]),
            verified_lossless=facts.verified_lossless,
        ):
            raise AssertionError("failed to apply drift request facts")
        return request_id

    def inject_evidence_drift(
        self,
        request_id: int,
        mutation: str,
        *,
        rename_codec_files: bool = False,
    ) -> None:
        """Make Beets/disk disagree with the request's linked evidence."""

        release = self._release_by_request[request_id]
        before = self._album_for_release(release.release_id)
        before_fingerprint = self._album_fingerprint(before)
        self.beets.mutate_release_out_of_band(
            release.release_id,
            mutation,
            rename_codec_files=rename_codec_files,
        )
        after = self._album_for_release(release.release_id)
        after_fingerprint = self._album_fingerprint(after)
        if before_fingerprint == after_fingerprint:
            raise AssertionError(
                f"drift mutation did not change the snapshot: {mutation}"
            )
        if "evidence_fingerprint_mismatch" not in {
            violation.code for violation in self.violations()
        }:
            raise AssertionError(
                f"world model did not observe injected drift: {mutation}"
            )

    def touch_current_evidence(
        self,
        request_id: int,
    ) -> CurrentEvidenceActionResult:
        """Run the production action-time rebuild against scratch Beets."""

        release = self._release_by_request[request_id]
        result = load_current_evidence_for_action(
            self.db,
            request_id=request_id,
            mb_release_id=release.release_id,
            beets_library_db_path=str(self.beets.library_db),
            beets_library_root=str(self.beets.library_root),
        )
        if result is None:
            raise AssertionError("drifted release disappeared from scratch Beets")
        return result

    def enrich_current_evidence(self, request_id: int) -> str:
        """Complete a changed installed snapshot through production helpers."""

        release = self._release_by_request[request_id]
        outcome = enrich_incomplete_current_evidence_for_request(
            self.db,
            request_id=request_id,
            mb_release_id=release.release_id,
            quality_ranks=QualityRankConfig.defaults(),
            beets_library_root=str(self.beets.library_root),
            spectral_analyzer=lambda _path: SpectralAnalysisDetail(
                attempted=True,
                grade="genuine",
                bitrate_kbps=96,
            ),
            probe_fn=lambda _path: V0ProbeEvidence(
                kind=V0_PROBE_ON_DISK_RESEARCH,
                min_bitrate_kbps=190,
                avg_bitrate_kbps=200,
                median_bitrate_kbps=198,
            ),
        )
        if outcome not in {"complete", "enriched"}:
            raise AssertionError(
                "production current-evidence enrichment did not converge: "
                f"request={request_id} outcome={outcome!r}"
            )
        current = self.touch_current_evidence(request_id)
        if not current.available:
            raise AssertionError(
                "production current-evidence enrichment stayed unavailable: "
                f"request={request_id} "
                f"reason={current.provenance.fallback_reason!r}"
            )
        return outcome

    def latest_download_outcome(self, request_id: int) -> str | None:
        """Return the newest audit outcome for one generated request."""

        for row in self.db.get_log(limit=100):
            if row.get("request_id") == request_id:
                outcome = row.get("outcome")
                return str(outcome) if outcome is not None else None
        return None

    def import_request(
        self,
        request_id: int,
        *,
        verified_lossless: bool = False,
        codec: str | None = None,
    ) -> bool:
        row = self._require_request(request_id)
        before_verified_proof = self._request_has_verified_proof(request_id)
        status = str(row["status"])
        release = self._release_by_request[request_id]
        attempt = replace(release, codec=codec or release.codec)
        before_album = self._album_for_release(release.release_id)
        is_upgrade = before_album is not None
        raw_previous_min_bitrate = row.get("min_bitrate")
        if is_upgrade and isinstance(raw_previous_min_bitrate, int):
            previous_min_bitrate = raw_previous_min_bitrate
        elif is_upgrade and raw_previous_min_bitrate is not None:
            raise AssertionError(
                "request min_bitrate has non-integer shape: "
                f"{raw_previous_min_bitrate!r}"
            )
        else:
            previous_min_bitrate = None
        if status != "wanted":
            raise AssertionError(
                f"request {request_id} cannot import from {status!r}"
            )

        require_transition_applied(finalize_request(
            self.db,
            request_id,
            RequestTransition.to_downloading(
                from_status="wanted",
                state_json=make_active_download_state_json([]),
            ),
        ))
        self._dispatch_counter += 1
        staged_path = (
            self.beets.incoming_root
            / f"dispatch-{request_id}-{self._dispatch_counter:04d}"
        )
        self.beets.stage_release(attempt, source_dir=staged_path)
        candidate_files = snapshot_audio_files(str(staged_path))
        min_bitrate = 900 if attempt.codec.casefold() == "flac" else 245
        measurement = AudioQualityMeasurement(
            min_bitrate_kbps=min_bitrate,
            avg_bitrate_kbps=min_bitrate,
            median_bitrate_kbps=min_bitrate,
            format=attempt.codec.upper(),
            is_cbr=False,
            spectral_grade="genuine",
            spectral_subject=EVIDENCE_SUBJECT_SOURCE,
            spectral_provenance=EVIDENCE_PROVENANCE_MEASURED,
        )
        proof = (
            VerifiedLosslessProof(
                provenance=EVIDENCE_PROVENANCE_MEASURED,
                source="flac",
                classifier="world_model",
                detail="generated verified lossless candidate",
            )
            if verified_lossless
            else None
        )
        candidate = make_album_quality_evidence(
            mb_release_id=attempt.release_id,
            source_path=str(staged_path),
            files=candidate_files,
            measurement=measurement,
            verified_lossless_proof=proof,
            codec=attempt.codec,
            container=attempt.codec,
            storage_format=attempt.codec.upper(),
        )
        self.db.upsert_album_quality_evidence(candidate)
        persisted_candidate = self.db.find_album_quality_evidence(
            mb_release_id=candidate.mb_release_id,
            snapshot_fingerprint=candidate.snapshot_fingerprint,
        )
        if persisted_candidate is None or persisted_candidate.id is None:
            raise AssertionError("world candidate evidence did not persist")
        origin_download_log_id = self.db.log_download(
            request_id,
            outcome="rejected",
            beets_detail="world-model candidate origin",
        )
        self.db.set_download_log_candidate_evidence(
            origin_download_log_id,
            persisted_candidate.id,
        )
        self.db.update_download_state_current_path(
            request_id,
            str(staged_path),
        )
        import_job = self.db.enqueue_import_job(
            IMPORT_JOB_AUTOMATION,
            request_id=request_id,
            dedupe_key=automation_import_dedupe_key(request_id),
            payload=automation_import_payload(),
            message="World-model automation import",
        )
        self.db.set_import_job_candidate_evidence(
            import_job.id,
            persisted_candidate.id,
        )
        self.db.mark_import_job_preview_importable(
            import_job.id,
            preview_result={"world_model": True},
        )
        claimed_job = self.db.claim_next_import_job(worker_id="world-model")
        if claimed_job is None or claimed_job.id != import_job.id:
            raise AssertionError("world automation import job was not claimable")
        candidate_result = CandidateEvidenceActionResult(
            evidence=persisted_candidate,
            provenance=ActionEvidenceProvenance(
                candidate_status="reused",
                snapshot_guard="matched",
            ),
        )
        current_before = load_current_evidence_for_action(
            self.db,
            request_id=request_id,
            mb_release_id=attempt.release_id,
            beets_library_db_path=str(self.beets.library_db),
            beets_library_root=str(self.beets.library_root),
        )
        if before_album is None and current_before is not None:
            raise AssertionError(
                "scratch Beets current evidence appeared before first import: "
                f"{current_before!r}"
            )

        def run_real_beets_import(**kwargs: Any) -> ImportOneRun:
            album = self.beets.import_staged_release(
                attempt,
                str(kwargs["path"]),
            )
            result = make_import_result(
                decision="import",
                new_min_bitrate=min_bitrate,
                prev_min_bitrate=previous_min_bitrate,
                imported_path=album.album_path,
                final_format=attempt.codec,
                verified_lossless=verified_lossless,
            )
            result = msgspec.structs.replace(
                result,
                source_measurement=measurement,
                verified_lossless_proof=proof,
            )
            return ImportOneRun(
                command=("in-process-beets-world",),
                returncode=0,
                stdout=result.to_sentinel_line(),
                stderr="",
                import_result=result,
            )

        outcome = dispatch_import_core(
            path=str(staged_path),
            mb_release_id=release.release_id,
            request_id=request_id,
            label=f"{release.artist} - {release.album}",
            beets_harness_path=(
                self._beets_harness_path
                if self._import_engine == "mirror-harness"
                else "in-process-beets-world"
            ),
            db=self.db,
            dl_info=DownloadInfo(filetype=attempt.codec),
            scenario="auto_import",
            force=False,
            run_import_fn=(
                self._run_beets_subprocess
                if self._import_engine == "mirror-harness"
                else run_real_beets_import
            ),
            candidate_import_job_id=claimed_job.id,
            candidate_download_log_id=origin_download_log_id,
            prevalidated_candidate_result=candidate_result,
            requeue_on_failure=True,
            beets_library_db_path=str(self.beets.library_db),
            beets_library_root=str(self.beets.library_root),
        )
        finalize_claimed_dispatch(self.db, claimed_job, outcome)
        after_row = self._require_request(request_id)
        after_album = self._album_for_release(release.release_id)
        self._transitions.append(LifecycleTransitionSnapshot(
            request_id=request_id,
            operation="upgrade_import" if is_upgrade else "import",
            before_status=str(row["status"]),
            after_status=str(after_row["status"]),
            before_release_id=release.release_id,
            after_release_id=str(after_row["mb_release_id"]),
            before_override=(
                str(row["search_filetype_override"])
                if row.get("search_filetype_override") is not None
                else None
            ),
            after_override=(
                str(after_row["search_filetype_override"])
                if after_row.get("search_filetype_override") is not None
                else None
            ),
            before_album_fingerprint=self._album_fingerprint(before_album),
            after_album_fingerprint=self._album_fingerprint(after_album),
            before_verified_lossless=before_verified_proof,
        ))
        if not outcome.success and outcome.code not in {
            "quality_pipeline_rejected",
            "have_analysis_error",
        }:
            subprocess_detail = self._subprocess_failure_detail()
            raise AssertionError(
                "production dispatch rejected world import for request "
                f"{request_id}: code={outcome.code!r} message={outcome.message!r} "
                f"{subprocess_detail}"
            )
        if staged_path.exists() and outcome.success:
            raise AssertionError(
                f"production dispatch left staged path behind: {staged_path}"
            )
        return outcome.success

    def force_import_request(
        self,
        request_id: int,
        *,
        codec: str | None = None,
        verified_lossless: bool = False,
    ) -> bool:
        """Drive the production force-import entry point over a real source."""

        row = self._require_request(request_id)
        before_verified_proof = self._request_has_verified_proof(request_id)
        release = self._release_by_request[request_id]
        attempt = replace(release, codec=codec or release.codec)
        before_album = self._album_for_release(release.release_id)
        self._dispatch_counter += 1
        origin = (
            self.beets.root
            / "slskd"
            / f"force-{request_id}-{self._dispatch_counter:04d}"
        )
        source = (
            self.beets.root
            / "failed_imports"
            / f"force-{request_id}-{self._dispatch_counter:04d}"
        )
        self.beets.stage_release(attempt, source_dir=origin)
        candidate_files = snapshot_audio_files(str(origin))
        min_bitrate = 900 if attempt.codec.casefold() == "flac" else 245
        measurement = AudioQualityMeasurement(
            min_bitrate_kbps=min_bitrate,
            avg_bitrate_kbps=min_bitrate,
            median_bitrate_kbps=min_bitrate,
            format=attempt.codec.upper(),
            is_cbr=False,
            spectral_grade="genuine",
            spectral_subject=EVIDENCE_SUBJECT_SOURCE,
            spectral_provenance=EVIDENCE_PROVENANCE_MEASURED,
        )
        proof = (
            VerifiedLosslessProof(
                provenance=EVIDENCE_PROVENANCE_MEASURED,
                source="flac",
                classifier="world_model",
                detail="generated force-import proof",
            )
            if verified_lossless
            else None
        )
        candidate = make_album_quality_evidence(
            mb_release_id=attempt.release_id,
            source_path=str(origin),
            files=candidate_files,
            measurement=measurement,
            verified_lossless_proof=proof,
            codec=attempt.codec,
            container=attempt.codec,
            storage_format=attempt.codec.upper(),
        )
        self.db.upsert_album_quality_evidence(candidate)
        persisted = self.db.find_album_quality_evidence(
            mb_release_id=candidate.mb_release_id,
            snapshot_fingerprint=candidate.snapshot_fingerprint,
        )
        if persisted is None or persisted.id is None:
            raise AssertionError("force candidate evidence did not persist")
        source.parent.mkdir(parents=True, exist_ok=True)
        origin.rename(source)
        download_log_id = self.db.log_download(
            request_id,
            soulseek_username=f"force-peer-{self._dispatch_counter}",
            outcome="rejected",
            validation_result=msgspec.json.encode({
                "scenario": "high_distance",
                "failed_path": str(source),
            }).decode(),
        )
        self.db.set_download_log_candidate_evidence(
            download_log_id,
            persisted.id,
        )
        import_job = self.db.enqueue_import_job(
            IMPORT_JOB_FORCE,
            request_id=request_id,
            dedupe_key=force_import_dedupe_key(download_log_id),
            payload=force_import_payload(
                download_log_id=download_log_id,
                failed_path=str(source),
                source_username=f"force-peer-{self._dispatch_counter}",
            ),
            message="World-model force import",
        )
        self.db.set_import_job_candidate_evidence(import_job.id, persisted.id)
        self.db.mark_import_job_preview_importable(
            import_job.id,
            preview_result={"world_model": True},
        )
        claimed_job = self.db.claim_next_import_job(worker_id="world-model")
        if claimed_job is None or claimed_job.id != import_job.id:
            raise AssertionError("world force-import job was not claimable")
        raw_previous_min_bitrate = row.get("min_bitrate")
        previous_min_bitrate = (
            int(raw_previous_min_bitrate)
            if before_album is not None
            and isinstance(raw_previous_min_bitrate, int)
            else None
        )

        def run_real_beets_import(**kwargs: Any) -> ImportOneRun:
            album = self.beets.import_staged_release(
                attempt,
                str(kwargs["path"]),
            )
            result = make_import_result(
                decision="import",
                new_min_bitrate=min_bitrate,
                prev_min_bitrate=previous_min_bitrate,
                imported_path=album.album_path,
                final_format=attempt.codec,
                verified_lossless=verified_lossless,
            )
            result = msgspec.structs.replace(
                result,
                source_measurement=measurement,
                verified_lossless_proof=proof,
            )
            return ImportOneRun(
                command=("in-process-force-beets-world",),
                returncode=0,
                stdout=result.to_sentinel_line(),
                stderr="",
                import_result=result,
            )

        outcome = dispatch_import_from_db(
            self.db,
            request_id,
            str(source),
            source_username=f"force-peer-{self._dispatch_counter}",
            import_job_id=claimed_job.id,
            download_log_id=download_log_id,
            cfg=CratediggerConfig(
                beets_harness_path=(
                    self._beets_harness_path
                    if self._import_engine == "mirror-harness"
                    else "in-process-force-beets-world"
                ),
            ),
            run_import_fn=(
                self._run_beets_subprocess
                if self._import_engine == "mirror-harness"
                else run_real_beets_import
            ),
            beets_library_db_path=str(self.beets.library_db),
            beets_library_root=str(self.beets.library_root),
        )
        finalize_claimed_dispatch(self.db, claimed_job, outcome)
        after = self._require_request(request_id)
        after_album = self._album_for_release(release.release_id)
        self._transitions.append(LifecycleTransitionSnapshot(
            request_id=request_id,
            operation="force_import",
            before_status=str(row["status"]),
            after_status=str(after["status"]),
            before_release_id=release.release_id,
            after_release_id=str(after["mb_release_id"]),
            before_override=self._optional_string(
                row.get("search_filetype_override")
            ),
            after_override=self._optional_string(
                after.get("search_filetype_override")
            ),
            before_album_fingerprint=self._album_fingerprint(before_album),
            after_album_fingerprint=self._album_fingerprint(after_album),
            before_verified_lossless=before_verified_proof,
        ))
        if not outcome.success and outcome.code not in {
            "quality_pipeline_rejected",
            "have_analysis_error",
        }:
            subprocess_detail = self._subprocess_failure_detail()
            raise AssertionError(
                "production force import failed operationally: "
                f"code={outcome.code!r} message={outcome.message!r} "
                f"{subprocess_detail}"
            )
        return outcome.success

    def replace_request(self, request_id: int) -> int:
        """Supersede one request through the production Replace service."""

        before = self._require_request(request_id)
        before_verified_proof = self._request_has_verified_proof(request_id)
        source = self._release_by_request[request_id]
        before_album = self._album_for_release(source.release_id)
        target = replace(
            source,
            release_id=self._next_replacement_id(source.release_id),
            year=min(source.year + 1, 2026),
            label=f"{source.label or 'Archive'} Replacement",
            catalognum=f"{source.catalognum or 'WORLD'}-R{self._operator_counter}",
        )
        group_id = str(before["mb_release_group_id"])
        target_payload = {
            "id": target.release_id,
            "title": target.album,
            "artist_name": target.artist,
            "artist_id": "world-model-artist",
            "release_group_id": group_id,
            "year": target.year,
            "country": "AU",
            "tracks": [
                {
                    "disc_number": 1,
                    "track_number": track,
                    "title": f"Track {track}",
                }
                for track in range(1, target.track_count + 1)
            ],
        }

        def mb_lookup(mbid: str, *, fresh: bool = False) -> dict[str, Any]:
            del fresh
            if mbid == target.release_id:
                return target_payload
            return {"id": mbid, "release_group_id": group_id}

        def discogs_lookup(
            release_id: int,
            *,
            fresh: bool = False,
        ) -> dict[str, Any]:
            del fresh
            if str(release_id) == target.release_id:
                return target_payload
            return {"id": str(release_id), "release_group_id": group_id}

        cfg = CratediggerConfig(
            beets_staging_dir=str(self.beets.incoming_root),
        )
        with BeetsDB(
            str(self.beets.library_db),
            library_root=str(self.beets.library_root),
        ) as current_beets:
            service = MbidReplaceService(
                db=self.db,
                config=cfg,
                beets_db_factory=lambda: current_beets,
                mb_lookup=mb_lookup,
                discogs_lookup=discogs_lookup,
                search_plan_service=SearchPlanService(self.db, cfg),
                beets_delete_fn=self._delete_release,
            )
            result = service.replace_request_mbid(
                request_id,
                target_mb_release_id=target.release_id,
            )
        if result.outcome != RESULT_REPLACED or result.new_request_id is None:
            raise AssertionError(
                f"production Replace failed: {result!r}"
            )
        new_request_id = result.new_request_id
        self._release_by_request[new_request_id] = target
        frozen = self._require_request(request_id)
        self._replaced_snapshots[request_id] = dict(frozen)
        after_album = self._album_for_release(source.release_id)
        self._transitions.append(LifecycleTransitionSnapshot(
            request_id=request_id,
            operation="replace_request",
            before_status=str(before["status"]),
            after_status=str(frozen["status"]),
            before_release_id=source.release_id,
            after_release_id=str(frozen["mb_release_id"]),
            before_override=self._optional_string(
                before.get("search_filetype_override")
            ),
            after_override=self._optional_string(
                frozen.get("search_filetype_override")
            ),
            before_album_fingerprint=self._album_fingerprint(before_album),
            after_album_fingerprint=self._album_fingerprint(after_album),
            before_verified_lossless=before_verified_proof,
            descendant_request_id=new_request_id,
        ))
        if after_album is not None:
            raise AssertionError("Replace left the superseded pressing in Beets")
        return new_request_id

    def ban_request_source(self, request_id: int) -> None:
        """Ban one installed source through the real destructive service."""

        before = self._require_request(request_id)
        before_verified_proof = self._request_has_verified_proof(request_id)
        release = self._release_by_request[request_id]
        before_album = self._album_for_release(release.release_id)
        if before_album is None:
            raise AssertionError(
                "ban-source world rule requires an installed album"
            )
        self._operator_counter += 1
        username = f"world-peer-{self._operator_counter}"
        self.db.log_download(
            request_id,
            soulseek_username=username,
            outcome="success",
        )
        with BeetsDB(
            str(self.beets.library_db),
            library_root=str(self.beets.library_root),
        ) as beets_db:
            result = ban_source(
                pipeline_db=self.db,
                beets_db=beets_db,
                request=BanSourceRequest(
                    request_id=request_id,
                    expected_release_id=release.release_id,
                ),
                beets_delete_fn=self._delete_release,
            )
        if isinstance(result, BanSourceReleaseMismatch):
            if not (
                before.get("mb_release_id")
                and before.get("discogs_release_id")
            ):
                raise AssertionError(
                    f"single-identity ban-source mismatch: {result!r}"
                )
            if self._require_request(request_id) != before:
                raise AssertionError("failed-closed dual-identity ban mutated request")
            if self._album_fingerprint(
                self._album_for_release(release.release_id)
            ) != self._album_fingerprint(before_album):
                raise AssertionError("failed-closed dual-identity ban mutated Beets")
            return
        if not isinstance(result, BanSourceSuccess):
            raise AssertionError(f"production ban-source failed: {result!r}")
        after = self._require_request(request_id)
        after_album = self._album_for_release(release.release_id)
        self._transitions.append(LifecycleTransitionSnapshot(
            request_id=request_id,
            operation="ban_source",
            before_status=str(before["status"]),
            after_status=str(after["status"]),
            before_release_id=release.release_id,
            after_release_id=str(after["mb_release_id"]),
            before_override=self._optional_string(
                before.get("search_filetype_override")
            ),
            after_override=self._optional_string(
                after.get("search_filetype_override")
            ),
            before_album_fingerprint=self._album_fingerprint(before_album),
            after_album_fingerprint=self._album_fingerprint(after_album),
            before_verified_lossless=before_verified_proof,
        ))
        if after_album is not None:
            raise AssertionError("ban-source left the banned release in Beets")
        if not any(
            row.get("username") == username
            for row in self.db.get_denylisted_users(request_id)
        ):
            raise AssertionError("ban-source did not persist source authority")

    def delete_wrong_match(self, request_id: int) -> None:
        """Create then manually delete a real failed-import source."""

        release = self._release_by_request[request_id]
        self._operator_counter += 1
        source = (
            self.beets.root
            / "failed_imports"
            / f"wrong-match-{request_id}-{self._operator_counter}"
        )
        self.beets.stage_release(release, source_dir=source)
        log_id = self.db.log_download(
            request_id,
            soulseek_username=f"wrong-peer-{self._operator_counter}",
            outcome="rejected",
            validation_result=msgspec.json.encode({
                "scenario": "high_distance",
                "failed_path": str(source),
            }).decode(),
        )
        result = delete_wrong_match_source(
            self.db,
            log_id,
            require_visible=True,
        )
        if not result.success:
            raise AssertionError(
                f"production wrong-match delete failed: {result!r}"
            )
        if source.exists():
            raise AssertionError(
                f"wrong-match source survived deletion: {source}"
            )
        entry = self.db.get_download_log_entry(log_id)
        if entry is None or decode_validation_envelope(
            entry.get("validation_result")
        ).failed_path:
            raise AssertionError("wrong-match audit still claims the deleted path")

    def request_ids_with_status(self, status: str) -> list[int]:
        return [
            request_id
            for request_id in sorted(self._release_by_request)
            if self._require_request(request_id)["status"] == status
        ]

    def request_ids_with_album(self) -> list[int]:
        return [
            request_id
            for request_id, release in sorted(self._release_by_request.items())
            if self._album_for_release(release.release_id) is not None
            and self._require_request(request_id)["status"] != "replaced"
        ]

    def request_ids_for_evidence_drift(
        self,
        mutation: EvidenceDriftMutationSeed,
    ) -> list[int]:
        """Return linked installed requests that can accept this mutation."""

        candidates: list[int] = []
        for request_id in self.request_ids_with_album():
            if self.db.get_request_current_evidence_id(request_id) is None:
                continue
            release = self._release_by_request[request_id]
            album = self._album_for_release(release.release_id)
            if album is None:
                continue
            if (
                mutation.mutation == "file_count_drift"
                and len(album.item_paths) < 2
            ):
                continue
            if (
                mutation.mutation == "codec_replacement"
                and any(
                    Path(path).suffix.casefold()
                    != f".{mutation.initial_codec}"
                    for path in album.item_paths
                )
            ):
                continue
            candidates.append(request_id)
        return candidates

    def active_request_ids(self) -> list[int]:
        return [
            request_id
            for request_id in sorted(self._release_by_request)
            if self._require_request(request_id)["status"] != "replaced"
        ]

    def verified_lossless_request_ids(self) -> list[int]:
        return [
            request_id
            for request_id in self.active_request_ids()
            if self._request_has_verified_proof(request_id)
            and self._album_for_release(
                self._release_by_request[request_id].release_id
            ) is not None
        ]

    def reset_to_wanted(self, request_id: int) -> None:
        row = self._require_request(request_id)
        before_verified_proof = self._request_has_verified_proof(request_id)
        release = self._release_by_request[request_id]
        before_album = self._album_for_release(release.release_id)
        raw_override = row.get("search_filetype_override")
        fields: dict[str, object] = {
            "search_filetype_override": resolve_user_requeue_override(
                raw_override if isinstance(raw_override, str) else None
            ),
        }
        if row.get("min_bitrate") is not None:
            fields["min_bitrate"] = row["min_bitrate"]
        require_transition_applied(finalize_request(
            self.db,
            request_id,
            RequestTransition.to_wanted_fields(
                from_status=str(row["status"]),
                fields=fields,
            ),
        ))
        after = self._require_request(request_id)
        self._transitions.append(LifecycleTransitionSnapshot(
            request_id=request_id,
            operation="reset_to_wanted",
            before_status=str(row["status"]),
            after_status=str(after["status"]),
            before_release_id=release.release_id,
            after_release_id=str(after["mb_release_id"]),
            before_override=(
                str(row["search_filetype_override"])
                if row.get("search_filetype_override") is not None
                else None
            ),
            after_override=(
                str(after["search_filetype_override"])
                if after.get("search_filetype_override") is not None
                else None
            ),
            before_album_fingerprint=self._album_fingerprint(before_album),
            after_album_fingerprint=self._album_fingerprint(
                self._album_for_release(release.release_id)
            ),
            before_verified_lossless=before_verified_proof,
        ))

    def violations(self) -> tuple[WorldViolation, ...]:
        albums = self.beets.snapshots()
        requests: list[RequestMembershipSnapshot] = []
        evidence: list[EvidenceDiskSnapshot] = []
        denylist_rows: list[DenylistAuthoritySnapshot] = []
        for row in self.db.list_non_replaced_requests():
            identity = ReleaseIdentity.from_fields(
                row.get("mb_release_id"),
                row.get("discogs_release_id"),
            )
            if identity is None:
                raise AssertionError(
                    f"request {row['id']} lost its exact release identity"
                )
            requests.append(RequestMembershipSnapshot(
                request_id=int(row["id"]),
                release_id=identity.release_id,
                status=str(row["status"]),
                imported_path=(
                    str(row["imported_path"])
                    if row.get("imported_path") is not None
                    else None
                ),
            ))
            album = next(
                (a for a in albums if a.release_id == identity.release_id),
                None,
            )
            current_id_raw = row.get("current_evidence_id")
            current_id = (
                int(current_id_raw)
                if isinstance(current_id_raw, int)
                else None
            )
            linked = self.db.load_album_quality_evidence_by_id(current_id)
            evidence.append(EvidenceDiskSnapshot(
                request_id=int(row["id"]),
                release_id=identity.release_id,
                status=str(row["status"]),
                album_path=album.album_path if album is not None else None,
                current_evidence_id=current_id,
                evidence_id=linked.id if linked is not None else None,
                evidence_release_id=(
                    linked.mb_release_id if linked is not None else None
                ),
                evidence_source_path=(
                    linked.source_path if linked is not None else None
                ),
                evidence_fingerprint=(
                    linked.snapshot_fingerprint if linked is not None else None
                ),
                actual_fingerprint=(
                    self._album_fingerprint(album)
                    if album is not None
                    else None
                ),
            ))
        for request_id in sorted(self._release_by_request):
            history = self.db.get_download_history(request_id)
            denylist = sorted(
                self.db.get_denylisted_users(request_id),
                key=lambda row: str(row.get("username") or ""),
            )
            for denylist_row in denylist:
                username = str(denylist_row.get("username") or "")
                decisions = derive_denylist_authorities(
                    username=username,
                    reason=str(denylist_row.get("reason") or ""),
                    history=history,
                )
                denylist_rows.append(DenylistAuthoritySnapshot(
                    request_id=request_id,
                    username=username,
                    authorizing_decisions=decisions,
                ))
        return (
            *check_folder_exclusivity(albums),
            *check_library_filesystem(albums),
            *check_status_membership(tuple(requests), albums),
            *check_evidence_disk_coherence(tuple(evidence)),
            *check_proof_lock_terminality(tuple(self._transitions)),
            *check_no_lossy_tier_widening(tuple(self._transitions)),
            *check_denylist_authority(tuple(denylist_rows)),
        )

    def assert_invariants(self) -> None:
        for request_id, snapshot in self._replaced_snapshots.items():
            assert_replaced_row_frozen(
                snapshot,
                self._require_request(request_id),
            )
        violations = self.violations()
        if violations:
            rendered = "\n".join(
                f"{violation.code}: {violation.detail}"
                for violation in violations
            )
            raise AssertionError(f"cross-engine world violations:\n{rendered}")

    def _require_request(self, request_id: int) -> "AlbumRequestRow":
        row = self.db.get_request(request_id)
        if row is None:
            raise AssertionError(f"request {request_id} disappeared")
        return row

    def _album_for_release(
        self,
        release_id: str,
    ) -> LibraryAlbumSnapshot | None:
        return next(
            (
                album
                for album in self.beets.snapshots()
                if album.release_id == release_id
            ),
            None,
        )

    @staticmethod
    def _album_fingerprint(album: LibraryAlbumSnapshot | None) -> str | None:
        if album is None:
            return None
        return snapshot_fingerprint(snapshot_audio_files(album.album_path))

    @staticmethod
    def _optional_string(value: object) -> str | None:
        return str(value) if value is not None else None

    def _next_replacement_id(self, source_release_id: str) -> str:
        self._operator_counter += 1
        identity = ReleaseIdentity.from_id(source_release_id)
        if identity is None:
            raise AssertionError(f"invalid source identity {source_release_id!r}")
        if identity.source == "discogs":
            return str(9_000_000 + self._operator_counter)
        return (
            "ffffffff-ffff-4fff-8fff-"
            f"{self._operator_counter:012x}"
        )

    def _delete_release(
        self,
        request: BeetsDeleteRequest,
    ) -> BeetsDeleteCompleted | BeetsDeleteFailed:
        album = self.beets.library.get_album(request.album_id)
        if album is None:
            return BeetsDeleteFailed(
                album_id=request.album_id,
                reason="album_not_found",
                detail="world-model exact album is absent",
                album_still_present=False,
            )
        snapshot = self.beets.snapshot_album(album)
        tracks = len(snapshot.item_paths)
        album_name = str(album.get("album") or "")
        artist_name = str(album.get("albumartist") or "")
        album.remove(delete=True)
        return BeetsDeleteCompleted(
            album_id=request.album_id,
            album_name=album_name,
            artist_name=artist_name,
            former_album_path=snapshot.album_path,
            deleted_tracks=tracks,
            deleted_artifacts=tracks,
            preserved_paths=(),
        )

    def _run_beets_subprocess(self, **kwargs: Any) -> ImportOneRun:
        """Run the production subprocess boundary against scratch Beets only."""

        subprocess_kwargs = dict(kwargs)
        # The outer dispatch owns the ephemeral PG transition. Production's
        # import_one compatibility write would duplicate it; omitting the ID
        # keeps this profile focused on the real Beets/harness boundary.
        subprocess_kwargs["request_id"] = None
        with self.beets.subprocess_environment():
            run = run_import_one(**subprocess_kwargs)
        self._last_subprocess_run = run
        return run

    def _subprocess_failure_detail(self) -> str:
        run = self._last_subprocess_run
        if run is None:
            return ""
        stderr_tail = run.stderr[-4000:].strip()
        return (
            f"subprocess_rc={run.returncode} stderr_tail={stderr_tail!r}"
        )

    def _request_has_verified_proof(self, request_id: int) -> bool:
        evidence_id = self.db.get_request_current_evidence_id(request_id)
        evidence = self.db.load_album_quality_evidence_by_id(evidence_id)
        return (
            evidence is not None
            and evidence.verified_lossless_proof is not None
        )

def repository_root() -> Path:
    return Path(__file__).resolve().parents[2]


__all__ = ["LifecycleWorld", "repository_root"]
