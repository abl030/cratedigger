"""Real PostgreSQL + real Beets support for the heavyweight world model."""

from __future__ import annotations

import os
from dataclasses import replace
from pathlib import Path
from typing import Any

import msgspec

from lib.beets_db import BeetsDB
from lib.config import CratediggerConfig
from lib.destructive_release_service import (
    BanSourceRequest,
    BanSourceSuccess,
    ban_source,
)
from lib.dispatch import dispatch_import_core, dispatch_import_from_db
from lib.dispatch.types import ImportOneRun
from lib.import_evidence import (
    ActionEvidenceProvenance,
    CandidateEvidenceActionResult,
    load_current_evidence_for_action,
)
from lib.quality import (
    EVIDENCE_PROVENANCE_MEASURED,
    EVIDENCE_SUBJECT_SOURCE,
    AudioQualityMeasurement,
    DownloadInfo,
    VerifiedLosslessProof,
    resolve_user_requeue_override,
)
from lib.quality_evidence import snapshot_audio_files, snapshot_fingerprint
from lib.mbid_replace_service import MbidReplaceService, RESULT_REPLACED
from lib.release_cleanup import ReleaseCleanupResult
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
    make_active_download_state_json,
    make_album_quality_evidence,
    make_import_result,
)
from tests.test_pipeline_db import TEST_DSN, make_db


class LifecycleWorld:
    """One disposable pipeline DB slate coupled to one Beets library."""

    def __init__(self, dsn: str, repo_root: str | os.PathLike[str]) -> None:
        if TEST_DSN != dsn:
            raise ValueError(
                "world model must use tests.test_pipeline_db.make_db() "
                "against its ephemeral TEST_DB_DSN"
            )
        self.db = make_db()
        try:
            self.beets = BeetsWorld(repo_root)
        except BaseException:
            self.db.close()
            raise
        self._release_by_request: dict[int, BeetsWorldRelease] = {}
        self._dispatch_counter = 0
        self._operator_counter = 0
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
                "title": f"Track {track}",
            }
            for track in range(1, release.track_count + 1)
        ])
        self._release_by_request[request_id] = release
        return request_id

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
            beets_harness_path="in-process-beets-world",
            db=self.db,
            dl_info=DownloadInfo(filetype=attempt.codec),
            scenario="auto_import",
            force=False,
            run_import_fn=run_real_beets_import,
            candidate_download_log_id=origin_download_log_id,
            prevalidated_candidate_result=candidate_result,
            requeue_on_failure=True,
            beets_library_db_path=str(self.beets.library_db),
            beets_library_root=str(self.beets.library_root),
        )
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
        if not outcome.success and outcome.code != "quality_pipeline_rejected":
            raise AssertionError(
                "production dispatch rejected world import for request "
                f"{request_id}: code={outcome.code!r} message={outcome.message!r} "
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
        source = (
            self.beets.root
            / "failed_imports"
            / f"force-{request_id}-{self._dispatch_counter:04d}"
        )
        self.beets.stage_release(attempt, source_dir=source)
        candidate_files = snapshot_audio_files(str(source))
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
            source_path=str(source),
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
            download_log_id=download_log_id,
            cfg=CratediggerConfig(
                beets_harness_path="in-process-force-beets-world",
            ),
            run_import_fn=run_real_beets_import,
            beets_library_db_path=str(self.beets.library_db),
            beets_library_root=str(self.beets.library_root),
        )
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
        if not outcome.success and outcome.code != "quality_pipeline_rejected":
            raise AssertionError(
                "production force import failed operationally: "
                f"code={outcome.code!r} message={outcome.message!r}"
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
        service = MbidReplaceService(
            db=self.db,
            config=cfg,
            beets_db_factory=lambda: self.beets,
            mb_lookup=mb_lookup,
            discogs_lookup=discogs_lookup,
            search_plan_service=SearchPlanService(self.db, cfg),
            remove_release_fn=self._remove_release,
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
                cleanup_release_fn=self._remove_release,
            )
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

    def _require_request(self, request_id: int) -> dict[str, object]:
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

    def _remove_release(self, **kwargs: Any) -> ReleaseCleanupResult:
        release_id = str(kwargs["release_id"])
        request_id = int(kwargs["request_id"])
        removed = self.beets.remove_release(release_id)
        absent_after = self._album_for_release(release_id) is None
        if absent_after and bool(kwargs.get("clear_pipeline_state", True)):
            self.db.clear_on_disk_quality_fields(request_id)
        return ReleaseCleanupResult(
            beets_removed=removed > 0 and absent_after,
            absent_after=absent_after,
            selector_failures=(),
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
