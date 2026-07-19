"""Real PostgreSQL + real Beets support for the heavyweight world model."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from lib.dispatch import dispatch_import_core
from lib.dispatch.types import EvidenceImportGate, ImportOneRun
from lib.quality import DownloadInfo
from lib.release_identity import ReleaseIdentity
from lib.transitions import (
    RequestTransition,
    finalize_request,
    require_transition_applied,
)
from lib.world_invariants import (
    RequestMembershipSnapshot,
    WorldViolation,
    check_folder_exclusivity,
    check_library_filesystem,
    check_status_membership,
)
from tests.beets_world import BeetsWorld, BeetsWorldRelease
from tests.helpers import make_active_download_state_json, make_import_result
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
            discogs_release_id=(
                identity.release_id
                if identity.source == "discogs"
                else None
            ),
        ))
        self._release_by_request[request_id] = release
        return request_id

    def import_request(self, request_id: int) -> None:
        row = self._require_request(request_id)
        status = str(row["status"])
        is_upgrade = status == "imported"
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
        if status == "imported":
            require_transition_applied(finalize_request(
                self.db,
                request_id,
                RequestTransition.to_wanted(from_status="imported"),
            ))
            status = "wanted"
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
        release = self._release_by_request[request_id]
        self._dispatch_counter += 1
        staged_path = (
            self.beets.incoming_root
            / f"dispatch-{request_id}-{self._dispatch_counter:04d}"
        )

        def run_real_beets_import(**kwargs: Any) -> ImportOneRun:
            album = self.beets.import_release(
                release,
                source_dir=str(kwargs["path"]),
            )
            result = make_import_result(
                decision="import",
                new_min_bitrate=900 + self._dispatch_counter,
                prev_min_bitrate=previous_min_bitrate,
                imported_path=album.album_path,
                final_format=release.codec,
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
            dl_info=DownloadInfo(filetype=release.codec),
            scenario="auto_import",
            quality_gate_fn=self._noop_quality_gate,
            run_import_fn=run_real_beets_import,
            evidence_gate_fn=self._empty_evidence_gate,
            beets_library_db_path=str(self.beets.library_db),
            beets_library_root=str(self.beets.library_root),
        )
        if not outcome.success:
            raise AssertionError(
                "production dispatch rejected world import for request "
                f"{request_id}: {outcome.code or outcome.message}"
            )
        if staged_path.exists():
            raise AssertionError(
                f"production dispatch left staged path behind: {staged_path}"
            )

    def request_ids_with_status(self, status: str) -> list[int]:
        return [
            request_id
            for request_id in sorted(self._release_by_request)
            if self._require_request(request_id)["status"] == status
        ]

    def violations(self) -> tuple[WorldViolation, ...]:
        albums = self.beets.snapshots()
        requests: list[RequestMembershipSnapshot] = []
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
        return (
            *check_folder_exclusivity(albums),
            *check_library_filesystem(albums),
            *check_status_membership(tuple(requests), albums),
        )

    def assert_invariants(self) -> None:
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

    @staticmethod
    def _empty_evidence_gate(
        *_args: object,
        **_kwargs: object,
    ) -> EvidenceImportGate:
        # Evidence lifecycle rules arrive in PR2. PR1 still runs the real
        # post-import scratch-Beets evidence refresh through dispatch.
        return EvidenceImportGate()

    @staticmethod
    def _noop_quality_gate(**_kwargs: object) -> None:
        # Proof-lock/terminal quality policy is PR2's invariant tranche.
        return None


def repository_root() -> Path:
    return Path(__file__).resolve().parents[2]


__all__ = ["LifecycleWorld", "repository_root"]
