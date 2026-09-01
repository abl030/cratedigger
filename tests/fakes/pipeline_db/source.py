"""``FakePipelineDBSource`` — the typed PipelineDBSource fake."""
from __future__ import annotations

from collections.abc import (
    Sequence,
)
from typing import (
    TYPE_CHECKING,
    Any,
    cast,
)

if TYPE_CHECKING:
    from cratedigger import TrackRecord
from lib import transitions
from tests.fakes.pipeline_db._db import FakePipelineDB


class FakePipelineDBSource:
    """Typed stand-in for ``album_source.DatabaseSource`` / similar.

    Production calls ``ctx.pipeline_db_source._get_db()`` (and a handful of
    higher-level methods) to reach the pipeline DB. Tests historically
    constructed this with ``MagicMock`` and ``source._get_db.return_value
    = ...``; replace with this typed fake so the surface is explicit and
    the test fails loudly if production calls an unexpected method.

    Surface mirrors the production source's six public callables:
    ``_get_db``, ``get_tracks``, ``get_wanted_searchable``, ``mark_done``,
    ``reject_and_requeue``, ``close``. The fake's behavior is intentionally
    minimal — tests that exercise real DB activity should rely on the
    underlying ``FakePipelineDB`` directly (via ``source.db``).
    """

    def __init__(self, db: FakePipelineDB | None = None) -> None:
        self.db: FakePipelineDB = db if db is not None else FakePipelineDB()
        # Call records — empty unless production reached a method.
        self.get_tracks_calls: list[Any] = []
        self.mark_done_calls: list[dict[str, Any]] = []
        self.reject_and_requeue_calls: list[dict[str, Any]] = []
        self.close_calls: int = 0
        # Test-configurable returns for the wanted iterator. Default empty
        # so the worker pipeline observes "nothing to do."
        self._wanted_searchable: list[Any] = []

    def _get_db(self) -> FakePipelineDB:
        return self.db

    def get_tracks(self, album_record: Any) -> list[TrackRecord]:
        self.get_tracks_calls.append(album_record)
        request_id = getattr(album_record, "db_request_id", None)
        if not request_id:
            return []
        rows = self.db._tracks.get(request_id, [])
        album_id = request_id * -1
        out: list[TrackRecord] = []
        for t in rows:
            out.append({
                "title": t["title"],
                "trackNumber": str(t.get("track_number") or ""),
                "mediumNumber": t["disc_number"],
                "duration": int((t.get("length_seconds") or 0) * 10_000_000),
                "id": 0,
                "albumId": album_id,
            })
        return out

    def set_wanted_searchable(self, records: list[Any]) -> None:
        """Configure what ``get_wanted_searchable`` returns."""
        self._wanted_searchable = list(records)

    def get_wanted_searchable(
        self,
        generator_id: str,
        limit: int | None = None,
        *,
        title_blacklist: Sequence[str] = (),
    ) -> list[Any]:
        del generator_id, title_blacklist
        if limit is None:
            return list(self._wanted_searchable)
        return list(self._wanted_searchable[:limit])

    def mark_done(
        self,
        album_record: Any,
        bv_result: Any,
        dest_path: Any = None,
        download_info: Any = None,
        import_job_id: int | None = None,
    ) -> Any:
        call = {
            "album_record": album_record,
            "bv_result": bv_result,
            "dest_path": dest_path,
            "download_info": download_info,
        }
        if import_job_id is not None:
            call["import_job_id"] = import_job_id
        self.mark_done_calls.append(call)
        if import_job_id is None or self.db.get_import_job(import_job_id) is None:
            return None
        from lib.dispatch import _do_mark_done
        from lib.quality import DownloadInfo

        request_id = getattr(album_record, "db_request_id", None)
        if not isinstance(request_id, int):
            return None
        dl_info = (
            download_info
            if isinstance(download_info, DownloadInfo)
            else DownloadInfo()
        )
        return _do_mark_done(
            cast(Any, self.db),
            request_id,
            dl_info,
            distance=getattr(bv_result, "distance", None),
            scenario=getattr(bv_result, "scenario", None),
            dest_path=dest_path,
            detail=getattr(bv_result, "detail", None),
            import_job_id=import_job_id,
        )

    def reject_and_requeue(
        self,
        album_record: Any,
        bv_result: Any,
        usernames: Any = None,
        download_info: Any = None,
        search_filetype_override: Any = None,
        cooled_down_users: set[str] | None = None,
        import_job_id: int | None = None,
    ) -> Any:
        self.reject_and_requeue_calls.append({
            "album_record": album_record,
            "bv_result": bv_result,
            "usernames": usernames,
            "download_info": download_info,
            "search_filetype_override": search_filetype_override,
            "cooled_down_users": cooled_down_users,
        })
        # Issue #1077, R4-5 (round-4 review): mirror ``album_source.
        # DatabaseSource.reject_and_requeue`` exactly — ONE falsy
        # ``request_id`` gate before branching, not a per-branch
        # ``isinstance(request_id, int)`` re-check. ``isinstance`` treats
        # ``request_id=0`` as valid and proceeds to write a full
        # requeue+log+denylist, where production's falsy check (``if not
        # request_id: return None`` — ``album_source.py``) writes nothing
        # for that same input. This also removes the fake-only
        # ``self.db.get_import_job(import_job_id) is not None``
        # requirement the deferred branch used to add: production takes
        # the deferred path on ``import_job_id is not None`` alone, so an
        # unseeded job id must NOT silently fall through to the
        # synchronous branch here — it must take the same deferred path
        # production does (and fail the same way production would, at the
        # eventual commit, not by taking a different route entirely).
        request_id = getattr(album_record, "db_request_id", None)
        if not request_id:
            return None
        if import_job_id is not None:
            from lib.dispatch import _record_rejection_and_maybe_requeue
            from lib.quality import DownloadInfo
            from lib.terminal_outcomes import (
                PendingImportTerminalOutcome,
                TerminalDenylist,
            )

            dl_info = (
                download_info
                if isinstance(download_info, DownloadInfo)
                else DownloadInfo()
            )
            pending = _record_rejection_and_maybe_requeue(
                cast(Any, self.db),
                request_id,
                dl_info,
                detail=getattr(bv_result, "detail", None),
                error=getattr(bv_result, "error", None),
                validation_result=(
                    dl_info.validation_result or bv_result.to_json()
                ),
                requeue=True,
                search_filetype_override=search_filetype_override,
                import_job_id=import_job_id,
            )
            assert isinstance(pending, PendingImportTerminalOutcome)
            return pending.append_denylists(*(
                TerminalDenylist(
                    username,
                    "beets validation rejected",
                    apply_cooldown=True,
                )
                for username in sorted(usernames or ())
            ))
        # Issue #1077, R3-6: mirror ``album_source.DatabaseSource``'s own
        # synchronous branch (``import_job_id is None``) instead of silently
        # no-op'ing it. The prior version of this fake only recorded the
        # call args and returned ``None`` here, so no test could ever prove
        # a rejection reaches a REAL persisted ``download_log`` row through
        # this entry point — exactly the "test infrastructure more
        # permissive than production" smell ``test-fidelity.md`` Rule A
        # exists to catch. Production writes directly via ``db.log_download``
        # on this path; do the same against the underlying ``FakePipelineDB``.
        from lib.quality import DownloadInfo

        dl = (
            download_info
            if isinstance(download_info, DownloadInfo)
            else DownloadInfo()
        )
        transition_kwargs: dict[str, object] = {}
        if search_filetype_override is not None:
            transition_kwargs["search_filetype_override"] = search_filetype_override
        transitions.require_transition_applied(
            transitions.finalize_request(
                self.db,
                request_id,
                transitions.RequestTransition.to_wanted_fields(
                    attempt_type="validation",
                    fields=transition_kwargs,
                ),
            )
        )
        validation_result = dl.validation_result or bv_result.to_json()
        download_log_id = self.db.log_download(
            request_id=request_id,
            soulseek_username=dl.username,
            filetype=dl.filetype,
            beets_detail=bv_result.detail,
            outcome="rejected",
            error_message=bv_result.error,
            bitrate=dl.bitrate,
            sample_rate=dl.sample_rate,
            bit_depth=dl.bit_depth,
            is_vbr=dl.is_vbr,
            was_converted=dl.was_converted,
            original_filetype=dl.original_filetype,
            slskd_filetype=dl.slskd_filetype,
            actual_filetype=dl.actual_filetype,
            actual_min_bitrate=dl.actual_min_bitrate,
            spectral_grade=(
                dl.download_spectral.grade if dl.download_spectral else None
            ),
            spectral_bitrate=(
                dl.download_spectral.bitrate_kbps if dl.download_spectral else None
            ),
            existing_min_bitrate=dl.existing_min_bitrate,
            existing_spectral_bitrate=(
                dl.current_spectral.bitrate_kbps if dl.current_spectral else None
            ),
            import_result=dl.import_result,
            validation_result=validation_result,
        )
        for username in usernames or ():
            self.db.add_denylist(request_id, username, "beets validation rejected")
            if (
                self.db.check_and_apply_cooldown(username)
                and cooled_down_users is not None
            ):
                cooled_down_users.add(username)
        return download_log_id

    def close(self) -> None:
        self.close_calls += 1
