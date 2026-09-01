"""FakePipelineDB requests cluster — mirrors ``lib/pipeline_db/requests.py``.

``album_requests`` writes, projections, and queries.
"""
from __future__ import annotations

import copy
import json
from collections.abc import (
    Mapping,
)
from datetime import UTC, datetime, timedelta
from typing import (
    TYPE_CHECKING,
    Any,
    cast,
)

import msgspec

if TYPE_CHECKING:
    from lib.pipeline_db import (
        AlbumRequestRow,
    )
from lib.import_queue import (
    IMPORT_JOB_FORCE,
    IMPORT_JOB_LOCAL,
)
from lib.pipeline_db import (
    MergeRekeyCollision,
)
from lib.pipeline_db._shared import (
    ACQUISITION_REQUEST_STATUSES,
    CURRENT_EVIDENCE_PREFIX,
    processing_owner_payload,
    validate_request_metadata_fields,
)
from lib.pipeline_db.decisions import (
    search_backoff_minutes,
)
from lib.pipeline_db.requests import (
    CAPTURE_DOWNLOAD_OUTCOMES,
    CAPTURE_IMPORT_JOB_TYPES,
    _linked_current_evidence_facts,
    collect_pipeline_overlays,
)
from lib.pipeline_db.rows import ArtistRequestRow
from lib.release_identity import ReleaseIdentity, normalize_release_id
from tests.fakes._shared import _as_datetime, _utcnow
from tests.fakes.pipeline_db._base import _FakePipelineDBBase
from tests.fakes.pipeline_db._shared import (
    _jsonb_column,
    _reject_nonstandard_json_constant,
)


class _FakeRequestsMixin(_FakePipelineDBBase):
    """``album_requests`` writes, projections, and queries."""


    def _processing_owner_join_aliases(
        self, row: Mapping[str, object],
    ) -> dict[str, object]:
        """The three aliases production's ``LEFT JOIN import_jobs
        processing_owner_job`` projects — the pointer join, with no
        latest-job inference."""
        owner_id = row.get("active_automation_import_job_id")
        owner = next(
            (
                job for job in self._import_jobs
                if job.get("id") == owner_id
            ),
            None,
        )
        return {
            "_processing_owner_job_id": (
                owner.get("id") if owner is not None else None
            ),
            "_processing_owner_status": (
                owner.get("status") if owner is not None else None
            ),
            "_processing_owner_preview_status": (
                owner.get("preview_status") if owner is not None else None
            ),
        }

    @staticmethod
    def _projected_request_row(row):
        """One request row as a read returns it.

        ``album_requests.active_download_state`` is JSONB written as a JSON
        string (``... = %s::jsonb`` in ``lib/pipeline_db/requests.py``), so
        every real read hands back parsed JSON — which is what
        ``AlbumRequestRow`` declares and what
        ``lib/download_recovery.py``'s ``_is_str_object_dict`` gate
        requires. ``set_downloading`` stores the writer's string (the
        fake's own internals re-parse it), so the parse belongs here, on
        the way out (issue #1278 item 7). Every request-row projection
        goes through this.

        Unparseable text is left ALONE rather than raised on, unlike
        ``_jsonb_column``'s other callers. Those read columns only the
        fake's own writers filled, so a string there is always writer
        form; a request row can also be injected wholesale by a fixture,
        already in READER form, and the two are indistinguishable here.
        Nothing is lost by degrading: PostgreSQL rejects malformed JSON at
        INSERT, so a value that will not parse is one no real column could
        hold and no real read could return.
        """
        projected = copy.deepcopy(dict(row))
        state = projected.get("active_download_state")
        try:
            projected["active_download_state"] = _jsonb_column(state)
        except json.JSONDecodeError:
            projected["active_download_state"] = state
        return projected

    def _request_presentation_copy(
        self,
        row,
    ):
        """Mirror the production pointer join without latest-job inference."""
        projected = self._projected_request_row(row)
        projected.update(self._processing_owner_join_aliases(projected))
        projected["processing_owner"] = processing_owner_payload(projected)
        projected.pop("_processing_owner_job_id")
        projected.pop("_processing_owner_status")
        projected.pop("_processing_owner_preview_status")
        return projected

    def get_request(self, request_id: int) -> AlbumRequestRow | None:
        row = self._requests.get(request_id)
        if row is None:
            return None
        return cast("AlbumRequestRow", self._request_presentation_copy(row))

    def request_marked_incomplete(self, request_id: int) -> bool:
        """Mirror ``PipelineDB.request_marked_incomplete`` (issue #1241)."""
        row = self._requests.get(request_id)
        return bool(
            row is not None and row.get("marked_incomplete_at") is not None
        )

    def set_marked_incomplete(self, request_id: int, *, marked: bool) -> str:
        """Mirror ``PipelineDB.set_marked_incomplete`` (issue #1241)."""
        row = self._requests.get(request_id)
        if row is None:
            return "not_found"
        if row.get("status") == "replaced":
            return "replaced"
        current = row.get("marked_incomplete_at")
        if marked and current is not None:
            return "already_marked"
        if not marked and current is None:
            return "already_clear"
        now = datetime.now(UTC)
        row["marked_incomplete_at"] = now if marked else None
        row["updated_at"] = now
        return "marked" if marked else "cleared"

    def get_request_by_mb_release_id(self, mb_release_id: str) -> AlbumRequestRow | None:
        for row in self._requests.values():
            if row.get("mb_release_id") == mb_release_id:
                return cast(
                    "AlbumRequestRow",
                    self._request_presentation_copy(row),
                )
        return None

    def get_request_by_discogs_release_id(self, discogs_release_id: str) -> AlbumRequestRow | None:
        for row in self._requests.values():
            if row.get("discogs_release_id") == discogs_release_id:
                return cast(
                    "AlbumRequestRow",
                    self._request_presentation_copy(row),
                )
        return None

    def get_request_by_release_id(self, release_id: object | None) -> AlbumRequestRow | None:
        normalized = normalize_release_id(release_id)
        if not normalized:
            return None

        race = self._request_creation_race
        if race is not None and normalized == race[0]:
            self._request_creation_race_lookups += 1
            if self._request_creation_race_lookups == 1:
                return None
            if self._request_creation_race_lookups == 2:
                _, status, discogs, disappear = race
                request_id = self.add_request(
                    mb_release_id=normalized,
                    discogs_release_id=normalized if discogs else None,
                    artist_name="Race",
                    album_title="Concurrent request",
                    source="request",
                    status=status,
                )
                row = self.get_request(request_id)
                assert row is not None
                self._request_creation_race = None
                if disappear:
                    self._requests.pop(request_id)
                return row

        identity = ReleaseIdentity.from_fields(normalized)
        if identity is None:
            return self.get_request_by_mb_release_id(normalized)

        if identity.source == "musicbrainz":
            return self.get_request_by_mb_release_id(identity.release_id)

        req = self.get_request_by_discogs_release_id(identity.release_id)
        if req:
            return req
        return self.get_request_by_mb_release_id(identity.release_id)

    def arm_request_creation_race(
        self,
        release_id: str,
        *,
        status: str,
        discogs: bool = False,
        disappear_after_in_lock_lookup: bool = False,
    ) -> None:
        """Materialize a competing row on the next in-lock identity lookup."""
        self._request_creation_race = (
            normalize_release_id(release_id) or release_id,
            status,
            discogs,
            disappear_after_in_lock_lookup,
        )
        self._request_creation_race_lookups = 0

    def update_status(
        self,
        request_id: int,
        status: str,
        *,
        expected_status: str | None = None,
        **extra: Any,
    ) -> bool:
        if status == "replaced":
            raise ValueError(
                "status='replaced' is owned by supersede_request_mbid")
        if status == "processing":
            raise ValueError(
                "status='processing' is owned by automation handoff")
        validate_request_metadata_fields(dict(extra))
        row = self._requests.get(request_id)
        if (
            row is None
            or row.get("status") == "replaced"
            or row.get("active_automation_import_job_id") is not None
        ):
            return False
        source_status = expected_status or str(row["status"])
        if row["status"] != source_status:
            return False
        row["status"] = status
        row["active_download_state"] = None
        row["updated_at"] = _utcnow()
        for key, val in extra.items():
            row[key] = val
        self.status_history.append((request_id, status))
        return True

    def compare_request_status(
        self,
        request_id: int,
        *,
        expected_status: str,
    ) -> bool:
        row = self._requests.get(request_id)
        return bool(
            row is not None
            and row.get("status") == expected_status
            and row.get("status") != "replaced"
            and row.get("active_automation_import_job_id") is None
        )

    def mark_imported_with_rescue(
        self,
        request_id: int,
        *,
        expected_status: str | None = None,
        **extra: Any,
    ) -> bool:
        """Mirror ``PipelineDB.mark_imported_with_rescue`` (U14).

        Atomic in-memory equivalent: writes ``status='imported'``,
        clears ``unfindable_category``, and on the FIRST rescue stamps
        ``rescued_at`` + ``prior_unfindable_category``. Reserved
        kwargs the production method rejects are rejected here too.
        """
        rescue_owned = {
            "unfindable_category",
            "unfindable_categorised_at",
        }
        bad_rescue_fields = sorted(set(extra) & rescue_owned)
        if bad_rescue_fields:
            raise ValueError(
                "mark_imported_with_rescue cannot accept rescue-owned fields: "
                + ", ".join(bad_rescue_fields)
            )
        validate_request_metadata_fields(dict(extra))
        row = self._requests.get(request_id)
        if (
            row is None
            or row.get("status") == "replaced"
            or row.get("active_automation_import_job_id") is not None
        ):
            return False
        source_status = expected_status or str(row["status"])
        if row["status"] != source_status:
            return False
        now = _utcnow()
        current_category = row.get("unfindable_category")
        already_rescued = row.get("rescued_at") is not None

        row["status"] = "imported"
        row["active_download_state"] = None
        row["updated_at"] = now
        if current_category is not None:
            row["unfindable_category"] = None
            row["unfindable_categorised_at"] = now
        if current_category is not None and not already_rescued:
            row["rescued_at"] = now
            row["prior_unfindable_category"] = current_category
        for key, val in extra.items():
            row[key] = val
        self.status_history.append((request_id, "imported"))
        return True

    def reset_to_wanted(
        self,
        request_id: int,
        *,
        expected_status: str | None = None,
        clear_retry_counters: bool = True,
        **fields: Any,
    ) -> bool:
        unknown = sorted(
            set(fields) - {
                "search_filetype_override",
                "min_bitrate",
                "prev_min_bitrate",
                "priority_started_at",
            }
        )
        if unknown:
            raise ValueError(
                "reset_to_wanted does not accept fields: "
                + ", ".join(unknown)
            )
        row = self._requests.get(request_id)
        if (
            row is None
            or row.get("status") == "replaced"
            or row.get("active_automation_import_job_id") is not None
        ):
            return False
        source_status = expected_status or str(row["status"])
        if row["status"] != source_status:
            return False
        now = _utcnow()
        row["status"] = "wanted"
        if clear_retry_counters:
            row["search_attempts"] = 0
            row["download_attempts"] = 0
            row["validation_attempts"] = 0
            row["next_retry_after"] = None
            row["last_attempt_at"] = None
        row["active_download_state"] = None
        row["updated_at"] = now
        if "search_filetype_override" in fields:
            row["search_filetype_override"] = fields["search_filetype_override"]
        if "prev_min_bitrate" in fields:
            row["prev_min_bitrate"] = fields["prev_min_bitrate"]
        if "min_bitrate" in fields:
            current_min_bitrate = row.get("min_bitrate")
            if (
                "prev_min_bitrate" not in fields
                and current_min_bitrate is not None
            ):
                row["prev_min_bitrate"] = current_min_bitrate
            row["min_bitrate"] = fields["min_bitrate"]
        if "priority_started_at" in fields:
            row["priority_started_at"] = fields["priority_started_at"]
        self.status_history.append((request_id, "wanted"))
        return True

    def reset_downloading_to_wanted(
        self,
        request_id: int,
        *,
        expected_status: str = "downloading",
        **fields: Any,
    ) -> bool:
        unknown = sorted(
            set(fields) - {
                "search_filetype_override",
                "min_bitrate",
                "prev_min_bitrate",
            }
        )
        if unknown:
            raise ValueError(
                "reset_downloading_to_wanted does not accept fields: "
                + ", ".join(unknown)
            )
        row = self._requests.get(request_id)
        if (
            row is None
            or expected_status != "downloading"
            or row["status"] != expected_status
        ):
            return False
        now = _utcnow()
        row["status"] = "wanted"
        row["active_download_state"] = None
        row["updated_at"] = now
        if "search_filetype_override" in fields:
            row["search_filetype_override"] = fields["search_filetype_override"]
        if "prev_min_bitrate" in fields:
            row["prev_min_bitrate"] = fields["prev_min_bitrate"]
        if "min_bitrate" in fields:
            current_min_bitrate = row.get("min_bitrate")
            if (
                "prev_min_bitrate" not in fields
                and current_min_bitrate is not None
            ):
                row["prev_min_bitrate"] = current_min_bitrate
            row["min_bitrate"] = fields["min_bitrate"]
        self.status_history.append((request_id, "wanted"))
        return True

    def set_downloading(
        self,
        request_id: int,
        state_json: str,
        *,
        expected_status: str = "wanted",
    ) -> bool:
        row = self._requests.get(request_id)
        if (
            row is None
            or expected_status != "wanted"
            or row["status"] != expected_status
        ):
            return False
        now = _utcnow()
        row["status"] = "downloading"
        row["active_download_state"] = state_json
        row["last_attempt_at"] = now
        row["updated_at"] = now
        self.status_history.append((request_id, "downloading"))
        return True

    def set_downloading_if_plan_current(
        self,
        request_id: int,
        state_json: str,
        *,
        plan_id: int,
        plan_ordinal: int,
        cycle_count_snapshot: int,
    ) -> bool:
        """Mirror of ``PipelineDB.set_downloading_if_plan_current``.

        Atomic plan-aware claim; refuses if status moved off ``wanted``
        OR the plan/ordinal/cycle no longer match the snapshot.
        """
        row = self._requests.get(request_id)
        if row is None or row["status"] != "wanted":
            return False
        if row.get("active_plan_id") != plan_id:
            return False
        if int(row.get("next_plan_ordinal") or 0) != plan_ordinal:
            return False
        if int(row.get("plan_cycle_count") or 0) != cycle_count_snapshot:
            return False
        now = _utcnow()
        row["status"] = "downloading"
        row["active_download_state"] = state_json
        row["last_attempt_at"] = now
        row["updated_at"] = now
        self.status_history.append((request_id, "downloading"))
        return True

    def update_download_state_if_downloading(
        self,
        request_id: int,
        state_json: str,
        *,
        expected_enqueued_at: str,
    ) -> bool:
        row = self._requests.get(request_id)
        self.update_download_state_calls.append((request_id, state_json))
        injected = self._update_download_state_errors.get(request_id)
        if injected is not None:
            raise injected
        try:
            outgoing_state = json.loads(
                state_json,
                parse_constant=_reject_nonstandard_json_constant,
            )
        except ValueError as exc:
            import psycopg2.errors
            raise psycopg2.errors.InvalidTextRepresentation(
                "invalid input syntax for type json",
            ) from exc
        if not isinstance(outgoing_state, dict):
            return False
        if row is None or row["status"] != "downloading":
            return False
        stored_state = row.get("active_download_state")
        if isinstance(stored_state, str):
            try:
                stored_state = json.loads(
                    stored_state,
                    parse_constant=_reject_nonstandard_json_constant,
                )
            except ValueError:
                return False
        if not isinstance(stored_state, dict):
            return False
        if stored_state.get("enqueued_at") != expected_enqueued_at:
            return False
        if outgoing_state.get("enqueued_at") != expected_enqueued_at:
            return False
        row["active_download_state"] = outgoing_state
        row["updated_at"] = _utcnow()
        return True

    def record_attempt(
        self,
        request_id: int,
        attempt_type: str,
        *,
        expected_status: str,
    ) -> bool:
        self.recorded_attempts.append((request_id, attempt_type))
        row = self._requests.get(request_id)
        if (
            row
            and row.get("status") == expected_status != "replaced"
            and row.get("active_automation_import_job_id") is None
        ):
            col = f"{attempt_type}_attempts"
            now = _utcnow()
            row[col] = (row.get(col) or 0) + 1
            row["last_attempt_at"] = now
            row["updated_at"] = now
            backoff_minutes = search_backoff_minutes(row[col] - 1)
            row["next_retry_after"] = now + timedelta(minutes=backoff_minutes)
            return True
        return False

    def clear_on_disk_quality_fields(self, request_id: int) -> None:
        self.clear_on_disk_quality_fields_calls.append(request_id)
        row = self._requests.get(request_id)
        if (
            row is None
            or row.get("status") == "replaced"
            or row.get("active_automation_import_job_id") is not None
        ):
            return
        row["verified_lossless"] = False
        row["current_spectral_grade"] = None
        row["current_spectral_bitrate"] = None
        row["current_lossless_source_v0_probe_min_bitrate"] = None
        row["current_lossless_source_v0_probe_avg_bitrate"] = None
        row["current_lossless_source_v0_probe_median_bitrate"] = None
        row["current_evidence_id"] = None
        row["updated_at"] = _utcnow()

    def get_downloading(self) -> list[AlbumRequestRow]:
        return cast(
            "list[AlbumRequestRow]",
            [self._projected_request_row(r)
             for r in self._requests.values()
             if r.get("status") == "downloading"],
        )

    def get_acquisition(
        self,
        *,
        youtube_limit: int = 50,
    ):
        """In-memory mirror of the combined one-read acquisition query."""
        self.query_counts["get_acquisition"] = (
            self.query_counts.get("get_acquisition", 0) + 1
        )
        active = [
            row for row in self._requests.values()
            if row.get("status") in ACQUISITION_REQUEST_STATUSES
        ]
        active.sort(key=lambda row: (
            _as_datetime(row.get("updated_at")),
            int(row["id"]),
        ))
        youtube = []
        youtube_limit = max(1, int(youtube_limit))
        for entry in sorted(
            self.download_logs,
            key=lambda item: (item.created_at, item.id),
        ):
            if entry.source != "youtube" or entry.outcome != "youtube_running":
                continue
            request = self._requests.get(entry.request_id) or {}
            youtube.append({
                "download_log_id": entry.id,
                "request_id": entry.request_id,
                "source": entry.source,
                "outcome": entry.outcome,
                "youtube_metadata": copy.deepcopy(entry.youtube_metadata),
                "created_at": entry.created_at,
                "artist_name": request.get("artist_name"),
                "album_title": request.get("album_title"),
                "mb_release_id": request.get("mb_release_id"),
                "request_status": request.get("status"),
                "processing_owner": None,
            })
            if len(youtube) >= youtube_limit:
                break
        return {
            "acquisition": [
                self._request_presentation_copy(row)
                for row in active
            ],
            "youtube_ingest": youtube,
        }

    def update_request_fields(
        self,
        request_id: int,
        **fields: Any,
    ) -> bool:
        expected_status_raw = fields.pop("expected_status", None)
        if (
            expected_status_raw is not None
            and not isinstance(expected_status_raw, str)
        ):
            raise TypeError("expected_status must be a string or None")
        expected_status = expected_status_raw
        validate_request_metadata_fields(dict(fields))
        self.update_request_fields_calls.append((request_id, dict(fields)))
        row = self._requests.get(request_id)
        if (
            not row
            or row.get("status") == "replaced"
            or row.get("active_automation_import_job_id") is not None
            or (
                expected_status is not None
                and row.get("status") != expected_status
            )
        ):
            return False
        if not fields:
            # Mirror production's control-only CAS: validate the row and
            # expected status, but do not manufacture an ``updated_at`` write.
            return True
        if fields.get("mb_release_id") is not None:
            # Production's UPDATE hits the same UNIQUE(mb_release_id)
            # as the INSERT — re-pointing a row at another row's mbid
            # raises there too (setting a row's own mbid is a no-op).
            self._assert_mb_release_id_unique(
                fields["mb_release_id"], exclude_id=request_id)
        row.update(fields)
        row["updated_at"] = _utcnow()
        return True

    def merge_rekey_collision(
        self,
        request_id: int,
        *,
        old_release_id: str,
        new_release_id: str,
    ) -> MergeRekeyCollision:
        """Mirror ``PipelineDB.merge_rekey_collision`` (#1080).

        Deliberately derived from the SAME state
        ``update_request_release_for_merge`` refuses on below — the other
        request rows and the evidence keyspace — so the pre-check and the
        write cannot drift apart in the fake the way they must not drift apart
        in PostgreSQL. No status filter on the rival: production's
        ``UNIQUE(mb_release_id)`` is global.
        """
        rival = next(
            (
                other_id
                for other_id, other in sorted(self._requests.items())
                if other_id != request_id
                and other.get("mb_release_id") == new_release_id
            ),
            None,
        )
        moving = [
            key[1] for key in self.album_quality_evidence
            if key[0] == old_release_id
        ]
        return MergeRekeyCollision(
            rival_request_id=rival,
            colliding_fingerprints=tuple(sorted(
                fingerprint for fingerprint in moving
                if (new_release_id, fingerprint) in self.album_quality_evidence
            )),
        )

    def update_request_release_for_merge(
        self,
        request_id: int,
        *,
        old_release_id: str,
        new_release_id: str,
        expected_import_job_id: int | None,
    ) -> bool:
        """Mirror ``PipelineDB.update_request_release_for_merge`` (#1059, #1089).

        Same predicates as production's compare-and-set: the row still holds
        ``old_release_id``, and the caller still holds the import claim that
        authorizes an identity write — the automation owner pointer
        (``processing`` + ``active_automation_import_job_id``), a
        ``running`` ``force_import`` job on an unowned, non-``replaced`` row
        (#1080), or the operator arm (#1089): ``expected_import_job_id is
        None``, ``status == 'imported'``, no automation owner, and no
        ``queued``/``running`` import job at all for this request — a real
        job id supplied by either claim arm NEVER satisfies this one. A
        survivor already held by another request is production's
        ``UNIQUE(mb_release_id)`` violation, which this write reports as False
        rather than raising.

        The request's ``album_quality_evidence`` rows move with it, in the
        same all-or-nothing step: evidence is content-addressed by
        ``(mb_release_id, snapshot_fingerprint)``, so leaving it at the
        merged-away id strands the request's verified-lossless proof. A
        fingerprint that already exists at the survivor is production's
        ``UNIQUE (mb_release_id, snapshot_fingerprint)`` violation — reported
        as False with nothing written, because choosing between two
        measurements of the same bytes is not this write's decision.
        """
        if not old_release_id or not new_release_id:
            raise ValueError("merge rekey requires both release ids")
        if old_release_id == new_release_id:
            raise ValueError(
                "refusing to rekey a request onto itself: "
                f"{old_release_id}"
            )
        self.update_request_release_for_merge_calls.append(
            (request_id, old_release_id, new_release_id, expected_import_job_id),
        )
        row = self._requests.get(request_id)
        if row is None or row.get("mb_release_id") != old_release_id:
            return False
        job = (
            self.get_import_job(expected_import_job_id)
            if expected_import_job_id is not None else None
        )
        automation_claim = (
            # Mirrors SQL's three-valued ``column = NULL`` — NEVER true, even
            # when the column itself is also NULL — which Python's ``==``
            # does not: without this guard ``None == None`` admits a
            # ``processing`` + unowned row whenever a caller (only the
            # operator arm, #1089) passes no job id at all.
            expected_import_job_id is not None
            and row.get("status") == "processing"
            and row.get("active_automation_import_job_id")
            == expected_import_job_id
        )
        force_claim = (
            # issue #1176 PR3 widened this arm to admit local_import too —
            # see lib.pipeline_db.requests.PipelineDB
            # .rekey_release_identity's docstring.
            job is not None
            and job.job_type in (IMPORT_JOB_FORCE, IMPORT_JOB_LOCAL)
            and job.status == "running"
            and job.request_id == request_id
            and row.get("active_automation_import_job_id") is None
            and row.get("status") not in ("processing", "replaced")
        )
        operator_claim = (
            expected_import_job_id is None
            and row.get("status") == "imported"
            and row.get("active_automation_import_job_id") is None
            and not any(
                candidate.get("request_id") == request_id
                and candidate.get("status") in ("queued", "running")
                for candidate in self._import_jobs
            )
        )
        if not (automation_claim or force_claim or operator_claim):
            return False
        for other_id, other in self._requests.items():
            if other_id != request_id and other.get("mb_release_id") == new_release_id:
                return False
        moving = [
            key for key in self.album_quality_evidence
            if key[0] == old_release_id
        ]
        if any(
            (new_release_id, fingerprint) in self.album_quality_evidence
            for _, fingerprint in moving
        ):
            return False
        for key in moving:
            evidence = self.album_quality_evidence.pop(key)
            moved = msgspec.structs.replace(
                evidence, mb_release_id=new_release_id,
            )
            self.album_quality_evidence[(new_release_id, key[1])] = moved
            if moved.id is not None:
                self._evidence_by_id[moved.id] = moved
        row["mb_release_id"] = new_release_id
        row["updated_at"] = _utcnow()
        return True

    # Each fake mirrors the production PipelineDB writer's contract:
    # one statement, no cursor mutation, autocommit-safe. Tests assert
    # against the persisted row state (and per-method call recorders
    # for the R20 cursor-isolation runtime guard).

    def list_unfindable_probe_candidates(
        self,
        *,
        limit: int,
        probe_interval_days: int,
    ) -> list[dict[str, Any]]:
        """Mirror PipelineDB.list_unfindable_probe_candidates.

        Pulls ``status='wanted'`` rows whose ``last_artist_probe_at``
        is NULL or older than ``probe_interval_days``, oldest first
        (NULL sorts before any timestamp).
        """
        if limit <= 0:
            return []
        cutoff = _utcnow() - timedelta(days=int(probe_interval_days))
        eligible: list[dict[str, Any]] = []
        for row in self._requests.values():
            if row.get("status") != "wanted":
                continue
            last = row.get("last_artist_probe_at")
            if last is not None:
                last_dt = _as_datetime(last)
                if last_dt > cutoff:
                    continue
            eligible.append({
                "id": row["id"],
                "artist_name": row.get("artist_name"),
                "unfindable_category": row.get("unfindable_category"),
                "last_artist_probe_at": row.get("last_artist_probe_at"),
                "last_artist_probe_match_count": row.get(
                    "last_artist_probe_match_count"),
            })

        def _sort_key(r: dict[str, Any]) -> tuple[int, datetime, int]:
            ts = r["last_artist_probe_at"]
            if ts is None:
                return (0, datetime.min.replace(tzinfo=UTC),
                        int(r["id"]))
            return (1, _as_datetime(ts), int(r["id"]))
        eligible.sort(key=_sort_key)
        return eligible[: int(limit)]

    def record_artist_probe(
        self,
        request_id: int,
        *,
        match_count: int,
        observed_at: datetime,
    ) -> None:
        """Mirror PipelineDB.record_artist_probe.

        Mirrors the ``AND status='wanted'`` guard from production: if
        the row has transitioned out of ``wanted`` (e.g. a concurrent
        ``mark_imported_with_rescue`` flipped it to ``imported`` while
        the probe was inflight), the write is a silent no-op. The call
        is still recorded on ``record_artist_probe_calls`` because tests
        need to see the attempt happened.
        """
        self.record_artist_probe_calls.append(
            (request_id, int(match_count), observed_at),
        )
        row = self._requests.get(request_id)
        if row is None:
            return
        if row.get("status") != "wanted":
            return
        row["last_artist_probe_at"] = observed_at
        row["last_artist_probe_match_count"] = int(match_count)
        row["updated_at"] = observed_at

    def set_unfindable_category(
        self,
        request_id: int,
        *,
        category: str | None,
        categorised_at: datetime,
    ) -> None:
        """Mirror PipelineDB.set_unfindable_category.

        Enforces the same 4-category vocabulary the production CHECK
        constraint guards. ``None`` clears the column.

        Mirrors the ``AND status='wanted'`` guard from production: if a
        concurrent rescue flipped the row out of ``wanted`` mid-probe,
        the late verdict write is a silent no-op. The call is still
        recorded on ``set_unfindable_category_calls`` so tests can see
        the attempt happened.
        """
        valid = {
            "artist_absent",
            "album_absent_artist_present",
            "one_track_structural",
            "wrong_pressing_available",
        }
        if category is not None and category not in valid:
            raise ValueError(
                f"set_unfindable_category: invalid category {category!r}")
        self.set_unfindable_category_calls.append(
            (request_id, category, categorised_at),
        )
        row = self._requests.get(request_id)
        if row is None:
            return
        if row.get("status") != "wanted":
            return
        row["unfindable_category"] = category
        row["unfindable_categorised_at"] = categorised_at
        row["updated_at"] = categorised_at

    def get_unfindable_search_log_signal(
        self,
        request_id: int,
        *,
        window_days: int,
        matcher_score_threshold: float,
    ) -> Any:
        """Mirror PipelineDB.get_unfindable_search_log_signal.

        Walks ``self.search_logs`` once, applies the same window + filters
        the production SQL applies, and returns the aggregated struct.
        """
        from lib.unfindable_detection_service import UnfindableSearchLogSignal

        cutoff = _utcnow() - timedelta(days=int(window_days))
        cycles: dict[int, int] = {}  # plan_cycle_snapshot -> found count
        wrong_pressing_hits = 0
        for entry in self.search_logs:
            if entry.request_id != request_id:
                continue
            if entry.attempt_consumed is not True:
                continue
            if entry.created_at <= cutoff:
                continue
            cycle = entry.plan_cycle_snapshot
            if cycle is not None:
                found_inc = 1 if entry.outcome == "found" else 0
                cycles[int(cycle)] = cycles.get(int(cycle), 0) + found_inc
            if (
                entry.rejection_reason == "strict_count_mismatch"
                and entry.matcher_score_top1 is not None
                and entry.matcher_score_top1 >= float(matcher_score_threshold)
            ):
                wrong_pressing_hits += 1
        zero_find_cycles = sum(1 for v in cycles.values() if v == 0)
        return UnfindableSearchLogSignal(
            zero_find_cycles=zero_find_cycles,
            wrong_pressing_hits=wrong_pressing_hits,
        )

    def add_request(self, artist_name: str, album_title: str, source: str,
                    mb_release_id: str | None = None,
                    mb_release_group_id: str | None = None,
                    mb_artist_id: str | None = None,
                    discogs_release_id: str | None = None,
                    year: int | None = None, country: str | None = None,
                    format: str | None = None,
                    source_path: str | None = None,
                    reasoning: str | None = None,
                    status: str = "wanted",
                    release_group_year: int | None = None,
                    is_va_compilation: bool = False) -> int:
        """Insert an album_requests row.

        Seeds the full ``album_requests`` column set (matching
        ``make_request_row`` in ``tests/helpers.py``) so fake-backed
        tests that then read DB-defaulted fields like ``beets_distance``
        or ``*_attempts`` see the same NULL/0 defaults production
        callers get from PostgreSQL. Codex R7.
        """
        if status == "processing":
            raise ValueError(
                "processing requests require an exact automation owner")
        self._assert_mb_release_id_unique(mb_release_id)
        self._next_request_id += 1
        rid = self._next_request_id
        now = _utcnow()
        self._requests[rid] = {
            "id": rid,
            "mb_release_id": mb_release_id,
            "mb_release_group_id": mb_release_group_id,
            "mb_artist_id": mb_artist_id,
            "discogs_release_id": discogs_release_id,
            "artist_name": artist_name,
            "album_title": album_title,
            "year": year,
            # U3 / R9 — release-group's first-release year. Populated by
            # the deploy-time backfill or U4's enqueue path; nullable.
            "release_group_year": release_group_year,
            # Migration 028 / U4 — VA detection flag, set at enqueue or by
            # the U3 backfill. NOT NULL DEFAULT FALSE matches the schema.
            "is_va_compilation": bool(is_va_compilation),
            "country": country,
            "format": format,
            "source": source,
            "source_path": source_path,
            "reasoning": reasoning,
            "status": status,
            # Migration 032 — resolver-populated catalog number; migration
            # 001 — download-time final container format. Neither is part
            # of ``AddRequestInput`` (production's INSERT column list), so
            # a freshly-added real row has both NULL until a later UPDATE
            # populates them. #523 read-projection parity (get_wanted_
            # searchable's ``ar.*``) surfaced these as missing from the
            # fake's row shape.
            "catalog_number": None,
            "final_format": None,
            "search_attempts": 0,
            "download_attempts": 0,
            "validation_attempts": 0,
            "last_attempt_at": None,
            "next_retry_after": None,
            "beets_distance": None,
            "beets_scenario": None,
            "search_filetype_override": None,
            "target_format": None,
            "min_bitrate": None,
            "prev_min_bitrate": None,
            "last_download_spectral_bitrate": None,
            "last_download_spectral_grade": None,
            "verified_lossless": False,
            "current_spectral_grade": None,
            "current_spectral_bitrate": None,
            "current_lossless_source_v0_probe_min_bitrate": None,
            "current_lossless_source_v0_probe_avg_bitrate": None,
            "current_lossless_source_v0_probe_median_bitrate": None,
            "active_download_state": None,
            # Migration 066 — exact active automation processor owner.
            "active_automation_import_job_id": None,
            # U1 persisted-search-plans cursor fields.
            "active_plan_id": None,
            "next_plan_ordinal": 0,
            "plan_cycle_count": 0,
            # Migration 028 / U12 — failure_class is materialised at
            # plan-wrap; NULL until the first cycle completes.
            "failure_class": None,
            # Migration 028 / U13 — unfindable detection state. All
            # nullable; the daily detection job populates the four-
            # category taxonomy via the dedicated systemd unit.
            "unfindable_category": None,
            "unfindable_categorised_at": None,
            "last_artist_probe_at": None,
            "last_artist_probe_match_count": None,
            # Migration 028 / U14 — long-tail-rescue audit columns.
            "rescued_at": None,
            "prior_unfindable_category": None,
            # Migration 082 / issue #1241 — operator incomplete mark.
            "marked_incomplete_at": None,
            # Migration 021 addressing FK.
            "current_evidence_id": None,
            # Migration 023 — supersede lineage.
            "replaces_request_id": None,
            "created_at": now,
            # Migration 062 — Bad Rip starts a fresh scheduler-priority
            # window without rewriting the request's creation audit.
            "priority_started_at": None,
            "updated_at": now,
        }
        return rid

    def supersede_request_mbid(
        self,
        old_request_id: int,
        *,
        new_mb_release_id: str,
        new_mb_release_group_id: str | None,
        new_mb_artist_id: str | None,
        new_artist_name: str,
        new_album_title: str,
        new_year: int | None,
        new_country: str | None,
        new_tracks: list[dict[str, Any]],
        new_discogs_release_id: str | None = None,
    ) -> int:
        """In-memory mirror of ``PipelineDB.supersede_request_mbid``.

        Raises ``MbidCollisionError`` when ``new_mb_release_id`` already
        exists in any row; ``SupersedeRaceError`` when the old row is
        missing or already ``status='replaced'``.
        """
        from lib.pipeline_db import (
            MbidCollisionError,
            SupersedeRaceError,
        )

        old_row = self._requests.get(old_request_id)
        if old_row is None:
            raise SupersedeRaceError(
                f"old request {old_request_id} not found"
            )
        if old_row.get("status") == "replaced":
            raise SupersedeRaceError(
                f"old request {old_request_id} already replaced"
            )
        if old_row.get("active_automation_import_job_id") is not None:
            raise SupersedeRaceError(
                f"old request {old_request_id} gained a processing owner "
                "before supersede"
            )
        # Collision check.
        for r in self._requests.values():
            if r.get("mb_release_id") == new_mb_release_id:
                raise MbidCollisionError(
                    f"target MBID {new_mb_release_id} already exists"
                )

        now = _utcnow()
        old_source = old_row.get("source", "request")
        # Flip the old row. Nothing else is mutated — characteristic fields
        # stay frozen.
        old_row["status"] = "replaced"
        old_row["updated_at"] = now

        # Insert new row via add_request to inherit the seeded defaults,
        # then patch the supersede-only fields.
        new_id = self.add_request(
            artist_name=new_artist_name,
            album_title=new_album_title,
            source=old_source,
            mb_release_id=new_mb_release_id,
            mb_release_group_id=new_mb_release_group_id,
            mb_artist_id=new_mb_artist_id,
            discogs_release_id=new_discogs_release_id,
            year=new_year,
            country=new_country,
            status="wanted",
        )
        self._requests[new_id]["replaces_request_id"] = old_request_id

        # Insert tracks.
        self._tracks[new_id] = [
            {
                "disc_number": t.get("disc_number", 1),
                "track_number": t["track_number"],
                "title": t["title"],
                "length_seconds": t.get("length_seconds"),
                "track_artist": t.get("track_artist"),
            }
            for t in new_tracks
        ]
        return new_id

    def get_request_by_replaces_request_id(
        self, replaced_id: int
    ) -> AlbumRequestRow | None:
        """Reverse-lookup the descendant row of ``replaced_id``."""
        for row in self._requests.values():
            if row.get("replaces_request_id") == replaced_id:
                return cast(
                    "AlbumRequestRow",
                    self._request_presentation_copy(row),
                )
        return None

    def get_oldest_request_chain_created_at(
        self, request_id: int
    ) -> datetime | None:
        """Oldest ``created_at`` across the replace chain, walking
        ``replaces_request_id`` back through superseded ancestors —
        mirrors the recursive CTE in ``_RequestsMixin``."""
        oldest: datetime | None = None
        seen: set[int] = set()
        cursor: int | None = request_id
        while cursor is not None and cursor not in seen:
            seen.add(cursor)
            row = self._requests.get(cursor)
            if row is None:
                break
            created = row.get("created_at")
            if created is not None:
                created = _as_datetime(created)
                if oldest is None or created < oldest:
                    oldest = created
            cursor = row.get("replaces_request_id")
        return oldest

    def list_requests_in_release_group(
        self,
        rg_id: str,
        *,
        exclude_replaced: bool = True,
        exclude_request_id: int | None = None,
    ) -> list[AlbumRequestRow]:
        """List rows in the same MB release group (newest id first)."""
        out: list[dict[str, Any]] = []
        for row in self._requests.values():
            if row.get("mb_release_group_id") != rg_id:
                continue
            if exclude_replaced and row.get("status") == "replaced":
                continue
            if exclude_request_id is not None and row.get("id") == exclude_request_id:
                continue
            out.append(self._request_presentation_copy(row))
        out.sort(key=lambda r: r["id"], reverse=True)
        return cast("list[AlbumRequestRow]", out)

    def list_active_release_group_ids(self) -> set[str]:
        """Distinct set of RG ids across non-replaced rows."""
        return {
            row["mb_release_group_id"]
            for row in self._requests.values()
            if row.get("status") != "replaced"
            and row.get("mb_release_group_id") is not None
        }

    def list_non_replaced_requests(self) -> list[AlbumRequestRow]:
        """Return active request rows ordered like PipelineDB."""
        rows = [
            r for r in self._requests.values()
            if r.get("status") != "replaced"
        ]
        rows.sort(key=lambda r: int(r["id"]))
        return cast(
            "list[AlbumRequestRow]",
            [self._request_presentation_copy(r) for r in rows],
        )

    def delete_request(self, request_id: int) -> bool:
        """Delete a request and cascade to child tables.

        Real ``album_requests`` has ``ON DELETE CASCADE`` foreign keys
        from ``album_tracks``, ``download_log``, ``search_log``, and
        ``source_denylist`` (see ``migrations/001_initial.sql``). Mirror
        that here so fake-backed tests cannot observe an impossible
        post-delete state where child rows survive their parent.
        """
        request = self._requests.get(request_id)
        if (
            request is not None
            and request.get("active_automation_import_job_id") is not None
        ):
            return False
        if request is None:
            return False
        self._requests.pop(request_id, None)
        self._tracks.pop(request_id, None)
        self.download_logs = [
            e for e in self.download_logs if e.request_id != request_id]
        self.search_logs = [
            e for e in self.search_logs if e.request_id != request_id]
        self.denylist = [
            e for e in self.denylist if e.request_id != request_id]
        # Migration 021: evidence is content-addressed; deleting a request
        # no longer cascades into evidence rows. Addressing FKs on
        # album_requests / download_log / import_jobs were nulled by the
        # earlier reassignments above.
        # U1: cascade plans + items with the request, mirroring the real
        # ON DELETE CASCADE FKs from migration 014.
        plan_ids_to_drop = [
            pid for pid, plan in self.search_plans.items()
            if plan.request_id == request_id
        ]
        for pid in plan_ids_to_drop:
            self.search_plans.pop(pid, None)
        self.search_plan_items = {
            iid: item for iid, item in self.search_plan_items.items()
            if item.plan_id not in plan_ids_to_drop
        }
        return True

    def get_wanted(self, limit: int | None = None) -> list[AlbumRequestRow]:
        """Return wanted requests past their retry gate.

        Production randomizes the diagnostic result.  The fake keeps insertion
        order for deterministic tests, but does not apply attempt-count
        priority.
        """
        now = _utcnow()
        eligible = [
            r for r in self._requests.values()
            if r.get("status") == "wanted"
            and (r.get("next_retry_after") is None
                 or r["next_retry_after"] <= now)
        ]
        if limit is not None:
            eligible = eligible[:int(limit)]
        return cast(
            "list[AlbumRequestRow]",
            [self._projected_request_row(r) for r in eligible],
        )

    def get_pipeline_overlay(
        self, mbids: list[str],
    ) -> dict[str, dict[str, Any]]:
        """Mirror of ``PipelineDB.get_pipeline_overlay`` — projects the
        overlay fields straight from seeded request rows (#445 item 2).
        Parity pinned by ``TestGetPipelineOverlay``."""
        musicbrainz_ids: set[str] = set()
        discogs_ids: set[str] = set()
        for raw_release_id in mbids:
            normalized = normalize_release_id(raw_release_id)
            if not normalized:
                continue
            identity = ReleaseIdentity.from_id(normalized)
            if identity is not None and identity.source == "discogs":
                discogs_ids.add(identity.release_id)
            else:
                musicbrainz_ids.add(normalized)
        # Only the WHERE clause is emulated here; the projection, identity
        # key, and dedicated-Discogs-column precedence are production's own
        # ``collect_pipeline_overlays``.
        matched: list[dict[str, object]] = []
        for r in self._requests.values():
            identity = ReleaseIdentity.from_fields(
                r.get("mb_release_id"),
                r.get("discogs_release_id"),
            )
            if identity is not None:
                matches = (
                    identity.release_id in musicbrainz_ids
                    if identity.source == "musicbrainz"
                    else identity.release_id in discogs_ids
                )
            else:
                fallback = normalize_release_id(r.get("mb_release_id"))
                matches = bool(fallback and fallback in musicbrainz_ids)
            if not matches:
                continue
            matched.append({
                **self._capture_and_evidence_select_aliases(r),
                "id": r["id"],
                "status": r.get("status"),
                "mb_release_id": r.get("mb_release_id"),
                "discogs_release_id": r.get("discogs_release_id"),
                "search_filetype_override": r.get("search_filetype_override"),
                "target_format": r.get("target_format"),
                "min_bitrate": r.get("min_bitrate"),
                "active_automation_import_job_id": r.get(
                    "active_automation_import_job_id"),
                **self._processing_owner_join_aliases(r),
            })
        return collect_pipeline_overlays(matched)

    def list_library_request_candidates(
        self,
        release_ids: list[str],
    ) -> list[ArtistRequestRow]:
        """Cardinality-preserving mirror of the production library read."""
        requested_identities = {
            identity.key
            for release_id in release_ids
            if (identity := ReleaseIdentity.from_id(release_id)) is not None
        }
        rows: list[ArtistRequestRow] = []
        for row in self._requests.values():
            identity = ReleaseIdentity.from_strict_fields(
                row.get("mb_release_id"),
                row.get("discogs_release_id"),
            )
            if identity is None or identity.key not in requested_identities:
                continue
            projected = self._request_presentation_copy(row)
            projected.update(self._capture_and_evidence_projection(row))
            rows.append(msgspec.convert(projected, type=ArtistRequestRow))
        rows.sort(key=lambda row: int(row["id"]))
        return rows

    def get_by_status(
        self,
        status: str,
        *,
        limit: int | None = None,
        newest_first: bool = False,
    ) -> list[AlbumRequestRow]:
        if newest_first:
            rows = sorted(
                (r for r in self._requests.values()
                 if r.get("status") == status),
                key=lambda r: _as_datetime(r.get("updated_at")),
                reverse=True)
        else:
            rows = sorted(
                (r for r in self._requests.values()
                 if r.get("status") == status),
                key=lambda r: _as_datetime(r.get("created_at")))
        if limit is not None:
            rows = rows[:int(limit)]
        return cast(
            "list[AlbumRequestRow]",
            [self._request_presentation_copy(r) for r in rows],
        )

    def search_requests(
        self, query: str, *, limit: int = 200, status: str | None = None,
    ) -> list[AlbumRequestRow]:
        """Mirror ``PipelineDB.search_requests``: case-insensitive
        substring over artist/album, optionally narrowed to one status."""
        q = (query or "").strip().lower()
        if not q:
            return []
        rows = [
            r for r in self._requests.values()
            if (q in str(r.get("artist_name") or "").lower()
                or q in str(r.get("album_title") or "").lower())
            and (status is None or r.get("status") == status)
        ]
        rows.sort(key=lambda r: (
            str(r.get("artist_name") or ""),
            r.get("year") is None,
            int(str(r.get("year") or 0)),
            int(str(r["id"])),
        ))
        return cast(
            "list[AlbumRequestRow]",
            [
                self._request_presentation_copy(r)
                for r in rows[:int(limit)]
            ],
        )


    def _capture_and_evidence_projection(
        self,
        row: Mapping[str, object],
    ) -> dict[str, object]:
        """The two specialized request SELECTs' facts, identity-gated.

        The raw aliases come from ``_capture_and_evidence_select_aliases``;
        gating the evidence pair on the request's exact pressing is
        production's own ``_linked_current_evidence_facts``.
        """
        raw = self._capture_and_evidence_select_aliases(row)
        verified_lossless, provisional_lossless = _linked_current_evidence_facts({
            "mb_release_id": row.get("mb_release_id"),
            "discogs_release_id": row.get("discogs_release_id"),
            **raw,
        })
        return {
            "has_captured_history": raw["has_captured_history"],
            "verified_lossless": verified_lossless,
            "provisional_lossless": provisional_lossless,
        }

    def _capture_and_evidence_select_aliases(
        self,
        row: Mapping[str, object],
    ) -> dict[str, object]:
        """Mirror ``_CAPTURE_AND_EVIDENCE_SELECT`` (lib/pipeline_db/
        requests.py) for one request row: the correlated capture EXISTS
        plus the three current-evidence aliases, RAW.

        The SQL computes the evidence trio off the LEFT-JOINed current
        evidence with no identity predicate — gating them on the request's
        exact pressing belongs to ``_linked_current_evidence_facts``, which
        both this fake and production call.
        """
        request_id = row.get("id")
        has_captured_history = row.get("status") == "imported"
        if isinstance(request_id, int) and not isinstance(request_id, bool):
            # Both vocabularies are imported from the module that owns
            # ``_CAPTURE_AND_EVIDENCE_SELECT`` rather than restated here:
            # this fake previously carried its own copy of each, and the
            # outcome half was once left un-widened while the job_type half
            # gained ``local_import`` in the same change.
            has_captured_history = has_captured_history or any(
                entry.request_id == request_id
                and entry.outcome in CAPTURE_DOWNLOAD_OUTCOMES
                for entry in self.download_logs
            )
            has_captured_history = has_captured_history or any(
                job.get("request_id") == request_id
                and job.get("status") == "completed"
                and job.get("job_type") in CAPTURE_IMPORT_JOB_TYPES
                for job in self._import_jobs
            )

        evidence = self._current_evidence_for_request(row)
        # ``COALESCE(current_evidence.verified_lossless, FALSE)``.
        linked_verified_lossless = bool(
            evidence is not None
            and evidence.verified_lossless_proof is not None
        )
        return {
            "has_captured_history": has_captured_history,
            "_linked_verified_lossless": linked_verified_lossless,
            "_linked_evidence_release_id": (
                evidence.mb_release_id if evidence is not None else None),
            # ``COALESCE(v0_subject, '') = 'source' AND NOT verified_lossless``.
            "provisional_lossless": bool(
                not linked_verified_lossless
                and evidence is not None
                and evidence.v0_metric is not None
                and evidence.v0_metric.subject == "source"
            ),
        }

    def _long_tail_projection(self, row: dict[str, Any]) -> dict[str, Any]:
        """Project a request row to the long-tail cohort SELECT shape.

        Mirrors ``PipelineDB._LONG_TAIL_SELECT``'s narrow column list +
        the ``in_flight_rescue`` stamp so tests can't rely on a column
        the production query doesn't return, plus the current-evidence
        accusation aliases the worklist chip's audit-only flags derive
        from.
        """
        keys = (
            "id", "artist_name", "album_title", "year", "status", "source",
            "mb_release_id", "mb_release_group_id", "discogs_release_id",
            "target_format", "min_bitrate", "search_filetype_override",
            "unfindable_category", "current_spectral_grade",
            "current_spectral_bitrate",
        )
        out: dict[str, Any] = {k: row.get(k) for k in keys}
        # track_count mirrors the production COUNT(*) over album_tracks.
        out["track_count"] = len(self._tracks.get(int(row["id"]), []))
        out["in_flight_rescue"] = self._has_youtube_running(int(row["id"]))
        out.update(self._accusation_alias_projection(
            self._current_evidence_for_request(row),
            CURRENT_EVIDENCE_PREFIX,
        ))
        return out

    def get_long_tail_cohort(self) -> list[dict[str, Any]]:
        """In-memory mirror of ``PipelineDB.get_long_tail_cohort``.

        Returns every ``wanted`` request projected to the cohort SELECT
        shape, id ASC, each stamped with ``in_flight_rescue``. Counts as
        ONE query for the N+1 guard.
        """
        self.query_counts["get_long_tail_cohort"] = (
            self.query_counts.get("get_long_tail_cohort", 0) + 1
        )
        rows = sorted(
            (r for r in self._requests.values()
             if r.get("status") == "wanted"),
            key=lambda r: int(r["id"]),
        )
        return [self._long_tail_projection(r) for r in rows]

    def get_long_tail_request(
        self, request_id: int,
    ) -> dict[str, Any] | None:
        """In-memory mirror of ``PipelineDB.get_long_tail_request``.

        Single-id variant — returns ``None`` when the row is missing or
        no longer ``wanted``.
        """
        self.query_counts["get_long_tail_request"] = (
            self.query_counts.get("get_long_tail_request", 0) + 1
        )
        row = self._requests.get(int(request_id))
        if row is None or row.get("status") != "wanted":
            return None
        return self._long_tail_projection(row)

    def count_by_status(self) -> dict[str | None, int]:
        counts: dict[str | None, int] = {}
        for r in self._requests.values():
            status = r.get("status")
            counts[status] = counts.get(status, 0) + 1
        return counts

    def list_requests_by_artist(
        self,
        artist_name: str,
        mb_artist_id: str = "",
    ) -> list[ArtistRequestRow]:
        needle = artist_name.lower()

        def _legacy_name_match(row: dict[str, Any]) -> bool:
            artist = str(row.get("artist_name") or "").lower()
            artist_id = row.get("mb_artist_id")
            artist_id_str = str(artist_id or "")
            return (
                needle in artist
                and (
                    artist_id is None
                    or artist_id_str == ""
                    or "-" not in artist_id_str
                )
            )

        rows: list[dict[str, Any]] = []
        for row in self._requests.values():
            if mb_artist_id:
                if row.get("mb_artist_id") == mb_artist_id or _legacy_name_match(row):
                    projected = self._request_presentation_copy(row)
                    projected.update(
                        self._capture_and_evidence_projection(row))
                    rows.append(projected)
            else:
                if needle in str(row.get("artist_name") or "").lower():
                    projected = self._request_presentation_copy(row)
                    projected.update(
                        self._capture_and_evidence_projection(row))
                    rows.append(projected)

        def _sort_key(row: dict[str, Any]) -> tuple[bool, int, str]:
            year = row.get("year")
            year_num = int(year) if isinstance(year, int) else 0
            title = str(row.get("album_title") or "")
            return (year is not None, year_num, title)

        rows.sort(key=_sort_key)
        return cast("list[ArtistRequestRow]", rows)

