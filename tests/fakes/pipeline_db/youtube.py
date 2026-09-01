"""FakePipelineDB youtube cluster — mirrors ``lib/pipeline_db/youtube.py``.

YouTube resolver mappings and rescue-ingest jobs.
"""
from __future__ import annotations

import copy
from typing import (
    Any,
)

import msgspec

from lib.import_queue import (
    IMPORT_JOB_ACTIVE_STATUSES,
    IMPORT_JOB_YOUTUBE,
    ImportJob,
)
from lib.pipeline_db import (
    PersistedYoutubeRow,
)
from tests.fakes._shared import _utcnow
from tests.fakes.pipeline_db._base import _FakePipelineDBBase
from tests.fakes.rows import (
    DownloadLogRow,
)


class _FakeYoutubeMixin(_FakePipelineDBBase):
    """YouTube resolver mappings and rescue-ingest jobs."""

    _YOUTUBE_TERMINAL_OUTCOMES: frozenset[str] = frozenset({
        "youtube_success", "youtube_failed",
    })

    def find_active_youtube_import_job(
        self,
        *,
        request_id: int,
        browse_id: str,
    ) -> ImportJob | None:
        rows = [
            row for row in self._import_jobs
            if row.get("job_type") == IMPORT_JOB_YOUTUBE
            and row.get("request_id") == request_id
            and row.get("status") in IMPORT_JOB_ACTIVE_STATUSES
        ]
        rows.sort(key=lambda row: row["id"])
        return ImportJob.from_row(copy.deepcopy(rows[0])) if rows else None

    def insert_youtube_running(
        self,
        *,
        request_id: int,
        browse_id: str,
        audio_playlist_id: str | None,
        yt_url: str,
        expected_track_count: int,
        resolver_mapping_id: int | None = None,
        per_track_video_ids: list[str] | None = None,
    ) -> int:
        """Mirror of ``PipelineDB.insert_youtube_running``.

        Raises ``YoutubeInFlightError`` when a ``youtube_running`` row
        already exists for the same ``request_id``, mirroring the real
        partial unique index (``one_youtube_running_per_request``) on
        the production schema.
        """
        existing_id: int | None = None
        for entry in self.download_logs:
            if (entry.source == "youtube"
                    and entry.outcome == "youtube_running"
                    and entry.request_id == request_id):
                existing_id = entry.id
                break
        if existing_id is not None:
            # Look up YoutubeInFlightError lazily so we always raise the
            # currently-loaded class. A prior test in this run may have
            # done ``importlib.reload(lib.pipeline_db)`` (e.g.
            # ``TestReleaseIdToLockKey::test_key_is_stable_across_imports``);
            # a module-level binding would point at the pre-reload class
            # and assertRaises in the caller would miss it.
            from lib.pipeline_db import YoutubeInFlightError as _YIFE
            raise _YIFE(request_id, existing_id)

        new_log_id = self._mint_download_log_id()
        metadata: dict[str, Any] = {
            "yt_url": yt_url,
            "browse_id": browse_id,
            "audio_playlist_id": audio_playlist_id,
            "expected_track_count": int(expected_track_count),
        }
        if resolver_mapping_id is not None:
            metadata["resolver_mapping_id"] = int(resolver_mapping_id)
        if per_track_video_ids is not None:
            metadata["per_track_video_ids"] = [
                str(video_id) for video_id in per_track_video_ids
            ]
        self.download_logs.append(DownloadLogRow(
            request_id=request_id,
            outcome="youtube_running",
            source="youtube",
            youtube_metadata=metadata,
            id=new_log_id,
        ))
        return new_log_id

    def enqueue_youtube_import_and_mark_success(
        self,
        *,
        download_log_id: int,
        request_id: int,
        dedupe_key: str,
        payload: dict[str, Any],
        message: str,
        terminal_metadata: dict[str, Any],
    ) -> ImportJob:
        job = self.enqueue_import_job(
            IMPORT_JOB_YOUTUBE,
            request_id=request_id,
            dedupe_key=dedupe_key,
            payload=payload,
            message=message,
        )
        self.update_youtube_terminal(
            download_log_id,
            "youtube_success",
            terminal_metadata,
        )
        return job

    def update_youtube_terminal(
        self,
        download_log_id: int,
        outcome: str,
        metadata_dict: dict[str, Any],
    ) -> None:
        """Mirror of ``PipelineDB.update_youtube_terminal``.

        Merges ``metadata_dict`` onto the existing ``youtube_metadata``
        blob the way the production ``||`` JSONB operator would.
        """
        if outcome not in self._YOUTUBE_TERMINAL_OUTCOMES:
            raise ValueError(
                f"update_youtube_terminal: outcome must be one of "
                f"{sorted(self._YOUTUBE_TERMINAL_OUTCOMES)!r}, got {outcome!r}"
            )
        for entry in self.download_logs:
            if entry.id == download_log_id:
                entry.outcome = outcome
                merged: dict[str, Any] = dict(entry.youtube_metadata or {})
                merged.update(metadata_dict)
                entry.youtube_metadata = merged
                return

        # Production UPDATE silently no-ops if the id doesn't exist;
        # mirror that.

    def claim_next_youtube_pending(
        self,
        *,
        worker_id: str | None,
        limit: int = 1,
    ) -> list[dict[str, Any]]:
        rows = sorted(
            (entry for entry in self.download_logs
             if entry.source == "youtube"
             and entry.outcome == "youtube_running"
             and not (entry.youtube_metadata or {}).get("worker_claimed_at")),
            key=lambda e: (e.created_at, e.id),
        )[:int(limit)]
        claimed_at = _utcnow().isoformat()
        for entry in rows:
            metadata = dict(entry.youtube_metadata or {})
            metadata["worker_claimed_at"] = claimed_at
            metadata["worker_id"] = worker_id
            entry.youtube_metadata = metadata
        return [
            {
                "id": entry.id,
                "request_id": entry.request_id,
                "source": entry.source,
                "outcome": entry.outcome,
                "youtube_metadata": copy.deepcopy(entry.youtube_metadata)
                if entry.youtube_metadata is not None else None,
                "created_at": entry.created_at,
            }
            for entry in rows
        ]

    def find_orphan_youtube_running(self) -> list[int]:
        """Mirror of ``PipelineDB.find_orphan_youtube_running``."""
        rows = sorted(
            (entry for entry in self.download_logs
             if entry.source == "youtube"
             and entry.outcome == "youtube_running"
             and (entry.youtube_metadata or {}).get("worker_claimed_at")),
            key=lambda e: (e.created_at, e.id),
        )
        return [entry.id for entry in rows]

    def seed_youtube_album_mapping(
        self,
        release_group_identifier: str,
        source: str,
        rows: list[dict[str, Any]],
    ) -> None:
        """Populate the cache for a (release_group_identifier, source) pair.

        Test helper — bypasses the upsert path so tests can pre-seed state
        without exercising the replace semantics under test.
        """
        self._youtube_album_mappings[
            (release_group_identifier, source)
        ] = [copy.deepcopy(r) for r in rows]

    def get_youtube_album_mapping(
        self,
        release_group_identifier: str,
        source: str,
    ) -> list[dict[str, Any]] | None:
        """Return all rows for the pair, ordered by ``yt_browse_id`` ASC.

        Returns ``None`` when the pair has never been resolved, and an
        empty list when it has been resolved to an empty matrix. The
        distinction matters: ``[]`` means "we checked and found nothing"
        (cache HIT — don't re-poll YT), while ``None`` means "we have
        no record" (cache MISS — go ask YT).

        Mirrors the real PipelineDB contract; the resolver gate is
        ``if not refresh and cached_rows is not None``.
        """
        if (release_group_identifier, source) not in self._youtube_album_mappings:
            return None
        rows = self._youtube_album_mappings[
            (release_group_identifier, source)
        ]
        return sorted(
            (copy.deepcopy(r) for r in rows),
            key=lambda r: r["yt_browse_id"],
        )

    def find_youtube_album_mapping_for_release(
        self,
        *,
        source: str,
        release_id: str,
        browse_id: str,
    ) -> dict[str, Any] | None:
        for (rg_id, row_source), rows in self._youtube_album_mappings.items():
            if row_source != source:
                continue
            for row in rows:
                if str(row.get("yt_browse_id") or "") != browse_id:
                    continue
                distances = row.get("distances") or []
                if not any(
                    isinstance(entry, dict)
                    and str(entry.get("mbid") or "") == str(release_id)
                    for entry in distances
                ):
                    continue
                out = copy.deepcopy(row)
                out.setdefault("release_group_identifier", rg_id)
                out.setdefault("source", row_source)
                return out
        return None

    def upsert_youtube_album_mapping(
        self,
        release_group_identifier: str,
        source: str,
        rows: list[PersistedYoutubeRow],
    ) -> None:
        """Atomically replace the matrix for ``(release_group_identifier, source)``.

        Partial updates are not supported — refresh always replaces. The
        real implementation wraps DELETE + INSERTs in a single transaction;
        the fake just overwrites the dict slot, which is atomic in the
        single-threaded test context.

        Converts each ``PersistedYoutubeRow`` to the stored read-shape dict
        via ``msgspec.to_builtins`` and stamps ``id``,
        ``release_group_identifier``, ``source``, and ``resolved_at`` onto
        each stored row. Production's SELECT projection
        (``PipelineDB.get_youtube_album_mapping``) always includes these
        DB-assigned columns (``id BIGSERIAL PRIMARY KEY``, ``resolved_at
        TIMESTAMPTZ NOT NULL DEFAULT NOW()`` — migration 034) even though
        callers never pass them into ``rows``. #523's read-projection
        parity gate surfaced this: the fake previously echoed the input
        dict verbatim, four keys short of what production's read returns.
        """
        stored: list[dict[str, Any]] = []
        for row in rows:
            self._next_youtube_mapping_id += 1
            stored_row: dict[str, Any] = msgspec.to_builtins(row)
            stored_row["id"] = self._next_youtube_mapping_id
            stored_row["release_group_identifier"] = release_group_identifier
            stored_row["source"] = source
            stored_row.setdefault("yt_audio_playlist_id", None)
            stored_row.setdefault("yt_year", None)
            stored_row.setdefault("album_title", None)
            stored_row.setdefault("album_artist", None)
            stored_row["resolved_at"] = _utcnow()
            stored.append(stored_row)
        self._youtube_album_mappings[
            (release_group_identifier, source)
        ] = stored


