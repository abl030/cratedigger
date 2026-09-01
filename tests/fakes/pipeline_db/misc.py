"""FakePipelineDB misc cluster — mirrors ``lib/pipeline_db/misc.py``.

Denylist, cooldowns, peers, triage, and unfindable detection.
"""
from __future__ import annotations

import copy
from datetime import datetime
from typing import (
    Any,
)

from lib.pipeline_db import (
    BadAudioHashInput,
    BadAudioHashRow,
    ReplacedRequestMutationError,
)
from tests.fakes._shared import _utcnow
from tests.fakes.pipeline_db._base import _FakePipelineDBBase
from tests.fakes.rows import (
    DenylistEntry,
    FieldResolutionRow,
    UserCooldownRow,
)


class _FakeMiscMixin(_FakePipelineDBBase):
    """Denylist, cooldowns, peers, triage, and unfindable detection."""

    def add_denylist(self, request_id: int, username: str,
                     reason: str | None = None) -> None:
        if any(
            entry.request_id == request_id and entry.username == username
            for entry in self.denylist
        ):
            return
        self.denylist.append(DenylistEntry(request_id, username, reason))

    def get_denylisted_users(self, request_id: int) -> list[dict[str, Any]]:
        return [
            {"username": e.username, "reason": e.reason, "created_at": None}
            for e in self.denylist if e.request_id == request_id
        ]

    def list_denylist_rows(self) -> list[dict[str, Any]]:
        return [
            {
                "request_id": entry.request_id,
                "username": entry.username,
                "reason": entry.reason,
                "created_at": None,
            }
            for entry in sorted(
                self.denylist,
                key=lambda row: (row.request_id, row.username),
            )
        ]

    def add_bad_audio_hashes(
        self,
        request_id: int,
        reported_username: str | None,
        reason: str | None,
        hashes: list[BadAudioHashInput],
    ) -> int:
        """Insert bad-rip hashes; dedupe on (hash_value, audio_format)."""
        existing = {
            (row.hash_value, row.audio_format) for row in self.bad_audio_hashes
        }
        inserted = 0
        for h in hashes:
            key = (h.hash_value, h.audio_format)
            if key in existing:
                continue
            existing.add(key)
            self._next_bad_audio_hash_id += 1
            self.bad_audio_hashes.append(BadAudioHashRow(
                id=self._next_bad_audio_hash_id,
                hash_value=h.hash_value,
                audio_format=h.audio_format,
                request_id=request_id,
                reported_username=reported_username,
                reason=reason,
                reported_at=_utcnow(),
            ))
            inserted += 1
        return inserted

    def lookup_bad_audio_hash(
        self,
        hash_value: bytes,
        audio_format: str,
    ) -> BadAudioHashRow | None:
        self.lookup_bad_audio_hash_calls.append((hash_value, audio_format))
        for row in self.bad_audio_hashes:
            if row.hash_value == hash_value and row.audio_format == audio_format:
                return row
        return None

    def has_any_bad_audio_hashes(self) -> bool:
        self.has_any_bad_audio_hashes_calls += 1
        return bool(self.bad_audio_hashes)

    def check_and_apply_cooldown(self, username: str,
                                  config: Any = None) -> bool:
        self.cooldowns_applied.append(username)
        if callable(self._cooldown_result):
            return self._cooldown_result(username)
        return self._cooldown_result

    def record_field_resolution(
        self,
        request_id: int,
        field_name: str,
        status: str,
        reason_code: str | None,
    ) -> bool:
        """UPSERT a row into ``field_resolutions`` mirroring migration 030.

        On conflict: increment ``attempts``, replace status / reason,
        bump ``resolved_at``. Tests assert directly against the dict.
        """
        request = self._requests.get(int(request_id))
        if request is None or request.get("status") == "replaced":
            return False
        key = (int(request_id), field_name)
        now = _utcnow()
        existing = self.field_resolutions.get(key)
        if existing is None:
            self._next_field_resolution_id += 1
            self.field_resolutions[key] = FieldResolutionRow(
                request_id=int(request_id),
                field_name=field_name,
                status=status,
                reason_code=reason_code,
                attempts=1,
                resolved_at=now,
                id=self._next_field_resolution_id,
            )
            return True
        existing.status = status
        existing.reason_code = reason_code
        existing.attempts += 1
        existing.resolved_at = now
        return True

    def get_field_resolution(
        self,
        request_id: int,
        field_name: str,
    ) -> dict[str, Any] | None:
        """Return the side-table row for ``(request_id, field_name)`` as a dict."""
        row = self.field_resolutions.get((int(request_id), field_name))
        if row is None:
            return None
        return {
            "id": row.id,
            "request_id": row.request_id,
            "field_name": row.field_name,
            "resolved_at": row.resolved_at,
            "status": row.status,
            "reason_code": row.reason_code,
            "attempts": row.attempts,
        }

    # Mirrors the four new ``PipelineDB`` triage methods so the service
    # layer can be exercised without a real Postgres. Each method bumps
    # ``self.query_counts`` exactly once per invocation so the N+1 guard
    # test can assert ``sum(query_counts.values()) <= 5`` across the
    # cohort path.

    def list_triage_page(
        self,
        *,
        filter_spec: Any,
        page_size: int,
        after_request_id: int | None,
    ) -> list[dict[str, Any]]:
        """In-memory mirror of ``PipelineDB.list_triage_page``."""
        self.query_counts["list_triage_page"] = (
            self.query_counts.get("list_triage_page", 0) + 1
        )
        kind = getattr(filter_spec, "kind", None)
        unfindable_category = getattr(filter_spec, "unfindable_category", None)
        field_name = getattr(filter_spec, "field_name", None)
        status_code = getattr(filter_spec, "status_code", None)
        reason_code = getattr(filter_spec, "reason_code", None)

        def keep(row: dict[str, Any]) -> bool:
            if kind == "unfindable":
                if row.get("unfindable_category") is None:
                    return False
                return not (unfindable_category is not None and row.get("unfindable_category") != unfindable_category)
            if kind == "data_quality":
                rid = int(row["id"])
                matched = False
                for (resolution_rid, _fname), fr in self.field_resolutions.items():
                    if resolution_rid != rid:
                        continue
                    if not fr.status.startswith("unresolved_"):
                        continue
                    if field_name is not None and fr.field_name != field_name:
                        continue
                    if status_code is not None and fr.status != status_code:
                        continue
                    if reason_code is not None and fr.reason_code != reason_code:
                        continue
                    matched = True
                    break
                return matched
            if kind == "search_not_converting":
                summary = self._compute_search_summary(int(row["id"]))
                return (summary is not None
                        and summary["total_searches"] > 0
                        and summary["found_count"] == 0)
            if kind == "converged":
                return int(row["id"]) in self.convergence_signals
            if kind == "all":
                return True
            raise ValueError(f"unsupported triage filter kind: {kind!r}")

        rows = sorted(
            (r for r in self._requests.values() if keep(r)),
            key=lambda r: int(r["id"]),
        )
        if after_request_id is not None:
            rows = [r for r in rows if int(r["id"]) > int(after_request_id)]
        rows = rows[: int(page_size)]
        # Return projection mirroring the real SELECT list — kept
        # deliberately narrow so tests can't accidentally rely on a
        # column the production page query doesn't include.
        projection_keys = (
            "id", "artist_name", "album_title", "year", "status", "source",
            "mb_release_id", "discogs_release_id", "release_group_year",
            "is_va_compilation", "catalog_number", "failure_class",
            "search_filetype_override", "unfindable_category",
            "unfindable_categorised_at", "last_artist_probe_at",
            "last_artist_probe_match_count", "rescued_at",
            "prior_unfindable_category",
        )
        return [{k: r.get(k) for k in projection_keys} for r in rows]

    def get_field_resolutions_for_requests(
        self,
        request_ids: list[int],
    ) -> dict[int, list[dict[str, Any]]]:
        """In-memory mirror of ``PipelineDB.get_field_resolutions_for_requests``."""
        self.query_counts["get_field_resolutions_for_requests"] = (
            self.query_counts.get("get_field_resolutions_for_requests", 0) + 1
        )
        wanted = {int(r) for r in request_ids}
        out: dict[int, list[dict[str, Any]]] = {}
        # Order by (request_id, field_name) to mirror the production
        # ORDER BY clause.
        for (rid, _fn), fr in sorted(
            self.field_resolutions.items(), key=lambda kv: kv[0]
        ):
            if rid not in wanted:
                continue
            out.setdefault(rid, []).append({
                "id": fr.id,
                "request_id": fr.request_id,
                "field_name": fr.field_name,
                "resolved_at": fr.resolved_at,
                "status": fr.status,
                "reason_code": fr.reason_code,
                "attempts": fr.attempts,
            })
        return out

    def set_tracks(self, request_id: int,
                   tracks: list[dict[str, Any]]) -> None:
        row = self._requests.get(request_id)
        if row is None:
            raise ValueError(f"request {request_id} not found")
        if row.get("status") == "replaced":
            raise ReplacedRequestMutationError(request_id)
        self._tracks[request_id] = [
            {
                "disc_number": t.get("disc_number", 1),
                "track_number": t["track_number"],
                "title": t["title"],
                "length_seconds": t.get("length_seconds"),
                # PR2 U2 / R13: per-track artist from upstream payload.
                # Real PipelineDB stores this in album_tracks.track_artist
                # (migration 029). NULL is the legitimate default — the
                # resolver fills it later via ``update_track_artists``.
                "track_artist": t.get("track_artist"),
            }
            for t in tracks
        ]

    def get_tracks(self, request_id: int) -> list[dict[str, Any]]:
        rows = list(self._tracks.get(request_id, []))
        rows.sort(key=lambda t: (t["disc_number"], t["track_number"]))
        return [copy.deepcopy(t) for t in rows]

    def update_track_artists(
        self, request_id: int,
        track_artists: list[str | None],
        *,
        expected_status: str | None = None,
    ) -> bool:
        """Mirror of ``PipelineDB.update_track_artists`` — apply per-track
        artists in (disc, track) order. Length mismatches are tolerated
        (fewer keeps existing, more drops extras) — same shape as real.
        """
        request = self._requests.get(request_id)
        if (
            request is None
            or request.get("status") == "replaced"
            or (
                expected_status is not None
                and request.get("status") != expected_status
            )
        ):
            return False
        if not track_artists:
            return True
        rows = self._tracks.get(request_id, [])
        if not rows:
            return True
        rows.sort(key=lambda t: (t["disc_number"], t["track_number"]))
        for row, artist in zip(rows, track_artists, strict=False):
            row["track_artist"] = artist
        return True

    def get_track_counts(self,
                         request_ids: list[int]) -> dict[int, int]:
        return {
            rid: len(self._tracks[rid])
            for rid in request_ids
            if self._tracks.get(rid)
        }

    def get_slskd_event_cursor(self) -> dict[str, Any] | None:
        cursor = self._slskd_event_cursor
        return dict(cursor) if cursor is not None else None

    def upsert_slskd_event_cursor(
        self,
        last_event_id: str,
        last_event_timestamp: str,
    ) -> None:
        self._slskd_event_cursor = {
            "last_event_id": last_event_id,
            "last_event_timestamp": last_event_timestamp,
            "updated_at": _utcnow(),
        }

    def add_cooldown(self, username: str, cooldown_until: datetime,
                     reason: str | None = None) -> None:
        """Upsert a cooldown keyed by username."""
        existing = self.user_cooldowns.get(username)
        created_at = existing.created_at if existing is not None else _utcnow()
        self.user_cooldowns[username] = UserCooldownRow(
            username=username,
            cooldown_until=cooldown_until,
            reason=reason,
            created_at=created_at,
        )

    def get_cooled_down_users(self) -> list[str]:
        now = _utcnow()
        return [
            c.username for c in self.user_cooldowns.values()
            if c.cooldown_until > now
        ]

