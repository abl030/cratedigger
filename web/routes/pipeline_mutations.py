"""Pipeline mutation route handlers.

Split from web/routes/pipeline.py (#546 W4) — the CRUD mutation cluster
(add/update/upgrade/set-quality/set-intent/ban-source/force-import/delete).
GET/read routes (log, status, recent, all, search, downloading, detail,
requests-by-rg, active-rgs, import-jobs) stay in ``web/routes/pipeline.py``.
"""

import json
import logging
import urllib.error
from pathlib import Path
from typing import Literal

import msgspec
from pydantic import BaseModel, Field, model_validator

from web.routes._pydantic import parse_body
from web.routes._registry import RouteRegistration, route
from web.routes._server_access import _server
from web.routes.pipeline import _serialize_import_job

logger = logging.getLogger(__name__)

from lib import transitions

# Module-level DI seam for ``transitions.finalize_request``. Routes call
# this name (not ``transitions.finalize_request`` directly) so tests can
# swap it via ``patch.object(routes.pipeline_mutations, "finalize_request",
# new=...)`` at the same module-level scope as ``web.server.db``. See the
# leaf-seam allowlist in ``tests/_mock_audit_scanner.py``.
finalize_request = transitions.finalize_request
from lib.audio_hash import AudioHashError, hash_audio_content
from lib.import_queue import (
    IMPORT_JOB_FORCE,
    force_import_dedupe_key,
    force_import_payload,
)
from lib.pipeline_db import BadAudioHashInput
from lib.quality import (QUALITY_LOSSLESS, QUALITY_UPGRADE_TIERS,
                         resolve_user_requeue_override,
                         should_clear_lossless_search_override)
from lib.release_identity import detect_release_source, normalize_release_id
from lib.release_cleanup import remove_and_reset_release
from lib.util import resolve_failed_path
from lib.validation_envelope import decode_validation_envelope
from web import mb as mb_api
from web import discogs as discogs_api
from web.wrong_match_file_service import source_dirs_from_validation_result


def _resolve_and_update_after_add(
    db,
    req_id: int,
    *,
    mb_release_id: str | None,
    discogs_release_id: str | None,
    mb_release_group_id: str | None,
    mb_artist_id: str | None,
    mb_release_payload: dict | None = None,
    discogs_release_payload: dict | None = None,
):
    """U4 helper: run ``resolve_all`` against a freshly inserted request
    and persist the resolved fields plus the VA flag.

    ``resolve_all`` is best-effort by design (proceed-with-NULL on any
    upstream failure); we never raise back up to the HTTP handler. The
    side-table rows recorded by the resolver service are the operator
    visibility into unresolved fields. ``is_va_compilation`` is set ONCE
    at enqueue per the immutability invariant — the row reads back
    ``FALSE`` from the schema's default until this call lands the
    resolved value.

    Returns the ``ResolveAllResult`` so the caller can forward the
    resolved ``release_group_year`` into plan generation. The resolved
    values are also persisted via ``update_request_fields`` here, so the
    caller does not need to know which columns the resolver touches.
    """
    from lib.field_resolver_service import (
        ResolveAllResult,
        apply_resolve_all_result,
        resolve_all,
    )

    skeleton = {
        "id": req_id,
        "mb_release_id": mb_release_id,
        "discogs_release_id": discogs_release_id,
        "mb_release_group_id": mb_release_group_id,
        "mb_artist_id": mb_artist_id,
    }
    try:
        result = resolve_all(
            skeleton,
            db,
            mb_release_payload=mb_release_payload,
            discogs_release_payload=discogs_release_payload,
        )
    except Exception as exc:  # noqa: BLE001
        # ``resolve_all`` already catches every per-resolver failure
        # internally; the only thing that can escape is a programmer
        # error in the orchestrator itself. Log + proceed with defaults
        # so the add request still lands.
        logger.exception(
            "post_pipeline_add: resolve_all crashed for request %s: %s",
            req_id, exc,
        )
        return ResolveAllResult()

    try:
        apply_resolve_all_result(
            db, req_id, result,
            existing_mb_release_group_id=mb_release_group_id,
        )
    except Exception as exc:  # noqa: BLE001
        logger.exception(
            "post_pipeline_add: update_request_fields failed for "
            "request %s: %s", req_id, exc,
        )
    return result


def _generate_plan_after_add(req_id, *, artist_name, album_title, year,
                              tracks, source, release_group_year=None,
                              is_va_compilation=False,
                              catalog_number=None):
    """Run shared plan generation after `set_tracks()` on the add path.

    Failures are recorded but never bubble up — the request is repairable
    via startup reconciliation or explicit regeneration. This keeps the
    add API contract stable: a 200 response means the request landed,
    even if plan generation needs another attempt.

    ``release_group_year`` (U5 of search-plan-entropy) feeds the
    generator's conditional ``unwild_rg_year`` slot for reissues. Pass
    ``None`` when unknown — the generator handles it gracefully.

    PR2 Apply #2: ``is_va_compilation`` and ``catalog_number`` are
    forwarded so the initial plan respects the resolver's verdict — the
    add path runs resolver → apply → generate, so by the time this is
    called the caller has both values. Per-track ``track_artist`` flows
    through ``tracks`` (already persisted by ``apply_resolve_all_result``
    → ``update_track_artists`` upstream, then re-read via ``get_tracks``
    in the caller).
    """
    from lib.config import read_runtime_config
    from lib.search_plan_service import SearchPlanService
    s = _server()
    try:
        svc = SearchPlanService(s._db(), read_runtime_config())
        svc.generate_for_new_request(
            req_id,
            artist_name=artist_name,
            album_title=album_title,
            year=year,
            tracks=tracks or [],
            source=source,
            release_group_year=release_group_year,
            is_va_compilation=is_va_compilation,
            catalog_number=catalog_number,
        )
    except Exception as exc:  # noqa: BLE001
        # Never fail the HTTP request because plan generation hiccupped.
        logger.warning(
            "post_pipeline_add: plan generation failed for request %s: %s",
            req_id, exc,
        )


class PipelineAddRequest(BaseModel):
    """HTTP body for ``POST /api/pipeline/add``.

    At least one of ``mb_release_id`` / ``discogs_release_id`` is required;
    the @model_validator below enforces that. Both IDs are normalised
    after parsing (downcase, strip) by ``normalize_release_id`` inside
    the handler — keeping normalisation in route logic, not in the model,
    matches how other handlers consume these fields.
    """

    mb_release_id: str | None = None
    discogs_release_id: str | None = None
    source: str = "request"

    @model_validator(mode="after")
    def _at_least_one_release_id(self) -> "PipelineAddRequest":
        if not self.mb_release_id and not self.discogs_release_id:
            raise ValueError("Missing mb_release_id or discogs_release_id")
        return self


def post_pipeline_add(h, body: dict) -> None:
    req = parse_body(h, body, PipelineAddRequest)
    if req is None:
        return
    s = _server()
    mbid = normalize_release_id(req.mb_release_id)
    discogs_id = normalize_release_id(req.discogs_release_id)
    source = req.source

    if discogs_id:
        # Discogs flow: store discogs ID in both columns for pipeline compat
        existing = s._db().get_request_by_release_id(discogs_id)
        if existing:
            payload: dict[str, object] = {
                "status": "exists",
                "id": existing["id"],
                "current_status": existing["status"],
            }
            if existing["status"] == "replaced":
                descendant = s._db().get_request_by_replaces_request_id(
                    existing["id"])
                if descendant is not None:
                    payload["descendant_request_id"] = descendant["id"]
                    payload["descendant_status"] = descendant.get("status")
            h._json(payload)
            return

        # Bypass the 24h meta cache — this write path persists artist /
        # title / tracks into `album_requests`. A stale cached snapshot
        # would silently bake yesterday's pre-correction metadata into
        # the pipeline DB (Codex review, issue #101).
        release = discogs_api.get_release(int(discogs_id), fresh=True)

        req_id = s._db().add_request(
            mb_release_id=discogs_id,
            discogs_release_id=discogs_id,
            mb_artist_id=str(release.get("artist_id") or ""),
            artist_name=release["artist_name"],
            album_title=release["title"],
            year=release.get("year"),
            country=release.get("country"),
            source=source,
        )

        if release.get("tracks"):
            s._db().set_tracks(req_id, release["tracks"])

        # U4: inline field resolution + VA detection. Discogs branch
        # never has an MB release/release-group payload, so the
        # resolver only sees the discogs release payload (Rule 1 of
        # VA detection covers the canonical ID match; rules 2 + 3 are
        # MB-only).
        resolved = _resolve_and_update_after_add(
            s._db(),
            req_id,
            mb_release_id=None,
            discogs_release_id=discogs_id,
            mb_release_group_id=None,
            mb_artist_id=str(release.get("artist_id") or "") or None,
            discogs_release_payload=release,
        )

        # Re-read tracks from the DB so the per-track ``track_artist``
        # column the resolver just wrote (PR2 Apply #1) flows into the
        # snapshot. The in-memory ``release["tracks"]`` is the raw
        # upstream payload and does NOT carry the resolver's output.
        post_resolve_tracks = s._db().get_tracks(req_id)
        _generate_plan_after_add(
            req_id,
            artist_name=release["artist_name"],
            album_title=release["title"],
            year=release.get("year"),
            tracks=post_resolve_tracks,
            source=source,
            release_group_year=resolved.release_group_year,
            is_va_compilation=resolved.is_va_compilation,
            catalog_number=resolved.catalog_number,
        )

        h._json({
            "status": "added",
            "id": req_id,
            "artist": release["artist_name"],
            "album": release["title"],
            "tracks": len(release.get("tracks", [])),
        })
        return

    # MusicBrainz flow
    existing = s._db().get_request_by_release_id(mbid)
    if existing:
        payload: dict[str, object] = {
            "status": "exists",
            "id": existing["id"],
            "current_status": existing["status"],
        }
        # R33 / U10: when the existing row is a frozen audit row from a
        # past Replace, surface the descendant id so the UI can render a
        # "previously abandoned — active request is at /pipeline/<id>"
        # forward-link instead of the generic "already in pipeline"
        # message.
        if existing["status"] == "replaced":
            descendant = s._db().get_request_by_replaces_request_id(
                existing["id"])
            if descendant is not None:
                payload["descendant_request_id"] = descendant["id"]
                payload["descendant_status"] = descendant.get("status")
        h._json(payload)
        return

    # Bypass the 24h meta cache — same reason as the Discogs branch
    # above. Writing stale metadata into the pipeline DB is worse than
    # an extra MB mirror round trip on add.
    release = mb_api.get_release(mbid, fresh=True)
    # The resolver service needs the full raw MB JSON (``label-info``
    # for catalog_number, per-track ``artist-credit`` for track_artist,
    # nested ``release-group`` primary-type for VA Rule 2 — none of
    # which survive ``get_release`` stripping). ``get_release`` calls
    # ``get_release_raw`` internally so this is a single network round
    # trip; the second call is a cache hit.
    release_raw = mb_api.get_release_raw(mbid, fresh=True)

    rg_id = release.get("release_group_id")

    req_id = s._db().add_request(
        mb_release_id=mbid,
        mb_release_group_id=rg_id,
        mb_artist_id=release.get("artist_id"),
        artist_name=release["artist_name"],
        album_title=release["title"],
        year=release.get("year"),
        country=release.get("country"),
        source=source,
    )

    if release.get("tracks"):
        s._db().set_tracks(req_id, release["tracks"])

    # U4: inline field resolution + VA detection. The resolver service
    # is the single source of truth for ``release_group_year`` (and
    # other R15 fields); proceed-with-NULL when the mirror is unreachable
    # or the field is missing upstream. ``is_va_compilation`` is set
    # ONCE at enqueue per the immutability invariant.
    resolved = _resolve_and_update_after_add(
        s._db(),
        req_id,
        mb_release_id=mbid,
        discogs_release_id=None,
        mb_release_group_id=rg_id,
        mb_artist_id=release.get("artist_id"),
        mb_release_payload=release_raw,
    )

    # Re-read tracks from the DB so the per-track ``track_artist``
    # column the resolver just wrote (PR2 Apply #1) flows into the
    # snapshot. The in-memory ``release["tracks"]`` is the raw upstream
    # payload and does NOT carry the resolver's output.
    post_resolve_tracks = s._db().get_tracks(req_id)
    _generate_plan_after_add(
        req_id,
        artist_name=release["artist_name"],
        album_title=release["title"],
        year=release.get("year"),
        tracks=post_resolve_tracks,
        source=source,
        release_group_year=resolved.release_group_year,
        is_va_compilation=resolved.is_va_compilation,
        catalog_number=resolved.catalog_number,
    )

    h._json({
        "status": "added",
        "id": req_id,
        "artist": release["artist_name"],
        "album": release["title"],
        "tracks": len(release.get("tracks", [])),
    })


class PipelineUpdateRequest(BaseModel):
    id: int = Field(gt=0)
    status: Literal["wanted", "imported", "manual"]


def post_pipeline_update(h, body: dict) -> None:
    req_body = parse_body(h, body, PipelineUpdateRequest)
    if req_body is None:
        return
    s = _server()
    req_id = req_body.id
    new_status = req_body.status

    req = s._db().get_request(int(req_id))
    if not req:
        h._error("Not found", 404)
        return

    if new_status == "wanted" and req["status"] != "wanted":
        mbid = req.get("mb_release_id")
        quality = None
        min_br = None
        b = s._beets_db()
        if mbid and b:
            if b.album_exists(mbid):
                # Preserve a stricter existing override (e.g. "lossless"
                # set by the quality gate) — reverting status shouldn't
                # re-open tiers the gate intentionally closed.
                quality = resolve_user_requeue_override(
                    req.get("search_filetype_override"))
                min_br = b.get_min_bitrate(mbid)
        wanted_fields: dict[str, object] = {}
        if quality is not None:
            wanted_fields["search_filetype_override"] = quality
        if min_br is not None:
            wanted_fields["min_bitrate"] = min_br
        finalize_request(
            s._db(),
            int(req_id),
            transitions.RequestTransition.to_wanted_fields(
                from_status=req["status"],
                fields=wanted_fields,
            ),
        )
    else:
        finalize_request(
            s._db(),
            int(req_id),
            transitions.RequestTransition.status_only(
                new_status,
                from_status=req["status"],
            ),
        )

    h._json({"status": "ok", "id": req_id, "new_status": new_status})


class PipelineUpgradeRequest(BaseModel):
    """HTTP body for ``POST /api/pipeline/upgrade``."""

    mb_release_id: str = Field(min_length=1)


def post_pipeline_upgrade(h, body: dict) -> None:
    req = parse_body(h, body, PipelineUpgradeRequest)
    if req is None:
        return
    s = _server()
    mbid = normalize_release_id(req.mb_release_id)
    if not mbid:
        # ``normalize_release_id`` strips/lowercases and can return None
        # for whitespace-only inputs that passed the min_length=1 check.
        h._error("Missing mb_release_id")
        return

    source = detect_release_source(mbid)

    min_bitrate = None
    b = s._beets_db()
    if b:
        min_bitrate = b.get_min_bitrate(mbid)

    existing = s._db().get_request_by_release_id(mbid)
    if existing:
        # Preserve a stricter existing override (e.g. "lossless" set by
        # the quality gate after a CBR 320 import) so clicking Upgrade
        # doesn't re-open tiers the gate already closed, which would
        # re-enqueue same-quality MP3 sources that get rejected as
        # downgrades in a loop.
        quality = resolve_user_requeue_override(
            existing.get("search_filetype_override"))
        req_id = existing["id"]
        transition_fields: dict[str, object] = {
            "search_filetype_override": quality,
        }
        if min_bitrate is not None:
            transition_fields["min_bitrate"] = min_bitrate
        finalize_request(
            s._db(),
            req_id,
            transitions.RequestTransition.to_wanted_fields(
                from_status=existing["status"],
                fields=transition_fields,
            ),
        )
        h._json({
            "status": "upgrade_queued",
            "id": req_id,
            "min_bitrate": min_bitrate,
            "search_filetype_override": quality,
        })
    else:
        # Brand-new request — no prior override to preserve.
        quality = QUALITY_UPGRADE_TIERS
        # Discogs upgrade leaves release_group_year NULL (no MB release-group).
        rg_year_upgrade: int | None = None
        # Bypass the 24h meta cache — both branches persist metadata
        # into the pipeline DB (artist / title / tracks). Stale cache
        # reads would silently bake pre-correction data from an earlier
        # browse. Cheap extra mirror hit on a write path.
        if source == "discogs":
            release = discogs_api.get_release(int(mbid), fresh=True)
            req_id = s._db().add_request(
                mb_release_id=mbid,
                discogs_release_id=mbid,
                mb_artist_id=str(release.get("artist_id") or ""),
                artist_name=release["artist_name"],
                album_title=release["title"],
                year=release.get("year"),
                country=release.get("country"),
                source="request",
            )
        else:
            release = mb_api.get_release(mbid, fresh=True)
            rg_id_upgrade = release.get("release_group_id")
            # ``get_release_group_year`` now propagates ``HTTPError(404)``
            # so the resolver service can disambiguate "MBID does not
            # exist" from "exists but missing year". On this orphan-
            # upgrade path we don't care about the distinction — leaving
            # the column NULL when the rg doesn't exist or has no year
            # is the right thing — so we treat both as None locally.
            rg_year_upgrade: int | None = None
            if rg_id_upgrade:
                try:
                    rg_year_upgrade = mb_api.get_release_group_year(
                        rg_id_upgrade)
                except urllib.error.HTTPError as exc:
                    if exc.code != 404:
                        raise
                    rg_year_upgrade = None
            req_id = s._db().add_request(
                mb_release_id=mbid,
                mb_release_group_id=rg_id_upgrade,
                mb_artist_id=release.get("artist_id"),
                artist_name=release["artist_name"],
                album_title=release["title"],
                year=release.get("year"),
                release_group_year=rg_year_upgrade,
                country=release.get("country"),
                source="request",
            )
        if release.get("tracks"):
            s._db().set_tracks(req_id, release["tracks"])
        _generate_plan_after_add(
            req_id,
            artist_name=release["artist_name"],
            album_title=release["title"],
            year=release.get("year"),
            tracks=release.get("tracks") or [],
            source="request",
            release_group_year=rg_year_upgrade,
        )
        # Newly added request — status is already 'wanted', set quality override
        finalize_request(
            s._db(),
            req_id,
            transitions.RequestTransition.to_wanted(
                from_status="wanted",
                search_filetype_override=quality,
                min_bitrate=min_bitrate,
            ),
        )
        h._json({
            "status": "upgrade_queued",
            "id": req_id,
            "min_bitrate": min_bitrate,
            "search_filetype_override": quality,
            "created": True,
        })


class PipelineSetQualityRequest(BaseModel):
    mb_release_id: str = Field(min_length=1)
    status: Literal["", "wanted", "imported", "manual"] = ""
    min_bitrate: int | None = None


def post_pipeline_set_quality(h, body: dict) -> None:
    req_body = parse_body(h, body, PipelineSetQualityRequest)
    if req_body is None:
        return
    s = _server()
    mbid = normalize_release_id(req_body.mb_release_id)
    new_status = req_body.status
    min_bitrate = req_body.min_bitrate

    if not mbid:
        h._error("Missing mb_release_id")
        return

    existing = s._db().get_request_by_release_id(mbid)
    if not existing:
        h._error("Not found in pipeline", 404)
        return

    req_id = existing["id"]

    if min_bitrate is not None:
        min_bitrate = int(min_bitrate)
        s._db().update_request_fields(req_id, min_bitrate=min_bitrate)

    if new_status:
        if new_status not in ("wanted", "imported", "manual"):
            h._error(f"Invalid status: {new_status}")
            return
        if new_status == "imported":
            if min_bitrate is None and mbid:
                b = s._beets_db()
                if b:
                    min_bitrate = b.get_avg_bitrate_kbps(mbid)
            imported_fields: dict[str, object] = {
                "search_filetype_override": None,
            }
            if min_bitrate is not None:
                imported_fields["min_bitrate"] = int(min_bitrate)
            finalize_request(
                s._db(),
                req_id,
                transitions.RequestTransition.to_imported_fields(
                    from_status=existing["status"],
                    fields=imported_fields,
                ),
            )
        elif new_status == "wanted" and existing["status"] != "wanted":
            finalize_request(
                s._db(),
                req_id,
                transitions.RequestTransition.to_wanted(
                    from_status=existing["status"]),
            )
        else:
            finalize_request(
                s._db(),
                req_id,
                transitions.RequestTransition.status_only(
                    new_status,
                    from_status=existing["status"],
                ),
            )

    h._json({
        "status": "ok",
        "id": req_id,
        "new_status": new_status or existing["status"],
        "min_bitrate": min_bitrate,
    })


class PipelineSetIntentRequest(BaseModel):
    """HTTP body for ``POST /api/pipeline/set-intent``.

    ``intent`` aliases (``flac``/``flac_only`` → ``lossless``,
    ``best_effort``/``upgrade`` → ``default``) are normalised inside the
    handler, not the model — the model accepts any string and the
    handler validates against the canonical set after the alias swap.
    """

    id: int = Field(gt=0)
    intent: str = ""


def post_pipeline_set_intent(h, body: dict) -> None:
    """Toggle lossless-on-disk intent for a pipeline request.

    Accepts intent: "lossless" (keep lossless on disk) or "default" (pipeline decides).
    Backward compat: "flac", "flac_only" → "lossless"; "best_effort" → "default".
    """
    req_body = parse_body(h, body, PipelineSetIntentRequest)
    if req_body is None:
        return
    s = _server()
    req_id = req_body.id
    intent_str = req_body.intent.strip()

    # Normalize to toggle: lossless or default
    _ALIASES = {"flac": "lossless", "flac_only": "lossless",
                "best_effort": "default", "upgrade": "default"}
    intent_str = _ALIASES.get(intent_str, intent_str)
    if intent_str not in ("lossless", "default"):
        h._error(f"Invalid intent: {intent_str!r}. Valid: lossless, default")
        return

    target_format = QUALITY_LOSSLESS if intent_str == "lossless" else None

    req = s._db().get_request(int(req_id))
    if not req:
        h._error("Not found", 404)
        return

    if req["status"] == "downloading":
        h._error("Cannot set intent while album is downloading")
        return

    if req["status"] == "imported" and target_format:
        # Re-queue to search for lossless source
        min_br = req.get("min_bitrate")
        finalize_request(
            s._db(),
            int(req_id),
            transitions.RequestTransition.to_wanted(
                from_status="imported",
                search_filetype_override=QUALITY_LOSSLESS,
                min_bitrate=min_br,
            ),
        )
        s._db().update_request_fields(int(req_id), target_format=target_format)
        h._json({
            "status": "ok",
            "id": int(req_id),
            "intent": intent_str,
            "target_format": target_format,
            "requeued": True,
        })
    else:
        # Just update the persistent intent for next search (wanted or manual)
        update_fields = {"target_format": target_format}
        if should_clear_lossless_search_override(
            new_target_format=target_format,
            old_target_format=req.get("target_format"),
            search_filetype_override=req.get("search_filetype_override"),
        ):
            update_fields["search_filetype_override"] = None
        s._db().update_request_fields(int(req_id), **update_fields)
        h._json({
            "status": "ok",
            "id": int(req_id),
            "intent": intent_str,
            "target_format": target_format,
            "requeued": False,
        })


class PipelineBanSourceRequest(BaseModel):
    request_id: int = Field(gt=0)
    mb_release_id: str = Field(min_length=1)
    username: str | None = None


def post_pipeline_ban_source(h, body: dict) -> None:
    req_body = parse_body(h, body, PipelineBanSourceRequest)
    if req_body is None:
        return
    s = _server()
    req_id = req_body.request_id
    username_in = req_body.username.strip() if req_body.username else ""
    mb_release_id = normalize_release_id(req_body.mb_release_id)

    if not mb_release_id:
        # ``normalize_release_id`` can strip whitespace down to None
        # even when the min_length=1 raw input passed Pydantic.
        h._error("Missing mb_release_id")
        return

    db = s._db()
    request_id_int = int(req_id)

    # E1.3: race-check — never run the bad-rip flow against a release
    # the importer is actively touching. The importer's beets-mutating
    # window can overlap with the file paths we're about to hash and
    # ``beet remove`` here; bail with 409 so the curator retries.
    active_job = db.get_active_import_job_for_request(request_id_int)
    if active_job is not None:
        h._json(
            {"error": "importer_busy", "retry_after_seconds": 30},
            status=409,
        )
        return

    # Resolve `reported_username` server-side (R3). Body still accepts
    # an explicit `username` for back-compat with non-UI callers; the
    # web UI no longer sends it. If neither side resolves a user, the
    # ban still proceeds (E1.1) — hashes are recorded with NULL and
    # no denylist row is written.
    reported_username: str | None = (
        username_in if username_in
        else db.get_recent_successful_uploader(request_id_int)
    )

    reason = "manually banned via web UI"

    # Hash capture MUST happen before ``remove_and_reset_release``
    # because that call deletes the underlying audio files. R6: a
    # per-track hash failure must not block the ban — accumulate
    # those failures and surface them in ``partial_failures``.
    hash_capture_errors: list[dict[str, object]] = []
    hashes: list[BadAudioHashInput] = []
    b = s._beets_db()
    if b:
        item_paths = b.get_item_paths(mb_release_id)
    else:
        item_paths = []

    if not item_paths:
        # E1.2: album not in beets (or release id mismatch). Don't
        # 404 — the user clicked "bad rip", they want the album gone
        # regardless. Surface as a hash_capture_error so the toast
        # explains why no hashes were recorded.
        hash_capture_errors.append({
            "track_path": None,
            "reason": "no_tracks_in_beets",
        })
    else:
        for _item_id, raw_path in item_paths:
            track_path = Path(raw_path)
            fmt = track_path.suffix.lstrip(".").lower()
            try:
                digest = hash_audio_content(track_path, fmt)
            except AudioHashError as exc:
                hash_capture_errors.append({
                    "track_path": str(track_path),
                    "reason": str(exc),
                })
                continue
            except Exception as exc:  # pragma: no cover — defensive
                hash_capture_errors.append({
                    "track_path": str(track_path),
                    "reason": f"unexpected error: {exc}",
                })
                continue
            hashes.append(BadAudioHashInput(
                hash_value=digest,
                audio_format=fmt,
            ))

    # Insert hashes BEFORE the denylist + remove so a downstream
    # failure (e.g. denylist DB error) still leaves the bad-byte
    # ripple-stop in place. ``add_bad_audio_hashes`` handles
    # ON CONFLICT (hash, format) DO NOTHING — re-clicks are no-ops.
    hashes_recorded = 0
    if hashes:
        hashes_recorded = db.add_bad_audio_hashes(
            request_id_int,
            reported_username,
            reason,
            hashes,
        )

    # Denylist only when we resolved a user (E1.1). When the route
    # was driven without a uploader-on-record, the bytes are still
    # protected via ``bad_audio_hashes``; nothing useful to denylist.
    if reported_username:
        db.add_denylist(request_id_int, reported_username, reason)

    # Atomic pair (issue #121): if the album is in beets, run
    # ``beet remove -d`` across every selector the release ID could
    # live under (UUID → ``mb_albumid`` only; Discogs numeric →
    # ``discogs_albumid`` AND ``mb_albumid`` so both new-layout and
    # legacy imports are covered). Once beets no longer holds it
    # (whether this handler just removed it or a prior ``beet rm``
    # did), clear the pipeline DB's on-disk quality fields in the
    # same call so nothing downstream reasons about ghost state.
    #
    # Issue #123 PR B: ``remove_and_reset_release`` now returns a
    # typed result. ``selector_failures`` surfaces per-selector
    # problems (timeout, non-zero rc, exception) so the ban-source
    # handler can tell a user the ban succeeded but the remove was
    # incomplete, rather than silently reporting success after a
    # denylist-committed / album-still-on-disk split brain.
    beets_removed = False
    cleanup_errors: list[dict[str, object]] = []
    if mb_release_id and b:
        cleanup = remove_and_reset_release(
            beets_db=b,
            pipeline_db=db,
            release_id=mb_release_id,
            request_id=request_id_int,
        )
        beets_removed = cleanup.beets_removed
        # ``msgspec.to_builtins`` so future fields on ``SelectorFailure``
        # (e.g. a timestamp) propagate to the route response without
        # anyone having to remember to update the literal here (issue
        # #123 PR B review feedback; ``SelectorFailure`` is a
        # ``msgspec.Struct`` post-issue #141).
        cleanup_errors = [msgspec.to_builtins(f)
                          for f in cleanup.selector_failures]

    req = db.get_request(request_id_int)
    if req:
        quality = resolve_user_requeue_override(
            req.get("search_filetype_override"))
        min_br = req.get("min_bitrate")
        ban_fields: dict[str, object] = {
            "search_filetype_override": quality,
        }
        if min_br is not None:
            ban_fields["min_bitrate"] = min_br
        finalize_request(
            db,
            request_id_int,
            transitions.RequestTransition.to_wanted_fields(
                from_status=req["status"],
                fields=ban_fields,
            ),
        )

    partial_failures: dict[str, list[dict[str, object]]] = {}
    if cleanup_errors:
        partial_failures["cleanup_errors"] = cleanup_errors
    if hash_capture_errors:
        partial_failures["hash_capture_errors"] = hash_capture_errors

    # Record the ban as a download_log event (#188 follow-up). It's just
    # another event — surfacing it through the same audit channel makes
    # it appear uniformly on recents, the pipeline tab's "last:" verdict
    # line, and per-row download history without per-surface plumbing.
    ban_detail = (
        f"Marked bad rip; {hashes_recorded} hashes captured"
        if hashes_recorded > 0
        else "Marked bad rip (no tracks hashed)"
    )
    ban_validation = json.dumps({
        "scenario": "curator_ban",
        "hashes_recorded": hashes_recorded,
        "denylisted_username": reported_username,
        "reason": reason,
        "cleanup_errors": cleanup_errors,
        "hash_capture_errors": hash_capture_errors,
    })
    db.log_download(
        request_id=request_id_int,
        soulseek_username=reported_username,
        outcome="curator_ban",
        beets_detail=ban_detail,
        validation_result=ban_validation,
    )

    payload: dict[str, object] = {
        "status": "ok",
        "username": reported_username,
        "beets_removed": beets_removed,
        "hashes_recorded": hashes_recorded,
    }
    if partial_failures:
        payload["partial_failures"] = partial_failures

    h._json(payload)


class PipelineForceImportRequest(BaseModel):
    download_log_id: int = Field(gt=0)


def post_pipeline_force_import(h, body: dict) -> None:
    req_body = parse_body(h, body, PipelineForceImportRequest)
    if req_body is None:
        return
    s = _server()
    log_id = req_body.download_log_id

    entry = s._db().get_download_log_entry(int(log_id))
    if not entry:
        h._error(f"Download log entry {log_id} not found", 404)
        return

    request_id = entry["request_id"]

    vr_raw = entry.get("validation_result")
    if not vr_raw:
        h._error("No validation_result on this download log entry")
        return
    vr = decode_validation_envelope(vr_raw)
    failed_path = vr.failed_path
    if not failed_path:
        h._error("No failed_path in validation_result")
        return

    req = s._db().get_request(request_id)
    if not req:
        h._error(f"Album request {request_id} not found", 404)
        return

    resolved_path = resolve_failed_path(str(failed_path))
    if resolved_path is None:
        h._error(f"Files not found at: {failed_path}")
        return

    job = s._db().enqueue_import_job(
        IMPORT_JOB_FORCE,
        request_id=request_id,
        dedupe_key=force_import_dedupe_key(int(log_id)),
        payload=force_import_payload(
            download_log_id=int(log_id),
            failed_path=resolved_path,
            source_username=entry.get("soulseek_username"),
            source_dirs=source_dirs_from_validation_result(vr),
        ),
        message=f"Force import queued for {req['artist_name']} - {req['album_title']}",
    )

    h._json({
        "status": "queued",
        "job_id": job.id,
        "job": _serialize_import_job(job),
        "deduped": bool(getattr(job, "deduped", False)),
        "request_id": request_id,
        "artist": req["artist_name"],
        "album": req["album_title"],
        "message": "Import queued",
    }, status=202)


class PipelineDeleteRequest(BaseModel):
    id: int = Field(gt=0)


def post_pipeline_delete(h, body: dict) -> None:
    req_body = parse_body(h, body, PipelineDeleteRequest)
    if req_body is None:
        return
    s = _server()
    req_id = req_body.id
    db = s._db()
    req = db.get_request(int(req_id))
    if not req:
        h._error("Not found", 404)
        return
    # ``album_requests.replaces_request_id`` uses ON DELETE RESTRICT
    # (migration 023) so a descendant Replace blocks deletion of the
    # frozen ancestor. Surface 409 with the descendant chain rather
    # than letting psycopg2 raise a 500 from the FK violation.
    descendant = db.get_request_by_replaces_request_id(int(req_id))
    if descendant is not None:
        descendant_ids: list[int] = []
        cursor: dict | None = descendant
        while cursor is not None:
            descendant_ids.append(int(cursor["id"]))
            cursor = db.get_request_by_replaces_request_id(int(cursor["id"]))
        h._json({
            "error": (
                f"request {req_id} is referenced by a superseding "
                "request — delete descendants first"
            ),
            "descendant_request_ids": descendant_ids,
        }, status=409)
        return
    import psycopg2.errors
    try:
        db.delete_request(int(req_id))
    except psycopg2.errors.ForeignKeyViolation as exc:
        # Defensive — a descendant landed between the read above and
        # the delete. Re-walk the chain so the operator gets the same
        # 409 response shape.
        descendant_ids = []
        descendant = db.get_request_by_replaces_request_id(int(req_id))
        cursor = descendant
        while cursor is not None:
            descendant_ids.append(int(cursor["id"]))
            cursor = db.get_request_by_replaces_request_id(int(cursor["id"]))
        h._json({
            "error": (
                f"request {req_id} is referenced by a superseding "
                f"request — delete descendants first ({exc})"
            ),
            "descendant_request_ids": descendant_ids,
        }, status=409)
        return
    h._json({"status": "ok", "id": req_id})


# ── Route tables ─────────────────────────────────────────────────

ROUTES: list[RouteRegistration] = [
    route(
        "POST", "/api/pipeline/add", post_pipeline_add,
        "Add a new pipeline request by MB or Discogs release id.",
        classified=True,
    ),
    route(
        "POST", "/api/pipeline/update", post_pipeline_update,
        "Change the status of a pipeline request.",
        classified=True,
    ),
    route(
        "POST", "/api/pipeline/upgrade", post_pipeline_upgrade,
        "Queue an upgrade search for a release (lossless tiers, MB / "
        "Discogs aware).",
        classified=True,
    ),
    route(
        "POST", "/api/pipeline/set-quality", post_pipeline_set_quality,
        "Set a request's min_bitrate and/or status.",
        classified=True,
    ),
    route(
        "POST", "/api/pipeline/set-intent", post_pipeline_set_intent,
        "Toggle lossless-on-disk intent for a request.",
        classified=True,
    ),
    route(
        "POST", "/api/pipeline/ban-source", post_pipeline_ban_source,
        "Mark a rip as bad: denylist the uploader, hash + bad-byte "
        "ripple-stop, and remove from beets.",
        classified=True,
    ),
    route(
        "POST", "/api/pipeline/force-import", post_pipeline_force_import,
        "Enqueue a force-import job for a rejected download_log row.",
        classified=True,
    ),
    route(
        "POST", "/api/pipeline/delete", post_pipeline_delete,
        "Delete a pipeline request (blocked when a superseding "
        "request exists).",
        classified=True,
    ),
]
