"""Pipeline API route handlers, extracted from server.py."""

import json
import re
import msgspec

from lib import transitions
from web.download_history_view import (
    build_download_history_row,
    build_download_history_rows,
    classify_download_log_row,
)
from lib.quality import (QUALITY_LOSSLESS, QUALITY_UPGRADE_TIERS,
                         resolve_user_requeue_override,
                         should_clear_lossless_search_override,
                         get_decision_tree, full_pipeline_decision)
from lib.release_identity import detect_release_source, normalize_release_id
from lib.release_cleanup import remove_and_reset_release
from lib.util import resolve_failed_path
from lib.spectral_check import (HF_DEFICIT_SUSPECT, HF_DEFICIT_MARGINAL,
                                ALBUM_SUSPECT_PCT, MIN_CLIFF_SLICES,
                                CLIFF_THRESHOLD_DB_PER_KHZ)
from web import mb as mb_api
from web import discogs as discogs_api


def _server():
    """Deferred import to avoid circular deps."""
    from web import server
    return server

# ── GET handlers ─────────────────────────────────────────────────


def get_pipeline_log(h, params: dict[str, list[str]]) -> None:
    outcome_filter = params.get("outcome", [None])[0]
    if outcome_filter not in (None, "imported", "rejected"):
        outcome_filter = None
    entries = _server()._db().get_log(limit=50, outcome_filter=outcome_filter)
    mbids = list(set(e["mb_release_id"] for e in entries if e.get("mb_release_id")))
    beets_info = _server().check_beets_library_detail(mbids) if mbids else {}
    result = []
    for e in entries:
        classified_row = classify_download_log_row(e)
        entry = classified_row.entry
        classified = classified_row.classified
        item = entry.to_json_dict()
        mbid = entry.mb_release_id
        bi = beets_info.get(mbid) if mbid else None
        item["in_beets"] = bi is not None
        if bi:
            item["beets_format"] = bi.get("beets_format")
            item["beets_bitrate"] = bi.get("beets_bitrate")
        item["badge"] = classified.badge
        item["badge_class"] = classified.badge_class
        item["border_color"] = classified.border_color
        item["verdict"] = classified.verdict
        item["summary"] = classified.summary
        # Issue #130: surface post-import `beet move` failures so the
        # Recents tab can render a warning chip without forcing the
        # operator to query JSONB manually. Null on clean rows.
        item["disambiguation_failure"] = classified.disambiguation_failure
        item["disambiguation_detail"] = classified.disambiguation_detail
        result.append(item)
    # Count outcomes for filter buttons (single query, no limit)
    count_cur = _server()._db()._execute("""
        SELECT
            COUNT(*) AS total,
            COUNT(*) FILTER (WHERE outcome IN ('success', 'force_import')) AS imported
        FROM download_log
    """)
    count_row = count_cur.fetchone()
    total = count_row["total"] if count_row else 0
    imported_c = count_row["imported"] if count_row else 0
    h._json({
        "log": result,
        "counts": {
            "all": total,
            "imported": imported_c,
            "rejected": total - imported_c,
        },
    })


def get_pipeline_status(h, params: dict[str, list[str]]) -> None:
    counts = _server()._db().count_by_status()
    wanted = _server()._db().get_wanted(limit=50)
    h._json({
        "counts": counts,
        "wanted": [
            {
                "id": w["id"],
                "artist": w["artist_name"],
                "album": w["album_title"],
                "mb_release_id": w["mb_release_id"],
                "source": w["source"],
                "created_at": str(w["created_at"]),
            }
            for w in wanted
        ],
    })


def get_pipeline_recent(h, params: dict[str, list[str]]) -> None:
    s = _server()
    recent = s._db().get_recent(limit=20)
    mbids = [r["mb_release_id"] for r in recent if r.get("mb_release_id")]
    beets_info = s.check_beets_library_detail(mbids) if mbids else {}
    # Batch fetch track counts and download history
    ids = [int(r["id"]) for r in recent]
    track_counts = s._db().get_track_counts(ids)
    history_batch = s._db().get_download_history_batch(ids)
    serialized = []
    for r in recent:
        item = s._serialize_row(r)
        mbid = r.get("mb_release_id")
        item["pipeline_tracks"] = track_counts.get(r["id"], 0)
        if mbid and mbid in beets_info:
            item["in_beets"] = True
            bi = beets_info[mbid]
            item["beets_tracks"] = bi["beets_tracks"]
            for k in ("beets_format", "beets_bitrate", "beets_samplerate", "beets_bitdepth"):
                if bi.get(k):
                    item[k] = bi[k]
        else:
            # Issue #123: artist+album fuzzy fallback deleted. Legacy
            # rows with an untagged beets copy now honestly read as
            # 'not in library' — fuzzy LIKE matches could return a
            # track count for an unrelated sibling pressing by the
            # same artist, which misled the UI's 'already on disk'
            # signal.
            item["in_beets"] = False
            item["beets_tracks"] = 0
        history = history_batch.get(r["id"], [])
        success = next((dl for dl in history if dl.get("outcome") == "success"), None)
        if success:
            for k in ("soulseek_username", "filetype", "bitrate",
                      "sample_rate", "bit_depth", "is_vbr",
                      "was_converted", "original_filetype"):
                val = success.get(k)
                if val is not None:
                    item["dl_" + k] = val
        serialized.append(item)
    h._json({"recent": serialized})


def get_pipeline_all(h, params: dict[str, list[str]]) -> None:
    s = _server()
    counts = s._db().count_by_status()
    all_data: dict[str, object] = {"counts": counts}
    # Collect all items across statuses, then batch-fetch history
    status_items: dict[str, list[dict]] = {}
    all_ids: list[int] = []
    for status in ("wanted", "downloading", "imported", "manual"):
        rows = [s._serialize_row(r) for r in s._db().get_by_status(status)]
        status_items[status] = rows
        all_ids.extend([int(str(r["id"])) for r in rows])
    history_batch = s._db().get_download_history_batch(all_ids)
    for status in ("wanted", "downloading", "imported", "manual"):
        items = []
        for item in status_items[status]:
            history = history_batch.get(item["id"], [])
            if history:
                last = build_download_history_row(history[0])
                item["last_verdict"] = last.verdict
                item["last_outcome"] = last.outcome
                item["last_username"] = last.soulseek_username
                item["download_count"] = len(history)
            items.append(item)
        all_data[status] = items
    h._json(all_data)


def _runtime_rank_config():
    """Load the runtime QualityRankConfig from the same config.ini the main
    cratedigger process reads, so web simulator matches production dispatch."""
    from lib.config import read_runtime_rank_config  # type: ignore[import-not-found]

    return read_runtime_rank_config()


def get_pipeline_constants(h, params: dict[str, list[str]]) -> None:
    """Return decision tree structure + thresholds for the diagram.

    The runtime rank config is threaded into ``get_decision_tree`` so the
    transcode-detection threshold displayed in the UI tracks the live
    ``cfg.mp3_vbr.excellent`` (issue #66 follow-up). Without this, an
    operator who retuned the gate would see a stale Decisions tab while
    the actual pipeline ran at the new threshold.
    """
    rank_cfg = _runtime_rank_config()
    tree = get_decision_tree(cfg=rank_cfg)
    tree["constants"]["HF_DEFICIT_SUSPECT"] = HF_DEFICIT_SUSPECT
    tree["constants"]["HF_DEFICIT_MARGINAL"] = HF_DEFICIT_MARGINAL
    tree["constants"]["ALBUM_SUSPECT_PCT"] = ALBUM_SUSPECT_PCT
    tree["constants"]["MIN_CLIFF_SLICES"] = MIN_CLIFF_SLICES
    tree["constants"]["CLIFF_THRESHOLD_DB_PER_KHZ"] = CLIFF_THRESHOLD_DB_PER_KHZ
    # Expose the runtime rank config to the UI so the Decisions tab shows
    # the configured gate_min_rank, bitrate_metric, and the same-rank
    # tolerance. The frontend renders these three as labeled badges at
    # the top of the tab (issue #68).
    tree["constants"]["rank_gate_min_rank"] = rank_cfg.gate_min_rank.name
    tree["constants"]["rank_bitrate_metric"] = rank_cfg.bitrate_metric.value
    tree["constants"]["rank_within_tolerance_kbps"] = (
        rank_cfg.within_rank_tolerance_kbps)
    # Expose the runtime audio_check_mode so the simulator presets can
    # reflect deployments with `[Beets Validation] audio_check = off`.
    # Without this, the Decisions tab would claim corrupt downloads get
    # rejected even though run_preimport_gates() skips validation there
    # (issue #91 codex round 2).
    from lib.config import read_runtime_config  # type: ignore[import-not-found]
    tree["constants"]["audio_check_mode"] = read_runtime_config().audio_check_mode
    h._json(tree)


def get_pipeline_simulate(h, params: dict[str, list[str]]) -> None:
    """Run full_pipeline_decision() with query-string inputs."""

    def _str(key: str) -> str | None:
        v = params.get(key, [None])[0]
        return v if v else None

    def _int(key: str) -> int | None:
        v = _str(key)
        return int(v) if v else None

    def _bool(key: str) -> bool:
        v = _str(key)
        return v in ("true", "1", "yes") if v else False

    # is_vbr defaults to None (not False) so the simulator can tell
    # "not supplied, derive from is_cbr" apart from "explicit CBR".
    def _opt_bool(key: str) -> bool | None:
        v = _str(key)
        if v is None:
            return None
        return v in ("true", "1", "yes")

    result = full_pipeline_decision(
        is_flac=_bool("is_flac"),
        min_bitrate=_int("min_bitrate") or 0,
        is_cbr=_bool("is_cbr"),
        is_vbr=_opt_bool("is_vbr"),
        avg_bitrate=_int("avg_bitrate"),
        spectral_grade=_str("spectral_grade"),
        spectral_bitrate=_int("spectral_bitrate"),
        existing_min_bitrate=_int("existing_min_bitrate"),
        existing_avg_bitrate=_int("existing_avg_bitrate"),
        existing_spectral_bitrate=_int("existing_spectral_bitrate"),
        override_min_bitrate=_int("override_min_bitrate"),
        existing_format=_str("existing_format"),
        existing_is_cbr=_bool("existing_is_cbr"),
        new_format=_str("new_format"),
        post_conversion_min_bitrate=_int("post_conversion_min_bitrate"),
        converted_count=_int("converted_count") or 0,
        verified_lossless=_bool("verified_lossless"),
        target_format=_str("target_format"),
        verified_lossless_target=_str("verified_lossless_target"),
        # Preimport gate inputs (issue #91). Defaults preserve legacy simulator
        # behavior — a caller that omits these runs the pipeline as if audio
        # validation passed and the auto path flattened the download.
        audio_check_mode=_str("audio_check_mode") or "normal",
        audio_corrupt=_bool("audio_corrupt"),
        import_mode=_str("import_mode") or "auto",
        has_nested_audio=_bool("has_nested_audio"),
        cfg=_runtime_rank_config(),
    )
    h._json(result)


def get_pipeline_detail(h, params: dict[str, list[str]], req_id_str: str) -> None:
    s = _server()
    req_id = int(req_id_str)
    req = s._db().get_request(req_id)
    if not req:
        h._error("Not found", 404)
        return
    tracks = s._db().get_tracks(req_id)
    history = s._db().get_download_history(req_id)
    history_items = [item.to_dict() for item in build_download_history_rows(history)]
    result: dict[str, object] = {
        "request": s._serialize_row(req),
        "tracks": tracks,
        "history": history_items,
    }
    mbid = req.get("mb_release_id")
    b = s._beets_db()
    if mbid and b:
        tracks = b.get_tracks_by_mb_release_id(mbid)
        if tracks is not None:
            result["beets_tracks"] = tracks
    h._json(result)


# ── POST handlers ────────────────────────────────────────────────


def post_pipeline_add(h, body: dict) -> None:
    s = _server()
    mbid = normalize_release_id(body.get("mb_release_id"))
    discogs_id = normalize_release_id(body.get("discogs_release_id"))
    source = body.get("source", "request")

    if not mbid and not discogs_id:
        h._error("Missing mb_release_id or discogs_release_id")
        return

    if discogs_id:
        # Discogs flow: store discogs ID in both columns for pipeline compat
        existing = s._db().get_request_by_release_id(discogs_id)
        if existing:
            h._json({
                "status": "exists",
                "id": existing["id"],
                "current_status": existing["status"],
            })
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

        h._json({
            "status": "added",
            "id": req_id,
            "artist": release["artist_name"],
            "album": release["title"],
            "tracks": len(release.get("tracks", [])),
        })
        return

    # MusicBrainz flow (unchanged)
    existing = s._db().get_request_by_release_id(mbid)
    if existing:
        h._json({
            "status": "exists",
            "id": existing["id"],
            "current_status": existing["status"],
        })
        return

    # Bypass the 24h meta cache — same reason as the Discogs branch
    # above. Writing stale metadata into the pipeline DB is worse than
    # an extra MB mirror round trip on add.
    release = mb_api.get_release(mbid, fresh=True)

    req_id = s._db().add_request(
        mb_release_id=mbid,
        mb_release_group_id=release.get("release_group_id"),
        mb_artist_id=release.get("artist_id"),
        artist_name=release["artist_name"],
        album_title=release["title"],
        year=release.get("year"),
        country=release.get("country"),
        source=source,
    )

    if release.get("tracks"):
        s._db().set_tracks(req_id, release["tracks"])

    h._json({
        "status": "added",
        "id": req_id,
        "artist": release["artist_name"],
        "album": release["title"],
        "tracks": len(release.get("tracks", [])),
    })


def post_pipeline_update(h, body: dict) -> None:
    s = _server()
    req_id = body.get("id")
    new_status = body.get("status", "").strip()

    if not req_id or not new_status:
        h._error("Missing id or status")
        return
    if new_status not in ("wanted", "imported", "manual"):
        h._error(f"Invalid status: {new_status}")
        return

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
        transitions.finalize_request(
            s._db(),
            int(req_id),
            transitions.RequestTransition.to_wanted_fields(
                from_status=req["status"],
                fields=wanted_fields,
            ),
        )
    else:
        transitions.finalize_request(
            s._db(),
            int(req_id),
            transitions.RequestTransition.status_only(
                new_status,
                from_status=req["status"],
            ),
        )

    h._json({"status": "ok", "id": req_id, "new_status": new_status})


def post_pipeline_upgrade(h, body: dict) -> None:
    s = _server()
    mbid = normalize_release_id(body.get("mb_release_id"))
    if not mbid:
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
        transitions.finalize_request(
            s._db(),
            req_id,
            transitions.RequestTransition.to_wanted(
                from_status=existing["status"],
                search_filetype_override=quality,
                min_bitrate=min_bitrate,
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
            req_id = s._db().add_request(
                mb_release_id=mbid,
                mb_artist_id=release.get("artist_id"),
                artist_name=release["artist_name"],
                album_title=release["title"],
                year=release.get("year"),
                country=release.get("country"),
                source="request",
            )
        if release.get("tracks"):
            s._db().set_tracks(req_id, release["tracks"])
        # Newly added request — status is already 'wanted', set quality override
        transitions.finalize_request(
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


def post_pipeline_set_quality(h, body: dict) -> None:
    s = _server()
    mbid = normalize_release_id(body.get("mb_release_id"))
    new_status = body.get("status", "").strip()
    min_bitrate = body.get("min_bitrate")

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
            transitions.finalize_request(
                s._db(),
                req_id,
                transitions.RequestTransition.to_imported_fields(
                    from_status=existing["status"],
                    fields=imported_fields,
                ),
            )
        elif new_status == "wanted" and existing["status"] != "wanted":
            transitions.finalize_request(
                s._db(),
                req_id,
                transitions.RequestTransition.to_wanted(
                    from_status=existing["status"]),
            )
        else:
            transitions.finalize_request(
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


def post_pipeline_set_intent(h, body: dict) -> None:
    """Toggle lossless-on-disk intent for a pipeline request.

    Accepts intent: "lossless" (keep lossless on disk) or "default" (pipeline decides).
    Backward compat: "flac", "flac_only" → "lossless"; "best_effort" → "default".
    """
    s = _server()
    req_id = body.get("id")
    intent_str = body.get("intent", "").strip()

    if not req_id:
        h._error("Missing id")
        return

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
        transitions.finalize_request(
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


def post_pipeline_ban_source(h, body: dict) -> None:
    s = _server()
    req_id = body.get("request_id")
    username = body.get("username", "").strip()
    mb_release_id = normalize_release_id(body.get("mb_release_id"))

    if not req_id or not username:
        h._error("Missing request_id or username")
        return

    s._db().add_denylist(int(req_id), username, "manually banned via web UI")

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
    cleanup_errors: list[dict[str, str]] = []
    b = s._beets_db()
    if mb_release_id and b:
        cleanup = remove_and_reset_release(
            beets_db=b,
            pipeline_db=s._db(),
            release_id=mb_release_id,
            request_id=int(req_id),
        )
        beets_removed = cleanup.beets_removed
        # ``msgspec.to_builtins`` so future fields on ``SelectorFailure``
        # (e.g. a timestamp) propagate to the route response without
        # anyone having to remember to update the literal here (issue
        # #123 PR B review feedback; ``SelectorFailure`` is a
        # ``msgspec.Struct`` post-issue #141).
        cleanup_errors = [msgspec.to_builtins(f)
                          for f in cleanup.selector_failures]

    req = s._db().get_request(int(req_id))
    if req:
        quality = resolve_user_requeue_override(
            req.get("search_filetype_override"))
        min_br = req.get("min_bitrate")
        ban_fields: dict[str, object] = {
            "search_filetype_override": quality,
        }
        if min_br is not None:
            ban_fields["min_bitrate"] = min_br
        transitions.finalize_request(
            s._db(),
            int(req_id),
            transitions.RequestTransition.to_wanted_fields(
                from_status=req["status"],
                fields=ban_fields,
            ),
        )

    h._json({
        "status": "ok",
        "username": username,
        "beets_removed": beets_removed,
        "cleanup_errors": cleanup_errors,
    })


def post_pipeline_force_import(h, body: dict) -> None:
    from lib.import_dispatch import dispatch_import_from_db

    s = _server()
    log_id = body.get("download_log_id")

    if not log_id:
        h._error("Missing download_log_id")
        return

    entry = s._db().get_download_log_entry(int(log_id))
    if not entry:
        h._error(f"Download log entry {log_id} not found", 404)
        return

    request_id = entry["request_id"]

    vr_raw = entry.get("validation_result")
    if not vr_raw:
        h._error("No validation_result on this download log entry")
        return
    vr = vr_raw if isinstance(vr_raw, dict) else json.loads(vr_raw)
    failed_path = vr.get("failed_path")
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

    outcome = dispatch_import_from_db(
        s._db(), request_id=request_id, failed_path=resolved_path,
        force=True, outcome_label="force_import",
        source_username=entry.get("soulseek_username"),
    )

    h._json({
        "status": "ok" if outcome.success else "error",
        "request_id": request_id,
        "artist": req["artist_name"],
        "album": req["album_title"],
        "message": outcome.message,
    })


def post_pipeline_delete(h, body: dict) -> None:
    s = _server()
    req_id = body.get("id")
    if not req_id:
        h._error("Missing id")
        return
    req = s._db().get_request(int(req_id))
    if not req:
        h._error("Not found", 404)
        return
    s._db().delete_request(int(req_id))
    h._json({"status": "ok", "id": req_id})


# ── Route tables ─────────────────────────────────────────────────

GET_ROUTES: dict[str, object] = {
    "/api/pipeline/log": get_pipeline_log,
    "/api/pipeline/status": get_pipeline_status,
    "/api/pipeline/recent": get_pipeline_recent,
    "/api/pipeline/all": get_pipeline_all,
    "/api/pipeline/constants": get_pipeline_constants,
    "/api/pipeline/simulate": get_pipeline_simulate,
}

GET_PATTERNS: list[tuple[re.Pattern[str], object]] = [
    (re.compile(r"^/api/pipeline/(\d+)$"), get_pipeline_detail),
]

POST_ROUTES: dict[str, object] = {
    "/api/pipeline/add": post_pipeline_add,
    "/api/pipeline/update": post_pipeline_update,
    "/api/pipeline/upgrade": post_pipeline_upgrade,
    "/api/pipeline/set-quality": post_pipeline_set_quality,
    "/api/pipeline/set-intent": post_pipeline_set_intent,
    "/api/pipeline/ban-source": post_pipeline_ban_source,
    "/api/pipeline/force-import": post_pipeline_force_import,
    "/api/pipeline/delete": post_pipeline_delete,
}
