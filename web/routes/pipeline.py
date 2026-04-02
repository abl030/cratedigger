"""Pipeline API route handlers, extracted from server.py."""

import json
import os
import re
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))


def _server():
    """Deferred import to avoid circular deps."""
    from web import server  # type: ignore[import-not-found]
    return server


# ── GET handlers ─────────────────────────────────────────────────


def get_pipeline_log(h, params: dict[str, list[str]]) -> None:
    from classify import classify_log_entry, LogEntry
    outcome_filter = params.get("outcome", [None])[0]
    if outcome_filter not in (None, "imported", "rejected"):
        outcome_filter = None
    entries = _server()._db().get_log(limit=50, outcome_filter=outcome_filter)
    mbids = list(set(e["mb_release_id"] for e in entries if e.get("mb_release_id")))
    beets_info = _server().check_beets_library_detail(mbids) if mbids else {}
    result = []
    for e in entries:
        entry = LogEntry.from_row(e)
        classified = classify_log_entry(entry)
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
    serialized = []
    for r in recent:
        item = s._serialize_row(r)
        mbid = r.get("mb_release_id")
        pipeline_tracks = len(s._db().get_tracks(r["id"]))
        item["pipeline_tracks"] = pipeline_tracks
        if mbid and mbid in beets_info:
            item["in_beets"] = True
            bi = beets_info[mbid]
            item["beets_tracks"] = bi["beets_tracks"]
            for k in ("beets_format", "beets_bitrate", "beets_samplerate", "beets_bitdepth"):
                if bi.get(k):
                    item[k] = bi[k]
        else:
            fallback = s.check_beets_by_artist_album(
                r.get("artist_name", ""), r.get("album_title", "")
            )
            if fallback is not None:
                item["in_beets"] = True
                item["beets_tracks"] = fallback
            else:
                item["in_beets"] = False
                item["beets_tracks"] = 0
        history = s._db().get_download_history(r["id"])
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
    from classify import classify_log_entry, LogEntry
    s = _server()
    counts = s._db().count_by_status()
    all_data: dict[str, object] = {"counts": counts}
    for status in ("wanted", "imported", "manual"):
        items = []
        for r in s._db().get_by_status(status):
            item = s._serialize_row(r)
            history = s._db().get_download_history(r["id"])
            if history:
                last = history[0]
                entry = LogEntry.from_row(last)
                classified = classify_log_entry(entry)
                item["last_verdict"] = classified.verdict
                item["last_outcome"] = last.get("outcome")
                item["last_username"] = last.get("soulseek_username")
                item["download_count"] = len(history)
            items.append(item)
        all_data[status] = items
    h._json(all_data)


def get_pipeline_constants(h, params: dict[str, list[str]]) -> None:
    """Return decision tree structure + thresholds for the diagram."""
    from quality import get_decision_tree
    from spectral_check import (HF_DEFICIT_SUSPECT, HF_DEFICIT_MARGINAL,
                                ALBUM_SUSPECT_PCT, MIN_CLIFF_SLICES,
                                CLIFF_THRESHOLD_DB_PER_KHZ)
    tree = get_decision_tree()
    tree["constants"]["HF_DEFICIT_SUSPECT"] = HF_DEFICIT_SUSPECT
    tree["constants"]["HF_DEFICIT_MARGINAL"] = HF_DEFICIT_MARGINAL
    tree["constants"]["ALBUM_SUSPECT_PCT"] = ALBUM_SUSPECT_PCT
    tree["constants"]["MIN_CLIFF_SLICES"] = MIN_CLIFF_SLICES
    tree["constants"]["CLIFF_THRESHOLD_DB_PER_KHZ"] = CLIFF_THRESHOLD_DB_PER_KHZ
    h._json(tree)


def get_pipeline_simulate(h, params: dict[str, list[str]]) -> None:
    """Run full_pipeline_decision() with query-string inputs."""
    from quality import full_pipeline_decision

    def _str(key: str) -> str | None:
        v = params.get(key, [None])[0]
        return v if v else None

    def _int(key: str) -> int | None:
        v = _str(key)
        return int(v) if v else None

    def _bool(key: str) -> bool:
        v = _str(key)
        return v in ("true", "1", "yes") if v else False

    result = full_pipeline_decision(
        is_flac=_bool("is_flac"),
        min_bitrate=_int("min_bitrate") or 0,
        is_cbr=_bool("is_cbr"),
        spectral_grade=_str("spectral_grade"),
        spectral_bitrate=_int("spectral_bitrate"),
        existing_min_bitrate=_int("existing_min_bitrate"),
        existing_spectral_bitrate=_int("existing_spectral_bitrate"),
        override_min_bitrate=_int("override_min_bitrate"),
        post_conversion_min_bitrate=_int("post_conversion_min_bitrate"),
        converted_count=_int("converted_count") or 0,
        verified_lossless=_bool("verified_lossless"),
    )
    h._json(result)


def get_pipeline_detail(h, params: dict[str, list[str]], req_id_str: str) -> None:
    from classify import classify_log_entry, LogEntry
    s = _server()
    req_id = int(req_id_str)
    req = s._db().get_request(req_id)
    if not req:
        h._error("Not found", 404)
        return
    tracks = s._db().get_tracks(req_id)
    history = s._db().get_download_history(req_id)
    history_items = []
    for dl in history:
        he = LogEntry.from_row(dl)
        hi = he.to_json_dict()
        classified = classify_log_entry(he)
        hi["verdict"] = classified.verdict
        hi["downloaded_label"] = classified.downloaded_label
        history_items.append(hi)
    result: dict[str, object] = {
        "request": s._serialize_row(req),
        "tracks": tracks,
        "history": history_items,
    }
    mbid = req.get("mb_release_id")
    b = s._beets_db()
    if mbid and b:
        album = b._conn.execute(
            "SELECT id FROM albums WHERE mb_albumid = ?", (mbid,)
        ).fetchone()
        if album:
            items = b._conn.execute(
                "SELECT title, track, disc, length, format, "
                "       bitrate, samplerate, bitdepth "
                "FROM items WHERE album_id = ? ORDER BY disc, track",
                (album[0],),
            ).fetchall()
            result["beets_tracks"] = [{
                "title": i[0], "track": i[1], "disc": i[2],
                "length": i[3], "format": i[4], "bitrate": i[5],
                "samplerate": i[6], "bitdepth": i[7],
            } for i in items]
    h._json(result)


# ── POST handlers ────────────────────────────────────────────────


def post_pipeline_add(h, body: dict) -> None:
    s = _server()
    import mb as mb_api
    mbid = body.get("mb_release_id", "").strip()
    source = body.get("source", "request")

    if not mbid:
        h._error("Missing mb_release_id")
        return

    existing = s._db().get_request_by_mb_release_id(mbid)
    if existing:
        h._json({
            "status": "exists",
            "id": existing["id"],
            "current_status": existing["status"],
        })
        return

    release = mb_api.get_release(mbid)

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
                quality = "flac,mp3 v0,mp3 320"
                min_br = b.get_min_bitrate(mbid)
        s._db().reset_to_wanted(int(req_id), quality_override=quality, min_bitrate=min_br)
    else:
        s._db().update_status(int(req_id), new_status)

    h._json({"status": "ok", "id": req_id, "new_status": new_status})


def post_pipeline_upgrade(h, body: dict) -> None:
    s = _server()
    import mb as mb_api
    mbid = body.get("mb_release_id", "").strip()
    if not mbid:
        h._error("Missing mb_release_id")
        return

    quality = "flac,mp3 v0,mp3 320"

    min_bitrate = None
    b = s._beets_db()
    if b:
        min_bitrate = b.get_min_bitrate(mbid)

    existing = s._db().get_request_by_mb_release_id(mbid)
    if existing:
        req_id = existing["id"]
        s._db().reset_to_wanted(req_id,
                                quality_override=quality,
                                min_bitrate=min_bitrate)
        h._json({
            "status": "upgrade_queued",
            "id": req_id,
            "min_bitrate": min_bitrate,
            "quality_override": quality,
        })
    else:
        release = mb_api.get_release(mbid)
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
        s._db().reset_to_wanted(req_id,
                                quality_override=quality,
                                min_bitrate=min_bitrate)
        h._json({
            "status": "upgrade_queued",
            "id": req_id,
            "min_bitrate": min_bitrate,
            "quality_override": quality,
            "created": True,
        })


def post_pipeline_set_quality(h, body: dict) -> None:
    s = _server()
    mbid = body.get("mb_release_id", "").strip()
    new_status = body.get("status", "").strip()
    min_bitrate = body.get("min_bitrate")

    if not mbid:
        h._error("Missing mb_release_id")
        return

    existing = s._db().get_request_by_mb_release_id(mbid)
    if not existing:
        h._error("Not found in pipeline", 404)
        return

    req_id = existing["id"]

    if min_bitrate is not None:
        min_bitrate = int(min_bitrate)
        s._db()._execute(
            "UPDATE album_requests SET min_bitrate = %s WHERE id = %s",
            (min_bitrate, req_id),
        )

    if new_status:
        if new_status not in ("wanted", "imported", "manual"):
            h._error(f"Invalid status: {new_status}")
            return
        if new_status == "imported":
            if min_bitrate is None and mbid:
                b = s._beets_db()
                if b:
                    min_bitrate = b.get_avg_bitrate_kbps(mbid)
            sets = "status = 'imported', quality_override = NULL, updated_at = NOW()"
            if min_bitrate is not None:
                sets += f", min_bitrate = {int(min_bitrate)}"
            s._db()._execute(
                f"UPDATE album_requests SET {sets} WHERE id = %s",
                (req_id,),
            )
        elif new_status == "wanted" and existing["status"] != "wanted":
            s._db().reset_to_wanted(req_id)
        else:
            s._db().update_status(req_id, new_status)

    h._json({
        "status": "ok",
        "id": req_id,
        "new_status": new_status or existing["status"],
        "min_bitrate": min_bitrate,
    })


def post_pipeline_ban_source(h, body: dict) -> None:
    s = _server()
    req_id = body.get("request_id")
    username = body.get("username", "").strip()
    mb_release_id = body.get("mb_release_id", "").strip()

    if not req_id or not username:
        h._error("Missing request_id or username")
        return

    s._db().add_denylist(int(req_id), username, "manually banned via web UI")

    beets_removed = False
    b = s._beets_db()
    if mb_release_id and b:
        album_in_beets = b.album_exists(mb_release_id)
        if album_in_beets:
            import subprocess as _sp
            result = _sp.run(
                ["beet", "remove", "-d", f"mb_albumid:{mb_release_id}"],
                capture_output=True, text=True, timeout=30,
                env={**os.environ, "HOME": "/home/abl030"},
            )
            beets_removed = result.returncode == 0

    req = s._db().get_request(int(req_id))
    if req:
        quality = req.get("quality_override") or "flac,mp3 v0,mp3 320"
        min_br = req.get("min_bitrate")
        s._db().reset_to_wanted(int(req_id), quality_override=quality, min_bitrate=min_br)

    h._json({
        "status": "ok",
        "username": username,
        "beets_removed": beets_removed,
    })


def post_pipeline_force_import(h, body: dict) -> None:
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
    mbid = req.get("mb_release_id")
    if not mbid:
        h._error("Album has no MusicBrainz release ID")
        return

    if not os.path.isdir(failed_path):
        for base in ["/mnt/virtio/music/slskd"]:
            candidate = os.path.join(base, failed_path)
            if os.path.isdir(candidate):
                failed_path = candidate
                break
    if not os.path.isdir(failed_path):
        h._error(f"Files not found at: {failed_path}")
        return

    import_one_path = os.path.join(
        os.path.dirname(__file__), "..", "..", "harness", "import_one.py")
    cmd = [
        sys.executable, import_one_path,
        failed_path, mbid,
        "--request-id", str(request_id),
        "--force",
    ]
    if req.get("min_bitrate"):
        cmd.extend(["--override-min-bitrate", str(req["min_bitrate"])])

    import subprocess as _sp
    result = _sp.run(cmd, capture_output=True, text=True, timeout=1800)

    import_result_json = None
    for line in result.stdout.splitlines():
        if "__IMPORT_RESULT__" in line:
            try:
                import_result_json = line.split("__IMPORT_RESULT__")[1].strip()
            except (IndexError, json.JSONDecodeError):
                pass

    s._db().log_download(
        request_id=request_id,
        outcome="force_import",
        import_result=import_result_json,
        staged_path=failed_path,
    )

    if result.returncode == 0:
        update_fields: dict[str, object] = {}
        if import_result_json:
            try:
                update_fields = s._extract_import_fields(json.loads(import_result_json))
            except (json.JSONDecodeError, TypeError):
                pass
        s._db().update_status(request_id, "imported", **update_fields)

    h._json({
        "status": "ok" if result.returncode == 0 else "error",
        "exit_code": result.returncode,
        "request_id": request_id,
        "artist": req["artist_name"],
        "album": req["album_title"],
        "stderr": result.stderr[-500:] if result.stderr else "",
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
    "/api/pipeline/ban-source": post_pipeline_ban_source,
    "/api/pipeline/force-import": post_pipeline_force_import,
    "/api/pipeline/delete": post_pipeline_delete,
}
