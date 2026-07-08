"""pipeline-cli request-lifecycle commands (#495 carve).

``list`` / ``add`` / ``status`` / ``retry`` / ``cancel`` / ``set`` /
``set-intent`` / ``disk-coverage`` — the core CRUD-ish surface over
``album_requests``, plus the MusicBrainz-fetch helpers the ``add`` path
needs.
"""

import argparse
import json
import os
import sys
import urllib.error
import urllib.request

import msgspec

from lib import transitions
from lib.disk_coverage_service import disk_coverage
from lib.release_identity import detect_release_source, normalize_release_id

# Module-level DI seam for ``transitions.finalize_request`` — see
# ``lib.dispatch.outcome_actions.finalize_request`` for the rationale.
# Each module that calls it binds its own copy (same pattern as
# ``web.routes.pipeline_mutations.finalize_request`` / ``harness.import_one.finalize_request``).
finalize_request = transitions.finalize_request

VALID_STATUSES = ["wanted", "imported", "manual"]


def _mb_api() -> str:
    """MusicBrainz WS/2 base — one config value, three consumers (KTD6).

    Reads [MusicBrainz] api_base from the runtime config (rendered by the
    NixOS module; public musicbrainz.org default) instead of a second
    hardcoded mirror URL drifting from web/mb.py's.
    """
    from lib.config import read_runtime_config
    from web.api_bases import mb_ws2_base
    return mb_ws2_base(read_runtime_config().musicbrainz_api_base)


def fetch_mb_release(mb_release_id):
    """Fetch release metadata + tracks from MusicBrainz API.

    Returns raw MB JSON. The ``media+release-groups+labels`` inc params
    are required so the field resolver service can extract
    ``label-info`` (catalog_number), per-track ``artist-credit``
    (track_artist), and the nested ``release-group`` primary-type/
    secondary-types (VA Rule 2). Without them, every MB request adds
    its rows to the side table as ``unresolved_field_missing_upstream``
    even though MB has the data.
    """
    url = (
        f"{_mb_api()}/release/{mb_release_id}"
        f"?inc=recordings+artist-credits+media+release-groups+labels&fmt=json"
    )
    req = urllib.request.Request(url)
    req.add_header("User-Agent", "pipeline-cli/1.0")
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            return json.loads(resp.read())
    except (urllib.error.HTTPError, urllib.error.URLError) as e:
        print(f"  [ERROR] MB API: {e}", file=sys.stderr)
        return None


def tracks_from_mb_release(release_data):
    """Extract track list from MB API release response.

    Includes pregap tracks (to match beets' default behaviour) but excludes
    data tracks (beets' ignore_data_tracks defaults to yes).
    """
    tracks = []
    for medium in release_data.get("media", []):
        disc = medium.get("position", 1)
        # Include pregap track if present (beets always counts these)
        if "pregap" in medium:
            pg = medium["pregap"]
            length_ms = pg.get("length") or (pg.get("recording") or {}).get("length")
            tracks.append({
                "disc_number": disc,
                "track_number": 0,
                "title": pg.get("title", ""),
                "length_seconds": round(length_ms / 1000, 1) if length_ms else None,
            })
        for track in medium.get("tracks", []):
            length_ms = track.get("length") or (track.get("recording") or {}).get("length")
            tracks.append({
                "disc_number": disc,
                "track_number": track.get("position", track.get("number", 0)),
                "title": track.get("title", ""),
                "length_seconds": round(length_ms / 1000, 1) if length_ms else None,
            })
    return tracks


def cmd_list(db, args):
    if args.search:
        # Status narrowing happens in SQL — a Python post-filter after
        # the LIMIT would silently drop matches on common tokens.
        albums = db.search_requests(args.search, status=args.filter_status)
    elif args.filter_status:
        albums = db.get_by_status(args.filter_status)
    else:
        rows = db._execute("SELECT * FROM album_requests ORDER BY created_at ASC").fetchall()
        albums = [dict(r) for r in rows]

    if not albums:
        print("No albums found.")
        return

    for a in albums:
        print(f"  [{a['id']:4d}] {a['status']:12s} {a['source']:10s} "
              f"{a['artist_name']} - {a['album_title']}  "
              f"({a['mb_release_id'] or a.get('discogs_release_id') or 'no-id'})")
    print(f"\n  Total: {len(albums)}")


def cmd_disk_coverage(db, args):
    from lib.beets_db import BeetsDB

    with BeetsDB(args.beets_db) as beets:
        result = disk_coverage(
            db,
            beets,
            include_rows=not args.counts_only,
            include_inverse=args.include_inverse,
        )
    print(json.dumps(msgspec.to_builtins(result), indent=2, sort_keys=True))


def cmd_add(db, args):
    release_id = normalize_release_id(args.mbid)
    source = args.source
    id_source = detect_release_source(release_id)

    if id_source == "discogs":
        return _cmd_add_discogs(db, release_id, source)
    return _cmd_add_mb(db, release_id, source)


def _build_search_plan_service(db):
    """Construct a `SearchPlanService` against the runtime config.

    CLI / web / startup all share the same source so generator-id and
    `SearchPlanConfig` cannot drift between paths.
    """
    from lib.config import read_runtime_config
    from lib.search_plan_service import SearchPlanService
    return SearchPlanService(db, read_runtime_config())


def _generate_plan_after_add(db, req_id, *, artist_name, album_title, year,
                              tracks, source, release_group_year=None,
                              is_va_compilation=False,
                              catalog_number=None):
    """Run plan generation for a freshly-added request.

    Failures are non-fatal: a deterministic / transient failure is
    recorded as a `search_plans` row and the CLI prints a one-liner so
    the operator knows the request is wanted-but-not-searchable until
    repaired.

    ``release_group_year`` (U5 of search-plan-entropy) feeds the
    generator's conditional ``unwild_rg_year`` slot. ``None`` is fine
    — the generator handles it gracefully.

    PR2 Apply #2: ``is_va_compilation`` and ``catalog_number`` are
    forwarded so the initial plan respects the resolver's verdict —
    mirrors the web add helper for CLI ⇄ API symmetry.
    """
    from lib.search_plan_service import (
        RESULT_FAILED_DETERMINISTIC,
        RESULT_FAILED_TRANSIENT,
        RESULT_SUCCESS,
    )

    svc = _build_search_plan_service(db)
    result = svc.generate_for_new_request(
        req_id,
        artist_name=artist_name,
        album_title=album_title,
        year=year,
        tracks=tracks,
        source=source,
        release_group_year=release_group_year,
        is_va_compilation=is_va_compilation,
        catalog_number=catalog_number,
    )
    if result.outcome == RESULT_SUCCESS:
        print(f"  Plan: active id={result.plan_id}")
    elif result.outcome == RESULT_FAILED_DETERMINISTIC:
        print(f"  Plan: FAILED ({result.failure_class}); request not searchable")
    elif result.outcome == RESULT_FAILED_TRANSIENT:
        print(f"  Plan: TRANSIENT FAIL ({result.failure_class}); will retry")


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
    """U4 helper for the CLI add path — mirrors the web helper.

    Both surfaces wrap ``field_resolver_service.resolve_all`` after the
    new request row is inserted, so the CLI and HTTP add stay symmetric
    (CLAUDE.md § "CLI ⇄ API surface symmetry"). The CLI prints a one-
    liner on resolver outcomes so the operator running the script
    knows whether a field landed NULL.
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
        print(f"  Field resolution crashed: {exc}", file=sys.stderr)
        return ResolveAllResult()

    try:
        apply_resolve_all_result(
            db, req_id, result,
            existing_mb_release_group_id=mb_release_group_id,
        )
    except Exception as exc:  # noqa: BLE001
        print(
            f"  Failed to persist resolved fields: {exc}",
            file=sys.stderr,
        )

    rg_year_str = (
        str(result.release_group_year)
        if result.release_group_year is not None else "NULL"
    )
    va_str = "yes" if result.is_va_compilation else "no"
    timed_out = (
        f" timed_out={','.join(result.timed_out_fields)}"
        if result.timed_out_fields else ""
    )
    print(
        f"  Resolved: rg_year={rg_year_str} is_va={va_str}{timed_out}"
    )
    return result


def _cmd_add_mb(db, mbid, source):
    """Add a MusicBrainz release to the pipeline."""
    existing = db.get_request_by_release_id(mbid)
    if existing:
        print(f"  Already in DB: id={existing['id']} status={existing['status']}")
        return

    print(f"  Fetching MB release {mbid}...")
    release = fetch_mb_release(mbid)
    if not release:
        print("  Failed to fetch release from MB API.")
        return

    artist_credit = release.get("artist-credit", [{}])
    artist_name = artist_credit[0].get("name", "Unknown") if artist_credit else "Unknown"
    artist_id = (artist_credit[0].get("artist", {}).get("id")
                 if artist_credit else None)
    rg_id = (release.get("release-group") or {}).get("id")
    year = None
    if release.get("date"):
        year = int(release["date"][:4]) if len(release["date"]) >= 4 else None

    req_id = db.add_request(
        mb_release_id=mbid,
        mb_release_group_id=rg_id,
        mb_artist_id=artist_id,
        artist_name=artist_name,
        album_title=release.get("title", "Unknown"),
        year=year,
        country=release.get("country"),
        source=source,
    )

    tracks = tracks_from_mb_release(release)
    if tracks:
        db.set_tracks(req_id, tracks)

    print(f"  Added: id={req_id} {artist_name} - {release.get('title')} ({len(tracks)} tracks)")
    # U4: inline field resolution + VA detection. Single resolver-service
    # invocation shared with the web add path (CLI ⇄ API symmetry).
    resolved = _resolve_and_update_after_add(
        db, req_id,
        mb_release_id=mbid,
        discogs_release_id=None,
        mb_release_group_id=rg_id,
        mb_artist_id=artist_id,
        mb_release_payload=release,
    )
    # Re-read tracks from the DB so the per-track ``track_artist``
    # column the resolver just wrote (PR2 Apply #1) flows into the
    # snapshot. The upstream ``tracks`` extracted from the MB payload
    # does NOT carry the resolver's output.
    post_resolve_tracks = db.get_tracks(req_id)
    _generate_plan_after_add(
        db, req_id,
        artist_name=artist_name,
        album_title=release.get("title", "Unknown"),
        year=year,
        tracks=post_resolve_tracks,
        source=source,
        release_group_year=resolved.release_group_year,
        is_va_compilation=resolved.is_va_compilation,
        catalog_number=resolved.catalog_number,
    )


def _cmd_add_discogs(db, discogs_id, source):
    """Add a Discogs release to the pipeline."""
    existing = db.get_request_by_release_id(discogs_id)
    if existing:
        print(f"  Already in DB: id={existing['id']} status={existing['status']}")
        return

    print(f"  Fetching Discogs release {discogs_id}...")
    try:
        from web import discogs as discogs_api
        release = discogs_api.get_release(int(discogs_id))
    except Exception as e:
        print(f"  Failed to fetch release from Discogs API: {e}")
        return

    req_id = db.add_request(
        mb_release_id=discogs_id,
        discogs_release_id=discogs_id,
        mb_artist_id=str(release.get("artist_id") or ""),
        artist_name=release["artist_name"],
        album_title=release["title"],
        year=release.get("year"),
        country=release.get("country"),
        source=source,
    )

    tracks = release.get("tracks", [])
    if tracks:
        db.set_tracks(req_id, tracks)

    print(f"  Added: id={req_id} {release['artist_name']} - {release['title']} ({len(tracks)} tracks)")
    # U4: inline field resolution + VA detection. Discogs add path has
    # no MB release/release-group payload available so the resolver only
    # sees the discogs release (Rule 1 of VA detection still fires on
    # canonical-ID match; rules 2 + 3 are MB-only).
    resolved = _resolve_and_update_after_add(
        db, req_id,
        mb_release_id=None,
        discogs_release_id=discogs_id,
        mb_release_group_id=None,
        mb_artist_id=str(release.get("artist_id") or "") or None,
        discogs_release_payload=release,
    )
    # Re-read tracks from the DB so the per-track ``track_artist``
    # column the resolver just wrote (PR2 Apply #1) flows into the
    # snapshot.
    post_resolve_tracks = db.get_tracks(req_id)
    _generate_plan_after_add(
        db, req_id,
        artist_name=release["artist_name"],
        album_title=release["title"],
        year=release.get("year"),
        tracks=post_resolve_tracks,
        source=source,
        release_group_year=resolved.release_group_year,
        is_va_compilation=resolved.is_va_compilation,
        catalog_number=resolved.catalog_number,
    )


def cmd_status(db, args):
    counts = db.count_by_status()
    if not counts:
        print("  Database is empty.")
        return
    total = sum(counts.values())
    print(f"  Pipeline DB status ({total} total):\n")
    for status in ["wanted", "downloading", "imported", "manual"]:
        c = counts.get(status, 0)
        if c > 0:
            print(f"    {status:15s} {c:4d}")


def cmd_retry(db, args):
    req = db.get_request(args.id)
    if not req:
        print(f"  Request {args.id} not found.")
        return
    finalize_request(
        db,
        args.id,
        transitions.RequestTransition.to_wanted(from_status=req["status"]),
    )
    print(f"  Reset to wanted: [{args.id}] {req['artist_name']} - {req['album_title']}")


def cmd_cancel(db, args):
    req = db.get_request(args.id)
    if not req:
        print(f"  Request {args.id} not found.")
        return
    finalize_request(
        db,
        args.id,
        transitions.RequestTransition.to_manual(from_status=req["status"]),
    )
    print(f"  Marked for manual download: [{args.id}] {req['artist_name']} - {req['album_title']}")


def cmd_set(db, args):
    req = db.get_request(args.id)
    if not req:
        print(f"  Request {args.id} not found.")
        return
    old_status = req["status"]
    if old_status == args.status:
        print(f"  [{args.id}] already has status '{args.status}'.")
        return
    finalize_request(
        db,
        args.id,
        transitions.RequestTransition.status_only(
            args.status,
            from_status=old_status,
        ),
    )
    print(f"  [{args.id}] {req['artist_name']} - {req['album_title']}: {old_status} → {args.status}")


def cmd_set_intent(db, args):
    """Toggle lossless-on-disk intent for a request.

    'lossless' — keep lossless on disk (overrides global verified_lossless_target)
    'default'  — pipeline decides (uses global verified_lossless_target)
    """
    from lib.quality import QUALITY_LOSSLESS, should_clear_lossless_search_override

    target_format = QUALITY_LOSSLESS if args.intent == "lossless" else None

    req = db.get_request(args.id)
    if not req:
        print(f"  Request {args.id} not found.")
        return
    if req["status"] == "downloading":
        print(f"  Cannot set intent while album is downloading.")
        return
    old_target = req.get("target_format")
    label = f"{req['artist_name']} - {req['album_title']}"

    if req["status"] == "imported" and target_format:
        # Re-queue to search for lossless source
        min_br = req.get("min_bitrate")
        finalize_request(
            db,
            args.id,
            transitions.RequestTransition.to_wanted(
                from_status="imported",
                search_filetype_override=QUALITY_LOSSLESS,
                min_bitrate=min_br,
            ),
        )
        db.update_request_fields(args.id, target_format=target_format)
        print(f"  [{args.id}] {label}: lossless on disk, re-queued for search")
    else:
        update_fields = {"target_format": target_format}
        if should_clear_lossless_search_override(
            new_target_format=target_format,
            old_target_format=old_target,
            search_filetype_override=req.get("search_filetype_override"),
        ):
            update_fields["search_filetype_override"] = None
        db.update_request_fields(args.id, **update_fields)
        action = "lossless on disk" if target_format else "default (pipeline decides)"
        print(f"  [{args.id}] {label}: {action} "
              f"(target_format: {old_target} → {target_format})")


def add_album_requests_subparsers(sub: argparse._SubParsersAction) -> None:
    """Add ``list`` / ``add`` / ``status`` / ``disk-coverage`` / ``retry`` /
    ``cancel`` / ``set`` / ``set-intent`` (#521 carve out of
    ``routes_meta._build_parser``, verbatim argument definitions)."""
    # list
    p_list = sub.add_parser("list", help="List album requests")
    p_list.add_argument("filter_status", nargs="?", help="Filter by status")
    p_list.add_argument(
        "--search",
        help="Case-insensitive substring match on artist or album "
             "(mirrors GET /api/pipeline/search)",
    )

    # add
    p_add = sub.add_parser("add", help="Add a new request by MBID or Discogs ID")
    p_add.add_argument("mbid", help="MusicBrainz release UUID or Discogs numeric release ID")
    p_add.add_argument("--source", default="request", choices=["request", "redownload", "manual"],
                       help="Source type (default: request)")

    # status
    sub.add_parser("status", help="Show counts by status")

    # disk-coverage
    p_disk = sub.add_parser(
        "disk-coverage",
        help="Show which active pipeline rows are actually present in beets",
    )
    p_disk.add_argument(
        "--beets-db",
        default=os.environ.get("BEETS_DB", "/mnt/virtio/Music/beets-library.db"),
        help="Path to beets SQLite DB (default: BEETS_DB or production path)",
    )
    p_disk.add_argument(
        "--counts-only",
        action="store_true",
        help="Suppress the off-disk row list and print counts only",
    )
    p_disk.add_argument(
        "--include-inverse",
        action="store_true",
        help="Also include beets albums with no active pipeline row",
    )

    # retry
    p_retry = sub.add_parser("retry", help="Reset a failed request to wanted")
    p_retry.add_argument("id", type=int, help="Request ID")

    # cancel
    p_cancel = sub.add_parser("cancel", help="Cancel a request (set to skipped)")
    p_cancel.add_argument("id", type=int, help="Request ID")

    # set
    p_set = sub.add_parser("set", help="Change the status of a request")
    p_set.add_argument("id", type=int, help="Request ID")
    p_set.add_argument("status", choices=VALID_STATUSES, help="New status")

    # set-intent
    p_intent = sub.add_parser("set-intent", help="Toggle lossless-on-disk for a request")
    p_intent.add_argument("id", type=int, help="Request ID")
    p_intent.add_argument("intent", choices=["lossless", "default"],
                          help="'lossless' = keep lossless on disk, 'default' = pipeline decides")
