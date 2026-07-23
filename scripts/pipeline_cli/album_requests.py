"""pipeline-cli request-lifecycle commands (#495 carve).

``list`` / ``add`` / ``status`` / ``set`` /
``set-intent`` / ``disk-coverage`` — the core CRUD-ish surface over
``album_requests``, plus the MusicBrainz-fetch helpers the ``add`` path
needs.
"""

from __future__ import annotations

import argparse
import json
import sys
import urllib.error
import urllib.request
from contextlib import AbstractContextManager
from typing import Protocol, TYPE_CHECKING
import msgspec

from lib import transitions
from lib.config import read_runtime_config
from lib.disk_coverage_service import DiskCoveragePipelineDB, disk_coverage
from lib.pipeline_db.rows import album_request_row
from lib.request_creation_service import (
    RequestCreationInput,
    RequestCreationService,
)
from lib.release_identity import detect_release_source, normalize_release_id

if TYPE_CHECKING:
    from lib.pipeline_db import (
        ActiveSearchPlan,
        SaturationSummary,
        SearchLogHistoryPage,
        SearchPlanInspection,
        SearchPlanItemInput,
    )
    from lib.pipeline_db.rows import AlbumRequestRow


class _DiskCoverageArgs(msgspec.Struct, frozen=True):
    beets_db: str | None = None
    beets_directory: str | None = None
    counts_only: bool = False
    include_inverse: bool = False


class _CursorRows(Protocol):
    """Narrow shape of the cursor ``PipelineDB._execute`` returns — just
    enough for the raw-SQL ``cmd_list`` fallback (issue #784, #409
    pattern)."""

    def fetchall(self) -> list[dict[str, object]]: ...


class _AlbumRequestsDB(
    transitions.TransitionsDB,
    DiskCoveragePipelineDB,
    Protocol,
):
    """Pipeline DB surface this module's ``cmd_*`` handlers touch, directly
    or via the collaborator services the ``add`` path constructs
    (``RequestCreationService`` / ``lib.disk_coverage_service.disk_coverage``)
    (issue #784, the #409
    narrow-protocol pattern). Inherits the two PUBLIC per-consumer
    protocols whose owning modules this file ALREADY imports eagerly
    (``transitions.TransitionsDB``, ``lib.disk_coverage_service.
    DiskCoveragePipelineDB``). ``lib.search_plan_service.SearchPlanDB``
    is NOT inherited even though it's also public — using it as a base
    class would force an eager top-level import of
    ``lib.search_plan_service``, which this module deliberately keeps
    lazy (imported inside the functions that need it, matching every
    other ``pipeline_cli`` command-family module), so its methods are
    mirrored below by signature instead. The field-resolver service's
    OWN narrow protocols (``_PdbRecorder`` / ``_ApplyResolveAllRecipient``
    in ``lib/field_resolver_service.py``) are private for the same
    reason AND leading-underscore, so their two methods are mirrored
    too, plus the handful of methods unique to this module's own
    ``cmd_*`` bodies (list/search/add/status/raw-SQL).

    ``add_request``'s MB/Discogs-JSON-sourced fields (``artist_name``,
    ``album_title``, ``mb_release_group_id``, ``mb_artist_id``,
    ``country``, ``year``) are narrowed to their real declared types
    via ``_str_or``/``_opt_str``/the inline ``isinstance``-guarded year
    parse below — graceful, non-crashing narrowing helpers, NOT a new
    ``assert isinstance(...)`` on external JSON (a prior sweep added
    exactly that here and a reviewer flagged it as borderline; issue
    #784 forbids repeating it). Each helper falls through to a safe
    default instead of raising on a malformed/differently-typed
    upstream value, so a live MB/Discogs payload (always the expected
    shape) round-trips unchanged and a hypothetical malformed one
    degrades instead of crashing the CLI.
    """

    def get_request_by_release_id(
        self, release_id: object | None,
    ) -> "AlbumRequestRow | None": ...

    # --- lib.search_plan_service.SearchPlanDB, mirrored (see class
    # docstring for why this isn't a base class) ---

    def advisory_lock(
        self, namespace: int, key: int,
    ) -> "AbstractContextManager[bool]": ...

    def get_active_search_plan(
        self, request_id: int,
    ) -> "ActiveSearchPlan | None": ...

    def create_successful_search_plan(
        self,
        *,
        request_id: int,
        generator_id: str,
        items: "list[SearchPlanItemInput]",
        metadata_snapshot: dict[str, object] | None = None,
        provenance: dict[str, object] | None = None,
        set_active: bool = True,
    ) -> int: ...

    def create_failed_search_plan(
        self,
        *,
        request_id: int,
        generator_id: str,
        failure_class: str,
        error_message: str | None = None,
        transient: bool,
        metadata_snapshot: dict[str, object] | None = None,
        provenance: dict[str, object] | None = None,
    ) -> int: ...

    def supersede_search_plan_with_replacement(
        self,
        *,
        request_id: int,
        generator_id: str,
        items: "list[SearchPlanItemInput]",
        metadata_snapshot: dict[str, object] | None = None,
        provenance: dict[str, object] | None = None,
    ) -> int: ...

    def advance_search_plan_cursor(
        self,
        request_id: int,
        *,
        target_ordinal: int,
        plan_item_count: int,
    ) -> tuple[int, int, int]: ...

    def get_saturation_summary(
        self, request_id: int, *, window_days: int = 14,
    ) -> "SaturationSummary": ...

    def get_search_history_page(
        self,
        request_id: int,
        *,
        limit: int,
        before_id: int | None = None,
    ) -> "SearchLogHistoryPage": ...

    def get_search_plan_inspection(
        self, request_id: int,
    ) -> "SearchPlanInspection": ...

    def search_requests(
        self, query: str, *, limit: int = 200, status: str | None = None,
    ) -> "list[AlbumRequestRow]": ...

    def get_by_status(
        self, status: str, *, limit: int | None = None,
        newest_first: bool = False,
    ) -> "list[AlbumRequestRow]": ...

    def count_by_status(self) -> dict[str | None, int]: ...

    def add_request(
        self,
        *,
        artist_name: str,
        album_title: str,
        source: str,
        mb_release_id: str | None = None,
        mb_release_group_id: str | None = None,
        mb_artist_id: str | None = None,
        discogs_release_id: str | None = None,
        year: int | None = None,
        country: str | None = None,
        format: str | None = None,
        source_path: str | None = None,
        reasoning: str | None = None,
        status: str = "wanted",
        release_group_year: int | None = None,
        is_va_compilation: bool = False,
    ) -> int: ...

    def update_track_artists(
        self,
        request_id: int,
        track_artists: list[str | None],
        *,
        expected_status: str | None = None,
    ) -> bool: ...

    def set_tracks(
        self, request_id: int, tracks: list[dict[str, object]],
    ) -> None: ...

    def get_tracks(self, request_id: int) -> list[dict[str, object]]: ...

    def update_request_fields(
        self,
        request_id: int,
        *,
        expected_status: str | None = None,
        **extra: object,
    ) -> bool: ...

    def record_field_resolution(
        self,
        request_id: int,
        field_name: str,
        status: str,
        reason_code: str | None,
    ) -> bool: ...

    def _execute(
        self, sql: str, params: tuple[object, ...] = (),
    ) -> _CursorRows: ...


def _json_dict(value: object) -> dict[str, object]:
    """Narrow an untyped nested MB/Discogs JSON value to a plain dict.

    ``msgspec.convert`` is the established wire-boundary adapter
    (CLAUDE.md "Wire-boundary types") — used here (rather than an
    ``isinstance`` assert) because narrowing a bare ``object`` to a
    generic container via ``isinstance`` loses the type argument
    (``dict[Unknown, Unknown]``), which still trips strict mode at
    every call site; ``msgspec.convert``'s own signature restores the
    declared type argument cleanly. No internal ``or {}`` fallback —
    callers that need one (matching the pre-existing ``X or {}`` guards
    in this file) supply it at the call site, so behaviour on an
    explicit JSON ``null`` is unchanged from before this migration.
    """
    return msgspec.convert(value, type=dict[str, object])


def _json_list_of_dicts(value: object) -> list[dict[str, object]]:
    """``_json_dict``'s twin for a JSON array of objects."""
    return msgspec.convert(value, type=list[dict[str, object]])


def _num_or_none(value: object) -> "int | float | None":
    """Narrow an external-JSON (MB) numeric field without asserting.

    Issue #784: a prior sweep asserted ``isinstance(x, (int, float))``
    on MB track-length fields and a reviewer flagged new external-JSON
    asserts as borderline. This degrades to ``None`` instead of
    crashing on a malformed/non-numeric payload — a real MB response
    always carries an int here, so this is a no-op for every live
    invocation.
    """
    return value if isinstance(value, (int, float)) else None


def _opt_str(value: object) -> str | None:
    """Narrow an optional external-JSON (MB/Discogs) string field.

    Same non-crashing-degrade rationale as ``_num_or_none``: an
    ``isinstance`` narrowing condition, never an ``assert`` (issue
    #784). MB/Discogs string fields (release-group id, artist id,
    country) are always ``str`` when present in a live payload, so
    this is a no-op for every real invocation.
    """
    return value if isinstance(value, str) else None


def _str_or(value: object, default: str) -> str:
    """``_opt_str``'s twin for a REQUIRED string field with a fallback.

    Used for ``artist_name``/``album_title`` — both feed
    request-creation service (a real ``str``-typed lib boundary), so a
    concrete ``str`` is unavoidable. Falls back to
    ``default`` instead of asserting on a malformed non-string payload.
    """
    return value if isinstance(value, str) else default


# Module-level DI seam for the operator transition service — see
# ``lib.dispatch.outcome_actions.finalize_request`` for the rationale.
# Each module that calls it binds its own copy (same pattern as
# ``web.routes.pipeline_mutations.finalize_request`` / ``harness.import_one.finalize_request``).
finalize_request = transitions.finalize_operator_request


def _transition_applied_or_report(
    result: transitions.TransitionResult,
) -> bool:
    """Print the CLI twin of the HTTP transition-conflict payload."""
    if not isinstance(result, transitions.TransitionConflict):
        return True
    print(json.dumps({
        "error": "transition_conflict",
        "reason": result.kind.value,
        "expected_status": result.expected_status,
        "actual_status": result.actual_status,
        "target_status": result.target_status,
    }))
    return False


def _request_fields_applied_or_report(
    db: _AlbumRequestsDB,
    request_id: int,
    *,
    expected_status: str,
    applied: bool,
) -> bool:
    """Map a metadata compare-and-set miss through the transition contract."""
    if applied:
        return True
    row = db.get_request(request_id)
    return _transition_applied_or_report(transitions.TransitionConflict(
        request_id=request_id,
        target_status=expected_status,
        kind=(
            transitions.TransitionConflictKind.not_found
            if row is None
            else transitions.TransitionConflictKind.stale_source
        ),
        expected_status=expected_status,
        actual_status=None if row is None else str(row["status"]),
    ))

VALID_STATUSES = ["wanted", "imported", "unsearchable"]


def _mb_api() -> str:
    """MusicBrainz WS/2 base — one config value, three consumers (KTD6).

    Reads [MusicBrainz] api_base from the runtime config (rendered by the
    NixOS module; public musicbrainz.org default) instead of a second
    hardcoded mirror URL drifting from web/mb.py's.
    """
    from lib.config import read_runtime_config
    from web.api_bases import mb_ws2_base
    return mb_ws2_base(read_runtime_config().musicbrainz_api_base)


def fetch_mb_release(mb_release_id: str) -> dict[str, object] | None:
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


def tracks_from_mb_release(
    release_data: dict[str, object],
) -> list[dict[str, object]]:
    """Extract track list from MB API release response.

    Includes pregap tracks (to match beets' default behaviour) but excludes
    data tracks (beets' ignore_data_tracks defaults to yes).
    """
    tracks: list[dict[str, object]] = []
    for medium in _json_list_of_dicts(release_data.get("media", [])):
        disc = medium.get("position", 1)
        # Include pregap track if present (beets always counts these)
        if "pregap" in medium:
            pg = _json_dict(medium["pregap"])
            recording = _json_dict(pg.get("recording") or {})
            length_ms = _num_or_none(pg.get("length")) or _num_or_none(
                recording.get("length"))
            tracks.append({
                "disc_number": disc,
                "track_number": 0,
                "title": pg.get("title", ""),
                "length_seconds": round(length_ms / 1000, 1) if length_ms else None,
            })
        for track in _json_list_of_dicts(medium.get("tracks", [])):
            recording = _json_dict(track.get("recording") or {})
            length_ms = _num_or_none(track.get("length")) or _num_or_none(
                recording.get("length"))
            tracks.append({
                "disc_number": disc,
                "track_number": track.get("position", track.get("number", 0)),
                "title": track.get("title", ""),
                "length_seconds": round(length_ms / 1000, 1) if length_ms else None,
            })
    return tracks


def cmd_list(db: _AlbumRequestsDB, args: argparse.Namespace) -> None:
    albums: list[AlbumRequestRow]
    if args.search:
        # Status narrowing happens in SQL — a Python post-filter after
        # the LIMIT would silently drop matches on common tokens.
        albums = db.search_requests(args.search, status=args.filter_status)
    elif args.filter_status:
        albums = db.get_by_status(args.filter_status)
    else:
        # Same unfiltered ``SELECT * FROM album_requests`` shape as
        # ``get_by_status``/``search_requests`` -- projected through the
        # same ``album_request_row`` adapter those methods use, rather
        # than a bare ``dict(r)``, so all three branches share one typed
        # element shape.
        rows = db._execute(
            "SELECT * FROM album_requests ORDER BY created_at ASC"
        ).fetchall()
        albums = [album_request_row(r) for r in rows]

    if not albums:
        print("No albums found.")
        return

    for a in albums:
        print(f"  [{a['id']:4d}] {a['status']:12s} {a['source']:10s} "
              f"{a['artist_name']} - {a['album_title']}  "
              f"({a['mb_release_id'] or a.get('discogs_release_id') or 'no-id'})")
    print(f"\n  Total: {len(albums)}")


def cmd_disk_coverage(db: _AlbumRequestsDB, args: object) -> None:
    from lib.beets_db import open_beets_db

    typed_args = msgspec.convert(vars(args), type=_DiskCoverageArgs)
    with open_beets_db(
        db_path=typed_args.beets_db,
        library_root=typed_args.beets_directory,
    ) as beets:
        result = disk_coverage(
            db,
            beets,
            include_rows=not typed_args.counts_only,
            include_inverse=typed_args.include_inverse,
        )
    print(json.dumps(msgspec.to_builtins(result), indent=2, sort_keys=True))


def cmd_add(db: _AlbumRequestsDB, args: argparse.Namespace) -> int | None:
    release_id = normalize_release_id(args.mbid)
    source = args.source
    id_source = detect_release_source(release_id)

    if id_source == "discogs":
        return _cmd_add_discogs(db, release_id, source)
    return _cmd_add_mb(db, release_id, source)


def _cmd_add_mb(db: _AlbumRequestsDB, mbid: str, source: str) -> int | None:
    """Add a MusicBrainz release to the pipeline."""
    existing = db.get_request_by_release_id(mbid)
    if existing and existing["status"] != "initializing":
        print(f"  Already in DB: id={existing['id']} status={existing['status']}")
        return None

    print(f"  Fetching MB release {mbid}...")
    release = fetch_mb_release(mbid)
    if not release:
        print("  Failed to fetch release from MB API.")
        return None

    # An absent/empty ``artist-credit`` list is handled identically below
    # (every access is guarded by ``if artist_credit:``), so a bare empty
    # default preserves the original ``[{}]`` fallback's observable
    # behaviour without an unparameterized-dict-literal default arg.
    artist_credit = _json_list_of_dicts(release.get("artist-credit", []))
    first_credit = artist_credit[0] if artist_credit else {}
    raw_artist_name = first_credit.get("name", "Unknown") if artist_credit else "Unknown"
    artist_name = _str_or(raw_artist_name, "Unknown")
    artist_id: str | None = None
    if artist_credit:
        credited_artist = _json_dict(first_credit.get("artist", {}))
        artist_id = _opt_str(credited_artist.get("id"))
    release_group = _json_dict(release.get("release-group") or {})
    rg_id = _opt_str(release_group.get("id"))
    year: int | None = None
    release_date = release.get("date")
    if isinstance(release_date, str) and len(release_date) >= 4:
        year = int(release_date[:4])
    title = _str_or(release.get("title", "Unknown"), "Unknown")
    country = _opt_str(release.get("country"))

    tracks = tracks_from_mb_release(release)
    result = RequestCreationService(db, read_runtime_config()).create_or_resume(
        RequestCreationInput(
            release_id=mbid,
            mb_release_id=mbid,
            mb_release_group_id=rg_id,
            mb_artist_id=artist_id,
            artist_name=artist_name,
            album_title=title,
            year=year,
            country=country,
            source=source,
            tracks=tracks,
            mb_release_payload=release,
        ),
    )
    if result.outcome == "busy":
        print("  Add busy; retry shortly.", file=sys.stderr)
        return 4
    if result.outcome == "initialization_failed":
        print(f"  Initialization failed for id={result.request_id}: {result.detail}", file=sys.stderr)
        return 4
    print(f"  {result.outcome.title()}: id={result.request_id} {artist_name} - {title} ({len(tracks)} tracks)")
    return None


def _cmd_add_discogs(
    db: _AlbumRequestsDB, discogs_id: str, source: str,
) -> int | None:
    """Add a Discogs release to the pipeline."""
    existing = db.get_request_by_release_id(discogs_id)
    if existing and existing["status"] != "initializing":
        print(f"  Already in DB: id={existing['id']} status={existing['status']}")
        return None

    print(f"  Fetching Discogs release {discogs_id}...")
    try:
        from web import discogs as discogs_api
        release = discogs_api.get_release(int(discogs_id))
    except Exception as e:
        print(f"  Failed to fetch release from Discogs API: {e}")
        return None

    # ``str(x or "")`` is the pre-existing coercion for this field (never
    # raises, unlike an ``isinstance`` assert) -- kept verbatim.
    artist_id = str(release.get("artist_id") or "")
    artist_name = _str_or(release["artist_name"], "Unknown")
    title = _str_or(release["title"], "Unknown")
    year_raw = release.get("year")
    year = year_raw if isinstance(year_raw, int) else None
    country = _opt_str(release.get("country"))

    tracks_raw = release.get("tracks", [])
    tracks: list[dict[str, object]] = (
        msgspec.convert(tracks_raw, type=list[dict[str, object]])
        if isinstance(tracks_raw, list) else []
    )
    result = RequestCreationService(db, read_runtime_config()).create_or_resume(
        RequestCreationInput(
            release_id=discogs_id,
            mb_release_id=discogs_id,
            discogs_release_id=discogs_id,
            mb_artist_id=artist_id or None,
            artist_name=artist_name,
            album_title=title,
            year=year,
            country=country,
            source=source,
            tracks=tracks,
            discogs_release_payload=release,
        ),
    )
    if result.outcome == "busy":
        print("  Add busy; retry shortly.", file=sys.stderr)
        return 4
    if result.outcome == "initialization_failed":
        print(f"  Initialization failed for id={result.request_id}: {result.detail}", file=sys.stderr)
        return 4
    print(f"  {result.outcome.title()}: id={result.request_id} {artist_name} - {title} ({len(tracks)} tracks)")
    return None


def cmd_status(db: _AlbumRequestsDB, args: argparse.Namespace) -> None:
    counts = db.count_by_status()
    if not counts:
        print("  Database is empty.")
        return
    total = sum(counts.values())
    print(f"  Pipeline DB status ({total} total):\n")
    for status in ["initializing", "wanted", "downloading", "imported", "unsearchable"]:
        c = counts.get(status, 0)
        if c > 0:
            print(f"    {status:15s} {c:4d}")


def cmd_set(db: _AlbumRequestsDB, args: argparse.Namespace) -> int:
    req = db.get_request(args.id)
    if not req:
        print(f"  Request {args.id} not found.")
        return 2
    old_status = req["status"]
    if old_status == "initializing":
        print("  Request is still initializing; retry the original add or upgrade.")
        return 4
    result = finalize_request(
        db,
        args.id,
        transitions.RequestTransition.status_only(
            args.status,
            from_status=old_status,
        ),
    )
    if not _transition_applied_or_report(result):
        return 4
    print(f"  [{args.id}] {req['artist_name']} - {req['album_title']}: {old_status} → {args.status}")
    return 0


def cmd_set_intent(db: _AlbumRequestsDB, args: argparse.Namespace) -> int:
    """Toggle lossless-on-disk intent for a request.

    'lossless' — keep lossless on disk (overrides global verified_lossless_target)
    'default'  — pipeline decides (uses global verified_lossless_target)
    """
    from lib.quality import QUALITY_LOSSLESS, should_clear_lossless_search_override

    target_format = QUALITY_LOSSLESS if args.intent == "lossless" else None

    req = db.get_request(args.id)
    if not req:
        print(f"  Request {args.id} not found.")
        return 2
    if req["status"] == "initializing":
        print("  Request is still initializing; retry the original add or upgrade.")
        return 4
    if req["status"] == "downloading":
        print(f"  Cannot set intent while album is downloading.")
        return 1
    if req["status"] == "replaced":
        result = finalize_request(
            db,
            args.id,
            transitions.RequestTransition.to_wanted(from_status="replaced"),
        )
        _transition_applied_or_report(result)
        return 4
    old_target = req.get("target_format")
    label = f"{req['artist_name']} - {req['album_title']}"

    if req["status"] == "imported" and target_format:
        # Re-queue to search for lossless source
        min_br = req.get("min_bitrate")
        result = finalize_request(
            db,
            args.id,
            transitions.RequestTransition.to_wanted(
                from_status="imported",
                search_filetype_override=QUALITY_LOSSLESS,
                min_bitrate=min_br,
            ),
        )
        if not _transition_applied_or_report(result):
            return 4
        applied = db.update_request_fields(
            args.id,
            expected_status="wanted",
            target_format=target_format,
        )
        if not _request_fields_applied_or_report(
            db,
            args.id,
            expected_status="wanted",
            applied=applied,
        ):
            return 4
        print(f"  [{args.id}] {label}: lossless on disk, re-queued for search")
    else:
        update_fields: dict[str, object] = {"target_format": target_format}
        if should_clear_lossless_search_override(
            new_target_format=target_format,
            old_target_format=old_target,
            search_filetype_override=req.get("search_filetype_override"),
        ):
            update_fields["search_filetype_override"] = None
        applied = db.update_request_fields(
            args.id,
            expected_status=str(req["status"]),
            **update_fields,
        )
        if not _request_fields_applied_or_report(
            db,
            args.id,
            expected_status=str(req["status"]),
            applied=applied,
        ):
            return 4
        action = "lossless on disk" if target_format else "default (pipeline decides)"
        print(f"  [{args.id}] {label}: {action} "
              f"(target_format: {old_target} → {target_format})")
    return 0


def add_album_requests_subparsers(
    sub: argparse._SubParsersAction[argparse.ArgumentParser],
) -> None:
    """Add ``list`` / ``add`` / ``status`` / ``disk-coverage`` / ``set`` /
    ``set-intent`` (#521 carve out of
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
        default=None,
        help="Explicit Beets SQLite override; requires --beets-directory.",
    )
    p_disk.add_argument(
        "--beets-directory",
        default=None,
        help="Library root paired with --beets-db.",
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

    # set
    p_set = sub.add_parser("set", help="Change the status of a request")
    p_set.add_argument("id", type=int, help="Request ID")
    p_set.add_argument("status", choices=VALID_STATUSES, help="New status")

    # set-intent
    p_intent = sub.add_parser("set-intent", help="Toggle lossless-on-disk for a request")
    p_intent.add_argument("id", type=int, help="Request ID")
    p_intent.add_argument("intent", choices=["lossless", "default"],
                          help="'lossless' = keep lossless on disk, 'default' = pipeline decides")
