"""Read-projection parity registry (issue #546 W1).

The write side has ``.claude/rules/test-fidelity.md`` Rule A: every
``PipelineDB`` write method carries a real-PG round-trip test. The READ
side has the mirror problem — ``FakePipelineDB`` (``tests/fakes/pipeline_db.py``)
hand-mirrors production ``SELECT`` projections across ~62 ``get_*`` /
``list_*`` methods. When the fake's projection drifts from production's
(a column the fake returns that production doesn't, or vice-versa),
fake-driven contract tests stay green while the live route 500s or
renders nulls. #523 found a 2/6 drift rate.

This module is the DATA half of the self-enforcing read-parity audit.
It has NO test cases and needs NO PostgreSQL — it is a pure
data/introspection module imported by both:

* the registry-driven parity DRIVER
  (``tests/test_pipeline_db.py::TestReadProjectionRegistryParity``),
  which seeds identical state through a real ``PipelineDB`` and a
  ``FakePipelineDB``, runs each seeder, and asserts key-set parity; and
* the completeness AUDIT
  (``tests/test_read_projection_audit.py``), which asserts every read
  mirror is covered by exactly one of: a registry seeder, an existing
  hand-written parity test, or the allowlist.

A ``Seeder`` is deliberately backend-agnostic: it takes a db that is
EITHER a real ``PipelineDB`` OR a ``FakePipelineDB`` (they share the
same duck-typed surface), seeds identical deterministic state, calls
exactly ONE read-projection method, and returns the projected rows
flattened to ``list[dict]``. Only KEYS are compared downstream (ids and
timestamps are backend-assigned/time-anchored), so seeders must never
put timestamps or random values in row KEYS. Every seeder must produce
>= 1 row on BOTH backends — a vacuous parity check is worthless.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Callable

from lib.pipeline_db import PersistedDistance, PersistedTrack, PersistedYoutubeRow


# A seeder takes a db (real ``PipelineDB`` or ``FakePipelineDB``), seeds
# identical state, calls ONE read method, and returns the projected rows
# flattened to ``list[dict]``.
Seeder = Callable[[Any], "list[dict[str, Any]]"]


# Read verbs whose methods carry a row projection that can drift from
# production. ``search_``/``find_``/``fetch_`` are here because they run
# raw SELECTs too (e.g. ``search_requests`` = ``SELECT *`` behind a LIVE
# route, ``find_youtube_album_mapping_for_release`` = a hand-listed SELECT)
# — restricting the universe to ``get_``/``list_`` let those escape the
# audit (the #546 W1 reviewers' F1 finding).
_READ_METHOD_PREFIXES: "tuple[str, ...]" = (
    "get_", "list_", "search_", "find_", "fetch_",
)


def enumerate_read_mirrors() -> "list[str]":
    """Introspect ``FakePipelineDB`` for its public read-projection methods.

    The authoritative universe is every public method whose name starts
    with one of ``_READ_METHOD_PREFIXES``. ``FakePipelineDB`` is the
    mirror, so its read surface IS the set of projections that can drift
    from production.

    Scalar-returning read verbs (``count_*`` → int, ``has_*`` / ``exists_*``
    → bool) are INTENTIONALLY excluded: they return a bool/int/scalar-map
    with no row projection, so the SELECT-column-drift class this audit
    guards against does not apply to them.
    """
    from tests.fakes import FakePipelineDB

    return sorted(
        name
        for name in dir(FakePipelineDB)
        if any(name.startswith(p) for p in _READ_METHOD_PREFIXES)
        and not name.startswith("_")
    )


# --------------------------------------------------------------------------
# Flattening helpers — normalise the varied read-method return shapes to a
# flat ``list[dict]`` so the parity driver only compares row key-sets.
# --------------------------------------------------------------------------

def _one(row: "dict[str, Any] | None") -> "list[dict[str, Any]]":
    """Single-dict-or-None return → ``[row]`` or ``[]``."""
    return [row] if row is not None else []


def _flatten_map_of_lists(
    mapping: "dict[Any, list[dict[str, Any]]]",
) -> "list[dict[str, Any]]":
    """``dict[key, list[row]]`` return → flat list of the inner rows."""
    return [row for rows in mapping.values() for row in rows]


# --------------------------------------------------------------------------
# Seeders. Each seeds identical state on whichever backend it is handed,
# then calls one read method and returns its rows flattened.
# --------------------------------------------------------------------------

# --- Request-family (album_requests SELECT * projections) -----------------

def _seed_get_request(db: Any) -> "list[dict[str, Any]]":
    rid = db.add_request(
        "Parity Artist", "Parity Album", "request",
        mb_release_id="mbrel-get-request")
    return _one(db.get_request(rid))


def _seed_get_request_by_mb_release_id(db: Any) -> "list[dict[str, Any]]":
    db.add_request(
        "Parity Artist", "Parity Album", "request",
        mb_release_id="mbrel-parity")
    return _one(db.get_request_by_mb_release_id("mbrel-parity"))


def _seed_get_request_by_discogs_release_id(db: Any) -> "list[dict[str, Any]]":
    db.add_request(
        "Parity Artist", "Parity Album", "request",
        discogs_release_id="12345")
    return _one(db.get_request_by_discogs_release_id("12345"))


def _seed_get_request_by_release_id(db: Any) -> "list[dict[str, Any]]":
    # A non-UUID / non-numeric id falls back to the mb_release_id lookup on
    # both backends (identical ReleaseIdentity logic).
    db.add_request(
        "Parity Artist", "Parity Album", "request",
        mb_release_id="relid-parity")
    return _one(db.get_request_by_release_id("relid-parity"))


def _seed_get_request_by_replaces_request_id(db: Any) -> "list[dict[str, Any]]":
    old_id = db.add_request(
        "Parity Artist", "Parity Album", "request",
        mb_release_id="super-old")
    db.supersede_request_mbid(
        old_id,
        new_mb_release_id="super-new",
        new_mb_release_group_id=None,
        new_mb_artist_id=None,
        new_artist_name="Parity Artist",
        new_album_title="Parity Album (superseded)",
        new_year=None,
        new_country=None,
        new_tracks=[],
    )
    return _one(db.get_request_by_replaces_request_id(old_id))


def _seed_get_wanted(db: Any) -> "list[dict[str, Any]]":
    db.add_request(
        "Parity Artist", "Parity Album", "request",
        mb_release_id="wanted-parity", status="wanted")
    return list(db.get_wanted())


def _seed_get_by_status(db: Any) -> "list[dict[str, Any]]":
    db.add_request(
        "Parity Artist", "Parity Album", "request",
        mb_release_id="bystatus-parity", status="wanted")
    return list(db.get_by_status("wanted"))


def _seed_get_recent(db: Any) -> "list[dict[str, Any]]":
    rid = db.add_request(
        "Parity Artist", "Parity Album", "request",
        mb_release_id="recent-parity")
    db.log_download(rid, outcome="success")
    return list(db.get_recent())


def _seed_list_non_replaced_requests(db: Any) -> "list[dict[str, Any]]":
    db.add_request(
        "Parity Artist", "Parity Album", "request",
        mb_release_id="nonreplaced-parity")
    return list(db.list_non_replaced_requests())


def _seed_list_requests_by_artist(db: Any) -> "list[dict[str, Any]]":
    db.add_request(
        "Parity Artist", "Parity Album", "request",
        mb_release_id="byartist-parity")
    return list(db.list_requests_by_artist("Parity Artist"))


def _seed_list_requests_in_release_group(db: Any) -> "list[dict[str, Any]]":
    db.add_request(
        "Parity Artist", "Parity Album", "request",
        mb_release_id="inrg-parity", mb_release_group_id="rg-parity")
    return list(db.list_requests_in_release_group("rg-parity"))


# --- Tracks / downloading / denylist / field-resolution -------------------

def _seed_get_tracks(db: Any) -> "list[dict[str, Any]]":
    rid = db.add_request(
        "Parity Artist", "Parity Album", "request",
        mb_release_id="tracks-parity")
    db.set_tracks(rid, [
        {"disc_number": 1, "track_number": 1, "title": "T",
         "length_seconds": 100},
    ])
    return list(db.get_tracks(rid))


def _seed_get_downloading(db: Any) -> "list[dict[str, Any]]":
    rid = db.add_request(
        "Parity Artist", "Parity Album", "request",
        mb_release_id="downloading-parity", status="wanted")
    db.set_downloading(rid, '{"state":"Queued"}')
    return list(db.get_downloading())


def _seed_get_denylisted_users(db: Any) -> "list[dict[str, Any]]":
    rid = db.add_request(
        "Parity Artist", "Parity Album", "request",
        mb_release_id="denylist-parity")
    db.add_denylist(rid, "peer", "reason")
    return list(db.get_denylisted_users(rid))


def _seed_get_field_resolution(db: Any) -> "list[dict[str, Any]]":
    rid = db.add_request(
        "Parity Artist", "Parity Album", "request",
        mb_release_id="fieldres-single-parity")
    db.record_field_resolution(
        rid, "catalog_number", "unresolved_404", "http_404")
    return _one(db.get_field_resolution(rid, "catalog_number"))


# --- download_log projections (all share the dl.* history projection) -----

def _seed_get_download_log_entry(db: Any) -> "list[dict[str, Any]]":
    rid = db.add_request(
        "Parity Artist", "Parity Album", "request",
        mb_release_id="dlentry-parity")
    lid = db.log_download(rid, outcome="success")
    return _one(db.get_download_log_entry(lid))


def _seed_get_download_history(db: Any) -> "list[dict[str, Any]]":
    rid = db.add_request(
        "Parity Artist", "Parity Album", "request",
        mb_release_id="dlhistory-parity")
    db.log_download(rid, outcome="success")
    return list(db.get_download_history(rid))


def _seed_get_download_history_batch(db: Any) -> "list[dict[str, Any]]":
    rid = db.add_request(
        "Parity Artist", "Parity Album", "request",
        mb_release_id="dlbatch-parity")
    db.log_download(rid, outcome="success")
    return _flatten_map_of_lists(db.get_download_history_batch([rid]))


def _seed_get_latest_download_summaries(db: Any) -> "list[dict[str, Any]]":
    rid = db.add_request(
        "Parity Artist", "Parity Album", "request",
        mb_release_id="dlsummary-parity")
    db.log_download(rid, outcome="success")
    summaries = db.get_latest_download_summaries([rid])
    # Each value is ``{"latest": <download_log row>, "count": n}`` — the
    # ``latest`` sub-dict is the projection that can drift.
    return [summary["latest"] for summary in summaries.values()]


def _seed_get_log(db: Any) -> "list[dict[str, Any]]":
    rid = db.add_request(
        "Parity Artist", "Parity Album", "request",
        mb_release_id="getlog-parity")
    db.log_download(rid, outcome="success")
    return list(db.get_log())


# --- search_log projections -----------------------------------------------

def _seed_get_search_history(db: Any) -> "list[dict[str, Any]]":
    rid = db.add_request(
        "Parity Artist", "Parity Album", "request",
        mb_release_id="searchhist-parity")
    db.log_search(
        rid, query="q", outcome="found", result_count=5, elapsed_s=1.0)
    return list(db.get_search_history(rid))


def _seed_get_search_plan_stats_history(db: Any) -> "list[dict[str, Any]]":
    rid = db.add_request(
        "Parity Artist", "Parity Album", "request",
        mb_release_id="planstatshist-parity")
    db.log_search(
        rid, query="q", outcome="found", result_count=5, elapsed_s=1.0)
    return list(db.get_search_plan_stats_history(rid))


def _seed_get_search_history_page(db: Any) -> "list[dict[str, Any]]":
    rid = db.add_request(
        "Parity Artist", "Parity Album", "request",
        mb_release_id="searchpage-parity")
    db.log_search(
        rid, query="q", outcome="found", result_count=5, elapsed_s=1.0)
    # ``.rows`` is a raw search_log SELECT * projection wrapped in a Struct
    # that does NOT key-validate the inner rows — unwrap it like the
    # get_download_history_batch map (F4).
    return list(db.get_search_history_page(rid, limit=10).rows)


def _seed_get_legacy_search_log_summary(db: Any) -> "list[dict[str, Any]]":
    rid = db.add_request(
        "Parity Artist", "Parity Album", "request",
        mb_release_id="legacysummary-parity")
    # A plain log_search writes plan_id=NULL — a legacy row.
    db.log_search(
        rid, query="q", outcome="found", result_count=5, elapsed_s=1.0)
    # Returns (count, head-sample rows); the head sample is a narrow
    # 9-column hand-listed SELECT — the projection to key-compare (F2).
    return list(db.get_legacy_search_log_summary(rid, limit=10)[1])


def _seed_search_requests(db: Any) -> "list[dict[str, Any]]":
    db.add_request(
        "Parity Artist", "Parity Album", "request",
        mb_release_id="searchreq-parity")
    return list(db.search_requests("Parity"))


# --- import_jobs projection ------------------------------------------------

def _seed_get_active_import_job_for_request(db: Any) -> "list[dict[str, Any]]":
    from lib.import_queue import IMPORT_JOB_MANUAL, manual_import_payload

    rid = db.add_request(
        "Parity Artist", "Parity Album", "request",
        mb_release_id="activejob-parity")
    db.enqueue_import_job(
        IMPORT_JOB_MANUAL,
        request_id=rid,
        dedupe_key=f"manual:{rid}",
        payload=manual_import_payload(failed_path="/tmp/parity"),
    )
    return _one(db.get_active_import_job_for_request(rid))


# --- youtube_album_mappings projection ------------------------------------

def _youtube_mapping_row(**overrides: Any) -> PersistedYoutubeRow:
    """The upsert-row shape from
    ``TestReadProjectionParity._youtube_mapping_row`` — duplicated here
    because the registry module can't reach that test-class staticmethod.
    """
    fields: "dict[str, Any]" = {
        "yt_browse_id": "MPREb_parity",
        "yt_audio_playlist_id": "OLAK5uy_parity",
        "yt_url": "https://music.youtube.com/playlist?list=OLAK5uy_parity",
        "yt_year": 2020,
        "yt_track_count": 10,
        "album_title": "Parity Album",
        "album_artist": "Parity Artist",
        "yt_tracks": [
            PersistedTrack(
                title="Track 1", video_id="v1", length_seconds=200,
                track_number=1, disc_number=1,
                artists=[{"name": "Artist"}],
            ),
        ],
        "distances": [PersistedDistance(mbid="mb-1", distance=0.05)],
    }
    fields.update(overrides)
    return PersistedYoutubeRow(**fields)


def _seed_find_youtube_album_mapping_for_release(
    db: Any,
) -> "list[dict[str, Any]]":
    db.upsert_youtube_album_mapping(
        "rg-find-parity", "mb",
        [_youtube_mapping_row(
            yt_browse_id="MPREb_find",
            distances=[PersistedDistance(mbid="mb-find-1", distance=0.05)],
        )])
    return _one(db.find_youtube_album_mapping_for_release(
        source="mb", release_id="mb-find-1", browse_id="MPREb_find"))


# --- plex pins / unfindable probe -----------------------------------------

def _seed_get_pending_plex_added_at_pins(db: Any) -> "list[dict[str, Any]]":
    db.add_plex_added_at_pin(
        imported_path="/x",
        original_added_at=1700000000,
        rating_key="rk",
        request_id=None,
    )
    # captured_before must be AFTER the pin's captured_at (stamped NOW()).
    captured_before = datetime.now(timezone.utc) + timedelta(days=1)
    return list(db.get_pending_plex_added_at_pins(
        captured_before=captured_before, limit=100))


def _seed_list_unfindable_probe_candidates(db: Any) -> "list[dict[str, Any]]":
    db.add_request(
        "Parity Artist", "Parity Album", "request",
        mb_release_id="probe-parity", status="wanted")
    return list(db.list_unfindable_probe_candidates(
        limit=10, probe_interval_days=7))


# --------------------------------------------------------------------------
# The registry. One entry per read-projection method newly covered by a
# seeded keyset-parity check. Methods already covered by a hand-written
# parity test (get_wrong_matches, get_pipeline_overlay, ...) are NOT here —
# the audit finds those via AST. Methods with no raw-SELECT row projection
# (typed Struct returns, scalars, computed metric dicts) are in ALLOWLIST.
# --------------------------------------------------------------------------

PARITY_REGISTRY: "dict[str, Seeder]" = {
    # Request-family (album_requests SELECT * projections).
    "get_request": _seed_get_request,
    "get_request_by_mb_release_id": _seed_get_request_by_mb_release_id,
    "get_request_by_discogs_release_id":
        _seed_get_request_by_discogs_release_id,
    "get_request_by_release_id": _seed_get_request_by_release_id,
    "get_request_by_replaces_request_id":
        _seed_get_request_by_replaces_request_id,
    "get_wanted": _seed_get_wanted,
    "get_by_status": _seed_get_by_status,
    "get_recent": _seed_get_recent,
    "search_requests": _seed_search_requests,
    "list_non_replaced_requests": _seed_list_non_replaced_requests,
    "list_requests_by_artist": _seed_list_requests_by_artist,
    "list_requests_in_release_group": _seed_list_requests_in_release_group,
    # Tracks / downloading / denylist / field-resolution.
    "get_tracks": _seed_get_tracks,
    "get_downloading": _seed_get_downloading,
    "get_denylisted_users": _seed_get_denylisted_users,
    "get_field_resolution": _seed_get_field_resolution,
    # download_log projections.
    "get_download_log_entry": _seed_get_download_log_entry,
    "get_download_history": _seed_get_download_history,
    "get_download_history_batch": _seed_get_download_history_batch,
    "get_latest_download_summaries": _seed_get_latest_download_summaries,
    "get_log": _seed_get_log,
    # search_log projections.
    "get_search_history": _seed_get_search_history,
    "get_search_history_page": _seed_get_search_history_page,
    "get_search_plan_stats_history": _seed_get_search_plan_stats_history,
    "get_legacy_search_log_summary": _seed_get_legacy_search_log_summary,
    # import_jobs projection.
    "get_active_import_job_for_request":
        _seed_get_active_import_job_for_request,
    # youtube_album_mappings projection.
    "find_youtube_album_mapping_for_release":
        _seed_find_youtube_album_mapping_for_release,
    # plex pins / unfindable probe.
    "get_pending_plex_added_at_pins": _seed_get_pending_plex_added_at_pins,
    "list_unfindable_probe_candidates": _seed_list_unfindable_probe_candidates,
}


# --------------------------------------------------------------------------
# The allowlist. Read mirrors that are NOT keyset-parity-checked, each with
# a one-line rationale. This is the ratchet — it only shrinks. A read mirror
# belongs here iff it has no raw ``SELECT`` row projection to key-compare:
#
#   * Typed Struct/dataclass returns — validated at the msgspec/dataclass
#     boundary; the caller sees typed attributes, not a dict projection.
#   * Scalar returns (int / str / list[str] / set[str] / dict[int,int] /
#     dict[int,dict]) — no per-row column projection.
#   * Computed-aggregate metric dicts — the key set is statically assembled
#     in Python, not a raw SELECT column list, so the SELECT-drift class
#     the parity gate guards against does not apply.
# --------------------------------------------------------------------------

ALLOWLIST: "dict[str, str]" = {
    # --- Typed Struct / dataclass returns ---
    "get_active_search_plan":
        "typed ActiveSearchPlan | None return (wraps a PersistedSearchPlan "
        "in .plan) — validated at the dataclass boundary, no dict "
        "projection to key-compare",
    "get_import_job":
        "typed ImportJob return — msgspec/dataclass boundary, no dict "
        "projection",
    "get_saturation_summary":
        "typed SaturationSummary return — computed aggregate, no row "
        "projection",
    "get_search_plan_inspection":
        "typed inspection dataclass return — no raw SELECT dict projection",
    "get_search_plan_stats":
        "typed SearchPlanStats return — computed aggregate, no row "
        "projection",
    "get_unfindable_search_log_signal":
        "typed UnfindableSearchLogSignal return — computed aggregate, no "
        "row projection",
    "list_active_import_jobs":
        "list[ImportJob] — typed dataclass rows, no dict projection",
    "list_active_import_jobs_for_wrong_match":
        "list[ImportJob] — typed dataclass rows, no dict projection",
    "list_import_job_timeline":
        "list[ImportJob] — typed dataclass rows, no dict projection",
    "list_import_jobs":
        "list[ImportJob] — typed dataclass rows, no dict projection",
    "list_search_plan_classification_for_requests":
        "typed classification dataclass values — no raw SELECT dict "
        "projection",
    "list_wanted_for_plan_reconciliation":
        "typed reconciliation-row dataclass return — no dict projection",
    "find_active_youtube_import_job":
        "typed ImportJob | None return — validated at the msgspec boundary",
    "find_album_quality_evidence":
        "typed AlbumQualityEvidence Struct return — validated at the "
        "msgspec boundary",
    # --- Scalar returns ---
    "get_cooled_down_users":
        "list[str] usernames — scalar, no row projection",
    "get_download_log_candidate_evidence_id":
        "int | None FK scalar — no row projection",
    "get_import_job_candidate_evidence_id":
        "int | None FK scalar — no row projection",
    "get_owned_transfer_keys":
        "set[tuple[str,str]] (username, filename) membership keys — "
        "scalar set, no row projection; fake<->PG semantics pinned by "
        "mirrored tests in test_fakes.py + test_pipeline_db.py",
    "get_recent_successful_uploader":
        "str | None username — scalar, no row projection",
    "get_request_current_evidence_id":
        "int | None FK scalar — no row projection",
    "get_track_counts":
        "dict[int,int] request_id → count — scalar aggregate, no row "
        "projection",
    "find_orphan_youtube_running":
        "scalar list[int] return — no row projection",
    "list_active_release_group_ids":
        "set[str] release-group ids — scalar, no row projection",
    # --- Computed-aggregate metric dicts ---
    "get_peer_metrics":
        "computed peer-telemetry metric dict — key set assembled in "
        "Python, not a raw SELECT column list",
    "get_pipeline_dashboard_metrics":
        "computed dashboard metric dict — key set assembled in Python, "
        "not a raw SELECT column list",
    "get_search_plan_readiness":
        "computed readiness metric dict — key set assembled in Python, "
        "not a raw SELECT column list",
}
