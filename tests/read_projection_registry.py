"""Read-projection parity registry (issue #546 W1).

The write side has ``.claude/rules/test-fidelity.md`` Rule A: every
``PipelineDB`` write method carries a real-PG round-trip test. The READ
side has the mirror problem — ``FakePipelineDB`` (``tests/fakes/pipeline_db/``)
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
flattened to ``list[dict]``. ``PARITY_REGISTRY`` compares only KEYS
(ids and timestamps are backend-assigned/time-anchored), so its seeders
must never put timestamps or random values in row KEYS. Every seeder
must produce >= 1 row on BOTH backends — a vacuous parity check is
worthless.

``VALUE_PARITY_REGISTRY`` (issue #1278 item 7) is the second axis: the
same backend-agnostic seeder, run on both backends, with every
non-excluded field compared by VALUE. It gates the mirrors whose values
are decided by SQL the fake reimplements — whether the key audit
EXCUSES them (``ALLOWLIST``: a percentile, a computed metric dict) or
merely HAND-COVERS their key set (a rollup view's aggregate, a
``DISTINCT ON`` collapse, a view join that decides membership, a
table's column DEFAULTs). In every case the key set was not the risk:
what the SQL computes is. Extracting that SQL into shared Python would
be the wrong fix — the database is the authority on its own
aggregation — so the gate is on the output. Each excluded field carries
its own written rationale; see ``ValueExclusion``.
"""
# ruff: noqa: UP037 - quoted Any annotations are part of the typing ratchet
from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any

from lib.cycle_counters import CycleCounters
from lib.pipeline_db import (
    CleanupJournalIntent,
    ConsumedAttemptInput,
    PersistedDistance,
    PersistedTrack,
    PersistedYoutubeRow,
    PipelineDB,
    SearchPlanItemInput,
)
from lib.quality import CandidateScore
from tests.fakes import FakePipelineDB

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
_READ_METHOD_PREFIXES: tuple[str, ...] = (
    "get_", "list_", "search_", "find_", "fetch_",
)


def enumerate_read_mirrors() -> list[str]:
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


def _seed_get_acquisition(
    db: PipelineDB | FakePipelineDB,
) -> "list[dict[str, Any]]":
    request_id = db.add_request(
        "Parity Artist",
        "Parity Processing Album",
        "request",
        mb_release_id="acquisition-parity",
    )
    enqueued_at = "2026-07-29T00:00:00+00:00"
    assert db.set_downloading(
        request_id,
        (
            '{"filetype":"flac",'
            f'"enqueued_at":"{enqueued_at}",'
            '"files":[]}'
        ),
        expected_status="wanted",
    )
    handoff = db.handoff_automation_import(
        request_id=request_id,
        expected_enqueued_at=enqueued_at,
        canonical_path="/processing/albums/acquisition-parity",
        message="read projection parity",
    )
    assert handoff.committed and handoff.job is not None
    db.insert_youtube_running(
        request_id=request_id,
        browse_id="acquisition-parity",
        audio_playlist_id=None,
        yt_url="https://music.youtube.com/playlist?list=acquisition-parity",
        expected_track_count=1,
    )
    payload = db.get_acquisition()
    return [
        *(dict(row) for row in payload["acquisition"]),
        *(dict(row) for row in payload["youtube_ingest"]),
    ]


def _seed_get_processing_cleanup_journal(
    db: Any,
) -> "list[dict[str, Any]]":
    request_id = db.add_request(
        "Parity Artist",
        "Parity Cleanup Album",
        "request",
        mb_release_id="cleanup-journal-parity",
    )
    enqueued_at = "2026-07-29T00:00:01+00:00"
    assert db.set_downloading(
        request_id,
        (
            '{"filetype":"flac",'
            f'"enqueued_at":"{enqueued_at}",'
            '"files":[]}'
        ),
        expected_status="wanted",
    )
    handoff = db.handoff_automation_import(
        request_id=request_id,
        expected_enqueued_at=enqueued_at,
        canonical_path="/processing/albums/cleanup-journal-parity",
        message="cleanup journal read projection parity",
    )
    assert handoff.committed and handoff.job is not None
    db.create_processing_cleanup_journal(
        request_id=request_id,
        job_id=handoff.job.id,
        intent=CleanupJournalIntent(
            action="no_op",
            source_path="/processing/albums/cleanup-journal-parity",
            source_manifest=({"path": "01.flac", "size": 12},),
            source_manifest_hash="cleanup-journal-parity-hash",
        ),
    )
    return _one(db.get_processing_cleanup_journal(
        request_id=request_id,
        job_id=handoff.job.id,
    ))


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


def _seed_list_denylist_rows(db: Any) -> "list[dict[str, Any]]":
    rid = db.add_request(
        "Parity Artist", "Parity Album", "request",
        mb_release_id="denylist-world-parity")
    db.add_denylist(rid, "peer", "reason")
    return list(db.list_denylist_rows())


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


def _seed_get_linked_import_logs(db: Any) -> "list[dict[str, Any]]":
    rid = db.add_request(
        "Parity Artist", "Parity Album", "request",
        mb_release_id="linked-import-log-parity")
    source_id = db.log_download(rid, outcome="rejected")
    db.log_download(
        rid,
        outcome="force_import",
        source_download_log_id=source_id,
    )
    return list(db.get_linked_import_logs([source_id]))


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
    captured_before = datetime.now(UTC) + timedelta(days=1)
    return list(db.get_pending_plex_added_at_pins(
        captured_before=captured_before, limit=100))


def _seed_get_pending_jellyfin_date_created_pins(db: Any) -> "list[dict[str, Any]]":
    db.add_jellyfin_date_created_pin(
        imported_path="/x",
        original_date_created="2026-04-26T18:31:04.4425337Z",
        album_item_id="alb-1",
        children_item_ids=["tr-1", "tr-2"],
        request_id=None,
    )
    # captured_before must be AFTER the pin's captured_at (stamped NOW()).
    captured_before = datetime.now(UTC) + timedelta(days=1)
    return list(db.get_pending_jellyfin_date_created_pins(
        captured_before=captured_before, limit=100))


def _seed_list_unfindable_probe_candidates(db: Any) -> "list[dict[str, Any]]":
    db.add_request(
        "Parity Artist", "Parity Album", "request",
        mb_release_id="probe-parity", status="wanted")
    return list(db.list_unfindable_probe_candidates(
        limit=10, probe_interval_days=7))


def _seed_get_unfindable_run_metrics(
    db: PipelineDB | FakePipelineDB,
) -> "list[dict[str, Any]]":
    # Typed narrower than this module's usual ``db: Any`` seeder signature
    # (both concrete backends are already imported here) -- avoids adding
    # a new escape-hatch occurrence to the tests-tree typing-ratchet
    # freeze (issue #765/#784) for a two-call seeder that doesn't need it.
    db.record_unfindable_run_metrics(
        cohort_total=10, due_backlog_at_start=5,
        batch_limit=5, candidates_processed=5, probes_attempted=5,
        breaker_tripped=False, duration_seconds=12.5,
        categorised_count=1, no_change_count=4)
    # dict(row) rather than list(...): get_unfindable_run_metrics returns
    # the typed UnfindableRunMetricsRow TypedDict, not a bare dict, and
    # list()'s invariance rejects it as-is against this module's
    # deliberately loose Seeder contract.
    return [dict(row) for row in db.get_unfindable_run_metrics(limit=5)]


# --------------------------------------------------------------------------
# The registry. One entry per read-projection method newly covered by a
# seeded keyset-parity check. Methods already covered by a hand-written
# parity test (get_wrong_matches, get_pipeline_overlay, ...) are NOT here —
# the audit finds those via AST. Methods with no raw-SELECT row projection
# (typed Struct returns, scalars, computed metric dicts) are in ALLOWLIST.
# --------------------------------------------------------------------------

PARITY_REGISTRY: dict[str, Seeder] = {
    # Request-family (album_requests SELECT * projections).
    "get_request": _seed_get_request,
    "get_request_by_mb_release_id": _seed_get_request_by_mb_release_id,
    "get_request_by_discogs_release_id":
        _seed_get_request_by_discogs_release_id,
    "get_request_by_release_id": _seed_get_request_by_release_id,
    "get_request_by_replaces_request_id":
        _seed_get_request_by_replaces_request_id,
    "get_acquisition": _seed_get_acquisition,
    "get_processing_cleanup_journal":
        _seed_get_processing_cleanup_journal,
    "get_wanted": _seed_get_wanted,
    "get_by_status": _seed_get_by_status,
    "search_requests": _seed_search_requests,
    "list_non_replaced_requests": _seed_list_non_replaced_requests,
    "list_requests_by_artist": _seed_list_requests_by_artist,
    "list_requests_in_release_group": _seed_list_requests_in_release_group,
    # Tracks / downloading / denylist / field-resolution.
    "get_tracks": _seed_get_tracks,
    "get_downloading": _seed_get_downloading,
    "get_denylisted_users": _seed_get_denylisted_users,
    "list_denylist_rows": _seed_list_denylist_rows,
    "get_field_resolution": _seed_get_field_resolution,
    # download_log projections.
    "get_download_log_entry": _seed_get_download_log_entry,
    "get_download_history": _seed_get_download_history,
    "get_download_history_batch": _seed_get_download_history_batch,
    "get_latest_download_summaries": _seed_get_latest_download_summaries,
    "get_log": _seed_get_log,
    "get_linked_import_logs": _seed_get_linked_import_logs,
    # search_log projections.
    "get_search_history": _seed_get_search_history,
    "get_search_history_page": _seed_get_search_history_page,
    "get_search_plan_stats_history": _seed_get_search_plan_stats_history,
    "get_legacy_search_log_summary": _seed_get_legacy_search_log_summary,
    # youtube_album_mappings projection.
    "find_youtube_album_mapping_for_release":
        _seed_find_youtube_album_mapping_for_release,
    # plex pins / unfindable probe.
    "get_pending_plex_added_at_pins": _seed_get_pending_plex_added_at_pins,
    "get_pending_jellyfin_date_created_pins": _seed_get_pending_jellyfin_date_created_pins,
    "list_unfindable_probe_candidates": _seed_list_unfindable_probe_candidates,
    "get_unfindable_run_metrics": _seed_get_unfindable_run_metrics,
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

ALLOWLIST: dict[str, str] = {
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
    "list_terminal_force_action_cleanup_jobs":
        "list[ImportJob] — typed terminal cleanup rows, no dict projection",
    "list_terminal_force_wrong_match_cleanup_jobs":
        "list[ImportJob] — typed terminal cleanup rows, no dict projection",
    "list_automation_import_jobs_for_startup_recovery":
        "list[ImportJob] — exact processing-owner typed dataclass rows, "
        "no dict projection",
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
    "get_convergence_signals":
        "dict[int, ConvergenceSignal] — typed msgspec Struct values; the "
        "raw aggregate projection is converted before crossing the boundary",
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
        "mirrored tests in test_fakes_transfer_ledger.py + "
        "test_pipeline_db.py",
    "get_retained_failure_paths":
        "set[str] retained paths — scalar set, no row projection; "
        "measurement-failure and quarantine semantics are pinned by mirrored "
        "tests in test_fakes.py + test_pipeline_db.py",
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


# --------------------------------------------------------------------------
# VALUE parity (issue #1278 item 7). The key gate above is silent about what
# an aggregate COMPUTES. Some of the entries below are ALLOWLISTED there (a
# percentile, a computed metric dict) — those stay allowlisted, since the
# rationale ("not a raw SELECT column list") is still true — and the rest
# are merely HAND-COVERED for keys by a test in tests/test_pipeline_db.py.
# Either way the key set was never the risk: SQL, not the column list,
# decides their values (view-join membership, a DISTINCT ON collapse, a
# table's column DEFAULTs).
#
# The two legitimate exclusion classes, plus surrogate keys, are spelled
# once and cited per field; anything else is a real divergence to fix in the
# fake (production SQL is the authority on its own aggregation).
# --------------------------------------------------------------------------

#: A backend's own surrogate keys. The suite resets tables with DELETE
#: (``tests.helpers.delete_all_rows``), which never resets a PostgreSQL
#: sequence, while ``FakePipelineDB`` counts from 1 in every instance —
#: so the two ids cannot match, by construction, whatever the seeder does.
_EXCL_SURROGATE_ID = (
    "backend-assigned surrogate key — PG sequences keep climbing across the "
    "suite's DELETE-based reset while the fake counts from 1 per instance"
)

#: Server clock vs the fake's clock. Real PG stamps ``NOW()`` inside the
#: statement; the fake calls ``_utcnow()`` in-process. Two different
#: instants, so any value derived from "the moment the read ran" differs by
#: however long the two calls were apart.
_EXCL_WALL_CLOCK = (
    "wall-clock instant — real PG stamps NOW() server-side, the fake calls "
    "_utcnow() in-process; the two moments are never the same"
)

#: Deliberate, documented approximation in the fake's dashboard mirror:
#: production uses ``percentile_cont`` (interpolating), the fake a
#: nearest-rank cut. See ``FakePipelineDB.get_pipeline_dashboard_metrics``.
_EXCL_PERCENTILE = (
    "percentile — production SQL interpolates with percentile_cont, the fake "
    "takes a nearest-rank cut (deliberate; the SQL is the authority on exact "
    "statistics)"
)


@dataclass(frozen=True)
class ValueExclusion:
    """One field held out of value parity, with its mandatory rationale.

    Both fields are required positionally, and a blank rationale raises —
    so an exclusion cannot be written without saying why the two backends
    are allowed to disagree on it. ``path`` is a dotted path into the
    projected row (``"totals.known_peers"``), with ``[]`` standing for
    "every element of this list" (``"days[].new_peers"``); it therefore
    excludes that field uniformly across rows and list elements rather
    than one lucky index.
    """

    path: str
    rationale: str

    def __post_init__(self) -> None:
        if not self.path.strip():
            raise ValueError("a value-parity exclusion needs a field path")
        if not self.rationale.strip():
            raise ValueError(
                f"value-parity exclusion {self.path!r} needs a one-line "
                f"rationale — say why the two backends may disagree on it"
            )


@dataclass(frozen=True)
class ValueParityEntry:
    """One value-gated read mirror: a seeder plus its held-out fields."""

    seeder: Seeder
    exclusions: tuple[ValueExclusion, ...] = field(default_factory=tuple)

    @property
    def excluded_paths(self) -> frozenset[str]:
        return frozenset(exclusion.path for exclusion in self.exclusions)


@dataclass(frozen=True)
class ValueParityResult:
    """What ``compare_projection_values`` found.

    ``substantive_leaves`` counts compared leaves that are neither ``None``
    nor an empty/zero value — the non-degeneracy measure. A payload of
    nothing but nulls, empty lists, and zeros compares equal for free, and
    that is precisely the vacuous pass this module's header warns about.

    ``excluded_hits`` names every exclusion path the walk actually reached.
    An exclusion whose field was renamed away, or whose seeder stopped
    producing it, silently stops excluding anything; the driver asserts
    every declared path is in here, so a dead exclusion is a failure
    rather than a comment that quietly became false.
    """

    mismatches: tuple[str, ...]
    compared_leaves: int
    substantive_leaves: int
    excluded_hits: frozenset[str]


def _is_substantive(value: object) -> bool:
    return value is not None and bool(value)


def _compare_node(
    real: object,
    fake: object,
    path: str,
    excluded: "frozenset[str]",
    mismatches: "list[str]",
    counts: "list[int]",
    hits: "set[str]",
) -> None:
    """Recursive value walk. ``counts`` is ``[compared, substantive]``.

    Exclusion matching is EXACT on the full path, never a prefix or
    substring: excluding ``"id"`` must not also excuse ``mb_release_id``
    (#1278 item 7 runner survivor S3).
    """
    if path in excluded:
        hits.add(path)
        return
    label = path or "<row>"
    if isinstance(real, dict) or isinstance(fake, dict):
        if not (isinstance(real, dict) and isinstance(fake, dict)):
            mismatches.append(
                f"{label}: real PG returned {type(real).__name__}, "
                f"FakePipelineDB returned {type(fake).__name__}")
            return
        real_keys = set(real)
        fake_keys = set(fake)
        if real_keys != fake_keys:
            mismatches.append(
                f"{label}: key sets differ — only in real PG "
                f"{sorted(real_keys - fake_keys)}, only in FakePipelineDB "
                f"{sorted(fake_keys - real_keys)}")
        for key in sorted(real_keys & fake_keys):
            _compare_node(
                real[key], fake[key],
                f"{path}.{key}" if path else str(key),
                excluded, mismatches, counts, hits)
        return
    if isinstance(real, list) or isinstance(fake, list):
        if not (isinstance(real, list) and isinstance(fake, list)):
            mismatches.append(
                f"{label}: real PG returned {type(real).__name__}, "
                f"FakePipelineDB returned {type(fake).__name__}")
            return
        if len(real) != len(fake):
            mismatches.append(
                f"{label}: real PG returned {len(real)} element(s), "
                f"FakePipelineDB returned {len(fake)}")
            return
        for real_item, fake_item in zip(real, fake, strict=True):
            _compare_node(
                real_item, fake_item, f"{path}[]",
                excluded, mismatches, counts, hits)
        return
    counts[0] += 1
    if _is_substantive(real):
        counts[1] += 1
    if real != fake:
        mismatches.append(
            f"{label}: real PG {real!r} != FakePipelineDB {fake!r}")


def compare_projection_values(
    real_rows: "list[dict[str, object]]",
    fake_rows: "list[dict[str, object]]",
    *,
    excluded: "frozenset[str]",
) -> ValueParityResult:
    """Compare two seeded projections field by field.

    Row count first (a membership difference is the most consequential
    divergence an aggregate can have), then every non-excluded leaf of
    every row, recursively.
    """
    mismatches: list[str] = []
    counts = [0, 0]
    hits: set[str] = set()
    if len(real_rows) != len(fake_rows):
        mismatches.append(
            f"row count: real PG returned {len(real_rows)} row(s), "
            f"FakePipelineDB returned {len(fake_rows)}")
    for index, (real_row, fake_row) in enumerate(
        zip(real_rows, fake_rows, strict=False)
    ):
        row_mismatches: list[str] = []
        _compare_node(
            real_row, fake_row, "", excluded, row_mismatches, counts, hits)
        mismatches.extend(f"row {index} {m}" for m in row_mismatches)
    return ValueParityResult(
        mismatches=tuple(mismatches),
        compared_leaves=counts[0],
        substantive_leaves=counts[1],
        excluded_hits=frozenset(hits),
    )


# --------------------------------------------------------------------------
# Value-parity seeders. Same contract as the key seeders, with one extra
# obligation: seed values that DISTINGUISH. A world where every count is 1
# and every string is "x" passes a value comparison with two fields swapped.
# --------------------------------------------------------------------------

#: Deliberately far outside every window under test (the search-summary
#: view's 14 days, the saturation default of 14, the dashboard's 24h/7d),
#: so no seeded row can drift across a boundary while the test runs.
_OUT_OF_WINDOW = timedelta(days=20)

_SEED_ANCHOR: "datetime | None" = None


def seed_anchor() -> datetime:
    """One shared "now" for every value-parity seeder in this process.

    A seeder runs TWICE — once per backend — so a fresh
    ``datetime.now(UTC)`` inside it would stamp two different instants and
    make every seeded timestamp differ by the microseconds between the two
    calls. Anchoring once per process makes seeded timestamps genuinely
    comparable, which is what lets ``last_search_at`` stay IN the value
    comparison instead of being excused as wall-clock noise. It is still
    derived from the real clock, so relative windows (14 days, 24 hours)
    mean the same thing they mean in production.
    """
    global _SEED_ANCHOR
    if _SEED_ANCHOR is None:
        _SEED_ANCHOR = datetime.now(UTC)
    return _SEED_ANCHOR


def _stamp_latest_search_log(
    db: PipelineDB | FakePipelineDB,
    *,
    created_at: datetime,
    plan_strategy: str | None = None,
) -> None:
    """Backend-appropriate stamp of the ``search_log`` row just written.

    ``log_search`` takes neither ``created_at`` nor ``plan_strategy`` on
    EITHER backend (plan context is written only by the plan-attempt
    loggers, which need a persisted plan), so a window-crossing world has
    to be stamped after the fact. The two branches write the same logical
    row; only the mechanism differs, which is the same latitude the
    ``_seed_*`` helpers already take when a backend needs its own call.
    ``MAX(id)`` IS the row just written: a seeder runs serially against a
    database emptied by ``make_db()``, with no concurrent writer.
    """
    if isinstance(db, FakePipelineDB):
        entry = db.search_logs[-1]
        entry.created_at = created_at
        entry.plan_strategy = plan_strategy
        return
    db._execute(
        "UPDATE search_log SET created_at = %s, plan_strategy = %s "
        "WHERE id = (SELECT MAX(id) FROM search_log)",
        (created_at, plan_strategy),
    )


def _candidate_scores(username: str) -> "list[CandidateScore]":
    return [CandidateScore(
        username=username, dir=f"/{username}/album", filetype="flac",
        matched_tracks=9, total_tracks=10, avg_ratio=0.91,
        missing_titles=["Missing One"], file_count=11,
    )]


def _seed_search_summary_world(
    db: PipelineDB | FakePipelineDB,
) -> "tuple[int, int]":
    """Two requests whose search history straddles the view's 14-day window.

    Request A carries BOTH in-window and out-of-window rows, so every
    aggregate the view computes has a different value depending on whether
    the window is applied. Request B is the must-still-work control: purely
    in-window, one found search, no rejection reasons.

    The in-window rejection reasons are a deliberate 1-1 TIE
    (``b_stale_metadata`` logged first, then ``a_count_mismatch``):
    PostgreSQL's ``MODE() WITHIN GROUP (ORDER BY sl.rejection_reason)``
    breaks it on the sort order, so the tie is decided by the reason
    itself, never by insertion order.
    """
    now = seed_anchor()
    request_a = db.add_request(
        "Value Parity Artist", "Straddling Album", "request",
        mb_release_id="search-summary-value-parity-a")
    request_b = db.add_request(
        "Value Parity Artist", "In Window Album", "request",
        mb_release_id="search-summary-value-parity-b")

    # --- Out of window: rich values the 14-day cut must throw away. ---
    db.log_search(
        request_a, query="ancient wide", outcome="no_match",
        result_count=0, elapsed_s=9.0, pre_filter_skip_count=7,
        rejection_reason="c_ancient_only",
        candidates=_candidate_scores("ancient-peer"))
    _stamp_latest_search_log(
        db, created_at=now - _OUT_OF_WINDOW,
        plan_strategy="ancient_strategy")
    db.log_search(
        request_a, query="ancient narrow", outcome="found",
        result_count=980, elapsed_s=8.0, pre_filter_skip_count=7,
        rejection_reason="c_ancient_only")
    _stamp_latest_search_log(
        db, created_at=now - _OUT_OF_WINDOW + timedelta(hours=1))

    # --- In window: the values both backends must actually report. ---
    db.log_search(
        request_a, query="recent one", outcome="no_match",
        result_count=4, elapsed_s=1.5, pre_filter_skip_count=2,
        rejection_reason="b_stale_metadata")
    _stamp_latest_search_log(db, created_at=now - timedelta(days=3))
    db.log_search(
        request_a, query="recent two", outcome="no_match",
        result_count=0, elapsed_s=2.5, pre_filter_skip_count=1,
        rejection_reason="a_count_mismatch",
        candidates=_candidate_scores("recent-peer"))
    _stamp_latest_search_log(
        db, created_at=now - timedelta(days=2),
        plan_strategy="recent_strategy")
    db.log_search(
        request_a, query="recent three", outcome="exhausted",
        result_count=960, elapsed_s=3.5)
    _stamp_latest_search_log(db, created_at=now - timedelta(days=1))
    # Inside the 14-day window but outside a 7-day one. Without a row in
    # the 7-14 day band, narrowing the window to 7 days changes nothing
    # and the mutant survives (#1278 item 7 runner survivor S4). It
    # carries NO rejection_reason on purpose, so the 1-1 tie above stays
    # a tie; its counts still move total_searches, near_cap_count and
    # pre_filter_skips_total when the window narrows.
    db.log_search(
        request_a, query="ten days back", outcome="no_match",
        result_count=955, elapsed_s=4.5, pre_filter_skip_count=6)
    _stamp_latest_search_log(db, created_at=now - timedelta(days=10))

    db.log_search(
        request_b, query="control", outcome="found",
        result_count=12, elapsed_s=0.5, pre_filter_skip_count=3,
        candidates=_candidate_scores("control-peer"))
    _stamp_latest_search_log(
        db, created_at=now - timedelta(hours=6),
        plan_strategy="control_strategy")
    return request_a, request_b


def _seed_value_get_search_summaries_for_requests(
    db: PipelineDB | FakePipelineDB,
) -> "list[dict[str, object]]":
    request_a, request_b = _seed_search_summary_world(db)
    # One call per request: the production SELECT has no ORDER BY, so a
    # single two-id call would compare rows in an arbitrary order.
    rows: list[dict[str, object]] = []
    for request_id in (request_a, request_b):
        summaries = db.get_search_summaries_for_requests([request_id])
        rows.extend(dict(row) for row in summaries.values())
    return rows


def _seed_value_list_triage_page(
    db: PipelineDB | FakePipelineDB,
) -> "list[dict[str, object]]":
    """``search_not_converting`` membership — the view join, not the rollup.

    Same window question as the summary rollup, one adapter further out:
    production JOINs ``request_search_summary`` and the fake calls its own
    ``_compute_search_summary``, so a windowless fake puts a request whose
    only searches are ancient on an operator worklist production leaves
    off it.
    """
    from lib.triage_service import parse_filter

    now = seed_anchor()
    ancient_only = db.add_request(
        "Triage Value Artist", "Ancient Searches Only", "request",
        mb_release_id="triage-value-parity-ancient", status="wanted")
    db.log_search(
        ancient_only, query="ancient", outcome="no_match", result_count=0,
        elapsed_s=1.0)
    _stamp_latest_search_log(db, created_at=now - _OUT_OF_WINDOW)

    converting_now = db.add_request(
        "Triage Value Artist", "Recent Searches", "request",
        mb_release_id="triage-value-parity-recent", status="wanted")
    db.log_search(
        converting_now, query="recent", outcome="no_match", result_count=3,
        elapsed_s=2.0)
    _stamp_latest_search_log(db, created_at=now - timedelta(days=2))

    return [
        dict(row) for row in db.list_triage_page(
            filter_spec=parse_filter("search_not_converting"),
            page_size=50,
            after_request_id=None,
        )
    ]


def _seed_value_get_saturation_summary(
    db: PipelineDB | FakePipelineDB,
) -> "list[dict[str, object]]":
    """Saturation over the same straddling world — a second window aggregate.

    Request A's ancient rows carry ``LimitReached`` final states and seven
    pre-filter skips each, so an unwindowed count would inflate every field
    of the summary at once.
    """
    import dataclasses

    request_a, _request_b = _seed_search_summary_world(db)
    db.log_search(
        request_a, query="saturated ancient", outcome="no_match",
        final_state="Completed, ResponseLimitReached",
        result_count=1000, elapsed_s=7.0, pre_filter_skip_count=5)
    _stamp_latest_search_log(
        db, created_at=seed_anchor() - _OUT_OF_WINDOW)
    db.log_search(
        request_a, query="saturated recent", outcome="no_match",
        final_state="Completed, FileLimitReached",
        result_count=1000, elapsed_s=6.0, pre_filter_skip_count=4)
    _stamp_latest_search_log(
        db, created_at=seed_anchor() - timedelta(days=5))
    return [dataclasses.asdict(db.get_saturation_summary(request_a))]


def _consume_next_plan_item(
    db: "PipelineDB | FakePipelineDB",
    request_id: int,
    *,
    result_count: int,
    elapsed_s: float,
    browse_time_s: float = 0.0,
    match_time_s: float = 0.0,
    peers_browsed: int = 0,
    fanout_waves: int = 0,
    pre_filter_skip_count: int = 0,
    rejection_reason: str | None = None,
    matcher_score_top1: float | None = None,
) -> None:
    """Consume the item the cursor actually schedules next, executor-style.

    Re-reads the active plan before every consume so the recorded plan
    context, ``plan_item_count`` and ``cycle_count_snapshot`` always match
    the cursor state — exactly what the executor does — and the row is
    never flagged stale.
    """
    active = db.get_active_search_plan(request_id)
    assert active is not None
    item = next(
        i for i in active.items if i.ordinal == active.next_ordinal)
    db.record_consumed_search_attempt(ConsumedAttemptInput(
        request_id=request_id,
        plan_id=active.plan.id,
        plan_item_id=item.id,
        plan_ordinal=item.ordinal,
        plan_strategy=item.strategy,
        plan_canonical_query_key=item.canonical_query_key,
        plan_repeat_group=item.repeat_group,
        plan_generator_id=active.plan.generator_id,
        query=item.query,
        outcome="no_match",
        result_count=result_count,
        elapsed_s=elapsed_s,
        browse_time_s=browse_time_s,
        match_time_s=match_time_s,
        peers_browsed=peers_browsed,
        fanout_waves=fanout_waves,
        pre_filter_skip_count=pre_filter_skip_count,
        rejection_reason=rejection_reason,
        matcher_score_top1=matcher_score_top1,
        plan_item_count=len(active.items),
        cycle_count_snapshot=active.cycle_count,
    ))


def _seed_value_plan_with_consumed_attempts(
    db: "PipelineDB | FakePipelineDB", *, mbid: str,
) -> int:
    """A superseded plan, an active plan spanning two cycles, and a
    legacy plan-less row — all through the REAL seams on either backend.

    This is the persisted-plan seeding the two former
    ``VALUE_GATE_EXEMPTIONS`` entries said their non-vacuous worlds
    needed (#1278 item-7 residual 3): ``record_consumed_search_attempt``
    is the only writer of plan context + ``plan_cycle_snapshot``, so both
    ``get_search_plan_stats`` and ``get_unfindable_search_log_signal``
    aggregate over rows only this seam can produce. The world is
    deliberately rich (review round: a one-plan/one-cycle world compared
    half of each payload vacuously):

      * plan A ("g-old", one item) takes one consumed attempt carrying
        the wrong-pressing forensic signature (strict_count_mismatch at
        a high matcher score), then is atomically superseded by plan B —
        so the stats' superseded cohort is non-empty;
      * plan B ("g-value-parity", two items) takes three consumed
        attempts: two in cycle 0 (the second wraps the cursor) and one
        in cycle 1 — so ``plan_cycle_snapshot`` grouping is load-bearing
        (two distinct zero-find cycles, not one);
      * one plain ``log_search`` row has no plan context — so the
        stats' ``legacy_bucket`` is non-empty.
    """
    request_id = db.add_request(
        "Parity Artist", "Parity Album", "request", mb_release_id=mbid)
    db.log_search(
        request_id, query="legacy planless q", outcome="no_match",
        result_count=3, elapsed_s=1.0)
    db.create_successful_search_plan(
        request_id=request_id, generator_id="g-old",
        items=[
            SearchPlanItemInput(
                ordinal=0, strategy="core", query="old q0",
                canonical_query_key="k-old-0"),
        ],
        set_active=True,
    )
    _consume_next_plan_item(
        db, request_id, result_count=40, elapsed_s=3.5,
        rejection_reason="strict_count_mismatch", matcher_score_top1=0.91)
    db.supersede_search_plan_with_replacement(
        request_id=request_id, generator_id="g-value-parity",
        items=[
            SearchPlanItemInput(
                ordinal=0, strategy="core", query="parity q0",
                canonical_query_key="k0"),
            SearchPlanItemInput(
                ordinal=1, strategy="wildcard", query="parity q1",
                canonical_query_key="k1"),
        ],
    )
    _consume_next_plan_item(
        db, request_id, result_count=12, elapsed_s=2.5,
        browse_time_s=1.25, match_time_s=0.5, peers_browsed=3,
        fanout_waves=1, pre_filter_skip_count=2)
    _consume_next_plan_item(db, request_id, result_count=7, elapsed_s=1.5)
    _consume_next_plan_item(db, request_id, result_count=5, elapsed_s=1.75)
    return request_id


def _seed_value_get_search_plan_stats(
    db: "PipelineDB | FakePipelineDB",
) -> "list[dict[str, object]]":
    """All three stats cohorts populated: active, superseded, legacy."""
    import dataclasses

    request_id = _seed_value_plan_with_consumed_attempts(
        db, mbid="planstats-value-parity")
    return [dataclasses.asdict(
        db.get_search_plan_stats(request_id, current_only=False))]


def _seed_value_get_unfindable_search_log_signal(
    db: "PipelineDB | FakePipelineDB",
) -> "list[dict[str, object]]":
    """Both classifier scalars non-zero, with the cycle grouping
    load-bearing: two distinct zero-find cycles (an any-row mutant
    reads 1) and one wrong-pressing hit above the threshold."""
    import dataclasses

    request_id = _seed_value_plan_with_consumed_attempts(
        db, mbid="unfindable-signal-value-parity")
    return [dataclasses.asdict(db.get_unfindable_search_log_signal(
        request_id, window_days=14, matcher_score_threshold=0.5))]


def _seed_value_get_peer_metrics(
    db: PipelineDB | FakePipelineDB,
) -> "list[dict[str, object]]":
    """Peer growth curve — a dense per-day series with a cumulative total."""
    db.record_peer_observations(["peer-alpha", "peer-beta", "peer-gamma"])
    # A second pass observes one known peer again and adds one new one, so
    # ``known_peers`` (4) and the day's ``new_peers`` are not the same
    # number as the observation count.
    db.record_peer_observations(["peer-alpha", "peer-delta"])
    return [dict(db.get_peer_metrics(days=3))]


def _seed_value_get_pipeline_dashboard_metrics(
    db: PipelineDB | FakePipelineDB,
) -> "list[dict[str, object]]":
    """The whole dashboard envelope, values and all.

    ``TestDashboardFakeParity`` already gates the SHAPE of this payload —
    key sets, list lengths, leaf type categories — and explicitly does not
    compare values. This gates what production's server-side aggregates
    and the fake's Python mirror each COMPUTE from the same telemetry. The
    seeded world deliberately gives every counter a different number.

    Search and cycle rows straddle the panels' own 24h/6h cuts: without a
    row on the far side of a boundary, widening or removing a window
    changes nothing and the mutant survives (#1278 item 7, reader F7).
    """
    now = seed_anchor()
    request_id = db.add_request(
        "Dashboard Value Artist", "Dashboard Value Album", "request",
        mb_release_id="dashboard-value-parity", status="wanted")
    db.log_search(
        request_id, query="found q", outcome="found", result_count=5,
        elapsed_s=2.0, variant="v1", final_state="Completed",
        browse_time_s=42.0, match_time_s=1.0, peers_browsed=110,
        peers_browsed_lazy=5, fanout_waves=6)
    db.log_search(
        request_id, query="loop", outcome="no_match", elapsed_s=1.0,
        result_count=0)
    db.log_search(
        request_id, query="third", outcome="exhausted", elapsed_s=3.0,
        result_count=7, peers_browsed=4, fanout_waves=2)
    # Outside 24h: every searches/outcome/peer counter on both windows
    # must exclude it.
    db.log_search(
        request_id, query="yesterday plus", outcome="found",
        result_count=11, elapsed_s=7.0, peers_browsed=9, fanout_waves=3)
    _stamp_latest_search_log(db, created_at=now - timedelta(hours=25))
    # Inside 24h but outside 6h: only the 6h window may exclude it.
    db.log_search(
        request_id, query="this morning", outcome="no_match",
        result_count=2, elapsed_s=5.0, peers_browsed=6, fanout_waves=1)
    _stamp_latest_search_log(db, created_at=now - timedelta(hours=9))
    db.record_cycle_metrics(
        cycle_total_s=300.0, wanted_total=10,
        counters=CycleCounters(
            browse_time_s=20.0, match_time_s=10.0, search_time_s=240.0,
            peers_browsed=8, fanout_waves=2, find_download_queued=4,
            find_download_completed=3, cycle_searches_watchdog_killed=1))
    # Outside 24h, so the outlier panel and both cycle windows must drop
    # it despite its far larger cycle_total_s.
    db.record_cycle_metrics(
        completed_at=now - timedelta(hours=26),
        cycle_total_s=900.0, wanted_total=12,
        counters=CycleCounters(
            browse_time_s=60.0, match_time_s=30.0, search_time_s=780.0,
            peers_browsed=17, fanout_waves=5, find_download_queued=9,
            find_download_completed=8, cycle_searches_watchdog_killed=2))
    db.record_peer_observations(["dash-peer-a", "dash-peer-b"])
    # The partition/probe CHECKs on unfindable_run_metrics (migration 077)
    # are real: categorised + no_change must equal candidates_processed,
    # and probes_attempted must equal it too when nothing was skipped.
    db.record_unfindable_run_metrics(
        cohort_total=10, due_backlog_at_start=6, batch_limit=4,
        candidates_processed=3, probes_attempted=3, breaker_tripped=False,
        duration_seconds=12.5, categorised_count=1, no_change_count=2)
    return [dict(db.get_pipeline_dashboard_metrics())]


def _verified_lossless_evidence_id(
    db: PipelineDB | FakePipelineDB,
    *,
    mb_release_id: str,
    source_path: str,
) -> int:
    """Persist one verified-lossless evidence row and return its id.

    Built through ``tests.evidence_helpers.make_album_quality_evidence`` so the
    content-addressed fingerprint is production's own, then read back by
    that fingerprint exactly as the linking call sites do.
    """
    from lib.quality import (
        AlbumQualityEvidenceFile,
        AudioQualityMeasurement,
        VerifiedLosslessProof,
    )
    from tests.evidence_helpers import make_album_quality_evidence

    evidence = make_album_quality_evidence(
        mb_release_id=mb_release_id,
        source_path=source_path,
        files=[AlbumQualityEvidenceFile(
            relative_path="01 - joined.flac",
            size_bytes=987654,
            mtime_ns=1_700_000_000_000_000_000,
            extension="flac",
            container="flac",
            codec="flac",
        )],
        measurement=AudioQualityMeasurement(
            min_bitrate_kbps=901, avg_bitrate_kbps=934, is_cbr=False,
            format="FLAC", spectral_grade="genuine",
            spectral_bitrate_kbps=None, spectral_subject="source",
            spectral_provenance="measured", cliff_hz=21000,
            codec_family="lossless", spectral_measurement_version=2,
        ),
        verified_lossless_proof=VerifiedLosslessProof(
            provenance="measured", source="flac",
            classifier="value-parity-classifier", detail="genuine",
        ),
        codec="flac", container="flac", storage_format="FLAC",
    )
    db.upsert_album_quality_evidence(evidence)
    persisted = db.find_album_quality_evidence(
        mb_release_id=evidence.mb_release_id,
        snapshot_fingerprint=evidence.snapshot_fingerprint,
    )
    assert persisted is not None and persisted.id is not None
    return persisted.id


def _seed_value_get_wrong_matches(
    db: PipelineDB | FakePipelineDB,
) -> "list[dict[str, object]]":
    """The Wrong Matches queue's ``DISTINCT ON`` collapse, by value.

    Two rejections share a ``failed_path`` (the newest must win, with a
    distinguishable ``import_result.decision`` on each so the collapse is
    checked by value and not just by count) and a third has its own. The
    legacy denorm quality columns are populated so the production
    ``COALESCE(evidence, denorm)`` fallback is compared with real values
    rather than a pair of nulls, and both audit blobs are seeded the way
    production writes them: as JSON strings into JSONB columns.

    Exactly ONE surviving row carries a linked
    ``album_quality_evidence`` row, so the ``LEFT JOIN``'s two sides are
    both compared: the joined row's real ``verified_lossless`` /
    measurement values, and the unjoined row's SQL NULLs. With every row
    unjoined, forcing the joined branch to ``None`` changes nothing and
    the mutant survives (#1278 item 7 runner survivor S5).
    """
    import json as _json

    request_id = db.add_request(
        "Wrong Match Artist", "Wrong Match Album", "request",
        mb_release_id="wrong-match-value-parity",
        mb_release_group_id="wrong-match-value-parity-rg")
    log_ids: dict[str, int] = {}
    for username, path, grade, bitrate, decision in (
        ("older-peer", "/processing/albums/wrong_matches/first",
         "C", 190, "superseded_decision"),
        ("newer-peer", "/processing/albums/wrong_matches/first",
         "A", 285, "surviving_decision"),
        ("other-peer", "/processing/albums/wrong_matches/second",
         "B", 240, "other_path_decision"),
    ):
        log_ids[username] = db.log_download(
            request_id=request_id,
            soulseek_username=username,
            outcome="rejected",
            spectral_grade=grade,
            spectral_bitrate=bitrate,
            import_result=_json.dumps({"decision": decision}),
            validation_result=_json.dumps({
                "scenario": "high_distance",
                "distance": 0.25,
                "failed_path": path,
            }),
        )
    db.set_download_log_candidate_evidence(
        log_ids["other-peer"],
        _verified_lossless_evidence_id(
            db, mb_release_id="wrong-match-value-parity",
            source_path="/processing/albums/wrong_matches/second"),
    )
    return [dict(row) for row in db.get_wrong_matches()]


def _seed_value_get_request(
    db: PipelineDB | FakePipelineDB,
) -> "list[dict[str, object]]":
    """A freshly added request, read back field by field.

    ``FakePipelineDB.add_request`` hand-mirrors this table's column
    DEFAULTs. The key registry's ``get_request`` entry proves the fake
    returns the same COLUMNS; a default that drifted to the wrong VALUE
    would sail straight through it.

    The row is then moved to ``downloading`` so ``active_download_state``
    carries a value: it is a JSONB column written as a JSON string, and a
    row left at ``wanted`` compares it as ``None`` on both sides — the
    vacuous pass that hid the fake projecting the raw string (#1278 item
    7, divergence 6).
    """
    request_id = db.add_request(
        "Default Mirror Artist", "Default Mirror Album", "request",
        mb_release_id="request-defaults-value-parity",
        mb_release_group_id="request-defaults-value-parity-rg",
        year=1998)
    assert db.set_downloading(
        request_id,
        (
            '{"filetype":"flac",'
            '"enqueued_at":"2026-07-29T00:00:00+00:00",'
            '"username":"defaults-peer",'
            '"attempt_fingerprint":"defaults-fingerprint",'
            '"files":[{"filename":"01.flac","size":4096}]}'
        ),
        expected_status="wanted",
    )
    row = db.get_request(request_id)
    return [dict(row)] if row is not None else []


VALUE_PARITY_REGISTRY: dict[str, ValueParityEntry] = {
    "get_request": ValueParityEntry(
        seeder=_seed_value_get_request,
        exclusions=(
            ValueExclusion("id", _EXCL_SURROGATE_ID),
            ValueExclusion("created_at", _EXCL_WALL_CLOCK),
            ValueExclusion("updated_at", _EXCL_WALL_CLOCK),
            ValueExclusion("last_attempt_at", _EXCL_WALL_CLOCK),
        ),
    ),
    "get_search_summaries_for_requests": ValueParityEntry(
        seeder=_seed_value_get_search_summaries_for_requests,
        exclusions=(
            ValueExclusion("request_id", _EXCL_SURROGATE_ID),
        ),
    ),
    "list_triage_page": ValueParityEntry(
        seeder=_seed_value_list_triage_page,
        exclusions=(
            ValueExclusion("id", _EXCL_SURROGATE_ID),
        ),
    ),
    "get_saturation_summary": ValueParityEntry(
        seeder=_seed_value_get_saturation_summary,
    ),
    "get_search_plan_stats": ValueParityEntry(
        seeder=_seed_value_get_search_plan_stats,
        exclusions=(
            ValueExclusion("request_id", _EXCL_SURROGATE_ID),
            ValueExclusion(
                "current.slots[].identity.plan_id", _EXCL_SURROGATE_ID),
            ValueExclusion(
                "current.query_groups[].identity.plan_id",
                _EXCL_SURROGATE_ID),
            ValueExclusion(
                "current.slots[].last_seen_at", _EXCL_WALL_CLOCK),
            ValueExclusion(
                "current.query_groups[].last_seen_at", _EXCL_WALL_CLOCK),
            ValueExclusion(
                "superseded_and_legacy.slots[].identity.plan_id",
                _EXCL_SURROGATE_ID),
            ValueExclusion(
                "superseded_and_legacy.query_groups[].identity.plan_id",
                _EXCL_SURROGATE_ID),
            ValueExclusion(
                "superseded_and_legacy.slots[].last_seen_at",
                _EXCL_WALL_CLOCK),
            ValueExclusion(
                "superseded_and_legacy.query_groups[].last_seen_at",
                _EXCL_WALL_CLOCK),
            ValueExclusion(
                "superseded_and_legacy.legacy_bucket.last_seen_at",
                _EXCL_WALL_CLOCK),
        ),
    ),
    "get_unfindable_search_log_signal": ValueParityEntry(
        seeder=_seed_value_get_unfindable_search_log_signal,
    ),
    # Two deliberate absences, both recorded in VALUE_GATE_EXEMPTIONS or
    # here so a later reader does not re-derive them:
    #
    # get_search_plan_readiness — its CASE ladder already has a dedicated
    # real-PG value-parity test with a strictly richer world,
    # ``tests/test_pipeline_db.py::TestPlanReadinessParity`` (all five
    # buckets, exact counts; #1278 item 7 PR 2). A registry entry would be
    # a weaker duplicate, not extra coverage.
    #
    # get_pipeline_overlay — DECLINED (#1278 item 7, PR 3; flagged for this
    # PR by PR #1289's body). The fake emulates production's WHERE clause
    # and join by hand and its own docstring calls that emulation
    # approximate, so a value entry would either trip on a divergence the
    # fake is documented to have or dodge it with a world narrow enough to
    # avoid the approximation — evidence either way, but not about the
    # seam. ``tests/test_pipeline_db.py::TestGetPipelineOverlay`` holds the
    # seam with absolute assertions on real PG instead
    # (``test_maps_known_mbids_with_overlay_fields``,
    # ``test_matches_and_keys_exact_mb_and_discogs_release_identities``,
    # ``test_numeric_overlay_supports_legacy_layout_and_prefers_dedicated_column``).
    "get_peer_metrics": ValueParityEntry(
        seeder=_seed_value_get_peer_metrics,
        exclusions=(
            ValueExclusion("totals.tracked_since", _EXCL_WALL_CLOCK),
        ),
    ),
    "get_wrong_matches": ValueParityEntry(
        seeder=_seed_value_get_wrong_matches,
        exclusions=(
            ValueExclusion("download_log_id", _EXCL_SURROGATE_ID),
            ValueExclusion("request_id", _EXCL_SURROGATE_ID),
        ),
    ),
    "get_pipeline_dashboard_metrics": ValueParityEntry(
        seeder=_seed_value_get_pipeline_dashboard_metrics,
        exclusions=(
            ValueExclusion("generated_at", _EXCL_WALL_CLOCK),
            ValueExclusion("cycles.recent[].id", _EXCL_SURROGATE_ID),
            ValueExclusion("cycles.recent[].created_at", _EXCL_WALL_CLOCK),
            ValueExclusion("cycles.outliers[].id", _EXCL_SURROGATE_ID),
            ValueExclusion("cycles.outliers[].created_at", _EXCL_WALL_CLOCK),
            ValueExclusion("cycles.windows[].median_cycle_s",
                           _EXCL_PERCENTILE),
            ValueExclusion("cycles.windows[].median_search_s",
                           _EXCL_PERCENTILE),
            ValueExclusion("cycles.windows[].p95_cycle_s", _EXCL_PERCENTILE),
            ValueExclusion("searches.windows[].median_elapsed_s",
                           _EXCL_PERCENTILE),
            ValueExclusion("searches.windows[].p95_elapsed_s",
                           _EXCL_PERCENTILE),
            ValueExclusion("coverage.oldest_last_search_at",
                           _EXCL_WALL_CLOCK),
            ValueExclusion("coverage.match_rate_series_24h[].bucket_start",
                           _EXCL_WALL_CLOCK),
            ValueExclusion("coverage.match_rate_series_28d[].bucket_start",
                           _EXCL_WALL_CLOCK),
            ValueExclusion("coverage.top_loop_suspects[].request_id",
                           _EXCL_SURROGATE_ID),
            ValueExclusion("coverage.top_loop_suspects[].last_search_at",
                           _EXCL_WALL_CLOCK),
            ValueExclusion("coverage.stale_wanted[].request_id",
                           _EXCL_SURROGATE_ID),
            ValueExclusion("coverage.stale_wanted[].last_search_at",
                           _EXCL_WALL_CLOCK),
            ValueExclusion("coverage.stale_wanted[].hours_since_search",
                           _EXCL_WALL_CLOCK),
            ValueExclusion("coverage.wanted_trend.latest_sample_at",
                           _EXCL_WALL_CLOCK),
            ValueExclusion("coverage.wanted_trend.series_24h[].sampled_at",
                           _EXCL_WALL_CLOCK),
            ValueExclusion("coverage.wanted_trend.windows[].start_sample_at",
                           _EXCL_WALL_CLOCK),
            ValueExclusion("coverage.wanted_trend.windows[].end_sample_at",
                           _EXCL_WALL_CLOCK),
            ValueExclusion("coverage.wanted_trend.windows[].delta_per_hour",
                           _EXCL_WALL_CLOCK),
            ValueExclusion("coverage.wanted_trend.windows[].drain_per_hour",
                           _EXCL_WALL_CLOCK),
            ValueExclusion("coverage.wanted_trend.windows[].eta_hours",
                           _EXCL_WALL_CLOCK),
            ValueExclusion("peers.totals.tracked_since", _EXCL_WALL_CLOCK),
            ValueExclusion("peers.heavy_queries[].search_log_id",
                           _EXCL_SURROGATE_ID),
            ValueExclusion("peers.heavy_queries[].request_id",
                           _EXCL_SURROGATE_ID),
            ValueExclusion("peers.heavy_queries[].created_at",
                           _EXCL_WALL_CLOCK),
            ValueExclusion("unfindable.recent_runs[].id", _EXCL_SURROGATE_ID),
            ValueExclusion("unfindable.recent_runs[].created_at",
                           _EXCL_WALL_CLOCK),
            ValueExclusion("unfindable.backlog_trend.latest_sample_at",
                           _EXCL_WALL_CLOCK),
            ValueExclusion("unfindable.backlog_trend.series[].sampled_at",
                           _EXCL_WALL_CLOCK),
        ),
    ),
}


#: The substring an ``ALLOWLIST`` rationale uses to say "this mirror is a
#: SQL-owned aggregate, not a SELECT column list". A bounded substring test
#: over hand-written rationale prose — deliberately NOT an inference about
#: what a method does. The rationale grammar is hand-maintained data, like
#: every other registry in this module; measured 2026-08-31 it selects
#: exactly the six aggregate entries and nothing else.
COMPUTED_RATIONALE_MARKER = "computed"


#: Read mirrors the key ``ALLOWLIST`` marks as computed aggregates that are
#: deliberately NOT in ``VALUE_PARITY_REGISTRY``, each with its reason. This
#: is the value axis's completeness ratchet: without it, an aggregate can be
#: excused from the KEY gate for being computed and then never value-gated
#: either, with nothing recording the gap.
VALUE_GATE_EXEMPTIONS: dict[str, str] = {
    "get_search_plan_readiness":
        "value-gated by the hand-written "
        "tests/test_pipeline_db.py::TestPlanReadinessParity, whose world is "
        "strictly richer (all five buckets, exact counts); a registry entry "
        "would be a weaker duplicate",
}
