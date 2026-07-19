"""Tests for ``lib.youtube_ingest_service.YoutubeIngestService``.

Covers ``submit`` and ``run_job`` against ``FakePipelineDB`` and the
kwarg-DI seams documented in the module. The plan's U3 test scenario
matrix is exercised in one ``TestCase`` per outcome branch (and one
subTest table for the wrong-state fan-out across all forbidden statuses).
"""

from __future__ import annotations

import unittest
from pathlib import Path
from typing import Any, Optional
from unittest.mock import patch

import msgspec

from lib.import_queue import IMPORT_JOB_YOUTUBE
from lib import pipeline_db as _pipeline_db_mod  # module-import so the raise
# site below resolves YoutubeInFlightError at call time. tests/test_pipeline_db.py
# does importlib.reload(pipeline_db); a stale symbol import here would raise the
# pre-reload class while the service catches the post-reload one — see the
# matching comment in lib/youtube_ingest_service.py.
from lib.youtube_ingest_service import (
    OUTCOME_EXIT_CODE,
    OUTCOME_HTTP_STATUS,
    RunResult,
    SubmitResult,
    YoutubeImportPayload,
    YoutubeIngestMetadata,
    YoutubeIngestService,
    YtdlpRunResult,
    classify_youtube_failure,
)
from tests.fakes import FakePipelineDB
from tests.helpers import make_request_row


# ---------------------------------------------------------------------------
# Fixtures + helpers.
# ---------------------------------------------------------------------------

MB_RG = "11111111-1111-1111-1111-111111111111"
MB_REL = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
BROWSE = "MPREb_happy_path"
PLAYLIST = "OLAK5uy-happy"
YT_URL = "https://music.youtube.com/playlist?list=OLAK5uy-happy"
EXPECTED_TRACKS = 10
DISCOGS_REL = "12345"
DISCOGS_MASTER = "999"


def _seed_resolver_row(
    pdb: FakePipelineDB,
    *,
    resolver_mapping_id: int = 501,
    rg: str = MB_RG,
    source: str = "mb",
    browse_id: str = BROWSE,
    yt_url: str = YT_URL,
    yt_audio_playlist_id: Optional[str] = PLAYLIST,
    distances_for_mbid: str = MB_REL,
    total_mb_tracks: Optional[int] = EXPECTED_TRACKS,
    extra_rows: Optional[list[dict[str, Any]]] = None,
) -> None:
    """Pre-seed ``youtube_album_mappings`` with one matching row."""
    rows: list[dict[str, Any]] = [{
        "id": resolver_mapping_id,
        "yt_browse_id": browse_id,
        "yt_audio_playlist_id": yt_audio_playlist_id,
        "yt_url": yt_url,
        "yt_year": 2024,
        "yt_track_count": total_mb_tracks or EXPECTED_TRACKS,
        "album_title": "Test Album",
        "album_artist": "Test Artist",
        "yt_tracks": [
            {"title": f"Track {i + 1}", "video_id": f"vid-{i + 1}"}
            for i in range(total_mb_tracks or EXPECTED_TRACKS)
        ],
        "distances": [
            {
                "mbid": distances_for_mbid,
                "outcome": "ok",
                "distance": 0.05,
                "total_local_tracks": total_mb_tracks,
                "total_mb_tracks": total_mb_tracks,
                "matched_tracks": total_mb_tracks,
            },
        ],
    }]
    if extra_rows:
        rows.extend(extra_rows)
    pdb.seed_youtube_album_mapping(rg, source, rows)


def _seed_wanted_request(
    pdb: FakePipelineDB,
    *,
    request_id: int = 42,
    status: str = "wanted",
    mb_release_id: str = MB_REL,
    mb_release_group_id: str = MB_RG,
) -> None:
    pdb.seed_request(make_request_row(
        id=request_id,
        status=status,
        mb_release_id=mb_release_id,
        mb_release_group_id=mb_release_group_id,
    ))


def _seed_discogs_request(
    pdb: FakePipelineDB,
    *,
    request_id: int = 77,
    status: str = "wanted",
    discogs_release_id: str = DISCOGS_REL,
    mb_release_id: Optional[str] = None,
) -> None:
    pdb.seed_request(make_request_row(
        id=request_id,
        status=status,
        mb_release_id=mb_release_id,
        mb_release_group_id=None,
        discogs_release_id=discogs_release_id,
        source="discogs",
    ))


def _tracks(count: int) -> list[dict[str, Any]]:
    return [
        {"track_number": i + 1, "title": f"Track {i + 1}"}
        for i in range(count)
    ]


def _track_count_returning(value: Optional[int]):
    """Factory: deterministic ``mb_track_count_fn`` returning a fixed value."""

    def _fn(_mbid: str) -> Optional[int]:
        return value

    return _fn


def _make_service(
    pdb: FakePipelineDB,
    *,
    mb_count: Optional[int] = EXPECTED_TRACKS,
    mb_track_count_fn: Any = None,
    ytdlp_runner_fn: Any = None,
    stage_dir_fn: Any = None,
    staging_root: Path = Path("/tmp/cratedigger-test-staging"),
) -> YoutubeIngestService:
    """Construct the service with sensible test defaults for every port."""
    kwargs: dict[str, Any] = {
        "mb_track_count_fn": (
            mb_track_count_fn
            if mb_track_count_fn is not None
            else _track_count_returning(mb_count)
        ),
        "staging_root": staging_root,
    }
    if ytdlp_runner_fn is not None:
        kwargs["ytdlp_runner_fn"] = ytdlp_runner_fn
    if stage_dir_fn is not None:
        kwargs["stage_dir_fn"] = stage_dir_fn
    return YoutubeIngestService(pdb, **kwargs)


# ---------------------------------------------------------------------------
# Outcome map completeness — pinned by the audit at module level.
# ---------------------------------------------------------------------------


class TestOutcomeMapsAreComplete(unittest.TestCase):
    def test_outcome_set_is_stable(self) -> None:
        """``OUTCOME_HTTP_STATUS`` and ``OUTCOME_EXIT_CODE`` must agree on keys."""
        self.assertEqual(
            set(OUTCOME_HTTP_STATUS),
            set(OUTCOME_EXIT_CODE),
            "OUTCOME_HTTP_STATUS / OUTCOME_EXIT_CODE keys drifted",
        )

    def test_every_submit_outcome_has_an_entry(self) -> None:
        """Every value in the ``SubmitOutcome`` Literal must map."""
        expected = {
            "accepted",
            "request_not_found",
            "wrong_state",
            "in_flight",
            "no_resolver_mapping",
            "track_count_precheck_failed",
            "transient",
        }
        self.assertEqual(set(OUTCOME_HTTP_STATUS), expected)
        self.assertEqual(set(OUTCOME_EXIT_CODE), expected)


# ---------------------------------------------------------------------------
# submit() — happy path.
# ---------------------------------------------------------------------------


class TestSubmitHappyPath(unittest.TestCase):
    """Covers AE1: wanted request + valid resolver mapping + matching counts."""

    def test_wanted_request_accepted(self) -> None:
        pdb = FakePipelineDB()
        _seed_wanted_request(pdb, request_id=42, status="wanted")
        _seed_resolver_row(pdb)
        svc = _make_service(pdb)

        result = svc.submit(request_id=42, browse_id=BROWSE)

        self.assertIsInstance(result, SubmitResult)
        self.assertEqual(result.outcome, "accepted")
        self.assertIsNotNone(result.download_log_id)
        # Exactly one new running row.
        running = [
            e for e in pdb.download_logs
            if e.source == "youtube" and e.outcome == "youtube_running"
        ]
        self.assertEqual(len(running), 1)
        row = running[0]
        self.assertEqual(row.request_id, 42)
        self.assertEqual(row.id, result.download_log_id)
        # Metadata blob should match the submission contract.
        metadata = row.youtube_metadata
        self.assertIsNotNone(metadata)
        assert metadata is not None  # narrow for pyright
        self.assertEqual(metadata["browse_id"], BROWSE)
        self.assertEqual(metadata["yt_url"], YT_URL)
        self.assertEqual(metadata["audio_playlist_id"], PLAYLIST)
        self.assertEqual(metadata["expected_track_count"], EXPECTED_TRACKS)
        self.assertEqual(metadata["resolver_mapping_id"], 501)
        self.assertEqual(
            metadata["per_track_video_ids"],
            [f"vid-{i + 1}" for i in range(EXPECTED_TRACKS)],
        )

    def test_unsearchable_request_accepted_covers_ae9(self) -> None:
        """AE9: rescue from ``unsearchable`` is identical to wanted."""
        pdb = FakePipelineDB()
        _seed_wanted_request(pdb, request_id=99, status="unsearchable")
        _seed_resolver_row(pdb)
        svc = _make_service(pdb)

        result = svc.submit(request_id=99, browse_id=BROWSE)

        self.assertEqual(result.outcome, "accepted")
        self.assertIsNotNone(result.download_log_id)
        running = [
            e for e in pdb.download_logs
            if e.source == "youtube" and e.outcome == "youtube_running"
        ]
        self.assertEqual(len(running), 1)
        self.assertEqual(running[0].request_id, 99)

    def test_discogs_request_accepted_from_cached_mapping(self) -> None:
        pdb = FakePipelineDB()
        _seed_discogs_request(
            pdb,
            request_id=77,
            status="wanted",
            mb_release_id=DISCOGS_REL,
        )
        pdb.set_tracks(77, _tracks(EXPECTED_TRACKS))
        _seed_resolver_row(
            pdb,
            rg=DISCOGS_MASTER,
            source="discogs",
            distances_for_mbid=DISCOGS_REL,
            total_mb_tracks=EXPECTED_TRACKS,
        )

        def _exploding_mb(_mbid: str) -> Optional[int]:
            raise AssertionError("Discogs submit must not call MB mirror")

        svc = _make_service(pdb, mb_track_count_fn=_exploding_mb)

        result = svc.submit(request_id=77, browse_id=BROWSE)

        self.assertEqual(result.outcome, "accepted")
        self.assertIsNotNone(result.download_log_id)
        running = [
            e for e in pdb.download_logs
            if e.source == "youtube" and e.outcome == "youtube_running"
        ]
        self.assertEqual(len(running), 1)
        metadata = running[0].youtube_metadata
        assert metadata is not None
        self.assertEqual(metadata["expected_track_count"], EXPECTED_TRACKS)


# ---------------------------------------------------------------------------
# submit() — wrong state (R3 / AE2).
# ---------------------------------------------------------------------------


class TestSubmitWrongState(unittest.TestCase):
    def test_imported_returns_wrong_state(self) -> None:
        pdb = FakePipelineDB()
        _seed_wanted_request(pdb, request_id=42, status="imported")
        _seed_resolver_row(pdb)
        svc = _make_service(pdb)

        result = svc.submit(request_id=42, browse_id=BROWSE)

        self.assertEqual(result.outcome, "wrong_state")
        self.assertIsNotNone(result.detail)
        assert result.detail is not None  # narrow for pyright
        self.assertIn("imported", result.detail)
        self.assertEqual(
            [e for e in pdb.download_logs if e.source == "youtube"], [])

    def test_forbidden_statuses_subtest_table(self) -> None:
        """Every non-{wanted, unsearchable} status rejects as wrong-state."""
        forbidden_statuses = ("downloading", "imported", "replaced")
        for status in forbidden_statuses:
            with self.subTest(status=status):
                pdb = FakePipelineDB()
                _seed_wanted_request(pdb, request_id=7, status=status)
                _seed_resolver_row(pdb)
                svc = _make_service(pdb)

                result = svc.submit(request_id=7, browse_id=BROWSE)

                self.assertEqual(result.outcome, "wrong_state")
                self.assertEqual(
                    [e for e in pdb.download_logs if e.source == "youtube"],
                    [],
                    f"unexpected row inserted for status={status}",
                )


# ---------------------------------------------------------------------------
# submit() — idempotency / in-flight (R4 / AE3).
# ---------------------------------------------------------------------------


class TestSubmitInFlight(unittest.TestCase):
    def test_in_flight_returns_existing_id(self) -> None:
        pdb = FakePipelineDB()
        _seed_wanted_request(pdb, request_id=42)
        _seed_resolver_row(pdb)
        svc = _make_service(pdb)

        first = svc.submit(request_id=42, browse_id=BROWSE)
        self.assertEqual(first.outcome, "accepted")
        self.assertIsNotNone(first.download_log_id)

        second = svc.submit(request_id=42, browse_id=BROWSE)
        self.assertEqual(second.outcome, "in_flight")
        # The existing id should be carried through so the operator can
        # inspect "you already have a rescue running for this request".
        self.assertEqual(second.download_log_id, first.download_log_id)
        self.assertIsNotNone(second.detail)
        assert second.detail is not None
        self.assertIn(str(first.download_log_id), second.detail)

    def test_active_youtube_import_job_blocks_duplicate_submit(self) -> None:
        from lib.import_queue import youtube_import_dedupe_key, youtube_import_payload

        pdb = FakePipelineDB()
        _seed_wanted_request(pdb, request_id=42)
        _seed_resolver_row(pdb)
        pdb.enqueue_import_job(
            IMPORT_JOB_YOUTUBE,
            request_id=42,
            dedupe_key=youtube_import_dedupe_key(777),
            payload=youtube_import_payload(
                staged_path="/tmp/yt-staged",
                request_id=42,
                browse_id=BROWSE,
            ),
        )
        svc = _make_service(pdb)

        result = svc.submit(request_id=42, browse_id=BROWSE)

        self.assertEqual(result.outcome, "in_flight")
        self.assertEqual(result.download_log_id, 777)
        self.assertEqual(result.import_job_id, 1)
        self.assertEqual(result.blocking_resource, "youtube_import")
        self.assertIsNotNone(result.detail)
        assert result.detail is not None
        self.assertIn("youtube_import job", result.detail)
        self.assertEqual(
            [e for e in pdb.download_logs if e.source == "youtube"], [])

    def test_active_youtube_import_blocks_different_browse_for_request(self) -> None:
        from lib.import_queue import youtube_import_dedupe_key, youtube_import_payload

        pdb = FakePipelineDB()
        _seed_wanted_request(pdb, request_id=42)
        _seed_resolver_row(pdb, browse_id="MPREb_new_choice")
        pdb.enqueue_import_job(
            IMPORT_JOB_YOUTUBE,
            request_id=42,
            dedupe_key=youtube_import_dedupe_key(778),
            payload=youtube_import_payload(
                staged_path="/tmp/yt-staged",
                request_id=42,
                browse_id="MPREb_existing_choice",
                download_log_id=778,
            ),
        )
        svc = _make_service(pdb)

        result = svc.submit(request_id=42, browse_id="MPREb_new_choice")

        self.assertEqual(result.outcome, "in_flight")
        self.assertEqual(result.download_log_id, 778)
        self.assertEqual(result.blocking_resource, "youtube_import")
        self.assertEqual(
            [e for e in pdb.download_logs if e.source == "youtube"], [])


# ---------------------------------------------------------------------------
# submit() — request not found.
# ---------------------------------------------------------------------------


class TestSubmitRequestNotFound(unittest.TestCase):
    def test_unknown_request_id_returns_not_found(self) -> None:
        pdb = FakePipelineDB()  # no seeded request
        svc = _make_service(pdb)

        result = svc.submit(request_id=9999, browse_id=BROWSE)

        self.assertEqual(result.outcome, "request_not_found")
        self.assertEqual(pdb.download_logs, [])


# ---------------------------------------------------------------------------
# submit() — no resolver mapping (R6 / AE4).
# ---------------------------------------------------------------------------


class TestSubmitNoResolverMapping(unittest.TestCase):
    def test_browse_id_not_in_mapping(self) -> None:
        pdb = FakePipelineDB()
        _seed_wanted_request(pdb, request_id=42)
        _seed_resolver_row(pdb, browse_id="MPREb_other_one")
        svc = _make_service(pdb)

        result = svc.submit(request_id=42, browse_id=BROWSE)

        self.assertEqual(result.outcome, "no_resolver_mapping")
        # No row inserted on the precheck-failure path.
        self.assertEqual(
            [e for e in pdb.download_logs if e.source == "youtube"], [])

    def test_no_resolver_rows_at_all(self) -> None:
        pdb = FakePipelineDB()
        _seed_wanted_request(pdb, request_id=42)
        # NO seed_resolver_row — completely empty.
        svc = _make_service(pdb)

        result = svc.submit(request_id=42, browse_id=BROWSE)
        self.assertEqual(result.outcome, "no_resolver_mapping")

    def test_request_with_no_mb_release_group_id(self) -> None:
        """Default resolver can't proceed without ``mb_release_group_id``."""
        pdb = FakePipelineDB()
        pdb.seed_request(make_request_row(
            id=42,
            status="wanted",
            mb_release_id=MB_REL,
            mb_release_group_id=None,
        ))
        _seed_resolver_row(pdb)
        svc = _make_service(pdb)

        result = svc.submit(request_id=42, browse_id=BROWSE)
        self.assertEqual(result.outcome, "no_resolver_mapping")

    def test_discogs_request_without_cached_mapping(self) -> None:
        pdb = FakePipelineDB()
        _seed_discogs_request(pdb, request_id=77)
        pdb.set_tracks(77, _tracks(EXPECTED_TRACKS))
        svc = _make_service(pdb)

        result = svc.submit(request_id=77, browse_id=BROWSE)

        self.assertEqual(result.outcome, "no_resolver_mapping")
        self.assertIsNotNone(result.detail)
        assert result.detail is not None
        self.assertIn("Discogs resolver mapping", result.detail)
        self.assertEqual(
            [e for e in pdb.download_logs if e.source == "youtube"], [])


# ---------------------------------------------------------------------------
# submit() — track-count precheck mismatch (R7 / AE5).
# ---------------------------------------------------------------------------


class TestSubmitTrackCountPrecheckFailed(unittest.TestCase):
    def test_resolver_cache_drifted_from_mb(self) -> None:
        """Resolver cache total_mb_tracks=10, MB now says 11 → mismatch."""
        pdb = FakePipelineDB()
        _seed_wanted_request(pdb, request_id=42)
        _seed_resolver_row(pdb, total_mb_tracks=10)
        # mb_track_count_fn returns 11 — MB drifted since the resolver
        # last ran.
        svc = _make_service(pdb, mb_count=11)

        result = svc.submit(request_id=42, browse_id=BROWSE)

        self.assertEqual(result.outcome, "track_count_precheck_failed")
        self.assertIsNotNone(result.detail)
        assert result.detail is not None
        self.assertIn("10", result.detail)
        self.assertIn("11", result.detail)
        self.assertEqual(
            [e for e in pdb.download_logs if e.source == "youtube"], [])

    def test_resolver_row_missing_distance_for_request_mbid(self) -> None:
        """If no distance entry matches request MBID → precheck failure."""
        pdb = FakePipelineDB()
        _seed_wanted_request(pdb, request_id=42, mb_release_id=MB_REL)
        # Distance points at a DIFFERENT mbid.
        _seed_resolver_row(
            pdb, distances_for_mbid="ffffffff-ffff-ffff-ffff-ffffffffffff")
        svc = _make_service(pdb)

        result = svc.submit(request_id=42, browse_id=BROWSE)
        self.assertEqual(result.outcome, "track_count_precheck_failed")

    def test_mb_mirror_returns_none(self) -> None:
        pdb = FakePipelineDB()
        _seed_wanted_request(pdb, request_id=42)
        _seed_resolver_row(pdb)
        svc = _make_service(pdb, mb_count=None)

        result = svc.submit(request_id=42, browse_id=BROWSE)
        self.assertEqual(result.outcome, "track_count_precheck_failed")

    def test_discogs_missing_stored_and_exact_count_rejects(self) -> None:
        pdb = FakePipelineDB()
        _seed_discogs_request(pdb, request_id=77)
        _seed_resolver_row(
            pdb,
            rg=DISCOGS_MASTER,
            source="discogs",
            distances_for_mbid=DISCOGS_REL,
            total_mb_tracks=None,
        )
        svc = _make_service(pdb)

        result = svc.submit(request_id=77, browse_id=BROWSE)

        self.assertEqual(result.outcome, "track_count_precheck_failed")
        self.assertIsNotNone(result.detail)
        assert result.detail is not None
        self.assertIn("no stored tracklist", result.detail)

    def test_discogs_stored_count_mismatch_rejects(self) -> None:
        pdb = FakePipelineDB()
        _seed_discogs_request(pdb, request_id=77)
        pdb.set_tracks(77, _tracks(EXPECTED_TRACKS - 1))
        _seed_resolver_row(
            pdb,
            rg=DISCOGS_MASTER,
            source="discogs",
            distances_for_mbid=DISCOGS_REL,
            total_mb_tracks=EXPECTED_TRACKS,
        )
        svc = _make_service(pdb)

        result = svc.submit(request_id=77, browse_id=BROWSE)

        self.assertEqual(result.outcome, "track_count_precheck_failed")
        self.assertIsNotNone(result.detail)
        assert result.detail is not None
        self.assertIn("stored Discogs track count", result.detail)


# ---------------------------------------------------------------------------
# submit() — transient DB failure.
# ---------------------------------------------------------------------------


class TestSubmitTransient(unittest.TestCase):
    def test_transient_db_error_during_insert(self) -> None:
        """If insert_youtube_running raises a non-InFlight exception we
        surface ``transient`` so retries are at the operator's option."""
        pdb = FakePipelineDB()
        _seed_wanted_request(pdb, request_id=42)
        _seed_resolver_row(pdb)
        svc = _make_service(pdb)

        with patch.object(
            pdb,
            "insert_youtube_running",
            side_effect=RuntimeError("lock contention"),
        ):
            result = svc.submit(request_id=42, browse_id=BROWSE)

        self.assertEqual(result.outcome, "transient")
        self.assertIsNotNone(result.detail)
        assert result.detail is not None
        self.assertIn("lock contention", result.detail)

    def test_transient_db_error_during_get_request(self) -> None:
        pdb = FakePipelineDB()
        svc = _make_service(pdb)
        with patch.object(
            pdb,
            "get_request",
            side_effect=RuntimeError("connection reset"),
        ):
            result = svc.submit(request_id=1, browse_id=BROWSE)
        self.assertEqual(result.outcome, "transient")

    def test_transient_mb_mirror_outage(self) -> None:
        pdb = FakePipelineDB()
        _seed_wanted_request(pdb, request_id=42)
        _seed_resolver_row(pdb)

        def _exploding_mb(_mbid: str) -> Optional[int]:
            raise RuntimeError("mb mirror connection refused")

        svc = YoutubeIngestService(pdb, mb_track_count_fn=_exploding_mb)
        result = svc.submit(request_id=42, browse_id=BROWSE)
        self.assertEqual(result.outcome, "transient")

    def test_in_flight_takes_precedence_over_generic_exception(self) -> None:
        """Sanity: ``YoutubeInFlightError`` must NOT be classified as transient.

        Regression guard — easy to flip ``in_flight`` into the generic
        ``except Exception`` arm during a refactor.
        """
        pdb = FakePipelineDB()
        _seed_wanted_request(pdb, request_id=42)
        _seed_resolver_row(pdb)
        svc = _make_service(pdb)

        def _explode_in_flight(**_kwargs: Any) -> int:
            raise _pipeline_db_mod.YoutubeInFlightError(42, 7777)

        with patch.object(
            pdb, "insert_youtube_running", side_effect=_explode_in_flight,
        ):
            result = svc.submit(request_id=42, browse_id=BROWSE)
        self.assertEqual(result.outcome, "in_flight")
        self.assertEqual(result.download_log_id, 7777)


# ---------------------------------------------------------------------------
# run_job — happy path (AE7).
# ---------------------------------------------------------------------------


class _RecordingStager:
    """Recorder ``stage_dir_fn``. Default behaviour: no-op (no disk IO)."""

    def __init__(self, raise_exc: Optional[BaseException] = None) -> None:
        self.calls: list[tuple[Path, Path]] = []
        self.raise_exc = raise_exc

    def __call__(self, src: Path, dest: Path) -> None:
        self.calls.append((src, dest))
        if self.raise_exc is not None:
            raise self.raise_exc


def _seed_running_row(
    pdb: FakePipelineDB,
    *,
    request_id: int = 42,
    expected_track_count: int = EXPECTED_TRACKS,
) -> int:
    """Insert a youtube_running row and return its download_log_id."""
    return pdb.insert_youtube_running(
        request_id=request_id,
        browse_id=BROWSE,
        audio_playlist_id=PLAYLIST,
        yt_url=YT_URL,
        expected_track_count=expected_track_count,
    )


class TestRunJobHappyPath(unittest.TestCase):
    """Covers AE7: yt-dlp produces N files for an N-track MBID."""

    def test_happy_path_stages_and_enqueues(self) -> None:
        pdb = FakePipelineDB()
        _seed_wanted_request(pdb, request_id=42, status="wanted")

        log_id = _seed_running_row(pdb)

        tmp_dir = Path("/tmp/yt-dlp-tempdir")
        staged_files = [
            tmp_dir / f"{i:02d}-track.opus" for i in range(EXPECTED_TRACKS)
        ]
        runner = _StubRunner(YtdlpRunResult(
            exit_code=0,
            stderr_excerpt=None,
            staged_files=staged_files,
        ))
        stager = _RecordingStager()

        svc = _make_service(
            pdb,
            ytdlp_runner_fn=runner,
            stage_dir_fn=stager,
        )
        result = svc.run_job(log_id)

        self.assertEqual(result.outcome, "youtube_success")
        self.assertIsNone(result.reason)

        # Stage was invoked exactly once with the runner's parent dir.
        self.assertEqual(len(stager.calls), 1)
        src, dest = stager.calls[0]
        self.assertEqual(src, tmp_dir)
        # Destination should be under the configured staging root.
        self.assertEqual(dest.parent, svc.staging_root)
        self.assertIn("request-42", dest.name)
        self.assertIn(f"log-{log_id}", dest.name)

        # Importer job enqueued with the right payload.
        jobs = [
            j for j in pdb.list_import_jobs(limit=50)
            if j.job_type == IMPORT_JOB_YOUTUBE
        ]
        self.assertEqual(len(jobs), 1)
        job = jobs[0]
        self.assertEqual(job.request_id, 42)
        payload = msgspec.convert(job.payload, type=YoutubeImportPayload)
        self.assertEqual(payload.request_id, 42)
        self.assertEqual(payload.browse_id, BROWSE)
        self.assertEqual(payload.download_log_id, log_id)
        self.assertEqual(payload.staged_path, str(dest))

        # Terminal log row.
        row = pdb.get_download_log_entry(log_id)
        self.assertIsNotNone(row)
        assert row is not None
        self.assertEqual(row["outcome"], "youtube_success")
        meta = row["youtube_metadata"]
        self.assertIsNotNone(meta)
        self.assertEqual(meta["observed_track_count"], EXPECTED_TRACKS)

    def test_discogs_happy_path_uses_expected_count_not_mb_mirror(self) -> None:
        import tempfile

        pdb = FakePipelineDB()
        _seed_discogs_request(
            pdb,
            request_id=77,
            status="wanted",
            mb_release_id=DISCOGS_REL,
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)
            source = tmp / "ytdlp-scratch"
            source.mkdir()
            staged_files = [
                source / "01.opus",
                source / "02.opus",
            ]
            for path in staged_files:
                path.write_bytes(b"opus")

            def _exploding_mb(_mbid: str) -> Optional[int]:
                raise AssertionError("Discogs run_job must not call MB mirror")

            _seed_resolver_row(
                pdb,
                rg=DISCOGS_MASTER,
                source="discogs",
                distances_for_mbid=DISCOGS_REL,
                total_mb_tracks=2,
            )
            pdb.set_tracks(77, _tracks(2))
            runner = _StubRunner(YtdlpRunResult(
                exit_code=0,
                stderr_excerpt=None,
                staged_files=staged_files,
                work_dir=source,
            ))
            stager = _RecordingStager()
            svc = _make_service(
                pdb,
                mb_track_count_fn=_exploding_mb,
                ytdlp_runner_fn=runner,
                stage_dir_fn=stager,
                staging_root=tmp / "staging",
            )
            submit = svc.submit(request_id=77, browse_id=BROWSE)
            self.assertEqual(submit.outcome, "accepted")
            assert submit.download_log_id is not None

            result = svc.run_job(submit.download_log_id)

            self.assertEqual(result.outcome, "youtube_success")
            self.assertEqual(len(stager.calls), 1)
            self.assertFalse(source.exists())

        jobs = [
            j for j in pdb.list_import_jobs(limit=50)
            if j.job_type == IMPORT_JOB_YOUTUBE
        ]
        self.assertEqual(len(jobs), 1)


class _StubRunner:
    """A simple callable that returns a canned YtdlpRunResult."""

    def __init__(
        self,
        result: YtdlpRunResult,
        *,
        raise_exc: Optional[BaseException] = None,
    ) -> None:
        self.result = result
        self.raise_exc = raise_exc
        self.calls: list[dict[str, Any]] = []

    def __call__(self, **kwargs: Any) -> YtdlpRunResult:
        self.calls.append(kwargs)
        if self.raise_exc is not None:
            raise self.raise_exc
        return self.result


# ---------------------------------------------------------------------------
# run_job — track-count gate (AE6 + R10).
# ---------------------------------------------------------------------------


class TestRunJobTrackCountGate(unittest.TestCase):
    def test_too_few_files_fails_with_track_count_mismatch(self) -> None:
        pdb = FakePipelineDB()
        _seed_wanted_request(pdb, request_id=42)
        log_id = _seed_running_row(pdb)

        tmp = Path("/tmp/yt-dlp-tempdir")
        # Got 9, expected 10.
        runner = _StubRunner(YtdlpRunResult(
            exit_code=0,
            stderr_excerpt=None,
            staged_files=[tmp / f"{i:02d}.opus" for i in range(EXPECTED_TRACKS - 1)],
        ))
        stager = _RecordingStager()
        svc = _make_service(
            pdb, ytdlp_runner_fn=runner, stage_dir_fn=stager)

        result = svc.run_job(log_id)

        self.assertEqual(result.outcome, "youtube_failed")
        self.assertEqual(result.reason, "track_count_mismatch")
        # Nothing staged.
        self.assertEqual(stager.calls, [])
        # No import job enqueued.
        self.assertEqual(
            [j for j in pdb.list_import_jobs(limit=50)
             if j.job_type == IMPORT_JOB_YOUTUBE],
            [],
        )
        # Terminal row carries the observed count.
        row = pdb.get_download_log_entry(log_id)
        assert row is not None
        meta = row["youtube_metadata"]
        self.assertEqual(meta["observed_track_count"], EXPECTED_TRACKS - 1)
        self.assertEqual(meta["reason"], "track_count_mismatch")

    def test_too_many_files_fails_with_track_count_mismatch(self) -> None:
        pdb = FakePipelineDB()
        _seed_wanted_request(pdb, request_id=42)
        log_id = _seed_running_row(pdb)

        tmp = Path("/tmp/yt-dlp-tempdir")
        runner = _StubRunner(YtdlpRunResult(
            exit_code=0,
            stderr_excerpt=None,
            staged_files=[tmp / f"{i:02d}.opus" for i in range(EXPECTED_TRACKS + 1)],
        ))
        stager = _RecordingStager()
        svc = _make_service(
            pdb, ytdlp_runner_fn=runner, stage_dir_fn=stager)

        result = svc.run_job(log_id)

        self.assertEqual(result.outcome, "youtube_failed")
        self.assertEqual(result.reason, "track_count_mismatch")
        self.assertEqual(stager.calls, [])
        row = pdb.get_download_log_entry(log_id)
        assert row is not None
        meta = row["youtube_metadata"]
        self.assertEqual(meta["observed_track_count"], EXPECTED_TRACKS + 1)


# ---------------------------------------------------------------------------
# run_job — yt-dlp non-zero exit (R20 / F4).
# ---------------------------------------------------------------------------


class TestRunJobYtdlpFailures(unittest.TestCase):
    def test_404_classified_correctly(self) -> None:
        pdb = FakePipelineDB()
        _seed_wanted_request(pdb, request_id=42)
        log_id = _seed_running_row(pdb)

        runner = _StubRunner(YtdlpRunResult(
            exit_code=1,
            stderr_excerpt="ERROR: HTTP Error 404: Not Found",
            staged_files=[],
        ))
        svc = _make_service(pdb, ytdlp_runner_fn=runner)

        result = svc.run_job(log_id)
        self.assertEqual(result.outcome, "youtube_failed")
        self.assertEqual(result.reason, "youtube_404")
        row = pdb.get_download_log_entry(log_id)
        assert row is not None
        meta = row["youtube_metadata"]
        self.assertEqual(meta["reason"], "youtube_404")
        self.assertEqual(
            meta["stderr_excerpt"], "ERROR: HTTP Error 404: Not Found")

    def test_ytdlp_failure_deletes_work_dir(self) -> None:
        import tempfile

        pdb = FakePipelineDB()
        _seed_wanted_request(pdb, request_id=42)
        log_id = _seed_running_row(pdb)

        with tempfile.TemporaryDirectory() as tmpdir:
            work_dir = Path(tmpdir) / "scratch"
            work_dir.mkdir()
            (work_dir / "partial.opus").write_bytes(b"partial")
            runner = _StubRunner(YtdlpRunResult(
                exit_code=1,
                stderr_excerpt="ERROR: HTTP Error 404: Not Found",
                staged_files=[],
                work_dir=work_dir,
            ))
            svc = _make_service(pdb, ytdlp_runner_fn=runner)

            result = svc.run_job(log_id)

            self.assertEqual(result.outcome, "youtube_failed")
            self.assertFalse(work_dir.exists())

    def test_cleanup_failure_is_persisted_in_metadata(self) -> None:
        import tempfile

        pdb = FakePipelineDB()
        _seed_wanted_request(pdb, request_id=42)
        log_id = _seed_running_row(pdb)

        with tempfile.TemporaryDirectory() as tmpdir:
            work_dir = Path(tmpdir) / "scratch"
            work_dir.mkdir()
            runner = _StubRunner(YtdlpRunResult(
                exit_code=1,
                stderr_excerpt="ERROR: HTTP Error 404: Not Found",
                staged_files=[],
                work_dir=work_dir,
            ))
            svc = _make_service(pdb, ytdlp_runner_fn=runner)

            with patch(
                "lib.youtube_ingest_service.shutil.rmtree",
                side_effect=OSError("permission denied"),
            ):
                result = svc.run_job(log_id)

            self.assertEqual(result.outcome, "youtube_failed")
            row = pdb.get_download_log_entry(log_id)
            assert row is not None
            meta = row["youtube_metadata"]
            self.assertIn("cleanup_error", meta)
            self.assertIn("permission denied", meta["cleanup_error"])

    def test_age_gated_classified_correctly(self) -> None:
        pdb = FakePipelineDB()
        _seed_wanted_request(pdb, request_id=42)
        log_id = _seed_running_row(pdb)

        runner = _StubRunner(YtdlpRunResult(
            exit_code=1,
            stderr_excerpt=(
                "ERROR: Sign in to confirm your age. Use --cookies or "
                "--cookies-from-browser to provide age verification."
            ),
            staged_files=[],
        ))
        svc = _make_service(pdb, ytdlp_runner_fn=runner)
        result = svc.run_job(log_id)
        self.assertEqual(result.outcome, "youtube_failed")
        self.assertEqual(result.reason, "youtube_age_gated")

    def test_region_locked_classified_correctly(self) -> None:
        pdb = FakePipelineDB()
        _seed_wanted_request(pdb, request_id=42)
        log_id = _seed_running_row(pdb)

        runner = _StubRunner(YtdlpRunResult(
            exit_code=1,
            stderr_excerpt=(
                "ERROR: This video is not available in your country."),
            staged_files=[],
        ))
        svc = _make_service(pdb, ytdlp_runner_fn=runner)
        result = svc.run_job(log_id)
        self.assertEqual(result.reason, "youtube_region_locked")

    def test_video_removed_classified_correctly(self) -> None:
        pdb = FakePipelineDB()
        _seed_wanted_request(pdb, request_id=42)
        log_id = _seed_running_row(pdb)

        runner = _StubRunner(YtdlpRunResult(
            exit_code=1,
            stderr_excerpt="ERROR: This video has been removed by the uploader.",
            staged_files=[],
        ))
        svc = _make_service(pdb, ytdlp_runner_fn=runner)
        result = svc.run_job(log_id)
        self.assertEqual(result.reason, "youtube_video_removed")

    def test_429_transient_network_classified_correctly(self) -> None:
        pdb = FakePipelineDB()
        _seed_wanted_request(pdb, request_id=42)
        log_id = _seed_running_row(pdb)

        runner = _StubRunner(YtdlpRunResult(
            exit_code=1,
            stderr_excerpt="ERROR: HTTP Error 429: Too Many Requests",
            staged_files=[],
        ))
        svc = _make_service(pdb, ytdlp_runner_fn=runner)
        result = svc.run_job(log_id)
        self.assertEqual(result.reason, "youtube_transient_network")

    def test_unknown_classification_falls_back(self) -> None:
        pdb = FakePipelineDB()
        _seed_wanted_request(pdb, request_id=42)
        log_id = _seed_running_row(pdb)

        runner = _StubRunner(YtdlpRunResult(
            exit_code=1,
            stderr_excerpt="ERROR: Unrecognized yt-dlp failure mode XYZ",
            staged_files=[],
        ))
        svc = _make_service(pdb, ytdlp_runner_fn=runner)
        result = svc.run_job(log_id)
        self.assertEqual(result.reason, "youtube_unknown")

    def test_classify_youtube_failure_pure_function(self) -> None:
        """Spot-check the pure classifier without going through run_job."""
        self.assertEqual(
            classify_youtube_failure(None), "youtube_unknown")
        self.assertEqual(
            classify_youtube_failure(""), "youtube_unknown")
        self.assertEqual(
            classify_youtube_failure("HTTP Error 404"), "youtube_404")
        self.assertEqual(
            classify_youtube_failure("Sign in to confirm your age"),
            "youtube_age_gated")


# ---------------------------------------------------------------------------
# run_job — KTD8: UTF-8 surrogates in yt-dlp stderr.
# ---------------------------------------------------------------------------


class TestRunJobUtf8SurrogateHandling(unittest.TestCase):
    """KTD8 / docs/solutions/subprocess-text-mode-utf8-strict-decode-crash.md.

    The U6 worker is responsible for invoking yt-dlp with
    ``text=True, errors='replace'`` so non-UTF-8 stderr decodes to
    replacement characters rather than crashing. The service's
    contract is: given an already-decoded stderr_excerpt (with the
    replacement chars present), classify + persist without crashing.
    """

    def test_surrogate_replacement_char_in_stderr_does_not_crash(self) -> None:
        pdb = FakePipelineDB()
        _seed_wanted_request(pdb, request_id=42)
        log_id = _seed_running_row(pdb)

        # The replacement character (U+FFFD) is what ``errors='replace'``
        # produces for an undecodable byte like 0xE2 alone. The service
        # must persist this verbatim without UnicodeError.
        excerpt = "ERROR: weird title � more text"
        runner = _StubRunner(YtdlpRunResult(
            exit_code=1,
            stderr_excerpt=excerpt,
            staged_files=[],
        ))
        svc = _make_service(pdb, ytdlp_runner_fn=runner)

        result = svc.run_job(log_id)

        self.assertEqual(result.outcome, "youtube_failed")
        row = pdb.get_download_log_entry(log_id)
        assert row is not None
        meta = row["youtube_metadata"]
        # The excerpt round-tripped through the fake DB cleanly.
        self.assertEqual(meta["stderr_excerpt"], excerpt)


# ---------------------------------------------------------------------------
# run_job — unhandled exception in runner.
# ---------------------------------------------------------------------------


class TestRunJobRunnerUnhandled(unittest.TestCase):
    def test_imported_request_fails_before_ytdlp(self) -> None:
        pdb = FakePipelineDB()
        _seed_wanted_request(pdb, request_id=42, status="imported")
        log_id = _seed_running_row(pdb)
        runner = _StubRunner(YtdlpRunResult(
            exit_code=0,
            stderr_excerpt=None,
            staged_files=[Path("/tmp/yt/01.opus")],
        ))
        svc = _make_service(pdb, ytdlp_runner_fn=runner)

        result = svc.run_job(log_id)

        self.assertEqual(result.outcome, "youtube_failed")
        self.assertEqual(result.reason, "request_no_longer_rescuable")
        self.assertEqual(runner.calls, [])

    def test_status_change_after_ytdlp_cleans_scratch_without_staging(self) -> None:
        import tempfile

        pdb = FakePipelineDB()
        _seed_wanted_request(pdb, request_id=42, status="wanted")
        log_id = _seed_running_row(pdb)
        stager = _RecordingStager()

        with tempfile.TemporaryDirectory() as tmpdir:
            work_dir = Path(tmpdir) / "scratch"
            work_dir.mkdir()
            staged_files = [
                work_dir / f"{i:02d}.opus" for i in range(EXPECTED_TRACKS)
            ]
            for file_path in staged_files:
                file_path.write_bytes(b"opus")

            def _runner(**_kwargs: Any) -> YtdlpRunResult:
                pdb.mark_imported_with_rescue(
                    42,
                    expected_status="wanted",
                )
                return YtdlpRunResult(
                    exit_code=0,
                    stderr_excerpt=None,
                    staged_files=staged_files,
                    work_dir=work_dir,
                )

            svc = _make_service(
                pdb,
                ytdlp_runner_fn=_runner,
                stage_dir_fn=stager,
            )
            result = svc.run_job(log_id)

            self.assertEqual(result.outcome, "youtube_failed")
            self.assertEqual(result.reason, "request_no_longer_rescuable")
            self.assertFalse(work_dir.exists())
            self.assertEqual(stager.calls, [])

    def test_runner_raises_classifies_as_worker_unhandled_exception(self) -> None:
        pdb = FakePipelineDB()
        _seed_wanted_request(pdb, request_id=42)
        log_id = _seed_running_row(pdb)

        runner = _StubRunner(
            YtdlpRunResult(exit_code=0, stderr_excerpt=None, staged_files=[]),
            raise_exc=RuntimeError("kaboom"),
        )
        svc = _make_service(pdb, ytdlp_runner_fn=runner)
        result = svc.run_job(log_id)
        self.assertEqual(result.outcome, "youtube_failed")
        self.assertEqual(result.reason, "worker_unhandled_exception")

    def test_staging_failure_classifies_as_staging_io_error(self) -> None:
        pdb = FakePipelineDB()
        _seed_wanted_request(pdb, request_id=42)
        log_id = _seed_running_row(pdb)

        tmp = Path("/tmp/yt-dlp-tempdir")
        runner = _StubRunner(YtdlpRunResult(
            exit_code=0,
            stderr_excerpt=None,
            staged_files=[
                tmp / f"{i:02d}.opus" for i in range(EXPECTED_TRACKS)
            ],
        ))
        stager = _RecordingStager(raise_exc=OSError("disk full"))
        svc = _make_service(
            pdb, ytdlp_runner_fn=runner, stage_dir_fn=stager)
        result = svc.run_job(log_id)
        self.assertEqual(result.outcome, "youtube_failed")
        self.assertEqual(result.reason, "staging_io_error")
        # No import job got enqueued.
        self.assertEqual(
            [j for j in pdb.list_import_jobs(limit=50)
             if j.job_type == IMPORT_JOB_YOUTUBE],
            [],
        )

    def test_enqueue_failure_deletes_staged_directory(self) -> None:
        import tempfile

        pdb = FakePipelineDB()
        _seed_wanted_request(pdb, request_id=42)
        log_id = _seed_running_row(pdb)

        with tempfile.TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)
            source = tmp / "source"
            source.mkdir()
            staged_file = source / "01.opus"
            staged_file.write_bytes(b"opus")
            staging_root = tmp / "staging" / "auto-import"
            moved_to: list[Path] = []

            def _move(src: Path, dest: Path) -> None:
                dest.parent.mkdir(parents=True, exist_ok=True)
                moved_to.append(dest)
                source.rename(dest)

            runner = _StubRunner(YtdlpRunResult(
                exit_code=0,
                stderr_excerpt=None,
                staged_files=[staged_file],
                work_dir=source,
            ))
            svc = _make_service(
                pdb,
                mb_count=1,
                ytdlp_runner_fn=runner,
                stage_dir_fn=_move,
                staging_root=staging_root,
            )

            with patch.object(
                pdb,
                "enqueue_youtube_import_and_mark_success",
                side_effect=RuntimeError("db down"),
            ):
                result = svc.run_job(log_id)

            self.assertEqual(result.outcome, "youtube_failed")
            self.assertEqual(result.reason, "import_enqueue_failed")
            row = pdb.get_download_log_entry(log_id)
            assert row is not None
            meta = row["youtube_metadata"]
            self.assertNotIn("quarantine_path", meta)
            self.assertNotIn("quarantine_error", meta)
            self.assertNotIn("cleanup_error", meta)
            self.assertEqual(len(moved_to), 1)
            self.assertFalse(moved_to[0].exists())

    def test_failed_terminal_write_is_visible(self) -> None:
        pdb = FakePipelineDB()
        _seed_wanted_request(pdb, request_id=42)
        log_id = _seed_running_row(pdb)
        runner = _StubRunner(YtdlpRunResult(
            exit_code=1,
            stderr_excerpt="ERROR: HTTP Error 404: Not Found",
            staged_files=[],
        ))
        svc = _make_service(pdb, ytdlp_runner_fn=runner)

        with patch.object(
            pdb,
            "update_youtube_terminal",
            side_effect=RuntimeError("audit write failed"),
        ):
            with self.assertRaises(RuntimeError):
                svc.run_job(log_id)


# ---------------------------------------------------------------------------
# run_job — malformed / missing input edge cases.
# ---------------------------------------------------------------------------


class TestRunJobMalformedInput(unittest.TestCase):
    def test_missing_download_log_row(self) -> None:
        pdb = FakePipelineDB()
        svc = _make_service(pdb)
        result = svc.run_job(99999)
        # Service can't write a terminal row when the row doesn't exist,
        # but it should still return a typed failure outcome.
        self.assertEqual(result.outcome, "youtube_failed")
        self.assertEqual(result.reason, "missing_download_log_row")

    def test_missing_request_row(self) -> None:
        pdb = FakePipelineDB()
        # log row points at request that doesn't exist.
        log_id = pdb.insert_youtube_running(
            request_id=42,
            browse_id=BROWSE,
            audio_playlist_id=PLAYLIST,
            yt_url=YT_URL,
            expected_track_count=EXPECTED_TRACKS,
        )
        svc = _make_service(pdb)
        result = svc.run_job(log_id)
        self.assertEqual(result.outcome, "youtube_failed")
        self.assertEqual(result.reason, "missing_request_row")
        row = pdb.get_download_log_entry(log_id)
        assert row is not None
        self.assertEqual(row["outcome"], "youtube_failed")


# ---------------------------------------------------------------------------
# YoutubeIngestMetadata wire-boundary tests.
# ---------------------------------------------------------------------------


class TestYoutubeIngestMetadataStruct(unittest.TestCase):
    def test_submission_blob_round_trips(self) -> None:
        m = YoutubeIngestMetadata(
            yt_url=YT_URL,
            browse_id=BROWSE,
            audio_playlist_id=PLAYLIST,
            expected_track_count=EXPECTED_TRACKS,
        )
        as_dict = msgspec.to_builtins(m)
        back = msgspec.convert(as_dict, type=YoutubeIngestMetadata)
        self.assertEqual(back, m)

    def test_terminal_blob_round_trips(self) -> None:
        m = YoutubeIngestMetadata(
            yt_url=YT_URL,
            browse_id=BROWSE,
            audio_playlist_id=PLAYLIST,
            expected_track_count=EXPECTED_TRACKS,
            reason="youtube_404",
            stderr_excerpt="ERROR: HTTP Error 404",
            observed_track_count=0,
        )
        as_dict = msgspec.to_builtins(m)
        back = msgspec.convert(as_dict, type=YoutubeIngestMetadata)
        self.assertEqual(back, m)

    def test_extra_keys_decoded_cleanly(self) -> None:
        """Production rows may carry pre-deploy extra keys; ``msgspec.convert``
        should ignore extras (forward compat)."""
        wire = {
            "yt_url": YT_URL,
            "browse_id": BROWSE,
            "audio_playlist_id": PLAYLIST,
            "extra_unknown_field": "value",
        }
        m = msgspec.convert(wire, type=YoutubeIngestMetadata)
        self.assertEqual(m.yt_url, YT_URL)


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
