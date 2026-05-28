"""Tests for ``scripts.youtube_ingest_worker``.

Coverage matrix mirrors plan U6:

* Startup orphan sweep (R22) — claimed ``youtube_running`` rows at startup
  are transitioned to ``youtube_failed`` with ``reason=worker_interrupted`` while
  accepted-but-unclaimed rows stay drainable.
* Drain loop happy path — one ``youtube_running`` row → ``run_job``
  succeeds → terminal ``youtube_success`` + a ``youtube_import`` job in
  ``import_jobs`` carrying ``staged_path``.
* Drain loop empty queue — ``time.sleep(poll_interval)`` is called.
* Stderr cap (KTD8 size bound) — 100 KiB stderr → 4 KiB excerpt.
* ``--`` separator argv shape — regression guard against future
  implementations that drop the separator.
* UTF-8 surrogate in yt-dlp stderr — shim binary emits raw ``0xE2``;
  ``_run_ytdlp`` survives without ``UnicodeDecodeError``.
* Unhandled exception in ``service.run_job`` — the worker catches it,
  writes ``youtube_failed`` reason=``worker_unhandled_exception`` and
  continues.
* Signal handling — ``KeyboardInterrupt`` during the loop returns
  exit 0 (clean shutdown).
* Advisory-lock contention — second worker startup with the lock held
  returns exit 0 immediately and does NOT sweep / loop.
"""

from __future__ import annotations

import os
import subprocess
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import patch

import msgspec

from lib.import_queue import IMPORT_JOB_YOUTUBE
from lib.pipeline_db import ADVISORY_LOCK_NAMESPACE_YOUTUBE_INGEST
from lib.youtube_ingest_service import (
    YoutubeImportPayload,
    YoutubeIngestService,
    YtdlpRunResult,
)
from scripts import youtube_ingest_worker as worker
from tests.fakes import FakePipelineDB
from tests.helpers import make_request_row


BROWSE = "MPREb_test_browse"
YT_URL = "https://music.youtube.com/playlist?list=OLAK5uy-test"
PLAYLIST = "OLAK5uy-test"
MB_REL = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
MB_RG = "11111111-1111-1111-1111-111111111111"
EXPECTED_TRACKS = 10


# ---------------------------------------------------------------------------
# Fixtures.
# ---------------------------------------------------------------------------


def _seed_wanted_request(pdb: FakePipelineDB, *, request_id: int = 42) -> None:
    pdb.seed_request(make_request_row(
        id=request_id,
        status="wanted",
        mb_release_id=MB_REL,
        mb_release_group_id=MB_RG,
    ))


def _seed_running_row(
    pdb: FakePipelineDB,
    *,
    request_id: int = 42,
    expected_track_count: int = EXPECTED_TRACKS,
) -> int:
    return pdb.insert_youtube_running(
        request_id=request_id,
        browse_id=BROWSE,
        audio_playlist_id=PLAYLIST,
        yt_url=YT_URL,
        expected_track_count=expected_track_count,
    )


def _service_with_fake_runner(
    pdb: FakePipelineDB,
    runner_result: YtdlpRunResult,
    *,
    staging_root: Path | None = None,
) -> YoutubeIngestService:
    """Build a YoutubeIngestService whose runner returns a canned result."""
    calls: list[dict[str, Any]] = []

    def _fake_runner(**kwargs: Any) -> YtdlpRunResult:
        calls.append(kwargs)
        return runner_result

    svc = YoutubeIngestService(
        pdb,
        ytdlp_runner_fn=_fake_runner,
        mb_track_count_fn=lambda _mbid: EXPECTED_TRACKS,
        stage_dir_fn=lambda src, dest: None,  # no-op: no disk IO in tests
        staging_root=staging_root or Path("/tmp/yt-test-staging"),
    )
    # Stash for assertion access.
    svc._test_runner_calls = calls  # type: ignore[attr-defined]
    return svc


# ---------------------------------------------------------------------------
# Startup orphan sweep (R22).
# ---------------------------------------------------------------------------


class TestSweepOrphanRunningRows(unittest.TestCase):
    """Only claimed ``youtube_running`` rows at startup are marked failed."""

    def test_sweeps_all_orphans(self) -> None:
        pdb = FakePipelineDB()
        _seed_wanted_request(pdb, request_id=42)
        _seed_wanted_request(pdb, request_id=43)
        log_id_1 = _seed_running_row(pdb, request_id=42)
        log_id_2 = _seed_running_row(pdb, request_id=43)
        pdb.claim_next_youtube_pending(worker_id="worker-a", limit=2)

        swept = worker.sweep_orphan_running_rows(pdb)

        self.assertEqual(sorted(swept), sorted([log_id_1, log_id_2]))

        # Both rows are now terminal with reason=worker_interrupted.
        for lid in (log_id_1, log_id_2):
            row = pdb.get_download_log_entry(lid)
            assert row is not None
            self.assertEqual(row["outcome"], "youtube_failed")
            meta = row["youtube_metadata"]
            assert meta is not None
            self.assertEqual(meta["reason"], "worker_interrupted")

    def test_accepted_unclaimed_rows_survive_startup_sweep(self) -> None:
        pdb = FakePipelineDB()
        _seed_wanted_request(pdb, request_id=42)
        log_id = _seed_running_row(pdb, request_id=42)

        swept = worker.sweep_orphan_running_rows(pdb)

        self.assertEqual(swept, [])
        row = pdb.get_download_log_entry(log_id)
        assert row is not None
        self.assertEqual(row["outcome"], "youtube_running")
        # The survivor is still drainable by the worker's claim path.
        self.assertEqual(
            [r["id"] for r in pdb.claim_next_youtube_pending(worker_id="w")],
            [log_id])

    def test_empty_queue_sweeps_nothing(self) -> None:
        pdb = FakePipelineDB()
        swept = worker.sweep_orphan_running_rows(pdb)
        self.assertEqual(swept, [])

    def test_terminal_rows_are_not_swept(self) -> None:
        """``youtube_success`` / ``youtube_failed`` rows are immune."""
        pdb = FakePipelineDB()
        _seed_wanted_request(pdb, request_id=42)
        log_id = _seed_running_row(pdb, request_id=42)
        pdb.update_youtube_terminal(
            log_id, "youtube_success", {"observed_track_count": 10})

        swept = worker.sweep_orphan_running_rows(pdb)
        self.assertEqual(swept, [])

    def test_sweep_cleans_derived_scratch_and_staging_paths(self) -> None:
        pdb = FakePipelineDB()
        _seed_wanted_request(pdb, request_id=42)
        log_id = _seed_running_row(pdb, request_id=42)
        pdb.claim_next_youtube_pending(worker_id="worker-a", limit=1)

        with tempfile.TemporaryDirectory() as tmpdir:
            temp_root = Path(tmpdir) / "scratch"
            staging_root = Path(tmpdir) / "staging"
            scratch = temp_root / f"ytdlp-req42-{BROWSE}-abc"
            staged = (
                staging_root
                / f"Test_Artist-Test_Album-{BROWSE}-request-42-log-{log_id}"
            )
            scratch.mkdir(parents=True)
            staged.mkdir(parents=True)
            (scratch / "partial.opus").write_bytes(b"partial")
            (staged / "01.opus").write_bytes(b"opus")

            swept = worker.sweep_orphan_running_rows(
                pdb,
                temp_dir=temp_root,
                staging_root=staging_root,
            )

            self.assertEqual(swept, [log_id])
            self.assertFalse(scratch.exists())
            self.assertFalse(staged.exists())
            row = pdb.get_download_log_entry(log_id)
            assert row is not None
            meta = row["youtube_metadata"]
            self.assertEqual(meta["reason"], "worker_interrupted")
            self.assertNotIn("cleanup_error", meta)


# ---------------------------------------------------------------------------
# Drain loop (drain_one + run_loop).
# ---------------------------------------------------------------------------


class TestDrainLoopHappyPath(unittest.TestCase):
    """One ``youtube_running`` row → success → ``youtube_import`` enqueued."""

    def test_drains_pending_row_to_terminal_success(self) -> None:
        pdb = FakePipelineDB()
        _seed_wanted_request(pdb, request_id=42)
        log_id = _seed_running_row(pdb)

        tmp = Path("/tmp/ytdlp-tempdir")
        runner_result = YtdlpRunResult(
            exit_code=0,
            stderr_excerpt=None,
            staged_files=[
                tmp / f"{i:02d}-track.opus" for i in range(EXPECTED_TRACKS)
            ],
        )
        service = _service_with_fake_runner(pdb, runner_result)

        processed = worker.drain_one(pdb, service)
        self.assertEqual(processed, log_id)

        row = pdb.get_download_log_entry(log_id)
        assert row is not None
        self.assertEqual(row["outcome"], "youtube_success")
        meta = row["youtube_metadata"]
        assert meta is not None
        self.assertIsNotNone(meta.get("worker_claimed_at"))

        jobs = [
            j for j in pdb.list_import_jobs(limit=50)
            if j.job_type == IMPORT_JOB_YOUTUBE
        ]
        self.assertEqual(len(jobs), 1)
        payload = msgspec.convert(jobs[0].payload, type=YoutubeImportPayload)
        self.assertEqual(payload.request_id, 42)
        self.assertEqual(payload.browse_id, BROWSE)
        self.assertTrue(payload.staged_path)

    def test_empty_queue_returns_none(self) -> None:
        pdb = FakePipelineDB()
        # Build a real (production-shape) service — drain shouldn't call
        # any of its methods because the queue is empty.
        service = _service_with_fake_runner(
            pdb,
            YtdlpRunResult(exit_code=0, stderr_excerpt=None, staged_files=[]),
        )
        processed = worker.drain_one(pdb, service)
        self.assertIsNone(processed)


class TestRunLoopBackoff(unittest.TestCase):
    """When the queue is empty, the loop sleeps poll_interval."""

    def test_empty_queue_sleeps_then_returns_when_iterations_capped(
        self,
    ) -> None:
        pdb = FakePipelineDB()
        service = _service_with_fake_runner(
            pdb,
            YtdlpRunResult(exit_code=0, stderr_excerpt=None, staged_files=[]),
        )
        sleeps: list[float] = []
        worker.run_loop(
            pdb,
            service=service,
            poll_interval=2.5,
            sleep_fn=sleeps.append,
            iterations=3,
        )
        # Loop shape: drain → check exit → sleep if idle. With
        # iterations=3, iteration N=3 returns BEFORE the sleep, so we
        # observe sleeps after iters 1 and 2 only.
        self.assertEqual(sleeps, [2.5, 2.5])

    def test_once_returns_after_first_iteration_without_sleep(self) -> None:
        pdb = FakePipelineDB()
        service = _service_with_fake_runner(
            pdb,
            YtdlpRunResult(exit_code=0, stderr_excerpt=None, staged_files=[]),
        )
        sleeps: list[float] = []
        rc = worker.run_loop(
            pdb,
            service=service,
            poll_interval=99.0,
            once=True,
            sleep_fn=sleeps.append,
        )
        self.assertEqual(rc, 0)
        # --once short-circuits BEFORE sleep.
        self.assertEqual(sleeps, [])


# ---------------------------------------------------------------------------
# Unhandled exception inside the loop.
# ---------------------------------------------------------------------------


class TestDrainUnhandledException(unittest.TestCase):
    """A crash in ``run_job`` does not kill the worker."""

    def test_unhandled_exception_writes_terminal_row_and_continues(
        self,
    ) -> None:
        pdb = FakePipelineDB()
        _seed_wanted_request(pdb, request_id=42)
        log_id = _seed_running_row(pdb)

        class _ExplodingService:
            def run_job(self, _download_log_id: int) -> Any:
                raise RuntimeError("simulated crash inside run_job")

        # drain_one catches the bare exception and writes a terminal row.
        worker.drain_one(pdb, _ExplodingService())  # type: ignore[arg-type]

        row = pdb.get_download_log_entry(log_id)
        assert row is not None
        self.assertEqual(row["outcome"], "youtube_failed")
        meta = row["youtube_metadata"]
        assert meta is not None
        self.assertEqual(meta["reason"], "worker_unhandled_exception")
        # The traceback excerpt is present and bounded.
        self.assertIsNotNone(meta.get("stderr_excerpt"))
        self.assertLessEqual(
            len(meta["stderr_excerpt"]),
            worker.STDERR_EXCERPT_BYTES_LIMIT)


# ---------------------------------------------------------------------------
# _run_ytdlp argv shape (regression guard).
# ---------------------------------------------------------------------------


class TestYtdlpArgvShape(unittest.TestCase):
    """Defense-in-depth: ``--`` separator before the URL positional."""

    def test_separator_immediately_before_url(self) -> None:
        argv = worker._build_ytdlp_argv(
            ytdlp_bin="/usr/bin/yt-dlp",
            url="https://music.youtube.com/playlist?list=FAKE",
            output_template="/tmp/out/%(title)s.%(ext)s",
        )
        # The URL must appear at the end, prefixed by '--'.
        self.assertEqual(argv[-2], "--")
        self.assertEqual(
            argv[-1], "https://music.youtube.com/playlist?list=FAKE")
        # Worker invocation is isolated from ambient yt-dlp config/cookies.
        self.assertIn("--ignore-config", argv)
        # ``--no-ignore-errors`` is set (R9).
        self.assertIn("--no-ignore-errors", argv)
        # ``--max-downloads`` aborts with a nonzero exit as soon as the Nth
        # file completes; rely on the post-download R10 count gate instead.
        self.assertNotIn("--max-downloads", argv)
        # bestaudio format.
        idx = argv.index("-f")
        self.assertEqual(argv[idx + 1], "bestaudio")

    def test_remux_video_webm_to_opus(self) -> None:
        # YouTube Music bestaudio is opus-in-webm (.webm) / aac-in-mp4,
        # neither of which the importer's audio-extension set recognizes
        # (it sees them as empty_fileset). --remux-video is stream-copy only
        # (fails rather than re-encoding), so webm(opus)->.opus and
        # mp4(aac)->.m4a are lossless container changes the importer accepts.
        argv = worker._build_ytdlp_argv(
            ytdlp_bin="/usr/bin/yt-dlp",
            url="https://music.youtube.com/playlist?list=FAKE",
            output_template="/tmp/out/%(title)s.%(ext)s",
        )
        self.assertIn("--remux-video", argv)
        self.assertEqual(
            argv[argv.index("--remux-video") + 1], "webm>opus/mp4>m4a")
        # Still a postprocessor flag, so it precedes the '--' separator.
        self.assertLess(argv.index("--remux-video"), argv.index("--"))

    def test_source_address_absent_by_default(self) -> None:
        # When no source address is configured the argv is unchanged — the
        # worker egresses on the host's default route (no VPN binding).
        argv = worker._build_ytdlp_argv(
            ytdlp_bin="/usr/bin/yt-dlp",
            url="https://music.youtube.com/playlist?list=FAKE",
            output_template="/tmp/out/%(title)s.%(ext)s",
        )
        self.assertNotIn("--source-address", argv)

    def test_source_address_injected_when_set(self) -> None:
        # Binding yt-dlp's client socket to the VPN-routed NIC IP makes its
        # egress match the host's source-IP policy-routing rule (ens19 →
        # pfSense WireGuard). The flag must precede the '--' separator.
        argv = worker._build_ytdlp_argv(
            ytdlp_bin="/usr/bin/yt-dlp",
            url="https://music.youtube.com/playlist?list=FAKE",
            output_template="/tmp/out/%(title)s.%(ext)s",
            source_address="192.168.1.36",
        )
        self.assertIn("--source-address", argv)
        idx = argv.index("--source-address")
        self.assertEqual(argv[idx + 1], "192.168.1.36")
        self.assertLess(idx, argv.index("--"))
        # The URL positional is still last, still '--'-separated.
        self.assertEqual(argv[-2], "--")
        self.assertEqual(
            argv[-1], "https://music.youtube.com/playlist?list=FAKE")


# ---------------------------------------------------------------------------
# _run_ytdlp via _subprocess_run kwarg-DI seam.
# ---------------------------------------------------------------------------


class TestRunYtdlpStderrCap(unittest.TestCase):
    """KTD8 size bound: stderr capped at 4 KiB."""

    def test_huge_stderr_is_capped(self) -> None:
        # 100 KiB of stderr.
        huge_stderr = "x" * (100 * 1024)

        def _fake_run(*_args: Any, **_kwargs: Any) -> Any:
            return subprocess.CompletedProcess(
                args=[], returncode=1, stdout="", stderr=huge_stderr)

        with tempfile.TemporaryDirectory() as tmp:
            result = worker._run_ytdlp(
                url=YT_URL,
                expected_track_count=10,
                temp_root=Path(tmp),
                subprocess_run=_fake_run,
                ytdlp_bin_resolver=lambda: "/usr/bin/yt-dlp",
            )
            self.assertIsNotNone(result.work_dir)
            assert result.work_dir is not None
            self.assertTrue(result.work_dir.is_dir())

        self.assertEqual(result.exit_code, 1)
        assert result.stderr_excerpt is not None
        self.assertLessEqual(
            len(result.stderr_excerpt), worker.STDERR_EXCERPT_BYTES_LIMIT)

    def test_real_process_huge_stderr_is_tail_capped(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            shim = Path(tmpdir) / "yt-dlp"
            with open(shim, "w") as fp:
                fp.write(
                    "#!/bin/sh\n"
                    "python3 - <<'PY'\n"
                    "import sys\n"
                    "sys.stderr.buffer.write(b'x' * (100 * 1024))\n"
                    "sys.stderr.buffer.write(b'TAIL-MARKER')\n"
                    "sys.exit(1)\n"
                    "PY\n"
                )
            os.chmod(shim, 0o755)

            result = worker._run_ytdlp(
                url=YT_URL,
                expected_track_count=1,
                temp_root=Path(tmpdir) / "scratch",
                ytdlp_bin_resolver=lambda: str(shim),
            )

        self.assertEqual(result.exit_code, 1)
        assert result.stderr_excerpt is not None
        self.assertLessEqual(
            len(result.stderr_excerpt), worker.STDERR_EXCERPT_BYTES_LIMIT)
        self.assertIn("TAIL-MARKER", result.stderr_excerpt)

    def test_stdout_is_discarded_and_stderr_is_piped(self) -> None:
        seen_kwargs: dict[str, Any] = {}

        def _fake_run(*_args: Any, **kwargs: Any) -> Any:
            seen_kwargs.update(kwargs)
            return subprocess.CompletedProcess(
                args=[], returncode=0, stdout=None, stderr="")

        with tempfile.TemporaryDirectory() as tmp:
            worker._run_ytdlp(
                url=YT_URL,
                expected_track_count=1,
                temp_root=Path(tmp),
                subprocess_run=_fake_run,
                ytdlp_bin_resolver=lambda: "/usr/bin/yt-dlp",
            )

        self.assertIs(seen_kwargs["stdout"], subprocess.DEVNULL)
        self.assertEqual(seen_kwargs["stderr"], subprocess.PIPE)
        self.assertNotIn("capture_output", seen_kwargs)

    def test_short_stderr_not_truncated(self) -> None:
        def _fake_run(*_args: Any, **_kwargs: Any) -> Any:
            return subprocess.CompletedProcess(
                args=[], returncode=0, stdout="", stderr="hi")

        with tempfile.TemporaryDirectory() as tmp:
            result = worker._run_ytdlp(
                url=YT_URL,
                expected_track_count=1,
                temp_root=Path(tmp),
                subprocess_run=_fake_run,
                ytdlp_bin_resolver=lambda: "/usr/bin/yt-dlp",
            )
        self.assertEqual(result.stderr_excerpt, "hi")

    def test_empty_stderr_returns_none(self) -> None:
        def _fake_run(*_args: Any, **_kwargs: Any) -> Any:
            return subprocess.CompletedProcess(
                args=[], returncode=0, stdout="", stderr="")

        with tempfile.TemporaryDirectory() as tmp:
            result = worker._run_ytdlp(
                url=YT_URL,
                expected_track_count=1,
                temp_root=Path(tmp),
                subprocess_run=_fake_run,
                ytdlp_bin_resolver=lambda: "/usr/bin/yt-dlp",
            )
        # Empty stderr → None (so the service's classifier short-circuits).
        self.assertIsNone(result.stderr_excerpt)


class TestRunYtdlpRejectsBadInput(unittest.TestCase):
    def test_zero_expected_track_count_rejected(self) -> None:
        with self.assertRaises(ValueError):
            worker._run_ytdlp(
                url=YT_URL,
                expected_track_count=0,
                subprocess_run=lambda *a, **kw: subprocess.CompletedProcess(
                    args=[], returncode=0, stdout="", stderr=""),
                ytdlp_bin_resolver=lambda: "/usr/bin/yt-dlp",
            )

    def test_none_expected_track_count_rejected(self) -> None:
        with self.assertRaises(ValueError):
            worker._run_ytdlp(
                url=YT_URL,
                expected_track_count=None,
                subprocess_run=lambda *a, **kw: subprocess.CompletedProcess(
                    args=[], returncode=0, stdout="", stderr=""),
                ytdlp_bin_resolver=lambda: "/usr/bin/yt-dlp",
            )


class TestRunYtdlpTimeout(unittest.TestCase):
    """TimeoutExpired is surfaced as a yt-dlp failure, not a crash."""

    def test_timeout_returns_typed_failure(self) -> None:
        def _fake_run(*_args: Any, **kwargs: Any) -> Any:
            raise subprocess.TimeoutExpired(
                cmd=["yt-dlp"], timeout=kwargs.get("timeout", 600))

        with tempfile.TemporaryDirectory() as tmp:
            result = worker._run_ytdlp(
                url=YT_URL,
                expected_track_count=10,
                temp_root=Path(tmp),
                subprocess_run=_fake_run,
                ytdlp_bin_resolver=lambda: "/usr/bin/yt-dlp",
            )
        # Negative exit_code is a recognisable marker that the gate
        # cannot misinterpret as success.
        self.assertEqual(result.exit_code, -1)
        assert result.stderr_excerpt is not None
        self.assertIn("timed out", result.stderr_excerpt.lower())


# ---------------------------------------------------------------------------
# UTF-8 surrogate in real yt-dlp stderr — shim binary (KTD8 contract).
# ---------------------------------------------------------------------------


class TestRunYtdlpUtf8Surrogate(unittest.TestCase):
    """KTD8 / docs/solutions/subprocess-text-mode-utf8-strict-decode-crash.md.

    A fake yt-dlp shim that emits a bare ``0xE2`` byte (no UTF-8
    continuation) on stderr must not crash ``_run_ytdlp``. Pre-fix the
    decode happens inside ``Popen._communicate`` and the surrounding
    try/except cannot catch ``UnicodeDecodeError``; post-fix the
    bad byte becomes U+FFFD.
    """

    def _make_shim(self, bin_dir: str, name: str, body: str) -> str:
        path = os.path.join(bin_dir, name)
        with open(path, "w") as f:
            f.write("#!/bin/sh\n" + body)
        os.chmod(path, 0o755)
        return path

    def test_shim_emits_bare_0xe2_does_not_crash(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            bin_dir = os.path.join(tmpdir, "bin")
            os.makedirs(bin_dir)
            # Fake yt-dlp: emit ~1 KiB of stderr containing a bare 0xE2
            # byte, then exit 1 so the service routes to a failure path.
            self._make_shim(bin_dir, "yt-dlp", (
                'printf "%.s ERROR pad\\n" {1..50} >&2\n'
                # Bare 0xE2 byte (not a UTF-8 lead).
                'printf "ERROR: weird title=caf\\xe2X end\\n" >&2\n'
                'exit 1\n'
            ))
            temp_root = Path(tmpdir) / "scratch"
            temp_root.mkdir()
            saved_path = os.environ.get("PATH", "")
            try:
                os.environ["PATH"] = bin_dir + os.pathsep + saved_path
                # Drives the real subprocess.run — exercises the
                # ``text=True, errors='replace'`` discipline end-to-end.
                result = worker._run_ytdlp(
                    url="https://example.invalid/",
                    expected_track_count=1,
                    temp_root=temp_root,
                )
            finally:
                os.environ["PATH"] = saved_path

        # No UnicodeDecodeError was raised; result is a typed failure.
        self.assertEqual(result.exit_code, 1)
        assert result.stderr_excerpt is not None
        # The replacement character (U+FFFD) is present where 0xE2 was.
        self.assertIn("�", result.stderr_excerpt)


# ---------------------------------------------------------------------------
# main(): advisory-lock contention.
# ---------------------------------------------------------------------------


class TestMainAdvisoryLockContention(unittest.TestCase):
    """Second worker startup with the lock held exits cleanly (rc=0)."""

    def test_lock_held_returns_zero_without_sweeping(self) -> None:
        # Use a fake PipelineDB that yields False from advisory_lock.
        # We patch the module-level PipelineDB constructor inside main()
        # so the real DB is never touched.
        pdb = FakePipelineDB()
        pdb.set_advisory_lock_result(False)

        sweep_calls: list[Any] = []

        def _fake_sweep(db: Any, **_kwargs: Any) -> list[int]:
            sweep_calls.append(db)
            return []

        with patch.object(worker, "PipelineDB", return_value=pdb), \
                patch.object(
                    worker, "sweep_orphan_running_rows",
                    side_effect=_fake_sweep):
            rc = worker.main(
                ["--temp-dir", "/tmp/yt-test-tempdir-contention", "--once"]
            )

        self.assertEqual(rc, 0)
        # Sweep NEVER runs when the lock isn't acquired.
        self.assertEqual(sweep_calls, [])
        # The advisory-lock call was recorded against the namespace.
        self.assertEqual(
            pdb.advisory_lock_calls,
            [(ADVISORY_LOCK_NAMESPACE_YOUTUBE_INGEST, 1)],
        )


class TestMainKeyboardInterrupt(unittest.TestCase):
    """SIGINT translates to KeyboardInterrupt; main returns 0."""

    def test_keyboard_interrupt_returns_zero(self) -> None:
        pdb = FakePipelineDB()
        pdb.set_advisory_lock_result(True)

        def _interrupting_run_loop(*_a: Any, **_kw: Any) -> int:
            raise KeyboardInterrupt

        with patch.object(worker, "PipelineDB", return_value=pdb), \
                patch.object(
                    worker, "run_loop", side_effect=_interrupting_run_loop), \
                patch.object(
                    worker, "build_service",
                    return_value=YoutubeIngestService(pdb)):
            rc = worker.main(
                ["--temp-dir", "/tmp/yt-test-tempdir-sigint", "--once"]
            )

        self.assertEqual(rc, 0)


class TestMainHappyPath(unittest.TestCase):
    """End-to-end ``main()`` with ``--once`` and one queued job."""

    def test_main_drains_one_and_exits_clean(self) -> None:
        pdb = FakePipelineDB()
        pdb.set_advisory_lock_result(True)
        _seed_wanted_request(pdb, request_id=42)

        tmp = Path("/tmp/yt-test-mainhappy")
        runner_result = YtdlpRunResult(
            exit_code=0,
            stderr_excerpt=None,
            staged_files=[
                tmp / f"{i:02d}.opus" for i in range(EXPECTED_TRACKS)
            ],
        )

        def _fake_build_service(db: Any, **_kw: Any) -> YoutubeIngestService:
            return _service_with_fake_runner(db, runner_result)

        # Seed an accepted, unclaimed row after the startup sweep boundary.
        # The real sweep now only fails claimed rows, but this keeps the
        # main() test focused on the drain path rather than setup ordering.
        def _fake_sweep(db: Any, **_kwargs: Any) -> list[int]:
            _seed_running_row(db)
            return []

        with patch.object(worker, "PipelineDB", return_value=pdb), \
                patch.object(
                    worker, "sweep_orphan_running_rows",
                    side_effect=_fake_sweep), \
                patch.object(
                    worker, "build_service",
                    side_effect=_fake_build_service):
            rc = worker.main(
                ["--temp-dir", "/tmp/yt-test-tempdir-mainhappy", "--once"]
            )

        self.assertEqual(rc, 0)
        # The pending row was drained.
        jobs = [
            j for j in pdb.list_import_jobs(limit=50)
            if j.job_type == IMPORT_JOB_YOUTUBE
        ]
        self.assertEqual(len(jobs), 1)


# ---------------------------------------------------------------------------
# build_service wiring sanity.
# ---------------------------------------------------------------------------


class TestBuildService(unittest.TestCase):
    def test_builds_with_production_runner_by_default(self) -> None:
        pdb = FakePipelineDB()
        with tempfile.TemporaryDirectory() as tmp:
            svc = worker.build_service(pdb, temp_dir=Path(tmp))
        # Default runner is a closure binding the temp_dir; assert it's
        # callable. (No-op invocation would require a real yt-dlp; we
        # only test the wiring layer here.)
        self.assertTrue(callable(svc.ytdlp_runner_fn))
        self.assertIs(svc.pdb, pdb)

    def test_accepts_injected_runner(self) -> None:
        pdb = FakePipelineDB()
        sentinel = YtdlpRunResult(
            exit_code=0, stderr_excerpt=None, staged_files=[])

        def _runner(**_kw: Any) -> YtdlpRunResult:
            return sentinel

        with tempfile.TemporaryDirectory() as tmp:
            svc = worker.build_service(
                pdb, temp_dir=Path(tmp), ytdlp_runner_fn=_runner)
        self.assertIs(svc.ytdlp_runner_fn, _runner)

    def test_uses_configured_staging_dir_auto_import_child(self) -> None:
        pdb = FakePipelineDB()
        with tempfile.TemporaryDirectory() as tmp:
            svc = worker.build_service(
                pdb,
                temp_dir=Path(tmp) / "scratch",
                staging_dir=Path(tmp) / "staging",
                ytdlp_runner_fn=lambda **_kw: YtdlpRunResult(
                    exit_code=0, stderr_excerpt=None, staged_files=[]),
            )
        self.assertEqual(svc.staging_root, Path(tmp) / "staging" / "auto-import")

    def test_threads_source_address_into_runner_argv(self) -> None:
        # build_service must forward source_address all the way to the
        # yt-dlp argv via the bound production runner — otherwise the option
        # is dead config the worker never applies.
        pdb = FakePipelineDB()
        captured: dict[str, list[str]] = {}

        def _fake_subprocess_run(
            argv: list[str], **_kw: Any
        ) -> SimpleNamespace:
            captured["argv"] = list(argv)
            return SimpleNamespace(returncode=0, stderr="")

        with tempfile.TemporaryDirectory() as tmp:
            svc = worker.build_service(
                pdb, temp_dir=Path(tmp), source_address="192.168.1.36")
            svc.ytdlp_runner_fn(
                url="https://music.youtube.com/playlist?list=FAKE",
                expected_track_count=1,
                output_dir=Path(tmp),
                subprocess_run=_fake_subprocess_run,
                ytdlp_bin_resolver=lambda: "/usr/bin/yt-dlp",
            )
        argv = captured["argv"]
        self.assertIn("--source-address", argv)
        self.assertEqual(
            argv[argv.index("--source-address") + 1], "192.168.1.36")

    def test_no_source_address_in_runner_argv_by_default(self) -> None:
        pdb = FakePipelineDB()
        captured: dict[str, list[str]] = {}

        def _fake_subprocess_run(
            argv: list[str], **_kw: Any
        ) -> SimpleNamespace:
            captured["argv"] = list(argv)
            return SimpleNamespace(returncode=0, stderr="")

        with tempfile.TemporaryDirectory() as tmp:
            svc = worker.build_service(pdb, temp_dir=Path(tmp))
            svc.ytdlp_runner_fn(
                url="https://music.youtube.com/playlist?list=FAKE",
                expected_track_count=1,
                output_dir=Path(tmp),
                subprocess_run=_fake_subprocess_run,
                ytdlp_bin_resolver=lambda: "/usr/bin/yt-dlp",
            )
        self.assertNotIn("--source-address", captured["argv"])


class TestMainSourceAddressWiring(unittest.TestCase):
    """The ``--source-address`` CLI flag must reach ``build_service``."""

    def test_source_address_flag_reaches_build_service(self) -> None:
        pdb = FakePipelineDB()
        pdb.set_advisory_lock_result(True)
        captured: dict[str, Any] = {}

        def _fake_build_service(db: Any, **kw: Any) -> YoutubeIngestService:
            captured.update(kw)
            return YoutubeIngestService(pdb)

        with patch.object(worker, "PipelineDB", return_value=pdb), \
                patch.object(
                    worker, "sweep_orphan_running_rows", return_value=[]), \
                patch.object(
                    worker, "build_service",
                    side_effect=_fake_build_service):
            rc = worker.main([
                "--temp-dir", "/tmp/yt-test-srcaddr",
                "--source-address", "192.168.1.36",
                "--once",
            ])

        self.assertEqual(rc, 0)
        self.assertEqual(captured.get("source_address"), "192.168.1.36")


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
