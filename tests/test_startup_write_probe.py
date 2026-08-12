"""Tests for lib.startup_write_probe (issue #1085).

Pure primitive/builder tests, plus deterministic pins per gated unit: break
exactly one required path, prove the real entrypoint exits before any queue
recovery/claim/DB mutation with a message naming the unit, the path, the
operation, and the errno class, restore the path, and prove the natural
retry gets past the probe cleanly. A dedicated pin proves the private-tree
primitive rejects an ownership/mode drift a generic descriptor open would
have accepted (issue #1085 review round 2, MUST FIX 1), and a "calls its
own builder" pin proves a copy-paste of the wrong unit's required-paths
import would be caught (MUST FIX 5).
"""

from __future__ import annotations

import logging
import os
import stat
import sys
import tempfile
import unittest
from collections.abc import Callable
from contextlib import ExitStack
from unittest.mock import patch

import cratedigger
from lib.config import CratediggerConfig
from lib.fs_authority import open_directory_path
from lib.processing_paths import processing_albums_dir
from lib.startup_write_probe import (
    RequiredPaths,
    StartupProbeError,
    cratedigger_required_paths,
    importer_required_paths,
    preview_worker_required_paths,
    probe_startup_paths,
    web_required_paths,
    youtube_ingest_required_paths,
)
from scripts import import_preview_worker, importer, youtube_ingest_worker
from tests.beets_config_startup_support import (
    _isolated_installed_authority,
    _patched_restart_boundary,
    _PostGateEffect,
    _record_admission_events,
    _restart_argv,
    _RestartCase,
    _snapshot_runtime_tree,
    assert_one_admission_before_effect,
)
from tests.fakes.beets_contract import BeetsContractWorld, snapshot_contract_world
from web import server

_QUIET = logging.getLogger("test-startup-write-probe")
_QUIET.addHandler(logging.NullHandler())
_QUIET.propagate = False


# ---------------------------------------------------------------------------
# Pure primitive tests: probe_startup_paths itself, no entrypoint involved.
# ---------------------------------------------------------------------------


class TestProbeStartupPathsPrimitives(unittest.TestCase):
    def test_read_probe_succeeds_on_a_real_directory(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            probe_startup_paths(
                unit="x", logger=_QUIET, required=RequiredPaths(read=(tmp,)))

    def test_write_probe_succeeds_and_leaves_no_debris(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            probe_startup_paths(
                unit="x", logger=_QUIET, required=RequiredPaths(write=(tmp,)))
            self.assertEqual(os.listdir(tmp), [])

    def test_missing_path_names_unit_path_operation_and_errno(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            missing = os.path.join(tmp, "does-not-exist")
            with self.assertRaises(StartupProbeError) as caught:
                probe_startup_paths(
                    unit="my-unit", logger=_QUIET,
                    required=RequiredPaths(write=(missing,)))
        message = str(caught.exception)
        self.assertIn("my-unit", message)
        self.assertIn(missing, message)
        self.assertIn("write", message)
        self.assertIn("ENOENT", message)

    def test_unreadable_directory_names_unit_path_operation_and_errno(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            target = os.path.join(tmp, "locked")
            os.mkdir(target)
            os.chmod(target, 0o000)
            try:
                with self.assertRaises(StartupProbeError) as caught:
                    probe_startup_paths(
                        unit="my-unit", logger=_QUIET,
                        required=RequiredPaths(read=(target,)))
            finally:
                os.chmod(target, 0o700)
        message = str(caught.exception)
        self.assertIn("my-unit", message)
        self.assertIn(target, message)
        # The descriptor open itself is what EACCES refuses here.
        self.assertIn("open", message)
        self.assertIn("EACCES", message)

    def test_write_only_denial_fails_write_but_not_read(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            target = os.path.join(tmp, "readonly")
            os.mkdir(target)
            os.chmod(target, 0o500)
            try:
                probe_startup_paths(
                    unit="x", logger=_QUIET, required=RequiredPaths(read=(target,)))
                with self.assertRaises(StartupProbeError) as caught:
                    probe_startup_paths(
                        unit="x", logger=_QUIET,
                        required=RequiredPaths(write=(target,)))
            finally:
                os.chmod(target, 0o700)
        self.assertIn("create", str(caught.exception))

    def test_logger_records_the_failure(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            missing = os.path.join(tmp, "gone")
            with self.assertLogs("test-startup-write-probe-logged", level="ERROR") as cm:
                logger = logging.getLogger("test-startup-write-probe-logged")
                with self.assertRaises(StartupProbeError):
                    probe_startup_paths(
                        unit="logged-unit", logger=logger,
                        required=RequiredPaths(write=(missing,)))
        self.assertTrue(
            any("logged-unit" in record for record in cm.output))

    def test_a_relative_or_empty_path_is_a_startup_probe_error_not_a_crash(
        self,
    ) -> None:
        # FilesystemAuthorityError from open_directory_path's own absolute-
        # path guard must surface as a StartupProbeError like every other
        # refusal, not as an unhandled exception.
        with self.assertRaises(StartupProbeError):
            probe_startup_paths(
                unit="x", logger=_QUIET,
                required=RequiredPaths(write=("relative",)))

    def test_private_tree_ownership_drift_is_refused(self) -> None:
        """MUST FIX 1: the private-tree write probe must reject a child
        that drifted off the strict 0700 contract, even though a generic
        descriptor open of the SAME directory succeeds."""
        with tempfile.TemporaryDirectory() as processing_dir, \
                tempfile.TemporaryDirectory() as slskd_dir:
            os.chmod(processing_dir, 0o700)
            albums = os.path.join(processing_dir, "albums")
            os.mkdir(albums, 0o750)
            os.chmod(albums, 0o750)
            try:
                # The generic primitive this probe deliberately avoids for
                # the private tree would have accepted this world -- proving
                # the private-tree primitive does real, additional work.
                with open_directory_path(albums):
                    pass
                with self.assertRaises(StartupProbeError) as caught:
                    probe_startup_paths(
                        unit="x", logger=_QUIET,
                        required=RequiredPaths(
                            private_processing_dir=processing_dir,
                            private_slskd_download_dir=slskd_dir,
                            private_write_children=("albums",),
                        ),
                    )
            finally:
                os.chmod(albums, 0o700)
        self.assertIn("untrusted_ownership", str(caught.exception))


# ---------------------------------------------------------------------------
# Per-unit required-path builders: a subTest table, one row per unit.
# ---------------------------------------------------------------------------


class TestRequiredPathsBuilders(unittest.TestCase):
    def setUp(self) -> None:
        self.cfg = CratediggerConfig(
            slskd_download_dir="/slskd",
            processing_dir="/processing",
            beets_staging_dir="/staging",
            var_dir="/var-dir",
        )
        self.unconfigured_staging_cfg = CratediggerConfig(
            slskd_download_dir="/slskd",
            processing_dir="/processing",
            beets_staging_dir="",
            var_dir="/var-dir",
        )

    def test_every_builder_produces_its_documented_paths(self) -> None:
        albums = processing_albums_dir(self.cfg.processing_dir)
        cases = (
            (
                "cratedigger",
                cratedigger_required_paths(self.cfg),
                RequiredPaths(
                    read=("/slskd",),
                    write=("/var-dir", "/slskd", "/staging"),
                    private_processing_dir="/processing",
                    private_slskd_download_dir="/slskd",
                    private_write_children=("albums",),
                ),
            ),
            (
                "importer",
                importer_required_paths(self.cfg),
                RequiredPaths(
                    read=("/slskd", albums, "/staging"),
                    write=("/staging",),
                    private_processing_dir="/processing",
                    private_slskd_download_dir="/slskd",
                    private_write_children=("albums",),
                ),
            ),
            (
                "preview",
                preview_worker_required_paths(self.cfg),
                RequiredPaths(
                    read=("/slskd", albums, "/staging"),
                    write=("/var-dir",),
                    private_processing_dir="/processing",
                    private_slskd_download_dir="/slskd",
                    private_write_root=True,
                    private_write_children=("albums", "preview"),
                ),
            ),
            (
                "web",
                web_required_paths(self.cfg),
                RequiredPaths(
                    read=("/slskd", albums, "/staging"),
                    write=("/var-dir", albums, "/staging"),
                    private_processing_dir="/processing",
                    private_slskd_download_dir="/slskd",
                    private_write_root=True,
                    private_write_children=("preview",),
                ),
            ),
            (
                "youtube-ingest",
                youtube_ingest_required_paths(
                    temp_dir="/yt-temp", staging_dir="/staging"),
                RequiredPaths(write=("/yt-temp", "/staging")),
            ),
        )
        for name, actual, expected in cases:
            with self.subTest(unit=name):
                self.assertEqual(actual, expected)

    def test_unconfigured_beets_staging_dir_is_never_probed(self) -> None:
        """MUST FIX 2: an empty (unconfigured) beets_staging_dir must be
        silently excluded from every builder's read/write lists, not
        probed as a broken required path."""
        albums = processing_albums_dir(self.unconfigured_staging_cfg.processing_dir)
        cases = (
            (
                "cratedigger",
                cratedigger_required_paths(self.unconfigured_staging_cfg),
                RequiredPaths(
                    read=("/slskd",),
                    write=("/var-dir", "/slskd"),
                    private_processing_dir="/processing",
                    private_slskd_download_dir="/slskd",
                    private_write_children=("albums",),
                ),
            ),
            (
                "importer",
                importer_required_paths(self.unconfigured_staging_cfg),
                RequiredPaths(
                    read=("/slskd", albums),
                    private_processing_dir="/processing",
                    private_slskd_download_dir="/slskd",
                    private_write_children=("albums",),
                ),
            ),
            (
                "preview",
                preview_worker_required_paths(self.unconfigured_staging_cfg),
                RequiredPaths(
                    read=("/slskd", albums),
                    write=("/var-dir",),
                    private_processing_dir="/processing",
                    private_slskd_download_dir="/slskd",
                    private_write_root=True,
                    private_write_children=("albums", "preview"),
                ),
            ),
            (
                "web",
                web_required_paths(self.unconfigured_staging_cfg),
                RequiredPaths(
                    read=("/slskd", albums),
                    write=("/var-dir", albums),
                    private_processing_dir="/processing",
                    private_slskd_download_dir="/slskd",
                    private_write_root=True,
                    private_write_children=("preview",),
                ),
            ),
        )
        for name, actual, expected in cases:
            with self.subTest(unit=name):
                self.assertEqual(actual, expected)
                self.assertNotIn("", actual.read)
                self.assertNotIn("", actual.write)

    def test_no_builder_targets_an_album_directory(self) -> None:
        """Hard constraint: never write inside processing/albums/<album>.

        Every write path is either a documented root or the ``albums``/
        ``preview`` PARENT directory itself; every private child name is
        exactly ``albums`` or ``preview`` -- never a nested path -- since
        ``open_private_child_directory`` only opens those two names.
        """
        albums = processing_albums_dir(self.cfg.processing_dir)
        for required in (
            cratedigger_required_paths(self.cfg),
            importer_required_paths(self.cfg),
            preview_worker_required_paths(self.cfg),
            web_required_paths(self.cfg),
        ):
            for path in required.write:
                self.assertFalse(
                    path.startswith(albums + os.sep),
                    f"{path} writes inside an album directory",
                )
            for child in required.private_write_children:
                self.assertIn(child, ("albums", "preview"))


# ---------------------------------------------------------------------------
# Deterministic pins: one broken required path per gated unit, driven
# through the REAL entrypoint main(). "cratedigger-unfindable" is
# deliberately absent -- it never calls this module at all (see the
# dedicated audit in tests/test_startup_write_probe_generated.py).
# ---------------------------------------------------------------------------


def _first_application_effect(events: list[str]) -> object:
    def _effect(*_args: object, **_kwargs: object) -> None:
        events.append("effect")
        raise _PostGateEffect
    return _effect


class TestEntrypointStartupProbePins(unittest.TestCase):
    def _pin_case(
        self,
        case: _RestartCase,
        world: BeetsContractWorld,
        *,
        broken_path: str,
        mode: int = 0o000,
        expected_errno: str = "EACCES",
    ) -> None:
        lock_path = world.runtime_dir / ".cratedigger.lock"
        before_contract = snapshot_contract_world(world)
        before_runtime = _snapshot_runtime_tree(world.runtime_dir)
        original_mode = stat.S_IMODE(os.stat(broken_path).st_mode)

        os.chmod(broken_path, mode)
        try:
            with (
                _isolated_installed_authority(),
                patch.object(sys, "argv", _restart_argv(case, world)),
                _patched_restart_boundary(case) as boundary,
                self.assertLogs(level="ERROR") as logs,
            ):
                self.assertEqual(case.entrypoint(), 1)
            boundary.assert_not_called()
            message = "\n".join(logs.output)
            self.assertIn(broken_path, message)
            self.assertIn(expected_errno, message)
            self.assertFalse(lock_path.exists())
        finally:
            os.chmod(broken_path, original_mode)

        # Only meaningful once broken_path's own mode is restored -- while
        # it is still broken, the deliberate mode drift IS the diff.
        self.assertEqual(snapshot_contract_world(world), before_contract)
        self.assertEqual(
            _snapshot_runtime_tree(world.runtime_dir), before_runtime)

        # Restore, and prove the natural retry gets PAST the probe: exactly
        # one admission precedes the next boundary this entrypoint owns
        # (DB construction) -- the same contract every real restart proves.
        events: list[str] = []
        with (
            _isolated_installed_authority(),
            patch.object(sys, "argv", _restart_argv(case, world)),
            _record_admission_events(case, events),
            _patched_restart_boundary(
                case, side_effect=_first_application_effect(events),
            ) as boundary,
            self.assertRaises(_PostGateEffect),
        ):
            case.entrypoint()
        boundary.assert_called_once()
        assert_one_admission_before_effect(tuple(events))

    def test_main_refuses_on_an_unusable_slskd_download_dir(self) -> None:
        world = BeetsContractWorld(role="main")
        self.addCleanup(world.close)
        self._pin_case(
            _RestartCase("main", cratedigger.main),
            world,
            broken_path=str(world.slskd_download_dir),
        )

    def test_importer_refuses_on_an_unusable_staging_dir(self) -> None:
        world = BeetsContractWorld(role="importer")
        self.addCleanup(world.close)
        self._pin_case(
            _RestartCase("importer", importer.main),
            world,
            broken_path=str(world.beets_staging_dir),
        )

    def test_preview_refuses_on_an_unusable_preview_scratch(self) -> None:
        world = BeetsContractWorld(role="preview")
        self.addCleanup(world.close)
        self._pin_case(
            _RestartCase("preview", import_preview_worker.main),
            world,
            broken_path=str(world.processing_dir / "preview"),
        )

    def test_web_refuses_on_an_unusable_processing_albums_dir(self) -> None:
        # Deliberately NOT var_dir/runtime_dir: the shared _pin_case
        # assertions (lock_path, _snapshot_runtime_tree) inspect
        # world.runtime_dir itself, which would be vacuous against a
        # broken runtime_dir. processing/albums is web's own generic
        # (non-private-tree) write target, distinct from cratedigger's
        # private-tree ownership pin below.
        world = BeetsContractWorld(role="web")
        self.addCleanup(world.close)
        self._pin_case(
            _RestartCase("web", server.main),
            world,
            broken_path=str(world.processing_dir / "albums"),
        )

    def test_main_refuses_on_a_private_tree_ownership_drift(self) -> None:
        """MUST FIX 1 regression, driven through the real entrypoint: a
        processing/albums child that drifted to 0750 (the #570/#578-shaped
        failure) must stop cratedigger before any DB mutation, naming
        ``untrusted_ownership`` -- not silently pass a generic-only check."""
        world = BeetsContractWorld(role="main")
        self.addCleanup(world.close)
        case = _RestartCase("main", cratedigger.main)
        albums = world.processing_dir / "albums"
        lock_path = world.runtime_dir / ".cratedigger.lock"
        before_contract = snapshot_contract_world(world)
        before_runtime = _snapshot_runtime_tree(world.runtime_dir)

        os.chmod(albums, 0o750)
        try:
            with (
                _isolated_installed_authority(),
                patch.object(sys, "argv", _restart_argv(case, world)),
                _patched_restart_boundary(case) as boundary,
                self.assertLogs(level="ERROR") as logs,
            ):
                self.assertEqual(case.entrypoint(), 1)
            boundary.assert_not_called()
            message = "\n".join(logs.output)
            self.assertIn(str(albums), message)
            self.assertIn("untrusted_ownership", message)
            self.assertFalse(lock_path.exists())
        finally:
            os.chmod(albums, 0o700)

        # Only meaningful once albums' own mode is restored -- while it is
        # still drifted, the deliberate mode change IS the diff.
        self.assertEqual(snapshot_contract_world(world), before_contract)
        self.assertEqual(
            _snapshot_runtime_tree(world.runtime_dir), before_runtime)


class TestYoutubeIngestStartupProbePin(unittest.TestCase):
    """No BeetsContractWorld here -- this worker never calls
    enforce_beets_startup; both required paths come straight off argv."""

    def test_refuses_on_an_unusable_temp_dir_and_recovers(self) -> None:
        from tests.fakes import FakePipelineDB

        with tempfile.TemporaryDirectory() as temp_dir, \
                tempfile.TemporaryDirectory() as staging_dir:
            os.chmod(temp_dir, 0o000)
            try:
                with (
                    patch.object(
                        youtube_ingest_worker, "PipelineDB",
                    ) as db_ctor,
                    self.assertLogs(
                        youtube_ingest_worker.logger, level="ERROR") as logs,
                ):
                    rc = youtube_ingest_worker.main([
                        "--temp-dir", temp_dir,
                        "--staging-dir", staging_dir,
                        "--once",
                    ])
                self.assertEqual(rc, 1)
                db_ctor.assert_not_called()
                message = "\n".join(logs.output)
                self.assertIn(temp_dir, message)
                self.assertIn("EACCES", message)
            finally:
                os.chmod(temp_dir, 0o700)

            # Restore, and prove the natural retry starts cleanly: the
            # worker reaches its own real DB boundary this time.
            pdb = FakePipelineDB()
            pdb.set_advisory_lock_result(False)
            with patch.object(
                youtube_ingest_worker, "PipelineDB", return_value=pdb,
            ):
                rc = youtube_ingest_worker.main([
                    "--temp-dir", temp_dir,
                    "--staging-dir", staging_dir,
                    "--once",
                ])
            # Advisory-lock contention (deliberately faked here) is the
            # expected "reached the DB and moved on" outcome, exit 0.
            self.assertEqual(rc, 0)
            self.assertEqual(
                pdb.advisory_lock_calls,
                [(youtube_ingest_worker.ADVISORY_LOCK_NAMESPACE_YOUTUBE_INGEST, 1)],
            )


# ---------------------------------------------------------------------------
# MUST FIX 5: each entrypoint must call its OWN required-paths builder.
# Today web/server.py calling importer_required_paths would pass every
# other pin and the generated property untouched.
# ---------------------------------------------------------------------------


class TestEntrypointCallsItsOwnBuilder(unittest.TestCase):
    _BUILDER_NAMES = (
        "cratedigger_required_paths",
        "importer_required_paths",
        "preview_worker_required_paths",
        "web_required_paths",
    )

    def _assert_only_builder_called(
        self, case: _RestartCase, world: BeetsContractWorld, expected: str,
    ) -> None:
        import lib.startup_write_probe as probe_module

        calls: list[str] = []
        originals: dict[str, Callable[[CratediggerConfig], RequiredPaths]] = {
            name: getattr(probe_module, name) for name in self._BUILDER_NAMES
        }

        def make_wrapper(
            name: str, original: Callable[[CratediggerConfig], RequiredPaths],
        ) -> Callable[[CratediggerConfig], RequiredPaths]:
            def wrapper(cfg: CratediggerConfig) -> RequiredPaths:
                calls.append(name)
                return original(cfg)
            return wrapper

        events: list[str] = []
        with ExitStack() as stack:
            stack.enter_context(_isolated_installed_authority())
            stack.enter_context(
                patch.object(sys, "argv", _restart_argv(case, world)))
            for name in self._BUILDER_NAMES:
                stack.enter_context(patch.object(
                    probe_module, name, new=make_wrapper(name, originals[name])))
            boundary = stack.enter_context(_patched_restart_boundary(
                case, side_effect=_first_application_effect(events)))
            with self.assertRaises(_PostGateEffect):
                case.entrypoint()
        boundary.assert_called_once()
        self.assertEqual(calls, [expected], calls)

    def test_main_calls_cratedigger_required_paths(self) -> None:
        world = BeetsContractWorld(role="main")
        self.addCleanup(world.close)
        self._assert_only_builder_called(
            _RestartCase("main", cratedigger.main),
            world, "cratedigger_required_paths",
        )

    def test_importer_calls_importer_required_paths(self) -> None:
        world = BeetsContractWorld(role="importer")
        self.addCleanup(world.close)
        self._assert_only_builder_called(
            _RestartCase("importer", importer.main),
            world, "importer_required_paths",
        )

    def test_preview_calls_preview_worker_required_paths(self) -> None:
        world = BeetsContractWorld(role="preview")
        self.addCleanup(world.close)
        self._assert_only_builder_called(
            _RestartCase("preview", import_preview_worker.main),
            world, "preview_worker_required_paths",
        )

    def test_web_calls_web_required_paths(self) -> None:
        world = BeetsContractWorld(role="web")
        self.addCleanup(world.close)
        self._assert_only_builder_called(
            _RestartCase("web", server.main),
            world, "web_required_paths",
        )


if __name__ == "__main__":
    unittest.main()
