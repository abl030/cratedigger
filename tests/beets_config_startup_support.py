"""Shared helpers for startup-only Beets authority tests."""

from __future__ import annotations

import logging
import os
import sys
import unittest
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from unittest.mock import MagicMock, patch

import cratedigger
from lib.beets_config_contract import BeetsRole
from scripts import import_preview_worker, importer
from tests.fakes.beets_contract import (
    BeetsContractWorld,
    snapshot_contract_world,
)
from web import server


class _PostGateEffect(Exception):
    """Stop an entrypoint at its first admitted application effect."""


class _AdmissionEventHandler(logging.Handler):
    """Record only the real startup adapter's admitted-result log."""

    def __init__(self, role: BeetsRole, events: list[str]) -> None:
        super().__init__(level=logging.INFO)
        self._admission_prefix = "Beets configuration admitted for "
        self._expected_prefix = f"{self._admission_prefix}{role} "
        self._events = events

    def emit(self, record: logging.LogRecord) -> None:
        message = record.getMessage()
        if message.startswith(self._admission_prefix):
            self._events.append(
                "admitted"
                if message.startswith(self._expected_prefix)
                else "wrong-role-admission"
            )


@contextmanager
def _isolated_installed_authority() -> Iterator[None]:
    """Restore process-global startup authority after a real admission."""
    from lib import config as runtime_config_module

    prior_runtime = os.environ.get("CRATEDIGGER_RUNTIME_CONFIG")
    prior_beetsdir = os.environ.get("BEETSDIR")
    prior_admitted = runtime_config_module._admitted_runtime_config
    runtime_config_module._admitted_runtime_config = None
    try:
        yield
    finally:
        runtime_config_module._admitted_runtime_config = prior_admitted
        if prior_runtime is None:
            os.environ.pop("CRATEDIGGER_RUNTIME_CONFIG", None)
        else:
            os.environ["CRATEDIGGER_RUNTIME_CONFIG"] = prior_runtime
        if prior_beetsdir is None:
            os.environ.pop("BEETSDIR", None)
        else:
            os.environ["BEETSDIR"] = prior_beetsdir


@dataclass(frozen=True)
class _RestartCase:
    role: BeetsRole
    entrypoint: Callable[[], int]


_RESTART_CASES: tuple[_RestartCase, ...] = (
    _RestartCase("main", cratedigger.main),
    _RestartCase("importer", importer.main),
    _RestartCase("preview", import_preview_worker.main),
    _RestartCase("web", server.main),
)


def _entrypoint_logger(case: _RestartCase) -> logging.Logger:
    if case.role == "main":
        return cratedigger.logger
    if case.role == "importer":
        return importer.logger
    if case.role == "preview":
        return import_preview_worker.logger
    return server.log


@contextmanager
def _record_admission_events(
    case: _RestartCase,
    events: list[str],
) -> Iterator[None]:
    logger = _entrypoint_logger(case)
    prior_level = logger.level
    handler = _AdmissionEventHandler(case.role, events)
    logger.setLevel(logging.INFO)
    logger.addHandler(handler)
    try:
        yield
    finally:
        logger.removeHandler(handler)
        logger.setLevel(prior_level)


def assert_one_admission_before_effect(events: tuple[str, ...]) -> None:
    """Require one admitted result/log before the first application effect."""
    admission_count = events.count("admitted")
    if admission_count != 1:
        raise AssertionError(
            "startup must emit exactly one admitted result/log before its "
            f"first application effect; observed {admission_count}: {events!r}"
        )
    if events != ("admitted", "effect"):
        raise AssertionError(
            "startup admission must precede exactly one first application "
            f"effect; observed {events!r}"
        )


def _snapshot_runtime_tree(
    root: Path,
) -> tuple[tuple[str, str, int, bytes | None], ...]:
    """Capture runtime names, contents, and modes around a rejected start."""
    snapshot: list[tuple[str, str, int, bytes | None]] = []
    for path in sorted(root.rglob("*")):
        relative = str(path.relative_to(root))
        mode = path.lstat().st_mode & 0o7777
        if path.is_symlink():
            snapshot.append((relative, "symlink", mode, os.readlink(path).encode()))
        elif path.is_file():
            snapshot.append((relative, "file", mode, path.read_bytes()))
        elif path.is_dir():
            snapshot.append((relative, "dir", mode, None))
        else:
            snapshot.append((relative, "other", mode, None))
    return tuple(snapshot)


def _snapshot_web_process_state() -> tuple[object, ...]:
    """Capture Beets, web globals, and caches that admission may rebind."""
    from beets import config as active_beets_config

    from web import cache

    return (
        active_beets_config["library"].as_filename(),
        active_beets_config["directory"].as_filename(),
        active_beets_config["statefile"].as_filename(),
        server.beets_db_path,
        server.beets_library_root,
        server.canonical_origin,
        server.insecure_mode,
        server._db_dsn,
        server.mb_api.MB_API_BASE,
        server._discogs.DISCOGS_API_BASE,
        id(cache._redis),
        server._rendered_index_document.cache_info(),
    )


@contextmanager
def _patched_rejection_cache_constructor(
    case: _RestartCase,
) -> Iterator[MagicMock | None]:
    """Expose the web cache's third-party constructor without running it."""
    if case.role == "web":
        with patch("redis.Redis") as constructor:
            yield constructor
        return
    yield None


@contextmanager
def _patched_rejection_beets_mutators(
    case: _RestartCase,
) -> Iterator[tuple[MagicMock, MagicMock] | None]:
    """Observe that web never rebinds Beets before a rejected admission."""
    if case.role == "web":
        from beets import config as active_beets_config

        with (
            patch.object(active_beets_config, "clear") as clear,
            patch.object(active_beets_config, "read") as read,
        ):
            yield clear, read
        return
    yield None


@contextmanager
def _patched_restart_boundary(
    case: _RestartCase,
    *,
    side_effect: object | None = None,
) -> Iterator[MagicMock]:
    """Stop a real entrypoint only at its external database boundary."""
    if case.role == "main":
        with patch("lib.migrator.psycopg2.connect", side_effect=side_effect) as boundary:
            yield boundary
        return
    if case.role == "importer":
        with patch("scripts.importer.PipelineDB", side_effect=side_effect) as boundary:
            yield boundary
        return
    if case.role == "preview":
        with patch("scripts.import_preview_worker.PipelineDB", side_effect=side_effect) as boundary:
            yield boundary
        return
    with patch("web.server.PipelineDB", side_effect=side_effect) as boundary:
        yield boundary


def _restart_argv(
    case: _RestartCase,
    world: BeetsContractWorld,
    *,
    probe_pre_gate_effects: bool = False,
) -> list[str]:
    common = [
        "--config", str(world.runtime_config),
        "--runtime-dir", str(world.runtime_dir),
    ]
    if case.role == "main":
        return ["cratedigger.py", *common]
    if case.role == "importer":
        return ["importer.py", *common, "--once"]
    if case.role == "preview":
        return ["import_preview_worker.py", *common, "--once"]
    argv = [
        "server.py", *common,
        "--canonical-origin", "https://music.example",
        "--dev-port", "0",
    ]
    if probe_pre_gate_effects:
        argv.extend([
            "--insecure-mode",
            "--redis-host", "must-not-connect.invalid",
            "--mb-api", "https://must-not-bind.invalid/ws/2",
            "--discogs-api", "https://must-not-bind.invalid/discogs",
        ])
    return argv


def _exercise_real_rejection_and_restart(
    test: unittest.TestCase,
    case: _RestartCase,
) -> None:
    """Reject, repair, and restart one real top-level application."""
    from lib import config as runtime_config_module
    from web import cache

    world = BeetsContractWorld(role=case.role)
    prior_main_config = cratedigger.cfg
    saved_umask = os.umask(0o027) if case.role == "main" else None
    prior_web_globals = (
        (
            server.beets_db_path,
            server.beets_library_root,
            server.canonical_origin,
            server.insecure_mode,
            server._db_dsn,
            server.mb_api.MB_API_BASE,
            server._discogs.DISCOGS_API_BASE,
            cache._redis,
        )
        if case.role == "web"
        else None
    )
    try:
        world.unseal()
        world._write_main_config(**{
            "import": {
                "autotag": True,
                "move": True,
                "write": False,
                "duplicate_keys": {
                    "album": ["mb_albumid", "discogs_albumid"],
                },
            },
        })
        world._seal(case.role)
        rejection_argv = _restart_argv(
            case,
            world,
            probe_pre_gate_effects=True,
        )
        before_rejection = snapshot_contract_world(world)
        before_runtime = _snapshot_runtime_tree(world.runtime_dir)
        lock_path = world.runtime_dir / ".cratedigger.lock"
        test.assertFalse(lock_path.exists())
        before_web = (
            _snapshot_web_process_state() if case.role == "web" else None
        )
        with (
            _isolated_installed_authority(),
            patch.object(sys, "argv", rejection_argv),
            _patched_restart_boundary(case) as effect,
            _patched_rejection_cache_constructor(case) as redis_constructor,
            _patched_rejection_beets_mutators(case) as beets_mutators,
        ):
            prior_runtime = os.environ.get("CRATEDIGGER_RUNTIME_CONFIG")
            prior_beetsdir = os.environ.get("BEETSDIR")
            test.assertEqual(case.entrypoint(), 1)
            effect.assert_not_called()
            test.assertEqual(
                os.environ.get("CRATEDIGGER_RUNTIME_CONFIG"),
                prior_runtime,
            )
            test.assertEqual(os.environ.get("BEETSDIR"), prior_beetsdir)
            test.assertIsNone(runtime_config_module._admitted_runtime_config)
            if case.role == "main":
                test.assertIs(cratedigger.cfg, prior_main_config)
            if case.role == "web":
                if redis_constructor is None:
                    raise AssertionError("web cache constructor was not observed")
                if beets_mutators is None:
                    raise AssertionError("web Beets mutators were not observed")
                clear_beets, read_beets = beets_mutators
                clear_beets.assert_not_called()
                read_beets.assert_not_called()
                redis_constructor.assert_not_called()
                test.assertEqual(_snapshot_web_process_state(), before_web)
        test.assertEqual(snapshot_contract_world(world), before_rejection)
        test.assertEqual(_snapshot_runtime_tree(world.runtime_dir), before_runtime)
        test.assertFalse(lock_path.exists())
        if saved_umask is not None:
            observed_umask = os.umask(0o027)
            test.assertEqual(observed_umask, 0o027)

        world.unseal()
        world._write_main_config()
        world._seal(case.role)
        before_restart = snapshot_contract_world(world)
        events: list[str] = []

        def first_application_effect(
            *_args: object,
            **_kwargs: object,
        ) -> None:
            events.append("effect")
            raise _PostGateEffect

        with (
            _isolated_installed_authority(),
            patch.object(sys, "argv", _restart_argv(case, world)),
            _record_admission_events(case, events),
            _patched_restart_boundary(
                case,
                side_effect=first_application_effect,
            ) as effect,
            test.assertRaises(_PostGateEffect),
        ):
            case.entrypoint()
        effect.assert_called_once()
        assert_one_admission_before_effect(tuple(events))
        test.assertEqual(snapshot_contract_world(world), before_restart)
        test.assertFalse(lock_path.exists())
    finally:
        if saved_umask is not None:
            os.umask(saved_umask)
        cratedigger.cfg = prior_main_config
        if prior_web_globals is not None:
            from beets import config as active_beets_config

            from web import cache

            (
                server.beets_db_path,
                server.beets_library_root,
                server.canonical_origin,
                server.insecure_mode,
                server._db_dsn,
                server.mb_api.MB_API_BASE,
                server._discogs.DISCOGS_API_BASE,
                cache._redis,
            ) = prior_web_globals
            active_beets_config.clear()
            active_beets_config.read(user=True, defaults=True)
        world.close()
