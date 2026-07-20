#!/usr/bin/env python3
"""Generated real-Beets destructive contracts across common config profiles."""

from __future__ import annotations

import os
import re
import subprocess as sp
import sys
import tempfile
import unittest
from dataclasses import dataclass, replace
from functools import lru_cache
from pathlib import Path
from typing import Any
from unittest.mock import patch

import msgspec
import yaml
from beets import library
from hypothesis import HealthCheck, example, given, settings, strategies as st

import tests._hypothesis_profiles  # noqa: F401
from lib.beets_album_op import remove_by_selector
from lib.beets_delete import (
    BeetsDeleteCompleted,
    BeetsDeleteFailed,
    BeetsDeleteOutcome,
    BeetsDeleteRequest,
)


REPO = Path(__file__).resolve().parent.parent
MODULE_TEXT = (REPO / "nix" / "module.nix").read_text(encoding="utf-8")
PLUGIN_MATCH = re.search(r'plugins = "([^"]+)";', MODULE_TEXT)
assert PLUGIN_MATCH is not None
PRODUCTION_PLUGINS = PLUGIN_MATCH.group(1)

MB_RELEASE = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
MB_SIBLING = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
DISCOGS_RELEASE = "12856590"
DISCOGS_SIBLING = "12856591"
PROFILES = (
    "minimal",
    "module_placeholder",
    "module_include",
    "module_unreadable_include",
    "importsource",
    "playlist_auto",
)


@dataclass(frozen=True)
class RealBeetsObservation:
    profile: str
    track_count: int
    source: str
    expected_config_failure: bool
    cli_failure_reason: str | None
    cli_album_present: bool
    cli_items_present: int
    cli_files_present: int
    cli_import_sources_present: int
    cli_sibling_present: bool
    child_returncode: int
    child_stdout: bytes
    child_outcome: BeetsDeleteOutcome
    child_album_present: bool
    child_items_present: int
    child_files_present: int
    child_import_sources_present: int
    child_sibling_present: bool


def assert_real_beets_contract(observation: RealBeetsObservation) -> None:
    """Common configs either complete exactly or fail before mutation."""
    if observation.child_stdout != msgspec.json.encode(observation.child_outcome):
        raise AssertionError("exact-delete stdout is not one canonical outcome frame")
    if observation.child_returncode != 0:
        raise AssertionError("exact-delete child did not return a typed outcome")
    if not observation.cli_sibling_present or not observation.child_sibling_present:
        raise AssertionError("an exact destructive operation touched a sibling pressing")
    if observation.cli_import_sources_present != observation.track_count:
        raise AssertionError("CLI removal touched separately owned import sources")
    if observation.child_import_sources_present != observation.track_count:
        raise AssertionError("exact-delete touched separately owned import sources")

    if observation.expected_config_failure:
        if observation.cli_failure_reason != "exception":
            raise AssertionError("unreadable include was not rejected by CLI preflight")
        if not observation.cli_album_present or observation.cli_items_present == 0:
            raise AssertionError("CLI config rejection mutated Beets metadata")
        if observation.cli_files_present != observation.track_count:
            raise AssertionError("CLI config rejection mutated library files")
        if not isinstance(observation.child_outcome, BeetsDeleteFailed):
            raise AssertionError("child config rejection was promoted to completion")
        if observation.child_outcome.reason != "configuration_error":
            raise AssertionError("child config rejection lost its typed reason")
        if not observation.child_album_present or observation.child_items_present == 0:
            raise AssertionError("child config rejection mutated Beets metadata")
        if observation.child_files_present != observation.track_count:
            raise AssertionError("child config rejection mutated library files")
        return

    if observation.cli_failure_reason is not None:
        raise AssertionError(f"real beet remove failed: {observation.cli_failure_reason}")
    if observation.cli_album_present or observation.cli_items_present:
        raise AssertionError("real beet remove left metadata behind")
    if observation.cli_files_present:
        raise AssertionError("real beet remove left owned tracks behind")
    if not isinstance(observation.child_outcome, BeetsDeleteCompleted):
        raise AssertionError(f"exact-delete failed: {observation.child_outcome!r}")
    if observation.child_album_present or observation.child_items_present:
        raise AssertionError("exact-delete left metadata behind")
    if observation.child_files_present:
        raise AssertionError("exact-delete left owned tracks behind")


def _profile_config(
    profile: str,
    *,
    root: Path,
    db_path: Path,
    config_dir: Path,
) -> None:
    plugins = ""
    extra: dict[str, object] = {}
    if profile.startswith("module_"):
        plugins = PRODUCTION_PLUGINS
        if profile == "module_placeholder":
            extra["discogs"] = {"user_token": "placeholder-token"}
        else:
            extra["include"] = ["secrets.yaml"]
            secret = config_dir / "secrets.yaml"
            secret.write_text(
                "discogs:\n  user_token: test-token\n", encoding="utf-8",
            )
            if profile == "module_unreadable_include":
                secret.chmod(0)
    elif profile == "importsource":
        plugins = "importsource"
        extra["importsource"] = {"suggest_removal": True}
    elif profile == "playlist_auto":
        plugins = "playlist"
        playlist_dir = config_dir / "playlists"
        playlist_dir.mkdir()
        extra["playlist"] = {
            "auto": True,
            "playlist_dir": str(playlist_dir),
            "relative_to": "library",
        }

    config: dict[str, object] = {
        "directory": str(root),
        "library": str(db_path),
        "plugins": plugins,
        "clutter": ["*.jpg", "cratedigger.json"],
        **extra,
    }
    (config_dir / "config.yaml").write_text(
        yaml.safe_dump(config, sort_keys=False), encoding="utf-8",
    )


def _seed(
    *,
    root: Path,
    db_path: Path,
    track_count: int,
    source: str,
) -> tuple[int, Path, Path, int]:
    target_dir = root / "Target" / "Album"
    sibling_dir = root / "Sibling" / "Album"
    source_dir = root.parent / "original-source"
    target_dir.mkdir(parents=True)
    sibling_dir.mkdir(parents=True)
    source_dir.mkdir(parents=True, exist_ok=True)
    target_items: list[library.Item] = []
    for index in range(1, track_count + 1):
        path = target_dir / f"{index:02d} Track.flac"
        path.write_bytes(f"audio-{index}".encode())
        source_path = source_dir / f"{index:02d} Track.flac"
        source_path.write_bytes(f"source-{index}".encode())
        fields: dict[str, Any] = {
            "path": str(path),
            "title": f"Track {index}",
            "artist": "Target",
            "album": "Album",
            "albumartist": "Target",
        }
        if source == "mb":
            fields["mb_albumid"] = MB_RELEASE
        else:
            fields["discogs_albumid"] = int(DISCOGS_RELEASE)
        item = library.Item(**fields)
        item["source_path"] = str(source_path)
        target_items.append(item)

    sibling_path = sibling_dir / "01 Sibling.flac"
    sibling_path.write_bytes(b"rare sibling")
    sibling_fields: dict[str, Any] = {
        "path": str(sibling_path),
        "title": "Sibling",
        "artist": "Sibling",
        "album": "Album",
        "albumartist": "Sibling",
    }
    if source == "mb":
        sibling_fields["mb_albumid"] = MB_SIBLING
    else:
        sibling_fields["discogs_albumid"] = int(DISCOGS_SIBLING)

    lib = library.Library(str(db_path), str(root))
    target = lib.add_album(target_items)
    sibling = lib.add_album([library.Item(**sibling_fields)])
    target_id = int(target.id)
    sibling_id = int(sibling.id)
    lib._close()
    return target_id, target_dir, source_dir, sibling_id


def _metadata_state(db_path: Path, root: Path, album_id: int) -> tuple[bool, int]:
    lib = library.Library(str(db_path), str(root))
    present = lib.get_album(album_id) is not None
    item_count = len(list(lib.items(f"album_id:{album_id}")))
    lib._close()
    return present, item_count


def _track_count(path: Path) -> int:
    return len(tuple(path.glob("*.flac"))) if path.exists() else 0


@lru_cache(maxsize=None)
def exercise_real_beets_world(
    profile: str,
    track_count: int,
    source: str,
) -> RealBeetsObservation:
    """Run each finite generated world once; fuzz reuses real observations."""
    with tempfile.TemporaryDirectory() as raw:
        base = Path(raw)
        cli_area = base / "cli"
        cli_area.mkdir()
        cli_root = cli_area / "library"
        cli_root.mkdir()
        cli_db = cli_area / "library.db"
        cli_config = cli_area / "config"
        cli_config.mkdir()
        _profile_config(
            profile, root=cli_root, db_path=cli_db, config_dir=cli_config,
        )
        cli_album_id, cli_album_dir, cli_source_dir, cli_sibling_id = _seed(
            root=cli_root, db_path=cli_db,
            track_count=track_count, source=source,
        )
        runtime = base / "config.ini"
        runtime.write_text(
            "[Beets]\n"
            f"directory = {cli_root}\n"
            f"config_dir = {cli_config}\n",
            encoding="utf-8",
        )
        selector = (
            f"mb_albumid:{MB_RELEASE}" if source == "mb"
            else f"discogs_albumid:{DISCOGS_RELEASE}"
        )
        with patch.dict(
            os.environ,
            {"CRATEDIGGER_RUNTIME_CONFIG": str(runtime)},
            clear=False,
        ):
            cli_failure = remove_by_selector(selector, timeout=30)
        cli_album_present, cli_items_present = _metadata_state(
            cli_db, cli_root, cli_album_id,
        )
        cli_sibling_present, _ = _metadata_state(
            cli_db, cli_root, cli_sibling_id,
        )

        child_area = base / "child"
        child_area.mkdir()
        child_root = child_area / "library"
        child_root.mkdir()
        child_db = child_area / "library.db"
        child_config = child_area / "config"
        child_config.mkdir()
        _profile_config(
            profile, root=child_root, db_path=child_db, config_dir=child_config,
        )
        child_album_id, child_album_dir, child_source_dir, child_sibling_id = _seed(
            root=child_root, db_path=child_db,
            track_count=track_count, source=source,
        )
        expected_release = MB_RELEASE if source == "mb" else DISCOGS_RELEASE
        request = BeetsDeleteRequest(
            album_id=child_album_id,
            expected_release_id=expected_release,
            library_db_path=str(child_db),
            library_root=str(child_root),
        )
        child_env = {**os.environ, "BEETSDIR": str(child_config)}
        child = sp.run(
            [sys.executable, str(REPO / "harness" / "delete_album.py")],
            input=msgspec.json.encode(request),
            capture_output=True,
            env=child_env,
            timeout=30,
        )
        outcome = msgspec.json.decode(child.stdout, type=BeetsDeleteOutcome)
        child_album_present, child_items_present = _metadata_state(
            child_db, child_root, child_album_id,
        )
        child_sibling_present, _ = _metadata_state(
            child_db, child_root, child_sibling_id,
        )
        return RealBeetsObservation(
            profile=profile,
            track_count=track_count,
            source=source,
            expected_config_failure=profile == "module_unreadable_include",
            cli_failure_reason=(cli_failure.reason if cli_failure else None),
            cli_album_present=cli_album_present,
            cli_items_present=cli_items_present,
            cli_files_present=_track_count(cli_album_dir),
            cli_import_sources_present=_track_count(cli_source_dir),
            cli_sibling_present=cli_sibling_present,
            child_returncode=child.returncode,
            child_stdout=child.stdout,
            child_outcome=outcome,
            child_album_present=child_album_present,
            child_items_present=child_items_present,
            child_files_present=_track_count(child_album_dir),
            child_import_sources_present=_track_count(child_source_dir),
            child_sibling_present=child_sibling_present,
        )


class TestGeneratedRealBeetsConfigMatrix(unittest.TestCase):
    def test_every_declared_common_config_cell(self) -> None:
        for profile in PROFILES:
            for track_count in (1, 2, 12):
                for source in ("mb", "discogs"):
                    with self.subTest(
                        profile=profile,
                        track_count=track_count,
                        source=source,
                    ):
                        assert_real_beets_contract(
                            exercise_real_beets_world(
                                profile, track_count, source,
                            ),
                        )

    @settings(suppress_health_check=[HealthCheck.too_slow])
    @example(profile="module_include", track_count=12, source="mb")
    @example(profile="module_unreadable_include", track_count=12, source="mb")
    @example(profile="importsource", track_count=2, source="discogs")
    @given(
        profile=st.sampled_from(PROFILES),
        track_count=st.sampled_from((1, 2, 12)),
        source=st.sampled_from(("mb", "discogs")),
    )
    def test_real_pinned_beets_common_config_worlds(
        self,
        profile: str,
        track_count: int,
        source: str,
    ) -> None:
        assert_real_beets_contract(
            exercise_real_beets_world(profile, track_count, source),
        )

    def test_checker_rejects_stdout_prefix_and_false_completion(self) -> None:
        valid = exercise_real_beets_world("minimal", 1, "mb")
        with self.assertRaisesRegex(AssertionError, "canonical outcome frame"):
            assert_real_beets_contract(replace(
                valid,
                child_stdout=b"plugin diagnostic\n" + valid.child_stdout,
            ))
        with self.assertRaisesRegex(AssertionError, "config rejection was promoted"):
            failed = exercise_real_beets_world(
                "module_unreadable_include", 1, "mb",
            )
            assert_real_beets_contract(replace(
                failed,
                child_outcome=valid.child_outcome,
                child_stdout=msgspec.json.encode(valid.child_outcome),
            ))


class TestDeleteChildFdProtocol(unittest.TestCase):
    def test_python_and_raw_fd_diagnostics_are_quarantined(self) -> None:
        code = r'''
import os
import harness.delete_album as child
from lib.beets_delete import BeetsDeleteFailed

def noisy(request):
    print("python stdout diagnostic", flush=True)
    os.write(1, b"raw fd1 diagnostic\n")
    return BeetsDeleteFailed(
        album_id=request.album_id,
        reason="configuration_error",
        detail="synthetic plugin failure",
        album_still_present=True,
    )

child.execute_pinned_beets_delete = noisy
child.main()
'''
        request = BeetsDeleteRequest(
            album_id=99,
            expected_release_id=MB_RELEASE,
            library_db_path="/nonexistent/library.db",
            library_root="/nonexistent/library",
        )
        proc = sp.run(
            [sys.executable, "-c", code],
            input=msgspec.json.encode(request),
            capture_output=True,
            cwd=REPO,
            timeout=10,
        )
        self.assertEqual(proc.returncode, 0, proc.stderr.decode(errors="replace"))
        outcome = msgspec.json.decode(proc.stdout, type=BeetsDeleteOutcome)
        self.assertEqual(proc.stdout, msgspec.json.encode(outcome))
        self.assertIn(b"python stdout diagnostic", proc.stderr)
        self.assertIn(b"raw fd1 diagnostic", proc.stderr)


if __name__ == "__main__":
    unittest.main()
