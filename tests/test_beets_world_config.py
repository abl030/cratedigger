"""Pins for the shared scratch-Beets shipped-config extraction (#743)."""

from __future__ import annotations

import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from lib.dispatch.subprocess_runner import run_import_one
from lib.util import beets_subprocess_env
from lib.beets_db import BeetsDB
from tests.beets_world import (
    BeetsWorld,
    build_subprocess_beets_config,
    extract_shipped_beets_world_config,
)


class TestShippedBeetsWorldConfig(unittest.TestCase):
    def test_extracts_load_bearing_shipped_import_contract(self) -> None:
        repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
        shipped = extract_shipped_beets_world_config(repo_root)

        self.assertIn("%aunique{albumartist album,path_disambig}", shipped.default_path_template)
        self.assertEqual(
            dict(shipped.album_fields),
            {
                "path_disambig": (
                    "albumdisambig or releasegroupdisambig or catalognum "
                    "or label or str(year)"
                ),
            },
        )
        self.assertEqual(
            set(shipped.duplicate_album_keys),
            {"mb_albumid", "discogs_albumid"},
        )

    def test_subprocess_config_is_disposable_exact_id_and_mirror_backed(self) -> None:
        repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
        shipped = extract_shipped_beets_world_config(repo_root)

        config = build_subprocess_beets_config(
            shipped,
            library_root=Path("/tmp/world/library"),
            library_db=Path("/tmp/world/library.db"),
            import_log=Path("/tmp/world/import.log"),
            mirror_url="http://mirror.invalid:5200",
        )

        self.assertEqual(config["directory"], "/tmp/world/library")
        self.assertEqual(config["library"], "/tmp/world/library.db")
        self.assertEqual(
            config["import"]["duplicate_keys"]["album"],
            ["mb_albumid", "discogs_albumid"],
        )
        self.assertEqual(config["plugins"], ["musicbrainz", "inline"])
        self.assertEqual(
            config["musicbrainz"],
            {"host": "mirror.invalid:5200", "https": False, "ratelimit": 100},
        )
        self.assertIn("%aunique", config["paths"]["default"])

    def test_subprocess_config_rejects_non_origin_mirror_urls(self) -> None:
        repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
        shipped = extract_shipped_beets_world_config(repo_root)

        with self.assertRaisesRegex(ValueError, "origin"):
            build_subprocess_beets_config(
                shipped,
                library_root=Path("/tmp/world/library"),
                library_db=Path("/tmp/world/library.db"),
                import_log=Path("/tmp/world/import.log"),
                mirror_url="http://mirror.invalid:5200/ws/2",
            )

    def test_subprocess_environment_never_reads_the_deployed_beets_db(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            deployed_runtime = Path(root) / "deployed-runtime.ini"
            deployed_runtime.write_text(
                "[Beets]\nconfig_dir = /deployed/config\n",
                encoding="utf-8",
            )
            repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
            world = BeetsWorld(
                repo_root,
                subprocess_mirror_url="http://mirror.invalid:5200",
            )
            self.addCleanup(world.close)
            with patch.dict(
                os.environ,
                {
                    "BEETSDIR": "/deployed/config",
                    "BEETS_DB": "/deployed/library.db",
                    "CRATEDIGGER_RUNTIME_CONFIG": str(deployed_runtime),
                },
            ):
                with world.subprocess_environment():
                    subprocess_env = beets_subprocess_env()
                    self.assertEqual(
                        subprocess_env["BEETSDIR"],
                        str(world.beets_config_dir),
                    )
                    self.assertEqual(
                        subprocess_env["BEETS_DB"],
                        str(world.library_db),
                    )
                    with BeetsDB() as beets:
                        self.assertEqual(beets.library_db_path, str(world.library_db))
                        self.assertEqual(beets.library_root, str(world.library_root))
                self.assertEqual(os.environ["BEETSDIR"], "/deployed/config")
                self.assertEqual(os.environ["BEETS_DB"], "/deployed/library.db")
                self.assertEqual(
                    os.environ["CRATEDIGGER_RUNTIME_CONFIG"],
                    str(deployed_runtime),
                )

    def test_explicit_runner_authority_survives_runtime_config_swap(self) -> None:
        """The default runner must use the pair Core snapshotted at launch."""
        repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
        with BeetsWorld(
            repo_root,
            subprocess_mirror_url="http://mirror.invalid:5200",
        ) as world, tempfile.TemporaryDirectory() as root:
            swapped = Path(root) / "swapped-runtime.ini"
            swapped.write_text(
                "[Beets]\n"
                "config_dir = /swapped/beets\n"
                "library = /swapped/library.db\n"
                "directory = /swapped/library\n"
                "python = /swapped/python\n",
                encoding="utf-8",
            )
            with patch.dict(os.environ, {
                "CRATEDIGGER_RUNTIME_CONFIG": str(swapped),
            }), patch("lib.dispatch.subprocess_runner.sp.run") as run:
                run.return_value.returncode = 1
                run.return_value.stdout = ""
                run.return_value.stderr = "expected test failure"
                # This call models the instant after dispatch has snapshotted
                # the original authority and before it launches import_one.
                run_import_one(
                    path="/scratch/source",
                    mb_release_id="aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
                    beets_harness_path="/scratch/harness/run_beets_harness.sh",
                    beets_config_dir=str(world.beets_config_dir),
                    beets_python="/original/pinned-python",
                    beets_library_db_path=str(world.library_db),
                    beets_library_root=str(world.library_root),
                )

            env = run.call_args.kwargs["env"]
            self.assertEqual(env["BEETSDIR"], str(world.beets_config_dir))
            self.assertEqual(env["BEETS_DB"], str(world.library_db))
            self.assertEqual(
                env["CRATEDIGGER_BEETS_PYTHON"], "/original/pinned-python",
            )


if __name__ == "__main__":
    unittest.main()
