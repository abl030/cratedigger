"""Runtime-config composition tests for the importer worker."""

from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from album_source import DatabaseSource
from lib.pipeline_db import PipelineDB
from scripts.importer import _build_runtime_context


class TestImporterRuntimeContext(unittest.TestCase):
    def test_database_source_receives_runtime_mirror_origins(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            config_path = Path(tmp) / "config.ini"
            config_path.write_text(
                "[MusicBrainz]\n"
                "api_base = http://musicbrainz-lxc.test:5200\n"
                "[Discogs]\n"
                "api_base = http://discogs-lxc.test:8086\n",
                encoding="utf-8",
            )
            db = PipelineDB.__new__(PipelineDB)
            db.dsn = "postgresql://runtime-config-test"

            with patch.dict(
                os.environ,
                {"CRATEDIGGER_RUNTIME_CONFIG": str(config_path)},
            ):
                ctx = _build_runtime_context(db)

        source = ctx.pipeline_db_source
        self.assertIsInstance(source, DatabaseSource)
        assert isinstance(source, DatabaseSource)
        self.assertEqual(
            source.musicbrainz_ws2_base,
            "http://musicbrainz-lxc.test:5200/ws/2",
        )
        self.assertEqual(
            source.discogs_api_base,
            "http://discogs-lxc.test:8086",
        )


if __name__ == "__main__":
    unittest.main()
