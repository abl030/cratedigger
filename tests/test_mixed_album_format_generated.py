"""An unchanged album keeps its rank format across source and Beets roles."""

from __future__ import annotations

import os
import sqlite3
import subprocess
import tempfile
import unittest
from contextlib import closing
from dataclasses import replace
from pathlib import Path
from typing import IO
from unittest.mock import patch

from hypothesis import example, given
from hypothesis import strategies as st

import tests._hypothesis_profiles  # noqa: F401
from harness.import_one import (
    _detect_source_format,
    _materialize_quality_evidence_action,
)
from lib.beets_db import BeetsDB
from lib.quality import (
    AudioQualityMeasurement,
    ImportResult,
    QualityEvidenceActionPayload,
    QualityRankConfig,
    full_pipeline_decision_from_evidence,
)
from lib.quality_evidence import evidence_from_album_info, evidence_from_import_result
from tests.test_beets_db import _create_test_db

_EXTENSIONS = {
    "aac": "m4a", "mp3": "mp3", "opus": "opus", "vorbis": "ogg", "wmav2": "wma",
}
_BEETS_FORMATS = {
    "aac": "AAC", "mp3": "MP3", "opus": "Opus",
    "vorbis": "Ogg", "wmav2": "Windows Media",
}
_RANK_FAMILIES = ("aac", "mp3", "opus", "vorbis", "wma")


class TestMixedAlbumFormat(unittest.TestCase):
    def _assert_same_album_is_not_an_upgrade(
        self, codecs: list[str], precedence: tuple[str, ...] | None = None,
    ) -> None:
        cfg = QualityRankConfig.defaults()
        if precedence is not None:
            cfg = replace(cfg, mixed_format_precedence=precedence)
        with tempfile.TemporaryDirectory() as temporary:
            album = Path(temporary) / "album"
            album.mkdir()
            db_path = str(Path(temporary) / "beets.db")
            _create_test_db(db_path)
            paths: dict[str, str] = {}
            with closing(sqlite3.connect(db_path)) as connection:
                connection.execute(
                    "INSERT INTO albums (id, mb_albumid) VALUES (1, 'crowz')"
                )
                for index, codec in enumerate(codecs):
                    path = album / f"{index:02d}.{_EXTENSIONS[codec]}"
                    path.write_bytes(b"test audio")
                    paths[str(path)] = codec
                    connection.execute(
                        "INSERT INTO items (album_id, bitrate, path, format) "
                        "VALUES (1, 128000, ?, ?)",
                        (os.fsencode(path), _BEETS_FORMATS[codec]),
                    )
                connection.commit()

            def ffprobe(
                argv: list[str], *, stdout: IO[str] | None = None, **_kwargs: object,
            ) -> subprocess.CompletedProcess[str]:
                codec = paths[argv[-1]]
                if stdout is None:
                    return subprocess.CompletedProcess(
                        argv, 0, '{"streams":[{"codec_name":"' + codec + '"}]}',
                    )
                stdout.write(
                    f"index=0|codec_type=audio|codec_name={codec}"
                    "|sample_rate=48000|channels=2\n"
                    "stream_index=0|nb_samples=48000\n"
                    "stream_index=0|size=16000\n"
                    f"format_name={_EXTENSIONS[codec]}\n"
                )
                return subprocess.CompletedProcess(argv, 0)

            with patch("lib.media_readiness.subprocess.run", side_effect=ffprobe):
                source_format = _detect_source_format(str(album), cfg)
                candidate_result = evidence_from_import_result(
                    mb_release_id="crowz",
                    source_path=str(album),
                    import_result=ImportResult(
                        source_measurement=AudioQualityMeasurement(
                            min_bitrate_kbps=128,
                            avg_bitrate_kbps=128,
                            median_bitrate_kbps=128,
                            is_cbr=True,
                            format=source_format,
                        ),
                    ),
                )
                with BeetsDB(db_path) as beets:
                    album_info = beets.get_album_info("crowz", cfg)
                assert album_info is not None
                current_result = evidence_from_album_info(
                    mb_release_id="crowz", album_info=album_info,
                )
                candidate = candidate_result.evidence
                current = current_result.evidence
                assert candidate is not None, candidate_result
                assert current is not None, current_result
                decision = full_pipeline_decision_from_evidence(candidate, current, cfg=cfg)
                self.assertFalse(decision["imported"], decision)
                self.assertEqual(source_format.lower(), album_info.format.lower())
                output = ImportResult()
                _materialize_quality_evidence_action(
                    work_path=str(album),
                    payload=QualityEvidenceActionPayload(candidate=candidate),
                    r=output,
                )
                assert output.final_format is not None
                self.assertEqual(output.final_format.lower(), album_info.format.lower())

    def test_crowz_four_aac_then_sixteen_mp3_does_not_upgrade_itself(self) -> None:
        self._assert_same_album_is_not_an_upgrade(["aac"] * 4 + ["mp3"] * 16)

    def test_custom_precedence_reaches_source_and_current_projections(self) -> None:
        self._assert_same_album_is_not_an_upgrade(
            ["mp3", "aac"], ("aac", "mp3"),
        )

    def test_wma_aac_keeps_the_same_rank_after_install(self) -> None:
        self._assert_same_album_is_not_an_upgrade(["wmav2", "aac"])

    @given(
        codecs=st.lists(st.sampled_from(tuple(_EXTENSIONS)), min_size=1, max_size=8),
        precedence=st.one_of(st.none(), st.permutations(_RANK_FAMILIES).map(tuple)),
    )
    @example(codecs=["aac", "mp3"], precedence=None)
    @example(codecs=["opus", "mp3"], precedence=None)
    @example(codecs=["mp3", "aac"], precedence=("aac", "mp3"))
    @example(codecs=["wmav2", "aac"], precedence=None)
    def test_track_order_and_mixture_keep_source_current_and_output_consistent(
        self, codecs: list[str], precedence: tuple[str, ...] | None,
    ) -> None:
        self._assert_same_album_is_not_an_upgrade(codecs, precedence)


class TestMixedImportStagePrecedence(unittest.TestCase):
    @given(
        codecs=st.lists(st.sampled_from(tuple(_EXTENSIONS)), min_size=1, max_size=8),
        precedence=st.permutations(_RANK_FAMILIES).map(tuple),
    )
    @example(codecs=["mp3", "aac"], precedence=("aac", "mp3", "opus", "vorbis"))
    @example(codecs=["wmav2", "aac"], precedence=("wma", "aac", "mp3", "opus", "vorbis"))
    def test_serialized_precedence_reaches_the_import_source_measurement(
        self, codecs: list[str], precedence: tuple[str, ...],
    ) -> None:
        from tests.test_import_one_stages import run_native_import_measurement

        families = {"wma" if codec == "wmav2" else codec for codec in codecs}
        expected = next(codec for codec in precedence if codec in families).upper()
        result = run_native_import_measurement(codecs, precedence)
        self.assertEqual(result.error, "test child stopped before library writes")
        assert result.source_measurement is not None
        self.assertEqual(result.source_measurement.format, expected)
        self.assertEqual(result.source_measurement.avg_bitrate_kbps, 128)
        self.assertIsNone(result.target_quality_contract)


if __name__ == "__main__":
    unittest.main()
