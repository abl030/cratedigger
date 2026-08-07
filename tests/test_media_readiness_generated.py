"""Generated metadata-mutant contract for issue #1062 media readiness."""

from __future__ import annotations

import shutil
import tempfile
import unittest
from pathlib import Path

from hypothesis import given, settings
from hypothesis import strategies as st

import tests._hypothesis_profiles  # noqa: F401
from lib.media_readiness import flac_total_samples_only_changed, prepare_media_readiness
from tests.audio_fixtures import make_test_flac
from tests.test_media_readiness import _zero_flac_duration_metadata


class TestFlacMetadataMutationProperty(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls._fixture_dir = tempfile.mkdtemp(prefix="media-readiness-fixture-")
        cls._fixture = Path(cls._fixture_dir) / "clean.flac"
        make_test_flac(str(cls._fixture), duration=1)

    @classmethod
    def tearDownClass(cls) -> None:
        shutil.rmtree(cls._fixture_dir)

    @settings(max_examples=8)
    @given(clear_md5=st.booleans(), clear_frame_sizes=st.booleans())
    def test_metadata_only_mutants_have_clean_stream_facts(
        self, *, clear_md5: bool, clear_frame_sizes: bool,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            clean_path = Path(tmp) / "clean.flac"
            broken_path = Path(tmp) / "broken.flac"
            shutil.copyfile(self._fixture, clean_path)
            shutil.copyfile(self._fixture, broken_path)
            clean = prepare_media_readiness(tmp)
            _zero_flac_duration_metadata(broken_path)
            if not clear_md5 or not clear_frame_sizes:
                raw = bytearray(broken_path.read_bytes())
                if not clear_md5:
                    raw[26:42] = self._fixture.read_bytes()[26:42]
                if not clear_frame_sizes:
                    raw[12:18] = self._fixture.read_bytes()[12:18]
                broken_path.write_bytes(raw)
            broken_bytes = broken_path.read_bytes()
            repaired = prepare_media_readiness(tmp)
            clean_facts = {Path(f.path).name: f for f in clean.files}
            repaired_facts = {Path(f.path).name: f for f in repaired.files}
            clean_fact = clean_facts["clean.flac"]
            repaired_fact = repaired_facts["broken.flac"]
            self.assertEqual(
                (
                    clean_fact.codec, clean_fact.container, clean_fact.sample_rate,
                    clean_fact.channels, clean_fact.bit_depth, clean_fact.sample_count,
                    clean_fact.duration_seconds, clean_fact.compressed_audio_bytes,
                    clean_fact.average_bitrate_kbps,
                ),
                (
                    repaired_fact.codec, repaired_fact.container, repaired_fact.sample_rate,
                    repaired_fact.channels, repaired_fact.bit_depth, repaired_fact.sample_count,
                    repaired_fact.duration_seconds, repaired_fact.compressed_audio_bytes,
                    repaired_fact.average_bitrate_kbps,
                ),
            )
            # The packet payload and every non-total-samples metadata byte
            # survive the property mutant and its repair exactly.
            self.assertTrue(
                flac_total_samples_only_changed(broken_bytes, broken_path.read_bytes()),
            )
