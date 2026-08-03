"""Generated patrol for strict raw PostgreSQL evidence decoding (#999)."""

from __future__ import annotations

import unittest
from copy import deepcopy

from hypothesis import given
from hypothesis import strategies as st

import tests._hypothesis_profiles  # noqa: F401
import tests.test_evidence_decoder as evidence_decoder
from tests.test_pipeline_db import requires_postgres


@requires_postgres
class TestStrictEvidenceDecoderGenerated(unittest.TestCase):
    """Generated mutants drive the raw adapter used by both live loads."""

    def setUp(self) -> None:
        self._case = evidence_decoder.TestStrictEvidenceDecoder()
        self._case.setUp()

    def tearDown(self) -> None:
        self._case.tearDown()

    @given(
        mutation=st.sampled_from((
            "aac_unknown",
            "audio_unknown",
            "diagnostic_unknown",
            "file_unknown",
            "wrong_bool",
            "null_required",
        )),
    )
    def test_every_nested_raw_pg_mutation_fails_closed(self, mutation: str) -> None:
        """The shared typed adapter never normalizes a malformed PG world."""
        _expected, loaded = self._case._stored()
        assert loaded.id is not None
        row, files = self._case._raw_rows(loaded.id)
        bad_row, bad_files = deepcopy(row), deepcopy(files)
        if mutation == "aac_unknown":
            tracks = bad_row["aac_lattice_tracks"]
            assert isinstance(tracks, list) and isinstance(tracks[0], dict)
            tracks[0]["unknown"] = True
        elif mutation == "audio_unknown":
            audio = bad_row["audio_validation"]
            assert isinstance(audio, dict)
            audio["unknown"] = True
        elif mutation == "diagnostic_unknown":
            audio = bad_row["audio_validation"]
            assert isinstance(audio, dict)
            diagnostics = audio["diagnostics"]
            assert isinstance(diagnostics, list) and isinstance(diagnostics[0], dict)
            diagnostics[0]["unknown"] = True
        elif mutation == "file_unknown":
            bad_files[0]["unknown"] = True
        elif mutation == "wrong_bool":
            bad_row["is_cbr"] = "false"
        else:
            bad_row["id"] = None

        evidence_decoder.assert_raw_pg_decoder_rejects(bad_row, bad_files)

    def test_decoder_checker_trips_on_a_known_bad_checker_mutant(self) -> None:
        """Qualification: a valid PG projection must not satisfy the checker."""
        _expected, loaded = self._case._stored()
        assert loaded.id is not None
        row, files = self._case._raw_rows(loaded.id)
        with self.assertRaises(AssertionError):
            evidence_decoder.assert_raw_pg_decoder_rejects(row, files)
