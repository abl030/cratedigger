"""Generated guards for current beets-library quality projections."""

from __future__ import annotations

import os
import sqlite3
import tempfile
import unittest

from hypothesis import given, strategies as st

import tests._hypothesis_profiles  # noqa: F401 - registers suite/push/fuzz
from lib.banding import (
    band_from_detail,
    compute_library_rank,
    current_library_bitrate,
)
from lib.beets_db import BeetsDB
from lib.quality import QualityRankConfig
from tests.test_beets_db import _create_test_db, _insert_album


def assert_positive_track_average_projection(
    *,
    bitrates_bps: list[int],
    detail: dict[str, object],
    selected_kbps: int,
    rank: str,
) -> None:
    """Current projection and rank must use the positive-track average."""
    positive = [value for value in bitrates_bps if value > 0]
    expected_min = min(positive) // 1000
    expected_avg = int((sum(positive) / len(positive)) / 1000)
    assert detail["beets_bitrate"] == expected_min
    assert detail["beets_avg_bitrate"] == expected_avg
    assert selected_kbps == expected_avg
    expected_rank = compute_library_rank(
        "MP3", expected_avg, QualityRankConfig.defaults()
    )
    assert rank == expected_rank


class TestInvariantCheckersTripOnViolations(unittest.TestCase):
    def test_positive_track_average_checker_rejects_min_selected_mutant(self) -> None:
        bitrates = [194_000, 320_000, 350_000]
        detail = {
            "beets_format": "MP3",
            "beets_bitrate": 194,
            "beets_avg_bitrate": 288,
        }
        with self.assertRaises(AssertionError):
            assert_positive_track_average_projection(
                bitrates_bps=bitrates,
                detail=detail,
                selected_kbps=194,
                rank=compute_library_rank(
                    "MP3", 194, QualityRankConfig.defaults()
                ),
            )


class TestCurrentLibraryQualityGenerated(unittest.TestCase):
    def setUp(self) -> None:
        self._tmpdir = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self._tmpdir.name, "beets.db")
        _create_test_db(self.db_path)
        _insert_album(
            self.db_path,
            1,
            "generated-release",
            [(320_000, "/music/initial.mp3")],
        )
        self.writer = sqlite3.connect(self.db_path)
        self.writer.execute("PRAGMA synchronous = OFF")
        self.db = BeetsDB(self.db_path)

    def tearDown(self) -> None:
        self.db.close()
        self.writer.close()
        self._tmpdir.cleanup()

    @given(
        st.integers(min_value=1_000, max_value=1_600_000),
        st.lists(
            st.integers(min_value=0, max_value=1_600_000),
            min_size=0,
            max_size=16,
        ),
    )
    def test_positive_track_average_drives_projection_and_rank(
        self, first_positive_bps: int, remaining_bps: list[int]
    ) -> None:
        bitrates_bps = [first_positive_bps, *remaining_bps]
        self.writer.execute("DELETE FROM items WHERE album_id = 1")
        self.writer.executemany(
            "INSERT INTO items (album_id, bitrate, path, format) "
            "VALUES (1, ?, ?, 'MP3')",
            [
                (bitrate, f"/music/{index:02d}.mp3")
                for index, bitrate in enumerate(bitrates_bps, start=1)
            ],
        )
        self.writer.commit()
        detail = self.db.check_mbids_detail(["generated-release"])[
            "generated-release"
        ]

        selected = current_library_bitrate(detail)
        rank = band_from_detail(
            "generated-release",
            {"generated-release"},
            {"generated-release": detail},
            QualityRankConfig.defaults(),
        )
        assert_positive_track_average_projection(
            bitrates_bps=bitrates_bps,
            detail=detail,
            selected_kbps=selected,
            rank=rank,
        )


if __name__ == "__main__":
    unittest.main()
