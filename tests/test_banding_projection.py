"""The badge and the decision rank come from ONE projection (issue #1145 F5).

``album_info_from_current`` is what the importer ranks against. Both operator
badge surfaces must read the same answer:

* the long-tail worklist / CLI, through ``band_current_resolutions``;
* the browse and label overlays, through ``check_mbids_detail`` →
  ``band_from_detail``.

Before this issue both re-derived the aggregates beside the projection, and
that drifted twice: first by a kbps at a band edge (#1144's rounding), then —
once ``album_info_from_current`` began minting an ``mp3 vN`` contract from the
items' LAME tags — by up to three tiers on a contract-bearing album, where the
badge said ``good`` and the importer said ``transparent``.

The worlds here are real Beets rows in a real SQLite library with the column
Beets actually populates, not hand-built ``AlbumInfo`` objects: the whole point
is that the two surfaces read the SAME source the decision does.
"""

from __future__ import annotations

import os
import shutil
import sqlite3
import tempfile
import unittest

from lib.banding import band_from_detail, resolve_current_release_bands
from lib.beets_db import BeetsDB
from lib.quality import QualityRankConfig, measurement_rank
from lib.quality.evidence_types import AudioQualityMeasurement

CFG = QualityRankConfig.defaults()


def _create_beets_db(path: str) -> None:
    conn = sqlite3.connect(path)
    conn.executescript("""
        CREATE TABLE albums (
            id INTEGER PRIMARY KEY,
            mb_albumid TEXT,
            discogs_albumid INTEGER
        );
        CREATE TABLE items (
            id INTEGER PRIMARY KEY,
            album_id INTEGER,
            path BLOB,
            title TEXT,
            track INTEGER,
            disc INTEGER,
            length REAL,
            format TEXT,
            bitrate INTEGER,
            samplerate INTEGER,
            bitdepth INTEGER,
            encoder_settings TEXT
        );
    """)
    conn.commit()
    conn.close()


class TestBothBadgeSurfacesReadTheDecisionProjection(unittest.TestCase):
    MBID = "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee"

    def setUp(self) -> None:
        self.tmpdir = tempfile.mkdtemp()
        self.db_path = os.path.join(self.tmpdir, "beets.db")
        self.library_root = os.path.join(self.tmpdir, "Music")
        os.makedirs(self.library_root, exist_ok=True)
        _create_beets_db(self.db_path)

    def tearDown(self) -> None:
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def _seed(self, rows: list[tuple[str, int, str | None]]) -> None:
        conn = sqlite3.connect(self.db_path)
        conn.execute(
            "INSERT INTO albums (id, mb_albumid, discogs_albumid) "
            "VALUES (1, ?, NULL)",
            (self.MBID,),
        )
        for index, (fmt, bitrate, settings) in enumerate(rows, start=1):
            path = os.path.join(
                self.library_root, "Artist", "Album", f"{index:02d}.mp3")
            conn.execute(
                "INSERT INTO items (id, album_id, path, title, track, disc, "
                "length, format, bitrate, samplerate, bitdepth, "
                "encoder_settings) VALUES (?, 1, ?, ?, ?, 1, 100.0, ?, "
                "?, 44100, NULL, ?)",
                (index, path.encode(), f"Track {index}", index, fmt,
                 bitrate, settings),
            )
        conn.commit()
        conn.close()

    def _both_bands(self) -> tuple[str, str, str]:
        """(long-tail band, browse band, the decision's own rank)."""
        with BeetsDB(self.db_path, library_root=self.library_root) as db:
            long_tail = resolve_current_release_bands(db, [self.MBID], CFG)
            detail = db.check_mbids_detail([self.MBID], CFG)
            info = db.get_album_info(self.MBID, CFG)
        browse = band_from_detail(self.MBID, {self.MBID}, detail, CFG)
        assert info is not None
        decision = measurement_rank(
            AudioQualityMeasurement(
                min_bitrate_kbps=info.min_bitrate_kbps,
                avg_bitrate_kbps=info.avg_bitrate_kbps,
                median_bitrate_kbps=info.median_bitrate_kbps,
                format=info.format,
                is_cbr=info.is_cbr,
            ),
            CFG,
        ).name.lower()
        return long_tail[self.MBID], browse, decision

    def test_a_proven_v0_album_bands_transparent_on_both_surfaces(self) -> None:
        """The F5 regression, in the shape that produced it.

        245 kbps of measured MP3 is ``good`` on the one ladder. These items
        prove ``-V 0``, so the decision ranks them ``transparent`` — and the
        badge must not say otherwise on either surface.
        """
        self._seed([("MP3", 245_000, "-V 0"), ("MP3", 245_000, "-V 0")])
        long_tail, browse, decision = self._both_bands()
        self.assertEqual(decision, "transparent")
        self.assertEqual(long_tail, "transparent")
        self.assertEqual(browse, "transparent")

    def test_the_same_bitrate_without_a_contract_bands_good(self) -> None:
        """Must-still-work: the badge is not simply pinned to transparent.

        Identical measured audio, no LAME tag: both surfaces read ``good``,
        the same answer the decision gives. Without this arm the pin above
        would pass against a projection that promoted everything.
        """
        self._seed([("MP3", 245_000, None), ("MP3", 245_000, None)])
        long_tail, browse, decision = self._both_bands()
        self.assertEqual(decision, "good")
        self.assertEqual(long_tail, "good")
        self.assertEqual(browse, "good")

    def test_a_sub_kilobit_average_rounds_the_same_way_on_both(self) -> None:
        """The #1144 half of the same invariant, at a band edge.

        Two tracks averaging 255,600 bps: floored that is 255 (``good``),
        rounded 256 (``excellent``). Both surfaces must read the projection's
        rounded answer.
        """
        self._seed([("MP3", 255_600, None), ("MP3", 255_600, None)])
        long_tail, browse, decision = self._both_bands()
        self.assertEqual(decision, "excellent")
        self.assertEqual(long_tail, "excellent")
        self.assertEqual(browse, "excellent")

    def test_a_mixed_codec_album_reduces_the_same_way_on_both(self) -> None:
        """``mixed_format_precedence`` reaches both surfaces.

        Browse used to band on the first entry of a comma-joined format list
        (``"FLAC,MP3"`` → ``FLAC``), which ignored the precedence tuple
        entirely and read this album as ``lossless``. Worst-codec-first
        reduces it to MP3, so it bands on the MP3 table — ``transparent``
        here only because the FLAC track drags the mean to 550 kbps. The
        load-bearing part is that it is not ``lossless``, and that all three
        agree.
        """
        self._seed([("FLAC", 900_000, None), ("MP3", 200_000, None)])
        long_tail, browse, decision = self._both_bands()
        self.assertNotEqual(decision, "lossless")
        self.assertEqual(decision, "transparent")
        self.assertEqual(long_tail, "transparent")
        self.assertEqual(browse, "transparent")

    def _both_bands_without_a_projection(self) -> tuple[str, str]:
        """(long-tail band, browse band) for an album that cannot project.

        Separate from ``_both_bands`` because there is no ``AlbumInfo`` to
        rank against here — the decision path has nothing to say, so the
        contract under test is that the two BADGES still agree.
        """
        with BeetsDB(self.db_path, library_root=self.library_root) as db:
            long_tail = resolve_current_release_bands(db, [self.MBID], CFG)
            detail = db.check_mbids_detail([self.MBID], CFG)
            self.assertIsNone(db.get_album_info(self.MBID, CFG))
        return (
            long_tail[self.MBID],
            band_from_detail(self.MBID, {self.MBID}, detail, CFG),
        )

    def test_a_bitrate_less_album_keeps_its_codec_only_band(self) -> None:
        """Must-still-work: no projection, but the codec still ranks.

        ``album_info_from_current`` returns nothing for an album whose items
        carry no usable bitrate. A FLAC album is still ``lossless`` on
        identity alone, and there is no contract to miss — a contract is
        minted from measured items and there are none.

        Asserted on BOTH surfaces. Routing ``beets_format`` through the
        projection without this fallback made browse read ``unknown`` while
        the worklist still read ``lossless`` — a divergence introduced by
        the very change that exists to remove one, and invisible to a pin
        that checks only the worklist.
        """
        self._seed([("FLAC", 0, None)])
        long_tail, browse = self._both_bands_without_a_projection()
        self.assertEqual(long_tail, "lossless")
        self.assertEqual(browse, "lossless")

    def test_a_bitrate_less_mp3_bands_the_same_way_on_both(self) -> None:
        """The lossy arm of the same fallback.

        Without it the two surfaces disagreed here too (``poor`` vs
        ``unknown``), so pinning only the FLAC case would leave the
        codec-only reduction itself unproven.
        """
        self._seed([("MP3", 0, None)])
        long_tail, browse = self._both_bands_without_a_projection()
        self.assertEqual(long_tail, "poor")
        self.assertEqual(browse, "poor")


if __name__ == "__main__":
    unittest.main()
