"""MP3 VBR quality contracts minted from real LAME headers (issue #1145 A).

Deterministic half of the invariant pair; the properties live in
``tests/test_mp3_ladder_generated.py``.

Every positive fixture here is produced by the REAL encoder (``lame``, in the
dev shell for exactly this reason) or by the real Beets column, never by a
hand-typed settings string. That is test-fidelity Rule C applied to an input:
a contract minted from a literal nobody can emit would be a green test for a
world that does not exist. The negative fixtures matter just as much — 4,331
of the 10,036 live MP3 items carry no encoder-settings string at all, and
ffmpeg's own ``libmp3lame`` writes a bare Xing header with no encoder
settings, so both "unreadable" shapes below are real populations, not invented
ones.
"""

from __future__ import annotations

import os
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from typing import ClassVar

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from lib.beets_db import BeetsDB, album_info_from_current
from lib.media_readiness import folder_mp3_encoder_settings, mp3_encoder_settings
from lib.quality import (
    AudioQualityMeasurement,
    QualityRank,
    QualityRankConfig,
    compare_quality,
    import_quality_decision,
    lame_vbr_level,
    measurement_rank,
    mp3_vbr_contract_format,
    mp3_vbr_contract_level,
)

CFG = QualityRankConfig.defaults()


def _encode_tone(root: Path, name: str, *lame_args: str) -> Path:
    """Encode one second of tone with the REAL LAME encoder."""
    wav = root / f"{name}.wav"
    if not wav.exists():
        subprocess.run(
            [
                "ffmpeg", "-y", "-f", "lavfi",
                "-i", "sine=frequency=440:duration=2", str(wav),
                "-loglevel", "error",
            ],
            check=True,
        )
    out = root / f"{name}.mp3"
    subprocess.run(
        ["lame", "--quiet", *lame_args, str(wav), str(out)], check=True,
    )
    wav.unlink()
    return out


class TestLameVbrLevelParsing(unittest.TestCase):
    """Every settings string the live library actually carries.

    The strings are the exact ``items.encoder_settings`` values measured on
    the 10,036 live MP3 items (issue #1145's investigation), so this table is
    a census, not a guess. ``--preset standard`` is deliberately UNMAPPED: it
    is the same encoding as ``--alt-preset standard`` and LAME 3.93 wrote it
    on 31 live items, but the operator enumerated exactly one preset alias and
    "fail closed — never guess a V level" governs the rest.
    """

    CASES: ClassVar = [
        ("plain -V 0", "-V 0", 0),
        ("plain -V 2", "-V 2", 2),
        ("-V 0 with --vbr-new", "-V 0 --vbr-new", 0),
        ("-V 2 with --vbr-new", "-V 2 --vbr-new", 2),
        ("-V 0 with --vbr-old", "-V 0 --vbr-old", 0),
        ("-V 1", "-V 1", 1),
        ("-V 4", "-V 4", 4),
        ("-V 5", "-V 5", 5),
        ("-V 9", "-V 9", 9),
        ("no space after the flag", "-V0", 0),
        ("the one mapped preset", "--alt-preset standard", 2),
        ("mapped preset, odd spacing", "  --alt-preset   standard ", 2),
        ("CBR -b 320", "-b 320", None),
        ("CBR -b 192", "-b 192", None),
        ("CBR -b 255+", "-b 255+", None),
        ("ABR", "--abr 255+", None),
        ("--preset standard is not mapped", "--preset standard", None),
        ("--preset insane", "--preset insane", None),
        ("--preset extreme", "--preset extreme", None),
        ("--alt-preset extreme", "--alt-preset extreme", None),
        ("--alt-preset numeric", "--alt-preset 246", None),
        ("--preset numeric", "--preset 240", None),
        ("blank", "", None),
        ("whitespace only", "   ", None),
        ("absent", None, None),
        # LAME's CLI shorthand for -V 4, which mutagen never writes into
        # the tag. Reading a level out of it would be a guess.
        ("lower-case -v carries no explicit level", "-v 0", None),
        ("two-digit level is unparsed, never truncated", "-V 10", None),
    ]

    def test_level_table(self) -> None:
        for desc, settings, expected in self.CASES:
            with self.subTest(desc=desc):
                self.assertEqual(lame_vbr_level(settings), expected)


class TestAlbumContractMinting(unittest.TestCase):
    """Unanimity or nothing."""

    CASES: ClassVar = [
        ("unanimous V0", ["-V 0", "-V 0", "-V 0"], "mp3 v0"),
        ("unanimous V2 across spellings",
         ["-V 2", "-V 2 --vbr-new", "--alt-preset standard"], "mp3 v2"),
        ("one file disagrees", ["-V 0", "-V 0", "-V 2"], None),
        ("one file untagged", ["-V 0", None, "-V 0"], None),
        ("one file is CBR", ["-V 0", "-b 320"], None),
        ("all untagged", [None, None], None),
        ("all CBR", ["-b 320", "-b 320"], None),
        ("empty fileset", [], None),
        ("caller could not establish an all-MP3 fileset", None, None),
    ]

    def test_mint_table(self) -> None:
        for desc, settings, expected in self.CASES:
            with self.subTest(desc=desc):
                self.assertEqual(mp3_vbr_contract_format(settings), expected)

    def test_minted_label_reads_back_as_its_own_level(self) -> None:
        """The writer and the reader share one vocabulary."""
        for level in range(10):
            with self.subTest(level=level):
                label = mp3_vbr_contract_format([f"-V {level}"] * 2)
                assert label is not None
                self.assertEqual(mp3_vbr_contract_level(label), level)


class TestRealEncoderProducesTheContract(unittest.TestCase):
    """The producer end: a real LAME encode, read back through production."""

    @classmethod
    def setUpClass(cls) -> None:
        cls._tmp = tempfile.mkdtemp(prefix="cratedigger-lame-contract-")

    @classmethod
    def tearDownClass(cls) -> None:
        shutil.rmtree(cls._tmp, ignore_errors=True)

    def _album(self, name: str) -> Path:
        album = Path(self._tmp) / name
        album.mkdir(parents=True, exist_ok=True)
        return album

    def test_real_v0_encode_mints_the_v0_contract(self) -> None:
        album = self._album("v0")
        for track in ("01", "02"):
            _encode_tone(album, track, "-V", "0")

        settings = folder_mp3_encoder_settings(str(album))
        assert settings is not None
        self.assertEqual(settings, ["-V 0", "-V 0"])
        self.assertEqual(mp3_vbr_contract_format(settings), "mp3 v0")

    def test_real_v2_encode_mints_the_v2_contract(self) -> None:
        album = self._album("v2")
        _encode_tone(album, "01", "-V", "2")

        self.assertEqual(
            mp3_vbr_contract_format(folder_mp3_encoder_settings(str(album))),
            "mp3 v2",
        )

    def test_real_cbr_encode_mints_nothing(self) -> None:
        album = self._album("cbr")
        _encode_tone(album, "01", "-b", "320")

        settings = folder_mp3_encoder_settings(str(album))
        assert settings is not None
        self.assertEqual(settings, ["-b 320"])
        self.assertIsNone(mp3_vbr_contract_format(settings))

    def test_a_mixed_v_level_album_mints_nothing(self) -> None:
        album = self._album("mixed-levels")
        _encode_tone(album, "01", "-V", "0")
        _encode_tone(album, "02", "-V", "2")

        self.assertIsNone(
            mp3_vbr_contract_format(folder_mp3_encoder_settings(str(album))))

    def test_ffmpeg_libmp3lame_writes_no_contract(self) -> None:
        """The untagged-library case, from a real encoder that omits the tag.

        ffmpeg's ``libmp3lame`` emits a bare Xing header, so mutagen reports
        no encoder settings at all. That is the same shape as the 4,331 live
        MP3 items with a blank ``encoder_settings`` column, and it must
        withhold the contract rather than fall back to a mode guess.
        """
        album = self._album("ffmpeg")
        out = album / "01.mp3"
        subprocess.run(
            [
                "ffmpeg", "-y", "-f", "lavfi",
                "-i", "sine=frequency=440:duration=1",
                "-c:a", "libmp3lame", "-q:a", "0", str(out),
                "-loglevel", "error",
            ],
            check=True,
        )

        self.assertIsNone(mp3_encoder_settings(out))
        self.assertEqual(folder_mp3_encoder_settings(str(album)), [None])
        self.assertIsNone(
            mp3_vbr_contract_format(folder_mp3_encoder_settings(str(album))))

    def test_a_non_mp3_file_in_the_folder_withholds_the_whole_album(self) -> None:
        album = self._album("mixed-codec")
        _encode_tone(album, "01", "-V", "0")
        flac = album / "02.flac"
        subprocess.run(
            [
                "ffmpeg", "-y", "-f", "lavfi",
                "-i", "sine=frequency=440:duration=1", str(flac),
                "-loglevel", "error",
            ],
            check=True,
        )

        self.assertIsNone(folder_mp3_encoder_settings(str(album)))

    def test_an_empty_folder_withholds_the_contract(self) -> None:
        self.assertIsNone(
            folder_mp3_encoder_settings(str(self._album("empty"))))

    def test_a_missing_folder_withholds_the_contract(self) -> None:
        self.assertIsNone(
            folder_mp3_encoder_settings(
                os.path.join(self._tmp, "does-not-exist")))

    def test_harness_source_format_mints_the_contract_for_a_v0_download(
        self,
    ) -> None:
        """The candidate side end to end, through the harness's own helper."""
        from harness.import_one import _detect_source_format

        album = self._album("harness-v0")
        _encode_tone(album, "01", "-V", "0")
        self.assertEqual(_detect_source_format(str(album)), "mp3 v0")

    def test_harness_source_format_keeps_the_bare_codec_without_a_tag(
        self,
    ) -> None:
        """Must-still-work: an untagged MP3 keeps the codec it always had."""
        from harness.import_one import _detect_source_format

        album = self._album("harness-bare")
        out = album / "01.mp3"
        subprocess.run(
            [
                "ffmpeg", "-y", "-f", "lavfi",
                "-i", "sine=frequency=440:duration=1",
                "-c:a", "libmp3lame", "-q:a", "0", str(out),
                "-loglevel", "error",
            ],
            check=True,
        )
        self.assertEqual(_detect_source_format(str(album)), "MP3")


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


class TestBeetsMintsTheContractFromItsOwnColumn(unittest.TestCase):
    """The installed side, against a real Beets-shaped SQLite library.

    This is the symmetric half of the harness test above: identical audio must
    carry the identical contract on both sides of a comparison, or the ladder
    collapse would rank a re-download of an installed album above itself.
    """

    MBID = "11111111-2222-3333-4444-555555555555"

    def setUp(self) -> None:
        self.tmpdir = tempfile.mkdtemp()
        self.db_path = os.path.join(self.tmpdir, "beets.db")
        self.library_root = os.path.join(self.tmpdir, "Music")
        os.makedirs(self.library_root, exist_ok=True)
        _create_beets_db(self.db_path)

    def tearDown(self) -> None:
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def _seed(self, rows: list[tuple[int, str | None]]) -> None:
        conn = sqlite3.connect(self.db_path)
        conn.execute(
            "INSERT INTO albums (id, mb_albumid, discogs_albumid) "
            "VALUES (1, ?, NULL)",
            (self.MBID,),
        )
        for index, (bitrate, settings) in enumerate(rows, start=1):
            path = os.path.join(
                self.library_root, "Artist", "Album", f"{index:02d}.mp3")
            conn.execute(
                "INSERT INTO items (id, album_id, path, title, track, disc, "
                "length, format, bitrate, samplerate, bitdepth, "
                "encoder_settings) VALUES (?, 1, ?, ?, ?, 1, 100.0, 'MP3', "
                "?, 44100, NULL, ?)",
                (index, path.encode(), f"Track {index}", index, bitrate,
                 settings),
            )
        conn.commit()
        conn.close()

    def _album_format(self) -> str:
        with BeetsDB(self.db_path, library_root=self.library_root) as db:
            info = db.get_album_info(self.MBID, CFG)
        assert info is not None
        return info.format

    def test_unanimous_lame_v0_items_mint_the_contract(self) -> None:
        self._seed([(245_000, "-V 0"), (250_000, "-V 0 --vbr-new")])
        self.assertEqual(self._album_format(), "mp3 v0")

    def test_the_mapped_preset_mints_v2(self) -> None:
        self._seed([(190_000, "--alt-preset standard")] * 2)
        self.assertEqual(self._album_format(), "mp3 v2")

    def test_one_untagged_item_withholds_the_whole_album(self) -> None:
        self._seed([(245_000, "-V 0"), (250_000, None)])
        self.assertEqual(self._album_format(), "MP3")

    def test_disagreeing_levels_withhold_the_contract(self) -> None:
        self._seed([(245_000, "-V 0"), (190_000, "-V 2")])
        self.assertEqual(self._album_format(), "MP3")

    def test_cbr_items_keep_the_bare_codec(self) -> None:
        self._seed([(320_000, "-b 320")] * 2)
        self.assertEqual(self._album_format(), "MP3")

    def test_an_unmeasured_item_cannot_veto_the_contract(self) -> None:
        """Only items that contribute a bitrate contribute a level.

        ``album_info_from_current`` derives every aggregate from the
        positive-bitrate items; a zero-bitrate row is not part of the album's
        measurement and must not be part of its contract either.
        """
        self._seed([(245_000, "-V 0"), (0, None)])
        self.assertEqual(self._album_format(), "mp3 v0")


class TestTheMintIsGatedOnTheReducedCodec(unittest.TestCase):
    """Only an album that reduces to MP3 may carry an MP3 contract.

    Reachable through an ordinary config change, not a hypothetical: a mixed
    FLAC+MP3 album reduces by ``mixed_format_precedence``, and an operator who
    reorders that tuple to prefer FLAC gets a FLAC-reduced album whose MP3
    items still carry ``-V 0``. Without the gate those items would mint
    ``mp3 v0`` for a lossless album and demote it out of LOSSLESS entirely.
    """

    @staticmethod
    def _mixed_album():
        from lib.beets_db import CurrentBeetsItem, CurrentBeetsUnique
        from lib.release_identity import ReleaseIdentity

        identity = ReleaseIdentity(
            source="musicbrainz",
            release_id="abcdef01-2345-6789-abcd-ef0123456789",
        )
        return CurrentBeetsUnique(
            identity=identity,
            album_id=3,
            album_path="/nonexistent/Mixed",
            items=(
                CurrentBeetsItem(
                    id=1, path="/nonexistent/Mixed/01.flac",
                    format="FLAC", bitrate=900_000, encoder_settings=None),
                CurrentBeetsItem(
                    id=2, path="/nonexistent/Mixed/02.mp3",
                    format="MP3", bitrate=245_000, encoder_settings="-V 0"),
            ),
            selectors=(),
        )

    def test_a_flac_reduced_mixed_album_never_mints_an_mp3_contract(
        self,
    ) -> None:
        from dataclasses import replace

        flac_first = replace(
            CFG,
            mixed_format_precedence=("flac", "mp3", "vorbis", "aac", "opus"),
        )
        info = album_info_from_current(self._mixed_album(), flac_first)
        assert info is not None
        self.assertEqual(info.format, "FLAC")
        self.assertEqual(
            measurement_rank(
                AudioQualityMeasurement(
                    min_bitrate_kbps=245, avg_bitrate_kbps=245,
                    median_bitrate_kbps=245, format=info.format,
                ),
                flac_first,
            ),
            QualityRank.LOSSLESS,
        )

    def test_the_codec_gate_alone_stops_a_unanimous_non_mp3_album(self) -> None:
        """The gate at its own seam, with unanimity deliberately satisfied.

        Stated honestly, and measured: across the whole live library every
        non-MP3 item's ``items.encoder_settings`` is blank (AAC, ALAC, FLAC,
        OGG, Opus and WMA — 83,621 items, zero populated), because the column
        comes from the LAME tag. So no live album can reduce to a non-MP3
        codec while every measured item reports a ``-V`` level. The world
        below is therefore built directly against the module-level helper
        rather than through Beets — this clause is fail-closed legislation for
        whatever writer fills that column next, and legislation still has to
        be shown to work. Without it a FLAC album would be labelled
        ``mp3 v0`` and drop out of LOSSLESS.
        """
        from lib.beets_db import CurrentBeetsItem, _mp3_contract_or_codec

        items = [
            (CurrentBeetsItem(
                id=index, path=f"/nonexistent/{index}.flac",
                format="FLAC", bitrate=900_000, encoder_settings="-V 0"),
             900_000)
            for index in (1, 2)
        ]
        # Unanimity is satisfied: only the reduced-codec gate can withhold.
        self.assertEqual(
            mp3_vbr_contract_format(
                item.encoder_settings for item, _bitrate in items),
            "mp3 v0",
        )
        self.assertEqual(_mp3_contract_or_codec("FLAC", items), "FLAC")
        # ...and the same items under an MP3 reduction do mint, so the test
        # is discriminating between the two, not just asserting a constant.
        self.assertEqual(_mp3_contract_or_codec("MP3", items), "mp3 v0")

    def test_the_same_album_reduced_to_mp3_still_withholds(self) -> None:
        """Must-still-work: the DEFAULT worst-codec reduction picks MP3 here,
        and unanimity then withholds anyway because the FLAC item carries no
        level. Two independent gates, both live."""
        info = album_info_from_current(self._mixed_album(), CFG)
        assert info is not None
        self.assertEqual(info.format, "MP3")


class TestContractChangesTheDecidedOutcome(unittest.TestCase):
    """The consequence, not a proxy: the contract flips import vs reject.

    Identical measured audio at 245 kbps against an installed bare MP3 at
    300. Bare, the candidate is GOOD under the one MP3 ladder and loses to
    the installed EXCELLENT; with its proven ``-V 0`` contract it is
    TRANSPARENT and wins. This is the two-tier gap the scope ordering
    (A before B) exists to close, asserted as the decided outcome rather
    than as a rank field.
    """

    EXISTING = AudioQualityMeasurement(
        min_bitrate_kbps=300, avg_bitrate_kbps=300,
        median_bitrate_kbps=300, format="MP3",
    )

    def _candidate(self, format_hint: str) -> AudioQualityMeasurement:
        return AudioQualityMeasurement(
            min_bitrate_kbps=245, avg_bitrate_kbps=245,
            median_bitrate_kbps=245, format=format_hint,
        )

    def test_bare_measured_245_is_a_downgrade(self) -> None:
        bare = self._candidate("MP3")
        self.assertEqual(measurement_rank(bare, CFG), QualityRank.GOOD)
        self.assertEqual(
            compare_quality(bare, self.EXISTING, CFG).verdict, "worse")
        self.assertEqual(
            import_quality_decision(bare, self.EXISTING, cfg=CFG).decision,
            "downgrade",
        )

    def test_the_same_audio_with_a_minted_contract_imports(self) -> None:
        contract = mp3_vbr_contract_format(["-V 0"] * 3)
        assert contract is not None
        proven = self._candidate(contract)
        self.assertEqual(measurement_rank(proven, CFG), QualityRank.TRANSPARENT)
        self.assertEqual(
            compare_quality(proven, self.EXISTING, CFG).verdict, "better")
        self.assertEqual(
            import_quality_decision(proven, self.EXISTING, cfg=CFG).decision,
            "import",
        )


class TestAlbumInfoProjectionIsPure(unittest.TestCase):
    """``album_info_from_current`` mints from the ROW, never from the file."""

    def test_the_projection_reads_no_files(self) -> None:
        from lib.beets_db import CurrentBeetsItem, CurrentBeetsUnique
        from lib.release_identity import ReleaseIdentity

        identity = ReleaseIdentity(
            source="musicbrainz",
            release_id="99999999-8888-7777-6666-555555555555",
        )
        current = CurrentBeetsUnique(
            identity=identity,
            album_id=7,
            # Nothing here exists on disk. Beets' own column is the whole
            # evidence, which is what keeps this projection cheap enough to
            # stay on the ordinary current-library resolution path.
            album_path="/nonexistent/Artist/Album",
            items=(
                CurrentBeetsItem(
                    id=1, path="/nonexistent/Artist/Album/01.mp3",
                    format="MP3", bitrate=245_000, encoder_settings="-V 0"),
                CurrentBeetsItem(
                    id=2, path="/nonexistent/Artist/Album/02.mp3",
                    format="MP3", bitrate=250_000, encoder_settings="-V 0"),
            ),
            selectors=(),
        )
        info = album_info_from_current(current, CFG)
        assert info is not None
        self.assertEqual(info.format, "mp3 v0")
        # ``formats_on_disk`` stays the bare canonical codec: it answers
        # "which codecs own this album", and a contract is not a codec.
        self.assertEqual(info.formats_on_disk, frozenset({"mp3"}))


if __name__ == "__main__":
    unittest.main()
