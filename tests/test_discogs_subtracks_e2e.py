"""End-to-end real-Beets proof for Discogs flat indexed subtracks.

The network provider seam is replaced with deterministic Discogs-shaped
metadata, but everything after candidate discovery is real: the production
harness processes, Beets matcher, default-to-flat retry, apply, filesystem
move, SQLite catalogue, and Cratedigger post-import accounting receipt.
"""

from __future__ import annotations

import json
import os
import shlex
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from harness import import_one
from lib.beets import FORCE_IMPORT_DISTANCE_THRESHOLD, beets_validate
from lib.beets_db import BeetsDB
from lib.quality import AUDIO_EXTENSIONS_DOTTED, QualityRankConfig

_RELEASE_ID = "2823685"
_TRACKS = (
    ("A1", "Space Oddity"),
    ("A2.1", "Unwashed And Somewhat Slightly Dazed"),
    ("A2.2", "Don't Sit Down"),
    ("A3", "Letter To Hermione"),
    ("A4", "Cygnet Committee"),
    ("B1", "Janine"),
    ("B2", "An Occasional Dream"),
    ("B3", "Wild Eyed Boy From Freecloud"),
    ("B4", "God Knows I'm Good"),
    ("B5", "Memory Of A Free Festival"),
)


def _write_audio(path: Path, title: str, *, duration: int = 1) -> None:
    subprocess.run(
        [
            "ffmpeg",
            "-loglevel",
            "error",
            "-f",
            "lavfi",
            "-i",
            f"sine=frequency=440:duration={duration}",
            "-metadata",
            "artist=David Bowie",
            "-metadata",
            "album=David Bowie",
            "-metadata",
            f"title={title}",
            "-c:a",
            "flac",
            str(path),
        ],
        check=True,
    )


def _write_discogs_candidate_shim(
    shim: Path,
    receipt: Path,
    *,
    unknown_component_duration: bool = False,
) -> None:
    shim.mkdir()
    duration_by_position = {
        "A2.1": "0:06",
        "A2.2": "" if unknown_component_duration else "0:12",
    }
    source = f'''\
import json


def _install():
    def _album():
        from beets.autotag.hooks import AlbumInfo, TrackInfo
        from beetsplug.discogs import DiscogsPlugin
        from harness.beets_compat import register_discogs_indexed_program

        duration_by_position = {duration_by_position!r}
        raw = [
            {{"type_": "track", "position": position, "title": title,
             "duration": duration_by_position.get(position, "0:01")}}
            for position, title in {list(_TRACKS)!r}
        ]
        plugin = object.__new__(DiscogsPlugin)
        physical = plugin._coalesce_tracks(raw)
        tracks = []
        for index, track in enumerate(physical, start=1):
            position = track["position"]
            track_info = TrackInfo(
                title=track["title"], artist="David Bowie",
                track_id=f"{_RELEASE_ID}-{{position}}",
                release_track_id=f"{_RELEASE_ID}-{{position}}",
                index=index, medium=1, medium_index=index,
                medium_total=len(physical),
                length=plugin.get_track_length(track["duration"]),
                track_alt=position, data_source="Discogs",
            )
            register_discogs_indexed_program(
                track_info,
                track.get(
                    "_cratedigger_discogs_indexed_component_count",
                    1,
                ),
                duration_complete=track.get(
                    "_cratedigger_discogs_indexed_duration_complete",
                    True,
                ),
            )
            tracks.append(track_info)
        return AlbumInfo(
            album="David Bowie", artist="David Bowie",
            album_id={int(_RELEASE_ID)}, tracks=tracks,
            data_source="Discogs", discogs_albumid={_RELEASE_ID!r},
        )

    def _candidates(items, artist, album, va_likely, extra_tags=None):
        return [_album()]

    def _albums_for_ids(ids):
        return [_album() for value in ids if str(value) == {_RELEASE_ID!r}]

    try:
        from beets import metadata_plugins as modern
    except ImportError:
        modern = None
    try:
        from beets.autotag import hooks as legacy
    except ImportError:
        legacy = None
    modern_seam = modern is not None and hasattr(modern, "candidates")
    legacy_seam = legacy is not None and hasattr(legacy, "album_candidates")
    if modern_seam == legacy_seam:
        raise RuntimeError(
            "candidate seam ambiguous: modern=%r legacy=%r"
            % (modern_seam, legacy_seam)
        )
    if modern_seam:
        modern.candidates = _candidates
        modern.albums_for_ids = _albums_for_ids
        seam = "modern"
    else:
        legacy.album_candidates = _candidates
        if hasattr(legacy, "albums_for_ids"):
            legacy.albums_for_ids = _albums_for_ids
        seam = "legacy"
    with open({str(receipt)!r}, "w", encoding="utf-8") as handle:
        json.dump({{"seam": seam}}, handle)


_install()
'''
    (shim / "sitecustomize.py").write_text(source, encoding="utf-8")


def _audio_files(root: Path) -> list[Path]:
    return sorted(
        path
        for path in root.rglob("*")
        if path.is_file() and path.suffix.lower() in AUDIO_EXTENSIONS_DOTTED
    )


def _write_harness_wrapper(root: Path) -> str:
    """Use the pinned shell directly, including inside a pure Nix build."""

    bash = shutil.which("bash")
    if bash is None:
        raise RuntimeError("bash is required for the real harness e2e")
    harness = Path(import_one.__file__).parent / "run_beets_harness.sh"
    wrapper = root / "run-beets-harness"
    wrapper.write_text(
        f"#!{bash}\nexec {shlex.quote(bash)} "
        f"{shlex.quote(str(harness))} \"$@\"\n",
        encoding="utf-8",
    )
    wrapper.chmod(0o755)
    return str(wrapper)


class TestDiscogsSubtracksEndToEnd(unittest.TestCase):
    def _exercise_manifest(
        self,
        *,
        split_subtracks: bool,
        unknown_component_duration: bool = False,
    ) -> None:
        expected_count = 10 if split_subtracks else 9
        with tempfile.TemporaryDirectory(
            prefix="cratedigger-discogs-subtracks-e2e-",
        ) as raw_root:
            root = Path(raw_root)
            source = root / "source"
            library = root / "library"
            beets_dir = root / "beets"
            shim = root / "shim"
            receipt = root / "candidate-receipt.json"
            source.mkdir()
            library.mkdir()
            beets_dir.mkdir()
            harness_path = _write_harness_wrapper(root)

            local_tracks = list(_TRACKS)
            if not split_subtracks:
                local_tracks[1:3] = [(
                    "A2",
                    "Unwashed And Somewhat Slightly Dazed / Don't Sit Down",
                )]
            for index, (position, title) in enumerate(local_tracks, start=1):
                filename_title = title.replace("/", "+")
                _write_audio(
                    source / f"{index:02d} - {filename_title}.flac",
                    title,
                    duration=(
                        18 if position == "A2"
                        else 6 if position == "A2.1"
                        else 12 if position == "A2.2"
                        else 1
                    ),
                )

            database = library / "library.db"
            (beets_dir / "config.yaml").write_text(
                "\n".join((
                    f"library: {database}",
                    f"directory: {library}",
                    "plugins: []",
                    "import:",
                    "  autotag: yes",
                    "  copy: no",
                    "  move: yes",
                    "  write: yes",
                    "  incremental: no",
                    "  duplicate_keys:",
                    "    album: [mb_albumid, discogs_albumid]",
                    "    item: [artist, title]",
                    "paths:",
                    "  default: $albumartist/$album/$track $title",
                    "",
                )),
                encoding="utf-8",
            )
            _write_discogs_candidate_shim(
                shim,
                receipt,
                unknown_component_duration=unknown_component_duration,
            )
            env = {
                "BEETSDIR": str(beets_dir),
                "PYTHONPATH": (
                    str(shim)
                    + os.pathsep
                    + str(Path(__file__).resolve().parent.parent)
                    + os.pathsep
                    + os.environ.get("PYTHONPATH", "")
                ),
            }

            with (
                patch.dict(os.environ, env, clear=False),
                patch.object(import_one, "HARNESS", harness_path),
            ):
                validation = beets_validate(
                    harness_path,
                    str(source),
                    _RELEASE_ID,
                    0.15,
                )
                self.assertTrue(validation.valid, validation.to_json())
                self.assertEqual(
                    len(validation.candidates[0].mapping),
                    expected_count,
                )
                self.assertEqual(validation.candidates[0].extra_items, [])

                outcome = import_one.run_import(
                    str(source),
                    _RELEASE_ID,
                    beets_config_dir=str(beets_dir),
                    beets_python=os.environ.get("CRATEDIGGER_BEETS_PYTHON"),
                    beets_library_db_path=str(database),
                    beets_library_root=str(library),
                )

            self.assertTrue(receipt.is_file())
            self.assertIn(
                json.loads(receipt.read_text(encoding="utf-8"))["seam"],
                ("modern", "legacy"),
            )
            self.assertEqual(outcome.exit_code, 0, outcome.failure_reason)
            self.assertEqual(outcome.admitted_audio_count, expected_count)
            self.assertEqual(outcome.applied_audio_count, expected_count)
            self.assertEqual(len(_audio_files(library)), expected_count)

            beets = BeetsDB(str(database), library_root=str(library))
            try:
                album = beets.get_album_info(
                    _RELEASE_ID,
                    QualityRankConfig.defaults(),
                )
            finally:
                beets.close()
            self.assertIsNotNone(album)
            assert album is not None
            self.assertEqual(album.track_count, expected_count)

    def test_bowie_ten_file_manifest_survives_real_match_apply_and_catalogue(
        self,
    ) -> None:
        self._exercise_manifest(split_subtracks=True)

    def test_complete_composite_remains_one_physical_file(self) -> None:
        self._exercise_manifest(split_subtracks=False)

    def test_split_manifest_survives_unknown_indexed_duration(self) -> None:
        self._exercise_manifest(
            split_subtracks=True,
            unknown_component_duration=True,
        )

    def _exercise_force_rejection(
        self,
        *,
        composite_duration: int,
        duplicate_component: bool,
    ) -> None:
        with tempfile.TemporaryDirectory(
            prefix="cratedigger-discogs-subtracks-force-e2e-",
        ) as raw_root:
            root = Path(raw_root)
            source = root / "source"
            library = root / "library"
            beets_dir = root / "beets"
            shim = root / "shim"
            receipt = root / "candidate-receipt.json"
            source.mkdir()
            library.mkdir()
            beets_dir.mkdir()
            harness_path = _write_harness_wrapper(root)

            local_tracks = list(_TRACKS)
            local_tracks[1:3] = [(
                "A2",
                "Unwashed And Somewhat Slightly Dazed",
            )]
            if duplicate_component:
                # The physical A2 composite already contains both indexed
                # components. This separately admitted file has matching
                # A2.2 metadata but must not authorize a flat reinterpretation.
                local_tracks.append(("EXTRA", "Don't Sit Down"))
            for index, (position, title) in enumerate(local_tracks, start=1):
                _write_audio(
                    source / f"{index:02d} - {title}.flac",
                    title,
                    duration=(
                        composite_duration
                        if position == "A2"
                        else 12 if position == "EXTRA"
                        else 1
                    ),
                )

            database = library / "library.db"
            (beets_dir / "config.yaml").write_text(
                "\n".join((
                    f"library: {database}",
                    f"directory: {library}",
                    "plugins: []",
                    "import:",
                    "  autotag: yes",
                    "  copy: no",
                    "  move: yes",
                    "  write: yes",
                    "  incremental: no",
                    "paths:",
                    "  default: $albumartist/$album/$track $title",
                    "",
                )),
                encoding="utf-8",
            )
            _write_discogs_candidate_shim(shim, receipt)
            env = {
                "BEETSDIR": str(beets_dir),
                "PYTHONPATH": (
                    str(shim)
                    + os.pathsep
                    + str(Path(__file__).resolve().parent.parent)
                    + os.pathsep
                    + os.environ.get("PYTHONPATH", "")
                ),
            }

            with (
                patch.dict(os.environ, env, clear=False),
                patch.object(
                    import_one,
                    "max_distance",
                    FORCE_IMPORT_DISTANCE_THRESHOLD,
                ),
                patch.object(import_one, "HARNESS", harness_path),
            ):
                validation = beets_validate(
                    harness_path,
                    str(source),
                    _RELEASE_ID,
                    FORCE_IMPORT_DISTANCE_THRESHOLD,
                )
                self.assertFalse(validation.valid, validation.to_json())
                outcome = import_one.run_import(
                    str(source),
                    _RELEASE_ID,
                    beets_config_dir=str(beets_dir),
                    beets_python=os.environ.get("CRATEDIGGER_BEETS_PYTHON"),
                    beets_library_db_path=str(database),
                    beets_library_root=str(library),
                )

            self.assertEqual(outcome.exit_code, 2)
            self.assertIn("candidate mapping would discard", outcome.failure_reason or "")
            self.assertEqual(
                len(_audio_files(source)),
                10 if duplicate_component else 9,
            )
            self.assertEqual(_audio_files(library), [])

    def test_incomplete_composite_fails_closed_even_with_force_distance(
        self,
    ) -> None:
        self._exercise_force_rejection(
            composite_duration=6,
            duplicate_component=False,
        )

    def test_complete_composite_plus_extra_cannot_be_flattened_under_force(
        self,
    ) -> None:
        self._exercise_force_rejection(
            composite_duration=18,
            duplicate_component=True,
        )


if __name__ == "__main__":
    unittest.main()
