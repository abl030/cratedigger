"""Deterministic pins for explicit audio mapping in production ffmpeg commands."""

from __future__ import annotations

from pathlib import Path
import unittest

from tests.structural_audits.ffmpeg_audio_map import (
    assert_ffmpeg_audio_mapping,
)


REPO_ROOT = Path(__file__).resolve().parent.parent
PRODUCTION_ROOTS = REPO_ROOT / "tools" / "production_python_sources.txt"


def _production_python_paths() -> tuple[Path, ...]:
    roots = (
        line.strip()
        for line in PRODUCTION_ROOTS.read_text(encoding="utf-8").splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    )
    paths: list[Path] = []
    for relative_root in roots:
        root = REPO_ROOT / relative_root
        if root.is_file():
            paths.append(root)
        else:
            paths.extend(path for path in root.rglob("*.py") if path.is_file())
    return tuple(sorted(paths))


class TestProductionFfmpegAudioMapAudit(unittest.TestCase):
    def test_every_production_ffmpeg_command_explicitly_maps_audio(self) -> None:
        sites = []
        for path in _production_python_paths():
            relative_path = str(path.relative_to(REPO_ROOT))
            source = path.read_text(encoding="utf-8")
            sites.extend(
                assert_ffmpeg_audio_mapping(source, filename=relative_path)
            )

        self.assertTrue(sites, "audit must reach real production ffmpeg commands")

    def test_known_bad_real_production_command_without_map_is_rejected(self) -> None:
        path = REPO_ROOT / "lib" / "v0_probe.py"
        source = path.read_text(encoding="utf-8")
        mutant = source.replace('"-map", "0:a",', "", 1)
        self.assertNotEqual(mutant, source, "fault injection must remove the map")
        with self.assertRaisesRegex(AssertionError, "missing literal -map 0:a"):
            assert_ffmpeg_audio_mapping(mutant, filename="lib/v0_probe.py")

    def test_metadata_mapping_does_not_masquerade_as_audio_mapping(self) -> None:
        source = '''
cmd = [
    "ffmpeg", "-i", source_path,
    "-map_metadata", "0",
    "-c:a", "libmp3lame", output_path,
]
'''
        with self.assertRaisesRegex(AssertionError, "missing literal -map 0:a"):
            assert_ffmpeg_audio_mapping(source, filename="metadata_only.py")

    def test_incremental_command_construction_fails_closed(self) -> None:
        source = '''
cmd = ["ffmpeg", "-i", source_path]
cmd.extend(["-map", "0:a", output_path])
'''
        with self.assertRaisesRegex(AssertionError, "missing literal -map 0:a"):
            assert_ffmpeg_audio_mapping(source, filename="incremental.py")

    def test_noncanonical_binary_alias_fails_closed(self) -> None:
        source = '''
FFMPEG = "ffmpeg"
cmd = [FFMPEG, "-i", source_path, "-map", "0:a", output_path]
'''
        with self.assertRaisesRegex(AssertionError, "non-canonical ffmpeg token"):
            assert_ffmpeg_audio_mapping(source, filename="aliased.py")

    def test_audio_mapping_allows_source_metadata_policy_to_remain_explicit(self) -> None:
        source = '''
cmd = [
    "ffmpeg", "-i", source_path,
    "-map", "0:a",
    "-map_metadata", "0",
    "-c:a", "libmp3lame", output_path,
]
'''
        sites = assert_ffmpeg_audio_mapping(source, filename="metadata_kept.py")
        self.assertEqual(len(sites), 1)


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
