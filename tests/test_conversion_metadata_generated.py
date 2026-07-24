"""Kept-output conversion preserves the beets match surface; art never rides through.

Issue #863 (superseding one sentence of #835 policy item 1): the #835 U5
conversion hardening stripped ALL metadata from kept outputs, so beets
matched converted albums on filenames and track lengths only — apply-time
distance inflated (request 8887 falsely rejected at 0.5637 after validating
at 0.082). The corrected policy:

- kept-output conversion preserves the source tag surface beets matches on
  (``-map_metadata 0``);
- embedded art NEVER rides through: picture streams are excluded by
  ``-map 0:a`` and the art-in-tag surfaces (``METADATA_BLOCK_PICTURE``,
  legacy ``COVERART``/``COVERARTMIME``) are deleted case-insensitively;
- the V0 probe spec still strips everything — it is a discarded
  measurement artifact.

Every check here drives the REAL ``convert_lossless`` with REAL ffmpeg over
real sox-generated FLACs — the conversion writer and the beets matcher share
the staged-files namespace, so this invariant lives at that composed
boundary (`.claude/rules/code-quality.md` § "Invariants live at the widest
boundary the change touches").
"""

import base64
import os
import tempfile
import unittest

from hypothesis import HealthCheck, example, given, settings
from hypothesis import strategies as st
from mutagen.flac import FLAC, Picture
from mutagen.id3 import ID3
from mutagen.oggopus import OggOpus

import tests._hypothesis_profiles  # noqa: F401  (registers/loads profiles)
from harness.import_one import (
    ConversionSpec,
    V0_SPEC,
    FLAC_SPEC,
    convert_lossless,
    parse_verified_lossless_target,
)
from tests.audio_fixtures import make_test_flac

# The tag surface beets' matcher consumes (normalized lowercase keys).
MATCH_TAG_KEYS = (
    "artist",
    "album",
    "title",
    "tracknumber",
    "date",
    "albumartist",
    "musicbrainz_albumid",
)

# Art-in-tag surfaces that must never survive conversion (any key casing).
ART_TAG_KEYS = ("metadata_block_picture", "coverart", "coverartmime")

_MB_ALBUM_ID = "ddecbaac-eb81-4bd2-9f38-0ee063e98451"

# 1x1 transparent PNG — enough bytes to be a real Picture payload.
_PNG = base64.b64decode(
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mP8"
    "z8BQDwAEhQGAhKmMIQAAAABJRU5ErkJggg=="
)


def make_tagged_flac(
    album_dir: str,
    *,
    artist: str = "Test Ärtist",
    album: str = "Tëst Album",
    title: str = "Song Øne",
    tracknumber: int = 3,
    date: str = "1994",
    art_tag_key: str | None = "METADATA_BLOCK_PICTURE",
    embed_picture_block: bool = True,
) -> dict[str, str]:
    """Create one tagged FLAC (with optional art surfaces); return its tags."""
    path = os.path.join(album_dir, "track.flac")
    make_test_flac(path, cutoff_hz=15500, duration=1)
    f = FLAC(path)
    f["ARTIST"] = artist
    f["ALBUM"] = album
    f["TITLE"] = title
    f["TRACKNUMBER"] = str(tracknumber)
    f["DATE"] = date
    f["ALBUMARTIST"] = artist
    f["MUSICBRAINZ_ALBUMID"] = _MB_ALBUM_ID
    if embed_picture_block:
        pic = Picture()
        pic.type = 3
        pic.mime = "image/png"
        pic.data = _PNG
        f.add_picture(pic)
    if art_tag_key is not None:
        payload = base64.b64encode(_PNG).decode()
        f[art_tag_key] = payload
        f["COVERARTMIME"] = "image/png"
    f.save()
    return {
        "artist": artist,
        "album": album,
        "title": title,
        "tracknumber": str(tracknumber),
        "date": date,
        "albumartist": artist,
        "musicbrainz_albumid": _MB_ALBUM_ID,
    }


def convert_album(album_dir: str, spec: ConversionSpec) -> str:
    """Run the REAL converter; return the single output path."""
    converted, failed, _, _ = convert_lossless(album_dir, spec)
    if failed or converted != 1:
        raise AssertionError(
            f"conversion did not complete: converted={converted} failed={failed}"
        )
    out = os.path.join(album_dir, "track." + spec.extension)
    if not os.path.isfile(out):
        raise AssertionError(f"expected conversion output missing: {out}")
    return out


def read_match_tags(out_path: str) -> dict[str, str]:
    """Read the normalized match-tag surface from a converted file."""
    tags: dict[str, str] = {}
    if out_path.endswith(".mp3"):
        id3 = ID3(out_path)
        frame_map = {
            "TPE1": "artist",
            "TALB": "album",
            "TIT2": "title",
            "TRCK": "tracknumber",
            "TDRC": "date",
            "TPE2": "albumartist",
        }
        for frame_id, key in frame_map.items():
            frames = id3.getall(frame_id)
            if frames:
                tags[key] = str(frames[0].text[0])
        for frame in id3.getall("TXXX"):
            if _MB_ALBUM_ID in [str(v) for v in frame.text]:
                tags["musicbrainz_albumid"] = _MB_ALBUM_ID
        return tags
    vorbis = FLAC(out_path) if out_path.endswith(".flac") else OggOpus(out_path)
    vorbis_items: list[tuple[str, list[str]]] = (
        list(vorbis.tags.items())  # pyright: ignore[reportAttributeAccessIssue]
        if vorbis.tags is not None else []
    )
    for key, values in vorbis_items:
        lowered = key.lower()
        if lowered in MATCH_TAG_KEYS and values:
            tags[lowered] = str(values[0])
    return tags


def read_art_surfaces(out_path: str) -> list[str]:
    """Every art surface present on a converted file (must be empty)."""
    surfaces: list[str] = []
    if out_path.endswith(".mp3"):
        id3 = ID3(out_path)
        if id3.getall("APIC"):
            surfaces.append("APIC")
        return surfaces
    audio = FLAC(out_path) if out_path.endswith(".flac") else OggOpus(out_path)
    audio_items: list[tuple[str, list[str]]] = (
        list(audio.tags.items())  # pyright: ignore[reportAttributeAccessIssue]
        if audio.tags is not None else []
    )
    for key, _values in audio_items:
        if key.lower() in ART_TAG_KEYS:
            surfaces.append(key)
    if out_path.endswith(".flac") and FLAC(out_path).pictures:
        surfaces.append("PICTURE_BLOCK")
    return surfaces


def check_match_surface_preserved(
    expected: dict[str, str], out_path: str,
) -> list[str]:
    """Violations: expected match tags the converted file no longer carries."""
    actual = read_match_tags(out_path)
    violations: list[str] = []
    for key in MATCH_TAG_KEYS:
        want = expected.get(key)
        if want is None:
            continue
        got = actual.get(key)
        if got is None:
            violations.append(f"{key}: missing (wanted {want!r})")
        elif key in ("tracknumber", "date"):
            if not got.startswith(want):
                violations.append(f"{key}: {got!r} != {want!r}")
        elif got != want:
            violations.append(f"{key}: {got!r} != {want!r}")
    return violations


def check_no_art_surface(out_path: str) -> list[str]:
    """Violations: art surfaces that survived conversion."""
    return read_art_surfaces(out_path)


_SUBPROCESS_SETTINGS = settings(
    max_examples=(
        48
        if os.environ.get("CRATEDIGGER_HYPOTHESIS_PROFILE") == "fuzz"
        else 10
    ),
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow],
)

_TAG_TEXT = st.text(
    alphabet=st.characters(
        codec="utf-8",
        categories=("L", "N", "P", "Zs"),
    ),
    min_size=1,
    max_size=24,
).filter(lambda s: s.strip() == s and s.strip() != "")


class TestConversionMetadataPin(unittest.TestCase):
    """Deterministic pins for the #863 conversion metadata policy."""

    def test_opus_target_preserves_match_tags_and_drops_art(self):
        with tempfile.TemporaryDirectory() as album:
            expected = make_tagged_flac(album)
            out = convert_album(album, parse_verified_lossless_target("opus 128"))
            self.assertEqual(
                check_match_surface_preserved(expected, out), [],
                "converted opus lost match-relevant tags",
            )
            self.assertEqual(
                check_no_art_surface(out), [],
                "embedded art rode through conversion",
            )

    def test_mp3_target_preserves_match_tags_and_drops_art(self):
        with tempfile.TemporaryDirectory() as album:
            expected = make_tagged_flac(album)
            out = convert_album(album, parse_verified_lossless_target("mp3 v0"))
            self.assertEqual(check_match_surface_preserved(expected, out), [])
            self.assertEqual(check_no_art_surface(out), [])

    def test_v0_probe_spec_still_strips_everything(self):
        """The V0 probe output is a discarded measurement artifact."""
        with tempfile.TemporaryDirectory() as album:
            make_tagged_flac(album)
            out = convert_album(album, V0_SPEC)
            self.assertEqual(read_match_tags(out), {})
            self.assertEqual(check_no_art_surface(out), [])

    def test_kept_output_specs_share_the_preserving_policy(self):
        """Every kept-output spec carries preserve + art-strip args."""
        for label in ("opus 128", "mp3 v0", "mp3 192", "aac 128"):
            with self.subTest(label=label):
                spec = parse_verified_lossless_target(label)
                self.assertIn("-map_metadata", spec.metadata_args)
                idx = spec.metadata_args.index("-map_metadata")
                self.assertEqual(spec.metadata_args[idx + 1], "0")
                self.assertIn("METADATA_BLOCK_PICTURE=", spec.metadata_args)
                self.assertIn("COVERART=", spec.metadata_args)
                self.assertIn("COVERARTMIME=", spec.metadata_args)
        with self.subTest(label="FLAC_SPEC"):
            self.assertIn("-map_metadata", FLAC_SPEC.metadata_args)
            idx = FLAC_SPEC.metadata_args.index("-map_metadata")
            self.assertEqual(FLAC_SPEC.metadata_args[idx + 1], "0")


class TestConversionMetadataProperty(unittest.TestCase):
    """Generated worlds over tags, art-key casings, and kept-output specs."""

    @example(
        artist="Ünïcode Ärtist",
        album="Album",
        title="Title",
        tracknumber=1,
        art_key="metadata_block_picture",
        spec_label="opus 128",
    )
    @example(
        artist="A",
        album="B",
        title="C",
        tracknumber=9,
        art_key="CoverArt",
        spec_label="mp3 v0",
    )
    @given(
        artist=_TAG_TEXT,
        album=_TAG_TEXT,
        title=_TAG_TEXT,
        tracknumber=st.integers(min_value=1, max_value=99),
        art_key=st.sampled_from(
            ("METADATA_BLOCK_PICTURE", "metadata_block_picture",
             "CoverArt", "COVERART")
        ),
        spec_label=st.sampled_from(("opus 128", "mp3 v0", "mp3 192")),
    )
    @_SUBPROCESS_SETTINGS
    def test_conversion_preserves_match_surface_and_never_art(
        self,
        artist: str,
        album: str,
        title: str,
        tracknumber: int,
        art_key: str,
        spec_label: str,
    ) -> None:
        with tempfile.TemporaryDirectory() as album_dir:
            expected = make_tagged_flac(
                album_dir,
                artist=artist,
                album=album,
                title=title,
                tracknumber=tracknumber,
                art_tag_key=art_key,
            )
            out = convert_album(
                album_dir, parse_verified_lossless_target(spec_label))
            self.assertEqual(
                check_match_surface_preserved(expected, out), [],
                f"match surface lost ({spec_label})",
            )
            self.assertEqual(
                check_no_art_surface(out), [],
                f"art surface survived ({spec_label}, key={art_key})",
            )


class TestConversionMetadataCheckersTripOnViolations(unittest.TestCase):
    """Known-bad worlds prove the checkers actually detect violations."""

    def test_preserve_checker_trips_on_the_old_strip_policy(self):
        """The pre-#863 strip spec must fail the preserve checker."""
        old_strip_spec = ConversionSpec(
            codec="libopus",
            codec_args=("-b:a", "128k"),
            extension="opus",
            label="opus 128 (old strip)",
            metadata_args=("-map_metadata", "-1", "-map_chapters", "-1"),
        )
        with tempfile.TemporaryDirectory() as album:
            expected = make_tagged_flac(album)
            out = convert_album(album, old_strip_spec)
            self.assertNotEqual(
                check_match_surface_preserved(expected, out), [],
                "checker failed to trip on fully stripped output",
            )

    def test_art_checker_trips_on_full_copy_without_art_strip(self):
        """Plain -map_metadata 0 carries the art tag; the checker must see it."""
        copy_all_spec = ConversionSpec(
            codec="libopus",
            codec_args=("-b:a", "128k"),
            extension="opus",
            label="opus 128 (copy all)",
            metadata_args=("-map_metadata", "0"),
        )
        with tempfile.TemporaryDirectory() as album:
            make_tagged_flac(album)
            out = convert_album(album, copy_all_spec)
            self.assertNotEqual(
                check_no_art_surface(out), [],
                "checker failed to trip on surviving art tag",
            )


if __name__ == "__main__":
    unittest.main()
