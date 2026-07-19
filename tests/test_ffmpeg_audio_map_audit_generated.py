"""Generated syntax patrol for the production ffmpeg audio-map contract."""

from __future__ import annotations

import unittest

from hypothesis import given
from hypothesis import strategies as st

import tests._hypothesis_profiles  # noqa: F401 - registers active profile
from tests.structural_audits.ffmpeg_audio_map import (
    assert_ffmpeg_audio_mapping,
)


_EXTRA_ARGS = st.lists(
    st.sampled_from(("-nostdin", "-y", "-hide_banner", "-loglevel", "error")),
    max_size=6,
)


def _command_source(args: list[str], *, multiline: bool) -> str:
    rendered = [repr(arg) for arg in args]
    separator = ",\n    " if multiline else ", "
    return f"cmd = [{separator.join(rendered)}]\n"


class TestGeneratedFfmpegAudioMapSyntax(unittest.TestCase):
    @given(prefix=_EXTRA_ARGS, suffix=_EXTRA_ARGS, multiline=st.booleans())
    def test_formatting_and_list_layout_cannot_hide_a_safe_audio_map(
        self,
        prefix: list[str],
        suffix: list[str],
        multiline: bool,
    ) -> None:
        source = _command_source(
            ["ffmpeg", *prefix, "-i", "input.flac", "-map", "0:a", *suffix],
            multiline=multiline,
        )
        sites = assert_ffmpeg_audio_mapping(source, filename="generated_safe.py")
        self.assertEqual(len(sites), 1)

    @given(
        replacement=st.sampled_from(
            (
                (),
                ("-map",),
                ("0:a",),
                ("-map", "0:v"),
                ("-map", "0", "0:a"),
            )
        ),
        prefix=_EXTRA_ARGS,
        suffix=_EXTRA_ARGS,
        multiline=st.booleans(),
    )
    def test_generated_non_audio_map_shapes_fail_closed(
        self,
        replacement: tuple[str, ...],
        prefix: list[str],
        suffix: list[str],
        multiline: bool,
    ) -> None:
        source = _command_source(
            ["ffmpeg", *prefix, "-i", "input.flac", *replacement, *suffix],
            multiline=multiline,
        )
        with self.assertRaises(AssertionError):
            assert_ffmpeg_audio_mapping(source, filename="generated_bad.py")


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
