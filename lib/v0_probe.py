"""Non-destructive MP3 V0 research measurements shared by preview/import.

The probe re-encodes source audio into a temporary directory and measures the
result.  Research probes are audit/display evidence only; policy code gives
weight exclusively to ``lossless_source_v0`` provenance.
"""

from __future__ import annotations

import logging
import math
import os
import statistics
import subprocess
import tempfile

from lib.quality import (
    AUDIO_EXTENSIONS_DOTTED,
    V0_PROBE_ON_DISK_RESEARCH,
    V0ProbeEvidence,
)

logger = logging.getLogger("cratedigger")

V0_CODEC = "libmp3lame"
V0_CODEC_ARGS = ("-q:a", "0")
V0_METADATA_ARGS = ("-map_metadata", "0", "-id3v2_version", "3")

_CONVERSION_TIMEOUT_FLOOR_S = 300
_CONVERSION_MIN_REALTIME_FACTOR = 4
_CONVERSION_TIMEOUT_HEADROOM_S = 120


def folder_bitrates(
    folder_path: str,
    ext_filter: set[str] | None = None,
) -> list[int]:
    """Return positive per-track audio bitrates in kbps."""

    bitrates: list[int] = []
    for fname in os.listdir(folder_path):
        ext = os.path.splitext(fname)[1].lower()
        if ext not in AUDIO_EXTENSIONS_DOTTED:
            continue
        if ext_filter is not None and ext not in ext_filter:
            continue
        fpath = os.path.join(folder_path, fname)
        try:
            result = subprocess.run(
                [
                    "ffprobe", "-v", "error", "-select_streams", "a:0",
                    "-show_entries", "stream=bit_rate", "-of", "csv=p=0",
                    fpath,
                ],
                capture_output=True,
                text=True,
                errors="replace",
                timeout=30,
            )
            value = result.stdout.strip().rstrip(",")
            if not value or not value.isdigit():
                result = subprocess.run(
                    [
                        "ffprobe", "-v", "error", "-show_entries",
                        "format=bit_rate", "-of", "csv=p=0", fpath,
                    ],
                    capture_output=True,
                    text=True,
                    errors="replace",
                    timeout=30,
                )
                value = result.stdout.strip().rstrip(",")
            if value and value.isdigit() and int(value) > 0:
                bitrates.append(int(value) // 1000)
        except Exception:  # noqa: BLE001 - research evidence is fail-soft
            logger.debug("V0 bitrate probe failed for %s", fpath, exc_info=True)
    return [value for value in bitrates if value > 0]


def probe_duration_seconds(path: str) -> float | None:
    """Read source duration cheaply for a duration-scaled ffmpeg timeout."""

    try:
        # getattr (not `from mutagen import File`) keeps this Any-typed:
        # mutagen's File() factory has an untyped `filething` parameter and
        # a partially-unknown overloaded return (many mutagen format
        # classes) — third-party, not ours to annotate. Same technique as
        # harness.import_one._probe_source_channels.
        import mutagen
        mutagen_file = getattr(mutagen, "File")
        media = mutagen_file(path)
    except Exception:  # noqa: BLE001 - absence/unreadable is a normal fallback
        return None
    if media is None:
        return None
    length = getattr(getattr(media, "info", None), "length", None)
    if isinstance(length, (int, float)) and length > 0:
        return float(length)
    return None


def conversion_timeout_seconds(duration_s: float | None) -> int:
    """Return a safe ffmpeg budget scaled for long-form tracks."""

    if duration_s is None or duration_s <= 0:
        return _CONVERSION_TIMEOUT_FLOOR_S
    scaled = (
        math.ceil(duration_s / _CONVERSION_MIN_REALTIME_FACTOR)
        + _CONVERSION_TIMEOUT_HEADROOM_S
    )
    return max(_CONVERSION_TIMEOUT_FLOOR_S, scaled)


def v0_probe_from_bitrates(
    bitrates: list[int],
    *,
    kind: str,
) -> V0ProbeEvidence | None:
    """Build the typed probe summary for one measured temporary encode."""

    if not bitrates:
        return None
    return V0ProbeEvidence(
        kind=kind,
        min_bitrate_kbps=min(bitrates),
        avg_bitrate_kbps=int(sum(bitrates) / len(bitrates)),
        median_bitrate_kbps=int(statistics.median(bitrates)),
    )


def probe_files_as_v0(
    album_path: str,
    files: list[str],
    *,
    kind: str,
) -> V0ProbeEvidence | None:
    """Re-encode relative ``files`` into temporary V0 MP3s and measure them."""

    if not files:
        return None
    with tempfile.TemporaryDirectory(prefix="cratedigger-v0-probe-") as temp_dir:
        for index, relative_path in enumerate(files):
            src_path = os.path.join(album_path, relative_path)
            base = os.path.splitext(os.path.basename(relative_path))[0]
            out_path = os.path.join(temp_dir, f"{index:03d}-{base}.mp3")
            try:
                result = subprocess.run(
                    [
                        "ffmpeg", "-i", src_path, "-map", "0:a",
                        "-ac", "2", "-c:a",
                        V0_CODEC, *V0_CODEC_ARGS, *V0_METADATA_ARGS,
                        "-y", out_path,
                    ],
                    capture_output=True,
                    text=True,
                    errors="replace",
                    timeout=conversion_timeout_seconds(
                        probe_duration_seconds(src_path)
                    ),
                )
            except (OSError, subprocess.TimeoutExpired):
                logger.warning("V0 research probe failed for %s", src_path)
                return None
            if (
                result.returncode != 0
                or not os.path.isfile(out_path)
                or os.path.getsize(out_path) == 0
            ):
                logger.warning("V0 research probe conversion failed for %s", src_path)
                return None
        return v0_probe_from_bitrates(
            folder_bitrates(temp_dir, ext_filter={".mp3"}),
            kind=kind,
        )


def probe_installed_album_as_v0(album_path: str) -> V0ProbeEvidence | None:
    """Fail-soft research probe for the exact installed album files."""

    if not os.path.isdir(album_path):
        return None
    files: list[str] = []
    for root, _dirs, filenames in os.walk(album_path):
        for filename in sorted(filenames):
            if os.path.splitext(filename)[1].lower() not in AUDIO_EXTENSIONS_DOTTED:
                continue
            files.append(os.path.relpath(os.path.join(root, filename), album_path))
    return probe_files_as_v0(
        album_path,
        sorted(files),
        kind=V0_PROBE_ON_DISK_RESEARCH,
    )
