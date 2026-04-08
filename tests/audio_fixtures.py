"""Synthetic audio fixtures for e2e conversion tests.

Generates deterministic FLAC files with controllable V0 bitrates using
sawtooth waves with sox lowpass filtering. The lowpass cutoff frequency
maps predictably to LAME V0 bitrate:

    12000 Hz → ~205 kbps (below 210 transcode threshold)
    15500 Hz → ~236 kbps (genuine lossless range)
    16000 Hz → ~259 kbps (high quality genuine)

Properties:
    - Deterministic: same cutoff = same V0 bitrate every run
    - Duration-independent: 5s files produce the same bitrate as 30s
    - Per-track controllable: different cutoffs per track give realistic variation

Requires sox (available in nix dev shell).
"""

import os
import subprocess


def make_test_flac(path: str, cutoff_hz: int = 15500, duration: int = 5) -> None:
    """Generate a single FLAC file with predictable V0 bitrate.

    Args:
        path: output file path (must end in .flac)
        cutoff_hz: lowpass cutoff — controls V0 bitrate
        duration: audio duration in seconds (5s is sufficient)
    """
    cmd = [
        "sox", "-n", "-r", "44100", "-c", "2", "-b", "16", path,
        "synth", str(duration),
        "sawtooth", "110", "sawtooth", "220", "sawtooth", "440",
        "sawtooth", "880", "sawtooth", "1760",
        "vol", "0.4", "tremolo", "5", "40",
        "sinc", f"-{cutoff_hz}",
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
    if result.returncode != 0:
        raise RuntimeError(f"sox failed: {result.stderr}")
    if not os.path.exists(path):
        raise RuntimeError(f"sox did not create {path}")


def make_test_album(album_dir: str, track_count: int = 3,
                    cutoff_hz: int = 15500, duration: int = 5) -> list[str]:
    """Generate a multi-track FLAC album directory.

    Returns list of created file paths.
    """
    os.makedirs(album_dir, exist_ok=True)
    paths = []
    for i in range(1, track_count + 1):
        path = os.path.join(album_dir, f"{i:02d} - Track {i}.flac")
        make_test_flac(path, cutoff_hz=cutoff_hz, duration=duration)
        paths.append(path)
    return paths


def get_bitrate_kbps(path: str) -> int:
    """Get bitrate of an audio file in kbps via ffprobe."""
    # Try audio stream bitrate first
    result = subprocess.run(
        ["ffprobe", "-v", "error", "-select_streams", "a:0",
         "-show_entries", "stream=bit_rate", "-of", "csv=p=0", path],
        capture_output=True, text=True, timeout=30,
    )
    br_str = result.stdout.strip().rstrip(",")
    # VBR MP3s return N/A — fall back to format bitrate
    if not br_str or not br_str.isdigit():
        result = subprocess.run(
            ["ffprobe", "-v", "error",
             "-show_entries", "format=bit_rate", "-of", "csv=p=0", path],
            capture_output=True, text=True, timeout=30,
        )
        br_str = result.stdout.strip().rstrip(",")
    if not br_str or not br_str.isdigit():
        raise RuntimeError(f"Could not determine bitrate for {path}")
    return int(br_str) // 1000
