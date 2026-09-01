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


def _synth_timeout_seconds(duration: int) -> int:
    """Wall-clock budget for one sox synthesis of ``duration`` seconds.

    sox costs roughly a hundredth of a second per second of audio it
    synthesises, so a FLAT budget is only right at one duration. The 30 s
    constant this replaced was sized for the 5 s default and left the one
    800 s caller 3.5x headroom (measured idle on doc1: 8.54 s against a
    30 s budget) — thin enough that a loaded gate host ate it, and
    `tests.test_conversion_e2e` flaked. The budget is a wedge detector,
    not a performance assertion, so it scales with the work asked for.
    """
    return 30 + duration // 2


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
    _run_sox(cmd, path, duration)


def make_long_test_flac(path: str, duration: int) -> None:
    """Generate a FLAC whose only contract is the duration ffprobe reads.

    ``make_test_flac``'s five oscillators, tremolo and sinc filter exist
    for its V0-bitrate contract, and a long one is expensive: 800 s cost
    8.54 s and 91.9 MB, paid into the shared test tmpfs whose exhaustion
    is its own recurring incident class (#1111, #1214). A caller that only
    needs a long container — the conversion-timeout wiring test probes the
    duration and fakes the conversion itself — gets one here for 0.80 s
    and 2.5 MB: mono, 8 kHz, one sine. Both figures measured on doc1 at
    load 2.3. There is NO bitrate contract; use ``make_test_flac`` if you
    need one.
    """
    cmd = [
        "sox", "-n", "-r", "8000", "-c", "1", "-b", "16", path,
        "synth", str(duration), "sine", "440",
    ]
    _run_sox(cmd, path, duration)


def _run_sox(cmd: list[str], path: str, duration: int) -> None:
    result = subprocess.run(
        cmd, capture_output=True, text=True,
        timeout=_synth_timeout_seconds(duration), check=False,
    )
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
        check=False,
    )
    br_str = result.stdout.strip().rstrip(",")
    # VBR MP3s return N/A — fall back to format bitrate
    if not br_str or not br_str.isdigit():
        result = subprocess.run(
            ["ffprobe", "-v", "error",
             "-show_entries", "format=bit_rate", "-of", "csv=p=0", path],
            capture_output=True, text=True, timeout=30,
            check=False,
        )
        br_str = result.stdout.strip().rstrip(",")
    if not br_str or not br_str.isdigit():
        raise RuntimeError(f"Could not determine bitrate for {path}")
    return int(br_str) // 1000
