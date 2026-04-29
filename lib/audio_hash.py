"""Audio-content hashing for bad-rip defense.

Public contract: 32-byte SHA-256 across ALL formats.

For FLAC, we decode to raw signed 16-bit little-endian PCM via ffmpeg and hash
that — NOT the 16-byte STREAMINFO MD5 — so the public contract is uniform across
formats and downstream code never has to special-case length. (Option (a) in the
U3 plan: simpler consumer, slightly slower than reading STREAMINFO directly.)

For lossy formats (mp3 / m4a / ogg / etc.), we run
``ffmpeg -i path -map 0:a -c copy -map_metadata -1 -f <muxer> -`` and SHA-256
the resulting metadata-stripped frame stream. ``-c copy`` means we never
re-encode lossy audio, so the hash is stable as long as the underlying file's
audio frames are byte-identical.

Format-mismatch evasion (same source → FLAC vs MP3 → different hashes) is
deliberately accepted in v1; the lookup composite key is ``(hash, audio_format)``
which means a poisoned MP3 cannot match against a FLAC entry anyway. See plan
2026-04-29-005, R4 + Out of Scope.
"""

from __future__ import annotations

import hashlib
import subprocess
from pathlib import Path
from typing import Final

_CHUNK_SIZE: Final[int] = 64 * 1024
_FFMPEG_TIMEOUT_S: Final[int] = 60

# Map normalised format -> muxer name passed to `ffmpeg -f`.
# FLAC is hashed over raw PCM rather than the .flac container so that the
# public 32-byte SHA-256 contract holds uniformly.
_LOSSY_MUXERS: Final[dict[str, str]] = {
    "mp3": "mp3",
    "m4a": "adts",
    "aac": "adts",
    "ogg": "ogg",
    "opus": "ogg",
}

# Extension -> normalised format.
_EXT_TO_FORMAT: Final[dict[str, str]] = {
    ".flac": "flac",
    ".mp3": "mp3",
    ".m4a": "m4a",
    ".aac": "aac",
    ".ogg": "ogg",
    ".opus": "opus",
}


class AudioHashError(Exception):
    """Raised when audio-content hashing fails.

    Wraps: missing path, mutagen read errors, ffmpeg subprocess failures,
    unsupported format strings. Callers per-track ``try/except`` and either
    surface the path as ``hash_capture_errors`` (R6) or fall through to the
    next gate (U5).
    """


def _format_from_ext(path: Path) -> str:
    ext = path.suffix.lower()
    fmt = _EXT_TO_FORMAT.get(ext)
    if fmt is None:
        raise AudioHashError(
            f"unsupported format: cannot infer audio_format from extension {ext!r}"
        )
    return fmt


def _hash_subprocess_stdout(cmd: list[str]) -> bytes:
    """Run ``cmd``, stream stdout into SHA-256 incrementally, return digest.

    Uses ``Popen`` (rather than ``subprocess.run(..., capture_output=True)``)
    so we don't hold the entire decoded PCM stream in memory — a 50-minute
    album decoded to s16le mono 44.1kHz is ~250 MB.
    """
    sha = hashlib.sha256()
    try:
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
    except FileNotFoundError as e:
        raise AudioHashError(f"ffmpeg not found on PATH: {e}") from e

    assert proc.stdout is not None
    assert proc.stderr is not None

    try:
        while True:
            chunk = proc.stdout.read(_CHUNK_SIZE)
            if not chunk:
                break
            sha.update(chunk)
        try:
            _, stderr = proc.communicate(timeout=_FFMPEG_TIMEOUT_S)
        except subprocess.TimeoutExpired as e:
            proc.kill()
            proc.communicate()
            raise AudioHashError(f"ffmpeg timed out: {' '.join(cmd)}") from e
    finally:
        if proc.stdout:
            proc.stdout.close()
        if proc.stderr:
            proc.stderr.close()

    if proc.returncode != 0:
        tail = stderr.decode("utf-8", errors="replace").strip().splitlines()[-5:]
        raise AudioHashError(
            f"ffmpeg failed (rc={proc.returncode}): {' / '.join(tail)}"
        )

    return sha.digest()


def _hash_flac_pcm(path: Path) -> bytes:
    """Decode FLAC to raw s16le mono-or-stereo PCM and SHA-256 that.

    We deliberately do NOT use ``STREAMINFO.md5_signature`` (which is 16 bytes
    of MD5 and would force the consumer to handle two digest lengths). Instead
    we decode through ffmpeg, which is deterministic for FLAC (lossless).
    """
    cmd = [
        "ffmpeg",
        "-hide_banner",
        "-loglevel", "error",
        "-fflags", "+bitexact",
        "-i", str(path),
        "-map", "0:a",
        "-map_metadata", "-1",
        "-flags", "+bitexact",
        "-fflags", "+bitexact",
        "-f", "s16le",
        "-acodec", "pcm_s16le",
        "-",
    ]
    return _hash_subprocess_stdout(cmd)


def _hash_lossy_container(path: Path, muxer: str) -> bytes:
    """Strip metadata from a lossy file with ``-c copy`` and SHA-256 the result.

    ``-c copy`` is critical: it means ffmpeg passes through the original
    compressed frames byte-for-byte. Re-encoding (which would happen without
    ``-c copy``) would make the hash unstable across ffmpeg/lame versions.

    ``-fflags +bitexact`` and ``-flags +bitexact`` suppress non-deterministic
    metadata (notably the OGG bitstream-serial, which ffmpeg otherwise
    randomises per invocation, breaking the hash even on identical input).
    """
    cmd = [
        "ffmpeg",
        "-hide_banner",
        "-loglevel", "error",
        "-fflags", "+bitexact",
        "-i", str(path),
        "-map", "0:a",
        "-c", "copy",
        "-map_metadata", "-1",
        "-flags", "+bitexact",
        "-fflags", "+bitexact",
        "-f", muxer,
        "-",
    ]
    return _hash_subprocess_stdout(cmd)


def _flac_streaminfo_is_zero(path: Path) -> bool:
    """Return True if the FLAC's STREAMINFO.md5_signature is missing/zero.

    The plan calls out that some FLAC encoders emit zero MD5; we detect that
    here so callers / tests can verify the fallback. We never actually USE the
    STREAMINFO MD5 for the public hash (option (a)); this is informational only
    and is exposed via :func:`flac_streaminfo_md5` for tests.
    """
    md5 = flac_streaminfo_md5(path)
    return md5 is None or md5 == b"\x00" * 16


def flac_streaminfo_md5(path: Path) -> bytes | None:
    """Return the FLAC STREAMINFO md5_signature as 16 bytes, or None.

    Exposed for tests and for any future fast-path that wants to deduplicate
    FLAC re-encodes without decoding. Not used by :func:`hash_audio_content`.
    """
    try:
        from mutagen.flac import FLAC  # type: ignore[import-untyped]
    except ImportError as e:  # pragma: no cover — mutagen ships in nix-shell
        raise AudioHashError(f"mutagen not available: {e}") from e
    try:
        flac = FLAC(str(path))
    except Exception as e:
        raise AudioHashError(f"failed to read FLAC {path}: {e}") from e
    info = flac.info
    raw: object = getattr(info, "md5_signature", None)
    if raw is None:
        return None
    if isinstance(raw, int):
        if raw == 0:
            return b"\x00" * 16
        try:
            return raw.to_bytes(16, "big")
        except OverflowError:
            return None
    if isinstance(raw, (bytes, bytearray)):
        b = bytes(raw)
        if len(b) != 16:
            return None
        return b
    return None


def hash_audio_content(path: Path, audio_format: str | None = None) -> bytes:
    """SHA-256 over compressed audio frames with all tags + artwork stripped.

    Args:
        path: Audio file on disk.
        audio_format: One of ``"flac" | "mp3" | "m4a" | "aac" | "ogg" | "opus"``.
            If ``None``, inferred from ``path.suffix``.

    Returns:
        Raw 32 bytes (SHA-256 digest).

    Raises:
        AudioHashError: missing file, unsupported format, ffmpeg/mutagen failure.
    """
    if not path.exists():
        raise AudioHashError(f"path does not exist: {path}")
    if not path.is_file():
        raise AudioHashError(f"path is not a regular file: {path}")

    fmt = (audio_format or _format_from_ext(path)).lower()

    if fmt == "flac":
        # Option (a): always 32-byte SHA-256, decoded PCM. The
        # ``flac_streaminfo_md5`` zero-check is purely informational; we run
        # the same ffmpeg PCM pipeline regardless, so a zero STREAMINFO
        # naturally falls through to the same code path.
        return _hash_flac_pcm(path)

    muxer = _LOSSY_MUXERS.get(fmt)
    if muxer is None:
        raise AudioHashError(f"unsupported format: {fmt!r}")
    return _hash_lossy_container(path, muxer)
