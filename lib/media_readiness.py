"""One owner for decode-valid media facts and allowlisted metadata repair.

The download and preview paths invoke this only after they have obtained an
owned canonical directory or a private snapshot.  It never normalizes a
peer-owned source path.  Full decode remains the corruption authority; this
module only recovers facts from bytes which that decode has already proved
stable and playable.
"""

from __future__ import annotations

import logging
import os
import stat
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

import msgspec

from lib.quality import AUDIO_EXTENSIONS_DOTTED

logger = logging.getLogger("cratedigger")

ReadinessFailureKind = Literal["audio_corrupt", "measurement_failed", "ambiguous"]


class MediaReadinessError(RuntimeError):
    """Typed failure before a consumer receives incomplete media facts."""

    def __init__(self, kind: ReadinessFailureKind, detail: str) -> None:
        super().__init__(detail)
        self.kind = kind


class _FfprobeStream(msgspec.Struct, kw_only=True, forbid_unknown_fields=False):
    codec_type: str | None = None
    codec_name: str | None = None
    sample_rate: str | int | None = None
    channels: int | str | None = None
    bits_per_raw_sample: int | str | None = None
    bits_per_sample: int | str | None = None


class _FfprobeFrame(msgspec.Struct, kw_only=True, forbid_unknown_fields=False):
    nb_samples: int | str | None = None


class _FfprobePacket(msgspec.Struct, kw_only=True, forbid_unknown_fields=False):
    size: int | str | None = None


class _FfprobePacketOrFrame(msgspec.Struct, kw_only=True, forbid_unknown_fields=False):
    type: Literal["packet", "frame"] | None = None
    nb_samples: int | str | None = None
    size: int | str | None = None


class _FfprobeFormat(msgspec.Struct, kw_only=True, forbid_unknown_fields=False):
    format_name: str | None = None


class _FfprobeReadinessWire(msgspec.Struct, kw_only=True, forbid_unknown_fields=False):
    """The single ffprobe JSON boundary used for stream-derived facts."""

    streams: list[_FfprobeStream] = []
    frames: list[_FfprobeFrame] = []
    packets: list[_FfprobePacket] = []
    packets_and_frames: list[_FfprobePacketOrFrame] = []
    format: _FfprobeFormat | None = None


@dataclass(frozen=True)
class MediaFileFacts:
    """One audio stream's facts, derived from frames when headers are absent."""

    path: str
    codec: str
    container: str
    sample_rate: int
    channels: int
    bit_depth: int | None
    sample_count: int
    duration_seconds: float
    compressed_audio_bytes: int
    average_bitrate_kbps: int | None


@dataclass(frozen=True)
class MediaReadiness:
    """Facts and the exact private files normalized for downstream readers."""

    files: tuple[MediaFileFacts, ...]
    normalized_paths: tuple[str, ...] = ()


def _audio_paths(folder_path: str) -> list[Path]:
    root = Path(folder_path)
    if not root.is_dir():
        raise MediaReadinessError("measurement_failed", "media folder is missing")
    return sorted(
        path for path in root.rglob("*")
        if (
            path.is_file()
            and not path.is_symlink()
            and path.suffix.lower() in AUDIO_EXTENSIONS_DOTTED
        )
    )


def repair_mp3_headers(folder_path: str) -> None:
    """Normalize repairable MP3 headers in the already-owned media view."""

    for path in _audio_paths(folder_path):
        if path.suffix.lower() != ".mp3":
            continue
        try:
            result = subprocess.run(
                ["mp3val", "-f", "-nb", str(path)],
                capture_output=True,
                text=True,
                errors="replace",
                timeout=60,
                check=False,
            )
            if "FIXED" in result.stdout:
                logger.info("MP3VAL: fixed %s", path.name)
        except FileNotFoundError:
            # Repair is opportunistic.  The following strict full decode is
            # still authoritative, so a missing optional normalizer cannot
            # manufacture a corruption verdict or bypass one.
            logger.warning("MP3VAL: mp3val not found on PATH — skipping header repair")
            return
        except subprocess.TimeoutExpired:
            logger.warning("MP3VAL: timeout on %s", path.name)
        except OSError as exc:
            logger.warning("MP3VAL: error on %s: %s", path.name, exc)


def _strict_decode(folder_path: str) -> None:
    """Use the existing full-decode contract before and after a repair."""

    from lib.util import validate_audio

    result = validate_audio(folder_path)
    if result.measurement_failed:
        raise MediaReadinessError("measurement_failed", result.error or "audio decode unavailable")
    if not result.valid:
        raise MediaReadinessError("audio_corrupt", result.error or "audio decode failed")


def _ffprobe_readiness(path: Path) -> _FfprobeReadinessWire:
    """Decode all stream, frame, and packet facts through one typed command."""
    try:
        result = subprocess.run(
            [
                "ffprobe", "-v", "error", "-select_streams", "a:0",
                "-show_streams", "-show_frames",
                "-show_packets", "-show_entries",
                (
                    "stream=codec_type,codec_name,sample_rate,channels,"
                    "bits_per_raw_sample,bits_per_sample:format=format_name:"
                    "frame=nb_samples:packet=size"
                ),
                "-of", "json", str(path),
            ],
            capture_output=True,
            text=True,
            errors="replace",
            timeout=60,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise MediaReadinessError("measurement_failed", f"ffprobe failed for {path.name}: {exc}") from exc
    if result.returncode != 0:
        raise MediaReadinessError("measurement_failed", f"ffprobe rejected {path.name}")
    try:
        return msgspec.json.decode(result.stdout, type=_FfprobeReadinessWire)
    except (msgspec.DecodeError, msgspec.ValidationError) as exc:
        raise MediaReadinessError(
            "measurement_failed", f"ffprobe returned invalid facts for {path.name}",
        ) from exc


def _positive_int(value: object) -> int | None:
    try:
        parsed = int(str(value))
    except (TypeError, ValueError):
        return None
    return parsed if parsed > 0 else None


def _frame_facts(path: Path, wire: _FfprobeReadinessWire) -> tuple[int, int]:
    frames = wire.frames
    packets = wire.packets
    if wire.packets_and_frames:
        frames = [entry for entry in wire.packets_and_frames if entry.type == "frame"]
        packets = [entry for entry in wire.packets_and_frames if entry.type == "packet"]
    if not frames:
        raise MediaReadinessError("measurement_failed", f"ffprobe found no audio frames for {path.name}")
    sample_count = 0
    for frame in frames:
        samples = _positive_int(frame.nb_samples)
        if samples is None:
            raise MediaReadinessError("measurement_failed", f"ffprobe omitted frame facts for {path.name}")
        sample_count += samples
    compressed_bytes = 0
    for packet in packets:
        packet_bytes = _positive_int(packet.size)
        if packet_bytes is None:
            raise MediaReadinessError("measurement_failed", f"ffprobe omitted packet facts for {path.name}")
        compressed_bytes += packet_bytes
    if sample_count <= 0 or compressed_bytes <= 0:
        raise MediaReadinessError("measurement_failed", f"empty frame facts for {path.name}")
    return sample_count, compressed_bytes


def _stream_facts(
    path: Path,
    wire: _FfprobeReadinessWire,
) -> tuple[str, int, int, int | None, str]:
    audio = [stream for stream in wire.streams if stream.codec_type == "audio"]
    if len(audio) != 1:
        raise MediaReadinessError("ambiguous", f"expected one audio stream in {path.name}, found {len(audio)}")
    stream = audio[0]
    codec = stream.codec_name
    sample_rate = _positive_int(stream.sample_rate)
    channels = _positive_int(stream.channels)
    bit_depth = _positive_int(stream.bits_per_raw_sample) or _positive_int(stream.bits_per_sample)
    if not isinstance(codec, str) or not codec or sample_rate is None or channels is None:
        raise MediaReadinessError("measurement_failed", f"ffprobe omitted stream facts for {path.name}")
    format_name = wire.format.format_name if wire.format is not None else None
    if not isinstance(format_name, str) or not format_name:
        raise MediaReadinessError("measurement_failed", f"ffprobe omitted container facts for {path.name}")
    return codec.lower(), sample_rate, channels, bit_depth, format_name.lower()


def _flac_streaminfo(raw: bytes) -> tuple[int, int] | None:
    if len(raw) < 42 or raw[:4] != b"fLaC" or raw[4] & 0x7F != 0 or int.from_bytes(raw[5:8], "big") != 34:
        return None
    packed = int.from_bytes(raw[18:26], "big")
    return 8, packed & ((1 << 36) - 1)


def _flac_streaminfo_from_path(path: Path) -> tuple[tuple[int, int] | None, int]:
    """Read only FLAC's fixed header/STREAMINFO, without following links."""

    try:
        fd = os.open(path, os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW)
    except OSError as exc:
        raise MediaReadinessError("measurement_failed", f"could not read FLAC header {path.name}: {exc}") from exc
    try:
        if not stat.S_ISREG(os.fstat(fd).st_mode):
            raise MediaReadinessError("measurement_failed", f"FLAC header target is not regular: {path.name}")
        raw = os.pread(fd, 42, 0)
    except OSError as exc:
        raise MediaReadinessError("measurement_failed", f"could not read FLAC header {path.name}: {exc}") from exc
    finally:
        os.close(fd)
    return _flac_streaminfo(raw), int.from_bytes(raw[8:10], "big") if len(raw) >= 10 else 0


def flac_total_samples_only_changed(before: bytes, after: bytes) -> bool:
    """Check the exact header-only FLAC repair contract (and nothing else)."""

    before_info = _flac_streaminfo(before)
    after_info = _flac_streaminfo(after)
    if before_info is None or after_info is None or len(before) != len(after):
        return False
    start, _ = before_info
    # STREAMINFO packs 20 sample-rate bits before the 36-bit total-samples
    # field.  Only the low nibble of byte 12 and the following five bytes may
    # differ; allowing the whole eight-byte word would silently rewrite the
    # rate/channel/bit-depth declaration.
    allowed = set(range(start + 13, start + 18))
    return all(
        left == right or index in allowed
        for index, (left, right) in enumerate(zip(before, after, strict=True))
    ) and (before[start + 12] & 0xF0) == (after[start + 12] & 0xF0)


def _repair_flac_total_samples(path: Path, sample_count: int) -> bool:
    flags = os.O_RDWR | os.O_CLOEXEC | os.O_NOFOLLOW
    try:
        fd = os.open(path, flags)
    except OSError as exc:
        raise MediaReadinessError("measurement_failed", f"could not open FLAC repair target {path.name}: {exc}") from exc
    try:
        if not stat.S_ISREG(os.fstat(fd).st_mode):
            raise MediaReadinessError("measurement_failed", f"FLAC repair target is not regular: {path.name}")
        before_header = os.pread(fd, 42, 0)
        info = _flac_streaminfo(before_header)
        if info is None:
            raise MediaReadinessError("measurement_failed", f"invalid FLAC metadata envelope for {path.name}")
        offset, declared_samples = info
        if declared_samples == sample_count:
            return False
        if sample_count >= 1 << 36:
            raise MediaReadinessError("measurement_failed", f"FLAC sample count overflows STREAMINFO for {path.name}")
        word_offset = offset + 10
        before_word = os.pread(fd, 8, word_offset)
        if len(before_word) != 8:
            raise MediaReadinessError("measurement_failed", f"truncated STREAMINFO for {path.name}")
        after_word = (
            (int.from_bytes(before_word, "big") & ~((1 << 36) - 1)) | sample_count
        ).to_bytes(8, "big")
        # The pwrite scope is exactly the STREAMINFO packed word.  Its high
        # 28 bits and every other file byte remain untouched by construction.
        if before_word[:3] != after_word[:3] or (before_word[3] & 0xF0) != (after_word[3] & 0xF0):
            raise MediaReadinessError("measurement_failed", f"unsafe FLAC repair refused for {path.name}")
        if os.pwrite(fd, after_word, word_offset) != len(after_word):
            raise MediaReadinessError("measurement_failed", f"short FLAC STREAMINFO write for {path.name}")
        os.fsync(fd)
        if os.pread(fd, 8, word_offset) != after_word:
            raise MediaReadinessError("measurement_failed", f"FLAC STREAMINFO verification failed for {path.name}")
        return True
    except OSError as exc:
        raise MediaReadinessError("measurement_failed", f"FLAC repair failed for {path.name}: {exc}") from exc
    finally:
        os.close(fd)


def _facts_for_path(path: Path) -> MediaFileFacts:
    wire = _ffprobe_readiness(path)
    codec, sample_rate, channels, bit_depth, container = _stream_facts(path, wire)
    sample_count, compressed_bytes = _frame_facts(path, wire)
    duration = sample_count / sample_rate
    bitrate = int((compressed_bytes * 8) / duration / 1000) if duration > 0 else None
    return MediaFileFacts(
        path=str(path.resolve()), codec=codec,
        container=container,
        sample_rate=sample_rate, channels=channels, bit_depth=bit_depth,
        sample_count=sample_count, duration_seconds=duration,
        compressed_audio_bytes=compressed_bytes, average_bitrate_kbps=bitrate,
    )


def inspect_media(folder_path: str) -> MediaReadiness:
    """Read canonical audio facts without altering the supplied directory."""

    return MediaReadiness(tuple(_facts_for_path(path) for path in _audio_paths(folder_path)))


def media_facts_for_path(path: str) -> MediaFileFacts:
    """Read one admitted audio file without mutating its containing folder."""

    candidate = Path(path)
    if candidate.suffix.lower() not in AUDIO_EXTENSIONS_DOTTED:
        raise MediaReadinessError("measurement_failed", f"unsupported audio extension: {candidate.name}")
    return _facts_for_path(candidate)


def media_facts_for_open_file(handle: int, *, container: str) -> MediaFileFacts:
    """Read facts through an already-authorized descriptor, never a raw path."""

    if f".{container.lower()}" not in AUDIO_EXTENSIONS_DOTTED:
        raise MediaReadinessError("measurement_failed", f"unsupported audio container: {container}")
    return _facts_for_path(Path(f"/proc/{os.getpid()}/fd/{handle}"))


def prepare_media_readiness(
    folder_path: str,
    *,
    fail_closed: bool = True,
) -> MediaReadiness:
    """Strictly decode, repair admitted private files, then return stream facts.

    Preview uses ``fail_closed=False`` because its existing measurement path
    owns persistence of corrupt/world-failure evidence.  The canonical
    processing path uses the default and refuses to hand a broken world to
    Beets.
    """

    try:
        repair_mp3_headers(folder_path)
        _strict_decode(folder_path)
        initial = inspect_media(folder_path)
        repaired: list[str] = []
        for fact in initial.files:
            if fact.container == "flac" and _repair_flac_total_samples(Path(fact.path), fact.sample_count):
                repaired.append(fact.path)
        if repaired:
            _strict_decode(folder_path)
        final = inspect_media(folder_path)
        return MediaReadiness(final.files, tuple(repaired))
    except MediaReadinessError:
        if fail_closed:
            raise
        # Preview has its own terminal measurement-failure lifecycle.  Every
        # readiness operation is best-effort there, not only strict decode:
        # an unavailable probe or a refused repair must not turn into a
        # worker exception/retry loop.
        return MediaReadiness(())


def normalize_media_metadata(
    folder_path: str,
    *,
    fail_closed: bool = True,
) -> MediaReadiness:
    """Enter the costly readiness path only for an admitted repair candidate.

    Normal albums retain their existing validation/measurement cadence.  This
    is intentionally narrow: zero STREAMINFO sample counts are the allowlisted
    repair condition, while all facts are still available on demand through
    """

    try:
        repair_mp3_headers(folder_path)
        needs_flac_repair = False
        for path in _audio_paths(folder_path):
            if path.suffix.lower() != ".flac":
                continue
            info, min_block_size = _flac_streaminfo_from_path(path)
            if info is None:
                # A non-FLAC byte sequence with a .flac suffix belongs to the
                # ordinary strict validator; do not turn a filename lie into a
                # metadata rewrite attempt.
                continue
            if info[1] == 0:
                needs_flac_repair = True
                break
            if min_block_size > 0 and info[1] < min_block_size:
                # A declared total below the file's smallest frame is impossible.
                # This preserves normal retry cost while admitting a cheap
                # contradiction signal into the one full recovery scan.
                needs_flac_repair = True
                break
        if not needs_flac_repair:
            return MediaReadiness(())
        return prepare_media_readiness(folder_path, fail_closed=fail_closed)
    except MediaReadinessError:
        if fail_closed:
            raise
        return MediaReadiness(())
