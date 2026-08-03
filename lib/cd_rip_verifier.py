"""Positive-only CD rip bit verification against AccurateRip and CTDB.

The verifier is deliberately an evidence producer, not a quality decider.
Every malformed, partial, unavailable, or non-matching provider response
returns ``None`` and therefore has exactly the same policy effect as code that
predates this module.  Only an exact CTDB whole-disc CRC or an all-track
AccurateRip ARv1/ARv2 match at one read offset produces durable evidence.

PCM is decoded into a temporary file a track at a time.  Checksum passes read
bounded chunks from that spool, so the admitted +/-5000 scan has O(tracks *
radius) boundary memory and never materializes an album-sized PCM array.
"""

from __future__ import annotations

import base64
import fcntl
import hashlib
import logging
import multiprocessing
import os
import re
import socket
import struct
import subprocess
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
import zlib
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from fractions import Fraction
from http.client import HTTPMessage, HTTPResponse, IncompleteRead
from multiprocessing.connection import Connection
from multiprocessing.connection import wait as wait_for_connections
from multiprocessing.process import BaseProcess
from pathlib import Path
from typing import IO, BinaryIO, Literal

import msgspec
import numpy as np

from lib.config import CratediggerConfig
from lib.quality import (
    AUDIO_EXTENSIONS_DOTTED,
    AccurateRipBitMatch,
    CdRipBitVerification,
    CdTocIdentity,
    CtdbWholeDiscMatch,
)

logger = logging.getLogger(__name__)

SECTOR_SAMPLES = 588
AR_EDGE_SAMPLES = 5 * SECTOR_SAMPLES
CTDB_STRIDE_SAMPLES = 5880
READ_OFFSET_RADIUS = 5000
UINT32_MASK = 0xFFFFFFFF
PCM_FRAME_BYTES = 4  # stereo signed-16 little-endian, viewed as one uint32
CHECKSUM_CHUNK_SAMPLES = 1_000_000
MAX_PROVIDER_BYTES = 8 * 1024 * 1024
MAX_CACHE_BYTES = 128 * 1024 * 1024
MAX_CACHE_FILES = 512
MAX_AR_BLOCKS = 4096
MAX_AR_CANDIDATES = 64
MAX_ARV2_CANDIDATES = 8
MAX_DISC_SAMPLES = 120 * 60 * 44100
POLITENESS_SECONDS = 0.7
PROVIDER_WALL_SECONDS = 30.0
VERIFIER_WALL_SECONDS = 300.0
PROVIDER_READ_BYTES = 64 * 1024
USER_AGENT = "Cratedigger-CD-Rip-Verifier/1 (+https://github.com/abl030/cratedigger)"


@dataclass(frozen=True)
class _TrackShape:
    path: Path
    samples: int
    source_format: Literal["flac", "alac"]
    track_number: int | None = None
    disc_number: int | None = None


@dataclass(frozen=True)
class _ArIndexes:
    checksums: list[dict[int, int]]
    frame450: list[dict[int, int]]
    response_sha256: str


@dataclass(frozen=True)
class _CtdbEntry:
    entry_id: str
    confidence: int
    crc32: int
    stride_samples: int
    response_toc_sectors: list[int]
    response_toc_shift_sectors: int
    response_sha256: str


class _FfprobeStream(msgspec.Struct, frozen=True):
    codec_name: str = ""
    sample_rate: str | int = ""
    channels: int = 0
    bits_per_raw_sample: str | int = ""
    bits_per_sample: str | int = ""
    duration_ts: str | int = 0
    time_base: str = ""


def _empty_ffprobe_tags() -> dict[str, str]:
    return {}


def _empty_ffprobe_streams() -> list[_FfprobeStream]:
    return []


class _FfprobeFormat(msgspec.Struct, frozen=True):
    tags: dict[str, str] = msgspec.field(default_factory=_empty_ffprobe_tags)


class _FfprobePayload(msgspec.Struct, frozen=True):
    streams: list[_FfprobeStream] = msgspec.field(
        default_factory=_empty_ffprobe_streams
    )
    format: _FfprobeFormat = msgspec.field(default_factory=_FfprobeFormat)


ProviderFetch = Callable[[str, Path, float], bytes | None]


def _check_deadline(deadline: float | None) -> None:
    if deadline is not None and time.monotonic() >= deadline:
        raise TimeoutError("CD rip verifier exceeded its wall deadline")


def _freedb_id(toc: list[int]) -> int:
    tracks = len(toc) - 1
    digits = "".join(str(abs(toc[index]) // 75 + 2) for index in range(tracks))
    digit_sum = sum(ord(char) - ord("0") for char in digits)
    duration = abs(toc[-1]) // 75 - abs(toc[0]) // 75
    return ((digit_sum % 255) << 24) | (duration << 8) | tracks


def _toc_ids(toc: list[int]) -> tuple[int, int, int, str, str]:
    tracks = len(toc) - 1
    id1 = sum(toc) & UINT32_MASK
    id2 = sum(max(1, offset) * ordinal for ordinal, offset in enumerate(toc, 1))
    id2 &= UINT32_MASK
    freedb = _freedb_id(toc)
    ar_id = f"{id1:08x}-{id2:08x}-{freedb:08x}"

    disc_payload = "01" + f"{tracks:02X}" + f"{toc[-1] + 150:08X}"
    disc_payload += "".join(f"{offset + 150:08X}" for offset in toc[:-1])
    disc_payload = disc_payload.ljust(804, "0")
    digest = hashlib.sha1(disc_payload.encode("ascii")).digest()
    mb_disc_id = (
        base64.b64encode(digest).decode("ascii")
        .replace("+", ".")
        .replace("/", "_")
        .replace("=", "-")
    )
    return id1, id2, freedb, ar_id, mb_disc_id


def build_cd_toc(track_lengths: list[int]) -> CdTocIdentity:
    """Build provider IDs for exact sector-aligned stereo sample counts."""
    if not track_lengths or len(track_lengths) > 99:
        raise ValueError("CD shape requires 1-99 tracks")
    if any(length <= 0 or length % SECTOR_SAMPLES for length in track_lengths):
        raise ValueError("CD tracks must have positive sector-aligned lengths")
    offsets: list[int] = []
    total = 0
    for length in track_lengths:
        offsets.append(total // SECTOR_SAMPLES)
        total += length
    toc = [*offsets, total // SECTOR_SAMPLES]
    _id1, _id2, _freedb, ar_id, mb_disc_id = _toc_ids(toc)
    return CdTocIdentity(
        track_offsets_sectors=offsets,
        leadout_sector=toc[-1],
        accuraterip_id=ar_id,
        musicbrainz_disc_id=mb_disc_id,
    )


def accuraterip_url(toc: CdTocIdentity) -> str:
    id1 = toc.accuraterip_id.split("-", 1)[0]
    tracks = len(toc.track_offsets_sectors)
    return (
        "https://www.accuraterip.com/accuraterip/"
        f"{id1[-1]}/{id1[-2]}/{id1[-3]}/"
        f"dBAR-{tracks:03d}-{toc.accuraterip_id}.bin"
    )


def ctdb_url(toc: CdTocIdentity) -> str:
    offsets = [*toc.track_offsets_sectors, toc.leadout_sector]
    toc_param = ":".join(str(offset) for offset in offsets)
    return (
        "https://db.cue.tools/lookup2.php?version=3&ctdb=1&fuzzy=1"
        f"&metadata=default&toc={toc_param}"
    )


def parse_accuraterip(blob: bytes, toc: CdTocIdentity) -> _ArIndexes:
    """Strictly parse the repeated dBAR header + per-track record format."""
    track_count = len(toc.track_offsets_sectors)
    toc_values = [*toc.track_offsets_sectors, toc.leadout_sector]
    expected_id1, expected_id2, expected_freedb, _ar_id, _mbid = _toc_ids(toc_values)
    checksums = [dict[int, int]() for _ in range(track_count)]
    frame450 = [dict[int, int]() for _ in range(track_count)]
    position = 0
    blocks = 0
    while position < len(blob):
        if blocks >= MAX_AR_BLOCKS or position + 13 > len(blob):
            raise ValueError("malformed AccurateRip header")
        count, id1, id2, freedb = struct.unpack_from("<BIII", blob, position)
        position += 13
        if (
            count != track_count
            or id1 != expected_id1
            or id2 != expected_id2
            or freedb != expected_freedb
        ):
            raise ValueError("AccurateRip response TOC identity mismatch")
        block_bytes = count * 9
        if position + block_bytes > len(blob):
            raise ValueError("truncated AccurateRip track block")
        for track_index in range(count):
            confidence, checksum, checksum450 = struct.unpack_from(
                "<BII", blob, position
            )
            position += 9
            if confidence <= 0:
                continue
            slot = checksums[track_index]
            slot[checksum] = max(slot.get(checksum, 0), confidence)
            if checksum450:
                offset_slot = frame450[track_index]
                offset_slot[checksum450] = max(
                    offset_slot.get(checksum450, 0), confidence
                )
        blocks += 1
    if position != len(blob) or blocks == 0:
        raise ValueError("empty or trailing AccurateRip response")
    return _ArIndexes(
        checksums=checksums,
        frame450=frame450,
        response_sha256=hashlib.sha256(blob).hexdigest(),
    )


_HEX32 = re.compile(r"[0-9a-fA-F]{1,8}\Z")


def parse_ctdb(xml: bytes, toc: CdTocIdentity) -> list[_CtdbEntry]:
    """Parse only complete, positive CTDB entries with the admitted stride."""
    track_count = len(toc.track_offsets_sectors)
    expected_toc = [*toc.track_offsets_sectors, toc.leadout_sector]
    lowered = xml.lower()
    if b"<!doctype" in lowered or b"<!entity" in lowered:
        raise ValueError("CTDB XML declarations are not admitted")
    try:
        root = ET.fromstring(xml)
    except ET.ParseError as exc:
        raise ValueError("malformed CTDB XML") from exc
    entries: list[_CtdbEntry] = []
    for element in root.iter():
        if element.tag.rsplit("}", 1)[-1] != "entry":
            continue
        attrs = element.attrib
        toc_text = attrs.get("toc", "")
        if re.fullmatch(r"\d+(?::\d+)+", toc_text) is None:
            continue
        response_toc = [int(value) for value in toc_text.split(":")]
        response_toc_shift = response_toc[0]
        normalized_toc = [
            value - response_toc_shift for value in response_toc
        ]
        if normalized_toc != expected_toc:
            continue
        track_crcs = attrs.get("trackcrcs", "").split()
        crc_text = attrs.get("crc32", "")
        if len(track_crcs) != track_count or not _HEX32.fullmatch(crc_text):
            continue
        if any(_HEX32.fullmatch(value) is None for value in track_crcs):
            continue
        try:
            confidence = int(attrs.get("confidence", "0"))
            stride = int(attrs.get("stride", str(CTDB_STRIDE_SAMPLES)))
        except ValueError:
            continue
        entry_id = attrs.get("id", "").strip()
        if confidence <= 0 or not entry_id or stride != CTDB_STRIDE_SAMPLES:
            continue
        entries.append(
            _CtdbEntry(
                entry_id=entry_id,
                confidence=confidence,
                crc32=int(crc_text, 16),
                stride_samples=stride,
                response_toc_sectors=response_toc,
                response_toc_shift_sectors=response_toc_shift,
                response_sha256=hashlib.sha256(xml).hexdigest(),
            )
        )
    return entries


def _read_pcm_values(
    pcm: BinaryIO,
    *,
    start: int,
    count: int,
    total_samples: int,
) -> np.ndarray:
    """Read a zero-padded global sample interval as little-endian uint32."""
    values = np.zeros(count, dtype=np.uint64)
    source_start = max(start, 0)
    source_end = min(start + count, total_samples)
    if source_end <= source_start:
        return values
    pcm.seek(source_start * PCM_FRAME_BYTES)
    raw = pcm.read((source_end - source_start) * PCM_FRAME_BYTES)
    if len(raw) != (source_end - source_start) * PCM_FRAME_BYTES:
        raise ValueError("short PCM spool read")
    decoded = np.frombuffer(raw, dtype="<u4").astype(np.uint64)
    destination = source_start - start
    values[destination:destination + len(decoded)] = decoded
    return values


def _weighted_interval(
    pcm: BinaryIO,
    *,
    source_start: int,
    weight_start: int,
    count: int,
    total_samples: int,
    include_arv2: bool,
    deadline: float | None = None,
) -> tuple[int, int, int]:
    v1 = 0
    v2 = 0
    unweighted = 0
    for chunk_start in range(0, count, CHECKSUM_CHUNK_SAMPLES):
        _check_deadline(deadline)
        chunk_count = min(CHECKSUM_CHUNK_SAMPLES, count - chunk_start)
        values = _read_pcm_values(
            pcm,
            start=source_start + chunk_start,
            count=chunk_count,
            total_samples=total_samples,
        )
        weights = np.arange(
            weight_start + chunk_start,
            weight_start + chunk_start + chunk_count,
            dtype=np.uint64,
        )
        products = values * weights
        v1 = (v1 + int(np.sum(products, dtype=np.uint64))) & UINT32_MASK
        unweighted = (
            unweighted + int(np.sum(values, dtype=np.uint64))
        ) & UINT32_MASK
        if include_arv2:
            folded = (products & np.uint64(UINT32_MASK)) + (products >> 32)
            v2 = (v2 + int(np.sum(folded, dtype=np.uint64))) & UINT32_MASK
        _check_deadline(deadline)
    return v1, v2, unweighted


def accuraterip_checksums_at(
    pcm: BinaryIO,
    *,
    track_start: int,
    track_length: int,
    track_index: int,
    track_count: int,
    read_offset: int,
    total_samples: int,
    deadline: float | None = None,
) -> tuple[int, int]:
    """Exact integer ARv1/ARv2 checksums from the corrected frozen algorithm."""
    lo = AR_EDGE_SAMPLES if track_index == 0 else 1
    hi = track_length - AR_EDGE_SAMPLES if track_index == track_count - 1 else track_length
    if hi < lo:
        raise ValueError("track is too short for AccurateRip edge exclusions")
    v1, v2, _sum = _weighted_interval(
        pcm,
        source_start=track_start + read_offset + lo - 1,
        weight_start=lo,
        count=hi - lo + 1,
        total_samples=total_samples,
        include_arv2=True,
        deadline=deadline,
    )
    return v1, v2


def _scan_weighted_v1(
    pcm: BinaryIO,
    *,
    track_start: int,
    source_lo: int,
    source_hi: int,
    weight_start: int,
    total_samples: int,
    wanted: set[int],
    radius: int,
    deadline: float | None = None,
) -> dict[int, int]:
    """Scan a fixed local-weight interval using exact mod-2^32 recurrence."""
    if source_hi < source_lo or not wanted:
        return {}
    count = source_hi - source_lo + 1
    weight_end = weight_start + count - 1
    source_start = track_start - radius + source_lo - 1
    checksum, _v2, interval_sum = _weighted_interval(
        pcm,
        source_start=source_start,
        weight_start=weight_start,
        count=count,
        total_samples=total_samples,
        include_arv2=False,
        deadline=deadline,
    )
    leaving = _read_pcm_values(
        pcm,
        start=source_start,
        count=radius * 2,
        total_samples=total_samples,
    )
    entering = _read_pcm_values(
        pcm,
        start=source_start + count,
        count=radius * 2,
        total_samples=total_samples,
    )
    hits = {-radius: checksum} if checksum in wanted else {}
    for step in range(radius * 2):
        _check_deadline(deadline)
        old = int(leaving[step])
        new = int(entering[step])
        checksum = (
            checksum
            - interval_sum
            - (weight_start - 1) * old
            + weight_end * new
        ) & UINT32_MASK
        interval_sum = (interval_sum - old + new) & UINT32_MASK
        offset = -radius + step + 1
        if checksum in wanted:
            hits[offset] = checksum
    return hits


def verify_accuraterip_pcm(
    pcm: BinaryIO,
    *,
    track_lengths: list[int],
    indexes: _ArIndexes,
    url: str,
    radius: int = READ_OFFSET_RADIUS,
    deadline: float | None = None,
) -> AccurateRipBitMatch | None:
    """Return only a same-version, same-offset, all-track AR match."""
    if radius < 0 or radius > READ_OFFSET_RADIUS:
        raise ValueError("read offset radius outside admitted range")
    track_count = len(track_lengths)
    if (
        track_count == 0
        or len(indexes.checksums) != track_count
        or any(not slot for slot in indexes.checksums)
    ):
        return None
    total_samples = sum(track_lengths)
    starts: list[int] = []
    position = 0
    for length in track_lengths:
        starts.append(position)
        position += length

    v1_matches_by_track: list[dict[int, int]] = []
    common_v1: set[int] | None = None
    common_450: set[int] | None = None
    for track_index, (track_start, track_length) in enumerate(
        zip(starts, track_lengths, strict=True)
    ):
        lo = AR_EDGE_SAMPLES if track_index == 0 else 1
        hi = (
            track_length - AR_EDGE_SAMPLES
            if track_index == track_count - 1
            else track_length
        )
        if hi < lo:
            return None
        v1_hits = _scan_weighted_v1(
            pcm,
            track_start=track_start,
            source_lo=lo,
            source_hi=hi,
            weight_start=lo,
            total_samples=total_samples,
            wanted=set(indexes.checksums[track_index]),
            radius=radius,
            deadline=deadline,
        )
        v1_matches_by_track.append(v1_hits)
        v1_offsets = set(v1_hits)
        common_v1 = (
            v1_offsets if common_v1 is None else common_v1 & v1_offsets
        )

        c450_slot = indexes.frame450[track_index]
        frame_lo = 450 * SECTOR_SAMPLES + 1
        frame_hi = frame_lo + SECTOR_SAMPLES - 1
        if c450_slot and frame_lo >= lo and frame_hi <= hi:
            c450_hits = _scan_weighted_v1(
                pcm,
                track_start=track_start,
                source_lo=max(lo, frame_lo),
                source_hi=min(hi, frame_hi),
                weight_start=1,
                total_samples=total_samples,
                wanted=set(c450_slot),
                radius=radius,
                deadline=deadline,
            )
            c450_offsets = set(c450_hits)
            common_450 = (
                c450_offsets
                if common_450 is None else common_450 & c450_offsets
            )

    v1_candidates: set[int] = set(common_v1 or ())
    if len(v1_candidates) > MAX_AR_CANDIDATES:
        v1_candidates = set[int]()
    matches: list[AccurateRipBitMatch] = []
    for offset in sorted(v1_candidates):
        _check_deadline(deadline)
        checksums = [slot[offset] for slot in v1_matches_by_track]
        confidences = [
            indexes.checksums[track_index][checksum]
            for track_index, checksum in enumerate(checksums)
        ]
        matches.append(
            AccurateRipBitMatch(
                provider="accuraterip",
                url=url,
                checksum_version="arv1",
                read_offset_samples=offset,
                track_confidences=confidences,
                track_checksums=checksums,
                response_sha256=indexes.response_sha256,
            )
        )

    v2_frame_candidates: set[int] = set(common_450 or ())
    if len(v2_frame_candidates) > MAX_ARV2_CANDIDATES:
        v2_frame_candidates = set[int]()
    v2_nonzero_candidates = v2_frame_candidates - {0}
    v2_candidates: set[int] = v2_nonzero_candidates | {0}
    for offset in sorted(v2_candidates):
        _check_deadline(deadline)
        v2_confidences: list[int] = []
        v2_checksums: list[int] = []
        for track_index, (track_start, track_length) in enumerate(
            zip(starts, track_lengths, strict=True)
        ):
            _v1, v2 = accuraterip_checksums_at(
                pcm,
                track_start=track_start,
                track_length=track_length,
                track_index=track_index,
                track_count=track_count,
                read_offset=offset,
                total_samples=total_samples,
                deadline=deadline,
            )
            slot = indexes.checksums[track_index]
            v2_confidences.append(slot.get(v2, 0))
            v2_checksums.append(v2)
        if all(v2_confidences):
            matches.append(
                AccurateRipBitMatch(
                    provider="accuraterip",
                    url=url,
                    checksum_version="arv2",
                    read_offset_samples=offset,
                    track_confidences=v2_confidences,
                    track_checksums=v2_checksums,
                    response_sha256=indexes.response_sha256,
                )
            )
    if not matches:
        return None
    # Deterministic and confidence-preserving when duplicate provider blocks
    # happen to make more than one complete match possible.
    return max(
        matches,
        key=lambda match: (
            min(match.track_confidences),
            sum(match.track_confidences),
            match.checksum_version == "arv2",
            -abs(match.read_offset_samples),
            -match.read_offset_samples,
        ),
    )


def _ctdb_crc(
    pcm: BinaryIO,
    total_samples: int,
    *,
    deadline: float | None = None,
) -> int | None:
    start = CTDB_STRIDE_SAMPLES
    stop = total_samples - CTDB_STRIDE_SAMPLES - (
        total_samples % CTDB_STRIDE_SAMPLES
    )
    if stop <= start:
        return None
    pcm.seek(start * PCM_FRAME_BYTES)
    remaining = (stop - start) * PCM_FRAME_BYTES
    checksum = 0
    while remaining:
        _check_deadline(deadline)
        chunk = pcm.read(min(remaining, CHECKSUM_CHUNK_SAMPLES * PCM_FRAME_BYTES))
        if not chunk:
            raise ValueError("short PCM spool read")
        checksum = zlib.crc32(chunk, checksum) & UINT32_MASK
        remaining -= len(chunk)
        _check_deadline(deadline)
    return checksum


def verify_ctdb_pcm(
    pcm: BinaryIO,
    *,
    total_samples: int,
    entries: list[_CtdbEntry],
    url: str,
    deadline: float | None = None,
) -> CtdbWholeDiscMatch | None:
    checksum = _ctdb_crc(pcm, total_samples, deadline=deadline)
    if checksum is None:
        return None
    matches = [entry for entry in entries if entry.crc32 == checksum]
    if not matches:
        return None
    entry = max(matches, key=lambda candidate: candidate.confidence)
    return CtdbWholeDiscMatch(
        provider="ctdb",
        url=url,
        entry_id=entry.entry_id,
        confidence=entry.confidence,
        crc32=checksum,
        stride_samples=entry.stride_samples,
        response_toc_sectors=entry.response_toc_sectors,
        response_toc_shift_sectors=entry.response_toc_shift_sectors,
        response_sha256=entry.response_sha256,
    )


def _probe_track(path: Path, *, deadline: float | None = None) -> _TrackShape:
    _check_deadline(deadline)
    timeout = 15.0
    if deadline is not None:
        timeout = min(timeout, max(0.001, deadline - time.monotonic()))
    result = subprocess.run(
        [
            "ffprobe", "-v", "error", "-select_streams", "a",
            "-show_entries",
            "stream=codec_name,sample_rate,channels,bits_per_raw_sample,bits_per_sample,duration_ts,time_base:format_tags",
            "-of", "json", str(path),
        ],
        capture_output=True,
        timeout=timeout,
        check=False,
    )
    _check_deadline(deadline)
    if result.returncode != 0 or len(result.stdout) > 256 * 1024:
        raise ValueError("ffprobe failed")
    payload = msgspec.json.decode(result.stdout, type=_FfprobePayload)
    if len(payload.streams) != 1:
        raise ValueError("exactly one audio stream is required")
    stream = payload.streams[0]
    codec = stream.codec_name
    extension = path.suffix.lower()
    if extension == ".flac" and codec == "flac":
        source_format: Literal["flac", "alac"] = "flac"
    elif extension in {".m4a", ".alac"} and codec == "alac":
        source_format = "alac"
    else:
        raise ValueError("source is not exact FLAC or ALAC")
    bits = stream.bits_per_raw_sample or stream.bits_per_sample
    if int(stream.sample_rate or 0) != 44100 or stream.channels != 2:
        raise ValueError("source is not 44.1 kHz stereo")
    if int(bits or 0) != 16:
        raise ValueError("source is not 16-bit PCM")
    duration_ts = int(stream.duration_ts or 0)
    time_base = Fraction(stream.time_base or "0/1")
    sample_count = duration_ts * time_base * 44100
    if sample_count.denominator != 1:
        raise ValueError("source duration is not an exact sample count")
    samples = sample_count.numerator
    if samples <= 0 or samples >= 2**32 or samples % SECTOR_SAMPLES:
        raise ValueError("source is not an admitted CD-shaped track")
    tags = {
        key.casefold(): value
        for key, value in payload.format.tags.items()
    }
    return _TrackShape(
        path=path,
        samples=samples,
        source_format=source_format,
        track_number=_metadata_number(tags, ("track", "tracknumber")),
        disc_number=_metadata_number(tags, ("disc", "discnumber")),
    )


def _metadata_number(
    tags: Mapping[str, object],
    names: tuple[str, ...],
) -> int | None:
    values = [tags[name] for name in names if name in tags]
    if not values:
        return None
    parsed: set[int] = set()
    for value in values:
        text = str(value).split("/", 1)[0].strip()
        if re.fullmatch(r"[1-9]\d*", text) is None:
            raise ValueError("invalid embedded CD track/disc number")
        parsed.add(int(text))
    if len(parsed) != 1:
        raise ValueError("conflicting embedded CD track/disc numbers")
    return next(iter(parsed))


def _order_tracks(shapes: list[_TrackShape]) -> list[_TrackShape]:
    """Use complete embedded numbering, else a strict natural-number order."""
    with_track_tags = [shape.track_number is not None for shape in shapes]
    if any(with_track_tags):
        if not all(with_track_tags):
            raise ValueError("partial embedded track ordering is ambiguous")
        disc_tags = [shape.disc_number is not None for shape in shapes]
        if any(disc_tags) and not all(disc_tags):
            raise ValueError("partial embedded disc ordering is ambiguous")
        discs = {
            shape.disc_number for shape in shapes
            if shape.disc_number is not None
        }
        if len(discs) > 1:
            raise ValueError("multi-disc metadata is not one CD-shaped album")
        numbers = [int(shape.track_number or 0) for shape in shapes]
        if sorted(numbers) != list(range(1, len(shapes) + 1)):
            raise ValueError("embedded track numbers must be unique and contiguous")
        return sorted(shapes, key=lambda shape: int(shape.track_number or 0))

    token_rows = [re.findall(r"\d+", shape.path.stem) for shape in shapes]
    aligned_candidates: set[tuple[int, ...]] = set()
    for token_index in range(min((len(tokens) for tokens in token_rows), default=0)):
        values = [int(tokens[token_index]) for tokens in token_rows]
        if sorted(values) == list(range(1, len(shapes) + 1)):
            aligned_candidates.add(tuple(values))
    if len(aligned_candidates) != 1:
        raise ValueError(
            "filename fallback requires one aligned, unique, contiguous "
            "track-number token"
        )
    numbers = next(iter(aligned_candidates))
    numbered = zip(numbers, shapes, strict=True)
    return [
        shape for _number, shape in sorted(numbered, key=lambda item: item[0])
    ]


def _album_shape(
    path: str,
    *,
    deadline: float | None = None,
) -> list[_TrackShape]:
    root = Path(path)
    if not root.is_dir():
        raise ValueError("album path is not a directory")
    nested_audio = [
        entry for entry in root.rglob("*")
        if entry.is_file()
        and entry.suffix.lower() in AUDIO_EXTENSIONS_DOTTED
        and entry.parent != root
    ]
    if nested_audio:
        raise ValueError("nested CD audio is not admitted")
    audio = [
        entry for entry in root.iterdir()
        if entry.is_file()
        and entry.suffix.lower() in AUDIO_EXTENSIONS_DOTTED
    ]
    if not audio:
        raise ValueError("album has no audio tracks")
    shapes = _order_tracks([
        _probe_track(track, deadline=deadline) for track in audio
    ])
    if len({shape.source_format for shape in shapes}) != 1:
        raise ValueError("mixed FLAC/ALAC album is not admitted")
    if sum(shape.samples for shape in shapes) > MAX_DISC_SAMPLES:
        raise ValueError("CD-shaped album exceeds the admitted disc duration")
    return shapes


def _decode_to_spool(
    tracks: list[_TrackShape],
    pcm: BinaryIO,
    *,
    deadline: float,
) -> None:
    for track in tracks:
        _check_deadline(deadline)
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError("album PCM decode exceeded verifier deadline")
        before = pcm.tell()
        result = subprocess.run(
            [
                "ffmpeg", "-v", "error", "-nostdin", "-i", str(track.path),
                "-map", "0:a", "-f", "s16le", "-acodec", "pcm_s16le",
                "-ar", "44100", "-ac", "2", "-",
            ],
            stdout=pcm,
            stderr=subprocess.PIPE,
            timeout=max(0.001, min(120, remaining)),
            check=False,
        )
        expected = track.samples * PCM_FRAME_BYTES
        if result.returncode != 0 or pcm.tell() - before != expected:
            raise ValueError("decoded PCM does not match the probed track shape")
        _check_deadline(deadline)
    pcm.flush()


def _ensure_private_dir(path: Path) -> None:
    path.mkdir(mode=0o700, parents=True, exist_ok=True)
    os.chmod(path, 0o700)


def _cache_path(cache_dir: Path, url: str) -> Path:
    return cache_dir / f"{hashlib.sha256(url.encode()).hexdigest()}.positive"


def _prune_cache(cache_dir: Path, *, deadline: float) -> None:
    _check_deadline(deadline)
    files = sorted(
        (path for path in cache_dir.glob("*.positive") if path.is_file()),
        key=lambda path: path.stat().st_mtime_ns,
        reverse=True,
    )
    retained_bytes = 0
    for index, path in enumerate(files):
        _check_deadline(deadline)
        size = path.stat().st_size
        retained_bytes += size
        if index >= MAX_CACHE_FILES or retained_bytes > MAX_CACHE_BYTES:
            try:
                path.unlink()
            except FileNotFoundError:
                pass


class _NoRedirectHandler(urllib.request.HTTPRedirectHandler):
    """Refuse redirects before urllib can issue a request to their target."""

    def redirect_request(
        self,
        req: urllib.request.Request,
        fp: IO[bytes],
        code: int,
        msg: str,
        headers: HTTPMessage,
        newurl: str,
    ) -> None:
        del req, fp, code, msg, headers, newurl


def _set_response_timeout(response: HTTPResponse, remaining: float) -> None:
    """Narrow the live response socket timeout to the wall time remaining."""
    fp: object | None = getattr(response, "fp", None)
    raw: object | None = getattr(fp, "raw", None)
    for owner in (response, fp, raw):
        socket_candidate: object | None = getattr(owner, "_sock", None)
        if isinstance(socket_candidate, socket.socket):
            socket_candidate.settimeout(max(0.001, remaining))
            return


def _read_bounded_response(response: HTTPResponse, *, deadline: float) -> bytes:
    declared = response.headers.get("Content-Length")
    declared_size: int | None = None
    if declared is not None:
        declared_size = int(declared)
        if declared_size < 0 or declared_size > MAX_PROVIDER_BYTES:
            raise ValueError("provider response size is outside admitted bounds")
    payload = bytearray()
    while declared_size is None or len(payload) < declared_size:
        _check_deadline(deadline)
        remaining = deadline - time.monotonic()
        _set_response_timeout(response, remaining)
        bytes_remaining = (
            declared_size - len(payload)
            if declared_size is not None
            else MAX_PROVIDER_BYTES + 1 - len(payload)
        )
        incomplete = False
        try:
            chunk = response.read1(min(
                PROVIDER_READ_BYTES,
                bytes_remaining,
            ))
        except IncompleteRead as exc:
            chunk = exc.partial
            incomplete = True
        _check_deadline(deadline)
        if not chunk:
            break
        payload.extend(chunk)
        if len(payload) > MAX_PROVIDER_BYTES:
            raise ValueError("provider response exceeds admitted size")
        if incomplete:
            break
    if declared_size is not None and len(payload) != declared_size:
        raise ValueError("provider response ended before Content-Length")
    return bytes(payload)


def _fetch_https_no_redirect(url: str, *, deadline: float) -> bytes | None:
    requested = urllib.parse.urlparse(url)
    if (
        requested.scheme != "https"
        or requested.hostname is None
        or requested.username is not None
        or requested.password is not None
        or requested.port not in (None, 443)
    ):
        return None
    request_deadline = min(
        deadline,
        time.monotonic() + PROVIDER_WALL_SECONDS,
    )
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    opener = urllib.request.build_opener(_NoRedirectHandler())
    try:
        opened = opener.open(
            request,
            timeout=max(0.001, request_deadline - time.monotonic()),
        )
        if not isinstance(opened, HTTPResponse):
            return None
        with opened as response:
            final = urllib.parse.urlparse(response.url)
            if (
                response.status != 200
                or final.scheme != "https"
                or final.hostname != requested.hostname
                or final.port != requested.port
            ):
                return None
            payload = _read_bounded_response(
                response,
                deadline=request_deadline,
            )
    except (
        OSError,
        TimeoutError,
        urllib.error.URLError,
        urllib.error.HTTPError,
        ValueError,
    ):
        return None
    return payload or None


def _acquire_lock(lock: BinaryIO, *, deadline: float) -> None:
    while True:
        _check_deadline(deadline)
        try:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            return
        except BlockingIOError:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                _check_deadline(deadline)
            time.sleep(min(0.05, remaining))


def _deadline_sleep(delay: float, *, deadline: float) -> None:
    delay = max(0.0, delay)
    if delay == 0:
        return
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        _check_deadline(deadline)
    time.sleep(min(delay, remaining))
    _check_deadline(deadline)


def fetch_positive(
    url: str,
    cache_dir: Path,
    deadline: float | None = None,
) -> bytes | None:
    """Read a verified-positive cache entry or fetch an uncommitted payload.

    Network bytes are returned to the verifier but are not durable yet. Only
    the caller that parses them and finds an exact album match may commit them
    with ``_store_positive``; a crash between I/O and verification therefore
    cannot strand malformed or non-matching bytes in the positive cache.
    """
    if urllib.parse.urlparse(url).scheme != "https":
        return None
    if deadline is None:
        deadline = time.monotonic() + PROVIDER_WALL_SECONDS
    _check_deadline(deadline)
    _ensure_private_dir(cache_dir)
    cache_path = _cache_path(cache_dir, url)
    lock_path = cache_dir / ".fetch.lock"
    with lock_path.open("a+b") as lock:
        _acquire_lock(lock, deadline=deadline)
        try:
            _check_deadline(deadline)
            try:
                cached = cache_path.read_bytes()
            except FileNotFoundError:
                cached = None
            if cached is not None:
                if 0 < len(cached) <= MAX_PROVIDER_BYTES:
                    return cached
                cache_path.unlink(missing_ok=True)

            stamp_path = cache_dir / ".last-fetch"
            try:
                last_fetch = float(stamp_path.read_text(encoding="ascii"))
            except (FileNotFoundError, ValueError, OSError):
                last_fetch = 0.0
            delay = min(
                POLITENESS_SECONDS,
                max(0.0, POLITENESS_SECONDS - (time.time() - last_fetch)),
            )
            _deadline_sleep(delay, deadline=deadline)
            stamp_path.write_text(str(time.time()), encoding="ascii")
            _check_deadline(deadline)
            return _fetch_https_no_redirect(url, deadline=deadline)
        finally:
            fcntl.flock(lock.fileno(), fcntl.LOCK_UN)


def _store_positive(
    url: str,
    cache_dir: Path,
    payload: bytes,
    *,
    deadline: float | None = None,
) -> None:
    """Atomically retain bytes only after an exact verifier match."""
    if not payload or len(payload) > MAX_PROVIDER_BYTES:
        return
    if deadline is None:
        deadline = time.monotonic() + PROVIDER_WALL_SECONDS
    _check_deadline(deadline)
    _ensure_private_dir(cache_dir)
    cache_path = _cache_path(cache_dir, url)
    lock_path = cache_dir / ".fetch.lock"
    with lock_path.open("a+b") as lock:
        _acquire_lock(lock, deadline=deadline)
        temporary_path: Path | None = None
        try:
            _check_deadline(deadline)
            with tempfile.NamedTemporaryFile(
                dir=cache_dir,
                delete=False,
            ) as temporary:
                temporary.write(payload)
                temporary.flush()
                os.fsync(temporary.fileno())
                temporary_path = Path(temporary.name)
            os.replace(temporary_path, cache_path)
            temporary_path = None
            _prune_cache(cache_dir, deadline=deadline)
        finally:
            if temporary_path is not None:
                temporary_path.unlink(missing_ok=True)
            fcntl.flock(lock.fileno(), fcntl.LOCK_UN)


def _provider_process_target(
    connection: Connection,
    fetch: ProviderFetch,
    url: str,
    cache_dir: Path,
    deadline: float,
) -> None:
    """Run one provider operation in a disposable spawn-context child."""
    try:
        try:
            payload = fetch(url, cache_dir, deadline)
            if payload is None:
                message = b"\x00"
            elif 0 < len(payload) <= MAX_PROVIDER_BYTES:
                message = b"\x01" + payload
            else:
                message = b"\x02"
        except BaseException:  # noqa: BLE001 - child reports no opinion
            message = b"\x02"
        try:
            connection.send_bytes(message)
        except (BrokenPipeError, OSError):
            pass
    finally:
        connection.close()


def _reap_provider_process(
    process: BaseProcess,
    *,
    terminate: bool,
) -> None:
    """Leave no provider child alive, including an uncooperative operation."""
    if not terminate:
        process.join(timeout=0.05)
    if process.is_alive():
        process.terminate()
        process.join(timeout=0.2)
    if process.is_alive():
        process.kill()
        process.join()
    process.close()


def _provider_payloads(
    fetch: ProviderFetch,
    requests: dict[str, tuple[str, Path]],
    *,
    deadline: float,
    provider_wall_seconds: float = PROVIDER_WALL_SECONDS,
) -> dict[str, bytes | None]:
    """Fetch providers concurrently in killable spawn-context children."""
    if provider_wall_seconds <= 0:
        raise ValueError("provider wall budget must be positive")
    context = multiprocessing.get_context("spawn")
    payloads: dict[str, bytes | None] = {
        provider: None for provider in requests
    }
    workers: dict[Connection, tuple[str, BaseProcess, float]] = {}
    try:
        for provider, (url, cache_dir) in requests.items():
            _check_deadline(deadline)
            lane_deadline = min(
                deadline,
                time.monotonic() + provider_wall_seconds,
            )
            receive, send = context.Pipe(duplex=False)
            process = context.Process(
                target=_provider_process_target,
                args=(send, fetch, url, cache_dir, lane_deadline),
                name=f"cratedigger-cd-rip-{provider.casefold()}",
            )
            try:
                process.start()
            except Exception:
                receive.close()
                send.close()
                logger.info(
                    "%s fetch withheld after provider process startup failure",
                    provider,
                    exc_info=True,
                )
                continue
            send.close()
            workers[receive] = (provider, process, lane_deadline)

        while workers:
            now = time.monotonic()
            expired = [
                connection
                for connection, (_provider, _process, lane_deadline)
                in workers.items()
                if now >= lane_deadline
            ]
            for connection in expired:
                provider, process, _lane_deadline = workers.pop(connection)
                connection.close()
                _reap_provider_process(process, terminate=True)
                logger.info("%s fetch withheld after provider deadline", provider)
            if not workers:
                break

            nearest_deadline = min(
                lane_deadline
                for _provider, _process, lane_deadline in workers.values()
            )
            ready_waitables = wait_for_connections(
                list(workers),
                timeout=max(0.0, nearest_deadline - time.monotonic()),
            )
            ready = [
                connection
                for connection in workers
                if connection in ready_waitables
            ]
            for connection in ready:
                provider, process, _lane_deadline = workers.pop(connection)
                try:
                    message = connection.recv_bytes(MAX_PROVIDER_BYTES + 1)
                    if message[:1] == b"\x01" and len(message) > 1:
                        payloads[provider] = message[1:]
                    elif message != b"\x00":
                        logger.info(
                            "%s fetch withheld after provider exception",
                            provider,
                        )
                except (EOFError, OSError):
                    logger.info(
                        "%s fetch withheld after provider exception",
                        provider,
                        exc_info=True,
                    )
                finally:
                    connection.close()
                    _reap_provider_process(process, terminate=False)
    finally:
        for connection, (_provider, process, _lane_deadline) in workers.items():
            connection.close()
            _reap_provider_process(process, terminate=True)
    return payloads


def _provider_payloads_direct(
    fetch: ProviderFetch,
    requests: dict[str, tuple[str, Path]],
    *,
    deadline: float,
    provider_wall_seconds: float,
) -> dict[str, bytes | None]:
    """In-process seam for deterministic injected test providers only."""
    payloads: dict[str, bytes | None] = {
        provider: None for provider in requests
    }
    for provider, (url, cache_dir) in requests.items():
        _check_deadline(deadline)
        lane_deadline = min(
            deadline,
            time.monotonic() + provider_wall_seconds,
        )
        try:
            payload = fetch(url, cache_dir, lane_deadline)
            if time.monotonic() < lane_deadline:
                payloads[provider] = payload
        except Exception:
            logger.info(
                "%s injected fetch withheld after provider exception",
                provider,
                exc_info=True,
            )
    return payloads


def _best_effort_cache_remove(path: Path, *, deadline: float) -> None:
    try:
        _check_deadline(deadline)
        path.unlink(missing_ok=True)
    except Exception:
        logger.info("CD rip cache removal failed", exc_info=True)


def _best_effort_cache_store(
    url: str,
    cache_dir: Path,
    payload: bytes,
    *,
    deadline: float,
) -> None:
    try:
        _store_positive(url, cache_dir, payload, deadline=deadline)
    except Exception:
        logger.info("CD rip positive cache maintenance failed", exc_info=True)


def verify_cd_rip(
    path: str,
    cfg: CratediggerConfig,
    *,
    fetch: ProviderFetch = fetch_positive,
    radius: int = READ_OFFSET_RADIUS,
    deadline: float | None = None,
    provider_wall_seconds: float = PROVIDER_WALL_SECONDS,
) -> CdRipBitVerification | None:
    """Verify one exact CD-shaped FLAC/ALAC album, returning positives only."""
    try:
        if deadline is None:
            deadline = time.monotonic() + VERIFIER_WALL_SECONDS
        _check_deadline(deadline)
        tracks = _album_shape(path, deadline=deadline)
        _check_deadline(deadline)
        track_lengths = [track.samples for track in tracks]
        toc = build_cd_toc(track_lengths)
        ar_url = accuraterip_url(toc)
        ct_url = ctdb_url(toc)
        cache_dir = Path(cfg.var_dir) / "cd-rip-cache" / "v1"
        if provider_wall_seconds <= 0:
            raise ValueError("provider wall budget must be positive")
        requests = {
            "AccurateRip": (ar_url, cache_dir / "accuraterip"),
            "CTDB": (ct_url, cache_dir / "ctdb"),
        }
        if fetch is fetch_positive:
            payloads = _provider_payloads(
                fetch,
                requests,
                deadline=deadline,
                provider_wall_seconds=provider_wall_seconds,
            )
        else:
            payloads = _provider_payloads_direct(
                fetch,
                requests,
                deadline=deadline,
                provider_wall_seconds=provider_wall_seconds,
            )
        ar_blob = payloads["AccurateRip"]
        ct_xml = payloads["CTDB"]
        _check_deadline(deadline)

        ar_indexes: _ArIndexes | None = None
        ctdb_entries: list[_CtdbEntry] = []
        if ar_blob is not None:
            try:
                ar_indexes = parse_accuraterip(ar_blob, toc)
            except Exception:  # noqa: BLE001 - isolate provider parsers
                logger.info("AccurateRip withheld malformed/mismatched response")
                _best_effort_cache_remove(
                    _cache_path(cache_dir / "accuraterip", ar_url),
                    deadline=deadline,
                )
        if ct_xml is not None:
            try:
                ctdb_entries = parse_ctdb(ct_xml, toc)
            except Exception:  # noqa: BLE001 - isolate provider parsers
                logger.info("CTDB withheld malformed response")
                _best_effort_cache_remove(
                    _cache_path(cache_dir / "ctdb", ct_url),
                    deadline=deadline,
                )
        if ar_indexes is None and not ctdb_entries:
            return None

        spool_dir = Path(cfg.var_dir) / "cd-rip-spool" / "v1"
        _ensure_private_dir(spool_dir)
        with tempfile.TemporaryFile(
            prefix="cratedigger-cd-rip-",
            dir=spool_dir,
        ) as pcm:
            _decode_to_spool(tracks, pcm, deadline=deadline)
            ctdb_match: CtdbWholeDiscMatch | None = None
            if ctdb_entries:
                try:
                    ctdb_match = verify_ctdb_pcm(
                        pcm,
                        total_samples=sum(track_lengths),
                        entries=ctdb_entries,
                        url=ct_url,
                        deadline=deadline,
                    )
                except Exception:
                    logger.info(
                        "CTDB checksum verifier withheld response",
                        exc_info=True,
                    )
            ar_match: AccurateRipBitMatch | None = None
            ar_verification_attempted = False
            if ctdb_match is None and ar_indexes is not None:
                ar_verification_attempted = True
                try:
                    ar_match = verify_accuraterip_pcm(
                        pcm,
                        track_lengths=track_lengths,
                        indexes=ar_indexes,
                        url=ar_url,
                        radius=radius,
                        deadline=deadline,
                    )
                except Exception:
                    logger.info(
                        "AccurateRip checksum verifier withheld response",
                        exc_info=True,
                    )
        if ar_blob is not None and ar_verification_attempted and ar_match is None:
            _best_effort_cache_remove(
                _cache_path(cache_dir / "accuraterip", ar_url),
                deadline=deadline,
            )
        if ct_xml is not None and ctdb_match is None:
            _best_effort_cache_remove(
                _cache_path(cache_dir / "ctdb", ct_url),
                deadline=deadline,
            )
        if ar_match is None and ctdb_match is None:
            return None
        evidence = CdRipBitVerification(
            source_format=tracks[0].source_format,
            toc=toc,
            accuraterip=ar_match,
            ctdb=ctdb_match,
        )
        if evidence.validation_errors():
            return None
        if ar_match is not None and ar_blob is not None:
            _best_effort_cache_store(
                ar_url,
                cache_dir / "accuraterip",
                ar_blob,
                deadline=deadline,
            )
        if ctdb_match is not None and ct_xml is not None:
            _best_effort_cache_store(
                ct_url,
                cache_dir / "ctdb",
                ct_xml,
                deadline=deadline,
            )
        return evidence
    except Exception:
        logger.info("CD rip verifier withheld evidence", exc_info=True)
        return None


__all__ = [
    "READ_OFFSET_RADIUS",
    "accuraterip_checksums_at",
    "accuraterip_url",
    "build_cd_toc",
    "ctdb_url",
    "fetch_positive",
    "parse_accuraterip",
    "parse_ctdb",
    "verify_accuraterip_pcm",
    "verify_cd_rip",
    "verify_ctdb_pcm",
]
