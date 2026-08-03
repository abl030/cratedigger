import fcntl
import http.server
import io
import multiprocessing
import os
import socketserver
import struct
import subprocess
import sys
import tempfile
import threading
import time
import unittest
import urllib.error
import urllib.request
import zlib
from itertools import pairwise
from pathlib import Path

import numpy as np

from lib.cd_rip_verifier import (
    CTDB_STRIDE_SAMPLES,
    UINT32_MASK,
    _album_shape,
    _ArIndexes,
    _decode_to_spool,
    _NoRedirectHandler,
    _order_tracks,
    _probe_track,
    _provider_payloads,
    _read_bounded_response,
    _store_positive,
    _TrackShape,
    accuraterip_checksums_at,
    build_cd_toc,
    ctdb_url,
    fetch_positive,
    parse_accuraterip,
    parse_ctdb,
    verify_accuraterip_pcm,
    verify_cd_rip,
    verify_ctdb_pcm,
)
from lib.config import CratediggerConfig


def _pcm(values: np.ndarray) -> io.BytesIO:
    return io.BytesIO(values.astype("<u4").tobytes())


class _CountingPcm(io.BytesIO):
    def __init__(self, values: np.ndarray) -> None:
        super().__init__(values.astype("<u4").tobytes())
        self.bytes_read = 0

    def read(self, size: int | None = -1) -> bytes:
        payload = super().read(size)
        self.bytes_read += len(payload)
        return payload


def _write_flac(album: Path, values: np.ndarray, *, sample_bits: int = 16) -> Path:
    album.mkdir()
    raw = album.parent / f"{album.name}.raw"
    if sample_bits == 16:
        raw.write_bytes(values.astype("<u4").tobytes())
        raw_format = "s16le"
    elif sample_bits == 24:
        raw.write_bytes(b"\0\0\0\0\0\0" * len(values))
        raw_format = "s24le"
    else:
        raise ValueError("test helper supports only 16- or 24-bit PCM")
    target = album / "01.flac"
    subprocess.run(
        [
            "ffmpeg", "-v", "error", "-nostdin",
            "-f", raw_format, "-ar", "44100", "-ac", "2",
            "-i", str(raw), "-c:a", "flac", str(target),
        ],
        check=True,
        timeout=30,
    )
    return target


def _weighted(values: np.ndarray, start: int, lo: int, hi: int) -> int:
    selected = values[start + lo - 1:start + hi].astype(np.uint64)
    weights = np.arange(lo, hi + 1, dtype=np.uint64)
    return int(np.sum(selected * weights, dtype=np.uint64)) & UINT32_MASK


def _ar_blob(
    track_lengths: list[int],
    rows: list[tuple[int, int, int]],
) -> bytes:
    toc = build_cd_toc(track_lengths)
    id1_text, id2_text, freedb_text = toc.accuraterip_id.split("-")
    header = struct.pack(
        "<BIII",
        len(track_lengths),
        int(id1_text, 16),
        int(id2_text, 16),
        int(freedb_text, 16),
    )
    return header + b"".join(struct.pack("<BII", *row) for row in rows)


def _single_track_provider_payloads(
    values: np.ndarray,
) -> tuple[bytes, bytes]:
    length = len(values)
    v1, _v2 = accuraterip_checksums_at(
        _pcm(values),
        track_start=0,
        track_length=length,
        track_index=0,
        track_count=1,
        read_offset=0,
        total_samples=length,
    )
    ar_blob = _ar_blob([length], [(9, v1, 0)])
    raw = values.astype("<u4").tobytes()
    stop = length - CTDB_STRIDE_SAMPLES - (
        length % CTDB_STRIDE_SAMPLES
    )
    ctdb_crc = zlib.crc32(
        raw[CTDB_STRIDE_SAMPLES * 4:stop * 4]
    ) & UINT32_MASK
    sectors = length // 588
    ctdb_xml = (
        f'<ctdb><entry toc="0:{sectors}" id="exact" confidence="11" '
        f'crc32="{ctdb_crc:08x}" stride="5880" trackcrcs="1"/></ctdb>'
    ).encode()
    return ar_blob, ctdb_xml


def _provider_pid_fetch(_url: str, _cache: Path, _deadline: float) -> bytes:
    return str(os.getpid()).encode("ascii")


def _wedged_provider_fetch(_url: str, _cache: Path, _deadline: float) -> None:
    time.sleep(60)


class AccurateRipVerifierTest(unittest.TestCase):
    def setUp(self) -> None:
        # Long enough for the first/last five-sector exclusions and frame 450.
        self.lengths = [470 * 588, 480 * 588]
        total = sum(self.lengths)
        rng = np.random.default_rng(962)
        self.values = rng.integers(0, 2**32, size=total, dtype=np.uint32)

    def _checksums(self, offset: int) -> list[tuple[int, int]]:
        spool = _pcm(self.values)
        starts = [0, self.lengths[0]]
        return [
            accuraterip_checksums_at(
                spool,
                track_start=start,
                track_length=length,
                track_index=index,
                track_count=len(self.lengths),
                read_offset=offset,
                total_samples=len(self.values),
            )
            for index, (start, length) in enumerate(
                zip(starts, self.lengths, strict=True)
            )
        ]

    def _frame450(self, track: int, offset: int) -> int:
        start = sum(self.lengths[:track]) + offset
        lo = 450 * 588 + 1
        selected = self.values[start + lo - 1:start + lo - 1 + 588].astype(
            np.uint64
        )
        weights = np.arange(1, 589, dtype=np.uint64)
        return int(np.sum(selected * weights, dtype=np.uint64)) & UINT32_MASK

    def test_all_track_arv1_match_at_constant_nonzero_offset(self) -> None:
        offset = -78
        checksums = self._checksums(offset)
        blob = _ar_blob(
            self.lengths,
            [
                (9 + index, pair[0], self._frame450(index, offset))
                for index, pair in enumerate(checksums)
            ],
        )
        indexes = parse_accuraterip(blob, build_cd_toc(self.lengths))

        match = verify_accuraterip_pcm(
            _pcm(self.values),
            track_lengths=self.lengths,
            indexes=indexes,
            url="https://www.accuraterip.com/example.bin",
        )

        self.assertIsNotNone(match)
        assert match is not None
        self.assertEqual(match.checksum_version, "arv1")
        self.assertEqual(match.read_offset_samples, offset)
        self.assertEqual(match.track_confidences, [9, 10])

    def test_all_track_arv2_only_match_uses_frame450_offset_candidates(self) -> None:
        offset = 108
        checksums = self._checksums(offset)
        self.assertTrue(any(v1 != v2 for v1, v2 in checksums))
        blob = _ar_blob(
            self.lengths,
            [
                (21 + index, pair[1], self._frame450(index, offset))
                for index, pair in enumerate(checksums)
            ],
        )
        indexes = parse_accuraterip(blob, build_cd_toc(self.lengths))

        match = verify_accuraterip_pcm(
            _pcm(self.values),
            track_lengths=self.lengths,
            indexes=indexes,
            url="https://www.accuraterip.com/example.bin",
        )

        self.assertIsNotNone(match)
        assert match is not None
        self.assertEqual(match.checksum_version, "arv2")
        self.assertEqual(match.read_offset_samples, offset)

    def test_standards_vector_uses_local_frame450_weights(self) -> None:
        """The dBAR c450 field weights its selected sector from 1 through 588."""
        length = 470 * 588
        offset = 108
        values = np.arange(length, dtype=np.uint32)
        lo = 2940
        hi = length - 2940
        selected = np.zeros(hi - lo + 1, dtype=np.uint64)
        source_start = offset + lo - 1
        source_end = min(offset + hi, length)
        selected[:source_end - source_start] = values[
            source_start:source_end
        ].astype(np.uint64)
        weights = np.arange(lo, hi + 1, dtype=np.uint64)
        products = selected * weights
        folded = (products & np.uint64(UINT32_MASK)) + (products >> 32)
        arv2 = int(np.sum(folded, dtype=np.uint64)) & UINT32_MASK
        # Independent frozen vector for source sector 450 at offset +108,
        # computed with local 1..588 weights. Absolute 264601..265188
        # weights produce a different value and cannot discover the offset.
        frame450 = 0xB038E334
        blob = _ar_blob(lengths := [length], [(17, arv2, frame450)])

        match = verify_accuraterip_pcm(
            _pcm(values),
            track_lengths=lengths,
            indexes=parse_accuraterip(blob, build_cd_toc(lengths)),
            url="https://www.accuraterip.com/vector.bin",
        )

        self.assertIsNotNone(match)
        assert match is not None
        self.assertEqual(match.checksum_version, "arv2")
        self.assertEqual(match.read_offset_samples, 108)
        self.assertEqual(match.track_checksums, [arv2])

    def test_many_v1_offsets_do_not_trigger_one_disc_pass_per_candidate(self) -> None:
        length = 12 * 588
        values = np.zeros(length, dtype=np.uint32)
        indexes = parse_accuraterip(
            _ar_blob([length], [(9, 0, 0)]),
            build_cd_toc([length]),
        )
        pcm = _CountingPcm(values)
        match = verify_accuraterip_pcm(
            pcm,
            track_lengths=[length],
            indexes=indexes,
            url="https://www.accuraterip.com/pathological.bin",
            radius=31,
        )

        self.assertIsNotNone(match)
        self.assertLess(
            pcm.bytes_read,
            length * 4,
            "63 ARv1 hits must not cause 63 full confirmation passes",
        )

    def test_frame_candidate_explosion_is_withheld_before_confirmation(self) -> None:
        length = 470 * 588
        values = np.ones(length, dtype=np.uint32)
        frame450 = sum(range(1, 589)) & UINT32_MASK
        indexes = _ArIndexes(
            checksums=[{0xDEADBEEF: 7}],
            frame450=[{frame450: 7}],
            response_sha256="a" * 64,
        )
        pcm = _CountingPcm(values)
        self.assertIsNone(verify_accuraterip_pcm(
            pcm,
            track_lengths=[length],
            indexes=indexes,
            url="https://www.accuraterip.com/pathological-v2.bin",
            radius=8,
        ))
        self.assertLess(
            pcm.bytes_read,
            length * 8 + 100_000,
            "the frame-450 cap must leave only the independent zero-offset pass",
        )

    def test_v1_candidate_explosion_does_not_suppress_arv2_zero(self) -> None:
        length = 12 * 588
        values = np.zeros(length, dtype=np.uint32)
        indexes = parse_accuraterip(
            _ar_blob([length], [(9, 0, 0)]),
            build_cd_toc([length]),
        )

        match = verify_accuraterip_pcm(
            _pcm(values),
            track_lengths=[length],
            indexes=indexes,
            url="https://www.accuraterip.com/v2-zero.bin",
            radius=32,
        )

        self.assertIsNotNone(match)
        assert match is not None
        self.assertEqual(match.checksum_version, "arv2")
        self.assertEqual(match.read_offset_samples, 0)

    def test_nine_frame_candidates_do_not_suppress_arv2_zero(self) -> None:
        length = 470 * 588
        values = np.ones(length, dtype=np.uint32)
        _v1, v2 = accuraterip_checksums_at(
            _pcm(values),
            track_start=0,
            track_length=length,
            track_index=0,
            track_count=1,
            read_offset=0,
            total_samples=length,
        )
        frame450 = sum(range(1, 589)) & UINT32_MASK
        indexes = _ArIndexes(
            checksums=[{v2: 7}],
            frame450=[{frame450: 7}],
            response_sha256="a" * 64,
        )

        match = verify_accuraterip_pcm(
            _pcm(values),
            track_lengths=[length],
            indexes=indexes,
            url="https://www.accuraterip.com/frame-v2-zero.bin",
            radius=4,
        )

        self.assertIsNotNone(match)
        assert match is not None
        self.assertEqual(match.checksum_version, "arv2")
        self.assertEqual(match.read_offset_samples, 0)

    def test_expired_verifier_deadline_interrupts_checksum_work(self) -> None:
        length = 10 * 588
        indexes = _ArIndexes(
            checksums=[{0: 1}],
            frame450=[{}],
            response_sha256="a" * 64,
        )

        with self.assertRaisesRegex(TimeoutError, "exceeded its wall deadline"):
            verify_accuraterip_pcm(
                _pcm(np.zeros(length, dtype=np.uint32)),
                track_lengths=[length],
                indexes=indexes,
                url="https://www.accuraterip.com/deadline.bin",
                deadline=time.monotonic() - 1,
            )

    def test_mixed_track_offsets_have_no_policy_fact(self) -> None:
        first = self._checksums(-12)[0]
        second = self._checksums(44)[1]
        blob = _ar_blob(
            self.lengths,
            [
                (5, first[0], self._frame450(0, -12)),
                (7, second[0], self._frame450(1, 44)),
            ],
        )
        indexes = parse_accuraterip(blob, build_cd_toc(self.lengths))

        self.assertIsNone(
            verify_accuraterip_pcm(
                _pcm(self.values),
                track_lengths=self.lengths,
                indexes=indexes,
                url="https://www.accuraterip.com/example.bin",
            )
        )

    def test_parser_rejects_truncation_and_toc_mismatch(self) -> None:
        valid = _ar_blob(self.lengths, [(1, 2, 3), (1, 4, 5)])
        toc = build_cd_toc(self.lengths)
        with self.assertRaisesRegex(ValueError, "truncated"):
            parse_accuraterip(valid[:-1], toc)
        changed = bytearray(valid)
        changed[1] ^= 1
        with self.assertRaisesRegex(ValueError, "identity mismatch"):
            parse_accuraterip(bytes(changed), toc)


class CtdbVerifierTest(unittest.TestCase):
    def test_only_whole_disc_crc_mints_match(self) -> None:
        values = np.arange(40_000, dtype=np.uint32)
        raw = values.astype("<u4").tobytes()
        stop = len(values) - CTDB_STRIDE_SAMPLES - (
            len(values) % CTDB_STRIDE_SAMPLES
        )
        checksum = zlib.crc32(
            raw[CTDB_STRIDE_SAMPLES * 4:stop * 4]
        ) & UINT32_MASK
        xml = (
            '<ctdb><entry toc="0:20:40" id="good" confidence="8026" '
            f'crc32="{checksum:08x}" stride="5880" trackcrcs="1 2"/>'
            '<entry toc="0:20:40" id="partial" confidence="9999" crc32="00000000" '
            'stride="5880" trackcrcs="1 2"/></ctdb>'
        ).encode()
        toc = build_cd_toc([20 * 588, 20 * 588])
        entries = parse_ctdb(xml, toc)

        match = verify_ctdb_pcm(
            io.BytesIO(raw),
            total_samples=len(values),
            entries=entries,
            url="https://db.cue.tools/example",
        )

        self.assertIsNotNone(match)
        assert match is not None
        self.assertEqual(match.entry_id, "good")
        self.assertEqual(match.confidence, 8026)
        self.assertEqual(match.response_toc_sectors, [0, 20, 40])
        self.assertEqual(match.response_toc_shift_sectors, 0)

    def test_fuzzy_entry_with_different_response_toc_is_not_admitted(self) -> None:
        toc = build_cd_toc([20 * 588, 20 * 588])
        exact = parse_ctdb(
            b'<ctdb><entry toc="0:20:40" id="exact" confidence="4" '
            b'crc32="1234" stride="5880" trackcrcs="1 2"/></ctdb>',
            toc,
        )
        fuzzy_sibling = parse_ctdb(
            b'<ctdb><entry toc="0:21:40" id="sibling" confidence="999" '
            b'crc32="1234" stride="5880" trackcrcs="1 2"/></ctdb>',
            toc,
        )

        self.assertEqual(len(exact), 1)
        self.assertEqual(exact[0].response_toc_sectors, [0, 20, 40])
        self.assertEqual(exact[0].response_toc_shift_sectors, 0)
        self.assertEqual(fuzzy_sibling, [])

    def test_frozen_entry_3137070_constant_shift_normalizes_exactly(self) -> None:
        submitted = [0, 13623, 27913, 39503, 53533, 63130, 86808, 102298]
        lengths = [
            (right - left) * 588
            for left, right in pairwise(submitted)
        ]
        raw = [sector + 32 for sector in submitted]
        raw_text = ":".join(str(sector) for sector in raw)
        track_crcs = " ".join(str(index) for index in range(1, 8))
        entries = parse_ctdb(
            (
                f'<ctdb><entry toc="{raw_text}" id="3137070" '
                f'confidence="42" crc32="c5f69e2f" stride="5880" '
                f'trackcrcs="{track_crcs}"/></ctdb>'
            ).encode(),
            build_cd_toc(lengths),
        )

        self.assertEqual(len(entries), 1)
        self.assertEqual(entries[0].entry_id, "3137070")
        self.assertEqual(entries[0].response_toc_sectors, raw)
        self.assertEqual(entries[0].response_toc_shift_sectors, 32)

        nonconstant = raw.copy()
        nonconstant[3] += 1
        nonconstant_text = ":".join(str(sector) for sector in nonconstant)
        self.assertEqual(
            parse_ctdb(
                (
                    f'<ctdb><entry toc="{nonconstant_text}" id="sibling" '
                    f'confidence="999" crc32="c5f69e2f" stride="5880" '
                    f'trackcrcs="{track_crcs}"/></ctdb>'
                ).encode(),
                build_cd_toc(lengths),
            ),
            [],
        )

    def test_malformed_and_partial_ctdb_are_no_match(self) -> None:
        with self.assertRaisesRegex(ValueError, "declarations"):
            parse_ctdb(b"<!DOCTYPE x><x/>", build_cd_toc([20 * 588]))
        entries = parse_ctdb(
            b'<ctdb><entry toc="0:20:40" id="x" confidence="4" crc32="1234" '
            b'stride="5880" trackcrcs="1"/></ctdb>',
            build_cd_toc([20 * 588, 20 * 588]),
        )
        self.assertEqual(entries, [])


class CdShapeAndProviderBoundaryTest(unittest.TestCase):
    def test_natural_filename_order_handles_tracks_one_two_and_ten(self) -> None:
        shapes = [
            _TrackShape(Path(f"{number}.flac"), 588, "flac")
            for number in (1, 10, 2, 9, 3, 8, 4, 7, 5, 6)
        ]

        ordered = _order_tracks(shapes)

        self.assertEqual(
            [shape.path.name for shape in ordered],
            [f"{number}.flac" for number in range(1, 11)],
        )

    def test_ambiguous_duplicate_filename_numbers_are_rejected(self) -> None:
        with self.assertRaisesRegex(ValueError, "one aligned"):
            _order_tracks([
                _TrackShape(Path("01 - One.flac"), 588, "flac"),
                _TrackShape(Path("1 - Also One.flac"), 588, "flac"),
            ])

    def test_artist_number_is_not_mistaken_for_track_number(self) -> None:
        for stems in (
            ["Blink-182 - 02", "Blink-182 - 01"],
            ["2001 - Album - 02", "2001 - Album - 01"],
        ):
            with self.subTest(stems=stems):
                ordered = _order_tracks([
                    _TrackShape(Path(f"{stem}.flac"), 588, "flac")
                    for stem in stems
                ])
                self.assertTrue(ordered[0].path.stem.endswith("01"))
                self.assertTrue(ordered[1].path.stem.endswith("02"))

    def test_duplicate_aligned_filename_order_is_one_candidate(self) -> None:
        ordered = _order_tracks([
            _TrackShape(Path("01 - Track 01.flac"), 588, "flac"),
            _TrackShape(Path("02 - Track 02.flac"), 588, "flac"),
        ])

        self.assertEqual(
            [shape.path.name for shape in ordered],
            ["01 - Track 01.flac", "02 - Track 02.flac"],
        )

    def test_conflicting_aligned_filename_orders_are_ambiguous(self) -> None:
        with self.assertRaisesRegex(ValueError, "one aligned"):
            _order_tracks([
                _TrackShape(Path("1 - Movement 2.flac"), 588, "flac"),
                _TrackShape(Path("2 - Movement 1.flac"), 588, "flac"),
            ])

    def test_complete_embedded_track_numbers_override_filenames(self) -> None:
        ordered = _order_tracks([
            _TrackShape(Path("z.flac"), 588, "flac", 2, 1),
            _TrackShape(Path("a.flac"), 588, "flac", 1, 1),
        ])

        self.assertEqual([shape.path.name for shape in ordered], ["a.flac", "z.flac"])

    def test_real_flac_and_alac_with_exact_cd_shape_are_admitted(self) -> None:
        samples = 470 * 588
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            raw = root / "track.pcm"
            raw.write_bytes(bytes(samples * 4))
            shapes = []
            for folder_name, extension, codec in (
                ("flac-album", ".flac", "flac"),
                ("alac-album", ".m4a", "alac"),
            ):
                album = root / folder_name
                album.mkdir()
                target = album / f"01{extension}"
                subprocess.run(
                    [
                        "ffmpeg", "-v", "error", "-nostdin",
                        "-f", "s16le", "-ar", "44100", "-ac", "2",
                        "-i", str(raw), "-c:a", codec, str(target),
                    ],
                    check=True,
                    timeout=30,
                )
                shapes.append(_album_shape(str(album))[0])

        self.assertEqual([shape.samples for shape in shapes], [samples, samples])
        self.assertEqual(
            [shape.source_format for shape in shapes], ["flac", "alac"]
        )

    def test_probe_rejects_non_cd_bit_depth(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            target = _write_flac(
                Path(tmpdir) / "album",
                np.zeros(12 * 588, dtype=np.uint32),
                sample_bits=24,
            )
            with self.assertRaisesRegex(ValueError, "16-bit"):
                _probe_track(target)

    def test_decode_does_not_expand_subsecond_remaining_deadline(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            fifo = Path(tmpdir) / "blocked.flac"
            os.mkfifo(fifo)
            with tempfile.TemporaryFile() as pcm:
                started = time.monotonic()
                with self.assertRaises(subprocess.TimeoutExpired):
                    _decode_to_spool(
                        [_TrackShape(fifo, 12 * 588, "flac")],
                        pcm,
                        deadline=started + 0.1,
                    )
                elapsed = time.monotonic() - started

        self.assertLess(elapsed, 0.5)

    def test_non_https_is_no_opinion_and_never_cached(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            cache = Path(tmpdir)
            self.assertIsNone(fetch_positive("http://example.test/ar", cache))
            self.assertEqual(list(cache.glob("*.positive")), [])

    def test_positive_payload_is_read_only_after_explicit_commit(self) -> None:
        url = "https://example.test/ar"
        with tempfile.TemporaryDirectory() as tmpdir:
            cache = Path(tmpdir)
            payload = b"provider-payload"
            _store_positive(url, cache, payload)
            self.assertEqual(fetch_positive(url, cache), payload)

    def test_contended_cache_lock_obeys_the_verifier_deadline(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            cache = Path(tmpdir)
            lock_path = cache / ".fetch.lock"
            with lock_path.open("a+b") as held:
                fcntl.flock(held.fileno(), fcntl.LOCK_EX)
                started = time.monotonic()
                with self.assertRaises(TimeoutError):
                    fetch_positive(
                        "https://www.accuraterip.com/locked.bin",
                        cache,
                        deadline=started + 0.12,
                    )
                elapsed = time.monotonic() - started
                fcntl.flock(held.fileno(), fcntl.LOCK_UN)

        self.assertLess(elapsed, 0.3)

    def test_future_politeness_stamp_is_clamped_to_one_interval(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            cache = Path(tmpdir)
            (cache / ".last-fetch").write_text(
                str(time.time() + 3600),
                encoding="ascii",
            )
            started = time.monotonic()
            self.assertIsNone(fetch_positive(
                "https://127.0.0.1:4443/unreachable",
                cache,
                deadline=started + 1.5,
            ))
            elapsed = time.monotonic() - started

        self.assertGreaterEqual(elapsed, 0.5)
        self.assertLess(elapsed, 1.3)

    def test_provider_requests_use_two_spawned_processes(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            payloads = _provider_payloads(
                _provider_pid_fetch,
                {
                    "AccurateRip": ("https://example.test/ar", root / "ar"),
                    "CTDB": ("https://example.test/ctdb", root / "ctdb"),
                },
                deadline=time.monotonic() + 5,
                provider_wall_seconds=3,
            )

        worker_pids = {
            int(payload.decode("ascii"))
            for payload in payloads.values()
            if payload is not None
        }
        self.assertEqual(len(worker_pids), 2)
        self.assertNotIn(os.getpid(), worker_pids)

    def test_timed_out_provider_children_are_reaped(self) -> None:
        before = {
            process.pid for process in multiprocessing.active_children()
        }
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            started = time.monotonic()
            payloads = _provider_payloads(
                _wedged_provider_fetch,
                {
                    "AccurateRip": ("https://example.test/ar", root / "ar"),
                    "CTDB": ("https://example.test/ctdb", root / "ctdb"),
                },
                deadline=started + 2,
                provider_wall_seconds=0.15,
            )
            elapsed = time.monotonic() - started
        after = {
            process.pid for process in multiprocessing.active_children()
        }

        self.assertEqual(payloads, {"AccurateRip": None, "CTDB": None})
        self.assertEqual(after, before)
        self.assertLess(elapsed, 1.0)

    def test_timed_out_provider_does_not_delay_process_exit(self) -> None:
        script = """
import tempfile
import time
from pathlib import Path
from lib.cd_rip_verifier import _provider_payloads
from tests.test_cd_rip_verifier import _wedged_provider_fetch

with tempfile.TemporaryDirectory() as tmpdir:
    root = Path(tmpdir)
    _provider_payloads(
        _wedged_provider_fetch,
        {
            "AccurateRip": ("https://example.test/ar", root / "ar"),
            "CTDB": ("https://example.test/ctdb", root / "ctdb"),
        },
        deadline=time.monotonic() + 2,
        provider_wall_seconds=0.15,
    )
"""
        started = time.monotonic()
        completed = subprocess.run(
            [sys.executable, "-c", script],
            capture_output=True,
            text=True,
            timeout=3,
            check=False,
        )
        elapsed = time.monotonic() - started

        self.assertEqual(completed.returncode, 0, completed.stderr)
        self.assertLess(elapsed, 2.0)

    def _verify_with_one_provider_failure(self, failing: str):
        values = np.arange(30 * 588, dtype=np.uint32)
        ar_blob, ctdb_xml = _single_track_provider_payloads(values)

        def fetch(
            url: str,
            _cache: Path,
            _deadline: float,
        ) -> bytes | None:
            provider = "accuraterip" if "accuraterip" in url else "ctdb"
            if provider == failing:
                raise OSError(f"{provider} unavailable")
            return ar_blob if provider == "accuraterip" else ctdb_xml

        with tempfile.TemporaryDirectory() as tmpdir:
            album = Path(tmpdir) / "album"
            _write_flac(album, values)
            return verify_cd_rip(
                str(album),
                CratediggerConfig(var_dir=tmpdir),
                fetch=fetch,
            )

    def test_accuraterip_fetch_exception_does_not_discard_exact_ctdb(self) -> None:
        result = self._verify_with_one_provider_failure("accuraterip")

        self.assertIsNotNone(result)
        assert result is not None
        self.assertIsNone(result.accuraterip)
        self.assertIsNotNone(result.ctdb)

    def test_ctdb_fetch_exception_does_not_discard_exact_accuraterip(self) -> None:
        result = self._verify_with_one_provider_failure("ctdb")

        self.assertIsNotNone(result)
        assert result is not None
        self.assertIsNotNone(result.accuraterip)
        self.assertIsNone(result.ctdb)

    def _verify_with_provider_payloads(
        self,
        ar_payload: bytes,
        ctdb_payload: bytes,
        *,
        cache_collision: str | None = None,
    ):
        values = np.arange(30 * 588, dtype=np.uint32)

        def fetch(
            url: str,
            _cache: Path,
            _deadline: float,
        ) -> bytes:
            return ar_payload if "accuraterip" in url else ctdb_payload

        with tempfile.TemporaryDirectory() as tmpdir:
            album = Path(tmpdir) / "album"
            _write_flac(album, values)
            cache_root = Path(tmpdir) / "cd-rip-cache" / "v1"
            if cache_collision is not None:
                cache_root.mkdir(parents=True)
                (cache_root / cache_collision).write_text(
                    "provider cache path collision",
                    encoding="ascii",
                )
            result = verify_cd_rip(
                str(album),
                CratediggerConfig(var_dir=tmpdir),
                fetch=fetch,
            )
            return result

    def test_accuraterip_parser_failure_does_not_discard_exact_ctdb(self) -> None:
        values = np.arange(30 * 588, dtype=np.uint32)
        _ar_blob_exact, ctdb_xml = _single_track_provider_payloads(values)

        result = self._verify_with_provider_payloads(b"truncated", ctdb_xml)

        self.assertIsNotNone(result)
        assert result is not None
        self.assertIsNone(result.accuraterip)
        self.assertIsNotNone(result.ctdb)

    def test_ctdb_parser_failure_does_not_discard_exact_accuraterip(self) -> None:
        values = np.arange(30 * 588, dtype=np.uint32)
        ar_blob, _ctdb_xml_exact = _single_track_provider_payloads(values)

        result = self._verify_with_provider_payloads(ar_blob, b"<ctdb")

        self.assertIsNotNone(result)
        assert result is not None
        self.assertIsNotNone(result.accuraterip)
        self.assertIsNone(result.ctdb)

    def test_accuraterip_cache_failure_does_not_discard_positive_proof(self) -> None:
        values = np.arange(30 * 588, dtype=np.uint32)
        ar_blob, _ctdb_xml = _single_track_provider_payloads(values)

        result = self._verify_with_provider_payloads(
            ar_blob,
            b"<ctdb",
            cache_collision="accuraterip",
        )

        self.assertIsNotNone(result)
        assert result is not None
        self.assertIsNotNone(result.accuraterip)
        self.assertIsNone(result.ctdb)

    def test_ctdb_cache_failure_does_not_discard_positive_proof(self) -> None:
        values = np.arange(30 * 588, dtype=np.uint32)
        ar_blob, ctdb_xml = _single_track_provider_payloads(values)

        result = self._verify_with_provider_payloads(
            ar_blob,
            ctdb_xml,
            cache_collision="ctdb",
        )

        self.assertIsNotNone(result)
        assert result is not None
        self.assertIsNone(result.accuraterip)
        self.assertIsNotNone(result.ctdb)

    def test_wedged_ar_lane_expires_without_discarding_cached_exact_ctdb(
        self,
    ) -> None:
        values = np.arange(30 * 588, dtype=np.uint32)
        _ar_blob_exact, ctdb_xml = _single_track_provider_payloads(values)
        with tempfile.TemporaryDirectory() as tmpdir:
            album = Path(tmpdir) / "album"
            _write_flac(album, values)
            toc = build_cd_toc([len(values)])
            cache_root = Path(tmpdir) / "cd-rip-cache" / "v1"
            ctdb_cache = cache_root / "ctdb"
            _store_positive(ctdb_url(toc), ctdb_cache, ctdb_xml)
            ar_cache = cache_root / "accuraterip"
            ar_cache.mkdir(parents=True)
            lock_path = ar_cache / ".fetch.lock"
            started = time.monotonic()
            with lock_path.open("a+b") as held:
                fcntl.flock(held.fileno(), fcntl.LOCK_EX)
                result = verify_cd_rip(
                    str(album),
                    CratediggerConfig(var_dir=tmpdir),
                    deadline=started + 6,
                    provider_wall_seconds=1.5,
                )
                fcntl.flock(held.fileno(), fcntl.LOCK_UN)
            elapsed = time.monotonic() - started

        self.assertIsNotNone(result)
        assert result is not None
        self.assertIsNone(result.accuraterip)
        self.assertIsNotNone(result.ctdb)
        self.assertLess(elapsed, 4.0)

    def test_exact_ctdb_short_circuits_failing_accuraterip_verifier(self) -> None:
        values = np.arange(30 * 588, dtype=np.uint32)
        ar_blob, ctdb_xml = _single_track_provider_payloads(values)

        def fetch(
            url: str,
            _cache: Path,
            _deadline: float,
        ) -> bytes:
            return ar_blob if "accuraterip" in url else ctdb_xml

        with tempfile.TemporaryDirectory() as tmpdir:
            album = Path(tmpdir) / "album"
            _write_flac(album, values)
            result = verify_cd_rip(
                str(album),
                CratediggerConfig(var_dir=tmpdir),
                fetch=fetch,
                radius=-1,
            )

        self.assertIsNotNone(result)
        assert result is not None
        self.assertIsNone(result.accuraterip)
        self.assertIsNotNone(result.ctdb)

    def test_pcm_spool_is_private_and_beneath_runtime_var_dir(self) -> None:
        length = 12 * 588
        values = np.arange(length, dtype=np.uint32)
        checksum, _v2 = accuraterip_checksums_at(
            _pcm(values),
            track_start=0,
            track_length=length,
            track_index=0,
            track_count=1,
            read_offset=0,
            total_samples=length,
        )
        blob = _ar_blob([length], [(9, checksum, 0)])

        def fetch(
            url: str,
            _cache: Path,
            _deadline: float,
        ) -> bytes | None:
            return blob if "accuraterip" in url else None

        with tempfile.TemporaryDirectory() as tmpdir:
            album = Path(tmpdir) / "album"
            _write_flac(album, values)
            result = verify_cd_rip(
                str(album),
                CratediggerConfig(var_dir=tmpdir),
                fetch=fetch,
            )
            expected = Path(tmpdir) / "cd-rip-spool" / "v1"
            self.assertEqual(expected.stat().st_mode & 0o777, 0o700)
            self.assertEqual(list(expected.iterdir()), [])

        self.assertIsNotNone(result)
        assert result is not None and result.accuraterip is not None
        self.assertEqual(result.accuraterip.track_checksums, [checksum])

    def test_redirect_handler_never_contacts_forbidden_target(self) -> None:
        target_hits = 0

        class Target(http.server.BaseHTTPRequestHandler):
            def do_GET(self) -> None:
                nonlocal target_hits
                target_hits += 1
                self.send_response(200)
                self.end_headers()

            def log_message(self, format: str, *args: object) -> None:
                del format, args

        with socketserver.TCPServer(("127.0.0.1", 0), Target) as target:
            target_port = int(target.server_address[1])

            class Redirect(http.server.BaseHTTPRequestHandler):
                def do_GET(self) -> None:
                    self.send_response(302)
                    self.send_header(
                        "Location", f"http://127.0.0.1:{target_port}/forbidden"
                    )
                    self.end_headers()

                def log_message(self, format: str, *args: object) -> None:
                    del format, args

            with socketserver.TCPServer(("127.0.0.1", 0), Redirect) as source:
                source_port = int(source.server_address[1])
                target_thread = threading.Thread(
                    target=target.serve_forever, daemon=True
                )
                source_thread = threading.Thread(
                    target=source.serve_forever, daemon=True
                )
                target_thread.start()
                source_thread.start()
                opener = urllib.request.build_opener(_NoRedirectHandler())
                with self.assertRaises(urllib.error.HTTPError) as raised:
                    opener.open(f"http://127.0.0.1:{source_port}/start", timeout=1)
                raised.exception.close()
                source.shutdown()
                target.shutdown()

        self.assertEqual(target_hits, 0)

    def test_slow_drip_cannot_extend_whole_response_deadline(self) -> None:
        class SlowDrip(http.server.BaseHTTPRequestHandler):
            def do_GET(self) -> None:
                self.send_response(200)
                self.end_headers()
                try:
                    for _index in range(100):
                        self.wfile.write(b"x")
                        self.wfile.flush()
                        time.sleep(0.02)
                except (BrokenPipeError, ConnectionResetError):
                    pass

            def log_message(self, format: str, *args: object) -> None:
                del format, args

        with socketserver.TCPServer(("127.0.0.1", 0), SlowDrip) as server:
            port = int(server.server_address[1])
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            with urllib.request.urlopen(
                f"http://127.0.0.1:{port}/slow", timeout=1
            ) as response:
                started = time.monotonic()
                with self.assertRaises(TimeoutError):
                    _read_bounded_response(
                        response,
                        deadline=started + 0.12,
                    )
                elapsed = time.monotonic() - started
            server.shutdown()

        self.assertLess(elapsed, 0.35)

    def test_fixed_content_length_stops_without_reading_closed_fp(self) -> None:
        payload = b"fixed-length-ctdb-body"

        class FixedLength(http.server.BaseHTTPRequestHandler):
            def do_GET(self) -> None:
                self.send_response(200)
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)

            def log_message(self, format: str, *args: object) -> None:
                del format, args

        with socketserver.TCPServer(("127.0.0.1", 0), FixedLength) as server:
            port = int(server.server_address[1])
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            with urllib.request.urlopen(
                f"http://127.0.0.1:{port}/fixed", timeout=1
            ) as response:
                self.assertEqual(
                    _read_bounded_response(
                        response,
                        deadline=time.monotonic() + 1,
                    ),
                    payload,
                )
            server.shutdown()

    def test_short_content_length_body_is_rejected(self) -> None:
        class ShortBody(http.server.BaseHTTPRequestHandler):
            def do_GET(self) -> None:
                self.send_response(200)
                self.send_header("Content-Length", "10")
                self.send_header("Connection", "close")
                self.end_headers()
                self.wfile.write(b"short")
                self.close_connection = True

            def log_message(self, format: str, *args: object) -> None:
                del format, args

        with socketserver.TCPServer(("127.0.0.1", 0), ShortBody) as server:
            port = int(server.server_address[1])
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            with urllib.request.urlopen(
                f"http://127.0.0.1:{port}/short", timeout=1
            ) as response, self.assertRaisesRegex(ValueError, "Content-Length"):
                _read_bounded_response(
                    response,
                    deadline=time.monotonic() + 1,
                )
            server.shutdown()


if __name__ == "__main__":
    unittest.main()
