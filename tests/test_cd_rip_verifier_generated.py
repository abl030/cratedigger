import io
import struct
import unittest

import numpy as np
from hypothesis import given, settings
from hypothesis import strategies as st

import tests._hypothesis_profiles  # noqa: F401 - shared registered profiles
from lib.cd_rip_verifier import (
    UINT32_MASK,
    _scan_weighted_v1,
    accuraterip_checksums_at,
    build_cd_toc,
    parse_accuraterip,
    verify_accuraterip_pcm,
)


def _spool(values: np.ndarray) -> io.BytesIO:
    return io.BytesIO(values.astype("<u4").tobytes())


def _blob(length: int, confidence: int, checksum: int) -> bytes:
    toc = build_cd_toc([length])
    id1, id2, freedb = (
        int(part, 16) for part in toc.accuraterip_id.split("-")
    )
    return struct.pack("<BIII", 1, id1, id2, freedb) + struct.pack(
        "<BII", confidence, checksum, 0
    )


class GeneratedAccurateRipVerifierTest(unittest.TestCase):
    @settings(max_examples=60, deadline=None)
    @given(
        seed=st.integers(min_value=0, max_value=2**32 - 1),
        offset=st.integers(min_value=-20, max_value=20),
        confidence=st.integers(min_value=1, max_value=255),
    )
    def test_exact_integer_scan_recovers_every_generated_constant_offset(
        self,
        seed: int,
        offset: int,
        confidence: int,
    ) -> None:
        length = 12 * 588
        rng = np.random.default_rng(seed)
        # Surrounding samples make both signed offset directions observable;
        # the verifier's album spool has no prefix/suffix, so emulate the
        # required zero padding by shifting the generated disc inside a larger
        # array and passing that shifted start to the checksum oracle.
        values = rng.integers(0, 2**32, size=length, dtype=np.uint32)
        checksum, _v2 = accuraterip_checksums_at(
            _spool(values),
            track_start=0,
            track_length=length,
            track_index=0,
            track_count=1,
            read_offset=offset,
            total_samples=length,
        )
        toc = build_cd_toc([length])
        indexes = parse_accuraterip(
            _blob(length, confidence, checksum),
            toc,
        )

        match = verify_accuraterip_pcm(
            _spool(values),
            track_lengths=[length],
            indexes=indexes,
            url="https://www.accuraterip.com/generated.bin",
            radius=20,
        )

        self.assertIsNotNone(match)
        assert match is not None
        self.assertEqual(match.read_offset_samples, offset)
        self.assertEqual(match.track_confidences, [confidence])

    @settings(max_examples=80, deadline=None)
    @given(
        values=st.lists(
            st.integers(min_value=0, max_value=2**32 - 1),
            min_size=40,
            max_size=120,
        ),
        lo=st.integers(min_value=1, max_value=10),
        radius=st.integers(min_value=0, max_value=8),
    )
    def test_recurrence_equals_direct_mod32_weighting(
        self,
        values: list[int],
        lo: int,
        radius: int,
    ) -> None:
        array = np.asarray(values, dtype=np.uint32)
        hi = len(array) - 10
        if hi < lo:
            return
        wanted: set[int] = set()
        expected: set[int] = set()
        for offset in range(-radius, radius + 1):
            selected = np.zeros(hi - lo + 1, dtype=np.uint64)
            source_start = offset + lo - 1
            left = max(source_start, 0)
            right = min(source_start + len(selected), len(array))
            if right > left:
                selected[left - source_start:right - source_start] = (
                    array[left:right]
                )
            weights = np.arange(lo, hi + 1, dtype=np.uint64)
            checksum = int(
                np.sum(selected * weights, dtype=np.uint64)
            ) & UINT32_MASK
            if offset % 2 == 0:
                wanted.add(checksum)
                expected.add(offset)

        actual = _scan_weighted_v1(
            _spool(array),
            track_start=0,
            source_lo=lo,
            source_hi=hi,
            weight_start=lo,
            total_samples=len(array),
            wanted=wanted,
            radius=radius,
        )

        # Collisions can add odd offsets, but every selected direct checksum
        # must be found. This property kills sign, leaving/entering, and
        # local-weight off-by-one mutants without assuming collision absence.
        self.assertTrue(expected <= set(actual))

    def test_track_one_2940_off_by_one_mutant_is_rejected(self) -> None:
        length = 12 * 588
        values = np.arange(length, dtype=np.uint32) * np.uint32(0x10203)
        # Mutant: starts track one at local weight/sample 2941, not 2940.
        lo = 2941
        hi = length - 2940
        mutant = int(np.sum(
            values[lo - 1:hi].astype(np.uint64)
            * np.arange(lo, hi + 1, dtype=np.uint64),
            dtype=np.uint64,
        )) & UINT32_MASK
        indexes = parse_accuraterip(
            _blob(length, 10, mutant),
            build_cd_toc([length]),
        )

        self.assertIsNone(verify_accuraterip_pcm(
            _spool(values),
            track_lengths=[length],
            indexes=indexes,
            url="https://www.accuraterip.com/mutant.bin",
            radius=0,
        ))

    def test_frozen_float_prefix_mutant_loses_uint64_precision(self) -> None:
        """Known-bad checker for fullrun2's concatenate promotion bug."""
        exact = np.asarray([0, 2**53 + 1], dtype=np.uint64)
        promoted = np.concatenate(([0], exact[1:]))

        self.assertEqual(promoted.dtype, np.dtype("float64"))
        self.assertNotEqual(int(promoted[1]), int(exact[1]))


if __name__ == "__main__":
    unittest.main()
