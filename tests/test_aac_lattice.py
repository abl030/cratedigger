"""Detector fidelity for lib/aac_lattice.py — issue #829 invariant A-I2.

``docs/research/calibration-data/derrien/aacdet.py.frozen`` is the validated
numpy port this module productizes, and ``validate.py.frozen`` beside it is the
mathematical self-validation that port was held to. This file re-runs that
self-validation against the SHIPPED code, so the port cannot silently drift
away from the reference implementation it inherited its numbers from:

    PASS  MDCT == direct DCT-IV definition (up to constant)
    PASS  TDAC perfect reconstruction (sine window)
    PASS  tau(s) formula is conservative vs the true null
    PASS  synthetic AAC codec round trip -> detector peaks at the true offset
    PASS  white-noise control stays below the paper's lambda=0.031

Two differences from the frozen validation, both deliberate:

* the offset-recovery and noise-control checks run through the PRODUCTION
  entry point ``analyze_track`` — a real WAV, a real ffmpeg decode, real frame
  selection — rather than calling ``detect_aac`` on hand-sliced segments. The
  production path is a superset of the frozen one, and Rule C wants the
  trigger produced rather than staged;
* the frozen port's scipy ``erfinv`` is replaced by bisection over
  ``math.erf``, so the round trip is pinned here directly.

The per-track error taxonomy is pinned with REAL failing inputs (a 96 kHz WAV,
undecodable bytes, a two-block file) rather than staged exceptions — those are
the three failures the live corpus actually produced.
"""

from __future__ import annotations

import contextlib
import os
import shutil
import struct
import subprocess
import sys
import tempfile
import unittest
import wave
from collections.abc import Callable
from unittest.mock import patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import numpy as np
import numpy.typing as npt

from lib.aac_lattice import (
    DEFAULT_NB_WIN,
    IBLEN_LONG,
    MAX_SCORED_TRACKS,
    AacLatticeAnalysis,
    AacLatticeDecodeError,
    AacLatticeError,
    AacLatticeTooShortError,
    AacLatticeUnsupportedRateError,
    BandPlan,
    album_audio_files,
    analyze_track,
    detect_aac,
    erfinv,
    init_aac,
    mdct,
    measure_album_aac_lattice,
    probe_sample_rate_khz,
    select_frames,
    tau_tables,
)
from lib.quality import AacLatticeCapture, AacLatticeTrackScore

F64 = npt.NDArray[np.float64]

# validate.py.frozen's seed and lattice offset, kept so the numbers below are
# comparable with the recorded reference run.
_SEED = 7
_TRUE_OFFSET = 313
# The paper's null-rate ceiling; the frozen run measured 0.0194 against it.
_PAPER_LAMBDA = 0.031


def _mdct_direct(x: F64) -> F64:
    """The textbook direct-sum MDCT, independent of the FFT implementation."""
    n2 = len(x)
    n = n2 // 2
    t = np.arange(n2, dtype=np.float64)
    return np.array([
        float(np.sum(x * np.cos(np.pi / n * (t + 0.5 + n / 2) * (k + 0.5))))
        for k in range(n)
    ], dtype=np.float64)


def _imdct(coefficients: F64) -> F64:
    """The matching inverse, used only to prove TDAC reconstruction."""
    n = len(coefficients)
    bins = np.arange(n, dtype=np.float64)
    return (2.0 / n) * np.array([
        float(np.sum(
            coefficients * np.cos(np.pi / n * (t + 0.5 + n / 2) * (bins + 0.5))
        ))
        for t in range(2 * n)
    ], dtype=np.float64)


def _synthesize_lattice_signal(
    rng: np.random.Generator,
    window: F64,
    low_l: npt.NDArray[np.int64],
    high_l: npt.NDArray[np.int64],
    scale: float,
    *,
    frames: int = 24,
    offset: int = _TRUE_OFFSET,
) -> F64:
    """A signal whose MDCT coefficients ARE on the power-law quantisation
    lattice at ``offset`` — an analysis -> per-band quantisation -> synthesis
    round trip, exactly as validate.py.frozen builds it.

    Frames must come from ONE underlying signal, otherwise TDAC aliasing never
    cancels and no lattice survives into the output. The signal is scaled once
    before quantisation and never after: the power-law lattice is not
    scale-invariant.
    """
    n = IBLEN_LONG
    sf = np.linspace(0.3, 0.7, 8)
    src = rng.standard_normal(n * (frames + 4))
    src = np.convolve(src, np.ones(12) / 12.0, mode="same")
    src = src / np.abs(src).max() * 0.7
    out = np.zeros_like(src)
    for frame in range(frames + 2):
        start = offset + frame * n
        coefficients = mdct(src[start:start + 2 * n] * window) / scale
        quantised = coefficients.copy()
        for band in range(len(low_l) - 1):
            lo, hi = int(low_l[band]), int(high_l[band]) + 1
            segment = coefficients[lo:hi]
            peak = float(np.abs(segment).max())
            if peak <= 0:
                continue
            scalefactor = 4 * np.log2(peak) + 16 / 3 + 60
            fraction = float(rng.choice(sf))
            step = 2 ** (-3 / 16 * (fraction * scalefactor - 60))
            quantised[lo:hi] = np.sign(segment) * (
                np.rint(np.abs(segment) ** 0.75 * step) / step
            ) ** (4 / 3)
        out[start:start + 2 * n] += _imdct(quantised) * window
    return out


def _write_wav(
    path: str, samples: F64, rate: int, sample_width: int = 2,
) -> None:
    """Write mono PCM. 16-bit is what the measured corpus is: the paired
    ground-truth arm is 44.1 kHz / 16 bit library FLAC."""
    with wave.open(path, "wb") as fh:
        fh.setnchannels(1)
        fh.setsampwidth(sample_width)
        fh.setframerate(rate)
        full = float(2 ** (8 * sample_width - 1))
        values = np.clip(
            np.rint(samples * full), -full, full - 1,
        ).astype(np.int64).tolist()
        fh.writeframes(struct.pack(f"<{len(values)}h", *values))


class _LatticeFixture:
    """One synthesized lattice signal + its noise control, built once."""

    _built: _LatticeFixture | None = None

    def __init__(self) -> None:
        rng = np.random.default_rng(_SEED)
        low_l, high_l, low_s, high_s, windows = init_aac(44)
        self.low_l = low_l
        self.high_l = high_l
        self.low_s = low_s
        self.high_s = high_s
        self.windows = windows
        self.tau_l, self.tau_s = tau_tables(low_l, high_l, low_s, high_s)
        probe = rng.standard_normal(2 * IBLEN_LONG)
        windowed = windows.long * probe
        self.scale = float(mdct(windowed)[0] / _mdct_direct(windowed)[0])
        self.signal = _synthesize_lattice_signal(
            rng, windows.long, low_l, high_l, self.scale,
        )
        self.noise = rng.standard_normal(len(self.signal)) * 0.2

    @classmethod
    def get(cls) -> _LatticeFixture:
        if cls._built is None:
            cls._built = _LatticeFixture()
        return cls._built


class TestPortMathematics(unittest.TestCase):
    """validate.py.frozen checks 1-3, against the shipped module."""

    def test_mdct_matches_the_direct_dct_iv_definition(self) -> None:
        rng = np.random.default_rng(_SEED)
        x = rng.standard_normal(2 * IBLEN_LONG)
        ours = mdct(x)
        direct = _mdct_direct(x)
        ratio = ours / direct
        # The reference MDCT.m carries a sign/scale convention; the port must
        # match it up to exactly that one constant, and the constant must be
        # unit magnitude.
        self.assertTrue(
            np.allclose(ratio, ratio[0], rtol=1e-8, atol=1e-8),
            f"MDCT is not a constant multiple of DCT-IV: "
            f"maxdev={np.abs(ratio - ratio[0]).max():.3e}",
        )
        self.assertLess(abs(abs(float(ratio[0])) - 1.0), 1e-9)

    def test_tdac_perfect_reconstruction_with_the_sine_window(self) -> None:
        rng = np.random.default_rng(_SEED)
        _low_l, _high_l, _low_s, _high_s, windows = init_aac(44)
        window = windows.long
        n = IBLEN_LONG
        signal = rng.standard_normal(n * 8)
        probe = window * signal[0:2 * n]
        scale = float(mdct(probe)[0] / _mdct_direct(probe)[0])
        rebuilt = np.zeros_like(signal)
        for frame in range(len(signal) // n - 1):
            segment = signal[frame * n:(frame + 2) * n] * window
            rebuilt[frame * n:(frame + 2) * n] += (
                _imdct(mdct(segment) / scale) * window
            )
        middle = rebuilt[n:-n]
        self.assertTrue(
            np.allclose(middle, signal[n:-n], atol=1e-9),
            f"TDAC reconstruction error "
            f"{np.abs(middle - signal[n:-n]).max():.3e}",
        )

    def test_tau_is_conservative_against_the_true_null(self) -> None:
        """The port reproduces main.m's tau formula exactly by construction;
        this measures how good that formula's Gaussian approximation is. It is
        loose on narrow bands, which makes tau CONSERVATIVE there — a finding
        about the paper, never a licence to be anti-conservative."""
        rng = np.random.default_rng(_SEED)
        low_l, high_l, low_s, high_s, _windows = init_aac(44)
        tau_l, _tau_s = tau_tables(low_l, high_l, low_s, high_s)
        widths = (high_l - low_l + 1)[:len(tau_l) - 1]
        for width in sorted(set(widths.tolist())):
            with self.subTest(width=width):
                index = int(np.nonzero(widths == width)[0][0])
                errors = rng.uniform(-0.5, 0.5, size=(200000, int(width)))
                empirical = float(
                    np.quantile((errors ** 2).mean(axis=1), 0.01)
                )
                self.assertLessEqual(
                    float(tau_l[index]), empirical * 1.001,
                    f"tau is anti-conservative at width {width}",
                )

    def test_tau_tables_are_pinned_to_their_computed_values(self) -> None:
        """``tau`` is the per-band significance threshold every hit in the
        sweep is compared against, so it sets the scale of the whole
        statistic. The conservativeness check above only asserts an
        inequality against a sampled null — it stays green for a tau an
        order of magnitude off in the safe direction, which would still
        move ``proba``/``z`` far enough to invalidate PR-B's operating
        points. These are the exact values the shipped formula computes."""
        low_l, high_l, low_s, high_s, _windows = init_aac(44)
        tau_l, tau_s = tau_tables(low_l, high_l, low_s, high_s)
        self.assertAlmostEqual(
            float(tau_l[0]), 0.008744873181812551, places=12,
        )
        self.assertAlmostEqual(
            float(tau_l[-1]), 0.0656362054553284, places=12,
        )
        self.assertAlmostEqual(
            float(tau_s[0]), 0.05268100876336167, places=12,
        )

    def test_erfinv_round_trips_through_erf(self) -> None:
        """The scipy replacement. Bisection over ``math.erf`` must invert it
        to double precision across the whole range tau_tables ever asks for."""
        import math
        for y in (
            -0.999, -0.9, -0.5, -1e-6, 0.0, 1e-6, 0.5, 0.9,
            0.95, 0.9552, 0.98, 0.999, 0.99999,
        ):
            with self.subTest(y=y):
                self.assertAlmostEqual(math.erf(erfinv(y)), y, places=12)
        # The one literal value in this file, and it is scipy's:
        # scipy.special.erfinv(0.98) == 1.6449763571331858.
        self.assertAlmostEqual(erfinv(0.98), 1.6449763571331858, places=12)

    def test_erfinv_rejects_its_domain_boundary(self) -> None:
        for y in (-1.0, 1.0, 2.0):
            with self.subTest(y=y), self.assertRaises(ValueError):
                erfinv(y)


def _sweep(signal: F64, frames: npt.NDArray[np.int64]) -> F64:
    """Run the production offset sweep over ``frames`` of ``signal``."""
    fixture = _LatticeFixture.get()
    plans = (
        BandPlan(fixture.low_l, fixture.high_l, IBLEN_LONG, 1),
        BandPlan(fixture.low_s, fixture.high_s, 128, 8),
    )
    offsets = np.arange(IBLEN_LONG, dtype=np.int64)
    span = 2 * IBLEN_LONG + IBLEN_LONG - 1
    segments = np.stack([
        signal[(int(f) - 1) * IBLEN_LONG:(int(f) - 1) * IBLEN_LONG + span]
        for f in frames
    ])
    return detect_aac(
        segments, offsets, np.linspace(0.3, 0.7, 8), plans,
        fixture.windows, fixture.tau_l, fixture.tau_s,
    )


class TestSyntheticLatticeThroughDetectAac(unittest.TestCase):
    """validate.py.frozen checks 4-5, at the ``detect_aac`` layer the frozen
    validation used — the direct comparison point with the recorded run."""

    def _profile(self, signal: F64) -> F64:
        return _sweep(signal, np.arange(4, 12, dtype=np.int64))

    def test_detector_peaks_at_the_true_offset(self) -> None:
        profile = self._profile(_LatticeFixture.get().signal)
        self.assertEqual(int(np.argmax(profile)), _TRUE_OFFSET)

    def test_white_noise_control_stays_below_the_paper_lambda(self) -> None:
        profile = self._profile(_LatticeFixture.get().noise)
        self.assertLess(float(profile.max()), _PAPER_LAMBDA)

    def test_the_statistic_itself_is_pinned_not_just_its_argmax(self) -> None:
        """Recovering the right offset is necessary and nowhere near
        sufficient. PR-B's operating points are calibrated numbers —
        ``max_z > 12`` and the k>=4 concentration rule — so the MAGNITUDES
        this detector produces are part of the contract, not incidental
        output. Every constant the sweep depends on moves them: widening
        the scalefactor grid, thinning it, changing the power-law exponent,
        or loosening the null all keep ``argmax`` at 313 while collapsing
        ``proba`` by more than half."""
        profile = self._profile(_LatticeFixture.get().signal)
        peak = float(profile.max())
        median = float(np.median(profile))
        self.assertAlmostEqual(peak, 0.13346354166666666, delta=5e-4)
        self.assertAlmostEqual(median, 0.011270491803278689, delta=5e-5)
        self.assertAlmostEqual(
            (peak - median) / float(profile.std()),
            29.57719476721612,
            delta=0.05,
        )

    def test_the_noise_control_statistic_is_pinned_too(self) -> None:
        profile = self._profile(_LatticeFixture.get().noise)
        self.assertAlmostEqual(
            float(profile.max()), 0.01854066985645933, delta=5e-4,
        )


class TestFrameSelectionDirectionAndPooling(unittest.TestCase):
    """``mode=high`` is the shipped frame-selection rule and ``mode=low`` is
    measured dead (track AUC 0.644 against 0.995, with ``proba`` saturating
    at exactly 1.0000 on BOTH classes — derrien-refinement/README.md §
    "The NAC probe and ``mode=low``"). Selecting in the wrong direction is
    therefore not a degradation, it is the retired statistic; and it is
    invisible to any fixture whose lattice is spread evenly across frames.

    The pooling is load-bearing for the same reason: a frame's energy is
    its own block PLUS the next one, because an MDCT frame spans two."""

    def test_selects_the_highest_energy_frames_in_descending_order(
        self,
    ) -> None:
        energies = np.array([1.0, 2.0, 3.0, 4.0, 5.0, 6.0])
        # Pooled: [1+2, 2+3, 3+4, 4+5] = [3, 5, 7, 9] -> frames 4, 3, 2, 1.
        self.assertEqual(select_frames(energies, 4).tolist(), [4, 3, 2, 1])

    def test_pools_two_consecutive_blocks_per_frame(self) -> None:
        """The one block that wins alone is not the one that wins pooled."""
        energies = np.array([0.0, 6.0, 0.0, 5.0, 5.0, 0.0, 0.0])
        # Pooled:   [6, 6, 5, 10, 5] -> frame 4 wins.
        # Unpooled: [0, 6, 0,  5, 5] -> frame 2 would win.
        self.assertEqual(select_frames(energies, 1).tolist(), [4])


class TestEnergyConcentratedLattice(unittest.TestCase):
    """The consequence pin for frame-selection direction: a signal whose
    lattice lives ONLY in its loud half. ``mode=high`` finds it; the retired
    ``mode=low`` rule lands in the quiet half and finds nothing."""

    signal: F64
    tmpdir: str

    @classmethod
    def setUpClass(cls) -> None:
        fixture = _LatticeFixture.get()
        rng = np.random.default_rng(11)
        loud = _synthesize_lattice_signal(
            rng, fixture.windows.long, fixture.low_l, fixture.high_l,
            fixture.scale, frames=16, offset=_TRUE_OFFSET,
        )
        cls.signal = np.concatenate([loud, rng.standard_normal(len(loud)) * 0.02])
        cls.tmpdir = tempfile.mkdtemp(prefix="aac_lattice_conc_")
        _write_wav(
            os.path.join(cls.tmpdir, "concentrated.wav"), cls.signal, 44100,
        )

    @classmethod
    def tearDownClass(cls) -> None:
        shutil.rmtree(cls.tmpdir, ignore_errors=True)

    @classmethod
    def _pooled_db(cls) -> F64:
        nblk = len(cls.signal) // IBLEN_LONG
        blocks = (
            cls.signal[:nblk * IBLEN_LONG].reshape(nblk, IBLEN_LONG) ** 2
        ).sum(axis=1)
        nmax = nblk - 2
        with np.errstate(divide="ignore"):
            return 10 * np.log10(blocks[:nmax] + blocks[1:nmax + 1])

    def test_production_selection_recovers_the_concentrated_lattice(
        self,
    ) -> None:
        analysis = analyze_track(
            os.path.join(self.tmpdir, "concentrated.wav")
        )
        self.assertEqual(analysis.offset, _TRUE_OFFSET)
        self.assertAlmostEqual(analysis.proba, 0.13671875, delta=5e-4)
        self.assertGreater(analysis.z, 12.0)

    def test_the_retired_low_energy_rule_finds_nothing_on_it(self) -> None:
        """Same signal, same sweep, only the frame-selection direction
        differs — and the statistic collapses below the paper's null-rate
        ceiling, i.e. to "no lattice here at all"."""
        low_frames = np.argsort(self._pooled_db())[:DEFAULT_NB_WIN] + 1
        profile = _sweep(self.signal, low_frames.astype(np.int64))
        self.assertNotEqual(int(np.argmax(profile)), _TRUE_OFFSET)
        self.assertLess(float(profile.max()), _PAPER_LAMBDA)


class TestAnalyzeTrackEndToEnd(unittest.TestCase):
    """The production entry point: real WAV, real ffmpeg decode, real frame
    selection, real sweep."""

    tmpdir: str

    @classmethod
    def setUpClass(cls) -> None:
        cls.tmpdir = tempfile.mkdtemp(prefix="aac_lattice_e2e_")
        fixture = _LatticeFixture.get()
        _write_wav(
            os.path.join(cls.tmpdir, "lattice.wav"), fixture.signal, 44100,
        )
        _write_wav(
            os.path.join(cls.tmpdir, "noise.wav"), fixture.noise, 44100,
        )
        _write_wav(
            os.path.join(cls.tmpdir, "hires.wav"), fixture.signal, 96000,
        )
        _write_wav(
            os.path.join(cls.tmpdir, "short.wav"), fixture.signal[:1000], 44100,
        )
        with open(os.path.join(cls.tmpdir, "garbage.flac"), "wb") as fh:
            fh.write(b"this is not audio" * 64)

    @classmethod
    def tearDownClass(cls) -> None:
        shutil.rmtree(cls.tmpdir, ignore_errors=True)

    def _path(self, name: str) -> str:
        return os.path.join(self.tmpdir, name)

    def test_recovers_the_planted_offset_exactly(self) -> None:
        analysis = analyze_track(self._path("lattice.wav"))
        self.assertEqual(analysis.offset, _TRUE_OFFSET)
        self.assertEqual(analysis.sample_rate, 44100)
        self.assertEqual(analysis.channels, 1)
        # The lattice sweep is a spike, not a plateau: the frozen run
        # recorded z=29.6 here.
        self.assertGreater(analysis.z, 12.0)
        # And the magnitudes themselves are the contract, not just the
        # argmax — PR-B calibrates ``max_z > 12`` against exactly this
        # scale. See TestSyntheticLatticeThroughDetectAac's sibling pin.
        self.assertAlmostEqual(analysis.proba, 0.12890625, delta=5e-4)
        self.assertAlmostEqual(analysis.z, 29.38866851865431, delta=0.05)

    def test_white_noise_control_stays_below_the_paper_lambda(self) -> None:
        analysis = analyze_track(self._path("noise.wav"))
        self.assertLess(analysis.proba, _PAPER_LAMBDA)
        self.assertAlmostEqual(
            analysis.proba, 0.017344497607655503, delta=5e-4,
        )

    def test_96khz_input_raises_the_unsupported_rate_error(self) -> None:
        """The detector has no scalefactor-band table above 48 kHz, so hi-res
        input cannot be scored at all — 33 of each paired-corpus arm's 215
        tracks errored out for exactly this reason."""
        with self.assertRaises(AacLatticeUnsupportedRateError):
            analyze_track(self._path("hires.wav"))

    def test_undecodable_input_raises_the_decode_error(self) -> None:
        with self.assertRaises(AacLatticeDecodeError):
            analyze_track(self._path("garbage.flac"))

    def test_two_block_input_raises_the_too_short_error(self) -> None:
        with self.assertRaises(AacLatticeTooShortError):
            analyze_track(self._path("short.wav"))

    def test_select_frames_rejects_a_signal_with_no_selectable_frame(
        self,
    ) -> None:
        with self.assertRaises(AacLatticeTooShortError):
            select_frames(np.zeros(2, dtype=np.float64), 8)


class TestSampleRatePreScreen(unittest.TestCase):
    """A track the detector cannot score must not cost a full decode.

    The spy is a leaf-seam recorder over the external process boundary that
    DELEGATES to the real ``subprocess.run``: ffprobe really runs, ffmpeg
    really would, and the assertion is on which binaries were invoked."""

    tmpdir: str

    @classmethod
    def setUpClass(cls) -> None:
        cls.tmpdir = tempfile.mkdtemp(prefix="aac_lattice_probe_")
        fixture = _LatticeFixture.get()
        _write_wav(
            os.path.join(cls.tmpdir, "hires.wav"), fixture.signal, 96000,
        )
        _write_wav(
            os.path.join(cls.tmpdir, "ok.wav"), fixture.signal[:8192], 44100,
        )
        with open(os.path.join(cls.tmpdir, "garbage.flac"), "wb") as fh:
            fh.write(b"this is not audio" * 64)

    @classmethod
    def tearDownClass(cls) -> None:
        shutil.rmtree(cls.tmpdir, ignore_errors=True)

    def _path(self, name: str) -> str:
        return os.path.join(self.tmpdir, name)

    def _binaries_invoked(self, name: str) -> list[str]:
        real_run = subprocess.run
        invoked: list[str] = []

        def spy(cmd, *args, **kwargs):
            if isinstance(cmd, list) and cmd:
                invoked.append(str(cmd[0]))
            return real_run(cmd, *args, **kwargs)

        with (
            patch("lib.aac_lattice.subprocess.run", side_effect=spy),
            contextlib.suppress(AacLatticeError),
        ):
            analyze_track(self._path(name))
        return invoked

    def test_probe_reads_the_rate_without_decoding(self) -> None:
        self.assertEqual(probe_sample_rate_khz(self._path("hires.wav")), 96)
        self.assertEqual(probe_sample_rate_khz(self._path("ok.wav")), 44)

    def test_an_unprobeable_file_reports_none_and_is_still_decoded(
        self,
    ) -> None:
        """Fail-soft: ffprobe not answering must not become a new verdict.
        The file still gets its decode, and still reports the same
        decode-failure evidence it always did."""
        self.assertIsNone(probe_sample_rate_khz(self._path("garbage.flac")))
        self.assertIn("ffmpeg", self._binaries_invoked("garbage.flac"))
        with self.assertRaises(AacLatticeDecodeError):
            analyze_track(self._path("garbage.flac"))

    def test_unsupported_rate_never_reaches_ffmpeg(self) -> None:
        invoked = self._binaries_invoked("hires.wav")
        self.assertEqual(invoked, ["ffprobe"])

    def test_a_supported_rate_is_decoded_as_before(self) -> None:
        """Must-still-work guard: the pre-screen must not fail closed on the
        cohort the measurement exists to serve."""
        invoked = self._binaries_invoked("ok.wav")
        self.assertEqual(invoked[0], "ffprobe")
        self.assertIn("ffmpeg", invoked)

    def test_the_error_taxonomy_is_unchanged_by_the_pre_screen(self) -> None:
        """The persisted per-track evidence string for a 96 kHz track is
        operator-visible and already pinned elsewhere; the pre-screen must
        produce the identical one."""
        with self.assertRaises(AacLatticeUnsupportedRateError) as caught:
            analyze_track(self._path("hires.wav"))
        self.assertEqual(str(caught.exception), "unsupported sample rate 96 kHz")


class TestAlbumMeasurementWithRealFailures(unittest.TestCase):
    """Invariant A-I4's deterministic pin: an album where EVERY track fails,
    with the three real failures the corpus produced. The album still gets a
    measurement, every failure is recorded, and nothing raises."""

    tmpdir: str

    @classmethod
    def setUpClass(cls) -> None:
        cls.tmpdir = tempfile.mkdtemp(prefix="aac_lattice_fail_")
        fixture = _LatticeFixture.get()
        _write_wav(
            os.path.join(cls.tmpdir, "01 hires.wav"), fixture.signal, 96000,
        )
        with open(os.path.join(cls.tmpdir, "02 garbage.flac"), "wb") as fh:
            fh.write(b"this is not audio" * 64)
        _write_wav(
            os.path.join(cls.tmpdir, "03 short.wav"), fixture.signal[:1000],
            44100,
        )
        with open(os.path.join(cls.tmpdir, "cover.jpg"), "wb") as fh:
            fh.write(b"\xff\xd8\xff")

    @classmethod
    def tearDownClass(cls) -> None:
        shutil.rmtree(cls.tmpdir, ignore_errors=True)

    def test_every_failure_is_recorded_and_nothing_raises(self) -> None:
        capture = measure_album_aac_lattice(self.tmpdir)
        self.assertEqual(
            [track.filename for track in capture.tracks],
            ["01 hires.wav", "02 garbage.flac", "03 short.wav"],
        )
        self.assertTrue(all(track.error for track in capture.tracks))
        self.assertEqual(capture.scored_tracks, 0)
        self.assertIsNone(capture.modal_offset)
        self.assertIsNone(capture.modal_count)
        self.assertIsNone(capture.max_z)
        self.assertEqual(capture.validation_errors(), [])
        self.assertIn(
            "AacLatticeUnsupportedRateError", capture.tracks[0].error or "",
        )

    def test_non_audio_files_are_never_scored(self) -> None:
        self.assertEqual(
            album_audio_files(self.tmpdir),
            ["01 hires.wav", "02 garbage.flac", "03 short.wav"],
        )


class TestAlbumAudioFileOrdering(unittest.TestCase):
    """Deterministic selection is load-bearing: the ``MAX_SCORED_TRACKS`` cap
    means "which tracks" must be a function of the album, not of filesystem
    iteration order."""

    def test_nested_discs_sort_by_relative_path(self) -> None:
        with tempfile.TemporaryDirectory(prefix="aac_lattice_order_") as root:
            for relative in (
                "CD2/01 b.flac", "CD1/02 a.flac", "CD1/01 a.flac", "top.flac",
            ):
                path = os.path.join(root, relative)
                os.makedirs(os.path.dirname(path), exist_ok=True)
                with open(path, "wb") as fh:
                    fh.write(b"x")
            self.assertEqual(
                album_audio_files(root),
                [
                    "CD1/01 a.flac", "CD1/02 a.flac", "CD2/01 b.flac",
                    "top.flac",
                ],
            )

    @staticmethod
    def _seed_flacs(root: str, count: int) -> list[str]:
        names = [f"{index:02d}.flac" for index in range(count)]
        for name in names:
            with open(os.path.join(root, name), "wb") as fh:
                fh.write(b"x")
        return names

    @staticmethod
    def _analyzer(failures: frozenset[str]) -> Callable[[str], AacLatticeAnalysis]:
        def analyze(path: str) -> AacLatticeAnalysis:
            if os.path.basename(path) in failures:
                raise AacLatticeUnsupportedRateError(
                    "unsupported sample rate 96 kHz"
                )
            return AacLatticeAnalysis(
                offset=960, z=20.0, proba=0.12,
                sample_rate=44100, channels=2,
            )
        return analyze

    def test_scoring_stops_at_the_cap(self) -> None:
        with tempfile.TemporaryDirectory(prefix="aac_lattice_cap_") as root:
            names = self._seed_flacs(root, MAX_SCORED_TRACKS + 4)
            capture = measure_album_aac_lattice(
                root, analyze_fn=self._analyzer(frozenset()),
            )
            self.assertEqual(capture.scored_tracks, MAX_SCORED_TRACKS)
            self.assertEqual(len(capture.tracks), MAX_SCORED_TRACKS)
            self.assertEqual(
                [track.filename for track in capture.tracks],
                names[:MAX_SCORED_TRACKS],
            )

    def test_a_failure_does_not_consume_the_cap(self) -> None:
        """The cap counts SCORED tracks, and that is the whole point.

        A hi-res album's first files all raise ``AacLatticeUnsupportedRateError``
        (96 kHz has no scalefactor-band table). If a failure consumed a cap
        slot, an album whose first six files were hi-res would record ZERO
        scored tracks and the k>=4 concentration rule could never fire on it
        — two of the nineteen research-corpus albums are exactly that shape."""
        with tempfile.TemporaryDirectory(prefix="aac_lattice_cap_fail_") as root:
            names = self._seed_flacs(root, MAX_SCORED_TRACKS + 5)
            failures = frozenset(names[:3])
            capture = measure_album_aac_lattice(
                root, analyze_fn=self._analyzer(failures),
            )
            self.assertEqual(capture.scored_tracks, MAX_SCORED_TRACKS)
            self.assertEqual(
                len(capture.tracks), MAX_SCORED_TRACKS + len(failures),
            )
            self.assertEqual(
                [track.filename for track in capture.tracks],
                names[:MAX_SCORED_TRACKS + len(failures)],
            )
            self.assertEqual(
                [t.filename for t in capture.tracks if t.error],
                sorted(failures),
            )
            self.assertEqual(capture.modal_offset, 960)
            self.assertEqual(capture.modal_count, MAX_SCORED_TRACKS)
            self.assertEqual(capture.validation_errors(), [])


class TestAacLatticeCaptureDerivation(unittest.TestCase):
    """``AacLatticeCapture.from_tracks`` — the album statistic PR-B's
    offset-concentration rule will read."""

    @staticmethod
    def _scored(name: str, offset: int, z: float) -> AacLatticeTrackScore:
        return AacLatticeTrackScore(
            filename=name, offset=offset, z=z, proba=0.1,
        )

    def test_apple_shaped_album_concentrates_on_one_offset(self) -> None:
        """960 is CoreAudio's lattice: qaac primes 2112 samples, and
        1024 - (2112 mod 1024) = 960."""
        capture = AacLatticeCapture.from_tracks([
            self._scored("01.flac", 960, 25.0),
            self._scored("02.flac", 960, 28.0),
            self._scored("03.flac", 960, 31.0),
            self._scored("04.flac", 960, 24.0),
            self._scored("05.flac", 17, 4.0),
            AacLatticeTrackScore(filename="06.flac", error="boom"),
        ])
        self.assertEqual(capture.modal_offset, 960)
        self.assertEqual(capture.modal_count, 4)
        self.assertEqual(capture.scored_tracks, 5)
        self.assertEqual(capture.max_z, 31.0)
        self.assertEqual(len(capture.tracks), 6)
        self.assertEqual(capture.validation_errors(), [])

    def test_genuine_shaped_album_has_a_modal_count_of_one(self) -> None:
        capture = AacLatticeCapture.from_tracks([
            self._scored("01.flac", 803, 5.1),
            self._scored("02.flac", 17, 4.4),
            self._scored("03.flac", 512, 6.2),
        ])
        self.assertEqual(capture.modal_count, 1)
        self.assertEqual(capture.scored_tracks, 3)
        self.assertEqual(capture.max_z, 6.2)

    def test_a_tie_breaks_to_the_lowest_offset(self) -> None:
        capture = AacLatticeCapture.from_tracks([
            self._scored("01.flac", 960, 20.0),
            self._scored("02.flac", 960, 20.0),
            self._scored("03.flac", 7, 3.0),
            self._scored("04.flac", 7, 3.0),
        ])
        self.assertEqual(capture.modal_offset, 7)
        self.assertEqual(capture.modal_count, 2)

    def test_an_all_failed_album_carries_no_album_statistics(self) -> None:
        capture = AacLatticeCapture.from_tracks([
            AacLatticeTrackScore(filename="01.flac", error="a"),
            AacLatticeTrackScore(filename="02.flac", error="b"),
        ])
        self.assertEqual(capture.scored_tracks, 0)
        self.assertIsNone(capture.modal_offset)
        self.assertIsNone(capture.modal_count)
        self.assertIsNone(capture.max_z)
        self.assertEqual(capture.validation_errors(), [])


class TestAacLatticeValidation(unittest.TestCase):
    """The Python twin of migration 069's shape CHECK."""

    def test_a_scored_track_needs_every_statistic(self) -> None:
        errors = AacLatticeTrackScore(
            filename="01.flac", offset=960,
        ).validation_errors()
        self.assertTrue(any("offset, z and proba" in e for e in errors))

    def test_a_failed_track_carries_no_statistics(self) -> None:
        errors = AacLatticeTrackScore(
            filename="01.flac", offset=960, z=1.0, proba=0.1, error="boom",
        ).validation_errors()
        self.assertTrue(any("carries no statistics" in e for e in errors))

    def test_an_offset_outside_the_lattice_is_rejected(self) -> None:
        errors = AacLatticeTrackScore(
            filename="01.flac", offset=1024, z=1.0, proba=0.1,
        ).validation_errors()
        self.assertTrue(any("0-1023" in e for e in errors))

    def test_a_nameless_track_is_rejected(self) -> None:
        errors = AacLatticeTrackScore(
            filename="", offset=1, z=1.0, proba=0.1,
        ).validation_errors()
        self.assertTrue(any("filename is required" in e for e in errors))

    def test_a_miscounted_capture_is_rejected(self) -> None:
        errors = AacLatticeCapture(
            tracks=[AacLatticeTrackScore(
                filename="01.flac", offset=1, z=1.0, proba=0.1,
            )],
            modal_offset=1, modal_count=1, scored_tracks=9, max_z=1.0,
        ).validation_errors()
        self.assertTrue(any("scored_tracks must count" in e for e in errors))

    def test_album_statistics_without_a_scored_track_are_rejected(self) -> None:
        errors = AacLatticeCapture(
            tracks=[AacLatticeTrackScore(filename="01.flac", error="x")],
            modal_offset=960, modal_count=1, scored_tracks=0, max_z=3.0,
        ).validation_errors()
        self.assertTrue(
            any("require a scored track" in e for e in errors)
        )

    def test_a_scored_album_without_statistics_is_rejected(self) -> None:
        errors = AacLatticeCapture(
            tracks=[AacLatticeTrackScore(
                filename="01.flac", offset=1, z=1.0, proba=0.1,
            )],
            scored_tracks=1,
        ).validation_errors()
        self.assertTrue(any("statistics are missing" in e for e in errors))

    def test_a_modal_count_above_the_scored_count_is_rejected(self) -> None:
        errors = AacLatticeCapture(
            tracks=[AacLatticeTrackScore(
                filename="01.flac", offset=1, z=1.0, proba=0.1,
            )],
            modal_offset=1, modal_count=4, scored_tracks=1, max_z=1.0,
        ).validation_errors()
        self.assertTrue(any("between 1 and scored_tracks" in e for e in errors))


if __name__ == "__main__":
    unittest.main()
