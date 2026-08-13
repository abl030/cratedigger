"""Generated properties for the one MP3 ladder and its proof (issue #1145).

Three production invariants, each paired with a deterministic pin elsewhere:

* **I1 — one ladder.** A bare measured MP3 ranks through exactly one band
  table, whatever ``is_cbr`` says. Pin:
  ``tests/test_quality_decisions.py::TestQualityRank``.
* **I2 — contract only by proof.** An ``mp3 vN`` contract is minted only from
  a parsed LAME ``-V N`` (or the one mapped preset), never inferred from
  bitrate, uniformity, or an encoding mode. Pin:
  ``tests/test_encoder_contract.py``.
* **I3 — comparison symmetry.** Two measurements of the same audio rank equal
  regardless of which side of a comparison they sit on. Pin:
  ``tests/test_encoder_contract.py::TestContractChangesTheDecidedOutcome``.
* **I4 — a spectral class only decides against another spectral class.**
  ``spectral_tiebreak`` fires only when BOTH sides' clamped values ARE their
  spectral classes; one bound side against one raw metric falls through to the
  tolerant ``metric_tiebreak``. Pin:
  ``tests/test_quality_decisions.py::TestCompareQualitySharedSpectralBucket``
  ``::test_one_bound_side_never_reaches_the_spectral_tiebreak``.

  This invariant is here because #1145 deleted ``_classify_with_cbr_bands``,
  which was ``both_spectral_bound``'s other consumer. The tiebreak guard
  became the flags' only reader and no test reached it, so both faithful
  mutants survived every quality/compare/spectral module.

Every checker below accumulates violations rather than short-circuiting on
the first ``raise``, so one clause cannot mask another (the
``mode_selection_violations`` pattern), and every clause has a
message-asserting known-bad self-test in
``TestMp3LadderCheckersTripOnViolations``.
"""

import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from hypothesis import example, given
from hypothesis import strategies as st

import tests._hypothesis_profiles  # noqa: F401  (loads the active profile)
from lib.quality import (
    AudioQualityMeasurement,
    CodecRankBands,
    QualityRank,
    QualityRankConfig,
    compare_quality,
    lame_vbr_level,
    measurement_rank,
    mp3_vbr_contract_format,
    mp3_vbr_contract_level,
    quality_rank,
)
from lib.quality.compare import _shared_spectral_bitrates
from lib.quality.spectral_interpretation import interpret_measurement
from lib.spectral_check import LAME_LOWPASS

CFG = QualityRankConfig.defaults()

#: Bare codec labels the rank model recognises, in the spellings production
#: really produces: Beets writes ``"MP3"``/``"FLAC"``, the harness upper-cases
#: the probed codec, and ``native_codec_format_label`` emits lower-case for the
#: non-MP3 families.
_BARE_LOSSY_LABELS = ("MP3", "mp3", "Opus", "opus", "AAC", "aac",
                      "Vorbis", "vorbis", "WMA", "wma")

#: The only spectral class values a producer can emit, DERIVED from the
#: producer's own table rather than transcribed
#: (``.claude/rules/test-fidelity.md`` Rule C). A number outside this set is
#: not a legacy bucket, carries no class under the codec-aware
#: interpretation, and so describes a world where the clamp these properties
#: patrol never fires at all.
_PRODUCIBLE_CLASSES: tuple[int, ...] = tuple(
    kbps for _cliff_hz, kbps in LAME_LOWPASS
)

#: Grades that authorise a spectral FINDING. Without one the interpretation is
#: audit-only, carries no class, and the clamp is withheld — so a strategy
#: that omitted them would generate nothing but the no-clamp path.
_ACCUSING_GRADES = ("suspect", "likely_transcode")

#: Settings strings drawn from the live ``items.encoder_settings`` census.
_LIVE_SETTINGS = (
    None, "", "   ",
    "-V 0", "-V 1", "-V 2", "-V 4", "-V 5", "-V 9",
    "-V 0 --vbr-new", "-V 2 --vbr-new", "-V 0 --vbr-old",
    "--alt-preset standard", "--preset standard",
    "--preset insane", "--preset extreme", "--alt-preset extreme",
    "-b 320", "-b 256", "-b 192", "-b 160", "-b 128", "-b 255+",
    "--abr 255+", "--preset 240", "--preset 256", "--alt-preset 246",
)


def _bitrates() -> st.SearchStrategy[int]:
    return st.integers(min_value=1, max_value=2000)


def _band_tables() -> st.SearchStrategy[CodecRankBands]:
    """Monotonic band tables, so the property is not pinned to the shipped one."""
    return st.tuples(
        st.integers(min_value=0, max_value=400),
        st.integers(min_value=0, max_value=400),
        st.integers(min_value=0, max_value=400),
        st.integers(min_value=0, max_value=400),
    ).map(lambda values: sorted(values, reverse=True)).map(
        lambda ordered: CodecRankBands(
            transparent=ordered[0], excellent=ordered[1],
            good=ordered[2], acceptable=ordered[3],
        )
    )


# ---------------------------------------------------------------------------
# I1 — one MP3 ladder
# ---------------------------------------------------------------------------

def single_ladder_violations(
    *,
    label: str,
    bitrate: int,
    cbr_rank: QualityRank,
    vbr_rank: QualityRank,
    table_rank: QualityRank,
    cfg: QualityRankConfig,
) -> list[str]:
    """Every way a measured lossy rank could still depend on an encoding mode.

    ``cbr_rank``/``vbr_rank`` are ``measurement_rank`` over the SAME audio with
    ``is_cbr`` true and false; ``table_rank`` is the family's own band table
    applied directly to the same bitrate.
    """
    violations: list[str] = []
    if cbr_rank != vbr_rank:
        violations.append(
            f"encoding mode changed the rank for {label!r} at {bitrate}: "
            f"is_cbr=True gave {cbr_rank.name}, is_cbr=False gave "
            f"{vbr_rank.name}"
        )
    if cbr_rank != table_rank:
        violations.append(
            f"measured rank for {label!r} at {bitrate} is not its own band "
            f"table's answer: got {cbr_rank.name}, table says "
            f"{table_rank.name}"
        )
    if label.strip().lower() == "mp3" and table_rank != cfg.mp3.rank_for(bitrate):
        violations.append(
            f"MP3 at {bitrate} did not route through cfg.mp3: got "
            f"{table_rank.name}, cfg.mp3 says {cfg.mp3.rank_for(bitrate).name}"
        )
    return violations


def _bands_for(label: str, cfg: QualityRankConfig) -> CodecRankBands:
    return {
        "mp3": cfg.mp3,
        "opus": cfg.opus,
        "aac": cfg.aac,
        "vorbis": cfg.vorbis,
        "wma": cfg.wma,
    }[label.strip().lower()]


# ---------------------------------------------------------------------------
# I2 — a contract is minted only from proof
# ---------------------------------------------------------------------------

def contract_minting_violations(
    *,
    settings: list[str | None],
    contract: str | None,
) -> list[str]:
    """The contract must be exactly the unanimous, explicit LAME level."""
    violations: list[str] = []
    levels = [lame_vbr_level(value) for value in settings]
    unanimous = (
        bool(levels) and None not in levels and len(set(levels)) == 1
    )
    if contract is not None and not unanimous:
        violations.append(
            f"minted {contract!r} without a unanimous explicit level: "
            f"{settings!r}"
        )
    if contract is None and unanimous:
        violations.append(
            f"withheld a contract from a unanimous explicit level: "
            f"{settings!r}"
        )
    if contract is not None:
        level = mp3_vbr_contract_level(contract)
        if level is None:
            violations.append(
                f"minted {contract!r}, which its own reader cannot parse"
            )
        elif levels and level != levels[0]:
            violations.append(
                f"minted {contract!r} for level {levels[0]!r}"
            )
    return violations


# ---------------------------------------------------------------------------
# I3 — comparison symmetry
# ---------------------------------------------------------------------------

_MIRRORED = {"better": "worse", "worse": "better", "equivalent": "equivalent"}


# ---------------------------------------------------------------------------
# I4 — a spectral class only decides against another spectral class
# ---------------------------------------------------------------------------

def spectral_tiebreak_violations(
    *,
    shared: tuple[int | None, int | None, bool, bool] | None,
    branch: str,
    verdict: str,
    new_value: int | None,
    existing_value: int | None,
) -> list[str]:
    """Every way the ``spectral_tiebreak`` branch could stop being a gate.

    ``shared`` is ``_shared_spectral_bitrates``' own return for the same pair
    — the production derivation of which side is bound, never a
    re-derivation here. ``branch``/``verdict``/``*_value`` come from the real
    ``compare_quality`` basis.
    """
    violations: list[str] = []
    if branch != "spectral_tiebreak":
        return violations
    if shared is None:
        violations.append(
            "spectral_tiebreak fired with no shared spectral clamp at all"
        )
        return violations
    _new_clamped, _existing_clamped, new_bound, existing_bound = shared
    if not (new_bound and existing_bound):
        violations.append(
            "spectral_tiebreak fired without both sides spectral-bound: "
            f"new_bound={new_bound}, existing_bound={existing_bound} "
            f"({new_value} vs {existing_value})"
        )
    if new_value is None or existing_value is None:
        violations.append(
            "spectral_tiebreak decided on a missing value: "
            f"{new_value} vs {existing_value}"
        )
    elif new_value == existing_value:
        violations.append(
            f"spectral_tiebreak fired on equal clamped values: {new_value}"
        )
    elif verdict != ("better" if new_value > existing_value else "worse"):
        violations.append(
            f"spectral_tiebreak verdict {verdict!r} contradicts its own "
            f"clamped values {new_value} vs {existing_value}"
        )
    return violations


def comparison_symmetry_violations(
    *,
    forward: str,
    reverse: str,
    identical_audio: bool,
) -> list[str]:
    """Swapping the two sides must mirror the verdict, never change it."""
    violations: list[str] = []
    if _MIRRORED.get(forward) != reverse:
        violations.append(
            f"verdict is not mirrored under a side swap: forward={forward!r}, "
            f"reverse={reverse!r}"
        )
    if identical_audio and forward != "equivalent":
        violations.append(
            f"identical audio did not compare equivalent: {forward!r}"
        )
    return violations


class TestOneMp3LadderGenerated(unittest.TestCase):
    @given(
        label=st.sampled_from(_BARE_LOSSY_LABELS),
        bitrate=_bitrates(),
    )
    @example(label="MP3", bitrate=245)   # the collapse's own boundary case
    @example(label="MP3", bitrate=320)
    @example(label="MP3", bitrate=127)
    def test_a_measured_lossy_rank_never_reads_the_encoding_mode(
        self, label: str, bitrate: int,
    ) -> None:
        def rank(is_cbr: bool) -> QualityRank:
            return measurement_rank(
                AudioQualityMeasurement(
                    min_bitrate_kbps=bitrate,
                    avg_bitrate_kbps=bitrate,
                    median_bitrate_kbps=bitrate,
                    format=label,
                    is_cbr=is_cbr,
                ),
                CFG,
            )

        violations = single_ladder_violations(
            label=label,
            bitrate=bitrate,
            cbr_rank=rank(True),
            vbr_rank=rank(False),
            table_rank=_bands_for(label, CFG).rank_for(bitrate),
            cfg=CFG,
        )
        self.assertEqual(violations, [], f"{label} @ {bitrate}")

    @given(bands=_band_tables(), bitrate=_bitrates())
    def test_the_mp3_table_is_the_only_input_to_a_measured_mp3_rank(
        self, bands: CodecRankBands, bitrate: int,
    ) -> None:
        """Retuning ``cfg.mp3`` retunes every measured MP3, and nothing else.

        Driven over arbitrary monotonic tables so the property cannot be
        satisfied by the shipped numbers happening to agree.
        """
        cfg = QualityRankConfig(mp3=bands)
        self.assertEqual(
            quality_rank("MP3", bitrate, cfg), bands.rank_for(bitrate))
        # A retuned MP3 table cannot move any other family.
        for other in ("Opus", "AAC", "Vorbis", "WMA"):
            self.assertEqual(
                quality_rank(other, bitrate, cfg),
                quality_rank(other, bitrate, CFG),
            )


class TestContractOnlyByProofGenerated(unittest.TestCase):
    @given(settings=st.lists(
        st.sampled_from(_LIVE_SETTINGS), min_size=0, max_size=6))
    @example(settings=["-V 0", "-V 0"])
    @example(settings=["-V 0", "-V 2"])
    @example(settings=["-V 0", None])
    @example(settings=["--alt-preset standard", "-V 2"])
    @example(settings=["--preset standard", "--preset standard"])
    @example(settings=[])
    def test_a_contract_is_exactly_the_unanimous_explicit_level(
        self, settings: list[str | None],
    ) -> None:
        violations = contract_minting_violations(
            settings=settings,
            contract=mp3_vbr_contract_format(settings),
        )
        self.assertEqual(violations, [], repr(settings))

    @given(
        settings=st.lists(
            st.sampled_from(_LIVE_SETTINGS), min_size=1, max_size=6),
        bitrate=_bitrates(),
        is_cbr=st.booleans(),
    )
    def test_bitrate_and_encoding_mode_never_enter_the_contract(
        self, settings: list[str | None], bitrate: int, is_cbr: bool,
    ) -> None:
        """The mint reads the LAME string and nothing else.

        ``bitrate``/``is_cbr`` are drawn and deliberately unused as MINT
        inputs: the assertion is that the contract for a given settings list
        is the same whatever measurement surrounds it, which is the
        inference this issue removed.
        """
        contract = mp3_vbr_contract_format(settings)
        measurement = AudioQualityMeasurement(
            min_bitrate_kbps=bitrate,
            avg_bitrate_kbps=bitrate,
            median_bitrate_kbps=bitrate,
            format=contract or "MP3",
            is_cbr=is_cbr,
        )
        self.assertEqual(mp3_vbr_contract_format(settings), contract)
        if contract is not None:
            level = mp3_vbr_contract_level(contract)
            assert level is not None
            # A contract is self-certifying: its rank is the configured
            # V-level rank, never the measured bitrate's band.
            self.assertEqual(
                measurement_rank(measurement, CFG), CFG.mp3_vbr_levels[level])


class TestComparisonSymmetryGenerated(unittest.TestCase):
    @given(
        left_label=st.sampled_from(_BARE_LOSSY_LABELS + ("mp3 v0", "mp3 v2")),
        right_label=st.sampled_from(_BARE_LOSSY_LABELS + ("mp3 v0", "mp3 v2")),
        left_bitrate=_bitrates(),
        right_bitrate=_bitrates(),
    )
    @example(
        left_label="MP3", right_label="MP3",
        left_bitrate=245, right_bitrate=245,
    )
    @example(
        left_label="mp3 v0", right_label="MP3",
        left_bitrate=245, right_bitrate=245,
    )
    def test_a_side_swap_mirrors_the_verdict(
        self,
        left_label: str,
        right_label: str,
        left_bitrate: int,
        right_bitrate: int,
    ) -> None:
        def measurement(label: str, bitrate: int) -> AudioQualityMeasurement:
            return AudioQualityMeasurement(
                min_bitrate_kbps=bitrate,
                avg_bitrate_kbps=bitrate,
                median_bitrate_kbps=bitrate,
                format=label,
            )

        left = measurement(left_label, left_bitrate)
        right = measurement(right_label, right_bitrate)
        violations = comparison_symmetry_violations(
            forward=compare_quality(left, right, CFG).verdict,
            reverse=compare_quality(right, left, CFG).verdict,
            identical_audio=(
                left_label == right_label and left_bitrate == right_bitrate
            ),
        )
        self.assertEqual(violations, [], f"{left!r} vs {right!r}")


def _spectral_measurement(
    label: str, raw: int, spectral_class: int, grade: str,
) -> AudioQualityMeasurement:
    return AudioQualityMeasurement(
        min_bitrate_kbps=raw,
        avg_bitrate_kbps=raw,
        median_bitrate_kbps=raw,
        format=label,
        spectral_grade=grade,
        spectral_bitrate_kbps=spectral_class,
    )


class TestSpectralTiebreakIsGatedGenerated(unittest.TestCase):
    """The clamp branch, driven over worlds that actually reach it."""

    @given(
        label=st.sampled_from(("MP3", "mp3")),
        new_raw=st.integers(min_value=64, max_value=400),
        existing_raw=st.integers(min_value=64, max_value=400),
        new_class=st.sampled_from(_PRODUCIBLE_CLASSES),
        existing_class=st.sampled_from(_PRODUCIBLE_CLASSES),
        new_grade=st.sampled_from(_ACCUSING_GRADES),
        existing_grade=st.sampled_from(_ACCUSING_GRADES),
    )
    # The asymmetric world the deterministic pin names: only the candidate's
    # class binds, so the branch must withhold.
    @example(
        label="MP3", new_raw=320, existing_raw=224,
        new_class=192, existing_class=256,
        new_grade="likely_transcode", existing_grade="likely_transcode",
    )
    # Its mirror — only the INSTALLED side bound.
    @example(
        label="MP3", new_raw=224, existing_raw=320,
        new_class=256, existing_class=192,
        new_grade="likely_transcode", existing_grade="likely_transcode",
    )
    # Both bound and differing: the branch's own live world.
    @example(
        label="MP3", new_raw=320, existing_raw=320,
        new_class=224, existing_class=192,
        new_grade="likely_transcode", existing_grade="likely_transcode",
    )
    def test_the_tiebreak_only_fires_with_both_sides_bound(
        self,
        label: str,
        new_raw: int,
        existing_raw: int,
        new_class: int,
        existing_class: int,
        new_grade: str,
        existing_grade: str,
    ) -> None:
        new = _spectral_measurement(label, new_raw, new_class, new_grade)
        existing = _spectral_measurement(
            label, existing_raw, existing_class, existing_grade)
        shared = _shared_spectral_bitrates(
            new, existing, CFG,
            new_spectral=interpret_measurement(new),
            existing_spectral=interpret_measurement(existing),
        )
        basis = compare_quality(new, existing, CFG)
        violations = spectral_tiebreak_violations(
            shared=shared,
            branch=basis.branch,
            verdict=basis.verdict,
            new_value=basis.new_value_kbps,
            existing_value=basis.existing_value_kbps,
        )
        self.assertEqual(violations, [], f"{new!r} vs {existing!r}")

    @given(
        new_raw=st.integers(min_value=64, max_value=400),
        existing_raw=st.integers(min_value=64, max_value=400),
        new_class=st.sampled_from(_PRODUCIBLE_CLASSES),
        existing_class=st.sampled_from(_PRODUCIBLE_CLASSES),
    )
    @example(
        new_raw=320, existing_raw=224, new_class=192, existing_class=256)
    def test_an_unbound_side_keeps_its_raw_metric(
        self,
        new_raw: int,
        existing_raw: int,
        new_class: int,
        existing_class: int,
    ) -> None:
        """The flags mean what the branch reads them as.

        A side is spectral-bound iff its class is at or below its own selected
        metric, and the value the clamp returns for an unbound side is that
        raw metric untouched. That is the fact ``spectral_tiebreak`` depends
        on, asserted at the helper rather than inferred from a verdict.
        """
        new = _spectral_measurement("MP3", new_raw, new_class, "suspect")
        existing = _spectral_measurement(
            "MP3", existing_raw, existing_class, "suspect")
        shared = _shared_spectral_bitrates(
            new, existing, CFG,
            new_spectral=interpret_measurement(new),
            existing_spectral=interpret_measurement(existing),
        )
        assert shared is not None, "same-codec accusing pair must be comparable"
        new_value, existing_value, new_bound, existing_bound = shared
        self.assertEqual(new_bound, new_class <= new_raw)
        self.assertEqual(existing_bound, existing_class <= existing_raw)
        self.assertEqual(new_value, new_class if new_bound else new_raw)
        self.assertEqual(
            existing_value, existing_class if existing_bound else existing_raw)


class TestMp3LadderCheckersTripOnViolations(unittest.TestCase):
    """Known-bad self-tests: one minimal world per CLAUSE, asserting that
    clause's own message. Every clause here is reachable while the clauses
    before it pass, so an accumulating checker cannot hide one behind another.
    """

    def test_single_ladder_clause_encoding_mode(self) -> None:
        violations = single_ladder_violations(
            label="MP3", bitrate=245,
            cbr_rank=QualityRank.TRANSPARENT,
            vbr_rank=QualityRank.GOOD,
            table_rank=QualityRank.TRANSPARENT,
            cfg=CFG,
        )
        self.assertTrue(
            any("encoding mode changed the rank" in item for item in violations),
            violations,
        )

    def test_single_ladder_clause_table_disagreement(self) -> None:
        violations = single_ladder_violations(
            label="Opus", bitrate=120,
            cbr_rank=QualityRank.POOR,
            vbr_rank=QualityRank.POOR,
            table_rank=QualityRank.TRANSPARENT,
            cfg=CFG,
        )
        self.assertTrue(
            any("is not its own band table's answer" in item
                for item in violations),
            violations,
        )

    def test_single_ladder_clause_mp3_routed_elsewhere(self) -> None:
        """MP3-specific clause, reached with the two earlier clauses clean.

        Both ranks agree and equal the ``table_rank`` handed in, so only the
        third clause can fire — the world a mutant routing MP3 at a second
        table would produce.
        """
        wrong = CFG.mp3.rank_for(245)
        self.assertNotEqual(wrong, QualityRank.TRANSPARENT)
        violations = single_ladder_violations(
            label="MP3", bitrate=245,
            cbr_rank=QualityRank.TRANSPARENT,
            vbr_rank=QualityRank.TRANSPARENT,
            table_rank=QualityRank.TRANSPARENT,
            cfg=CFG,
        )
        self.assertEqual(len(violations), 1, violations)
        self.assertIn("did not route through cfg.mp3", violations[0])

    def test_contract_clause_minted_without_unanimity(self) -> None:
        violations = contract_minting_violations(
            settings=["-V 0", "-b 320"], contract="mp3 v0",
        )
        self.assertTrue(
            any("without a unanimous explicit level" in item
                for item in violations),
            violations,
        )

    def test_contract_clause_withheld_from_proof(self) -> None:
        violations = contract_minting_violations(
            settings=["-V 2", "-V 2"], contract=None,
        )
        self.assertEqual(len(violations), 1, violations)
        self.assertIn("withheld a contract", violations[0])

    def test_contract_clause_unparseable_label(self) -> None:
        """Reached with unanimity satisfied, so only this clause can fire."""
        violations = contract_minting_violations(
            settings=["-V 0", "-V 0"], contract="mp3 vzero",
        )
        self.assertEqual(len(violations), 1, violations)
        self.assertIn("its own reader cannot parse", violations[0])

    def test_contract_clause_wrong_level(self) -> None:
        violations = contract_minting_violations(
            settings=["-V 0", "-V 0"], contract="mp3 v2",
        )
        self.assertEqual(len(violations), 1, violations)
        self.assertIn("minted 'mp3 v2' for level 0", violations[0])

    def test_symmetry_clause_unmirrored_verdict(self) -> None:
        violations = comparison_symmetry_violations(
            forward="better", reverse="better", identical_audio=False,
        )
        self.assertEqual(len(violations), 1, violations)
        self.assertIn("not mirrored under a side swap", violations[0])

    def test_symmetry_clause_identical_audio(self) -> None:
        """Mirrored but not equivalent — only the second clause can fire."""
        violations = comparison_symmetry_violations(
            forward="better", reverse="worse", identical_audio=True,
        )
        self.assertEqual(len(violations), 1, violations)
        self.assertIn("identical audio did not compare equivalent",
                      violations[0])

    def test_tiebreak_clause_no_shared_clamp(self) -> None:
        violations = spectral_tiebreak_violations(
            shared=None, branch="spectral_tiebreak", verdict="better",
            new_value=224, existing_value=192,
        )
        self.assertEqual(len(violations), 1, violations)
        self.assertIn("no shared spectral clamp at all", violations[0])

    def test_tiebreak_clause_one_side_unbound(self) -> None:
        """The exact shape both surviving mutants produce."""
        violations = spectral_tiebreak_violations(
            shared=(192, 224, True, False), branch="spectral_tiebreak",
            verdict="worse", new_value=192, existing_value=224,
        )
        self.assertEqual(len(violations), 1, violations)
        self.assertIn("without both sides spectral-bound", violations[0])

    def test_tiebreak_clause_missing_value(self) -> None:
        """Both bound, so only the value clause can fire."""
        violations = spectral_tiebreak_violations(
            shared=(None, 192, True, True), branch="spectral_tiebreak",
            verdict="better", new_value=None, existing_value=192,
        )
        self.assertEqual(len(violations), 1, violations)
        self.assertIn("decided on a missing value", violations[0])

    def test_tiebreak_clause_equal_values(self) -> None:
        violations = spectral_tiebreak_violations(
            shared=(192, 192, True, True), branch="spectral_tiebreak",
            verdict="better", new_value=192, existing_value=192,
        )
        self.assertEqual(len(violations), 1, violations)
        self.assertIn("fired on equal clamped values", violations[0])

    def test_tiebreak_clause_verdict_contradicts_values(self) -> None:
        violations = spectral_tiebreak_violations(
            shared=(224, 192, True, True), branch="spectral_tiebreak",
            verdict="worse", new_value=224, existing_value=192,
        )
        self.assertEqual(len(violations), 1, violations)
        self.assertIn("contradicts its own", violations[0])

    def test_tiebreak_checker_is_silent_on_every_other_branch(self) -> None:
        """Must-still-work: the checker judges its own branch and no other."""
        for branch in ("rank", "metric_tiebreak", "label_contract_same_rank",
                       "cross_family_same_rank", "spectral_candidate_bound"):
            with self.subTest(branch=branch):
                self.assertEqual(
                    spectral_tiebreak_violations(
                        shared=(192, 224, True, False), branch=branch,
                        verdict="worse", new_value=192, existing_value=224,
                    ),
                    [],
                )


if __name__ == "__main__":
    unittest.main()
