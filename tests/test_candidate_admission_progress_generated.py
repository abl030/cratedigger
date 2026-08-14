"""Generated patrol: candidate admission never blocks on an unsatisfiable fact.

Issue #1162. The importer requeues a job to preview whenever candidate
evidence is not admissible, on the premise that a re-measurement will
change the world. That premise is load-bearing: when it is false the two
workers form a livelock neither can escape, because each pass re-derives
exactly the same state (job 60635 ran 2,463 passes over 39.2 h; its
successor 60708 another 238; both were ended from outside the pipeline).

The invariant this patrols:

    Admission must never demand a fact that a re-measurement of the same
    bytes is DEFINED not to produce -- and must not, in exempting one,
    stop enforcing anything else.

The instance covered here is the CD-rip bypass. ``lib/measurement.py``
skips the spectral gate outright when a measured CD-rip bit verification
is present, so those bytes are never re-graded and their spectral
generation can never advance. The sibling instance -- lossy codecs that
``_needs_spectral_check`` skips for a codec reason -- is NOT exempted,
deliberately (issue #1167), and the ``spectral_vl`` proof shape below
pins that the exemption is keyed on the CD-rip fact itself rather than on
any measured verified-lossless proof.

This composes the real evidence rows with the real admission loaders.
"""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from typing import ClassVar, Literal

from hypothesis import example, given
from hypothesis import strategies as st

import tests._hypothesis_profiles  # noqa: F401
from lib.quality import (
    AccurateRipBitMatch,
    AudioQualityMeasurement,
    CdRipBitVerification,
    CdTocIdentity,
    EvidenceProvenance,
    VerifiedLosslessProof,
)
from lib.quality.decisions import mint_verified_lossless_proof
from lib.quality_evidence import (
    load_candidate_evidence_for_decision,
    load_candidate_evidence_for_source,
    snapshot_audio_files,
)
from lib.spectral_check import SPECTRAL_MEASUREMENT_VERSION
from tests.fakes import FakePipelineDB
from tests.helpers import make_album_quality_evidence, make_request_row

Generation = Literal["null", "old", "current"]
ProofShape = Literal["none", "measured", "carried", "spectral_vl"]
# No ``bitrate``-only member: ``storage_validation_errors`` rejects a spectral
# bitrate without a grade ("spectral bitrate requires a spectral grade"), so a
# PERSISTED row can never carry one, and this loader only ever reads persisted
# rows. Production's staleness predicate still tests the bitrate as
# fail-closed legislation for a future writer; that half is unreachable from
# here by construction rather than untested.
SpectralFact = Literal["none", "grade", "both"]

_GENERATION_GATE_REASON = "spectral measurement generation is not current"


def _generation_value(generation: Generation) -> int | None:
    if generation == "null":
        return None
    if generation == "old":
        return max(0, SPECTRAL_MEASUREMENT_VERSION - 1)
    return SPECTRAL_MEASUREMENT_VERSION


def cd_rip_proof(
    *,
    provenance: EvidenceProvenance = "measured",
) -> CdRipBitVerification:
    """A production-shaped AccurateRip all-track bit match."""
    return CdRipBitVerification(
        provenance=provenance,
        source_format="flac",
        toc=CdTocIdentity(
            track_offsets_sectors=[0, 7013],
            leadout_sector=38019,
            accuraterip_id="001f0be7-0195fc21-030a4a12",
            musicbrainz_disc_id="8NDppmvWEFT9wu_FMwlZKkhZDgE-",
        ),
        accuraterip=AccurateRipBitMatch(
            provider="accuraterip",
            url="https://www.accuraterip.com/accuraterip/x.bin",
            checksum_version="arv2",
            read_offset_samples=0,
            track_confidences=[12, 12],
            track_checksums=[0xAABBCCDD, 0x11223344],
            response_sha256="a" * 64,
        ),
    )


def spectral_verified_lossless_proof() -> VerifiedLosslessProof:
    """A measured verified-lossless proof that is NOT a CD-rip proof.

    Minted by the production policy owner rather than hand-typed, so the
    classifier is whatever production actually spells (Rule C).
    """
    proof = mint_verified_lossless_proof(
        True,
        was_converted_from="flac",
        detected_source_format="flac",
        spectral_grade="genuine",
    )
    assert proof is not None
    return proof


def admission_progress_violations(
    *,
    generation: Generation,
    proof_shape: ProofShape,
    spectral_fact: SpectralFact,
    policy_error: bool,
    decision_admitted: bool,
    decision_reason: str,
    cache_admitted: bool,
) -> list[str]:
    """Name every violated admission-progress invariant.

    Accumulating rather than short-circuiting, so clause ordering can never
    mask a violation (``.claude/rules/code-quality.md``).
    """
    violations: list[str] = []
    stale = spectral_fact != "none" and _generation_value(generation) != (
        SPECTRAL_MEASUREMENT_VERSION
    )
    # Only a MEASURED CD-rip fact witnesses the producer's own spectral skip.
    # A measured verified-lossless proof with a spectral classifier does not:
    # those bytes are re-gradable, so the demand stays satisfiable.
    bypassed = proof_shape == "measured"

    # The bypass exempts exactly one demand. An independent policy defect must
    # still block, or the exemption has become a blanket admission (#1162
    # review F2).
    if policy_error and decision_admitted:
        violations.append(
            "an independent policy error was swallowed by the bypass"
        )
    if policy_error and cache_admitted:
        violations.append(
            "cache admitted a row carrying an independent policy error"
        )

    if policy_error:
        return violations

    if stale and bypassed and not decision_admitted:
        violations.append(
            "a re-measurement cannot re-grade CD-rip-proven bytes, so this "
            "block is unsatisfiable and requeues forever"
        )
    if stale and bypassed and _GENERATION_GATE_REASON in decision_reason:
        violations.append(
            "CD-rip-proven bytes were blocked by the generation gate"
        )
    if stale and bypassed and not cache_admitted:
        violations.append(
            "cache admission re-measures CD-rip-proven bytes every pass"
        )
    if stale and not bypassed and decision_admitted:
        violations.append(
            "a re-gradable stale spectral tuple reached the decider"
        )
    if not stale and not decision_admitted:
        violations.append("generation-current evidence was blocked")
    return violations


class TestCandidateAdmissionProgressGenerated(unittest.TestCase):
    # The exact live world: a pre-stamp grade preserved on CD-rip-proven
    # bytes (evidence 29228, request 712).
    @example(
        generation="null",
        proof_shape="measured",
        spectral_fact="grade",
        policy_error=False,
    )
    # A carried proof does not witness the producer's own bypass.
    @example(
        generation="old",
        proof_shape="carried",
        spectral_fact="grade",
        policy_error=False,
    )
    # Nor does a measured verified-lossless proof with a spectral classifier.
    @example(
        generation="old",
        proof_shape="spectral_vl",
        spectral_fact="grade",
        policy_error=False,
    )
    # The bypass must not swallow an unrelated policy defect.
    @example(
        generation="null",
        proof_shape="measured",
        spectral_fact="grade",
        policy_error=True,
    )
    # A grade carrying its paired bitrate is stale on the same terms.
    @example(
        generation="old",
        proof_shape="none",
        spectral_fact="both",
        policy_error=False,
    )
    @given(
        generation=st.sampled_from(("null", "old", "current")),
        proof_shape=st.sampled_from(
            ("none", "measured", "carried", "spectral_vl")
        ),
        spectral_fact=st.sampled_from(("none", "grade", "both")),
        policy_error=st.booleans(),
    )
    def test_admission_never_blocks_on_an_unsatisfiable_fact(
        self,
        generation: Generation,
        proof_shape: ProofShape,
        spectral_fact: SpectralFact,
        policy_error: bool,
    ) -> None:
        db = FakePipelineDB()
        db.seed_request(
            make_request_row(id=42, mb_release_id="admission-release")
        )
        download_log_id = db.log_download(request_id=42, outcome="rejected")

        with tempfile.TemporaryDirectory() as root:
            source = Path(root, "candidate")
            source.mkdir()
            for track in ("01 - Intro.flac", "02 - Drunk Driving.flac"):
                Path(source, track).write_bytes(b"fLaC audio")
            files = snapshot_audio_files(str(source))

            cd_rip = (
                cd_rip_proof(provenance="measured")
                if proof_shape == "measured"
                else cd_rip_proof(provenance="carried")
                if proof_shape == "carried"
                else None
            )
            if cd_rip is not None:
                verified_lossless = cd_rip.verified_lossless_proof()
            elif proof_shape == "spectral_vl":
                verified_lossless = spectral_verified_lossless_proof()
            else:
                verified_lossless = None

            has_fact = spectral_fact != "none"
            evidence = make_album_quality_evidence(
                preserve_spectral_measurement_version=True,
                mb_release_id="admission-release",
                # A blank source_path is the download_log 37206 shape: a real
                # policy defect that is independent of the spectral gate.
                source_path="" if policy_error else str(source),
                files=files,
                codec="flac",
                container="flac",
                storage_format="FLAC",
                cd_rip_verification=cd_rip,
                verified_lossless_proof=verified_lossless,
                measurement=AudioQualityMeasurement(
                    min_bitrate_kbps=380,
                    avg_bitrate_kbps=460,
                    median_bitrate_kbps=451,
                    format="FLAC",
                    is_cbr=False,
                    spectral_grade=(
                        "likely_transcode"
                        if spectral_fact in ("grade", "both")
                        else None
                    ),
                    spectral_bitrate_kbps=(
                        900 if spectral_fact == "both" else None
                    ),
                    spectral_subject="source" if has_fact else None,
                    spectral_provenance="measured" if has_fact else None,
                    spectral_measurement_version=(
                        _generation_value(generation) if has_fact else None
                    ),
                ),
            )
            db.upsert_album_quality_evidence(evidence)
            stored = db.find_album_quality_evidence(
                mb_release_id=evidence.mb_release_id,
                snapshot_fingerprint=evidence.snapshot_fingerprint,
            )
            assert stored is not None and stored.id is not None
            db.set_download_log_candidate_evidence(download_log_id, stored.id)

            decision = load_candidate_evidence_for_decision(
                db,
                source_path=str(source),
                download_log_id=download_log_id,
            )
            cache = load_candidate_evidence_for_source(
                db,
                source_path=str(source),
                download_log_id=download_log_id,
            )

            violations = admission_progress_violations(
                generation=generation,
                proof_shape=proof_shape,
                spectral_fact=spectral_fact,
                policy_error=policy_error,
                decision_admitted=decision.evidence is not None,
                decision_reason=decision.reason or "",
                cache_admitted=cache.evidence is not None,
            )
            self.assertEqual(violations, [], "; ".join(violations))


class TestAdmissionProgressCheckerTripsOnViolations(unittest.TestCase):
    """Per-clause known-bad proof: every clause can actually fail."""

    _BASE: ClassVar[dict[str, object]] = {
        "generation": "null",
        "proof_shape": "measured",
        "spectral_fact": "grade",
        "policy_error": False,
        "decision_admitted": True,
        "decision_reason": "",
        "cache_admitted": True,
    }

    def test_swallowed_policy_error_clause_trips(self) -> None:
        violations = admission_progress_violations(
            **{**self._BASE, "policy_error": True, "cache_admitted": False}
        )
        self.assertIn(
            "independent policy error was swallowed", "; ".join(violations)
        )

    def test_cache_swallowed_policy_error_clause_trips(self) -> None:
        violations = admission_progress_violations(
            **{
                **self._BASE,
                "policy_error": True,
                "decision_admitted": False,
            }
        )
        self.assertIn(
            "cache admitted a row carrying", "; ".join(violations)
        )

    def test_unsatisfiable_block_clause_trips(self) -> None:
        violations = admission_progress_violations(
            **{**self._BASE, "decision_admitted": False}
        )
        self.assertIn("unsatisfiable", "; ".join(violations))

    def test_generation_gate_reason_clause_trips(self) -> None:
        violations = admission_progress_violations(
            **{**self._BASE, "decision_reason": _GENERATION_GATE_REASON}
        )
        self.assertIn(
            "blocked by the generation gate", "; ".join(violations)
        )

    def test_cache_remeasure_clause_trips(self) -> None:
        violations = admission_progress_violations(
            **{**self._BASE, "cache_admitted": False}
        )
        self.assertIn("every pass", "; ".join(violations))

    def test_regradable_stale_admission_clause_trips(self) -> None:
        violations = admission_progress_violations(
            **{**self._BASE, "proof_shape": "none"}
        )
        self.assertIn("reached the decider", "; ".join(violations))

    def test_spectral_vl_proof_does_not_count_as_bypassed(self) -> None:
        """A measured non-CD-rip proof must not license the exemption."""
        violations = admission_progress_violations(
            **{**self._BASE, "proof_shape": "spectral_vl"}
        )
        self.assertIn("reached the decider", "; ".join(violations))

    def test_current_generation_block_clause_trips(self) -> None:
        violations = admission_progress_violations(
            **{
                **self._BASE,
                "generation": "current",
                "proof_shape": "none",
                "decision_admitted": False,
            }
        )
        self.assertIn(
            "generation-current evidence was blocked", "; ".join(violations)
        )


if __name__ == "__main__":
    unittest.main()
