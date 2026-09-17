"""Native encodes for the #1411 role-reversal regression."""

from lib.quality.evidence_types import AlbumQualityEvidence, CodecFamily
from lib.quality.pipeline import (
    evidence_spectral_context,
    full_pipeline_decision,
    full_pipeline_decision_from_evidence,
    override_bitrate_from_current_evidence,
)
from tests.evidence_helpers import build_parity_candidate_evidence


def native_encode(
    codec: CodecFamily,
    bitrate: int,
    *,
    minimum: int | None = None,
    is_cbr: bool = False,
    cliff_hz: int | None = None,
    spectral_bitrate: int = 128,
) -> AlbumQualityEvidence:
    return build_parity_candidate_evidence(
        is_flac=False, min_bitrate=bitrate if minimum is None else minimum,
        avg_bitrate=bitrate, is_cbr=is_cbr,
        native_codec=codec, native_format=codec.upper(),
        spectral_grade="likely_transcode", spectral_bitrate=spectral_bitrate,
        cliff_hz=cliff_hz, codec_family=codec,
        mb_release_id="e439e21e-c01c-4a02-a911-b91b050ed87f",
    )


def decide_native_pair(candidate: AlbumQualityEvidence, current: AlbumQualityEvidence):
    """Run both public decision inputs and require complete result parity."""
    new = candidate.measurement
    have = current.measurement
    assert new.min_bitrate_kbps is not None
    flat = full_pipeline_decision(
        is_flac=False, min_bitrate=new.min_bitrate_kbps,
        avg_bitrate=new.avg_bitrate_kbps, is_cbr=new.is_cbr,
        spectral_grade=new.spectral_grade, spectral_bitrate=new.spectral_bitrate_kbps,
        new_format=new.format,
        existing_min_bitrate=have.min_bitrate_kbps,
        existing_avg_bitrate=have.avg_bitrate_kbps,
        existing_is_cbr=have.is_cbr, existing_format=have.format,
        existing_spectral_grade=have.spectral_grade,
        existing_spectral_bitrate=have.spectral_bitrate_kbps,
        override_min_bitrate=override_bitrate_from_current_evidence(current),
        candidate_spectral_context=evidence_spectral_context(candidate),
        existing_spectral_context=evidence_spectral_context(current),
    )
    evidence = full_pipeline_decision_from_evidence(candidate, current)
    assert flat == evidence, "native decision twins disagree"
    return evidence


def assert_no_cross_codec_reversal(forward: dict, backward: dict) -> None:
    assert not (forward["imported"] and backward["imported"]), "both replacements import"
    left = forward["comparison_basis"]
    right = backward["comparison_basis"]
    assert (
        left["new_value_kbps"], left["existing_value_kbps"],
        left["new_rank"], left["existing_rank"],
    ) == (
        right["existing_value_kbps"], right["new_value_kbps"],
        right["existing_rank"], right["new_rank"],
    ), "encode quality changes with candidate/current role"
