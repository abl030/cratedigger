"""Pure import/quality decision functions and their I/O Structs.

Extracted verbatim from the monolithic ``lib/quality.py`` (issue #477).
Pure move: every definition is AST-identical to the original.
"""

from typing import Literal, Optional
import msgspec

from lib.quality.evidence_types import (
    AudioQualityMeasurement,
    SPECTRAL_TRANSCODE_GRADES,
    V0ProbeEvidence,
    is_comparable_lossless_source_probe,
)
from lib.quality.ranks import QualityRank, QualityRankConfig, gate_rank
from lib.quality.compare import compare_quality


DECISION_PROVISIONAL_LOSSLESS_UPGRADE = "provisional_lossless_upgrade"
DECISION_SUSPECT_LOSSLESS_DOWNGRADE = "suspect_lossless_downgrade"
DECISION_SUSPECT_LOSSLESS_PROBE_MISSING = "suspect_lossless_probe_missing"
DECISION_LOSSLESS_SOURCE_LOCKED = "lossless_source_locked"


QUALITY_MIN_BITRATE_KBPS = 210  # V0 floor — below this triggers upgrade
TRANSCODE_MIN_BITRATE_KBPS = 210  # V0 from genuine lossless is always >= this


# ---------------------------------------------------------------------------
# MeasurementFailure — U4 wire-boundary type for preview measurement failures
# ---------------------------------------------------------------------------

MeasurementFailureReason = Literal[
    "snapshot_stale",          # source folder changed after retry (AE5)
    "source_vanished",         # ENOENT mid-measure (AE6); also covers
                               # path-missing pre-claim and force-import
                               # failed_path-no-longer-on-disk cases
    "materialization_error",   # tempdir copy / shutil failure during measure
    "measurement_crashed",     # ffmpeg / sox / mutagen blew up
    "evidence_persist_failed", # DB write failed after measurement completed
    "request_not_found",       # parent album_request gone (no-finalize subcase)
    "missing_release_id",      # request has no mb_release_id
    "download_log_not_found",  # force/manual UI: download_log row gone
    "missing_failed_path",     # force/manual UI: download_log lacks failed_path
]


class MeasurementFailure(msgspec.Struct, frozen=True):
    """Typed wire-boundary payload for preview-side measurement failures.

    Carried through ``import_jobs.preview_result`` (JSONB) and
    ``download_log.validation_result`` (JSONB). The Recents UI grep-classifies
    on ``reason`` to render the appropriate badge.

    ``reason`` is a coarse ``Literal`` tag drawn from the
    ``MeasurementFailureReason`` taxonomy — callers can switch on it
    without parsing free text. ``detail`` is a short human-readable string
    for logs and the audit trail; do not parse it. ``source_path`` is the
    folder/file the measurement attempted, when known (``""`` when the
    failure happened before any path was resolved — e.g. ``request_not_found``).

    Wire-boundary type per ``.claude/rules/code-quality.md`` § "Wire-boundary
    types" — encode via ``msgspec.json.encode`` / ``msgspec.to_builtins``,
    decode via ``msgspec.convert``. Strict validation at the boundary catches
    drift between the Struct's declared taxonomy and what the producer wrote.
    Mirrors the precedent set by ``lib.beets_album_op.BeetsOpFailure``.
    """
    reason: MeasurementFailureReason
    detail: str
    source_path: str = ""


# ---------------------------------------------------------------------------
# U11: ``preimport_decide`` and ``PreimportDecision`` have been folded into
# ``full_pipeline_decision_from_evidence``. The four folder/audio-integrity
# facts (``audio_corrupt``, ``bad_audio_hash``, ``nested_layout``,
# ``empty_fileset``) are now early-exit reject branches at the top of that
# function. There is exactly one decision function for the importer.
#
# See CLAUDE.md § "Quality decisions live in ONE place" and the U11 entry in
# ``docs/plans/2026-05-16-002-refactor-evidence-canonical-cleanup-plan.md``.
# ---------------------------------------------------------------------------


def spectral_import_decision(spectral_grade, spectral_bitrate, existing_spectral_bitrate):
    """Decide whether to import a download based on spectral analysis.

    Pure comparison of spectral evidence against spectral evidence. Container
    bitrate is intentionally NOT consulted — that violates evidence-set parity
    (absence of an existing spectral measurement is not evidence the existing
    file is genuine, only that we haven't measured it yet).

    Returns one of:
        "import"          — spectral says genuine/marginal, proceed
        "import_upgrade"  — spectral says suspect but better than existing
        "import_no_exist" — spectral says suspect but nothing on disk yet
        "reject"          — spectral says suspect and not better than existing

    Inputs:
        spectral_grade:             "genuine" | "marginal" | "suspect" | "likely_transcode"
        spectral_bitrate:           estimated bitrate from cliff detection (kbps), or None
        existing_spectral_bitrate:  spectral estimate of what's already in beets (kbps), or 0/None
    """
    if spectral_grade not in ("suspect", "likely_transcode"):
        return "import"

    new_q = spectral_bitrate or 0
    existing_q = existing_spectral_bitrate or 0

    if new_q and existing_q and new_q <= existing_q:
        return "reject"
    elif new_q and existing_q and new_q > existing_q:
        return "import_upgrade"
    elif not existing_q:
        return "import_no_exist"
    else:
        return "import"


# ---------------------------------------------------------------------------
# import_one.py decisions (FLAC conversion path)
# ---------------------------------------------------------------------------

def import_quality_decision(
    new: AudioQualityMeasurement,
    existing: "AudioQualityMeasurement | None",
    is_transcode: bool = False,
    cfg: "QualityRankConfig | None" = None,
) -> str:
    """Decide whether to import based on codec-aware quality comparison (issue #60).

    Called in import_one.py after FLAC→V0 conversion (if applicable)
    and before running the beets harness.

    Uses compare_quality() which classifies both measurements into
    QualityRank bands (via quality_rank/measurement_rank), so cross-codec
    comparisons (Opus 128 vs MP3 V0) are correctly treated as equivalent.

    The verified_lossless bypass is now a tier-gated preference:
    ``verified_lossless=True`` still forces an import when the verdict is
    "better" or "equivalent", but NOT when it would be a downgrade — this
    blocks a deliberately too-low ``verified_lossless_target`` (e.g. Opus
    64) from replacing a good existing album.

    Returns one of:
        "import"              — new files are better (or no existing), proceed
        "downgrade"           — new files are worse, skip (exit 5)
        "transcode_upgrade"   — transcode but better than existing, import + denylist (exit 6)
        "transcode_downgrade" — transcode and not better, skip + denylist (exit 6)
        "transcode_first"     — transcode but nothing on disk yet, import (exit 6)

    Args:
        new: measurement of the new download
        existing: measurement of what's already in beets, or None
                  (caller resolves override_min_bitrate into existing.min_bitrate_kbps)
        is_transcode: True if FLAC→V0 produced a transcode (from transcode_detection)
        cfg: QualityRankConfig. Defaults to QualityRankConfig.defaults().
    """
    if cfg is None:
        cfg = QualityRankConfig.defaults()

    if existing is None:
        return "transcode_first" if is_transcode else "import"

    verdict = compare_quality(new, existing, cfg)

    # verified_lossless is a soft preference: "better" or "equivalent" still
    # import, but "worse" is blocked regardless of verified_lossless status.
    # This prevents a deliberately too-low verified-lossless target from
    # blindly replacing a good existing album (issue #60 acceptance criterion).
    if new.verified_lossless and verdict in ("better", "equivalent"):
        return "transcode_upgrade" if is_transcode else "import"

    if verdict == "better":
        return "transcode_upgrade" if is_transcode else "import"

    # "worse" or "equivalent" without verified_lossless bypass → reject.
    return "transcode_downgrade" if is_transcode else "downgrade"


class MeasuredImportDecisionInput(msgspec.Struct, frozen=True):
    """Pure input for the measured import decision reducer.

    This is the common shape shared by the typed simulator, import preview,
    and the real import harness once files have been measured. It deliberately
    contains no filesystem, database, or subprocess concerns.
    """
    new_measurement: AudioQualityMeasurement
    existing_measurement: Optional[AudioQualityMeasurement] = None
    is_transcode: bool = False


class MeasuredImportDecisionResult(msgspec.Struct, frozen=True):
    """Pure measured-decision result with preview-friendly classification."""
    decision: str
    exit_code: int = 0
    would_import: bool = False
    confident_reject: bool = False
    uncertain: bool = False
    cleanup_eligible: bool = False
    stage_chain: list[str] = []
    reason: Optional[str] = None


class ProvisionalLosslessDecisionInput(msgspec.Struct, frozen=True):
    """Pure input for suspect lossless-source provisional grind-up."""

    candidate_probe: Optional[V0ProbeEvidence] = None
    existing_probe: Optional[V0ProbeEvidence] = None
    spectral_grade: Optional[str] = None
    supported_lossless_source: bool = False


class ProvisionalLosslessDecisionResult(msgspec.Struct, frozen=True):
    """Decision result for the suspect lossless-source lane."""

    decision: Optional[str] = None
    would_import: bool = False
    confident_reject: bool = False
    cleanup_eligible: bool = False
    reason: Optional[str] = None
    stage_chain: list[str] = []


def provisional_lossless_decision(
    candidate: ProvisionalLosslessDecisionInput,
    *,
    cfg: "QualityRankConfig | None" = None,
) -> ProvisionalLosslessDecisionResult:
    """Compare suspect lossless-source V0 probes inside the provisional lane.

    Returns ``decision=None`` for candidates that should continue through the
    existing import policy (native lossy, clean lossless, or unsupported
    sources). For suspect supported lossless sources, V0 probe avg bitrate is
    the v1 comparison signal and ``within_rank_tolerance_kbps`` is the only
    tolerance knob.

    When ``supported_lossless_source`` is False (lossy candidate) AND
    ``existing_probe`` is a comparable lossless-source probe, the function
    returns ``DECISION_LOSSLESS_SOURCE_LOCKED`` — a lossy candidate cannot
    produce a comparable measurement, and the recorded probe is the truth-
    of-source anchor. ``candidate_probe`` is ignored in that branch.
    """
    if cfg is None:
        cfg = QualityRankConfig.defaults()

    if not candidate.supported_lossless_source:
        # Lossless-source lock: when the existing album was previously
        # imported as a provisional lossless source we transcoded down (so
        # current_lossless_source_v0_probe_avg_bitrate is the only V0-grade
        # signal we have), a lossy candidate cannot produce comparable
        # evidence and must not be allowed to override on raw avg alone.
        # The recorded V0 probe is the truth-of-source anchor; only another
        # lossless-container candidate (which can be ground to V0) is
        # eligible to displace it.
        if is_comparable_lossless_source_probe(candidate.existing_probe):
            assert candidate.existing_probe is not None
            existing_avg = candidate.existing_probe.avg_bitrate_kbps
            decision = DECISION_LOSSLESS_SOURCE_LOCKED
            return ProvisionalLosslessDecisionResult(
                decision=decision,
                would_import=False,
                confident_reject=True,
                cleanup_eligible=True,
                reason=(
                    f"existing has lossless-source V0 probe "
                    f"{existing_avg}kbps; lossy candidate cannot produce "
                    f"comparable evidence (only another lossless source "
                    f"can override)"
                ),
                stage_chain=[f"stage2_provisional:{decision}"],
            )
        return ProvisionalLosslessDecisionResult()

    if candidate.spectral_grade not in SPECTRAL_TRANSCODE_GRADES:
        return ProvisionalLosslessDecisionResult()

    if not is_comparable_lossless_source_probe(candidate.candidate_probe):
        decision = DECISION_SUSPECT_LOSSLESS_PROBE_MISSING
        return ProvisionalLosslessDecisionResult(
            decision=decision,
            would_import=False,
            confident_reject=True,
            cleanup_eligible=True,
            reason="suspect lossless source lacks a comparable V0 probe",
            stage_chain=[f"stage2_provisional:{decision}"],
        )

    candidate_probe = candidate.candidate_probe
    assert candidate_probe is not None
    candidate_avg = candidate_probe.avg_bitrate_kbps
    assert candidate_avg is not None
    existing_probe = (
        candidate.existing_probe
        if is_comparable_lossless_source_probe(candidate.existing_probe)
        else None
    )

    if existing_probe is None:
        decision = DECISION_PROVISIONAL_LOSSLESS_UPGRADE
        return ProvisionalLosslessDecisionResult(
            decision=decision,
            would_import=True,
            reason="no existing comparable lossless-source V0 probe",
            stage_chain=[f"stage2_provisional:{decision}"],
        )

    existing_avg = existing_probe.avg_bitrate_kbps
    assert existing_avg is not None
    delta = candidate_avg - existing_avg
    if delta <= cfg.within_rank_tolerance_kbps:
        decision = DECISION_SUSPECT_LOSSLESS_DOWNGRADE
        return ProvisionalLosslessDecisionResult(
            decision=decision,
            would_import=False,
            confident_reject=True,
            cleanup_eligible=True,
            reason=(
                f"candidate V0 probe avg {candidate_avg}kbps is not more than "
                f"{cfg.within_rank_tolerance_kbps}kbps above existing "
                f"{existing_avg}kbps"
            ),
            stage_chain=[f"stage2_provisional:{decision}"],
        )

    decision = DECISION_PROVISIONAL_LOSSLESS_UPGRADE
    return ProvisionalLosslessDecisionResult(
        decision=decision,
        would_import=True,
        reason=(
            f"candidate V0 probe avg {candidate_avg}kbps beats existing "
            f"{existing_avg}kbps by more than "
            f"{cfg.within_rank_tolerance_kbps}kbps"
        ),
        stage_chain=[f"stage2_provisional:{decision}"],
    )


def build_existing_quality_measurement(
    *,
    min_bitrate_kbps: int | None,
    avg_bitrate_kbps: int | None = None,
    median_bitrate_kbps: int | None = None,
    format: str | None = None,
    is_cbr: bool = False,
    override_min_bitrate: int | None = None,
    spectral_grade: str | None = None,
    spectral_bitrate_kbps: int | None = None,
) -> AudioQualityMeasurement | None:
    """Build an existing-album measurement from primitive quality facts.

    The spectral override clamps avg/median only for CBR albums. VBR existing
    albums keep their real avg/median so a stale spectral floor cannot erase
    the genuine rank signal that compare_quality() should use.
    """
    if min_bitrate_kbps is None:
        return None

    effective_min = (
        override_min_bitrate
        if override_min_bitrate is not None
        else min_bitrate_kbps
    )
    raw_avg = avg_bitrate_kbps if avg_bitrate_kbps is not None else min_bitrate_kbps
    raw_median = (
        median_bitrate_kbps
        if median_bitrate_kbps is not None
        else raw_avg
    )
    if is_cbr and override_min_bitrate is not None:
        effective_avg = override_min_bitrate
        effective_median = override_min_bitrate
    else:
        effective_avg = raw_avg
        effective_median = raw_median

    return AudioQualityMeasurement(
        min_bitrate_kbps=effective_min,
        avg_bitrate_kbps=effective_avg,
        median_bitrate_kbps=effective_median,
        format=format,
        is_cbr=is_cbr,
        spectral_grade=spectral_grade,
        spectral_bitrate_kbps=spectral_bitrate_kbps,
    )


def measured_import_decision(
    measured: MeasuredImportDecisionInput,
    *,
    cfg: "QualityRankConfig | None" = None,
) -> MeasuredImportDecisionResult:
    """Reduce measured import facts to a decision and preview classification."""
    decision = import_quality_decision(
        measured.new_measurement,
        measured.existing_measurement,
        measured.is_transcode,
        cfg=cfg,
    )
    exit_code = 0
    if decision == "downgrade":
        exit_code = 5
    elif decision == "transcode_downgrade":
        exit_code = 6

    would_import = decision in {
        "import",
        "transcode_upgrade",
        "transcode_first",
        DECISION_PROVISIONAL_LOSSLESS_UPGRADE,
    }
    confident_reject = decision in {
        "downgrade",
        "transcode_downgrade",
        DECISION_SUSPECT_LOSSLESS_DOWNGRADE,
        DECISION_SUSPECT_LOSSLESS_PROBE_MISSING,
    }
    reason = decision
    if measured.existing_measurement is None:
        reason = f"{decision}: no existing album"
    elif confident_reject:
        reason = (
            f"{decision}: measured candidate is not an upgrade over existing"
        )

    return MeasuredImportDecisionResult(
        decision=decision,
        exit_code=exit_code,
        would_import=would_import,
        confident_reject=confident_reject,
        uncertain=False,
        cleanup_eligible=confident_reject,
        stage_chain=[f"stage2_import:{decision}"],
        reason=reason,
    )


def transcode_detection(converted_count, post_conversion_min_bitrate,
                        spectral_grade=None,
                        cfg: "QualityRankConfig | None" = None):
    """Detect whether a FLAC→V0 conversion produced a transcode.

    Called in import_one.py after convert_flac_to_v0().

    Returns True if the converted files are likely transcodes
    (MP3 wrapped in FLAC container).

    Inputs:
        converted_count:             number of FLAC files converted
        post_conversion_min_bitrate: min bitrate after conversion (kbps), or None
        spectral_grade:              album spectral grade, or None if unavailable
        cfg:                         QualityRankConfig — the spectral-fallback
                                     threshold is taken from
                                     ``cfg.mp3_vbr.excellent``. When omitted,
                                     falls back to the legacy
                                     ``TRANSCODE_MIN_BITRATE_KBPS`` constant
                                     (210) so existing callers stay
                                     bit-for-bit compatible. Issue #66.
    """
    if converted_count == 0:
        return False
    if post_conversion_min_bitrate is None:
        return False
    # When spectral data is available, it's authoritative.
    # 'error' is inconclusive (decoder failed on every track — usually a
    # missing codec or hostile input). Fail closed: treat as transcode so
    # determine_verified_lossless can't pass it without V0 corroboration.
    if spectral_grade is not None:
        # Cliff detected, suspect, or analysis failed = treat as transcode
        if spectral_grade in ("suspect", "likely_transcode", "error"):
            return True
        # No cliff = not a transcode (lo-fi lossless produces low V0 bitrates)
        return False
    # No spectral data — fall back to bitrate threshold. Derived from cfg
    # so the threshold tracks gate retuning automatically: an operator who
    # lowers mp3_vbr.excellent to accept lower-quality V0 also implicitly
    # lowers what counts as "credible V0" for the spectral fallback.
    threshold = (cfg.mp3_vbr.excellent if cfg is not None
                 else TRANSCODE_MIN_BITRATE_KBPS)
    return post_conversion_min_bitrate < threshold


# ---------------------------------------------------------------------------
# Verified lossless derivation (post-import, used by album_source.py)
# ---------------------------------------------------------------------------

_LOSSLESS_EXTS = {"flac", "m4a", "wav", "alac"}


# V0-avg trust override thresholds. A lossless_source_v0 probe with avg
# AND min at-or-above these levels is strong evidence the source carried
# enough HF complexity that LAME couldn't strip it — i.e. a real lossless
# master, not a fake-FLAC of a lossy intermediate. Below either bar we
# defer to spectral as before. Tuned against Bill Hicks 1990 "Dangerous"
# (avg=241/min=219, spoken-word lossless that spectral false-positives as
# suspect because speech has no HF energy for the music-tuned thresholds
# to measure against).
V0_OVERRIDE_AVG_KBPS: int = 230
V0_OVERRIDE_MIN_KBPS: int = 200


def v0_probe_overrides_spectral(probe: "V0ProbeEvidence | None") -> bool:
    """Decide whether a V0 probe is strong enough to override a suspect
    spectral grade and certify the source as genuine lossless.

    Only ``lossless_source_v0`` probes are eligible — research probes
    (``native_lossy_research_v0``, ``on_disk_research_v0``) carry no
    policy weight here.
    """
    if not is_comparable_lossless_source_probe(probe):
        return False
    assert probe is not None  # narrowed by the helper above
    avg = probe.avg_bitrate_kbps
    mn = probe.min_bitrate_kbps
    if avg is None or mn is None:
        return False
    return avg >= V0_OVERRIDE_AVG_KBPS and mn >= V0_OVERRIDE_MIN_KBPS


def determine_verified_lossless(
    target_format: Optional[str],
    spectral_grade: Optional[str],
    converted_count: int,
    is_transcode: bool,
    v0_probe: "V0ProbeEvidence | None" = None,
    *,
    has_lossy_passthrough: bool = False,
) -> bool:
    """Single source of truth for verified lossless status (pure).

    Two paths:
    1. target_format="lossless"/"flac" (lossless kept on disk): verified if
       spectral says genuine or marginal, or if no spectral ran (None).
       The lossless source IS on disk — no conversion needed to prove it.
    2. Default (lossless→V0/target): verified if we actually converted
       lossless files AND spectral didn't flag them as transcodes.

    V0-avg trust override (issue #205-style — Bill Hicks): in either path,
    when spectral disagrees with V0 evidence (suspect/likely_transcode but a
    ``lossless_source_v0`` probe at avg≥230kbps AND min≥200kbps), trust
    the V0 probe and certify as verified. The override is monotonic — it
    only flips False→True, never True→False.

    Mixed-source guard (``has_lossy_passthrough``): when the source folder
    contains lossless audio AND audio that will pass through unconverted
    (e.g. 15 FLAC + 2 MP3 bonus tracks), the album can never be
    verified-lossless regardless of converted_count / spectral / V0. The
    decision layer rejects these sources outright via
    ``preimport_mixed_source`` in ``full_pipeline_decision_from_evidence``;
    this argument is the harness-side defense in depth so the persisted
    candidate measurement field is honest even on the never-imported row.
    """
    if has_lossy_passthrough:
        return False
    if target_format in ("flac", "lossless"):
        if spectral_grade in ("genuine", "marginal", None):
            return True
        return v0_probe_overrides_spectral(v0_probe)
    if converted_count > 0 and not is_transcode:
        return True
    if converted_count > 0 and v0_probe_overrides_spectral(v0_probe):
        return True
    return False


def is_verified_lossless(was_converted: bool, original_filetype: Optional[str],
                         spectral_grade: Optional[str]) -> bool:
    """Legacy derivation for album_source.py fallback path.

    Used when import_one.py didn't set verified_lossless_override
    (old download_log rows). Delegates to determine_verified_lossless
    for the standard (non-FLAC-on-disk) case.

    Stricter than determine_verified_lossless: requires spectral_grade="genuine"
    exactly, and validates the original filetype was lossless.
    """
    if not was_converted or original_filetype is None or spectral_grade != "genuine":
        return False
    return original_filetype.lower() in _LOSSLESS_EXTS


def quality_gate_decision(
    current: AudioQualityMeasurement,
    cfg: "QualityRankConfig | None" = None,
) -> str:
    """Codec-aware post-import quality gate (issue #60).

    Classifies ``current`` via ``gate_rank()`` (which applies the spectral
    clamp) and compares against ``cfg.gate_min_rank``.

    Returns one of: "accept", "requeue_upgrade", "requeue_lossless".

    Args:
        current: measurement of the files now on disk (from beets DB + spectral)
        cfg: QualityRankConfig. Defaults to QualityRankConfig.defaults().
    """
    if cfg is None:
        cfg = QualityRankConfig.defaults()

    rank = gate_rank(current, cfg)

    if rank == QualityRank.UNKNOWN or rank < cfg.gate_min_rank:
        return "requeue_upgrade"
    if (not current.verified_lossless and current.is_cbr
            and rank < QualityRank.LOSSLESS):
        return "requeue_lossless"
    return "accept"
