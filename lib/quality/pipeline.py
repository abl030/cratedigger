"""The decision twins: full_pipeline_decision (flat-kwargs simulator) and
full_pipeline_decision_from_evidence (evidence-pipeline production decider).

PARITY CONTRACT: the twins MUST produce the same outcome on the same
album (pinned by tests/test_quality_classification.py). They stay in one
module on purpose — do not split them apart.

Extracted verbatim from the monolithic ``lib/quality.py`` (issue #477).
Pure move: every definition is AST-identical to the original.
"""

from typing import Any
import msgspec

from lib.quality.evidence_types import (
    AlbumQualityEvidence,
    AlbumQualityV0Metric,
    AudioQualityMeasurement,
    QualityComparisonBasis,
    SPECTRAL_TRANSCODE_GRADES,
    TargetQualityContract,
    V0ProbeEvidence,
    V0_PROBE_LOSSLESS_SOURCE,
    V0_SOURCE_LINEAGE_LOSSLESS_SOURCE,
    _NONCOMPARABLE_NEUTRAL_V0_PROBE_KIND,
)
from lib.quality.ranks import QualityRankConfig
from lib.quality.filetypes import has_mixed_lossless_and_lossy
from lib.quality.compare import comparison_format_hint
from lib.quality.import_result_types import QualityEvidenceActionProvenance
from lib.quality.gates import (
    preimport_audio_gate,
    preimport_nested_gate,
    spectral_gate_trigger,
)
from lib.quality.decisions import (
    DECISION_LOSSLESS_SOURCE_LOCKED,
    MeasuredImportDecisionInput,
    ProvisionalLosslessDecisionInput,
    ProvisionalLosslessDecisionResult,
    _LOSSLESS_EXTS,
    build_existing_quality_measurement,
    determine_verified_lossless,
    measured_import_decision,
    provisional_lossless_decision,
    quality_gate_decision,
    spectral_import_decision,
    transcode_detection,
    v0_probe_overrides_spectral,
)
from lib.quality.dispatch_actions import compute_effective_override_bitrate


# ---------------------------------------------------------------------------
# Full pipeline decision — combines all three stages
# ---------------------------------------------------------------------------

def full_pipeline_decision(
    # File properties
    is_flac,
    min_bitrate,
    is_cbr,
    # VBR + avg bitrate for the preimport spectral gate trigger (issue #93).
    # ``is_vbr`` defaults to ``not is_cbr`` when omitted so legacy callers
    # retain current behavior. ``avg_bitrate`` gates VBR MP3 through spectral
    # when below cfg.mp3_vbr.excellent — genuine V0 (~245kbps avg) skips,
    # fake V0 (~180kbps avg) gets analyzed.
    is_vbr: bool | None = None,
    avg_bitrate: int | None = None,
    # Spectral analysis
    spectral_grade=None,
    spectral_bitrate=None,
    # Existing state
    existing_min_bitrate=None,
    existing_avg_bitrate: int | None = None,
    existing_spectral_bitrate: int | None = None,
    existing_spectral_grade: str | None = None,
    override_min_bitrate=None,
    existing_format: str | None = None,
    existing_is_cbr: bool = False,
    # Post-conversion (FLAC path only)
    post_conversion_min_bitrate=None,
    converted_count=0,
    # Pipeline state
    verified_lossless=False,
    # Verified lossless target format (e.g. "opus 128", "mp3 v2")
    verified_lossless_target=None,
    # Target format (user intent — "flac" skips conversion)
    target_format=None,
    # New download format label (codec-aware, passed through to measurements)
    new_format: str | None = None,
    # Preimport gates (issue #91). Default to a passing audio check + the auto
    # path so legacy simulator calls don't change behavior.
    audio_check_mode: str = "normal",
    audio_corrupt: bool = False,
    import_mode: str = "auto",
    has_nested_audio: bool = False,
    # Rank-model config (defaults() for legacy callers)
    cfg: "QualityRankConfig | None" = None,
    *,
    post_conversion_is_cbr: bool | None = None,
    candidate_v0_probe_avg: int | None = None,
    candidate_v0_probe_min: int | None = None,
    existing_v0_probe_avg: int | None = None,
    existing_v0_probe_kind: str | None = None,
    candidate_v0_probe_kind: str | None = None,
    supported_lossless_source: bool | None = None,
):
    """Run the full decision chain and return the final outcome.

    This simulates what happens when a download completes and flows through
    process_completed_album → import_one.py → _check_quality_gate.

    Codec-aware: when ``new_format`` / ``existing_format`` are supplied, the
    simulator classifies both measurements via quality_rank() — matching
    production dispatch behavior. Legacy callers that omit them still get
    sensible defaults derived from ``is_flac``/``target_format``/``is_cbr``.

    Returns a dict:
        {
            "stage0_spectral_gate": str,  # would spectral analysis run?
            "stage1_spectral": str,       # pre-import spectral decision (None when gate skipped)
            "stage2_import": str,         # import/downgrade/transcode decision
            "stage3_quality_gate": str,   # post-import quality gate decision
            "final_status": str,          # what the pipeline DB ends up as
            "imported": bool,             # whether files were imported to beets
            "denylisted": bool,           # whether source user gets denylisted
            "keep_searching": bool,       # whether the system keeps looking for better
            "comparison_basis": dict | None,  # QualityComparisonBasis builtins from stage 2
        }
    """
    if cfg is None:
        cfg = QualityRankConfig.defaults()
    result: dict[str, Any] = {
        "preimport_audio": None,
        "preimport_nested": None,
        # U11: keys carrying the folded folder/audio-integrity rejects from
        # ``full_pipeline_decision_from_evidence``. The simulator does not
        # take these facts as flat kwargs, so they stay None here — the
        # simulator surfaces ``audio_corrupt`` via the
        # ``audio_check_mode='strict' + audio_corrupt=True`` kwargs which
        # routes through ``preimport_audio``. ``bad_audio_hash`` and
        # ``empty_fileset`` are only reachable through the evidence
        # entrypoint; their presence here keeps both deciders'
        # dict shapes identical.
        "preimport_bad_hash": None,
        "preimport_empty_fileset": None,
        "preimport_mixed_source": None,
        "stage0_spectral_gate": None,
        "stage1_spectral": None,
        "stage2_import": None,
        "stage3_quality_gate": None,
        "final_status": None,
        "imported": False,
        "denylisted": False,
        "keep_searching": False,
        "target_final_format": None,
        "verified_lossless": bool(verified_lossless),
        # The QualityComparisonBasis from measured_import_decision, as plain
        # builtins (msgspec.to_builtins) — this dict rides json.dumps'd API
        # responses and preview JSONB, so it must stay JSON-plain. None when
        # stage 2 never compared against an existing album (early rejects,
        # provisional lane, no existing). Consumers that persist it onto
        # ImportResult convert back with msgspec.convert at their boundary.
        "comparison_basis": None,
    }

    # --- Preimport gates (issue #91) ---
    # Ordering mirrors the live flow: lib.dispatch.dispatch_import_from_db
    # checks inspection.has_nested_audio *before* calling
    # measure_preimport_state, so a force/manual import of a nested corrupt
    # folder is rejected as nested_layout (not audio_corrupt). The nested
    # gate returns "skipped_auto"
    # on the auto path, which is a no-op — the auto pipeline flattens
    # downloads upstream in process_completed_album, so audio integrity is
    # the first real reject.
    #
    # Post-reject state also mirrors the two live paths:
    #   * Auto-import rejects call reject_and_requeue() which transitions
    #     the request back to "wanted" and bumps the validation attempt
    #     counter → final_status="wanted", keep_searching=True.
    #   * Force/manual-import rejects call _record_rejection_and_maybe_requeue
    #     with requeue=False — the request's current status (often "manual"
    #     or "imported") is left untouched → final_status=None (unchanged),
    #     keep_searching=False.
    nested_outcome = preimport_nested_gate(import_mode, has_nested_audio)
    result["preimport_nested"] = nested_outcome
    if nested_outcome == "reject_nested":
        # Force/manual-only reject — status stays whatever it was.
        result["final_status"] = None
        result["keep_searching"] = False
        return result

    audio_outcome = preimport_audio_gate(audio_check_mode, audio_corrupt)
    result["preimport_audio"] = audio_outcome
    if audio_outcome == "reject_corrupt":
        if import_mode == "auto":
            # Auto-path rejects call reject_and_requeue(), which denylists
            # every source username (album_source.py:280). Mirror that side
            # effect in the simulator so the Decisions tab and
            # pipeline-cli quality don't underreport what the live pipeline
            # actually does on an audio_corrupt reject.
            result["final_status"] = "wanted"
            result["keep_searching"] = True
            result["denylisted"] = True
        else:
            # Force/manual: no status transition, no denylist (live helper
            # _record_rejection_and_maybe_requeue leaves denylisting to the
            # caller's action.denylist, which audio_corrupt rejects don't set).
            result["final_status"] = None
            result["keep_searching"] = False
        return result

    # --- Stage 0: Spectral gate trigger (issue #93) ---
    # Mirrors lib.measurement._needs_spectral_check. Tells the operator
    # whether the preimport spectral gate would even run on this file,
    # so a VBR MP3 transcode masquerading as V0 (avg < threshold) is
    # distinguishable from genuine V0 in simulator output.
    gate = spectral_gate_trigger(
        is_flac=bool(is_flac),
        is_cbr=is_cbr,
        is_vbr=is_vbr,
        avg_bitrate_kbps=avg_bitrate,
        vbr_threshold_kbps=cfg.mp3_vbr.excellent,
    )
    result["stage0_spectral_gate"] = gate

    # --- Stage 1: Pre-import spectral (MP3/CBR path) ---
    # For FLACs, spectral runs inside import_one.py instead, but the
    # logic is the same: detect transcodes before importing.
    #
    # Only run stage 1 when the gate would actually execute. For VBR MP3
    # with high avg bitrate, production skips spectral entirely — so even
    # if the caller supplies a spectral_grade, simulating that gate firing
    # would misrepresent production behavior.
    stage0_gates_stage1 = gate == "would_run" or is_flac
    provisional_source_candidate = bool(
        is_flac if supported_lossless_source is None else supported_lossless_source
    )
    has_provisional_probe_input = (
        candidate_v0_probe_avg is not None
        or (
            is_flac
            and target_format not in ("flac", "lossless")
            and post_conversion_min_bitrate is not None
        )
    )
    if spectral_grade and stage0_gates_stage1:
        result["stage1_spectral"] = spectral_import_decision(
            spectral_grade, spectral_bitrate, existing_spectral_bitrate or 0)

        if (result["stage1_spectral"] == "reject"
                and not (provisional_source_candidate
                         and has_provisional_probe_input)):
            result["final_status"] = "wanted"  # stays wanted, denylist user
            result["denylisted"] = True
            result["keep_searching"] = True
            return result

    # --- Stage 2: Import decision ---
    # Existing measurement — carries format if the caller provided one,
    # otherwise defaults to "MP3" so legacy simulator scenarios (which only
    # carry a min_bitrate) still classify against the MP3 VBR/CBR band
    # tables. Production always provides a real format via BeetsDB.
    #
    # Supplying existing_avg_bitrate matters under the default
    # cfg.bitrate_metric=AVG policy — otherwise a VBR album with avg=245 but
    # min=180 gets ranked off min=180 (GOOD instead of TRANSPARENT) and
    # downstream comparisons misrepresent production. When the caller didn't
    # measure an avg, nothing is fabricated: metric selection falls back to
    # min and the persisted basis says "min" (dl 36660 display-lie class).
    effective_existing_format = existing_format if existing_format is not None else "MP3"
    existing_m = build_existing_quality_measurement(
        min_bitrate_kbps=existing_min_bitrate,
        avg_bitrate_kbps=existing_avg_bitrate,
        format=effective_existing_format,
        is_cbr=existing_is_cbr,
        override_min_bitrate=override_min_bitrate,
        spectral_grade=existing_spectral_grade,
        spectral_bitrate_kbps=existing_spectral_bitrate,
    )

    if is_flac and target_format in ("flac", "lossless"):
        # FLAC kept on disk (no conversion).
        stage2_new_format = new_format or "flac"
        result["target_final_format"] = stage2_new_format
        candidate_probe_min = candidate_v0_probe_min
        candidate_probe_full = V0ProbeEvidence(
            kind=candidate_v0_probe_kind or V0_PROBE_LOSSLESS_SOURCE,
            avg_bitrate_kbps=candidate_v0_probe_avg,
            min_bitrate_kbps=candidate_probe_min,
        ) if candidate_v0_probe_avg is not None else None
        will_be_verified = determine_verified_lossless(
            target_format, spectral_grade,
            converted_count=0, is_transcode=False,
            v0_probe=candidate_probe_full)
        v0_verified_override = (
            spectral_grade in SPECTRAL_TRANSCODE_GRADES
            and v0_probe_overrides_spectral(candidate_probe_full)
        )
        # avg/median stay None — only the min crosses this interface. A
        # fabricated avg=min makes _selected_bitrate_with_source label a min
        # value "avg" in the persisted basis (dl 36660: "avg 216k" beside an
        # honest "V0 255kbps avg" on the same card). None falls back to the
        # min with the honest "min" label; the classified value is identical.
        new_m = AudioQualityMeasurement(
            min_bitrate_kbps=min_bitrate,
            format=stage2_new_format,
            spectral_grade=spectral_grade,
            spectral_bitrate_kbps=spectral_bitrate)
        if v0_verified_override:
            provisional = ProvisionalLosslessDecisionResult()
        else:
            provisional = provisional_lossless_decision(
                ProvisionalLosslessDecisionInput(
                    candidate_probe=V0ProbeEvidence(
                        kind=candidate_v0_probe_kind or V0_PROBE_LOSSLESS_SOURCE,
                        avg_bitrate_kbps=candidate_v0_probe_avg,
                        min_bitrate_kbps=candidate_probe_min,
                    ) if candidate_v0_probe_avg is not None else None,
                    existing_probe=V0ProbeEvidence(
                        kind=existing_v0_probe_kind or V0_PROBE_LOSSLESS_SOURCE,
                        avg_bitrate_kbps=existing_v0_probe_avg,
                    ) if existing_v0_probe_avg is not None else None,
                    spectral_grade=spectral_grade,
                    supported_lossless_source=provisional_source_candidate,
                ),
                cfg=cfg,
            )
        if provisional.decision is not None:
            result["stage2_import"] = provisional.decision
            if provisional.confident_reject:
                result["final_status"] = "wanted"
                result["denylisted"] = True
                result["keep_searching"] = True
                return result
            result["imported"] = True
            result["denylisted"] = True
            result["keep_searching"] = True
            result["final_status"] = "wanted"
            result["target_final_format"] = stage2_new_format
            return result
        measured = measured_import_decision(
            MeasuredImportDecisionInput(new_m, existing_m), cfg=cfg)
        result["stage2_import"] = measured.decision
        result["comparison_basis"] = (
            msgspec.to_builtins(measured.comparison_basis)
            if measured.comparison_basis is not None else None)

        if result["stage2_import"] == "downgrade":
            result["final_status"] = "imported"
            result["keep_searching"] = True
            return result
        result["imported"] = True

        # Genuine FLAC on disk is verified lossless (for quality gate). Route
        # through determine_verified_lossless so the V0-avg trust override is
        # consulted and the simulator stays in lockstep with import_one.py.
        if will_be_verified:
            verified_lossless = True
            result["verified_lossless"] = True

        gate_bitrate = min_bitrate
        gate_avg_bitrate = min_bitrate  # FLAC: lossless, avg == min is fine
        gate_cbr = False
        gate_format = stage2_new_format  # "flac"
        gate_contract = None
    elif is_flac:
        # FLAC path: convert first, then decide
        is_transcode = transcode_detection(
            converted_count, post_conversion_min_bitrate,
            spectral_grade=spectral_grade, cfg=cfg)
        candidate_probe_min = (
            candidate_v0_probe_min
            if candidate_v0_probe_min is not None
            else post_conversion_min_bitrate
        )
        candidate_probe_full = V0ProbeEvidence(
            kind=candidate_v0_probe_kind or V0_PROBE_LOSSLESS_SOURCE,
            avg_bitrate_kbps=candidate_v0_probe_avg,
            min_bitrate_kbps=candidate_probe_min,
        ) if (
            candidate_v0_probe_avg is not None
            or candidate_probe_min is not None
        ) else None
        will_be_verified = determine_verified_lossless(
            target_format, spectral_grade,
            converted_count=converted_count,
            is_transcode=is_transcode,
            v0_probe=candidate_probe_full)
        v0_verified_override = (
            is_transcode and v0_probe_overrides_spectral(candidate_probe_full)
        )
        policy_is_transcode = is_transcode and not v0_verified_override
        stage2_new_format = comparison_format_hint(
            explicit_format=new_format,
            verified_lossless_target=(
                verified_lossless_target if will_be_verified else None),
            converted_count=converted_count,
            is_transcode=policy_is_transcode,
        )
        # avg/median stay None — the flat interface carries only the
        # post-conversion MIN for this side. See the flac-keep site above:
        # a fabricated avg=min is how the persisted basis learned to call a
        # min value "avg" (dl 36660).
        new_m = AudioQualityMeasurement(
            min_bitrate_kbps=min_bitrate,
            format=new_format or "flac",
            verified_lossless=will_be_verified,
            spectral_grade=spectral_grade,
            spectral_bitrate_kbps=spectral_bitrate)
        target_contract = (
            TargetQualityContract.from_format(
                stage2_new_format,
                projected_is_cbr=post_conversion_is_cbr,
            )
            if stage2_new_format is not None
            else None
        )
        # The audit target names only an output policy that would actually be
        # materialized. The temporary V0 comparison proxy and a rejected
        # transcode are not final targets.
        result["target_final_format"] = (
            verified_lossless_target
            if will_be_verified and verified_lossless_target
            else None
        )
        provisional_probe_avg = (
            candidate_v0_probe_avg
            if candidate_v0_probe_avg is not None
            else post_conversion_min_bitrate
        )
        if v0_verified_override:
            provisional = ProvisionalLosslessDecisionResult()
        else:
            provisional = provisional_lossless_decision(
                ProvisionalLosslessDecisionInput(
                    candidate_probe=V0ProbeEvidence(
                        kind=candidate_v0_probe_kind or V0_PROBE_LOSSLESS_SOURCE,
                        avg_bitrate_kbps=provisional_probe_avg,
                        min_bitrate_kbps=candidate_probe_min,
                    ) if provisional_probe_avg is not None else None,
                    existing_probe=V0ProbeEvidence(
                        kind=existing_v0_probe_kind or V0_PROBE_LOSSLESS_SOURCE,
                        avg_bitrate_kbps=existing_v0_probe_avg,
                    ) if existing_v0_probe_avg is not None else None,
                    spectral_grade=spectral_grade,
                    supported_lossless_source=provisional_source_candidate,
                ),
                cfg=cfg,
            )
        if provisional.decision is not None:
            result["stage2_import"] = provisional.decision
            if provisional.confident_reject:
                result["final_status"] = "wanted"
                result["denylisted"] = True
                result["keep_searching"] = True
                return result
            result["imported"] = True
            result["denylisted"] = True
            result["keep_searching"] = True
            result["final_status"] = "wanted"
            if verified_lossless_target:
                result["target_final_format"] = verified_lossless_target
            return result
        measured = measured_import_decision(
            MeasuredImportDecisionInput(
                new_m,
                existing_m,
                policy_is_transcode,
                target_contract,
                (
                    V0ProbeEvidence(
                        kind=(
                            candidate_v0_probe_kind
                            or V0_PROBE_LOSSLESS_SOURCE
                        ),
                        min_bitrate_kbps=candidate_probe_min,
                    )
                    if converted_count > 0
                    or post_conversion_min_bitrate is not None
                    else None
                ),
            ),
            cfg=cfg,
        )
        result["stage2_import"] = measured.decision
        result["comparison_basis"] = (
            msgspec.to_builtins(measured.comparison_basis)
            if measured.comparison_basis is not None else None)

        if result["stage2_import"] == "downgrade":
            result["final_status"] = "imported"  # keeps existing
            result["keep_searching"] = True
            return result
        elif result["stage2_import"] == "transcode_downgrade":
            result["final_status"] = "wanted"
            result["denylisted"] = True
            result["keep_searching"] = True
            return result
        elif result["stage2_import"] in ("transcode_upgrade", "transcode_first"):
            result["imported"] = True
            result["denylisted"] = True
            result["keep_searching"] = True
            # Still runs quality gate after import
        else:
            result["imported"] = True

        # Genuine FLAC→V0 sets verified_lossless. Routed through
        # determine_verified_lossless so the V0-avg trust override (Bill
        # Hicks shape — spectral=suspect on spoken-word with high V0
        # evidence) flips False→True consistently with import_one.py.
        if will_be_verified:
            verified_lossless = True
            result["verified_lossless"] = True

        # Target format conversion: if verified lossless + target configured,
        # use the target label for the quality gate (e.g. "opus 128") so the
        # rank model classifies against the actual on-disk contract.
        if verified_lossless and verified_lossless_target:
            result["target_final_format"] = verified_lossless_target
            gate_format = verified_lossless_target
        else:
            gate_format = stage2_new_format
        gate_contract = (
            TargetQualityContract.from_format(
                gate_format,
                projected_is_cbr=post_conversion_is_cbr,
            )
            if gate_format is not None
            else None
        )

        # Use post-conversion bitrate for quality gate. The simulator
        # doesn't take a separate post-conversion avg, so avg == min here;
        # in production the real avg comes from beets after import.
        gate_bitrate = post_conversion_min_bitrate or min_bitrate
        gate_avg_bitrate = gate_bitrate
        gate_cbr = False  # V0 conversion always produces VBR
    else:
        # MP3 path: import directly. No format label for native MP3 downloads
        # unless the caller provided one — the rank model falls back to the
        # bare-codec bitrate classification via `new_format=None`.
        #
        # Use the caller-supplied avg_bitrate when present (falls back to
        # min_bitrate otherwise). Under the default cfg.bitrate_metric=AVG
        # policy a VBR V0 at min=200/avg=245 must rank on avg=245 — otherwise
        # the import/downgrade comparison and the post-import gate both see
        # the wrong tier.
        stage2_new_format = comparison_format_hint(
            explicit_format=new_format,
            native_codec_family="MP3",
        )
        # No fabricated fallbacks: when the caller measured no avg, the
        # basis metric falls back to (and honestly says) "min". Median is
        # not part of this interface at all.
        new_m = AudioQualityMeasurement(
            min_bitrate_kbps=min_bitrate,
            avg_bitrate_kbps=avg_bitrate,
            format=stage2_new_format,
            is_cbr=is_cbr,
            spectral_grade=spectral_grade,
            spectral_bitrate_kbps=spectral_bitrate)
        # Lossless-source lock: a recorded existing lossless-source V0 probe
        # is the truth-of-source anchor. Lossy candidates have no comparable
        # measurement and are rejected before measured_import_decision can
        # be misled by an on-disk avg that is just our own transcode floor.
        lossy_lock = provisional_lossless_decision(
            ProvisionalLosslessDecisionInput(
                candidate_probe=None,
                existing_probe=V0ProbeEvidence(
                    kind=existing_v0_probe_kind or V0_PROBE_LOSSLESS_SOURCE,
                    avg_bitrate_kbps=existing_v0_probe_avg,
                ) if existing_v0_probe_avg is not None else None,
                spectral_grade=spectral_grade,
                supported_lossless_source=False,
            ),
            cfg=cfg,
        )
        if lossy_lock.decision == DECISION_LOSSLESS_SOURCE_LOCKED:
            result["stage2_import"] = lossy_lock.decision
            result["final_status"] = "wanted"
            result["denylisted"] = True
            result["keep_searching"] = True
            return result
        measured = measured_import_decision(
            MeasuredImportDecisionInput(new_m, existing_m), cfg=cfg)
        result["stage2_import"] = measured.decision
        result["comparison_basis"] = (
            msgspec.to_builtins(measured.comparison_basis)
            if measured.comparison_basis is not None else None)

        if result["stage2_import"] == "downgrade":
            result["final_status"] = "imported"  # keeps existing
            result["keep_searching"] = True
            return result

        result["imported"] = True
        gate_bitrate = min_bitrate
        # Real avg only; the gate's metric selection falls back to min when
        # avg is unmeasured (same classified value as the old fabricated
        # fallback — gate_m is internal and never persisted as a basis).
        gate_avg_bitrate = avg_bitrate
        gate_cbr = is_cbr
        gate_format = stage2_new_format
        gate_contract = None

    # --- Stage 3: Post-import quality gate ---
    gate_spectral_bitrate = None
    effective_gate_bitrate = compute_effective_override_bitrate(
        gate_bitrate, spectral_bitrate, spectral_grade)
    if (gate_bitrate is not None
            and effective_gate_bitrate is not None
            and effective_gate_bitrate < gate_bitrate):
        gate_spectral_bitrate = spectral_bitrate
    gate_measurement_format = (
        gate_contract.format.split()[0]
        if gate_contract is not None
        else gate_format
    )
    gate_m = AudioQualityMeasurement(
        min_bitrate_kbps=gate_bitrate,
        avg_bitrate_kbps=gate_avg_bitrate,
        median_bitrate_kbps=gate_avg_bitrate,
        format=gate_measurement_format,
        is_cbr=gate_cbr,
        verified_lossless=verified_lossless,
        spectral_grade=spectral_grade,
        spectral_bitrate_kbps=gate_spectral_bitrate)
    result["stage3_quality_gate"] = quality_gate_decision(
        gate_m, cfg=cfg, target_contract=gate_contract
    )

    if result["stage3_quality_gate"] == "accept":
        result["final_status"] = "imported"
    elif result["stage3_quality_gate"] == "requeue_upgrade":
        result["final_status"] = "wanted"
        result["denylisted"] = True
        result["keep_searching"] = True
    elif result["stage3_quality_gate"] == "requeue_lossless":
        result["final_status"] = "wanted"
        result["keep_searching"] = True

    return result


class AlbumQualityEvidenceDecisionFacts(msgspec.Struct, frozen=True):
    """Action-time facts that are not intrinsic album-quality evidence.

    Force/manual callers use ``import_mode`` for provenance only. Beets
    distance bypass is intentionally outside this quality comparison.
    """

    audio_check_mode: str = "normal"
    audio_corrupt: bool = False
    import_mode: str = "auto"
    has_nested_audio: bool = False
    verified_lossless_target: str | None = None
    target_format: str | None = None
    converted_count: int | None = None
    post_conversion_min_bitrate: int | None = None
    post_conversion_is_cbr: bool | None = None


class QualityEvidenceActionPayload(msgspec.Struct, frozen=True):
    """Action-time payload that authorizes import mutation from evidence.

    This payload is generated for a specific import action. It is not a stored
    preview verdict: the candidate/current evidence and decision reflect the
    action-time reducer inputs and output that allowed mutation.
    """

    candidate: AlbumQualityEvidence
    current: AlbumQualityEvidence | None = None
    decision: dict[str, Any] = msgspec.field(default_factory=dict)
    decision_name: str | None = None
    target_format: str | None = None
    verified_lossless_target: str | None = None
    provenance: QualityEvidenceActionProvenance = msgspec.field(
        default_factory=QualityEvidenceActionProvenance
    )


def evidence_decision_name(
    result: dict[str, object],
    *,
    default: str = "quality_reject",
) -> str:
    """Return the dispatch decision represented by a quality decision dict.

    Recognises the U11 folder/audio-integrity early-exit rejects via
    ``preimport_audio`` / ``preimport_nested`` / ``preimport_bad_hash`` /
    ``preimport_empty_fileset`` dict keys, plus the existing stage-* keys.
    """

    # Folder/audio-integrity rejects fire *before* the quality stages run,
    # so check them first — if a four-fact reject is present, stage2/stage3
    # will be None and falling through to the quality default would lose
    # the specific reason.
    if result.get("preimport_audio") == "reject_corrupt":
        return "audio_corrupt"
    if result.get("preimport_bad_hash") == "reject_bad_hash":
        return "bad_audio_hash"
    if result.get("preimport_nested") == "reject_nested":
        return "nested_layout"
    if result.get("preimport_empty_fileset") == "reject_empty":
        return "empty_fileset"
    if result.get("preimport_mixed_source") == "reject_mixed_source":
        return "mixed_source"
    for key in ("stage2_import", "stage3_quality_gate"):
        value = result.get(key)
        if isinstance(value, str) and value:
            return value
    if (
        result.get("stage1_spectral") == "reject"
        and not result.get("stage2_import")
    ):
        return "spectral_reject"
    return default


def comparison_basis_from_decision(
    result: "dict[str, Any] | None",
) -> "QualityComparisonBasis | None":
    """Re-type the JSON-plain ``comparison_basis`` a decision dict carries.

    The decision dict stores the basis as ``msgspec.to_builtins`` output so
    it survives json.dumps'd API responses and the evidence-action wire.
    This is the one converter back to the typed Struct — used by dispatch
    when synthesizing the reject-side ImportResult and by the harness when
    consuming the action file. Strict convert: dispatch and harness ship in
    the same deploy, so shape drift is a bug worth failing on.
    """
    if not result:
        return None
    raw = result.get("comparison_basis")
    if raw is None:
        return None
    return msgspec.convert(raw, type=QualityComparisonBasis)


QUALITY_DECISION_IMPORT_STAGE_DECISIONS: frozenset[str] = frozenset({
    "import",
    "preflight_existing",
    "transcode_upgrade",
    "transcode_first",
    "provisional_lossless_upgrade",
})
QUALITY_DECISION_REJECT_STAGE_DECISIONS: frozenset[str] = frozenset({
    "downgrade",
    "transcode_downgrade",
    "suspect_lossless_downgrade",
    "suspect_lossless_probe_missing",
    "lossless_source_locked",
})
QUALITY_DECISION_REQUEUE_DECISIONS: frozenset[str] = frozenset({
    "requeue_upgrade",
    "requeue_lossless",
})


def classify_quality_import_stages(
    stage2: object,
    stage3: object,
    *,
    imported: bool,
) -> tuple[str, bool, str | None]:
    """Classify import-stage outcomes for preview/audit cleanup policy.

    Returns ``(verdict, cleanup_eligible, reason)``. ``cleanup_eligible`` means
    the rejection is safe to use for source-folder cleanup; import/requeue
    outcomes are never cleanup-eligible.
    """

    stage2_decision = str(stage2) if isinstance(stage2, str) else None
    stage3_decision = str(stage3) if isinstance(stage3, str) else None

    if stage2_decision in QUALITY_DECISION_REJECT_STAGE_DECISIONS:
        return "confident_reject", True, stage2_decision

    if stage2_decision in QUALITY_DECISION_IMPORT_STAGE_DECISIONS or imported:
        reason = (
            stage3_decision
            if stage3_decision in QUALITY_DECISION_REQUEUE_DECISIONS
            else stage2_decision or stage3_decision or "import"
        )
        return "would_import", False, reason

    if stage3_decision in QUALITY_DECISION_REQUEUE_DECISIONS:
        return "uncertain", False, stage3_decision

    return "uncertain", False, stage2_decision or stage3_decision or "unknown"


def classify_full_pipeline_decision(
    decision: dict[str, object],
) -> tuple[str, bool, str | None]:
    """Classify a full pipeline decision dict for preview/cleanup display."""

    if decision.get("preimport_nested") == "reject_nested":
        return "confident_reject", True, "nested_layout"
    if decision.get("preimport_audio") == "reject_corrupt":
        return "confident_reject", True, "audio_corrupt"
    # U11: bad-hash and empty-fileset early-exit rejects.
    if decision.get("preimport_bad_hash") == "reject_bad_hash":
        return "confident_reject", True, "bad_audio_hash"
    if decision.get("preimport_empty_fileset") == "reject_empty":
        return "confident_reject", True, "empty_fileset"
    if decision.get("preimport_mixed_source") == "reject_mixed_source":
        return "confident_reject", True, "mixed_source"
    if (
        decision.get("stage1_spectral") == "reject"
        and not decision.get("stage2_import")
    ):
        return "confident_reject", True, "spectral_reject"
    return classify_quality_import_stages(
        decision.get("stage2_import"),
        decision.get("stage3_quality_gate"),
        imported=bool(decision.get("imported")),
    )


def _require_evidence_ready(
    role: str,
    evidence: AlbumQualityEvidence,
) -> None:
    reasons = evidence.policy_incomplete_reasons()
    if reasons:
        joined = "; ".join(reasons)
        raise ValueError(f"{role} album quality evidence is incomplete: {joined}")


def _first_bitrate(*values: int | None) -> int:
    for value in values:
        if value is not None:
            return value
    raise ValueError("album quality evidence has no bitrate metric")


def _normalised_format(value: str | None) -> str | None:
    if value is None:
        return None
    normalised = value.strip().lower().lstrip(".")
    return normalised or None


def _lossless_source_from_evidence(evidence: AlbumQualityEvidence) -> bool:
    metric = evidence.v0_metric
    if (
        metric is not None
        and metric.source_lineage == V0_SOURCE_LINEAGE_LOSSLESS_SOURCE
    ):
        return True

    measurement = evidence.measurement
    if measurement.verified_lossless and evidence.verified_lossless_proof is not None:
        return True
    candidates = (
        measurement.was_converted_from,
        evidence.storage_format,
        evidence.codec,
        evidence.container,
        measurement.format,
    )
    for candidate in candidates:
        fmt = _normalised_format(candidate)
        if fmt == "m4a":
            # M4A is only a container; AAC and ALAC share it. Treat ALAC
            # evidence as lossless, but never infer lossless source from a
            # bare .m4a extension/container.
            continue
        if fmt in _LOSSLESS_EXTS or fmt == "lossless":
            return True
    return _normalised_format(evidence.codec) == "alac"


def _policy_v0_probe_from_metric(
    metric: AlbumQualityV0Metric | None,
) -> V0ProbeEvidence | None:
    if metric is None:
        return None
    kind = (
        V0_PROBE_LOSSLESS_SOURCE
        if metric.source_lineage == V0_SOURCE_LINEAGE_LOSSLESS_SOURCE
        else _NONCOMPARABLE_NEUTRAL_V0_PROBE_KIND
    )
    return V0ProbeEvidence(
        kind=kind,
        min_bitrate_kbps=metric.min_bitrate_kbps,
        avg_bitrate_kbps=metric.avg_bitrate_kbps,
        median_bitrate_kbps=metric.median_bitrate_kbps,
    )


def _evidence_target_format(
    candidate: AlbumQualityEvidence,
    facts: AlbumQualityEvidenceDecisionFacts,
) -> str | None:
    return facts.target_format if facts.target_format is not None else candidate.target_format


def _evidence_target_is_cbr(
    candidate: AlbumQualityEvidence,
    facts: AlbumQualityEvidenceDecisionFacts,
    *,
    target_format: str | None,
) -> bool | None:
    """Resolve projected mode without borrowing source/output measurements."""

    if facts.post_conversion_is_cbr is not None:
        return facts.post_conversion_is_cbr
    if (
        target_format is not None
        and target_format == candidate.target_format
        and candidate.target_is_cbr is not None
    ):
        return candidate.target_is_cbr
    if target_format is None:
        return None
    return TargetQualityContract.from_format(target_format).is_cbr


def _new_format_hint_from_evidence(
    candidate: AlbumQualityEvidence,
    *,
    supported_lossless_source: bool,
    target_format: str | None,
) -> str | None:
    if supported_lossless_source and target_format not in ("flac", "lossless"):
        return None
    return candidate.measurement.format or candidate.storage_format


def override_bitrate_from_current_evidence(
    current: AlbumQualityEvidence | None,
) -> int | None:
    if current is None:
        return None
    measurement = current.measurement
    current_min = measurement.min_bitrate_kbps
    effective = compute_effective_override_bitrate(
        current_min,
        measurement.spectral_bitrate_kbps,
        measurement.spectral_grade,
    )
    if current_min is not None and effective is not None and effective != current_min:
        return effective
    return None


def full_pipeline_decision_from_evidence(
    candidate: AlbumQualityEvidence,
    current: AlbumQualityEvidence | None = None,
    *,
    facts: AlbumQualityEvidenceDecisionFacts | None = None,
    cfg: "QualityRankConfig | None" = None,
) -> dict[str, Any]:
    """Run the full quality policy from neutral album-quality evidence.

    This is THE single decision function for the importer. Callers
    provide durable ``AlbumQualityEvidence`` rows plus narrow action facts;
    old V0 probe ``kind`` constants are not accepted as public inputs.

    The decision dict shape (shared with ``full_pipeline_decision``):

        {
            "preimport_audio": str | None,
            "preimport_nested": str | None,
            "preimport_bad_hash": str | None,       # U11
            "preimport_empty_fileset": str | None,  # U11
            "preimport_mixed_source": str | None,   # mixed-source reject
            "stage0_spectral_gate": str | None,
            "stage1_spectral": str | None,
            "stage2_import": str | None,
            "stage3_quality_gate": str | None,
            "final_status": str | None,
            "imported": bool,
            "denylisted": bool,
            "keep_searching": bool,
            "target_final_format": str | None,
            "verified_lossless": bool,
            "comparison_basis": dict | None,  # QualityComparisonBasis builtins
        }

    Folder/audio-integrity facts are read directly off ``candidate`` as
    early-exit rejects (in priority order):

      1. ``audio_corrupt``  — sets ``preimport_audio='reject_corrupt'``
      2. ``bad_audio_hash`` — sets ``preimport_bad_hash='reject_bad_hash'``
      3. ``nested_layout``  — sets ``preimport_nested='reject_nested'``
      4. ``empty_fileset``  — sets ``preimport_empty_fileset='reject_empty'``
      5. ``mixed_source``   — sets
         ``preimport_mixed_source='reject_mixed_source'`` when the
         snapshot contains both lossless and lossy containers (e.g.
         15 FLAC + 2 MP3). Keeps Cratedigger release-based — never
         partially-imports an album.

    The accompanying ``evidence_decision_name`` maps these dict shapes to
    ``audio_corrupt`` / ``bad_audio_hash`` / ``nested_layout`` /
    ``empty_fileset`` / ``mixed_source`` decision strings, which the
    importer feeds to ``dispatch_action`` and the unified
    ``_reject_import_from_evidence_decision`` helper.
    """

    if facts is None:
        facts = AlbumQualityEvidenceDecisionFacts()

    _require_evidence_ready("candidate", candidate)
    if current is not None:
        _require_evidence_ready("current", current)

    # --- U11 folder/audio-integrity early-exit rejects ---
    # The four facts live directly on the persisted ``AlbumQualityEvidence``
    # row (added by U1+U2/U3 migrations). Order matches the deleted
    # ``preimport_decide``: corrupt > bad-hash > nested > empty.
    #
    # SQL defaults for U1 fields (migration 019) are ``audio_corrupt=FALSE``,
    # ``folder_layout='flat'``, ``audio_file_count=0``, ``filetype_band=''``.
    # Legacy rows decoding under those defaults must not trigger
    # ``empty_fileset`` when files are present — reconcile against the
    # snapshot ``files`` list, mirroring the prior ``_build_preimport_
    # measurement_from_evidence`` reconciliation.
    def _early_reject_result(
        *,
        preimport_audio: str | None = None,
        preimport_nested: str | None = None,
        preimport_bad_hash: str | None = None,
        preimport_empty_fileset: str | None = None,
        preimport_mixed_source: str | None = None,
        denylisted: bool,
    ) -> dict[str, Any]:
        # Mirror the live preimport-fact reject side effects: auto-import
        # rejects re-queue to ``wanted``; force/manual leaves the
        # request alone (the unified reject helper forces ``requeue=True``
        # for these decisions regardless, but the dict's ``final_status``
        # / ``keep_searching`` reflect the auto path the simulator
        # describes, matching the existing ``preimport_nested`` /
        # ``preimport_audio`` early-exit shape produced by
        # ``full_pipeline_decision``).
        auto = facts.import_mode == "auto"
        return {
            "preimport_audio": preimport_audio,
            "preimport_nested": preimport_nested,
            "preimport_bad_hash": preimport_bad_hash,
            "preimport_empty_fileset": preimport_empty_fileset,
            "preimport_mixed_source": preimport_mixed_source,
            "stage0_spectral_gate": None,
            "stage1_spectral": None,
            "stage2_import": None,
            "stage3_quality_gate": None,
            "final_status": "wanted" if auto else None,
            "imported": False,
            "denylisted": bool(denylisted and auto),
            "keep_searching": bool(auto),
            "target_final_format": None,
            "verified_lossless": False,
            "comparison_basis": None,
        }

    if candidate.audio_corrupt:
        return _early_reject_result(
            preimport_audio="reject_corrupt",
            denylisted=True,
        )

    if candidate.matched_bad_audio_hash_id is not None:
        return _early_reject_result(
            preimport_bad_hash="reject_bad_hash",
            denylisted=True,
        )

    if candidate.folder_layout == "nested":
        return _early_reject_result(
            preimport_nested="reject_nested",
            denylisted=False,
        )

    # Reconcile audio_file_count against snapshot files: legacy rows decode
    # the SQL default 0 but may carry snapshot files. Only the
    # explicit-and-corroborated zero case (count=0 AND no snapshot files)
    # is the empty_fileset reject.
    effective_audio_file_count = (
        len(candidate.files) if candidate.files else candidate.audio_file_count
    )
    if effective_audio_file_count == 0:
        return _early_reject_result(
            preimport_empty_fileset="reject_empty",
            denylisted=False,
        )

    # Mixed-source reject: lossless + lossy containers in the same folder.
    # Cratedigger stays release-based — a partial FLAC+MP3 source must
    # never get partially-imported and stamped verified-lossless. See
    # ``has_mixed_lossless_and_lossy`` and the Fast Times reproduction.
    if has_mixed_lossless_and_lossy(candidate.files):
        return _early_reject_result(
            preimport_mixed_source="reject_mixed_source",
            denylisted=True,
        )

    candidate_measurement = candidate.measurement
    current_measurement = current.measurement if current is not None else None
    candidate_probe = _policy_v0_probe_from_metric(candidate.v0_metric)
    current_probe = (
        _policy_v0_probe_from_metric(current.v0_metric)
        if current is not None
        else None
    )

    target_format = _evidence_target_format(candidate, facts)
    post_conversion_is_cbr = _evidence_target_is_cbr(
        candidate,
        facts,
        target_format=target_format,
    )
    supported_lossless_source = _lossless_source_from_evidence(candidate)
    post_conversion_min = (
        facts.post_conversion_min_bitrate
        if facts.post_conversion_min_bitrate is not None
        else (
            candidate_probe.min_bitrate_kbps
            if supported_lossless_source and candidate_probe is not None
            else None
        )
    )
    converted_count = facts.converted_count
    if converted_count is None:
        converted_count = (
            1
            if (
                supported_lossless_source
                and target_format not in ("flac", "lossless")
                and post_conversion_min is not None
            )
            else 0
        )

    existing_min = None
    existing_avg = None
    existing_format = None
    existing_is_cbr = False
    existing_spectral_grade = None
    existing_spectral_bitrate = None
    if current_measurement is not None:
        assert current is not None
        existing_min = current_measurement.min_bitrate_kbps
        existing_avg = current_measurement.avg_bitrate_kbps
        existing_format = current_measurement.format or current.storage_format
        existing_is_cbr = current_measurement.is_cbr
        existing_spectral_grade = current_measurement.spectral_grade
        existing_spectral_bitrate = current_measurement.spectral_bitrate_kbps

    return full_pipeline_decision(
        is_flac=supported_lossless_source,
        min_bitrate=_first_bitrate(
            candidate_measurement.min_bitrate_kbps,
            candidate_measurement.avg_bitrate_kbps,
            candidate_measurement.median_bitrate_kbps,
        ),
        is_cbr=candidate_measurement.is_cbr,
        avg_bitrate=candidate_measurement.avg_bitrate_kbps,
        spectral_grade=candidate_measurement.spectral_grade,
        spectral_bitrate=candidate_measurement.spectral_bitrate_kbps,
        existing_min_bitrate=existing_min,
        existing_avg_bitrate=existing_avg,
        existing_spectral_bitrate=existing_spectral_bitrate,
        existing_spectral_grade=existing_spectral_grade,
        override_min_bitrate=override_bitrate_from_current_evidence(current),
        existing_format=existing_format,
        existing_is_cbr=existing_is_cbr,
        post_conversion_min_bitrate=post_conversion_min,
        post_conversion_is_cbr=post_conversion_is_cbr,
        converted_count=converted_count,
        verified_lossless=candidate_measurement.verified_lossless,
        verified_lossless_target=facts.verified_lossless_target,
        target_format=target_format,
        new_format=_new_format_hint_from_evidence(
            candidate,
            supported_lossless_source=supported_lossless_source,
            target_format=target_format,
        ),
        audio_check_mode=facts.audio_check_mode,
        audio_corrupt=facts.audio_corrupt,
        import_mode=facts.import_mode,
        has_nested_audio=facts.has_nested_audio,
        cfg=cfg,
        candidate_v0_probe_avg=(
            candidate_probe.avg_bitrate_kbps
            if candidate_probe is not None
            else None
        ),
        candidate_v0_probe_min=(
            candidate_probe.min_bitrate_kbps
            if candidate_probe is not None
            else None
        ),
        existing_v0_probe_avg=(
            current_probe.avg_bitrate_kbps
            if current_probe is not None
            else None
        ),
        existing_v0_probe_kind=(
            current_probe.kind if current_probe is not None else None
        ),
        candidate_v0_probe_kind=(
            candidate_probe.kind if candidate_probe is not None else None
        ),
        supported_lossless_source=supported_lossless_source,
    )
