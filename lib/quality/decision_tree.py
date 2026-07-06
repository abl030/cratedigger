"""get_decision_tree — the Decisions-tab UI feeder.

Extracted verbatim from the monolithic ``lib/quality.py`` (issue #477).
Pure move: every definition is AST-identical to the original.
"""

from typing import Any

from lib.quality.ranks import QualityRankConfig
from lib.quality.filetypes import QUALITY_UPGRADE_TIERS
from lib.quality.decisions import (
    DECISION_LOSSLESS_SOURCE_LOCKED,
    DECISION_PROVISIONAL_LOSSLESS_UPGRADE,
    DECISION_SUSPECT_LOSSLESS_DOWNGRADE,
    DECISION_SUSPECT_LOSSLESS_PROBE_MISSING,
    QUALITY_MIN_BITRATE_KBPS,
)


# ---------------------------------------------------------------------------
# Decision tree metadata — consumed by the web UI diagram
# ---------------------------------------------------------------------------

def get_decision_tree(
    cfg: "QualityRankConfig | None" = None,
) -> dict[str, Any]:
    """Return the full pipeline decision structure as data.

    The web UI renders this as a diagram. Contract tests verify this matches
    the actual decision functions. When a function changes, update this too —
    the tests will catch divergence.

    ``cfg`` drives the thresholds that depend on the runtime rank model.
    When omitted, ``QualityRankConfig.defaults()`` is used so the legacy
    "show the hardcoded defaults" behavior still works. The web route at
    ``web/routes/decisions.py:get_pipeline_constants`` passes the live
    runtime cfg so operators who retune ``mp3_vbr.excellent`` see the
    Decisions tab update in lockstep with ``transcode_detection()``
    (issue #66 follow-up).
    """
    effective_cfg = cfg if cfg is not None else QualityRankConfig.defaults()
    # The spectral-fallback threshold for transcode_detection() is derived
    # from cfg.mp3_vbr.excellent (see #66). Expose it as the "live" constant
    # so the web UI's Decisions tab reflects the runtime rank model instead
    # of a stale module-level default.
    transcode_threshold = effective_cfg.mp3_vbr.excellent
    return {
        "constants": {
            "QUALITY_MIN_BITRATE_KBPS": QUALITY_MIN_BITRATE_KBPS,
            "TRANSCODE_MIN_BITRATE_KBPS": transcode_threshold,
            "QUALITY_UPGRADE_TIERS": QUALITY_UPGRADE_TIERS,
        },
        "paths": ["flac", "mp3"],
        "path_labels": {"flac": "FLAC path", "mp3": "MP3 path"},
        "stages": [
            {
                "id": "preimport_audio",
                "title": "Preimport Audio Integrity",
                "path": "preimport",
                "function": "preimport_audio_gate",
                "when": "Every import path, before any FLAC/MP3 branching "
                        "(lib.measurement.measure_preimport_state audio gate)",
                "inputs": ["cfg.audio_check_mode", "validate_audio() result"],
                "rules": [
                    {"condition": "audio_check_mode = off",
                     "result": "skipped_off", "color": "amber",
                     "effect": "gate disabled by config, nothing is rejected here"},
                    {"condition": "validate_audio reports no failed files",
                     "result": "pass", "color": "green"},
                    {"condition": "validate_audio reports one or more failed files",
                     "result": "reject_corrupt", "color": "red",
                     "effect": "import rejected, files moved to failed_imports/"},
                ],
                "outcomes": ["pass", "reject_corrupt", "skipped_off"],
                "note": "Runs ffmpeg full-decode on every audio file. Flip to "
                        "'off' under [Beets Validation] audio_check to bypass "
                        "the gate (e.g. while debugging a false positive).",
            },
            {
                "id": "preimport_nested",
                "title": "Preimport Nested-Layout Gate",
                "path": "preimport",
                "function": "preimport_nested_gate",
                "when": "Force-import and manual-import paths only — the auto "
                        "path flattens downloads before dispatch",
                "inputs": ["import_mode", "inspect_local_files().has_nested_audio"],
                "rules": [
                    {"condition": "import_mode = auto",
                     "result": "skipped_auto", "color": "green",
                     "effect": "auto path flattens upstream; nested detection "
                               "is not a gate here"},
                    {"condition": "force/manual + flat layout",
                     "result": "pass", "color": "green"},
                    {"condition": "force/manual + audio files in subfolders",
                     "result": "reject_nested", "color": "red",
                     "effect": "harness.import_one uses os.listdir for "
                               "bitrate/convert; nested layouts would silently "
                               "misclassify — reject with 'flatten the folder' detail"},
                ],
                "outcomes": ["pass", "reject_nested", "skipped_auto"],
                "note": "Only the force/manual paths hit this gate; the auto "
                        "pipeline flattens downloads in process_completed_album "
                        "before dispatch runs.",
            },
            {
                "id": "flac_spectral",
                "title": "Spectral Analysis",
                "path": "flac",
                "function": "spectral_check.analyze_album",
                "when": "Raw FLAC files before conversion",
                "inputs": ["audio files (sox bandpass 12-20kHz)"],
                "rules": [
                    {"condition": "HF deficit < {HF_DEFICIT_MARGINAL}dB",
                     "result": "genuine", "color": "green"},
                    {"condition": "{HF_DEFICIT_MARGINAL}-{HF_DEFICIT_SUSPECT}dB",
                     "result": "marginal", "color": "amber"},
                    {"condition": ">= {HF_DEFICIT_SUSPECT}dB or cliff",
                     "result": "suspect", "color": "red"},
                ],
                "note": "Album grade: only 'suspect' counts — "
                        ">={ALBUM_SUSPECT_PCT}% suspect = album suspect. "
                        "100% marginal = album genuine",
            },
            {
                "id": "flac_convert",
                "title": "Convert FLAC \u2192 V0",
                "path": "flac",
                "function": "convert_flac_to_v0",
                "when": "FLAC files present",
                "inputs": ["FLAC audio files"],
                "rules": [
                    {"condition": "ffmpeg -q:a 0 (VBR V0)",
                     "result": "MP3 V0 files", "color": "green"},
                ],
                "note": "Post-conversion min bitrate measured across all tracks",
            },
            {
                "id": "transcode",
                "title": "Transcode Detection",
                "path": "flac",
                "function": "transcode_detection",
                "when": "After FLAC \u2192 V0 conversion",
                "inputs": ["converted_count", "post_conversion_min_bitrate",
                           "spectral_grade"],
                "rules": [
                    {"condition": "spectral = suspect/likely_transcode",
                     "result": "is_transcode = true", "color": "red",
                     "effect": "cliff detected = transcode regardless of bitrate"},
                    {"condition": "spectral = genuine/marginal",
                     "result": "is_transcode = false", "color": "green",
                     "effect": "no cliff = not transcode (lo-fi OK)"},
                    {"condition": f"no spectral: post_conv_br < {transcode_threshold}kbps",
                     "result": "is_transcode = true", "color": "red",
                     "effect": "fallback when spectral unavailable"},
                ],
                "note": f"Spectral grade is authoritative when available. "
                        f"Bitrate threshold ({transcode_threshold}kbps, "
                        f"derived from cfg.mp3_vbr.excellent) is fallback only",
            },
            {
                "id": "verified_lossless",
                "title": "Verified Lossless",
                "path": "flac",
                "function": "will_be_verified_lossless",
                "when": "After transcode detection",
                "inputs": ["converted_count", "is_transcode"],
                "rules": [
                    {"condition": "converted > 0 AND NOT is_transcode",
                     "result": "will_be_verified_lossless = true",
                     "color": "green"},
                    {"condition": "converted > 0 AND comparable lossless_source_v0 avg≥230/min≥200",
                     "result": "will_be_verified_lossless = true",
                     "color": "green",
                     "effect": "V0 source evidence overrides suspect/likely_transcode spectral false positives"},
                    {"condition": "is_transcode OR not converted",
                     "result": "will_be_verified_lossless = false",
                     "color": "amber"},
                ],
            },
            {
                "id": "target_conversion",
                "title": "Target Conversion (Optional)",
                "path": "flac",
                "function": "convert_lossless",
                "when": "After verified lossless, if verified_lossless_target is set",
                "inputs": ["verified_lossless", "verified_lossless_target",
                           "original lossless files"],
                "rules": [
                    {"condition": "verified_lossless AND target configured",
                     "result": "lossless → configured target (V0 discarded)",
                     "color": "green",
                     "effect": "V0 bitrate stored as v0_verification_bitrate"},
                    {"condition": "NOT verified_lossless OR no target configured",
                     "result": "Keep V0 files (standard path)",
                     "color": "amber"},
                ],
                "note": "V0 exists only to verify genuineness. The final target "
                        "may be Opus, MP3, AAC, or any other supported format.",
            },
            {
                "id": "provisional_lossless",
                "title": "Provisional Lossless-Source",
                "path": "flac",
                "function": "provisional_lossless_decision",
                "when": "Supported lossless-container sources whose spectral "
                        "grade is suspect or likely_transcode",
                "inputs": ["candidate lossless_source_v0 avg",
                           "current lossless_source_v0 avg",
                           "spectral_grade",
                           "cfg.within_rank_tolerance_kbps"],
                "rules": [
                    {"condition": "lossy candidate AND existing has comparable lossless-source V0 probe",
                     "result": DECISION_LOSSLESS_SOURCE_LOCKED,
                     "color": "red",
                     "effect": "reject; only another lossless source can override the recorded V0 anchor"},
                    {"condition": "spectral = genuine/marginal",
                     "result": "continue", "color": "green",
                     "effect": "clean sources stay on the verified-lossless path"},
                    {"condition": "candidate V0 passes verified-lossless override",
                     "result": "continue", "color": "green",
                     "effect": "source is treated as verified, not provisional"},
                    {"condition": "suspect source has no comparable V0 probe",
                     "result": DECISION_SUSPECT_LOSSLESS_PROBE_MISSING,
                     "color": "red",
                     "effect": "reject distinctly; do not invent a probe"},
                    {"condition": "no existing comparable source probe",
                     "result": DECISION_PROVISIONAL_LOSSLESS_UPGRADE,
                     "color": "amber",
                     "effect": "import unverified, denylist source, keep searching"},
                    {"condition": "candidate_avg - existing_avg <= within_rank_tolerance_kbps",
                     "result": DECISION_SUSPECT_LOSSLESS_DOWNGRADE,
                     "color": "red",
                     "effect": "reject as equivalent/worse suspect source"},
                    {"condition": "candidate_avg beats existing by more than tolerance",
                     "result": DECISION_PROVISIONAL_LOSSLESS_UPGRADE,
                     "color": "amber",
                     "effect": "import unverified, update current source probe"},
                ],
                "outcomes": [
                    DECISION_PROVISIONAL_LOSSLESS_UPGRADE,
                    DECISION_SUSPECT_LOSSLESS_DOWNGRADE,
                    DECISION_SUSPECT_LOSSLESS_PROBE_MISSING,
                    DECISION_LOSSLESS_SOURCE_LOCKED,
                ],
                "note": "Uses source-probe avg as evidence. Native lossy "
                        "research probes are non-comparable in v1.",
            },
            {
                "id": "mp3_spectral_gate",
                "title": "Spectral Gate Trigger",
                "path": "mp3",
                "function": "spectral_gate_trigger",
                "when": "MP3 downloads (pre-analysis decision, issue #93)",
                "inputs": ["is_cbr", "is_vbr", "avg_bitrate",
                           "cfg.mp3_vbr.excellent"],
                "rules": [
                    {"condition": "CBR MP3",
                     "result": "would_run", "color": "amber",
                     "effect": "classic transcode-cliff case"},
                    {"condition": "VBR MP3 AND avg unknown",
                     "result": "would_run", "color": "amber",
                     "effect": "conservative default"},
                    {"condition": f"VBR MP3 AND avg < {transcode_threshold}kbps",
                     "result": "would_run", "color": "amber",
                     "effect": "fake V0 transcode territory — analyze"},
                    {"condition": f"VBR MP3 AND avg >= {transcode_threshold}kbps",
                     "result": "skipped_vbr_high_avg", "color": "green",
                     "effect": "genuine V0 range — skip to Quality Comparison"},
                ],
                "outcomes": ["would_run", "skipped_vbr_high_avg",
                             "skipped_flac"],
                "note": f"Threshold ({transcode_threshold}kbps) comes from "
                        f"cfg.mp3_vbr.excellent — same V0 boundary "
                        f"transcode_detection() uses (single source of truth)",
            },
            {
                "id": "mp3_spectral",
                "title": "Spectral Decision",
                "path": "mp3",
                "function": "spectral_import_decision",
                "when": "MP3 downloads where the gate trigger said would_run",
                "inputs": ["spectral_grade", "spectral_bitrate",
                           "existing_spectral_bitrate"],
                "rules": [
                    {"condition": "grade is genuine or marginal",
                     "result": "import", "color": "green"},
                    {"condition": "suspect/likely_transcode AND new_br <= existing",
                     "result": "reject", "color": "red",
                     "effect": "denylist source"},
                    {"condition": "suspect/likely_transcode AND new_br > existing",
                     "result": "import_upgrade", "color": "amber",
                     "effect": "import + denylist"},
                    {"condition": "suspect/likely_transcode AND no existing",
                     "result": "import_no_exist", "color": "amber",
                     "effect": "import (something > nothing)"},
                ],
                "outcomes": ["import", "import_upgrade", "import_no_exist",
                             "reject"],
            },
            {
                "id": "import_decision",
                "title": "Quality Comparison",
                "path": "shared",
                "function": "import_quality_decision",
                "when": "All downloads before beets import",
                "inputs": ["new: AudioQualityMeasurement",
                           "existing: AudioQualityMeasurement | None",
                           "is_transcode"],
                "rules": [
                    {"condition": "existing is CBR: override_min_bitrate drives min + avg + median (fake-CBR-320 protection)",
                     "result": "clamped", "color": "amber",
                     "effect": "clobber all three bitrate fields to the spectral floor"},
                    {"condition": "existing is VBR: override_min_bitrate drives min only; avg + median keep beets values",
                     "result": "preserved", "color": "green",
                     "effect": "real avg signal survives — a 152kbps transcode can't win against a genuine 225-avg VBR album"},
                    {"condition": "BOTH new and existing have spectral_bitrate: compare_quality uses min(selected_metric, spectral_bitrate) for rank bucket only, except lower-real-rank transcode-grade candidates cannot beat non-transcode existing albums",
                     "result": "shared_spectral_clamp", "color": "amber",
                     "effect": "spectral can demote both sides into the same bucket, but it cannot promote a lower-rank transcode over a higher-rank genuine/non-transcode existing album; otherwise raw avg/median/min still breaks ties so the pipeline can grind upward when spectral is pessimistic"},
                    {"condition": "new.verified_lossless = true AND compare_quality(new, existing) in {better,equivalent}",
                     "result": "import", "color": "green",
                     "effect": "verified-lossless imports only when it is not worse"},
                    {"condition": "compare_quality(new, existing) = better AND is_transcode",
                     "result": "transcode_upgrade", "color": "amber",
                     "effect": "import + denylist + keep searching"},
                    {"condition": "compare_quality(new, existing) = better AND NOT is_transcode",
                     "result": "import", "color": "green"},
                    {"condition": "compare_quality(new, existing) in {worse,equivalent} AND is_transcode",
                     "result": "transcode_downgrade", "color": "red",
                     "effect": "reject + denylist"},
                    {"condition": "compare_quality(new, existing) in {worse,equivalent}",
                     "result": "downgrade", "color": "red",
                     "effect": "reject"},
                    {"condition": "existing is None AND is_transcode",
                     "result": "transcode_first", "color": "amber",
                     "effect": "import (something > nothing) + denylist"},
                ],
                "outcomes": ["import", "downgrade", "transcode_upgrade",
                             "transcode_downgrade", "transcode_first",
                             "preflight_existing",
                             DECISION_PROVISIONAL_LOSSLESS_UPGRADE,
                             DECISION_SUSPECT_LOSSLESS_DOWNGRADE,
                             DECISION_SUSPECT_LOSSLESS_PROBE_MISSING],
                "note": ("Caller resolves override_min_bitrate into "
                         "existing.min_bitrate_kbps unconditionally. "
                         "avg/median follow only when existing is CBR "
                         "(fake-CBR-320 protection). For VBR existing, "
                         "avg/median preserve beets values so the rank "
                         "comparison sees the real signal — the "
                         "loop-breaking fix for Unter Null - The Failure "
                         "Epiphany (req 1749, 2026-04-21). The "
                         "shared-spectral bucket fires only when both sides "
                         "carry spectral_bitrate and only for rank bucketing, "
                         "so a single stale estimate (Springsteen shape — "
                         "existing genuine 320 with spectral=96, new V0 240 "
                         "with no spectral) still follows the container path. "
                         "A transcode-grade candidate over a non-transcode "
                         "existing album is first checked against real "
                         "selected-metric rank; if that rank regresses, the "
                         "candidate is a downgrade. Otherwise equal buckets "
                         "still converge upward by the configured bitrate "
                         "metric."),
            },
            {
                "id": "quality_gate",
                "title": "Post-Import Quality Gate",
                "path": "shared",
                "function": "quality_gate_decision",
                "when": "After successful beets import",
                "inputs": ["current: AudioQualityMeasurement"],
                "rules": [
                    {"condition": "rank = measurement_rank(current); spectral clamp applies only when spectral grade is suspect/likely_transcode",
                     "result": "(computed)", "color": "green",
                     "effect": "spectral only lowers the rank when the current on-disk file looks transcode-like"},
                    {"condition": "rank = UNKNOWN OR rank < cfg.gate_min_rank",
                     "result": "requeue_upgrade", "color": "amber",
                     "effect": f"search {QUALITY_UPGRADE_TIERS}"},
                    {"condition": "current.is_cbr AND NOT current.verified_lossless AND rank < LOSSLESS",
                     "result": "requeue_lossless", "color": "amber",
                     "effect": "search lossless only"},
                    {"condition": "else",
                     "result": "accept", "color": "green",
                     "effect": "done"},
                ],
                "outcomes": ["accept", "requeue_upgrade", "requeue_lossless"],
            },
            {
                "id": "dispatch",
                "title": "Import Dispatch",
                "path": "shared",
                "function": "dispatch_action",
                "when": "After import_one.py returns a decision",
                "inputs": ["ImportResult.decision"],
                "rules": [
                    {"condition": "import / preflight_existing",
                     "result": "mark_done + quality_gate", "color": "green",
                     "effect": "imported, run quality gate"},
                    {"condition": "downgrade",
                     "result": "record_rejection + denylist", "color": "red",
                     "effect": "not an upgrade, denylist source"},
                    {"condition": "transcode_upgrade / transcode_first",
                     "result": "mark_done + denylist + requeue", "color": "amber",
                     "effect": "imported but transcode, keep searching"},
                    {"condition": "transcode_downgrade",
                     "result": "record_rejection + denylist + requeue", "color": "red",
                     "effect": "transcode not an upgrade, keep searching"},
                    {"condition": "provisional_lossless_upgrade",
                     "result": "mark_done + denylist + requeue", "color": "amber",
                     "effect": "imported as unverified source-probe upgrade"},
                    {"condition": "suspect_lossless_downgrade / suspect_lossless_probe_missing",
                     "result": "record_rejection + denylist + requeue", "color": "red",
                     "effect": "suspect source not a comparable improvement"},
                    {"condition": "other (error/crash/timeout)",
                     "result": "record_rejection", "color": "red",
                     "effect": "import failed"},
                ],
                "outcomes": ["import", "preflight_existing", "downgrade",
                             "transcode_upgrade", "transcode_first",
                             "transcode_downgrade",
                             DECISION_PROVISIONAL_LOSSLESS_UPGRADE,
                             DECISION_SUSPECT_LOSSLESS_DOWNGRADE,
                             DECISION_SUSPECT_LOSSLESS_PROBE_MISSING,
                             "conversion_failed",
                             "import_failed", "mbid_missing"],
            },
        ],
    }
