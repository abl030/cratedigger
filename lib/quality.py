"""Quality decision logic for the download pipeline.

Pure functions — no database, no filesystem, no external dependencies.
Used by soularr.py and import_one.py, tested directly against real audio fixtures.
"""

QUALITY_UPGRADE_TIERS = "flac,mp3 v0,mp3 320"
QUALITY_MIN_BITRATE_KBPS = 210  # V0 floor — below this triggers upgrade
TRANSCODE_MIN_BITRATE_KBPS = 210  # V0 from genuine lossless is always >= this


# ---------------------------------------------------------------------------
# Pre-import spectral decision (MP3/CBR path in process_completed_album)
# ---------------------------------------------------------------------------

def spectral_import_decision(spectral_grade, spectral_bitrate, existing_spectral_bitrate):
    """Decide whether to import a download based on spectral analysis.

    Called in process_completed_album() for non-FLAC downloads after
    spectral analysis runs on the downloaded files.

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

def import_quality_decision(new_min_bitrate, existing_min_bitrate, override_min_bitrate=None,
                            is_transcode=False, will_be_verified_lossless=False):
    """Decide whether to import based on bitrate comparison.

    Called in import_one.py after FLAC→V0 conversion (if applicable)
    and before running the beets harness.

    Returns one of:
        "import"              — new files are better (or no existing), proceed
        "downgrade"           — new files are worse, skip (exit 5)
        "transcode_upgrade"   — transcode but better than existing, import + denylist (exit 6)
        "transcode_downgrade" — transcode and not better, skip + denylist (exit 6)
        "transcode_first"     — transcode but nothing on disk yet, import (exit 6)

    Inputs:
        new_min_bitrate:      min bitrate of new files (kbps)
        existing_min_bitrate: min bitrate in beets for this MBID (kbps), or None
        override_min_bitrate: pipeline DB override (when beets value is wrong), or None
        is_transcode:         True if FLAC→V0 produced sub-210kbps output
        will_be_verified_lossless: True if genuine FLAC was converted to V0
    """
    # Genuine FLAC→V0 always wins — V0 bitrate is numerically lower than
    # CBR 320 but objectively better quality (verified lossless source).
    if will_be_verified_lossless:
        return "import"

    effective_existing = override_min_bitrate if override_min_bitrate is not None else existing_min_bitrate

    if effective_existing is not None and new_min_bitrate is not None:
        if new_min_bitrate <= effective_existing:
            if is_transcode:
                return "transcode_downgrade"
            return "downgrade"
        else:
            if is_transcode:
                return "transcode_upgrade"
            return "import"
    elif existing_min_bitrate is None and is_transcode:
        return "transcode_first"
    else:
        return "import"


def transcode_detection(converted_count, post_conversion_min_bitrate):
    """Detect whether a FLAC→V0 conversion produced a transcode.

    Called in import_one.py after convert_flac_to_v0().

    Returns True if the converted files are likely transcodes
    (MP3 wrapped in FLAC container).

    Inputs:
        converted_count:            number of FLAC files converted
        post_conversion_min_bitrate: min bitrate after conversion (kbps), or None
    """
    if converted_count == 0:
        return False
    if post_conversion_min_bitrate is None:
        return False
    return post_conversion_min_bitrate < TRANSCODE_MIN_BITRATE_KBPS


# ---------------------------------------------------------------------------
# Post-import quality gate (runs after successful import in soularr.py)
# ---------------------------------------------------------------------------

def quality_gate_decision(min_bitrate, is_cbr, verified_lossless, spectral_bitrate=None):
    """Pure decision logic for the post-import quality gate.

    Returns one of: "accept", "requeue_upgrade", "requeue_flac".

    Inputs:
        min_bitrate:      min track bitrate in kbps (from beets DB)
        is_cbr:           True if all tracks have the same bitrate
        verified_lossless: True if imported from spectral-verified genuine FLAC
        spectral_bitrate: estimated original bitrate from spectral cliff detection (kbps)
    """
    gate_br = min_bitrate

    # Spectral bitrate overrides if lower (catches fake 320s)
    if spectral_bitrate is not None and spectral_bitrate < gate_br:
        gate_br = spectral_bitrate

    # Verified lossless overrides low bitrate (quiet/simple music is fine)
    if verified_lossless and gate_br < QUALITY_MIN_BITRATE_KBPS:
        gate_br = QUALITY_MIN_BITRATE_KBPS  # force pass

    if gate_br < QUALITY_MIN_BITRATE_KBPS:
        return "requeue_upgrade"
    elif not verified_lossless and is_cbr:
        return "requeue_flac"
    else:
        return "accept"


# ---------------------------------------------------------------------------
# Full pipeline decision — combines all three stages
# ---------------------------------------------------------------------------

def full_pipeline_decision(
    # File properties
    is_flac,
    min_bitrate,
    is_cbr,
    # Spectral analysis
    spectral_grade=None,
    spectral_bitrate=None,
    # Existing state
    existing_min_bitrate=None,
    existing_spectral_bitrate=None,
    override_min_bitrate=None,
    # Post-conversion (FLAC path only)
    post_conversion_min_bitrate=None,
    converted_count=0,
    # Pipeline state
    verified_lossless=False,
):
    """Run the full decision chain and return the final outcome.

    This simulates what happens when a download completes and flows through
    process_completed_album → import_one.py → _check_quality_gate.

    Returns a dict:
        {
            "stage1_spectral": str,     # pre-import spectral decision
            "stage2_import": str,       # import/downgrade/transcode decision
            "stage3_quality_gate": str,  # post-import quality gate decision
            "final_status": str,        # what the pipeline DB ends up as
            "imported": bool,           # whether files were imported to beets
            "denylisted": bool,         # whether source user gets denylisted
            "keep_searching": bool,     # whether the system keeps looking for better
        }
    """
    result = {
        "stage1_spectral": None,
        "stage2_import": None,
        "stage3_quality_gate": None,
        "final_status": None,
        "imported": False,
        "denylisted": False,
        "keep_searching": False,
    }

    # --- Stage 1: Pre-import spectral (MP3/CBR path) ---
    # For FLACs, spectral runs inside import_one.py instead, but the
    # logic is the same: detect transcodes before importing.
    if spectral_grade:
        result["stage1_spectral"] = spectral_import_decision(
            spectral_grade, spectral_bitrate, existing_spectral_bitrate or 0)

        if result["stage1_spectral"] == "reject":
            result["final_status"] = "wanted"  # stays wanted, denylist user
            result["denylisted"] = True
            result["keep_searching"] = True
            return result

    # --- Stage 2: Import decision ---
    if is_flac:
        # FLAC path: convert first, then decide
        is_transcode = transcode_detection(converted_count, post_conversion_min_bitrate)
        import_br = post_conversion_min_bitrate if post_conversion_min_bitrate else min_bitrate

        will_be_verified = (converted_count > 0 and not is_transcode)
        result["stage2_import"] = import_quality_decision(
            import_br, existing_min_bitrate, override_min_bitrate, is_transcode,
            will_be_verified_lossless=will_be_verified)

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

        # For genuine FLAC→V0, set verified_lossless
        if (converted_count > 0 and not is_transcode and
                spectral_grade in ("genuine", "marginal", None)):
            verified_lossless = True

        # Use post-conversion bitrate for quality gate
        gate_bitrate = post_conversion_min_bitrate or min_bitrate
        gate_cbr = False  # V0 conversion always produces VBR
    else:
        # MP3 path: import directly
        result["stage2_import"] = import_quality_decision(
            min_bitrate, existing_min_bitrate, override_min_bitrate)

        if result["stage2_import"] == "downgrade":
            result["final_status"] = "imported"  # keeps existing
            result["keep_searching"] = True
            return result

        result["imported"] = True
        gate_bitrate = min_bitrate
        gate_cbr = is_cbr

    # --- Stage 3: Post-import quality gate ---
    result["stage3_quality_gate"] = quality_gate_decision(
        gate_bitrate, gate_cbr, verified_lossless, spectral_bitrate)

    if result["stage3_quality_gate"] == "accept":
        result["final_status"] = "imported"
    elif result["stage3_quality_gate"] == "requeue_upgrade":
        result["final_status"] = "wanted"
        result["denylisted"] = True
        result["keep_searching"] = True
    elif result["stage3_quality_gate"] == "requeue_flac":
        result["final_status"] = "wanted"
        result["keep_searching"] = True

    return result
