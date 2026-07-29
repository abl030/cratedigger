"""Pre-import gate triggers (spectral, audio-integrity, nested-layout).

Extracted verbatim from the monolithic ``lib/quality.py`` (issue #477).
Pure move: every definition is AST-identical to the original — except
``spectral_gate_trigger``, which issue #829 Phase 5 PR2b made codec-aware
so it stops claiming production would measure codecs it never measures.
"""

from lib.quality.evidence_types import CODEC_FAMILY_MP3, CodecFamily

# ---------------------------------------------------------------------------
# Pre-import spectral decision (MP3/CBR path in process_completed_album)
# ---------------------------------------------------------------------------

def spectral_gate_trigger(
    *,
    is_flac: bool,
    is_cbr: bool | None,
    is_vbr: bool | None = None,
    avg_bitrate_kbps: int | None = None,
    vbr_threshold_kbps: int,
    codec_family: CodecFamily | None,
) -> str:
    """Decide whether the PREIMPORT spectral gate would run on this file.

    Mirrors ``lib.measurement._needs_spectral_check`` — and only that. That
    helper reads a filetype string and answers "lossless source → always;
    MP3 → per the VBR rules; **every other codec → never, they have no
    calibrated cliff policy**". This mirror used to see only
    ``is_flac``/``is_cbr``/``is_vbr`` and so answered ``"would_run"`` for an
    AAC or Opus candidate the preimport gate would never have run on — the
    codec-blind seam issue #829 exists to close. ``codec_family`` is
    required (not defaulted) precisely so a caller cannot silently
    reintroduce that blindness.

    **This is not a claim that the album was never measured.**
    ``harness/import_one.py`` calls ``collect_attempt_spectral_audit``
    unconditionally for every candidate that reaches it, and the
    current-library evidence path measures installed albums whatever their
    codec — 19 AAC and 5 Vorbis source-subject rows carry a measured grade
    on prod today, two AAC rows with a raw ``cliff_hz``. A non-MP3 album
    really can arrive here holding spectral evidence. The narrower and
    exact claim is: the PREIMPORT gate, whose verdict Stage 1 consumes,
    does not fire for this codec, and no cliff policy is calibrated for it,
    so whatever was measured elsewhere yields no class.

    Returns one of:
        "skipped_flac"          — FLACs use convert → V0 → transcode_detection,
                                  not the MP3 preimport spectral gate
        "skipped_uncalibrated_codec"
                                — the measured codec is not MP3 (or is
                                  unknown), so the preimport gate never
                                  fires and no cliff policy is calibrated
                                  for it
        "skipped_vbr_high_avg"  — VBR MP3 with avg bitrate >= threshold;
                                  genuine V0 falls through without analysis
        "would_run"             — CBR MP3, MP3 with unknown VBR, or VBR MP3
                                  with avg below / equal to / unknown

    When ``is_vbr`` is None but ``is_cbr`` is known, ``is_vbr`` is derived
    as ``not is_cbr``. Callers that have genuine ambiguity (mutagen failed
    to read bitrate_mode) pass ``is_vbr=None`` AND ``is_cbr=None`` and an
    MP3 routes to "would_run" — the conservative choice within MP3.

    An unknown ``codec_family`` skips rather than running: production's
    ``_needs_spectral_check`` reaches its ``is_mp3`` test with the same
    unknown label and answers False. Conservatism here means *withholding a
    spectral opinion*, never rejecting an album.
    """
    if is_flac:
        return "skipped_flac"
    if codec_family != CODEC_FAMILY_MP3:
        return "skipped_uncalibrated_codec"
    if is_vbr is None and is_cbr is not None:
        is_vbr = not is_cbr
    if not is_vbr:
        return "would_run"
    if avg_bitrate_kbps is not None and avg_bitrate_kbps >= vbr_threshold_kbps:
        return "skipped_vbr_high_avg"
    return "would_run"


def preimport_audio_gate(audio_check_mode: str, audio_corrupt: bool) -> str:
    """Decide the outcome of the preimport audio-integrity gate.

    Mirrors the audio-integrity check that ``measure_preimport_state``
    performs in ``lib.measurement``: ``validate_audio`` runs an ffmpeg
    full-decode pass unless the operator has set
    ``[Beets Validation] audio_check = off``.

    Returns one of:
        "skipped_off"     — cfg.audio_check_mode == "off", validate_audio is not called
        "reject_corrupt"  — validate_audio reported one or more failed files
        "pass"            — validation ran and every file decoded cleanly

    Keeping this as its own pure helper lets ``full_pipeline_decision`` and
    the Decisions tab document a distinct "you have audio_check off" path,
    which is a common source of surprise when an obvious-looking corrupt
    download gets through in one deployment but not another.
    """
    if audio_check_mode == "off":
        return "skipped_off"
    return "reject_corrupt" if audio_corrupt else "pass"


def preimport_nested_gate(has_nested_audio: bool) -> str:
    """Decide the outcome of the preimport nested-folder gate.

    Mirrors ``lib.dispatch.dispatch_import_from_db``'s fail-fast
    rejection of nested imports: the preimport gates recurse,
    but the downstream ``harness/import_one.py`` still uses ``os.listdir``
    for bitrate measurement and conversion. A nested import
    would pass the gates and then produce an empty/misclassified measurement.

    The auto path is already flattened by ``process_completed_album`` before
    dispatch runs. If a nested folder nevertheless reaches this shared
    decision boundary, caller identity does not make it safe.

    Returns one of:
        "reject_nested"  — nested audio files present
        "pass"           — flat layout
    """
    return "reject_nested" if has_nested_audio else "pass"
