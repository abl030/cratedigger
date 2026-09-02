"""Pre-import gate triggers (spectral, audio-integrity, nested-layout).

Extracted verbatim from the monolithic ``lib/quality.py`` (issue #477).
Pure move: every definition is AST-identical to the original — except
``spectral_gate_trigger``, which issue #829 Phase 5 PR2b made codec-aware
so it stops claiming production would measure codecs it never measures.
"""

from typing import Literal

from lib.quality.evidence_types import CODEC_FAMILY_MP3, CodecFamily

# ---------------------------------------------------------------------------
# Pre-import spectral decision (MP3/CBR path in process_completed_album)
# ---------------------------------------------------------------------------

def spectral_gate_trigger(
    *,
    is_flac: bool,
    codec_family: CodecFamily | None,
) -> str:
    """Decide whether the PREIMPORT spectral gate would run on this file.

    Mirrors ``lib.measurement._needs_spectral_check`` — and only that. That
    helper reads a filetype string and answers "lossless source → run; MP3 →
    run; **every other codec → never, they have no calibrated cliff
    policy**". This mirror used to see only ``is_flac``/``is_cbr``/``is_vbr``
    and so answered ``"would_run"`` for an AAC or Opus candidate the
    preimport gate would never have run on — the codec-blind seam issue #829
    exists to close. ``codec_family`` is required (not defaulted) precisely so
    a caller cannot silently reintroduce that blindness.

    Issue #1145 removed the VBR skip from both sides: an MP3 is scanned
    whatever its declared mode or album average, because neither is evidence
    about provenance — the mode is the encoder's own Xing/Info header, and a
    transcode re-encoded high genuinely has a high average. The
    ``skipped_vbr_high_avg`` outcome, the threshold parameter, and the
    one-kbps ``<=``/``>=`` boundary disagreement between this mirror and
    ``_needs_spectral_check`` all went with it. 675 historical
    ``download_log`` rows still carry the retired string (measured
    2026-08-14); it is opaque audit text there and nothing re-derives it.

    TWO divergences survive. Both are recorded rather than closed, and both
    are one-directional: this mirror can only ever *withhold* a spectral
    opinion where production measured, never present an album as scanned
    that was not.

    First, ``skipped_flac``. ``_needs_spectral_check`` answers True for a
    lossless candidate (preview must produce affirmative evidence for it),
    while this mirror reports ``"skipped_flac"`` because the verdict Stage 1
    consumes for a FLAC comes from convert → V0 → ``transcode_detection``,
    not from the MP3 preimport gate. Same codec, two different questions;
    ``full_pipeline_decision`` reads that by passing
    ``stage0_gates_stage1 = gate == "would_run" or is_flac``.

    Second, an unresolved codec. The two sides are NOT given the same
    information, so this one is a real disagreement rather than two
    questions: ``_needs_spectral_check`` receives the candidate's filetype
    STRING and answers a substring test on it (``"mp3" in filetype and
    "flac" not in filetype``), while this mirror receives an already
    RESOLVED ``codec_family``. ``resolve_measured_codec_family`` fails
    closed to ``None`` before anything else on a mixed-codec album, and for
    any row whose labels resolve to no family::

        filetype "mp3",      family None  -> production True,
                                             mirror skipped_uncalibrated_codec
        filetype "m4a, mp3", family None  -> production True,
                                             mirror skipped_uncalibrated_codec

    Keying this mirror off the filetype string instead would reconcile them
    and reintroduce exactly the codec blindness issue #829 Phase 5 PR2b
    removed: reading a codec out of a label is what made this function claim
    ``would_run`` for AAC and Opus candidates. The divergence is the price of
    that fix, and it is paid in the conservative direction.

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
        "would_run"             — any MP3
    """
    if is_flac:
        return "skipped_flac"
    if codec_family != CODEC_FAMILY_MP3:
        return "skipped_uncalibrated_codec"
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

    A nested folder must reject: the downstream ``harness/import_one.py``
    still uses ``os.listdir`` for bitrate measurement and conversion, so a
    nested import would pass the gates and then produce an
    empty/misclassified measurement.

    The auto path is already flattened by ``process_completed_album`` before
    dispatch runs. If a nested folder nevertheless reaches this shared
    decision boundary, caller identity does not make it safe.

    This gate no longer runs ahead of the audio-integrity gate in
    production: dispatch reads persisted ``AlbumQualityEvidence`` and never
    measures directly (``lib.dispatch.dispatch_import_from_db`` never calls
    ``measure_preimport_state``), so there is no live pre-check to mirror.
    ``preimport_corrupt_outranks_nested`` is the one function that decides
    which of the two facts a decision reports when a candidate carries both
    (issue #1355 item 1).

    Returns one of:
        "reject_nested"  — nested audio files present
        "pass"           — flat layout
    """
    return "reject_nested" if has_nested_audio else "pass"


def preimport_corrupt_outranks_nested(
    *, audio_corrupt: bool, nested_layout: bool,
) -> Literal["audio_corrupt", "nested_layout"] | None:
    """The one precedence between corrupt audio and nested folder shape.

    ``full_pipeline_decision`` (the flat-kwargs simulator twin) and
    ``lib.quality.pipeline.candidate_preimport_reject_fact`` (feeding
    ``full_pipeline_decision_from_evidence``, production's real decider)
    used to independently encode this ordering and disagreed on a
    candidate that is both corrupt and nested: the evidence twin checked
    corrupt first, the flat twin checked nested first (issue #1355 item 1).
    The two facts are not mutually exclusive — ``measure_preimport_state``
    derives folder layout from a single path enumeration before it runs the
    audio-integrity decode, so either can be true regardless of the other,
    and both can land on the same persisted evidence row.

    The consequence is policy-visible: ``dispatch_actions.decision_denylists``
    denylists ``audio_corrupt`` but not ``nested_layout``. Corrupt audio
    always outranks folder shape here, so a peer whose upload decodes as
    garbage is denylisted whether or not its folder also happens to be
    nested. Both callers route through this one function so they cannot
    drift apart again.
    """
    if audio_corrupt:
        return "audio_corrupt"
    if nested_layout:
        return "nested_layout"
    return None
