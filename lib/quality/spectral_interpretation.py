"""Per-codec spectral interpretation (issue #829 Phase 5 PR2a/PR2b).

Pure decisions only. PR2a stated the semantics; **PR2b wires them into the
decider** — ``interpret_measurement`` + ``decision_class_kbps`` are the two
entry points every decision seam uses, and no seam may read a raw
``spectral_bitrate_kbps`` for a decision again. Operator-facing display is
still unwired (PR4).

Why this module exists
----------------------
``lib/spectral_check.py``'s ``LAME_LOWPASS`` table is a byte-exact
transcription of LAME's own MP3 encoder lowpass array, but
``analyze_album`` measures EVERY codec and the result is persisted as
decision-facing evidence. An ordinary AAC's natural rolloff therefore gets
read as "MP3 128 transcode" — the live defect that opened issue #829
(download 37946, request 6387, Wavves — *Wavves*: a 256 kbps CBR AAC
graded ``likely_transcode`` with ``spectral_bitrate_kbps=128``).

Every constant below is MEASURED, on four independent arms totalling
60,102 production-primitive measurements (TRAINING / ROUND-1 / ROUND-2 /
ROUND-3). Do not re-derive, adjust or "improve" them. Evidence:
``docs/research/spectral-calibration-findings.md`` and
``docs/plans/2026-07-27-001-feat-829-phase5-implementation-plan.md`` §1.

The per-codec semantics, in one table
-------------------------------------
=================  ====================================================
codec family       what a measured cliff asserts
=================  ====================================================
mp3                a nominal kbps class, on an invertible ladder
vorbis (q0-q4)     a nominal kbps class, on an invertible ladder
aac                a one-sided CONTENT FLOOR. Never a bitrate, never a
                   transcode accusation: 94-96% of all AAC cliffs on
                   every arm land in 13-18 kHz, produced by everything
                   from 96 to 320 kbps across ffmpeg-native, libfdk AND
                   Apple CoreAudio.
opus (>=32k)       nothing at all. Statistically indistinguishable from
                   genuine lossless on every arm. Audit-only,
                   unconditional.
HE-AAC (SBR)       nothing at all. ``fdk-he1-64`` is 96-100% no-cliff,
                   i.e. it READS AS LOSSLESS. There is no object-type
                   pre-gate: see "The SBR pre-gate is not implemented"
                   below.
lossless           unchanged from today: the cliff is the fake-FLAC
                   detector and ``genuine`` is the affirmative input
                   verified-lossless proof requires. This module never
                   derives a kbps class for a lossless container.
other / unknown    nothing. Fail closed.
=================  ====================================================

The SBR pre-gate is not implemented
-----------------------------------
PR2a's plan reserved an ``sbr_present`` fact (AAC object type 5/29) to
demote HE-AAC to audit-only. PR3 deleted that parameter unwired
(``scope.md``: inert plumbing is dead code). It was measured not to be
needed: doc2 carries **zero HE-AAC** (409 AAC-LC and 39 ALAC in the
library, 22 ALAC in slskd), and HE-AAC cannot structurally reach the
verified-lossless proof gate anyway — ``converted_count`` only counts
files ``_is_lossless_file`` accepts and HE-AAC probes as plain ``aac``.
The genuinely dangerous shape, HE-AAC laundered INTO a FLAC container,
has no AAC object type left to read; that case is the ultrasonic proof
leg's, not a pre-gate's. Evidence: the Phase 5 plan §1.5e.

**No cliff asserts NOTHING for any codec.** The high end of every ladder
is invisible in the 12-20 kHz production window. That is a permanent
property, not a gap to be filled later.

**The verdict is ``spectral_grade``; this module does not compute one.**
``spectral_grade`` is the album-level union of BOTH detector legs — the
cliff AND the HF deficit (``lib/spectral_check.py::classify_track`` /
``classify_album``) — and the importer already gates on it
(``compute_effective_override_bitrate``). So this module reads the grade
and re-uses that gate rather than reconstructing a narrower verdict from
cliff presence, which would drop every deficit-only detection. Everything
here interprets evidence the grade has already authorized; it never
overrides the grade, and the one place it deliberately declines to consult
the grade is the AAC content floor, whose branch says why.

**"Fail closed" here means: withhold the spectral opinion.** It never
means reject the album. A caller that gets no class, or no comparison,
falls through to rank and the other evidence exactly as it does for any
candidate that was never spectrally measured.

Comparability
-------------
From the findings doc, verbatim:

    Compare in inferred-class space, never in cutoff space, and only when
    BOTH sides have an invertible ladder (MP3, Vorbis q0-q4). AAC
    contributes a one-sided floor. Opus and HE-AAC contribute nothing.
    **No ladder on either side => no comparison — unknown, not equal.**

Cutoff Hz is not a common currency: a 17 kHz cliff means ~160 kbps in MP3
and 256-320 kbps in AAC — the same number, a factor of two apart in true
quality.

Two further constraints, which the findings doc does not state.

**A class is only comparable against another class derived the same way.**
A class re-derived from ``cliff_hz`` through the detector-space buckets
here is systematically one tier higher than a legacy stored
``spectral_bitrate_kbps`` produced by the old encoder-space
``LAME_LOWPASS`` table. Comparing across derivations overstates whichever
side was re-derived — the mechanism behind the live Fall 2007 upgrade loop
(evidence id 34219, request 8902, Iron & Wine — *Fall 2007*:
``cliff_hz=16500`` re-derives to the 160 class while its own stored legacy
value is 128). So both-from-``cliff_hz`` compares, both-from-stored-bucket
compares, and a mixed pair does not.

**Cross-codec comparison is licensed only in ``cliff_hz`` basis.** The
measured 98% MP3<->Vorbis ordering accuracy was obtained on classes derived
through each codec's OWN ladder; it says nothing about LAME-bucketed legacy
values read as if they were Vorbis classes. The legacy bucket is the LAME
table's output whatever the codec — faithful for MP3, a known
one-directional over-estimate for Vorbis ("q4's real 128 kbps read as
est-192") — so an MP3-legacy vs Vorbis-legacy pair compares table bias, not
content. Same-codec legacy pairs stay comparable: they share the bias.

This rule is LIVE, not theoretical. Five prod rows resolve to a Vorbis
measured subject through ``format`` — evidence ids 33935, 33941, 33942,
33943, 33974, all ``cliff_hz`` NULL, stored buckets 96/128/192/192/128,
four of them ``likely_transcode`` and so authorized. The two carrying 192
are the documented LAME over-read of Vorbis being promoted into a Vorbis
class, which is precisely the pair this rule refuses. (A prod query
grouping on the raw codec string reports these as ``ogg`` and misses
them.)

Reading the resolution ladder against live data: rows with
``spectral_subject IS NULL`` and ``was_converted_from`` set number 14 on
prod, and all 14 carry no spectral grade at all — so the "not converted"
branch that shape lands in is never reached by an interpretation. Recorded
here so it does not get re-litigated.
"""

from collections.abc import Sequence
from dataclasses import dataclass
from functools import cache
from typing import Literal

from lib.quality.evidence_types import (
    CODEC_FAMILY_AAC,
    CODEC_FAMILY_LOSSLESS,
    CODEC_FAMILY_MP3,
    CODEC_FAMILY_OPUS,
    CODEC_FAMILY_OTHER,
    CODEC_FAMILY_VORBIS,
    EVIDENCE_SUBJECT_SOURCE,
    SPECTRAL_TRANSCODE_GRADES,
    AudioQualityMeasurement,
    CodecFamily,
    EvidenceSubject,
)
from lib.quality.filetypes import SOX_NATIVE_AUDIO_EXTENSIONS

# ---------------------------------------------------------------------------
# Measured constants. Four arms, 60,102 measurements. Do not adjust.
# ---------------------------------------------------------------------------

#: MP3 detector-space buckets: ``(exclusive upper cliff Hz, nominal kbps
#: class)``, ascending. DETECTOR space, not encoder-lowpass space —
#: ``detect_cliff`` reports the first slice of the steep run, roughly one
#: tier below the encoder's actual lowpass, which is why the shipped
#: ``LAME_LOWPASS`` table systematically under-rates MP3s (CBR-192 buckets
#: as 160 for 75% of tracks through the spec-derived table). Measured
#: medians: CBR-96 -> 14500, 128 -> 15500, 160 -> 16500, 192 -> 18000,
#: 224/256 -> 19000, 320 -> 19500.
MP3_DETECTOR_CLASS_BUCKETS: tuple[tuple[int, int], ...] = (
    (15000, 96),
    (16000, 128),
    (17250, 160),
    (18250, 192),
    (19250, 256),
)
#: Class for an MP3 cliff at or above the last bucket boundary.
MP3_TOP_CLASS_KBPS = 320

#: Vorbis q0-q4 detector-space buckets, same shape as the MP3 table. The
#: source-extracted ladder replicated EXACTLY on all four arms (q0 14500,
#: q2 16000, q3 17000, q4 18500). q5+ is ~85% invisible in the production
#: window, so for those a missing cliff asserts nothing — which is already
#: this module's universal no-cliff rule.
VORBIS_DETECTOR_CLASS_BUCKETS: tuple[tuple[int, int], ...] = (
    (15250, 64),
    (16500, 96),
    (17750, 112),
    (19000, 128),
)
#: Class for a Vorbis cliff at or above the last bucket boundary.
VORBIS_TOP_CLASS_KBPS = 160

#: An AAC cliff below this is junk-class and asserts no floor at all.
AAC_FLOOR_JUNK_BELOW_HZ = 13000
#: An AAC cliff at or above this lifts the content floor.
AAC_FLOOR_LIFT_AT_HZ = 18500
#: The only floor a 13-18.5 kHz AAC cliff supports. Cliffs in that band are
#: produced by encoder rates from 96 all the way to 320 kbps, so the single
#: honest assertion is the bottom of that range.
AAC_FLOOR_LOW_CLASS_KBPS = 96
#: The floor a >=18.5 kHz AAC cliff supports (~190 class).
AAC_FLOOR_HIGH_CLASS_KBPS = 190


# ---------------------------------------------------------------------------
# Vocabulary
# ---------------------------------------------------------------------------

#: Which regime a codec family's spectral evidence belongs to.
SpectralSemantics = Literal[
    "ladder",                 # invertible cliff Hz -> kbps class (mp3, vorbis)
    "content_floor",          # one-sided lower bound only (aac)
    "lossless_authenticity",  # fake-lossless detector, never a class
    "audit_only",             # asserts nothing about quality
]

#: Where an inferred class came from. Classes are comparable only against
#: classes with the SAME basis.
SpectralDerivationBasis = Literal["cliff_hz", "stored_bucket", "none"]

#: Which field the measured subject's codec family was resolved from.
MeasuredCodecBasis = Literal[
    "codec_family",         # PR1 capture; already the measured subject's family
    "format",               # non-converted row: the measured codec label
    "storage_format",       # non-converted row: the stored codec label
    "was_converted_from",   # converted row: the measured SOURCE's format
    "mixed_album",          # album spans codec families — fail closed
    "conflicting_labels",   # format and storage_format disagree — fail closed
    "unresolved",           # nothing usable — fail closed
]

SpectralInterpretationReason = Literal[
    "ladder_class_from_cliff",
    "ladder_class_from_stored_bucket",
    "ladder_grade_not_transcode",
    "ladder_stored_value_not_a_bucket",
    "ladder_no_evidence",
    "aac_content_floor_low",
    "aac_content_floor_high",
    "aac_cliff_below_measurable_floor",
    "aac_no_cliff",
    "opus_no_spectral_signal",
    "lossless_transcode_grade",
    "lossless_grade_not_transcode",
    "uncalibrated_codec_family",
    "unknown_codec_family",
    "mixed_codec_album",
]

SpectralComparabilityReason = Literal[
    "comparable_same_derivation",
    "left_not_decision_grade",
    "right_not_decision_grade",
    "mixed_derivation_basis",
    "cross_codec_legacy_bucket",
]

#: Which decoder produced a spectral measurement. NOT a cosmetic detail:
#: ``ultrasonic_deficit_db`` is not comparable across the two, so the
#: proof leg's threshold is scoped by it (issue #829 Phase 5 plan §1.5c).
SpectralDecodePath = Literal["sox_native", "ffmpeg_resampled"]
SPECTRAL_DECODE_PATH_SOX_NATIVE: SpectralDecodePath = "sox_native"
SPECTRAL_DECODE_PATH_FFMPEG_RESAMPLED: SpectralDecodePath = (
    "ffmpeg_resampled"
)

REASON_LADDER_CLASS_FROM_CLIFF: SpectralInterpretationReason = (
    "ladder_class_from_cliff"
)
REASON_LADDER_CLASS_FROM_STORED_BUCKET: SpectralInterpretationReason = (
    "ladder_class_from_stored_bucket"
)
REASON_LADDER_GRADE_NOT_TRANSCODE: SpectralInterpretationReason = (
    "ladder_grade_not_transcode"
)
REASON_LADDER_STORED_VALUE_NOT_A_BUCKET: SpectralInterpretationReason = (
    "ladder_stored_value_not_a_bucket"
)
REASON_LADDER_NO_EVIDENCE: SpectralInterpretationReason = "ladder_no_evidence"
REASON_AAC_CONTENT_FLOOR_LOW: SpectralInterpretationReason = (
    "aac_content_floor_low"
)
REASON_AAC_CONTENT_FLOOR_HIGH: SpectralInterpretationReason = (
    "aac_content_floor_high"
)
REASON_AAC_CLIFF_BELOW_MEASURABLE_FLOOR: SpectralInterpretationReason = (
    "aac_cliff_below_measurable_floor"
)
REASON_AAC_NO_CLIFF: SpectralInterpretationReason = "aac_no_cliff"
REASON_OPUS_NO_SPECTRAL_SIGNAL: SpectralInterpretationReason = (
    "opus_no_spectral_signal"
)
REASON_LOSSLESS_TRANSCODE_GRADE: SpectralInterpretationReason = (
    "lossless_transcode_grade"
)
REASON_LOSSLESS_GRADE_NOT_TRANSCODE: SpectralInterpretationReason = (
    "lossless_grade_not_transcode"
)
REASON_UNCALIBRATED_CODEC_FAMILY: SpectralInterpretationReason = (
    "uncalibrated_codec_family"
)
REASON_UNKNOWN_CODEC_FAMILY: SpectralInterpretationReason = (
    "unknown_codec_family"
)
REASON_MIXED_CODEC_ALBUM: SpectralInterpretationReason = "mixed_codec_album"

#: The two codec families whose ladder is invertible, i.e. whose inferred
#: class may participate in a comparison.
LADDER_CODEC_FAMILIES: frozenset[CodecFamily] = frozenset(
    {CODEC_FAMILY_MP3, CODEC_FAMILY_VORBIS}
)


# ---------------------------------------------------------------------------
# Inputs and results
#
# These are constructed from our own typed Python and never cross a JSON
# wire, so ``@dataclass`` is correct here, not ``msgspec.Struct``
# (.claude/rules/code-quality.md, "Wire-boundary types").
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class SpectralEvidenceFacts:
    """The evidence fields codec-aware spectral interpretation reads.

    Field names deliberately mirror the persisted columns so PR2b's adapter
    is a straight field copy: ``spectral_grade`` / ``codec_family`` /
    ``spectral_subject`` / ``was_converted_from`` / ``format`` /
    ``cliff_hz`` / ``spectral_bitrate_kbps`` come from
    ``AudioQualityMeasurement``, and ``storage_format`` / ``filetype_band``
    from ``AlbumQualityEvidence``.

    ``spectral_grade`` is production's spectral VERDICT and is required
    input, not decoration — see ``interpret_spectral_cliff``.
    """

    spectral_grade: str | None = None
    codec_family: CodecFamily | None = None
    spectral_subject: EvidenceSubject | None = None
    was_converted_from: str | None = None
    format: str | None = None
    storage_format: str | None = None
    filetype_band: str = ""
    cliff_hz: int | None = None
    spectral_bitrate_kbps: int | None = None


@dataclass(frozen=True)
class SpectralCodecContext:
    """The codec-resolution context a flat ``(grade, bitrate)`` pair lacks.

    ``full_pipeline_decision`` is a flat-kwargs simulator: its world is
    described by scalars, and the scalars it already carries
    (``spectral_grade`` / ``spectral_bitrate`` / the format hint) cannot say
    which codec produced the measurement. This carries exactly the rest —
    one keyword per side instead of six — and ``facts()`` recombines it with
    the flat scalars so there is never a second source of truth for the
    grade, the stored bucket or the format label.

    Every field is consumed by ``resolve_measured_codec_family``,
    ``interpret_spectral_cliff`` or the ultrasonic proof leg.

    The last three are the proof-leg facts (issue #829 Phase 5 PR3). They
    are NOT passed to ``facts()``: codec interpretation does not read
    them, and a field that reaches an interpreter that ignores it is the
    inert plumbing PR3 deleted elsewhere in this module.
    ``spectral_decode_path`` is resolved by the adapter that holds the
    real containers (``resolve_spectral_decode_path``), never re-derived
    from the format label a decision path may have defaulted.
    """

    codec_family: CodecFamily | None = None
    cliff_hz: int | None = None
    filetype_band: str = ""
    storage_format: str | None = None
    spectral_subject: EvidenceSubject | None = None
    was_converted_from: str | None = None
    ultrasonic_deficit_db: float | None = None
    spectral_measurement_version: int | None = None
    spectral_decode_path: SpectralDecodePath | None = None

    def interpret(
        self,
        measurement: "AudioQualityMeasurement | None",
    ) -> "SpectralInterpretation":
        """Interpret a measurement through THIS context's album-level facts.

        The one way to combine a captured context with the measurement it
        describes. A caller that holds a context must never fall back to
        ``interpret_measurement`` on the bare measurement: the context is
        exactly the extra evidence (``storage_format``, ``filetype_band``)
        that can fail a mixed-codec album closed, so dropping it resolves a
        codec production withheld.
        """
        if measurement is None:
            return interpret_spectral_evidence(self.facts(
                spectral_grade=None, spectral_bitrate_kbps=None, format=None,
            ))
        return interpret_spectral_evidence(self.facts(
            spectral_grade=measurement.spectral_grade,
            spectral_bitrate_kbps=measurement.spectral_bitrate_kbps,
            format=measurement.format,
        ))

    def facts(
        self,
        *,
        spectral_grade: str | None,
        spectral_bitrate_kbps: int | None,
        format: str | None,
    ) -> SpectralEvidenceFacts:
        """Recombine this context with the flat measured scalars."""
        return SpectralEvidenceFacts(
            spectral_grade=spectral_grade,
            codec_family=self.codec_family,
            spectral_subject=self.spectral_subject,
            was_converted_from=self.was_converted_from,
            format=format,
            storage_format=self.storage_format,
            filetype_band=self.filetype_band,
            cliff_hz=self.cliff_hz,
            spectral_bitrate_kbps=spectral_bitrate_kbps,
        )


@dataclass(frozen=True)
class MeasuredCodecFamilyResolution:
    """Which codec family the spectral measurement actually describes.

    ``family is None`` is an explicit unknown — fail closed, never a guess.
    """

    family: CodecFamily | None
    basis: MeasuredCodecBasis


@dataclass(frozen=True)
class SpectralInterpretation:
    """What one album's spectral evidence asserts, in its own codec's terms.

    ``invertible_ladder`` and ``floor_only`` are properties of the CODEC
    (does this family have an invertible ladder at all / is any class it
    yields a one-sided floor). ``decision_grade`` is a property of THIS
    interpretation: a class was actually inferred on an invertible ladder,
    so it may be compared. A ladder codec with no cliff and no stored
    bucket is ``invertible_ladder=True, decision_grade=False`` — no cliff
    asserts nothing.

    ``supports_transcode_accusation`` is the one-way valve issue #829
    exists to install: it is False for every AAC, Opus and HE-AAC
    interpretation no matter what was measured.
    """

    codec_family: CodecFamily | None
    semantics: SpectralSemantics
    inferred_class_kbps: int | None
    decision_grade: bool
    invertible_ladder: bool
    floor_only: bool
    supports_transcode_accusation: bool
    basis: SpectralDerivationBasis
    reason: SpectralInterpretationReason


@dataclass(frozen=True)
class SpectralComparability:
    """Whether two interpretations' classes may be compared at all."""

    comparable: bool
    reason: SpectralComparabilityReason


# ---------------------------------------------------------------------------
# Measured-subject codec resolution
# ---------------------------------------------------------------------------

#: Container / codec labels that resolve to exactly one measured family.
#: ``format`` and ``storage_format`` carry mixed case and trailing
#: qualifiers live ("AAC", "ALAC", "MP3", "opus 128", "mp3 v0"), so a label
#: is lowercased and reduced to its first whitespace token before lookup.
_FORMAT_TOKEN_TO_FAMILY: dict[str, CodecFamily] = {
    "mp3": CODEC_FAMILY_MP3,
    "aac": CODEC_FAMILY_AAC,
    "opus": CODEC_FAMILY_OPUS,
    "vorbis": CODEC_FAMILY_VORBIS,
    "flac": CODEC_FAMILY_LOSSLESS,
    "wav": CODEC_FAMILY_LOSSLESS,
    "wave": CODEC_FAMILY_LOSSLESS,
    "alac": CODEC_FAMILY_LOSSLESS,
    "aiff": CODEC_FAMILY_LOSSLESS,
    "aif": CODEC_FAMILY_LOSSLESS,
    "ape": CODEC_FAMILY_LOSSLESS,
    "wma": CODEC_FAMILY_OTHER,
}

#: Bare containers that name no codec. ``m4a``/``mp4`` is AAC or ALAC;
#: ``ogg``/``oga`` is Vorbis or Opus. There is no file to probe at decision
#: time, so these fail closed to unknown rather than guessing — guessing is
#: precisely the codec-blind bug class issue #829 exists to fix.
_AMBIGUOUS_FORMAT_TOKENS: frozenset[str] = frozenset(
    {"m4a", "mp4", "ogg", "oga"}
)

#: Derived ``filetype_band`` values that span codec families
#: (``lib/quality_evidence.py::derive_filetype_band``). ``mixed_lossless``
#: is deliberately absent: flac + wav + alac in one album is still exactly
#: one family (``lossless``) with one set of semantics.
_MIXED_FILETYPE_BANDS: frozenset[str] = frozenset({"mixed", "mixed_lossy"})


def _normalise_format_token(label: str | None) -> str | None:
    """Reduce a live format label to its bare lowercase codec token."""
    if label is None:
        return None
    stripped = label.strip().lower()
    if not stripped:
        return None
    token = stripped.split()[0].lstrip(".")
    return token or None


def _family_from_label(label: str | None) -> CodecFamily | None:
    """Resolve one format/container label to a measured codec family.

    Returns None for an empty, ambiguous or unrecognised label.
    """
    token = _normalise_format_token(label)
    if token is None or token in _AMBIGUOUS_FORMAT_TOKENS:
        return None
    return _FORMAT_TOKEN_TO_FAMILY.get(token)


def is_mixed_codec_album(filetype_band: str) -> bool:
    """True when the fileset spans codec families.

    A comma-joined band ("m4a, mp3", "flac, m4a") comes straight from the
    slskd filetype string; ``mixed``/``mixed_lossy`` are the derived bands.
    Such an album has no single codec family AND its spectral grade was
    averaged across codecs, so it fails closed regardless of what any
    format label says.
    """
    band = (filetype_band or "").strip().lower()
    if not band:
        return False
    return "," in band or band in _MIXED_FILETYPE_BANDS


#: Bare container tokens sox decodes natively, derived from the ONE
#: routing table in ``lib/quality/filetypes.py`` (which
#: ``lib/spectral_check.py`` also consumes). Deriving rather than
#: restating is load-bearing: the router and the ultrasonic threshold's
#: scope must never be able to drift apart.
_SOX_NATIVE_CONTAINER_TOKENS: frozenset[str] = frozenset(
    ext.lstrip(".") for ext in SOX_NATIVE_AUDIO_EXTENSIONS
)


def spectral_decode_path_for_container(
    label: str | None,
) -> SpectralDecodePath | None:
    """Which decoder ``lib/spectral_check.py`` routes this container to.

    ``None`` is an explicit unknown — an empty, ambiguous or unrecognised
    label. ``m4a``/``mp4``/``ogg``/``oga`` are NOT ambiguous for this
    question the way they are for codec family: ``analyze_track`` routes
    purely on the file extension, so ``.ogg`` is sox-native whether it
    holds Vorbis or Opus and ``.m4a`` is ffmpeg-routed whether it holds
    AAC or ALAC. Only a label naming no container at all is unknown.
    """
    token = _normalise_format_token(label)
    if token is None:
        return None
    if token in _SOX_NATIVE_CONTAINER_TOKENS:
        return SPECTRAL_DECODE_PATH_SOX_NATIVE
    if token in _FORMAT_TOKEN_TO_FAMILY or token in _AMBIGUOUS_FORMAT_TOKENS:
        # A container we recognise and sox cannot open natively: the
        # analyzer decodes it through ``_ffmpeg_to_wav`` at 48kHz.
        return SPECTRAL_DECODE_PATH_FFMPEG_RESAMPLED
    return None


def resolve_spectral_decode_path(
    *,
    spectral_subject: EvidenceSubject | None,
    was_converted_from: str | None,
    container_labels: Sequence[str],
) -> SpectralDecodePath | None:
    """Which decode path produced this row's spectral measurement.

    The container axis of the same measured-subject ladder
    ``resolve_measured_codec_family`` walks, and it needs its own walk:
    ``codec_family`` short-circuits that ladder, and one family spans
    several containers with different decoders (``lossless`` covers
    sox-native ``.flac``/``.wav`` AND ffmpeg-routed ``.m4a`` ALAC). The
    PR1-captured rows that carry an ``ultrasonic_deficit_db`` at all are
    exactly the rows that short-circuit, so reusing the family resolution
    would answer "unknown" for every row this matters for.

    Two cases, mirroring the family ladder:

    * A converted row wearing its SOURCE's spectral under R19
      (``spectral_subject='source'`` and ``was_converted_from`` set) was
      measured on the SOURCE container, not on the files now on disk.
      This is the common case: 15,399 of 15,547 live proofs carry
      ``spectral_provenance='carried'``.
    * Otherwise the measured subject IS the snapshot, so its own file
      containers answer. They must all agree; a mixed fileset is an
      explicit unknown rather than a first-file guess.

    ``None`` means "cannot be established" and is the fail-closed answer.
    """
    if spectral_subject == EVIDENCE_SUBJECT_SOURCE and was_converted_from:
        return spectral_decode_path_for_container(was_converted_from)
    paths: set[SpectralDecodePath | None] = {
        spectral_decode_path_for_container(label)
        for label in container_labels
    }
    if len(paths) != 1:
        return None
    return next(iter(paths))


def resolve_measured_codec_family(
    facts: SpectralEvidenceFacts,
) -> MeasuredCodecFamilyResolution:
    """Resolve the codec family of what was actually MEASURED.

    The codec that matters is the codec of the measured subject, not the
    codec the evidence row describes. 6,193 live rows are ``codec=opus,
    was_converted_from=flac, spectral_subject=source`` — an Opus copy
    correctly wearing its source FLAC's spectral evidence under R19
    (``lib/current_library_evidence.py::preserve_existing_source_spectral``).
    Keying off the row's own codec would discard the largest lossless-lineage
    evidence cohort in the library and score it on the wrong semantics.

    Ladder, in order:

    1. A mixed-codec album fails closed FIRST, whatever else is present.
    2. ``codec_family`` (PR1 capture) — already the measured subject's
       family by construction.
    3. Otherwise, when the row IS converted (``spectral_subject='source'``
       and ``was_converted_from`` set): ``was_converted_from`` ONLY.
       ``format`` describes the derivative, not the measured source — 95
       live rows read ``codec=opus, was_converted_from=m4a, format=opus``.
    4. Otherwise (not converted): ``format``, then ``storage_format``.
       Both describe the same bytes here, so labels resolving to different
       families are a data anomaly and fail closed. That last rule is a
       deliberate tightening beyond the Phase 5 plan's ladder; it is
       strictly fail-closed, so it can only ever withhold an opinion.
    5. Otherwise unknown.
    """
    if is_mixed_codec_album(facts.filetype_band):
        return MeasuredCodecFamilyResolution(None, "mixed_album")

    if facts.codec_family is not None:
        return MeasuredCodecFamilyResolution(facts.codec_family, "codec_family")

    converted = (
        facts.spectral_subject == EVIDENCE_SUBJECT_SOURCE
        and facts.was_converted_from is not None
    )
    if converted:
        source_family = _family_from_label(facts.was_converted_from)
        if source_family is None:
            return MeasuredCodecFamilyResolution(None, "unresolved")
        return MeasuredCodecFamilyResolution(source_family, "was_converted_from")

    from_format = _family_from_label(facts.format)
    from_storage = _family_from_label(facts.storage_format)
    if (
        from_format is not None
        and from_storage is not None
        and from_format != from_storage
    ):
        return MeasuredCodecFamilyResolution(None, "conflicting_labels")
    if from_format is not None:
        return MeasuredCodecFamilyResolution(from_format, "format")
    if from_storage is not None:
        return MeasuredCodecFamilyResolution(from_storage, "storage_format")
    return MeasuredCodecFamilyResolution(None, "unresolved")


# ---------------------------------------------------------------------------
# Per-codec interpretation
# ---------------------------------------------------------------------------


def ladder_class_kbps(codec_family: CodecFamily, cliff_hz: int) -> int | None:
    """Nominal kbps class for a cliff on an invertible ladder.

    Returns None for a codec family that has no ladder — AAC's cliff is a
    content floor and Opus's carries no signal at all, so neither may be
    read through a class ladder.
    """
    if codec_family == CODEC_FAMILY_MP3:
        buckets, top = MP3_DETECTOR_CLASS_BUCKETS, MP3_TOP_CLASS_KBPS
    elif codec_family == CODEC_FAMILY_VORBIS:
        buckets, top = VORBIS_DETECTOR_CLASS_BUCKETS, VORBIS_TOP_CLASS_KBPS
    else:
        return None
    for upper_hz, class_kbps in buckets:
        if cliff_hz < upper_hz:
            return class_kbps
    return top


def aac_content_floor_kbps(cliff_hz: int) -> int | None:
    """One-sided content floor for an AAC cliff, or None when it asserts none.

    Never a bitrate and never an accusation: cliffs in 13-18.5 kHz are
    native AAC behaviour across every encoder family measured, and a cliff
    below 13 kHz is junk-class, which supports no lower bound at all.

    Two judgement calls neither the findings doc nor the Phase 5 plan
    settles, recorded so they are visible rather than inferred:

    * **The floor value is 96, the BOTTOM of the measured "96-128 class"
      band.** The same 13-18 kHz cliffs are produced by encoder rates from
      96 all the way to 320 kbps, so the only assertion the evidence
      supports is ``>= 96``.
    * **``[18000, 18500)`` routes to the LOW floor.** The measured
      statements are "13000-18000 floors at the 96-128 class" and
      ">=18500 lifts to ~190"; the 500 Hz between them is unmeasured. It
      has not earned the lift, so it does not get it — the conservative
      direction understates quality, which is the recoverable error.
    """
    if cliff_hz < AAC_FLOOR_JUNK_BELOW_HZ:
        return None
    if cliff_hz >= AAC_FLOOR_LIFT_AT_HZ:
        return AAC_FLOOR_HIGH_CLASS_KBPS
    return AAC_FLOOR_LOW_CLASS_KBPS


def _audit_only(
    codec_family: CodecFamily | None,
    reason: SpectralInterpretationReason,
) -> SpectralInterpretation:
    """An interpretation that asserts nothing about quality."""
    return SpectralInterpretation(
        codec_family=codec_family,
        semantics="audit_only",
        inferred_class_kbps=None,
        decision_grade=False,
        invertible_ladder=False,
        floor_only=False,
        supports_transcode_accusation=False,
        basis="none",
        reason=reason,
    )


def _positive(value: int | None) -> int | None:
    """A stored bucket is usable only when it is a real positive class."""
    if value is None or value <= 0:
        return None
    return value


@cache
def _lame_bucket_values() -> frozenset[int]:
    """The only classes a legacy stored bucket can legitimately hold.

    Sourced from ``lib/spectral_check.py::LAME_LOWPASS`` rather than
    restated as a literal, the same discipline as ``_grade_authorizes``
    reading ``SPECTRAL_TRANSCODE_GRADES``. ``estimate_bitrate_from_cliff``
    returns ONLY values from that table, so no legitimate legacy class can
    fall outside this set; 112 and 224 are included because they are real
    table entries a legacy row could carry even though today's writer
    never emits them.

    The import is deferred and memoised because ``lib.spectral_check``
    imports ``lib.quality``. A module-level import here would close that
    into a cycle whose resolution depended on the order of this package's
    re-exports in ``__init__`` — the same reason
    ``spectral_check.codec_family_from_extension`` defers its own imports.
    """
    from lib.spectral_check import LAME_LOWPASS

    return frozenset(class_kbps for _lowpass_hz, class_kbps in LAME_LOWPASS)


def _grade_authorizes(spectral_grade: str | None) -> bool:
    """Whether production's spectral verdict authorizes a spectral finding.

    This is the SAME gate the importer already applies —
    ``lib/quality/dispatch_actions.py::compute_effective_override_bitrate``
    admits the spectral bitrate only for ``SPECTRAL_TRANSCODE_GRADES``
    (``suspect`` / ``likely_transcode``) and ignores it for
    ``genuine`` / ``marginal`` / ``error`` / ``None`` — reading the same
    frozenset rather than restating the policy, so this module cannot
    drift from it (``.claude/rules/code-quality.md``, "Quality decisions
    live in ONE place").

    ``spectral_grade`` is the album verdict over BOTH detector legs, the
    cliff and the HF deficit (``lib/spectral_check.py::classify_track`` /
    ``classify_album``). Reconstructing a narrower verdict from cliff
    presence alone silently drops every deficit-only detection — 890 live
    lossless rows are graded ``likely_transcode``/``suspect`` with no
    ``cliff_hz`` and no ``spectral_bitrate_kbps`` at all.
    """
    return spectral_grade in SPECTRAL_TRANSCODE_GRADES


def interpret_spectral_cliff(
    codec_family: CodecFamily | None,
    *,
    spectral_grade: str | None,
    cliff_hz: int | None = None,
    stored_bitrate_kbps: int | None = None,
) -> SpectralInterpretation:
    """Interpret one album's spectral evidence in its own codec's terms.

    ``spectral_grade`` is REQUIRED, keyword-only, and has no default: it is
    production's spectral verdict and this module re-uses it rather than
    reconstructing one. ``_grade_authorizes`` owns the rule and its
    provenance. Consequences:

    * a class is inferred only when the grade authorizes it — from
      ``cliff_hz`` as well as from the stored bucket, because a
      ``genuine`` album verdict means the album-level decision already
      rejected the minority cliff that produced that ``cliff_hz``;
    * ``supports_transcode_accusation`` tracks the grade for the families
      whose spectral evidence is admissible (mp3 / vorbis / lossless), and
      is hard-False for every family where #829 proved it is not (aac,
      opus, HE-AAC, ``other``, unknown).

    ``stored_bitrate_kbps`` is the legacy ``spectral_bitrate_kbps`` column
    — usually the old codec-blind LAME-table bucket, but not always. It is
    used ONLY as a fallback class for an authorized ladder codec that has
    no raw ``cliff_hz`` and whose stored value IS a ``LAME_LOWPASS``
    member. The no-cliff case is the overwhelming majority of rows:
    ``cliff_hz`` capture only began with PR1, so only a small and growing
    minority carry it. **The stored value never becomes a class for AAC,
    Opus, HE-AAC, ``other`` or an unknown family** — that is exactly the
    download-37946 defect.
    """
    if codec_family is None:
        return _audit_only(None, REASON_UNKNOWN_CODEC_FAMILY)

    if codec_family == CODEC_FAMILY_LOSSLESS:
        # Unchanged semantics, deliberately. The cliff is the fake-lossless
        # detector and this module never derives a kbps class for it. A
        # legacy lossless row records its cliff only as a stored bucket, so
        # either field counts as cliff presence.
        #
        # The detector is armed by the GRADE, never by cliff presence:
        # evidence 33735 is a FLAC graded ``likely_transcode`` with
        # ``cliff_hz`` NULL and ``spectral_bitrate_kbps`` NULL — caught by
        # the HF-deficit leg alone. Reading cliff presence here would
        # disarm 890 such live rows.
        flagged = _grade_authorizes(spectral_grade)
        return SpectralInterpretation(
            codec_family=codec_family,
            semantics="lossless_authenticity",
            inferred_class_kbps=None,
            decision_grade=False,
            invertible_ladder=False,
            floor_only=False,
            supports_transcode_accusation=flagged,
            basis="none",
            reason=(
                REASON_LOSSLESS_TRANSCODE_GRADE
                if flagged
                else REASON_LOSSLESS_GRADE_NOT_TRANSCODE
            ),
        )

    if codec_family in LADDER_CODEC_FAMILIES:
        # ``decision_grade`` is DERIVED from "a class was actually
        # inferred", never asserted from the family alone.
        #
        # The raw cliff is FINAL when present: it is deliberately not
        # possible to fall through to the legacy bucket behind it. If
        # ``LADDER_CODEC_FAMILIES`` and ``ladder_class_kbps`` ever drift
        # apart, a cliffed row on the drifted family yields NO class
        # rather than laundering a LAME-shaped bucket into that codec's
        # class space — which is the download-37946 defect's exact shape.
        # (Unreachable today: the two constants list the same two
        # families.)
        inferred: int | None = None
        basis: SpectralDerivationBasis = "none"
        authorized = _grade_authorizes(spectral_grade)
        stored = _positive(stored_bitrate_kbps)
        # The stored column does not always hold a LAME bucket: 2,503 of
        # 30,251 live rows carry a container bitrate there instead, up to
        # 738 (evidence 5144 is one). The grade gate removes 5,120 of
        # them; this allowlist closes the rest of the boundary. The
        # ``cliff_hz`` path is deliberately unguarded — a cliff is a raw
        # measurement, not a bucket.
        stored_is_bucket = stored is not None and stored in _lame_bucket_values()
        if cliff_hz is not None:
            if authorized:
                inferred = ladder_class_kbps(codec_family, cliff_hz)
                basis = "cliff_hz" if inferred is not None else "none"
        elif authorized and stored_is_bucket:
            inferred = stored
            basis = "stored_bucket"

        reason: SpectralInterpretationReason
        if basis == "cliff_hz":
            reason = REASON_LADDER_CLASS_FROM_CLIFF
        elif basis == "stored_bucket":
            reason = REASON_LADDER_CLASS_FROM_STORED_BUCKET
        elif (cliff_hz is not None or stored is not None) and not authorized:
            # Evidence exists but production's album verdict does not
            # authorize a spectral finding from it. Named separately from
            # "nothing was measured" so PR4 can say which it was.
            reason = REASON_LADDER_GRADE_NOT_TRANSCODE
        elif cliff_hz is None and stored is not None:
            # Authorized (the branch above claimed the other case) and no
            # cliff, so the only blocker left is that the stored value is
            # not a bucket at all.
            reason = REASON_LADDER_STORED_VALUE_NOT_A_BUCKET
        else:
            reason = REASON_LADDER_NO_EVIDENCE
        return SpectralInterpretation(
            codec_family=codec_family,
            semantics="ladder",
            inferred_class_kbps=inferred,
            decision_grade=inferred is not None,
            invertible_ladder=True,
            floor_only=False,
            # Admissible family, so the accusation tracks the grade — NOT
            # the presence of a class. A deficit-only ``suspect`` MP3 is
            # still flagged by production even though no cliff gives it a
            # class; the lossless branch above makes the same point.
            supports_transcode_accusation=authorized,
            basis=basis,
            reason=reason,
        )

    if codec_family == CODEC_FAMILY_AAC:
        # The stored legacy bucket is deliberately NOT consulted here: it
        # is the LAME table's output, and reading it as an AAC class is
        # the download-37946 defect itself.
        #
        # ``spectral_grade`` is deliberately NOT consulted either, and the
        # asymmetry against the ladder branch is intentional. The floor is
        # not a decision input — ``decision_grade`` and
        # ``supports_transcode_accusation`` are hard-False on every path
        # below, so it can neither order two albums nor accuse one. Gating
        # it on the grade would make the floor available only when
        # production had already made the mistaken AAC accusation that
        # #829 exists to remove: the grade for an AAC album is derived
        # from cliffs the calibration proves are NATIVE behaviour, so it
        # is exactly the input that must not be trusted here.
        floor = None if cliff_hz is None else aac_content_floor_kbps(cliff_hz)
        if floor is None:
            aac_reason: SpectralInterpretationReason = (
                REASON_AAC_NO_CLIFF
                if cliff_hz is None
                else REASON_AAC_CLIFF_BELOW_MEASURABLE_FLOOR
            )
        elif floor == AAC_FLOOR_HIGH_CLASS_KBPS:
            aac_reason = REASON_AAC_CONTENT_FLOOR_HIGH
        else:
            aac_reason = REASON_AAC_CONTENT_FLOOR_LOW
        return SpectralInterpretation(
            codec_family=codec_family,
            semantics="content_floor",
            inferred_class_kbps=floor,
            # A floor is a one-sided lower bound: it never orders two
            # albums and it never accuses one.
            decision_grade=False,
            invertible_ladder=False,
            floor_only=True,
            supports_transcode_accusation=False,
            basis="none" if floor is None else "cliff_hz",
            reason=aac_reason,
        )

    if codec_family == CODEC_FAMILY_OPUS:
        return _audit_only(codec_family, REASON_OPUS_NO_SPECTRAL_SIGNAL)

    return _audit_only(codec_family, REASON_UNCALIBRATED_CODEC_FAMILY)


def interpret_spectral_evidence(
    facts: SpectralEvidenceFacts,
) -> SpectralInterpretation:
    """Resolve the measured subject's codec family, then interpret."""
    resolution = resolve_measured_codec_family(facts)
    if resolution.family is None:
        return _audit_only(
            None,
            REASON_MIXED_CODEC_ALBUM
            if resolution.basis == "mixed_album"
            else REASON_UNKNOWN_CODEC_FAMILY,
        )
    return interpret_spectral_cliff(
        resolution.family,
        spectral_grade=facts.spectral_grade,
        cliff_hz=facts.cliff_hz,
        stored_bitrate_kbps=facts.spectral_bitrate_kbps,
    )


# ---------------------------------------------------------------------------
# Comparability
# ---------------------------------------------------------------------------


def spectral_classes_comparable(
    left: SpectralInterpretation,
    right: SpectralInterpretation,
) -> SpectralComparability:
    """Whether two interpretations' inferred classes may be compared.

    Symmetric in its arguments except for which side the refusal names.
    Comparable only when all three hold:

    1. Both sides are decision-grade — an invertible ladder that actually
       inferred a class.
    2. Both derived that class the same way. A class re-derived from
       ``cliff_hz`` sits systematically one tier above a legacy stored
       bucket, so a mixed pair measures derivation, not quality.
    3. Either both sides are the same codec family, OR the shared basis is
       ``cliff_hz``. **Cross-codec comparison is licensed only in
       ``cliff_hz`` basis**: the measured 98% MP3<->Vorbis ordering
       accuracy was obtained on classes derived through each codec's OWN
       ladder, and says nothing about LAME-bucketed legacy values read as
       if they were Vorbis classes. The legacy bucket is faithful for MP3
       and a known one-directional over-estimate for Vorbis ("q4's real
       128 kbps read as est-192"), so weighing one against the other
       compares table bias, not content. Live on prod, not theoretical:
       five rows resolve to a Vorbis measured subject through ``format``
       (evidence ids 33935, 33941, 33942, 33943, 33974), two of them
       carrying the over-read 192 bucket. See the module docstring.

    Anything else withholds the comparison — unknown, not equal — and the
    caller falls through to rank and the other evidence.
    """
    if not left.decision_grade:
        return SpectralComparability(False, "left_not_decision_grade")
    if not right.decision_grade:
        return SpectralComparability(False, "right_not_decision_grade")
    if left.basis != right.basis:
        return SpectralComparability(False, "mixed_derivation_basis")
    if left.basis != "cliff_hz" and left.codec_family != right.codec_family:
        return SpectralComparability(False, "cross_codec_legacy_bucket")
    return SpectralComparability(True, "comparable_same_derivation")


def codec_context_from_measurement(
    measurement: AudioQualityMeasurement | None,
    *,
    storage_format: str | None = None,
    filetype_band: str = "",
    container_labels: Sequence[str] = (),
) -> SpectralCodecContext:
    """Lift a measurement's codec-resolution fields into a context.

    The adapter from a persisted ``AudioQualityMeasurement`` (plus the
    album-level fields that live on ``AlbumQualityEvidence``) to the one
    keyword ``full_pipeline_decision`` takes per side.

    ``container_labels`` are the snapshot's own file containers. They
    answer the ultrasonic proof leg's decode-path question for a row whose
    spectral describes the files on disk; a converted row wearing its
    source's spectral is answered by ``was_converted_from`` instead. A
    caller that has no snapshot passes none, and the decode path stays an
    explicit unknown — fail closed.
    """
    if measurement is None:
        return SpectralCodecContext(
            filetype_band=filetype_band,
            storage_format=storage_format,
        )
    return SpectralCodecContext(
        codec_family=measurement.codec_family,
        cliff_hz=measurement.cliff_hz,
        filetype_band=filetype_band,
        storage_format=storage_format,
        spectral_subject=measurement.spectral_subject,
        was_converted_from=measurement.was_converted_from,
        ultrasonic_deficit_db=measurement.ultrasonic_deficit_db,
        spectral_measurement_version=(
            measurement.spectral_measurement_version
        ),
        spectral_decode_path=resolve_spectral_decode_path(
            spectral_subject=measurement.spectral_subject,
            was_converted_from=measurement.was_converted_from,
            container_labels=container_labels,
        ),
    )


def interpret_measurement(
    measurement: AudioQualityMeasurement | None,
    *,
    storage_format: str | None = None,
    filetype_band: str = "",
) -> SpectralInterpretation:
    """Interpret an ``AudioQualityMeasurement``'s own spectral fields.

    The decision-path entry point (issue #829 Phase 5 PR2b). Every field
    the measurement carries is a straight copy; ``storage_format`` and
    ``filetype_band`` live on ``AlbumQualityEvidence``, so a caller that
    holds the evidence row passes them and a caller that only holds a
    measurement does not (the measurement's own ``format`` already carries
    the storage-format fallback wherever the pipeline resolved one).

    ``measurement is None`` — the "no existing album" shape — interprets as
    an unknown family, i.e. it asserts nothing.
    """
    if measurement is None:
        return _audit_only(None, REASON_UNKNOWN_CODEC_FAMILY)
    return interpret_spectral_evidence(SpectralEvidenceFacts(
        spectral_grade=measurement.spectral_grade,
        codec_family=measurement.codec_family,
        spectral_subject=measurement.spectral_subject,
        was_converted_from=measurement.was_converted_from,
        format=measurement.format,
        storage_format=storage_format,
        filetype_band=filetype_band,
        cliff_hz=measurement.cliff_hz,
        spectral_bitrate_kbps=measurement.spectral_bitrate_kbps,
    ))


def decision_class_kbps(interpretation: SpectralInterpretation) -> int | None:
    """The spectral class that may participate in a decision, else None.

    The ONE accessor every decision seam uses instead of reading a raw
    ``spectral_bitrate_kbps``. ``None`` means the spectral leg withholds its
    opinion — the caller falls through to rank and the other evidence, which
    is never a rejection and never an accusation.

    A ``content_floor`` (AAC) interpretation carries an
    ``inferred_class_kbps`` that is a LOWER bound, the opposite direction to
    every clamp in the decision path, and is deliberately never
    decision-grade — so it never reaches a caller through here.
    """
    return (
        interpretation.inferred_class_kbps
        if interpretation.decision_grade
        else None
    )
