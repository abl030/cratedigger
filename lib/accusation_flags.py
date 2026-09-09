"""The audit-only accusation pair, and the evidence columns it reads.

Issue #829 Phase 5 PR4 put ONE derivation behind every operator surface
that paints a spectral grade: ``accusation_flags`` says whether a measured
grade may be rendered AS a transcode accusation, and which of the two
withholding worlds it is in when it may not. Three input adapters reach
it — a whole evidence row, a joined column block, a proof-gate verdict —
so two surfaces describing the same album cannot state different findings.

This block lived in ``web/classify.py`` until issue #1389. It is a
derivation over persisted evidence, not presentation, and one of its
consumers is a ``lib`` service (``lib/long_tail_service.py``), which had
to reach it through a function-local import to dodge the resulting cycle.
``web/classify.py`` keeps the rendering that reads these flags.

The ``_as_*`` narrowers below come with it: they narrow the persisted
evidence columns off a joined row, and
``web/classify.py::proof_gate_projection`` narrows its own aliases for
those same columns through the very same helpers. (The two read
different alias prefixes, not one shared block.) One owner keeps two
readers of one column set from narrowing it differently.
"""

from __future__ import annotations

from collections.abc import Mapping

import msgspec

from lib.quality import (
    SPECTRAL_TRANSCODE_GRADES,
    AlbumQualityEvidence,
    AudioQualityMeasurement,
    CodecFamily,
    EvidenceSubject,
    interpret_measurement,
)

#: WHY a grade's accusation is withheld. The two reasons are different
#: facts and only one of them may be described to the operator: an
#: audit-only family's cliff IS native encoder behaviour, whereas an
#: unresolved or mixed-codec album's cliff means nothing is known about
#: the encoder at all. Asserting the first sentence over the second world
#: fabricates a fact about a codec the pipeline could not identify.
ACCUSATION_WITHHELD_AUDIT_ONLY_CODEC = "audit_only_codec"
ACCUSATION_WITHHELD_CODEC_UNRESOLVED = "codec_unresolved"


def _accusation_withheld_reason(
    *,
    admissible: bool,
    codec_family: CodecFamily | None,
    grade: object,
) -> str | None:
    """Which of the two withholding worlds this row is in, or None.

    ``None`` whenever there is no accusation to withhold — no grade, a
    grade that does not accuse, or an admissible finding.
    """
    if grade not in SPECTRAL_TRANSCODE_GRADES:
        return None
    if admissible:
        return None
    if codec_family is None:
        return ACCUSATION_WITHHELD_CODEC_UNRESOLVED
    return ACCUSATION_WITHHELD_AUDIT_ONLY_CODEC


def _measurement_accusation_withheld_reason(
    measurement: AudioQualityMeasurement | None,
    *,
    storage_format: str | None = None,
    filetype_band: str = "",
) -> str | None:
    """``_accusation_withheld_reason`` for a caller holding a measurement."""
    if measurement is None:
        return None
    interpretation = interpret_measurement(
        measurement,
        storage_format=storage_format,
        filetype_band=filetype_band,
    )
    return _accusation_withheld_reason(
        admissible=interpretation.supports_transcode_accusation,
        codec_family=interpretation.codec_family,
        grade=measurement.spectral_grade,
    )


def _accusation_admissible(
    measurement: AudioQualityMeasurement | None,
    *,
    storage_format: str | None = None,
    filetype_band: str = "",
) -> bool | None:
    """Whether a measurement's grade may be rendered as an accusation.

    ``None`` when there is no grade, or when the grade is not one that
    could accuse anything — a ``genuine`` album has no accusation to
    withhold, and reporting "not admissible" there would read as a codec
    verdict rather than a not-applicable.

    The resolution is production's own ``interpret_measurement``, never a
    codec guessed from a format label: an Opus copy wearing its source
    FLAC's spectral under R19 must resolve to the LOSSLESS family, and a
    label-only shortcut would neutralize a real transcode finding on it.
    """
    if measurement is None:
        return None
    if measurement.spectral_grade not in SPECTRAL_TRANSCODE_GRADES:
        return None
    return interpret_measurement(
        measurement,
        storage_format=storage_format,
        filetype_band=filetype_band,
    ).supports_transcode_accusation


class AccusationFlags(msgspec.Struct, frozen=True):
    """The audit-only display pair for ONE measured spectral grade.

    Every operator surface that paints a spectral grade renders it through
    this pair (issue #829 Phase 5 PR4): ``admissible`` says whether the
    grade may be shown AS a transcode accusation, ``withheld`` says which
    of the two withholding worlds it is in when it may not. Both are
    ``None`` when there is nothing to withhold — no grade, a grade that
    does not accuse, or no evidence joined at all — and every consumer
    treats that as "render the historical accusing form", which is the
    fail-accusing direction for a display-only fact.
    """

    admissible: bool | None = None
    withheld: str | None = None


def accusation_flags(
    measurement: AudioQualityMeasurement | None,
    *,
    storage_format: str | None = None,
    filetype_band: str = "",
) -> AccusationFlags:
    """THE derivation: both audit-only flags for one measurement.

    Six operator surfaces read this one function through three input
    adapters (a whole evidence row, a joined column block, a proof-gate
    verdict). The flags and the rule behind them therefore cannot differ
    between two surfaces describing the same album, which is the same
    property ``proof_verdict_from_facts`` gives the verdict tier.
    """
    return AccusationFlags(
        admissible=_accusation_admissible(
            measurement,
            storage_format=storage_format,
            filetype_band=filetype_band,
        ),
        withheld=_measurement_accusation_withheld_reason(
            measurement,
            storage_format=storage_format,
            filetype_band=filetype_band,
        ),
    )


def evidence_accusation_flags(
    evidence: AlbumQualityEvidence | None,
) -> AccusationFlags:
    """``accusation_flags`` for a caller holding a whole evidence row.

    The storage-format and filetype-band arguments come off the evidence
    exactly as ``lib/import_preview.py`` passes them to the decider, so a
    surface reading the loaded row resolves the same codec the decision
    path did.
    """
    if evidence is None:
        return AccusationFlags()
    return accusation_flags(
        evidence.measurement,
        storage_format=evidence.storage_format,
        filetype_band=evidence.filetype_band,
    )


def evidence_column_accusation_flags(
    row: Mapping[str, object],
    *,
    prefix: str,
) -> AccusationFlags:
    """``accusation_flags`` for a caller holding a joined column block.

    ``prefix`` selects which of a row's evidence joins to read —
    ``CANDIDATE_EVIDENCE_PREFIX`` for the attempt's own candidate,
    ``CURRENT_EVIDENCE_PREFIX`` for the request's installed copy. The
    columns are projected by
    ``lib/pipeline_db/_shared.py::accusation_evidence_columns`` under the
    same two prefixes, so the SQL and this adapter cannot drift into
    reading different measurements.

    A join that matched nothing yields all-NULL columns, hence a
    measurement with no grade, hence ``AccusationFlags()`` — the surface
    keeps its historical render.
    """
    return accusation_flags(
        AudioQualityMeasurement(
            format=_as_str(row.get(f"{prefix}format")),
            spectral_grade=_as_str(row.get(f"{prefix}spectral_grade")),
            spectral_bitrate_kbps=_as_int(
                row.get(f"{prefix}spectral_bitrate")),
            spectral_subject=_as_evidence_subject(
                row.get(f"{prefix}spectral_subject")),
            was_converted_from=_as_str(
                row.get(f"{prefix}was_converted_from")),
            cliff_hz=_as_int(row.get(f"{prefix}cliff_hz")),
            codec_family=_as_codec_family(row.get(f"{prefix}codec_family")),
        ),
        storage_format=_as_str(row.get(f"{prefix}storage_format")),
        filetype_band=_as_str(row.get(f"{prefix}filetype_band")) or "",
    )


def _as_str(value: object) -> str | None:
    return value if isinstance(value, str) else None


def _as_int(value: object) -> int | None:
    return value if isinstance(value, int) and not isinstance(value, bool) else None


def _as_float(value: object) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    return float(value)


def _as_codec_family(value: object) -> CodecFamily | None:
    """Narrow a persisted ``codec_family`` column to its Literal.

    Decoded through the Literal itself rather than against a restated list
    of family names, so the vocabulary has exactly one owner. Fails CLOSED:
    an unrecognised family withholds every spectral opinion, which is the
    direction issue #829 installed. Migration 065's CHECK makes the failure
    branch unreachable in production; it exists so a future vocabulary
    change cannot reach the interpreter as an unvalidated string.
    """
    try:
        return msgspec.convert(value, type=CodecFamily)
    except msgspec.ValidationError:
        return None


def _as_evidence_subject(value: object) -> EvidenceSubject | None:
    """Narrow a persisted ``spectral_subject`` column to its Literal."""
    try:
        return msgspec.convert(value, type=EvidenceSubject)
    except msgspec.ValidationError:
        return None
