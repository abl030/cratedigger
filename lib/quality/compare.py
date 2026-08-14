"""Pairwise quality comparison (compare_quality) and format-hint helpers.

Extracted verbatim from the monolithic ``lib/quality.py`` (issue #477).
Pure move: every definition is AST-identical to the original.
"""


from lib.quality.evidence_types import (
    SPECTRAL_TRANSCODE_GRADES,
    AudioQualityMeasurement,
    QualityComparisonBasis,
    TargetQualityContract,
    V0ProbeEvidence,
)
from lib.quality.ranks import (
    QualityRank,
    QualityRankConfig,
    _codec_family_of,
    _parse_bitrate_label,
    _parse_vbr_level,
    _selected_bitrate,
    _selected_bitrate_with_source,
    _selected_quality_bitrate_with_source,
    measurement_rank,
    quality_rank,
)
from lib.quality.spectral_interpretation import (
    SpectralInterpretation,
    decision_class_kbps,
    interpret_measurement,
    spectral_classes_comparable,
)


def _rank_gap_is_within_tolerance(
    *,
    new_value: int | None,
    existing_value: int | None,
    new_format: str | None,
    existing_format: str | None,
    either_spectral_bound: bool,
    cfg: QualityRankConfig,
) -> bool:
    """Whether a RANK difference is too small to be a real quality difference.

    ``quality_rank`` is a step function evaluated with NO tolerance, and it
    decides first — ``cfg.within_rank_tolerance_kbps`` only ever ran in the
    same-rank ``metric_tiebreak`` below. That was survivable while the MP3
    ladder's edges were 245/210/170/130, and the reason is the CANDIDATE
    side, not the installed one. Both cliffs need a pair: an installed album
    just below an edge AND a candidate on it. Measured on the 2026-08-14
    library (1,101 measured all-MP3 albums): 817 of them average EXACTLY a
    collapsed edge — 614 at 320, 127 at 192, 45 at 128, 31 at 256 — while
    only 5 land exactly on a retired VBR edge. Issue #1145 moved every cliff
    onto the nominal values three quarters of the population sits on, so the
    pairing went from vanishingly rare to routine. (The installed half was
    always populated: 38 albums sit 1-5 kbps below a collapsed edge, and 75
    sat below a retired one — so "the old edges fell between the common
    bitrates" is NOT the explanation, and measuring it was what showed that.)
    Each such pair is a full replace + ``beet move`` + media-server churn for
    a difference no listener can hear — the dl 39947 failure this series
    began with, re-entering by a different door.

    So the tolerance is applied to the rank comparison too, under exactly the
    conditions that make the two values comparable:

    * **Neither side's spectral clamp actually BOUND.** A bound side's value
      is a spectral CLASS, not a measured rate; letting a ±5 window cancel a
      rank the clamp produced would weaken the clamp in both directions —
      suppressing a demotion it earned, or suppressing an upgrade over an
      album it demoted.

      The test is ``either`` bound rather than "a shared clamp exists"
      because those are different questions: when both sides carry spectral
      evidence and NEITHER estimate falls below its own raw metric, both
      clamped values simply ARE the raw metrics, and the window is as
      applicable there as anywhere. **This is fail-closed legislation for a
      world the corpus does not yet hold, not a live branch.** Measured
      2026-08-14 by running this function over every candidate/current pair
      in the rebuilt live corpus (17,096 pairs): 22 ``rank_within_tolerance``
      firings, all 22 unclamped, **ZERO with a shared clamp**. An independent
      count over a wider pair enumeration reached 31 firings and the same
      zero; the total depends on which pairs you admit, the zero does not,
      and the zero is the load-bearing half. The coarser gate would move no
      live row today; it is
      written this way so that a future album carrying spectral evidence on
      both sides — which the clamp path is steadily making more common — does
      not silently fall out of the guard for a reason that has nothing to do
      with why the guard exists.
    * **Same codec family**, so the two numbers mean the same thing. A
      cross-family rank difference is real (``cross_family_same_rank`` only
      ever fires at EQUAL rank).
    * **Both sides bare codec labels.** An explicit label's rank ignores the
      measured bitrate entirely, so a kbps delta says nothing about it.
    * **Both values present.**

    The window is the configured one, so a genuinely larger gap still
    promotes on rank: 320-vs-200 remains an upgrade. Under every shipped band
    table the narrowest edge spacing (16 kbps, Opus and Vorbis) is wider than
    the default 5 kbps window, so a qualifying gap straddles at most one edge;
    a config that narrowed a band below the tolerance would not break the
    rule, only widen what it cancels.
    """
    if either_spectral_bound:
        return False
    if new_value is None or existing_value is None:
        return False
    if _codec_family_of(new_format) != _codec_family_of(existing_format):
        return False
    if _is_explicit_label(new_format) or _is_explicit_label(existing_format):
        return False
    return abs(new_value - existing_value) <= cfg.within_rank_tolerance_kbps


def _is_explicit_label(format_hint: str | None) -> bool:
    """True if format_hint carries an explicit quality contract (VBR or bitrate).

    "mp3 v0" / "opus 128" / "mp3 320" are contracts. "MP3" / "Opus" / "FLAC"
    are bare codec names from beets items.format. Within the same rank tier,
    a contract + anything is equivalent — only bare-vs-bare compares on bitrate.
    """
    if format_hint is None:
        return False
    if _parse_vbr_level(format_hint) is not None:
        return True
    return _parse_bitrate_label(format_hint) is not None


def comparison_format_hint(
    *,
    explicit_format: str | None = None,
    target_format: str | None = None,
    verified_lossless_target: str | None = None,
    converted_count: int = 0,
    is_transcode: bool = False,
    verified_lossless_proof: bool = False,
    native_codec_family: str | None = None,
) -> str | None:
    """Format hint to use for the pre-import quality comparison.

    This keeps production import_one.py and the simulator on the same rules.
    An unproven transcode-grade source keeps the pessimistic temporary-MP3
    projection, but a proof-bearing source compares the configured bytes that
    would actually end up on disk.  The proof only authorizes that output
    projection; the configured target's own quality rank still governs whether
    it may replace the installed album.
    """
    if explicit_format is not None:
        return explicit_format
    if target_format in ("flac", "lossless"):
        return "flac"
    if (
        converted_count > 0
        and verified_lossless_proof
        and verified_lossless_target is not None
    ):
        return verified_lossless_target
    if converted_count > 0 and not is_transcode:
        return verified_lossless_target or "mp3 v0"
    if converted_count > 0:
        return "MP3"
    return native_codec_family


# Probed-codec / extension → native-lossy rank-model format label. Only codecs
# with a lossy rank band table are mapped; everything else returns None.
_NATIVE_CODEC_LABELS: dict[str, str] = {
    "opus": "opus",
    "aac": "aac",
    "vorbis": "vorbis",
    "wma": "wma",
    "wmav1": "wma",
    "wmav2": "wma",
    "wmapro": "wma",
    "wmavoice": "wma",
    "mp3": "MP3",
    "mp3float": "MP3",
}
_NATIVE_EXT_LABELS: dict[str, str] = {
    "opus": "opus",
    "aac": "aac",
    "m4a": "aac",
    "wma": "wma",
    "mp3": "MP3",
}


def native_codec_format_label(
    codec: str | None, ext: str | None = None
) -> str | None:
    """Map a probed codec name (or file-extension fallback) to the native-lossy
    ``AudioQualityMeasurement.format`` label the rank model keys on.

    Returns a label ``_codec_family_of`` recognises (for example ``"opus"``,
    ``"vorbis"``, ``"wma"``, or ``"MP3"``), or None for codecs with no lossy
    rank band. The probed codec name wins over the extension — an Opus stream
    in an ``.ogg`` container is "opus", not vorbis.

    This is the fix for the Opus-recorded-as-MP3 bug: native lossy downloads
    used to be hardcoded to "MP3", so a genuine Opus 124 was scored on the
    MP3-VBR band table and rejected as a downgrade against an MP3 128.
    """
    def _norm(value: str | None) -> str | None:
        return (value or "").strip().lower().lstrip(".") or None

    codec_norm = _norm(codec)
    if codec_norm is not None:
        # A probed codec name is authoritative — if it has no lossy band we
        # return None rather than guessing from the (possibly generic)
        # container extension.
        return _NATIVE_CODEC_LABELS.get(codec_norm)

    ext_norm = _norm(ext)
    if ext_norm is not None:
        return _NATIVE_EXT_LABELS.get(ext_norm)
    return None


def _shared_spectral_bitrates(
    new: AudioQualityMeasurement,
    existing: AudioQualityMeasurement,
    cfg: QualityRankConfig,
    *,
    new_v0_probe: V0ProbeEvidence | None = None,
    new_spectral: SpectralInterpretation,
    existing_spectral: SpectralInterpretation,
) -> tuple[int | None, int | None, bool, bool] | None:
    """Return rank-bucket bitrates when both sides carry COMPARABLE classes.

    The clamp takes ``min(selected_metric, inferred_class)`` per side — the
    codec-aware spectral class becomes an upper bound on the rank bucket.
    Same-bucket tie-breaks still use the raw configured bitrate metric in
    ``compare_quality()``; otherwise an equal spectral floor would erase a
    real avg-bitrate upgrade and stop the pipeline from grinding upward when
    spectral analysis is too pessimistic.

    Issue #829 Phase 5 PR2b replaced "both sides carry a raw
    ``spectral_bitrate_kbps``" with ``spectral_classes_comparable``. The old
    test admitted an ordinary AAC's natural rolloff — read through LAME's
    MP3 encoder table as "128" — into a cross-codec clamp against an MP3
    (download 37946). The comparability rule additionally refuses a pair
    whose classes were derived differently (a ``cliff_hz`` re-derivation sits
    systematically one tier above a legacy stored bucket) and a cross-codec
    pair in stored-bucket basis. Every refusal WITHHOLDS the clamp — the
    caller falls through to rank and the other evidence, which is never a
    rejection.

    Still deliberately *narrow*: a stale estimate on only one side
    (Springsteen shape: existing CBR 320 genuine+96, new MP3 V0 240 no
    spectral) keeps the container comparison — the rule that
    ``test_springsteen_genuine_but_96kbps`` pins. The one asymmetric case
    that does bind is ``_candidate_spectral_bound`` below, which exists for
    the opposite shape.

    NO LONGER grade-tolerant, and that is a deliberate reversal. This clamp
    used to fire on any two estimates, on the theory that two independent
    measurements agreeing is corroborating evidence (Eno case,
    ``download_log.id=3291``). The four-arm calibration measured what those
    estimates are on an album production already graded ``genuine``:
    natural rolloff. Since PR2b a class exists only when the album verdict
    authorizes a spectral finding (``_grade_authorizes``, the same
    ``SPECTRAL_TRANSCODE_GRADES`` gate ``compute_effective_override_bitrate``
    always applied), so two ``genuine`` albums are no longer clamped at all
    and their raw metrics decide. The caller still guards the asymmetric
    case where a transcode-grade candidate would otherwise use a higher
    spectral floor to replace a non-transcode-grade existing album with a
    higher real quality rank.

    Returns ``(new_value, existing_value, new_spectral_bound,
    existing_spectral_bound)`` — the two ``*_spectral_bound`` flags tell the
    caller which side's returned value IS the spectral class (clamp bound,
    ``class <= raw``) versus which is still the untouched raw metric (clamp
    did not bind, ``class > raw``). Those flags gate the same-rank
    ``spectral_tiebreak`` (issue #813 Finding 1) — a like-for-like tiebreak
    needs BOTH sides holding a class, not one class against one raw metric.

    That is now their ONLY consumer: issue #1145 deleted
    ``_classify_with_cbr_bands``. The MP3 class ladder is a set of nominal
    kbps values (96/112/128/160/192/224/256/320) and ``QualityRankConfig.mp3``
    draws its four thresholds from that same ladder, so a spectral-bound value
    now classifies through the one table like everything else. There is no
    second, more generous MP3 table left to inflate it, so the "force CBR
    bands when both sides are bound" rule has nothing left to force.

    Being the only consumer is why the gate is pinned directly rather than
    left to prose: ``tests/test_quality_decisions.py``
    ``::test_one_bound_side_never_reaches_the_spectral_tiebreak`` drives the
    asymmetric world, and
    ``tests/test_mp3_ladder_generated.py::TestSpectralTiebreakIsGatedGenerated``
    patrols it. Two mutants are recorded against them — pinning the flag
    ``True`` at the assignment, and dropping it from the guard — and each is
    killed by both the pin and the property.
    """
    if not spectral_classes_comparable(new_spectral, existing_spectral).comparable:
        return None
    new_class = decision_class_kbps(new_spectral)
    existing_class = decision_class_kbps(existing_spectral)
    if new_class is None or existing_class is None:
        # Unreachable: comparability requires both sides decision-grade, and
        # a decision-grade interpretation always carries a class. Fail closed
        # rather than assert — withholding is always safe.
        return None
    new_br = _selected_quality_bitrate_with_source(new, cfg, new_v0_probe)[0]
    existing_br = _selected_bitrate(existing, cfg)
    new_bound = new_br is None or new_class <= new_br
    existing_bound = existing_br is None or existing_class <= existing_br
    new_value = new_class if new_bound else new_br
    existing_value = existing_class if existing_bound else existing_br
    return new_value, existing_value, new_bound, existing_bound


def _candidate_spectral_bound(
    new: AudioQualityMeasurement,
    existing: AudioQualityMeasurement,
    cfg: QualityRankConfig,
    *,
    new_format: str | None,
    new_v0_probe: V0ProbeEvidence | None,
    new_spectral: SpectralInterpretation,
    existing_spectral: SpectralInterpretation,
) -> int | None:
    """Bound a transcode candidate by its own class against a known-clean HAVE.

    The Fall 2007 anti-loop (issue #911, folded into #829 Phase 5 PR2b —
    request 8902, Iron & Wine *Fall 2007*, evidence id 34219). A candidate
    MP3 CBR 320 carrying a decision-grade transcode class re-derives from
    ``cliff_hz=16500`` to the 160 class, but its RAW 320 container
    manufactures a ``transparent`` rank and displaces a genuine MP3 CBR 160
    that has no spectral bitrate at all. Later the genuine 160 displaces it
    back, and the request loops forever.

    ``_shared_spectral_bitrates`` cannot reach this: the clean HAVE has no
    class (a ``genuine`` verdict authorizes none), so there is nothing to
    compare symmetrically. ``_transcode_candidate_real_rank_regresses``
    cannot reach it either: the candidate's RAW rank is *higher*, which is
    exactly the manufactured claim. So this is the one asymmetric bound in
    the comparison — narrowly gated:

    * the candidate's interpretation is decision-grade AND supports a
      transcode accusation (an invertible ladder whose album verdict
      authorized a spectral finding). AAC, Opus and HE-AAC can never
      satisfy this, by construction in ``interpret_spectral_cliff``;
    * the current copy is KNOWN non-transcode — it has an affirmative
      spectral grade that is not a transcode grade. A HAVE that was never
      measured is not "known clean" and keeps today's container comparison;
    * the current copy contributes no class of its own (implied by the
      grade, asserted anyway so the symmetric clamp always wins);
    * the candidate's format is a bare measured codec, not an explicit
      contract label — a contract's rank ignores measured bitrate entirely,
      so a bound would be a claim the rank never consumes;
    * the bound actually binds (``class <= raw metric``).

    Returns the bounded value, or None when any gate declines. The caller
    then decides on RANK ALONE — see ``compare_quality``.
    """
    if not (new_spectral.decision_grade and new_spectral.supports_transcode_accusation):
        return None
    if _is_explicit_label(new_format):
        return None
    # The HAVE's grade is read RAW, deliberately. For an AAC this PR
    # declares the grade meaningless as a CLASS — its cliff is native
    # behaviour — yet a ``genuine`` AAC still counts as "known non-transcode"
    # here. That asymmetry is a choice, not leftover codec-blindness:
    # tightening it (demanding the HAVE's own interpretation admit an
    # accusation) would make this bound fire MORE often, i.e. reject more
    # candidates, and the conservative direction is to keep the bound narrow.
    # A ``genuine`` verdict is also the one thing the grade says that no
    # codec calibration contradicts — the calibration says AAC cliffs cannot
    # convict, never that they falsely acquit.
    existing_grade = existing.spectral_grade
    if existing_grade is None or existing_grade in SPECTRAL_TRANSCODE_GRADES:
        return None
    if decision_class_kbps(existing_spectral) is not None:
        return None
    new_class = decision_class_kbps(new_spectral)
    if new_class is None:
        return None
    new_br = _selected_quality_bitrate_with_source(new, cfg, new_v0_probe)[0]
    if new_br is not None and new_class > new_br:
        return None
    return new_class


def _transcode_candidate_real_rank_regresses(
    new: AudioQualityMeasurement,
    existing: AudioQualityMeasurement,
    cfg: QualityRankConfig,
    *,
    new_target_contract: TargetQualityContract | None = None,
    new_v0_probe: V0ProbeEvidence | None = None,
) -> bool:
    """Whether a transcode-grade candidate is lower real rank than existing.

    Shared spectral floors are useful supporting evidence, but they must not
    launder a lower-rank transcode over a higher-rank non-transcode existing
    album. Compare the real configured measurement rank before the spectral
    clamp for that asymmetric grade transition only.
    """
    if new.spectral_grade not in SPECTRAL_TRANSCODE_GRADES:
        return False
    if existing.spectral_grade is None:
        return False
    if existing.spectral_grade in SPECTRAL_TRANSCODE_GRADES:
        return False
    return measurement_rank(
        new,
        cfg,
        target_contract=new_target_contract,
        v0_probe=new_v0_probe,
    ) < measurement_rank(existing, cfg)


def compare_quality(
    new: AudioQualityMeasurement,
    existing: AudioQualityMeasurement,
    cfg: QualityRankConfig,
    *,
    new_target_contract: TargetQualityContract | None = None,
    new_v0_probe: V0ProbeEvidence | None = None,
    new_spectral: SpectralInterpretation | None = None,
    existing_spectral: SpectralInterpretation | None = None,
) -> QualityComparisonBasis:
    """Codec-aware quality comparison.

    Primary key is the QualityRank. Within the same rank:
    - LOSSLESS → always "equivalent" (bitrate variance has no quality meaning).
    - Different codec families → "equivalent" (Opus 128 vs MP3 V0 are
      perceptually indistinguishable at the TRANSPARENT band).
    - Same codec family, either side carries an explicit label ("mp3 v0" /
      "opus 128" / "mp3 320") → "equivalent". Labels are quality contracts
      and within the same rank tier are perceptually equivalent regardless of
      bitrate deltas (a 207 kbps V0 on lo-fi and a 245 kbps V0 on dense material
      are both TRANSPARENT — this is the lo-fi genuine V0 case).
    - Same codec family, both bare codec names → compare the configured metric
      with cfg.within_rank_tolerance_kbps tolerance.

    Shared-spectral bucket: when BOTH measurements carry ``spectral_bitrate_kbps``,
    clamp each side's classified bitrate to ``min(selected_metric, spectral)``
    for rank. When the clamp binds on BOTH sides (the clamped value on each
    side IS its spectral estimate, not its raw metric) and the clamped
    values land in the SAME rank but still differ from each other, that
    difference decides the same-rank tiebreak directly (branch
    ``spectral_tiebreak``) — otherwise the coarse rank band could bucket
    two genuinely unequal spectral readings together and the fully
    unclamped raw metric would decide instead, which can reverse a real
    spectral ordering (issue #813 Finding 1). When only one (or neither)
    side is spectral-bound, that "both bound" requirement intentionally
    withholds this tiebreak — comparing a spectral estimate against a raw
    configured metric with no tolerance is not a like-for-like comparison,
    and it falls through to the raw-metric tiebreak below instead. Only a
    TRUE spectral tie (clamped values EQUAL, both bound) also falls through
    to the raw configured metric, so higher-average files can still replace
    lower-average files within an identical spectral bucket — this keeps
    spectral as a demotion signal without letting a pessimistic estimate
    permanently freeze the album at the first source that happened to land
    in that bucket (Mark DeNardo request 1308). See
    ``_shared_spectral_bitrates`` for the narrow guard that keeps the
    Springsteen case (single stale estimate) on the container path. A
    transcode-grade candidate over a non-transcode-grade existing album has
    one extra guard before any of this: if its real selected-metric rank is
    lower before the spectral clamp, it is worse.

    Returns a ``QualityComparisonBasis`` — the verdict plus the branch that
    fired and the values that decided it, emitted HERE per-branch so the
    persisted explanation can never disagree with the decision (the request
    6039 lesson: any re-derivation outside this function eventually lies).
    Callers that only need the verdict read ``.verdict``.

    ``new_spectral`` / ``existing_spectral`` carry each side's codec-aware
    ``SpectralInterpretation`` (issue #829 Phase 5 PR2b). They default to
    ``interpret_measurement`` over the measurement's own fields — the
    measurement carries ``codec_family``/``cliff_hz``/``spectral_grade``, so
    every caller is codec-aware without threading. Callers that hold the
    whole ``AlbumQualityEvidence`` row pass an interpretation built with the
    album-level context the measurement cannot carry (``filetype_band``'s
    mixed-codec fail-closed), which can only ever WITHHOLD more.

    Pure function. No I/O, no hardcoded numbers — every threshold comes from cfg.
    """
    if new_spectral is None:
        new_spectral = interpret_measurement(new)
    if existing_spectral is None:
        existing_spectral = interpret_measurement(existing)
    new_br, new_metric = _selected_quality_bitrate_with_source(
        new, cfg, new_v0_probe
    )
    existing_br, existing_metric = _selected_bitrate_with_source(existing, cfg)
    new_format = (
        new_target_contract.format
        if new_target_contract is not None
        else new.format
    )

    def _truthful_display_value(
        measurement: AudioQualityMeasurement,
        metric: str,
        value: int | None,
    ) -> tuple[str, int | None]:
        """Name the evidence that actually classified one side.

        Explicit labels are encoder/storage contracts. Their rank ignores the
        measured bitrate, so persisting ``min 191k`` beside ``opus 128`` lies:
        191k may be a temporary V0 proxy. Numeric contracts retain their
        declared value for machine-readable audit; V-level contracts need no
        synthetic kbps value because the format label is the complete fact.
        """
        format_hint = (
            new_format if measurement is new else measurement.format
        )
        if _is_explicit_label(format_hint):
            declared = (
                _parse_bitrate_label(format_hint)
                if format_hint is not None else None
            )
            return "contract", declared
        return metric, value

    def _basis(
        verdict: str,
        branch: str,
        new_rank: QualityRank,
        existing_rank: QualityRank,
        new_value: int | None = None,
        existing_value: int | None = None,
        spectral_clamped: bool = False,
        tolerance_kbps: int | None = None,
    ) -> QualityComparisonBasis:
        display_new_metric, display_new_value = _truthful_display_value(
            new, new_metric, new_value,
        )
        display_existing_metric, display_existing_value = (
            _truthful_display_value(
                existing, existing_metric, existing_value,
            )
        )
        return QualityComparisonBasis(
            verdict=verdict,
            branch=branch,
            new_rank=new_rank.name.lower(),
            existing_rank=existing_rank.name.lower(),
            new_metric=display_new_metric,
            existing_metric=display_existing_metric,
            new_value_kbps=display_new_value,
            existing_value_kbps=display_existing_value,
            # Lowercase-normalized: the hint's casing differs between the
            # simulator and evidence twins ("flac" vs "FLAC") while meaning
            # the same thing — display upper-cases, parity compares.
            new_format=new_format.lower() if new_format else None,
            existing_format=existing.format.lower() if existing.format else None,
            spectral_clamped=spectral_clamped,
            tolerance_kbps=tolerance_kbps,
        )

    if _transcode_candidate_real_rank_regresses(
        new,
        existing,
        cfg,
        new_target_contract=new_target_contract,
        new_v0_probe=new_v0_probe,
    ):
        return _basis(
            "worse", "transcode_rank_regression",
            measurement_rank(
                new,
                cfg,
                target_contract=new_target_contract,
                v0_probe=new_v0_probe,
            ), measurement_rank(existing, cfg),
            new_value=new_br, existing_value=existing_br,
        )

    shared = _shared_spectral_bitrates(
        new, existing, cfg,
        new_v0_probe=new_v0_probe,
        new_spectral=new_spectral,
        existing_spectral=existing_spectral,
    )
    if shared is None:
        # Fall 2007 anti-loop (issue #911): a transcode candidate whose own
        # class is decision-grade, weighed against a known-clean HAVE that
        # carries no class. The bound decides on RANK ALONE — the raw
        # same-rank tiebreak below would re-admit the very container bitrate
        # the class has already contradicted, which is the loop. Import only
        # when the bounded rank is STRICTLY better than the current raw rank.
        bounded_new_br = _candidate_spectral_bound(
            new, existing, cfg,
            new_format=new_format,
            new_v0_probe=new_v0_probe,
            new_spectral=new_spectral,
            existing_spectral=existing_spectral,
        )
        if bounded_new_br is not None:
            bound_new_rank = quality_rank(new_format, bounded_new_br, cfg)
            bound_existing_rank = measurement_rank(existing, cfg)
            if bound_new_rank > bound_existing_rank:
                bound_verdict = "better"
            elif bound_new_rank < bound_existing_rank:
                bound_verdict = "worse"
            else:
                bound_verdict = "equivalent"
            return _basis(
                bound_verdict, "spectral_candidate_bound",
                bound_new_rank, bound_existing_rank,
                new_value=bounded_new_br, existing_value=existing_br,
                spectral_clamped=True,
            )
        new_rank = measurement_rank(
            new,
            cfg,
            target_contract=new_target_contract,
            v0_probe=new_v0_probe,
        )
        existing_rank = measurement_rank(existing, cfg)
        rank_new_value, rank_existing_value = new_br, existing_br
        spectral_clamped = False
        both_spectral_bound = False
        either_spectral_bound = False
    else:
        clamped_new_br, clamped_existing_br, new_bound, existing_bound = shared
        # ``both_spectral_bound`` survives the #1145 ladder collapse: it no
        # longer picks a band table (there is one per family now), but it
        # still gates the same-rank ``spectral_tiebreak`` below, which is only
        # like-for-like when BOTH clamped values ARE spectral classes. That
        # branch is its only remaining reader — see ``_shared_spectral_bitrates``
        # for the pin and the property that hold it up.
        both_spectral_bound = new_bound and existing_bound
        either_spectral_bound = new_bound or existing_bound
        new_rank = quality_rank(new_format, clamped_new_br, cfg)
        existing_rank = quality_rank(existing.format, clamped_existing_br, cfg)
        rank_new_value, rank_existing_value = clamped_new_br, clamped_existing_br
        spectral_clamped = True

    # A rank difference the configured tolerance says is not a difference.
    # This runs BEFORE the rank short-circuit because rank is a no-tolerance
    # step function that would otherwise never let ``metric_tiebreak`` see
    # these two values at all (issue #1145 H2).
    if new_rank != existing_rank and _rank_gap_is_within_tolerance(
        new_value=rank_new_value,
        existing_value=rank_existing_value,
        new_format=new_format,
        existing_format=existing.format,
        either_spectral_bound=either_spectral_bound,
        cfg=cfg,
    ):
        return _basis(
            "equivalent", "rank_within_tolerance", new_rank, existing_rank,
            new_value=rank_new_value, existing_value=rank_existing_value,
            spectral_clamped=spectral_clamped,
            tolerance_kbps=cfg.within_rank_tolerance_kbps,
        )

    if new_rank > existing_rank:
        return _basis(
            "better", "rank", new_rank, existing_rank,
            new_value=rank_new_value, existing_value=rank_existing_value,
            spectral_clamped=spectral_clamped,
        )
    if new_rank < existing_rank:
        return _basis(
            "worse", "rank", new_rank, existing_rank,
            new_value=rank_new_value, existing_value=rank_existing_value,
            spectral_clamped=spectral_clamped,
        )

    # Same rank. UNKNOWN has no orderable quality evidence, so measured
    # bitrate cannot turn one unmapped codec into an upgrade over another.
    # Keep the existing ``metric_missing`` basis vocabulary: the metrics are
    # deliberately not comparable for this rank even when byte probes found
    # numeric bitrates.
    if new_rank == QualityRank.UNKNOWN:
        return _basis(
            "equivalent", "metric_missing", new_rank, existing_rank,
            spectral_clamped=spectral_clamped,
        )

    # LOSSLESS is always equivalent — FLAC bitrates vary with sample rate and
    # bit depth, not quality.
    if new_rank == QualityRank.LOSSLESS:
        return _basis(
            "equivalent", "lossless_same_rank", new_rank, existing_rank,
            spectral_clamped=spectral_clamped,
        )

    new_family = _codec_family_of(new_format)
    existing_family = _codec_family_of(existing.format)

    # Different codec families at the same rank: perceptually equivalent.
    if new_family != existing_family:
        return _basis(
            "equivalent", "cross_family_same_rank", new_rank, existing_rank,
            new_value=new_br, existing_value=existing_br,
            spectral_clamped=spectral_clamped,
        )

    # Same codec family. If either side has an explicit label, the label is
    # authoritative — within the same rank tier they are equivalent.
    if _is_explicit_label(new_format) or _is_explicit_label(existing.format):
        return _basis(
            "equivalent", "label_contract_same_rank", new_rank, existing_rank,
            new_value=new_br, existing_value=existing_br,
            spectral_clamped=spectral_clamped,
        )

    # When the shared-spectral clamp fired, BOTH sides are spectral-bound
    # (the clamped value on each side IS the spectral estimate, not the raw
    # metric — see ``_shared_spectral_bitrates``), AND the clamped values
    # themselves still differ, that difference decides the tiebreak
    # directly — issue #813 Finding 1's remaining Stage1/Stage2
    # disagreement. The coarse QualityRank band can bucket two genuinely
    # UNEQUAL spectral estimates into the same rank (e.g. spectral 287 and
    # 317 both land in "transparent"); falling through to the fully-
    # unclamped raw metric at that point lets a worse-spectral candidate
    # win purely because its (already known-unreliable, that's why spectral
    # exists) declared container happens to be higher. The clamped values
    # are the more direct evidence and decide with the same strict
    # (no-tolerance) comparison Stage 1 (``spectral_import_decision``)
    # already uses — this is what makes the two stages agree instead of
    # disagree. Requiring BOTH bound is essential: when only one (or
    # neither) side is spectral-bound, the "clamped value" on the unbound
    # side is just its raw configured metric, and comparing a spectral
    # estimate against a raw metric with no tolerance is not a like-for-
    # like tiebreak — it silently becomes the ``metric_tiebreak`` below
    # minus its ±5 kbps tolerance. A TRUE spectral tie (clamped values
    # EQUAL) carries no differentiating signal and still falls through to
    # the raw-metric tiebreak below, exactly as before (Mark DeNardo
    # request 1308: tied spectral 128==128, raw 192 beats 128 must still
    # import).
    if (spectral_clamped
            and both_spectral_bound
            and rank_new_value is not None
            and rank_existing_value is not None
            and rank_new_value != rank_existing_value):
        verdict = "better" if rank_new_value > rank_existing_value else "worse"
        return _basis(
            verdict, "spectral_tiebreak", new_rank, existing_rank,
            new_value=rank_new_value, existing_value=rank_existing_value,
            spectral_clamped=spectral_clamped,
        )

    # Both bare codec names — compare the chosen raw metric with tolerance.
    # When the shared-spectral bucket fired, rank has already been demoted by
    # the spectral floor. The tiebreaker deliberately stays on the raw metric
    # so equal spectral buckets can still converge upward by bitrate.
    if new_br is None or existing_br is None:
        return _basis(
            "equivalent", "metric_missing", new_rank, existing_rank,
            new_value=new_br, existing_value=existing_br,
            spectral_clamped=spectral_clamped,
        )
    delta = new_br - existing_br
    verdict = (
        "equivalent" if abs(delta) <= cfg.within_rank_tolerance_kbps
        else ("better" if delta > 0 else "worse")
    )
    return _basis(
        verdict, "metric_tiebreak", new_rank, existing_rank,
        new_value=new_br, existing_value=existing_br,
        spectral_clamped=spectral_clamped,
        tolerance_kbps=cfg.within_rank_tolerance_kbps,
    )
