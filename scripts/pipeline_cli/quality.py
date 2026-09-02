"""pipeline-cli quality/debug commands (#495 carve).

``quality`` — simulate common download scenarios against a request's
current quality state. ``repair-spectral`` — find and fix albums stuck
by stale ``current_spectral_bitrate`` (issue #18).
"""

from __future__ import annotations

import argparse
import json
from typing import TYPE_CHECKING, Protocol, TypedDict

from lib import transitions
from lib.json_narrow import json_dict
from scripts.pipeline_cli._format import _fmt_br

if TYPE_CHECKING:
    from contextlib import AbstractContextManager

    from lib.quality import AlbumQualityEvidence, QualityRankConfig


class _LiveReplayDB(Protocol):
    """The three reads the live-candidate replay performs.

    The #409 narrow-Protocol pattern: this helper touches nothing beyond
    these three reads, and saying so is worth more than reusing the wider
    command-level port below that happens to cover them.
    """

    def get_latest_download_log_candidate_evidence_id(
        self, request_id: int,
    ) -> int | None: ...

    def load_album_quality_evidence_by_id(
        self, evidence_id: int,
    ) -> AlbumQualityEvidence | None: ...

    def get_request_current_evidence_id(
        self, request_id: int,
    ) -> int | None: ...


class _RepairCursor(Protocol):
    """The one read ``repair-spectral`` performs on a raw-SQL cursor."""

    def fetchall(self) -> list[dict[str, object]]: ...


class _QualityCommandDB(transitions.TransitionsDB, Protocol):
    """The exact pipeline-DB surface these two commands use.

    Both were annotated with the concrete ``PipelineDB`` before issue
    #1277, on the stated grounds that ``load_quality_gate_state`` took a
    nominal class a Protocol could not satisfy. That reason is gone — that
    loader now takes its own three-read ``QualityGateStateDB`` port — so
    these commands state their real surface instead: the transition engine
    (``finalize_request``), those linked-evidence reads, the IMPORT
    advisory lock ``repair-spectral`` serialises on, and the two raw
    request writes it performs. ``PipelineDB`` and ``FakePipelineDB`` both
    satisfy it structurally, so tests drive these commands with a fake and
    no ``cast``.

    ``_execute`` is declared underscore-and-all — cross-module private use
    is the house convention (PR #775), and ``repair-spectral``'s DELETE
    genuinely has no typed writer to go through.
    """

    def get_latest_download_log_candidate_evidence_id(
        self, request_id: int,
    ) -> int | None: ...

    def load_album_quality_evidence_by_id(
        self, evidence_id: int | None,
    ) -> AlbumQualityEvidence | None: ...

    def get_request_current_evidence_id(
        self, request_id: int,
    ) -> int | None: ...

    def advisory_lock(
        self, namespace: int, key: int,
    ) -> AbstractContextManager[bool]: ...

    def update_request_fields(
        self, request_id: int, **extra: object,
    ) -> bool: ...

    def _execute(
        self, sql: str, params: tuple[object, ...] = (),
    ) -> _RepairCursor: ...


class _ScenarioParams(TypedDict, total=False):
    """Candidate-side ``full_pipeline_decision`` kwargs a scenario may set."""

    is_flac: bool
    min_bitrate: int
    is_cbr: bool
    is_vbr: bool
    avg_bitrate: int
    spectral_grade: str
    spectral_bitrate: int
    converted_count: int
    post_conversion_min_bitrate: int
    post_conversion_is_cbr: bool
    candidate_v0_probe_avg: int
    candidate_v0_probe_min: int
    candidate_v0_probe_kind: str
    audio_corrupt: bool
    has_nested_audio: bool
    audio_check_mode: str

# Module-level DI seam for ``transitions.finalize_request`` — see
# ``lib.dispatch.outcome_actions.finalize_request`` for the rationale.
# Each module that calls it binds its own copy (same pattern as
# ``web.routes.pipeline_mutations.finalize_request`` / ``scripts.pipeline_cli.album_requests.finalize_request``).
finalize_request = transitions.finalize_request


def _load_runtime_rank_config() -> QualityRankConfig:
    """Load the runtime QualityRankConfig from the active config.ini."""
    from lib.config import read_runtime_rank_config

    return read_runtime_rank_config()


def _load_runtime_verified_lossless_target() -> str:
    """Load the runtime verified_lossless_target from the active config.ini."""
    from lib.config import read_verified_lossless_target

    return read_verified_lossless_target()


def _load_runtime_audio_check_mode() -> str:
    """Load the runtime audio_check_mode from the active config.ini.

    Used by the quality simulator so the preimport audio gate scenario
    reflects the deployment's `[Beets Validation] audio_check` setting
    (issue #91). On deployments with `audio_check = off`, the scenario
    shows `skipped_off` instead of `reject_corrupt`.
    """
    from lib.config import read_runtime_config

    return read_runtime_config().audio_check_mode


def _quality_preview_target_label(
    target_format: str | None,
    verified_lossless_target: str | None,
) -> str:
    """Human label for the on-disk destination used in quality previews."""
    if target_format in ("flac", "lossless"):
        return "flac"
    if verified_lossless_target:
        return verified_lossless_target
    return "V0"


def _print_decision_outcome(
    name: str,
    result: dict[str, object],
    *,
    q_override: str | None,
    gate_unavailable_reason: str | None,
) -> None:
    """Print one decision-dict outcome in the shared ``cmd_quality`` format.

    Shared by the synthetic scenario matrix and the live-candidate replay
    tier (issue #813 tooling tier) — one display path, not two.
    """
    from lib.quality import STAGE2_COUNTERFACTUAL_UNAVAILABLE, search_tiers

    imported = "IMPORT" if result["imported"] else "REJECT"
    parts = [imported]
    if result["denylisted"]:
        parts.append("denylist")
    if result["keep_searching"]:
        parts.append("keep searching")
    final = result["final_status"] or "?"
    decision_chain = " → ".join(
        f"{s}={result[s]}"
        for s in ["preimport_audio", "preimport_nested", "preimport_bad_hash",
                  "preimport_empty_fileset", "preimport_mixed_source",
                  "stage0_spectral_gate", "stage1_spectral",
                  "stage2_import", "stage3_quality_gate"]
        if result[s] is not None)

    print(f"    {name}:")
    print(f"      → {', '.join(parts)} (final: {final})")
    if decision_chain:
        print(f"      chain: {decision_chain}")

    # A Stage-1 spectral reject short-circuits before Stage 2 runs, so the
    # chain above stops at ``stage1_spectral:reject`` and says nothing about
    # whether the candidate was actually an upgrade. The decider now reports
    # that counterfactual (issue #829 Phase 5 PR2d); it is the whole point
    # of issue #813's disagreement question, so surface it here.
    counterfactual = result.get("stage2_import_if_stage1_deferred")
    if counterfactual == STAGE2_COUNTERFACTUAL_UNAVAILABLE:
        print("      if stage 1 had deferred: stage 2 could not be evaluated")
    elif isinstance(counterfactual, str) and counterfactual:
        verdict = json_dict(
            result.get("comparison_basis_if_stage1_deferred")).get("verdict")
        scored = (
            f", scoring the candidate {verdict}"
            if isinstance(verdict, str) and verdict
            else ""
        )
        print(f"      if stage 1 had deferred: stage2={counterfactual}{scored}")

    # For rejections that keep searching: simulate what happens after
    if not result["imported"] and result["keep_searching"]:
        if q_override:
            tiers, _ = search_tiers(q_override, [])
            print(f"      next search: {', '.join(tiers)}")
        elif gate_unavailable_reason is not None:
            print("      no backfill simulation (linked evidence unavailable)")
        else:
            # Importer narrowing requires an independent attempt-local
            # audit of the exact HAVE copy. Candidate spectral fields in
            # this scenario are deliberately not substituted for it.
            print("      no backfill simulation "
                  "(attempt-local HAVE audit not modeled; keep all tiers)")


def _print_proof_gate_verdict(
    side: str,
    evidence: AlbumQualityEvidence,
) -> None:
    """Print one album's proof-gate tier, fired legs and proof generation.

    Issue #829 Phase 5 PR4. Read-only display over facts the evidence row
    already carries, through the SAME derivation the web evidence panel
    uses (``proof_verdict_from_evidence`` /
    ``verified_lossless_generation_label``) so the two surfaces cannot
    state different findings for the same album.

    A fired leg is a triage signal, never an accusation: withholding a
    proof never rejects, denylists or accuses (Phase 5 plan §2).
    """
    from lib.quality import (
        SPECTRAL_TRANSCODE_GRADES,
        evidence_is_source_semantic,
        proof_tier_statement,
        proof_verdict_from_evidence,
        verified_lossless_generation_label,
    )

    verdict = proof_verdict_from_evidence(evidence)
    legs = ", ".join(verdict.fired_legs) if verdict.fired_legs else "none"
    # A tier number is meaningless when no leg adjudicated: "tier 5" over an
    # empty evaluated set would read as a clearance nothing tested for.
    tier = f"tier {verdict.tier} — " if verdict.has_finding else ""
    print(f"      proof gate {side}: {tier}"
          f"{proof_tier_statement(verdict)} (fired legs: {legs})")
    # WHICH legs ran, and how they came out. The distinction between a leg
    # that PASSED and one that WITHHELD is the whole reason those outcomes
    # are three-state: a withheld leg asserts nothing, and most of the
    # library will never have ultrasonic or lattice evidence at any price.
    print(f"      legs {side}: ultrasonic="
          f"{verdict.ultrasonic_outcome or 'not evaluated'}, "
          f"aac-lattice={verdict.aac_lattice_outcome or 'not evaluated'}")
    # The same lineage attribution gate Recents applies after its exact-release
    # identity gate. A legacy-lineage row is not always this album's own
    # snapshot — migration 021 §6b cross-walked pre-content-addressing rows
    # onto whichever content-addressed row their release already had — so an
    # ungated read attributed a sibling's proof on 4,910 live requests
    # (6,608 IN/HAVE sides) while Recents said nothing about the same album.
    # Two operator surfaces stating different proofs for one album is the
    # drift ``proof_verdict_from_evidence`` already exists to prevent.
    proof = evidence.verified_lossless_proof
    generation = (
        verified_lossless_generation_label(proof.classifier)
        if proof is not None
        and evidence_is_source_semantic(evidence.lineage_version)
        else None
    )
    if generation is not None:
        print(f"      verified lossless {side}: proved by {generation}")
        cd_rip = evidence.cd_rip_verification
        if cd_rip is not None:
            toc = cd_rip.toc
            print(
                f"      CD rip {side}: algorithm={cd_rip.algorithm}, "
                f"source={cd_rip.source_format}/{cd_rip.provenance}, "
                f"tracks={len(toc.track_offsets_sectors)}, "
                f"ARID={toc.accuraterip_id}, MB disc={toc.musicbrainz_disc_id}"
            )
            if cd_rip.accuraterip is not None:
                ar = cd_rip.accuraterip
                confidences = ",".join(
                    str(confidence) for confidence in ar.track_confidences
                )
                checksums = ",".join(
                    f"{checksum:08x}" for checksum in ar.track_checksums
                )
                print(
                    f"      AccurateRip {side}: {ar.checksum_version.upper()} "
                    f"offset={ar.read_offset_samples:+d}, "
                    f"track confidences=[{confidences}], "
                    f"track checksums=[{checksums}], provider={ar.url}, "
                    f"response-sha256={ar.response_sha256}"
                )
            if cd_rip.ctdb is not None:
                ctdb = cd_rip.ctdb
                print(
                    f"      CTDB {side}: whole-disc crc32={ctdb.crc32:08x}, "
                    f"confidence={ctdb.confidence}, entry={ctdb.entry_id}, "
                    f"response-toc={ctdb.response_toc_sectors}, "
                    f"toc-shift={ctdb.response_toc_shift_sectors}, "
                    f"provider={ctdb.url}, "
                    f"response-sha256={ctdb.response_sha256}"
                )
    grade = evidence.measurement.spectral_grade
    if not verdict.spectral_accusation_admissible and (
        grade in SPECTRAL_TRANSCODE_GRADES
    ):
        # The measured grade stays visible as the audit fact it is, but it
        # is NOT a transcode finding — the download-37946 defect (a 256 kbps
        # CBR AAC graded ``likely_transcode``). WHY it is not a finding
        # matters: an unresolved codec supports no statement about any
        # encoder, so it must not be described as native rolloff.
        reason = (
            "the codec could not be resolved, so the grade is withheld"
            if verdict.codec_family is None
            else "audit-only for this codec — not a transcode finding"
        )
        print(f"      note {side}: spectral grade {grade!r} is {reason}")


def _print_live_candidate_replay(
    db: _LiveReplayDB,
    request_id: int,
    *,
    expected_release_id: object | None,
    rank_cfg: QualityRankConfig,
    target_format: str | None,
    verified_lossless_target: str | None,
    runtime_audio_check: str,
    q_override: str | None,
    gate_unavailable_reason: str | None,
    marked_incomplete: bool = False,
) -> None:
    """Replay the request's actual last-candidate evidence through the real
    decider (issue #813 tooling tier).

    Every synthetic scenario above is a canned grade/bitrate combo — none
    reproduce the exact live candidate a real download produced, so live
    verification of a quality-decision change previously needed an offline
    decider run (PR #812's Mark DeNardo/request 1308 verification). This
    replays the SAME persisted ``AlbumQualityEvidence`` the importer itself
    would have decided from, through the SAME production decider
    (``full_pipeline_decision_from_evidence``) — never a second reimplementation.

    Read-only: loads persisted rows, decides, prints. No writes.
    """
    from lib.beets_db import exact_release_identity_matches
    from lib.quality import (
        AlbumQualityEvidenceDecisionFacts,
        full_pipeline_decision_from_evidence,
    )
    from lib.quality_evidence import candidate_evidence_for_policy

    print("\n  What the last real candidate actually decided:")

    candidate_evidence_id = db.get_latest_download_log_candidate_evidence_id(
        request_id)
    if candidate_evidence_id is None:
        print("    (no download attempt has left measured candidate "
              "evidence yet)")
        return
    candidate = db.load_album_quality_evidence_by_id(candidate_evidence_id)
    if candidate is None:
        print(f"    (candidate evidence #{candidate_evidence_id} is "
              "referenced but missing — data integrity issue)")
        return
    if not exact_release_identity_matches(
        expected_release_id, candidate.mb_release_id
    ):
        print(
            f"    (candidate evidence #{candidate_evidence_id} is referenced "
            "but its exact release identity does not match this request — "
            "ignored)"
        )
        return
    candidate = candidate_evidence_for_policy(candidate)

    current_evidence_id = db.get_request_current_evidence_id(request_id)
    current = (
        db.load_album_quality_evidence_by_id(current_evidence_id)
        if current_evidence_id is not None
        else None
    )
    if current is not None and not exact_release_identity_matches(
        expected_release_id, current.mb_release_id
    ):
        print(
            f"    (current evidence #{current_evidence_id} has a different "
            "exact release identity — ignored for this replay)"
        )
        current = None

    facts = AlbumQualityEvidenceDecisionFacts(
        audio_check_mode=runtime_audio_check,
        target_format=target_format,
        verified_lossless_target=verified_lossless_target,
    )
    m = candidate.measurement
    label = (
        f"Candidate evidence #{candidate.id} "
        f"(measured {m.format or '(unknown)'} "
        f"{_fmt_br(m.min_bitrate_kbps)}, "
        f"spectral={m.spectral_grade or 'n/a'}, "
        f"measured_at={candidate.measured_at})"
    )
    try:
        result = full_pipeline_decision_from_evidence(
            candidate, current, facts=facts, cfg=rank_cfg)
    except ValueError as exc:
        print(f"    {label}:")
        print(f"      → could not decide: {exc}")
        return
    _print_decision_outcome(
        label, result,
        q_override=q_override,
        gate_unavailable_reason=gate_unavailable_reason,
    )
    if marked_incomplete:
        # Issue #1241: the replay above shows the UNPROVEN-attempt shape
        # (extra_tracks / mbid_not_found / no_choose_match). A beets-whole
        # attempt on this operator-marked request disregards the installed
        # side; show that decision too so the mark's effect is visible.
        import msgspec

        marked_facts = msgspec.structs.replace(
            facts,
            installed_marked_incomplete=True,
            candidate_covers_declared_program=True,
        )
        marked_result = full_pipeline_decision_from_evidence(
            candidate, current, facts=marked_facts, cfg=rank_cfg)
        _print_decision_outcome(
            "Same candidate, beets-whole attempt (#1241 mark disregards "
            "the installed side)",
            marked_result,
            q_override=q_override,
            gate_unavailable_reason=gate_unavailable_reason,
        )
    _print_proof_gate_verdict("IN", candidate)
    if current is not None:
        _print_proof_gate_verdict("HAVE", current)


def cmd_quality(db: _QualityCommandDB, args: argparse.Namespace) -> None:
    """Show quality state and simulate decisions for common download scenarios."""
    from lib.dispatch import load_quality_gate_state
    from lib.quality import (
        SpectralCodecContext,
        compute_effective_override_bitrate,
        full_pipeline_decision,
        gate_rank,
        quality_gate_decision,
        rejection_backfill_override,
    )

    rank_cfg = _load_runtime_rank_config()

    req = db.get_request(args.id)
    if not req:
        print(f"  Request {args.id} not found.")
        return

    label = f"{req['artist_name']} - {req['album_title']}"
    request_min_br = req.get("min_bitrate")
    request_current_br = req.get("current_spectral_bitrate")
    q_override = req.get("search_filetype_override")
    request_spectral_grade = req.get("current_spectral_grade")
    request_final_format = req.get("final_format")
    target_format = req.get("target_format")
    verified_lossless_target = _load_runtime_verified_lossless_target() or None
    # Existing-side lossless-source V0 probe — anchors the lossless_source_locked
    # rule. When set, lossy candidates short-circuit to reject inside the
    # provisional lane regardless of how their on-disk avg compares.
    request_v0_probe_avg = req.get(
        "current_lossless_source_v0_probe_avg_bitrate"
    )

    try:
        gate_state = load_quality_gate_state(
            request_id=args.id,
            db=db,
            mb_id=req.get("mb_release_id"),
        )
    except Exception:  # noqa: BLE001 - boundary converts or isolates collaborator failures
        # This is a diagnostic command. Missing/stale evidence must fail open
        # without reviving the legacy request spectral scalar as authority.
        gate_state = None

    linked_current_measurement = (
        gate_state.measurement if gate_state is not None else None
    )
    linked_current_verified_lossless_proof = (
        gate_state.verified_lossless_proof if gate_state is not None else False
    )
    linked_current_v0_probe_avg = (
        gate_state.source_v0_avg_bitrate_kbps
        if gate_state is not None
        else None
    )

    print(f"  {label}")
    print(f"  Status: {req['status']}")
    print(f"  Rank config: metric={rank_cfg.bitrate_metric.value}")
    print(f"  Verified-lossless output: "
          f"{_quality_preview_target_label(target_format, verified_lossless_target)}")
    print(
        "  Request quality stamps (audit only): "
        f"min_bitrate={_fmt_br(request_min_br)}, "
        f"spectral_grade={request_spectral_grade or 'n/a'}, "
        f"spectral_bitrate={_fmt_br(request_current_br)}, "
        f"source_v0_avg={_fmt_br(request_v0_probe_avg)}, "
        f"final_format={request_final_format or 'n/a'}"
    )
    print()

    # --- Current quality gate ---
    current = linked_current_measurement
    min_br = current.min_bitrate_kbps if current is not None else None
    avg_br = current.avg_bitrate_kbps if current is not None else None
    median_br = current.median_bitrate_kbps if current is not None else None
    existing_format_hint = current.format if current is not None else None
    is_cbr = current.is_cbr if current is not None else False
    spectral_grade = current.spectral_grade if current is not None else None
    current_br = (
        current.spectral_bitrate_kbps if current is not None else None
    )
    gate_unavailable_reason = (
        None
        if current is not None and min_br is not None
        else (
            "linked current evidence unavailable"
            if current is None
            else "linked current evidence has no minimum bitrate"
        )
    )
    if current is not None and min_br is not None:
        # gate_rank centralizes the spectral clamp the gate applies, so the
        # displayed label always matches the verdict.
        current_rank = gate_rank(
            current,
            rank_cfg,
            verified_lossless_proof=linked_current_verified_lossless_proof,
        )
        gate = quality_gate_decision(
            current,
            cfg=rank_cfg,
            verified_lossless_proof=linked_current_verified_lossless_proof,
        )
        gate_label = {"accept": "DONE", "requeue_upgrade": "NEEDS UPGRADE",
                      "requeue_lossless": "NEEDS LOSSLESS"}[gate]
        print(f"  Quality gate:  {gate_label}  (rank={current_rank.name})")
        print(f"    min_bitrate={_fmt_br(min_br)}, "
              f"avg_bitrate={_fmt_br(avg_br) if avg_br else 'n/a'}, "
              f"median_bitrate={_fmt_br(median_br) if median_br else 'n/a'}, "
              f"format={existing_format_hint or '(unknown)'}, "
              "verified_lossless_proof="
              f"{linked_current_verified_lossless_proof}, is_cbr={is_cbr}")
        if linked_current_v0_probe_avg is not None:
            print(f"    linked_source_v0_probe_avg={linked_current_v0_probe_avg}kbps "
                  f"(locks lossy candidates)")
        if q_override:
            print(f"    searching: {q_override}")
    else:
        print(f"  Quality gate:  UNAVAILABLE ({gate_unavailable_reason})")
        print("    current-album comparisons omitted; scenarios continue")

    # --- Rejection backfill status ---
    backfill = rejection_backfill_override(
        current_measurement=linked_current_measurement,
        spectral_evidence_source="linked_current_evidence",
        cfg=rank_cfg,
    )
    if backfill and backfill != q_override:
        print(f"  Backfill:      would set search_filetype_override='{backfill}' on next rejection")
    elif q_override == "lossless":
        print("  Backfill:      not needed (search_filetype_override already set)")
    elif q_override:
        print("  Backfill:      won't fire lossless-only (ordinary per-tier narrowing remains)")
    elif linked_current_measurement is None:
        print("  Backfill:      won't fire (linked current evidence unavailable)")
    else:
        print("  Backfill:      won't fire (conditions not met)")

    # --- Simulate common scenarios ---
    # A missing mode makes current-album comparisons nonclaiming.  Candidate
    # scenarios can still exercise their independent decision paths.
    comparable_min_br = (
        min_br if current is not None else None
    )
    comparable_current_br = (
        current_br if current is not None else None
    )
    comparable_spectral_grade = (
        spectral_grade if current is not None else None
    )
    # Codec-aware (issue #829 Phase 5 PR2b). The context comes from the gate
    # state, NOT from the measurement alone: only the evidence row carries
    # ``storage_format``/``filetype_band``, and only those can fail a
    # mixed-codec album closed. Re-deriving here would let the simulator
    # display a class production withholds (review S6).
    comparable_spectral_context = (
        gate_state.spectral_context
        if gate_state is not None and gate_state.spectral_context is not None
        else SpectralCodecContext()
    )
    effective_existing = compute_effective_override_bitrate(
        comparable_min_br, comparable_spectral_context.interpret(current))
    override_min_bitrate = None
    if (effective_existing is not None and comparable_min_br is not None
            and effective_existing != comparable_min_br):
        override_min_bitrate = effective_existing

    lossless_target_label = _quality_preview_target_label(
        target_format, verified_lossless_target)
    scenarios: list[tuple[str, _ScenarioParams]] = [
        # --- FLAC downloads ---
        (f"Genuine FLAC → {lossless_target_label} (high bitrate)", {
            "is_flac": True, "min_bitrate": 245, "is_cbr": False,
            "spectral_grade": "genuine", "converted_count": 12,
            "post_conversion_min_bitrate": 245,
            "post_conversion_is_cbr": False}),
        (f"Genuine FLAC → {lossless_target_label} (lo-fi, 207kbps)", {
            "is_flac": True, "min_bitrate": 207, "is_cbr": False,
            "spectral_grade": "genuine", "converted_count": 12,
            "post_conversion_min_bitrate": 207,
            "post_conversion_is_cbr": False}),
        (f"Marginal FLAC → {lossless_target_label}", {
            "is_flac": True, "min_bitrate": 240, "is_cbr": False,
            "spectral_grade": "marginal", "converted_count": 12,
            "post_conversion_min_bitrate": 240,
            "post_conversion_is_cbr": False}),
        ("Suspect FLAC (transcode, 190kbps)", {
            "is_flac": True, "min_bitrate": 190, "is_cbr": False,
            "spectral_grade": "suspect", "converted_count": 12,
            "post_conversion_min_bitrate": 190,
            "post_conversion_is_cbr": False,
            "candidate_v0_probe_avg": 190,
            "candidate_v0_probe_min": 190,
            "candidate_v0_probe_kind": "lossless_source_v0"}),
        ("Suspect FLAC (transcode, 245kbps)", {
            "is_flac": True, "min_bitrate": 245, "is_cbr": False,
            "spectral_grade": "suspect", "converted_count": 12,
            "post_conversion_min_bitrate": 245,
            "post_conversion_is_cbr": False,
            "candidate_v0_probe_avg": 229,
            "candidate_v0_probe_min": 199,
            "candidate_v0_probe_kind": "lossless_source_v0"}),
        # Bill Hicks 1990 "Dangerous" shape: spoken-word lossless that
        # spectral_check false-positives as suspect (high HF deficit
        # against music-tuned thresholds), but the lossless_source_v0
        # probe corroborates a genuine master. The V0-avg trust override
        # in determine_verified_lossless flips this to verified.
        ("Suspect FLAC + lossless_source_v0 avg=241/min=219 (V0 override)", {
            "is_flac": True, "min_bitrate": 219, "is_cbr": False,
            "spectral_grade": "suspect", "converted_count": 10,
            "post_conversion_min_bitrate": 219,
            "post_conversion_is_cbr": False,
            "candidate_v0_probe_avg": 241,
            "candidate_v0_probe_min": 219,
            "candidate_v0_probe_kind": "lossless_source_v0"}),
        # --- MP3 VBR downloads ---
        # ``avg_bitrate`` no longer selects whether the preimport spectral
        # gate runs: issue #1145 retired that skip, so every MP3 below is
        # scanned. The averages still matter — they are what the measured
        # rank classifies — but the gate reads the codec alone.
        ("MP3 V0 genuine (avg 245kbps)", {
            "is_flac": False, "min_bitrate": 240, "is_cbr": False,
            "is_vbr": True, "avg_bitrate": 245}),
        ("MP3 V0 (low, avg 205kbps)", {
            "is_flac": False, "min_bitrate": 205, "is_cbr": False,
            "is_vbr": True, "avg_bitrate": 205}),
        ("VBR transcode (Go! Team shape, avg 182kbps)", {
            "is_flac": False, "min_bitrate": 126, "is_cbr": False,
            "is_vbr": True, "avg_bitrate": 182,
            "spectral_grade": "likely_transcode", "spectral_bitrate": 96}),
        ("MP3 V2 (avg 190kbps)", {
            "is_flac": False, "min_bitrate": 190, "is_cbr": False,
            "is_vbr": True, "avg_bitrate": 190}),
        # --- MP3 CBR downloads (no spectral) ---
        ("CBR 320 (no spectral)", {
            "is_flac": False, "min_bitrate": 320, "is_cbr": True}),
        ("CBR 256 (no spectral)", {
            "is_flac": False, "min_bitrate": 256, "is_cbr": True}),
        ("CBR 192 (no spectral)", {
            "is_flac": False, "min_bitrate": 192, "is_cbr": True}),
        # --- MP3 CBR downloads (with spectral) ---
        ("CBR 320 genuine", {
            "is_flac": False, "min_bitrate": 320, "is_cbr": True,
            "spectral_grade": "genuine"}),
        ("CBR 320 suspect (~128kbps)", {
            "is_flac": False, "min_bitrate": 320, "is_cbr": True,
            "spectral_grade": "suspect", "spectral_bitrate": 128}),
        ("CBR 320 suspect (~192kbps)", {
            "is_flac": False, "min_bitrate": 320, "is_cbr": True,
            "spectral_grade": "suspect", "spectral_bitrate": 192}),
        ("CBR 256 genuine", {
            "is_flac": False, "min_bitrate": 256, "is_cbr": True,
            "spectral_grade": "genuine"}),
        ("CBR 192 genuine", {
            "is_flac": False, "min_bitrate": 192, "is_cbr": True,
            "spectral_grade": "genuine"}),
    ]
    # --- Preimport gate scenarios (issue #91) ---
    # Audio and nested-layout gates short-circuit before any FLAC/MP3 stage
    # runs. These let operators see the rejection paths that
    # lib.measurement.measure_preimport_state measures and
    # lib.quality.pipeline decides from (issue #1355 item 1: corrupt audio
    # outranks a nested folder when a candidate carries both facts).
    #
    # `audio_check_mode` is read from the active runtime config and
    # applied to every scenario — on deployments with
    # `[Beets Validation] audio_check = off`, ALL scenarios must report
    # `preimport_audio=skipped_off`, not just the synthetic preimport
    # ones (Codex round 3 P2). Scenarios that explicitly want to
    # demonstrate the gate (e.g. the audio_corrupt demo) override this
    # value.
    runtime_audio_check = _load_runtime_audio_check_mode()
    scenarios.extend([
        # `audio_check_mode` not set here — defaults to the runtime value
        # below so the scenario honestly reflects the deployment: on an
        # `audio_check = off` deployment this prints `skipped_off`, which
        # is what the live pipeline would do (Codex round 2 P3 + round 3 P2).
        ("PREIMPORT: Audio corrupt (ffmpeg fail)", {
            "is_flac": False, "min_bitrate": 256, "is_cbr": False,
            "audio_corrupt": True}),
        ("PREIMPORT: Nested folders", {
            "is_flac": False, "min_bitrate": 320, "is_cbr": True,
            "has_nested_audio": True}),
    ])

    marked_incomplete = req.get("marked_incomplete_at") is not None
    if marked_incomplete:
        print(
            "\n  ⚠ Operator-marked incomplete (#1241, "
            f"marked_incomplete_at={req.get('marked_incomplete_at')}): a "
            "candidate beets proves whole disregards the installed side "
            "and is admitted as into an empty slot."
        )

    print("\n  What would happen if we downloaded:")
    for name, params in scenarios:
        # Apply runtime audio_check_mode as a default; scenarios that
        # explicitly override it still win (dict unpack order).
        params_with_runtime: _ScenarioParams = {
            "audio_check_mode": runtime_audio_check,
            **params,
        }
        result = full_pipeline_decision(
            existing_min_bitrate=comparable_min_br,
            # Forward avg_bitrate too — under the default AVG policy the
            # simulator must compare against the real album avg, not min,
            # or VBR albums rank at the wrong tier in stage 2/3 output
            # (issue #93 codex round 4).
            existing_avg_bitrate=avg_br,
            existing_spectral_grade=comparable_spectral_grade,
            existing_spectral_bitrate=comparable_current_br,
            existing_spectral_context=comparable_spectral_context,
            override_min_bitrate=override_min_bitrate,
            existing_format=(
                existing_format_hint
                if current is not None
                else None
            ),
            existing_is_cbr=is_cbr,
            candidate_verified_lossless_proof=False,
            target_format=target_format,
            verified_lossless_target=verified_lossless_target,
            existing_v0_probe_avg=linked_current_v0_probe_avg,
            cfg=rank_cfg,
            current_verified_lossless_proof=(
                linked_current_verified_lossless_proof
            ),
            # Issue #1241: every synthetic scenario already assumes the
            # download validates whole (that is what "downloaded" means
            # here), so coverage is a constant True — inert unless the
            # request is operator-marked incomplete.
            installed_marked_incomplete=marked_incomplete,
            candidate_covers_declared_program=True,
            **params_with_runtime)

        _print_decision_outcome(
            name, result,
            q_override=q_override,
            gate_unavailable_reason=gate_unavailable_reason,
        )

    _print_live_candidate_replay(
        db, args.id,
        expected_release_id=req.get("mb_release_id"),
        rank_cfg=rank_cfg,
        target_format=target_format,
        verified_lossless_target=verified_lossless_target,
        runtime_audio_check=runtime_audio_check,
        q_override=q_override,
        gate_unavailable_reason=gate_unavailable_reason,
        marked_incomplete=marked_incomplete,
    )


def cmd_repair_spectral(
    db: _QualityCommandDB, args: argparse.Namespace,
) -> int | None:
    """Find and repair albums stuck by stale current_spectral_bitrate.

    Identifies wanted albums where current_spectral_grade is genuine but
    current_spectral_bitrate still holds a stale transcode estimate,
    causing the quality gate to requeue indefinitely (issue #18).
    """
    from lib.dispatch import load_quality_gate_state
    from lib.pipeline_db import ADVISORY_LOCK_NAMESPACE_IMPORT
    from lib.quality import quality_gate_decision

    rank_cfg = _load_runtime_rank_config()

    # Find candidates: genuine on disk but spectral bitrate < min_bitrate
    # (genuine files should have no spectral cliff → bitrate should be NULL)
    cur = db._execute("""
        SELECT id, artist_name, album_title, min_bitrate,
               current_spectral_bitrate, current_spectral_grade,
               last_download_spectral_bitrate, last_download_spectral_grade,
               verified_lossless
        FROM album_requests
        WHERE status = 'wanted'
          AND current_spectral_grade = 'genuine'
          AND current_spectral_bitrate IS NOT NULL
    """)
    candidates = [dict(r) for r in cur.fetchall()]

    if not candidates:
        print("No stuck albums found.")
        return

    print(f"Found {len(candidates)} album(s) with stale spectral data:\n")

    repaired = 0
    for req in candidates:
        rid = req["id"]
        # Raw-SQL boundary: the cursor hands back untyped column values, and
        # every lock/transition/write below keys on this id being the int
        # the column really is. Assert it here rather than widening the
        # cursor's row type back to ``Any``.
        assert isinstance(rid, int), f"album_requests.id is not an int: {rid!r}"
        with db.advisory_lock(
            ADVISORY_LOCK_NAMESPACE_IMPORT,
            rid,
        ) as acquired:
            if not acquired:
                print(f"  [{rid:>4}] repair busy; retry later")
                return 4
            current = db.get_request(rid)
            processing_locked = transitions.processing_locked_conflict(
                current,
                rid,
                "repair_spectral",
                expected_status="wanted",
            )
            if processing_locked is not None:
                print(json.dumps(
                    transitions.transition_conflict_payload(
                        processing_locked
                    )
                ))
                return 4
            if (
                current is None
                or current["status"] != "wanted"
                or current["current_spectral_grade"] != "genuine"
                or current["current_spectral_bitrate"] is None
            ):
                print(f"  [{rid:>4}] transition conflict: row changed during repair")
                return 4

            label = f"{current['artist_name']} - {current['album_title']}"
            stale_br = current["current_spectral_bitrate"]
            state = load_quality_gate_state(
                request_id=rid,
                db=db,
            )
            effective_min_br = (
                state.measurement.min_bitrate_kbps
                if state is not None
                else current["min_bitrate"]
            )
            print(f"  [{rid:>4}] {label}")
            print(f"         min_bitrate={effective_min_br}kbps, "
                  f"stale current_spectral={stale_br}kbps")

            decision = (
                quality_gate_decision(
                    state.measurement,
                    cfg=rank_cfg,
                    verified_lossless_proof=state.verified_lossless_proof,
                )
                if state is not None
                else "requeue_upgrade"
            )
            print(f"         after repair: quality_gate_decision → {decision}")

            if args.dry_run:
                print(
                    "         [DRY RUN] would clear spectral + remove "
                    "stale denylists"
                )
                continue

            expected_after_transition = "wanted"
            if decision == "accept" and effective_min_br is not None:
                transition_result = finalize_request(
                    db,
                    rid,
                    transitions.RequestTransition.to_imported(
                        from_status="wanted",
                        min_bitrate=effective_min_br,
                    ),
                )
                if isinstance(
                    transition_result,
                    transitions.TransitionConflict,
                ):
                    print(
                        f"         transition conflict: "
                        f"{transition_result.kind.value} "
                        f"(actual={transition_result.actual_status})"
                    )
                    return 4
                expected_after_transition = "imported"

            cleared = db.update_request_fields(
                rid,
                expected_status=expected_after_transition,
                last_download_spectral_bitrate=None,
                current_spectral_bitrate=None,
            )
            if not cleared:
                print("         transition conflict: row changed during repair")
                return 4

            del_cur = db._execute("""
                DELETE FROM source_denylist
                WHERE request_id = %s
                  AND (reason LIKE 'quality gate: spectral%%'
                       OR reason LIKE 'spectral:%%')
                RETURNING username, reason
            """, (rid,))
            removed = del_cur.fetchall()
            for entry in removed:
                print(
                    f"         un-denylisted: {entry['username']} "
                    f"({entry['reason']})"
                )

            if decision == "accept" and effective_min_br is not None:
                print("         → transitioned to imported")
            else:
                print(f"         → remains wanted (gate says {decision})")

            repaired += 1

    print(f"\nRepaired {repaired} album(s)." if not args.dry_run
          else f"\n[DRY RUN] Would repair {len(candidates)} album(s).")


def add_quality_subparsers(
    sub: argparse._SubParsersAction[argparse.ArgumentParser],
) -> None:
    """Add ``quality`` / ``repair-spectral`` (#521 carve out of
    ``routes_meta._build_parser``, verbatim argument definitions)."""
    # quality
    p_quality = sub.add_parser("quality", help="Show quality state and simulate decisions")
    p_quality.add_argument("id", type=int, help="Request ID")

    # repair-spectral
    p_repair = sub.add_parser("repair-spectral",
                              help="Fix albums stuck by stale current_spectral_bitrate (#18)")
    p_repair.add_argument("--dry-run", action="store_true",
                          help="Show what would be repaired without changing anything")
