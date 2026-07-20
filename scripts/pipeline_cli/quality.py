"""pipeline-cli quality/debug commands (#495 carve).

``quality`` — simulate common download scenarios against a request's
current quality state. ``repair-spectral`` — find and fix albums stuck
by stale ``current_spectral_bitrate`` (issue #18).
"""

import argparse

from typing import TypedDict

from lib import transitions
from scripts.pipeline_cli._format import _fmt_br


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


def _load_runtime_rank_config():
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


def cmd_quality(db, args):
    """Show quality state and simulate decisions for common download scenarios."""
    from lib.dispatch import load_quality_gate_state
    from lib.quality import (full_pipeline_decision, quality_gate_decision,
                             gate_rank,
                             rejection_backfill_override,
                             search_tiers, compute_effective_override_bitrate)

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
    except Exception:
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
        print(f"  Backfill:      not needed (search_filetype_override already set)")
    elif q_override:
        print("  Backfill:      won't fire lossless-only (ordinary per-tier narrowing remains)")
    elif linked_current_measurement is None:
        print("  Backfill:      won't fire (linked current evidence unavailable)")
    else:
        print(f"  Backfill:      won't fire (conditions not met)")

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
    effective_existing = compute_effective_override_bitrate(
        comparable_min_br, comparable_current_br, comparable_spectral_grade)
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
            "post_conversion_is_cbr": False}),
        ("Suspect FLAC (transcode, 245kbps)", {
            "is_flac": True, "min_bitrate": 245, "is_cbr": False,
            "spectral_grade": "suspect", "converted_count": 12,
            "post_conversion_min_bitrate": 245,
            "post_conversion_is_cbr": False}),
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
        # avg_bitrate drives the new preimport spectral gate (issue #93):
        # VBR with avg >= cfg.mp3_vbr.excellent skips spectral entirely,
        # below gates through analysis even without a spectral_grade input.
        ("MP3 V0 genuine (avg 245kbps, gate skips)", {
            "is_flac": False, "min_bitrate": 240, "is_cbr": False,
            "is_vbr": True, "avg_bitrate": 245}),
        ("MP3 V0 (low, avg 205kbps, gate runs)", {
            "is_flac": False, "min_bitrate": 205, "is_cbr": False,
            "is_vbr": True, "avg_bitrate": 205}),
        ("VBR transcode (Go! Team shape, avg 182kbps)", {
            "is_flac": False, "min_bitrate": 126, "is_cbr": False,
            "is_vbr": True, "avg_bitrate": 182,
            "spectral_grade": "likely_transcode", "spectral_bitrate": 96}),
        ("MP3 V2 (avg 190kbps, gate runs)", {
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
    # runs. These let operators see the rejection paths that live in
    # lib.measurement.measure_preimport_state and
    # lib.dispatch.dispatch_import_from_db.
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

    print(f"\n  What would happen if we downloaded:")
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
            **params_with_runtime)

        imported = "IMPORT" if result["imported"] else "REJECT"
        parts = [imported]
        if result["denylisted"]:
            parts.append("denylist")
        if result["keep_searching"]:
            parts.append("keep searching")
        final = result["final_status"] or "?"
        decision_chain = " → ".join(
            f"{s}={result[s]}"
            for s in ["preimport_audio", "preimport_nested",
                      "stage0_spectral_gate", "stage1_spectral",
                      "stage2_import", "stage3_quality_gate"]
            if result[s] is not None)

        print(f"    {name}:")
        print(f"      → {', '.join(parts)} (final: {final})")
        if decision_chain:
            print(f"      chain: {decision_chain}")

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


def cmd_repair_spectral(db, args):
    """Find and repair albums stuck by stale current_spectral_bitrate.

    Identifies wanted albums where current_spectral_grade is genuine but
    current_spectral_bitrate still holds a stale transcode estimate,
    causing the quality gate to requeue indefinitely (issue #18).
    """
    from lib.dispatch import load_quality_gate_state
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
        label = f"{req['artist_name']} - {req['album_title']}"
        stale_br = req["current_spectral_bitrate"]
        state = load_quality_gate_state(
            request_id=rid,
            db=db,
        )
        effective_min_br = (
            state.measurement.min_bitrate_kbps
            if state is not None
            else req["min_bitrate"]
        )
        print(f"  [{rid:>4}] {label}")
        print(f"         min_bitrate={effective_min_br}kbps, "
              f"stale current_spectral={stale_br}kbps")

        # Check what quality gate would decide after clearing stale data
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
            print(f"         [DRY RUN] would clear spectral + remove stale denylists")
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
            if isinstance(transition_result, transitions.TransitionConflict):
                print(
                    f"         transition conflict: "
                    f"{transition_result.kind.value} "
                    f"(actual={transition_result.actual_status})")
                return 4
            expected_after_transition = "imported"

        # Clear only if the row is still in the status this repair established.
        cleared = db.update_request_fields(
            rid,
            expected_status=expected_after_transition,
            last_download_spectral_bitrate=None,
            current_spectral_bitrate=None,
        )
        if not cleared:
            print("         transition conflict: row changed during repair")
            return 4

        # Remove denylist entries caused by stale spectral
        del_cur = db._execute("""
            DELETE FROM source_denylist
            WHERE request_id = %s
              AND (reason LIKE 'quality gate: spectral%%'
                   OR reason LIKE 'spectral:%%')
            RETURNING username, reason
        """, (rid,))
        removed = del_cur.fetchall()
        for entry in removed:
            print(f"         un-denylisted: {entry['username']} ({entry['reason']})")

        if decision == "accept" and effective_min_br is not None:
            print(f"         → transitioned to imported")
        else:
            print(f"         → remains wanted (gate says {decision})")

        repaired += 1

    print(f"\nRepaired {repaired} album(s)." if not args.dry_run
          else f"\n[DRY RUN] Would repair {len(candidates)} album(s).")


def add_quality_subparsers(sub: argparse._SubParsersAction) -> None:
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
