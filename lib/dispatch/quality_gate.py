"""Post-import quality gate.

Loads the on-disk measurement for a just-imported album and runs
``quality_gate_decision`` to accept / requeue-for-upgrade / requeue-for-
lossless. ``finalize_request`` is the module-local DI seam.
"""

from __future__ import annotations

import logging
from typing import Sequence, TYPE_CHECKING

from lib import transitions

# Module-level DI seam for ``transitions.finalize_request``.
finalize_request = transitions.finalize_request

from lib.quality import (QUALITY_LOSSLESS, QUALITY_UPGRADE_TIERS,
                         compute_effective_override_bitrate, extract_usernames,
                         quality_gate_decision)

from lib.dispatch.types import QualityGateState

if TYPE_CHECKING:
    from lib.pipeline_db import PipelineDB
    from lib.quality import QualityRankConfig

logger = logging.getLogger("cratedigger")


def load_quality_gate_state(
    *,
    request_id: int,
    db: "PipelineDB",
    mb_id: str | None = None,
    quality_ranks: "QualityRankConfig | None" = None,
) -> QualityGateState | None:
    """Load the current on-disk measurement for quality-gate evaluation.

    Shared adapter for all post-import quality-gate callers. This is the
    single place that combines:
    - Beets on-disk metadata (min/avg/format/is_cbr)
    - request-row overrides (`final_format`, `verified_lossless`)
    - grade-aware spectral override logic
    """
    from lib.beets_db import BeetsDB
    from lib.quality import (
        AudioQualityMeasurement,
        QualityRankConfig,
        TargetQualityContract,
    )

    if quality_ranks is None:
        quality_ranks = QualityRankConfig.defaults()

    req = None
    try:
        req = db.get_request(request_id)
    except Exception:
        logger.debug("QUALITY GATE: DB lookup failed for request row")

    resolved_mb_id = mb_id or (str(req["mb_release_id"]) if req and req.get("mb_release_id") else None)
    if not resolved_mb_id:
        return None

    with BeetsDB() as beets:
        info = beets.get_album_info(resolved_mb_id, quality_ranks)
    if not info:
        return None

    min_br_kbps = info.min_bitrate_kbps
    spectral_grade = req.get("current_spectral_grade") if req else None
    raw_br = req.get("current_spectral_bitrate") if req else None
    raw_br_int = raw_br if isinstance(raw_br, int) else None
    spectral_br: int | None = None
    effective = compute_effective_override_bitrate(
        min_br_kbps, raw_br_int, spectral_grade)
    if effective is not None and effective < min_br_kbps:
        spectral_br = raw_br_int

    album_format = info.format
    target_contract = None
    verified_lossless = bool(req.get("verified_lossless")) if req else False
    if req and req.get("final_format"):
        target_contract = TargetQualityContract.from_projection(
            str(req["final_format"]),
            # Bare MP3 is not self-describing. At the post-import gate the
            # materialized album is the available confirmation of the
            # projection mode; keep it on the contract instead of borrowing
            # it implicitly inside rank classification.
            projected_is_cbr=info.is_cbr,
        )

    current = AudioQualityMeasurement(
        min_bitrate_kbps=min_br_kbps,
        avg_bitrate_kbps=info.avg_bitrate_kbps,
        median_bitrate_kbps=info.median_bitrate_kbps,
        format=album_format,
        is_cbr=info.is_cbr,
        verified_lossless=verified_lossless,
        spectral_bitrate_kbps=spectral_br,
    )
    return QualityGateState(
        measurement=current,
        min_bitrate_kbps=min_br_kbps,
        spectral_bitrate_kbps=spectral_br,
        spectral_grade=spectral_grade,
        target_contract=target_contract,
    )


def _check_quality_gate_core(
    mb_id: str,
    label: str,
    request_id: int,
    files: Sequence[object],
    db: "PipelineDB",
    quality_ranks: "QualityRankConfig | None" = None,
) -> None:
    """Post-import quality gate — standalone version taking plain params + PipelineDB.

    Reads beets DB for on-disk quality, runs quality_gate_decision, dispatches
    requeue/accept. Used by both auto-import (via wrapper) and core dispatch.

    ``quality_ranks`` is used by ``BeetsDB.get_album_info()`` to reduce
    mixed-format albums via ``cfg.mixed_format_precedence``. Defaults to
    ``QualityRankConfig.defaults()`` so existing tests and callers that
    don't care about mixed-format reduction still work. Commit 5 will thread
    the real runtime config through from dispatch_import_core().
    """
    from lib.quality import QualityRankConfig, gate_rank

    if quality_ranks is None:
        quality_ranks = QualityRankConfig.defaults()

    if not mb_id:
        return
    try:
        state = load_quality_gate_state(
            request_id=request_id,
            db=db,
            mb_id=mb_id,
            quality_ranks=quality_ranks,
        )
        if not state:
            return
        current = state.measurement
        min_br_kbps = state.min_bitrate_kbps
        spectral_br = state.spectral_bitrate_kbps
        spectral_grade = state.spectral_grade
        if spectral_br is not None:
            logger.info(f"QUALITY GATE: using current_spectral={spectral_br}kbps "
                        f"(lower than beets min_bitrate={min_br_kbps}kbps, "
                        f"grade={spectral_grade})")
        decision = quality_gate_decision(
            current,
            cfg=quality_ranks,
            target_contract=state.target_contract,
        )

        spectral_note = f" (spectral={spectral_br}kbps)" if spectral_br else ""

        if decision == "requeue_upgrade":
            upgrade_override = QUALITY_UPGRADE_TIERS
            transitions.require_transition_applied(finalize_request(
                db,
                request_id,
                transitions.RequestTransition.to_wanted(
                    from_status="imported",
                    search_filetype_override=upgrade_override,
                    min_bitrate=min_br_kbps,
                ),
            ))
            usernames = extract_usernames(files)
            gate_br = compute_effective_override_bitrate(
                min_br_kbps, spectral_br, spectral_grade) or min_br_kbps
            actual_rank = gate_rank(
                current,
                quality_ranks,
                target_contract=state.target_contract,
            )
            gate_min = quality_ranks.gate_min_rank
            br_note = (f"spectral {spectral_br}kbps (beets {min_br_kbps}kbps)"
                       if spectral_br and spectral_br < min_br_kbps
                       else f"{min_br_kbps}kbps")
            reason = (f"quality gate: rank {actual_rank.name} < {gate_min.name} "
                      f"({br_note})")
            for username in usernames:
                db.add_denylist(request_id, username, reason)
            logger.info(
                f"QUALITY GATE: {label} "
                f"rank={actual_rank.name} < {gate_min.name} "
                f"(gate_bitrate={gate_br}kbps{spectral_note}), "
                f"queued for upgrade, denylisted {usernames} "
                f"(searching {upgrade_override})")
        elif decision == "requeue_lossless":
            lossless_override = QUALITY_LOSSLESS
            transitions.require_transition_applied(finalize_request(
                db,
                request_id,
                transitions.RequestTransition.to_wanted(
                    from_status="imported",
                    search_filetype_override=lossless_override,
                    min_bitrate=min_br_kbps,
                ),
            ))
            logger.info(
                f"QUALITY GATE: {label} "
                f"min_bitrate={min_br_kbps}kbps CBR, not verified lossless — "
                f"searching for lossless to verify")
        else:  # accept
            transitions.require_transition_applied(finalize_request(
                db,
                request_id,
                transitions.RequestTransition.to_imported(
                    from_status="imported",
                    min_bitrate=min_br_kbps,
                    search_filetype_override=None,  # done searching
                ),
            ))
            if current.verified_lossless:
                logger.info(f"QUALITY GATE: {label} min_bitrate={min_br_kbps}kbps — quality OK")
            else:
                logger.info(f"QUALITY GATE: {label} min_bitrate={min_br_kbps}kbps VBR — quality OK")
    except Exception:
        logger.exception("QUALITY GATE: failed to check quality")
