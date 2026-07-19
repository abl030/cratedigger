"""Post-import quality gate.

Loads the linked installed-copy measurement for a just-imported album and runs
``quality_gate_decision`` to accept / requeue-for-upgrade / requeue-for-
lossless. ``finalize_request`` is the module-local DI seam.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Callable, Sequence, TYPE_CHECKING

import msgspec

from lib import transitions

# Module-level DI seam for ``transitions.finalize_request``.
finalize_request = transitions.finalize_request

from lib.quality import (compute_effective_override_bitrate, extract_usernames,
                         quality_gate_decision)
from lib.quality.decisions import post_import_search_action

from lib.dispatch.types import QualityGateState
from lib.terminal_outcomes import TerminalDenylist


@dataclass(frozen=True)
class QualityGatePlan:
    """Request/denylist writes produced by a post-import gate decision."""

    transition: transitions.RequestTransition
    denylists: tuple[TerminalDenylist, ...] = ()
    successful_terminal_acceptance: bool = False


def _evidence_unavailable_plan() -> QualityGatePlan:
    """Keep acquisition open when installed-copy evidence is unavailable.

    Missing or failed linked evidence cannot prove either transparent quality
    or verified-lossless lineage.  The conservative action is a full-tier
    retry: the imported copy remains on disk and only the request reopens —
    the next cycle rebuilds the evidence and re-settles the request.

    Decision 18: NO denylist.  A local bookkeeping failure is never
    attributed to the winning peer; a denylist attaches only after a
    quality decision on successfully loaded evidence.  (The shared
    ``post_import_search_action`` mapping sets ``denylist=True`` for
    ``requeue_upgrade`` — that applies to decided retentions, deliberately
    not to this environment-failure path.)
    """

    action = post_import_search_action("requeue_upgrade")
    return QualityGatePlan(
        transition=transitions.RequestTransition.to_wanted(
            from_status="imported",
            search_filetype_override=action.search_filetype_override,
        ),
        denylists=(),
    )


if TYPE_CHECKING:
    from lib.pipeline_db import PipelineDB
    from lib.quality import QualityRankConfig

logger = logging.getLogger("cratedigger")


def load_quality_gate_state(
    *,
    request_id: int,
    db: "PipelineDB",
    mb_id: str | None = None,
    expected_current_evidence_id: int | None = None,
) -> QualityGateState | None:
    """Load quality-gate facts exclusively from linked current evidence."""
    from lib.quality_evidence import current_evidence_rebuild_reasons
    from lib.release_identity import normalize_release_id

    resolved_mb_id = mb_id
    if not resolved_mb_id:
        try:
            req = db.get_request(request_id)
        except Exception:
            logger.debug("QUALITY GATE: DB lookup failed for request row")
            req = None
        resolved_mb_id = (
            str(req["mb_release_id"])
            if req and req.get("mb_release_id")
            else None
        )
    if not resolved_mb_id:
        return None

    evidence_id = db.get_request_current_evidence_id(request_id)
    if (
        expected_current_evidence_id is not None
        and evidence_id != expected_current_evidence_id
    ):
        return None
    current_evidence = (
        db.load_album_quality_evidence_by_id(evidence_id)
        if evidence_id is not None
        else None
    )
    if (
        current_evidence is None
        or normalize_release_id(current_evidence.mb_release_id)
        != normalize_release_id(resolved_mb_id)
        or current_evidence_rebuild_reasons(current_evidence)
    ):
        return None

    linked_measurement = current_evidence.measurement
    min_br_kbps = linked_measurement.min_bitrate_kbps
    if min_br_kbps is None:
        return None
    spectral_grade = linked_measurement.spectral_grade
    raw_br_int = linked_measurement.spectral_bitrate_kbps
    spectral_br: int | None = None
    effective = compute_effective_override_bitrate(
        min_br_kbps, raw_br_int, spectral_grade)
    if effective is not None and effective < min_br_kbps:
        spectral_br = raw_br_int

    current = msgspec.structs.replace(
        linked_measurement,
        spectral_bitrate_kbps=spectral_br,
    )
    source_v0_avg = None
    if (
        current_evidence.v0_metric is not None
        and current_evidence.v0_metric.subject == "source"
    ):
        source_v0_avg = current_evidence.v0_metric.avg_bitrate_kbps
    return QualityGateState(
        measurement=current,
        verified_lossless_proof=(
            current_evidence.verified_lossless_proof is not None
        ),
        source_v0_avg_bitrate_kbps=source_v0_avg,
    )


def _check_quality_gate_core(
    mb_id: str,
    label: str,
    request_id: int,
    files: Sequence[object],
    db: "PipelineDB",
    quality_ranks: "QualityRankConfig | None" = None,
    expected_current_evidence_id: int | None = None,
    apply: bool = True,
    state_loader: Callable[..., QualityGateState | None] = load_quality_gate_state,
) -> QualityGatePlan | None:
    """Apply the post-import policy to linked current evidence."""
    from lib.quality import QualityRankConfig

    if quality_ranks is None:
        quality_ranks = QualityRankConfig.defaults()

    if not mb_id:
        return
    plan: QualityGatePlan
    try:
        state = state_loader(
            request_id=request_id,
            db=db,
            mb_id=mb_id,
            expected_current_evidence_id=expected_current_evidence_id,
        )
        if not state:
            plan = _evidence_unavailable_plan()
            logger.warning(
                "QUALITY GATE: %s linked current evidence unavailable; "
                "reopening full-tier search",
                label,
            )
        else:
            current = state.measurement
            min_br_kbps = current.min_bitrate_kbps
            assert min_br_kbps is not None
            spectral_br = current.spectral_bitrate_kbps
            spectral_grade = current.spectral_grade
            if spectral_br is not None:
                logger.info(f"QUALITY GATE: using current_spectral={spectral_br}kbps "
                            f"(lower than linked min_bitrate={min_br_kbps}kbps, "
                            f"grade={spectral_grade})")
            decision = quality_gate_decision(
                current,
                cfg=quality_ranks,
                verified_lossless_proof=state.verified_lossless_proof,
            )
            action = post_import_search_action(decision)

            spectral_note = f" (spectral={spectral_br}kbps)" if spectral_br else ""

            if action.status == "wanted":
                transition = transitions.RequestTransition.to_wanted(
                    from_status="imported",
                    search_filetype_override=action.search_filetype_override,
                    min_bitrate=min_br_kbps,
                )
                usernames = extract_usernames(files) if action.denylist else set()
                reason = (
                    "quality gate: transparent installed copy independently "
                    "verified genuine; continuing lossless-only search"
                    if decision == "requeue_lossless"
                    else "quality gate: no verified-lossless proof; continuing full-tier search"
                )
                denylists = tuple(
                    TerminalDenylist(username, reason)
                    for username in sorted(usernames)
                )
                logger.info(
                    f"QUALITY GATE: {label} "
                    f"min_bitrate={min_br_kbps}kbps{spectral_note}, "
                    f"decision={decision}, denylisted {usernames}, "
                    f"search_override={action.search_filetype_override!r}")
                successful_terminal_acceptance = False
            else:  # verified-lossless proof accepts terminally
                transition = transitions.RequestTransition.to_imported(
                    from_status="imported",
                    min_bitrate=min_br_kbps,
                    search_filetype_override=action.search_filetype_override,
                )
                denylists = ()
                logger.info(
                    f"QUALITY GATE: {label} min_bitrate={min_br_kbps}kbps "
                    "— verified-lossless proof accepts"
                )
                # Authority: "A successful exact-release terminal import
                # acceptance supersedes an operator-owned `unsearchable`
                # search stop and records the request as `imported`." —
                # https://github.com/abl030/cratedigger/issues/737#issuecomment-5013436918
                successful_terminal_acceptance = True
            plan = QualityGatePlan(
                transition=transition,
                denylists=denylists,
                successful_terminal_acceptance=(
                    successful_terminal_acceptance
                ),
            )
    except Exception:
        logger.exception(
            "QUALITY GATE: failed to load or decide linked quality; "
            "reopening full-tier search"
        )
        plan = _evidence_unavailable_plan()

    # Apply outside the evidence/decision try block.  A transition failure is
    # not an evidence failure and must propagate to dispatch instead of being
    # swallowed and leaving the request terminally imported.
    if apply:
        transitions.require_transition_applied(finalize_request(
            db,
            request_id,
            plan.transition,
        ))
        for entry in plan.denylists:
            db.add_denylist(request_id, entry.username, entry.reason)
    return plan
