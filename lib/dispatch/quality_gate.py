"""Post-import quality gate.

Loads the linked installed-copy measurement for a just-imported album and runs
``quality_gate_decision`` to accept / requeue-for-upgrade / requeue-for-
lossless.
"""

from __future__ import annotations

import logging
from collections.abc import Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol

import msgspec

from lib import transitions
from lib.dispatch.types import QualityGateState
from lib.quality import (
    codec_context_from_measurement,
    compute_effective_override_bitrate,
    extract_usernames,
    quality_gate_decision,
    resolve_retained_search_override,
)
from lib.quality.decisions import post_import_search_action
from lib.terminal_outcomes import RequestPolicyOutcome, TerminalDenylist


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
    from lib.dispatch.types import DispatchDB
    from lib.pipeline_db.rows import AlbumRequestRow
    from lib.quality import (
        AlbumQualityEvidence,
        AudioQualityMeasurement,
        QualityRankConfig,
        TargetQualityContract,
    )
    from lib.quality.decisions import QualityGateDecision


class _QualityGateDecisionFn(Protocol):
    """Exact callable contract for the pure post-import decision seam."""

    def __call__(
        self,
        current: AudioQualityMeasurement,
        cfg: QualityRankConfig | None = None,
        *,
        target_contract: TargetQualityContract | None = None,
        verified_lossless_proof: bool = False,
    ) -> QualityGateDecision: ...

logger = logging.getLogger("cratedigger")


class QualityGateStateDB(Protocol):
    """The three linked-evidence reads the gate-state loader performs.

    Declared separately from ``DispatchDB`` (which satisfies it) because
    this loader is also the entry point ``pipeline-cli quality`` and
    ``pipeline-cli repair-spectral`` reach it through, and neither of those
    holds anything like dispatch's surface. Annotating it with the whole
    dispatch port would force those commands to claim a surface they do not
    have — the #409 narrow-port pattern, applied to the one function two
    very different callers share.
    """

    def get_request(self, request_id: int) -> AlbumRequestRow | None: ...

    def get_request_current_evidence_id(
        self, request_id: int,
    ) -> int | None: ...

    def load_album_quality_evidence_by_id(
        self, evidence_id: int | None,
    ) -> AlbumQualityEvidence | None: ...


def load_quality_gate_state(
    *,
    request_id: int,
    db: QualityGateStateDB,
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
        except Exception:  # noqa: BLE001 - boundary converts or isolates collaborator failures
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
    # The gate measurement carries the album's own codec-aware spectral
    # class, never the raw LAME-table bucket (issue #829 Phase 5 PR2b).
    # The context is captured, not just the verdict: it rides the returned
    # state so the simulator resolves the codec with the same album-level
    # facts this gate did.
    linked_context = codec_context_from_measurement(
        linked_measurement,
        storage_format=current_evidence.storage_format,
        filetype_band=current_evidence.filetype_band,
    )
    linked_spectral = linked_context.interpret(linked_measurement)
    spectral_br: int | None = None
    effective = compute_effective_override_bitrate(min_br_kbps, linked_spectral)
    if effective is not None and effective < min_br_kbps:
        spectral_br = effective

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
        spectral_context=linked_context,
    )


class _QualityGateStateLoader(Protocol):
    """Exact contract for the injected gate-state loader seam.

    Spelled out rather than left as ``Callable[..., QualityGateState |
    None]`` so the ``db=`` argument below is actually checked. That check is
    what proves ``DispatchDB`` satisfies ``QualityGateStateDB`` — the claim
    that port's docstring makes, and which nothing verified while this seam
    erased its own argument types.
    """

    def __call__(
        self,
        *,
        request_id: int,
        db: QualityGateStateDB,
        mb_id: str | None = ...,
        expected_current_evidence_id: int | None = ...,
    ) -> QualityGateState | None: ...


def _check_quality_gate_core(
    mb_id: str,
    label: str,
    request_id: int,
    files: Sequence[object],
    db: DispatchDB,
    quality_ranks: QualityRankConfig | None = None,
    expected_current_evidence_id: int | None = None,
    apply: bool = True,
    state_loader: _QualityGateStateLoader = load_quality_gate_state,
    quality_decision_fn: _QualityGateDecisionFn = quality_gate_decision,
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
            decision = quality_decision_fn(
                current,
                cfg=quality_ranks,
                verified_lossless_proof=state.verified_lossless_proof,
            )
            action = post_import_search_action(decision)

            spectral_note = f" (spectral={spectral_br}kbps)" if spectral_br else ""

            if action.status == "wanted":
                request = db.get_request(request_id)
                raw_existing_override = (
                    request.get("search_filetype_override")
                    if request is not None
                    else None
                )
                existing_override = (
                    raw_existing_override
                    if isinstance(raw_existing_override, str)
                    else None
                )
                search_override = resolve_retained_search_override(
                    existing_override,
                    action.search_filetype_override,
                )
                transition = transitions.RequestTransition.to_wanted(
                    from_status="imported",
                    search_filetype_override=search_override,
                    min_bitrate=min_br_kbps,
                )
                usernames = extract_usernames(files) if action.denylist else set[str]()
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
                    f"search_override={search_override!r}")
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
        # The transition and every denylist entry commit together in one
        # PostgreSQL transaction (issue #1355 item A2) — this used to be a
        # ``finalize_request`` call followed by a separate ``add_denylist``
        # per username, each its own autocommit, so a crash mid-loop could
        # leave some peers denylisted and others not.
        db.persist_request_policy_outcome(RequestPolicyOutcome(
            request_id=request_id,
            transition=plan.transition,
            denylists=plan.denylists,
            successful_terminal_acceptance=plan.successful_terminal_acceptance,
        ))
    return plan
