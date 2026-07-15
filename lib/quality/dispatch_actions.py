"""Pure post-decision action derivation and search-override narrowing."""

from dataclasses import dataclass
from typing import Any, Literal

from lib.quality.evidence_types import (
    AudioQualityMeasurement,
    SPECTRAL_TRANSCODE_GRADES,
)
from lib.quality.download_state import DownloadInfo
from lib.quality.filetypes import (
    LOSSLESS_CODECS,
    QUALITY_LOSSLESS,
    rejection_backfill_override,
)
from lib.quality.import_result_types import SpectralAnalysisDetail
from lib.quality.ranks import QualityRankConfig
from lib.quality.decisions import (
    DECISION_LOSSLESS_SOURCE_LOCKED,
    DECISION_PROVISIONAL_LOSSLESS_UPGRADE,
    DECISION_SUSPECT_LOSSLESS_DOWNGRADE,
    DECISION_SUSPECT_LOSSLESS_PROBE_MISSING,
)


# ---------------------------------------------------------------------------
# Dispatch logic — extracted from import_dispatch.py for testability
# ---------------------------------------------------------------------------


@dataclass
class DispatchAction:
    """What actions to take after import_one.py returns a decision."""
    mark_done: bool = False
    record_rejection: bool = False
    denylist: bool = False
    requeue: bool = False
    cleanup: bool = True
    trigger_notifiers: bool = False
    run_quality_gate: bool = False


def dispatch_action(decision: str) -> DispatchAction:
    """Map an ImportResult.decision string to the set of actions to take (pure).

    Encodes the if/elif dispatch chain from the import dispatch flow.
    """
    if decision in ("import", "preflight_existing"):
        return DispatchAction(mark_done=True, trigger_notifiers=True,
                              run_quality_gate=True, cleanup=True)
    elif decision == "downgrade":
        return DispatchAction(record_rejection=True, denylist=True, cleanup=True)
    elif decision == DECISION_PROVISIONAL_LOSSLESS_UPGRADE:
        return DispatchAction(mark_done=True, denylist=True, requeue=True,
                              trigger_notifiers=True, cleanup=True)
    elif decision in ("transcode_upgrade", "transcode_first"):
        return DispatchAction(mark_done=True, denylist=True, requeue=True,
                              trigger_notifiers=True, cleanup=True)
    elif decision in (
        DECISION_SUSPECT_LOSSLESS_DOWNGRADE,
        DECISION_SUSPECT_LOSSLESS_PROBE_MISSING,
        DECISION_LOSSLESS_SOURCE_LOCKED,
    ):
        return DispatchAction(record_rejection=True, denylist=True,
                              requeue=True, cleanup=True)
    elif decision == "transcode_downgrade":
        return DispatchAction(record_rejection=True, denylist=True, requeue=True,
                              cleanup=True)
    elif decision == "spectral_reject":
        return DispatchAction(record_rejection=True, denylist=True, cleanup=True)
    elif decision == "audio_corrupt":
        # U11: folder/audio-integrity reject. The source decoded as garbage —
        # denylist the peer + clean the staged dir. Caller forces requeue
        # (always self-heal on the four-fact rejects).
        return DispatchAction(record_rejection=True, denylist=True, cleanup=True)
    elif decision == "bad_audio_hash":
        # U11: folder/audio-integrity reject. The candidate matched a curated
        # bad-audio hash — denylist the peer + clean. Caller forces requeue.
        return DispatchAction(record_rejection=True, denylist=True, cleanup=True)
    elif decision == "nested_layout":
        # U11: folder-shape reject (audio in subdirectories). Not a peer
        # quality problem — no denylist. Clean the staged dir; caller forces
        # requeue.
        return DispatchAction(record_rejection=True, denylist=False, cleanup=True)
    elif decision == "empty_fileset":
        # U11: folder-shape reject (no audio files). Not a peer quality
        # problem — no denylist. Clean the staged dir; caller forces requeue.
        return DispatchAction(record_rejection=True, denylist=False, cleanup=True)
    elif decision == "mixed_source":
        # Mixed lossless + lossy in one folder. Peer chose to bundle lossy
        # bonus tracks alongside lossless audio — denylist them so the same
        # source doesn't burn another cycle on the same mixed bag. Clean
        # the staged dir; caller forces requeue via
        # ``_PREIMPORT_FACT_REJECT_DECISIONS`` so the album stays searchable
        # for a fully-lossless source. See ``has_mixed_lossless_and_lossy``.
        return DispatchAction(record_rejection=True, denylist=True, cleanup=True)
    elif decision == "duplicate_remove_guard_failed":
        return DispatchAction(record_rejection=True, denylist=True, requeue=False,
                              cleanup=False)
    else:  # import_failed, conversion_failed, mbid_missing, crash, etc.
        return DispatchAction(record_rejection=True)


def compute_effective_override_bitrate(
    container_bitrate: int | None,
    spectral_bitrate: int | None,
    spectral_grade: str | None,
) -> int | None:
    """Compute the grade-aware effective override bitrate.

    Spectral bitrate only participates when ``spectral_grade`` is in
    ``SPECTRAL_TRANSCODE_GRADES`` (``suspect`` / ``likely_transcode``). For any
    other grade — ``genuine``, ``marginal``, ``error``, ``None``, or an unknown
    future value — the spectral input is ignored and the container bitrate is
    returned untouched.

    When spectral is authorized, the function returns the lower of the two
    available values (conservative). Used by the auto / force import seams to
    derive ``--override-min-bitrate`` for ``import_one.py`` and by the quality
    gate to determine whether to apply a spectral override to the gate bitrate.
    """
    if spectral_grade not in SPECTRAL_TRANSCODE_GRADES:
        return container_bitrate
    if container_bitrate is None and spectral_bitrate is None:
        return None
    if container_bitrate is None:
        return spectral_bitrate
    if spectral_bitrate is None:
        return container_bitrate
    return min(container_bitrate, spectral_bitrate)


def extract_usernames(files: Any) -> set[str]:
    """Extract unique non-empty usernames from a list of file objects."""
    return {f.username for f in files if f.username}


def rejected_download_tier(dl_info: "DownloadInfo") -> str | None:
    """Determine which search_filetype_override tier a rejected download corresponds to.

    Maps from DownloadInfo properties to the tier string used in search_filetype_override
    CSV (e.g. "flac", "mp3 v0", "mp3 320").
    """
    slskd_ft = (dl_info.slskd_filetype or dl_info.filetype or "").lower().strip()
    if slskd_ft in LOSSLESS_CODECS or dl_info.was_converted:
        return "lossless"
    if "mp3" in slskd_ft:
        if dl_info.is_vbr:
            return "mp3 v0"
        bitrate = dl_info.bitrate
        if bitrate is None:
            return None
        kbps = bitrate // 1000 if bitrate > 1000 else bitrate
        return f"mp3 {kbps}"
    return None


def narrow_override_on_downgrade(search_filetype_override: str | None,
                                 dl_info: "DownloadInfo") -> str | None:
    """Remove the rejected filetype tier from search_filetype_override after downgrade.

    When a download is rejected as a downgrade (existing quality >= download),
    searching for the same tier again will produce the same result. Remove it
    to prevent infinite retry loops (e.g. downloading genuine CBR 320 six times).

    Returns the narrowed override string, or None if no change is needed.
    """
    if not search_filetype_override:
        return None
    tier = rejected_download_tier(dl_info)
    if not tier:
        return None
    tiers = [t.strip() for t in search_filetype_override.split(",")]
    if tier not in tiers:
        return None
    narrowed = [t for t in tiers if t != tier]
    if not narrowed:
        return None  # Don't remove the last tier
    return ",".join(narrowed)


@dataclass(frozen=True)
class RejectionSearchOverrideResolution:
    """Pure result of importer rejection-search convergence."""

    override: str | None
    reason: Literal["transparent_have", "rejected_tier", "preserve"]


def resolve_rejection_search_override(
    *,
    decision: str | None,
    current_override: str | None,
    dl_info: DownloadInfo,
    current_measurement: AudioQualityMeasurement | None,
    spectral_evidence_source: Literal[
        "attempt_have_audit", "linked_current_evidence"
    ],
    have_spectral_audit: SpectralAnalysisDetail | None = None,
    cfg: QualityRankConfig | None = None,
) -> RejectionSearchOverrideResolution:
    """Resolve importer narrowing in its single production order.

    A trusted transparent HAVE copy wins and closes every lossy tier. When
    that stronger rule fails open, ordinary downgrades retain the established
    rejected-tier removal. Transcode downgrades and unrelated decisions leave
    the existing override untouched by returning ``override=None``.
    """
    if decision not in ("downgrade", "transcode_downgrade"):
        return RejectionSearchOverrideResolution(None, "preserve")

    transparent_override = rejection_backfill_override(
        current_measurement=current_measurement,
        spectral_evidence_source=spectral_evidence_source,
        have_spectral_audit=have_spectral_audit,
        cfg=cfg,
    )
    if transparent_override is not None:
        return RejectionSearchOverrideResolution(
            transparent_override,
            "transparent_have",
        )

    if decision == "downgrade":
        tier_override = narrow_override_on_downgrade(
            current_override,
            dl_info,
        )
        if tier_override is not None:
            return RejectionSearchOverrideResolution(
                tier_override,
                "rejected_tier",
            )

    return RejectionSearchOverrideResolution(None, "preserve")


def narrow_override_on_lossless_source_lock(
    current: str | None,
) -> str | None:
    """Narrow ``search_filetype_override`` to lossless-only when the
    ``lossless_source_locked`` decision fires.

    Once a library row carries a comparable lossless-source V0 probe,
    no lossy candidate can override it -- the lock catches every lossy
    candidate at triage and routes it to ``confident_reject`` with
    cleanup eligibility. Without this narrowing, the search planner
    keeps asking Soulseek for the album with no filetype filter, peer
    after peer serves the same lossy file, each download is locked-out
    and auto-deleted. This helper closes that wasted-cycle window by
    pinning the search to lossless tiers.

    Returns:
        ``"lossless"`` when the override needs to change to lossless-only.
        ``None`` when the override is already ``"lossless"`` (no-op).

    Called from both the importer rejection branch
    (``lib.dispatch``) and the wrong-match cleanup triage
    (``lib.wrong_match_cleanup_service``).
    """
    if current == QUALITY_LOSSLESS:
        return None
    return QUALITY_LOSSLESS
