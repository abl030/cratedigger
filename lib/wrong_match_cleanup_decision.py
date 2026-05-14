"""Action-time cleanup authorization for Wrong Matches."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from lib.import_preview import preview_import_from_download_log

CLEANUP_DECISION_PROVENANCE = "preview_import_from_download_log"


@dataclass(frozen=True)
class WrongMatchCleanupDecision:
    """Fresh authorization result for destructive Wrong Matches cleanup."""

    download_log_id: int
    delete_allowed: bool
    uncertain: bool
    provenance: str
    verdict: str
    confident_reject: bool
    cleanup_eligible: bool
    preview_decision: str | None = None
    reason: str | None = None
    detail: str | None = None

    @property
    def skip_reason(self) -> str:
        return (
            self.reason
            or self.preview_decision
            or self.detail
            or self.verdict
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "download_log_id": self.download_log_id,
            "delete_allowed": self.delete_allowed,
            "uncertain": self.uncertain,
            "provenance": self.provenance,
            "verdict": self.verdict,
            "confident_reject": self.confident_reject,
            "cleanup_eligible": self.cleanup_eligible,
            "preview_decision": self.preview_decision,
            "reason": self.reason,
            "detail": self.detail,
        }


def decide_wrong_match_cleanup(
    db: Any,
    download_log_id: int,
) -> WrongMatchCleanupDecision:
    """Recompute whether Wrong Matches cleanup may delete a source folder."""
    preview = preview_import_from_download_log(db, download_log_id)
    delete_allowed = bool(
        preview.confident_reject and preview.cleanup_eligible
    )
    return WrongMatchCleanupDecision(
        download_log_id=download_log_id,
        delete_allowed=delete_allowed,
        uncertain=bool(preview.uncertain),
        provenance=CLEANUP_DECISION_PROVENANCE,
        verdict=preview.verdict,
        confident_reject=bool(preview.confident_reject),
        cleanup_eligible=bool(preview.cleanup_eligible),
        preview_decision=preview.decision,
        reason=preview.reason,
        detail=preview.detail,
    )
