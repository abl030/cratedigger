"""slskd search-id write-ahead-ledger sweep (issue #576).

Root cause and design are documented in migration 044 and
``lib/pipeline_db/search_ledger.py``. This module is the convergence half:
every cycle, delete completed slskd searches whose id is in cratedigger's
ledger, healing every leak path a process death can create (kill/SIGTERM
mid-cycle, a submit-retry's half-created earlier attempt, a submit error
after slskd already accepted the POST, a post-accept collection crash).

Invariants (``.claude/rules/code-quality.md`` Red/Green TDD; tests in
``tests/test_slskd_searches.py`` + ``tests/test_search_ledger_generated.py``):

* **I1 (no leak, kill-proof)** — every slskd search cratedigger creates is
  eventually deleted from slskd, even if the creating process died at ANY
  point after the POST. The write-ahead ledger row (inserted BEFORE the
  POST — see ``record_search_id`` call sites in ``cratedigger.py`` /
  ``lib/unfindable_detection_service.py``) is what makes this sweep able
  to find and clean up a search whose creator never got to run its own
  ``finally``-block delete.
* **I2 (write-ahead)** — enforced upstream, at the creation sites and in
  ``lib.slskd_client.SlskdSearchesApi.search_text``'s explicit-id
  contract. This module only reads the ledger; it never writes a search
  id before a POST.
* **I3 (good-citizen, #571 doctrine)** — the sweep NEVER deletes or stops
  an slskd search whose id is not in the ledger. A search cratedigger
  didn't create might belong to a human sharing the instance.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from lib.context import CratediggerContext

logger = logging.getLogger("cratedigger")


# Module-level constants, no config knob (single-operator doctrine —
# .claude/rules/scope.md). GRACE leaves in-flight searches of the
# current/previous cycle alone and gives the operator a UI inspection
# window before the sweep can touch a search. PRUNE_RETENTION keeps the
# ledger table bounded once a row is confirmed swept.
SEARCH_LEDGER_SWEEP_GRACE_S: float = 3600.0
SEARCH_LEDGER_PRUNE_RETENTION_DAYS: int = 7


@dataclass(frozen=True)
class SearchSweepSummary:
    """Aggregate result of one search-ledger sweep pass.

    ``mutated`` gates the cycle's INFO summary line, matching
    ``converge_slskd_orphans``'s Phase 0 contract: a sweep that changed
    nothing stays silent.
    """
    deleted: int = 0
    already_gone: int = 0
    foreign_skipped: int = 0

    @property
    def mutated(self) -> bool:
        return bool(self.deleted or self.already_gone)


def converge_slskd_searches(ctx: "CratediggerContext") -> SearchSweepSummary:
    """Phase 0 convergence (issue #576): reap ledgered slskd searches.

    Per cycle:
      1. Read ledger rows older than GRACE and not yet confirmed deleted.
      2. Fetch every slskd-resident search (best-effort — a fetch failure
         skips this cycle's reconciliation, but pruning still runs).
      3. For each ledgered id whose slskd state starts with ``Completed``,
         delete it (best-effort per id; a failure is logged and the rest
         are still attempted).
      4. For each ledgered id absent from slskd's list, count it as
         already-gone (the fast-path delete in ``execute_search``'s
         ``finally`` already worked) — no action needed.
      5. Ledgered ids still ``InProgress``/``Queued`` are left alone; a
         later cycle's sweep catches them once they settle.
      6. Mark every deleted/already-gone id as swept, then prune rows
         confirmed swept more than ``SEARCH_LEDGER_PRUNE_RETENTION_DAYS``
         ago.

    I3: an slskd search whose id is NOT in the ledger is never touched —
    not deleted, not stopped, not even inspected beyond counting it for
    the summary line.

    Best-effort throughout: this function never raises for an external
    failure. Wrap it in the caller (matching how ``converge_slskd_orphans``
    is invoked in Phase 0) for defense-in-depth regardless.
    """
    db = ctx.pipeline_db_source._get_db()
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(seconds=SEARCH_LEDGER_SWEEP_GRACE_S)
    ledgered_rows = db.get_unswept_search_ids(older_than=cutoff)

    deleted_ids: list[str] = []
    already_gone_ids: list[str] = []
    foreign_skipped = 0

    if ledgered_rows:
        ledgered_ids = {row["search_id"] for row in ledgered_rows}
        all_searches = _fetch_all_searches(ctx.slskd)
        if all_searches is not None:
            live_by_id: dict[Any, dict[str, Any]] = {
                s.get("id"): s for s in all_searches if s.get("id") is not None
            }
            for search_id in ledgered_ids:
                live = live_by_id.get(search_id)
                if live is None:
                    # Already gone — the fast-path delete in
                    # execute_search's finally already worked, or slskd
                    # never actually created it despite the ledger write.
                    already_gone_ids.append(search_id)
                    continue
                state = str(live.get("state") or "")
                if not state.startswith("Completed"):
                    # In-flight — never stop/delete a live search; a
                    # later cycle sweeps it once it settles.
                    continue
                try:
                    ctx.slskd.searches.delete(search_id)
                    deleted_ids.append(search_id)
                except Exception:
                    logger.warning(
                        "SEARCH-LEDGER sweep: failed to delete search %s; "
                        "will retry next cycle", search_id, exc_info=True)

            foreign_skipped = sum(
                1 for s in all_searches
                if s.get("id") not in ledgered_ids
                and str(s.get("state") or "").startswith("Completed"))

            to_mark = deleted_ids + already_gone_ids
            if to_mark:
                db.mark_search_ids_deleted(to_mark)

    try:
        db.prune_search_ledger(
            deleted_before=now - timedelta(days=SEARCH_LEDGER_PRUNE_RETENTION_DAYS))
    except Exception:
        logger.warning("SEARCH-LEDGER sweep: prune failed", exc_info=True)

    summary = SearchSweepSummary(
        deleted=len(deleted_ids),
        already_gone=len(already_gone_ids),
        foreign_skipped=foreign_skipped,
    )
    if summary.mutated:
        logger.info(
            "SEARCH-LEDGER sweep: deleted=%d already_gone=%d foreign_skipped=%d",
            summary.deleted, summary.already_gone, summary.foreign_skipped)
    return summary


def _fetch_all_searches(slskd_client: Any) -> list[dict[str, Any]] | None:
    """Best-effort ``GET /searches`` — ``None`` on failure skips this
    cycle's reconciliation without aborting the sweep (pruning still
    runs)."""
    try:
        return slskd_client.searches.get_all()
    except Exception:
        logger.warning(
            "SEARCH-LEDGER sweep: failed to fetch slskd searches; "
            "skipping reconciliation this cycle", exc_info=True)
        return None
