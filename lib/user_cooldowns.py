"""Per-cycle global user-cooldown loading (issue #39)."""
from __future__ import annotations

import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from lib.context import CratediggerContext

logger = logging.getLogger("cratedigger")


def load_user_cooldowns(ctx: CratediggerContext) -> set[str]:
    """Registered Phase-0 step: populate ``ctx.cooled_down_users``.

    Replaces the set object on the context; Phase 1's context is built after
    Phase 0 completes and forwards the set by reference
    (``cratedigger.build_phase1_context``), so both phases see the loaded
    roster.

    DB failures deliberately propagate to ``lib/convergence.py``: the registry
    owns cycle-preserving failure isolation.
    """
    db = ctx.pipeline_db_source._get_db()
    cooled = set(db.get_cooled_down_users())
    ctx.cooled_down_users = cooled
    if cooled:
        logger.info(f"User cooldowns active: {', '.join(sorted(cooled))}")
    return cooled
