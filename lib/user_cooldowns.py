"""Per-cycle global user-cooldown loading (issue #39)."""
from __future__ import annotations

import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from lib.context import CratediggerContext

logger = logging.getLogger("cratedigger")


def load_user_cooldowns(ctx: CratediggerContext) -> set[str]:
    """Registered Phase-0 step: populate ``ctx.cooled_down_users``.

    Updates the context's set IN PLACE (clear + update), never replacing the
    object: ``cratedigger.build_phase1_context`` forwards the set by
    reference, and in-place mutation keeps every alias coherent no matter
    when it was taken — replacement would strand any alias captured before
    this step ran.

    DB failures deliberately propagate to ``lib/convergence.py``: the registry
    owns cycle-preserving failure isolation.
    """
    db = ctx.pipeline_db_source._get_db()
    cooled = set(db.get_cooled_down_users())
    ctx.cooled_down_users.clear()
    ctx.cooled_down_users.update(cooled)
    if cooled:
        logger.info(f"User cooldowns active: {', '.join(sorted(cooled))}")
    return cooled
