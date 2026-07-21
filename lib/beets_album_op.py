"""Legacy typed Beets-operation failure payload.

Selector-based destructive subprocess helpers were retired in issue #762.
``BeetsOpFailure`` remains because historical ``ImportResult`` JSON stores
this exact wire shape under ``disambiguation_failure``.  It is a compatibility
type for persisted audit data, not authority to invoke a Beets mutation.
"""

from __future__ import annotations

from typing import Literal

import msgspec


BeetsOpFailureReason = Literal["timeout", "nonzero_rc", "exception"]


class BeetsOpFailure(msgspec.Struct, frozen=True):
    """Historical typed failure preserved for ImportResult decoding."""

    reason: BeetsOpFailureReason
    detail: str
    selector: str = ""
