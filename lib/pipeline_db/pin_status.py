"""Closed status taxonomies for media-server Recently Added pins."""
from typing import Literal, get_args


# These Literals and the latest named CHECK constraints in migrations/ are the
# only two authored sync points for the pin-status taxonomies. The frozensets
# are derived, never hand-maintained; TestPinStatusTaxonomySync fails if Python
# and SQL drift apart.
PlexPinStatus = Literal["pending", "done", "skipped"]
PlexTerminalPinStatus = Literal["done", "skipped"]
PLEX_PIN_STATUSES: frozenset[str] = frozenset(get_args(PlexPinStatus))
PLEX_TERMINAL_PIN_STATUSES: frozenset[str] = frozenset(
    get_args(PlexTerminalPinStatus)
)

JellyfinPinStatus = Literal["pending", "done", "skipped", "expired"]
JellyfinTerminalPinStatus = Literal["done", "skipped", "expired"]
JELLYFIN_PIN_STATUSES: frozenset[str] = frozenset(get_args(JellyfinPinStatus))
JELLYFIN_TERMINAL_PIN_STATUSES: frozenset[str] = frozenset(
    get_args(JellyfinTerminalPinStatus)
)
