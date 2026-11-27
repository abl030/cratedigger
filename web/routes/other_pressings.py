"""The on-demand "matches another pressing" check (issue #1445).

One route over ``lib/other_pressing_check.check_other_pressings``; the
``pipeline-cli other-pressings`` command relays to it, so both surfaces
share one code path.
"""

import msgspec

from lib.other_pressing_check import PressingCheckOutcome, check_other_pressings
from web.routes._registry import RouteHandler, RouteRegistration, pattern_route
from web.runtime import runtime

#: Every outcome carries operator sentences; the status only says whether
#: the answer is final (200), the row is unknown (404) or the mirror could
#: not be asked (503, try again).
_OUTCOME_STATUS: dict[PressingCheckOutcome, int] = {
    PressingCheckOutcome.CHECKED: 200,
    PressingCheckOutcome.NO_MATCHED_RELEASE: 200,
    PressingCheckOutcome.DISCOGS_RELEASE: 200,
    PressingCheckOutcome.RELEASE_NOT_FOUND: 200,
    PressingCheckOutcome.RELEASE_HAS_NO_TRACKS: 200,
    PressingCheckOutcome.DOWNLOAD_LOG_NOT_FOUND: 404,
    PressingCheckOutcome.MIRROR_UNAVAILABLE: 503,
}


def get_other_pressings(
    h: RouteHandler, params: dict[str, list[str]], download_log_id_str: str,
) -> None:
    """``GET /api/other-pressings/<download_log_id>``: read-only."""
    result = check_other_pressings(
        int(download_log_id_str), pdb=runtime().db())
    h._json(
        msgspec.to_builtins(result),
        status=_OUTCOME_STATUS.get(result.outcome, 500),
    )


ROUTES: list[RouteRegistration] = [
    pattern_route(
        "GET", r"^/api/other-pressings/(\d+)$",
        get_other_pressings,
        "Read-only check whether one download_log row's files match another "
        "pressing in the target's MusicBrainz release group, and whether "
        "the target's listed lengths come from a real disc.",
        classified=True,
    ),
]
