"""Shared operational budgets for the route-backed YouTube album resolver."""

from __future__ import annotations

YOUTUBE_RESOLVER_DEADLINE_SECONDS = 60.0

YOUTUBE_HTTP_CONNECT_TIMEOUT_SECONDS = 5.0
YOUTUBE_HTTP_READ_TIMEOUT_SECONDS = 30.0
YOUTUBE_HTTP_RETRY_TOTAL = 3
YOUTUBE_HTTP_RETRY_DELAY_CAP_SECONDS = 10

# One YT request may make the initial attempt plus three retries. Each attempt
# has bounded connect/read waits and every Retry/Retry-After delay is capped.
YOUTUBE_HTTP_MAX_CALL_SECONDS = (
    (YOUTUBE_HTTP_RETRY_TOTAL + 1)
    * (
        YOUTUBE_HTTP_CONNECT_TIMEOUT_SECONDS
        + YOUTUBE_HTTP_READ_TIMEOUT_SECONDS
    )
    + YOUTUBE_HTTP_RETRY_TOTAL * YOUTUBE_HTTP_RETRY_DELAY_CAP_SECONDS
)

# Production checks the cooperative deadline before and after every opaque
# collaborator. The slowest configured in-flight collaborator is one complete
# YT request (170s); Redis adds at most one second on either side. Persistence
# starts only while still inside the deadline and incomplete work is never
# written. Eight seconds covers JSON/framing/scheduling above the 232s bound.
YOUTUBE_RESOLVER_RESPONSE_BUDGET_SECONDS = (
    YOUTUBE_RESOLVER_DEADLINE_SECONDS
    + YOUTUBE_HTTP_MAX_CALL_SECONDS
    + 10.0
)

# The CLI must outlive the configured server response envelope. This is not a
# cancellation primitive: opaque collaborators retain their own transport
# timeouts, while the resolver prevents further work after an observed breach.
YOUTUBE_ALBUM_API_TIMEOUT_SECONDS = 300.0

assert (
    YOUTUBE_ALBUM_API_TIMEOUT_SECONDS
    > YOUTUBE_RESOLVER_RESPONSE_BUDGET_SECONDS
)
