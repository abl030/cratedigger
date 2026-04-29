"""Label GET route handlers — Discogs (Phase A).

Source-qualified URL prefix follows the artist convention: Phase A
ships `/api/discogs/label/*`; Phase B will add a `/api/label/{mbid}`
parallel route once the MB label adapter exists. The `LabelEntity`
contract is source-tagged and identical across sources so the
frontend renders the same shape regardless of upstream.
"""
from __future__ import annotations

import re
import urllib.error
from typing import TYPE_CHECKING

import msgspec

from web import discogs as discogs_api
from web.routes._overlay import overlay_release_rows

if TYPE_CHECKING:
    from http.server import BaseHTTPRequestHandler


def _server():
    """Lazy import to avoid the circular dependency with server.py."""
    from web import server
    return server


# Auto-flip threshold: if the label's `release_count` exceeds this AND the
# caller did not pass `?include_sublabels=` explicitly, default to False.
# Mirrors `BIG_LABEL_THRESHOLD` in `web/js/labels.js`; keep in sync. The
# recursive sub-label CTE on UMG-class labels takes 30+s upstream and the
# single-threaded HTTPServer here would freeze the whole UI for any other
# in-flight request. Direct API consumers can still opt in by passing
# `?include_sublabels=true` explicitly.
BIG_LABEL_THRESHOLD = 1000

# Accepted spellings for the `?include_sublabels=` query parameter. Anything
# outside this set returns 400 — silently coercing typos masks frontend bugs
# and lets bots scribble cache entries with garbage payload-flag combos.
_INCLUDE_SUBLABELS_TRUE = {"true", "1"}
_INCLUDE_SUBLABELS_FALSE = {"false", "0"}
_INCLUDE_SUBLABELS_VALID = _INCLUDE_SUBLABELS_TRUE | _INCLUDE_SUBLABELS_FALSE

# Pagination defaults + per_page upper bound. The discogs-api mirror clamps
# per_page to 100 internally, but we accept up to 200 here so the bound is
# governed by our policy, not happenstance of upstream behavior.
_DEFAULT_PAGE = 1
_DEFAULT_PER_PAGE = 100
_MAX_PER_PAGE = 200


def _parse_positive_int(
    raw: str, *, max_value: int | None = None
) -> int | None:
    """Parse a positive int with an optional upper clamp.

    Returns the clamped int on valid input, ``None`` on parse failure
    or non-positive values. Caller turns ``None`` into a 400.
    """
    try:
        v = int(raw)
    except (TypeError, ValueError):
        return None
    if v < 1:
        return None
    if max_value is not None and v > max_value:
        return max_value
    return v


def _label_entity_payload(entity) -> dict:
    """Convert a `LabelEntity` Struct to a JSON-safe dict.

    Per `.claude/rules/code-quality.md` § "Wire-boundary types":
    `msgspec.to_builtins` recurses into nested Structs;
    `dataclasses.asdict` would not. We never round-trip a Struct
    through `asdict` — it returns the Struct unchanged and `json.dumps`
    fails downstream.
    """
    return msgspec.to_builtins(entity)


def get_discogs_label_search(
    h: BaseHTTPRequestHandler, params: dict[str, list[str]]
) -> None:
    """`GET /api/discogs/label/search?q=...`.

    Search hits are label entities, not releases — no library overlay
    here. The frontend uses the entity fields (`release_count`,
    `country`, `parent_label_name`) for disambiguation.
    """
    q = params.get("q", [""])[0].strip()
    if not q:
        h._error("Missing query parameter 'q'")  # type: ignore[attr-defined]
        return
    hits = discogs_api.search_labels(q)
    h._json({"results": [_label_entity_payload(e) for e in hits]})  # type: ignore[attr-defined]


def get_discogs_label_detail(
    h: BaseHTTPRequestHandler, params: dict[str, list[str]], label_id: str
) -> None:
    """`GET /api/discogs/label/{id}` — label entity + overlaid catalogue.

    Defaults `include_sublabels=true` per Key Decisions; opt-out via
    `?include_sublabels=false`. Big labels (release_count >
    BIG_LABEL_THRESHOLD) auto-flip to `include_sublabels=False` unless
    the caller explicitly opted in — protects the single-threaded web
    server from the upstream UMG-class recursive CTE (30+s).

    The Discogs adapter raises `urllib.error.HTTPError` on a 404 from
    the mirror; we surface that as a JSON 404 so the frontend can
    render a "label not found" state rather than a 5xx. Both
    `get_label` and `get_label_releases` are wrapped — the label can
    vanish between the two calls.
    """
    # Distinguish "user passed nothing" (apply auto-flip) from "user passed
    # something" (respect their choice). `params` only contains keys actually
    # present in the query string.
    explicit = "include_sublabels" in params
    if explicit:
        raw_flag = params["include_sublabels"][0].strip().lower()
        if raw_flag not in _INCLUDE_SUBLABELS_VALID:
            h._error(  # type: ignore[attr-defined]
                "Invalid include_sublabels — expected one of "
                "true/false/1/0", 400)
            return
        include_sublabels = raw_flag in _INCLUDE_SUBLABELS_TRUE
    else:
        # Default; possibly auto-flipped below once we know release_count.
        include_sublabels = True

    # Pagination — always parsed and forwarded so the adapter call shape is
    # explicit at every site. 400 on invalid input rather than silent default
    # coercion: a frontend pagination bug should fail loudly, not show page 1.
    if "page" in params:
        page = _parse_positive_int(params["page"][0])
        if page is None:
            h._error(  # type: ignore[attr-defined]
                "Invalid page — expected a positive integer", 400)
            return
    else:
        page = _DEFAULT_PAGE
    if "per_page" in params:
        per_page = _parse_positive_int(
            params["per_page"][0], max_value=_MAX_PER_PAGE)
        if per_page is None:
            h._error(  # type: ignore[attr-defined]
                "Invalid per_page — expected a positive integer", 400)
            return
    else:
        per_page = _DEFAULT_PER_PAGE

    try:
        entity = discogs_api.get_label(label_id)
    except urllib.error.HTTPError as e:
        if e.code == 404:
            h._error("Label not found", 404)  # type: ignore[attr-defined]
            return
        raise

    # Auto-flip for big labels — only when the caller did not opt in or out.
    if not explicit and entity.release_count > BIG_LABEL_THRESHOLD:
        include_sublabels = False

    try:
        releases_resp = discogs_api.get_label_releases(
            label_id, include_sublabels=include_sublabels,
            page=page, per_page=per_page)
    except urllib.error.HTTPError as e:
        if e.code == 404:
            h._error("Label not found", 404)  # type: ignore[attr-defined]
            return
        raise
    releases = releases_resp["results"]

    overlay_release_rows(releases, [r["id"] for r in releases])

    # Sub-labels come through the entity's parent — we don't have a
    # `sub_labels` list on the public LabelEntity by design (it's source-
    # agnostic and MB will not return that shape). Phase A surfaces an
    # empty list here; the discogs-api detail endpoint carries
    # `sub_labels`, but the cratedigger adapter elides them on the
    # entity. Frontend (U6) reads `sub_label_name` per release row for
    # the rollup badge, which is what actually matters for rendering.
    sub_labels: list[dict] = []

    h._json({  # type: ignore[attr-defined]
        "label": _label_entity_payload(entity),
        "releases": releases,
        "sub_labels": sub_labels,
        "pagination": releases_resp.get("pagination", {}),
        "include_sublabels": releases_resp.get(
            "include_sublabels", include_sublabels),
        # Plan 003 U4. Adapter sets True when an upstream 503 forced a
        # fallback to include_sublabels=False; the UI surfaces a banner.
        # Default False on every successful response.
        "sub_labels_dropped": releases_resp.get("sub_labels_dropped", False),
    })


# ── Route tables ─────────────────────────────────────────────────────

GET_ROUTES: dict[str, object] = {
    "/api/discogs/label/search": get_discogs_label_search,
}

GET_PATTERNS: list[tuple[re.Pattern[str], object]] = [
    (re.compile(r"^/api/discogs/label/(\d+)$"), get_discogs_label_detail),
]
