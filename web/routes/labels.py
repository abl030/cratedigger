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
    `?include_sublabels=false`. The Discogs adapter raises
    `urllib.error.HTTPError` on a 404 from the mirror; we surface that
    as a JSON 404 so the frontend can render a "label not found" state
    rather than a 5xx.
    """
    raw_flag = params.get("include_sublabels", ["true"])[0].strip().lower()
    include_sublabels = raw_flag != "false"

    try:
        entity = discogs_api.get_label(label_id)
    except urllib.error.HTTPError as e:
        if e.code == 404:
            h._error("Label not found", 404)  # type: ignore[attr-defined]
            return
        raise

    releases_resp = discogs_api.get_label_releases(
        label_id, include_sublabels=include_sublabels)
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
    })


# ── Route tables ─────────────────────────────────────────────────────

GET_ROUTES: dict[str, object] = {
    "/api/discogs/label/search": get_discogs_label_search,
}

GET_PATTERNS: list[tuple[re.Pattern[str], object]] = [
    (re.compile(r"^/api/discogs/label/(\d+)$"), get_discogs_label_detail),
]
