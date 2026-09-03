"""Retag ``-W`` divergence cohort audit API (#1093 item 1) and its one
mutation: the per-album file-tag sync (#1260 — the census card's "Write
tags" button)."""

from __future__ import annotations

import logging

import msgspec
from pydantic import BaseModel, Field

from lib.beets_db import beets_authority_availability_category
from lib.beets_tag_sync import (
    TAG_SYNC_HTTP_STATUS,
    sync_album_file_tags_from_borrowed_factory,
)
from lib.retag_divergence_audit import (
    is_valid_album_id,
    parse_after_album_id_cursor,
    scan_retag_divergence_from_borrowed_factory,
    scan_retag_divergence_single_album_from_borrowed_factory,
)
from web.routes._pydantic import parse_body
from web.routes._registry import (
    RouteHandler,
    RouteRegistration,
    pattern_route,
    route,
)
from web.runtime import runtime

log = logging.getLogger(__name__)

#: nginx's documented default when a vhost sets no explicit
#: ``proxy_read_timeout`` (http://nginx.org/en/docs/http/ngx_http_proxy_module.html#proxy_read_timeout).
#: The deployed ``music.ablz.au`` vhost sets none, so this is the real
#: ceiling one HTTP request has (#1093 review round 4, finding 3).
NGINX_DEFAULT_PROXY_READ_TIMEOUT_SECONDS = 60.0

#: Wall-clock bound for this route's per-album read LOOP only — NOT the
#: whole request (#1093 review round 4, finding 3). ``beets.
#: list_album_mb_identities()`` (~3.2s measured live) and the JSON encode
#: below both run UNBOUNDED, outside this timer, so the real request time
#: is this deadline PLUS that unbounded overhead (measured live: a 40.0s
#: deadline produced a ~41.9-43.2s total route time). A measured fully
#: UNBOUNDED census over the live ~93k-item library took ~196s, which is
#: why this route bounds the loop at all. A bounded scan reports
#: ``complete=False`` — the report SHAPE never changes; run
#: ``pipeline-cli audit retag-divergence`` (no deadline) for the full
#: census, or chain calls with ``after_album_id``/``next_after_album_id``
#: and accumulate the results caller-side across multiple bounded
#: requests — no single bounded response ever reports ``status=="clean"``
#: for the whole library on its own (see :func:`get_retag_divergence_audit`
#: and ``lib/retag_divergence_audit.py``'s module docstring; #1093 review
#: round 5, finding 1).
#: `tests/web/test_routes_retag_divergence_audit.py` pins that this value
#: leaves real margin under ``NGINX_DEFAULT_PROXY_READ_TIMEOUT_SECONDS`` —
#: not merely "less than", which a check asserting only `> 0` would have
#: let a value as large as 10 hours pass.
API_SCAN_DEADLINE_SECONDS = 40.0

# Enforced here, not only in the test pin: a value that merely satisfied
# `> 0` (e.g. accidentally repinned to hours) must fail at import time, not
# only in `TestApiScanDeadlineConstant` (#1093 review round 4, finding 3).
assert API_SCAN_DEADLINE_SECONDS < NGINX_DEFAULT_PROXY_READ_TIMEOUT_SECONDS, (
    "API_SCAN_DEADLINE_SECONDS must leave real margin under the reverse "
    "proxy's read timeout — see the constant's own docstring above"
)


def _parse_after_album_id(
    params: dict[str, list[str]],
) -> tuple[int | None, bool]:
    """Parse the optional resume-cursor query param.

    Returns ``(value, ok)``; ``ok=False`` means the caller supplied a value
    outside the strict cursor grammar and the route should refuse the
    request rather than silently ignore or reinterpret it. Shares
    ``lib.retag_divergence_audit.parse_after_album_id_cursor`` with the
    CLI's ``--after-album-id`` so both surfaces refuse the same malformed
    inputs — a bare ``int()`` here would have silently accepted a leading
    sign, underscore digit-grouping, surrounding whitespace, and non-ASCII
    digit characters (#1093 review round 5, finding 5).
    """
    raw = params.get("after_album_id")
    if not raw or not raw[0]:
        return None, True
    try:
        return parse_after_album_id_cursor(raw[0]), True
    except ValueError:
        return None, False


def get_retag_divergence_audit(h: RouteHandler, params: dict[str, list[str]]) -> None:
    """``?after_album_id=N`` resumes a previous truncated scan — pass the
    prior response's ``next_after_album_id`` to continue the census where
    it stopped, chaining calls until ``next_after_album_id`` comes back
    ``null`` (#1093 review round 4, finding 4). A response ``status`` of
    ``"clean"`` means THIS call, on its own, both started from the true
    beginning (no ``after_album_id`` given) AND ran to completion — the one
    response in a resumed chain that answers "no divergence" for a range it
    did not scan in full will instead be ``"incomplete"``, even with zero
    findings. A caller chaining calls to reconstruct a whole-library
    verdict must accumulate that verdict itself across every response in
    the chain, not read it off any single one (#1093 review round 5,
    finding 1 — see ``lib/retag_divergence_audit.py``'s module docstring
    for the full rationale).

    Status-code mapping: ``clean``/``divergence_found`` → 200 (the audit
    ran and answered the question, whatever the answer); ``incomplete`` →
    409 (wrong state — the world blocked a complete answer, so a caller
    must not read this as "no divergence"); ``beets_unavailable`` → 503
    (transient/retryable — the audit never actually ran at all). Both non-
    200 mappings follow `.claude/rules/code-quality.md` § CLI ⇄ API
    Surface Symmetry's convention table (`409` wrong state, `503`
    transient/retryable) — #1093 review round 4, finding 5.
    `GET /api/audit/world` used to return 200 for its own analogous
    beets-unavailable bucket — a pre-existing deviation from this same
    convention, closed by issue #1355 item 4: it now returns 503 for
    `beets_unavailable` too, matching this route exactly.
    """
    after_album_id, ok = _parse_after_album_id(params)
    if not ok:
        h._error("after_album_id must be an integer")
        return
    rt = runtime()
    try:
        def beets_factory():
            beets = rt.beets_db()
            if beets is None:
                raise FileNotFoundError("Beets DB not configured")
            return beets

        report = scan_retag_divergence_from_borrowed_factory(
            beets_factory,
            deadline_seconds=API_SCAN_DEADLINE_SECONDS,
            after_album_id=after_album_id,
        )
        payload = msgspec.to_builtins(report)
    except Exception:
        log.exception("retag divergence audit failed unexpectedly")
        h._json({"error": "Retag divergence audit failed"}, status=503)
        return
    status_code = {
        "clean": 200,
        "divergence_found": 200,
        "incomplete": 409,
        "beets_unavailable": 503,
    }[report.status]
    h._json(payload, status=status_code)


def get_retag_divergence_audit_album(
    h: RouteHandler, params: dict[str, list[str]], album_id_str: str,
) -> None:
    """``GET /api/audit/retag-divergence/album/<id>`` (#1142).

    A cheap, explicit per-album recheck — roughly ten file reads,
    milliseconds — reusing the SAME pure classifier and tag reader as the
    whole-library census (``lib.retag_divergence_audit.
    scan_retag_divergence_single_album``), never the whole-library scan
    itself. No deadline, no cursor, no partial verdict: this either
    answers for the one named album or it doesn't answer at all.

    Status-code mapping:
      * 200 — ``found`` (any album class, including ``agrees`` — an
        explicit per-album check reports agreement too, unlike the
        whole-library report which only ever lists non-agreeing albums)
      * 400 — the id is past SQLite's signed-64-bit ``INTEGER`` range
        (:data:`lib.retag_divergence_audit.SQLITE_MAX_INTEGER`) — invalid
        client input, rejected before ever reaching Beets, never the
        misleading transient/retryable 503 an uncaught
        ``sqlite3.OverflowError`` from binding it as a query parameter
        would otherwise produce (#1142 review N10). Also covers an id
        with MORE digits than Python's own ``int()`` conversion accepts
        at all (``sys.int_info.default_max_str_digits``, 4300) — that
        raises a bare ``ValueError`` on parse, before there is even an
        ``int`` to range-check, and must be caught here rather than
        propagating out to the generic 500/traceback/DB-reconnect path
        (#1142 review N3, fresh round).
      * 404 — ``not_found`` (no album with this id in Beets)
      * 503 — ``beets_unavailable`` (Beets DB not configured, or a
        classified SQLite open/query failure) — same convention as
        :func:`get_retag_divergence_audit`.
    """
    try:
        album_id = int(album_id_str)
    except ValueError:
        h._error(f"album id {album_id_str!r} is out of range")
        return
    if not is_valid_album_id(album_id):
        h._error(f"album id {album_id} is out of range")
        return
    rt = runtime()
    try:
        def beets_factory():
            beets = rt.beets_db()
            if beets is None:
                raise FileNotFoundError("Beets DB not configured")
            return beets

        result = scan_retag_divergence_single_album_from_borrowed_factory(
            beets_factory, album_id,
        )
    except Exception:
        log.exception("per-album retag divergence check failed unexpectedly")
        h._json({"error": "Retag divergence check failed"}, status=503)
        return
    if result.status == "found":
        assert result.album is not None
        h._json(msgspec.to_builtins(result.album))
        return
    if result.status == "not_found":
        h._json({"error": f"No Beets album with id {album_id}"}, status=404)
        return
    h._json(
        {"error": result.unavailable_detail or "Beets DB not available"},
        status=503,
    )


class TagSyncBody(BaseModel):
    """Body of ``POST .../album/<id>/sync-tags`` — the identity the
    operator saw on the card, re-pinned server-side (compare-and-set)."""

    expected_mb_albumid: str = Field(min_length=1)


def post_retag_divergence_sync_tags(
    h: RouteHandler, body: dict[str, object], album_id_str: str,
) -> None:
    """``POST /api/audit/retag-divergence/album/<id>/sync-tags`` (#1260).

    The census card's "Write tags" button: one guarded ``beet write``
    scoped to exactly this album, DB→file, verified by re-reading the
    files through the census's own single-album scan
    (``lib.beets_tag_sync``). The one canonical execution path shared
    with ``pipeline-cli sync-file-tags`` (a thin HTTP adapter to this
    route) and the merge seam's best-effort call.

    Status-code mapping (``lib.beets_tag_sync.TAG_SYNC_HTTP_STATUS``):
      * 200 — ``synced`` (files re-read as agreeing) or
              ``already_synced`` (they already did; no write ran)
      * 400 — invalid body (Pydantic), or an album id past SQLite's
              signed-64-bit range / Python's int-parse limits (mirroring
              the recheck route's own boundary)
      * 404 — ``not_found`` (no Beets album with this id)
      * 409 — ``identity_mismatch`` (the DB no longer names the
              authorized identity — recheck and retry),
              ``db_identity_absent`` (nothing to write), or
              ``residual_divergence`` (the write ran but the re-read
              files still disagree — the payload's ``album`` carries the
              per-item detail)
      * 503 — ``release_locked`` (another process holds the RELEASE
              lock; retry) or ``beets_unavailable``, plus the same
              route-level bare 503 shapes as the recheck route when the
              Beets handle itself cannot be opened
    """
    try:
        album_id = int(album_id_str)
    except ValueError:
        h._error(f"album id {album_id_str!r} is out of range")
        return
    if not is_valid_album_id(album_id):
        h._error(f"album id {album_id} is out of range")
        return
    payload = parse_body(h, body, TagSyncBody)
    if payload is None:
        return
    rt = runtime()
    try:
        beets = rt.beets_db()
    except Exception as exc:
        category = beets_authority_availability_category(exc)
        if category is None and not isinstance(exc, OSError):
            raise
        log.exception(
            "Beets DB could not be opened for the tag sync (%s)",
            category or type(exc).__name__,
        )
        h._json({"error": "Beets DB not available"}, status=503)
        return
    if beets is None:
        h._json({"error": "Beets DB not available"}, status=503)
        return
    try:
        result = sync_album_file_tags_from_borrowed_factory(
            lambda: beets,
            rt.db(),
            album_id=album_id,
            expected_mb_albumid=payload.expected_mb_albumid,
        )
    except Exception:
        log.exception("per-album tag sync failed unexpectedly")
        h._json({"error": "Tag sync failed"}, status=503)
        return
    h._json(
        msgspec.to_builtins(result),
        status=TAG_SYNC_HTTP_STATUS.get(result.outcome, 500),
    )


ROUTES: list[RouteRegistration] = [
    route(
        "GET",
        "/api/audit/retag-divergence",
        get_retag_divergence_audit,
        "Read-only census of albums whose Beets DB identity moved (the "
        "retag) but whose installed file tags did not; accepts "
        "?after_album_id=N to resume a truncated scan.",
        classified=True,
    ),
    pattern_route(
        "GET",
        r"^/api/audit/retag-divergence/album/(\d+)$",
        get_retag_divergence_audit_album,
        "Cheap, explicit per-album retag-divergence recheck — the same "
        "classifier as the whole-library census, over one album's own "
        "files only.",
        classified=True,
    ),
    pattern_route(
        "POST",
        r"^/api/audit/retag-divergence/album/(\d+)/sync-tags$",
        post_retag_divergence_sync_tags,
        "Write one album's file tags from its Beets DB identity (the "
        "census card's Write-tags action) — guarded beet write, verified "
        "by re-reading the files.",
        classified=True,
    ),
]
