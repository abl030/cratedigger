"""pipeline-cli YouTube resolver + rescue-ingest commands (#495 carve).

``youtube-album`` — call the canonical cache-writing web resolver.
``youtube-rescue`` — submit a rescue ingest for one request.
"""
# ruff: noqa: UP037 - quoted Any annotation is part of the typing ratchet

from __future__ import annotations

import argparse
import sys
from collections.abc import Callable
from typing import TYPE_CHECKING, Any, Protocol

import msgspec

from lib.youtube_album_limits import YOUTUBE_ALBUM_API_TIMEOUT_SECONDS

# Typed resolver responses retain the service's explicit outcome-to-exit
# authority. Route-level error objects fall back to the shared HTTP status
# mapping in api_mutations.
from lib.youtube_album_service import (
    OUTCOME_EXIT_CODE,
    YoutubeAlbumResolverResult,
)

# U4 / CLI ⇄ API symmetry: import the YT-rescue ingest service's outcome
# → exit-code mapping with an alias (the youtube_album_service one above
# is already bound). Keep this the single source of truth for the CLI; the
# U5 route imports OUTCOME_HTTP_STATUS from the same module for HTTP-side
# mapping.
from lib.youtube_ingest_service import (
    OUTCOME_EXIT_CODE as YOUTUBE_INGEST_EXIT_CODE,
)
from lib.youtube_ingest_service import (
    default_youtube_ingest_service_factory,
)
from scripts.pipeline_cli.api_mutations import _ApiMutation, _relay_decoded

if TYPE_CHECKING:
    from lib.pipeline_db.rows import AlbumRequestRow, DownloadLogWithEvidenceRow
    from lib.youtube_ingest_service import SubmitResult


class _YoutubeRescueDB(Protocol):
    """Structural ``db`` surface forwarded into the YT-rescue ingest service
    factory (issue #784, #409 pattern).

    Mirrors ``lib.youtube_ingest_service._PipelineDB`` method-for-method so
    ``FakePipelineDB`` / the production ``PipelineDB`` conform without
    importing that private symbol across the module boundary.
    ``cmd_youtube_rescue`` never calls these directly — it passes ``db``
    straight into the injected ``service_factory`` (default
    ``default_youtube_ingest_service_factory``), which builds a
    ``YoutubeIngestService`` typed against the same surface.
    """

    def get_request(self, request_id: int) -> AlbumRequestRow | None: ...

    def get_youtube_album_mapping(
        self, release_group_identifier: str, source: str,
    ) -> list[dict[str, Any]] | None: ...

    def find_youtube_album_mapping_for_release(
        self, *, source: str, release_id: str, browse_id: str,
    ) -> dict[str, Any] | None: ...

    def get_tracks(self, request_id: int) -> list[dict[str, Any]]: ...

    def insert_youtube_running(
        self,
        *,
        request_id: int,
        browse_id: str,
        audio_playlist_id: str | None,
        yt_url: str,
        expected_track_count: int,
        resolver_mapping_id: int | None = None,
        per_track_video_ids: list[str] | None = None,
    ) -> int: ...

    def update_youtube_terminal(
        self, download_log_id: int, outcome: str, metadata_dict: dict[str, Any],
    ) -> None: ...

    def get_download_log_entry(
        self, log_id: int,
    ) -> DownloadLogWithEvidenceRow | None: ...

    def enqueue_import_job(
        self,
        job_type: str,
        *,
        request_id: int | None = None,
        dedupe_key: str | None = None,
        payload: dict[str, Any] | None = None,
        message: str | None = None,
    ) -> Any: ...

    def enqueue_youtube_import_and_mark_success(
        self,
        *,
        download_log_id: int,
        request_id: int,
        dedupe_key: str,
        payload: dict[str, Any],
        message: str,
        terminal_metadata: dict[str, Any],
    ) -> Any: ...

    def find_active_youtube_import_job(
        self, *, request_id: int, browse_id: str,
    ) -> "Any | None": ...


class _SubmitsYoutubeRescue(Protocol):
    """Structural surface the rescue service factory returns — the single
    method ``cmd_youtube_rescue`` calls (``.submit``). Both
    ``YoutubeIngestService`` and the test stub conform (issue #784, #409
    pattern).

    Positional-only: the CLI always calls ``svc.submit(rid, browse)``
    positionally, so the parameter *names* are not part of the contract —
    this lets duck-typed stubs with differently-named params conform."""

    def submit(
        self, request_id: int, browse_id: str, /,
    ) -> SubmitResult: ...


type _YoutubeAlbumApiResponse = YoutubeAlbumResolverResult | dict[str, object]


def _decode_youtube_album_response(
    status: int,
    body: bytes,
) -> _YoutubeAlbumApiResponse:
    """Decode service results while preserving route-level error objects."""
    if status in (200, 404, 503):
        return msgspec.json.decode(body, type=YoutubeAlbumResolverResult)
    return msgspec.json.decode(body, type=dict[str, object])


def _render_youtube_album_response(
    identifier: str,
    response: _YoutubeAlbumApiResponse,
    *,
    json_output: bool,
) -> str:
    """Render the route response using youtube-album's stable CLI contract."""
    if json_output or isinstance(response, dict):
        return msgspec.json.encode(response).decode()

    lines = [
        f"  identifier:             {identifier}",
        f"  outcome:                {response.outcome}",
    ]
    if response.release_group_identifier:
        lines.append(
            "  release group:          "
            f"{response.release_group_identifier} ({response.source})",
        )
    lines.append(f"  from cache:             {response.from_cache}")
    if response.error_message:
        lines.append(f"  error:                  {response.error_message}")
    if response.duration_ms is not None:
        lines.append(f"  latency:                {response.duration_ms} ms")
    if response.youtube_releases:
        lines.append(
            f"  matrix ({len(response.youtube_releases)} YT release(s)):",
        )
        for yt_release in response.youtube_releases:
            year = yt_release.year if yt_release.year is not None else "—"
            lines.append(
                f"    - {yt_release.yt_browse_id}  "
                f"year={year}  tracks={yt_release.track_count}",
            )
            lines.append(f"      url: {yt_release.yt_url}")
            for distance in yt_release.distances:
                distance_label = (
                    f"{distance.distance:.4f}"
                    if distance.distance is not None
                    else "n/a"
                )
                suffix = ""
                if (
                    distance.matched_tracks is not None
                    and distance.total_mb_tracks is not None
                ):
                    suffix = (
                        f"  matched={distance.matched_tracks}/"
                        f"{distance.total_mb_tracks}"
                    )
                error_suffix = (
                    f"  err={distance.error_message}"
                    if distance.error_message
                    else ""
                )
                lines.append(
                    f"      · {distance.mbid}  outcome={distance.outcome}  "
                    f"dist={distance_label}{suffix}{error_suffix}",
                )
    else:
        lines.append("  matrix:                 (empty)")
    return "\n".join(lines)


def _youtube_album_exit_code(
    _status: int,
    response: _YoutubeAlbumApiResponse,
) -> int | None:
    """Use service outcomes for resolver results; defer route errors to HTTP."""
    if isinstance(response, dict):
        return None
    return OUTCOME_EXIT_CODE.get(response.outcome, 1)


def cmd_youtube_album(_db: object, args: argparse.Namespace) -> int:
    """Resolve through the route while preserving service outcome exit codes."""
    return _relay_decoded(
        args.api_endpoint,
        _ApiMutation(
            path="/api/youtube-album",
            body={
                "identifier": args.identifier,
                "refresh": bool(args.refresh),
            },
        ),
        decoder=_decode_youtube_album_response,
        renderer=lambda response: _render_youtube_album_response(
            args.identifier,
            response,
            json_output=bool(args.json),
        ),
        exit_code_for=_youtube_album_exit_code,
        timeout_seconds=YOUTUBE_ALBUM_API_TIMEOUT_SECONDS,
    )


def cmd_youtube_rescue(
    db: _YoutubeRescueDB,
    args: argparse.Namespace,
    *,
    service_factory: Callable[[_YoutubeRescueDB], _SubmitsYoutubeRescue] | None = None,
) -> int:
    """``pipeline-cli youtube-rescue <request_id> <browse_id> [--json]``.

    Submit a YouTube-Music rescue ingest for one album request. Counterpart
    of ``POST /api/pipeline/<id>/youtube-rescue`` (U5). Both surfaces wrap
    ``YoutubeIngestService.submit`` — keep them in sync (see ``CLAUDE.md``
    § "CLI ⇄ API surface symmetry"). The outcome → exit-code mapping is
    imported directly from the service module
    (``YOUTUBE_INGEST_EXIT_CODE``) to keep a single source of truth.

    Exit codes (from ``lib.youtube_ingest_service.OUTCOME_EXIT_CODE``):
      * 0 — ``accepted``
      * 2 — ``request_not_found``
      * 3 — ``no_resolver_mapping``, ``track_count_precheck_failed``
            (semantic input violations)
      * 4 — ``wrong_state`` (request is not ``wanted`` / ``unsearchable``),
            ``in_flight`` (an existing ``youtube_running`` row already
            owns this request — re-issue once it's terminal)
      * 5 — ``transient`` (DB / MB-mirror hiccup; retry)
      * 1 — unknown outcome (safety net)
    """
    factory = service_factory or default_youtube_ingest_service_factory
    svc = factory(db)
    result = svc.submit(int(args.request_id), str(args.browse_id))

    if getattr(args, "json", False):
        print(msgspec.json.encode(result).decode())
    else:
        if result.outcome == "accepted":
            print(
                f"accepted: download_log_id={result.download_log_id}")
        else:
            # Failure paths print classified outcome + detail to stderr
            # so success-only consumers can pipe stdout without noise.
            sys.stderr.write(
                f"{result.outcome}"
                f"{f': {result.detail}' if result.detail else ''}\n"
            )
            if result.download_log_id is not None:
                # ``in_flight`` carries the existing log id; surface so
                # the operator knows where to look.
                sys.stderr.write(
                    f"  existing download_log_id={result.download_log_id}\n"
                )

    return YOUTUBE_INGEST_EXIT_CODE.get(result.outcome, 1)


def add_youtube_subparsers(
    sub: argparse._SubParsersAction[argparse.ArgumentParser],
) -> None:
    """Add ``youtube-album`` / ``youtube-rescue`` (#521 carve out of
    ``routes_meta._build_parser``, verbatim argument definitions)."""
    # youtube-album (U7): MBID/Discogs ID → YT Music album matrix.
    # Counterpart of ``POST /api/youtube-album`` (U8).
    p_ya = sub.add_parser(
        "youtube-album",
        help="Resolve MBID/Discogs ID → YouTube Music album matrix "
             "(auto-widens to release group; N×M beets distances per "
             "YT sibling × MB sibling)",
    )
    p_ya.add_argument(
        "identifier",
        help="MB release/release-group MBID OR Discogs release/master ID "
             "(service auto-discriminates via leaf-then-group fallback)",
    )
    p_ya.add_argument(
        "--refresh", action="store_true",
        help="Bypass BOTH the durable cache (youtube_album_mappings) "
             "AND the in-process Redis HTTP accelerator, forcing a "
             "fresh YouTube Music fetch. The fresh response is then "
             "written back to both layers. (Default: serve from cache.)",
    )
    p_ya.add_argument(
        "--json", action="store_true",
        help="Print the full resolver response as structured JSON "
             "instead of the human-readable distance matrix.",
    )
    # youtube-rescue (U4): submit a YouTube Music rescue ingest for one
    # request. Counterpart of ``POST /api/pipeline/<id>/youtube-rescue``
    # (U5). Both surfaces wrap ``YoutubeIngestService.submit``.
    p_yr = sub.add_parser(
        "youtube-rescue",
        help="Submit a YouTube Music rescue ingest for one request "
             "(requires a resolver mapping; emits a youtube_running "
             "download_log row).",
    )
    p_yr.add_argument(
        "request_id", type=int,
        help="album_requests.id to attach the rescue to",
    )
    p_yr.add_argument(
        "browse_id",
        help="YouTube Music browse_id (e.g. MPREb_...); must already "
             "be cached in youtube_album_mappings for this request's "
             "release group",
    )
    p_yr.add_argument(
        "--json", action="store_true",
        help="Print structured JSON ({outcome, download_log_id, detail}) "
             "instead of plain text.",
    )
