"""Release selection and enqueue helpers extracted from cratedigger.py."""

from __future__ import annotations

import copy
import logging
import threading
import time
from collections.abc import Callable, Generator, Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, field, replace
from typing import TYPE_CHECKING, Any, Literal

from lib.browse import _fanout_browse_users, download_filter, get_browse_coordinator
from lib.download import build_active_download_state
from lib.grab_list import DownloadFile, GrabListEntry
from lib.matching import MatchResult, check_for_match, get_album_by_id
from lib.processing_paths import attempt_fingerprint_or_none
from lib.quality import AUDIO_EXTENSIONS, CandidateScore
from lib.slskd_transfers import (
    SlskdEnqueueOutcome,
    cancel_and_delete,
    rederive_transfer_ids,
    slskd_do_enqueue,
    slskd_enqueue_with_outcome,
)

if TYPE_CHECKING:
    from cratedigger import SlskdDirectory, TrackRecord
    from lib.config import CratediggerConfig
    from lib.context import CratediggerContext
    from lib.cycle_counters import CycleCounters
    from lib.search import SearchResult


logger = logging.getLogger("cratedigger")


def _is_dict(value: object) -> bool:
    """``isinstance(value, dict)`` wrapped behind a plain-``bool`` return.

    A bare ``isinstance(x, dict)`` on an ``Any``-typed expression (e.g.
    ``ctx.slskd``'s results — that attribute is typed ``Any`` so tests can
    wire ``FakeSlskdAPI``) permanently flow-narrows that name to bare
    ``dict[Unknown, Unknown]`` for the rest of the branch, propagating
    Unknown through every later ``.get(...)`` on it. Routing the check
    through a helper with no ``TypeGuard`` return type keeps pyright from
    narrowing on the call, preserving the graceful ``.get(key, default)``
    tolerance these external-response readers already rely on. The
    parameter itself is ``object`` (not ``Any``) — a function call never
    narrows its argument's type in the caller's scope regardless of the
    callee's parameter type, so ``object`` here costs no new escape hatch.
    """
    return isinstance(value, dict)


MatchFn = Callable[
    [Sequence["TrackRecord"], str, list[str], str, "CratediggerContext"],
    MatchResult,
]
"""Type of the matching callable injected into ``_iter_wave_matches`` and
``try_enqueue`` / ``try_multi_enqueue``. Production passes
:func:`lib.matching.check_for_match`; tests can pass a stub callable that
controls per-user match outcomes instead of patching the module attribute."""

ReleaseLike = Any
"""Type of ``try_multi_enqueue``'s ``release`` parameter (a MusicBrainz-
mirror-shaped release with a ``.media`` list of discs) -- ``Any`` because
CLAUDE.md's "no adapter code between MB and Discogs" invariant means this
crosses both source shapes. Named so ``try_multi_enqueue`` /
``_try_multi_enqueue_impl`` share ONE ``Any`` token (issue #765 typing
ratchet) instead of the wrapper-plus-impl split duplicating a bare ``Any``
annotation."""

_ClaimResolutionStatus = Literal[
    "accepted",
    "verified_no_acceptance",
    "poll_recovery",
    "stale",
    "failed",
]


@dataclass(frozen=True)
class _ClaimResolution:
    status: _ClaimResolutionStatus
    downloads: list[DownloadFile] | None = None


@dataclass(frozen=True)
class EnqueueAttempt:
    """Outcome of a single enqueue path after matching candidate directories.

    ``candidates`` carries the per-dir forensic scores collected by
    `check_for_match` for every dir touched during this attempt — including
    sub-count gate failures and cross-check rejections. U5 will surface this
    list in the persisted `search_log.candidates` JSONB blob.

    ``pre_filter_skip_count`` aggregates dirs the asymmetric pre-filter
    rejected before browse across every ``check_for_match`` call this
    attempt contributed to; persisted on ``search_log`` for skip-pressure
    telemetry.

    ``conflicting_request_ids`` (issue #1196 item 2) is every OTHER
    request id the cross-request enqueue guard (#1178) skipped a
    candidate for during this attempt -- forensics-only. A non-empty
    value never means the whole attempt failed; other candidates may
    still have matched (``matched=True``).
    """

    matched: bool
    downloads: list[Any] | None = None
    enqueue_failed: bool = False
    candidates: tuple[CandidateScore, ...] = ()
    pre_filter_skip_count: int = 0
    conflicting_request_ids: frozenset[int] = frozenset()


@dataclass(frozen=True)
class FindDownloadMetrics:
    """What one album's find_download walk cost, carried back to the owner.

    A strict subset of ``lib.cycle_counters.CycleCounters``, by the same
    names: five of these also land on the walk's ``search_log`` row as
    per-search attribution, and the owner thread adds all eight into its
    own cycle counters. The three cache counters a worker can accumulate
    but this value does not carry (``cache_errors``,
    ``cache_fuse_tripped``, ``cache_write_errors``) are therefore counted
    only when the drain happens on the owner's context.
    """

    browse_time_s: float = 0.0
    match_time_s: float = 0.0
    peers_browsed: int = 0
    peers_browsed_lazy: int = 0
    fanout_waves: int = 0
    cache_pos_hits: int = 0
    cache_neg_hits: int = 0
    cache_misses: int = 0

    @classmethod
    def from_counters(cls, counters: CycleCounters) -> FindDownloadMetrics:
        """Project this walk's counters out of the worker's whole value."""
        return cls(
            browse_time_s=counters.browse_time_s,
            match_time_s=counters.match_time_s,
            peers_browsed=counters.peers_browsed,
            peers_browsed_lazy=counters.peers_browsed_lazy,
            fanout_waves=counters.fanout_waves,
            cache_pos_hits=counters.cache_pos_hits,
            cache_neg_hits=counters.cache_neg_hits,
            cache_misses=counters.cache_misses,
        )


@dataclass(frozen=True)
class FindDownloadResult:
    """Final outcome of matching + enqueue for one album.

    ``candidates`` is the per-dir forensic score list aggregated across every
    filetype attempt that ran for this album. The same dir under different
    filetypes shows up as two distinct entries — that is intentional
    diagnostic information. U5 plumbs this onto ``SearchResult.candidates``
    and persists the top-20 to ``search_log.candidates`` JSONB.
    """

    outcome: Literal["found", "no_match", "enqueue_failed"]
    grab_entry: GrabListEntry | None = None
    candidates: tuple[CandidateScore, ...] = ()
    metrics: FindDownloadMetrics | None = None
    # Aggregate pre-filter skip count across every (filetype, disc,
    # wave) ``check_for_match`` call this walk contributed to.
    pre_filter_skip_count: int = 0
    # Issue #1196 item 2: union of every ``EnqueueAttempt.
    # conflicting_request_ids`` this walk observed -- the cross-request
    # guard skip's forensics marker, distinct from network absence.
    conflicting_request_ids: frozenset[int] = frozenset()


class _WorkerPipelineDBSource:
    """Sentinel DB source for worker contexts.

    Track and denylist data must be prefetched before worker execution. If a
    worker reaches this source, the caller forgot to snapshot an input.
    """

    def _get_db(self) -> None:
        raise AssertionError("find_download worker attempted owner DB access")

    def get_tracks(self, album_record: object) -> list[TrackRecord]:
        raise AssertionError("find_download worker attempted owner DB access")

    def get_wanted_searchable(
        self, *args: object, **kwargs: object,
    ) -> list[object]:
        raise AssertionError("find_download worker attempted owner DB access")

    def mark_done(self, *args: object, **kwargs: object) -> object:
        raise AssertionError("find_download worker attempted owner DB access")

    def reject_and_requeue(self, *args: object, **kwargs: object) -> object:
        raise AssertionError("find_download worker attempted owner DB access")

    def close(self) -> None:
        raise AssertionError("find_download worker attempted owner DB access")


class FindDownloadOwnerPathError(RuntimeError):
    """Owner-thread orchestration failed after find_download work was queued."""


def prepare_find_download_context(
    album: Any,
    ctx: CratediggerContext,
    search_result: SearchResult | None = None,
) -> CratediggerContext:
    """Build a worker-local context for one album's find_download run."""
    album_id = album.id
    request_id = abs(album_id)
    tracks = get_album_tracks(album, ctx)
    denied_users = _get_denied_users(album_id, ctx)
    coordinator = get_browse_coordinator(
        ctx, ctx.cfg.browse_global_max_workers,
    )
    search_cache = copy.deepcopy(
        search_result.cache_entries
        if search_result is not None
        else ctx.search_cache.get(album_id, {})
    )
    users = set(search_cache)
    user_upload_speed = {
        user: speed
        for user, speed in (
            getattr(search_result, "upload_speeds", None) or ctx.user_upload_speed
        ).items()
        if user in users
    }
    dir_count_source = (
        getattr(search_result, "dir_audio_counts", None)
        or ctx.search_dir_audio_count
    )
    search_dir_audio_count: dict[str, dict[str, int]] = {}
    for user, filetypes in search_cache.items():
        source_counts = dir_count_source.get(user, {})
        wanted_dirs = {
            file_dir
            for dirs in filetypes.values()
            for file_dir in dirs
        }
        selected = {
            file_dir: source_counts[file_dir]
            for file_dir in wanted_dirs
            if file_dir in source_counts
        }
        if selected:
            search_dir_audio_count[user] = selected

    from lib.context import CratediggerContext, CycleCollaborators

    # A per-worker fork, never the owner's instance: PeerCache is not shared
    # across threads. The None arm is for a context built without a peer
    # cache at all; ``connect_from_config`` never returns None (an
    # unreachable Redis gives a real, cold PeerCache with client=None), so
    # no production caller of this function reaches it today.
    peer_cache = ctx.peer_cache.fork() if ctx.peer_cache is not None else None

    plan_execution = (
        getattr(search_result, "plan_execution", None)
        if search_result is not None else None
    )

    return CratediggerContext(
        # Every collaborator named explicitly, never dataclasses.replace on
        # the owner's value: replace() kwargs are checked by pyright for
        # neither name nor type, so the two substitutions below (a worker's
        # own DB source, a FORKED peer cache) would sit beside four
        # unchecked forwards (#1313).
        collaborators=CycleCollaborators(
            cfg=ctx.cfg,
            slskd=ctx.slskd,
            pipeline_db_source=_WorkerPipelineDBSource(),
            download_ownership=ctx.download_ownership,
            claimed_queue_keys_registry=ctx.claimed_queue_keys_registry,
            peer_cache=peer_cache,
        ),
        search_cache={album_id: search_cache},
        folder_cache=ctx.folder_cache,
        user_upload_speed=user_upload_speed,
        search_dir_audio_count=search_dir_audio_count,
        current_album_cache={album_id: album},
        denied_users_cache={request_id: set(denied_users)},
        cooled_down_users=set(ctx.cooled_down_users),
        prefetched_album_tracks={album_id: list(tracks)},
        browse_coordinator=coordinator,
        browse_coordinator_lock=ctx.browse_coordinator_lock,
        active_plan_execution=plan_execution,
    )


def _with_metrics(
    result: FindDownloadResult,
    ctx: CratediggerContext,
) -> FindDownloadResult:
    return FindDownloadResult(
        outcome=result.outcome,
        grab_entry=result.grab_entry,
        candidates=result.candidates,
        metrics=FindDownloadMetrics.from_counters(ctx.counters),
        pre_filter_skip_count=result.pre_filter_skip_count,
        conflicting_request_ids=result.conflicting_request_ids,
    )


def release_trackcount_mode(releases: list[Any]) -> Any:
    """Return the most common track count among candidate releases."""
    track_count: dict[Any, int] = {}

    for release in releases:
        trackcount = release.track_count
        if trackcount in track_count:
            track_count[trackcount] += 1
        else:
            track_count[trackcount] = 1

    most_common_trackcount = None
    max_count = 0

    for trackcount, count in track_count.items():
        if count > max_count:
            max_count = count
            most_common_trackcount = trackcount

    return most_common_trackcount


def choose_release(
    artist_name: str,
    releases: list[Any],
    release_cfg: CratediggerConfig,
) -> Any:
    """Choose the best release candidate to try first."""
    most_common_trackcount = release_trackcount_mode(releases)

    for release in releases:
        if not release.monitored:
            continue
        country = release.country[0] if release.country else None
        if release.format[1] == "x" and release_cfg.allow_multi_disc:
            format_accepted = (
                release.format.split("x", 1)[1] in release_cfg.accepted_formats
            )
        else:
            format_accepted = release.format in release_cfg.accepted_formats
        if format_accepted:
            logger.info(
                f"Selected monitored release for {artist_name}: {release.status}, "
                f"{country}, {release.format}, Mediums: {release.medium_count}, "
                f"Tracks: {release.track_count}, ID: {release.id}"
            )
            return release

    for release in releases:
        country = release.country[0] if release.country else None

        if release.format[1] == "x" and release_cfg.allow_multi_disc:
            format_accepted = (
                release.format.split("x", 1)[1] in release_cfg.accepted_formats
            )
        else:
            format_accepted = release.format in release_cfg.accepted_formats

        if release_cfg.use_most_common_tracknum:
            track_count_bool = release.track_count == most_common_trackcount
        else:
            track_count_bool = True

        if (
            (release_cfg.skip_region_check or country in release_cfg.accepted_countries)
            and format_accepted
            and release.status == "Official"
            and track_count_bool
        ):
            logger.info(
                ", ".join(
                    [
                        f"Selected release for {artist_name}: {release.status}",
                        str(country),
                        release.format,
                        f"Mediums: {release.medium_count}",
                        f"Tracks: {release.track_count}",
                        f"ID: {release.id}",
                    ]
                )
            )
            return release

    if release_cfg.use_most_common_tracknum:
        for release in releases:
            if release.track_count == most_common_trackcount:
                return release

    return releases[0]


def _get_denied_users(album_id: int, ctx: CratediggerContext) -> set[str]:
    """Get denied users from the pipeline DB source_denylist."""
    request_id = abs(album_id)
    if request_id in ctx.denied_users_cache:
        return ctx.denied_users_cache[request_id]
    denied: set[str] = set()
    try:
        db = ctx.pipeline_db_source._get_db()
        denied.update(e["username"] for e in db.get_denylisted_users(request_id))
    except AssertionError:
        raise
    except Exception:  # noqa: BLE001, S110 - best-effort boundary must not mask primary work
        pass
    ctx.denied_users_cache[request_id] = denied
    return denied


def _get_user_dirs(
    results_for_user: dict[str, list[str]],
    allowed_filetype: str,
) -> list[str] | None:
    """Get candidate directories for a user, handling catch-all merging."""
    if allowed_filetype == "*":
        seen: set[str] = set()
        file_dirs: list[str] = []
        for ft_dirs in results_for_user.values():
            for d in ft_dirs:
                if d not in seen:
                    seen.add(d)
                    file_dirs.append(d)
        return file_dirs or None
    if allowed_filetype not in results_for_user:
        return None
    return results_for_user[allowed_filetype]


def _prefixed_directory_files(
    directory: SlskdDirectory,
    file_dir: str,
) -> list[dict[str, Any]]:
    """Build admitted enqueue payloads without mutating browse results.

    A selected audio manifest is indivisible: if slskd did not advertise a
    positive size for every audio file, reject the candidate rather than
    silently dropping tracks or claiming download ownership for known-empty
    material. Import-time validation remains authoritative for the bytes that
    do arrive.
    """
    invalid_audio: list[str] = []
    for file in directory["files"]:
        filename = file["filename"]
        ext = filename.rsplit(".", 1)[-1].lower() if "." in filename else ""
        if ext not in AUDIO_EXTENSIONS:
            continue
        size = file.get("size")
        if isinstance(size, bool) or not isinstance(size, int) or size <= 0:
            invalid_audio.append(filename)
    if invalid_audio:
        logger.warning(
            "Rejecting candidate with missing/non-positive advertised audio "
            "sizes: %s",
            invalid_audio[:3],
        )
        return []
    return [
        {**file, "filename": file_dir + "\\" + file["filename"]}
        for file in directory["files"]
    ]


@dataclass(frozen=True)
class DownloadOwnershipClaim:
    entry: GrabListEntry
    request_id: int | None
    attempted: bool
    claimed: bool
    enqueued_at: str | None = None


@dataclass
class _AttemptTally:
    """What one enqueue attempt accumulates and owes back either way.

    The single-album and multi-disc lanes have almost nothing in common —
    per-peer fallback versus first-match-only per disc, measured at 13 of
    72 and 112 statements shared in order (issue #1313). What they DO share
    is this: whatever happens, the attempt owes the caller every candidate
    score it collected and every dir the pre-filter skipped, because
    ``search_log`` persists both as the request's search forensics whether
    or not anything matched.

    That used to be a convention spelled out at nineteen separate
    ``EnqueueAttempt(...)`` sites, each repeating ``candidates=`` and
    ``pre_filter_skip_count=``, with the skip count carried in a
    one-element list so a generator could mutate it through the caller's
    reference. A return that dropped ``pre_filter_skip_count`` would have
    blanked that half of a request's search forensics with nothing going
    red anywhere; ``candidates`` was luckier, covered indirectly by two
    integration slices that assert the persisted ``search_log`` row. Now
    the shape carries both and the call sites say only what is different
    about that particular outcome.
    """

    candidates: list[CandidateScore] = field(
        default_factory=list[CandidateScore])
    pre_filter_skips: int = 0
    conflicting_request_ids: set[int] = field(default_factory=set[int])

    def record_match(self, match_result: MatchResult) -> None:
        """Absorb one ``check_for_match`` result, matched or not."""
        self.candidates.extend(match_result.candidates)
        self.pre_filter_skips += match_result.pre_filter_skip_count

    def record_conflicts(self, request_ids: set[int]) -> None:
        """Absorb one cross-request guard hit (issue #1178).

        A union, not an assignment: the single-album lane can hit the guard
        on several candidates in one call, and every id it skipped for
        belongs on the attempt even if a LATER candidate went on to match.
        """
        self.conflicting_request_ids |= request_ids

    def matched(self, downloads: list[DownloadFile] | None) -> EnqueueAttempt:
        """The attempt found and kept a candidate.

        ``downloads`` is typed optional because ``_ClaimResolution.downloads``
        is, and Pyright cannot narrow it at the recovery call sites — not
        because a matched attempt may carry nothing. Every path that reaches
        here passes a real list (the poll-recovery paths pass
        ``claim.entry.files``), and it must: ``_try_filetype`` asserts
        ``attempt.downloads is not None`` the moment ``matched`` is true, so
        a matched attempt with no downloads takes down the cycle.
        """
        return self._attempt(matched=True, downloads=downloads)

    def unmatched(self, *, enqueue_failed: bool = False) -> EnqueueAttempt:
        """The attempt kept nothing.

        ``enqueue_failed`` separates "something went wrong after we committed
        to a candidate" — slskd or the claim refusing, a persist failing, an
        exception out of the enqueue loop — from "nothing matched at all",
        which is the ordinary result of a search and not a failure.
        """
        return self._attempt(matched=False, enqueue_failed=enqueue_failed)

    def _attempt(
        self,
        *,
        matched: bool,
        downloads: list[DownloadFile] | None = None,
        enqueue_failed: bool = False,
    ) -> EnqueueAttempt:
        return EnqueueAttempt(
            matched=matched,
            downloads=downloads,
            enqueue_failed=enqueue_failed,
            candidates=tuple(self.candidates),
            pre_filter_skip_count=self.pre_filter_skips,
            conflicting_request_ids=frozenset(self.conflicting_request_ids),
        )


def _album_request_id(album: Any) -> int | None:
    request_id = getattr(album, "db_request_id", None)
    if isinstance(request_id, bool) or not isinstance(request_id, int):
        return None
    return request_id if request_id > 0 else None


def _planned_downloads(
    *,
    username: str,
    file_dir: str,
    files: Sequence[dict[str, Any]],
) -> list[DownloadFile]:
    return [
        DownloadFile(
            filename=str(file["filename"]),
            id="",
            file_dir=file_dir,
            username=username,
            size=int(file.get("size") or 0),
        )
        for file in files
    ]


def _peer_is_online_for_enqueue(username: str, ctx: CratediggerContext) -> bool:
    """Probe slskd's user-status endpoint just before enqueue to avoid
    issuing a doomed enqueue against a peer who has gone offline since
    we cached their browse data.

    Returns False ONLY when slskd reports ``presence == "Offline"``.
    ``Online`` and ``Away`` both return True (away peers can still serve
    uploads). On any exception (transient slskd error, unknown user,
    network blip), fall through and return True — slskd_enqueue_with_outcome
    classifies a real peer-offline rejection via the response body.
    """
    try:
        status = ctx.slskd.users.status(username)
    except Exception:
        logger.debug(
            "users.status probe raised for %s; falling through to enqueue",
            username,
            exc_info=True,
        )
        return True
    presence = ""
    # ``ctx.slskd`` is typed ``Any`` (so tests can wire ``FakeSlskdAPI``),
    # so ``status`` is ``Any`` here too. A bare ``isinstance(status, dict)``
    # would permanently flow-narrow it to bare ``dict[Unknown, Unknown]``
    # for the rest of this branch, propagating Unknown through the
    # ``.get(...)`` below — checking via a plain-``bool``-returning helper
    # (no ``TypeGuard``) keeps ``status`` genuinely ``Any``.
    if _is_dict(status):
        presence_value = status.get("presence")
        if isinstance(presence_value, str):
            presence = presence_value
    return presence != "Offline"


def _planned_grab_entry(
    album: Any,
    files: list[DownloadFile],
    allowed_filetype: str,
) -> GrabListEntry:
    release_date = str(getattr(album, "release_date", "") or "")
    return GrabListEntry(
        album_id=int(getattr(album, "id", 0) or 0),
        files=files,
        filetype=allowed_filetype,
        title=str(getattr(album, "title", "")),
        artist=str(getattr(album, "artist_name", "")),
        year=release_date[:4],
        mb_release_id=str(getattr(album, "db_mb_release_id", "") or ""),
        db_request_id=_album_request_id(album),
        db_source=getattr(album, "db_source", None),
        db_search_filetype_override=getattr(
            album, "db_search_filetype_override", None),
        db_target_format=getattr(album, "db_target_format", None),
    )


def _state_json_for_entry(
    entry: GrabListEntry,
    *,
    enqueued_at: str | None = None,
) -> str:
    return build_active_download_state(
        entry,
        enqueued_at=enqueued_at,
        last_progress_at=enqueued_at,
    ).to_json()


# --- Cross-request enqueue guard (issue #1178) --------------------------
#
# Two concurrent requests for different pressings of the same album can
# both browse to the SAME peer directory and both accept the SAME
# (username, filename) queue keys: nothing anywhere previously asked "is
# this queue key already held by another request" before claiming
# ownership. The first import's materialize/unlink then consumes the
# second request's world (event_path_gone_from_disk), which re-downloads
# the whole album from scratch.
#
# Cross-cycle layer FIRST: lib.download_ownership.DownloadOwnershipWriter's
# open_conflict_check_session opens ONE fresh DB handle per try_enqueue /
# try_multi_enqueue invocation (never per candidate -- PR2 review F7,
# opening one per matched candidate across a whole cycle's worker pool
# risked a connection storm at post-browse convergence) and reads the
# transfer ledger for a PRIOR cycle's accepted ownership whose owner is
# still 'downloading' AND still on its CURRENT attempt (scoped by
# active_download_state's enqueued_at witness -- see
# lib/pipeline_db/transfer_ledger.py's docstring for why an abandoned
# earlier attempt must never block; PR2 review F2). A read-only check with
# no side effect, so a conflict here reports without ever touching the
# same-cycle registry below. Neither call site's own try/except (which
# wraps only the enqueue-outcome resolution, AFTER a successful claim)
# covers this session or its queries, so a DB error here (whether opening
# the session or running a query) is not swallowed as "this one candidate
# failed" -- it propagates uncaught through the whole find_download call
# for the album, ultimately caught at the find-download worker boundary
# (_apply_find_future) and reported as an "error" outcome for the WHOLE
# attempt this cycle. That is still fail CLOSED for what this guard
# exists to prevent: by construction, no earlier candidate in the same
# call can have reached a claim (a claim success returns immediately; a
# claim refusal breaks the loop), so the exception fires with no claim
# and no enqueue anywhere in this call -- the request is untouched and
# retried next cycle on normal cadence. Failing OPEN instead (treating a
# DB blip as "no conflict") would re-admit exactly the #1178 double-claim
# this guard exists to prevent.
#
# Same-cycle layer SECOND (and last): cratedigger.py's find-download
# ThreadPoolExecutor runs every album concurrently within ONE process per
# cycle, so a cycle-scoped registry (``ClaimedQueueKeysRegistry``, one
# instance per cycle, threaded into every worker context by reference --
# see ``prepare_find_download_context`` -- the same pattern as
# ``ctx.download_ownership``) -- checked and updated atomically under one
# lock -- catches a same-cycle race before either side has written
# anything durable. Registration happens ONLY once the cross-cycle check
# has already cleared, so an attempt the cross-cycle layer vetoes never
# "poisons" the registry for its OTHER, otherwise-free keys -- that
# poisoning is a real bug the generated property in
# tests/test_cross_request_enqueue_guard_generated.py found empirically
# when registration ran first (a same-cycle sibling later legitimately
# wanting one of those keys was wrongly blocked by a claim that was never
# actually granted).
#
# A registration whose candidate never ends up actually claiming anything
# is released via ``ClaimedQueueKeysRegistry.release`` -- three call sites
# in try_enqueue / try_multi_enqueue, all for the identical reason: an
# attempt that never actually claimed anything must not keep blocking an
# innocent sibling for the rest of the cycle (PR2 review F5). (1) the
# matched peer turns out to be offline (checked AFTER the guard, per the
# ordering above); (2) the ownership claim itself is refused (the
# request's row no longer matches the expected 'wanted' CAS); (3) the
# enqueue outcome resolves to verified_no_acceptance (the claim was reset
# and confirmed no transfer landed). Ambiguous outcomes (poll_recovery /
# stale, where the request may still genuinely own the transfer) are
# deliberately NOT released.
#
# Residual (deliberately deferred, PR2 review F6): a guard skip logs like
# an ordinary no_match, which could in theory feed the unfindable-detection
# branch-4 signal -- but the blocking window this guard can ever produce is
# bounded by a sibling's single in-flight download attempt, far shorter
# than the unfindable horizon, so this is not treated as a defect here.
#
# One instance of ClaimedQueueKeysRegistry is constructed per cycle
# (main()'s owner ctx); process exit is its natural reset, so it
# carries no TTL/cleanup machinery.


class ClaimedQueueKeysRegistry:
    """Cycle-scoped same-cycle layer of the #1178 cross-request enqueue
    guard.

    Threaded through ``CratediggerContext.claimed_queue_keys_registry`` by
    reference -- one instance per cycle, shared by every find-download
    worker context the owner thread derives via
    ``prepare_find_download_context`` (the same wiring pattern as
    ``ctx.download_ownership``). Replaces an earlier module-global dict +
    test-only reset hook (#1178 PR2 review F7): a cycle-scoped object
    needs no reset hook, and each test constructs its own instance.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._keys: dict[tuple[str, str], int] = {}

    def register_or_conflicting_owners(
        self,
        keys: Sequence[tuple[str, str]],
        request_id: int,
    ) -> set[int]:
        """Atomically claim ``keys`` for ``request_id``, or report the
        OTHER request id(s) already holding any of them.

        The whole key set is checked THEN registered under one lock
        acquisition, so two threads racing the SAME keys can never both
        observe "unclaimed" -- there is no TOCTOU window between the
        check and the registration. Keys already owned by ``request_id``
        itself are never a conflict: multi-wave retries and repeated
        candidates for the SAME request within one cycle must not
        self-block.
        """
        with self._lock:
            conflicting = {
                self._keys[key]
                for key in keys
                if key in self._keys and self._keys[key] != request_id
            }
            if conflicting:
                return conflicting
            for key in keys:
                self._keys[key] = request_id
            return set()

    def release(
        self,
        keys: Sequence[tuple[str, str]],
        request_id: int,
    ) -> None:
        """Release exactly the keys THIS attempt registered (#1178 PR2
        review F5): called when a guard-cleared attempt's claim is
        subsequently refused, or its enqueue is verified to have landed
        nothing, so those keys don't stay registered under a request that
        owns nothing for the rest of the cycle -- which would wrongly
        block an innocent sibling candidate for the same keys. Only
        releases a key still owned by ``request_id``: registration is
        all-or-nothing, so this attempt either owns every key it
        registered or (having conflicted) owns none of them, and this
        call is correctly a no-op in the latter case.
        """
        with self._lock:
            for key in keys:
                if self._keys.get(key) == request_id:
                    del self._keys[key]


ConflictChecker = Callable[[Sequence[tuple[str, str]], int], "set[int]"]
"""Bound to ONE open cross-cycle DB handle for the life of a single
``try_enqueue`` / ``try_multi_enqueue`` invocation -- see
``_cross_cycle_conflict_checker`` and
``DownloadOwnershipWriter.open_conflict_check_session`` (issue #1178 PR2
review F7)."""


@contextmanager
def _cross_cycle_conflict_checker(
    ctx: CratediggerContext,
) -> Generator[ConflictChecker | None]:
    """Open ONE cross-cycle DB session covering every guard check in one
    ``try_enqueue`` / ``try_multi_enqueue`` invocation (issue #1178 PR2
    review F7) -- opening a fresh connection per MATCHED CANDIDATE (the
    guard runs before the peer-online probe, for every candidate, across
    a worker pool sized to the whole cycle) risks a transient connection
    storm at post-browse convergence. Safe to share one handle across
    every guard check in the invocation because ``try_enqueue`` /
    ``try_multi_enqueue`` each run on a single worker thread for their
    whole call. Yields ``None`` (the same "untracked" degrade
    ``_cross_request_conflict_ids`` already documents) when
    ``ctx.download_ownership`` is not wired.
    """
    writer = ctx.download_ownership
    if writer is None:
        yield None
        return
    with writer.open_conflict_check_session() as check:
        yield check


def _cross_request_conflict_ids(
    files: list[DownloadFile],
    request_id: int | None,
    ctx: CratediggerContext,
    *,
    check_cross_cycle: ConflictChecker | None,
) -> set[int]:
    """Return the OTHER request id(s) already holding any of ``files``'s
    ``(username, filename)`` queue keys -- cross-cycle via
    ``check_cross_cycle`` (one DB handle shared across the whole calling
    ``try_enqueue`` / ``try_multi_enqueue`` invocation -- see
    ``_cross_cycle_conflict_checker``), then (only when that is clear)
    same-cycle via ``ctx.claimed_queue_keys_registry`` (issue #1178 PR2).

    Called BEFORE ``_claim_initial_download_ownership`` at every
    ``try_enqueue`` / ``try_multi_enqueue`` candidate. A non-empty result
    means: do not claim, do not enqueue -- the caller skips this candidate
    and continues searching, exactly like the peer-cooldown/denylist skip
    (never a failure outcome; the request stays on normal cadence).

    ``request_id is None`` (no pipeline-DB-backed request) or an empty
    ``files`` list never conflicts -- there is nothing to protect.
    ``check_cross_cycle is None`` (the same "untracked" fallback
    ``_claim_initial_download_ownership`` already recognises, when
    ``ctx.download_ownership`` is not wired) or
    ``ctx.claimed_queue_keys_registry`` itself not wired independently
    degrades each respective layer to a no-op. The cross-cycle check runs
    FIRST and has no side effect, so this ordering never registers a key
    in the same-cycle registry for an attempt the cross-cycle layer is
    about to veto anyway -- see the module comment above for why that
    ordering matters.
    """
    if request_id is None or not files:
        return set()
    keys = [(f.username, f.filename) for f in files]
    if check_cross_cycle is not None:
        cross_cycle_conflict = check_cross_cycle(keys, request_id)
        if cross_cycle_conflict:
            return cross_cycle_conflict
    if ctx.claimed_queue_keys_registry is None:
        return set()
    return ctx.claimed_queue_keys_registry.register_or_conflicting_owners(
        keys, request_id)


def _release_claimed_queue_keys(
    files: list[DownloadFile],
    request_id: int | None,
    ctx: CratediggerContext,
) -> None:
    """Release ``ctx.claimed_queue_keys_registry``'s claim on ``files``'s
    queue keys for ``request_id`` (issue #1178 PR2 review F5) -- a no-op
    when there is no registry wired, no tracked request, or no files.
    """
    if request_id is None or not files:
        return
    if ctx.claimed_queue_keys_registry is None:
        return
    keys = [(f.username, f.filename) for f in files]
    ctx.claimed_queue_keys_registry.release(keys, request_id)


def _claim_initial_download_ownership(
    album: Any,
    files: list[DownloadFile],
    allowed_filetype: str,
    ctx: CratediggerContext,
) -> DownloadOwnershipClaim:
    entry = _planned_grab_entry(album, files, allowed_filetype)
    request_id = entry.db_request_id
    writer = ctx.download_ownership
    if writer is None or request_id is None:
        return DownloadOwnershipClaim(
            entry=entry,
            request_id=request_id,
            attempted=False,
            claimed=False,
        )

    state = build_active_download_state(entry)
    plan_execution = getattr(ctx, "active_plan_execution", None)
    claimed = bool(writer.claim_downloading(
        request_id,
        state.to_json(),
        plan_execution=plan_execution,
    ))
    if not claimed:
        logger.info(
            "Skipped slskd enqueue for request %s because ownership claim "
            "was blocked; request is no longer wanted "
            "(or active plan was regenerated mid-flight)",
            request_id,
        )
    return DownloadOwnershipClaim(
        entry=entry,
        request_id=request_id,
        attempted=True,
        claimed=claimed,
        enqueued_at=state.enqueued_at,
    )


def _entry_with_files(
    entry: GrabListEntry,
    files: list[DownloadFile],
) -> GrabListEntry:
    return replace(entry, files=files)


def _copy_download_observations(
    planned: list[DownloadFile],
    observed: Sequence[Any],
) -> None:
    by_key = {
        (download.username, download.filename): download
        for download in planned
    }
    for source in observed:
        target = by_key.get((source.username, source.filename))
        if target is None:
            continue
        target.id = source.id
        target.status = getattr(source, "status", None)
        target.retry = getattr(source, "retry", None)
        target.bytes_transferred = getattr(source, "bytes_transferred", None)
        target.last_state = getattr(source, "last_state", None)
        target.last_exception = getattr(source, "last_exception", None)


def _clear_download_observations(files: Sequence[DownloadFile]) -> None:
    for file in files:
        file.id = ""
        file.status = None
        file.retry = None
        file.bytes_transferred = None
        file.last_state = None
        file.last_exception = None


def _visible_transfer_files(files: Sequence[DownloadFile]) -> list[DownloadFile]:
    return [
        file for file in files
        if file.id or file.status is not None
    ]


def _visible_claim_transfers(
    claim: DownloadOwnershipClaim,
    ctx: CratediggerContext,
) -> tuple[bool, list[DownloadFile]]:
    verification_entry = copy.deepcopy(claim.entry)
    _clear_download_observations(verification_entry.files)
    snapshot_ok = rederive_transfer_ids(
        verification_entry,
        ctx.slskd,
        not_before=claim.enqueued_at,
    )
    if not snapshot_ok:
        return False, []

    visible = _visible_transfer_files(verification_entry.files)
    if visible:
        _copy_download_observations(claim.entry.files, verification_entry.files)
    return True, visible


def _persist_claimed_download_state(
    claim: DownloadOwnershipClaim,
    files: list[DownloadFile],
    ctx: CratediggerContext,
) -> bool:
    if not claim.claimed or claim.request_id is None:
        return True
    writer = ctx.download_ownership
    if writer is None:
        return True
    assert claim.enqueued_at is not None
    entry = _entry_with_files(claim.entry, files)
    updated = bool(writer.update_state_if_downloading(
        claim.request_id,
        _state_json_for_entry(entry, enqueued_at=claim.enqueued_at),
        expected_enqueued_at=claim.enqueued_at,
    ))
    if not updated:
        logger.warning(
            "Accepted slskd enqueue for request %s, but the guarded "
            "active_download_state update was blocked; suppressing stale "
            "attempt result",
            claim.request_id,
        )
    return updated


def _reset_claim_after_verified_no_acceptance(
    claim: DownloadOwnershipClaim,
    ctx: CratediggerContext,
    *,
    reason: str,
) -> _ClaimResolution:
    if not claim.claimed or claim.request_id is None:
        return _ClaimResolution("verified_no_acceptance")
    writer = ctx.download_ownership
    if writer is None:
        return _ClaimResolution("verified_no_acceptance")
    assert claim.enqueued_at is not None

    snapshot_ok, visible = _visible_claim_transfers(claim, ctx)
    if snapshot_ok and not visible:
        writer.reset_after_no_acceptance(claim.request_id)
        return _ClaimResolution("verified_no_acceptance")

    updated = writer.update_state_if_downloading(
        claim.request_id,
        _state_json_for_entry(claim.entry, enqueued_at=claim.enqueued_at),
        expected_enqueued_at=claim.enqueued_at,
    )
    if not updated:
        logger.warning(
            "%s for request %s lost its claim before recovery state "
            "could be persisted; suppressing stale attempt result",
            reason,
            claim.request_id,
        )
        return _ClaimResolution("stale")
    logger.warning(
        "%s for request %s could not prove no slskd transfer exists; "
        "leaving planned download ownership for recovery",
        reason,
        claim.request_id,
    )
    return _ClaimResolution("poll_recovery", claim.entry.files)


def _stamp_enqueue_failure_reason(
    files: Sequence[DownloadFile],
    reason: str | None,
) -> None:
    """Stamp a captured enqueue-failure reason onto every planned file's
    ``last_exception`` before an ambiguous claim is left for poll
    recovery (issue #564 C4/I3) — so the eventual vanished-transfer
    timeout message can name the real cause instead of reporting zero
    evidence. No-op when no reason was captured.
    """
    if not reason:
        return
    stamped = f"enqueue failed: {reason}"
    for f in files:
        f.last_exception = stamped


def _leave_claim_for_poll_recovery(
    claim: DownloadOwnershipClaim,
    ctx: CratediggerContext,
    *,
    reason: str,
) -> _ClaimResolution:
    if not claim.claimed or claim.request_id is None:
        return _ClaimResolution("failed")
    writer = ctx.download_ownership
    if writer is not None:
        assert claim.enqueued_at is not None
        updated = writer.update_state_if_downloading(
            claim.request_id,
            _state_json_for_entry(claim.entry, enqueued_at=claim.enqueued_at),
            expected_enqueued_at=claim.enqueued_at,
        )
        if not updated:
            logger.warning(
                "%s for request %s lost its claim before recovery state "
                "could be persisted; suppressing stale attempt result",
                reason,
                claim.request_id,
            )
            return _ClaimResolution("stale")

    logger.warning(
        "%s for request %s; "
        "leaving planned download ownership for the next poll cycle",
        reason,
        claim.request_id,
    )
    return _ClaimResolution("poll_recovery", claim.entry.files)


def _handle_claimed_partial_failure(
    claim: DownloadOwnershipClaim,
    accepted: list[DownloadFile],
    ctx: CratediggerContext,
) -> _ClaimResolution:
    if not claim.claimed or claim.request_id is None:
        return _ClaimResolution("verified_no_acceptance")
    writer = ctx.download_ownership
    if writer is None:
        return _ClaimResolution("verified_no_acceptance")
    assert claim.enqueued_at is not None

    _visible_claim_transfers(claim, ctx)
    _copy_download_observations(claim.entry.files, accepted)
    state_json = _state_json_for_entry(
        claim.entry,
        enqueued_at=claim.enqueued_at,
    )
    if not writer.update_state_if_downloading(
        claim.request_id,
        state_json,
        expected_enqueued_at=claim.enqueued_at,
    ):
        logger.warning(
            "Partial multi-disc enqueue for request %s lost its claim before "
            "recovery state could be persisted; suppressing stale attempt "
            "result without cancelling transfers",
            claim.request_id,
        )
        return _ClaimResolution("stale")

    accepted_by_key = {
        (download.username, download.filename)
        for download in accepted
    }
    accepted_planned = [
        download for download in claim.entry.files
        if (download.username, download.filename) in accepted_by_key
    ]
    if any(not download.id for download in accepted_planned):
        logger.warning(
            "Partial multi-disc enqueue for request %s could not be verified "
            "as cancelled because accepted transfers lack IDs; leaving "
            "request downloading for recovery",
            claim.request_id,
        )
        return _ClaimResolution("poll_recovery", claim.entry.files)
    files_to_cancel = [download for download in claim.entry.files if download.id]
    cancelled = cancel_and_delete(files_to_cancel, ctx)
    post_cancel_snapshot_ok, visible_after_cancel = _visible_claim_transfers(claim, ctx)
    if cancelled and post_cancel_snapshot_ok and not visible_after_cancel:
        writer.reset_after_no_acceptance(claim.request_id)
        return _ClaimResolution("verified_no_acceptance")

    if not writer.update_state_if_downloading(
        claim.request_id,
        _state_json_for_entry(claim.entry, enqueued_at=claim.enqueued_at),
        expected_enqueued_at=claim.enqueued_at,
    ):
        logger.warning(
            "Partial multi-disc enqueue for request %s lost its claim after "
            "cancellation; suppressing stale attempt result",
            claim.request_id,
        )
        return _ClaimResolution("stale")
    logger.warning(
        "Partial multi-disc enqueue for request %s could not be verified as "
        "cancelled; leaving request downloading for recovery",
        claim.request_id,
    )
    return _ClaimResolution("poll_recovery", claim.entry.files)


def _resolve_enqueue_claim_outcome(
    *,
    outcome: SlskdEnqueueOutcome,
    claim: DownloadOwnershipClaim,
    previously_accepted: list[DownloadFile],
    ctx: CratediggerContext,
    rejected_reason: str,
    ambiguous_reason: str,
) -> _ClaimResolution:
    """Resolve one enqueue result against the request ownership claim."""
    if outcome.status == "accepted" and outcome.downloads is not None:
        return _ClaimResolution("accepted", outcome.downloads)

    if outcome.status == "rejected":
        if previously_accepted:
            resolution = _handle_claimed_partial_failure(
                claim,
                previously_accepted,
                ctx,
            )
        else:
            resolution = _reset_claim_after_verified_no_acceptance(
                claim,
                ctx,
                reason=rejected_reason,
            )
        if (
            resolution.status == "verified_no_acceptance"
            and previously_accepted
            and not claim.claimed
        ):
            cancel_and_delete(previously_accepted, ctx)
        return resolution

    if claim.claimed:
        if previously_accepted:
            _copy_download_observations(
                claim.entry.files,
                previously_accepted,
            )
        _stamp_enqueue_failure_reason(claim.entry.files, outcome.reason)
        return _leave_claim_for_poll_recovery(
            claim,
            ctx,
            reason=ambiguous_reason,
        )

    if previously_accepted:
        cancel_and_delete(previously_accepted, ctx)
    return _ClaimResolution("failed")


def _enqueue_with_claim_outcome(
    *,
    claim: DownloadOwnershipClaim,
    username: str,
    files: list[dict[str, Any]],
    file_dir: str,
    ctx: CratediggerContext,
) -> SlskdEnqueueOutcome:
    # T1 (issue #571): the attempt fingerprint is computed from the WHOLE
    # attempt's planned file set (claim.entry.files) -- not just this
    # call's `files` (a per-disc/per-user subset in multi-disc) -- so
    # every ledger row from every disc's enqueue call carries the same
    # fingerprint, matching what canonical_processing_path derives later
    # from the same full manifest (issue #550 phase 2).
    attempt_fp = attempt_fingerprint_or_none(claim.entry.files)
    # not_before (issue #822 item 3): claim.enqueued_at is captured strictly
    # before this POST (_claim_initial_download_ownership stamps it via
    # build_active_download_state before writer.claim_downloading runs),
    # so it's a valid lower bound for the reconciliation match below --
    # the same boundary rederive_transfer_ids already uses via
    # claim.enqueued_at (_visible_claim_transfers). None in the untracked
    # fallback (no ctx.download_ownership / no request_id), which degrades
    # to the existing all-history reconciliation.
    if claim.claimed:
        return slskd_enqueue_with_outcome(
            username=username,
            files=files,
            file_dir=file_dir,
            ctx=ctx,
            request_id=claim.request_id,
            attempt_fp=attempt_fp,
            not_before=claim.enqueued_at,
        )
    downloads = slskd_do_enqueue(
        username=username,
        files=files,
        file_dir=file_dir,
        ctx=ctx,
        request_id=claim.request_id,
        attempt_fp=attempt_fp,
        not_before=claim.enqueued_at,
    )
    if downloads is None:
        return SlskdEnqueueOutcome(status="unknown")
    return SlskdEnqueueOutcome(status="accepted", downloads=downloads)


def get_album_tracks(album: Any, ctx: CratediggerContext) -> list[TrackRecord]:
    """Get tracks for an album from the pipeline DB source."""
    if album.id in ctx.prefetched_album_tracks:
        return ctx.prefetched_album_tracks[album.id]
    return ctx.pipeline_db_source.get_tracks(album)


def _eligible_user_dirs(
    results: dict[str, dict[str, list[str]]],
    allowed_filetype: str,
    album_id: int,
    ctx: CratediggerContext,
) -> tuple[list[str], dict[str, list[str]]]:
    """Filter+rank users into a fan-out work plan.

    Returns ``(ordered_users, user_dirs)`` where:
      * ``ordered_users`` is the iteration order — descending upload speed,
        skipping cooled-down / denylisted users and users with no candidate
        dirs at this filetype.
      * ``user_dirs`` maps surviving username → candidate dirs at this
        filetype, used to build the fan-out work list.
    """
    denied_users = _get_denied_users(album_id, ctx)
    sorted_users = sorted(
        results.keys(),
        key=lambda u: ctx.user_upload_speed.get(u, 0),
        reverse=True,
    )
    ordered: list[str] = []
    user_dirs: dict[str, list[str]] = {}
    for username in sorted_users:
        if username in ctx.cooled_down_users:
            logger.info(
                f"Skipping user '{username}' for album ID {album_id}: "
                f"on cooldown (recent download failures)"
            )
            continue
        if username in denied_users:
            logger.info(
                f"Skipping user '{username}' for album ID {album_id}: denylisted "
                f"(previously provided mislabeled quality)"
            )
            continue
        file_dirs = _get_user_dirs(results[username], allowed_filetype)
        if file_dirs is None:
            continue
        ordered.append(username)
        user_dirs[username] = file_dirs
    return ordered, user_dirs


def _log_album_browse(
    artist_name: str,
    album_name: str,
    allowed_filetype: str,
    kind: str,
    *,
    matched: bool,
    match_wave: int | None,
    eligible: int,
    peers: int,
    waves: int,
) -> None:
    """Emit a per-album browse-cost summary for #198 instrumentation.

    One line per try_enqueue call (and per disc in try_multi_enqueue).
    Fields chosen so we can answer two open questions: in which wave do
    matches land (validates wave-cap), and how many peers per album
    (validates peer-ranking / negative-cache).
    """
    logger.info(
        f"album_browse: artist={artist_name!r} album={album_name!r} "
        f"filetype={allowed_filetype} kind={kind} matched={matched} "
        f"match_wave={match_wave} eligible={eligible} peers={peers} "
        f"waves={waves}"
    )


def _iter_wave_matches(
    tracks: Sequence[TrackRecord],
    eligible_users: list[str],
    user_dirs: dict[str, list[str]],
    allowed_filetype: str,
    ctx: CratediggerContext,
    tally: _AttemptTally,
    *,
    match_fn: MatchFn = check_for_match,
) -> Iterator[tuple[str, MatchResult, int]]:
    """Yield ``(username, match_result, wave_index)`` for every dir match.

    ``wave_index`` is 0-based and identifies which fan-out wave produced
    the match. Used by callers for per-album browse instrumentation
    (``album_browse:`` log line) so we can validate wave-cap and
    peer-ranking strategies against real data — see #198.

    Wave-based fan-out (issue #198 U3): chunks ``eligible_users`` into waves
    of ``cfg.browse_top_k``, runs ``_fanout_browse_users`` to populate
    ``ctx.folder_cache`` for the wave's uncached ``(user, dir)`` pairs in
    parallel, then iterates ``check_for_match`` against the warm cache in
    upload-speed order.

    No client-side per-wave deadline or per-cycle budget — slskd's own
    per-peer TCP read timeout bounds wave wall-time. The previous client
    deadlines were starving the pipeline (see 2026-05-02 regression).

    Side effects: records every ``check_for_match`` result into the caller's
    ``tally`` — matched or not, since a skipped dir is forensics too — and
    bumps primary fan-out browse timing and ``ctx.counters.fanout_waves`` /
    ``ctx.counters.peers_browsed``.

    Caller is responsible for stopping iteration (``break``) once a match is
    enqueued; the generator stops fan-out work as soon as iteration stops.
    """
    cfg = ctx.cfg
    K = cfg.browse_top_k
    for wave_idx, wave_start in enumerate(range(0, len(eligible_users), K)):
        wave = eligible_users[wave_start:wave_start + K]

        work: list[tuple[str, str]] = []
        for username in wave:
            if username in ctx.broken_user:
                continue
            cached = ctx.folder_cache.get(username, {})
            for file_dir in user_dirs.get(username, []):
                if file_dir not in cached:
                    work.append((username, file_dir))

        if work:
            t0 = time.monotonic()
            browse_result = None
            try:
                browse_result = _fanout_browse_users(
                    work, ctx.slskd, ctx,
                    max_workers=cfg.browse_global_max_workers,
                )
            finally:
                elapsed = time.monotonic() - t0
                ctx.counters.browse_time_s += elapsed
            ctx.counters.fanout_waves += 1
            browse_attempts = getattr(browse_result, "browse_attempts", len(work))
            negative_skip_items = set(getattr(browse_result, "negative_skips", ()))
            ctx.peer_cache_negative_skips.update(negative_skip_items)
            negative_skips = len(negative_skip_items)
            ctx.counters.peers_browsed += browse_attempts
            n_returned = sum(
                1 for (u, d) in work if d in ctx.folder_cache.get(u, {})
            )
            logger.info(
                f"wave: K={K} n_uncached={len(work)} n_returned={n_returned} "
                f"n_negative_skips={negative_skips} "
                f"n_browse_attempts={browse_attempts} elapsed_s={elapsed:.1f}"
            )

        for username in wave:
            if username in ctx.broken_user:
                continue
            file_dirs = user_dirs.get(username)
            if not file_dirs:
                continue
            match_result = match_fn(
                tracks, allowed_filetype, file_dirs, username, ctx,
            )
            tally.record_match(match_result)
            if match_result.matched:
                yield username, match_result, wave_idx


def try_enqueue(
    all_tracks: Sequence[TrackRecord],
    results: dict[str, dict[str, list[str]]],
    allowed_filetype: str,
    ctx: CratediggerContext,
    *,
    match_fn: MatchFn = check_for_match,
) -> EnqueueAttempt:
    """Single album match and enqueue.

    Wave-based: eligible users are chunked into waves of
    ``cfg.browse_top_k``; each wave runs ``_fanout_browse_users`` in
    parallel, then iterates matching against the warm cache. Returns on
    the first successful enqueue; falls through to the next user (and
    next wave) on enqueue failure.

    Thin resource-management wrapper: opens ONE cross-cycle DB session
    covering every guard check this call makes, closed on every exit path
    (issue #1178 PR2 review F7) -- the actual matching/claim/enqueue work
    is ``_try_enqueue_impl``, unchanged in shape, just taking the open
    session as a parameter instead of opening its own per guard check.
    """
    with _cross_cycle_conflict_checker(ctx) as check_cross_cycle:
        return _try_enqueue_impl(
            all_tracks, results, allowed_filetype, ctx,
            check_cross_cycle, match_fn=match_fn,
        )


def _try_enqueue_impl(
    all_tracks: Sequence[TrackRecord],
    results: dict[str, dict[str, list[str]]],
    allowed_filetype: str,
    ctx: CratediggerContext,
    check_cross_cycle: ConflictChecker | None,
    *,
    match_fn: MatchFn,
) -> EnqueueAttempt:
    album_id = all_tracks[0]["albumId"]
    album = get_album_by_id(album_id, ctx)
    album_name = album.title
    artist_name = album.artist_name

    eligible, user_dirs = _eligible_user_dirs(results, allowed_filetype, album_id, ctx)
    peers_before = ctx.counters.peers_browsed
    waves_before = ctx.counters.fanout_waves

    had_enqueue_failure = False
    # Issue #1196 item 2: the tally's conflicting ids are the union of every
    # candidate skipped for a cross-request conflict during this whole call,
    # and every return path carries them (matched or not) so the caller can
    # surface the marker regardless of whether a LATER candidate matched.
    tally = _AttemptTally()
    match_wave: int | None = None
    for username, match_result, wave_idx in _iter_wave_matches(
        all_tracks, eligible, user_dirs, allowed_filetype, ctx, tally,
        match_fn=match_fn,
    ):
        if match_wave is None:
            match_wave = wave_idx
        directory = download_filter(allowed_filetype, match_result.directory, ctx.cfg)
        files_to_enqueue = _prefixed_directory_files(directory, match_result.file_dir)
        if not files_to_enqueue:
            logger.warning(
                "Matched %s - %s from %s at %s, but no enqueueable files "
                "remained after filtering and admission; skipping candidate",
                artist_name,
                album_name,
                username,
                allowed_filetype,
            )
            continue
        # Guard checked BEFORE the peer-online probe: a conflicted candidate
        # skips straight to the next one without paying for a network round
        # trip we would only throw away. A registered same-cycle claim that
        # then hits the peer-offline `continue` below is released just like
        # the claim-refused / verified-no-acceptance paths further down --
        # this candidate never actually claimed anything, so those keys
        # must not stay registered under a request that owns nothing for
        # the rest of the cycle (issue #1178 PR2 review F5).
        planned_files = _planned_downloads(
            username=username,
            file_dir=match_result.file_dir,
            files=files_to_enqueue,
        )
        request_id = _album_request_id(album)
        conflicting_requests = _cross_request_conflict_ids(
            planned_files, request_id, ctx,
            check_cross_cycle=check_cross_cycle,
        )
        if conflicting_requests:
            tally.record_conflicts(conflicting_requests)
            logger.info(
                "cross-request enqueue conflict (issue #1178): skipping "
                "%s for album %s from %s -- queue keys already held by "
                "request(s) %s",
                request_id,
                album_id,
                username,
                sorted(conflicting_requests),
            )
            continue
        if not _peer_is_online_for_enqueue(username, ctx):
            _release_claimed_queue_keys(planned_files, request_id, ctx)
            logger.info(
                "peer offline at enqueue: skipping %s for album %s",
                username,
                album_id,
            )
            continue
        claim = _claim_initial_download_ownership(
            album,
            planned_files,
            allowed_filetype,
            ctx,
        )
        if claim.attempted and not claim.claimed:
            _release_claimed_queue_keys(planned_files, request_id, ctx)
            had_enqueue_failure = True
            break
        try:
            outcome = _enqueue_with_claim_outcome(
                claim=claim,
                username=username,
                files=files_to_enqueue,
                file_dir=match_result.file_dir,
                ctx=ctx,
            )
            resolution = _resolve_enqueue_claim_outcome(
                outcome=outcome,
                claim=claim,
                previously_accepted=[],
                ctx=ctx,
                rejected_reason="slskd rejected enqueue",
                ambiguous_reason="slskd enqueue outcome was ambiguous",
            )
            if resolution.status == "accepted":
                assert resolution.downloads is not None
                downloads = resolution.downloads
                if not _persist_claimed_download_state(claim, downloads, ctx):
                    had_enqueue_failure = True
                    break
                _log_album_browse(
                    artist_name, album_name, allowed_filetype, "single",
                    matched=True, match_wave=match_wave,
                    eligible=len(eligible),
                    peers=ctx.counters.peers_browsed - peers_before,
                    waves=ctx.counters.fanout_waves - waves_before,
                )
                return tally.matched(downloads)
            if resolution.status == "poll_recovery":
                _log_album_browse(
                    artist_name, album_name, allowed_filetype, "single",
                    matched=True, match_wave=match_wave,
                    eligible=len(eligible),
                    peers=ctx.counters.peers_browsed - peers_before,
                    waves=ctx.counters.fanout_waves - waves_before,
                )
                return tally.matched(resolution.downloads)
            if resolution.status == "verified_no_acceptance":
                # Verified no acceptance: the claim was reset and confirmed
                # no transfer landed, so this attempt now owns nothing --
                # release its same-cycle registry claim too (#1178 PR2
                # review F5), or it would keep blocking an innocent
                # sibling for the rest of the cycle.
                _release_claimed_queue_keys(
                    planned_files, claim.request_id, ctx)
            if (
                resolution.status == "verified_no_acceptance"
                and claim.request_id is not None
            ):
                # Surface the rejection in download_log so the failure is
                # visible immediately rather than disappearing into a
                # silent status flip. Today the only path that produces a
                # verified rejection is a peer-offline classification from
                # slskd_enqueue_with_outcome (see _is_user_offline_http_error).
                db = ctx.pipeline_db_source._get_db()
                db.log_download(
                    request_id=claim.request_id,
                    soulseek_username=username,
                    filetype=allowed_filetype,
                    outcome="user_offline",
                    error_message=outcome.reason or "user offline at enqueue",
                )
            had_enqueue_failure = True
            if resolution.status == "stale":
                break
            logger.info(
                f"Failed to enqueue download to slskd for "
                f"{artist_name} - {album_name} from {username}"
            )
        except Exception as e:  # noqa: BLE001 - boundary converts or isolates collaborator failures
            if claim.claimed:
                recovery = _leave_claim_for_poll_recovery(
                    claim,
                    ctx,
                    reason="slskd enqueue raised after ownership claim",
                )
                if recovery.status == "poll_recovery":
                    _log_album_browse(
                        artist_name, album_name, allowed_filetype, "single",
                        matched=True, match_wave=match_wave,
                        eligible=len(eligible),
                        peers=ctx.counters.peers_browsed - peers_before,
                        waves=ctx.counters.fanout_waves - waves_before,
                    )
                    return tally.matched(recovery.downloads)
                had_enqueue_failure = True
                break
            had_enqueue_failure = True
            logger.warning(f"Exception enqueueing tracks: {e}")
            logger.info(
                f"Exception enqueueing download to slskd for "
                f"{artist_name} - {album_name} from {username}"
            )
    logger.info(f"Failed to enqueue {artist_name} - {album_name}")
    _log_album_browse(
        artist_name, album_name, allowed_filetype, "single",
        matched=False, match_wave=match_wave,
        eligible=len(eligible),
        peers=ctx.counters.peers_browsed - peers_before,
        waves=ctx.counters.fanout_waves - waves_before,
    )
    return tally.unmatched(enqueue_failed=had_enqueue_failure)


def try_multi_enqueue(
    release: ReleaseLike,
    all_tracks: Sequence[TrackRecord],
    results: dict[str, dict[str, list[str]]],
    allowed_filetype: str,
    ctx: CratediggerContext,
    *,
    match_fn: MatchFn = check_for_match,
) -> EnqueueAttempt:
    """Locate and enqueue a multi-disc album.

    Uses the same wave-based fan-out as ``try_enqueue``, applied per disc.
    The folder cache populated by disc-1's waves carries into disc-2 (and
    so on) — successive discs find their peers warm-cached and skip the
    fan-out network round-trip.

    Unlike ``try_enqueue``'s per-user wave loop, each disc takes only the
    FIRST match (``next(_iter_wave_matches(...))``) rather than iterating
    candidates. A cross-request enqueue-guard hit (issue #1178 PR2 review
    F4) therefore skips the WHOLE multi-disc candidate for this cycle —
    there is no per-peer fallback to try next — and the request picks up
    again on normal cadence. This is accepted, documented behaviour:
    multi-disc candidates are already scarce, and per-peer fallback here
    would multiply the disc-matching cost combinatorially.

    Thin resource-management wrapper, same pattern as ``try_enqueue``: a
    single cross-cycle DB session covers this whole call's guard check(s)
    (issue #1178 PR2 review F7); the actual work is ``_try_multi_enqueue_impl``.
    """
    with _cross_cycle_conflict_checker(ctx) as check_cross_cycle:
        return _try_multi_enqueue_impl(
            release, all_tracks, results, allowed_filetype, ctx,
            check_cross_cycle, match_fn=match_fn,
        )


def _try_multi_enqueue_impl(
    release: ReleaseLike,
    all_tracks: Sequence[TrackRecord],
    results: dict[str, dict[str, list[str]]],
    allowed_filetype: str,
    ctx: CratediggerContext,
    check_cross_cycle: ConflictChecker | None,
    *,
    match_fn: MatchFn,
) -> EnqueueAttempt:
    split_release: list[dict[str, Any]] = []
    for media in release.media:
        disk_tracks: list[TrackRecord] = [
            track for track in all_tracks
            if track["mediumNumber"] == media.medium_number
        ]
        disk: dict[str, Any] = {}
        disk["source"] = None
        disk["tracks"] = disk_tracks
        disk["disk_no"] = media.medium_number
        disk["disk_count"] = len(release.media)
        split_release.append(disk)
    total = len(split_release)
    count_found = 0
    album_id = all_tracks[0]["albumId"]
    album = get_album_by_id(album_id, ctx)
    album_name = album.title
    artist_name = album.artist_name
    eligible, user_dirs = _eligible_user_dirs(results, allowed_filetype, album_id, ctx)
    tally = _AttemptTally()
    # #550 defect #1: a peer's per-disc sibling folders can cross-match —
    # disc N's tracks strict-accept an EARLIER disc's folder (radio-series
    # titles restart per disc; a 0.5 filename ratio even tolerates
    # "Disc 1"/"Disc 2" title differences). Exclude every already-assigned
    # (username, file_dir) from later discs' candidates so one folder can
    # never source two discs.
    used_sources: set[tuple[str, str]] = set()
    for disk in split_release:
        ctx.negative_matches.clear()
        peers_before = ctx.counters.peers_browsed
        waves_before = ctx.counters.fanout_waves
        remaining_user_dirs = {
            dirs_username: [
                dir_name for dir_name in dirs
                if (dirs_username, dir_name) not in used_sources
            ]
            for dirs_username, dirs in user_dirs.items()
        }
        first_match = next(
            _iter_wave_matches(
                disk["tracks"], eligible, remaining_user_dirs,
                allowed_filetype, ctx, tally, match_fn=match_fn,
            ),
            None,
        )
        if first_match is None:
            _log_album_browse(
                artist_name, album_name, allowed_filetype,
                f"multi-disc{disk['disk_no']}",
                matched=False, match_wave=None,
                eligible=len(eligible),
                peers=ctx.counters.peers_browsed - peers_before,
                waves=ctx.counters.fanout_waves - waves_before,
            )
            return tally.unmatched()
        username, match_result, match_wave = first_match
        directory = download_filter(
            allowed_filetype, match_result.directory, ctx.cfg,
        )
        files_to_enqueue = _prefixed_directory_files(directory, match_result.file_dir)
        if not files_to_enqueue:
            logger.warning(
                "Matched %s - %s disc %s from %s at %s, but no files "
                "remained after filtering and admission; aborting multi-disc "
                "candidate",
                artist_name,
                album_name,
                disk["disk_no"],
                username,
                allowed_filetype,
            )
            _log_album_browse(
                artist_name, album_name, allowed_filetype,
                f"multi-disc{disk['disk_no']}",
                matched=False, match_wave=match_wave,
                eligible=len(eligible),
                peers=ctx.counters.peers_browsed - peers_before,
                waves=ctx.counters.fanout_waves - waves_before,
            )
            return tally.unmatched()
        _log_album_browse(
            artist_name, album_name, allowed_filetype,
            f"multi-disc{disk['disk_no']}",
            matched=True, match_wave=match_wave,
            eligible=len(eligible),
                peers=ctx.counters.peers_browsed - peers_before,
                waves=ctx.counters.fanout_waves - waves_before,
        )
        disk["source"] = (username, directory, match_result.file_dir)
        used_sources.add((username, match_result.file_dir))
        count_found += 1
        logger.info(
            "MANIFEST-TRACE multidisc-match request=%s disc=%s/%s user=%s "
            "disc_files=%s file_dir=%r",
            _album_request_id(album),
            disk["disk_no"],
            total,
            username,
            len(files_to_enqueue),
            match_result.file_dir,
        )
    if count_found == total:
        planned_downloads: list[DownloadFile] = []
        for disk in split_release:
            username, directory, file_dir = disk["source"]
            files_to_enqueue = _prefixed_directory_files(directory, file_dir)
            if not files_to_enqueue:
                logger.warning(
                    "Matched %s - %s disc %s from %s at %s, but no files "
                    "remained after filtering and admission; aborting multi-disc "
                    "candidate",
                    artist_name,
                    album_name,
                    disk["disk_no"],
                    username,
                    allowed_filetype,
                )
                return tally.unmatched()
            disk_planned = _planned_downloads(
                username=username,
                file_dir=file_dir,
                files=files_to_enqueue,
            )
            for file in disk_planned:
                file.disk_no = disk["disk_no"]
                file.disk_count = disk["disk_count"]
            planned_downloads.extend(disk_planned)
        # #550 fail-closed coverage gate: every downstream stage (slskd
        # transfers, observation copying, event stamping, the import
        # manifest check) keys by (username, filename), so duplicate keys
        # silently collapse to a partial-disc manifest that validates as
        # "extra audio, no missing". A grab that cannot prove full unique
        # coverage is not a grab — keep searching.
        unique_transfer_keys = {
            (file.username, file.filename) for file in planned_downloads
        }
        if len(unique_transfer_keys) < len(planned_downloads):
            logger.warning(
                "MULTI-DISC UNDER-COVERAGE: request=%s planned=%s unique=%s "
                "— duplicate transfer identities across discs; rejecting "
                "candidate and continuing search",
                _album_request_id(album),
                len(planned_downloads),
                len(unique_transfer_keys),
            )
            return tally.unmatched()
        conflicting_requests = _cross_request_conflict_ids(
            planned_downloads, _album_request_id(album), ctx,
            check_cross_cycle=check_cross_cycle,
        )
        if conflicting_requests:
            tally.record_conflicts(conflicting_requests)
            logger.warning(
                "MULTI-DISC CROSS-REQUEST CONFLICT (issue #1178): "
                "request=%s queue keys already held by request(s) %s; "
                "rejecting candidate and continuing search",
                _album_request_id(album),
                sorted(conflicting_requests),
            )
            return tally.unmatched()
        claim = _claim_initial_download_ownership(
            album,
            planned_downloads,
            allowed_filetype,
            ctx,
        )
        if claim.attempted and not claim.claimed:
            # This candidate never actually claimed anything -- release
            # its same-cycle registry claim (#1178 PR2 review F5).
            _release_claimed_queue_keys(
                planned_downloads, _album_request_id(album), ctx)
            return tally.unmatched(enqueue_failed=True)

        all_downloads: list[DownloadFile] = []
        enqueued = 0
        for disk in split_release:
            username, directory, file_dir = disk["source"]
            files_to_enqueue = _prefixed_directory_files(directory, file_dir)
            try:
                outcome = _enqueue_with_claim_outcome(
                    claim=claim,
                    username=username,
                    files=files_to_enqueue,
                    file_dir=file_dir,
                    ctx=ctx,
                )
                resolution = _resolve_enqueue_claim_outcome(
                    outcome=outcome,
                    claim=claim,
                    previously_accepted=all_downloads,
                    ctx=ctx,
                    rejected_reason=(
                        "slskd rejected first multi-disc enqueue"
                        if not all_downloads
                        else "slskd rejected multi-disc enqueue"
                    ),
                    ambiguous_reason=(
                        "slskd enqueue outcome was ambiguous"
                        if not all_downloads
                        else "multi-disc enqueue outcome was ambiguous"
                    ),
                )
                if resolution.status == "accepted":
                    assert resolution.downloads is not None
                    downloads = resolution.downloads
                    for file in downloads:
                        file.disk_no = disk["disk_no"]
                        file.disk_count = disk["disk_count"]
                    all_downloads.extend(downloads)
                    enqueued += 1
                else:
                    logger.info(
                        f"Failed to enqueue download to slskd for "
                        f"{artist_name} - {album_name} from {username}"
                    )
                    if resolution.status == "poll_recovery":
                        return tally.matched(resolution.downloads)
                    if resolution.status == "verified_no_acceptance":
                        # Verified no acceptance: the claim was reset and
                        # confirmed no transfer landed -- release the
                        # same-cycle registry claim too (#1178 PR2 review
                        # F5), or it would keep blocking an innocent
                        # sibling for the rest of the cycle.
                        _release_claimed_queue_keys(
                            planned_downloads, claim.request_id, ctx)
                    return tally.unmatched(enqueue_failed=True)
            except Exception:
                logger.exception("Exception enqueueing tracks")
                logger.info(
                    f"Exception enqueueing download to slskd for "
                    f"{artist_name} - {album_name} from {username}"
                )
                if claim.claimed:
                    if all_downloads:
                        _copy_download_observations(
                            claim.entry.files,
                            all_downloads,
                        )
                    reason = (
                        "multi-disc enqueue raised after ownership claim"
                        if all_downloads
                        else "slskd enqueue raised after ownership claim"
                    )
                    recovery = _leave_claim_for_poll_recovery(
                        claim,
                        ctx,
                        reason=reason,
                    )
                    if recovery.status == "poll_recovery":
                        return tally.matched(recovery.downloads)
                elif all_downloads:
                    cancel_and_delete(all_downloads, ctx)
                return tally.unmatched(enqueue_failed=True)
        logger.info(
            "MANIFEST-TRACE multidisc-enqueue request=%s enqueued_discs=%s/%s "
            "planned_files=%s accepted_files=%s",
            _album_request_id(album),
            enqueued,
            total,
            len(planned_downloads),
            len(all_downloads),
        )
        if enqueued == total:
            if not _persist_claimed_download_state(claim, all_downloads, ctx):
                return tally.unmatched(enqueue_failed=True)
            return tally.matched(all_downloads)
        if len(all_downloads) > 0:
            recovery = _handle_claimed_partial_failure(
                claim,
                all_downloads,
                ctx,
            )
            if recovery.status == "poll_recovery":
                return tally.matched(recovery.downloads)
            if not claim.claimed:
                cancel_and_delete(all_downloads, ctx)
        return tally.unmatched(enqueue_failed=True)

    # Unreachable, and required anyway: every path through the per-disc loop
    # above either returns or increments ``count_found``, so ``count_found ==
    # total`` always holds by the time control gets here — but Pyright cannot
    # prove that, and the function must return an EnqueueAttempt. A mutant
    # flipping this to ``tally.matched(None)`` therefore survives the whole
    # suite (issue #1313, mutant runner mutant 6): that is an unreachable
    # statement, not a coverage gap, and no test can close it.
    return tally.unmatched()


def _try_filetype(
    album: Any,
    results: dict[str, dict[str, list[str]]],
    allowed_filetype: str,
    ctx: CratediggerContext,
) -> FindDownloadResult:
    """Try to match and enqueue an album at a specific filetype quality."""
    album_id = album.id
    artist_name = album.artist_name
    releases = list(album.releases)
    has_monitored = any(r.monitored for r in releases)
    had_enqueue_failure = False
    accumulated: list[CandidateScore] = []
    pre_filter_skip_count_total = 0
    # Issue #1196 item 2: union of every EnqueueAttempt's
    # conflicting_request_ids across every release/wave this filetype
    # attempt tried.
    conflicting_ids_total: set[int] = set()

    for _ in range(len(releases)):
        if not releases:
            break
        release = choose_release(artist_name, releases, ctx.cfg)
        releases.remove(release)
        all_tracks = get_album_tracks(album, ctx)
        if not all_tracks:
            logger.warning(
                f"No tracks for {artist_name} - {album.title} "
                f"(release {release.id}) — skipping"
            )
            continue

        attempt = try_enqueue(all_tracks, results, allowed_filetype, ctx)
        accumulated.extend(attempt.candidates)
        pre_filter_skip_count_total += attempt.pre_filter_skip_count
        conflicting_ids_total |= attempt.conflicting_request_ids
        if not attempt.matched and len(release.media) > 1:
            attempt = try_multi_enqueue(
                release, all_tracks, results, allowed_filetype, ctx
            )
            accumulated.extend(attempt.candidates)
            pre_filter_skip_count_total += attempt.pre_filter_skip_count
            conflicting_ids_total |= attempt.conflicting_request_ids

        if attempt.matched:
            assert attempt.downloads is not None
            from lib.import_manifest import manifest_trace_summary
            logger.info(
                "MANIFEST-TRACE grab-accept request=%s album=%r filetype=%s "
                "%s release_tracks=%s release_media=%s path=%s",
                album.db_request_id,
                album.title,
                allowed_filetype,
                manifest_trace_summary(attempt.downloads),
                getattr(release, "track_count", "?"),
                len(release.media),
                "multi" if len(release.media) > 1 else "single",
            )
            grab_entry = GrabListEntry(
                album_id=album_id,
                files=attempt.downloads,
                filetype=allowed_filetype,
                title=album.title,
                artist=artist_name,
                year=album.release_date[0:4],
                mb_release_id=release.foreign_release_id,
                db_request_id=album.db_request_id,
                db_source=album.db_source,
                db_search_filetype_override=album.db_search_filetype_override,
                db_target_format=album.db_target_format,
            )
            return FindDownloadResult(
                outcome="found",
                grab_entry=grab_entry,
                candidates=tuple(accumulated),
                pre_filter_skip_count=pre_filter_skip_count_total,
                conflicting_request_ids=frozenset(conflicting_ids_total),
            )

        if attempt.enqueue_failed:
            had_enqueue_failure = True

        if has_monitored and release.monitored:
            logger.info(
                f"Monitored release ({release.track_count} tracks) not found on "
                f"Soulseek for {artist_name} - {album.title} at quality "
                f"{allowed_filetype}, skipping non-monitored releases"
            )
            break
        if has_monitored and not release.monitored:
            break

    return FindDownloadResult(
        outcome="enqueue_failed" if had_enqueue_failure else "no_match",
        candidates=tuple(accumulated),
        pre_filter_skip_count=pre_filter_skip_count_total,
        conflicting_request_ids=frozenset(conflicting_ids_total),
    )


def find_download(
    album: Any,
    ctx: CratediggerContext,
) -> FindDownloadResult:
    """Walk search results and enqueue the best matching download."""
    album_id = album.id
    artist_name = album.artist_name
    results = ctx.search_cache[album_id]

    ctx.negative_matches.clear()
    ctx.current_album_cache[album_id] = album

    from lib.quality import effective_search_tiers

    filetypes_to_try, catch_all = effective_search_tiers(
        album.db_search_filetype_override, album.db_target_format,
        list(ctx.cfg.allowed_filetypes))

    if album.db_search_filetype_override or album.db_target_format:
        logger.info(
            f"Search override for {artist_name} - {album.title}: "
            f"searching {filetypes_to_try}"
        )

    had_enqueue_failure = False
    accumulated: list[CandidateScore] = []
    pre_filter_skip_count_total = 0
    # Issue #1196 item 2: union across every filetype/catch-all walk.
    conflicting_ids_total: set[int] = set()
    for allowed_filetype in filetypes_to_try:
        logger.info(f"Checking for Quality: {allowed_filetype}")
        result = _try_filetype(album, results, allowed_filetype, ctx)
        accumulated.extend(result.candidates)
        pre_filter_skip_count_total += result.pre_filter_skip_count
        conflicting_ids_total |= result.conflicting_request_ids
        if result.outcome == "found":
            return _with_metrics(FindDownloadResult(
                outcome="found",
                grab_entry=result.grab_entry,
                candidates=tuple(accumulated),
                pre_filter_skip_count=pre_filter_skip_count_total,
                conflicting_request_ids=frozenset(conflicting_ids_total),
            ), ctx)
        if result.outcome == "enqueue_failed":
            had_enqueue_failure = True

    if (
        catch_all
        and "*" not in [ft.strip() for ft in (ctx.cfg.allowed_filetypes or ())]
    ):
        logger.info(
            f"No match at preferred quality for {artist_name} - {album.title}, "
            f"trying catch-all (any audio format)"
        )
        result = _try_filetype(album, results, "*", ctx)
        accumulated.extend(result.candidates)
        pre_filter_skip_count_total += result.pre_filter_skip_count
        conflicting_ids_total |= result.conflicting_request_ids
        if result.outcome == "found":
            return _with_metrics(FindDownloadResult(
                outcome="found",
                grab_entry=result.grab_entry,
                candidates=tuple(accumulated),
                pre_filter_skip_count=pre_filter_skip_count_total,
                conflicting_request_ids=frozenset(conflicting_ids_total),
            ), ctx)
        if result.outcome == "enqueue_failed":
            had_enqueue_failure = True

    return _with_metrics(FindDownloadResult(
        outcome="enqueue_failed" if had_enqueue_failure else "no_match",
        candidates=tuple(accumulated),
        pre_filter_skip_count=pre_filter_skip_count_total,
        conflicting_request_ids=frozenset(conflicting_ids_total),
    ), ctx)
