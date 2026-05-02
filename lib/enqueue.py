"""Release selection and enqueue helpers extracted from cratedigger.py."""

from __future__ import annotations

from dataclasses import dataclass
import logging
import time
from typing import TYPE_CHECKING, Any, Iterator, Literal, Sequence, cast

from lib.browse import _fanout_browse_users, download_filter
from lib.download import cancel_and_delete, slskd_do_enqueue
from lib.grab_list import GrabListEntry
from lib.matching import MatchResult, check_for_match, get_album_by_id
from lib.quality import CandidateScore

if TYPE_CHECKING:
    from cratedigger import SlskdDirectory, TrackRecord
    from lib.config import CratediggerConfig
    from lib.context import CratediggerContext


logger = logging.getLogger("cratedigger")


@dataclass(frozen=True)
class EnqueueAttempt:
    """Outcome of a single enqueue path after matching candidate directories.

    ``candidates`` carries the per-dir forensic scores collected by
    `check_for_match` for every dir touched during this attempt — including
    sub-count gate failures and cross-check rejections. U5 will surface this
    list in the persisted `search_log.candidates` JSONB blob.
    """

    matched: bool
    downloads: list[Any] | None = None
    enqueue_failed: bool = False
    candidates: tuple[CandidateScore, ...] = ()


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
    candidates: tuple[CandidateScore, ...] = ()


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
    except Exception:
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
    """Build enqueue payloads without mutating cached browse results."""
    return [
        {**file, "filename": file_dir + "\\" + file["filename"]}
        for file in directory["files"]
    ]


def get_album_tracks(album: Any, ctx: CratediggerContext) -> list[TrackRecord]:
    """Get tracks for an album from the pipeline DB source."""
    return cast("list[TrackRecord]", ctx.pipeline_db_source.get_tracks(album))


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


def _iter_wave_matches(
    tracks: Sequence[TrackRecord],
    eligible_users: list[str],
    user_dirs: dict[str, list[str]],
    allowed_filetype: str,
    ctx: CratediggerContext,
    accumulated: list[CandidateScore],
) -> Iterator[tuple[str, MatchResult]]:
    """Yield ``(username, match_result)`` for every dir that matches strictly.

    Wave-based fan-out (issue #198 U3): chunks ``eligible_users`` into waves
    of ``cfg.browse_top_k``, runs ``_fanout_browse_users`` to populate
    ``ctx.folder_cache`` for the wave's uncached ``(user, dir)`` pairs in
    parallel, then iterates ``check_for_match`` against the warm cache in
    upload-speed order. Honors ``cfg.browse_cycle_budget_s`` — short-circuits
    before any wave (and between waves) once cumulative cycle browse time
    exceeds the budget.

    Side effects: appends per-dir ``CandidateScore`` entries into
    ``accumulated`` (caller-owned), bumps ``ctx.cycle_browse_time_s``,
    ``ctx.fanout_waves``, ``ctx.peers_browsed``, ``ctx.peers_timed_out``;
    extends ``ctx.broken_user`` with timed-out usernames (per-cycle scope —
    a fresh ``CratediggerContext`` next cycle starts empty).

    Caller is responsible for stopping iteration (``break``) once a match is
    enqueued; the generator stops fan-out work as soon as iteration stops.
    """
    cfg = ctx.cfg
    if ctx.cycle_browse_time_s >= cfg.browse_cycle_budget_s:
        logger.info(
            f"cycle_browse_budget_exhausted: skipping wave (budget="
            f"{cfg.browse_cycle_budget_s:.0f}s, used="
            f"{ctx.cycle_browse_time_s:.1f}s)"
        )
        return

    K = cfg.browse_top_k
    for wave_start in range(0, len(eligible_users), K):
        wave = eligible_users[wave_start:wave_start + K]

        work: list[tuple[str, str]] = []
        for username in wave:
            cached = ctx.folder_cache.get(username, {})
            for file_dir in user_dirs.get(username, []):
                if file_dir not in cached:
                    work.append((username, file_dir))

        if work:
            t0 = time.monotonic()
            timed_out = _fanout_browse_users(
                work, ctx.slskd, ctx,
                max_workers=cfg.browse_global_max_workers,
                deadline_s=cfg.browse_wave_deadline_s,
            )
            elapsed = time.monotonic() - t0
            ctx.cycle_browse_time_s += elapsed
            ctx.fanout_waves += 1
            ctx.peers_browsed += len(work)
            ctx.peers_timed_out += len(timed_out)
            for username in timed_out:
                if username not in ctx.broken_user:
                    ctx.broken_user.append(username)
            n_returned = sum(
                1 for (u, d) in work if d in ctx.folder_cache.get(u, {})
            )
            logger.info(
                f"wave: K={K} n_uncached={len(work)} n_returned={n_returned} "
                f"n_timed_out={len(timed_out)} elapsed_s={elapsed:.1f}"
            )

        for username in wave:
            if username in ctx.broken_user:
                continue
            file_dirs = user_dirs.get(username)
            if not file_dirs:
                continue
            match_result = check_for_match(
                tracks, allowed_filetype, file_dirs, username, ctx,
            )
            accumulated.extend(match_result.candidates)
            if match_result.matched:
                yield username, match_result

        if ctx.cycle_browse_time_s >= cfg.browse_cycle_budget_s:
            logger.info(
                f"cycle_browse_budget_exhausted: stopping waves (budget="
                f"{cfg.browse_cycle_budget_s:.0f}s, used="
                f"{ctx.cycle_browse_time_s:.1f}s)"
            )
            return


def try_enqueue(
    all_tracks: Sequence[TrackRecord],
    results: dict[str, dict[str, list[str]]],
    allowed_filetype: str,
    ctx: CratediggerContext,
) -> EnqueueAttempt:
    """Single album match and enqueue.

    Wave-based: eligible users are chunked into waves of
    ``cfg.browse_top_k``; each wave runs ``_fanout_browse_users`` in
    parallel, then iterates matching against the warm cache. Returns on
    the first successful enqueue; falls through to the next user (and
    next wave) on enqueue failure.
    """
    album_id = all_tracks[0]["albumId"]
    album = get_album_by_id(album_id, ctx)
    album_name = album.title
    artist_name = album.artist_name

    eligible, user_dirs = _eligible_user_dirs(results, allowed_filetype, album_id, ctx)

    had_enqueue_failure = False
    accumulated: list[CandidateScore] = []
    for username, match_result in _iter_wave_matches(
        all_tracks, eligible, user_dirs, allowed_filetype, ctx, accumulated,
    ):
        directory = download_filter(allowed_filetype, match_result.directory, ctx.cfg)
        files_to_enqueue = _prefixed_directory_files(directory, match_result.file_dir)
        try:
            downloads = slskd_do_enqueue(
                username=username,
                files=files_to_enqueue,
                file_dir=match_result.file_dir,
                ctx=ctx,
            )
            if downloads is not None:
                return EnqueueAttempt(
                    matched=True,
                    downloads=downloads,
                    candidates=tuple(accumulated),
                )
            had_enqueue_failure = True
            logger.info(
                f"Failed to enqueue download to slskd for "
                f"{artist_name} - {album_name} from {username}"
            )
        except Exception as e:
            had_enqueue_failure = True
            logger.warning(f"Exception enqueueing tracks: {e}")
            logger.info(
                f"Exception enqueueing download to slskd for "
                f"{artist_name} - {album_name} from {username}"
            )
    logger.info(f"Failed to enqueue {artist_name} - {album_name}")
    return EnqueueAttempt(
        matched=False,
        enqueue_failed=had_enqueue_failure,
        candidates=tuple(accumulated),
    )


def try_multi_enqueue(
    release: Any,
    all_tracks: Sequence[TrackRecord],
    results: dict[str, dict[str, list[str]]],
    allowed_filetype: str,
    ctx: CratediggerContext,
) -> EnqueueAttempt:
    """Locate and enqueue a multi-disc album.

    Uses the same wave-based fan-out as ``try_enqueue``, applied per disc.
    The folder cache populated by disc-1's waves carries into disc-2 (and
    so on) — successive discs find their peers warm-cached and skip the
    fan-out network round-trip.
    """
    split_release: list[dict[str, Any]] = []
    for media in release.media:
        disk: dict[str, Any] = {}
        disk["source"] = None
        disk["tracks"] = []
        disk["disk_no"] = media.medium_number
        disk["disk_count"] = len(release.media)
        for track in all_tracks:
            if track["mediumNumber"] == media.medium_number:
                disk["tracks"].append(track)
        split_release.append(disk)
    total = len(split_release)
    count_found = 0
    album_id = all_tracks[0]["albumId"]
    album = get_album_by_id(album_id, ctx)
    album_name = album.title
    artist_name = album.artist_name
    eligible, user_dirs = _eligible_user_dirs(results, allowed_filetype, album_id, ctx)
    accumulated: list[CandidateScore] = []
    for disk in split_release:
        ctx.negative_matches.clear()
        first_match = next(
            _iter_wave_matches(
                disk["tracks"], eligible, user_dirs, allowed_filetype, ctx,
                accumulated,
            ),
            None,
        )
        if first_match is None:
            return EnqueueAttempt(matched=False, candidates=tuple(accumulated))
        username, match_result = first_match
        directory = download_filter(
            allowed_filetype, match_result.directory, ctx.cfg,
        )
        disk["source"] = (username, directory, match_result.file_dir)
        count_found += 1
    if count_found == total:
        all_downloads = []
        enqueued = 0
        for disk in split_release:
            username, directory, file_dir = disk["source"]
            files_to_enqueue = _prefixed_directory_files(directory, file_dir)
            try:
                downloads = slskd_do_enqueue(
                    username=username,
                    files=files_to_enqueue,
                    file_dir=file_dir,
                    ctx=ctx,
                )
                if downloads is not None:
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
                    if len(all_downloads) > 0:
                        cancel_and_delete(all_downloads, ctx)
                    return EnqueueAttempt(
                        matched=False,
                        enqueue_failed=True,
                        candidates=tuple(accumulated),
                    )
            except Exception:
                logger.exception("Exception enqueueing tracks")
                logger.info(
                    f"Exception enqueueing download to slskd for "
                    f"{artist_name} - {album_name} from {username}"
                )
                if len(all_downloads) > 0:
                    cancel_and_delete(all_downloads, ctx)
                return EnqueueAttempt(
                    matched=False,
                    enqueue_failed=True,
                    candidates=tuple(accumulated),
                )
        if enqueued == total:
            return EnqueueAttempt(
                matched=True,
                downloads=all_downloads,
                candidates=tuple(accumulated),
            )
        if len(all_downloads) > 0:
            cancel_and_delete(all_downloads, ctx)
        return EnqueueAttempt(
            matched=False,
            enqueue_failed=True,
            candidates=tuple(accumulated),
        )

    return EnqueueAttempt(matched=False, candidates=tuple(accumulated))


def _try_filetype(
    album: Any,
    results: dict[str, dict[str, list[str]]],
    allowed_filetype: str,
    grab_list: dict[int, GrabListEntry],
    ctx: CratediggerContext,
) -> FindDownloadResult:
    """Try to match and enqueue an album at a specific filetype quality."""
    album_id = album.id
    artist_name = album.artist_name
    releases = list(album.releases)
    has_monitored = any(r.monitored for r in releases)
    had_enqueue_failure = False
    accumulated: list[CandidateScore] = []

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
        if not attempt.matched and len(release.media) > 1:
            attempt = try_multi_enqueue(
                release, all_tracks, results, allowed_filetype, ctx
            )
            accumulated.extend(attempt.candidates)

        if attempt.matched:
            assert attempt.downloads is not None
            grab_list[album_id] = GrabListEntry(
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
                outcome="found", candidates=tuple(accumulated),
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
    )


def find_download(
    album: Any,
    grab_list: dict[int, GrabListEntry],
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
    for allowed_filetype in filetypes_to_try:
        logger.info(f"Checking for Quality: {allowed_filetype}")
        result = _try_filetype(album, results, allowed_filetype, grab_list, ctx)
        accumulated.extend(result.candidates)
        if result.outcome == "found":
            return FindDownloadResult(
                outcome="found", candidates=tuple(accumulated),
            )
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
        result = _try_filetype(album, results, "*", grab_list, ctx)
        accumulated.extend(result.candidates)
        if result.outcome == "found":
            return FindDownloadResult(
                outcome="found", candidates=tuple(accumulated),
            )
        if result.outcome == "enqueue_failed":
            had_enqueue_failure = True

    return FindDownloadResult(
        outcome="enqueue_failed" if had_enqueue_failure else "no_match",
        candidates=tuple(accumulated),
    )
