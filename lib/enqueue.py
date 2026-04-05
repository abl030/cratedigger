"""Release selection and enqueue helpers extracted from soularr.py."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, Sequence, cast

from lib.browse import download_filter
from lib.download import cancel_and_delete, slskd_do_enqueue
from lib.grab_list import GrabListEntry
from lib.matching import check_for_match, get_album_by_id

if TYPE_CHECKING:
    from soularr import SlskdDirectory, TrackRecord
    from lib.config import SoularrConfig
    from lib.context import SoularrContext


logger = logging.getLogger("soularr")


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
    release_cfg: SoularrConfig,
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


def _get_denied_users(album_id: int, ctx: SoularrContext) -> set[str]:
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


def get_album_tracks(album: Any, ctx: SoularrContext) -> list[TrackRecord]:
    """Get tracks for an album from the pipeline DB source."""
    return cast("list[TrackRecord]", ctx.pipeline_db_source.get_tracks(album))


def try_enqueue(
    all_tracks: Sequence[TrackRecord],
    results: dict[str, dict[str, list[str]]],
    allowed_filetype: str,
    ctx: SoularrContext,
) -> tuple[bool, list[Any] | None]:
    """Single album match and enqueue."""
    album_id = all_tracks[0]["albumId"]
    album = get_album_by_id(album_id, ctx)
    album_name = album.title
    artist_name = album.artist_name
    denied_users = _get_denied_users(album_id, ctx)
    sorted_users = sorted(
        results.keys(),
        key=lambda u: ctx.user_upload_speed.get(u, 0),
        reverse=True,
    )
    for username in sorted_users:
        if username in denied_users:
            logger.info(
                f"Skipping user '{username}' for album ID {album_id}: denylisted "
                f"(previously provided mislabeled quality)"
            )
            continue
        file_dirs = _get_user_dirs(results[username], allowed_filetype)
        if file_dirs is None:
            continue
        logger.debug(f"Parsing result from user: {username}")
        found, directory, file_dir = check_for_match(
            all_tracks, allowed_filetype, file_dirs, username, ctx
        )
        if found:
            directory = download_filter(allowed_filetype, directory, ctx.cfg)
            files_to_enqueue = _prefixed_directory_files(directory, file_dir)
            try:
                downloads = slskd_do_enqueue(
                    username=username,
                    files=files_to_enqueue,
                    file_dir=file_dir,
                    ctx=ctx,
                )
                if downloads is not None:
                    return True, downloads
                logger.info(
                    f"Failed to enqueue download to slskd for "
                    f"{artist_name} - {album_name} from {username}"
                )
            except Exception as e:
                logger.warning(f"Exception enqueueing tracks: {e}")
                logger.info(
                    f"Exception enqueueing download to slskd for "
                    f"{artist_name} - {album_name} from {username}"
                )
    logger.info(f"Failed to enqueue {artist_name} - {album_name}")
    return False, None


def try_multi_enqueue(
    release: Any,
    all_tracks: Sequence[TrackRecord],
    results: dict[str, dict[str, list[str]]],
    allowed_filetype: str,
    ctx: SoularrContext,
) -> tuple[bool, list[Any] | None]:
    """Locate and enqueue a multi-disc album."""
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
    denied_users = _get_denied_users(album_id, ctx)
    for disk in split_release:
        ctx.negative_matches.clear()
        for username in results:
            if username in denied_users:
                logger.info(
                    f"Skipping user '{username}' for album ID {album_id} "
                    f"(multi-disc): denylisted (previously provided mislabeled quality)"
                )
                continue
            file_dirs = _get_user_dirs(results[username], allowed_filetype)
            if file_dirs is None:
                continue
            found, directory, file_dir = check_for_match(
                disk["tracks"], allowed_filetype, file_dirs, username, ctx
            )
            if found:
                directory = download_filter(allowed_filetype, directory, ctx.cfg)
                disk["source"] = (username, directory, file_dir)
                count_found += 1
                break
        else:
            return False, None
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
                        return False, None
            except Exception:
                logger.exception("Exception enqueueing tracks")
                logger.info(
                    f"Exception enqueueing download to slskd for "
                    f"{artist_name} - {album_name} from {username}"
                )
                if len(all_downloads) > 0:
                    cancel_and_delete(all_downloads, ctx)
                    return False, None
        if enqueued == total:
            return True, all_downloads
        if len(all_downloads) > 0:
            cancel_and_delete(all_downloads, ctx)
        return False, None

    return False, None


def _try_filetype(
    album: Any,
    results: dict[str, dict[str, list[str]]],
    allowed_filetype: str,
    grab_list: dict[int, GrabListEntry],
    ctx: SoularrContext,
) -> bool:
    """Try to match and enqueue an album at a specific filetype quality."""
    album_id = album.id
    artist_name = album.artist_name
    releases = list(album.releases)
    has_monitored = any(r.monitored for r in releases)

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

        found, downloads = try_enqueue(all_tracks, results, allowed_filetype, ctx)
        if not found and len(release.media) > 1:
            found, downloads = try_multi_enqueue(
                release, all_tracks, results, allowed_filetype, ctx
            )

        if found:
            assert downloads is not None
            grab_list[album_id] = GrabListEntry(
                album_id=album_id,
                files=downloads,
                filetype=allowed_filetype,
                title=album.title,
                artist=artist_name,
                year=album.release_date[0:4],
                mb_release_id=release.foreign_release_id,
                db_request_id=album.db_request_id,
                db_source=album.db_source,
                db_quality_override=album.db_quality_override,
            )
            return True

        if has_monitored and release.monitored:
            logger.info(
                f"Monitored release ({release.track_count} tracks) not found on "
                f"Soulseek for {artist_name} - {album.title} at quality "
                f"{allowed_filetype}, skipping non-monitored releases"
            )
            break
        if has_monitored and not release.monitored:
            break

    return False


def find_download(
    album: Any,
    grab_list: dict[int, GrabListEntry],
    ctx: SoularrContext,
) -> bool:
    """Walk search results and enqueue the best matching download."""
    album_id = album.id
    artist_name = album.artist_name
    results = ctx.search_cache[album_id]

    ctx.negative_matches.clear()
    ctx.current_album_cache[album_id] = album

    from lib.quality import search_tiers

    filetypes_to_try, catch_all = search_tiers(
        album.db_quality_override, list(ctx.cfg.allowed_filetypes))

    if album.db_quality_override:
        logger.info(
            f"Quality override for {artist_name} - {album.title}: "
            f"searching {filetypes_to_try}"
        )

    for allowed_filetype in filetypes_to_try:
        logger.info(f"Checking for Quality: {allowed_filetype}")
        if _try_filetype(album, results, allowed_filetype, grab_list, ctx):
            return True

    if (
        catch_all
        and "*" not in [ft.strip() for ft in (ctx.cfg.allowed_filetypes or ())]
    ):
        logger.info(
            f"No match at preferred quality for {artist_name} - {album.title}, "
            f"trying catch-all (any audio format)"
        )
        if _try_filetype(album, results, "*", grab_list, ctx):
            return True

    return False
