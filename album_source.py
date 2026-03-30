"""Album source — Pipeline DB as the source of wanted albums.

Provides the interface soularr.py uses to get wanted albums, fetch tracks,
and report completion. Records are returned in a normalized dict shape that
the search/download/matching code expects.
"""

import json
import logging
import os
import urllib.request
import urllib.error

logger = logging.getLogger("soularr")

MB_API_BASE = "http://192.168.1.35:5200/ws/2"


class AlbumRecord:
    """Normalized album record from a pipeline DB row."""

    @staticmethod
    def from_db_row(row, tracks):
        """Build a normalized album record from a pipeline DB row + tracks.

        The returned dict has the same keys that soularr.py's search_and_queue(),
        find_download(), and choose_release() expect.
        """
        # Build media structure from tracks (grouped by disc)
        discs = {}
        for t in tracks:
            d = t["disc_number"]
            if d not in discs:
                discs[d] = []
            discs[d].append(t)

        media = []
        for disc_num in sorted(discs.keys()):
            disc_tracks = discs[disc_num]
            media.append({
                "mediumNumber": disc_num,
                "mediumFormat": row.get("format") or "Digital Media",
                "trackCount": len(disc_tracks),
            })

        total_tracks = sum(len(dt) for dt in discs.values())
        num_discs = len(discs)

        # Build format string: "CD", "2xCD", "Digital Media"
        base_format = row.get("format") or "Digital Media"
        format_str = f"{num_discs}x{base_format}" if num_discs > 1 else base_format

        # Build a single release entry (one per DB request — the pinned edition)
        release = {
            "id": row["id"] * -1,  # Negative ID space for DB records
            "foreignReleaseId": row["mb_release_id"] or "",
            "title": row["album_title"],
            "trackCount": total_tracks,
            "mediumCount": num_discs,
            "format": format_str,
            "media": media,
            "monitored": True,  # Always monitored — it's explicitly wanted
            "country": [row.get("country") or "US"],
            "status": "Official",
        }

        # Build the normalized album record
        return {
            "id": row["id"] * -1,  # Negative ID space for DB records
            "title": row["album_title"],
            "releaseDate": f"{row.get('year') or '0000'}-01-01T00:00:00Z",
            "artistId": 0,  # Not used for search
            "artist": {
                "artistName": row["artist_name"],
                "foreignArtistId": row.get("mb_artist_id") or "",
            },
            "releases": [release],
            # Pipeline DB metadata
            "_db_request_id": row["id"],
            "_db_source": row["source"],
            "_db_mb_release_id": row["mb_release_id"],
            "_db_quality_override": row.get("quality_override"),
        }


class DatabaseSource:
    """Fetch wanted albums from pipeline.db."""

    def __init__(self, dsn):
        self.dsn = dsn
        self._db = None

    def _get_db(self):
        if self._db is None:
            import sys
            lib_dir = os.path.join(os.path.dirname(__file__), "lib")
            if lib_dir not in sys.path:
                sys.path.insert(0, lib_dir)
            from pipeline_db import PipelineDB
            self._db = PipelineDB(self.dsn)
        return self._db

    def get_wanted(self, limit=None):
        """Get wanted albums as normalized records."""
        db = self._get_db()
        wanted = db.get_wanted(limit=limit)
        records = []
        for row in wanted:
            tracks = db.get_tracks(row["id"])
            if not tracks:
                # Try to populate tracks from MB API
                tracks = self._populate_tracks(row)
            record = AlbumRecord.from_db_row(row, tracks)
            records.append(record)
        return records

    def get_tracks(self, album_record):
        """Get tracks for an album in normalized track format.

        Returns list of dicts with keys: title, trackNumber, mediumNumber, duration.
        """
        request_id = album_record.get("_db_request_id")
        if not request_id:
            return []

        db = self._get_db()
        tracks = db.get_tracks(request_id)
        album_id = request_id * -1  # Negative ID space
        return [
            {
                "title": t["title"],
                "trackNumber": str(t["track_number"]),
                "mediumNumber": t["disc_number"],
                "duration": int((t.get("length_seconds") or 0) * 10000000),  # ticks (100ns units)
                "id": 0,
                "albumId": album_id,
            }
            for t in tracks
        ]

    def update_status(self, album_record, status, **extra):
        """Update album status in the pipeline DB."""
        request_id = album_record.get("_db_request_id")
        if not request_id:
            return
        db = self._get_db()
        db.update_status(request_id, status, **extra)

    def mark_done(self, album_record, bv_result, dest_path=None,
                  download_info=None):
        """Mark album as imported."""
        from lib.quality import DownloadInfo, is_verified_lossless
        request_id = album_record.get("_db_request_id")
        if not request_id:
            return

        db = self._get_db()
        distance = bv_result.get("distance")
        dl = download_info if isinstance(download_info, DownloadInfo) else DownloadInfo()

        update_fields = dict(
            beets_distance=distance,
            beets_scenario=bv_result.get("scenario"),
            imported_path=dest_path,
        )
        if dl.spectral_bitrate is not None:
            update_fields["spectral_bitrate"] = dl.spectral_bitrate
        if dl.spectral_grade:
            update_fields["spectral_grade"] = dl.spectral_grade
        if is_verified_lossless(dl.was_converted, dl.original_filetype,
                                dl.spectral_grade):
            update_fields["verified_lossless"] = True
        db.update_status(request_id, "imported", **update_fields)

        db.log_download(
            request_id=request_id,
            soulseek_username=dl.username,
            filetype=dl.filetype,
            beets_distance=distance,
            beets_scenario=bv_result.get("scenario"),
            beets_detail=bv_result.get("detail"),
            outcome="success",
            staged_path=dest_path,
            bitrate=dl.bitrate,
            sample_rate=dl.sample_rate,
            bit_depth=dl.bit_depth,
            is_vbr=dl.is_vbr,
            was_converted=dl.was_converted,
            original_filetype=dl.original_filetype,
            slskd_filetype=dl.slskd_filetype,
            slskd_bitrate=dl.slskd_bitrate,
            actual_filetype=dl.actual_filetype,
            actual_min_bitrate=dl.actual_min_bitrate,
            spectral_grade=dl.spectral_grade,
            spectral_bitrate=dl.spectral_bitrate,
            existing_min_bitrate=dl.existing_min_bitrate,
            existing_spectral_bitrate=dl.existing_spectral_bitrate,
            import_result=dl.import_result,
            validation_result=dl.validation_result,
        )

    def mark_failed(self, album_record, bv_result, usernames=None,
                    download_info=None):
        """Log the failure and denylist users, but keep album wanted for retry."""
        from lib.quality import DownloadInfo
        request_id = album_record.get("_db_request_id")
        if not request_id:
            return

        db = self._get_db()
        dl = download_info if isinstance(download_info, DownloadInfo) else DownloadInfo()
        db.update_status(request_id, "wanted",
                         beets_distance=bv_result.get("distance"),
                         beets_scenario=bv_result.get("scenario"))
        db.record_attempt(request_id, "validation")

        db.log_download(
            request_id=request_id,
            soulseek_username=dl.username,
            filetype=dl.filetype,
            beets_distance=bv_result.get("distance"),
            beets_scenario=bv_result.get("scenario"),
            beets_detail=bv_result.get("detail"),
            outcome="rejected",
            error_message=bv_result.get("error"),
            bitrate=dl.bitrate,
            sample_rate=dl.sample_rate,
            bit_depth=dl.bit_depth,
            is_vbr=dl.is_vbr,
            was_converted=dl.was_converted,
            original_filetype=dl.original_filetype,
            slskd_filetype=dl.slskd_filetype,
            slskd_bitrate=dl.slskd_bitrate,
            actual_filetype=dl.actual_filetype,
            actual_min_bitrate=dl.actual_min_bitrate,
            spectral_grade=dl.spectral_grade,
            spectral_bitrate=dl.spectral_bitrate,
            existing_min_bitrate=dl.existing_min_bitrate,
            existing_spectral_bitrate=dl.existing_spectral_bitrate,
            import_result=dl.import_result,
            validation_result=dl.validation_result,
        )

        # Denylist source users
        if usernames:
            for username in usernames:
                db.add_denylist(request_id, username, "beets validation rejected")

    def get_denylisted_users(self, album_record):
        """Get denylisted usernames for an album."""
        request_id = album_record.get("_db_request_id")
        if not request_id:
            return set()
        db = self._get_db()
        entries = db.get_denylisted_users(request_id)
        return {e["username"] for e in entries}

    def _populate_tracks(self, row):
        """Fetch tracks from MB API and store in DB."""
        mb_id = row.get("mb_release_id")
        if not mb_id:
            return []

        try:
            url = f"{MB_API_BASE}/release/{mb_id}?inc=recordings&fmt=json"
            req = urllib.request.Request(url)
            req.add_header("User-Agent", "soularr-db/1.0")
            with urllib.request.urlopen(req, timeout=15) as resp:
                data = json.loads(resp.read())
        except Exception:
            logger.warning(f"Failed to fetch tracks from MB API for {mb_id}")
            return []

        tracks = []
        for medium in data.get("media", []):
            disc = medium.get("position", 1)
            for track in medium.get("tracks", []):
                length_ms = track.get("length") or (track.get("recording") or {}).get("length")
                tracks.append({
                    "disc_number": disc,
                    "track_number": track.get("position", track.get("number", 0)),
                    "title": track.get("title", ""),
                    "length_seconds": round(length_ms / 1000, 1) if length_ms else None,
                })

        if tracks:
            db = self._get_db()
            db.set_tracks(row["id"], tracks)

        return tracks

    def close(self):
        if self._db:
            self._db.close()
            self._db = None


