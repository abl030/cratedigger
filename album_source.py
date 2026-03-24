"""Album source abstraction — Lidarr or Pipeline DB as source of wanted albums.

Provides a unified interface so soularr.py can get wanted albums, fetch tracks,
and report completion regardless of whether the source is Lidarr or the pipeline DB.

Both sources return records in the same shape as Lidarr's API, so the existing
search/download/matching code works without modification.
"""

import json
import logging
import os
import sqlite3
import urllib.request
import urllib.error

logger = logging.getLogger("soularr")

MB_API_BASE = "http://192.168.1.35:5200/ws/2"


class AlbumRecord:
    """Normalized album record — wraps both Lidarr and DB sources into Lidarr-shaped dicts."""

    @staticmethod
    def from_db_row(row, tracks):
        """Build a Lidarr-shaped album record from a pipeline DB row + tracks.

        The returned dict has the same keys that soularr.py's search_and_queue(),
        find_download(), and choose_release() expect from Lidarr API records.
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

        # Build format string like Lidarr: "CD", "2xCD", "Digital Media"
        base_format = row.get("format") or "Digital Media"
        lidarr_format = f"{num_discs}x{base_format}" if num_discs > 1 else base_format

        # Build a single release entry (in Lidarr, albums have multiple releases;
        # in DB mode, we have exactly one — the pinned edition)
        release = {
            "id": row["id"] * -1,  # Negative to avoid collision with real Lidarr IDs
            "foreignReleaseId": row["mb_release_id"] or "",
            "title": row["album_title"],
            "trackCount": total_tracks,
            "mediumCount": num_discs,
            "format": lidarr_format,
            "media": media,
            "monitored": True,  # Always monitored — it's explicitly wanted
            "country": [row.get("country") or "US"],
            "status": "Official",
        }

        # Build the Lidarr-shaped album record
        return {
            "id": row["id"] * -1,  # Negative ID space for DB records
            "title": row["album_title"],
            "releaseDate": f"{row.get('year') or '0000'}-01-01T00:00:00Z",
            "artistId": 0,  # Not used for search, only for Lidarr API calls
            "artist": {
                "artistName": row["artist_name"],
                "foreignArtistId": row.get("mb_artist_id") or "",
            },
            "releases": [release],
            # Pipeline DB metadata (not in Lidarr records)
            "_db_request_id": row["id"],
            "_db_source": row["source"],
            "_db_mb_release_id": row["mb_release_id"],
        }


class DatabaseSource:
    """Fetch wanted albums from pipeline.db."""

    def __init__(self, db_path):
        self.db_path = db_path
        self._db = None

    def _get_db(self):
        if self._db is None:
            # Import pipeline_db from the lib/ directory in the same repo.
            # At runtime this is the Nix store path (e.g., /nix/store/.../lib/).
            import sys
            lib_dir = os.path.join(os.path.dirname(__file__), "lib")
            if lib_dir not in sys.path:
                sys.path.insert(0, lib_dir)
            from pipeline_db import PipelineDB
            self._db = PipelineDB(self.db_path)
        return self._db

    def get_wanted(self, limit=None):
        """Get wanted albums as Lidarr-shaped records."""
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
        """Get tracks for an album in Lidarr track format.

        Returns list of dicts with keys: title, trackNumber, mediumNumber, duration
        (matching what lidarr.get_tracks() returns).
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
                "duration": int((t.get("length_seconds") or 0) * 10000000),  # Lidarr uses ticks
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

    def mark_done(self, album_record, bv_result, dest_path=None):
        """Mark album as completed (staged or imported based on source type)."""
        request_id = album_record.get("_db_request_id")
        source = album_record.get("_db_source", "request")
        if not request_id:
            return

        db = self._get_db()
        distance = bv_result.get("distance")

        if source == "redownload":
            # Always stage for manual review
            db.update_status(request_id, "staged",
                             beets_distance=distance,
                             beets_scenario=bv_result.get("scenario"))
        else:
            # Request: mark as staged (auto-import happens in process_completed_album)
            db.update_status(request_id, "staged",
                             beets_distance=distance,
                             beets_scenario=bv_result.get("scenario"))

        # Log the download
        db.log_download(
            request_id=request_id,
            beets_distance=distance,
            beets_scenario=bv_result.get("scenario"),
            beets_detail=bv_result.get("detail"),
            outcome="staged",
            staged_path=dest_path,
        )

    def mark_failed(self, album_record, bv_result, usernames=None):
        """Log the failure and denylist users, but keep album wanted for retry."""
        request_id = album_record.get("_db_request_id")
        if not request_id:
            return

        db = self._get_db()
        db.update_status(request_id, "wanted",
                         beets_distance=bv_result.get("distance"),
                         beets_scenario=bv_result.get("scenario"))
        db.record_attempt(request_id, "validation")

        # Log the download attempt
        db.log_download(
            request_id=request_id,
            beets_distance=bv_result.get("distance"),
            beets_scenario=bv_result.get("scenario"),
            beets_detail=bv_result.get("detail"),
            outcome="rejected",
            error_message=bv_result.get("error"),
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


class LidarrSource:
    """Wraps existing Lidarr-based record fetching (preserves current behavior)."""

    def __init__(self, lidarr_client, get_records_fn):
        """
        Args:
            lidarr_client: The initialized LidarrAPI instance
            get_records_fn: The existing get_records() function from soularr.py
        """
        self.lidarr = lidarr_client
        self._get_records = get_records_fn

    def get_wanted(self, limit=None):
        """Get wanted records from Lidarr (delegates to existing get_records)."""
        # In Lidarr mode, get_records() is called per search_source (missing/cutoff)
        # The caller in main() handles that iteration — this is just a pass-through
        raise NotImplementedError(
            "LidarrSource.get_wanted() should not be called directly. "
            "Use the existing get_records() flow in main()."
        )

    def get_tracks(self, album_record):
        """Get tracks from Lidarr API."""
        return self.lidarr.get_tracks(
            artistId=album_record["artistId"],
            albumId=album_record["id"],
            albumReleaseId=album_record["releases"][0]["id"],
        )

    def update_status(self, album_record, status, **extra):
        """No-op for Lidarr mode — status is managed by Lidarr itself."""
        pass

    def mark_done(self, album_record, bv_result, dest_path=None):
        """No-op for Lidarr mode — handled by existing unmonitor_album()."""
        pass

    def mark_failed(self, album_record, bv_result, usernames=None):
        """No-op for Lidarr mode — handled by existing cutoff_denylist."""
        pass

    def get_denylisted_users(self, album_record):
        """No-op for Lidarr mode — handled by existing cutoff_denylist."""
        return set()

    def close(self):
        pass
