"""Soularr configuration dataclass.

Replaces the 50+ module-level globals in soularr.py with a single
frozen dataclass. Constructed once from config.ini via from_ini().
"""

import configparser
import os
from dataclasses import dataclass, field
from typing import Optional


@dataclass(frozen=True)
class SoularrConfig:
    """All configuration values, read-only after initialization."""

    # --- Slskd ---
    slskd_api_key: str = ""
    slskd_host_url: str = "http://localhost:5030"
    slskd_url_base: str = "/"
    slskd_download_dir: str = ""
    stalled_timeout: int = 3600
    remote_queue_timeout: int = 300
    delete_searches: bool = True

    # --- Search ---
    ignored_users: tuple[str, ...] = ()
    minimum_match_ratio: float = 0.5
    page_size: int = 10
    search_blacklist: tuple[str, ...] = ()
    album_prepend_artist: bool = False
    track_prepend_artist: bool = False
    search_timeout: int = 5000
    maximum_peer_queue: int = 50
    minimum_peer_upload_speed: int = 0
    search_for_tracks: bool = False
    parallel_searches: int = 8
    title_blacklist: tuple[str, ...] = ()

    # --- Release ---
    use_most_common_tracknum: bool = True
    allow_multi_disc: bool = True
    accepted_countries: tuple[str, ...] = (
        "Europe", "Japan", "United Kingdom", "United States",
        "[Worldwide]", "Australia", "Canada",
    )
    skip_region_check: bool = False
    accepted_formats: tuple[str, ...] = ("CD", "Digital Media", "Vinyl")

    # --- Download ---
    download_filtering: bool = False
    use_extension_whitelist: bool = False
    extensions_whitelist: tuple[str, ...] = ("txt", "nfo", "jpg")
    allowed_filetypes: tuple[str, ...] = ("flac", "mp3")

    # --- Beets ---
    beets_validation_enabled: bool = False
    beets_harness_path: str = ""
    beets_distance_threshold: float = 0.15
    beets_staging_dir: str = ""
    audio_check_mode: str = "normal"
    beets_tracking_file: str = ""

    # --- Pipeline DB ---
    pipeline_db_enabled: bool = False
    pipeline_db_dsn: str = "postgresql://soularr@localhost/soularr"

    # --- Meelo ---
    meelo_url: Optional[str] = None
    meelo_username: Optional[str] = None
    meelo_password: Optional[str] = None

    # --- Plex ---
    plex_url: Optional[str] = None
    plex_token: Optional[str] = None
    plex_library_section_id: Optional[str] = None

    # --- Paths (derived from args) ---
    lock_file_path: str = ""
    config_file_path: str = ""

    @classmethod
    def from_ini(cls, config: configparser.ConfigParser,
                 config_dir: str = ".", var_dir: str = ".") -> "SoularrConfig":
        """Parse a ConfigParser into a SoularrConfig.

        Reproduces the exact same parsing logic as main() in soularr.py.
        """
        def get(section, key, fallback=""):
            return config.get(section, key, fallback=fallback)

        def getbool(section, key, fallback=False):
            return config.getboolean(section, key, fallback=fallback)

        def getint(section, key, fallback=0):
            return config.getint(section, key, fallback=fallback)

        def getfloat(section, key, fallback=0.0):
            return config.getfloat(section, key, fallback=fallback)

        def split_csv(section, key, fallback=""):
            raw = get(section, key, fallback)
            return tuple(s.strip() for s in raw.split(",") if s.strip())

        # Filetypes parsing
        raw_filetypes = get("Search Settings", "allowed_filetypes", "flac,mp3")
        if "," in raw_filetypes:
            allowed_filetypes = tuple(s.strip() for s in raw_filetypes.split(",") if s.strip())
        else:
            allowed_filetypes = (raw_filetypes.strip(),)

        # Ignored users
        ignored_raw = get("Search Settings", "ignored_users", "")
        ignored_users = tuple(u.strip() for u in ignored_raw.split(",") if u.strip())

        # Blacklists
        search_bl_raw = get("Search Settings", "search_blacklist", "")
        search_blacklist = tuple(w.strip() for w in search_bl_raw.split(",") if w.strip())
        title_bl_raw = get("Search Settings", "title_blacklist", "")
        title_blacklist = tuple(w.strip() for w in title_bl_raw.split(",") if w.strip())

        return cls(
            # Slskd
            slskd_api_key=get("Slskd", "api_key"),
            slskd_host_url=get("Slskd", "host_url", "http://localhost:5030"),
            slskd_url_base=get("Slskd", "url_base", "/"),
            slskd_download_dir=get("Slskd", "download_dir"),
            stalled_timeout=getint("Slskd", "stalled_timeout", 3600),
            remote_queue_timeout=getint("Slskd", "remote_queue_timeout", 300),
            delete_searches=getbool("Slskd", "delete_searches", True),
            # Search
            ignored_users=ignored_users,
            minimum_match_ratio=getfloat("Search Settings", "minimum_filename_match_ratio", 0.5),
            page_size=getint("Search Settings", "number_of_albums_to_grab", 10),
            search_blacklist=search_blacklist,
            album_prepend_artist=getbool("Search Settings", "album_prepend_artist", False),
            track_prepend_artist=getbool("Search Settings", "track_prepend_artist", False),
            search_timeout=getint("Search Settings", "search_timeout", 5000),
            maximum_peer_queue=getint("Search Settings", "maximum_peer_queue", 50),
            minimum_peer_upload_speed=getint("Search Settings", "minimum_peer_upload_speed", 0),
            search_for_tracks=getbool("Search Settings", "search_for_tracks", False),
            parallel_searches=getint("Search Settings", "parallel_searches", 8),
            title_blacklist=title_blacklist,
            # Release
            use_most_common_tracknum=getbool("Release Settings", "use_most_common_tracknum", True),
            allow_multi_disc=getbool("Release Settings", "allow_multi_disc", True),
            accepted_countries=split_csv("Release Settings", "accepted_countries",
                                         "Europe,Japan,United Kingdom,United States,[Worldwide],Australia,Canada"),
            skip_region_check=getbool("Release Settings", "skip_region_check", False),
            accepted_formats=split_csv("Release Settings", "accepted_formats",
                                       "CD,Digital Media,Vinyl"),
            # Download
            download_filtering=getbool("Download Settings", "download_filtering", False),
            use_extension_whitelist=getbool("Download Settings", "use_extension_whitelist", False),
            extensions_whitelist=split_csv("Download Settings", "extensions_whitelist", "txt,nfo,jpg"),
            allowed_filetypes=allowed_filetypes,
            # Beets
            beets_validation_enabled=getbool("Beets Validation", "enabled", False),
            beets_harness_path=get("Beets Validation", "harness_path", ""),
            beets_distance_threshold=getfloat("Beets Validation", "distance_threshold", 0.15),
            beets_staging_dir=get("Beets Validation", "staging_dir", ""),
            audio_check_mode=get("Beets Validation", "audio_check", "normal"),
            beets_tracking_file=get("Beets Validation", "tracking_file", ""),
            # Pipeline DB
            pipeline_db_enabled=getbool("Pipeline DB", "enabled", False),
            pipeline_db_dsn=get("Pipeline DB", "dsn", "postgresql://soularr@localhost/soularr"),
            # Meelo
            meelo_url=get("Meelo", "url") or None,
            meelo_username=get("Meelo", "username") or None,
            meelo_password=get("Meelo", "password") or None,
            # Plex
            plex_url=get("Plex", "url") or None,
            plex_token=get("Plex", "token") or None,
            plex_library_section_id=get("Plex", "library_section_id") or None,
            # Paths
            lock_file_path=os.path.join(var_dir, ".soularr.lock"),
            config_file_path=os.path.join(config_dir, "config.ini"),
        )
