# TODO

## Discogs-sourced albums can't use the upgrade pipeline

Albums imported via the Discogs beets plugin have numeric Discogs IDs (e.g. `2048516`) in `mb_albumid` instead of MusicBrainz UUIDs. The entire upgrade pipeline assumes MB UUIDs — the web UI upgrade button, `import_one.py` harness matching, and the quality gate all pass the MBID to MusicBrainz APIs or beets `--search-id`.

To fix this properly:
- Detect Discogs IDs (numeric vs UUID format) in the pipeline
- Fetch metadata from Discogs API instead of MusicBrainz
- Import via beets Discogs plugin (`-s discogs:ID`) instead of MB matching
- Disable the upgrade button in the UI for Discogs albums until this is implemented (currently shows an error toast)

Affected album: The Mountain Goats - Ghana (`mb_albumid = 2048516`)
