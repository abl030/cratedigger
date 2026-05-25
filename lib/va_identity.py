"""Canonical Various Artists identity constants.

Single declaration site for the upstream-mirror IDs that identify
"Various Artists" releases / artists across MusicBrainz and Discogs.
Both ``web/mb.py`` / ``web/discogs.py`` and ``lib/field_resolver_service.py``
read from here so the values can never drift apart between the
ingestion path and the resolver/generator paths.

Pre-consolidation the same constants existed in three places
(``lib/field_resolver_service.py`` redeclared what ``web/mb.py`` and
``web/discogs.py`` already had). ce-code-review flagged the duplication
on PR #370.

The values themselves are upstream-mirror facts:

* MB's canonical "Various Artists" artist MBID is fixed at
  ``89ad4ac3-39f7-470e-963a-56509c546377`` (visible on
  https://musicbrainz.org/artist/89ad4ac3-...).
* Discogs's CC0 dump uses ``194`` as the foreign key in
  ``release_artist`` for VA-credited releases; the artist row itself
  is intentionally absent from the dump.

If either upstream ever changes these (extremely unlikely), update
this file and re-run the U3 backfill to re-detect VA across the
existing wanted cohort.
"""

from __future__ import annotations

MB_VA_ARTIST_MBID: str = "89ad4ac3-39f7-470e-963a-56509c546377"

DISCOGS_VA_ARTIST_ID: str = "194"
