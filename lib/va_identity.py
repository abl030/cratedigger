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

import re

MB_VA_ARTIST_MBID: str = "89ad4ac3-39f7-470e-963a-56509c546377"

DISCOGS_VA_ARTIST_ID: str = "194"

_VA_PHRASE_RE = re.compile(r"\bvarious\s+artists\b", re.IGNORECASE)
_EMPTY_BRACKETS_RE = re.compile(r"\(\s*\)|\[\s*\]")

# Lucene boolean operators (uppercase-only in Lucene syntax) left
# dangling at the remainder's edges after the phrase strip — e.g.
# "best of AND Various Artists" → "best of AND". The MB builder wraps
# the remainder in `arid:… AND (…)`, so an edge operator the raw
# passthrough tolerated would now sit inside a group. Interior
# operators stay: they were valid before the strip and still are.
_DANGLING_OPERATORS = frozenset({"AND", "OR", "NOT", "&&", "||"})


def split_va_query(query: str) -> tuple[str, bool]:
    """Split free-text search input into (remainder, va_detected).

    Strips every "Various Artists" phrase from the query so the browse
    search builders can route the VA intent to an artist-id pin instead
    of letting the tokens AND into the title match (#199 — on both
    mirrors the tokens can never match a title, so pre-fix VA queries
    returned zero or junk results).

    A bare "various" only counts when it is the entire query: "Various
    Positions" and "Various Blends" are real title/artist strings that
    must pass through untouched. Bracket pairs emptied by the strip
    ("Rock Christmas (Various Artists)") are removed so the remainder
    stays a clean title query.
    """
    stripped, n_hits = _VA_PHRASE_RE.subn(" ", query)
    if n_hits == 0:
        if query.strip().lower() == "various":
            return "", True
        return query, False
    # Fix-point: removing an inner empty pair can empty its outer pair
    # ("((Various Artists))" → "(( ))" → "( )" → "").
    while True:
        stripped, n_brackets = _EMPTY_BRACKETS_RE.subn(" ", stripped)
        if n_brackets == 0:
            break
    tokens = stripped.split()
    while tokens and tokens[0] in _DANGLING_OPERATORS:
        tokens.pop(0)
    while tokens and tokens[-1] in _DANGLING_OPERATORS:
        tokens.pop()
    return " ".join(tokens), True
