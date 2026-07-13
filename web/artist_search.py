"""Shared exact-artist search enrichment.

MusicBrainz and Discogs expose related identities differently, but both
adapters normalize them to the same artist-hit shape before calling this
pure merge. Release catalogs remain separate; this only makes canonical
alternate identities discoverable beside an exact search hit.
"""

from __future__ import annotations


def merge_exact_artist_identities(
    base: list[dict],
    *,
    exact_id: str,
    related: list[dict],
    limit: int = 20,
) -> list[dict]:
    """Keep the exact hit first, then related identities, then other hits."""
    exact = next(
        (row for row in base if str(row.get("id", "")) == exact_id),
        None,
    )
    if exact is None:
        return base[:limit]

    ordered = [exact, *related, *base]
    merged: list[dict] = []
    seen: set[str] = set()
    for row in ordered:
        row_id = str(row.get("id", ""))
        if not row_id or row_id in seen:
            continue
        seen.add(row_id)
        merged.append(row)
        if len(merged) >= limit:
            break
    return merged
