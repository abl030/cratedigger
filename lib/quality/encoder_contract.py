"""LAME encoder-settings → MP3 VBR quality contract (issue #1145 scope A).

``quality_rank`` step 3 treats an ``mp3 vN`` format label as a self-certifying
contract: V0 is V0 whatever the measured average says. Until this module the
only producer of that label was Cratedigger's own lossless → V0 conversion, so
every acquired or installed MP3 arrived as the bare codec string ``"MP3"`` and
had to be ranked from its measured bitrate alone.

That was tolerable while bare MP3 had a generous VBR ladder to fall into. Once
the two MP3 ladders collapse onto the CBR numbers (transparent >= 320) a
genuine V0 averaging ~245 kbps would fall two tiers, so the contract has to
come from somewhere real. LAME writes its own invocation into the LAME tag,
mutagen surfaces it as ``MP3.info.encoder_settings``, and Beets persists the
same string on every item as ``items.encoder_settings``. Both sides of a
comparison can therefore mint the identical contract from the identical fact.

Two rules, both fail-closed:

* **Only an explicit level counts.** ``-V N`` is parsed; the single
  ``--alt-preset standard`` alias is mapped because LAME documents it as
  exactly ``-V 2``. Nothing else is mapped — not ``--preset standard``
  (31 live items, the same encoding but a spelling the operator did not
  enumerate), not ``--preset extreme``, not a CBR ``-b`` setting, and never
  ``bitrate_mode``, bitrate uniformity, or the measured average. Guessing a
  V level is precisely the unreliable-boolean failure this issue removes.
* **An album mints only by unanimity.** Every file must report the same
  explicit level. One unlabelled or disagreeing file withholds the contract
  for the whole album, which falls back to the single measured ladder.
"""

from __future__ import annotations

import re
from collections.abc import Iterable

#: LAME writes the level it was invoked with. ``-V`` is upper case in every
#: live string (``-V 0``, ``-V 2 --vbr-new``, ``-V 0 --vbr-old``); a lower-case
#: ``-v`` is LAME's *verbose* flag and must never be read as a quality level,
#: so this pattern is deliberately case-sensitive. The trailing guard rejects
#: a multi-digit run so a hypothetical ``-V 10`` is unparsed rather than
#: silently truncated to V1.
_LAME_VBR_LEVEL_RE = re.compile(r"(?:^|\s)-V\s*([0-9])(?![0-9])")

#: The one preset alias LAME documents as an exact ``-V`` equivalence and the
#: operator authorised mapping. Compared against the whitespace-normalised,
#: lower-cased setting string in full — a substring match would promote
#: ``--alt-preset standard-ish`` nonsense.
LAME_PRESET_VBR_LEVELS: dict[str, int] = {
    "--alt-preset standard": 2,
}

#: Highest V level LAME defines. ``mp3_vbr_levels`` is a 10-tuple indexed by
#: this value, so anything outside the range must not be minted.
_MAX_VBR_LEVEL = 9


def mp3_vbr_contract_level(format_hint: str) -> int | None:
    """Parse the V level out of an ``mp3 vN`` format label, else ``None``.

    The reader half of the vocabulary ``mp3_vbr_contract_format`` writes, kept
    beside it so one module owns the label's spelling. ``quality_rank`` step 3
    indexes ``cfg.mp3_vbr_levels`` with the result;
    ``AlbumQualityEvidence.storage_validation_errors`` uses it to admit the
    contract as a measured storage label. A declared-bitrate label
    (``"mp3 320"``) is deliberately not a match.
    """
    parts = format_hint.strip().lower().split()
    if len(parts) < 2 or parts[0] != "mp3":
        return None
    quality = parts[1]
    if len(quality) >= 2 and quality[0] == "v" and quality[1:].isdigit():
        level = int(quality[1:])
        if 0 <= level <= _MAX_VBR_LEVEL:
            return level
    return None


def lame_vbr_level(encoder_settings: str | None) -> int | None:
    """Return the explicit LAME VBR level a settings string certifies.

    ``None`` for a missing, blank, unrecognised, or CBR/ABR setting string —
    the caller then has no contract and ranks on the measured bitrate.
    """
    if encoder_settings is None:
        return None
    raw = encoder_settings.strip()
    if not raw:
        return None
    match = _LAME_VBR_LEVEL_RE.search(raw)
    if match is not None:
        level = int(match.group(1))
        return level if 0 <= level <= _MAX_VBR_LEVEL else None
    normalised = " ".join(raw.lower().split())
    return LAME_PRESET_VBR_LEVELS.get(normalised)


def mp3_vbr_contract_format(
    encoder_settings: Iterable[str | None] | None,
) -> str | None:
    """Return the album's ``mp3 vN`` contract, or ``None`` when unproven.

    ``encoder_settings`` is one entry per MP3 file in the album (``None`` for a
    file whose LAME tag is absent or unreadable). ``None`` for the whole
    argument means the caller could not establish an all-MP3 fileset at all.

    A contract requires an unambiguous, unanimous, explicit level across every
    file: an empty fileset, any file without a level, or two different levels
    all withhold it. The returned label is exactly the vocabulary
    ``mp3_vbr_contract_level`` reads back.
    """
    if encoder_settings is None:
        return None
    levels: set[int] = set()
    for settings in encoder_settings:
        level = lame_vbr_level(settings)
        if level is None:
            return None
        levels.add(level)
        if len(levels) > 1:
            return None
    if len(levels) != 1:
        return None
    return f"mp3 v{levels.pop()}"
