# Perceptual hashing is not a usable cover-art correctness signal

**Measured 2026-08-19 during the cratedigger#1200 pressing-exact art audit.
Recorded so nobody rebuilds the same automated check.**

## The idea that does not work

Auditing whether an album carries the *right* cover art looks like a perceptual
hashing problem: hash the installed `cover.jpg`, hash the image the release's
own source advertises, and flag the pairs whose Hamming distance exceeds some
threshold. It is cheap, it is fully automatic, and it does not work here.

## What the numbers said

A dhash comparison over the affected cohort flagged **116 mismatches. Only 40
were genuinely wrong art — a ~64% false-positive rate.** The distances do not
separate:

| Case | Hamming distance |
| --- | --- |
| Correct art | 14 |
| Correct art | 49 |
| Wrong art | 34 |
| Wrong art | 46 |

A correct album scored 49 while a wrong one scored 34. No threshold splits
those four points, and no amount of tuning creates a boundary that does not
exist. Visual triage was required for the whole flagged set.

## Why the signal is absent

Discogs primary images are frequently **amateur photographs of physical
sleeves**, not clean digital scans. The same artwork is photographed with
different borders, shrink wrap, hype stickers, price tags, lighting and colour
casts, and at different crops and angles. A perceptual hash faithfully reports
that two such images are far apart — it is measuring the photography, not the
artwork. The signal it returns is real; it is just not the question being
asked.

This is a property of the source corpus, so it is not fixed by a different
hash. pHash, aHash, wavelet hashing, and a larger hash size all still measure
the photograph. An embedding model that is explicitly invariant to sleeve
photography conditions would be a different proposition, but that is a research
project, not an audit check.

## What to do instead

- **For "does this album have art at all?"** — query the beets `artpath` field.
  That is exact and cheap, and it is the check that actually found the seven
  art-less albums in cratedigger#1203 item 3.
- **For "is this the right art?"** — visual triage of a bounded cohort, or
  trust the identity of the source. Cratedigger's real defence is that
  `cover_art_url` keys on the exact Discogs release id and is ranked above
  title-matching sources, so correct-by-construction replaced the audit
  (cratedigger#1200). Prefer making the wrong art unreachable over detecting it
  afterwards.
- **If a bulk art check is ever needed again**, size the manual triage into the
  plan from the start. The automated pass narrows nothing useful: at a 64%
  false-positive rate a human still looks at every flagged pair.

## Related

- `docs/solutions/runtime-errors/plex-asciify-paths-album-split.md` — the other
  art/path incident where an automated-looking signal misled.
- `docs/beets-primer.md` § "One-shot config overlays" — how the item-3
  remediation actually ran.
