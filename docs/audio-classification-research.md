# Toward a Trustworthy Audio Classification System

This document captures the April 12-13, 2026 research pass on Cratedigger's
audio classification system: what the current classifier does, what the
corpus work found, where the current rules are weak, and what existing prior
art already exists outside this repo.

Tracking issue: `#83` ("Build a trustworthy audio classification system").

The goal is not "make spectral rejects go down." The goal is to build an
audio classification system we can trust enough to make automated import
decisions without quietly filling the library with garbage or repeatedly
rejecting legitimate edge-case music.

## Scope

This write-up is only about the audio classification component:

- detecting bad MP3 / fake-lossless / lossy-origin audio
- deciding when a download should be auto-rejected, auto-imported, or staged
- evaluating whether Cratedigger's current detector is too trigger-happy

It is not about the wider Cratedigger pipeline, web UI, or beets integration
except where those systems affect classifier inputs or policy decisions.

## Current Cratedigger Classifier

The current detector lives primarily in:

- `lib/spectral_check.py`
- `lib/quality.py`
- `lib/download.py`
- `scripts/spectral_corpus.py`

### Track-level method

The core detector in `lib/spectral_check.py` is a rule-based spectral check:

1. Use `sox` to measure RMS energy in one reference band (`1-4 kHz`).
2. Measure 16 narrow `500 Hz` slices from `12 kHz` to `20 kHz`.
3. Detect a "cliff" when adjacent slices fall steeply enough.
4. Measure "HF deficit" by comparing the top four slices (`18-20 kHz`) to the
   reference band.
5. Classify the track with fixed thresholds.

As of this research pass, the important thresholds are:

- `HF_DEFICIT_SUSPECT = 60 dB`
- `HF_DEFICIT_MARGINAL = 40 dB`
- `CLIFF_THRESHOLD_DB_PER_KHZ = -12 dB/kHz`
- `MIN_CLIFF_SLICES = 2`
- album suspect threshold = `60%`
- album likely-transcode threshold = `75%`

Current behavior in plain English:

- any detected cliff marks a track `suspect`
- no cliff but very large HF deficit also marks a track `suspect`
- enough suspect tracks pushes the album to `suspect` or `likely_transcode`
- a cliff-derived bitrate estimate is used as an "effective source quality"
  estimate

### Policy layer

The spectral classifier is not the whole decision system. The policy layer in
`lib/quality.py` decides what to do with those results.

For MP3 imports, Stage 1 is a pre-import spectral gate:

- if spectral says `genuine` or `marginal`, import proceeds
- if spectral says `suspect` / `likely_transcode`, the new spectral estimate is
  compared against what is already on disk
- if the new download looks no better than what already exists, it can be
  hard-rejected as `spectral_reject`

This is important because Cratedigger is not currently trying to solve
"classification in the abstract." It is solving "should this download replace
what I already have?"

## Why This Research Started

The immediate question was:

> We have a lot of spectral rejects, especially high nominal bitrate ones. Are
> we creating too many false positives, or is Soulseek really full of bad
> files?

That question split naturally into three parts:

1. Are the reject counts inflated by repeat attempts?
2. Are the high-bitrate rejects obviously junk when inspected directly?
3. Is the detector internally consistent, and where does it fail?

## Initial Findings: Reject Volume vs. Distinct Bad Sources

The raw reject count was real, but it exaggerated how many distinct cases we
were seeing.

During the live corpus pass on April 13, 2026:

- `485` rows existed with `scenario='spectral_reject'`
- those collapsed to `130` distinct request IDs
- `355` of those rows were high-bitrate rejects (`>= 224 kbps`)
- those high-bitrate rows represented `102` distinct requests

High-bitrate bucket summary from `/tmp/spectral-corpus-eval.json`:

| Bucket | Rows | Requests |
|--------|------|----------|
| `224-255` | 44 | 20 |
| `256-319` | 79 | 31 |
| `320` | 232 | 65 |

Interpretation:

- the pipeline is seeing many nominally high-bitrate rejects
- the "there are tons of rejects" feeling is partly real
- but a lot of the volume is retry spam or repeated bad mirrors of the same
  album

So the first answer was: Soulseek really does contain a lot of junk, and the
database counts also amplify that by retrying bad albums across multiple users.

## Direct Folder Scans: Not Just Hallucinated Rejects

The next question was whether the rejected folders actually looked bad when
inspected directly from disk.

Representative failed-import folders and beets copies were scanned during the
research pass. The broad result was:

- many nominal `320 kbps` folders really do look bad under the current model
- the detector is not simply rejecting every high-bitrate MP3
- there are real edge cases where the evidence is weaker and more ambiguous

Examples from the live pass:

- several failed-import folders with `320 kbps` containers rescanned as
  `suspect` or `likely_transcode`
- an existing beets copy of `Panjabi MC - The Album` rescanned as `genuine`
  despite the failed download folder looking bad

That comparison mattered because it showed the model is capable of separating
"bad 320" from "good 320" in at least some real cases.

## First Problem: Rescanning With the Same Analyzer Is Not Ground Truth

The first version of the corpus script only rescanned folders with the same
production analyzer. That was useful, but it was not an independent validation.

It could support statements like:

- "the same files still look bad when rescanned"
- "the DB state is not obviously stale or corrupted"

It could not support statements like:

- "the classifier is objectively correct"
- "this is definitely not a false positive"

That led to the first important correction in this research pass:

The original corpus script was a consistency check, not a truth test.

## The Evaluation Harness

To move beyond a pure same-model rescan, `scripts/spectral_corpus.py` was
extended into a proper evaluation harness.

The harness now does three kinds of work:

1. Query live `spectral_reject` rows and de-duplicate repeated bad sources.
2. Rescan failed-import folders and on-disk controls with the current model.
3. Generate synthetic "known-good" and "known-bad" transcodes from trusted
   lossless source albums.

The synthetic profiles currently include:

- real `FLAC -> MP3 320`
- `MP3 128 -> MP3 320`
- `MP3 192 -> MP3 320`
- `AAC 256 -> MP3 320`
- `Vorbis q9 -> MP3 320`

The harness also exports raw evidence:

- album grade
- album suspect percentage
- per-track grades
- cliff vs. HF-only suspect counts
- nominal bitrate stats from the files on disk

Helper tests live in `tests/test_spectral_corpus.py`.

## Synthetic Ground-Truth Findings

The synthetic results were the clearest signal in the whole exercise.

From `/tmp/spectral-corpus-eval.json` on April 13, 2026:

| Profile | Expected | Cases | Matches | Result |
|---------|----------|-------|---------|--------|
| `mp3_320` | known good | 2 | 2 | both `genuine` |
| `mp3_128_to_320` | known bad | 2 | 2 | both `likely_transcode` |
| `mp3_192_to_320` | known bad | 2 | 2 | both `likely_transcode` |
| `aac_256_to_320` | known bad | 2 | 0 | both `genuine` |
| `vorbis_q9_to_320` | known bad | 2 | 0 | both `genuine` |

This gave a much sharper picture of what the current classifier is actually
good at.

### What it does well

It catches lowpass-heavy / obviously degraded transcodes reasonably well:

- classic low-bitrate MP3-to-MP3 upconversions
- obvious cliff-bearing transcodes
- many fake high-bitrate files whose spectrum still carries an older MP3-like
  cutoff

### What it does not do well

It does not reliably detect high-quality streaming-style lossy-to-lossy
transcodes:

- AAC 256 -> MP3 320 passed as `genuine`
- Vorbis q9 -> MP3 320 passed as `genuine`

That is a major limitation, and it reframed the project:

The current system is a "strong lowpass / cliff detector," not a general
lossy-origin detector.

## The Colleen Case

`Colleen - Libres antes del final` became the key boundary case in this
research pass.

Why it mattered:

- it is modern music, not an obvious 1990s cassette edge case
- it was rejected despite a very high nominal bitrate
- it looked plausible enough that it could easily have been a false positive
- but it could also still be a streaming rip

The saved failed-import scan reported:

- nominal bitrate range: `339-443 kbps`
- album grade: `suspect`
- estimated bitrate: `96 kbps`
- suspect rate: `60%`
- track breakdown:
  - `3` suspect
  - `2` marginal
  - only `1` suspect track had an actual cliff
  - `2` suspect tracks were HF-deficit-only

This was the first strong sign that the current policy could be letting a
single cliff track dominate the whole album verdict for a high nominal bitrate
release.

## The Narrow Patch and Why It Is Not Settled

As an experiment, the Stage 1 MP3 spectral gate in `lib/quality.py` was
patched so that nominal `256/320` MP3 downloads would not hard-reject on the
strength of only one cliff track.

The new logic uses `cliff_track_count` passed down from `lib/download.py`:

- high nominal MP3 (`>= 256 kbps`)
- only one cliff track
- do not trust the cliff-derived bitrate enough to hard-reject

This was intentionally narrow. It did not rewrite the classifier; it only
softened one policy decision.

### What happened after the patch

The spot-check result was:

- obvious junk cases with many cliffs still rejected
- the Colleen-style single-cliff case flipped

That made the patch useful as an experiment, but it also exposed a risk:

This may have been a genuine improvement, or it may have been a hand-tuned
heuristic picked to save one uncomfortable edge case.

The correct interpretation is:

- the patch is evidence
- it is not proof
- it should not be mistaken for a validated final rule

That is the point where the research turned away from one-off threshold tuning
and toward system-level trust.

## Current State of Confidence

After this research pass, the honest summary of the current classifier is:

### Things we can say with reasonable confidence

- Soulseek really is full of bad files, including nominal high-bitrate ones.
- The current detector catches a lot of obvious lowpass-heavy junk.
- The reject volume is inflated by repeated bad sources.
- The detector is not blindly rejecting every 320 kbps MP3.

### Things we cannot honestly claim yet

- that the current detector is "correct" in a ground-truth sense
- that HF-deficit-only evidence is strong enough for hard auto-rejects
- that modern high-quality streaming-origin transcodes are being caught
- that the Colleen patch is anything more than a plausible local fix

## Prior Art: We Are Not Starting From Zero

The GitHub sweep found clear prior work in audio authenticity / lossy-origin
classification.

The important distinction is that most of the wheel already exists, but in
different shapes:

- research-backed detector
- heuristic analyzers
- fake-lossless wrappers
- desktop tools

The part that is less common is plugging this into a Soulseek-style automated
import gate with real downgrade / upgrade policy.

### Most relevant external projects

| Project | Type | Why it matters | License | Integration value |
|---------|------|----------------|---------|-------------------|
| `cannam/vamp-lossy-encoding-detector` | research-backed detector | codec-independent lossy-origin detection based on ICASSP 2017 | BSD-style per README | best external reference detector |
| `abalajiksh/audiocheckr` | headless DSP analyzer | profile-aware detection and multi-method spectral analysis | AGPL-3.0 | best source of ideas, not ideal code to vendor casually |
| `Angel2mp3/AudioAuditor` | desktop + CLI analyzer | broad heuristic analysis, effective cutoff logic, active project | Apache-2.0 | useful heuristics and output ideas |
| `sirjaren/redoflacs` | batch wrapper | operational authenticity analysis around older tools | MIT | useful integration precedent |
| `auCDtect` / `LAC` frontends | legacy authenticity tools | shows the problem space is old | mixed / wrappers | mostly historical context |

### What mattered most from prior art

#### 1. `vamp-lossy-encoding-detector`

This is the strongest direct prior art for actual lossy-origin classification.

Important traits:

- designed to detect lossy-origin signals regardless of current container
- can flag lossy-origin audio inside WAV / FLAC
- based on published research
- explicitly warns that it is useful but not perfect

For Cratedigger, this makes it a good shadow-mode baseline detector even if it
turns out to be awkward to embed directly.

#### 2. `audiocheckr`

This is the closest conceptual overlap with what Cratedigger needs as a headless
CLI / library-style system.

Two ideas from it stand out as immediately relevant:

- profile-aware sensitivity by content type
- multi-method detection instead of one cliff heuristic

That maps directly to the biggest weakness observed locally:

- electronic / ambient / unusual modern material can produce spectral shapes
  that do not fit a single fixed-threshold rule

The licensing note matters:

- Cratedigger is GPL-3.0
- `audiocheckr` is AGPL-3.0
- borrowing concepts is fine
- vendoring code should be a deliberate licensing decision, not an accidental
  copy-paste

#### 3. `AudioAuditor`

This project matters less as a dependency and more as proof that heuristic
cutoff-based systems can be built out much further than Cratedigger currently has.

Useful ideas:

- richer JSON / CLI output
- multiple related measurements instead of a single score
- explicit "actual bitrate" / effective ceiling reporting
- broader forensic framing rather than only one reject bit

### What prior art does not solve for us

The GitHub scan did not reveal a widely adopted public benchmark corpus that
everyone uses. That matters because it means the evaluation problem is still
ours to solve even if we reuse outside detectors.

In other words:

- prior art helps with algorithms
- it does not remove the need for a local trust-building harness

## What "Trustworthy" Should Mean Here

The right target is not "one magic model that never makes mistakes."

A trustworthy classifier in Cratedigger should mean:

1. We understand what kinds of bad files it catches.
2. We understand what kinds of bad files it misses.
3. We know which evidence is strong enough for hard auto-reject.
4. We know which evidence should only trigger manual review or soft staging.
5. We can measure regressions when thresholds or models change.

That implies a system, not just a function.

## Recommended Direction

### 1. Keep the corpus harness and make it the center of the work

The best thing produced during this research pass was not the narrow gate
patch. It was the evaluation harness.

It should become the long-term foundation for classifier work:

- live corpus summaries
- synthetic controls
- raw evidence export
- shadow comparisons across detectors

### 2. Add external detectors in shadow mode before trusting them in policy

The safest next step is not "replace Cratedigger's classifier."

It is:

- run an external detector in shadow mode
- record agreement / disagreement against Cratedigger's current detector
- inspect the disagreement set manually

The best candidate for this is `vamp-lossy-encoding-detector`.

If the integration friction is acceptable, it should be used as:

- an offline comparison backend in `scripts/spectral_corpus.py`
- then an optional shadow backend in the live pipeline

### 3. Adopt profile-aware logic internally

The single biggest idea worth stealing from prior art is profile-aware
sensitivity.

Cratedigger should likely distinguish between at least:

- standard modern pop / rock / electronic
- ambient / drone / noise
- lo-fi / cassette / archival
- speech / podcast

This does not require machine learning. It only requires admitting that one
fixed threshold set is not equally trustworthy across all content.

### 4. Split strong evidence from weak evidence

The current system already hints at this split, but not cleanly enough.

A better policy model would distinguish:

- multi-track cliff evidence
- single-track cliff evidence
- HF-deficit-only evidence
- agreement between independent detectors

Probable policy shape:

- hard reject:
  - multi-track cliff evidence
  - or strong agreement between independent detectors
- soft reject / manual review:
  - single-cliff high nominal bitrate cases
  - HF-deficit-only cases
  - profile-mismatched edge cases
- import:
  - genuine / marginal results with no stronger contrary evidence

### 5. Treat streaming-origin transcodes as a separate unsolved problem

The harness already showed that high-quality `AAC 256 -> MP3 320` and
`Vorbis q9 -> MP3 320` are not reliably caught by the current detector.

That means the project should explicitly separate two problem classes:

- obvious lowpass / cliff-bearing garbage
- modern, high-quality lossy-origin audio

The first problem is partially solved.

The second is not.

## Concrete Near-Term Work

### High priority

- add a shadow backend for `vamp-lossy-encoding-detector`
- extend `scripts/spectral_corpus.py` to compare Cratedigger vs. external detectors
- record disagreement sets in machine-readable output
- keep building a labeled edge corpus from:
  - failed imports
  - trusted library files
  - synthetic transcodes

### Medium priority

- add profile-aware thresholds to the internal classifier
- split album evidence into:
  - cliff-backed suspect tracks
  - HF-only suspect tracks
  - detector-agreement score
- add a "manual review" outcome between `reject` and `import`

### Low priority

- revisit ML-based approaches after the evaluation harness is mature
- investigate whether `AudioAuditor`'s heuristics or `audiocheckr`'s
  multi-method cutoff logic are worth selectively porting

## Working Conclusions

The main conclusions from this chat and research pass are:

1. The current Cratedigger audio classifier is useful, but narrow.
2. It catches obvious transcodes better than subtle modern lossy-origin files.
3. Soulseek really does contain a lot of bad high-bitrate files.
4. The current raw reject counts overstate the number of distinct bad cases.
5. The Colleen case showed that single-cliff high-bitrate policy decisions are
   not trustworthy enough to be treated as solved.
6. We are not inventing audio classification from scratch. There is real prior
   work, and we should integrate or borrow from it.
7. The most valuable thing we now have is the start of an evaluation harness.

## Decision

For now, the right framing is:

- do not trust one-off threshold tweaks as "the fix"
- do trust the harness as the foundation for future work
- treat external detectors as baselines to compare against, not as magic
  replacements
- move toward a system that produces evidence we can inspect, not just a final
  binary verdict

## References

External projects inspected during this research pass:

- <https://github.com/cannam/vamp-lossy-encoding-detector>
- <https://github.com/abalajiksh/audiocheckr>
- <https://github.com/Angel2mp3/AudioAuditor>
- <https://github.com/sirjaren/redoflacs>
- <https://github.com/piotrderen/auCDtect-Frontend>
- <https://github.com/baarkerlounger/LosslessChecker>
- <https://github.com/emps/Lossless-Audio-Checker-GUI>

Relevant local files:

- `lib/spectral_check.py`
- `lib/quality.py`
- `lib/download.py`
- `scripts/spectral_corpus.py`
- `tests/test_spectral_corpus.py`
- `docs/quality-verification.md`
