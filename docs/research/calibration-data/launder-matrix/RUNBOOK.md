# Launder-matrix harness — runbook for family agents

Scratch only. Nothing here is tracked; do not copy into the repo.

- code + outputs: `/home/abl030/.claude/jobs/fb229c18/tmp/matrix/` (doc1)
- built launder FLACs: `/mnt/virtio/Music/matrix/<variant>/<request_id>/NNN.flac`
- ALL python runs from the repo root:
  `cd /home/abl030/cratedigger/.claude/worktrees/829-phase5-pr2 && nix-shell --run "python3 /home/abl030/.claude/jobs/fb229c18/tmp/matrix/<script> ..."`

## The rule that shapes everything

Every variant is measured as an **actual launder** — encoded, decoded back to
16-bit FLAC, measured through the real production path. Never infer a launder
result from native-encode statistics. Opus is the standing proof: native
`.opus` numbers say ~60% caught, the real launder says 100%.

## Three steps

```
build.py    --variants a,b,c [--albums 8918,8923] [--workers 4]
measure.py  --tag T --variants a,b,c [--albums ...] [--workers 6]
derrien.py  --tag T --variants a,b,c [--albums ...] [--workers 24]
score.py    --tag T [--control genuine]
```

All three are idempotent and resumable — re-running skips completed work
(`/mnt/virtio/Music/matrix/<v>/index.json`, `measure_cache/<tag>/`,
`derrien_cache/<tag>/`). Safe to interrupt and restart.

`--variants` always wants BOTH controls: `genuine,null-flac`.

## Outputs (committed calibration-data format)

| file | shape |
|---|---|
| `results-<tag>.tsv` | 27 col, headerless — merges with `results*.tsv.gz` |
| `extended-<tag>.tsv` | 6 col, headerless — merges with `extended*.tsv.gz` |
| `albums-<tag>.json` | production `AlbumResult` facts per album |
| `derrien-<tag>.tsv` | per track: proba, median, std, z, offset, pmode |
| `gate-<tag>.json` | per album: legs+verdict at T=62 and T=59.5, U, Derrien |
| `tracks-<tag>.tsv` | per track: the 11 gate columns + the 3 Derrien columns |

## Adding a variant

Edit `variants.py`. Every entry MUST carry the exact `argv` template — that is
the record, and it is written into `index.json` along with the encoder version
string and one fully-rendered example command. The old matrix's `lame-v0` is
unusable precisely because its command line was not preserved.

`kind`: `genuine` (no build) | `null` (no lossy step) | `local` | `remote_qaac`.

## Corpus

`sources.tsv` (19 albums, 230 tracks, 1044 min) -> `mkmanifest.py` ->
`manifest.json`. **8920 and 8931 are 96 kHz/24-bit** — they carry a resampling
and a requantisation confound; exclude them unless that is the thing being
measured, and always score them against their own `null-flac`.

## Things that will bite you

- **`lame -V0` disables the polyphase lowpass** (stock LAME 3.100 behaviour,
  it prints so). `ffmpeg -q:a 0 -c:a libmp3lame` is **bit-identical** to it —
  verified. Keep only one of `lame-v0` / `ffmpeg-q0` in a full matrix.
- **libopus always encodes at 48 kHz**, so an Opus launder decodes to a 48 kHz
  FLAC even from a 44.1 kHz source. That is real, not a harness resample — but
  it means the 20.5-21.5 kHz measurement band sits far below Nyquist, unlike a
  44.1 kHz file where it sits at the edge. Do not read "Opus is caught" as
  purely a codec result without saying so.
- **libfdk_aac is not in this ffmpeg** (licensing). HE-AAC/SBR cannot be built
  here. `aacffm-*` is ffmpeg's native encoder; `qaac-*` is Apple CoreAudio.
  They are different encoders and behave differently — never conflate them.
- **Derrien's genuine baseline is strongly album-dependent** (max proba 0.0180
  on Grouper *Ruins* vs 0.1082 on Autechre *Tri Repetae*). A pooled zero-FP
  threshold is set by the worst album and destroys the statistic. Score
  per album against that album's own genuine control.
- Run intermediates stage on doc1 local disk and are deleted per album; only
  final FLACs touch virtiofs. Keep `--workers` modest — virtiofs has a
  documented IOPS failure mode on this fleet.

## Cost (measured)

Per variant, all 19 albums / 230 tracks: build ~5 min, measure ~1 min,
Derrien ~3.2 core-hours (~8 min wall at 24 workers), ~6 GB disk.
**~15 min wall and ~6 GB per variant.** Derrien dominates; it is 49 s CPU
per track and is the only reason to think about scheduling at all.
