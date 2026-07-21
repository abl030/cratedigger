# Quality Verification

See also `docs/audio-classification-research.md` for the April 2026 research
log on classifier trust, corpus evaluation, false-positive risk, and external
prior art.

## Gold Standard Pipeline

The highest quality acquisition path for the library:

1. **Download lossless** (FLAC, ALAC, WAV) from Soulseek
2. **Verify with spectral analysis** — confirm the lossless file is genuinely lossless (not a lossy transcode wrapped in a lossless container)
3. **Convert to VBR V0** — `ffmpeg -codec:a libmp3lame -q:a 0`
4. **Import to beets** — the VBR V0 probe remains an auditable source fingerprint

VBR bitrate is useful evidence, not verification by itself. A genuine CD rip
converted to V0 commonly produces ~240-260kbps while a lossy transcode commonly
lands lower, but only an explicit verified-lossless proof completes acquisition,
and that proof lock is absolute in every import mode. The proof requires
affirmative spectral evidence, or the narrow V0 trust override after spectral
analysis ran and disagreed.

## Current Verification Methods

There is no quality acceptance floor. A structurally usable exact-release copy
may be retained regardless of codec, bitrate, or rank. Post-import policy then
has three outcomes: verified-lossless proof completes acquisition; an
unverified `TRANSPARENT` installed copy with its own `genuine` spectral fact
stays wanted but narrows to lossless-only; every other unverified copy stays
wanted on the full search surface.

### 1. VBR V0 source probe (implemented)

After lossless-to-V0 conversion, the resulting bitrate reveals source quality:
- **Genuine lossless**: ~220-280kbps (varies by musical complexity)
- **Transcode from ~192kbps**: ~190-210kbps
- **Transcode from ~128kbps**: ~160-180kbps

The codec rank bands may use these bitrates for relative comparison, but
`transcode_detection()` has no grade-blind bitrate fallback. Missing or errored
spectral analysis cannot be converted into a positive quality fact. The one
verification exception is the fixed V0 trust override: a comparable source
probe with avg ≥230kbps and min ≥200kbps can override an affirmative
`suspect`/`likely_transcode` spectral disagreement.

Limitation: This source-probe signal exists only when a lossless-container
candidate can be converted or temporarily probed. It does not by itself prove
native MP3 downloads (e.g. 320kbps that was upsampled from 128kbps).

### Provisional lossless-source probes

When a supported lossless-container download (FLAC, ALAC, WAV, or ALAC-in-M4A)
is spectrally `suspect` or `likely_transcode`, the importer no longer has to
discard the source outright. It records the MP3 V0 probe produced from that
source as `lossless_source_v0` attempt evidence and compares the probe average
against the current linked evidence row's V0 metric with `subject='source'`.

Policy:

- Missing current comparable source probe: import as
  `provisional_lossless_upgrade`.
- Candidate probe average above the current comparable probe by more than
  `QualityRankConfig.within_rank_tolerance_kbps`: import as
  `provisional_lossless_upgrade`.
- Candidate probe average equal, worse, or within tolerance: reject as
  `suspect_lossless_downgrade`.
- Missing candidate probe on a suspect lossless source: reject as
  `suspect_lossless_probe_missing`.

Provisional imports are deliberately not verified lossless. They may still use
the configured lossless-source storage target, but `verified_lossless` remains
false, the source is denylisted, normal post-import notifications run, and the
search policy keeps acquisition open. Automation returns the request to
`wanted`; terminal persistence retains an operator search stop current when
the request row is locked, while recording the same quality decision and
narrowing fields.

Exception: a comparable `lossless_source_v0` probe at avg ≥230kbps and min
≥200kbps is treated as stronger evidence than a suspect/`likely_transcode`
spectral grade. Those imports are verified lossless and follow the normal
`imported` path instead of the provisional keep-searching path.

Native lossy and on-disk V0 probes are research evidence only. Active evidence
stores them as `subject='installed', provenance='measured'`; they are not source
anchors and do not affect the provisional comparison lane.

### 2. Spectral Band Energy Analysis (historical v1 method)

Uses `sox` bandpass filtering to measure energy ratios in high-frequency bands relative to a 1-4kHz reference band. Genuine high-quality audio has consistent energy across the spectrum. Transcodes show a sharp drop at the original encoding's lowpass cutoff frequency.

#### Test Results (2026-03-28)

```
Label                                  ref RMS     14-16k%  16-18k%  18-20k%
Genuine FLAC (lossless)                0.118154      4.1%     2.7%     1.8%
Genuine V0 (from FLAC)                 0.118188      4.2%     2.8%     1.8%
Genuine 320 (from FLAC)                0.118158      4.1%     2.7%     1.8%
TRANSCODE 128->320                     0.112545      4.2%     1.0%     0.0%
TRANSCODE 192->320                     0.114673      4.2%     2.2%     0.7%
Hot Garden Stomp (suspect 320)         0.075199      0.7%     0.6%     0.4%
```

Observations:
- Genuine V0 is spectrally identical to FLAC — the conversion preserves the quality fingerprint
- The **18-20kHz band** is the most discriminating: 0.0% for 128 transcode vs 1.8% for genuine
- The **16-18kHz band** separates 192 transcodes: 1.0% (128-transcode) vs 2.2% (192-transcode) vs 2.7% (genuine)
- Hot Garden Stomp (320kbps, 1993 cassette) has less high-frequency energy than a 128->320 transcode — source was likely ~96kbps or lower
- LAC (Lossless Audio Checker) is useless for this purpose — reported "Clean" on all files including obvious transcodes

#### Method

```python
# For each track, measure RMS energy in bandpass-filtered ranges
sox file.mp3 -n sinc 1000-4000 stat    # Reference band (1-4kHz)
sox file.mp3 -n sinc 14000-16000 stat  # High frequency band 1
sox file.mp3 -n sinc 16000-18000 stat  # High frequency band 2
sox file.mp3 -n sinc 18000-20000 stat  # High frequency band 3

# Calculate: band_energy / reference_energy * 100 = percentage
# Genuine: 14-16k > 2.5%, 16-18k > 2.0%, 18-20k > 1.0%
# Suspect: any band significantly below these thresholds
```

Dependencies: `sox` (in nixpkgs)

#### LAME Lowpass Table (from source code)

| Bitrate (kbps) | Lowpass (Hz) | 14-16k% | 16-18k% | 18-20k% |
|----------------|-------------|---------|---------|---------|
| 96             | 15,100      | < 1%    | < 1%    | < 1%    |
| 128            | 17,000      | normal  | ~1%     | ~0%     |
| 160            | 17,500      | normal  | ~1%     | ~0%     |
| 192            | 18,600      | normal  | normal  | < 1%    |
| 256            | 19,700      | normal  | normal  | reduced |
| 320 CBR        | 20,500      | normal  | normal  | normal  |
| V0             | **disabled** | normal  | normal  | normal  |
| V2             | 18,671      | normal  | normal  | < 1%    |
| Lossless (CD)  | 22,050      | normal  | normal  | normal  |

Source: LAME `lame.c` `optimum_bandwidth()` function.

#### The 16kHz Shelf (strongest single indicator)

All MP3 encoders have a fundamental limitation: there is no scale factor band 21 (sfb21) for frequencies above ~16kHz. This forces the encoder to choose between less accurate representation above 16kHz or less efficient storage below. The result is a characteristic energy step-down ("shelf") at 16kHz that is:

- Present in **ALL** MP3 files regardless of bitrate
- **NOT** present in genuine lossless, vinyl rips, or cassette rips
- The strongest single automated indicator of MP3 origin

To detect the shelf, check the ratio: `energy(14-16kHz) / energy(16-18kHz)`
- Genuine lossless: ratio close to **1.0** (gradual decrease)
- MP3 transcode: ratio **3x-10x** (sharp cliff at 16kHz)

#### Edge Cases

- **Lo-fi recordings** (boombox, cassette, AM radio): Naturally have limited high-frequency content. The energy ratio approach handles this because it compares RELATIVE to the 1-4kHz band, not absolute levels. But very lo-fi material may have low ratios simply due to recording quality, not transcoding.
- **Classical/acoustic music**: May have less high-frequency energy than rock/electronic, but still maintains relative proportions. Need wider thresholds.
- **Cassette recordings**: Tape hiss adds energy across all frequencies including high bands. Genuine cassette rips may actually show MORE high frequency energy (as noise) than clean digital recordings.
- **Natural rolloff vs. artificial cutoff**: Vinyl and cassette have gradual, smooth HF rolloff. MP3 transcodes have sharp, blocky cutoffs. The shape matters more than the location.

#### Performance

Sox bandpass + stats takes ~0.5-1s per band per track. For 4 bands on a 12-track album: ~24-48s.

**Optimisation**: Analyse only the first 30 seconds: `sox "$file" -n trim 0 30 sinc 16k-18k stats`. Cuts time by ~75% with negligible accuracy loss (encoding parameters are consistent throughout a track).

### 3. Existing Tools Evaluated

| Tool | Works? | Notes |
|------|--------|-------|
| **LAC** (losslessaudiochecker) | **No** | In nixpkgs but useless — said "Clean" on 128→FLAC transcode |
| **spectro** (`pip install spectro`) | Maybe | Has automated `check` command with built-in thresholds, worth testing |
| **fakeflac** (GitHub) | Maybe | FFT + backward sweep for discontinuity, Python + scipy |
| **FLAC_Detective** (GitHub) | Maybe | 11-rule scoring system, claims to handle vinyl/cassette edge cases |
| **auCDtect** | No | Windows only, only analyses WAV for CD origin detection |
| **Fakin' The Funk** | No | Windows-only GUI |

### 4. Published Research

- **D'Alessandro & Shi (2009)**: "MP3 Bit Rate Quality Detection through Frequency Spectrum Analysis" — 97% overall accuracy using SVM on 100 frequency bands in the 16-20kHz range. Seminal paper.
- **FLAD**: Neural network (EfficientNet) achieving 99.75% accuracy. Analyses 2.4-20kHz, suggesting lossy artifacts exist in mid-frequencies too, not just at the cutoff. Heavy deps (PyTorch).

## V2: Spectral Gradient Analysis (tested 2026-03-28)

The wide-band energy ratio approach (v1) produces too many false positives on lo-fi and quiet music. V2 uses 500Hz slices and detects the **shape** of the rolloff instead of absolute levels.

### Method

1. Divide 12-20kHz into 16 x 500Hz slices
2. Measure RMS energy in each slice via `sox file -n trim 0 30 sinc {lo}-{hi} stat`
3. Compute gradient (dB/kHz) between adjacent slices
4. **Cliff detection**: 2+ consecutive slices with gradient steeper than -12 dB/kHz
5. **HF deficit**: average dB of top 4 slices (18-20kHz) vs reference band (1-4kHz)

### Per-track classification

- **SUSPECT**: cliff detected, OR HF deficit > 60dB
- **MARGINAL**: HF deficit 40-60dB, no cliff
- **GENUINE**: HF deficit < 40dB, no cliff

### Album-level classification

- **LIKELY_TRANSCODE**: >75% of tracks SUSPECT
- **SUSPECT**: >60% of tracks SUSPECT
- **GENUINE**: <60% suspect
- Never auto-reject; flag for review

### Verification and explicit non-verification lanes

1. **Lossless-container downloads**: Run spectral check pre-conversion. Genuine
   or marginal sources continue through the verified-lossless path. Suspect or
   likely-transcode sources still produce a V0 source probe, but they use the
   provisional lossless-source comparison lane instead of becoming verified.
2. **MP3 downloads (especially CBR 320)**: Run spectral check post-download. Cliff + high deficit = upsampled garbage.
3. **High-band native VBR MP3**: The named policy may deliberately skip
   spectral analysis. This is an explicit non-verification lane: bitrate can
   drive relative rank, but it cannot mint verified-lossless proof.

If a candidate-side required scan is missing or errors, preview records
`measurement_failed`; if fresh analysis of an installed HAVE is missing or
errors, dispatch records `have_analysis_error`. Both are environment failures,
not quality verdicts: the request returns to ordinary wanted searching with no
denylist or narrowing consequence. A later attempt measures again from scratch.

### Tuning results (Mountain Goats library, 65 albums)

Tested across the entire Mountain Goats catalogue — a worst-case scenario as the band's early work (1991-2000) was recorded on boomboxes and cassette recorders with genuinely minimal high-frequency content.

At `HF_DEFICIT_SUSPECT=60dB + cliff detection`:
- **19 correctly flagged SUSPECT** (confirmed bad source, transcodes, or upsampled 320s)
- **46 correctly GENUINE** (including lo-fi albums with good V0 conversion bitrates)
- **0 false positives** on albums with verified good sources
- Successfully catches: cliffs at 16kHz (128kbps transcodes), cliffs at 18kHz (192kbps), upsampled CBR 320, terrible pre-pipeline rips

Historically, albums downloaded as FLAC and converted to high-bitrate V0 with
no cliff passed this corpus. That observation informs the V0 override, but
bitrate alone is not proof in the current policy.

### What the spectral check catches that V0 conversion doesn't

- **CBR 320 downloads**: V0 conversion only happens for FLACs. Native MP3 320 downloads skip conversion entirely. Spectral check catches upsampled garbage (e.g. Hot Garden Stomp at 52dB deficit, Songs for Peter Hughes at 72dB + cliffs).
- **Pre-pipeline imports**: Albums imported before the pipeline existed have no download history or V0 conversion data. Spectral check is the only way to assess their quality.

### Reference: HF deficit ranges observed

| Source quality | HF deficit range | Cliffs? |
|---------------|-----------------|---------|
| Genuine CD rip (FLAC) | 28-46dB | None |
| Genuine V0 from FLAC | 32-48dB | None |
| Lo-fi genuine (Mountain Goats boombox era) | 42-59dB | None |
| Transcode 192→anything | 53-67dB | Often (at 18kHz) |
| Transcode 128→anything | 71-84dB | Always (at 16kHz) |
| Upsampled CBR 320 (from ~96kbps) | 52-97dB | Sometimes |
| Quiet jazz/classical (genuine CD) | 33-57dB | None |
| Children's choir (genuine CD) | 31-62dB | None |

## Edge cases

- **Lo-fi recordings** (Mountain Goats boombox era): genuine V0 from verified FLAC can produce ~207 kbps. The `"mp3 v0"` label can still classify as `TRANSPARENT`, but rank never completes acquisition. Proof completes it; an unverified transparent installed copy narrows to lossless-only only when its own spectral grade is `genuine`.
- **Mixed-source CBR** (e.g. 13 tracks at 320 + 1 track at 192): looks like VBR to `COUNT(DISTINCT bitrate)` but is not genuine V0. There is no acceptance floor and no grade-blind CBR narrowing. Unless the installed result is both `TRANSPARENT` and spectrally `genuine`, it remains wanted on the full search surface.
- **Fake FLACs**: MP3 wrapped in a lossless container. Spectral detects the
  cliff pre-conversion, and the V0 probe becomes comparable source-subject
  evidence. Source denylisted, file imported only as provisional when the probe
  is meaningfully better than the current comparable source probe, and the
  request stays wanted.
- **Discogs-sourced albums**: numeric IDs stored in `mb_release_id` for pipeline compat. Beets auto-routes numeric IDs to the Discogs plugin via `--search-id`. `detect_release_source()` in `lib/release_identity.py` distinguishes UUID vs numeric format for conditional UI rendering. The full pipeline (search, download, validate, import, quality gate) works identically for both sources.

## Downgrade prevention

- `--override-min-bitrate` arg: preview/dispatch derive the comparison floor from linked current evidence or the same attempt's fresh HAVE audit. When spectral says the installed files are 128 kbps but the container says 320 kbps (fake CBR), the spectral truth is used so genuine upgrades are not blocked. Request-row quality stamps never feed this value.
- `ImportResult.verified_lossless_proof` is the sole acquisition claim. `AudioQualityMeasurement` contains only byte observations; evidence persistence derives its CHECK-tied convenience boolean from proof presence rather than re-deriving verification from a measurement.
- Spectral request-state writes always go through `RequestSpectralStateUpdate` so the historical grade/bitrate stamps stay atomic. Active decisions use the linked evidence row's spectral fact, not those request scalars.
- `--target-format` flag: when `target_format="lossless"` (or legacy `"flac"`), keeps lossless on disk. ALAC/WAV sources are normalized to FLAC via `FLAC_SPEC`. A temporary V0 probe is still produced when needed for provisional source comparison. Keeping a lossless container does not itself verify it; the import needs affirmative proof.
- `--verified-lossless-target` flag: target format after verified lossless, and the configured lossless-source storage target for accepted provisional imports (e.g. "opus 128", "mp3 v2", "aac 128"). Passed from `dispatch_import_core()` when `cfg.verified_lossless_target` is set. When the target has the same `.mp3` extension as V0, V0 files are removed before target conversion.
- `--force` flag: skips the distance check (`max_distance=999`) for force-importing rejected albums. Used by `pipeline_cli.py force-import` and `POST /api/pipeline/force-import`.
- Exit codes: 0=imported, 1=conversion failed, 2=beets failed, 3=path not found, 5=downgrade or suspect-lossless rejection, 6=transcode/provisional path (may or may not have imported as an upgrade).

## Comparison basis — the persisted decision explanation

Every `compare_quality()` call returns a `QualityComparisonBasis`
(`lib/quality/evidence_types.py`): the verdict plus which branch fired
(`rank`, `metric_tiebreak`, `label_contract_same_rank`,
`cross_family_same_rank`, `lossless_same_rank`, `metric_missing`,
`transcode_rank_regression`), the per-side ranks, the values that decided
that branch (spectral-clamped values on a clamped rank comparison, raw
configured-metric values on a tiebreak), and the per-side statistic actually
classified (`min`/`avg`/`median` — the configured metric falls back to min
when unmeasured). An explicit codec label such as `opus 128` is instead
persisted as `contract`: the label's declared bitrate is policy, not a measured
statistic. A temporary V0 probe may still inform source quality, but it never
becomes an `OPUS` measurement. `import_quality_decision()` stamps
`verified_lossless_bypass=True` only when the bypass changed the outcome
(an "equivalent" verdict imported).

The basis rides `MeasuredImportDecisionResult.comparison_basis` →
`ImportResult.comparison_basis` (harness stdout + `download_log.import_result`
JSONB), the decision dict's `comparison_basis` key (as `msgspec.to_builtins`
plain dict — the dict crosses json.dumps'd API responses), the evidence
action file, and the dispatch-synthesized reject `ImportResult`. Re-typing
back from the dict goes through `comparison_basis_from_decision()` — the one
converter.

Version 4 import results persist five disjoint concerns:

- `source_measurement` is measured from the downloaded bytes before mutation;
- `verified_lossless_proof` is the optional acquisition claim, deliberately
  separate from every measurement;
- `v0_probe` is the temporary research/provisional encode;
- `target_quality_contract` is configured policy used explicitly by comparison
  and gate ranking. It owns the target bitrate mode (`is_cbr`) as well as its
  label, so source/output CBR observations cannot change projected MP3 rank;
  bare `MP3` therefore requires an explicit projected or materialized CBR/VBR
  fact, while labels such as `mp3 v0`, `mp3 320`, and `opus 128` remain
  self-describing; and
- `materialized_measurement` is built from the postflight Beets album info
  after conversion and import. It records the actual stored codec plus
  min/avg/median bitrate. This is deliberately separate from
  `comparison_basis` (the policy explanation), and `v0_probe` (a temporary
  research encode). Audit UIs must use the materialized measurement for claims
  about output bytes and must leave historical output unknown when that field
  is absent. V1/v2 rows pass through the explicit legacy projection and carry
  `legacy_projection_version`; only that quarantined reader preserves their
  historically ambiguous `new_measurement` shape.

Every new measured format is a bare codec label (`FLAC`, `MP3`, `AAC`, `Opus`,
`Vorbis`, `WMA`):
profile/bitrate labels such as `mp3 v0` and `opus 128` belong only to the
target contract. Source measurements cannot carry `was_converted_from`; that
field describes materialized output lineage. These rules are enforced at the
v4 wire decoder/encoder and again before evidence persistence. Active evidence
rows carry `lineage_version=4`: spectral and V0 facts add `subject`
(`installed` | `source`) and `provenance` (`measured` | `carried`), while
verified-lossless lives only in its proof object. Migration 055 maps old field
names best-effort; current-evidence loaders treat v1/v3 rows as rebuild-required
rather than guessing v4 meaning. Actual import/action attempts remeasure the
exact installed Beets album before deciding. A same-snapshot repair preserves
its original `measured_at` and atomic neutral facts so historical Recents cards
remain pre-attempt evidence.

**Motivation (request 6039 / download_log 36608):** a genuine avg 196→288
rank upgrade (GOOD → TRANSPARENT) rendered as "Upgrade: MP3 V2 to MP3 V2"
because every UI label re-derived from min bitrate (194 on both sides).
The web UI (`web/classify.py::_verdict_from_basis`, the Recents evidence
strip, and the detail grid's "Compared" row) renders the persisted basis
verbatim when present; rows predating the field fall back to the legacy
min-based labels. Never re-derive a comparison for display — that
re-derivation is how the display learned to lie.

**Metric labels are truthful at synthesis too (download_log 36660):** the
same lie can be injected one seam earlier — the decision layer used to
synthesize comparison measurements with `avg` fabricated `= min` (the
lossless-conversion path carries only the post-conversion min across the
flat decision interface), so a persisted basis read "avg 216k" while the
files' real avg was 255. Explicit targets now persist the codec contract
(`OPUS 128 contract`); without a contract, synthesized measurements leave
unmeasured stats `None` so `_selected_bitrate_with_source` falls back to the
min and labels it `min`. Guarded by the `assert_basis_metrics_truthful` generated
property (`tests/test_quality_generated.py`) and the request-8781 pins in
`tests/test_quality_classification.py`.

## Further research

- [ ] Test `spectro` pip package as second-opinion validation.
- [ ] Reduce spectral-analysis cost (16 sox calls per track x 30s trim is
  roughly 8s/track, or ~100s per 12-track album).


## Evidence addressing, propagation, and ownership

> Relocated from CLAUDE.md (2026-07-04 doc simplification) — this is canonical policy, not narrative.

**Evidence is content-addressed.** `album_quality_evidence` rows are keyed
by `(mb_release_id, snapshot_fingerprint)`; addressing entities reference
them via `import_jobs.candidate_evidence_id`,
`download_log.candidate_evidence_id`, and
`album_requests.current_evidence_id`. Triage walks the FK chain (direct →
cross-walk via `request_id` → measure as last resort). Evidence is never
deleted unless the files actually change.

**`source_path` is immutable capture provenance, not live path authority.**
It records where the evidence snapshot was first measured. A same-address
upsert may fill a legacy blank value, but it never replaces an existing
nonblank path when the same bytes are later observed in staging, quarantine,
or the Beets library. Candidate actions validate their active job path against
the stored file manifest and carry that transient path separately from the
evidence row. Current-library consumers resolve the exact release through the
fresh typed Beets authority, then validate that current path against the same
fingerprint. Neither boundary treats historical `source_path` as the location
to launch or scan.

When an exact release leaves Beets, `clear_on_disk_quality_fields` unlinks
`album_requests.current_evidence_id` together with the other installed-state
fields. The content-addressed evidence row remains as audit history; only its
claim to describe the request's current files is removed.

**Evidence survives the candidate → library transition by explicit markers.**
After a successful import, `propagate_candidate_evidence_to_current` builds a
new row from the installed library snapshot. Installed bitrate, format,
container, file inventory, and other on-disk facts are freshly measured.
Installed-subject spectral and V0 facts never cross a fingerprint change.
The ordinary enrichment path remeasures those facts against the installed
snapshot when policy needs them.

**The canonical acquisition-fact set is exactly:** verified-lossless proof,
source-subject spectral, and the source-subject V0 anchor. These facts cannot be
re-derived from converted library bytes, so they carry to the new row with
`provenance='carried'`. Their `subject='source'` continues to say that they
describe the upstream acquisition bytes, not the installed derivative.
Propagation reads those markers directly; codec names and conversion shape are
never used as a lineage heuristic. Wrong-match cleanup may compare future
candidates against these explicit source anchors. Rebuilds, migrations, and
operator one-shots must reference this canonical set rather than restating a
subset.

The same rule governs every evidence rebuild: proof and source-subject facts
carry unconditionally with provenance `carried`; installed-subject facts are
remeasured and can only have provenance `measured`. Proof is conceptually a
source acquisition fact, so it needs only its provenance marker.

**A changed installed snapshot is linked before neutral enrichment, but it is
not immediately action authority.** The new content-addressed row must exist
first so the preview-owned spectral and V0 writers can target its exact id and
fingerprint. Such a rebuild sets `current_enrichment_required=true`; action
loaders keep failing closed on every unchanged retry until that exact row has
both a spectral result and either a V0 metric or the persisted once-only V0
attempt marker. Source-subject spectral/V0 facts may satisfy the gate because
they survive byte changes by definition; installed-subject facts do not and
must be measured again. The marker is monotonic for a content address, so a
same-address upsert from another writer cannot erase the retry gate. It need
not be cleared after enrichment: completeness of the required facts is what
makes the row authoritative.

**A genuine spectral grade does not prove source bitrate for fullband codecs.**
Opus can retain a fullband cutoff at low bitrates, so a native Opus scan may
look spectrally genuine without establishing that the acquisition was
high-bitrate or transparent. More importantly, scanning an installed Opus copy
derived from a lossless source measures the encoder output, not the upstream
acquisition; persisting that result as source lineage would discard the fact we
need to retain. That is why lossless-derived installed spectral facts are
forbidden: the source-subject spectral fact carries instead. A native-Opus
genuine grade may still participate in the documented narrowing policy, but it
is weak spectral evidence, never proof of bitrate or verified-lossless
acquisition.

**Missing or incomplete current evidence converges at the failure point.** A
current-evidence row's spectral scan and on-disk V0 research normally
complete during import preview — but a request whose downloads always
fail never reaches preview, so its HAVE snapshot (and therefore the
Recents IN/HAVE strip) stays absent or partial forever. Download-phase
failure finalizers (`_timeout_album` and the materialize-grace reset in
`lib/download.py`) therefore perform two fail-soft steps. Before recording
the failed attempt, `prepare_current_evidence_for_failure`
(`lib/import_preview.py`) loads or backfills only the exact release's
canonical current snapshot. Even an already-linked complete row is freshly
reauthorized against the exact Beets identity and current fingerprint; when
both still match, the immutable evidence row is reused without rewriting it.
After the download log and request-state reset are safely persisted,
`enrich_incomplete_current_evidence_for_request`
plans exactly the missing pieces (`plan_current_evidence_enrichment`, pure),
measures the on-disk copy directly, and persists through the same
preview-owned once-only helpers — never overwriting present evidence, never
re-probing an attempted snapshot, and refusing stale on-disk state. Adapter
or backfill failures and actual measurement work consume the per-cycle
`CratediggerContext.evidence_enrichment_budget`; complete or authoritatively
absent library copies cost nothing. Over time the failed-download cohort's
evidence converges without delaying or bypassing download cleanup. Automation
failure finalizers also reset the request to `wanted`. Terminal persistence
checks the operator search stop under its request-row lock for every `wanted`
transition, including rejection, HAVE-abort, and local-completion bundles. It
retains policy fields plus attempt/backoff accounting without clearing the
stop. An operator lifecycle command already waiting behind that lock retries
against the committed status, so neither concurrency ordering loses the
operator action.

**A blank `source_path` is policy-incomplete.** The field is required capture
provenance, even though live path authority comes from the active job or fresh
Beets resolution. Legacy 2026-05 library backfills wrote `source_path=''` and
therefore left evidence without an auditable capture location. Before issue
#711, that
incomplete HAVE side silently disabled all three spectral protections in the
import comparison (download_log 37206: a ~96k transcode replaced a better copy
as a "better" avg-bitrate tiebreak). Fresh HAVE analysis is now a prerequisite;
failure aborts the attempt as `have_analysis_error`. `policy_incomplete_reasons`
therefore rejects blank paths; the action loader
(`ensure_current_evidence_for_action`) and the preview loader
(`load_current_evidence_for_preview`) rebuild such rows from beets.
When the on-disk files are unchanged (the legacy-backfill case) the
rebuilt row shares the `(mb_release_id, snapshot_fingerprint)` content
address, so the upsert fills the blank `source_path` in place — same row id,
FK untouched. A nonblank capture path is immutable and is never rewritten by
another observation of that address. If the files have changed since capture,
backfill writes
a fresh row, repoints the FK, and persists the changed-snapshot enrichment
gate described above. Either way enrichment can then complete the surviving
row. The candidate-reuse preview fast path first verifies the content
snapshot, then projects the candidate spectral fact from that content-addressed
evidence without scanning those bytes again. It separately persists any
required attempt-time HAVE scan through
`persist_exact_current_spectral_from_attempt` before marking the job
importable, so reused-evidence force and automation imports decide against the
same completed HAVE the full measurement path would see. A changed candidate
snapshot misses the front gate and runs full preview measurement again.

**Search narrowing companion.** When `lossless_source_locked` fires —
in the importer (`lib/dispatch/core.py`) or wrong-match cleanup
triage (`lib/wrong_match_cleanup_service.py`) — the request's
`search_filetype_override` is narrowed to `"lossless"` via
`narrow_override_on_lossless_source_lock` (`lib/quality/dispatch_actions.py`). Future
search cycles only ask Soulseek for lossless tiers, so the lock
doesn't fire repeatedly against new peers serving the same lossy
file. No plan-generator change is needed — `generate_search_plan`
produces query strategies, and the filetype filter is applied
downstream in `lib/enqueue.py::effective_search_tiers` from the request's
override column.

A second lossless-only narrowing applies when an attempt proves that
the exact installed HAVE copy is both **TRANSPARENT** under the canonical
codec rank bands and spectrally **genuine**. The importer uses the independent
attempt-local HAVE audit; validation rejects may use only the request's linked,
complete current-evidence row. Candidate spectral results and the legacy
request scalar are not substitutes. MP3, AAC, Opus, Vorbis, and WMA participate
through `measurement_rank`; unknown codec families, merely EXCELLENT lossy
copies, and missing/failed/suspect/marginal audits fail open and do not
authorize lossless-only narrowing. Ordinary downgrade convergence still
removes the exact rejected tier from an existing search ladder. A positive
result writes only
`search_filetype_override="lossless"`: `target_format` remains untouched, and
`search_tiers` disables the catch-all fallback for that override. The normal
forever cadence continues, now searching only for the remaining meaningful
upgrade: lossless. That narrowing is monotonic across successful retained
imports: post-import persistence and the quality gate preserve an existing
`"lossless"` override even when the new retained-copy decision would normally
propose the full search surface. Only verified-lossless terminal acceptance or
explicit operator intent may clear it; evidence/decision failures retain their
separate fail-open recovery policy. Every other unverified retained copy that
starts unrestricted stays wanted on the full search surface. Only
verified-lossless proof ends acquisition, in every import mode.

Older library rows may still have NULL spectral / V0 / bad-hash facts. The
deploy transition materializes each member of the canonical acquisition-fact
set defined above that already exists in request history, but it never invents
missing facts. Wrong-match and narrowing policy wait for a complete linked
evidence row; fresh attempts remeasure the installed bytes.
`lossless_source_locked` remains a separate defense-in-depth narrowing path. See
`docs/brainstorms/2026-05-17-propagate-source-evidence-on-transcode-requirements.md`.
