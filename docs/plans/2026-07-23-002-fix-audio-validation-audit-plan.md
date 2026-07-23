# Audio-Only Validation and Durable Failure Audit — Plan (issue #835)

**Status:** implementation-ready after live reproduction and official FFmpeg
contract audit

**Issue:** https://github.com/abl030/cratedigger/issues/835

**Decision record:**
https://github.com/abl030/cratedigger/issues/835#issuecomment-5055079046

**Date:** 2026-07-23

## Verdict

Issue #835 is a real data-integrity defect, not merely noisy FFmpeg output.

The exact Syro calibration FLAC passed the current `validate_audio()` because
FFmpeg's default error resilience printed frame/decode failures and returned
zero. The production-shaped FLAC-to-Opus command did the same and produced a
structurally valid derivative. Comparing decoded lengths showed that the
derivative was about 104 ms / 4,608 source samples shorter. Conversion can
therefore launder a recoverably corrupt FLAC into an apparently clean Opus
while deleting the only source that still carries the decoder evidence.

The database cannot currently explain that event:

- `validate_audio()` discards all stderr on exit zero;
- successful conversion discards stderr;
- failed conversion prints only a short stderr tail to the service journal;
- `AlbumQualityEvidence` persists only `audio_corrupt` plus one scalar
  `audio_error`;
- `ConversionInfo` persists counts and format facts, not process diagnostics.

The fix is one coherent boundary: a read-only, audio-only validator emits a
typed report; preview persists that report as content evidence; the existing
importer decider remains the only authority that rejects and denylists bad
audio; conversion uses the same strict decoder policy and cannot delete the
source unless it completed successfully.

## Authority and corrected invariants

Authority: "Metadata is completely out of scope for this validator: malformed
tags, pictures, chapters, and exit-zero metadata diagnostics are ignored and
no metadata state is persisted." —
https://github.com/abl030/cratedigger/issues/835#issuecomment-5055079046

Authority: "Any owned, readable source that FFmpeg cannot fully validate as
audio is bad audio: persist the exact bounded failure, let preview persist
`audio_corrupt` evidence, and let the importer’s existing unified decision
path denylist the peer, move the source to `failed_imports/bad_files`, and
resume searching." —
https://github.com/abl030/cratedigger/issues/835#issuecomment-5055079046

Authority: "`measurement_failed` is reserved for failures in Cratedigger’s
world—permissions/readability, missing executable, vanished or changed path,
external process termination, or persistence failure—and must never
blame/denylist the peer." —
https://github.com/abl030/cratedigger/issues/835#issuecomment-5055079046

Authority: "Every path retained as `measurement_failed` evidence must be
protected from automatic reaping." —
https://github.com/abl030/cratedigger/issues/835#issuecomment-5055079046

The implementation invariants are:

1. A readable file whose mapped audio stream cannot complete a full strict
   decode is persisted as `audio_corrupt`; preview does not decide, and the
   importer rejects it through `full_pipeline_decision_from_evidence`.
2. Exit-zero stderr has no policy or audit meaning. It is discarded without
   matching, counting, hashing, or classifying tags, pictures, chapters, or
   any other metadata.
3. A failure to perform the measurement is not content evidence. It produces
   `measurement_failed`, never `audio_corrupt`, and never a source denylist
   row.
4. Every validation attempt that reaches a terminal preview state leaves a
   typed audit: content evidence for pass/reject, or the typed preview failure
   payload for a world failure.
5. `failed_imports/` remains unconditionally reaper-protected. A referenced
   `measurement_failed` source that could not be moved there is also never
   automatically reaped.
6. Conversion never removes or replaces a source until FFmpeg returns success
   and the output exists and is nonempty.
7. Historical converted derivatives do not acquire fabricated proof about a
   deleted source. They remain explicitly `legacy_unrecorded`.

## FFmpeg contract

The supported command shape is:

```text
ffmpeg -hide_banner -nostdin -v error
  -max_error_rate 0
  -abort_on empty_output_stream
  -err_detect:a crccheck+bitstream+buffer+explode
  -vn -sn -dn -i INPUT
  -map 0:a
  -map_metadata -1
  -map_chapters -1
  -f null -
```

The contract is deliberately smaller than "interpret every FFmpeg exit code":

- exit `0` means the selected audio completed under this command;
- exit `69` is FFmpeg's documented final status when the counted decoder-frame
  error rate exceeds `-max_error_rate`;
- another positive exit has no documented semantic taxonomy and is persisted
  honestly as `ffmpeg_failed_unclassified`;
- a timeout, missing executable, negative signal return, or filesystem failure
  is classified by the Python subprocess/filesystem layer;
- `-err_detect:a` is audio-scoped because unqualified `-err_detect` can also
  tighten the demuxer; decoder support for its flags remains codec-specific;
- `-xerror` is not used because it is global, broader than audio decoding, and
  prevents a complete-file check.

Official references:

- https://www.ffmpeg.org/ffmpeg.html#Advanced-options
- https://raw.githubusercontent.com/FFmpeg/FFmpeg/n8.1.1/doc/ffmpeg.texi
- https://raw.githubusercontent.com/FFmpeg/FFmpeg/n8.1.1/fftools/ffmpeg_dec.c
- https://www.ffmpeg.org/ffmpeg-all.html#Codec-Options
- https://raw.githubusercontent.com/FFmpeg/FFmpeg/n8.1.1/libavcodec/flacdec.c
- https://www.ffmpeg.org/ffmpeg.html#Stream-selection

FFmpeg must still parse enough of the container to discover its audio streams.
The guarantee is not "metadata is never read"; it is "metadata is never mapped
into the output, never persisted by the validator, and exit-zero metadata
diagnostics never influence the result."

## Failure classification

`validate_audio()` first enumerates the stable audio path set and proves each
file is readable by this process. If FFmpeg fails ambiguously, it performs one
complete read of that same file to distinguish an input/filesystem failure
from readable bytes that FFmpeg could not validate.

| Observation | Typed outcome | Policy consequence |
|---|---|---|
| mode is `off` | `skipped` | no corrupt fact; explicitly not a pass |
| no audio files | existing empty-fileset fact | the existing decider owns the reject |
| FFmpeg exit `0` | `passed` | no stderr retained |
| FFmpeg exit `69`, readable source | `audio_corrupt/decode_error` | evidence → importer reject → denylist + quarantine |
| other positive FFmpeg exit, readable source | `audio_corrupt/ffmpeg_failed_unclassified` | same reject path; preserve exact bounded explanation |
| generous timeout, readable source | `audio_corrupt/decode_timeout` | same reject path; prevents reacquiring the same pathological source |
| input open/read fails | `measurement_failed/read_error` | no denylist; retain/protect source path |
| FFmpeg missing/cannot start | `measurement_failed/process_unavailable` | no denylist; retain/protect source path |
| FFmpeg terminated by signal | `measurement_failed/process_interrupted` | no denylist; retain/protect source path |
| source changed/vanished or DB persistence fails | existing typed `measurement_failed` reason | no denylist; retain/protect source path |

Human stderr is explanation, never the classifier. A positive FFmpeg failure
on readable bytes is a source rejection because this fixed command has no
writable media output, no metadata work, and no optional stream-selection
work left to perform. Its report still says `ffmpeg_failed_unclassified`
rather than inventing a decoder meaning that FFmpeg does not document.

## Persisted model

Add migration `064_audio_validation_report.sql`.

Define the JSON wire types in the quality package and re-export them through
`lib.quality`:

```text
AudioToolDiagnostic
  relative_path
  category
  return_code
  stderr_excerpt
  stderr_bytes
  stderr_sha256
  stderr_truncated

AudioValidationReport
  policy_id
  tool
  tool_version
  outcome
  files_checked
  files_failed
  diagnostics
  omitted_diagnostics
```

The fixed bounds are part of the contract:

- at most 16 failure diagnostics per album;
- at most 2 KiB of normalized stderr per diagnostic;
- original byte count, SHA-256, and truncation state preserve the fact that a
  larger diagnostic existed;
- success/skipped reports contain no stderr data;
- one lazily cached `ffmpeg -version` probe supplies the version identity
  without one extra process per track.

`album_quality_evidence.audio_validation` becomes a non-null JSONB column.
It is the canonical validation audit; `audio_corrupt` and `audio_error` remain
the decision/query projection for the existing decider and UI, with storage
validation plus a SQL CHECK preventing disagreement with the report.

The pure-SQL migration backfills truthfully:

- existing `audio_corrupt=true` rows become `legacy_failure` reports carrying
  the existing scalar diagnostic;
- every other existing row becomes `legacy_unrecorded`;
- no historical row becomes `passed`.

World failures cannot become `AlbumQualityEvidence`, because no authoritative
content measurement completed. The same `AudioValidationReport` is nested in
`MeasurementFailure` and therefore persists through both
`import_jobs.preview_result` and `download_log.validation_result`.

`ConversionInfo` gains bounded per-file process failures using the same
diagnostic type. That object already crosses harness stdout and
`download_log.import_result`, so no parallel conversion-log table is added.

## Coverage ledger

| Requirement | Production owner | Evidence owed |
|---|---|---|
| Strict audio-only full decode | `lib/util.py` | real corrupt FLAC pin + generated frame mutations |
| Metadata has zero meaning | validator command/result boundary | arbitrary exit-zero stderr property; unset-MD5 file remains byte-identical |
| World failure is distinct | validation result union + `lib/import_preview.py` | permissions/read/process-signal table + preview lifecycle slice |
| Preview persists; importer decides | `lib/measurement.py`, `lib/quality_evidence.py`, existing full decider | evidence round trip + real dispatch reject/denylist/quarantine slice |
| Structured evidence audit | migration + `lib/quality/evidence_types.py` + `lib/pipeline_db/evidence.py` | real PostgreSQL every-field round trip and CHECK violations |
| Structured failure-attempt audit | `MeasurementFailure`, preview worker terminal bundle | fake/PG terminal persistence parity |
| Reaper never touches retained failures | `lib/slskd_transfers.py`, PipelineDB read projection | aged owned measurement-failure path deterministic + generated property |
| Conversion cannot launder decoder failure | `harness/import_one.py`, `ConversionInfo` | production-shaped FLAC→Opus failure pin |
| Source survives conversion failure | `convert_lossless()` | filesystem assertion for ordinary and same-extension temp output |
| Logs remain bounded | validator + harness logging | one-line logger assertions and large-stderr bound |
| Operator can inspect cause | existing evidence/download-log joins and CLI views | CLI JSON/human rendering test; no new action/route |
| Legacy truth remains honest | migration | backfill tests for pass/failure legacy rows |
| Live runtime is fixed | doc2 deployed wrapper + live retained corrupt canary | strict validator rejects canary; DB audit and quarantine/denylist visible |

## Implementation units

### U1 — Write the invariant pair red-first

**Files:** `tests/test_audio_validation_generated.py`,
`tests/test_util.py`, `tests/test_integration_slices.py`,
`docs/generated-testing.md`.

- Generate a clean FLAC in test setup with the Nix-provided FFmpeg, locate the
  audio frame region, and mutate bytes without committing binary fixtures.
- Pin a Syro-shaped mid-frame mutation that the old command accepts and the
  strict command rejects.
- Add a bounded Hypothesis property over frame offsets and mutation masks.
- The property drives the real validator subprocess, not a model of it.
- Add a module-level invariant checker plus a known-bad self-test proving that
  the old `rc=0 means clean` observation fails the checker.
- Add arbitrary exit-zero stderr as the metadata-independence property: every
  string passes and produces no persisted diagnostic.
- Add the decision consequence slice: corrupt evidence reaches
  `full_pipeline_decision_from_evidence`, writes the peer denylist, moves the
  exact source under `failed_imports/bad_files`, and returns the request to
  searchable work.

The generated module uses the repo's subprocess profile bounds and is added to
the documented generated-test table/fuzz burst automatically by filename.

### U2 — Introduce the typed validator

**Files:** new quality wire-type module, `lib/quality/__init__.py`,
`lib/util.py`, `lib/measurement.py`, focused tests.

- Replace the mutable `AudioValidationResult` dataclass with a typed result
  union/report that distinguishes content failure from measurement failure.
- Extract one audio-file validator and let the album function own traversal,
  aggregation, caps, and one-line logging.
- Use one shared argv builder so validation tests can pin the exact policy.
- Remove the FLAC unset-MD5 mutation path. An unset checksum passes when audio
  decodes; validation never rewrites source bytes.
- Prove file readability before attributing a failure to content; on ambiguous
  FFmpeg failure, complete one raw read before selecting
  `audio_corrupt` versus `measurement_failed`.
- Keep preview neutral: measurement stores the report/facts; it never writes
  the denylist.

### U3 — Persist validation state end to end

**Files:** `migrations/064_audio_validation_report.sql`,
`lib/quality/evidence_types.py`, `lib/quality_evidence.py`,
`lib/pipeline_db/evidence.py`, `lib/quality/decisions.py`,
`lib/import_preview.py`, `scripts/import_preview_worker.py`,
`tests/fakes/pipeline_db.py`, real-PG/fake/serialization tests.

- Add the non-null report column and truth-preserving migration backfill.
- Thread the report through `PreimportMeasurement`,
  `AlbumQualityEvidence.sorted_for_storage()`, both evidence builders, the
  typed PipelineDB write/read boundary, and the fake.
- Preserve `audio_corrupt`/`audio_error` as report projections used by the
  current full decider; reject incoherent writes before SQL and in PostgreSQL.
- Extend `MeasurementFailure` with an optional validation report for failures
  in Cratedigger's world.
- Ensure both terminal JSONB surfaces preserve every diagnostic field and
  reject wrong wire types with `msgspec.ValidationError`.
- Extend existing CLI/read projections to show the structured cause through
  current commands; do not add a new operator action.

### U4 — Make retention explicit

**Files:** `lib/pipeline_db/download_log.py` or the narrowest existing read
mixin, `lib/slskd_transfers.py`, `tests/fakes/pipeline_db.py`,
`tests/test_disk_reaper_generated.py`, real-PG/fake tests.

- Keep the existing unconditional `failed_imports/` protected subtree.
- Load paths referenced by persisted `measurement_failed` audit rows and add
  only paths within the configured download root to the protected set.
- Missing paths are harmless; outside-root paths grant no deletion authority.
- The protection is permanent until explicit operator cleanup, matching the
  retained audit contract.
- Add a deterministic aged ledger-owned path with request status `wanted` and
  a generated property over ownership, age, location, and failure reference.
- Add a known-bad checker that trips when a referenced path is removed.

### U5 — Harden and audit conversion

**Files:** `harness/import_one.py`,
`lib/quality/import_result_types.py`, conversion/import-result tests.

- Use the same strict audio decoder input options during every lossless
  conversion.
- Replace source metadata copying with fixed
  `-map_metadata -1 -map_chapters -1`; retain MP3 muxer options such as
  `-id3v2_version 3` separately from metadata policy.
- Add `-hide_banner -nostdin -v error` and bounded capture.
- Treat exit `69`, another nonzero status, timeout, missing/empty output, and
  filesystem replacement failure as distinct typed conversion diagnostics.
- Remove raw stderr printing. Emit one bounded summary and carry the detailed
  failure through `ConversionInfo`.
- On ambiguous conversion failure, rerun the shared read-only validator
  against the retained source. Record whether the source itself failed or the
  encoder/materialization failed; do not invent meaning from the conversion
  exit code.
- Delete/replace a source only after success and nonempty output. Preserve the
  source and remove only the temporary output on every failure.

Preview remains the only normal measurement owner. The conversion recheck is a
failure-only safety diagnostic after an operation that was already authorized
by current persisted evidence; it does not create an independent accept path.

### U6 — Documentation, remediation, and live proof

**Files:** `docs/beets-primer.md`, `docs/quality-verification.md`,
`docs/generated-testing.md`, relevant code docstrings.

- Replace stale claims that plain FFmpeg exit zero proves valid audio.
- Document the persisted report, metadata non-policy, denylist/quarantine
  lifecycle, and conversion failure audit.
- During deploy, run a transient one-shot over retained production source:
  current FLAC library material plus `failed_imports/` audio. Do not commit a
  scanner.
- Persist/record newly found bad retained sources through existing audit and
  operator surfaces; do not delete anything automatically.
- Do not scan installed Opus and claim that it proves its deleted FLAC source
  was clean. Those historical rows remain `legacy_unrecorded`.

## Focused validation

During convergence:

```text
nix-shell --run "python3 -m unittest tests.test_util -v"
nix-shell --run "python3 -m unittest tests.test_audio_validation_generated -v"
nix-shell --run "python3 -m unittest tests.test_import_result -v"
nix-shell --run "python3 -m unittest tests.test_import_preview tests.test_import_dispatch -v"
nix-shell --run "python3 -m unittest tests.test_integration_slices -v"
nix-shell --run "python3 -m unittest tests.test_pipeline_db tests.test_fakes -v"
nix-shell --run "python3 -m unittest tests.test_disk_reaper_generated -v"
nix-shell --run "bash scripts/fuzz_burst.sh"
```

Before the first push, after the final tree is reviewed and committed:

```text
nix-shell --run "pyright --threads 4"
nix-shell --run "bash scripts/run_tests.sh"
```

If the full gates expose a defect, fix it with focused checks, commit and
review the changed tree, then restart the final gate sequence once.

## Live verification

After merge and deploy through the repository deploy skill:

1. Verify migration `064` applied and every new evidence row has a non-null
   typed validation report.
2. Verify web, preview worker, importer, and migration units are healthy.
3. Derive the active source from `cratedigger.service`'s exact `ExecStart` and
   observe a naturally scheduled successor cycle on that source.
4. Use a copied/generated corrupt FLAC canary under the controlled processing
   path. Prove:
   - validation records `audio_corrupt`;
   - the importer decider rejects;
   - the peer/source is denylisted;
   - the source lands under `failed_imports/bad_files`;
   - the request resumes searching;
   - the download/evidence row contains the bounded decoder report;
   - journald contains one summary rather than raw multiline stderr.
5. Use a permissions/readability canary. Prove it records
   `measurement_failed`, writes no denylist, and its retained path survives a
   reaper invocation even when old and ledger-owned.
6. Exercise one known-clean album and confirm `passed`, no persisted stderr,
   and ordinary import/conversion behavior.
7. Run the transient retained-source sweep and attach its counts/results to
   issue #835.

The issue stays open until this proof is recorded. The PR references #835
without a closing keyword; close deliberately after live verification.

## Out of scope

- Persisting, classifying, repairing, or displaying malformed metadata,
  pictures, lyrics, chapters, or exit-zero FFmpeg diagnostics.
- `flac -t` as a parallel product validator or an in-validator MD5 repair.
- A committed production rescan/backfill script.
- Claiming old Opus derivatives prove their missing source was clean.
- Changing quality ranks, verified-lossless policy, or the single importer
  decision owner.
- Automatic deletion of anything retained under `failed_imports/` or by a
  `measurement_failed` audit.

## Delivery shape

One PR is appropriate: validator semantics, evidence persistence, conversion
hardening, and lifecycle proof are one data-integrity boundary. Splitting the
migration or conversion audit from the validator would create a deploy window
where corruption is detected but still unauditable, or conversion is
fail-closed without a durable reason.

Use an isolated worktree based on current `origin/main`; preserve the dirty
shared checkout. Commit the plan separately before implementation, keep issue
references non-closing, review the converged implementation as one tree, then
merge with a GitHub merge commit, deploy, prove live, and close #835.
