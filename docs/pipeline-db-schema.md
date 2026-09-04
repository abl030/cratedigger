# Pipeline DB Schema (key fields + JSONB audit blobs)

The pipeline DB is PostgreSQL. DSN: `10.20.0.11:5432/cratedigger`. Access via `pipeline-cli` on doc2, or from doc1 via `ssh doc2 'pipeline-cli ...'`.

Full schema lives in `migrations/*.sql`. This doc covers the fields that appear in debugging and the JSONB audit blobs.

## `album_requests` — scheduler priority

- `created_at TIMESTAMPTZ NOT NULL` is the immutable creation audit and starts
  the ordinary 24-hour new-request window.
- `priority_started_at TIMESTAMPTZ NULL` starts the same 24-hour scheduler
  window for an explicit urgent operator action. Bad Rip is its sole producer:
  it requeues the existing exact-release request without falsifying
  `created_at`. Ordinary/manual requeues leave the field unchanged and do not
  start a new window.

Both timestamps affect only cohort allocation after the normal status, retry,
active-plan, conflicting-work, and title gates pass. They never bypass backoff.

## `album_quality_evidence` — active quality evidence

Active reusable album-quality evidence is stored relationally, not in JSONB.
Evidence is **content-addressed**: identity is `(mb_release_id,
snapshot_fingerprint)`. Addressing entities reference evidence rows via FK
columns (`import_jobs.candidate_evidence_id`,
`download_log.candidate_evidence_id`, `album_requests.current_evidence_id`).
The same audio collapses into one canonical row regardless of how many
addressing entities point at it; differing file inventories produce
different fingerprints under the same release id.

Key fields:

- `mb_release_id TEXT NOT NULL` — the MusicBrainz release this evidence
  describes.
- `snapshot_fingerprint TEXT NOT NULL` — SHA-256 over the per-file tuple
  `(relative_path, size_bytes, extension, container, codec)`, sorted by
  `relative_path`, JSON-encoded with stable key order. Computed by
  `lib.quality_evidence.snapshot_fingerprint`.
- `source_path TEXT NOT NULL` — the on-disk root where measurement
  happened.
- `UNIQUE (mb_release_id, snapshot_fingerprint)` plus
  `INDEX (mb_release_id)` for prefix lookups.
- `measured_at TIMESTAMPTZ` — when this evidence snapshot was measured.
- `codec`, `container`, `storage_format` — measured source/storage facts.
  For lineage-v3/v4 rows, `storage_format` is the same bare codec label as
  `format`; bitrate/profile labels never live in either field.
- `target_format` — projected target policy from the typed import contract,
  independent of the measured source. It may be NULL.
- `target_is_cbr` — album-wide bitrate mode measured from the projected
  target/probe files. It belongs to the target contract, independently of
  both the downloaded source and materialized output measurements. A bare
  `MP3` target is incomplete without this explicit mode. Measurement-only
  rows for facts rejected before target policy is consulted leave both
  `target_format` and `target_is_cbr` NULL rather than guessing a mode.
- `lineage_version SMALLINT` — `1` marks historical rows whose storage/target
  projection is ambiguous; `3` marks separated source and target facts; `4`
  adds the two-axis evidence vocabulary; `5` re-derives the MP3 rank against
  one band table instead of two selected by an inferred mode (issue #1145).
  4 and 5
  share the v4 fact vocabulary, which is why every version-gated shape CHECK
  and every `lineage_version < 4` merge predicate stays at 4 — those mean
  "predates the two-axis vocabulary", not "predates the current version".
  Migration 078 changes the default to 5 and every typed writer persists
  `lib.quality.CURRENT_EVIDENCE_LINEAGE_VERSION` explicitly. Older rows
  rebuild on their next policy touch instead of being interpreted as current.
- `min_bitrate_kbps`, `avg_bitrate_kbps`, `median_bitrate_kbps`, `format`,
  `is_cbr`, `spectral_grade`, `spectral_bitrate_kbps`, `spectral_subject`,
  `spectral_provenance`, `was_converted_from` — the wrapped
  `AudioQualityMeasurement` facts. The measurement has no verified-lossless
  boolean; an observation about bytes cannot assert acquisition completion.
- `cliff_hz`, `codec_family`, `ultrasonic_deficit_db`,
  `spectral_measurement_version` — issue #829 Phase 5 PR1 measured facts
  captured in the SAME pass as `spectral_grade` above (one atomic fact,
  eight columns wide — a stale writer without a grade preserves all eight
  together, and none of the four may be non-NULL without a grade).
  `cliff_hz` is the raw in-window cliff frequency (Hz) `detect_cliff()`
  returns — `spectral_bitrate_kbps` is only its bucketed interpretation.
  `codec_family` is `mp3` / `aac` / `opus` / `vorbis` / `lossless` / `other`
  (CHECK-constrained), resolved from the real probed codec for the two
  containers extension cannot disambiguate (`.ogg`, `.m4a`).
  `ultrasonic_deficit_db` is the level-invariant ultrasonic deficit
  (`ref_db(1-4kHz) - mean(20.5-22kHz slices)`, averaged across tracks) —
  PR3's proof-leg statistic; not read by any decision yet.
  `spectral_measurement_version` is `2` for rows measured by the PR1+
  `lib/spectral_check.py` code; NULL marks a legacy generation. A grade is
  reusable only when this value exactly matches the running analyzer, except
  for a recognised source-subject grade whose lossless source is provably
  preserved rather than regenerable. Exact candidate and ordinary installed
  snapshots are remeasured on their next preview; an old source-subject fact
  from a lossless original remains policy-usable with its original generation
  because scanning the installed derivative would describe different bytes
  (R19). This is on-touch convergence, not a committed backfill.
- `audio_validation JSONB NOT NULL` — the bounded typed report from the
  audio-only strict FFmpeg policy. New reports are `passed`,
  `audio_corrupt`, or `skipped`; the migration uses `legacy_failure` for
  historical rows already known corrupt and `legacy_unrecorded` everywhere
  else. It never fabricates a historical pass. The report stores counts,
  FFmpeg version, at most 16 per-file diagnostics, and at most 2 KiB of
  normalized stderr per diagnostic with original byte count/hash/truncation.
  Metadata, pictures, tags, chapters, and exit-zero stderr are not persisted.
- `audio_corrupt BOOLEAN`, `audio_error TEXT`, `folder_layout TEXT` (`flat` | `nested`),
  `audio_file_count INTEGER`, `filetype_band TEXT`,
  `matched_bad_audio_hash_id`, `matched_bad_audio_hash_path` — the four
  folder/audio-integrity facts the importer's
  `full_pipeline_decision_from_evidence` reads as early-exit reject
  branches (U11).
  `audio_corrupt` is a query/decision projection constrained to agree with
  `audio_validation`; `audio_error` is its bounded album-level summary.
  `album_quality_evidence_files.decode_ok` identifies the exact individual
  files, using relative paths rather than basename guesses.
- `v0_min_bitrate_kbps`, `v0_avg_bitrate_kbps`,
  `v0_median_bitrate_kbps`, `v0_subject`, `v0_provenance` — one neutral V0
  metric plus its two-axis markers. Legacy policy-shaped probe kinds are
  rejected here.
- `on_disk_v0_research_attempted BOOLEAN` — monotonic once-only claim that the
  exact content-addressed installed snapshot has already had its neutral V0
  research opportunity, including a probe that produced no metric.
- `current_enrichment_required BOOLEAN` — monotonic marker set when a changed
  installed snapshot is linked before its exact spectral/V0 enrichment has
  completed. Action loaders require the row's neutral enrichment facts while
  this marker is true, so an unchanged retry cannot turn a newly linked but
  unenriched row into authority. Source-subject facts may survive the rebuild;
  missing installed-subject facts must be measured anew. Same-address upserts
  combine the marker with logical OR and cannot clear it.
- `verified_lossless BOOLEAN` plus `verified_lossless_provenance`,
  `verified_lossless_source`, `verified_lossless_classifier`,
  `verified_lossless_detail` — the proof object is the sole writable owner of
  verified-lossless. SQL derives the convenience boolean from proof presence,
  and a CHECK requires the boolean and complete proof tuple to agree.

### Derived provisional-lossless convergence

Migration 071 adds the request-local
`derive_request_convergence_signal(request_id)` SQL function, its partial
candidate index, and `download_log.candidate_evidence_direct`. The boolean is
true only when a terminal producer positively linked the evidence measured for
that exact attempt. Migration-021 sibling cross-walks stay false unless the
historical job/log pair is uniquely attributable by request, evidence, and its
transaction-stable terminal timestamp. The companion
`candidate_contributor_usernames TEXT[]` stores normalized structured
Soulseek identities. New rows receive it from the real download-file set;
historical display strings containing commas are ambiguous and remain
ineligible rather than being split. Ambiguous history fails closed.

The function derives an operator signal from evidence and download history; it
persists no signal, policy flag, counter, or cadence state. A signal exists
only while the request's linked current evidence
is canonical provisional lossless (`v0_subject='source'`, unverified) and the
newest consecutive eligible Soulseek run has at least five qualifying candidate
observations and at least five distinct atomic peer usernames in one
nearest-500 Hz raw-cliff band. Contributor arrays are unnested as structured
identities; presentation strings are never parsed. Eligible observations require positive
direct attribution, an exact-release `strong_match` at distance <= 0.15, and
unverified lossless-codec, source-subject, measurement-v2 evidence. Ineligible
legacy cross-walks, world errors, non-exact matches, and high-distance rows are
ignored. An eligible NULL/no-cliff row or a different cliff band is an upward
break that resets the current run. The signal carries distinct codec and
snapshot counts plus the raw minimum, maximum, and spread; sharing a rounded
band is never described as identical raw cliffs.

This is constancy, not proof. It never changes search cadence automatically.
The explicit stop action sends the signal's opaque SHA-256 token. One static
PostgreSQL statement rederives and compares every visible fact and qualifying
attempt identity, plus the exact linked current-evidence identity, in one MVCC
snapshot while changing only
`wanted -> unsearchable`. A committed evidence/log writer before that snapshot
changes the token and loses the CAS. If a current-evidence writer holds the
request row while stop waits, the final `UPDATE` also compares the derived
current-evidence ID against the newer target-row version, so the stop loses the
CAS instead of updating stale authority. A non-conflicting later writer
linearizes after the operator decision. This is PostgreSQL atomicity only, not a
cross-system claim. Evidence remains provisional and untouched; the ordinary
Resume transition reopens searching.

Every v4 spectral or V0 fact answers the same two questions: `subject` says
which bytes it describes (`installed` or `source`), and `provenance` says how
it reached this row (`measured` or `carried`). `installed` + `carried` is
invalid: installed bytes are re-measured. When a fingerprint changes, the
canonical acquisition-fact set defined in
[`quality-verification.md`](quality-verification.md#evidence-addressing-propagation-and-ownership)
may cross to the new row with provenance `carried`; installed-subject facts do
not. Carry decisions use these markers directly and never infer lineage from
codec names.

Migration 055 maps the old vocabulary mechanically before v4 rebuilds take
over: `v0_source_lineage` becomes `v0_subject` (`lossless_source` → `source`,
research/unknown values → `installed`); `v0_source_provenance` becomes
`v0_provenance` (probe-kind and measurement-fallback values → `measured`);
`verified_lossless_proof_origin` becomes `verified_lossless_provenance`
(`import_result` → `measured`, `legacy_request_seed` → `carried`); and the
redundant `v0_proof_provenance` column is dropped.

Migration 072 briefly narrowed R19 with a v4 database CHECK. Migration 073
retires it: `was_converted_from` is durable output lineage and can coexist
with a fresh `installed` spectral measurement. A HAVE scan changes the
spectral subject, not the fact that FLAC became ALAC, Opus, or Vorbis.
Current-evidence rebuilds preserve that lineage only for the same installed
snapshot, and the current-only spectral writer leaves it untouched. A fresh
candidate measurement with NULL lineage clears legacy candidate contamination
at the same content address unless the canonical row is also current-linked.
For that shared-row case, storage retains the installed history while every
candidate projection uses `was_converted_from=NULL` — action-time policy,
`pipeline-cli quality`, the decision differential, Recents, and Wrong Matches
all see the same source semantics, so output lineage never becomes a source
measurement.
Source-subject V0 and verified-lossless proof remain provenance rather than a
database claim that current bytes are irrecoverable. The application preserves
old source-subject spectral only when the exact manifest also proves a known
lossy installed codec; it fails closed for native lossless, mixed, or
unresolved media, including ALAC's normal `.m4a` container.

## `album_quality_evidence_files` — snapshot guard rows

Each active evidence row owns typed file-snapshot rows:

- `evidence_id BIGINT` — FK to `album_quality_evidence(id) ON DELETE CASCADE`.
- `ordinal INTEGER` and `relative_path TEXT` — stable sorted snapshot order.
- `size_bytes BIGINT`, `mtime_ns BIGINT`, `extension TEXT`,
  `container TEXT`, `codec TEXT` — file identity and container facts used to
  decide whether cached evidence is still valid.

Action provenance such as reused/recomputed/backfilled/fallback outcomes is not
stored in these evidence tables; preview/import/cleanup result surfaces own
that audit trail.

## Library derived facts (not schema)

The Library and browse read seams derive independent facts; there is no
PostgreSQL holdings projection, badge column, migration, or backfill:

- Current presence comes only from a successful live Beets read for the exact
  MusicBrainz or Discogs release identity. A sibling pressing never counts.
- `has_captured_history` is true when the request has a durable successful
  download outcome (`success`, `force_import`, or `manual_import`), a completed
  successful import job (`automation_import`, `force_import`, `youtube_import`,
  or `local_import` — issue #1176 PR1 round 2: a successful local import
  genuinely is a capture, so it confers this exactly as force/youtube imports
  do; `manual_import` is not in this job-type list — that job_type is retired
  entirely by migration 080, unlike the still-live `manual_import` *outcome*
  named above), or the accepted current `status='imported'` legacy
  fallback. Historical witnesses remain true across later status changes. The
  status-only fallback deliberately disappears if the operator explicitly
  reopens that legacy request, until ordinary acquisition writes a witness.
- Current installed quality comes from the successful live Beets read.
  Verified Lossless and provisional proof come from the request's linked
  `current_evidence_id`; evidence never establishes current presence by itself.
- Captured, Missing, Untracked, and Replaced are presentation facts, not
  request statuses. A retagged sibling therefore leaves the old exact request
  Captured plus Missing and renders the newly held exact identity Untracked.

A failed Beets read is a broken read boundary: the API logs and returns an
error without changing `album_requests` and without fabricating Missing or
Untracked rows.

## `album_requests` — quality-tracking fields

- `status TEXT` — `initializing` is a provisional, non-runnable direct-creation
  state. Its publication to `wanted` is a service-owned compare-and-set after
  canonical tracks, field-resolution audit, and an initial plan outcome are persisted.
  Active vocabulary: `wanted`, `downloading`, `processing`, `imported`,
  `unsearchable`; terminal audit vocabulary: `replaced`. `processing` is the
  private processor-owned interval between an exact download-incarnation
  handoff and one terminal automation outcome. `unsearchable` is an explicit
  operator-owned search stop and is independent of source cleanup.
  Ordinary transitions are fail-closed and use SQL compare-and-set against the
  exact observed/declared source status. `replaced` has no outgoing edge and is
  created only by the one-way `supersede_request_mbid` transaction.

  The explicit operator transition graph still has 11 edges (none out of
  `initializing`): `wanted → downloading/unsearchable/imported/wanted`; `downloading → wanted/imported`; `imported →
  wanted/imported`; and `unsearchable → wanted/imported/unsearchable`.
  `downloading → unsearchable` cannot abandon an active transfer, and
  `imported → unsearchable` cannot retroactively stop a completed request.
  Status-only self-transitions for `wanted`, `imported`, and `unsearchable` are
  true no-ops: they do not change `updated_at` or any other byte. There is no
  `downloading → downloading` edge because acquiring download ownership must
  remain an explicit compare-and-set operation.

  `downloading → processing` is not part of that generic graph. The
  automation handoff owns it and atomically attaches the new job; exact-owner
  terminal commands privately perform `processing → wanted|imported`.
  A proven-dead world failure records audit evidence and performs
  `processing → wanted` without replacing the owner job. Generic lifecycle,
  intent, quality, replacement, force-import, ban/delete, and library-delete
  actions return `processing_locked` instead of mutating an owned request.

  Once a row becomes `replaced`, its lifecycle, retry counters, scheduler
  fields, active download metadata, evidence pointer, and active search-plan
  pointer are frozen. Late workers use exact-status compare-and-set writes and
  stop when ownership has changed. A completed search may still append a stale
  forensic `search_log` row (`stale_reason='request_replaced'`), but it cannot
  advance the ancestor cursor or backoff. Search-plan generation, supersession,
  and manual cursor advance reject the replaced ancestor.
- `search_filetype_override TEXT` — transient CSV filetype list (e.g. `"lossless,mp3 v0,mp3 320"` or just `"lossless"`). Overrides global `allowed_filetypes` for search. The post-import policy — identical for automatic and force-import callers (decision 19) — writes `"lossless"` only for a transparent, spectrally genuine copy (decision 17: the grade's subject label does not gate narrowing) and for the provisional lossless-source lane; other unverified retained copies return to the full search surface. Only a proof-bearing copy completes acquisition. The `"lossless"` virtual tier matches FLAC, ALAC, and WAV.
- `target_format TEXT` — persistent user intent for desired format on disk (`"lossless"` or NULL). Set only by user action (CLI/web set-intent toggle). Never cleared by quality gate. When set, keeps lossless on disk (normalizes ALAC/WAV → FLAC) instead of converting to V0/target.
- `marked_incomplete_at TIMESTAMPTZ` — operator's incomplete mark (issue
  #1241, migration 082). NULL = unmarked. Set/cleared only by
  `pipeline-cli mark-incomplete` / `POST /api/pipeline/mark-incomplete` /
  the dashboard's Library Completeness card — never by measurement — and
  cleared automatically by a terminal import acceptance whose candidate
  beets proved whole. While set, the quality decider disregards the
  installed side for any beets-whole candidate (fresh-import admission);
  see `docs/quality-verification.md` § "Operator incomplete mark".
- `min_bitrate INTEGER` — current min track bitrate in kbps (from beets).
- `prev_min_bitrate INTEGER` — previous min_bitrate before last upgrade. Shows delta in UI.
- `verified_lossless BOOLEAN` — historical request stamp written at the import event. Active terminal authority belongs to the complete verified-lossless proof on the linked evidence row. Proof requires affirmative spectral `genuine`/`marginal` evidence, or the explicit V0 trust override after spectral disagreement; absent or errored analysis never verifies. Suspect lossless-container imports stay false when accepted provisionally.
- `last_download_spectral_grade TEXT` — spectral grade of the most recent download attempt.
- `last_download_spectral_bitrate INTEGER` — estimated bitrate from the most recent download's spectral analysis.
- `current_spectral_grade TEXT`, `current_spectral_bitrate INTEGER` — point-in-time request stamps for history/rendering. Active decisions read the linked evidence row's atomic spectral fact; these scalars never seed or override evidence.
- `current_lossless_source_v0_probe_min_bitrate INTEGER`, `current_lossless_source_v0_probe_avg_bitrate INTEGER`, `current_lossless_source_v0_probe_median_bitrate INTEGER` — point-in-time request stamps for history/rendering. The comparable source anchor used by policy is the linked evidence row's `v0_metric` with `subject='source'`.
- `active_download_state JSONB` — persisted download state for async polling
  (filetype, `enqueued_at`, `attempt_fingerprint`, per-file
  username/filename/size). Set by `set_downloading()`, cleared on
  completion/timeout. While the request remains `downloading`, the exact
  stored `enqueued_at` text is the immutable incarnation witness for
  whole-state rewrites: initial enqueue ownership, slskd event stamping,
  pre-purge terminal-evidence harvest, and poll reduction update only when
  status is still `downloading`, the stored witness equals the caller's
  expected text, and the outgoing state retains that same text. Paths and
  transfer keys are evidence within an attempt, never its identity. Event
  classification additionally requires a parseable current witness and a
  completion occurrence at or after it; post-event polling admits only exact
  `(request_id, enqueued_at)` pairs captured before the transfer snapshot.
  `attempt_fingerprint` (issue #1196) is a SEPARATE, orthogonal identity: a
  deterministic digest of this attempt's whole (username, filename) transfer
  key set, written once at claim time
  (`lib.download.build_active_download_state`) and carried unchanged across
  every poll-cycle rewrite. `enqueued_at` remains the sole incarnation
  witness governing whole-state CAS writes (unchanged by this field);
  `attempt_fingerprint` instead answers "is this the SAME attempt" for the
  cross-request enqueue guard's ledger join
  (`PipelineDB.get_conflicting_transfer_request_ids`, below) by exact
  transfer-key identity rather than a clock comparison. Optional and omitted
  from JSON when unset (`omit_defaults=True`) — historical rows and an
  empty-manifest attempt both decode with it `None`.
  The witnessed handoff preserves this JSONB as immutable
  attempt/manifest provenance while entering `processing`. It stamps the
  canonical path and `processing_started_at` before the transaction commits,
  but neither path nor timestamp is authority. Poll/event/timeout code stops
  at that boundary.
- `active_automation_import_job_id INTEGER` — exact processor owner. A deferred
  unique/FK/constraint-trigger bundle requires this pointer iff
  `status='processing'`, requires the named row to belong to this request, and
  requires that row to be an active `automation_import` job (`queued`,
  `running`, or historical `recovery_required`). Historical terminal jobs are
  never adopted. New world failures do not enter `recovery_required`; they
  record failure evidence and return the request to `wanted`. The owner pointer
  and `active_download_state` are cleared together only by the exact-owner
  terminal transaction.

## `download_log` — quality-tracking fields

- `slskd_filetype TEXT` — the captured source filetype (`"flac"`, `"mp3"`)
  used to build the downloaded-quality label. Peer-advertised bitrate is not
  stored; quality decisions and displays use measured evidence.
- `actual_filetype TEXT` — what's on disk after download/conversion.
- `spectral_grade TEXT` — spectral analysis of the downloaded files.
- `spectral_bitrate INTEGER` — estimated original bitrate from spectral.
- `existing_min_bitrate INTEGER` — beets min bitrate before this download.
- `existing_spectral_bitrate INTEGER` — spectral estimate of existing files before download.
- `v0_probe_kind TEXT` — lineage for this attempt's optional V0 probe evidence. V0 probes run on every candidate and are operator-facing across the UI (Recents strip/detail, Wrong Matches; research kinds render qualified — "(from lossy)" / "(on-disk re-encode)"). Only `lossless_source_v0` is comparable for the provisional-lossless policy lane; `native_lossy_research_v0` and `on_disk_research_v0` are real V0-transcode research measurements excluded from that lane.
- `v0_probe_min_bitrate INTEGER`, `v0_probe_avg_bitrate INTEGER`, `v0_probe_median_bitrate INTEGER` — min/avg/median track bitrates for this attempt's probe.
- `existing_v0_probe_kind TEXT` — lineage of the comparable probe state used before this attempt, when present.
- `existing_v0_probe_min_bitrate INTEGER`, `existing_v0_probe_avg_bitrate INTEGER`, `existing_v0_probe_median_bitrate INTEGER` — point-in-time baseline probe values used for history rendering and audit.
- `candidate_evidence_direct BOOLEAN NOT NULL DEFAULT FALSE` — positive
  attribution that `candidate_evidence_id` describes this exact attempt's
  bytes. Historical sibling cross-walks remain addressable for rendering but
  are excluded from convergence.
- `candidate_contributor_usernames TEXT[]` — normalized structured Soulseek
  identities captured from the attempt's real file manifest. Commas and other
  punctuation remain part of one array element. Ambiguous historical
  comma-joined presentation text is left NULL and excluded.
- `outcome TEXT` — CHECK-constrained vocabulary: `success`, `rejected`, `failed`, `timeout`, `force_import`, historical `manual_import`, `curator_ban`, `measurement_failed`, `user_offline`, `have_analysis_error`, `youtube_running`, `youtube_success`, `youtube_failed`. New manual imports cannot be submitted; the value remains readable for existing audit rows. `measurement_failed` is a candidate-preview environment failure; `have_analysis_error` is a failed fresh analysis of the installed HAVE. Automation returns to `wanted`; operator jobs preserve their current lifecycle state. Neither failure mints a quality verdict, denylist entry, or narrowing decision.
- A failed force-import or YouTube-import execution writes one linked
  `outcome='failed'` row (`source_download_log_id` points to the action's
  original force/YouTube row) in the same transaction that terminalizes its
  `import_jobs` row. The terminal row derives `source` from that exact origin
  in the same INSERT. A missing or cross-request origin is refused as a link,
  but still terminalizes atomically with an unlinked audit whose source is
  derived from the exact job type and whose bounded diagnostic names the
  provenance refusal; it never parks a running job. Thus a YouTube attempt
  never becomes a misleading slskd row. Its bounded,
  prefix-inclusive diagnostic explicitly names the failed attempt so Recents
  remains the durable operator surface. This non-owning action never
  transitions the request: `wanted`, an explicit `unsearchable` stop, and
  terminal `imported` remain exactly as they were.
- `source TEXT NOT NULL DEFAULT 'slskd'` — sourcing-channel discriminator added by migration 037. CHECK constraint admits `'slskd'`, `'youtube'`, and (migration 080, issue #1176 PR1) `'local'`. The default backfilled every pre-037 row to `'slskd'` in one ALTER (no separate backfill script per the single-operator no-backfill-script rule). Consumers rendering `download_log` rows (`pipeline-cli show`, web routes' "recent attempts") use this column to distinguish channels. Two writers can reach the operator-visible terminal row: `_insert_terminal_download_audit`'s job_type-derived SQL CASE (`lib/pipeline_db/terminal_outcomes.py`) derives `'local'` for a `local_import` job's terminal outcome; and `_insert_nonjob_download_audit` (`lib/pipeline_db/terminal_outcomes.py`) is a separate job-less INSERT that both `PipelineDB.persist_request_success_outcome` (issue #1355 item A1) and `PipelineDB.persist_request_rejection_outcome` (issue #1355 item 3) call directly, inside their own transaction, so a job-less acceptance's or rejection's audit row commits atomically alongside its request transition and any denylist/cooldown writes instead of as a separate autocommit call (`_do_mark_done`'s job-less branch used to call `log_download` directly as a second, separate statement — issue #1355 item A1 closed that gap) — it relies on the same schema `'slskd'` default rather than passing `source=` explicitly. The import-local lane (issue #1176 PR3, `lib/local_import_service.py` → `pipeline-cli import-local` / `POST /api/pipeline/import-local`) is the sole writer that reaches the `'local'` value in production — every dispatch for a `local_import` job carries `import_job_id`, so it always goes through the SQL CASE path, never a job-less writer.
- `youtube_metadata JSONB` — YT-specific audit payload added by migration 037. Nullable; populated only for `source='youtube'` rows. Typed at the read seam as `lib.youtube_ingest_service.YoutubeIngestMetadata: msgspec.Struct`. Carries `yt_url`, `browse_id`, `audio_playlist_id`, optional `expected_track_count`, `resolver_mapping_id`, `per_track_video_ids`, and terminal-state fields (`reason`, `stderr_excerpt`, `observed_track_count`).
- **Partial unique index `one_youtube_running_per_request` ON `download_log (request_id) WHERE source = 'youtube' AND outcome = 'youtube_running'`** — added by migration 037. Enforces idempotency at the DB layer: at most one in-flight YT rescue per `request_id` at any time. Application-level pre-insert checks would race; this index is atomic. Once the row transitions to a terminal `youtube_success` / `youtube_failed`, the index admits the next submission.

Historical interrupted request auto-import cleanup rows use
`outcome='failed'` with `beets_scenario='abandoned_auto_import'` and a readable
`error_message`. Current exact-owner processing no longer writes this scenario;
the old rows remain readable interruption evidence rather than source
rejections, denylist decisions, wrong matches, or bad-audio evidence.
`abandoned_auto_import` is registered HISTORICAL (no current writer) in
`tests/test_wrong_match_scenario_producer_audit.py`, whose only reader is a
`WHERE … <> 'abandoned_auto_import'` exclusion at `lib/pipeline_db/misc.py:262`.

Audio-integrity failures split at the evidence boundary:

- Stable readable bytes that the strict audio-only FFmpeg policy cannot fully
  decode become `audio_corrupt` content evidence. The importer remains the
  only decision owner: it rejects through
  `full_pipeline_decision_from_evidence`, deny-lists the source peer, and —
  since issue #1077 (D3) — **deletes the source outright**. A bad rip has no
  salvage value for operator review, so it is never quarantined: for an
  exact-owner request import, the automation processing cleanup's plan-free
  default (`canonical_source_cleanup_intent`) removes the whole owned
  canonical tree in place, journaled exactly like any other disposable
  auto-import source; a force-import failure on this decision deletes the
  ORIGINAL Wrong Matches source via the same helper the cleanup reducer uses
  (`lib.wrong_matches.cleanup_wrong_match_source`), not the disposable
  private action copy dispatch itself touches. No `failed_path` is ever
  recorded, so the row never appears in the Wrong Matches worklist. The
  historical `failed_imports/bad_files/` quarantine cohort from before this
  fix remains readable and disk-reaper-protected
  (`get_retained_failure_paths`'s `post_commit_quarantine` audit-key check),
  and `wrong_match_row_is_visible` still excludes those rows by
  `terminal_import_decision` alone — the decision that actually rejected
  the candidate, not an incidental `audio_corrupt` flag on linked
  candidate evidence attached to something else that rejected for an
  unrelated reason (issue #1077, F2: that evidence-flag clause used to
  ALSO hide a kept row rejected for a different scenario, and was
  removed). See `docs/rejection-routing.md` for the full routing table.
- Permissions, changed/vanished paths, unavailable/interrupted FFmpeg, and
  persistence failures are `measurement_failed`. Their typed report lives in
  the preview/job validation payload, they never write a denylist, and the
  persisted `staged_path` is an unconditional disk-reaper protection. The
  reaper aborts the whole sweep if it cannot load this retention projection.

## `import_jobs` — shared importer queue

All beets-mutating import work is submitted to `import_jobs` and drained by
`cratedigger-importer`. Web/CLI force-import, automation completed-download
processing, and YouTube rescue all share this table.

Key fields:

- `job_type TEXT` — active values are `force_import`, `automation_import`, `youtube_import`, and (issue #1176 PR3) `local_import` — `lib/local_import_service.py::enqueue_local_import` is the sole writer, reached through `pipeline-cli import-local` / `POST /api/pipeline/import-local`. It shares `dispatch_import_from_db` with force-import (`lib/dispatch/entry_points.py`), differing in three deliberate ways: `source_reference_path=None` so every audit row it writes (Wrong Matches `failed_path` included) names the disposable private action copy, never the operator's real folder; validation runs at the ordinary automation `beets_distance_threshold`, not `FORCE_IMPORT_DISTANCE_THRESHOLD` — a candidate that fails lands as an ordinary Wrong Matches row instead of importing despite the verdict; and its own attempt scenario (`"local_import"`, deliberately excluded from `FORCE_IMPORT_SCENARIOS` so its action copy is cleaned up on every outcome that reaches `dispatch_import_core` — accept, or a reject in the evidence pipeline — not only success; the strict-validation guard's own reject never reaches `dispatch_import_core`, so that specific cleanup path is never consulted for it, but it is still a terminal failure bundle, so the importer's shared terminal cleanup (`_cleanup_terminal_force_action`) DOES run for it — it just finds nothing, because relocating the action copy into `wrong_matches/` always runs FIRST, synchronously, before the terminal bundle is ever persisted, so the resulting Wrong Matches row is force-importable; see `docs/rejection-routing.md`). It never takes the `processing` pointer (mirrors force). `manual_import` is retired entirely: it was the job_type for `post_manual_import`, an HTTP endpoint named in security finding CD-SEC-03 (whose remediation is unrelated — CD-SEC-03 was fixed via `post_import_preview`'s path-authority work) and removed by the issue #737 mode-blind refactor (commit `0a6314ec`, 2026-07-18). It carried zero live rows, and migration 080 drops it from the CHECK (it is no longer even historically enqueueable — contrast with the `manual_import` *outcome* on `download_log`, a separate taxonomy on a separate column that is unaffected and still carries live audit rows).
- `status TEXT` — `queued`, `running`, `recovery_required`, `completed`, or
  `failed`. `recovery_required` remains readable for historical rows whose
  Beets launch was durably authorized but no terminal acknowledgement
  committed. No current writer creates that status; startup convergence closes
  a positively dead historical row through the same fail-to-`wanted` terminal
  bundle used for current abandoned executions.
- Startup handles current non-automation `running` rows by launch authority:
  unlaunched force/YouTube jobs safely return to `queued`; launch-authorized
  jobs never replay Beets and instead use the linked failed-audit terminal
  command above. There is no backfill or compatibility recovery path for old
  non-automation `recovery_required` rows.
- `request_id INTEGER` — the related `album_requests.id`.
- `dedupe_key TEXT` — active queue dedupe key. A partial unique index prevents
  duplicate queued/running/historical-recovery jobs while allowing a later job
  after terminal convergence or ordinary completion.
- `payload JSONB` — typed job input. `ImportJob.from_row` decodes it once into
  the strict Struct selected by `job_type`, rejecting unknown fields and wrong
  types before any database mutation and again when a row is projected. Force
  jobs require a positive `download_log_id` and nonempty `failed_path`, with
  optional `source_username` and `source_dirs`; automation jobs carry no
  fields; YouTube jobs require positive `request_id`/`download_log_id` values
  and nonempty `staged_path`/`browse_id` values; `local_import` jobs require a
  positive `request_id` and a nonempty `source_path` — no `download_log_id`,
  since a local import has no originating `download_log` row.
- `result JSONB`, `message`, `error` — terminal worker result visible to web
  and CLI callers. Result and preview-result display/audit data remain broadly
  decoded; `preview_status` continues to accept historical/raw
  `would_import`/`uncertain` display/audit values. Neither is an input-payload
  contract.
- **Partial unique index `one_active_youtube_import_per_request` ON
  `import_jobs (request_id) WHERE job_type = 'youtube_import' AND status IN
  ('queued', 'running', 'recovery_required')`** — added by migration 038 and
  widened by migration 060. Keeps the post-yt-dlp
  importer handoff request-scoped, so a second browse id cannot enqueue a
  parallel active YouTube import for the same request.
- **Partial unique index `one_active_local_import_per_request` ON
  `import_jobs (request_id) WHERE job_type = 'local_import' AND status IN
  ('queued', 'running', 'recovery_required')`** — added by migration 080
  (issue #1176 PR1), mirroring the YouTube index above. A local import has no
  originating `download_log` row to dedupe against (unlike `force_import`/
  `youtube_import`, which key their dedupe string on a download_log id), so
  the request is the only natural grain for both the dedupe key
  (`local_import_dedupe_key`) and this index.
- `attempts`, `worker_id`, `started_at`, `heartbeat_at`, `completed_at` —
  claim and recovery metadata.
- `execution_invocation_id`, `execution_host_boot_id`,
  `execution_systemd_unit`, `execution_worker_pid`, and
  `execution_worker_start_ticks` — complete persisted identity of the execution
  that currently claimed an automation owner. The optional
  `execution_beets_pid`/`execution_beets_start_ticks` pair records the exact
  authorized child. These fields support positive live/dead/unknown evidence;
  they never grant ownership. Same-boot death requires absence of the exact
  unit/invocation, PID/start-ticks identities, and cgroup. Probe errors or
  contradictory facts are unknown and do not requeue.
- `expected_request_status` — the request status captured atomically when the
  job is enqueued. Launch authorization compares the live request row with
  this stored precondition; the importer cannot make the check tautological by
  rereading the live value immediately before launch.
- `beets_launch_authorized_at`, `beets_launch_release_id`,
  `beets_launch_source_path`, `beets_launch_request_status`, and
  `beets_launch_snapshot_fingerprint` — the exact release/request/source
  authority atomically recorded immediately before the Beets subprocess may
  start. These are evidence for refusing an ambiguous replay, not evidence
  that Beets did or did not finish. Issue #1089: when an abandoned owner is
  recovered, `beets_launch_release_id`/`beets_launch_source_path` are also
  the precondition pair `lib.automation_recovery_debris.remove_recovery_debris`
  reads to find and remove a killed Beets child's own crash debris (a
  committed catalog row whose items never left this exact source path) —
  the same two columns, a second reader, no new write.
- `preview_status TEXT` — async readiness/audit stage: `waiting`, `running`,
  `evidence_ready`, legacy `would_import`, `confident_reject`, `uncertain`,
  `measurement_failed`, or `error`. `evidence_ready` means candidate evidence
  exists for the final action-time check; it is not import authority. Only a
  queued `evidence_ready` job is claimable. Typed product enqueue creates
  `waiting`, and the preview worker advances successful measurement to
  `evidence_ready`. The physical column still has its historical
  `would_import` SQL default: raw/default legacy inserts therefore remain
  non-runnable, as do historical terminal `would_import`/`uncertain` values.
  Workers recompute the mutating decision from fresh current evidence plus
  snapshot-valid candidate evidence at import time.
- `preview_result JSONB`, `preview_message`, `preview_error` — durable
  no-mutation preview audit visible in Recents and CLI output. Stored verdicts
  are display/audit facts; they must not authorize import, cleanup, denylist, or
  request-current updates.
- `preview_attempts`, `preview_worker_id`, `preview_started_at`,
  `preview_heartbeat_at`, `preview_completed_at` — async preview claim and
  recovery metadata.
- `importable_at TIMESTAMPTZ` — the candidate scan's sort key (`ORDER BY
  importable_at ASC NULLS LAST`); the serial importer claims only queued
  `evidence_ready` jobs. When it gets set depends on which writer created the
  row, because `mark_import_job_preview_importable` only `COALESCE`s it.
  `enqueue_import_job` and `enqueue_youtube_import_and_mark_success` write it
  NULL at insert, so a force, local or YouTube job takes its
  preview-completion time. `handoff_automation_import` omits it from its
  column list, so migrations 005/018's `DEFAULT NOW()` fires and an automation
  job keeps its **handoff** time instead. Measured 2026-09-02 over rows created
  since 2026-07-31: all 1,350 automation jobs have `importable_at =
  created_at`, and none of the 92 other jobs do. The same omission applies to
  `preview_message` (`DEFAULT 'Preview gate disabled'`) and
  `preview_completed_at` (`DEFAULT NOW()`), so a fresh automation job carries
  both while sitting at `preview_status = 'waiting'`; unlike `importable_at`,
  those two are overwritten once preview finishes.

On importer/preview startup, a pre-existing automation execution changes only
after the shared liveness probe positively proves its persisted execution
dead. A dead pre-launch exact owner can requeue the same job. Once launch was
authorized, a positively dead execution records failed audit evidence, finishes
or truthfully refuses its exact journaled cleanup, fails the job, and returns
the request to `wanted` atomically. Historical `recovery_required` owners take
that same convergence path. Missing or stale heartbeat alone is never death
proof; a live or unknown execution is untouched. The importer also holds the DB
advisory singleton lock, so an accidentally started second worker exits instead
of recovering a live worker's job.

`GET /api/import-jobs/{id}/recovery` and
`pipeline-cli import-job-recovery show JOB_ID` expose one
`AutomationRecoveryDetail`: the exact owner/request/release/path, launch and
lease snapshot, live/dead/unknown transcript, typed completion and exact
library observations, and cleanup state. This surface is read-only diagnostics;
there is no retry/close mutation. Force-import and YouTube jobs whose exact
launched execution dies become terminal `failed` rows rather than entering a
human-gated state; request lifecycle remains owned by their existing flows.

Covered job-backed terminal outcomes cross one DB transaction boundary. This
includes force-import and validated automation dispatch outcomes, automation's
local `Completed` / `CompletionFailed` fallbacks, and request-backed preview
measurement failures. Force/YouTube jobs retain their prior request semantics.
For automation owners, processor cleanup first completes under the still
attached owner; the terminal command verifies that exact owner and consumes
the completed/no-op cleanup receipt while its request transition, owner/state
clear, retry accounting, mandatory `download_log` audit, source
denylist/cooldown writes, and exact job terminalization commit together through
`lib/terminal_outcomes.py`. A request, owner, receipt, or job CAS conflict
rolls the whole bundle back. The terminal commit is the final processor-owned
step; there is no authorized post-terminal cleanup.

Async preview workers run outside the beets mutation lane. They claim queued
jobs with `preview_status='waiting'`, call the no-mutation import preview path,
persist candidate evidence when an owner exists, then either mark the job ready
for the final import-time check or fail the preview with audit details. This
lets spectral/measurement work run with tunable parallelism while beets writes
stay serial, without letting preview decisions become later mutation authority.

The preview/evidence lane is required for new runnable work: typed product
enqueue creates `waiting`, and only the preview worker can advance it to
`evidence_ready`. The retained physical SQL default `would_import` is for raw
or legacy rows only and is non-runnable; it is not a typed enqueue path or
import authority. When workers are disabled, queued work remains safely
non-runnable. Legacy completed/failed rows may carry `would_import` so
historical terminal import history does not look like active preview backlog.
Do not bulk-convert those rows; that would restore preview-decision authority.
The Recents Imports endpoint lists active `queued`, `running`, and historical
`recovery_required` jobs; terminal `completed`/`failed` rows remain durable
audit history and must not be rendered as live queue work.

## `processing_cleanup_journal` — exact-owner filesystem intent

One active automation owner may have one journal row keyed by
`(job_id, request_id)`. Deferred integrity triggers require the journal, job,
and request pointer to describe the same active processing owner.

- `revision` is the monotonic checkpoint CAS.
- `action` is one deterministic processor action: exact source-tree removal,
  exact quarantine rename, or a typed no-op after positive absence proof.
- `source_path`, `source_manifest`, and `source_manifest_hash` freeze the
  authorized source bytes before the first cleanup mutation.
- `destination_path`, destination manifest/hash, and
  `selected_destination_path` freeze quarantine collision selection before
  rename; retries never allocate a different target.
- `step_progress` records idempotent unlink/rmdir/rename progress. Every
  mutation is bracketed by the pinned owner-session cancellation check and a
  journal CAS.
- `completed_receipt` and `completed_at` are both present or both absent. The
  terminal transaction consumes only a typed completed/no-op receipt matching
  the exact owner and pending outcome.

Cleanup locks rows in the global request → request jobs by ID → journals by
job ID order. Only the exact attached owner may advance the journal. If the
current worker cannot complete or truthfully refuse cleanup, it fail-stops with
the owner and journal intact; a later exact death proof resumes automatic
convergence rather than requiring an operator mutation.

## `download_log.import_result` JSONB

`import_one.py` emits an `ImportResult` JSON blob (`__IMPORT_RESULT__` sentinel on stdout). Version 4 contains the downloaded `source_measurement`, the prior `current_measurement`, an optional top-level `verified_lossless_proof`, typed `target_quality_contract`, typed V0 probe evidence, the quality comparison, postflight verification (beets_id, path), the post-import `materialized_measurement`, and an attempt-local `spectral` audit. Measurements describe bytes and cannot carry a verified-lossless boolean; proof presence is the acquisition claim. Source measurements always describe the downloaded bytes; a target such as `opus 128` is policy and V0 min/avg/median remain exclusively under `v0_probe`. `materialized_measurement` describes the bytes Beets actually stored. Historical v1/v2/v3 rows are decoded only by `ImportResult`'s marked legacy projections (`legacy_projection_version`); the v3 projection maps its measurement-level verified-lossless and unmarked spectral facts into explicit v4 proof, subject, and provenance fields. New v4 rows never infer lineage from equality between values. Every import path (success, downgrade, transcode, provisional, suspect-lossless rejection, error, timeout, crash) logs to download_log.

```sql
SELECT import_result->>'decision',
       import_result->'source_measurement'->>'format',
       import_result->'target_quality_contract'->>'format',
       import_result->'comparison_basis'->>'new_metric',
       import_result->'v0_probe'->>'avg_bitrate_kbps',
       import_result->'materialized_measurement'->>'avg_bitrate_kbps',
       import_result->'materialized_measurement'->>'min_bitrate_kbps',
       import_result->'spectral'->>'grade',
       import_result->'spectral'->'per_track'->0->>'hf_deficit_db'
FROM download_log ORDER BY id DESC LIMIT 10;
```

## `download_log.validation_result` JSONB

`beets_validate()` returns a `ValidationResult` with the full candidate list from the harness. Every validation (success or rejection) stores this. Contains: all beets candidates with distance breakdown per component (album, artist, tracks, media, source, year...), full track lists per candidate, the item→track mapping (which local file matched which MB track), local file list, beets recommendation level, soulseek username, download folder, failed_path, denylisted users, corrupt files.

`validation_result.distance` and `validation_result.scenario` are the sole
writer inputs for the denormalized `download_log.beets_distance` and
`download_log.beets_scenario` query columns. `PipelineDB.log_download`
projects them centrally; writers must not pass the same values separately.
Payloads that genuinely omit those envelope keys, such as
`MeasurementFailure`, may supply explicit top-level metadata.

`beets_validate` always names a scenario — exactly one of three (issue #888):

- a `choose_match` was decoded and decided: `strong_match` / `high_distance` /
  `extra_tracks` / `unmapped_audio` / `mbid_not_found`;
- no error was recorded and none was ever offered: `no_choose_match`;
- an error was recorded first — the harness would not start, the strict wire
  decode refused a `choose_match`, the read loop raised, or the 120s timeout
  fired: `validation_error`. This is a separate name on purpose. The
  strict-decode case is one where beets DID offer a match and Cratedigger
  declined to decode it, so folding it into `no_choose_match` would assert the
  opposite of what happened — and its mass trigger is a beets version bump
  changing a field type, i.e. every album at once.

The last two carry a `validation_result.harness_session` audit
(`message_types`, `session_end_seen`, `stderr_tail`): the observation of how
the run ended, plus what the next person needs to work out why.

**`harness_session` is written only by runs that happen after issue #888
shipped.** It is absent on every historical row, including the 276 the #888
backfill named — the backfill could set the scenario honestly but could not
invent evidence for a run that already happened. So it discriminates
*new* rows only; use `beets_scenario` for the cohort itself.

```sql
-- Runs where beets offered nothing to review, with what the harness said.
-- `messages` is NULL on rows named by the backfill rather than by a live run.
SELECT id, created_at, beets_scenario,
       validation_result->'harness_session'->>'message_types'   AS messages,
       validation_result->'harness_session'->>'session_end_seen' AS session_end,
       left(validation_result->'harness_session'->>'stderr_tail', 200) AS stderr
FROM download_log
WHERE beets_scenario IN ('no_choose_match', 'validation_error')
ORDER BY id DESC LIMIT 20;
```

For historical `abandoned_auto_import` audit rows, `validation_result.failed_path`
points at the prefixed failed-import folder when a leftover staged
directory existed. A missing staged directory may produce the same audit
scenario without a `validation_result` body; `error_message` remains the
operator-facing reason.

```sql
-- Why was distance high?
SELECT validation_result->'candidates'->0->'distance_breakdown'
FROM download_log WHERE id = <id>;

-- Which local file matched which MB track?
SELECT m->'item'->>'path', m->'item'->>'title', m->'track'->>'title'
FROM download_log, jsonb_array_elements(validation_result->'candidates'->0->'mapping') AS m
WHERE id = <id>;
```

## `slskd_transfer_ledger` — transfer ownership and file evidence

Migration 045 creates one write-ahead row for every file Cratedigger attempts
to enqueue. The row is intent evidence until slskd accepts the POST; migration
051 adds nullable `accepted_at`, which is stamped immediately after acceptance.
Completion events add file paths only to already-confirmed rows; they never
promote pending intent. A definitively rejected POST therefore cannot gain
destructive authority from a later same-key human completion.

**Acceptance is scoped to the request that made the POST** (issue #1278 item
2). `confirm_transfer_enqueue(username, filename, request_id=...)` promotes
only that request's own newest pending row, and `slskd_enqueue_with_outcome`
skips the confirm entirely when it has no request id — mirroring the
write-ahead INSERT, which skips for the same reason. Unscoped, the UPDATE
took the newest pending row for the key across the whole table, so one
request's acceptance could promote another's rejected or still-in-flight
intent into destructive ownership it never earned. Two requests reaching the
same `(username, filename)` is not hypothetical — it is the collision the
**Cross-request enqueue guard** below exists for. No production caller ever
reached the unscoped path (every enqueue carries an `album_requests` id), so
this closed a latent hazard rather than an incident.

The durable ownership key is `(username, filename)`: slskd assigns a fresh
transfer ID when it retries the same queued file, so an attempt-local ID cannot
prove or disprove ownership of a later terminal record. Every terminal
`Completed,*` record with a confirmed queue key is removed individually using
its current slskd ID; a pending or unledgered key is never touched.

Migration 051 derives historical acceptance from the old positive evidence,
then removes the obsolete `transfer_id` and `completed_at` columns and their
indexes. `local_path` remains separate, authoritative file evidence: only
the completion event feed stamps it, and disk deletion still requires that
event-stamped path or another positive ownership signal. Terminal transfer
cleanup does not infer a filesystem path from the queue key.

Migration 083 states the implication between those two columns as a CHECK:
`local_path IS NULL OR accepted_at IS NOT NULL`. `get_owned_local_paths` --
the disk reaper's ownership set -- selects on `local_path IS NOT NULL`
alone, which was safe only because `stamp_transfer_completion` (today's
only writer of that column) requires acceptance in a different statement.
With the constraint the reaper's query is an accepted-ownership query by
construction, against every writer rather than the audited one. The
violating cohort was empty when it shipped (45,203 rows, 30,511 with a
path, 0 without acceptance), since migration 051's backfill already
covered every historical row.

**Keyed ownership reads.** `get_owned_transfer_keys` answers "every key we
own" for once-per-cycle convergence; `get_owned_transfer_keys_for(keys)`
answers the same question about specific keys. It has two callers of quite
different shape: the destructive paths in `lib/slskd_transfers.py`, which run
on find_download worker threads and ask about one attempt's files; and
`lib/slskd_events.py::ingest_download_file_events`, which destroys nothing —
it decides whether a completion event may stamp a local path onto
`active_download_state` (issue #1278 item 1) — runs once per cycle on Phase
1's own background thread, and asks about every key in its event window, up
to the 10,000-event page cap. Both spell the same `accepted_at IS NOT NULL`
gate,
and neither is request-scoped: an accepted row under any request proves
*Cratedigger* created the transfer, which is the question a shared slskd
poses. "Which request holds this key" is a different question with its own
method, `get_conflicting_transfer_request_ids`, which carries its own status
and attempt scoping. The keyed form zips its input server-side through
`unnest(...)` rather than growing the SQL text — the same fixed-shape pattern
`get_conflicting_transfer_request_ids` uses, and what keeps the events
caller's much larger key list a single bound statement.

**One ingestion pass, one ownership rule.** `lib/slskd_events.py` performs
two writes off the same decoded completion events. `stamp_transfer_completion`
always required an accepted row; `_stamp_local_paths` required only a
`(username, filename)` match plus a time bound, so a foreign client
completing our exact queue key on a shared slskd handed us a `localFilename`
we wrote into `active_download_state` as authoritative — and materialization,
validation and import then treated a stranger's file as the album we
downloaded. Issue #1278 item 1 gates that stamp on the same accepted-POST
rule, per queue key rather than per attempt. Measured on the live ledger:
pending rows still arrive weekly (36 in the seven days to 2026-08-27) while
keys never once accepted stopped appearing in July — so a per-attempt gate
would refuse keys a same-key retry currently recovers, while a per-key gate
excludes only a key we have never owned. The refusal is reported on the cycle
log line as `unowned_completions=`. It closes the never-owned case only: a
foreign completion at a key we *do* own remains indistinguishable on this
evidence, and the processor's source-path validation is the next boundary.

Retention is a strict 90-day cutoff on `enqueued_at`: a row exactly at the
cutoff survives. Older pending rows (`accepted_at IS NULL`) are deleted even
when their request remains `wanted` or `downloading`, because enqueue intent
alone has no ownership value and long-tail requests search forever. Older
accepted rows keep active-request protection; accepted evidence is deleted
only when the request is inactive or hard-deleted.

**Cross-request enqueue guard (issue #1178).** Two concurrent requests for
different pressings of the same album can browse to the same peer directory
and match the same `(username, filename)` files — nothing previously asked
"is this queue key already held by another request" before claiming
ownership, so both requests would accept the same files, share one
`attempt_fingerprint`, and race for the same canonical processing folder;
the loser saw its files vanish mid-import (`event_path_gone_from_disk`) and
re-downloaded the whole album. `lib.enqueue._cross_request_conflict_ids` now
runs before every `try_enqueue` / `try_multi_enqueue` claim: a cross-cycle
check first (`PipelineDB.get_conflicting_transfer_request_ids`, called
through ONE `DownloadOwnershipWriter.open_conflict_check_session` handle
shared across the whole calling invocation — never opened per candidate,
see below — a read-only join of this table to `album_requests` requiring
an accepted row whose owner is *currently* `downloading` — never
`processing`, per the never-add-processing-to-a-transfer-status-set
invariant), then a cycle-scoped registry
(`lib.enqueue.ClaimedQueueKeysRegistry`, one instance per cycle, threaded
into every find-download worker context by reference the same way
`ctx.download_ownership` already is — not a module global; there is no
process-wide state and nothing to reset between tests). The cross-cycle read
runs first deliberately: registering in the registry before the cross-cycle
check would "poison" it for a rejected attempt's OTHER, otherwise-free keys,
wrongly blocking an unrelated same-cycle sibling.

The cross-cycle join is additionally scoped to the owner's CURRENT attempt.
A `'downloading'` owner can carry many historical accepted ledger rows —
live-DB measurement found 80.3% of accepted rows belong to a non-current
attempt, up to 76 distinct accepted attempt fingerprints for one request —
so without this scope an owner actively downloading a FRESH peer's files
would falsely block a sibling on a queue key from that SAME owner's OWN
abandoned attempt from 30 days earlier.

As of issue #1196 item 1 the scope is EXACT attempt identity, not a clock
comparison: `active_download_state.attempt_fingerprint` (see above) carries
the same `lib.processing_paths.attempt_fingerprint` value the ledger's own
`attempt_fingerprint` column carries for every row this attempt writes —
both derived from the identical transfer-key set, so they agree BY
CONSTRUCTION. When the owner's state carries that key, the join requires
EXACT fingerprint equality
(`l.attempt_fingerprint = r.active_download_state ->> 'attempt_fingerprint'`):
no app-clock-vs-PG-clock assumption, no skew window, no failure direction
to reason about — a stale attempt's ledger rows simply carry a different
fingerprint and never match, however new their `enqueued_at` looks.

As of issue #1199 item 2, the ELSE arm is unconditional `TRUE`: a NULL,
missing, or malformed `active_download_state` — or one that exists but
LACKS the `attempt_fingerprint` key — fails CLOSED unconditionally, with
no clock involved at all. Every accepted row for that `'downloading'`
owner counts as in-scope (blocks), regardless of the ledger row's own
`enqueued_at`. A PRE-#1199 version of this query instead fell back to a
clock comparison (`AND l.enqueued_at >= COALESCE((r.active_download_state
->> 'enqueued_at')::timestamptz, '-infinity')`) when the state existed but
lacked the fingerprint key — a deploy-window accommodation for an
in-flight download claimed by code from before that field existed. Live
measurement on 2026-08-19 found the cohort empty (1 `downloading` request,
0 NULL states, 0 lacking `attempt_fingerprint`), so the fallback arm, its
`::timestamptz` cast, and its fake mirror were deleted as dead code per
the no-deprecated-helpers rule (`.claude/rules/scope.md`) — there is no
longer any attempt-boundary rescue for a fingerprint-less state; only the
fingerprint-equality arm above scopes to the current attempt. A live
`'downloading'` owner reaching this ELSE arm is possible only via a
NULL/malformed state or a pre-#1196 historical row: `build_active_download_
state` sets `attempt_fingerprint` to `None` only when `entry.files` is
empty, and every enqueue persist site in `lib/enqueue.py` guards
`files_to_enqueue`/`planned_files` non-empty before that state is ever
built — a future change that lets an empty-files claim through would
silently make that owner's every accepted key block, not just its current
attempt's (issue #1199 review F9).

A `replaced` owner (Replace-lineage attempt sharing) or one that has already
moved on (`wanted`/`imported`) never blocks; the same request re-claiming its
own keys (poll-loop retries) never self-blocks. A guard hit skips the
candidate exactly like the peer-cooldown/denylist skip — no claim, no
enqueue, no new backoff, the request stays on normal cadence. A registered
same-cycle claim is released — so it cannot keep blocking an innocent sibling
for the rest of the cycle — at three points once the guard has already
cleared for that candidate: the matched peer turns out to be offline
(checked immediately after the guard, per the ordering below), the
ownership claim itself is refused (the request's row no longer matches the
expected `wanted` CAS), or the enqueue outcome resolves to
`verified_no_acceptance` (the claim was reset and confirmed no transfer
landed).

The guard is checked BEFORE the peer-online probe in `try_enqueue`, so a
conflicted candidate never pays for a network round trip it would only throw
away; the cross-cycle DB session is opened ONCE per `try_enqueue` /
`try_multi_enqueue` call (never per candidate — a fresh connection per
matched candidate, across a worker pool sized to the whole cycle, risked a
transient connection storm at post-browse convergence) and is safe to share
because each call runs on a single worker thread for its whole invocation.

A guard skip is logged like an ordinary `no_match` — the `search_log.outcome`
column is deliberately UNCHANGED by issue #1196 item 2, precisely so
`unfindable_detection`'s classification inputs stay byte-identical (see
`search_log.cross_request_conflict_request_ids` below, which is never
referenced by `get_unfindable_search_log_signal`'s SQL). `unfindable_
detection`'s branch 4 signal only fires after `REQUIRED_ZERO_FIND_CYCLES`
(3) consecutive zero-find plan cycles for the SAME request — a single
skipped cycle cannot trip it — and a guard skip can only recur across
cycles while the blocking sibling remains `status='downloading'` on the
contested keys; once that sibling's attempt resolves (imported, replaced,
or reset to `wanted`), the skip stops recurring. This is not treated as a
defect. What DOES change (#1196 item 2): the guard-skip search_log row
additionally carries `cross_request_conflict_request_ids`, naming the
conflicting owner(s) — an operator reading `pipeline-cli triage show` or
the same-shaped `/api/triage/<id>` response can now tell "this attempt was
deliberately declined because a sibling already held the queue keys" apart
from "no peer had it", even though both produce the same `outcome`.

## Persisted search plans (migration 014)

Search execution is plan-driven. Each wanted request owns a materialised
`search_plans` row with an ordered list of `search_plan_items` (the runnable
queries) and a cursor on `album_requests` (`active_plan_id`, `next_plan_ordinal`,
`plan_cycle_count`). `search_attempts` no longer selects queries; it remains
only as scheduler/backoff history. The pure generator that produces plan
items lives in `lib/search.py` and is keyed by `SEARCH_PLAN_GENERATOR_ID`
(`search-plan/<date>-<seq>`), which is bumped manually whenever generation-
affecting code or config changes — see "Generator id discipline" below.

### `search_plans`

| Column | Type | Notes |
|---|---|---|
| `id` | `SERIAL PRIMARY KEY` | |
| `request_id` | `INTEGER NOT NULL` | FK → `album_requests(id) ON DELETE CASCADE` |
| `generator_id` | `TEXT NOT NULL` | Mirrors `SEARCH_PLAN_GENERATOR_ID` at write time |
| `status` | `TEXT NOT NULL` | One of `active`, `superseded`, `failed_deterministic`, `failed_transient` |
| `failure_class` | `TEXT NULL` | `no_runnable_query`, `metadata_incomplete`, `resolver_unavailable`, `dependency_failure`, `unknown` |
| `metadata_snapshot` | `JSONB NULL` | Snapshot of the release metadata used to generate this plan |
| `provenance` | `JSONB NULL` | Bounded provenance: dropped tokens, deduped variants, omitted candidates |
| `error_message` | `TEXT NULL` | Sanitized human-readable error (no credentials / host paths) |
| `superseded_at` | `TIMESTAMPTZ NULL` | Set when an active plan flips to `superseded` |
| `superseded_by_plan_id` | `INTEGER NULL` | FK → `search_plans(id) ON DELETE SET NULL` |
| `created_at` | `TIMESTAMPTZ NOT NULL DEFAULT NOW()` | |

Indexes:

- `idx_search_plans_request_status (request_id, status)` — active-plan lookup
- `idx_search_plans_generator (generator_id)` — current vs old-generator scans
- `idx_search_plans_request_created_at (request_id, created_at DESC)` — supersession trail
- `uniq_search_plans_one_active_per_request (request_id) WHERE status = 'active'` — partial unique; one active plan per request
- Composite-unique `(id, request_id)` — supports the active-plan FK below

Plan statuses:

- **`active`** — current successful plan for this request. The cursor
  on `album_requests` points here. Only one per request (partial unique).
- **`superseded`** — was active, replaced by a newer successful plan.
  Stays readable for forensic audit; `superseded_by_plan_id` walks
  forward to the replacement.
- **`failed_deterministic`** — sticky for the current generator id
  (e.g. no runnable query for any tier). Reconciliation will not retry.
- **`failed_transient`** — retryable (resolver outage, dependency hiccup).
  Reconciliation retries on the next startup.

### `search_plan_items`

| Column | Type | Notes |
|---|---|---|
| `id` | `SERIAL PRIMARY KEY` | |
| `plan_id` | `INTEGER NOT NULL` | FK → `search_plans(id) ON DELETE CASCADE` |
| `ordinal` | `INTEGER NOT NULL CHECK (ordinal >= 0)` | Cursor position, 0-indexed |
| `strategy` | `TEXT NOT NULL` | Free-form: `default`, `unwild`, `unwild_year`, `track_<idx>`, ... |
| `query` | `TEXT NOT NULL CHECK (length(btrim(query, ' \t\n\r\f\v')) > 0)` | Runnable query — never blank |
| `canonical_query_key` | `TEXT NULL` | Normalised key for dedupe and per-query usefulness aggregation |
| `repeat_group` | `TEXT NULL` | Shared by intentionally-repeated default slots |
| `provenance` | `JSONB NULL` | Per-item provenance |
| | | UNIQUE `(plan_id, ordinal)` |

Indexes:

- `idx_search_plan_items_plan_ordinal (plan_id, ordinal)` — cursor reads
- `idx_search_plan_items_canonical_key (canonical_query_key)` — per-query rollups

### `album_requests` cursor fields

| Column | Type | Notes |
|---|---|---|
| `active_plan_id` | `INTEGER NULL` | Composite FK → `search_plans(id, request_id)`, `ON DELETE SET NULL (active_plan_id)` |
| `next_plan_ordinal` | `INTEGER NOT NULL DEFAULT 0` | Index into the active plan's items |
| `plan_cycle_count` | `INTEGER NOT NULL DEFAULT 0` | Increments only when the cursor wraps past the final ordinal |

Constraints:

- `album_requests_active_plan_owner_fkey (active_plan_id, id) → search_plans(id, request_id)` — guarantees the active plan belongs to this request, not another. Plan deletion only nulls `active_plan_id`; the request id stays intact.
- `next_plan_ordinal >= 0`, `plan_cycle_count >= 0`.

Index: `idx_album_requests_wanted_active_plan (status, active_plan_id) WHERE status = 'wanted'` supports the all-wanted reconciliation scan.

### `search_log` plan-context fields

Migration 014 adds nullable plan-context columns. Historical rows stay
valid with `NULL` plan context and any `outcome` value — including
`exhausted` — so legacy reporting remains queryable. The `outcome`
CHECK constraint is intentionally untouched.

| Column | Type | Notes |
|---|---|---|
| `plan_id` | `INTEGER NULL` | FK → `search_plans(id) ON DELETE SET NULL` |
| `plan_item_id` | `INTEGER NULL` | FK → `search_plan_items(id) ON DELETE SET NULL` |
| `plan_ordinal` | `INTEGER NULL` | Mirrors the executed item ordinal |
| `plan_strategy` | `TEXT NULL` | Mirrors the executed slot strategy |
| `plan_canonical_query_key` | `TEXT NULL` | For per-query stats grouping |
| `plan_repeat_group` | `TEXT NULL` | For per-repeat-group stats grouping |
| `plan_generator_id` | `TEXT NULL` | Stamped at log time so post-cutover stats can filter by current generator |
| `execution_stage` | `TEXT NULL` | `pre_attempt`, `accepted`, `stale_completion`, `reconciliation` |
| `attempt_consumed` | `BOOLEAN NULL` | True iff this row consumed a slot (advanced cursor) |
| `cursor_update_status` | `TEXT NULL` | `advanced`, `wrapped`, `unchanged`, `stale` |
| `stale_reason` | `TEXT NULL` | Short tag explaining why a row is stale (e.g. `regenerated_mid_flight`, `plan_or_ordinal_drift`) |
| `plan_cycle_snapshot` | `INTEGER NULL` | Snapshot of `plan_cycle_count` at log time, for cycle bucketing without rejoining the request row |

Indexes:

- `idx_search_log_plan_item (plan_item_id)`
- `idx_search_log_canonical_query_key (plan_canonical_query_key)`
- `idx_search_log_plan_id_created_at (plan_id, created_at DESC)`

### `search_log` outcomes — no-new-`exhausted` policy

Outcomes still recognised by the schema: `found` (matched + enqueued),
`no_match` (results but no suitable download), `no_results` (0 results
from slskd), `timeout`, `error`, `empty_query` (can't build query),
`exhausted` (legacy reset signal). The canonical Python spelling is
`lib.pipeline_db.SEARCH_LOG_OUTCOMES` (`SearchLogOutcome` Literal),
pinned to migration 010's CHECK by `tests/test_migrator.py`.

After the persisted-search-plans cutover, **new code never writes
`outcome='exhausted'`**. Plan wrap is the replacement: the executor
records a normal accepted-search outcome (`no_match`, `no_results`,
`error`, etc.) and the consumed-attempt DB method sets
`cursor_update_status = 'wrapped'` plus increments
`plan_cycle_count`. Historical `outcome='exhausted'` rows from before
the cutover stay valid and continue to render in the existing dashboard
position labelled as historical. See
`docs/persisted-search-plans-rollout.md` for the SQL spot-check that
confirms zero new exhausted rows after the deploy timestamp.

### Execution stage, attempt-consumed, cursor-update status

These four audit markers (`execution_stage`, `attempt_consumed`,
`cursor_update_status`, `stale_reason`) make pre-attempt failures,
accepted attempts, and stale post-regeneration completions
distinguishable in `search_log`:

- `execution_stage='pre_attempt'`, `attempt_consumed=false`,
  `cursor_update_status='unchanged'` — submission/setup failed before
  slskd accepted the search. Non-consuming. Backoff still applies.
- `execution_stage='accepted'`, `attempt_consumed=true`,
  `cursor_update_status='advanced'` — happy path; ordinal moved forward.
- `execution_stage='accepted'`, `attempt_consumed=true`,
  `cursor_update_status='wrapped'` — final ordinal; cursor wrapped to
  0 and `plan_cycle_count` incremented. **This replaces
  `outcome='exhausted'`** as the cycle-wrap signal.
- `execution_stage='stale_completion'`, `attempt_consumed=false`,
  `cursor_update_status='stale'`, `stale_reason=<tag>` — a regeneration
  superseded the active plan after the search was submitted. Log-only;
  active cursor / status / scheduling are not mutated.
- `execution_stage='reconciliation'` — emitted by startup
  reconciliation (rare). Not a normal slot execution.

### `candidates` JSONB

Top 20 peer scores per search, sorted by `(matched_tracks DESC, avg_ratio DESC)`. Each entry is a `lib.quality.CandidateScore` (`msgspec.Struct`):

```json
{"username": "peer", "dir": "...", "filetype": "lossless",
 "matched_tracks": 24, "total_tracks": 26, "avg_ratio": 0.91,
 "missing_titles": ["..."], "file_count": 26}
```

Empty array `[]` for `no_results` / `no_match` outcomes; `NULL` for `error`, `timeout`, `exhausted`, `empty_query`. Decoded at exactly one site per consumer (`web/routes/pipeline.py::get_pipeline_detail` and `scripts/pipeline_cli/show.py::cmd_show`) via `msgspec.convert(blob, type=list[CandidateScore])`.

### `final_state`

The slskd terminal state for the search (`Completed`, `ResponseLimitReached`, `TimedOut`, `Errored`, etc.). `NULL` on historical `exhausted` outcomes (no slskd round-trip) and on `pre_attempt` rows where slskd was never reached.

### Generator id discipline

`SEARCH_PLAN_GENERATOR_ID` in `lib/search.py` is the **single runtime
source** of "which generator output is current". CLI add, web add,
startup reconciliation, regeneration, and the executor all read this
constant. Bump it (date-stamped string,
e.g. `search-plan/2026-05-08-2`) **whenever** any of the following
change:

- generator output rules (which slots are emitted, in what order)
- query tokenisation
- the low-entropy token set (currently `the`, `you`, `from`, `and`)
- slot ordering / ranking
- dedupe behaviour
- repeat-group identity
- provenance shape

Plans whose `generator_id` differs from the current id are "old-
generator" plans. Startup reconciliation supersedes them with new
plans on the next cycle. Tests pin both the literal id and a
representative ladder snapshot, so any output drift forces
`tests/test_search.py::test_generator_id_constant_is_pinned` to fail
until the id is intentionally bumped.

## Search-plan iteration 2 (migrations 026–033)

Iteration 2 layers observability + detection state onto the
persisted-search-plans surface. Every column below was added by a
PR1 migration; PR3 wires the writes. The iter2 brainstorm and plan
docs (`docs/brainstorms/2026-05-25-search-plan-iteration-2-requirements.md`,
`docs/plans/2026-05-25-001-feat-search-plan-iteration-2-plan.md`) are
the requirement-id source of truth — each entry below points at the
R-id it satisfies.

### `search_log` forensics columns (migration 027, written by PR3 U11)

Seven nullable scalars that let triage SQL skip JSONB introspection
into `candidates`. Populated at log-write time by
`lib/pipeline_db/search_plan.py::log_search` via the matcher
(`lib/matching.py::check_for_match`) and search-executor layer.
Historical rows pre-deploy carry `NULL` in all seven; new rows
post-PR3 populate every applicable column.

| Column | Type | Notes |
|---|---|---|
| `rejection_reason` | `TEXT NULL` | Dominant matcher rejection from the top-scored candidate. One of `strict_count_mismatch`, `avg_ratio_low`, `cross_check_failed`, `all_skipped_pre_filter`, `bitrate_below_min`, `denylisted_user`, `cooldown`, `cap_truncation_no_survivors`. `NULL` on `outcome='found'` or when there were no candidates to reject. (R22) |
| `result_count_uncapped` | `INTEGER NULL` | slskd's true `responseCount` before the cratedigger 1000-cap is applied. `result_count` remains the post-cap count. Comparing the two surfaces saturated searches honestly. (R23) |
| `query_token_count` | `INTEGER NULL` | `len(query.split())` — total tokens, including duplicates and stopwords. (R24) |
| `query_distinct_token_count` | `INTEGER NULL` | `len(set(query.split()))` — distinct tokens. Low distinctiveness correlates with bag-of-words slskd searches that match too many peers. (R24) |
| `expected_track_count` | `INTEGER NULL` | The request's `total_tracks` snapshotted at search-execution time. Not slskd's result count, not a hardcoded value — the operator's expectation for this release. (R25) |
| `matcher_score_top1` | `REAL NULL` | The top candidate's composite score (`matched_tracks + avg_ratio`) from `candidates[0]`. `0.0` on `no_results` / `no_match` with empty candidate set. (R26) |
| `query_template` | `TEXT NULL` | Operator-readable shape derived from `plan_strategy` (e.g. `{artist} {title}`, `{artist} {track_N}`, `{catalog_number}`). Lets `GROUP BY query_template` surface which template shapes are productive vs noise. (R27) |

### `search_log.cross_request_conflict_request_ids` (migration 079, issue #1196 item 2)

`INTEGER[] NULL`, no CHECK constraint (mirrors `rejection_reason`'s plain
nullable shape). The cross-request enqueue-guard (#1178) skip marker: `NULL`
when the guard never fired for this search; a non-NULL array names every
OTHER request id whose held queue keys made a candidate in this attempt
decline. Threaded from `lib.enqueue`'s `EnqueueAttempt`/`FindDownloadResult`
through `SearchResult.cross_request_conflict_ids`
(`cratedigger._apply_find_download_result`) into
`ConsumedAttemptInput.cross_request_conflict_request_ids`
(`cratedigger._log_search_result` →
`PipelineDB.record_consumed_search_attempt`). Deliberately a SEPARATE
column from the seven migration-027 forensics scalars above and from
`outcome` — `get_unfindable_search_log_signal`'s SQL never references it,
so it cannot change `classify_unfindable_from_state`'s inputs. Rendered on
`pipeline-cli triage show` (`conflict=<ids>` per recent entry) and the
matching `SearchLogEntry.cross_request_conflict_request_ids` field on the
`/api/triage/<id>` response.

### `album_requests` observability columns (migration 028, written by PR3 U12 / U13 / U14)

Eight columns covering plan-wrap classification, VA detection,
unfindable categorisation, and long-tail-rescue audit. CHECK
constraints on the three enum-shaped TEXT columns surface typos as
constraint violations rather than silent corruption.

| Column | Type | Notes |
|---|---|---|
| `failure_class` | `TEXT NULL` | 5-bucket cycle classification: `A_zero_results_dominant`, `B_cands_never_match`, `D_found_but_no_import`, `E_mixed`, `resolved`. Written by `lib/search_plan_service.py` at plan-wrap inside the cursor-advance transaction (PR3 U12). `NULL` until the first cycle wraps. A wrap with zero searches in the cycle leaves it `NULL` (defensive: "no signal" is not a classification). CHECK enforces the enum. (R28) |
| `is_va_compilation` | `BOOLEAN NOT NULL DEFAULT FALSE` | VA detection flag set at enqueue by `lib/field_resolver_service.py::detect_va_compilation` (3-rule detector — canonical VA MBID match, Compilation release-group + divergent track credits, split-artist joinphrase). Consumed by `_generate_va_plan` in the generator. (R12) |
| `unfindable_category` | `TEXT NULL` | 4-bucket cohort taxonomy: `artist_absent`, `album_absent_artist_present`, `one_track_structural`, `wrong_pressing_available`. Written by `lib/unfindable_detection_service.py` on its daily cadence (PR3 U13). Cleared on long-tail-rescue (U14). CHECK enforces the enum. Partial index `idx_album_requests_unfindable_category` over rows where the column is non-NULL supports the operator triage scan. (R18, R19) |
| `unfindable_categorised_at` | `TIMESTAMPTZ NULL` | When the categoriser last ran for this request. Used by the detection job to pick the K oldest probes per run. |
| `last_artist_probe_at` | `TIMESTAMPTZ NULL` | Most recent artist-only catalog probe against slskd. Per-request probe cadence target is ~7 days. |
| `last_artist_probe_match_count` | `INTEGER NULL` | Result count from the last artist-only probe. Feeds the `artist_absent` vs `album_absent_artist_present` classifier branch. |
| `rescued_at` | `TIMESTAMPTZ NULL` | Long-tail-rescue audit timestamp. Set by the importer success path (PR3 U14, `lib/dispatch/` → `PipelineDB.mark_imported_with_rescue`) when a request that was carrying an `unfindable_category` transitions to `imported`. First-rescue-wins — immutable once set; Replace flows do not re-stamp it. (R21) |
| `prior_unfindable_category` | `TEXT NULL` | The `unfindable_category` value cleared by the rescue (same enum + CHECK as `unfindable_category`). Lets `SELECT prior_unfindable_category, COUNT(*) FROM album_requests WHERE rescued_at IS NOT NULL` surface which cohorts the watch loop actually rescues over time. (R21) |

R20 ("the system never stops searching") is enforced structurally: the
`cratedigger-unfindable.service` shares no code path with the regular
5-min search loop and an `ast.parse` walk over
`lib/unfindable_detection_service.py` + `scripts/run_unfindable_detection.py`
rejects any reference to cursor-mutation names.

### `album_requests.catalog_number` (migration 032, resolved at enqueue)

| Column | Type | Notes |
|---|---|---|
| `catalog_number` | `TEXT NULL` | Resolved at enqueue via the dual-source field resolver (MB + Discogs), populating the `catalog_number` plan-strategy slot the PR2 generator adds. |

### `album_tracks.track_artist` (migration 029, populated at enqueue)

| Column | Type | Notes |
|---|---|---|
| `track_artist` | `TEXT NULL` | Per-track artist persisted from the resolver output. Consumed by PR2's VA plan generation (`va_track_artist_*` slots). NULL until resolution succeeds for that track. |

### Discogs `album_tracks` manifests are normalized rip-shaped (issue #1261, no migration)

Discogs encodes hidden-track runs as sub-positions of one physical track
(`10.1 Song / 10.2 (silence) / 10.3 Untitled`), and a rip of that disc has
ONE file at position 10. `lib/discogs_positions.py::normalize_release_tracks`
— the one canonical parser, feeding every Discogs manifest persist path
(`web/discogs.py::get_release` for the add/Replace/CLI persist callers
and the browse display, `album_source.py::_populate_tracks_discogs` for
the search worker's empty-manifest fallback) — collapses each
sub-position group into a single row: title from the first
non-placeholder sub-entry (blank and `(silence)`-style titles are
placeholders), known durations summed, grouping keyed on the literal
position base so unparseable bases stay distinct; a flat index parent
sharing the base joins its group with its title authoritative and its
duration, when present, taken as the physical track's total.
Empty-position-AND-empty-duration heading rows (Discogs side/disc/bonus
section labels the mirror flattens into the tracklist — the flavor that
stuck Kid A at 14 expected tracks for a 10-file rip) are dropped when
the release positions its other tracks; an empty position WITH a
duration is ambiguous and survives (the measured mirror rule in
`lib/library_completeness.py`), an all-unpositioned tracklist keeps
every row, and a nested `sub_tracks` index parent is never treated as a
heading. Video-marker positions (`Video`, `Video 1` — enhanced-CD bonus
rows, request 5936's shape) drop as non-audio unless every non-heading
row is video-marked: a whole-release video pressing's content is
rip-real (the Placebo `ignore_video_tracks` precedent,
`docs/plans/2026-05-12-001-feat-video-track-wrong-matches-plan.md`).
Bare vinyl side letters (`A`/`B`) parse as track 1 of their
side, consistent with the existing `A1`/`B1` side-as-disc convention;
`1A`-style and trailing-dot (`1.`) positions parse instead of falling
to the track-0 sentinel.
The matcher's strict count gate compares candidate folders against this
manifest, so a per-sub-entry manifest makes every real copy unmatchable
forever (28 wanted requests, including Kid A, were stuck this way; this
code only shapes fetches from the mirror going forward — their
already-persisted rows are corrected by a deploy-window one-shot,
re-fetch through the fixed normalizer plus plan regeneration, run when
this change deploys). `get_release_raw` keeps literal positions for
source-audit consumers.

### `album_request_field_resolutions` (migration 030, side table)

Tracks per-(request, field) resolution attempts for the four
network-dependent fields (`release_group_year`, `release_group_id`,
`track_artist`, `catalog_number`). Used by enqueue-time inline
resolution + the operator deploy-window backfill heredoc.

| Column | Type | Notes |
|---|---|---|
| `request_id` | `INTEGER NOT NULL` | FK → `album_requests(id) ON DELETE CASCADE` |
| `field_name` | `TEXT NOT NULL` | One of the four resolved fields above |
| `status` | `TEXT NOT NULL` | `resolved`, `unresolved_no_data`, `unresolved_4xx_client`, `unresolved_mirror_unavailable`, `unresolved_timeout`, `unresolved_field_missing_upstream` |
| `attempted_at` | `TIMESTAMPTZ NOT NULL DEFAULT NOW()` | Last attempt timestamp |
| (audit metadata) | | Details preserved per-row for forensic queries against upstream-data gaps |

Transient buckets (`unresolved_mirror_unavailable`, `unresolved_timeout`)
retry on the next enqueue path; permanent buckets (`unresolved_no_data`,
`unresolved_4xx_client`, `unresolved_field_missing_upstream`) record
the audit trail without retry.

### `request_search_summary` view (migration 031, consumed by PR4)

Per-request 14-day rollup over `search_log` for the future operator
triage surface. Plain VIEW, not materialised — operator triage
frequency is human-paced and the bounded scan cost rides on the
existing `idx_search_log_request_created_at` composite index.

| Column | Type | Notes |
|---|---|---|
| `request_id` | `INTEGER` | Group key |
| `total_searches` | `BIGINT` | Search count in the 14-day window |
| `with_cands_count` | `BIGINT` | Rows where `candidates` JSONB is non-empty |
| `found_count` | `BIGINT` | Rows where `outcome='found'` |
| `near_cap_count` | `BIGINT` | Rows where `result_count >= 950` — popular albums hitting the 1000-cap |
| `zero_results_count` | `BIGINT` | Rows where `result_count = 0` |
| `pre_filter_skips_total` | `BIGINT` | Sum of `pre_filter_skip_count` (column added by migration 025) |
| `first_strategy_with_cands` | `TEXT` | Oldest `plan_strategy` in the window that produced ≥1 candidate |
| `dominant_rejection_reason` | `TEXT` | `MODE()` over `rejection_reason` (R22 column) for non-NULL rows |
| `last_search_at` | `TIMESTAMPTZ` | `MAX(created_at)` |

The 14-day window is intentional — triage windows that need older
data should query `search_log` directly. (R29)

## Wrong Matches and Force-Import

Albums rejected by beets validation (high distance, wrong pressing) are moved
to `wrong_matches/` under the slskd download dir, alongside
`failed_imports/`, with their `failed_path`
stored in `download_log.validation_result` JSONB. Wrong Matches cleanup consumes
already-persisted candidate/current evidence only; it never previews,
measures, or backfills evidence at delete time. Confident cleanup-eligible
force-mode rejects are deleted and cleared; would-import, uncertain, missing
evidence, stale evidence, active-job, and missing-path rows stay actionable for
manual review or converge.

Wrong Matches is a candidate/pressing-identity review surface, not a general
failed-import bucket. Two distinct predicates govern it, kept deliberately
separate (issue #1077, D1/D6): **worklist visibility**
(`rejection_scenario_is_wrong_match_candidate`) is a small exclusion set —
folder/audio-integrity fact rejects (`audio_corrupt`, `bad_audio_hash`,
`nested_layout`, `empty_fileset`, `mixed_source`) and the quality-only
`spectral_reject` scenario are excluded, and none of them quarantine with a
reviewable folder any more (`audio_corrupt` bans and deletes outright; the
other four and `spectral_reject` were never quarantined in the first place —
they clean up immediately as disposable processing state). **Cleanup-lane
admission** (`rejection_scenario_is_delete_eligible`) is a separate, narrower
explicit allowlist — exactly `extra_tracks`, `high_distance`,
`mbid_not_found`, `no_choose_match` may reach the reducer
(`lib.wrong_match_cleanup_service.cleanup_wrong_match`) at all. World
failures with a reviewable folder (`unmapped_audio`, `untracked_audio`,
`request_missing_mbid`, `request_missing_request_id`) are kept, banned, and
shown, but the reducer never even looks at them — nor does any unknown or
novel scenario string, nor `None`. Kept ⟺ its contributing peers are
denylisted; kept ⟹ visible in the worklist. SQL, the test fake, and
post-rejection cleanup all consume the neutral taxonomy in
`lib/wrong_match_policy.py`; a new non-match rejection scenario must be
classified there once rather than copied into each adapter, and
`tests/test_wrong_match_scenario_producer_audit.py` fails closed on any
scenario a producer spells but nobody classified. Full routing table,
producer-by-producer: `docs/rejection-routing.md`.

The quarantine lifecycle view surfaces unreferenced album folders across
every scanned root. `pipeline-cli triage quarantine` is a thin adapter over
`GET /api/triage/quarantine` (issue #1122 F1) — the route is the ONE
execution authority for both surfaces, because the processing tree it reads
is a private `0700 cratedigger:users` directory only the web service
identity can traverse; run directly in the operator's own CLI process the
scan raised `EACCES` and killed the whole view (the same #1063 shape that
already moved `force-import`, `replace`, and their cohort onto the
permissioned Unix socket):

```bash
pipeline-cli triage quarantine --json
curl https://music.ablz.au/api/triage/quarantine
```

`lib.quarantine_triage_service.list_unreferenced_quarantine_folders` scans
only immediate real directories under exactly four roots, two bases times
two markers: `<slskd_download_dir>/failed_imports/` and
`<slskd_download_dir>/wrong_matches/` (no current writer targets the
former any more — that cohort predates issue #1077 — but it can still hold
legacy folders), plus `<processing_dir>/albums/failed_imports/` and
`<processing_dir>/albums/wrong_matches/`, where every CURRENT kept
rejection actually lands (issue #1077; `lib.import_manifest._allocate_target`
always targets `wrong_matches/` now). Both `failed_imports/` roots exclude
the same code-owned `bad_files/`/`untracked_audio/` category buckets from
being misreported as album folders or recursively expanded — a live
`failed_imports/bad_files/` entry on the PROCESSING side (issue #1122 F3)
proved the bucket is not download-dir-specific. `lib.fs_authority`'s
`open_configured_quarantine_directory` separately enumerates a THIRD base,
`<beets_staging_dir>/failed_imports/` and `<beets_staging_dir>/wrong_matches/`,
for single-path containment checks; this service does NOT scan that base — its legacy
quarantine folders predate the current rejection pipeline and have not been
shown to fit this scan's DB-reference model uniformly. That is a known,
stated gap, not an oversight.

A visible Wrong Matches row protects its immediate album root in whichever
of the four quarantine roots it lives under, whether its persisted
`failed_path` is relative (`failed_imports/Artist - Album`), absolute, or a
descendant of that album root — except that a RELATIVE reference can only
ever reach the two download-dir-rooted roots, since relative resolution
always joins the single configured slskd download dir; the processing-side
roots are a wholly separate tree, so processing-sourced rejections always
persist an ABSOLUTE `failed_path` in the first place. References outside
every configured quarantine root do not claim local folders. A
`status='replaced'` parent is frozen audit history and is excluded by the
shared default Wrong Matches visibility rule, so its reference does not hide
a quarantine folder. The explicit
`/api/wrong-matches?include_replaced=true` history view still surfaces
those rows without changing lifecycle triage.

Results are sorted by folder name and carry `name`, absolute `path`, and
`mtime_ns`; the JSON envelope reports `quarantine_root` (the legacy
`failed_imports/` field), `wrong_matches_root`,
`processing_failed_imports_root`, and `processing_wrong_matches_root`. A
genuinely absent root is a valid empty state.
`lib.slskd_transfers`'s disk reaper protects only the two download-dir-rooted
roots forever (issue #571); the processing-side roots have no automated
reaper at all. Post-rejection cleanup (`lib.wrong_match_cleanup_service.cleanup_wrong_match`)
does automatically rmtree a delete-eligible REFERENCED folder there right
after its own rejection — but no automated sweep ever revisits an
UNREFERENCED folder in either processing-side root, which is exactly why
this operator-facing sweep matters there.
Configuration, DB, validation-envelope, directory-read, and mid-scan race
errors fail the whole view as CLI exit `5` / HTTP `503`; partial state is never
presented as an empty or trustworthy orphan list. Deletion remains an explicit
operator decision through the existing Wrong Matches delete surfaces.

`download_log.validation_result.wrong_match_triage` is the typed persisted
audit for Wrong Matches cleanup. Automatic post-rejection cleanup and operator
cleanup both write it, preserving the action, reason, decision, stage chain,
and frozen candidate/current evidence snapshots. Recents History renders this
audit as display-only metadata alongside the original Beets rejection.

After manual review, force-import overrides the distance check. The request
handler or CLI command validates the row/path synchronously, then enqueues a
`force_import` job. `cratedigger-importer` runs the actual beets mutation.

**Force import is the same path as any other import, with the Beets distance
overridden — nothing else about it is special-cased** (#1080). It runs the
same exact-release validation the automation lane runs
(`lib/download_validation.py::validate_release_with_merge_redirect`), handing
it `lib/beets.py::FORCE_IMPORT_DISTANCE_THRESHOLD` instead of
`beets_distance_threshold`; `harness/import_one.py` under `--force` raises
its apply-time `max_distance` to that same number. Two consequences:

- A request whose MusicBrainz release was merged away is rescued by whichever
  lane reaches it first. Before #1080 force skipped validation entirely and
  met the merged-away release at
  `harness/import_one.py::_find_target_candidate`,
  which has no redirect concept, so it rejected `mbid_missing` forever
  (live: `download_log` 39846 versus the automation lane's 39802 on request
  346). The merge rekey's fence therefore admits the force lane's claim —
  a `running` `force_import` job on a request with no automation owner that
  is neither `processing` nor `replaced` — alongside the automation owner
  pointer.
- The validation result is **identity resolution for force, never a verdict**.
  Force exists to import despite the validation verdict, so nothing branches
  on `valid`: an `extra_tracks` / `no_choose_match` / `validation_error`
  result still reaches the Beets launch exactly as it did before. The one
  exception is not a verdict either: if the seam retagged the installed album
  onto the survivor and the rekey was then refused (the race the pre-check
  cannot close), force refuses to launch, because the id the row still names
  is no longer where the library filed that album. The refusal is recorded as
  a `download_log` row with `outcome='failed'`, and the request keeps whatever
  runnable status it had.
- A rekey the seam refuses BEFORE touching the library — the survivor is
  already held by another request, or already carries an evidence row at the
  fingerprint being moved — also writes a `download_log` row with
  `outcome='failed'` naming the collision, and force then launches exactly as
  it did before #1080. That audit is the only durable record of the world:
  the collision persists until an operator resolves it, so every later force
  attempt would otherwise repeat a bare `mbid_missing` with no reason
  attached. One row per execution that reaches the branch (one per force
  action, one per completed-download validation); it is deliberately not
  deduplicated.

**Path resolution**: old entries stored relative paths
(`failed_imports/Foo - Bar`); new entries under `wrong_matches/` store absolute
paths. Force-import resolves relative paths against
`/mnt/virtio/music/slskd/` automatically.

Wrong Matches Converge is a web triage layer on top of the same queue. The UI
defaults each release to a `180` milli-distance loosen threshold, marks
candidate rows green when `validation_result.distance <= 0.180`, then posts to
`/api/wrong-matches/converge`. Green rows are enqueued as `force_import` jobs;
the queued job still owns the source path until it terminates. When Converge
runs, non-green rows for that release are deleted from disk and cleared from
the review list immediately (unrelated to the force-import terminal handling
below — see `lib.wrong_match_delete_service`).

1. Look up `download_log` entry by ID via `get_download_log_entry()` → extract `failed_path` from `validation_result` JSONB.
2. Resolve path (handle both relative and absolute) → verify files still exist.
3. Look up `mb_release_id` from `album_requests` via `request_id`.
4. Enqueue `import_jobs(job_type='force_import')` with a dedupe key for the `download_log` row.
5. `cratedigger-importer` claims the job and calls the existing dispatch path, including `import_one.py --force` (raises that run's apply-time `max_distance` to 999 — everything else runs normally: conversion, spectral, quality comparison).
6. The worker marks the job `completed` or `failed`; the import internals still write `download_log` and `album_requests` outcomes.
7. **Force-import success consumes its source** (issue #1077, D7): the worker
   deletes the reviewed source directory and clears the actionable
   `failed_path` pointer — completing the operator's own explicit action —
   via `lib.wrong_matches.cleanup_wrong_match_source`, the same helper the
   cleanup reducer uses. **Failure on the `audio_corrupt` decision** also
   deletes the source (D3: bad rips are never preserved). **Failure on
   every other decision** preserves the source exactly as-is
   (`"preserved_operator_force_source"`) — the original force/quarantine
   directory is operator authority and audit evidence; cleanup of the raw
   source requires a distinct operator action, never a quality result. This
   reverses the mid-July "dismiss but preserve" regression, which had
   stranded 64 of 90 wrong-match-sourced force imports as invisible disk
   folders (verified live 2026-08-12) by preserving on both outcomes. The
   failed job and `download_log` audit rows always remain regardless. A
   crash between the terminal `completed`/`failed` write and this receipt
   is durably retried, never lost (issue #1122): every importer startup
   replays the same decision, from the persisted `result` JSONB alone, for
   any terminal force job whose era-marked receipt is not yet PROVEN
   successful — see `docs/rejection-routing.md` § "Force-import outcomes
   (D7)" for the exact positive selection rule, query, and replay function
   names.

```bash
pipeline_cli.py force-import <download_log_id>
pipeline_cli.py import-jobs --status failed
pipeline_cli.py wrong-match-triage --apply --json
# or: POST /api/pipeline/force-import {"download_log_id": N}
```

`wrong-match-triage` is destructive and intentionally processes the full
Wrong Matches queue. It requires `--apply`, rejects scope flags, and returns
per-outcome counts matching the web `/api/wrong-matches/triage` summary.
