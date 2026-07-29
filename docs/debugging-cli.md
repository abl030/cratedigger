# Pipeline CLI operator reference

`pipeline-cli` is Cratedigger's writable operator and agent control plane. Its
typed subcommands are the normal interface for routine lifecycle, import,
destructive, and repair actions; each mutation follows the shared service/API
contract. Five existing web mutations are deliberately API-backed: the CLI
relays their canonical route response and does not construct a PipelineDB or
configure mirrors. `pipeline-cli routes` (or `pipeline-cli routes --json`) discovers the
live parser surface. This document is the authoritative active command
reference; `tests/test_docs_audit.py` requires its capability list to match the
parser exactly.

## Access on doc2

Run the installed CLI on doc2. From doc1, let `sudo` read the root-readable
secret into the operator shell's environment; the CLI itself still runs as the
operator, not root. `routes` needs no database credential:

```bash
ssh doc2 'pipeline-cli routes --json'
```

The installed wrapper has two independent authority shapes:

- `pipeline-delete`, `set-quality`, `upgrade`, `wrong-match-converge`, and
  `resolve-rg` connect to `/run/cratedigger-web/web.sock`. The caller must be a
  member of `services.cratedigger.web.accessGroup` (default
  `cratedigger-web`). That membership grants complete local HTTP/API authority,
  not merely permission to execute those five subcommands.
- Every other command retains its existing resource-specific boundary:
  PostgreSQL credentials/peer identity for database work, filesystem and Beets
  access for media/quarantine operations, and the relevant secret-file groups
  for commands that consume secrets. Web access group membership does not
  grant any of those resources.

Add only explicit trusted operator/agent identities to the web access group,
then start a fresh login/session so the supplementary group is present. nginx
and the non-root Cratedigger service identity are added by the module; nginx is
deliberately not added to `cratedigger-ops`, `users`, the Discogs secret group,
or another media/secret authority group.

Changing an installed Nix-store wrapper's executable mode is not an
authorization boundary: store programs are normally readable and can be
invoked through their interpreter. Protect the socket, database, filesystem,
Beets, and secret resources instead.

For SQL, pass multi-line input through stdin rather than argv; this preserves
dollar-quoted SQL and avoids shell expansion:

```bash
ssh doc2 'export PGPASSWORD="$(sudo grep "^PGPASSWORD=" /run/secrets/cratedigger-pgpass | cut -d= -f2)"; pipeline-cli query -' <<'SQL'
SELECT id, artist_name, album_title
FROM album_requests
ORDER BY updated_at DESC
LIMIT 10;
SQL
```

## Raw SQL safety boundary

`pipeline-cli query` accepts one statement and runs it in an explicit
read-only transaction by default. It is a safety and intent boundary, not an
authentication boundary: the trusted operator connection remains full-privilege.
Default mode rejects multi-statement/transaction-control escape shapes; use
typed commands for routine product mutations.

The narrow intentional escape hatch is exactly:

```bash
pipeline-cli query --write --confirm WRITE - <<'SQL'
UPDATE source_denylist SET reason = 'one-off correction' WHERE id = 123;
SQL
```

Both flags are required. A missing or mismatched token fails before caller SQL
is executed. Successful write statements with no result set report `Query
executed successfully.` just like other no-result SQL. SQL failures and invalid
raw-SQL invocation return exit 1; typed commands use the shared convention:
success 0, not found 2, input/semantic violation 3, conflict 4, transient 5.

## Operational guidance

- Start quality diagnosis with `pipeline-cli show`, `quality`, and
  `import-preview`; raw SQL is for bounded diagnostics or the deliberate
  escape hatch above.
- `show`'s download history prints the same failure `verdict` the web UI
  renders, plus the bounded per-transfer evidence behind it (labelled
  `Peer message`, or `Storage error` when the failures were slskd writing to
  our own share). Both surfaces wrap `lib/failure_presentation.py`, so they
  cannot disagree. The line is derived, not stored: `error_message` and
  `transfer_detail` in the DB and the journal stay raw, so query them
  directly when you need the untruncated text.
- `import-job-recovery show` prints the current revisioned recovery evidence.
  For an automation-owned processing album, pass that opaque revision back to
  `retry` or `close`; a concurrent ownership, launch, lease, or cleanup change
  is rejected instead of applying a decision to stale evidence.
- `import-job-recovery retry` queues a fresh automation job only when the
  recorded execution is proven dead and exact evidence is unchanged.
  `import-job-recovery close` records an explicitly reconciled lifecycle result
  without replay. Both require an audit reason; automation close additionally
  requires `--result-status wanted|imported`.
- `ban-source`, `library-delete`, and Wrong Matches deletion commands are
  irreversible operator actions. Their confirmation tokens are intent checks,
  not authorization; inspect the exact release first.
- Use `routes` for discovery and this document for current capabilities.

`ban-source` exits 0 only after the server-resolved exact Beets release is
absent. If the denylist, hashes, and request state committed but the release
still exists, it exits 4 with `status="partial"` and
`error="cleanup_incomplete"`. Inspect that exact album and use the guarded
`library-delete` recovery path; do not blindly retry a commit-ambiguous
deletion.

`library-delete` has three truthful terminal shapes. `status=ok` means the
exact Beets row and owned artifacts are absent; `preserved_paths` lists unknown
content left untouched, and `notifications` records media submissions or
warnings. `error=delete_incomplete` leaves the PostgreSQL request in place,
skips media notification, and exits 4; an ordinary filesystem failure keeps
the Beets row as retry authority. A lost subprocess/protocol acknowledgement
is explicitly manual: its JSON retains the preflight album, artist, exact
former path, and pipeline identity while deletion counts are `null`. Do not
infer success from metadata absence. `status=partial` with
`album_deleted=true` means Beets deletion completed but the named pipeline row
remains after a purge failure.

## API-backed mutation commands

`pipeline-delete`, `set-quality`, `upgrade`, `wrong-match-converge`, and
`resolve-rg` call the canonical web route over the module-owned Unix socket.
The installed Nix wrapper selects that socket while constructing the parser:
it accepts no `--api-base`, carries no Basic username/password, and has no TCP,
direct-database, or duplicate-service fallback. In the installed production
nginx/Unix topology, the socket permission establishes local authority; the CLI
then writes the trusted `cli` request channel, while nginx overwrites any
browser-supplied marker. A forged channel header does not bypass that installed
path because the backend has no production TCP listener.

The standalone source/development entry point still defaults to
`http://127.0.0.1:8085` and exposes global `--api-base ORIGIN` for deliberate
TCP adapter testing. A directly started `web/server.py` listener using
`--dev-port` trusts `X-Cratedigger-Request-Channel: cli` without a Unix
permission boundary;
it is deliberately insecure, must remain loopback-only, and exists only for
adapter development/tests. Never expose it or use it as a production escape
hatch. The installed wrapper does not expose `--api-base`.

Valid JSON route responses, including 5xx responses, are relayed on stdout.
Any 2xx exits 0; 404 exits 2; 400/422 exits 3; 409 exits 4; other statuses exit
5. Locally generated transport/protocol failures (including a missing or
unreachable socket, malformed development origins, redirects, or non-object
JSON responses) exit 5 with a structured error on stderr and never fall back.
`pipeline-delete ID --confirm DELETE` and
`wrong-match-converge ID THRESHOLD_MILLI --apply` make no HTTP call when the
local intent gate is missing. Those words/flags are intent checks layered
inside socket authorization, never credentials.

## Command capability surface

- `pipeline-cli add` — Add a MusicBrainz or Discogs request.
- `pipeline-cli audit world` — Read-only PipelineDB, Beets, evidence, and disk coherence audit.
- `pipeline-cli ban-source` — Remove a server-resolved bad rip and requeue its request when appropriate.
- `pipeline-cli beets-distance` — Measure a rejected download against an exact release.
- `pipeline-cli disk-coverage` — Compare active pipeline rows with Beets library coverage.
- `pipeline-cli force-import` — Queue a rejected download for the importer lane.
- `pipeline-cli import-job-recovery close` — Close an explicitly reconciled ambiguous Beets operation without replay.
- `pipeline-cli import-job-recovery retry` — Queue a fresh operation after exact evidence proves the prior execution dead.
- `pipeline-cli import-job-recovery show` — Show revisioned evidence for one recovery-required import job.
- `pipeline-cli import-jobs` — List import queue jobs.
- `pipeline-cli import-preview` — Inspect an import preview and its evidence inputs.
- `pipeline-cli library-delete` — Delete one exact server-owned Beets album.
- `pipeline-cli list` — List album requests.
- `pipeline-cli long-tail` — Show the wanted long-tail worklist.
- `pipeline-cli pipeline-delete` — Delete a pipeline request through its canonical web route.
- `pipeline-cli quality` — Simulate quality decisions and replay current candidate evidence.
- `pipeline-cli query` — Run one read-only SQL statement, or the explicit write escape hatch.
- `pipeline-cli repair-spectral` — Repair stale spectral state.
- `pipeline-cli replace` — Supersede a request with another exact pressing in its release family.
- `pipeline-cli resolve-rg` — Resolve a request release group through its canonical web route.
- `pipeline-cli routes` — Discover every parser command, argument, and description.
- `pipeline-cli search-plan advance` — Advance one persisted search-plan cursor.
- `pipeline-cli search-plan dry-run` — Generate a request plan without persisting it.
- `pipeline-cli search-plan history` — Read cursor-paginated per-request search history.
- `pipeline-cli search-plan regenerate` — Regenerate one persisted request plan.
- `pipeline-cli search-plan saturation` — Show recent search-plan saturation and pre-filter skips.
- `pipeline-cli search-plan show` — Show one request's plan, cursor, items, and provenance.
- `pipeline-cli set` — Apply a typed request lifecycle transition.
- `pipeline-cli set-intent` — Set lossless-on-disk intent.
- `pipeline-cli set-quality` — Set request quality through its canonical web route.
- `pipeline-cli show` — Show a request, attempts, and quality state.
- `pipeline-cli status` — Show request counts by lifecycle status.
- `pipeline-cli triage list` — List a named triage cohort.
- `pipeline-cli triage quarantine` — Read-only unreferenced immediate quarantine-folder scan.
- `pipeline-cli triage show` — Compose per-request unfindable, field, and search forensics.
- `pipeline-cli wrong-match-delete` — Delete one visible Wrong Matches source folder.
- `pipeline-cli wrong-match-converge` — Converge one Wrong Matches request through its canonical web route.
- `pipeline-cli wrong-match-delete-group` — Delete visible Wrong Matches folders for one request.
- `pipeline-cli wrong-match-triage` — Converge the full Wrong Matches queue using persisted evidence.
- `pipeline-cli youtube-album` — Resolve a release to the YouTube Music album
  matrix directly through the shared resolver service; accepts
  `<identifier> [--refresh] [--json]`, with human-readable output by default
  and the full result available as JSON. It uses the configured database and
  mirrors and remains available when `web.enable = false`; it does not use the
  optional web Unix socket or fall back to an HTTP adapter when that socket is
  absent. Only a non-refresh lookup by an already-cached MusicBrainz
  release-group identifier can return `ok` with `from_cache = true` without
  contacting a mirror or YouTube Music. A Discogs lookup must consult the
  configured mirror first because release IDs and master IDs share the integer
  namespace; after the mirror establishes the master, the normal post-widen
  durable-cache read may return `ok` with `from_cache = true`.

  Exhausting the configured HTTP status retries raises the public typed
  `requests.exceptions.RetryError` boundary. The resolver classifies that as
  `unresolved_mirror_unavailable` without parsing nested exception text. A
  refresh with an existing nonempty durable matrix returns those exact rows as
  `ok` / `from_cache = true` and includes the availability detail; an absent or
  empty refresh cache returns the unavailable outcome. A retry exhaustion while
  fetching one non-seed sibling excludes that sibling and continues the matrix.
  A direct `YTMusicServerError` for HTTP 429 remains the distinct
  `unresolved_4xx_client` classification.
- `pipeline-cli youtube-rescue` — Submit a YouTube Music rescue ingest.
- `pipeline-cli upgrade` — Queue an exact release upgrade through its canonical web route.

## World audit scope

`pipeline-cli audit world` and `GET /api/audit/world` are thin adapters over
the same read-only service and return the same report shape. The audit checks
folder exclusivity, physical library files, imported-request membership,
current evidence addressing, and source-denylist authority. It never changes
PostgreSQL, Beets, or library files.

The report separately names temporal invariants it cannot establish from one
current-state snapshot: whether a replaced row stayed frozen after supersede,
whether an earlier operation respected a proof lock, and whether an earlier
operation widened a lossless-only search tier. Those properties remain owned
by the stateful world model; a clean live audit does not claim to prove them.

The strict audit stays strict in unattended checks: it reports every current
violation and exits nonzero whenever any exist. The separate
`cratedigger-world-audit-debt-gate` automation binary can classify that exact
JSON report against an explicitly initialized, root-owned known-debt state:

```bash
pipeline-cli audit world --json |
  cratedigger-world-audit-debt-gate --state /path/to/known-debt.json
```

The state contains schema-versioned member digests and aggregate code counts,
not request IDs, release IDs, paths, or violation text. An exact stable cohort
passes as `tracked_debt`; an exact subset passes and atomically removes the
converged members. A new member, a changed violation for a known identity,
growth, duplicate input, or unavailable/invalid state fails closed without
changing the authority state. Initialization is deliberately separate and
exclusive:

```bash
pipeline-cli audit world --json |
  cratedigger-world-audit-debt-gate \
    --state /path/to/known-debt.json --initialize
```

Initialization never replaces an existing state and is a controlled rollout
action, not a recovery path for a red gate. The downstream service owns the
production state path, root permissions, strict-audit capture, and aggregate
notification.

Authority: "A stable or shrinking known cohort should be reported as tracked
debt rather than making an otherwise successful daily run fail." —
<https://github.com/abl030/cratedigger/issues/910>

## Live-corpus render differential

`.claude/rules/test-fidelity.md` § "Rule D" requires a PR that changes
operator-facing derived text to measure the change against the real rows:
old renderer vs new, over the whole corpus, reporting changed-row counts by
changed field. `scripts/render_differential.py` is that harness. This section
owns the one step that needs live-DB access — exporting the corpus.

The corpus is **one JSON row object per line, in the exact shape the
production read seam hands the renderer** — that is the whole fidelity
claim, so it is copied from `lib/pipeline_db/download_log.py::get_log`'s
unfiltered SELECT rather than trimmed to what looks needed. Both evidence
joins are load-bearing: the candidate-evidence aliases (`_evidence_*`, which
the harness feeds to the production overlay) and the **current-evidence
aliases (`_current_evidence_*`), which `_project_current_library_have` reads
to overwrite `existing_format` and its siblings on 2,603 of 36,312 live
rows**. Dropping them does not fail — it silently renders four watched text
fields against values production never shows.

```bash
ssh doc2 'export PGPASSWORD=$(sudo cat /run/secrets/cratedigger-pgpass | grep "^PGPASSWORD=" | cut -d= -f2); pipeline-cli query --json -' <<'SQL' > /tmp/corpus.json
SELECT dl.*,
       e.format AS _evidence_source_format,
       e.min_bitrate_kbps AS _evidence_source_min_bitrate,
       e.avg_bitrate_kbps AS _evidence_source_avg_bitrate,
       e.median_bitrate_kbps AS _evidence_source_median_bitrate,
       e.lineage_version AS _evidence_lineage_version,
       e.spectral_grade AS _evidence_spectral_grade,
       e.spectral_bitrate_kbps AS _evidence_spectral_bitrate,
       e.v0_subject AS _evidence_v0_probe_kind,
       e.v0_min_bitrate_kbps AS _evidence_v0_probe_min_bitrate,
       e.v0_avg_bitrate_kbps AS _evidence_v0_probe_avg_bitrate,
       e.v0_median_bitrate_kbps AS _evidence_v0_probe_median_bitrate,
       current_evidence.id AS _current_evidence_id,
       (current_evidence.measured_at <= dl.created_at)
           AS _current_evidence_is_pre_attempt,
       current_evidence.format AS _current_evidence_format,
       current_evidence.min_bitrate_kbps AS _current_evidence_min_bitrate,
       current_evidence.avg_bitrate_kbps AS _current_evidence_avg_bitrate,
       current_evidence.median_bitrate_kbps AS _current_evidence_median_bitrate,
       current_evidence.spectral_grade AS _current_evidence_spectral_grade,
       current_evidence.spectral_bitrate_kbps AS _current_evidence_spectral_bitrate,
       current_evidence.v0_subject AS _current_evidence_v0_probe_kind,
       current_evidence.v0_min_bitrate_kbps AS _current_evidence_v0_probe_min_bitrate,
       current_evidence.v0_avg_bitrate_kbps AS _current_evidence_v0_probe_avg_bitrate,
       current_evidence.v0_median_bitrate_kbps AS _current_evidence_v0_probe_median_bitrate,
       origin.beets_distance AS original_beets_distance,
       ar.album_title, ar.artist_name, ar.mb_release_id,
       ar.year, ar.country, ar.status AS request_status,
       ar.min_bitrate AS request_min_bitrate,
       ar.prev_min_bitrate, ar.search_filetype_override,
       ar.source AS request_source
FROM download_log dl
LEFT JOIN album_quality_evidence e ON e.id = dl.candidate_evidence_id
LEFT JOIN download_log origin ON origin.id = dl.source_download_log_id
JOIN album_requests ar ON dl.request_id = ar.id
LEFT JOIN album_quality_evidence current_evidence
    ON current_evidence.id = ar.current_evidence_id
WHERE dl.id > 0 AND dl.id <= 4000
ORDER BY dl.id;
SQL
```

**Export the whole table, not a filtered slice.** The classify target's
`prepare` pass indexes every row that points back at another through
`source_download_log_id`, so a successor import outside the export would
silently drop the linked-evidence back-fill that production performs.

`pipeline-cli query --json` prints one indented JSON array, so a whole-table
export is hundreds of megabytes in a single payload. **Batch it by id** — the
`WHERE dl.id > … AND dl.id <= …` window above, stepped across `min(id)` to
`max(id)` — and concatenate the batches into the JSONL the harness reads:

```bash
nix-shell --run "python3 -c '
import glob, json
with open(\"/tmp/corpus.jsonl\", \"w\") as out:
    for path in sorted(glob.glob(\"/tmp/corpus-batch-*.json\")):
        for row in json.load(open(path)):
            out.write(json.dumps(row, separators=(\",\", \":\")) + \"\n\")
'"
```

Batch order does not matter: the differential keys rows by id, and it fails
closed if the two sides ever cover different rows.

This is a read query. Never use `--write` for corpus export. The pipeline DB
subnet is doc2-local, so the export runs there and the render runs wherever
the code is.

Then follow the three render/render/diff commands in
`.claude/rules/test-fidelity.md` § "Rule D".
