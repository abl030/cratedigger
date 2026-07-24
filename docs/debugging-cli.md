# Pipeline CLI operator reference

`pipeline-cli` is Cratedigger's writable operator and agent control plane. Its
typed subcommands are the normal interface for routine lifecycle, import,
destructive, and repair actions; each mutation follows the shared service/API
contract. `pipeline-cli routes` (or `pipeline-cli routes --json`) discovers the
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
- `import-job-recovery --resolution close` records an explicitly reconciled
  operation without replay. Use `--resolution retry` only after proving Beets
  did not apply the prior operation; both require an audit reason.
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

## Command capability surface

- `pipeline-cli add` — Add a MusicBrainz or Discogs request.
- `pipeline-cli audit world` — Read-only PipelineDB, Beets, evidence, and disk coherence audit.
- `pipeline-cli ban-source` — Remove a server-resolved bad rip and requeue its request when appropriate.
- `pipeline-cli beets-distance` — Measure a rejected download against an exact release.
- `pipeline-cli disk-coverage` — Compare active pipeline rows with Beets library coverage.
- `pipeline-cli force-import` — Queue a rejected download for the importer lane.
- `pipeline-cli import-job-recovery` — Close or retry an explicitly reconciled ambiguous Beets operation.
- `pipeline-cli import-jobs` — List import queue jobs.
- `pipeline-cli import-preview` — Inspect an import preview and its evidence inputs.
- `pipeline-cli library-delete` — Delete one exact server-owned Beets album.
- `pipeline-cli list` — List album requests.
- `pipeline-cli long-tail` — Show the wanted long-tail worklist.
- `pipeline-cli quality` — Simulate quality decisions and replay current candidate evidence.
- `pipeline-cli query` — Run one read-only SQL statement, or the explicit write escape hatch.
- `pipeline-cli repair-spectral` — Repair stale spectral state.
- `pipeline-cli replace` — Supersede a request with another exact pressing in its release family.
- `pipeline-cli routes` — Discover every parser command, argument, and description.
- `pipeline-cli search-plan advance` — Advance one persisted search-plan cursor.
- `pipeline-cli search-plan dry-run` — Generate a request plan without persisting it.
- `pipeline-cli search-plan history` — Read cursor-paginated per-request search history.
- `pipeline-cli search-plan regenerate` — Regenerate one persisted request plan.
- `pipeline-cli search-plan saturation` — Show recent search-plan saturation and pre-filter skips.
- `pipeline-cli search-plan show` — Show one request's plan, cursor, items, and provenance.
- `pipeline-cli set` — Apply a typed request lifecycle transition.
- `pipeline-cli set-intent` — Set lossless-on-disk intent.
- `pipeline-cli show` — Show a request, attempts, and quality state.
- `pipeline-cli status` — Show request counts by lifecycle status.
- `pipeline-cli triage list` — List a named triage cohort.
- `pipeline-cli triage quarantine` — Read-only unreferenced immediate quarantine-folder scan.
- `pipeline-cli triage show` — Compose per-request unfindable, field, and search forensics.
- `pipeline-cli wrong-match-delete` — Delete one visible Wrong Matches source folder.
- `pipeline-cli wrong-match-delete-group` — Delete visible Wrong Matches folders for one request.
- `pipeline-cli wrong-match-triage` — Converge the full Wrong Matches queue using persisted evidence.
- `pipeline-cli youtube-album` — Resolve a release to the YouTube Music album matrix.
- `pipeline-cli youtube-rescue` — Submit a YouTube Music rescue ingest.

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
