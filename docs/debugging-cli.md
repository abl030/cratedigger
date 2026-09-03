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

- `pipeline-delete`, `set-quality`, `upgrade`, `wrong-match-converge`,
  `merge-rekey`, `sync-file-tags`, `library-census-refresh`, `resolve-rg`,
  `wrong-match-delete`, `wrong-match-delete-group`,
  `wrong-match-triage`, `wrong-match-triage-cancel`, `replace`,
  `force-import`, `import-local`, `beets-distance`,
  `import-preview --download-log-id`, and `triage quarantine` connect to
  `/run/cratedigger-web/web.sock`. The caller must be a
  member of `services.cratedigger.web.accessGroup` (default
  `cratedigger-web`). That membership grants complete local HTTP/API authority,
  not merely permission to execute those subcommands.

  Everything after `resolve-rg` in that list joined it in issue #1063: each one
  reads or destroys a path under the private `0700 cratedigger:users`
  processing tree, which only the service identity can traverse. Executed in
  the invoking operator's own process they reported intact folders as missing,
  cleared their Wrong Matches pointers, and claimed success. Routing them
  through the socket makes ONE identity own those filesystem facts. There is no
  direct-database fallback: if the socket is unreachable the command exits 5
  and changes nothing. `triage quarantine` joined the cohort in issue #1122
  F1: it only READS the processing tree rather than destroying anything, but
  the identity problem is the same — run in the operator's own process the
  scan raised `EACCES` and killed the whole view, including the
  download-dir-rooted roots the operator could otherwise have read.
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
- `import-job-recovery show` prints read-only exact-owner, liveness, completion,
  library, and cleanup evidence. It remains useful for historical
  `recovery_required` rows, which startup convergence handles automatically
  once the exact persisted execution is positively proven dead.
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

`pipeline-delete`, `set-quality`, `upgrade`, `wrong-match-converge`,
`merge-rekey`, `sync-file-tags`, `library-census-refresh`, `resolve-rg`,
`wrong-match-delete`, `wrong-match-delete-group`,
`wrong-match-triage`, `wrong-match-triage-cancel`, `replace`,
`force-import`, `import-local`, `beets-distance`, and
`import-preview --download-log-id` call the canonical web route over the
module-owned Unix socket.
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

Valid JSON route responses, including 5xx responses, are relayed on stdout —
except for the commands that kept their own text/`--json` presentation
(`wrong-match-delete`, `wrong-match-delete-group`, `wrong-match-triage`,
`replace`, `force-import`, `import-local`, `beets-distance`, `import-preview`), which render
the route's payload exactly as they did when they executed in-process.
Any 2xx exits 0; 404 exits 2; 400/422 exits 3; 409 exits 4; other statuses
exit 5. Per-command overrides preserve exit codes those commands already had:
the two Wrong Matches delete commands and `beets-distance` map 500 to exit 1,
and `beets-distance` additionally maps its own 410 Gone (`folder_missing` /
`no_audio`) to exit 4.
Each routed command carries its own request deadline sized to the work the
route performs — an enqueue is 15s, a source delete 300s, a group delete 900s,
Replace 300s (inline mirror lookups), a download-log preview 900s (inline
snapshot + measurement), and a beets distance 180s. `wrong-match-triage` starts
the canonical background sweep and then follows
`/api/wrong-matches/triage/status` to completion, so it still blocks and prints
the whole summary; the sweep itself is deliberately unbounded, and a few
consecutive failed status polls are retried before the follow gives up.

**`wrong-match-triage` can be cancelled with Ctrl-C (issue #1083).** The sweep
runs on a background thread inside `cratedigger-web`; the command only follows
it — so Ctrl-C is caught and turned into one `POST
/api/wrong-matches/triage/cancel`, the exact same route the web UI's Stop
button uses, then the CLI keeps following `/api/wrong-matches/triage/status`
to print whatever terminal state actually lands. There is no direct call and
no direct-DB fallback. Cancellation is observed BETWEEN rows inside
`cleanup_all_wrong_matches`, never mid-delete, so a row already in flight when
you hit Ctrl-C always finishes normally; only the next row is skipped. The
final state is `cancelled`, distinct from `completed` and `failed`, and its
`summary` reports exactly what ran before the stop — nothing already deleted
is rolled back. A cancel that races a sweep which is already finishing, or one
sent with no sweep running, both just return the current status; neither is an
error. A cancel served while the CLI's OWN start POST is still in flight —
before the server's `start()` has even flipped the sweep to `running` — used
to silently no-op and let that sweep run unstoppable by this invocation; it
is now STICKY (issue #1106), but only because this handler is the ONE
caller that sends `{"arm_pending": true}` on the cancel POST — every other
caller (the web UI's Stop button, `wrong-match-triage-cancel` below) stays
unarmed and is a pure no-op here, on purpose (see the next paragraph). An
armed cancel is recorded server-side, and the very next `start()` admitted
within a bounded (order-of-ten-second) window consumes it — still admitted
(still 202/`running`) but pre-cancelled before any row runs, so it lands
`cancelled` with zero rows processed. Every armed cancel REFRESHES that
recorded timestamp (latest-wins, not first-wins — review round 1 found a
first-wins slot lets a later, genuinely-in-window cancel be silently
dropped once an earlier one had gone stale-but-unconsumed), and any
`start()` clears it whether consumed or expired, so a cancel of an
already-finished sweep that nothing starts again within the window just
expires — it does not poison whatever gets started next, unrelated, later.
A second Ctrl-C while the CLI is still waiting for that final status
falls through to the ordinary uncaught `KeyboardInterrupt` and detaches the
terminal — the sweep (and any cancel already in flight) keeps running
server-side regardless. Its `--json` output is the STATUS envelope (`state`,
`started_at`, `finished_at`, `summary`, `error`), not the bare summary the
in-process command printed; the counts live under `summary`. A cancel POST
that fails outright (refused socket, timeout, or a route-level 500) gets one
retry before the CLI gives up and says so on stderr, then still falls through
to following the sweep's status — a swallowed failure would otherwise have
the CLI claim it stopped the sweep while the whole remaining queue kept
deleting underneath it.

**`wrong-match-triage-cancel` reaches the same cancel route directly, with no
attached sweep to poll.** `Ctrl-C` only helps when an operator is still
attached to the terminal that started the sweep; a sweep started over SSH
whose connection then drops (SIGHUP, not `KeyboardInterrupt`) keeps running
with nothing left to catch a signal, and a fresh `wrong-match-triage --apply`
on reconnect just 409s against the still-running sweep (issue #1083 review).
`wrong-match-triage-cancel` is the same `POST /api/wrong-matches/triage/cancel`
the Ctrl-C handler and the web UI's Stop button use, called directly with no
polling of its own — run it, then a separate `wrong-match-triage --apply` (or
the status route) picks up the terminal state. Always exits 0: the route
itself never refuses, whether or not a sweep happens to be running. It sends
NO `arm_pending` (defaults false, issue #1106): it has no start POST of its
own to race, so it must stay a pure no-op when nothing is running — arming
here would risk the fresh `wrong-match-triage --apply` this sequence
recommends next landing pre-cancelled before it ever processes a row.

Locally generated transport/protocol failures (including a missing or
unreachable socket, malformed development origins, redirects, or non-object
JSON responses) exit 5 with a structured error on stderr and never fall back.
`pipeline-delete ID --confirm DELETE` and
`wrong-match-converge ID THRESHOLD_MILLI --apply` make no HTTP call when the
local intent gate is missing. Those words/flags are intent checks layered
inside socket authorization, never credentials.

## Command capability surface

- `pipeline-cli add` — Add a MusicBrainz or Discogs request.
- `pipeline-cli audit retag-divergence` — Read-only census of albums whose
  Beets DB `mb_albumid` moved but whose installed file tags did not (the
  retag `-W` residual, #1093 item 1). Accepts `--after-album-id` to resume
  a truncated scan. Exit 0 iff `status="clean"`; exit 1 iff
  `"divergence_found"`; exit 4 iff `"incomplete"`; exit 5 iff
  `"beets_unavailable"`. Full mapping and rationale: § "Retag divergence
  audit scope" below.
- `pipeline-cli audit retag-divergence-album` — Cheap, explicit per-album
  retag-divergence recheck (#1142): the same classifier/tag-reader as
  `audit retag-divergence`, over exactly one album's own files — no
  deadline, no cursor, milliseconds not minutes. `<album_id>` positional.
  Exit 0 iff the album was found (any class, including `agrees` — this is
  an interactive lookup, not a health-check gate); exit 2 iff no album with
  that id exists; exit 5 iff Beets is unavailable or an unexpected
  transport/decode/render defect occurs. Full detail: § "Retag divergence
  audit scope" below.
- `pipeline-cli audit world` — Read-only PipelineDB, Beets, evidence, and disk coherence audit.
- `pipeline-cli ban-source` — Remove a server-resolved bad rip and requeue its request when appropriate.
- `pipeline-cli beets-distance` — Measure a rejected download against an exact
  release. A folder that could not be observed or read reports
  `folder_unavailable` (exit 5), never `folder_missing`/`no_audio`. When only
  PART of the folder could be read, the outcome stays `ok` and the render
  prints a `PARTIAL READ` block naming the refusal, with honest per-reason
  wording (#1086): a world failure (EACCES/EIO/ESTALE/etc.) reads "could not
  be read, may be transient"; a containment refusal (a symlink or a
  socket/device node) reads "refused rather than followed/opened" and is
  never worded like a flaky disk. Either way the distance is real but it was
  computed over fewer local tracks than the album holds (#1063). The wire
  response also carries a structured `partial_read_is_containment` boolean
  for consumers that need to branch on the kind rather than the free text.
  A refusal counts whether it fires when the file is OPENED (EACCES on the
  private tree), MID-READ (the EIO/ESTALE this deployment's nested virtiofs
  produces on an already-open descriptor — the two carry different evidence,
  and only the second occurs on the live mount), or via the `lstat` guard
  that refuses every symlink AND every non-regular name (a FIFO, socket, or
  device node) BEFORE beets ever touches the path — a symlink loop, a
  symlink to a real file outside the quarantine root, and a dangling symlink
  are ALL refused identically, whatever they point to (#1086; before this
  guard a dangling symlink read as a proven absence only because `os.stat`
  followed it into `ENOENT`, a symlink to a real external file was silently
  followed and fingerprinted, and a `*.flac` FIFO with no writer hung the
  request thread forever rather than being refused). A symlinked
  SUBDIRECTORY gets the same refusal at the walk level, not only a symlinked
  file: `os.walk` silently declines to descend into one, so it is refused
  explicitly rather than reported as zero audio files. The rule still runs
  both ways: a name the errno PROVES is gone (`ENOENT`/`ENOTDIR` on a name
  that is not itself a symlink — e.g. a file unlinked after the walk listed
  it) is not a refusal, does not set `partial_read`, and leaves a folder
  holding only such names as `no_audio`, because that folder was observed
  and read. That matches what the Wrong Matches file explorer reports for
  the same on-disk entry (both ends ask
  `lib.fs_authority.errno_proves_absence`).
- `pipeline-cli disk-coverage` — Compare active pipeline rows with Beets library coverage; each off-disk row reports whether its exact identity is `missing` or `ambiguous`.
- `pipeline-cli force-import` — Queue a rejected download for the importer lane.
- `pipeline-cli import-local` — Import a folder already on disk (`<request_id> <source_path>`) against a request's exact release (issue #1176); strict pressing-identity validation, no relaxed-threshold escape hatch — a candidate that fails lands as an ordinary Wrong Matches row.
- `pipeline-cli import-job-recovery show` — Show read-only exact evidence for one import job.
- `pipeline-cli import-jobs` — List import queue jobs.
- `pipeline-cli import-preview` — Inspect an import preview and its evidence inputs.
- `pipeline-cli library-delete` — Delete one exact server-owned Beets album.
- `pipeline-cli list` — List album requests.
- `pipeline-cli long-tail` — Show the wanted long-tail worklist. Exact-authority
  conflicts emit the shared typed error and exit 4; expected Beets authority
  unavailability emits the retryable typed error and exits 5. Neither condition
  is rendered as an actionable Missing cohort.
- `pipeline-cli merge-rekey` — Rekey an imported request onto the MusicBrainz
  merge survivor Beets already holds, through its canonical web route.
  Request-ledger-only; never mutates Beets. Refuses `not_merged` (422) when
  MusicBrainz ANSWERED and names no redirect for the stored id (e.g. request
  #8792, Slipknot Vol. 3 — two current albums, no merge) — distinct from
  `mirror_unavailable` (503), which means no answer was obtained at all
  (unconfigured, or the mirror is unreachable); a down mirror is never read
  as MusicBrainz confirming the request was never merged.
  `library_not_at_survivor` (409) unless Beets resolves exactly one album at
  the survivor first, and `library_still_at_stored` (409) unless Beets
  resolves NOTHING at the merged-away id — "the survivor is occupied" alone
  does not witness the library actually moved, nor that the album occupying
  it is this request's own: the survivor adoption must be witnessed against
  the request's linked current evidence, or the request refuses
  `evidence_fingerprint_mismatch` (409) — no linked evidence at all, a
  linked evidence row that no longer exists, unreadable survivor files, a
  survivor album that walked cleanly but has zero audio files (vanished or
  genuinely empty — not witnessable), or a freshly computed content
  fingerprint that genuinely does not match all produce this outcome; an
  unrelated, pipeline-untracked album can otherwise sit at the merged MBID
  and the merge-rekey action would transplant this request's proof onto
  bytes nobody measured for it, so the operator decides in every case.
  `survivor_collision` (409)
  when a rival request or a colliding evidence fingerprint already occupies
  the survivor (an operator must resolve it directly — retrying cannot
  help); `rekey_refused` (409) covers the write's own refusal, with two
  differently-worded causes (#1089 MINOR-4, review round 2): a
  PRE-EXISTING queued/running import job — the most ordinary cause, where
  nothing raced and the message says wait for it to finish rather than
  retry — or a genuine race (the request's own status/owner/identity
  changed concurrently, or a rival took the survivor in the same instant),
  where the message says retry. `beets_unavailable` (503) is a classified
  Beets SQLite authority failure.
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
- `pipeline-cli mark-incomplete` — Set/clear the operator's incomplete mark
  on a request (`--clear` to unmark). Marked, the quality decider disregards
  the installed copy for any candidate beets proves whole, so a complete
  candidate is admitted as into an empty slot; a covered terminal acceptance
  clears the mark automatically (issue #1241).
- `pipeline-cli library-census-refresh` — Request an out-of-schedule
  library-completeness census run through its canonical web route: writes
  the trigger file the module's census path unit watches (the daily
  oneshot stays the single execution path). The dashboard card's
  "Run census now" button is the same route. 200/0 requested; 503/5
  unconfigured or state dir unwritable.
- `pipeline-cli set-quality` — Set request quality through its canonical web route.
- `pipeline-cli show` — Show a request, attempts, and quality state.
- `pipeline-cli sync-file-tags` — Write one
  Beets album's file tags from its DB identity through its canonical web
  route (#1260): the heal for the merge retag's `-W` residual the
  `audit retag-divergence` census surfaces. Compare-and-set: refuses
  `identity_mismatch` (409/4) when the album no longer names the expected
  identity; `already_synced` (200/0) writes nothing; `synced` (200/0) is
  claimed only after re-reading the files agree; `residual_divergence`
  (409/4) reports a write whose re-read files still disagree, with per-item
  detail; `release_locked`/`beets_unavailable` (503/5) are retryable.
- `pipeline-cli status` — Show request counts by lifecycle status.
- `pipeline-cli triage list` — List a named triage cohort.
- `pipeline-cli triage quarantine` — Read-only unreferenced immediate quarantine-folder scan through its canonical web route.
- `pipeline-cli triage show` — Compose per-request unfindable, field, search,
  and provisional-lossless convergence forensics. A converged row prints the
  distinct peer/snapshot/codec counts, raw cliff range and spread, and opaque
  signal token needed by the stop command while status is `wanted`. An
  `unsearchable` signal prints the canonical `pipeline-cli set ID wanted`
  resume command instead of an unusable stop command.
- `pipeline-cli triage stop` — Explicitly stop a still-wanted request after
  reviewing its current
  convergence signal. One PostgreSQL snapshot rederives and compares the full
  token before changing the request to reversible `unsearchable`; it never
  accepts or proves the holding. Database unavailability exits 5 without a
  mutation, including failure while constructing the database connection.
  Malformed tokens fail at argparse before any database connection.
  Usage: `pipeline-cli triage stop ID --signal-token TOKEN --confirm STOP`.
- `pipeline-cli wrong-match-delete` — Delete one visible Wrong Matches source folder.
- `pipeline-cli wrong-match-converge` — Converge one Wrong Matches request through its canonical web route.
- `pipeline-cli wrong-match-delete-group` — Delete visible Wrong Matches folders for one request.
- `pipeline-cli wrong-match-triage` — Converge the full Wrong Matches queue using persisted evidence.
- `pipeline-cli wrong-match-triage-cancel` — Request cancellation of the in-flight Wrong Matches triage sweep, if any.
- `pipeline-cli youtube-album` — Resolve a release to the YouTube Music album
  matrix directly through the shared resolver service; accepts
  `<identifier> [--refresh] [--watch-url YOUTUBE_URL] [--json]`, with human-readable output by default
  and the full result available as JSON. It uses the configured database and
  mirrors and remains available when `web.enable = false`; it does not use the
  optional web Unix socket or fall back to an HTTP adapter when that socket is
  absent. Only a non-refresh lookup by an already-cached MusicBrainz
  release-group identifier can return `ok` with `from_cache = true` without
  contacting a mirror or YouTube Music. A Discogs lookup must consult the
  configured mirror first because release IDs and master IDs share the integer
  namespace; after the mirror establishes the master, the normal post-widen
  durable-cache read may return `ok` with `from_cache = true`.
  `--watch-url` accepts public HTTPS video and playlist URLs on `youtube.com`,
  `www.youtube.com`, and `music.youtube.com`. A `list` parameter selects the
  complete ordered playlist even when the URL also carries `v`; without
  `list`, the exact video resolves to its album browse ID. The selected result
  replaces the complete persisted matrix. Rescue still receives only the
  persisted browse/playlist ID; the raw operator URL never reaches rescue or
  yt-dlp.

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
folder exclusivity, physical library files, library-root containment,
imported-request membership, current evidence addressing, and
source-denylist authority. It never changes PostgreSQL, Beets, or library
files.

The report separately names temporal invariants it cannot establish from one
current-state snapshot: whether a replaced row stayed frozen after supersede,
whether an earlier operation respected a proof lock, and whether an earlier
operation widened a lossless-only search tier. Those properties remain owned
by the stateful world model; a clean live audit does not claim to prove them.

The public report groups every finding by the authority that owns it:

| Bucket | Owner | Codes |
|---|---|---|
| A | Cratedigger integrity | `proof_lock_broken`, `lossy_tier_widened`, `denylist_without_authority`, `current_evidence_dangling`, `evidence_release_mismatch`, `evidence_capture_path_missing`, `request_identity_missing` |
| B | Current-holdings projection health | `current_beets_missing`, `current_beets_ambiguous`, `current_beets_authority_unavailable`, `evidence_fingerprint_mismatch`, `evidence_link_without_album`, `current_evidence_missing`, `album_fingerprint_unavailable` |
| C | Beets/library health | `album_empty`, `item_outside_album_folder`, `folder_shared`, `album_folder_missing`, `album_item_missing`, `beets_identity_missing`, `album_folder_outside_library_root`, `album_item_outside_library_root` |

Machine-readable output has `status` (`clean`, `observations_only`, or
`integrity_failed`), `complete`, separate `counts.bucket_a|bucket_b|bucket_c`,
and `groups.a|b|c`, each with `bucket`, `owner`, `count`, and `members`. There
is no public flat violations list or all-findings alarm count. A COMPLETE
clean or B/C-only report exits 0 from the CLI and returns HTTP 200 from the
API — Bucket B/C observations are surfaced without failing the command. Any
A finding makes the CLI exit 1 while the API still returns HTTP 200 with
`status=integrity_failed`, preserving all B/C observations beside it.

An expected inability to open or query the canonical Beets authority —
`complete=false`, the Bucket B `current_beets_authority_unavailable`
observation — is a non-successful CLI exit 5 / HTTP 503 (issue #1355 item 4):
the audit never actually ran, so exit 0 there would let a cron, systemd unit,
agent, or `&&` chain read "audit completed cleanly" from a run that lacked
authority. This used to be CLI 0 / HTTP 200, a pre-existing deviation from
`pipeline-cli audit retag-divergence`'s own `beets_unavailable` -> 5/503
convention (below); the deviation is closed. An unexpected schema, decoder,
invariant, programming, close, or serialization defect remains a transport
failure: CLI exit 5 or HTTP 503, same as the Beets-unavailable case, though
programmatic callers should not need to distinguish the two —
`groups.b.members` names `current_beets_authority_unavailable` when the cause
is availability, and the `error` field is set when it is a transport defect.
Incompleteness is never evidence that an album is absent.

The separate `cratedigger-world-audit-debt-gate` automation binary applies a
stricter exact-known-cohort policy to a complete report. It may reject a new
B/C observation even though `pipeline-cli audit world` itself exits 0, and it
never lets an incomplete report converge tracked debt. It classifies the JSON
against an explicitly initialized, root-owned known-debt state:

```bash
pipeline-cli audit world --json |
  cratedigger-world-audit-debt-gate --state /path/to/known-debt.json
```

The state contains schema-versioned member digests and aggregate code counts,
not request IDs, release IDs, paths, or violation text. An exact stable cohort
passes as `tracked_debt`; an exact subset passes and atomically removes the
converged members. For every **gated** code, a new member, a changed violation
for a known identity, growth, duplicate input, or unavailable/invalid state
fails closed without changing the authority state.

`lib/world_audit_debt.py::NON_GATING_VIOLATION_CODES` names the codes this gate
reports but never gates on and never tracks as debt — currently just
`evidence_fingerprint_mismatch`. Its members are excluded from the known
cohort, from convergence accounting, and from the pass/fail decision, and a
state written before a code joined that set is rebaselined on load, so no
operator one-shot is needed. `strict_violations` still counts them, and
`non_gating_violations` / `non_gating_by_code` name the resulting gap. This
is a per-code list, not a bucket rule: `current_beets_missing` shares bucket B
and still fails the gate. The daily wrapper's `jq` protocol filter projects a
fixed key set and therefore does not yet surface the two `non_gating_*` fields;
`pipeline-cli audit world --json` lists every such violation in full.

A stale evidence fingerprint means a persisted evidence row describes an older
byte state of an album directory. Every decision consumer reauthorizes the
installed snapshot at action time and rebuilds rather than failing closed, so
it cannot produce a wrong import decision — see `docs/quality-verification.md`
§ "Evidence addressing, propagation, and ownership" and CLAUDE.md invariant 12.

Authority: "stale evidence will -always- auto heal at import time. so it sinoyk
does not matter. its not something we need tit rsck and we shoukd just teach
the workd audit thing ti ignire it." (operator, 2026-08-21) —
<https://github.com/abl030/cratedigger/issues/1233#issuecomment-5363639933>

Initialization is deliberately separate and exclusive:

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

## Retag divergence audit scope

`pipeline-cli audit retag-divergence` and `GET /api/audit/retag-divergence`
are thin adapters over the same read-only `lib/retag_divergence_audit.py`
service and return the same report shape. It never changes PostgreSQL,
Beets, or library files.

The import-time MusicBrainz merge retag (`lib/beets_retag.py`) runs
`beet modify -a -M -W -y`. `-W` is deliberate and stays: it keeps the retag
to one `Album.store()` transaction instead of a partial per-file write race.
The accepted cost is that a successful retag moves the Beets DB's
`mb_albumid` without touching any installed file's tag — so after a
successful retag whose subsequent import rejects, the DB names the survivor
while every installed file still carries the merged-away id. This audit is
the cohort-wide instrument for that residual (issue #1093 item 1): it reads
every Beets album's own DB `mb_albumid` beside each item's installed file
tag and reports every album where they disagree.

**Path containment is LEXICAL, not symlink-resolving.**
`lib/beets_db.py::BeetsDB.list_album_mb_identities` verifies every stored
item path stays inside the configured `library_root` by path-STRING
comparison (`os.path.abspath` + `os.path.commonpath`), covering both a
relative and an already-absolute stored path — unlike
`resolve_current_releases`, which only checks the relative case. This is a
deliberate match to that pre-existing mechanism, not a stricter
`os.path.realpath` check: a symlink INSIDE the library root that points
outside it is CONTAINED by this test and gets opened; a library root that
is itself a symlink refuses an absolute path naming the real target. A path
that fails this lexical check (or cannot be resolved at all, including with
no configured root) is never opened — it is reported `unreadable` with a
fixed detail distinguishable from a genuine read/parse failure's exception
text (`lib/retag_divergence_audit.py::REFUSED_OUTSIDE_LIBRARY_ROOT_DETAIL`).
Live evidence for why this matters: one real album's 51 items were stored
as absolute paths under the private `processing/albums/` tree, entirely
outside the library root — the CLI (operator identity) and the API
(service identity) have different read permissions there, so without
containment the two "symmetric" surfaces could report differently for the
same library.

Per-item classification: `agrees` (not reported), `diverges` (the DB names
an identity the file tag does not match, including a blank file tag — the
`-W` residual shape), `file_tag_present_db_absent` (the DB has no
`mb_albumid` at all but the file tag still carries one — the #570 Discogs
neutralization shape, expected near-zero and never filtered), and
`unreadable` (the file was never verified to agree — a genuine read
failure or a refused out-of-root path; fail closed either way, never
counted as agreeing). Per-album `album_class` is a DISPLAY aggregate over
its items by fixed precedence — `unreadable` > `diverges` >
`file_tag_present_db_absent` > `agrees` — plus `empty` for a real zero-item
album row. Only non-agreeing albums are listed; full counts
(`albums_scanned`, `items_scanned`, `items_refused`, `items_unreadable`,
and one count per non-agreeing class) are always reported. `items_scanned`
counts every item path considered — read AND refused; `items_refused` is
the subset never opened at all; `items_unreadable` is the superset of
`items_refused` that also includes genuine read/parse failures.

**`status` is independent of the per-album display class.** An album whose
display class reads `unreadable` (because unreadable outranks everything)
can still contain a genuine `diverges` item, and the report's headline
answer must not miss that — so `albums_diverging` and
`albums_file_tag_present_db_absent` are independent presence counts ("this
album contains at least one such item"), never gated by which class won
that album's display precedence. `status` is `divergence_found` (at least
one album contains a genuine `diverges` or `file_tag_present_db_absent`
item — decided independent of precedence and of any unreadable/empty
finding elsewhere, and takes priority over every condition below,
including a resume cursor being set); `incomplete` (no genuine divergence
anywhere, but this call could not vouch for the WHOLE library — an
unreadable/refused/empty-only finding; the scan hit its time deadline
before finishing (`complete=false`); OR this call started from a resume
cursor, i.e. `after_album_id` was given on input — this is NOT "clean",
whatever the scanned range itself looked like); or `clean`, which requires
ALL THREE of: nothing listed, `complete=true`, AND `after_album_id is
None` on input — the scan actually answered the whole-library question, on
its own, without help from any other call (#1093 review round 5, finding
1: a resumed call that completes cleanly over the range it scanned still
cannot vouch for whatever a cursor skipped, so it is never allowed to
report `clean`). `beets_unavailable` covers the Beets authority itself
being unopenable or unqueryable.

**The API route bounds ONLY its per-album read LOOP to
`API_SCAN_DEADLINE_SECONDS`** (40s, `web/routes/retag_divergence_audit.py`)
**— not the whole request, and the CLI is always the full, unbounded
census.** `beets.list_album_mb_identities()` (one DB fetch before the loop
starts; ~3.2s measured live) and the JSON encode of the result both run
UNBOUNDED, outside this timer — the real measured route time for a 40.0s
deadline was ~41.9-43.2s. A fully UNBOUNDED census over the live
~93k-item library took ~196s, which is why the loop is bounded at all: the
deployed vhost's reverse proxy has no configured `proxy_read_timeout`, so
it falls back to nginx's 60s default and would 504 while the backend kept
scanning past it. A bounded scan that runs out of loop time reports
`complete=false` over exactly the albums it reached — the report SHAPE
never changes, and `albums_scanned`/`items_scanned`/etc. describe only
that reached prefix. `tests/web/test_routes_retag_divergence_audit.py::
TestApiScanDeadlineConstant` pins that the deadline leaves REAL margin
under nginx's default, not merely `> 0`.

**`?after_album_id=N` (CLI: `--after-album-id`) resumes a truncated scan**
— pass the prior response's `next_after_album_id` to continue the census
past where it stopped, chaining calls until `next_after_album_id` comes
back `null`/`None`. No SINGLE response in that chain — not even the one
that reaches the end — is itself allowed to report `clean` (see `status`
above); the CALLER accumulates the whole-library verdict across the whole
chain instead: start at `after_album_id=None`, keep resuming with each
report's `next_after_album_id` until it comes back `None`, and conclude
"library-wide clean" only if EVERY page in that chain reported no
divergence (#1093 review round 5, finding 1 — a single bounded response
can only ever vouch for the range it itself scanned). The report echoes
its own input cursor back as `after_album_id`, so a caller (or an
auditor reading a stored response) can always tell which shape produced
it. `next_after_album_id` also satisfies `complete == (next_after_album_id
is None)` in every case, including a scan truncated before reaching even
one album, AND a `beets_unavailable` report (round 5, finding 3; round 6,
finding 2 — a caller resuming across a transient `SQLITE_BUSY`/
`SQLITE_LOCKED` failure gets the same cursor back, not a bare `null` that
reads as "done") — it is never left `null` while `complete` is `false`.

`after_album_id` accepts only a plain nonnegative ASCII-digit string —
deliberately narrower than Python's bare `int()`, which silently accepts
a leading sign, underscore digit-grouping (`"1_0"` → `10`), surrounding
whitespace, and non-ASCII digit characters. A cursor is a read-only
replay value, not user arithmetic, so a malformed or reinterpreted cursor
is refused (`400`/CLI argparse error) rather than silently resolved to a
different album id than was typed
(`lib/retag_divergence_audit.py::parse_after_album_id_cursor`, shared by
both the CLI and the API — #1093 review round 5, finding 5).

Machine-readable output has `status` (`clean`, `divergence_found`,
`incomplete`, or `beets_unavailable`), `complete`, `counts`, `albums`
(only the non-agreeing ones, each with every item's classification),
`after_album_id` (echoes the cursor this call was given — `null` iff it
started from the true beginning), and `next_after_album_id` (`null` iff
`complete`; otherwise the cursor to resume from). **Exit/status-code
mapping** follows `.claude/rules/code-quality.md` § CLI ⇄ API Surface
Symmetry's convention table: `clean` → `0`/`200` (the audit ran and the
cohort really is empty); `divergence_found` → `1`/`200` (a genuine finding,
the one thing this instrument exists to surface); `incomplete` → `4`/`409`
("wrong state" — the world blocked a complete answer, so a caller must
never read this as "no divergence" the way a bare `0`/`200` would invite);
`beets_unavailable` → `5`/`503` (transient/retryable — the audit never
actually ran at all, so `0`/`200` there would let a cron or
`&& echo "cohort empty"` read "no divergence" from a report that answered
nothing). Only `clean` means "I answered the question and the cohort is
empty" — every other status means either a real finding or an incomplete
answer, and none of the three non-clean statuses may be silently read as
success. `pipeline-cli audit world` / `GET /api/audit/world` used to exit
`0`/`200` for their own analogous beets-unavailable bucket, a pre-existing
deviation from this same documented convention; issue #1355 item 4 closed
it, so both siblings now exit `5`/`503` for their beets-unavailable case
alike. World audit has no `incomplete` counterpart: `build_world_audit_report`
has exactly one producer of `complete=false` (`_unavailable_beets_report`),
so its only incomplete cause is Beets being unavailable outright, unlike
retag divergence's own `incomplete`, which can also come from per-album
unreadable/empty/refused-only findings with no deadline involved at all. An
unexpected schema, decoder, invariant, programming, close, or serialization
defect remains a transport failure: CLI exit 5 or HTTP 503. A downstream
reader closing a CLI pipe early (e.g.
`pipeline-cli audit retag-divergence | head`, or any consumer that exits
before reading anything) exits 0, never 120. stdout to a pipe is
block-buffered, not flushed per `print()` call, so a `BrokenPipeError` from
an early-closing reader can surface two different ways depending on
payload size: a large single write can raise it synchronously, DURING the
`print()` call (already caught by an ordinary `except BrokenPipeError`);
a small one can complete inside Python's own buffer with no exception at
all, deferring the actual OS-level failure to Python's automatic
interpreter-shutdown flush — which happens AFTER the function has already
returned and OUTSIDE every `except` clause in it, printing "Exception
ignored while flushing sys.stdout" and exiting the whole process 120
regardless of any `sys.exit(rc)` already requested (#1093 review round 5,
finding 4 — the round-4 handler assumed every real pipe surfaces the
exception the way a synthetic always-raising test double does, which the
small-payload case does not; verified end-to-end against a REAL OS pipe
whose reader closes having read nothing, in
`tests/test_pipeline_cli.py::TestRealBrokenPipeHandling`). Both callers
force an explicit `sys.stdout.flush()` right after their render, inside
their own `try`, so the failure surfaces THERE instead of at shutdown;
once caught, the handler redirects stdout's file descriptor to
`/dev/null` so the unavoidable final flush becomes a no-op instead of a
second, uncaught `BrokenPipeError` — `cmd_audit_world` and
`cmd_audit_retag_divergence` share the identical
`_handle_broken_pipe_and_exit_cleanly` handler.

### Daily whole-library census snapshot + dashboard card (#1142)

The whole-library census above (~93,700 files / ~200s measured live) is far
too expensive to run at dashboard render or normal web API request time. A
separate daily oneshot, `cratedigger-retag-census.service`
(`scripts/run_retag_divergence_census.py`), scheduled by
`cratedigger-retag-census.timer` (`OnCalendar=daily`, mirroring
`cratedigger-unfindable.timer`'s own cadence/jitter), runs the exact same
unbounded `scan_retag_divergence_from_factory` call as the CLI and
atomically publishes the result as a `RetagDivergenceCensusSnapshot`
(`lib/retag_divergence_census_snapshot.py`) — a same-directory temp file
plus `os.replace`, reusing `lib.sidecar_service`'s already-proven atomic
writer — to `<cfg.stateDir>/retag-divergence-census.json`
(`/var/lib/cratedigger/retag-divergence-census.json` in the deployed
default). Beets-only: this oneshot has no pipeline-DB dependency at all.

Exit codes are distinct from the interactive CLI census above, since this
is a scheduled job, not an operator-run command: `0` — a snapshot WAS
published, covering every `report.status` the scan itself considers a
real answer: `clean`, `divergence_found`, **and `incomplete`** (a single
unreadable file anywhere in the whole-library scan is enough to make the
report `incomplete` even though nothing truncated or resumed it — the
daily writer still publishes that report, since it's genuine information
the dashboard should show). Exit `0` means "the job did its job", not
"the library is clean" — read the published report's own `status` for
that. `1` (`EXIT_BEETS_UNAVAILABLE`) — Beets was unreachable this run;
**nothing is published** — a `beets_unavailable` report never actually
scanned anything, so publishing it would silently replace yesterday's
real answer with a fabricated all-zero "nothing wrong" snapshot, which
is worse than leaving the last real answer in place; `2`
(`EXIT_CONFIG_ABORT`) — the runtime config/Beets contract was rejected
before any scan ran, no snapshot attempted; `3` (`EXIT_RUN_FAILED`) — an
unexpected exception escaped the scan or the atomic write, no snapshot
written. Every non-zero exit case writes NOTHING: atomic publication
plus the `beets_unavailable` skip together mean any prior valid snapshot
at the path is untouched whenever a run does not produce a fresh,
genuinely-scanned one.

`GET /api/pipeline/dashboard` embeds this persisted snapshot read-only
under `retag_divergence_census` (`web/routes/pipeline_dashboard.py`) —
it never scans. Three honest states: `"missing"` (no snapshot path
configured, or the daily job has never published one — a fresh deploy or
every run so far crashed before completing), `"unreadable"` (a snapshot
file exists but failed to decode — logged server-side, never a 500 for
the whole dashboard), `"ok"` (a real published snapshot, `generated_at` +
`duration_seconds` + the full `report`). The web UI renders this as its
own "Beets DB ↔ File Tags Drift" card, deliberately distinct from the
Disk Coverage card above it (pipeline-ledger ↔ Beets DB — a different
question entirely).

For an album the dashboard lists, `GET
/api/audit/retag-divergence/album/<id>` (CLI: `pipeline-cli audit
retag-divergence-album <id>`) offers a cheap, explicit per-album
recheck — reusing the exact SAME pure classifier and tag reader as the
whole-library census (`scan_retag_divergence_single_album`), over just
that album's own files: roughly ten file reads, milliseconds, no
deadline, no cursor. Unlike the whole-library report (which lists only
non-agreeing albums), an explicit per-album check reports `agrees` too —
the operator asked about THIS album specifically. Status-code mapping:
`200` — found (any class); `400`/CLI parser rejection — the id is past
SQLite's signed-64-bit `INTEGER` range (`lib.retag_divergence_audit.
SQLITE_MAX_INTEGER`) and can never be bound as a query parameter at all —
rejected before ever reaching Beets, never the misleading 503 an uncaught
`sqlite3.OverflowError` would otherwise produce; `404` — no album with
that id in Beets; `503` — Beets unavailable. The dashboard's "Recheck"
button patches just that
album's row in place (`recheckRetagDivergenceAlbum` in `web/js/pipeline.js`)
rather than reloading the whole dashboard.

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
       e.format AS _evidence_format,
       e.codec_family AS _evidence_codec_family,
       e.cliff_hz AS _evidence_cliff_hz,
       e.storage_format AS _evidence_storage_format,
       e.filetype_band AS _evidence_filetype_band,
       e.spectral_subject AS _evidence_spectral_subject,
       e.was_converted_from AS _evidence_was_converted_from,
       e.ultrasonic_deficit_db AS _evidence_ultrasonic_deficit_db,
       e.spectral_measurement_version AS _evidence_spectral_measurement_version,
       e.aac_lattice_modal_count AS _evidence_aac_lattice_modal_count,
       e.aac_lattice_scored_tracks AS _evidence_aac_lattice_scored_tracks,
       e.aac_lattice_max_z AS _evidence_aac_lattice_max_z,
       e.verified_lossless_classifier AS _evidence_verified_lossless_classifier,
       (SELECT array_agg(DISTINCT f.extension)
          FROM album_quality_evidence_files f
         WHERE f.evidence_id = e.id) AS _evidence_container_extensions,
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
       current_evidence.codec_family AS _current_evidence_codec_family,
       current_evidence.cliff_hz AS _current_evidence_cliff_hz,
       current_evidence.storage_format AS _current_evidence_storage_format,
       current_evidence.filetype_band AS _current_evidence_filetype_band,
       current_evidence.spectral_subject AS _current_evidence_spectral_subject,
       current_evidence.was_converted_from
           AS _current_evidence_was_converted_from,
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

The third block (`_evidence_codec_family` onward, issue #829 Phase 5 PR4)
is what `web/classify.py::proof_gate_projection` reads to derive the
proof-gate verdict, the audit-only-codec flag, and the proof generation.
`_evidence_container_extensions` is the ultrasonic leg's decode-path input;
dropping it does not fail, it silently withholds a leg the decider
adjudicated.

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

### Library-row badge differential

Library badge changes use the same diff engine with one shared corpus of exact
`/api/library/artist` album rows. Build the artist selector union from both
authorities; otherwise a pipeline-only Captured/Missing row or a Beets-only
Untracked row can silently fall outside the measurement:

```bash
ssh doc2 'export PGPASSWORD=$(sudo cat /run/secrets/cratedigger-pgpass | grep "^PGPASSWORD=" | cut -d= -f2); pipeline-cli query --json -' <<'SQL' > /tmp/library-pipeline-artists.json
SELECT DISTINCT artist_name AS name, COALESCE(mb_artist_id, '') AS mbid
FROM album_requests
WHERE COALESCE(artist_name, '') <> ''
ORDER BY name, mbid;
SQL

nix-shell --run "python3 /dev/stdin" > /tmp/library-beets-artists.json <<'PY'
import json
import sqlite3

db = sqlite3.connect(
    "file:/mnt/virtio/cratedigger/beets-db/beets-library.db?mode=ro",
    uri=True,
)
rows = db.execute(
    "SELECT DISTINCT albumartist, COALESCE(mb_albumartistid, '') "
    "FROM albums WHERE COALESCE(albumartist, '') <> '' "
    "ORDER BY albumartist, mb_albumartistid"
).fetchall()
print(json.dumps([{"name": name, "mbid": mbid} for name, mbid in rows]))
PY
```

Start the read-only live-db dev server from `docs/web-dev-server.md`, including
both `--beets-db` and `--beets-directory`. Then fetch every selector through
the production route, deduplicate exact repeated rows caused by legacy artist
name fallbacks, and add only the synthetic corpus identity:

```bash
nix-shell --run "python3 /dev/stdin" <<'PY'
import json
import urllib.parse
import urllib.request

selectors = set()
for path in (
    "/tmp/library-pipeline-artists.json",
    "/tmp/library-beets-artists.json",
):
    for row in json.load(open(path, encoding="utf-8")):
        selectors.add((str(row["name"]).strip(), str(row["mbid"]).strip()))

albums = {}
for name, mbid in sorted(selectors):
    query = urllib.parse.urlencode({"name": name, "mbid": mbid})
    with urllib.request.urlopen(
        f"http://127.0.0.1:8096/api/library/artist?{query}", timeout=30
    ) as response:
        for album in json.load(response)["albums"]:
            key = json.dumps(album, sort_keys=True, separators=(",", ":"))
            albums[key] = album

with open("/tmp/library-badge-corpus.jsonl", "w", encoding="utf-8") as out:
    for corpus_id, key in enumerate(sorted(albums), start=1):
        row = dict(albums[key])
        row["_corpus_id"] = corpus_id
        out.write(json.dumps(row, separators=(",", ":")) + "\n")
PY
```

Render the same corpus through each tree's complete production Library row.
The driver is safe to copy into an older base because it imports that tree's
own `renderLibraryAlbumRow`; it does not reimplement the row-to-badge adapter:

```bash
BASE_REF=origin/feature/759-beets-ownership-cutover
BASE_DIR=$(mktemp -d)
git archive "$BASE_REF" | tar -x -C "$BASE_DIR"
cp scripts/render_library_badges.mjs "$BASE_DIR/scripts/"

nix-shell --run "node $BASE_DIR/scripts/render_library_badges.mjs \
  --corpus /tmp/library-badge-corpus.jsonl \
  --out /tmp/library-badges-base.jsonl"
nix-shell --run "node scripts/render_library_badges.mjs \
  --corpus /tmp/library-badge-corpus.jsonl \
  --out /tmp/library-badges-current.jsonl"
nix-shell --run "python3 scripts/render_differential.py diff \
  --base /tmp/library-badges-base.jsonl \
  --current /tmp/library-badges-current.jsonl"
```

The output field is the full `row_html`, not a hand-built badge fragment. Put
the total and changed-row counts in the PR body. Screenshots remain separate
visual evidence and never substitute for this complete live-row census.
## Live-corpus decision differential

`scripts/decision_differential.py` is the decision-level counterpart to the
render differential. Its corpus is an evidence graph, not a sidecar pairing
file: each candidate row carries the exact nullable
`album_requests.current_evidence_id` **and** its request's exact
`mb_release_id`. Each non-null reference resolves to the complete
current-evidence row in the same export; both evidence rows must match the
request release exactly. `is_candidate` tells the replay which rows to decide;
current-only rows provide the paired evidence but are not themselves
candidates. A null `current_evidence_id` is the ordinary no-HAVE case.

Export through the deployment-owned `decision-differential` wrapper; it owns
the SQL projection and pins the deployed source plus Python environment. Run
it on doc2 so the DSN stays in the deployed runtime boundary:

```bash
EXPORT_DIR=/tmp/cratedigger-decision-corpus-$(date +%Y%m%d-%H%M%S)
mkdir -p "$EXPORT_DIR"
ssh doc2 'export PGPASSWORD=$(sudo cat /run/secrets/cratedigger-pgpass | grep "^PGPASSWORD=" | cut -d= -f2); work=$(mktemp -d /tmp/cratedigger-decision-corpus.XXXXXX); decision-differential export --dsn postgresql://cratedigger@10.20.0.11:5432/cratedigger --corpus "$work/corpus.jsonl" --coverage "$work/coverage.json"; export_rc=$?; printf "%s\n" "$export_rc" > "$work/export-status"; tar -C "$work" -cf - corpus.jsonl coverage.json export-status; exit 0' | tar -C "$EXPORT_DIR" -xf -
test "$(cat "$EXPORT_DIR/export-status")" = 0 || test "$(cat "$EXPORT_DIR/export-status")" = 2
nix-shell --run "python3 scripts/decision_differential.py verify --corpus $EXPORT_DIR/corpus.jsonl --coverage $EXPORT_DIR/coverage.json"
```

The remote export exit is captured before transfer: `0` is green and `2` is
named historical debt. The local verifier runs only over the transferred pair.
`verify` proves strict structural/self-consistency and rejects stale or
mismatched artifacts; it is not a signature or live-authenticity mechanism.
PostgreSQL's repeatable-read export and its real-PG tests are the independent
completeness authority.

The exporter opens one repeatable-read, read-only PostgreSQL snapshot. It
replaces each output atomically and independently: `corpus.jsonl` has one complete typed evidence
row per exported ID, and `coverage.json` records source-link counts, collapsed
identical associations, conflicts, authorityless links/IDs, paired/unpaired
valid candidates, referenced current IDs, every debt class, and the
expected-vs-written ID/content checks. Coverage binds the corpus SHA-256.
Before each base and current `decide`, run the mandatory executable pair check;
it rejects stale, substituted, or malformed corpus/coverage pairs:

```bash
nix-shell --run "python3 scripts/decision_differential.py verify \
  --corpus /tmp/decision-corpus.jsonl \
  --coverage /tmp/decision-corpus-coverage.json"
nix-shell --run "python3 scripts/decision_differential.py decide \
  --corpus /tmp/decision-corpus.jsonl \
  --coverage /tmp/decision-corpus-coverage.json \
  --out /tmp/decision-current.jsonl"
```

The corpus and coverage bytes are
deterministic for the same snapshot regardless of internal batch size.

Exit status `0` means the complete source graph was authoritative. Exit status
`2` means the files were still written but coverage found historical debt (a
conflict, missing/dangling evidence, mismatched release, or authorityless
source link). Treat that as non-green evidence; do not hand-edit the corpus or
substitute a sibling pressing. Valid authoritative candidates remain exported
alongside the complete named debt so a differential can proceed on that cohort.

Run `verify` above, then both trees with the same complete `decision-corpus.jsonl`; the replay
resolves IDs before emitting a result, so the row order does not matter. Use
`--counterfactual` when measuring a proof-promotion change: it strips only the
candidate proof and retains the paired current proof.

For a large corpus, pass `export --batch-size N`. Batching changes only the
internal explicit-evidence fetches; it does not partition source links or alter
the single-snapshot graph, so raw corpus IDs are neither duplicated nor lost.
