# Rust rewrite — research note 01: the beets harness

**Status:** ideation. No code committed. This doc captures what the
beets harness actually does today, what's broken about it, and what
would be in scope for a Rust replacement. The Rust rewrite will live
in a separate repo; this is the source-of-truth research the new
repo inherits.

**Predecessor:** `docs/beets-rpc-design.md` proposed a long-lived
Python JSON-RPC server replacing the per-op subprocess pattern. This
doc is the more aggressive variant — replace the harness *and*
beets's import-time machinery with a Rust binary, keeping Python
beets only as an out-of-band operator toolkit (tagging-workspace,
library-wide commands like `beet ls`, `beet missing`).

**Why start here:** the harness is the dirtiest seam in cratedigger.
Every category of historical bug in this codebase that resulted in
data loss, silent rejection, or wrong-match drift has the harness or
its subprocess contract on the call path. If a Rust rewrite is
worth doing at all, this is the highest-leverage piece. Everything
else (web server, pipeline orchestrator, slskd polling, quality
decisions) can be ported later or left in Python — the harness is
load-bearing for correctness in a way the others are not.

---

## 1. What the harness is

`harness/beets_harness.py` (672 lines) plus `harness/import_one.py`
(1711 lines) plus `harness/run_beets_harness.sh`. Two distinct
subprocess entry points sharing a JSON wire protocol with their
Python callers in cratedigger.

**`beets_harness.py`** subclasses `beets.importer.session.ImportSession`
and overrides four interactive prompts (`choose_match`, `choose_item`,
`resolve_duplicate`, `should_resume`), emitting each as JSON over
stdout and reading an action over stdin. The controller in cratedigger
gets to drive beets's import flow as if it were the user.

**`import_one.py`** is the orchestrator for one album: spectral
analysis, lossless→V0 conversion, transcode detection, quality
gate, harness invocation, post-import verification, permission
repair, staged-dir cleanup, pipeline DB update. ~20 phases. Emits
a single `ImportResult` JSON sentinel (`__IMPORT_RESULT__{...}`) on
success or crash.

**Wire boundary types** today (all `msgspec.Struct`, all in
`lib/quality.py`):

| Direction              | Type                       | Purpose                                       | Survives? |
|------------------------|----------------------------|-----------------------------------------------|-----------|
| harness → controller   | `ChooseMatchMessage`       | candidates + items + recommendation per task  | dies — replaced by direct RPC return |
| harness → controller   | `CandidateSummary`         | one beets `AlbumMatch` (rename `album_id`→`mbid`) | survives, but always 0/1-element list under `--search-id` |
| harness → controller   | `HarnessItem`              | one local file's tags + format + bitrate      | survives |
| harness → controller   | `HarnessTrackInfo`         | one MB/Discogs track (track_id forced str)    | survives |
| harness → controller   | `TrackMapping`             | item↔track binding for a candidate            | survives |
| harness → controller   | `DuplicateRemoveCandidate` | duplicate album detail for `resolve_duplicate`| dies — see §10 |
| controller → harness   | `{"action":...}`           | apply / skip / asis / tracks / albums         | dies — interactive loop goes away |
| controller → harness   | `{"action":"keep|remove|merge|skip"}` | duplicate resolution               | dies — see §10 |
| import_one → caller    | `ImportResult`             | full import outcome + audit trail             | survives (shape preserved) |
| beets_validate → caller| `ValidationResult`         | candidate list + distance + scenario          | survives (shape preserved) |

The Rust binary collapses the multi-message interactive protocol to
two RPC-shaped subcommands; see §5. The 8 wire types shrink to 6,
and the controller stops being a participant in beets's import-time
prompt loop.

Decoded at exactly two sites: `lib/beets.py::beets_validate`
(`msgspec.convert(msg, type=ChooseMatchMessage)`) and
`lib/import_dispatch.py::run_import_one` (parses the
`__IMPORT_RESULT__` sentinel via `ImportResult.from_json`). After
that, every consumer works with the typed Struct.

**The slice of beets this actually exercises:**

```python
from beets import config, library, plugins
from beets.importer.session import ImportSession
from beets.importer.tasks import Action, ImportTask as BeetsImportTask
from beets.ui import get_path_formats, get_replacements
```

Plus a monkey-patch on `BeetsImportTask.find_duplicates` to make
duplicate queries release-ID-aware (mapping `AlbumInfo.album_id` →
`mb_albumid` in the temp Album used to build the query).

Plus `plugins.load_plugins()` + `plugins.send("library_opened",
lib=lib)` to fire `musicbrainz`, `discogs`, `fetchart`, `embedart`,
`lyrics`, `lastgenre`, `scrub`, `info`, `missing`, `duplicates`,
`edit`, `fromfilename`, `ftintitle`, `the`, `inline` — though only
a subset actually run on import (see §5 bucket 4).

---

## 2. What the harness costs

Six categories of recurring failure, every one earned in production.
Full sha list in the git archaeology source notes; this is the
distilled taxonomy.

### 2.1 Data integrity / data loss

The Palo Santo cluster: 13+ commits, 5 review rounds, full strategy
reversal twice. Shearwater "Palo Santo" 11-track 2006 edition was
wiped when the 19-track 2007 reissue imported as an "upgrade".

**Root cause:** beets reads `duplicate_keys` strictly from
`config["import"]["duplicate_keys"]["album"]`. The user's YAML had
the block at the top level, not under `import:`. Beets silently fell
back to default `[albumartist, album]`. `find_duplicates()` matched
a cross-MBID sibling on title alone. The harness answered
`{"action":"remove"}` thinking it was a stale same-MBID row, and
beets's `task.should_remove_duplicates` blast radius wiped the
sibling's files.

**Defenses now layered into the harness:**
- Startup assertion `_assert_duplicate_keys_include_mb_albumid` —
  refuses to start unless config has the right shape.
- `duplicate_keys` restricted to exact release IDs only
  (`mb_albumid`, `discogs_albumid`).
- Beets duplicate lookup field-mapped (`album_id` → `mb_albumid`).
- MBID-swap audit log at `/mnt/virtio/Music/.harness-mutations.jsonl`
  for cases where someone retags an existing album via `--search-id`
  (the 2026-04-14 Lucksmiths incident).
- Cratedigger answers `{"action":"remove"}` only when beets reports
  exactly **one** same-release duplicate; otherwise exit code 7
  (`duplicate_remove_guard_failed`) and stage to manual review.

Adjacent: `convert_lossless(keep_source=False)` was destroying user's
only FLACs on force/manual imports before the quality decision ran;
fixed with `--preserve-source`.

### 2.2 Silent failure / 0 candidates

Two distinct subprocess-contract bugs caused identical symptoms
("scenario=mbid_not_found" despite the album being in the local MB
mirror):

- `HOME=/root` under systemd → Discogs plugin's patched config not
  found → silent 0 candidates. Fix: `lib/util.py::beets_subprocess_env`.
- `pythonEnv` on systemd PATH → harness found a bare `beet` with no
  plugins → silent 0 candidates. Fix: drop pythonEnv from path,
  resolve `beet` via Nix-wrapped binary's exec chain.

Same symptom, two unrelated env-leak vectors. Pattern: every `sp.run([beet,
...])` call inherits a hidden contract over PYTHONPATH, HOME, PATH,
stdin behavior, exit code semantics. Each fix was local; none
generalized.

### 2.3 Wrong-match / int-vs-str drift

Beets's Discogs plugin returns `album_id`/`releasegroup_id`/`track_id`
as JSON integers (Discogs API returns numbers). Cratedigger compared
with str-typed pipeline DB IDs via `==`. Pyright didn't see it
because `dict.get()` is opaque. Same trap fired in two modules
within days (`lib/beets.py` then `harness/import_one.py`) —
defensive coercion at the consumer side does not scale.

**Structural fix:** `_id_str` helper at the source normalizes IDs
to str before emission. Wire is always clean. `msgspec.Struct`
declared as `str` validates at the boundary. Drift now raises
`msgspec.ValidationError` with a typed location instead of silent
`==`-returns-False.

### 2.4 Subprocess plumbing (encoding, timeouts, stdin)

- `subprocess.run(text=True)` defaults to UTF-8 *strict* decoding
  during capture, *before* `subprocess.run` returns. ffmpeg/sox echo
  CP1252-tagged Vorbis comments → `UnicodeDecodeError` propagates
  from a code path `try/except TimeoutExpired` doesn't catch.
  Request 580 (78 Saab — *Crossed Lines*) crashed in a permanent
  retry loop because nothing was attributed to the peer. 15 vulnerable
  sites across 6 files; all now use `errors="replace"`.
- `beet remove` waited on stdin prompt under non-tty, got EOF, rc=1.
  Fix: pipe `"y\n"`.
- `beet move` `TimeoutExpired` after a successful import meant the
  JSON sentinel never fired and dispatch caller thought import failed
  when it had succeeded.
- Stderr was sliced to 500 chars, hiding the actual exception line at
  the bottom of multi-frame Python tracebacks.

### 2.5 Protocol / type drift

Resolved by the msgspec policy in `.claude/rules/code-quality.md`
§ "Wire-boundary types". One decode site, strict types, symmetric
encode. Eight wire-boundary types ported.

### 2.6 Performance / timeouts

Mostly: ensuring one stuck subprocess doesn't block the whole 5-min
cycle. 120s validation timeout, 1800s import timeout, 30/120s beet
op timeouts. Process-group `SIGTERM` (`os.killpg(os.getpgid(...))`)
to clean up beets's children.

### Honest assessment

**The duplicate-detection seam is structurally fragile.** Five review
rounds and two strategy reversals on Palo Santo before the third
architecture stuck. The current invariants (mb_albumid mandatory in
duplicate_keys, exactly-one same-release duplicate to authorize
remove, MBID-scoped pre-flight remove that physically can't match
siblings) work but are defense-in-depth — not a structurally safe
seam.

**The wire boundary is in a much better place** after the msgspec
unification. Type drift now raises at the boundary instead of hiding
in `dict.get()`.

**The subprocess-contract surface is the one a Rust rewrite directly
shrinks.** A Rust binary that owns tag writing, the SQLite schema,
and MusicBrainz fetch in-process kills HOME/PATH/PYTHONPATH/stdin/
text-mode-decode as failure modes — not because Rust is better, but
because the subprocess goes away. Rust→ffmpeg/sox/`beet ls` calls
still inherit the same plumbing problems.

---

## 3. Caller inventory — what the JSON contract must keep working

The Rust binary replaces `beets_harness.py` (and likely
`import_one.py`'s harness-driving phase). Every caller that today
constructs a subprocess argv and parses JSON output is a contract
that must be preserved bit-for-bit during migration, or migrated in
lockstep.

| Caller                                                      | Today's argv shape                                                          | What it parses                  |
|-------------------------------------------------------------|-----------------------------------------------------------------------------|---------------------------------|
| `lib/beets.py::beets_validate`                              | `harness --pretend --noincremental --search-id <MBID> <path>`               | `ChooseMatchMessage`, builds `ValidationResult` |
| `lib/import_dispatch.py::run_import_one`                    | `python3 import_one.py <path> <MBID> [--force --preserve-source ...]`       | `__IMPORT_RESULT__` sentinel → `ImportResult` |
| `scripts/importer.py::execute_import_job` (importer worker) | wraps `dispatch_import_from_db` → `dispatch_import_core` → `run_import_one` | (transitively) `ImportResult`   |
| `scripts/pipeline_cli.py::cmd_force_import` / `cmd_manual_import` | enqueues importer job; importer wraps `run_import_one`                | (transitively) `ImportResult`   |
| `lib/beets_album_op.py::_run_beet_op`                       | `beet remove -a [-d] <selector>` / `beet move -a id:N`                      | rc + stderr → `BeetsOpFailure`  |
| `lib/release_cleanup.py::remove_album_by_selectors`         | iterates selectors → delegates to `_run_beet_op`                            | list of `BeetsOpResult`         |
| `tagging-workspace/scripts/*` (operator-only, not runtime)  | `harness --search-id <MBID>` directly; `beet ls`; `beet missing`            | varied; out of runtime scope    |

Key contract elements that must not silently drift:
- The 8 wire-boundary Structs (field names, types, defaults).
- The two-sentinel emission style (`__IMPORT_RESULT__` for
  `import_one.py`; newline-delimited JSON for the harness inner loop).
- Exit codes 0/1/2/3/4/5/6/7/99 with their decision semantics —
  parsed in dispatch's downgrade/transcode/duplicate-guard branches.
- The MBID-swap audit log path and event shape.
- The duplicate-keys assertion (a Rust replacement that owns
  duplicate detection moves the assertion *into the Rust DB layer*
  but should keep the same failure mode: refuse to start with bad
  config).

---

## 4. What slice of beets we actually use

### Bucket 1 — Hard requirements (must reimplement or shell out for)

| Capability | Rust crate or approach |
|---|---|
| Tag writing into FLAC / MP3 / Opus / AAC / M4A (mb_albumid, mb_trackid, mb_releasegroupid, mb_artistid, full MB metadata, lyrics, art) | `lofty-rs` (read+write, all formats we touch). `symphonia` is read-only — not enough. |
| MusicBrainz release fetch by ID against `192.168.1.35:5200` (mirror) | `musicbrainz_rs` — supports `inc=recordings+artist-credits+labels+release-groups+media+discids` which is what beets's `musicbrainz` plugin requests. Configurable host + rate-limit. |
| Path template rendering with `aunique{}` disambiguation, replacements, `inline`-style `short_mbid` | Hand-written formatter. ~500 lines. The hard part is `aunique`: must enumerate existing albums and pick a minimal disambiguator from `[albumtype year label catalognum albumdisambig releasegroupdisambig short_mbid]`. No off-the-shelf templating crate fits — beets's DSL is too custom. |
| Library SQLite schema — *same DB file*, same `albums`/`items` columns | `rusqlite` (sync, thin, matches how Python uses it). Schema must stay byte-identical; tagging-workspace, Plex, Meelo, the web UI all read it. |
| **Same-MBID upsert** + split-brain assertion (replaces beets's `find_duplicates` / `resolve_duplicate` machinery — see §10) | Plain SQL inside a transaction: `SELECT id FROM albums WHERE mb_albumid=?` → 0 means insert, 1 means delete-then-insert, >1 aborts with `split_brain_detected`. ~50 lines. |
| Atomic file move + permission repair (umask 0, walk-and-chmod 0775/0664) | `std::fs::rename` + cross-FS fallback (`fs_extra` or hand-rolled copy+remove). `walkdir` + `set_permissions`. |
| JSON wire protocol (6 surviving Struct types) | `serde` + `serde_json`. Field names are the API. `_id_str` semantics preserved (str on the wire, never int). |
| MBID-swap audit log append | `OpenOptions::append` + `serde_json::to_writer`. Trivial. |

### Bucket 2 — Used but easy to replace

- FLAC→V0 / target-format conversion: already a separate ffmpeg
  subprocess in `import_one.py::convert_lossless`. Rust shells out
  the same way. No upside porting to `lame`/`opusenc` bindings.
- Per-track bitrate probe: `ffprobe` subprocess. Same.
- Spectral analysis: `lib/spectral_check.py` shells `sox`. Stays
  external.
- `beet remove` / `beet move`: become direct SQLite mutation +
  `fs::remove_file` + `fs::rename`. The `y\n` stdin prompt goes away
  with the subprocess.
- `Action` enum: dies entirely. `import_one` only ever sends `apply`
  or `skip`, and once duplicate detection goes (§10) the action enum
  has nothing to discriminate. The Rust binary either imports the
  forced MBID or returns a typed failure.
- Distance threshold check: just a float compare against 0.15
  (validate) / 0.5 (import). The actual distance *calculation* is
  Bucket 4.

### Bucket 3 — Used but could keep shelling to Python beets

These are rare and not on the import hot path:

- `beet ls -a -f "<format>"`, `beet missing` — only called from
  `tagging-workspace/scripts/*`, which is operator-only and runs wherever beets is installed (currently doc2).
  Out of scope for the runtime binary; keep using Python beets.
- `beet fetchart -a`, `beet embedart -a`, `beet replaygain -a`,
  `beet mbsync` — *not* currently called by anything in cratedigger.
  Defer entirely. If cover art / lyrics / genre enrichment is wanted
  per-import, post-import shell-out to `beet fetchart -a query:mb_albumid:<X>`
  is one line and runs after the Rust binary has done the move +
  tag write. Strangler-fig: cosmetic plugins stay Python, hot path
  goes Rust.

### Bucket 4 — Used but we don't actually depend on

- **Beets's candidate scoring / distance calculation.** Cratedigger
  pre-selects the MBID via `--search-id` and only checks the resulting
  candidate's distance against a threshold. No code branches on the
  per-field distance breakdown. A Rust replacement can compute distance
  with `strsim` (Levenshtein/Jaro-Winkler) over artist + album + per-track
  titles + track count and produce a 0..1 score — doesn't need to
  match beets bit-for-bit because no decision depends on equality.
- `chroma`/`acoustid`: configured `auto: false`, never runs auto. Drop.
- `scrub`: runs at apply time, strips old tags. Lofty-based replacement
  writes a canonical tag set from scratch — effectively "scrub by
  replacement". Drop the explicit pass.
- `info`, `missing`, `duplicates`, `edit`, `the`, `fromfilename`:
  CLI-only or no-op when an MBID is forced. Drop.
- `ftintitle`: tiny rule (move "feat." from artist to title at apply).
  ~20 lines inline.
- `inline`: only computed field used is `short_mbid: mb_albumid[:8]`.
  Special-case it in the path renderer; don't reimplement an inline
  Python-eval DSL.
- `lastgenre` / `lyrics` / `fetchart` / `embedart`: HTTP fetches +
  tag writes. Bucket 3 strangler — shell out to Python beets after
  the Rust import lands the album, or reimplement piecemeal once
  the core works.
- `discogs` plugin: when target is a Discogs ID, currently calls into
  the patched plugin which hits `discogs.ablz.au`. Calling the local
  Rust Discogs mirror's JSON API directly from Rust is *easier* than
  going through beets — skip the plugin.
- `should_resume` interactive prompt: always answered `False`. Drop.
- `Action.ASIS/TRACKS/ALBUMS` and `should_merge_duplicates`: never
  used in practice. Drop.
- `choose_item` (singleton-track import flow): never used —
  cratedigger imports albums only. Drop.
- `--quiet-fallback` CLI flag: declared but never wired up even in
  Python. Drop.
- `--upstream` (musicbrainz.org instead of mirror): operator-only,
  never on the runtime path. Drop.
- Multi-`--search-id` (passing >1 ID per call): never used —
  cratedigger forces exactly one MBID per import. Drop.

---

## 5. Realistic Rust replacement scope

A single Rust binary with two RPC-shaped subcommands, no interactive
prompt loop, no `ImportSession` simulation, no policy knobs. One-shot
per call; daemon mode is a later optimization. Pick a name later;
this is ideation.

```
cratedigger-tag validate <path> --mb <MBID>
    → ValidationResult JSON on stdout, exit 0
    → no DB writes, no file mutations
    → used by lib/beets.py::beets_validate

cratedigger-tag import <path> --mb <MBID>
    → ImportResult JSON on stdout via __IMPORT_RESULT__ sentinel
    → tags + moves + SQLite upsert
    → used by harness/import_one.py (replacing its run_import phase)
```

**Two flags total. No knobs.** All today's `import_one` orchestration
flags (`--force`, `--preserve-source`, `--target-format`,
`--override-min-bitrate`, `--verified-lossless-target`,
`--quality-rank-config`, `--existing-v0-probe-*-bitrate`, `--dry-run`,
`--request-id`) stay caller-side. By the time the Rust binary runs,
the files are already in their target format, lossless source is
preserved-or-not per caller policy, the quality decision is already
made, and the pipeline DB row is the caller's to update.

**Trust boundary:** the binary answers *can-I-do-this?* questions
(MBID exists in MB mirror? files openable? track count alignable?)
but never *should-I-do-this?* policy. Distance is reported, never
gated. `--force` becomes meaningless because there's no MAX_DISTANCE
check to force past. If you call `import` with a path full of
unrelated audio and a random MBID, it tags those files with that
MBID's metadata and inserts them — same trust shape as
`beet remove -a id:N -d` today. The pipeline composes
validate → gate → import; the binary just executes.

**What still refuses** (infrastructural, not policy): MBID not
findable in MB mirror, files unreadable, zero items in path, track
count differs so wildly a sane mapping can't be constructed (e.g.,
12 local files vs. 2 release tracks). Each surfaces as a typed
failure in `ImportResult` / `ValidationResult` with the same
exit-code semantics dispatch already understands.

Owns:

1. **Library SQLite schema** — opens `/mnt/virtio/Music/beets-library.db`
   with `rusqlite`. Reads/writes the same columns Plex, Meelo, the
   web UI, and tagging-workspace already depend on.
2. **Tag writing** via `lofty-rs` for FLAC/MP3/Opus/AAC/M4A.
3. **MusicBrainz fetch** via `musicbrainz_rs` against the local
   mirror.
4. **Path template renderer** with `aunique` disambiguation and
   inlined `short_mbid` — bit-for-bit parity with beets is required
   here because folder layouts are visible to humans and must not
   shift on first import.
5. **Same-MBID upsert** inside a transaction (see §10). Replaces
   beets's `find_duplicates` / `resolve_duplicate` flow entirely.
6. **JSON wire protocol** — serde-emitted Structs with the same
   field names cratedigger's Python decoders already expect. Decoded
   unchanged by `lib/beets.py::beets_validate` and the
   `__IMPORT_RESULT__` parser.
7. **MBID-swap audit log** append.
8. **File move + permission repair**.
9. **Distance score** — simple `strsim`-based, threshold-only.

Subprocess boundaries that **stay external**:

- `ffmpeg`/`ffprobe` for conversion + probe.
- `sox` for spectral analysis (already a `lib/spectral_check.py`
  responsibility — stays Python or moves to Rust shelling sox; either
  way unchanged).
- Cosmetic plugins (`fetchart`/`embedart`/`lyrics`/`lastgenre`):
  optional post-import shell-out to Python `beet` for now.

What stays Python:
- `harness/import_one.py` — kept Python initially as the
  *orchestrator*: spectral, conversion, quality decision, cleanup,
  pipeline DB update. The Rust binary replaces only its
  `run_import()` phase (the harness driving). Later phases of the
  rewrite can move orchestration to Rust too, but it's not required
  for this slice.
- `lib/quality.py` decision functions, the simulator
  (`pipeline-cli quality`), the spectral analyzer.
- `tagging-workspace/scripts/*` (operator toolkit, not runtime).
- Cratedigger pipeline itself (downloads, search, web, CLI, importer
  worker) — all stay Python for this first slice.

Sizing estimate (revised after dropping duplicate-detection,
interactive prompts, ASIS/TRACKS/ALBUMS, choose_item, multi-MBID,
upstream, quiet-fallback):

- Path-template renderer with `aunique`: ~500 lines, still the
  hardest individual piece.
- SQLite schema layer + same-MBID upsert + split-brain check: ~250 lines.
- Tag writer (lofty wrapper + format-specific quirks): ~600 lines.
- MusicBrainz client wrapper + retry/rate-limit: ~300 lines.
- JSON wire protocol + serde Structs (6 types, two subcommands): ~250 lines.
- Move + permission repair + audit log: ~200 lines.
- Distance scoring: ~150 lines.
- CLI / config / glue: ~300 lines.

Call it ~2.5-3k lines of Rust for the core, down from ~3-4k in the
pre-trim sketch. Plus tests — the acceptance test is the existing
Python suite running unchanged against the Rust binary, because the
wire contract for `ValidationResult` and `ImportResult` is
preserved.

---

## 6. Critical safety invariants the Rust binary inherits

These are non-negotiable. Every one was earned in production. Bake
them in as `assert!`/integration tests/CI gates, not optional
defenses.

1. **Same-MBID upsert is the only destructive primitive.** Inside a
   single SQLite transaction: count rows for the target `mb_albumid`.
   Zero rows → fresh insert. Exactly one row → delete that row +
   its files, then insert. More than one row → abort with
   `split_brain_detected` (downstream effect: stage to manual
   review, same as today's exit code 7). The user-curated-collection
   invariant ("multiple editions/pressings are intentional, never
   merge") means we never ask any question about cross-MBID rows;
   the upsert is `mb_albumid`-scoped by construction.
2. **Post-import: exactly one album row per `mb_albumid`.** Zero
   means insert failed; >1 means the upsert raced with another
   writer. Either is an error.
3. **MBID-swap audit log is append-only and best-effort.** Never
   blocks import. Failures logged to stderr only. Format:
   `{event, ts, path, old_mb_albumid, new_mb_albumid, argv, ppid}`.
   Out-of-band retag tooling (tagging-workspace) is the only realistic
   producer here, but the runtime binary should also log a swap event
   if the on-disk tags carry an MBID different from the requested one.
4. **All IDs on the wire are strings, normalized at the source.** Not
   the consumer. Discogs returns int; serialize as str before emit.
5. **No DB writes in `validate` mode.** The validate subcommand reads
   files, fetches MB, computes mapping + distance, returns
   `ValidationResult`. Touches no SQLite tables, no files on disk.
6. **Permission repair** (umask 0, walk-and-chmod) after every move.
   GH #84 reasons.
7. **`--preserve-source`** semantics: never delete the user's only
   lossless copy until the quality decision approves the import.
8. **No policy gating inside the binary.** Distance, quality, force,
   preserve-source, target-format are all caller-side decisions.
   The binary refuses on *infrastructural* failures (MBID not in MB
   mirror, files unreadable, track-count unmappable) only. If the
   caller hands it nonsense, it imports nonsense — that's the trust
   contract.
9. **Codec-aware quality ranking** stays in `lib/quality.py`. The
   Rust binary never sees `QualityRankConfig` because the quality
   decision happened upstream in `import_one.py` before the binary
   was called.

---

## 7. What this slice does *not* fix

Honest list of subprocess-contract bugs that survive:

- ffmpeg/ffprobe/sox subprocess calls still inherit PATH, PYTHONPATH,
  HOME, stdin EOF, and UTF-8-strict-decode semantics. Rust's
  `std::process::Command` doesn't UTF-8-decode stderr for you, so
  the strict-decode class goes away by default — but rate-limit /
  PATH / `which()` / version-mismatch bugs all stay.
- Quality decisions stay in `lib/quality.py`. The Rust binary is an
  IO + matching boundary, not a decision layer.
- Beets-version drift (when the `beets` package upgrades) still
  affects tagging-workspace operator scripts and any post-import
  cosmetic shell-out. Mitigated by Nix pinning, same as today.
- The `tagging-workspace` MBID-drift class (Lucksmiths incident) is
  only mitigated by the audit log — it's not prevented. If a Rust
  rewrite eventually owns library-wide retags too, the audit becomes
  enforceable. For this slice, the audit is the same best-effort log.

---

## 8. Open questions for the next research note

0. **Same-MBID upsert under concurrent writers.** With duplicate
   detection gone, the upsert transaction is the new
   correctness-critical primitive. Need to confirm `rusqlite`'s
   `BEGIN IMMEDIATE` / `BEGIN EXCLUSIVE` semantics give us what we
   need against concurrent readers (Plex, Meelo, the web UI's
   `BeetsDB` queries). Most likely `BEGIN IMMEDIATE` + WAL mode is
   enough; worst case we hold the existing pipeline DB advisory
   lock across the upsert. Verify before writing the upsert path.
1. **Path-template parity.** How exact does `aunique` need to be?
   If we change folder names on existing albums, downstream tools
   (Plex, Meelo, the web UI's Library tab) re-scan; symlinks
   theoretically break. Need to take a snapshot of every existing
   library album's path, generate the Rust-renderer's path for the
   same metadata, diff, and decide if drift is acceptable or if
   parity is hard-required. The `aunique` algorithm itself is
   deterministic; the question is whether our reimplementation
   matches.
2. **`scrub` behavior under "write canonical tag set from scratch".**
   Does this lose user-edited tags we want to keep (e.g.
   tagging-workspace fix_undated retags)? Probably not, because
   tagging-workspace also goes through `beet apply` and rewrites
   tags from MB — but worth confirming with a sampling of edge
   cases.
3. **Plugin event semantics for cosmetic plugins.** If we shell out
   `beet fetchart -a` after a Rust import, does fetchart's
   `art_size` config propagate? Does `albumtypes` filtering still
   work? Should be fine because the album row exists in the SQLite
   DB by the time fetchart runs — but worth a Playwright smoke on
   doc2 (where beets runs) with a real album before declaring it a viable strangler.
4. **Daemon vs one-shot lifecycle.** The Python harness pays a
   ~1-2s plugin-load cost per spawn. A Rust binary loads in <100ms;
   per-call is fine indefinitely. No need to design a daemon for
   this slice. Revisit if the cratedigger-web force-import flow
   becomes hot.
5. **MusicBrainz fetch caching.** Beets's musicbrainz plugin caches
   release lookups per-process. A one-shot Rust binary doesn't
   benefit from in-process caching. Either skip caching (the local
   mirror is fast enough) or add a small SQLite-backed cache. Skip
   for the first slice.
6. **Test infrastructure.** The Python suite has 1400 tests, many
   integration slices that spawn the harness. Strategy:
    - Phase 0: Rust binary passes the existing
      `tests/test_beets_validation.py` and the
      `tests/test_integration_slices.py` slices unchanged, because
      the wire contract is preserved.
    - Phase 1: Rust-side unit tests in the new repo for the
      path renderer, tag writer, MB client, duplicate query,
      distance scorer.
    - Phase 2: golden-output tests — run Python harness and Rust
      binary on the same album, diff the JSON. Where they differ on
      Bucket-4 fields we don't depend on, document the difference;
      where they differ on Bucket-1 fields, fail the test.

---

## 9. Why this is the keystone slice for the broader Rust rewrite

The thing that bogs down language rewrites isn't line-count — it's
*multiple invented interop boundaries*. Each one is a wire contract
you have to define, version, marshal, error-handle on both sides.
Two languages × N boundaries = N contracts drifting independently,
and every refactor touches two languages.

Under this design, `cratedigger-tag` is the only **invented**
Python↔Rust boundary. The other things both languages touch already
exist as boundaries today, regardless of language:

| Boundary                                  | Pre-existing contract               |
|-------------------------------------------|-------------------------------------|
| Postgres pipeline DB                      | SQL schema in `migrations/NNN_*.sql` |
| Beets SQLite library DB                   | beets's own schema (Plex, Meelo, tagging-workspace already read it) |
| slskd HTTP API                            | external service                    |
| MusicBrainz mirror HTTP                   | external service                    |
| ffmpeg / ffprobe / sox subprocess         | each tool's CLI contract            |

`rusqlite`/`sqlx` read SQLite and Postgres the same way `sqlite3` and
`psycopg2` do; `reqwest` calls slskd and MB the same way `requests`
does. None of those become rewrite costs.

Consequence: each Python subsystem — web server, pipeline orchestrator,
slskd polling, dispatch, importer worker, search plan generator, CLI —
ports to Rust independently, in any order, without touching the binary
contract or coordinating with other ports. The binary becomes the
public API of the new repo. There is no "dual-stack period" cost
beyond ordinary co-existence on shared databases.

**The discipline that keeps this true:** do not introduce a second
invented Python↔Rust boundary. No sidecar daemon, no second helper
subprocess, no Python-side REST shim for Rust to call, no shared
in-memory state. If a future Rust subsystem needs something currently
Python-internal, route it through the binary or through Postgres.
Both languages already speak both fluently; neither needs a third
channel. Every additional invented boundary linearly multiplies the
maintenance cost of the migration; the win in this design is keeping
that count at exactly one.

A secondary but real win: the binary's contract is small enough to
write down completely (two subcommands, two flags, six wire types).
That's the kind of contract a single PR lands and a single test
suite pins. The contour of the boundary is stable and reviewable.
The same exercise on "what cratedigger.py does today" couldn't be
written down, because half of it is implicit in shared module state,
ambient subprocess env, and undocumented invariants. Reducing the
total contract surface is itself the architectural improvement.

---

## 10. Drop the duplicate-detection seam entirely

The single biggest scope cut. Beets's duplicate-detection machinery
(the `find_duplicates` call, the `resolve_duplicate` prompt, the
four-action `keep|remove|merge|skip` enum, `should_remove_duplicates`,
`should_merge_duplicates`) exists because beets's matcher can return
multiple candidates and a human picks one, *possibly mid-flight
discovering* "I already have this." That's a UX for the beets
interactive importer.

Cratedigger doesn't have that UX or that ambiguity. It pre-selects
the MBID. Two scenarios cover everything `import_one` actually does:

1. **Same MBID already in library** → re-import / upgrade. Right
   primitive: an upsert. Delete the existing row + its files,
   insert new. No question to ask.
2. **Different MBID, same artist/album** → curated-collection invariant
   says always keep both. No question to ask.

Neither needs a "duplicate" concept. The whole defense-in-depth surface
that grew up around the Palo Santo data-loss incident exists to prevent
beets's duplicate-detection seam from firing wrongly:

- `_assert_duplicate_keys_include_mb_albumid` startup guard
- `_find_duplicates_with_mapped_release_ids` + the
  `BeetsImportTask.find_duplicates` monkey-patch
- `duplicate_keys` restricted to release-IDs-only via beets config
- `DuplicateRemoveCandidate` Struct
- `resolve_duplicate` wire message + four-action response
- `_duplicate_remove_guard_failure` + exit code 7
- "Authorize remove only on exactly-one same-release duplicate" rule

All of it goes when the seam goes. The Palo Santo bug class becomes
structurally impossible because the binary never asks beets a
question that beets can answer wrongly.

What survives in much smaller form: the **same-MBID upsert + split-brain
check** (§6.1). That is *not* duplicate detection — it's an invariant
assertion on a MBID-scoped query. The `split_brain_detected` failure
mode replicates exit code 7's downstream effect (stage to manual
review) without any of the wire-protocol surface.

Rough win: ~13 commits + 5 review rounds + 2 strategy reversals of
historical pain disappear from the codebase. The single most fragile
seam in cratedigger today is gone, not just papered over.

This was the user's call: the duplicate-detection saga was a 50-commit
detour that traced back to a misconfigured YAML key. The lesson is to
not reproduce surfaces that exist only to absorb a bug class we don't
need to inherit.

---

## 11. References

Source files (cratedigger repo, all paths absolute):

- `harness/beets_harness.py` — wire protocol, duplicate-key patch,
  MBID swap audit, plugin loading
- `harness/import_one.py` — orchestration, conversion, quality gate,
  post-import verification, ImportResult emission
- `harness/run_beets_harness.sh` — env extraction from Nix-wrapped beet
- `lib/beets.py::beets_validate` — strict-typed wire decoder (the
  contract a Rust binary must honor)
- `lib/beets_db.py` — SQLite schema/columns the Rust binary
  populates
- `lib/beets_album_op.py` — `beet remove`/`beet move` surface that
  becomes direct SQLite mutation in Rust
- `lib/quality.py` — Struct definitions for all wire-boundary types,
  `QualityRankConfig`, `ImportResult`, `ValidationResult`
- `lib/import_dispatch.py::run_import_one` — the caller whose argv
  shape and `__IMPORT_RESULT__` parser the Rust binary must satisfy
- `docs/beets-rpc-design.md` — predecessor design (Python long-lived
  RPC); method surface table is directly applicable
- `docs/beets-primer.md` — operational notes
- `docs/solutions/runtime-errors/subprocess-text-mode-utf8-strict-decode-crash.md`
- `docs/solutions/testing/mocked-contract-tests-miss-helper-mirror-integration-bugs.md`
- `CLAUDE.md` § "Resolved canonical RCs" — Palo Santo + Lucksmiths
  RCs documented
- `~/.config/beets/config.yaml` — ground-truth plugin list, path
  templates, match thresholds, duplicate_keys

Key tests:

- `tests/test_harness_config_guard.py` — duplicate-keys assertion
  regression
- `tests/test_beets_validation.py` — env kwarg + msgspec decode
- `tests/test_no_dual_load.py` — module-load hygiene
- `tests/test_import_one_stages.py` — UTF-8 stderr regression
- `tests/test_integration_slices.py` — slice tests that exercise
  real harness invocation
