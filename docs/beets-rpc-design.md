# Beets RPC — design sketch

**Status:** draft, not implemented. This document captures the
proposed architecture for replacing cratedigger's subprocess-based
beets invocation with a long-lived JSON-RPC protocol. Nothing here
has been built yet; the intent is to review the shape before we start
porting callsites.

**Why now:** 2026-04-21 outage. `cratedigger-web`'s Nix wrapper leaked
`${src}/lib` onto PYTHONPATH. Every force-import's post-import
`beet remove` subprocess inherited that PYTHONPATH, loaded our
`lib/beets.py` as the top-level `beets` module (shadowing the real
PyPI package), and crashed on `import msgspec`. Three split-brain rows
accumulated for one MBID before we caught it. The fix was two lines
in `nix/module.nix`, but the underlying problem is that every
subprocess call to `beet` carries a hidden contract over PYTHONPATH,
HOME, PATH, stdin, exit codes, and rc semantics that we have to get
right at every single callsite.

## What the subprocess contract has cost us

Chronology of bugs caused solely by the `sp.run(["beet", ...])`
pattern:

| Date       | Bug                                                     | Fix                           |
|------------|---------------------------------------------------------|-------------------------------|
| PR #131 P3 | harness's systemd PATH missed `beet`                    | `beet_bin()` helper           |
| #99        | Discogs plugin silent 0 candidates because HOME=/root   | `beets_subprocess_env()`      |
| `ac07aa2`  | `beet remove` waited on stdin prompt, got EOF, rc=1     | pipe `y\n` to stdin           |
| #127       | `beet move` `TimeoutExpired` left half-moved state      | catch, classify, surface      |
| 2026-04-21 | `cratedigger-web` PYTHONPATH leak shadowed `beets`      | drop `${src}/lib` from exports|

Every fix was local. None generalised. The next fix will be for a
hidden env/IO dependency we haven't hit yet.

## Who spawns beet today

Production callsites (`grep "sp\\.run\\|sp\\.Popen" -- lib/ harness/`):

- `lib/beets.py::beets_validate` — invokes `run_beets_harness.sh`
  for the pre-import validation flow. Already uses a JSON protocol
  over stdin/stdout, but spawns fresh each time.
- `lib/beets_album_op.py::_run_beet_op` — the centralised `beet
  remove` / `beet move` wrapper (issue #133). Every post-import
  stale-row cleanup, ban-source, and sibling canonicalization goes
  through here.
- `lib/release_cleanup.py::remove_album_by_selectors` — iterates
  selectors (`mb_albumid:X`, `discogs_albumid:Y`) delegating to the
  op wrapper.
- `harness/import_one.py` — spawns the harness and then spawns
  `beet move` for sibling canonicalization, all in the beets env.
- `lib/import_dispatch.py` — spawns `import_one.py` (itself running
  in the beets env).

Five entry points, three wrappers around them, two env-setup
functions. The shape is correct for "one-shot subprocess per op"
but the cumulative surface is where the bugs live.

## Design

### Single long-lived process

Replace every `sp.run([beet, ...])` with an RPC to a single
long-lived Python process running inside beets' env. Call it
`beets_rpc.py`. It:

1. Imports beets once at startup — `beets.library.Library(db_path)`,
   `beets.config.read()`, plugins loaded.
2. Reads newline-delimited JSON requests from stdin.
3. Executes each request via the beets Python API (not subprocess).
4. Writes newline-delimited JSON responses to stdout.
5. Emits log lines to stderr, already structured as
   `{"level": "...", "msg": "..."}`.
6. Exits cleanly on EOF or `{"method": "shutdown"}`.

The process is spawned once per cratedigger cycle (≈5 min) and
shut down when the cycle ends. For `cratedigger-web`, one per
request — or pooled, TBD (see Open questions).

### Wire protocol — newline-delimited JSON

Existing harness already does this. Concrete shape:

```json
→ {"id": 1, "method": "remove_album_by_id", "params": {"album_id": 5994, "delete_files": true}}
← {"id": 1, "ok": true, "result": {"deleted_paths": ["/Beets/Unter Null/Sick Fuck/01.mp3", ...]}}

→ {"id": 2, "method": "get_album_by_mbid", "params": {"mb_albumid": "7df3ad34-..."}}
← {"id": 2, "ok": true, "result": {"id": 10327, "path": "/Beets/...", "items": [...]}}

→ {"id": 3, "method": "validate_import", "params": {"path": "/Incoming/...", "target_mbid": "...", "distance_threshold": 0.15}}
← {"id": 3, "ok": true, "result": {"candidates": [...], "mapping": [...], ...}}

→ {"id": 4, "method": "run_import", "params": {"path": "/Incoming/...", "target_mbid": "...", "apply_metadata": true, "target_format": "lossless"}}
← {"id": 4, "ok": true, "result": {"imported_beets_id": 10338, "new_path": "/Beets/...", "conversion": {...}, ...}}

→ {"id": 5, "method": "shutdown"}
← {"id": 5, "ok": true, "result": null}
```

Errors:

```json
← {"id": 2, "ok": false, "error": {"reason": "not_found", "detail": "no album with mb_albumid=...", "selector": "mb_albumid:..."}}
```

`error.reason` is a `Literal[...]` enum mirroring the existing
`BeetsOpFailureReason` so we don't lose typing granularity:
`"timeout" | "nonzero_rc" | "exception" | "not_found" | "validation_error" | "conversion_error"`.

### Method surface (initial)

Minimal to collapse current callsites. More added as needed.

| Method                     | Replaces                                      | Notes                                                    |
|---------------------------|-----------------------------------------------|----------------------------------------------------------|
| `validate_import`         | `lib/beets.py::beets_validate` + harness     | Pretend import with `--search-id`, returns full candidates |
| `run_import`              | `harness/import_one.py` main flow             | Full import; emits same `ImportResult` shape              |
| `remove_album_by_id`      | `beets_album_op.remove_album`                 | PK match, safe by construction                            |
| `remove_by_selector`      | `beets_album_op.remove_by_selector`           | For ban-source Discogs/MB selector loop                  |
| `move_album`              | `beets_album_op.move_album`                   | PK match, re-reads new path, fixes perms                 |
| `get_album_by_id`         | `beets_db.get_album_info` (fetched via lib)   | Replaces direct SQLite read when we need authoritative   |
| `list_albums_by_mbid`     | `beets_db.get_release_ids_by_album_id`        | For split-brain detection; returns list of ids           |
| `fix_library_modes`       | `lib.permissions.fix_library_modes`           | Can stay in cratedigger side; included only if simpler    |
| `shutdown`                | —                                             | Clean exit                                                |

We keep `lib/beets_db.py` read-only queries as-is — they hit the
beets SQLite directly and are much cheaper than an RPC round-trip.
The RPC layer is for **writes, imports, and authoritative reads
that need beets' own cache coherence**.

### Client shape

`lib/beets_rpc_client.py`, new file. Replaces `lib/beets_album_op.py`
subprocess primitives.

```python
class BeetsRpc:
    def __init__(self, binary: str = "beets-rpc", env: dict | None = None) -> None:
        self._proc = sp.Popen([binary], ...)
        self._lock = threading.Lock()  # serialise requests; RPC is single-threaded
        self._next_id = 1
        self._stderr_drainer = threading.Thread(...)  # fan out to logger

    def call(self, method: str, params: dict) -> Any:
        """Block until response. Raises RpcError on error."""
        with self._lock:
            self._proc.stdin.write(json.dumps({"id": ..., "method": method, "params": params}) + "\n")
            self._proc.stdin.flush()
            line = self._proc.stdout.readline()
        resp = json.loads(line)
        if not resp["ok"]:
            raise RpcError(resp["error"])
        return resp["result"]

    def __enter__(self): return self
    def __exit__(self, *a): self.call("shutdown", {}); self._proc.wait(timeout=5)

    # Typed helpers per method
    def remove_album_by_id(self, album_id: int, *, delete_files: bool = True) -> RemoveResult: ...
    def run_import(self, path: str, target_mbid: str, ...) -> ImportResult: ...
    # etc.
```

Typed helpers return `msgspec.Struct` types, decoded via
`msgspec.convert` — same wire-boundary pattern as
`lib/beets.py::beets_validate` today.

### Lifecycle

- `cratedigger.service` cycle: spawn one `BeetsRpc` at the top of
  `main()` via `ctx.beets_rpc = BeetsRpc()`. Every callsite that
  currently takes a path and spawns a subprocess takes
  `ctx.beets_rpc` instead. The context manager guarantees shutdown
  on both normal exit and crash.
- `cratedigger-web`: one per pipeline endpoint call. Force-import,
  manual-import, ban-source all instantiate their own for the
  duration of the call. Alternatively, a pool — see Open questions.
- `pipeline-cli` commands that touch beets: same as web.

### What it kills

Once all callsites are ported:

- **`lib/beets_album_op.py`** → replaced by `lib/beets_rpc_client.py`.
  5 invariants collapse to "spawn rpc, call method". No stdin
  prompts (we pass `delete_files=True` in JSON). No rc parsing (we
  get `{"ok": false, "error": {...}}`). No PATH/PYTHONPATH
  inheritance — RPC process has its own env, set once.
- **`lib/util.py::beets_subprocess_env`** → delete. Only the RPC
  wrapper sets HOME now.
- **`lib/util.py::beet_bin`** → delete. Only the RPC wrapper needs
  to find `beet`, and it's in its own PATH.
- **`lib/release_cleanup.py`**'s multi-selector iteration → becomes
  a single RPC call with a list of selectors. Per-selector failure
  reporting stays (the RPC returns a list of `BeetsOpFailure`).
- **`harness/run_beets_harness.sh`** → delete. One shell wrapper
  less. Replaced by the RPC spawn.
- **`harness/beets_harness.py`** → may stay as an implementation
  detail of `validate_import` inside the RPC, but no longer a
  separate subprocess entry point.
- **The beet-stdin-prompt fix in `beets_album_op.py`** (`ac07aa2`)
  → gone; we never touch the `beet` CLI.
- **The PYTHONPATH guard in `tests/test_nix_module.py`** → still
  valid (it protects against a different class of bug), but less
  load-bearing because no subprocess inherits the leak.

Rough sizing: ~1200 lines of cratedigger code, plus the harness
shell script, replaced by ~400 lines of RPC server + ~200 lines of
client. Net −800 lines, with the deleted code being exactly the
fragile subprocess-contract code.

## Open questions

1. **One RPC process per cycle, or a daemon?**
   - Per-cycle (preferred start): simpler lifecycle, no
     long-term resource concerns, startup cost is beets' config
     load + plugin init (≈1–2s, dominated by the Discogs plugin).
     Fine for a 5-minute cycle.
   - Long-lived daemon on doc2: avoids startup cost entirely, but
     introduces restart-on-crash logic, the beets DB staleness
     question (does beets cache albums across requests?), and
     systemd-unit management.
   - `cratedigger-web` will be the stress test — if force-imports
     are rare enough, per-call is fine; if users force-import in
     bursts, pooling makes sense.

2. **Concurrency.** Beets' `library.Library` is not thread-safe
   (SQLite, global `config` object). The RPC must serialise. For
   the cratedigger cycle that's fine — we already serialise imports
   via the advisory lock. For the web service it means force-imports
   are queued sequentially, which is also fine (they're rare).

3. **Crash recovery.** If the RPC process dies mid-import, we
   inherit the current problem: new album on disk, stale row still
   in beets. Mitigation options:
   - Client detects subprocess death → respawns → runs a reconcile
     pass (`list_albums_by_mbid` on the target MBID, dispatch
     cleanup if split-brain).
   - Every `run_import` call takes a `request_id` so the RPC can
     write a checkpoint file (`/var/lib/cratedigger/rpc-state/<req>.json`)
     before and after each destructive step. Client on restart
     reads checkpoints, finishes or rolls back.
   - Both of the above are small. Per-cycle RPC also means the
     blast radius of a crash is one album, not a whole batch.

4. **Stderr draining.** Beets logs to stderr via its own logger.
   The RPC process runs beets' logging to stderr (same as today's
   harness). Client reads stderr in a thread, fans out to
   cratedigger's logger with a `[rpc]` prefix. Already how the
   existing harness works — just formalise it.

5. **Testing.** The client has a `FakeBeetsRpc` with recorded
   method calls (same pattern as `FakePipelineDB`,
   `FakeSlskdAPI`). Integration slice tests spawn a real RPC
   against a temp beets DB (already how
   `tests/test_conversion_e2e.py` style works). The RPC server
   itself is tested inside the beets dev shell — pointing at a
   temp library + temp music dir. Full matrix is cheap because
   spawning the RPC has no Nix deploy step.

6. **Does beets' library support what we need?**
   - `Library.get_album(id)` — yes.
   - `lib.albums(query)` — yes, same as harness uses.
   - `album.remove(delete=True)` — yes, one-row remove, same API
     as `util.remove` at item level but scoped to one album.
   - `album.move()` — yes.
   - `ImportSession` — yes (that's how the harness works today).
   - Plugin side effects (fetchart, lastgenre) fire on import —
     same as today.
   - **TBD:** Discogs plugin's `--search-id` numeric route. Need
     to verify it works when we drive `ImportSession` directly
     (without the CLI argparse layer). Worst case: we pre-fetch
     the Discogs candidates ourselves and hand them to the session.

7. **Discogs ↔ MusicBrainz dual-source.** Current code passes
   MBIDs and numeric Discogs IDs through the same field
   (`mb_release_id` / `album_requests.mb_release_id`). The RPC
   should keep this opaque — `validate_import(target_release_id=...)`
   where the RPC inspects the format and dispatches to the right
   plugin. `detect_release_source()` from `lib/quality.py` moves
   into the RPC server.

## Porting plan

Smallest-to-biggest, one PR per step. Each step is shippable on
its own — the old subprocess path stays until every callsite is
ported.

1. **Build `beets_rpc.py` server with `remove_album_by_id` only.**
   Write it in `harness/beets_rpc.py`. Update
   `harness/run_beets_harness.sh` to be a no-arg spawn. Add unit
   tests for the one method. Don't wire into production yet.
2. **Build `BeetsRpc` client with typed helpers.** Add
   `FakeBeetsRpc` in `tests/fakes.py`. Write integration slice in
   `tests/test_integration_slices.py` that spawns a real RPC
   against an ephemeral beets DB and asserts `remove_album_by_id`
   works.
3. **Port `beets_album_op.remove_album` to delegate to
   `BeetsRpc.remove_album_by_id`.** Keep the old subprocess code
   behind a feature flag (`CRATEDIGGER_BEETS_RPC=1`) for one
   deploy cycle to confirm parity. Then delete.
4. **Add `remove_by_selector`, port the rest of
   `beets_album_op`.**
5. **Add `move_album`, port the sibling canonicalization in
   `harness/import_one.py`.**
6. **Add `validate_import`, port `lib/beets.py::beets_validate`.
   Delete `run_beets_harness.sh` + `beets_harness.py` as a
   standalone subprocess entry point** (code moves into RPC
   server).
7. **Add `run_import`, port `harness/import_one.py` main flow.**
   This is the big one — needs checkpointing (open question 3).
8. **Delete `beets_subprocess_env`, `beet_bin`, the PYTHONPATH
   guard test.** Scar tissue audit. Pyright clean, test suite
   green.

Each step keeps the old path working. At step 8 we delete the
old path in one mechanical commit.

Total: roughly 2 weeks of focused work, probably 3–4 weeks calendar.
Each step is independently shippable, so we can pause at any
boundary.

## What this does NOT fix

- The data-loss root cause is still in beets (`find_duplicates`
  can return cross-MBID siblings in some scenarios we haven't
  reproduced). The harness's `"keep"` workaround stays; the RPC
  inherits it.
- Quality decisions remain in `lib/quality.py`. The RPC is an IO
  boundary, not a decision layer.
- Beets version upgrades can still break us if they change
  library APIs. Testing against the dev shell's pinned beets is
  the same safety net we have today.

## Decision

Do option (1) now (PYTHONPATH fix, this album cleanup) — shipped
as `fix(nix): don't leak src/lib and src/web into subprocess
PYTHONPATH`. Then start option (2) with step 1 of the porting
plan above. Review this doc before writing the first line of
`beets_rpc.py`.
