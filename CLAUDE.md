# Cratedigger

Music archival pipeline: searches Soulseek via slskd, validates downloads
against an exact MusicBrainz/Discogs release through Beets, verifies quality,
imports. Web UI at `music.ablz.au`. The pipeline DB (PostgreSQL) owns
acquisition requests; Beets owns the library.

Start each session with `hostname` (`proxmox-vm` = doc1, which is usually where
you are) and read `.claude/memory/MEMORY.md`. Work in a git worktree and remove it when done.

## Product invariants

- **Exact pressing identity.** A request names one MB release or Discogs
  release. Never substitute a sibling pressing, never match by release group or
  `candidate_index`. Duplicate editions in the library are intentional: never
  merge or delete them.
- **Never stop searching.** No auto-throttling of hard-to-find requests.
- **Never auto-decide irreversible things** (replace, delete, accept a
  different pressing). Those are the operator's calls.
- **Failures restart, nothing parks.** On any failure, record audit evidence
  and return the request to a runnable state. No statuses that only a human can
  clear.
- Auto-import only `source='request'`; redownloads stage for manual review.
- Never `beet remove -d`. Beets mutations go through the harness and the
  existing lanes in `lib/beets*.py`.
- Import decisions live in one place:
  `lib/quality/pipeline.py::full_pipeline_decision_from_evidence`.
- Every operator action exists on both `pipeline-cli` and the web API, sharing
  one code path.
- Single operator: no backwards-compat shims, no committed one-shot backfill
  scripts. Schema changes are new numbered files in `migrations/`; never edit
  a shipped one.

## Infrastructure

- doc1 `192.168.1.29`: this repo, dev host, nixosconfig (`~/nixosconfig`,
  Forgejo-hosted, signed commits).
- doc2 `192.168.1.35`: runs cratedigger and Beets. The pipeline DB is at
  `10.20.0.11:5432`, reachable only from doc2.
- Shared storage `/mnt/virtio`: Beets DB/library, staging, processing tree.

```bash
ssh doc2 'sudo journalctl -u cratedigger --since "5 min ago"'
ssh doc2 'sudo systemctl start cratedigger --no-block'   # always --no-block
# DB queries: SQL via stdin, pgpass is env-format
ssh doc2 'export PGPASSWORD=$(sudo cat /run/secrets/cratedigger-pgpass | grep "^PGPASSWORD=" | cut -d= -f2); pipeline-cli query -' <<'SQL'
SELECT ...;
SQL
```

`pipeline-cli query` is read-only unless given `--write --confirm WRITE -`.

## Working

- Python always runs in the dev shell: `nix-shell --run "..."`.
- Tests: `nix-shell --run "bash scripts/run_tests.sh"` runs everything and
  reports every failure at once. Green before merge. CI does not run tests;
  the nightly daily gate keeps `main` green, so a red branch is your change.
- Merge PRs with "Create a merge commit". Merged `main` deploys itself
  nightly; `scripts/deploy.sh` (from the shared checkout) deploys now.
- After editing `.claude/agents/` or `.mcp.json`:
  `nix-shell --run "python3 tools/generate-ai-adapters.py"`. `AGENTS.md`
  symlinks here.

Subsystem detail lives in `docs/`; past lessons in `docs/solutions/`.
