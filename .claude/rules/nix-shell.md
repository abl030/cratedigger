---
paths:
  - "**/*.py"
  - "tests/**"
  - "shell.nix"
---

# Nix Shell — Required for All Python

ALL Python commands must run inside `nix-shell --run "..."`. The dev shell provides psycopg2, sox, ffmpeg, music-tag, beets. Running python3 directly causes import failures and skipped tests.

```bash
nix-shell --run "bash scripts/run_tests.sh"           # full suite
bash scripts/test.sh tests.<mod>                       # targeted + ambient gates
nix-shell --run "python3 -m unittest tests.<mod> -v"   # isolated test debugging
nix-shell --run "python3 -c '...'"                     # one-off
```

NEVER run `python3` outside nix-shell in this repo.

`nix develop --command <cmd>` is an exact equivalent and is what
`scripts/test.sh` runs internally, and what the final gate behind
`scripts/run_final_gate.sh` launches as its own child (issue #1229): `flake.nix`'s `devShells.default` IS `./nix/shell.nix`, the same
derivation `shell.nix` delegates to, so the environment — store paths,
`CRATEDIGGER_BEETS_PYTHON`, and the `scripts/test_tmpfs.sh` shellHook that
allocates `TMPDIR` — is identical. It is preferred for anything scripted
because it evaluates a locked flake and so hits Nix's own eval cache
(measured ~5.2s → ~0.5s per entry on a clean tree). The `nix-shell` forms
above stay correct and are fine to type by hand.
