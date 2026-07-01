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
nix-shell --run "python3 -m unittest tests.<mod> -v"   # single module
nix-shell --run "python3 -c '...'"                     # one-off
```

NEVER run `python3` outside nix-shell in this repo.
