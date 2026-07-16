#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

if [ $# -eq 0 ]; then
    nix-shell --run "pyright --threads 4 cratedigger.py lib/*.py album_source.py"
else
    nix-shell --run "pyright --threads 4 $*"
fi
