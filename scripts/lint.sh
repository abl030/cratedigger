#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
PYRIGHT_THREADS=$(nproc)
if (( PYRIGHT_THREADS > 8 )); then
    PYRIGHT_THREADS=8
fi

if [ $# -eq 0 ]; then
    nix-shell --run "pyright --threads $PYRIGHT_THREADS cratedigger.py lib/*.py album_source.py"
else
    nix-shell --run "pyright --threads $PYRIGHT_THREADS $*"
fi
