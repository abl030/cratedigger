#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

if [ $# -eq 0 ]; then
    nix-shell --run "python3 -m unittest discover tests -v"
else
    nix-shell --run "python3 -m unittest $1 -v"
fi
