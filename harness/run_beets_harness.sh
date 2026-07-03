#!/usr/bin/env bash
# Runs beets_harness.py on the cratedigger-pinned beets python env.
#
# Usage:
#   ./harness/run_beets_harness.sh /mnt/virtio/Music/AI/SomeArtist/SomeAlbum
#   ./harness/run_beets_harness.sh --pretend /mnt/virtio/Music/AI/SomeArtist
#
# The harness communicates over stdin/stdout using newline-delimited JSON.
# Beets logs go to stderr.
#
# The interpreter comes from CRATEDIGGER_BEETS_PYTHON — exported by
# lib/util.py::beets_subprocess_env() from the runtime config's
# [Beets] python key (rendered by the NixOS module from the pinned beets
# env), or by the dev shell's shellHook. The harness resolves its beets
# config via BEETSDIR (set by the same env helper) — never a per-user
# ~/.config/beets. The Home-Manager-era wrapper archaeology (grepping the
# HM beet wrapper's internals for its interpreter and site-packages) is
# gone (tier-2 plan R6): a missing interpreter is an actionable error,
# not a silent fallback to whatever profile exists on the host.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
HARNESS="$SCRIPT_DIR/beets_harness.py"

if [[ -z "${CRATEDIGGER_BEETS_PYTHON:-}" ]]; then
    echo "Error: CRATEDIGGER_BEETS_PYTHON is not set. It is exported by" >&2
    echo "beets_subprocess_env() from [Beets] python in config.ini (the" >&2
    echo "NixOS module renders it), or by the dev shell. There is no" >&2
    echo "Home Manager fallback." >&2
    exit 1
fi

exec "$CRATEDIGGER_BEETS_PYTHON" "$HARNESS" "$@"
