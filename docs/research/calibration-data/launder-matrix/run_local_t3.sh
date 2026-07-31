#!/usr/bin/env bash
# T3 resume.
# wmav2 cannot encode 96 kHz input (ffmpeg rc=234), so the WMA family runs on
# the 17 44.1k/16 albums only and gets its OWN tag, `localw`, rather than
# silently truncating the 19-album tag `local`.
set -u
M=/home/abl030/.claude/jobs/fb229c18/tmp/matrix
REPO=/home/abl030/cratedigger/.claude/worktrees/829-phase5-pr2
cd "$REPO" || exit 1
N44="8916,8917,8918,8919,8921,8922,8923,8924,8925,8926,8928,8929,8930,8932,8933,8934,8935"
ALL="null-flac,lame-v0,lame-v2,vorbis-q6,vorbis-q8,aacffm-256,aacffm-320,lame-cbr192,lame-cbr256,lame-cbr320,vorbis-q4,vorbis-q5,vorbis-q7,vorbis-q10,aacffm-128,aacffm-192,opus-96,opus-192,opus-256,lame-cbr128"
WMA="wma-128,wma-192,wma-320"

step() { echo "===== $1 START $(date +%T) ====="; nix-shell --run "$2"; echo "===== $1 END rc=$? $(date +%T) ====="; }

step "build-wma"    "python3 $M/build.py --variants $WMA --albums $N44 --workers 8"
step "build-cbr128" "python3 $M/build.py --variants lame-cbr128 --workers 8"

# seed the localw tag's controls from the already-computed local caches
mkdir -p "$M/measure_cache/localw" "$M/derrien_cache/localw"
for v in genuine null-flac; do
  cp -n "$M/measure_cache/local/${v}__"*.json "$M/measure_cache/localw/" 2>/dev/null
  cp -n "$M/derrien_cache/local/${v}__"*.json "$M/derrien_cache/localw/" 2>/dev/null
done

step "measure-local"  "python3 $M/measure.py --tag local  --variants genuine,$ALL --workers 8"
step "measure-localw" "python3 $M/measure.py --tag localw --variants genuine,null-flac,$WMA --albums $N44 --workers 8"
echo "T3 DONE $(date +%T)"
