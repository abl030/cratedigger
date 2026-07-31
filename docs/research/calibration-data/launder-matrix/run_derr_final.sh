#!/usr/bin/env bash
# Remaining Derrien work, then a final merged emit from cache for both tags.
set -u
M=/home/abl030/.claude/jobs/fb229c18/tmp/matrix
REPO=/home/abl030/cratedigger/.claude/worktrees/829-phase5-pr2
cd "$REPO" || exit 1
N44="8916,8917,8918,8919,8921,8922,8923,8924,8925,8926,8928,8929,8930,8932,8933,8934,8935"
ALL="genuine,null-flac,lame-v0,lame-v2,vorbis-q6,vorbis-q8,aacffm-256,aacffm-320,lame-cbr192,lame-cbr256,lame-cbr320,vorbis-q4,vorbis-q5,vorbis-q7,vorbis-q10,aacffm-128,aacffm-192,opus-96,opus-192,opus-256,lame-cbr128"
WMA="wma-128,wma-192,wma-320"

step() { echo "===== $1 START $(date +%T) ====="; nix-shell --run "$2"; echo "===== $1 END rc=$? $(date +%T) ====="; }

step "derr-opus-cbr128" "python3 $M/derrien.py --tag local  --variants opus-96,opus-192,opus-256,lame-cbr128 --workers 12"
step "derr-wma"         "python3 $M/derrien.py --tag localw --variants genuine,null-flac,$WMA --albums $N44 --workers 12"
step "derr-merge-local" "python3 $M/derrien.py --tag local  --variants $ALL --workers 2"
echo "DERRIEN DONE $(date +%T)"
