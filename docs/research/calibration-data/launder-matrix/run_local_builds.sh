#!/usr/bin/env bash
# Local-encoder family driver: build + measure, tier by tier.
# Scratch. Not tracked.
set -u
M=/home/abl030/.claude/jobs/fb229c18/tmp/matrix
REPO=/home/abl030/cratedigger/.claude/worktrees/829-phase5-pr2
cd "$REPO" || exit 1

T1="null-flac,lame-v0,lame-v2,vorbis-q6,vorbis-q8,aacffm-256,aacffm-320"
T2="lame-cbr192,lame-cbr256,lame-cbr320,vorbis-q4,vorbis-q5,vorbis-q7,vorbis-q10,aacffm-128,aacffm-192"
T3="opus-96,opus-192,opus-256,wma-128,wma-192,wma-320,lame-cbr128"

step() {  # name, cmd
  echo "===== $1 START $(date +%T) ====="
  nix-shell --run "$2"
  echo "===== $1 END rc=$? $(date +%T) ====="
}

step "build-T1"   "python3 $M/build.py --variants $T1 --workers 8"
step "measure-T1" "python3 $M/measure.py --tag local --variants genuine,$T1 --workers 6"
step "build-T2"   "python3 $M/build.py --variants $T2 --workers 8"
step "measure-T2" "python3 $M/measure.py --tag local --variants genuine,$T1,$T2 --workers 6"
step "build-T3"   "python3 $M/build.py --variants $T3 --workers 8"
step "measure-T3" "python3 $M/measure.py --tag local --variants genuine,$T1,$T2,$T3 --workers 6"
echo "ALL BUILDS+MEASURES DONE $(date +%T)"
