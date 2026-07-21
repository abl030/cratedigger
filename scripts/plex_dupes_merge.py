"""
Merge duplicate Plex album rows produced by the May-18 asciify_paths rename.

Plex merge API:
    PUT /library/metadata/{primary_rk}/merge?ids={ghost_rk1},{ghost_rk2},...

The merge is destructive in the sense that ghost rows disappear; play counts /
ratings on the primary are preserved.

Strategy for each same-folder dup group:
  * Primary = member with the highest track_count. Tiebreaker: lowest
    ratingKey (older entry, more likely to carry play history).
  * Secondaries = every other member of the group.

Defaults to DRY RUN. Pass --commit to actually issue merges.

Usage:
    PLEX_TOKEN=$(ssh doc2 'sudo cat /run/cratedigger-secrets/PLEX_TOKEN') \
      python3 merge_dupes.py dupes.after.json                  # dry-run
    PLEX_TOKEN=$(...)            python3 merge_dupes.py dupes.after.json --commit
    PLEX_TOKEN=$(...)            python3 merge_dupes.py dupes.after.json --commit --limit 5
"""
from __future__ import annotations

import argparse
import json
import os
import ssl
import sys
import urllib.request

import msgspec

BASE = "https://plex.ablz.au"
CTX = ssl.create_default_context()
CTX.check_hostname = False
CTX.verify_mode = ssl.CERT_NONE


class _MemberRow(msgspec.Struct):
    """One album row within a dup group, as emitted by plex_dupes_audit.py."""

    ratingKey: str
    track_count: int


class _GroupRow(msgspec.Struct):
    """One duplicate (artist, title, year) group, as emitted by plex_dupes_audit.py."""

    parent_title: str
    title: str
    year: str
    classification: str
    members: list[_MemberRow]


class _DupesFile(msgspec.Struct):
    """The subset of plex_dupes_audit.py's JSON output this script consumes."""

    groups: list[_GroupRow]
    summary: dict[str, int]


def pick_primary(members: list[_MemberRow]) -> _MemberRow:
    return sorted(
        members,
        key=lambda m: (-m.track_count, int(m.ratingKey)),
    )[0]


def merge(primary_rk: str, ghost_rks: list[str], token: str) -> tuple[int, bytes]:
    ids = ",".join(ghost_rks)
    url = f"{BASE}/library/metadata/{primary_rk}/merge?ids={ids}&X-Plex-Token={token}"
    req = urllib.request.Request(url, method="PUT")
    with urllib.request.urlopen(req, context=CTX, timeout=30) as r:
        return r.status, r.read()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("json_path", help="dupes.after.json from build_dupes_json.py")
    ap.add_argument("--commit", action="store_true",
                    help="Actually issue merges. Default is dry-run.")
    ap.add_argument("--limit", type=int, default=0,
                    help="Stop after this many groups (0 = all).")
    ap.add_argument("--only-same-folder", action="store_true", default=True,
                    help="Only merge same-folder groups. On by default.")
    ap.add_argument("--include-diff-folder", action="store_true",
                    help="Also merge diff-folder groups (DANGEROUS, may include "
                         "legitimate multi-edition albums; review first).")
    args = ap.parse_args()

    token = os.environ.get("PLEX_TOKEN")
    if not token:
        sys.exit("PLEX_TOKEN env var required")

    with open(args.json_path) as f:
        raw = json.load(f)
    data = msgspec.convert(raw, type=_DupesFile)
    print(f"Loaded {len(data.groups)} dup groups from {args.json_path}", file=sys.stderr)
    print(f"  same_folder: {data.summary.get('same_folder', 0)}", file=sys.stderr)
    print(f"  diff_folder: {data.summary.get('diff_folder', 0)}", file=sys.stderr)

    targets: list[tuple[_GroupRow, _MemberRow, list[_MemberRow]]] = []
    for g in data.groups:
        if g.classification == "diff_folder" and not args.include_diff_folder:
            continue
        if g.classification not in ("same_folder", "diff_folder"):
            continue
        if len(g.members) < 2:
            continue
        primary = pick_primary(g.members)
        ghosts = [m for m in g.members if m.ratingKey != primary.ratingKey]
        if not ghosts:
            continue
        targets.append((g, primary, ghosts))

    if args.limit:
        targets = targets[: args.limit]

    print(f"\nGroups to merge: {len(targets)}", file=sys.stderr)
    mode = "COMMIT" if args.commit else "DRY-RUN"
    print(f"Mode: {mode}\n", file=sys.stderr)

    ok = 0
    fail = 0
    for i, (g, primary, ghosts) in enumerate(targets, 1):
        ghost_rks = [m.ratingKey for m in ghosts]
        ghost_counts = [m.track_count for m in ghosts]
        label = f"{g.parent_title} / {g.title} ({g.year})"
        print(f"[{i:>4}/{len(targets)}] {label}")
        print(f"        keep rk={primary.ratingKey:>6} tracks={primary.track_count:>3}  "
              f"merge ghosts={list(zip(ghost_rks, ghost_counts))}")
        if not args.commit:
            continue
        try:
            status, _body = merge(primary.ratingKey, ghost_rks, token)
            if 200 <= status < 300:
                ok += 1
                print(f"        ✓ HTTP {status}")
            else:
                fail += 1
                print(f"        ✗ HTTP {status}")
        except Exception as e:
            fail += 1
            print(f"        ✗ ERROR: {e}")

    if args.commit:
        print(f"\nDone. ok={ok} fail={fail}", file=sys.stderr)
    else:
        print(f"\nDry-run only. Re-run with --commit to execute.", file=sys.stderr)


if __name__ == "__main__":
    main()
