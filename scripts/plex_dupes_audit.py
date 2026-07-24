"""
Build a JSON snapshot of all duplicate (artist, title, year) Plex album groups
classified as same-folder (track-rename ghost) or diff-folder (folder-rename ghost).

Inputs:
  - XML at /tmp/plex-asciify-cleanup/plex_albums.{before,after}.xml
  - Plex token via $PLEX_TOKEN env var

Output: JSON to stdout. Run as:
  PLEX_TOKEN=$(ssh doc2 'sudo cat /run/cratedigger-secrets/PLEX_TOKEN') \
    nix-shell --run "python3 scripts/plex_dupes_audit.py /tmp/plex-asciify-cleanup/plex_albums.before.xml" \
    > /tmp/plex-asciify-cleanup/dupes.before.json
"""
from __future__ import annotations

import json
import os
import sys
from collections import defaultdict
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from typing import TypedDict
import urllib.request
from xml.etree.ElementTree import Element

from defusedxml import ElementTree as ET

BASE = "https://plex.ablz.au"


class _AlbumEntry(TypedDict):
    ratingKey: str | None
    title: str
    parentTitle: str
    year: str
    addedAt: int


class _FailureEntry(TypedDict):
    ratingKey: str | None
    error: str


class _RkInfo(TypedDict, total=False):
    files: list[str]
    folders: list[str]
    track_count: int


def fetch_children(rk: str | None, token: str) -> tuple[str | None, bytes]:
    url = f"{BASE}/library/metadata/{rk}/children?X-Plex-Token={token}"
    with urllib.request.urlopen(url, timeout=15) as r:
        return rk, r.read()


def _parse_children_xml(raw: bytes) -> Element:
    return ET.fromstring(raw, forbid_dtd=True)


def _load_albums(xml_path: str) -> list[_AlbumEntry]:
    albums: list[_AlbumEntry] = []
    root = ET.parse(xml_path, forbid_dtd=True).getroot()
    assert root is not None
    for d in root.findall('.//Directory'):
        if d.get('type') != 'album':
            continue
        albums.append({
            'ratingKey': d.get('ratingKey'),
            'title': d.get('title') or '',
            'parentTitle': d.get('parentTitle') or '',
            'year': d.get('year') or '',
            'addedAt': int(d.get('addedAt') or 0),
        })
    return albums


def main() -> None:
    xml_path = sys.argv[1]
    token = os.environ["PLEX_TOKEN"]
    albums = _load_albums(xml_path)

    groups: defaultdict[tuple[str, str, str], list[_AlbumEntry]] = defaultdict(list)
    for a in albums:
        key = (a['parentTitle'].lower(), a['title'].lower(), a['year'])
        groups[key].append(a)

    dup_groups: dict[tuple[str, str, str], list[_AlbumEntry]] = {
        k: v for k, v in groups.items() if len(v) > 1
    }
    all_rks: list[str | None] = [a['ratingKey'] for v in dup_groups.values() for a in v]
    print(f"total_albums={len(albums)} dup_groups={len(dup_groups)} rks_to_fetch={len(all_rks)}", file=sys.stderr)

    rk_data: dict[str | None, _RkInfo] = {}
    failures: list[_FailureEntry] = []
    done = 0
    with ThreadPoolExecutor(max_workers=12) as ex:
        futs: dict[Future[tuple[str | None, bytes]], str | None] = {
            ex.submit(fetch_children, rk, token): rk for rk in all_rks
        }
        for fut in as_completed(futs):
            rk = futs[fut]
            try:
                _, raw = fut.result()
                root = _parse_children_xml(raw)
                files: list[str] = []
                for part in root.findall('.//Part'):
                    f = part.get('file')
                    if f:
                        files.append(f)
                folders = sorted({os.path.dirname(f) for f in files})
                rk_data[rk] = {'files': files, 'folders': folders, 'track_count': len(files)}
                done += 1
                if done % 200 == 0:
                    print(f"progress {done}/{len(all_rks)}", file=sys.stderr)
            except Exception as e:
                failures.append({'ratingKey': rk, 'error': str(e)})

    # Retry failures once
    if failures:
        print(f"retrying {len(failures)} failures", file=sys.stderr)
        retry_rks: list[str | None] = [f['ratingKey'] for f in failures]
        failures = []
        with ThreadPoolExecutor(max_workers=4) as ex:
            futs = {ex.submit(fetch_children, rk, token): rk for rk in retry_rks}
            for fut in as_completed(futs):
                rk = futs[fut]
                try:
                    _, raw = fut.result()
                    root = _parse_children_xml(raw)
                    files = [f for f in (p.get('file') for p in root.findall('.//Part')) if f]
                    folders = sorted({os.path.dirname(f) for f in files})
                    rk_data[rk] = {'files': files, 'folders': folders, 'track_count': len(files)}
                except Exception as e:
                    failures.append({'ratingKey': rk, 'error': str(e)})

    group_rows: list[dict[str, object]] = []
    for key, members in sorted(dup_groups.items()):
        parent_title, title, year = key
        all_folders: set[str] = set()
        for m in members:
            all_folders |= set(rk_data.get(m['ratingKey'], {}).get('folders', []))
        classification = 'same_folder' if len(all_folders) == 1 else ('diff_folder' if len(all_folders) > 1 else 'unknown')
        group_rows.append({
            'parent_title': parent_title,
            'title': title,
            'year': year,
            'classification': classification,
            'folders_seen': sorted(all_folders),
            'members': [
                {
                    'ratingKey': m['ratingKey'],
                    'addedAt': m['addedAt'],
                    'track_count': rk_data.get(m['ratingKey'], {}).get('track_count', 0),
                    'folders': rk_data.get(m['ratingKey'], {}).get('folders', []),
                }
                for m in members
            ],
        })

    # Sanity counts
    from collections import Counter
    class_counts = Counter(g['classification'] for g in group_rows)
    out: dict[str, object] = {
        'total_albums': len(albums),
        'dup_groups_total': len(dup_groups),
        'fetch_failures': failures,
        'groups': group_rows,
        'summary': dict(class_counts),
    }
    print(f"classification: {dict(class_counts)}", file=sys.stderr)
    print(json.dumps(out, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
