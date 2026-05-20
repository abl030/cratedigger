"""
Build a JSON snapshot of all duplicate (artist, title, year) Plex album groups
classified as same-folder (track-rename ghost) or diff-folder (folder-rename ghost).

Inputs:
  - XML at /tmp/plex-asciify-cleanup/plex_albums.{before,after}.xml
  - Plex token via $PLEX_TOKEN env var

Output: JSON to stdout. Run as:
  PLEX_TOKEN=$(ssh doc2 'sudo cat /run/cratedigger-secrets/PLEX_TOKEN') \
    python3 build_dupes_json.py /tmp/plex-asciify-cleanup/plex_albums.before.xml \
    > /tmp/plex-asciify-cleanup/dupes.before.json
"""
import json
import os
import ssl
import sys
import xml.etree.ElementTree as ET
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
import urllib.request

XML_PATH = sys.argv[1]
TOKEN = os.environ["PLEX_TOKEN"]
BASE = "https://plex.ablz.au"
CTX = ssl.create_default_context()
CTX.check_hostname = False
CTX.verify_mode = ssl.CERT_NONE

def fetch_children(rk):
    url = f"{BASE}/library/metadata/{rk}/children?X-Plex-Token={TOKEN}"
    with urllib.request.urlopen(url, context=CTX, timeout=15) as r:
        return rk, r.read()

albums = []
for d in ET.parse(XML_PATH).getroot().findall('.//Directory'):
    if d.get('type') != 'album':
        continue
    albums.append({
        'ratingKey': d.get('ratingKey'),
        'title': d.get('title') or '',
        'parentTitle': d.get('parentTitle') or '',
        'year': d.get('year') or '',
        'addedAt': int(d.get('addedAt') or 0),
    })

groups = defaultdict(list)
for a in albums:
    key = (a['parentTitle'].lower(), a['title'].lower(), a['year'])
    groups[key].append(a)

dup_groups = {k: v for k, v in groups.items() if len(v) > 1}
all_rks = [a['ratingKey'] for v in dup_groups.values() for a in v]
print(f"total_albums={len(albums)} dup_groups={len(dup_groups)} rks_to_fetch={len(all_rks)}", file=sys.stderr)

rk_data = {}
failures = []
done = 0
with ThreadPoolExecutor(max_workers=12) as ex:
    futs = {ex.submit(fetch_children, rk): rk for rk in all_rks}
    for fut in as_completed(futs):
        rk = futs[fut]
        try:
            _, raw = fut.result()
            root = ET.fromstring(raw)
        except Exception as e:
            failures.append({'ratingKey': rk, 'error': str(e)})
            continue
        files = []
        for part in root.findall('.//Part'):
            f = part.get('file')
            if f:
                files.append(f)
        folders = sorted({os.path.dirname(f) for f in files})
        rk_data[rk] = {'files': files, 'folders': folders, 'track_count': len(files)}
        done += 1
        if done % 200 == 0:
            print(f"progress {done}/{len(all_rks)}", file=sys.stderr)

# Retry failures once
if failures:
    print(f"retrying {len(failures)} failures", file=sys.stderr)
    retry_rks = [f['ratingKey'] for f in failures]
    failures = []
    with ThreadPoolExecutor(max_workers=4) as ex:
        futs = {ex.submit(fetch_children, rk): rk for rk in retry_rks}
        for fut in as_completed(futs):
            rk = futs[fut]
            try:
                _, raw = fut.result()
                root = ET.fromstring(raw)
                files = [f for f in (p.get('file') for p in root.findall('.//Part')) if f]
                folders = sorted({os.path.dirname(f) for f in files})
                rk_data[rk] = {'files': files, 'folders': folders, 'track_count': len(files)}
            except Exception as e:
                failures.append({'ratingKey': rk, 'error': str(e)})

out = {
    'total_albums': len(albums),
    'dup_groups_total': len(dup_groups),
    'fetch_failures': failures,
    'groups': []
}

for key, members in sorted(dup_groups.items()):
    parent_title, title, year = key
    all_folders = set()
    for m in members:
        all_folders |= set(rk_data.get(m['ratingKey'], {}).get('folders', []))
    classification = 'same_folder' if len(all_folders) == 1 else ('diff_folder' if len(all_folders) > 1 else 'unknown')
    out['groups'].append({
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
class_counts = Counter(g['classification'] for g in out['groups'])
out['summary'] = dict(class_counts)
print(f"classification: {dict(class_counts)}", file=sys.stderr)
print(json.dumps(out, indent=2, ensure_ascii=False))
