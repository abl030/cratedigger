# Web dev server

Use `scripts/web_dev_server.py` in two layers:

- `--data live-db` runs local route code against a real read-only PostgreSQL
  session and the backend host's filesystem.
- `--data prod-api` serves your checked-out frontend files locally while
  proxying `/api/*` to another read-only backend. Despite the name, it can
  target any remote base URL, not just prod.

For Wrong Matches, `live-db` must run on a host that can see the rejected
folders on disk. DB reachability alone is not enough because
`/api/wrong-matches/explorer` and `/api/wrong-matches/audio` open real files.
In this homelab, `doc1` and `doc2` qualify as backend hosts; Framework and
Windows do not unless the relevant paths are mounted locally.

Canonical remote-dev flow from any machine with SSH access:

1. Start a `live-db` backend on a host that can see the files. If that host
   does not have direct DB reachability, tunnel PostgreSQL first:
   ```bash
   ssh -N -L 15432:192.168.100.11:5432 doc2
   PIPELINE_DB_DSN=postgresql://cratedigger@127.0.0.1:15432/cratedigger \
     nix-shell --run "python3 scripts/web_dev_server.py --data live-db --host 127.0.0.1 --port 8096"
   ```
2. Tunnel that backend to your local machine if `8096` is not already reachable:
   ```bash
   ssh -N -L 18096:127.0.0.1:8096 <backend-host>
   ```
3. On your local checkout, serve the frontend against the tunneled backend:
   ```bash
   nix-shell --run "python3 scripts/web_dev_server.py --data prod-api --prod-base-url http://127.0.0.1:18096 --host 127.0.0.1 --port 8096"
   ```

Open `http://127.0.0.1:8096`. This gives live reload for local `web/` edits
without exposing the Postgres port to the laptop. The proxy forwards `Range`
headers, so Wrong Matches audio playback and scrubbing still work through the
tunnel.

`--beets-db` is optional in this flow. Wrong Matches does not need it; only
beets-backed library badges and lookups do.
