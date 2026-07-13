---
name: project_681_discogs_artist_wire_contract
description: "Issue #681 durable handoff for the strict Discogs artist masters and appearances contract"
metadata:
  node_type: memory
  type: project
---

Issue #681 shipped the executable cross-repository contract used by Discogs artist comparison. The canonical semantics live in `docs/discogs-mirror.md` under **Artist catalogue wire contract**; read that section before changing `web/discogs.py`, the Discogs artist SQL, or comparison fixtures.

The producer pins are in `~/discogs-api/tests/artist_masters_sql.rs` and run against a real ephemeral PostgreSQL instance. They cover numeric master versus `release-<id>` namespaces, `master_id IS NULL` plus the legacy `master_id=0` sentinel, master and masterless appearances, structural types aggregated across child pressings, qualifier-only and empty-array paths, deterministic ordering, and nullable master credits.

The consumer pins are in `tests/test_discogs_api.py`. Cratedigger strictly decodes every required wire field with `msgspec`, rejects missing or wrongly typed fields, strips the `release-` prefix only for rows marked masterless, and normalizes `primary_artist_id: null` to the public empty-string value. Both repositories share the `Mixed appearance master` nullable fixture; change it only in a coordinated producer/consumer update.

For live UI evidence, `scripts/web_dev_server.py --data live-db` must receive explicit `--mb-api` and `--discogs-api` bases. Require the exact `/api/artist/compare` request used for the screenshot to return HTTP 200 before accepting pixels; a plausible MB-only page is not evidence that the Discogs route ran. The executable recipe is in `docs/web-dev-server.md` and `docs/solutions/ui-dev-server-screenshot-loop.md`.

Shipped as discogs-api PR #11 (`707aa82d`) and Cratedigger PR #686 (`ce7d01c7`), deployed through nixosconfig anchor `8891e04f`, and released as `v2026.07.13-12`.
