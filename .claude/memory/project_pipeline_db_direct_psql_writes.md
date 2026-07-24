---
name: project-pipeline-db-direct-psql-writes
description: "Pipeline DB raw SQL defaults read-only; explicit pipeline-cli writes use --write --confirm WRITE on doc2"
metadata: 
  node_type: memory
  type: reference
  originSessionId: c0bc0b00-2d3a-4737-b971-de45f59eedb8
---

For a deliberately exceptional raw **write** against the cratedigger pipeline
DB (e.g. a one-shot data fix), use the writable operator control plane on doc2:

```bash
pipeline-cli query --write --confirm WRITE - <<'SQL'
UPDATE source_denylist SET username = username WHERE request_id = 5219;
SQL
```

The default `pipeline-cli query` path is one explicit read-only transaction;
the two write flags are an intent/safety boundary, not authorization. Typed
subcommands remain the routine mutation path. `psql` remains available for
database administration with the **real** DSN:

```
postgresql://cratedigger@10.20.0.11:5432/cratedigger
```

The nspawn DB host is **10.20.0.11**, NOT the `192.168.100.11` printed in
CLAUDE.md (that address is stale — same class of staleness as the discogs
mirror DB, see [[project-discogs-api-deploy-and-db-access]]). The real DSN is
in `/var/lib/cratedigger/config.ini` (`dsn = …`) and the
`PIPELINE_DB_DSN` env on `cratedigger.service`. Password is env-format in
`/run/secrets/cratedigger-pgpass` (`PGPASSWORD=…`).

Working `psql` incantation (pipe SQL via stdin heredoc — psql reads stdin with
no `-c`; avoids nested-quote expansion over ssh):

```bash
ssh doc2 'export PGPASSWORD=$(sudo grep "^PGPASSWORD=" /run/secrets/cratedigger-pgpass | cut -d= -f2); psql "postgresql://cratedigger@10.20.0.11:5432/cratedigger" -tA -F " | "' <<'SQL'
DELETE FROM source_denylist WHERE request_id = 5219 AND username = 'denleschae' RETURNING id, username;
SQL
```

`psql` IS on doc2's PATH (so is `pg_dump`). Always SELECT-confirm before a
DELETE, and use `RETURNING` so the mutation prints what it touched.
