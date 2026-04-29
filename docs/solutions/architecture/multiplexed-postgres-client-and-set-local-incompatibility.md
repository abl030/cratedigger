---
title: "Multiplexed Postgres clients and SET LOCAL do not mix"
date: 2026-04-29
category: architecture
problem_type: production-hardening
component: discogs-api
tags:
  - postgres
  - tokio-postgres
  - deadpool-postgres
  - statement-timeout
  - connection-pool
related_plans:
  - docs/plans/2026-04-29-001-feat-label-viewer-phase-a-plan.md
  - docs/plans/2026-04-29-002-fix-discogs-api-connection-pool-plan.md
---

# Multiplexed Postgres clients and SET LOCAL do not mix

## Context

The Discogs label viewer added a recursive sub-label release query. On
UMG-class label trees, that query can run long enough to block cratedigger's
single-threaded web server while it waits on the Discogs mirror.

The first attempted hardening wrapped the recursive CTE in
`BEGIN; SET LOCAL statement_timeout = '5s'; ... COMMIT;` on the existing
shared `tokio_postgres::Client`. That was reverted because the client was
multiplexed over one TCP connection.

## Guidance

Do not put transaction-scoped or session-scoped state on a shared multiplexed
Postgres client that serves unrelated HTTP requests. If a query needs
`SET LOCAL`, advisory state, temp tables, or any transaction-local behavior,
first acquire a dedicated connection from a pool and run the transaction on
that connection.

For `discogs-api`, the safe shape is:

```rust
let mut client = get_client(pool).await?;
let tx = client.transaction().await?;
tx.batch_execute("SET LOCAL statement_timeout = '15s'").await?;
let rows = tx.query(sql, params).await?;
tx.commit().await?;
```

The importer is different: it is a single-purpose oneshot process doing COPY
work, so a dedicated `tokio_postgres::Client` is still appropriate there.

## Why This Matters

`tokio_postgres::Client` supports pipelining over a single connection. That is
good for simple independent queries, but explicit transactions serialize
everything behind the same session. If a statement inside the transaction times
out, PostgreSQL can leave the transaction aborted until rollback. On a shared
client, that aborted state can bleed into later requests and make healthy
routes fail.

A pool scopes the blast radius. One slow recursive CTE can time out, roll back,
and return a typed 503 while other requests acquire separate connections and
continue normally.

## When To Apply

Apply this rule when a service uses one long-lived async Postgres client across
handlers and a new feature needs any of:

- `SET LOCAL statement_timeout`
- explicit `BEGIN` / `COMMIT`
- temporary session settings
- transaction-local advisory locks
- logic that must survive a canceled statement without poisoning other traffic

## Example

In Plan 002, `discogs-api` moved Axum query handlers from a shared
`tokio_postgres::Client` to a `deadpool-postgres` pool. Only the recursive
label releases branch gets a transaction-scoped `statement_timeout`, currently
15 seconds after live measurement showed UMG-class rollups take about 12
seconds on doc2. When PostgreSQL raises SQLSTATE `57014` (`query_canceled`),
the handler returns HTTP 503 and cratedigger retries once without sub-label
rollup.

The pool itself also has bounded wait/create/recycle timeouts. A saturated pool
returns a typed 503 instead of letting request tasks await a connection forever.
