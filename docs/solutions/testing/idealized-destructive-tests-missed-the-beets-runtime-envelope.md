---
title: "Idealized destructive tests missed the real Beets runtime envelope"
date: 2026-07-20
category: testing
problem_type: test-pyramid-gap
component: beets
tags:
  - testing
  - generated-testing
  - beets
  - plugins
  - subprocess-protocol
  - destructive-operations
related_issues:
  - 777
---

# Idealized destructive tests missed the real Beets runtime envelope

## Incident

Ban Source committed the durable bad-rip evidence and reset request 8878, but
left its exact Beets album in the library while reporting success. A related
delete-child path could complete the mutation and then be reported as failed.

The first theory blamed Beets' per-track confirmation behavior. A real pinned
Beets probe disproved it: one confirmation can remove a multi-track album. The
shared cause was the operator runtime configuration. `pipeline-cli` ran as the
operator user, while the module-rendered `secrets.yaml` was service-readable
only. Beets treated the unreadable include as non-fatal. The Discogs plugin then
entered interactive OAuth during plugin loading, wrote its prompt to stdout,
and consumed the fixed confirmation byte or the child protocol's stdin.

That produced two superficially different failures:

- selector cleanup lost its only confirmation byte before `beet remove` read
  it, so the exact album remained;
- exact child deletion could commit, but plugin text prefixed the JSON frame,
  so the parent rejected the acknowledgement.

## Why the tests all passed

The tests generated many policy states but replaced the relevant production
boundaries with idealized functions:

- selector tests mocked `subprocess.run` and asserted the hand-written input;
- exact-delete tests used `plugins: []`, never the module's plugin set or
  included secret file;
- lifecycle generators injected a synthetic cleanup result instead of running
  pinned Beets under real configuration profiles;
- protocol tests accepted any stdout that a JSON decoder could parse rather
  than enforcing the single-frame byte contract;
- adapters treated committed bad-rip evidence as total success even when the
  postcondition said the Beets album remained.

The generated space was large, but the seam containing the bug had been mocked
away. More examples cannot recover behavior excluded from the model.

## Permanent testing rule

Generated tests for destructive third-party integrations must cross the real
runtime envelope, not only an idealized decision function. The permanent Beets
matrix executes the pinned CLI and exact-delete child with:

- minimal and complete production plugin configurations;
- placeholder, readable, unreadable, and invalid-UTF-8 included secrets;
- independently combined `importsource.suggest_removal`, automatic playlist,
  missing-plugin, and include-level plugin-override worlds;
- MusicBrainz and Discogs identities;
- single-track, two-track, and twelve-track albums;
- a sibling pressing that must survive every world.

Each valid world must remove exactly the target metadata and files. Invalid
configuration must fail before mutation. Child stdout must be exactly one
canonical typed result frame. Lifecycle completion must be equivalent to the
verified postcondition: the exact Beets authority is absent.

The matrix drives the full real Ban Source cleanup chain as well as the exact
child. The module VM independently removes two 12-track albums through the
actual rendered configuration as a non-root operator: one through the pinned
CLI and one through the exact-delete child, whose stdout must be one canonical
typed frame.

The implementation uses Beets' noninteractive `--force` contract, closes
stdin, disables only `importsource` for selector removal because its source-file
deletion is outside Cratedigger's authority, validates declared configuration
includes and confirms every configured plugin actually loaded before mutation,
quarantines all child diagnostics to stderr, and reports retained Beets
authority as typed partial failure.

## Qualification

For this class of fix, fault injection must separately prove that the suite
kills removal of each defense: noninteractive force, configuration preflight,
plugin isolation, stdout quarantine, canonical-frame validation, postcondition
truthfulness, and operator access to the module-rendered secret. A generic
"known bad" object is insufficient when several independent boundaries
participated in the incident.
