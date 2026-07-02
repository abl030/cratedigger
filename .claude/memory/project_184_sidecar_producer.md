---
name: project-184-sidecar-producer
description: Issue
metadata: 
  node_type: memory
  type: project
  originSessionId: 34095288-8b49-4e7f-b03c-345219829579
---

Issue #184 **producer + backfill shipped 2026-06-18** (PRs #459 + #460). Every
verified-lossless import writes `cratedigger.json` into the beets album folder,
built from the content-addressed `album_quality_evidence` (`lib/sidecar.py`,
`lib/sidecar_service.py`, hook in `dispatch_import_core`). It is **derived
state** — regenerable anytime via the same `write_sidecar_for_request` the
backfill uses; beets clobbering it is not data loss. File is `0644`
world-readable (must be — slskd reshare / other instances read it) and
non-hidden so peers browsing shares can see it.

**Trust model: rank-first / verify-always, unsigned.** The sidecar is a
cooperation hint, never a substitute for the consumer's own verification — so
faking is a non-problem and signing is out of scope.

**Backfill result (6283 verified-lossless imported requests):** 3910 written,
2143 `skipped_no_evidence`, 119 `skipped_evidence_stale`, 111
`skipped_not_verified_lossless`. The 2143 no-evidence cohort is the
pre-2026-05-17 evidence-canonical wart (library rows imported before evidence
rows existed) — they get sidecars when re-imported; we don't fabricate
evidence. The service self-validates the current evidence's `snapshot_fingerprint`
against the on-disk files and skips stale rather than publishing a payload that
doesn't match the bytes.

**DEFERRED: the consumer half** — ranking a peer's verified-lossless source
first during search so the grind is skipped. Only useful once this format
propagates; not built yet.

**Reusable: agent-driven one-shot in the cratedigger env on doc2** (the backfill
pattern, per [[feedback-single-operator-no-backfill-scripts]]): derive
`PYTHONPATH` + the `python3-*-env` interpreter from `cat $(which pipeline-cli)`,
then `ssh doc2 'sudo bash -s'` with `export PGPASSWORD=$(cat
/run/secrets/cratedigger-pgpass | grep ^PGPASSWORD= | cut -d= -f2)`,
`cd /var/lib/cratedigger`, and a nested `python - <<'PYEOF'` heredoc using
`read_runtime_config("/var/lib/cratedigger/config.ini")` + `PipelineDB(dsn)` +
`BeetsDB(library_root=cfg.beets_directory)`. Importer runs as `User=root`, so
sudo matches its file ownership. The store path changes each deploy — re-derive
it, never hardcode.
