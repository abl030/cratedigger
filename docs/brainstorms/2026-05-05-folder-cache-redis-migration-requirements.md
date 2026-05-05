---
date: 2026-05-05
topic: folder-cache-redis-migration
issue: 201
---

# Folder Cache → Redis Migration

## Summary

Replace the on-disk JSON `folder_cache` (now ~4 GB resident in Python dicts at runtime) with a Redis-backed peer cache. Migrate all three persisted caches (`folder_cache`, `user_upload_speed`, `search_dir_audio_count`) to a new `lib/peer_cache.py` module, add a negative-cache namespace so wasted browses against dead `(user, dir)` pairs are not re-issued every cycle, and delete `lib/cache.py`. The negative cache is the primary motivation; the JSON load-tax fix is a free side-effect.

---

## Problem Frame

`lib/cache.py` persists three runtime caches to a single JSON file at `/var/lib/cratedigger/cratedigger_cache.json`. Today on doc2 that file is 538 MB on disk and the resident Python dicts after load are ~4 GB. Two structural costs:

1. **Load-everything-at-startup tax.** Every cycle reads and JSON-parses the full file before any work begins. Grows monotonically with unique-peer count.
2. **Unbounded growth, positives only.** The 24h per-entry TTL only evicts entries that haven't been re-fetched in 24h, so popular peers stay forever. The format also can't realistically store *negatives* — a JSON file of every `(user, dir)` pair we've ever failed to browse would balloon load time further, and the on-disk format has no eviction policy beyond the flat TTL.

A separate observation from #198 wave data over 10h / 3,984 cycles shows p10 wave return rate of 0.38 — 10% of waves get back less than 38% of what they ask for. The current cache has no way to remember which `(user, dir)` pairs already failed, so cratedigger re-spams the same dead peers every 5 minutes for the same wanted album. That redundancy is a meaningful slice of the 1300 s p95 browse tail.

Redis is **already deployed** as `services.redis.servers.cratedigger` (127.0.0.1:6379 on doc2) and used by `web/cache.py` for the web UI metadata cache. Adding the pipeline-side peer cache to the same instance is zero infra cost.

---

## Requirements

**Storage migration**
- R1. All three persisted caches (`folder_cache`, `user_upload_speed`, `search_dir_audio_count`) move to Redis. The on-disk JSON file at `/var/lib/cratedigger/cratedigger_cache.json` is no longer written or read by cratedigger.
- R2. `lib/cache.py` is deleted, including `save_caches`, `load_caches`, the per-entry `_ts` wrapping, and the `cache_load_s` instrumentation field on `CratediggerContext`.
- R3. A new module `lib/peer_cache.py` houses the pipeline-side Redis client. It is structurally separate from `web/cache.py` — different concern (pipeline-cycle peer state vs web-UI response cache), no shared mutable state. The fail-safe init/get/set patterns from `web/cache.py` may be reused but not imported.
- R4. The Redis namespaces are: `peer_dir:{user}:{dir}` (positive), `peer_dir_neg:{user}:{dir}` (negative), `peer_speed:{user}` (upload speed), `peer_dir_count:{user}:{dir}` (audio count).
- R5. Values that contain structured payloads (directory listings) are stored as msgpack-encoded, zstd-compressed bytes. Scalar values (speed, audio count) are stored as plain integers. The compression boundary is in `lib/peer_cache.py`; callers see typed Python values.

**Cache semantics**
- R6. **Positive TTL: 7 days, flat.** Both `peer_dir` and `peer_dir_count` use server-side `EX` for expiry. No per-entry timestamp bookkeeping in application code.
- R7. **Negative TTL: 7 days, flat.** Same horizon as positives. No backoff, no per-`(user, dir)` failure counting.
- R8. **Speed TTL: 24h.** Upload speeds change with peer hardware/network state at much faster cadence than directory contents.
- R9. **Negative-cache short-circuit.** When the wave/match path is about to submit a browse for `(user, dir)` and `peer_dir_neg:{user}:{dir}` is set, the browse is skipped entirely — no slskd request issued, no timeout waited.
- R10. **Negative writes on browse failure.** When a browse for `(user, dir)` returns no result, errors, or times out, `peer_dir_neg:{user}:{dir}` is set with the negative TTL. Positive results clear any stale negative for the same key (write-positive deletes the negative).
- R11. **User-level cooldowns retain authority.** The existing 3-day user cooldown after 5 consecutive failures (`docs/cooldowns.md`) is unchanged. The negative cache only short-circuits per-`(user, dir)` browses; the cooldown check still gates whether the user is contacted at all.

**Failure mode**
- R12. **Redis down → cache miss, cycle continues.** Same fail-safe as `web/cache.py`: a connection error, timeout, or any Redis exception returns `None` from a get and a no-op from a set. The wave path treats this identically to a cold cache.
- R13. **Tight client timeouts.** 200 ms connect timeout, 100 ms operation timeout. Localhost Redis with point lookups answers sub-millisecond; if it can't, treat as down for this cycle. The pipeline's cycle budget is more sensitive to per-call latency than the web UI's.
- R14. **No retry on Redis errors.** A failed get/set is logged at debug level (not warning — too noisy at thousands of lookups per cycle) and the cache layer falls through. Redis-down is not an error condition for the pipeline.

**Telemetry**
- R15. The cycle-summary log line includes `cache_pos_hits=N cache_neg_hits=N cache_misses=N` counters covering all four namespaces in aggregate. The legacy `cache_load_s` field is removed.
- R16. On startup the pipeline logs `Redis connected: <host>:<port>` (or `Redis unavailable: <reason>, running without cache`) once, matching `web/cache.py`'s pattern.

**Configuration**
- R17. `nix/module.nix` exposes `--redis-host` and `--redis-port` to the pipeline the same way they are exposed to `cratedigger-web` today.
- R18. `nix/module.nix` exposes module options for `peer_cache_pos_ttl_seconds` (default 604800 = 7d), `peer_cache_neg_ttl_seconds` (default 604800), and `peer_cache_speed_ttl_seconds` (default 86400). These plumb into the rendered `config.ini` and through `CratediggerConfig`.
- R19. `nix/module.nix` configures the existing Redis server with `maxmemory 1gb` and `maxmemory-policy allkeys-lru`. Both are module options; only `maxmemory` is intended to be tuned per-deploy.
- R20. The `maxmemory-policy` is hardcoded to `allkeys-lru`. There is no use case in this repo for a different eviction policy.

**First deploy**
- R21. **Cold start, no JSON-to-Redis bootstrap.** On the first cycle after deploy, Redis is empty; every browse is a cache miss. The cycle is expected to be slower than steady-state. No throwaway migration script reads the existing `cratedigger_cache.json`.
- R22. The existing `cratedigger_cache.json` file is not deleted by the deploy. It is left in place as a one-cycle safety net (operator can restore it manually if needed). A follow-up commit may delete it once the new cache has been observed in steady-state for a week.

---

## Out of Scope (explicit, in-scope follow-ups acceptable)

- **slskd search responses cache.** Search is cheap relative to browse, and the staleness model gets messy with pipeline state transitions. Not added.
- **Backoff on negative cache.** Our access cadence is "1–2 hits per `(user, dir)` per day" which is too slow for backoff to engage meaningfully; flat TTL captures the same behavior. The code shape leaves backoff easy to add later if telemetry justifies it.
- **Querying cache state from the web UI.** No "show me what's cached for user X" endpoint. Add only if a real diagnostic need surfaces.

## In Scope as Additional Namespaces (lower priority, same migration)

These reuse the same Redis instance and fail-safe pattern; same module:

- **slskd user info / peer metadata** — `peer_info:{user}` for `users.info` calls (status, country, shared file count). TTL ~1h.
- **Beets album fingerprint lookups** — `beets_release:{mb_release_id}` for "is this MB release ID already in the library." TTL 5 min. Dedupes the many calls made during matching within and across cycles.
- **MB mirror lookups inside the pipeline** — anywhere `cratedigger.py` / `lib/*` hits the MB mirror directly, route through the existing `web/cache.py` `meta:` namespace (24h TTL). Zero new code, free win.

---

## Acceptance Examples

- AE1. **Covers R1, R2, R21.** After deploy, `/var/lib/cratedigger/cratedigger_cache.json` is no longer modified by cratedigger (mtime stops advancing). The first post-deploy cycle's log shows no `cache_load_s` entry. Subsequent cycles' logs show `cache_pos_hits` rising as the cache warms.

- AE2. **Covers R9, R10, R15.** Given a `(user, dir)` pair where slskd previously returned a timeout, when the next wave submits work that would target that pair, then no slskd browse is issued for it, the cycle-summary line records `cache_neg_hits=N` reflecting the skip, and `peer_dir_neg:{user}:{dir}` is observable via `redis-cli` with a TTL between 0 and 7 days.

- AE3. **Covers R12, R13.** Given Redis is stopped mid-cycle (`systemctl stop redis-cratedigger`), the cycle completes without raising, all subsequent lookups return cache miss within 200 ms each, and the cycle-summary line shows `cache_misses` rising while `cache_pos_hits` and `cache_neg_hits` stop incrementing. The next cycle after Redis is restarted resumes hitting the cache normally.

- AE4. **Covers R5.** A `peer_dir:{user}:{dir}` entry inspected via `redis-cli GET` returns binary (zstd-compressed msgpack); decoding through `lib.peer_cache.get_peer_dir(user, dir)` returns the same Python data structure that the legacy code path returned for the same `(user, dir)` pair.

- AE5. **Covers R17–R19.** A `nixos-rebuild switch` with `services.cratedigger.peer_cache_pos_ttl_seconds = 86400` results in `/var/lib/cratedigger/config.ini` containing `peer_cache_pos_ttl_seconds = 86400`, and the running cratedigger process applies that TTL to new positive writes (verifiable via `redis-cli TTL peer_dir:{user}:{dir}`).

- AE6. **Covers R6 (negative case).** A `(user, dir)` for which we wrote a negative on day 0 still returns a cache hit (skip-browse) on day 6, and on day 8 (after the 7-day TTL has expired) the next match path issues a fresh slskd browse for that pair.

- AE7. **Covers R11.** A user under cooldown is never contacted regardless of the negative cache state for their `(user, dir)` pairs. The cooldown check happens before the cache lookup in the wave path.

---

## Open Questions Deferred to Planning

- Exact module API for `lib/peer_cache.py` (function shapes, exception types). Will follow the typed-protocol patterns in `lib/quality.py` and `web/cache.py`.
- Test infrastructure: `fakeredis` is the obvious fit for unit tests in `tests/test_peer_cache.py`. Orchestration tests in `tests/test_integration_slices.py` will mock at the `lib.peer_cache` boundary the same way `lib.beets` is mocked today.
- Whether the new `lib/peer_cache.py` exports a module-level connection (matches `web/cache.py`) or attaches to `CratediggerContext`. Module-level is simpler and matches the existing pattern; context-attached is more testable. Decide during planning.
- Concrete `maxmemory` default. 1 GB is conservative given the 4 GB Python-dict footprint and 256 MB realistic compressed footprint; 2 GB is luxurious. Operator-tunable so the default is not load-bearing.

---

## Relationship to #198

Orthogonal. #198's parallel browse fan-out and raised pipeline depth ship first; this Redis migration ships independently after. No ordering dependency, no shared code surface, no migration choreography.
