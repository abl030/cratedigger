---
last-verified: 2026-05-01
slskd-source: github.com/slskd/slskd
---

# slskd Concurrency Internals

Durable reference for how slskd handles concurrent requests, derived from reading the C# server source. Updates here should cite specific files at slskd's commit you read against and the date of the read.

## Browse calls (`users.directory`, `users.browse`) — UNTHROTTLED

`GET /api/v0/users/{username}/browse` and `POST /api/v0/users/{username}/directory` (UsersController.cs:121-149, 181-214) delegate **directly** to `ISoulseekClient.BrowseAsync()` / `GetDirectoryContentsAsync()`. There is **no** slskd-layer throttle:

- No `SemaphoreSlim` gating the HTTP entry
- No per-peer mutex
- No queue
- `BrowseTracker` only emits progress events, doesn't gate

**Implication for clients:** Fan out 20–50 concurrent browse calls without serialization at the slskd layer. Real ceiling is Soulseek.NET's per-peer connection pool, which slskd does not expose.

## Search creation (`POST /api/v0/searches`) — SERIALIZED at the HTTP entry

`SearchRequestLimiter` (SearchesController.cs:67) is `SemaphoreSlim(1, 1)`. `SearchRequestLimiter.Wait(0)` (SearchesController.cs:94-96) returns **HTTP 429 immediately** if a second POST overlaps the first. The limiter releases as soon as the search is registered (~100 ms), not when results arrive.

**Implication:** Submit searches sequentially. Two simultaneous POSTs lose; back off and retry the loser.

## Active searches — QUEUED inside Soulseek.NET

`maximumConcurrentSearches: 2` (Program.cs:712) is enforced by Soulseek.NET, not by slskd. Once the POST returns successfully, slskd has accepted the search; if more than 2 are active, Soulseek.NET queues internally and processes them as slots free.

**Implication:** Pipeline depth above 2 is fine — submitting search 3 doesn't fail, it just waits in Soulseek.NET. Excess pipelining buys more *result-collection* parallelism, not more *active-search* parallelism.

## Other concurrency primitives (do not gate browse/search)

| Primitive | Location | Scope | Default |
|---|---|---|---|
| `SearchRequestLimiter` | SearchesController.cs:67 | Search **submission** only | `SemaphoreSlim(1,1)` |
| `GlobalEnqueueSemaphore` | Application.cs:253 | Download enqueue only | `SemaphoreSlim(10,10)` |
| `IncomingSearchRequestSemaphore` | Application.cs:153-155 | **Incoming peer** searches (when we are searched) | `Throttling.Search.Incoming.Concurrency`, default 10 |

There is **no API-level rate limiter on incoming HTTP** as a whole.

## Per-peer behavior

- No server-side "broken peer" cooldown. A dead peer hangs every browse to it independently.
- Browse timeout is **Soulseek.NET's TCP-level timeout, ~30–60 s**. Not configurable per-call. Configurable globally on the `SoulseekClient` via `SoulseekClientOptions`, but slskd doesn't expose this.
- Concurrent browses to the **same** peer may queue inside Soulseek.NET (per-peer connection state). Different peers parallelize.

**Implication for tail latency:** A fan-out wave's wall-clock is dominated by the slowest dead peer. Add a client-side deadline (e.g. 20 s) and treat non-responders as broken-for-this-cycle rather than waiting out the TCP timeout.

## Common client design pitfalls (from cratedigger experience)

1. **Don't size browse parallelism around the search-creation limit.** Browses don't share that semaphore. The relevant ceiling is Soulseek.NET's connection pool, which is much wider.
2. **Don't fire concurrent search POSTs.** They race the `SearchRequestLimiter`; one returns 429. Pattern: submit sequentially with retry, then poll/collect in parallel — see `docs/parallel-search.md`.
3. **Bound fan-out waves with a deadline.** Without one, a single dead peer blows out the whole wave by 60 s. Collect what came back, mark non-responders, move on.
4. **Cache hit ≠ no work.** A 538 MB JSON cache file (or equivalent) reads + parses on every cycle startup. Persistence isn't free; it's traded against re-browsing.

## How to refresh this doc

```bash
# Clone shallow
git clone --depth 50 https://github.com/slskd/slskd.git ~/code/slskd
# Re-read the cited files. Search for SemaphoreSlim, Channel, ConcurrentQueue,
# Mutex, RateLimit, Throttling. Update the file:line citations and the
# last-verified frontmatter.
```

When in doubt, dispatch an Explore subagent against `~/code/slskd/src` with the questions in this doc.
