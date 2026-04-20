---
name: slskd concurrency limits
description: slskd has hard limits on concurrent searches — SemaphoreSlim(1,1) on API, maximumConcurrentSearches=2 in Soulseek.NET
type: project
---

slskd has two hard concurrency limits discovered from source code analysis (2026-03-31):

1. **API level**: `SemaphoreSlim(1,1)` on `POST /api/v0/searches` — only one search can be submitted at a time. Returns 429 (rate limit) or 409 (conflict) for concurrent POSTs.
2. **Soulseek.NET level**: `maximumConcurrentSearches: 2` — only 2 searches active on the Soulseek network simultaneously.

**Why:** Submitting >2 searches causes them to queue internally in slskd. Combined with the "Queued" state bug (our code checked `!= "InProgress"` which passed for Queued), searches appeared to complete instantly with 0 results.

**How to apply:** Soularr batches searches in pairs (BATCH_SIZE=2). Submit 2, wait for both to complete, process results, then submit next 2. The `_submit_search()` function retries on both 429 and 409 with exponential backoff. Search state polling must check for "Completed" in the state string, not just "not InProgress".
