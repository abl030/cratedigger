---
name: feedback_finish_the_job
description: Always wire up new functionality end-to-end — don't just build infrastructure and leave it disconnected
type: feedback
---

Don't build infrastructure and leave it unwired. If the user asks for "catch-all when no beets files", that means: build it AND make it trigger automatically. Getting 99% there and not turning it on is worse than not building it — it's dead code that looks like it works.

**Why:** Repeated pattern of building dataclasses/functions/modes but not connecting them to the actual pipeline flow. The user has called this out multiple times.

**How to apply:** Before marking any feature complete, trace the full path from trigger to effect. Ask: "Does this actually run in production without manual config?" If not, it's not done.
