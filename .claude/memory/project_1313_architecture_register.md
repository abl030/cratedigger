---
name: 1313-architecture-register
description: "#1313 is the open 2026-09-01 architecture-review register (successor to #1278); 6 strong candidates, top pick = JobLane collapse"
metadata:
  type: project
---

Issue #1313 (opened 2026-09-01) is the active architecture-review covering issue, successor to the closed [[1278-register-closed]] register. It came from a two-agent depth-and-seams scan of main @ 0476b77f (pipeline core; web + test infra), run via mattpocock's improve-codebase-architecture skill at the operator's request; the operator diverted from the skill's grilling loop and asked for a #1278-style register instead.

Strong candidates, ranked: (1) JobLane collapse of the two import-job queues — borders #1312 but is a separate mirrored family, scope comment posted on #1312; (2) current-library (HAVE) evidence module; (3) CratediggerContext collaborators/scratch split (deletes the 888-line AST audit); (4) FakePipelineDB package split mirroring lib/pipeline_db/; (5) typed WebRuntime; (6) shared JS test harness.

**Why:** all counts in the register were measured by scan agents against 0476b77f and must be re-verified before building (the register says so itself, per #1312's precedent).

**How to apply:** when picking up refactor work in this repo, read #1313 first and de-dupe against it; the visual report artifact is https://claude.ai/code/artifact/4252c7fd-a358-4afb-a6f5-93a9724493bf.
