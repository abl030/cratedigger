---
name: 1277-dispatch-request-shipped
description: "2026-08-26: #1277 dispatch deepening COMPLETE — DispatchRequest + DispatchDB port live; #1278 is the architecture-review debt register"
metadata:
  type: project
---

2026-08-26: Issue #1277 (PR #1279, merge `ebc545e7`) shipped, deployed, live-verified, closed. `dispatch_import_core`: 36 kw-only params/1004 lines → frozen `DispatchRequest` (27 fields) + `DispatchDB` Protocol + kwarg-DI seams = 10 params/373 lines. The `db=` ignore cluster at dispatch is ZERO; `dispatch_import_with_fake_db` bridge deleted (use `make_dispatch_request` from tests/helpers.py).

Key facts for later work:
- `DispatchDB`'s SHAPE (signatures) is guarded **only by Pyright** (conformance pins in `lib/dispatch/__init__.py`); the runtime `isinstance` test checks member names only.
- The evidence-sidecar recording must stay BEFORE the two launch-authority refusals in `dispatch_import_core` (leak otherwise; pinned by `TestEvidenceActionSidecarIsRemovedOnLaunchRefusal`); its placement before the evidence-stage early return is inert fail-closed legislation.
- Remaining `db=` debt: `measure_preimport_state` (14 sites) — next cluster, recorded on [[1278-architecture-review-register]] (#1278, which also holds the other 9 review candidates + post-ship reflection).
- The review artifact: https://claude.ai/code/artifact/9cbd68a6-82b5-4f54-8966-b429aae62a0f
