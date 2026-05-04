---
title: "RuntimeMaxSec is silently ignored on Type=oneshot systemd services"
date: 2026-05-04
category: deployment
problem_type: production-hardening
component: nix-module
tags:
  - systemd
  - nix
  - oneshot
  - timeout
  - defense-in-depth
related_plans:
  - docs/plans/2026-05-04-001-fix-search-watchdog-and-cycle-gate-removal-plan.md
related_prs:
  - "#213"
  - "#214"
---

# RuntimeMaxSec is silently ignored on Type=oneshot systemd services

## Context

Issue #212 / PR #213 added a process-level safety net for `cratedigger.service`:
if anything escapes the in-band 90s per-search progress watchdog (clock-injection
bug, TCP-hung `state()` blocking the watchdog's own deadline check, etc.),
systemd should SIGTERM the process at 60 minutes so the timer can schedule the
next cycle. The plan called for `RuntimeMaxSec=1h` on the service.

The plan was code-reviewed (single-agent code review, multi-agent doc-review
during planning), the moduleVm Nix check passed, the full Python test suite
passed, and the deploy completed cleanly. On first cycle after deploy:

```
cratedigger.service: RuntimeMaxSec= has no effect in combination with Type=oneshot. Ignoring.
```

The defense-in-depth was non-functional from the moment it shipped.

## Root cause

Per `systemd.service(5)`:

> `RuntimeMaxSec=` … This option does not apply to `Type=oneshot`.

For a `Type=oneshot` unit, the service is "active" exactly while `ExecStart` is
running — there's no notion of a long-lived runtime separate from start-up.
`RuntimeMaxSec` measures elapsed runtime *after* the unit reaches `active`,
which never happens for a oneshot in a way the option can act on. systemd
prints the warning at unit-load time and silently drops the directive.

The right knob for a oneshot is `TimeoutStartSec=` — for oneshot the entire
service IS the start phase, so a `TimeoutStartSec` cap bounds the total
ExecStart wall-clock and SIGTERMs (then SIGKILLs after `TimeoutStopSec`) when
exceeded.

## Guidance

When picking a "runaway-process safety net" directive for a NixOS service,
**check the service's `Type=` first.** The mapping is not symmetric:

| Service `Type=`                    | Use                                               |
|------------------------------------|---------------------------------------------------|
| `oneshot`                          | `TimeoutStartSec=<cap>` — bounds the ExecStart phase |
| `simple` / `notify` / `forking`    | `RuntimeMaxSec=<cap>` — bounds the running phase     |
| `exec`                             | `RuntimeMaxSec=<cap>` — same as simple               |

A single rule of thumb: **`RuntimeMaxSec` is for long-running services;
`TimeoutStartSec` is for things that "do a job and exit."** If the unit's
`Type=` is `oneshot`, reach for `TimeoutStartSec` automatically.

Mixed-type modules should pick the directive per service, not module-wide.
`cratedigger-importer.service` (`Type=simple`) would correctly use
`RuntimeMaxSec`; `cratedigger.service` (`Type=oneshot`) needs
`TimeoutStartSec`.

## Why review and CI didn't catch it

- **`nix build .#checks.x86_64-linux.moduleVm`** evaluates the NixOS module to
  produce a unit file. It does not run `systemd-analyze verify` on the rendered
  unit, and the module evaluator has no semantic knowledge that
  `RuntimeMaxSec` + `Type=oneshot` is a no-op. The check passed because the
  Nix expression was syntactically valid.
- **Single-agent code review** treated the `RuntimeMaxSec` line as a literal
  copy of the plan's R13 spec. The reviewer's correctness lens looked at the
  Python code (where the bugs would be) and accepted the Nix line as
  configuration-only. The reviewer did not cross-check the directive against
  the service's `Type=`.
- **Manual reading** of the module diff suffered from the same issue — the
  plan said `RuntimeMaxSec`, the diff added `RuntimeMaxSec`, both reviewers
  matched on the literal string and missed the type-mismatch.
- **The journalctl warning is a runtime signal**, not a build-time signal.
  systemd emits it on every unit-load, but you only see it if you tail the
  service log after deploy. The `Done` from `nixos-rebuild switch` does not
  surface it.

## Detection at review-time (next time)

Two cheap pre-deploy checks would have caught it:

1. **Render the unit and run `systemd-analyze verify` on it.** This catches
   `RuntimeMaxSec=` + `Type=oneshot` and reports it as a warning.

   ```bash
   # Inside a NixOS VM check or CI:
   systemd-analyze verify /etc/systemd/system/cratedigger.service
   ```

2. **Pre-flight grep on PR diffs touching `nix/module.nix`.** Any addition of
   `RuntimeMaxSec` adjacent to `Type = "oneshot"` is a code smell worth
   flagging by hand. A linter rule could express this exactly:

   ```
   In any serviceConfig block where Type == "oneshot", reject RuntimeMaxSec.
   Suggest TimeoutStartSec instead.
   ```

For now: **manually verify any `RuntimeMaxSec` addition** by scrolling up to
the `Type=` line and confirming the type allows it.

## Related: timer-driven oneshots

`cratedigger.service` is invoked by `cratedigger.timer` every 5 minutes. The
combination `Type=oneshot` + `cratedigger.timer.OnUnitActiveSec=5min` makes
`TimeoutStartSec` the only sensible place to cap runtime. If a cycle is
SIGTERMed at the cap, the timer fires the next cycle on schedule —
no orphaned state, because cycle-boundary checkpointing already tolerates a
forced kill (the importer service owns beets writes independently).

## Resolution

PR #214 (rebase-merged 2026-05-04) replaces:

```nix
RuntimeMaxSec = "1h";
```

with:

```nix
TimeoutStartSec = "1h";
```

After redeploy, the warning was gone and the directive is active.
