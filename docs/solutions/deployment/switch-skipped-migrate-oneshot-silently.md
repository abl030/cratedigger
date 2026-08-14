---
title: "A nixos-rebuild switch listed the migrate unit in both phases and ran neither"
date: 2026-08-15
category: deployment
problem_type: silent-skip
component: nixos-module
tags:
  - systemd
  - switch-to-configuration
  - migrations
  - RemainAfterExit
  - job-replacement
  - deploy-verification
related_issues:
  - 1161
---

# A `nixos-rebuild switch` listed the migrate unit in both phases and ran neither

## Symptom

Deploying #1145 on 2026-08-14 (nixosconfig `f8b07ad5`, cratedigger `8a0228dc`),
migration 078 never applied. The pipeline hard-failed every cycle for ~4
minutes until the migrator was run by hand.

Every documented deploy checkpoint passed. `nixos-upgrade.service` reached
`inactive/dead/success`, the fleet anchor advanced to the signed revision, and
the switch log listed the migrate unit in *both* phases:

```
15:26:51 stopping the following units: cratedigger-db-migrate.service, ...
15:26:51 starting the following units: cratedigger-db-migrate.service, ...
```

But `journalctl -u cratedigger-db-migrate.service` had **no entries at all**
between 02:51:10 and the manual restart at 15:31:09 — no `Stopped`, no
`Starting`, nothing. Meanwhile:

```
ActiveState=active   SubState=exited   Result=success
ActiveEnterTimestamp=Fri 2026-08-14 02:51:10 AWST     <- ~12.5 h before the switch
ConditionResult=yes  AssertResult=yes  NeedDaemonReload=no
```

The `ExecStart` wrapper *had* been updated and did contain `078_*.sql`.

## Root cause

Three things composed.

**1. The migrate unit's stop job is ordered behind its dependents' stops.**
`cratedigger-web`, `cratedigger-importer`, `cratedigger-import-preview-worker`
and `cratedigger-youtube-ingest` all `Requires=` + `After=` the migrate unit,
so on a stop transaction the dependents must stop first. On every healthy
deploy in the journal, the migrate unit's `Deactivated successfully` lands 1–2
ms after the *last* dependent's:

| Deploy | last dependent stops | migrate stops |
|---|---|---|
| Aug 13 21:41 | `37.278862` (importer) / `.388788` (preview) | `.390185` |
| Aug 14 02:51 | `05.488590` (importer) | `.490280` |
| Aug 14 19:42 | `59.596450` (importer) | `.597880` |

**2. A concurrent `systemctl start` replaced that queued stop job.** An
unrelated 60-second reconciler timer (`OnUnitInactiveSec=1min`) fired at
15:26:45.960, inside the switch window, and ran `systemctl --no-block start`
over a list of cratedigger units. Four of them `Requires=` the migrate unit,
so that call enqueued a start job for it too. `systemctl start` defaults to
job mode `replace`, and `JOB_START` conflicts with `JOB_STOP` — so the
still-queued stop job was replaced.

**3. `RemainAfterExit` made the replacement start silent.** The unit had never
left `active (exited)`, so `unit_start()` returned `-EALREADY`. The job
completed instantly, `ExecStart` never forked, and systemd logged nothing.
switch-to-configuration's own start phase, ~4 s later, hit the same
`-EALREADY` for a second silent no-op.

The race was lost by about one second: the reconciler's start landed at
15:26:46.77 while the importer — draining gracefully under
`KillMode = "mixed"` after 12.5 h of work — only finished stopping at
15:26:47.79. On the three neighbouring deploys the importer drained in 0.01 s,
3.08 s and 4.48 s, and the reconciler happened not to fire mid-switch.

## Why nothing caught it

- **The runbook checked a proxy.** It asserted `ActiveState=active`,
  `SubState=exited`, `Result=success` — all of which a `RemainAfterExit`
  oneshot satisfies indefinitely after a run from any prior deploy.
- **`Requires=` does not mean "ran".** The documented protection ("a failed
  migration blocks the workers from coming up") only covers a migration that
  FAILED. `Requires=` on a `RemainAfterExit` oneshot is satisfied by the unit
  merely being *active*, so all four workers started cleanly against an
  un-migrated schema.
- The only thing that caught it was `lib/migrator.py::assert_schema_current`
  on the timer-driven side, which exists precisely because `cratedigger.service`
  deliberately does *not* `Requires=` the migrate unit. It turned silent
  corruption (new code writing `lineage_version=5` into a column whose CHECK
  admitted only `(1,3,4)`) into a 4-minute outage.

## Fix

1. **`stopIfChanged = false` on the migrate unit** (`nix/module.nix`). This
   moves it out of switch-to-configuration's stop+start pair and into its
   restart list. systemd's job-merge table collapses `JOB_START` into
   `JOB_RESTART`, so a concurrent start can no longer swallow the re-run. The
   restart phase also runs after the stop phase and before the start phase, so
   the migration completes before the `Requires=` workers come back.
2. **The reconciler must not fire during a switch** (downstream wrapper).
   Narrows the window that lets any mid-switch start fight the switch over
   unit state — including starting workers on pre-daemon-reload unit
   definitions.
3. **Deploy verification compares `InvocationID`**
   (`scripts/verify_cratedigger_cycle.sh capture-migrate` /
   `verify-migrate-ran`), the same discipline already applied to
   `nixos-upgrade.service` and `cratedigger.service`.

## The generalisable lesson

**A `RemainAfterExit` oneshot that other units `Requires=` can have its
switch-time re-run silently swallowed by any concurrent start.** The
ingredients are common: `RemainAfterExit` (so `start` is `-EALREADY`),
`Requires=` dependents (so the stop job queues behind their stops), a slow
dependent (so the queue window is wide), and anything that starts the unit
concurrently (so the queued stop is replaced). Give such a unit
`stopIfChanged = false`. `examples/cratedigger.nix`'s own
`beets-runtime-ready.service` has exactly this shape and carries the same fix.

**And: never verify a `RemainAfterExit` oneshot by its state fields.** They
cannot distinguish "ran for this switch" from "succeeded days ago". Compare
`InvocationID` against a value captured before the trigger.

## Pins

- `nix/tests/module-vm.nix` — the rendered `X-StopIfChanged=false` in
  `[Service]` (the real adapter switch-to-configuration parses), plus the
  behaviour pair: a plain `start` is a silent no-op on the active oneshot, a
  `restart` genuinely re-runs it.
- `tests/test_nix_module.py::TestMigrateUnitCannotBeSwallowedByAConcurrentStart`
  — source pins for `stopIfChanged`/`restartIfChanged`/`RemainAfterExit`.
- `tests/test_deploy_cycle_verifier.py::TestMigrateRanForThisSwitch` — the
  exact #1161 world (all three state fields green, invocation unchanged) fails
  verification.
