# Authoritative systemd deploy holds on NixOS

Issue [#750](https://github.com/abl030/cratedigger/issues/750) records two
separate systemd races that made the former strict-deploy recipe unsafe.

First, NixOS materializes these generated units under
`/etc/systemd/system`. That directory outranks `/run/systemd/system`, so
`systemctl mask --runtime` could create a real `/run` link while systemd kept
loading and running the higher-precedence `/etc` unit. The authoritative
runtime location is `/run/systemd/system.control`.

Second, masking a timer does not cancel a service start job the timer already
queued. A timer can therefore report masked while its oneshot still has a
`start/waiting` job ready to run. Replacing the service unit itself with a mask
is not a safe answer: reloading a running oneshot through changed service
metadata previously caused systemd to terminate the active cycle.

## Permanent boundary

`scripts/cratedigger_deploy_hold.py` owns the strict lifecycle. It accepts no
unit names and never masks a service. Its fixed trigger timers are the main,
unfindable, and metadata-gate-watchdog timers; its fixed drain set is every
metadata-gate-guarded Cratedigger service, including web, preview, importer,
and YouTube ingest.

Acquisition creates exact `/dev/null` links under
`/run/systemd/system.control`, proves every timer is `LoadState=masked`, and
stops the timers. Before publishing a receipt, the helper verifies the
separately deployed downstream contract: main and YouTube have their exact
independent start inhibitors, web/preview/importer do not, and the metadata
gate's guarded/resume sets include YouTube. The helper records intent before
creating its manual metadata hold and each link/inhibitor in a root-owned
mode-0700 receipt under `/run/cratedigger-deploy-hold`; an interrupted
`acquire` can therefore be rerun without adopting unrelated state. Initial
publication and final retirement of that receipt are atomic directory
renames; reserved partial staging/retirement directories contain only
validated root-owned files and are safe for the same command to finish after
interruption. A pre-existing hold/link or a changed owned link is an error;
release never guesses ownership.

**Quiesce drains before it stops (#1078).** With the timers masked, `acquire`
owns a temporary start inhibitor for main and YouTube -- neither has anything
else blocking a fresh start yet, and web (a controlled worker, still up) could
still trigger one -- then drains `PRODUCER_SERVICE_UNITS` (main, unfindable,
watchdog, YouTube ingest): cancels only exact `start/waiting` jobs, lets
running oneshots finish naturally, requires two consecutive inactive/job-free
samples, and resets an exact service already in a job-free terminal `failed`
state to `inactive/dead` before those stable samples (running work is never
reset). Only *then* does it wait -- bounded by its own shorter timeout,
separate from the overall service-drain budget -- for the still-running
importer/preview to empty the automation queue, and only *after* that does it
take the manual metadata-gate hold that stops the controlled workers (web,
preview, importer) and reinforces the stop on main/YouTube. The temporary
producer-start inhibitors are released once the gate hold is active, then the
controlled workers are drained the same way the producers were.

Taking the gate hold before draining the queue is exactly the pre-#1078 bug:
the hold's external tool stops the importer and preview workers, which are
the only thing that ever drains `active_automation_jobs` and
`dirty_downloading_rows` -- so a queued job or mid-handoff row could never
clear, and the old-lifecycle preflight below could never pass. Reordering
removes the deadlock by construction: every later failure still leaves the
deployment more quiesced than it started (masks and, once taken, the gate
hold), the same "fail into maximum quiescence" property acquisition has
always had.

Once the queue has drained and the gate hold is active, and before migration,
acquisition queries the live schema through read-only `pipeline-cli query`.
It fails under the authoritative hold unless active automation jobs, every
recovery-required job, staged/launch-marked downloading rows, and
missing/malformed PR1 `enqueued_at` witnesses are all zero.
`active_automation_jobs`/`dirty_downloading_rows` are exactly the two fields
the queue-drain wait above already resolves in the ordinary case; only a
`recovery_required_jobs` or `malformed_enqueued_at_rows` anomaly -- neither of
which anything drains -- can still fail here, immediately, with the full
boundary (masks and gate hold) already established. The helper performs no
migration, cleanup, or lifecycle repair.

Recovery is deliberately staged:

1. keep all timer masks, create receipt-owned main and YouTube start
   inhibitors, release the manual gate, explicitly start and prove
   web/preview/importer, and exercise an overlapping `resume-if-clear` while
   both producers remain inactive; then remove only the main inhibitor and
   start one controlled main cycle;
2. open only the main timer and capture its ordinary successor;
3. restore the watchdog and unfindable timers, remove the owned YouTube
   inhibitor immediately before resuming the metadata gate;
4. clear the receipt only after the exact ordinary successor verifies.

`scripts/verify_cratedigger_cycle.sh` owns invocation capture and terminal
proof. The deploy workflow passes its captured ordinary `InvocationID` through
the hold receipt; the hold helper does not reimplement journal verification.

If a release phase fails, stop. Leave the receipt and remaining owned masks in
place, inspect the named phase and exact link/job state, and run
`recover-held` to re-mask all three timers, restore the manual gate, drain exact
jobs, remove only receipt-owned unchanged producer inhibitors, and return to
the held phase before restarting release. Rerun an
interrupted `acquire` directly; rerun an interrupted `complete` when its
retired-receipt cleanup is pending. Do not remove receipt markers or
`system.control` links or metadata-gate inhibitor files by hand: doing so
discards the ownership evidence that makes recovery safe.

`recover-held` on a receipt still at the acquiring phase shares acquire's own
producer-drain-before-hold order rather than the else-branch above -- an
acquiring receipt has never reached HELD, so recovery must re-prove exactly
what `acquire` proves, in the same order, or it would reintroduce the same
#1078 deadlock while "recovering."

## `abort`: the way out of a hold that should never complete

`recover-held` only ever re-proves or re-establishes the strict boundary; it
offers no way out of a receipt whose preconditions will never be satisfied --
an anomaly preflight field (`recovery_required_jobs` or
`malformed_enqueued_at_rows`, both of which nothing drains), a stale
controlled-start contract, or a SIGINT, dropped SSH, or host reboot that left
the receipt stranded partway through acquisition. Before #1078 the only
documented answer was "do not remove the receipt by hand," with nothing
offered instead.

`abort` walks the same ownership markers acquisition records intent through
and releases exactly the ones the receipt owns, in the reverse of the order
acquisition took them, then restarts what that ownership implies it stopped:
the manual gate hold, if owned (restarting the controlled workers the
external gate tool stopped when the hold was taken); every owned
producer-start inhibitor; and every owned timer control-link mask
(restarting that timer, which returns `cratedigger.service`,
`cratedigger-unfindable.service`, and the watchdog to their ordinary
cadence). Neither producer is ever started directly -- each is only ever
timer- or externally-triggered, and `cratedigger-youtube-ingest` has no timer
at all, so clearing its inhibitor is what returns it to ordinary operation.
It then removes the receipt, resuming an interrupted retirement the same way
`complete` does if a prior `abort` was itself interrupted mid-retirement.

It follows the same ownership discipline as every other command here: it
never adopts or mutates an object the receipt did not itself create, and
fails closed (leaving the receipt in place) if an unowned object -- most
plausibly a producer-start inhibitor an operator created independently --
sits where only a receipt-owned one is expected. It is safe to run from every
known receipt phase, including phases release has already progressed past
(prepared-controlled, main-timer-open, complete-pending), where it is simply
a stronger, invocation-unaware form of walking the same release chain
backward.
