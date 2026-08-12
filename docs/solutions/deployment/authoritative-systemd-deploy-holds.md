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
drains `TIMER_DRIVEN_PRODUCER_UNITS` (main, unfindable, watchdog): cancels
only exact `start/waiting` jobs, lets running oneshots finish naturally,
requires two consecutive inactive/job-free samples, and resets an exact
service already in a job-free terminal `failed` state to `inactive/dead`
before those stable samples (running work is never reset). Each of these
three is `Type=oneshot`, triggered only by its own now-masked timer, so it
goes idle on its own -- nothing needs to actively stop it. Only *then* does
it wait -- bounded by its own shorter timeout, separate from the overall
service-drain budget -- for the still-running importer/preview to empty the
automation queue, and only *after* that does it take the manual metadata-gate
hold that stops the controlled workers (web, preview, importer) and YouTube
ingest.

YouTube ingest is deliberately excluded from the pre-hold producer drain and
drained afterward instead, once the gate hold has actually stopped it -- the
same `GATE_STOPPED_UNITS` restart target `abort` restarts, alongside the
three controlled workers. The drain that proves this happened is
`SERVICE_UNITS` (every unit the module knows about), not the narrower
`GATE_STOPPED_UNITS`: `cratedigger.service` is itself gate-guarded, so the
hold should already have stopped it if it was running, but nothing before
that re-verified it for main specifically. This pre-hold window is the one
place this module ever leaves `cratedigger.service` without a start
inhibitor -- `prepare_controlled` establishes one for its own post-release
window, precisely because a manual start or a foreign hold's
`resume-if-clear` would otherwise start main there too. Here, the timer
mask alone does not cover either: it blocks only the timer trigger, not a
manual `systemctl start` or an unrelated hold's `resume-if-clear` (#1078
BLOCKER F3). YouTube is
`Type=simple`, `wantedBy=multi-user.target`, `Restart=on-failure`, with no
timer at all -- an always-on daemon nothing before the gate hold ever asks to
stop. Draining it pre-hold (the original #1078 fix's mistake) waits the full
service-drain timeout for a unit nothing is going to stop -- that drain's
unit set (`PRODUCER_SERVICE_UNITS`) includes `cratedigger-unfindable.service`,
so as of issue #1112 review round 2 it is bounded by
`_PRODUCER_DRAIN_TIMEOUT_SECONDS` (21600s / 6h), not the shorter
`_DRAIN_TIMEOUT_SECONDS` that bounds unit sets without unfindable in them --
then fails with the gate hold never taken -- the exact failure shape #1078
exists to remove, reproduced by the reorder itself. The pre-hold window also
owns no
temporary start inhibitor: masking already blocks a fresh *timer* trigger
(though not an unrelated hold's own resume-if-clear, which starts
`cratedigger.service` directly via the gate's `resume_units` regardless of
the timer's mask state), and YouTube is not being waited on in this window
at all, so there is no persistent `/var/lib` artifact to orphan if the host
reboots mid-window at all -- unlike the manual hold and producer inhibitors
taken later, which do persist and are exactly what `abort`'s reboot recovery
below adopts.

Taking the gate hold before draining the queue is exactly the pre-#1078 bug:
the hold's external tool stops the importer and preview workers, which are
the only thing that ever drains `active_automation_jobs` and
`dirty_downloading_rows` -- so a queued job or mid-handoff row could never
clear, and the old-lifecycle preflight below could never pass. Reordering
removes the deadlock by construction: every later failure still leaves the
deployment more quiesced than it started (masks and, once taken, the gate
hold), the same "fail into maximum quiescence" property acquisition has
always had.

Once the queue has drained (or an anomaly short-circuits the wait -- see
below) and the gate hold is active, and before migration, acquisition queries
the live schema through read-only `pipeline-cli query`. It fails under the
authoritative hold unless active automation jobs, every recovery-required
job, staged/launch-marked downloading rows, and missing/malformed PR1
`enqueued_at` witnesses are all zero. `active_automation_jobs`/
`dirty_downloading_rows` are exactly the two fields the queue-drain wait above
already resolves in the ordinary case; a `recovery_required_jobs` or
`malformed_enqueued_at_rows` anomaly -- neither of which anything drains --
fails here instead, immediately, with the full boundary (masks and gate hold)
already established. `recovery_required_jobs` is also counted inside
`active_automation_jobs`'s own SQL (`status IN ('queued', 'running',
'recovery_required')`), so a stuck recovery-required job would otherwise make
the queue-drain wait above run its full 30-minute timeout and then report
only the misleading aggregate. The wait therefore stops the moment either
anomaly field is dirty, rather than waiting for a count that can never reach
zero, and lets this final check report the complete, accurate field dict
instead. The helper performs no migration, cleanup, or lifecycle repair.

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
controlled-start contract, or a SIGINT or dropped SSH that left the receipt
stranded partway through acquisition while the host stayed up. Before #1078
the only documented answer was "do not remove the receipt by hand," with
nothing offered instead.

**`abort` survives a host reboot too (#1096).** The receipt under `/run` and
the timer control-links under `/run/systemd/system.control` are both tmpfs
and do not survive one -- but the manual gate hold and the producer start
inhibitors under `/var/lib/cratedigger-metadata-gate` are real disk state and
can outlive the receipt that took them. #1078's own producer-drain-before-hold
window keeps no receipt-owned object on persistent storage, so it does not
widen that exposure; the exposure that remains is exactly `prepare_controlled`'s
YouTube start inhibitor (persistent, owned across `prepared-controlled`/
`main-timer-open`) and, for a reboot at or after `PHASE_HELD` before
`prepare_controlled` releases it, the manual hold itself.

The fix is a persistent sibling marker beside each of those two owned-object
classes -- `deploy-hold-owned-manual` and `deploy-hold-owned-inhibit-<unit>`,
both directly in `/var/lib/cratedigger-metadata-gate` (never inside `holds/`,
which the gate reads as hold *reasons*). `mark_manual_hold_owned()` /
`mark_inhibitor_owned()` write the persistent marker before the object itself
is ever created, so an object without its marker is provably foreign;
`unmark_manual_hold_owned()` / `unmark_inhibitor_owned()` remove it only after
the object is already gone. While a receipt exists it remains the sole
authority -- these persistent markers are consulted only when no receipt (live
or retired) exists, which is exactly the shape a reboot leaves behind.

A receiptless `abort` (`_adopt_persistent_markers_or_refuse`) reads exactly
those markers: with none present at all -- an ordinary clean boot with no
prior deploy hold -- it refuses precisely like it always has, so a boot can
never turn into a mass restart. With one or more present, it proves no
unmarked (foreign) object or foreign metadata-gate hold conflicts before
touching anything, then adopts exactly the marked objects -- releasing the
manual hold and/or removing the marked inhibitors, restarting and proving
active whatever they blocked (reusing the identical restart-then-disown shape
the receipt-owned branches below already use, including for an orphan marker
whose object was never actually created because the crash landed between the
marker write and the object's own creation -- starting an already-running or
never-actually-blocked unit is an idempotent no-op), then clearing the
markers -- ending at the same ordinary, unheld operation every other path
through `abort` reaches. It never re-establishes a receipt or a phase: after
a reboot there is nothing to recover TO, only ordinary operation to restore.
`acquire`'s own refusal for an object carrying one of these markers now names
`abort` as the way out (distinct from its refusal for a genuinely unmarked,
foreign object, which still refuses with no such pointer). `recover-held`
still requires a receipt -- the phase knowledge a reboot destroys is exactly
what it exists to resume -- so the supported reboot recovery is always
`abort` followed by a fresh `acquire`.

Rejected alternatives (issue comment
[5266609958](https://github.com/abl030/cratedigger/issues/1096#issuecomment-5266609958)):
moving the whole receipt to `/var/lib` (the tmpfs receipt's reboot-clears-
stale-state property is deliberate), and encoding ownership in the inhibitor
file's own content (the inhibitor's content is a format the external gate
tool reads; changing it is a wider, riskier change than a sibling marker).

Every ownership class this receipt could hold -- the manual gate hold, every
owned producer-start inhibitor, every owned timer control-link mask -- is
validated up front, before any mutation. A refusal here therefore never
leaves the boundary half torn down: without it, discovering an unowned
inhibitor mid-teardown (after the hold was already released and workers
already started) would leave `abort` stuck, unable to finish releasing and
unable to cleanly return to HELD either (`recover-held` hits the identical
conflict re-taking the hold).

`abort` then walks the same ownership markers acquisition records intent
through and releases exactly the ones the receipt owns, in the reverse of the
order acquisition took them -- restarting what that ownership implies it
stopped only after that restart is *proven*, and disowning only after that
proof, so an interrupted retry never sees "nothing owned" while the
underlying object is still stopped:

- the manual gate hold, if owned. Releasing it is what the external gate tool
  consults to let every gate-guarded unit start again, so `abort` restarts
  all four -- web, preview, importer, and YouTube ingest, itself gate-guarded
  since #1078 -- and proves every one is stably active
  (`_wait_controlled_workers_active`, the same check `prepare_controlled`
  uses) before trusting the release and disowning the hold. A foreign hold
  (for example the monthly discogs-import hold) makes that proof fail loudly
  instead of `abort` silently exiting 0 with every worker still down. A
  `systemctl start` that a gate-guarded unit's `ExecCondition` silently
  skips still returns success -- the CLI sees a condition skip, not a
  failure;
- every owned producer-start inhibitor;
- every owned timer control-link mask, restarted and proven active before
  being disowned -- restarting that timer is what returns
  `cratedigger-unfindable.service` and the watchdog to their ordinary
  cadence, and `cratedigger.service` too if nothing restarted it already.
  Unlike the other two, main is also named in the metadata gate's own
  `resume_units`, so a `resume-if-clear` call -- the one `abort` issues
  above, or one raised by an unrelated hold's release -- can and does start
  it directly, independent of its timer. `cratedigger-unfindable.service`
  and the watchdog are absent from `resume_units`, so they really are only
  ever timer-triggered.

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
