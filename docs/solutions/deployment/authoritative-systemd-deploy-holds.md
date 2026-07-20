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
unfindable, and metadata-gate-watchdog timers; its fixed drain set is their
three services.

Acquisition creates exact `/dev/null` links under
`/run/systemd/system.control`, proves every timer is `LoadState=masked`, stops
the timers, cancels only exact `start/waiting` jobs, lets running oneshots
finish naturally, and requires two consecutive inactive/job-free samples.
An exact service already in a job-free terminal `failed` state is reset to
`inactive/dead` before those stable samples; running work is never reset.
The helper records intent before creating its manual metadata hold and each
link in a root-owned mode-0700 receipt under
`/run/cratedigger-deploy-hold`; an interrupted `acquire` can therefore be
rerun without adopting unrelated state. Initial publication and final
retirement of that receipt are atomic directory renames; reserved partial
staging/retirement directories contain only validated root-owned files and are
safe for the same command to finish after interruption. A
pre-existing hold/link or a changed owned link is an error; release never
guesses ownership.

Recovery is deliberately staged:

1. keep all timer masks while one controlled main cycle is started and verified;
2. open only the main timer and capture its ordinary successor;
3. restore the watchdog and unfindable timers and resume the metadata gate;
4. clear the receipt only after the exact ordinary successor verifies.

`scripts/verify_cratedigger_cycle.sh` owns invocation capture and terminal
proof. The deploy workflow passes its captured ordinary `InvocationID` through
the hold receipt; the hold helper does not reimplement journal verification.

If a release phase fails, stop. Leave the receipt and remaining owned masks in
place, inspect the named phase and exact link/job state, and run
`recover-held` to re-mask all three timers, restore the manual gate, drain exact
jobs, and return to the held phase before restarting release. Rerun an
interrupted `acquire` directly; rerun an interrupted `complete` when its
retired-receipt cleanup is pending. Do not remove receipt markers or
`system.control` links by hand: doing so discards the ownership evidence that
makes recovery safe.
