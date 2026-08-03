---
name: metadata-gate-discogs-import
description: Metadata gate held → web 502 pattern; release procedure; discogs LXC has no fleet-deploy path (sudo fix pushed 2026-08-02, deploy pending)
metadata:
  type: project
---

2026-08-02 outage RCA: monthly `discogs-import.timer` on doc2 takes a durable
metadata-gate hold (`/var/lib/cratedigger-metadata-gate/holds/discogs-import`)
and stops the cratedigger web/API/pipeline units, then SSHes (forced-command,
doc2 host key) to the discogs LXC (192.168.1.44) to run the dump import. The
guest's forced command used `/run/current-system/sw/bin/sudo` (non-setuid store
copy) instead of `/run/wrappers/bin/sudo`, so every attempt failed instantly;
the hold is fail-closed and was retained -> `cratedigger-web` ExecCondition
(`cratedigger-metadata-gate`) skipped every start -> socket hit
start-limit-hit -> 502.

**Recovery procedure** (safe when the import never ran / probes pass):
`sudo cratedigger-metadata-gate status|check` then `release discogs-import`,
`systemctl reset-failed cratedigger-web.{service,socket}`, then
`resume-if-clear` (restarts web, importer, preview worker, youtube-ingest,
pipeline + unfindable timers). CLI: `check|start-check|hold R|release R|resume-if-clear|status|watchdog`.

**Fix pushed, deploy pending:** nixosconfig `84e1c2ba` on Forgejo master fixes
`hosts/discogs/configuration-lxc.nix` (wrappers sudo). NOT yet deployed to the
guest as of 2026-08-02: the discogs (and musicbrainz) LXCs have NO
`nixos-upgrade.service` (autoupdate module not enabled), so `fleet-deploy
discogs` fails; abl030 there has password-required sudo only. Deploy needs the
operator: `ssh discogs` then `sudo fleet-update`. Must land before the next
monthly import (~2026-09-01 04:00) or the site goes down the same way again.
