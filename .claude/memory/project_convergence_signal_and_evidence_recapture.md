---
name: project-convergence-signal-and-evidence-recapture
description: 2026-07-27 — inter-candidate cliff constancy ("the network has converged") as a search-stop signal, and the denylist-release technique for recapturing evidence
metadata:
  type: project
---

## The convergence signal

Found via Iron & Wine — *The Creek Drank the Cradle* (request 1240): **226
candidates, 170 distinct peers, 5 codecs — every one reporting `likely_transcode`
at exactly `spectral_bitrate = 96`.** The album was tracked to a 4-track
cassette, which rolls off ~14–15 kHz; `detect_cliff` reads that as the 96k
bucket. The band-limit is in the master, not the encoding.

**What repeated identical cliff position across independent peers proves:**
NOT that the music is legitimate — it cannot separate "genuine band-limited
master" from "one lossy origin everyone re-shared", since both give an
identical cliff everywhere. It proves **convergence: no better copy exists on
the network.** A *search-termination* signal, not a *provenance* signal.

**What must never be done with it:** auto-promote to verified lossless (that
claims bit-faithfulness; convergence establishes only search exhaustion), or
auto-stop the search (the never-stop invariant bans auto-throttling on apparent
unfindability).

**Correct shape (operator decision, 2026-07-27):** surface converged requests in
triage/dashboard with the evidence — candidate count, distinct peers, distinct
codecs, `cliff_hz` spread — and ask "stop?". The operator answers; accepting
sets `unsearchable`, which is already defined as an explicit, reversible
operator search stop. The operator may hold external knowledge spectral
analysis can never supply (that this record was tracked to tape), so the row
ends **unproven but understood**.

An earlier proposal to keep searching silently was rejected: it retains the
whole peer/bandwidth cost while discarding the benefit, and is an antisocial
default for a distributed module.

**The falsification invariant is the real value.** The convergence claim is
falsifiable and the falsification IS the signal — a candidate that *differs*
becomes the highest-value event in the request's history. Break upward (no
cliff / near-Nyquist) = the real master exists, long-tail rescue. Break sideways
= likely a different pressing, surface but never act. Break downward = one
peer's bad transcode, now provably noise.

Only rigorous with **`cliff_hz`** (PR1, migration 065, deployed 2026-07-27):
`96` is a coarse bucket that cannot distinguish "170 peers at exactly 14500"
from "scattered 13.8–14.9 kHz".

Full write-up: https://github.com/abl030/cratedigger/issues/829#issuecomment-5088045737
and its correction https://github.com/abl030/cratedigger/issues/829#issuecomment-5088076688

## Evidence recapture by denylist release

To generate fresh evidence under new measurement code without waiting for
organic candidates: delete `source_denylist` rows so those peers become
eligible again. 2026-07-27 released **762 rows** (17,816 → 17,055) across all 39
provisional-lossless requests. Back the rows up first (`pipeline-cli query
--json`) — restoring is an insert by id. Deletion is via
`pipeline-cli query --write --confirm WRITE -`; there is no un-ban CLI
subcommand.

Cohort selection that worked: requests where `status='wanted'` and the current
evidence has `verified_lossless IS NOT TRUE` and lossless lineage
(`was_converted_from` or `codec` in flac/wav/alac). Cap per request with
`row_number() OVER (PARTITION BY request_id ORDER BY id)`.

## Gotcha: beets-rejected candidates DO get full evidence

**Do not assume a `high_distance` / `mbid_not_found` rejection means nothing was
measured.** Measured on prod: of 9,131 evidence rows linked from beets-rejected
candidates, 100% carry `min_bitrate_kbps` and `codec`, 98.6% carry
`spectral_grade`, 97% have `audio_file_count > 0`.

The trap: `download_log`'s OWN `spectral_grade` / `actual_min_bitrate` columns
are sparse on those rows (2,101 and 0 respectively of 12,133), so reading them
suggests no evidence exists. The evidence lives on the row reached through
`download_log.candidate_evidence_id` → `album_quality_evidence`. Reading the
wrong column family caused an incorrect decision to exclude the largest
denylist tranche from a recapture sweep.

Related: [[project-829-spectral-calibration]]
