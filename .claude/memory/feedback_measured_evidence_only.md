---
name: measured-evidence-only
description: Never propose capturing/using slskd-advertised metadata for quality — advertised bitrates are wrong more often than not; the quality model is measurement-only
metadata: 
  node_type: memory
  type: feedback
  originSessionId: 2f201277-ea14-4a12-96d9-f567f6debb65
---

The operator rejected capturing the slskd-advertised bitrate (#599, closed won't-do 2026-07-10): "slsk users lie, or slskd lies. it doesn't matter who lies really… the slskd bitrate is more often than not wrong and who cares."

**Why:** the pipeline's quality model is deliberately measurement-only — spectral analysis, V0 probes, actual per-track bitrates. Advertised metadata from peers is noise; "advertised vs measured" comparisons and "lying peer" heuristics built on it are worthless when the advertisement is usually wrong.

**How to apply:** don't propose features that ingest, display, or gate on peer-advertised quality claims. When dead advertised-metadata plumbing turns up (e.g. the never-written `slskd_bitrate` column, now slated for deletion in #598), the right move is deletion, not wiring it up. `slskd_filetype` is the exception — populated and used for the `downloaded_label`, but it's derived from the measured filetype, not the advertisement. Related: [[project-575-ui-consolidation]].

**The flip side (2026-07-11, PR #612):** MEASURED evidence is load-bearing and must never be hidden or mislabeled. The operator: "V0 ain't just for lossless research, it's turned pretty load-bearing and is really useful for me" — V0 probes run on every candidate (research probes are real ffmpeg V0-transcode measurements) and render everywhere with lineage qualifiers ("(from lossy)"), never suppressed as "audit-only". And every displayed number must say which statistic it is (min vs avg — the "Min bitrate" row, `min 216k` strip labels, basis metric truthfulness property). Hiding or mislabeling a real measurement is the same class of sin as displaying a fake one.
