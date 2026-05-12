---
title: "fix: Include MusicBrainz video tracks in Beets matching"
type: fix
status: active
date: 2026-05-12
origin: docs/brainstorms/2026-05-12-video-track-wrong-matches-requirements.md
---

# fix: Include MusicBrainz video tracks in Beets matching

## Summary

Fix video-backed MusicBrainz releases by setting Beets' existing match option:

```yaml
match:
  ignore_video_tracks: false
```

This includes MusicBrainz recordings marked `video=true` in the normal Beets candidate track list. Validation, Wrong Matches, preview, and final import already use the same Beets config through the existing harness/import pipeline, so the first implementation should be a config change, not a new Cratedigger matching path.

## Problem

Beets defaults `match.ignore_video_tracks` to true. For releases like Placebo `We Come in Pieces`, the target MusicBrainz release is DVD-backed and its recordings are marked as video. With the default setting, Beets filters those recordings before matching, so the target MBID can appear as a zero-candidate `mbid_not_found` even when the audio files are a legitimate rip of the release.

Local verification showed the same release returns zero target tracks when `ignore_video_tracks = true` and 20 target tracks when `ignore_video_tracks = false`.

## Decision

Change the Nix-managed Beets config in `/home/abl030/nixosconfig/modules/home-manager/services/beets.nix`:

```nix
match = {
  ignore_video_tracks = false;
  strong_rec_thresh = 0.10;
  medium_rec_thresh = 0.25;
  preferred = {
    countries = ["AU" "US" "GB|UK"];
    media = ["Digital Media|File" "CD"];
    original_year = true;
  };
};
```

## Scope

In scope:

- One Beets config line: `match.ignore_video_tracks = false`.
- Deploy the Nix-managed Beets config through the normal nixosconfig workflow.
- Verify the Placebo-style MBID now produces normal Beets candidate evidence.
- Let existing Wrong Matches validation data populate through the existing validation/triage/backfill paths.

Out of scope:

- No new Wrong Matches UI states.
- No per-row refresh/retry button.
- No video-specific import queue payload.
- No custom MusicBrainz parser.
- No copied `import_one.py`.
- No direct `beet import` path.
- No support promise for arbitrary video containers such as `.mkv`, `.vob`, `.ts`, or `.avi`.

## Why This Is Enough

The failure is caused by Beets filtering target metadata before matching. The flag changes that filtering behavior globally for Beets' normal matching. Because Cratedigger already validates and imports through Beets, this keeps the fix in the system that owns the behavior.

If a folder contains supported audio files ripped from a DVD/Blu-ray release, Beets can match those audio files against video-labeled MusicBrainz recordings once the recordings are not filtered out. If the folder contains only unsupported video files, Beets/mediafile still will not import them as audio. That is unchanged.

## Verification

1. Confirm the generated Beets config contains:

   ```yaml
   match:
     ignore_video_tracks: false
   ```

2. Run matched validation for:

   - Artist: `Placebo`
   - Release: `We Come in Pieces`
   - MBID: `603d1428-0f0b-42b3-9d37-6d87488dd74b`

3. Expected result:

   - Beets no longer returns zero target tracks for the MBID.
   - Wrong Matches shows normal candidate/distance/mapping evidence if the source files match the release.
   - Existing missing-track rules still apply: if the local source does not contain the release's required tracks, do not import it.

## Follow-Up Only If Needed

If global inclusion causes a real regression in ordinary matching, revisit with a narrower harness-scoped flag. Do not build UI state machinery or a parallel matching/import pipeline unless a concrete regression proves the global Beets option is not viable.
