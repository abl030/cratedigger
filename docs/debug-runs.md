# Debug Runs Log

## Issue: Hound Chronicles / Hot Garden Stomp double match (Run 3, 2026-03-28)

Album 6/20 was "The Hound Chronicles / Hot Garden Stomp" (compilation, different MBID from standalone "Hot Garden Stomp" at album 7/20). The compilation matched TWO sources:
- MP3 320 from `shortcut` (16 tracks)
- FLAC 16/44.1 from `Tymemage` (32 tracks)

This appears to be the multi-disc match path (`try_multi_enqueue`) finding both a single-disc MP3 match and a full FLAC match. Need to investigate:
- Why did it match two sources? Did `try_enqueue` fail and fall through to `try_multi_enqueue`?
- Are both downloads queued or just the FLAC?
- The 32-track FLAC from Tymemage was the second SUCCESSFUL MATCH — does that overwrite the first?

Also: "Hot Garden Stomp" standalone (album 7) and "The Hound Chronicles / Hot Garden Stomp" (album 6) are separate pipeline requests. The standalone was queued for upgrade because we identified its 320kbps files as garbage. The compilation might have been a separate wanted request from the user. These are different MBIDs — downloading both is correct behaviour but may cause beets path conflicts if they share the same artist/title folder.

## Run 2 Results Summary (2026-03-28 13:57-14:28, PID 1654705)

### Successful upgrades:
- **Zopilote Machine**: FLAC genuine → existing was 128kbps transcode → upgraded 153→221kbps, quality OK
- **Come Come Sunset Tree**: FLAC genuine → existing was 160kbps transcode → upgraded 189→242kbps, quality OK
- **All Eternals Deck**: FLAC genuine → imported at 242kbps, quality OK
- **Hail and Farewell**: MP3 320 genuine → both new and existing genuine → imported 325>320

### Spectral catches:
- **Yam, King of Crops**: MP3 V0 from amyslskduser → spectral `likely_transcode` at 192kbps → quality gate used spectral (192 < 210) instead of beets (222) → re-queued, denylisted amyslskduser

### Bugs found:
1. **Aquarium Drunkard downgrade false positive**: FLAC from zozke → V0 at 227kbps → import_one said "227 ≤ 320 downgrade" but existing 320 is garbage. Fix: `--override-min-bitrate` from pipeline DB (deployed but ran on old code)
2. **All Eternals Deck spectral false positive**: Album grade=genuine but one outlier track set `estimated_bitrate=192` → quality gate used it to re-queue. Fix: only set album estimated_bitrate when album grade is suspect (fixed and deployed)

### Failed downloads (user offline):
- Songs for Petronius, Hot Garden Stomp, Songs About Fire, Life of World to Come, Philyra, Songs for Peter Hughes — all from bl0atedfisher

## Run 3 (2026-03-28 15:00-, PID 1693403)

Running with both fixes deployed. Key tests:
- Aquarium Drunkard matched FLAC from zozke again — will test --override-min-bitrate fix
- Hound Chronicles / Hot Garden Stomp matched MP3 320 from shortcut — will test spectral on known garbage
