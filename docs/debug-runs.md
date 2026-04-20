# Debug Runs Log — Spectral Quality Verification (2026-03-28)

## Run 1 (14:21-14:28, PID 1654705, old code — no spectral CBR 320 check yet)

First run with spectral check in import_one.py (FLAC path) and quality gate spectral awareness. CBR 320 spectral check in cratedigger.py was deployed mid-run so didn't apply.

### Results:
| Album | Source | Type | Spectral New | Spectral Existing | Outcome |
|-------|--------|------|-------------|-------------------|---------|
| Aquarium Drunkard | zozke | FLAC | — | — | BUG: DOWNGRADE PREVENTED (V0 227 ≤ beets 320) |
| Hail and Farewell | amyslskduser | MP3 320 | genuine | genuine 320 | OK: imported 325>320 |
| Zopilote Machine | Unpiloted4193 | FLAC | genuine | **likely_transcode 128** | PERFECT: upgraded 153→221 |
| Come Come Sunset Tree | MrWindUpBird | FLAC | genuine | **likely_transcode 160** | PERFECT: upgraded 189→242 |
| All Eternals Deck | web-graffiti | FLAC | genuine | — | OK: imported 242 |
| Yam King of Crops | amyslskduser | MP3 V0 | **likely_transcode 192** | — | PERFECT: spectral gate caught (beets 222, spectral 192 < 210) |
| 6 albums | bl0atedfisher | FLAC | — | — | Failed download (user offline) |

### Bugs found:
1. **BUG: FLAC→V0 downgrade blocked by fake CBR 320 on disk** — Aquarium Drunkard: genuine FLAC→V0 at 227kbps blocked because beets says existing is 320kbps, but that 320 is upsampled garbage. `import_one.py` reads beets directly, doesn't know pipeline DB set `min_bitrate=0`.
   - **Fix**: Add `--override-min-bitrate` arg to import_one.py, cratedigger.py passes pipeline DB `min_bitrate` value
   - **Status**: Fixed and deployed (commit d3e50ff)

### Cleanup:
- Un-denied zozke (falsely denied for Aquarium Drunkard)
- bl0atedfisher file-based denylist resets each run

---

## Run 2 (14:29-15:00, PID 1674348, CBR 320 spectral check deployed but --override fix ran on OLD Nix store)

Mountain Goats only (non-MG deferred). Had CBR 320 spectral check in cratedigger.py but import_one.py was still old code (no --override-min-bitrate) because cratedigger was started before the deploy finished.

### Results:
| Album | Source | Type | Spectral New | Spectral Existing | Outcome |
|-------|--------|------|-------------|-------------------|---------|
| Taboo VI | AnderMachines | FLAC | — | — | Failed download (user slow) |
| All Eternals Deck | Smeg-for-Brains | MP3 320 | genuine (est 192) | genuine (est 96) | BUG: imported but quality gate re-queued (false spectral estimate) |
| Philyra | bl0atedfisher | FLAC | — | — | Failed download |
| Aquarium Drunkard | zozke | FLAC | — | — | BUG: DOWNGRADE still (old code) |
| Heretic Pride | amyslskduser | MP3 V0 | genuine | genuine | OK: staged for import |
| Yam King of Crops | amyslskduser | FLAC | — | — | Failed download |
| Songs for Peter Hughes | bl0atedfisher | FLAC | — | — | Failed download |
| Protein Source | AnderMachines | FLAC | — | — | (still downloading when stopped) |

### Bugs found:
2. **BUG: Album estimated_bitrate set from single outlier track** — All Eternals Deck: album grade was `genuine` (only 1/15 tracks suspect = 7%) but the one outlier track had `estimated_bitrate=192`. This was propagated to album level and quality gate used it (`spectral_bitrate=192 < 210`) to re-queue the album.
   - **Root cause**: `analyze_album()` set `estimated_bitrate = min(all_track_estimates)` regardless of album grade
   - **Fix**: Only set album `estimated_bitrate` when album grade is `suspect` or `likely_transcode`
   - **Status**: Fixed and deployed (commit 0d888b4)

3. **BUG: --override-min-bitrate not applied** — Aquarium Drunkard still blocked because cratedigger process loaded old Nix store code before deploy completed
   - **Root cause**: Operational — started cratedigger before deploy finished
   - **Fix**: No code change needed, just need fresh process after deploy
   - **Status**: Next run will use correct code

### Cleanup:
- Un-denied zozke (again, for Aquarium Drunkard)
- Un-denied Smeg-for-Brains (All Eternals Deck false positive)

---

## Run 3 (15:00-, PID 1693403, both fixes deployed)

Mountain Goats only. Both fixes deployed:
1. `--override-min-bitrate` passes pipeline DB value to import_one.py
2. Album `estimated_bitrate` only set when album grade is suspect

### Matches queued (as of album 10/20):
| Album | Source | Type | Key test |
|-------|--------|------|----------|
| See America Right | pleasureprince | MP3 V0 | VBR — no spectral needed |
| Songs for Peter Hughes | bl0atedfisher | FLAC | FLAC→V0 conversion test |
| Taking the Dative | bl0atedfisher | FLAC | FLAC→V0 |
| Songs for Petronius | bl0atedfisher | FLAC | FLAC→V0 |
| Hound Chronicles / HGS | shortcut | MP3 320 | **CBR 320 spectral check on known garbage** |
| Hound Chronicles / HGS | Tymemage | FLAC 16/44.1 | Multi-disc FLAC (32 tracks) |
| Hot Garden Stomp | bl0atedfisher | FLAC | FLAC→V0 on known garbage album |
| Songs About Fire | bl0atedfisher | FLAC | FLAC→V0 |
| Aquarium Drunkard | zozke | FLAC | **Testing --override-min-bitrate fix** |

### Issue to investigate later:
- **Hound Chronicles / Hot Garden Stomp double match**: Album 6 matched BOTH MP3 320 from shortcut (16 tracks) AND FLAC from Tymemage (32 tracks). These are two matches for the same pipeline request — one from `try_enqueue` (single disc) and one from `try_multi_enqueue` (multi disc). Need to check if both downloads are queued or just the FLAC. Also: "Hound Chronicles / Hot Garden Stomp" (compilation, album 6) vs "Hot Garden Stomp" (standalone, album 7) are separate pipeline requests with different MBIDs — downloading both is correct.

### Results: (pending — waiting for downloads)

---

## Key Decisions & Assumptions

1. **VBR MP3 downloads skip spectral check** — VBR bitrate IS the quality signal. A V0 file at 240kbps from genuine lossless can't be faked. Only CBR 320 and FLAC need spectral verification.

2. **CBR 320 spectral check runs in cratedigger.py, not import_one.py** — Because CBR 320 downloads don't go through FLAC→V0 conversion, they need spectral checking at the cratedigger level before staging/import.

3. **FLAC spectral check runs in import_one.py** — Informational alongside the V0 conversion. The V0 bitrate is still the primary gate for FLACs. Spectral on existing beets files reveals the "truth" about what's on disk for comparison.

4. **Pipeline DB `min_bitrate` overrides beets bitrate for downgrade check** — When we queue an album for upgrade because we know its files are garbage (e.g. `min_bitrate=0`), the downgrade check in import_one.py must use the pipeline's assessment, not beets' nominal bitrate.

5. **Album `estimated_bitrate` set from ANY outlier track** — Even a single bad track means the album has a quality problem worth upgrading. Initially tried only setting on suspect albums (Bug 10) but reverted — the user wants the worst track to drive the upgrade decision.

6. **Quality gate uses `min(spectral_bitrate, beets_min_bitrate)` for threshold** — If spectral says 192kbps but beets says 222kbps, use 192. This catches fake 320s that would otherwise pass the 210kbps threshold.

7. **Spectral thresholds (from tuning against 65 Mountain Goats albums + 6 genre test suite)**:
   - HF deficit > 60dB OR cliff detected → SUSPECT
   - HF deficit 40-60dB → MARGINAL
   - HF deficit < 40dB → GENUINE
   - Cliff: 2+ consecutive slices with gradient < -12 dB/kHz
   - Album: >60% tracks suspect → SUSPECT album

---

## Run 3 Results (15:00-15:36, PID 1693403, both fixes deployed)

Both fixes deployed and running on new code:
1. `--override-min-bitrate` passes pipeline DB value to import_one.py
2. Album `estimated_bitrate` only set when album is suspect (later reverted)

### Results:
| Album | Source | Type | Outcome |
|-------|--------|------|---------|
| See America Right | pleasureprince | MP3 V0 | OK: genuine V0, upgraded 184→219, quality OK |
| **Aquarium Drunkard** | zozke | FLAC | **OVERRIDE FIX WORKS**: `[OVERRIDE] pipeline says 0kbps, beets says 320kbps`, V0 227 > 0, imported, quality OK 224kbps |
| **Life of World to Come** | bright.wood7932 | FLAC | PERFECT: genuine, existing spectral 128 transcode, upgraded 172→237 |
| Songs for Peter Hughes | bl0atedfisher | FLAC | Failed download |
| Taking the Dative | bl0atedfisher | FLAC | Failed download |
| Songs for Petronius | bl0atedfisher | FLAC | Failed download |
| Hot Garden Stomp | bl0atedfisher | FLAC | Failed download |
| Songs About Fire | bl0atedfisher | FLAC | Failed download |
| Yam King of Crops | bl0atedfisher | FLAC | Failed download |
| Philyra | bl0atedfisher | FLAC | Failed download |
| Protein Source | AnderMachines | FLAC | Downloading very slowly, stuck on requeue errors |

### Bugs found this run:
None — all fixes confirmed working.

---

## Display Fixes (post-run 3)

### Fixed:
1. **Removed album-level Scenario from detail panel** — was showing old import's "strong_match" for a "high_distance" rejection, causing confusion
2. **search_filetype_override downloads show "Upgraded" not "Quality mismatch"** — when replacing garbage CBR 320 with genuine V0, the nominal bitrate goes down (320→224) but it's still an upgrade. Now checks if `search_filetype_override` is set.
3. **Reverted Bug 10 fix** — album `estimated_bitrate` now set from any outlier track again. A single bad track at 192kbps means the album should be upgraded.

### Remaining display issues (cosmetic, not blocking):
- Nine Black Poppies shows "Quality mismatch - accepted" (no search_filetype_override, was manually accepted pre-spectral era)
- Heretic Pride 11:23 shows ↓MP3 V2→MP3 160k (genuine downgrade from ShadowoftheHunter that happened pre-spectral)
