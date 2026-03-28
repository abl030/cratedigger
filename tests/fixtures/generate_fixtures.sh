#!/usr/bin/env bash
# Generate test album fixtures from a source FLAC album.
#
# Usage: ./generate_fixtures.sh [source_dir]
#   source_dir defaults to the Helen Matthews album.
#
# Produces ~20 test albums in tests/fixtures/albums/, each encoding the
# source tracks differently to exercise every quality gate branch.
# Idempotent — deletes and regenerates everything.

set -euo pipefail

SOURCE="${1:-/mnt/data/Media/Music/New/Helen Matthews/1998 - A Long Time Coming}"
OUTDIR="$(cd "$(dirname "$0")" && pwd)/albums"

if [ ! -d "$SOURCE" ]; then
    echo "ERROR: Source directory not found: $SOURCE" >&2
    echo "Pass a directory of FLAC files as argument." >&2
    exit 1
fi

# Count source FLACs
FLACS=("$SOURCE"/*.flac)
if [ ${#FLACS[@]} -eq 0 ]; then
    echo "ERROR: No FLAC files in $SOURCE" >&2
    exit 1
fi
echo "Source: $SOURCE (${#FLACS[@]} tracks)"

# Use first 8 tracks for speed (enough to test album-level classification)
MAX_TRACKS=8

rm -rf "$OUTDIR"
mkdir -p "$OUTDIR"

# Helper: copy first N FLAC tracks to a target dir
copy_flacs() {
    local dest="$1"
    mkdir -p "$dest"
    local count=0
    for f in "$SOURCE"/*.flac; do
        cp "$f" "$dest/"
        count=$((count + 1))
        [ $count -ge $MAX_TRACKS ] && break
    done
}

# Helper: encode all FLACs in a dir to MP3 with given args, remove FLACs
encode_mp3() {
    local dir="$1"
    shift
    for f in "$dir"/*.flac; do
        local mp3="${f%.flac}.mp3"
        ffmpeg -y -i "$f" -codec:a libmp3lame "$@" "$mp3" 2>/dev/null
        rm "$f"
    done
}

# Helper: encode FLACs to MP3 then back to FLAC (fake lossless)
make_fake_flac() {
    local dir="$1"
    shift
    for f in "$dir"/*.flac; do
        local mp3="${f%.flac}.mp3"
        ffmpeg -y -i "$f" -codec:a libmp3lame "$@" "$mp3" 2>/dev/null
        rm "$f"
        ffmpeg -y -i "$mp3" "${mp3%.mp3}.flac" 2>/dev/null
        rm "$mp3"
    done
}

echo ""
echo "=== Generating test albums ==="

# ---------------------------------------------------------------------------
# 1. GENUINE FLAC — untouched CD rip
# ---------------------------------------------------------------------------
echo "[01] genuine_flac"
copy_flacs "$OUTDIR/01_genuine_flac"

# ---------------------------------------------------------------------------
# 2. GENUINE V0 — FLAC → VBR V0 (gold standard target)
# ---------------------------------------------------------------------------
echo "[02] genuine_v0"
copy_flacs "$OUTDIR/02_genuine_v0"
encode_mp3 "$OUTDIR/02_genuine_v0" -q:a 0

# ---------------------------------------------------------------------------
# 3. GENUINE V2 — FLAC → VBR V2 (lower quality VBR)
# ---------------------------------------------------------------------------
echo "[03] genuine_v2"
copy_flacs "$OUTDIR/03_genuine_v2"
encode_mp3 "$OUTDIR/03_genuine_v2" -q:a 2

# ---------------------------------------------------------------------------
# 4. CBR 320 — all tracks at 320kbps CBR
# ---------------------------------------------------------------------------
echo "[04] cbr_320"
copy_flacs "$OUTDIR/04_cbr_320"
encode_mp3 "$OUTDIR/04_cbr_320" -b:a 320k

# ---------------------------------------------------------------------------
# 5. CBR 256 — all tracks at 256kbps CBR
# ---------------------------------------------------------------------------
echo "[05] cbr_256"
copy_flacs "$OUTDIR/05_cbr_256"
encode_mp3 "$OUTDIR/05_cbr_256" -b:a 256k

# ---------------------------------------------------------------------------
# 6. CBR 192 — all tracks at 192kbps CBR
# ---------------------------------------------------------------------------
echo "[06] cbr_192"
copy_flacs "$OUTDIR/06_cbr_192"
encode_mp3 "$OUTDIR/06_cbr_192" -b:a 192k

# ---------------------------------------------------------------------------
# 7. CBR 128 — all tracks at 128kbps CBR
# ---------------------------------------------------------------------------
echo "[07] cbr_128"
copy_flacs "$OUTDIR/07_cbr_128"
encode_mp3 "$OUTDIR/07_cbr_128" -b:a 128k

# ---------------------------------------------------------------------------
# 8. CBR 96 — garbage quality
# ---------------------------------------------------------------------------
echo "[08] cbr_96"
copy_flacs "$OUTDIR/08_cbr_96"
encode_mp3 "$OUTDIR/08_cbr_96" -b:a 96k

# ---------------------------------------------------------------------------
# 9. FAKE FLAC 128 — MP3 128k wrapped in FLAC container
# ---------------------------------------------------------------------------
echo "[09] fake_flac_128"
copy_flacs "$OUTDIR/09_fake_flac_128"
make_fake_flac "$OUTDIR/09_fake_flac_128" -b:a 128k

# ---------------------------------------------------------------------------
# 10. FAKE FLAC 96 — MP3 96k wrapped in FLAC container (upsampled garbage)
# ---------------------------------------------------------------------------
echo "[10] fake_flac_96"
copy_flacs "$OUTDIR/10_fake_flac_96"
make_fake_flac "$OUTDIR/10_fake_flac_96" -b:a 96k

# ---------------------------------------------------------------------------
# 11. FAKE FLAC 192 — MP3 192k wrapped in FLAC container
# ---------------------------------------------------------------------------
echo "[11] fake_flac_192"
copy_flacs "$OUTDIR/11_fake_flac_192"
make_fake_flac "$OUTDIR/11_fake_flac_192" -b:a 192k

# ---------------------------------------------------------------------------
# 12. FAKE FLAC 320 — MP3 320k wrapped in FLAC (hard to detect)
# ---------------------------------------------------------------------------
echo "[12] fake_flac_320"
copy_flacs "$OUTDIR/12_fake_flac_320"
make_fake_flac "$OUTDIR/12_fake_flac_320" -b:a 320k

# ---------------------------------------------------------------------------
# 13. MIXED BITRATE — 7 tracks at 320k + 1 track at 192k (all CBR)
# ---------------------------------------------------------------------------
echo "[13] mixed_cbr_320_192"
mkdir -p "$OUTDIR/13_mixed_cbr_320_192"
count=0
for f in "$SOURCE"/*.flac; do
    base=$(basename "${f%.flac}.mp3")
    if [ $count -lt 7 ]; then
        ffmpeg -y -i "$f" -codec:a libmp3lame -b:a 320k "$OUTDIR/13_mixed_cbr_320_192/$base" 2>/dev/null
    else
        ffmpeg -y -i "$f" -codec:a libmp3lame -b:a 192k "$OUTDIR/13_mixed_cbr_320_192/$base" 2>/dev/null
    fi
    count=$((count + 1))
    [ $count -ge $MAX_TRACKS ] && break
done

# ---------------------------------------------------------------------------
# 14. MIXED BITRATE — 7 tracks at 320k + 1 track at 128k (all CBR)
# ---------------------------------------------------------------------------
echo "[14] mixed_cbr_320_128"
mkdir -p "$OUTDIR/14_mixed_cbr_320_128"
count=0
for f in "$SOURCE"/*.flac; do
    base=$(basename "${f%.flac}.mp3")
    if [ $count -lt 7 ]; then
        ffmpeg -y -i "$f" -codec:a libmp3lame -b:a 320k "$OUTDIR/14_mixed_cbr_320_128/$base" 2>/dev/null
    else
        ffmpeg -y -i "$f" -codec:a libmp3lame -b:a 128k "$OUTDIR/14_mixed_cbr_320_128/$base" 2>/dev/null
    fi
    count=$((count + 1))
    [ $count -ge $MAX_TRACKS ] && break
done

# ---------------------------------------------------------------------------
# 15. MIXED VBR+CBR — 6 tracks VBR V0 + 2 tracks CBR 320
# ---------------------------------------------------------------------------
echo "[15] mixed_vbr_cbr"
mkdir -p "$OUTDIR/15_mixed_vbr_cbr"
count=0
for f in "$SOURCE"/*.flac; do
    base=$(basename "${f%.flac}.mp3")
    if [ $count -lt 6 ]; then
        ffmpeg -y -i "$f" -codec:a libmp3lame -q:a 0 "$OUTDIR/15_mixed_vbr_cbr/$base" 2>/dev/null
    else
        ffmpeg -y -i "$f" -codec:a libmp3lame -b:a 320k "$OUTDIR/15_mixed_vbr_cbr/$base" 2>/dev/null
    fi
    count=$((count + 1))
    [ $count -ge $MAX_TRACKS ] && break
done

# ---------------------------------------------------------------------------
# 16. V0 FROM TRANSCODE — 128k MP3 → FLAC → V0 (should produce low bitrate)
# ---------------------------------------------------------------------------
echo "[16] v0_from_transcode"
copy_flacs "$OUTDIR/16_v0_from_transcode"
# First degrade to 128k MP3, then re-encode to FLAC, then the test will convert to V0
make_fake_flac "$OUTDIR/16_v0_from_transcode" -b:a 128k

# ---------------------------------------------------------------------------
# 17. CBR 320 MIXED WITH V0 — simulates Soulseek user with mixed sources
# 4 tracks CBR 320, 4 tracks VBR V0
# ---------------------------------------------------------------------------
echo "[17] mixed_320_v0"
mkdir -p "$OUTDIR/17_mixed_320_v0"
count=0
for f in "$SOURCE"/*.flac; do
    base=$(basename "${f%.flac}.mp3")
    if [ $count -lt 4 ]; then
        ffmpeg -y -i "$f" -codec:a libmp3lame -b:a 320k "$OUTDIR/17_mixed_320_v0/$base" 2>/dev/null
    else
        ffmpeg -y -i "$f" -codec:a libmp3lame -q:a 0 "$OUTDIR/17_mixed_320_v0/$base" 2>/dev/null
    fi
    count=$((count + 1))
    [ $count -ge $MAX_TRACKS ] && break
done

# ---------------------------------------------------------------------------
# 18. GENUINE V0 LOW BITRATE — quiet/simple tracks produce lower V0 bitrates
# Use only the quietest/simplest tracks (simulate lo-fi)
# ---------------------------------------------------------------------------
echo "[18] genuine_v0_quiet"
mkdir -p "$OUTDIR/18_genuine_v0_quiet"
count=0
for f in "$SOURCE"/*.flac; do
    base=$(basename "${f%.flac}.mp3")
    # Reduce volume to -20dB to simulate quiet recording → lower V0 bitrate
    ffmpeg -y -i "$f" -af "volume=-20dB" -codec:a libmp3lame -q:a 0 "$OUTDIR/18_genuine_v0_quiet/$base" 2>/dev/null
    count=$((count + 1))
    [ $count -ge $MAX_TRACKS ] && break
done

# ---------------------------------------------------------------------------
# 19. FAKE FLAC V0 — fake FLAC encoded to V0 by LAME (transcode → V0)
#     128k source → FLAC wrapper → V0 should produce low bitrate ~150-180kbps
# ---------------------------------------------------------------------------
echo "[19] v0_from_fake_flac_128"
copy_flacs "$OUTDIR/19_v0_from_fake_flac_128"
# Degrade: FLAC → 128k MP3 → FLAC (fake), then encode to V0
for f in "$OUTDIR/19_v0_from_fake_flac_128"/*.flac; do
    mp3="${f%.flac}_temp.mp3"
    ffmpeg -y -i "$f" -codec:a libmp3lame -b:a 128k "$mp3" 2>/dev/null
    rm "$f"
    # Re-wrap as FLAC
    ffmpeg -y -i "$mp3" "${f}" 2>/dev/null
    rm "$mp3"
    # Now convert fake FLAC → V0 (simulating what import_one.py does)
    v0="${f%.flac}.mp3"
    ffmpeg -y -i "$f" -codec:a libmp3lame -q:a 0 -map_metadata 0 -id3v2_version 3 "$v0" 2>/dev/null
    rm "$f"
done

# ---------------------------------------------------------------------------
# 20. DOUBLE TRANSCODE — 128k → FLAC → 128k (extra degraded)
# ---------------------------------------------------------------------------
echo "[20] double_transcode"
copy_flacs "$OUTDIR/20_double_transcode"
# First generation: FLAC → 128k
encode_mp3 "$OUTDIR/20_double_transcode" -b:a 128k
# Second generation: 128k MP3 → FLAC → 128k MP3
for f in "$OUTDIR/20_double_transcode"/*.mp3; do
    flac="${f%.mp3}_temp.flac"
    ffmpeg -y -i "$f" "$flac" 2>/dev/null
    rm "$f"
    ffmpeg -y -i "$flac" -codec:a libmp3lame -b:a 128k "${flac%_temp.flac}.mp3" 2>/dev/null
    rm "$flac"
done

# ---------------------------------------------------------------------------
# 21. VBR V0 WITH ONE BAD TRACK — 7 genuine V0 + 1 track encoded from 128k source
# ---------------------------------------------------------------------------
echo "[21] v0_one_bad_track"
mkdir -p "$OUTDIR/21_v0_one_bad_track"
count=0
for f in "$SOURCE"/*.flac; do
    base=$(basename "${f%.flac}.mp3")
    if [ $count -lt 7 ]; then
        ffmpeg -y -i "$f" -codec:a libmp3lame -q:a 0 "$OUTDIR/21_v0_one_bad_track/$base" 2>/dev/null
    else
        # Degrade to 128k first, then V0 — will produce ~150kbps V0
        temp_mp3="/tmp/spectral_test_temp_128.mp3"
        ffmpeg -y -i "$f" -codec:a libmp3lame -b:a 128k "$temp_mp3" 2>/dev/null
        temp_flac="/tmp/spectral_test_temp.flac"
        ffmpeg -y -i "$temp_mp3" "$temp_flac" 2>/dev/null
        ffmpeg -y -i "$temp_flac" -codec:a libmp3lame -q:a 0 "$OUTDIR/21_v0_one_bad_track/$base" 2>/dev/null
        rm -f "$temp_mp3" "$temp_flac"
    fi
    count=$((count + 1))
    [ $count -ge $MAX_TRACKS ] && break
done

# ---------------------------------------------------------------------------
# 22. GENUINE FLAC CONVERTED TO V0 — simulates the gold standard pipeline output
#     (what import_one.py produces from genuine FLAC)
# ---------------------------------------------------------------------------
echo "[22] gold_standard_v0"
copy_flacs "$OUTDIR/22_gold_standard_v0"
for f in "$OUTDIR/22_gold_standard_v0"/*.flac; do
    mp3="${f%.flac}.mp3"
    ffmpeg -y -i "$f" -codec:a libmp3lame -q:a 0 -map_metadata 0 -id3v2_version 3 "$mp3" 2>/dev/null
    rm "$f"
done

echo ""
echo "=== Done: $(ls -d "$OUTDIR"/*/ | wc -l) test albums generated ==="
echo ""
du -sh "$OUTDIR"
echo ""
ls -1d "$OUTDIR"/*/
