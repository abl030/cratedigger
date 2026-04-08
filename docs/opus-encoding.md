# Opus Encoding Settings

**Decision (2026-04-08): Keep current settings. No changes needed.**

```
ffmpeg -i <src> -c:a libopus -b:a 128k -map_metadata 0 -y <out>.opus
```

## Why 128kbps VBR

- Opus 128kbps matches or exceeds LAME V0 (~245kbps) at roughly half the bitrate
- Kamedo2 2014 listening test (38 participants, 40 tracks, ABC/HR): Opus at 96k target (107k actual average) scored 4.65/5.0. At 128k, it's at the transparency ceiling — additional bits yield no audible return
- Jean-Marc Valin (Opus lead developer): near-transparency at 128kbps requires ability to burst to 256kbps on difficult passages — our unconstrained VBR allows this
- Hydrogenaudio consensus places Opus 128kbps equivalent to LAME V2-V0 depending on material

## Why not lower (96kbps)?

- 96k scores 4.65/5.0 — excellent but not at the ceiling
- "Opus killer sample" threads exist at 128k and above; 96k is more exposed
- Savings are ~30% of an already tiny file — not worth the risk on a curated collection

## VBR mode: unconstrained (default)

ffmpeg defaults to unconstrained VBR for libopus (`OPUS_SET_VBR(1)`, `OPUS_SET_VBR_CONSTRAINT(0)`). This is better than the library's own default (constrained). No burst ceiling — the encoder freely allocates 256kbps+ to transients while averaging 128k.

Tracks averaging 95kbps are normal and correct. The encoder uses fewer bits on sparse/quiet passages and bursts on complex ones. If you can't ABX them against the FLAC source, the bitrate is doing its job.

| ffmpeg `-vbr` | Mode | Burst limit |
|---|---|---|
| `off` | Hard CBR | Fixed at target |
| `on` **(default)** | Unconstrained VBR | None |
| `constrained` | Constrained VBR | ~1 frame buffering |

## Frame duration: 20ms (default) is optimal

**Do not use `-frame_duration 60`.** Every authoritative source says frames above 20ms reduce quality for music.

At 128kbps stereo fullband, Opus runs in CELT-only mode, which natively supports 2.5/5/10/20ms. Setting 60ms doesn't give a 60ms MDCT window — it packs three 20ms CELT frames into one packet, reducing the encoder's adaptability with no benefit.

The 40/60ms durations exist for low-bitrate speech over RTP where fewer packets save UDP/IP header overhead. In Ogg containers (stored files), there's no per-frame overhead to save.

### Sources

- **Xiph Wiki (Recommended Settings):** "Unless operating at very low bitrates over RTP, there is no reason to use frame sizes above 20ms, as those will have slightly lower quality for music encoding."
- **Xiph Wiki (FAQ):** "For file encoding, using a frame size larger than 20ms will usually result in worse quality for the same bitrate because it constrains the encoder in the decisions it can make."
- **Jean-Marc Valin**, [xiph/opus#32](https://github.com/xiph/opus/issues/32): "Unless you're using RTP and the related header overhead, you're not gaining anything from using frame sizes larger than 20ms."
- **Ralph Giles**, [xiph/opus#42](https://github.com/xiph/opus/issues/42): "Larger frame sizes reduce quality for a given codec bitrate... limit the encoder's ability to switch modes in response to signal changes."
- **ffmpeg docs:** "Sizes greater than 20ms are only interesting at fairly low bitrates."

## Compatibility

Not a concern for this pipeline. All playback devices are Opus-native. Plex transcoding handles edge cases (never encountered). Gapless playback is automatic — Ogg container precisely defines audio start/end via pre-skip field (unlike MP3's fragile LAME Xing header).

## References

### Listening tests and quality evidence
- **Kamedo2 2014 multiformat test** — 38 participants, 40 tracks, ABC/HR, bootstrap analysis (1M permutations). Opus 1.1 at 96kbps target (107k actual) scored 4.65/5.0 vs MP3 LAME V5 at 136kbps scoring 4.24 (p=0.000)
- **LAME documentation** — V0-V3 "will normally produce transparent results"; no ABX evidence that perceived quality improves above V0
- **LAME Bug #506** (2019-2020) — confirmed psychoacoustic regression in LAME 3.100, specific test signal produced 10/10 correct ABX vs V0

### Opus developer statements
- **Jean-Marc Valin** on frame duration, [xiph/opus#32](https://github.com/xiph/opus/issues/32): "Unless you're using RTP... you're not gaining anything from using frame sizes larger than 20ms"
- **Jean-Marc Valin** on VBR bursting: near-transparency at 128kbps average requires encoding some short segments at up to 256kbps
- **Ralph Giles** on frame duration, [xiph/opus#42](https://github.com/xiph/opus/issues/42): "Larger frame sizes reduce quality for a given codec bitrate"

### Standards and documentation
- [RFC 6716](https://www.rfc-editor.org/rfc/rfc6716) — Opus codec specification (CELT frame sizes in Section 3.1, efficiency vs frame size in Section 2.1.4)
- [Xiph Wiki — Opus Recommended Settings](https://wiki.xiph.org/Opus_Recommended_Settings) — frame size, bitrate, and application type guidance
- [Xiph Wiki — Opus FAQ](https://wiki.xiph.org/OpusFAQ) — frame size impact on file encoding quality
- [ffmpeg codecs documentation (libopus)](https://ffmpeg.org/ffmpeg-codecs.html) — VBR modes, frame_duration, compression_level
- [ffmpeg libopusenc.c source](https://github.com/FFmpeg/FFmpeg/blob/master/libavcodec/libopusenc.c) — VBR default mapping (unconstrained)
- [libopus Encoder CTLs API](https://opus-codec.org/docs/opus_api-1.3.1/group__opus__encoderctls.html) — OPUS_SET_VBR, OPUS_SET_VBR_CONSTRAINT, OPUS_SET_EXPERT_FRAME_DURATION
- [opusenc source](https://github.com/xiph/opus-tools/blob/master/src/opusenc.c) — opus-tools frame duration handling
- [Codec Wiki — Opus](https://wiki.x266.mov/docs/audio/Opus) — frame size adaptability note
- [Hydrogenaudio Wiki — Opus](https://wiki.hydrogenaudio.org/index.php?title=Opus) — frame size efficiency costs

### Codec architecture (from the artifact report)
- CELT single-stage MDCT: up to 960 coefficients per 20ms frame at 48kHz (~25Hz resolution vs MP3's ~42Hz)
- Pyramid Vector Quantization (PVQ) for spectral shape, range coder (not Huffman)
- Energy preservation: every band maintains total energy regardless of bit budget (no spectral holes)
- Adaptive time-frequency resolution: up to 8 short blocks (2.5ms) with per-band Walsh-Hadamard transforms
- 2.5ms minimum block size vs MP3's 4.4ms short blocks for transient handling
- Spectral folding fills zero-bit bands from lower frequencies (preserves temporal envelope)

## Known Opus limitations

- Embedded cover art from FLAC does not transfer to Opus via ffmpeg (Ogg containers don't support attached pictures). Beets' fetchart plugin re-fetches art from Cover Art Archive during import.
- R128 vs ReplayGain: Opus mandates EBU R128 (-23 LUFS) vs ReplayGain's -18 LUFS — a 5dB offset. Not relevant since we don't mix MP3 and Opus in the same library view.

## Architecture reference

Opus is one of several configurable target formats via `verified_lossless_target` in `config.ini`. Conversion runs in `harness/import_one.py`:
1. `conversion_target()` — decides target format based on target_format, verified_lossless, verified_lossless_target config
2. `convert_lossless(path, spec)` — single conversion function parameterized by `ConversionSpec`
3. `parse_verified_lossless_target("opus 128")` — parses config string into `ConversionSpec`
4. Only runs on verified lossless (genuine FLAC confirmed by spectral analysis + V0 verification)
5. Transcodes detected by spectral analysis are not converted — target is skipped, V0 fallback used
