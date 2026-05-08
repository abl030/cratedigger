---
title: "subprocess text mode decodes captured streams as UTF-8 strict"
date: 2026-05-08
category: runtime-errors
problem_type: runtime_error
component: harness
severity: high
tags:
  - subprocess
  - encoding
  - ffmpeg
  - sox
  - flac
  - vorbis-comments
  - defense-in-depth
related_pr: https://github.com/abl030/cratedigger/pull/232
related_files:
  - harness/import_one.py
  - lib/spectral_check.py
  - lib/util.py
  - lib/import_dispatch.py
  - lib/beets.py
  - lib/beets_album_op.py
  - lib/audio_hash.py
---

# subprocess text mode decodes captured streams as UTF-8 strict

## Context

Live trigger: request 580 (78 Saab — *Crossed Lines*), download from
Soulseek peer `trelospatrinos`, 2026-05-08 14:23 UTC. The album was
already in beets at 320 kbps CBR; the pipeline was searching for a
lossless upgrade. trelospatrinos's FLAC files passed validation, audio
integrity, and spectral analysis, then crashed at the `[CONVERT]` step
with:

```
decision:  crash
exit_code: 99
error:     UnicodeDecodeError: 'utf-8' codec can't decode byte 0xe2 in
           position 32388: invalid continuation byte
```

The request bounced back to `wanted` (status reset, no denylist applied
because the peer wasn't at fault), so the same crash could repeat
indefinitely on every cycle.

## Guidance

When running an external binary that operates on user-supplied data
(audio files, files with arbitrary metadata, anything tagged in an
unknown encoding), **always pair `text=True` with `errors="replace"`**
on `subprocess.run` and `subprocess.Popen`:

```python
# Correct — bad bytes become U+FFFD instead of crashing the caller.
result = subprocess.run(
    cmd,
    capture_output=True,
    text=True,
    errors="replace",   # <-- the safety net
    timeout=300,
)

# Same applies to Popen:
proc = subprocess.Popen(
    cmd,
    stdout=subprocess.PIPE, stderr=subprocess.PIPE,
    text=True, errors="replace",
)
```

The fix is mechanical, but the failure mode it prevents is sneaky:
`text=True` defaults to UTF-8 strict, and the decode happens **during
capture inside `subprocess.run` itself**, before the function ever
returns. A `try/except` block around the call that only catches
`subprocess.TimeoutExpired` (the most common pattern) will not catch
the decode error — `UnicodeDecodeError` propagates straight up from
`_translate_newlines()` deep inside `Popen._communicate`.

Prior art lived in this repo at `lib/audio_hash.py:113` — that file
already used `stderr.decode("utf-8", errors="replace")` after capturing
in binary mode. The pattern wasn't generalized to other sites, and
fifteen `subprocess.run(..., text=True, ...)` calls across six files
were left vulnerable for over a year before request 580 surfaced the
problem.

## Why this matters

The decode-on-capture failure mode has three properties that combine to
make it a high-impact, low-detection bug:

1. **It's invisible in tests.** Most unit tests mock `subprocess.run`
   with clean `MagicMock(stdout="", stderr="")` returns. The decoding
   step is never exercised. Even integration tests that use real
   subprocesses usually feed clean inputs that don't trip strict UTF-8.

2. **It's impossible to catch with the obvious try/except.** Code that
   already wraps the call in `try: ... except subprocess.TimeoutExpired:`
   looks defensive, but the protection is only against one specific
   failure mode. `UnicodeDecodeError` is raised from a completely
   different code path inside `Popen._communicate` →
   `_translate_newlines` → `data.decode(encoding, errors)`.

3. **Real-world audio metadata is rarely clean UTF-8.** Soulseek-sourced
   FLAC files are often tagged with non-UTF-8 encodings (CP1252 from
   Windows rippers, Shift-JIS from Japanese sources, raw Latin-1).
   ffmpeg/sox echo Vorbis comment values into stderr in their verbose
   output, so any tagged file with a non-ASCII character can trigger
   the crash. Position 32388 in the live error corresponds to a tag
   value being dumped roughly 32 KB into ffmpeg's stream-info section.

The blast radius is also worse than it looks. A single bad file from
any peer crashes the entire import for that album, returns the request
to `wanted` without recording a real decision, and lets the same peer
be retried on the next cycle — the failure isn't attributed to the
peer (no denylist write), so the loop is permanent until a different
peer is tried or the underlying bug is fixed.

## When to apply

Pair `text=True` with `errors="replace"` whenever **all** of the
following are true:

- The subprocess output is captured (`capture_output=True`,
  `stdout=PIPE`, or `stderr=PIPE`).
- The subprocess operates on data that came from outside our trust
  boundary — user uploads, peer downloads, scraped metadata, or any
  binary's verbose mode that echoes file content.
- The caller decodes the captured streams as text (`text=True` /
  `encoding=...`).

Sites in this repo that should always follow the pattern:

- ffmpeg / ffprobe / sox / flac / mp3val on audio files
- beets harness drivers (beets reads tags via mutagen and re-emits them)
- any subprocess that runs a user-supplied script or processes a
  user-supplied filename

Sites where it's safe to rely on UTF-8 strict:

- Subprocesses that only emit machine-generated output (JSON, CSV,
  numeric stdout) and never echo user data on stderr.
- Subprocesses operating on data we generated ourselves in this
  process (round-tripping our own UTF-8 strings).

When in doubt, add `errors="replace"`. The cost is one keyword
argument; the consumers in this codebase already tolerate U+FFFD
because they parse numeric values, search for known substrings, or
truncate stderr to the last 200 characters for logging.

## Examples

**Crash site (request 580 reproduction):**

```python
# harness/import_one.py — convert_lossless, before
result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
# ↑ Raises UnicodeDecodeError if ffmpeg stderr contains any non-UTF-8 byte.
#   The surrounding `try/except subprocess.TimeoutExpired` does NOT catch it.
```

```python
# harness/import_one.py — convert_lossless, after
result = subprocess.run(
    cmd, capture_output=True, text=True, errors="replace", timeout=300,
)
# ↑ Bad bytes become U+FFFD. ffmpeg returncode and stderr text are still
#   inspected normally; the only difference is that bad bytes survive
#   capture instead of crashing the caller.
```

**RED test that reproduces the failure mode** (see
`tests/test_import_one_stages.py::TestConvertLosslessNonUtf8Stderr`):

```python
def test_convert_lossless_tolerates_non_utf8_ffmpeg_stderr(self):
    """A fake ffmpeg shim emits 0xE2+ASCII to stderr. Pre-fix this raises
    UnicodeDecodeError before subprocess.run returns; post-fix it returns
    cleanly."""
    bin_dir = tmp + "/bin"
    write_shim(bin_dir, "ffmpeg", body=
        'OUT="${@: -1}"\n'
        'printf "metadata: title=caf\\xe2X end\\n" >&2\n'   # bare 0xE2
        'printf "id3" > "$OUT"\n'
        'exit 0\n'
    )
    with override_path(bin_dir):
        converted, failed, ext = convert_lossless(album, V0_SPEC)
    assert (converted, failed, ext) == (1, 0, "flac")
```

The test approach generalizes: drop a `#!/bin/sh` shim that emits the
problematic bytes, prepend its directory to `PATH`, run the function
under test. No mocks, no reliance on real ffmpeg behavior — directly
exercises the `subprocess.run(text=True)` decode path.

**Sites fixed in [PR #232](https://github.com/abl030/cratedigger/pull/232)**
(15 sites across 6 files):

- `harness/import_one.py` — 5× `subprocess.run` (ffprobe, ffmpeg) +
  1× `subprocess.Popen` (beets harness driver)
- `lib/spectral_check.py` — 2× (sox, ffmpeg)
- `lib/util.py` — 4× (mp3val, ffmpeg validate, flac repair, ffmpeg retest)
- `lib/import_dispatch.py` — 1× (import_one subprocess)
- `lib/beets.py` — 1× (harness Popen)
- `lib/beets_album_op.py` — 1× (`beet remove` / `beet move`)
