/**
 * Unit tests for web/js/util.js — pure utility functions.
 * Run with: node tests/test_js_util.mjs
 */

import { qualityLabel, toAWST, awstDate, awstTime, awstDateTime, esc, overrideToIntent, detectSource, externalReleaseUrl, sourceLabel } from '../web/js/util.js';

let passed = 0;
let failed = 0;

function assert(condition, msg) {
  if (condition) {
    passed++;
  } else {
    failed++;
    console.error(`  FAIL: ${msg}`);
  }
}

function assertEqual(actual, expected, msg) {
  if (actual === expected) {
    passed++;
  } else {
    failed++;
    console.error(`  FAIL: ${msg} — expected ${JSON.stringify(expected)}, got ${JSON.stringify(actual)}`);
  }
}

// --- qualityLabel tests ---
console.log('qualityLabel()');
assertEqual(qualityLabel('FLAC', 1000), 'FLAC', 'FLAC ignores bitrate');
assertEqual(qualityLabel('ALAC', 800), 'ALAC', 'ALAC ignores bitrate');
assertEqual(qualityLabel('MP3', 320), 'MP3 320', 'MP3 320kbps');
assertEqual(qualityLabel('MP3', 295), 'MP3 320', 'MP3 295 rounds to 320');
assertEqual(qualityLabel('MP3', 245), 'MP3 V0', 'MP3 245 = V0');
assertEqual(qualityLabel('MP3', 220), 'MP3 V0', 'MP3 220 = V0');
assertEqual(qualityLabel('MP3', 190), 'MP3 V2', 'MP3 190 = V2');
assertEqual(qualityLabel('MP3', 170), 'MP3 V2', 'MP3 170 = V2');
assertEqual(qualityLabel('MP3', 128), 'MP3 128k', 'MP3 128 shows raw');
assertEqual(qualityLabel('MP3', 0), 'MP3', 'MP3 0 bitrate = just format');
assertEqual(qualityLabel('MP3', null), 'MP3', 'MP3 null bitrate = just format');
assertEqual(qualityLabel(null, 320), '?', 'null format = ?');
assertEqual(qualityLabel('', 320), '?', 'empty format = ?');
assertEqual(qualityLabel('MP3,FLAC', 250), 'MP3 V0', 'comma-separated uses first');

// --- toAWST tests ---
console.log('toAWST()');
// UTC midnight = 8am AWST
assertEqual(toAWST('2026-04-01T00:00:00Z'), '2026-04-01T08:00:00', 'UTC midnight = 08:00 AWST');
assertEqual(toAWST('2026-04-01T16:00:00Z'), '2026-04-02T00:00:00', 'UTC 16:00 = next day 00:00 AWST');
assertEqual(toAWST('2026-12-31T20:00:00Z'), '2027-01-01T04:00:00', 'year boundary');

// --- awstDate tests ---
console.log('awstDate()');
assertEqual(awstDate('2026-04-01T00:00:00Z'), '2026-04-01', 'date from UTC midnight');

// --- awstTime tests ---
console.log('awstTime()');
assertEqual(awstTime('2026-04-01T00:00:00Z'), '08:00', 'time from UTC midnight');

// --- awstDateTime tests ---
console.log('awstDateTime()');
assertEqual(awstDateTime('2026-04-01T00:00:00Z'), '2026-04-01 08:00', 'datetime from UTC midnight');

// --- esc tests ---
console.log('esc()');
assertEqual(esc('hello'), 'hello', 'plain text unchanged');
assertEqual(esc('<script>alert(1)</script>'), '&lt;script&gt;alert(1)&lt;/script&gt;', 'escapes HTML tags');
assertEqual(esc('a & b'), 'a &amp; b', 'escapes ampersand');
assertEqual(esc('"quotes"'), '&quot;quotes&quot;', 'escapes double quotes');
assertEqual(esc("Guns N' Roses"), 'Guns N&#39; Roses', 'escapes single quotes');
assertEqual(esc('back\\slash'), 'back&#92;slash', 'escapes backslashes');
assertEqual(esc("it\\'s"), 'it&#92;&#39;s', 'escapes backslash+quote combo');
assertEqual(esc(''), '', 'empty string');
assertEqual(esc(null), '', 'null returns empty');
assertEqual(esc(undefined), '', 'undefined returns empty');

// --- overrideToIntent tests ---
console.log('overrideToIntent()');
assertEqual(overrideToIntent(null), 'default', 'null → default');
assertEqual(overrideToIntent(undefined), 'default', 'undefined → default');
assertEqual(overrideToIntent(''), 'default', 'empty string → default');
assertEqual(overrideToIntent('lossless'), 'lossless', '"lossless" → lossless');
assertEqual(overrideToIntent('flac'), 'lossless', '"flac" (backward compat) → lossless');
assertEqual(overrideToIntent('flac,mp3 v0,mp3 320'), 'default', 'CSV → default');
assertEqual(overrideToIntent('unknown'), 'default', 'unknown → default');

// --- detectSource tests ---
console.log('detectSource()');
assertEqual(detectSource('89ad4ac3-39f7-470e-963a-56509c546377'), 'musicbrainz', 'UUID → musicbrainz');
assertEqual(detectSource('2048516'), 'discogs', 'numeric → discogs');
assertEqual(detectSource(''), 'unknown', 'empty → unknown');
assertEqual(detectSource(null), 'unknown', 'null → unknown');
assertEqual(detectSource(undefined), 'unknown', 'undefined → unknown');
assertEqual(detectSource('NONE'), 'unknown', 'NONE → unknown');

// --- externalReleaseUrl tests ---
console.log('externalReleaseUrl()');
assertEqual(
  externalReleaseUrl('89ad4ac3-39f7-470e-963a-56509c546377'),
  'https://musicbrainz.org/release/89ad4ac3-39f7-470e-963a-56509c546377',
  'MB UUID → musicbrainz.org'
);
assertEqual(
  externalReleaseUrl('2048516'),
  'https://www.discogs.com/release/2048516',
  'Discogs numeric → discogs.com'
);

// --- sourceLabel tests ---
console.log('sourceLabel()');
assertEqual(sourceLabel('89ad4ac3-39f7-470e-963a-56509c546377'), 'MusicBrainz', 'UUID → MusicBrainz');
assertEqual(sourceLabel('2048516'), 'Discogs', 'numeric → Discogs');

// --- Summary ---
console.log(`\n${passed} passed, ${failed} failed`);
if (failed > 0) process.exit(1);
