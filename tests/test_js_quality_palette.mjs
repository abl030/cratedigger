/** Shared quality/spectral palette contract tests. */

import fs from 'node:fs';
import {
  QUALITY_RANK_ORDER,
  SPECTRAL_GRADE_ORDER,
  SPECTRAL_GRADE_TONES,
  qualityRankBadgeClass,
  qualityToneClass,
  spectralGradeBadgeClass,
  spectralGradeClass,
  spectralGradeLabel,
  spectralGradeTone,
} from '../web/js/quality_palette.js';

let passed = 0;
let failed = 0;

function assert(condition, message) {
  if (condition) passed++;
  else {
    failed++;
    console.error(`  FAIL: ${message}`);
  }
}

function assertEqual(actual, expected, message) {
  assert(actual === expected,
    `${message} — expected ${JSON.stringify(expected)}, got ${JSON.stringify(actual)}`);
}

const expectedRanks = [
  'unknown', 'poor', 'acceptable', 'good', 'excellent', 'transparent', 'lossless',
];
const expectedSpectral = [
  'likely_transcode', 'suspect', 'marginal', 'genuine',
];

console.log('quality bucket palette spans unknown through bright-green lossless');
assertEqual(JSON.stringify(QUALITY_RANK_ORDER), JSON.stringify(expectedRanks),
  'quality ranks stay ordered from least evidence/quality to best');
for (const rank of expectedRanks) {
  assertEqual(qualityToneClass(rank), `quality-tone-${rank}`,
    `${rank} gets its canonical text-tone class`);
  assertEqual(qualityRankBadgeClass(rank), `badge-rank-${rank}`,
    `${rank} gets its canonical badge class`);
}
assertEqual(qualityToneClass('future_rank'), 'quality-tone-unknown',
  'unknown rank tokens fail closed to the neutral tone');
assertEqual(qualityRankBadgeClass('future_rank'), 'badge-rank-unknown',
  'unknown rank badge tokens fail closed to the neutral badge');

console.log('spectral grades reuse ordered bucket colours from red to bright green');
assertEqual(JSON.stringify(SPECTRAL_GRADE_ORDER), JSON.stringify(expectedSpectral),
  'spectral grades stay ordered from likely transcode to genuine');
assertEqual(SPECTRAL_GRADE_TONES.likely_transcode, 'poor',
  'likely transcode reuses the red poor tone');
assertEqual(SPECTRAL_GRADE_TONES.suspect, 'acceptable',
  'suspect reuses the orange acceptable tone');
assertEqual(SPECTRAL_GRADE_TONES.marginal, 'good',
  'marginal reuses the yellow good tone');
assertEqual(SPECTRAL_GRADE_TONES.genuine, 'lossless',
  'genuine reuses the bright-green lossless tone');

for (const grade of expectedSpectral) {
  const tone = SPECTRAL_GRADE_TONES[grade];
  assertEqual(spectralGradeTone(grade), tone, `${grade} resolves to ${tone}`);
  assertEqual(spectralGradeClass(grade), `spectral-grade quality-tone-${tone}`,
    `${grade} inline text uses the shared ${tone} colour`);
  assertEqual(spectralGradeBadgeClass(grade),
    `badge spectral-grade badge-rank-${tone}`,
    `${grade} badges use the shared ${tone} foreground and background`);
}
assertEqual(spectralGradeTone('future_grade'), 'unknown',
  'unknown spectral tokens fail closed to neutral');
assertEqual(spectralGradeTone('constructor'), 'unknown',
  'inherited object keys cannot escape the neutral fallback');
assertEqual(spectralGradeLabel('likely_transcode'), 'likely transcode',
  'spectral tokens are humanized consistently');

console.log('generated spectral ordering property covers every ordered grade pair');
const rankIndex = new Map(QUALITY_RANK_ORDER.map((rank, index) => [rank, index]));
for (let low = 0; low < SPECTRAL_GRADE_ORDER.length; low++) {
  for (let high = low + 1; high < SPECTRAL_GRADE_ORDER.length; high++) {
    const lowGrade = SPECTRAL_GRADE_ORDER[low];
    const highGrade = SPECTRAL_GRADE_ORDER[high];
    assert(rankIndex.get(spectralGradeTone(lowGrade))
      < rankIndex.get(spectralGradeTone(highGrade)),
    `${lowGrade} stays visually below ${highGrade}`);
  }
}

console.log('known-bad swapped spectral mapping violates the ordering property');
const badTones = { ...SPECTRAL_GRADE_TONES, likely_transcode: 'good', suspect: 'poor' };
const badOrdered = SPECTRAL_GRADE_ORDER.every((grade, index) => index === 0
  || rankIndex.get(badTones[SPECTRAL_GRADE_ORDER[index - 1]])
    < rankIndex.get(badTones[grade]));
assert(!badOrdered, 'the checker rejects a likely-transcode/suspect colour reversal');

console.log('CSS owns one red-to-green palette for text and badges');
const css = fs.readFileSync(new URL('../web/index.html', import.meta.url), 'utf8');
for (const [rank, foreground] of Object.entries({
  unknown: '#888',
  poor: '#d66',
  acceptable: '#d96',
  good: '#ec6',
  excellent: '#cd6',
  transparent: '#6d6',
  lossless: '#8f8',
})) {
  assert(css.includes(`--quality-${rank}-fg: ${foreground};`),
    `${rank} foreground is declared once as ${foreground}`);
  assert(css.includes(`.quality-tone-${rank} { color: var(--quality-${rank}-fg); }`),
    `${rank} inline text consumes its shared foreground token`);
  assert(css.includes(`.badge-rank-${rank} { background: var(--quality-${rank}-bg); color: var(--quality-${rank}-fg); }`),
    `${rank} badges consume both shared palette tokens`);
}
assert(css.includes('.badge-quality-outline { border: 1px solid currentColor; }'),
  'quality badge outlines preserve their canonical rank background');

console.log(`\n${passed} passed, ${failed} failed`);
if (failed > 0) process.exit(1);
