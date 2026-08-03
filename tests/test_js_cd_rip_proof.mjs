/** Unit tests for the positive-only CD-rip proof presentation. */

import { cdRipProofPresentation } from '../web/js/cd_rip_proof.js';
import {
  validAccurateRipProof,
  validCtdbProof,
} from './fixtures/cd_rip_proof.mjs';

let passed = 0;
let failed = 0;

function assertEqual(actual, expected, message) {
  if (actual === expected) passed++;
  else {
    failed++;
    console.error(`  FAIL: ${message} — expected '${expected}', got '${actual}'`);
  }
}

console.log('cdRipProofPresentation() renders CTDB confidence');
{
  assertEqual(
    cdRipProofPresentation(validCtdbProof())?.text,
    'CD bit-verified · CTDB confidence 24',
    'CTDB proof names its positive whole-disc confidence',
  );
}

console.log('cdRipProofPresentation() renders conservative AccurateRip confidence');
{
  assertEqual(
    cdRipProofPresentation(validAccurateRipProof())?.text,
    'CD bit-verified · AccurateRip min confidence 1',
    'AccurateRip proof reports the minimum across every track',
  );
}

console.log('cdRipProofPresentation() is positive-only and fail-closed');
{
  const partial = {
    algorithm: 'cd-rip-bit-verifier-v1',
    ctdb: { provider: 'ctdb', confidence: 24 },
  };
  const malformed = validAccurateRipProof();
  malformed.accuraterip.track_confidences = [5, 0, 8];

  assertEqual(cdRipProofPresentation(null), null,
    'absent evidence has no negative presentation');
  assertEqual(cdRipProofPresentation(partial), null,
    'a partial provider shape cannot mint a proof label');
  assertEqual(cdRipProofPresentation(malformed), null,
    'a non-positive track confidence cannot mint a proof label');
}

console.log(`\n${passed} passed, ${failed} failed`);
if (failed > 0) process.exit(1);
