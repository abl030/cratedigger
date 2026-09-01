/** Unit tests for the positive-only CD-rip proof presentation. */

import { cdRipProofPresentation } from '../web/js/cd_rip_proof.js';
import {
  validAccurateRipProof,
  validCtdbProof,
  validDualProviderProof,
} from './fixtures/cd_rip_proof.mjs';

import { suite } from './js_harness.mjs';

const t = suite(import.meta.url);

t.section('cdRipProofPresentation() renders CTDB confidence');
{
  t.equal(
    cdRipProofPresentation(validCtdbProof())?.text,
    'CD bit-verified · CTDB confidence 24',
    'CTDB proof names its positive whole-disc confidence',
  );
}

t.section('cdRipProofPresentation() renders conservative AccurateRip confidence');
{
  t.equal(
    cdRipProofPresentation(validAccurateRipProof())?.text,
    'CD bit-verified · AccurateRip min confidence 1',
    'AccurateRip proof reports the minimum across every track',
  );
}

t.section('cdRipProofPresentation() is positive-only and fail-closed');
{
  const partial = {
    algorithm: 'cd-rip-bit-verifier-v1',
    ctdb: { provider: 'ctdb', confidence: 24 },
  };
  const malformed = validAccurateRipProof();
  malformed.accuraterip.track_confidences = [5, 0, 8];

  t.equal(cdRipProofPresentation(null), null,
    'absent evidence has no negative presentation');
  t.equal(cdRipProofPresentation(partial), null,
    'a partial provider shape cannot mint a proof label');
  t.equal(cdRipProofPresentation(malformed), null,
    'a non-positive track confidence cannot mint a proof label');
}

t.section('cdRipProofPresentation() rejects deterministic known-bad wire mutations');
{
  const topLevelMutations = [
    ['algorithm', (proof) => { proof.algorithm = 'cd-rip-bit-verifier-v0'; }],
    ['provenance', (proof) => { proof.provenance = 'guessed'; }],
    ['source format', (proof) => { proof.source_format = 'mp3'; }],
    ['empty TOC', (proof) => { proof.toc.track_offsets_sectors = []; }],
    ['nonzero first track', (proof) => { proof.toc.track_offsets_sectors[0] = 1; }],
    ['invalid leadout', (proof) => { proof.toc.leadout_sector = 0; }],
  ];
  for (const [name, mutate] of topLevelMutations) {
    for (const makeProof of [validCtdbProof, validAccurateRipProof]) {
      const proof = makeProof();
      mutate(proof);
      t.equal(cdRipProofPresentation(proof), null,
        `${name} mutation fails closed for each provider`);
    }
  }

  const providerMutations = [
    ['CTDB provider tag', validCtdbProof,
      (proof) => { proof.ctdb.provider = 'accuraterip'; }],
    ['CTDB URL', validCtdbProof,
      (proof) => { proof.ctdb.url = 'http://db.cue.tools/lookup2.php'; }],
    ['CTDB SHA', validCtdbProof,
      (proof) => { proof.ctdb.response_sha256 = 'not-a-sha'; }],
    ['CTDB response TOC', validCtdbProof,
      (proof) => { proof.ctdb.response_toc_sectors[1] += 1; }],
    ['AccurateRip provider tag', validAccurateRipProof,
      (proof) => { proof.accuraterip.provider = 'ctdb'; }],
    ['AccurateRip URL', validAccurateRipProof,
      (proof) => { proof.accuraterip.url = 'http://accuraterip.invalid'; }],
    ['AccurateRip SHA', validAccurateRipProof,
      (proof) => { proof.accuraterip.response_sha256 = 'A'.repeat(64); }],
    ['confidence cardinality', validAccurateRipProof,
      (proof) => { proof.accuraterip.track_confidences.pop(); }],
    ['checksum cardinality', validAccurateRipProof,
      (proof) => { proof.accuraterip.track_checksums.pop(); }],
  ];
  for (const [name, makeProof, mutate] of providerMutations) {
    const proof = makeProof();
    mutate(proof);
    t.equal(cdRipProofPresentation(proof), null,
      `${name} mutation cannot mint a label`);
  }

  const dualProvider = validDualProviderProof();
  t.equal(
    cdRipProofPresentation(dualProvider)?.text,
    'CD bit-verified · CTDB confidence 11 + AccurateRip min confidence 3',
    'two complete positive providers retain both conservative confidences',
  );
}

t.done();
