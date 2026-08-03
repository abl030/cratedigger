/** Producer-serialized lib.quality CdRipBitVerification fixtures. */

import { readFileSync } from 'node:fs';

const fixtures = JSON.parse(readFileSync(
  new URL('./cd_rip_proof.json', import.meta.url),
  'utf8',
));

function cloneFixture(name) {
  const fixture = fixtures[name];
  if (!fixture) throw new Error(`unknown generated CD-rip fixture: ${name}`);
  return structuredClone(fixture);
}

export function validCtdbProof(confidence = 24) {
  return cloneFixture(`ctdb_${confidence}`);
}

export function validAccurateRipProof() {
  return cloneFixture('accuraterip_min_1');
}

export function validDualProviderProof() {
  return cloneFixture('ctdb_11_accuraterip_min_3');
}
