/**
 * Unit tests for web/js/replace_offer.js — the inverted Replace button's
 * offer decision (issue #1366 part 2). One pin per reason, the honesty
 * rules for the tooltip, and the precedence between the own key and the
 * paired key. The generated patrol over the whole input space lives in
 * tests/test_replace_offer_generated.py.
 *
 * Run with: node tests/test_js_replace_offer.mjs
 */

import {
  NO_REQUEST_TITLE,
  REPLACE_OFFER_REASONS,
  pairNoun,
  replaceOfferState,
} from '../web/js/replace_offer.js';

import { suite } from './js_harness.mjs';

const t = suite(import.meta.url);

const MASTER = { id: '11052', kind: 'work', source: 'discogs', label: 'Absolution' };
const RELEASE = { id: '3938744', kind: 'release', source: 'discogs', label: 'Fraulein' };
const RG = { id: '6f151223-f3a3-3e57-810f-598f7897006c', kind: 'work', source: 'mb', label: 'Absolution' };

function input(overrides) {
  return {
    ownKey: 'rg-own',
    ownActive: false,
    lookupFailed: false,
    pairing: 'checked',
    pair: null,
    pairActive: false,
    rowSource: 'mb',
    ...overrides,
  };
}

t.section('enabled — the row\'s own key holds an active request');
{
  const offer = replaceOfferState(input({ ownActive: true, pair: MASTER, pairActive: true }));
  t.equal(offer.enabled, true, 'own key wins');
  t.equal(offer.reason, 'own', 'reason is own even when the pair is active too');
  t.equal(offer.title, '', 'an own-key offer carries no tooltip');
}

t.section('enabled — only the paired key holds an active request');
{
  const offer = replaceOfferState(input({ pair: MASTER, pairActive: true }));
  t.equal(offer.enabled, true, 'a request on the other pathway is an offer');
  t.equal(offer.reason, 'paired', 'reason is paired');
  t.contains(offer.title, 'other pathway', 'tooltip says the request is on the other pathway');
  t.contains(offer.title, 'Discogs master "Absolution"', 'tooltip names the paired master');
  const viaRelease = replaceOfferState(input({ pair: RELEASE, pairActive: true }));
  t.contains(viaRelease.title, 'Discogs release "Fraulein"', 'a masterless pair is named as a release');
  const viaRg = replaceOfferState(input({ rowSource: 'discogs', ownKey: '11052', pair: RG, pairActive: true }));
  t.contains(viaRg.title, 'MusicBrainz release group "Absolution"', 'an MB pair is named as a release group');
}

t.section('enabled — a masterless Discogs row (no own key) can still be enabled through its pair');
{
  const offer = replaceOfferState(input({ rowSource: 'discogs', ownKey: null, pair: RG, pairActive: true }));
  t.equal(offer.enabled, true, 'masterless row lights up through the paired MB release group');
  t.equal(offer.reason, 'paired', 'reason is paired');
}

t.section('ownActive is ignored without an own key; pairActive is ignored without a pair');
{
  const noKey = replaceOfferState(input({ ownKey: null, ownActive: true, pairing: 'checked' }));
  t.equal(noKey.enabled, false, 'a stray ownActive without a key never enables');
  t.equal(noKey.reason, 'masterless_no_pair', 'and the explanation is the masterless one');
  const noPair = replaceOfferState(input({ pair: null, pairActive: true }));
  t.equal(noPair.enabled, false, 'a stray pairActive without a pair never enables');
  t.equal(noPair.reason, 'no_pair', 'and the explanation is no pair');
}

t.section('disabled — the lookup failed and there was something to look up');
{
  const own = replaceOfferState(input({ lookupFailed: true }));
  t.equal(own.reason, 'lookup_unavailable', 'a failed lookup with an own key is unavailable');
  t.contains(own.title, 'Could not check', 'tooltip says could not check');
  t.excludes(own.title, 'No existing request', 'a failed lookup is never described as confirmed absence');
  const paired = replaceOfferState(input({ lookupFailed: true, pair: MASTER }));
  t.contains(paired.title, 'or its paired Discogs master', 'with a pair, the tooltip names the paired group as also unchecked');
  const masterlessPaired = replaceOfferState(input({ rowSource: 'discogs', ownKey: null, lookupFailed: true, pair: RG }));
  t.equal(masterlessPaired.reason, 'lookup_unavailable', 'a masterless row WITH a pair had a key to check, so a failed lookup is unavailable');
}

t.section('disabled — the lookup failed but nothing could have been looked up');
{
  const offer = replaceOfferState(input({ rowSource: 'discogs', ownKey: null, lookupFailed: true, pair: null }));
  t.equal(offer.reason, 'masterless_no_pair', 'no key on either side: the failed lookup was irrelevant, not unavailable');
  const unchecked = replaceOfferState(input({ rowSource: 'discogs', ownKey: null, lookupFailed: true, pair: null, pairing: 'pending' }));
  t.equal(unchecked.reason, 'pairing_unchecked', 'with the pairing unchecked, THAT is the honest explanation');
}

t.section('disabled — the pairing could not be checked');
{
  const own = replaceOfferState(input({ pairing: 'pending' }));
  t.equal(own.reason, 'pairing_unchecked', 'reason');
  t.contains(own.title, 'could not be checked', 'tooltip says the pairing could not be checked');
  t.contains(own.title, 'No existing request in this release group;', 'the own-group absence IS confirmed and said');
  const masterless = replaceOfferState(input({ rowSource: 'discogs', ownKey: null, pairing: 'pending' }));
  t.contains(masterless.title, 'This release has no master;', 'a masterless row says so instead of naming a group');
}

t.section('disabled — a surface with no compare keeps the pre-#1366 own-group copy and never mentions pairing');
{
  const own = replaceOfferState(input({ pairing: 'none' }));
  t.equal(own.reason, 'no_request', 'own key inactive, nothing else to consult');
  t.equal(own.title, NO_REQUEST_TITLE, 'the tooltip is exactly the pre-#1366 copy');
  t.equal(own.title, 'No existing request in this release group', 'spelled out, so a copy edit here is a deliberate one');
  const masterless = replaceOfferState(input({ rowSource: 'discogs', ownKey: null, pairing: 'none' }));
  t.equal(masterless.reason, 'masterless', 'a masterless row on such a surface gets its own reason');
  t.equal(masterless.title, NO_REQUEST_TITLE, 'and the same pre-#1366 copy');
  for (const offer of [own, masterless]) {
    t.excludes(offer.title.toLowerCase(), 'pair', 'no pairing is mentioned where none exists');
    t.excludes(offer.title, 'other pathway', 'nor the other pathway');
  }
  const failed = replaceOfferState(input({ pairing: 'none', lookupFailed: true }));
  t.equal(failed.reason, 'lookup_unavailable', 'a failed lookup still wins over plain absence');
  t.equal(failed.title, 'Could not check for an existing request in this release group. Collapse and re-expand to retry.',
    'with the pre-#1366 scope');
  const ignoredPair = replaceOfferState(input({ pairing: 'none', pair: MASTER, pairActive: true }));
  t.equal(ignoredPair.enabled, false, 'a pair handed to a surface that never ran the compare is not believed');
  t.equal(ignoredPair.reason, 'no_request', 'and does not change the explanation');
}

t.section('disabled — pairing checked, no pair found');
{
  const mb = replaceOfferState(input({ rowSource: 'mb' }));
  t.equal(mb.reason, 'no_pair', 'reason');
  t.contains(mb.title, 'no paired Discogs master or release', 'an MB row names what was looked for on Discogs');
  const discogs = replaceOfferState(input({ rowSource: 'discogs', ownKey: '11052' }));
  t.contains(discogs.title, 'no paired MusicBrainz release group', 'a Discogs row names the MB side');
  const masterless = replaceOfferState(input({ rowSource: 'discogs', ownKey: null }));
  t.equal(masterless.reason, 'masterless_no_pair', 'a masterless row with no pair gets its own reason');
  t.contains(masterless.title, 'no master and no paired MusicBrainz release group', 'and its own copy, naming what the other pathway would have paired');
}

t.section('disabled — pair found, no request on either side');
{
  const offer = replaceOfferState(input({ pair: MASTER }));
  t.equal(offer.reason, 'pair_inactive', 'reason');
  t.contains(offer.title, 'either pathway', 'tooltip says both sides were checked');
  t.contains(offer.title, 'Discogs master "Absolution"', 'and names the pair');
}

t.section('reason vocabulary and pairNoun');
{
  t.deepEqual([...REPLACE_OFFER_REASONS].sort(), [
    'lookup_unavailable', 'masterless', 'masterless_no_pair', 'no_pair', 'no_request', 'own',
    'pair_inactive', 'paired', 'pairing_unchecked',
  ], 'the exported vocabulary is exactly the nine reasons');
  t.equal(pairNoun(MASTER), 'Discogs master', 'master noun');
  t.equal(pairNoun(RELEASE), 'Discogs release', 'release noun');
  t.equal(pairNoun(RG), 'MusicBrainz release group', 'release group noun');
}

t.done();
