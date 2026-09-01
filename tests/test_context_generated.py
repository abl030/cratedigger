"""Generated properties for context collaborator derivation (issue #1313).

``CycleCollaborators`` makes forgetting a collaborator a type error at a
construction site. That is only half the invariant: a context DERIVED from
the cycle owner — Phase 1's polling context, a find-download worker's — has
to carry the RIGHT value in each of the six slots, and "right" differs per
slot and per lane.

The properties below drive the two real derivation functions
(``cratedigger.build_phase1_context`` and
``lib.enqueue.prepare_find_download_context``) over generated owner worlds
and assert the whole six-slot outcome, so a derivation that quietly drops or
substitutes one is a counterexample rather than a silent degradation. PR
#1280's shipped defect was exactly a dropped slot.

The checkers accumulate violations rather than raising at the first, so
clause ordering cannot mask a later one;
``TestDerivationCheckersTripOnViolations`` proves each clause trips, by its
own message.

The worker world (``WorkerCollaborators``: no slskd client, therefore no
reachable destructive path) has no generated world space at all — its four
absent slots are unconditional read-only properties — so it is pinned
deterministically in ``tests/test_context.py`` and deliberately not
property-tested here.
"""
from __future__ import annotations

import os
import sys
import unittest
from collections.abc import Callable
from unittest.mock import MagicMock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from hypothesis import given
from hypothesis import strategies as st

import tests._hypothesis_profiles  # noqa: F401  (loads the active profile)
from cratedigger import build_phase1_context
from lib.config import CratediggerConfig
from lib.context import CratediggerContext, CycleCollaborators
from lib.download_ownership import DownloadOwnershipWriter
from lib.enqueue import ClaimedQueueKeysRegistry, prepare_find_download_context
from lib.peer_cache import PeerCache
from tests.fakes import FakePipelineDB, FakePipelineDBSource, FakeSlskdAPI

# --- Clause messages, spelled once so the checkers and their known-bad
# --- self-tests cannot drift apart.
CFG_CLAUSE = "cfg: a derived context must carry the owner's config"
SLSKD_CLAUSE = "slskd: a derived context must carry the owner's slskd client"
OWNERSHIP_CLAUSE = (
    "download_ownership: the owner's ledger-ownership writer must be "
    "forwarded by identity, or every destructive path fails closed")
REGISTRY_CLAUSE = (
    "claimed_queue_keys_registry: the cycle's one same-cycle claim registry "
    "must be forwarded by identity")
OWN_SOURCE_CLAUSE = (
    "pipeline_db_source: a derived context must open its own source, never "
    "borrow the owner thread's connection")
PHASE1_COOLDOWN_CLAUSE = (
    "cooled_down_users: Phase 1 shares the owner's SET OBJECT, so a cooldown "
    "it discovers reaches Phase 2's workers")
PHASE1_PEER_CACHE_CLAUSE = (
    "peer_cache: Phase 1 runs on another thread and takes no peer cache; the "
    "owner's instance is not shareable unforked")
WORKER_COOLDOWN_COPY_CLAUSE = (
    "cooled_down_users: a find-download worker takes a COPY, so one worker's "
    "discovery cannot mutate a set another is reading")
WORKER_COOLDOWN_CONTENTS_CLAUSE = (
    "cooled_down_users: the worker's copy must start with the owner's "
    "contents")
WORKER_PEER_FORK_CLAUSE = (
    "peer_cache: a find-download worker takes a FORK, never the owner's "
    "instance, because PeerCache is not thread-shared")
WORKER_NO_PEER_CACHE_CLAUSE = (
    "peer_cache: no owner cache means the worker gets none either")


# ---------------------------------------------------------------------------
# Invariant checkers
# ---------------------------------------------------------------------------

def shared_derivation_violations(
    owner: CratediggerContext, derived: CratediggerContext,
) -> list[str]:
    """The four slots every derived cycle context carries unchanged, plus
    the one it must NOT share.

    ``download_ownership`` and ``claimed_queue_keys_registry`` are shared BY
    IDENTITY on purpose: the writer opens a fresh DB handle per operation so
    worker threads can share one instance, and the registry is the cycle's
    single same-cycle claim ledger (#1178 PR2 review F7). Copying either
    would silently split the ledger.

    ``pipeline_db_source`` is the inverse: psycopg2 connections are not
    thread-safe, so a derived context on another thread needs its own.
    """
    violations: list[str] = []
    if derived.cfg is not owner.cfg:
        violations.append(CFG_CLAUSE)
    if derived.slskd is not owner.slskd:
        violations.append(SLSKD_CLAUSE)
    if derived.download_ownership is not owner.download_ownership:
        violations.append(OWNERSHIP_CLAUSE)
    if (derived.claimed_queue_keys_registry
            is not owner.claimed_queue_keys_registry):
        violations.append(REGISTRY_CLAUSE)
    if derived.pipeline_db_source is owner.pipeline_db_source:
        violations.append(OWN_SOURCE_CLAUSE)
    return violations


def phase1_derivation_violations(
    owner: CratediggerContext, derived: CratediggerContext,
) -> list[str]:
    """``build_phase1_context``'s two lane-specific slots."""
    violations = shared_derivation_violations(owner, derived)
    if derived.cooled_down_users is not owner.cooled_down_users:
        violations.append(PHASE1_COOLDOWN_CLAUSE)
    if derived.peer_cache is not None:
        violations.append(PHASE1_PEER_CACHE_CLAUSE)
    return violations


def worker_derivation_violations(
    owner: CratediggerContext, derived: CratediggerContext,
) -> list[str]:
    """``prepare_find_download_context``'s lane-specific slots."""
    violations = shared_derivation_violations(owner, derived)
    if derived.cooled_down_users is owner.cooled_down_users:
        violations.append(WORKER_COOLDOWN_COPY_CLAUSE)
    if derived.cooled_down_users != owner.cooled_down_users:
        violations.append(WORKER_COOLDOWN_CONTENTS_CLAUSE)
    if owner.peer_cache is not None and derived.peer_cache is owner.peer_cache:
        violations.append(WORKER_PEER_FORK_CLAUSE)
    if owner.peer_cache is None and derived.peer_cache is not None:
        violations.append(WORKER_NO_PEER_CACHE_CLAUSE)
    return violations


# ---------------------------------------------------------------------------
# Builders + strategies
# ---------------------------------------------------------------------------

def _cycle_ctor() -> Callable[..., CycleCollaborators]:
    """The class behind an unchecked-argument callable type.

    The self-tests below construct collaborator sets from a mutated dict of
    field values on purpose; ``Callable[..., X]`` states "arguments
    unchecked here" in the type system rather than with an escape hatch.
    """
    return CycleCollaborators


def _writer() -> DownloadOwnershipWriter:
    ledger = FakePipelineDB()
    return DownloadOwnershipWriter(
        db_factory=lambda: ledger, close_after_use=False)


def _peer_cache() -> PeerCache:
    # client=None is exactly what connect_from_config returns when Redis is
    # unreachable, so this is a real cold cache, not a stand-in.
    return PeerCache(None, ttl_seconds=60, speed_ttl_seconds=10)


_OWNERSHIP = st.booleans().map(
    lambda wired: _writer() if wired else None)
_REGISTRY = st.booleans().map(
    lambda wired: ClaimedQueueKeysRegistry() if wired else None)
_PEER_CACHE = st.booleans().map(
    lambda wired: _peer_cache() if wired else None)
_COOLDOWNS = st.sets(
    st.sampled_from(("peer-a", "peer-b", "peer-c")), max_size=3)


@st.composite
def owner_contexts(draw: st.DrawFn) -> CratediggerContext:
    """A cycle-owner context over the wired/unwired collaborator worlds."""
    ctx = CratediggerContext(
        collaborators=CycleCollaborators(
            cfg=CratediggerConfig(),
            slskd=FakeSlskdAPI(),
            pipeline_db_source=FakePipelineDBSource(FakePipelineDB()),
            download_ownership=draw(_OWNERSHIP),
            claimed_queue_keys_registry=draw(_REGISTRY),
            peer_cache=draw(_PEER_CACHE),
        ),
    )
    ctx.cooled_down_users.update(draw(_COOLDOWNS))
    return ctx


# ---------------------------------------------------------------------------
# Properties
# ---------------------------------------------------------------------------

class TestDerivedContextsCarryTheOwnersCollaborators(unittest.TestCase):

    @given(owner=owner_contexts())
    def test_phase1_context_derivation(self, owner: CratediggerContext):
        derived = build_phase1_context(
            cfg=owner.cfg,
            slskd=owner.slskd,
            pipeline_db_source=FakePipelineDBSource(FakePipelineDB()),
            owner_ctx=owner,
        )
        self.assertEqual(phase1_derivation_violations(owner, derived), [])

    @given(owner=owner_contexts())
    def test_find_download_worker_context_derivation(
        self, owner: CratediggerContext,
    ):
        album = MagicMock(id=1, db_request_id=1)
        derived = prepare_find_download_context(album, owner)
        self.assertEqual(worker_derivation_violations(owner, derived), [])


# ---------------------------------------------------------------------------
# Known-bad self-tests — one per clause, asserting THAT clause's message
# ---------------------------------------------------------------------------

class TestDerivationCheckersTripOnViolations(unittest.TestCase):
    """Every clause of every checker, tripped by its own minimal world.

    Each world makes exactly one clause's condition true; the checkers
    accumulate rather than short-circuit, so the assertion names the clause
    it means rather than whichever fired first.
    """

    def _owner(self, **overrides: object) -> CratediggerContext:
        fields: dict[str, object] = {
            "cfg": CratediggerConfig(),
            "slskd": FakeSlskdAPI(),
            "pipeline_db_source": FakePipelineDBSource(FakePipelineDB()),
            "download_ownership": _writer(),
            "claimed_queue_keys_registry": ClaimedQueueKeysRegistry(),
            "peer_cache": _peer_cache(),
        }
        fields.update(overrides)
        return CratediggerContext(collaborators=_cycle_ctor()(**fields))

    def _derived(
        self, owner: CratediggerContext, **overrides: object,
    ) -> CratediggerContext:
        """A conforming derived context, with exactly one slot broken."""
        collaborators = owner.collaborators
        assert isinstance(collaborators, CycleCollaborators)
        fields: dict[str, object] = {
            "cfg": collaborators.cfg,
            "slskd": collaborators.slskd,
            # Its own source: conforming by default, so only the override
            # under test can trip a clause.
            "pipeline_db_source": FakePipelineDBSource(FakePipelineDB()),
            "download_ownership": collaborators.download_ownership,
            "claimed_queue_keys_registry": (
                collaborators.claimed_queue_keys_registry),
            "peer_cache": collaborators.peer_cache,
        }
        fields.update(overrides)
        return CratediggerContext(collaborators=_cycle_ctor()(**fields))

    # --- shared_derivation_violations ------------------------------------

    def test_cfg_clause(self):
        owner = self._owner()
        derived = self._derived(owner, cfg=CratediggerConfig())
        self.assertIn(
            CFG_CLAUSE, shared_derivation_violations(owner, derived))

    def test_slskd_clause(self):
        owner = self._owner()
        derived = self._derived(owner, slskd=FakeSlskdAPI())
        self.assertIn(
            SLSKD_CLAUSE, shared_derivation_violations(owner, derived))

    def test_download_ownership_clause(self):
        owner = self._owner()
        derived = self._derived(owner, download_ownership=None)
        self.assertIn(
            OWNERSHIP_CLAUSE, shared_derivation_violations(owner, derived))

    def test_claimed_queue_keys_registry_clause(self):
        owner = self._owner()
        derived = self._derived(
            owner, claimed_queue_keys_registry=ClaimedQueueKeysRegistry())
        self.assertIn(
            REGISTRY_CLAUSE, shared_derivation_violations(owner, derived))

    def test_pipeline_db_source_clause(self):
        owner = self._owner()
        derived = self._derived(
            owner, pipeline_db_source=owner.pipeline_db_source)
        self.assertIn(
            OWN_SOURCE_CLAUSE, shared_derivation_violations(owner, derived))

    # --- phase1_derivation_violations -------------------------------------

    def test_phase1_cooled_down_users_clause(self):
        owner = self._owner()
        # peer_cache=None keeps the OTHER phase-1 clause satisfied, so this
        # world trips exactly the cooldown-sharing one.
        derived = self._derived(owner, peer_cache=None)
        derived.cooled_down_users = set(owner.cooled_down_users)
        self.assertIn(
            PHASE1_COOLDOWN_CLAUSE,
            phase1_derivation_violations(owner, derived))

    def test_phase1_peer_cache_clause(self):
        owner = self._owner()
        derived = self._derived(owner)
        derived.cooled_down_users = owner.cooled_down_users
        self.assertIn(
            PHASE1_PEER_CACHE_CLAUSE,
            phase1_derivation_violations(owner, derived))

    # --- worker_derivation_violations -------------------------------------

    def test_worker_shared_cooldown_set_clause(self):
        owner = self._owner()
        owner.cooled_down_users.add("peer-a")
        derived = self._derived(owner, peer_cache=_peer_cache())
        derived.cooled_down_users = owner.cooled_down_users
        self.assertIn(
            WORKER_COOLDOWN_COPY_CLAUSE,
            worker_derivation_violations(owner, derived))

    def test_worker_cooldown_contents_clause(self):
        owner = self._owner()
        owner.cooled_down_users.add("peer-a")
        derived = self._derived(owner, peer_cache=_peer_cache())
        derived.cooled_down_users = set()
        self.assertIn(
            WORKER_COOLDOWN_CONTENTS_CLAUSE,
            worker_derivation_violations(owner, derived))

    def test_worker_unforked_peer_cache_clause(self):
        owner = self._owner()
        derived = self._derived(owner, peer_cache=owner.peer_cache)
        derived.cooled_down_users = set(owner.cooled_down_users)
        self.assertIn(
            WORKER_PEER_FORK_CLAUSE,
            worker_derivation_violations(owner, derived))

    def test_worker_invented_peer_cache_clause(self):
        owner = self._owner(peer_cache=None)
        derived = self._derived(owner, peer_cache=_peer_cache())
        derived.cooled_down_users = set(owner.cooled_down_users)
        self.assertIn(
            WORKER_NO_PEER_CACHE_CLAUSE,
            worker_derivation_violations(owner, derived))


if __name__ == "__main__":
    unittest.main()
