-- 074_canonical_release_id.sql -- MusicBrainz merge survivors (#1059)
--
-- MusicBrainz editors merge two release entries; the loser's MBID becomes a
-- permanent 301 to the survivor and `mbsync` retags the local files onto the
-- survivor.  The stored acquisition id is frozen history -- "I went and got
-- release X, here is the proof" -- and never moves.  This column records what
-- MusicBrainz calls that release *now*, so the request->album join and the
-- import-time match can both resolve over the union of the two identities
-- without asking MusicBrainz on any read.
--
-- Only an OBSERVED 301 ever writes here.  A body field, a metadata match, or a
-- release-group relative never does.  A 4xx is never read as "this release is
-- gone".

ALTER TABLE album_requests
    ADD COLUMN canonical_release_id TEXT,
    ADD COLUMN canonical_resolved_at TIMESTAMPTZ;

COMMENT ON COLUMN album_requests.canonical_release_id IS
    'MusicBrainz merge survivor for mb_release_id, proven by an observed 301. NULL means no merge is known. Never equals mb_release_id; never derived from a response body field, metadata, or a release-group relative.';

COMMENT ON COLUMN album_requests.canonical_resolved_at IS
    'When the 301 backing canonical_release_id was observed. Provenance for the redirect-proof rule; NULL exactly when canonical_release_id is NULL.';

-- Provenance is not optional: a survivor without the observation that proved
-- it cannot be distinguished from a guess.  The converse is deliberately not
-- constrained -- there is no "checked, no redirect" timestamp, because the
-- sweep re-asks every row every day and stores nothing when nothing changed.
ALTER TABLE album_requests
    ADD CONSTRAINT album_requests_canonical_requires_observation
    CHECK (
        canonical_release_id IS NULL
        OR canonical_resolved_at IS NOT NULL
    );

-- A survivor that equals the acquisition id is not a merge, it is a no-op the
-- reconciler must never persist.  Enforced rather than merely intended: a
-- self-referential canonical would make the union resolver's two-identity
-- probe collapse to one and silently hide a real split.
ALTER TABLE album_requests
    ADD CONSTRAINT album_requests_canonical_is_not_acquisition
    CHECK (
        canonical_release_id IS NULL
        OR mb_release_id IS NULL
        OR canonical_release_id <> mb_release_id
    );

-- The populated set is single digits today and converges on the true merge
-- rate, so this exists for the audit/CLI enumeration, not for the join --
-- which reads the column off request rows it has already loaded.
CREATE INDEX idx_album_requests_canonical_release_id
    ON album_requests (canonical_release_id)
    WHERE canonical_release_id IS NOT NULL;
