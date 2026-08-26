-- Path evidence implies acceptance (issue #1278 candidate 1).
--
-- `get_owned_local_paths` -- the disk reaper's "is this file mine?" set,
-- documented in lib/slskd_transfers.py as "the sole positive disk
-- ownership signal" -- selects on `local_path IS NOT NULL` ALONE. It is
-- the one ledger read that does not spell the `accepted_at IS NOT NULL`
-- acceptance gate every other ownership query spells, so on its own text
-- it grants destructive disk authority to a merely-pending write-ahead
-- intent.
--
-- It is safe today only because of an argument made in a DIFFERENT
-- statement: `stamp_transfer_completion` is the only writer of
-- `local_path`, and its UPDATE requires `accepted_at IS NOT NULL`. That
-- is prose-enforced coupling between two queries in two different
-- methods -- exactly the shape that decays when a third writer appears
-- (a repair one-shot, a future backfill, an operator's raw SQL fix).
--
-- This constraint moves the implication into the schema, where it holds
-- against every writer rather than against the one we audited. After it,
-- `local_path IS NOT NULL` IS an accepted-ownership predicate by
-- construction, and the reaper's query needs no companion argument to be
-- correct.
--
-- Historically consistent by construction: migration 051 backfilled
-- `accepted_at` for every row carrying a `local_path` (`WHERE ... OR
-- local_path IS NOT NULL`), so no row has ever been able to reach the
-- violating shape through a shipped writer. Measured on the live
-- database 2026-08-26 before writing this file: 45,203 ledger rows,
-- 30,511 carrying a `local_path`, 0 with a path and no acceptance.
--
-- NOT VALID is deliberately NOT used: the table validates in full here
-- because the violating cohort is empty, so there is no long lock to
-- avoid and no second validation step to remember later.

ALTER TABLE slskd_transfer_ledger
    ADD CONSTRAINT slskd_transfer_ledger_path_implies_acceptance
    CHECK (local_path IS NULL OR accepted_at IS NOT NULL);
