-- A live slskd transfer ID identifies exactly one enqueue attempt.
-- Historical retry reconciliation could copy the same still-visible terminal
-- ID onto later write-ahead rows for the same peer/file. Preserve every
-- forensic row, but retain the ID on exactly one: prefer authoritative path
-- evidence, then any terminal stamp, then the earliest enqueue (the true
-- attempt when no later evidence exists).
WITH ranked AS (
    SELECT id,
           row_number() OVER (
               PARTITION BY transfer_id
               ORDER BY
                   CASE
                       WHEN local_path IS NOT NULL THEN 0
                       WHEN completed_at IS NOT NULL THEN 1
                       ELSE 2
                   END,
                   enqueued_at ASC,
                   id ASC
           ) AS duplicate_rank
    FROM slskd_transfer_ledger
    WHERE transfer_id IS NOT NULL
)
UPDATE slskd_transfer_ledger AS ledger
SET transfer_id = NULL
FROM ranked
WHERE ledger.id = ranked.id
  AND ranked.duplicate_rank > 1;

CREATE UNIQUE INDEX idx_slskd_transfer_ledger_transfer_id_unique
    ON slskd_transfer_ledger (transfer_id)
    WHERE transfer_id IS NOT NULL;
