-- slskd reissues transfer IDs when it retries the same queued file.  The
-- write-ahead-ledgered (username, filename) queue key is therefore the durable
-- ownership boundary once the POST is known to have been accepted. A pending
-- write-ahead intent alone is not destructive authority: the POST may have
-- been definitively rejected and a human may later enqueue the same key.

ALTER TABLE slskd_transfer_ledger
    ADD COLUMN accepted_at TIMESTAMPTZ;

-- Preserve every historical positive acceptance signal before removing the
-- attempt-local columns. Reconciled IDs prove the POST returned a transfer,
-- terminal stamps prove an owned terminal observation, and local paths prove
-- an authoritative completion event. Rows with none of these remain pending.
UPDATE slskd_transfer_ledger
SET accepted_at = COALESCE(completed_at, enqueued_at)
WHERE transfer_id IS NOT NULL
   OR completed_at IS NOT NULL
   OR local_path IS NOT NULL;

DROP INDEX idx_slskd_transfer_ledger_transfer_id_unique;
DROP INDEX idx_slskd_transfer_ledger_open;

ALTER TABLE slskd_transfer_ledger
    DROP COLUMN transfer_id,
    DROP COLUMN completed_at;
