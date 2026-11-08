-- This stage confirms an injected processor, not HTTP delivery.
-- Old pending rows (including unknown destinations) remain unchanged.
ALTER TABLE events DROP CONSTRAINT events_status_check;
ALTER TABLE events ADD CONSTRAINT events_status_check
    CHECK (status IN ('pending', 'processing', 'processed'));
ALTER TABLE events ADD COLUMN lease_token uuid;
ALTER TABLE events ADD COLUMN lease_expires_at timestamptz;
ALTER TABLE events ADD COLUMN processing_attempts bigint NOT NULL DEFAULT 0
    CHECK (processing_attempts >= 0);
ALTER TABLE events ADD COLUMN processed_at timestamptz;
ALTER TABLE events ADD CONSTRAINT events_processing_state CHECK (
    (status = 'pending' AND lease_token IS NULL AND lease_expires_at IS NULL
        AND processed_at IS NULL)
    OR
    (status = 'processing' AND lease_token IS NOT NULL AND lease_expires_at IS NOT NULL
        AND processed_at IS NULL AND processing_attempts > 0)
    OR
    (status = 'processed' AND lease_token IS NULL AND lease_expires_at IS NULL
        AND processed_at IS NOT NULL AND processing_attempts > 0)
);
CREATE INDEX events_pending_claim_idx ON events (created_at, id)
    WHERE destination_id IS NOT NULL AND fingerprint_version = 2 AND status = 'pending';
CREATE INDEX events_expired_claim_idx ON events (lease_expires_at, created_at, id)
    WHERE destination_id IS NOT NULL AND fingerprint_version = 2 AND status = 'processing';
