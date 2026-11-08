-- Keep processed callbacks from Stage3A distinct from confirmed HTTP delivery.
ALTER TABLE events ADD COLUMN delivered_at timestamptz;
ALTER TABLE events DROP CONSTRAINT events_status_check;
ALTER TABLE events ADD CONSTRAINT events_status_check
    CHECK (status IN ('pending', 'processing', 'processed', 'delivered'));
ALTER TABLE events DROP CONSTRAINT events_processing_state;
ALTER TABLE events ADD CONSTRAINT events_processing_state CHECK (
    (status = 'pending' AND lease_token IS NULL AND lease_expires_at IS NULL
        AND processed_at IS NULL AND delivered_at IS NULL)
    OR
    (status = 'processing' AND lease_token IS NOT NULL AND lease_expires_at IS NOT NULL
        AND processed_at IS NULL AND delivered_at IS NULL AND processing_attempts > 0)
    OR
    (status = 'processed' AND lease_token IS NULL AND lease_expires_at IS NULL
        AND processed_at IS NOT NULL AND delivered_at IS NULL AND processing_attempts > 0)
    OR
    (status = 'delivered' AND lease_token IS NULL AND lease_expires_at IS NULL
        AND processed_at IS NOT NULL AND delivered_at IS NOT NULL AND processing_attempts > 0)
);
