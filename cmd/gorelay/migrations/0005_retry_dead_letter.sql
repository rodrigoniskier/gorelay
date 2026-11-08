-- Preserve existing event bodies, counters and earlier migrations. Unknown older
-- attempt history cannot be reconstructed and is deliberately not fabricated.
ALTER TABLE events ADD COLUMN retry_max_attempts integer
    CHECK (retry_max_attempts BETWEEN 1 AND 100);
ALTER TABLE events ADD COLUMN next_attempt_at timestamptz;
ALTER TABLE events ADD COLUMN last_failure_code text CHECK (last_failure_code IN (
    'timeout', 'network', 'http_transient', 'http_terminal',
    'destination_not_configured', 'invalid_event', 'configuration', 'canceled',
    'processor_error', 'lease_expired', 'attempts_exhausted'
));
ALTER TABLE events ADD COLUMN dead_letter_at timestamptz;
ALTER TABLE events ADD COLUMN dead_letter_reason text
    CHECK (dead_letter_reason IN ('attempts_exhausted', 'terminal_failure'));
ALTER TABLE events DROP CONSTRAINT events_status_check;
ALTER TABLE events ADD CONSTRAINT events_status_check CHECK (
    status IN ('pending', 'processing', 'processed', 'delivered', 'retry_wait', 'dead_letter')
);
ALTER TABLE events DROP CONSTRAINT events_processing_state;
ALTER TABLE events ADD CONSTRAINT events_processing_state CHECK (
    (status = 'pending' AND lease_token IS NULL AND lease_expires_at IS NULL
        AND processed_at IS NULL AND delivered_at IS NULL AND next_attempt_at IS NULL
        AND dead_letter_at IS NULL AND dead_letter_reason IS NULL)
    OR
    (status = 'processing' AND lease_token IS NOT NULL AND lease_expires_at IS NOT NULL
        AND processed_at IS NULL AND delivered_at IS NULL AND next_attempt_at IS NULL
        AND dead_letter_at IS NULL AND dead_letter_reason IS NULL AND processing_attempts > 0)
    OR
    (status = 'processed' AND lease_token IS NULL AND lease_expires_at IS NULL
        AND processed_at IS NOT NULL AND delivered_at IS NULL AND next_attempt_at IS NULL
        AND dead_letter_at IS NULL AND dead_letter_reason IS NULL AND processing_attempts > 0)
    OR
    (status = 'delivered' AND lease_token IS NULL AND lease_expires_at IS NULL
        AND processed_at IS NOT NULL AND delivered_at IS NOT NULL AND next_attempt_at IS NULL
        AND dead_letter_at IS NULL AND dead_letter_reason IS NULL AND processing_attempts > 0)
    OR
    (status = 'retry_wait' AND lease_token IS NULL AND lease_expires_at IS NULL
        AND processed_at IS NULL AND delivered_at IS NULL AND next_attempt_at IS NOT NULL
        AND dead_letter_at IS NULL AND dead_letter_reason IS NULL AND last_failure_code IS NOT NULL
        AND processing_attempts > 0 AND retry_max_attempts IS NOT NULL)
    OR
    (status = 'dead_letter' AND lease_token IS NULL AND lease_expires_at IS NULL
        AND processed_at IS NULL AND delivered_at IS NULL AND next_attempt_at IS NULL
        AND dead_letter_at IS NOT NULL AND dead_letter_reason IS NOT NULL
        AND last_failure_code IS NOT NULL AND processing_attempts > 0
        AND retry_max_attempts IS NOT NULL)
);
CREATE TABLE event_attempts (
    event_id uuid NOT NULL REFERENCES events(id),
    attempt_number bigint NOT NULL CHECK (attempt_number > 0),
    lease_token uuid NOT NULL UNIQUE,
    started_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    finished_at timestamptz,
    outcome text NOT NULL DEFAULT 'in_progress' CHECK (outcome IN (
        'in_progress', 'processed', 'delivered', 'retry_scheduled', 'dead_letter', 'lease_expired'
    )),
    failure_code text CHECK (failure_code IN (
        'timeout', 'network', 'http_transient', 'http_terminal',
        'destination_not_configured', 'invalid_event', 'configuration', 'canceled',
        'processor_error', 'lease_expired', 'attempts_exhausted'
    )),
    http_status integer CHECK (http_status BETWEEN 100 AND 599),
    next_attempt_at timestamptz,
    PRIMARY KEY (event_id, attempt_number),
    CHECK ((outcome = 'in_progress' AND finished_at IS NULL)
        OR (outcome <> 'in_progress' AND finished_at IS NOT NULL)),
    CHECK ((outcome = 'retry_scheduled' AND next_attempt_at IS NOT NULL)
        OR (outcome <> 'retry_scheduled' AND next_attempt_at IS NULL))
);
CREATE INDEX events_retry_due_idx ON events (next_attempt_at, created_at, id)
    WHERE destination_id IS NOT NULL AND fingerprint_version = 2 AND status = 'retry_wait';
CREATE INDEX events_dead_letter_idx ON events (dead_letter_at, id)
    WHERE status = 'dead_letter';
CREATE UNIQUE INDEX event_attempts_one_active_idx ON event_attempts (event_id)
    WHERE outcome = 'in_progress';
