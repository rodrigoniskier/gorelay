CREATE TABLE events (
    id uuid PRIMARY KEY,
    idempotency_key text NOT NULL UNIQUE CHECK (octet_length(idempotency_key) BETWEEN 1 AND 200),
    event_type text NOT NULL CHECK (octet_length(event_type) BETWEEN 1 AND 128),
    payload jsonb NOT NULL CHECK (jsonb_typeof(payload) = 'object'),
    request_hash bytea NOT NULL CHECK (octet_length(request_hash) = 32),
    status text NOT NULL DEFAULT 'pending' CHECK (status = 'pending'),
    created_at timestamptz NOT NULL DEFAULT now()
);
