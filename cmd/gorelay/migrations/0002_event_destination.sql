-- Keep legacy rows and their v1 hashes unchanged: a missing destination is unknown,
-- never guessed. NOT VALID preserves those rows while enforcing all new writes.
ALTER TABLE events ADD COLUMN destination_id text;
ALTER TABLE events ADD COLUMN fingerprint_version smallint NOT NULL DEFAULT 1;
ALTER TABLE events ADD CONSTRAINT events_destination_required CHECK (
    destination_id IS NOT NULL AND
    octet_length(destination_id) BETWEEN 1 AND 128 AND
    destination_id COLLATE "C" ~ '^[A-Za-z0-9][A-Za-z0-9._:-]*$'
) NOT VALID;
ALTER TABLE events ADD CONSTRAINT events_fingerprint_version CHECK (fingerprint_version IN (1, 2));
ALTER TABLE events ALTER COLUMN fingerprint_version SET DEFAULT 2;
