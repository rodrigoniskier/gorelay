package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math/rand/v2"
	"time"
)

var errAttemptHistoryMissing = errors.New("attempt history missing")

// retryDeliveryStore adds HTTP failure policy without another claiming loop.
// The event's attempt budget is pinned by the shared transactional claim.
type retryDeliveryStore struct {
	deliveryLeaseStore
	policy RetryPolicy
}

func (s retryDeliveryStore) Claim(ctx context.Context, duration time.Duration) (Lease, error) {
	if err := s.policy.Validate(); err != nil {
		return Lease{}, err
	}
	return s.postgresStore.claim(ctx, duration, s.policy.MaxAttempts)
}

func (s retryDeliveryStore) Fail(ctx context.Context, lease Lease, failure error) error {
	if err := s.policy.Validate(); err != nil {
		return err
	}
	decision := s.policy.Decide(failure, lease.Attempt, rand.Float64())
	return s.postgresStore.RecordFailure(ctx, lease, decision)
}

func (s postgresStore) claim(ctx context.Context, duration time.Duration, maxAttempts int) (Lease, error) {
	if duration < 100*time.Millisecond || duration > 24*time.Hour {
		return Lease{}, errors.New("lease duration must be between 100ms and 24h")
	}
	if maxAttempts < 1 || maxAttempts > 100 {
		return Lease{}, errors.New("maximum attempts must be between 1 and 100")
	}
	token, err := newEventID()
	if err != nil {
		return Lease{}, err
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return Lease{}, err
	}
	defer tx.Rollback()

	// An active last attempt is allowed to finish. Only expired ownership or an
	// otherwise inactive eligible row can exhaust. No destination is fabricated.
	// A read-only probe keeps idle polling free of write transactions: the
	// locking sweep below runs only when exhausted rows actually exist.
	var needsSweep bool
	err = tx.QueryRowContext(ctx, `SELECT EXISTS (
    SELECT 1 FROM events
    WHERE destination_id IS NOT NULL AND fingerprint_version = 2
      AND processing_attempts >= COALESCE(retry_max_attempts, $1::integer)
      AND (status IN ('pending', 'retry_wait')
           OR (status = 'processing' AND lease_expires_at <= clock_timestamp())))`, maxAttempts).Scan(&needsSweep)
	if err != nil {
		return Lease{}, err
	}
	if needsSweep {
		err = sweepExhausted(ctx, tx, maxAttempts)
		if err != nil {
			return Lease{}, err
		}
	}

	var lease Lease
	err = tx.QueryRowContext(ctx, `
WITH candidate AS (
    SELECT id FROM events
    WHERE destination_id IS NOT NULL AND fingerprint_version = 2
      AND processing_attempts < COALESCE(retry_max_attempts, $3::integer)
      AND (status = 'pending'
           OR (status = 'retry_wait' AND next_attempt_at <= clock_timestamp())
           OR (status = 'processing' AND lease_expires_at <= clock_timestamp()))
    ORDER BY created_at, id
    FOR UPDATE SKIP LOCKED
    LIMIT 1
)
UPDATE events AS e
SET status = 'processing', lease_token = $1,
    lease_expires_at = clock_timestamp() + ($2::bigint * interval '1 microsecond'),
    processing_attempts = e.processing_attempts + 1,
    retry_max_attempts = COALESCE(e.retry_max_attempts, $3::integer),
    next_attempt_at = NULL, dead_letter_at = NULL, dead_letter_reason = NULL
FROM candidate WHERE e.id = candidate.id
RETURNING e.id, e.destination_id, e.event_type, e.payload, e.status, e.created_at,
          e.lease_token, e.lease_expires_at, e.processing_attempts`, token, duration.Microseconds(), maxAttempts).Scan(
		&lease.Event.ID, &lease.Event.DestinationID, &lease.Event.Type,
		&lease.Event.Payload, &lease.Event.Status, &lease.Event.CreatedAt,
		&lease.Token, &lease.ExpiresAt, &lease.Attempt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		// The bounded exhaustion sweep must survive even with no new work.
		if err := tx.Commit(); err != nil {
			return Lease{}, err
		}
		return Lease{}, errNoClaimableEvent
	}
	if err != nil {
		return Lease{}, err
	}

	// A previous expired claim consumed its slot even if it crashed before send.
	_, err = tx.ExecContext(ctx, `UPDATE event_attempts
SET outcome = 'lease_expired', finished_at = clock_timestamp(),
    failure_code = 'lease_expired', http_status = NULL, next_attempt_at = NULL
WHERE event_id = $1 AND attempt_number < $2 AND outcome = 'in_progress'`, lease.Event.ID, lease.Attempt)
	if err != nil {
		return Lease{}, err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO event_attempts(event_id, attempt_number, lease_token)
VALUES($1,$2,$3)`, lease.Event.ID, lease.Attempt, lease.Token)
	if err != nil {
		return Lease{}, err
	}
	if err := tx.Commit(); err != nil {
		return Lease{}, err
	}
	return lease, nil
}

func sweepExhausted(ctx context.Context, tx *sql.Tx, maxAttempts int) error {
	_, err := tx.ExecContext(ctx, `
WITH exhausted AS (
    SELECT id FROM events
    WHERE destination_id IS NOT NULL AND fingerprint_version = 2
      AND processing_attempts >= COALESCE(retry_max_attempts, $1::integer)
      AND (status IN ('pending', 'retry_wait')
           OR (status = 'processing' AND lease_expires_at <= clock_timestamp()))
    ORDER BY created_at, id
    FOR UPDATE SKIP LOCKED
    LIMIT 32
), finalized AS (
    UPDATE events AS e
    SET status = 'dead_letter', retry_max_attempts = COALESCE(e.retry_max_attempts, $1::integer),
        next_attempt_at = NULL, lease_token = NULL, lease_expires_at = NULL,
        dead_letter_at = clock_timestamp(), dead_letter_reason = 'attempts_exhausted',
        last_failure_code = 'attempts_exhausted'
    FROM exhausted WHERE e.id = exhausted.id
    RETURNING e.id
)
UPDATE event_attempts AS a
SET outcome = 'lease_expired', finished_at = clock_timestamp(),
    failure_code = 'lease_expired', http_status = NULL, next_attempt_at = NULL
FROM finalized WHERE a.event_id = finalized.id AND a.outcome = 'in_progress'`, maxAttempts)
	return err
}

func validFailureCode(code string) bool {
	switch code {
	case "timeout", "network", "http_transient", "http_terminal", "destination_not_configured",
		"invalid_event", "configuration", "canceled", "processor_error", "lease_expired", "attempts_exhausted":
		return true
	default:
		return false
	}
}

// RecordFailure fences state and history together after locking the event row.
// Only fixed safe codes/status are persisted, never arbitrary error text.
func (s postgresStore) RecordFailure(ctx context.Context, lease Lease, decision FailureDecision) error {
	if !validFailureCode(decision.Code) || (decision.HTTPStatus != 0 && (decision.HTTPStatus < 100 || decision.HTTPStatus > 599)) {
		return errors.New("invalid failure classification")
	}
	if decision.Delay < 0 || decision.Delay > 24*time.Hour || (!decision.Terminal && decision.Delay < time.Microsecond) {
		return errors.New("invalid retry delay")
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var status string
	var token sql.NullString
	var attempt int64
	var budget sql.NullInt64
	err = tx.QueryRowContext(ctx, `SELECT status, lease_token, processing_attempts, retry_max_attempts
FROM events WHERE id=$1 FOR UPDATE`, lease.Event.ID).Scan(&status, &token, &attempt, &budget)
	if errors.Is(err, sql.ErrNoRows) {
		return errLeaseLost
	}
	if err != nil {
		return err
	}
	if status != "processing" || !token.Valid || token.String != lease.Token || attempt != lease.Attempt {
		return errLeaseLost
	}
	// Claim always pins the per-event budget. Falling back to a compiled-in
	// default here would silently ignore GORELAY_MAX_ATTEMPTS if that changed.
	if !budget.Valid {
		return errors.New("event attempt budget is not pinned")
	}

	var next sql.NullTime
	var resultingStatus string
	err = tx.QueryRowContext(ctx, `
UPDATE events
SET status = CASE WHEN $3 OR processing_attempts >= retry_max_attempts
                  THEN 'dead_letter' ELSE 'retry_wait' END,
    next_attempt_at = CASE WHEN $3 OR processing_attempts >= retry_max_attempts
                          THEN NULL ELSE clock_timestamp() + ($5::bigint * interval '1 microsecond') END,
    dead_letter_at = CASE WHEN $3 OR processing_attempts >= retry_max_attempts
                          THEN clock_timestamp() ELSE NULL END,
    dead_letter_reason = CASE WHEN $3 THEN 'terminal_failure'
                             WHEN processing_attempts >= retry_max_attempts
                             THEN 'attempts_exhausted' ELSE NULL END,
    last_failure_code = $4, lease_token = NULL, lease_expires_at = NULL
WHERE id=$1 AND lease_token=$2 AND status='processing'
  AND lease_expires_at > clock_timestamp()
RETURNING status, next_attempt_at`, lease.Event.ID, lease.Token, decision.Terminal,
		decision.Code, decision.Delay.Microseconds()).Scan(&resultingStatus, &next)
	if errors.Is(err, sql.ErrNoRows) {
		return errLeaseLost
	}
	if err != nil {
		return err
	}
	outcome := "dead_letter"
	if resultingStatus == "retry_wait" {
		outcome = "retry_scheduled"
	}
	var nextTime *time.Time
	if next.Valid {
		nextTime = &next.Time
	}
	if err = closeAttempt(ctx, tx, lease.Event.ID, lease.Token, outcome, decision.Code, decision.HTTPStatus, nextTime, budget.Valid); err != nil {
		return err
	}
	return tx.Commit()
}

func closeAttempt(ctx context.Context, tx *sql.Tx, id, token, outcome, code string, httpStatus int, next *time.Time, required bool) error {
	result, err := tx.ExecContext(ctx, `UPDATE event_attempts
SET outcome=$3, finished_at=clock_timestamp(), failure_code=NULLIF($4,''),
    http_status=NULLIF($5::integer,0), next_attempt_at=$6
WHERE event_id=$1 AND lease_token=$2 AND outcome='in_progress'`, id, token, outcome, code, httpStatus, next)
	if err != nil {
		return err
	}
	count, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if count > 1 || (required && count != 1) {
		return fmt.Errorf("%w: matching unfinished entry required", errAttemptHistoryMissing)
	}
	return nil
}
