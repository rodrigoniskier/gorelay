package main

import (
	"context"
	"database/sql"
	"errors"
	"time"
)

var errNoClaimableEvent = errors.New("no claimable event")
var errLeaseLost = errors.New("lease lost")

// Lease contains internal ownership data. Tokens must not be logged or included
// in the HTTP Event representation. A claim is usable only after its commit.
type Lease struct {
	Event     Event
	Token     string
	ExpiresAt time.Time
	Attempt   int64
}

// Claim leases one pending or expired event. The short transaction holds the row
// lock only until the new token is committed, never while a processor executes.
func (s postgresStore) Claim(ctx context.Context, duration time.Duration) (Lease, error) {
	return s.claim(ctx, duration, DefaultRetryPolicy().MaxAttempts)
}

// Complete confirms callback processing, not external delivery. Ownership must
// still be current and unexpired at the conditional update. Acquire the row lock
// first, then check wall-clock expiry: a holder may have waited beyond expiry on
// a transaction that locked the row without changing its tuple version.
func (s postgresStore) Complete(ctx context.Context, id, token string) error {
	return s.complete(ctx, id, token, false)
}

// CompleteDelivery shares the audited lock/token/expiry checks with Complete.
// Only the HTTP-success adapter selects the delivered terminal outcome.
func (s postgresStore) CompleteDelivery(ctx context.Context, id, token string) error {
	return s.complete(ctx, id, token, true)
}

func (s postgresStore) complete(ctx context.Context, id, token string, delivered bool) error {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return err
	}
	defer tx.Rollback()

	var lockedID string
	var budget sql.NullInt64
	err = tx.QueryRowContext(ctx, `SELECT id, retry_max_attempts FROM events WHERE id = $1 FOR UPDATE`, id).Scan(&lockedID, &budget)
	if errors.Is(err, sql.ErrNoRows) {
		return errLeaseLost
	}
	if err != nil {
		return err
	}
	result, err := tx.ExecContext(ctx, `
UPDATE events
SET status = CASE WHEN $3 THEN 'delivered' ELSE 'processed' END,
    lease_token = NULL, lease_expires_at = NULL,
    processed_at = clock_timestamp(),
    delivered_at = CASE WHEN $3 THEN clock_timestamp() ELSE NULL END,
    next_attempt_at = NULL, dead_letter_at = NULL, dead_letter_reason = NULL,
    last_failure_code = NULL
WHERE id = $1 AND lease_token = $2 AND status = 'processing'
  AND lease_expires_at > clock_timestamp()`, id, token, delivered)
	if err != nil {
		return err
	}
	count, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if count != 1 {
		return errLeaseLost
	}
	outcome := "processed"
	if delivered {
		outcome = "delivered"
	}
	if err = closeAttempt(ctx, tx, id, token, outcome, "", 0, nil, budget.Valid); err != nil {
		return err
	}
	return tx.Commit()
}

// Claim is embedded unchanged; only successful HTTP completion differs.
type deliveryLeaseStore struct{ postgresStore }

func (s deliveryLeaseStore) Complete(ctx context.Context, id, token string) error {
	return s.postgresStore.CompleteDelivery(ctx, id, token)
}
