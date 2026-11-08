package main

import (
	"context"
	"database/sql"
	"errors"
	"net/http"
	"time"
)

type dashboardEvent struct {
	ID            string     `json:"id"`
	Type          string     `json:"type"`
	DestinationID *string    `json:"destination_id"`
	Status        string     `json:"status"`
	Attempts      int64      `json:"attempts"`
	NextAttemptAt *time.Time `json:"next_attempt_at"`
	CreatedAt     time.Time  `json:"created_at"`
}
type dashboardOverview struct {
	Total                  int64            `json:"total"`
	Pending                int64            `json:"pending"`
	Processing             int64            `json:"processing"`
	RetryWait              int64            `json:"retry_wait"`
	Delivered              int64            `json:"delivered"`
	DeadLetter             int64            `json:"dead_letter"`
	Processed              int64            `json:"processed"`
	ActiveLeases           int64            `json:"active_leases"`
	ExpiredLeases          int64            `json:"expired_leases"`
	SuccessNumerator       int64            `json:"success_numerator"`
	SuccessDenominator     int64            `json:"success_denominator"`
	SuccessRate            *float64         `json:"success_rate"`
	SuccessfulProcessingMS *float64         `json:"successful_processing_ms"`
	Events                 []dashboardEvent `json:"events"`
}
type dashboardAttempt struct {
	Number        int64      `json:"number"`
	StartedAt     time.Time  `json:"started_at"`
	FinishedAt    *time.Time `json:"finished_at"`
	Outcome       string     `json:"outcome"`
	FailureCode   *string    `json:"failure_code"`
	HTTPStatus    *int       `json:"http_status"`
	NextAttemptAt *time.Time `json:"next_attempt_at"`
}
type dashboardDetail struct {
	Event    dashboardEvent     `json:"event"`
	Attempts []dashboardAttempt `json:"attempts"`
}

const dashboardEventColumns = `id,event_type,destination_id,status,processing_attempts,next_attempt_at,created_at`

type dashboardScanner interface{ Scan(...any) error }

func scanDashboardEvent(row dashboardScanner) (dashboardEvent, error) {
	var e dashboardEvent
	err := row.Scan(&e.ID, &e.Type, &e.DestinationID, &e.Status, &e.Attempts, &e.NextAttemptAt, &e.CreatedAt)
	return e, err
}

// A repeatable-read snapshot keeps totals and the bounded recent list coherent.
// Counts include historical generic "processed" separately, never as delivered.
func readDashboardOverview(ctx context.Context, db *sql.DB) (dashboardOverview, error) {
	o := dashboardOverview{Events: make([]dashboardEvent, 0)}
	tx, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true, Isolation: sql.LevelRepeatableRead})
	if err != nil {
		return o, err
	}
	defer tx.Rollback()
	err = tx.QueryRowContext(ctx, `SELECT count(*),count(*) FILTER(WHERE status='pending'),count(*) FILTER(WHERE status='processing'),count(*) FILTER(WHERE status='retry_wait'),count(*) FILTER(WHERE status='delivered'),count(*) FILTER(WHERE status='dead_letter'),count(*) FILTER(WHERE status='processed'),count(*) FILTER(WHERE status='processing' AND lease_expires_at>statement_timestamp()),count(*) FILTER(WHERE status='processing' AND lease_expires_at<=statement_timestamp()) FROM events`).Scan(&o.Total, &o.Pending, &o.Processing, &o.RetryWait, &o.Delivered, &o.DeadLetter, &o.Processed, &o.ActiveLeases, &o.ExpiredLeases)
	if err != nil {
		return o, err
	}
	o.SuccessNumerator = o.Delivered
	o.SuccessDenominator = o.Delivered + o.DeadLetter
	if o.SuccessDenominator > 0 {
		v := 100 * float64(o.Delivered) / float64(o.SuccessDenominator)
		o.SuccessRate = &v
	}
	// This is the mean claim-to-confirm duration of successful recorded attempts,
	// not wire latency, throughput, or a fabricated end-to-end measurement.
	err = tx.QueryRowContext(ctx, `SELECT avg(extract(epoch FROM (finished_at-started_at))*1000)::double precision FROM event_attempts WHERE outcome='delivered' AND finished_at>=started_at`).Scan(&o.SuccessfulProcessingMS)
	if err != nil {
		return o, err
	}
	rows, err := tx.QueryContext(ctx, `SELECT `+dashboardEventColumns+` FROM events ORDER BY created_at DESC,id DESC LIMIT 100`)
	if err != nil {
		return o, err
	}
	for rows.Next() {
		e, eerr := scanDashboardEvent(rows)
		if eerr != nil {
			rows.Close()
			return o, eerr
		}
		o.Events = append(o.Events, e)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return o, err
	}
	return o, tx.Commit()
}
func readDashboardDetail(ctx context.Context, db *sql.DB, id string) (dashboardDetail, error) {
	d := dashboardDetail{Attempts: make([]dashboardAttempt, 0)}
	tx, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true, Isolation: sql.LevelRepeatableRead})
	if err != nil {
		return d, err
	}
	defer tx.Rollback()
	d.Event, err = scanDashboardEvent(tx.QueryRowContext(ctx, `SELECT `+dashboardEventColumns+` FROM events WHERE id=$1`, id))
	if err != nil {
		return d, err
	}
	rows, err := tx.QueryContext(ctx, `SELECT attempt_number,started_at,finished_at,outcome,failure_code,http_status,next_attempt_at FROM event_attempts WHERE event_id=$1 ORDER BY attempt_number DESC LIMIT 100`, id)
	if err != nil {
		return d, err
	}
	for rows.Next() {
		var a dashboardAttempt
		if err = rows.Scan(&a.Number, &a.StartedAt, &a.FinishedAt, &a.Outcome, &a.FailureCode, &a.HTTPStatus, &a.NextAttemptAt); err != nil {
			rows.Close()
			return d, err
		}
		d.Attempts = append(d.Attempts, a)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return d, err
	}
	return d, tx.Commit()
}
func registerDashboardData(mux *http.ServeMux, db *sql.DB) {
	mux.HandleFunc("GET /dashboard/api/overview", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		if db == nil {
			eventError(w, 503, "dashboard storage unavailable")
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
		defer cancel()
		o, err := readDashboardOverview(ctx, db)
		if err != nil {
			eventError(w, 503, "dashboard storage unavailable")
			return
		}
		writeJSON(w, 200, o)
	})
	mux.HandleFunc("GET /dashboard/api/events/{id}", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		id := r.PathValue("id")
		if !validEventID(id) {
			eventError(w, 400, "invalid event id")
			return
		}
		if db == nil {
			eventError(w, 503, "dashboard storage unavailable")
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
		defer cancel()
		d, err := readDashboardDetail(ctx, db, id)
		if errors.Is(err, sql.ErrNoRows) {
			eventError(w, 404, "event not found")
			return
		}
		if err != nil {
			eventError(w, 503, "dashboard storage unavailable")
			return
		}
		writeJSON(w, 200, d)
	})
}
