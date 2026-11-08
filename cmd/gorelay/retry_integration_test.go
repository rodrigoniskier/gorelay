package main

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
)

func retryTestPolicy() RetryPolicy {
	return RetryPolicy{MaxAttempts: 5, BaseDelay: time.Millisecond, MaxDelay: 2 * time.Millisecond}
}
func retryTestStore(db *sql.DB, p RetryPolicy) retryDeliveryStore {
	return retryDeliveryStore{deliveryLeaseStore{postgresStore{db: db}}, p}
}
func forceRetryDue(t *testing.T, db *sql.DB, id string) {
	t.Helper()
	if _, err := db.ExecContext(t.Context(), `UPDATE events SET next_attempt_at=clock_timestamp()-interval '1 second' WHERE id=$1 AND status='retry_wait'`, id); err != nil {
		t.Fatal(err)
	}
}
func attemptOutcomes(t *testing.T, db *sql.DB, id string) []string {
	t.Helper()
	rows, err := db.QueryContext(t.Context(), `SELECT outcome FROM event_attempts WHERE event_id=$1 ORDER BY attempt_number`, id)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var got []string
	for rows.Next() {
		var v string
		if err = rows.Scan(&v); err != nil {
			t.Fatal(err)
		}
		got = append(got, v)
	}
	if err = rows.Err(); err != nil {
		t.Fatal(err)
	}
	return got
}
func assertRetryCount(t *testing.T, db *sql.DB, id string, want int) {
	t.Helper()
	var count, history int
	if err := db.QueryRowContext(t.Context(), `SELECT processing_attempts,(SELECT count(*) FROM event_attempts WHERE event_id=events.id) FROM events WHERE id=$1`, id).Scan(&count, &history); err != nil || count != want || history != want {
		t.Fatalf("counter %d history %d want %d err %v", count, history, want, err)
	}
}

func TestRetry503503200(t *testing.T) {
	db := leaseDB(t)
	e := insertLeaseEvent(t, db, "503-sequence")
	var calls atomic.Int64
	target := localDeliveryServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get(DeliveryIDHeader) != e.ID {
			t.Error("unstable delivery ID")
		}
		if calls.Add(1) <= 2 {
			w.WriteHeader(503)
		} else {
			w.WriteHeader(200)
		}
	}))
	cfg := deliveryWorkerConfig()
	p := deliveryProcessor(t, target.URL, cfg)
	cancel, done := startDeliveryPool(t, retryTestStore(db, retryTestPolicy()), p, cfg, nil)
	awaitDeliveryState(t, db, e.ID, "delivered")
	cancel()
	awaitWorkerExit(t, done)
	if calls.Load() != 3 {
		t.Fatalf("HTTP calls %d", calls.Load())
	}
	assertRetryCount(t, db, e.ID, 3)
	if got := strings.Join(attemptOutcomes(t, db, e.ID), ","); got != "retry_scheduled,retry_scheduled,delivered" {
		t.Fatalf("outcomes %s", got)
	}
}
func TestRetry429AfterPersistedAndBounded(t *testing.T) {
	db := leaseDB(t)
	e := insertLeaseEvent(t, db, "429-after")
	var calls atomic.Int64
	target := localDeliveryServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			w.Header().Set("Retry-After", "120")
			w.WriteHeader(429)
		} else {
			w.WriteHeader(200)
		}
	}))
	cfg := deliveryWorkerConfig()
	httpProcessor := deliveryProcessor(t, target.URL, cfg)
	policy := RetryPolicy{MaxAttempts: 5, BaseDelay: time.Second, MaxDelay: 2 * time.Second}
	store := retryTestStore(db, policy)
	first, err := store.Claim(t.Context(), cfg.LeaseDuration)
	if err != nil {
		t.Fatal(err)
	}
	deliveryErr := httpProcessor.Process(t.Context(), first.Event)
	if deliveryErr == nil {
		t.Fatal("429 accepted as success")
	}
	if err = store.Fail(t.Context(), first, deliveryErr); err != nil {
		t.Fatal(err)
	}
	var matching bool
	var delay float64
	if err = db.QueryRowContext(t.Context(), `SELECT e.next_attempt_at=a.next_attempt_at,extract(epoch FROM(e.next_attempt_at-a.finished_at)) FROM events e JOIN event_attempts a ON a.event_id=e.id AND a.attempt_number=1 WHERE e.id=$1`, e.ID).Scan(&matching, &delay); err != nil || !matching || delay < 1.95 || delay > 2.05 {
		t.Fatalf("Retry-After schedule match=%v delay=%f err=%v", matching, delay, err)
	}
	if _, err = store.Claim(t.Context(), cfg.LeaseDuration); !errors.Is(err, errNoClaimableEvent) {
		t.Fatalf("retry sent before persisted time %v", err)
	}
	forceRetryDue(t, db, e.ID)
	second, err := store.Claim(t.Context(), cfg.LeaseDuration)
	if err != nil {
		t.Fatal(err)
	}
	if err = httpProcessor.Process(t.Context(), second.Event); err != nil {
		t.Fatal(err)
	}
	if err = store.Complete(t.Context(), second.Event.ID, second.Token); err != nil {
		t.Fatal(err)
	}
	assertRetryCount(t, db, e.ID, 2)
}
func TestRetry400DeadLetterWithoutAutomaticRepeat(t *testing.T) {
	db := leaseDB(t)
	e := insertLeaseEvent(t, db, "terminal400")
	var calls atomic.Int64
	target := localDeliveryServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls.Add(1); w.WriteHeader(400) }))
	cfg := deliveryWorkerConfig()
	p := deliveryProcessor(t, target.URL, cfg)
	store := retryTestStore(db, retryTestPolicy())
	cancel, done := startDeliveryPool(t, store, p, cfg, nil)
	awaitDeliveryState(t, db, e.ID, "dead_letter")
	cancel()
	awaitWorkerExit(t, done)
	if calls.Load() != 1 {
		t.Fatalf("terminal repeated %d", calls.Load())
	}
	assertRetryCount(t, db, e.ID, 1)
	var intact bool
	if err := db.QueryRowContext(t.Context(), `SELECT dead_letter_reason='terminal_failure' AND last_failure_code='http_terminal' AND dead_letter_at IS NOT NULL AND next_attempt_at IS NULL AND lease_token IS NULL AND payload='{"value":1}'::jsonb FROM events WHERE id=$1`, e.ID).Scan(&intact); err != nil || !intact {
		t.Fatalf("DLQ state %v %v", intact, err)
	}
	for range 3 {
		if _, err := store.Claim(t.Context(), cfg.LeaseDuration); !errors.Is(err, errNoClaimableEvent) {
			t.Fatalf("DLQ automatically reclaimed %v", err)
		}
	}
}
func TestRetryTimeoutThenSuccess(t *testing.T) {
	db := leaseDB(t)
	e := insertLeaseEvent(t, db, "timeout-retry")
	var calls atomic.Int64
	target := localDeliveryServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Error(err)
		}
		if calls.Add(1) == 1 {
			<-r.Context().Done()
			return
		}
		w.WriteHeader(200)
	}))
	cfg := deliveryWorkerConfig()
	p, err := NewHTTPProcessor(HTTPProcessorConfig{Worker: cfg, Destinations: map[string]string{"local-fixture": target.URL}, RequestTimeout: 80 * time.Millisecond, ConnectTimeout: 20 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(p.Close)
	cancel, done := startDeliveryPool(t, retryTestStore(db, retryTestPolicy()), p, cfg, nil)
	awaitDeliveryState(t, db, e.ID, "delivered")
	cancel()
	awaitWorkerExit(t, done)
	if calls.Load() != 2 {
		t.Fatalf("timeout calls %d", calls.Load())
	}
	assertRetryCount(t, db, e.ID, 2)
	var failure string
	if err = db.QueryRowContext(t.Context(), `SELECT failure_code FROM event_attempts WHERE event_id=$1 AND attempt_number=1`, e.ID).Scan(&failure); err != nil || failure != "timeout" {
		t.Fatalf("timeout history %s %v", failure, err)
	}
}
func TestRetryFiveFailuresExhaustBudget(t *testing.T) {
	db := leaseDB(t)
	e := insertLeaseEvent(t, db, "five-failures")
	var calls atomic.Int64
	target := localDeliveryServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls.Add(1); w.WriteHeader(503) }))
	cfg := deliveryWorkerConfig()
	p := deliveryProcessor(t, target.URL, cfg)
	store := retryTestStore(db, retryTestPolicy())
	cancel, done := startDeliveryPool(t, store, p, cfg, nil)
	awaitDeliveryState(t, db, e.ID, "dead_letter")
	cancel()
	awaitWorkerExit(t, done)
	if calls.Load() != 5 {
		t.Fatalf("budget HTTP calls %d", calls.Load())
	}
	assertRetryCount(t, db, e.ID, 5)
	if got := strings.Join(attemptOutcomes(t, db, e.ID), ","); got != "retry_scheduled,retry_scheduled,retry_scheduled,retry_scheduled,dead_letter" {
		t.Fatalf("budget outcomes %s", got)
	}
	var reason string
	var max int
	if err := db.QueryRowContext(t.Context(), `SELECT dead_letter_reason,retry_max_attempts FROM events WHERE id=$1`, e.ID).Scan(&reason, &max); err != nil || reason != "attempts_exhausted" || max != 5 {
		t.Fatalf("budget reason %s max %d err %v", reason, max, err)
	}
	if _, err := store.Claim(t.Context(), cfg.LeaseDuration); !errors.Is(err, errNoClaimableEvent) {
		t.Fatalf("sixth claim %v", err)
	}
}
func TestRetryFencingFailureAndConcurrentSchedule(t *testing.T) {
	db := leaseDB(t)
	e := insertLeaseEvent(t, db, "failure-fencing")
	store := retryTestStore(db, RetryPolicy{MaxAttempts: 5, BaseDelay: time.Second, MaxDelay: time.Minute})
	first, err := store.Claim(t.Context(), 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	decision := FailureDecision{Code: "http_transient", HTTPStatus: 503, Delay: time.Second}
	start := make(chan struct{})
	results := make(chan error, 2)
	var wg sync.WaitGroup
	for range 2 {
		wg.Go(func() { <-start; results <- (postgresStore{db: db}).RecordFailure(t.Context(), first, decision) })
	}
	close(start)
	wg.Wait()
	close(results)
	wins, lost := 0, 0
	for err := range results {
		if err == nil {
			wins++
		} else if errors.Is(err, errLeaseLost) {
			lost++
		} else {
			t.Fatal(err)
		}
	}
	if wins != 1 || lost != 1 {
		t.Fatalf("schedules won %d lost %d", wins, lost)
	}
	forceRetryDue(t, db, e.ID)
	second, err := store.Claim(t.Context(), 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err = (postgresStore{db: db}).RecordFailure(t.Context(), first, decision); !errors.Is(err, errLeaseLost) {
		t.Fatalf("old worker scheduled newer attempt %v", err)
	}
	if err = store.Complete(t.Context(), e.ID, first.Token); !errors.Is(err, errLeaseLost) {
		t.Fatalf("old worker confirmed %v", err)
	}
	if err = store.Complete(t.Context(), e.ID, second.Token); err != nil {
		t.Fatal(err)
	}
	assertRetryCount(t, db, e.ID, 2)
}
func TestRetryCrashSlotsCannotExceedPinnedBudget(t *testing.T) {
	db := leaseDB(t)
	e := insertLeaseEvent(t, db, "crash-budget")
	policy := RetryPolicy{MaxAttempts: 2, BaseDelay: time.Second, MaxDelay: time.Minute}
	store := retryTestStore(db, policy)
	first, err := store.Claim(t.Context(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	expireLease(t, db, e.ID)
	// A differently configured worker cannot increase the already-pinned budget.
	other := retryTestStore(db, RetryPolicy{MaxAttempts: 100, BaseDelay: time.Second, MaxDelay: time.Minute})
	second, err := other.Claim(t.Context(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if second.Attempt != 2 {
		t.Fatal("crash slot not counted")
	}
	expireLease(t, db, e.ID)
	if _, err = other.Claim(t.Context(), time.Second); !errors.Is(err, errNoClaimableEvent) {
		t.Fatalf("third crash claim accepted %v", err)
	}
	awaitDeliveryState(t, db, e.ID, "dead_letter")
	assertRetryCount(t, db, e.ID, 2)
	if got := strings.Join(attemptOutcomes(t, db, e.ID), ","); got != "lease_expired,lease_expired" {
		t.Fatalf("unknown crash outcomes %s", got)
	}
	if err = store.Complete(t.Context(), e.ID, first.Token); !errors.Is(err, errLeaseLost) {
		t.Fatal("dead-letter accepted stale completion")
	}
}
func TestRetryConcurrentClaimAndCompletionVersusFailure(t *testing.T) {
	db := leaseDB(t)
	e := insertLeaseEvent(t, db, "race-outcome")
	store := retryTestStore(db, retryTestPolicy())
	start := make(chan struct{})
	claims := make(chan Lease, 2)
	errs := make(chan error, 2)
	var wg sync.WaitGroup
	for range 2 {
		wg.Go(func() { <-start; l, err := store.Claim(t.Context(), 5*time.Second); claims <- l; errs <- err })
	}
	close(start)
	wg.Wait()
	close(claims)
	close(errs)
	wins, empty := 0, 0
	var lease Lease
	for err := range errs {
		if err == nil {
			wins++
		} else if errors.Is(err, errNoClaimableEvent) {
			empty++
		} else {
			t.Fatal(err)
		}
	}
	for l := range claims {
		if l.Token != "" {
			lease = l
		}
	}
	if wins != 1 || empty != 1 {
		t.Fatalf("retry claim winners %d empty %d", wins, empty)
	}
	results := make(chan error, 2)
	start = make(chan struct{})
	wg.Go(func() { <-start; results <- store.Complete(t.Context(), e.ID, lease.Token) })
	wg.Go(func() {
		<-start
		results <- (postgresStore{db: db}).RecordFailure(t.Context(), lease, FailureDecision{Code: "http_transient", HTTPStatus: 503, Delay: time.Second})
	})
	close(start)
	wg.Wait()
	close(results)
	wins, lost := 0, 0
	for err := range results {
		if err == nil {
			wins++
		} else if errors.Is(err, errLeaseLost) {
			lost++
		} else {
			t.Fatal(err)
		}
	}
	if wins != 1 || lost != 1 {
		t.Fatalf("outcome race won %d lost %d", wins, lost)
	}
	assertRetryCount(t, db, e.ID, 1)
	outcomes := attemptOutcomes(t, db, e.ID)
	if len(outcomes) != 1 || (outcomes[0] != "delivered" && outcomes[0] != "retry_scheduled") {
		t.Fatalf("conflicting history %v", outcomes)
	}
}

func TestRetryClaimAndHistoryAreAtomic(t *testing.T) {
	db := leaseDB(t)
	e := insertLeaseEvent(t, db, "history-insert-fails")
	if _, err := db.ExecContext(t.Context(), `ALTER TABLE event_attempts ADD CONSTRAINT fixture_reject_history CHECK(false)`); err != nil {
		t.Fatal(err)
	}
	if _, err := retryTestStore(db, retryTestPolicy()).Claim(t.Context(), time.Second); err == nil {
		t.Fatal("claim succeeded without history")
	}
	var untouched bool
	if err := db.QueryRowContext(t.Context(), `SELECT status='pending' AND processing_attempts=0 AND retry_max_attempts IS NULL AND lease_token IS NULL FROM events WHERE id=$1`, e.ID).Scan(&untouched); err != nil || !untouched {
		t.Fatalf("partial claim persisted %v", err)
	}
	if got := attemptOutcomes(t, db, e.ID); len(got) != 0 {
		t.Fatal("history persisted despite failed claim")
	}
}
func TestRetryHistoryFailureAfterHTTP200DuplicatesStableID(t *testing.T) {
	db := leaseDB(t)
	e := insertLeaseEvent(t, db, "ledger-window")
	var mu sync.Mutex
	var ids []string
	target := localDeliveryServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		ids = append(ids, r.Header.Get(DeliveryIDHeader))
		mu.Unlock()
		w.WriteHeader(200)
	}))
	if _, err := db.ExecContext(t.Context(), `ALTER TABLE event_attempts ADD CONSTRAINT fixture_reject_delivered_history CHECK(outcome<>'delivered')`); err != nil {
		t.Fatal(err)
	}
	cfg := deliveryWorkerConfig()
	p := deliveryProcessor(t, target.URL, cfg)
	failed := make(chan struct{}, 1)
	store := retryTestStore(db, retryTestPolicy())
	cancel, done := startDeliveryPool(t, store, p, cfg, func(error) {
		select {
		case failed <- struct{}{}:
		default:
		}
	})
	awaitWorkerSignal(t, failed)
	assertNoDelivery(t, db, e.ID)
	if _, err := db.ExecContext(t.Context(), `ALTER TABLE event_attempts DROP CONSTRAINT fixture_reject_delivered_history`); err != nil {
		t.Fatal(err)
	}
	expireLease(t, db, e.ID)
	awaitDeliveryState(t, db, e.ID, "delivered")
	cancel()
	awaitWorkerExit(t, done)
	mu.Lock()
	observed := append([]string(nil), ids...)
	mu.Unlock()
	if len(observed) != 2 || observed[0] != e.ID || observed[1] != e.ID {
		t.Fatalf("duplicate window IDs count %d", len(observed))
	}
	assertRetryCount(t, db, e.ID, 2)
	if got := strings.Join(attemptOutcomes(t, db, e.ID), ","); got != "lease_expired,delivered" {
		t.Fatalf("ambiguous acceptance history %s", got)
	}
}
func TestRetryMigrationPreservesExistingCounterAndNoInventedHistory(t *testing.T) {
	db := integrationDB(t)
	ctx := t.Context()
	if _, err := db.ExecContext(ctx, `CREATE TABLE schema_migrations(version text PRIMARY KEY,checksum text NOT NULL,applied_at timestamptz NOT NULL DEFAULT now())`); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"0001_events.sql", "0002_event_destination.sql", "0003_processing_leases.sql", "0004_http_delivery.sql"} {
		b, err := migrationFiles.ReadFile("migrations/" + name)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = db.ExecContext(ctx, string(b)); err != nil {
			t.Fatal(err)
		}
		if _, err = db.ExecContext(ctx, `INSERT INTO schema_migrations(version,checksum) VALUES($1,$2)`, name, fmt.Sprintf("%x", sha256.Sum256(b))); err != nil {
			t.Fatal(err)
		}
	}
	id, _ := newEventID()
	token, _ := newEventID()
	if _, err := db.ExecContext(ctx, `INSERT INTO events(id,idempotency_key,event_type,payload,request_hash,destination_id,status,processing_attempts,lease_token,lease_expires_at) VALUES($1,'old-count','fixture','{}',$2,'local-fixture','processing',9,$3,clock_timestamp()-interval '1 second')`, id, make([]byte, 32), token); err != nil {
		t.Fatal(err)
	}
	if err := migrate(ctx, db); err != nil {
		t.Fatal(err)
	}
	if got := attemptOutcomes(t, db, id); len(got) != 0 {
		t.Fatal("migration fabricated old attempt history")
	}
	if _, err := retryTestStore(db, DefaultRetryPolicy()).Claim(ctx, time.Second); !errors.Is(err, errNoClaimableEvent) {
		t.Fatalf("old exhausted event got another attempt %v", err)
	}
	awaitDeliveryState(t, db, id, "dead_letter")
	var count int
	if err := db.QueryRowContext(ctx, `SELECT processing_attempts FROM events WHERE id=$1`, id).Scan(&count); err != nil || count != 9 {
		t.Fatalf("old counter reset %d %v", count, err)
	}
	if got := attemptOutcomes(t, db, id); len(got) != 0 {
		t.Fatal("recovery fabricated unknowable historical attempts")
	}
}

// Helper modes exercise a pending retry from fresh OS processes, not a Go-only
// store reconstruction. Outside helper mode this returns, never skips a SQL test.
func TestRetryProcessHelper(t *testing.T) {
	mode := os.Getenv("GORELAY_TEST_RETRY_MODE")
	if mode == "" {
		return
	}
	cfg, err := pgx.ParseConfig(os.Getenv("GORELAY_TEST_RETRY_DSN"))
	if err != nil {
		t.Fatal("retry helper DSN")
	}
	cfg.RuntimeParams["search_path"] = os.Getenv("GORELAY_TEST_RETRY_SCHEMA")
	db := sql.OpenDB(stdlib.GetConnector(*cfg))
	defer db.Close()
	store := retryTestStore(db, DefaultRetryPolicy())
	id := os.Getenv("GORELAY_TEST_RETRY_EVENT")
	if mode == "probe" {
		if _, err := store.Claim(t.Context(), time.Second); !errors.Is(err, errNoClaimableEvent) {
			t.Fatalf("future retry eligible in fresh process %v", err)
		}
		var state string
		if err := db.QueryRowContext(t.Context(), `SELECT status FROM events WHERE id=$1`, id).Scan(&state); err != nil || state != "retry_wait" {
			t.Fatal("retry state lost after restart")
		}
		return
	}
	worker := deliveryWorkerConfig()
	processor, err := NewHTTPProcessor(HTTPProcessorConfig{Worker: worker, Destinations: map[string]string{"local-fixture": os.Getenv("GORELAY_TEST_RETRY_TARGET")}, RequestTimeout: time.Second, ConnectTimeout: 100 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer processor.Close()
	cancel, done := startDeliveryPool(t, store, processor, worker, nil)
	awaitDeliveryState(t, db, id, "delivered")
	cancel()
	awaitWorkerExit(t, done)
}
func TestRestartWithPendingRetry(t *testing.T) {
	db := leaseDB(t)
	e := insertLeaseEvent(t, db, "restart-retry")
	var calls atomic.Int64
	target := localDeliveryServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get(DeliveryIDHeader) != e.ID {
			t.Error("delivery ID changed across process")
		}
		if calls.Add(1) == 1 {
			w.WriteHeader(503)
		} else {
			w.WriteHeader(200)
		}
	}))
	worker := deliveryWorkerConfig()
	processor := deliveryProcessor(t, target.URL, worker)
	store := retryTestStore(db, RetryPolicy{MaxAttempts: 5, BaseDelay: time.Minute, MaxDelay: time.Minute})
	lease, err := store.Claim(t.Context(), worker.LeaseDuration)
	if err != nil {
		t.Fatal(err)
	}
	failure := processor.Process(t.Context(), lease.Event)
	if failure == nil {
		t.Fatal("initial failure missing")
	}
	if err = store.Fail(t.Context(), lease, failure); err != nil {
		t.Fatal(err)
	}
	var schema string
	var before time.Time
	if err = db.QueryRowContext(t.Context(), `SHOW search_path`).Scan(&schema); err != nil {
		t.Fatal(err)
	}
	if err = db.QueryRowContext(t.Context(), `SELECT next_attempt_at FROM events WHERE id=$1`, e.ID).Scan(&before); err != nil {
		t.Fatal(err)
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	runChild := func(mode string) {
		t.Helper()
		ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
		defer cancel()
		cmd := exec.CommandContext(ctx, executable, "-test.run=^TestRetryProcessHelper$", "-test.timeout=12s")
		cmd.Env = append(os.Environ(), "GORELAY_TEST_RETRY_MODE="+mode, "GORELAY_TEST_RETRY_DSN="+os.Getenv("TEST_DATABASE_URL"), "GORELAY_TEST_RETRY_SCHEMA="+schema, "GORELAY_TEST_RETRY_EVENT="+e.ID, "GORELAY_TEST_RETRY_TARGET="+target.URL)
		if err := cmd.Run(); err != nil {
			t.Fatalf("retry subprocess %s failed: %v", mode, err)
		}
	}
	runChild("probe")
	var after time.Time
	if err = db.QueryRowContext(t.Context(), `SELECT next_attempt_at FROM events WHERE id=$1`, e.ID).Scan(&after); err != nil || !before.Equal(after) {
		t.Fatal("pending schedule recomputed by restart")
	}
	forceRetryDue(t, db, e.ID)
	runChild("deliver")
	if calls.Load() != 2 {
		t.Fatalf("restart HTTP count %d", calls.Load())
	}
	assertRetryCount(t, db, e.ID, 2)
}
func TestRetryRuntime503Then200(t *testing.T) {
	db := leaseDB(t)
	e := insertLeaseEvent(t, db, "runtime-retry")
	var calls atomic.Int64
	target := localDeliveryServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			w.WriteHeader(503)
		} else {
			w.WriteHeader(200)
		}
	}))
	var schema string
	if err := db.QueryRowContext(t.Context(), `SHOW search_path`).Scan(&schema); err != nil {
		t.Fatal(err)
	}
	dsn := os.Getenv("TEST_DATABASE_URL")
	if strings.Contains(dsn, "://") {
		u, err := url.Parse(dsn)
		if err != nil {
			t.Fatal("DSN config")
		}
		q := u.Query()
		q.Set("search_path", schema)
		u.RawQuery = q.Encode()
		dsn = u.String()
	} else {
		dsn += " search_path=" + schema
	}
	mapping, _ := json.Marshal(map[string]string{"local-fixture": target.URL})
	for key, value := range map[string]string{"DATABASE_URL": dsn, "GORELAY_WORKERS": "2", "GORELAY_HTTP_DEMO": "1", "GORELAY_DESTINATIONS": string(mapping), "GORELAY_LEASE_DURATION": "5s", "GORELAY_WORKER_DB_TIMEOUT": "500ms", "GORELAY_POLL_INTERVAL": "10ms", "GORELAY_HTTP_TIMEOUT": "1s", "GORELAY_HTTP_CONNECT_TIMEOUT": "100ms", "GORELAY_MAX_ATTEMPTS": "5", "GORELAY_RETRY_BASE_DELAY": "1ms", "GORELAY_RETRY_MAX_DELAY": "2ms"} {
		t.Setenv(key, value)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- run(ctx, "127.0.0.1:0"); close(done) }()
	t.Cleanup(func() { cancel(); awaitWorkerExit(t, done) })
	awaitDeliveryState(t, db, e.ID, "delivered")
	cancel()
	awaitWorkerExit(t, done)
	if calls.Load() != 2 {
		t.Fatalf("runtime retry calls %d", calls.Load())
	}
	assertRetryCount(t, db, e.ID, 2)
}
