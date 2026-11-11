package main

import (
	"bufio"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
)

func leaseDB(t *testing.T) *sql.DB {
	t.Helper()
	db := integrationDB(t)
	if err := migrate(t.Context(), db); err != nil {
		t.Fatal(err)
	}
	return db
}
func insertLeaseEvent(t *testing.T, db *sql.DB, key string) Event {
	t.Helper()
	e, created, err := (postgresStore{db: db}).Create(t.Context(), key, eventInput{DestinationID: "local-fixture", Type: "fixture", Payload: json.RawMessage(`{"value":1}`)})
	if err != nil || !created {
		t.Fatalf("insert fixture %v", err)
	}
	return e
}
func expireLease(t *testing.T, db *sql.DB, id string) {
	t.Helper()
	if _, err := db.ExecContext(t.Context(), `UPDATE events SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE id=$1`, id); err != nil {
		t.Fatal(err)
	}
}

func TestTwoWorkersOneEvent(t *testing.T) {
	db := leaseDB(t)
	event := insertLeaseEvent(t, db, "competition")
	start := make(chan struct{})
	type result struct {
		lease Lease
		err   error
	}
	results := make(chan result, 2)
	var wg sync.WaitGroup
	for range 2 {
		wg.Go(func() {
			<-start
			l, err := (postgresStore{db: db}).Claim(t.Context(), 5*time.Second)
			results <- result{l, err}
		})
	}
	close(start)
	wg.Wait()
	close(results)
	won, empty := 0, 0
	for r := range results {
		if r.err == nil {
			won++
			if r.lease.Event.ID != event.ID || r.lease.Attempt != 1 || !validEventID(r.lease.Token) {
				t.Fatal("wrong claimed event")
			}
		} else if errors.Is(r.err, errNoClaimableEvent) {
			empty++
		} else {
			t.Fatal(r.err)
		}
	}
	if won != 1 || empty != 1 {
		t.Fatalf("winners %d empty %d", won, empty)
	}
	var attempts int
	if err := db.QueryRowContext(t.Context(), `SELECT processing_attempts FROM events WHERE id=$1`, event.ID).Scan(&attempts); err != nil || attempts != 1 {
		t.Fatalf("attempts %d %v", attempts, err)
	}
}
func TestLeaseRecoveryAndFencing(t *testing.T) {
	db := leaseDB(t)
	store := postgresStore{db: db}
	e := insertLeaseEvent(t, db, "recovery")
	first, err := store.Claim(t.Context(), 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = store.Claim(t.Context(), time.Second); !errors.Is(err, errNoClaimableEvent) {
		t.Fatalf("active lease reclaimed %v", err)
	}
	expireLease(t, db, e.ID)
	if err = store.Complete(t.Context(), e.ID, first.Token); !errors.Is(err, errLeaseLost) {
		t.Fatalf("expired completion %v", err)
	}
	second, err := store.Claim(t.Context(), 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if second.Event.ID != e.ID || second.Token == first.Token || second.Attempt != 2 {
		t.Fatal("recovery did not rotate ownership")
	}
	if err = store.Complete(t.Context(), e.ID, first.Token); !errors.Is(err, errLeaseLost) {
		t.Fatalf("stale completion %v", err)
	}
	if err = store.Complete(t.Context(), e.ID, second.Token); err != nil {
		t.Fatal(err)
	}
	if err = store.Complete(t.Context(), e.ID, second.Token); !errors.Is(err, errLeaseLost) {
		t.Fatalf("double completion %v", err)
	}
	if _, err = store.Claim(t.Context(), time.Second); !errors.Is(err, errNoClaimableEvent) {
		t.Fatalf("processed reclaimed %v", err)
	}
	var clean bool
	if err = db.QueryRowContext(t.Context(), `SELECT status='processed' AND lease_token IS NULL AND lease_expires_at IS NULL AND processed_at IS NOT NULL FROM events WHERE id=$1`, e.ID).Scan(&clean); err != nil || !clean {
		t.Fatalf("processed state %v %v", clean, err)
	}
}
func TestClaimSkipsLockedRow(t *testing.T) {
	db := leaseDB(t)
	first := insertLeaseEvent(t, db, "locked")
	second := insertLeaseEvent(t, db, "available")
	tx, err := db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	var locked string
	if err = tx.QueryRowContext(t.Context(), `SELECT id FROM events WHERE id=$1 FOR UPDATE`, first.ID).Scan(&locked); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	lease, err := (postgresStore{db: db}).Claim(ctx, time.Second)
	if err != nil || lease.Event.ID != second.ID {
		t.Fatalf("locked row not skipped %v", err)
	}
}
func TestConcurrentCompletion(t *testing.T) {
	db := leaseDB(t)
	insertLeaseEvent(t, db, "completion")
	store := postgresStore{db: db}
	lease, err := store.Claim(t.Context(), 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	start := make(chan struct{})
	results := make(chan error, 2)
	var wg sync.WaitGroup
	for range 2 {
		wg.Go(func() { <-start; results <- store.Complete(t.Context(), lease.Event.ID, lease.Token) })
	}
	close(start)
	wg.Wait()
	close(results)
	won, lost := 0, 0
	for err := range results {
		if err == nil {
			won++
		} else if errors.Is(err, errLeaseLost) {
			lost++
		} else {
			t.Fatal(err)
		}
	}
	if won != 1 || lost != 1 {
		t.Fatalf("completion winners %d lost %d", won, lost)
	}
}
func TestCompleteChecksExpiryAfterLockWait(t *testing.T) {
	db := leaseDB(t)
	insertLeaseEvent(t, db, "lock-expiry")
	store := postgresStore{db: db}
	lease, err := store.Claim(t.Context(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	tx, err := db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	var id string
	var blocker int
	if err = tx.QueryRowContext(t.Context(), `SELECT id FROM events WHERE id=$1 FOR UPDATE`, lease.Event.ID).Scan(&id); err != nil {
		t.Fatal(err)
	}
	if err = tx.QueryRowContext(t.Context(), `SELECT pg_backend_pid()`).Scan(&blocker); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	result := make(chan error, 1)
	go func() { result <- store.Complete(ctx, id, lease.Token) }()
	deadline := time.Now().Add(3 * time.Second)
	blocked := false
	for time.Now().Before(deadline) {
		var n int
		if err = db.QueryRowContext(ctx, `SELECT count(*) FROM pg_stat_activity WHERE $1=ANY(pg_blocking_pids(pid))`, blocker).Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n > 0 {
			blocked = true
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if !blocked {
		t.Fatal("completion did not wait on held lock")
	}
	expired := false
	for time.Now().Before(deadline) {
		if err = db.QueryRowContext(ctx, `SELECT lease_expires_at<=clock_timestamp() FROM events WHERE id=$1`, id).Scan(&expired); err != nil {
			t.Fatal(err)
		}
		if expired {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if !expired {
		t.Fatal("lease did not expire")
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	select {
	case err = <-result:
		if !errors.Is(err, errLeaseLost) {
			t.Fatalf("holder confirmed after expiry waiting on unchanged row: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("completion stuck")
	}
}
func TestLeaseLegacyExclusion(t *testing.T) {
	db := integrationDB(t)
	ctx := t.Context()
	sql1, err := migrationFiles.ReadFile("migrations/0001_events.sql")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = db.ExecContext(ctx, string(sql1)); err != nil {
		t.Fatal(err)
	}
	if _, err = db.ExecContext(ctx, `CREATE TABLE schema_migrations(version text PRIMARY KEY,checksum text NOT NULL,applied_at timestamptz NOT NULL DEFAULT now())`); err != nil {
		t.Fatal(err)
	}
	if _, err = db.ExecContext(ctx, `INSERT INTO schema_migrations(version,checksum) VALUES('0001_events.sql',$1)`, fmt.Sprintf("%x", sha256.Sum256(sql1))); err != nil {
		t.Fatal(err)
	}
	id, _ := newEventID()
	if _, err = db.ExecContext(ctx, `INSERT INTO events(id,idempotency_key,event_type,payload,request_hash) VALUES($1,'unknown','legacy','{}',$2)`, id, make([]byte, 32)); err != nil {
		t.Fatal(err)
	}
	if err = migrate(ctx, db); err != nil {
		t.Fatal(err)
	}
	if _, err = (postgresStore{db: db}).Claim(ctx, time.Second); !errors.Is(err, errNoClaimableEvent) {
		t.Fatalf("legacy claimed %v", err)
	}
	var untouched bool
	if err = db.QueryRowContext(ctx, `SELECT destination_id IS NULL AND status='pending' AND processing_attempts=0 AND lease_token IS NULL FROM events WHERE id=$1`, id).Scan(&untouched); err != nil || !untouched {
		t.Fatalf("legacy changed %v", err)
	}
}
func TestClaimCancellationAndConstraints(t *testing.T) {
	db := leaseDB(t)
	e := insertLeaseEvent(t, db, "cancel-claim")
	store := postgresStore{db: db}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := store.Claim(ctx, time.Second); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled claim %v", err)
	}
	for _, query := range []string{
		`UPDATE events SET status='processing' WHERE id=$1`,
		`UPDATE events SET processing_attempts=-1 WHERE id=$1`,
		`UPDATE events SET status='processed' WHERE id=$1`,
	} {
		if _, err := db.ExecContext(t.Context(), query, e.ID); err == nil {
			t.Fatal("invalid state accepted")
		}
	}
	for _, duration := range []time.Duration{0, 99 * time.Millisecond, 25 * time.Hour} {
		if _, err := store.Claim(t.Context(), duration); err == nil {
			t.Fatal("invalid lease accepted")
		}
	}
}

// The test binary is also a minimal worker process for the restart test. Outside
// explicit helper mode it returns without opening SQL; it is not a skipped SQL test.
func TestLeaseProcessHelper(t *testing.T) {
	mode := os.Getenv("GORELAY_TEST_WORKER_MODE")
	if mode == "" {
		return
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	connection, err := pgx.ParseConfig(os.Getenv("GORELAY_TEST_WORKER_DSN"))
	if err != nil {
		t.Fatal("helper database config")
	}
	connection.RuntimeParams["search_path"] = os.Getenv("GORELAY_TEST_WORKER_SCHEMA")
	db := sql.OpenDB(stdlib.GetConnector(*connection))
	defer db.Close()
	process := func(ctx context.Context, e Event) error {
		if mode == "hold" {
			fmt.Println("WORKER_CLAIMED")
			<-ctx.Done()
			return ctx.Err()
		}
		return nil
	}
	cfg := WorkerConfig{Workers: 1, LeaseDuration: 5 * time.Second, PollInterval: 5 * time.Millisecond, DBTimeout: time.Second}
	pool, err := NewWorkerPool(postgresStore{db: db}, process, cfg, func(error) {})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- pool.Run(ctx) }()
	if mode == "hold" {
		<-ctx.Done()
		awaitWorkerExit(t, done)
		t.Fatal("holding helper was not killed")
	}
	for ctx.Err() == nil {
		var status string
		if err = db.QueryRowContext(ctx, `SELECT status FROM events WHERE id=$1`, os.Getenv("GORELAY_TEST_WORKER_EVENT")).Scan(&status); err != nil {
			t.Fatal("helper status query")
		}
		if status == "processed" {
			cancel()
			awaitWorkerExit(t, done)
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("restarted helper did not process")
}
func TestWorkerProcessRestart(t *testing.T) {
	db := leaseDB(t)
	e := insertLeaseEvent(t, db, "restart")
	var schema string
	if err := db.QueryRowContext(t.Context(), `SHOW search_path`).Scan(&schema); err != nil {
		t.Fatal(err)
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	newChild := func(mode string) (*exec.Cmd, context.CancelFunc) {
		ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
		cmd := exec.CommandContext(ctx, executable, "-test.run=^TestLeaseProcessHelper$", "-test.timeout=12s")
		cmd.Env = append(os.Environ(), "GORELAY_TEST_WORKER_MODE="+mode, "GORELAY_TEST_WORKER_DSN="+os.Getenv("TEST_DATABASE_URL"), "GORELAY_TEST_WORKER_SCHEMA="+schema, "GORELAY_TEST_WORKER_EVENT="+e.ID)
		return cmd, cancel
	}
	child, cancel := newChild("hold")
	defer cancel()
	stdout, err := child.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err = child.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if child.ProcessState == nil {
			child.Process.Kill()
			child.Wait()
		}
	}()
	ready := make(chan struct{}, 1)
	go func() {
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			if strings.TrimSpace(scanner.Text()) == "WORKER_CLAIMED" {
				ready <- struct{}{}
				return
			}
		}
	}()
	awaitWorkerSignal(t, ready)
	var oldToken string
	if err = db.QueryRowContext(t.Context(), `SELECT lease_token FROM events WHERE id=$1`, e.ID).Scan(&oldToken); err != nil {
		t.Fatal(err)
	}
	if err = child.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	if err = child.Wait(); err == nil {
		t.Fatal("killed worker exited successfully")
	}
	store := postgresStore{db: db}
	if _, err = store.Claim(t.Context(), time.Second); !errors.Is(err, errNoClaimableEvent) {
		t.Fatalf("crash lost durable active lease %v", err)
	}
	expireLease(t, db, e.ID)
	restarted, stop := newChild("finish")
	defer stop()
	if err = restarted.Run(); err != nil {
		t.Fatalf("restarted worker failed: %v", err)
	}
	var status string
	var attempts int
	if err = db.QueryRowContext(t.Context(), `SELECT status,processing_attempts FROM events WHERE id=$1`, e.ID).Scan(&status, &attempts); err != nil || status != "processed" || attempts != 2 {
		t.Fatalf("restart state %s attempts %d err %v", status, attempts, err)
	}
	if err = store.Complete(t.Context(), e.ID, oldToken); !errors.Is(err, errLeaseLost) {
		t.Fatalf("old process token accepted %v", err)
	}
}

type observedLeaseStore struct {
	postgresStore
	claims chan error
}

func (s observedLeaseStore) Claim(ctx context.Context, d time.Duration) (Lease, error) {
	l, err := s.postgresStore.Claim(ctx, d)
	select {
	case s.claims <- err:
	default:
	}
	return l, err
}
func TestTwoRealPoolsOneEventAndCancellation(t *testing.T) {
	db := leaseDB(t)
	e := insertLeaseEvent(t, db, "two-pools")
	claims := make(chan error, 16)
	store := observedLeaseStore{postgresStore{db: db}, claims}
	started := make(chan struct{}, 2)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	cfg := WorkerConfig{Workers: 1, LeaseDuration: 30 * time.Second, PollInterval: 10 * time.Millisecond, DBTimeout: time.Second}
	var callbacks int
	var mu sync.Mutex
	process := func(ctx context.Context, _ Event) error {
		mu.Lock()
		callbacks++
		mu.Unlock()
		started <- struct{}{}
		<-ctx.Done()
		return ctx.Err()
	}
	done := make(chan error, 2)
	for range 2 {
		pool, err := NewWorkerPool(store, process, cfg, func(error) {})
		if err != nil {
			t.Fatal(err)
		}
		pool.shutdownGrace = 50 * time.Millisecond // this test asserts cancellation propagates
		go func() { done <- pool.Run(ctx) }()
	}
	awaitWorkerSignal(t, started)
	empty := false
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	for !empty {
		select {
		case err := <-claims:
			if errors.Is(err, errNoClaimableEvent) {
				empty = true
			} else if err != nil {
				t.Fatal(err)
			}
		case <-deadline.C:
			t.Fatal("second pool did not compete")
		}
	}
	cancel()
	awaitWorkerExit(t, done)
	awaitWorkerExit(t, done)
	mu.Lock()
	count := callbacks
	mu.Unlock()
	if count != 1 {
		t.Fatalf("simultaneous callbacks %d", count)
	}
	var status string
	var attempts int
	if err := db.QueryRowContext(t.Context(), `SELECT status,processing_attempts FROM events WHERE id=$1`, e.ID).Scan(&status, &attempts); err != nil || status != "processing" || attempts != 1 {
		t.Fatalf("cancellation state %s %d %v", status, attempts, err)
	}
}
func TestRealPoolRecoversProcessorError(t *testing.T) {
	db := leaseDB(t)
	e := insertLeaseEvent(t, db, "processor-error")
	cfg := WorkerConfig{Workers: 2, LeaseDuration: time.Second, PollInterval: 10 * time.Millisecond, DBTimeout: 500 * time.Millisecond}
	var calls int
	var mu sync.Mutex
	processor := func(context.Context, Event) error {
		mu.Lock()
		defer mu.Unlock()
		calls++
		if calls == 1 {
			return errors.New("fixture failure")
		}
		return nil
	}
	pool, err := NewWorkerPool(postgresStore{db: db}, processor, cfg, func(error) {})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- pool.Run(ctx) }()
	complete := false
	for ctx.Err() == nil {
		var status string
		if err := db.QueryRowContext(ctx, `SELECT status FROM events WHERE id=$1`, e.ID).Scan(&status); err != nil {
			t.Fatal(err)
		}
		if status == "processed" {
			complete = true
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	awaitWorkerExit(t, done)
	mu.Lock()
	count := calls
	mu.Unlock()
	if !complete || count != 2 {
		t.Fatalf("recovery complete %v calls %d", complete, count)
	}
	var attempts int
	if err := db.QueryRowContext(t.Context(), `SELECT processing_attempts FROM events WHERE id=$1`, e.ID).Scan(&attempts); err != nil || attempts != 2 {
		t.Fatalf("error recovery attempts %d %v", attempts, err)
	}
}
