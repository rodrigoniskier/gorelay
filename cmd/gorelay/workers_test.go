package main

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type memoryLeaseStore struct {
	mu              sync.Mutex
	remaining       int
	claims          int
	completed       int
	completedSignal chan struct{}
	completeErr     error
}

func (s *memoryLeaseStore) Claim(ctx context.Context, d time.Duration) (Lease, error) {
	if err := ctx.Err(); err != nil {
		return Lease{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.remaining == 0 {
		return Lease{}, errNoClaimableEvent
	}
	s.remaining--
	s.claims++
	return Lease{Event: Event{ID: "fixture"}, Token: "fixture", ExpiresAt: time.Now().Add(d)}, nil
}
func (s *memoryLeaseStore) Complete(ctx context.Context, id, token string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.completeErr != nil {
		return s.completeErr
	}
	s.completed++
	if s.completedSignal != nil {
		select {
		case s.completedSignal <- struct{}{}:
		default:
		}
	}
	return nil
}
func testWorkerConfig() WorkerConfig {
	return WorkerConfig{Workers: 2, LeaseDuration: time.Second, PollInterval: 5 * time.Millisecond, DBTimeout: time.Second}
}
func awaitWorkerSignal(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatal("worker did not signal")
	}
}
func awaitWorkerExit(t *testing.T, ch <-chan error) {
	t.Helper()
	select {
	case err := <-ch:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("worker did not stop")
	}
}

func TestWorkerPoolBoundAndCancellation(t *testing.T) {
	store := &memoryLeaseStore{remaining: 10}
	cfg := testWorkerConfig()
	cfg.Workers = 3
	started := make(chan struct{}, 3)
	var active atomic.Int64
	pool, err := NewWorkerPool(store, func(ctx context.Context, _ Event) error {
		active.Add(1)
		defer active.Add(-1)
		started <- struct{}{}
		<-ctx.Done()
		return ctx.Err()
	}, cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- pool.Run(ctx) }()
	for range 3 {
		awaitWorkerSignal(t, started)
	}
	if active.Load() != 3 {
		t.Fatalf("active %d", active.Load())
	}
	if err := pool.Run(ctx); err == nil {
		t.Fatal("same pool started twice")
	}
	cancel()
	awaitWorkerExit(t, done)
	store.mu.Lock()
	defer store.mu.Unlock()
	if active.Load() != 0 || store.claims != 3 || store.completed != 0 {
		t.Fatalf("active=%d claims=%d completed=%d", active.Load(), store.claims, store.completed)
	}
}
func TestWorkerPoolCompletesOnlySuccess(t *testing.T) {
	for _, tc := range []struct {
		name                    string
		processErr, completeErr error
		want                    int
	}{
		{"success", nil, nil, 1}, {"handler failure", errors.New("failure"), nil, 0}, {"stale completion", nil, errLeaseLost, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			signal := make(chan struct{}, 1)
			store := &memoryLeaseStore{remaining: 1, completedSignal: signal, completeErr: tc.completeErr}
			pool, err := NewWorkerPool(store, func(context.Context, Event) error { return tc.processErr }, testWorkerConfig(), func(error) {
				select {
				case signal <- struct{}{}:
				default:
				}
			})
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- pool.Run(ctx) }()
			awaitWorkerSignal(t, signal)
			cancel()
			awaitWorkerExit(t, done)
			store.mu.Lock()
			defer store.mu.Unlock()
			if store.completed != tc.want {
				t.Fatalf("completed %d", store.completed)
			}
		})
	}
}
func TestWorkerConfigValidation(t *testing.T) {
	valid := testWorkerConfig()
	if _, err := NewWorkerPool(nil, func(context.Context, Event) error { return nil }, valid, nil); err == nil {
		t.Fatal("nil store accepted")
	}
	if _, err := NewWorkerPool(&memoryLeaseStore{}, nil, valid, nil); err == nil {
		t.Fatal("nil processor accepted")
	}
	for _, mutate := range []func(*WorkerConfig){
		func(c *WorkerConfig) { c.Workers = 0 }, func(c *WorkerConfig) { c.Workers = 33 },
		func(c *WorkerConfig) { c.LeaseDuration = 99 * time.Millisecond }, func(c *WorkerConfig) { c.LeaseDuration = 25 * time.Hour },
		func(c *WorkerConfig) { c.PollInterval = 0 }, func(c *WorkerConfig) { c.PollInterval = 2 * time.Minute },
		func(c *WorkerConfig) { c.DBTimeout = 0 }, func(c *WorkerConfig) { c.DBTimeout = 2 * time.Second },
	} {
		c := valid
		mutate(&c)
		if c.validate() == nil {
			t.Fatalf("invalid config accepted %+v", c)
		}
	}
}
func TestWorkerEnvironment(t *testing.T) {
	names := []string{"GORELAY_WORKERS", "GORELAY_LEASE_DURATION", "GORELAY_POLL_INTERVAL", "GORELAY_WORKER_DB_TIMEOUT"}
	for _, n := range names {
		t.Setenv(n, "")
	}
	cfg, err := workerConfigFromEnv()
	if err != nil || cfg.Workers != 0 {
		t.Fatalf("default %+v %v", cfg, err)
	}
	for _, tc := range []struct{ name, value string }{
		{"GORELAY_WORKERS", "33"}, {"GORELAY_WORKERS", "bad"}, {"GORELAY_WORKERS", "-1"},
		{"GORELAY_LEASE_DURATION", "bad"}, {"GORELAY_POLL_INTERVAL", "0s"}, {"GORELAY_WORKER_DB_TIMEOUT", "0s"},
	} {
		t.Run(tc.name+tc.value, func(t *testing.T) {
			t.Setenv(tc.name, tc.value)
			if _, err := workerConfigFromEnv(); err == nil {
				t.Fatal("invalid environment accepted")
			}
		})
	}
	t.Setenv("GORELAY_WORKERS", "2")
	t.Setenv("DATABASE_URL", "")
	if err := run(t.Context(), "127.0.0.1:0"); err == nil || !strings.Contains(err.Error(), "without a processor") {
		t.Fatalf("enabled command should fail without processor: %v", err)
	}
}
